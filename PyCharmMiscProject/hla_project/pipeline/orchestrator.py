"""
Pipeline orchestrator.
Coordinates the full literature mining pipeline from search to knowledge graph.
"""

import logging
import time
from typing import Dict, List, Optional

import config

logger = logging.getLogger(__name__)


def run_search(disease: str = "all", source: str = "all", fetch_batch_size: int = None) -> List[Dict]:
    """
    Run the search step for specified disease(s) and source(s).

    Args:
        disease: Disease code (MS, NMOSD, MG, GBS) or 'all'
        source: 'pubmed', 'biorxiv', or 'all'
        fetch_batch_size: Batch size for fetching paper details
    """
    results = []
    diseases = list(config.DISEASES.keys()) if disease == "all" else [disease.upper()]

    for d in diseases:
        if d not in config.DISEASES:
            logger.error(f"Unknown disease code: {d}")
            continue

        if source in ("pubmed", "all"):
            from search.pubmed_search import search_and_save as pubmed_search
            logger.info(f"Searching PubMed for {d}...")
            result = pubmed_search(d, fetch_batch_size=fetch_batch_size)
            results.append(result)

        if source in ("biorxiv", "all"):
            from search.biorxiv_search import search_and_save as biorxiv_search
            logger.info(f"Searching bioRxiv for {d}...")
            result = biorxiv_search(d)
            results.append(result)

    return results


def run_download(batch_size: int = 200, include_fulltext: bool = True, include_pdf: bool = True) -> Dict:
    """
    Run the download step: fetch missing abstracts, PMC full-text, and PDFs.
    """
    summary = {}

    # Fetch missing abstracts
    from download.abstract_fetcher import fetch_missing_abstracts, get_abstract_stats
    logger.info("Fetching missing abstracts...")
    abstracts_fetched = fetch_missing_abstracts(batch_size=batch_size)
    summary["abstracts_fetched"] = abstracts_fetched
    summary["abstract_stats"] = get_abstract_stats()

    if include_fulltext:
        # Fetch PMC full-text
        from download.pmc_fulltext import fetch_all_fulltext, get_fulltext_stats
        logger.info("Fetching PMC full-text...")
        fulltext_fetched = fetch_all_fulltext(batch_size=batch_size)
        summary["fulltext_fetched"] = fulltext_fetched
        summary["fulltext_stats"] = get_fulltext_stats()

    if include_pdf:
        # Process PDFs
        from download.pdf_parser import fetch_all_pdfs
        logger.info("Downloading and parsing PDFs...")
        pdfs_processed = fetch_all_pdfs()
        summary["pdfs_processed"] = pdfs_processed

    return summary


def run_extraction(
    batch_size: int = 100,
    model: str = None,
    disease_filter: str = None,
    dry_run: bool = False,
) -> Dict:
    """Run the LLM extraction step."""
    from extraction.batch_processor import process_batch, get_extraction_stats

    logger.info(f"Running extraction (batch_size={batch_size}, model={model or config.OPENAI_MODEL})...")
    result = process_batch(
        batch_size=batch_size,
        model=model,
        disease_filter=disease_filter,
        dry_run=dry_run,
    )

    stats = get_extraction_stats()
    result["overall_stats"] = stats

    return result


def run_validation() -> Dict:
    """Run validation on all extracted data."""
    from extraction.validator import validate_extraction
    from db.connection import get_session
    from db.models import HLAAssociation, HLACombination, Paper

    logger.info("Validating extracted data...")

    with get_session() as session:
        total_assocs = session.query(HLAAssociation).count()
        total_combos = session.query(HLACombination).count()

        # Check for common issues
        issues = {
            "invalid_allele_format": 0,
            "inconsistent_or_effect": 0,
            "missing_p_value": 0,
            "nonsignificant_p": 0,
            "extreme_or": 0,
        }

        import re
        allele_pattern = re.compile(r"^HLA-[A-Z][A-Za-z0-9]*\*\d+:\d+")

        associations = session.query(HLAAssociation).all()
        for a in associations:
            if not allele_pattern.match(a.allele or ""):
                issues["invalid_allele_format"] += 1
            if a.odds_ratio and a.effect:
                if (a.effect == "risk" and a.odds_ratio < 1) or (a.effect == "protective" and a.odds_ratio > 1):
                    issues["inconsistent_or_effect"] += 1
            if a.p_value is None:
                issues["missing_p_value"] += 1
            elif a.p_value > 0.05:
                issues["nonsignificant_p"] += 1
            if a.odds_ratio and a.odds_ratio > 100:
                issues["extreme_or"] += 1

    return {
        "total_associations": total_assocs,
        "total_combinations": total_combos,
        "issues": issues,
    }


def run_build_graph(clear_existing: bool = False) -> Dict:
    """Build the Neo4j knowledge graph from extracted data."""
    from knowledge_graph.graph_builder import build_graph

    logger.info("Building knowledge graph...")
    try:
        stats = build_graph(clear_existing=clear_existing)
        return stats
    except Exception as e:
        logger.error(f"Graph build skipped due to Neo4j connectivity or runtime error: {e}")
        return {
            "status": "skipped",
            "error": str(e),
        }


def run_full_pipeline(
    disease: str = "all",
    source: str = "all",
    extraction_batch_size: int = 100,
    fetch_batch_size: int = None,
    model: str = None,
    skip_search: bool = False,
    skip_download: bool = False,
    skip_extraction: bool = False,
    skip_graph: bool = False,
) -> Dict:
    """
    Run the complete pipeline end-to-end.

    Returns a summary dict with results from each step.
    """
    pipeline_summary = {}
    start_time = time.time()

    # Step 1: Search
    if not skip_search:
        logger.info("=" * 60)
        logger.info("STEP 1: SEARCHING")
        logger.info("=" * 60)
        search_results = run_search(disease=disease, source=source, fetch_batch_size=fetch_batch_size)
        pipeline_summary["search"] = search_results

    # Step 2: Download
    if not skip_download:
        logger.info("=" * 60)
        logger.info("STEP 2: DOWNLOADING")
        logger.info("=" * 60)
        download_results = run_download()
        pipeline_summary["download"] = download_results

    # Step 3: Extract
    if not skip_extraction:
        logger.info("=" * 60)
        logger.info("STEP 3: EXTRACTING")
        logger.info("=" * 60)
        extraction_results = run_extraction(
            batch_size=extraction_batch_size,
            model=model,
        )
        pipeline_summary["extraction"] = extraction_results

    # Step 4: Validate
    if not skip_extraction:
        logger.info("=" * 60)
        logger.info("STEP 4: VALIDATING")
        logger.info("=" * 60)
        validation_results = run_validation()
        pipeline_summary["validation"] = validation_results

    # Step 5: Build graph
    if not skip_graph:
        logger.info("=" * 60)
        logger.info("STEP 5: BUILDING KNOWLEDGE GRAPH")
        logger.info("=" * 60)
        graph_results = run_build_graph(clear_existing=True)
        pipeline_summary["graph"] = graph_results

    elapsed = time.time() - start_time
    pipeline_summary["elapsed_seconds"] = round(elapsed, 1)

    logger.info("=" * 60)
    logger.info(f"PIPELINE COMPLETE in {elapsed:.1f} seconds")
    logger.info("=" * 60)

    return pipeline_summary
