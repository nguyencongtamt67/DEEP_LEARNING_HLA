"""
Batch processor for LLM extraction of HLA-disease associations.
Handles concurrent processing, rate limiting, resume capability, and cost tracking.
"""

import asyncio
import json
import logging
import re
import time
from datetime import datetime
from typing import List, Dict, Optional

from sqlalchemy import select
from tqdm import tqdm

import config
from db.connection import get_session
from db.models import Paper, HLAAssociation, HLACombination, ExtractionLog
from extraction.llm_extractor import extract_from_paper
from extraction.validator import normalize_allele_name

logger = logging.getLogger(__name__)

# Regex to strip leading comparison operators from LLM-returned numeric strings
# e.g. "<0.001", ">1.5", "≤0.05", "~2.3", "= 0.01"
_NUMERIC_PREFIX_RE = re.compile(r"^[<>≤≥~=≈]+\s*")


def _safe_float(value) -> Optional[float]:
    """
    Safely convert a value to float.

    Handles LLM quirks such as:
      - comparison prefixes: "<0.001" -> 0.001
      - scientific notation strings: "1.2e-5" -> 0.000012
      - non-numeric strings: "N/A", "not reported" -> None
    """
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        cleaned = _NUMERIC_PREFIX_RE.sub("", value.strip())
        if not cleaned:
            return None
        try:
            return float(cleaned)
        except ValueError:
            logger.debug(f"Could not convert '{value}' to float, returning None")
            return None
    return None


def _safe_int(value) -> Optional[int]:
    """
    Safely convert a value to int.

    Handles LLM quirks such as:
      - float-like strings: "1234.0" -> 1234
      - comma-separated: "1,234" -> 1234
      - comparison prefixes: ">500" -> 500
      - non-numeric strings -> None
    """
    if value is None:
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return int(value)
    if isinstance(value, str):
        cleaned = _NUMERIC_PREFIX_RE.sub("", value.strip())
        cleaned = cleaned.replace(",", "")
        if not cleaned:
            return None
        try:
            return int(float(cleaned))
        except ValueError:
            logger.debug(f"Could not convert '{value}' to int, returning None")
            return None
    return None


def get_pending_papers(batch_size: int = None, disease_filter: str = None) -> List[Dict]:
    """
    Get papers that haven't been extracted yet.
    Returns list of paper dicts ready for extraction.
    """
    if batch_size is None:
        batch_size = config.EXTRACTION_BATCH_SIZE

    with get_session() as session:
        terminal_status_subq = (
            select(ExtractionLog.paper_id)
            .where(ExtractionLog.status.in_(["completed", "skipped"]))
            .distinct()
        )

        query = session.query(Paper).filter(
            ~Paper.id.in_(terminal_status_subq),  # No completed or skipped extraction
            (Paper.abstract.isnot(None)) | (Paper.has_full_text == True),
        )

        if disease_filter:
            # Filter by disease in title/abstract (rough filter)
            disease_name = config.DISEASES.get(disease_filter, {}).get("name", disease_filter)
            query = query.filter(
                Paper.title.ilike(f"%{disease_name}%")
                | Paper.abstract.ilike(f"%{disease_name}%")
            )

        papers = query.limit(batch_size).all()

        result = []
        for p in papers:
            result.append({
                "id": p.id,
                "pmid": p.pmid,
                "doi": p.doi,
                "title": p.title,
                "abstract": p.abstract,
                "full_text": p.full_text,
                "has_full_text": p.has_full_text,
            })

        return result


def process_single_paper(paper_data: Dict, model: str = None) -> Dict:
    """
    Process a single paper: extract and save results.
    Returns the extraction result dict.
    """
    paper_id = paper_data["id"]
    pmid = paper_data.get("pmid")

    logger.info(f"Processing paper {paper_id} (PMID: {pmid})")

    # Extract
    result = extract_from_paper(
        pmid=pmid,
        abstract=paper_data.get("abstract"),
        full_text=paper_data.get("full_text"),
        model=model,
    )

    # Save to database
    _save_extraction_result(paper_id, result, model)

    return result


def _save_extraction_result(paper_id: int, result: Dict, model: str = None):
    """Save extraction results to the database."""
    with get_session() as session:
        # Create extraction log
        log = ExtractionLog(
            paper_id=paper_id,
            status=result["status"],
            model_used=model or config.OPENAI_MODEL,
            tokens_input=result.get("tokens_input", 0),
            tokens_output=result.get("tokens_output", 0),
            cost_usd=result.get("cost_usd", 0),
            error_message=result.get("error"),
            extracted_at=datetime.utcnow() if result["status"] == "completed" else None,
        )
        session.add(log)

        # Save extracted associations
        data = result.get("data")
        if data and result["status"] == "completed":
            _save_associations(session, paper_id, data)
            _save_combinations(session, paper_id, data)


def _save_associations(session, paper_id: int, data: Dict):
    """Save individual HLA associations to the database."""
    disease = data.get("disease", "Unknown")
    population = data.get("population")
    sample_size = data.get("sample_size", {}) or {}

    for assoc_data in data.get("hla_associations", []):
        allele = assoc_data.get("allele", "")
        if not allele:
            continue

        # Normalize allele name
        allele = normalize_allele_name(allele)

        # Parse confidence interval
        ci = assoc_data.get("confidence_interval")
        ci_lower = _safe_float(ci[0]) if ci and len(ci) >= 2 else None
        ci_upper = _safe_float(ci[1]) if ci and len(ci) >= 2 else None

        assoc = HLAAssociation(
            paper_id=paper_id,
            disease=disease,
            population=population,
            allele=allele,
            effect=assoc_data.get("effect"),
            odds_ratio=_safe_float(assoc_data.get("odds_ratio")),
            ci_lower=ci_lower,
            ci_upper=ci_upper,
            p_value=_safe_float(assoc_data.get("p_value")),
            sample_cases=_safe_int(sample_size.get("cases")),
            sample_controls=_safe_int(sample_size.get("controls")),
            context=assoc_data.get("context"),
        )
        session.add(assoc)


def _save_combinations(session, paper_id: int, data: Dict):
    """Save HLA haplotype combinations to the database."""
    disease = data.get("disease", "Unknown")
    population = data.get("population")
    sample_size = data.get("sample_size", {}) or {}

    for combo_data in data.get("hla_combinations", []):
        alleles = combo_data.get("alleles", [])
        if not alleles:
            continue

        # Normalize allele names
        alleles = [normalize_allele_name(a) for a in alleles if a is not None]
        alleles = [a for a in alleles if a]
        if not alleles:
            continue

        ci = combo_data.get("confidence_interval")
        ci_lower = _safe_float(ci[0]) if ci and len(ci) >= 2 else None
        ci_upper = _safe_float(ci[1]) if ci and len(ci) >= 2 else None

        combo = HLACombination(
            paper_id=paper_id,
            disease=disease,
            population=population,
            alleles=alleles,
            haplotype_name=combo_data.get("haplotype"),
            effect=combo_data.get("effect"),
            odds_ratio=_safe_float(combo_data.get("odds_ratio")),
            ci_lower=ci_lower,
            ci_upper=ci_upper,
            p_value=_safe_float(combo_data.get("p_value")),
            sample_cases=_safe_int(sample_size.get("cases")),
            sample_controls=_safe_int(sample_size.get("controls")),
            context=combo_data.get("context"),
        )
        session.add(combo)


def process_batch(
    batch_size: int = None,
    model: str = None,
    disease_filter: str = None,
    dry_run: bool = False,
) -> Dict:
    """
    Process a batch of papers through the extraction pipeline.

    Args:
        batch_size: Number of papers to process
        model: OpenAI model to use
        disease_filter: Optional disease code to filter papers
        dry_run: If True, show what would be processed without actually calling the API

    Returns:
        Summary dict with counts and costs
    """
    if batch_size is None:
        batch_size = config.EXTRACTION_BATCH_SIZE

    papers = get_pending_papers(batch_size=batch_size, disease_filter=disease_filter)

    if not papers:
        logger.info("No pending papers to process")
        return {
            "processed": 0,
            "completed": 0,
            "failed": 0,
            "skipped": 0,
            "total_cost": 0,
            "total_tokens_input": 0,
            "total_tokens_output": 0,
        }

    logger.info(f"Processing batch of {len(papers)} papers")

    if dry_run:
        logger.info(f"DRY RUN: Would process {len(papers)} papers")
        for p in papers[:5]:
            logger.info(f"  - Paper {p['id']} (PMID: {p['pmid']}): {p['title'][:80]}...")
        return {"dry_run": True, "would_process": len(papers)}

    # Process papers sequentially with rate limiting
    completed = 0
    failed = 0
    skipped = 0
    total_cost = 0.0
    total_tokens_in = 0
    total_tokens_out = 0

    for paper_data in tqdm(papers, desc="Extracting HLA associations"):
        result = process_single_paper(paper_data, model=model)

        status = result.get("status", "failed")
        if status == "completed":
            completed += 1
        elif status == "failed":
            failed += 1
        elif status == "skipped":
            skipped += 1

        total_cost += result.get("cost_usd", 0)
        total_tokens_in += result.get("tokens_input", 0)
        total_tokens_out += result.get("tokens_output", 0)

        # Rate limiting between requests
        time.sleep(60.0 / config.OPENAI_RATE_LIMIT_RPM)

    summary = {
        "processed": len(papers),
        "completed": completed,
        "failed": failed,
        "skipped": skipped,
        "total_cost": round(total_cost, 4),
        "total_tokens_input": total_tokens_in,
        "total_tokens_output": total_tokens_out,
    }

    logger.info(f"Batch processing complete: {summary}")
    return summary


def get_extraction_stats() -> Dict:
    """Get statistics about the extraction process."""
    with get_session() as session:
        total_papers = session.query(Paper).count()
        total_logs = session.query(ExtractionLog).count()
        completed = session.query(ExtractionLog).filter_by(status="completed").count()
        failed = session.query(ExtractionLog).filter_by(status="failed").count()
        skipped = session.query(ExtractionLog).filter_by(status="skipped").count()
        total_associations = session.query(HLAAssociation).count()
        total_combinations = session.query(HLACombination).count()

        # Cost stats
        from sqlalchemy import func
        cost_result = session.query(func.sum(ExtractionLog.cost_usd)).scalar()
        total_cost = float(cost_result) if cost_result else 0

    return {
        "total_papers": total_papers,
        "extraction_attempts": total_logs,
        "completed": completed,
        "failed": failed,
        "skipped": skipped,
        "pending": total_papers - total_logs,
        "total_associations": total_associations,
        "total_combinations": total_combinations,
        "total_cost_usd": round(total_cost, 4),
    }
