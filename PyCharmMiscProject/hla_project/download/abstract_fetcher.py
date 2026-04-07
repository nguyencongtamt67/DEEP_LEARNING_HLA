"""
Abstract fetcher module.
Fetches and updates abstracts for papers that are missing them.
Also handles re-fetching abstracts from PubMed when needed.
"""

import time
import logging
from typing import List, Dict, Optional
from xml.etree import ElementTree

from Bio import Entrez
from tqdm import tqdm

import config
from db.connection import get_session
from db.models import Paper

logger = logging.getLogger(__name__)

# Configure Entrez
Entrez.email = config.NCBI_EMAIL
Entrez.api_key = config.NCBI_API_KEY
Entrez.tool = config.NCBI_TOOL


def fetch_missing_abstracts(batch_size: int = None) -> int:
    """
    Find papers with missing abstracts and fetch them from PubMed.
    Returns the number of abstracts successfully fetched.
    """
    if batch_size is None:
        batch_size = config.PUBMED_BATCH_SIZE

    with get_session() as session:
        # Find papers with missing abstracts that have PMIDs
        papers = (
            session.query(Paper)
            .filter(
                Paper.pmid.isnot(None),
                (Paper.abstract.is_(None)) | (Paper.abstract == ""),
            )
            .all()
        )

        if not papers:
            logger.info("No papers with missing abstracts found.")
            return 0

        logger.info(f"Found {len(papers)} papers with missing abstracts")

        pmid_to_paper = {p.pmid: p for p in papers}
        pmids = list(pmid_to_paper.keys())
        updated = 0

        for start in tqdm(range(0, len(pmids), batch_size), desc="Fetching abstracts"):
            batch_pmids = pmids[start : start + batch_size]
            ids_str = ",".join(batch_pmids)

            try:
                handle = Entrez.efetch(
                    db="pubmed",
                    id=ids_str,
                    rettype="xml",
                    retmode="xml",
                )
                xml_data = handle.read()
                handle.close()

                abstracts = _parse_abstracts_from_xml(xml_data)

                for pmid, abstract_text in abstracts.items():
                    if pmid in pmid_to_paper and abstract_text:
                        pmid_to_paper[pmid].abstract = abstract_text
                        updated += 1

                time.sleep(1.0 / config.PUBMED_RATE_LIMIT)

            except Exception as e:
                logger.error(f"Error fetching abstracts batch at {start}: {e}")
                continue

        session.flush()

    logger.info(f"Updated {updated} abstracts out of {len(pmids)} papers")
    return updated


def _parse_abstracts_from_xml(xml_data: bytes) -> Dict[str, str]:
    """Parse PubMed XML and extract PMID -> abstract text mapping."""
    abstracts = {}

    try:
        root = ElementTree.fromstring(xml_data)
    except ElementTree.ParseError as e:
        logger.error(f"XML parse error: {e}")
        return abstracts

    for article in root.findall(".//PubmedArticle"):
        pmid_elem = article.find(".//MedlineCitation/PMID")
        if pmid_elem is None:
            continue
        pmid = pmid_elem.text

        abstract_elem = article.find(".//Article/Abstract")
        if abstract_elem is not None:
            parts = []
            for text_elem in abstract_elem.findall("AbstractText"):
                label = text_elem.get("Label", "")
                text = _get_text_content(text_elem)
                if label:
                    parts.append(f"{label}: {text}")
                else:
                    parts.append(text)
            abstracts[pmid] = "\n".join(parts)

    return abstracts


def _get_text_content(element) -> str:
    """Extract all text content from an XML element."""
    if element is None:
        return ""
    parts = []
    if element.text:
        parts.append(element.text)
    for child in element:
        if child.text:
            parts.append(child.text)
        if child.tail:
            parts.append(child.tail)
    return "".join(parts).strip()


def get_abstract_stats() -> Dict:
    """Get statistics about abstract availability in the database."""
    with get_session() as session:
        total = session.query(Paper).count()
        with_abstract = (
            session.query(Paper)
            .filter(Paper.abstract.isnot(None), Paper.abstract != "")
            .count()
        )
        without_abstract = total - with_abstract

    return {
        "total_papers": total,
        "with_abstract": with_abstract,
        "without_abstract": without_abstract,
        "coverage": f"{with_abstract / total * 100:.1f}%" if total > 0 else "0%",
    }
