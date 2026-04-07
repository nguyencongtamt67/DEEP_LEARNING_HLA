"""
PMC full-text fetcher module.
Downloads full-text articles from PubMed Central (PMC) Open Access subset.
"""

import time
import logging
import ssl
import urllib.parse
import urllib.request
from typing import List, Dict, Optional
from xml.etree import ElementTree

from Bio import Entrez
import requests
from tqdm import tqdm

import config
from db.connection import get_session
from db.models import Paper

logger = logging.getLogger(__name__)

# Configure Entrez
Entrez.email = config.NCBI_EMAIL
Entrez.api_key = config.NCBI_API_KEY
Entrez.tool = config.NCBI_TOOL

PMC_OA_API = "https://www.ncbi.nlm.nih.gov/pmc/utils/oa/oa.fcgi"
ID_CONVERTER_API = "https://www.ncbi.nlm.nih.gov/pmc/utils/idconv/v1.0/"


def _build_ssl_context() -> ssl.SSLContext:
    """Create an SSL context from configured CA bundle options."""
    cafile = config.SSL_CERT_FILE or config.REQUESTS_CA_BUNDLE or None
    if cafile:
        return ssl.create_default_context(cafile=cafile)
    return ssl.create_default_context()


def _fetch_pmc_xml_via_http(pmc_id: str, context: ssl.SSLContext) -> bytes:
    """Fallback XML fetch using NCBI E-utilities endpoint over HTTPS."""
    params = {
        "db": "pmc",
        "id": pmc_id,
        "rettype": "xml",
        "retmode": "xml",
        "tool": config.NCBI_TOOL,
        "email": config.NCBI_EMAIL,
    }
    if config.NCBI_API_KEY:
        params["api_key"] = config.NCBI_API_KEY

    url = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/efetch.fcgi?" + urllib.parse.urlencode(params)
    req = urllib.request.Request(url, headers={"User-Agent": config.NCBI_TOOL})
    with urllib.request.urlopen(req, context=context, timeout=45) as resp:
        return resp.read()


def find_pmc_papers() -> List[Paper]:
    """Find papers in DB that have a PMC ID but no full text yet."""
    with get_session() as session:
        papers = (
            session.query(Paper)
            .filter(
                Paper.pmc_id.isnot(None),
                Paper.has_full_text == False,
            )
            .all()
        )
        # Detach from session to use outside context
        for p in papers:
            session.expunge(p)
        return papers


def convert_pmids_to_pmcids(pmids: List[str]) -> Dict[str, str]:
    """
    Convert PMIDs to PMC IDs using the NCBI ID Converter API.
    Returns mapping of PMID -> PMC ID.
    """
    pmid_to_pmc = {}

    for start in range(0, len(pmids), 200):
        batch = pmids[start : start + 200]
        ids_str = ",".join(batch)

        try:
            resp = requests.get(
                ID_CONVERTER_API,
                params={
                    "ids": ids_str,
                    "format": "json",
                    "tool": config.NCBI_TOOL,
                    "email": config.NCBI_EMAIL,
                },
                timeout=30,
            )
            resp.raise_for_status()
            data = resp.json()

            for record in data.get("records", []):
                pmid = record.get("pmid", "")
                pmcid = record.get("pmcid", "")
                if pmid and pmcid:
                    pmid_to_pmc[pmid] = pmcid

            time.sleep(0.5)

        except Exception as e:
            logger.error(f"Error converting PMIDs batch: {e}")
            continue

    logger.info(f"Converted {len(pmid_to_pmc)} PMIDs to PMC IDs out of {len(pmids)}")
    return pmid_to_pmc


def fetch_pmc_fulltext(pmc_id: str) -> Optional[str]:
    """
    Fetch full-text XML from PMC and extract the body text.
    Returns the extracted text or None on failure.
    """
    try:
        # First attempt via Biopython Entrez (default path)
        handle = Entrez.efetch(
            db="pmc",
            id=pmc_id,
            rettype="xml",
            retmode="xml",
        )
        xml_data = handle.read()
        handle.close()
        text = _parse_pmc_xml(xml_data)
        return text
    except ssl.SSLCertVerificationError as e:
        logger.warning(
            f"SSL verification failed for PMC {pmc_id} via Entrez: {e}. "
            "Retrying with explicit CA bundle settings."
        )
    except Exception as e:
        logger.error(f"Error fetching PMC {pmc_id} via Entrez: {e}")

    try:
        xml_data = _fetch_pmc_xml_via_http(pmc_id, _build_ssl_context())
        text = _parse_pmc_xml(xml_data)
        return text
    except ssl.SSLCertVerificationError as e:
        if not config.ALLOW_INSECURE_SSL:
            logger.error(
                f"Error fetching PMC {pmc_id}: SSL verification failed after fallback: {e}. "
                "Set SSL_CERT_FILE or REQUESTS_CA_BUNDLE, or enable ALLOW_INSECURE_SSL=true as a temporary workaround."
            )
            return None

        logger.warning(
            f"Retrying PMC {pmc_id} with SSL verification disabled because ALLOW_INSECURE_SSL=true"
        )
        try:
            insecure_ctx = ssl._create_unverified_context()
            xml_data = _fetch_pmc_xml_via_http(pmc_id, insecure_ctx)
            text = _parse_pmc_xml(xml_data)
            return text
        except Exception as insecure_err:
            logger.error(f"Error fetching PMC {pmc_id} with insecure SSL fallback: {insecure_err}")
            return None
    except Exception as e:
        logger.error(f"Error fetching PMC {pmc_id}: {e}")
        return None


def _parse_pmc_xml(xml_data: bytes) -> Optional[str]:
    """Parse PMC XML and extract body text."""
    try:
        root = ElementTree.fromstring(xml_data)
    except ElementTree.ParseError as e:
        logger.error(f"PMC XML parse error: {e}")
        return None

    # Extract body text
    body = root.find(".//body")
    if body is None:
        # Try alternative paths
        body = root.find(".//article-body")
        if body is None:
            return None

    sections = []

    for sec in body.iter("sec"):
        title_elem = sec.find("title")
        section_title = title_elem.text if title_elem is not None else ""

        paragraphs = []
        for p in sec.findall("p"):
            text = _extract_all_text(p)
            if text:
                paragraphs.append(text)

        if paragraphs:
            if section_title:
                sections.append(f"\n## {section_title}\n")
            sections.extend(paragraphs)

    # If no sections found, try to get all paragraphs directly
    if not sections:
        for p in body.findall(".//p"):
            text = _extract_all_text(p)
            if text:
                sections.append(text)

    return "\n\n".join(sections) if sections else None


def _extract_all_text(element) -> str:
    """Recursively extract all text from an XML element and its children."""
    parts = []
    if element.text:
        parts.append(element.text)
    for child in element:
        child_text = _extract_all_text(child)
        if child_text:
            parts.append(child_text)
        if child.tail:
            parts.append(child.tail)
    return " ".join(parts).strip()


def update_pmc_ids_in_db() -> int:
    """
    Find papers without PMC IDs, attempt to convert their PMIDs.
    Returns number of papers updated with PMC IDs.
    """
    with get_session() as session:
        papers = (
            session.query(Paper)
            .filter(
                Paper.pmid.isnot(None),
                Paper.pmc_id.is_(None),
            )
            .all()
        )

        if not papers:
            logger.info("No papers need PMC ID lookup")
            return 0

        pmids = [p.pmid for p in papers]
        pmid_to_paper = {p.pmid: p for p in papers}

        pmid_to_pmc = convert_pmids_to_pmcids(pmids)

        updated = 0
        for pmid, pmc_id in pmid_to_pmc.items():
            if pmid in pmid_to_paper:
                pmid_to_paper[pmid].pmc_id = pmc_id
                updated += 1

        session.flush()

    logger.info(f"Updated {updated} papers with PMC IDs")
    return updated


def fetch_all_fulltext(batch_size: int = 50) -> int:
    """
    Fetch full-text for all papers that have PMC IDs but no full text.
    Returns number of papers successfully updated with full text.
    """
    # First, try to find PMC IDs for papers that don't have them
    update_pmc_ids_in_db()

    with get_session() as session:
        papers = (
            session.query(Paper)
            .filter(
                Paper.pmc_id.isnot(None),
                Paper.has_full_text == False,
            )
            .all()
        )

        if not papers:
            logger.info("No papers with PMC IDs need full text fetching")
            return 0

        logger.info(f"Fetching full text for {len(papers)} papers from PMC")
        fetched = 0

        for paper in tqdm(papers, desc="Fetching PMC full text"):
            text = fetch_pmc_fulltext(paper.pmc_id)
            if text:
                paper.full_text = text
                paper.has_full_text = True
                fetched += 1

            time.sleep(1.0 / config.PUBMED_RATE_LIMIT)

        session.flush()

    logger.info(f"Successfully fetched full text for {fetched}/{len(papers)} papers")
    return fetched


def get_fulltext_stats() -> Dict:
    """Get statistics about full-text availability."""
    with get_session() as session:
        total = session.query(Paper).count()
        with_pmc = session.query(Paper).filter(Paper.pmc_id.isnot(None)).count()
        with_fulltext = session.query(Paper).filter(Paper.has_full_text == True).count()

    return {
        "total_papers": total,
        "with_pmc_id": with_pmc,
        "with_full_text": with_fulltext,
        "pmc_coverage": f"{with_pmc / total * 100:.1f}%" if total > 0 else "0%",
        "fulltext_coverage": f"{with_fulltext / total * 100:.1f}%" if total > 0 else "0%",
    }
