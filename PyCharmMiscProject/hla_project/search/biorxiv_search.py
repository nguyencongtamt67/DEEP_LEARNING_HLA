"""
bioRxiv/medRxiv search module.
Searches for HLA-disease association preprints using the bioRxiv/medRxiv API.

API reference: https://api.medrxiv.org
Endpoint (date-range):
    https://api.medrxiv.org/details/[server]/[YYYY-MM-DD]/[YYYY-MM-DD]/[cursor]/[format]
Endpoint (recent N posts):
    https://api.medrxiv.org/details/[server]/[N]/[cursor]/[format]
Endpoint (recent N days):
    https://api.medrxiv.org/details/[server]/[Nd]/[cursor]/[format]
Endpoint (single DOI):
    https://api.medrxiv.org/details/[server]/[DOI]/na/[format]

Parameters:
    server  - "biorxiv" or "medrxiv"
    cursor  - pagination offset (default 0); 100 results per page
    format  - "json" or "xml" (OAI-PMH XML)

API metadata fields returned per paper:
    doi, title, authors, author_corresponding,
    author_corresponding_institution, date, version, type,
    license, category, jats_xml_path, abstract, published, server
"""

import time
import logging
from typing import List, Dict, Optional, Tuple
from datetime import datetime, date, timedelta

import requests
from tqdm import tqdm

import config
from db.connection import get_session
from db.models import Paper, SearchLog

logger = logging.getLogger(__name__)

# Official bioRxiv/medRxiv content-detail endpoint (serves both servers)
BIORXIV_API_BASE = "https://api.medrxiv.org/details"
PAGE_SIZE = 100  # API returns max 100 results per call
DEFAULT_FORMAT = "json"
CHUNK_DAYS = 90  # Split wide date ranges into ~3-month intervals to avoid 500 errors
# bioRxiv launched 2013-11, medRxiv launched 2019-06
SERVER_START_DATES = {
    "biorxiv": "2013-11-01",
    "medrxiv": "2019-06-01",
}


def build_search_terms(disease_code: str) -> List[str]:
    """Build search keyword list for a specific disease."""
    disease_info = config.DISEASES.get(disease_code)
    if not disease_info:
        raise ValueError(f"Unknown disease code: {disease_code}")

    # Combine disease terms with HLA terms
    terms = []
    for dt in disease_info["query_terms"]:
        for ht in ["HLA", "MHC", "Human Leukocyte Antigen"]:
            terms.append(f"{dt} {ht}")
    return terms


def _api_get(url: str) -> Optional[Dict]:
    """
    Send a GET request to the bioRxiv/medRxiv API and return parsed JSON.
    Returns None on any network or parsing error.
    """
    try:
        resp = requests.get(url, timeout=30)
        resp.raise_for_status()
        return resp.json()
    except requests.RequestException as e:
        logger.error(f"HTTP error for {url}: {e}")
        return None
    except ValueError as e:
        logger.error(f"JSON decode error for {url}: {e}")
        return None


def _generate_date_chunks(
    start_date: str, end_date: str, server: str, chunk_days: int = CHUNK_DAYS
) -> List[Tuple[str, str]]:
    """
    Split a wide date range into smaller intervals of *chunk_days* days.

    The API returns a 500 error for very large date ranges, so we query in
    manageable slices.  The start date is also clipped to the server launch
    date (bioRxiv: 2013-11, medRxiv: 2019-06) to avoid pointless requests.

    Returns:
        List of (chunk_start, chunk_end) strings in YYYY-MM-DD format.
    """
    start = datetime.strptime(start_date, "%Y-%m-%d").date()
    end = datetime.strptime(end_date, "%Y-%m-%d").date()

    # Clip to server launch date
    server_start_str = SERVER_START_DATES.get(server)
    if server_start_str:
        server_start = datetime.strptime(server_start_str, "%Y-%m-%d").date()
        if start < server_start:
            start = server_start

    if start > end:
        return []

    chunks = []
    cursor = start
    while cursor <= end:
        chunk_end = min(cursor + timedelta(days=chunk_days - 1), end)
        chunks.append((cursor.strftime("%Y-%m-%d"), chunk_end.strftime("%Y-%m-%d")))
        cursor = chunk_end + timedelta(days=1)

    return chunks


def _fetch_chunk(
    server: str,
    chunk_start: str,
    chunk_end: str,
    disease_keywords: List[str],
    hla_keywords: List[str],
    max_results: int,
) -> List[Dict]:
    """
    Fetch and keyword-filter all papers in a single date-range chunk.

    Paginates through the API (100 results per page) until all papers in
    the interval have been retrieved or *max_results* is reached.
    """
    matched = []
    cursor = 0

    while cursor < max_results:
        url = (
            f"{BIORXIV_API_BASE}/{server}"
            f"/{chunk_start}/{chunk_end}/{cursor}/{DEFAULT_FORMAT}"
        )

        data = _api_get(url)
        if data is None:
            break

        # The 'messages' array: cursor, count of all items, count of new papers
        messages = data.get("messages", [{}])
        msg = messages[0] if messages else {}
        total = int(msg.get("total", 0))

        papers_batch = data.get("collection", [])
        if not papers_batch:
            break

        for paper in papers_batch:
            title = (paper.get("title") or "").lower()
            abstract = (paper.get("abstract") or "").lower()
            text = f"{title} {abstract}"

            has_disease = any(kw in text for kw in disease_keywords)
            has_hla = any(kw in text for kw in hla_keywords)

            if has_disease and has_hla:
                parsed = _parse_biorxiv_paper(paper, server)
                if parsed:
                    matched.append(parsed)

        cursor += PAGE_SIZE

        if cursor >= total:
            break

        time.sleep(0.5)  # Rate limiting

    return matched


def search_biorxiv(
    disease_code: str,
    server: str = "biorxiv",
    start_date: str = "2000-01-01",
    end_date: str = "2025-12-31",
    max_results: int = 5000,
) -> List[Dict]:
    """
    Search bioRxiv/medRxiv for papers matching HLA-disease terms.

    Uses the content-detail endpoint with a date-range interval:
        https://api.medrxiv.org/details/[server]/[start]/[end]/[cursor]/json

    Wide date ranges are automatically split into 30-day chunks to avoid
    API 500 errors.  The start date is clipped to the server launch date.
    Results are deduplicated by DOI across chunks.

    Args:
        disease_code: Key into config.DISEASES (e.g. "MS", "NMOSD").
        server: "biorxiv" or "medrxiv".
        start_date: Start of date range (YYYY-MM-DD).
        end_date: End of date range (YYYY-MM-DD).
        max_results: Safety cap on total results to fetch.

    Returns:
        List of parsed paper dicts matching disease + HLA criteria.
    """
    disease_info = config.DISEASES.get(disease_code)
    if not disease_info:
        raise ValueError(f"Unknown disease code: {disease_code}")

    # Build keyword sets for filtering
    disease_keywords = [t.lower() for t in disease_info["query_terms"]]
    hla_keywords = ["hla", "mhc", "human leukocyte antigen", "major histocompatibility"]

    chunks = _generate_date_chunks(start_date, end_date, server)
    logger.info(
        f"Searching {server} for disease: {disease_code} "
        f"({len(chunks)} date chunks)"
    )

    seen_dois: set = set()
    all_papers: List[Dict] = []

    for chunk_start, chunk_end in tqdm(
        chunks, desc=f"{server} chunks", disable=not logger.isEnabledFor(logging.INFO)
    ):
        matched = _fetch_chunk(
            server, chunk_start, chunk_end,
            disease_keywords, hla_keywords, max_results,
        )

        # Deduplicate across chunks (updated preprints may span chunks)
        for paper in matched:
            doi = paper["doi"]
            if doi not in seen_dois:
                seen_dois.add(doi)
                all_papers.append(paper)

        if len(all_papers) >= max_results:
            all_papers = all_papers[:max_results]
            break

    logger.info(f"Found {len(all_papers)} relevant papers on {server} for {disease_code}")
    return all_papers


def fetch_paper_by_doi(doi: str, server: str = "biorxiv") -> Optional[Dict]:
    """
    Fetch metadata for a single paper by DOI.

    Endpoint:
        https://api.medrxiv.org/details/[server]/[DOI]/na/json

    If multiple versions exist the most recent version is returned.

    Args:
        doi: The DOI of the paper (e.g. "10.1101/2020.09.09.20191205").
        server: "biorxiv" or "medrxiv".

    Returns:
        Parsed paper dict, or None if not found / error.
    """
    url = f"{BIORXIV_API_BASE}/{server}/{doi}/na/{DEFAULT_FORMAT}"

    data = _api_get(url)
    if data is None:
        return None

    collection = data.get("collection", [])
    if not collection:
        logger.warning(f"No results for DOI {doi} on {server}")
        return None

    # Return the most recent version (last entry)
    return _parse_biorxiv_paper(collection[-1], server)


def _parse_biorxiv_paper(paper: Dict, server: str) -> Optional[Dict]:
    """
    Parse a bioRxiv/medRxiv API response entry into our standard format.

    Maps the API metadata fields:
        doi, title, authors, author_corresponding,
        author_corresponding_institution, date, version, type,
        license, category, jats_xml_path, abstract, published, server
    """
    doi = paper.get("doi")
    if not doi:
        return None

    # Parse year from date (YYYY-MM-DD)
    year = None
    date_str = paper.get("date", "")
    if date_str:
        try:
            year = int(date_str[:4])
        except (ValueError, IndexError):
            pass

    return {
        # Fields that map to the Paper DB model
        "pmid": None,  # bioRxiv papers don't have PMIDs
        "doi": doi,
        "title": paper.get("title", "No title"),
        "authors": paper.get("authors", ""),
        "journal": f"{paper.get('server', server)} (preprint)",
        "year": year,
        "abstract": paper.get("abstract", ""),
        "source": paper.get("server", server),
        "pmc_id": None,
        # Additional API metadata (kept for downstream processing)
        "author_corresponding": paper.get("author_corresponding", ""),
        "author_corresponding_institution": paper.get(
            "author_corresponding_institution", ""
        ),
        "date": date_str,
        "version": paper.get("version", ""),
        "type": paper.get("type", ""),
        "license": paper.get("license", ""),
        "category": paper.get("category", ""),
        "jats_xml_path": paper.get("jatsxml", ""),
        "published": paper.get("published", ""),
    }


def save_papers_to_db(papers: List[Dict]) -> int:
    """
    Save bioRxiv papers to PostgreSQL. Skips duplicates by DOI.
    Returns the number of newly inserted papers.
    """
    inserted = 0

    with get_session() as session:
        for paper_data in papers:
            doi = paper_data.get("doi")
            if not doi:
                continue

            # Skip if DOI already exists
            existing = session.query(Paper).filter_by(doi=doi).first()
            if existing:
                logger.debug(f"Paper DOI {doi} already exists, skipping.")
                continue

            paper = Paper(
                pmid=paper_data.get("pmid"),
                doi=doi,
                title=paper_data["title"],
                authors=paper_data.get("authors"),
                journal=paper_data.get("journal"),
                year=paper_data.get("year"),
                abstract=paper_data.get("abstract"),
                source=paper_data.get("source", "biorxiv"),
                pmc_id=paper_data.get("pmc_id"),
                has_full_text=False,
            )
            session.add(paper)
            inserted += 1

    logger.info(f"Inserted {inserted} new bioRxiv papers (skipped {len(papers) - inserted} duplicates)")
    return inserted


def search_and_save(disease_code: str) -> Dict:
    """
    Full bioRxiv search workflow for a disease.
    Searches both bioRxiv and medRxiv.
    """
    all_papers = []

    for server in ["biorxiv", "medrxiv"]:
        papers = search_biorxiv(
            disease_code=disease_code,
            server=server,
            start_date=config.SEARCH_DATE_RANGE[0].replace("/", "-"),
            end_date=config.SEARCH_DATE_RANGE[1].replace("/", "-"),
        )
        all_papers.extend(papers)

    inserted = save_papers_to_db(all_papers)

    # Log the search
    with get_session() as session:
        log = SearchLog(
            disease_code=disease_code,
            source="biorxiv",
            query_text=f"HLA + {config.DISEASES[disease_code]['name']} (biorxiv + medrxiv)",
            result_count=len(all_papers),
        )
        session.add(log)

    summary = {
        "disease": disease_code,
        "disease_name": config.DISEASES[disease_code]["name"],
        "source": "biorxiv+medrxiv",
        "found": len(all_papers),
        "inserted": inserted,
    }
    logger.info(f"bioRxiv search complete: {summary}")
    return summary


def search_all_diseases() -> List[Dict]:
    """Search bioRxiv/medRxiv for all configured diseases."""
    results = []
    for disease_code in config.DISEASES:
        result = search_and_save(disease_code)
        results.append(result)
    return results
