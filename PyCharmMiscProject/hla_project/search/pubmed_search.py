"""
PubMed search module using NCBI Entrez E-utilities.
Searches for HLA-disease association papers and stores results in PostgreSQL.
"""

import time
import logging
import ssl
from typing import List, Dict, Optional
from xml.etree import ElementTree
import urllib.request

from Bio import Entrez
from tqdm import tqdm

import config
from db.connection import get_session
from db.models import Paper, SearchLog

logger = logging.getLogger(__name__)

# Configure Entrez
Entrez.email = config.NCBI_EMAIL
Entrez.api_key = config.NCBI_API_KEY
Entrez.tool = config.NCBI_TOOL

# Configure SSL context to handle certificate issues
ssl_context = ssl.create_default_context()
ssl_context.check_hostname = False
ssl_context.verify_mode = ssl.CERT_NONE

# Install custom opener with SSL context and timeout
opener = urllib.request.build_opener(
    urllib.request.HTTPSHandler(context=ssl_context),
    urllib.request.HTTPHandler()
)
# Set a reasonable timeout to prevent hanging
opener.addheaders = [('User-Agent', 'Python-urllib/3.14')]
urllib.request.install_opener(opener)


def build_query(disease_code: str) -> str:
    """
    Build a PubMed search query for a specific disease.
    Combines disease terms with HLA terms and association terms.
    """
    disease_info = config.DISEASES.get(disease_code)
    if not disease_info:
        raise ValueError(f"Unknown disease code: {disease_code}. Valid codes: {list(config.DISEASES.keys())}")

    # Disease terms (OR)
    disease_part = " OR ".join(disease_info["query_terms"])

    # HLA terms (OR)
    hla_part = " OR ".join(config.HLA_TERMS)

    # Association terms (OR)
    assoc_part = " OR ".join(config.ASSOCIATION_TERMS)

    query = f"({disease_part}) AND ({hla_part}) AND ({assoc_part})"

    # Add date range
    start_date, end_date = config.SEARCH_DATE_RANGE
    query += f" AND ({start_date}[PDAT] : {end_date}[PDAT])"

    # Add language filter
    for lang in config.SEARCH_LANGUAGES:
        query += f" AND {lang}[LA]"

    return query


def search_pmids(query: str, retmax: int = 10000) -> List[str]:
    """
    Search PubMed and return a list of PMIDs matching the query.
    """
    logger.info(f"Searching PubMed with query: {query[:200]}...")

    handle = Entrez.esearch(
        db="pubmed",
        term=query,
        retmax=retmax,
        usehistory="y",
        sort="relevance",
    )
    results = Entrez.read(handle)
    handle.close()

    pmid_list = results.get("IdList", [])
    total_count = int(results.get("Count", 0))

    logger.info(f"Found {total_count} total results, retrieved {len(pmid_list)} PMIDs")

    # If there are more results than retmax, fetch all using history
    if total_count > retmax:
        logger.warning(
            f"Query returned {total_count} results but retmax is {retmax}. "
            "Consider increasing retmax or refining the query."
        )

    return pmid_list


def fetch_paper_details(pmids: List[str], batch_size: int = None) -> List[Dict]:
    """
    Fetch detailed metadata for a list of PMIDs using efetch.
    Returns a list of paper dictionaries.
    """
    if batch_size is None:
        batch_size = config.PUBMED_FETCH_BATCH_SIZE

    all_papers = []
    failed_batches = []

    for start in tqdm(range(0, len(pmids), batch_size), desc="Fetching paper details"):
        batch = pmids[start : start + batch_size]
        ids_str = ",".join(batch)

        max_retries = 5  # Increased retries
        retry_delay = 3.0  # Increased base delay

        for attempt in range(max_retries):
            try:
                # Set timeout for the request
                import socket
                socket.setdefaulttimeout(30)  # 30 second timeout

                handle = Entrez.efetch(
                    db="pubmed",
                    id=ids_str,
                    rettype="xml",
                    retmode="xml",
                )
                xml_data = handle.read()
                handle.close()

                papers = _parse_pubmed_xml(xml_data)
                all_papers.extend(papers)

                # Rate limiting - increased delay between batches
                time.sleep(2.0 / config.PUBMED_RATE_LIMIT)
                break  # Success, exit retry loop

            except Exception as e:
                logger.warning(f"Error fetching batch starting at {start} (attempt {attempt + 1}/{max_retries}): {e}")

                if attempt < max_retries - 1:
                    # Exponential backoff with longer delays
                    sleep_time = retry_delay * (2 ** attempt) + (attempt * 5)  # Add extra delay per attempt
                    logger.info(f"Retrying in {sleep_time:.1f} seconds...")
                    time.sleep(sleep_time)
                else:
                    logger.error(f"Failed to fetch batch starting at {start} after {max_retries} attempts")
                    failed_batches.append((start, len(batch)))
                    break

    if failed_batches:
        logger.warning(f"Failed to fetch {len(failed_batches)} batches: {failed_batches}")

    return all_papers


def _parse_pubmed_xml(xml_data: bytes) -> List[Dict]:
    """Parse PubMed XML response and extract paper metadata."""
    papers = []

    try:
        root = ElementTree.fromstring(xml_data)
    except ElementTree.ParseError as e:
        logger.error(f"XML parse error: {e}")
        return papers

    for article in root.findall(".//PubmedArticle"):
        try:
            paper = _extract_article_data(article)
            if paper:
                papers.append(paper)
        except Exception as e:
            logger.error(f"Error parsing article: {e}")
            continue

    return papers


def _extract_article_data(article) -> Optional[Dict]:
    """Extract structured data from a single PubmedArticle XML element."""
    medline = article.find(".//MedlineCitation")
    if medline is None:
        return None

    # PMID
    pmid_elem = medline.find("PMID")
    pmid = pmid_elem.text if pmid_elem is not None else None
    if not pmid:
        return None

    article_elem = medline.find("Article")
    if article_elem is None:
        return None

    # Title
    title_elem = article_elem.find("ArticleTitle")
    title = _get_text_content(title_elem) if title_elem is not None else "No title"

    # Abstract
    abstract_elem = article_elem.find(".//Abstract")
    abstract = ""
    if abstract_elem is not None:
        abstract_parts = []
        for text_elem in abstract_elem.findall("AbstractText"):
            label = text_elem.get("Label", "")
            text = _get_text_content(text_elem)
            if label:
                abstract_parts.append(f"{label}: {text}")
            else:
                abstract_parts.append(text)
        abstract = "\n".join(abstract_parts)

    # Authors
    authors_list = []
    for author in article_elem.findall(".//Author"):
        last_name = author.findtext("LastName", "")
        fore_name = author.findtext("ForeName", "")
        if last_name:
            authors_list.append(f"{last_name} {fore_name}".strip())
    authors = "; ".join(authors_list)

    # Journal
    journal_elem = article_elem.find(".//Journal/Title")
    journal = journal_elem.text if journal_elem is not None else None

    # Year
    year = None
    pub_date = article_elem.find(".//Journal/JournalIssue/PubDate")
    if pub_date is not None:
        year_elem = pub_date.find("Year")
        if year_elem is not None and year_elem.text:
            try:
                year = int(year_elem.text)
            except ValueError:
                pass
        if year is None:
            medline_date = pub_date.findtext("MedlineDate", "")
            if medline_date:
                for part in medline_date.split():
                    try:
                        year = int(part[:4])
                        break
                    except ValueError:
                        continue

    # DOI
    doi = None
    for id_elem in article.findall(".//ArticleIdList/ArticleId"):
        if id_elem.get("IdType") == "doi":
            doi = id_elem.text
            break

    # PMC ID
    pmc_id = None
    for id_elem in article.findall(".//ArticleIdList/ArticleId"):
        if id_elem.get("IdType") == "pmc":
            pmc_id = id_elem.text
            break

    return {
        "pmid": pmid,
        "doi": doi,
        "title": title,
        "authors": authors,
        "journal": journal,
        "year": year,
        "abstract": abstract,
        "source": "pubmed",
        "pmc_id": pmc_id,
    }


def _get_text_content(element) -> str:
    """Get all text content from an XML element, including tail text of children."""
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


def save_papers_to_db(papers: List[Dict]) -> int:
    """
    Save fetched papers to PostgreSQL. Skips duplicates (by PMID).
    Returns the number of newly inserted papers.
    """
    inserted = 0

    with get_session() as session:
        for paper_data in papers:
            # Skip if PMID already exists
            existing = session.query(Paper).filter_by(pmid=paper_data["pmid"]).first()
            if existing:
                logger.debug(f"Paper PMID {paper_data['pmid']} already exists, skipping.")
                continue

            paper = Paper(
                pmid=paper_data["pmid"],
                doi=paper_data.get("doi"),
                title=paper_data["title"],
                authors=paper_data.get("authors"),
                journal=paper_data.get("journal"),
                year=paper_data.get("year"),
                abstract=paper_data.get("abstract"),
                source=paper_data.get("source", "pubmed"),
                pmc_id=paper_data.get("pmc_id"),
                has_full_text=False,
            )
            session.add(paper)
            inserted += 1

        session.flush()

    logger.info(f"Inserted {inserted} new papers (skipped {len(papers) - inserted} duplicates)")
    return inserted


def search_and_save(disease_code: str, fetch_batch_size: int = None) -> Dict:
    """
    Full search workflow: build query, search PubMed, fetch details, save to DB.
    Returns a summary dict.
    """
    query = build_query(disease_code)
    logger.info(f"Searching for disease: {disease_code} ({config.DISEASES[disease_code]['name']})")

    # Search for PMIDs
    pmids = search_pmids(query)
    if not pmids:
        logger.warning(f"No results found for {disease_code}")
        return {"disease": disease_code, "query": query, "found": 0, "inserted": 0}

    # Fetch paper details
    papers = fetch_paper_details(pmids, batch_size=fetch_batch_size)

    # Save to database
    inserted = save_papers_to_db(papers)

    # Log the search
    with get_session() as session:
        log = SearchLog(
            disease_code=disease_code,
            source="pubmed",
            query_text=query,
            result_count=len(pmids),
        )
        session.add(log)

    summary = {
        "disease": disease_code,
        "disease_name": config.DISEASES[disease_code]["name"],
        "query": query,
        "found": len(pmids),
        "fetched": len(papers),
        "inserted": inserted,
    }
    logger.info(f"Search complete: {summary}")
    return summary


def search_all_diseases() -> List[Dict]:
    """Search PubMed for all configured diseases."""
    results = []
    for disease_code in config.DISEASES:
        result = search_and_save(disease_code)
        results.append(result)
    return results
