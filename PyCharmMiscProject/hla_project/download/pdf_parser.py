"""
PDF download and parsing module.
Downloads PDFs from PMC OA and publisher links, then extracts text and tables.
"""

import logging
import time
from pathlib import Path
from typing import Optional, Dict, List

import requests
import fitz  # PyMuPDF
import pdfplumber
from tqdm import tqdm

import config
from db.connection import get_session
from db.models import Paper

logger = logging.getLogger(__name__)

PMC_OA_API = "https://www.ncbi.nlm.nih.gov/pmc/utils/oa/oa.fcgi"


def get_pmc_pdf_url(pmc_id: str) -> Optional[str]:
    """
    Get PDF download URL for a PMC article from the OA service.
    Returns the PDF URL or None if not available in OA.
    """
    try:
        resp = requests.get(
            PMC_OA_API,
            params={"id": pmc_id},
            timeout=30,
        )
        resp.raise_for_status()

        # Parse XML response
        from xml.etree import ElementTree
        root = ElementTree.fromstring(resp.content)

        # Look for PDF link
        for link in root.findall(".//link"):
            fmt = link.get("format", "")
            if fmt.lower() == "pdf":
                href = link.get("href", "")
                if href:
                    # Convert FTP to HTTPS if needed
                    if href.startswith("ftp://"):
                        href = href.replace("ftp://", "https://")
                    return href

    except Exception as e:
        logger.debug(f"Could not get PDF URL for {pmc_id}: {e}")

    return None


def download_pdf(url: str, save_path: Path) -> bool:
    """
    Download a PDF from a URL to the specified path.
    Returns True if successful.
    """
    try:
        resp = requests.get(
            url,
            timeout=60,
            headers={"User-Agent": "HLA-Pipeline/1.0 (research tool)"},
            stream=True,
        )
        resp.raise_for_status()

        # Check content type
        content_type = resp.headers.get("Content-Type", "")
        if "pdf" not in content_type.lower() and "octet-stream" not in content_type.lower():
            logger.warning(f"Unexpected content type for {url}: {content_type}")

        save_path.parent.mkdir(parents=True, exist_ok=True)
        with open(save_path, "wb") as f:
            for chunk in resp.iter_content(chunk_size=8192):
                f.write(chunk)

        # Verify file is valid PDF
        if save_path.stat().st_size < 1000:
            logger.warning(f"Downloaded file is suspiciously small: {save_path}")
            save_path.unlink(missing_ok=True)
            return False

        return True

    except Exception as e:
        logger.error(f"Error downloading PDF from {url}: {e}")
        return False


def extract_text_from_pdf(pdf_path: Path) -> Optional[str]:
    """
    Extract text from a PDF using PyMuPDF (fitz).
    Returns the extracted text or None on failure.
    """
    try:
        doc = fitz.open(str(pdf_path))
        text_parts = []

        for page_num in range(len(doc)):
            page = doc[page_num]
            text = page.get_text("text")
            if text.strip():
                text_parts.append(text.strip())

        doc.close()

        if text_parts:
            return "\n\n".join(text_parts)

    except Exception as e:
        logger.error(f"Error extracting text from {pdf_path}: {e}")

    return None


def extract_tables_from_pdf(pdf_path: Path) -> List[Dict]:
    """
    Extract tables from a PDF using pdfplumber.
    Returns a list of tables, each as a dict with 'page' and 'data' keys.
    """
    tables = []

    try:
        with pdfplumber.open(str(pdf_path)) as pdf:
            for page_num, page in enumerate(pdf.pages):
                page_tables = page.extract_tables()
                for table_idx, table in enumerate(page_tables):
                    if table and len(table) > 1:
                        tables.append({
                            "page": page_num + 1,
                            "table_index": table_idx,
                            "data": table,
                        })

    except Exception as e:
        logger.error(f"Error extracting tables from {pdf_path}: {e}")

    return tables


def tables_to_text(tables: List[Dict]) -> str:
    """Convert extracted tables to a readable text format."""
    text_parts = []

    for table_info in tables:
        page = table_info["page"]
        data = table_info["data"]

        text_parts.append(f"\n[Table from page {page}]")

        for row in data:
            # Clean None values
            cleaned = [str(cell).strip() if cell else "" for cell in row]
            text_parts.append(" | ".join(cleaned))

    return "\n".join(text_parts)


def process_paper_pdf(paper_id: int, pmc_id: str) -> Optional[str]:
    """
    Full PDF processing pipeline for a single paper:
    1. Get PDF URL from PMC OA
    2. Download PDF
    3. Extract text
    4. Extract tables
    5. Combine into full text

    Returns the extracted text or None.
    """
    # Get download URL
    pdf_url = get_pmc_pdf_url(pmc_id)
    if not pdf_url:
        logger.debug(f"No PDF available for {pmc_id}")
        return None

    # Download PDF
    pdf_filename = f"{pmc_id.replace('PMC', '')}.pdf"
    pdf_path = config.PDF_DIR / pdf_filename

    if not pdf_path.exists():
        success = download_pdf(pdf_url, pdf_path)
        if not success:
            return None

    # Extract text
    text = extract_text_from_pdf(pdf_path)

    # Extract tables and append
    tables = extract_tables_from_pdf(pdf_path)
    if tables:
        table_text = tables_to_text(tables)
        if text:
            text += "\n\n" + table_text
        else:
            text = table_text

    return text


def fetch_all_pdfs() -> int:
    """
    Download and process PDFs for all papers with PMC IDs that don't have full text.
    Returns the number of successfully processed papers.
    """
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
            logger.info("No papers need PDF processing")
            return 0

        logger.info(f"Processing PDFs for {len(papers)} papers")
        processed = 0

        for paper in tqdm(papers, desc="Processing PDFs"):
            text = process_paper_pdf(paper.id, paper.pmc_id)
            if text:
                paper.full_text = text
                paper.has_full_text = True
                processed += 1

            time.sleep(0.5)  # Rate limiting

        session.flush()

    logger.info(f"Successfully processed {processed}/{len(papers)} PDFs")
    return processed
