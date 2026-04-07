"""
Central configuration management for the HLA Literature Mining Pipeline.
Loads settings from .env and provides typed access to all configuration values.
"""

import os
from pathlib import Path
from dotenv import load_dotenv

# Load environment variables from .env
BASE_DIR = Path(__file__).resolve().parent
load_dotenv(BASE_DIR / ".env")


# --- NCBI / PubMed ---
NCBI_API_KEY = os.getenv("NICB_API_KEY", "")
NCBI_EMAIL = os.getenv("NCBI_EMAIL", "hla-pipeline@research.local")
NCBI_TOOL = "hla_literature_mining"
PUBMED_BATCH_SIZE = 200
PUBMED_RATE_LIMIT = 10  # requests per second with API key

# --- OpenAI ---
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "")
OPENAI_MODEL = os.getenv("OPENAI_MODEL", "gpt-4o")
OPENAI_MAX_TOKENS = 4096
OPENAI_TEMPERATURE = 0.0
OPENAI_CONCURRENT_REQUESTS = 5
OPENAI_RATE_LIMIT_RPM = 500

# --- PostgreSQL ---
DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgresql://postgres:postgres@localhost:5432/postgres"
)
DATABASE_SCHEMA = os.getenv("DATABASE_SCHEMA", "hla-research-db")

# --- Neo4j ---
NEO4J_URI = os.getenv("NEO4J_URI", "bolt://localhost:7687")
NEO4J_USER = os.getenv("NEO4J_USER", "neo4j")
NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD", "neo4j")

# --- Network / TLS ---
REQUESTS_CA_BUNDLE = os.getenv("REQUESTS_CA_BUNDLE", "")
SSL_CERT_FILE = os.getenv("SSL_CERT_FILE", "")
ALLOW_INSECURE_SSL = os.getenv("ALLOW_INSECURE_SSL", "false").lower() in {
    "1",
    "true",
    "yes",
    "on",
}

# --- Search queries ---
DISEASES = {
    "MS": {
        "name": "Multiple Sclerosis",
        "query_terms": ["Multiple Sclerosis"],
    },
    "NMOSD": {
        "name": "Neuromyelitis Optica Spectrum Disorder",
        "query_terms": ["Neuromyelitis Optica", "NMOSD", "Devic disease"],
    },
    "MG": {
        "name": "Myasthenia Gravis",
        "query_terms": ["Myasthenia Gravis"],
    },
    "GBS": {
        "name": "Guillain-Barre Syndrome",
        "query_terms": [
            "Guillain-Barre Syndrome",
            "Guillain Barre",
            "GBS neuropathy",
        ],
    },
    "TM": {
        "name": "Transverse Myelitis",
        "query_terms": ["Transverse Myelitis", "TM"],
    },
    "ON": {
        "name": "Optic Neuritis",
        "query_terms": ["Optic Neuritis", "ON"],
    },
    "AIE": {
        "name": "Autoimmune Encephalitis",
        "query_terms": ["Autoimmune Encephalitis", "AE", "AIE"],
    },
    "CIDP": {
        "name": "Chronic Inflammatory Demyelinating Polyneuropathy",
        "query_terms": [
            "Chronic Inflammatory Demyelinating Polyneuropathy",
            "CIDP",
        ],
    },
    "ADEM": {
        "name": "Acute Disseminated Encephalomyelitis",
        "query_terms": ["Acute Disseminated Encephalomyelitis", "ADEM"],
    },
    "MOGAD": {
        "name": "MOG Antibody Disease",
        "query_terms": [
            "MOG Antibody Disease",
            "MOG-IgG-associated disease",
            "MOGAD",
            "MOG-IgG",
        ],
    },
}

HLA_TERMS = [
    "HLA",
    "MHC",
    '"Human Leukocyte Antigen"',
    '"Major Histocompatibility Complex"',
]

ASSOCIATION_TERMS = [
    "association",
    "risk",
    "susceptibility",
    "genetic",
    "polymorphism",
]

SEARCH_DATE_RANGE = ("2000/01/01", "2025/12/31")
SEARCH_LANGUAGES = ["English"]

# --- File paths ---
DATA_DIR = BASE_DIR / "data"
PDF_DIR = DATA_DIR / "pdfs"
PDF_DIR.mkdir(parents=True, exist_ok=True)

# --- Batch processing ---
EXTRACTION_BATCH_SIZE = 100
PUBMED_FETCH_BATCH_SIZE = 25  # Further reduced for stability
PUBMED_RATE_LIMIT = 3  # Reduced from 10 to be more conservative
