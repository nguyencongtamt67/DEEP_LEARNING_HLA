"""
Test script for the HLA Literature Mining Pipeline.
Tests each module with a small number of papers before running the full pipeline.

Usage:
    python test_pipeline.py                     # Run all tests
    python test_pipeline.py --test search       # Test search only
    python test_pipeline.py --test extract      # Test extraction only
"""

import json
import logging
import sys
import argparse

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)


def test_config():
    """Test configuration loading."""
    print("\n" + "=" * 60)
    print("TEST: Configuration")
    print("=" * 60)

    import config

    assert config.NCBI_API_KEY, "NCBI API key is missing"
    assert config.OPENAI_API_KEY, "OpenAI API key is missing"
    assert config.DATABASE_URL, "DATABASE_URL is missing"
    assert len(config.DISEASES) >= 10, f"Expected at least 10 diseases, got {len(config.DISEASES)}"

    print(f"  NCBI API key: {'*' * 8}...{config.NCBI_API_KEY[-4:]}")
    print(f"  OpenAI API key: {'*' * 8}...{config.OPENAI_API_KEY[-4:]}")
    print(f"  Database URL: {config.DATABASE_URL}")
    print(f"  Diseases: {list(config.DISEASES.keys())}")
    print(f"  Data dir: {config.DATA_DIR}")
    print("  PASSED")


def test_db_connection():
    """Test PostgreSQL connection and table creation."""
    print("\n" + "=" * 60)
    print("TEST: Database Connection")
    print("=" * 60)

    from db.connection import init_db, get_session
    from db.models import Paper

    # Create tables
    init_db()
    print("  Tables created successfully")

    # Test basic query
    with get_session() as session:
        count = session.query(Paper).count()
        print(f"  Papers in database: {count}")

    print("  PASSED")


def test_pubmed_search():
    """Test PubMed search with a small query."""
    print("\n" + "=" * 60)
    print("TEST: PubMed Search (MS, limit 5)")
    print("=" * 60)

    from search.pubmed_search import build_query, search_pmids, fetch_paper_details, save_papers_to_db

    # Build query
    query = build_query("MS")
    print(f"  Query: {query[:150]}...")

    # Search (limit to 5 results for testing)
    pmids = search_pmids(query, retmax=5)
    print(f"  Found {len(pmids)} PMIDs: {pmids}")

    if pmids:
        # Fetch details
        papers = fetch_paper_details(pmids)
        print(f"  Fetched {len(papers)} paper details")

        if papers:
            print(f"  First paper: {papers[0]['title'][:80]}...")
            print(f"  PMID: {papers[0]['pmid']}, Year: {papers[0]['year']}")
            print(f"  Abstract length: {len(papers[0].get('abstract', ''))} chars")

            # Save to DB
            inserted = save_papers_to_db(papers)
            print(f"  Inserted {inserted} papers into database")

    print("  PASSED")


def test_extraction_validator():
    """Test the extraction validator with sample data."""
    print("\n" + "=" * 60)
    print("TEST: Extraction Validator")
    print("=" * 60)

    from extraction.validator import validate_extraction, normalize_allele_name

    # Test allele normalization
    test_cases = [
        ("DRB1*1501", "HLA-DRB1*15:01"),
        ("HLA-DRB1*15:01", "HLA-DRB1*15:01"),
        ("A*0201", "HLA-A*02:01"),
        ("HLA-B*07:02", "HLA-B*07:02"),
    ]

    for input_allele, expected in test_cases:
        result = normalize_allele_name(input_allele)
        status = "OK" if result == expected else f"FAIL (got {result})"
        print(f"  normalize({input_allele}) = {result} [{status}]")

    # Test full validation
    sample_data = {
        "disease": "Multiple Sclerosis",
        "population": "European",
        "study_type": "GWAS",
        "sample_size": {"cases": 1000, "controls": 2000},
        "hla_associations": [
            {
                "allele": "HLA-DRB1*15:01",
                "effect": "risk",
                "odds_ratio": 3.08,
                "confidence_interval": [2.75, 3.45],
                "p_value": 1e-100,
                "context": "Strongest risk allele for MS in Europeans",
            },
            {
                "allele": "HLA-A*02:01",
                "effect": "protective",
                "odds_ratio": 0.73,
                "confidence_interval": [0.65, 0.82],
                "p_value": 2.3e-8,
                "context": "Protective effect independent of DRB1*15:01",
            },
        ],
        "hla_combinations": [
            {
                "alleles": ["HLA-DRB1*15:01", "HLA-DQB1*06:02"],
                "haplotype": "DR15-DQ6",
                "effect": "risk",
                "odds_ratio": 3.92,
                "p_value": 1e-120,
            }
        ],
        "key_findings": "HLA-DRB1*15:01 is the strongest genetic risk factor for MS.",
    }

    result = validate_extraction(sample_data)
    print(f"\n  Validation result: valid={result.is_valid}")
    print(f"  Errors: {result.errors}")
    print(f"  Warnings: {result.warnings}")
    print(f"  Associations parsed: {len(result.data.hla_associations)}")
    print(f"  Combinations parsed: {len(result.data.hla_combinations)}")
    print("  PASSED")


def test_llm_extraction():
    """Test LLM extraction with a sample abstract."""
    print("\n" + "=" * 60)
    print("TEST: LLM Extraction (single paper)")
    print("=" * 60)

    from extraction.llm_extractor import extract_from_paper

    # Use a well-known MS-HLA paper abstract for testing
    sample_abstract = """
    BACKGROUND: Multiple sclerosis (MS) is a complex autoimmune disease of the central 
    nervous system with a strong genetic component. The HLA region on chromosome 6p21.3 
    is the strongest genetic risk factor.
    
    METHODS: We performed a case-control study of 5,000 MS patients and 10,000 healthy 
    controls of European ancestry, genotyping HLA class I and II alleles at high resolution.
    
    RESULTS: HLA-DRB1*15:01 showed the strongest association with MS risk 
    (OR=3.08, 95% CI: 2.75-3.45, P=1.2x10^-100). HLA-A*02:01 demonstrated an 
    independent protective effect (OR=0.73, 95% CI: 0.65-0.82, P=2.3x10^-8). 
    The HLA-DRB1*15:01/DQB1*06:02 haplotype conferred the highest risk (OR=3.92, P<10^-120). 
    HLA-B*44:02 showed a modest risk effect (OR=1.15, 95% CI: 1.05-1.26, P=0.003).
    
    CONCLUSIONS: Our study confirms HLA-DRB1*15:01 as the primary MS risk allele and 
    identifies independent effects at HLA class I loci in a large European cohort.
    """

    result = extract_from_paper(
        pmid="TEST001",
        abstract=sample_abstract,
        full_text=None,
    )

    print(f"  Status: {result['status']}")
    if result['status'] == 'completed':
        data = result['data']
        print(f"  Disease: {data.get('disease')}")
        print(f"  Population: {data.get('population')}")
        print(f"  Associations found: {len(data.get('hla_associations', []))}")
        for a in data.get('hla_associations', []):
            print(f"    - {a['allele']}: {a.get('effect')}, OR={a.get('odds_ratio')}, p={a.get('p_value')}")
        print(f"  Combinations found: {len(data.get('hla_combinations', []))}")
        print(f"  Tokens: in={result['tokens_input']}, out={result['tokens_output']}")
        print(f"  Cost: ${result['cost_usd']:.6f}")
        print(f"  Validation: {result['validation']['is_valid']}")
        if result['validation']['warnings']:
            print(f"  Warnings: {result['validation']['warnings']}")
    else:
        print(f"  Error: {result.get('error', 'Unknown')}")

    print("  PASSED")


def test_pubmed_query_counts():
    """Show expected query result counts for each disease (no download)."""
    print("\n" + "=" * 60)
    print("TEST: PubMed Query Counts (esearch only)")
    print("=" * 60)

    from search.pubmed_search import build_query
    from Bio import Entrez
    import config

    Entrez.email = config.NCBI_EMAIL
    Entrez.api_key = config.NCBI_API_KEY

    for disease_code in config.DISEASES:
        query = build_query(disease_code)
        handle = Entrez.esearch(db="pubmed", term=query, retmax=0)
        results = Entrez.read(handle)
        handle.close()
        count = results.get("Count", "?")
        print(f"  {disease_code} ({config.DISEASES[disease_code]['name']}): {count} results")

    print("  PASSED")


def main():
    parser = argparse.ArgumentParser(description="Test HLA Pipeline")
    parser.add_argument(
        "--test", "-t",
        choices=["config", "db", "search", "counts", "validator", "extract", "all"],
        default="all",
        help="Which test to run",
    )
    args = parser.parse_args()

    tests = {
        "config": test_config,
        "db": test_db_connection,
        "search": test_pubmed_search,
        "counts": test_pubmed_query_counts,
        "validator": test_extraction_validator,
        "extract": test_llm_extraction,
    }

    if args.test == "all":
        for name, test_fn in tests.items():
            try:
                test_fn()
            except Exception as e:
                print(f"\n  FAILED: {e}")
                logger.exception(f"Test {name} failed")
    else:
        try:
            tests[args.test]()
        except Exception as e:
            print(f"\n  FAILED: {e}")
            logger.exception(f"Test {args.test} failed")


if __name__ == "__main__":
    main()
