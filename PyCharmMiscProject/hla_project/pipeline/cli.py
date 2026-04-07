"""
CLI interface for the HLA Literature Mining Pipeline.
Provides commands for each step of the pipeline.
"""

import json
import logging
import sys

import click

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

# Add project root to path
sys.path.insert(0, ".")


@click.group()
@click.option("--verbose", "-v", is_flag=True, help="Enable debug logging")
def cli(verbose):
    """HLA Literature Mining Pipeline - Extract HLA-disease associations from published literature."""
    if verbose:
        logging.getLogger().setLevel(logging.DEBUG)


# ============================================================
# Search commands
# ============================================================
@cli.command()
@click.option(
    "--disease", "-d",
    type=click.Choice(["MS", "NMOSD", "MG", "GBS", "all"], case_sensitive=False),
    default="all",
    help="Disease to search for",
)
@click.option(
    "--source", "-s",
    type=click.Choice(["pubmed", "biorxiv", "all"], case_sensitive=False),
    default="pubmed",
    help="Search source",
)
def search(disease, source):
    """Search PubMed/bioRxiv for HLA-disease association papers."""
    from pipeline.orchestrator import run_search

    click.echo(f"Searching for {disease} on {source}...")
    results = run_search(disease=disease, source=source)

    click.echo("\n--- Search Results ---")
    for r in results:
        click.echo(
            f"  {r.get('disease', '?')} ({r.get('source', r.get('disease_name', '?'))}): "
            f"found={r.get('found', 0)}, inserted={r.get('inserted', 0)}"
        )

    total_found = sum(r.get("found", 0) for r in results)
    total_inserted = sum(r.get("inserted", 0) for r in results)
    click.echo(f"\nTotal: {total_found} found, {total_inserted} new papers inserted")


# ============================================================
# Download commands
# ============================================================
@cli.command()
@click.option("--batch-size", "-b", default=200, help="Batch size for downloads")
@click.option("--abstracts-only", is_flag=True, help="Only fetch abstracts, skip full-text")
@click.option("--no-pdf", is_flag=True, help="Skip PDF download and parsing")
def download(batch_size, abstracts_only, no_pdf):
    """Download abstracts, full-text, and PDFs for papers in the database."""
    from pipeline.orchestrator import run_download

    click.echo(f"Downloading content (batch_size={batch_size})...")
    results = run_download(
        batch_size=batch_size,
        include_fulltext=not abstracts_only,
        include_pdf=not no_pdf and not abstracts_only,
    )

    click.echo("\n--- Download Results ---")
    click.echo(f"  Abstracts fetched: {results.get('abstracts_fetched', 0)}")
    if "fulltext_fetched" in results:
        click.echo(f"  Full-text fetched: {results['fulltext_fetched']}")
    if "pdfs_processed" in results:
        click.echo(f"  PDFs processed: {results['pdfs_processed']}")

    if "abstract_stats" in results:
        stats = results["abstract_stats"]
        click.echo(f"\n  Abstract coverage: {stats.get('coverage', '?')}")
    if "fulltext_stats" in results:
        stats = results["fulltext_stats"]
        click.echo(f"  Full-text coverage: {stats.get('fulltext_coverage', '?')}")


# ============================================================
# Extraction commands
# ============================================================
@cli.command()
@click.option("--batch-size", "-b", default=100, help="Number of papers per batch")
@click.option("--model", "-m", default=None, help="OpenAI model (default: gpt-4o)")
@click.option(
    "--disease", "-d",
    type=click.Choice(["MS", "NMOSD", "MG", "GBS"], case_sensitive=False),
    default=None,
    help="Filter by disease",
)
@click.option("--dry-run", is_flag=True, help="Show what would be processed without calling API")
def extract(batch_size, model, disease, dry_run):
    """Extract HLA associations from papers using LLM."""
    from pipeline.orchestrator import run_extraction

    if dry_run:
        click.echo("DRY RUN - no API calls will be made")

    click.echo(f"Extracting associations (batch={batch_size}, model={model or 'gpt-4o'})...")
    results = run_extraction(
        batch_size=batch_size,
        model=model,
        disease_filter=disease,
        dry_run=dry_run,
    )

    click.echo("\n--- Extraction Results ---")
    click.echo(f"  Processed: {results.get('processed', 0)}")
    click.echo(f"  Completed: {results.get('completed', 0)}")
    click.echo(f"  Failed: {results.get('failed', 0)}")
    click.echo(f"  Skipped: {results.get('skipped', 0)}")
    click.echo(f"  Total cost: ${results.get('total_cost', 0):.4f}")

    if "overall_stats" in results:
        stats = results["overall_stats"]
        click.echo(f"\n--- Overall Stats ---")
        click.echo(f"  Total papers: {stats.get('total_papers', 0)}")
        click.echo(f"  Extracted: {stats.get('completed', 0)}")
        click.echo(f"  Pending: {stats.get('pending', 0)}")
        click.echo(f"  Total associations: {stats.get('total_associations', 0)}")
        click.echo(f"  Total cost: ${stats.get('total_cost_usd', 0):.4f}")


# ============================================================
# Validation command
# ============================================================
@cli.command()
def validate():
    """Validate extracted HLA association data."""
    from pipeline.orchestrator import run_validation

    click.echo("Validating extracted data...")
    results = run_validation()

    click.echo("\n--- Validation Results ---")
    click.echo(f"  Total associations: {results.get('total_associations', 0)}")
    click.echo(f"  Total combinations: {results.get('total_combinations', 0)}")

    issues = results.get("issues", {})
    click.echo("\n  Issues found:")
    for issue, count in issues.items():
        status = "OK" if count == 0 else f"WARNING ({count})"
        click.echo(f"    {issue}: {status}")


# ============================================================
# Knowledge graph command
# ============================================================
@cli.command("build-graph")
@click.option("--clear", is_flag=True, help="Clear existing graph before building")
def build_graph(clear):
    """Build the Neo4j knowledge graph from extracted data."""
    from pipeline.orchestrator import run_build_graph

    if clear:
        click.confirm("This will delete all existing data in Neo4j. Continue?", abort=True)

    click.echo("Building knowledge graph...")
    stats = run_build_graph(clear_existing=clear)

    click.echo("\n--- Knowledge Graph Stats ---")
    for key, value in stats.items():
        click.echo(f"  {key}: {value}")


# ============================================================
# Full pipeline command
# ============================================================
@cli.command("run-all")
@click.option(
    "--disease", "-d",
    type=click.Choice(["MS", "NMOSD", "MG", "GBS", "all"], case_sensitive=False),
    default="all",
    help="Disease(s) to process",
)
@click.option(
    "--source", "-s",
    type=click.Choice(["pubmed", "biorxiv", "all"], case_sensitive=False),
    default="all",
    help="Search source(s)",
)
@click.option("--batch-size", "-b", default=100, help="Extraction batch size")
@click.option("--fetch-batch-size", default=None, help="PubMed fetch batch size (default: 50)")
@click.option("--model", "-m", default=None, help="OpenAI model")
@click.option("--skip-search", is_flag=True, help="Skip search step")
@click.option("--skip-download", is_flag=True, help="Skip download step")
@click.option("--skip-extraction", is_flag=True, help="Skip extraction step")
@click.option("--skip-graph", is_flag=True, help="Skip graph building step")
def run_all(disease, source, batch_size, fetch_batch_size, model, skip_search, skip_download, skip_extraction, skip_graph):
    """Run the complete pipeline end-to-end."""
    from pipeline.orchestrator import run_full_pipeline

    click.echo("=" * 60)
    click.echo("HLA LITERATURE MINING PIPELINE")
    click.echo("=" * 60)

    results = run_full_pipeline(
        disease=disease,
        source=source,
        extraction_batch_size=batch_size,
        fetch_batch_size=fetch_batch_size,
        model=model,
        skip_search=skip_search,
        skip_download=skip_download,
        skip_extraction=skip_extraction,
        skip_graph=skip_graph,
    )

    click.echo(f"\nPipeline completed in {results.get('elapsed_seconds', 0)} seconds")
    click.echo(json.dumps(results, indent=2, default=str))


# ============================================================
# Stats command
# ============================================================
@cli.command()
def stats():
    """Show pipeline statistics."""
    from download.abstract_fetcher import get_abstract_stats
    from download.pmc_fulltext import get_fulltext_stats
    from extraction.batch_processor import get_extraction_stats

    click.echo("\n=== Pipeline Statistics ===\n")

    click.echo("--- Abstracts ---")
    abs_stats = get_abstract_stats()
    for k, v in abs_stats.items():
        click.echo(f"  {k}: {v}")

    click.echo("\n--- Full-text ---")
    ft_stats = get_fulltext_stats()
    for k, v in ft_stats.items():
        click.echo(f"  {k}: {v}")

    click.echo("\n--- Extraction ---")
    ext_stats = get_extraction_stats()
    for k, v in ext_stats.items():
        click.echo(f"  {k}: {v}")

    try:
        from knowledge_graph.graph_builder import get_graph_stats
        click.echo("\n--- Knowledge Graph ---")
        graph_stats = get_graph_stats()
        for k, v in graph_stats.items():
            click.echo(f"  {k}: {v}")
    except Exception:
        click.echo("\n--- Knowledge Graph ---")
        click.echo("  (Neo4j not available)")


# ============================================================
# Init DB command
# ============================================================
@cli.command("init-db")
def init_db():
    """Initialize the PostgreSQL database (create tables)."""
    from db.connection import init_db as _init_db

    click.echo("Initializing database...")
    _init_db()
    click.echo("Database initialized successfully!")


if __name__ == "__main__":
    cli()
