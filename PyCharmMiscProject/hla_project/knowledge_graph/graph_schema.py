"""
Neo4j knowledge graph schema definition.
Defines node types, relationship types, constraints, and indexes.
"""

import logging

from knowledge_graph.neo4j_connection import get_neo4j_session

logger = logging.getLogger(__name__)

# ============================================================
# Node labels and their properties
# ============================================================
NODE_SCHEMAS = {
    "Disease": {
        "properties": ["name", "code", "category"],
        "unique_key": "name",
    },
    "HLAAllele": {
        "properties": ["name", "gene", "allele_group", "protein", "resolution"],
        "unique_key": "name",
    },
    "Haplotype": {
        "properties": ["name", "alleles"],
        "unique_key": "name",
    },
    "Population": {
        "properties": ["name", "region", "continent"],
        "unique_key": "name",
    },
    "Paper": {
        "properties": ["pmid", "doi", "title", "year", "journal", "authors"],
        "unique_key": "pmid",
    },
}

# ============================================================
# Relationship types and their properties
# ============================================================
RELATIONSHIP_SCHEMAS = {
    "ASSOCIATED_WITH": {
        "from": "HLAAllele",
        "to": "Disease",
        "properties": [
            "effect",
            "odds_ratio",
            "ci_lower",
            "ci_upper",
            "p_value",
            "sample_cases",
            "sample_controls",
            "context",
            "paper_pmid",
            "population",
        ],
    },
    "HAPLOTYPE_ASSOCIATED_WITH": {
        "from": "Haplotype",
        "to": "Disease",
        "properties": [
            "effect",
            "odds_ratio",
            "ci_lower",
            "ci_upper",
            "p_value",
            "context",
            "paper_pmid",
        ],
    },
    "PART_OF": {
        "from": "HLAAllele",
        "to": "Haplotype",
        "properties": [],
    },
    "REPORTS": {
        "from": "Paper",
        "to": "HLAAllele",
        "properties": ["extraction_source"],
    },
    "STUDIES": {
        "from": "Paper",
        "to": "Disease",
        "properties": ["study_type"],
    },
    "STUDIES_POPULATION": {
        "from": "Paper",
        "to": "Population",
        "properties": [],
    },
    "OBSERVED_IN": {
        "from": "HLAAllele",
        "to": "Population",
        "properties": ["frequency", "paper_pmid"],
    },
}


def create_constraints_and_indexes():
    """Create uniqueness constraints and indexes in Neo4j."""
    with get_neo4j_session() as session:
        # Uniqueness constraints (also create indexes)
        constraints = [
            "CREATE CONSTRAINT disease_name IF NOT EXISTS FOR (d:Disease) REQUIRE d.name IS UNIQUE",
            "CREATE CONSTRAINT allele_name IF NOT EXISTS FOR (a:HLAAllele) REQUIRE a.name IS UNIQUE",
            "CREATE CONSTRAINT haplotype_name IF NOT EXISTS FOR (h:Haplotype) REQUIRE h.name IS UNIQUE",
            "CREATE CONSTRAINT population_name IF NOT EXISTS FOR (p:Population) REQUIRE p.name IS UNIQUE",
            "CREATE CONSTRAINT paper_pmid IF NOT EXISTS FOR (p:Paper) REQUIRE p.pmid IS UNIQUE",
        ]

        for constraint in constraints:
            try:
                session.run(constraint)
                logger.info(f"Created constraint: {constraint[:60]}...")
            except Exception as e:
                logger.debug(f"Constraint may already exist: {e}")

        # Additional indexes for search performance
        indexes = [
            "CREATE INDEX allele_gene IF NOT EXISTS FOR (a:HLAAllele) ON (a.gene)",
            "CREATE INDEX paper_year IF NOT EXISTS FOR (p:Paper) ON (p.year)",
            "CREATE INDEX paper_doi IF NOT EXISTS FOR (p:Paper) ON (p.doi)",
        ]

        for index in indexes:
            try:
                session.run(index)
                logger.info(f"Created index: {index[:60]}...")
            except Exception as e:
                logger.debug(f"Index may already exist: {e}")

    logger.info("Neo4j constraints and indexes created")


def parse_allele_info(allele_name: str) -> dict:
    """
    Parse an HLA allele name into its components.
    E.g., HLA-DRB1*15:01 -> {gene: DRB1, allele_group: 15, protein: 01, resolution: 2-field}
    """
    import re

    info = {"name": allele_name, "gene": None, "allele_group": None, "protein": None, "resolution": None}

    match = re.match(r"HLA-([A-Za-z0-9]+)\*(\d+):(\d+)(?::(\d+))?(?::(\d+))?", allele_name)
    if match:
        info["gene"] = match.group(1)
        info["allele_group"] = match.group(2)
        info["protein"] = match.group(3)

        fields = 2
        if match.group(4):
            fields = 3
        if match.group(5):
            fields = 4
        info["resolution"] = f"{fields}-field"
    else:
        # Try simpler pattern
        match = re.match(r"HLA-([A-Za-z0-9]+)", allele_name)
        if match:
            info["gene"] = match.group(1)
            info["resolution"] = "serological"

    return info
