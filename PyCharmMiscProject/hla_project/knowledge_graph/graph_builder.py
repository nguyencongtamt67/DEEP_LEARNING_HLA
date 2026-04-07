"""
Knowledge graph builder.
Reads extracted HLA-disease associations from PostgreSQL and builds the Neo4j graph.
"""

import logging
from typing import List, Dict, Optional

from tqdm import tqdm

import config
from db.connection import get_session
from db.models import Paper, HLAAssociation, HLACombination
from knowledge_graph.neo4j_connection import get_neo4j_session, run_query
from knowledge_graph.graph_schema import create_constraints_and_indexes, parse_allele_info

logger = logging.getLogger(__name__)

# Population region mapping
POPULATION_REGIONS = {
    "European": "Europe",
    "Caucasian": "Europe",
    "Asian": "Asia",
    "Japanese": "Asia",
    "Chinese": "Asia",
    "Han Chinese": "Asia",
    "Korean": "Asia",
    "Vietnamese": "Asia",
    "Thai": "Asia",
    "Indian": "Asia",
    "South Asian": "Asia",
    "African": "Africa",
    "African American": "North America",
    "Hispanic": "Americas",
    "Latino": "Americas",
    "Brazilian": "South America",
    "Middle Eastern": "Middle East",
    "Turkish": "Middle East",
    "Iranian": "Middle East",
    "Australian": "Oceania",
    "Mixed": "Global",
    "Multi-ethnic": "Global",
}


def build_graph(clear_existing: bool = False):
    """
    Main function to build the complete knowledge graph.

    Args:
        clear_existing: If True, clear the Neo4j database before building
    """
    if clear_existing:
        from knowledge_graph.neo4j_connection import clear_database
        clear_database()
        logger.info("Cleared existing graph")

    # Create schema
    create_constraints_and_indexes()

    # Load data from PostgreSQL
    logger.info("Loading data from PostgreSQL...")

    with get_session() as session:
        papers = session.query(Paper).all()
        associations = session.query(HLAAssociation).all()
        combinations = session.query(HLACombination).all()

        # Detach from session
        paper_data = [_paper_to_dict(p) for p in papers]
        assoc_data = [_assoc_to_dict(a) for a in associations]
        combo_data = [_combo_to_dict(c) for c in combinations]

    logger.info(
        f"Loaded {len(paper_data)} papers, {len(assoc_data)} associations, "
        f"{len(combo_data)} combinations"
    )

    # Build nodes
    _create_disease_nodes()
    _create_paper_nodes(paper_data)
    _create_allele_nodes(assoc_data, combo_data)
    _create_population_nodes(assoc_data)
    _create_haplotype_nodes(combo_data)

    # Build relationships
    _create_association_relationships(assoc_data, paper_data)
    _create_co_occurs_relationships(assoc_data)
    _create_frequent_in_relationships(assoc_data)
    _create_combination_relationships(combo_data, paper_data)
    _create_paper_relationships(paper_data, assoc_data, combo_data)

    # Get stats
    stats = get_graph_stats()
    logger.info(f"Knowledge graph built: {stats}")
    return stats


def _paper_to_dict(paper: Paper) -> Dict:
    return {
        "id": paper.id,
        "pmid": paper.pmid,
        "doi": paper.doi,
        "title": paper.title,
        "year": paper.year,
        "journal": paper.journal,
        "authors": paper.authors,
    }


def _assoc_to_dict(assoc: HLAAssociation) -> Dict:
    return {
        "paper_id": assoc.paper_id,
        "disease": assoc.disease,
        "population": assoc.population,
        "allele": assoc.allele,
        "effect": assoc.effect,
        "odds_ratio": assoc.odds_ratio,
        "ci_lower": assoc.ci_lower,
        "ci_upper": assoc.ci_upper,
        "p_value": assoc.p_value,
        "sample_cases": assoc.sample_cases,
        "sample_controls": assoc.sample_controls,
        "context": assoc.context,
    }


def _combo_to_dict(combo: HLACombination) -> Dict:
    return {
        "paper_id": combo.paper_id,
        "disease": combo.disease,
        "population": combo.population,
        "alleles": combo.alleles,
        "haplotype_name": combo.haplotype_name,
        "effect": combo.effect,
        "odds_ratio": combo.odds_ratio,
        "ci_lower": combo.ci_lower,
        "ci_upper": combo.ci_upper,
        "p_value": combo.p_value,
        "context": combo.context,
    }


def _create_disease_nodes():
    """Create Disease nodes for each configured disease."""
    logger.info("Creating Disease nodes...")

    with get_neo4j_session() as session:
        for code, info in config.DISEASES.items():
            session.run(
                """
                MERGE (d:Disease {name: $name})
                SET d.code = $code, d.category = 'Autoimmune Neurological'
                """,
                {"name": info["name"], "code": code},
            )

    logger.info(f"Created {len(config.DISEASES)} Disease nodes")


def _create_paper_nodes(papers: List[Dict]):
    """Create Paper nodes."""
    logger.info(f"Creating {len(papers)} Paper nodes...")

    with get_neo4j_session() as session:
        for paper in tqdm(papers, desc="Creating Paper nodes"):
            if not paper.get("pmid"):
                continue
            session.run(
                """
                MERGE (p:Paper {pmid: $pmid})
                SET p.doi = $doi, p.title = $title, p.year = $year,
                    p.journal = $journal, p.authors = $authors
                """,
                {
                    "pmid": paper["pmid"],
                    "doi": paper.get("doi"),
                    "title": paper.get("title", ""),
                    "year": paper.get("year"),
                    "journal": paper.get("journal"),
                    "authors": paper.get("authors"),
                },
            )


def _create_allele_nodes(associations: List[Dict], combinations: List[Dict]):
    """Create HLAAllele nodes from all extracted data."""
    # Collect unique alleles
    alleles = set()
    for a in associations:
        if a.get("allele"):
            alleles.add(a["allele"])
    for c in combinations:
        for allele in (c.get("alleles") or []):
            alleles.add(allele)

    logger.info(f"Creating {len(alleles)} HLAAllele nodes...")

    with get_neo4j_session() as session:
        for allele_name in tqdm(alleles, desc="Creating HLAAllele nodes"):
            info = parse_allele_info(allele_name)
            session.run(
                """
                MERGE (a:HLAAllele {name: $name})
                SET a.gene = $gene, a.allele_group = $allele_group,
                    a.protein = $protein, a.resolution = $resolution
                """,
                {
                    "name": allele_name,
                    "gene": info.get("gene"),
                    "allele_group": info.get("allele_group"),
                    "protein": info.get("protein"),
                    "resolution": info.get("resolution"),
                },
            )


def _create_population_nodes(associations: List[Dict]):
    """Create Population nodes."""
    populations = set()
    for a in associations:
        pop = a.get("population")
        if pop:
            populations.add(pop)

    logger.info(f"Creating {len(populations)} Population nodes...")

    with get_neo4j_session() as session:
        for pop_name in populations:
            region = POPULATION_REGIONS.get(pop_name, "Unknown")
            session.run(
                """
                MERGE (p:Population {name: $name})
                SET p.region = $region
                """,
                {"name": pop_name, "region": region},
            )


def _create_haplotype_nodes(combinations: List[Dict]):
    """Create Haplotype nodes and PART_OF relationships to alleles."""
    haplotypes = {}
    for c in combinations:
        name = c.get("haplotype_name")
        alleles = c.get("alleles", [])
        if name and alleles:
            if name not in haplotypes:
                haplotypes[name] = set()
            for a in alleles:
                haplotypes[name].add(a)

    logger.info(f"Creating {len(haplotypes)} Haplotype nodes...")

    with get_neo4j_session() as session:
        for hap_name, allele_set in haplotypes.items():
            alleles_list = sorted(allele_set)
            session.run(
                """
                MERGE (h:Haplotype {name: $name})
                SET h.alleles = $alleles
                """,
                {"name": hap_name, "alleles": alleles_list},
            )

            # Create PART_OF relationships
            for allele_name in alleles_list:
                session.run(
                    """
                    MATCH (a:HLAAllele {name: $allele})
                    MATCH (h:Haplotype {name: $haplotype})
                    MERGE (a)-[:PART_OF]->(h)
                    """,
                    {"allele": allele_name, "haplotype": hap_name},
                )


def _create_association_relationships(associations: List[Dict], papers: List[Dict]):
    """Create ASSOCIATED_WITH relationships between alleles and diseases."""
    logger.info(f"Creating {len(associations)} ASSOCIATED_WITH relationships...")

    # Build paper_id -> pmid lookup
    paper_id_to_pmid = {p["id"]: p.get("pmid") for p in papers}

    with get_neo4j_session() as session:
        for assoc in tqdm(associations, desc="Creating associations"):
            allele = assoc.get("allele")
            disease = assoc.get("disease")
            if not allele or not disease:
                continue

            pmid = paper_id_to_pmid.get(assoc["paper_id"])

            # Normalize disease name to match our node names
            disease_node = _normalize_disease_name(disease)

            session.run(
                """
                MATCH (a:HLAAllele {name: $allele})
                MATCH (d:Disease {name: $disease})
                CREATE (a)-[r:ASSOCIATED_WITH {
                    effect: $effect,
                    odds_ratio: $odds_ratio,
                    ci_lower: $ci_lower,
                    ci_upper: $ci_upper,
                    p_value: $p_value,
                    sample_cases: $sample_cases,
                    sample_controls: $sample_controls,
                    context: $context,
                    paper_pmid: $pmid,
                    population: $population
                }]->(d)
                """,
                {
                    "allele": allele,
                    "disease": disease_node,
                    "effect": assoc.get("effect"),
                    "odds_ratio": assoc.get("odds_ratio"),
                    "ci_lower": assoc.get("ci_lower"),
                    "ci_upper": assoc.get("ci_upper"),
                    "p_value": assoc.get("p_value"),
                    "sample_cases": assoc.get("sample_cases"),
                    "sample_controls": assoc.get("sample_controls"),
                    "context": assoc.get("context"),
                    "pmid": pmid,
                    "population": assoc.get("population"),
                },
            )


def _create_co_occurs_relationships(associations: List[Dict]):
    """Create CO_OCCURS_WITH relationships between alleles in the same paper."""
    logger.info("Creating CO_OCCURS_WITH relationships...")

    # Count co-occurrences by paper
    cooccurrence_counts = {}
    paper_alleles = {}
    for assoc in associations:
        paper_id = assoc.get("paper_id")
        allele = assoc.get("allele")
        if not paper_id or not allele:
            continue

        paper_alleles.setdefault(paper_id, set()).add(allele)

    for alleles in paper_alleles.values():
        allele_list = sorted(list(alleles))
        for i in range(len(allele_list)):
            for j in range(i + 1, len(allele_list)):
                pair = (allele_list[i], allele_list[j])
                cooccurrence_counts[pair] = cooccurrence_counts.get(pair, 0) + 1

    with get_neo4j_session() as session:
        for (a1, a2), freq in cooccurrence_counts.items():
            session.run(
                """
                MATCH (x:HLAAllele {name: $a1}), (y:HLAAllele {name: $a2})
                MERGE (x)-[r:CO_OCCURS_WITH]->(y)
                SET r.frequency = $frequency, r.LD_coefficient = NULL
                """,
                {"a1": a1, "a2": a2, "frequency": freq},
            )
            session.run(
                """
                MATCH (x:HLAAllele {name: $a1}), (y:HLAAllele {name: $a2})
                MERGE (y)-[r:CO_OCCURS_WITH]->(x)
                SET r.frequency = $frequency, r.LD_coefficient = NULL
                """,
                {"a1": a1, "a2": a2, "frequency": freq},
            )


def _create_frequent_in_relationships(associations: List[Dict]):
    """Create FREQUENT_IN relationships from alleles to populations."""
    logger.info("Creating FREQUENT_IN relationships...")

    with get_neo4j_session() as session:
        for assoc in associations:
            allele = assoc.get("allele")
            population = assoc.get("population")
            if not allele or not population:
                continue

            percentage = None
            sample_cases = assoc.get("sample_cases")
            sample_controls = assoc.get("sample_controls")
            if sample_cases is not None and sample_controls is not None and sample_cases + sample_controls > 0:
                percentage = float(sample_cases) / float(sample_cases + sample_controls) * 100

            session.run(
                """
                MATCH (a:HLAAllele {name: $allele})
                MATCH (p:Population {name: $population})
                MERGE (a)-[r:FREQUENT_IN]->(p)
                SET r.percentage = $percentage
                """,
                {"allele": allele, "population": population, "percentage": percentage},
            )


def _create_combination_relationships(combinations: List[Dict], papers: List[Dict]):
    """Create HAPLOTYPE_ASSOCIATED_WITH relationships."""
    paper_id_to_pmid = {p["id"]: p.get("pmid") for p in papers}

    combos_with_haplotype = [c for c in combinations if c.get("haplotype_name")]
    logger.info(f"Creating {len(combos_with_haplotype)} haplotype associations...")

    with get_neo4j_session() as session:
        for combo in combos_with_haplotype:
            haplotype = combo.get("haplotype_name")
            disease = combo.get("disease")
            if not haplotype or not disease:
                continue

            pmid = paper_id_to_pmid.get(combo["paper_id"])
            disease_node = _normalize_disease_name(disease)

            session.run(
                """
                MATCH (h:Haplotype {name: $haplotype})
                MATCH (d:Disease {name: $disease})
                CREATE (h)-[r:HAPLOTYPE_ASSOCIATED_WITH {
                    effect: $effect,
                    odds_ratio: $odds_ratio,
                    ci_lower: $ci_lower,
                    ci_upper: $ci_upper,
                    p_value: $p_value,
                    context: $context,
                    paper_pmid: $pmid
                }]->(d)
                """,
                {
                    "haplotype": haplotype,
                    "disease": disease_node,
                    "effect": combo.get("effect"),
                    "odds_ratio": combo.get("odds_ratio"),
                    "ci_lower": combo.get("ci_lower"),
                    "ci_upper": combo.get("ci_upper"),
                    "p_value": combo.get("p_value"),
                    "context": combo.get("context"),
                    "pmid": pmid,
                },
            )


def _create_paper_relationships(papers: List[Dict], associations: List[Dict], combinations: List[Dict]):
    """Create REPORTS, STUDIES, and STUDIES_POPULATION relationships for papers."""
    logger.info("Creating Paper relationships...")

    paper_id_to_pmid = {p["id"]: p.get("pmid") for p in papers}

    # Collect paper -> disease, paper -> allele, paper -> population mappings
    paper_diseases = {}
    paper_alleles = {}
    paper_populations = {}

    for a in associations:
        pid = a["paper_id"]
        if pid not in paper_diseases:
            paper_diseases[pid] = set()
            paper_alleles[pid] = set()
            paper_populations[pid] = set()

        if a.get("disease"):
            paper_diseases[pid].add(a["disease"])
        if a.get("allele"):
            paper_alleles[pid].add(a["allele"])
        if a.get("population"):
            paper_populations[pid].add(a["population"])

    for c in combinations:
        pid = c["paper_id"]
        if pid not in paper_diseases:
            paper_diseases[pid] = set()
            paper_alleles[pid] = set()
            paper_populations[pid] = set()

        if c.get("disease"):
            paper_diseases[pid].add(c["disease"])
        for allele in (c.get("alleles") or []):
            paper_alleles[pid].add(allele)
        if c.get("population"):
            paper_populations[pid].add(c["population"])

    with get_neo4j_session() as session:
        for pid, pmid in paper_id_to_pmid.items():
            if not pmid:
                continue

            # STUDIES relationships
            for disease in paper_diseases.get(pid, []):
                disease_node = _normalize_disease_name(disease)
                session.run(
                    """
                    MATCH (p:Paper {pmid: $pmid})
                    MATCH (d:Disease {name: $disease})
                    MERGE (p)-[:STUDIES]->(d)
                    """,
                    {"pmid": pmid, "disease": disease_node},
                )

            # REPORTS relationships
            for allele in paper_alleles.get(pid, []):
                session.run(
                    """
                    MATCH (p:Paper {pmid: $pmid})
                    MATCH (a:HLAAllele {name: $allele})
                    MERGE (p)-[:REPORTS]->(a)
                    """,
                    {"pmid": pmid, "allele": allele},
                )

            # STUDIES_POPULATION relationships
            for pop in paper_populations.get(pid, []):
                session.run(
                    """
                    MATCH (p:Paper {pmid: $pmid})
                    MATCH (pop:Population {name: $population})
                    MERGE (p)-[:STUDIES_POPULATION]->(pop)
                    """,
                    {"pmid": pmid, "population": pop},
                )


def _normalize_disease_name(disease: str) -> str:
    """Normalize disease names to match the standard Disease node names."""
    disease_lower = disease.lower().strip()

    mapping = {
        # Original diseases
        "multiple sclerosis": "Multiple Sclerosis",
        "ms": "Multiple Sclerosis",
        "nmosd": "Neuromyelitis Optica Spectrum Disorder",
        "neuromyelitis optica": "Neuromyelitis Optica Spectrum Disorder",
        "neuromyelitis optica spectrum disorder": "Neuromyelitis Optica Spectrum Disorder",
        "devic disease": "Neuromyelitis Optica Spectrum Disorder",
        "myasthenia gravis": "Myasthenia Gravis",
        "mg": "Myasthenia Gravis",
        "guillain-barre syndrome": "Guillain-Barre Syndrome",
        "guillain-barré syndrome": "Guillain-Barre Syndrome",
        "guillain barre syndrome": "Guillain-Barre Syndrome",
        "gbs": "Guillain-Barre Syndrome",
        # New diseases
        "transverse myelitis": "Transverse Myelitis",
        "tm": "Transverse Myelitis",
        "optic neuritis": "Optic Neuritis",
        "on": "Optic Neuritis",
        "autoimmune encephalitis": "Autoimmune Encephalitis",
        "aie": "Autoimmune Encephalitis",
        "ae": "Autoimmune Encephalitis",
        "chronic inflammatory demyelinating polyneuropathy": "Chronic Inflammatory Demyelinating Polyneuropathy",
        "cidp": "Chronic Inflammatory Demyelinating Polyneuropathy",
        "acute disseminated encephalomyelitis": "Acute Disseminated Encephalomyelitis",
        "adem": "Acute Disseminated Encephalomyelitis",
        "mog antibody disease": "MOG Antibody Disease",
        "mogad": "MOG Antibody Disease",
        "mog-igg-associated disease": "MOG Antibody Disease",
        "mog-igg": "MOG Antibody Disease",
    }

    return mapping.get(disease_lower, disease)


def get_graph_stats() -> Dict:
    """Get statistics about the knowledge graph."""
    stats = {}

    queries = {
        "total_nodes": "MATCH (n) RETURN count(n) AS count",
        "total_relationships": "MATCH ()-[r]->() RETURN count(r) AS count",
        "diseases": "MATCH (d:Disease) RETURN count(d) AS count",
        "alleles": "MATCH (a:HLAAllele) RETURN count(a) AS count",
        "haplotypes": "MATCH (h:Haplotype) RETURN count(h) AS count",
        "populations": "MATCH (p:Population) RETURN count(p) AS count",
        "papers": "MATCH (p:Paper) RETURN count(p) AS count",
        "associations": "MATCH ()-[r:ASSOCIATED_WITH]->() RETURN count(r) AS count",
        "haplotype_associations": "MATCH ()-[r:HAPLOTYPE_ASSOCIATED_WITH]->() RETURN count(r) AS count",
    }

    for key, query in queries.items():
        try:
            result = run_query(query)
            stats[key] = result[0]["count"] if result else 0
        except Exception as e:
            logger.error(f"Error getting stat '{key}': {e}")
            stats[key] = "error"

    return stats
