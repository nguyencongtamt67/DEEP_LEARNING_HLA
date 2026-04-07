"""
Module 2.1 – Network Construction: HLA–Disease Association Network

Pulls ASSOCIATED_WITH edges from Neo4j, filters by statistical significance,
builds a weighted directed NetworkX graph, runs analysis, and exports to
Gephi-compatible GEXF format.

Usage:
    cd /Users/cotaluvcat/PyCharmMiscProject/hla_project
    python3 network/hla_disease_network.py

Outputs (written to hla_project/data/network/):
    hla_disease_network.gexf        – Gephi import file
    hla_disease_network.graphml     – alternative export
    network_stats.txt               – hub alleles, disease-specific alleles
    community_assignments.csv       – Louvain community per allele node
    network_visualization.png       – quick NetworkX plot
"""

import logging
import math
import csv
import sys
from pathlib import Path
from collections import defaultdict

import networkx as nx
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.cm as cm
import matplotlib.colors as mcolors

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from knowledge_graph.neo4j_connection import run_query
import config

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")

# ── Output directory ──────────────────────────────────────────────────────────
OUT_DIR = Path(__file__).resolve().parent.parent / "data" / "network"
OUT_DIR.mkdir(parents=True, exist_ok=True)

# ── Significance thresholds ───────────────────────────────────────────────────
P_VALUE_THRESHOLD = 0.05      # p < 0.05
# CI filter: OR CI does not include 1 → ci_lower > 1 (risk) or ci_upper < 1 (protective)

# ── Locus → colour mapping for node colouring ────────────────────────────────
LOCUS_COLORS = {
    "A":    "#e74c3c",   # red
    "B":    "#e67e22",   # orange
    "C":    "#f1c40f",   # yellow
    "DRB1": "#2ecc71",   # green
    "DQB1": "#3498db",   # blue
    "DQA1": "#9b59b6",   # purple
    "DPB1": "#1abc9c",   # teal
    "DPA1": "#34495e",   # dark
}
DEFAULT_LOCUS_COLOR = "#95a5a6"  # grey


# ─────────────────────────────────────────────────────────────────────────────
# Step 1 – Extract from Neo4j
# ─────────────────────────────────────────────────────────────────────────────

def fetch_associations() -> list[dict]:
    """
    Pull all ASSOCIATED_WITH edges from Neo4j.
    Returns a list of dicts with allele, disease, OR, CI, p_value, population,
    paper_pmid.
    """
    logger.info("Fetching ASSOCIATED_WITH edges from Neo4j…")
    query = """
    MATCH (a:HLAAllele)-[r:ASSOCIATED_WITH]->(d:Disease)
    RETURN
        a.name          AS allele,
        d.name          AS disease,
        r.odds_ratio    AS odds_ratio,
        r.ci_lower      AS ci_lower,
        r.ci_upper      AS ci_upper,
        r.p_value       AS p_value,
        r.population    AS population,
        r.paper_pmid    AS paper_pmid
    """
    rows = run_query(query)
    logger.info(f"  Retrieved {len(rows):,} raw edges")
    return rows


# ─────────────────────────────────────────────────────────────────────────────
# Step 2 – Filter by significance
# ─────────────────────────────────────────────────────────────────────────────

def _ci_excludes_one(ci_lower, ci_upper) -> bool:
    """True if the 95% CI does not span 1 (both bounds on the same side)."""
    if ci_lower is not None and ci_upper is not None:
        return ci_upper < 1.0 or ci_lower > 1.0
    return False  # unknown CI → don't count as CI-confirmed


def filter_significant(rows: list[dict]) -> list[dict]:
    """
    Keep rows that are statistically significant:
      - p_value < 0.05, OR
      - CI does not include 1
    """
    significant = []
    for r in rows:
        p  = r.get("p_value")
        p_ok = (p is not None and p < P_VALUE_THRESHOLD)
        ci_ok = _ci_excludes_one(r.get("ci_lower"), r.get("ci_upper"))
        if p_ok or ci_ok:
            significant.append(r)

    logger.info(
        f"  After significance filter: {len(significant):,} / {len(rows):,} edges kept"
    )
    return significant


# ─────────────────────────────────────────────────────────────────────────────
# Step 3 – Aggregate multi-study edges
# ─────────────────────────────────────────────────────────────────────────────

def aggregate_edges(rows: list[dict]) -> list[dict]:
    """
    Multiple papers may report the same allele→disease pair.
    Aggregate by (allele, disease):
      - weight  = mean(log(OR) * -log10(p)) across studies (skip if OR or p missing)
      - OR      = mean OR
      - p_value = min p_value
      - study_count = number of studies
      - populations = comma-joined unique populations
    """
    buckets: dict[tuple, list] = defaultdict(list)
    for r in rows:
        key = (r["allele"], r["disease"])
        buckets[key].append(r)

    aggregated = []
    for (allele, disease), studies in buckets.items():
        ors     = [s["odds_ratio"] for s in studies if s.get("odds_ratio") is not None]
        ps      = [s["p_value"]    for s in studies if s.get("p_value")    is not None]
        ci_lows = [s["ci_lower"]   for s in studies if s.get("ci_lower")   is not None]
        ci_ups  = [s["ci_upper"]   for s in studies if s.get("ci_upper")   is not None]
        pops    = list({s["population"] for s in studies if s.get("population")})

        mean_or = sum(ors) / len(ors) if ors else None
        min_p   = min(ps) if ps else None

        # Edge weight: log(OR) × –log10(p) — only when both are available
        weights = []
        for s in studies:
            _or = s.get("odds_ratio")
            _p  = s.get("p_value")
            if _or and _or > 0 and _p and _p > 0:
                weights.append(math.log(_or) * (-math.log10(_p)))
        weight = sum(weights) / len(weights) if weights else (
            math.log(mean_or) if mean_or and mean_or > 0 else None
        )

        aggregated.append({
            "allele":       allele,
            "disease":      disease,
            "odds_ratio":   mean_or,
            "ci_lower":     sum(ci_lows) / len(ci_lows) if ci_lows else None,
            "ci_upper":     sum(ci_ups)  / len(ci_ups)  if ci_ups  else None,
            "p_value":      min_p,
            "weight":       weight if weight is not None else 0.0,
            "study_count":  len(studies),
            "population":   ", ".join(pops) if pops else "Unknown",
            "effect":       "risk" if (mean_or and mean_or > 1) else "protective",
        })

    logger.info(f"  Aggregated to {len(aggregated):,} unique allele→disease edges")
    return aggregated


# ─────────────────────────────────────────────────────────────────────────────
# Step 4 – Build NetworkX graph
# ─────────────────────────────────────────────────────────────────────────────

def _locus_from_allele(allele: str) -> str:
    """Extract locus name from allele string, e.g. 'HLA-DRB1*15:01' → 'DRB1'."""
    if not allele:
        return "Unknown"
    # Strip HLA- prefix
    name = allele.replace("HLA-", "").replace("hla-", "")
    # Take the part before the asterisk (or first digit)
    for i, ch in enumerate(name):
        if ch == "*" or ch.isdigit():
            return name[:i]
    return name


def build_network(edges: list[dict]) -> nx.DiGraph:
    """
    Build a weighted directed graph:
      HLAAllele → Disease
    Node attributes:
      type, locus (alleles), color
    Edge attributes:
      weight, odds_ratio, ci_lower, ci_upper, p_value, study_count,
      population, effect
    """
    G = nx.DiGraph()

    # Disease set from config
    disease_names = {v["name"] for v in config.DISEASES.values()}

    for e in edges:
        allele  = e["allele"]
        disease = e["disease"]
        locus   = _locus_from_allele(allele)

        # Add / update HLA allele node
        if not G.has_node(allele):
            G.add_node(
                allele,
                node_type="HLAAllele",
                locus=locus,
                color=LOCUS_COLORS.get(locus, DEFAULT_LOCUS_COLOR),
            )

        # Add / update Disease node
        if not G.has_node(disease):
            G.add_node(
                disease,
                node_type="Disease",
                locus="",
                color="#e91e63",  # pink for disease nodes
            )

        w = e["weight"] if e["weight"] else 0.0
        G.add_edge(
            allele, disease,
            weight=abs(w),          # abs for layout algorithms
            raw_weight=w,
            odds_ratio=e["odds_ratio"],
            ci_lower=e["ci_lower"],
            ci_upper=e["ci_upper"],
            p_value=e["p_value"],
            study_count=e["study_count"],
            population=e["population"],
            effect=e["effect"],
        )

    logger.info(
        f"Network built: {G.number_of_nodes()} nodes, {G.number_of_edges()} edges"
    )
    return G


# ─────────────────────────────────────────────────────────────────────────────
# Step 5 – Analysis
# ─────────────────────────────────────────────────────────────────────────────

def analyse_network(G: nx.DiGraph) -> dict:
    """
    Run network analysis:
      - Hub alleles (top-degree HLA nodes)
      - Disease-specific alleles (degree = 1)
      - Louvain community detection on undirected projection
    Returns a stats dict.
    """
    allele_nodes  = [n for n, d in G.nodes(data=True) if d.get("node_type") == "HLAAllele"]
    disease_nodes = [n for n, d in G.nodes(data=True) if d.get("node_type") == "Disease"]

    # Degree of allele nodes (out-degree in directed graph = number of diseases linked)
    allele_degrees = {n: G.out_degree(n) for n in allele_nodes}
    sorted_by_degree = sorted(allele_degrees.items(), key=lambda x: -x[1])

    # Hub alleles: top-10% or top-20 by degree
    top_n = max(20, len(sorted_by_degree) // 10)
    hub_alleles = sorted_by_degree[:top_n]

    # Disease-specific alleles: connected to exactly 1 disease
    disease_specific = [(n, deg) for n, deg in allele_degrees.items() if deg == 1]

    # Louvain community detection (run on undirected)
    try:
        import community as community_louvain
        UG = G.to_undirected()
        partition = community_louvain.best_partition(UG, weight="weight", random_state=42)
        n_communities = len(set(partition.values()))
        # Annotate nodes with community
        nx.set_node_attributes(G, partition, name="community")
        logger.info(f"  Louvain: {n_communities} communities detected")
    except ImportError:
        logger.warning("python-louvain not available; skipping community detection")
        partition = {}
        n_communities = 0

    stats = {
        "total_allele_nodes": len(allele_nodes),
        "total_disease_nodes": len(disease_nodes),
        "total_edges": G.number_of_edges(),
        "hub_alleles": hub_alleles,
        "disease_specific_alleles": disease_specific,
        "n_communities": n_communities,
        "partition": partition,
    }
    return stats


# ─────────────────────────────────────────────────────────────────────────────
# Step 6 – Visualization (NetworkX quick plot)
# ─────────────────────────────────────────────────────────────────────────────

def visualize_network(G: nx.DiGraph, out_path: Path):
    """
    Force-directed layout (Fruchterman-Reingold).
    Node size ∝ degree, color by locus, edge color by effect.
    """
    logger.info("Generating network visualization…")

    # Use spring layout (Fruchterman–Reingold)
    pos = nx.spring_layout(G, weight="weight", seed=42, k=0.8)

    # Node sizes proportional to total degree
    degrees = dict(G.degree())
    max_deg = max(degrees.values()) if degrees else 1
    node_sizes = [300 + 1500 * (degrees[n] / max_deg) for n in G.nodes()]

    # Node colours
    node_colors = [G.nodes[n].get("color", DEFAULT_LOCUS_COLOR) for n in G.nodes()]

    # Edge colours: red = risk, blue = protective
    edge_colors = [
        "#e74c3c" if G.edges[u, v].get("effect") == "risk" else "#3498db"
        for u, v in G.edges()
    ]

    # Edge widths proportional to weight
    weights = [G.edges[u, v].get("weight", 0.5) for u, v in G.edges()]
    max_w   = max(weights) if weights else 1
    edge_widths = [0.5 + 3.0 * (w / max_w) for w in weights]

    fig, ax = plt.subplots(figsize=(20, 16))
    nx.draw_networkx_nodes(G, pos, node_size=node_sizes, node_color=node_colors,
                           alpha=0.85, ax=ax)
    nx.draw_networkx_edges(G, pos, edge_color=edge_colors, width=edge_widths,
                           alpha=0.6, arrows=True, arrowsize=12, ax=ax)

    # Label only disease nodes + hub alleles (top 20)
    hub_set = {n for n, _ in sorted(
        {n: G.out_degree(n) for n in G.nodes()}.items(), key=lambda x: -x[1]
    )[:20]}
    disease_set = {n for n, d in G.nodes(data=True) if d.get("node_type") == "Disease"}
    label_nodes = hub_set | disease_set
    labels = {n: n for n in label_nodes}
    nx.draw_networkx_labels(G, pos, labels=labels, font_size=7, ax=ax)

    # Legend
    legend_elements = [
        plt.Line2D([0], [0], marker="o", color="w", markerfacecolor="#e74c3c",
                   markersize=10, label="Risk edge (OR > 1)"),
        plt.Line2D([0], [0], marker="o", color="w", markerfacecolor="#3498db",
                   markersize=10, label="Protective edge (OR < 1)"),
        plt.Line2D([0], [0], marker="o", color="w", markerfacecolor="#e91e63",
                   markersize=12, label="Disease node"),
    ]
    for locus, color in LOCUS_COLORS.items():
        legend_elements.append(
            plt.Line2D([0], [0], marker="o", color="w", markerfacecolor=color,
                       markersize=8, label=f"HLA-{locus}")
        )
    ax.legend(handles=legend_elements, loc="upper left", fontsize=8)
    ax.set_title("HLA–Disease Association Network\n(edge width ∝ effect size × significance)",
                 fontsize=14)
    ax.axis("off")
    plt.tight_layout()
    plt.savefig(out_path, dpi=150)
    plt.close()
    logger.info(f"  Saved visualization → {out_path}")


# ─────────────────────────────────────────────────────────────────────────────
# Step 7 – Export (GEXF for Gephi, GraphML, CSV stats)
# ─────────────────────────────────────────────────────────────────────────────

def export_network(G: nx.DiGraph, stats: dict):
    """Export graph and analysis results to data/network/."""

    # GEXF (Gephi)
    gexf_path = OUT_DIR / "hla_disease_network.gexf"
    # GEXF does not allow None values — sanitize a copy first
    G_gexf = G.copy()
    for u, v, data in G_gexf.edges(data=True):
        for k in list(data.keys()):
            if data[k] is None:
                data[k] = "" if isinstance(data.get(k, ""), str) else 0.0
        # Ensure numeric fields are float, not None
        for num_field in ("odds_ratio", "ci_lower", "ci_upper", "p_value", "weight", "raw_weight"):
            if data.get(num_field) is None:
                data[num_field] = 0.0
        for str_field in ("population", "effect"):
            if data.get(str_field) is None:
                data[str_field] = ""
    for n, data in G_gexf.nodes(data=True):
        for k in list(data.keys()):
            if data[k] is None:
                data[k] = ""
    nx.write_gexf(G_gexf, str(gexf_path))
    logger.info(f"  Saved GEXF  → {gexf_path}")

    # GraphML
    graphml_path = OUT_DIR / "hla_disease_network.graphml"
    nx.write_graphml(G_gexf, str(graphml_path))
    logger.info(f"  Saved GraphML → {graphml_path}")

    # Network stats text report
    stats_path = OUT_DIR / "network_stats.txt"
    with open(stats_path, "w") as f:
        f.write("=== HLA–Disease Network Stats ===\n\n")
        f.write(f"Total HLA allele nodes : {stats['total_allele_nodes']}\n")
        f.write(f"Total Disease nodes    : {stats['total_disease_nodes']}\n")
        f.write(f"Total edges            : {stats['total_edges']}\n")
        f.write(f"Louvain communities    : {stats['n_communities']}\n\n")

        f.write(f"── Hub Alleles (top {len(stats['hub_alleles'])} by disease degree) ──\n")
        for allele, deg in stats["hub_alleles"]:
            f.write(f"  {allele:<30}  degree = {deg}\n")

        f.write(f"\n── Disease-Specific Alleles ({len(stats['disease_specific_alleles'])} total) ──\n")
        for allele, deg in sorted(stats["disease_specific_alleles"],
                                  key=lambda x: x[0]):
            f.write(f"  {allele}\n")
    logger.info(f"  Saved stats → {stats_path}")

    # Community CSV
    if stats["partition"]:
        comm_path = OUT_DIR / "community_assignments.csv"
        with open(comm_path, "w", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(["node", "node_type", "locus", "community"])
            for node, comm_id in sorted(stats["partition"].items()):
                node_type = G.nodes[node].get("node_type", "")
                locus     = G.nodes[node].get("locus", "")
                writer.writerow([node, node_type, locus, comm_id])
        logger.info(f"  Saved communities → {comm_path}")


# ─────────────────────────────────────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────────────────────────────────────

def run():
    logger.info("=== Module 2.1: HLA–Disease Network Construction ===")

    # 1. Fetch
    raw = fetch_associations()
    if not raw:
        logger.error("No ASSOCIATED_WITH edges found in Neo4j. Run graph builder first.")
        return

    # 2. Filter
    significant = filter_significant(raw)
    if not significant:
        logger.warning("No edges passed significance filter. Lowering threshold or check data.")
        # Fall back to all edges so the pipeline still produces output
        significant = raw

    # 3. Aggregate
    edges = aggregate_edges(significant)

    # 4. Build graph
    G = build_network(edges)

    # 5. Analyse
    stats = analyse_network(G)

    # Print summary to console
    print("\n=== Network Summary ===")
    print(f"  HLA allele nodes : {stats['total_allele_nodes']}")
    print(f"  Disease nodes    : {stats['total_disease_nodes']}")
    print(f"  Edges            : {stats['total_edges']}")
    print(f"  Communities      : {stats['n_communities']}")
    print(f"\nTop 10 hub alleles:")
    for allele, deg in stats["hub_alleles"][:10]:
        print(f"    {allele:<30}  connected to {deg} disease(s)")
    print(f"\nDisease-specific alleles (connected to 1 disease): "
          f"{len(stats['disease_specific_alleles'])}")

    # 6. Visualize
    visualize_network(G, OUT_DIR / "network_visualization.png")

    # 7. Export
    export_network(G, stats)

    print(f"\nAll outputs written to: {OUT_DIR}")


if __name__ == "__main__":
    run()
