"""
Import KEGG pathway protein reference (Excel) into the kegg_protein_reference table.

Columns in source file:
    gene_id, gene, desc, pathway, pathway_id, kegg_link

Usage:
    cd /Users/cotaluvcat/PyCharmMiscProject/hla_project
    python3 db/import_kegg_proteins.py \
        --xlsx '/Users/cotaluvcat/Downloads/kegg_proteins_live.xlsx'
"""

import argparse
import sys
import logging
from pathlib import Path

import pandas as pd
from sqlalchemy import create_engine, text

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import config
from db.connection import engine

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
logger = logging.getLogger(__name__)

CREATE_TABLE_SQL = f"""
CREATE TABLE IF NOT EXISTS "{config.DATABASE_SCHEMA}".kegg_protein_reference (
    id          SERIAL PRIMARY KEY,
    gene_id     INTEGER      NOT NULL,          -- NCBI gene ID e.g. 112744
    gene        VARCHAR(50)  NOT NULL,          -- gene symbol e.g. IL17F
    description TEXT,                           -- full gene description
    pathway     VARCHAR(200),                   -- pathway name e.g. JAK-STAT Signaling
    pathway_id  VARCHAR(20),                    -- KEGG pathway ID e.g. hsa04659
    kegg_link   TEXT,                           -- URL to KEGG entry
    created_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (gene_id, pathway_id)
);

CREATE INDEX IF NOT EXISTS idx_kegg_gene
    ON "{config.DATABASE_SCHEMA}".kegg_protein_reference(gene);

CREATE INDEX IF NOT EXISTS idx_kegg_pathway_id
    ON "{config.DATABASE_SCHEMA}".kegg_protein_reference(pathway_id);

CREATE INDEX IF NOT EXISTS idx_kegg_pathway_name
    ON "{config.DATABASE_SCHEMA}".kegg_protein_reference(pathway);
"""


def create_table():
    with engine.connect() as conn:
        conn.execute(text(CREATE_TABLE_SQL))
        conn.commit()
    logger.info("Table kegg_protein_reference ready.")


def import_xlsx(xlsx_path: str):
    logger.info(f"Reading Excel: {xlsx_path}")
    df = pd.read_excel(xlsx_path)

    # Rename to match DB columns
    df = df.rename(columns={
        "gene_id":   "gene_id",
        "gene":      "gene",
        "desc":      "description",
        "pathway":   "pathway",
        "pathway_id": "pathway_id",
        "kegg_link": "kegg_link",
    })

    df = df[["gene_id", "gene", "description", "pathway", "pathway_id", "kegg_link"]]

    # gene_id must be integer; drop any rows where it is missing/non-numeric
    df["gene_id"] = pd.to_numeric(df["gene_id"], errors="coerce")
    before = len(df)
    df = df.dropna(subset=["gene_id"])
    df["gene_id"] = df["gene_id"].astype(int)
    if len(df) < before:
        logger.warning(f"Dropped {before - len(df)} rows with non-numeric gene_id")

    logger.info(f"Loaded {len(df):,} rows. Importing...")

    with engine.connect() as conn:
        for start in range(0, len(df), 500):
            chunk = df.iloc[start:start + 500]
            rows = chunk.to_dict(orient="records")
            conn.execute(
                text(f"""
                    INSERT INTO "{config.DATABASE_SCHEMA}".kegg_protein_reference
                        (gene_id, gene, description, pathway, pathway_id, kegg_link)
                    VALUES
                        (:gene_id, :gene, :description, :pathway, :pathway_id, :kegg_link)
                    ON CONFLICT (gene_id, pathway_id) DO NOTHING
                """),
                rows,
            )
            logger.info(f"  Inserted rows {start + 1}–{min(start + 500, len(df))}")
        conn.commit()

    logger.info("Import complete.")


def main():
    parser = argparse.ArgumentParser(description="Import KEGG protein reference Excel")
    parser.add_argument("--xlsx", required=True, help="Path to the .xlsx file")
    args = parser.parse_args()

    create_table()
    import_xlsx(args.xlsx)

    with engine.connect() as conn:
        result = conn.execute(
            text(f'SELECT COUNT(*) FROM "{config.DATABASE_SCHEMA}".kegg_protein_reference')
        )
        count = result.scalar()
    logger.info(f"Total rows in kegg_protein_reference: {count:,}")


if __name__ == "__main__":
    main()
