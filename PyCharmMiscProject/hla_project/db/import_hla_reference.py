"""
Import IMGT/HLA allele reference CSV into the hla_allele_reference table.

Usage:
    cd /Users/cotaluvcat/PyCharmMiscProject/hla_project
    PYTHONPATH=. ../.venv/bin/python db/import_hla_reference.py \
        --csv '/Users/cotaluvcat/Downloads/HLA_Standardized_Final_v3_63_0 - Copy.csv'
"""

import argparse
import sys
import logging
from pathlib import Path

import pandas as pd
from sqlalchemy import text

# Make sure hla_project package is importable when run directly
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import config
from db.connection import engine

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
logger = logging.getLogger(__name__)

CREATE_TABLE_SQL = f"""
CREATE TABLE IF NOT EXISTS "{config.DATABASE_SCHEMA}".hla_allele_reference (
    id              SERIAL PRIMARY KEY,
    allele_id       VARCHAR(20)  NOT NULL UNIQUE,   -- e.g. HLA00001
    allele          VARCHAR(50)  NOT NULL,           -- full precision: A*01:01:01:01
    locus           VARCHAR(10)  NOT NULL,           -- gene locus: A, B, DRB1 ...
    allele_4digit   VARCHAR(20)  NOT NULL,           -- 2-field: A*01:01
    db_version      VARCHAR(50),
    download_date   DATE,
    created_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_hla_ref_allele
    ON "{config.DATABASE_SCHEMA}".hla_allele_reference(allele);

CREATE INDEX IF NOT EXISTS idx_hla_ref_locus
    ON "{config.DATABASE_SCHEMA}".hla_allele_reference(locus);

CREATE INDEX IF NOT EXISTS idx_hla_ref_4digit
    ON "{config.DATABASE_SCHEMA}".hla_allele_reference(allele_4digit);
"""


def create_table():
    with engine.connect() as conn:
        conn.execute(text(CREATE_TABLE_SQL))
        conn.commit()
    logger.info("Table hla_allele_reference ready.")


def import_csv(csv_path: str):
    logger.info(f"Reading CSV: {csv_path}")
    df = pd.read_csv(csv_path)

    # Rename CSV columns -> DB columns
    df = df.rename(columns={
        "AlleleID":    "allele_id",
        "Allele":      "allele",
        "Locus":       "locus",
        "Allele_4_digit": "allele_4digit",
        "DB_Version":  "db_version",
        "Download_Date": "download_date",
    })

    # Keep only the columns we need
    df = df[["allele_id", "allele", "locus", "allele_4digit", "db_version", "download_date"]]

    # Convert download_date to proper date (NaT becomes NULL)
    df["download_date"] = pd.to_datetime(df["download_date"], errors="coerce").dt.date

    logger.info(f"Loaded {len(df):,} rows. Importing...")

    # Use INSERT ... ON CONFLICT DO NOTHING so re-runs are safe
    with engine.connect() as conn:
        for start in range(0, len(df), 1000):
            chunk = df.iloc[start:start + 1000]
            rows = chunk.to_dict(orient="records")
            conn.execute(
                text(f"""
                    INSERT INTO "{config.DATABASE_SCHEMA}".hla_allele_reference
                        (allele_id, allele, locus, allele_4digit, db_version, download_date)
                    VALUES
                        (:allele_id, :allele, :locus, :allele_4digit, :db_version, :download_date)
                    ON CONFLICT (allele_id) DO NOTHING
                """),
                rows,
            )
            logger.info(f"  Inserted rows {start + 1}–{min(start + 1000, len(df))}")
        conn.commit()

    logger.info("Import complete.")


def main():
    parser = argparse.ArgumentParser(description="Import HLA allele reference CSV")
    parser.add_argument("--csv", required=True, help="Path to the CSV file")
    args = parser.parse_args()

    create_table()
    import_csv(args.csv)

    # Print summary
    with engine.connect() as conn:
        result = conn.execute(
            text(f'SELECT COUNT(*) FROM "{config.DATABASE_SCHEMA}".hla_allele_reference')
        )
        count = result.scalar()
    logger.info(f"Total rows in hla_allele_reference: {count:,}")


if __name__ == "__main__":
    main()
