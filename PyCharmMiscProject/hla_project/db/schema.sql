-- HLA Literature Mining Pipeline - PostgreSQL Schema
-- Run: psql -d postgres -f db/schema.sql
-- Uses schema "hla-research-db" within the postgres database.

CREATE SCHEMA IF NOT EXISTS "hla-research-db";
SET search_path TO "hla-research-db", public;

CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- ============================================================
-- Papers table: stores metadata for each paper
-- ============================================================
CREATE TABLE IF NOT EXISTS papers (
    id              SERIAL PRIMARY KEY,
    pmid            VARCHAR(20) UNIQUE,
    doi             VARCHAR(255),
    title           TEXT NOT NULL,
    authors         TEXT,
    journal         VARCHAR(500),
    year            INTEGER,
    abstract        TEXT,
    full_text       TEXT,
    source          VARCHAR(50) NOT NULL DEFAULT 'pubmed',  -- pubmed, biorxiv, pmc
    pmc_id          VARCHAR(20),
    has_full_text   BOOLEAN DEFAULT FALSE,
    created_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_papers_pmid ON papers(pmid);
CREATE INDEX IF NOT EXISTS idx_papers_doi ON papers(doi);
CREATE INDEX IF NOT EXISTS idx_papers_year ON papers(year);
CREATE INDEX IF NOT EXISTS idx_papers_source ON papers(source);

-- ============================================================
-- HLA Associations: individual allele-disease associations
-- ============================================================
CREATE TABLE IF NOT EXISTS hla_associations (
    id                  SERIAL PRIMARY KEY,
    paper_id            INTEGER NOT NULL REFERENCES papers(id) ON DELETE CASCADE,
    disease             VARCHAR(300) NOT NULL,
    population          VARCHAR(200),
    allele              VARCHAR(500) NOT NULL,
    effect              VARCHAR(20),           -- risk, protective, neutral
    odds_ratio          FLOAT,
    ci_lower            FLOAT,
    ci_upper            FLOAT,
    p_value             FLOAT,
    sample_cases        INTEGER,
    sample_controls     INTEGER,
    context             TEXT,
    created_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_hla_assoc_paper ON hla_associations(paper_id);
CREATE INDEX IF NOT EXISTS idx_hla_assoc_disease ON hla_associations(disease);
CREATE INDEX IF NOT EXISTS idx_hla_assoc_allele ON hla_associations(allele);
CREATE INDEX IF NOT EXISTS idx_hla_assoc_population ON hla_associations(population);
CREATE INDEX IF NOT EXISTS idx_hla_assoc_effect ON hla_associations(effect);

-- ============================================================
-- HLA Combinations: haplotype / multi-allele associations
-- ============================================================
CREATE TABLE IF NOT EXISTS hla_combinations (
    id                  SERIAL PRIMARY KEY,
    paper_id            INTEGER NOT NULL REFERENCES papers(id) ON DELETE CASCADE,
    disease             VARCHAR(300) NOT NULL,
    population          VARCHAR(200),
    alleles             JSONB NOT NULL,          -- ["HLA-DRB1*15:01", "HLA-DQB1*06:02"]
    haplotype_name      VARCHAR(100),
    effect              VARCHAR(20),
    odds_ratio          FLOAT,
    ci_lower            FLOAT,
    ci_upper            FLOAT,
    p_value             FLOAT,
    sample_cases        INTEGER,
    sample_controls     INTEGER,
    context             TEXT,
    created_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_hla_combo_paper ON hla_combinations(paper_id);
CREATE INDEX IF NOT EXISTS idx_hla_combo_disease ON hla_combinations(disease);
CREATE INDEX IF NOT EXISTS idx_hla_combo_alleles ON hla_combinations USING GIN(alleles);

-- ============================================================
-- Extraction logs: track LLM extraction status per paper
-- ============================================================
CREATE TABLE IF NOT EXISTS extraction_logs (
    id              SERIAL PRIMARY KEY,
    paper_id        INTEGER NOT NULL REFERENCES papers(id) ON DELETE CASCADE,
    status          VARCHAR(20) NOT NULL DEFAULT 'pending',  -- pending, processing, completed, failed, skipped
    model_used      VARCHAR(200),  -- Increased from 50 to 200 for Hugging Face model names
    tokens_input    INTEGER,
    tokens_output   INTEGER,
    cost_usd        FLOAT,
    error_message   TEXT,
    extracted_at    TIMESTAMP,
    created_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_extraction_paper ON extraction_logs(paper_id);
CREATE INDEX IF NOT EXISTS idx_extraction_status ON extraction_logs(status);

-- ============================================================
-- Search logs: track search queries and results
-- ============================================================
CREATE TABLE IF NOT EXISTS search_logs (
    id              SERIAL PRIMARY KEY,
    disease_code    VARCHAR(10) NOT NULL,
    source          VARCHAR(50) NOT NULL,
    query_text      TEXT NOT NULL,
    result_count    INTEGER,
    executed_at     TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
