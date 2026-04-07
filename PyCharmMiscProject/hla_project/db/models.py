"""
SQLAlchemy ORM models for the HLA Literature Mining Pipeline.
These mirror the tables defined in schema.sql.
All tables live in the schema specified by config.DATABASE_SCHEMA.
"""

from datetime import datetime
from sqlalchemy import (
    Column, Integer, String, Float, Text, Boolean, DateTime, ForeignKey, JSON
)
from sqlalchemy.orm import declarative_base, relationship

import config

Base = declarative_base()

# Schema name for all tables (e.g. "hla-research-db")
SCHEMA = config.DATABASE_SCHEMA


class Paper(Base):
    __tablename__ = "papers"
    __table_args__ = {"schema": SCHEMA}

    id = Column(Integer, primary_key=True, autoincrement=True)
    pmid = Column(String(20), unique=True, nullable=True, index=True)
    doi = Column(String(255), nullable=True, index=True)
    title = Column(Text, nullable=False)
    authors = Column(Text, nullable=True)
    journal = Column(String(500), nullable=True)
    year = Column(Integer, nullable=True, index=True)
    abstract = Column(Text, nullable=True)
    full_text = Column(Text, nullable=True)
    source = Column(String(50), nullable=False, default="pubmed", index=True)
    pmc_id = Column(String(20), nullable=True)
    has_full_text = Column(Boolean, default=False)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    # Relationships
    associations = relationship("HLAAssociation", back_populates="paper", cascade="all, delete-orphan")
    combinations = relationship("HLACombination", back_populates="paper", cascade="all, delete-orphan")
    extraction_logs = relationship("ExtractionLog", back_populates="paper", cascade="all, delete-orphan")

    def __repr__(self):
        return f"<Paper(id={self.id}, pmid={self.pmid}, title={self.title[:50]}...)>"


class HLAAssociation(Base):
    __tablename__ = "hla_associations"
    __table_args__ = {"schema": SCHEMA}

    id = Column(Integer, primary_key=True, autoincrement=True)
    paper_id = Column(Integer, ForeignKey(f"{SCHEMA}.papers.id", ondelete="CASCADE"), nullable=False, index=True)
    disease = Column(String(300), nullable=False, index=True)
    population = Column(String(200), nullable=True, index=True)
    allele = Column(String(500), nullable=False, index=True)
    effect = Column(String(20), nullable=True)
    odds_ratio = Column(Float, nullable=True)
    ci_lower = Column(Float, nullable=True)
    ci_upper = Column(Float, nullable=True)
    p_value = Column(Float, nullable=True)
    sample_cases = Column(Integer, nullable=True)
    sample_controls = Column(Integer, nullable=True)
    context = Column(Text, nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow)

    # Relationships
    paper = relationship("Paper", back_populates="associations")

    def __repr__(self):
        return f"<HLAAssociation(allele={self.allele}, disease={self.disease}, effect={self.effect})>"


class HLACombination(Base):
    __tablename__ = "hla_combinations"
    __table_args__ = {"schema": SCHEMA}

    id = Column(Integer, primary_key=True, autoincrement=True)
    paper_id = Column(Integer, ForeignKey(f"{SCHEMA}.papers.id", ondelete="CASCADE"), nullable=False, index=True)
    disease = Column(String(300), nullable=False, index=True)
    population = Column(String(200), nullable=True)
    alleles = Column(JSON, nullable=False)  # ["HLA-DRB1*15:01", "HLA-DQB1*06:02"]
    haplotype_name = Column(String(100), nullable=True)
    effect = Column(String(20), nullable=True)
    odds_ratio = Column(Float, nullable=True)
    ci_lower = Column(Float, nullable=True)
    ci_upper = Column(Float, nullable=True)
    p_value = Column(Float, nullable=True)
    sample_cases = Column(Integer, nullable=True)
    sample_controls = Column(Integer, nullable=True)
    context = Column(Text, nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow)

    # Relationships
    paper = relationship("Paper", back_populates="combinations")

    def __repr__(self):
        return f"<HLACombination(alleles={self.alleles}, disease={self.disease})>"


class ExtractionLog(Base):
    __tablename__ = "extraction_logs"
    __table_args__ = {"schema": SCHEMA}

    id = Column(Integer, primary_key=True, autoincrement=True)
    paper_id = Column(Integer, ForeignKey(f"{SCHEMA}.papers.id", ondelete="CASCADE"), nullable=False, index=True)
    status = Column(String(20), nullable=False, default="pending", index=True)
    model_used = Column(String(200), nullable=True)  # Increased from 50 to 200 for Hugging Face model names
    tokens_input = Column(Integer, nullable=True)
    tokens_output = Column(Integer, nullable=True)
    cost_usd = Column(Float, nullable=True)
    error_message = Column(Text, nullable=True)
    extracted_at = Column(DateTime, nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow)

    # Relationships
    paper = relationship("Paper", back_populates="extraction_logs")

    def __repr__(self):
        return f"<ExtractionLog(paper_id={self.paper_id}, status={self.status})>"


class SearchLog(Base):
    __tablename__ = "search_logs"
    __table_args__ = {"schema": SCHEMA}

    id = Column(Integer, primary_key=True, autoincrement=True)
    disease_code = Column(String(10), nullable=False)
    source = Column(String(50), nullable=False)
    query_text = Column(Text, nullable=False)
    result_count = Column(Integer, nullable=True)
    executed_at = Column(DateTime, default=datetime.utcnow)

    def __repr__(self):
        return f"<SearchLog(disease={self.disease_code}, source={self.source}, count={self.result_count})>"
