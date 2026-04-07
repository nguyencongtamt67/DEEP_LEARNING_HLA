"""
Database connection management using SQLAlchemy.
Provides a session factory and engine for the PostgreSQL database.
Uses a dedicated schema (default: "hla-research-db") within the postgres database.
"""

from sqlalchemy import create_engine, event, text
from sqlalchemy.orm import sessionmaker, Session
from contextlib import contextmanager

import config

# Quote the schema name since it contains a hyphen
_schema_quoted = f'"{config.DATABASE_SCHEMA}"'

engine = create_engine(
    config.DATABASE_URL,
    pool_size=10,
    max_overflow=20,
    pool_pre_ping=True,
    echo=False,
)


# Set search_path to our schema on every new connection
@event.listens_for(engine, "connect")
def set_search_path(dbapi_connection, connection_record):
    cursor = dbapi_connection.cursor()
    cursor.execute(f"SET search_path TO {_schema_quoted}, public")
    cursor.close()
    dbapi_connection.commit()


SessionLocal = sessionmaker(bind=engine, autocommit=False, autoflush=False)


@contextmanager
def get_session() -> Session:
    """Context manager that provides a transactional database session."""
    session = SessionLocal()
    try:
        yield session
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()


def init_db():
    """Create the schema (if needed) and all tables defined in models."""
    # Create the schema first
    with engine.connect() as conn:
        conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {_schema_quoted}"))
        conn.commit()

    from db.models import Base
    Base.metadata.create_all(bind=engine)
