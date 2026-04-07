"""
Neo4j connection management.
Provides a driver and session factory for the Neo4j graph database.
"""

import logging
from contextlib import contextmanager

from neo4j import GraphDatabase

import config

logger = logging.getLogger(__name__)

_driver = None


def get_driver():
    """Get or create the Neo4j driver (singleton)."""
    global _driver
    if _driver is None:
        _driver = GraphDatabase.driver(
            config.NEO4J_URI,
            auth=(config.NEO4J_USER, config.NEO4J_PASSWORD),
        )
        # Verify connectivity
        try:
            _driver.verify_connectivity()
            logger.info(f"Connected to Neo4j at {config.NEO4J_URI}")
        except Exception as e:
            logger.error(f"Failed to connect to Neo4j: {e}")
            raise
    return _driver


@contextmanager
def get_neo4j_session(database: str = "neo4j"):
    """Context manager for Neo4j sessions."""
    driver = get_driver()
    session = driver.session(database=database)
    try:
        yield session
    finally:
        session.close()


def close_driver():
    """Close the Neo4j driver."""
    global _driver
    if _driver is not None:
        _driver.close()
        _driver = None
        logger.info("Neo4j driver closed")


def run_query(query: str, parameters: dict = None, database: str = "neo4j"):
    """Execute a Cypher query and return results."""
    with get_neo4j_session(database=database) as session:
        result = session.run(query, parameters or {})
        return [record.data() for record in result]


def clear_database(database: str = "neo4j"):
    """Clear all nodes and relationships (use with caution!)."""
    with get_neo4j_session(database=database) as session:
        session.run("MATCH (n) DETACH DELETE n")
        logger.warning("Neo4j database cleared")
