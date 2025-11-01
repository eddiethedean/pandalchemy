"""Simple PostgreSQL test to verify connection works."""

import logging

import pytest
from sqlalchemy import text

from pandalchemy import DataBase

# Setup logging for debugging
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)


@pytest.mark.postgres
@pytest.mark.timeout(10)
def test_postgres_connection(postgres_engine):
    """Simple test to verify PostgreSQL connection."""
    logger.info("Starting test_postgres_connection")
    engine = postgres_engine

    # Simple connection test
    logger.info("Opening connection")
    with engine.connect() as conn:
        logger.info("Executing SELECT 1")
        result = conn.execute(text("SELECT 1 as test"))
        logger.info("Fetching result")
        assert result.fetchone()[0] == 1
        logger.info("Committing")
        conn.commit()
    logger.info("test_postgres_connection completed")


@pytest.mark.postgres
@pytest.mark.timeout(30)
def test_postgres_basic_create(postgres_engine):
    """Test basic table creation in PostgreSQL."""
    import pandas as pd

    logger.info("Starting test_postgres_basic_create")
    logger.info("Creating DataBase instance")
    db = DataBase(postgres_engine)
    logger.info("DataBase created, loading tables")

    df = pd.DataFrame({"id": [1, 2], "name": ["Alice", "Bob"]})

    logger.info("Calling create_table")
    table = db.create_table("test_users", df, "id")
    logger.info("create_table returned, calling push")
    table.push()
    logger.info("push completed, calling pull")

    # Verify
    db.pull()
    logger.info("pull completed, checking assertions")
    assert len(db["test_users"]) == 2
    assert db["test_users"].get_row(1)["name"] == "Alice"
    logger.info("test_postgres_basic_create completed")
