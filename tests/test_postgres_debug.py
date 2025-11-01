"""Diagnostic tests to investigate PostgreSQL connection and lock issues."""

import logging

import pandas as pd
import pytest
from sqlalchemy import text

from pandalchemy import DataBase

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)


@pytest.mark.postgres
def test_postgres_locks_after_table_creation(postgres_engine):
    """Check PostgreSQL locks after table creation."""
    logger.info("Checking locks before any operations")

    # Check locks before
    with postgres_engine.connect() as conn:
        result = conn.execute(
            text("""
            SELECT locktype, relation::regclass, mode, granted
            FROM pg_locks
            WHERE relation IS NOT NULL
        """)
        )
        locks_before = result.fetchall()
        logger.info(f"Locks before: {locks_before}")

    # Create table
    logger.info("Creating DataBase and table")
    db = DataBase(postgres_engine)
    df = pd.DataFrame({"id": [1, 2], "name": ["Alice", "Bob"]})
    table = db.create_table("debug_users", df, "id")
    logger.info("Table created, checking locks after create_table")

    # Check locks after create_table
    with postgres_engine.connect() as conn:
        result = conn.execute(
            text("""
            SELECT locktype, relation::regclass, mode, granted
            FROM pg_locks
            WHERE relation IS NOT NULL
        """)
        )
        locks_after_create = result.fetchall()
        logger.info(f"Locks after create_table: {locks_after_create}")

    # Push
    logger.info("Calling push")
    table.push()
    logger.info("Push completed, checking locks after push")

    # Check locks after push
    with postgres_engine.connect() as conn:
        result = conn.execute(
            text("""
            SELECT locktype, relation::regclass, mode, granted
            FROM pg_locks
            WHERE relation IS NOT NULL
        """)
        )
        locks_after_push = result.fetchall()
        logger.info(f"Locks after push: {locks_after_push}")

    # Check active connections
    with postgres_engine.connect() as conn:
        result = conn.execute(
            text("""
            SELECT pid, usename, datname, state, query
            FROM pg_stat_activity
            WHERE datname = current_database()
            AND pid != pg_backend_pid()
        """)
        )
        active_connections = result.fetchall()
        logger.info(f"Active connections: {active_connections}")


@pytest.mark.postgres
def test_postgres_activity_during_operations(postgres_engine):
    """Check PostgreSQL activity during table operations."""
    logger.info("Starting activity check")

    # Check activity before
    with postgres_engine.connect() as conn:
        result = conn.execute(
            text("""
            SELECT count(*) as active_transactions
            FROM pg_stat_activity
            WHERE state = 'active'
            AND datname = current_database()
        """)
        )
        active_before = result.fetchone()[0]
        logger.info(f"Active transactions before: {active_before}")

    # Create table
    logger.info("Creating DataBase")
    db = DataBase(postgres_engine)
    logger.info("DataBase created")

    df = pd.DataFrame({"id": [1], "name": ["Test"]})

    logger.info("Calling create_table")
    table = db.create_table("debug_test", df, "id")
    logger.info("create_table completed")

    # Check activity after create_table
    with postgres_engine.connect() as conn:
        result = conn.execute(
            text("""
            SELECT count(*) as active_transactions
            FROM pg_stat_activity
            WHERE state = 'active'
            AND datname = current_database()
        """)
        )
        active_after_create = result.fetchone()[0]
        logger.info(f"Active transactions after create_table: {active_after_create}")

    logger.info("Calling push")
    table.push()
    logger.info("push completed")

    # Check activity after push
    with postgres_engine.connect() as conn:
        result = conn.execute(
            text("""
            SELECT count(*) as active_transactions
            FROM pg_stat_activity
            WHERE state = 'active'
            AND datname = current_database()
        """)
        )
        active_after_push = result.fetchone()[0]
        logger.info(f"Active transactions after push: {active_after_push}")


@pytest.mark.postgres
def test_postgres_inspect_connection_usage(postgres_engine):
    """Test how inspect(engine) uses connections."""
    from sqlalchemy import inspect as sqlalchemy_inspect

    logger.info("Testing inspect connection usage")

    # Check connections before
    with postgres_engine.connect() as conn:
        result = conn.execute(
            text("""
            SELECT count(*) FROM pg_stat_activity
            WHERE datname = current_database()
        """)
        )
        conn_count_before = result.fetchone()[0]
        logger.info(f"Connections before inspect: {conn_count_before}")

    # Use inspect
    logger.info("Calling inspect(engine)")
    inspector = sqlalchemy_inspect(postgres_engine)
    logger.info("Getting table names")
    tables = inspector.get_table_names()
    logger.info(f"Table names: {tables}")

    # Check connections after
    with postgres_engine.connect() as conn:
        result = conn.execute(
            text("""
            SELECT count(*) FROM pg_stat_activity
            WHERE datname = current_database()
        """)
        )
        conn_count_after = result.fetchone()[0]
        logger.info(f"Connections after inspect: {conn_count_after}")

    # Try to explicitly close inspector connection
    # Note: inspect() might open connections that need closing
