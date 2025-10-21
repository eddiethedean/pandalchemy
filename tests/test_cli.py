"""Tests for CLI functionality."""


import pytest
from sqlalchemy import create_engine, text

from pandalchemy.cli import info_command, main, validate_command


def test_cli_help(capsys):
    """Test CLI help output."""
    with pytest.raises(SystemExit) as exc_info:
        main(['pandalchemy', '--help'])

    assert exc_info.value.code == 0
    captured = capsys.readouterr()
    output = captured.out + captured.err
    assert 'pandalchemy' in output.lower() or 'usage' in output.lower()


def test_cli_version(capsys):
    """Test --version flag."""
    with pytest.raises(SystemExit) as exc_info:
        main(['pandalchemy', '--version'])

    assert exc_info.value.code == 0
    captured = capsys.readouterr()
    output = captured.out + captured.err
    assert '1.0.0' in output


def test_cli_no_arguments(capsys):
    """Test CLI with no arguments shows help."""
    with pytest.raises(SystemExit) as exc_info:
        main(['pandalchemy'])

    assert exc_info.value.code == 0


def test_info_command(capsys):
    """Test info command."""
    result = info_command()

    assert result == 0
    captured = capsys.readouterr()
    assert 'Pandalchemy Version' in captured.out
    assert '1.0.0' in captured.out
    assert 'Features:' in captured.out


def test_cli_info_command(capsys):
    """Test info command via CLI."""
    result = main(['pandalchemy', 'info'])

    assert result == 0
    captured = capsys.readouterr()
    assert 'Pandalchemy Version' in captured.out


def test_validate_command_valid_connection(capsys, tmp_path):
    """Test validate command with valid connection."""
    # Create a temporary database
    db_path = tmp_path / "test.db"
    connection_string = f"sqlite:///{db_path}"

    # Create a table in it
    engine = create_engine(connection_string)
    with engine.begin() as conn:
        conn.execute(text("CREATE TABLE test (id INTEGER PRIMARY KEY)"))
    engine.dispose()

    result = validate_command(connection_string)

    assert result == 0
    captured = capsys.readouterr()
    assert 'Connection successful' in captured.out
    assert 'Found' in captured.out


def test_validate_command_invalid_connection(capsys):
    """Test validate command with invalid connection."""
    result = validate_command("invalid://connection/string")

    assert result == 1
    captured = capsys.readouterr()
    output = captured.out + captured.err
    assert 'failed' in output.lower() or 'error' in output.lower()


def test_cli_validate_valid(capsys, tmp_path):
    """Test validate command via CLI with valid connection."""
    db_path = tmp_path / "test.db"
    connection_string = f"sqlite:///{db_path}"

    # Create database
    engine = create_engine(connection_string)
    with engine.begin() as conn:
        conn.execute(text("CREATE TABLE test (id INTEGER)"))
    engine.dispose()

    result = main(['pandalchemy', 'validate', connection_string])

    assert result == 0


def test_cli_validate_invalid(capsys):
    """Test validate command via CLI with invalid connection."""
    result = main(['pandalchemy', 'validate', 'invalid://connection'])

    assert result == 1


def test_validate_command_with_tables(capsys, tmp_path):
    """Test validate command displays table names."""
    db_path = tmp_path / "test.db"
    connection_string = f"sqlite:///{db_path}"

    # Create database with multiple tables
    engine = create_engine(connection_string)
    with engine.begin() as conn:
        conn.execute(text("CREATE TABLE users (id INTEGER PRIMARY KEY)"))
        conn.execute(text("CREATE TABLE posts (id INTEGER PRIMARY KEY)"))
        conn.execute(text("CREATE TABLE comments (id INTEGER PRIMARY KEY)"))
    engine.dispose()

    result = validate_command(connection_string)

    assert result == 0
    captured = capsys.readouterr()
    assert 'Found 3 table' in captured.out
    assert 'users' in captured.out or 'Tables:' in captured.out


def test_validate_command_many_tables(capsys, tmp_path):
    """Test validate command with many tables (>10)."""
    db_path = tmp_path / "test.db"
    connection_string = f"sqlite:///{db_path}"

    # Create database with many tables
    engine = create_engine(connection_string)
    with engine.begin() as conn:
        for i in range(15):
            conn.execute(text(f"CREATE TABLE table_{i} (id INTEGER PRIMARY KEY)"))
    engine.dispose()

    result = validate_command(connection_string)

    assert result == 0
    captured = capsys.readouterr()
    assert 'Found 15 table' in captured.out
    assert '... and 5 more' in captured.out or 'and 5 more' in captured.out


def test_cli_info_shows_dependencies(capsys):
    """Test that info command shows dependency versions."""
    result = info_command()

    assert result == 0
    captured = capsys.readouterr()
    assert 'Pandas Version' in captured.out
    assert 'SQLAlchemy Version' in captured.out
    assert 'Python Version' in captured.out


def test_validate_empty_database(capsys, tmp_path):
    """Test validate command with empty database (no tables)."""
    db_path = tmp_path / "empty.db"
    connection_string = f"sqlite:///{db_path}"

    # Create empty database
    engine = create_engine(connection_string)
    engine.dispose()

    result = validate_command(connection_string)

    assert result == 0
    captured = capsys.readouterr()
    assert 'Found 0 table' in captured.out


def test_info_command_shows_github_link(capsys):
    """Test that info command shows GitHub link."""
    result = info_command()

    assert result == 0
    captured = capsys.readouterr()
    assert 'github' in captured.out.lower()

