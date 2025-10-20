"""
Pandalchemy command-line interface.

Provides useful commands for working with pandalchemy databases.
"""

import argparse
import sys

from pandalchemy import __version__


def main(argv=None):
    """
    Main entry point for pandalchemy CLI.

    Args:
        argv: List of command-line arguments (defaults to sys.argv)

    Returns:
        int: Exit code (0 for success, non-zero for failure)
    """
    if argv is None:
        argv = sys.argv

    parser = argparse.ArgumentParser(
        prog='pandalchemy',
        description='Pandalchemy - Pandas + SQLAlchemy with Change Tracking',
        epilog='For more information, visit: https://github.com/eddiethedean/pandalchemy'
    )

    parser.add_argument(
        '--version',
        action='version',
        version=f'pandalchemy {__version__}'
    )

    subparsers = parser.add_subparsers(dest='command', help='Available commands')

    # Info command
    subparsers.add_parser(
        'info',
        help='Display information about pandalchemy installation'
    )

    # Validate command
    validate_parser = subparsers.add_parser(
        'validate',
        help='Validate a database connection string'
    )
    validate_parser.add_argument(
        'connection_string',
        help='Database connection string (e.g., sqlite:///example.db)'
    )

    args = parser.parse_args(argv[1:] if len(argv) > 1 else ['--help'])

    if args.command == 'info':
        return info_command()
    elif args.command == 'validate':
        return validate_command(args.connection_string)

    return 0


def info_command():
    """Display information about pandalchemy."""
    import pandas as pd
    import sqlalchemy

    print(f"Pandalchemy Version: {__version__}")
    print(f"Python Version: {sys.version.split()[0]}")
    print(f"Pandas Version: {pd.__version__}")
    print(f"SQLAlchemy Version: {sqlalchemy.__version__}")
    print("\nFeatures:")
    print("  ✓ Automatic change tracking")
    print("  ✓ Optimized SQL execution plans")
    print("  ✓ Transaction safety with rollback")
    print("  ✓ Schema evolution support")
    print("\nFor documentation, visit:")
    print("  https://github.com/eddiethedean/pandalchemy")

    return 0


def validate_command(connection_string: str):
    """
    Validate a database connection string.

    Args:
        connection_string: Database URL to validate

    Returns:
        int: 0 if valid, 1 if invalid
    """
    from sqlalchemy import create_engine
    from sqlalchemy.exc import SQLAlchemyError

    print(f"Validating connection: {connection_string}")

    try:
        engine = create_engine(connection_string)

        # Try to connect
        with engine.connect():
            print("✓ Connection successful!")

            # Try to get table names
            from sqlalchemy import inspect
            inspector = inspect(engine)
            tables = inspector.get_table_names()

            print(f"✓ Found {len(tables)} table(s)")
            if tables:
                print("  Tables:", ", ".join(tables[:10]))
                if len(tables) > 10:
                    print(f"  ... and {len(tables) - 10} more")

        engine.dispose()
        return 0

    except SQLAlchemyError as e:
        print(f"✗ Connection failed: {e}")
        return 1
    except Exception as e:
        print(f"✗ Error: {e}")
        return 1
