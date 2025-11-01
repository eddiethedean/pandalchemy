# Running PostgreSQL Tests

## Quick Command

To run **all PostgreSQL tests** with a single command:

```bash
pytest tests/test_postgres_simple.py tests/test_postgres_debug.py tests/example_multi_db_test.py -k postgres
```

This runs:
- All tests in PostgreSQL-specific test files
- All parametrized tests with `[postgres]` parameterization from multidb tests
- **Total: 8 PostgreSQL tests** ✅

## Alternative: Simple Filter

If you want a shorter command, you can use:

```bash
pytest -k postgres
```

**Note**: This finds tests with "postgres" in their name or nodeid, but may miss some parametrized tests when run from the root directory. For comprehensive coverage, use the command above.

## Using Markers

You can also use the postgres marker:

```bash
pytest -m postgres
```

This runs all tests marked with `@pytest.mark.postgres`, including:
- Tests using `postgres_engine` fixture  
- Tests explicitly marked with `@pytest.mark.postgres`

## Test Files

PostgreSQL tests are located in:
- `tests/test_postgres_simple.py` - Basic PostgreSQL connection and table creation
- `tests/test_postgres_debug.py` - Diagnostic tests for locks and connections
- `tests/example_multi_db_test.py` - Multi-database tests (includes PostgreSQL parameterizations)

## Examples

```bash
# Run all PostgreSQL tests (recommended - finds all 8 tests)
pytest tests/test_postgres_simple.py tests/test_postgres_debug.py tests/example_multi_db_test.py -k postgres

# Run with verbose output
pytest tests/test_postgres_simple.py tests/test_postgres_debug.py tests/example_multi_db_test.py -k postgres -v

# Shorter alternative (may miss some parametrized tests)
pytest -k postgres

# Run only tests with explicit postgres marker
pytest -m postgres

# Run specific PostgreSQL test file
pytest tests/test_postgres_simple.py -v
```

## Quick Alias

Add this to your `~/.bashrc` or `~/.zshrc` for a super short command:

```bash
alias pytest-postgres='pytest tests/test_postgres_simple.py tests/test_postgres_debug.py tests/example_multi_db_test.py -k postgres'
```

Then simply run:
```bash
pytest-postgres
```

