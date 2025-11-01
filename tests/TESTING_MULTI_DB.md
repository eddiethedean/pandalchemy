# Testing pandalchemy on Multiple Databases

pandalchemy supports testing across multiple database types (SQLite, PostgreSQL, MySQL) to ensure compatibility.

## Quick Start

### 1. Install Database Drivers

```bash
pip install -r requirements-dev.txt
```

This installs:
- `testing.postgresql` for PostgreSQL (creates isolated instances)
- `testing.mysqld` for MySQL (creates isolated instances)
- `psycopg2-binary` for PostgreSQL
- `pymysql` for MySQL

### 2. Set Up Database Connections

#### PostgreSQL

**Using testing.postgresql (Recommended)**

The `postgres_engine` fixture uses `testing.postgresql` which automatically creates isolated PostgreSQL instances for each test. No configuration needed!

```bash
pip install testing.postgresql
```

The fixture will automatically:
- Create a temporary PostgreSQL instance per test
- Clean up after each test
- Provide complete isolation between tests

**Alternative: Manual PostgreSQL (if testing.postgresql not available)**

Set the `TEST_POSTGRES_URL` environment variable:

```bash
export TEST_POSTGRES_URL="postgresql://user:password@localhost/testdb"
```

#### MySQL

**Using testing.mysqld (Recommended)**

The `mysql_engine` fixture uses `testing.mysqld` which automatically creates isolated MySQL instances for each test. No configuration needed!

```bash
pip install testing.mysqld
```

The fixture will automatically:
- Create a temporary MySQL instance per test
- Clean up after each test
- Provide complete isolation between tests

**Alternative: Manual MySQL (if testing.mysqld not available)**

Set the `TEST_MYSQL_URL` environment variable:

```bash
export TEST_MYSQL_URL="mysql+pymysql://user:password@localhost/testdb"
```

### 3. Run Tests

#### Run tests on all available databases

Tests using the `db_engine` fixture will automatically run on all configured databases:

```bash
# Test all databases (SQLite, and PostgreSQL/MySQL if configured)
pytest tests/ -k "your_test_pattern"
```

#### Run tests on specific database types

```bash
# Only SQLite (default)
pytest tests/

# Only PostgreSQL (requires TEST_POSTGRES_URL)
pytest -m postgres tests/

# Only MySQL (uses testing.mysqld if available, or TEST_MYSQL_URL)
pytest -m mysql tests/

# Multiple database types
pytest -m "postgres or mysql" tests/
```

## Available Fixtures

### `db_engine` (Parametrized)

**Recommended for most tests.** Automatically runs tests on all available database types.

```python
def test_basic_operations(db_engine):
    """This test will run on SQLite, PostgreSQL, and MySQL (if configured)."""
    db = DataBase(db_engine)
    table = db.create_table('users', df, 'id')
    table.push()
    # ... test code ...
```

### `sqlite_engine`

Create a temporary SQLite database (file-based).

```python
def test_sqlite_specific(sqlite_engine):
    """Test that requires file-based SQLite."""
    db = DataBase(sqlite_engine)
    # ... test code ...
```

### `memory_engine`

Create an in-memory SQLite database (fastest for simple tests).

```python
def test_memory_db(memory_engine):
    """Test using in-memory SQLite."""
    db = DataBase(memory_engine)
    # ... test code ...
```

### `postgres_engine`

Create a PostgreSQL engine (skips if `TEST_POSTGRES_URL` not set).

```python
@pytest.mark.postgres
def test_postgres_specific(postgres_engine):
    """Test specific to PostgreSQL features."""
    db = DataBase(postgres_engine)
    # ... test code ...
```

### `mysql_engine`

Create a MySQL engine using `testing.mysqld` (creates isolated instance per test, no configuration needed).

```python
@pytest.mark.mysql
def test_mysql_specific(mysql_engine):
    """Test specific to MySQL features."""
    db = DataBase(mysql_engine)
    # ... test code ...
```

## Using Docker for Local Testing

### PostgreSQL

```bash
# Start PostgreSQL container
docker run --name test-postgres \
  -e POSTGRES_PASSWORD=testpass \
  -e POSTGRES_DB=testdb \
  -p 5432:5432 \
  -d postgres:15

# Set connection URL
export TEST_POSTGRES_URL="postgresql://postgres:testpass@localhost:5432/testdb"

# Run tests
pytest -m postgres tests/
```

### MySQL

```bash
# Start MySQL container
docker run --name test-mysql \
  -e MYSQL_ROOT_PASSWORD=testpass \
  -e MYSQL_DATABASE=testdb \
  -p 3306:3306 \
  -d mysql:8.0

# Set connection URL
export TEST_MYSQL_URL="mysql+pymysql://root:testpass@localhost:3306/testdb"

# Run tests
pytest -m mysql tests/
```

### Docker Compose (All Databases)

Create `docker-compose.test.yml`:

```yaml
version: '3.8'
services:
  postgres:
    image: postgres:15
    environment:
      POSTGRES_PASSWORD: testpass
      POSTGRES_DB: testdb
    ports:
      - "5432:5432"
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U postgres"]
      interval: 5s
      timeout: 5s
      retries: 5

  mysql:
    image: mysql:8.0
    environment:
      MYSQL_ROOT_PASSWORD: testpass
      MYSQL_DATABASE: testdb
    ports:
      - "3306:3306"
    healthcheck:
      test: ["CMD", "mysqladmin", "ping", "-h", "localhost"]
      interval: 5s
      timeout: 5s
      retries: 5
```

Run:

```bash
docker-compose -f docker-compose.test.yml up -d

export TEST_POSTGRES_URL="postgresql://postgres:testpass@localhost:5432/testdb"
export TEST_MYSQL_URL="mysql+pymysql://root:testpass@localhost:3306/testdb"

pytest tests/
```

## CI/CD Integration

### GitHub Actions Example

```yaml
name: Tests

on: [push, pull_request]

jobs:
  test:
    runs-on: ubuntu-latest
    strategy:
      matrix:
        database: [sqlite, postgres, mysql]
    
    services:
      postgres:
        image: postgres:15
        env:
          POSTGRES_PASSWORD: testpass
          POSTGRES_DB: testdb
        ports:
          - 5432:5432
        options: >-
          --health-cmd pg_isready
          --health-interval 10s
          --health-timeout 5s
          --health-retries 5
      
      mysql:
        image: mysql:8.0
        env:
          MYSQL_ROOT_PASSWORD: testpass
          MYSQL_DATABASE: testdb
        ports:
          - 3306:3306
        options: >-
          --health-cmd "mysqladmin ping"
          --health-interval 10s
          --health-timeout 5s
          --health-retries 5

    steps:
      - uses: actions/checkout@v3
      - uses: actions/setup-python@v4
        with:
          python-version: '3.9'
      
      - name: Install dependencies
        run: |
          pip install -r requirements-dev.txt
      
      - name: Set database URLs
        run: |
          if [ "${{ matrix.database }}" == "postgres" ]; then
            echo "TEST_POSTGRES_URL=postgresql://postgres:testpass@localhost:5432/testdb" >> $GITHUB_ENV
          elif [ "${{ matrix.database }}" == "mysql" ]; then
            echo "TEST_MYSQL_URL=mysql+pymysql://root:testpass@localhost:3306/testdb" >> $GITHUB_ENV
          fi
      
      - name: Run tests
        run: |
          if [ "${{ matrix.database }}" == "sqlite" ]; then
            pytest tests/ -m "not postgres and not mysql"
          elif [ "${{ matrix.database }}" == "postgres" ]; then
            pytest tests/ -m postgres
          else
            pytest tests/ -m mysql
          fi
```

## Best Practices

1. **Use `db_engine` fixture** for tests that should work on all databases
2. **Use specific fixtures** (`postgres_engine`, `mysql_engine`) only for database-specific features
3. **Mark database-specific tests** with `@pytest.mark.postgres` or `@pytest.mark.mysql`
4. **Clean up is automatic** - fixtures drop all tables after each test
5. **Skip gracefully** - tests skip automatically if database is not available

## Troubleshooting

### Connection Errors

- Verify database is running: `pg_isready` or `mysqladmin ping`
- Check connection string format (see SQLAlchemy docs)
- Ensure firewall allows connections to database ports

### Import Errors

- Install drivers: `pip install psycopg2-binary pymysql`
- For PostgreSQL on Windows, you may need `psycopg2` instead of `psycopg2-binary`

### Permission Errors

- Ensure test database user has CREATE/DROP table permissions
- For MySQL, ensure user has privileges on test database

## Example Test Migration

### Before (SQLite only)

```python
def test_user_operations(sqlite_engine):
    db = DataBase(sqlite_engine)
    # ... test code ...
```

### After (Multi-database)

```python
def test_user_operations(db_engine):
    """Test runs on SQLite, PostgreSQL, and MySQL."""
    db = DataBase(db_engine)
    # ... test code ...
```

Tests will automatically run once per available database type!

