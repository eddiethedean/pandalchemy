# Test Database Setup

To run all tests (including PostgreSQL and MySQL), you need to set up test databases.

## Option 1: Using Docker (Recommended)

1. **Start Docker Desktop** (if not already running)

2. **Start test databases:**
   ```bash
   docker-compose -f docker-compose.test.yml up -d
   ```

3. **Wait for databases to be ready** (about 10-15 seconds)

4. **Set environment variables:**
   ```bash
   export TEST_POSTGRES_URL="postgresql://test:test@localhost:5432/testdb"
   export TEST_MYSQL_URL="mysql+pymysql://test:test@localhost:3306/testdb"
   ```

5. **Run tests:**
   ```bash
   pytest tests/ -n 4
   ```

6. **Stop databases when done:**
   ```bash
   docker-compose -f docker-compose.test.yml down
   ```

## Option 2: Using the Setup Script

Run the automated setup script:

```bash
./setup-test-dbs.sh
```

This will start the databases and show you the environment variables to export.

## Option 3: Using Existing Databases

If you already have PostgreSQL and MySQL running, just set the environment variables:

```bash
export TEST_POSTGRES_URL="postgresql://user:password@localhost:5432/dbname"
export TEST_MYSQL_URL="mysql+pymysql://user:password@localhost:3306/dbname"
```

## Installing Required Drivers

The database drivers are already included in the dev dependencies:

- `psycopg2-binary` - PostgreSQL driver
- `pymysql` - MySQL driver
- `asyncpg` - PostgreSQL async driver
- `aiomysql` - MySQL async driver

Install them with:
```bash
pip install -e ".[dev]"
```

## Quick Test

To verify your databases are set up correctly:

```bash
python -c "
from sqlalchemy import create_engine, text
import os

# Test PostgreSQL
if os.environ.get('TEST_POSTGRES_URL'):
    engine = create_engine(os.environ['TEST_POSTGRES_URL'])
    with engine.connect() as conn:
        result = conn.execute(text('SELECT 1'))
        print('✓ PostgreSQL connected:', result.scalar())
    engine.dispose()

# Test MySQL
if os.environ.get('TEST_MYSQL_URL'):
    engine = create_engine(os.environ['TEST_MYSQL_URL'])
    with engine.connect() as conn:
        result = conn.execute(text('SELECT 1'))
        print('✓ MySQL connected:', result.scalar())
    engine.dispose()
"
```

## Troubleshooting

- **Docker not running**: Start Docker Desktop
- **Port conflicts**: Change ports in `docker-compose.test.yml` (5432 for PostgreSQL, 3306 for MySQL)
- **Connection refused**: Wait a bit longer for databases to start (they need 10-15 seconds)
- **Authentication failed**: Check that the credentials in your URLs match the docker-compose configuration

