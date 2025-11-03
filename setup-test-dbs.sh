#!/bin/bash
# Setup script for test databases

set -e

echo "Setting up test databases..."

# Check if docker is available
if ! command -v docker &> /dev/null; then
    echo "Error: Docker is not installed. Please install Docker to run test databases."
    echo "Alternatively, set TEST_POSTGRES_URL and TEST_MYSQL_URL environment variables."
    exit 1
fi

# Start databases using docker-compose
echo "Starting PostgreSQL and MySQL containers..."
docker-compose -f docker-compose.test.yml up -d

# Wait for databases to be ready
echo "Waiting for databases to be ready..."
sleep 5

# Check if PostgreSQL is ready
until docker-compose -f docker-compose.test.yml exec -T postgres pg_isready -U test > /dev/null 2>&1; do
    echo "Waiting for PostgreSQL..."
    sleep 2
done

# Check if MySQL is ready
until docker-compose -f docker-compose.test.yml exec -T mysql mysqladmin ping -h localhost -u test -ptest > /dev/null 2>&1; do
    echo "Waiting for MySQL..."
    sleep 2
done

echo "✓ Databases are ready!"

# Export environment variables
export TEST_POSTGRES_URL="postgresql://test:test@localhost:5432/testdb"
export TEST_MYSQL_URL="mysql+pymysql://test:test@localhost:3306/testdb"

echo ""
echo "Database URLs:"
echo "  TEST_POSTGRES_URL=$TEST_POSTGRES_URL"
echo "  TEST_MYSQL_URL=$TEST_MYSQL_URL"
echo ""
echo "To use these in your current shell, run:"
echo "  export TEST_POSTGRES_URL='$TEST_POSTGRES_URL'"
echo "  export TEST_MYSQL_URL='$TEST_MYSQL_URL'"
echo ""
echo "Or run tests with:"
echo "  TEST_POSTGRES_URL='$TEST_POSTGRES_URL' TEST_MYSQL_URL='$TEST_MYSQL_URL' pytest tests/ -n 4"
echo ""
echo "To stop databases, run:"
echo "  docker-compose -f docker-compose.test.yml down"

