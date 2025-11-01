# PostgreSQL Test Expansion Opportunities

## Current Status

Currently running **46 PostgreSQL tests** ✅ (Expanded from 8!)
- 17 from `test_integration.py` (converted)
- 11 from `test_transactions.py` (converted)
- 15 from `test_schema_changes.py` (converted)
- 2 from `test_postgres_simple.py`
- 3 from `test_postgres_debug.py`  
- 2 from `example_multi_db_test.py` (parametrized)

## Potential Expansion

There are **~80+ additional tests** that could run on PostgreSQL by converting them to use the `db_engine` fixture:

### High Priority (Core Functionality)

| Test File | Tests | Current Fixture | Status |
|-----------|-------|----------------|--------|
| `test_integration.py` | **17** | `sqlite_engine` | ❌ SQLite only |
| `test_transactions.py` | **11** | `memory_db` | ❌ SQLite only |
| `test_schema_changes.py` | **15** | `memory_db` | ❌ SQLite only |
| `test_edge_cases.py` | **27** | `memory_db` | ❌ SQLite only |
| `test_sql_operations.py` | **11** | `sqlite_test_engine` | ❌ SQLite only |

**Subtotal: 81 tests**

### Medium Priority (Specialized Features)

| Test File | Tests | Current Setup | Status |
|-----------|-------|---------------|--------|
| `test_composite_pk_integration.py` | ~8 | `tmp_path` + SQLite | ❌ SQLite only |
| `test_complex_relationships.py` | ~10 | `tmp_path` + SQLite | ❌ SQLite only |
| `test_real_world_workflows.py` | ~15 | `tmp_path` + SQLite | ❌ SQLite only |
| `test_query_update_patterns.py` | ~8 | Various | ❌ SQLite only |
| `test_schema_evolution_complex.py` | ~6 | `tmp_path` + SQLite | ❌ SQLite only |
| `test_error_recovery.py` | ~5 | `tmp_path` + SQLite | ❌ SQLite only |
| `test_data_integrity.py` | ~8 | Various | ❌ SQLite only |

**Subtotal: ~60 tests**

### Lower Priority (Performance/Concurrent)

| Test File | Tests | Current Setup | Status |
|-----------|-------|---------------|--------|
| `test_performance.py` | ~5 | `tmp_path` + SQLite | ❌ SQLite only |
| `test_concurrent_scenarios.py` | ~3 | `tmp_path` + SQLite | ❌ SQLite only |

**Subtotal: ~8 tests**

## Total Potential: ~150 PostgreSQL Tests

By converting tests to use `db_engine` fixture instead of SQLite-specific fixtures, we could expand from **8 → ~150+ PostgreSQL tests**!

## Conversion Pattern

### Before (SQLite only):
```python
@pytest.fixture
def memory_db():
    engine = create_engine('sqlite:///:memory:')
    return DataBase(engine)

def test_something(memory_db):
    # test code...
```

### After (Multi-database):
```python
@pytest.mark.multidb
def test_something(db_engine):
    db = DataBase(db_engine)
    # test code... (same test logic!)
```

## Benefits

1. **Comprehensive coverage**: Test core functionality on PostgreSQL
2. **Find database-specific bugs**: Catch issues that only appear in PostgreSQL
3. **Increase confidence**: More tests = more reliable PostgreSQL support
4. **Minimal effort**: Most tests just need fixture change, not logic changes

## Recommended Conversion Order

1. ✅ **test_integration.py** (17 tests) - Core CRUD operations
2. ✅ **test_transactions.py** (11 tests) - Transaction handling  
3. ✅ **test_schema_changes.py** (15 tests) - Schema operations
4. ✅ **test_edge_cases.py** (27 tests) - Edge case handling
5. **test_sql_operations.py** (11 tests) - SQL operation helpers
6. **test_composite_pk_integration.py** (8 tests) - Composite keys
7. Other specialized tests...

