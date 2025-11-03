# Async Testing Status and Workaround

## Current Status

✅ **pytest-green-light integrated** - Added to dev dependencies and configured  
⚠️ **Greenlet context issue persists** - Tests still fail with `MissingGreenlet` errors  
✅ **Workarounds implemented** - Greenlet context established in all async methods  
✅ **Issue documented** - Created detailed report for pytest-green-light

## Problem

The `pytest-green-light` plugin's autouse fixture `ensure_greenlet_context` runs and establishes greenlet context, but this context does **not persist** to the async context where SQLAlchemy async engines actually make connections.

### Root Cause

SQLAlchemy's async engines require greenlet context to be established in the **exact same async context** where connections are made. The issue is:

1. `pytest-green-light`'s fixture runs in its own async context (fixture lifecycle)
2. When test functions execute `async with engine.begin()`, they run in a different async context
3. Greenlet context from fixture's context doesn't carry over

### Evidence

- Manual tests with `asyncio.run()` work perfectly ✅
- Tests with `pytest-asyncio` fail with `MissingGreenlet` ❌
- The plugin fixture runs (visible in `--setup-show`) but context is lost

## Current Workaround

We've added greenlet context establishment in all async methods that make database connections:

### Files Modified

1. **`src/pandalchemy/async_operations.py`**:
   - Added `_ensure_greenlet_context()` helper function
   - Called right before `async with engine.begin()` in:
     - `async_pull_table()`
     - `async_execute_plan()`

2. **`src/pandalchemy/async_base.py`**:
   - Called `_ensure_greenlet_context()` in:
     - `AsyncTableDataFrame.push()`
     - `AsyncTableDataFrame.pull()`
     - `AsyncDataBase.load_tables()`
     - `AsyncDataBase.push()`

### Workaround Code

```python
async def _ensure_greenlet_context() -> None:
    """
    Ensure greenlet context is established before async database operations.
    
    NOTE: This is a workaround for pytest-green-light not properly persisting
    greenlet context from its fixture to the test's async context. Once
    pytest-green-light is fixed, this can be removed.
    """
    try:
        from sqlalchemy.util._concurrency_py3k import greenlet_spawn
    except ImportError:
        try:
            from sqlalchemy.util import greenlet_spawn
        except ImportError:
            return
    
    if greenlet_spawn is not None:
        def _noop() -> None:
            pass
        await greenlet_spawn(_noop)
```

## Why Workaround Still Fails

Even with workarounds in place, tests still fail. This suggests the issue is deeper:

1. **Event Loop Context**: pytest-asyncio might be creating new event loops or contexts that don't preserve greenlet context
2. **Timing Issue**: Greenlet context might need to be established at a more specific moment (during connection creation, not before)
3. **Multiple Contexts**: The async call chain (`load_tables()` → `pull()` → `async_pull_table()` → `engine.begin()`) might be crossing async context boundaries

## Next Steps

### Option 1: Fix pytest-green-light (Recommended)
The proper fix should be in `pytest-green-light` to establish greenlet context in the same async context where tests run. See `GREENLET_CONTEXT_ISSUE.md` in the pytest-green-light repository for detailed analysis and proposed solutions.

### Option 2: Investigate Deeper
Research if there's a way to establish greenlet context that persists across pytest-asyncio's event loop management, or if we need to wrap the entire test execution differently.

### Option 3: Alternative Approach
Consider using a different async testing strategy or accepting that async tests need manual greenlet context establishment in production code.

## Related Documentation

- **Issue Report**: `/pytest-sqlalchemy-async-greenlet/GREENLET_CONTEXT_ISSUE.md`
- **Plugin Status**: `pytest-green-light` is installed and loaded, but context persistence doesn't work
- **SQLAlchemy Docs**: https://docs.sqlalchemy.org/en/20/orm/extensions/asyncio.html

## Test Status

```
FAILED tests/test_async.py::test_async_database_initialization[asyncio]
FAILED tests/test_async.py::test_async_table_creation_and_push[asyncio]
FAILED tests/test_async.py::test_async_table_modification_and_push[asyncio]
FAILED tests/test_async.py::test_async_table_pull[asyncio]
FAILED tests/test_async.py::test_async_insert_and_delete[asyncio]
FAILED tests/test_async.py::test_async_database_push_parallel[asyncio]
FAILED tests/test_async.py::test_async_table_with_conflict_resolution[asyncio]
FAILED tests/test_async.py::test_async_table_dataframe_pandas_operations[asyncio]
```

All 8 async tests fail with `MissingGreenlet` errors, despite workarounds.

