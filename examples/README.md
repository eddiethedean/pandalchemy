# Pandalchemy Examples

This directory contains comprehensive, runnable examples demonstrating all features of pandalchemy.

## Quick Start

Each example is a Jupyter notebook that you can run interactively:

```bash
jupyter notebook examples/01_crud_operations.ipynb
# Or open in VS Code, JupyterLab, etc.
```

All examples use in-memory SQLite databases, so they require no setup.

## Examples Overview

### Core Features

1. **[01_crud_operations.ipynb](01_crud_operations.ipynb)** - Create, Read, Update, Delete
   - Adding rows (`add_row`, `bulk_insert`)
   - Reading rows (`get_row`, `row_exists`)
   - Updating rows (`update_row`, `upsert_row`)
   - Deleting rows (`delete_row`)
   - Error handling and validation

2. **[02_change_tracking.ipynb](02_change_tracking.ipynb)** - Automatic Change Tracking
   - How tracking works
   - Getting change summaries
   - Inspecting execution plans
   - Understanding tracked operations
   - Tracker lifecycle

3. **[03_composite_primary_keys.ipynb](03_composite_primary_keys.ipynb)** - Multi-Column Primary Keys
   - Creating composite PK tables
   - Using pandas MultiIndex
   - CRUD with tuple keys
   - Many-to-many relationships
   - Querying with MultiIndex

4. **[04_auto_increment.ipynb](04_auto_increment.ipynb)** - Auto-Incrementing IDs
   - Setting up auto-increment
   - Adding rows without IDs
   - Getting next PK value
   - Mixing manual and auto IDs
   - Best practices

5. **[05_conditional_operations.ipynb](05_conditional_operations.ipynb)** - Bulk Operations
   - `update_where()` for conditional updates
   - `delete_where()` for conditional deletes
   - Boolean masks and filtering
   - Complex conditions with `&`, `|`, `~`
   - Performance optimization

6. **[06_schema_evolution.ipynb](06_schema_evolution.ipynb)** - Schema Modifications
   - Adding columns (`add_column_with_default`)
   - Dropping columns (`drop_column_safe`)
   - Renaming columns (`rename_column_safe`)
   - Changing types (`convert_column_type`)
   - Schema change workflows

7. **[07_transactions.ipynb](07_transactions.ipynb)** - Transaction Safety
   - ACID guarantees
   - Automatic rollback on errors
   - Multi-table transactions
   - Error recovery patterns
   - Validation workflows

### Advanced Features

8. **[08_index_based_primary_keys.ipynb](08_index_based_primary_keys.ipynb)** - Index as Primary Key ⭐ NEW
   - Using DataFrame index as PK
   - Automatic index naming
   - Validation and error handling
   - Index vs column-based PKs
   - Time series data

9. **[09_immutable_primary_keys.ipynb](09_immutable_primary_keys.ipynb)** - PK Immutability
   - Why PKs can't be modified
   - PK as DataFrame index
   - How to "change" a PK (delete + insert)
   - Composite PK immutability
   - Best practices

10. **[10_pandas_integration.ipynb](10_pandas_integration.ipynb)** - Full Pandas API
    - All pandas operations work
    - Filtering and selection
    - Grouping and aggregation
    - Data transformation
    - Datetime operations
    - Merging and joining

### Real-World Examples

11. **[11_real_world_ecommerce.ipynb](11_real_world_ecommerce.ipynb)** - E-Commerce System
    - Complete workflow example
    - Product catalog
    - Order processing
    - Inventory management
    - Customer analytics
    - Multi-table transactions

### Legacy Examples

- **[basic_usage.py](basic_usage.py)** - Original basic example (comprehensive)
- **[advanced_usage.py](advanced_usage.py)** - Original advanced example

## Learning Path

### Beginner
Start with these to understand the basics:
1. `01_crud_operations.py` - Learn basic operations
2. `02_change_tracking.py` - Understand how changes are tracked
3. `07_transactions.py` - Learn about transaction safety

### Intermediate
Once comfortable with basics:
4. `03_composite_primary_keys.py` - Multi-column keys
5. `04_auto_increment.py` - Automatic ID generation
6. `05_conditional_operations.py` - Bulk operations
7. `06_schema_evolution.py` - Schema changes

### Advanced
For advanced features:
8. `08_index_based_primary_keys.py` - New index-based PK feature
9. `09_immutable_primary_keys.py` - Understanding PK constraints
10. `10_pandas_integration.py` - Full pandas capabilities

### Real-World
See it all together:
11. `11_real_world_ecommerce.py` - Complete application

## Feature Quick Reference

| Feature | Example |
|---------|---------|
| CRUD Operations | 01, 11 |
| Change Tracking | 02, 07 |
| Composite PKs | 03, 09 |
| Auto-Increment | 04, 11 |
| Conditional Updates | 05, 10, 11 |
| Schema Changes | 06, 11 |
| Transactions | 07, 11 |
| Index-based PKs | 08 ⭐ NEW |
| PK Immutability | 09 |
| Pandas Integration | 10, 11 |
| Multi-table | 07, 11 |

## Running Examples

### Open Individual Notebook
```bash
# In Jupyter
jupyter notebook examples/01_crud_operations.ipynb

# In JupyterLab
jupyter lab examples/01_crud_operations.ipynb

# In VS Code
code examples/01_crud_operations.ipynb
```

### Run All Cells
Open any notebook and select "Run All" from the menu, or use keyboard shortcuts:
- **Jupyter**: Cell → Run All
- **VS Code**: Run All Cells button
- **JupyterLab**: Run → Run All Cells

### Export to Python
If you need Python scripts:
```bash
jupyter nbconvert --to python examples/01_crud_operations.ipynb
```

## Requirements

All examples use:
- Python 3.9+
- pandalchemy (installed)
- SQLite (included with Python)
- pandas (dependency of pandalchemy)
- Jupyter (for running notebooks): `pip install jupyter` or `pip install jupyterlab`

No additional setup or database configuration needed!

## Tips

- **Interactive execution**: Run cells one at a time to see each step
- **Experiment**: Modify cells and re-run to test different scenarios
- **Add your own cells**: Insert new cells to try your own code
- **Error examples**: Many examples include error handling sections showing what NOT to do
- **Restart & Run All**: Use this to verify everything works from scratch

## Contributing

Have a great example to add? Please contribute!

1. Create a new numbered notebook (e.g., `12_your_feature.ipynb`)
2. Follow the existing format:
   - Title cell (markdown) explaining the example
   - Code cells with numbered sections
   - Detailed output showing what's happening
   - Error handling demonstrations
   - Final cell with summary of key takeaways
3. Update this README with your example
4. Submit a pull request

## Questions?

- Check the main [README.md](../README.md) for API documentation
- See [CONTRIBUTING.rst](../CONTRIBUTING.rst) for contribution guidelines
- Open an issue on GitHub for bugs or questions

---

Happy coding with pandalchemy! 🐼⚗️

