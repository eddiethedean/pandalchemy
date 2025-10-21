# Example Source Files

This directory contains Python source files that are converted to Jupyter notebooks.

## Format

Python files use special comment markers to define cells:

### Markdown Cell
```python
# %% [markdown]
# # Title
# 
# This is markdown content.
# - Bullet points work
# - **Bold** and *italic* too
```

### Code Cell
```python
# %%
import pandas as pd

# Regular Python code
df = pd.DataFrame({'a': [1, 2, 3]})
df
```

## Workflow

1. **Edit** Python files in this directory (`examples_source/`)
2. **Convert** to notebooks by running:
   ```bash
   python convert_source_to_notebooks.py
   ```
3. **Result** Notebooks are created in `examples/` directory

## Advantages

- ✅ Easy to edit in any text editor
- ✅ Easy to see diffs in git
- ✅ No JSON formatting issues
- ✅ Can run as Python scripts for testing
- ✅ Clean separation of content from format

## Creating New Examples

1. Create a new file: `examples_source/##_name.py`
2. Use the cell markers (`# %%` and `# %% [markdown]`)
3. Run the converter
4. Test the notebook in Jupyter

## Tips

- In markdown cells, prefix each line with `# `
- Code cells don't need any prefix
- The last expression in a code cell will be displayed as output
- Use blank lines (`#`) in markdown for spacing
- Keep code cells focused on one concept

