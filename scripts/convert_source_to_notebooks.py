#!/usr/bin/env python
"""Convert Python source files with cell markers to Jupyter notebooks."""

import json
import re
from pathlib import Path


def parse_source_file(filepath):
    """Parse a Python file with %% cell markers into cells."""
    with open(filepath, 'r') as f:
        content = f.read()
    
    cells = []
    current_cell_type = None
    current_cell_lines = []
    
    for line in content.split('\n'):
        # Check for cell markers
        if line.strip() == '# %%':
            # Save previous cell
            if current_cell_lines:
                cells.append(create_cell(current_cell_type, current_cell_lines))
                current_cell_lines = []
            current_cell_type = 'code'
        elif line.strip() == '# %% [markdown]':
            # Save previous cell
            if current_cell_lines:
                cells.append(create_cell(current_cell_type, current_cell_lines))
                current_cell_lines = []
            current_cell_type = 'markdown'
        else:
            # Add line to current cell
            current_cell_lines.append(line)
    
    # Save last cell
    if current_cell_lines:
        cells.append(create_cell(current_cell_type, current_cell_lines))
    
    return cells


def create_cell(cell_type, lines):
    """Create a notebook cell from lines."""
    cell = {
        "cell_type": cell_type,
        "metadata": {}
    }
    
    if cell_type == 'code':
        cell["execution_count"] = None
        cell["outputs"] = []
        
        # Join lines and split again to handle properly
        content = '\n'.join(lines).strip()
        if content:
            # Split into lines for source, keeping newlines except on last line
            source_lines = content.split('\n')
            cell["source"] = [line + '\n' for line in source_lines[:-1]]
            if source_lines:
                cell["source"].append(source_lines[-1])  # Last line without newline
        else:
            cell["source"] = []
    
    elif cell_type == 'markdown':
        # Remove leading '# ' from markdown lines
        cleaned_lines = []
        for line in lines:
            if line.startswith('# '):
                cleaned_lines.append(line[2:])
            elif line.startswith('#'):
                cleaned_lines.append(line[1:])
            else:
                cleaned_lines.append(line)
        
        content = '\n'.join(cleaned_lines).strip()
        if content:
            source_lines = content.split('\n')
            cell["source"] = [line + '\n' for line in source_lines[:-1]]
            if source_lines:
                cell["source"].append(source_lines[-1])  # Last line without newline
        else:
            cell["source"] = []
    
    return cell


def python_to_notebook(source_file, output_file):
    """Convert a Python source file to a Jupyter notebook."""
    cells = parse_source_file(source_file)
    
    notebook = {
        "cells": cells,
        "metadata": {
            "kernelspec": {
                "display_name": "Python 3",
                "language": "python",
                "name": "python3"
            },
            "language_info": {
                "codemirror_mode": {
                    "name": "ipython",
                    "version": 3
                },
                "file_extension": ".py",
                "mimetype": "text/x-python",
                "name": "python",
                "nbconvert_exporter": "python",
                "pygments_lexer": "ipython3",
                "version": "3.9.0"
            }
        },
        "nbformat": 4,
        "nbformat_minor": 4
    }
    
    with open(output_file, 'w') as f:
        json.dump(notebook, f, indent=2)


def main():
    source_dir = Path('examples_source')
    output_dir = Path('examples')
    
    if not source_dir.exists():
        print(f"Error: {source_dir} directory not found")
        return
    
    output_dir.mkdir(exist_ok=True)
    
    # Find all Python files in source directory
    source_files = sorted(source_dir.glob('*.py'))
    
    print("=" * 70)
    print("Converting Python Sources to Notebooks")
    print("=" * 70)
    
    for source_file in source_files:
        output_file = output_dir / source_file.with_suffix('.ipynb').name
        
        try:
            python_to_notebook(source_file, output_file)
            
            # Count cells
            with open(output_file) as f:
                nb = json.load(f)
            
            print(f"✓ {source_file.name} → {output_file.name} ({len(nb['cells'])} cells)")
        except Exception as e:
            print(f"✗ {source_file.name}: {e}")
    
    print("=" * 70)
    print(f"Converted {len(source_files)} files")
    print("=" * 70)


if __name__ == '__main__':
    main()

