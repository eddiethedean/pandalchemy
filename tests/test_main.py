"""Tests for __main__ module execution."""

import subprocess
import sys


def test_main_module_execution():
    """Test that python -m pandalchemy works correctly."""
    # Run the module with --help to avoid SystemExit issues
    result = subprocess.run(
        [sys.executable, '-m', 'pandalchemy', '--help'],
        capture_output=True,
        text=True
    )

    # Should exit successfully
    assert result.returncode == 0

    # Should show help text
    assert 'pandalchemy' in result.stdout.lower()
    assert 'usage' in result.stdout.lower() or 'Pandalchemy' in result.stdout


def test_main_module_version():
    """Test that python -m pandalchemy --version works."""
    result = subprocess.run(
        [sys.executable, '-m', 'pandalchemy', '--version'],
        capture_output=True,
        text=True
    )

    assert result.returncode == 0
    assert '1.0.0' in result.stdout or '1.0.0' in result.stderr

