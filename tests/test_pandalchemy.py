
from pandalchemy.cli import main


def test_main():
    """Test CLI main function."""
    # CLI now shows help if no command given
    # It will call sys.exit, so we need to handle that
    import pytest
    with pytest.raises(SystemExit) as exc_info:
        main([])
    # Help returns exit code 0
    assert exc_info.value.code == 0
