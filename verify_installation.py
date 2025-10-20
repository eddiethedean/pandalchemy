#!/usr/bin/env python
"""
Verification script for pandalchemy installation.

This script verifies that all components are working correctly.
Run this after installation to ensure everything is set up properly.
"""

import sys
import traceback


def test_imports():
    """Test that all modules can be imported."""
    print("Testing imports...")
    try:
        import pandalchemy as pa
        from pandalchemy import DataBase, Table, TrackedDataFrame
        from pandalchemy import ChangeTracker, ChangeType, ExecutionPlan
        from pandalchemy import sql_ops
        print("  ✓ All imports successful")
        return True
    except ImportError as e:
        print(f"  ✗ Import failed: {e}")
        traceback.print_exc()
        return False


def test_version():
    """Test that version is correct."""
    print("\nTesting version...")
    try:
        import pandalchemy as pa
        version = pa.__version__
        print(f"  ✓ Version: {version}")
        if version >= "0.2.0":
            print("  ✓ Version check passed")
            return True
        else:
            print(f"  ✗ Version {version} is older than 0.2.0")
            return False
    except Exception as e:
        print(f"  ✗ Version check failed: {e}")
        return False


def test_basic_functionality():
    """Test basic functionality."""
    print("\nTesting basic functionality...")
    try:
        from sqlalchemy import create_engine
        import pandas as pd
        import pandalchemy as pa
        
        # Create in-memory database
        engine = create_engine('sqlite:///:memory:')
        db = pa.DataBase(engine)
        print("  ✓ Database created")
        
        # Create a table
        data = pd.DataFrame({
            'id': [1, 2, 3],
            'name': ['Alice', 'Bob', 'Charlie']
        })
        table = db.create_table('test', data, primary_key='id')
        print("  ✓ Table created")
        
        # Modify table
        db['test']['age'] = [25, 30, 35]
        print("  ✓ Column added")
        
        # Check tracking
        summary = db['test'].get_changes_summary()
        if summary['has_changes']:
            print("  ✓ Changes tracked")
        else:
            print("  ✗ Changes not tracked properly")
            return False
        
        # Push changes
        db['test'].push()
        print("  ✓ Changes pushed")
        
        # Verify
        db.pull()
        if 'age' in db['test'].columns:
            print("  ✓ Changes persisted")
        else:
            print("  ✗ Changes did not persist")
            return False
        
        print("  ✓ All functionality tests passed")
        return True
        
    except Exception as e:
        print(f"  ✗ Functionality test failed: {e}")
        traceback.print_exc()
        return False


def test_dependencies():
    """Test that all dependencies are available."""
    print("\nTesting dependencies...")
    dependencies = [
        ('pandas', '1.5.0'),
        ('sqlalchemy', '2.0.0'),
        ('numpy', '1.20.0'),
    ]
    
    all_ok = True
    for package, min_version in dependencies:
        try:
            module = __import__(package)
            version = getattr(module, '__version__', 'unknown')
            print(f"  ✓ {package} {version}")
            
            # Check version if possible
            if version != 'unknown':
                from packaging import version as pkg_version
                if pkg_version.parse(version) < pkg_version.parse(min_version):
                    print(f"    ⚠ Version {version} is older than minimum {min_version}")
        except ImportError:
            print(f"  ✗ {package} not found")
            all_ok = False
        except Exception as e:
            print(f"  ⚠ {package} check failed: {e}")
    
    # Check optional dependencies
    optional = ['fullmetalalchemy', 'transmutation']
    for package in optional:
        try:
            __import__(package)
            print(f"  ✓ {package} (optional)")
        except ImportError:
            print(f"  ⚠ {package} (optional) not found - some features may not work")
    
    return all_ok


def main():
    """Run all verification tests."""
    print("=" * 60)
    print("Pandalchemy Installation Verification")
    print("=" * 60)
    
    results = []
    
    # Run tests
    results.append(("Imports", test_imports()))
    results.append(("Version", test_version()))
    results.append(("Dependencies", test_dependencies()))
    results.append(("Functionality", test_basic_functionality()))
    
    # Summary
    print("\n" + "=" * 60)
    print("Verification Summary")
    print("=" * 60)
    
    for name, passed in results:
        status = "✓ PASS" if passed else "✗ FAIL"
        print(f"{status:8} {name}")
    
    all_passed = all(result[1] for result in results)
    
    print("=" * 60)
    if all_passed:
        print("✓ All verification tests passed!")
        print("Pandalchemy is correctly installed and ready to use.")
        return 0
    else:
        print("✗ Some verification tests failed.")
        print("Please check the output above for details.")
        return 1


if __name__ == '__main__':
    sys.exit(main())

