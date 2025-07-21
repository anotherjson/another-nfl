#!/usr/bin/env python3
"""Test script for Phase 3 functionality."""

import subprocess
import sys
from pathlib import Path


def run_command(command: list[str], description: str) -> bool:
    """Run a command and return success status."""
    print(f"🔍 Testing: {description}")
    print(f"   Command: {' '.join(command)}")
    
    try:
        result = subprocess.run(command, capture_output=True, text=True, check=True)
        print(f"   ✅ Success")
        return True
    except subprocess.CalledProcessError as e:
        print(f"   ❌ Failed: {e.stderr}")
        return False
    except Exception as e:
        print(f"   ❌ Error: {e}")
        return False


def test_phase3_setup():
    """Test Phase 3 setup and basic functionality."""
    print("🚀 Testing Phase 3: dbt + Dagster Integration")
    print("=" * 60)
    
    tests = [
        # Environment tests
        (["uv", "--version"], "uv package manager"),
        (["uv", "run", "python", "--version"], "Python in uv environment"),
        
        # CLI tool tests (Phase 1 & 2)
        (["uv", "run", "python", "-m", "src.cli", "--help"], "CLI tool availability"),
        (["uv", "run", "python", "-m", "src.cli", "explore", "datasets"], "Dataset listing"),
        (["uv", "run", "python", "-m", "src.cli", "extract", "status"], "Extraction status"),
        
        # dbt tests (Phase 3)
        (["ls", "dbt/dbt_project.yml"], "dbt project file"),
        (["ls", "dbt/profiles.yml"], "dbt profiles file"),
        (["ls", "dbt/models/staging"], "dbt staging models directory"),
        (["ls", "dbt/models/intermediate"], "dbt intermediate models directory"),
        (["ls", "dbt/models/marts"], "dbt marts models directory"),
        
        # Dagster tests (Phase 3)
        (["ls", "nfl_dagster/definitions.py"], "Dagster definitions file"),
        (["ls", "nfl_dagster/assets"], "Dagster assets directory"),
        (["ls", "nfl_dagster/schedules.py"], "Dagster schedules file"),
        
        # Configuration system tests
        (["uv", "run", "python", "-c", "from src.config_loader import ConfigLoader; print(f'Datasets: {len(ConfigLoader().list_datasets())}')"], "Configuration system"),
    ]
    
    passed = 0
    total = len(tests)
    
    for command, description in tests:
        if run_command(command, description):
            passed += 1
        print()
    
    print("📊 Test Results:")
    print(f"   Passed: {passed}/{total}")
    print(f"   Success Rate: {passed/total*100:.1f}%")
    
    if passed == total:
        print("🎉 All Phase 3 setup tests passed!")
        return True
    else:
        print("⚠️  Some tests failed - check setup")
        return False


def test_sample_extraction():
    """Test a simple data extraction."""
    print("\n🔬 Testing Sample Data Extraction")
    print("=" * 40)
    
    # Test team_desc extraction (always reliable)
    success = run_command([
        "uv", "run", "python", "-m", "src.cli",
        "explore", "data", "team_desc", "--limit", "3"
    ], "Sample team description data")
    
    return success


if __name__ == "__main__":
    print("NFL Data Pipeline - Phase 3 Testing")
    print("🏈 Comprehensive Production System Test")
    print()
    
    # Run main setup tests
    setup_success = test_phase3_setup()
    
    # Run sample extraction test
    extraction_success = test_sample_extraction()
    
    print("\n" + "="*60)
    if setup_success and extraction_success:
        print("🎉 Phase 3 system is ready!")
        print("✅ dbt project structure created")
        print("✅ Dagster pipeline configured") 
        print("✅ CLI tools functioning")
        print("✅ Configuration system operational")
        sys.exit(0)
    else:
        print("❌ Phase 3 setup needs attention")
        print("🔧 Check error messages above for details")
        sys.exit(1)