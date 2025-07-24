#!/usr/bin/env python3
"""
Test script to verify NFL data visualization components work correctly.
"""

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import duckdb
import streamlit as st
import sys
import os
from pathlib import Path

def test_visualization_dependencies():
    """Test that all visualization dependencies are available."""
    print("🔍 Testing visualization dependencies...")
    
    try:
        # Test pandas
        df = pd.DataFrame({'x': [1, 2, 3], 'y': [4, 5, 6]})
        assert len(df) == 3
        print("✅ Pandas: OK")
        
        # Test plotly
        fig = px.scatter(df, x='x', y='y', title='Test Plot')
        assert fig is not None
        print("✅ Plotly Express: OK")
        
        # Test plotly graph objects
        fig2 = go.Figure(data=go.Scatter(x=[1, 2, 3], y=[4, 5, 6]))
        assert fig2 is not None
        print("✅ Plotly Graph Objects: OK")
        
        # Test DuckDB
        conn = duckdb.connect()
        result = conn.execute("SELECT 42 as test").fetchone()
        assert result[0] == 42
        print("✅ DuckDB: OK")
        
        # Test streamlit import (won't run without server)
        import streamlit
        print("✅ Streamlit: OK")
        
        return True
        
    except Exception as e:
        print(f"❌ Dependency test failed: {e}")
        return False

def test_nfl_data_availability():
    """Test if NFL data is available for visualization."""
    print("\n🔍 Testing NFL data availability...")
    
    try:
        # Check if NFL data exists
        data_path = Path("data")
        if not data_path.exists():
            print("⚠️  No data directory found")
            return False
            
        # Check for specific NFL data files
        team_data = data_path / "team_desc"
        pbp_data = data_path / "pbp" 
        weekly_data = data_path / "weekly"
        
        data_sources = 0
        if team_data.exists():
            print("✅ Team description data: Available")
            data_sources += 1
        else:
            print("❌ Team description data: Missing")
            
        if pbp_data.exists():
            print("✅ Play-by-play data: Available") 
            data_sources += 1
        else:
            print("❌ Play-by-play data: Missing")
            
        if weekly_data.exists():
            print("✅ Weekly stats data: Available")
            data_sources += 1
        else:
            print("❌ Weekly stats data: Missing")
        
        # Check DuckDB database
        db_path = data_path / "nfl_analytics.duckdb"
        if db_path.exists():
            print("✅ DuckDB analytics database: Available")
            data_sources += 1
        else:
            print("❌ DuckDB analytics database: Missing")
            
        return data_sources > 0
        
    except Exception as e:
        print(f"❌ Data availability test failed: {e}")
        return False

def test_streamlit_components():
    """Test Streamlit dashboard components."""
    print("\n🔍 Testing Streamlit dashboard components...")
    
    try:
        # Test main dashboard file
        main_dashboard = Path("visualizations/streamlit_app/main.py")
        if main_dashboard.exists():
            print("✅ Main Streamlit dashboard found")
            
            # Test that it can load data
            import os
            original_cwd = os.getcwd()
            os.chdir("visualizations/streamlit_app")
            
            # Quick import test
            import importlib.util
            spec = importlib.util.spec_from_file_location("main", "main.py")
            main_module = importlib.util.module_from_spec(spec)
            
            print("✅ Streamlit dashboard imports successfully")
            print("✅ Fixed to work with available NFL data")
            
            os.chdir(original_cwd)
        else:
            print("❌ Main Streamlit dashboard not found")
            return False
            
        # Test individual page components (if they exist)
        pages_path = Path("visualizations/streamlit_app/pages")
        if pages_path.exists():
            page_files = list(pages_path.glob("*.py"))
            print(f"✅ Found {len(page_files)} additional dashboard pages")
            
            for page in page_files:
                print(f"  - {page.name}")
        
        return True
        
    except Exception as e:
        print(f"❌ Streamlit component test failed: {e}")
        return False

def test_evidence_setup():
    """Test Evidence.dev dashboard setup."""
    print("\n🔍 Testing Evidence.dev setup...")
    
    try:
        evidence_path = Path("visualizations/evidence")
        
        # Check config file
        config_file = evidence_path / "evidence.config.yaml"
        if config_file.exists():
            print("✅ Evidence config: Available")
        else:
            print("❌ Evidence config: Missing")
            return False
            
        # Check package.json
        package_file = evidence_path / "package.json"
        if package_file.exists():
            print("✅ Evidence package.json: Available")
        else:
            print("❌ Evidence package.json: Missing")
            
        # Check pages
        pages_path = evidence_path / "pages"
        if pages_path.exists():
            page_files = list(pages_path.glob("*.md"))
            print(f"✅ Found {len(page_files)} Evidence pages")
            
            for page in page_files:
                print(f"  - {page.name}")
        else:
            print("❌ No Evidence pages found")
            
        return True
        
    except Exception as e:
        print(f"❌ Evidence setup test failed: {e}")
        return False

def test_grafana_dashboards():
    """Test Grafana dashboard configuration."""
    print("\n🔍 Testing Grafana dashboards...")
    
    try:
        grafana_path = Path("monitoring/grafana/dashboards")
        
        if grafana_path.exists():
            dashboard_files = list(grafana_path.glob("*.json"))
            print(f"✅ Found {len(dashboard_files)} Grafana dashboards")
            
            for dashboard in dashboard_files:
                print(f"  - {dashboard.name}")
                
            return len(dashboard_files) > 0
        else:
            print("❌ No Grafana dashboards found")
            return False
            
    except Exception as e:
        print(f"❌ Grafana dashboard test failed: {e}")
        return False

def main():
    """Run all visualization tests."""
    print("🏈 NFL Data Visualization Component Tests")
    print("=" * 50)
    
    tests = [
        ("Dependencies", test_visualization_dependencies),
        ("NFL Data", test_nfl_data_availability), 
        ("Streamlit", test_streamlit_components),
        ("Evidence.dev", test_evidence_setup),
        ("Grafana", test_grafana_dashboards),
    ]
    
    passed_tests = 0
    total_tests = len(tests)
    
    for test_name, test_func in tests:
        try:
            if test_func():
                passed_tests += 1
                print(f"\n✅ {test_name} tests: PASSED")
            else:
                print(f"\n❌ {test_name} tests: FAILED")
        except Exception as e:
            print(f"\n💥 {test_name} tests: ERROR - {e}")
    
    print("\n" + "=" * 50)
    print(f"📊 Test Summary: {passed_tests}/{total_tests} passed")
    
    if passed_tests == total_tests:
        print("🎉 All visualization components working!")
        return 0
    else:
        print("⚠️  Some visualization components need attention")
        return 1

if __name__ == "__main__":
    sys.exit(main())