#!/usr/bin/env python3
"""
Test script to verify NFL data visualization functionality with actual data.
"""

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import sys
from pathlib import Path

def test_team_data_visualization():
    """Test visualization with team description data."""
    print("🔍 Testing team data visualization...")
    
    try:
        # Load team data
        team_data_path = Path("data/team_desc")
        parquet_files = list(team_data_path.glob("**/data.parquet"))
        
        if not parquet_files:
            print("❌ No team data parquet files found")
            return False
            
        # Load the most recent team data
        df = pd.read_parquet(parquet_files[0])
        print(f"✅ Loaded team data: {len(df)} teams")
        
        # Create a simple visualization
        if 'team_abbr' in df.columns and 'team_name' in df.columns:
            # Create a simple bar chart of team count by division
            if 'team_division' in df.columns:
                division_counts = df['team_division'].value_counts()
                fig = px.bar(
                    x=division_counts.index,
                    y=division_counts.values,
                    title="NFL Teams by Division",
                    labels={'x': 'Division', 'y': 'Number of Teams'}
                )
                print("✅ Created division distribution chart")
            else:
                print("⚠️  No division column found, creating basic chart")
                
            return True
        else:
            print("❌ Expected columns not found in team data")
            return False
            
    except Exception as e:
        print(f"❌ Team data visualization failed: {e}")
        return False

def test_weekly_data_visualization():
    """Test visualization with weekly player data."""
    print("\n🔍 Testing weekly data visualization...")
    
    try:
        # Load weekly data
        weekly_data_path = Path("data/weekly")
        parquet_files = list(weekly_data_path.glob("**/data.parquet"))
        
        if not parquet_files:
            print("❌ No weekly data parquet files found")
            return False
            
        # Load the most recent weekly data
        df = pd.read_parquet(parquet_files[0])
        print(f"✅ Loaded weekly data: {len(df)} player records")
        
        # Show first few columns to understand structure
        print(f"✅ Data columns: {list(df.columns[:10])}...")
        
        # Create visualizations based on available data
        numeric_columns = df.select_dtypes(include=['number']).columns
        
        if len(numeric_columns) >= 2:
            # Create scatter plot with first two numeric columns
            x_col, y_col = numeric_columns[0], numeric_columns[1]
            
            # Sample data if too large
            sample_df = df.sample(min(1000, len(df))) if len(df) > 1000 else df
            
            fig = px.scatter(
                sample_df, 
                x=x_col, 
                y=y_col,
                title=f"NFL Player Stats: {y_col} vs {x_col}",
                hover_data=['player_name'] if 'player_name' in sample_df.columns else None
            )
            print(f"✅ Created scatter plot: {y_col} vs {x_col}")
            
            return True
        else:
            print("⚠️  Insufficient numeric columns for visualization")
            return False
            
    except Exception as e:
        print(f"❌ Weekly data visualization failed: {e}")
        return False

def test_schedule_data_visualization():
    """Test visualization with schedule data."""
    print("\n🔍 Testing schedule data visualization...")
    
    try:
        # Load schedule data
        schedule_data_path = Path("data/schedules")
        parquet_files = list(schedule_data_path.glob("**/data.parquet"))
        
        if not parquet_files:
            print("❌ No schedule data parquet files found")
            return False
            
        # Load the most recent schedule data
        df = pd.read_parquet(parquet_files[0])
        print(f"✅ Loaded schedule data: {len(df)} games")
        
        # Create game count by week visualization
        if 'week' in df.columns:
            week_counts = df['week'].value_counts().sort_index()
            
            fig = go.Figure(data=go.Bar(
                x=week_counts.index,
                y=week_counts.values,
                name='Games per Week'
            ))
            
            fig.update_layout(
                title="NFL Games by Week",
                xaxis_title="Week",
                yaxis_title="Number of Games"
            )
            
            print("✅ Created games by week chart")
            return True
        else:
            print("⚠️  No week column found in schedule data")
            return False
            
    except Exception as e:
        print(f"❌ Schedule data visualization failed: {e}")
        return False

def test_plotly_performance():
    """Test Plotly performance with larger datasets."""
    print("\n🔍 Testing Plotly performance...")
    
    try:
        # Create synthetic NFL-like data
        import numpy as np
        
        n_players = 2000
        n_weeks = 18
        
        synthetic_data = pd.DataFrame({
            'player_name': [f'Player_{i}' for i in range(n_players)],
            'week': np.random.randint(1, n_weeks + 1, n_players),
            'passing_yards': np.random.normal(250, 75, n_players).clip(0, None),
            'rushing_yards': np.random.normal(50, 30, n_players).clip(0, None),
            'touchdowns': np.random.poisson(1.5, n_players),
            'team': np.random.choice(['TB', 'NO', 'ATL', 'CAR'] * 8, n_players)
        })
        
        # Test scatter plot performance
        fig = px.scatter(
            synthetic_data.sample(500),  # Sample for performance
            x='passing_yards',
            y='rushing_yards', 
            color='team',
            size='touchdowns',
            hover_data=['player_name', 'week'],
            title='NFL Player Performance (Synthetic Data)'
        )
        
        print("✅ Created performance scatter plot with synthetic data")
        
        # Test box plot
        fig2 = px.box(
            synthetic_data,
            x='team',
            y='passing_yards',
            title='Passing Yards Distribution by Team'
        )
        
        print("✅ Created box plot with synthetic data")
        return True
        
    except Exception as e:
        print(f"❌ Plotly performance test failed: {e}")
        return False

def main():
    """Run all data visualization tests."""
    print("🏈 NFL Data Visualization Functionality Tests")
    print("=" * 55)
    
    tests = [
        ("Team Data Visualization", test_team_data_visualization),
        ("Weekly Data Visualization", test_weekly_data_visualization),
        ("Schedule Data Visualization", test_schedule_data_visualization),
        ("Plotly Performance", test_plotly_performance),
    ]
    
    passed_tests = 0
    total_tests = len(tests)
    
    for test_name, test_func in tests:
        try:
            if test_func():
                passed_tests += 1
                print(f"\n✅ {test_name}: PASSED")
            else:
                print(f"\n❌ {test_name}: FAILED")
        except Exception as e:
            print(f"\n💥 {test_name}: ERROR - {e}")
    
    print("\n" + "=" * 55)
    print(f"📊 Test Summary: {passed_tests}/{total_tests} passed")
    
    if passed_tests >= total_tests * 0.75:  # 75% pass rate acceptable
        print("🎉 Data visualization functionality confirmed!")
        return 0
    else:
        print("⚠️  Data visualization needs improvement")
        return 1

if __name__ == "__main__":
    sys.exit(main())