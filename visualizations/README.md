# NFL Data Visualization Layer

## Overview

The visualization layer provides comprehensive NFL analytics through multiple dashboard platforms:

- **Streamlit**: Interactive Python-based analytics application
- **Evidence**: SQL-based dashboard-as-code framework  
- **Grafana**: Executive-level monitoring and KPI dashboards

## Architecture

```
NFL Data → dbt Models → DuckDB → Visualization Layer
                  ↓
    Streamlit + Evidence + Grafana Dashboards
```

## Deployment

### Development
```bash
# Install dependencies
uv sync --dev

# Start Streamlit (development)
uv run streamlit run visualizations/streamlit_app/main.py

# Start Evidence (development) 
cd visualizations/evidence
npm install
npm run dev
```

### Production (Docker)
```bash
# Start all visualization services
docker-compose up -d streamlit-app evidence-app grafana

# Access dashboards
# Streamlit: http://localhost:8501
# Evidence: http://localhost:3002  
# Grafana: http://localhost:3001
```

## Dashboard Features

### Streamlit Analytics App
- **Interactive filtering** by team, week, position
- **Real-time data** connections to DuckDB
- **Fantasy football** analytics and projections
- **Player performance** trends and comparisons
- **Team efficiency** analysis with EPA metrics

### Evidence Dashboard-as-Code
- **SQL-based** report generation
- **Version controlled** dashboard definitions
- **Automatic documentation** from dbt models
- **Executive reporting** with scheduled exports
- **Mobile responsive** layouts

### Grafana Executive Dashboards
- **High-level KPIs** and team performance metrics
- **Real-time monitoring** integration with existing stack
- **Alert management** for data quality issues
- **Custom NFL-themed** dashboards and panels

## Data Sources

All dashboards connect to the same data sources:
- **PostgreSQL**: DuckLake catalog and metadata
- **DuckDB**: Analytics database with dbt models
- **dbt Models**: Staging, intermediate, and marts layers

## Key Metrics Available

### Team Performance
- Expected Points Added (EPA) by team/week
- Pass vs Rush efficiency analysis
- Win probability and game state metrics
- Weekly rankings and consistency scores

### Player Analytics  
- Fantasy points (standard and PPR scoring)
- Target share and reception metrics
- Position-specific performance rankings
- Rookie and veteran comparisons

### Game Analysis
- Betting line analysis and weather impact
- Home field advantage metrics
- Point spread and over/under trends
- Situational performance by game state

## Development Guidelines

### Adding New Visualizations

1. **Streamlit Pages**: Add to `visualizations/streamlit_app/pages/`
2. **Evidence Reports**: Add to `visualizations/evidence/pages/`
3. **Grafana Dashboards**: Export JSON to `monitoring/grafana/dashboards/`

### Data Query Patterns
```python
# Streamlit - Use caching for performance
@st.cache_data
def load_team_data():
    conn = duckdb.connect(db_path, read_only=True)
    return conn.execute(query).df()
```

```sql
-- Evidence - Direct SQL in markdown
SELECT team, AVG(total_epa) as avg_epa
FROM mart_weekly_team_stats  
WHERE season = 2023
GROUP BY team
```

### Performance Optimization
- Use **read-only** database connections
- Implement **appropriate caching** strategies  
- **Limit data ranges** with date/week filters
- **Aggregate data** appropriately for visualizations

## Monitoring and Maintenance

### Dagster Integration
Visualization assets are integrated into the Dagster pipeline:
- `streamlit_dashboard_refresh`: Health checks and cache management
- `evidence_dashboard_build`: Automated report generation  
- `dashboard_data_snapshots`: Pre-computed summary tables
- `dashboard_data_quality_check`: Data validation and alerts

### Health Monitoring
```bash
# Check dashboard status
curl -f http://localhost:8501/_stcore/health    # Streamlit
curl -f http://localhost:3002/health            # Evidence  
curl -f http://localhost:3001/api/health        # Grafana
```

### Troubleshooting
- **Connection issues**: Verify database connectivity and credentials
- **Performance problems**: Check query performance and caching
- **Data inconsistencies**: Run dbt tests and data quality checks
- **Dashboard errors**: Check application logs and error handling

## Future Enhancements

### Planned Features
- **Real-time data** streaming for live game analysis
- **Machine learning** model integration and predictions
- **Advanced analytics** with player tracking data
- **Mobile apps** for fantasy and betting insights
- **API endpoints** for external integrations

### Technical Improvements
- **Authentication** and role-based access control
- **Performance monitoring** with detailed metrics
- **A/B testing** framework for dashboard optimization
- **Custom themes** and white-label capabilities