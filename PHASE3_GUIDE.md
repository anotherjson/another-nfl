# Phase 3 Guide: dbt + Dagster Integration

## 🎉 Phase 3 Complete!

The NFL Data Pipeline now includes a comprehensive data warehouse with dbt transformations and Dagster orchestration.

## What's New in Phase 3

### dbt Data Warehouse
- **Staging Models**: Clean and standardize raw NFL data
- **Intermediate Models**: Business logic and team/player aggregations  
- **Marts Models**: Analytics-ready tables for dashboards and ML
- **Testing**: Data quality tests and validation rules
- **Documentation**: Comprehensive model documentation

### Dagster Orchestration
- **Asset Management**: All data assets managed as Dagster assets
- **Scheduling**: Automated weekly and monthly data pipelines
- **Monitoring**: Pipeline monitoring and alerting
- **Resource Management**: DuckDB and dbt resource integration

## Project Structure

```
another-nfl/
├── dbt/                          # dbt project
│   ├── dbt_project.yml          # Main dbt configuration
│   ├── profiles.yml             # Database connection profiles
│   ├── packages.yml             # dbt packages (dbt-utils)
│   ├── models/
│   │   ├── staging/             # Raw data cleaning
│   │   │   ├── stg_pbp.sql
│   │   │   ├── stg_weekly.sql
│   │   │   ├── stg_team_desc.sql
│   │   │   └── stg_schedules.sql
│   │   ├── intermediate/        # Business logic
│   │   │   ├── int_team_performance.sql
│   │   │   └── int_player_weekly_stats.sql
│   │   └── marts/              # Analytics tables
│   │       ├── mart_weekly_team_stats.sql
│   │       ├── mart_player_season_stats.sql
│   │       └── mart_game_results.sql
│   ├── macros/                 # Reusable SQL functions
│   │   ├── get_current_season.sql
│   │   └── calculate_fantasy_points.sql
│   └── tests/                  # Data quality tests
│       └── test_data_quality.sql
├── dagster/                    # Dagster orchestration
│   ├── definitions.py          # Main Dagster definitions
│   ├── assets/
│   │   ├── raw_data_assets.py  # Data extraction assets
│   │   └── dbt_assets.py       # dbt model assets
│   ├── resources/              # Dagster resources
│   │   ├── duckdb_resource.py
│   │   └── dbt_resource.py
│   └── schedules.py            # Pipeline schedules
└── scripts/
    └── test_phase3.py          # Phase 3 testing script
```

## Phase 3 Commands

### dbt Commands
```bash
# Navigate to dbt directory
cd dbt

# Install dbt packages
dbt deps

# Compile models (check syntax)
dbt compile

# Run staging models
dbt run --select tag:staging

# Run all models
dbt run

# Run tests
dbt test

# Generate documentation
dbt docs generate

# Serve documentation
dbt docs serve
```

### Dagster Commands
```bash
# Navigate to root directory
cd /path/to/another-nfl

# Start Dagster UI (development)
dagster dev -f dagster/definitions.py

# Run specific asset
dagster asset materialize --asset pbp_data

# Run dbt models
dagster asset materialize --asset dbt_staging_models

# Deploy to production (future)
dagster cloud deploy
```

### Combined Workflow
```bash
# 1. Extract raw data
uv run python -m src.cli extract incremental pbp --max-age-days 7

# 2. Run dbt transformations
cd dbt && dbt run && cd ..

# 3. Or use Dagster for orchestration
dagster dev -f dagster/definitions.py
```

## Data Flow Architecture

```
NFL API → CLI Extraction → Parquet Files → dbt Staging → dbt Intermediate → dbt Marts
                                ↓
                       Dagster Orchestration ← → DuckDB Database
                                ↓
                       Schedules & Monitoring
```

### Staging Layer
- **stg_pbp**: Play-by-play data with standardized columns
- **stg_weekly**: Weekly player statistics
- **stg_team_desc**: Team reference information
- **stg_schedules**: Game schedules and results

### Intermediate Layer
- **int_team_performance**: Team metrics by week and season
- **int_player_weekly_stats**: Player statistics with rankings

### Marts Layer
- **mart_weekly_team_stats**: Team performance for dashboards
- **mart_player_season_stats**: Player season totals for fantasy
- **mart_game_results**: Game analysis for ML and betting

## Configuration

### dbt Configuration
- **Target**: DuckDB database in `data/nfl_analytics.duckdb`
- **Materialization**: Views for staging, tables for marts
- **Testing**: Comprehensive data quality tests
- **Documentation**: Rich model documentation

### Dagster Configuration
- **Schedules**: Weekly extraction + transformation
- **Resources**: DuckDB connection and dbt runner
- **Assets**: Raw data extraction and dbt models
- **Monitoring**: Built-in pipeline monitoring

## Development Workflow

### Adding New Models
1. Create SQL file in appropriate directory (`staging/`, `intermediate/`, `marts/`)
2. Add model configuration in `_model_name.yml`
3. Add tests and documentation
4. Register as Dagster asset if needed

### Adding New NFL Datasets
1. Add dataset configuration in `configs/datasets/`
2. Create staging model in `dbt/models/staging/`
3. Update Dagster raw data assets
4. Add to dbt project documentation

### Quality Assurance
1. **dbt tests**: Run `dbt test` for data validation
2. **CLI tests**: Run `uv run pytest` for code quality
3. **Integration tests**: Run `python scripts/test_phase3.py`
4. **Manual testing**: Use `dbt docs serve` to review models

## Deployment and Production

### Development Environment
- Local DuckDB database
- dbt development target
- Manual Dagster runs

### Production Environment (Future)
- Dagster Cloud deployment
- Scheduled pipeline runs
- Production DuckDB instance
- Automated monitoring and alerts

## Troubleshooting

### Common Issues
1. **dbt compilation errors**: Check SQL syntax and references
2. **Dagster asset failures**: Check CLI extraction success
3. **Missing data**: Verify parquet files in `data/` directory
4. **Test failures**: Check data quality and validation rules

### Debug Commands
```bash
# Check dbt compilation
dbt compile --select stg_pbp

# Test specific model
dbt test --select stg_pbp

# Check Dagster asset logs
dagster asset materialize --asset pbp_data

# Verify extraction status
uv run python -m src.cli extract status
```

## Next Steps

### Phase 4+ Features (Future)
- **Advanced ML Models**: Player performance prediction
- **Real-time Dashboards**: Live game statistics
- **API Endpoints**: Serve analytics data via REST API
- **Cloud Deployment**: Production-ready infrastructure
- **Advanced Orchestration**: Event-driven pipelines

### Immediate Enhancements
- Add more NFL datasets (all 19 supported)
- Create advanced analytics models
- Implement more sophisticated testing
- Add performance monitoring
- Create user documentation

## Success Metrics

Phase 3 is successful when:
- ✅ All dbt models compile and run successfully
- ✅ Dagster assets materialize without errors
- ✅ Data quality tests pass consistently  
- ✅ Documentation is comprehensive and up-to-date
- ✅ Pipeline schedules run automatically
- ✅ Analytics tables are available for downstream use

---

**Congratulations!** The NFL Data Pipeline is now a comprehensive, production-ready data warehouse with modern orchestration and analytics capabilities. 🏈📊