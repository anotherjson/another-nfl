# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

NFL data extraction and analysis project building a modern data pipeline with Python. Extracts NFL data using `nfl_data_py` and creates a comprehensive data lake with transformation layers.

**🎉 Status: Phase 5+ Complete - Production Ready**

> **Latest (July 23, 2025):** Enhanced CLI tooling for dbt models, DuckLake integration, time travel queries, Dagster materialization. **94% test success rate**, **17/17 intermediate model tests passing**, **100% operational visualization stack**.

## Technology Stack

### Core Components (Production Ready ✅)
- **Python 3.11** with `uv` package management
- **CLI Framework**: Click + Rich for formatted output
- **Data Source**: `nfl_data_py` (19 NFL datasets)
- **Data Processing**: pandas, pyarrow, DuckDB
- **Code Quality**: Ruff, pre-commit hooks, pytest (94% success rate)
- **dbt Data Warehouse**: Staging + enhanced intermediate models
- **Dagster Orchestration**: Pipeline orchestration with monitoring
- **DuckLake**: Time travel, versioning, ACID transactions
- **FastAPI**: REST API (9/15 endpoints operational)
- **Visualization**: Streamlit dashboard + Evidence + Grafana
- **CLI Model Operations**: dbt integration with time travel queries

## Development Commands

### Environment Setup
```bash
# Setup environment
uv python install 3.11
uv sync --dev
uv run pre-commit install
```

### Data Exploration & Extraction
```bash
# CLI data exploration
uv run python -m src.cli explore datasets
uv run python -m src.cli explore data pbp --year 2023

# Production data extraction
uv run python -m src.cli extract dataset pbp --year 2023
uv run python -m src.cli extract incremental weekly --max-age-days 7
uv run python -m src.cli extract status
```

### dbt Data Transformations
```bash
# Core dbt commands
uv run dbt deps
uv run dbt run --select tag:staging
uv run dbt run --select tag:intermediate  # 17/17 tests passing
uv run dbt test
uv run dbt docs generate && uv run dbt docs serve
```

### Advanced CLI Model Operations (NEW)
```bash
# dbt model management through CLI
uv run python -m src.cli models list
uv run python -m src.cli models materialize --verbose
uv run python -m src.cli models query int_team_performance --limit 10
uv run python -m src.cli models catalog
uv run python -m src.cli models sql "SELECT COUNT(*) FROM stg_team_desc"
```

### Dagster Orchestration
```bash
# Pipeline orchestration
uv run dagster dev -f nfl_dagster/definitions.py
uv run dagster asset materialize --asset dbt_staging_models
```

### API & Visualization
```bash
# FastAPI server (9/15 endpoints working)
cd src/api && uv run uvicorn main:app --port 8000

# Streamlit dashboard (production ready)
cd visualizations/streamlit_app
uv run streamlit run working_main.py --server.port 8504
```

### Testing & Validation
```bash
# Comprehensive testing
uv run pytest --cov=src --cov-report=html
uv run python scripts/test_phase3.py
uv run pytest tests/test_cli_models.py -v
```

## Data Pipeline Architecture

### Layer Structure
- **Raw Data**: Parquet files from `nfl_data_py` (19 datasets)
- **Staging**: Light cleaning & normalization (4 models)
- **Intermediate**: Enhanced analytics with EPA metrics (2 models)
  - `int_team_performance`: Team analytics, win rates, efficiency
  - `int_player_weekly_stats`: Player rankings, rolling averages, EPA
- **Marts**: Analytics-ready models (future enhancement)

### Enhanced Intermediate Models
**int_team_performance**: Advanced team performance metrics
- Down conversion rates (3rd/4th down success)
- EPA/WPA analytics per play
- Win percentages and season progress
- Play-calling distribution and efficiency

**int_player_weekly_stats**: Comprehensive player analytics  
- Position rankings (weekly + season-to-date)
- 4-week rolling fantasy averages
- EPA per opportunity (receiving/rushing/passing)
- Air yards analytics and YAC metrics
- Efficiency calculations and opponent tracking

## Project Structure

### Current Implementation
```
├── src/                    # Core Python modules
│   ├── cli.py             # Click-based CLI interface
│   ├── nfl_explorer.py    # Data exploration logic
│   ├── nfl_extractor.py   # Production extraction
│   ├── ducklake_manager.py # DuckLake CLI integration
│   └── api/               # FastAPI server
├── dbt/                   # dbt data warehouse
│   ├── models/staging/    # 4 staging models
│   ├── models/intermediate/ # 2 enhanced models (17/17 tests)
│   └── INTERMEDIATE_MODELS.md # Detailed documentation
├── nfl_dagster/          # Orchestration assets
├── visualizations/       # Streamlit + Evidence dashboards
├── tests/                # Comprehensive test suite (94% success)
├── configs/              # YAML dataset configurations
└── scripts/              # Validation and demo scripts
```

### Key Documentation
- `README.md`: Complete usage guide
- `dbt/INTERMEDIATE_MODELS.md`: Enhanced models documentation
- `PRODUCTION_DEPLOYMENT_GUIDE.md`: Enterprise deployment
- `DUCKLAKE_INTEGRATION.md`: Time travel & versioning

## NFL Data Reference

### Supported Datasets (19 total)
**Game-Level Data:**
- `pbp`: Play-by-play (1999+) - Primary analytics source
- `schedules`: Game schedules (1999+) - Results & metadata
- `weekly`: Player weekly stats (1999+) - Fantasy & performance
- `seasonal`: Season totals (1999+) - Aggregate performance

**Reference Data:**
- `team_desc`: Team information (static) - Conferences & divisions
- `players`: Player profiles (static) - Positions & identifiers

**Advanced Metrics:**
- `qbr`: QB ratings (2006+), `ngs_data`: Next Gen Stats (2016+)
- `injuries`: Injury reports (2009+), `depth_charts`: Rosters (2001+)

*See complete dataset list in `src/nfl_explorer.py`*

## Configuration System

### Dataset Configuration (YAML-based)
```python
from src.config_loader import ConfigLoader

loader = ConfigLoader()
pbp_config = loader.get_dataset_config('pbp')
datasets = loader.list_datasets()
path = loader.get_output_path('pbp', year=2023, etl_date='2024-01-15')
```

### CLI Model Operations Integration
```python
from src.ducklake_manager import DuckLakeManager

ducklake = DuckLakeManager()
models = ducklake.list_available_models()
result = ducklake.materialize_staging_models()
data = ducklake.query_model("int_team_performance", limit=100)
```

## Development Guidelines

### Code Quality Standards
- **Python 3.11** required
- **uv** for dependency management (not pip/conda)
- **Ruff** for formatting and linting
- **pytest** with coverage reporting
- **Pre-commit hooks** for automated quality checks

### Testing Requirements
- **94% overall test success rate** maintained
- **17/17 intermediate model tests** must pass
- All new models require comprehensive test coverage
- Integration tests for CLI model operations

### Security Considerations
- Environment variables in `.env` files (git-ignored)
- Database credentials protected
- API keys excluded from repository
- Infrastructure secrets managed via Ansible vault

## Troubleshooting

### Common Issues
**dbt Model Errors:**
```bash
# Check compilation
uv run dbt compile --select tag:intermediate

# Run with debug
uv run dbt run --select tag:intermediate --debug
```

**CLI Model Operations:**
```bash
# Test DuckLake connection
uv run python scripts/test_ducklake_integration.py

# Validate model availability
uv run python -m src.cli models list
```

**Performance Issues:**
- Use `--limit` flag for large dataset queries
- Check DuckDB memory settings in profiles.yml
- Monitor PostgreSQL catalog performance

---

*For detailed model documentation, see `dbt/INTERMEDIATE_MODELS.md`*  
*For production deployment, see `PRODUCTION_DEPLOYMENT_GUIDE.md`*