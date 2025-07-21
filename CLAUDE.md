# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is an NFL data extraction and analysis project that builds a modern data pipeline using Python. The goal is to extract NFL data using the `nfl_data_py` package and create a comprehensive data lake with transformation layers.

## Technology Stack

- **Python Environment**: `uv` for virtual environment and package management
- **Data Source**: `nfl_data_py` Python package for NFL datasets
- **Data Storage**: Parquet files partitioned by ETL date
- **Data Lake**: DuckLake with metadata stored in PostgreSQL
- **Orchestration**: Dagster for data pipeline orchestration
- **Transformations**: dbt for data modeling and transformations
- **Containerization**: Podman with Docker Compose for service orchestration
- **Infrastructure**: Ansible for environment setup and deployment
- **Code Quality**: Ruff for formatting and linting
- **Testing**: pytest for all CLI tools and functions
- **CLI**: Click framework for command-line interface

## Development Commands

```bash
# Install Python 3.11 and set up environment
uv python install 3.11
uv init --python 3.11
uv sync

# Code formatting and linting
ruff format .
ruff check .

# Run tests with coverage
pytest --cov=src --cov-report=html --cov-report=term

# Run CLI tool
python -m src.cli --help

# Pre-commit hooks (after setup)
pre-commit run --all-files
```

## Project Architecture

### Phase-Based Development
The project follows a structured phase-based approach:

1. **Phase 1**: Environment setup, project structure, CLI tool for NFL data exploration
   - Install Python 3.11 using uv
   - Create pyproject.toml with project metadata, dependencies, and tool configurations
   - Set up pre-commit hooks for ruff
   - Create CLI structure with subcommands covering all NFL datasets:
     - `explore datasets` - List available NFL datasets
     - `explore data <dataset> --year <year>` - Show sample data with verbose error handling
     - `read parquet <file>` - Read and display parquet files
   - Set up pytest with coverage reporting and testing patterns
   - Create README documentation for CLI usage
   - Build comprehensive tests for all CLI tools
   - Test CLI tools and show console outputs
2. **Phase 2**: Data extraction functions with configuration for each NFL dataset
3. **Phase 3**: dbt staging models, Dagster integration, comprehensive testing
4. **Future Phases**: Advanced transformations, ML models, dashboards

### Data Pipeline Structure
- **Raw Data**: Extracted from `nfl_data_py` and stored as partitioned Parquet files
- **Staging Layer**: dbt models for light data cleaning and normalization
- **Intermediate Layer**: Team and position-specific data models
- **Final Layer**: Analytics-ready models for dashboards and ML

### Functional Programming Paradigm
The codebase follows functional programming principles throughout the data pipeline.

## Key Directories

- `references/`: Project documentation and tool references
- `data/`: Raw and processed data files (git-ignored)
- `src/`: Source code for CLI tools and data functions
- `tests/`: Test files for all components
- Future directories will include:
  - `dbt/`: dbt models and configurations
  - `dagster/`: Orchestration assets and schedules

## NFL Data Considerations

- Each NFL dataset has different starting years for historical data
- Data should be extracted by year and partitioned by `etl_date`
- Separate folders should exist for each dataset type
- Configuration files will define extraction parameters for each dataset

## Environment Setup

- **Python Version**: 3.11 (installed via uv)
- **Global Tools**: Only Ansible, uv, git, and GitHub CLI should be installed globally
- **Dependency Management**: All other dependencies managed through uv virtual environments
- **Code Quality**: Pre-commit hooks with ruff for automatic formatting and linting
- **Testing**: pytest with coverage reporting and comprehensive test patterns
- **Security**: Environment variables and sensitive information must be git-ignored
- **Deployment**: Both dev and prod environments supported through Ansible configurations