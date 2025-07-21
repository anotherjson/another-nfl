# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is an NFL data extraction and analysis project that builds a modern data pipeline using Python. The goal is to extract NFL data using the `nfl_data_py` package and create a comprehensive data lake with transformation layers.

## Technology Stack

### Currently Implemented (Phase 1 ✅ + Phase 2 ✅)
- **Python Environment**: Python 3.11 with `uv` for virtual environment and package management
- **CLI Framework**: Click for command-line interface with Rich for formatted output
- **Data Source**: `nfl_data_py` Python package (19 NFL datasets supported)
- **Data Analysis**: pandas for data manipulation, pyarrow for Parquet file handling
- **Code Quality**: Ruff for formatting and linting with pre-commit hooks
- **Testing**: pytest with coverage reporting (comprehensive test suite)
- **Documentation**: Comprehensive README and usage examples
- **Configuration System**: YAML-based configuration for all 19 NFL datasets with validation
- **Configuration Management**: PyYAML integration with comprehensive config loader
- **Production Extraction**: NFLDataExtractor with retry logic, validation, and error handling
- **Data Partitioning**: Year-based and ETL-date partitioning with configurable paths
- **Incremental Processing**: ExtractionManager with state tracking and incremental updates
- **Data Validation**: Comprehensive validation with required column checks and size estimates

### Future Implementation (Phase 3+)
- **Data Storage**: Parquet files partitioned by ETL date
- **Data Lake**: DuckLake with metadata stored in PostgreSQL
- **Orchestration**: Dagster for data pipeline orchestration
- **Transformations**: dbt for data modeling and transformations
- **Containerization**: Podman with Docker Compose for service orchestration
- **Infrastructure**: Ansible for environment setup and deployment

## Development Commands

```bash
# Install Python 3.11 and set up environment
uv python install 3.11
uv init --python 3.11
uv sync --dev

# Code formatting and linting
uv run ruff format .
uv run ruff check .

# Run tests with coverage
uv run pytest --cov=src --cov-report=html --cov-report=term

# Run CLI tool commands
uv run python -m src.cli --help
uv run python -m src.cli explore datasets
uv run python -m src.cli explore data team_desc --limit 3
uv run python -m src.cli read data/sample.parquet --info

# Pre-commit hooks
uv run pre-commit install
uv run pre-commit run --all-files
```

## Project Architecture

### Phase-Based Development
The project follows a structured phase-based approach:

1. **Phase 1**: ✅ **COMPLETED** - Environment setup, project structure, CLI tool for NFL data exploration
   - ✅ Python 3.11 environment with uv package manager
   - ✅ Complete pyproject.toml with dependencies and tool configurations
   - ✅ Pre-commit hooks with ruff for code quality
   - ✅ CLI structure with Click framework covering all 19 NFL datasets:
     - `explore datasets` - List available NFL datasets with rich table formatting
     - `explore data <dataset> --year <year>` - Sample data with filtering and verbose error handling
     - `read parquet <file> --info` - Read and analyze parquet files with detailed information
   - ✅ pytest with 82% coverage and comprehensive test patterns
   - ✅ Complete README documentation with usage examples
   - ✅ Full test suite with 44 test cases across CLI, NFLExplorer, and ParquetReader
   - ✅ Enhanced .gitignore for environment variables, database configs, and sensitive files
   - ✅ Rich formatted console output with beautiful tables and error handling

2. **Phase 2**: Data extraction functions with configuration for each NFL dataset
   - Create configuration files defining extraction parameters for each dataset
   - Build functions for extracting data from nfl_data_py with year-based partitioning
   - Implement data validation and quality checks
   - Add support for incremental data extraction
   - Build comprehensive tests for extraction functions

3. **Phase 3**: dbt staging models, Dagster integration, comprehensive testing
   - Set up dbt project structure following best practices
   - Create staging models for light transformations of raw datasets
   - Build intermediate models for team and position-specific data
   - Integrate Dagster for orchestrating dbt and data extraction
   - Create final analytics-ready models for dashboards and ML

4. **Future Phases**: Advanced transformations, ML models, dashboards
   - Advanced data transformations and feature engineering
   - Machine learning model development and deployment
   - Dashboard and visualization tools integration
   - Real-time data processing capabilities

### Data Pipeline Structure
- **Raw Data**: Extracted from `nfl_data_py` and stored as partitioned Parquet files
- **Staging Layer**: dbt models for light data cleaning and normalization
- **Intermediate Layer**: Team and position-specific data models
- **Final Layer**: Analytics-ready models for dashboards and ML

### Functional Programming Paradigm
The codebase follows functional programming principles throughout the data pipeline.

## Key Directories

### Current Structure (Phase 1 ✅ + Phase 2 ✅)
- `references/`: Project documentation and tool references
- `data/`: Raw and processed data files (git-ignored, contains sample parquet files and extraction state)
- `src/`: Source code for CLI tools and data functions
  - `cli.py`: Main CLI interface with Click commands
  - `nfl_explorer.py`: NFL data exploration logic (19 datasets supported)
  - `parquet_reader.py`: Parquet file reading and analysis
  - `config_loader.py`: YAML configuration loader with validation
  - `nfl_extractor.py`: Production data extraction with retry logic and validation (NEW)
  - `extraction_manager.py`: Incremental extraction and state management (NEW)
- `configs/`: Dataset extraction configurations
  - `datasets/`: YAML configuration files for all 19 NFL datasets
- `tests/`: Comprehensive test files for all components (99 test cases)
  - `test_cli.py`: CLI command tests with mocking
  - `test_nfl_explorer.py`: NFLExplorer functionality tests
  - `test_parquet_reader.py`: ParquetReader tests with temporary files
  - `test_config_loader.py`: Configuration system tests
  - `test_nfl_extractor.py`: Production extraction tests (NEW)
  - `test_extraction_manager.py`: Incremental extraction tests (NEW)
- `.pre-commit-config.yaml`: Pre-commit hooks configuration
- `pyproject.toml`: Project dependencies and tool configurations
- `README.md`: Complete usage documentation and examples

### Future Directories (Phase 2+)
- `dbt/`: dbt models and configurations
- `dagster/`: Orchestration assets and schedules
- `ansible/`: Infrastructure as code for deployment

## Production Data Extraction (Phase 2 ✅)

### NFLDataExtractor - Production Extraction Engine
Robust production-ready data extraction with comprehensive error handling:

```python
from src.nfl_extractor import NFLDataExtractor

# Initialize extractor
extractor = NFLDataExtractor()

# Extract single dataset with full capabilities
data, metadata = extractor.extract_dataset(
    dataset_name='pbp',
    year=2023,
    validate=True,
    save_to_disk=True
)

# Extract multiple years
results = extractor.extract_multiple_years(
    dataset_name='weekly',
    years=[2020, 2021, 2022],
    validate=True,
    save_to_disk=True
)
```

**Key Features:**
- **Retry Logic**: Configurable retry attempts with exponential backoff
- **Data Validation**: Validates required columns, row counts, and data quality
- **File Management**: Automatic directory creation and parquet file generation
- **Error Handling**: Comprehensive error capture and logging
- **Metadata Tracking**: Detailed extraction metrics and timing information

### ExtractionManager - Incremental Processing
Smart incremental extraction with state management:

```python
from src.extraction_manager import ExtractionManager

# Initialize manager
manager = ExtractionManager()

# Incremental extraction (skips already-extracted data)
summary = manager.extract_incremental(
    dataset_name='pbp',
    years=[2020, 2021, 2022, 2023],
    force_refresh=False,
    max_age_days=1
)

# Get extraction status
status = manager.get_extraction_summary('pbp')
```

**Key Features:**
- **State Tracking**: JSON-based state file tracking all extractions
- **Incremental Processing**: Only extracts missing or stale data
- **Age-based Refresh**: Configurable data freshness requirements
- **Cleanup Management**: Automatic cleanup of old extractions
- **Status Reporting**: Comprehensive extraction summaries and statistics

### Data Partitioning & Storage
Configurable partitioning scheme based on dataset requirements:
- **Year-based datasets**: `data/{dataset}/{year}/etl_date={etl_date}/data.parquet`
- **Static datasets**: `data/{dataset}/etl_date={etl_date}/data.parquet`
- **Compression**: Snappy compression for optimal performance
- **Format**: Apache Parquet for analytics-ready storage

## Configuration System (Phase 2 ✅)

### Dataset Configuration Management
The project now includes a comprehensive YAML-based configuration system for all 19 NFL datasets:

- **Individual YAML files** for each dataset in `configs/datasets/`
- **Validation system** ensures configuration integrity
- **Dynamic path generation** for year-based and ETL-date partitioning
- **Dataset metadata** including start years, data types, and extraction parameters

### Configuration Structure
Each dataset configuration includes:
```yaml
name: dataset_name
description: "Human-readable description"
function_name: nfl_data_py_function_name
start_year: 1999  # or null for no year limitation
requires_year: true/false
data_type: category (e.g., game_level, player_weekly)
partition_by: year | etl_date_only
validation:
  required_columns: [list of expected columns]
  expected_size_mb: estimated file size
extraction:
  timeout_seconds: API timeout
  retry_attempts: number of retries
output:
  file_format: parquet
  compression: snappy
  path_template: "data/{dataset}/{year}/etl_date={etl_date}/data.parquet"
```

### Using the Configuration System
```python
from src.config_loader import ConfigLoader

# Load all configurations
loader = ConfigLoader()

# Get specific dataset config
pbp_config = loader.get_dataset_config('pbp')

# List all datasets
datasets = loader.list_datasets()

# Get datasets by type
game_datasets = loader.get_datasets_by_type('game_level')

# Validate year for dataset
loader.validate_year_for_dataset('pbp', 2023)

# Generate output path
path = loader.get_output_path('pbp', year=2023, etl_date='2024-01-15')
```

## NFL Data Considerations

### Supported Datasets (Phase 1 ✅)
The CLI tool currently supports all 19 datasets from `nfl_data_py`:
- **pbp**: Play-by-play data (1999+)
- **weekly**: Weekly player statistics (1999+)
- **seasonal**: Seasonal player statistics (1999+)
- **weekly_rosters**: Weekly team rosters (1999+)
- **seasonal_rosters**: Seasonal team rosters (1999+)
- **schedules**: Game schedules (1999+)
- **team_desc**: Team descriptions and information (no year limit)
- **officials**: Game officials (2001+)
- **combine**: NFL Combine results (1987+)
- **draft_picks**: NFL Draft picks (1936+)
- **qbr**: Weekly QBR data (2006+)
- **weekly_pfr**: Pro Football Reference weekly stats (1932+)
- **seasonal_pfr**: Pro Football Reference seasonal stats (1932+)
- **injuries**: Player injury reports (2009+)
- **depth_charts**: Team depth charts (2001+)
- **snap_counts**: Player snap counts (2012+)
- **ftn_data**: Fantasy Points allowed data (2018+)
- **ngs_data**: Next Gen Stats data (2016+)
- **players**: Player information (no year limit)

### Data Processing Guidelines
- Each NFL dataset has different starting years for historical data
- Data should be extracted by year and partitioned by `etl_date`
- Separate folders should exist for each dataset type
- Configuration files will define extraction parameters for each dataset
- Year validation is implemented in the CLI tool to prevent invalid requests

## Environment Setup

### Current Setup (Phase 1 ✅)
- **Python Version**: 3.11 (installed via uv)
- **Package Manager**: uv for virtual environment and dependency management
- **Dependencies**: 
  - Core: click, nfl_data_py, pandas, pyarrow, rich
  - Dev: pytest, pytest-cov, ruff, pre-commit
- **Code Quality**: Pre-commit hooks with ruff for automatic formatting and linting
- **Testing**: pytest with 82% coverage reporting and comprehensive test patterns
- **Security**: Enhanced .gitignore protecting environment variables, database configs, API keys, and sensitive files
- **CLI Tool**: Fully functional with rich formatted output and comprehensive error handling

### Deployment Guidelines
- **Global Tools**: Only Ansible, uv, git, and GitHub CLI should be installed globally
- **Dependency Management**: All other dependencies managed through uv virtual environments
- **Environment Variables**: All sensitive configuration must use .env files (git-ignored)
- **Database Configs**: Database connection strings and configurations must be git-ignored
- **Infrastructure**: Both dev and prod environments supported through Ansible configurations (Phase 2+)

### Security Considerations
The .gitignore file comprehensively protects:
- Environment variables (.env*, config files, secrets)
- Database files and connection strings
- API keys and credentials (including NFL API keys)
- Cloud provider configurations (.aws/, .azure/, .gcp/)
- Infrastructure as code secrets (terraform.tfvars, vault files)
- Project-specific files (dagster configs, dbt profiles, parquet data files)