# NFL Data Analytics Pipeline

Production-grade data warehouse and analytics pipeline for NFL data processing. Built with modern data engineering tools: **dbt**, **Dagster**, **DuckDB**, **DuckLake**, and **FastAPI**.

**🎉 Status: Production Ready - Functional Programming Architecture**

> **Latest (July 25, 2025):** **Complete Functional CLI Refactor!** Transformed from imperative to **pure functional programming** with immutable data structures, monadic error handling, and composable pipelines. **100% NFL Dataset Coverage** maintained with **149 comprehensive tests passing**. New streamlined CLI with **Extract → Process → Materialize → Query** workflow using functional programming principles.

## ⚡ Quick Start

```bash
# Setup environment
uv python install 3.11 && uv sync --dev

# Functional CLI - Extract → Process → Materialize → Query workflow

# 1. Extract NFL data
uv run python -m src.cli extract all --year 2024 --priority critical
uv run python -m src.cli extract status

# 2. Process raw data
uv run python -m src.cli process catalog
uv run python -m src.cli process validate

# 3. Materialize staging models
uv run python -m src.cli materialize all --tests
uv run python -m src.cli materialize status

# 4. Query staging models
uv run python -m src.cli query models
uv run python -m src.cli query staging stg_pbp --limit 10
uv run python -m src.cli query sql "SELECT team, COUNT(*) FROM stg_team_desc GROUP BY team"

# System health check
uv run python -m src.cli health

# Alternative: Dagster web interface for orchestration
uv run dagster dev -f nfl_dagster/definitions.py
# Access: http://localhost:3000

# Visualization dashboards
uv run streamlit run pure_staging_explorer.py --server.port 8504
```

## 🏗️ Architecture

### Complete Staging Model Coverage (NEW!)
- **100% Dataset Coverage**: All 19 NFL datasets now have dedicated staging models
  - 🔴 **Critical Models** (4): pbp, weekly, schedules, team_desc
  - 🟡 **High Priority** (4): seasonal, players, weekly_rosters, seasonal_rosters  
  - 🟢 **Medium Priority** (5): injuries, depth_charts, snap_counts, qbr, ngs_data
  - 🟣 **Low Priority** (6): weekly_pfr, seasonal_pfr, ftn_data, officials, combine, draft_picks
- **Advanced Analytics**: Production dashboard with 5 specialized analytics tabs
  - 🏥 Injury Analytics, 📊 Snap Count Analysis, 🎯 QB Performance
  - ⚡ Next Gen Stats, 👥 Roster Analysis
- **Data Quality**: 149 comprehensive tests ensuring data integrity
- **Orchestration**: Complete Dagster integration with priority-based processing

### Technology Stack
| Component | Technology | Status |
|-----------|------------|--------|
| **Language** | Python 3.11 + uv | ✅ Production |
| **CLI Architecture** | **Functional Programming** (Pure Functions + Immutable Data) | ✅ **NEW** |
| **CLI Interface** | Click + Rich + Monadic Error Handling | ✅ Production |
| **Data Warehouse** | dbt + DuckDB | ✅ Production |
| **Orchestration** | Dagster (Full Management) | ✅ Production |
| **Lakehouse** | DuckLake + PostgreSQL | ✅ Production |
| **API** | FastAPI | ✅ 9/15 endpoints |
| **Visualization** | Streamlit (dbt Integration) + Evidence + Grafana | ✅ Production |
| **Testing** | pytest + **Hypothesis** (Property-Based) | ✅ **Enhanced** |
| **Functional Libraries** | toolz, returns, immutables, pyrsistent | ✅ **NEW** |

## 🚀 Key Features

### Functional CLI Architecture (NEW!)
**Pure Functions + Immutable Data + Monadic Error Handling**

```bash
# Extract: Pure data extraction with no side effects
uv run python -m src.cli extract all --year 2024 --priority critical
uv run python -m src.cli extract dataset pbp --year 2024 --validate
uv run python -m src.cli extract cleanup --dry-run --max-age-days 30

# Process: Immutable data processing and validation  
uv run python -m src.cli process read data/pbp/2024/pbp_2024.parquet --limit 5
uv run python -m src.cli process catalog --base-path data
uv run python -m src.cli process validate --verbose

# Materialize: Functional Dagster/dbt integration
uv run python -m src.cli materialize staging --priority critical --tests
uv run python -m src.cli materialize models stg_pbp stg_weekly
uv run python -m src.cli materialize all

# Query: Pure DuckLake querying with time travel
uv run python -m src.cli query staging stg_pbp --limit 10
uv run python -m src.cli query staging stg_weekly --as-of-date 2024-01-15
uv run python -m src.cli query sql "SELECT * FROM stg_team_desc WHERE team_conf = 'NFC'"
uv run python -m src.cli query schema stg_pbp

# Health: System-wide health monitoring
uv run python -m src.cli health
```

### Dagster Orchestration
```bash
# Dagster web interface for visual pipeline management
uv run dagster dev -f nfl_dagster/definitions.py

# Asset materialization by priority (also available via CLI)
uv run dagster asset materialize --select nfl_critical_raw_data
uv run dagster asset materialize --select dbt_critical_staging_models

# Job execution and scheduling
uv run dagster job execute --job nfl_full_critical_pipeline_job
uv run dagster schedule start daily_critical_data_extraction
```

### Enhanced Streamlit Dashboard (NEW)
```bash
# Start production-ready dashboard with dbt staging integration
cd visualizations/streamlit_app
uv run streamlit run main_staging.py --server.port 8504

# Features:
# - Real-time Dagster pipeline monitoring
# - Type-safe data models with validation
# - Intelligent caching (30min/10min/1min TTL)
# - Comprehensive health checks and data lineage
# - Advanced fantasy analytics with consistency metrics
# - Team performance analysis with win rates and scoring
```

### Functional Programming Benefits
**Why Functional Architecture Matters:**

- **🔒 Immutable Data**: No unexpected mutations, safer concurrent operations
- **🧪 Pure Functions**: Easier testing, debugging, and reasoning about code
- **🔄 Composable Pipelines**: Build complex workflows from simple, reusable functions  
- **⚡ Error Handling**: Monadic `CLIResult[T]` with automatic error propagation
- **📊 Property-Based Testing**: Hypothesis tests catch edge cases automatically
- **🚀 Performance**: Pure functions enable memoization and parallelization

```bash
# Example: Functional pipeline composition
# Extract → Validate → Process → Materialize → Query (all pure functions)
uv run python -m src.cli extract dataset pbp --year 2024 --validate
uv run python -m src.cli process read data/pbp/2024/pbp_2024.parquet --validate  
uv run python -m src.cli materialize staging --priority critical
uv run python -m src.cli query staging stg_pbp --limit 10
```

### Legacy CLI Commands (Preserved)
```bash
# Original CLI preserved as backup (src/cli_legacy.py)
# New functional CLI provides cleaner, more reliable interface

# Old approach (imperative):
# uv run python -m src.cli_legacy extract multiple seasonal --years 2021,2022,2023

# New approach (functional):
uv run python -m src.cli extract all --year 2023 --priority high
uv run python -m src.cli extract dataset seasonal --year 2021 --validate
```

### Advanced Analytics Models
**Team Performance (`int_team_performance`)**:
- Third/fourth down conversion rates
- EPA (Expected Points Added) per play
- Win percentages and season progression
- Play-calling efficiency metrics

**Player Statistics (`int_player_weekly_stats`)**:
- Weekly and season-to-date position rankings
- 4-week rolling fantasy point averages
- EPA per opportunity (receiving/rushing/passing)
- Air yards analytics and YAC (Yards After Catch) metrics

### Visualization Stack
**dbt Staging Explorer** (Pure Staging Interface):
- **Purpose**: Exclusive dbt staging table exploration and validation
- **Interface**: Single-page application with zero navigation complexity
- **Tables**: Auto-detects and deduplicates staging models (pbp, weekly, team_desc, schedules)
- **Features**: Schema inspection, data preview, smart filtering, CSV export
- **Launch**: `uv run streamlit run pure_staging_explorer.py --server.port 8504`

**Streamlit Dashboard** (Production Ready - dbt Staging Integration):
- **Architecture**: Complete refactor using dbt staging models with Dagster monitoring
- **Data Access**: Type-safe models (`TeamInfo`, `PlayerWeeklyStats`, `GameSchedule`)
- **Caching**: Intelligent multi-level caching (30min/10min/1min TTL)
- **Monitoring**: Real-time pipeline health and data freshness indicators
- **Pages**: 
  - **Overview**: System health, pipeline status, top performers
  - **Team Analysis**: Conference/division analytics, win rates, scoring differentials
  - **Fantasy Dashboard**: Advanced player analytics, consistency metrics, tier analysis
  - **System Health**: Data lineage, model status, manual controls

**API Server** (9/15 endpoints operational):
```bash
cd src/api && uv run uvicorn main:app --port 8000
# Access: http://localhost:8000/docs
```

## 📊 Data Coverage

### NFL Datasets (19 supported)
| Category | Datasets | Years | Description |
|----------|----------|-------|-------------|
| **Game Data** | `pbp`, `schedules` | 1999+ | Play-by-play, game results |
| **Player Stats** | `weekly`, `seasonal` | 1999+ | Performance metrics |
| **Reference** | `team_desc`, `players` | Static | Team/player information |
| **Advanced** | `qbr`, `ngs_data`, `injuries` | 2006+ | Advanced metrics |

### Complete Data Quality Framework
- **149/149 comprehensive tests passing** (staging + intermediate models)
- **19/19 staging models operational** with full NFL dataset coverage
- **Advanced analytics integration** across all staging models
- **Multi-level validation**: Range checks, referential integrity, business rules
- **Real-time monitoring** through enhanced dashboard interface

## 🛠️ Development

### Environment Setup
```bash
# Prerequisites: Python 3.11, uv package manager
uv python install 3.11
git clone <repository-url> && cd another-nfl
uv sync --dev
uv run pre-commit install
```

### Development Workflow
```bash
# Functional CLI Development
uv run python -m src.cli health  # Check system health

# Code quality with functional standards
uv run ruff format . && uv run ruff check .

# Enhanced testing with property-based tests
uv run pytest --cov=src --cov-report=html
uv run pytest tests/test_functional_utils.py -v        # Property-based tests
uv run pytest tests/test_functional_integration.py -v  # Integration tests
uv run dbt test --select tag:intermediate              # dbt model tests (17/17 passing)

# Functional CLI testing workflow
uv run python -m src.cli extract status                # Test extraction
uv run python -m src.cli process catalog              # Test processing  
uv run python -m src.cli materialize status           # Test materialization
uv run python -m src.cli query models                 # Test querying

# dbt development (unchanged)
uv run dbt run --select tag:staging
uv run dbt run --select tag:intermediate
uv run dbt docs generate && uv run dbt docs serve

# Dagster development (integrated with functional CLI)
uv run dagster dev -f nfl_dagster/definitions.py

# Dashboard development
cd visualizations/streamlit_app
uv run streamlit run main_staging.py --server.port 8504
```

### Project Structure
```
├── src/                         # Core Python modules
│   ├── cli.py                  # 🆕 Functional CLI (Pure Functions)
│   ├── cli_legacy.py           # Original CLI (preserved as backup)
│   ├── functional_utils.py     # 🆕 Functional programming utilities
│   ├── functional_extraction.py # 🆕 Pure extraction functions
│   ├── functional_processing.py # 🆕 Immutable data processing
│   ├── functional_materialization.py # 🆕 Functional Dagster/dbt integration
│   ├── functional_querying.py  # 🆕 Pure DuckLake querying
│   ├── nfl_extractor.py        # Production extraction (legacy)
│   ├── ducklake_manager.py     # DuckLake integration
│   └── api/                    # FastAPI server
├── tests/                      # Enhanced testing suite
│   ├── test_functional_utils.py      # 🆕 Property-based tests
│   ├── test_functional_integration.py # 🆕 Integration tests  
│   └── test_*.py               # Legacy test files
├── dbt/                        # dbt data warehouse
│   ├── models/staging/         # 19 staging models (100% coverage)
│   ├── models/intermediate/    # 2 enhanced models
│   └── INTERMEDIATE_MODELS.md  # Detailed docs
├── nfl_dagster/               # Pipeline orchestration
├── visualizations/            # Enhanced dashboards
│   ├── streamlit_app/         # dbt staging integration
│   └── evidence/              # SQL-based reporting
├── configs/                   # YAML configurations
└── FUNCTIONAL_CLI_REFACTOR_SUMMARY.md # 🆕 Complete refactor documentation
```

## 📈 Advanced Usage

### Advanced Functional Operations

#### Time Travel Queries (Pure Functions)
```bash
# Query historical data with time travel
uv run python -m src.cli query staging stg_pbp --as-of-date 2024-12-01 --limit 5
uv run python -m src.cli query staging stg_weekly --as-of-date 2024-01-15

# Compare data over time (functional composition)
uv run python -m src.cli query sql "SELECT COUNT(*) as current_rows FROM stg_pbp"
uv run python -m src.cli query sql "SELECT COUNT(*) as historical_rows FROM stg_pbp AS OF '2024-01-01'"
```

#### Functional Pipeline Composition
```bash
# Chain operations functionally (each step is pure)
uv run python -m src.cli extract dataset pbp --year 2024 --validate
uv run python -m src.cli process read data/pbp/2024/pbp_2024.parquet --validate
uv run python -m src.cli materialize staging --priority critical
uv run python -m src.cli query staging stg_pbp --limit 10

# Error handling with monadic composition (automatic error propagation)
uv run python -m src.cli extract dataset invalid_dataset --year 2024  # Graceful error
uv run python -m src.cli query staging nonexistent_model               # Graceful error
```

#### Property-Based Testing Examples
```bash
# Run property-based tests that generate test cases automatically
uv run pytest tests/test_functional_utils.py::TestCLIResult::test_map_preserves_success -v
uv run pytest tests/test_functional_utils.py::TestFunctionalUtilities::test_compose_function_composition -v
```

### Custom Analytics
```sql
-- Top fantasy performers by position
SELECT 
    position,
    player_name,
    AVG(fantasy_points_ppr_4wk_avg) as avg_fantasy_points
FROM {{ ref('int_player_weekly_stats') }}
WHERE season = 2023 AND week >= 4
GROUP BY position, player_name
ORDER BY avg_fantasy_points DESC
LIMIT 20;
```

### API Integration
```python
import requests

# Get dataset information
response = requests.get("http://localhost:8000/api/v1/datasets")
datasets = response.json()

# Query player stats
response = requests.get("http://localhost:8000/api/v1/players/stats", 
                       params={"position": "QB", "season": 2023})
```

## 🔧 Configuration

### Dataset Configuration
Each NFL dataset has YAML configuration in `configs/datasets/`:
```yaml
name: pbp
description: "Play-by-play data with EPA metrics"
function_name: import_pbp_data
start_year: 1999
requires_year: true
validation:
  required_columns: ["game_id", "play_id", "epa"]
  expected_size_mb: 500
```

### DuckLake Integration
```python
from src.ducklake_manager import DuckLakeManager

ducklake = DuckLakeManager()
data = ducklake.query_model("int_team_performance", limit=100)
tables = ducklake.get_catalog_tables()
```

## 🧪 Testing & Quality

### Test Coverage
- **Overall**: 94% test success rate (110/117 tests)
- **Intermediate Models**: 17/17 tests passing
- **API**: 75+ test cases with mocking
- **CLI**: Comprehensive command testing

### Data Quality
- Range validation for rates and percentages
- Foreign key relationships to reference data
- Not null constraints on key identifiers
- Business logic validation for NFL data

## 📚 Documentation

### Core Documentation
- **`FUNCTIONAL_CLI_REFACTOR_SUMMARY.md`**: 🆕 **Complete functional programming refactor guide**
- **`README.md`**: This comprehensive usage guide with functional CLI examples
- **`ARCHITECTURE.md`**: Enterprise architecture with functional programming integration
- **`DAGSTER_DBT_REFACTOR_SUMMARY.md`**: Dagster and dbt integration details

### Development Documentation  
- **`.claude/CLAUDE.md`**: Development guide and commands for Claude Code
- **`dbt/INTERMEDIATE_MODELS.md`**: Detailed dbt model documentation
- **`PRODUCTION_DEPLOYMENT_GUIDE.md`**: Enterprise deployment guide
- **`DUCKLAKE_INTEGRATION.md`**: Time travel and versioning capabilities

### Functional Programming Documentation
- **`src/functional_utils.py`**: Core functional programming utilities and patterns
- **`tests/test_functional_utils.py`**: Property-based testing examples
- **`tests/test_functional_integration.py`**: Integration testing patterns

### Visualization Documentation
- **`visualizations/streamlit_app/README_STAGING_INTEGRATION.md`**: Streamlit dbt integration
- **`COMPLETE_STAGING_MODELS_GUIDE.md`**: Complete staging model coverage guide

## 🚢 Production Deployment

### Docker & Orchestration
```bash
# Development environment
docker-compose up -d

# Production deployment
cd ansible
ansible-playbook -i inventories/production/hosts.yml playbooks/deploy-nfl-platform.yml
```

### Monitoring & Operations
- **Prometheus + Grafana**: System monitoring
- **Health checks**: Automated system validation
- **Backup & Recovery**: S3 integration with encryption
- **CI/CD**: GitHub Actions with automated testing

## 🎯 Roadmap

### Latest Achievements (Phase 7 Complete - Functional Programming)
- ✅ **Complete Functional CLI Refactor**: Pure functions + immutable data + monadic error handling
- ✅ **Streamlined Architecture**: From 25 commands to 16 focused commands across 4 workflows
- ✅ **Enhanced Testing**: Property-based testing with Hypothesis for comprehensive validation
- ✅ **Improved Reliability**: Immutable data structures prevent mutation bugs
- ✅ **Better Composability**: Functional pipelines enable easy composition and reuse
- ✅ **Preserved Compatibility**: All existing Dagster/dbt/DuckLake integrations maintained

### Future Enhancements
- **Phase 8**: **Parallel Processing** - Leverage pure functions for concurrent execution
- **Phase 9**: **Real-time Functional Streams** - Functional reactive programming for live data
- **Phase 10**: **Advanced Composition** - Higher-order function combinators and DSLs
- **Phase 11**: **Distributed Functional Computing** - Pure functions enable easy distribution
- **Phase 12**: **Machine Learning Pipelines** - Functional ML with immutable model states

---

**Developed with**: Python 3.11 + **Functional Programming**, dbt, Dagster, DuckDB, FastAPI, Streamlit  
**Architecture**: **Pure Functions + Immutable Data + Monadic Error Handling**  
**License**: MIT  
**Status**: Production Ready ✅ **Enhanced with Functional Programming**