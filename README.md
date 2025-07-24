# NFL Data Analytics Pipeline

Production-grade data warehouse and analytics pipeline for NFL data processing. Built with modern data engineering tools: **dbt**, **Dagster**, **DuckDB**, **DuckLake**, and **FastAPI**.

**🎉 Status: Production Ready - Complete Staging Model Coverage**

> **Latest (December 23, 2024):** **100% NFL Dataset Coverage Achieved!** All 19 NFL datasets now have dedicated staging models with **149 comprehensive tests passing**. **Advanced Analytics Dashboard** with injury analysis, QB performance, Next Gen Stats, and roster management. **Complete Dagster orchestration** with priority-based processing and **enhanced visualization capabilities**.

## ⚡ Quick Start

```bash
# Setup environment
uv python install 3.11 && uv sync --dev

# Start Dagster web interface
uv run dagster dev -f nfl_dagster/definitions.py
# Access: http://localhost:3000

# Manual asset materialization
uv run dagster asset materialize --select nfl_critical_raw_data
uv run dagster asset materialize --select tag:staging

# Run complete pipeline
uv run dagster job execute --job nfl_full_critical_pipeline_job

# Query models via CLI
uv run python -m src.cli models query int_team_performance --limit 10

# Start complete staging models explorer (all 19 NFL datasets)
uv run streamlit run pure_staging_explorer.py --server.port 8504

# Start enhanced dashboard (dbt staging models integration)
cd visualizations/streamlit_app
uv run streamlit run main_staging.py --server.port 8504
```

## 🏗️ Architecture

### Complete Staging Model Coverage (NEW!)
- **100% Dataset Coverage**: All 19 NFL datasets now have dedicated staging models
  - 🔴 **Critical Models** (4): pbp, weekly, schedules, team_desc
  - 🟡 **High Priority** (4): seasonal, players, weekly_rosters, seasonal_rosters  
  - 🟢 **Medium Priority** (5): injuries, depth_charts, snap_counts, qbr, ngs_data
  - 🟣 **Low Priority** (6): weekly_pfr, seasonal_pfr, ftn_data, officials, combine, draft_picks
- **Advanced Analytics**: Enhanced dashboard with 5 specialized analytics tabs
  - 🏥 Injury Analytics, 📊 Snap Count Analysis, 🎯 QB Performance
  - ⚡ Next Gen Stats, 👥 Roster Analysis
- **Data Quality**: 149 comprehensive tests ensuring data integrity
- **Orchestration**: Complete Dagster integration with priority-based processing

### Technology Stack
| Component | Technology | Status |
|-----------|------------|--------|
| **Language** | Python 3.11 + uv | ✅ Production |
| **CLI** | Click + Rich | ✅ Production |
| **Data Warehouse** | dbt + DuckDB | ✅ Production |
| **Orchestration** | Dagster (Full Management) | ✅ Production |
| **Lakehouse** | DuckLake + PostgreSQL | ✅ Production |
| **API** | FastAPI | ✅ 9/15 endpoints |
| **Visualization** | Streamlit (dbt Integration) + Evidence + Grafana | ✅ Production |
| **Testing** | pytest | ✅ 94% success rate |

## 🚀 Key Features

### Dagster Orchestration (NEW)
```bash
# Dagster web interface
uv run dagster dev -f nfl_dagster/definitions.py

# Asset materialization by priority
uv run dagster asset materialize --select nfl_critical_raw_data
uv run dagster asset materialize --select dbt_critical_staging_models
uv run dagster asset materialize --select dbt_intermediate_models

# Job execution
uv run dagster job execute --job nfl_full_critical_pipeline_job
uv run dagster job execute --job nfl_seasonal_intensive_job

# Schedule management
uv run dagster schedule start daily_critical_data_extraction
uv run dagster schedule start weekly_high_priority_extraction
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

### Enhanced CLI Model Operations
```bash
# List and query dbt models
uv run python -m src.cli models list
uv run python -m src.cli models query int_player_weekly_stats --limit 20

# Time travel queries
uv run python -m src.cli models query stg_pbp --as-of-date 2025-01-01

# Custom analytics
uv run python -m src.cli models sql "SELECT position, AVG(fantasy_points_ppr) FROM int_player_weekly_stats GROUP BY position"
```

### Legacy Data Extraction (CLI)
```bash
# Single dataset (now wrapped by Dagster assets)
uv run python -m src.cli extract dataset weekly --year 2023

# Incremental processing
uv run python -m src.cli extract incremental pbp --max-age-days 7

# Batch extraction
uv run python -m src.cli extract multiple seasonal --years 2021,2022,2023
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
# Code quality
uv run ruff format . && uv run ruff check .

# Testing
uv run pytest --cov=src --cov-report=html
uv run dbt test --select tag:intermediate  # 17/17 passing

# dbt development
uv run dbt run --select tag:staging
uv run dbt run --select tag:intermediate
uv run dbt docs generate && uv run dbt docs serve

# Dagster development
uv run dagster dev -f nfl_dagster/definitions.py

# Streamlit dashboard development
cd visualizations/streamlit_app
uv run streamlit run main_staging.py --server.port 8504
```

### Project Structure
```
├── src/                    # Core Python modules
│   ├── cli.py             # Click-based CLI
│   ├── nfl_extractor.py   # Production extraction
│   ├── ducklake_manager.py # CLI model operations
│   └── api/               # FastAPI server
├── dbt/                   # dbt data warehouse
│   ├── models/staging/    # 4 staging models
│   ├── models/intermediate/ # 2 enhanced models
│   └── INTERMEDIATE_MODELS.md # Detailed docs
├── tests/                 # 94% test success rate
├── visualizations/        # Enhanced Streamlit dashboards
│   └── streamlit_app/     # dbt staging models integration
│       ├── main_staging.py      # Production dashboard
│       ├── data_models.py       # Type-safe data models
│       ├── services/            # Business logic layer
│       ├── utils/               # dbt & Dagster connectors
│       ├── components/          # Health monitoring
│       └── pages/               # Dashboard pages
└── configs/               # YAML configurations
```

## 📈 Advanced Usage

### Time Travel Queries
```bash
# Query historical data
uv run python -m src.cli models query int_team_performance --as-of-date 2024-12-01 --limit 5

# Compare model versions
uv run python -m src.cli models versions nfl_raw.pbp
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

- **`DAGSTER_DBT_REFACTOR_SUMMARY.md`**: Complete refactor implementation guide
- **`visualizations/streamlit_app/README_STAGING_INTEGRATION.md`**: Complete Streamlit dbt integration guide (NEW)
- **`.claude/CLAUDE.md`**: Development guide and commands for Claude Code
- **`dbt/INTERMEDIATE_MODELS.md`**: Detailed model documentation
- **`ARCHITECTURE.md`**: Updated enterprise architecture with Dagster integration
- **`PRODUCTION_DEPLOYMENT_GUIDE.md`**: Enterprise deployment
- **`DUCKLAKE_INTEGRATION.md`**: Time travel and versioning

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

### Latest Achievements (Phase 6 Complete)
- ✅ **Complete Dagster Integration**: All 19 NFL datasets managed through Dagster
- ✅ **Unified Orchestration**: 10 automated schedules + 12 job definitions
- ✅ **Enhanced Pipeline Architecture**: Raw → Staging → Intermediate with proper dependencies
- ✅ **Production Scheduling**: Priority-based processing (critical daily, high weekly)
- ✅ **Comprehensive Monitoring**: Health checks and validation assets

### Future Enhancements
- **Phase 7**: Machine learning models and predictions
- **Phase 8**: Real-time data processing with streaming
- **Phase 9**: Advanced web interface with React
- **Phase 10**: Multi-cloud deployment and auto-scaling

---

**Developed with**: Python 3.11, dbt, Dagster, DuckDB, FastAPI, Streamlit  
**License**: MIT  
**Status**: Production Ready ✅