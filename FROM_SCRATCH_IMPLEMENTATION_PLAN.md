# NFL Analytics Platform - From Scratch Implementation Plan

## 🎯 Project Overview

Build a production-grade NFL data analytics platform using modern data engineering practices with functional programming architecture, complete orchestration, and enterprise deployment capabilities.

**Target Architecture**: Functional Programming + dbt + Dagster + DuckLake + FastAPI + Streamlit

## 📋 Implementation Phases

### Phase 1: Foundation & Environment Setup (Week 1-2)

#### 1.1 Development Environment
```bash
# Essential setup tasks
- Python 3.11+ environment with uv package manager
- Git repository initialization with proper .gitignore
- Pre-commit hooks configuration (ruff, pytest, mypy)
- Docker and docker-compose setup for local development
- Virtual environment with dependency management
```

#### 1.2 Core Dependencies
```toml
# pyproject.toml foundation
[project]
dependencies = [
    # Core data processing
    "pandas>=2.0.0",
    "pyarrow>=12.0.0", 
    "duckdb>=0.10.0",
    "nfl_data_py>=0.3.0",
    
    # Functional programming
    "toolz>=0.12.0",
    "returns>=0.22.0", 
    "immutables>=0.20",
    "pyrsistent>=0.20.0",
    
    # CLI and UI
    "click>=8.1.0",
    "rich>=13.0.0",
    "streamlit>=1.32.0",
    
    # Data warehouse and orchestration
    "dbt-core>=1.8.0",
    "dbt-duckdb>=1.8.0", 
    "dagster>=1.8.0",
    "dagster-dbt>=0.24.0",
    
    # API and web
    "fastapi>=0.104.0",
    "uvicorn>=0.24.0",
    "pydantic>=2.4.0",
    
    # Database and lakehouse
    "psycopg2-binary>=2.9.0",
    "python-dotenv>=1.0.0",
]

[project.optional-dependencies]
dev = [
    "pytest>=7.4.0",
    "pytest-cov>=4.1.0",
    "ruff>=0.1.0",
    "pre-commit>=3.4.0",
    "hypothesis>=6.0.0",  # Property-based testing
]
```

#### 1.3 Project Structure
```
nfl-analytics/
├── src/
│   ├── __init__.py
│   ├── cli.py                    # Functional CLI entry point
│   ├── functional_utils.py       # Core functional utilities
│   ├── functional_extraction.py  # Pure extraction functions
│   ├── functional_processing.py  # Immutable data processing
│   ├── functional_materialization.py # Functional dbt/Dagster
│   ├── functional_querying.py    # Pure querying functions
│   ├── config_loader.py          # Configuration management
│   └── api/                      # FastAPI server
├── dbt/                          # dbt data warehouse
│   ├── dbt_project.yml
│   ├── profiles.yml
│   ├── models/
│   │   ├── staging/             # Raw data staging
│   │   ├── intermediate/        # Business logic
│   │   └── marts/              # Analytics-ready
│   ├── macros/
│   └── tests/
├── nfl_dagster/                 # Pipeline orchestration
│   ├── __init__.py
│   ├── definitions.py
│   ├── assets/
│   ├── jobs/
│   ├── schedules/
│   └── resources/
├── tests/                       # Comprehensive testing
├── configs/                     # YAML configurations
├── docker/                      # Container configurations
├── ansible/                     # Deployment automation
└── docs/                        # Documentation
```

### Phase 2: Functional Programming Foundation (Week 2-3)

#### 2.1 Core Functional Utilities (`src/functional_utils.py`)
```python
# Key components to implement:

@dataclass(frozen=True)
class CLIResult(Generic[T]):
    """Immutable result container with monadic interface"""
    success: bool
    data: T | None
    error: str | None
    
    def map(self, func: Callable[[T], R]) -> CLIResult[R]:
        """Apply function to data if successful"""
    
    def flat_map(self, func: Callable[[T], CLIResult[R]]) -> CLIResult[R]:
        """Monadic bind for chaining operations"""

@dataclass(frozen=True) 
class ExtractionConfig:
    """Immutable extraction configuration"""
    dataset_name: str
    year: int | None
    validate: bool
    save_to_disk: bool
    output_path: Path | None

# Function composition utilities
def compose(*functions): """Compose functions right-to-left"""
def pipe(data, *functions): """Pipe data through functions left-to-right"""
def curry(func): """Transform function to allow partial application"""
def safe_call(func): """Decorator for safe function calls with error handling"""
```

#### 2.2 Property-Based Testing Setup
```python
# tests/test_functional_utils.py
from hypothesis import given, strategies as st

@given(st.integers())
def test_cli_result_map_preserves_success(value):
    """Property: mapping over successful result preserves success"""
    result = CLIResult.ok(value)
    mapped = result.map(lambda x: x * 2)
    assert mapped.success

@given(st.text())
def test_cli_result_error_propagation(error_msg):
    """Property: errors propagate through map operations"""
    result = CLIResult.error(error_msg)
    mapped = result.map(lambda x: x * 2)
    assert not mapped.success
    assert mapped.error == error_msg
```

### Phase 3: Data Extraction Layer (Week 3-4)

#### 3.1 Pure Extraction Functions (`src/functional_extraction.py`)
```python
# Core extraction pipeline functions:

@safe_call
def extract_single_dataset(config: ExtractionConfig) -> ExtractionMetadata:
    """Pure function to extract single NFL dataset"""
    
@safe_call  
def extract_multiple_years(dataset_name: str, years: list[int]) -> tuple[ExtractionMetadata, ...]:
    """Extract dataset across multiple years immutably"""

@safe_call
def validate_extraction(data: pd.DataFrame, config: DatasetConfig) -> ValidationReport:
    """Pure validation function returning immutable report"""

# Pipeline composition
def build_extraction_pipeline(config: ExtractionConfig) -> Pipeline[ExtractionMetadata]:
    """Compose extraction pipeline from pure functions"""
    return compose(
        validate_extraction,
        extract_single_dataset,
        load_dataset_config
    )
```

#### 3.2 Dataset Configuration System
```yaml
# configs/datasets/pbp.yaml
name: pbp
description: "Play-by-play data with EPA metrics"  
function_name: import_pbp_data
start_year: 1999
requires_year: true
priority: critical
validation:
  required_columns: ["game_id", "play_id", "epa"]
  expected_size_mb: 500
  data_quality_checks:
    - no_nulls: ["game_id", "play_id"]
    - value_ranges:
        epa: [-10, 10]
        down: [1, 4]
```

#### 3.3 CLI Commands Implementation
```bash
# Target CLI interface:
nfl extract all --year 2024 --priority critical
nfl extract dataset pbp --year 2024 --validate  
nfl extract status
nfl extract cleanup --dry-run --max-age-days 30
```

### Phase 4: Data Processing Layer (Week 4-5)

#### 4.1 Immutable Data Processing (`src/functional_processing.py`)
```python
# Key processing functions:

@safe_call
def freeze_dataframe(df: pd.DataFrame) -> ImmutableDataFrame:
    """Convert DataFrame to immutable structure"""

@safe_call  
def process_parquet_file(file_path: Path, limit: int | None = None) -> ProcessingResult:
    """Pure function to process parquet files"""

@safe_call
def scan_data_directory(base_path: Path) -> DataCatalog:
    """Scan directory and return immutable catalog"""

@safe_call
def validate_all_datasets(base_path: Path) -> ValidationSummary:
    """Validate all datasets and return summary"""

# Pipeline functions
def build_processing_pipeline(config: ProcessingConfig) -> Pipeline[ProcessingResult]:
    """Compose processing pipeline"""
```

#### 4.2 CLI Commands
```bash
# Target processing interface:
nfl process read data/pbp/2024/pbp_2024.parquet --limit 10 --validate
nfl process catalog --base-path data
nfl process validate --verbose
```

### Phase 5: dbt Data Warehouse (Week 5-6)

#### 5.1 dbt Project Structure
```sql
-- dbt/models/staging/stg_pbp.sql
{{ config(materialized='view') }}

select
    game_id,
    play_id,
    season,
    week,
    down,
    epa,
    -- Add standardized columns
    created_at,
    updated_at
from {{ source('nfl_raw', 'pbp_data') }}
where game_id is not null
  and play_id is not null
```

#### 5.2 dbt Configuration
```yaml
# dbt/dbt_project.yml
name: 'nfl_analytics'
version: '1.0.0'

models:
  nfl_analytics:
    staging:
      +materialized: view
      +docs:
        node_color: "#8FBC8F"
    intermediate:
      +materialized: table
      +docs:
        node_color: "#4682B4"
    marts:
      +materialized: table
      +docs:
        node_color: "#CD853F"

# Global variables
vars:
  current_season: 2024
  regular_season_weeks: 18
```

#### 5.3 Staging Models (19 models total)
```
Priority Model Implementation Order:
1. Critical (Week 5): stg_pbp, stg_weekly, stg_schedules, stg_team_desc
2. High (Week 6): stg_seasonal, stg_players, stg_weekly_rosters, stg_seasonal_rosters
3. Medium (Week 7): stg_injuries, stg_qbr, stg_ngs_data, stg_snap_counts, stg_depth_charts
4. Low (Week 8): stg_weekly_pfr, stg_seasonal_pfr, stg_ftn_data, stg_officials, stg_combine, stg_draft_picks
```

### Phase 6: Dagster Orchestration (Week 6-7)

#### 6.1 Asset Definitions (`nfl_dagster/assets/`)
```python
# nfl_dagster/assets/nfl_raw_data_assets.py
from dagster import asset, AssetExecutionContext

@asset(group_name="nfl_critical_raw_data")
def pbp_raw_data(context: AssetExecutionContext) -> None:
    """Extract PBP data using functional extraction pipeline"""
    config = ExtractionConfig(
        dataset_name="pbp",
        year=2024,
        validate=True,
        save_to_disk=True,
        output_path=Path("data/pbp/2024")
    )
    result = extract_single_dataset(config)
    # Handle result and register with DuckLake
```

#### 6.2 Job Definitions
```python
# nfl_dagster/jobs/nfl_pipeline_jobs.py
from dagster import define_asset_job

nfl_critical_raw_data_job = define_asset_job(
    name="nfl_critical_raw_data_job",
    selection=["pbp_raw_data", "weekly_raw_data", "schedules_raw_data", "team_desc_raw_data"]
)

nfl_critical_staging_job = define_asset_job(
    name="nfl_critical_staging_job", 
    selection=["stg_pbp", "stg_weekly", "stg_schedules", "stg_team_desc"]
)
```

#### 6.3 Scheduling System
```python
# nfl_dagster/schedules/nfl_data_schedules.py  
from dagster import schedule

@schedule(
    job=nfl_critical_raw_data_job,
    cron_schedule="0 6 * * *",  # Daily at 6 AM
)
def daily_critical_data_extraction(context):
    """Schedule critical data extraction daily"""
    return {}
```

### Phase 7: DuckLake Lakehouse Integration (Week 7-8)

#### 7.1 DuckLake Manager
```python
# src/ducklake_manager.py
class DuckLakeManager:
    """Manage DuckLake lakehouse with time travel capabilities"""
    
    def register_table(self, table_name: str, parquet_path: Path) -> None:
        """Register parquet files as DuckLake table"""
        
    def query_model(self, model_name: str, as_of_date: str | None = None) -> pd.DataFrame:
        """Query model with optional time travel"""
        
    def get_catalog_tables(self) -> list[str]:
        """List all available tables in catalog"""
```

#### 7.2 Time Travel Queries
```python
# src/functional_querying.py
@safe_call
def time_travel_query(model_name: str, as_of_date: str, limit: int | None = None) -> QueryResult:
    """Execute time travel query on DuckLake"""

@safe_call  
def query_staging_model(model_name: str, limit: int | None = None) -> QueryResult:
    """Query current version of staging model"""
```

#### 7.3 CLI Integration
```bash
# Target querying interface:
nfl query staging stg_pbp --limit 10
nfl query staging stg_weekly --as-of-date 2024-01-15
nfl query sql "SELECT team, COUNT(*) FROM stg_team_desc GROUP BY team"
nfl query models --verbose
```

### Phase 8: Functional Materialization (Week 8-9)

#### 8.1 dbt-Dagster Integration (`src/functional_materialization.py`)
```python
@safe_call
def materialize_by_priority(priority: str, run_tests: bool = True) -> MaterializationResult:
    """Materialize models by priority level using functional approach"""

@safe_call
def materialize_specific_models(model_names: list[str], run_tests: bool = True) -> MaterializationResult:
    """Materialize specific models functionally"""

@safe_call
def get_materialization_status() -> MaterializationStatus:
    """Get current status of all materialized models"""

# Pipeline composition  
def build_materialization_pipeline(config: MaterializationConfig) -> Pipeline[MaterializationResult]:
    """Compose materialization pipeline from pure functions"""
```

#### 8.2 CLI Commands
```bash
# Target materialization interface:
nfl materialize all --tests
nfl materialize staging --priority critical --tests
nfl materialize models stg_pbp stg_weekly
nfl materialize status
```

### Phase 9: FastAPI REST API (Week 9-10)

#### 9.1 API Structure (`src/api/`)
```python
# src/api/main.py
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

app = FastAPI(title="NFL Analytics API", version="1.0.0")

@app.get("/health")
async def health_check():
    """System health endpoint"""
    
@app.get("/api/v1/datasets")
async def list_datasets():
    """List all available datasets"""

@app.get("/api/v1/players/stats")
async def get_player_stats(position: str = None, season: int = None):
    """Get player statistics with filtering"""
```

#### 9.2 Data Models
```python
# src/api/models.py
from pydantic import BaseModel
from typing import Optional

class PlayerStats(BaseModel):
    player_id: str
    player_name: str
    position: str
    team: str
    season: int
    fantasy_points: float

class TeamInfo(BaseModel):
    team_id: str
    team_name: str
    conference: str
    division: str
```

#### 9.3 Integration with Functional Layer
```python
# Use functional querying for API endpoints
@app.get("/api/v1/staging/{model_name}")
async def query_staging_model_api(model_name: str, limit: int = 100):
    """Query staging model via API"""
    result = query_staging_model(model_name, limit)
    if not result.success:
        raise HTTPException(status_code=500, detail=result.error)
    return result.data
```

### Phase 10: Streamlit Visualization (Week 10-11)

#### 10.1 Dashboard Architecture
```python
# visualizations/streamlit_app/main.py
import streamlit as st
from services.staging_service import StagingService

def main():
    st.set_page_config(page_title="NFL Analytics", layout="wide")
    
    # Initialize services
    staging_service = StagingService()
    
    # Multi-page application
    pages = {
        "Overview": show_overview_page,
        "Team Analysis": show_team_analysis, 
        "Fantasy Dashboard": show_fantasy_dashboard,
        "System Health": show_system_health
    }
```

#### 10.2 dbt Integration
```python
# visualizations/streamlit_app/services/staging_service.py
class StagingService:
    """Service for accessing dbt staging models"""
    
    def get_team_performance_data(self) -> pd.DataFrame:
        """Get team performance from staging models"""
        result = query_staging_model("stg_weekly")
        return result.unwrap() if result.success else pd.DataFrame()
    
    def get_player_stats(self, position: str = None) -> pd.DataFrame:
        """Get player statistics with filtering"""
```

#### 10.3 Caching Strategy  
```python
# Intelligent multi-level caching
@st.cache_data(ttl=1800)  # 30 min TTL for team data
def load_team_data():
    """Load team data with caching"""

@st.cache_data(ttl=600)   # 10 min TTL for player data  
def load_player_data():
    """Load player data with caching"""

@st.cache_data(ttl=60)    # 1 min TTL for health data
def load_health_data():
    """Load system health data"""
```

### Phase 11: Testing & Quality Assurance (Week 11-12)

#### 11.1 Comprehensive Testing Strategy
```python
# tests/test_functional_integration.py
import pytest
from hypothesis import given, strategies as st

class TestFunctionalPipeline:
    """Integration tests for complete functional pipeline"""
    
    def test_extract_process_materialize_query_pipeline(self):
        """Test complete workflow integration"""
        # Extract → Process → Materialize → Query
        
    @given(st.text(), st.integers(min_value=1999, max_value=2024))
    def test_extraction_with_property_based_testing(self, dataset_name, year):
        """Property-based testing for extraction"""
```

#### 11.2 dbt Testing
```sql
-- dbt/tests/test_data_quality.sql
select 
    'pbp_data' as model,
    count(*) as row_count,
    count(distinct game_id) as unique_games,
    min(season) as min_season,
    max(season) as max_season
from {{ ref('stg_pbp') }}
having count(*) = 0  -- Test should fail if no data
```

#### 11.3 Quality Gates
```bash
# Quality assurance commands
uv run ruff format . && uv run ruff check .     # Code formatting
uv run pytest --cov=src --cov-report=html      # Unit testing  
uv run dbt test --select tag:staging           # dbt model tests
uv run dagster asset materialize --select "*"  # Integration tests
```

### Phase 12: Production Deployment (Week 12-13)

#### 12.1 Containerization (`docker/`)
```dockerfile
# docker/nfl-app/Dockerfile
FROM python:3.11-slim

# Install uv package manager
RUN pip install uv

# Copy and install dependencies
COPY pyproject.toml uv.lock ./
RUN uv sync --frozen

# Copy application code
COPY src/ ./src/
COPY dbt/ ./dbt/
COPY nfl_dagster/ ./nfl_dagster/

EXPOSE 8000
CMD ["uv", "run", "uvicorn", "src.api.main:app", "--host", "0.0.0.0", "--port", "8000"]
```

#### 12.2 Docker Compose
```yaml
# docker-compose.yml
version: '3.8'
services:
  nfl-app:
    build: ./docker/nfl-app
    ports:
      - "8000:8000"
    environment:
      - DATABASE_URL=postgresql://user:pass@postgres:5432/nfl_db
    depends_on:
      - postgres
      
  dagster:
    build: ./docker/dagster-server  
    ports:
      - "3000:3000"
    volumes:
      - ./data:/opt/dagster/data
      
  postgres:
    image: postgres:15
    environment:
      POSTGRES_DB: nfl_db
      POSTGRES_USER: user
      POSTGRES_PASSWORD: pass
    volumes:
      - postgres_data:/var/lib/postgresql/data

volumes:
  postgres_data:
```

#### 12.3 Ansible Deployment
```yaml
# ansible/playbooks/deploy-nfl-platform.yml
---
- hosts: nfl_servers
  become: yes
  vars:
    app_name: nfl-analytics
    docker_compose_version: "2.20.0"
    
  tasks:
    - name: Install Docker and dependencies
      apt:
        name: ['docker.io', 'docker-compose']
        state: present
        
    - name: Deploy application stack
      docker_compose:
        project_src: "{{ app_directory }}"
        state: present
```

### Phase 13: Monitoring & Operations (Week 13-14)

#### 13.1 Health Monitoring
```python
# src/health_monitor.py
@safe_call
def check_system_health() -> HealthReport:
    """Comprehensive system health check"""
    return HealthReport(
        database_status=check_database_connection(),
        dbt_models_status=check_dbt_models(),
        dagster_status=check_dagster_daemon(),
        data_freshness=check_data_freshness(),
        disk_usage=check_disk_usage()
    )
```

#### 13.2 Grafana Dashboards
```json
# monitoring/grafana/dashboards/nfl-platform-overview.json
{
  "dashboard": {
    "title": "NFL Platform Overview",
    "panels": [
      {
        "title": "Data Pipeline Health",
        "type": "stat"
      },
      {
        "title": "API Response Times", 
        "type": "graph"
      }
    ]
  }
}
```

#### 13.3 Automated Backup
```bash
# scripts/backup-nfl-data.sh
#!/bin/bash
# Automated backup of NFL data and metadata

# Backup DuckDB files
pg_dump nfl_catalog > backup/catalog_$(date +%Y%m%d).sql

# Backup parquet data
rsync -av data/ backup/data_$(date +%Y%m%d)/

# Upload to S3 with encryption
aws s3 sync backup/ s3://nfl-analytics-backup/ --sse
```

## 🎯 Success Criteria & Milestones

### Technical Milestones
- [ ] **Week 2**: Functional programming foundation complete
- [ ] **Week 4**: Complete data extraction pipeline (19 datasets)
- [ ] **Week 6**: All 19 dbt staging models operational  
- [ ] **Week 8**: Dagster orchestration with scheduling
- [ ] **Week 10**: DuckLake time travel queries working
- [ ] **Week 12**: FastAPI with 15+ endpoints operational
- [ ] **Week 13**: Streamlit dashboard with dbt integration
- [ ] **Week 14**: Production deployment with monitoring

### Quality Gates
- [ ] **100% Pure Functions**: All core logic as pure functions
- [ ] **Property-Based Testing**: Hypothesis tests for all utilities
- [ ] **Test Coverage**: >90% test coverage across all modules
- [ ] **dbt Tests**: 100% of model tests passing
- [ ] **Documentation**: Complete API docs and user guides
- [ ] **Performance**: Sub-second response times for API endpoints

### Business Value
- [ ] **Complete NFL Coverage**: All 19 NFL datasets accessible
- [ ] **Real-time Analytics**: Live dashboard updates
- [ ] **Historical Analysis**: Time travel queries for any date
- [ ] **API Integration**: Full REST API for external systems
- [ ] **Production Ready**: Automated deployment and monitoring

## 🔧 Technical Decisions & Rationale

### Functional Programming Choice
- **Pure Functions**: Easier testing, debugging, and reasoning
- **Immutable Data**: Prevents mutation bugs and race conditions
- **Monadic Error Handling**: Clean error propagation without exceptions
- **Composability**: Build complex pipelines from simple functions

### Technology Stack Justification
- **dbt**: Industry standard for data transformation with SQL
- **Dagster**: Modern orchestration with asset-centric approach
- **DuckDB**: High-performance analytical database for local development
- **DuckLake**: Time travel capabilities for data versioning
- **FastAPI**: Fast, modern API framework with automatic documentation
- **Streamlit**: Rapid dashboard development with Python integration

### Data Architecture Decisions
- **Lakehouse Pattern**: Combines data lake flexibility with warehouse reliability
- **Staging → Intermediate → Marts**: Standard dbt modeling approach
- **Priority-Based Processing**: Critical data processed more frequently
- **Time Travel**: Historical analysis capabilities for compliance and debugging

## 🚀 Getting Started Implementation

### Immediate Next Steps (Week 1)
1. **Environment Setup**:
   ```bash
   mkdir nfl-analytics && cd nfl-analytics
   python3.11 -m pip install uv
   uv init --python 3.11
   uv add click rich pandas pyarrow toolz returns
   ```

2. **Basic Project Structure**:
   ```bash
   mkdir -p src/{api,tests} dbt/{models/{staging,intermediate,marts},macros} nfl_dagster/{assets,jobs,schedules}
   touch src/{__init__.py,cli.py,functional_utils.py}
   ```

3. **Core Functional Utilities**: Start with `CLIResult` monad and basic composition functions

4. **First Extraction Function**: Implement extraction for single high-priority dataset (pbp)

5. **Basic CLI**: Simple CLI with extract command for testing

### Implementation Priority Order
1. **Functional Foundation** → **Data Extraction** → **dbt Staging**
2. **Dagster Orchestration** → **DuckLake Integration** → **CLI Commands**
3. **FastAPI** → **Streamlit Dashboard** → **Testing**
4. **Production Deployment** → **Monitoring** → **Documentation**

This plan provides a comprehensive roadmap for building the NFL analytics platform from scratch, incorporating all lessons learned and best practices from the current implementation while following functional programming principles throughout.