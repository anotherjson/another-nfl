# NFL Pipeline - Quick Reference Guide

## 🎯 System Status: Phase 5 Complete ✅

Production visualization platform with Streamlit + Evidence dashboards deployed via Podman containers.

## ⚡ Essential Commands

### Environment Setup
```bash
uv sync --dev                           # Install dependencies
uv run python -m src.cli --help        # Test CLI availability (explore + extract commands)
uv run python scripts/test_phase3.py   # Test complete Phase 3 system
```

### Data Exploration (Always Works)
```bash
uv run python -m src.cli explore datasets                    # List all 19 datasets
uv run python -m src.cli explore data team_desc --limit 3    # Reliable test dataset
uv run python -m src.cli explore data pbp --year 2020        # Year-specific data
uv run python -m src.cli read data/sample_players.parquet    # Read parquet files
```

### Production Extraction (Phase 2)
```bash
# Single dataset extraction
uv run python -m src.cli extract dataset pbp --year 2023 --verbose

# Multi-year processing
uv run python -m src.cli extract multiple weekly --years 2020,2021,2022

# Smart incremental processing
uv run python -m src.cli extract incremental schedules --max-age-days 7

# Status and management
uv run python -m src.cli extract status
uv run python -m src.cli extract status pbp --verbose
uv run python -m src.cli extract cleanup --max-age-days 30 --dry-run
```

### Data Warehouse & Analytics (Phase 3)
```bash
# dbt transformations (Phase 3) - ✅ Working
uv run dbt deps                       # Install dbt packages
uv run dbt run --select tag:staging  # Run staging models (4 models working)
uv run dbt run                        # Run all models
uv run dbt test                       # Run data quality tests
uv run dbt docs generate && uv run dbt docs serve  # Documentation

# Dagster orchestration (Phase 3) - ✅ Working  
uv run dagster dev -f nfl_dagster/definitions.py   # Start Dagster UI
uv run dagster asset materialize --asset pbp_data         # Extract raw data
uv run dagster asset materialize --asset dbt_staging_models  # Run staging models

# Combined workflows - ✅ Working
uv run dbt run --select tag:staging && uv run dbt test  # dbt staging pipeline
uv run python scripts/test_phase3.py  # Complete system validation (100% success)
```

### Visualization Services (Phase 5) - ✅ ACTIVE
```bash
# Container status
podman ps                                               # Check running dashboards
podman logs nfl-streamlit --tail 10                    # Streamlit logs
podman logs nfl-evidence --tail 5                      # Evidence logs

# Dashboard access
curl http://localhost:8501/_stcore/health              # Streamlit health check
curl http://localhost:3002/                           # Evidence dashboard
open http://localhost:8501                            # Interactive analytics
open http://localhost:3002                            # Executive reports

# Container management
podman stop nfl-streamlit nfl-evidence                # Stop dashboards
podman start nfl-streamlit nfl-evidence               # Restart dashboards
podman network ls                                     # Check network config
```

### Production Deployment (Phase 4)
```bash
# Docker development environment (when Docker available)
docker-compose up -d streamlit-app evidence-app       # Start visualization services
docker-compose ps                                     # Check service status  
docker-compose logs -f streamlit-app                  # Follow dashboard logs

# Production deployment with Ansible
cd ansible
ansible-playbook -i inventories/production/hosts.yml \
  playbooks/deploy-nfl-platform.yml \
  --vault-password-file .vault_pass                   # Full deployment

# Production operations  
./scripts/production-health-check.sh                  # Comprehensive health check
./scripts/backup-nfl-data.sh                         # Manual backup
./scripts/maintenance.sh health --verbose             # System health
```

### Development
```bash
uv run ruff format . && uv run ruff check .     # Format and lint
uv run pytest tests/test_cli.py -v              # Run CLI tests (reliable)
uv run pytest tests/test_config_loader.py -v    # Configuration tests (reliable)
uv run pytest tests/test_cli_extract.py -v      # Extraction CLI tests (reliable)
uv run pytest --cov=src                         # Full test suite with coverage
```

### Troubleshooting
```bash
uv run python -m src.cli explore data team_desc              # Always works
uv run python -m src.cli extract status                      # Check extraction state
uv run python -c "from src.config_loader import ConfigLoader; print(len(ConfigLoader().list_datasets()))"  # Test config
cd dbt && dbt compile                                         # Test dbt compilation
dagster instance info                                         # Test Dagster setup
```

## 📊 Dataset Quick Reference

### Safe for Testing (No Network)
- `team_desc` - Team information (always available)
- `players` - Player information (always available)

### Year-Required Datasets (Network Dependent)
- `pbp` - Play-by-play (1999+) [Large files]
- `weekly` - Weekly stats (1999+)  
- `seasonal` - Season stats (1999+)
- `schedules` - Game schedules (1999+)
- `weekly_rosters` - Team rosters (1999+)

### Advanced Datasets
- `snap_counts` (2012+)
- `ngs_data` (2016+) 
- `ftn_data` (2018+)
- `combine` (1987+)
- `draft_picks` (1936+)

## 🔧 Common Code Patterns

### Configuration System (Phase 2)
```python
from src.config_loader import ConfigLoader

loader = ConfigLoader()
config = loader.get_dataset_config('pbp')
datasets = loader.list_datasets()  # All 19 datasets
path = loader.get_output_path('pbp', year=2023, etl_date='2024-01-15')
```

### Production Extraction (Phase 2)
```python
from src.nfl_extractor import NFLDataExtractor

extractor = NFLDataExtractor()
data, metadata = extractor.extract_dataset('pbp', year=2023, validate=True, save_to_disk=True)
results = extractor.extract_multiple_years('weekly', [2020, 2021, 2022])
```

### Incremental Processing (Phase 2) 
```python
from src.extraction_manager import ExtractionManager

manager = ExtractionManager()
summary = manager.extract_incremental('pbp', years=[2020, 2021], max_age_days=7)
status = manager.get_extraction_summary('weekly')
```

### dbt Operations (Phase 3)
```python
from dagster.resources.dbt_resource import DbtResource

dbt = DbtResource()
result = dbt.run(select="tag:staging")    # Run staging models
test_result = dbt.test()                  # Run data tests
docs_result = dbt.docs_generate()         # Generate documentation
```

### Dagster Assets (Phase 3)
```python
from dagster import asset, AssetExecutionContext

@asset(description="Extract NFL team data")
def team_data(context: AssetExecutionContext):
    # Asset implementation
    return {"success": True}
```

### CLI Testing
```python
from click.testing import CliRunner
from src.cli import main

runner = CliRunner()
result = runner.invoke(main, ['extract', 'status'])
assert result.exit_code == 0
```

## ⚠️ Important Notes

### What's Normal vs. What's Broken

#### ✅ Normal (Don't Worry)
- Some NFL datasets fail with network errors
- "name 'Error' is not defined" messages from external APIs
- Test failures in `test_nfl_explorer.py` due to mocking issues
- API rate limits or timeouts
- Individual year/dataset extraction failures

#### 🚨 Fix These  
- CLI doesn't respond to `--help`
- Configuration system fails to load
- Import errors in Python modules
- `team_desc` dataset fails to load
- All CLI/config tests fail

### File Structure
```
src/
  cli.py                 # Main CLI (explore + extract commands)
  config_loader.py       # YAML configuration system
  nfl_extractor.py       # Production extraction engine
  extraction_manager.py  # Incremental processing
  nfl_explorer.py        # Data exploration
  parquet_reader.py      # File operations
dbt/                     # Data warehouse (Phase 3)
  models/staging/        # Raw data cleaning models
  models/intermediate/   # Business logic models  
  models/marts/         # Analytics-ready models
nfl_dagster/            # Pipeline orchestration (Phase 3) - ✅ Working
  assets/               # Data assets (raw + dbt)
  resources/           # DuckDB and dbt resources
configs/datasets/        # 19 YAML configuration files
tests/                   # 117+ comprehensive tests
scripts/                 # Phase 3 validation scripts
```

## 🎯 Success Checklist

- [ ] `uv run python -m src.cli --help` shows explore + extract commands
- [ ] Configuration loads 19 datasets: `ConfigLoader().list_datasets()`
- [ ] `team_desc` dataset extracts successfully  
- [ ] `extract status` command works
- [ ] CLI tests pass consistently
- [ ] Error messages are helpful and clear
- [ ] Phase 3 validation passes: `uv run python scripts/test_phase3.py`
- [ ] dbt models compile: `cd dbt && dbt compile`
- [ ] Dagster definitions load: `dagster instance info`

## Validation Checklist

Quick checks to verify everything works:

```bash
# 1. Environment
uv run python -m src.cli --help

# 2. Configuration system
uv run python -c "from src.config_loader import ConfigLoader; print(f'Datasets: {len(ConfigLoader().list_datasets())}')"

# 3. Data exploration
uv run python -m src.cli explore data team_desc --limit 2

# 4. Production extraction
uv run python -m src.cli extract status

# 5. Error handling
uv run python -m src.cli explore data invalid_dataset

# 6. Code quality
uv run ruff check .

# 7. Tests
uv run pytest tests/test_cli.py -v
uv run pytest tests/test_config_loader.py -v

# 8. Phase 3 validation
uv run python scripts/test_phase3.py
```

If all the above work, the system is functioning correctly.

## 📖 Need More Info?

- **NEW_CLAUDE_ONBOARDING.md** - Comprehensive onboarding for new contributors
- **CLAUDE.md** - Complete technical reference
- **README.md** - User-facing documentation
- **TROUBLESHOOTING.md** - Problem-solving guide

## Key Reminders

1. **Use `uv run`** for all Python commands
2. **Test with `team_desc`** dataset first (always works)
3. **Configuration system** loads all 19 datasets automatically
4. **CLI tests are reliable** - focus on those for validation
5. **Network errors are expected** with some NFL datasets
6. **Phase 3 is complete** - Full data warehouse and orchestration ready
7. **117+ test cases** provide comprehensive coverage
8. **dbt models** provide analytics-ready data transformations
9. **Dagster pipeline** orchestrates the complete data flow

---

*Last Updated: Phase 5 completion - Production visualization platform with Streamlit + Evidence dashboards running on Podman*

## 🏈 Current Visualization Services

- **Streamlit Analytics**: http://localhost:8501 - Interactive NFL analytics with team performance, fantasy football, and betting intelligence
- **Evidence Reports**: http://localhost:3002 - SQL-based executive dashboards with automated reporting
- **Container Status**: `podman ps` shows nfl-streamlit and nfl-evidence running on nfl_network

**🎉 The NFL data pipeline is now a complete end-to-end analytics platform with production visualization capabilities!**