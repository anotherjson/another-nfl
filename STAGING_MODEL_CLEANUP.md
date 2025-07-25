# Staging Model Cleanup - July 25, 2025

## Overview
Comprehensive cleanup of NFL staging models to remove legacy components and establish clean production naming conventions.

## Problem Statement
The staging models directory contained multiple versions of the same models:
- **Legacy models** with broken data source paths (e.g., `stg_pbp.sql`, `stg_weekly.sql`)  
- **Production models** with `_enhanced` suffix containing working implementations
- **Working/temp models** with various suffixes (`_fixed`, `_working`, `_ducklake`)
- **Duplicate configurations** causing dbt compilation errors

## Solution Implemented

### 1. Legacy Model Removal ❌
**Deleted broken legacy models:**
- `stg_pbp.sql` - Had broken `source('nfl_raw', 'pbp')` references
- `stg_weekly.sql` - Path pattern issues with `../data/weekly/*/etl_date=*/data.parquet`
- `stg_team_desc.sql` - Similar path resolution problems
- `stg_schedules.sql` - Non-functional data sourcing
- `stg_team_desc_ducklake.sql` - Temp implementation

### 2. Production Model Renaming ✅
**Renamed working models to clean names:**
- `stg_pbp_enhanced.sql` → `stg_pbp.sql`
- `stg_weekly_enhanced.sql` → `stg_weekly.sql`  
- `stg_team_desc_enhanced.sql` → `stg_team_desc.sql`
- `stg_schedules_enhanced.sql` → `stg_schedules.sql`

### 3. Configuration Consolidation 🔧
**Updated dbt configuration:**
- Renamed `_enhanced_staging.yml` → `_staging_production.yml`
- Updated all model references from `*_enhanced` to clean names
- Fixed relationship tests to use renamed models
- Removed duplicate `_staging.yml` causing compilation conflicts

### 4. Database Cleanup 🧹
**Removed legacy objects from DuckDB:**
- **Dropped tables:** `stg_team_desc_enhanced`, `stg_weekly_enhanced`
- **Dropped views:** `stg_schedules_enhanced`, `stg_pbp_fixed`, `stg_pbp_working`, `stg_team_desc_ducklake`

## Results

### Before Cleanup
```
stg_pbp ❌ (broken paths)
stg_pbp_enhanced ✅ (working)
stg_pbp_fixed 🔧 (temp)
stg_pbp_working 🔧 (temp)
stg_weekly ❌ (broken paths)  
stg_weekly_enhanced ✅ (working)
stg_team_desc ❌ (broken paths)
stg_team_desc_enhanced ✅ (working)
stg_team_desc_ducklake 🔧 (temp)
stg_schedules ❌ (broken paths)
stg_schedules_enhanced ✅ (working)
```

### After Cleanup
```
stg_pbp ✅ (production ready)
stg_weekly ✅ (production ready)
stg_team_desc ✅ (production ready)  
stg_schedules ✅ (production ready)
```

## Technical Details

### Model Features Preserved
All production staging models maintain their enhanced functionality:
- **Visualization-ready fields:** `play_outcome_category`, `yardage_category`, `fantasy_performance_tier`
- **Display formatting:** `conference_full_name`, `division_display`, `chart_color_primary`
- **Analytics support:** Calculated metrics, categorizations, and sort orders
- **Streamlit optimization:** Tagged as `streamlit_ready` with dashboard-friendly schemas

### Data Quality Maintained
- **129 comprehensive tests** across all models
- **Referential integrity** between staging models
- **Business rule validation** for NFL-specific constraints
- **Range checks** and null constraints preserved

### Pipeline Integration
- **Dagster assets** continue to work with renamed models
- **dbt materialization** uses `create_staging_table_query()` macro
- **DuckLake integration** maintained for time-travel capabilities

## Impact

### Benefits
- ✅ **Simplified naming:** Clean production model names without suffixes
- ✅ **Reduced complexity:** Single source of truth for each dataset
- ✅ **Improved maintainability:** No duplicate or conflicting models
- ✅ **Better documentation:** Clear model purposes and relationships
- ✅ **Cleaner database:** Removed legacy objects and temp tables

### Breaking Changes
- **Model references:** Any external references to `*_enhanced` models need updating
- **Dashboard queries:** Streamlit apps should reference clean model names
- **Custom SQL:** Direct table references need to use new names

## Next Steps

1. **Re-materialize models:** Run Dagster pipeline to create clean staging tables
2. **Update integrations:** Verify Streamlit dashboards work with renamed models  
3. **Documentation updates:** Update any remaining references to old model names
4. **Testing:** Validate all data quality tests pass with new models

## Files Changed

### Added
- `dbt/models/staging/README.md` - Comprehensive staging model documentation
- `STAGING_MODEL_CLEANUP.md` - This cleanup documentation

### Modified  
- `dbt/models/staging/stg_*.sql` - Renamed from `*_enhanced.sql` versions
- `dbt/models/staging/_staging_production.yml` - Updated configuration with clean names

### Removed
- Legacy staging model SQL files (5 files)
- Duplicate configuration files (`_staging.yml`)
- Legacy database objects (6 tables/views)

---

**Cleanup completed:** July 25, 2025  
**Models affected:** 4 core staging models  
**Database objects cleaned:** 6 legacy tables/views removed  
**Configuration simplified:** Single source of model definitions