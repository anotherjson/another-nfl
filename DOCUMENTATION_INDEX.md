# NFL Analytics Platform - Documentation Index

## 🚀 Getting Started

1. **[README.md](README.md)** - Main project overview, setup, and usage
2. **[.claude/NEW_CLAUDE_ONBOARDING.md](.claude/NEW_CLAUDE_ONBOARDING.md)** - Complete onboarding for new contributors
3. **[QUICK_REFERENCE.md](QUICK_REFERENCE.md)** - Essential commands and quick reference

## 📋 Core Documentation

### Development & Operations
- **[.claude/CLAUDE.md](.claude/CLAUDE.md)** - Complete Dagster-managed pipeline guide (Claude Code)
- **[DAGSTER_DBT_REFACTOR_SUMMARY.md](DAGSTER_DBT_REFACTOR_SUMMARY.md)** - Complete refactor implementation (NEW)
- **[TROUBLESHOOTING.md](TROUBLESHOOTING.md)** - Problem-solving and debugging guide
- **[OPERATIONS_RUNBOOK.md](OPERATIONS_RUNBOOK.md)** - Production operations procedures

### Technical Specifications  
- **[ARCHITECTURE.md](ARCHITECTURE.md)** - Updated enterprise architecture with Dagster integration
- **[API_REFERENCE.md](API_REFERENCE.md)** - Comprehensive API documentation
- **[REALTIME_GUIDE.md](REALTIME_GUIDE.md)** - Real-time processing implementation

### Deployment & Integration
- **[PRODUCTION_DEPLOYMENT_GUIDE.md](PRODUCTION_DEPLOYMENT_GUIDE.md)** - Production deployment procedures
- **[DUCKLAKE_INTEGRATION.md](DUCKLAKE_INTEGRATION.md)** - Complete DuckLake lakehouse integration

## 📊 Development History

- **[.claude/PHASE5_ACCOMPLISHMENTS.md](.claude/PHASE5_ACCOMPLISHMENTS.md)** - Recent quality improvements and milestones

## 🗂️ Specialized Documentation

### Visualization
- **[visualizations/README.md](visualizations/README.md)** - Dashboard architecture and usage

### Tools & References  
- **[references/tool_references.md](references/tool_references.md)** - External tool documentation

### Claude Code Documentation
- **[.claude/](.claude/)** - All Claude Code specific documentation
  - **[CLAUDE.md](.claude/CLAUDE.md)** - Main instruction file and development guide
  - **[CLI_REFERENCE.md](.claude/CLI_REFERENCE.md)** - Command reference
  - **[NEW_CLAUDE_ONBOARDING.md](.claude/NEW_CLAUDE_ONBOARDING.md)** - Onboarding guide
  - **[TROUBLESHOOTING_CLI.md](.claude/TROUBLESHOOTING_CLI.md)** - CLI troubleshooting

---

## Documentation Usage Guide

### For New Users
1. Start with **README.md** for project overview
2. Use **QUICK_REFERENCE.md** for essential commands
3. Check **TROUBLESHOOTING.md** if issues arise

### For New Contributors
1. Read **.claude/NEW_CLAUDE_ONBOARDING.md** for complete setup
2. Reference **.claude/CLAUDE.md** for development guidelines
3. Use **ARCHITECTURE.md** for system understanding

### For Production Deployment
1. Follow **PRODUCTION_DEPLOYMENT_GUIDE.md** 
2. Reference **OPERATIONS_RUNBOOK.md** for ongoing operations
3. Use **DUCKLAKE_INTEGRATION.md** for lakehouse setup

### For Advanced Features
1. **API_REFERENCE.md** for REST API implementation
2. **REALTIME_GUIDE.md** for real-time processing
3. **ARCHITECTURE.md** for enterprise architecture

---

## 📝 Documentation Updates Summary

**Latest Update:** July 25, 2025

### Dagster Integration Documentation (NEW):
- **Added `DAGSTER_DBT_REFACTOR_SUMMARY.md`**: Complete implementation guide for Dagster-managed pipeline
- **Updated `README.md`**: New Dagster orchestration commands and workflow
- **Updated `ARCHITECTURE.md`**: Complete pipeline flow with asset dependencies
- **Updated `.claude/CLAUDE.md`**: Primary Dagster commands with legacy CLI reference
- **Updated `QUICK_REFERENCE.md`**: Dagster-first workflow with essential commands

### Key Changes:
- **New Architecture**: Complete Dagster management of all 19 NFL datasets
- **Updated Commands**: Dagster-first approach with legacy CLI still available
- **Production Focus**: 10 automated schedules + 12 job definitions documented
- **Asset Management**: Priority-based processing (critical/high/medium/low)
- **Comprehensive Coverage**: All documentation updated to reflect new pipeline

### Implementation Status:
- ✅ **Phase 6 Complete**: Full Dagster integration with unified orchestration
- ✅ **All 19 NFL datasets** managed as Dagster assets
- ✅ **Production schedules** with automated processing
- ✅ **17/17 intermediate model tests** still passing

*Documentation now reflects complete Dagster-managed architecture.*