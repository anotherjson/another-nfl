# NFL Platform Architecture

Production-ready data platform for NFL analytics with enterprise-scale architecture, containerization, and operational excellence.

## System Overview

### Core Components
| Layer | Technology | Purpose | Status |
|-------|------------|---------|--------|
| **CLI** | Click + Rich | Data operations & model management | ✅ Production |
| **Data Warehouse** | dbt + DuckDB | Staging & intermediate models | ✅ Production |
| **Orchestration** | Dagster | Pipeline scheduling & monitoring | ✅ Production |
| **Lakehouse** | DuckLake + PostgreSQL | Time travel & versioning | ✅ Production |
| **API** | FastAPI + Pydantic | REST endpoints & documentation | ✅ 9/15 endpoints |
| **Visualization** | Streamlit + Evidence + Grafana | Dashboards & monitoring | ✅ Production |
| **Infrastructure** | Docker + Ansible | Containerization & deployment | ✅ Production |

## Data Architecture

### Pipeline Flow
```
NFL Data Sources → Extraction → Staging → Intermediate → Marts → Analytics
     (19 datasets)      ↓          ↓           ↓          ↓         ↓
    nfl_data_py    Parquet     dbt Models   Enhanced    Future   Dashboards
                   Files       (4 models)   Analytics   Models      API
                                           (2 models)
```

### Data Layers
1. **Raw Data**: Parquet files partitioned by year and ETL date
2. **Staging**: 4 dbt models for cleaning and normalization
3. **Intermediate**: 2 enhanced models with EPA analytics and advanced metrics
4. **Marts**: Analytics-ready models (future development)
5. **Serving**: API endpoints and visualization dashboards

### Enhanced Intermediate Models
**int_team_performance**: Team efficiency analytics
- Down conversion rates (3rd/4th down success)
- EPA (Expected Points Added) per play
- Win percentages and season progression
- Play-calling distribution and efficiency

**int_player_weekly_stats**: Player performance analytics
- Position rankings (weekly + season-to-date)
- 4-week rolling fantasy averages
- EPA per opportunity metrics
- Air yards analytics and YAC efficiency

## Technology Architecture

### CLI Model Operations (NEW)
Advanced CLI interface for dbt model management:

```
CLI Commands → DuckLakeManager → DuckDB/PostgreSQL → Rich Output
     ↓               ↓                   ↓              ↓
  Click Parser   Query Engine      Data Sources   Formatted Tables
  Validation     Dagster Trigger   Time Travel    Progress Bars
  Parameters     Schema Inspection Version Control Error Handling
```

**Key Features**:
- Direct dbt model querying with time travel
- Dagster asset materialization triggers
- Custom SQL execution against DuckLake
- Interactive catalog browsing
- Rich formatted output with progress tracking

### Data Warehouse Architecture
**dbt Project Structure**:
```
dbt/
├── models/
│   ├── staging/           # 4 models (light cleaning)
│   ├── intermediate/      # 2 models (EPA analytics)
│   └── marts/            # Future (analytics-ready)
├── macros/               # Reusable SQL functions
├── tests/                # Data quality tests (17/17 passing)
└── docs/                 # Auto-generated documentation
```

**DuckDB Integration**:
- High-performance analytical queries
- Native Parquet support with pushdown optimization
- Memory-efficient processing for large datasets
- Integration with PostgreSQL for metadata management

### DuckLake Lakehouse
**Time Travel & Versioning**:
```
PostgreSQL Catalog ←→ DuckDB Engine ←→ Parquet Files
      ↓                    ↓                ↓
  Table Metadata      Query Engine     Data Storage
  Version History     Schema Evolution  Partitioning
  ACID Transactions   Performance Opts  Compression
```

**Capabilities**:
- Query data "as of" any specific date
- Complete version history and audit trail
- ACID transactions for data consistency
- Schema evolution with backward compatibility

## API Architecture

### FastAPI Implementation
**Operational Status**: 9/15 endpoints working (60% functional)

| Endpoint Category | Status | Description |
|------------------|--------|-------------|
| Health & System | ✅ Working | `/health`, `/metrics` |
| Dataset Info | ✅ Working | `/datasets`, `/datasets/{name}` |
| Player Stats | ✅ Working | `/players/stats` |
| Team Analysis | ⚠️ Partial | Some endpoints operational |
| Advanced Analytics | 🚧 Future | Complex queries & predictions |

**Architecture Features**:
- **OpenAPI 3.0.3** specification with interactive docs
- **Pydantic validation** for request/response models
- **Async support** for high-performance endpoints
- **Error handling** with proper HTTP status codes
- **CORS enabled** for web application integration

## Visualization Architecture

### Streamlit Dashboard (Production Ready)
**Multi-page Application**:
- **Overview**: Data metrics, pipeline status, system health
- **Team Analysis**: Conference/division analytics with interactive charts
- **Player Stats**: Fantasy tiers, position analysis, performance rankings
- **Schedule Analysis**: Game patterns, competitive balance metrics

**Technical Implementation**:
- **Direct DuckDB integration** (no PostgreSQL dependency)
- **Optimized staging views** with data limits for performance
- **Plotly visualizations** with filtering and real-time updates
- **Session state management** for user preferences

### Monitoring Stack
**Grafana + Prometheus**:
- System resource monitoring (CPU, memory, disk)
- Application performance metrics (query times, error rates)
- Data pipeline health (extraction success, model tests)
- Custom dashboards for NFL analytics trends

## Deployment Architecture

### Containerization (Docker)
```yaml
# Production Services
services:
  - nfl-app:        # Main application server
  - dagster:        # Pipeline orchestration
  - postgres:       # DuckLake catalog
  - grafana:        # Monitoring dashboards
  - prometheus:     # Metrics collection
```

### Infrastructure as Code (Ansible)
**Automated Deployment**:
- Server provisioning and configuration
- Application deployment with zero downtime
- SSL certificate management
- Security hardening and firewall rules
- Backup and disaster recovery setup

### Production Environment
**High Availability Setup**:
- Load balancing with health checks
- Database replication and failover
- Encrypted data at rest and in transit
- Automated backup to S3 with retention policies
- Comprehensive logging and alerting

## Data Quality & Testing

### Test Coverage (94% Success Rate)
| Component | Tests | Status |
|-----------|-------|--------|
| **Overall System** | 110/117 | ✅ 94% passing |
| **Intermediate Models** | 17/17 | ✅ 100% passing |
| **API Endpoints** | 75+ cases | ✅ Comprehensive |
| **CLI Operations** | Full coverage | ✅ Mocked & validated |

### Data Quality Framework
**dbt Tests** (17/17 passing):
- Range validation for rates and percentages (0-1)
- Foreign key relationships to reference data
- Not null constraints on key identifiers
- Business logic validation for NFL data

**Automated Validation**:
- Pre-commit hooks for code quality (Ruff)
- CI/CD pipeline with automated testing
- Data freshness monitoring
- Schema evolution detection

## Security Architecture

### Data Protection
- **Environment variables** in `.env` files (git-ignored)
- **Database credentials** encrypted and protected
- **API keys** excluded from repository
- **Infrastructure secrets** managed via Ansible vault

### Network Security
- SSL/TLS termination at load balancer
- Internal service communication encryption
- Rate limiting and request validation
- CORS policies for web applications

## Performance Optimization

### Query Performance
- **Table materialization** for intermediate models
- **Indexing strategy** on primary keys (player_id/team_id + season + week)
- **Partitioning** by season for large datasets
- **Compression** using Snappy for Parquet files

### Scalability Considerations
- **Incremental dbt models** for production workflows
- **Connection pooling** for database efficiency
- **Caching strategy** for frequently accessed data
- **Horizontal scaling** support for high load

## Development Workflow

### Local Development
```bash
# Environment setup
uv python install 3.11 && uv sync --dev

# Development cycle
uv run ruff format . && uv run ruff check .  # Code quality
uv run pytest --cov=src                      # Testing
uv run dbt run --select tag:intermediate     # Model development
uv run dagster dev                           # Pipeline testing
```

### Code Quality Standards
- **Python 3.11** with type hints and modern features
- **Ruff** for formatting and linting
- **Pre-commit hooks** for automated quality checks
- **pytest** with comprehensive coverage reporting
- **Documentation** requirements for all models and functions

## Future Architecture Enhancements

### Planned Improvements (Phase 6+)
- **Machine Learning Pipeline**: Model training and prediction serving
- **Real-time Processing**: Live game data ingestion with streaming
- **Advanced Web Interface**: React-based dashboard with complex analytics
- **Multi-cloud Deployment**: Kubernetes orchestration for cloud flexibility
- **GraphQL API**: Flexible query interface for complex data relationships

### Scalability Roadmap
- **Microservices Architecture**: Service decomposition for independent scaling
- **Event-driven Processing**: Kafka/Pulsar for real-time data streams
- **Distributed Computing**: Spark integration for large-scale analytics
- **Global CDN**: Edge caching for improved response times
- **Auto-scaling**: Dynamic resource allocation based on demand

---

**Architecture Status**: Production Ready ✅  
**Test Coverage**: 94% (110/117 tests passing)  
**Model Quality**: 17/17 intermediate model tests passing  
**API Functionality**: 9/15 endpoints operational  
**Deployment**: Fully automated with Ansible  

*Last Updated: July 23, 2025*