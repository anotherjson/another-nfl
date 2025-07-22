# NFL Platform Architecture Documentation

## Overview

The NFL Platform is a comprehensive, production-ready data platform designed for extracting, transforming, and analyzing NFL data at enterprise scale. The architecture follows modern data engineering practices with containerization, infrastructure as code, monitoring, and operational excellence.

## System Architecture

### High-Level Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                        Internet                                  │
└─────────────────────────┬───────────────────────────────────────┘
                          │
┌─────────────────────────▼───────────────────────────────────────┐
│                 Load Balancer Layer                             │
│  ┌─────────────────────────────────────────────────────────────┐│
│  │              Nginx Load Balancer                            ││
│  │         ┌─────────────┐  ┌─────────────┐                   ││
│  │         │ SSL/TLS     │  │   Rate      │                   ││
│  │         │ Termination │  │  Limiting   │                   ││
│  │         └─────────────┘  └─────────────┘                   ││
│  └─────────────────────────────────────────────────────────────┘│
└─────────────────────────┬───────────────────────────────────────┘
                          │
┌─────────────────────────▼───────────────────────────────────────┐
│                  Application Layer                              │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐              │
│  │App Server 1 │  │App Server 2 │  │App Server 3 │              │
│  │             │  │             │  │             │              │
│  │┌───────────┐│  │┌───────────┐│  │┌───────────┐│              │
│  ││ Extractor ││  ││dbt Runner ││  ││ Dagster   ││              │
│  ││           ││  ││           ││  ││ Server    ││              │
│  │└───────────┘│  │└───────────┘│  │└───────────┘│              │
│  │┌───────────┐│  │┌───────────┐│  │┌───────────┐│              │
│  ││dbt Runner ││  ││ Extractor ││  ││ Extractor ││              │
│  │└───────────┘│  │└───────────┘│  │└───────────┘│              │
│  │┌───────────┐│  │┌───────────┐│  │┌───────────┐│              │
│  ││ Dagster   ││  ││ Dagster   ││  ││dbt Runner ││              │
│  │└───────────┘│  │└───────────┘│  │└───────────┘│              │
│  └─────────────┘  └─────────────┘  └─────────────┘              │
└─────────────────────────┬───────────────────────────────────────┘
                          │
┌─────────────────────────▼───────────────────────────────────────┐
│                   Database Layer                                │
│  ┌─────────────────────┐        ┌─────────────────────┐          │
│  │ PostgreSQL Primary  │◄──────►│PostgreSQL Replica  │          │
│  │                     │        │                     │          │
│  │ ┌─────────────────┐ │        │ ┌─────────────────┐ │          │
│  │ │ NFL Catalog     │ │        │ │ NFL Catalog     │ │          │
│  │ │ DuckLake Tables │ │        │ │ DuckLake Tables │ │          │
│  │ │ Metadata        │ │        │ │ Metadata        │ │          │
│  │ └─────────────────┘ │        │ └─────────────────┘ │          │
│  └─────────────────────┘        └─────────────────────┘          │
└─────────────────────────┬───────────────────────────────────────┘
                          │
┌─────────────────────────▼───────────────────────────────────────┐
│                   Storage Layer                                 │
│  ┌─────────────────────┐        ┌─────────────────────┐          │
│  │    DuckDB Files     │        │   NFL Parquet Data  │          │
│  │                     │        │                     │          │
│  │ ┌─────────────────┐ │        │ ┌─────────────────┐ │          │
│  │ │ Analytics DB    │ │        │ │ Raw Data        │ │          │
│  │ │ Transformed     │ │        │ │ Partitioned     │ │          │
│  │ │ Data Models     │ │        │ │ by Year/ETL     │ │          │
│  │ └─────────────────┘ │        │ └─────────────────┘ │          │
│  └─────────────────────┘        └─────────────────────┘          │
└─────────────────────────┬───────────────────────────────────────┘
                          │
┌─────────────────────────▼───────────────────────────────────────┐
│                   Backup Layer                                  │
│  ┌─────────────────────────────────────────────────────────────┐│
│  │                  S3 Backup Storage                          ││
│  │  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐         ││
│  │  │   Daily     │  │  Database   │  │    Config   │         ││
│  │  │  Backups    │  │   Dumps     │  │   Backups   │         ││
│  │  │ (Encrypted) │  │ (Encrypted) │  │ (Encrypted) │         ││
│  │  └─────────────┘  └─────────────┘  └─────────────┘         ││
│  └─────────────────────────────────────────────────────────────┘│
└─────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────┐
│                  Monitoring Layer                               │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐              │
│  │ Prometheus  │  │  Grafana    │  │ Alertmanager│              │
│  │             │  │             │  │             │              │
│  │ ┌─────────┐ │  │ ┌─────────┐ │  │ ┌─────────┐ │              │
│  │ │ Metrics │ │  │ │Dashboard│ │  │ │ Alerts  │ │              │
│  │ │Collection│ │  │ │         │ │  │ │ Rules   │ │              │
│  │ └─────────┘ │  │ └─────────┘ │  │ └─────────┘ │              │
│  └─────────────┘  └─────────────┘  └─────────────┘              │
└─────────────────────────────────────────────────────────────────┘
```

## Component Architecture

### Application Services

#### 1. NFL Data Extractor
**Purpose**: Extract NFL data from the `nfl_data_py` API and store as partitioned Parquet files.

```
┌─────────────────────────────────────────┐
│            NFL Extractor                │
│                                         │
│  ┌─────────────────────────────────────┐│
│  │        CLI Interface                ││
│  │  ┌─────────────────────────────────┐││
│  │  │ • explore datasets              │││
│  │  │ • explore data <dataset>        │││
│  │  │ • extract dataset <name>        │││
│  │  │ • extract multiple <datasets>   │││
│  │  │ • extract incremental           │││
│  │  └─────────────────────────────────┘││
│  └─────────────────────────────────────┘│
│  ┌─────────────────────────────────────┐│
│  │     NFLDataExtractor Engine         ││
│  │  ┌─────────────────────────────────┐││
│  │  │ • Retry logic                   │││
│  │  │ • Data validation               │││
│  │  │ • Error handling                │││
│  │  │ • Progress tracking             │││
│  │  └─────────────────────────────────┘││
│  └─────────────────────────────────────┘│
│  ┌─────────────────────────────────────┐│
│  │    Extraction Manager               ││
│  │  ┌─────────────────────────────────┐││
│  │  │ • Incremental processing        │││
│  │  │ • State management              │││
│  │  │ • Age-based refresh             │││
│  │  │ • Cleanup management            │││
│  │  └─────────────────────────────────┘││
│  └─────────────────────────────────────┘│
└─────────────────────────────────────────┘
```

**Technologies**: Python 3.11, Click, Rich, pandas, pyarrow, PyYAML
**Data Sources**: 19 NFL datasets via `nfl_data_py`
**Output**: Partitioned Parquet files in `/data/{dataset}/{year}/etl_date={date}/`

#### 2. dbt Data Transformer  
**Purpose**: Transform raw NFL data into analytics-ready models with data quality testing.

```
┌─────────────────────────────────────────┐
│             dbt Runner                  │
│                                         │
│  ┌─────────────────────────────────────┐│
│  │        Staging Models               ││
│  │  ┌─────────────────────────────────┐││
│  │  │ • stg_pbp                       │││
│  │  │ • stg_weekly                    │││
│  │  │ • stg_schedules                 │││
│  │  │ • stg_team_desc                 │││
│  │  │ • stg_team_desc_ducklake        │││
│  │  └─────────────────────────────────┘││
│  └─────────────────────────────────────┘│
│  ┌─────────────────────────────────────┐│
│  │      Intermediate Models            ││
│  │  ┌─────────────────────────────────┐││
│  │  │ • int_player_weekly_stats       │││
│  │  │ • int_team_performance          │││
│  │  └─────────────────────────────────┘││
│  └─────────────────────────────────────┘│
│  ┌─────────────────────────────────────┐│
│  │         Mart Models                 ││
│  │  ┌─────────────────────────────────┐││
│  │  │ • mart_game_results             │││
│  │  │ • mart_player_season_stats      │││
│  │  │ • mart_weekly_team_stats        │││
│  │  └─────────────────────────────────┘││
│  └─────────────────────────────────────┘│
└─────────────────────────────────────────┘
```

**Technologies**: dbt Core, DuckDB adapter, Jinja2
**Input**: Raw Parquet files and DuckLake catalog
**Output**: Transformed data models and data quality tests

#### 3. Dagster Orchestrator
**Purpose**: Orchestrate the entire data pipeline with scheduling, monitoring, and dependency management.

```
┌─────────────────────────────────────────┐
│          Dagster Server                 │
│                                         │
│  ┌─────────────────────────────────────┐│
│  │           Assets                    ││
│  │  ┌─────────────────────────────────┐││
│  │  │ • Raw data assets               │││
│  │  │ • dbt model assets              │││
│  │  │ • DuckLake catalog assets       │││
│  │  └─────────────────────────────────┘││
│  └─────────────────────────────────────┘│
│  ┌─────────────────────────────────────┐│
│  │         Resources                   ││
│  │  ┌─────────────────────────────────┐││
│  │  │ • DuckDB connection             │││
│  │  │ • dbt project                   │││
│  │  │ • DuckLake integration          │││
│  │  └─────────────────────────────────┘││
│  └─────────────────────────────────────┘│
│  ┌─────────────────────────────────────┐│
│  │        Schedules                    ││
│  │  ┌─────────────────────────────────┐││
│  │  │ • Weekly data extraction        │││
│  │  │ • Daily dbt transformation      │││
│  │  │ • Monthly data cleanup          │││
│  │  └─────────────────────────────────┘││
│  └─────────────────────────────────────┘│
└─────────────────────────────────────────┘
```

**Technologies**: Dagster, dagster-dbt, PostgreSQL backend
**Features**: Asset dependency management, scheduling, monitoring, web UI

### Database Architecture

#### PostgreSQL Catalog Database
**Purpose**: Centralized metadata catalog for DuckLake lakehouse functionality.

```
┌─────────────────────────────────────────────────────────────────┐
│                    PostgreSQL Database                          │
│                                                                 │
│  ┌─────────────────────┐    ┌─────────────────────────────────┐ │
│  │   nfl_platform      │    │        nfl_catalog              │ │
│  │                     │    │                                 │ │
│  │ ┌─────────────────┐ │    │ ┌──────────────────────────────┐│ │
│  │ │ Dagster         │ │    │ │      ducklake_catalog        ││ │
│  │ │ - runs          │ │    │ │ - tables                     ││ │
│  │ │ - assets        │ │    │ │ - snapshots                  ││ │
│  │ │ - schedules     │ │    │ │ - schema_versions            ││ │
│  │ │ - event_logs    │ │    │ └──────────────────────────────┘│ │
│  │ └─────────────────┘ │    │ ┌──────────────────────────────┐│ │
│  └─────────────────────┘    │ │      nfl_metadata            ││ │
│                             │ │ - extraction_log             ││ │
│                             │ │ - data_quality_checks        ││ │
│                             │ └──────────────────────────────┘│ │
│                             └─────────────────────────────────┘ │
└─────────────────────────────────────────────────────────────────┘
```

**Features**:
- Dagster metadata storage
- DuckLake table catalog with versioning
- Extraction history and data quality tracking
- High availability with primary/replica setup

#### DuckDB Analytics Engine
**Purpose**: High-performance analytical processing with DuckLake lakehouse capabilities.

```
┌─────────────────────────────────────────┐
│            DuckDB Engine                │
│                                         │
│  ┌─────────────────────────────────────┐│
│  │      Extensions Loaded              ││
│  │  ┌─────────────────────────────────┐││
│  │  │ • httpfs (S3 access)            │││
│  │  │ • parquet (file format)         │││
│  │  │ • postgres_scanner              │││
│  │  └─────────────────────────────────┘││
│  └─────────────────────────────────────┘│
│  ┌─────────────────────────────────────┐│
│  │       Memory Configuration          ││
│  │  ┌─────────────────────────────────┐││
│  │  │ • 8GB memory limit              │││
│  │  │ • 8 threads                     │││
│  │  │ • Optimized for analytics       │││
│  │  └─────────────────────────────────┘││
│  └─────────────────────────────────────┘│
│  ┌─────────────────────────────────────┐│
│  │      DuckLake Integration           ││
│  │  ┌─────────────────────────────────┐││
│  │  │ • Time travel queries           │││
│  │  │ • ACID transactions             │││
│  │  │ • Schema evolution              │││
│  │  └─────────────────────────────────┘││
│  └─────────────────────────────────────┘│
└─────────────────────────────────────────┘
```

## Deployment Architecture

### Container Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                    Docker Container Stack                       │
│                                                                 │
│  ┌─────────────────────────────────────────────────────────────┐│
│  │                  Network: nfl_network                       ││
│  │                    (172.20.0.0/16)                         ││
│  └─────────────────────────────────────────────────────────────┘│
│                                                                 │
│  ┌─────────────┐ ┌─────────────┐ ┌─────────────┐ ┌────────────┐ │
│  │   nginx     │ │nfl-extractor│ │ dbt-runner  │ │ dagster-   │ │
│  │             │ │             │ │             │ │  server    │ │
│  │ ┌─────────┐ │ │ ┌─────────┐ │ │ ┌─────────┐ │ │ ┌────────┐ │ │
│  │ │SSL/TLS  │ │ │ │Python   │ │ │ │dbt Core │ │ │ │Dagster │ │ │
│  │ │Rate     │ │ │ │CLI      │ │ │ │DuckDB   │ │ │ │Web UI  │ │ │
│  │ │Limiting │ │ │ │Rich     │ │ │ │         │ │ │ │Assets  │ │ │
│  │ └─────────┘ │ │ └─────────┘ │ │ └─────────┘ │ │ └────────┘ │ │
│  │   :80/:443  │ │    :8000    │ │    N/A      │ │   :3000    │ │
│  └─────────────┘ └─────────────┘ └─────────────┘ └────────────┘ │
│                                                                 │
│  ┌─────────────┐ ┌─────────────┐ ┌─────────────┐ ┌────────────┐ │
│  │ postgres    │ │ prometheus  │ │  grafana    │ │ postgres   │ │
│  │             │ │             │ │             │ │ (replica)  │ │
│  │ ┌─────────┐ │ │ ┌─────────┐ │ │ ┌─────────┐ │ │ ┌────────┐ │ │
│  │ │Primary  │ │ │ │Metrics  │ │ │ │Dashboard│ │ │ │Replica │ │ │
│  │ │DuckLake │ │ │ │Collection│ │ │ │Alerts   │ │ │ │        │ │ │
│  │ │Catalog  │ │ │ │Rules    │ │ │ │         │ │ │ │        │ │ │
│  │ └─────────┘ │ │ └─────────┘ │ │ └─────────┘ │ │ └────────┘ │ │
│  │    :5432    │ │    :9090    │ │    :3001    │ │   :5433    │ │
│  └─────────────┘ └─────────────┘ └─────────────┘ └────────────┘ │
└─────────────────────────────────────────────────────────────────┘
```

### Infrastructure as Code

#### Ansible Automation

```
┌─────────────────────────────────────────┐
│         Ansible Structure               │
│                                         │
│  ansible/                               │
│  ├── inventories/                       │
│  │   ├── production/                    │
│  │   │   ├── hosts.yml                  │
│  │   │   └── group_vars/                │
│  │   └── staging/                       │
│  │       ├── hosts.yml                  │
│  │       └── group_vars/                │
│  ├── playbooks/                         │
│  │   ├── site.yml                       │
│  │   ├── deploy-nfl-platform.yml        │
│  │   └── rollback-nfl-platform.yml      │
│  ├── roles/                             │
│  │   ├── common/                        │
│  │   ├── security/                      │
│  │   ├── docker-setup/                  │
│  │   ├── nfl-platform/                  │
│  │   ├── database/                      │
│  │   ├── monitoring/                    │
│  │   └── backup/                        │
│  └── ansible.cfg                        │
└─────────────────────────────────────────┘
```

**Key Features**:
- Multi-environment inventory management
- Role-based deployment with security hardening
- Zero-downtime rolling deployments
- Automated rollback capabilities
- Encrypted secrets management with Ansible Vault

## Data Flow Architecture

### Extract → Transform → Load Pipeline

```
┌─────────────────────────────────────────────────────────────────┐
│                        Data Pipeline Flow                       │
│                                                                 │
│  ┌─────────────────────────────────────────────────────────────┐│
│  │                    EXTRACT                                  ││
│  │                                                             ││
│  │  NFL API                                                    ││
│  │     ↓                                                       ││
│  │  nfl_data_py ──→ NFLDataExtractor ──→ Parquet Files        ││
│  │                      ↓                    ↓                 ││
│  │                 Validation &         Partitioned           ││
│  │                Error Handling       Year/ETL Date          ││
│  │                                                             ││
│  └─────────────────────────────────────────────────────────────┘│
│                              ↓                                  │
│  ┌─────────────────────────────────────────────────────────────┐│
│  │                   REGISTER                                  ││
│  │                                                             ││
│  │  DuckLake Catalog ←── Registration ←── Parquet Files       ││
│  │       ↓                    ↓                                ││
│  │  Time Travel          Versioning                            ││
│  │  Capabilities         & Snapshots                           ││
│  │                                                             ││
│  └─────────────────────────────────────────────────────────────┘│
│                              ↓                                  │
│  ┌─────────────────────────────────────────────────────────────┐│
│  │                   TRANSFORM                                 ││
│  │                                                             ││
│  │  dbt Models                                                 ││
│  │     ↓                                                       ││
│  │  Staging ──→ Intermediate ──→ Marts                        ││
│  │     ↓             ↓              ↓                          ││
│  │  Light        Business      Analytics                       ││
│  │  Cleaning     Logic         Ready                           ││
│  │                                                             ││
│  └─────────────────────────────────────────────────────────────┘│
│                              ↓                                  │
│  ┌─────────────────────────────────────────────────────────────┐│
│  │                  ORCHESTRATE                                ││
│  │                                                             ││
│  │  Dagster Pipeline                                           ││
│  │     ↓                                                       ││
│  │  Asset Dependency Management                                ││
│  │     ↓                                                       ││
│  │  Scheduling & Monitoring                                    ││
│  │                                                             ││
│  └─────────────────────────────────────────────────────────────┘│
└─────────────────────────────────────────────────────────────────┘
```

## Security Architecture

### Multi-Layer Security

```
┌─────────────────────────────────────────────────────────────────┐
│                      Security Layers                            │
│                                                                 │
│  ┌─────────────────────────────────────────────────────────────┐│
│  │                  Network Security                           ││
│  │  • UFW Firewall Rules                                      ││
│  │  • SSL/TLS Encryption (Let's Encrypt)                      ││
│  │  • Rate Limiting (Nginx)                                   ││
│  │  • VPC Isolation                                           ││
│  └─────────────────────────────────────────────────────────────┘│
│  ┌─────────────────────────────────────────────────────────────┐│
│  │                 Application Security                        ││
│  │  • Container Security Scanning                             ││
│  │  • Non-root Container Users                                ││
│  │  • Resource Limits                                         ││
│  │  • Health Check Endpoints                                  ││
│  └─────────────────────────────────────────────────────────────┘│
│  ┌─────────────────────────────────────────────────────────────┐│
│  │                   Data Security                             ││
│  │  • Encryption at Rest (Database)                           ││
│  │  • Encrypted Backups (S3/GPG)                              ││
│  │  • Database Access Controls                                ││
│  │  • Audit Logging                                           ││
│  └─────────────────────────────────────────────────────────────┘│
│  ┌─────────────────────────────────────────────────────────────┐│
│  │                 Access Security                             ││
│  │  • SSH Key Authentication                                  ││
│  │  • Ansible Vault (Secrets)                                 ││
│  │  • Multi-factor Authentication                             ││
│  │  • Role-based Access Control                               ││
│  └─────────────────────────────────────────────────────────────┘│
│  ┌─────────────────────────────────────────────────────────────┐│
│  │               Monitoring Security                           ││
│  │  • Failed Login Detection (Fail2Ban)                       ││
│  │  • File Integrity Monitoring (AIDE)                        ││
│  │  • Security Log Analysis                                   ││
│  │  • Real-time Alerting                                      ││
│  └─────────────────────────────────────────────────────────────┘│
└─────────────────────────────────────────────────────────────────┘
```

## Monitoring Architecture

### Comprehensive Observability Stack

```
┌─────────────────────────────────────────────────────────────────┐
│                    Monitoring Stack                             │
│                                                                 │
│  ┌─────────────────────────────────────────────────────────────┐│
│  │                   Metrics Layer                             ││
│  │                                                             ││
│  │  ┌─────────────┐    ┌─────────────┐    ┌─────────────┐     ││
│  │  │ Node        │    │ Docker      │    │ Application │     ││
│  │  │ Exporter    │    │ Metrics     │    │ Metrics     │     ││
│  │  │             │    │             │    │             │     ││
│  │  │ • CPU       │    │ • Container │    │ • NFL Data  │     ││
│  │  │ • Memory    │    │ • Images    │    │ • Pipeline  │     ││
│  │  │ • Disk      │    │ • Networks  │    │ • DB Conn   │     ││
│  │  │ • Network   │    │ • Volumes   │    │ • Errors    │     ││
│  │  └─────────────┘    └─────────────┘    └─────────────┘     ││
│  │           │                │                    │          ││
│  │           └────────────────┼────────────────────┘          ││
│  │                            ↓                               ││
│  │                    ┌─────────────┐                         ││
│  │                    │ Prometheus  │                         ││
│  │                    │             │                         ││
│  │                    │ • Scraping  │                         ││
│  │                    │ • Storage   │                         ││
│  │                    │ • Rules     │                         ││
│  │                    │ • Alerts    │                         ││
│  │                    └─────────────┘                         ││
│  └─────────────────────────────────────────────────────────────┘│
│                                 ↓                               │
│  ┌─────────────────────────────────────────────────────────────┐│
│  │                 Visualization Layer                         ││
│  │                                                             ││
│  │                    ┌─────────────┐                         ││
│  │                    │  Grafana    │                         ││
│  │                    │             │                         ││
│  │                    │ • System    │                         ││
│  │                    │   Overview  │                         ││
│  │                    │ • NFL Data  │                         ││
│  │                    │   Pipeline  │                         ││
│  │                    │ • Database  │                         ││
│  │                    │   Health    │                         ││
│  │                    │ • Alerts    │                         ││
│  │                    └─────────────┘                         ││
│  └─────────────────────────────────────────────────────────────┘│
│                                 ↓                               │
│  ┌─────────────────────────────────────────────────────────────┐│
│  │                  Alerting Layer                             ││
│  │                                                             ││
│  │  ┌─────────────────────────────────────────────────────────┐││
│  │  │                Alert Rules                              │││
│  │  │                                                         │││
│  │  │ • Service Down                                          │││
│  │  │ • High Resource Usage                                   │││
│  │  │ • NFL Data Pipeline Failures                            │││
│  │  │ • Database Connection Issues                            │││
│  │  │ • SSL Certificate Expiry                                │││
│  │  │ • Security Violations                                   │││
│  │  └─────────────────────────────────────────────────────────┘││
│  │                              ↓                             ││
│  │  ┌─────────────────────────────────────────────────────────┐││
│  │  │              Notification Channels                     │││
│  │  │                                                         │││
│  │  │ • Slack Integration                                     │││
│  │  │ • Email Notifications                                   │││
│  │  │ • PagerDuty Integration                                 │││
│  │  │ • Webhook Endpoints                                     │││
│  │  └─────────────────────────────────────────────────────────┘││
│  └─────────────────────────────────────────────────────────────┘│
└─────────────────────────────────────────────────────────────────┘
```

## Backup & Disaster Recovery

### Multi-Tier Backup Strategy

```
┌─────────────────────────────────────────────────────────────────┐
│                   Backup Architecture                           │
│                                                                 │
│  ┌─────────────────────────────────────────────────────────────┐│
│  │                  Source Data                                ││
│  │                                                             ││
│  │  ┌─────────────┐ ┌─────────────┐ ┌─────────────┐           ││
│  │  │ NFL Parquet │ │ PostgreSQL  │ │ Config      │           ││
│  │  │ Data Files  │ │ Databases   │ │ Files       │           ││
│  │  └─────────────┘ └─────────────┘ └─────────────┘           ││
│  └─────────────────────────────────────────────────────────────┘│
│                                 ↓                               │
│  ┌─────────────────────────────────────────────────────────────┐│
│  │                 Backup Process                              ││
│  │                                                             ││
│  │  Daily at 2 AM                                              ││
│  │  ┌─────────────────────────────────────────────────────────┐││
│  │  │ 1. Create compressed archives                           │││
│  │  │ 2. PostgreSQL pg_dump                                  │││
│  │  │ 3. Configuration snapshot                               │││
│  │  │ 4. Generate backup metadata                             │││
│  │  │ 5. GPG encryption                                       │││
│  │  │ 6. Upload to S3                                         │││
│  │  │ 7. Cleanup old backups                                  │││
│  │  └─────────────────────────────────────────────────────────┘││
│  └─────────────────────────────────────────────────────────────┘│
│                                 ↓                               │
│  ┌─────────────────────────────────────────────────────────────┐│
│  │                Storage Tiers                                ││
│  │                                                             ││
│  │  ┌─────────────┐ ┌─────────────┐ ┌─────────────┐           ││
│  │  │   Local     │ │     S3      │ │  S3 Glacier │           ││
│  │  │   Storage   │ │  Standard   │ │  Deep Arch  │           ││
│  │  │             │ │             │ │             │           ││
│  │  │ • 7 days    │ │ • 30 days   │ │ • 7 years   │           ││
│  │  │ • Fast      │ │ • Standard  │ │ • Archive   │           ││
│  │  │   Recovery  │ │   Recovery  │ │   Storage   │           ││
│  │  └─────────────┘ └─────────────┘ └─────────────┘           ││
│  └─────────────────────────────────────────────────────────────┘│
│                                 ↓                               │
│  ┌─────────────────────────────────────────────────────────────┐│
│  │               Recovery Process                               ││
│  │                                                             ││
│  │  ┌─────────────────────────────────────────────────────────┐││
│  │  │ • List available backups                                │││
│  │  │ • Download from S3 if needed                            │││
│  │  │ • Decrypt backup files                                  │││
│  │  │ • Extract and validate                                  │││
│  │  │ • Stop running services                                 │││
│  │  │ • Restore data/database/configs                         │││
│  │  │ • Start services and verify                             │││
│  │  └─────────────────────────────────────────────────────────┘││
│  └─────────────────────────────────────────────────────────────┘│
└─────────────────────────────────────────────────────────────────┘
```

**Recovery Objectives**:
- **RTO** (Recovery Time Objective): 4 hours
- **RPO** (Recovery Point Objective): 24 hours
- **Backup Verification**: Weekly automated restore tests
- **Geographic Distribution**: Multi-region S3 replication

## Operational Architecture

### DevOps & CI/CD Pipeline

```
┌─────────────────────────────────────────────────────────────────┐
│                     CI/CD Pipeline                              │
│                                                                 │
│  ┌─────────────────────────────────────────────────────────────┐│
│  │                  Source Control                             ││
│  │                                                             ││
│  │  Git Repository (GitHub)                                    ││
│  │  ├── Feature Branches                                       ││
│  │  ├── Pull Requests                                          ││
│  │  └── Main Branch                                            ││
│  └─────────────────────────────────────────────────────────────┘│
│                              ↓                                  │
│  ┌─────────────────────────────────────────────────────────────┐│
│  │               Continuous Integration                        ││
│  │                                                             ││
│  │  GitHub Actions                                             ││
│  │  ├── Code Quality (Ruff)                                    ││
│  │  ├── Testing (pytest)                                       ││
│  │  ├── Security Scanning                                      ││
│  │  ├── dbt Compilation                                        ││
│  │  └── Phase 3 Validation                                     ││
│  └─────────────────────────────────────────────────────────────┘│
│                              ↓                                  │
│  ┌─────────────────────────────────────────────────────────────┐│
│  │                Container Build                              ││
│  │                                                             ││
│  │  Multi-Architecture Builds                                  ││
│  │  ├── nfl-extractor                                          ││
│  │  ├── dbt-runner                                             ││
│  │  ├── dagster-server                                         ││
│  │  └── postgres-ducklake                                      ││
│  └─────────────────────────────────────────────────────────────┘│
│                              ↓                                  │
│  ┌─────────────────────────────────────────────────────────────┐│
│  │              Deployment Pipeline                            ││
│  │                                                             ││
│  │  ┌─────────────┐ ┌─────────────┐ ┌─────────────┐           ││
│  │  │  Staging    │ │ Production  │ │  Rollback   │           ││
│  │  │ Deployment  │ │ Deployment  │ │ Capability  │           ││
│  │  │             │ │             │ │             │           ││
│  │  │ • Automated │ │ • Manual    │ │ • Automatic │           ││
│  │  │ • Testing   │ │   Approval  │ │   Trigger   │           ││
│  │  │ • Health    │ │ • Zero      │ │ • Previous  │           ││
│  │  │   Checks    │ │   Downtime  │ │   Version   │           ││
│  │  └─────────────┘ └─────────────┘ └─────────────┘           ││
│  └─────────────────────────────────────────────────────────────┘│
└─────────────────────────────────────────────────────────────────┘
```

This comprehensive architecture documentation provides a complete view of the NFL Platform's production-ready infrastructure, designed for enterprise-scale data operations with high availability, security, and operational excellence.