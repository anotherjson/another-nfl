# NFL Platform Operations Runbook

## Overview

This runbook provides step-by-step procedures for operating the NFL Platform in production environments. It covers daily operations, incident response, maintenance procedures, and troubleshooting guidelines.

## Daily Operations Checklist

### Morning Health Check (10 minutes)
```bash
# 1. Run comprehensive health check
./scripts/production-health-check.sh

# 2. Check service status
docker-compose ps

# 3. Review overnight alerts
# Check Grafana → Alerting → Alert History

# 4. Verify data freshness
uv run python -m src.cli extract status

# 5. Check disk space and resources
./scripts/maintenance.sh status
```

### Weekly Maintenance (30 minutes)
```bash
# 1. System cleanup
./scripts/maintenance.sh cleanup --dry-run
./scripts/maintenance.sh cleanup

# 2. Database optimization
./scripts/maintenance.sh vacuum

# 3. Update system packages
ansible-playbook -i inventories/production/hosts.yml \
  playbooks/security-update.yml \
  --vault-password-file .vault_pass

# 4. Backup verification
./scripts/restore-nfl-data.sh --list | head -5
```

### Monthly Operations (2 hours)
```bash
# 1. Full system optimization
./scripts/maintenance.sh optimize --force

# 2. Security audit
ansible-playbook -i inventories/production/hosts.yml \
  playbooks/security-audit.yml \
  --vault-password-file .vault_pass

# 3. Capacity planning review
# Analyze Grafana dashboards for resource trends

# 4. Disaster recovery test
# Perform controlled restore test in staging environment
```

## Incident Response Procedures

### Severity Classification

#### **Severity 1: Critical (15-minute response)**
- Complete system outage
- Data corruption or loss
- Security breach
- All services unavailable

#### **Severity 2: High (1-hour response)**
- Single service failure
- Performance degradation >50%
- Database connectivity issues
- Monitoring system failure

#### **Severity 3: Medium (4-hour response)**
- Individual dataset extraction failures
- Non-critical feature issues
- Warning alerts
- Documentation issues

### Incident Response Workflow

#### 1. Initial Response (First 5 minutes)
```bash
# Assess situation
./scripts/production-health-check.sh
docker-compose ps
./scripts/maintenance.sh status

# Check recent changes
git log --oneline -n 10

# Notify team (if Severity 1-2)
# Use Slack webhook or email notification
```

#### 2. Containment (Next 15 minutes)
```bash
# Stop affected services if needed
docker-compose stop <service-name>

# Switch to backup systems if available
# Redirect traffic to standby instances

# Preserve evidence
docker-compose logs <service-name> > incident-logs-$(date +%Y%m%d_%H%M%S).txt
```

#### 3. Investigation and Resolution
```bash
# Analyze logs
docker-compose logs <service-name> --tail 1000

# Check resource constraints
docker stats
df -h
free -h

# Review configuration changes
git diff HEAD~5 HEAD

# Apply fixes based on root cause analysis
```

#### 4. Recovery Verification
```bash
# Test all services
./scripts/production-health-check.sh

# Verify data integrity
uv run python scripts/test_phase3.py

# Monitor for 30 minutes after resolution
# Watch Grafana dashboards for stability
```

## Service-Specific Troubleshooting

### NFL Data Extractor Issues

#### Symptom: Extraction Failures
```bash
# Check extraction status
uv run python -m src.cli extract status --verbose

# Review recent extraction attempts
tail -100 /opt/nfl-logs/extractor/extractor.log

# Test single dataset extraction
uv run python -m src.cli extract dataset team_desc --verbose

# Check API connectivity
curl -I "https://github.com/cooperdff/nfl_data_py"
```

**Common Fixes:**
- Restart extractor service: `docker-compose restart nfl-extractor`
- Clear extraction state: `rm -f data/extraction_state.json`
- Retry with different dataset: `uv run python -m src.cli extract dataset schedules`

#### Symptom: Data Quality Issues
```bash
# Run data validation
uv run python -c "
from src.nfl_extractor import NFLDataExtractor
extractor = NFLDataExtractor()
data, meta = extractor.extract_dataset('team_desc', validate=True, save_to_disk=False)
print(f'Rows: {len(data)}, Columns: {len(data.columns)}')
"

# Check parquet file integrity
uv run python -m src.cli read data/team_desc/etl_date=*/data.parquet --info
```

### Database Issues

#### Symptom: PostgreSQL Connection Failures
```bash
# Check PostgreSQL status
docker exec -it nfl-postgres pg_isready

# Check connection count
docker exec -it nfl-postgres psql -U nfl_admin -d nfl_platform \
  -c "SELECT count(*) as connections FROM pg_stat_activity;"

# Check database disk space
docker exec -it nfl-postgres df -h /var/lib/postgresql/data

# Review PostgreSQL logs
docker-compose logs postgres --tail 200
```

**Common Fixes:**
- Restart PostgreSQL: `docker-compose restart postgres`
- Clear connection pool: `docker exec -it nfl-postgres psql -U nfl_admin -d nfl_platform -c "SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname='nfl_platform' AND state='idle';"`
- Increase connection limit: Update `postgresql.conf` and restart

#### Symptom: DuckDB Performance Issues
```bash
# Check DuckDB file size and integrity
ls -lah data/nfl_analytics*.duckdb

# Test DuckDB connection
docker exec -it nfl-dbt-runner duckdb data/nfl_analytics.duckdb \
  "SELECT count(*) FROM information_schema.tables;"

# Optimize DuckDB
docker exec -it nfl-dbt-runner duckdb data/nfl_analytics.duckdb \
  "VACUUM; ANALYZE;"
```

### Dagster Issues

#### Symptom: Web UI Not Accessible
```bash
# Check Dagster service status
docker-compose ps | grep dagster

# Check Dagster logs
docker-compose logs dagster-server --tail 100

# Verify port binding
netstat -tlnp | grep :3000

# Test GraphQL endpoint
curl -X POST http://localhost:3000/graphql \
  -H "Content-Type: application/json" \
  -d '{"query": "query { assetsOrError { ... on AssetConnection { nodes { key { path } } } } }"}'
```

**Common Fixes:**
- Restart Dagster service: `docker-compose restart dagster-server`
- Clear Dagster storage: `docker volume rm nfl-platform_dagster_storage`
- Update Dagster configuration: Edit `docker/dagster-server/dagster.yaml`

#### Symptom: Asset Materialization Failures
```bash
# Check recent runs
uv run dagster instance info

# Check specific asset logs
uv run dagster asset materialize --asset pbp_data

# Review asset dependencies
uv run dagster asset list
```

### dbt Issues

#### Symptom: Model Compilation Failures
```bash
# Test dbt compilation
cd dbt && uv run dbt compile

# Check dbt configuration
cd dbt && uv run dbt debug

# Run specific model
cd dbt && uv run dbt run --select stg_team_desc

# Check model dependencies
cd dbt && uv run dbt list --select stg_team_desc+
```

**Common Fixes:**
- Update dbt packages: `cd dbt && uv run dbt deps`
- Clear dbt cache: `cd dbt && rm -rf target/ dbt_packages/`
- Fix SQL syntax: Review error messages and update model files

## Performance Troubleshooting

### High Resource Usage

#### CPU Issues
```bash
# Identify high CPU processes
docker stats --no-stream | sort -k3 -hr

# Check system load
uptime
top -bn1 | head -20

# Analyze container resource limits
docker inspect <container-name> | grep -A 10 "Resources"
```

**Solutions:**
- Scale horizontally: Add more application servers
- Optimize queries: Review slow-running dbt models
- Increase resource limits: Update `docker-compose.prod.yml`

#### Memory Issues
```bash
# Check memory usage by container
docker stats --no-stream --format "table {{.Name}}\t{{.MemUsage}}\t{{.MemPerc}}"

# Check system memory
free -h
cat /proc/meminfo | head -20

# Check for memory leaks
docker exec -it <container> ps aux --sort=-%mem | head -10
```

**Solutions:**
- Restart high-memory containers: `docker-compose restart <service>`
- Increase memory limits: Update container resource limits
- Optimize data processing: Review large dataset handling

#### Disk Space Issues
```bash
# Check disk usage by directory
du -sh /opt/nfl-data/*
du -sh /opt/nfl-logs/*
du -sh /var/lib/docker/*

# Check Docker disk usage
docker system df

# Find large log files
find /opt/nfl-logs -name "*.log" -size +100M -exec ls -lh {} \;
```

**Solutions:**
- Run cleanup: `./scripts/maintenance.sh cleanup`
- Expand storage: Add additional disk space
- Archive old data: Move to S3 cold storage

## Backup and Recovery Procedures

### Emergency Recovery Scenarios

#### Complete System Failure
```bash
# 1. Assess damage and preserve evidence
./scripts/maintenance.sh status > system-failure-$(date +%Y%m%d_%H%M%S).log

# 2. Stop all services
docker-compose down

# 3. List available backups
./scripts/restore-nfl-data.sh --list

# 4. Restore from most recent backup
./scripts/restore-nfl-data.sh --from-s3 nfl-platform-backup-YYYYMMDD_HHMMSS

# 5. Start services and verify
docker-compose up -d
./scripts/production-health-check.sh
```

#### Data Corruption Recovery
```bash
# 1. Stop affected services
docker-compose stop nfl-extractor dbt-runner

# 2. Identify corrupted data
uv run python -c "
import pandas as pd
try:
    df = pd.read_parquet('data/corrupted-file.parquet')
    print('File is readable')
except Exception as e:
    print(f'Corruption detected: {e}')
"

# 3. Restore specific data only
./scripts/restore-nfl-data.sh --data-only backup-file.tar.gz

# 4. Verify data integrity
uv run python scripts/test_phase3.py

# 5. Resume services
docker-compose start nfl-extractor dbt-runner
```

#### Database Recovery
```bash
# 1. Stop database-dependent services
docker-compose stop dagster-server dbt-runner

# 2. Backup current database (if possible)
docker exec -it nfl-postgres pg_dump -U nfl_admin nfl_platform > emergency-backup.sql

# 3. Restore database from backup
./scripts/restore-nfl-data.sh --database-only backup-file.tar.gz

# 4. Verify database integrity
docker exec -it nfl-postgres psql -U nfl_admin -d nfl_platform \
  -c "SELECT schemaname, tablename, n_tup_ins FROM pg_stat_user_tables;"

# 5. Restart services
docker-compose start dagster-server dbt-runner
```

## Security Incident Response

### Suspected Security Breach
```bash
# 1. Immediate containment
# Isolate affected systems
sudo ufw deny from <suspicious-ip>

# Stop potentially compromised services
docker-compose stop

# 2. Evidence collection
# Preserve system state
sudo cp -r /var/log /tmp/incident-logs-$(date +%Y%m%d_%H%M%S)
sudo cp -r /opt/nfl-logs /tmp/incident-logs-$(date +%Y%m%d_%H%M%S)

# Check for unauthorized access
sudo last -n 50
sudo grep "Failed password" /var/log/auth.log | tail -20

# 3. Analysis and remediation
# Check for malicious processes
sudo ps aux | grep -v "\[" | sort -k3 -nr | head -20

# Review file modifications
sudo find /opt/nfl-platform -mtime -1 -type f -exec ls -la {} \;

# 4. Recovery
# Change all passwords and SSH keys
# Update Ansible vault with new credentials
# Redeploy with security patches
```

### Failed Login Attempts
```bash
# Check fail2ban status
sudo fail2ban-client status sshd

# Review authentication logs
sudo grep "authentication failure" /var/log/auth.log | tail -20

# Check current SSH connections
sudo netstat -tnpa | grep :22 | grep ESTABLISHED

# Update security rules if needed
sudo ufw status numbered
```

## Monitoring and Alerting

### Alert Response Procedures

#### High Resource Usage Alert
1. **Immediate**: Check current resource usage with `./scripts/maintenance.sh status`
2. **Analyze**: Identify root cause (high traffic, memory leak, inefficient queries)
3. **Mitigate**: Scale resources or optimize performance
4. **Monitor**: Watch for 30 minutes to ensure stability

#### Service Down Alert
1. **Immediate**: Attempt service restart with `docker-compose restart <service>`
2. **Investigate**: Check logs with `docker-compose logs <service> --tail 200`
3. **Escalate**: If restart fails, follow incident response procedure
4. **Document**: Record cause and resolution in incident log

#### Data Quality Alert
1. **Assess**: Run data validation checks
2. **Identify**: Determine if issue is with source data or processing
3. **Communicate**: Notify data consumers if data is compromised
4. **Resolve**: Re-extract/reprocess data as needed

### Custom Monitoring Queries

#### Prometheus Queries for NFL Platform
```promql
# Service availability
up{job="nfl-platform"}

# Container resource usage
container_memory_usage_bytes{name=~"nfl-.*"} / container_spec_memory_limit_bytes{name=~"nfl-.*"}

# Database connections
pg_stat_activity_count

# NFL data freshness (custom metric)
time() - nfl_last_successful_extraction_timestamp
```

#### Health Check Automation
```bash
# Create monitoring script
cat > /opt/nfl-platform/scripts/monitoring-check.sh << 'EOF'
#!/bin/bash
HEALTH_RESULT=$(./scripts/production-health-check.sh)
if [ $? -ne 0 ]; then
  # Send alert to monitoring system
  curl -X POST "${WEBHOOK_URL}" -d "{\"text\":\"NFL Platform health check failed: ${HEALTH_RESULT}\"}"
fi
EOF

# Add to crontab for automated monitoring
echo "*/5 * * * * /opt/nfl-platform/scripts/monitoring-check.sh" | crontab -
```

## Escalation Procedures

### Contact Information
- **Level 1 Support**: Development Team (Response: 15 minutes)
- **Level 2 Support**: DevOps Team (Response: 1 hour)
- **Level 3 Support**: Architecture Team (Response: 4 hours)

### Escalation Triggers
- **Immediate Escalation**: Severity 1 incidents, security breaches, data loss
- **Scheduled Escalation**: Unresolved issues after 2 hours (Sev 2) or 8 hours (Sev 3)
- **Management Notification**: Customer-impacting outages, SLA violations

### Communication Templates

#### Incident Notification
```
SUBJECT: [SEV{1-3}] NFL Platform - {Brief Description}

INCIDENT DETAILS:
- Time: {UTC timestamp}
- Services Affected: {list}
- Impact: {description}
- Root Cause: {if known}
- ETA for Resolution: {if known}
- Next Update: {timestamp}
```

#### Resolution Notification
```
SUBJECT: [RESOLVED] NFL Platform - {Brief Description}

RESOLUTION SUMMARY:
- Incident Duration: {time}
- Root Cause: {description}
- Resolution: {actions taken}
- Prevention: {preventive measures}
- Post-Incident Review: {if scheduled}
```

This runbook provides comprehensive procedures for operating the NFL Platform in production. Regular updates should be made as the system evolves and new operational patterns emerge.