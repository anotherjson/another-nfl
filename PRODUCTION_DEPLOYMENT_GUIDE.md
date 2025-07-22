# NFL Platform Production Deployment Guide

This guide provides comprehensive instructions for deploying the NFL Platform to production using Docker and Ansible.

## 🏗️ Architecture Overview

The production deployment consists of:

- **Application Servers** (3x): NFL extractor, dbt runner, Dagster orchestration
- **Database Servers** (2x): PostgreSQL primary + replica for DuckLake catalog
- **Load Balancer** (1x): Nginx with SSL termination
- **Monitoring** (1x): Prometheus + Grafana + Alerting

## 📋 Prerequisites

### Infrastructure Requirements

- **Minimum 6 servers** (or VMs):
  - 3x Application servers: 4 vCPU, 8GB RAM, 100GB storage
  - 2x Database servers: 2 vCPU, 4GB RAM, 200GB storage
  - 1x Load balancer: 2 vCPU, 2GB RAM, 50GB storage
  
- **Network Requirements**:
  - All servers in same VPC/network
  - SSH access from deployment machine
  - Internet access for package downloads

### Software Requirements

- **Control Machine**:
  - Ansible >= 2.9
  - Docker >= 20.10
  - AWS CLI (for S3 backups)
  - Git

- **Target Servers**:
  - Ubuntu 20.04+ or CentOS 8+
  - SSH key-based authentication
  - Sudo access for deployment user

## 🔐 Security Setup

### 1. Generate SSH Keys

```bash
# Generate deployment key
ssh-keygen -t ed25519 -C "nfl-platform-deployment" -f ~/.ssh/nfl_deploy_key

# Copy public key to all servers
for server in nfl-app-01 nfl-app-02 nfl-app-03 nfl-db-01 nfl-db-02 nfl-lb-01; do
  ssh-copy-id -i ~/.ssh/nfl_deploy_key.pub user@$server
done
```

### 2. Create Ansible Vault

```bash
cd ansible

# Create vault password file
echo "your-secure-vault-password" > .vault_pass
chmod 600 .vault_pass

# Create encrypted variables
ansible-vault create inventories/production/group_vars/vault.yml
```

Add to `vault.yml`:
```yaml
---
# Server connection details
vault_nfl_app_01_ip: "10.0.1.10"
vault_nfl_app_02_ip: "10.0.1.11"
vault_nfl_app_03_ip: "10.0.1.12"
vault_nfl_db_01_ip: "10.0.1.20"
vault_nfl_db_02_ip: "10.0.1.21"
vault_nfl_lb_01_ip: "10.0.1.30"
vault_nfl_monitor_01_ip: "10.0.1.40"

# SSH configuration
vault_ansible_user: "nfl-deploy"
vault_ssh_key_path: "~/.ssh/nfl_deploy_key"

# Database credentials
vault_postgres_db: "nfl_platform"
vault_postgres_user: "nfl_admin"
vault_postgres_password: "secure-db-password-change-this"

# Application secrets
vault_app_repo_url: "https://github.com/your-org/another-nfl.git"
vault_app_branch: "main"
vault_nfl_platform_version: "latest"

# Monitoring
vault_grafana_password: "secure-grafana-password"

# SSL/Domain
vault_domain_name: "nfl-platform.your-domain.com"

# Backup encryption
vault_backup_encryption_key: "your-backup-encryption-key"

# Notifications
vault_slack_webhook_url: "https://hooks.slack.com/services/YOUR/SLACK/WEBHOOK"
```

## 🚀 Deployment Steps

### 1. Prepare Infrastructure

```bash
cd ansible

# Test connectivity to all servers
ansible all -i inventories/production/hosts.yml -m ping --vault-password-file .vault_pass

# Run infrastructure setup
ansible-playbook -i inventories/production/hosts.yml \
  playbooks/site.yml \
  --vault-password-file .vault_pass \
  --tags common,security,docker-setup
```

### 2. Deploy Database Layer

```bash
# Deploy PostgreSQL databases
ansible-playbook -i inventories/production/hosts.yml \
  playbooks/site.yml \
  --vault-password-file .vault_pass \
  --tags database \
  --limit nfl_db_servers
```

### 3. Deploy Application Layer

```bash
# Build and deploy application containers
ansible-playbook -i inventories/production/hosts.yml \
  playbooks/deploy-nfl-platform.yml \
  --vault-password-file .vault_pass \
  --extra-vars "nfl_platform_version=v1.0.0"
```

### 4. Configure Load Balancer

```bash
# Deploy Nginx load balancer with SSL
ansible-playbook -i inventories/production/hosts.yml \
  playbooks/site.yml \
  --vault-password-file .vault_pass \
  --tags loadbalancer \
  --limit nfl_load_balancers
```

### 5. Setup Monitoring

```bash
# Deploy monitoring stack
ansible-playbook -i inventories/production/hosts.yml \
  playbooks/site.yml \
  --vault-password-file .vault_pass \
  --tags monitoring \
  --limit nfl_monitoring
```

### 6. SSL Certificate Setup

```bash
# Install SSL certificates (Let's Encrypt or custom)
ansible-playbook -i inventories/production/hosts.yml \
  playbooks/ssl-setup.yml \
  --vault-password-file .vault_pass
```

## 🔍 Verification

### Health Checks

```bash
# Run production health checks
./scripts/production-health-check.sh

# Check all services are running
curl -f https://your-domain.com/health
curl -f https://your-domain.com/dagster/
curl -f https://your-domain.com/grafana/
```

### Access URLs

- **Dagster UI**: `https://your-domain.com/dagster/`
- **Grafana Dashboard**: `https://your-domain.com/grafana/`
- **Prometheus**: `http://monitoring-server:9090/`

## 📊 Monitoring Setup

### Grafana Configuration

1. Login to Grafana: `https://your-domain.com/grafana/`
2. Username: `admin`, Password: `<vault_grafana_password>`
3. Import dashboards from `monitoring/grafana/dashboards/`
4. Configure alert notifications

### Prometheus Alerts

Alerts are automatically configured for:
- Service downtime
- High resource usage
- NFL data pipeline failures
- Database issues
- SSL certificate expiration

## 🔄 CI/CD Integration

### GitHub Actions Setup

1. Add repository secrets:
   ```
   PRODUCTION_SSH_KEY=<private_key_content>
   PRODUCTION_HOSTS=<server_ips>
   ANSIBLE_VAULT_PASSWORD=<vault_password>
   SLACK_WEBHOOK_URL=<slack_webhook>
   GRAFANA_API_TOKEN=<grafana_token>
   ```

2. Push to main branch triggers deployment
3. Manual deployment via GitHub Actions UI

### Deployment Process

1. **Automated Testing**: Lint, test, compile
2. **Container Build**: Multi-arch Docker images
3. **Staging Deploy**: Test on staging environment
4. **Production Deploy**: Blue-green deployment
5. **Health Checks**: Comprehensive validation
6. **Notifications**: Slack/email notifications

## 💾 Backup & Recovery

### Automated Backups

```bash
# Setup automated backups (runs daily at 2 AM)
crontab -e

# Add this line:
0 2 * * * /opt/nfl-platform/scripts/backup-nfl-data.sh
```

### Backup Components

- **NFL Data**: All parquet files and extraction state
- **PostgreSQL**: Full database dumps with catalog
- **Configurations**: Docker, Ansible, and app configs
- **Logs**: Recent logs (last 7 days)

### Backup Storage

- **Local**: `/opt/nfl-backups/` (encrypted)
- **S3**: `s3://nfl-platform-backups-prod/` (versioned)
- **Retention**: 30 days local, 90 days S3

### Restore Process

```bash
# List available backups
./scripts/restore-nfl-data.sh --list

# Restore from local backup
./scripts/restore-nfl-data.sh nfl-platform-backup-20241220_120000

# Restore from S3
./scripts/restore-nfl-data.sh --from-s3 nfl-platform-backup-20241220_120000

# Data-only restore
./scripts/restore-nfl-data.sh --data-only backup-file.tar.gz
```

## 🔧 Maintenance

### Regular Maintenance Tasks

```bash
# Run maintenance script
./scripts/maintenance.sh health --verbose
./scripts/maintenance.sh cleanup
./scripts/maintenance.sh optimize
```

### Scheduled Maintenance

- **Daily**: Log rotation, temp file cleanup
- **Weekly**: Database vacuum, Docker cleanup
- **Monthly**: Security updates, certificate renewal

### Update Process

```bash
# Update containers
./scripts/maintenance.sh update --force

# Or via Ansible
ansible-playbook -i inventories/production/hosts.yml \
  playbooks/deploy-nfl-platform.yml \
  --vault-password-file .vault_pass \
  --extra-vars "nfl_platform_version=v1.1.0"
```

## 🚨 Troubleshooting

### Common Issues

1. **Service Won't Start**
   ```bash
   docker-compose logs <service-name>
   ./scripts/maintenance.sh health --verbose
   ```

2. **Database Connection Issues**
   ```bash
   # Check database status
   ansible nfl_db_servers -i inventories/production/hosts.yml \
     -m shell -a "pg_isready -h localhost -p 5432"
   ```

3. **High Resource Usage**
   ```bash
   # System analysis
   ./scripts/maintenance.sh status
   ./scripts/maintenance.sh disk
   ```

4. **Data Pipeline Failures**
   ```bash
   # Check extraction logs
   docker logs nfl-extractor
   
   # Check dbt logs
   docker logs nfl-dbt-runner
   
   # Check Dagster
   docker logs nfl-dagster-server
   ```

### Recovery Procedures

1. **Service Recovery**: Automatic restart via Docker
2. **Database Recovery**: PostgreSQL replica promotion
3. **Data Recovery**: Restore from most recent backup
4. **Full System Recovery**: Rebuild from Infrastructure as Code

## 🔒 Security Considerations

### Network Security
- UFW firewall configured on all servers
- SSH key-based authentication only
- VPC isolation with security groups
- SSL/TLS encryption for all external traffic

### Application Security
- Container security scanning
- Regular security updates
- Secrets management via Ansible Vault
- Database encryption at rest

### Monitoring Security
- Failed login attempt detection
- File integrity monitoring (AIDE)
- Log analysis and alerting
- Security patch notifications

## 📈 Scaling Considerations

### Horizontal Scaling
- Add more application servers via Ansible
- Database read replicas for query scaling
- Load balancer configuration updates

### Vertical Scaling
- Increase container resource limits
- Database server upgrades
- Storage capacity expansion

### Performance Optimization
- DuckDB query optimization
- Data partitioning strategies
- Cache layer implementation
- CDN for static assets

---

## 🏁 Deployment Checklist

- [ ] Infrastructure servers provisioned
- [ ] SSH keys distributed
- [ ] Ansible vault configured
- [ ] DNS records configured
- [ ] SSL certificates obtained
- [ ] Database deployed and configured
- [ ] Application containers deployed
- [ ] Load balancer configured
- [ ] Monitoring stack deployed
- [ ] Backup system configured
- [ ] Health checks passing
- [ ] CI/CD pipeline configured
- [ ] Documentation updated
- [ ] Team trained on operations

**🎉 Your NFL Platform is now production-ready!**