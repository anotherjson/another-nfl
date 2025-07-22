#!/bin/bash
# NFL Platform Data Backup Script

set -e

# Configuration
BACKUP_BASE_DIR="${BACKUP_BASE_DIR:-/opt/nfl-backups}"
NFL_DATA_DIR="${NFL_DATA_DIR:-/opt/nfl-data}"
S3_BUCKET="${S3_BUCKET:-nfl-platform-backups-prod}"
ENCRYPTION_KEY_FILE="${ENCRYPTION_KEY_FILE:-/etc/nfl-platform/backup-key}"
RETENTION_DAYS="${RETENTION_DAYS:-30}"
TIMESTAMP=$(date +%Y%m%d_%H%M%S)
BACKUP_NAME="nfl-platform-backup-${TIMESTAMP}"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

log() {
    echo -e "${BLUE}[$(date +'%Y-%m-%d %H:%M:%S')]${NC} $1"
}

success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

# Create backup directories
create_backup_dirs() {
    log "Creating backup directory structure"
    
    mkdir -p "${BACKUP_BASE_DIR}/${BACKUP_NAME}"/{data,database,configs,logs}
    
    success "Backup directories created"
}

# Backup NFL data files
backup_data_files() {
    log "Starting data files backup"
    
    if [ -d "$NFL_DATA_DIR" ]; then
        # Use tar with compression and progress
        tar -czf "${BACKUP_BASE_DIR}/${BACKUP_NAME}/data/nfl-data.tar.gz" \
            -C "$NFL_DATA_DIR" \
            --exclude="*.tmp" \
            --exclude="logs/*" \
            . \
            || { error "Data backup failed"; return 1; }
        
        # Create manifest file
        find "$NFL_DATA_DIR" -type f -name "*.parquet" -exec stat -c "%n %s %Y" {} \; \
            > "${BACKUP_BASE_DIR}/${BACKUP_NAME}/data/parquet-manifest.txt"
        
        success "Data files backed up successfully"
    else
        warning "NFL data directory not found: $NFL_DATA_DIR"
    fi
}

# Backup PostgreSQL database
backup_database() {
    log "Starting PostgreSQL database backup"
    
    # Database connection details from environment
    PGHOST="${PGHOST:-postgres}"
    PGPORT="${PGPORT:-5432}"
    PGUSER="${PGUSER:-nfl_admin}"
    PGDATABASE="${PGDATABASE:-nfl_platform}"
    
    # Backup main database
    if command -v pg_dump >/dev/null 2>&1; then
        pg_dump \
            --host="$PGHOST" \
            --port="$PGPORT" \
            --username="$PGUSER" \
            --dbname="$PGDATABASE" \
            --format=custom \
            --compress=9 \
            --verbose \
            --file="${BACKUP_BASE_DIR}/${BACKUP_NAME}/database/nfl_platform.dump" \
            || { error "Database backup failed"; return 1; }
        
        # Backup catalog database
        pg_dump \
            --host="$PGHOST" \
            --port="$PGPORT" \
            --username="$PGUSER" \
            --dbname="nfl_catalog" \
            --format=custom \
            --compress=9 \
            --verbose \
            --file="${BACKUP_BASE_DIR}/${BACKUP_NAME}/database/nfl_catalog.dump" \
            || { error "Catalog database backup failed"; return 1; }
        
        # Backup global objects (users, roles)
        pg_dumpall \
            --host="$PGHOST" \
            --port="$PGPORT" \
            --username="$PGUSER" \
            --globals-only \
            --file="${BACKUP_BASE_DIR}/${BACKUP_NAME}/database/globals.sql" \
            || { error "Global objects backup failed"; return 1; }
        
        success "PostgreSQL databases backed up successfully"
    else
        warning "pg_dump not found, skipping database backup"
    fi
}

# Backup configuration files
backup_configs() {
    log "Starting configuration files backup"
    
    config_dirs=(
        "/opt/nfl-config"
        "/etc/nfl-platform"
        "/opt/nfl-platform/current/configs"
        "/opt/nfl-platform/current/docker"
        "/opt/nfl-platform/current/ansible"
    )
    
    for config_dir in "${config_dirs[@]}"; do
        if [ -d "$config_dir" ]; then
            base_name=$(basename "$config_dir")
            tar -czf "${BACKUP_BASE_DIR}/${BACKUP_NAME}/configs/${base_name}.tar.gz" \
                -C "$(dirname "$config_dir")" \
                "$base_name" \
                || warning "Failed to backup $config_dir"
        fi
    done
    
    success "Configuration files backed up"
}

# Backup important logs
backup_logs() {
    log "Starting logs backup (last 7 days)"
    
    log_dirs=(
        "/opt/nfl-logs"
        "/var/log/nfl-platform"
        "/opt/dagster/logs"
    )
    
    for log_dir in "${log_dirs[@]}"; do
        if [ -d "$log_dir" ]; then
            # Only backup recent logs (last 7 days)
            find "$log_dir" -type f -mtime -7 -name "*.log" -o -name "*.json" | \
            tar -czf "${BACKUP_BASE_DIR}/${BACKUP_NAME}/logs/$(basename "$log_dir")-logs.tar.gz" \
                -T - \
                || warning "Failed to backup logs from $log_dir"
        fi
    done
    
    success "Recent logs backed up"
}

# Create backup metadata
create_backup_metadata() {
    log "Creating backup metadata"
    
    cat > "${BACKUP_BASE_DIR}/${BACKUP_NAME}/backup-info.json" <<EOF
{
  "backup_name": "${BACKUP_NAME}",
  "timestamp": "${TIMESTAMP}",
  "date": "$(date -Iseconds)",
  "hostname": "$(hostname)",
  "version": "$(git -C /opt/nfl-platform/current rev-parse HEAD 2>/dev/null || echo 'unknown')",
  "backup_size_bytes": $(du -sb "${BACKUP_BASE_DIR}/${BACKUP_NAME}" | cut -f1),
  "components": {
    "data_files": $([ -f "${BACKUP_BASE_DIR}/${BACKUP_NAME}/data/nfl-data.tar.gz" ] && echo "true" || echo "false"),
    "database": $([ -f "${BACKUP_BASE_DIR}/${BACKUP_NAME}/database/nfl_platform.dump" ] && echo "true" || echo "false"),
    "configs": $([ -d "${BACKUP_BASE_DIR}/${BACKUP_NAME}/configs" ] && echo "true" || echo "false"),
    "logs": $([ -d "${BACKUP_BASE_DIR}/${BACKUP_NAME}/logs" ] && echo "true" || echo "false")
  }
}
EOF
    
    success "Backup metadata created"
}

# Encrypt backup
encrypt_backup() {
    if [ -f "$ENCRYPTION_KEY_FILE" ]; then
        log "Encrypting backup"
        
        tar -czf - -C "$BACKUP_BASE_DIR" "$BACKUP_NAME" | \
        gpg --symmetric \
            --cipher-algo AES256 \
            --passphrase-file "$ENCRYPTION_KEY_FILE" \
            --batch --yes \
            --output "${BACKUP_BASE_DIR}/${BACKUP_NAME}.tar.gz.gpg"
        
        if [ $? -eq 0 ]; then
            # Remove unencrypted backup
            rm -rf "${BACKUP_BASE_DIR}/${BACKUP_NAME}"
            success "Backup encrypted successfully"
            return 0
        else
            error "Backup encryption failed"
            return 1
        fi
    else
        log "No encryption key found, creating compressed archive"
        
        tar -czf "${BACKUP_BASE_DIR}/${BACKUP_NAME}.tar.gz" \
            -C "$BACKUP_BASE_DIR" \
            "$BACKUP_NAME"
        
        if [ $? -eq 0 ]; then
            rm -rf "${BACKUP_BASE_DIR}/${BACKUP_NAME}"
            success "Backup compressed successfully"
            return 0
        else
            error "Backup compression failed"
            return 1
        fi
    fi
}

# Upload to S3
upload_to_s3() {
    if command -v aws >/dev/null 2>&1; then
        log "Uploading backup to S3"
        
        backup_file="${BACKUP_BASE_DIR}/${BACKUP_NAME}.tar.gz"
        if [ -f "${backup_file}.gpg" ]; then
            backup_file="${backup_file}.gpg"
        fi
        
        aws s3 cp "$backup_file" \
            "s3://${S3_BUCKET}/$(date +%Y)/$(date +%m)/" \
            --storage-class STANDARD_IA \
            || { error "S3 upload failed"; return 1; }
        
        success "Backup uploaded to S3"
    else
        warning "AWS CLI not found, skipping S3 upload"
    fi
}

# Clean old backups
cleanup_old_backups() {
    log "Cleaning up old local backups (older than ${RETENTION_DAYS} days)"
    
    find "$BACKUP_BASE_DIR" -name "nfl-platform-backup-*" -mtime +$RETENTION_DAYS -delete
    
    # Clean S3 backups older than retention period
    if command -v aws >/dev/null 2>&1; then
        cutoff_date=$(date -d "${RETENTION_DAYS} days ago" +%Y-%m-%d)
        
        aws s3 ls "s3://${S3_BUCKET}/" --recursive | \
        while read -r line; do
            backup_date=$(echo "$line" | awk '{print $1}')
            backup_key=$(echo "$line" | awk '{print $4}')
            
            if [[ "$backup_date" < "$cutoff_date" ]]; then
                aws s3 rm "s3://${S3_BUCKET}/${backup_key}" || true
            fi
        done
    fi
    
    success "Old backups cleaned up"
}

# Main backup process
main() {
    log "Starting NFL Platform Backup Process"
    log "Backup name: ${BACKUP_NAME}"
    
    # Pre-backup checks
    if ! mkdir -p "$BACKUP_BASE_DIR"; then
        error "Cannot create backup base directory: $BACKUP_BASE_DIR"
        exit 1
    fi
    
    # Check disk space (require at least 10GB free)
    available_space=$(df "$BACKUP_BASE_DIR" | awk 'NR==2 {print $4}')
    required_space=$((10 * 1024 * 1024))  # 10GB in KB
    
    if [ "$available_space" -lt "$required_space" ]; then
        error "Insufficient disk space for backup"
        exit 1
    fi
    
    # Execute backup steps
    create_backup_dirs
    backup_data_files
    backup_database
    backup_configs
    backup_logs
    create_backup_metadata
    
    if ! encrypt_backup; then
        error "Backup process failed at encryption step"
        exit 1
    fi
    
    upload_to_s3
    cleanup_old_backups
    
    # Final verification
    backup_file="${BACKUP_BASE_DIR}/${BACKUP_NAME}.tar.gz"
    if [ -f "${backup_file}.gpg" ]; then
        backup_file="${backup_file}.gpg"
    fi
    
    if [ -f "$backup_file" ]; then
        backup_size=$(du -h "$backup_file" | cut -f1)
        success "Backup completed successfully!"
        success "Backup file: $backup_file"
        success "Backup size: $backup_size"
    else
        error "Backup file not found after completion"
        exit 1
    fi
}

# Trap cleanup
cleanup() {
    error "Backup interrupted"
    rm -rf "${BACKUP_BASE_DIR}/${BACKUP_NAME}" 2>/dev/null || true
    exit 1
}

trap cleanup INT TERM

# Run main function
main "$@"