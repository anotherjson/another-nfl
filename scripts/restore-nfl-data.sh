#!/bin/bash
# NFL Platform Data Restore Script

set -e

# Configuration
BACKUP_BASE_DIR="${BACKUP_BASE_DIR:-/opt/nfl-backups}"
NFL_DATA_DIR="${NFL_DATA_DIR:-/opt/nfl-data}"
S3_BUCKET="${S3_BUCKET:-nfl-platform-backups-prod}"
ENCRYPTION_KEY_FILE="${ENCRYPTION_KEY_FILE:-/etc/nfl-platform/backup-key}"
RESTORE_DIR="/tmp/nfl-restore-$$"

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

# Usage information
usage() {
    cat <<EOF
Usage: $0 [OPTIONS] <backup-name|backup-file>

Restore NFL Platform from backup

OPTIONS:
    -l, --list              List available backups
    -d, --data-only         Restore only data files
    -b, --database-only     Restore only database
    -c, --configs-only      Restore only configuration files
    -f, --force             Force restore without confirmation
    -s, --from-s3          Download backup from S3
    -v, --verify           Verify backup integrity before restore
    -h, --help             Show this help message

EXAMPLES:
    $0 --list
    $0 nfl-platform-backup-20241220_120000
    $0 --from-s3 nfl-platform-backup-20241220_120000
    $0 --data-only --force backup.tar.gz
    
EOF
}

# List available backups
list_backups() {
    log "Listing available backups"
    
    echo
    echo "Local backups in $BACKUP_BASE_DIR:"
    if ls -la "$BACKUP_BASE_DIR"/nfl-platform-backup-*.tar.gz* 2>/dev/null; then
        echo
    else
        echo "  No local backups found"
        echo
    fi
    
    # List S3 backups if AWS CLI is available
    if command -v aws >/dev/null 2>&1; then
        echo "S3 backups in s3://$S3_BUCKET:"
        aws s3 ls "s3://$S3_BUCKET/" --recursive --human-readable \
            | grep "nfl-platform-backup-" \
            | sort -k1,2 -r \
            | head -20
        echo
    else
        warning "AWS CLI not available, cannot list S3 backups"
    fi
}

# Download backup from S3
download_from_s3() {
    local backup_name="$1"
    
    log "Downloading backup from S3: $backup_name"
    
    if ! command -v aws >/dev/null 2>&1; then
        error "AWS CLI not found"
        return 1
    fi
    
    # Find the backup in S3
    s3_path=$(aws s3 ls "s3://$S3_BUCKET/" --recursive | grep "$backup_name" | head -1 | awk '{print $4}')
    
    if [ -z "$s3_path" ]; then
        error "Backup not found in S3: $backup_name"
        return 1
    fi
    
    local_file="${BACKUP_BASE_DIR}/$(basename "$s3_path")"
    
    aws s3 cp "s3://${S3_BUCKET}/${s3_path}" "$local_file" \
        || { error "Failed to download backup from S3"; return 1; }
    
    echo "$local_file"
}

# Decrypt and extract backup
extract_backup() {
    local backup_file="$1"
    local extract_dir="$2"
    
    log "Extracting backup: $backup_file"
    
    mkdir -p "$extract_dir"
    
    if [[ "$backup_file" == *.gpg ]]; then
        if [ ! -f "$ENCRYPTION_KEY_FILE" ]; then
            error "Encryption key file not found: $ENCRYPTION_KEY_FILE"
            return 1
        fi
        
        log "Decrypting backup"
        gpg --decrypt \
            --cipher-algo AES256 \
            --passphrase-file "$ENCRYPTION_KEY_FILE" \
            --batch --yes \
            "$backup_file" | \
        tar -xzf - -C "$extract_dir" \
            || { error "Failed to decrypt and extract backup"; return 1; }
    else
        tar -xzf "$backup_file" -C "$extract_dir" \
            || { error "Failed to extract backup"; return 1; }
    fi
    
    success "Backup extracted successfully"
}

# Verify backup integrity
verify_backup() {
    local extract_dir="$1"
    
    log "Verifying backup integrity"
    
    backup_info="${extract_dir}/nfl-platform-backup-*/backup-info.json"
    
    if [ ! -f $backup_info ]; then
        warning "Backup metadata not found, skipping verification"
        return 0
    fi
    
    # Check if all expected components are present
    local has_data=$(jq -r '.components.data_files' $backup_info 2>/dev/null || echo "false")
    local has_database=$(jq -r '.components.database' $backup_info 2>/dev/null || echo "false")
    local has_configs=$(jq -r '.components.configs' $backup_info 2>/dev/null || echo "false")
    
    local backup_dir=$(dirname $backup_info)
    
    if [ "$has_data" = "true" ] && [ ! -f "$backup_dir/data/nfl-data.tar.gz" ]; then
        error "Data files missing from backup"
        return 1
    fi
    
    if [ "$has_database" = "true" ] && [ ! -f "$backup_dir/database/nfl_platform.dump" ]; then
        error "Database backup missing"
        return 1
    fi
    
    if [ "$has_configs" = "true" ] && [ ! -d "$backup_dir/configs" ]; then
        error "Configuration files missing"
        return 1
    fi
    
    success "Backup integrity verified"
    
    # Display backup information
    echo
    log "Backup Information:"
    jq -r '. | "  Backup Date: " + .date + "\n  Version: " + .version + "\n  Size: " + (.backup_size_bytes | tostring) + " bytes"' $backup_info
    echo
}

# Restore data files
restore_data() {
    local backup_dir="$1"
    
    log "Restoring data files"
    
    data_backup="${backup_dir}/data/nfl-data.tar.gz"
    
    if [ ! -f "$data_backup" ]; then
        warning "No data backup found in backup set"
        return 0
    fi
    
    # Create backup of current data
    if [ -d "$NFL_DATA_DIR" ]; then
        log "Backing up current data to ${NFL_DATA_DIR}.backup"
        mv "$NFL_DATA_DIR" "${NFL_DATA_DIR}.backup.$(date +%Y%m%d_%H%M%S)"
    fi
    
    mkdir -p "$NFL_DATA_DIR"
    
    # Extract data files
    tar -xzf "$data_backup" -C "$NFL_DATA_DIR" \
        || { error "Failed to restore data files"; return 1; }
    
    # Verify parquet files if manifest exists
    manifest="${backup_dir}/data/parquet-manifest.txt"
    if [ -f "$manifest" ]; then
        log "Verifying parquet files against manifest"
        
        missing_files=0
        while read -r file_path size mtime; do
            if [ ! -f "$file_path" ]; then
                warning "Missing file: $file_path"
                ((missing_files++))
            fi
        done < "$manifest"
        
        if [ $missing_files -eq 0 ]; then
            success "All parquet files verified"
        else
            warning "$missing_files parquet files are missing"
        fi
    fi
    
    success "Data files restored successfully"
}

# Restore database
restore_database() {
    local backup_dir="$1"
    
    log "Restoring PostgreSQL database"
    
    db_backup="${backup_dir}/database/nfl_platform.dump"
    catalog_backup="${backup_dir}/database/nfl_catalog.dump"
    globals_backup="${backup_dir}/database/globals.sql"
    
    if [ ! -f "$db_backup" ]; then
        warning "No database backup found in backup set"
        return 0
    fi
    
    # Database connection details
    PGHOST="${PGHOST:-postgres}"
    PGPORT="${PGPORT:-5432}"
    PGUSER="${PGUSER:-nfl_admin}"
    
    if ! command -v pg_restore >/dev/null 2>&1; then
        error "pg_restore not found"
        return 1
    fi
    
    # Test database connection
    if ! pg_isready -h "$PGHOST" -p "$PGPORT" -U "$PGUSER" >/dev/null 2>&1; then
        error "Cannot connect to PostgreSQL database"
        return 1
    fi
    
    # Restore global objects first
    if [ -f "$globals_backup" ]; then
        log "Restoring global database objects"
        psql -h "$PGHOST" -p "$PGPORT" -U "$PGUSER" -d postgres -f "$globals_backup" \
            || warning "Failed to restore global objects"
    fi
    
    # Drop and recreate databases (destructive operation)
    log "Recreating databases (this will drop existing data!)"
    
    psql -h "$PGHOST" -p "$PGPORT" -U "$PGUSER" -d postgres <<EOF
DROP DATABASE IF EXISTS nfl_platform;
DROP DATABASE IF EXISTS nfl_catalog;
CREATE DATABASE nfl_platform;
CREATE DATABASE nfl_catalog;
EOF
    
    # Restore main database
    log "Restoring main database"
    pg_restore \
        --host="$PGHOST" \
        --port="$PGPORT" \
        --username="$PGUSER" \
        --dbname="nfl_platform" \
        --verbose \
        --clean \
        --create \
        --if-exists \
        "$db_backup" \
        || { error "Failed to restore main database"; return 1; }
    
    # Restore catalog database
    if [ -f "$catalog_backup" ]; then
        log "Restoring catalog database"
        pg_restore \
            --host="$PGHOST" \
            --port="$PGPORT" \
            --username="$PGUSER" \
            --dbname="nfl_catalog" \
            --verbose \
            --clean \
            --create \
            --if-exists \
            "$catalog_backup" \
            || warning "Failed to restore catalog database"
    fi
    
    success "Database restored successfully"
}

# Restore configuration files
restore_configs() {
    local backup_dir="$1"
    
    log "Restoring configuration files"
    
    configs_dir="${backup_dir}/configs"
    
    if [ ! -d "$configs_dir" ]; then
        warning "No configuration backup found in backup set"
        return 0
    fi
    
    # Restore each configuration archive
    for config_archive in "$configs_dir"/*.tar.gz; do
        if [ -f "$config_archive" ]; then
            config_name=$(basename "$config_archive" .tar.gz)
            target_dir=""
            
            case "$config_name" in
                "nfl-config")
                    target_dir="/opt/nfl-config"
                    ;;
                "nfl-platform")
                    target_dir="/etc/nfl-platform"
                    ;;
                "configs")
                    target_dir="/opt/nfl-platform/current/configs"
                    ;;
                "docker")
                    target_dir="/opt/nfl-platform/current/docker"
                    ;;
                "ansible")
                    target_dir="/opt/nfl-platform/current/ansible"
                    ;;
            esac
            
            if [ -n "$target_dir" ]; then
                log "Restoring $config_name to $target_dir"
                
                # Backup current configs
                if [ -d "$target_dir" ]; then
                    mv "$target_dir" "${target_dir}.backup.$(date +%Y%m%d_%H%M%S)"
                fi
                
                mkdir -p "$(dirname "$target_dir")"
                tar -xzf "$config_archive" -C "$(dirname "$target_dir")" \
                    || warning "Failed to restore $config_name"
            fi
        fi
    done
    
    success "Configuration files restored successfully"
}

# Main restore function
main() {
    local backup_file=""
    local data_only=false
    local database_only=false
    local configs_only=false
    local force=false
    local from_s3=false
    local verify=true
    
    # Parse command line arguments
    while [[ $# -gt 0 ]]; do
        case $1 in
            -l|--list)
                list_backups
                exit 0
                ;;
            -d|--data-only)
                data_only=true
                shift
                ;;
            -b|--database-only)
                database_only=true
                shift
                ;;
            -c|--configs-only)
                configs_only=true
                shift
                ;;
            -f|--force)
                force=true
                shift
                ;;
            -s|--from-s3)
                from_s3=true
                shift
                ;;
            -v|--no-verify)
                verify=false
                shift
                ;;
            -h|--help)
                usage
                exit 0
                ;;
            -*)
                error "Unknown option: $1"
                usage
                exit 1
                ;;
            *)
                backup_file="$1"
                shift
                ;;
        esac
    done
    
    # Validate arguments
    if [ -z "$backup_file" ]; then
        error "Backup file or name is required"
        usage
        exit 1
    fi
    
    # Handle S3 download
    if [ "$from_s3" = true ]; then
        backup_file=$(download_from_s3 "$backup_file")
        if [ $? -ne 0 ]; then
            exit 1
        fi
    elif [ ! -f "$backup_file" ]; then
        # Try to find backup in standard location
        candidate="${BACKUP_BASE_DIR}/${backup_file}"
        if [ -f "${candidate}.tar.gz" ]; then
            backup_file="${candidate}.tar.gz"
        elif [ -f "${candidate}.tar.gz.gpg" ]; then
            backup_file="${candidate}.tar.gz.gpg"
        elif [ -f "$candidate" ]; then
            backup_file="$candidate"
        else
            error "Backup file not found: $backup_file"
            exit 1
        fi
    fi
    
    log "Starting NFL Platform Restore Process"
    log "Backup file: $backup_file"
    log "Restore directory: $RESTORE_DIR"
    
    # Confirmation prompt
    if [ "$force" != true ]; then
        echo
        warning "This will overwrite existing NFL Platform data!"
        echo -n "Are you sure you want to continue? (yes/no): "
        read -r confirmation
        
        if [ "$confirmation" != "yes" ]; then
            log "Restore cancelled"
            exit 0
        fi
    fi
    
    # Extract backup
    if ! extract_backup "$backup_file" "$RESTORE_DIR"; then
        exit 1
    fi
    
    # Verify backup if requested
    if [ "$verify" = true ]; then
        if ! verify_backup "$RESTORE_DIR"; then
            exit 1
        fi
    fi
    
    # Find the extracted backup directory
    backup_dir=$(find "$RESTORE_DIR" -name "nfl-platform-backup-*" -type d | head -1)
    
    if [ -z "$backup_dir" ]; then
        error "Cannot find backup directory in extracted files"
        exit 1
    fi
    
    # Perform restore based on options
    if [ "$data_only" = true ]; then
        restore_data "$backup_dir"
    elif [ "$database_only" = true ]; then
        restore_database "$backup_dir"
    elif [ "$configs_only" = true ]; then
        restore_configs "$backup_dir"
    else
        # Full restore
        restore_data "$backup_dir"
        restore_database "$backup_dir"
        restore_configs "$backup_dir"
    fi
    
    success "NFL Platform restore completed successfully!"
    
    log "Post-restore recommendations:"
    log "1. Restart all NFL Platform services"
    log "2. Verify data integrity and functionality"
    log "3. Check application logs for any issues"
    log "4. Run health checks to ensure everything is working"
}

# Cleanup function
cleanup() {
    log "Cleaning up temporary files"
    rm -rf "$RESTORE_DIR" 2>/dev/null || true
}

# Trap cleanup
trap cleanup EXIT INT TERM

# Run main function
main "$@"