#!/bin/bash
# NFL Platform Maintenance Script

set -e

# Configuration
NFL_DATA_DIR="${NFL_DATA_DIR:-/opt/nfl-data}"
NFL_LOGS_DIR="${NFL_LOGS_DIR:-/opt/nfl-logs}"
DOCKER_COMPOSE_FILE="${DOCKER_COMPOSE_FILE:-/opt/nfl-platform/current/docker-compose.yml}"
DOCKER_COMPOSE_PROD_FILE="${DOCKER_COMPOSE_PROD_FILE:-/opt/nfl-platform/current/docker-compose.prod.yml}"
LOG_RETENTION_DAYS="${LOG_RETENTION_DAYS:-30}"
DATA_RETENTION_DAYS="${DATA_RETENTION_DAYS:-90}"

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
Usage: $0 [COMMAND] [OPTIONS]

NFL Platform Maintenance Commands

COMMANDS:
    cleanup         Clean old logs and temporary files
    optimize        Optimize database and clean Docker resources
    health          Run comprehensive health checks
    update          Update NFL Platform containers
    restart         Restart NFL Platform services
    status          Show system status and resource usage
    vacuum          Vacuum and analyze PostgreSQL databases
    logs            Manage and rotate log files
    disk            Disk space analysis and cleanup

OPTIONS:
    --dry-run       Show what would be done without executing
    --force         Force operations without confirmation
    --verbose       Verbose output
    --help          Show this help message

EXAMPLES:
    $0 cleanup --dry-run
    $0 optimize --force
    $0 health --verbose
    $0 update

EOF
}

# Cleanup old logs and temporary files
cleanup() {
    local dry_run=$1
    
    log "Starting cleanup process"
    
    if [ "$dry_run" = true ]; then
        log "DRY RUN - No changes will be made"
    fi
    
    # Clean old log files
    log "Cleaning old log files (older than $LOG_RETENTION_DAYS days)"
    
    if [ -d "$NFL_LOGS_DIR" ]; then
        if [ "$dry_run" = true ]; then
            find "$NFL_LOGS_DIR" -name "*.log" -mtime +$LOG_RETENTION_DAYS -print
            find "$NFL_LOGS_DIR" -name "*.json" -mtime +$LOG_RETENTION_DAYS -print
        else
            files_removed=$(find "$NFL_LOGS_DIR" -name "*.log" -mtime +$LOG_RETENTION_DAYS -delete -print | wc -l)
            json_files_removed=$(find "$NFL_LOGS_DIR" -name "*.json" -mtime +$LOG_RETENTION_DAYS -delete -print | wc -l)
            success "Removed $files_removed log files and $json_files_removed JSON files"
        fi
    fi
    
    # Clean old Docker logs
    log "Cleaning Docker container logs"
    
    if [ "$dry_run" = true ]; then
        docker system df
    else
        docker system prune -f --filter "until=24h"
        success "Docker system cleaned"
    fi
    
    # Clean old data extraction files
    log "Cleaning old data extraction files (older than $DATA_RETENTION_DAYS days)"
    
    if [ -d "$NFL_DATA_DIR" ]; then
        if [ "$dry_run" = true ]; then
            find "$NFL_DATA_DIR" -name "*.tmp" -mtime +1 -print
            find "$NFL_DATA_DIR" -name "*.lock" -mtime +1 -print
        else
            temp_files=$(find "$NFL_DATA_DIR" -name "*.tmp" -mtime +1 -delete -print | wc -l)
            lock_files=$(find "$NFL_DATA_DIR" -name "*.lock" -mtime +1 -delete -print | wc -l)
            success "Removed $temp_files temporary files and $lock_files lock files"
        fi
    fi
    
    # Clean old backup files (keep last 10)
    log "Cleaning old backup files (keeping last 10)"
    
    backup_dir="/opt/nfl-backups"
    if [ -d "$backup_dir" ]; then
        if [ "$dry_run" = true ]; then
            ls -t "$backup_dir"/nfl-platform-backup-*.tar.gz* 2>/dev/null | tail -n +11
        else
            backups_removed=$(ls -t "$backup_dir"/nfl-platform-backup-*.tar.gz* 2>/dev/null | tail -n +11 | xargs rm -f | wc -l)
            success "Removed $backups_removed old backup files"
        fi
    fi
}

# Optimize database and Docker resources
optimize() {
    local force=$1
    
    log "Starting optimization process"
    
    # Docker optimization
    log "Optimizing Docker resources"
    
    if [ "$force" = true ] || confirm "Clean unused Docker images?"; then
        docker image prune -f
        docker volume prune -f
        docker network prune -f
        success "Docker resources optimized"
    fi
    
    # Database optimization
    log "Optimizing PostgreSQL databases"
    
    if [ "$force" = true ] || confirm "Vacuum and analyze databases?"; then
        vacuum_databases
    fi
    
    # Data optimization
    log "Optimizing NFL data storage"
    
    if [ -d "$NFL_DATA_DIR" ]; then
        # Compress old parquet files
        find "$NFL_DATA_DIR" -name "*.parquet" -mtime +30 -exec gzip {} \; 2>/dev/null || true
        success "Old parquet files compressed"
    fi
}

# Vacuum and analyze PostgreSQL databases
vacuum_databases() {
    log "Vacuuming PostgreSQL databases"
    
    # Database connection details
    PGHOST="${PGHOST:-postgres}"
    PGPORT="${PGPORT:-5432}"
    PGUSER="${PGUSER:-nfl_admin}"
    
    if ! command -v psql >/dev/null 2>&1; then
        warning "psql not found, skipping database vacuum"
        return
    fi
    
    # Test connection
    if ! pg_isready -h "$PGHOST" -p "$PGPORT" -U "$PGUSER" >/dev/null 2>&1; then
        warning "Cannot connect to PostgreSQL, skipping database vacuum"
        return
    fi
    
    # Vacuum main database
    log "Vacuuming nfl_platform database"
    psql -h "$PGHOST" -p "$PGPORT" -U "$PGUSER" -d nfl_platform \
        -c "VACUUM ANALYZE;" \
        || warning "Failed to vacuum nfl_platform database"
    
    # Vacuum catalog database
    log "Vacuuming nfl_catalog database"
    psql -h "$PGHOST" -p "$PGPORT" -U "$PGUSER" -d nfl_catalog \
        -c "VACUUM ANALYZE;" \
        || warning "Failed to vacuum nfl_catalog database"
    
    success "Database vacuum completed"
}

# Run comprehensive health checks
health_check() {
    local verbose=$1
    
    log "Running NFL Platform health checks"
    
    local overall_health=0
    
    # Check disk space
    log "Checking disk space"
    
    disk_usage=$(df / | awk 'NR==2 {print $5}' | sed 's/%//')
    
    if [ "$disk_usage" -gt 85 ]; then
        error "Disk usage is high: ${disk_usage}%"
        overall_health=1
    elif [ "$disk_usage" -gt 75 ]; then
        warning "Disk usage is moderate: ${disk_usage}%"
    else
        success "Disk usage is acceptable: ${disk_usage}%"
    fi
    
    # Check memory usage
    log "Checking memory usage"
    
    memory_usage=$(free | awk 'NR==2{printf "%.0f", $3*100/$2}')
    
    if [ "$memory_usage" -gt 90 ]; then
        error "Memory usage is high: ${memory_usage}%"
        overall_health=1
    elif [ "$memory_usage" -gt 80 ]; then
        warning "Memory usage is moderate: ${memory_usage}%"
    else
        success "Memory usage is acceptable: ${memory_usage}%"
    fi
    
    # Check Docker containers
    log "Checking Docker containers"
    
    if command -v docker >/dev/null 2>&1; then
        unhealthy_containers=$(docker ps --filter "health=unhealthy" --format "table {{.Names}}" | tail -n +2)
        
        if [ -n "$unhealthy_containers" ]; then
            error "Unhealthy containers found:"
            echo "$unhealthy_containers"
            overall_health=1
        else
            success "All containers are healthy"
        fi
        
        if [ "$verbose" = true ]; then
            log "Container status:"
            docker ps --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}"
        fi
    else
        warning "Docker not available for health check"
    fi
    
    # Check database connectivity
    log "Checking database connectivity"
    
    PGHOST="${PGHOST:-postgres}"
    PGPORT="${PGPORT:-5432}"
    PGUSER="${PGUSER:-nfl_admin}"
    
    if command -v pg_isready >/dev/null 2>&1; then
        if pg_isready -h "$PGHOST" -p "$PGPORT" -U "$PGUSER" >/dev/null 2>&1; then
            success "Database is accessible"
            
            # Check database connections
            if [ "$verbose" = true ]; then
                connection_count=$(psql -h "$PGHOST" -p "$PGPORT" -U "$PGUSER" -d nfl_platform \
                    -t -c "SELECT count(*) FROM pg_stat_activity;" 2>/dev/null | xargs)
                log "Active database connections: $connection_count"
            fi
        else
            error "Database is not accessible"
            overall_health=1
        fi
    else
        warning "pg_isready not available for database check"
    fi
    
    # Check data freshness
    log "Checking data freshness"
    
    if [ -d "$NFL_DATA_DIR" ]; then
        latest_file=$(find "$NFL_DATA_DIR" -name "*.parquet" -type f -printf '%T@ %p\n' 2>/dev/null | sort -n | tail -1 | cut -d' ' -f2-)
        
        if [ -n "$latest_file" ]; then
            file_age=$(( $(date +%s) - $(stat -c %Y "$latest_file" 2>/dev/null || echo 0) ))
            age_hours=$((file_age / 3600))
            
            if [ $age_hours -gt 48 ]; then
                warning "Data is stale (${age_hours} hours old)"
            else
                success "Data is fresh (${age_hours} hours old)"
            fi
            
            if [ "$verbose" = true ]; then
                log "Latest data file: $latest_file"
            fi
        else
            warning "No data files found"
        fi
    else
        warning "NFL data directory not found"
    fi
    
    # Final health assessment
    if [ $overall_health -eq 0 ]; then
        success "Overall system health: GOOD 🏈"
    else
        error "Overall system health: NEEDS ATTENTION ⚠️"
    fi
    
    return $overall_health
}

# Update NFL Platform containers
update_containers() {
    local force=$1
    
    log "Updating NFL Platform containers"
    
    if [ ! -f "$DOCKER_COMPOSE_FILE" ]; then
        error "Docker Compose file not found: $DOCKER_COMPOSE_FILE"
        return 1
    fi
    
    if [ "$force" != true ] && ! confirm "Update all containers?"; then
        log "Update cancelled"
        return 0
    fi
    
    # Pull latest images
    log "Pulling latest container images"
    
    docker-compose -f "$DOCKER_COMPOSE_FILE" -f "$DOCKER_COMPOSE_PROD_FILE" pull
    
    # Restart services with new images
    log "Restarting services with updated images"
    
    docker-compose -f "$DOCKER_COMPOSE_FILE" -f "$DOCKER_COMPOSE_PROD_FILE" up -d
    
    success "Container update completed"
}

# Restart NFL Platform services
restart_services() {
    log "Restarting NFL Platform services"
    
    if [ ! -f "$DOCKER_COMPOSE_FILE" ]; then
        error "Docker Compose file not found: $DOCKER_COMPOSE_FILE"
        return 1
    fi
    
    # Graceful restart
    docker-compose -f "$DOCKER_COMPOSE_FILE" -f "$DOCKER_COMPOSE_PROD_FILE" restart
    
    # Wait for services to be ready
    log "Waiting for services to be ready..."
    sleep 30
    
    # Quick health check
    if health_check false >/dev/null 2>&1; then
        success "Services restarted successfully"
    else
        warning "Services restarted but health check failed"
    fi
}

# Show system status
show_status() {
    log "NFL Platform System Status"
    echo
    
    # System resources
    log "System Resources:"
    echo "  CPU Usage: $(top -bn1 | grep "Cpu(s)" | awk '{print $2}' | cut -d'%' -f1)%"
    echo "  Memory: $(free -h | awk 'NR==2{printf "%s/%s (%.0f%%)", $3,$2,$3*100/$2}')"
    echo "  Disk: $(df -h / | awk 'NR==2{printf "%s/%s (%s)", $3,$2,$5}')"
    echo
    
    # Docker status
    if command -v docker >/dev/null 2>&1; then
        log "Docker Containers:"
        docker ps --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}" | head -10
        echo
    fi
    
    # Database status
    PGHOST="${PGHOST:-postgres}"
    PGPORT="${PGPORT:-5432}"
    PGUSER="${PGUSER:-nfl_admin}"
    
    if command -v psql >/dev/null 2>&1 && pg_isready -h "$PGHOST" -p "$PGPORT" -U "$PGUSER" >/dev/null 2>&1; then
        log "Database Status:"
        psql -h "$PGHOST" -p "$PGPORT" -U "$PGUSER" -d nfl_platform \
            -c "SELECT datname, numbackends as connections FROM pg_stat_database WHERE datname IN ('nfl_platform', 'nfl_catalog');" \
            2>/dev/null || true
        echo
    fi
    
    # Data status
    if [ -d "$NFL_DATA_DIR" ]; then
        log "Data Status:"
        echo "  Total size: $(du -sh "$NFL_DATA_DIR" 2>/dev/null | cut -f1 || echo "unknown")"
        echo "  Files: $(find "$NFL_DATA_DIR" -type f 2>/dev/null | wc -l)"
        echo "  Latest: $(find "$NFL_DATA_DIR" -type f -printf '%TY-%Tm-%Td %TH:%TM %p\n' 2>/dev/null | sort | tail -1 | cut -d' ' -f3- || echo "unknown")"
    fi
}

# Confirmation helper
confirm() {
    local message="$1"
    echo -n "$message (y/N): "
    read -r response
    [[ "$response" =~ ^[Yy]$ ]]
}

# Log file management
manage_logs() {
    local action="${1:-rotate}"
    
    case "$action" in
        rotate)
            log "Rotating log files"
            
            if command -v logrotate >/dev/null 2>&1; then
                # Create logrotate config for NFL Platform
                cat > /tmp/nfl-logrotate.conf <<EOF
$NFL_LOGS_DIR/*/*.log {
    daily
    missingok
    rotate 7
    compress
    delaycompress
    notifempty
    sharedscripts
    postrotate
        docker-compose -f $DOCKER_COMPOSE_FILE restart 2>/dev/null || true
    endscript
}
EOF
                
                logrotate -f /tmp/nfl-logrotate.conf
                rm -f /tmp/nfl-logrotate.conf
                success "Log rotation completed"
            else
                warning "logrotate not available, manually cleaning logs"
                cleanup false
            fi
            ;;
        analyze)
            log "Analyzing log files"
            
            if [ -d "$NFL_LOGS_DIR" ]; then
                echo "Log file sizes:"
                find "$NFL_LOGS_DIR" -name "*.log" -exec du -sh {} \; | sort -hr | head -10
                
                echo
                echo "Recent errors (last 100 lines):"
                find "$NFL_LOGS_DIR" -name "*.log" -exec grep -l "ERROR\|FATAL\|Exception" {} \; | \
                head -5 | \
                xargs tail -100 | \
                grep -E "ERROR|FATAL|Exception" | \
                tail -20
            fi
            ;;
    esac
}

# Main function
main() {
    local command=""
    local dry_run=false
    local force=false
    local verbose=false
    
    # Parse arguments
    while [[ $# -gt 0 ]]; do
        case $1 in
            cleanup|optimize|health|update|restart|status|vacuum|logs|disk)
                command="$1"
                shift
                ;;
            --dry-run)
                dry_run=true
                shift
                ;;
            --force)
                force=true
                shift
                ;;
            --verbose)
                verbose=true
                shift
                ;;
            --help)
                usage
                exit 0
                ;;
            *)
                error "Unknown option: $1"
                usage
                exit 1
                ;;
        esac
    done
    
    # Default command
    if [ -z "$command" ]; then
        command="status"
    fi
    
    log "NFL Platform Maintenance - Command: $command"
    
    # Execute command
    case "$command" in
        cleanup)
            cleanup $dry_run
            ;;
        optimize)
            optimize $force
            ;;
        health)
            health_check $verbose
            ;;
        update)
            update_containers $force
            ;;
        restart)
            restart_services
            ;;
        status)
            show_status
            ;;
        vacuum)
            vacuum_databases
            ;;
        logs)
            manage_logs rotate
            ;;
        disk)
            log "Disk usage analysis:"
            du -sh "$NFL_DATA_DIR"/* 2>/dev/null | sort -hr | head -20 || true
            ;;
        *)
            error "Unknown command: $command"
            usage
            exit 1
            ;;
    esac
}

# Run main function
main "$@"