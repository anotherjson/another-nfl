#!/bin/bash
# Production Health Check Script for NFL Platform

set -e

# Configuration
PRODUCTION_URL="${PRODUCTION_URL:-https://nfl-platform.example.com}"
TIMEOUT="${TIMEOUT:-30}"
RETRY_COUNT="${RETRY_COUNT:-3}"
HEALTH_CHECK_INTERVAL="${HEALTH_CHECK_INTERVAL:-10}"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Logging function
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

# Health check function with retry logic
check_endpoint() {
    local url="$1"
    local expected_status="${2:-200}"
    local description="$3"
    local retry_count=0
    
    log "Checking $description at $url"
    
    while [ $retry_count -lt $RETRY_COUNT ]; do
        if curl -s -f --max-time $TIMEOUT -o /dev/null -w "%{http_code}" "$url" | grep -q "$expected_status"; then
            success "$description is healthy"
            return 0
        else
            warning "Attempt $((retry_count + 1))/$RETRY_COUNT failed for $description"
            retry_count=$((retry_count + 1))
            if [ $retry_count -lt $RETRY_COUNT ]; then
                sleep $HEALTH_CHECK_INTERVAL
            fi
        fi
    done
    
    error "$description health check failed after $RETRY_COUNT attempts"
    return 1
}

# Check JSON response endpoint
check_json_endpoint() {
    local url="$1"
    local expected_field="$2"
    local description="$3"
    
    log "Checking JSON endpoint: $description"
    
    response=$(curl -s --max-time $TIMEOUT "$url" || echo "{}")
    
    if echo "$response" | jq -e ".$expected_field" >/dev/null 2>&1; then
        success "$description JSON response is valid"
        return 0
    else
        error "$description JSON response is invalid or missing field '$expected_field'"
        error "Response: $response"
        return 1
    fi
}

# Check database connectivity
check_database() {
    log "Checking database connectivity"
    
    # This would typically connect to a monitoring endpoint that checks DB
    if check_endpoint "$PRODUCTION_URL/api/health/database" 200 "Database connectivity"; then
        success "Database is accessible"
        return 0
    else
        error "Database connectivity check failed"
        return 1
    fi
}

# Check data freshness
check_data_freshness() {
    log "Checking NFL data freshness"
    
    # Check when data was last updated
    current_time=$(date +%s)
    last_update=$(curl -s --max-time $TIMEOUT "$PRODUCTION_URL/api/data/last-update" | jq -r '.timestamp // 0')
    
    if [ "$last_update" = "0" ]; then
        warning "Could not retrieve last update timestamp"
        return 1
    fi
    
    age=$((current_time - last_update))
    max_age=$((24 * 60 * 60))  # 24 hours
    
    if [ $age -lt $max_age ]; then
        success "Data is fresh (updated $((age / 3600)) hours ago)"
        return 0
    else
        warning "Data is stale (updated $((age / 3600)) hours ago)"
        return 1
    fi
}

# Main health check sequence
main() {
    log "Starting NFL Platform Production Health Check"
    log "Production URL: $PRODUCTION_URL"
    log "Timeout: ${TIMEOUT}s, Retries: $RETRY_COUNT"
    echo
    
    # Track overall health
    overall_status=0
    
    # Basic connectivity checks
    log "=== Basic Connectivity Checks ==="
    
    if ! check_endpoint "$PRODUCTION_URL/health" 200 "Load balancer health"; then
        overall_status=1
    fi
    
    if ! check_endpoint "$PRODUCTION_URL/dagster/" 200 "Dagster UI"; then
        overall_status=1
    fi
    
    if ! check_endpoint "$PRODUCTION_URL/grafana/" 200 "Grafana dashboard"; then
        overall_status=1
    fi
    
    echo
    
    # Application-specific checks
    log "=== Application Health Checks ==="
    
    if ! check_json_endpoint "$PRODUCTION_URL/dagster/graphql" "data" "Dagster GraphQL API"; then
        overall_status=1
    fi
    
    if ! check_database; then
        overall_status=1
    fi
    
    echo
    
    # Data quality checks
    log "=== Data Quality Checks ==="
    
    if ! check_data_freshness; then
        overall_status=1
    fi
    
    # Check if extraction services are running
    if ! check_endpoint "$PRODUCTION_URL/api/services/extractor/status" 200 "NFL data extractor service"; then
        overall_status=1
    fi
    
    # Check if dbt models are up to date
    if ! check_endpoint "$PRODUCTION_URL/api/services/dbt/status" 200 "dbt transformation service"; then
        overall_status=1
    fi
    
    echo
    
    # Performance checks
    log "=== Performance Checks ==="
    
    # Measure response time
    response_time=$(curl -s -o /dev/null -w "%{time_total}" --max-time $TIMEOUT "$PRODUCTION_URL/health")
    if (( $(echo "$response_time < 2.0" | bc -l) )); then
        success "Response time is acceptable (${response_time}s)"
    else
        warning "Response time is slow (${response_time}s)"
        overall_status=1
    fi
    
    # Check SSL certificate
    ssl_days=$(echo | openssl s_client -servername "${PRODUCTION_URL#https://}" -connect "${PRODUCTION_URL#https://}:443" 2>/dev/null | openssl x509 -noout -dates | grep notAfter | cut -d= -f2 | xargs -I {} date -d "{}" +%s)
    current_time=$(date +%s)
    days_until_expiry=$(( (ssl_days - current_time) / 86400 ))
    
    if [ $days_until_expiry -gt 30 ]; then
        success "SSL certificate is valid ($days_until_expiry days remaining)"
    else
        warning "SSL certificate expires soon ($days_until_expiry days remaining)"
        overall_status=1
    fi
    
    echo
    
    # Security checks
    log "=== Security Checks ==="
    
    # Check security headers
    if curl -s -I --max-time $TIMEOUT "$PRODUCTION_URL" | grep -q "Strict-Transport-Security"; then
        success "HSTS header is present"
    else
        warning "HSTS header is missing"
        overall_status=1
    fi
    
    if curl -s -I --max-time $TIMEOUT "$PRODUCTION_URL" | grep -q "X-Content-Type-Options"; then
        success "X-Content-Type-Options header is present"
    else
        warning "X-Content-Type-Options header is missing"
        overall_status=1
    fi
    
    echo
    
    # Final status
    log "=== Health Check Summary ==="
    
    if [ $overall_status -eq 0 ]; then
        success "All health checks passed! NFL Platform is healthy 🏈"
        exit 0
    else
        error "Some health checks failed! Please investigate 🚨"
        exit 1
    fi
}

# Trap cleanup
cleanup() {
    log "Health check interrupted"
    exit 1
}

trap cleanup INT TERM

# Run main function
main "$@"