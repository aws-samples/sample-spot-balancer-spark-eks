#!/bin/bash
# Run all e2e tests

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/common.sh"

log_info "========================================="
log_info "Running Spot Balancer E2E Test Suite"
log_info "========================================="

# Check prerequisites
log_info "Checking prerequisites..."

if ! command -v kubectl &> /dev/null; then
    log_error "kubectl not found. Please install kubectl."
    exit 1
fi

if ! command -v jq &> /dev/null; then
    log_error "jq not found. Please install jq."
    exit 1
fi

if ! kubectl cluster-info &> /dev/null; then
    log_error "Cannot connect to Kubernetes cluster. Check your kubeconfig."
    exit 1
fi

# Check if EKS Auto Mode is enabled
if ! kubectl get nodepools &> /dev/null; then
    log_error "NodePools not found. This cluster may not have EKS Auto Mode enabled."
    log_error "See: https://docs.aws.amazon.com/eks/latest/userguide/automode.html"
    exit 1
fi

log_success "Prerequisites check passed"
log_success "EKS Auto Mode is enabled"

# Check webhook deployment
if ! check_webhook_health; then
    log_error "Webhook is not healthy. Please deploy the webhook first."
    exit 1
fi

# Track test results
TESTS_PASSED=0
TESTS_FAILED=0
FAILED_TESTS=()

# Run tests
run_test() {
    local test_script=$1
    local test_name=$(basename "$test_script" .sh)
    
    log_info ""
    log_info "Running test: $test_name"
    log_info "----------------------------------------"
    
    if bash "$test_script"; then
        TESTS_PASSED=$((TESTS_PASSED + 1))
        log_success "✓ $test_name PASSED"
    else
        TESTS_FAILED=$((TESTS_FAILED + 1))
        FAILED_TESTS+=("$test_name")
        log_error "✗ $test_name FAILED"
    fi
}

# Run all test scripts
run_test "$SCRIPT_DIR/test_spark_spot_allocation.sh"
run_test "$SCRIPT_DIR/test_spark_ondemand_allocation.sh"
run_test "$SCRIPT_DIR/test_spark_mixed_allocation.sh"

# Print summary
log_info ""
log_info "========================================="
log_info "Test Suite Summary"
log_info "========================================="
log_info "Total tests: $((TESTS_PASSED + TESTS_FAILED))"
log_success "Passed: $TESTS_PASSED"

if [ $TESTS_FAILED -gt 0 ]; then
    log_error "Failed: $TESTS_FAILED"
    log_error "Failed tests:"
    for test in "${FAILED_TESTS[@]}"; do
        log_error "  - $test"
    done
    exit 1
else
    log_success "All tests passed!"
    exit 0
fi
