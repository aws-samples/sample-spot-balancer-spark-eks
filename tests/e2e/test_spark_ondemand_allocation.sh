#!/bin/bash
# Test: Full on-demand allocation (spot-ratio=0.0)
# Verifies that all executors are placed on on-demand instances

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/common.sh"

TEST_NAME="Full On-Demand Allocation"
SPOT_RATIO="0.0"
EXECUTOR_COUNT=5
JOB_SUFFIX=$(generate_job_suffix)
JOB_ID="spark-pi-${JOB_SUFFIX}"

log_info "========================================="
log_info "Test: $TEST_NAME"
log_info "Job ID: $JOB_ID"
log_info "Spot Ratio: $SPOT_RATIO"
log_info "Executor Count: $EXECUTOR_COUNT"
log_info "========================================="

# Check webhook health
if ! check_webhook_health; then
    log_error "Webhook health check failed"
    exit 1
fi

# Create namespace if it doesn't exist
kubectl create namespace "$TEST_NAMESPACE" 2>/dev/null || true

# Deploy Spark job
log_info "Deploying Spark Pi job..."
sed -e "s/\${TEST_NAMESPACE}/$TEST_NAMESPACE/g" \
    -e "s/\${SPOT_RATIO}/$SPOT_RATIO/g" \
    -e "s/\${EXECUTOR_COUNT}/$EXECUTOR_COUNT/g" \
    -e "s/\${JOB_SUFFIX}/$JOB_SUFFIX/g" \
    "$SCRIPT_DIR/spark-pi-job.yaml" | kubectl apply -f -

# Wait for driver pod to be running
if ! wait_for_pod_phase "spark-pi-${JOB_SUFFIX}" "$TEST_NAMESPACE" "Running" 120; then
    log_error "Driver pod failed to start"
    kubectl describe pod "spark-pi-${JOB_SUFFIX}" -n "$TEST_NAMESPACE"
    cleanup_job "$JOB_ID" "$TEST_NAMESPACE"
    exit 1
fi

# Wait for executors to be created
if ! wait_for_executors "$JOB_ID" "$TEST_NAMESPACE" "$EXECUTOR_COUNT" 300; then
    log_error "Executors failed to start"
    kubectl get pods -n "$TEST_NAMESPACE" -l "${JOB_ID_LABEL}=${JOB_ID}"
    cleanup_job "$JOB_ID" "$TEST_NAMESPACE"
    exit 1
fi

# Give executors time to be scheduled
log_info "Waiting for executors to be scheduled..."
sleep 45

# Wait for executors to actually be in Running state
log_info "Verifying executors are running..."
max_wait=60
elapsed=0
while [ $elapsed -lt $max_wait ]; do
    running_count=$(kubectl get pods -n "$TEST_NAMESPACE" \
        -l "${WORKLOAD_ROLE_LABEL}=executor,${JOB_ID_LABEL}=${JOB_ID}" \
        -o json 2>/dev/null | jq -r '[.items[] | select(.status.phase == "Running")] | length')
    
    log_info "Running executors: $running_count / $EXECUTOR_COUNT"
    
    if [ "$running_count" -ge "$EXECUTOR_COUNT" ]; then
        log_success "All executors are running"
        break
    fi
    
    sleep 5
    elapsed=$((elapsed + 5))
done

# Verify allocation
log_info "Verifying executor allocation..."
read -r total spot_count ondemand_count unscheduled <<< $(get_executor_allocation "$JOB_ID" "$TEST_NAMESPACE")

log_info "Results:"
log_info "  Total executors: $total"
log_info "  Spot executors: $spot_count"
log_info "  On-Demand executors: $ondemand_count"
log_info "  Unscheduled: $unscheduled"

# Verify spot ratio (should be 0.0 with small tolerance)
if ! verify_spot_ratio "$JOB_ID" "$TEST_NAMESPACE" "$SPOT_RATIO" 0.1; then
    log_error "Spot ratio verification failed"
    kubectl get pods -n "$TEST_NAMESPACE" -l "${JOB_ID_LABEL}=${JOB_ID}" -o wide
    cleanup_job "$JOB_ID" "$TEST_NAMESPACE"
    exit 1
fi

# Additional verification: ensure NO spot executors
if [ "$spot_count" -gt 0 ]; then
    log_error "Found $spot_count spot executors when expecting 0"
    cleanup_job "$JOB_ID" "$TEST_NAMESPACE"
    exit 1
fi

log_success "========================================="
log_success "Test PASSED: $TEST_NAME"
log_success "All $ondemand_count executors placed on on-demand instances"
log_success "========================================="

# Cleanup
cleanup_job "$JOB_ID" "$TEST_NAMESPACE"

exit 0
