#!/bin/bash
# Common functions for e2e tests

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Default values
TEST_NAMESPACE="${TEST_NAMESPACE:-sandbox}"
WEBHOOK_NAMESPACE="${WEBHOOK_NAMESPACE:-spark-tools}"
CAPACITY_TYPE_LABEL="${CAPACITY_TYPE_LABEL:-karpenter.sh/capacity-type}"
WORKLOAD_ROLE_LABEL="${WORKLOAD_ROLE_LABEL:-spark-role}"
JOB_ID_LABEL="${JOB_ID_LABEL:-emr-containers.amazonaws.com/job.id}"
EKS_COMPUTE_TYPE_LABEL="${EKS_COMPUTE_TYPE_LABEL:-eks.amazonaws.com/compute-type}"

log_info() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

log_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

log_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Wait for pod to reach a specific phase
wait_for_pod_phase() {
    local pod_name=$1
    local namespace=$2
    local phase=$3
    local timeout=${4:-300}
    
    log_info "Waiting for pod $pod_name to reach phase $phase (timeout: ${timeout}s)..."
    
    local elapsed=0
    while [ $elapsed -lt $timeout ]; do
        local current_phase=$(kubectl get pod "$pod_name" -n "$namespace" -o jsonpath='{.status.phase}' 2>/dev/null || echo "NotFound")
        
        if [ "$current_phase" = "$phase" ]; then
            log_success "Pod $pod_name reached phase $phase"
            return 0
        fi
        
        if [ "$current_phase" = "Failed" ] && [ "$phase" != "Failed" ]; then
            log_error "Pod $pod_name failed"
            kubectl describe pod "$pod_name" -n "$namespace"
            return 1
        fi
        
        sleep 5
        elapsed=$((elapsed + 5))
    done
    
    log_error "Timeout waiting for pod $pod_name to reach phase $phase"
    return 1
}

# Wait for executors to be created
wait_for_executors() {
    local job_id=$1
    local namespace=$2
    local expected_count=$3
    local timeout=${4:-300}
    
    log_info "Waiting for $expected_count executors for job $job_id (timeout: ${timeout}s)..."
    
    local elapsed=0
    while [ $elapsed -lt $timeout ]; do
        local current_count=$(kubectl get pods -n "$namespace" \
            -l "${WORKLOAD_ROLE_LABEL}=executor,${JOB_ID_LABEL}=${job_id}" \
            --no-headers 2>/dev/null | wc -l | tr -d ' ')
        
        if [ "$current_count" -ge "$expected_count" ]; then
            log_success "Found $current_count executors (expected: $expected_count)"
            return 0
        fi
        
        log_info "Current executor count: $current_count / $expected_count"
        sleep 5
        elapsed=$((elapsed + 5))
    done
    
    log_error "Timeout waiting for executors"
    return 1
}

# Get executor allocation counts
get_executor_allocation() {
    local job_id=$1
    local namespace=$2
    
    local total=$(kubectl get pods -n "$namespace" \
        -l "${WORKLOAD_ROLE_LABEL}=executor,${JOB_ID_LABEL}=${job_id}" \
        --no-headers 2>/dev/null | wc -l | tr -d ' ')
    
    # Get pods and check the actual node's capacity type label
    local spot_count=0
    local ondemand_count=0
    local unscheduled_count=0
    
    while IFS= read -r pod_json; do
        local node_name=$(echo "$pod_json" | jq -r '.spec.nodeName // empty')
        
        if [ -z "$node_name" ]; then
            unscheduled_count=$((unscheduled_count + 1))
        else
            # Query the node directly for its capacity type label
            local capacity_type=$(kubectl get node "$node_name" -o jsonpath='{.metadata.labels.karpenter\.sh/capacity-type}' 2>/dev/null || echo "")
            
            if [ "$capacity_type" = "spot" ]; then
                spot_count=$((spot_count + 1))
            elif [ "$capacity_type" = "on-demand" ]; then
                ondemand_count=$((ondemand_count + 1))
            else
                unscheduled_count=$((unscheduled_count + 1))
            fi
        fi
    done < <(kubectl get pods -n "$namespace" \
        -l "${WORKLOAD_ROLE_LABEL}=executor,${JOB_ID_LABEL}=${job_id}" \
        -o json 2>/dev/null | jq -c '.items[]')
    
    echo "$total $spot_count $ondemand_count $unscheduled_count"
}

# Verify spot ratio
verify_spot_ratio() {
    local job_id=$1
    local namespace=$2
    local expected_ratio=$3
    local tolerance=${4:-0.15}
    
    log_info "Verifying spot ratio for job $job_id (expected: $expected_ratio, tolerance: ±$tolerance)"
    
    read -r total spot_count ondemand_count unscheduled <<< $(get_executor_allocation "$job_id" "$namespace")
    
    log_info "Executor allocation: Total=$total, Spot=$spot_count, On-Demand=$ondemand_count, Unscheduled=$unscheduled"
    
    if [ "$total" -eq 0 ]; then
        log_error "No executors found"
        return 1
    fi
    
    if [ "$unscheduled" -gt 0 ]; then
        log_warning "$unscheduled executors have no capacity type label"
    fi
    
    local actual_ratio=$(echo "scale=4; $spot_count / $total" | bc)
    local diff=$(echo "scale=4; if ($actual_ratio > $expected_ratio) $actual_ratio - $expected_ratio else $expected_ratio - $actual_ratio" | bc)
    
    log_info "Actual spot ratio: $actual_ratio (diff: $diff)"
    
    if (( $(echo "$diff <= $tolerance" | bc -l) )); then
        log_success "Spot ratio is within tolerance"
        return 0
    else
        log_error "Spot ratio outside tolerance (diff: $diff > $tolerance)"
        return 1
    fi
}

# Check webhook health
check_webhook_health() {
    log_info "Checking webhook health..."
    
    local webhook_pod=$(kubectl get pods -n "$WEBHOOK_NAMESPACE" \
        -l app=spot-balancer-webhook \
        -o jsonpath='{.items[0].metadata.name}' 2>/dev/null)
    
    if [ -z "$webhook_pod" ]; then
        log_error "Webhook pod not found in namespace $WEBHOOK_NAMESPACE"
        return 1
    fi
    
    local phase=$(kubectl get pod "$webhook_pod" -n "$WEBHOOK_NAMESPACE" \
        -o jsonpath='{.status.phase}')
    
    if [ "$phase" != "Running" ]; then
        log_error "Webhook pod is not running (phase: $phase)"
        return 1
    fi
    
    log_success "Webhook is healthy"
    return 0
}

# Cleanup job resources
cleanup_job() {
    local job_id=$1
    local namespace=$2
    
    log_info "Cleaning up job $job_id..."
    
    kubectl delete pods -n "$namespace" \
        -l "${JOB_ID_LABEL}=${job_id}" \
        --grace-period=0 --force 2>/dev/null || true
    
    log_success "Cleanup complete"
}

# Generate unique job suffix
generate_job_suffix() {
    echo "$(date +%s)-$$"
}

# Cordon nodes to force new node creation (optional)
cordon_existing_nodes() {
    log_info "Cordoning existing nodes to force new node creation..."
    
    kubectl get nodes -o name 2>/dev/null | while IFS= read -r node; do
        if [ -n "$node" ]; then
            if kubectl cordon "$node" 2>/dev/null; then
                echo "$node"
            fi
        fi
    done
}

# Uncordon nodes
uncordon_nodes() {
    local nodes=("$@")
    
    if [ ${#nodes[@]} -eq 0 ]; then
        return
    fi
    
    log_info "Uncordoning nodes..."
    for node in "${nodes[@]}"; do
        kubectl uncordon "$node" 2>/dev/null || true
    done
    log_success "Uncordoned ${#nodes[@]} node(s)"
}
