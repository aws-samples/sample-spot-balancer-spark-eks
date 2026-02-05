# Tests

Run unit tests locally without a cluster, and end to end tests with a cluster.

## Unit tests (no Kubernetes)

Test core webhook logic (ratio, patching behavior, request handling).
### Prerequisites
1. Rename the root directory from sample-spot-balancer-spark-eks to sample_spot_balancer_spark_eks

```bash
mv sample-spot-balancer-spark-eks sample_spot_balancer_spark_eks
```

Command (from repo root):
```bash
APP_ENV=test PYTHONPATH=. pytest -q ./tests
```

## End-to-End Tests 

The e2e directory contains end-to-end tests for the Spot Balancer webhook

### Prerequisites

1. **EKS cluster with Auto Mode enabled**:
   - EKS Auto Mode cluster (see [EKS Auto Mode docs](https://docs.aws.amazon.com/eks/latest/userguide/automode.html))
   
2. **Tools installed**:
   - `kubectl` configured to access your EKS cluster
   - `jq` for JSON parsing
   - `aws` CLI (optional, for cluster management)
3. **Spot balancer tool deployed with STRICT value for SPOT_PREFERENCE**

### Deploy Custom NodePools 

Deploy the Spark executor and tooling node pools:

```bash

# Deploy NodePool
kubectl apply -f ./e2e/node_pools.yaml

```

The tests are configured to use this NodePool via the `spark-role: executor` nodeSelector.


### Execute tests

```bash
# Verify EKS Auto Mode is enabled
kubectl get nodepools

# Set your test namespace. This needs to match the webhook selector in the spot balancer templace
export TEST_NAMESPACE=sandbox

# Set webhook namespace
export WEBHOOK_NAMESPACE=spark-tools

# Optional: Force new node creation (cordons existing nodes during test)
export FORCE_NEW_NODES=false

cd ./e2e

# Run the full e2e test suite
./run_e2e.sh

# Or run individual tests
./test_spark_spot_allocation.sh
./test_spark_ondemand_allocation.sh
./test_spark_mixed_allocation.sh
```

### Test Scenarios. 

### 1. Full Spot Allocation (spot-ratio=1.0)
Tests that a Spark job with 100% spot ratio places all executors on spot instances.

### 2. Full On-Demand Allocation (spot-ratio=0.0)
Tests that a Spark job with 0% spot ratio places all executors on on-demand instances.

### 3. Mixed Allocation (spot-ratio=0.7)
Tests that a Spark job with 70% spot ratio maintains approximately 70/30 spot/on-demand distribution.
