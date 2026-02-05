# End-to-End Tests for Spot Balancer

This directory contains end-to-end tests for the Spot Balancer webhook running on a live Kubernetes cluster with EKS Auto Mode.

## Prerequisites

1. **EKS cluster with Auto Mode enabled**:
   - EKS Auto Mode cluster (see [EKS Auto Mode docs](https://docs.aws.amazon.com/eks/latest/userguide/automode.html))
   
2. **Tools installed**:
   - `kubectl` configured to access your EKS cluster
   - `jq` for JSON parsing
   - `aws` CLI (optional, for cluster management)
   - Spot balancer tool deployed with STRICT value for SPOT_PREFERENCE

## Deploy Custom NodePools 

Deploy the Spark executor and tooling node pools:

```bash

# Deploy NodePool
kubectl apply -f ./node_pools.yaml

```

The tests are configured to use this NodePool via the `spark-role: executor` nodeSelector.

## EKS Auto Mode Configuration

EKS Auto Mode automatically manages compute resources using NodePools. The webhook uses the standard `karpenter.sh/capacity-type` label to control spot vs on-demand placement:
- `karpenter.sh/capacity-type: spot` - Routes pods to spot instances
- `karpenter.sh/capacity-type: on-demand` - Routes pods to on-demand instances

EKS Auto Mode will automatically provision the appropriate EC2 instances based on these labels.

## Quick Start

```bash
# Verify EKS Auto Mode is enabled
kubectl get nodepools

# Set your test namespace. This needs to match the webhook selector in the spot balancer templace
export TEST_NAMESPACE=sandbox

# Set webhook namespace
export WEBHOOK_NAMESPACE=spark-tools

# Optional: Force new node creation (cordons existing nodes during test)
export FORCE_NEW_NODES=false

# Run the full e2e test suite
./run_e2e.sh

# Or run individual tests
./test_spark_spot_allocation.sh
./test_spark_ondemand_allocation.sh
./test_spark_mixed_allocation.sh
```

## Test Scenarios. 

### 1. Full Spot Allocation (spot-ratio=1.0)
Tests that a Spark job with 100% spot ratio places all executors on spot instances.

### 2. Full On-Demand Allocation (spot-ratio=0.0)
Tests that a Spark job with 0% spot ratio places all executors on on-demand instances.

### 3. Mixed Allocation (spot-ratio=0.7)
Tests that a Spark job with 70% spot ratio maintains approximately 70/30 spot/on-demand distribution.

