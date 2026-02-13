# Spot Balancer for Spark on EKS with auto-mode
This repository contains an example of how to create a Kubernetes Mutating webhook to influence job placement using spark with EKS auto-mode.

## Overview
Spot Balancer ensures Spark workloads running on Kubernetes with Karpenter maintain a controlled mix of spot and on‑demand instances per job. This allows you to take advantage of spot savings while materially reducing job failure risk from spot terminations.


## How this works

When Spark runs on Kubernetes with Karpenter, instance placement is managed at the infrastructure level using NodePools, not at the job level. Even when multiple NodePools are configured to represent different spot and on-demand ratios, the scheduler does not guarantee that an individual Spark job will be spread across them.

As a result, a single Spark job can be scheduled entirely on spot nodes. If those spot instances are reclaimed, the entire job may fail at once.

Karpenter can detect pending spot terminations and provides a time window before node removal. This works well for lightweight, stateless services. Spark executors are stateful and can be expensive to relocate, and executor migration within this window might be not be possible for some workloads.

Spark is designed to tolerate some executor loss. Individual executor terminations are expected and often recoverable, especially when data is replicated across executors. However, Spark does not tolerate losing a large portion of executors simultaneously.

Spot usage is safe as long as failures are limited in scope. Spot Balancer enforces this constraint by preventing any single job from being overly exposed to spot capacity, making Spark a strong fit for controlled spot usage.

Spark jobs are submitted to the cluster with a label identifying it as an executor and with a spot ratio. The spot ratio is a value between 0.0-1.0 that sets the desired ratio between spot and on-demand instances for the job's executors.

This does so using a Kubernetes Mutating webhook to decide whether to route an executor to be run on a spot or on-demand instance based on the job's spot ratio. Karpenter is used internally to provision the hardware to run.

The spot balancer provides 2 modes: `STRICT` and `BEST_EFFORT`
- `STRICT` desired spot ratio will be honored, which can result in executors waiting for spot instances to become available.
- `BEST_EFFORT` spot instances will not be marked with a label, but implicitly picked up by Karpenter to run on spot, which results in no guarantee that an executor will run on spot, but will eagerly do so when spot instances are available.

Stores the ratio of on-demand versus spot instances on a job-basis, storing the information in Redis.

### Call Diagram with a 70% spot target

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                         Spark Job Submission                                │
│                                                                             │
│  1. Spark Driver Pod created with:                                          │
│     - Label: spark-role=driver                                              │
│     - Annotation: workload/spot-ratio=0.7 (70% spot target)                 │
│     - Label: emr-containers.amazonaws.com/job.id=<job-id>                   │
└──────────────────────────────────┬──────────────────────────────────────────┘
                                   │
                                   ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                    Spark Executor Pod Creation                              │
│                                                                             │
│  2. Executor Pod created with:                                              │
│     - Label: spark-role=executor                                            │
│     - Label: emr-containers.amazonaws.com/job.id=<job-id>                   │
│     - No nodeSelector (yet)                                                 │
└──────────────────────────────────┬──────────────────────────────────────────┘
                                   │
                                   ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│              Kubernetes API Server (Mutating Webhook)                       │
│                                                                             │
│  3. K8s intercepts Pod creation → calls Spot Balancer webhook               │
│     POST /mutate with AdmissionReview payload                               │
└──────────────────────────────────┬──────────────────────────────────────────┘
                                   │
                                   ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                      Spot Balancer Webhook Logic                            │
│                                                                             │
│  4. Webhook processes request:                                              │
│     a) Checks if pod is executor (spark-role=executor)                      │
│     b) Extracts job-id from labels                                          │
│     c) Queries Redis for cached spot ratio OR                               │
│        queries K8s API for driver pod annotation                            │
│     d) Reads current executor counts from Redis:                            │
│        - exec-count:<namespace>:<job-id> → (on-demand: 2, spot: 5)          │
│     e) Calculates next executor placement:                                  │
│        - Current ratio: 5/(2+5) = 0.71                                      │
│        - Target ratio: 0.7                                                  │
│        - Decision: place on on-demand (to balance toward target)            │
│     f) Updates Redis counter: (on-demand: 3, spot: 5)                       │
│     g) Returns JSONPatch to add nodeSelector                                │
└──────────────────────────────────┬──────────────────────────────────────────┘
                                   │
                                   ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                    Kubernetes API Server Response                           │
│                                                                             │
│  5. K8s applies patch to executor pod:                                      │
│     spec:                                                                   │
│       nodeSelector:                                                         │
│         spark-role: executor                                                │
│         karpenter.sh/capacity-type: on-demand                               │
└──────────────────────────────────┬──────────────────────────────────────────┘
                                   │
                                   ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                    Kubernetes Scheduler + Karpenter                         │
│                                                                             │
│  6. K8s Scheduler:                                                          │
│     - Finds nodes matching nodeSelector                                     │
│     - If no matching nodes exist, pod stays Pending                         │
│                                                                             │
│  7. Karpenter:                                                              │
│     - Detects pending pod with capacity-type=on-demand                      │
│     - Provisions new on-demand EC2 instance via NodePool                    │
│     - Registers node with K8s cluster                                       │
└──────────────────────────────────┬──────────────────────────────────────────┘
                                   │
                                   ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                         Executor Pod Running                                │
│                                                                             │
│  8. Pod scheduled on on-demand node                                         │
│     - Spark executor joins job                                              │
│     - Job maintains 70% spot / 30% on-demand distribution                   │
└─────────────────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────────────────┐
│                      Pod Deletion (Validating Webhook)                      │
│                                                                             │
│  9. When executor pod deleted:                                              │
│     - K8s calls POST /validate with DELETE operation                        │
│     - Webhook decrements Redis counter based on capacity-type               │
│     - Keeps state accurate for future placement decisions                   │
└─────────────────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────────────────┐
│                      Background Reconciliation                              │
│                                                                             │
│  10. Periodic reconcile worker (every 30 min):                              │
│      - Lists all executor pods across cluster                               │
│      - Recalculates actual on-demand/spot counts per job                    │
│      - Updates Redis to fix any drift from missed events                    │
└─────────────────────────────────────────────────────────────────────────────┘

Components:
- Redis: Stores per-job executor counts and cached spot ratios
- Spot Balancer: Flask webhook service with mutating + validating endpoints
- Kubernetes API: Invokes webhooks during pod admission control
- Karpenter: Provisions EC2 instances based on pod nodeSelector requirements
- Spark: Submits driver + executor pods with job labels and annotations
```

## Config (env)
- Behavior: `SPOT_PREFERENCE` (BEST_EFFORT|STRICT), `DEFAULT_SPOT_RATIO`, `WEBHOOK_TIMEOUT_SECONDS`
- Redis: `REDIS_URL` (required), `REDIS_DEFAULT_TTL_SECONDS`
- Keys: `CAPACITY_TYPE_LABEL`, `WORKLOAD_ROLE_LABEL`, `DRIVER_ROLE_VALUE`, `EXECUTOR_ROLE_VALUE`, `JOB_ID_LABEL`, `SPOT_RATIO_ANNOTATION`
- Misc: `RECONCILE_ENABLED`, `RECONCILE_INTERVAL_SECONDS`, `LOG_LEVEL`

## Prerequisites

1) EKS cluster with auto-mode enabled.
2) **EMR on EKS Auto Mode:** If you use EMR on EKS Auto Mode, its default Node Pool is on-demand only. To allow Spot Balancer to provision spot instances, configure a custom Node Pool that includes spot capacity.
3) App image pushed to a registry available to the cluster (for example, ECR). Use multi-platform builds if possible. For example:
```
docker buildx build --platform linux/amd64,linux/arm64 -t sample_spot_balancer_spark_eks:0.1 .
```

## Deploy
1) Install cert-manager to manage TLS certificate for Kubernetes Mutating Webhook:
```bash
kubectl apply -f https://github.com/cert-manager/cert-manager/releases/download/v1.15.3/cert-manager.crds.yaml
helm repo add jetstack https://charts.jetstack.io && helm repo update
helm install cert-manager jetstack/cert-manager -n cert-manager --create-namespace --version v1.15.3
```
2) Copy the file spot_balancer.template.yaml to spot_balancer.yaml and fill the following placeholders:
   - `<NAMESPACE>`: where the webhook runs (e.g., `spark-tools`)
   - `<IMAGE:TAG>`: container image for the webhook (e.g., your ECR tag)
   - `<REDIS_URL>`: Redis endpoint; `redis://localhost:6379/0` if using the sidecar
   - `<DEFAULT_SPOT_RATIO>`: fallback ratio when driver annotation is missing (e.g., `1` or `0.5`)
   - `<WORKLOAD_ROLE_LABEL>`: pod label key that marks role (default `spark-role`)
   - `<DRIVER_ROLE_VALUE>` / `<EXECUTOR_ROLE_VALUE>`: role values (default `driver` / `executor`)
   - `<SPOT_RATIO_ANNOTATION>`: annotation key on the driver that holds the ratio (e.g., `workload/spot-ratio`)
   - `<NODEPOOL_LABEL_VALUE>`: node label value for your on-demand pool (e.g., `tooling`). Example NodePool definitions for testing are in [./tests/e2e/node_pools.yaml](./tests/e2e/node_pools.yaml).
   - `<TARGET_NAMESPACES_COMMA_SEPARATED>`: list of workload namespaces to target (e.g., `sandbox`)
3) Apply:
```bash
kubectl apply -f ./deploy/spot_balancer.yaml
kubectl -n <NAMESPACE> rollout status deploy/spot-balancer-webhook
```

Notes
- Service + webhooks expect TLS Secret `spot-balancer-webhook-tls` issued by cert-manager (auto-mounted at `/tls`).
- If you don’t want cert-manager and want to bring your own certificate: create a `kubernetes.io/tls` Secret named `spot-balancer-webhook-tls` in `<NAMESPACE>` with `tls.crt` and `tls.key`, remove the Issuer/Certificate from the YAML, and set `clientConfig.caBundle` in the webhook configurations to the base64-encoded issuing CA (for self-signed: the cert itself).

### Verify deployment
```bash
kubectl -n <NAMESPACE> get deploy spot-balancer-webhook
kubectl -n <NAMESPACE> logs deploy/spot-balancer-webhook -c spot-balancer --tail=100
kubectl get mutatingwebhookconfiguration spot-balancer-webhook -o yaml | grep namespace:
```

### Tests

For full unit and e2e tests instructions, see `./tests/README.md`.
