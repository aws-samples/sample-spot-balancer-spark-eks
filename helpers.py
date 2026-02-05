import base64
import json
import logging
import re
from typing import Any

from .store.interface import KeyValueStore

log = logging.getLogger("spot-balancer")


def validate_spot_ratio(ratio: float | str | None) -> tuple[bool, str | None]:
    """
    Validate spot ratio is a number between 0.0 and 1.0 inclusive.
    Returns (is_valid, error_message).
    """
    if ratio is None:
        return False, "Spot ratio cannot be None"
    
    try:
        ratio_float = float(ratio)
    except (ValueError, TypeError):
        return False, f"Spot ratio must be a valid number, got: {ratio}"
    
    if ratio_float < 0.0 or ratio_float > 1.0:
        return False, f"Spot ratio must be between 0.0 and 1.0, got: {ratio_float}"
    
    return True, None


def validate_kubernetes_name(name: str | None, field_name: str = "name") -> tuple[bool, str | None]:
    """
    Validate Kubernetes resource name (namespace, job_id, etc.) as a UTF-8 string:
    - Must be 1-255 characters
    - Can contain any valid UTF-8 characters
    Returns (is_valid, error_message).
    """
    if name is None or name == "":
        return False, f"{field_name} cannot be empty or None"
    
    if not isinstance(name, str):
        return False, f"{field_name} must be a string, got: {type(name).__name__}"
    
    if len(name) > 255:
        return False, f"{field_name} must be 255 characters or less, got: {len(name)}"
    
    # Validate UTF-8 encoding
    try:
        name.encode('utf-8')
    except UnicodeEncodeError:
        return False, f"{field_name} must be a valid UTF-8 string"
    
    return True, None


def make_admission_response(
    uid: str, allowed: bool = True, patch: list[dict[str, Any]] | None = None
) -> dict[str, Any]:
    """Return the AdmissionReview the webhook sends to K8s to allow and optionally patch a Pod."""
    resp = {"uid": uid, "allowed": allowed}

    if patch:
        resp["patchType"] = "JSONPatch"
        resp["patch"] = base64.b64encode(json.dumps(patch).encode()).decode()

    return {
        "apiVersion": "admission.k8s.io/v1",
        "kind": "AdmissionReview",
        "response": resp,
    }


def spot_ratio_from_driver(
    core: Any, settings: Any, ns: str, job_id: str
) -> float | None:
    """Retrieve the job's target spot ratio (policy) from the driver annotation to guide executor balancing."""
    # Validate namespace and job_id
    is_valid_ns, ns_error = validate_kubernetes_name(ns, "namespace")
    if not is_valid_ns:
        log.error("Invalid namespace: %s", ns_error)
        return None
    
    is_valid_job, job_error = validate_kubernetes_name(job_id, "job_id")
    if not is_valid_job:
        log.error("Invalid job_id: %s", job_error)
        return None
    
    log.info("Looking for driver pod with job_id=%s in namespace=%s", job_id, ns)

    try:
        pods = core.list_namespaced_pod(
            ns,
            label_selector=f"{settings.job_id_label}={job_id},{settings.role_label}={settings.driver_role_value}",
            _request_timeout=getattr(settings, "webhook_timeout_seconds", 5),
        ).items

        log.info("Found %d driver pods for job %s", len(pods), job_id)

        if not pods:
            log.warning("No driver pods found for job=%s in namespace=%s", job_id, ns)
            return None

        driver_pod = pods[0]
        annotations = driver_pod.metadata.annotations or {}

        default_spot_ratio = str(settings.default_spot_ratio)
        spot_ratio_annotation = annotations.get(
            settings.spot_ratio_annotation, default_spot_ratio
        )

        # Validate the spot ratio
        is_valid_ratio, ratio_error = validate_spot_ratio(spot_ratio_annotation)
        if not is_valid_ratio:
            log.error("Invalid spot ratio from driver annotation: %s", ratio_error)
            return None

        return float(spot_ratio_annotation)

    except Exception as e:
        log.error("Error getting spot ratio from driver: %s", str(e))
        return None


def spot_ratio_from_executor(pod: Any, settings: Any) -> float:
    """Support workloads that set the ratio on executors: read annotation or fall back to default."""
    try:
        raw = (pod.annotations or {}).get(settings.spot_ratio_annotation)
        if raw is None or raw == "":
            return float(settings.default_spot_ratio)

        # Validate the spot ratio
        is_valid, error = validate_spot_ratio(raw)
        if not is_valid:
            log.warning("Invalid spot ratio from executor annotation: %s. Using default.", error)
            return float(settings.default_spot_ratio)

        return float(raw)

    except Exception:
        return float(settings.default_spot_ratio)


def count_executors(
    datastore: KeyValueStore | None, ns: str, job_id: str
) -> tuple[int, int]:
    """Read per-job executor counts from Redis used to decide the next executor's capacity."""
    # Validate namespace and job_id
    is_valid_ns, ns_error = validate_kubernetes_name(ns, "namespace")
    if not is_valid_ns:
        log.error("Invalid namespace in count_executors: %s", ns_error)
        return (0, 0)
    
    is_valid_job, job_error = validate_kubernetes_name(job_id, "job_id")
    if not is_valid_job:
        log.error("Invalid job_id in count_executors: %s", job_error)
        return (0, 0)
    
    log.info("Reading executor counters from Redis for job_id=%s ns=%s", job_id, ns)
    if datastore is None:
        return (0, 0)
    key = f"exec-count:{ns}:{job_id}"
    cached = datastore.get(key)
    if cached is not None:
        return cached[1]
    # If missing, return 0s; periodic reconcile will correct drift
    return (0, 0)


def choose_bucket(od: int, spot: int, spot_ratio: float) -> str | None:
    """Choose the next executor's capacity (spot vs on-demand) to steer toward the job's target ratio."""
    # Validate spot ratio
    is_valid, error = validate_spot_ratio(spot_ratio)
    if not is_valid:
        log.error("Invalid spot ratio in choose_bucket: %s", error)
        return None

    if spot_ratio <= 0.0:
        return "on-demand"
    elif spot_ratio >= 1.0:
        return "spot"

    total = od + spot
    if total == 0:
        return "spot"
    
    current_spot_frac = spot / total
    log.info("Spot / total fraction = %f", current_spot_frac)
    return "spot" if current_spot_frac <= spot_ratio else "on-demand"


def patch_selector(settings: Any, capacity: str) -> list[dict[str, Any]]:
    """Produce a JSONPatch to force scheduler node selection for the chosen capacity and role."""
    return [
        {
            "op": "add",
            "path": "/spec/nodeSelector",
            "value": {
                settings.role_label: settings.executor_role_value,
                settings.capacity_type_label: capacity,
            },
        }
    ]


def reconcile_all(core: Any, settings: Any, datastore: KeyValueStore | None) -> None:
    """Repair Redis counters from live cluster state so decisions remain correct after restarts or misses."""
    if datastore is None:
        log.warning("Datastore unavailable; skipping reconcile")
        return

    log.info("Starting reconcile of Redis state from K8s")

    try:
        pods = core.list_pod_for_all_namespaces(
            label_selector=f"{settings.role_label}={settings.executor_role_value},{settings.job_id_label}",
            _request_timeout=getattr(settings, "webhook_timeout_seconds", 5),
        ).items

        by_job = {}

        for pod in pods:
            if pod.metadata.deletion_timestamp:
                continue

            ns = pod.metadata.namespace
            labels = pod.metadata.labels or {}
            job_id = labels.get(settings.job_id_label)

            if not job_id:
                continue

            capacity = (pod.spec.node_selector or {}).get(settings.capacity_type_label)
            key = (ns, job_id)
            od, spot = by_job.get(key, (0, 0))

            if capacity == "on-demand":
                od += 1
            elif capacity == "spot":
                spot += 1

            by_job[key] = (od, spot)

        for (ns, job_id), counts in by_job.items():
            key = f"exec-count:{ns}:{job_id}"
            datastore.set(key, counts)

        log.info("Reconciled %d job executor counters", len(by_job))

    except Exception as e:
        log.error("Reconcile error: %s", e)


def adjust_executor_count(
    datastore: KeyValueStore | None,
    ns: str,
    job_id: str,
    delta_od: int = 0,
    delta_spot: int = 0,
    ttl_seconds: int | None = None,
) -> None:
    """Update Redis counters to reflect decisions/deletions so the running distribution stays accurate."""
    if datastore is None:
        return

    # Validate namespace and job_id
    is_valid_ns, ns_error = validate_kubernetes_name(ns, "namespace")
    if not is_valid_ns:
        log.error("Invalid namespace in adjust_executor_count: %s", ns_error)
        return
    
    is_valid_job, job_error = validate_kubernetes_name(job_id, "job_id")
    if not is_valid_job:
        log.error("Invalid job_id in adjust_executor_count: %s", job_error)
        return

    key = f"exec-count:{ns}:{job_id}"
    current = datastore.get(key)

    od, spot = (0, 0) if current is None else tuple(current[1])
    od = max(0, od + delta_od)
    spot = max(0, spot + delta_spot)

    datastore.set(
        key, (od, spot), ttl_seconds=ttl_seconds if ttl_seconds is not None else 0
    )
