"""
Minimal models for Kubernetes AdmissionReview and Pod used by this webhook.
We intentionally parse only the fields we need and ignore unknowns so that
new Kubernetes fields don't break this app.

References:
- AdmissionReview request/response shape:
  https://kubernetes.io/docs/reference/access-authn-authz/extensible-admission-controllers/#request-and-response
- Pod (core/v1) API reference:
  https://kubernetes.io/docs/reference/generated/kubernetes-api/latest/#pod-v1-core
"""

from dataclasses import dataclass
from typing import Any, Optional


def _get(d: dict[str, Any], key: str, default):
    # Safe nested getter for dicts
    v = d.get(key)
    return v if isinstance(v, type(default)) else default


@dataclass
class PodModel:
    name: str
    namespace: str
    labels: dict[str, str]
    annotations: dict[str, str]
    node_selector: dict[str, str]

    @staticmethod
    def from_dict(d: dict[str, Any]) -> "PodModel":
        meta = _get(d, "metadata", {})
        spec = _get(d, "spec", {})
        name = _get(meta, "name", "")
        namespace = _get(meta, "namespace", "")
        labels = _get(meta, "labels", {})
        annotations = _get(meta, "annotations", {})
        node_selector = _get(spec, "nodeSelector", {})
        return PodModel(
            name=name or "",
            namespace=namespace or "",
            labels=labels or {},
            annotations=annotations or {},
            node_selector=node_selector or {},
        )


@dataclass
class AdmissionRequestModel:
    uid: str
    obj: PodModel
    old_obj: PodModel | None
    operation: str = "CREATE"

    @staticmethod
    def from_dict(d: dict[str, Any]) -> Optional["AdmissionRequestModel"]:
        if not isinstance(d, dict):
            return None
        uid = str(d.get("uid", ""))
        op = str(d.get("operation", "CREATE"))
        obj_raw = d.get("object", {})
        if not isinstance(obj_raw, dict):
            obj_raw = {}
        old_raw = d.get("oldObject", {})
        if not isinstance(old_raw, dict):
            old_raw = {}
        return AdmissionRequestModel(
            uid=uid,
            obj=PodModel.from_dict(obj_raw),
            old_obj=PodModel.from_dict(old_raw) if old_raw else None,
            operation=op,
        )


@dataclass
class AdmissionReviewModel:
    request: AdmissionRequestModel

    @staticmethod
    def from_dict(d: dict[str, Any]) -> Optional["AdmissionReviewModel"]:
        if not isinstance(d, dict):
            return None
        req_raw = d.get("request")
        req = (
            AdmissionRequestModel.from_dict(req_raw)
            if isinstance(req_raw, dict)
            else None
        )
        if req is None:
            return None
        return AdmissionReviewModel(request=req)


def extract_job_id(pod: PodModel, job_id_label: str) -> str | None:
    return pod.labels.get(job_id_label)


def extract_role(pod: PodModel, role_label: str) -> str | None:
    return pod.labels.get(role_label)


def extract_capacity_type_from_pod(
    pod: PodModel, capacity_type_label: str
) -> str | None:
    return pod.node_selector.get(capacity_type_label)
