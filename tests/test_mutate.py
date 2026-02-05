import base64
import importlib
import sys
import json
import os
from typing import Any, Dict

import pytest


def import_app(monkeypatch: pytest.MonkeyPatch) -> Any:
	# Prevent real in-cluster config
	monkeypatch.setenv("APP_ENV", "test")
	monkeypatch.setenv("LOG_LEVEL", "WARNING")
	# Stub kubernetes module if missing
	if "kubernetes" not in sys.modules:
		k8s_stub = type("k8s", (), {})()
		config_stub = type("cfg", (), {"load_incluster_config": lambda: None})()
		client_stub = type("cli", (), {"CoreV1Api": lambda: object()})()
		k8s_stub.config = config_stub
		k8s_stub.client = client_stub
		sys.modules["kubernetes"] = k8s_stub
		sys.modules["kubernetes.config"] = config_stub  # type: ignore
		sys.modules["kubernetes.client"] = client_stub  # type: ignore
	try:
		import kubernetes.config as k8s_config  # type: ignore
		monkeypatch.setattr(k8s_config, "load_incluster_config", lambda: None, raising=False)
	except Exception:
		pass

	# Import as package so relative imports work
	return importlib.import_module("sample_spot_balancer_spark_eks.app")


def admission_review(uid: str, pod: Dict[str, Any]) -> Dict[str, Any]:
	return {
		"apiVersion": "admission.k8s.io/v1",
		"kind": "AdmissionReview",
		"request": {
			"uid": uid,
			"object": pod,
		},
	}


def minimal_pod(name="p1", ns="default", labels=None, annotations=None) -> Dict[str, Any]:
	return {
		"metadata": {
			"name": name,
			"namespace": ns,
			"labels": labels or {},
			"annotations": annotations or {},
		},
	}


def decode_patch(resp_json: Dict[str, Any]):
	patch_b64 = resp_json["response"].get("patch")
	if not patch_b64:
		return None
	raw = base64.b64decode(patch_b64).decode()
	return json.loads(raw)


def test_mutate_non_executor_allows(monkeypatch: pytest.MonkeyPatch):
	app = import_app(monkeypatch)
	client = app.app.test_client()

	pod = minimal_pod(labels={"spark-role": "driver"})
	req = admission_review("u1", pod)
	r = client.post("/mutate", json=req)
	assert r.status_code == 200
	body = r.get_json()
	assert body["response"]["allowed"] is True
	assert "patch" not in body["response"]


def test_mutate_executor_no_job_id_allows(monkeypatch: pytest.MonkeyPatch):
	app = import_app(monkeypatch)
	client = app.app.test_client()

	pod = minimal_pod(labels={"spark-role": "executor"})
	req = admission_review("u2", pod)
	r = client.post("/mutate", json=req)
	assert r.status_code == 200
	body = r.get_json()
	assert body["response"]["allowed"] is True
	assert "patch" not in body["response"]


def test_mutate_executor_no_driver_ratio_allows(monkeypatch: pytest.MonkeyPatch):
	app = import_app(monkeypatch)
	client = app.app.test_client()

	# Provide a job id
	labels = {
		"spark-role": "executor",
		"emr-containers.amazonaws.com/job.id": "job_1",
	}
	pod = minimal_pod(labels=labels)
	req = admission_review("u3", pod)

	# Force ratio lookup to None on driver
	import sys as _sys
	routes_mod = _sys.modules.get("routes")
	monkeypatch.setattr(routes_mod, "spot_ratio_from_driver", lambda core, settings, ns, job: None, raising=False)

	r = client.post("/mutate", json=req)
	assert r.status_code == 200
	body = r.get_json()
	assert body["response"]["allowed"] is True
	assert "patch" not in body["response"]


@pytest.mark.parametrize(
	"bucket,expect_patch",
	[
		(None, False),
		("spot", False),
		("on-demand", True),
	],
)
def test_mutate_executor_decision_paths(monkeypatch: pytest.MonkeyPatch, bucket, expect_patch):
	app = import_app(monkeypatch)
	client = app.app.test_client()

	labels = {
		"spark-role": "executor",
		"emr-containers.amazonaws.com/job.id": "job-2",
	}
	pod = minimal_pod(labels=labels, name="exec-1")
	req = admission_review("u4", pod)

	# Patch in routes module namespace (used by Flask handlers)
	import sys as _sys
	routes_mod = _sys.modules.get("routes")
	monkeypatch.setattr(routes_mod, "spot_ratio_from_driver", lambda core, settings, ns, job: 0.5, raising=False)
	monkeypatch.setattr(routes_mod, "count_executors", lambda ds, ns, job: (1, 1), raising=False)
	monkeypatch.setattr(routes_mod, "choose_bucket", lambda od, sp, ratio: bucket, raising=False)

	r = client.post("/mutate", json=req)
	assert r.status_code == 200
	body = r.get_json()
	assert body["response"]["allowed"] is True
	patch = decode_patch(body)
	if expect_patch:
		assert isinstance(patch, list)
		selector = patch[0]["value"]
		assert selector["karpenter.sh/capacity-type"] == "on-demand"
	else:
		assert patch is None


def test_mutate_invalid_payload_400(monkeypatch: pytest.MonkeyPatch):
	app = import_app(monkeypatch)
	client = app.app.test_client()

	r = client.post("/mutate", json={"not": "admission-review"})
	assert r.status_code == 400
	body = r.get_json()
	assert body["response"]["allowed"] is False


def test_health_ok(monkeypatch: pytest.MonkeyPatch):
	app = import_app(monkeypatch)
	client = app.app.test_client()
	r = client.get("/health")
	assert r.status_code == 200
	assert r.get_json()["status"] == "healthy"

def _validate_review(uid: str, pod: Dict[str, Any]) -> Dict[str, Any]:
	return {
		"apiVersion": "admission.k8s.io/v1",
		"kind": "AdmissionReview",
		"request": {
			"uid": uid,
			"operation": "DELETE",
			"object": pod,
		},
	}


def test_validate_delete_decrements_on_demand(monkeypatch: pytest.MonkeyPatch):
	app = import_app(monkeypatch)
	client = app.app.test_client()
	pod = minimal_pod(
		name="p-del",
		labels={"spark-role": "executor", "emr-containers.amazonaws.com/job.id": "job-X"},
		annotations={},
	)
	# Simulate nodeSelector capacity-type on pod
	pod["spec"] = {"nodeSelector": {"karpenter.sh/capacity-type": "on-demand"}}
	req = _validate_review("ud1", pod)
	# capture decrements
	calls = {"od": 0, "spot": 0}
	import sys as _sys
	routes_mod = _sys.modules.get("routes")
	def _adj(ds, ns, job, delta_od=0, delta_spot=0, ttl_seconds=None):
		if delta_od:
			calls["od"] += delta_od
		if delta_spot:
			calls["spot"] += delta_spot
	monkeypatch.setattr(routes_mod, "adjust_executor_count", _adj, raising=False)
	r = client.post("/validate", json=req)
	assert r.status_code == 200
	body = r.get_json()
	assert body["response"]["allowed"] is True
	assert calls["od"] < 0


def test_validate_delete_decrements_spot(monkeypatch: pytest.MonkeyPatch):
	app = import_app(monkeypatch)
	client = app.app.test_client()
	pod = minimal_pod(
		name="p-del2",
		labels={"spark-role": "executor", "emr-containers.amazonaws.com/job.id": "job_Y"},
		annotations={},
	)
	pod["spec"] = {"nodeSelector": {"karpenter.sh/capacity-type": "spot"}}
	req = _validate_review("ud2", pod)
	calls = {"od": 0, "spot": 0}
	import sys as _sys
	routes_mod = _sys.modules.get("routes")
	def _adj(ds, ns, job, delta_od=0, delta_spot=0, ttl_seconds=None):
		if delta_od:
			calls["od"] += delta_od
		if delta_spot:
			calls["spot"] += delta_spot
	monkeypatch.setattr(routes_mod, "adjust_executor_count", _adj, raising=False)
	r = client.post("/validate", json=req)
	assert r.status_code == 200
	body = r.get_json()
	assert body["response"]["allowed"] is True
	assert calls["spot"] < 0
