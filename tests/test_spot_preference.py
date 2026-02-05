import base64
import importlib
import sys
from typing import Any, Dict

import pytest

def import_app(monkeypatch: pytest.MonkeyPatch) -> Any:
	# test env + stub k8s
	monkeypatch.setenv("APP_ENV", "test")
	monkeypatch.setenv("LOG_LEVEL", "WARNING")
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
	return importlib.import_module(f"sample_spot_balancer_spark_eks.app")


def _mk_req():
	pod = {
		"metadata": {
			"name": "p",
			"namespace": "default",
			"labels": {
				"spark-role": "executor",
				"emr-containers.amazonaws.com/job.id": "job-pref",
			},
			"annotations": {},
		},
	}
	req = {
		"apiVersion": "admission.k8s.io/v1",
		"kind": "AdmissionReview",
		"request": {
			"uid": "u-pref",
			"object": pod,
		},
	}
	return req


def _decode_patch(resp_json: Dict[str, Any]):
	patch_b64 = resp_json["response"].get("patch")
	if not patch_b64:
		return None
	import json as _json
	raw = base64.b64decode(patch_b64).decode()
	return _json.loads(raw)


def test_best_effort_semantics(monkeypatch: pytest.MonkeyPatch):
	# BEST_EFFORT by default
	app = import_app(monkeypatch)
	client = app.app.test_client()
	req = _mk_req()
	# Patch in routes module namespace
	routes_mod = sys.modules.get("routes")
	monkeypatch.setattr(routes_mod, "spot_ratio_from_driver", lambda core, settings, ns, job: 0.5, raising=False)
	monkeypatch.setattr(routes_mod, "count_executors", lambda ds, ns, job: (2, 1), raising=False)
	# spot -> no patch
	monkeypatch.setattr(routes_mod, "choose_bucket", lambda od, sp, ratio: "spot", raising=False)
	r1 = client.post("/mutate", json=req)
	assert r1.status_code == 200
	assert "patch" not in r1.get_json()["response"]
	# None -> no patch
	monkeypatch.setattr(routes_mod, "choose_bucket", lambda od, sp, ratio: None, raising=False)
	r2 = client.post("/mutate", json=req)
	assert r2.status_code == 200
	assert "patch" not in r2.get_json()["response"]
	# on-demand -> patch
	monkeypatch.setattr(routes_mod, "choose_bucket", lambda od, sp, ratio: "on-demand", raising=False)
	r3 = client.post("/mutate", json=req)
	assert r3.status_code == 200
	assert "patch" in r3.get_json()["response"]


def test_strict_semantics(monkeypatch: pytest.MonkeyPatch):
	# STRICT: patch to spot when chosen; if None (no spot now), still patch spot and wait
	monkeypatch.setenv("SPOT_PREFERENCE", "STRICT")
	# Ensure fresh settings reflecting env by reloading modules
	sys.modules.pop("sample_spot_balancer_spark_eks.config", None)
	sys.modules.pop("sample_spot_balancer_spark_eks.app", None)
	app = import_app(monkeypatch)
	client = app.app.test_client()
	req = _mk_req()
	routes_mod = sys.modules.get("routes")
	monkeypatch.setattr(routes_mod, "spot_ratio_from_driver", lambda core, settings, ns, job: 0.8, raising=False)
	monkeypatch.setattr(routes_mod, "count_executors", lambda ds, ns, job: (0, 0), raising=False)
	# bucket = spot -> expect spot patch
	monkeypatch.setattr(routes_mod, "choose_bucket", lambda od, sp, ratio: "spot", raising=False)
	r1 = client.post("/mutate", json=req)
	body1 = r1.get_json()
	p1 = _decode_patch(body1)
	assert isinstance(p1, list)
	assert p1[0]["value"]["karpenter.sh/capacity-type"] == "spot"
	# bucket = None -> still patch spot (wait for capacity)
	monkeypatch.setattr(routes_mod, "choose_bucket", lambda od, sp, ratio: None, raising=False)
	r2 = client.post("/mutate", json=req)
	body2 = r2.get_json()
	p2 = _decode_patch(body2)
	assert isinstance(p2, list)
	assert p2[0]["value"]["karpenter.sh/capacity-type"] == "spot"
