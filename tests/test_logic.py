import importlib
import types
from typing import Any
import importlib.util

import pytest



def import_app(monkeypatch: pytest.MonkeyPatch) -> Any:
	"""
	Import app.py with in-cluster config disabled. Returns the loaded module.
	"""
	# Stub kubernetes incluster config before import
	monkeypatch.setenv("APP_ENV", "test")
	monkeypatch.setenv("LOG_LEVEL", "WARNING")
	try:
		import kubernetes.config as k8s_config  # type: ignore
		monkeypatch.setattr(k8s_config, "load_incluster_config", lambda: None, raising=False)
	except Exception:
		pass

	return importlib.import_module("sample_spot_balancer_spark_eks.app")


def import_helpers():
	return importlib.import_module("sample_spot_balancer_spark_eks.helpers")


def import_models():
	return importlib.import_module("sample_spot_balancer_spark_eks.models")


@pytest.mark.parametrize(
	"od,spot,ratio,expected",
	[
		# First executor: prefer spot
		(0, 0, 0.5, "spot"),
		# Maintain target ratio
		(1, 0, 0.5, "spot"),
		(0, 1, 0.5, "on-demand"),
		# Prefer spot but already above ratio -> on-demand
		(0, 1, 0.9, "on-demand"),
	],
)
def test_choose_bucket(monkeypatch: pytest.MonkeyPatch, od, spot, ratio, expected):
	app = import_app(monkeypatch)
	helpers = import_helpers()
	assert helpers.choose_bucket(od, spot, ratio) == expected


def test_patch_selector_builds_json_patch(monkeypatch: pytest.MonkeyPatch):
	app = import_app(monkeypatch)
	helpers = import_helpers()
	patch = helpers.patch_selector(app.settings, "on-demand")
	assert isinstance(patch, list)
	assert patch[0]["op"] == "add"
	assert patch[0]["path"] == "/spec/nodeSelector"
	selector = patch[0]["value"]
	assert selector["spark-role"] == "executor"
	assert selector["karpenter.sh/capacity-type"] == "on-demand"


def test_spark_job_id(monkeypatch: pytest.MonkeyPatch):
	app = import_app(monkeypatch)
	models = import_models()
	pod = {
		"metadata": {
			"labels": {"emr-containers.amazonaws.com/job.id": "job-123"},
		}
	}
	pm = models.PodModel.from_dict(pod)
	assert models.extract_job_id(pm, app.settings.job_id_label) == "job-123"


def test_spot_ratio_from_driver_with_annotation_and_default(monkeypatch: pytest.MonkeyPatch):
	app = import_app(monkeypatch)
	helpers = import_helpers()

	class DummyPodMeta:
		def __init__(self, annotations):
			self.annotations = annotations
			self.name = "driver-1"

	class DummyPod:
		def __init__(self, annotations):
			self.metadata = DummyPodMeta(annotations)

	class DummyCore:
		def __init__(self, items):
			self._items = items

		class Resp:
			def __init__(self, items):
				self.items = items

		def list_namespaced_pod(self, ns, label_selector=None, **kwargs):
			return DummyCore.Resp(self._items)

	# Case 1: explicit annotation
	app.core = DummyCore([DummyPod({"workload/spot-ratio": "0.8"})])
	assert helpers.spot_ratio_from_driver(app.core, app.settings, "ns", "job") == 0.8

	# Case 2: missing annotation -> default from env or 0.5
	app.core = DummyCore([DummyPod({})])
	# Ensure default env not set to influence behavior
	monkeypatch.delenv("DEFAULT_SPOT_RATIO", raising=False)
	assert helpers.spot_ratio_from_driver(app.core, app.settings, "ns", "job") == 0.5


def test_count_executors_reads_redis(monkeypatch: pytest.MonkeyPatch):
	app = import_app(monkeypatch)
	helpers = import_helpers()
	class DummyDS:
		def get(self, key: str):
			import time
			return (time.time() + 60, (3, 4))
	ds = DummyDS()
	od, spot = helpers.count_executors(ds, "ns", "job")
	assert od == 3 and spot == 4


