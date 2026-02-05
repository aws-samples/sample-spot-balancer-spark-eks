import importlib
import os
from typing import Any

import pytest


def import_config() -> Any:
	spec = importlib.util.spec_from_file_location(
		"spot_balancer_config",
		"./config.py",
	)
	assert spec and spec.loader
	mod = importlib.util.module_from_spec(spec)
	loader = spec.loader  # type: ignore[assignment]
	assert isinstance(loader, importlib.abc.Loader)
	loader.exec_module(mod)  # type: ignore[arg-type]
	return mod


def test_defaults(monkeypatch: pytest.MonkeyPatch):
	# Clear env to ensure defaults are used
	for k in [
		"SPOT_PREFERENCE",
		"WEBHOOK_TIMEOUT_SECONDS",
		"DEFAULT_SPOT_RATIO",
		"CAPACITY_TYPE_LABEL",
		"WORKLOAD_ROLE_LABEL",
		"SPARK_ROLE_LABEL",
		"JOB_ID_LABEL",
		"SPOT_RATIO_ANNOTATION",
	]:
		monkeypatch.delenv(k, raising=False)

	conf = import_config()
	settings = conf.load()
	assert settings.spot_preference == "BEST_EFFORT"
	assert settings.webhook_timeout_seconds == 5
	assert settings.default_spot_ratio == 0.5
	assert settings.capacity_type_label == "karpenter.sh/capacity-type"
	assert settings.role_label == "spark-role"
	assert settings.job_id_label == "emr-containers.amazonaws.com/job.id"
	assert settings.spot_ratio_annotation == "workload/spot-ratio"


def test_env_overrides(monkeypatch: pytest.MonkeyPatch):
	monkeypatch.setenv("SPOT_PREFERENCE", "strict")  # lower-case acceptable
	monkeypatch.setenv("WEBHOOK_TIMEOUT_SECONDS", "7")
	monkeypatch.setenv("DEFAULT_SPOT_RATIO", "0.8")
	monkeypatch.setenv("CAPACITY_TYPE_LABEL", "cap")
	monkeypatch.setenv("WORKLOAD_ROLE_LABEL", "role")
	monkeypatch.setenv("JOB_ID_LABEL", "joblbl")
	monkeypatch.setenv("SPOT_RATIO_ANNOTATION", "ann")

	conf = import_config()
	settings = conf.load()
	assert settings.spot_preference == "STRICT"
	assert settings.webhook_timeout_seconds == 7
	assert settings.default_spot_ratio == 0.8
	assert settings.capacity_type_label == "cap"
	assert settings.role_label == "role"
	assert settings.job_id_label == "joblbl"
	assert settings.spot_ratio_annotation == "ann"


def test_invalid_values_fallback(monkeypatch: pytest.MonkeyPatch):
	monkeypatch.setenv("SPOT_PREFERENCE", "weird")
	monkeypatch.setenv("WEBHOOK_TIMEOUT_SECONDS", "not-int")
	monkeypatch.setenv("DEFAULT_SPOT_RATIO", "not-float")

	conf = import_config()
	settings = conf.load()
	assert settings.spot_preference == "BEST_EFFORT"
	assert settings.webhook_timeout_seconds == 5
	assert settings.default_spot_ratio == 0.5
