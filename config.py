import os
from dataclasses import dataclass
from typing import Literal

SpotPreference = Literal["BEST_EFFORT", "STRICT"]


def _get_env(name: str, default: str) -> str:
    val = os.getenv(name)
    return val if val is not None and val != "" else default


def _parse_int(name: str, default: int) -> int:
    val = _get_env(name, str(default))
    try:
        return int(val)
    except Exception:
        return default


def _parse_float(name: str, default: float) -> float:
    val = _get_env(name, str(default))
    try:
        return float(val)
    except Exception:
        return default


def _parse_spot_pref(
    name: str, default: SpotPreference = "BEST_EFFORT"
) -> SpotPreference:
    val = _get_env(name, default)
    val = val.upper()
    return val if val in ("BEST_EFFORT", "STRICT") else default


@dataclass(frozen=True)
class Settings:
    # Behavior
    spot_preference: SpotPreference = "BEST_EFFORT"
    webhook_timeout_seconds: int = 5
    default_spot_ratio: float = 0.5
    redis_url: str = ""
    redis_default_ttl_seconds: int = 86400
    app_env: str = "production"
    reconcile_enabled: bool = True
    reconcile_interval_seconds: int = 1800

    # Keys (labels/annotations)
    capacity_type_label: str = "karpenter.sh/capacity-type"
    role_label: str = (
        "spark-role"  # generic; default remains spark-role for compatibility
    )
    job_id_label: str = "emr-containers.amazonaws.com/job.id"
    spot_ratio_annotation: str = "workload/spot-ratio"
    # Role values
    driver_role_value: str = "driver"
    executor_role_value: str = "executor"


def load() -> Settings:
    # Allow both WORKLOAD_ROLE_LABEL and legacy SPARK_ROLE_LABEL
    role_label_env = _get_env("WORKLOAD_ROLE_LABEL", "")
    if not role_label_env:
        role_label_env = _get_env("SPARK_ROLE_LABEL", "spark-role")
    return Settings(
        spot_preference=_parse_spot_pref("SPOT_PREFERENCE", "BEST_EFFORT"),
        webhook_timeout_seconds=_parse_int("WEBHOOK_TIMEOUT_SECONDS", 5),
        default_spot_ratio=_parse_float("DEFAULT_SPOT_RATIO", 0.5),
        redis_url=_get_env("REDIS_URL", ""),
        redis_default_ttl_seconds=_parse_int("REDIS_DEFAULT_TTL_SECONDS", 86400),
        app_env=_get_env("APP_ENV", "production"),
        reconcile_enabled=_get_env("RECONCILE_ENABLED", "true").lower()
        in ("1", "true", "yes"),
        reconcile_interval_seconds=_parse_int("RECONCILE_INTERVAL_SECONDS", 1800),
        
		capacity_type_label=_get_env(
            "CAPACITY_TYPE_LABEL", "karpenter.sh/capacity-type"
        ),
        role_label=role_label_env,
        job_id_label=_get_env("JOB_ID_LABEL", "emr-containers.amazonaws.com/job.id"),
        spot_ratio_annotation=_get_env("SPOT_RATIO_ANNOTATION", "workload/spot-ratio"),

        driver_role_value=_get_env("DRIVER_ROLE_VALUE", "driver"),
        executor_role_value=_get_env("EXECUTOR_ROLE_VALUE", "executor"),
    )


# Singleton settings for app usage (optional in tests)
settings: Settings = load()
