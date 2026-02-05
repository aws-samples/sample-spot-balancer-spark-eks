"""
Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
SPDX-License-Identifier: MIT-0
"""
import logging
import os
import threading
import time

from flask import Flask
from kubernetes import client, config

from .config import settings
from .helpers import reconcile_all
from .routes import create_routes
from .store.redis_store import RedisStore

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO").upper(),
    format="%(asctime)s %(levelname)s %(message)s",
)
log = logging.getLogger("spot-balancer")

# Initialize Kubernetes client; avoid constructing real client in tests
if os.getenv("APP_ENV", getattr(settings, "app_env", "production")) == "test":
    core = object()
else:
    config.load_incluster_config()
    core = client.CoreV1Api()

app = Flask(__name__)
datastore = None
_reconcile_thread = None
try:
    if not getattr(settings, "redis_url", ""):
        if getattr(settings, "app_env", "production") != "test":
            raise RuntimeError("REDIS_URL is required but not set")
        else:
            log.warning("REDIS_URL not set in test env; datastore disabled for tests")
    else:
        datastore = RedisStore(
            settings.redis_url,
            getattr(settings, "redis_default_ttl_seconds", 86400),
        )
        
except Exception as e:
    if getattr(settings, "app_env", "production") == "test":
        log.warning("Failed to initialize Redis in test env: %s; datastore disabled", e)
    else:
        raise


def _start_reconcile_worker():
    if getattr(settings, "app_env", "production") == "test":
        return

    if not getattr(settings, "reconcile_enabled", True):
        log.info("Reconcile worker disabled by config")
        return

    global _reconcile_thread
    if _reconcile_thread is not None:
        return

    interval = max(60, int(getattr(settings, "reconcile_interval_seconds", 1800)))

    def _loop():
        while True:
            try:
                reconcile_all(core, settings, datastore)
            except Exception:
                pass
            finally:
                time.sleep(interval)

    _reconcile_thread = threading.Thread(
        target=_loop, name="reconcile-worker", daemon=True
    )
    _reconcile_thread.start()
    log.info("Reconcile worker started (interval=%ss)", interval)


bp = create_routes(core, settings, datastore)
app.register_blueprint(bp)

# Start reconcile worker eagerly in non-test envs (compatible with gunicorn)
_start_reconcile_worker()

if __name__ == "__main__":
    log.info("Starting webhook server...")
    _start_reconcile_worker()
    app.run(
        host="0.0.0.0",
        port=int(os.getenv("PORT", "8443")),
        ssl_context=("tls/tls.crt", "tls/tls.key"),
    )
