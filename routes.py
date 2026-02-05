import logging
import sys as _sys

from flask import Blueprint, jsonify, request

from .helpers import (
    adjust_executor_count,
    choose_bucket,
    count_executors,
    make_admission_response,
    patch_selector,
    reconcile_all,
    spot_ratio_from_driver,
    validate_kubernetes_name,
    validate_spot_ratio,
)
from .models import AdmissionReviewModel

log = logging.getLogger("spot-balancer")

# Expose module under short name 'routes' for tests that monkeypatch via sys.modules
_sys.modules.setdefault("routes", _sys.modules[__name__])


def create_routes(core, settings, datastore):
    bp = Blueprint("webhook", __name__)

    @bp.route("/health", methods=["GET"])
    def health():
        return {"status": "healthy"}, 200

    @bp.route("/mutate", methods=["POST"])
    def mutate():
        try:
            review_json = request.get_json()

            admission = AdmissionReviewModel.from_dict(review_json or {})
            if admission is None:
                log.warning("Invalid AdmissionReview payload for /mutate")
                return jsonify(make_admission_response(uid="", allowed=False)), 400

            req = admission.request
            uid = req.uid
            pod = req.obj
            ns = pod.namespace

            workload_role = pod.labels.get(settings.role_label)
            if workload_role != settings.executor_role_value:
                return jsonify(make_admission_response(uid, True))

            job_id = pod.labels.get(settings.job_id_label)
            if not job_id:
                log.info("No job_id label on executor; allowing without mutation")
                return jsonify(make_admission_response(uid, True))

            # Validate namespace and job_id format
            is_valid_ns, ns_error = validate_kubernetes_name(ns, "namespace")
            if not is_valid_ns:
                log.error("Invalid namespace in /mutate: %s", ns_error)
                return jsonify(make_admission_response(uid, False)), 400
            
            is_valid_job, job_error = validate_kubernetes_name(job_id, "job_id")
            if not is_valid_job:
                log.error("Invalid job_id in /mutate: %s", job_error)
                return jsonify(make_admission_response(uid, False)), 400

            # Ratio from driver annotation for the job (original convention), cached per job in Redis
            ratio = None
            if datastore is not None:
                try:
                    _cache = datastore.get(f"ratio:{ns}:{job_id}")
                    if _cache is not None:
                        ratio = float(_cache[1])
                        log.info(
                            "Loaded ratio from Redis for ns=%s job_id=%s -> %.3f",
                            ns,
                            job_id,
                            ratio,
                        )
                except Exception:
                    ratio = None

            if ratio is None:
                ratio = spot_ratio_from_driver(core, settings, ns, job_id)
                if ratio is not None and datastore is not None:
                    try:
                        # Validate ratio before caching
                        is_valid_ratio, ratio_error = validate_spot_ratio(ratio)
                        if not is_valid_ratio:
                            log.error("Invalid spot ratio from driver: %s", ratio_error)
                            return jsonify(make_admission_response(uid, False)), 400
                        
                        datastore.set(
                            f"ratio:{ns}:{job_id}",
                            float(ratio),
                            ttl_seconds=getattr(
                                settings, "redis_default_ttl_seconds", 86400
                            ),
                        )
                        log.info(
                            "Cached ratio in Redis for ns=%s job_id=%s -> %.3f",
                            ns,
                            job_id,
                            ratio,
                        )
                    except Exception:
                        pass

            if ratio is None:
                # If ratio unavailable, allow without patch (fallback behavior)
                log.info(
                    "No ratio available for ns=%s job_id=%s; allowing without mutation",
                    ns,
                    job_id,
                )
                return jsonify(make_admission_response(uid, True))

            # Final validation of the ratio before using it
            is_valid_ratio, ratio_error = validate_spot_ratio(ratio)
            if not is_valid_ratio:
                log.error("Invalid spot ratio before decision: %s", ratio_error)
                return jsonify(make_admission_response(uid, False)), 400

            od, spot = count_executors(datastore, ns, job_id)
            bucket = choose_bucket(od, spot, ratio)
            _pref = getattr(settings, "spot_preference", "BEST_EFFORT").upper()
            log.info(
                "Decision inputs: ns=%s job_id=%s preference=%s ratio=%.3f od=%s spot=%s -> bucket=%s",
                ns,
                job_id,
                _pref,
                ratio,
                od,
                spot,
                bucket,
            )

            if _pref == "STRICT":
                if bucket == "spot":
                    adjust_executor_count(
                        datastore,
                        ns,
                        job_id,
                        delta_spot=1,
                        ttl_seconds=settings.redis_default_ttl_seconds,
                    )
                    patch = patch_selector(settings, "spot")
                    log.info("STRICT: patching to spot for ns=%s job_id=%s", ns, job_id)

                    return jsonify(make_admission_response(uid, True, patch))

                if bucket is None:
                    # STRICT: enforce spot even if capacity not currently available; let it wait
                    adjust_executor_count(
                        datastore,
                        ns,
                        job_id,
                        delta_spot=1,
                        ttl_seconds=settings.redis_default_ttl_seconds,
                    )
                    patch = patch_selector(settings, "spot")
                    log.info(
                        "STRICT: patching to spot (force wait) for ns=%s job_id=%s",
                        ns,
                        job_id,
                    )

                    return jsonify(make_admission_response(uid, True, patch))

                # bucket == "on-demand"
                adjust_executor_count(
                    datastore,
                    ns,
                    job_id,
                    delta_od=1,
                    ttl_seconds=settings.redis_default_ttl_seconds,
                )
                patch = patch_selector(settings, "on-demand")
                log.info(
                    "STRICT: patching to on-demand for ns=%s job_id=%s", ns, job_id
                )

                return jsonify(make_admission_response(uid, True, patch))
            else:
                # BEST_EFFORT
                if bucket is None or bucket == "spot":
                    if bucket == "spot":
                        adjust_executor_count(
                            datastore,
                            ns,
                            job_id,
                            delta_spot=1,
                            ttl_seconds=settings.redis_default_ttl_seconds,
                        )
                        log.info(
                            "BEST_EFFORT: choosing spot (no patch) for ns=%s job_id=%s",
                            ns,
                            job_id,
                        )
                    else:
                        log.info(
                            "BEST_EFFORT: no decision (no patch) for ns=%s job_id=%s",
                            ns,
                            job_id,
                        )
                    return jsonify(make_admission_response(uid, True))

                # on-demand
                adjust_executor_count(
                    datastore,
                    ns,
                    job_id,
                    delta_od=1,
                    ttl_seconds=settings.redis_default_ttl_seconds,
                )
                patch = patch_selector(settings, "on-demand")
                log.info(
                    "BEST_EFFORT: patching to on-demand for ns=%s job_id=%s", ns, job_id
                )
                return jsonify(make_admission_response(uid, True, patch))
        except Exception as e:
            log.error("Error in /mutate: %s", e)
            return (
                jsonify(
                    make_admission_response(
                        uid="",
                        allowed=True,
                    )
                ),
                500,
            )

    @bp.route("/validate", methods=["POST"])
    def validate():
        """
        Validating webhook: handle Pod DELETE to decrement counters.
        """
        try:
            review_json = request.get_json()
            admission = AdmissionReviewModel.from_dict(review_json or {})
            if admission is None:
                log.warning("Invalid AdmissionReview payload for /validate")
                return jsonify(make_admission_response(uid="", allowed=False)), 400

            req = admission.request
            uid = req.uid
            if req.operation != "DELETE":
                return jsonify(make_admission_response(uid, True))

            # For DELETE, Kubernetes provides the previous state under oldObject
            pod = req.old_obj or req.obj
            ns = pod.namespace

            # Only handle executors
            workload_role = pod.labels.get(settings.role_label)
            if workload_role != settings.executor_role_value:
                return jsonify(make_admission_response(uid, True))

            job_id = pod.labels.get(settings.job_id_label)
            if not job_id:
                log.info("DELETE without job_id; allowing")
                return jsonify(make_admission_response(uid, True))

            # Validate namespace and job_id format
            is_valid_ns, ns_error = validate_kubernetes_name(ns, "namespace")
            if not is_valid_ns:
                log.warning("Invalid namespace in /validate DELETE: %s. Allowing anyway.", ns_error)
                return jsonify(make_admission_response(uid, True))
            
            is_valid_job, job_error = validate_kubernetes_name(job_id, "job_id")
            if not is_valid_job:
                log.warning("Invalid job_id in /validate DELETE: %s. Allowing anyway.", job_error)
                return jsonify(make_admission_response(uid, True))

            capacity = pod.node_selector.get(settings.capacity_type_label)
            if capacity == "on-demand":
                adjust_executor_count(
                    datastore,
                    ns,
                    job_id,
                    delta_od=-1,
                    ttl_seconds=settings.redis_default_ttl_seconds,
                )
                log.info(
                    "DELETE: decremented on-demand for ns=%s job_id=%s", ns, job_id
                )
            elif capacity == "spot":
                adjust_executor_count(
                    datastore,
                    ns,
                    job_id,
                    delta_spot=-1,
                    ttl_seconds=settings.redis_default_ttl_seconds,
                )
                log.info("DELETE: decremented spot for ns=%s job_id=%s", ns, job_id)
            else:
                # In BEST_EFFORT, spot selections are not patched; treat unset as spot
                adjust_executor_count(
                    datastore,
                    ns,
                    job_id,
                    delta_spot=-1,
                    ttl_seconds=settings.redis_default_ttl_seconds,
                )
                log.info(
                    "DELETE: capacity unset; decrementing spot (BEST_EFFORT) for ns=%s job_id=%s",
                    ns,
                    job_id,
                )

            return jsonify(make_admission_response(uid, True))
        except Exception:
            log.error("Error in /validate", exc_info=True)
            return jsonify(make_admission_response(uid="", allowed=True))

    return bp
