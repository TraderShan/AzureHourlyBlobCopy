"""HTTP control plane and Durable orchestration for the hourly AzCopy worker."""

# ----------------------------------------------------------------------------
# MODULE OVERVIEW - function_app.py
# Azure Functions entry points (Python v2 programming model). Every function the host runs is
# registered here; the real work lives in copy_job.py, copy_reliability.py, and
# copy_recovery.py.
#
# How a normal hour runs:
#   ADF StartCopy     -> POST /api/copy-hour        -> copy_hour() starts the orchestrator
#   orchestrator      -> copy_hour_v3_attempt       -> copy_reliability.execute()
#                        (retries with backoff; on final failure -> copy_terminal_report)
#   ADF GetCopyStatus -> GET /api/copy-status/<id>  -> copy_status() returns safe status
#
# Background recovery (optional):
#   timer copy_recovery_tick -> copy_recovery_orchestrator -> copy_recovery_due, then
#   copy_recovery_window for each due hour (copy_recovery.due_keys / recover).
#
# Both HTTP routes require a Function key (AuthLevel.FUNCTION); ADF reads it from Key Vault.
# Covered by tests/test_http.py, which loads this file with fake Azure Functions modules.
# ----------------------------------------------------------------------------
import json
import logging
import os
import re
from datetime import datetime, timedelta, timezone

import azure.durable_functions as df
import azure.functions as func

from copy_job import INSTANCE_PATTERN, CopyError, Window, emit, env_flag, env_match
from copy_reliability import execute
from copy_recovery import due_keys, record_terminal, recover, safe_failure

# ----------------------------------------------------------------------------
# SECTION 1 - App object and shared helpers
# 'app' is the Functions application; every decorator below registers a function on it.
# ----------------------------------------------------------------------------
app = df.DFApp(http_auth_level=func.AuthLevel.FUNCTION)


# Build a JSON HTTP response that proxies and clients must not cache.
def json_response(body, status=200):
    return func.HttpResponse(json.dumps(body), status_code=status, mimetype="application/json",
                             headers={"Cache-Control": "no-store"})


# Read COPY_RETRY_COUNT (0-2 extra attempts) and COPY_RETRY_BASE_SECONDS (1-300). copy_hour()
# stores the values in the orchestration input, so changing the settings later does not
# affect runs that already started.
def retry_settings():
    count = int(env_match(os.environ, "COPY_RETRY_COUNT", r"[0-2]", "2"))
    delay = int(env_match(os.environ, "COPY_RETRY_BASE_SECONDS", r"[1-9][0-9]?|[12][0-9]{2}|300", "30"))
    return {"_maxRetries": count, "_retryBaseSeconds": delay}


# ----------------------------------------------------------------------------
# SECTION 2 - HTTP API called by Azure Data Factory
# Both calls return quickly, avoiding the 230-second HTTP limit for long copies.
# ----------------------------------------------------------------------------
# POST /api/copy-hour  (ADF activity 'StartCopy' in adf/pl_hourly_blob_copy.json)
# Body: windowStart, windowEnd, pipelineRunId, optional mode. The hour must be closed.
# Starts copy_hour_orchestrator with a deterministic instance ID (Window.instance_id); if that
# instance already exists it is NOT started again. Returns {instanceId, accepted}; ADF passes
# instanceId to the status route. 400 = bad request, 500 = bad retry settings, 503 = Durable
# storage unavailable.
@app.route(route="copy-hour", methods=["POST"])
@app.durable_client_input(client_name="client")
async def copy_hour(req: func.HttpRequest, client):
    try:
        window = Window.parse(req.get_json())
        if window.end > datetime.now(timezone.utc):
            raise ValueError("Window is still open")
    except ValueError:
        return json_response({"error": "Expected a closed UTC-aligned hour and a valid pipelineRunId."}, 400)

    try:
        state = await client.get_status(window.instance_id)
        if not state:
            try:
                policy = retry_settings()
            except CopyError as exc:
                emit("start_failed", logging.ERROR, instanceId=window.instance_id, **window.as_dict(),
                     errorCode=exc.code, **exc.details)
                return json_response({"error": "Invalid retry configuration."}, 500)
            accepted = await client.start_new("copy_hour_orchestrator", instance_id=window.instance_id,
                                              client_input={**window.as_dict(), **policy, "_recoveryV3": True})
            if accepted != window.instance_id:
                raise RuntimeError("Unexpected instance ID")
    except Exception:
        emit("start_failed", logging.ERROR, instanceId=window.instance_id, **window.as_dict())
        return json_response({"error": "Could not start or look up the copy."}, 503)
    # ADF polls our restricted endpoint; no Durable management URLs or keys are returned.
    return json_response({"instanceId": window.instance_id, "accepted": True})


# GET /api/copy-status/<instanceId>  (ADF activity 'GetCopyStatus', polled in a loop)
# ADF reads runtimeStatus (Completed / Running / Pending / ... / Failed / NotVisible),
# errorCode, and recoveryReportBlob. Only allowlisted fields are returned: counts and
# outcome on success; a validated error code and report path on failure. A Completed run
# with an unexpected result shape is reported as 'InvalidCopyResult' so ADF fails it.
@app.route(route="copy-status/{instance_id}", methods=["GET"])
@app.durable_client_input(client_name="client")
async def copy_status(req: func.HttpRequest, client):
    instance_id = req.route_params.get("instance_id", "")
    if not INSTANCE_PATTERN.fullmatch(instance_id):
        return json_response({"error": "Invalid copy instance ID."}, 400)
    try:
        state = await client.get_status(instance_id)
    except Exception:
        return json_response({"error": "Could not read copy status."}, 503)
    if not state or state.runtime_status is None:
        return json_response({"instanceId": instance_id, "runtimeStatus": "NotVisible"})
    if state.name != "copy_hour_orchestrator":
        return json_response({"error": "Not an hourly copy instance."}, 404)
    status = state.runtime_status.name
    body = {"instanceId": instance_id, "runtimeStatus": status}
    if status == "Completed":
        # Return only the expected safe result fields, never arbitrary Durable payloads.
        result = state.output
        counts = ("transfersCompleted", "transfersFailed", "transfersSkipped", "bytesTransferred")
        if (not isinstance(result, dict) or result.get("outcome") not in ("NoData", "CopyFinished", "Verified", "Reconciled")
                or any(type(result.get(k)) is not int or result[k] < 0 for k in counts)
                or result["transfersFailed"] != 0):
            body["runtimeStatus"] = "InvalidCopyResult"
        else:
            body["result"] = {k: result[k] for k in ("outcome", *counts)}
            for key in ("verifiedFiles", "checkedFiles", "missingBefore"):
                if type(result.get(key)) is int and result[key] >= 0:
                    body["result"][key] = result[key]
            job_id = result.get("azcopyJobId")
            if isinstance(job_id, str) and re.fullmatch(r"[0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12}", job_id):
                body["result"]["azcopyJobId"] = job_id
    elif status == "Failed":
        custom = getattr(state, "custom_status", None)
        code = custom.get("errorCode") if isinstance(custom, dict) else None
        if isinstance(code, str) and re.fullmatch(r"[a-z_]{1,64}", code):
            body["errorCode"] = code
        report = custom.get("reportBlob") if isinstance(custom, dict) else None
        if isinstance(report, str) and re.fullmatch(r"recovery/v1/(?:[a-f0-9]{64}|unconfigured)/[0-9]{4}(?:/[0-9]{2}){3}/[a-f0-9]{32}-terminal\.json", report):
            body["recoveryReportBlob"] = report
    return json_response(body)


# ----------------------------------------------------------------------------
# SECTION 3 - Copy orchestration (Durable Functions)
# Orchestrator code is replayed by Durable Functions, so it must stay deterministic: no I/O,
# clocks, or random values here; that work belongs in activities.
# ----------------------------------------------------------------------------
# Routes by input shape so runs started by older versions finish with their original logic:
#   no _maxRetries  -> v1 (single copy_hour_activity call)
#   _recoveryV3     -> current behavior, run_v3_copy()  (copy_hour() always sets this)
#   otherwise       -> v2 retry loop below (copy_hour_attempt)
@app.orchestration_trigger(context_name="context")
def copy_hour_orchestrator(context: df.DurableOrchestrationContext):
    data = context.get_input()
    if "_maxRetries" not in data:
        # Preserve the original orchestration history shape for old in-flight instances.
        return (yield context.call_activity("copy_hour_activity", data))
    if data.get("_recoveryV3"):
        return (yield from run_v3_copy(context, data))
    # Preserve v2's activity/timer history for in-flight orchestrations.
    for n in range(data["_maxRetries"] + 1):
        result = yield context.call_activity("copy_hour_attempt", {**data, "_attempt": n + 1})
        failure = result.get("failure")
        if not failure:
            return result
        if not failure["retryable"] or n >= data["_maxRetries"]:
            context.set_custom_status({"errorCode": failure["errorCode"], "attempts": n + 1})
            raise CopyError(failure["errorCode"])
        delay = data["_retryBaseSeconds"] * (2 ** n)
        yield context.create_timer(context.current_utc_datetime + timedelta(seconds=delay))


# Current retry loop (generator used with 'yield from' by the orchestrator):
#   attempt 1   uses the requested mode
#   attempt 2+  forced to 'reconcile', so only missing files are copied after a partial copy
#   retryable failure -> wait _retryBaseSeconds * 2^n (30 s, 60 s by default) and try again
#   final failure     -> copy_terminal_report activity (report + ledger), store errorCode,
#                        attempts, and reportBlob in custom status for copy_status(), then fail
# The last AzCopy job ID seen is carried forward so the report can reference it.
# If the activity itself crashes, the outcome is unknown: 'worker_outcome_unknown', not retried.
def run_v3_copy(context, data):
    last_job = None
    for n in range(data["_maxRetries"] + 1):
        attempt = {**data, "_attempt": n + 1, "_requestedMode": data["mode"]}
        # After a partial primary copy always compare inventories before starting AzCopy again.
        # Strict manifest mode still performs its manifest and marker checks.
        if n:
            attempt["mode"] = "reconcile"
        try:
            result = yield context.call_activity("copy_hour_v3_attempt", attempt)
            failure = result.get("failure")
        except Exception:
            failure = {"errorCode": "worker_outcome_unknown", "retryable": False}
        if not failure:
            return result
        if failure.get("azcopyJobId"):
            last_job = failure["azcopyJobId"]
        elif last_job:
            failure = {**failure, "azcopyJobId": last_job, "jobFromEarlierAttempt": True}
        if not failure["retryable"] or n >= data["_maxRetries"]:
            try:
                report = yield context.call_activity("copy_terminal_report", {
                    "window": data, "failure": failure, "attempt": n + 1})
            except Exception:
                report = {}
            context.set_custom_status({"errorCode": failure["errorCode"], "attempts": n + 1, **report})
            raise CopyError(failure["errorCode"])
        delay = data["_retryBaseSeconds"] * (2 ** n)
        yield context.create_timer(context.current_utc_datetime + timedelta(seconds=delay))


# ----------------------------------------------------------------------------
# SECTION 4 - Activities (run the actual work)
# copy_hour_activity    v1 only: errors propagate as activity failures
# copy_hour_attempt     v2 only: returns {'failure': {errorCode, retryable}}
# copy_hour_v3_attempt  current: returns {'failure': safe_failure(...)} including job ID
# copy_terminal_report  calls copy_recovery.record_terminal() after the final failure
# Keep the v1/v2 activities while older orchestrations might still be replaying.
# ----------------------------------------------------------------------------
@app.activity_trigger(input_name="payload")
def copy_hour_activity(payload: dict):
    return execute(payload)


# v2 activity (kept for replay of older orchestrations).
@app.activity_trigger(input_name="payload")
def copy_hour_attempt(payload: dict):
    try:
        return execute(payload)
    except CopyError as err:
        return {"failure": {"errorCode": err.code, "retryable": err.retryable}}


# Current activity: one attempt via copy_reliability.execute(); failures returned as safe data.
@app.activity_trigger(input_name="payload")
def copy_hour_v3_attempt(payload: dict):
    try:
        return execute(payload)
    except CopyError as err:
        return {"failure": safe_failure(err)}


# Final-failure activity: report and recovery ledger via copy_recovery.record_terminal().
@app.activity_trigger(input_name="payload")
def copy_terminal_report(payload: dict):
    return record_terminal(payload)


# ----------------------------------------------------------------------------
# SECTION 5 - Background recovery
# Schedule comes from the COPY_RECOVERY_SCHEDULE setting (default every 15 minutes, UTC).
# Nothing runs unless COPY_RECOVERY_AUTO_ENABLED=true.
# ----------------------------------------------------------------------------
# Timer: start the single recovery dispatcher (fixed instance ID) unless one is already
# active, so ticks never overlap.
@app.timer_trigger(schedule="%COPY_RECOVERY_SCHEDULE%", arg_name="timer", run_on_startup=False, use_monitor=True)
@app.durable_client_input(client_name="client")
async def copy_recovery_tick(timer: func.TimerRequest, client):
    try:
        if not env_flag(os.environ, "COPY_RECOVERY_AUTO_ENABLED", "false"):
            return
        key = "hourly-copy-recovery-dispatch"
        state = await client.get_status(key)
        if state and state.runtime_status and state.runtime_status.name in {
            "Running", "Pending", "ContinuedAsNew", "Suspended"
        }:
            return
        if await client.start_new("copy_recovery_orchestrator", instance_id=key) != key:
            raise CopyError("recovery_dispatch_start_failed")
    except Exception:
        emit("recovery_dispatch_failed", logging.ERROR, errorCode="recovery_dispatch_start_failed")
        raise CopyError("recovery_dispatch_start_failed") from None


# Dispatcher: get the due hours once, then recover them one at a time (serially) so an
# incident cannot launch an unbounded number of copies.
@app.orchestration_trigger(context_name="context")
def copy_recovery_orchestrator(context: df.DurableOrchestrationContext):
    keys = yield context.call_activity("copy_recovery_due", "")
    outcomes = []
    for key in keys:
        try:
            outcomes.append((yield context.call_activity("copy_recovery_window", key)))
        except Exception:
            # Claims are finite; the next scan can retry an expired claim within its budget.
            outcomes.append("InfrastructureFailure")
    return outcomes


# Wrapper for the two recovery activities: log a safe failure event and re-raise a CopyError
# without any raw exception text.
def recovery_activity(event, fn, *args):
    try:
        return fn(*args)
    except Exception as exc:
        failure = safe_failure(exc)
        emit(event, logging.ERROR, **failure)
        raise CopyError(failure["errorCode"]) from None


# Activity: list due ledger keys (copy_recovery.due_keys).
@app.activity_trigger(input_name="unused")
def copy_recovery_due(unused: str):
    return recovery_activity("recovery_scan_failed", due_keys)


# Activity: recover one hour (copy_recovery.recover).
@app.activity_trigger(input_name="key")
def copy_recovery_window(key: str):
    return recovery_activity("recovery_window_failed", recover, key)
