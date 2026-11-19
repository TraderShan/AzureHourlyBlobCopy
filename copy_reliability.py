"""Leased, verifiable hourly transfers and missing-file reconciliation."""

# ----------------------------------------------------------------------------
# MODULE OVERVIEW - copy_reliability.py
# Middle layer: runs ONE attempt for ONE hour safely. Built on copy_job.py.
# Azure SDK packages are imported inside functions, so copy_job.py and the tests can load
# this module without contacting Azure.
#
# Flow of one attempt (execute -> transfer):
#   1. Read settings and open storage clients (storage_session).
#   2. Take a lease on the destination hour so only one worker writes it (LeaseGuard).
#   3. Either run an ordinary full-hour AzCopy, or inventory source/destination, copy only
#      missing files, re-check, and (strict manifest mode) publish the _SUCCESS marker.
#   4. Log the outcome and return safe counts to the Durable orchestrator.
#
# Who uses it:
#   - function_app.py activities call execute() for every primary attempt.
#   - copy_recovery.recover() calls execute() for background retries, and
#     copy_recovery.inspection() reuses storage_session, create_guard, read_doc, index_files,
#     parse_manifest, differences, and the SDK error helpers for failure reports.
#   - tests/test_reliability.py covers this module with in-memory fake containers.
# ----------------------------------------------------------------------------
import base64
import binascii
import hashlib
import json
import logging
import os
import re
from contextlib import ExitStack, contextmanager
from dataclasses import dataclass
from threading import Event, Thread
from time import monotonic

from copy_job import LOCK_ROOT, CopyError, Settings, Window, emit, recovery_partition, run_copy, sas_token

# ----------------------------------------------------------------------------
# SECTION 1 - Constants
# DOC_LIMIT        Largest manifest read_doc() will download (4 MiB).
# WORK_BUDGET      Time allowed for one attempt (56 min). host.json stops the activity at
#                  58 min, so the worker stops first and reports a clean, retryable error.
# EMPTY_STATS      Zero counts returned when no AzCopy job runs. The status endpoint in
#                  function_app.py requires these four integers on every successful result.
# TRANSIENT_CODES  HTTP status codes treated as temporary by sdk_failure().
# ----------------------------------------------------------------------------
DOC_LIMIT = 4 * 1024 * 1024
WORK_BUDGET = 56 * 60  # Includes SDK work; host.json gives the worker 58 minutes.
EMPTY_STATS = dict(transfersCompleted=0, transfersFailed=0, transfersSkipped=0, bytesTransferred=0)
TRANSIENT_CODES = {408, 429, 500, 502, 503, 504}


# ----------------------------------------------------------------------------
# SECTION 2 - Azure SDK error classification
# Turn Azure SDK exceptions into safe CopyErrors without copying their message text.
# Also imported by copy_recovery.py for its ledger and report storage calls.
# ----------------------------------------------------------------------------
# HTTP status from an SDK exception, or None.
def status_code(exc):
    code = getattr(exc, "status_code", None)
    return code if type(code) is int else None


# Azure error code (for example 'BlobNotFound'), or None.
def service_code(exc):
    value = getattr(exc, "error_code", None)
    return getattr(value, "value", value)


# Map an SDK exception to CopyError('<stage>_transient' | '<stage>_permission' | '<stage>_failed').
# Only '_transient' is retryable. 'stage' names the step, e.g. 'source_list' or 'lease_acquire'.
def sdk_failure(stage, exc):
    code = status_code(exc)
    kind = type(exc).__name__
    transient = code in TRANSIENT_CODES or kind in {"ServiceRequestError", "ServiceResponseError"}
    category = "transient" if transient else "permission" if code in (401, 403) or kind in {
        "ClientAuthenticationError", "CredentialUnavailableError"
    } else "failed"
    detail = {"httpStatus": code} if code is not None else {}
    return CopyError(f"{stage}_{category}", retryable=transient, **detail)


# ----------------------------------------------------------------------------
# SECTION 3 - Storage session
# Opens every client one attempt needs and closes them together. Used by transfer() and by
# copy_recovery.inspection(). Yields (source, target, control, source_sas, dest_sas):
#   source/target  container clients for listing, manifest reads, and marker writes
#   control        a second destination client used only for the lease (create, renew, release)
#   *_sas          '' in managed identity mode; Key Vault SAS strings in key_vault_sas mode
# SDK retries are disabled so Durable Functions is the single retry layer.
# ----------------------------------------------------------------------------
@contextmanager
def storage_session(cfg):
    from azure.identity import ManagedIdentityCredential
    from azure.storage.blob import ContainerClient

    logging.getLogger("azure").setLevel(logging.CRITICAL)
    # Route SDK retries through the durable policy rather than multiplying retry layers.
    options = {"retry_total": 0, "connection_timeout": 5, "read_timeout": 10}
    with ExitStack() as stack:
        identity = stack.enter_context(ManagedIdentityCredential(client_id=cfg.client_id or None, **options))
        src_sas = dst_sas = ""
        if cfg.auth_mode == "key_vault_sas":
            from azure.keyvault.secrets import SecretClient

            try:
                vault = stack.enter_context(SecretClient(cfg.key_vault_url, identity, **options))
                src_sas = sas_token(vault.get_secret(cfg.source_secret).value, "source")
                dst_sas = sas_token(vault.get_secret(cfg.dest_secret).value, "destination")
            except CopyError:
                raise
            except Exception as exc:
                raise sdk_failure("key_vault", exc) from None

        def client(url, token):
            return stack.enter_context(ContainerClient.from_container_url(url, credential=token or identity, **options))

        source = client(cfg.source.url, src_sas)
        target = client(cfg.destination.url, dst_sas)
        control = client(cfg.destination.url, dst_sas)  # Separate transport for renewal thread.
        yield source, target, control, src_sas, dst_sas


# ----------------------------------------------------------------------------
# SECTION 4 - Destination-hour lease
# A renewable 60-second Blob lease on a small control blob, keyed by the DESTINATION hour,
# stops two participating workers (primary run, rolling reconcile, background recovery, or
# another Function App) from writing the same hour at once.
#   lock_name()    control blob name under LOCK_ROOT (a hash, so no paths are exposed)
#   LeaseGuard     acquire / renew in a background thread / check() / release
#   create_guard() creates the control blob if needed and returns a LeaseGuard
# transfer() and copy_recovery.inspection() use: `with create_guard(...) as guard:`.
# ----------------------------------------------------------------------------
def lock_name(target_path):
    # The target container already scopes the lock. Two routes targeting the same hour must contend.
    return f"{LOCK_ROOT}/v1/{hashlib.sha256(target_path.encode('utf-8')).hexdigest()}.lock"


# duration    lease length in seconds
# renew_every renewal interval of the background thread
# margin      safety margin; the guard treats the lease as lost this many seconds early
# budget      absolute monotonic deadline (execute()'s WORK_BUDGET, or 45 s during
#             copy_recovery.inspection())
class LeaseGuard:
    duration = 60
    renew_every = 15
    margin = 8

    def __init__(self, lease, budget):
        self.lease = lease
        self.budget = budget
        self.stop = Event()
        self.lost = Event()
        self.renew_failure = None
        self.valid_until = 0
        self.thread = None

    # Acquire the lease. If another worker holds it, raise retryable 'window_locked' so
    # Durable Functions waits and tries again instead of copying concurrently.
    def __enter__(self):
        began = monotonic()
        try:
            self.lease.acquire(lease_duration=self.duration, timeout=10)
        except Exception as exc:
            if service_code(exc) in {"LeaseAlreadyPresent", "LeaseIsBreakingAndCannotBeAcquired"}:
                raise CopyError("window_locked", retryable=True) from None
            raise sdk_failure("lease_acquire", exc) from None
        self.valid_until = began + self.duration - self.margin
        self.thread = Thread(target=self._renew, name="copy-lease-renew", daemon=True)
        try:
            self.thread.start()
        except Exception:
            # No thread can renew; the finite lease will expire on its own.
            raise CopyError("lease_thread_failed", retryable=True) from None
        return self

    # Background thread: renew every renew_every seconds. Any failure, or renewing too late,
    # sets 'lost' so the next check() stops the work.
    def _renew(self):
        while not self.stop.wait(self.renew_every):
            began = monotonic()
            old_until = self.valid_until
            if began >= old_until:
                self.lost.set()
                return
            try:
                self.lease.renew(timeout=10)
            except Exception as exc:
                self.renew_failure = sdk_failure("lease_renew", exc)
                self.lost.set()
                return
            if monotonic() >= old_until:
                self.lost.set()
                return
            self.valid_until = began + self.duration - self.margin

    # Called before each storage step and, via run_copy(check=...), during AzCopy. Raises
    # 'window_lease_lost' (or the renewal failure) or 'activity_budget_exhausted'; both retryable.
    def check(self):
        now = monotonic()
        if self.lost.is_set() or now >= self.valid_until:
            self.lost.set()
            if self.renew_failure is not None:
                raise self.renew_failure
            raise CopyError("window_lease_lost", retryable=True)
        if now >= self.budget:
            raise CopyError("activity_budget_exhausted", retryable=True)

    # Stop renewing and release the lease. If release fails, the lease simply expires.
    def __exit__(self, *_):
        self.stop.set()
        self.thread.join(timeout=17)
        if self.thread.is_alive():
            emit("lease_release_deferred", logging.WARNING)  # Finite lease will expire.
            return
        try:
            self.lease.release(timeout=5)
        except Exception:
            emit("lease_release_deferred", logging.WARNING)


# Create the empty control blob (if new) and wrap it in a LeaseGuard. Used by transfer() and
# copy_recovery.inspection().
def create_guard(control, target_path, budget):
    from azure.storage.blob import BlobLeaseClient

    blob = control.get_blob_client(lock_name(target_path))
    try:
        blob.upload_blob(b"", overwrite=False, timeout=10)
    except Exception as exc:
        if service_code(exc) not in {"BlobAlreadyExists", "ConditionNotMet", "LeaseIdMissing"}:
            raise sdk_failure("lock_create", exc) from None
    return LeaseGuard(BlobLeaseClient(blob), budget)


# ----------------------------------------------------------------------------
# SECTION 5 - Inventories and the producer manifest
# BlobFacts is the per-file record compared between manifest, source, and destination:
# size, optional Content-MD5, and ETag (used to detect source changes during a copy).
# ----------------------------------------------------------------------------
@dataclass(frozen=True)
class BlobFacts:
    size: int
    md5: bytes | None = None
    etag: str | None = None


# Accept only safe relative paths: no '..', control characters, wildcards, or names that
# collide with the manifest/_SUCCESS control files. Applied to manifest entries and to the
# missing-file list before it is handed to AzCopy.
def valid_path(path, reserved):
    if not isinstance(path, str) or not 1 <= len(path) <= 1024 or path.split("/", 1)[0] in reserved:
        return False
    if re.search(r"[\x00-\x1f\x7f\\*]", path) or path != path.strip() or any(0xD800 <= ord(c) <= 0xDFFF for c in path):
        return False
    return all(part and part not in {".", ".."} for part in path.split("/"))


# Parse _MANIFEST.json (version 1) into {relative path: BlobFacts}. Rejects duplicates, bad
# sizes, bad MD5 values, and oversize lists. See examples/hour-manifest.example.json.
def parse_manifest(data, limit, reserved):
    try:
        def reject_duplicates(pairs):
            result = {}
            for key, value in pairs:
                if key in result:
                    raise ValueError("Duplicate JSON key")
                result[key] = value
            return result

        packet = json.loads(data, object_pairs_hook=reject_duplicates)
        if (not isinstance(packet, dict) or type(packet.get("version")) is not int or packet["version"] != 1
                or not isinstance(packet.get("files"), list)):
            raise ValueError("Unsupported manifest")
        if len(packet["files"]) > limit:
            raise CopyError("window_file_limit", limit=limit)
        files = {}
        for item in packet["files"]:
            if not isinstance(item, dict) or not valid_path(item.get("path"), reserved):
                raise ValueError("Invalid path")
            path, size = item["path"], item.get("size")
            if path in files or type(size) is not int or size < 0:
                raise ValueError("Invalid size or duplicate path")
            md5 = None
            if "contentMd5" in item:
                md5 = base64.b64decode(item["contentMd5"], validate=True)
                if len(md5) != 16:
                    raise ValueError("Invalid MD5 size")
            files[path] = BlobFacts(size, md5)
        return files
    except (ValueError, TypeError, RecursionError, binascii.Error):
        raise CopyError("invalid_source_manifest") from None


# Download a small document (manifest or _SUCCESS marker) with a size cap.
# Returns None if the blob does not exist; other failures become CopyErrors.
def read_doc(container, path, stage, limit=DOC_LIMIT):
    blob = container.get_blob_client(path)
    try:
        # A bounded range avoids downloading arbitrary large documents into memory.
        result = blob.download_blob(offset=0, length=limit + 1, max_concurrency=1, timeout=15).readall()
    except Exception as exc:
        if service_code(exc) == "BlobNotFound":
            return None
        if service_code(exc) == "InvalidRange" and status_code(exc) == 416:
            # Azure can reject a ranged read of an existing empty blob. Do not treat this as missing.
            raise CopyError(f"{stage}_invalid_document") from None
        raise sdk_failure(stage, exc) from None
    if len(result) > limit:
        raise CopyError(f"{stage}_too_large")
    return result


# Page through the blobs under one hour prefix and yield (relative path, BlobFacts).
# Skips control files and ADLS folder placeholders; checks the lease between pages.
def iter_files(container, prefix, reserved, guard, stage):
    try:
        guard.check()
        pages = container.list_blobs(name_starts_with=prefix, include=["metadata"],
                                    results_per_page=1000, timeout=15).by_page()
        while True:
            guard.check()
            page = next(pages, None)
            if page is None:
                break
            for blob in page:
                name = blob.name
                if not name.startswith(prefix):
                    raise CopyError("unexpected_list_path")
                relative = name[len(prefix):]
                if relative and relative.split("/", 1)[0] not in reserved and (blob.metadata or {}).get("hdi_isfolder", "").lower() != "true":
                    cs = blob.content_settings
                    md5 = bytes(cs.content_md5) if cs is not None and cs.content_md5 is not None else None
                    yield relative, BlobFacts(blob.size, md5, blob.etag)
    except CopyError:
        raise
    except Exception as exc:
        raise sdk_failure(stage, exc) from None


# Collect iter_files() into a dict, enforcing COPY_MAX_WINDOW_FILES.
def index_files(container, prefix, reserved, guard, stage, limit):
    found = {}
    for relative, facts in iter_files(container, prefix, reserved, guard, stage):
        found[relative] = facts
        if len(found) > limit:
            raise CopyError("window_file_limit", limit=limit)
    return found


# Compare two inventories. Returns (missing, changed, extra):
#   missing  in expected but not in actual -> candidates to copy
#   changed  same name, different size or MD5 -> conflicts (never overwritten)
#   extra    in actual but not expected
# require_md5=True (strict mode) treats a missing destination MD5 as a mismatch when the
# manifest supplies one. Also used by copy_recovery.inspection() for reports.
def differences(expected, actual, require_md5):
    missing = expected.keys() - actual.keys()
    changed = set()
    for path in expected.keys() & actual.keys():
        a, b = expected[path], actual[path]
        if (a.size != b.size or
                (a.md5 is not None and (require_md5 or b.md5 is not None) and a.md5 != b.md5)):
            changed.add(path)
    return missing, changed, actual.keys() - expected.keys()


# Strict mode: the source must match the manifest exactly. Files still missing at the
# source are retryable (the producer may still be uploading); anything else is a conflict.
def verify_source(expected, actual):
    absent, changed, extra = differences(expected, actual, require_md5=True)
    if changed or extra:
        raise CopyError("source_manifest_mismatch", changedCount=len(changed), unexpectedCount=len(extra), missingCount=len(absent))
    if absent:
        raise CopyError("source_manifest_files_missing", retryable=True, missingCount=len(absent))


# ----------------------------------------------------------------------------
# SECTION 6 - Verification marker and conflict reporting
# The _SUCCESS marker is written once, only after strict verification succeeds. It records
# the hour, file count, SHA-256 of the manifest bytes, and a hash of the source hour URL.
# Downstream consumers wait for it.
# ----------------------------------------------------------------------------
# An existing marker must describe the same manifest; otherwise the hour was published
# differently and is left for operator review ('published_manifest_conflict').
def check_marker(data, expected_marker):
    try:
        valid = json.loads(data) == expected_marker
    except (ValueError, TypeError, RecursionError):
        valid = False
    if not valid:
        raise CopyError("published_manifest_conflict")


# Create _SUCCESS without overwrite. If it already exists, it must match (check_marker).
def publish_marker(target, marker_path, marker, guard):
    from azure.storage.blob import ContentSettings

    encoded = json.dumps(marker, sort_keys=True, separators=(",", ":")).encode()
    guard.check()
    try:
        target.get_blob_client(marker_path).upload_blob(
            encoded, overwrite=False, timeout=10, content_settings=ContentSettings(content_type="application/json"))
    except Exception as exc:
        if service_code(exc) not in {"BlobAlreadyExists", "ConditionNotMet"}:
            raise sdk_failure("marker_write", exc) from None
        check_marker(read_doc(target, marker_path, "marker_read", 4096), marker)
    guard.check()


# Log conflict COUNTS (event 'copy_conflicts'). If COPY_WRITE_CONFLICT_REPORT=true, also
# write a create-only JSON list of conflicting names under LOCK_ROOT in the destination.
# 'stage' is 'before' or 'after' the copy. Returns details attached to the CopyError.
def log_conflicts(cfg, window, target, guard, fields, expected, actual, changed, extra, stage):
    details = {"changedCount": len(changed), "unexpectedCount": len(extra)}
    if cfg.write_conflict_report:
        control_hash = lock_name(cfg.destination.path(window)).rsplit("/", 1)[-1].removesuffix(".lock")
        name = f"{LOCK_ROOT}/v1/conflicts/{control_hash}/{window.run_id.hex}-{stage}.json"
        # Blob names can be sensitive. Put them only in the opt-in target report, not in telemetry.
        limit = 1000
        rows = [{"path": item, "reason": "changed", "sourceSize": expected[item].size, "targetSize": actual[item].size}
                for item in sorted(changed)[:limit]]
        rows += [{"path": item, "reason": "unexpected", "targetSize": actual[item].size}
                 for item in sorted(extra)[:limit - len(rows)]]
        data = {**fields, **details, "stage": stage, "truncated": len(changed) + len(extra) > limit, "files": rows}
        guard.check()
        try:
            target.get_blob_client(name).upload_blob(json.dumps(data).encode(), overwrite=False, timeout=10)
            details["conflictReportBlob"] = name
        except Exception as exc:
            if service_code(exc) in {"BlobAlreadyExists", "ConditionNotMet"}:
                details["conflictReportBlob"] = name
            else:
                emit("conflict_report_write_failed", logging.WARNING, **fields,
                     errorCode=sdk_failure("conflict_report", exc).code)
    emit("copy_conflicts", logging.WARNING, **fields, **details)
    return details


# ----------------------------------------------------------------------------
# SECTION 7 - transfer(): one leased attempt for one hour
# Called only by execute(). Returns a result dict whose 'outcome' is one of:
#   NoData        ordinary copy found an empty hour (allowed by COPY_ALLOW_EMPTY)
#   CopyFinished  ordinary full-hour AzCopy completed
#   Verified      strict manifest mode verified the hour and _SUCCESS exists
#   Reconciled    non-manifest reconcile copied what was missing; no marker is written
# Raises CopyError for every other result (conflicts, missing files, lease loss, ...).
# ----------------------------------------------------------------------------
def transfer(cfg, window, budget, fields):
    src_prefix, dst_prefix = cfg.source.path(window) + "/", cfg.destination.path(window) + "/"
    reserved = {cfg.manifest_name, cfg.success_name}
    with storage_session(cfg) as (source, target, control, src_sas, dst_sas):
        with create_guard(control, cfg.destination.path(window), budget) as guard:
            guard.check()
            manifest_path, marker_path = src_prefix + cfg.manifest_name, dst_prefix + cfg.success_name
            old_marker = read_doc(target, marker_path, "marker_read", 4096)
            guard.check()
            if old_marker is not None and not cfg.manifest_required:
                raise CopyError("published_window_requires_manifest")
            # PATH A - ordinary copy (no manifest, mode 'copy'): confirm at least one source file exists,
            # then run one full-hour AzCopy. COPY_OVERWRITE applies only on this path.
            if not cfg.manifest_required and window.mode == "copy":
                listing = iter_files(source, src_prefix, reserved, guard, "source_list")
                try:
                    has_file = next(listing, None) is not None
                finally:
                    listing.close()
                guard.check()
                if not has_file:
                    if not cfg.allow_empty:
                        raise CopyError("empty_source_window")
                    return {"outcome": "NoData", **EMPTY_STATS}
                return {"outcome": "CopyFinished", **run_copy(cfg, window, src_sas, dst_sas, check=guard.check, budget=budget)}

            # PATH B - strict manifest mode, or reconcile mode (including all v3 retries and recovery).
            # Step 1: decide what SHOULD exist. Strict: the producer's manifest. Otherwise: the files
            # currently visible at the source.
            strict = cfg.manifest_required
            marker = None
            marker_present = old_marker is not None
            if strict:
                raw = read_doc(source, manifest_path, "manifest_read")
                if raw is None:
                    raise CopyError("manifest_not_ready", retryable=True)
                expected = parse_manifest(raw, cfg.max_files, reserved)
                if not expected and not cfg.allow_empty:
                    raise CopyError("empty_source_window")
                marker = {"version": 1, "windowStart": fields["windowStart"], "fileCount": len(expected),
                          "manifestSha256": hashlib.sha256(raw).hexdigest(),
                          "sourceRouteSha256": hashlib.sha256(cfg.source.hour_url(window).encode()).hexdigest()}
                if marker_present:
                    check_marker(old_marker, marker)
            src = index_files(source, src_prefix, reserved, guard, "source_list", cfg.max_files)
            if strict:
                verify_source(expected, src)
            else:
                expected = src
                if not expected:
                    if fields.get('runKind') == 'recovery':
                        raise CopyError('recovery_source_not_observed', retryable=True)
                    if fields.get('requestedMode') == 'copy':
                        raise CopyError('retry_source_not_observed', retryable=True)
            # Step 2: compare with the destination. Conflicts are logged and never overwritten.
            dst = index_files(target, dst_prefix, reserved, guard, "target_list", cfg.max_files)
            missing, changed, extra = differences(expected, dst, require_md5=strict)
            # In strict mode leave the whole destination untouched if verification already found a conflict.
            if changed or (strict and extra):
                conflict_details = log_conflicts(cfg, window, target, guard, fields, expected, dst, changed,
                                                 extra if strict else set(), "before")
                if strict:
                    raise CopyError("target_conflicts", **conflict_details)
            else:
                conflict_details = None
            before_conflicts = changed
            if marker_present and missing:
                # Do not silently repair a previously published immutable hour under an old marker.
                raise CopyError("published_data_missing", missingCount=len(missing))
            if any(not valid_path(p, reserved) for p in missing):
                raise CopyError("unsupported_selection_path")
            # Step 3: copy ONLY the missing files (exact list, no overwrite), then list the destination
            # again so the result reflects what actually landed.
            stats = dict(EMPTY_STATS)
            if missing:
                stats = run_copy(cfg, window, src_sas, dst_sas, paths=missing, check=guard.check, budget=budget)
                dst = index_files(target, dst_prefix, reserved, guard, "target_list", cfg.max_files)
            # Step 4: re-check. Anything still missing is retryable; new conflicts are not.
            still_missing, changed, extra = differences(expected, dst, require_md5=strict)
            if changed or (strict and extra):
                if conflict_details is None or changed != before_conflicts:
                    conflict_details = log_conflicts(cfg, window, target, guard, fields, expected, dst, changed,
                                                     extra if strict else set(), "after")
                raise CopyError("target_conflicts", **conflict_details,
                                **{k: v for k, v in stats.items() if k == "azcopyJobId"})
            if still_missing:
                raise CopyError("target_files_missing", retryable=True, missingCount=len(still_missing),
                                **{k: v for k, v in stats.items() if k == "azcopyJobId"})
            if strict:
                # Compare source ETags to its own snapshot (not to target ETags). Also re-read the manifest.
                after = index_files(source, src_prefix, reserved, guard, "source_list", cfg.max_files)
                verify_source(expected, after)
                if src != after or raw != read_doc(source, manifest_path, "manifest_read"):
                    raise CopyError("source_changed_during_verification", retryable=True)
                if not marker_present:
                    publish_marker(target, marker_path, marker, guard)
                guard.check()
                return {"outcome": "Verified", "verifiedFiles": len(expected), **stats}
            guard.check()
            return {"outcome": "Reconciled", "checkedFiles": len(expected), "missingBefore": len(missing), **stats}


# ----------------------------------------------------------------------------
# SECTION 8 - execute(): activity entry point
# Called by function_app.py activities (copy_hour_activity, copy_hour_attempt,
# copy_hour_v3_attempt) and by copy_recovery.recover().
# payload = Window.as_dict() plus private keys set by the callers:
#   _attempt, _maxRetries   from the orchestrator; decide copy_attempt_failed vs copy_failed
#   _requestedMode          from function_app.run_v3_copy(); the mode of the original request
#   _runKind, _recoveryAttempt, _originalRunId, _expectedRoute
#                           from copy_recovery.recover() for background recovery
# Logs copy_started, then copy_empty / copy_finished / copy_verified / copy_reconciled on
# success, or copy_attempt_failed / copy_failed / recovery_attempt_failed on failure.
# Any unexpected exception becomes CopyError('worker_error') so no raw text leaks out.
# ----------------------------------------------------------------------------
def execute(payload):
    window = Window.parse(payload)
    attempt = payload.get("_attempt", 1)
    attempts = payload.get("_maxRetries", 0) + 1
    fields = {**window.as_dict(), "instanceId": window.instance_id, "attempt": attempt}
    for input_key, log_key in (("_runKind", "runKind"), ("_originalRunId", "originalPipelineRunId"),
                               ("_requestedMode", "requestedMode"), ("_recoveryAttempt", "recoveryAttempt")):
        if input_key in payload:
            fields[log_key] = payload[input_key]
    started = monotonic()
    emit("copy_started", **fields)
    try:
        cfg = Settings.read(os.environ)
        if '_expectedRoute' in payload and payload['_expectedRoute'] != recovery_partition(cfg):
            raise CopyError('recovery_route_changed')
        result = transfer(cfg, window, started + WORK_BUDGET, fields)
    except Exception as exc:
        safe = exc if isinstance(exc, CopyError) else CopyError("worker_error")
        will_retry = safe.retryable and attempt < attempts
        is_recovery = fields.get('runKind') == 'recovery'
        event = "recovery_attempt_failed" if is_recovery else "copy_attempt_failed" if will_retry else "copy_failed"
        emit(event, logging.WARNING if will_retry or is_recovery else logging.ERROR,
             **fields, durationSeconds=round(monotonic() - started, 2), errorCode=safe.code,
             retryable=safe.retryable, **safe.details)
        raise CopyError(safe.code, retryable=safe.retryable, **safe.details) from None
    result = {**result, "durationSeconds": round(monotonic() - started, 2)}
    event = {"NoData": "copy_empty", "Verified": "copy_verified", "Reconciled": "copy_reconciled"}.get(result["outcome"], "copy_finished")
    emit(event, **fields, **result)
    return result
