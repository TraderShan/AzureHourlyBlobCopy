"""Configuration, exact-window selection, and AzCopy execution (Azure public cloud)."""

# ----------------------------------------------------------------------------
# MODULE OVERVIEW - copy_job.py
# Lowest layer of the worker. It has no Azure SDK imports, so every other module can use it.
#
# What lives here:
#   1. Shared constants, the CopyError type, and the emit() structured logger.
#   2. Window: validates the hour that Azure Data Factory (ADF) sends and builds the
#      YYYY/MM/DD/HH folder path and the Durable Functions instance ID.
#   3. Settings: reads and validates the Function App settings (storage accounts, prefixes,
#      authentication, timeouts, manifest options).
#   4. AzCopy execution: builds the command line, runs the process, and classifies results.
#
# Who uses it:
#   - function_app.py: Window, CopyError, emit, env_match, env_flag, INSTANCE_PATTERN.
#   - copy_reliability.py: Settings, Window, run_copy, sas_token, LOCK_ROOT, recovery_partition.
#   - copy_recovery.py: Settings, Window, env helpers, recovery_partition.
#   - tests/test_copy_job.py covers this module directly.
# ----------------------------------------------------------------------------
import hashlib
import json
import logging
import os
import re
import shutil
import subprocess
import tempfile
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from time import monotonic
from urllib.parse import parse_qs, quote
from uuid import UUID

# ----------------------------------------------------------------------------
# SECTION 1 - Shared constants
# LOCK_ROOT        Reserved top-level folder in the destination container. copy_reliability.py
#                  stores lease blobs and optional conflict reports under it. Settings.read()
#                  rejects a DEST_PREFIX that starts with this name.
# LOG_PREFIX       Prefix on every structured log line written by emit(). The KQL files in
#                  monitor/ filter on 'hourly_blob_copy '; changing it breaks those queries and alerts.
# INSTANCE_PATTERN Shape of Window.instance_id. function_app.copy_status() rejects any other ID.
# TIMESTAMP        Accepted ISO 8601 form for windowStart/windowEnd (used by utc_hour()).
# SAS_FORM         Characters allowed in a SAS query string read from Key Vault (used by sas_token()).
# ----------------------------------------------------------------------------
LOCK_ROOT = "_hourly_blob_copy_control"
LOG_PREFIX = "hourly_blob_copy "
INSTANCE_PATTERN = re.compile(r"blobcopy-[0-9a-f]{32}-[0-9]{10}")
TIMESTAMP = re.compile(r"[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}(?:\.([0-9]{1,7}))?(?:Z|[+-][0-9]{2}:[0-5][0-9])")
SAS_FORM = re.compile(r"[A-Za-z0-9%+/.=~_&-]+")


# ----------------------------------------------------------------------------
# SECTION 2 - Error type and structured logging
# CopyError is the only failure type that crosses module boundaries. It carries:
#   code       short snake_case reason, e.g. 'azcopy_transient' or 'target_conflicts'
#   retryable  True only for temporary problems; drives Durable retries in function_app.py
#              and background recovery in copy_recovery.py
#   details    safe numbers and IDs only (counts, HTTP status, AzCopy job ID), never blob
#              names, URLs, SAS tokens, or raw service messages
# copy_reliability.execute() logs it; copy_recovery.safe_failure() allowlists its details
# before they are stored in Durable history, reports, or the recovery ledger.
# ----------------------------------------------------------------------------
class CopyError(RuntimeError):
    """Only developer-defined error codes and safe numbers cross the worker boundary."""

    def __init__(self, code, *, retryable=False, **details):
        super().__init__(code)
        self.code = code
        self.retryable = retryable
        self.details = details


# Write one structured log line: LOG_PREFIX followed by compact JSON with an 'event' name.
# Application Insights stores these as AppTraces; monitor/*.kql parse them for dashboards and
# alerts. Called from every module. Only pass safe fields (see CopyError above).
def emit(event, level=logging.INFO, **fields):
    logging.log(level, LOG_PREFIX + json.dumps({"event": event, **fields}, separators=(",", ":")))


# ----------------------------------------------------------------------------
# SECTION 3 - Hour window parsing
# The ADF tumbling-window trigger supplies windowStart/windowEnd. These helpers make sure the
# request names exactly one closed UTC hour before any storage work starts.
# ----------------------------------------------------------------------------
# Parse one timestamp, convert any offset to UTC, and require the top of an hour.
# Used only by Window.parse(). Raises ValueError, which the HTTP starter returns as 400.
def utc_hour(value):
    match = TIMESTAMP.fullmatch(value) if isinstance(value, str) else None
    if not match:
        raise ValueError("Expected an ISO 8601 timestamp with offset")
    try:
        dt = datetime.fromisoformat(value).astimezone(timezone.utc)
    except OverflowError:
        raise ValueError("Timestamp is outside the supported range") from None
    # ADF may send seven fractional digits; don't let Python truncate a nonzero digit.
    if dt.minute or dt.second or (match[1] or "").strip("0"):
        raise ValueError("Expected the top of a UTC hour")
    return dt


# One validated copy request: the hour, the ADF pipeline run ID, and the mode.
#   mode 'copy'      normal hourly run (ordinary full-hour copy unless strict manifest mode)
#   mode 'reconcile' compare inventories and copy only missing files
# Window.parse() is the single validator for every caller:
#   - function_app.copy_hour()      the ADF POST body
#   - copy_reliability.execute()    the payload of every activity attempt
#   - copy_recovery                 ledger rows and background retry windows
@dataclass(frozen=True)
class Window:
    start: datetime
    end: datetime
    run_id: UUID
    mode: str = "copy"

    @classmethod
    def parse(cls, payload):
        if not isinstance(payload, dict):
            raise ValueError("Expected a JSON object")
        start = utc_hour(payload.get("windowStart"))
        end = utc_hour(payload.get("windowEnd"))
        run_id = payload.get("pipelineRunId")
        if not isinstance(run_id, str) or not re.fullmatch(r"[0-9a-fA-F]{8}(?:-[0-9a-fA-F]{4}){3}-[0-9a-fA-F]{12}", run_id):
            raise ValueError("Expected an ADF pipeline run UUID")
        if end - start != timedelta(hours=1):
            raise ValueError("Expected a one-hour window")
        mode = payload.get("mode", "copy")
        if mode not in ("copy", "reconcile"):
            raise ValueError("Invalid copy mode")
        return cls(start, end, UUID(run_id), mode)

    # Folder segment taken from the window START, e.g. 2026/11/18/14 for 14:00-15:00 UTC.
    # Used by Location.path() to build source/destination paths and by copy_recovery as a key.
    @property
    def date_path(self):
        d = self.start
        return f"{d.year:04}/{d.month:02}/{d.day:02}/{d.hour:02}"

    # Deterministic Durable Functions instance ID: same ADF run + same hour = same ID, so a
    # resubmitted request finds the existing run instead of starting a duplicate copy.
    @property
    def instance_id(self):
        return "blobcopy-" + self.run_id.hex + "-" + self.date_path.replace("/", "")

    # JSON-safe form passed between the orchestrator and activities, logged in every event, and
    # parsed again by Window.parse() on the other side.
    def as_dict(self):
        return {"windowStart": self.start.isoformat().replace("+00:00", "Z"),
                "windowEnd": self.end.isoformat().replace("+00:00", "Z"),
                "pipelineRunId": str(self.run_id), "mode": self.mode}


# ----------------------------------------------------------------------------
# SECTION 4 - Function App settings
# Helpers that read app settings with strict validation. A bad value raises
# CopyError('invalid_setting', setting=<name>), which is not retried.
# env_match/env_flag are also used by function_app.py (retry and timer settings) and
# copy_recovery.py (recovery settings).
# ----------------------------------------------------------------------------
def env_match(env, key, pattern, default=""):
    value = env.get(key, default).strip()
    if not re.fullmatch(pattern, value):
        raise CopyError("invalid_setting", setting=key)
    return value


# Read a 'true'/'false' setting as a bool.
def env_flag(env, key, default):
    return env_match(env, key, r"true|false", default) == "true"


# Normalize SOURCE_PREFIX/DEST_PREFIX: backslashes become '/', outer slashes are trimmed, and
# empty, '.', '..', control characters, and '*' segments are rejected. '' means container root.
def read_prefix(env, key, default=""):
    # Accept Windows-style separators as input; Azure blob paths use forward slashes.
    raw = env.get(key, default).replace("\\", "/").strip("/")
    if raw and any(not part or part in {".", ".."} or re.search(r"[\x00-\x1f\x7f*]", part)
                   for part in raw.split("/")):
        raise CopyError("invalid_setting", setting=key)
    return raw


# One side of the transfer (account + container + optional prefix).
#   url        container endpoint. With Private Link the same hostname resolves to a private IP.
#   path()     prefix + hour folder; used for listings, lock names, and report paths
#   hour_url() URL-encoded hour directory passed to AzCopy (trailing slash = directory)
@dataclass(frozen=True)
class Location:
    account: str
    container: str
    prefix: str

    @property
    def url(self):
        return f"https://{self.account}.blob.core.windows.net/{self.container}"

    def path(self, window):
        return "/".join(filter(None, (self.prefix, window.date_path)))

    def hour_url(self, window):
        return f"{self.url}/{quote(self.path(window), safe='/')}/"


# Immutable snapshot of the Function App settings for one attempt. It is read fresh by
# copy_reliability.execute() and by copy_recovery (record_terminal, due_keys, recover), so a
# changed setting applies to the next attempt, never to a copy already running.
# Note: read() builds this with positional arguments, so field order must stay in sync.
@dataclass(frozen=True)
class Settings:
    source: Location
    destination: Location
    auth_mode: str
    client_id: str
    key_vault_url: str
    source_secret: str
    dest_secret: str
    binary: str
    timeout: int
    overwrite: str
    allow_empty: bool
    preserve_tier: bool
    manifest_required: bool
    manifest_name: str
    success_name: str
    max_files: int
    write_conflict_report: bool

    @classmethod
    def read(cls, env):
        def location(side, prefix):
            return Location(
                env_match(env, f"{side}_STORAGE_ACCOUNT", r"[a-z0-9]{3,24}"),
                env_match(env, f"{side}_CONTAINER", r"(?=.{3,63}$)[a-z0-9]+(?:-[a-z0-9]+)*"),
                prefix,
            )

        # DEST_PREFIX defaults to SOURCE_PREFIX (mirror the layout); an explicit '' means root.
        # Key Vault settings are read only in key_vault_sas mode; managed identity needs none.
        source_prefix = read_prefix(env, "SOURCE_PREFIX")
        mode = env_match(env, "COPY_AUTH_MODE", r"managed_identity|key_vault_sas", "managed_identity")
        client_id = env_match(env, "COPY_MI_CLIENT_ID", r"(?:[0-9a-fA-F]{8}(?:-[0-9a-fA-F]{4}){3}-[0-9a-fA-F]{12})?")
        kv_url = source_secret = dest_secret = ""
        if mode == "key_vault_sas":
            kv_url = env_match(env, "COPY_KEY_VAULT_URL", r"https://[a-zA-Z0-9-]{3,24}\.vault\.azure\.net/?").rstrip("/")
            source_secret = env_match(env, "COPY_SOURCE_SAS_SECRET", r"[a-zA-Z0-9-]{1,127}")
            dest_secret = env_match(env, "COPY_DEST_SAS_SECRET", r"[a-zA-Z0-9-]{1,127}")
        timeout = int(env_match(env, "AZCOPY_TIMEOUT_SECONDS", r"[0-9]{1,4}", "2700"))
        if not 1 <= timeout <= 3300:
            raise CopyError("invalid_setting", setting="AZCOPY_TIMEOUT_SECONDS")
        source = location("SOURCE", source_prefix)
        destination = location("DEST", read_prefix(env, "DEST_PREFIX", source_prefix))
        # Prevent same-container feedback loops, including across different hours.
        if source.url == destination.url:
            raise CopyError("same_source_and_target_container")
        if destination.prefix.split("/", 1)[0] == LOCK_ROOT:
            raise CopyError("reserved_target_prefix")
        control_name = r"[A-Za-z0-9_][A-Za-z0-9_.-]{0,127}"
        manifest_name = env_match(env, "COPY_MANIFEST_NAME", control_name, "_MANIFEST.json")
        success_name = env_match(env, "COPY_SUCCESS_MARKER_NAME", control_name, "_SUCCESS")
        if manifest_name == success_name:
            raise CopyError("duplicate_control_names")
        max_files = int(env_match(env, "COPY_MAX_WINDOW_FILES", r"[1-9][0-9]{0,4}", "10000"))
        return cls(source, destination, mode, client_id, kv_url, source_secret, dest_secret,
                   env.get("AZCOPY_PATH", "/usr/local/bin/azcopy"), timeout,
                   env_match(env, "COPY_OVERWRITE", r"false|true|ifSourceNewer", "false"),
                   env_flag(env, "COPY_ALLOW_EMPTY", "true"),
                   env_flag(env, "COPY_PRESERVE_ACCESS_TIER", "false"),
                   env_flag(env, "COPY_REQUIRE_MANIFEST", "false"), manifest_name, success_name, max_files,
                   env_flag(env, "COPY_WRITE_CONFLICT_REPORT", "false"))


# Stable hash of the route (both endpoints, prefixes, and manifest policy). copy_recovery.py
# uses it as the Azure Table PartitionKey and report folder; execute() compares it with
# '_expectedRoute' so queued recovery never runs against a reconfigured feed.
def recovery_partition(cfg):
    # A changed pair, prefix or verification policy cannot consume another route's work.
    route = [cfg.source.url, cfg.source.prefix, cfg.destination.url, cfg.destination.prefix,
             cfg.manifest_required, cfg.manifest_name, cfg.success_name]
    return hashlib.sha256(json.dumps(route, separators=(',', ':')).encode()).hexdigest()


# ----------------------------------------------------------------------------
# SECTION 5 - AzCopy command, environment, and result handling
# run_copy() is the only entry point other modules call (copy_reliability.transfer()).
# The other helpers are internal steps, also covered directly by tests/test_copy_job.py.
# ----------------------------------------------------------------------------
# Validate a SAS query string fetched from Key Vault (optional key_vault_sas mode).
# Called by copy_reliability.storage_session(). Rejects full URLs and tokens without 'sig'.
def sas_token(value, side):
    token = (value or "").strip().removeprefix("?")
    if not SAS_FORM.fullmatch(token) or not parse_qs(token).get("sig"):
        raise CopyError("invalid_sas_secret", side=side)
    return token  # Preserve the original encoding; do not decode/re-encode the signature.


# Build the AzCopy argument list. Two shapes:
#   full-hour copy (file_list=None): recursive copy of the hour folder; honors COPY_OVERWRITE;
#      the manifest and _SUCCESS control files are excluded
#   selected files (file_list set): copy only the listed relative paths, never overwrite,
#      non-recursive; used for reconciliation, strict manifest mode, and recovery
# JSON output is requested so parse_summary() and azcopy_signals() can read it.
def make_command(cfg, window, binary, source_sas, dest_sas, file_list=None):
    def append_sas(url, token):
        return url + ("?" + token if token else "")

    # Use directory URLs with trailing slashes. --as-subdir=false avoids /HH/HH/.
    cmd = [binary, "copy", append_sas(cfg.source.hour_url(window), source_sas),
            append_sas(cfg.destination.hour_url(window), dest_sas), "--from-to=BlobBlob",
            "--recursive=true", "--as-subdir=false", f"--overwrite={cfg.overwrite if not file_list else 'false'}",
            f"--s2s-preserve-access-tier={str(cfg.preserve_tier).lower()}",
            "--s2s-detect-source-changed=true", "--output-type=json", "--log-level=NONE",
            "--disable-telemetry=true"]
    # Names are fixed safe basenames. Exclude them if doing a full directory transfer.
    if file_list:
        cmd.append(f"--list-of-files={file_list}")
        # Treat the list as individual blobs, never expand a listed name into a subtree.
        cmd[cmd.index("--recursive=true")] = "--recursive=false"
    else:
        cmd.append(f"--exclude-path={cfg.manifest_name};{cfg.success_name}")
    return cmd


# Minimal environment for the AzCopy process: no Function secrets or host settings are
# inherited. Logs and job plans go to the temporary scratch folder that run_copy() deletes.
# In managed identity mode AzCopy logs in with MSI (optionally a user-assigned client ID).
def child_environment(cfg, scratch):
    env = {k: v for k, v in os.environ.items() if k in {
        "PATH", "LD_LIBRARY_PATH", "LANG", "LC_ALL", "SSL_CERT_FILE", "SSL_CERT_DIR",
        "HTTP_PROXY", "HTTPS_PROXY", "NO_PROXY", "http_proxy", "https_proxy", "no_proxy",
        "AZCOPY_CONCURRENCY_VALUE", "AZCOPY_BUFFER_GB",
    }}
    env.update(HOME=scratch, TMPDIR=scratch, AZCOPY_LOG_LOCATION=scratch, AZCOPY_JOB_PLAN_LOCATION=scratch)
    if cfg.auth_mode == "managed_identity":
        env.update({k: os.environ[k] for k in ("IDENTITY_ENDPOINT", "IDENTITY_HEADER", "MSI_ENDPOINT", "MSI_SECRET")
                    if k in os.environ})
        env["AZCOPY_AUTO_LOGIN_TYPE"] = "MSI"
        if cfg.client_id:
            env["AZCOPY_MSI_CLIENT_ID"] = cfg.client_id
    return env


# Read the final 'EndOfJob' JSON record and return the four counts the status endpoint needs.
# Raises if the job reported failures, did not complete, or transferred/skipped nothing.
def parse_summary(lines):
    """Read just the allowed fields, never log raw AzCopy output."""
    summary = None
    for line in lines:
        try:
            row = json.loads(line)
            if isinstance(row, dict) and row.get("MessageType") == "EndOfJob":
                content = row.get("MessageContent")
                summary = json.loads(content) if isinstance(content, str) else content
        except (ValueError, UnicodeError):
            continue
    if not isinstance(summary, dict):
        raise CopyError("azcopy_missing_summary")

    def count(key):
        val = summary.get(key)
        if not re.fullmatch(r"[0-9]{1,24}", str(val)):
            raise CopyError("azcopy_invalid_summary")
        return int(val)

    stats = dict(transfersCompleted=count("TransfersCompleted"), transfersFailed=count("TransfersFailed"),
                 transfersSkipped=count("TransfersSkipped"), bytesTransferred=count("TotalBytesTransferred"))
    if stats["transfersFailed"] or summary.get("JobStatus") not in ("Completed", "CompletedWithSkipped"):
        raise CopyError("azcopy_job_failed", **stats)
    if not stats["transfersCompleted"] and not stats["transfersSkipped"]:
        raise CopyError("azcopy_no_transfers")
    return stats


# Patterns that classify a failed AzCopy run without ever logging its output:
#   _PERMANENT  authorization, missing container, and 4xx responses -> do not retry
#   _TRANSIENT  throttling, timeouts, 5xx, and network errors       -> safe to retry
# Scan AzCopy diagnostics in private temporary files. Never emit the original text.
_PERMANENT = re.compile(rb"\b(?:ERROR CODE|ErrorCode)[: =]+(?:AuthorizationPermissionMismatch|AuthorizationFailure|AuthenticationFailed|InvalidAuthenticationInfo|ContainerNotFound|InvalidResourceName|AccountIsDisabled|KeyBasedAuthenticationNotPermitted|AuthorizationSourceIPMismatch)\b|\b(?:RESPONSE|HTTP(?:/1\.[01])?)\s+(?:400|401|403|404|409)\b", re.I)
_TRANSIENT = re.compile(rb"\b(?:ERROR CODE|ErrorCode)[: =]+(?:ServerBusy|OperationTimedOut|InternalError|TooManyRequests)\b|\b(?:RESPONSE|HTTP(?:/1\.[01])?)\s+(?:408|429|500|502|503|504)\b|context deadline exceeded|i/o timeout|connection reset by peer|temporary failure in name resolution", re.I)


# Scan captured stdout/stderr and return (permanent?, transient?, {'azcopyJobId': ...}).
# The job ID is logged so operators can correlate a run with AzCopy diagnostics.
def azcopy_signals(*streams):
    permanent = transient = False
    job_id = None
    for stream in streams:
        stream.seek(0)
        for line in stream:
            permanent |= bool(_PERMANENT.search(line))
            transient |= bool(_TRANSIENT.search(line))
            try:
                record = json.loads(line)
                if isinstance(record, dict) and record.get("MessageType") in ("Init", "EndOfJob"):
                    content = record.get("MessageContent")
                    obj = json.loads(content) if isinstance(content, str) else content
                    job = obj.get("JobID") if isinstance(obj, dict) else None
                    if isinstance(obj, dict) and isinstance(obj.get("FailedTransfers"), list):
                        for failure in obj["FailedTransfers"]:
                            code = str(failure.get("ErrorCode")) if isinstance(failure, dict) else ""
                            if re.fullmatch(r"[A-Za-z0-9]{1,64}", code):
                                signal = f"ERROR CODE: {code}".encode()
                                permanent |= code in {"400", "401", "403", "404", "409"} or bool(_PERMANENT.search(signal))
                                transient |= code in {"408", "429", "500", "502", "503", "504"} or bool(_TRANSIENT.search(signal))
                    if isinstance(job, str) and re.fullmatch(r"[0-9a-fA-F]{8}(?:-[0-9a-fA-F]{4}){3}-[0-9a-fA-F]{12}", job):
                        job_id = job.lower()
            except (ValueError, UnicodeError):
                continue
    return permanent, transient, {"azcopyJobId": job_id} if job_id else {}


# Run one AzCopy job and return safe counts (+ job ID). Called by copy_reliability.transfer().
#   check   callback run before start and about once per second during the copy. transfer()
#           passes LeaseGuard.check, so a lost lease or exhausted budget stops AzCopy.
#   paths   None = full-hour copy; a set = copy exactly these relative paths
#   budget  absolute monotonic deadline shared with the rest of the attempt (WORK_BUDGET)
# Raises CopyError: azcopy_transient (retryable), azcopy_permanent, azcopy_unclassified,
# azcopy_timeout, and others. Unknown failures are deliberately not retryable.
def run_copy(cfg, window, source_sas, dest_sas, *, check=lambda: None, paths=None, budget=None):
    if paths is not None and not paths:
        raise CopyError("empty_file_selection")
    binary = shutil.which(cfg.binary) if cfg.binary else None
    if not binary:
        raise CopyError("azcopy_not_executable")
    binary = os.path.abspath(binary)
    with (tempfile.TemporaryDirectory(prefix="hourly-copy-") as scratch,
          tempfile.TemporaryFile(dir=scratch) as output, tempfile.TemporaryFile(dir=scratch) as errors):
        # Write the selected-file list (one relative path per line) for AzCopy's --list-of-files.
        file_list = None
        if paths is not None:
            file_list = os.path.join(scratch, "selection.txt")
            with open(file_list, "w", encoding="utf-8", newline="\n") as selected:
                for path in sorted(paths):
                    selected.write(path + "\n")
        # Stop at whichever comes first: AZCOPY_TIMEOUT_SECONDS or the attempt's overall budget.
        check()
        deadline = min(monotonic() + cfg.timeout, budget if budget is not None else float("inf"))
        if monotonic() >= deadline:
            raise CopyError("activity_budget_exhausted", retryable=True)
        # Start AzCopy and poll it; check() runs between polls. The finally block kills the
        # process if anything (timeout, lost lease) interrupts the loop.
        try:
            with subprocess.Popen(make_command(cfg, window, binary, source_sas, dest_sas, file_list),
                                  env=child_environment(cfg, scratch), cwd=scratch, stdin=subprocess.DEVNULL,
                                  stdout=output, stderr=errors) as process:
                try:
                    while True:
                        check()
                        remaining = deadline - monotonic()
                        if remaining <= 0:
                            raise CopyError("azcopy_timeout", retryable=True)
                        try:
                            code = process.wait(timeout=min(1, remaining))
                            check()
                            break
                        except subprocess.TimeoutExpired:
                            continue
                finally:
                    if process.poll() is None:
                        process.kill()
                        process.wait()
        except CopyError as exc:
            _, _, job = azcopy_signals(output, errors)
            raise CopyError(exc.code, retryable=exc.retryable, **exc.details, **job) from None
        except OSError:
            raise CopyError("azcopy_start_failed") from None

        # Process finished: classify it. A zero exit code still needs a valid success summary.
        permanent, transient, job = azcopy_signals(output, errors)
        output.seek(0)
        try:
            counts = parse_summary(output)
        except CopyError as exc:
            counts = {}
            if not code:
                raise CopyError(exc.code, **exc.details, **job) from None
            counts = exc.details
        if code:
            category = "azcopy_permanent" if permanent else "azcopy_transient" if transient else "azcopy_unclassified"
            raise CopyError(category, retryable=transient and not permanent, exitCode=code, **counts, **job)
        return {**counts, **job}
