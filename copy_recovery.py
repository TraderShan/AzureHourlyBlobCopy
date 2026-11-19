"""Restricted recovery reports and a bounded, persistent retry ledger."""

# ----------------------------------------------------------------------------
# MODULE OVERVIEW - copy_recovery.py
# Top layer: what happens AFTER an hour has failed. Optional; everything here is off unless
# COPY_RECOVERY_STORAGE_ACCOUNT is set. Background retries also need
# COPY_RECOVERY_AUTO_ENABLED=true.
#
# Two jobs:
#   1. record_terminal(): when the orchestrator gives up on an hour, write a private JSON
#      report (what is missing/conflicting) and, for retryable failures, add a row to the
#      recovery ledger (Azure Table).
#   2. due_keys() + recover(): on a timer, pick due ledger rows and re-run the hour in
#      reconcile mode through copy_reliability.execute(), with a bounded number of attempts.
#
# Ledger row life cycle (PartitionKey = route hash, RowKey = YYYYMMDDHH):
#   Pending -> Running (claimed) -> Resolved
#                                -> Pending again (temporary failure, later due time)
#                                -> Exhausted (attempt cap reached)
#                                -> OperatorRequired (permanent failure, e.g. conflicts)
#
# Who uses it: function_app.py activities copy_terminal_report (record_terminal),
# copy_recovery_due (due_keys), and copy_recovery_window (recover); function_app.py also
# uses safe_failure(). Covered by tests/test_recovery.py.
# ----------------------------------------------------------------------------
import json
import logging
import os
import re
from contextlib import ExitStack, contextmanager
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from itertools import islice
from time import monotonic
from uuid import UUID, uuid4, uuid5

from copy_job import CopyError, Settings, Window, emit, env_flag, env_match, recovery_partition
from copy_reliability import (create_guard, differences, execute, index_files, parse_manifest,
                              read_doc, sdk_failure, service_code, status_code, storage_session)

# ----------------------------------------------------------------------------
# SECTION 1 - Constants and small helpers
# NAMESPACE     UUID namespace for deterministic run IDs of background attempts (retry_window)
# JOB_ID        allowed AzCopy job ID format (lowercase UUID)
# CATEGORY      allowed error code format (lowercase snake_case)
# CLAIM_MINUTES how long a 'Running' claim is valid; longer than the 58-minute activity
#               timeout, so an interrupted claim becomes due again only after the work stopped
# ----------------------------------------------------------------------------
NAMESPACE = UUID('990b4f17-a76b-461b-981a-b5af2310bbcb')
JOB_ID = re.compile(r'[0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12}')
CATEGORY = re.compile(r'[a-z_]{1,64}')
CLAIM_MINUTES = 65  # Greater than the 58-minute Functions activity timeout.


# Single clock for this module; tests replace it to simulate time passing.
def now_utc():
    return datetime.now(timezone.utc)


# Reduce any exception to an allowlisted dict: errorCode, retryable, and a few safe numbers
# (AzCopy job ID, exit code, HTTP status, transfer counts). This is what Durable history,
# reports, and the ledger store. Used here and by function_app.copy_hour_v3_attempt() and
# function_app.recovery_activity().
def safe_failure(err):
    if not isinstance(err, CopyError):
        err = CopyError('worker_error')
    result = {'errorCode': err.code if CATEGORY.fullmatch(err.code) else 'worker_error',
              'retryable': err.retryable is True}
    job = err.details.get('azcopyJobId')
    if isinstance(job, str) and JOB_ID.fullmatch(job):
        result['azcopyJobId'] = job
    for key in ('exitCode', 'httpStatus', 'transfersCompleted', 'transfersFailed', 'transfersSkipped'):
        if type(err.details.get(key)) is int:
            result[key] = err.details[key]
    return result


# ----------------------------------------------------------------------------
# SECTION 2 - Recovery settings and storage clients
# RecoverySettings.read() returns None when COPY_RECOVERY_STORAGE_ACCOUNT is blank, which
# turns reports and the ledger off. Limits: attempts 1-8, delay 1-240 min, batch 1-5,
# report items 1-1000 per category.
# ----------------------------------------------------------------------------
@dataclass(frozen=True)
class RecoverySettings:
    account: str
    table: str
    container: str
    client_id: str
    max_attempts: int
    delay: int
    batch: int
    report_limit: int

    @classmethod
    def read(cls, env):
        account = env_match(env, 'COPY_RECOVERY_STORAGE_ACCOUNT', r'(?:[a-z0-9]{3,24})?')
        if not account:
            return None

        def integer(key, default, high):
            value = int(env_match(env, key, r'[1-9][0-9]{0,3}', str(default)))
            if value > high:
                raise CopyError('invalid_setting', setting=key)
            return value

        return cls(account, env_match(env, 'COPY_RECOVERY_TABLE', r'[A-Za-z][A-Za-z0-9]{2,62}', 'HourlyCopyRecovery'),
                   env_match(env, 'COPY_RECOVERY_REPORT_CONTAINER', r'(?=.{3,63}$)[a-z0-9]+(?:-[a-z0-9]+)*', 'hourly-copy-recovery'),
                   env_match(env, 'COPY_RECOVERY_MI_CLIENT_ID', r'(?:[0-9a-fA-F]{8}(?:-[0-9a-fA-F]{4}){3}-[0-9a-fA-F]{12})?'),
                   integer('COPY_RECOVERY_MAX_ATTEMPTS', 3, 8), integer('COPY_RECOVERY_DELAY_MINUTES', 15, 240),
                   integer('COPY_RECOVERY_BATCH_SIZE', 2, 5), integer('COPY_RECOVERY_REPORT_LIMIT', 250, 1000))



# Open the ledger table and the report container with the recovery managed identity.
# Yields (table, reports, update mode, match condition); the last two are passed to
# transition() so ledger updates only succeed if the row has not changed (ETag check).
@contextmanager
def recovery_session(cfg):
    from azure.core import MatchConditions
    from azure.data.tables import TableClient, UpdateMode
    from azure.identity import ManagedIdentityCredential
    from azure.storage.blob import ContainerClient

    logging.getLogger('azure').setLevel(logging.CRITICAL)
    options = {'retry_total': 0, 'connection_timeout': 5, 'read_timeout': 10}
    with ExitStack() as stack:
        identity = stack.enter_context(ManagedIdentityCredential(client_id=cfg.client_id or None, **options))
        table = stack.enter_context(TableClient(f'https://{cfg.account}.table.core.windows.net', cfg.table,
                                                credential=identity, **options))
        reports = stack.enter_context(ContainerClient(f'https://{cfg.account}.blob.core.windows.net', cfg.container,
                                                      credential=identity, **options))
        yield table, reports, UpdateMode.MERGE, MatchConditions.IfNotModified


# ----------------------------------------------------------------------------
# SECTION 3 - Failure snapshot for reports
# inspection() takes a short (45 s) leased look at the failed hour and describes it. Called
# only by record_terminal(). It never copies data. If a listing or the manifest cannot be
# read, that check is recorded as unavailable instead of reporting files as missing.
# Result keys: inspection ('complete' | 'partial' | 'unavailable'), counts, items (names, up
# to 'limit' per category), truncated, checksUnavailable.
# ----------------------------------------------------------------------------
def inspection(cfg, window, limit):
    """Re-inventory post-failure; report absence only when a relevant inventory was read."""
    summary = {'basis': 'producer_manifest' if cfg.manifest_required else 'observed_source',
               'inspection': 'unavailable', 'checksUnavailable': [], 'counts': {}, 'items': {}, 'truncated': False}
    reserved = {cfg.manifest_name, cfg.success_name}
    src_prefix, dst_prefix = cfg.source.path(window) + '/', cfg.destination.path(window) + '/'
    budget = monotonic() + 45
    expected = src = dst = None

    # store(): record one category (exact count plus the first 'limit' names, sorted).
    # inspect(): run one listing step; on failure log it under checksUnavailable and continue.
    def store(label, names):
        names = sorted(names)
        summary['counts'][label] = len(names)
        summary['items'][label] = names[:limit]
        summary['truncated'] |= len(names) > limit

    def inspect(label, fn):
        try:
            guard.check()
            return fn()
        except Exception as exc:
            problem = exc.code if isinstance(exc, CopyError) else 'inspection_failed'
            summary['checksUnavailable'].append({'check': label, 'errorCode': problem})
            return None

    try:
        with storage_session(cfg) as (source, target, control, _, __):
            with create_guard(control, cfg.destination.path(window), budget) as guard:
                if cfg.manifest_required:
                    def get_manifest():
                        raw = read_doc(source, src_prefix + cfg.manifest_name, 'manifest_read')
                        if raw is None:
                            raise CopyError('manifest_not_ready')
                        return parse_manifest(raw, cfg.max_files, reserved)
                    expected = inspect('manifest', get_manifest)
                src = inspect('source', lambda: index_files(source, src_prefix, reserved, guard, 'source_list', cfg.max_files))
                dst = inspect('destination', lambda: index_files(target, dst_prefix, reserved, guard, 'target_list', cfg.max_files))
                inspect('snapshot_lease', guard.check)
    except Exception as exc:
        summary['checksUnavailable'].append({'check': 'snapshot', 'errorCode': exc.code if isinstance(exc, CopyError) else 'inspection_failed'})
        return summary

    # Without a manifest, the source listing is the only definition of 'expected'. If nothing
    # authoritative could be read, report observed names as 'unverified' and stop.
    if not cfg.manifest_required:
        expected = src
    if expected is None:
        # These observed names cannot be compared against an authoritative file list.
        if src is not None:
            store('unverified', src.keys())
        else:
            summary['unverifiedPopulationUnknown'] = True
        return summary
    if dst is not None:
        missing, changed, extra = differences(expected, dst, require_md5=cfg.manifest_required)
        store('missingAtDestination', missing)
        store('conflictsAtDestination', changed)
        store('unexpectedAtDestination', extra)
        compared_by_size_only = {p for p in expected.keys() & dst.keys() if p not in changed
                                 and (expected[p].md5 is None or dst[p].md5 is None)}
        store('sizeOnlyMatches', compared_by_size_only)
    unverified = set(expected) if dst is None or src is None else set()
    if cfg.manifest_required and src is not None:
        missing, changed, extra = differences(expected, src, require_md5=True)
        store('missingAtSource', missing)
        store('conflictsAtSource', changed)
        store('unexpectedAtSource', extra)
        unverified.update(missing | changed)
    # If the lease was lost during inspection, another worker may have changed the hour; drop the
    # comparisons and mark every expected file 'unverified'.
    if any(check['check'] == 'snapshot_lease' for check in summary['checksUnavailable']):
        unverified.update(expected)
        summary['counts'] = {}
        summary['items'] = {}
        summary['truncated'] = False
    store('unverified', unverified)
    summary['inspection'] = 'partial' if summary['checksUnavailable'] else 'complete'
    return summary


# ----------------------------------------------------------------------------
# SECTION 4 - Reports and ledger writes
# report_path()  recovery/v1/<route>/<YYYY/MM/DD/HH>/<run id>-<label>.json in the private
#                report container. function_app.copy_status() only exposes '-terminal' paths
#                of this exact shape to ADF.
# write_report() create-only upload; an existing report is never replaced.
# enqueue()      insert the ledger row; a second failure for the same route/hour returns
#                'already_tracked', so duplicate ADF requests share one retry budget.
# ----------------------------------------------------------------------------
def report_path(route, window, label):
    day = window.date_path
    return f'recovery/v1/{route}/{day}/{window.run_id.hex}-{label}.json'


# Create-only upload of one report document (see section note above).
def write_report(reports, name, document):
    from azure.storage.blob import ContentSettings

    try:
        reports.get_blob_client(name).upload_blob(json.dumps(document, separators=(',', ':')).encode(), overwrite=False,
                                                content_settings=ContentSettings(content_type='application/json'), timeout=15)
    except Exception as exc:
        if service_code(exc) not in {'BlobAlreadyExists', 'ConditionNotMet'}:
            raise sdk_failure('recovery_report_write', exc) from None


# Insert the Pending ledger row, due after COPY_RECOVERY_DELAY_MINUTES.
def enqueue(table, route, window, failure, settings, now):
    # A single bounded retry budget per route/hour, even across duplicate ADF requests.
    row = {'PartitionKey': route, 'RowKey': window.date_path.replace('/', ''), 'State': 'Pending',
           'DueUtc': now + timedelta(minutes=settings.delay), 'Attempts': 0,
           'WindowStart': window.as_dict()['windowStart'], 'WindowEnd': window.as_dict()['windowEnd'],
           'OriginalRunId': str(window.run_id), 'LastErrorCode': failure['errorCode']}
    try:
        table.create_entity(row)
        return 'created'
    except Exception as exc:
        if status_code(exc) == 409 and service_code(exc) in {'EntityAlreadyExists', 'ResourceAlreadyExists'}:
            return 'already_tracked'
        raise sdk_failure('recovery_queue_write', exc) from None


# Entry point for function_app.copy_terminal_report() (and for recover() after its final
# attempt). payload: window, failure (from safe_failure), attempt, optional reportKind and
# originalRunId. Order: queue the row first (retryable primary failures only), then inspect
# and write the report, so a report-store problem does not prevent recovery. Storage and
# report problems do not raise: it returns {'reportBlob': path} or {} and logs
# recovery_record_failed.
def record_terminal(payload):
    window = Window.parse(payload['window'])
    fields = {**window.as_dict(), 'instanceId': window.instance_id, 'attempt': payload['attempt']}
    failure = payload['failure']
    phase = payload.get('reportKind', 'terminal')
    if failure['errorCode'] == 'worker_outcome_unknown':
        emit('copy_outcome_unknown', logging.ERROR, **fields, **failure)
    try:
        settings = RecoverySettings.read(os.environ)
        if settings is None:
            return {}
        try:
            cfg = Settings.read(os.environ)
        except CopyError:
            cfg = None
        route = recovery_partition(cfg) if cfg is not None else 'unconfigured'
        report = report_path(route, window, phase)
        with recovery_session(settings) as (table, reports, _, __):
            disposition = 'operator_review' if not failure['retryable'] else 'not_queued'
            if phase == 'terminal' and failure['retryable'] and cfg is not None:
                try:
                    result = enqueue(table, route, window, failure, settings, now_utc())
                    disposition = result
                    emit('recovery_queued', **fields, queueResult=result, errorCode=failure['errorCode'])
                except CopyError as exc:
                    disposition = 'queue_write_failed'
                    emit('recovery_record_failed', logging.ERROR, **fields, errorCode=exc.code, **exc.details)
            snapshot = inspection(cfg, window, settings.report_limit) if cfg is not None else {
                'inspection': 'unavailable', 'counts': {}, 'items': {}, 'unverifiedPopulationUnknown': True,
                'checksUnavailable': [{'check': 'settings', 'errorCode': 'invalid_setting'}]}
            document = {'version': 1, **fields, 'recordedAtUtc': now_utc().isoformat(),
                        'failure': failure, 'kind': phase, 'recoveryDisposition': disposition, 'originalPipelineRunId': payload.get('originalRunId', str(window.run_id)),
                        **snapshot}
            write_report(reports, report, document)
        emit('recovery_report_saved', **fields, recoveryReportBlob=report, errorCode=failure['errorCode'])
        return {'reportBlob': report}
    except Exception as exc:
        code = exc.code if isinstance(exc, CopyError) else 'recovery_report_unavailable'
        emit('recovery_record_failed', logging.ERROR, **fields, errorCode=code)
        return {}


# ----------------------------------------------------------------------------
# SECTION 5 - Background recovery (timer-driven)
# function_app.copy_recovery_tick (timer) starts copy_recovery_orchestrator, which calls
# due_keys() once and then recover(key) for each key, one at a time.
# ----------------------------------------------------------------------------
# Return up to COPY_RECOVERY_BATCH_SIZE RowKeys (YYYYMMDDHH) for THIS route that are
# Pending or have an expired Running claim, and whose DueUtc has passed. Empty when automatic
# recovery is disabled.
def due_keys():
    if not env_flag(os.environ, 'COPY_RECOVERY_AUTO_ENABLED', 'false'):
        return []
    settings = RecoverySettings.read(os.environ)
    if settings is None:
        raise CopyError('recovery_storage_required')
    route = recovery_partition(Settings.read(os.environ))
    with recovery_session(settings) as (table, _, __, ___):
        rows = table.query_entities('PartitionKey eq @route and (State eq @pending or State eq @running) and DueUtc le @now',
                                    parameters={'route': route, 'pending': 'Pending', 'running': 'Running', 'now': now_utc()},
                                    select=['RowKey'], results_per_page=settings.batch)
        return [row['RowKey'] for row in islice(rows, settings.batch)]


# Conditional (ETag) update of a ledger row. Returns False if another worker changed or
# removed the row first, which is how competing dispatchers avoid double work.
def transition(table, row, values, mode, condition):
    try:
        table.update_entity({'PartitionKey': row['PartitionKey'], 'RowKey': row['RowKey'], **values},
                            etag=row.metadata['etag'], mode=mode, match_condition=condition)
        return True
    except Exception as exc:
        if status_code(exc) in (404, 412):
            return False
        raise sdk_failure('recovery_queue_update', exc) from None


# Build the Window for a background attempt: same hour, mode 'reconcile', and a new
# deterministic run ID per (route, hour, attempt) for logging and correlation.
def retry_window(row, attempt):
    new_id = uuid5(NAMESPACE, f"{row['PartitionKey']}:{row['RowKey']}:{attempt}")
    return Window.parse({'windowStart': row['WindowStart'], 'windowEnd': row['WindowEnd'],
                         'pipelineRunId': str(new_id), 'mode': 'reconcile'})


# Process one due hour. Steps:
#   1. Re-read the row; skip if it is no longer due or not Pending/Running.
#   2. If the attempt cap is already reached (e.g. an expired final claim), mark Exhausted.
#   3. Claim it: Running + attempt count + ClaimId + DueUtc = now + CLAIM_MINUTES.
#   4. Run copy_reliability.execute() in reconcile mode with _expectedRoute, so a changed
#      configuration fails safely instead of copying a different feed.
#   5. Confirm the claim is still ours, then record Resolved, Pending (with exponential
#      backoff, max 24 h), Exhausted, or OperatorRequired.
#   6. On a final failure, write a 'recovery-terminal' report via record_terminal().
# Returns the new state, or Skipped / ClaimLost.
def recover(key):
    if not re.fullmatch(r'[0-9]{10}', key) or not env_flag(os.environ, 'COPY_RECOVERY_AUTO_ENABLED', 'false'):
        return 'Skipped'
    settings = RecoverySettings.read(os.environ)
    if settings is None:
        raise CopyError('recovery_storage_required')
    cfg = Settings.read(os.environ)
    route = recovery_partition(cfg)
    now = now_utc()
    with recovery_session(settings) as (table, _, mode, condition):
        try:
            row = table.get_entity(route, key)
        except Exception as exc:
            if status_code(exc) == 404 and service_code(exc) in {'ResourceNotFound', 'EntityNotFound'}:
                return 'Skipped'
            raise sdk_failure('recovery_queue_read', exc) from None
        if row['State'] not in ('Pending', 'Running') or row['DueUtc'] > now:
            return 'Skipped'
        attempt = row['Attempts'] + 1
        window = retry_window(row, attempt)
        if window.date_path.replace('/', '') != key or window.end > now:
            raise CopyError('invalid_recovery_window')
        fields = {**window.as_dict(), 'recoveryAttempt': attempt, 'originalPipelineRunId': row['OriginalRunId']}
        if attempt > settings.max_attempts:
            if transition(table, row, {'State': 'Exhausted', 'LastErrorCode': 'recovery_claim_expired'}, mode, condition):
                emit('recovery_exhausted', logging.ERROR, **fields, errorCode='recovery_claim_expired')
            return 'Exhausted'
        token = uuid4().hex
        if not transition(table, row, {'State': 'Running', 'Attempts': attempt, 'ClaimId': token,
                                       'DueUtc': now + timedelta(minutes=CLAIM_MINUTES)}, mode, condition):
            return 'Skipped'
        # Recover by inventory, not by replaying the old report's list. Never replace a target blob.
        failure = None
        result = {}
        try:
            result = execute({**window.as_dict(), '_attempt': 1, '_maxRetries': 0,
                     '_runKind': 'recovery', '_recoveryAttempt': attempt,
                     '_expectedRoute': route, '_originalRunId': row['OriginalRunId']})
        except Exception as exc:
            failure = safe_failure(exc)
        live_row = table.get_entity(route, key)
        if live_row.get('ClaimId') != token or live_row['State'] != 'Running':
            return 'ClaimLost'
        if failure is None:
            if transition(table, live_row, {'State': 'Resolved', 'LastErrorCode': '', 'ResolvedUtc': now_utc()}, mode, condition):
                emit('recovery_resolved', **fields, outcome=result['outcome'])
                return 'Resolved'
            return 'ClaimLost'
        retry = failure['retryable'] and attempt < settings.max_attempts
        state = 'Pending' if retry else 'Exhausted' if failure['retryable'] else 'OperatorRequired'
        updates = {'State': state, 'LastErrorCode': failure['errorCode']}
        if retry:
            updates['DueUtc'] = now_utc() + timedelta(minutes=min(1440, settings.delay * 2 ** (attempt - 1)))
        if not transition(table, live_row, updates, mode, condition):
            return 'ClaimLost'
    emit('recovery_deferred' if retry else 'recovery_exhausted', logging.WARNING if retry else logging.ERROR,
         **fields, **failure)
    # Only the last recovery failure needs a fresh report; original report stays immutable.
    if not retry:
        record_terminal({'window': window.as_dict(), 'failure': failure, 'attempt': attempt,
                         'reportKind': 'recovery-terminal', 'originalRunId': row['OriginalRunId']})
    return state
