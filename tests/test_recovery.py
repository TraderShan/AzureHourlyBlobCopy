# ----------------------------------------------------------------------------
# TEST MODULE - test_recovery.py
# Tests copy_recovery.py: terminal reports, the recovery ledger state machine, ETag claims,
# backoff and exhaustion, and the failure snapshot (inspection). Time is simulated by
# patching now_utc(); the Azure Table is the in-memory FakeTable below.
# Run all tests from the project root: python -m unittest discover -s tests -v
# ----------------------------------------------------------------------------
import copy
import json
import os
import unittest
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock, patch

import copy_recovery as cr
from copy_job import CopyError, Settings, Window, recovery_partition
from copy_reliability import BlobFacts
from test_copy_job import PAYLOAD, SETTINGS
from test_reliability import AzureError, Container

# BASE  simulated 'now', days after the test hour (beyond the rolling lookback window)
# ENV   recovery enabled with automatic dispatch
# TEMP  a retryable failure with an AzCopy job ID; FINAL  a permanent conflict
BASE = datetime(2026, 11, 28, 16, tzinfo=timezone.utc)  # Well outside the v2 lookback.
ENV = SETTINGS | {'COPY_RECOVERY_STORAGE_ACCOUNT': 'opsstorage', 'COPY_RECOVERY_AUTO_ENABLED': 'true'}
TEMP = {'errorCode': 'azcopy_transient', 'retryable': True, 'azcopyJobId': PAYLOAD['pipelineRunId']}
FINAL = {'errorCode': 'target_conflicts', 'retryable': False}


# ----------------------------------------------------------------------------
# In-memory Azure Table fake
# Entity carries an ETag in .metadata like the SDK. FakeTable enforces create-once and
# ETag-conditional updates, and implements the due-row query used by due_keys().
# ----------------------------------------------------------------------------
class Entity(dict):
    def __init__(self, data, etag):
        super().__init__(data)
        self.metadata = {'etag': etag}


# Dict-backed table: create once, ETag-checked update, and the due-row query.
class FakeTable:
    def __init__(self):
        self.rows = {}

    def create_entity(self, row):
        key = (row['PartitionKey'], row['RowKey'])
        if key in self.rows:
            raise AzureError(409, 'EntityAlreadyExists')
        self.rows[key] = Entity(row, 1)

    def get_entity(self, part, key):
        if (part, key) not in self.rows:
            raise AzureError(404, 'ResourceNotFound')
        return copy.deepcopy(self.rows[part, key])

    def update_entity(self, values, etag, mode, match_condition):
        key = (values['PartitionKey'], values['RowKey'])
        row = self.rows[key]
        if etag != row.metadata['etag']:
            raise AzureError(412, 'UpdateConditionNotSatisfied')
        self.rows[key] = Entity(row | values, etag + 1)

    def query_entities(self, query, parameters, **kwargs):
        assert 'DueUtc le @now' in query and 'WindowStart' not in query
        return (row for (p, _), row in self.rows.items() if p == parameters['route']
                and row['State'] in (parameters['pending'], parameters['running']) and row['DueUtc'] <= parameters['now'])


# record_terminal(), due_keys(), recover(), transition(), and safe_failure(). setUp patches
# the session, clock, report writer, and inspection() so only ledger logic is tested.
class RecoveryTests(unittest.TestCase):
    def setUp(self):
        self.table, self.report_docs = FakeTable(), {}
        self.window = Window.parse(PAYLOAD)
        self.route = recovery_partition(Settings.read(SETTINGS))
        self.key = self.window.date_path.replace('/', '')
        self.cfg = cr.RecoverySettings.read(ENV)
        self.now = BASE

        @contextmanager
        def session(_):
            yield self.table, object(), 'merge', 'if-not-modified'

        def write(_, name, document):
            self.report_docs.setdefault(name, document)

        for patcher in (patch.dict(os.environ, ENV, clear=True), patch.object(cr, 'recovery_session', session),
                        patch.object(cr, 'now_utc', lambda: self.now), patch.object(cr, 'write_report', write),
                        patch.object(cr, 'inspection', return_value={'inspection': 'complete', 'counts': {}, 'items': {}})):
            patcher.start()
            self.addCleanup(patcher.stop)

    # record(): simulate the orchestrator's final failure. schedule(): record a retryable failure
    # and advance the clock past the 15-minute delay so the hour is due.
    def record(self, failure):
        return cr.record_terminal({'window': PAYLOAD, 'failure': failure, 'attempt': 3})['reportBlob']

    def schedule(self):
        self.record(TEMP)
        self.now += timedelta(minutes=15)

    def test_terminal_report_has_original_failure_and_safe_location(self):
        name = self.record(TEMP)
        report = self.report_docs[name]
        self.assertEqual(report['failure']['azcopyJobId'], PAYLOAD['pipelineRunId'])
        self.assertEqual(report['windowStart'], self.window.as_dict()['windowStart'])
        self.assertEqual(report['pipelineRunId'], PAYLOAD['pipelineRunId'])
        self.assertNotIn('https', name)
        self.record(TEMP)
        self.assertEqual(len(self.table.rows), 1)
        self.assertEqual(len(self.report_docs), 1)
        self.assertEqual(cr.due_keys(), [])
        self.now += timedelta(minutes=15)
        self.assertEqual(cr.due_keys(), [self.key])

    def test_permanent_error_reports_but_never_queues(self):
        self.record(FINAL)
        self.assertFalse(self.table.rows)
        self.assertEqual(len(self.report_docs), 1)

    def test_due_item_survives_lookback_and_rechecks_not_stale_report(self):
        self.schedule()
        self.assertEqual(cr.due_keys(), [self.key])
        with patch.object(cr, 'execute', return_value={'outcome': 'Reconciled'}) as worker:
            self.assertEqual(cr.recover(self.key), 'Resolved')
            self.assertEqual(cr.recover(self.key), 'Skipped')
        inputs = worker.call_args.args[0]
        self.assertEqual(inputs['mode'], 'reconcile')
        self.assertEqual(inputs['_expectedRoute'], self.route)
        self.assertEqual(inputs['_originalRunId'], PAYLOAD['pipelineRunId'])
        self.assertNotIn('items', inputs)
        self.assertEqual(self.table.get_entity(self.route, self.key)['Attempts'], 1)

    def test_exponential_recovery_then_exhaustion(self):
        self.schedule()
        with patch.object(cr, 'execute', side_effect=CopyError('azcopy_transient', retryable=True)):
            self.assertEqual(cr.recover(self.key), 'Pending')
            self.assertEqual(cr.recover(self.key), 'Skipped')
            self.now += timedelta(minutes=15)
            self.assertEqual(cr.recover(self.key), 'Pending')
            self.now += timedelta(minutes=29)
            self.assertEqual(cr.recover(self.key), 'Skipped')
            self.now += timedelta(minutes=1)
            self.assertEqual(cr.recover(self.key), 'Exhausted')
        assert self.table.get_entity(self.route, self.key)['Attempts'] == 3
        self.assertEqual(cr.due_keys(), [])
        self.assertTrue(any(path.endswith('recovery-terminal.json') for path in self.report_docs))
        self.record(TEMP)
        self.assertEqual(self.table.get_entity(self.route, self.key)['State'], 'Exhausted')

    def test_compare_and_swap_rejects_stale_claim_and_changed_route(self):
        self.schedule()
        a, b = [self.table.get_entity(self.route, self.key) for _ in range(2)]
        until = self.now + timedelta(minutes=65)
        self.assertTrue(cr.transition(self.table, a, {'State': 'Running', 'DueUtc': until, 'Attempts': 1}, 'merge', 'etag'))
        self.assertFalse(cr.transition(self.table, b, {'State': 'Running'}, 'merge', 'etag'))
        self.assertEqual(cr.due_keys(), [])
        self.now = until
        self.assertEqual(cr.due_keys(), [self.key])
        with patch.dict(os.environ, {'DEST_PREFIX': 'other'}):
            self.assertEqual(cr.due_keys(), [])
            self.assertEqual(cr.recover(self.key), 'Skipped')

    def test_expired_last_claim_is_exhausted_without_extra_copy(self):
        self.schedule()
        row = self.table.get_entity(self.route, self.key)
        cr.transition(self.table, row, {'State': 'Running', 'DueUtc': self.now, 'Attempts': 3}, 'merge', 'etag')
        with patch.object(cr, 'execute') as worker, self.assertLogs(level='ERROR'):
            self.assertEqual(cr.recover(self.key), 'Exhausted')
            worker.assert_not_called()
        self.assertEqual(self.table.get_entity(self.route, self.key)['LastErrorCode'], 'recovery_claim_expired')

    def test_recovery_conflict_stops_attempts(self):
        self.schedule()
        with patch.object(cr, 'execute', side_effect=CopyError('target_conflicts')):
            self.assertEqual(cr.recover(self.key), 'OperatorRequired')
        self.assertEqual(cr.due_keys(), [])

    def test_report_only_mode_keeps_ledger_but_does_not_dispatch(self):
        with patch.dict(os.environ, {'COPY_RECOVERY_AUTO_ENABLED': 'false'}):
            self.schedule()
            self.assertTrue(self.table.rows)
            self.assertEqual(cr.due_keys(), [])
            self.assertEqual(cr.recover(self.key), 'Skipped')

    def test_report_store_failure_does_not_prevent_enqueue(self):
        with patch.object(cr, 'write_report', side_effect=CopyError('recovery_report_write_permission')):
            self.assertEqual(cr.record_terminal({'window': PAYLOAD, 'failure': TEMP, 'attempt': 3}), {})
        self.assertTrue(self.table.rows)

    def test_error_details_are_allowlisted(self):
        error = CopyError('azcopy_transient', retryable=True, exitCode=1, azcopyJobId=PAYLOAD['pipelineRunId'],
                          unexpected='sensitive_placeholder')
        self.assertEqual(cr.safe_failure(error), TEMP | {'exitCode': 1})


# inspection() against fake containers: destination conflicts vs missing files, unreadable
# listings, missing manifest, plus a static check of monitor/verification-overdue.kql.
class SnapshotTests(unittest.TestCase):
    def setUp(self):
        self.window = Window.parse(PAYLOAD)
        self.prefix = self.window.date_path + '/'
        self.source, self.target = Container(), Container()
        self.source.docs[self.prefix + '_MANIFEST.json'] = json.dumps({'version': 1, 'files': [
            {'path': 'present.txt', 'size': 5}, {'path': 'missing.txt', 'size': 2},
            {'path': 'not-at-source.txt', 'size': 1}]}).encode()
        self.source.files[self.prefix + 'present.txt'] = BlobFacts(5)
        self.source.files[self.prefix + 'missing.txt'] = BlobFacts(2)
        self.target.files[self.prefix + 'present.txt'] = BlobFacts(4)
        self.cfg = Settings.read(SETTINGS | {'COPY_REQUIRE_MANIFEST': 'true'})

        @contextmanager
        def session(_):
            yield self.source, self.target, None, '', ''

        @contextmanager
        def guard(*_):
            yield SimpleNamespace(check=Mock())

        for patcher in (patch.object(cr, 'storage_session', session), patch.object(cr, 'create_guard', guard)):
            patcher.start()
            self.addCleanup(patcher.stop)

    def test_snapshot_distinguishes_destination_conflicts_from_missing_source(self):
        doc = cr.inspection(self.cfg, self.window, 1)
        self.assertEqual(doc['inspection'], 'complete')
        self.assertEqual(doc['counts']['missingAtDestination'], 2)
        self.assertEqual(doc['items']['missingAtDestination'], ['missing.txt'])
        self.assertEqual(doc['items']['conflictsAtDestination'], ['present.txt'])
        self.assertEqual(doc['items']['unverified'], ['not-at-source.txt'])
        self.assertTrue(doc['truncated'])

    def test_unreadable_destination_is_not_reported_as_all_missing(self):
        with patch.object(self.target, 'list_blobs', side_effect=AzureError(403, 'AuthorizationFailure')):
            doc = cr.inspection(self.cfg, self.window, 5)
        self.assertEqual(doc['inspection'], 'partial')
        self.assertNotIn('missingAtDestination', doc['counts'])
        self.assertEqual(doc['counts']['unverified'], 3)

    def test_missing_manifest_identifies_observed_names_as_unverified(self):
        self.source.docs.clear()
        doc = cr.inspection(self.cfg, self.window, 5)
        self.assertEqual(doc['inspection'], 'unavailable')
        self.assertEqual(doc['items']['unverified'], ['missing.txt', 'present.txt'])
        self.assertNotIn('missingAtDestination', doc['counts'])

    def test_monitor_queries_check_each_due_hour_not_just_recent_success(self):
        root = Path(__file__).resolve().parents[1] / 'monitor'
        kql = (root / 'verification-overdue.kql').read_text()
        self.assertIn('range WindowStart from FirstDue to LastDue step 1h', kql)
        self.assertIn('join kind=leftanti', kql)
        self.assertIn("== 'copy_verified'", kql)
        self.assertIn('WindowStart >= ExpectedFrom', kql)
        self.assertIn('param enabled bool = false', (root / 'verification-overdue.bicep').read_text())


if __name__ == '__main__':
    unittest.main()
