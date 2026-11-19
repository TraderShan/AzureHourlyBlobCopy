# ----------------------------------------------------------------------------
# TEST MODULE - test_reliability.py
# Tests copy_reliability.py: strict manifest verification, reconciliation, conflicts, empty
# hours, recovery runs, the destination lease, and selected-file AzCopy runs.
# Storage is simulated in memory; AzCopy is either patched or a fake script.
# Run all tests from the project root: python -m unittest discover -s tests -v
# ----------------------------------------------------------------------------
import hashlib
import json
import os
import sys
import tempfile
import unittest
from contextlib import contextmanager
from pathlib import Path
from time import monotonic
from types import SimpleNamespace
from unittest.mock import Mock, patch

from copy_job import CopyError, Settings, Window, run_copy
from copy_reliability import (BlobFacts, EMPTY_STATS, LeaseGuard, differences, execute, lock_name,
                              parse_manifest, sdk_failure, transfer)
from test_copy_job import PAYLOAD, SETTINGS, summary


# ----------------------------------------------------------------------------
# In-memory Azure fakes (also imported by test_recovery.py)
# AzureError  mimics an SDK exception: status_code and error_code only, untrusted message
# Container   mimics ContainerClient: 'docs' hold small documents (manifest, _SUCCESS,
#             reports) and 'files' hold listed blobs as BlobFacts; uploads never overwrite
# ----------------------------------------------------------------------------
class AzureError(Exception):
    def __init__(self, status, code):
        self.status_code, self.error_code = status, code
        super().__init__("untrusted Azure diagnostic")


# Fake ContainerClient; see the section note above.
class Container:
    def __init__(self):
        self.docs = {}
        self.files = {}
        self.prefixes = []

    def get_blob_client(self, path):
        def download(**_):
            if path not in self.docs:
                raise AzureError(404, "BlobNotFound")
            return SimpleNamespace(readall=lambda: self.docs[path])
        def upload(content, **options):
            assert options["overwrite"] is False
            if path in self.docs:
                raise AzureError(409, "BlobAlreadyExists")
            self.docs[path] = content
        return SimpleNamespace(download_blob=download, upload_blob=upload)

    def list_blobs(self, **kwargs):
        prefix = kwargs["name_starts_with"]
        self.prefixes.append(prefix)
        items = [SimpleNamespace(name=name, metadata={}, size=f.size, etag=f.etag,
                                 content_settings=SimpleNamespace(content_md5=f.md5))
                 for name, f in self.files.items() if name.startswith(prefix)]
        return SimpleNamespace(by_page=lambda: iter([items]))


# transfer()/execute() with storage_session, create_guard, run_copy, and publish_marker
# patched. The fake run_copy copies the requested files and records each selection in
# self.copy_calls, so tests can assert exactly which files were copied.
class WorkerTests(unittest.TestCase):
    def setUp(self):
        self.source, self.target = Container(), Container()
        self.cfg = Settings.read(SETTINGS | {"COPY_REQUIRE_MANIFEST": "true", "COPY_OVERWRITE": "true"})
        self.window = Window.parse(PAYLOAD)
        self.prefix = self.window.date_path + "/"
        self.guard = SimpleNamespace(check=Mock())
        self.copy_calls = []

        @contextmanager
        def session(_):
            yield self.source, self.target, None, "", ""

        @contextmanager
        def guard(*_):
            yield self.guard

        def azcopy(*_, paths, **_kw):
            self.copy_calls.append(set(paths))
            for relative in paths:
                self.target.files[self.prefix + relative] = self.source.files[self.prefix + relative]
            return {**EMPTY_STATS, "transfersCompleted": len(paths)}

        def marker(target, path, doc, *_):
            if path in target.docs:
                raise AssertionError("Marker is not write-once")
            target.docs[path] = json.dumps(doc).encode()

        for dst, fn in (("storage_session", session), ("create_guard", guard), ("run_copy", azcopy), ("publish_marker", marker)):
            patcher = patch("copy_reliability." + dst, fn)
            patcher.start()
            self.addCleanup(patcher.stop)

    # Helpers: start() runs one transfer for the test hour; manifest() writes _MANIFEST.json.
    def start(self, cfg=None, mode="copy"):
        return transfer(cfg or self.cfg, Window.parse(PAYLOAD | {"mode": mode}), monotonic() + 300,
                        self.window.as_dict())

    def manifest(self, *entries):
        path = self.prefix + "_MANIFEST.json"
        self.source.docs[path] = json.dumps({"version": 1, "files": list(entries)}).encode()
        return path

    def test_strict_copies_only_missing_and_commits_marker_after_verified(self):
        m = self.manifest({"path":"present.csv","size":1}, {"path":"nested/new.csv","size":2})
        self.source.files[self.prefix + "present.csv"] = BlobFacts(1, etag='s1')
        self.target.files[self.prefix + "present.csv"] = BlobFacts(1, etag='d1')
        self.source.files[self.prefix + "nested/new.csv"] = BlobFacts(2, etag='s2')
        result = self.start()
        self.assertEqual(self.copy_calls, [{"nested/new.csv"}])
        self.assertEqual((result["outcome"], result["verifiedFiles"]), ("Verified", 2))
        marker = json.loads(self.target.docs[self.prefix + "_SUCCESS"])
        self.assertEqual(marker["manifestSha256"], hashlib.sha256(self.source.docs[m]).hexdigest())
        self.assertEqual(self.start()["transfersCompleted"], 0)
        self.assertEqual(self.copy_calls, [{"nested/new.csv"}])
        self.assertTrue(all(p == self.prefix for p in self.source.prefixes + self.target.prefixes))

    def test_no_marker_for_unready_or_partial_manifest(self):
        with self.assertRaisesRegex(CopyError, "manifest_not_ready") as cm:
            self.start()
        self.assertTrue(cm.exception.retryable)
        self.manifest({"path":"absent.csv","size":1})
        with self.assertRaisesRegex(CopyError, "source_manifest_files_missing"):
            self.start()
        self.assertEqual(self.target.docs, {})
        self.assertFalse(self.copy_calls)

    def test_target_conflict_prevents_strict_copy_or_marker(self):
        self.manifest({"path":"changed.csv","size":2}, {"path":"missing.csv","size":1})
        self.source.files.update({self.prefix + "changed.csv": BlobFacts(2), self.prefix + "missing.csv": BlobFacts(1)})
        self.target.files[self.prefix + "changed.csv"] = BlobFacts(1)
        with self.assertLogs(level="WARNING"), self.assertRaisesRegex(CopyError, "target_conflicts"):
            self.start()
        self.assertEqual(self.target.docs, {})
        self.assertFalse(self.copy_calls)

    def test_reconcile_copies_missing_reports_changed_and_never_marks_verified(self):
        cfg = Settings.read(SETTINGS | {"COPY_OVERWRITE": "true"})
        self.source.files.update({self.prefix + "changed.csv": BlobFacts(2), self.prefix + "missing.csv": BlobFacts(1)})
        self.target.files[self.prefix + "changed.csv"] = BlobFacts(1)
        with self.assertLogs(level="WARNING") as log, self.assertRaisesRegex(CopyError, "target_conflicts"):
            self.start(cfg=cfg, mode="reconcile")
        self.assertIn('"changedCount":1', log.output[0])
        self.assertEqual(self.copy_calls, [{"missing.csv"}])
        self.assertEqual(self.target.files[self.prefix + "changed.csv"].size, 1)
        self.assertFalse(self.target.docs)
        self.source.files.pop(self.prefix + "changed.csv")
        result = self.start(cfg=cfg, mode="reconcile")
        self.assertEqual((result["outcome"], result["missingBefore"]), ("Reconciled", 0))
        self.assertEqual(len(self.copy_calls), 1)

    def test_recovery_rechecks_live_destination_and_copies_only_missing(self):
        # An old failure report might have listed both files; one has since landed.
        for name in ("landed.csv", "still-missing.csv"):
            self.source.files[self.prefix + name] = BlobFacts(1)
        self.target.files[self.prefix + "landed.csv"] = BlobFacts(1)
        cfg = Settings.read(SETTINGS | {"COPY_OVERWRITE": "true"})
        with patch.dict(os.environ, SETTINGS | {"COPY_OVERWRITE": "true"}, clear=True):
            output = execute(PAYLOAD | {"mode": "reconcile", "_runKind": "recovery"})
        self.assertEqual(self.copy_calls, [{"still-missing.csv"}])
        self.assertEqual(output["missingBefore"], 1)
        self.assertFalse(cfg.manifest_required)

    def test_recovery_unobserved_source_is_not_success(self):
        with (patch.dict(os.environ, SETTINGS, clear=True), self.assertLogs(level="WARNING"),
              self.assertRaisesRegex(CopyError, "recovery_source_not_observed") as failure):
            execute(PAYLOAD | {"mode": "reconcile", "_runKind": "recovery"})
        self.assertTrue(failure.exception.retryable)
        self.assertFalse(self.copy_calls)

    def test_conflict_report_is_optional_and_names_stay_out_of_telemetry(self):
        cfg = Settings.read(SETTINGS | {"COPY_WRITE_CONFLICT_REPORT": "true"})
        name = "confidential/path.csv"
        self.source.files[self.prefix + name] = BlobFacts(8)
        self.target.files[self.prefix + name] = BlobFacts(1)
        with self.assertLogs(level="WARNING") as log, self.assertRaisesRegex(CopyError, "target_conflicts") as failure:
            self.start(cfg=cfg, mode="reconcile")
        report = failure.exception.details["conflictReportBlob"]
        self.assertNotIn(name, " ".join(log.output))
        data = json.loads(self.target.docs[report])
        self.assertEqual(data["files"], [{"path": name, "reason": "changed", "sourceSize": 8, "targetSize": 1}])

    def test_empty_window_contracts(self):
        self.assertEqual(self.start(cfg=Settings.read(SETTINGS))["outcome"], "NoData")
        with self.assertRaisesRegex(CopyError, "empty_source_window"):
            self.start(cfg=Settings.read(SETTINGS | {"COPY_ALLOW_EMPTY": "false"}))
        self.manifest()
        self.assertEqual(self.start()["verifiedFiles"], 0)
        self.assertIn(self.prefix + "_SUCCESS", self.target.docs)
        self.assertFalse(self.copy_calls)
        with self.assertRaisesRegex(CopyError, "published_window_requires_manifest"):
            self.start(cfg=Settings.read(SETTINGS), mode="reconcile")

    def test_published_window_rejects_changed_manifest(self):
        self.manifest()
        self.start()
        existing = dict(self.target.docs)
        self.manifest({"path":"late.csv","size":3})
        with self.assertRaisesRegex(CopyError, "published_manifest_conflict"):
            self.start()
        self.assertEqual(self.target.docs, existing)

    def test_source_change_during_copy_does_not_publish(self):
        self.manifest({"path":"data.csv","size":2})
        path = self.prefix + "data.csv"
        self.source.files[path] = BlobFacts(2, etag="v1")

        def fake(*_, **_kw):
            self.target.files[path] = BlobFacts(2)
            self.source.files[path] = BlobFacts(2, etag="v2")
            return EMPTY_STATS

        with patch("copy_reliability.run_copy", fake), self.assertRaisesRegex(CopyError, "source_changed_during_verification"):
            self.start()
        self.assertFalse(self.target.docs)

    def test_reports_only_safe_attempt_failure(self):
        with (patch.dict(os.environ, SETTINGS, clear=True),
              patch("copy_reliability.transfer", side_effect=CopyError("source_list_transient", retryable=True)),
              self.assertLogs(level="WARNING") as logs,
              self.assertRaisesRegex(CopyError, "source_list_transient")):
            execute(PAYLOAD | {"_attempt": 1, "_maxRetries": 2})
        self.assertIn("copy_attempt_failed", logs.output[0])
        with (patch.dict(os.environ, SETTINGS, clear=True),
              patch("copy_reliability.transfer", side_effect=RuntimeError("sensitive_placeholder")),
              self.assertLogs(level="ERROR") as logs,
              self.assertRaisesRegex(CopyError, "worker_error")):
            execute(PAYLOAD)
        self.assertNotIn("sensitive_placeholder", str(logs.output))


# LeaseGuard behavior, SDK error classification, exact --list-of-files arguments, stopping
# AzCopy when the lease is lost, and manifest schema validation.
class LeaseAndSelectionTests(unittest.TestCase):
    def test_lease_conflict_and_lost_lease(self):
        now = monotonic()
        busy = SimpleNamespace(acquire=Mock(side_effect=AzureError(409,"LeaseAlreadyPresent")))
        with self.assertRaisesRegex(CopyError, "window_locked") as cm:
            LeaseGuard(busy, now + 100).__enter__()
        self.assertTrue(cm.exception.retryable)
        available = Mock()
        with LeaseGuard(available, now + 100) as guard:
            guard.check()
            guard.lost.set()
            with self.assertRaisesRegex(CopyError, "window_lease_lost"):
                guard.check()
        available.acquire.assert_called_once_with(lease_duration=60, timeout=10)
        available.release.assert_called_once()
        self.assertNotEqual(lock_name("2026/11/18/14"), lock_name("2026/11/18/15"))

    def test_sdk_retries_only_transient(self):
        self.assertTrue(sdk_failure("source_list", AzureError(429, "ServerBusy")).retryable)
        self.assertFalse(sdk_failure("source_list", AzureError(403, "AuthorizationFailure")).retryable)

    def test_targeted_argv_uses_exact_relative_list_and_forces_no_overwrite(self):
        with tempfile.TemporaryDirectory() as directory:
            exe = Path(directory) / "azcopy-stub"
            payload = summary(JobID="dddddddd-bbbb-aaaa-cccc-eeeeeeeeeeee")
            exe.write_text(f'''#!{sys.executable}
import sys
from pathlib import Path
flag = next(arg for arg in sys.argv if arg.startswith('--list-of-files='))
assert Path(flag.split('=', 1)[1]).read_text() == 'nested/file one.csv\\nother.csv\\n'
assert '--overwrite=false' in sys.argv
assert '--recursive=false' in sys.argv
print({payload!r})
''')
            exe.chmod(0o700)
            cfg = Settings.read(SETTINGS | {"AZCOPY_PATH": str(exe), "COPY_OVERWRITE": "true"})
            result = run_copy(cfg, Window.parse(PAYLOAD), "", "", paths={"other.csv", "nested/file one.csv"})
            self.assertEqual(result["azcopyJobId"], PAYLOAD["pipelineRunId"])

    def test_lost_lease_interrupts_process(self):
        with tempfile.TemporaryDirectory() as directory:
            exe = Path(directory) / "azcopy-stub"
            exe.write_text(f"#!{sys.executable}\nimport time\ntime.sleep(15)\n")
            exe.chmod(0o700)
            cfg = Settings.read(SETTINGS | {"AZCOPY_PATH": str(exe)})
            cutoff = monotonic() + 0.3
            def check():
                if monotonic() > cutoff:
                    raise CopyError("window_lease_lost", retryable=True)
            with self.assertRaisesRegex(CopyError, "window_lease_lost"):
                run_copy(cfg, Window.parse(PAYLOAD), "", "", check=check)
            self.assertLess(monotonic() - cutoff, 4)

    def test_manifest_schema_and_optional_md5(self):
        for invalid in ({"path":"../../outside","size":1}, {"path":"x","size":True}, {"path":"_SUCCESS","size":1},
                        {"path":"x","size":1,"contentMd5":"not valid"}):
            with self.subTest(invalid=invalid), self.assertRaisesRegex(CopyError, "invalid_source_manifest"):
                parse_manifest(json.dumps({"version":1,"files":[invalid]}), 3, {"_SUCCESS"})
        expected = parse_manifest(b'{"version":1,"files":[{"path":"zero","size":0,"contentMd5":"1B2M2Y8AsgTpgAmY7PhCfg=="}]}', 3, set())
        self.assertEqual(differences(expected, {"zero":BlobFacts(0)}, True)[1], {"zero"})
        self.assertFalse(differences(expected, {"zero":BlobFacts(0)}, False)[1])


if __name__ == "__main__":
    unittest.main()
