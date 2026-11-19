# ----------------------------------------------------------------------------
# TEST MODULE - test_copy_job.py
# Unit tests for copy_job.py (hour windows, settings, AzCopy command and result handling)
# plus checks that the ADF JSON definitions are wired together correctly.
# No Azure access: AzCopy is replaced by small Python scripts written to a temp folder.
# Run all tests from the project root: python -m unittest discover -s tests -v
# ----------------------------------------------------------------------------
import json
import os
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from copy_job import (CopyError, Settings, Window, child_environment,
                      make_command, parse_summary, run_copy, sas_token)

# ----------------------------------------------------------------------------
# Shared test data
# PAYLOAD and SETTINGS (a minimal valid ADF request and Function App settings) and summary()
# (a fake AzCopy 'EndOfJob' line) are imported by test_http.py, test_reliability.py, and
# test_recovery.py. Change them with care.
# ----------------------------------------------------------------------------
PAYLOAD = {"windowStart": "2026-11-18T14:00:00.0000000Z", "windowEnd": "2026-11-18T15:00:00Z",
           "pipelineRunId": "dddddddd-bbbb-aaaa-cccc-eeeeeeeeeeee"}
SETTINGS = {"SOURCE_STORAGE_ACCOUNT": "sourceacct", "SOURCE_CONTAINER": "incoming",
            "DEST_STORAGE_ACCOUNT": "targetacct", "DEST_CONTAINER": "received"}


# Fake AzCopy 'EndOfJob' JSON line; override any field with keyword arguments.
def summary(**kwargs):
    content = dict(JobStatus="Completed", TransfersCompleted="2", TransfersFailed="0",
                   TransfersSkipped="0", TotalBytesTransferred="12") | kwargs
    return json.dumps({"MessageType": "EndOfJob", "MessageContent": json.dumps(content)})


# Window.parse(): UTC conversion, midnight/month/year boundaries, and rejected inputs.
class WindowTests(unittest.TestCase):
    def test_midnight_crosses_year(self):
        data = PAYLOAD | {"windowStart": "2025-12-31T19:00:00-05:00", "windowEnd": "2026-01-01T01:00:00Z"}
        result = Window.parse(data)
        self.assertEqual(result.date_path, "2026/01/01/00")
        self.assertEqual(result.as_dict()["windowStart"], "2026-01-01T00:00:00Z")
        self.assertEqual(Window.parse(result.as_dict()).instance_id, result.instance_id)

    def test_23_hour_crosses_month(self):
        data = PAYLOAD | {"windowStart": "2026-11-30T23:00:00Z", "windowEnd": "2026-12-01T00:00:00Z"}
        self.assertEqual(Window.parse(data).date_path, "2026/11/30/23")

    def test_rejects_missing_timezone_imprecise_or_non_hour(self):
        for value in ("2026-11-18T14:00:00", "2026-11-18T14:01:00Z", "2026-11-18T14:00:00.0000001Z",
                      "2026-11-18T14:00:00+00:60", "0001-01-01T00:00:00+01:00"):
            with self.subTest(value=value), self.assertRaises(ValueError):
                Window.parse(PAYLOAD | {"windowStart": value})

    def test_rejects_invalid_payload_and_nonhour_window(self):
        for invalid in ([], PAYLOAD | {"pipelineRunId": "not-run-id"}, PAYLOAD | {"windowEnd": PAYLOAD["windowStart"]}):
            with self.subTest(value=invalid), self.assertRaises(ValueError):
                Window.parse(invalid)


# Settings.read(), make_command(), child_environment(), sas_token(), parse_summary(), and
# run_copy() against fake AzCopy executables (success, failure classes, timeout).
class CopyTests(unittest.TestCase):
    window = Window.parse(PAYLOAD)

    def test_root_copy_scopes_both_sides_to_hour_directories(self):
        cmd = make_command(Settings.read(SETTINGS), self.window, "azcopy", "", "")
        self.assertEqual(cmd[2:4], ["https://sourceacct.blob.core.windows.net/incoming/2026/11/18/14/",
                                    "https://targetacct.blob.core.windows.net/received/2026/11/18/14/"])
        self.assertIn("--as-subdir=false", cmd)
        self.assertIn("--recursive=true", cmd)
        self.assertIn("--overwrite=false", cmd)
        self.assertEqual(cmd[1], "copy")

    def test_nested_prefix_mirroring_and_encoding(self):
        cfg = Settings.read(SETTINGS | {"SOURCE_PREFIX": r"Level 1\Level#2\Level?3"})
        cmd = make_command(cfg, self.window, "azcopy", "", "")
        self.assertEqual(cfg.source.path(self.window), "Level 1/Level#2/Level?3/2026/11/18/14")
        for url in cmd[2:4]:
            self.assertTrue(url.endswith("Level%201/Level%232/Level%3F3/2026/11/18/14/"))
        cfg = Settings.read(SETTINGS | {"SOURCE_PREFIX": "nested", "DEST_PREFIX": ""})
        self.assertEqual(cfg.destination.path(self.window), self.window.date_path)

    def test_rejects_wrong_targets_wildcard_overwrite_timeout_and_bad_vault(self):
        for override in ({"DEST_STORAGE_ACCOUNT": "sourceacct", "DEST_CONTAINER": "incoming"},
                         {"SOURCE_PREFIX": "level/*"}, {"COPY_OVERWRITE": "prompt"}, {"AZCOPY_TIMEOUT_SECONDS": "4000"},
                         {"COPY_AUTH_MODE": "key_vault_sas", "COPY_KEY_VAULT_URL": "https://example.org/"}):
            with self.subTest(which=list(override)), self.assertRaises(CopyError):
                Settings.read(SETTINGS | override)

    def test_sas_mode_does_not_forward_host_credentials(self):
        cfg = Settings.read(SETTINGS | {"COPY_AUTH_MODE": "key_vault_sas",
                                        "COPY_KEY_VAULT_URL": "https://sample-vault.vault.azure.net/",
                                        "COPY_SOURCE_SAS_SECRET": "src", "COPY_DEST_SAS_SECRET": "dst"})
        with patch.dict(os.environ, {"IDENTITY_HEADER": "placeholder", "AzureWebJobsStorage": "placeholder"}):
            env = child_environment(cfg, "/tmp/fake")
        self.assertNotIn("IDENTITY_HEADER", env)
        self.assertNotIn("AzureWebJobsStorage", env)
        self.assertNotIn("AZCOPY_AUTO_LOGIN_TYPE", env)
        token = sas_token("?sv=fake&sig=fake%2Fvalue%3D", "source")
        self.assertTrue(make_command(cfg, self.window, "azcopy", token, "")[2].endswith("?" + token))
        with self.assertRaisesRegex(CopyError, "invalid_sas_secret"):
            sas_token("https://example.org/?sig=not-a-token", "source")

    def test_managed_identity_environment(self):
        cfg = Settings.read(SETTINGS | {"COPY_MI_CLIENT_ID": PAYLOAD["pipelineRunId"]})
        with patch.dict(os.environ, {"IDENTITY_ENDPOINT": "placeholder"}):
            env = child_environment(cfg, "/tmp/fake")
        self.assertEqual(env["IDENTITY_ENDPOINT"], "placeholder")
        self.assertEqual(env["AZCOPY_AUTO_LOGIN_TYPE"], "MSI")
        self.assertEqual(env["AZCOPY_MSI_CLIENT_ID"], PAYLOAD["pipelineRunId"])

    def test_summary_success_skip_failure_and_missing(self):
        self.assertEqual(parse_summary(["noise", summary()])["bytesTransferred"], 12)
        skip = summary(JobStatus="CompletedWithSkipped", TransfersCompleted=0, TransfersSkipped="2", TotalBytesTransferred="0")
        self.assertEqual(parse_summary([skip])["transfersSkipped"], 2)
        for lines, code in (([summary(JobStatus="CompletedWithErrors", TransfersFailed=1)], "azcopy_job_failed"),
                            ([summary(JobStatus="Failed")], "azcopy_job_failed"),
                            ([summary(TransfersCompleted=0)], "azcopy_no_transfers"),
                            ([summary(TransfersSkipped=-1)], "azcopy_invalid_summary"),
                            (["not a summary"], "azcopy_missing_summary")):
            with self.subTest(code=code), self.assertRaisesRegex(CopyError, code):
                parse_summary(lines)

    # fake() writes a tiny Python script that stands in for the AzCopy binary.
    def test_subprocess_shim_success_nonzero_timeout_and_missing_binary(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "azcopy-stub"
            cfg = Settings.read(SETTINGS | {"AZCOPY_PATH": str(path), "AZCOPY_TIMEOUT_SECONDS": "1"})
            with self.assertRaisesRegex(CopyError, "azcopy_not_executable"):
                run_copy(cfg, self.window, "", "")

            def fake(source):
                path.write_text(f"#!{sys.executable}\n{source}\n", encoding="utf-8")
                path.chmod(0o700)

            fake(f"print({summary()!r})")
            self.assertEqual(run_copy(cfg, self.window, "", "")["transfersCompleted"], 2)
            fake(f"print({summary(JobStatus='Failed', TransfersFailed=1)!r})\nraise SystemExit(1)")
            with self.assertRaisesRegex(CopyError, "azcopy_unclassified") as error:
                run_copy(cfg, self.window, "", "")
            self.assertEqual(error.exception.details["transfersFailed"], 1)
            fake("import sys\nprint('RESPONSE 503', file=sys.stderr)\nraise SystemExit(1)")
            with self.assertRaisesRegex(CopyError, "azcopy_transient") as error:
                run_copy(cfg, self.window, "", "")
            self.assertTrue(error.exception.retryable)
            fake("import sys\nprint('ERROR CODE: AuthorizationPermissionMismatch', file=sys.stderr)\nraise SystemExit(1)")
            with self.assertRaisesRegex(CopyError, "azcopy_permanent") as error:
                run_copy(cfg, self.window, "", "")
            self.assertFalse(error.exception.retryable)
            fake("import time\ntime.sleep(5)")
            with self.assertRaisesRegex(CopyError, "azcopy_timeout"):
                run_copy(cfg, self.window, "", "")


# Static checks of adf/*.json: hourly trigger -> pipeline parameters, run ID in the start
# body, Until polling loop, Fail activity, and the reconcile schedule's child parameters.
class ADFTests(unittest.TestCase):
    def test_trigger_pipeline_refs_and_failure_logic(self):
        root = Path(__file__).resolve().parents[1] / "adf"
        trigger = json.loads((root / "tr_hourly_blob_copy.json").read_text())["properties"]
        pipeline = json.loads((root / "pl_hourly_blob_copy.json").read_text())
        self.assertEqual((trigger["typeProperties"]["frequency"], trigger["typeProperties"]["interval"]), ("Hour", 1))
        self.assertEqual(trigger["pipeline"]["pipelineReference"]["referenceName"], pipeline["name"])
        self.assertEqual(trigger["typeProperties"]["retryPolicy"]["count"], 0)
        for name in ("windowStart", "windowEnd"):
            self.assertIn(name, pipeline["properties"]["parameters"])
            self.assertEqual(trigger["pipeline"]["parameters"][name]["value"], f"@trigger().outputs.{name}Time")
        start, poll, require = pipeline["properties"]["activities"]
        self.assertIn("pipeline().RunId", start["typeProperties"]["body"]["value"])
        self.assertEqual(poll["type"], "Until")
        self.assertEqual(require["typeProperties"]["ifFalseActivities"][0]["type"], "Fail")

    def test_reconciliation_schedule_starts_child_in_reconcile_mode(self):
        root = Path(__file__).resolve().parents[1] / "adf"
        trigger = json.loads((root / "tr_reconcile_hourly_blob_copy.json").read_text())["properties"]
        parent = json.loads((root / "pl_reconcile_hourly_blob_copy.json").read_text())
        binding = trigger["pipelines"][0]
        self.assertEqual(binding["pipelineReference"]["referenceName"], parent["name"])
        self.assertEqual(binding["parameters"]["scheduledTime"]["value"], "@trigger().scheduledTime")
        self.assertEqual(binding["parameters"]["firstHourOffset"], 2)
        loop = parent["properties"]["activities"][1]["typeProperties"]
        self.assertIn("lookbackHours", loop["items"]["value"])
        child = loop["activities"][0]["typeProperties"]
        self.assertTrue(child["waitOnCompletion"])
        self.assertEqual(child["parameters"]["mode"], "reconcile")
        self.assertIn("scheduledTime", child["parameters"]["windowStart"]["value"])


if __name__ == "__main__":
    unittest.main()
