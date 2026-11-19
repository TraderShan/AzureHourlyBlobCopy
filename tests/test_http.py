"""Exercise HTTP routing logic with fake Functions decorators/requests and a fake Durable client."""

# ----------------------------------------------------------------------------
# TEST MODULE - test_http.py
# Tests function_app.py without the Azure Functions runtime. setUpClass() loads the file with
# fake 'azure.functions' and 'azure.durable_functions' modules; Durable clients are AsyncMocks,
# and orchestrators are driven step by step as generators (next/send/throw).
# Run all tests from the project root: python -m unittest discover -s tests -v
# ----------------------------------------------------------------------------
import importlib.util
import json
import sys
import unittest
from datetime import timedelta
from copy_job import CopyError
from pathlib import Path
from types import SimpleNamespace, ModuleType
from unittest.mock import AsyncMock, patch

from test_copy_job import PAYLOAD
from copy_job import Window


# Stand-in for df.DFApp: accepts the same constructor argument and turns every decorator
# (route, triggers, durable_client_input) into a no-op so the functions can be called directly.
class FakeApp:
    def __init__(self, **kwargs):
        assert kwargs == {"http_auth_level": "function"}

    def decorator(self, **kwargs):
        return lambda f: f

    route = durable_client_input = orchestration_trigger = activity_trigger = timer_trigger = decorator


# Covers copy_hour (start/no duplicate start/open window), copy_status (allowlisted output),
# the v2 and v3 orchestrator retry paths, the recovery timer, and report path exposure.
class HttpTests(unittest.IsolatedAsyncioTestCase):
    @classmethod
    def setUpClass(cls):
        azure = ModuleType("azure")
        functions = SimpleNamespace(AuthLevel=SimpleNamespace(FUNCTION="function"), HttpRequest=object, TimerRequest=object,
                                    HttpResponse=lambda body, **kwargs: SimpleNamespace(data=json.loads(body), **kwargs))
        durable = SimpleNamespace(DFApp=FakeApp, DurableOrchestrationContext=object)
        path = Path(__file__).resolve().parents[1] / "function_app.py"
        spec = importlib.util.spec_from_file_location("function_app_under_test", path)
        cls.module = importlib.util.module_from_spec(spec)
        with patch.dict(sys.modules, {"azure": azure, "azure.functions": functions, "azure.durable_functions": durable}):
            spec.loader.exec_module(cls.module)

    window = Window.parse(PAYLOAD)

    # Minimal fake HttpRequest with a JSON body and route parameters.
    def request(self, payload=None, instance_id=None):
        return SimpleNamespace(get_json=lambda: payload, route_params={"instance_id": instance_id or self.window.instance_id})

    async def test_missing_falsey_status_starts_copy(self):
        class Absent:
            def __bool__(self):
                return False
        client = SimpleNamespace(get_status=AsyncMock(return_value=Absent()), start_new=AsyncMock(return_value=self.window.instance_id))
        reply = await self.module.copy_hour(self.request(PAYLOAD), client)
        self.assertEqual(reply.status_code, 200)
        self.assertEqual(reply.data, {"instanceId": self.window.instance_id, "accepted": True})
        client.start_new.assert_awaited_once_with("copy_hour_orchestrator", instance_id=self.window.instance_id,
                                                  client_input={**self.window.as_dict(), "_maxRetries": 2, "_retryBaseSeconds": 30, "_recoveryV3": True})

    async def test_resubmission_does_not_restart_visible_instance(self):
        client = SimpleNamespace(get_status=AsyncMock(return_value=object()), start_new=AsyncMock())
        self.assertTrue((await self.module.copy_hour(self.request(PAYLOAD), client)).data["accepted"])
        client.start_new.assert_not_awaited()

    async def test_open_window_is_rejected_before_durable_call(self):
        client = SimpleNamespace(get_status=AsyncMock())
        future = PAYLOAD | {"windowStart": "2099-01-01T14:00:00Z", "windowEnd": "2099-01-01T15:00:00Z"}
        self.assertEqual((await self.module.copy_hour(self.request(future), client)).status_code, 400)
        client.get_status.assert_not_awaited()

    async def test_status_exposes_only_allowed_fields(self):
        result = dict(outcome="NoData", transfersCompleted=0, transfersFailed=0, transfersSkipped=0,
                      bytesTransferred=0, unexpected="sensitive_placeholder")
        state = SimpleNamespace(name="copy_hour_orchestrator", runtime_status=SimpleNamespace(name="Completed"), output=result)
        client = SimpleNamespace(get_status=AsyncMock(return_value=state))
        reply = await self.module.copy_status(self.request(), client)
        self.assertEqual(reply.data["runtimeStatus"], "Completed")
        self.assertNotIn("unexpected", reply.data["result"])
        state.runtime_status.name = "Failed"
        self.assertNotIn("result", (await self.module.copy_status(self.request(), client)).data)
        state.runtime_status.name = "Completed"
        state.output = "unrecognized"
        self.assertEqual((await self.module.copy_status(self.request(), client)).data["runtimeStatus"], "InvalidCopyResult")

    # v2 orchestrator path (no _recoveryV3 flag): backoff timers of 30 s then 60 s.
    def test_durable_only_retries_transient_results_with_backoff(self):
        now = self.window.start
        ctx = SimpleNamespace(get_input=lambda: {**PAYLOAD, "_maxRetries": 2, "_retryBaseSeconds": 30},
                              call_activity=lambda name, data: (name, data["_attempt"]),
                              create_timer=lambda due: due, current_utc_datetime=now, set_custom_status=lambda _: None)
        schedule = self.module.copy_hour_orchestrator(ctx)
        self.assertEqual(next(schedule), ("copy_hour_attempt", 1))
        temporary = {"failure": {"errorCode": "azcopy_transient", "retryable": True}}
        self.assertEqual(schedule.send(temporary), now + timedelta(seconds=30))
        self.assertEqual(schedule.send(None), ("copy_hour_attempt", 2))
        self.assertEqual(schedule.send(temporary), now + timedelta(seconds=60))
        self.assertEqual(schedule.send(None), ("copy_hour_attempt", 3))
        with self.assertRaisesRegex(CopyError, "azcopy_transient"):
            schedule.send(temporary)
        permanent = self.module.copy_hour_orchestrator(ctx)
        next(permanent)
        with self.assertRaisesRegex(CopyError, "source_list_permission"):
            permanent.send({"failure": {"errorCode": "source_list_permission", "retryable": False}})

    # v3 path: the retry switches to reconcile mode and the terminal report keeps the job ID.
    def test_v3_retries_use_reconcile_and_finalizer_keeps_job_id(self):
        custom = []
        ctx = SimpleNamespace(get_input=lambda: {**PAYLOAD, "mode": "copy", "_maxRetries": 1,
                                                 "_retryBaseSeconds": 30, "_recoveryV3": True},
                              call_activity=lambda name, data: (name, data),
                              current_utc_datetime=self.window.start, create_timer=lambda due: due,
                              set_custom_status=custom.append)
        run = self.module.copy_hour_orchestrator(ctx)
        name, initial = next(run)
        self.assertEqual(name, "copy_hour_v3_attempt")
        self.assertEqual(initial["mode"], "copy")
        transient = {"failure": {"retryable": True, "errorCode": "azcopy_transient",
                                 "azcopyJobId": PAYLOAD["pipelineRunId"]}}
        self.assertEqual(run.send(transient), self.window.start + timedelta(seconds=30))
        self.assertEqual(run.send(None)[1]["mode"], "reconcile")
        name, report = run.send({"failure": {"retryable": False, "errorCode": "target_conflicts"}})
        self.assertEqual(name, "copy_terminal_report")
        self.assertEqual(report["failure"]["azcopyJobId"], PAYLOAD["pipelineRunId"])
        self.assertTrue(report["failure"]["jobFromEarlierAttempt"])
        with self.assertRaisesRegex(CopyError, "target_conflicts"):
            run.send({"reportBlob": "safe-report"})
        self.assertEqual(custom[0]["reportBlob"], "safe-report")

    def test_unexpected_activity_failure_creates_non_retryable_diagnostic(self):
        ctx = SimpleNamespace(get_input=lambda: {**PAYLOAD, "mode": "copy", "_maxRetries": 2, "_recoveryV3": True},
                              call_activity=lambda name, data: (name, data), set_custom_status=lambda _: None)
        run = self.module.copy_hour_orchestrator(ctx)
        next(run)
        name, data = run.throw(RuntimeError("internal details"))
        self.assertEqual(name, "copy_terminal_report")
        self.assertEqual(data["failure"], {"errorCode": "worker_outcome_unknown", "retryable": False})
        with self.assertRaisesRegex(CopyError, "worker_outcome_unknown"):
            run.throw(RuntimeError("report failed"))

    async def test_recovery_timer_does_not_launch_overlap(self):
        from unittest.mock import patch
        durable_id = "hourly-copy-recovery-dispatch"
        state = SimpleNamespace(runtime_status=SimpleNamespace(name="Running"))
        client = SimpleNamespace(get_status=AsyncMock(return_value=state), start_new=AsyncMock(return_value=durable_id))
        with patch.dict("os.environ", {"COPY_RECOVERY_AUTO_ENABLED": "true"}):
            await self.module.copy_recovery_tick(None, client)
            client.start_new.assert_not_awaited()
            state.runtime_status.name = "Completed"
            await self.module.copy_recovery_tick(None, client)
        client.start_new.assert_awaited_once_with("copy_recovery_orchestrator", instance_id=durable_id)

    async def test_failure_status_only_exposes_a_scoped_report_key(self):
        state = SimpleNamespace(name="copy_hour_orchestrator", runtime_status=SimpleNamespace(name="Failed"),
                                custom_status={"errorCode": "azcopy_transient", "reportBlob": "https://example.org/?sig=unsafe"})
        client = SimpleNamespace(get_status=AsyncMock(return_value=state))
        reply = await self.module.copy_status(self.request(), client)
        self.assertNotIn("recoveryReportBlob", reply.data)
        state.custom_status["reportBlob"] = "recovery/v1/unconfigured/2026/11/18/14/" + "a" * 32 + "-terminal.json"
        reply = await self.module.copy_status(self.request(), client)
        self.assertEqual(reply.data["recoveryReportBlob"], state.custom_status["reportBlob"])


if __name__ == "__main__":
    unittest.main()
