"""Exercise the actual main, adapter and integer single-instance boundary."""
from datetime import datetime, timedelta, timezone
import importlib
import json
import sys
from types import SimpleNamespace

import pytest
import startup_observation as observation
from tests.test_screen_precreation_recovery import application, shell_runtime

REAL_OBSERVE = observation.observe_startup


@pytest.mark.parametrize("outcome,expected", [("web", 0), ("exit", 3)])
def test_shell_close_preserves_start_check_and_returns_integer(
        application, shell_runtime, monkeypatch, tmp_path, outcome, expected):
    module, calls = application
    host, _, _ = shell_runtime
    monkeypatch.setitem(sys.modules, "startup_observation", observation)
    monkeypatch.setattr(observation, "observe_startup", REAL_OBSERVE)
    directory = tmp_path / "start-check"
    directory.mkdir()
    monkeypatch.setattr(observation, "START_CHECK_DIRECTORY", directory)
    date = datetime.now(timezone(timedelta(hours=9))).strftime("%Y%m%d")
    path = directory / (date + "-LABEL.json")
    captured = []

    def run_app(*args, **kwargs):
        # This is the startup observation already made before the shell closes.
        REAL_OBSERVE(())
        captured.append(path.read_bytes())
        return outcome

    host.run_app = run_app
    single = importlib.import_module("label_match_single_instance")
    monkeypatch.setattr(single, "_write_instance_owner_notice", lambda lease: None)
    monkeypatch.setattr(single, "_try_update_shutdown_stall_notice", lambda *a, **k: None)
    lease = SimpleNamespace(owner=True, close=lambda: calls.append("lease_closed"))
    from tests._startup_timing_fixture import startup_body_namespace
    namespace = startup_body_namespace(module._test_main)
    namespace["run_guarded_entrypoint"] = lambda start, **kw: single.run_guarded_entrypoint(
        start, **kw, acquire=lambda scope: lease)
    result = module._test_main([])
    assert type(result) is int and result == expected
    assert len(captured) == 1 and path.read_bytes() == captured[0]
    assert "UNKNOWN_CHECK" not in json.loads(path.read_bytes())["reasons"]
    assert "main_exception" not in calls
    assert calls.count("lease_closed") == 1
