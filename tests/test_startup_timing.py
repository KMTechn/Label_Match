"""Startup observations retain the existing admission and refusal behavior."""
import json
from types import SimpleNamespace

import pytest

import auto_update as updates
import current_user_onboarding as onboarding
import startup_timing as timing
import user_relay
from tests.test_window_first_startup import steady_start, reset_window_start
from tests.test_onboarding_lifecycle_scope import managed_onboarding
from tests.test_current_user_onboarding import isolated_data_generation_release
from tests.test_onboarding_writer_admission import _onboard


def _change(path, **fields):
    record = json.loads(path.read_bytes())
    record.update(fields)
    path.write_text(json.dumps(record), encoding="utf-8")


@pytest.mark.parametrize("code", [
    "ARGUMENTS", "CONTROL_ABSENT", "VERSION_LAYOUT", "ORDINARY_USER_CONTEXT",
    "ACTIVE_SCHEMA", "TRANSITION_SCHEMA", "TRANSITION_PHASE", "APPLY_CLAIM",
    "PREPARED_PENDING", "STARTUP_PROTOCOL", "ACTIVE_BUILD", "VERSION_DIRECTORY",
    "RECOVERY_IDENTITY", "RELAY_STOP", "SEND_HOLD", "ONBOARDING_STATUS",
    "ONBOARDING_SCOPE", "ONBOARDING_PATH", "REQUIRED_FILE", "REGISTRATION_UNVERIFIED",
    "REGISTRATION_ORIGIN", "INPUT_SIZE", "RELAY_OWNER_ABSENT", "RELAY_NOT_FAST",
    "INPUT_UNREADABLE",
])
def test_fast_miss_records_one_fixed_code_without_input_values(steady_start, monkeypatch, code):
    from startup_send_gate import create_startup_send_hold
    item, traces = steady_start, []
    paths, control = item.paths, item.control
    root, arguments = paths.app_root, ()
    if code == "ARGUMENTS":
        arguments = ("untrusted-input",)
    elif code == "CONTROL_ABSENT":
        monkeypatch.setattr(updates, "control_root", lambda _: None)
    elif code == "VERSION_LAYOUT":
        root = control.parent / "current"
    elif code == "ORDINARY_USER_CONTEXT":
        monkeypatch.setattr(updates, "ordinary_user_context", lambda *_: False)
    elif code == "ACTIVE_SCHEMA":
        _change(control / "active.json", schema="untrusted-input")
    elif code == "TRANSITION_SCHEMA":
        _change(control / "transition.json", schema="untrusted-input")
    elif code == "TRANSITION_PHASE":
        _change(control / "transition.json", phase="RUNNING")
    elif code == "APPLY_CLAIM":
        (control / "apply-claim.json").write_text("{}")
    elif code == "PREPARED_PENDING":
        monkeypatch.setattr(updates, "prepared_pending", lambda *_: True)
    elif code == "STARTUP_PROTOCOL":
        _change(root / "portable-manifest.json", startup_update_protocol=0)
    elif code in {"ACTIVE_BUILD", "VERSION_DIRECTORY"}:
        _change(control / "active.json", sha="b" * 40)
        if code == "VERSION_DIRECTORY":
            import label_data_generation
            monkeypatch.setattr(label_data_generation, "portable_release_identity", lambda _: {"build_id": "b" * 40})
    elif code == "RECOVERY_IDENTITY":
        _change(control / "transition.json", candidate_sha="b" * 40)
        monkeypatch.setattr(updates, "recovery_sha", lambda *_a, **_k: "b" * 40)
    elif code == "RELAY_STOP":
        stop = user_relay.user_relay_stop_path(paths.direct_sync_root)
        stop.parent.mkdir(parents=True, exist_ok=True)
        stop.write_text("untrusted-input")
    elif code == "SEND_HOLD":
        create_startup_send_hold(paths.direct_sync_root)
    elif code == "ONBOARDING_STATUS":
        _change(paths.onboarding_report_path, status="untrusted-input")
    elif code == "ONBOARDING_SCOPE":
        _change(paths.onboarding_report_path, state_scope="untrusted-input")
    elif code == "ONBOARDING_PATH":
        _change(paths.onboarding_report_path, data_root=str(paths.data_root / "untrusted-input"))
    elif code == "REQUIRED_FILE":
        paths.settings_path.unlink()
    elif code == "REGISTRATION_UNVERIFIED":
        _change(paths.registration_report_path, server_registration_verified=False)
    elif code == "REGISTRATION_ORIGIN":
        _change(paths.registration_report_path, server_base_url="https://untrusted.invalid")
        _change(paths.logistics_profile_path, base_url="https://different.invalid")
    elif code == "INPUT_SIZE":
        paths.identity_path.write_bytes(b" " * (onboarding.BOOTSTRAP_INTEGRITY_MAX_BYTES + 1))
    elif code == "RELAY_OWNER_ABSENT":
        monkeypatch.setattr(user_relay, "_existing_user_relay_owner", lambda *_a, **_k: None)
    elif code == "RELAY_NOT_FAST":
        monkeypatch.setattr(user_relay, "_existing_user_relay_owner", lambda *_a, **_k: {"fast_path": False})
    else:
        def unreadable(*_a, **_k):
            raise ValueError("untrusted-input")
        monkeypatch.setattr(onboarding, "_read_json", unreadable)
    with timing.startup_timing_scope(lambda stage, **details: traces.append((stage, details))):
        assert onboarding.try_window_first_start(root, environ=item.environment, arguments=arguments) is None
    assert len(traces) == 1
    stage, details = traces[0]
    assert stage == "fast_window_first_miss"
    assert set(details) == {"result_code", "elapsed_ms"}
    assert details["result_code"] == code
    assert isinstance(details["elapsed_ms"], (float, int)) and details["elapsed_ms"] >= 0
    assert "untrusted-input" not in json.dumps(traces)
    assert item.helpers == []


def test_full_start_records_each_stage_without_changing_helper_sequence(managed_onboarding):
    paths, environment, _control, helpers = managed_onboarding
    traces = []
    with timing.startup_timing_scope(lambda stage, **details: traces.append((stage, details))):
        report = _onboard(paths, environment, relay_launcher=lambda _: {"status": "ALIVE"})
    assert report["status"] == "READY"
    assert [row["mode"] for row in helpers] == ["Verify", "ObserveLifecycle"]
    expected = {"full_lifecycle_verify", "full_observe_lifecycle",
                "full_legacy_task_query", "full_bootstrap_bytes", "full_data_generation",
                "full_db_open", "full_task_retirement", "full_relay_reuse"}
    assert expected <= {stage for stage, _ in traces}
    for stage, details in traces:
        assert stage in expected
        assert set(details) == {"result_code", "elapsed_ms"}
        assert details["result_code"] == "RETURNED" and details["elapsed_ms"] >= 0


def test_inventory_timing_uses_the_real_inventory_reader(tmp_path):
    bundle = tmp_path / "bundle"
    (bundle / "app").mkdir(parents=True)
    (bundle / "app/sink.py").write_text(
        'from writer_session_fence import writer_sink\n'
        '@writer_sink("fixture_sink")\ndef write():\n    pass\n')
    (bundle / "INSTALL_THIS_PC.ps1").write_text("")
    events = []
    with timing.startup_timing_scope(lambda stage, **row: events.append((stage, row))):
        digest = updates.version_writer_inventory_sha256(bundle)
    assert len(digest) == 64
    assert len(events) == 1 and events[0][0] == "full_inventory"
    assert events[0][1]["result_code"] == "RETURNED"


def test_timing_preserves_return_exception_and_callback_failure(monkeypatch):
    ticks = iter([10.0, 10.25, 20.0, 20.5, 30.0, 30.25])
    monkeypatch.setattr(timing, "time", SimpleNamespace(monotonic=lambda: next(ticks)))
    events = []
    sentinel = object()
    @timing.startup_timed("full_db_open")
    def run(fail=False):
        if fail:
            raise ValueError("private input")
        return sentinel
    with timing.startup_timing_scope(lambda stage, **row: events.append(row)):
        assert run() is sentinel
        with pytest.raises(ValueError):
            run(True)
    assert events == [{"result_code": "RETURNED", "elapsed_ms": 250.0},
                      {"result_code": "RAISED", "elapsed_ms": 500.0}]
    def broken(*_a, **_k):
        raise RuntimeError("diagnostic writer unavailable")
    with timing.startup_timing_scope(broken):
        assert run() is sentinel


def test_main_function_marker_precedes_first_admission(monkeypatch):
    import Label_Match as gui
    events = []
    monkeypatch.setattr(gui, "_label_match_startup_trace", lambda stage, **_: events.append(stage))
    monkeypatch.setattr(gui, "_first_run_onboarding_enabled", lambda: True)
    class StopProbe(Exception):
        pass
    def admission(*_a, **_k):
        events.append("admission")
        raise StopProbe
    monkeypatch.setattr(onboarding, "try_window_first_start", admission)
    with pytest.raises(StopProbe):
        gui.main([])
    assert events == ["main_function_enter", "admission"]


def test_new_trace_rows_have_no_paths_or_input_values(tmp_path, monkeypatch):
    import Label_Match as gui
    monkeypatch.setenv("TEMP", str(tmp_path))
    for stage in timing.VALUE_FREE_STAGES:
        gui._label_match_startup_trace(stage, result_code="RETURNED", elapsed_ms=1.5)
    rows = [json.loads(line) for path in tmp_path.rglob("*.log") for line in path.read_text().splitlines()]
    assert len(rows) == len(timing.VALUE_FREE_STAGES)
    for row in rows:
        assert set(row) == {"timestamp", "pid", "stage", "version", "frozen", "result_code", "elapsed_ms"}
