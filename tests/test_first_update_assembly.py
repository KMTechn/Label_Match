from types import SimpleNamespace
import sys

import pytest

import user_relay
import label_match_screen as screen
from tests.test_install_server_origin import _live_origins, _origin
from tests.test_relay_second_start_admission import live_relay, OPERATOR, SHA  # noqa: F401
from tests.test_current_user_onboarding import isolated_data_generation_release  # noqa: F401


def test_changed_origin_precedes_an_eligible_recent_relay(live_relay, tmp_path, monkeypatch):
    import current_user_onboarding as onboarding
    live_relay.status.update(source_commit=SHA, sid=OPERATOR, ordinary_user=True)
    paths = _live_origins(live_relay, tmp_path, monkeypatch, _origin("installed"), _origin())
    with pytest.raises(onboarding.CurrentUserOnboardingError) as caught:
        user_relay._existing_user_relay_owner(
            live_relay.current, paths.direct_sync_root, is_bootstrap=True,
            allow_fast_path=True, fast_only=True,
        )
    assert caught.value.status == "RECOVERY_REQUIRED"
    assert caught.value.cause_code == "INSTALL_SERVER_ORIGIN_CHANGED"
    assert live_relay.observed == []


def test_default_stop_owns_the_shell_udf_but_not_another_app(tmp_path, monkeypatch):
    from tests.test_app_stop_lifecycle import process
    local = tmp_path / "local"
    monkeypatch.setenv("LOCALAPPDATA", str(tmp_path / "requester"))
    app_root = tmp_path / "current"
    udf = screen.shell_data_dir(environ={"LOCALAPPDATA": str(local)}) / "webview2"
    relay = local / "KMTech" / "DirectSync" / "label_match"
    rows = [
        process(20, 1, "msedgewebview2.exe", f'msedgewebview2.exe --user-data-dir="{udf}"',
                name="msedgewebview2.exe"),
        process(30, 1, "msedgewebview2.exe", f'msedgewebview2.exe --user-data-dir="{tmp_path / "other"}"',
                name="msedgewebview2.exe"),
    ]
    monkeypatch.setattr(user_relay, "_app_stop_process_snapshot", lambda **_: rows)
    monkeypatch.setattr(user_relay, "request_user_relay_stop", lambda *_a, **_k: {"status": "ABSENT"})
    result = user_relay.request_app_stop(relay, app_root=app_root, timeout_seconds=0)
    assert result["status"] == "UNKNOWN"
    assert result["remaining_process_count"] == 1


@pytest.mark.parametrize("code", ["INSTALL_SERVER_ORIGIN_CHANGED", "UPDATE_MUTEX_ABANDONED_VERIFIED",
                                  "UPDATE_MUTEX_ABANDONED_RECOVERY_REQUIRED"])
def test_new_fixed_codes_survive_public_output(code):
    from auto_update import support_record
    from failure_redaction import public_record
    assert public_record({"cause_code": code}) == {"cause_code": code}
    assert support_record({"reason_code": code}, stage="FAILED")["reason_code"] == code
    assert "cause_code" not in public_record({"cause_code": "UNREVIEWED_ASSEMBLY_CODE"})


def test_screen_force_hook_is_explicitly_pending(tmp_path, monkeypatch):
    calls = []
    monkeypatch.setitem(sys.modules, "host", SimpleNamespace(run_app=lambda *_a, **k: calls.append(k)))
    monkeypatch.setattr(screen, "shell_data_dir", lambda: tmp_path)
    screen.screen_runner(SimpleNamespace())()
    assert calls[0]["on_force_exit"] is None
    assert calls[0]["write_guard"] is screen.shell_write_guard


def test_screen_handoff_retains_window_first_report(monkeypatch):
    import Label_Match as module
    report = {"start_path": "FAST_WINDOW_FIRST"}
    host = {"active": False, "requested": True, "catalog_unconfirmed": False}
    monkeypatch.setattr(module, "LABEL_MATCH_SCREEN_HOST", host)
    monkeypatch.setattr(module, "_startup_app_stop_requested", lambda: False)
    import current_user_onboarding as onboarding
    monkeypatch.setattr(onboarding, "admit_window_first_start", lambda value: None)
    monkeypatch.setattr(module, "_prepare_startup_catalog_attempt", lambda: pytest.fail("catalog before window"))
    monkeypatch.setattr(screen, "screen_runner", lambda owner: lambda: 0)
    assert module._run_label_match_application(relay_reuse_report=report) == 0
    assert host["relay_reuse_report"] is report


def test_screen_owner_keeps_report_and_tracks_shell_ready(monkeypatch):
    import label_screen_bridge
    report = {"start_path": "FAST_WINDOW_FIRST"}
    host = {"relay_reuse_report": report}
    app = SimpleNamespace(after=lambda *_a: None, mainloop=lambda: None,
                          _finish_after_mainloop=lambda: None)
    bridge = SimpleNamespace(_view_count=0, attach_owner=lambda processor: None,
                             request_exit=lambda: None)
    calls = []
    module = SimpleNamespace(LABEL_MATCH_SCREEN_HOST=host)
    def run(**kwargs):
        calls.append(kwargs)
        host["run"](app)
    module._run_label_match_application = run
    monkeypatch.setattr(label_screen_bridge, "LabelMatchScreenProcessor", lambda *_a: None)
    screen.owner_main_for(module)(bridge)
    assert calls == [{"relay_reuse_report": report}]
    assert app._startup_screen_visible() is False
    bridge._view_count = 1
    assert app._startup_screen_visible() is True
