"""Installed FAST admission binds the exact inactive apply-claim input."""
import json
from pathlib import Path
import subprocess
from types import SimpleNamespace

import pytest

import auto_update as updates
import current_user_onboarding as onboarding
import writer_session_fence as fence
from startup_send_gate import StartupGatedSession, startup_send_hold_path, create_startup_send_hold
from tests.test_window_first_startup import (
    steady_start, reset_window_start, _product_apply_claim, _watch_window_front,
)
from tests.test_onboarding_lifecycle_scope import managed_onboarding
from tests.test_current_user_onboarding import isolated_data_generation_release


@pytest.mark.parametrize("condition", [
    "running", "dead-owner", "unknown-status", "unknown-schema", "empty", "corrupt",
    "nonobject", "duplicate", "oversized", "unreadable", "reparse",
])
def test_nonsettled_or_unproven_claim_keeps_full_start(steady_start, monkeypatch, condition, tmp_path):
    import Label_Match as gui
    item = steady_start
    claim = item.control / "apply-claim.json"
    junction = None
    if condition in {"running", "dead-owner"}:
        _product_apply_claim(item, monkeypatch, settled=False)
        if condition == "dead-owner":
            record = updates.read_json(claim)
            claim.write_text(json.dumps(dict(record, pid=0)))
    elif condition in {"unknown-status", "unknown-schema", "unreadable"}:
        _product_apply_claim(item, monkeypatch)
        if condition != "unreadable":
            record = updates.read_json(claim)
            record["status" if condition == "unknown-status" else "schema"] = "UNKNOWN"
            claim.write_text(json.dumps(record))
        else:
            actual_open = Path.open
            def denied(path, *args, **kwargs):
                if path == claim:
                    raise PermissionError("fixture access denied")
                return actual_open(path, *args, **kwargs)
            monkeypatch.setattr(Path, "open", denied)
    elif condition == "reparse":
        target = tmp_path / "claim-target"
        target.mkdir()
        result = subprocess.run(["cmd.exe", "/d", "/c", "mklink", "/J", str(claim), str(target)],
            capture_output=True, creationflags=subprocess.CREATE_NO_WINDOW)
        assert result.returncode == 0
        junction = claim
    else:
        raw = {"empty": b"", "corrupt": b"{", "nonobject": b"[]",
               "duplicate": b'{"schema":"code-update-apply-claim-v1","status":"RUNNING","status":"SETTLED"}',
               "oversized": b" " * (1024 * 1024 + 1)}[condition]
        claim.write_bytes(raw)
    try:
        with monkeypatch.context() as front:
            _watch_window_front(front)
            assert onboarding.try_window_first_start(item.paths.app_root, environ=item.environment) is None
        # Exercise the main selection too: FULL owns the update gate.
        full = []
        monkeypatch.setattr(gui, "_label_match_runtime_app_root", lambda: item.paths.app_root)
        monkeypatch.setattr(gui, "_first_run_onboarding_enabled", lambda: True)
        monkeypatch.setattr(gui, "release_identity", lambda _: {})
        monkeypatch.setattr(updates, "startup_gate", lambda *_a: full.append(True) or 31)
        assert gui.main([]) == 31 and full == [True]
        assert item.helpers == []
    finally:
        if junction is not None:
            junction.rmdir()  # Remove this test's link only; retain its target.


@pytest.mark.parametrize("boundary", ["during-read", "after-admission"])
@pytest.mark.parametrize("change", ["running", "settled-bytes", "deleted"])
def test_claim_change_refuses_sends_and_preserves_hold_and_local_data(steady_start, monkeypatch, boundary, change):
    item = steady_start
    _product_apply_claim(item, monkeypatch)
    claim = item.control / "apply-claim.json"
    original = claim.read_bytes()
    def mutate():
        if change == "running":
            _product_apply_claim(item, monkeypatch, settled=False)
        elif change == "settled-bytes":
            claim.write_bytes(original + b"\n")
        else:
            claim.unlink()
    if boundary == "during-read":
        actual = updates.read_settled_apply_claim_input
        def racing(control):
            raw = actual(control)
            mutate()
            return raw
        monkeypatch.setattr(updates, "read_settled_apply_claim_input", racing)
    report = onboarding.try_window_first_start(item.paths.app_root, environ=item.environment)
    assert report["start_path"] == "FAST_WINDOW_FIRST"
    assert report["inputs"][claim] == original
    if boundary == "after-admission":
        onboarding.admit_window_first_start(report)
        mutate()
    else:
        report["hold"] = create_startup_send_hold(item.paths.direct_sync_root)
    hold_path = startup_send_hold_path(item.paths.direct_sync_root)
    hold = hold_path.read_bytes()
    ledger, settings = item.paths.ledger_path.read_bytes(), item.paths.settings_path.read_bytes()
    if boundary == "during-read":
        with pytest.raises(onboarding.DataGenerationError):
            onboarding.admit_window_first_start(report)
    else:
        with pytest.raises(onboarding.CurrentUserOnboardingError):
            onboarding.complete_window_first_verification(report)
    sent = []
    session = StartupGatedSession(item.paths.direct_sync_root,
        SimpleNamespace(post=lambda *_a, **_k: sent.append(True)))
    with pytest.raises(fence.WriterFencedError):
        session.post("https://invalid.example")
    assert sent == [] and hold_path.read_bytes() == hold
    assert item.paths.ledger_path.read_bytes() == ledger
    assert item.paths.settings_path.read_bytes() == settings
    assert item.helpers == []


def test_absent_claim_is_also_bound_against_later_creation(steady_start, monkeypatch):
    item = steady_start
    report = onboarding.try_window_first_start(item.paths.app_root, environ=item.environment)
    assert report["inputs"][item.control / "apply-claim.json"] is None
    _product_apply_claim(item, monkeypatch, settled=False)
    with pytest.raises(onboarding.DataGenerationError):
        onboarding.admit_window_first_start(report)
