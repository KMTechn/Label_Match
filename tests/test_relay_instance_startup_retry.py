"""Exercise the real Windows singleton mutex through the relay main entry."""
import contextlib
import json
import os
from pathlib import Path
import subprocess
import sys
import threading
import time
from types import SimpleNamespace

import pytest

import user_relay as relay


pytestmark = pytest.mark.skipif(os.name != "nt", reason="real Windows mutex required")


def wait_for(predicate):
    deadline = time.monotonic() + 10
    while not predicate():
        assert time.monotonic() < deadline, "native observer did not reach its barrier"
        time.sleep(0.005)


@contextlib.contextmanager
def observer(root, mode):
    guard = getattr(sys.modules.get("sitecustomize"), "__file__", "")
    child = subprocess.Popen(
        [sys.executable, "-I", "-B", str(Path(__file__).with_name("_relay_instance_observer_child.py")),
         str(root), mode, guard],
        stdout=subprocess.DEVNULL, stderr=subprocess.PIPE,
        creationflags=subprocess.CREATE_NO_WINDOW,
    )
    try:
        wait_for(lambda: (root / "observer-ready").exists() or child.poll() is not None)
        assert child.poll() is None, "native observer exited before acquiring its mutex"
        yield child
    finally:
        (root / "observer-stop").touch()
        child.communicate(timeout=10)
        assert child.returncode == 0, "native observer failed"


@pytest.fixture
def entry(monkeypatch, tmp_path):
    import auto_update
    import current_user_onboarding as onboarding
    import label_data_generation
    import logistics_runtime_profile
    import producer_runtime_client

    direct = tmp_path / "direct"
    paths = SimpleNamespace(
        data_root=tmp_path / "data", ledger_path=tmp_path / "ledger.sqlite3",
        direct_sync_root=direct, queue_dir=tmp_path / "queue",
        logistics_profile_path=tmp_path / "profile.json", settings_path=tmp_path / "settings.json",
    )
    monkeypatch.setattr(auto_update, "startup_gate", lambda *a, **k: None)
    monkeypatch.setattr(auto_update, "trial_stop_requested", lambda *a: False)
    monkeypatch.setattr(auto_update, "relay_ready_metadata", lambda *a: {})
    monkeypatch.setattr(label_data_generation, "release_identity", lambda *a: None)
    monkeypatch.setattr(label_data_generation, "normalize_data_arguments", lambda *a: None)
    monkeypatch.setattr(relay, "check_user_data_open", lambda *a: None)
    monkeypatch.setattr(relay, "record_user_data_open", lambda *a: None)
    monkeypatch.setattr(onboarding, "resolve_current_user_onboarding_paths", lambda *a: paths)
    monkeypatch.setattr(onboarding, "apply_current_user_runtime_environment", lambda *a: None)
    monkeypatch.setattr(logistics_runtime_profile, "load_logistics_runtime_profile",
                        lambda **k: SimpleNamespace(tls_ca_bundle_path=""))
    monkeypatch.setattr(producer_runtime_client, "checkpoint_sqlite_database", lambda *a: None)
    entered, reasons = [], []

    def run_loop(*args, **kwargs):
        entered.append(True)
        return {"last_cycle": {"status": "IDLE"}}

    monkeypatch.setattr(relay, "run_persistent_relay_loop", run_loop)
    monkeypatch.setattr(relay, "_record_relay_reason", lambda reason, **k: reasons.append(reason))

    def invoke():
        return relay.main([
            "--app-root", str(tmp_path), "--direct-sync-root", str(direct),
            "--scan-source-dir", str(tmp_path / "scan"), "--once",
        ], credential_input={})

    return SimpleNamespace(root=tmp_path, direct=direct, invoke=invoke, entered=entered, reasons=reasons)


def test_observer_overlap_does_not_silently_discard_ten_relay_starts(entry):
    results = []
    with observer(entry.root, "observe"):
        for attempt in range(10):
            turn = str(attempt + 1)
            (entry.root / "observer-turn").write_text(turn)
            wait_for(lambda: (entry.root / "observer-held").exists()
                     and (entry.root / "observer-held").read_text() == turn)
            results.append(entry.invoke())
    quiet = entry.reasons.count("RELAY_INSTANCE_LEASE_UNAVAILABLE")
    (entry.root / "mutex-overlap-result.json").write_text(json.dumps({
        "starts": 10, "quiet_lease_unavailable_rc0": quiet, "loop_entries": len(entry.entered),
        "returncodes": results, "real_windows_mutex": True,
    }))
    assert results == [0] * 10
    assert quiet == 0
    assert len(entry.entered) == 10


def test_real_other_owner_still_exits_quietly_after_two_seconds(entry):
    with observer(entry.root, "hold"):
        started = time.monotonic()
        assert entry.invoke() == 0
        elapsed = time.monotonic() - started
    assert elapsed >= 2.0
    assert entry.entered == []
    assert entry.reasons == ["RELAY_INSTANCE_LEASE_UNAVAILABLE"]


def test_stop_marker_interrupts_singleton_retry(entry):
    stop = relay.user_relay_stop_path(entry.direct)
    stop.parent.mkdir(parents=True)
    with observer(entry.root, "hold"):
        timer = threading.Timer(0.2, stop.touch)
        timer.start()
        try:
            started = time.monotonic()
            assert entry.invoke() == 0
            elapsed = time.monotonic() - started
        finally:
            timer.join()
    assert elapsed < 2.0
    assert entry.entered == []
    assert entry.reasons == ["RELAY_STOP_MARKER_PRESENT"]
