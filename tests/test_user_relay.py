import json
from pathlib import Path
from types import SimpleNamespace

import current_user_scheduled_task
import user_relay


def test_hkcu_autostart_uses_hardened_onedir_main_and_exact_readback(tmp_path):
    app_root = tmp_path / "hardened"
    app_root.mkdir()
    executable = app_root / "Label_Match.exe"
    executable.write_bytes(b"exe")
    stored = {}

    report = user_relay.install_user_relay_autostart(
        app_root,
        setter=lambda value: stored.update(value=value),
        getter=lambda: stored.get("value", ""),
    )

    assert report["status"] == "PASS"
    assert report["principal"] == "current_user"
    assert report["registry_hive"] == "HKEY_CURRENT_USER"
    assert str(executable.resolve()) in report["command"]
    assert "--label-match-user-relay" in report["command"]
    assert "schtasks" not in report["command"].lower()


def test_session_relay_command_reuses_product_host_and_explicit_user_roots(tmp_path):
    app_root = tmp_path / "app"
    app_root.mkdir()
    (app_root / "Label_Match.exe").write_bytes(b"exe")
    relay_root = tmp_path / "user" / "direct-sync"
    data_root = tmp_path / "user" / "label-data"
    ca_bundle = tmp_path / "user" / "profile" / "tls" / "ca-bundle.pem"
    ca_bundle.parent.mkdir(parents=True)
    ca_bundle.write_bytes(b"private-ca-fixture")

    command = user_relay.build_session_direct_sync_command(
        app_root=app_root,
        direct_sync_root=relay_root,
        scan_source_dir=data_root,
        tls_ca_bundle_path=ca_bundle,
    )

    assert command[:2] == [
        str((app_root / "Label_Match.exe").resolve()),
        "--label-match-direct-sync-relay",
    ]
    assert (
        str((relay_root / "queue" / "direct_sync_relay.sqlite3").resolve()) in command
    )
    assert str(data_root.resolve()) in command
    assert "포장실작업이벤트로그_*.csv" in command
    assert "direct-sync-relay-label-match-current-user" in command
    assert "--source-host-id" not in command
    assert command[command.index("--tls-ca-bundle-path") + 1] == str(ca_bundle)


def test_portable_commands_use_signed_runtime_and_explicit_app_root(tmp_path):
    app_root = tmp_path / "canonical"
    (app_root / "runtime").mkdir(parents=True)
    (app_root / "app").mkdir()
    (app_root / "runtime" / "python.exe").write_bytes(b"runtime")
    (app_root / "runtime" / "pythonw.exe").write_bytes(b"runtime")
    (app_root / "app" / "main.py").write_text("pass\n", encoding="utf-8")

    persistent = user_relay.build_user_relay_command(app_root)
    scheduled = user_relay.build_session_direct_sync_command(
        app_root=app_root,
        direct_sync_root=tmp_path / "state",
        scan_source_dir=tmp_path / "data",
        runtime_status_path=tmp_path / "state" / "status" / "scheduled.json",
        log_path=tmp_path / "state" / "logs" / "scheduled.jsonl",
        worker_id=user_relay.LABEL_MATCH_SCHEDULED_WORKER_ID,
    )

    assert persistent[:4] == [
        str((app_root / "runtime" / "pythonw.exe").resolve()),
        "-I",
        "-B",
        str((app_root / "app" / "main.py").resolve()),
    ]
    assert persistent[-2:] == [
        "--app-root",
        str(app_root.resolve()),
    ]
    assert scheduled[:4] == [
        str((app_root / "runtime" / "python.exe").resolve()),
        "-I",
        "-B",
        str((app_root / "app" / "main.py").resolve()),
    ]
    assert user_relay.LABEL_MATCH_SCHEDULED_WORKER_ID in scheduled
    assert (
        str((tmp_path / "state" / "status" / "scheduled.json").resolve()) in scheduled
    )


def test_persistent_loop_maps_missing_cycle_value_to_unknown(tmp_path):
    result = user_relay.run_persistent_relay_loop(
        lambda: None,
        status_path=tmp_path / "status.json",
        interval_seconds=0,
        max_cycles=1,
    )

    assert result["cycle_count"] == 1
    assert result["last_cycle"]["status"] == "UNKNOWN"
    persisted = json.loads((tmp_path / "status.json").read_text(encoding="utf-8"))
    assert persisted["last_cycle"]["status"] == "UNKNOWN"
    assert persisted["persistent_retry"] is True


def test_persistent_loop_retries_failure_then_records_success(tmp_path):
    outcomes = iter(({"status": "FAIL"}, {"status": "acked"}))
    result = user_relay.run_persistent_relay_loop(
        lambda: next(outcomes),
        status_path=tmp_path / "status.json",
        interval_seconds=0,
        max_cycles=2,
    )

    assert result["cycle_count"] == 2
    assert result["last_cycle"]["status"] == "acked"


class _Lease:
    def __init__(self):
        self.closed = False

    def close(self):
        self.closed = True


def test_stop_request_proves_single_instance_absence(tmp_path):
    leases = [None, _Lease()]
    waits = []

    report = user_relay.request_user_relay_stop(
        tmp_path / "direct-sync",
        timeout_seconds=1,
        wait=lambda seconds: waits.append(seconds),
        lease_factory=lambda _key: leases.pop(0),
    )

    assert report["status"] == "ABSENT"
    assert waits == [0.25]
    assert Path(report["stop_request_path"]).is_file()


def test_scheduled_mode_uses_dedicated_status_and_worker(monkeypatch, tmp_path):
    monkeypatch.setenv("LOCALAPPDATA", str(tmp_path / "LocalAppData"))
    app_root = (tmp_path / "app").resolve()
    (app_root / "runtime").mkdir(parents=True)
    (app_root / "app").mkdir()
    (app_root / "runtime" / "python.exe").write_bytes(b"runtime")
    (app_root / "app" / "main.py").write_text("pass\n", encoding="utf-8")
    monkeypatch.setattr(current_user_scheduled_task, "CANONICAL_ROOT", app_root)
    observed = []
    monkeypatch.setattr(
        "logistics_runtime_profile.load_logistics_runtime_profile",
        lambda **_kwargs: SimpleNamespace(tls_ca_bundle_path="ca.pem"),
    )
    monkeypatch.setattr(
        user_relay,
        "_runtime_cycle",
        lambda **kwargs: observed.append(kwargs) or {"process_status": "PASS"},
    )
    monkeypatch.setattr(
        user_relay,
        "_acquire_relay_lease",
        lambda _key: SimpleNamespace(close=lambda: None),
    )

    assert user_relay.scheduled_main(["--app-root", str(app_root)]) == 0

    assert len(observed) == 1
    call = observed[0]
    assert call["worker_id"] == user_relay.LABEL_MATCH_SCHEDULED_WORKER_ID
    assert call["reason"] == "SCHEDULED_CURRENT_USER"
    assert call["runtime_status_path"].name == "scheduled_direct_sync_relay_status.json"
    assert call["log_path"].name == "scheduled_direct_sync_relay.jsonl"
    scheduled_status = json.loads(
        call["runtime_status_path"].read_text(encoding="utf-8")
    )
    assert scheduled_status["status"] == "PASS"
    assert scheduled_status["outcome"] == "bounded_one_cycle"
    assert len(scheduled_status["action_sha256"]) == 64


def test_scheduled_mode_accepts_only_a_fresh_matching_persistent_owner(tmp_path):
    root = (tmp_path / "canonical").resolve()
    state = (tmp_path / "state").resolve()
    status = {
        "status": "RUNNING",
        "worker_id": user_relay.LABEL_MATCH_WORKER_ID,
        "process_id": 123,
        "app_root": str(root),
        "direct_sync_root": str(state),
        "updated_at": user_relay._now(),
    }

    owner = user_relay._fresh_persistent_owner(
        status,
        app_root=root,
        direct_sync_root=state,
        pid_exists=lambda process_id: process_id == 123,
    )

    assert owner["process_id"] == 123
    assert (
        user_relay._fresh_persistent_owner(
            {**status, "direct_sync_root": str(tmp_path / "other")},
            app_root=root,
            direct_sync_root=state,
            pid_exists=lambda _process_id: True,
        )
        is None
    )
