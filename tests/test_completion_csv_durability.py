"""Existing completion synchronization and ACKED-orphan recovery regressions.

Run only in Main's admitted guest with the B1 pre-import isolation/provider
closure. Storage and startup methods are real; no Tcl interpreter is created.
"""

import builtins
from contextlib import closing
import csv
import json
import os
from pathlib import Path
import sqlite3
from types import SimpleNamespace

import pytest

from tests.test_label_match_core import (
    _b1_app,
    _b1_close,
    _b1_csv_io,
    _b1_inject_sql_fault,
    _b1_rows,
    load_label_match_module,
)


def _seed_acked_orphan(module, tmp_path, monkeypatch):
    app, actions, _clock = _b1_app(module, tmp_path, monkeypatch)
    # Orphan reconstruction requires the immutable seal evidence, in addition
    # to the physical PHS2 used by the existing local-storage fixture.
    seal_qr = (
        "TRF=1|BND=TRANSFER-B1|AUTH_SCOPE=SCOPE-B1|CLC=ITEM-B1|QT=4|"
        f"HSH={'d' * 64}|EPOCH=1|PLANE=SHADOW_CANDIDATE|PE=1|"
        "SID=SEAL-B1|SREV=1|STK=local-test-seal"
    )
    app.current_set_info["sealed_transfer"] = {
        "SID": "SEAL-B1", "SREV": 1, "STK": "local-test-seal",
        "_seal_qr_payload": seal_qr,
    }
    try:
        hits = []
        with monkeypatch.context() as injection:
            _b1_inject_sql_fault(app.package_outbox, injection, "marker_commit", hits)
            assert app._begin_central_package_submission() is False
        assert hits == ["marker_commit"]
        assert not any(action.startswith("sound:") for action in actions)
        commands, events = _b1_rows(tmp_path)
        assert len(commands) == len(events) == 1
        assert commands[0]["local_completion_committed"] == 0
    finally:
        _b1_close(app.data_manager)

    # Seed the historical ACKED/marker=0 state that startup supports. Today's
    # claim_next requires marker=1, so this is explicit local fixture setup,
    # not a claim that the current sender or a real server produced this row.
    with closing(sqlite3.connect(tmp_path / "package_logistics_outbox.sqlite3")) as conn:
        with conn:
            cursor = conn.execute(
                "UPDATE package_command_outbox SET status='ACKED',receipt_json='{}' "
                "WHERE set_id='b1-first' AND local_completion_committed=0"
            )
            assert cursor.rowcount == 1
    (tmp_path / module.Label_Match.FILES.CURRENT_STATE).rename(
        tmp_path / "pre-orphan-current-state.json"
    )
    return commands[0], events


@pytest.mark.parametrize("fault", [None, "fsync", "write_open"])
def test_acked_orphan_requires_existing_csv_sync_before_marker(
    tmp_path, monkeypatch, fault,
):
    module = load_label_match_module()
    original, original_events = _seed_acked_orphan(module, tmp_path, monkeypatch)
    app, actions, _clock = _b1_app(module, tmp_path, monkeypatch, accept=False)
    path = tmp_path / "포장실작업이벤트로그_B1_20260831.csv"
    app.sealed_transfer_exchange_store = SimpleNamespace(blocking_rows=lambda **k: [])
    app._reconcile_pending_sealed_transfer_exchanges = lambda **k: None
    del app._publish_durable_commit_block  # Exercise the product recovery block.
    monkeypatch.setattr(module.messagebox, "showwarning", lambda *a, **k: None)
    barriers, opened, synced, refused = [], [], [], []
    real_flush = app.data_manager.flush
    real_open, real_fsync = builtins.open, os.fsync

    def flush(*args, **kwargs):
        result = real_flush(*args, **kwargs)
        barriers.append(True)
        return result

    def open_csv(file, mode="r", *args, **kwargs):
        if Path(file).resolve() == path.resolve() and mode == "r+":
            assert barriers, "matched CSV opened before the real writer barrier"
            if fault == "write_open":
                refused.append("write_open")
                raise PermissionError("injected matched CSV write-open refusal")
            stream = real_open(file, mode, *args, **kwargs)
            assert stream.writable() and stream.readable()
            opened.append(stream.fileno())
            return stream
        return real_open(file, mode, *args, **kwargs)

    def fsync(fd):
        if not os.path.samestat(os.fstat(fd), path.stat()):
            return real_fsync(fd)
        assert fd in opened, "existing CSV must use a write-capable descriptor"
        commands, events = _b1_rows(tmp_path)
        assert len(commands) == 1 and commands[0]["local_completion_committed"] == 0
        assert events == original_events
        if fault == "fsync":
            refused.append("fsync")
            raise OSError("injected matched CSV fsync refusal")
        real_fsync(fd)
        synced.append(True)

    try:
        with monkeypatch.context() as injection:
            injection.setattr(app.data_manager, "flush", flush)
            injection.setattr(module, "open", open_csv, raising=False)
            injection.setattr(module.os, "fsync", fsync)
            app._load_current_set_state()
        commands, events = _b1_rows(tmp_path)
        assert len(commands) == 1 and commands[0]["status"] == "ACKED"
        assert commands[0]["draft_json"] == original["draft_json"]
        assert commands[0]["idempotency_key"] == original["idempotency_key"]
        assert events == original_events
        assert actions == []  # No success sound, drain, summary, or dismissal.
        if fault is None:
            assert barriers and opened and synced == [True] and not refused
            assert commands[0]["local_completion_committed"] == 1
            assert app.package_outbox.list_local_completion_pending() == []
            assert app.current_set_info["raw"] == []
            assert not (tmp_path / module.Label_Match.FILES.CURRENT_STATE).exists()
        else:
            assert refused == [fault] and synced == []
            assert commands[0]["local_completion_committed"] == 0
            assert [row["idempotency_key"] for row in
                    app.package_outbox.list_local_completion_pending()] == [original["idempotency_key"]]
            assert app.current_set_info["id"] == "b1-first"
            assert app.current_set_info["raw"] == [
                "PHS=2|SRC=KMTECH_INPUT_TAG|ITG=ITG-B1|CLC=ITEM-B1|LBL=LBL-B1|HSH=0123456789abcdef"
            ]
            with (tmp_path / module.Label_Match.FILES.CURRENT_STATE).open(encoding="utf-8") as stream:
                saved = json.load(stream)
            assert saved["current_set_info"]["id"] == "b1-first"
            assert saved["current_set_info"]["raw"] == app.current_set_info["raw"]
            assert app._workflow_recovered is True
            assert app._workflow_blocking_notice.kind == "submission_blocked"
            assert app._workflow_notice_action_text == "저장 재시도"
            assert app._workflow_notice_action == app._retry_blocked_submission
    finally:
        _b1_close(app.data_manager)


def test_matching_csv_keeps_real_writer_failure_sticky(tmp_path, monkeypatch):
    module = load_label_match_module()
    manager = module.DataManager(str(tmp_path), "포장실", "worker", "STICKY")
    path = Path(manager._get_log_filepath())
    try:
        io = _b1_csv_io(module, monkeypatch, path, fault="csv_fsync")
        manager.log_event("TRAY_COMPLETE", {"set_id": "sticky-set"})
        with pytest.raises(RuntimeError, match="injected B1"):
            manager.flush(timeout=5)
        assert io["faults"] == ["csv_fsync"] and io["synced"] == []
        original_errors = list(manager._writer_errors)
        before = path.read_bytes()
        with pytest.raises(module.PackageLogisticsError, match="could not be synchronized"):
            module._label_match_local_completion_event_exists(manager, "sticky-set")
        assert manager._writer_errors == original_errors
        assert io["synced"] == [] and path.read_bytes() == before
    finally:
        _b1_close(manager, writer_fault=True)


def test_matching_csv_is_revalidated_after_writer_barrier(tmp_path, monkeypatch):
    module = load_label_match_module()
    manager = module.DataManager(str(tmp_path), "포장실", "worker", "RECHECK")
    path = Path(manager._get_log_filepath())
    try:
        manager.log_event("TRAY_COMPLETE", {"set_id": "recheck-set"})
        manager.flush(timeout=5)
        original = path.read_bytes()
        barriers = []
        real_flush = manager.flush

        def replace_after_barrier(*args, **kwargs):
            result = real_flush(*args, **kwargs)
            barriers.append(True)
            path.rename(tmp_path / "original-matching.csv")
            with path.open("w", encoding="utf-8-sig", newline="") as stream:
                writer = csv.writer(stream)
                writer.writerow(["timestamp", "worker_name", "event", "details"])
                writer.writerow(["2026-09-07", "worker", "TRAY_COMPLETE", '{"set_id":"different-set"}'])
            return result

        monkeypatch.setattr(manager, "flush", replace_after_barrier)
        with pytest.raises(module.PackageLogisticsError, match="could not be synchronized"):
            module._label_match_local_completion_event_exists(manager, "recheck-set")
        assert barriers == [True]
        assert (tmp_path / "original-matching.csv").read_bytes() == original
        with path.open(encoding="utf-8-sig", newline="") as stream:
            rows = list(csv.DictReader(stream))
        assert len(rows) == 1 and json.loads(rows[0]["details"]) == {"set_id": "different-set"}
    finally:
        _b1_close(manager)


def test_malformed_or_unrelated_discovery_rows_remain_absent(tmp_path):
    module = load_label_match_module()
    manager = module.DataManager(str(tmp_path), "포장실", "worker", "ABSENT")
    path = Path(manager._get_log_filepath())
    try:
        assert module._label_match_local_completion_event_exists(manager, "missing-set") is False
        assert not path.exists()
        with path.open("w", encoding="utf-8-sig", newline="") as stream:
            writer = csv.writer(stream)
            writer.writerow(["timestamp", "worker_name", "event", "details"])
            for details in ("{", "null", "[]", '{"set_id":"different-set"}'):
                writer.writerow(["2026-09-07", "worker", "TRAY_COMPLETE", details])
            writer.writerow(["2026-09-07", "worker", "SET_RESTORED", '{"set_id":"missing-set"}'])
        before = path.read_bytes()
        assert module._label_match_local_completion_event_exists(manager, "missing-set") is False
        assert path.read_bytes() == before
    finally:
        _b1_close(manager)
