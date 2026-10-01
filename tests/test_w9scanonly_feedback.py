"""G10 uses existing audio and reports actual transmission evidence."""
import copy
import sqlite3
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

import pytest

import Label_Match as lm
from tests.test_legacy_label_transition import MASTER
from tests.test_tk_serial_ui_lane import FakeTkRoot
from tests.test_w9scanonly_packaging import packaging, products, scan  # noqa: F401
from tests.test_w9scanonly_progress import PROGRESS, open_progress, progress_app  # noqa: F401
from package_logistics import PackageTransportError


def sound_app(monkeypatch):
    monkeypatch.setattr(lm, "_label_match_automated_test_mode", lambda: False)
    app = object.__new__(lm.Label_Match)
    app.initialized_successfully, app.run_tests = True, False
    root = FakeTkRoot()
    app.after = root.after
    played = []
    def sound(key):
        return SimpleNamespace(play=lambda: played.append(key), duration_ms=40)
    app.sound_objects = {key: sound(key) for key in ("scan_master", "scan_1", "scan_2", "scan_3", "pass", "fail")}
    app.sounds = app.sound_objects
    return app, root, played


def test_all_accepted_scan_positions_use_one_short_sound(monkeypatch):
    app, _root, played = sound_app(monkeypatch)
    for key in ("scan_master", "scan_1", "scan_2", "scan_3"):
        app._play_sound(key)
    assert played == ["scan_master"] * 4


def test_rejection_repeats_existing_error_audio_and_completion_uses_long_audio(monkeypatch):
    app, root, played = sound_app(monkeypatch)
    app._play_sound("fail")
    while root.run_one():
        pass
    assert played == ["fail"] * 3
    app._play_sound("pass")
    assert played[-1] == "pass"


def test_next_accepted_scan_cancels_old_error_repeats(monkeypatch):
    app, root, played = sound_app(monkeypatch)
    app._play_sound("fail")
    app._play_sound("scan_1")
    while root.run_one():
        pass
    assert played == ["fail", "scan_master"]


def test_feedback_follows_checkpoint_and_durable_completion(packaging):
    app, _dialogs = packaging
    order = []
    save = app._save_current_set_state
    def checkpoint():
        result = save()
        order.append("checkpoint")
        return result
    app._save_current_set_state = checkpoint
    def play(key):
        if key.startswith("scan_"):
            assert order and order[-1] == "checkpoint"
        if key == "pass":
            assert app.durable_commits, "completion audio must follow durable intent/CSV/marker"
        order.append(key)
    app._play_sound = play
    for value in [MASTER, *products()]:
        scan(app, value)
    assert order.count("pass") == 1


def test_checkpoint_failure_emits_no_success_or_completion_audio(packaging):
    app, _dialogs = packaging
    scan(app, MASTER)
    app._save_current_set_state = lambda: False
    app.sounds.clear()
    before = copy.deepcopy(app.current_set_info)
    from package_logistics import PackageLogisticsError
    with pytest.raises(PackageLogisticsError, match="could not be saved"):
        scan(app, products()[0])
    assert app.current_set_info == before
    assert not any(key.startswith("scan_") or key == "pass" for key in app.sounds)


def test_connection_summary_uses_acked_times_and_counts_each_unresolved_transmission(tmp_path):
    app = object.__new__(lm.Label_Match)
    relay = tmp_path / "relay.sqlite3"
    package = tmp_path / "package.sqlite3"
    stamp = "2026-10-01T00:03:00Z"
    with sqlite3.connect(relay) as conn:
        conn.execute("CREATE TABLE direct_sync_relay_batches(status TEXT, updated_at TEXT)")
        conn.executemany("INSERT INTO direct_sync_relay_batches VALUES (?,?)", [("acked", stamp), ("pending", stamp)])
    with sqlite3.connect(package) as conn:
        conn.execute("CREATE TABLE package_command_outbox(status TEXT, updated_at TEXT)")
        conn.executemany("INSERT INTO package_command_outbox VALUES (?,?)", [("PENDING", stamp), ("ACKED", stamp)])
        conn.execute("CREATE TABLE deferred_intents(state TEXT, updated_at TEXT)")
        conn.executemany("INSERT INTO deferred_intents VALUES (?,?)", [("SUPERSEDED", stamp), ("WAITING_DEPENDENCY", stamp)])
    app.package_outbox = SimpleNamespace(db_path=str(package))
    pending, last_success = app._read_logistics_transmission_summary(str(relay))
    assert pending == 3, "materialized SUPERSEDED capture must not double count its package"
    assert last_success is not None
    label = SimpleNamespace(configure=lambda **kw: setattr(app, "connection_text", kw["text"]))
    app.logistics_connection_label = label
    app._render_logistics_connection("normal", pending, last_success)
    assert "연결: 정상" in app.connection_text and "대기 3건" in app.connection_text
    assert "마지막 성공 전송" in app.connection_text and "없음" not in app.connection_text
    app._render_logistics_connection("retrying", pending, None)
    assert "연결: 끊김" in app.connection_text and "대기 3건" in app.connection_text
    assert "없음" not in app.connection_text, "disconnection must retain the last actual ACK time"


def test_unreadable_transmission_queue_keeps_unknown_count(tmp_path):
    app = object.__new__(lm.Label_Match)
    broken = tmp_path / "broken.sqlite3"
    broken.write_bytes(b"not sqlite")
    assert app._read_logistics_transmission_summary(str(broken)) == (None, None)


def install_connection_relay(app, tmp_path, monkeypatch, status="idle"):
    db = tmp_path / "relay.sqlite3"
    expires = (datetime.now(timezone.utc) + timedelta(hours=1)).isoformat()
    with sqlite3.connect(db) as conn:
        conn.execute("CREATE TABLE direct_sync_relay_batches(status TEXT, updated_at TEXT)")
        conn.execute("CREATE TABLE direct_sync_runtime_authority(status TEXT, expires_at TEXT, pending_request_json TEXT, updated_at TEXT)")
        conn.execute("INSERT INTO direct_sync_runtime_authority VALUES ('ACTIVE', ?, NULL, ?)", (expires, expires))
    monkeypatch.setattr(lm, "_label_match_direct_sync_runtime_paths", lambda context: {"db_path": str(db)})
    app.direct_sync_bootstrap_context = {}
    app._relay_screen_status_payload = {"status": status}
    app.logistics_connection_label = SimpleNamespace(
        configure=lambda **kw: setattr(app, "connection_text", kw["text"]))
    app._refresh_relay_screen_status()
    return db


def offline_progress(**kwargs):
    raise PackageTransportError("synthetic connection lost")


def test_progress_failure_overrides_idle_relay(progress_app, tmp_path, monkeypatch):
    app, dialogs, root = progress_app
    install_connection_relay(app, tmp_path, monkeypatch)
    app.package_logistics_client = SimpleNamespace(get_progress=offline_progress)
    open_progress(app)
    root.run_until(lambda: app.__dict__.get("_logistics_progress_connected") is False, timeout=5)
    assert "연결 끊김" in app.progress_text
    assert "서버 연결: 끊김" in app.connection_text
    assert dialogs == []


def test_local_idle_cannot_clear_loss_without_server_response(progress_app, tmp_path, monkeypatch):
    app, _dialogs, root = progress_app
    install_connection_relay(app, tmp_path, monkeypatch, "retry_wait")
    app.package_logistics_client = SimpleNamespace(get_progress=offline_progress)
    open_progress(app)
    root.run_until(lambda: app.__dict__.get("_logistics_progress_connected") is False, timeout=5)
    assert "서버 연결: 끊김" in app.connection_text
    app._relay_screen_status_payload = {"status": "idle"}
    app._refresh_relay_screen_status()
    assert app._logistics_relay_state == "normal"
    assert "서버 연결: 끊김" in app.connection_text


def test_new_relay_retry_overrides_old_progress_success(progress_app, tmp_path, monkeypatch):
    app, _dialogs, root = progress_app
    db = install_connection_relay(app, tmp_path, monkeypatch)
    app.package_logistics_client = SimpleNamespace(get_progress=lambda **kw: copy.deepcopy(PROGRESS))
    open_progress(app)
    root.run_until(lambda: app.__dict__.get("_logistics_progress_connected") is True, timeout=5)
    assert "서버 연결: 정상" in app.connection_text
    with sqlite3.connect(db) as conn:
        conn.execute("INSERT INTO direct_sync_relay_batches VALUES ('retry_wait', ?)", (datetime.now(timezone.utc).isoformat(),))
    app._refresh_relay_screen_status()
    assert app._logistics_relay_state == "retrying"
    assert "서버 연결: 끊김" in app.connection_text
    assert "연결 끊김" in app.progress_text


def test_idle_and_live_lease_alone_leave_connection_unconfirmed(progress_app, tmp_path, monkeypatch):
    app, _dialogs, _root = progress_app
    install_connection_relay(app, tmp_path, monkeypatch)
    assert "서버 연결: 확인 중" in app.connection_text
    assert "마지막 성공 전송 없음" in app.connection_text


def test_fresh_progress_response_restores_connection_despite_old_retry(progress_app, tmp_path, monkeypatch):
    app, _dialogs, root = progress_app
    install_connection_relay(app, tmp_path, monkeypatch, "retry_wait")
    app.package_logistics_client = SimpleNamespace(get_progress=offline_progress)
    open_progress(app)
    root.run_until(lambda: app.__dict__.get("_logistics_progress_connected") is False, timeout=5)
    app.package_logistics_client = SimpleNamespace(get_progress=lambda **kw: copy.deepcopy(PROGRESS))
    app._refresh_logistics_progress()
    root.run_until(lambda: app.__dict__.get("_logistics_progress_connected") is True, timeout=5)
    app._refresh_relay_screen_status()
    assert "서버 연결: 정상" in app.connection_text
    assert "마지막 성공 전송 없음" in app.connection_text


def test_historical_ack_cannot_clear_loss_but_fresh_ack_can(progress_app, tmp_path, monkeypatch):
    app, _dialogs, root = progress_app
    db = install_connection_relay(app, tmp_path, monkeypatch)
    old = datetime.now(timezone.utc) - timedelta(minutes=1)
    with sqlite3.connect(db) as conn:
        conn.execute("INSERT INTO direct_sync_relay_batches VALUES ('acked', ?)", (old.isoformat(),))
    app._refresh_relay_screen_status()
    app.package_logistics_client = SimpleNamespace(get_progress=offline_progress)
    open_progress(app)
    root.run_until(lambda: app.__dict__.get("_logistics_progress_connected") is False, timeout=5)
    app._refresh_relay_screen_status()
    assert "서버 연결: 끊김" in app.connection_text
    assert app._logistics_last_transmission == old
    fresh = datetime.now(timezone.utc)
    with sqlite3.connect(db) as conn:
        conn.execute("UPDATE direct_sync_relay_batches SET updated_at=?", (fresh.isoformat(),))
    app._refresh_relay_screen_status()
    assert "서버 연결: 정상" in app.connection_text
    assert app._logistics_last_transmission == fresh


@pytest.mark.parametrize("table,status", [
    ("direct_sync_relay_batches", "retry_wait"),
    ("package_command_outbox", "PENDING"),
    ("package_cancellation_outbox", "PENDING"),
    ("sealed_transfer_exchange_intents", "RETRY_WAIT"),
])
def test_new_transport_failure_overrides_success_without_relay_state_change(progress_app, tmp_path, monkeypatch, table, status):
    app, _dialogs, root = progress_app
    db = install_connection_relay(app, tmp_path, monkeypatch)
    app.package_logistics_client = SimpleNamespace(get_progress=lambda **kw: copy.deepcopy(PROGRESS))
    open_progress(app)
    root.run_until(lambda: app.__dict__.get("_logistics_progress_connected") is True, timeout=5)
    app._relay_screen_status_payload = {"status": "retry_wait"}
    app._refresh_relay_screen_status()
    app._refresh_logistics_progress()
    root.run_until(lambda: "서버 연결: 정상" in app.connection_text, timeout=5)
    with sqlite3.connect(db) as conn:
        if table == "direct_sync_relay_batches":
            conn.execute("INSERT INTO direct_sync_relay_batches VALUES (?, ?)", (status, datetime.now(timezone.utc).isoformat()))
        else:
            conn.execute(f"CREATE TABLE {table}(status TEXT, updated_at TEXT, last_error_code TEXT)")
            conn.execute(f"INSERT INTO {table} VALUES (?, ?, 'TRANSPORT_ERROR')", (status, datetime.now(timezone.utc).isoformat()))
    app._refresh_relay_screen_status()
    assert app._logistics_relay_state == "retrying"
    assert "서버 연결: 끊김" in app.connection_text
    assert "대기 1건" in app.connection_text


def test_delayed_progress_poll_does_not_override_newer_relay_loss(progress_app, tmp_path, monkeypatch):
    app, _dialogs, root = progress_app
    install_connection_relay(app, tmp_path, monkeypatch)
    app.package_logistics_client = SimpleNamespace(get_progress=lambda **kw: copy.deepcopy(PROGRESS))
    open_progress(app)
    app._logistics_progress_thread.join(timeout=5)
    assert not app._logistics_progress_thread.is_alive()
    app._relay_screen_status_payload = {"status": "retry_wait"}
    app._refresh_relay_screen_status()
    root.run_until(lambda: app.__dict__.get("_logistics_progress_connected") is True, timeout=5)
    assert "서버 연결: 끊김" in app.connection_text
