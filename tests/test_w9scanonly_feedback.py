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
    assert app.connection_text == "회사 서버 연결됨 · 보낼 것 3건"
    assert app._logistics_last_transmission == last_success
    assert "마지막 전송" not in app.connection_text
    app._render_logistics_connection("retrying", pending, None)
    assert app.connection_text == "회사 서버 연결 안 됨 · 이 PC에 저장 중 · 보낼 것 3건"
    assert app._logistics_last_transmission == last_success, "disconnection must retain the last actual ACK time"


@pytest.mark.parametrize("connected,pending,expected", [
    (True, 0, "회사 서버 연결됨 · 보낼 것 0건"),
    (None, 0, "회사 서버 확인 중"),
    (False, 3, "회사 서버 연결 안 됨 · 이 PC에 저장 중 · 보낼 것 3건"),
    (True, None, "회사 서버 연결됨"),
])
def test_connection_footer_uses_field_guidance_without_inventing_success(connected, pending, expected):
    app = object.__new__(lm.Label_Match)
    options = {}
    app.logistics_connection_label = SimpleNamespace(configure=lambda **kw: options.update(kw))
    stamp = datetime.now(timezone.utc)
    observation = (stamp, connected) if connected is not None else None
    app._render_logistics_connection("normal", pending, None, progress_observation=observation)
    assert options["text"] == expected
    assert options["style"] == "Status.TLabel"
    assert options["background"] == ("#FFEDD5" if connected is False else "#F8FAFC")
    assert app.__dict__.get("_logistics_last_transmission") is None
    assert "없음" not in options["text"]


@pytest.mark.parametrize("connected,count,review,age,width,expected", [
    (True, 3, 2, 601, 1366, "관리자 확인 필요 2건 · 보낼 것 3건"),
    (False, 3, 2, 0, 1366, "관리자 확인 필요 2건 · 보낼 것 3건"),
    (None, None, 2, 0, 1366, "관리자 확인 필요 2건"),
    (False, 3, 0, 601, 1366, "회사 서버 연결 안 됨 · 이 PC에 저장 중 · 보낼 것 3건"),
    (True, 3, 0, 601, 1366, "회사 서버로 못 보낸 것 3건"),
    (True, 3, 0, 600, 1366, "회사 서버 연결됨 · 보낼 것 3건"),
    (True, 0, 0, 601, 1366, "회사 서버 연결됨 · 보낼 것 0건"),
    (None, 3, 0, 601, 1366, "회사 서버 확인 중"),
    (True, 3, 2, 601, 1024, "관리자 확인 2건"),
    (False, 3, 0, 601, 1024, "연결 안 됨 · 보낼 것 3건"),
    (True, 3, 0, 601, 1024, "못 보낸 것 3건"),
    (None, 0, 0, 0, 1024, "확인 중"),
    (True, 0, 0, 0, 1024, "연결됨 · 0건"),
])
def test_common_footer_priority_short_forms_and_ten_minute_boundary(connected, count, review, age, width, expected):
    app = object.__new__(lm.Label_Match)
    options = {}
    app.winfo_width = lambda: width
    app.logistics_connection_label = SimpleNamespace(configure=lambda **kw: options.update(kw))
    app._logistics_admin_review_count = review
    app._logistics_oldest_pending_at = datetime.now(timezone.utc) - timedelta(seconds=age) if age else None
    app.entry = SimpleNamespace(focus_set=lambda: pytest.fail("status stole scanner focus"),
                                delete=lambda *args: pytest.fail("status erased a scan"))
    stamp = datetime.now(timezone.utc)
    observation = (stamp, connected) if connected is not None else None
    app._render_logistics_connection("normal", count, None, progress_observation=observation)
    assert options["text"] == expected
    assert options["wraplength"] == 0 and options["width"] == 1
    assert "\n" not in options["text"] and "…" not in options["text"]
    assert not any(word in options["text"] for word in ("대기", "outbox", "동기화", "반영", "없음"))
    assert options["font"][1] < 14
    assert options["font"][2] == ("bold" if review else "normal")
    if review:
        assert options["foreground"] == "#B91C1C"
    elif connected is False:
        assert options["background"] == "#FFEDD5"
    elif connected is True and count and age > 600:
        assert options["foreground"] == "#9A3412"
    else:
        assert options["foreground"] == "#64748B"


@pytest.mark.parametrize("days", [0, 1])
def test_last_actual_ack_time_only_appears_in_accumulating_state(days):
    app = object.__new__(lm.Label_Match)
    options = {}
    app.logistics_connection_label = SimpleNamespace(configure=lambda **kw: options.update(kw))
    now = datetime.now(timezone.utc)
    ack = now - timedelta(days=days)
    local = ack.astimezone()
    expected_time = local.strftime("%H:%M") if days == 0 else f"{local.month}. {local.day}. {local:%H:%M}"
    app._logistics_oldest_pending_at = now - timedelta(minutes=11)
    app._render_logistics_connection("normal", 3, ack)
    assert options["text"] == f"회사 서버로 못 보낸 것 3건 · 마지막 전송 {expected_time}"
    app._render_logistics_connection("normal", 0, None)
    assert options["text"] == "회사 서버 연결됨 · 보낼 것 0건"
    assert app._logistics_last_transmission == ack


def test_unreadable_transmission_queue_keeps_unknown_count(tmp_path):
    app = object.__new__(lm.Label_Match)
    broken = tmp_path / "broken.sqlite3"
    broken.write_bytes(b"not sqlite")
    assert app._read_logistics_transmission_summary(str(broken)) == (None, None)


@pytest.mark.parametrize("table", ["direct_sync_relay_batches", "package_command_outbox",
                                 "package_cancellation_outbox", "sealed_transfer_exchange_intents",
                                 "deferred_intents"])
def test_oldest_unsent_age_comes_from_existing_rows_without_writes(tmp_path, table):
    app = object.__new__(lm.Label_Match)
    db = tmp_path / "queue.sqlite3"
    column = "state" if table == "deferred_intents" else "status"
    pending = "VALIDATED" if table == "deferred_intents" else "PENDING"
    done = "SUPERSEDED" if table == "deferred_intents" else "ACKED"
    now = datetime.now(timezone.utc)
    oldest = now - timedelta(minutes=11)
    with sqlite3.connect(db) as conn:
        conn.execute(f"CREATE TABLE {table}({column} TEXT, created_at TEXT, updated_at TEXT)")
        conn.executemany(f"INSERT INTO {table} VALUES (?,?,?)", [
            (pending, oldest.isoformat(), now.isoformat()),
            (done, (oldest - timedelta(days=1)).isoformat(), now.isoformat()),
        ])
    before = db.read_bytes()
    count, ack = app._read_logistics_transmission_summary(str(db))
    assert count == 1 and app._logistics_oldest_pending_at == oldest
    assert ack == (None if table == "deferred_intents" else now)
    assert db.read_bytes() == before
    with sqlite3.connect(db) as conn:
        conn.execute(f"DELETE FROM {table} WHERE {column}=?", (pending,))
    assert app._read_logistics_transmission_summary(str(db))[0] == 0
    assert app._logistics_oldest_pending_at is None


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
    assert "회사 서버 연결 안 됨" in app.connection_text
    assert dialogs == []


def test_local_idle_cannot_clear_loss_without_server_response(progress_app, tmp_path, monkeypatch):
    app, _dialogs, root = progress_app
    install_connection_relay(app, tmp_path, monkeypatch, "retry_wait")
    app.package_logistics_client = SimpleNamespace(get_progress=offline_progress)
    open_progress(app)
    root.run_until(lambda: app.__dict__.get("_logistics_progress_connected") is False, timeout=5)
    assert "회사 서버 연결 안 됨" in app.connection_text
    app._relay_screen_status_payload = {"status": "idle"}
    app._refresh_relay_screen_status()
    assert app._logistics_relay_state == "normal"
    assert "회사 서버 연결 안 됨" in app.connection_text


def test_new_relay_retry_overrides_old_progress_success(progress_app, tmp_path, monkeypatch):
    app, _dialogs, root = progress_app
    db = install_connection_relay(app, tmp_path, monkeypatch)
    app.package_logistics_client = SimpleNamespace(get_progress=lambda **kw: copy.deepcopy(PROGRESS))
    open_progress(app)
    root.run_until(lambda: app.__dict__.get("_logistics_progress_connected") is True, timeout=5)
    assert "회사 서버 연결됨" in app.connection_text
    with sqlite3.connect(db) as conn:
        conn.execute("INSERT INTO direct_sync_relay_batches VALUES ('retry_wait', ?)", (datetime.now(timezone.utc).isoformat(),))
    app._refresh_relay_screen_status()
    assert app._logistics_relay_state == "retrying"
    assert "회사 서버 연결 안 됨" in app.connection_text
    assert "연결 끊김" in app.progress_text


def test_idle_and_live_lease_alone_leave_connection_unconfirmed(progress_app, tmp_path, monkeypatch):
    app, _dialogs, _root = progress_app
    install_connection_relay(app, tmp_path, monkeypatch)
    assert "회사 서버 확인 중" in app.connection_text
    assert app.__dict__.get("_logistics_last_transmission") is None
    assert "없음" not in app.connection_text


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
    assert "회사 서버 연결됨" in app.connection_text
    assert app.__dict__.get("_logistics_last_transmission") is None
    assert "없음" not in app.connection_text


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
    assert "회사 서버 연결 안 됨" in app.connection_text
    assert app._logistics_last_transmission == old
    fresh = datetime.now(timezone.utc)
    with sqlite3.connect(db) as conn:
        conn.execute("UPDATE direct_sync_relay_batches SET updated_at=?", (fresh.isoformat(),))
    app._refresh_relay_screen_status()
    assert "회사 서버 연결됨" in app.connection_text
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
    root.run_until(lambda: "회사 서버 연결됨" in app.connection_text, timeout=5)
    with sqlite3.connect(db) as conn:
        if table == "direct_sync_relay_batches":
            conn.execute("INSERT INTO direct_sync_relay_batches VALUES (?, ?)", (status, datetime.now(timezone.utc).isoformat()))
        else:
            conn.execute(f"CREATE TABLE {table}(status TEXT, updated_at TEXT, last_error_code TEXT)")
            conn.execute(f"INSERT INTO {table} VALUES (?, ?, 'TRANSPORT_ERROR')", (status, datetime.now(timezone.utc).isoformat()))
    app._refresh_relay_screen_status()
    assert app._logistics_relay_state == "retrying"
    assert "회사 서버 연결 안 됨" in app.connection_text
    assert "보낼 것 1건" in app.connection_text


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
    assert "회사 서버 연결 안 됨" in app.connection_text


@pytest.mark.parametrize("newer_success", [True, False])
def test_late_progress_response_keeps_newer_transport_observation(progress_app, tmp_path, monkeypatch, newer_success):
    import threading

    app, _dialogs, _root = progress_app
    db = install_connection_relay(app, tmp_path, monkeypatch)
    instant = [datetime.now(timezone.utc)]

    class Clock(datetime):
        @classmethod
        def now(cls, tz=None):
            return instant[0].astimezone(tz) if tz else instant[0].astimezone().replace(tzinfo=None)

    monkeypatch.setattr(lm, "datetime", Clock)
    entered, release = threading.Event(), threading.Event()

    def delayed(**kwargs):
        entered.set()
        assert release.wait(5), "test did not release the progress request"
        if newer_success:
            raise PackageTransportError("older request failed late")
        return copy.deepcopy(PROGRESS)

    app.package_logistics_client = SimpleNamespace(get_progress=delayed)
    try:
        open_progress(app)  # Request A starts first and remains in flight.
        assert entered.wait(5)
        instant[0] += timedelta(seconds=1)
        stamp = instant[0]
        with sqlite3.connect(db) as conn:
            conn.execute("INSERT INTO direct_sync_relay_batches VALUES (?, ?)",
                         ("acked" if newer_success else "retry_wait", stamp.isoformat()))
        app._refresh_relay_screen_status()  # Newer request B finishes first.
        instant[0] += timedelta(seconds=1)
        release.set()
        app._logistics_progress_thread.join(timeout=5)
        assert not app._logistics_progress_thread.is_alive()
        app._poll_logistics_progress()  # A finishes after B, in either direction.
        expected = "정상" if newer_success else "끊김"
        assert ("회사 서버 연결됨" if newer_success else "회사 서버 연결 안 됨") in app.connection_text
        assert f"연결 {expected}" in app.progress_text
        assert app.__dict__.get("_logistics_last_transmission") == (stamp if newer_success else None)
    finally:
        release.set()


@pytest.mark.parametrize("newer_success", ["progress", "ack"])
@pytest.mark.parametrize("table,status", [
    ("direct_sync_relay_batches", "retry_wait"),
    ("package_command_outbox", "PENDING"),
    ("package_cancellation_outbox", "PENDING"),
    ("sealed_transfer_exchange_intents", "RETRY_WAIT"),
])
def test_delayed_retry_read_keeps_newer_success(progress_app, tmp_path, monkeypatch, newer_success, table, status):
    app, _dialogs, _root = progress_app
    db = install_connection_relay(app, tmp_path, monkeypatch)
    older_failure = datetime.now(timezone.utc) - timedelta(minutes=1)
    with sqlite3.connect(db) as conn:
        if table != "direct_sync_relay_batches":
            conn.execute(f"CREATE TABLE {table}(status TEXT, updated_at TEXT, last_error_code TEXT)")
            conn.execute(f"INSERT INTO {table} VALUES (?, ?, 'TRANSPORT_ERROR')", (status, older_failure.isoformat()))
        else:
            conn.execute("INSERT INTO direct_sync_relay_batches VALUES (?, ?)", (status, older_failure.isoformat()))
    if newer_success == "progress":
        app.package_logistics_client = SimpleNamespace(get_progress=lambda **kw: copy.deepcopy(PROGRESS))
        open_progress(app)
        app._logistics_progress_thread.join(timeout=5)
        assert not app._logistics_progress_thread.is_alive()
        app._poll_logistics_progress()
    else:
        fresh_ack = datetime.now(timezone.utc)
        with sqlite3.connect(db) as conn:
            conn.execute("INSERT INTO direct_sync_relay_batches VALUES ('acked', ?)", (fresh_ack.isoformat(),))
    app._refresh_relay_screen_status()
    assert app._logistics_relay_state == ("retrying" if table == "direct_sync_relay_batches" else "normal")
    assert "회사 서버 연결됨" in app.connection_text
    assert "연결 정상" in app.progress_text
    assert app.__dict__.get("_logistics_last_transmission") == (fresh_ack if newer_success == "ack" else None)
