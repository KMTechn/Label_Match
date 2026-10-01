"""G10 uses existing audio and reports actual transmission evidence."""
import copy
import sqlite3
from types import SimpleNamespace

import pytest

import Label_Match as lm
from tests.test_legacy_label_transition import MASTER
from tests.test_tk_serial_ui_lane import FakeTkRoot
from tests.test_w9scanonly_packaging import packaging, products, scan  # noqa: F401


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
