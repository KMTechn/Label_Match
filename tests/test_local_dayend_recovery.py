"""Local unfinished trays survive the next working day without operator input."""

import base64
from datetime import datetime, timedelta
import hashlib
import json
import os
from pathlib import Path
import uuid

import pytest

from tests.test_label_match_core import load_label_match_module
from tests.test_legacy_label_transition import (
    MASTER, _close, _events, _fresh_set, _legacy_set, _load_history, _packaging_app, _scan,
)


@pytest.fixture
def dayend_case(tmp_path, monkeypatch):
    module = load_label_match_module()

    class Clock(datetime):
        @classmethod
        def now(cls, tz=None):
            value = cls(2026, 10, 12, 8, 0)
            return value if tz is None else value.replace(tzinfo=tz)

    monkeypatch.setattr(module, "datetime", Clock)
    app, _ = _packaging_app(module, tmp_path, monkeypatch,
                            registered=False, transition=False)
    del app.__dict__["_save_current_set_state"]
    app._owned_deferred_capture_for_set = lambda _saved: None
    app._reconcile_pending_sealed_transfer_exchanges = lambda **_kwargs: None
    app._reconcile_active_package_submission = lambda: None
    app.sealed_transfer_exchange_store = type("Exchange", (), {
        "blocking_rows": lambda self, **_kwargs: [],
    })()

    def no_dialog(*_args, **_kwargs):
        pytest.fail("day-end recovery requested operator confirmation")

    for name in ("showinfo", "showwarning", "askyesno", "askokcancel"):
        monkeypatch.setattr(module.messagebox, name, no_dialog)

    path = Path(app._package_current_state_path())

    def save(days_back=1, **changes):
        current = _fresh_set()
        current.update({
            "id": str(uuid.uuid4()), "raw": _legacy_set(401)[:2],
            "parsed": [MASTER, MASTER], "start_time": Clock.now() - timedelta(days=days_back),
            "item_name_override": "저장한 세트",
        })
        current.update(changes)
        state = {"current_set_info": current,
                 "timestamp": (Clock.now() - timedelta(days=days_back)).isoformat()}
        assert app.data_manager.save_current_state(state)
        stamp = (Clock.now() - timedelta(days=days_back)).timestamp()
        os.utime(path, (stamp, stamp))
        app.current_set_info = _fresh_set()
        return path.read_bytes()

    yield module, app, path, save
    _close(app)


@pytest.mark.parametrize("days_back", [1, 3])
def test_previous_day_tray_restores_then_accepts_the_next_product(dayend_case, days_back):
    module, app, path, save = dayend_case
    original = save(days_back)
    saved = json.loads(original)["current_set_info"]

    app._load_current_set_state()

    assert app.current_set_info["id"] == saved["id"]
    assert app.current_set_info["raw"] == saved["raw"]
    assert app.current_set_info["parsed"] == saved["parsed"]
    assert app.current_set_info["start_time"] == datetime.fromisoformat(saved["start_time"])
    assert path.read_bytes() == original
    assert app.__dict__.get("_workflow_blocking_notice") is None
    archives = list(path.parent.glob(path.name + ".resume-*"))
    assert len(archives) == 1 and archives[0].read_bytes() == original
    assert app._phs_label_guidance_notice.message == (
        "어제 하던 세트를 이어서 엽니다 — 저장한 세트·남은 3개")

    _scan(module, app, _legacy_set(401)[2])
    assert len(app.current_set_info["raw"]) == 3
    assert len(json.loads(path.read_bytes())["current_set_info"]["raw"]) == 3
    assert archives[0].read_bytes() == original


def test_previous_day_bytes_are_copied_before_the_existing_validator(dayend_case, monkeypatch):
    _module, app, path, save = dayend_case
    original = save()
    archive = path.with_name(path.name + ".resume-" + hashlib.sha256(original).hexdigest())
    validate = app._read_package_recovery_file
    checked = []

    def check(candidate, **kwargs):
        if Path(candidate) == path:
            assert archive.read_bytes() == original
            checked.append(True)
        return validate(candidate, **kwargs)

    monkeypatch.setattr(app, "_read_package_recovery_file", check)
    app._load_current_set_state_checked()
    assert checked and app._workflow_recovered


@pytest.mark.parametrize("damage", ["shape", "syntax", "encoding"])
def test_damaged_previous_day_tray_is_held_and_new_trays_can_start(dayend_case, damage):
    module, app, path, save = dayend_case
    original = save()
    if damage == "shape":
        state = json.loads(original)
        state["current_set_info"]["raw"] = [{}]
        original = json.dumps(state, ensure_ascii=False).encode("utf-8")
    elif damage == "syntax":
        original = original[:-2]
    else:
        original = b"\xff\xfe"
    path.write_bytes(original)
    stamp = (module.datetime.now() - timedelta(days=1)).timestamp()
    os.utime(path, (stamp, stamp))

    app._load_current_set_state_checked()

    assert not app.current_set_info["raw"]
    assert app.__dict__.get("_workflow_blocking_notice") is None
    hold_id = "CURRENT:" + hashlib.sha256(original).hexdigest()
    hold = app.package_outbox.get_workbench_hold(hold_id)
    assert hold is not None
    snapshot = json.loads(hold["snapshot_json"])
    assert Path(snapshot["archive_path"]).read_bytes() == original
    assert base64.b64decode(snapshot["raw_base64"]) == original
    assert app._phs_label_guidance_notice.message == (
        "어제 하던 세트를 확인 대기로 두었습니다. 관리자에게 알리세요.")

    _scan(module, app, _legacy_set(501)[0])
    assert len(app.current_set_info["raw"]) == 1
    assert json.loads(path.read_bytes())["current_set_info"]["raw"] == app.current_set_info["raw"]
    assert Path(snapshot["archive_path"]).read_bytes() == original
    app.current_set_info = _fresh_set()
    app._load_current_set_state_checked()
    assert len(app.current_set_info["raw"]) == 1
    assert app.package_outbox.get_workbench_hold(hold_id) == hold


def test_same_day_tray_keeps_the_existing_restore_without_an_archive(dayend_case):
    _module, app, path, save = dayend_case
    original = save(0)
    app._load_current_set_state_checked()
    assert path.read_bytes() == original
    assert len(app.current_set_info["raw"]) == 2
    assert list(path.parent.glob(path.name + ".held-*")) == []
    assert list(path.parent.glob(path.name + ".resume-*")) == []
    assert "어제" not in app._phs_label_guidance_notice.message


@pytest.mark.parametrize("kind", ["central", "transition"])
def test_existing_durable_and_transition_exceptions_keep_their_files(dayend_case, kind):
    module, app, path, save = dayend_case
    changes = {"central_inherit_all": True} if kind == "central" else {
        "transition_class": sorted(module.label_transition.LOCAL_CLASSES)[0]}
    original = save(**changes)
    app._load_current_set_state_checked()
    assert path.read_bytes() == original
    assert list(path.parent.glob(path.name + ".held-*")) == []
    assert list(path.parent.glob(path.name + ".resume-*")) == []


@pytest.mark.parametrize("component", ["hold", "move"])
def test_failed_durable_hold_keeps_the_current_slot_locked(dayend_case, monkeypatch, component):
    module, app, path, save = dayend_case
    state = json.loads(save())
    state["current_set_info"]["parsed"] = [{}]
    original = json.dumps(state, ensure_ascii=False).encode("utf-8")
    path.write_bytes(original)

    def refuse_hold(**_kwargs):
        raise OSError("injected hold failure")

    with monkeypatch.context() as patch:
        if component == "hold":
            patch.setattr(app.package_outbox, "hold_workbench_set", refuse_hold)
        else:
            patch.setattr(app, "_move_package_pin_file", lambda *_args, **_kwargs: False)
        app._load_current_set_state_checked()
        assert path.read_bytes() == original
        assert app._workflow_blocking_notice.kind == "submission_blocked"
        _scan(module, app, _legacy_set(501)[0])
        assert not app.current_set_info["raw"]
        assert path.read_bytes() == original

    app._load_current_set_state_checked()
    hold = app.package_outbox.get_workbench_hold("CURRENT:" + hashlib.sha256(original).hexdigest())
    assert Path(json.loads(hold["snapshot_json"])["archive_path"]).read_bytes() == original
    _scan(module, app, _legacy_set(501)[0])
    assert len(app.current_set_info["raw"]) == 1


def test_failed_archive_preserves_original_and_blocks_new_trays(dayend_case, monkeypatch):
    module, app, path, save = dayend_case
    original = save()
    write = module.write_checked_bytes

    def refuse_archive(candidate, *args, **kwargs):
        if ".resume-" in Path(candidate).name:
            raise OSError("archive write unavailable")
        return write(candidate, *args, **kwargs)

    monkeypatch.setattr(module, "write_checked_bytes", refuse_archive)
    app._load_current_set_state_checked()
    assert path.read_bytes() == original
    assert not app.current_set_info["raw"]
    assert app._workflow_blocking_notice.kind == "submission_blocked"
    _scan(module, app, _legacy_set(501)[0])
    assert not app.current_set_info["raw"]
    assert path.read_bytes() == original
    monkeypatch.setattr(module, "write_checked_bytes", write)
    app._load_current_set_state_checked()
    assert len(app.current_set_info["raw"]) == 2
    assert app._workflow_notice_action is None
    assert path.with_name(path.name + ".resume-" + hashlib.sha256(original).hexdigest()).read_bytes() == original


@pytest.mark.parametrize("timestamp", ["unreadable-time", 123])
@pytest.mark.parametrize("transition", [False, True])
def test_noncentral_unreadable_timestamp_is_held_instead_of_deleted(dayend_case, timestamp, transition):
    module, app, path, save = dayend_case
    state = json.loads(save())
    if transition:
        state["current_set_info"]["transition_class"] = sorted(module.label_transition.LOCAL_CLASSES)[0]
    state["timestamp"] = timestamp
    original = json.dumps(state, ensure_ascii=False).encode("utf-8")
    path.write_bytes(original)
    app._load_current_set_state_checked()
    hold = app.package_outbox.get_workbench_hold("CURRENT:" + hashlib.sha256(original).hexdigest())
    assert hold is not None
    assert Path(json.loads(hold["snapshot_json"])["archive_path"]).read_bytes() == original
    assert app.__dict__.get("_workflow_blocking_notice") is None
    _scan(module, app, _legacy_set(501)[0])
    assert len(app.current_set_info["raw"]) == 1


def test_resumed_tray_keeps_its_old_identity_and_counts_only_when_completed_today(dayend_case):
    module, app, path, save = dayend_case
    original = save()
    saved = json.loads(original)["current_set_info"]
    values = _legacy_set(401)
    values[-1] = values[-1][:-8] + (module.datetime.now() - timedelta(days=1)).strftime("%Y%m%d")
    saved["production_date"] = app._extract_production_date(values[-1])
    assert saved["production_date"] is not None
    saved["id"] = (module.datetime.now() - timedelta(days=1)).strftime("%Y%m%d") + "-" + saved["id"]
    state = json.loads(original)
    state["current_set_info"] = saved
    assert app.data_manager.save_current_state(state)
    app._load_current_set_state_checked()
    assert app.current_set_info["id"] == saved["id"]
    assert app.current_set_info["start_time"].date() == (module.datetime.now() - timedelta(days=1)).date()
    assert app.current_set_info["raw"] == saved["raw"]
    assert app.current_set_info["production_date"] == saved["production_date"]
    assert dict(app.scan_count) == {}
    for barcode in values[2:]:
        _scan(module, app, barcode)
    assert app.data_manager.flush()
    completed = _events(path.parent, app.Events.TRAY_COMPLETE)
    assert len(completed) == 1
    row, details = completed[0]
    assert details["set_id"] == saved["id"]
    assert details["scanned_product_barcodes"] == values
    assert details["production_date"] == saved["production_date"]
    assert datetime.fromisoformat(row["timestamp"]).date() == module.datetime.now().date()
    history = _load_history(module, app)
    assert len(history["set_details_map"]) == 1
    assert sum(sum(counts.values()) for counts in history["scan_count"].values()) == 1
    _scan(module, app, _legacy_set(501)[0])
    assert app.current_set_info["id"] != saved["id"]
    assert app.current_set_info["start_time"].date() == module.datetime.now().date()
