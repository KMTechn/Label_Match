"""A shell daemon callback uses only an owned, frozen recovery checkpoint."""
from datetime import datetime
from contextlib import nullcontext
import json
from pathlib import Path
import threading
import time

import pytest

import label_data_manager as persistence
from writer_session_fence import writer_admission
from tests.test_label_match_core import load_label_match_module
from tests.test_legacy_label_transition import _close, _fresh_set, _legacy_set, _packaging_app


@pytest.fixture
def checkpoint_case(tmp_path, monkeypatch):
    module = load_label_match_module()
    app, _ = _packaging_app(module, tmp_path, monkeypatch, registered=False, transition=False)
    del app.__dict__["_save_current_set_state"]
    app.current_set_info = _fresh_set()
    app.current_set_info.update(id="force-exit-set", raw=_legacy_set(401)[:2],
                                start_time=datetime.now(), parsed=[])
    original_write = persistence.write_checked_bytes
    monkeypatch.setattr(persistence, "write_checked_bytes",
                        lambda *_args, **_kwargs: (_ for _ in ()).throw(OSError("synthetic write failure")))
    assert not app._save_current_set_state()
    monkeypatch.setattr(persistence, "write_checked_bytes", original_write)
    yield module, app, Path(app._package_current_state_path()), original_write
    _close(app)


def call_from_daemon(app):
    outcomes, errors = [], []

    def call():
        try:
            outcomes.append(app.save_current_set_on_force_exit())
        except BaseException as error:
            errors.append(type(error).__name__)

    worker = threading.Thread(target=call, daemon=True)
    began = time.monotonic()
    worker.start()
    worker.join(2)
    assert not worker.is_alive(), "force-exit callback exceeded the shell join"
    assert not errors
    return outcomes, time.monotonic() - began


def test_force_exit_checkpoint_runs_once_on_another_thread(checkpoint_case, monkeypatch):
    _module, app, path, original_write = checkpoint_case
    owner = threading.get_ident()
    writes = []

    def checked_write(*args, **kwargs):
        assert threading.get_ident() != owner
        writes.append(args[0])
        return original_write(*args, **kwargs)

    monkeypatch.setattr(persistence, "write_checked_bytes", checked_write)
    for name in ("after", "winfo_exists", "update", "update_idletasks", "_render_operator_workbench"):
        monkeypatch.setattr(app, name, lambda *_a, **_k: pytest.fail("Tk touched by force-exit callback"))
    # The UI's mutable dict changes after publication: the callback must use
    # the consistent frozen set, not follow a live nested reference.
    expected = list(app.current_set_info["raw"])
    app.current_set_info["raw"].append("unpublished")
    assert call_from_daemon(app)[0] == [True]
    assert call_from_daemon(app)[0] == [False]
    assert len(writes) == 1
    assert json.loads(path.read_text(encoding="utf-8"))["current_set_info"]["raw"] == expected


@pytest.mark.parametrize("failure", ["write", "state_lock", "snapshot_lock", "writer_admission", "pending_update"])
def test_force_exit_checkpoint_failure_returns_without_exception(checkpoint_case, monkeypatch, failure):
    module, app, path, _write = checkpoint_case
    writes = []

    def unavailable(*args, **_kwargs):
        writes.append(args[0])
        raise OSError("synthetic write failure")

    monkeypatch.setattr(persistence, "write_checked_bytes", unavailable)
    if failure == "pending_update":
        control = path.parent / "machine/update-control"
        control.mkdir(parents=True)
        (control / "policy.json").write_text("{}")
        (control / "transition.json").write_text(json.dumps({"phase": "FAILED"}))
        monkeypatch.setattr(module, "__file__", str(control.parent / "current/app/Label_Match.py"))
        import auto_update
        monkeypatch.setattr(auto_update, "assert_ordinary_recovery",
                            lambda *_a, **_k: pytest.fail("slow update observer entered by force-exit hook"))
    lock = (app.data_manager._state_lock if failure == "state_lock" else
            app.__dict__.get("_force_exit_snapshot_lock") if failure == "snapshot_lock" else None)
    if lock is not None:
        lock.acquire()
    try:
        with writer_admission("gui_settings_save") if failure == "writer_admission" else nullcontext():
            outcomes, elapsed = call_from_daemon(app)
        assert outcomes == [False] and elapsed < 1
        assert len(writes) == (1 if failure == "write" else 0)
        assert not path.exists()
    finally:
        if lock is not None:
            lock.release()


@pytest.mark.parametrize("boundary", ["saved", "cleared", "direct_saved"])
def test_normal_save_or_clear_discards_old_force_exit_snapshot(checkpoint_case, monkeypatch, boundary):
    _module, app, path, _write = checkpoint_case
    if boundary == "saved":
        assert app._save_current_set_state()
    elif boundary == "direct_saved":
        assert app.data_manager.save_current_state({"current_set_info": {"id": "newer-set", "raw": []}})
    else:
        app._delete_current_set_state()
    before = path.read_bytes() if path.exists() else None
    monkeypatch.setattr(persistence, "write_checked_bytes",
                        lambda *_a, **_k: pytest.fail("stale force-exit write"))
    assert call_from_daemon(app)[0] == [False]
    assert (path.read_bytes() if path.exists() else None) == before


def test_force_exit_checkpoint_restores_same_set_on_next_start(checkpoint_case, monkeypatch):
    module, app, path, _write = checkpoint_case
    expected = json.loads(json.dumps(app.current_set_info, cls=module.DateTimeEncoder))
    assert call_from_daemon(app)[0] == [True]
    assert persistence.read_recovery_file(path, current_state=True)["verified"] is True
    # A fresh processing instance consumes the existing reader and startup
    # recovery path; only UI presentation and unrelated reconciliation are faked.
    restored, _ = _packaging_app(module, path.parent, monkeypatch, registered=False, transition=False)
    restored._owned_deferred_capture_for_set = lambda _saved: None
    restored._reconcile_pending_sealed_transfer_exchanges = lambda **_kw: None
    restored._reconcile_active_package_submission = lambda: None
    restored.sealed_transfer_exchange_store = type("Exchange", (), {"blocking_rows": lambda self, **_kw: []})()
    try:
        restored._load_current_set_state()
        assert restored.current_set_info["id"] == expected["id"]
        assert restored.current_set_info["raw"] == expected["raw"]
        assert restored.current_set_info["start_time"] == datetime.fromisoformat(expected["start_time"])
    finally:
        _close(restored)


@pytest.mark.parametrize("phase", ["PREPARED", "FAILED", "APPLIED_PENDING_READY", "DEFERRED"])
def test_force_exit_admission_never_enters_update_recovery_observer(tmp_path, monkeypatch, phase):
    import auto_update as updates
    control = tmp_path / "update-control"
    identity = "0" * 40
    monkeypatch.setattr(updates, "control_root", lambda _root: control)
    monkeypatch.setattr(updates, "runtime_matches", lambda _root: True)
    monkeypatch.setattr(updates, "load_policy", lambda _root: {"operator_sid": "owned-test"})
    monkeypatch.setattr(updates, "ordinary_user_context", lambda *_a: True)
    monkeypatch.setattr(updates, "optional_record",
                        lambda _path: {"phase": phase, "recovery_pending": phase == "DEFERRED"})
    monkeypatch.setattr(updates, "read_json", lambda _path: {"sha": identity, "source_commit": identity})
    monkeypatch.setattr(updates, "assert_ordinary_recovery",
                        lambda *_a: pytest.fail("update observer entered by bounded force-exit admission"))
    with pytest.raises(ValueError):
        updates.assert_version_writer(tmp_path / "app", source="gui_force_exit_checkpoint",
            environ={"KMTECH_LM_UPDATE_DEFERRED_AT": updates.now_utc()})
