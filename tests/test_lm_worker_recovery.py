"""Worker recovery keeps evidence and audit without administrator authority."""
import json
import threading

import pytest

import Label_Match as app_module
from label_admin_pin import (
    AdminPinError, WORKER_RECOVERY_ACTIONS, WorkerRecoveryOperation,
    active_worker_operation,
)
from tests.test_label_admin_pin import _active_set_pin_app, SOURCE_A
from tests.test_label_admin_pin_ui_lane import pin_lane


@pytest.fixture
def worker_lane(pin_lane, monkeypatch):
    monkeypatch.setattr(app_module, "WORKER_RECOVERY_ACTIONS", WORKER_RECOVERY_ACTIONS)
    monkeypatch.setattr(pin_lane.app, "_prompt_package_admin_pin", lambda: pytest.fail("worker PIN prompt"))
    monkeypatch.setattr(pin_lane.app, "_package_pin_client", lambda: pytest.fail("worker PIN transport"))
    return pin_lane


def apply_worker(app, action, target, physical=""):
    done, prepared = app._prepare_package_worker_action(action, target, physical)
    assert not done
    return app._execute_package_worker_action(action, target, physical, prepared)


def audit_rows(app):
    with app.package_outbox._connect() as conn:
        return conn.execute(
            "SELECT manager_id,verification_id,operator_id,operation_key,pin_action,action "
            "FROM package_workbench_hold_audit ORDER BY audit_id"
        ).fetchall()


def assert_worker_audit(app, action):
    rows = audit_rows(app)
    assert rows
    actor = app._package_pin_operator()["local_id"]
    assert all(row[0] == "" and row[1] == "" and row[2] == actor for row in rows)
    assert all(row[3].startswith("worker:") and row[4] == action for row in rows)
    assert not any(row[5] in {"PIN_ATTEMPT", "PIN_REVIEW_HOLD"} for row in rows)
    assert active_worker_operation.get() is None


def test_f5_hold_never_prompts_and_repeated_request_keeps_one_original(worker_lane):
    case = worker_lane
    raw = case.journal.path.read_bytes()
    assert case.submit()
    case.root.run_until(lambda: bool(case.finished or case.errors))
    assert case.finished == [True] and case.errors == []
    held = case.app.package_outbox.get_label_exchange_hold(case.item[3:])
    assert bytes(held["journal_bytes"]) == raw
    assert __import__("pathlib").Path(held["archive_path"]).read_bytes() == raw
    assert not case.journal.path.exists() and not case.app._admin_pin_store.path.exists()
    assert case.submit()
    case.root.run_until(lambda: len(case.finished) == 2 or bool(case.errors))
    assert case.finished == [True, True] and not case.errors
    assert len(case.app.package_outbox.list_label_exchange_holds()) == 1
    assert len({row[3] for row in audit_rows(case.app)}) == 1
    assert_worker_audit(case.app, "LABEL.F5_HOLD")


@pytest.mark.parametrize("linked", [False, True])
def test_worker_hold_preserves_current_and_linked_journal_bytes(tmp_path, monkeypatch, linked):
    app, journal, current, _server, set_id = _active_set_pin_app(tmp_path, monkeypatch, linked=linked)
    original = current.read_bytes()
    action = "LABEL.F5_HOLD" if linked else "LABEL.SET_HOLD"
    journal_raw = journal.path.read_bytes() if linked else None
    target = "F5:" + app._label_recovery_hold_id() if linked else set_id
    monkeypatch.setattr(app, "_package_pin_client", lambda: pytest.fail("worker PIN transport"))
    assert apply_worker(app, action, target)
    held = app.package_outbox.get_workbench_hold(set_id)
    assert held and not current.exists()
    assert json.loads(held["snapshot_json"])["current_set_info"]["id"] == set_id
    archive = app._package_pin_current_archive_path("SET:" + set_id)
    assert archive.read_bytes() == original
    if linked:
        label = app.package_outbox.get_label_exchange_hold(target[3:])
        assert bytes(label["journal_bytes"]) == journal_raw
    assert not app._admin_pin_store.path.exists()
    assert_worker_audit(app, action)


def test_changed_current_is_rejected_before_worker_hold(tmp_path, monkeypatch):
    app, _journal, current, _server, set_id = _active_set_pin_app(tmp_path, monkeypatch, linked=False)
    done, prepared = app._prepare_package_worker_action("LABEL.SET_HOLD", set_id)
    assert not done
    changed = current.read_bytes() + b" "
    current.write_bytes(changed)
    assert app._execute_package_worker_action("LABEL.SET_HOLD", set_id, "", prepared) is False
    assert current.read_bytes() == changed
    assert app.package_outbox.get_workbench_hold(set_id) is None
    assert audit_rows(app) == [] and active_worker_operation.get() is None


def test_changed_worker_is_rejected_before_worker_hold(tmp_path, monkeypatch):
    app, _journal, current, _server, set_id = _active_set_pin_app(tmp_path, monkeypatch, linked=False)
    original = current.read_bytes()
    done, prepared = app._prepare_package_worker_action("LABEL.SET_HOLD", set_id)
    assert not done
    app.worker_name = "changed-worker"
    assert app._execute_package_worker_action("LABEL.SET_HOLD", set_id, "", prepared) is False
    assert current.read_bytes() == original
    assert app.package_outbox.get_workbench_hold(set_id) is None
    assert audit_rows(app) == []


def test_interrupted_worker_move_resumes_the_same_hold(tmp_path, monkeypatch):
    app, _journal, current, _server, set_id = _active_set_pin_app(tmp_path, monkeypatch, linked=False)
    original = current.read_bytes()
    move = app._move_package_pin_file
    monkeypatch.setattr(app, "_move_package_pin_file", lambda *_args, **_kwargs: False)
    assert apply_worker(app, "LABEL.SET_HOLD", set_id) is False
    held = app.package_outbox.get_workbench_hold(set_id)
    assert held and current.read_bytes() == original
    monkeypatch.setattr(app, "_move_package_pin_file", move)
    assert apply_worker(app, "LABEL.SET_HOLD", set_id)
    assert app.package_outbox.get_workbench_hold(set_id)["snapshot_json"] == held["snapshot_json"]
    assert len({row[3] for row in audit_rows(app)}) == 1
    assert app._package_pin_current_archive_path("SET:" + set_id).read_bytes() == original
    assert_worker_audit(app, "LABEL.SET_HOLD")


def test_worker_handoff_records_only_handoff_and_never_closes_admin_intent(worker_lane, monkeypatch):
    case = worker_lane
    original = case.journal.path.read_bytes()
    monkeypatch.setattr(case.app._admin_pin_store, "transition", lambda *_a, **_k: pytest.fail("administrator effect"))
    case.app._admin_pin_closing_intent = {
        "operation_key": "original-operation", "target_fingerprint": "original-target",
        "source_evidence_sha256": "original-source",
    }
    assert case.submit("LABEL.SUPPORT_HANDOFF")
    case.root.run_until(lambda: bool(case.finished or case.errors))
    assert case.finished and not case.errors
    assert case.journal.path.read_bytes() == original
    assert case.app.package_outbox.list_label_exchange_holds() == []
    assert not case.app._admin_pin_store.path.exists()
    with case.app.package_outbox._connect() as conn:
        assert conn.execute("SELECT observed FROM package_workbench_hold_audit WHERE action='HANDOFF'").fetchone()[0] == "UNRESOLVED"
    assert_worker_audit(case.app, "LABEL.SUPPORT_HANDOFF")


def test_worker_physical_lookup_uses_device_read_and_does_not_restore(worker_lane, monkeypatch):
    case = worker_lane
    original = case.journal.path.read_bytes()
    reads = []
    def read(physical, _item, **kwargs):
        reads.append((physical, kwargs.get("force_central_read")))
        return None, {"member_count": 7}, None, None
    monkeypatch.setattr(case.app, "_resolve_central_phs2_scan_overlay", read)
    assert apply_worker(case.app, "LABEL.PHS_LOOKUP", "PHS_LOOKUP", SOURCE_A)
    assert reads and all(row == (SOURCE_A, True) for row in reads)
    assert case.journal.path.read_bytes() == original
    assert case.app.package_outbox.list_label_exchange_holds() == []
    assert not case.app._admin_pin_store.path.exists()
    assert_worker_audit(case.app, "LABEL.PHS_LOOKUP")


def test_worker_context_cannot_enter_recheck_or_restore(worker_lane):
    app = worker_lane.app
    assert app._prepare_package_worker_action("LABEL.RECHECK", worker_lane.item) == (True, False)
    assert app._execute_package_worker_action("LABEL.RECHECK", worker_lane.item, "", None) is False
    scope = WorkerRecoveryOperation("worker:test", "LABEL.SUPPORT_HANDOFF", worker_lane.item, "actor", "worker", ("", ""))
    token = active_worker_operation.set(scope)
    try:
        with pytest.raises(AdminPinError):
            app._package_pin_apply("LABEL.RECHECK", worker_lane.item, "__RESTORE_CHANGED_PIN_FILE__")
        with pytest.raises(AdminPinError):
            app._restore_changed_pin_file(worker_lane.item)
    finally:
        active_worker_operation.reset(token)
    assert not app._admin_pin_store.path.exists()
    assert audit_rows(app) == []


def test_worker_request_reservation_blocks_duplicates_and_closes_before_apply(worker_lane, monkeypatch):
    case = worker_lane
    entered, release = threading.Event(), threading.Event()
    target = case.app._package_worker_target
    def wait(*args):
        entered.set()
        assert release.wait(10)
        return target(*args)
    monkeypatch.setattr(case.app, "_package_worker_target", wait)
    try:
        assert case.submit()
        case.root.run_until(entered.is_set)
        assert case.submit() is False
        case.window.alive = False
    finally:
        release.set()
    case.root.run_until(lambda: not case.app.ui_lane.is_busy() and not case.app.__dict__.get("_package_pin_pending_targets"))
    assert case.journal.path.exists()
    assert case.finished == [] and case.errors == []
    assert case.app.package_outbox.list_label_exchange_holds() == []
