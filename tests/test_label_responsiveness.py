"""Headless callback bounds and snapshot fencing; no Tcl interpreter required."""
import queue
import sqlite3
import threading
import time
from types import SimpleNamespace

import pytest

from tests.test_label_match_core import load_label_match_module


def test_package_review_writer_lock_does_not_block_tk_callback(tmp_path):
    module = load_label_match_module()
    app = object.__new__(module.Label_Match)
    database = tmp_path / "review.sqlite3"
    app.package_outbox = module.PackageOutbox(database)
    app.current_set_info = {}
    app.run_tests = False
    callbacks, readers = [], []
    owner = threading.get_ident()
    app.after = lambda delay, callback: callbacks.append((delay, callback)) or len(callbacks)
    app._reconcile_pending_sealed_transfer_exchanges = lambda **kwargs: None
    app._reconcile_active_package_submission = lambda: None
    original = app.package_outbox.list_conflicts

    def read(**kwargs):
        readers.append(threading.get_ident())
        return original(**kwargs)

    app.package_outbox.list_conflicts = read
    locked = threading.Event()

    def hold_writer():
        with sqlite3.connect(database) as conn:
            conn.execute("BEGIN IMMEDIATE")
            locked.set()
            time.sleep(.2)
            conn.rollback()

    writer = threading.Thread(target=hold_writer)
    writer.start()
    try:
        assert locked.wait(2)
        started = time.perf_counter()
        worker = app._refresh_package_cancellation_review_notice()
        app._poll_package_outbox_drain()
        assert time.perf_counter() - started < .1
        assert writer.is_alive()
        assert "_package_review_snapshot" not in app.__dict__
    finally:
        writer.join(3)
        app.package_outbox_thread.join(15)
    assert not worker.is_alive()
    app._poll_package_outbox_drain()
    assert readers and all(reader != owner for reader in readers)
    assert app._package_create_review_rows == ()
    assert all(isinstance(rows, (str, type(None))) for _, _, rows in app._package_review_snapshot.reviews)


@pytest.mark.parametrize("change", ["generation", "set", "action"])
def test_package_review_snapshot_ignores_stale_context(change):
    module = load_label_match_module()
    app = object.__new__(module.Label_Match)
    app.current_set_info = {"id": "old"}
    app.package_outbox = SimpleNamespace(list_conflicts=lambda **kwargs: [
        {"idempotency_key": "review", "local_completion_committed": 1},
    ])
    snapshot = app._read_package_review_snapshot()
    if change == "generation":
        app._ui_lane_generation = 1
    elif change == "set":
        app.current_set_info = {"id": "new"}
    else:
        app._package_status_epoch = 1
    assert app._refresh_package_cancellation_review_notice(snapshot) is None
    assert "_package_create_review_notice" not in app.__dict__
    app._refresh_package_cancellation_review_notice(app._read_package_review_snapshot())
    assert app._package_create_review_notice is not None


def test_workbench_exchange_notice_uses_snapshot_but_action_guard_reads_store():
    module = load_label_match_module()
    app = object.__new__(module.Label_Match)
    app.current_set_info = {"id": "set", "raw": ["MASTER"], "parsed": ["ITEM"]}
    app.operator_workbench_ready = True
    app.initialized_successfully = True
    calls = []
    attempt = SimpleNamespace(status="OPERATOR_REVIEW", idempotency_key="saved-command",
                              local_apply_status="PENDING", seal_verification_status="PENDING")
    app.sealed_transfer_exchange_store = SimpleNamespace(
        blocking_rows=lambda **kwargs: calls.append(threading.get_ident()) or ["row"],
    )
    app.sealed_transfer_exchange_coordinator = SimpleNamespace(_attempt=lambda row: attempt)
    app._package_review_snapshot = app._read_package_review_snapshot()
    app._selected_qa_scan_iid = lambda: None
    app._selected_exact_rescan_iid = lambda: None
    app._render_qa_scan_detail = lambda *args: None
    app._render_exact_rescan_detail = lambda *args: None
    app._set_exact_rescan_tab_visible = lambda *args, **kwargs: None
    app._update_operator_item_panel = lambda *args: None
    notices = []
    app._set_workflow_notice_ui = lambda notice, action: notices.append(notice)
    app._render_operator_workbench()
    app._render_operator_workbench()
    assert len(calls) == 1
    assert notices[-1].allow_exchange_recovery is True
    assert "F4" in notices[-1].message
    assert app._current_sealed_transfer_exchange_attempt() is attempt
    assert len(calls) == 2
