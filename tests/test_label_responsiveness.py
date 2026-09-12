"""Headless callback bounds and snapshot fencing; no Tcl interpreter required."""
import queue
import sqlite3
import threading
import time
from types import SimpleNamespace

import pytest

from tests.test_label_match_core import _FakeEntry, _FakeLabel, _RecordingTree, load_label_match_module


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


def history_app(row_count=5000, summary_count=0):
    module = load_label_match_module()
    app = object.__new__(module.Label_Match)
    app.current_set_info = {}
    app.history_load_generation = 1
    app.history_tree = _RecordingTree()
    app.summary_tree = _RecordingTree()
    app.initialized_successfully = True
    app.history_load_pending = True
    app.history_active_load_pending = True
    app.run_tests = True
    app.history_queue = queue.Queue()
    app._history_values_for_display = tuple
    app._apply_history_view_mode = lambda: None
    app._render_history_detail = lambda: None
    app._refresh_session_tree = lambda: None
    app._render_operator_workbench = lambda: None
    app._set_summary_date_label = lambda *args: None
    app._summary_date_text = lambda value: "2026-09-12"
    app._format_summary_code_cell = str
    callbacks = []
    app.after = lambda delay, callback: callbacks.append(callback) or len(callbacks)
    counts = {"2026-09-12": {(f"ITEM-{i}", "-"): 1 for i in range(summary_count)}}
    result = {
        'load_generation': 1, 'updates_active_state': True,
        'scan_count': counts, 'global_scanned_set': {'known-barcode'},
        'set_details_map': {f'set-{i}': {'set_id': f'set-{i}'} for i in range(row_count)},
        'sorted_sets': [(f'set-{i}', {'values': [0, 'PHS2', '', '', '', '', 'PASS'], 'tags': ('pass',)}) for i in range(row_count)],
        'summary_items': module.Label_Match._summary_items(counts),
    }
    app.history_queue.put(result)
    return app, callbacks


def test_large_history_deletes_inserts_and_summary_are_bounded_and_allow_input():
    app, callbacks = history_app(summary_count=2000)
    for i in range(5000):
        app.history_tree.insert('', 'end', iid=f'old-{i}')
    operations = []

    def timed(operation):
        def call(*args, **kwargs):
            operations.append(True)
            deadline = time.perf_counter() + .00005
            while time.perf_counter() < deadline:
                pass
            return operation(*args, **kwargs)
        return call

    for tree in (app.history_tree, app.summary_tree):
        tree.insert = timed(tree.insert)
        tree.delete = timed(tree.delete)
    callbacks.append(app._process_history_queue)
    maximum = 0
    count = 0
    while callbacks:
        previous = len(operations)
        start = time.perf_counter()
        callbacks.pop(0)()
        maximum = max(maximum, time.perf_counter() - start)
        assert len(operations) - previous <= 100
        count += 1
        if count == 1:
            assert app._history_display_applying is True
            assert app.history_active_load_pending is False
            assert app.global_scanned_set == {'known-barcode'}
            # Exercise actual scan admission while thousands of rows remain.
            app.entry = _FakeEntry('accepted-during-paint')
            app.status_label = _FakeLabel()
            app.is_blinking = False
            app.current_set_info = {'exact_rescan_active': True}
            app._sealed_transfer_exchange_blocks_local_action = lambda action: False
            accepted = []
            app._process_exact_rescan_product = accepted.append
            app.process_input()
            assert accepted == ['accepted-during-paint']
            assert app.entry.deleted is True
    assert maximum < .1  # Baseline single callback exceeds 500 ms with this cost.
    assert count > 100
    assert set(app.history_tree.rows) == {f'set-{i}' for i in range(5000)}
    assert len(app.summary_tree.rows) == 2000


def test_new_history_generation_cancels_queued_display_chunks():
    app, callbacks = history_app()
    app._process_history_queue()
    assert callbacks and app._history_display_applying
    app.history_load_generation = 2
    app.history_queue.put({
        'load_generation': 2, 'updates_active_state': True,
        'scan_count': {}, 'global_scanned_set': {'new'},
        'set_details_map': {'new': {}},
        'sorted_sets': [('new', {'values': [0, 'NEW'], 'tags': ()})],
    })
    app._process_history_queue()
    while callbacks:
        callbacks.pop(0)()
    assert set(app.history_tree.rows) == {'new'}
    assert app.set_details_map == {'new': {}}
    assert app.global_scanned_set == {'new'}


def test_live_completion_and_cancellation_survive_history_display_chunks():
    app, callbacks = history_app(summary_count=2)
    for i in range(200):
        app.history_tree.insert('', 'end', iid=f'old-{i}')
    app.history_tree.insert('', 'end', iid='live', values=('in-progress',))
    app._process_history_queue()
    assert callbacks
    app.history_tree.delete('live')
    app.history_tree.insert('', 'end', iid='live', values=('new-completion',))
    app.set_details_map['live'] = {'set_id': 'live'}
    app._remove_history_details_for_iid('set-4999')
    app.scan_count = {'2026-09-12': {('LIVE', '-'): 3}}
    app._render_summary_tree(app.scan_count)
    while callbacks:
        callbacks.pop(0)()
    assert app.history_tree.rows['live']['values'] == ('new-completion',)
    assert 'set-4999' not in app.history_tree.rows
    assert [row['values'] for row in app.summary_tree.rows.values()] == [('LIVE', '-', 3)]


def test_past_history_paint_removes_current_scan_display_but_keeps_live_indexes():
    app, callbacks = history_app(row_count=1)
    app.current_set_info = {'id': 'live', 'raw': ['LIVE']}
    app.set_details_map = {'live-complete': {}}
    app.global_scanned_set = {'LIVE'}
    app.scan_count = {}
    app.history_tree.insert('', 'end', iid='live', values=('in-progress',))
    result = app.history_queue.get_nowait()
    result['updates_active_state'] = False
    app.history_queue.put(result)
    app._process_history_queue()
    while callbacks:
        callbacks.pop(0)()
    assert set(app.history_tree.rows) == {'set-0'}
    assert app.set_details_map == {'live-complete': {}}
    assert app.global_scanned_set == {'LIVE'}
