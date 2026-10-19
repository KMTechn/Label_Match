"""Record ownership through completed-tray cancellation and ordinary CSV writes."""
import copy
import csv
import json
from pathlib import Path
import uuid

import pytest

import Label_Match as lm
import label_worker_roster as roster
from tests.test_label_worker_record_identity import (
    IDENTITY, select_worker,
    test_f2_completed_tray_cancel_keeps_local_identity_out_of_nested_csv_details as run_f2,
)
from tests.test_w9scanonly_packaging import packaging, products, scan, completions
from tests.test_legacy_label_transition import MASTER, _app_for_recovery, _close
from tests.test_lm_d05_worker_pick import pending_start, enable_save, rows
from tests.test_label_worker_roster import BOB


def cancellation_details(app):
    assert app.data_manager.flush(timeout=5)
    records = [row for path in Path(app.save_directory).rglob('*.csv')
               for row in csv.DictReader(path.open(encoding='utf-8-sig', newline=''))]
    cancelled = [json.loads(row['details']) for row in records
                 if row['event'] == app.Events.TRAY_COMPLETION_CANCELLED]
    assert len(cancelled) == 1
    return cancelled[0]


@pytest.mark.parametrize('enabled', [True, False])
def test_f2_cancel_records_exact_selected_identity(packaging, tmp_path, enabled):
    run_f2(packaging, tmp_path, enabled)
    app, _dialogs = packaging
    details = cancellation_details(app)
    assert ('worker_name_snapshot' in details) is enabled
    assert roster.record_identity(details) == (IDENTITY if enabled else {})




def test_completion_keeps_set_owner_when_selected_person_changes(packaging, tmp_path):
    app, dialogs = packaging
    select_worker(app, tmp_path, enabled=True)
    scan(app, MASTER)
    original = copy.deepcopy(app.current_set_info)
    app.worker_id = str(uuid.uuid4())
    app.worker_name = '다음 작업자'
    for value in products():
        scan(app, value)
    [(_row, details)] = completions(app)
    assert roster.record_identity(original) == IDENTITY
    assert roster.record_identity(details) == IDENTITY
    assert dialogs == []


def test_unselected_worker_leaves_out_of_set_identity_absent(packaging):
    app, _dialogs = packaging
    app.worker_name = ''
    app.worker_id = None
    app._worker_pick_pending = True
    app._worker_identity_enabled = True
    identity, enabled = app._worker_record_context()
    assert identity == {}
    assert enabled is True
    app.data_manager.log_event(app.Events.APP_START, {})
    assert app.data_manager.flush(timeout=5)
    records = [row for path in Path(app.save_directory).rglob('*.csv')
               for row in csv.DictReader(path.open(encoding='utf-8-sig', newline=''))]
    [details] = [json.loads(row['details']) for row in records
                 if row['event'] == app.Events.APP_START]
    assert 'worker_id' not in details
    assert 'worker_name_snapshot' not in details


@pytest.mark.parametrize('v1,stage_b', [(True, False), (False, True)])
def test_selected_local_worker_uses_stage_b_gate(packaging, tmp_path, monkeypatch, v1, stage_b):
    app, _dialogs = packaging
    select_worker(app, tmp_path, enabled=v1)
    app._worker_roster_stage_b = stage_b
    store = app._roster_b
    store.local['workers'].append({'worker_id': IDENTITY['worker_id']})
    monkeypatch.setattr(store, 'active_worker', lambda **kwargs: {
        'worker_id': IDENTITY['worker_id'], 'display_name': IDENTITY['worker_name_snapshot']})
    assert app._worker_record_context() == (IDENTITY, stage_b)


def test_shortage_snapshot_owner_wins_over_current_selection(packaging, tmp_path):
    app, _dialogs = packaging
    select_worker(app, tmp_path, enabled=True)
    scan(app, MASTER)
    app.current_set_info['shortage_undo_snapshot'] = dict(IDENTITY)
    app.worker_id = str(uuid.uuid4())
    app.worker_name = '다음 작업자'
    assert app._worker_record_context() == (IDENTITY, True)


def test_legacy_selection_keeps_null_worker_id(packaging):
    app, _dialogs = packaging
    app._roster_b = None
    app._worker_roster = None
    app.worker_id = str(uuid.uuid4())
    app._worker_identity_enabled = True
    identity, enabled = app._worker_record_context()
    assert identity == {'worker_id': None, 'worker_name_snapshot': app.worker_name}
    assert enabled is True


def test_first_scan_attempt_uses_selected_worker_before_set_acceptance(packaging, tmp_path):
    app, _dialogs = packaging
    select_worker(app, tmp_path, enabled=True)
    assert not app.current_set_info.get('id')
    observed = []
    log_event = app.data_manager.log_event

    def observe(event, details, **kwargs):
        if event == app.Events.SCAN_ATTEMPT:
            observed.append((bool(app.current_set_info.get('id')), app._worker_record_context()))
        return log_event(event, details, **kwargs)

    app.data_manager.log_event = observe
    scan(app, MASTER)
    assert observed == [(False, (IDENTITY, True))]
    assert app.current_set_info.get('id')
    [attempt] = rows(app, app.Events.SCAN_ATTEMPT)
    assert roster.record_identity(json.loads(attempt['details'])) == IDENTITY


@pytest.mark.parametrize('event_kind', ['scan', 'complete'])
def test_identityless_checkpoint_keeps_absent_identity_after_first_pick(
        packaging, tmp_path, monkeypatch, event_kind):
    app, _dialogs = packaging
    select_worker(app, tmp_path, enabled=True)
    values = products()
    scan(app, MASTER)
    scan(app, values[0])
    checkpoint = app.data_manager.load_current_state()
    for record in (checkpoint, checkpoint['current_set_info']):
        record.pop('worker_id', None)
        record.pop('worker_name_snapshot', None)
    original = copy.deepcopy(checkpoint['current_set_info'])
    assert original['id'] and len(original['raw']) == 2
    assert app.data_manager.save_current_state(checkpoint)
    _close(app)
    restored = _app_for_recovery(lm, tmp_path, monkeypatch)
    try:
        restored._worker_pick_pending = True
        restored.worker_name = ''
        restored._load_current_set_state()
        assert restored.current_set_info['id'] == original['id']
        assert restored.current_set_info['raw'] == original['raw']
        assert roster.record_identity(restored.current_set_info) == {}
        selection_root = tmp_path / 'first-pick'
        selection_root.mkdir()
        pending_start(restored, selection_root,
                      {'worker_name': IDENTITY['worker_name_snapshot'], 'worker_id': IDENTITY['worker_id']},
                      current=restored.current_set_info)
        assert restored._worker_pick_pending and not restored._roster_worker_allowed()
        manager, directory, history = restored.data_manager, restored.save_directory, restored.history_tree
        unique_id = manager.unique_id
        enable_save(restored, tmp_path)
        restored.data_manager.close(timeout=5)
        restored.data_manager, restored.save_directory = manager, directory
        restored.history_tree, restored.unique_id = history, unique_id
        selected = next(worker for worker in restored._worker_roster_choices() if worker['worker_id'] == BOB)
        assert selected['worker_id'] != IDENTITY['worker_id']
        restored._save_settings_and_close(object(), selected['display_name'])
        assert restored.worker_id == BOB and not restored._worker_pick_pending
        assert restored.current_set_info['id'] == original['id']
        assert roster.record_identity(restored.current_set_info) == {}
        restored._worker_identity_enabled = True
        for value in values[1:]:
            scan(restored, value)
        [(_row, completion)] = completions(restored)
        attempts = [json.loads(row['details']) for row in rows(restored, restored.Events.SCAN_ATTEMPT)]
        remaining = [details for details in attempts if details.get('raw_input') in values[1:]]
        assert len(remaining) == len(values[1:])
        records = remaining if event_kind == 'scan' else [completion]
        assert records
        for details in records:
            assert 'worker_id' not in details
            assert 'worker_name_snapshot' not in details
    finally:
        _close(restored)


@pytest.mark.parametrize('captured', [False, True])
def test_idle_shortage_snapshot_does_not_fall_through_to_selected_worker(packaging, tmp_path, captured):
    app, _dialogs = packaging
    select_worker(app, tmp_path, enabled=True)
    app._worker_roster_stage_b = True
    scan(app, MASTER)
    snapshot = copy.deepcopy(app.current_set_info)
    if not captured:
        snapshot.pop('worker_id')
        snapshot.pop('worker_name_snapshot')
    app.current_set_info = {'id': None, 'raw': [], 'shortage_undo_snapshot': snapshot}
    app.worker_id = str(uuid.uuid4())
    app.worker_name += ' (선택)'
    assert app._worker_record_context() == (IDENTITY if captured else {}, True)
