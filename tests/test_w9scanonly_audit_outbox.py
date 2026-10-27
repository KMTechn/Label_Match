"""Independent checks of the new unfinished-checkpoint retry branch."""
import copy
import pytest

from tests.test_w9scanonly_central import central  # noqa: F401
from tests.test_deferred_lease_clock_retry import clock_case  # noqa: F401
from tests.test_w9scanonly_packaging import packaging, scan, products  # noqa: F401
from tests.test_legacy_label_transition import MASTER, _events
from tests.test_w9scanonly_short_tray import scanner_clock  # noqa: F401


@pytest.mark.parametrize('action', ['exchange', 'undo'])
@pytest.mark.parametrize('append_fault', [False, True])
def test_real_csv_write_failure_restart_preserves_the_action_audit(central, tmp_path, monkeypatch, action, append_fault):
    import Label_Match as lm
    from types import SimpleNamespace
    from tests.test_legacy_label_transition import _app_for_recovery
    app, _dialogs, case, root, _releases = central
    master = case.group['scan_payload']
    target = case.snapshot['work_group_source']['members'][0]['normalized_barcode']
    replacement = case.group['item_id'] + '-NATIVE-AUDIT-REPLACEMENT'
    scan(app, master)
    root.run_until(lambda: not app.__dict__.get('_phs2_background_pending') and not app.ui_lane.is_busy(), timeout=5)
    if action == 'exchange':
        for value in ('KMCMD:EXCHANGE', target):
            scan(app, value)
    else:
        scan(app, target)
        assert app.current_set_info['raw'] == [master, target], 'fault precondition: the product scan must be admitted'
    assert app.data_manager.flush(timeout=5)
    manager = app.data_manager
    original_open = manager._open_file
    rejected = []

    def unavailable_csv(path, mode, *args, **kwargs):
        if str(path).lower().endswith('.csv') and 'a' in mode and not rejected:
            rejected.append(str(path))
            raise OSError('independent native CSV append unavailable')
        return original_open(path, mode, *args, **kwargs)

    if append_fault:
        monkeypatch.setattr(manager, '_open_file', unavailable_csv)
    scan(app, replacement if action == 'exchange' else 'KMCMD:UNDO')
    assert bool(rejected) is append_fault and len(manager._writer_errors) == int(append_fault)
    saved = manager.load_current_state()['current_set_info']
    key = saved['scanner_exchanges'][0]['idempotency_key'] if action == 'exchange' else None
    if append_fault:
        with pytest.raises(RuntimeError, match='independent native CSV append unavailable'):
            manager.close(timeout=5)
    else:
        manager.close(timeout=5)
    assert not manager.log_thread.is_alive()
    restored = _app_for_recovery(lm, tmp_path, monkeypatch)
    # Transfer fixture cleanup to the newly opened manager; the failed writer
    # was joined above and its original error remains in the retained log.
    app.data_manager = restored.data_manager
    for name in ('deferred_intent_capture', 'package_logistics_client', 'package_outbox',
                 'package_operation_lease_store', 'package_operation_lease_keyring'):
        setattr(restored, name, getattr(case.app, name))
    restored.items_data.update(app.items_data)
    restored.operator_workbench_ready = True
    restored._render_operator_workbench = lambda: SimpleNamespace(scan_input_enabled=True)
    restored._start_package_outbox_drain = lambda: None
    restored._load_current_set_state()
    if action == 'exchange':
        assert restored.current_set_info['raw'] == [master, replacement]
        scan(restored, 'KMCMD:UNDO')
        assert restored.current_set_info['raw'] == [master]
    assert restored.data_manager.flush(timeout=5)
    if action == 'exchange':
        audit = _events(restored.save_directory, restored.Events.SCAN_OK)
        assert any(details.get('scanner_exchange', {}).get('idempotency_key') == key
                   for _time, details in audit), 'native append failure + restart + UNDO erased the durable exchange audit'
    else:
        audit = _events(restored.save_directory, restored.Events.SET_RESTORED)
        assert any(details.get('reason') == 'SCANNER_UNDO' and details.get('scanned_product_barcodes') == [master]
                   for _time, details in audit), 'native append failure + restart did not recover the saved UNDO audit'


@pytest.mark.parametrize('append_fault', [False, True])
def test_exchange_append_retry_keeps_the_exchange_audit_after_undo(central, monkeypatch, append_fault):
    app, dialogs, case, root, _releases = central
    master = case.group['scan_payload']
    target = case.snapshot['work_group_source']['members'][0]['normalized_barcode']
    replacement = case.group['item_id'] + '-REVIEW-AUDIT-REPLACEMENT'
    scan(app, master)
    root.run_until(lambda: not app.__dict__.get('_phs2_background_pending'), timeout=5)
    for value in ('KMCMD:EXCHANGE', target):
        scan(app, value)
    log = app.data_manager.log_event

    def fail_exchange(event, details):
        if details.get('scanner_exchange'):
            raise OSError('independent review: exchange audit append unavailable')
        return log(event, details)

    if append_fault:
        monkeypatch.setattr(app.data_manager, 'log_event', fail_exchange)
    scan(app, replacement)
    pair = copy.deepcopy(app.data_manager.load_current_state()['current_set_info']['scanner_exchanges'][0])
    monkeypatch.setattr(app.data_manager, 'log_event', log)
    if append_fault:
        app._retry_blocked_submission()
    scan(app, 'KMCMD:UNDO')
    assert app.data_manager.flush(timeout=5)
    assert app.current_set_info['raw'] == [master]
    assert not app.data_manager.load_current_state()['current_set_info'].get('scanner_exchanges')
    events = _events(app.save_directory, app.Events.SCAN_OK)
    assert any(details.get('scanner_exchange', {}).get('idempotency_key') == pair['idempotency_key']
               for _time, details in events), 'accepted durable exchange vanished from the audit after retry and UNDO'
    assert dialogs == []


def test_pending_exchange_and_undo_survive_another_scan_without_blocking_input(central, monkeypatch):
    app, dialogs, case, root, _ = central
    master = case.group['scan_payload']
    members = case.snapshot['work_group_source']['members']
    target = members[0]['normalized_barcode']
    replacement = case.group['item_id'] + '-OUTBOX-REPLACEMENT'
    scan(app, master)
    root.run_until(lambda: not app.__dict__.get('_phs2_background_pending') and not app.ui_lane.is_busy(), timeout=5)
    log = app.data_manager.log_event

    def unavailable(event, details):
        if details.get('scanner_exchange') or details.get('reason') == 'SCANNER_UNDO':
            raise OSError('audit unavailable while the scanner continues')
        return log(event, details)

    monkeypatch.setattr(app.data_manager, 'log_event', unavailable)
    for value in ('KMCMD:EXCHANGE', target, replacement, 'KMCMD:UNDO'):
        scan(app, value)
    assert app.current_set_info['raw'] == [master]
    pending = app.data_manager.load_current_state()['scanner_audit_outbox']
    assert len(pending) == 2
    assert not app.blocks
    assert not app.__dict__.get('_workflow_blocking_notice')
    frozen = copy.deepcopy(pending)
    scan(app, members[1]['normalized_barcode'])
    assert app.current_set_info['raw'] == [master, members[1]['normalized_barcode']]
    assert app.data_manager.load_current_state()['scanner_audit_outbox'] == frozen
    monkeypatch.setattr(app.data_manager, 'log_event', log)
    app._retry_blocked_submission()
    assert not app.data_manager.load_current_state().get('scanner_audit_outbox')
    assert dialogs == []


def test_restart_replay_after_csv_fsync_and_checkpoint_cleanup_failure_is_idempotent(packaging, monkeypatch):
    import csv
    import json
    from pathlib import Path
    import Label_Match as lm
    app, _ = packaging
    for value in (MASTER, products()[0]):
        scan(app, value)
    manager = app.data_manager
    record = manager.prepare_audit_event(app.Events.SET_RESTORED, {
        'set_id': app.current_set_info['id'], 'reason': 'SCANNER_UNDO',
        'scanned_product_barcodes': [MASTER]})
    assert manager.save_current_state({'current_set_info': app.current_set_info,
                                       'scanner_audit_outbox': [record]})
    save = manager.save_current_state
    monkeypatch.setattr(manager, 'save_current_state', lambda *args, **kwargs: False)
    assert not manager.replay_audit_events()
    # The CSV is durable but acknowledging it in the checkpoint failed.
    assert manager.load_current_state()['scanner_audit_outbox'] == [record]
    monkeypatch.setattr(manager, 'save_current_state', save)
    manager.close(timeout=5)
    app.data_manager = lm.DataManager(app.save_directory, manager.process_name,
                                     'NEXT-SHIFT', manager.unique_id)
    assert app.data_manager.replay_audit_events()
    assert app.data_manager.replay_audit_events()
    rows = []
    for path in Path(app.save_directory).rglob('*.csv'):
        with path.open(encoding='utf-8-sig', newline='') as stream:
            rows.extend(row for row in csv.DictReader(stream)
                        if json.loads(row['details']).get('scanner_audit') == record['details']['scanner_audit'])
    assert len(rows) == 1
    assert rows[0]['timestamp'] == record['details']['scanner_audit']['occurred_at']
    assert rows[0]['worker_name'] == record['details']['scanner_audit']['operator']
    assert not app.data_manager.load_current_state().get('scanner_audit_outbox')
    assert app.data_manager.load_current_state()['worker_name'] == manager.worker_name


def test_reset_preserves_pending_audit_without_resurrecting_the_old_set(packaging):
    app, _ = packaging
    scan(app, MASTER)
    manager = app.data_manager
    record = manager.prepare_audit_event(app.Events.SET_RESTORED, {
        'set_id': app.current_set_info['id'], 'reason': 'SCANNER_UNDO',
        'scanned_product_barcodes': []})
    assert manager.save_current_state({'current_set_info': app.current_set_info,
                                       'scanner_audit_outbox': [record]})
    manager.delete_current_state()
    saved = manager.load_current_state()
    assert saved['current_set_info']['raw'] == []
    assert saved['scanner_audit_outbox'] == [record]
    assert manager.replay_audit_events()


def test_empty_audit_identity_cannot_replace_the_saved_checkpoint(packaging):
    app, _ = packaging
    scan(app, MASTER)
    manager = app.data_manager
    before = manager.load_current_state()
    event = manager.prepare_audit_event(app.Events.SET_RESTORED, {'reason': 'SCANNER_UNDO'})
    event['details']['scanner_audit']['key'] = ''
    assert not manager.save_current_state({'current_set_info': app.current_set_info,
                                          'scanner_audit_outbox': [event]})
    assert manager.load_current_state() == before


@pytest.mark.parametrize('origin', ['snapshot', 'archive', 'live-snapshot'])
def test_held_audit_replays_once_after_two_restarts_without_changing_pin_evidence(packaging, origin):
    import hashlib
    from pathlib import Path
    import Label_Match as lm
    app, _ = packaging
    scan(app, MASTER)
    manager = app.data_manager
    record = manager.prepare_audit_event(app.Events.SET_RESTORED, {
        'set_id': app.current_set_info['id'], 'reason': 'SCANNER_UNDO'})
    assert manager.save_current_state({'current_set_info': app.current_set_info,
                                       'scanner_audit_outbox': [record]})
    state = manager.load_current_state()
    current = Path(app.save_directory) / manager._current_state_filename()
    original = current.read_bytes()
    digest = hashlib.sha256(original).hexdigest()
    archive = current.with_name(current.name + '.held-' + digest)
    # The hold preserves the original checkpoint and its PIN-bound bytes.
    app.package_outbox.hold_workbench_set(
        set_id=str(app.current_set_info['id']), source_phs2='', source_input_tag_id='',
        snapshot=state if origin != 'archive' else {
            'archive_path': str(archive), 'current_file_sha256': digest},
        reason='synthetic verified hold', held_by='TEST-ADMIN')
    held = app.package_outbox.list_workbench_holds()[0]['snapshot_json']
    if origin != 'live-snapshot':
        current.replace(archive)
    manager.close(timeout=5)
    for _restart in range(2):
        app.data_manager = lm.DataManager(app.save_directory, manager.process_name,
                                         'NEXT-SHIFT', manager.unique_id)
        app.current_set_info = {'id': None, 'raw': [], 'parsed': []}
        if origin == 'live-snapshot':
            app._replay_scanner_audits(include_holds=True)
        else:
            app._load_current_set_state()
        assert app.data_manager.flush(timeout=5)
        rows = [details for _, details in _events(app.save_directory, app.Events.SET_RESTORED)
                if details.get('scanner_audit') == record['details']['scanner_audit']]
        assert len(rows) == 1
        if origin == 'live-snapshot':
            assert current.read_bytes() == original, 'audit ACK must preserve the PIN-bound active-file SHA'
        else:
            assert not current.exists(), 'audit replay must preserve PIN-bound active-file absence'
            assert archive.read_bytes() == original
        assert app.package_outbox.list_workbench_holds()[0]['snapshot_json'] == held
        app.data_manager.close(timeout=5)


def test_csv_fsync_failure_replays_the_same_row_and_preserves_original_error(packaging, monkeypatch):
    from contextlib import contextmanager
    import label_data_manager as storage
    app, _ = packaging
    scan(app, MASTER)
    manager = app.data_manager
    record = manager.prepare_audit_event(app.Events.SET_RESTORED, {
        'set_id': app.current_set_info['id'], 'reason': 'SCANNER_UNDO'})
    assert manager.save_current_state({'current_set_info': app.current_set_info,
                                       'scanner_audit_outbox': [record]})
    original_open, original_fsync = manager._open_file, storage.os.fsync
    csv_handles = set()

    @contextmanager
    def track_csv(*args, **kwargs):
        with original_open(*args, **kwargs) as stream:
            if str(args[0]).endswith('.csv'):
                csv_handles.add(stream.fileno())
            try:
                yield stream
            finally:
                csv_handles.discard(stream.fileno())

    def unavailable_fsync(fd):
        if fd in csv_handles:
            raise OSError('native CSV fsync unavailable')
        return original_fsync(fd)

    monkeypatch.setattr(manager, '_open_file', track_csv)
    monkeypatch.setattr(storage.os, 'fsync', unavailable_fsync)
    assert not manager.replay_audit_events()
    errors = list(manager._writer_errors)
    assert len(errors) == 1
    assert manager.load_current_state()['scanner_audit_outbox'] == [record]
    monkeypatch.setattr(storage.os, 'fsync', original_fsync)
    assert manager.replay_audit_events()
    assert manager._writer_errors == errors
    assert manager.flush(timeout=5)
    matching = [details for _, details in _events(app.save_directory, app.Events.SET_RESTORED)
                if details.get('scanner_audit') == record['details']['scanner_audit']]
    assert len(matching) == 1


def test_shortage_release_retries_with_one_original_audit(packaging, scanner_clock, monkeypatch):
    app, _ = packaging
    app.progress_bar.configure = app.progress_bar.update
    for value in (MASTER, products()[0], MASTER):
        scanner_clock[0] += 5
        scan(app, value)
    assert app.current_set_info.get('shortage_undo_snapshot')
    save = app.data_manager.save_current_state
    failed = []

    def checkpoint_failure(state, **kwargs):
        if 'scanner_audit_outbox' not in state and not state['current_set_info'].get('shortage_release_pending') and not failed:
            failed.append(True)
            return False
        return save(state, **kwargs)

    monkeypatch.setattr(app.data_manager, 'save_current_state', checkpoint_failure)
    assert not app._release_shortage_confirmation()
    assert failed
    monkeypatch.setattr(app.data_manager, 'save_current_state', save)
    assert app._release_shortage_confirmation()
    events = [details for _, details in _events(app.save_directory, app.Events.SCAN_OK)
              if details.get('short_confirmation_released')]
    assert len(events) == 1


def test_late_source_review_audit_survives_csv_failure_and_next_scan(central, monkeypatch):
    import threading
    app, dialogs, case, root, releases = central
    entered, release = threading.Event(), threading.Event()
    releases.append(release)
    resolve = app.package_logistics_client.resolve_package_source_evidence

    def delayed(draft):
        entered.set()
        release.wait()
        return resolve(draft)

    monkeypatch.setattr(app.package_logistics_client, 'resolve_package_source_evidence', delayed)
    master = case.group['scan_payload']
    scan(app, master)
    root.run_until(entered.is_set, timeout=5)
    scan(app, case.group['item_id'] + '-UNKNOWN-SAMPLE')
    log = app.data_manager.log_event

    def unavailable(event, details):
        if event == app.Events.POST_REVIEW_REQUIRED:
            raise OSError('review CSV unavailable')
        return log(event, details)

    monkeypatch.setattr(app.data_manager, 'log_event', unavailable)
    release.set()
    root.run_until(lambda: not app.__dict__.get('_phs2_background_pending'), timeout=5)
    saved = app.data_manager.load_current_state()
    pending = saved['scanner_audit_outbox']
    assert len(pending) == 1 and pending[0]['event_type'] == app.Events.POST_REVIEW_REQUIRED
    assert not app.blocks
    monkeypatch.setattr(app.data_manager, 'log_event', log)
    member = case.snapshot['work_group_source']['members'][0]['normalized_barcode']
    scan(app, member)
    assert app.current_set_info['raw'] == [master, case.group['item_id'] + '-UNKNOWN-SAMPLE', member]
    assert not app.data_manager.load_current_state().get('scanner_audit_outbox')
    assert len(_events(app.save_directory, app.Events.POST_REVIEW_REQUIRED)) == 1
    assert dialogs == []


@pytest.mark.parametrize('append_fault', [False, True])
def test_undo_append_retry_replays_the_undo_audit(packaging, monkeypatch, append_fault):
    app, dialogs = packaging
    for value in (MASTER, products()[0]):
        scan(app, value)
    log = app.data_manager.log_event

    def fail_undo(event, details):
        if event == app.Events.SET_RESTORED:
            raise OSError('independent review: undo audit append unavailable')
        return log(event, details)

    if append_fault:
        monkeypatch.setattr(app.data_manager, 'log_event', fail_undo)
    scan(app, 'KMCMD:UNDO')
    monkeypatch.setattr(app.data_manager, 'log_event', log)
    if append_fault:
        app._retry_blocked_submission()
    assert app.data_manager.flush(timeout=5)
    assert app.current_set_info['raw'] == [MASTER]
    events = _events(app.save_directory, app.Events.SET_RESTORED)
    assert any(details.get('reason') == 'SCANNER_UNDO' and details.get('scanned_product_barcodes') == [MASTER]
               for _time, details in events), 'successful retry left the durable audit without its undo event'
    assert dialogs == []
