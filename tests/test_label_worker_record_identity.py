"""Roster stage two: captured identity, additive recovery and immutable wire bodies."""
import copy
import csv
from datetime import datetime
import json
from pathlib import Path
import subprocess
import sys
from types import SimpleNamespace
import uuid

import pytest

import Label_Match as lm
import label_worker_roster as roster
from package_logistics import (PackageApiError, PackageClientConfig, PackageCommandDraft,
    PackageLogisticsClient, PackageOutbox, PackageOutboxProcessor, PackageTransportError)
from tests.test_label_worker_roster import ALICE, SCOPE, answer
from tests.test_legacy_label_transition import MASTER, _app_for_recovery, _close
from tests.test_w9scanonly_packaging import packaging, products, scan, completions  # noqa: F401
from tests.test_w9scanonly_central import central  # noqa: F401
from tests.test_deferred_lease_clock_retry import clock_case  # noqa: F401
from tests.test_w9scanonly_short_tray import scanner_clock  # noqa: F401

IDENTITY = {'worker_id': ALICE, 'worker_name_snapshot': 'Original worker'}


def select_worker(app, tmp_path, *, enabled):
    app.worker_name = IDENTITY['worker_name_snapshot']
    app.app_settings = {'worker_name': app.worker_name, 'worker_id': ALICE}
    app.app_settings_path = str(tmp_path / 'settings/app_settings.json')
    if not getattr(getattr(app.__dict__.get('package_logistics_client'), 'config', None), 'authority_scope_id', None):
        app.package_logistics_client = SimpleNamespace(config=SimpleNamespace(authority_scope_id=SCOPE))
    app._init_worker_roster()
    response = answer(name=app.worker_name)
    response['authority_scope_id'] = app._roster_scope
    app._apply_worker_roster_answer(0, app._roster_scope, response)
    app._worker_identity_enabled = enabled


def capability(enabled):
    return {'capabilities': {'worker_record_identity_v1': {'enabled': enabled}}}


@pytest.mark.parametrize('enabled', [False, True])
def test_bundle_snapshot_survives_rename_and_disabled_worker_finishes(packaging, tmp_path, enabled):
    app, dialogs = packaging
    select_worker(app, tmp_path, enabled=enabled)
    values = [MASTER, *products()]
    for value in values[:2]:
        scan(app, value)
    saved = app.data_manager.load_current_state()
    assert roster.record_identity(saved) == IDENTITY
    assert roster.record_identity(saved['current_set_info']) == IDENTITY
    changed = answer(2, disabled=True, name='Corrected worker')
    changed['authority_scope_id'] = app._roster_scope
    app._apply_worker_roster_answer(app._roster_request_serial, app._roster_scope, changed)
    app.worker_name = 'Corrected worker'
    for value in values[2:]:
        scan(app, value)
    [(row, details)] = completions(app)
    assert row['worker_name'] == IDENTITY['worker_name_snapshot']
    assert details['scan_count'] == 5
    assert ('worker_id' in details) is enabled
    if enabled:
        assert roster.record_identity(details) == IDENTITY
    [intent] = Path(app.save_directory).glob('_worker_record_identity/*.json')
    durable = json.loads(intent.read_text(encoding='utf-8'))
    assert roster.record_identity(durable) == IDENTITY
    assert durable['current_set_info']['raw'] == values
    selected = []
    app.open_settings_window = lambda: selected.append(True)
    scan(app, MASTER)
    assert selected == [True] and app.current_set_info['raw'] == [] and dialogs == []
    changed = answer(3, name='Corrected worker')
    changed['authority_scope_id'] = app._roster_scope
    app._apply_worker_roster_answer(app._roster_request_serial, app._roster_scope, changed)
    scan(app, MASTER)
    assert app.current_set_info['worker_id'] == ALICE
    assert app.current_set_info['worker_name_snapshot'] == 'Corrected worker'


def test_legacy_temporary_selection_records_null_without_inventing_uuid(packaging):
    app, _dialogs = packaging
    app.worker_id = ALICE  # An old settings value cannot turn fallback into a roster choice.
    app._worker_roster = None
    scan(app, MASTER)
    assert app.current_set_info['worker_id'] is None
    assert app.current_set_info['worker_name_snapshot'] == app.worker_name
    app._worker_identity_enabled = True
    for value in products():
        scan(app, value)
    [(_row, details)] = completions(app)
    assert details['worker_id'] is None and details['worker_name_snapshot'] == app.worker_name


@pytest.mark.parametrize('enabled', [False, True])
def test_shortage_undo_keeps_local_identity_out_of_nested_csv_details(packaging, tmp_path, scanner_clock, enabled):
    app, dialogs = packaging
    select_worker(app, tmp_path, enabled=enabled)
    for value in [MASTER, products()[0], MASTER]:
        scanner_clock[0] += 5
        scan(app, value)
    assert len(completions(app)) == 1
    scan(app, 'KMCMD:UNDO')
    app.data_manager.flush(timeout=5)
    records = [row for path in Path(app.save_directory).rglob('*.csv')
               for row in csv.DictReader(path.open(encoding='utf-8-sig', newline=''))]
    [cancelled] = [json.loads(row['details']) for row in records
                   if row['event'] == app.Events.TRAY_COMPLETION_CANCELLED]
    assert ('worker_id' in cancelled) is enabled
    assert 'worker_id' not in cancelled['details']
    assert 'worker_name_snapshot' not in cancelled['details']
    assert roster.record_identity(app.current_set_info) == IDENTITY
    assert roster.record_identity(app.data_manager.load_current_state()) == IDENTITY
    assert dialogs == []


def test_completion_retry_must_persist_the_frozen_signal_before_logging(packaging, tmp_path, monkeypatch):
    app, _dialogs = packaging
    select_worker(app, tmp_path, enabled=True)
    scan(app, MASTER)
    save = app._save_current_set_state
    monkeypatch.setattr(app, '_save_current_set_state', lambda: False)
    monkeypatch.setattr(app, '_queue_authoritative_package', lambda **kwargs: None)
    arguments = dict(details={'set_id': app.current_set_info['id']}, item_code=app.current_set_info['parsed'][0],
                     is_manual_complete=False, result=app.Results.PASS, central_inherit_all=False,
                     set_id_for_log=app.current_set_info['id'])
    for _attempt in range(2):
        with pytest.raises(lm.PackageLogisticsError):
            app._commit_finalized_set_durable(**arguments)
        assert completions(app) == []
    monkeypatch.setattr(app, '_save_current_set_state', save)
    app._worker_identity_enabled = False
    app._commit_finalized_set_durable(**arguments)
    [(_row, details)] = completions(app)
    assert roster.record_identity(details) == IDENTITY


@pytest.mark.parametrize('enabled', [False, True])
@pytest.mark.parametrize('restart', [False, True])
def test_local_completion_identity_write_failure_keeps_the_checkpoint_decision(packaging, tmp_path, monkeypatch, enabled, restart):
    import label_data_manager as module
    app, dialogs = packaging
    select_worker(app, tmp_path, enabled=enabled)
    write = module.write_checked_bytes
    failed = []
    def interrupt(path, *args, **kwargs):
        if '_worker_record_identity' in Path(path).parts and not failed:
            failed.append(True)
            raise OSError('synthetic intent write interruption')
        return write(path, *args, **kwargs)
    monkeypatch.setattr(module, 'write_checked_bytes', interrupt)
    for value in [MASTER, *products()]:
        scan(app, value)
    assert failed == [True]
    assert len(completions(app)) == 1
    state = app.data_manager.load_current_state()
    decision = IDENTITY if enabled else {}
    assert state['current_set_info']['completion_worker_identity'] == decision
    assert app.current_set_info['completion_worker_identity'] == decision
    restored = None
    try:
        if restart:
            _close(app)
            restored = _app_for_recovery(lm, tmp_path, monkeypatch)
            restored.worker_id, restored.worker_name = ALICE, 'Corrected worker'
            restored._worker_identity_enabled = not enabled
            restored._load_current_set_state()
            retry = restored
        else:
            app._worker_identity_enabled = not enabled
            retry = app
        assert retry.current_set_info['completion_worker_identity'] == decision
        retry._retry_blocked_submission()
        assert len(completions(retry)) == 1
        [intent] = Path(retry.save_directory).glob('_worker_record_identity/*.json')
        assert json.loads(intent.read_text(encoding='utf-8'))['current_set_info']['completion_worker_identity'] == decision
        assert retry.current_set_info['raw'] == [] and dialogs == []
    finally:
        if restored is not None:
            _close(restored)


@pytest.mark.parametrize('enabled', [False, True])
@pytest.mark.parametrize('restart', [False, True])
def test_completion_lane_failure_keeps_first_decision_on_retry(packaging, tmp_path, monkeypatch, enabled, restart):
    from tests.test_tk_serial_ui_lane import FakeTkRoot
    from tk_serial_ui_lane import TkSerialUiLane
    app, dialogs = packaging
    select_worker(app, tmp_path, enabled=enabled)
    values = [MASTER, *products()]
    for value in values[:4]:
        scan(app, value)
    root = FakeTkRoot()
    app._ui_lane_generation = 0
    app._ui_lane_busy_label = app._ui_lane_busy_task = ''
    app._tk_shutdown_requested = False
    app.after, app.after_cancel = root.after, root.after_cancel
    app.ui_lane = TkSerialUiLane(root, poll_ms=1, generation_provider=lambda: app._ui_lane_generation)
    log = app.data_manager.log_event
    def interrupt(event, details, **kwargs):
        if event == app.Events.TRAY_COMPLETE:
            raise OSError('completion interruption before CSV enqueue')
        return log(event, details, **kwargs)
    monkeypatch.setattr(app.data_manager, 'log_event', interrupt)
    restored = None
    try:
        scan(app, values[4])
        root.run_until(lambda: not app.ui_lane.is_busy(), timeout=5)
        assert completions(app) == []
        saved = app.data_manager.load_current_state()['current_set_info']
        decision = IDENTITY if enabled else {}
        assert saved['completion_worker_identity'] == decision
        assert app.current_set_info['completion_worker_identity'] == decision
        assert not list(Path(app.save_directory).glob('_worker_record_identity/*.json'))
        monkeypatch.setattr(app.data_manager, 'log_event', log)
        if restart:
            _close(app)
            restored = _app_for_recovery(lm, tmp_path, monkeypatch)
            restored.worker_id, restored.worker_name = ALICE, 'Corrected worker'
            restored._worker_identity_enabled = not enabled
            restored._load_current_set_state()
            retry = restored
        else:
            app._worker_identity_enabled = not enabled
            retry = app
        assert roster.record_identity(retry.current_set_info) == IDENTITY
        assert retry.current_set_info['completion_worker_identity'] == decision
        retry._retry_blocked_submission()
        root.run_until(lambda: not app.ui_lane.is_busy(), timeout=5)
        [(row, details)] = completions(retry)
        assert row['worker_name'] == IDENTITY['worker_name_snapshot']
        assert ('worker_id' in details) is enabled
        assert retry.current_set_info['raw'] == []
        [intent] = Path(retry.save_directory).glob('_worker_record_identity/*.json')
        assert json.loads(intent.read_text(encoding='utf-8'))['current_set_info']['completion_worker_identity'] == decision
        assert dialogs == []
    finally:
        if app.ui_lane.is_busy():
            root.run_until(lambda: not app.ui_lane.is_busy(), timeout=5)
        app.ui_lane.close_idle()
        if restored is not None:
            _close(restored)


@pytest.mark.parametrize('enabled', [False, True])
def test_uncommitted_completion_undo_can_change_scans_without_archiving(packaging, tmp_path, monkeypatch, enabled):
    app, dialogs = packaging
    select_worker(app, tmp_path, enabled=enabled)
    values = products()
    for value in [MASTER, *values[:3]]:
        scan(app, value)
    log = app.data_manager.log_event
    def interrupt(event, details, **kwargs):
        if event == app.Events.TRAY_COMPLETE:
            raise OSError('completion interruption before CSV enqueue')
        return log(event, details, **kwargs)
    monkeypatch.setattr(app.data_manager, 'log_event', interrupt)
    scan(app, values[3])
    assert completions(app) == []
    assert not list(Path(app.save_directory).glob('_worker_record_identity/*.json'))
    monkeypatch.setattr(app.data_manager, 'log_event', log)
    app._worker_identity_enabled = not enabled
    scan(app, 'KMCMD:UNDO')
    assert len(app.current_set_info['raw']) == 4
    replacement = products(prefix='EDITED')[3]
    scan(app, replacement)
    [(_row, details)] = completions(app)
    assert ('worker_id' in details) is enabled
    [intent] = Path(app.save_directory).glob('_worker_record_identity/*.json')
    original = intent.read_bytes()
    assert json.loads(original)['current_set_info']['raw'][-1] == replacement
    assert app.current_set_info['raw'] == [] and dialogs == []
    # An old immutable archive cannot freeze a still-editable checkpoint.
    state = {'current_set_info': {**json.loads(original)['current_set_info'], 'raw': [MASTER]}}
    assert app.data_manager.save_current_state(state)
    assert intent.read_bytes() == original
    with pytest.raises(ValueError, match='completed worker record identity differs'):
        app.data_manager._save_completed_worker_identity(state['current_set_info'])
    assert intent.read_bytes() == original


@pytest.mark.parametrize('enabled', [False, True])
@pytest.mark.parametrize('restart', [False, True])
def test_csv_completed_archive_retry_rejects_edits_and_keeps_original_snapshot(
        packaging, tmp_path, monkeypatch, enabled, restart):
    import label_data_manager as module
    app, dialogs = packaging
    app._publish_durable_commit_block = lm.Label_Match._publish_durable_commit_block.__get__(app)
    select_worker(app, tmp_path, enabled=enabled)
    write = module.write_checked_bytes
    cuts = []
    def interrupt(path, *args, **kwargs):
        if '_worker_record_identity' in Path(path).parts and not cuts:
            cuts.append(True)
            raise OSError('archive interrupted after CSV completion')
        return write(path, *args, **kwargs)
    monkeypatch.setattr(module, 'write_checked_bytes', interrupt)
    original = [MASTER, *products(prefix='COMPLETED')]
    for value in original:
        scan(app, value)
    assert cuts == [True] and len(completions(app)) == 1
    restored = None
    try:
        retry = app
        if restart:
            _close(app)
            restored = _app_for_recovery(lm, tmp_path, monkeypatch)
            restored._publish_durable_commit_block = lm.Label_Match._publish_durable_commit_block.__get__(restored)
            restored._load_current_set_state()
            retry = restored
        assert retry._workflow_blocking_notice.title == '완료됨 · 기록 마무리 중'
        checkpoint = retry.data_manager.load_current_state()['current_set_info']
        for value in ('KMCMD:UNDO', 'KMCMD:EXCHANGE', original[-1], products(prefix='CHANGED')[3]):
            scan(retry, value)
            assert retry.current_set_info['raw'] == original
            assert not retry.current_set_info.get('scanner_exchange_phase')
        assert retry._reset_current_set(full_reset=True) is False
        assert retry.data_manager.load_current_state()['current_set_info'] == checkpoint
        # A late callback changing the live object must not replace the CSV's
        # completion snapshot during the follow-up retry.
        retry.current_set_info['raw'][-1] = products(prefix='CALLBACK')[3]
        assert retry._save_current_set_state()
        assert retry.data_manager.load_current_state()['current_set_info']['raw'] == original
        retry._worker_identity_enabled = not enabled
        retry._retry_blocked_submission()
        [(_row, details)] = completions(retry)
        [record] = Path(retry.save_directory).glob('_worker_record_identity/*.json')
        saved = json.loads(record.read_text(encoding='utf-8'))['current_set_info']
        assert saved['raw'] == details['scanned_product_barcodes'] == original
        assert saved['completion_worker_identity'] == (IDENTITY if enabled else {})
        assert retry.current_set_info['raw'] == [] and dialogs == []
    finally:
        if restored is not None:
            _close(restored)


def test_csv_completed_outbox_marker_failure_retries_without_requeuing(packaging, tmp_path, monkeypatch):
    app, dialogs = packaging
    select_worker(app, tmp_path, enabled=True)
    original = [MASTER, *products(prefix='MARKER')]
    for value in original[:-1]:
        scan(app, value)
    # Exercise the completion consumer with a durable command whose local
    # marker fails, independent of the scanner's background delivery path.
    app.current_set_info['raw'].append(original[-1])
    app.current_set_info['parsed'].append(MASTER)
    queued, marked = [], []
    metadata = {'idempotency_key': 'completed-marker', 'status': 'PENDING',
                'operation_lease_id': 'completed-lease',
                'operation_lease_completed_at': '2026-10-04T00:00:00Z'}
    def queue(**kwargs):
        queued.append(True)
        return metadata
    def mark(*args, **kwargs):
        marked.append(kwargs)
        if len(marked) == 1:
            raise OSError('outbox marker interrupted after CSV completion')
    monkeypatch.setattr(app, '_queue_authoritative_package', queue)
    monkeypatch.setattr(app.package_outbox, 'mark_local_completion_committed', mark)
    args = dict(details={'set_id': app.current_set_info['id'], 'final_result': app.Results.PASS,
                         'scanned_product_barcodes': original}, item_code=MASTER,
                is_manual_complete=False, result=app.Results.PASS, central_inherit_all=True,
                set_id_for_log=app.current_set_info['id'])
    with pytest.raises(OSError, match='outbox marker interrupted'):
        app._commit_finalized_set_durable(**args)
    scan(app, 'KMCMD:UNDO')
    scan(app, 'KMCMD:EXCHANGE')
    assert app.current_set_info['raw'] == original
    app.current_set_info['raw'][-1] = products(prefix='CALLBACK')[3]
    completed = app._commit_finalized_set_durable(**args)
    assert queued == [True] and len(marked) == 2 and marked[0] == marked[1]
    [(_row, details)] = completions(app)
    [record] = Path(app.save_directory).glob('_worker_record_identity/*.json')
    assert json.loads(record.read_text(encoding='utf-8'))['current_set_info']['raw'] == original
    assert completed['details']['scanned_product_barcodes'] == details['scanned_product_barcodes'] == original
    assert completed['details']['package_logistics'] == details['package_logistics'] == metadata
    assert dialogs == []


@pytest.mark.parametrize('follow_up', ['failure_archive', 'empty_failure_archive', 'shortage_checkpoint'])
def test_csv_completed_failure_or_shortage_follow_up_keeps_its_verdict(
        packaging, tmp_path, monkeypatch, scanner_clock, follow_up):
    import label_data_manager as module
    app, dialogs = packaging
    select_worker(app, tmp_path, enabled=True)
    cuts = []
    original = [] if follow_up == 'empty_failure_archive' else [MASTER, products(prefix='FOLLOWUP')[0]]
    if follow_up.endswith('failure_archive'):
        write = module.write_checked_bytes
        def interrupt(path, *args, **kwargs):
            if '_worker_record_identity' in Path(path).parts and not cuts:
                cuts.append(True)
                raise OSError('failure archive interrupted after CSV completion')
            return write(path, *args, **kwargs)
        monkeypatch.setattr(module, 'write_checked_bytes', interrupt)
    else:
        save = app.data_manager.save_current_state
        def interrupt(state, **kwargs):
            current = state.get('current_set_info') or {}
            if current.get('shortage_undo_snapshot') and not current.get('raw') and not cuts:
                cuts.append(True)
                return False
            return save(state, **kwargs)
        monkeypatch.setattr(app.data_manager, 'save_current_state', interrupt)
    for value in original:
        scanner_clock[0] += 5
        scan(app, value)
    if follow_up.endswith('failure_archive'):
        scan(app, 'OTHER-ITEM-WRONG-PRODUCT')
    else:
        scanner_clock[0] += 5
        scan(app, MASTER)
    assert cuts == [True]
    [(_row, first)] = completions(app)
    before = copy.deepcopy(app.current_set_info)
    scan(app, 'KMCMD:UNDO')
    scan(app, 'KMCMD:EXCHANGE')
    assert app.current_set_info == before
    app._retry_blocked_submission()
    [(_row, final)] = completions(app)
    assert final == first and dialogs == []
    if follow_up.endswith('failure_archive'):
        assert final['final_result'] == (app.Results.FAIL_MISMATCH if original else app.Results.FAIL_INPUT_ERROR)
    else:
        assert final['shortage_confirmed'] is True
        # The documented shortage exception reopens only after the follow-up
        # checkpoint is safely saved, until the next material master label.
        scan(app, 'KMCMD:UNDO')
        assert app.current_set_info['raw'] == original


def test_completed_ui_follow_up_failure_does_not_recount_or_reopen_the_set(packaging, tmp_path, monkeypatch):
    app, dialogs = packaging
    select_worker(app, tmp_path, enabled=True)
    original = [MASTER, *products(prefix='UIFOLLOWUP')]
    configure = app.save_status_label.config
    cuts = []
    def interrupt(**kwargs):
        if not cuts:
            cuts.append(True)
            raise OSError('UI follow-up interrupted after CSV completion')
        return configure(**kwargs)
    monkeypatch.setattr(app.save_status_label, 'config', interrupt)
    for value in original[:-1]:
        scan(app, value)
    with pytest.raises(OSError, match='UI follow-up interrupted'):
        scan(app, original[-1])
    assert len(completions(app)) == 1
    scan(app, 'KMCMD:UNDO')
    scan(app, 'KMCMD:EXCHANGE')
    assert app.current_set_info['raw'] == original
    app._retry_blocked_submission()
    assert len(completions(app)) == 1
    assert sum(count for values in app.scan_count.values() for count in values.values()) == 1
    assert app.current_set_info['raw'] == [] and dialogs == []


@pytest.mark.parametrize('enabled', [False, True])
def test_offline_restart_and_new_process_keep_the_original_identity(packaging, tmp_path, monkeypatch, enabled):
    app, _dialogs = packaging
    select_worker(app, tmp_path, enabled=enabled)
    for value in [MASTER, products()[0]]:
        scan(app, value)
    state_path = Path(app.save_directory) / app.FILES.CURRENT_STATE
    saved = state_path.read_bytes()
    script = ('import json,sys; from label_data_manager import read_recovery_file; '
              'v=read_recovery_file(sys.argv[1],current_state=True); '
              'assert v["verified"]; s=v["value"]["current_set_info"]; '
              'print(json.dumps({k:s[k] for k in ("worker_id","worker_name_snapshot")}))')
    child = subprocess.run([sys.executable, '-B', '-c', script, str(state_path)],
        capture_output=True, text=True, check=True)
    assert json.loads(child.stdout) == IDENTITY and state_path.read_bytes() == saved
    _close(app)
    restored = _app_for_recovery(lm, tmp_path, monkeypatch)
    restored.worker_id = ALICE
    restored.worker_name = 'Corrected worker'
    restored._worker_identity_enabled = enabled
    try:
        restored._load_current_set_state()
        assert roster.record_identity(restored.current_set_info) == IDENTITY
        assert restored._save_current_set_state()
        state = restored.data_manager.load_current_state()
        assert roster.record_identity(state) == IDENTITY
        assert state['worker_name'] == IDENTITY['worker_name_snapshot']
    finally:
        _close(restored)


@pytest.mark.parametrize('enabled', [False, True])
def test_audit_replay_uses_frozen_details_after_signal_and_login_change(tmp_path, enabled):
    manager = lm.DataManager(str(tmp_path), '포장실', IDENTITY['worker_name_snapshot'], 'identity-audit')
    manager.record_context = lambda: (IDENTITY, enabled)
    audit = manager.prepare_audit_event('SCAN_OK', {'set_id': 'audit-set'})
    assert roster.record_identity(audit) == IDENTITY
    assert ('worker_id' in audit['details']) is enabled
    assert manager.save_current_state({'current_set_info': {'id': 'audit-set', 'raw': [MASTER], **IDENTITY},
                                       'scanner_audit_outbox': [audit]})
    manager.close(timeout=5)
    restored = lm.DataManager(str(tmp_path), '포장실', 'Another worker', 'identity-audit')
    restored.record_context = lambda: ({'worker_id': None, 'worker_name_snapshot': 'Another worker'}, not enabled)
    try:
        assert restored.replay_audit_events()
        assert restored.replay_audit_events()
        paths = list(tmp_path.rglob('*.csv'))
        rows = [row for path in paths for row in csv.DictReader(path.open(encoding='utf-8-sig', newline=''))]
        assert len(rows) == 1 and rows[0]['worker_name'] == IDENTITY['worker_name_snapshot']
        assert json.loads(rows[0]['details']) == audit['details']
    finally:
        restored.close(timeout=5)


@pytest.mark.parametrize('enabled', [False, True])
def test_csv_golden_preserves_columns_filename_and_details(tmp_path, enabled):
    from label_data_manager import DataManager
    fixed = SimpleNamespace(now=lambda: datetime(2026, 10, 4, 1, 2, 3))
    manager = DataManager(str(tmp_path), '포장실', IDENTITY['worker_name_snapshot'], 'PC-GOLDEN',
        open_file=open, clock=lambda: fixed, enrich_event=lambda _event, details, _id: details,
        datetime_encoder=lambda: lm.DateTimeEncoder, current_state_filename=lambda: '_state.json',
        durable_event_types=lambda: {'SCAN_OK'}, csv_safe_cell=lambda value: value)
    manager.record_context = lambda: (IDENTITY, enabled)
    details = {'set_id': 'golden-set', 'scanner_exchanges': [{'target_barcode': 'OLD',
        'replacement_barcode': 'NEW', **IDENTITY}]}
    try:
        manager.log_event('SCAN_OK', details)
        manager.flush(timeout=5)
        path = tmp_path / '포장실작업이벤트로그_PC-GOLDEN_20261004.csv'
        assert path.is_file()
        raw = path.read_bytes()
        assert raw.startswith(b'\xef\xbb\xbftimestamp,worker_name,event,details\r\n')
        if not enabled:
            assert raw == (Path(__file__).parent / 'fixtures/worker-record-identity-off.csv').read_bytes()
        row = next(csv.DictReader(path.open(encoding='utf-8-sig', newline='')))
        payload = json.loads(row['details'])
        assert ('worker_id' in payload) is enabled
        if enabled:
            assert roster.record_identity(payload) == IDENTITY
        assert payload['scanner_exchanges'] == [{'target_barcode': 'OLD', 'replacement_barcode': 'NEW'}]
    finally:
        manager.close(timeout=5)


@pytest.mark.parametrize('advertisement', [capability(False), {}, {'capabilities': {}}, capability('true'), None])
def test_unavailable_or_nonboolean_capability_does_not_enable_metadata(advertisement):
    class Client:
        def get_capabilities(self):
            if advertisement is None:
                raise OSError('offline')
            return advertisement
    assert not roster.query_identity_capability(Client())


@pytest.mark.parametrize('enabled', [False, True])
def test_create_request_and_restarted_retry_reuse_saved_body_and_identity(tmp_path, enabled):
    from tests.test_package_logistics import _draft, _projection, _receipt, SCOPE as LOGISTICS_SCOPE
    original = _draft()
    draft = PackageCommandDraft.from_dict({**original.to_dict(), 'worker_identity': IDENTITY})
    outbox = PackageOutbox(tmp_path / 'commands.sqlite3')
    row = outbox.enqueue(draft)
    outbox.mark_local_completion_committed(row['idempotency_key'])
    bodies = []
    gate = [enabled]
    def server(method, url, _headers, body, _timeout):
        if url.endswith('/capabilities'):
            return {'ok': True, 'data': capability(gate[0])}
        if method == 'GET':
            raise PackageApiError(404, 'RECEIPT_NOT_FOUND', 'not yet visible')
        bodies.append(body)
        if len(bodies) == 1:
            raise PackageTransportError('ACK unavailable')
        return {'ok': True, 'data': _receipt(draft)}
    client = PackageLogisticsClient(PackageClientConfig('https://logistics.test', uuid.uuid4().hex,
        LOGISTICS_SCOPE, 'test-host', 'test-device'), transport=server)
    client.get_bundle = lambda *args, **kwargs: _projection()
    assert PackageOutboxProcessor(outbox, client).drain(limit=1) == {'acked': 0, 'retry': 1, 'conflict': 0}
    stored = outbox.get_by_set_id(draft.set_id)
    frozen = stored['command_json']
    payload = json.loads(frozen)['payload']
    assert ('worker_identity' in payload) is enabled
    if enabled:
        assert payload['worker_identity'] == IDENTITY
    else:
        _, baseline_command = client.build_create_package_command(original, idempotency_key=row['idempotency_key'])
        assert bodies[0] == json.dumps(baseline_command, ensure_ascii=False, sort_keys=True).encode('utf-8')
    gate[0] = not enabled
    restarted = PackageOutbox(tmp_path / 'commands.sqlite3')
    with restarted._connect() as conn:
        conn.execute("UPDATE package_command_outbox SET retry_after_at='2000-01-01T00:00:00Z'")
    assert PackageOutboxProcessor(restarted, client).drain(limit=1) == {'acked': 1, 'retry': 0, 'conflict': 0}
    assert bodies[0] == bodies[1]
    assert restarted.get_by_set_id(draft.set_id)['command_json'] == frozen
    assert json.loads(restarted.get_by_set_id(draft.set_id)['draft_json'])['worker_identity'] == IDENTITY


def test_scanner_delivery_keeps_identity_when_rebuilding_source_and_lease(central, tmp_path):
    app, _dialogs, case, root, _releases = central
    from datetime import timezone
    case.instant[0] = datetime(2026, 9, 5, 3, 28, 54, tzinfo=timezone.utc)
    select_worker(app, tmp_path, enabled=True)
    scan(app, case.group['scan_payload'])
    root.run_until(lambda: not app.__dict__.get('_phs2_background_pending'), timeout=5)
    set_id = app.current_set_info['id']
    for member in case.snapshot['work_group_source']['members']:
        scan(app, member['normalized_barcode'])
    rows = completions(app)
    assert roster.record_identity(rows[0][1]) == IDENTITY
    saved = app.package_outbox.get_by_set_id(set_id)
    draft = PackageCommandDraft.from_dict(json.loads(saved['draft_json']))
    assert dict(draft.worker_identity) == IDENTITY
    prepared = app._prepare_scanner_package_delivery(draft)
    assert dict(prepared.worker_identity) == IDENTITY


@pytest.mark.parametrize('enabled', [False, True])
def test_scanner_exchange_local_identity_and_frozen_wire_retry(tmp_path, enabled):
    from sealed_transfer_exchange import SealedTransferExchangeStore, SealedTransferExchangeCoordinator
    from tests.test_sealed_transfer_exchange import _target_projection, _receipt, ITEM
    pair = {'idempotency_key': 'worker-identity-exchange', 'target_barcode': 'BC-OLD',
        'replacement_barcode': 'BC-NEW', 'operator': IDENTITY['worker_name_snapshot'],
        'occurred_at': '2026-10-04T01:02:03Z', **IDENTITY}
    commands = []
    def send(command):
        commands.append(copy.deepcopy(command))
        if len(commands) == 1:
            raise PackageTransportError('ACK unavailable')
        exact = copy.deepcopy(command)
        exact['payload']['pairs'] = [{'old_unit_id': 'unit-old', 'new_unit_id': 'unit-new', 'new_source_bundle_id': None}]
        receipt = _receipt(exact)
        receipt['data']['pairs'][0].update(old_barcode='BC-OLD', new_barcode='BC-NEW', new_source_kind='EXCHANGE_GOOD_REGISTERED')
        receipt['data']['source_membership_changes'] = []
        return receipt
    gate = [enabled]
    client = SimpleNamespace(resolve_package_source_projection=lambda draft: _target_projection(),
        get_receipt_if_exists=lambda *args, **kwargs: None, replace_and_reseal_transfer=send,
        get_capabilities=lambda: capability(gate[0]))
    store = SealedTransferExchangeStore(tmp_path / 'exchange.sqlite3')
    coordinator = SealedTransferExchangeCoordinator(store, client)
    arguments = dict(physical_qr=f'PHS=2|SRC=KMTECH_INPUT_TAG|ITG=TAG-1|CLC={ITEM}|LBL=LABEL-1|HSH=0123456789abcdef',
        item_code=ITEM, local_completion_id='local-set', local_completed_at='2026-10-04T01:03:00Z')
    with pytest.raises(PackageTransportError):
        coordinator.deliver_scanner_exchange(pair, **arguments)
    [saved] = store.blocking_rows(set_id='local-set')
    assert json.loads(saved['old_seal_fields_json'])['worker_identity'] == IDENTITY
    assert ('worker_identity' in commands[0]['payload']) is enabled
    gate[0] = not enabled
    coordinator = SealedTransferExchangeCoordinator(SealedTransferExchangeStore(tmp_path / 'exchange.sqlite3'), client)
    assert coordinator.deliver_scanner_exchange(pair, **arguments)['status'] == 'ACKED'
    assert commands[0] == commands[1]


def test_legacy_state_and_draft_keep_their_absent_identity_and_fingerprint(tmp_path):
    from tests.test_package_logistics import _draft
    draft = _draft()
    assert 'worker_identity' not in draft.to_dict()
    assert PackageCommandDraft.from_dict(draft.to_dict()).fingerprint() == draft.fingerprint()
    manager = lm.DataManager(str(tmp_path), '포장실', 'Old worker', 'legacy-state')
    try:
        assert manager.save_current_state({'current_set_info': {'id': 'old-set', 'raw': [MASTER]}})
        saved = manager.load_current_state()
        assert 'worker_id' not in saved and 'worker_id' not in saved['current_set_info']
    finally:
        manager.close(timeout=5)
