"""Cold-process recovery of the durable short-tray ownership checkpoint."""
import copy
import json
import os
from pathlib import Path
import subprocess
import sys
from types import SimpleNamespace

import pytest

import Label_Match as lm
import label_worker_roster as roster
from tests.test_label_worker_record_identity import IDENTITY, select_worker
from tests.test_label_worker_roster import SCOPE
from tests.test_legacy_label_transition import MASTER, _app_for_recovery, _close
from tests.test_lm_d05_worker_pick import rows
from tests.test_w9scanonly_packaging import packaging, products, scan
from tests.test_w9scanonly_short_tray import scanner_clock


ROOT = Path(__file__).resolve().parents[1]


def produce_checkpoint(directory, enabled):
    patch = pytest.MonkeyPatch()
    fixture = packaging.__wrapped__(directory, patch)
    app, _dialogs = next(fixture)
    select_worker(app, directory, enabled=enabled)
    app._worker_roster_stage_b = enabled
    clock = scanner_clock.__wrapped__(patch)
    save = app.data_manager.save_current_state

    def stop_after_durable_checkpoint(state, **kwargs):
        saved = save(state, **kwargs)
        current = state.get('current_set_info') or {}
        if saved and current.get('shortage_undo_snapshot') and not current.get('id'):
            assert roster.record_identity(current['shortage_undo_snapshot']) == IDENTITY
            assert app.data_manager.flush(timeout=5)
            os._exit(0)
        return saved

    patch.setattr(app.data_manager, 'save_current_state', stop_after_durable_checkpoint)
    for value in [MASTER, products()[0], MASTER, MASTER]:
        clock[0] += 4
        scan(app, value)
    raise AssertionError('durable short-tray checkpoint was not reached')


def recover_checkpoint(directory, enabled):
    patch = pytest.MonkeyPatch()
    app = _app_for_recovery(lm, directory, patch)
    try:
        old_manager = app.data_manager
        unique_id = old_manager.unique_id
        checkpoint = old_manager.load_current_state()['current_set_info']
        snapshot = checkpoint['shortage_undo_snapshot']
        assert not checkpoint.get('id') and checkpoint['raw'] == []
        assert roster.record_identity(snapshot) == IDENTITY
        assert snapshot['id']
        old_manager.close(timeout=5)
        app.data_manager = lm.DataManager(
            app.save_directory, app.Worker.PACKAGING, lm.WORKER_UNDECIDED_LABEL, unique_id)
        app.worker_name = ''
        app.worker_id = None
        app._worker_pick_pending = True
        app.app_settings = {'worker_name': IDENTITY['worker_name_snapshot'], 'worker_id': IDENTITY['worker_id']}
        app.app_settings_path = str(directory / 'restart-settings.json')
        app.package_logistics_client = SimpleNamespace(config=SimpleNamespace(authority_scope_id=SCOPE))
        app._init_worker_roster()
        app._worker_identity_enabled = enabled
        app._worker_roster_stage_b = enabled
        assert app.worker_id is None and app.worker_name == '' and app._worker_pick_pending
        assert app.data_manager.worker_name == lm.WORKER_UNDECIDED_LABEL
        prepared = []
        prepare = app.data_manager.prepare_audit_event

        def observe(event, details, **kwargs):
            audit = prepare(event, details, **kwargs)
            if details.get('short_confirmation_released'):
                prepared.append(copy.deepcopy(audit))
            return audit

        app.data_manager.prepare_audit_event = observe
        app._load_current_set_state()
        assert app.data_manager.flush(timeout=5)
        released = []
        for row in rows(app, app.Events.SCAN_OK):
            details = json.loads(list(row.values())[3])
            if details.get('short_confirmation_released'):
                released.append((row, details))
        assert len(prepared) == len(released) == 1
        [audit] = prepared
        [(row, details)] = released
        assert details['set_id'] == snapshot['id']
        assert len(rows(app, app.Events.TRAY_COMPLETE)) == 1
        return {
            'audit_count': len(prepared), 'csv_count': len(released),
            'audit_identity_preserved': roster.record_identity(audit) == IDENTITY,
            'audit_operator_preserved': audit['details']['scanner_audit']['operator'] == IDENTITY['worker_name_snapshot'],
            'csv_operator_preserved': list(row.values())[1] == IDENTITY['worker_name_snapshot'],
            'csv_identity_preserved': roster.record_identity(details) == (IDENTITY if enabled else {}),
            'unpicked_after_recovery': app._worker_pick_pending and app.worker_name == '' and app.worker_id is None,
        }
    finally:
        _close(app)
        patch.undo()


@pytest.mark.parametrize('enabled', [False, True])
def test_shortage_release_restart_preserves_snapshot_owner(tmp_path, enabled):
    def child(phase):
        return subprocess.run(
            [sys.executable, '-B', '-m', 'tests.test_label_worker_shortage_identity_restart',
             phase, str(tmp_path), str(int(enabled))],
            cwd=ROOT, capture_output=True, text=True,
            creationflags=getattr(subprocess, 'CREATE_NO_WINDOW', 0))

    produced = child('checkpoint')
    assert produced.returncode == 0
    recovered = child('recover')
    assert recovered.returncode == 0
    result = json.loads(recovered.stdout.splitlines()[-1])
    assert result['audit_count'] == result['csv_count'] == 1
    assert result['unpicked_after_recovery']
    assert result['audit_identity_preserved']
    assert result['audit_operator_preserved']
    assert result['csv_operator_preserved']
    assert result['csv_identity_preserved']


if __name__ == '__main__':
    try:
        phase, directory, enabled = sys.argv[1], Path(sys.argv[2]), sys.argv[3] == '1'
        if phase == 'checkpoint':
            produce_checkpoint(directory, enabled)
        elif phase == 'recover':
            print(json.dumps(recover_checkpoint(directory, enabled)))
        else:
            raise ValueError('unknown internal test phase')
    except BaseException as error:
        print(json.dumps({'error_type': type(error).__name__}))
        raise SystemExit(1)
