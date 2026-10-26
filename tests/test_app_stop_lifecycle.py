"""Preserving stop requests across startup, retry and process-tree shutdown."""
import json
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

import Label_Match as lm
import auto_update
import user_relay
from tests.test_f6_pointer_confirmation import app


def write_request(root):
    path = user_relay.app_stop_request_path(root)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps({
        'schema_version': user_relay.APP_STOP_SCHEMA,
        'request_id': 'a' * 32,
        'requested_at': datetime.now(timezone.utc).isoformat(),
    }), encoding='utf-8')
    return path


@pytest.mark.parametrize('stage', ['installation', 'onboarding'])
def test_startup_request_stops_at_completed_unit(monkeypatch, tmp_path, stage):
    monkeypatch.setenv('LABEL_MATCH_DIRECT_SYNC_ROOT', str(tmp_path))
    monkeypatch.setattr(lm, 'release_identity', lambda _: {})
    monkeypatch.setattr(auto_update, 'startup_gate', lambda *a: None)
    monkeypatch.setattr(lm, 'dispatch_product_mode', lambda _: None)
    monkeypatch.setattr(lm, 'verify_factory_contract_startup', lambda: None)
    monkeypatch.setattr(lm, '_label_match_startup_trace', lambda *a, **k: None)
    monkeypatch.setattr(lm, '_first_run_onboarding_enabled', lambda: True)
    monkeypatch.setattr(lm, 'requires_bootstrap_integrity', lambda _: False)
    monkeypatch.setattr(lm, 'check_user_data_open', lambda _: None)
    completed = []
    def install(_):
        if stage == 'installation':
            write_request(tmp_path)
        completed.append('installation')
    def onboarding(*a, **k):
        if stage == 'onboarding':
            write_request(tmp_path)
        completed.append('onboarding')
        return {}
    monkeypatch.setattr(lm, 'validate_pending_worker_install', install)
    monkeypatch.setattr(lm, 'onboard_current_user', onboarding)
    start = Mock(side_effect=AssertionError('application must remain stopped'))
    monkeypatch.setattr(lm, 'run_guarded_entrypoint', start)
    assert lm.main([]) == 0
    assert completed == (['installation'] if stage == 'installation' else ['installation', 'onboarding'])
    start.assert_not_called()


def test_catalog_request_stops_before_retry_dialog(monkeypatch, tmp_path):
    monkeypatch.setenv('LABEL_MATCH_DIRECT_SYNC_ROOT', str(tmp_path))
    import item_catalog_sync
    def catalog():
        write_request(tmp_path)
        raise lm.ItemCatalogSyncError('unavailable', cause_code=item_catalog_sync.REQUEST_FAILED_NO_CACHE)
    monkeypatch.setattr(lm, 'prepare_startup_item_catalog', catalog)
    monkeypatch.setattr(lm, 'offline_catalog_startable', lambda **k: False)
    retry = Mock(side_effect=AssertionError('retry window must remain closed'))
    monkeypatch.setattr(lm, '_offer_item_catalog_startup_retry', retry)
    assert lm._run_label_match_application() == 0
    retry.assert_not_called()


def test_catalog_retry_window_observes_request(monkeypatch, tmp_path):
    monkeypatch.setenv('LABEL_MATCH_DIRECT_SYNC_ROOT', str(tmp_path))
    callbacks = []
    parent = SimpleNamespace(withdraw=Mock(), destroy=Mock(), after=lambda ms, cb: callbacks.append(cb))
    monkeypatch.setattr(lm.tk, 'Tk', lambda: parent)
    def retry(*a, **k):
        write_request(tmp_path)
        assert callbacks, 'stop poll must be armed while the modal is open'
        callbacks[0]()
        assert parent.destroy.called
        return False
    monkeypatch.setattr(lm.messagebox, 'askretrycancel', retry)
    import item_catalog_sync
    error = lm.ItemCatalogSyncError('unavailable', cause_code=item_catalog_sync.REQUEST_FAILED_NO_CACHE)
    assert lm._offer_item_catalog_startup_retry(error) is False
    assert callbacks
    assert parent.destroy.called


def test_initial_load_request_uses_safe_destroy(app, tmp_path):
    app.initialized_successfully = False
    app.direct_sync_bootstrap_context = {'program_data_root': str(tmp_path)}
    write_request(tmp_path)
    app.destroy = Mock(side_effect=lambda: setattr(app, '_tk_destroy_complete', True))
    app.on_closing = Mock(side_effect=AssertionError('not ready for ordinary close'))
    app.after = Mock()
    app._poll_app_stop_request()
    app.destroy.assert_called_once()
    app.on_closing.assert_not_called()


def test_poll_survives_consumed_request_and_aborted_close(app, tmp_path):
    app.current_set_info['raw'] = []
    app.direct_sync_bootstrap_context = {'program_data_root': str(tmp_path)}
    app.after = Mock(return_value='poll')
    def close(**k):
        app._app_close_in_progress = True
    app.on_closing = Mock(side_effect=close)
    write_request(tmp_path)
    app._poll_app_stop_request()
    assert app.after.call_count == 1
    app._app_close_in_progress = False
    write_request(tmp_path)
    app._poll_app_stop_request()
    assert app.on_closing.call_count == 2


def test_mainloop_deadline_marks_and_exits(app, monkeypatch, tmp_path):
    app.save_directory = str(tmp_path)
    app._app_close_in_progress = True
    app._tracked_tk_shutdown_threads = lambda: []
    app.update = Mock(side_effect=AssertionError('shutdown deadline did not exit'))
    monkeypatch.setattr(lm, 'LABEL_MATCH_APP_CLOSE_PROCESS_TIMEOUT_SECONDS', 0, raising=False)
    monkeypatch.setattr(lm.threading, 'Timer', lambda *a, **k: SimpleNamespace(start=Mock(), cancel=Mock()))
    class ProcessExit(BaseException):
        pass
    exit_process = Mock(side_effect=ProcessExit)
    monkeypatch.setattr(lm.os, '_exit', exit_process)
    with pytest.raises(ProcessExit):
        app._finish_after_mainloop()
    exit_process.assert_called_once_with(1)
    assert (tmp_path / 'label_match_shutdown_stall.json').is_file()


def test_real_close_abort_rearms_one_poll_and_next_request_closes(app, tmp_path):
    app.current_set_info['raw'] = []
    app.direct_sync_bootstrap_context = {'program_data_root': str(tmp_path)}
    app.after = Mock(return_value='poll')
    app._resume_deferred_validation_after_close_cancel = Mock(return_value=True)
    app._start_package_outbox_drain = Mock()
    app.data_manager.close.side_effect = RuntimeError('temporary save failure')
    write_request(tmp_path)
    app._poll_app_stop_request()
    assert not app._app_close_in_progress
    assert app.after.call_count == 1
    assert user_relay.app_stop_request_path(tmp_path).exists()
    app.data_manager.close.side_effect = None
    write_request(tmp_path)
    app._poll_app_stop_request()
    assert app._app_close_in_progress
    assert not user_relay.app_stop_request_path(tmp_path).exists()


def process(pid, parent, image, command='', created=1, name='pythonw.exe'):
    return {'pid': pid, 'parent_pid': parent, 'image': str(image), 'command': command,
            'created_at': created, 'name': name, 'session_id': 1}


def test_stop_ownership_tracks_webview_descendants_and_pid_reuse(tmp_path):
    root, udf = tmp_path / 'app', tmp_path / 'data' / 'webview2'
    python = process(10, 1, root / 'runtime' / 'pythonw.exe')
    browser = process(20, 10, 'msedgewebview2.exe', f'webview --user-data-dir="{udf}"',
                      created=2, name='msedgewebview2.exe')
    child = process(30, 20, 'msedgewebview2.exe', created=3, name='msedgewebview2.exe')
    other = process(40, 1, 'msedgewebview2.exe', f'webview --user-data-dir="{udf}-other"',
                    name='msedgewebview2.exe')
    known = {}
    assert {r['pid'] for r in user_relay._owned_app_stop_processes(
        [python, browser, child, other], root, udf, known)} == {10, 20, 30}
    assert {r['pid'] for r in user_relay._owned_app_stop_processes([child, other], root, udf, known)} == {30}
    reused = dict(child, created_at=4, parent_pid=1)
    assert user_relay._owned_app_stop_processes([reused, other], root, udf, known) == []


def test_stop_result_waits_for_app_tree_and_reports_remaining(monkeypatch, tmp_path):
    root, udf = tmp_path / 'app', tmp_path / 'data' / 'webview2'
    row = process(10, 1, root / 'runtime' / 'pythonw.exe')
    monkeypatch.setattr(user_relay, 'request_user_relay_stop', lambda root, **k: {'status': 'ABSENT'})
    monkeypatch.setattr(user_relay, '_app_stop_process_snapshot', lambda **k: [row], raising=False)
    report = user_relay.request_app_stop(tmp_path / 'relay', app_root=root, webview_root=udf, timeout_seconds=1)
    assert report['status'] == 'UNKNOWN'
    assert report['remaining_process_count'] == 1
    rows = iter([[row], []])
    monkeypatch.setattr(user_relay, '_app_stop_process_snapshot', lambda **k: next(rows))
    report = user_relay.request_app_stop(tmp_path / 'relay', app_root=root, webview_root=udf, wait=lambda _: None)
    assert report['status'] == 'STOPPED'
    assert report['remaining_process_count'] == 0


def test_observe_rechecks_missing_image_and_skips_only_proven_foreign(tmp_path):
    """Run only the observer function, with synthetic CIM rows and no native queries."""
    import subprocess
    source = (Path(__file__).parents[1] / 'tools' / 'apply_prepared_update.ps1').read_text(encoding='utf-8-sig')
    initializer = source[source.index('function Initialize-LabelStartupUpdateProcessToken {'):source.index('function Invoke-LabelStartupReadOnlyQuery {')]
    function = source[source.index('function Get-StartupUpdateProcesses('):source.index('function Assert-StartupUpdateVersion(')]
    script = tmp_path / 'observe.ps1'
    prelude = """
Add-Type 'public static class KMTechStartupUpdateToken { public static bool Ordinary(uint pid) { return true; } }'
function Start-Sleep { param($Milliseconds) }
function Invoke-CimMethod { param($InputObject,$MethodName,$ErrorAction) return @{ReturnValue=0;Sid='foreign'} }
$script:reads=0
function Get-CimInstance {
 param($ClassName,$Filter,$ErrorAction)
 $script:reads++
 if ($script:reads -eq 1) { return [pscustomobject]@{Name='pythonw.exe';ExecutablePath='';ProcessId=10;CreationDate=[datetime]'2026-01-01';SessionId=2} }
 return @()
}
"""
    script.write_text(prelude + initializer + function + "\n$r=Get-StartupUpdateProcesses 'E:\\synthetic\\current' 'owner'; if($r.Count -ne 0 -or $script:reads -lt 2){exit 1}\n"
                      + "$script:reads=0\nfunction Get-CimInstance { param($ClassName,$Filter,$ErrorAction) $script:reads++; return [pscustomobject]@{Name='pythonw.exe';ExecutablePath='';ProcessId=10;CreationDate=[datetime]'2026-01-01';SessionId=2} }\n"
                      + "$r=Get-StartupUpdateProcesses 'E:\\synthetic\\current' 'owner'; if($r.Count -ne 0){exit 2}\n"
                      + "$r=Get-StartupUpdateProcesses 'E:\\synthetic\\current' 'foreign'; if($r.Count -ne 1 -or $r[0].observable){exit 3}\n",
                      encoding='utf-8-sig')
    result = subprocess.run([auto_update.system_executable('WindowsPowerShell/v1.0/powershell.exe'),
                             '-NoProfile', '-NonInteractive', '-WindowStyle', 'Hidden', '-File', str(script)],
                            capture_output=True, creationflags=subprocess.CREATE_NO_WINDOW)
    assert result.returncode == 0


@pytest.mark.parametrize('blocked', ['ui', 'notice'])
def test_blocked_cleanup_exits_process_and_preserves_durable_state(tmp_path, blocked):
    import os
    import subprocess
    import sys
    import time
    source = '''
import json,sys,time
from pathlib import Path
from unittest.mock import Mock
import Label_Match as lm
from package_logistics import PackageOutbox
from tests.test_package_logistics import _draft_for_set
root=Path(sys.argv[1])
manager=lm.DataManager(str(root), 'stop-probe', 'operator', 'probe')
assert manager.save_current_state({'current_set_info':{'id':'unfinished','raw':['MASTER'],'parsed':[]}})
manager.log_event(lm.Label_Match.Events.APP_START, {'message':'stop probe'})
manager.flush(timeout=2)
outbox=PackageOutbox(root/'pending.sqlite3')
outbox.enqueue(_draft_for_set('STOP-PENDING'))
app=lm.Label_Match.__new__(lm.Label_Match)
app.save_directory=str(root)
app._app_close_in_progress=True
app._tracked_tk_shutdown_threads=lambda: []
app.data_manager=manager
def blocked_update():
 time.sleep(3)
 raise AssertionError('blocked callback returned')
app.update=blocked_update
app._show_app_close_cleanup_delay=Mock()
lm.LABEL_MATCH_APP_CLOSE_PROCESS_TIMEOUT_SECONDS=0.1
lm.LABEL_MATCH_APP_CLOSE_EXIT_GRACE_SECONDS=0.5
if sys.argv[2]=='notice':
 original_notice=lm._try_update_shutdown_stall_notice
 def blocked_notice(*a,**k):
  original_notice(*a,**k)
  time.sleep(3)
 lm._try_update_shutdown_stall_notice=blocked_notice
(root/'began.json').write_text(json.dumps({'started':time.time()}))
app._finish_after_mainloop()
'''
    env = dict(os.environ)
    env.update(KMTECH_LABEL_WRITER_TEST_MODE='1', KMTECH_LABEL_WRITER_CONTROL_ROOT=str(tmp_path / 'control'))
    child = subprocess.Popen([sys.executable, '-B', '-c', source, str(tmp_path), blocked], env=env,
                             stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                             creationflags=subprocess.CREATE_NO_WINDOW)
    output, errors = child.communicate(timeout=10)
    import re
    assert (tmp_path / 'began.json').is_file(), re.findall(r'\b\w+(?:Error|Exception)\b|\w+\.py", line \d+', errors.decode('utf-8', errors='replace'))
    assert child.returncode == 1
    started = json.loads((tmp_path / 'began.json').read_text())['started']
    assert time.time() - started < 2
    assert (tmp_path / 'label_match_shutdown_stall.json').is_file()
    from label_data_manager import read_recovery_file
    assert read_recovery_file(tmp_path / lm.Label_Match.FILES.CURRENT_STATE, current_state=True)['value']['current_set_info']['raw'] == ['MASTER']
    from package_logistics import PackageOutbox
    assert PackageOutbox(tmp_path / 'pending.sqlite3').get_by_set_id('STOP-PENDING') is not None


def test_requesting_cli_does_not_count_itself_as_remaining(tmp_path):
    root, udf = tmp_path / 'app', tmp_path / 'webview2'
    requester = process(10, 1, root / 'runtime' / 'python.exe')
    assert user_relay._owned_app_stop_processes([requester], root, udf, {}, own_pid=10) == []
