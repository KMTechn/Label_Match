import ast
from contextlib import nullcontext
import inspect
import json
from pathlib import Path
import shutil
import subprocess

import pytest
import user_relay as relay


@pytest.mark.parametrize('engine', ['powershell.exe', 'pwsh.exe'])
def test_product_app_stop_observer_parses_without_errors(tmp_path, engine):
    body = ast.parse(inspect.getsource(relay._app_stop_process_snapshot))
    script = next(node.value.value for node in ast.walk(body)
                  if isinstance(node, ast.Assign) and any(isinstance(t, ast.Name) and t.id == 'script' for t in node.targets))
    source = tmp_path / 'product-observer.ps1'
    source.write_text(script, encoding='utf-8')
    parser = tmp_path / 'parse.ps1'
    parser.write_text("param($Source)\n$t=$null;$e=$null;[void][Management.Automation.Language.Parser]::ParseFile($Source,[ref]$t,[ref]$e);if($e.Count){exit 1};exit 0\n")
    result = subprocess.run([shutil.which(engine), '-NoLogo', '-NoProfile', '-NonInteractive',
                             '-WindowStyle', 'Hidden', '-File', str(parser), str(source)],
                            capture_output=True, creationflags=subprocess.CREATE_NO_WINDOW)
    assert result.returncode == 0


@pytest.mark.parametrize('entry', ['current', 'version', 'portable'])
def test_stop_owns_only_same_install_current_and_valid_version_runtimes(tmp_path, entry):
    managed = tmp_path / 'managed'
    version = managed / 'versions' / ('a' * 40)
    app_root = {'current': managed / 'current', 'version': version,
                'portable': tmp_path / 'portable'}[entry]
    images = [app_root / 'runtime' / 'python.exe',
              managed / 'current' / 'runtime' / 'pythonw.exe',
              version / 'runtime' / 'python.exe',
              managed / 'versions' / ('b' * 40) / 'runtime' / 'python.exe',
              managed / 'versions' / ('g' * 40) / 'runtime' / 'python.exe',
              managed / 'versions' / ('a' * 39) / 'runtime' / 'python.exe',
              managed / 'versions' / ('c' * 40) / 'runtime' / 'nested' / 'python.exe',
              tmp_path / 'foreign' / 'versions' / ('a' * 40) / 'runtime' / 'python.exe',
              tmp_path / 'managed-sibling' / 'current' / 'runtime' / 'python.exe']
    rows = [dict(pid=100 + index, parent_pid=0, name=image.name, image=str(image),
                 command='', created_at='stable-birth', session_id=1)
            for index, image in enumerate(images)]
    known = {}
    selected = relay._owned_app_stop_processes(rows, app_root, tmp_path / 'webview', known, own_pid=999)
    assert {row['pid'] for row in selected} == ({100} if entry == 'portable' else {100, 101, 102, 103})


def test_actual_cim_observation_allows_cli_stopped_rc_zero(tmp_path, monkeypatch, capsys):
    from auto_update import process_security
    import label_match_screen
    app = tmp_path / 'managed' / 'versions' / ('a' * 40)
    app.mkdir(parents=True)
    control = app.parent.parent / 'update-control'
    control.mkdir()
    identity = process_security(__import__('os').getpid())
    (control / 'policy.json').write_text(json.dumps(dict(operator_sid=identity['sid'])))
    monkeypatch.setattr(label_match_screen, 'shell_data_dir', lambda **_k: tmp_path / 'screen')
    monkeypatch.setattr(relay, 'writer_admission', lambda *_a, **_k: nullcontext())
    monkeypatch.setattr(relay, 'request_user_relay_stop', relay.request_user_relay_stop.__wrapped__)
    monkeypatch.setattr(relay, 'APP_STOP_TIMEOUT_SECONDS', 15.0)
    actual = relay._app_stop_process_snapshot
    observations = []
    def observe(**kwargs):
        rows = actual(**kwargs)
        observations.append(len(rows))
        return rows
    monkeypatch.setattr(relay, '_app_stop_process_snapshot', observe)
    rc = relay.main(['--request-app-stop', '--app-root', str(app),
                     '--direct-sync-root', str(tmp_path / 'relay')])
    report = json.loads(capsys.readouterr().out)
    assert rc == 0 and report['status'] == 'STOPPED'
    assert report['remaining_process_count'] == 0 and report['process_observation_error'] is None
    assert len(observations) >= 2


def test_active_version_stop_reads_install_policy_for_proven_foreign_image(tmp_path, monkeypatch):
    app = tmp_path / 'managed' / 'versions' / ('a' * 40)
    app.mkdir(parents=True)
    control = app.parent.parent / 'update-control'
    control.mkdir()
    (control / 'policy.json').write_text(json.dumps(dict(operator_sid='fixture-worker')))
    foreign = dict(pid=100, parent_pid=0, name='python.exe', image='', command='',
                   created_at='stable-birth', session_id=1, sid='fixture-foreign')
    monkeypatch.setattr(relay, '_app_stop_process_snapshot', lambda **_k: [foreign])
    monkeypatch.setattr(relay, 'writer_admission', lambda *_a, **_k: nullcontext())
    monkeypatch.setattr(relay, 'request_user_relay_stop', relay.request_user_relay_stop.__wrapped__)
    report = relay.request_app_stop(tmp_path / 'relay', app_root=app,
        webview_root=tmp_path / 'webview', timeout_seconds=1.0, wait=lambda _s: None)
    assert report['status'] == 'STOPPED' and report['remaining_process_count'] == 0
