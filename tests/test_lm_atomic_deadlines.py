"""Deadline regressions at the Python and actual PowerShell helper boundaries."""
import ctypes
import json
from pathlib import Path
import shutil
import subprocess
from types import SimpleNamespace

import pytest

import label_atomic_replace as atomic
import user_relay

ROOT = Path(__file__).resolve().parents[1]


@pytest.mark.parametrize('code', [5, 32])
def test_oversleep_never_calls_native_replace_after_the_first_refusal(tmp_path, monkeypatch, code):
    clock, calls = [0.0], []
    original = ctypes.WinError(code)
    def once(*args, **kwargs):
        calls.append((args, kwargs))
        if len(calls) == 1:
            raise original
        pytest.fail('native retry after deadline')
    monkeypatch.setattr(atomic, '_replace_once', once)
    monkeypatch.setattr(atomic.time, 'monotonic', lambda: clock[0])
    monkeypatch.setattr(atomic.time, 'sleep', lambda _delay: clock.__setitem__(0, 1.1))
    with pytest.raises(OSError) as caught:
        atomic.atomic_replace(tmp_path / 'source', tmp_path / 'target')
    assert caught.value is original and len(calls) == 1


def test_relay_stop_publication_and_wait_share_the_original_deadline(tmp_path, monkeypatch):
    clock, deadlines, waits = [0.0], [], []
    monkeypatch.setattr(user_relay.time, 'monotonic', lambda: clock[0])
    publish = user_relay._write_machine_json_atomic
    def slow_publish(path, payload, *, deadline):
        deadlines.append(deadline)
        clock[0] = 0.035
        return publish(path, payload, deadline=deadline)
    def wait(seconds):
        waits.append(seconds)
        clock[0] += seconds
    monkeypatch.setattr(user_relay, '_write_machine_json_atomic', slow_publish)
    report = user_relay.request_user_relay_stop(
        tmp_path, timeout_seconds=0.04, wait=wait,
        marker_lease_factory=lambda _path: SimpleNamespace(close=lambda: None),
        lease_factory=lambda _path: None)
    assert report['status'] == 'UNKNOWN'
    assert deadlines == [0.04] and clock[0] == pytest.approx(0.04)
    assert sum(waits) == pytest.approx(0.005)


def test_app_stop_passes_one_deadline_to_both_publications(tmp_path, monkeypatch):
    clock, deadlines = [0.0], []
    monkeypatch.setattr(user_relay.time, 'monotonic', lambda: clock[0])
    monkeypatch.setattr(user_relay, '_app_stop_process_snapshot', lambda **kwargs: [])
    publish = user_relay._write_machine_json_atomic
    def slow_publish(path, payload, *, deadline):
        deadlines.append(deadline)
        clock[0] += 0.015
        return publish(path, payload, deadline=deadline)
    monkeypatch.setattr(user_relay, '_write_machine_json_atomic', slow_publish)
    monkeypatch.setattr(user_relay, '_acquire_relay_lease', lambda _path: None)
    real_stop = user_relay.request_user_relay_stop
    def stop(root, **kwargs):
        return real_stop(root, **kwargs,
            marker_lease_factory=lambda _path: SimpleNamespace(close=lambda: None),
            lease_factory=lambda _path: None)
    monkeypatch.setattr(user_relay, 'request_user_relay_stop', stop)
    report = user_relay.request_app_stop(
        tmp_path, app_root=tmp_path / 'app', webview_root=tmp_path / 'webview2',
        timeout_seconds=0.04, wait=lambda seconds: clock.__setitem__(0, clock[0] + seconds))
    assert report['status'] == 'UNKNOWN'
    assert deadlines == [0.04, 0.04] and clock[0] == pytest.approx(0.04)


@pytest.mark.parametrize('engine', ['powershell', 'pwsh'])
def test_actual_powershell_retry_deadlines_and_access_classification(tmp_path, engine):
    child = subprocess.run([shutil.which(engine), '-NoProfile', '-NonInteractive',
        '-ExecutionPolicy', 'Bypass', '-File', str(ROOT / 'tests/lm_atomic_deadlines.ps1'),
        '-HostRoot', str(ROOT)], capture_output=True, creationflags=subprocess.CREATE_NO_WINDOW)
    assert child.returncode == 0
    assert child.stdout.strip() == b'PASS' and not child.stderr


@pytest.mark.parametrize('source', [
    'Move-Item -LiteralPath $source -Destination $target',
    'Microsoft.PowerShell.Management\\Move-Item -LiteralPath $source -Destination $target',
    'Rename-Item -LiteralPath $source -NewName $target',
    '[System.IO.File]::Move($source,$target)',
    '[IO.File]::Replace(\n $source,\n $target,\n $null)',
    '[Native]::MoveFileExW($source,$target,9)',
])
def test_powershell_ast_guard_rejects_publication_spelling_variants(tmp_path, source):
    candidate = tmp_path / 'candidate.ps1'
    candidate.write_text(source, encoding='utf-8')
    child = subprocess.run([shutil.which('pwsh'), '-NoProfile', '-NonInteractive', '-File',
        str(ROOT / 'tests/lm_atomic_guard.ps1'), '-SourceFile', str(candidate)],
        capture_output=True, creationflags=subprocess.CREATE_NO_WINDOW)
    assert child.returncode == 3
    assert json.loads(child.stdout)['sites'] == 1
