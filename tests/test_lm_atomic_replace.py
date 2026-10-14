"""Bounded atomic publication retries without changing readers or admissions."""
import ast
import ctypes
from ctypes import wintypes
import hashlib
import json
from pathlib import Path
import subprocess
import shutil
import time

import pytest

import auto_update
from tools.refresh_atomic_replace_declarations import CONSUMERS

ROOT = Path(__file__).resolve().parents[1]


def held_reader(path):
    kernel = ctypes.WinDLL('kernel32', use_last_error=True)
    kernel.CreateFileW.argtypes = [wintypes.LPCWSTR, wintypes.DWORD, wintypes.DWORD,
                                  wintypes.LPVOID, wintypes.DWORD, wintypes.DWORD, wintypes.HANDLE]
    kernel.CreateFileW.restype = wintypes.HANDLE
    kernel.CloseHandle.argtypes = [wintypes.HANDLE]
    kernel.CloseHandle.restype = wintypes.BOOL
    handle = kernel.CreateFileW(str(path), 0x80000000, 1, None, 3, 0x80, None)
    assert handle != ctypes.c_void_p(-1).value
    return kernel, handle


def test_update_control_read_overlap_retries_without_failure(tmp_path, monkeypatch):
    target = tmp_path / 'active.json'
    target.write_text(json.dumps({'sha': 'b' * 40}), encoding='utf-8')
    kernel, handle = held_reader(target)
    released = []
    original_sleep = time.sleep
    def release_reader(delay):
        assert 0 < delay <= 0.05
        if not released:
            assert kernel.CloseHandle(handle)
            released.append(True)
        original_sleep(delay)
    monkeypatch.setattr(time, 'sleep', release_reader)
    try:
        auto_update.write_json_durable(target, {'schema': auto_update.SCHEMA, 'sha': 'a' * 40})
        assert released == [True]
        assert json.loads(target.read_bytes())['sha'] == 'a' * 40
    finally:
        if not released:
            assert kernel.CloseHandle(handle)


@pytest.mark.parametrize('publisher', ['download', 'onboarding', 'relay'])
def test_l2_and_onboarding_relay_read_overlap(tmp_path, monkeypatch, publisher):
    import current_user_onboarding
    import user_relay
    target = tmp_path / ('manifest.json' if publisher == 'download' else 'status.json')
    target.write_bytes(b'{"before":true}')
    kernel, handle = held_reader(target)
    released = []
    def release_reader(delay):
        assert 0 < delay <= 0.05
        if not released:
            assert kernel.CloseHandle(handle)
            released.append(True)
    monkeypatch.setattr(time, 'sleep', release_reader)
    try:
        if publisher == 'download':
            temps = {}
            for name in ('manifest.json', 'manifest.sig', 'packet.zip'):
                temps[name] = tmp_path / (name + '.tmp')
                temps[name].write_bytes(b'published')
            auto_update.publish_download(tmp_path, temps)
            assert all((tmp_path / name).read_bytes() == b'published' for name in temps)
        else:
            owner = current_user_onboarding if publisher == 'onboarding' else user_relay
            owner._write_json_atomic(target, {'after': True})
            assert json.loads(target.read_bytes()) == {'after': True}
        assert released == [True]
    finally:
        if not released:
            assert kernel.CloseHandle(handle)


@pytest.mark.parametrize('code', [5, 32])
def test_persistent_refusal_keeps_first_native_exception_and_bound(tmp_path, monkeypatch, code):
    import label_atomic_replace as atomic
    source, target = tmp_path / 'temporary', tmp_path / 'target'
    source.write_bytes(b'new'); target.write_bytes(b'old')
    errors = []
    clock = [0.0]
    def once(*_args, **_kwargs):
        error = ctypes.WinError(code)
        errors.append(error)
        raise error
    monkeypatch.setattr(atomic, '_replace_once', once)
    monkeypatch.setattr(atomic.time, 'monotonic', lambda: clock[0])
    monkeypatch.setattr(atomic.time, 'sleep', lambda delay: clock.__setitem__(0, clock[0] + delay))
    with pytest.raises(OSError) as caught:
        atomic.atomic_replace(source, target)
    assert caught.value is errors[0] and caught.value.winerror == code
    assert 1.0 <= clock[0] <= 1.05 and 2 <= len(errors) <= 22
    assert source.read_bytes() == b'new' and target.read_bytes() == b'old'


@pytest.mark.parametrize('error', [ctypes.WinError(2), ctypes.WinError(87), OSError(5, 'non-Windows errno')])
def test_permanent_refusal_has_no_retry_or_exception_conversion(tmp_path, monkeypatch, error):
    import label_atomic_replace as atomic
    calls = []
    def once(*_args, **_kwargs):
        calls.append(True)
        raise error
    monkeypatch.setattr(atomic, '_replace_once', once)
    monkeypatch.setattr(atomic.time, 'sleep', lambda _delay: pytest.fail('permanent refusal was retried'))
    with pytest.raises(OSError) as caught:
        atomic.atomic_replace(tmp_path / 'source', tmp_path / 'target')
    assert caught.value is error and calls == [True]


def test_app_atomic_calls_are_centralized_and_shared_copies_byte_pinned():
    from tools.refresh_atomic_replace_declarations import helper_body, validate
    validate(ROOT)
    guard = json.loads((ROOT / 'tests/fixtures/lm_atomic_exclusions.json').read_bytes())
    excluded = {entry['path']: entry['sha256'] for entry in guard['files']}
    for name, sha in excluded.items():
        assert hashlib.sha256((ROOT / name).read_bytes()).hexdigest() == sha, name
    assert 'shell/shell_fileio.py' in excluded
    named_tests = {tuple(entry) for entry in guard['test_doubles']}
    seen_tests = set()
    files = subprocess.check_output(['git', 'ls-files', '*.py', '*.ps1'], cwd=ROOT, text=True).splitlines()
    files.append('label_atomic_replace.py')
    forbidden = []
    for name in set(files):
        if name in excluded or name == 'label_atomic_replace.py':
            continue
        source = (ROOT / name).read_text(encoding='utf-8-sig')
        if name.endswith('.py'):
            class Calls(ast.NodeVisitor):
                def __init__(self): self.scope = []
                def visit_FunctionDef(self, node):
                    self.scope.append(node.name); self.generic_visit(node); self.scope.pop()
                def visit_ClassDef(self, node):
                    self.scope.append(node.name); self.generic_visit(node); self.scope.pop()
                def visit_Attribute(self, node):
                    if (node.attr in ('MoveFileExW', 'ReplaceFileW')
                            or (node.attr == 'replace' and isinstance(node.value, ast.Name) and node.value.id == 'os')):
                        site = (name, '.'.join(self.scope))
                        if site in named_tests:
                            seen_tests.add(site)
                        else:
                            forbidden.append((name, node.lineno))
                    self.generic_visit(node)
            Calls().visit(ast.parse(source))
        elif not name.startswith('tests/'):
            import re
            if name in {'INSTALL_THIS_PC.ps1', 'tools/label_writer_fence.ps1', 'tools/label_worker_shortcuts.ps1', "INSTALL_CANONICAL_PORTABLE.ps1"}:
                # Copies are generated from the one canonical function and
                # validated above; pinned function strings are not direct calls.
                source = source.replace(helper_body(ROOT), '')
                source = source.replace(helper_body(ROOT).replace("'", "''"), '')
            if re.search(r'::(?:MoveFileExW|ReplaceFileW)\s*\(|\[IO.File\]::(?:Replace|Move)\s*\(', source, re.I):
                forbidden.append((name, 'native call'))
    assert seen_tests == named_tests
    assert forbidden == []


@pytest.mark.parametrize("consumer", CONSUMERS)
def test_atomic_powershell_body_pin_rejects_caller_drift(tmp_path, consumer):
    from tools.refresh_atomic_replace_declarations import validate
    for name in ("INSTALL_THIS_PC.ps1", *CONSUMERS):
        path = tmp_path / name
        path.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(ROOT / name, path)
    validate(tmp_path)
    path = tmp_path / consumer
    source = path.read_text(encoding='utf-8-sig')
    import re
    source = re.sub(r"\$expectedAtomicBodySha256 = '[0-9a-f]{64}'", "$expectedAtomicBodySha256 = '" + '0' * 64 + "'", source)
    path.write_text(source, encoding='utf-8-sig')
    with pytest.raises(ValueError):
        validate(tmp_path)


@pytest.mark.parametrize('engine', ['powershell.exe', 'pwsh.exe'])
def test_powershell_atomic_overlap_bound_and_pinned_callers(tmp_path, engine):
    result = subprocess.run([shutil.which(engine), '-NoLogo', '-NoProfile', '-NonInteractive', '-WindowStyle', 'Hidden',
        '-File', str(ROOT / 'tests/lm_atomic_replace.ps1'), '-HostRoot', str(ROOT), '-FixtureRoot', str(tmp_path)],
        capture_output=True, creationflags=subprocess.CREATE_NO_WINDOW)
    (tmp_path / 'atomic.stdout').write_bytes(result.stdout)
    (tmp_path / 'atomic.stderr').write_bytes(result.stderr)
    assert result.returncode == 0
    assert result.stdout.strip() == b'PASS'
    assert not result.stderr
