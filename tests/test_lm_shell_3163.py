"""Accepted shell bytes and its real startup watchdog, with native UI providers replaced."""
from contextlib import nullcontext
import json
from pathlib import Path
import sys
import threading
from types import SimpleNamespace

import pytest

import label_match_screen as screen
from writer_sink_inventory import _shell_file_relative

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / 'shell'))
import host


class Event:
    def __iadd__(self, callback):
        return self


@pytest.mark.parametrize('profile', ['absent', 'empty', 'warm'])
def test_actual_copied_shell_arms_90_or_30_seconds_from_ebwebview(tmp_path, monkeypatch, profile):
    udf = tmp_path / 'webview2'
    if profile != 'absent':
        udf.mkdir()
    if profile == 'warm':
        (udf / 'EBWebView').mkdir()
    observed, window = [], []
    armed = threading.Event()
    real_wait = threading.Event.wait
    def wait(event, timeout=None):
        if threading.current_thread().name == 'shell-start-watch':
            observed.append(timeout)
            armed.set()
        return real_wait(event, timeout)
    monkeypatch.setattr(threading.Event, 'wait', wait)
    def create_window(*args, **kwargs):
        value = SimpleNamespace(_callbacks={}, js_api=kwargs['js_api'], destroy=lambda: None,
            events=SimpleNamespace(**{name: Event() for name in
                ['before_load', 'loaded', 'shown', 'closing', 'closed', 'before_show']}))
        window.append(value)
        return value
    def start(callback, **kwargs):
        assert Path(kwargs['storage_path']) == udf.resolve()
        assert armed.wait(2)
        callback()
        window[0].js_api._on_ready({})
    monkeypatch.setitem(sys.modules, 'webview', SimpleNamespace(settings={}, create_window=create_window, start=start))
    native = SimpleNamespace(**{name: (lambda *args: None) for name in
        ['before_load','loaded','notify_push','pause_page','start','page_ready','close','pick_file','show_closing']})
    monkeypatch.setattr(host, 'NativeHost', lambda *args, **kwargs: native)
    monkeypatch.setattr(host, 'browser_preset', lambda *args: None)
    monkeypatch.setattr(host, 'is_interactive_session', lambda: True)
    assert host.run_app('rw', tmp_path, udf=udf, write_guard=lambda *args: nullcontext()) == 'web'
    assert observed == [30 if profile == 'warm' else 90]


@pytest.mark.parametrize('failure', [False, True])
def test_adapter_passes_exact_udf_and_records_only_types_and_elapsed_ms(tmp_path, monkeypatch, failure):
    fileio = __import__('shell_fileio')
    monkeypatch.setattr(screen, 'shell_write_guard', lambda *args: nullcontext())
    original_error = OSError('unrecorded detail')
    host_error = ValueError('unrecorded host detail')
    def start(*args, **kwargs):
        if failure:
            raise original_error
        return 'web'
    view = SimpleNamespace(start=start, windows=[], create_window=lambda *args, **kwargs: None)
    drains, options = [], []
    def run_app(profile, data, **kwargs):
        options.append(kwargs)
        try:
            return view.start()
        finally:
            kwargs['before_exit'](1 if failure else 0, 0.2)
    monkeypatch.setattr(screen, '_prepare_screen', lambda version: (run_app, view, tmp_path))
    monkeypatch.setattr(screen, 'close_and_wait_writers', lambda timeout: drains.append(timeout))
    module = SimpleNamespace(APP_VERSION='v2.0.105', LABEL_MATCH_SCREEN_HOST={'error': host_error})
    if failure:
        with pytest.raises(screen.ScreenUnavailable):
            screen.screen_runner(module)()
    else:
        assert screen.screen_runner(module)() == 0
    assert options[0]['udf'] == tmp_path / 'webview2'
    assert view.start is start and drains == [0.2]
    records = json.loads((tmp_path / 'shell-start-observation.json').read_bytes())
    assert [r['kind'] for r in records] == ['start_entered', 'start_exception' if failure else 'start_returned', 'before_exit']
    assert all(set(r) == {'kind','elapsed_ms','error_type','host_error_type'} for r in records)
    assert all(type(r['elapsed_ms']) is int and r['elapsed_ms'] >= 0 for r in records)
    assert all(r['host_error_type'] == 'ValueError' for r in records)
    assert records[-1]['error_type'] == ('OSError' if failure else None)
    assert b'unrecorded' not in (tmp_path / 'shell-start-observation.json').read_bytes()


@pytest.mark.parametrize('path', ['../host.py','shell-v0/host.py','shell-v3.16.3/../host.py','C:/shell/host.py'])
def test_ready_kit_prefix_mapping_keeps_the_installed_shell_boundary(path):
    with pytest.raises(ValueError):
        _shell_file_relative({'version':'v3.16.3'}, path)


def test_duplicate_kit_and_installed_paths_remain_ambiguous(tmp_path):
    import shutil
    from writer_sink_inventory import derive_writer_sink_inventory, WriterSinkInventoryError
    shutil.copytree(ROOT / 'shell', tmp_path / 'shell')
    shutil.copyfile(ROOT / 'label_match_screen.py', tmp_path / 'label_match_screen.py')
    manifest = tmp_path / 'shell/SHELL-READY.json'
    ready = json.loads(manifest.read_bytes())
    row = dict(ready['files'][0], path=_shell_file_relative(ready, ready['files'][0]['path']))
    ready['files'].append(row)
    manifest.write_text(json.dumps(ready), encoding='utf-8')
    with pytest.raises(WriterSinkInventoryError):
        derive_writer_sink_inventory(tmp_path)
