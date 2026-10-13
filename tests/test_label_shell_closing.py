"""Headless close contracts and installed-scanner candidate compatibility."""
import hashlib
import json
from pathlib import Path
import shutil
import sys
import threading
import types

import pytest

from writer_sink_inventory import derive_writer_sink_inventory, WriterSinkInventoryError
from tools.refresh_first_update_declarations import validate_shell_ready_pin

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "shell"))
import host
from bridge import Bridge


@pytest.fixture
def shell_candidate(tmp_path):
    shutil.copytree(ROOT / "shell", tmp_path / "shell")
    shutil.copyfile(ROOT / "label_match_screen.py", tmp_path / "label_match_screen.py")
    return tmp_path


@pytest.mark.parametrize("ready_name", ["SHELL-READY.json", "SHELL-READY-vNext.json"])
def test_installed_scanner_accepts_another_self_consistent_shell(shell_candidate, ready_name):
    ready_path = shell_candidate / "shell/SHELL-READY.json"
    ready = json.loads(ready_path.read_bytes())
    ready["version"] = "vNext"
    path = shell_candidate / "shell/bridge.py"
    path.write_bytes(path.read_bytes() + b"\n# candidate revision\n")
    for entry in ready["files"]:
        if entry["path"] == "shell/bridge.py":
            entry["sha256"] = hashlib.sha256(path.read_bytes()).hexdigest()
            entry["bytes"] = path.stat().st_size
    ready_path.unlink()
    (ready_path.parent / ready_name).write_text(json.dumps(ready), encoding="utf-8")
    assert any(row.source == "shell_fileio" for row in derive_writer_sink_inventory(shell_candidate))


@pytest.mark.parametrize("drift", ["byte", "python", "installed_guard", "admission", "ready"])
def test_installed_scanner_rejects_inconsistent_or_unfenced_shell(shell_candidate, drift):
    if drift == "byte":
        path = shell_candidate / "shell/bridge.py"
        raw = path.read_bytes()
        path.write_bytes(bytes([raw[0] ^ 1]) + raw[1:])
    elif drift == "python":
        (shell_candidate / "shell/extra.py").write_bytes(b"pass\n")
    elif drift == "ready":
        (shell_candidate / "shell/SHELL-READY.json").unlink()
    else:
        path = shell_candidate / "label_match_screen.py"
        source = path.read_text(encoding="utf-8")
        if drift == "installed_guard":
            source = source.replace("write_guard=shell_write_guard", "write_guard=None")
        else:
            source = source.replace('writer_admission("shell_fileio")', 'writer_admission("other")')
        path.write_text(source, encoding="utf-8")
    with pytest.raises(WriterSinkInventoryError):
        derive_writer_sink_inventory(shell_candidate)


def test_declaration_generator_keeps_the_reviewed_ready_pin(shell_candidate):
    validate_shell_ready_pin(ROOT)
    path = shell_candidate / "shell/SHELL-READY.json"
    ready = json.loads(path.read_bytes())
    ready["version"] = "vNext"
    path.write_text(json.dumps(ready), encoding="utf-8")
    assert derive_writer_sink_inventory(shell_candidate)
    with pytest.raises(ValueError, match="pinned SHELL-READY"):
        validate_shell_ready_pin(shell_candidate)


@pytest.mark.parametrize("answer", [True, False])
def test_window_x_waits_for_one_late_owner_answer(tmp_path, answer):
    release, called, changed = threading.Event(), threading.Event(), threading.Event()

    class Processor:
        def __init__(self):
            self.questions = 0

        def state(self):
            return {}

        def closing(self):
            self.questions += 1
            called.set()
            assert release.wait(5)
            return answer

    processor = Processor()
    bridge = Bridge(tmp_path, "rw", processor_factory=lambda: processor)
    modes = []

    def show(mode):
        modes.append(mode)
        if mode is None:
            changed.set()

    bridge.set_exit_handler(changed.set)
    waiter = None
    try:
        waiter = host.ClosingWait(bridge, show, timeout=5)
        assert waiter.closing() is False
        assert called.is_set()
        assert modes == ["waiting"]
        assert waiter.closing() is False
        assert processor.questions == 1
        assert not bridge._exit_requested
        release.set()
        assert changed.wait(5)
        assert bridge._exit_requested is answer
        assert modes == (["waiting"] if answer else ["waiting", None])
        assert not waiter.forced
    finally:
        release.set()
        if waiter is not None:
            waiter.close()
        bridge.close()


def test_close_strip_warns_and_returns_focus_after_cancel(monkeypatch):
    focused, disposed = [], []
    panel = types.SimpleNamespace(ContainsFocus=True, Dispose=lambda: disposed.append(True))
    label = types.SimpleNamespace()
    close_button = types.SimpleNamespace()
    wait_button = types.SimpleNamespace(Focus=lambda: focused.append("wait"))
    controls = [panel]
    form = types.SimpleNamespace(
        IsDisposed=False, Controls=types.SimpleNamespace(Remove=controls.remove),
        BeginInvoke=lambda action: action(),
        browser=types.SimpleNamespace(webview=types.SimpleNamespace(Focus=lambda: focused.append("page"))),
    )
    monkeypatch.setitem(sys.modules, "System", types.SimpleNamespace(Action=lambda callback: callback))
    native = host.NativeHost.__new__(host.NativeHost)
    native.window = types.SimpleNamespace(native=form)
    native._closed = False
    native._closing_banner = panel, label, close_button, wait_button
    native.bridge = types.SimpleNamespace(events=types.SimpleNamespace(record=lambda *a, **k: pytest.fail("display failed")))
    native.show_closing("waiting", lambda action: None)
    assert label.Text == "끝내는 중 — 남은 일을 마치고 닫습니다"
    assert not close_button.Visible and not wait_button.Visible
    native.show_closing("choice", lambda action: None)
    assert label.Text == "응답이 없습니다 —\n그래도 닫으면 저장 중인 것은 잃을 수 있습니다"
    assert close_button.Visible and wait_button.Visible
    assert focused == ["wait"]
    native.show_closing(None, lambda action: None)
    assert controls == [] and disposed == [True]
    assert native._closing_banner is None and focused == ["wait", "page"]
