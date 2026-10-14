"""Headless regressions for recovery inside the existing application lease."""
import ast
import builtins
from contextlib import nullcontext
from pathlib import Path
import sys
import types
import queue
import threading

import pytest
from tests.test_label_screen_default import entrypoint


@pytest.fixture
def application(monkeypatch, tmp_path):
    main, _, host = entrypoint(monkeypatch, tmp_path)
    source = Path.cwd() / "Label_Match.py"
    tree = ast.parse(source.read_text(encoding="utf-8-sig"))
    body = next(n for n in tree.body if isinstance(n, ast.FunctionDef)
                and n.name == "_run_label_match_application")
    module = types.ModuleType("screen_recovery_application")
    module.__file__ = str(source)
    calls = []
    class App:
        def __init__(self, **kwargs):
            calls.append("processing")
            calls.append("visible-tk")
        def title(self): return ""
        def state(self): return "normal"
        def mainloop(self): calls.append("tk-loop")
        def _finish_after_mainloop(self): calls.append("finish")
    module.__dict__.update(
        sys=sys, Path=Path, Label_Match=App,
        LABEL_MATCH_SCREEN_HOST={"requested": True, "active": False,
                                 "run": None, "catalog_unconfirmed": False},
        _startup_app_stop_requested=lambda: False,
        _prepare_startup_catalog_attempt=lambda: (None, False),
        _schedule_reused_relay_verification=lambda *a: calls.append("verify"),
        _label_match_startup_trace=lambda stage, **kw: calls.append(stage),
    )
    exec(compile(ast.Module(body=[body], type_ignores=[]), str(source), "exec"), module.__dict__)
    monkeypatch.setitem(sys.modules, module.__name__, module)
    import current_user_onboarding as onboarding
    monkeypatch.setattr(onboarding, "admit_window_first_start", lambda report: calls.append("hold"))
    import auto_update as update
    monkeypatch.setattr(update, "trial_context", lambda root: None)
    for name in ("publish_gui_ready", "trial_stop_requested", "confirmed_trial"):
        monkeypatch.setattr(update, name, lambda *a: False)
    observation = types.ModuleType("startup_observation")
    observation.observe_startup = lambda: None
    monkeypatch.setitem(sys.modules, "startup_observation", observation)
    module.LABEL_MATCH_SCREEN_HOST = host
    host["requested"] = True
    main.__globals__["_run_label_match_application"] = module._run_label_match_application
    def guarded(*a, **kw):
        calls.append("lease-enter")
        try:
            return module._run_label_match_application(relay_reuse_report={"start_path": "FAST_WINDOW_FIRST"})
        finally:
            calls.append("lease-exit")
    main.__globals__["run_guarded_entrypoint"] = guarded
    module._test_main = main
    return module, calls


@pytest.fixture
def shell_runtime(monkeypatch, tmp_path):
    import label_match_screen as screen
    host = types.ModuleType("host")
    host.run_app = lambda *a, **kw: pytest.fail("failed preparation must not start the owner or a shell window")
    monkeypatch.setitem(sys.modules, "host", host)
    fileio = types.ModuleType("shell_fileio")
    fileio.set_write_guard = lambda guard: None
    fileio.make_dirs = lambda *a: None
    monkeypatch.setitem(sys.modules, "shell_fileio", fileio)
    webview = types.ModuleType("webview")
    webview.settings = {"WEBVIEW2_RUNTIME_PATH": None}
    webview.windows = []
    webview.create_window = lambda *a, **kw: pytest.fail("preparation must precede window creation")
    monkeypatch.setitem(sys.modules, "webview", webview)
    guilib = types.ModuleType("webview.guilib")
    guilib.initialize = lambda *a: types.SimpleNamespace(renderer="edgechromium")
    monkeypatch.setitem(sys.modules, "webview.guilib", guilib)
    core = types.ModuleType("Microsoft.Web.WebView2.Core")
    core.CoreWebView2Environment = types.SimpleNamespace(GetAvailableBrowserVersionString=lambda *a: "available")
    monkeypatch.setitem(sys.modules, "Microsoft.Web.WebView2.Core", core)
    monkeypatch.setattr(screen, "shell_data_dir", lambda: tmp_path / "screen")
    monkeypatch.setattr(screen, "shell_write_guard", lambda *a: nullcontext())
    return host, fileio, guilib


@pytest.mark.parametrize("failure", ["import", "directory", "webview2"])
def test_precreation_failure_uses_one_tk_inside_one_lease(application, shell_runtime, monkeypatch, failure):
    module, calls = application
    host, fileio, guilib = shell_runtime
    if failure == "import":
        original = builtins.__import__
        def import_module(name, *a, **kw):
            if name == "host": raise ImportError("unavailable")
            return original(name, *a, **kw)
        monkeypatch.setattr(builtins, "__import__", import_module)
    elif failure == "directory":
        def fail(*a): raise OSError("unavailable")
        fileio.make_dirs = fail
    else:
        guilib.initialize = lambda *a: types.SimpleNamespace(renderer="mshtml")
    result = module._test_main([])
    assert result == 0
    for event in ("lease-enter", "lease-exit", "hold", "processing", "visible-tk", "tk-loop", "finish", "verify"):
        assert calls.count(event) == 1
    codes = [s for s in calls if s.startswith("SHELL_")]
    assert codes == [{"import": "SHELL_IMPORT_FAILED", "directory": "SHELL_DATA_DIR_FAILED",
                      "webview2": "SHELL_WEBVIEW2_UNAVAILABLE"}[failure]]
    assert module.LABEL_MATCH_SCREEN_HOST["active"] is False


def test_failure_after_window_creation_never_starts_tk(application, shell_runtime, monkeypatch):
    module, calls = application
    host, _, _ = shell_runtime
    webview = sys.modules["webview"]
    webview.create_window = lambda *a, **kw: object()
    def fail(*a, **kw):
        webview.create_window()
        raise RuntimeError("after creation")
    host.run_app = fail
    with pytest.raises(RuntimeError):
        module._run_label_match_application()
    assert "processing" not in calls
    assert not any(s.startswith("SHELL_") for s in calls)


def test_writer_refusal_before_window_does_not_fall_back(application, shell_runtime):
    from writer_session_fence import WriterFenceError
    module, calls = application
    _, fileio, _ = shell_runtime
    def fail(*a): raise WriterFenceError("WRITER_FENCE_FAILED", "refused")
    fileio.make_dirs = fail
    with pytest.raises(WriterFenceError):
        module._run_label_match_application()
    assert "processing" not in calls
    assert not any(s.startswith("SHELL_") for s in calls)


def test_creation_failure_reuses_hidden_owner_tk(application, shell_runtime, monkeypatch):
    module, calls = application
    host, _, _ = shell_runtime
    scheduled = queue.Queue()
    attached = threading.Event()
    stopped = threading.Event()
    class App:
        def __init__(self, **kw):
            calls.append("processing")
            self.visible = False
        def after(self, delay, callback): scheduled.put(callback)
        def mainloop(self):
            while not self.visible and not stopped.is_set():
                scheduled.get()()
        def winfo_viewable(self): return self.visible
        def deiconify(self):
            self.visible = True
            calls.append("visible-tk")
        def _finish_after_mainloop(self): calls.append("finish")
    module.Label_Match = App
    class Bridge:
        _view_count = 0
        def attach_owner(self, processor): attached.set()
        def pump(self): return True
        def close(self): calls.append("bridge-close")
        def request_exit(self): calls.append("bridge-exit")
        def fail_owner(self, error): raise error
    class Processor:
        def __init__(self, *a): pass
        def uninstall(self): calls.append("uninstall")
    processor = types.ModuleType("label_screen_bridge")
    processor.LabelMatchScreenProcessor = Processor
    monkeypatch.setitem(sys.modules, "label_screen_bridge", processor)
    owners = []
    def run(*a, **kw):
        owner = threading.Thread(target=kw["owner_main"], args=(Bridge(),))
        owners.append(owner)
        owner.start()
        attached.wait()
        raise OSError("before creation")
    host.run_app = run
    try:
        assert module._test_main([]) == 0
    finally:
        stopped.set()
        scheduled.put(lambda: None)
        for owner in owners: owner.join()
    for event in ("lease-enter", "lease-exit", "hold", "processing", "visible-tk", "finish", "uninstall", "bridge-close"):
        assert calls.count(event) == 1
    assert [s for s in calls if s.startswith("SHELL_")] == ["SHELL_WINDOW_FAILED"]
