from __future__ import annotations
import ast
from pathlib import Path
import sys
import traceback
from types import SimpleNamespace
import pytest
import writer_session_fence as fence
ROOT = Path(__file__).resolve().parents[1]

from _startup_timing_fixture import bind_startup_timing_namespace as _bind_startup_timing_namespace

from _startup_timing_fixture import startup_body_namespace as _startup_body_namespace
from _startup_timing_fixture import bind_startup_exception_namespace as _bind_startup_exception_namespace

def entrypoint(monkeypatch, tmp_path, *, refused=None, update_result=None):
    import auto_update
    import current_user_onboarding
    import startup_observation
    calls = []
    host = {'requested': False, 'active': False}
    monkeypatch.setattr(current_user_onboarding, 'try_window_first_start', lambda *args, **kwargs: None)
    monkeypatch.setattr(auto_update, 'startup_gate', lambda root, args: update_result)
    monkeypatch.setattr(startup_observation, 'begin_startup', lambda *args, **kwargs: None)
    monkeypatch.setattr(startup_observation, 'observe_startup', lambda *args, **kwargs: None)

    def guard(start, **kwargs):
        calls.append('guard')
        return int(start() or 0)

    def check(*args):
        if refused is not None:
            raise refused
    namespace = dict(sys=sys, os=__import__('os'), Path=Path, __file__=str(ROOT / 'Label_Match.py'), LABEL_MATCH_NEW_SCREEN_ARGUMENT='--new-screen', LABEL_MATCH_SCREEN_HOST=host, _first_run_onboarding_enabled=lambda: False, _label_match_runtime_app_root=lambda: tmp_path, release_identity=lambda root: {}, dispatch_product_mode=lambda args: None, verify_factory_contract_startup=lambda: None, _startup_app_stop_requested=lambda: False, validate_pending_worker_install=lambda root: None, check_user_data_open=check, _label_match_startup_trace=lambda *args, **kwargs: None, _default_label_match_settings_path=lambda: tmp_path / 'settings.json', resolve_data_scope=lambda **kwargs: str(tmp_path), resource_path=lambda value: value, run_guarded_entrypoint=guard, _run_label_match_application=lambda **kwargs: calls.append('web' if host['requested'] else 'tk'), WriterFenceError=fence.WriterFenceError, CurrentUserOnboardingError=type('OnboardingRefused', (RuntimeError,), {}), DataGenerationError=type('DataRefused', (RuntimeError,), {}), ItemCatalogSyncError=type('CatalogRefused', (RuntimeError,), {}), _show_installation_recovery_required=lambda error: calls.append('refused'), ONBOARDING_EXIT_CODE=1)
    tree = ast.parse((ROOT / 'Label_Match.py').read_text(encoding='utf-8-sig'))
    main = next((node for node in tree.body if isinstance(node, ast.FunctionDef) and node.name == 'main'))
    _bind_startup_timing_namespace(namespace)
    _bind_startup_exception_namespace(namespace)
    namespace.setdefault("traceback", traceback)
    exec(compile(ast.Module(body=[main], type_ignores=[]), str(ROOT / 'Label_Match.py'), 'exec'), namespace)
    return (namespace['main'], calls, host)

@pytest.mark.parametrize('arguments, expected', [([], 'web'), (['--ui', 'tk'], 'tk'), (['--old-screen'], 'tk'), (['--new-screen'], 'web'), (['--ui', 'web'], 'web'), (['--ui', 'unknown'], 'web'), (['--ui'], 'web'), (['--ui=tk'], 'tk'), (['--old-screen', '--ui', 'web'], 'tk')])
def test_default_and_emergency_ui_arguments(monkeypatch, tmp_path, arguments, expected):
    main, calls, host = entrypoint(monkeypatch, tmp_path)
    assert main(arguments) == 0
    assert calls == ['guard', expected]

def test_startup_refusal_does_not_reach_either_screen(monkeypatch, tmp_path):
    main, calls, host = entrypoint(monkeypatch, tmp_path, refused=fence.WriterFenceError('TEST_START_REFUSED', 'startup refused'))
    assert main([]) == 1 and calls == ['refused']

def test_update_in_progress_does_not_reach_either_screen(monkeypatch, tmp_path):
    main, calls, host = entrypoint(monkeypatch, tmp_path, update_result=17)
    assert main([]) == 17 and calls == []

def test_auto_update_restart_without_ui_arguments_selects_new_screen(monkeypatch, tmp_path):
    import auto_update
    source = ast.parse(Path(auto_update.__file__).read_text(encoding='utf-8-sig'))
    launch = next((node for node in source.body if isinstance(node, ast.FunctionDef) and node.name == 'launch_version'))
    calls = []
    version = tmp_path / 'versions' / 'candidate'
    (version / 'runtime').mkdir(parents=True)
    (version / 'app').mkdir()
    namespace = dict(auto_update.__dict__)

    def start(command, **kwargs):
        calls.append(command)
        return SimpleNamespace(pid=17)
    namespace.update(version_bundle=lambda *args: version, version_manifest=lambda bundle: {'source_commit': 'candidate'}, start_ordinary_process=start)
    exec(compile(ast.Module(body=[launch], type_ignores=[]), auto_update.__file__, 'exec'), namespace)
    namespace['launch_version'](tmp_path, 'candidate', [])
    assert len(calls) == 1
    assert '--ui' not in calls[0] and '--old-screen' not in calls[0]
    main, routes, host = entrypoint(monkeypatch, tmp_path)
    assert main(calls[0][4:]) == 0 and routes == ['guard', 'web']


@pytest.mark.parametrize("arguments", [[], ["--ui", "tk"], ["--old-screen"], ["--ui", "unknown"]])
def test_ui_selection_preserves_startup_order_and_settings(monkeypatch, tmp_path, arguments):
    import auto_update
    import startup_observation
    main, calls, host = entrypoint(monkeypatch, tmp_path)
    settings = tmp_path / "settings.json"
    settings.write_bytes(b'{"unchanged":true}\n')
    before = settings.read_bytes()
    order = []
    namespace = _startup_body_namespace(main)
    def note(name, value=None):
        order.append(name)
        return value
    namespace["release_identity"] = lambda root: note("identity", {})
    def update(root, args):
        assert args == ["--unrelated-option"]
        return note("update")
    monkeypatch.setattr(auto_update, "startup_gate", update)
    namespace["dispatch_product_mode"] = lambda args: note("dispatch")
    monkeypatch.setattr(startup_observation, "begin_startup", lambda *args, **kwargs: note("begin"))
    namespace["verify_factory_contract_startup"] = lambda: note("factory")
    namespace["validate_pending_worker_install"] = lambda root: note("pending")
    namespace["check_user_data_open"] = lambda root: note("data")
    original_guard = namespace["run_guarded_entrypoint"]
    def guard(*args, **kwargs):
        note("guard")
        return original_guard(*args, **kwargs)
    namespace["run_guarded_entrypoint"] = guard
    assert main([*arguments, "--unrelated-option"]) == 0
    assert order == ["identity", "update", "dispatch", "begin", "factory", "pending", "data", "guard"]
    assert settings.read_bytes() == before


def test_emergency_ui_applies_only_to_one_launch(monkeypatch, tmp_path):
    main, calls, host = entrypoint(monkeypatch, tmp_path)
    assert main(["--ui", "tk"]) == 0
    assert calls == ["guard", "tk"]
    calls.clear()
    assert main([]) == 0
    assert calls == ["guard", "web"]
