"""Enter the real startup function and finish onboarding before any real window."""
import json
from pathlib import Path
from types import SimpleNamespace
import pytest
import current_user_onboarding as onboarding
from tests import test_current_user_onboarding as support
from tests.test_current_user_onboarding import isolated_data_generation_release

class ReadyReached(BaseException):
    pass


def test_actual_startup_continues_after_diagnostic_refusal(tmp_path, monkeypatch):
    import Label_Match as gui
    app = tmp_path / "app"
    app.mkdir()
    environment = support._environment(tmp_path)
    environment['KMTECH_TEST_API_TOKEN'] = 'REDACTED'
    for name, value in environment.items():
        monkeypatch.setenv(name, value)
    paths = onboarding.resolve_current_user_onboarding_paths(app, environ=environment)
    seen = []
    def register(selected):
        support._ready_state(selected)
        return 0
    def autostart(*args, **kwargs):
        return {**support._autostart(*args, **kwargs), 'message': 'REDACTED'}
    def actual_onboarding(*args, **kwargs):
        result = onboarding.onboard_current_user(app, environ=environment,
            require_bootstrap_integrity=False, registration_runner=register,
            profile_loader=support._profile_loader, credential_loader=support._credential_loader,
            ledger_factory=support._ledger_factory, autostart_installer=autostart,
            relay_launcher=support._relay_start, _startup=kwargs.get("_startup", False), scheduled_task_remover=support._scheduled_task_absent,
            legacy_task_quiescence_reader=support._legacy_task_quiescent)
        seen.append(result['status'])
        return result
    def reached(*args, **kwargs):
        raise ReadyReached()
    monkeypatch.setattr(gui, 'onboard_current_user', actual_onboarding)
    monkeypatch.setattr(gui, '_first_run_onboarding_enabled', lambda: True)
    monkeypatch.setattr(gui, 'verify_factory_contract_startup', lambda: None)
    monkeypatch.setattr(gui, '_show_first_run_onboarding_error', lambda error: None)
    import auto_update
    monkeypatch.setattr(onboarding, 'try_window_first_start', lambda *args, **kwargs: None)
    monkeypatch.setattr(gui, '_label_match_runtime_app_root', lambda: app)
    monkeypatch.setattr(gui, 'release_identity', lambda *args: {})
    monkeypatch.setattr(auto_update, 'startup_gate', lambda *args, **kwargs: None)
    monkeypatch.setattr(gui, 'dispatch_product_mode', lambda *args: None)
    monkeypatch.setattr(gui, '_startup_app_stop_requested', lambda: False)
    monkeypatch.setattr(gui, 'validate_pending_worker_install', lambda *args: None)
    monkeypatch.setattr(gui, 'check_user_data_open', lambda *args: None)
    monkeypatch.setattr(gui, 'requires_bootstrap_integrity', lambda *args: False)
    monkeypatch.setattr(gui, '_default_label_match_settings_path', reached)
    with pytest.raises(ReadyReached):
        gui.main([])
    assert seen == ['READY']
    persisted = json.loads(paths.onboarding_report_path.read_bytes())
    assert persisted['status'] == 'READY'
    assert persisted['server_registration_verified'] is True
    raw = paths.onboarding_report_path.read_text(encoding='utf-8')
    assert 'REDACTED' not in raw and 'KMTECH_TEST_API_TOKEN' not in raw
