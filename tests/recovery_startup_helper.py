import Label_Match as lm
from package_logistics import PackageOutbox

def configure_main(monkeypatch, tmp_path, path):
    import auto_update
    import startup_observation
    monkeypatch.setattr(lm, 'LABEL_MATCH_SCREEN_HOST', dict(lm.LABEL_MATCH_SCREEN_HOST))
    monkeypatch.setattr(auto_update, 'startup_gate', lambda *a, **k: None)
    for name in ('dispatch_product_mode', 'validate_pending_worker_install', 'check_user_data_open', 'verify_factory_contract_startup', '_label_match_startup_trace'):
        monkeypatch.setattr(lm, name, lambda *a, **k: None)
    monkeypatch.setattr(lm, 'release_identity', lambda *a: {})
    monkeypatch.setattr(lm, '_first_run_onboarding_enabled', lambda: False)
    monkeypatch.setattr(lm, '_startup_app_stop_requested', lambda: False)
    monkeypatch.setattr(lm, '_default_label_match_settings_path', lambda: tmp_path / 'settings.json')
    monkeypatch.setattr(lm, 'resolve_data_scope', lambda **k: str(tmp_path))
    monkeypatch.setattr(lm, 'run_guarded_entrypoint', lambda *a, **k: PackageOutbox(path))
    notices = []
    monkeypatch.setattr(startup_observation, 'begin_startup', lambda *a, **k: None)
    monkeypatch.setattr(startup_observation, 'observe_startup', lambda reasons, **k: notices.append((reasons, k)))
    return notices
