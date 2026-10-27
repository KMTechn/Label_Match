"""Diagnostic publication failure must preserve the actual onboarding result."""
import json
from pathlib import Path
import pytest
import current_user_onboarding as onboarding
from tests import test_current_user_onboarding as support
from tests.test_current_user_onboarding import isolated_data_generation_release

@pytest.mark.parametrize('scene', ['late-marker', 'replacement-marker', 'json-number'])
def test_report_redaction_does_not_fail_actual_onboarding(tmp_path, monkeypatch, scene):
    app = tmp_path / 'app'
    app.mkdir()
    environment = support._environment(tmp_path)
    paths = onboarding.resolve_current_user_onboarding_paths(app, environ=environment)
    selected = ('KMTECH_TEST_API_TOKEN', 'REDACTED' if scene == 'replacement-marker' else 'KMTECH_TEST_SHORT' if scene == 'late-marker' else '4826')
    environment[selected[0]] = selected[1]
    if scene == 'late-marker':
        environment['KMTECH_TEST_SHORT_PASSWORD'] = 'x'
    for name, value in environment.items():
        if name.startswith("KMTECH_TEST_"):
            monkeypatch.setenv(name, value)
    calls = []
    def register(p):
        calls.append(p)
        support._ready_state(p)
        receipt = json.loads(p.registration_report_path.read_bytes())
        receipt['base_url'] = 'https://synthetic.invalid'
        p.registration_report_path.write_text(json.dumps(receipt), encoding='utf-8')
        return 0
    def autostart(*args, **kwargs):
        result = dict(support._autostart(*args, **kwargs))
        result['message'] = int(selected[1]) if scene == 'json-number' else selected[1]
        return result
    report = onboarding.onboard_current_user(app, environ=environment,
        require_bootstrap_integrity=False, registration_runner=register,
        profile_loader=support._profile_loader, credential_loader=support._credential_loader,
        ledger_factory=support._ledger_factory, autostart_installer=autostart,
        relay_launcher=support._relay_start, scheduled_task_remover=support._scheduled_task_absent,
        legacy_task_quiescence_reader=support._legacy_task_quiescent)
    assert report['status'] == 'READY'
    persisted = json.loads(paths.onboarding_report_path.read_bytes())
    receipt = json.loads(paths.registration_report_path.read_bytes())
    assert persisted['status'] == 'READY'
    assert persisted['server_registration_verified'] is True
    assert receipt['server_registration_verified'] is True
    assert receipt['base_url'] == 'https://synthetic.invalid'
    raw = paths.onboarding_report_path.read_text(encoding='utf-8')
    assert selected[0] not in raw and selected[1] not in raw
    assert len(calls) == 1
    assert paths.ledger_path.is_file()
    from report_publication import failure_count, UNAVAILABLE
    assert failure_count(paths.onboarding_report_path) == 1
    counters = list(paths.status_dir.glob('.report-unavailable-count-*.json'))
    assert len(counters) == 1
    assert json.loads(counters[0].read_bytes()) == {'count': 1}
    assert persisted['relay_autostart']['message'] == UNAVAILABLE


def test_original_registration_failure_is_still_failed(tmp_path):
    app = tmp_path / 'app'
    app.mkdir()
    environment = support._environment(tmp_path)
    paths = onboarding.resolve_current_user_onboarding_paths(app, environ=environment)
    with pytest.raises(onboarding.CurrentUserOnboardingError) as captured:
        onboarding.onboard_current_user(app, environ=environment,
            require_bootstrap_integrity=False, registration_runner=lambda p: 7,
            profile_loader=support._profile_loader, credential_loader=support._credential_loader,
            ledger_factory=support._ledger_factory, autostart_installer=support._autostart,
            relay_launcher=support._relay_start, scheduled_task_remover=support._scheduled_task_absent,
        legacy_task_quiescence_reader=support._legacy_task_quiescent)
    assert captured.value.status == 'FAILED'
    assert json.loads(paths.onboarding_report_path.read_bytes())['status'] == 'FAILED'


def test_registration_receipt_diagnostic_preserves_ready_and_origin(tmp_path, monkeypatch):
    from tools import register_label_match_worker_pc as registration_writer
    app = tmp_path / "app"
    app.mkdir()
    environment = support._environment(tmp_path)
    environment['KMTECH_TEST_API_TOKEN'] = '4826'
    monkeypatch.setenv('KMTECH_TEST_API_TOKEN', '4826')
    paths = onboarding.resolve_current_user_onboarding_paths(app, environ=environment)
    def register(selected):
        support._ready_state(selected)
        receipt = json.loads(selected.registration_report_path.read_bytes())
        receipt.update(base_url='https://synthetic.invalid', detail=4826)
        registration_writer._write_registration_report(selected.registration_report_path, receipt)
        return 0
    result = onboarding.onboard_current_user(app, environ=environment,
        require_bootstrap_integrity=False, registration_runner=register,
        profile_loader=support._profile_loader, credential_loader=support._credential_loader,
        ledger_factory=support._ledger_factory, autostart_installer=support._autostart,
        relay_launcher=support._relay_start, scheduled_task_remover=support._scheduled_task_absent,
        legacy_task_quiescence_reader=support._legacy_task_quiescent)
    assert result['status'] == 'READY'
    receipt = json.loads(paths.registration_report_path.read_bytes())
    assert receipt['server_registration_verified'] is True
    assert receipt['base_url'] == 'https://synthetic.invalid'
    raw = paths.registration_report_path.read_text(encoding='utf-8')
    assert '4826' not in raw and 'KMTECH_TEST_API_TOKEN' not in raw
    assert json.loads(paths.onboarding_report_path.read_bytes())['status'] == 'READY'


def test_report_failure_preserves_original_registration_failure(tmp_path, monkeypatch):
    from report_publication import failure_count
    app = tmp_path / "app"
    app.mkdir()
    environment = support._environment(tmp_path)
    environment['KMTECH_TEST_API_TOKEN'] = 'REDACTED'
    monkeypatch.setenv('KMTECH_TEST_API_TOKEN', 'REDACTED')
    paths = onboarding.resolve_current_user_onboarding_paths(app, environ=environment)
    original = ValueError('REDACTED')
    def register(selected):
        raise original
    with pytest.raises(onboarding.CurrentUserOnboardingError) as captured:
        onboarding.onboard_current_user(app, environ=environment,
            require_bootstrap_integrity=False, registration_runner=register,
            profile_loader=support._profile_loader, credential_loader=support._credential_loader,
            ledger_factory=support._ledger_factory, autostart_installer=support._autostart,
            relay_launcher=support._relay_start, scheduled_task_remover=support._scheduled_task_absent,
            legacy_task_quiescence_reader=support._legacy_task_quiescent)
    assert captured.value.status == 'FAILED'
    assert captured.value.__cause__ is original
    persisted = json.loads(paths.onboarding_report_path.read_bytes())
    assert persisted['status'] == 'FAILED'
    raw = paths.onboarding_report_path.read_text(encoding='utf-8')
    assert 'REDACTED' not in raw and 'KMTECH_TEST_API_TOKEN' not in raw
    assert failure_count(paths.onboarding_report_path) == 1
