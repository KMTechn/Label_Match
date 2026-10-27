"""GUI and CLI starts reach real first setup using the accepted startup proof."""
from contextlib import nullcontext
import json
import os
from pathlib import Path

import pytest

import auto_update as updates
import current_user_onboarding as onboarding
import Label_Match as app
import label_match_product_host as host
from tests.test_auto_update_startup_race import (
    managed_onboarding, base_managed_onboarding, isolated_data_generation_release,
)
from tests.test_portable_gui_onboarding import portable_gui
from tests.test_auto_update import OLD, NEW


class FirstSetupReached(BaseException):
    """Stop before enrollment, credentials, persistence or any GUI creation."""


@pytest.fixture
def first_setup(managed_onboarding, portable_gui, monkeypatch):
    paths, environment, control, _helpers = managed_onboarding
    selected = Path(paths.app_root)
    for name, value in environment.items():
        monkeypatch.setenv(name, value)
    monkeypatch.setattr(app, '__file__', str(selected / 'app/Label_Match.py'))
    monkeypatch.setattr(updates, '__file__', str(selected / 'app/auto_update.py'))
    monkeypatch.setattr(host, '__file__', str(selected / 'app/label_match_product_host.py'))
    monkeypatch.setattr(app, '_first_run_onboarding_enabled', lambda: True)
    monkeypatch.setattr(app, '_label_match_runtime_app_root', lambda: str(selected))
    monkeypatch.setattr(app, 'run_guarded_entrypoint',
                        lambda *_args, **_kwargs: pytest.fail('first setup was skipped'))
    monkeypatch.setattr(onboarding, 'try_window_first_start', lambda *_args, **_kwargs: None)
    monkeypatch.setattr(app, 'check_user_data_open', lambda *_args: None)
    monkeypatch.setattr(app, 'validate_pending_worker_install', lambda *_args: None)
    monkeypatch.setattr(app, '_startup_app_stop_requested', lambda: False)
    monkeypatch.setattr(updates, 'startup_update_lock', lambda *_args: nullcontext(True))
    monkeypatch.setattr(updates, 'current_generation_status', lambda *_args: 'ALLOW')
    monkeypatch.setattr(updates, 'process_snapshot', lambda *_args: [])
    monkeypatch.setattr(updates, 'request_bootstrap', lambda *_args, **_kwargs: None)
    monkeypatch.delenv('KMTECH_LM_UPDATE_DEFERRED_AT', raising=False)
    monkeypatch.delenv('KMTECH_LM_UPDATE_TRIAL_NONCE', raising=False)
    reached = []

    def enter_setup(root, **_kwargs):
        reached.append(Path(root))
        raise FirstSetupReached()

    monkeypatch.setattr(onboarding, 'prepare_current_user_data_open', enter_setup)
    return paths, control, reached


@pytest.mark.parametrize('mode', ['gui', 'cli'])
@pytest.mark.parametrize('phase,updated', [
    ('CONFIRMED', True), ('DEFERRED', True), ('PREPARED', True), ('CONFIRMED', False),
])
def test_actual_main_enters_first_setup(first_setup, monkeypatch, mode, phase, updated):
    paths, control, reached = first_setup
    selected = Path(paths.app_root)
    if not updated:
        fixed = control.parent / 'current/portable-manifest.json'
        fixed.write_text(json.dumps({'source_commit': NEW}))
    state = updates.read_json(control / 'transition.json')
    if phase == 'PREPARED':
        state = {**state, 'candidate_sha': '3' * 40, 'previous_sha': NEW}
    elif phase == 'CONFIRMED':
        state = {**state, 'candidate_sha': NEW, 'previous_sha': OLD}
    updates.write_json_durable(control / 'transition.json',
                               {**state, 'phase': phase})
    if phase == 'PREPARED':
        monkeypatch.setenv('KMTECH_LM_UPDATE_DEFERRED_AT', updates.now_utc())
    before = {path: path.read_bytes() for path in control.iterdir() if path.is_file()}
    arguments = [] if mode == 'gui' else ['--onboard-current-user', '--app-root', str(selected)]
    with pytest.raises(FirstSetupReached):
        app.main(arguments)
    assert reached == [selected]
    assert all(path.read_bytes() == content for path, content in before.items())


@pytest.mark.parametrize('mode', ['gui', 'cli'])
@pytest.mark.parametrize('refusal', ['unapproved', 'inactive_code', 'other_sha', 'generation', 'system'])
def test_prepared_start_preserves_existing_refusal(first_setup, monkeypatch, mode, refusal):
    paths, control, reached = first_setup
    state = updates.read_json(control / 'transition.json')
    updates.write_json_durable(control / 'transition.json', {**state, 'phase': 'PREPARED'})
    if refusal != 'unapproved':
        monkeypatch.setenv('KMTECH_LM_UPDATE_DEFERRED_AT', updates.now_utc())
    if refusal == 'inactive_code':
        updates.write_json_durable(control / 'active.json', {'schema': updates.SCHEMA, 'sha': OLD})
    if refusal == 'other_sha':
        monkeypatch.setattr(updates, 'process_snapshot', lambda *_args: [
            {'pid': 99123, 'observable': True, 'ordinary_user': True, 'sha': OLD,
             'sid': updates.load_policy(control)['operator_sid'], 'role': 'relay',
             'created_at': updates.now_utc()}])
    if refusal == 'generation':
        monkeypatch.setattr(updates, 'current_generation_status', lambda *_args: 'REFUSE')
    if refusal == 'system':
        monkeypatch.setattr(updates, 'ordinary_user_context', lambda *_args: False)
    arguments = [] if mode == 'gui' else ['--onboard-current-user', '--app-root', str(paths.app_root)]
    try:
        result = app.main(arguments)
    except (ValueError, RuntimeError):
        result = 20
    assert result in {0, 20}
    assert reached == []


@pytest.mark.parametrize('phase', ['FAILED', 'UNKNOWN', 'APPLIED_PENDING_READY', 'ROLLBACK_PENDING_READY', 'ROLLBACK_PENDING'])
def test_other_accepted_deferred_starts_share_the_same_proof(first_setup, monkeypatch, phase):
    paths, control, _reached = first_setup
    state = {'schema': updates.SCHEMA, 'phase': phase, 'candidate_sha': NEW,
             'previous_sha': NEW, 'nonce': 'b' * 32, 'transition_id': 'c' * 32}
    updates.write_json_durable(control / 'transition.json', state)
    monkeypatch.setenv('KMTECH_LM_UPDATE_DEFERRED_AT', updates.now_utc())
    monkeypatch.setattr(updates, 'apply_claim_status', lambda *_args: 'ABSENT')
    if phase == 'ROLLBACK_PENDING':
        monkeypatch.setattr(updates, 'process_snapshot', lambda *_args: [
            {'pid': 99123, 'observable': True, 'ordinary_user': True, 'sha': NEW,
             'sid': updates.load_policy(control)['operator_sid'], 'role': 'relay',
             'created_at': updates.now_utc()}])
    before = {path: path.read_bytes() for path in control.iterdir() if path.is_file()}
    assert updates.startup_gate(Path(paths.app_root) / 'app', []) is None
    assert updates.resolve_lifecycle_context(control.parent, operation='register', startup=True)['from_commit'] == NEW
    assert all(path.read_bytes() == content for path, content in before.items())


@pytest.mark.parametrize('phase', ['APPLIED_PENDING_READY', 'ROLLBACK_PENDING_READY'])
@pytest.mark.parametrize('changed_claim', [False, True])
def test_trial_first_setup_rechecks_the_original_claim(first_setup, monkeypatch, phase, changed_claim):
    paths, control, _reached = first_setup
    state = {'schema': updates.SCHEMA, 'phase': phase, 'candidate_sha': NEW,
             'previous_sha': NEW, 'nonce': 'b' * 32, 'transition_id': 'c' * 32}
    updates.write_json_durable(control / 'transition.json', state)
    stage = Path(updates.load_policy(control)['staging_root'])
    claim = {'nonce': state['nonce'], 'transition_id': state['transition_id'], 'pid': os.getpid()}
    (stage / 'trial.json').write_text(json.dumps(claim))
    monkeypatch.setenv('KMTECH_LM_UPDATE_TRIAL_NONCE', state['nonce'])
    assert updates.startup_gate(Path(paths.app_root) / 'app', []) is None
    if changed_claim:
        (stage / 'trial.json').write_text(json.dumps({**claim, 'pid': os.getpid() + 1}))
        with pytest.raises(ValueError):
            updates.resolve_lifecycle_context(control.parent, operation='register', startup=True)
    else:
        assert updates.resolve_lifecycle_context(control.parent, operation='register', startup=True)['from_commit'] == NEW


@pytest.mark.parametrize('change', ['no_gate', 'generation', 'system', 'writer', 'journal'])
def test_registration_rechecks_start_proof_before_entering_setup(first_setup, monkeypatch, change):
    paths, control, _reached = first_setup
    state = {'schema': updates.SCHEMA, 'phase': 'PREPARED', 'previous_sha': NEW,
             'candidate_sha': '3' * 40}
    updates.write_json_durable(control / 'transition.json', state)
    monkeypatch.setenv('KMTECH_LM_UPDATE_DEFERRED_AT', updates.now_utc())
    if change != 'no_gate':
        assert updates.startup_gate(Path(paths.app_root) / 'app', []) is None
    if change == 'generation':
        monkeypatch.setattr(updates, 'current_generation_status', lambda *_args: 'REFUSE')
    elif change == 'system':
        monkeypatch.setattr(updates, 'ordinary_user_context', lambda *_args: False)
    elif change == 'writer':
        monkeypatch.setattr(updates, 'process_snapshot', lambda *_args: [
            {'pid': 99123, 'observable': True, 'ordinary_user': True, 'sha': OLD,
             'sid': updates.load_policy(control)['operator_sid'], 'role': 'relay', 'created_at': updates.now_utc()}])
    elif change == 'journal':
        updates.write_json_durable(control / 'transition.json', {**state, 'candidate_sha': '4' * 40})
    with pytest.raises(ValueError):
        updates.resolve_lifecycle_context(control.parent, operation='register', startup=True)


@pytest.mark.parametrize('operation', ['install', 'inspect', 'register'])
def test_start_proof_does_not_change_manual_classification(first_setup, monkeypatch, operation):
    paths, control, _reached = first_setup
    updates.write_json_durable(control / 'transition.json',
                               {'schema': updates.SCHEMA, 'phase': 'PREPARED',
                                'candidate_sha': '3' * 40, 'previous_sha': NEW})
    monkeypatch.setenv('KMTECH_LM_UPDATE_DEFERRED_AT', updates.now_utc())
    assert updates.startup_gate(Path(paths.app_root) / 'app', []) is None
    with pytest.raises(ValueError):
        updates.resolve_lifecycle_context(control.parent, operation=operation)
