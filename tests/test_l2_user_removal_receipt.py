import json

import auto_update
from current_user_onboarding import remove_current_user_setup, resolve_current_user_onboarding_paths
from tests.test_current_user_onboarding import _environment, _ready_state, isolated_data_generation_release


def test_repeated_user_removal_reissues_bound_receipt(tmp_path):
    diagnostic = tmp_path / 'l2-receipt-result.json'
    def mark(stage, **details):
        diagnostic.write_text(json.dumps(dict(stage=stage, **details)), encoding='utf-8')
    mark('paths')
    app = tmp_path / 'release'
    app.mkdir()
    environment = _environment(tmp_path)
    paths = resolve_current_user_onboarding_paths(app, environ=environment)
    _ready_state(paths)
    paths.ledger_path.parent.mkdir(parents=True, exist_ok=True)
    paths.ledger_path.write_bytes(b'preserved ledger')
    sid = auto_update.current_process_sid()
    events = []

    def absent(name):
        events.append(name)
        return {'status': 'ABSENT'}

    arguments = dict(environ=environment,
        autostart_remover=lambda: absent('run'),
        relay_stopper=lambda _: absent('relay'),
        scheduled_task_remover=lambda _: absent('task'))
    mark('first_removal')
    try:
        first = remove_current_user_setup(app, **arguments)
    except Exception as exc:
        mark('first_removal_error', error_type=type(exc).__name__)
        raise
    mark('first_receipt', sid_matches=first.get('operator_sid') == sid)
    assert first['operator_sid'] == sid
    old = dict(first)
    old.pop('operator_sid')
    paths.removal_report_path.write_text(json.dumps(old), encoding='utf-8')
    mark('second_removal')
    try:
        second = remove_current_user_setup(app, **arguments)
    except Exception as exc:
        mark('second_removal_error', error_type=type(exc).__name__)
        raise
    stored = json.loads(paths.removal_report_path.read_bytes())
    mark('second_receipt', status_matches=second.get('status') == stored.get('status') == 'PASS_DATA_PRESERVED',
        sid_matches=second.get('operator_sid') == stored.get('operator_sid') == sid,
        version_matches=second.get('report_version') == first.get('report_version'),
        callbacks_twice=events == ['run','relay','task'] * 2,
        ledger_preserved=paths.ledger_path.read_bytes() == b'preserved ledger',
        identity_preserved=paths.identity_path.is_file(),profile_preserved=paths.logistics_profile_path.is_file())
    assert second['status'] == stored['status'] == 'PASS_DATA_PRESERVED'
    assert second['operator_sid'] == stored['operator_sid'] == sid
    assert second['report_version'] == first['report_version']
    assert events == ['run', 'relay', 'task'] * 2
    assert paths.ledger_path.read_bytes() == b'preserved ledger'
    assert paths.identity_path.is_file()
    assert paths.logistics_profile_path.is_file()
