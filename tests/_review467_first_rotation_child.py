"""Isolated first reattach with a retained fake CNG key and no prior documents."""
import json
from pathlib import Path
from types import SimpleNamespace
import sys

import pytest
from tests import test_register_label_match_reattach as fixtures
from tests.test_review_reattach_persistence import (
    _use_real_profile, _install_test_producer_crypto,
    _install_fake_server_admission,
)
from tests.test_current_user_onboarding import _environment
import current_user_onboarding as onboarding


def prepare_case(root, patch, seed_name='fixture-parent'):
    fixture_root = root / seed_name
    fixture_root.mkdir(exist_ok=True)
    (root / 'app').mkdir(exist_ok=True)
    case = fixtures.reattach_case.__wrapped__(fixture_root, patch)
    paths = onboarding.resolve_current_user_onboarding_paths(root / 'app', environ=_environment(root))
    # Retarget the fixture without creating/removing any product document.
    # This also leaves a future fix's first-phase binding files intact.
    case.data = paths.direct_sync_root
    case.data.mkdir(parents=True, exist_ok=True)
    case.identity_path = paths.identity_path
    for option, value in (('--data-dir', paths.direct_sync_root), ('--sync-dir', paths.data_root)):
        case.args[case.args.index(option) + 1] = str(value)
    case.report = lambda: json.loads(paths.registration_report_path.read_text())
    context = json.loads((root / 'fixture-context.json').read_text())
    sid = '-'.join(['S', '1', '5', '21', *(str(100 + i) for i in range(4))])
    patch.setattr(case.module, '_current_machine_guid', lambda: context['machine_guid'])
    patch.setattr(case.module, '_current_user_sid', lambda: sid)
    import tools.register_label_match_worker_pc as official_registration
    patch.setattr(official_registration, '_current_machine_guid', lambda: context['machine_guid'])
    patch.setattr(official_registration, '_current_user_sid', lambda: sid)
    patch.setattr(case.module.socket, 'gethostname', lambda: context['pc_name'])
    candidate = case.module._derive_identity(SimpleNamespace(
        data_dir=str(case.data), identity_path=str(paths.identity_path),
        pc_id='', machine_guid='', source_host_id='', producer_install_id='',
        producer_id='', key_id='', secret_ref_target='',
    ), case.module.DEFAULT_SERVER_BASE_URL.rstrip('/') + '/api/producer-ingest/v1')
    case.identity = {'schema_version': case.module.PRODUCER_IDENTITY_SCHEMA_VERSION,
                    **{k: candidate[k] for k in ('producer_id','producer_install_id','source_host_id','pc_id')}}
    case.reused_initial_key = True
    _use_real_profile(case, patch, paths.logistics_profile_path)
    _install_test_producer_crypto(case)
    server_path = root / 'server-state.json'
    if not server_path.exists():
        fingerprint = fixtures.fake_possession_descriptor()['fingerprint']
        server_path.write_text(json.dumps({
            'epoch': 1, 'credential_epoch': 1, 'released_epoch': 0,
            'identity_released_epoch': 0, 'retired_generations': 0,
            'producer_status': 'active', 'key_status': 'active',
            'producer_id': case.identity['producer_id'],
            'source_host_id': case.identity['source_host_id'],
            'fingerprint': fingerprint, 'possession_key_fingerprint': fingerprint,
        }))
    _install_fake_server_admission(case, server_path)
    include_bundle = case.response_change
    def rotated(response):
        include_bundle(response)
        state = json.loads(server_path.read_text())
        state['epoch'] += 1
        state['credential_epoch'] = state['epoch']
        state['retired_generations'] += 1
        response['credential_epoch'] = state['epoch']
        response['client_receipt']['credential_epoch'] = state['epoch']
        server_path.write_text(json.dumps(state))
    case.response_change = rotated
    if '--server-base-url' in case.args:
        base_url = case.args[case.args.index('--server-base-url') + 1]
    elif '--endpoint-url' in case.args:
        base_url = case.args[case.args.index('--endpoint-url') + 1].split('/api/', 1)[0]
    else:
        base_url = case.module.DEFAULT_SERVER_BASE_URL
    return case, paths, server_path, base_url


def main(root, mode):
    patch = pytest.MonkeyPatch()
    case, paths, server_path, _base = prepare_case(root, patch, seed_name='fixture-child')
    before = {
        'had_normal_report_before': paths.registration_report_path.exists(),
        'had_receipt_before': paths.registration_receipt_path.exists(),
        'had_identity_before': paths.identity_path.exists(),
    }
    def phase(returned, code):
        value = dict(before, registration_returned=returned, exit=code,
                     retained_key_reused=bool(case.initial_key_calls),
                     server_epoch=json.loads(server_path.read_text())['epoch'])
        (root / 'first-phase.json').write_text(json.dumps(value))
    if mode == 'response_exit':
        change = case.response_change
        def lost_response(response):
            change(response)
            phase(False, None)
            __import__('os')._exit(0)
        case.response_change = lost_response
    elif mode in {'marker_before', 'diagnostic_after'}:
        real_json = case.module._write_json
        def unavailable(path, payload):
            if Path(path).name == case.module.REATTACH_PERSISTENCE_FILENAME and (
                mode == 'marker_before' or payload.get('server_credential_rotated') is True
            ):
                raise OSError('injected owned diagnostic write failure')
            return real_json(path, payload)
        patch.setattr(case.module, '_write_json', unavailable)
    else:
        def unavailable(*_args, **_kwargs):
            raise OSError('injected owned producer storage failure')
        patch.setattr(case.module, '_write_dpapi_secret', unavailable)
    result = case.run()
    phase(True, result)
    patch.undo()
    return 0


if __name__ == '__main__':
    raise SystemExit(main(Path(sys.argv[1]), sys.argv[2]))
