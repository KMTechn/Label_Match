"""Isolated fake-server rotation failure; the parent verifies a fresh official start."""
import json
from pathlib import Path
import sys

import pytest

from tests import test_register_label_match_reattach as fixtures
from tests.test_review_reattach_persistence import (
    _retarget_to_official_paths, _use_real_profile, _preserved_binding, _install_fake_server_admission,
)
from tests.test_current_user_onboarding import _environment
import current_user_onboarding as onboarding


def main(root, mode):
    root.mkdir()
    (root / 'fixture').mkdir()
    (root / 'app').mkdir()
    patch = pytest.MonkeyPatch()
    patch.setattr(fixtures, '_source_build_id', lambda: 'b468c5d8ce62e5dac77fd5c1454157dbabb11cab')
    case = fixtures.reattach_case.__wrapped__(root / 'fixture', patch)
    paths = onboarding.resolve_current_user_onboarding_paths(root / 'app', environ=_environment(root))
    _retarget_to_official_paths(case, paths)
    _use_real_profile(case, patch, paths.logistics_profile_path)
    _preserved_binding(case)
    server_path = root / 'server-state.json'
    server_path.write_text(json.dumps({
        'epoch': case.report()['credential_epoch'], 'retired_generations': 0,
        'producer_status': 'active', 'key_status': 'active', 'identity_released_epoch': 0,
        'producer_id': case.identity['producer_id'], 'source_host_id': case.identity['source_host_id'],
        'fingerprint': case.report()['possession_key_fingerprint'],
    }))
    _install_fake_server_admission(case, server_path)
    include_bundle = case.response_change
    def rotated(response):
        include_bundle(response)
        state = json.loads(server_path.read_text())
        state['epoch'] += 1
        state['retired_generations'] += 1
        response['credential_epoch'] = state['epoch']
        response['client_receipt']['credential_epoch'] = state['epoch']
        server_path.write_text(json.dumps(state))
        if mode == 'progress':
            (root / 'child-result.json').write_text(json.dumps({
                'failed_registration_exit': 3, 'server_rotated': True,
            }))
            __import__('os')._exit(0)
    case.response_change = rotated
    real_write = case.module._write_dpapi_secret
    def unavailable(*args, **kwargs):
        if mode == 'partial':
            real_write(*args, **kwargs)
            (root / 'child-result.json').write_text(json.dumps({
                'failed_registration_exit': 3, 'server_rotated': True,
            }))
            __import__('os')._exit(0)
        raise OSError()
    patch.setattr(case.module, '_write_dpapi_secret', unavailable)
    result = case.run()
    (root / 'child-result.json').write_text(json.dumps({
        'failed_registration_exit': result,
        'server_rotated': any(url.endswith('/reattach') for url, _ in case.calls),
    }))
    patch.undo()
    return 0


if __name__ == '__main__':
    raise SystemExit(main(Path(sys.argv[1]), sys.argv[2]))
