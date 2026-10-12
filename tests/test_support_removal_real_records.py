"""Removal accepts real initial and supported-placement record shapes."""
from pathlib import Path
from types import SimpleNamespace
import json
import shutil
import uuid
import pytest
import auto_update as update
import current_user_onboarding as onboarding
import label_data_generation as guard
from tests.test_support_packet_removal import packets, _remove
from tests.test_support_removal_after_update import updated_packets, _record_installed_packet

@pytest.fixture
def initial_packets(tmp_path, monkeypatch, packets):
    source, installed, environment = packets
    from protected_admin import current_process_sid
    seed = SimpleNamespace(installed=installed, support=source, environment=environment, subject=current_process_sid(), observed=[], paths=onboarding.resolve_current_user_onboarding_paths(installed, environ=environment), event=Path(environment['LABEL_MATCH_SAVE_DIR']) / 'work.csv')
    key = (Path(__file__).resolve().parents[1] / 'config/update-manifest-key-config.json').read_bytes()
    for bundle in (installed, source):
        (bundle / 'app/update-manifest-key-config.json').write_bytes(key)
    managed = tmp_path / 'initial-managed'
    fixed = managed / 'current'
    shutil.copytree(seed.installed, fixed, ignore=shutil.ignore_patterns('__pycache__'))
    _record_installed_packet(tmp_path, fixed)
    assert not (seed.support / 'bootstrap-integrity.json').exists()
    sha = update.read_json(fixed / 'portable-manifest.json')['source_commit']
    active = managed / 'versions' / sha
    shutil.copytree(fixed, active)
    control = managed / 'update-control'
    stage = managed / 'stage'
    control.mkdir()
    stage.mkdir()
    update.write_json_durable(control / 'policy.json', {'schema': 'label-match-startup-update-policy-v1', 'operator_sid': seed.subject, 'operator_local_app_data': str(tmp_path / 'operator'), 'staging_root': str(stage), 'channel': 'stable', 'feed_url': '/'.join(('https:', '', 'example.invalid', 'feed'))})
    update.write_json_durable(control / 'active.json', {'schema': update.SCHEMA, 'sha': sha, 'transition_id': 'CANONICAL_INSTALL'})
    update.write_json_durable(control / 'transition.json', {'schema': update.SCHEMA, 'phase': 'DEFERRED', 'deferred_at': '2026-10-12T00:00:00Z', 'reason': 'canonical installation', 'failed_shas': []})

    def remove(packet=seed.support):
        monkeypatch.setattr(guard, '__file__', str(packet / 'app/label_data_generation.py'))
        guard._RELEASE_IDENTITY_PASS.clear()
        return _remove(fixed, seed.environment, seed.observed)
    return SimpleNamespace(seed=seed, fixed=fixed, active=active, control=control, remove=remove)

def test_initial_install_records_allow_same_release_support_removal(initial_packets, monkeypatch):
    packets = initial_packets
    selected = []
    original = update.version_bundle

    def version_bundle(control, sha):
        root = original(control, sha)
        selected.append(root)
        return root
    monkeypatch.setattr(update, 'version_bundle', version_bundle)
    pointer = update.read_json(packets.control / 'active.json')
    state = update.read_json(packets.control / 'transition.json')
    assert pointer['transition_id'] == 'CANONICAL_INSTALL'
    assert 'transition_id' not in state
    event = packets.seed.event.read_bytes()
    assert packets.remove()['status'] == 'PASS_DATA_PRESERVED'
    assert packets.seed.event.read_bytes() == event
    assert selected == [packets.active]

def test_supported_placement_records_allow_candidate_source_removal(tmp_path, updated_packets):
    packets = updated_packets
    event = packets.seed.event.read_bytes()
    before = {path: path.read_bytes() for path in packets.markers}
    assert packets.remove(packets.active)['status'] == 'PASS_DATA_PRESERVED'
    candidate_sha = update.read_json(packets.candidate / 'portable-manifest.json')['source_commit']
    placed = packets.fixed.parent / 'versions' / candidate_sha
    shutil.copytree(packets.candidate, placed, ignore=shutil.ignore_patterns('__pycache__'))
    _record_installed_packet(tmp_path, placed)
    state = dict(schema=update.SCHEMA, phase='DEFERRED', failed_shas=[], deferred_at='2026-10-12T00:00:00Z', reason='supported placement', transition_id=uuid.uuid4().hex)
    update.persist_switch(packets.control, state, candidate_sha)
    assert update.read_json(packets.control / 'active.json')['transition_id'] == state['transition_id']
    assert update.read_json(packets.control / 'transition.json')['transition_id'] == state['transition_id']
    assert packets.remove(packets.candidate)['status'] == 'PASS_DATA_PRESERVED'
    assert packets.remove(packets.candidate)['status'] == 'PASS_DATA_PRESERVED'
    assert packets.seed.event.read_bytes() == event
    assert all((path.read_bytes() == value for path, value in before.items()))
