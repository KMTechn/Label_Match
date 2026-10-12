import subprocess
'Actual A-to-B application with removal bound to the selected stock packet.'
from contextlib import nullcontext
from pathlib import Path
from types import SimpleNamespace
import json
import re
import shutil
import pytest
import auto_update as update
import current_user_onboarding as onboarding
import label_data_generation as guard
import Label_Match
from tests.test_support_packet_removal import packets, _remove
from tests.test_current_user_onboarding import _write_bootstrap_inventory_record
from tests.test_auto_update import bind_test_packet_settings

def _record_installed_packet(_tmp_path, packet):
    _write_bootstrap_inventory_record(packet)
    path = packet / 'bootstrap-integrity.json'
    record = json.loads(path.read_bytes())
    record['code_root'] = '.'
    path.write_bytes(json.dumps(record).encode('utf-8'))

def signed(current, candidate):
    current = dict(current, writer_sink_inventory_contract_sha256=current['contract_sha256'])
    candidate = dict(candidate, writer_sink_inventory_contract_sha256=candidate['contract_sha256'])
    pair = dict(zip(update.PIN_FIELDS, (current['source_commit'], candidate['source_commit'], current['contract_sha256'], candidate['contract_sha256'])))
    reverse = dict(zip(update.PIN_FIELDS, (pair['to_commit'], pair['from_commit'], pair['to_contract_sha256'], pair['from_contract_sha256'])))
    forward, rollback = update.bidirectional_pins([pair, reverse], current=current, candidate=candidate)
    return dict(version=candidate['version'], source_commit=candidate['source_commit'], approved_transitions=[pair, reverse], auto_update=dict(from_commit=pair['from_commit'], to_commit=pair['to_commit'], forward_contract_sha256=forward, rollback_contract_sha256=rollback))

@pytest.fixture
def updated_packets(tmp_path, monkeypatch, packets):
    import auto_update as update
    import current_user_onboarding as onboarding
    import label_data_generation as guard
    from tests.test_auto_update import OLD, NEW, LATER
    source, installed, environment = packets
    from protected_admin import current_process_sid
    seed = SimpleNamespace(installed=installed, support=source, environment=environment, subject=current_process_sid(), observed=[], paths=onboarding.resolve_current_user_onboarding_paths(installed, environ=environment), event=Path(environment['LABEL_MATCH_SAVE_DIR']) / 'work.csv')
    key_bytes = (Path(__file__).resolve().parents[1] / 'config/update-manifest-key-config.json').read_bytes()
    for bundle in (installed, source):
        (bundle / 'app/update-manifest-key-config.json').write_bytes(key_bytes)
    managed = tmp_path / 'm'
    fixed = managed / 'current'
    old = managed / 'versions' / OLD
    active = managed / 'versions' / NEW
    candidate = tmp_path / 'candidate'
    old_support = tmp_path / 'support-a'
    for target, version, build in ((fixed, 'v2.0.98', OLD), (seed.support, 'v2.0.99', NEW), (candidate, 'v2.0.100', 'c' * 40)):
        if target != seed.support:
            shutil.copytree(seed.installed, target, ignore=shutil.ignore_patterns('bootstrap-integrity.json'))
        path = target / 'portable-manifest.json'
        metadata = json.loads(path.read_bytes())
        for name in ('data_generation_signed_manifest', 'data_generation_signature_hex'):
            metadata.pop(name, None)
        metadata.update(version=version, source_commit=build, startup_update_protocol=1, contract_sha256=('d' if build == OLD else 'e') * 64, data_api_contract_sha256='f' * 64, writer_sink_inventory_contract_sha256='f' * 64)
        path.write_text(json.dumps(metadata), encoding='utf-8')
        (target / 'app/Label_Match.py').write_text(f'CURRENT_VERSION = "{version}"\n', encoding='utf-8')
        code_path = target / 'app/label_data_generation.py'
        code = code_path.read_text(encoding='utf-8')
        code = re.sub('(?m)^CURRENT_VERSION\\s*=.*$', 'CURRENT_VERSION = ' + repr(version), code)
        code_path.write_bytes(code.encode('utf-8'))
    _record_installed_packet(tmp_path, fixed)
    shutil.copytree(fixed, old)
    shutil.copytree(fixed, old_support, ignore=shutil.ignore_patterns('bootstrap-integrity.json'))
    _record_installed_packet(tmp_path, seed.support)
    candidate_record = seed.support / 'bootstrap-integrity.json'
    control = managed / 'update-control'
    stage = tmp_path / 'stage'
    control.mkdir()
    stage.mkdir()
    policy = {'schema': 'label-match-startup-update-policy-v1', 'operator_sid': seed.subject, 'operator_local_app_data': str(tmp_path / 'operator'), 'staging_root': str(stage), 'channel': 'stable', 'feed_url': '/'.join(('https:', '', 'example.invalid', 'feed'))}
    bind_test_packet_settings(control, policy)
    update.write_json_durable(control / 'policy.json', policy)
    settings = (fixed / 'app/auto-update-settings.json').read_bytes()
    for bundle in (old, seed.support, candidate, old_support):
        (bundle / 'app/auto-update-settings.json').write_bytes(settings)
    for bundle in (fixed, old, seed.support):
        _record_installed_packet(tmp_path, bundle)
    update.write_json_durable(control / 'active.json', {'schema': update.SCHEMA, 'sha': OLD, 'transition_id': 'CANONICAL_INSTALL'})
    update.write_json_durable(control / 'transition.json', {'schema': update.SCHEMA, 'phase': 'DEFERRED', 'deferred_at': '2026-10-12T00:00:00Z', 'reason': 'canonical installation', 'failed_shas': []})
    update.write_json_durable(stage / 'prepared.json', {'source_commit': NEW})
    for name in ('manifest.json', 'manifest.sig', 'packet.zip'):
        (stage / name).write_bytes(b'isolated archive adapter input')

    def installed_proof(_control, mode, *, code_root=None):
        if mode == 'Verify':
            detail = onboarding.verify_bootstrap_integrity(SimpleNamespace(app_root=code_root, bootstrap_integrity_path=code_root / 'bootstrap-integrity.json'), required=True)
            assert detail['status'] == 'PASS'
        return ''

    def manifest(bundle, **_kwargs):
        return update.read_json(bundle / 'portable-manifest.json')

    def extract(_packet, output, _policy):
        shutil.copytree(seed.support, output / 'Label_Match')

    def processes(_control):
        state = update.optional_record(control / 'transition.json')
        if (not state or state['phase'] == 'PREPARED') or (state.get('phase') == 'DEFERRED' and (not state.get('candidate_sha'))):
            return []
        assert state['phase'] == 'APPLIED_PENDING_READY'
        update.write_json_durable(stage / 'trial.json', {'nonce': state['nonce'], 'transition_id': state['transition_id'], 'pid': 42})
        update.write_json_durable(stage / 'ready.json', {**{check: True for check in update.READY_CHECKS}, 'nonce': state['nonce'], 'transition_id': state['transition_id'], 'sha': NEW, 'pid': 42, 'ready_at': LATER})
        return [dict(pid=pid, observable=True, sha=NEW, sid=seed.subject, role=role, ordinary_user=True, created_at=LATER) for pid, role in ((42, 'gui'), (43, 'relay'))]
    monkeypatch.setattr(update, 'update_lock', lambda *_a, **_k: nullcontext())
    monkeypatch.setattr(update, 'fixed_helper', installed_proof)
    monkeypatch.setattr(update, 'version_manifest', manifest)
    monkeypatch.setattr(update, 'verify_prepared_packet', lambda *_a, **_k: ({**signed(manifest(old), manifest(seed.support)), 'source_commit': NEW}, {'archive': {}, 'sha256': '3' * 64}))
    monkeypatch.setattr(update, 'copy_prepared_file', lambda source, target, **_k: target.write_bytes(source.read_bytes()))
    monkeypatch.setattr(update, 'extract_prepared_code', extract)
    monkeypatch.setattr(update, 'process_snapshot', processes)
    monkeypatch.setattr(update, 'now_utc', lambda: LATER)
    monkeypatch.setattr(guard, 'CURRENT_VERSION', 'v2.0.98')
    guard.release_identity(fixed)
    guard.record_user_data_open(fixed, [seed.paths.data_root], environ=seed.environment)
    assert update.apply_prepared(control) == 0
    assert update.read_json(control / 'active.json')['sha'] == NEW
    assert update.read_json(control / 'transition.json')['phase'] == 'CONFIRMED'
    assert update.read_json(fixed / 'portable-manifest.json')['source_commit'] == OLD
    candidate_record.unlink()
    monkeypatch.setattr(guard, '__file__', str(active / 'app/label_data_generation.py'))
    guard._RELEASE_IDENTITY_PASS.clear()
    monkeypatch.setattr(guard, 'CURRENT_VERSION', 'v2.0.99')
    guard.release_identity(active)
    guard.record_user_data_open(active, [seed.paths.data_root], environ=seed.environment)
    markers = guard.marker_paths([seed.paths.data_root], seed.subject)
    assert all((json.loads(path.read_bytes())['data_compatible_min_version'] == 'v2.0.99' for path in markers))

    def remove(packet=seed.support):
        monkeypatch.setattr(guard, '__file__', str(packet / 'app/label_data_generation.py'))
        monkeypatch.setattr(guard, 'CURRENT_VERSION', update.read_json(packet / 'portable-manifest.json')['version'])
        guard._RELEASE_IDENTITY_PASS.clear()
        if packet == fixed:
            guard.release_identity(fixed)
        return _remove(fixed, seed.environment, seed.observed)
    return SimpleNamespace(root=managed, fixed=fixed, active=active, support=seed.support, old_support=old_support, candidate=candidate, control=control, markers=markers, seed=seed, remove=remove)

def test_b_support_packet_removes_after_daily_update(updated_packets):
    p = updated_packets
    before = {path: path.read_bytes() for path in p.markers}
    event = p.seed.event.read_bytes()
    assert p.remove()['status'] == 'PASS_DATA_PRESERVED'
    assert p.seed.event.read_bytes() == event
    assert all((path.read_bytes() == raw for path, raw in before.items()))

def test_a_support_packet_cannot_remove_b_generation(updated_packets):
    p = updated_packets
    with pytest.raises(onboarding.CurrentUserOnboardingError) as caught:
        p.remove(p.old_support)
    assert str(caught.value.__cause__) == guard.UNKNOWN_MESSAGE
    assert not p.seed.observed

def test_fixed_a_host_still_refuses_b_data(updated_packets):
    p = updated_packets
    with pytest.raises(onboarding.CurrentUserOnboardingError) as caught:
        p.remove(p.fixed)
    assert str(caught.value.__cause__) != guard.UNKNOWN_MESSAGE
    assert not p.seed.observed

def test_active_b_host_removes_with_fixed_a_logical_root(updated_packets):
    assert updated_packets.remove(updated_packets.active)['status'] == 'PASS_DATA_PRESERVED'

@pytest.mark.parametrize('shell', ['powershell.exe', 'pwsh.exe'])
def test_reinstall_selects_verified_active_b_host(tmp_path, shell, updated_packets):
    p = updated_packets
    installed_verified = True
    repo = Path(__file__).resolve().parents[1]
    text = (repo / 'INSTALL_CANONICAL_PORTABLE.ps1').read_text(encoding='utf-8-sig')
    anchor = text.index('    $removalRoot = if ($existingVerified)')
    start = text.rfind('    if (-not $pristineInstall', 0, anchor)
    end = text.index('    $unquiesced = @(UnquiescedProductWriters)', anchor)
    block = text[start:end]
    report = tmp_path / 'removal.json'
    report.write_text(json.dumps({'status': 'PASS_DATA_PRESERVED', 'relay_process': {'status': 'ABSENT'}}), encoding='utf-8')
    script = tmp_path / 'removal-branch.ps1'
    script.write_text("\n$ErrorActionPreference='Stop'\n$pristineInstall=$false; $legacyFirstRegistration=$false; $healthyWithoutCode=$false\n$healthyLifecycle=$null; $healthyRemovedInstall=$false\n$old=@(); $source='SUPPORT_PACKET_ROOT'; $install='FIXED_PACKET_ROOT'; $lifecycleInstalled='ACTIVE_PACKET_ROOT'\n$existingVerified=VERIFIED\n$writerFenceControlRoot=''; $writerSessionId=''; $writerAttemptId=''; $writerTransactionId=''\n$removalPath='REPORT'\nfunction Product([string]$Root,[string]$Mode,[string[]]$ExtraArguments=@(),[string]$LogicalAppRoot='') {\n    if (-not $LogicalAppRoot) {$LogicalAppRoot=$Root}\n    $script:called=@{execution=$Root;logical=$LogicalAppRoot;mode=$Mode}\n}\nfunction Snapshot {return @{exists=$false}}\nfunction Get-LabelCurrentUserScheduledTasks {return @()}\nfunction Get-LabelOfficialLegacyTaskSnapshot {return $null}\nfunction Wait-LabelRelayPreimageExit {return}\nfunction Set-LabelWriterFenceInstalledTree {return $null}\nBLOCK\n$script:called | ConvertTo-Json -Compress\n".replace('VERIFIED', '$true' if installed_verified else '$false').replace('REPORT', str(report).replace("'", "''")).replace('BLOCK', block).replace('SUPPORT_PACKET_ROOT', str(p.support).replace("'", "''")).replace('FIXED_PACKET_ROOT', str(p.fixed).replace("'", "''")).replace('ACTIVE_PACKET_ROOT', str(p.active).replace("'", "''")), encoding='utf-8-sig')
    completed = subprocess.run([shell, '-NoLogo', '-NoProfile', '-NonInteractive', '-ExecutionPolicy', 'Bypass', '-File', str(script)], capture_output=True, creationflags=subprocess.CREATE_NO_WINDOW)
    result = json.loads(completed.stdout.decode('utf-8-sig'))
    assert completed.returncode == 0
    dispatch = json.loads(completed.stdout.decode('utf-8-sig'))
    assert dispatch['logical'] == str(p.fixed)
    assert dispatch['execution'] == str(p.active)
    assert p.remove(Path(dispatch['execution']))['status'] == 'PASS_DATA_PRESERVED'



@pytest.mark.parametrize('case', ['pointer', 'partial_control', 'state', 'confirmed_sha',
    'rolled_back_sha', 'active_record', 'active_code', 'source_code', 'source_manifest',
    'trust_key', 'control_changed', 'external_record'])
def test_updated_removal_rejects_forged_or_partial_proof(tmp_path, updated_packets, monkeypatch, case):
    p = updated_packets
    event = p.seed.event.read_bytes()
    before = {path: path.read_bytes() for path in p.markers}
    if case == 'pointer':
        path = p.control / 'active.json'
        value = update.read_json(path)
        value['sha'] = 'd' * 40
        path.write_bytes(json.dumps(value).encode('utf-8'))
    elif case == 'partial_control':
        (p.control / 'policy.json').unlink()
    elif case in {'state', 'confirmed_sha', 'rolled_back_sha'}:
        path = p.control / 'transition.json'
        value = update.read_json(path)
        if case == 'state':
            value['phase'] = 'APPLIED_PENDING_READY'
        elif case == 'confirmed_sha':
            value['candidate_sha'] = 'a' * 40
        else:
            value.update(phase='ROLLED_BACK', previous_sha='b' * 40)
        path.write_bytes(json.dumps(value).encode('utf-8'))
    elif case in {'active_record', 'active_code', 'source_code'}:
        target = (p.active / 'bootstrap-integrity.json' if case == 'active_record' else
            (p.active if case == 'active_code' else p.support) / 'app/main.py')
        target.write_bytes(target.read_bytes() + b'\nchanged')
    elif case == 'source_manifest':
        (p.support / 'portable-manifest.json').write_bytes((p.fixed / 'portable-manifest.json').read_bytes())
    elif case == 'trust_key':
        path = p.active / 'app/update-manifest-key-config.json'
        path.write_bytes(path.read_bytes() + b'\n')
        _record_installed_packet(tmp_path, p.active)
    elif case == 'external_record':
        _record_installed_packet(tmp_path, p.support)
    else:
        import label_exact_clone_resolution as clone
        original = clone.portable_inventory_binding
        def inventory(root, *args, **kwargs):
            result = original(root, *args, **kwargs)
            if root == p.active:
                path = p.control / 'active.json'
                value = update.read_json(path)
                value['sha'] = 'a' * 40
                path.write_bytes(json.dumps(value).encode('utf-8'))
            return result
        monkeypatch.setattr(clone, 'portable_inventory_binding', inventory)
    with pytest.raises(onboarding.CurrentUserOnboardingError) as caught:
        p.remove()
    assert isinstance(caught.value.__cause__, guard.DataGenerationError)
    assert str(caught.value.__cause__) == guard.UNKNOWN_MESSAGE
    assert not p.seed.observed
    assert p.seed.event.read_bytes() == event
    assert all(path.read_bytes() == raw for path, raw in before.items())
