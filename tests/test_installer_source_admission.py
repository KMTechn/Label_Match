"""Canonical's source inventory admits fresh stock before installation records exist."""
import json
import os
from pathlib import Path

import pytest

import label_data_generation as generation
from label_exact_clone_resolution import portable_inventory_binding, ExactCloneResolutionError

ROOT = Path(__file__).resolve().parents[1]


@pytest.fixture
def packet(tmp_path):
    root = tmp_path / 'source'
    for relative, raw in {
        'portable-manifest.json': json.dumps({'schema': 'label-match-portable-tree-v1',
            'version': generation.CURRENT_VERSION, 'source_commit': 'a' * 40}).encode(),
        'app/label_data_generation.py': ('CURRENT_VERSION = "' + generation.CURRENT_VERSION + '"\n').encode(),
        'app/main.py': b'# isolated stock entry\n',
        'runtime/pythonw.exe': b'isolated runtime',
        'INSTALL_THIS_PC.ps1': b'# placement',
        'tools/bootstrap_integrity.ps1': b'# integrity',
        'tools/label_writer_fence.ps1': b'# fence',
    }.items():
        path = root / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(raw)
    return root


def test_fresh_source_is_bound_without_an_installation_record(packet):
    before = portable_inventory_binding(packet)
    identity = generation._installer_source_identity(packet, source_inventory_sha256=before['sha256'])
    assert identity == {'version': generation.CURRENT_VERSION, 'build_id': 'a' * 40,
        'data_compatible_min_version': generation.CURRENT_VERSION, 'compatibility_verified': False}
    assert not (packet / 'bootstrap-integrity.json').exists()
    assert portable_inventory_binding(packet) == before
    # Installed startup must still require the later installation record.
    with pytest.raises(generation.DataGenerationError):
        generation.release_identity(packet)


@pytest.mark.parametrize('relative', ['portable-manifest.json', 'app/label_data_generation.py',
                                    'runtime/pythonw.exe', 'app/extra.py'])
def test_source_changes_after_canonical_inventory_are_refused(packet, relative):
    pin = portable_inventory_binding(packet)['sha256']
    (packet / relative).write_bytes(b'changed source')
    with pytest.raises(generation.DataGenerationError):
        generation._installer_source_identity(packet, source_inventory_sha256=pin)


@pytest.mark.parametrize('change', ['version', 'commit', 'unsigned_compatibility'])
def test_valid_inventory_does_not_authorize_invalid_release_metadata(packet, change):
    path = packet / 'portable-manifest.json'
    metadata = json.loads(path.read_text())
    if change == 'version':
        metadata['version'] = 'v9.9.9'
    elif change == 'commit':
        metadata['source_commit'] = 'not-a-commit'
    else:
        metadata['data_generation_signed_manifest'] = {'data_compatible_min_version': 'v1.0.0'}
    path.write_text(json.dumps(metadata))
    pin = portable_inventory_binding(packet)['sha256']
    with pytest.raises(generation.DataGenerationError):
        generation._installer_source_identity(packet, source_inventory_sha256=pin)


def test_identity_samples_must_match_the_files_hashed_in_the_inventory(packet):
    with pytest.raises(ExactCloneResolutionError):
        portable_inventory_binding(packet, bound_files={'portable-manifest.json': b'other bytes'})


def test_source_pin_cannot_be_used_for_actual_onboarding(packet, monkeypatch):
    import current_user_onboarding as onboarding
    monkeypatch.setattr(onboarding, 'onboard_current_user', lambda *_args, **_kwargs: pytest.fail('actual onboarding entered'))
    with pytest.raises(SystemExit) as refused:
        onboarding.onboarding_main(['--app-root', str(packet), '--installer-source-inventory-sha256', 'a' * 64])
    assert refused.value.code == 2


@pytest.mark.parametrize('engine', ['powershell.exe', 'pwsh'])
def test_source_digest_matches_canonical_ordinal_unicode_paths(packet, tmp_path, engine):
    from tests.test_shared_ancestor_protection import run_functions
    (packet / '\U0001f600.txt').write_bytes(b'supplementary path')
    (packet / '\ue000.txt').write_bytes(b'private use path')
    result = run_functions(tmp_path, ROOT / 'INSTALL_CANONICAL_PORTABLE.ps1',
        ['PortableInventory', 'Full', 'Sha', 'UInt64BE', 'HexBytes'],
        '. $env:LM_ADMISSION_SHARED\n(PortableInventory $env:LM_ADMISSION_ROOT).sha256',
        engine=engine, values={'LM_ADMISSION_ROOT': str(packet),
                              'LM_ADMISSION_SHARED': str(ROOT / 'kmtech_shared/powershell/portable.ps1')})
    assert result.returncode == 0, result.stderr
    assert result.stdout.strip() == portable_inventory_binding(packet)['sha256']


def test_actual_extracted_stock_uses_the_same_admission_when_selected(packet):
    root = Path(os.environ['LM_TEST_EXTRACTED_STOCK']) if 'LM_TEST_EXTRACTED_STOCK' in os.environ else packet
    before = portable_inventory_binding(root)
    assert not (root / 'bootstrap-integrity.json').exists()
    identity = generation._installer_source_identity(root, source_inventory_sha256=before['sha256'])
    assert identity['version'] == generation.CURRENT_VERSION
    assert len(identity['build_id']) == 40
    assert not (root / 'bootstrap-integrity.json').exists()
    assert portable_inventory_binding(root) == before


def test_nested_credential_owner_uses_the_same_source_inventory(packet, tmp_path, monkeypatch):
    import protected_admin
    from current_user_onboarding import prepare_current_user_data_open
    monkeypatch.setattr(protected_admin, 'current_process_sid', lambda: 'S-1-5-21-101')
    local = tmp_path / 'local'
    environment = {'LOCALAPPDATA': str(local), 'PROGRAMDATA': str(tmp_path / 'program'),
        'LABEL_MATCH_SAVE_DIR': str(tmp_path / 'data')}
    credential = local / 'KMTech/DirectSync/label_match/credential.json'
    credential.parent.mkdir(parents=True)
    secret_root = tmp_path / 'owner'
    credential.write_text(json.dumps({'secret_ref': 'dpapi:isolated', 'secret_data_dir': str(secret_root)}))
    marker = secret_root / 'secrets/data-generation.json'
    from kmtech_shared.data_generation import record_generation
    marker.parent.mkdir(parents=True)
    record_generation(marker, 'v9.9.9', app_id='Label_Match', subject_sid='S-1-5-21-101', build_id='b' * 40)
    before = marker.read_bytes()
    with pytest.raises(Exception) as refused:
        prepare_current_user_data_open(packet, environ=environment, installer_admission=True,
            source_inventory_sha256=portable_inventory_binding(packet)['sha256'])
    assert refused.value.__class__.__name__ == 'CurrentUserOnboardingError'
    assert refused.value.__cause__.__class__.__name__ == 'DataGenerationError'
    assert marker.read_bytes() == before
    assert not (packet / 'bootstrap-integrity.json').exists()


@pytest.mark.parametrize('engine', ['powershell.exe', 'pwsh'])
@pytest.mark.parametrize('refuse', [False, True])
def test_canonical_preparation_result_and_failure_audit(tmp_path, engine, refuse):
    from tests.test_shared_ancestor_protection import run_functions
    result = run_functions(tmp_path, ROOT / 'INSTALL_CANONICAL_PORTABLE.ps1',
        ['Invoke-LabelSourceDataAdmission', 'Save', 'Full'], r'''
. $env:LM_ADMISSION_SHARED
function Get-LabelLaunchFailure { param($Failure,$Phase)
 if($Failure.Exception.Message -cne 'ISOLATED_SOURCE_REFUSED'){throw 'First cause changed.'}
 return [ordered]@{status='FAILED';stage='SUPPORT';reason_code='INSTALLER_FAILED';report_status='REPORT_UNAVAILABLE'}
}
function Product { param($Root,$Mode,$ExtraArguments)
 if($Mode -cne '--onboard-current-user' -or
    $ExtraArguments[0] -cne '--app-root' -or $ExtraArguments[1] -cne $env:LM_ADMISSION_ROOT -or
    $ExtraArguments[2] -cne '--check-data-generation-only' -or
    $ExtraArguments[3] -cne '--installer-source-inventory-sha256' -or
    $ExtraArguments[4] -cne ('a'*64)){throw 'Source admission arguments differ.'}
 if($env:LM_ADMISSION_REFUSE -eq '1'){throw 'ISOLATED_SOURCE_REFUSED'}
}
$auditRoot=Join-Path $env:LM_ADMISSION_ROOT 'audit'
$external=Join-Path $env:LM_ADMISSION_ROOT 'external.json'
try {
 Invoke-LabelSourceDataAdmission $env:LM_ADMISSION_ROOT $env:LM_ADMISSION_ROOT @{sha256=('a'*64)} $auditRoot $external
 if($env:LM_ADMISSION_REFUSE -eq '1'){throw 'Refusal was swallowed.'}
 if(Test-Path -LiteralPath $auditRoot){throw 'Successful read-only admission wrote an audit.'}
 'ADMITTED'
} catch {
 if($env:LM_ADMISSION_REFUSE -ne '1' -or $_.Exception.Message -cne 'ISOLATED_SOURCE_REFUSED'){throw}
 $files=@(Get-ChildItem -LiteralPath $auditRoot -File)
 if($files.Count -ne 1){throw 'Failure audit absent or duplicated.'}
 $audit=Get-Content -LiteralPath $files[0].FullName -Raw | ConvertFrom-Json
 if($audit.status -cne 'FAILED' -or $audit.preparation_status -cne 'FAILED_PREPARATION_ADMISSION' -or
    $audit.rollback.status -cne 'NOT_REQUIRED' -or $audit.rollback.applied){throw 'Failure state differs.'}
 if((Get-FileHash -LiteralPath $files[0].FullName).Hash -cne (Get-FileHash -LiteralPath $external).Hash){throw 'External evidence differs.'}
 'REFUSED'
}
''', engine=engine, values={'LM_ADMISSION_ROOT': str(tmp_path), 'LM_ADMISSION_REFUSE': str(int(refuse)),
                           'LM_ADMISSION_SHARED': str(ROOT / 'kmtech_shared/powershell/portable.ps1')})
    assert result.returncode == 0, result.stderr
    assert result.stdout.count('install_status=FAILED') == int(refuse)
    assert ('REFUSED' if refuse else 'ADMITTED') in result.stdout
