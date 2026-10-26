"""Independent preservation checks; disposable input and real removal functions."""
import hashlib
import json
from pathlib import Path
import uuid
import random

import pytest

import auto_update as update
from tests.test_supported_removal_convergence import _owned_artifact, _run_removal, _write_inventory as _inventory
from tests.test_supported_removal_convergence import _r5_junction


@pytest.fixture
def tmp_path(tmp_path_factory):
    # Keep routine NTFS probes below MAX_PATH; long-path coverage is separate.
    root = tmp_path_factory.getbasetemp().parent / "p" / uuid.uuid4().hex[:6]
    root.mkdir(parents=True)
    return root


def _io(path):
    return Path('\\\\?\\' + str(path))


@pytest.mark.parametrize("slot", ["current", "version", "incoming"])
@pytest.mark.parametrize("extra_unknown", [False, True])
def test_changed_manifest_listed_file_is_preserved_at_original_path(tmp_path, slot, extra_unknown):
    root = tmp_path / "r"
    artifact, owned = _owned_artifact(root, slot)
    if slot != "incoming":
        _inventory(artifact, owned)
        sentinel = artifact / "app/main.py"
    else:
        update.write_json_durable(artifact / "manifest.json", {
            "app_id": "Label_Match", "packet_sha256": hashlib.sha256(owned[artifact / 'packet.zip']).hexdigest(),
            "packet_size": len(owned[artifact / 'packet.zip']), "source_commit": '2' * 40})
        sentinel = artifact / "packet.zip"
    raw = b"unique-user-registration;quantity=7;pending=UNKNOWN"
    sentinel.write_bytes(raw)
    if extra_unknown:
        (artifact / "other-user-file").write_bytes(b"must-also-stay")
    response = _run_removal(root, tmp_path, install=False)
    assert response["status"] == "PASS"
    assert sentinel.is_file() and sentinel.read_bytes() == raw, "name-only match relocated changed user content"


@pytest.mark.parametrize("slot", ["current", "version", "incoming"])
def test_readable_inventory_metadata_is_archived_under_explicit_p3_exception(tmp_path, slot):
    root = tmp_path / "r"
    artifact, owned = _owned_artifact(root, slot)
    if slot != "incoming":
        _inventory(artifact, owned)
    sentinel = artifact / ("manifest.json" if slot == "incoming" else "bootstrap-integrity.json")
    raw = sentinel.read_bytes()
    assert _run_removal(root, tmp_path, install=False)["status"] == "PASS"
    assert not sentinel.exists()
    archives = (root / 'removal-evidence').glob('*/' + sentinel.relative_to(root).as_posix())
    assert any(p.read_bytes() == raw for p in archives), 'metadata exception must preserve the original bytes as evidence'


@pytest.mark.parametrize("slot", ["current", "version", "incoming"])
def test_unreadable_inventory_preserves_material_and_external_junction(tmp_path, slot):
    root = tmp_path / "r"
    artifact, owned = _owned_artifact(root, slot)
    if slot == "incoming":
        (artifact / "manifest.json").write_bytes(b'{"app_id":')
    else:
        (artifact / "bootstrap-integrity.json").write_bytes(b'{"files":')
    legacy = artifact / "u"
    legacy.mkdir()
    sentinel = legacy / "pending"
    raw = b"unique-registered-quantity-and-pending"
    sentinel.write_bytes(raw)
    alias = tmp_path / "registered-data-alias"
    _r5_junction(alias, legacy, tmp_path)
    assert alias.resolve() == legacy.resolve()
    assert _run_removal(root, tmp_path, install=False)["status"] == "PASS"
    assert sentinel.is_file() and sentinel.read_bytes() == raw, "lexical overlap check missed resolved registered root"
    assert alias.is_junction() and alias.resolve() == legacy.resolve()


@pytest.mark.parametrize("slot", ["current", "version", "incoming"])
def test_readonly_unknown_data_is_unchanged(tmp_path, slot):
    import os
    import stat
    root = tmp_path / "r"
    artifact, owned = _owned_artifact(root, slot)
    if slot != "incoming":
        _inventory(artifact, owned)
    sentinel = artifact / "user-pending"
    raw = b"readonly-quantity-and-pending"
    sentinel.write_bytes(raw)
    os.chmod(sentinel, stat.S_IREAD)
    assert _run_removal(root, tmp_path, install=False)["status"] == "PASS"
    assert sentinel.read_bytes() == raw and sentinel.stat().st_file_attributes & 1


def test_partial_incoming_removal_resume_preserves_unregistered_unknown_file(tmp_path):
    root = tmp_path / "r"
    artifact, owned = _owned_artifact(root, "incoming")
    sentinel = artifact / "unregistered-pending"
    raw = b"unique-user-quantity=7;pending=UNKNOWN"
    sentinel.write_bytes(raw)
    response = _run_removal(root, tmp_path, install=False, cut="manifest.json")
    assert response["status"] == "PASS" and response["interrupted"]
    assert sentinel.is_file() and sentinel.read_bytes() == raw, "resume reclassified unknown staging data after manifest retirement"


def test_custom_packaging_data_inside_code_is_preserved_without_discovery(tmp_path):
    root = tmp_path / 'r'
    artifact, owned = _owned_artifact(root, 'current')
    custom = artifact / 'custom-packaging-data/pending'
    custom.parent.mkdir(); custom.write_bytes(b'quantity=7;pending=UNKNOWN')
    settings = artifact / 'config/app_settings.json'
    settings.parent.mkdir(); settings.write_text(json.dumps({'custom_save_path': str(custom.parent)}))
    before = {p: p.read_bytes() for p in (custom, settings)}
    assert _run_removal(root, tmp_path, install=False)['status'] == 'PASS'
    assert all(p.read_bytes() == raw for p, raw in before.items())


@pytest.mark.parametrize('slot', ['current', 'version'])
@pytest.mark.parametrize('seed', range(8))
def test_only_matching_manifest_files_move_amid_arbitrary_material(tmp_path, slot, seed):
    root = tmp_path / 'r'
    artifact, owned = _owned_artifact(root, slot)
    rng = random.Random(seed)
    for index in range(4):
        path = artifact / ('m' * 65) / ('n' * 65) / f'module-{index}.py'
        _io(path.parent).mkdir(parents=True, exist_ok=True)
        _io(path).write_bytes(f'owned-{index}'.encode())
        owned[path] = _io(path).read_bytes()
    _inventory(artifact, owned)
    changed = rng.choice([p for p in owned if p.name not in {'portable-manifest.json'}])
    _io(changed).write_bytes(b'quantity=7;pending=UNKNOWN;same-name-different-content')
    unknown = {changed: _io(changed).read_bytes()}
    for index in range(8):
        base = rng.choice([artifact, root, tmp_path / 'outside', root / 'update-control/u', root / 'versions/u'])
        path = base / ('x' * rng.choice([3, 67])) / f'user-{index}'
        _io(path.parent).mkdir(parents=True, exist_ok=True)
        _io(path).write_bytes(f'quantity={index};pending=UNKNOWN;registration=original'.encode())
        unknown[path] = _io(path).read_bytes()
    target = tmp_path / 'junction-target'
    target.mkdir()
    sentinel = target / 'main.py'
    sentinel.write_bytes(b'junction-user-material')
    unknown[sentinel] = sentinel.read_bytes()
    redirect = artifact / 'redirect'
    _r5_junction(redirect, target, tmp_path)
    # Even a matching manifest entry cannot authorize a reparse component.
    owned[redirect / 'main.py'] = sentinel.read_bytes()
    _inventory(artifact, {p: (raw if p == changed else _io(p).read_bytes()) for p, raw in owned.items()})
    assert _run_removal(root, tmp_path, install=False)['status'] == 'PASS'
    assert all(_io(p).is_file() and _io(p).read_bytes() == raw for p, raw in unknown.items())
    assert redirect.is_junction() and redirect.resolve() == target.resolve()
    for path in owned:
        if path not in {changed, redirect / 'main.py'}:
            assert not _io(path).exists(), f'matching owned file left behind: {path}'
    assert update.read_json(root / 'removed.json')['lifecycle_state'] == 'REMOVED'


@pytest.mark.parametrize('cut', ['main.py', 'pythonw.exe', 'portable-manifest.json'])
def test_restart_between_owned_file_moves_keeps_other_bytes(tmp_path, cut):
    root = tmp_path / 'r'
    artifact, owned = _owned_artifact(root, 'current')
    _inventory(artifact, owned)
    sentinel = artifact / 'user-pending'
    sentinel.write_bytes(b'quantity=7;pending=UNKNOWN')
    result = _run_removal(root, tmp_path, install=False, cut=cut)
    assert result['interrupted'] and result['status'] == 'PASS'
    assert sentinel.read_bytes() == b'quantity=7;pending=UNKNOWN'
    assert all(not path.exists() for path in owned)


def test_preexisting_empty_directory_is_not_deleted_for_an_absent_listed_file(tmp_path):
    root = tmp_path / 'r'
    artifact, owned = _owned_artifact(root, 'current')
    empty = artifact / 'never-owned'
    empty.mkdir()
    _inventory(artifact, {**owned, empty / 'missing.py': b'absent-declared-content'})
    assert _run_removal(root, tmp_path, install=False)['status'] == 'PASS'
    assert empty.is_dir() and not list(empty.iterdir())


@pytest.mark.parametrize('cut', ['', 'main.py', 'pythonw.exe', 'portable-manifest.json',
                                  'metadata-before', 'bootstrap-integrity.json'])
def test_normal_current_removal_then_install_after_every_file_and_metadata_cut(tmp_path, monkeypatch, cut):
    from tests.test_supported_removal_convergence import _new_stock
    root = tmp_path / 'r'
    _, owned = _owned_artifact(root, 'current')
    result = _run_removal(root, tmp_path, install=False, cut=cut)
    assert result['status'] == 'PASS' and result['interrupted'] is bool(cut)
    assert not (root / 'current').exists()
    monkeypatch.setattr(update, 'lifecycle_task_observation', lambda *args: {'status': 'PASS', 'task_exists': False})
    assert update.resolve_lifecycle_context(root, operation='install')['lifecycle_state'] == 'REMOVED'
    _new_stock(root)
    assert _run_removal(root, tmp_path)['installed']
    assert all(any(p.read_bytes() == raw for p in
        (root / 'removal-evidence').glob('*/' + path.relative_to(root).as_posix())) for path, raw in owned.items())


def test_current_with_unlisted_material_blocks_install_after_metadata_retirement(tmp_path, monkeypatch):
    root = tmp_path / 'r'
    artifact, _ = _owned_artifact(root, 'current')
    sentinel = artifact / 'pending'
    sentinel.write_bytes(b'quantity=7;pending=UNKNOWN')
    assert _run_removal(root, tmp_path, install=False)['status'] == 'PASS'
    monkeypatch.setattr(update, 'lifecycle_task_observation', lambda *args: pytest.fail('must stop before native'))
    with pytest.raises(ValueError, match='설치가 만드는 경로.*current') as refusal:
        update.resolve_lifecycle_context(root, operation='install')
    assert '\n' not in str(refusal.value)
    assert sentinel.read_bytes() == b'quantity=7;pending=UNKNOWN'


def test_metadata_changed_after_planning_stays_in_single_removal(tmp_path, monkeypatch):
    from tests import test_supported_removal_convergence as helper
    root = tmp_path / 'r'
    artifact, _ = _owned_artifact(root, 'current')
    inventory = artifact / 'bootstrap-integrity.json'
    expected = inventory.read_bytes() + b' '
    script = helper.REMOVAL_SCRIPT.replace(
        '    & $actualAtomicReplace @PSBoundParameters',
        '''    & $actualAtomicReplace @PSBoundParameters
    if ((Split-Path -Leaf $Source) -ceq 'main.py') {
        [IO.File]::AppendAllText((Join-Path $Root 'current/bootstrap-integrity.json'),' ')
    }''')
    # This case measures the before-move recheck in one real invocation.
    # A later independent attempt can accept the newly read metadata snapshot.
    script = script.replace('[void](& $invokeRemoval)\n[void](& $invokeRemoval)', '')
    start = script.index("if ((Get-LabelRemovalArtifacts $Root).artifacts")
    end = script.index("if ($Install -eq 'True')", start)
    script = script[:start] + script[end:]
    monkeypatch.setattr(helper, 'REMOVAL_SCRIPT', script)
    assert _run_removal(root, tmp_path, install=False)['status'] == 'PASS'
    assert inventory.read_bytes() == expected
    marker = update.read_json(root / 'removed.json')
    assert marker['lifecycle_state'] == 'REMOVED'
    assert any(d['disposition'] == 'METADATA_CHANGED_RETAINED' for d in marker['artifact_dispositions'])


@pytest.mark.parametrize('slot', ['current', 'version', 'incoming'])
@pytest.mark.parametrize('share', ['Read', 'None'])
def test_busy_owned_file_keeps_inventory_and_completion_pending(tmp_path, monkeypatch, slot, share):
    from tests import test_supported_removal_convergence as helper
    root = tmp_path / 'r'
    artifact, owned = _owned_artifact(root, slot)
    locked = artifact / ('packet.zip' if slot == 'incoming' else 'app/main.py')
    inventory = artifact / ('manifest.json' if slot == 'incoming' else 'bootstrap-integrity.json')
    script = helper.REMOVAL_SCRIPT[:helper.REMOVAL_SCRIPT.index('$current=Join-Path $Root')] + r'''
$lockedPath=Join-Path $Root '__LOCKED__'
$inventoryPath=Join-Path $Root '__INVENTORY__'
$inventoryBefore=[IO.File]::ReadAllText($inventoryPath)
$stream=[IO.File]::Open($lockedPath,[IO.FileMode]::Open,[IO.FileAccess]::Read,[IO.FileShare]::__SHARE__)
$firstError=''
try {
    try { [void](& $invokeRemoval) } catch { $firstError=$_.Exception.Message }
    $firstMarker=Test-Path -LiteralPath (Join-Path $Root 'removed.json')
    $firstInventory=Test-Path -LiteralPath $inventoryPath
    $inventorySame=$firstInventory -and [IO.File]::ReadAllText($inventoryPath) -ceq $inventoryBefore
    $lockedRetained=Test-Path -LiteralPath $lockedPath
} finally { $stream.Dispose() }
[void](& $invokeRemoval)
[void](& $invokeRemoval)
@{status='PASS';first_error=$firstError;first_marker=$firstMarker;first_inventory=$firstInventory;
 inventory_same=$inventorySame;locked_retained=$lockedRetained} | ConvertTo-Json -Compress
'''
    script = script.replace('__LOCKED__', locked.relative_to(root).as_posix())
    script = script.replace('__INVENTORY__', inventory.relative_to(root).as_posix()).replace('__SHARE__', share)
    monkeypatch.setattr(helper, 'REMOVAL_SCRIPT', script)
    result = _run_removal(root, tmp_path, install=False)
    assert result['first_error'] and not result['first_marker']
    assert result['locked_retained'] and result['first_inventory'] and result['inventory_same']
    if share == 'None':
        assert 'REMOVAL_FILE_OBSERVATION_UNKNOWN' in result['first_error']
        assert '제거 미완' in result['first_error'] and str(locked) in result['first_error']
        assert '\n' not in result['first_error']
    assert all(not path.exists() for path in owned)
    assert update.read_json(root / 'removed.json')['lifecycle_state'] == 'REMOVED'


@pytest.mark.parametrize('slot', ['current', 'version', 'incoming'])
def test_hash_becomes_unreadable_after_planning_keeps_retry_inventory(tmp_path, monkeypatch, slot):
    from tests import test_supported_removal_convergence as helper
    root = tmp_path / 'r'
    artifact, owned = _owned_artifact(root, slot)
    inventory = artifact / ('manifest.json' if slot == 'incoming' else 'bootstrap-integrity.json')
    locked = inventory
    raw = inventory.read_bytes()
    target = '    & $actualAtomicReplace @PSBoundParameters'
    assert helper.REMOVAL_SCRIPT.count(target) == 1
    script = helper.REMOVAL_SCRIPT.replace(
        target,
        '''    & $actualAtomicReplace @PSBoundParameters
    if ($null -eq $script:lateStream -and (Split-Path -Leaf $Source) -cin @('main.py','pythonw.exe','packet.zip')) {
        $script:lateStream=[IO.File]::Open((Join-Path $Root '__LOCKED__'),[IO.FileMode]::Open,[IO.FileAccess]::Read,[IO.FileShare]::None)
    }''')
    script = script[:script.index('$current=Join-Path $Root')] + r'''
$script:lateStream=$null;$firstError='';$firstCauses=@()
try { [void](& $invokeRemoval) } catch {
    $firstError=$_.Exception.Message
    for($cause=$_.Exception;$null -ne $cause;$cause=$cause.InnerException){$firstCauses+=$cause.Message}
}
$firstMarker=Test-Path -LiteralPath (Join-Path $Root 'removed.json')
$firstInventory=Test-Path -LiteralPath (Join-Path $Root '__INVENTORY__')
if ($null -ne $script:lateStream) { $script:lateStream.Dispose() }
@{status='PASS';first_error=$firstError;first_causes=$firstCauses;first_marker=$firstMarker;first_inventory=$firstInventory} | ConvertTo-Json -Compress
'''
    script = script.replace('__LOCKED__', locked.relative_to(root).as_posix())
    script = script.replace('__INVENTORY__', inventory.relative_to(root).as_posix())
    monkeypatch.setattr(helper, 'REMOVAL_SCRIPT', script)
    result = _run_removal(root, tmp_path, install=False)
    assert 'REMOVAL_FILE_OBSERVATION_UNKNOWN' in result['first_error']
    assert not result['first_marker'] and result['first_inventory']
    assert inventory.read_bytes() == raw and locked.exists()
    monkeypatch.undo()
    assert _run_removal(root, tmp_path, install=False)['status'] == 'PASS'
    assert all(not path.exists() for path in owned)


@pytest.mark.parametrize('slot', ['current', 'version'])
def test_readonly_emptied_owned_parent_removes_without_manual_retry(tmp_path, slot):
    import stat
    root = tmp_path / 'r'
    artifact, owned = _owned_artifact(root, slot)
    parent = artifact / 'app'
    parent.chmod(stat.S_IREAD)
    assert _run_removal(root, tmp_path, install=False)['status'] == 'PASS'
    assert all(not path.exists() for path in owned)
    assert not parent.exists() and not artifact.exists()
    assert update.read_json(root / 'removed.json')['lifecycle_state'] == 'REMOVED'


@pytest.mark.parametrize('slot', ['current', 'version'])
@pytest.mark.parametrize('material', ['empty', 'nonempty'])
def test_readonly_foreign_parent_attributes_and_material_stay(tmp_path, slot, material):
    import stat
    root = tmp_path / 'r'
    artifact, owned = _owned_artifact(root, slot)
    if material == 'nonempty':
        foreign = artifact / 'app'
        sentinel = foreign / 'user-pending'
        sentinel.write_bytes(b'quantity=7;pending=UNKNOWN')
    else:
        foreign = artifact / 'foreign-empty'
        foreign.mkdir()
        _inventory(artifact, {**owned, foreign / 'missing.py': b'never-present'})
        sentinel = None
    foreign.chmod(stat.S_IREAD)
    attributes = foreign.stat().st_file_attributes
    assert _run_removal(root, tmp_path, install=False)['status'] == 'PASS'
    assert foreign.is_dir() and foreign.stat().st_file_attributes == attributes
    if sentinel:
        assert sentinel.read_bytes() == b'quantity=7;pending=UNKNOWN'
    else:
        assert not list(foreign.iterdir())


@pytest.mark.parametrize('slot', ['current', 'version'])
def test_empty_owned_parent_cleanup_refusal_reports_retained_without_failing(tmp_path, monkeypatch, slot):
    from tests import test_supported_removal_convergence as helper
    root = tmp_path / 'r'
    artifact, owned = _owned_artifact(root, slot)
    parent = artifact / 'app'
    script = helper.REMOVAL_SCRIPT.replace(
        "$current=Join-Path $Root", r'''
function Remove-Item {
    [CmdletBinding()]param([string]$LiteralPath)
    if ((Split-Path -Leaf $LiteralPath) -ceq 'app') { throw 'injected empty-directory cleanup refusal' }
    Microsoft.PowerShell.Management\Remove-Item @PSBoundParameters
}
$current=Join-Path $Root''')
    monkeypatch.setattr(helper, 'REMOVAL_SCRIPT', script)
    result = _run_removal(root, tmp_path, install=False)
    assert result['status'] == 'PASS'
    assert parent.is_dir() and not list(parent.iterdir())
    assert all(not path.exists() for path in owned)
    reports = [update.read_json(root / 'removed.json')] + [
        update.read_json(path) for path in (root / 'removal-evidence').glob('*/removed.json')]
    assert all(report['lifecycle_state'] == 'REMOVED' for report in reports)
    assert any(str(parent) in report['retained_paths'] and
               any(row['path'] == str(parent) and row['disposition'] == 'EMPTY_OWNED_PARENT_RETAINED'
                   for row in report['artifact_dispositions']) for report in reports)
