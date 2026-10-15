"""Byte-level distribution checks; no installer or PowerShell source is executed."""
import io
import json
from pathlib import Path
import shutil
import subprocess
import zipfile

import pytest

from tools import build_portable_release_candidate as stock
from tools.check_powershell_source_encoding import assert_distribution_encoding

ROOT = Path(__file__).resolve().parents[1]
PS51 = Path('C:/Windows/System32/WindowsPowerShell/v1.0/powershell.exe')


@pytest.fixture(scope='module')
def distributed_scripts(tmp_path_factory):
    root = tmp_path_factory.mktemp('distributed-powershell')
    stock._copy_application(ROOT, root / 'app')
    names = (stock.CANONICAL_INSTALLER_FILENAME, stock.LEGACY_INSTALLER_FILENAME,
             stock.BOOTSTRAP_INTEGRITY_HELPER, stock.WRITER_FENCE_HELPER,
             stock.WORKER_SHORTCUT_HELPER, 'tools/apply_prepared_update.ps1')
    for name in names:
        destination = root / name
        destination.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(ROOT / name, destination)
    return root


def test_actual_distributed_scripts_require_bom(distributed_scripts):
    assert assert_distribution_encoding(distributed_scripts) >= 6


@pytest.mark.parametrize('depth', [0, 1, 2])
@pytest.mark.parametrize('bom', [True, False])
def test_non_ascii_leaf_bom_is_required_in_directory_zip_and_nested_zip(tmp_path, depth, bom):
    raw = (b'\xef\xbb\xbf' if bom else b'') + "Write-Output '검사'\n".encode('utf-8')
    name = 'leaf.ps1'
    for _ in range(depth):
        buffer = io.BytesIO()
        with zipfile.ZipFile(buffer, 'w') as archive:
            archive.writestr(name, raw)
        name, raw = 'nested.zip', buffer.getvalue()
    (tmp_path / name).write_bytes(raw)
    if bom:
        assert assert_distribution_encoding(tmp_path) == 1
    else:
        with pytest.raises(ValueError, match='POWERSHELL_UTF8_BOM_REQUIRED'):
            assert_distribution_encoding(tmp_path)


def test_actual_distribution_and_nested_archive_have_identical_ps_bytes(tmp_path, distributed_scripts):
    inner = io.BytesIO()
    scripts = [p for p in distributed_scripts.rglob('*') if p.suffix.casefold() in ('.ps1', '.psm1', '.psd1')]
    with zipfile.ZipFile(inner, 'w') as archive:
        for path in scripts:
            archive.writestr(path.relative_to(distributed_scripts).as_posix(), path.read_bytes())
    outer = tmp_path / 'packet.zip'
    with zipfile.ZipFile(outer, 'w') as archive:
        archive.writestr('payload.zip', inner.getvalue())
    assert assert_distribution_encoding(outer) == len(scripts)


def test_ps51_cp949_file_loading_and_explicit_utf8_parse_without_errors(tmp_path, distributed_scripts):
    script = tmp_path / 'parse.ps1'
    result = tmp_path / 'parser.json'
    script.write_text(r'''
param([string]$Root,[string]$Result)
$ErrorActionPreference='Stop'
if($PSVersionTable.PSVersion.Major -ne 5){throw 'PS51_REQUIRED'}
$rows=@(Get-ChildItem -LiteralPath $Root -Recurse -File | Where-Object {$_.Extension -in '.ps1','.psm1','.psd1'} | ForEach-Object {
    $raw=[IO.File]::ReadAllBytes($_.FullName)
    $bom=$raw.Length -ge 3 -and $raw[0] -eq 239 -and $raw[1] -eq 187 -and $raw[2] -eq 191
    $utf8=[Text.Encoding]::UTF8.GetString($raw).TrimStart([char]0xfeff)
    $loaded=if($bom){$utf8}else{[Text.Encoding]::GetEncoding(949).GetString($raw)}
    $tokens=$null;$errors=$null
    $null=[Management.Automation.Language.Parser]::ParseInput($loaded,[ref]$tokens,[ref]$errors)
    $cp949=$errors.Count
    $null=[Management.Automation.Language.Parser]::ParseInput($utf8,[ref]$tokens,[ref]$errors)
    $explicit=$errors.Count
    $null=[Management.Automation.Language.Parser]::ParseFile($_.FullName,[ref]$tokens,[ref]$errors)
    @{cp949=$cp949;utf8=$explicit;file=$errors.Count}
})
@{major=$PSVersionTable.PSVersion.Major;rows=$rows} | ConvertTo-Json -Depth 4 | Set-Content -LiteralPath $Result -Encoding UTF8
''', encoding='utf-8-sig')
    completed = subprocess.run([str(PS51), '-NoProfile', '-NonInteractive', '-File', str(script),
                                '-Root', str(distributed_scripts), '-Result', str(result)],
                               capture_output=True, timeout=60)
    assert completed.returncode == 0
    data = json.loads(result.read_text(encoding='utf-8-sig'))
    assert data['major'] == 5 and len(data['rows']) >= 6
    assert all(row == dict(cp949=0, utf8=0, file=0) for row in data['rows'])
