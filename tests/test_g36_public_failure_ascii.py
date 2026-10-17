"""Run each shipped public emission with the shared G36 vectors, without installation."""
import json
from pathlib import Path
import re
import shutil
import subprocess

import pytest

ROOT = Path(__file__).resolve().parents[1]
VECTORS = json.loads((ROOT / 'tests/fixtures/g36_public_line_vectors.json').read_text(encoding='utf-8'))['vectors']
PRODUCERS = [(relative, index) for relative, count in (
    ('INSTALL_CANONICAL_PORTABLE.ps1', 3), ('INSTALL_THIS_PC.ps1', 1),
    ('tools/apply_prepared_update.ps1', 1), ('install_label_match_direct_sync.ps1', 1)) for index in range(count)]


@pytest.mark.parametrize('engine', ['powershell', 'pwsh'])
@pytest.mark.parametrize('codepage', [949, 65001])
@pytest.mark.parametrize('relative,index', PRODUCERS)
def test_public_failure_emission_matches_g36_vectors(tmp_path, engine, codepage, relative, index):
    executable = shutil.which(engine)
    assert executable, 'required PowerShell engine absent'
    source = (ROOT / relative).read_text(encoding='utf-8-sig')
    # Extract the actual emission block. No installer, ACL, credential or native path runs.
    blocks = re.findall(r"(?m)^([ \t]*(?:\$publicJson[^\r\n]*\r?\n[ \t]*\$publicAscii[^\r\n]*\r?\n)?[ \t]*\[Console\]::Error\.WriteLine\('LABEL_MATCH_INSTALLER_FAILURE='[^\r\n]*)", source)
    assert len(blocks) == sum(r == relative for r, _ in PRODUCERS)
    data = tmp_path / 'vectors.json'
    data.write_text(json.dumps(VECTORS, ensure_ascii=True), encoding='ascii')
    script = tmp_path / 'emit.ps1'
    script.write_text(r'''
param([string]$VectorPath,[int]$CodePage)
$ErrorActionPreference='Stop'
[Console]::OutputEncoding=[Text.Encoding]::GetEncoding($CodePage)
function ConvertTo-KmtechPublicRecord($Value){return $Value}
foreach($vector in (Get-Content -LiteralPath $VectorPath -Raw -Encoding UTF8|ConvertFrom-Json)){
 $record=$publicFailure=$publicFields=$public=[ordered]@{guidance=[string]$vector.text}
''' + blocks[index] + '\n}\n', encoding='utf-8-sig')
    completed = subprocess.run([executable, '-NoProfile', '-NonInteractive', '-ExecutionPolicy', 'Bypass',
        '-File', str(script), str(data), str(codepage)], capture_output=True,
        creationflags=subprocess.CREATE_NO_WINDOW)
    assert completed.returncode == 0, 'isolated emission failed; values withheld'
    expected = b''.join(('LABEL_MATCH_INSTALLER_FAILURE={"guidance":' + v['expected_json_string_ascii'] + '}\r\n').encode('ascii') for v in VECTORS)
    assert completed.stderr == expected, 'G36 bytes differ; values withheld'
    assert all(0x20 <= b <= 0x7e for line in completed.stderr.split(b'\r\n') for b in line)
    assert [json.loads(line.split(b'=', 1)[1])['guidance'] for line in completed.stderr.splitlines()] == [v['text'] for v in VECTORS]
