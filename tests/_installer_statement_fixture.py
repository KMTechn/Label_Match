"""Read the installer's real outer transaction with the PowerShell compiler."""
import hashlib
import json
from pathlib import Path
import shutil
import subprocess

from tests._bounded_powershell import run as run_bounded

_OFFSETS = {}


def top_level_transaction(installer: Path, source: str, scratch: Path) -> str:
    raw = installer.read_bytes().decode('utf-8-sig')
    source_bom = 1 if source.startswith('\ufeff') else 0
    assert raw.replace('\r\n', '\n').replace('\r', '\n') == source[source_bom:]
    identity = hashlib.sha256(raw.encode('utf-8')).hexdigest()
    if identity not in _OFFSETS:
        parser = scratch / 'installer-transaction-ast.ps1'
        parser.write_text('''param([string]$SourcePath)
$ErrorActionPreference='Stop'
$tokens=$null;$parseErrors=$null
$tree=[Management.Automation.Language.Parser]::ParseFile($SourcePath,[ref]$tokens,[ref]$parseErrors)
if(@($parseErrors).Count){throw 'installer syntax is invalid'}
$transactions=@($tree.EndBlock.Statements | Where-Object {$_ -is [Management.Automation.Language.TryStatementAst]})
if($transactions.Count -ne 1){throw 'installer outer transaction is ambiguous'}
@{offset=$transactions[0].Extent.StartOffset} | ConvertTo-Json -Compress
''', encoding='utf-8')
        result = run_bounded([shutil.which('pwsh'), '-NoLogo', '-NoProfile', '-File',
            str(parser), '-SourcePath', str(installer)], capture_output=True,
            creationflags=subprocess.CREATE_NO_WINDOW, timeout=180)
        assert result.returncode == 0
        offset = json.loads(result.stdout)['offset']
        assert isinstance(offset, int) and offset >= 0
        prefix = raw.encode('utf-16-le')[:offset * 2].decode('utf-16-le')
        _OFFSETS[identity] = len(prefix.replace('\r\n', '\n').replace('\r', '\n'))
    return source[_OFFSETS[identity] + source_bom:]
