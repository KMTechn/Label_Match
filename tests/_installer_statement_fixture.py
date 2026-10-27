"""Select the real installer transaction with the PowerShell compiler."""
import hashlib
import json
from pathlib import Path
import shutil
import subprocess

from tests._bounded_powershell import run as run_bounded

_TRANSACTIONS = {}


def installer_transaction(installer: Path, source: str, scratch: Path, *, marker: str) -> str:
    raw = installer.read_bytes().decode('utf-8-sig')
    assert raw.replace('\r\n', '\n').replace('\r', '\n') == source.removeprefix('\ufeff')
    identity = hashlib.sha256((raw + marker).encode('utf-8')).hexdigest()
    if identity not in _TRANSACTIONS:
        parser = scratch / 'installer-transaction-ast.ps1'
        selection = scratch / 'installer-transaction-marker.json'
        body = scratch / 'installer-transaction-body.ps1'
        selection.write_text(json.dumps({'marker': marker}), encoding='utf-8')
        parser.write_text("""param([string]$SourcePath,[string]$MarkerPath,[string]$BodyPath)
$ErrorActionPreference='Stop'
$tokens=$null;$parseErrors=$null
$tree=[Management.Automation.Language.Parser]::ParseFile($SourcePath,[ref]$tokens,[ref]$parseErrors)
if(@($parseErrors).Count){throw 'installer syntax is invalid'}
$selection=Get-Content -LiteralPath $MarkerPath -Raw | ConvertFrom-Json
$prefix=$selection.marker.TrimStart().Replace("`r`n","`n").Replace("`r","`n")
$transactions=@($tree.FindAll({param($node)
    $node -is [Management.Automation.Language.AssignmentStatementAst] -and
    $node.Extent.Text.Replace("`r`n","`n").Replace("`r","`n").StartsWith($prefix)
},$true))
if($transactions.Count -ne 1){throw 'installer transaction marker is ambiguous'}
$markerStatement=$transactions[0]
$owner=@($tree.EndBlock.Statements | Where-Object {
    $_.Extent.StartOffset -le $markerStatement.Extent.StartOffset -and
    $_.Extent.EndOffset -ge $markerStatement.Extent.EndOffset
})
if($owner.Count -ne 1){throw 'installer transaction owner is ambiguous'}
if($owner[0] -eq $markerStatement){
    $selected=@($tree.EndBlock.Statements | Where-Object {$_.Extent.StartOffset -ge $markerStatement.Extent.StartOffset})
}else{
    $selected=@($markerStatement.Parent.Statements | Where-Object {$_.Extent.StartOffset -ge $markerStatement.Extent.StartOffset})
    $selected+=@($tree.EndBlock.Statements | Where-Object {$_.Extent.StartOffset -gt $owner[0].Extent.EndOffset})
}
$body=($selected | ForEach-Object {$_.Extent.Text}) -join "`n"
$bodyTokens=$null;$bodyErrors=$null
$null=[Management.Automation.Language.Parser]::ParseInput($body,[ref]$bodyTokens,[ref]$bodyErrors)
if(@($bodyErrors).Count){throw 'fixture transaction syntax is invalid'}
[IO.File]::WriteAllText($BodyPath,$body,[Text.UTF8Encoding]::new($false))
@{matches=$transactions.Count} | ConvertTo-Json -Compress
""", encoding='utf-8')
        result = run_bounded([shutil.which('pwsh'), '-NoLogo', '-NoProfile', '-File',
            str(parser), '-SourcePath', str(installer), '-MarkerPath', str(selection),
            '-BodyPath', str(body)], capture_output=True,
            creationflags=subprocess.CREATE_NO_WINDOW, timeout=180)
        assert result.returncode == 0
        assert json.loads(result.stdout)['matches'] == 1
        _TRANSACTIONS[identity] = body.read_text(encoding='utf-8-sig')
    return _TRANSACTIONS[identity]
