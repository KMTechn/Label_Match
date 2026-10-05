"""Actual catch/G/fence with isolated file and native process I/O stand-ins."""
from pathlib import Path
import hashlib
import json
import re
import shutil
import subprocess

import pytest
from tests.test_writer_transition import _definitions, _environment, _quote

REPO = Path(__file__).resolve().parents[1]


@pytest.mark.parametrize("shell", ["powershell.exe", "pwsh.exe"])
@pytest.mark.parametrize("failure", ['product', 'task', 'generation', 'code'])
def test_rollback_attempts_guarded_tail_and_preserves_failures(tmp_path, shell, failure):
    executable = shutil.which(shell)
    if not executable:
        pytest.skip("PowerShell is unavailable")
    tree = tmp_path / "installed"
    files = {
        "runtime/python.exe": b"native runtime stand-in",
        "runtime/pythonw.exe": b"native runtime stand-in",
        "app/main.py": b"pass\n",
        "app/Label_Match.py": b'APP_VERSION = "v2.0.95"\n',
    }
    for name, raw in files.items():
        path = tree / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(raw)
    for name in ["INSTALL_CANONICAL_PORTABLE.ps1", "INSTALL_THIS_PC.ps1",
                 "tools/bootstrap_integrity.ps1", "tools/label_writer_fence.ps1"]:
        path = tree / name
        path.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(REPO / name, path)
    shutil.copy2(REPO / "portable/launch-label-match.cmd", tree / "launch-label-match.cmd")
    commit = subprocess.check_output(["git", "-C", str(REPO), "rev-parse", "HEAD"], text=True).strip()
    (tree / "portable-manifest.json").write_text(json.dumps({
        "schema": "label-match-portable-tree-v1", "version": "v2.0.95",
        "source_commit": commit, "source_tree": commit,
        "entrypoint": "runtime/pythonw.exe app/main.py", "launcher": "launch-label-match.cmd",
        "allowed_unsigned_app_pe": [], "forbidden_package_roots": [],
        "runtime_pythonw_sha256": hashlib.sha256((tree / "runtime/pythonw.exe").read_bytes()).hexdigest(),
        "launcher_sha256": hashlib.sha256((tree / "launch-label-match.cmd").read_bytes()).hexdigest(),
    }), encoding="utf-8")
    source = (REPO / "INSTALL_CANONICAL_PORTABLE.ps1").read_text(encoding="utf-8-sig")
    tail = source[source.index("catch {\n    $original = $_"):]
    rollback_sources = re.search(r"(?ms)^\$rollbackSources = @\(.*?^\)", source).group()
    output = tmp_path / "result.json"
    script = tmp_path / "catch.ps1"
    code = _definitions() + f"""
. (Get-LabelSharedPortableLeafPath {_quote(REPO)} 'data_generation.ps1')
. {_quote(REPO / 'tools/label_writer_fence.ps1')}
. {_quote(REPO / 'tools/bootstrap_integrity.ps1')} -SharedCodeRoot {_quote(REPO)}
# JSON I/O adapter retains strings on PS7.5+ (the PS5.1 product default).
if ((Get-Command ConvertFrom-Json).Parameters.ContainsKey('DateKind')) {{
    $PSDefaultParameterValues['ConvertFrom-Json:DateKind']='String'
}}
$source={_quote(tree)}; $install=$source; $existingVerified=$true; $mutated=$true
$replacementRollbackRoot=$source; $legacyFirstRegistration=$false; $healthyRemovedInstall=$false
$healthyRemovalReportBefore=$null; $removalReportExisted=$false; $SkipSignatureValidationForTest=$true
$stop={_quote(tmp_path / 'stop.json')}; $removalPath={_quote(tmp_path / 'removal.json')}
$stopBefore=@{{exists=$false;sha256='';backup_path=''}}
$before=@{{exists=$true;kind='String';data='native old relay command'}}
$script:run=@{{exists=$true;kind='String';data='native new relay command'}}
$taskBefore=@{{exists=$false;enabled=$false;xml_sha256='';backup_path=''}}
$script:attempts=@{{product=0;run=0;task=0;relay=0;g=0}}
$script:relayLive=$false
$old=@([pscustomobject]@{{CommandLine='native old relay command';ExecutablePath=(Join-Path $install 'runtime\\pythonw.exe')}})
function Relays {{ if ($script:relayLive) {{ return $old }}; return @() }}
function Snapshot {{ return $script:run }}
function Restore($Before) {{ $script:attempts.run++; $script:run=$Before }}
$script:realTaskRestore=${{function:RestoreScheduledTask}}
function RestoreScheduledTask($Before) {{
    $script:attempts.task++
    if ({_quote(failure)} -ceq 'task') {{ throw 'injected native task restore failure' }}
    & $script:realTaskRestore $Before
}}
function Get-ScheduledTask {{ param($TaskName,$TaskPath,$ErrorAction) return $null }}
function Product {{ param($Root,$Mode,$ExtraArguments) $script:attempts.product++; throw 'injected native product removal failure' }}
function StartRaw($Line) {{ $script:attempts.relay++; $script:relayLive=$true; return 2147418112 }}
$script:realCim=${{function:Get-CimInstance}}
function Get-CimInstance([string]$ClassName,[string]$Filter,[string]$ErrorAction='Continue') {{
 if ($ClassName -ceq 'Win32_Process' -and $Filter -match '^\s*ProcessId\s*=\s*(\d+)\s*$' -and [int]$Matches[1] -eq 2147418112) {{ return $old[0] }}
 & $script:realCim @PSBoundParameters
}}
$script:realDecision=(Get-Command Get-KmtechDataDowngradeDecision).ScriptBlock
function Get-KmtechDataDowngradeDecision([string[]]$MarkerPaths,[string]$TargetVersion,[string]$AppId,[string]$SubjectSid,[string]$TargetBuildId) {{
    $script:attempts.g++
    & $script:realDecision -MarkerPaths $MarkerPaths -TargetVersion $TargetVersion -AppId $AppId -SubjectSid $SubjectSid -TargetBuildId $TargetBuildId
}}
function InvokeFrozenIntegrityProbe($Frozen,$Root) {{
 if ('{failure}' -ceq 'code') {{ throw 'injected code verification failure' }}
    [void](PinnedFileBytes (Join-Path $Frozen.root 'tools/bootstrap_integrity.ps1') $Frozen.integrity_sha256)
    [void](Assert-BootstrapIntegrityRecord $Root)
}}
[void](Write-BootstrapIntegrityRecord -Root $install -CodeRoot $install)
$installedPreimageInventory=PortableInventory $install; $receiptSource=$installedPreimageInventory
$frozenPlacement=@{{root={_quote(REPO)};integrity_sha256=(Sha {_quote(REPO / 'tools/bootstrap_integrity.ps1')})}}
$generationSubjectSid=[Security.Principal.WindowsIdentity]::GetCurrent().User.Value
$generationMarkerPaths=@({_quote(tmp_path / 'data-generation.json')})
$version=if ({_quote(failure)} -ceq 'generation') {{'v2.0.96'}} else {{'v2.0.95'}}
[void](Write-KmtechDataGeneration -MarkerPath $generationMarkerPaths[0] -Version $version -AppId 'Label_Match' -SubjectSid $generationSubjectSid -BuildId {_quote(commit)})
$writerFenceControlRoot=$env:KMTECH_LABEL_WRITER_CONTROL_ROOT
$writerSessionId=[Guid]::NewGuid().ToString('N'); $writerAttemptId=[Guid]::NewGuid().ToString('N')
$writerTransactionId=[Guid]::NewGuid().ToString('N'); $writerStartedAt=[DateTime]::UtcNow.ToString('o')
$writerDelegationToken=[Guid]::NewGuid().ToString('N')+[Guid]::NewGuid().ToString('N')
$writerOrchestratorSha256=Sha {_quote(REPO / 'INSTALL_CANONICAL_PORTABLE.ps1')}
$writerContractSha256=Sha {_quote(REPO / 'tools/label_writer_fence.ps1')}
$Script:LabelWriterFenceInstalledIdentity=[pscustomobject]@{{
 session_id=$writerSessionId; attempt_id=$writerAttemptId; replacement_transaction_id=$writerTransactionId;
 orchestrator_sha256=$writerOrchestratorSha256; writer_contract_sha256=$writerContractSha256;
 inventory_sha256=$Script:LabelWriterFenceInventorySha256
}}
$writerEnvironmentBefore=@($WriterDelegationEnvironmentNames | ForEach-Object {{ [Environment]::GetEnvironmentVariable($_, 'Process') }})
{rollback_sources}
$writerAuthority=Enter-LabelWriterSessionAuthority -SessionId $writerSessionId -AttemptId $writerAttemptId -OrchestratorSha256 $writerOrchestratorSha256 -ReplacementTransactionId $writerTransactionId -WriterContractSha256 $writerContractSha256
[void](Start-LabelWriterFence -ControlRoot $writerFenceControlRoot -Status 'RESTORING' -SessionId $writerSessionId -AttemptId $writerAttemptId -ReplacementTransactionId $writerTransactionId -SessionStartedAtUtc $writerStartedAt -OrchestratorSha256 $writerOrchestratorSha256 -WriterContractSha256 $writerContractSha256 -AuthorityOwnedByCaller)
$writerFenceStarted=$true; $EvidencePath=''; $auditPath={_quote(tmp_path / 'audit.json')}
$audit=[ordered]@{{rollback=[ordered]@{{}};writer_fence=[ordered]@{{status='RESTORING'}}}}
try {{
try {{ throw 'injected install failure' }}
{tail}
}} catch {{}}
$result=@{{attempts=$script:attempts;fence_active=(Test-Path (Get-LabelWriterFenceActivePath $writerFenceControlRoot));audit=$audit;relay_live=$script:relayLive}}
[IO.File]::WriteAllText({_quote(output)},($result | ConvertTo-Json -Depth 12))
"""
    script.write_text(code, encoding="utf-8-sig")
    completed = subprocess.run([executable, "-NoProfile", "-NonInteractive", "-ExecutionPolicy", "Bypass", "-File", str(script)],
        env=_environment(tmp_path), capture_output=True, text=True, timeout=180,
        creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0))
    (tmp_path / "stdout.log").write_text(completed.stdout, encoding="utf-8")
    (tmp_path / "stderr.log").write_text(completed.stderr, encoding="utf-8")
    assert completed.returncode == 0
    result = json.loads(output.read_text(encoding="utf-8-sig"))
    assert result["attempts"]["g"] >= 3
    assert result["audit"]["status"] == "ROLLBACK_FAILED"
    assert result["audit"]["rollback"]["step_failures"]
    assert "관리자" in result["audit"]["user_message"]
    if failure == "product":
        assert result["attempts"]["product"] == 1
        assert result["attempts"]["run"] == 1
        assert result["attempts"]["task"] == 1
        assert result["attempts"]["relay"] == 1
        assert result["audit"]["rollback"]["runtime_restored"] is True
        assert result["fence_active"] is False
    else:
        assert result["fence_active"] is True
        assert result["attempts"]["relay"] == 0
        assert result["audit"]["rollback"]["runtime_restored"] is False
        if failure == "task":
            assert result["attempts"]["run"] == 1
            assert result["attempts"]["task"] == 1

    if failure == "code":
        assert result["attempts"]["run"] == 1
        assert result["attempts"]["task"] == 1
