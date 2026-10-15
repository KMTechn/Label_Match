param([string]$Repo,[string]$Root,[string]$Placement,[string]$Failure)
$ErrorActionPreference='Stop'
trap {[IO.File]::WriteAllText((Join-Path $Root 'failure-site.json'),(@{type=$_.Exception.GetType().Name;line=$_.InvocationInfo.ScriptLineNumber;locations=@([regex]::Matches($_.ScriptStackTrace,'line (\d+)')|ForEach-Object{[int]$_.Groups[1].Value})}|ConvertTo-Json -Compress));throw}
$tokens=$null;$errors=$null
$ast=[Management.Automation.Language.Parser]::ParseFile((Join-Path $Repo 'INSTALL_CANONICAL_PORTABLE.ps1'),[ref]$tokens,[ref]$errors)
if($errors.Count){throw 'SOURCE_PARSE_FAILED'}
foreach($name in @('Assert-LabelInstallerRelayPreimageSession','Stop-LabelInstallerRelayPreimage','Write-LabelInstallerRelayRecoveryGuidance','Normalize-RelayExecutableQuoting','Assert-RollbackRelayPreimage','Wait-LabelRollbackRelayReady')){
    $node=@($ast.FindAll({param($n)$n -is [Management.Automation.Language.FunctionDefinitionAst] -and $n.Name -ceq $name},$true))
    if($node.Count -ne 1){throw 'FUNCTION_SELECTION_FAILED'}
    . ([scriptblock]::Create($node[0].Extent.Text))
}
Set-StrictMode -Version Latest
function Same($A,$B){[IO.Path]::GetFullPath($A) -ieq [IO.Path]::GetFullPath($B)}
$code=Join-Path $Root 'release';$exe=Join-Path $code 'runtime/pythonw.exe'
$subject='S-1-5-21-111-222-333-1001';$stamp=[DateTime]::UtcNow.AddSeconds(-1)
$row=[pscustomobject]@{ProcessId=33333;CreationDate=$stamp;ExecutablePath=$exe;CommandLine=($exe+' --label-match-user-relay')}
$Script:live=@($row);$Script:stopCalls=0
$old=@($row)
function Relays {return @($Script:live)}
function Get-CanonicalRelaySecurity($ProcessId){return @{sid=$subject;session_id=1;elevated=$false;integrity_rid=8192}}
function Wait-LabelRelayPreimageExit($Expected){if($Script:live.Count){throw 'PROCESS_NOT_STOPPED'}}
function Invoke-LabelPinnedProbe {
    param($Python,[byte[]]$ProbeBytes,$ExpectedSha256,$ProbeArguments,$FailureMessage)
    $hash=[Security.Cryptography.SHA256]::Create()
    try{$actual=([BitConverter]::ToString($hash.ComputeHash($ProbeBytes))).Replace('-','').ToLowerInvariant()}finally{$hash.Dispose()}
    if($actual -cne $ExpectedSha256 -or $ProbeArguments[1] -cne $Root){throw 'OFFICIAL_STOP_BINDING_DIFFERS'}
    $Script:stopCalls++;$Script:live=@()
    return '{"status":"ABSENT"}'
}
function Assert-LabelShortcutCopyPath($Path){}
function Test-RelayPersistentRetry($Record){return $Record.persistent_retry -eq $true}
$status=Join-Path $Root 'relay.json';$sha='a'*40
function PublishReady {
    [IO.File]::WriteAllText($status,(@{status='RUNNING';source_commit=$sha;ordinary_user=$true;process_id=$row.ProcessId;code_root=$code;updated_at=[DateTime]::UtcNow.ToString('o');persistent_retry=$true}|ConvertTo-Json))
}
PublishReady
if($Placement -cnotin @('REUSED_VERIFIED','INSTALL_REQUIRED')){throw 'PLACEMENT_DIFFERS'}
$mainCommands=@($ast.FindAll({param($n)
    if($n -isnot [Management.Automation.Language.CommandAst]){return $false}
    for($p=$n.Parent;$null -ne $p;$p=$p.Parent){if($p -is [Management.Automation.Language.FunctionDefinitionAst]){return $false}}
    return $true
},$true))
$stops=@($mainCommands|Where-Object{$_.GetCommandName() -ceq 'Stop-LabelInstallerRelayPreimage'})
$admission=@($mainCommands|Where-Object{$_.GetCommandName() -ceq 'Assert-WriterTransition'})
$fences=@($mainCommands|Where-Object{$_.GetCommandName() -ceq 'Start-LabelWriterFence'})
if($stops.Count -ne 1 -or $admission.Count -lt 1 -or $fences.Count -eq 0){throw 'MAIN_SELECTION_FAILED'}
foreach($fence in $fences){if($fence.Extent.StartOffset -lt $admission[0].Extent.StartOffset){throw 'FENCE_BEFORE_ADMISSION'}}
if($stops[0].Extent.StartOffset -lt $admission[0].Extent.StartOffset){throw 'STOP_BEFORE_ADMISSION'}
$defaultDirectSyncRoot=$Root;$source=$code;$generationSubjectSid=$subject
$normalStop=[scriptblock]::Create($stops[0].Parent.Extent.Text)
if($Failure -ceq 'absent'){
    $old=@();$Script:live=@()
    . $normalStop
    $guide=Write-LabelInstallerRelayRecoveryGuidance
    if($Script:stopCalls -ne 0 -or $guide -notlike '*--remove-current-user-setup*--onboard-current-user*'){throw 'ABSENCE_GUIDANCE_FAILED'}
}elseif($Failure -ceq 'undeclared'){
    # File-set rejection precedes the exact production stop pipeline.
    if(-not(Wait-LabelRollbackRelayReady $old $status $sha $stamp 1)){throw 'PREIMAGE_NOT_READY'}
    if($Script:stopCalls -ne 0){throw 'REJECTION_STOPPED_RELAY'}
}else{
    . $normalStop
    if($Script:stopCalls -ne 1 -or $Script:live.Count -ne 0){throw 'NORMAL_STOP_NOT_PROVEN'}
    if($Failure -cnotin @('success')){throw 'FAILURE_POINT_DIFFERS'}
    # Native launch facade supplies a fresh process; the actual readiness
    # helper must read the durable cycle and compare its process identity.
    $readyStarted=[DateTime]::UtcNow
    $row=[pscustomobject]@{ProcessId=33334;CreationDate=$readyStarted;ExecutablePath=$exe;CommandLine=($exe+' --label-match-user-relay')}
    $Script:live=@($row);PublishReady
    if(-not(Wait-LabelRollbackRelayReady @($row) $status $sha $readyStarted 1)){throw 'FINAL_READY_NOT_PROVEN'}
}
'PASS'
