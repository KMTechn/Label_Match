param([string]$Repo,[string]$Root,[string]$Scenario)
$ErrorActionPreference='Stop'
trap {
    $code=if($_.Exception.Message -match '^[A-Z][A-Z0-9_]{0,80}$'){$_.Exception.Message}else{'UNCLASSIFIED'}
    [IO.File]::WriteAllText((Join-Path $Root 'session-failure.json'),(@{type=$_.Exception.GetType().Name;line=$_.InvocationInfo.ScriptLineNumber;code=$code;locations=@([regex]::Matches($_.ScriptStackTrace,': line ([0-9]+)')|ForEach-Object{[int]$_.Groups[1].Value})}|ConvertTo-Json -Compress))
    throw
}
$tokens=$null;$errors=$null
$ast=[Management.Automation.Language.Parser]::ParseFile((Join-Path $Repo 'INSTALL_CANONICAL_PORTABLE.ps1'),[ref]$tokens,[ref]$errors)
if($errors.Count){throw 'SOURCE_PARSE_FAILED'}
foreach($name in @('Normalize-RelayExecutableQuoting','Assert-RollbackRelayPreimage','Assert-LabelInstallerRelayPreimageSession',
    'Stop-LabelInstallerRelayPreimage','Stop-LabelRollbackCandidateRelay','StartRaw')){
    $node=@($ast.FindAll({param($n)$n -is [Management.Automation.Language.FunctionDefinitionAst] -and $n.Name -ceq $name},$true))
    if($node.Count -ne 1){throw 'FUNCTION_MISSING'}
    . ([scriptblock]::Create($node[0].Extent.Text))
}
$forward=@($ast.FindAll({param($n)$n -is [Management.Automation.Language.TryStatementAst] -and
    $n.CatchClauses.Count -eq 1 -and $n.CatchClauses[0].Body.Extent.Text.Contains("'install_status=FAILED'")},$true))
if($forward.Count -ne 1){throw 'OUTER_CATCH_MISSING'}
$failureHandler=[scriptblock]::Create(($forward[0].CatchClauses[0].Body.Statements|ForEach-Object{$_.Extent.Text}) -join "`n")
$admission=@($forward[0].Body.Statements|Where-Object{$_.Extent.Text -match '^\[void\]\(Assert-LabelInstallerRelayPreimageSession \$old '})
if($admission.Count -ne 1 -or $admission[0] -ne $forward[0].Body.Statements[0]){throw 'ADMISSION_NOT_FIRST'}
$admit=[scriptblock]::Create($admission[0].Extent.Text)
$rollback=@($ast.FindAll({param($n)$n -is [Management.Automation.Language.FunctionDefinitionAst] -and $n.Name -ceq 'Invoke-LabelCanonicalRollback'},$true))[0]
$stopStep=@($rollback.Body.FindAll({param($n)$n -is [Management.Automation.Language.CommandAst] -and
    $n.GetCommandName() -ceq 'Invoke-LabelRollbackStep' -and $n.Extent.Text.Contains("'candidate_relay_stop'")},$true))
if($stopStep.Count -ne 1){throw 'ROLLBACK_STOP_STEP_MISSING'}
$candidateStep=[scriptblock]::Create($stopStep[0].Parent.Extent.Text)
Set-StrictMode -Version Latest
function Same($A,$B){[IO.Path]::GetFullPath($A) -ieq [IO.Path]::GetFullPath($B)}
$source=Join-Path $Root 'stock';$install=Join-Path $Root 'current';$defaultDirectSyncRoot=$Root
$exe=Join-Path $source 'runtime\pythonw.exe'
function Command($CodeRoot){return ((Join-Path $CodeRoot 'runtime\pythonw.exe')+' --label-match-user-relay')}
$subject=[Security.Principal.WindowsIdentity]::GetCurrent().User.Value
$generationSubjectSid=$subject
$stamp=[DateTime]::UtcNow.AddSeconds(-5)
$row=[pscustomobject]@{ProcessId=33333;CreationDate=$stamp;ExecutablePath=$exe;CommandLine=(Command $source)}
$old=@($row);$Script:live=@($row);$Script:LabelRollbackCandidateRelays=@()
$Script:relaySession=1;$Script:relaySid=$subject;$Script:stopCalls=0;$Script:starts=0;$Script:rollbackCalls=0
function Relays {return @($Script:live)}
function Get-CanonicalRelaySecurity($ProcessId){
    if($ProcessId -eq $PID){return @{sid=$subject;session_id=1;elevated=$false;integrity_rid=8192}}
    return @{sid=$Script:relaySid;session_id=$Script:relaySession;elevated=$false;integrity_rid=8192}
}
function Invoke-CimMethod {
    $Script:starts++
    $Script:live=@([pscustomobject]@{ProcessId=33334;CreationDate=[DateTime]::UtcNow;ExecutablePath=$exe;CommandLine=(Command $source)})
    return @{ReturnValue=0;ProcessId=33334}
}
function Get-CimInstance {return $Script:live[0]}
function Wait-LabelRelayPreimageExit($Expected){if($Script:live.Count){throw 'RELAY_STILL_PRESENT'}}
$marker=Join-Path $Root 'relay-stop-request.json';[IO.File]::WriteAllText($marker,'preserved-preimage')
$beforeMarker=[IO.File]::ReadAllBytes($marker)
function Invoke-LabelPinnedProbe {
    param($Python,[byte[]]$ProbeBytes,$ExpectedSha256,$ProbeArguments,$FailureMessage)
    $hash=[Security.Cryptography.SHA256]::Create()
    try{$actual=([BitConverter]::ToString($hash.ComputeHash($ProbeBytes))).Replace('-','').ToLowerInvariant()}finally{$hash.Dispose()}
    if($actual -cne $ExpectedSha256){throw 'PROBE_PIN_DIFFERS'}
    $Script:stopCalls++;$Script:live=@();[IO.File]::WriteAllText($marker,'this-round-stop')
    return '{"status":"ABSENT"}'
}
function Invoke-LabelRollbackStep($Name,[scriptblock]$Body){. $Body}
function Invoke-LabelCanonicalRollback {
    $Script:rollbackCalls++
    . $candidateStep
    $audit.rollback.code_restored=$true
}
function Get-LabelLaunchFailure($Failure,$Stage){return @{guidance='REFUSED';exception_type=$Failure.Exception.GetType().Name}}
function Save-LabelRollbackContext {}
function Save($Path,$Value){[IO.File]::WriteAllText($Path,($Value|ConvertTo-Json -Depth 20))}
function Snapshot {return @{exists=$false;kind='';data=''}}
$auditPath=Join-Path $Root 'audit.json';$EvidencePath=''
$audit=[ordered]@{status='UNKNOWN';rollback=[ordered]@{relay_readiness='UNPROVEN'}}
$Script:LabelCanonicalConfirmation='UNATTEMPTED';$Script:LabelPlacementSession=$null
$startupLifecycle=@{lifecycle_state='INSTALLED'};$mutated=$false;$placementAttempted=$false
$legacyFirstRegistration=$false;$writerFenceStarted=$true;$rollbackStartupProven=$false;$rollbackRuntimeProven=$false
$before=@{exists=$false;kind='';data=''}
if($Scenario -eq 'other-session'){$Script:relaySession=2}
elseif($Scenario -eq 'other-sid'){$Script:relaySid='OTHER'}
elseif($Scenario -eq 'pid-change'){$Script:live=@([pscustomobject]@{ProcessId=33335;CreationDate=$stamp;ExecutablePath=$exe;CommandLine=(Command $source)})}
elseif($Scenario -eq 'birth-change'){$Script:live=@([pscustomobject]@{ProcessId=33333;CreationDate=$stamp.AddSeconds(1);ExecutablePath=$exe;CommandLine=(Command $source)})}
$refusedPid=$Script:live[0].ProcessId;$refusedBirth=$Script:live[0].CreationDate
try {
    try {
        . $admit
        if($Scenario -eq 'untracked-rollback'){$mutated=$true;$Script:relaySession=2;throw 'INJECTED_AFTER_MUTATION'}
        if($Scenario -in @('verified-candidate','candidate-other-session','candidate-birth-change')){
            $Script:live=@()
            [void](StartRaw (Command $source) -TrackRollbackCandidate -CandidateCodeRoot $source)
            $mutated=$true
            if($Scenario -eq 'candidate-other-session'){$Script:relaySession=2}
            if($Scenario -eq 'candidate-birth-change'){
                $copy=$Script:live[0].PSObject.Copy();$copy.CreationDate=$copy.CreationDate.AddSeconds(1);$Script:live=@($copy)
            }
            $refusedPid=$Script:live[0].ProcessId;$refusedBirth=$Script:live[0].CreationDate
            throw 'INJECTED_AFTER_LAUNCH'
        }
        throw 'ADMISSION_SHOULD_REFUSE'
    } catch {. $failureHandler}
} catch {}
$saved=Get-Content -LiteralPath $auditPath -Raw -Encoding UTF8|ConvertFrom-Json
if($Scenario -eq 'verified-candidate'){
    if($Script:stopCalls -ne 1 -or $Script:live.Count -ne 0 -or $saved.status -cne 'FAILED_ROLLED_BACK'){throw 'VERIFIED_STOP_FAILED'}
}else{
    if($Script:stopCalls -ne 0 -or [Convert]::ToBase64String([IO.File]::ReadAllBytes($marker)) -cne [Convert]::ToBase64String($beforeMarker)){throw 'FOREIGN_RELAY_MUTATED'}
    if($Script:live.Count -ne 1){throw 'RELAY_LOST'}
    if($Script:live[0].ProcessId -ne $refusedPid -or $Script:live[0].CreationDate -ne $refusedBirth){throw 'RELAY_IDENTITY_CHANGED'}
    if(-not $mutated -and ($Script:rollbackCalls -ne 0 -or $Script:starts -ne 0 -or $saved.status -cne 'FAILED_SUPPORTED_REMOVAL_REQUIRED')){throw 'MUTATION_BEFORE_ADMISSION'}
    if($mutated -and ($Script:rollbackCalls -ne 1 -or $saved.status -cne 'ROLLBACK_FAILED')){throw 'ROLLBACK_NOT_CONNECTED'}
}
'PASS'
