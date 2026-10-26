param([string]$Repo,[string]$Root,[string]$Case)
$ErrorActionPreference='Stop'
[Console]::OutputEncoding=[Text.UTF8Encoding]::new($false)
$tokens=$null;$errors=$null
$ast=[Management.Automation.Language.Parser]::ParseFile((Join-Path $Repo 'INSTALL_CANONICAL_PORTABLE.ps1'),[ref]$tokens,[ref]$errors)
if($errors.Count){throw 'SOURCE_PARSE_FAILED'}
foreach($name in @('Arg','Normalize-RelayExecutableQuoting','Test-LabelInstallerRelayChild','Assert-LabelInstallerRelayPreimageSession','Stop-LabelInstallerRelayPreimage')){
    $nodes=@($ast.FindAll({param($n)$n -is [Management.Automation.Language.FunctionDefinitionAst] -and $n.Name -ceq $name},$true))
    if($nodes.Count -eq 1){. ([scriptblock]::Create($nodes[0].Extent.Text))}
}
Set-StrictMode -Version Latest
function Same($A,$B){[IO.Path]::GetFullPath($A) -ieq [IO.Path]::GetFullPath($B)}
$lifecycleInstalled=Join-Path $Root 'installed release'
$source=$lifecycleInstalled;$defaultDirectSyncRoot=Join-Path $Root 'state'
$generationSubjectSid='S-1-5-21-111-222-333-1001'
$birth=[DateTime]::UtcNow.AddSeconds(-30)
$parentExe=Join-Path $lifecycleInstalled 'runtime/pythonw.exe'
$childExe=Join-Path $lifecycleInstalled 'runtime/python.exe'
$entry=Join-Path $lifecycleInstalled 'app/label_match_relay_entry.py'
if($Case -ceq 'frozen'){$parentExe=Join-Path $lifecycleInstalled 'Label_Match.exe';$childExe=$parentExe}
$parent=[pscustomobject]@{ProcessId=31001;CreationDate=$birth;ExecutablePath=$parentExe;CommandLine=((Arg $parentExe)+' --label-match-user-relay')}
$child=[pscustomobject]@{ProcessId=31002;ParentProcessId=31001;CreationDate=$birth.AddSeconds(1);ExecutablePath=$childExe;CommandLine=((Arg $childExe)+' -I -B '+(Arg $entry)+' --label-match-direct-sync-relay --worker-id example')}
if($Case -ceq 'frozen'){$child.CommandLine=(Arg $childExe)+' --label-match-direct-sync-relay --worker-id example'}
$old=@($parent);$Script:live=@($parent);$Script:writers=@($parent,$child);$Script:stopCalls=0
$Script:childSecurity=@{sid=$generationSubjectSid;session_id=1;elevated=$false;integrity_rid=8192}
switch -CaseSensitive ($Case){
    'standalone' {$old=@();$Script:live=@();$Script:writers=@($child)}
    'other-parent' {$child.ParentProcessId=32001}
    'other-session' {$Script:childSecurity.session_id=2}
    'other-sid' {$Script:childSecurity.sid='S-1-5-21-111-222-333-1002'}
    'elevated' {$Script:childSecurity.elevated=$true}
    'other-integrity' {$Script:childSecurity.integrity_rid=4096}
    'older-child' {$child.CreationDate=$birth.AddSeconds(-1)}
    'other-root' {$child.ExecutablePath=Join-Path $Root 'other/runtime/python.exe'}
    'other-entry' {$child.CommandLine=(Arg $childExe)+' -I -B '+(Arg (Join-Path $Root 'other/entry.py'))+' --label-match-direct-sync-relay --worker-id example'}
    'unknown-security' {$Script:childSecurity=@{}}
    'missing-parent' {$child.PSObject.Properties.Remove('ParentProcessId')}
}
function Relays {return @($Script:live)}
function UnquiescedProductWriters {return @($Script:writers)}
function Get-CanonicalRelaySecurity($ProcessId){
    if($ProcessId -eq $child.ProcessId){return $Script:childSecurity}
    return @{sid=$generationSubjectSid;session_id=1;elevated=$false;integrity_rid=8192}
}
function Assert-RollbackRelayPreimage($Expected){return @($Script:live)}
function Wait-LabelRelayPreimageExit($Expected){if($Script:live.Count){throw 'RELAY_REMAINS'}}
function Invoke-LabelPinnedProbe {
    param($Python,[byte[]]$ProbeBytes,$ExpectedSha256,$ProbeArguments,$FailureMessage)
    $Script:stopCalls++
    # The request waits for the running child's 20-second cycle, then the relay.
    $Script:writers=@();$Script:live=@()
    return '{"status":"ABSENT"}'
}
$preflight=@($ast.FindAll({param($n)$n -is [Management.Automation.Language.ForEachStatementAst] -and $n.Extent.Text -match '^foreach\(\$writer in @\(UnquiescedProductWriters\)\)'},$true))
if($preflight.Count -ne 1){throw 'PREFLIGHT_SELECTION_FAILED'}
$accepted=$Case -cin @('normal','frozen')
$rejected=$false
try{. ([scriptblock]::Create($preflight[0].Extent.Text))}catch{
    if($_.Exception.Message -cne 'An unrelated product writer prevents installation.'){throw}
    $rejected=$true
}
if($accepted){
    if($rejected){throw 'VERIFIED_CHILD_REJECTED_BEFORE_STOP'}
    Stop-LabelInstallerRelayPreimage $old $defaultDirectSyncRoot $source $generationSubjectSid
    if($Script:stopCalls -ne 1 -or @(UnquiescedProductWriters).Count -ne 0){throw 'CHILD_NOT_WAITED_WITH_RELAY'}
}else{
    if(-not $rejected -or $Script:stopCalls -ne 0){throw 'UNRELATED_WRITER_ACCEPTED'}
}
'PASS'
