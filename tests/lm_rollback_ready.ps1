param([string]$Repo,[string]$Root,[string]$Scenario)
$ErrorActionPreference='Stop'
$tokens=$null;$errors=$null;$ast=[Management.Automation.Language.Parser]::ParseFile((Join-Path $Repo 'INSTALL_CANONICAL_PORTABLE.ps1'),[ref]$tokens,[ref]$errors)
if($errors.Count){throw 'parse failed'}
$names=@('Assert-RollbackRelayPreimage','Wait-LabelRollbackRelayReady','Assert-LabelShortcutCopyPath')
foreach($node in $ast.FindAll({param($n)$n -is [Management.Automation.Language.FunctionDefinitionAst]},$false)){
    if($node.Name -cin $names){. ([scriptblock]::Create($node.Extent.Text))}
}
function Same($a,$b){[IO.Path]::GetFullPath($a) -ieq [IO.Path]::GetFullPath($b)}
function Normalize-RelayExecutableQuoting($CommandLine,$Executable){$CommandLine}
function Test-RelayPersistentRetry($Record){$Record.persistent_retry -is [bool] -and $Record.persistent_retry}
$now=[DateTime]::UtcNow;$sha='a'*40;$install=Join-Path $Root 'current'
$expected=[pscustomobject]@{ProcessId=71;ExecutablePath=(Join-Path $install 'runtime/pythonw.exe');CommandLine='relay';CreationDate=$now.AddMinutes(-2)}
$actual=[pscustomobject]@{ProcessId=72;ExecutablePath=$expected.ExecutablePath;CommandLine=$expected.CommandLine;CreationDate=$now}
if($Scenario -ceq 'original'){$actual=$expected}
function Relays { $actual }
$status=Join-Path $Root 'status.json'
$record=@{status='RUNNING';source_commit=$sha;ordinary_user=$true;process_id=$actual.ProcessId;
    code_root=(Join-Path $install 'app');updated_at=$now.ToString('o');persistent_retry=$true}
if($Scenario -ceq 'stale'){$record.updated_at=$now.AddMinutes(-5).ToString('o')}
if($Scenario -ceq 'wrong_pid'){$record.process_id=99}
if($Scenario -ceq 'wrong_sha'){$record.source_commit='b'*40}
if($Scenario -cne 'no_signal'){[IO.File]::WriteAllText($status,($record|ConvertTo-Json),[Text.UTF8Encoding]::new($false))}
$caught=$null;$ready=$false
$budget=if($Scenario -cin @('ready','original')){2}else{.2}
try{$ready=Wait-LabelRollbackRelayReady @($expected) $status $sha $now $budget}catch{$caught=$_}
if($Scenario -cin @('ready','original') -and $caught){
    @($Error|ForEach-Object{@{type=$_.Exception.GetType().FullName;line=$_.InvocationInfo.ScriptLineNumber}})|ConvertTo-Json -Depth 6|Set-Content -LiteralPath (Join-Path $Root 'error-taxonomy.json')
}
if($Scenario -cin @('ready','original')){if($caught -or -not $ready){throw 'valid preparation not proven'}}
else{if($null -eq $caught -or $ready){throw 'PID or stale metadata accepted'}}
'PASS'
