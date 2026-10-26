param([string]$HostRoot,[string]$FixtureRoot,[int]$PreparationSeconds,[int]$QuerySeconds,[switch]$ExitBeforeReady)
$ErrorActionPreference='Stop'
Import-Module (Join-Path $PSHOME 'Modules/Microsoft.PowerShell.Utility/Microsoft.PowerShell.Utility.psd1')
$parseTokens=$null;$parseErrors=$null
$tree=[Management.Automation.Language.Parser]::ParseFile((Join-Path $HostRoot 'tools/apply_prepared_update.ps1'),[ref]$parseTokens,[ref]$parseErrors)
if($parseErrors.Count){throw 'SOURCE_PARSE'}
foreach($name in @('Invoke-LabelStartupReadOnlyQuery','Assert-LabelStartupNoReparsePoint','Get-StartupUpdateProcesses','Initialize-LabelStartupUpdateProcessToken')){
    $definitions=@($tree.FindAll({param($n)$n -is [Management.Automation.Language.FunctionDefinitionAst] -and $n.Name -ceq $name},$false))
    if($definitions.Count -eq 0 -and $name -ceq 'Initialize-LabelStartupUpdateProcessToken'){continue}
    if($definitions.Count -ne 1){throw 'SOURCE_COUNT'}
    . ([scriptblock]::Create($definitions[0].Extent.Text))
}
$countPath=Join-Path $FixtureRoot 'declarations.txt'
$literal="'"+$countPath.Replace("'","''")+"'"
$script:childPrefix=@'
function global:Add-Type {
    param([string]$TypeDefinition)
    [IO.File]::AppendAllText(__COUNT__,'1')
    Start-Sleep -Seconds __PREPARATION__
    Microsoft.PowerShell.Utility\Add-Type -TypeDefinition $TypeDefinition
}
function global:Get-CimInstance {
    Start-Sleep -Seconds __QUERY__
    return @()
}
'@.Replace('__COUNT__',$literal).Replace('__PREPARATION__',[string]$PreparationSeconds).Replace('__QUERY__',[string]$QuerySeconds)
if($ExitBeforeReady){$script:childPrefix="exit 0`n"+$script:childPrefix}
$newProcess={
    $process=[Diagnostics.Process]::new()
    # Preserve the real native process, handle and ownership observations. Only
    # the child's compiler/CIM boundaries receive controlled latency.
    $process | Add-Member NoteProperty OriginalStart $process.PSObject.Methods['Start']
    $process | Add-Member ScriptMethod Start {
        $encoded=($this.StartInfo.Arguments -split ' ')[-1]
        $query=[Text.Encoding]::Unicode.GetString([Convert]::FromBase64String($encoded))
        $prepared=[Convert]::ToBase64String([Text.Encoding]::Unicode.GetBytes($script:childPrefix+"`n"+$query))
        $this.StartInfo.Arguments=$this.StartInfo.Arguments.Substring(0,$this.StartInfo.Arguments.Length-$encoded.Length)+$prepared
        return $this.OriginalStart.Invoke()
    } -Force
    return $process
}
$status='RETURNED';$empty=$false;$errorType=''
$clock=[Diagnostics.Stopwatch]::StartNew()
try{
    $text=Invoke-LabelStartupReadOnlyQuery -QueryMode Observe -Root (Join-Path $FixtureRoot 'managed/current') -NewProcess $newProcess
    $empty=($text -ceq '[]')
}catch{
    $errorType=$_.Exception.GetType().Name
    $status=if($_.Exception.Message -cmatch '^STARTUP_QUERY_[A-Z_]+$'){$_.Exception.Message}else{'UNEXPECTED'}
}
$declarations=if([IO.File]::Exists($countPath)){[IO.File]::ReadAllText($countPath).Length}else{0}
@{status=$status;empty=$empty;seconds=$clock.Elapsed.TotalSeconds;declarations=$declarations;error_type=$errorType} | ConvertTo-Json -Compress
