param([string]$HostRoot,[string]$FixtureRoot,[ValidateSet('slow','blocked','reuse')][string]$Scenario)
$ErrorActionPreference='Stop'
Import-Module (Join-Path $PSHOME 'Modules/Microsoft.PowerShell.Utility/Microsoft.PowerShell.Utility.psd1')
$parseTokens=$null;$parseErrors=$null
$tree=[Management.Automation.Language.Parser]::ParseFile((Join-Path $HostRoot 'INSTALL_THIS_PC.ps1'),[ref]$parseTokens,[ref]$parseErrors)
if($parseErrors.Count){throw 'SOURCE_PARSE'}
$body=@($tree.FindAll({param($n)$n -is [Management.Automation.Language.FunctionDefinitionAst] -and $n.Name -ceq 'Invoke-LabelAtomicReplace'},$false))
if($body.Count -ne 1){throw 'SOURCE_COUNT'}
# Execute the exact product body, including its type-existence check and native API.
. ([scriptblock]::Create($body[0].Extent.Text))
if('KMTechStartupUpdateAtomicFile' -as [type]){throw 'EXPECTED_COLD_PROCESS'}
$script:declarations=0
function Add-Type {param([string]$TypeDefinition)
    $script:declarations++
    if($Scenario -ceq 'slow'){Microsoft.PowerShell.Utility\Start-Sleep -Milliseconds 1500}
    Microsoft.PowerShell.Utility\Add-Type -TypeDefinition $TypeDefinition
}
$source=Join-Path $FixtureRoot 'source';$target=Join-Path $FixtureRoot 'target'
[IO.File]::WriteAllText($source,'first')
Invoke-LabelAtomicReplace $source $target -Mode WriteThrough
if($script:declarations -ne 1 -or -not ('KMTechStartupUpdateAtomicFile' -as [type])){throw 'DECLARATION_COUNT'}
if([IO.File]::Exists($source) -or [IO.File]::ReadAllText($target) -cne 'first'){throw 'COLD_PUBLICATION_BYTES'}
if($Scenario -ceq 'blocked'){
    [IO.File]::WriteAllText($source,'second')
    $reader=[IO.File]::Open($target,'Open','Read','Read')
    $failure=$null;$clock=[Diagnostics.Stopwatch]::StartNew()
    try{Invoke-LabelAtomicReplace $source $target -Mode WriteThrough}catch{$failure=$_.Exception.GetBaseException()}
    finally{$reader.Dispose()}
    if($failure -isnot [ComponentModel.Win32Exception] -or $failure.NativeErrorCode -notin @(5,32)){throw 'NATIVE_REFUSAL_LOST'}
    if($clock.Elapsed.TotalMilliseconds -lt 1000 -or $clock.Elapsed.TotalSeconds -gt 2){throw 'NATIVE_DEADLINE_CHANGED'}
    if([IO.File]::ReadAllText($source) -cne 'second' -or [IO.File]::ReadAllText($target) -cne 'first'){throw 'REFUSAL_CHANGED_BYTES'}
}elseif($Scenario -ceq 'reuse'){
    [IO.File]::WriteAllText($source,'second')
    Invoke-LabelAtomicReplace $source $target -Mode WriteThrough
    if([IO.File]::Exists($source) -or [IO.File]::ReadAllText($target) -cne 'second'){throw 'REUSED_PUBLICATION_BYTES'}
}
if($script:declarations -ne 1){throw 'DECLARATION_REPEATED'}
'PASS'
