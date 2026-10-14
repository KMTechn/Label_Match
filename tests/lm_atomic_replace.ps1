param([string]$HostRoot,[string]$FixtureRoot)
$ErrorActionPreference='Stop'
Import-Module (Join-Path $PSHOME 'Modules/Microsoft.PowerShell.Utility/Microsoft.PowerShell.Utility.psd1')
$tokens=$null;$errors=$null
$tree=[Management.Automation.Language.Parser]::ParseFile((Join-Path $HostRoot 'INSTALL_THIS_PC.ps1'),[ref]$tokens,[ref]$errors)
if($errors.Count){throw 'SOURCE_PARSE'}
$body=@($tree.FindAll({param($n)$n -is [Management.Automation.Language.FunctionDefinitionAst] -and $n.Name -ceq 'Invoke-LabelAtomicReplace'},$false))
if($body.Count -ne 1){throw 'SOURCE_COUNT'}
. ([scriptblock]::Create($body[0].Extent.Text))
# Warm the native declaration before measuring the bounded retry path.
$warm=Join-Path $FixtureRoot 'warm';[IO.File]::WriteAllText($warm,'warm')
Invoke-LabelAtomicReplace $warm ($warm+'.done') -Mode WriteThrough
$script:reader=$null;$script:sleeps=0;$script:release=$true
function Start-Sleep {param([int]$Milliseconds)
    if($Milliseconds -le 0 -or $Milliseconds -gt 50){throw 'RETRY_INTERVAL'}
    $script:sleeps++
    if($script:release){$script:reader.Dispose();$script:reader=$null;$script:release=$false}
    else{Microsoft.PowerShell.Utility\Start-Sleep -Milliseconds $Milliseconds}
}
foreach($mode in @('Move','Replace','WriteThrough')){
    $source=Join-Path $FixtureRoot ($mode+'.source');$target=Join-Path $FixtureRoot ($mode+'.target')
    [IO.File]::WriteAllText($source,'new')
    if($mode -cne 'Move'){[IO.File]::WriteAllText($target,'old')}
    $held=if($mode -ceq 'Move'){$source}else{$target}
    $script:reader=[IO.File]::Open($held,'Open','Read','Read')
    $script:release=$true;$script:sleeps=0
    try{Invoke-LabelAtomicReplace $source $target -Mode $mode}
    finally{if($null -ne $script:reader){$script:reader.Dispose();$script:reader=$null}}
    if($script:sleeps -ne 1 -or [IO.File]::ReadAllText($target) -cne 'new'){throw 'OVERLAP_NOT_RETRIED'}
    [IO.File]::WriteAllText($source,'newer')
    if($mode -ceq 'Move'){[IO.File]::Delete($target)}
    $held=if($mode -ceq 'Move'){$source}else{$target}
    $script:reader=[IO.File]::Open($held,'Open','Read','Read')
    $script:release=$false;$script:sleeps=0;$failure=$null
    $clock=[Diagnostics.Stopwatch]::StartNew()
    try{Invoke-LabelAtomicReplace $source $target -Mode $mode}catch{$failure=$_.Exception.GetBaseException()}
    finally{$script:reader.Dispose();$script:reader=$null}
    if($null -eq $failure -or $script:sleeps -lt 2 -or $clock.Elapsed.TotalSeconds -gt 2){throw 'PERSISTENT_BOUND'}
    $code=if($failure -is [ComponentModel.Win32Exception]){$failure.NativeErrorCode}else{$failure.HResult -band 0xffff}
    if($code -notin @(5,32) -or [IO.File]::ReadAllText($source) -cne 'newer'){throw 'ORIGINAL_NATIVE_REFUSAL'}
    if($mode -cne 'Move' -and [IO.File]::ReadAllText($target) -cne 'new'){throw 'TARGET_CHANGED_ON_REFUSAL'}
}
$script:sleeps=0;$failure=$null
try{Invoke-LabelAtomicReplace (Join-Path $FixtureRoot 'missing') (Join-Path $FixtureRoot 'missing-target') -Mode WriteThrough}catch{$failure=$_.Exception.GetBaseException()}
if($null -eq $failure -or $failure.NativeErrorCode -ne 2 -or $script:sleeps -ne 0){throw 'PERMANENT_REFUSAL_RETRIED'}
foreach($name in @('label_writer_fence.ps1','label_worker_shortcuts.ps1')){
    . (Join-Path (Join-Path $HostRoot 'tools') $name)
    $source=Join-Path $FixtureRoot ($name+'.new');$target=Join-Path $FixtureRoot ($name+'.done')
    [IO.File]::WriteAllText($source,'pinned')
    Invoke-LabelPinnedAtomicReplace $source $target -Mode Move
    if([IO.File]::ReadAllText($target) -cne 'pinned'){throw 'PINNED_CALLER'}
}
Write-Output 'PASS'
