param([string]$SourceFile,[string]$Root,[string]$Scenario)
$ErrorActionPreference='Stop'
$tokens=$null;$errors=$null
$ast=[Management.Automation.Language.Parser]::ParseFile($SourceFile,[ref]$tokens,[ref]$errors)
if($errors.Count){throw 'source parse failed'}
$names=@('Copy-LabelAtomicStream','Copy-LabelAtomicFile','Invoke-LabelAtomicReplace','Invoke-LabelPinnedAtomicReplace','Assert-LabelShortcutCopyPath','Assert-LabelShortcutPath','Get-LabelShortcutSha256')
foreach($node in $ast.FindAll({param($n) $n -is [Management.Automation.Language.FunctionDefinitionAst]},$false)){
    if($node.Name -cin $names){. ([scriptblock]::Create($node.Extent.Text))}
}
function Assert-NoReparsePoint($Path,$Purpose){Assert-LabelShortcutCopyPath $Path}
function Sha($Path){(Get-FileHash -LiteralPath $Path -Algorithm SHA256).Hash.ToLowerInvariant()}
function Get-FileSha256($Path){Sha $Path}
if(-not (Get-Command Assert-LabelShortcutCopyPath -ErrorAction SilentlyContinue)){
    function Assert-LabelShortcutCopyPath($Path){if((Get-Item -LiteralPath (Split-Path -Parent $Path)).Attributes -band [IO.FileAttributes]::ReparsePoint){throw 'reparse'}}
}
$source=Join-Path $Root 'preimage';$target=Join-Path $Root 'live'
[IO.File]::WriteAllBytes($source,([byte[]](1..255)*128))
[IO.File]::WriteAllText($target,'original live state')
$sourceHash=Sha $source;$targetHash=Sha $target
$held=$null;$failure=$null
if($Scenario -ceq 'partial'){
    function Copy-LabelAtomicStream($SourceStream,$DestinationStream){
        $buffer=New-Object byte[] 1024
        [void]$SourceStream.Read($buffer,0,$buffer.Length)
        $DestinationStream.Write($buffer,0,$buffer.Length);$DestinationStream.Flush($true)
        throw [IO.IOException]::new('injected transfer failure')
    }
}elseif($Scenario -ceq 'locked'){
    $held=[IO.File]::Open($target,'Open','Read','Read')
}elseif($Scenario -ceq 'refused5'){
    $script:firstRefusal=[ComponentModel.Win32Exception]::new(5)
    function Invoke-LabelAtomicReplace {throw $script:firstRefusal}
    function Invoke-LabelPinnedAtomicReplace {throw $script:firstRefusal}
}elseif($Scenario -ceq 'absent'){Remove-Item -LiteralPath $target}
$timer=[Diagnostics.Stopwatch]::StartNew()
try{Copy-LabelAtomicFile $source $target}catch{$failure=$_}finally{if($null -ne $held){$held.Dispose()}}
if((Sha $source) -cne $sourceHash){throw 'preimage changed'}
if($Scenario -cin @('partial','locked','refused5')){
    if($null -eq $failure -or (Sha $target) -cne $targetHash){throw 'failed copy changed live state'}
    if($Scenario -ceq 'locked' -and ($timer.Elapsed.TotalMilliseconds -lt 900 -or $timer.Elapsed.TotalSeconds -gt 3)){throw 'native retry bound differs'}
    if($Scenario -ceq 'refused5' -and $failure.Exception -ne $script:firstRefusal){throw 'first native refusal changed'}
    $temporary=@(Get-ChildItem -LiteralPath $Root -Filter 'live.tmp.*' -File)
    if($temporary.Count -ne 1){throw 'prepared evidence absent'}
    if($Scenario -ceq 'partial' -and $temporary[0].Length -ne 1024){throw 'partial transfer not exercised'}
}elseif($null -ne $failure -or (Sha $target) -cne $sourceHash){throw 'successful copy readback differs'}
'PASS'
