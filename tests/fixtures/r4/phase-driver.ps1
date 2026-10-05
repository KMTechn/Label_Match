param([string]$Repo,[string]$Output,[ValidateSet('success','parent_failure','child_failure')][string]$Case)
$ErrorActionPreference='Stop'
$ExtractedFunctionNames=@('Invoke-LabelRollbackStep','Get-LabelLaunchFailure',
    'Protect-LabelFailureText')
$tokens=$null;$errors=$null
$ast=[Management.Automation.Language.Parser]::ParseFile((Join-Path $Repo 'INSTALL_CANONICAL_PORTABLE.ps1'),[ref]$tokens,[ref]$errors)
if($errors.Count){throw 'R4 product parse failure'}
foreach($name in $ExtractedFunctionNames){
    $nodes=@($ast.FindAll({param($n)$n -is [Management.Automation.Language.FunctionDefinitionAst] -and $n.Name -ceq $name},$true))
    if($nodes.Count -ne 1){throw ('R4 extraction ambiguous: '+$name)}
    . ([scriptblock]::Create($nodes[0].Extent.Text))
}
$Script:LabelRollbackErrors=[Collections.Generic.List[object]]::new()
$Script:LabelRollbackFailures=[Collections.Generic.List[object]]::new()
$Script:LabelRollbackSteps=[Collections.Generic.List[object]]::new()
$writerDelegationToken=$null
. Invoke-LabelRollbackStep 'parent' {
    . Invoke-LabelRollbackStep 'child_one' { if($Case -ceq 'child_failure'){throw 'R4 synthetic child failure'} }
    . Invoke-LabelRollbackStep 'child_two' { }
    if($Case -ceq 'parent_failure'){throw 'R4 synthetic parent failure'}
}
[ordered]@{steps=$Script:LabelRollbackSteps.ToArray();failure_phases=@($Script:LabelRollbackFailures | ForEach-Object phase)} |
    ConvertTo-Json -Depth 8 | Set-Content -LiteralPath $Output -Encoding UTF8
