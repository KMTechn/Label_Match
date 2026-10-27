param([Parameter(Mandatory=$true)][string]$Root)
$ErrorActionPreference='Stop'
$tokens=$null;$errors=$null
$guard=[Management.Automation.Language.Parser]::ParseFile((Join-Path $Root 'tests/lm_atomic_guard.ps1'),[ref]$tokens,[ref]$errors)
if($errors.Count){throw 'atomic guard syntax is invalid'}
$functions=@($guard.FindAll({param($node) $node -is [Management.Automation.Language.FunctionDefinitionAst] -and $node.Name -ceq 'Get-AtomicPublicationSites'},$true))
if($functions.Count -ne 1){throw 'atomic guard compiler is ambiguous'}
. ([ScriptBlock]::Create($functions[0].Extent.Text))
$sourcePath=Join-Path $Root 'INSTALL_THIS_PC.ps1'
$source=[IO.File]::ReadAllText($sourcePath)
$sites=@(Get-AtomicPublicationSites $source | Where-Object {$_.function -ceq 'Restore-RemovalFilePreimage'})
if($sites.Count -ne 1){throw 'removal restore must contain exactly one atomic publication site'}
$tree=[Management.Automation.Language.Parser]::ParseFile($sourcePath,[ref]$tokens,[ref]$errors)
if($errors.Count){throw 'installer syntax is invalid'}
$calls=@($tree.FindAll({param($node)
    $node -is [Management.Automation.Language.InvokeMemberExpressionAst] -and
    $node.Extent.StartLineNumber -eq $sites[0].line -and
    $node.Expression -is [Management.Automation.Language.TypeExpressionAst] -and
    $node.Expression.TypeName.FullName -in @('IO.File','System.IO.File') -and
    $node.Member.Value -ceq 'Move' -and $node.Arguments.Count -eq 2
},$true))
if($calls.Count -ne 1){throw 'removal restore publication must be the approved file move'}
@{file='INSTALL_THIS_PC.ps1';function='Restore-RemovalFilePreimage';sha256=$sites[0].sha256;count=$sites.Count} | ConvertTo-Json -Compress
