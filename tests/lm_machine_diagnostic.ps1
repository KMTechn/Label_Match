param([string]$HostRoot,[string]$FixtureRoot,[int]$ChildExit,[switch]$WriterFails)
$ErrorActionPreference='Stop'
$tokens=$null;$errors=$null
$tree=[Management.Automation.Language.Parser]::ParseFile((Join-Path $HostRoot 'INSTALL_CANONICAL_PORTABLE.ps1'),[ref]$tokens,[ref]$errors)
if($errors.Count){throw 'SOURCE_PARSE'}
$branch=@($tree.FindAll({param($n)$n -is [Management.Automation.Language.IfStatementAst] -and $n.Extent.Text.Contains('Machine code placement failed.')},$true))
$failureFunction=@($tree.FindAll({param($n)$n -is [Management.Automation.Language.FunctionDefinitionAst] -and $n.Name -ceq 'Get-LabelLaunchFailure'},$false))
$traps=@($tree.FindAll({param($n)$n -is [Management.Automation.Language.TrapStatementAst]},$false))
if($branch.Count -ne 1 -or $failureFunction.Count -ne 1 -or $traps.Count -ne 1){throw 'SOURCE_BOUNDARY_COUNT'}
. ([scriptblock]::Create($failureFunction[0].Extent.Text))
function ConvertTo-KmtechPublicRecord($fields){return $fields}
function ConvertTo-KmtechRedactedSinkObject($fields){return $fields}
function Write-KmtechProtectedFailureSidecar {param($Text)throw [IO.IOException]::new('test writer refusal')}
$Script:KmtechRedactionRules=@{safe_messages=@('현장 관리자에게 문의하세요.')}
$reference=if($WriterFails){$null}else{@{path='fixture-reference';sha256=('0'*64)}}
$Script:LabelPlacementChildFailure=@{placement_code='PLACEMENT_DIAGNOSTIC_UNAVAILABLE';phase='canonical_placement';exit_code=$ChildExit;full_exception_trace=$reference}
$audit=[ordered]@{};$auditFile=Join-Path $FixtureRoot 'audit.json'
function Save($Path,$Payload){[IO.File]::WriteAllText($Path,($Payload | ConvertTo-Json -Depth 10),[Text.UTF8Encoding]::new($false))}
# Exercise the product machine refusal, relevant catch/audit statements and
# the unchanged top-level trap. Native placement is the child-exit seam.
$machine=$branch[0].Parent
while($machine -isnot [Management.Automation.Language.FunctionDefinitionAst]){$machine=$machine.Parent}
$try=@($machine.Body.FindAll({param($n)$n -is [Management.Automation.Language.TryStatementAst] -and $n.Body.Extent.Text.Contains('InvokeFrozenPlacementHelper $frozen $parameters')},$true))
if($try.Count -ne 1){throw 'MACHINE_CATCH_COUNT'}
$catch=$try[0].CatchClauses[0].Body.Statements
$auditStatements=@($catch | Where-Object {$_.Extent.Text.StartsWith('$audit.status=') -or $_.Extent.Text.StartsWith('if ($originalFailure.Exception.Data.Contains')})
if($auditStatements.Count -ne 2){throw 'MACHINE_AUDIT_COUNT'}
$script=$traps[0].Extent.Text+"`n"+'try {'+"`n"+'$exit=$ChildExit'+"`n"+$branch[0].Extent.Text+"`n"+'} catch {'+"`n"+'$originalFailure=$_'+"`n"+($auditStatements.Extent.Text -join "`n")+"`n"+'Save $auditFile $audit'+"`n"+'throw $originalFailure'+"`n"+'}'
& ([scriptblock]::Create($script))
throw 'FALSE_SUCCESS'
