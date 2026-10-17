param([string]$Repo,[string]$Request,[string]$Root)
$ErrorActionPreference='Stop'
$requestValue=Get-Content -LiteralPath $Request -Raw | ConvertFrom-Json
$cases=$requestValue.cases
foreach($case in $cases) {
    $caseRoot=Join-Path $Root $case
    [void][IO.Directory]::CreateDirectory($caseRoot)
    try { & (Join-Path $Repo 'tests\l2_removal_proofs.ps1') -Repo $Repo -Case $case -Root $caseRoot -KitArguments ([string[]]$requestValue.kit_arguments) | Out-Null }
    catch { if(-not [IO.File]::Exists((Join-Path $caseRoot 'l2-result.json'))){throw 'L2_CASE_DID_NOT_REPORT'} }
    if(-not [IO.File]::Exists((Join-Path $caseRoot 'l2-result.json'))){throw 'L2_CASE_RESULT_MISSING'}
}
exit 0
