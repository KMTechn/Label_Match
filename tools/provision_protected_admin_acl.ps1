[CmdletBinding()]
param(
    [string]$ReaderPrincipal = "",
    [string]$ProfilePath = "",
    [switch]$Replace,
    [switch]$DryRun
)

$ErrorActionPreference = "Stop"
$installerPath = Join-Path $PSScriptRoot "Label_Match_Protected_Admin_Install.exe"
if (-not (Test-Path -LiteralPath $installerPath -PathType Leaf)) {
    throw "Protected administrator installer is missing from the release directory."
}
if ($DryRun -and $Replace) {
    throw "Replace cannot be combined with DryRun."
}
if (-not $DryRun) {
    if ([string]::IsNullOrWhiteSpace($ReaderPrincipal)) {
        throw "ReaderPrincipal must identify the narrow Windows user that runs Label Match."
    }
    $identity = [System.Security.Principal.WindowsIdentity]::GetCurrent()
    $windowsPrincipal = [System.Security.Principal.WindowsPrincipal]::new($identity)
    if (-not $windowsPrincipal.IsInRole([System.Security.Principal.WindowsBuiltInRole]::Administrator)) {
        throw "Protected administrator provisioning requires an elevated PowerShell session."
    }
}

$installerArguments = @()
if ($DryRun) {
    $installerArguments += "--dry-run"
}
else {
    $installerArguments += @("--reader-principal", $ReaderPrincipal)
}
if (-not [string]::IsNullOrWhiteSpace($ProfilePath)) {
    $installerArguments += @("--profile-path", $ProfilePath)
}
if ($Replace) {
    $installerArguments += "--replace"
}

& $installerPath @installerArguments
if ($LASTEXITCODE -ne 0) {
    throw "Protected administrator installer failed with exit code $LASTEXITCODE."
}

$mode = if ($DryRun) { "dry-run" } else { "installed" }
Write-Output "protected_admin_provision=PASS mode=$mode"
