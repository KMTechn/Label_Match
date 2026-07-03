param(
    [switch]$DryRun,
    [string]$ServerBaseUrl = "https://worker.kmtecherp.com",
    [string]$ProgramDataRoot = "",
    [string]$ScanSourceDir = "C:\ProgramData\KMTech\Label_Match\data",
    [string]$EnrollmentTokenFile = "",
    [string]$TaskName = "",
    [string]$TaskRunUser = "",
    [string]$TaskRunPasswordEnv = "",
    [string]$TaskRunPasswordFile = "",
    [switch]$AllowInteractiveTaskForLocalTest
)

$ErrorActionPreference = "Stop"

function Get-Sha256Hex([string]$Text) {
    $sha = [System.Security.Cryptography.SHA256]::Create()
    try {
        $bytes = [System.Text.Encoding]::UTF8.GetBytes($Text)
        $hash = $sha.ComputeHash($bytes)
        return -join ($hash | ForEach-Object { $_.ToString("x2") })
    }
    finally {
        $sha.Dispose()
    }
}

function Get-SafeToken([string]$Value, [string]$Fallback) {
    $text = if ([string]::IsNullOrWhiteSpace($Value)) { $Fallback } else { $Value.Trim() }
    $text = [regex]::Replace($text, '[^A-Za-z0-9._-]+', '-')
    $text = $text.Trim('.', '-', '_')
    if ([string]::IsNullOrWhiteSpace($text)) {
        $text = $Fallback
    }
    if ($text.Length -gt 96) {
        $text = $text.Substring(0, 96).Trim('.', '-', '_')
    }
    return $text
}

function Get-MachineStableSuffix() {
    $identity = ""
    try {
        $identity = (Get-ItemProperty -Path "HKLM:\SOFTWARE\Microsoft\Cryptography" -Name MachineGuid -ErrorAction Stop).MachineGuid
    }
    catch {
        $identity = "$env:COMPUTERNAME|$env:USERDOMAIN"
    }
    return (Get-Sha256Hex $identity).Substring(0, 12)
}

$safePcId = Get-SafeToken $env:COMPUTERNAME "worker-pc"
$sourceHostId = ("label-match-{0}-{1}" -f $safePcId, (Get-MachineStableSuffix)).ToLowerInvariant()
if ([string]::IsNullOrWhiteSpace($ProgramDataRoot)) {
    $ProgramDataRoot = "C:\ProgramData\KMTech\DirectSync\$sourceHostId"
}
if ([string]::IsNullOrWhiteSpace($TaskName)) {
    $TaskName = "direct-sync-relay-$sourceHostId"
}

function Write-Utf8JsonFile([string]$Path, $Payload) {
    $parent = Split-Path -Parent $Path
    if (-not [string]::IsNullOrWhiteSpace($parent)) {
        New-Item -ItemType Directory -Path $parent -Force | Out-Null
    }
    $json = $Payload | ConvertTo-Json -Depth 20
    $utf8NoBom = New-Object System.Text.UTF8Encoding($false)
    [System.IO.File]::WriteAllText($Path, $json + [System.Environment]::NewLine, $utf8NoBom)
}

function Set-LabelMatchSavePath([string]$AppRoot, [string]$TargetSaveDir) {
    $configDir = Join-Path $AppRoot "config"
    $settingsPath = Join-Path $configDir "app_settings.json"
    New-Item -ItemType Directory -Path $configDir -Force | Out-Null
    $payload = [ordered]@{}
    if (Test-Path -LiteralPath $settingsPath) {
        try {
            $existing = Get-Content -LiteralPath $settingsPath -Raw -Encoding UTF8 | ConvertFrom-Json
            foreach ($property in $existing.PSObject.Properties) {
                $payload[$property.Name] = $property.Value
            }
        }
        catch {
            $payload["settings_recreated_after_parse_error"] = $true
        }
    }
    $targetFull = [System.IO.Path]::GetFullPath($TargetSaveDir)
    $defaultFull = [System.IO.Path]::GetFullPath("C:\ProgramData\KMTech\Label_Match\data")
    $payload["custom_save_path"] = if ($targetFull.Equals($defaultFull, [System.StringComparison]::OrdinalIgnoreCase)) { "" } else { $targetFull }
    Write-Utf8JsonFile $settingsPath $payload
    return $settingsPath
}

function Resolve-ToolCommand([string]$ExePath, [string]$PythonScriptPath) {
    if (Test-Path -LiteralPath $ExePath) {
        return @($ExePath)
    }
    $python = Get-Command python -ErrorAction SilentlyContinue
    if (-not $python) {
        throw "Bundled tool executable is missing and Python is not installed. Missing: $ExePath"
    }
    return @($python.Source, $PythonScriptPath)
}

$appRoot = Split-Path -Parent $MyInvocation.MyCommand.Path
$toolsDir = Join-Path $appRoot "tools"
$installPackCommand = Resolve-ToolCommand `
    -ExePath (Join-Path $toolsDir "direct_sync_relay_install_pack.exe") `
    -PythonScriptPath (Join-Path $toolsDir "direct_sync_relay_install_pack.py")
$runnerExe = Join-Path $toolsDir "direct_sync_relay_runner.exe"
$registrationExe = Join-Path $toolsDir "register_label_match_worker_pc.exe"
$reportDir = Join-Path $ProgramDataRoot "status"
$reportPath = Join-Path $reportDir "label_match_direct_sync_install.json"

New-Item -ItemType Directory -Path $ScanSourceDir -Force | Out-Null
New-Item -ItemType Directory -Path $reportDir -Force | Out-Null
$settingsPath = Set-LabelMatchSavePath -AppRoot $appRoot -TargetSaveDir $ScanSourceDir

$arguments = @()
if ($installPackCommand.Count -gt 1) {
    $arguments += $installPackCommand[1]
}
$arguments += @(
    "--self-enroll",
    "--app-root", $appRoot,
    "--server-base-url", $ServerBaseUrl,
    "--program-data-root", $ProgramDataRoot,
    "--scan-source-dir", $ScanSourceDir,
    "--task-name", $TaskName,
    "--report-path", $reportPath
)
if (Test-Path -LiteralPath $runnerExe) {
    $arguments += @("--runner-exe", $runnerExe)
}
if (Test-Path -LiteralPath $registrationExe) {
    $arguments += @("--registration-exe", $registrationExe)
}
if (-not [string]::IsNullOrWhiteSpace($EnrollmentTokenFile)) {
    $arguments += @("--enrollment-token-file", $EnrollmentTokenFile)
}
if (-not [string]::IsNullOrWhiteSpace($TaskRunUser)) {
    $arguments += @("--task-run-user", $TaskRunUser)
}
if (-not [string]::IsNullOrWhiteSpace($TaskRunPasswordEnv)) {
    $arguments += @("--task-run-password-env", $TaskRunPasswordEnv)
}
if (-not [string]::IsNullOrWhiteSpace($TaskRunPasswordFile)) {
    $arguments += @("--task-run-password-file", $TaskRunPasswordFile)
}
if ($AllowInteractiveTaskForLocalTest.IsPresent) {
    $arguments += @("--allow-interactive-task-for-local-test")
}
if (-not $DryRun.IsPresent) {
    $arguments += @("--apply")
}

& $installPackCommand[0] @arguments
$exitCode = $LASTEXITCODE

$installReport = $null
if (Test-Path -LiteralPath $reportPath) {
    try {
        $installReport = Get-Content -LiteralPath $reportPath -Raw -Encoding UTF8 | ConvertFrom-Json
    }
    catch {
        $installReport = $null
    }
}
$registrationSummary = $null
if ($null -ne $installReport -and $null -ne $installReport.self_enrollment_registration) {
    $registrationSummary = $installReport.self_enrollment_registration.registration_report_summary
}

$summary = [ordered]@{
    installer_report_version = "label-match-direct-sync-one-step-install-v1"
    status = if ($exitCode -eq 0) { if ($DryRun.IsPresent) { "DRY_RUN" } else { "PASS" } } else { "BLOCKED" }
    blocked_reason = if ($null -ne $installReport) { $installReport.blocked_reason } else { $null }
    registration_blocked_reason = if ($null -ne $registrationSummary) { $registrationSummary.blocked_reason } else { $null }
    exit_code = $exitCode
    app_root = $appRoot
    settings_path = $settingsPath
    scan_source_dir = [System.IO.Path]::GetFullPath($ScanSourceDir)
    program_data_root = [System.IO.Path]::GetFullPath($ProgramDataRoot)
    install_pack_report_path = [System.IO.Path]::GetFullPath($reportPath)
    enrollment_token_file_present = -not [string]::IsNullOrWhiteSpace($EnrollmentTokenFile)
    bundled_runner_exe_present = Test-Path -LiteralPath $runnerExe
    bundled_registration_exe_present = Test-Path -LiteralPath $registrationExe
    task_name = $TaskName
    source_host_id = if ($null -ne $registrationSummary) { $registrationSummary.source_host_id } else { $null }
    producer_install_id = if ($null -ne $registrationSummary) { $registrationSummary.producer_install_id } else { $null }
    producer_id = if ($null -ne $registrationSummary) { $registrationSummary.producer_id } else { $null }
    key_id = if ($null -ne $registrationSummary) { $registrationSummary.key_id } else { $null }
    manual_pc_approval_required = if ($null -ne $registrationSummary) { $registrationSummary.manual_pc_approval_required } else { $null }
}
$summaryPath = Join-Path $reportDir "label_match_one_step_install_summary.json"
Write-Utf8JsonFile $summaryPath $summary

if ($exitCode -eq 0 -and -not $DryRun.IsPresent) {
    try {
        Start-ScheduledTask -TaskName $TaskName -ErrorAction SilentlyContinue
    }
    catch {
        # The scheduled task still exists and runs on its interval; startup is best-effort.
    }
}

exit $exitCode
