#Requires -RunAsAdministrator

param(
    [switch]$DryRun,
    [switch]$AllowNoncanonicalLayoutForTest,
    [string]$ServerBaseUrl = "https://worker.kmtecherp.com",
    [string]$ProgramDataRoot = "",
    [string]$ScanSourceDir = "C:\ProgramData\KMTech\Label_Match\data",
    [string]$EnrollmentTokenFile = "",
    [string]$ExistingProducerManifestPath = "",
    [string]$ExistingCredentialPath = "",
    [string]$TaskName = "",
    [string]$LogisticsProfilePath = "C:\ProgramData\KMTech\Logistics\profiles\Label_Match\runtime-profile.json",
    [string]$TaskRunUser = "",
    [string]$AppRunUser = "*S-1-5-32-545",
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
    $ProgramDataRoot = "C:\ProgramData\KMTech\DirectSync\label_match"
}
if ([string]::IsNullOrWhiteSpace($TaskName)) {
    $TaskName = "direct-sync-relay-label-match"
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

function Remove-NewMachineProfilesFromRegistrationReport(
    [string]$RegistrationReportPath,
    [string]$ExpectedLogisticsProfilePath
) {
    if (-not (Test-Path -LiteralPath $RegistrationReportPath -PathType Leaf)) {
        return
    }
    $payload = Get-Content -LiteralPath $RegistrationReportPath -Raw -Encoding UTF8 | ConvertFrom-Json
    if ($null -eq $payload.machine_profiles) {
        return
    }
    $profilePath = [System.IO.Path]::GetFullPath($ExpectedLogisticsProfilePath)
    $profileRoot = Split-Path -Parent $profilePath
    $allowed = @(
        $profilePath,
        [System.IO.Path]::GetFullPath((Join-Path $profileRoot "secrets\bearer-token.dpapi"))
    )
    foreach ($property in $payload.machine_profiles.PSObject.Properties) {
        $profile = $property.Value
        if ([string]$profile.status -cne "installed") {
            continue
        }
        foreach ($createdPath in @($profile.created_paths)) {
            $fullPath = [System.IO.Path]::GetFullPath([string]$createdPath)
            if ($allowed -notcontains $fullPath) {
                throw "Refusing to roll back an unexpected machine profile path."
            }
            if (Test-Path -LiteralPath $fullPath -PathType Leaf) {
                Remove-Item -LiteralPath $fullPath -Force -ErrorAction Stop
            }
        }
    }
}

function Test-SamePath([string]$Left, [string]$Right) {
    $leftFull = [System.IO.Path]::GetFullPath($Left).TrimEnd('\')
    $rightFull = [System.IO.Path]::GetFullPath($Right).TrimEnd('\')
    return $leftFull.Equals($rightFull, [System.StringComparison]::OrdinalIgnoreCase)
}

function Set-LabelMatchSavePath([string]$AppRoot, [string]$TargetSaveDir) {
    $rootSettingsPath = Join-Path $AppRoot "config\app_settings.json"
    $internalRoot = Join-Path $AppRoot "_internal"
    $settingsPaths = if (Test-Path -LiteralPath $internalRoot -PathType Container) {
        @((Join-Path $internalRoot "config\app_settings.json"), $rootSettingsPath)
    }
    else {
        @($rootSettingsPath)
    }
    $settingsPath = $settingsPaths[0]
    $payload = [ordered]@{}
    $existingSettingsPath = $settingsPaths | Where-Object { Test-Path -LiteralPath $_ -PathType Leaf } | Select-Object -First 1
    if (-not [string]::IsNullOrWhiteSpace($existingSettingsPath)) {
        try {
            $existing = Get-Content -LiteralPath $existingSettingsPath -Raw -Encoding UTF8 | ConvertFrom-Json
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
    foreach ($targetPath in $settingsPaths) {
        Write-Utf8JsonFile $targetPath $payload
    }
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

function Resolve-PythonExe() {
    $candidates = @(
        $env:KMTECH_PYTHON_EXE,
        "C:\Program Files\Python312\python.exe",
        "C:\Program Files\Python314\python.exe"
    )
    $python = Get-Command python -ErrorAction SilentlyContinue
    if ($python) {
        $candidates += $python.Source
    }
    foreach ($candidate in $candidates) {
        if (-not [string]::IsNullOrWhiteSpace($candidate) -and (Test-Path -LiteralPath $candidate)) {
            return [System.IO.Path]::GetFullPath($candidate)
        }
    }
    throw "Python is required for the Label_Match direct-sync relay runner, but python.exe was not found."
}

$appRoot = Split-Path -Parent $MyInvocation.MyCommand.Path
$toolsDir = Join-Path $appRoot "tools"
$installPackCommand = @(
    Resolve-ToolCommand `
        -ExePath (Join-Path $toolsDir "direct_sync_relay_install_pack\direct_sync_relay_install_pack.exe") `
        -PythonScriptPath (Join-Path $toolsDir "direct_sync_relay_install_pack.py")
)
$runnerExe = Join-Path $toolsDir "direct_sync_relay_runner.exe"
$runnerScript = Join-Path $toolsDir "direct_sync_relay_runner.py"
$registrationExe = Join-Path $toolsDir "register_label_match_worker_pc.exe"
$runnerExeAvailable = Test-Path -LiteralPath $runnerExe -PathType Leaf
$registrationExeAvailable = Test-Path -LiteralPath $registrationExe -PathType Leaf
$pythonExe = ""
if (-not $runnerExeAvailable -or -not $registrationExeAvailable) {
    $pythonExe = Resolve-PythonExe
}
$reportDir = Join-Path $ProgramDataRoot "status"
$reportPath = Join-Path $reportDir "label_match_direct_sync_install.json"
$registrationReportPath = Join-Path $reportDir "label_match_worker_pc_registration.json"
$expectedInstallRoot = "C:\KMTech\Apps\Label_Match\current"
$expectedDirectSyncRoot = "C:\ProgramData\KMTech\DirectSync\label_match"
$expectedTaskName = "direct-sync-relay-label-match"
$expectedTaskLauncherPath = Join-Path $expectedDirectSyncRoot "bin\run_direct-sync-relay-label-match.vbs"
$expectedStateDbPath = Join-Path $expectedDirectSyncRoot "queue\direct_sync_relay.sqlite3"
$actualInstallRoot = [System.IO.Path]::GetFullPath($appRoot)
$actualDirectSyncRoot = [System.IO.Path]::GetFullPath($ProgramDataRoot)
$actualTaskLauncherPath = Join-Path $actualDirectSyncRoot ("bin\run_{0}.vbs" -f $TaskName)
$actualStateDbPath = Join-Path $actualDirectSyncRoot "queue\direct_sync_relay.sqlite3"
$installRootMatches = Test-SamePath $actualInstallRoot $expectedInstallRoot
$directSyncRootMatches = Test-SamePath $actualDirectSyncRoot $expectedDirectSyncRoot
$taskNameMatches = $TaskName -ceq $expectedTaskName
$taskLauncherPathMatches = Test-SamePath $actualTaskLauncherPath $expectedTaskLauncherPath
$stateDbPathMatches = Test-SamePath $actualStateDbPath $expectedStateDbPath
$localTestOverrideEnabled = (
    $AllowNoncanonicalLayoutForTest.IsPresent -and
    [string]$env:KMTECH_FACTORY_INSTALL_TEST_MODE -ceq "1"
)
$productionLayoutMatches = (
    $installRootMatches -and
    $directSyncRootMatches -and
    $taskNameMatches -and
    $taskLauncherPathMatches -and
    $stateDbPathMatches
)
$fieldLayoutContract = [ordered]@{
    status = if ($productionLayoutMatches) { "PASS" } else { "MISMATCH" }
    expected_install_root = $expectedInstallRoot
    actual_install_root = $actualInstallRoot
    expected_direct_sync_root = $expectedDirectSyncRoot
    actual_direct_sync_root = $actualDirectSyncRoot
    expected_task_name = $expectedTaskName
    actual_task_name = $TaskName
    expected_task_launcher_path = $expectedTaskLauncherPath
    actual_task_launcher_path = $actualTaskLauncherPath
    expected_state_db_path = $expectedStateDbPath
    actual_state_db_path = $actualStateDbPath
    install_root_matches = $installRootMatches
    direct_sync_root_matches = $directSyncRootMatches
    task_name_matches = $taskNameMatches
    task_launcher_path_matches = $taskLauncherPathMatches
    state_db_path_matches = $stateDbPathMatches
    production_layout_matches = $productionLayoutMatches
    local_test_override_requested = $AllowNoncanonicalLayoutForTest.IsPresent
    local_test_override_enabled = $localTestOverrideEnabled
    production_apply_allowed = $productionLayoutMatches
}

if (-not $DryRun.IsPresent -and -not $productionLayoutMatches -and -not $localTestOverrideEnabled) {
    $blockedPlan = [ordered]@{
        report_version = "label-match-direct-sync-install-pack-v1"
        status = "BLOCKED"
        blocked_reason = if ($AllowNoncanonicalLayoutForTest.IsPresent) { "noncanonical layout override requires KMTECH_FACTORY_INSTALL_TEST_MODE=1" } else { "production install requires the canonical Label_Match field layout" }
        apply = $true
        uninstall = $false
        field_layout_contract = $fieldLayoutContract
    }
    Write-Utf8JsonFile $reportPath $blockedPlan
    Write-Utf8JsonFile (Join-Path $reportDir "label_match_one_step_install_summary.json") ([ordered]@{
        installer_report_version = "label-match-direct-sync-one-step-install-v1"
        status = "BLOCKED"
        blocked_reason = $blockedPlan.blocked_reason
        exit_code = 2
        source_host_id = $sourceHostId
        field_layout_contract = $fieldLayoutContract
    })
    exit 2
}

$reuseExistingIdentity = (
    -not [string]::IsNullOrWhiteSpace($ExistingProducerManifestPath) -or
    -not [string]::IsNullOrWhiteSpace($ExistingCredentialPath)
)
if ($reuseExistingIdentity) {
    if (
        [string]::IsNullOrWhiteSpace($ExistingProducerManifestPath) -or
        [string]::IsNullOrWhiteSpace($ExistingCredentialPath)
    ) {
        throw "ExistingProducerManifestPath and ExistingCredentialPath must be provided together."
    }
    foreach ($existingPath in @($ExistingProducerManifestPath, $ExistingCredentialPath)) {
        if (-not (Test-Path -LiteralPath $existingPath -PathType Leaf)) {
            throw "Existing registered identity file does not exist."
        }
    }
    if (-not [string]::IsNullOrWhiteSpace($EnrollmentTokenFile)) {
        throw "EnrollmentTokenFile cannot be combined with existing registered identity files."
    }
}

New-Item -ItemType Directory -Path $ScanSourceDir -Force | Out-Null
New-Item -ItemType Directory -Path $reportDir -Force | Out-Null
$settingsPath = Set-LabelMatchSavePath -AppRoot $appRoot -TargetSaveDir $ScanSourceDir

$arguments = @()
if ($installPackCommand.Count -gt 1) {
    $arguments += $installPackCommand[1]
}
$arguments += @(
    "--app-root", $appRoot,
    "--server-base-url", $ServerBaseUrl,
    "--program-data-root", $ProgramDataRoot,
    "--scan-source-dir", $ScanSourceDir,
    "--source-host-id", $sourceHostId,
    "--app-run-user", $AppRunUser,
    "--task-name", $TaskName,
    "--report-path", $reportPath,
    "--app-settings-path", $settingsPath
)
if ($reuseExistingIdentity) {
    $arguments += @(
        "--producer-manifest-path", $ExistingProducerManifestPath,
        "--credential-path", $ExistingCredentialPath
    )
}
else {
    $arguments += @(
        "--self-enroll",
        "--require-machine-credential-bundle",
        "--logistics-profile-path", $LogisticsProfilePath
    )
}
if (-not [string]::IsNullOrWhiteSpace($pythonExe)) {
    $arguments += @("--python-exe", $pythonExe)
}
if ($runnerExeAvailable) {
    $arguments += @("--runner-exe", $runnerExe)
}
elseif (-not (Test-Path -LiteralPath $runnerScript -PathType Leaf)) {
    throw "Python relay runner script is missing. Missing: $runnerScript"
}
if ($registrationExeAvailable) {
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
if ($AllowNoncanonicalLayoutForTest.IsPresent) {
    $arguments += @("--allow-noncanonical-layout-for-test")
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

$taskStartStatus = "NOT_RUN"
$taskStartError = $null
if ($exitCode -eq 0 -and -not $DryRun.IsPresent) {
    try {
        Start-ScheduledTask -TaskName $TaskName -ErrorAction Stop
        $taskStartStatus = "STARTED"
    }
    catch {
        $taskStartStatus = "FAILED"
        $taskStartError = $_.Exception.Message
        $exitCode = 1
        Unregister-ScheduledTask -TaskName $TaskName -Confirm:$false -ErrorAction SilentlyContinue
        try {
            Remove-NewMachineProfilesFromRegistrationReport $registrationReportPath $LogisticsProfilePath
        }
        catch {
            $taskStartError += "; machine profile rollback failed: $($_.Exception.Message)"
        }
    }
}

$summary = [ordered]@{
    installer_report_version = "label-match-direct-sync-one-step-install-v1"
    status = if ($exitCode -eq 0) { if ($DryRun.IsPresent) { "DRY_RUN" } else { "PASS" } } else { "BLOCKED" }
    blocked_reason = if ($taskStartStatus -eq "FAILED") { "scheduled task immediate start failed" } elseif ($null -ne $installReport) { $installReport.blocked_reason } else { $null }
    registration_blocked_reason = if ($null -ne $registrationSummary) { $registrationSummary.blocked_reason } else { $null }
    exit_code = $exitCode
    app_root = $appRoot
    settings_path = $settingsPath
    scan_source_dir = [System.IO.Path]::GetFullPath($ScanSourceDir)
    program_data_root = [System.IO.Path]::GetFullPath($ProgramDataRoot)
    logistics_profile_path = [System.IO.Path]::GetFullPath($LogisticsProfilePath)
    install_pack_report_path = [System.IO.Path]::GetFullPath($reportPath)
    enrollment_token_file_present = -not [string]::IsNullOrWhiteSpace($EnrollmentTokenFile)
    existing_identity_reused = $reuseExistingIdentity
    bundled_runner_exe_present = Test-Path -LiteralPath $runnerExe
    python_runner_script_present = Test-Path -LiteralPath $runnerScript
    python_exe = $pythonExe
    bundled_registration_exe_present = Test-Path -LiteralPath $registrationExe
    task_name = $TaskName
    field_layout_contract = if ($null -ne $installReport -and $null -ne $installReport.field_layout_contract) { $installReport.field_layout_contract } else { $fieldLayoutContract }
    scheduled_task_start = [ordered]@{
        status = $taskStartStatus
        error = $taskStartError
    }
    app_run_user = $AppRunUser
    app_runtime_acl = if ($null -ne $installReport) { $installReport.app_runtime_acl } else { $null }
    source_host_id = if ($null -ne $registrationSummary) { $registrationSummary.source_host_id } else { $sourceHostId }
    producer_install_id = if ($null -ne $registrationSummary) { $registrationSummary.producer_install_id } else { $null }
    producer_id = if ($null -ne $registrationSummary) { $registrationSummary.producer_id } else { $null }
    key_id = if ($null -ne $registrationSummary) { $registrationSummary.key_id } else { $null }
    manual_pc_approval_required = if ($null -ne $registrationSummary) { $registrationSummary.manual_pc_approval_required } else { $null }
}
$summaryPath = Join-Path $reportDir "label_match_one_step_install_summary.json"
Write-Utf8JsonFile $summaryPath $summary

exit $exitCode
