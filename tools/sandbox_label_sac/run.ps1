param(
    [Parameter(Mandatory = $true)][string]$EvidenceRoot,
    [Parameter(Mandatory = $true)][string]$PositiveRoot,
    [Parameter(Mandatory = $true)][string]$PositiveInventoryPath,
    [Parameter(Mandatory = $true)][string]$ExpectedPositiveCommit,
    [Parameter(Mandatory = $true)][string]$ExpectedPositiveManifestSha256,
    [Parameter(Mandatory = $true)][string]$ExpectedPositiveInventorySha256,
    [Parameter(Mandatory = $true)][string]$NegativeRoot,
    [Parameter(Mandatory = $true)][string]$ExpectedNegativeSha256,
    [string]$MoveScript = 'E:\KMTech\autoloop-20260824\TASKSPEC\Move-ToTestMonitor.ps1'
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'
$ProgressPreference = 'SilentlyContinue'

$harnessRoot = Split-Path -Parent $MyInvocation.MyCommand.Path
$guestScript = Join-Path $harnessRoot 'guest.ps1'
$negativeExe = Join-Path $negativeRoot 'Label_Match.exe'
$positiveManifestPath = Join-Path $PositiveRoot 'portable-manifest.json'
$completePath = Join-Path $EvidenceRoot 'complete.marker'
$summaryPath = Join-Path $EvidenceRoot 'summary.json'
$hostRunPath = Join-Path $EvidenceRoot 'host-run.json'
$inputBindingPath = Join-Path $EvidenceRoot 'input-binding.json'
$moveLogPath = Join-Path $EvidenceRoot 'move-to-display3.log'
$moveErrorPath = Join-Path $EvidenceRoot 'move-to-display3.stderr.log'
$wsbPath = Join-Path $EvidenceRoot 'seq296-label-sac.wsb'
$utf8 = [Text.UTF8Encoding]::new($false)
$sandboxNames = @('WindowsSandbox', 'WindowsSandboxClient', 'WindowsSandboxServer', 'WindowsSandboxRemoteSession')

function Write-BoundedJson {
    param([Parameter(Mandatory = $true)][string]$Path, [Parameter(Mandatory = $true)][object]$Value)
    $json = $Value | ConvertTo-Json -Depth 20
    if ($json.Length -gt 8388608) { throw ('Refusing JSON larger than 8 MiB: {0}' -f $Path) }
    [IO.File]::WriteAllText($Path, ($json + [Environment]::NewLine), $utf8)
}

function Get-SandboxProcesses {
    return @(Get-Process -ErrorAction SilentlyContinue | Where-Object { $sandboxNames -contains $_.ProcessName })
}

function Get-HostSacState {
    return [int64](Get-ItemPropertyValue -LiteralPath 'HKLM:\SYSTEM\CurrentControlSet\Control\CI\Policy' -Name VerifiedAndReputablePolicyState)
}

foreach ($digest in @($ExpectedPositiveManifestSha256, $ExpectedPositiveInventorySha256, $ExpectedNegativeSha256)) {
    if ($digest -cnotmatch '^[0-9a-fA-F]{64}$') { throw 'Every artifact SHA-256 binding must be exact 64-hex.' }
}
if ($ExpectedPositiveCommit -cnotmatch '^[0-9a-fA-F]{40}$') { throw 'Expected positive commit must be exact 40-hex.' }
$ExpectedPositiveManifestSha256 = $ExpectedPositiveManifestSha256.ToLowerInvariant()
$ExpectedPositiveInventorySha256 = $ExpectedPositiveInventorySha256.ToLowerInvariant()
$ExpectedNegativeSha256 = $ExpectedNegativeSha256.ToLowerInvariant()
$ExpectedPositiveCommit = $ExpectedPositiveCommit.ToLowerInvariant()

foreach ($requiredFile in @($guestScript, $positiveInventoryPath, $positiveManifestPath, $negativeExe, $moveScript)) {
    if (-not (Test-Path -LiteralPath $requiredFile -PathType Leaf)) { throw ('Required file is missing: {0}' -f $requiredFile) }
}
foreach ($requiredDirectory in @($harnessRoot, $positiveRoot, $negativeRoot)) {
    if (-not (Test-Path -LiteralPath $requiredDirectory -PathType Container)) { throw ('Required directory is missing: {0}' -f $requiredDirectory) }
}
if (@(Get-SandboxProcesses).Count -ne 0) { throw 'Refusing to mix this proof with an existing Windows Sandbox session.' }
if (Test-Path -LiteralPath $EvidenceRoot) {
    if (@(Get-ChildItem -LiteralPath $EvidenceRoot -Force).Count -ne 0) {
        throw ('Evidence root is not empty: {0}' -f $EvidenceRoot)
    }
}
else { New-Item -ItemType Directory -Path $EvidenceRoot | Out-Null }

$hostStateBefore = Get-HostSacState
if ($hostStateBefore -ne 0) { throw ('Host SAC state must remain 0, got {0}.' -f $hostStateBefore) }
$inventory = Get-Content -LiteralPath $positiveInventoryPath -Raw | ConvertFrom-Json
if ($inventory.pe_count -ne 46 -or $inventory.valid_count -ne 46 -or $inventory.unsigned_count -ne 0 -or $inventory.other_status_count -ne 0) {
    throw ('Host portable PE inventory does not match 46/46/0/0: {0}/{1}/{2}/{3}' -f $inventory.pe_count, $inventory.valid_count, $inventory.unsigned_count, $inventory.other_status_count)
}
if (@($inventory.unsigned_paths).Count -ne 0) { throw 'Host portable PE inventory has unexpected unsigned paths.' }
$inventoryHash = (Get-FileHash -LiteralPath $PositiveInventoryPath -Algorithm SHA256).Hash.ToLowerInvariant()
if ($inventoryHash -cne $ExpectedPositiveInventorySha256) { throw 'Positive PE inventory SHA-256 binding mismatch.' }
$manifestHash = (Get-FileHash -LiteralPath $positiveManifestPath -Algorithm SHA256).Hash.ToLowerInvariant()
if ($manifestHash -cne $ExpectedPositiveManifestSha256) { throw 'Positive portable manifest SHA-256 binding mismatch.' }
$manifest = Get-Content -LiteralPath $positiveManifestPath -Raw | ConvertFrom-Json
if ([string]$manifest.source_commit -cne $ExpectedPositiveCommit) { throw 'Positive portable source commit binding mismatch.' }
$negativeHash = (Get-FileHash -LiteralPath $negativeExe -Algorithm SHA256).Hash.ToLowerInvariant()
if ($negativeHash -cne $expectedNegativeSha256) { throw 'Frozen Label negative-control identity mismatch.' }

$inputBinding = [pscustomobject][ordered]@{
    schema = 'label-sac-input-binding-v1'
    positive_source_commit = $ExpectedPositiveCommit
    positive_manifest_sha256 = $ExpectedPositiveManifestSha256
    positive_inventory_sha256 = $ExpectedPositiveInventorySha256
    negative_executable_sha256 = $ExpectedNegativeSha256
}
Write-BoundedJson -Path $inputBindingPath -Value $inputBinding

$wsb = @"
<Configuration>
  <VGpu>Disable</VGpu>
  <Networking>Disable</Networking>
  <AudioInput>Disable</AudioInput>
  <VideoInput>Disable</VideoInput>
  <PrinterRedirection>Disable</PrinterRedirection>
  <ClipboardRedirection>Disable</ClipboardRedirection>
  <MemoryInMB>6144</MemoryInMB>
  <MappedFolders>
    <MappedFolder>
      <HostFolder>$([Security.SecurityElement]::Escape($harnessRoot))</HostFolder>
      <SandboxFolder>C:\Seq296\guest</SandboxFolder>
      <ReadOnly>true</ReadOnly>
    </MappedFolder>
    <MappedFolder>
      <HostFolder>$([Security.SecurityElement]::Escape($negativeRoot))</HostFolder>
      <SandboxFolder>C:\Seq296\mapped-negative</SandboxFolder>
      <ReadOnly>true</ReadOnly>
    </MappedFolder>
    <MappedFolder>
      <HostFolder>$([Security.SecurityElement]::Escape($positiveRoot))</HostFolder>
      <SandboxFolder>C:\Seq296\mapped-positive</SandboxFolder>
      <ReadOnly>true</ReadOnly>
    </MappedFolder>
    <MappedFolder>
      <HostFolder>$([Security.SecurityElement]::Escape($EvidenceRoot))</HostFolder>
      <SandboxFolder>C:\Seq296\evidence</SandboxFolder>
      <ReadOnly>false</ReadOnly>
    </MappedFolder>
  </MappedFolders>
  <LogonCommand>
    <Command>powershell.exe -NoProfile -NonInteractive -ExecutionPolicy Bypass -File C:\Seq296\guest\guest.ps1</Command>
  </LogonCommand>
</Configuration>
"@
[IO.File]::WriteAllText($wsbPath, $wsb, $utf8)

$sandboxExecutable = (Get-Command WindowsSandbox.exe -ErrorAction Stop).Source
$requestedAt = [DateTimeOffset]::UtcNow
$launched = Start-Process -FilePath $sandboxExecutable -ArgumentList ('"' + $wsbPath + '"') -PassThru
$trackedIds = @($launched.Id)
$windowProcess = $null
$windowDeadline = [DateTimeOffset]::UtcNow.AddSeconds(45)
while ($null -eq $windowProcess -and [DateTimeOffset]::UtcNow -lt $windowDeadline) {
    $newProcesses = @(Get-SandboxProcesses)
    $trackedIds = @($trackedIds + @($newProcesses | ForEach-Object Id) | Sort-Object -Unique)
    $windowProcess = @(
        $newProcesses |
            Where-Object {
                try {
                    $_.Refresh()
                    $_.ProcessName -eq 'WindowsSandboxRemoteSession' -and $_.MainWindowHandle -ne 0
                }
                catch { $false }
            } |
            Sort-Object StartTime |
            Select-Object -First 1
    ) | Select-Object -First 1
    if ($null -eq $windowProcess) { Start-Sleep -Milliseconds 200 }
}
if ($null -eq $windowProcess) { throw 'Windows Sandbox remote-session window PID was not found within 45 seconds.' }

# Required no-focus DISPLAY3 placement begins immediately after the remote UI appears.
$moveArguments = @(
    '-NoProfile',
    '-ExecutionPolicy', 'Bypass',
    '-File', $moveScript,
    '-ProcessId', [string]$windowProcess.Id,
    '-WatchSec', '300'
)
$moveRequestedAt = [DateTimeOffset]::UtcNow
$moveHelper = Start-Process -FilePath 'powershell.exe' -ArgumentList $moveArguments -PassThru -WindowStyle Hidden -RedirectStandardOutput $moveLogPath -RedirectStandardError $moveErrorPath

$deadline = $requestedAt.AddMinutes(12)
while (-not (Test-Path -LiteralPath $completePath -PathType Leaf) -and [DateTimeOffset]::UtcNow -lt $deadline) {
    $new = @(Get-SandboxProcesses | Where-Object { $trackedIds -notcontains $_.Id })
    $trackedIds = @($trackedIds + @($new | ForEach-Object Id) | Sort-Object -Unique)
    Start-Sleep -Milliseconds 500
}
if (-not (Test-Path -LiteralPath $completePath -PathType Leaf)) { throw 'Sandbox guest did not produce completion evidence within twelve minutes.' }
if ((Get-Item -LiteralPath $completePath).Length -gt 4096) { throw 'Completion marker is unexpectedly large.' }
if (-not (Test-Path -LiteralPath $summaryPath -PathType Leaf)) { throw 'Summary is missing after completion marker.' }
if ((Get-Item -LiteralPath $summaryPath).Length -gt 1048576) { throw 'Summary is unexpectedly large.' }
$summary = Get-Content -LiteralPath $summaryPath -Raw | ConvertFrom-Json

$shutdownDeadline = [DateTimeOffset]::UtcNow.AddSeconds(45)
do {
    $remaining = @(Get-SandboxProcesses | Where-Object { $trackedIds -contains $_.Id })
    if ($remaining.Count -eq 0) { break }
    Start-Sleep -Milliseconds 500
} while ([DateTimeOffset]::UtcNow -lt $shutdownDeadline)
$forced = @()
foreach ($process in @(Get-SandboxProcesses | Where-Object { $trackedIds -contains $_.Id })) {
    Stop-Process -Id $process.Id -Force -ErrorAction Stop
    $forced += $process.Id
}

$moveDeadline = [DateTimeOffset]::UtcNow.AddSeconds(10)
while (-not $moveHelper.HasExited -and [DateTimeOffset]::UtcNow -lt $moveDeadline) {
    Start-Sleep -Milliseconds 200
    $moveHelper.Refresh()
}
if (-not $moveHelper.HasExited) {
    $moveProcess = Get-CimInstance Win32_Process -Filter "ProcessId=$($moveHelper.Id)" -ErrorAction SilentlyContinue
    if ($moveProcess -and $moveProcess.CommandLine -like '*Move-ToTestMonitor.ps1*') {
        Stop-Process -Id $moveHelper.Id -Force -ErrorAction Stop
    }
}

$hostStateAfter = Get-HostSacState
$moveLog = ''
if ((Test-Path -LiteralPath $moveLogPath -PathType Leaf) -and (Get-Item -LiteralPath $moveLogPath).Length -le 1048576) {
    $moveLog = Get-Content -LiteralPath $moveLogPath -Raw
}
$moveSucceeded = $moveLog -match 'MOVED hwnd=.*DISPLAY3'
$hostRun = [pscustomobject][ordered]@{
    schema = 'seq296-label-sandbox-host/v1'
    requested_at_utc = $requestedAt.ToString('o')
    completed_at_utc = [DateTimeOffset]::UtcNow.ToString('o')
    sandbox_executable = $sandboxExecutable
    wsb_path = $wsbPath
    launch_process_id = $launched.Id
    move_target_process_id = $windowProcess.Id
    move_helper_process_id = $moveHelper.Id
    move_requested_at_utc = $moveRequestedAt.ToString('o')
    move_script = $moveScript
    move_watch_seconds = 300
    move_log_path = $moveLogPath
    move_succeeded = $moveSucceeded
    summary_verdict = $summary.verdict
    input_binding = $inputBinding
    host_sac_state_before = $hostStateBefore
    host_sac_state_after = $hostStateAfter
    host_sac_unchanged_zero = ($hostStateBefore -eq 0 -and $hostStateAfter -eq 0)
    negative_control_sha256 = $negativeHash
    candidate_pe_count = $inventory.pe_count
    candidate_valid_count = $inventory.valid_count
    candidate_unsigned_count = $inventory.unsigned_count
    candidate_other_status_count = $inventory.other_status_count
    forced_cleanup_process_ids = $forced
}
Write-BoundedJson -Path $hostRunPath -Value $hostRun
if (-not $hostRun.host_sac_unchanged_zero) { throw 'Host SAC state changed unexpectedly.' }
if (-not $hostRun.move_succeeded) { throw 'Windows Sandbox remote window was not proven moved to DISPLAY3.' }
if ($summary.verdict -cne 'PASS_SAC_ENFORCE_LABEL_NEGATIVE_THEN_PORTABLE_GUI') {
    throw ('Sandbox verdict failed: {0}' -f $summary.verdict)
}
if (
    [string]$summary.input_binding.positive_source_commit -cne $ExpectedPositiveCommit -or
    [string]$summary.input_binding.positive_manifest_sha256 -cne $ExpectedPositiveManifestSha256 -or
    [string]$summary.input_binding.negative_executable_sha256 -cne $ExpectedNegativeSha256
) { throw 'Sandbox summary input binding differs from the host preflight.' }
Write-Output ('VERDICT={0}' -f $summary.verdict)
Write-Output ('NEGATIVE_BLOCK_PROVEN={0}' -f $summary.negative_control.block_proven)
Write-Output ('POSITIVE_PE={0}/{1}/{2}/{3}' -f $summary.positive_candidate.inventory.pe_count, $summary.positive_candidate.inventory.valid_count, $summary.positive_candidate.inventory.not_signed_count, $summary.positive_candidate.inventory.other_status_count)
Write-Output ('POSITIVE_GUI_PASS={0}' -f $summary.positive_candidate.gui_launcher.pass)
Write-Output ('POSITIVE_CI_COUNT={0}' -f $summary.positive_candidate.code_integrity.returned_count)
Write-Output ('HOST_SAC={0}->{1}' -f $hostStateBefore, $hostStateAfter)
Write-Output ('SUMMARY={0}' -f $summaryPath)
Write-Output ('HOST_RUN={0}' -f $hostRunPath)
