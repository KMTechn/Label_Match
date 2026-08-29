[CmdletBinding()]
param(
    [string]$SourceRoot = "",
    [string]$InstallRoot = "C:\KMTech\Apps\Label_Match\current",
    [string]$EvidencePath = "",
    [switch]$PlanOnly,
    [switch]$AllowNoncanonicalLayoutForTest,
    [switch]$SkipSignatureValidationForTest
)

# TEMPLATE-CONFIG-GUARD-BEGIN
# This guard is the only behavior added to the accepted installer template.
# It is inert after every Label placeholder is replaced and fails closed before then.
$TemplateConfigurationValues = @(
    'C:\KMTech\Apps\Label_Match\current',
    'KMTech\Label_Match\install-audit',
    'KMTech\DirectSync\label_match\control\label_match_user_relay.stop.json',
    'KMTech\DirectSync\label_match\status',
    'label-match-canonical-portable-install-v1',
    'label-match-portable-tree-v1',
    'launch-label-match.cmd',
    'KMTech.LabelMatch.Relay',
    '--label-match-user-relay',
    'label_match_user_relay.json',
    'Label_Match'
)
$unresolvedTemplateValues = @($TemplateConfigurationValues | Where-Object {
    [string]$_ -match '^__[A-Z0-9_]+__$'
})
if ($unresolvedTemplateValues.Count -ne 0) {
    throw 'Installer template is not configured.'
}
# TEMPLATE-CONFIG-GUARD-END

$ErrorActionPreference = 'Stop'
$CanonicalRoot = 'C:\KMTech\Apps\Label_Match\current'
$RunKey = 'Software\Microsoft\Windows\CurrentVersion\Run'
$RunName = 'KMTech.LabelMatch.Relay'
$testMode = $AllowNoncanonicalLayoutForTest -and
    [string]$env:KMTECH_FACTORY_INSTALL_TEST_MODE -ceq '1'
if ($SkipSignatureValidationForTest -and -not $testMode) {
    throw 'Signature bypass is test-only.'
}

function Full([string]$Value, [string]$Purpose) {
    if (-not [IO.Path]::IsPathRooted($Value) -or $Value.StartsWith('\\?\')) {
        throw "$Purpose must be an ordinary absolute path."
    }
    $result = [IO.Path]::GetFullPath($Value).TrimEnd('\')
    if ($result -eq [IO.Path]::GetPathRoot($result)) { throw "$Purpose is too broad." }
    return $result
}

function Same([string]$Left, [string]$Right) {
    return (Full $Left 'left path').Equals((Full $Right 'right path'), 'OrdinalIgnoreCase')
}

function Sha([string]$Path) {
    $stream = [IO.File]::OpenRead($Path)
    $hash = [Security.Cryptography.SHA256]::Create()
    try {
        return ([BitConverter]::ToString($hash.ComputeHash($stream))).Replace('-', '').ToLowerInvariant()
    }
    finally {
        $hash.Dispose()
        $stream.Dispose()
    }
}

function Arg([string]$Value) {
    if ($Value.Contains('"')) { throw 'A command path contains a quote.' }
    if ($Value -match '\s') { return '"' + $Value + '"' }
    return $Value
}

function Command([string]$Root) {
    return ('{0} -I -B {1} --label-match-user-relay' -f
        (Arg (Join-Path $Root 'runtime\pythonw.exe')),
        (Arg (Join-Path $Root 'app\main.py')))
}

function Manifest([string]$Root, [bool]$UnsignedOk) {
    foreach ($relative in @(
        'portable-manifest.json',
        'runtime\python.exe',
        'runtime\pythonw.exe',
        'app\main.py',
        'launch-label-match.cmd',
        'INSTALL_CANONICAL_PORTABLE.ps1',
        'INSTALL_THIS_PC.ps1',
        'tools\bootstrap_integrity.ps1'
    )) {
        if (-not (Test-Path -LiteralPath (Join-Path $Root $relative) -PathType Leaf)) {
            throw "Portable tree is missing $relative."
        }
    }
    foreach ($item in @((Get-Item $Root -Force)) + @(Get-ChildItem $Root -Force -Recurse)) {
        if (($item.Attributes -band [IO.FileAttributes]::ReparsePoint) -ne 0) {
            throw "Portable tree contains a reparse point: $($item.FullName)"
        }
    }
    $path = Join-Path $Root 'portable-manifest.json'
    if ((Get-Item $path).Length -gt 65536) { throw 'Portable manifest is oversized.' }
    $value = Get-Content $path -Raw -Encoding UTF8 | ConvertFrom-Json
    if (
        [string]$value.schema -cne 'label-match-portable-tree-v1' -or
        [string]$value.entrypoint -cne 'runtime/pythonw.exe app/main.py' -or
        [string]$value.launcher -cne 'launch-label-match.cmd' -or
        @($value.allowed_unsigned_app_pe).Count -ne 0 -or
        @($value.forbidden_package_roots).Count -ne 0 -or
        (Sha (Join-Path $Root 'runtime\pythonw.exe')) -cne
            ([string]$value.runtime_pythonw_sha256).ToLowerInvariant() -or
        (Sha (Join-Path $Root 'launch-label-match.cmd')) -cne
            ([string]$value.launcher_sha256).ToLowerInvariant()
    ) {
        throw 'Portable manifest readback failed.'
    }
    if (-not $UnsignedOk) {
        foreach ($relative in @('runtime\python.exe', 'runtime\pythonw.exe')) {
            if ([string](Get-AuthenticodeSignature (Join-Path $Root $relative)).Status -cne 'Valid') {
                throw "Signed CPython readback failed: $relative"
            }
        }
    }
    return $value
}

function Snapshot {
    $key = [Microsoft.Win32.Registry]::CurrentUser.OpenSubKey($RunKey, $false)
    if ($null -eq $key) { return [ordered]@{ exists = $false; kind = ''; data = '' } }
    try {
        try { $kind = [string]$key.GetValueKind($RunName) }
        catch [IO.IOException] { return [ordered]@{ exists = $false; kind = ''; data = '' } }
        if ($kind -notin @('String', 'ExpandString')) { throw "Unsupported Run type: $kind" }
        $data = [string]$key.GetValue(
            $RunName,
            $null,
            [Microsoft.Win32.RegistryValueOptions]::DoNotExpandEnvironmentNames
        )
        return [ordered]@{ exists = $true; kind = $kind; data = $data }
    }
    finally { $key.Dispose() }
}

function Restore($Before) {
    $key = [Microsoft.Win32.Registry]::CurrentUser.CreateSubKey($RunKey, $true)
    try {
        if ([bool]$Before.exists) {
            $key.SetValue(
                $RunName,
                [string]$Before.data,
                [Microsoft.Win32.RegistryValueKind]::$($Before.kind)
            )
        }
        else { $key.DeleteValue($RunName, $false) }
    }
    finally { $key.Dispose() }
}

function Save([string]$Path, $Value) {
    New-Item -ItemType Directory -Path (Split-Path -Parent $Path) -Force | Out-Null
    $temp = "$Path.tmp.$PID"
    [IO.File]::WriteAllText(
        $temp,
        ($Value | ConvertTo-Json -Depth 8) + [Environment]::NewLine,
        (New-Object Text.UTF8Encoding($false))
    )
    Move-Item $temp $Path -Force
}

function Relays {
    return @(Get-CimInstance Win32_Process | Where-Object {
        [string]$_.CommandLine -like '*--label-match-user-relay*' -and
        [string]$_.ExecutablePath -match '(?i)(pythonw?\.exe|Label_Match\.exe)$'
    })
}

function Product([string]$Root, [string]$Mode) {
    $args = '-I -B {0} {1} --app-root {2}' -f
        (Arg (Join-Path $Root 'app\main.py')),
        $Mode,
        (Arg $Root)
    $process = Start-Process `
        (Join-Path $Root 'runtime\pythonw.exe') `
        -ArgumentList $args `
        -WindowStyle Hidden `
        -PassThru
    # Start-Process -Wait includes the persistent relay child; wait only for the product host.
    $process.WaitForExit()
    if ($process.ExitCode -ne 0) { throw "Product mode failed: $Mode/$($process.ExitCode)" }
}

function StartRaw([string]$Line) {
    $created = Invoke-CimMethod `
        -ClassName Win32_Process `
        -MethodName Create `
        -Arguments @{ CommandLine = $Line }
    if ([uint32]$created.ReturnValue -ne 0) { throw 'Rollback process start failed.' }
    return [int]$created.ProcessId
}

if (-not $SourceRoot) { $SourceRoot = $PSScriptRoot }
$source = Full $SourceRoot 'SourceRoot'
$install = Full $InstallRoot 'InstallRoot'
if (-not $testMode -and -not (Same $install $CanonicalRoot)) {
    throw 'InstallRoot is not canonical.'
}
$sourceManifest = Manifest $source $SkipSignatureValidationForTest
$wanted = Command $install
if ($PlanOnly) {
    "install_status=PLAN_ONLY"
    "install_root=$install"
    "autostart_command=$wanted"
    'registry_changed=false'
    exit 0
}

$runId = (Get-Date).ToUniversalTime().ToString('yyyyMMddTHHmmssfffZ') + '-' +
    [Guid]::NewGuid().ToString('N')
$lad = Full $env:LOCALAPPDATA 'LOCALAPPDATA'
$localAuditRoot = Join-Path $lad 'KMTech\Label_Match\install-audit'
$auditPath = Join-Path $localAuditRoot "canonical-portable-$runId.json"
$elevationLogPath = Join-Path $localAuditRoot "canonical-portable-$runId-elevated.jsonl"
$statusRoot = Join-Path $lad 'KMTech\DirectSync\label_match\status'
$stop = Join-Path $lad 'KMTech\DirectSync\label_match\control\label_match_user_relay.stop.json'
$onboardingPath = Join-Path $statusRoot 'current_user_onboarding.json'
$removalPath = Join-Path $statusRoot 'current_user_removal.json'
$relayPath = Join-Path $statusRoot 'label_match_user_relay.json'
$before = Snapshot
$old = @(Relays)
$stopBefore = [ordered]@{ exists = $false; sha256 = ''; backup_path = '' }
if (Test-Path -LiteralPath $stop -PathType Leaf) {
    New-Item -ItemType Directory -Path $localAuditRoot -Force | Out-Null
    $stopBackup = Join-Path $localAuditRoot "canonical-portable-$runId-stop-preimage.json"
    Copy-Item -LiteralPath $stop -Destination $stopBackup -Force
    $stopBefore = [ordered]@{
        exists = $true
        sha256 = Sha $stop
        backup_path = $stopBackup
    }
}
if ([bool]$stopBefore.exists -and $old.Count -gt 0) {
    throw 'Relay preimage is internally inconsistent: stop marker and running relay coexist.'
}
$audit = [ordered]@{
    schema = 'label-match-canonical-portable-install-v1'
    status = 'PREIMAGE_SAVED'
    run_id = $runId
    captured_at = (Get-Date).ToUniversalTime().ToString('o')
    install_root = $install
    code_placement = 'PENDING'
    source_commit = [string]$sourceManifest.source_commit
    runtime_pythonw_sha256 = Sha (Join-Path $source 'runtime\pythonw.exe')
    runtime_pythonw_signature = [string](
        Get-AuthenticodeSignature (Join-Path $source 'runtime\pythonw.exe')
    ).Status
    elevation_log_path = $elevationLogPath
    registry_value = $RunName
    preimage = $before
    after = [ordered]@{ exists = $true; kind = 'String'; data = $wanted }
    relay_process_preimage_count = $old.Count
    stop_marker_path = $stop
    stop_marker_preimage = $stopBefore
    rollback = [ordered]@{ available = $true; applied = $false; runtime_restored = $false }
}
Save $auditPath $audit
if ($EvidencePath) { Save (Full $EvidencePath 'EvidencePath') $audit }

$winps = Join-Path ([Environment]::SystemDirectory) 'WindowsPowerShell\v1.0\powershell.exe'
$placement = 'INSTALL_REQUIRED'
$existingVerified = $false
if (Test-Path $install -PathType Container) {
    try {
        $candidate = Manifest $install $SkipSignatureValidationForTest
        $helper = (Join-Path $PSScriptRoot 'tools\bootstrap_integrity.ps1').Replace("'", "''")
        $escapedRoot = $install.Replace("'", "''")
        & $winps -NoLogo -NoProfile -NonInteractive -Command `
            ". '$helper'; [void](Assert-BootstrapIntegrityRecord '$escapedRoot')"
        if ($LASTEXITCODE -ne 0) { throw 'integrity differs' }
        $existingVerified = $true
        if (
            [string]$candidate.source_commit -ceq [string]$sourceManifest.source_commit -and
            (Sha (Join-Path $install 'runtime\pythonw.exe')) -ceq
                (Sha (Join-Path $source 'runtime\pythonw.exe'))
        ) { $placement = 'REUSED_VERIFIED' }
    }
    catch {
        $existingVerified = $false
        $placement = 'INSTALL_REQUIRED'
    }
}
if ($placement -eq 'INSTALL_REQUIRED') {
    if ((Test-Path $install -PathType Container) -and -not $existingVerified) {
        throw 'Existing canonical tree is not eligible for verified replacement.'
    }
    $bootstrap = @(
        '-NoLogo',
        '-NoProfile',
        '-NonInteractive',
        '-ExecutionPolicy',
        'Bypass',
        '-File',
        (Join-Path $PSScriptRoot 'INSTALL_THIS_PC.ps1'),
        '-SourceRoot',
        $source,
        '-InstallRoot',
        $install,
        '-ElevationLogPath',
        $elevationLogPath
    )
    if ($testMode) { $bootstrap += '-AllowNoncanonicalLayoutForTest' }
    if ($existingVerified) { $bootstrap += '-ReplaceExistingVerifiedPortable' }
    & $winps @bootstrap
    if ($LASTEXITCODE -ne 0) { throw "Code placement failed: $LASTEXITCODE" }
    $placement = 'PASS'
}
$installedManifest = Manifest $install $SkipSignatureValidationForTest
if ([string]$installedManifest.source_commit -cne [string]$sourceManifest.source_commit) {
    throw 'Installed identity differs.'
}
$audit.code_placement = $placement
$audit.runtime_pythonw_sha256 = Sha (Join-Path $install 'runtime\pythonw.exe')
$audit.runtime_pythonw_signature = [string](
    Get-AuthenticodeSignature (Join-Path $install 'runtime\pythonw.exe')
).Status
Save $auditPath $audit
if ($EvidencePath) { Save (Full $EvidencePath 'EvidencePath') $audit }

$mutated = $false
try {
    $mutated = $true
    Product $install '--remove-current-user-setup'
    $removal = Get-Content $removalPath -Raw -Encoding UTF8 | ConvertFrom-Json
    if (
        (Snapshot).exists -or
        [string]$removal.status -cne 'PASS_DATA_PRESERVED' -or
        [string]$removal.relay_process.status -cne 'ABSENT'
    ) { throw 'Removal readback failed.' }

    $started = (Get-Date).ToUniversalTime()
    Product $install '--onboard-current-user'
    $onboarding = Get-Content $onboardingPath -Raw -Encoding UTF8 | ConvertFrom-Json
    $after = Snapshot
    if (
        [string]$onboarding.status -cne 'READY' -or
        [string]$onboarding.relay_autostart.command -cne $wanted -or
        -not $after.exists -or
        [string]$after.data -cne $wanted
    ) { throw 'Onboarding Run readback failed.' }
    if (Test-Path -LiteralPath $stop) { throw 'Relay stop marker survived onboarding.' }

    $pidValue = [int]$onboarding.relay_start.process_id
    Start-Sleep -Seconds 5
    $process = Get-CimInstance `
        Win32_Process `
        -Filter "ProcessId = $pidValue" `
        -ErrorAction SilentlyContinue
    if (
        $null -eq $process -or
        -not (Same ([string]$process.ExecutablePath) (Join-Path $install 'runtime\pythonw.exe'))
    ) { throw 'Relay process proof failed.' }

    $deadline = (Get-Date).AddSeconds(75)
    $relay = $null
    while ((Get-Date) -lt $deadline) {
        if (
            (Test-Path $relayPath) -and
            (Get-Item $relayPath).LastWriteTimeUtc -ge $started.AddSeconds(-1)
        ) {
            $relay = Get-Content $relayPath -Raw -Encoding UTF8 | ConvertFrom-Json
            if ([bool]$relay.persistent_retry) { break }
        }
        Start-Sleep -Milliseconds 500
    }
    if ($null -eq $relay -or -not [bool]$relay.persistent_retry) {
        throw 'Fresh relay status proof failed.'
    }

    $audit.status = 'PASS'
    $audit.completed_at = (Get-Date).ToUniversalTime().ToString('o')
    $audit.stop_marker_absent = -not (Test-Path $stop)
    $audit.onboarding = [ordered]@{
        status = [string]$onboarding.status
        action = [string]$onboarding.action
        autostart_writer = 'product_onboarding'
    }
    $audit.exact_launch = [ordered]@{
        status = 'PROVEN'
        process_id = $pidValue
        executable = [string]$process.ExecutablePath
        relay_status = [string]$relay.status
        persistent_retry = [bool]$relay.persistent_retry
    }
    Save $auditPath $audit
    if ($EvidencePath) { Save (Full $EvidencePath 'EvidencePath') $audit }
    'install_status=PASS'
    "install_root=$install"
    "code_placement_status=$placement"
    'autostart_status=PROVEN_NON_REBOOT_APPROXIMATION'
    "autostart_command=$wanted"
    "autostart_process_id=$pidValue"
    "stop_marker_absent=$($audit.stop_marker_absent.ToString().ToLowerInvariant())"
    'cold_boot_status=UNPROVEN'
    "audit_path=$auditPath"
    "elevation_log_path=$elevationLogPath"
}
catch {
    $original = $_
    try {
        if ($mutated) {
            try { Product $install '--remove-current-user-setup' } catch {}
            Restore $before
        }
        if ([bool]$stopBefore.exists) {
            Copy-Item -LiteralPath ([string]$stopBefore.backup_path) -Destination $stop -Force
            if ((Sha $stop) -cne [string]$stopBefore.sha256) {
                throw 'stop marker restore failed'
            }
        }
        elseif (Test-Path $stop) {
            Remove-Item $stop -Force
        }
        foreach ($item in $old) {
            $newPid = StartRaw ([string]$item.CommandLine)
            Start-Sleep -Seconds 3
            $restored = Get-CimInstance `
                Win32_Process `
                -Filter "ProcessId = $newPid" `
                -ErrorAction SilentlyContinue
            if (
                $null -eq $restored -or
                -not (Same ([string]$restored.ExecutablePath) ([string]$item.ExecutablePath))
            ) { throw 'runtime restore failed' }
        }
        $check = Snapshot
        if (
            [bool]$check.exists -ne [bool]$before.exists -or
            [string]$check.kind -cne [string]$before.kind -or
            [string]$check.data -cne [string]$before.data
        ) { throw 'registry restore failed' }
        $audit.status = 'FAILED_ROLLED_BACK'
        $audit.rollback.applied = $mutated
        $audit.rollback.runtime_restored = $true
        $audit.failure_type = $original.Exception.GetType().Name
        Save $auditPath $audit
        if ($EvidencePath) { Save (Full $EvidencePath 'EvidencePath') $audit }
    }
    catch {
        throw "AUTOSTART_ROLLBACK_FAILED: $($_.Exception.GetType().Name)"
    }
    throw $original
}
