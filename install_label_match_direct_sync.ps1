param(
    [switch]$DryRun,
    [switch]$Uninstall,
    [switch]$Rollback,
    [string]$EvidenceArchiveRoot = "",
    [switch]$AllowNoncanonicalLayoutForTest,
    [string]$ManagedInstallRoot = "",
    [string]$SourceManifestSha256 = "",
    [ValidateSet("absent", "exact_reused")]
    [string]$InstallPrestate = "absent",
    [string]$InstallRootForTest = "",
    [string]$CommonProgramsRootForTest = "",
    [string]$RollbackReceiptRootForTest = "",
    [string]$ServerBaseUrl = "https://worker.kmtecherp.com",
    [string]$SourceHostId = "",
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
    [switch]$AllowInteractiveTaskForLocalTest,
    [System.Management.Automation.PSReference]$PublicWrapperExitCode = $null
)

$ErrorActionPreference = "Stop"
$MutableAppRelativePaths = @("_internal/config/app_settings.json")

if (-not $DryRun.IsPresent) {
    $identity = [Security.Principal.WindowsIdentity]::GetCurrent()
    $principal = New-Object Security.Principal.WindowsPrincipal($identity)
    if (-not $principal.IsInRole([Security.Principal.WindowsBuiltInRole]::Administrator)) {
        throw "Administrator privileges are required for installation or removal."
    }
}

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
if ([string]::IsNullOrWhiteSpace($SourceHostId)) {
    $resolvedSourceHostId = ("label-match-{0}-{1}" -f $safePcId, (Get-MachineStableSuffix)).ToLowerInvariant()
}
else {
    $resolvedSourceHostId = (Get-SafeToken $SourceHostId "").ToLowerInvariant()
    if ([string]::IsNullOrWhiteSpace($resolvedSourceHostId)) {
        throw "SourceHostId override must contain at least one safe identity character."
    }
}
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

function Get-FileSha256([string]$Path) {
    $fullPath = [System.IO.Path]::GetFullPath($Path)
    $stream = [System.IO.File]::Open(
        $fullPath,
        [System.IO.FileMode]::Open,
        [System.IO.FileAccess]::Read,
        [System.IO.FileShare]::Read
    )
    try {
        $sha = [System.Security.Cryptography.SHA256]::Create()
        if ($null -eq $sha) {
            throw "Unable to create SHA-256 authority."
        }
        try {
            $digest = $sha.ComputeHash($stream)
            if ($null -eq $digest -or $digest.Length -ne 32) {
                throw "SHA-256 authority returned a malformed digest."
            }
            return ([System.BitConverter]::ToString($digest)).Replace("-", "").ToLowerInvariant()
        }
        finally {
            $sha.Dispose()
        }
    }
    finally {
        $stream.Dispose()
    }
}

function Get-RelativeFilePath([string]$Root, [string]$Path) {
    $rootFull = [System.IO.Path]::GetFullPath($Root).TrimEnd('\')
    $pathFull = [System.IO.Path]::GetFullPath($Path)
    if (-not $pathFull.StartsWith($rootFull + '\', [System.StringComparison]::OrdinalIgnoreCase)) {
        throw "File escapes owned root: $pathFull"
    }
    return $pathFull.Substring($rootFull.Length + 1).Replace('\', '/')
}

function Get-ImmutableAppInventoryIdentity([string]$Root, [string[]]$MutableRelativePaths) {
    $rootFull = [System.IO.Path]::GetFullPath($Root)
    if (-not (Test-Path -LiteralPath $rootFull -PathType Container)) {
        throw "Installed app root is missing while computing its immutable inventory."
    }
    if (
        $MutableRelativePaths.Count -ne 1 -or
        [string]$MutableRelativePaths[0] -cne "_internal/config/app_settings.json"
    ) {
        throw "Mutable app inventory allowlist differs from the exact runtime-settings contract."
    }
    $mutablePath = Join-Path $rootFull $MutableRelativePaths[0].Replace('/', '\')
    $mutableFull = [System.IO.Path]::GetFullPath($mutablePath)
    $mutableInsideRoot = Test-PathInside $mutableFull $rootFull
    $mutableFileExists = Test-Path -LiteralPath $mutableFull -PathType Leaf
    if (
        -not $mutableInsideRoot -or
        -not $mutableFileExists
    ) {
        throw "Mutable runtime settings file is missing or escapes the installed app root."
    }
    $mutableItem = Get-Item -LiteralPath $mutableFull -Force -ErrorAction Stop
    if (($mutableItem.Attributes -band [System.IO.FileAttributes]::ReparsePoint) -ne 0) {
        throw "Mutable runtime settings file is a reparse point."
    }
    $entries = @(
        Get-ChildItem -LiteralPath $rootFull -Force -File -Recurse | ForEach-Object {
            $relative = Get-RelativeFilePath $rootFull $_.FullName
            if ([string]$relative -cne "_internal/config/app_settings.json") {
                [ordered]@{
                    path = $relative
                    size = [long]$_.Length
                    sha256 = Get-FileSha256 $_.FullName
                }
            }
        } | Sort-Object @{ Expression = { $_.path.ToLowerInvariant() } }, @{ Expression = { $_.path } }
    )
    $builder = New-Object System.Text.StringBuilder
    foreach ($entry in $entries) {
        [void]$builder.Append($entry.path).Append("`t").Append($entry.size).Append("`t").Append($entry.sha256).Append("`n")
    }
    return [ordered]@{
        immutable_file_count = $entries.Count
        immutable_sha256 = Get-Sha256Hex $builder.ToString()
    }
}

function Get-FileResourceRecord([string]$Path, [bool]$ExistedBefore) {
    $exists = Test-Path -LiteralPath $Path -PathType Leaf
    return [ordered]@{
        path = [System.IO.Path]::GetFullPath($Path)
        prestate = if ($ExistedBefore) { "existing" } else { "absent" }
        disposition = if ($ExistedBefore) { "reused" } elseif ($exists) { "created" } else { "not_created" }
        sha256 = if ($exists) { Get-FileSha256 $Path } else { $null }
        size = if ($exists) { [long](Get-Item -LiteralPath $Path).Length } else { $null }
    }
}

function Read-RequiredInstallSummary(
    [string]$Path,
    [string]$ExpectedManifestSha256,
    [string]$ExpectedAppRoot,
    [string]$ExpectedProgramDataRoot,
    [string]$ExpectedScanSourceDir,
    [string]$ExpectedTaskName,
    [string]$ExpectedLauncherPath,
    [string]$ExpectedLogisticsProfilePath
) {
    if (-not (Test-Path -LiteralPath $Path -PathType Leaf)) {
        throw "Install ownership summary is missing: $Path"
    }
    try {
        $summary = Get-Content -LiteralPath $Path -Raw -Encoding UTF8 | ConvertFrom-Json
    }
    catch {
        throw "Install ownership summary is invalid JSON: $Path"
    }
    if (
        $summary.installer_report_version -cne "label-match-direct-sync-one-step-install-v2" -or
        $summary.status -cne "PASS" -or
        [string]$summary.source_manifest_sha256 -cne $ExpectedManifestSha256 -or
        -not (Test-SamePath ([string]$summary.resources.app_root.path) $ExpectedAppRoot) -or
        -not (Test-SamePath ([string]$summary.program_data_root) $ExpectedProgramDataRoot) -or
        -not (Test-SamePath ([string]$summary.scan_source_dir) $ExpectedScanSourceDir) -or
        [string]$summary.task_name -cne $ExpectedTaskName -or
        -not (Test-SamePath ([string]$summary.logistics_profile_path) $ExpectedLogisticsProfilePath) -or
        -not (Test-SamePath ([string]$summary.resources.direct_sync_root.path) $ExpectedProgramDataRoot) -or
        -not (Test-SamePath ([string]$summary.resources.data_root.path) $ExpectedScanSourceDir) -or
        -not (Test-SamePath ([string]$summary.resources.launcher.path) $ExpectedLauncherPath) -or
        [string]$summary.resources.scheduled_task.name -cne $ExpectedTaskName -or
        -not (Test-SamePath ([string]$summary.resources.machine_profile_root.path) (Split-Path -Parent $ExpectedLogisticsProfilePath))
    ) {
        throw "Install ownership summary does not bind the exact removal resource set."
    }
    $appRootResource = $summary.resources.app_root
    $mutablePaths = @($appRootResource.mutable_relative_paths)
    if (
        [string]$appRootResource.inventory_contract -cne "label-match-app-immutable-inventory-v1" -or
        $mutablePaths.Count -ne 1 -or
        [string]$mutablePaths[0] -cne "_internal/config/app_settings.json" -or
        [int]$appRootResource.immutable_file_count -lt 1 -or
        [string]$appRootResource.immutable_inventory_sha256 -cnotmatch '^[0-9a-f]{64}$'
    ) {
        throw "Install ownership summary app inventory contract is invalid."
    }
    $directSyncResources = @($summary.resources.direct_sync_owned_files)
    $expectedTaskWrapperPath = Join-Path $ExpectedProgramDataRoot ("bin\run_{0}.ps1" -f $ExpectedTaskName)
    $expectedTaskLauncherPath = Join-Path $ExpectedProgramDataRoot ("bin\run_{0}.vbs" -f $ExpectedTaskName)
    $directSyncResourcePaths = @($directSyncResources | ForEach-Object { [System.IO.Path]::GetFullPath([string]$_.path) })
    if (
        $directSyncResources.Count -ne 4 -or
        @($directSyncResourcePaths | Select-Object -Unique).Count -ne 4 -or
        $directSyncResourcePaths -notcontains [System.IO.Path]::GetFullPath($expectedTaskWrapperPath) -or
        $directSyncResourcePaths -notcontains [System.IO.Path]::GetFullPath($expectedTaskLauncherPath)
    ) {
        throw "Install ownership summary DirectSync file inventory is not exact."
    }
    foreach ($resource in $directSyncResources) {
        if (
            [string]$resource.disposition -ceq "created" -and
            -not (Test-PathInside ([string]$resource.path) $ExpectedProgramDataRoot)
        ) {
            throw "Install ownership summary contains an out-of-root DirectSync removal path."
        }
    }
    $allowedMachineProfileFiles = @(
        [System.IO.Path]::GetFullPath($ExpectedLogisticsProfilePath),
        [System.IO.Path]::GetFullPath((Join-Path (Split-Path -Parent $ExpectedLogisticsProfilePath) "secrets\bearer-token.dpapi"))
    )
    $machineProfileResources = @($summary.resources.machine_profile_files)
    $machineProfileResourcePaths = @($machineProfileResources | ForEach-Object { [System.IO.Path]::GetFullPath([string]$_.path) })
    if (
        $machineProfileResources.Count -ne 2 -or
        @($machineProfileResourcePaths | Select-Object -Unique).Count -ne 2 -or
        @($machineProfileResourcePaths | Where-Object { $allowedMachineProfileFiles -notcontains $_ }).Count -ne 0
    ) {
        throw "Install ownership summary machine-profile file inventory is not exact."
    }
    foreach ($resource in $machineProfileResources) {
        if (
            [string]$resource.disposition -ceq "created" -and
            $allowedMachineProfileFiles -notcontains [System.IO.Path]::GetFullPath([string]$resource.path)
        ) {
            throw "Install ownership summary contains an unexpected machine-profile removal path."
        }
    }
    $createdDirectoryPaths = $summary.resources.created_directory_paths
    $directoryBindings = @(
        [ordered]@{ name = "data_root"; leaf = $ExpectedScanSourceDir },
        [ordered]@{ name = "direct_sync_root"; leaf = $ExpectedProgramDataRoot },
        [ordered]@{ name = "machine_profile_root"; leaf = Split-Path -Parent $ExpectedLogisticsProfilePath },
        [ordered]@{ name = "launcher_parent"; leaf = Split-Path -Parent $ExpectedLauncherPath }
    )
    foreach ($binding in $directoryBindings) {
        if ($null -eq $createdDirectoryPaths -or $createdDirectoryPaths.PSObject.Properties.Name -notcontains $binding.name) {
            throw "Install ownership summary is missing created-directory ancestry."
        }
        $leafFull = [System.IO.Path]::GetFullPath([string]$binding.leaf)
        foreach ($recorded in @($createdDirectoryPaths.($binding.name))) {
            if ([string]::IsNullOrWhiteSpace([string]$recorded)) {
                throw "Install ownership summary contains an empty created-directory path."
            }
            $recordedFull = [System.IO.Path]::GetFullPath([string]$recorded)
            $recordedParent = [System.IO.Path]::GetDirectoryName($recordedFull.TrimEnd('\'))
            if (
                [string]::IsNullOrWhiteSpace($recordedParent) -or
                $recordedParent -eq $recordedFull -or
                (-not (Test-SamePath $leafFull $recordedFull) -and -not (Test-PathInside $leafFull $recordedFull))
            ) {
                throw "Install ownership summary contains an invalid created-directory ancestor."
            }
        }
    }
    return $summary
}

function Get-ShortcutProperties([string]$Path) {
    if (-not (Test-Path -LiteralPath $Path -PathType Leaf)) { return $null }
    $shell = New-Object -ComObject WScript.Shell
    $shortcut = $shell.CreateShortcut($Path)
    return [ordered]@{
        target_path = [System.IO.Path]::GetFullPath([string]$shortcut.TargetPath)
        working_directory = [System.IO.Path]::GetFullPath([string]$shortcut.WorkingDirectory)
        icon_location = [string]$shortcut.IconLocation
    }
}

function Test-ShortcutContract($Properties, [string]$ExpectedTarget, [string]$ExpectedWorkingDirectory) {
    if ($null -eq $Properties) { return $false }
    $iconPath = ([string]$Properties.icon_location).Split(',')[0]
    return (
        (Test-SamePath ([string]$Properties.target_path) $ExpectedTarget) -and
        (Test-SamePath ([string]$Properties.working_directory) $ExpectedWorkingDirectory) -and
        (Test-SamePath $iconPath $ExpectedTarget)
    )
}

function Ensure-AllUsersLauncher([string]$Path, [string]$Target, [string]$WorkingDirectory) {
    $before = Get-ShortcutProperties $Path
    if ($null -ne $before -and -not (Test-ShortcutContract $before $Target $WorkingDirectory)) {
        throw "Refusing to overwrite an unowned Start Menu launcher: $Path"
    }
    $created = $null -eq $before
    if ($created) {
        $parent = Split-Path -Parent $Path
        New-Item -ItemType Directory -Path $parent -Force | Out-Null
        $shell = New-Object -ComObject WScript.Shell
        $shortcut = $shell.CreateShortcut($Path)
        $shortcut.TargetPath = $Target
        $shortcut.WorkingDirectory = $WorkingDirectory
        $shortcut.IconLocation = "$Target,0"
        $shortcut.Save()
    }
    $after = Get-ShortcutProperties $Path
    if (-not (Test-ShortcutContract $after $Target $WorkingDirectory)) {
        throw "All-users Start Menu launcher verification failed: $Path"
    }
    return [ordered]@{
        path = [System.IO.Path]::GetFullPath($Path)
        scope = "all_users"
        prestate = if ($created) { "absent" } else { "exact_reused" }
        disposition = if ($created) { "created" } else { "reused" }
        target_path = [System.IO.Path]::GetFullPath($Target)
        working_directory = [System.IO.Path]::GetFullPath($WorkingDirectory)
        icon_path = [System.IO.Path]::GetFullPath($Target)
        sha256 = Get-FileSha256 $Path
        verified = $true
    }
}

function Remove-OwnedFile([string]$Path, $Resource, [string]$Label) {
    if ([string]$Resource.disposition -cne "created") {
        return [ordered]@{ path = $Path; disposition = "preserved_preexisting"; absence_proven = $false }
    }
    if (-not (Test-Path -LiteralPath $Path -PathType Leaf)) {
        return [ordered]@{ path = $Path; disposition = "already_absent"; absence_proven = $true }
    }
    if ([string]::IsNullOrWhiteSpace([string]$Resource.sha256) -or (Get-FileSha256 $Path) -cne [string]$Resource.sha256) {
        throw "$Label drifted after installation; refusing removal: $Path"
    }
    Remove-Item -LiteralPath $Path -Force -ErrorAction Stop
    if (Test-Path -LiteralPath $Path) { throw "$Label remains after removal: $Path" }
    return [ordered]@{ path = $Path; disposition = "removed"; absence_proven = $true }
}

function Assert-NoReparsePath([string]$Path, [string]$Label) {
    $cursor = [System.IO.Path]::GetFullPath($Path)
    while (-not (Test-Path -LiteralPath $cursor)) {
        $parent = [System.IO.Path]::GetDirectoryName($cursor.TrimEnd('\'))
        if ([string]::IsNullOrWhiteSpace($parent) -or $parent -eq $cursor) {
            throw "$Label has no existing filesystem ancestor."
        }
        $cursor = $parent
    }
    while (-not [string]::IsNullOrWhiteSpace($cursor)) {
        $item = Get-Item -LiteralPath $cursor -Force -ErrorAction Stop
        if (($item.Attributes -band [System.IO.FileAttributes]::ReparsePoint) -ne 0) {
            throw "$Label crosses a reparse point: $cursor"
        }
        $parent = [System.IO.Path]::GetDirectoryName($cursor.TrimEnd('\'))
        if ([string]::IsNullOrWhiteSpace($parent) -or $parent -eq $cursor) { break }
        $cursor = $parent
    }
}

function Get-MissingDirectoryChain([string]$DirectoryPath) {
    $cursor = [System.IO.Path]::GetFullPath($DirectoryPath)
    $missing = @()
    while (-not (Test-Path -LiteralPath $cursor)) {
        $parent = [System.IO.Path]::GetDirectoryName($cursor.TrimEnd('\'))
        if ([string]::IsNullOrWhiteSpace($parent) -or $parent -eq $cursor) {
            throw "Refusing to classify a filesystem root as installer-created."
        }
        $missing += $cursor
        $cursor = $parent
    }
    if (-not (Test-Path -LiteralPath $cursor -PathType Container)) {
        throw "Directory ancestry is blocked by a non-directory path: $cursor"
    }
    return @($missing)
}

function Remove-OwnedEmptyDirectoryChain([object[]]$RecordedPaths, [string]$ManagedLeafDirectory) {
    $allowed = @{}
    $cursor = [System.IO.Path]::GetFullPath($ManagedLeafDirectory)
    while ($true) {
        $parent = [System.IO.Path]::GetDirectoryName($cursor.TrimEnd('\'))
        if ([string]::IsNullOrWhiteSpace($parent) -or $parent -eq $cursor) { break }
        $allowed[$cursor.ToLowerInvariant()] = $true
        $cursor = $parent
    }
    $normalized = @()
    foreach ($recorded in @($RecordedPaths)) {
        $full = [System.IO.Path]::GetFullPath([string]$recorded)
        if (-not $allowed.ContainsKey($full.ToLowerInvariant())) {
            throw "Recorded installer-created directory escapes its managed ancestry: $full"
        }
        if ($normalized -notcontains $full) { $normalized += $full }
    }
    foreach ($path in @($normalized | Sort-Object Length -Descending)) {
        if (-not (Test-Path -LiteralPath $path)) { continue }
        if (-not (Test-Path -LiteralPath $path -PathType Container)) {
            throw "Installer-created directory path became a non-directory: $path"
        }
        if (@(Get-ChildItem -LiteralPath $path -Force).Count -ne 0) {
            throw "Installer-created directory is not empty after rollback cleanup: $path"
        }
        Remove-Item -LiteralPath $path -Force -ErrorAction Stop
    }
    return @($normalized)
}

function Copy-BoundedRollbackEvidence(
    [string]$DataRoot,
    [string]$DirectSyncRoot,
    [string]$EvidenceRoot,
    [int]$MaximumFiles = 10000,
    [long]$MaximumBytes = 2147483648
) {
    $evidenceFull = [System.IO.Path]::GetFullPath($EvidenceRoot)
    if (Test-Path -LiteralPath $evidenceFull) {
        throw "Rollback evidence root must be absent before collection: $evidenceFull"
    }
    Assert-NoReparsePath $evidenceFull "rollback evidence root"
    New-Item -ItemType Directory -Path $evidenceFull -ErrorAction Stop | Out-Null
    Assert-NoReparsePath $evidenceFull "rollback evidence root"
    $sources = @(
        [ordered]@{ root = [System.IO.Path]::GetFullPath($DataRoot); prefix = "label-data" },
        [ordered]@{ root = Join-Path ([System.IO.Path]::GetFullPath($DirectSyncRoot)) "queue"; prefix = "direct-sync/queue" },
        [ordered]@{ root = Join-Path ([System.IO.Path]::GetFullPath($DirectSyncRoot)) "spool"; prefix = "direct-sync/spool" },
        [ordered]@{ root = Join-Path ([System.IO.Path]::GetFullPath($DirectSyncRoot)) "upload_status"; prefix = "direct-sync/upload_status" },
        [ordered]@{ root = Join-Path ([System.IO.Path]::GetFullPath($DirectSyncRoot)) "status"; prefix = "direct-sync/status" },
        [ordered]@{ root = Join-Path ([System.IO.Path]::GetFullPath($DirectSyncRoot)) "logs"; prefix = "direct-sync/logs" },
        [ordered]@{ root = Join-Path ([System.IO.Path]::GetFullPath($DirectSyncRoot)) "control"; prefix = "direct-sync/control" }
    )
    $inventory = @()
    $totalBytes = [long]0
    foreach ($source in $sources) {
        if (-not (Test-Path -LiteralPath $source.root -PathType Container)) { continue }
        Assert-NoReparsePath $source.root "rollback evidence source"
        foreach ($item in Get-ChildItem -LiteralPath $source.root -Force -Recurse) {
            if (($item.Attributes -band [System.IO.FileAttributes]::ReparsePoint) -ne 0) {
                throw "Rollback evidence source contains a reparse point: $($item.FullName)"
            }
            if ($item.PSIsContainer) { continue }
            if ($inventory.Count + 1 -gt $MaximumFiles) { throw "Rollback evidence exceeds the bounded file-count limit." }
            $totalBytes += [long]$item.Length
            if ($totalBytes -gt $MaximumBytes) { throw "Rollback evidence exceeds the bounded byte limit." }
            $relative = Get-RelativeFilePath $source.root $item.FullName
            $archiveRelative = "$($source.prefix)/$relative"
            $destination = Join-Path $evidenceFull $archiveRelative.Replace('/', '\')
            New-Item -ItemType Directory -Path (Split-Path -Parent $destination) -Force | Out-Null
            Copy-Item -LiteralPath $item.FullName -Destination $destination -ErrorAction Stop
            $sourceHash = Get-FileSha256 $item.FullName
            $destinationHash = Get-FileSha256 $destination
            if ([long](Get-Item -LiteralPath $destination).Length -ne [long]$item.Length -or $sourceHash -cne $destinationHash) {
                throw "Rollback evidence byte parity failed: $archiveRelative"
            }
            $inventory += [ordered]@{ path = $archiveRelative; size = [long]$item.Length; sha256 = $sourceHash }
        }
    }
    $inventoryPath = Join-Path $evidenceFull "evidence-inventory.json"
    Write-Utf8JsonFile $inventoryPath ([ordered]@{
        schema_version = "label-match-rollback-evidence-v1"
        status = "PASS"
        limits = [ordered]@{ maximum_files = $MaximumFiles; maximum_bytes = $MaximumBytes }
        file_count = $inventory.Count
        total_bytes = $totalBytes
        files = $inventory
        byte_parity_verified = $true
    })
    return [ordered]@{
        status = "PASS"
        inventory_path = $inventoryPath
        file_count = $inventory.Count
        total_bytes = $totalBytes
        byte_parity_verified = $true
    }
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

function Test-PathInside([string]$Candidate, [string]$Root) {
    $candidateFull = [System.IO.Path]::GetFullPath($Candidate)
    $rootFull = [System.IO.Path]::GetFullPath($Root).TrimEnd('\') + '\'
    return $candidateFull.StartsWith($rootFull, [System.StringComparison]::OrdinalIgnoreCase)
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
    $targetFull = [System.IO.Path]::GetFullPath($TargetSaveDir)
    $defaultFull = [System.IO.Path]::GetFullPath("C:\ProgramData\KMTech\Label_Match\data")
    $expectedCustomSavePath = if ($targetFull.Equals($defaultFull, [System.StringComparison]::OrdinalIgnoreCase)) { "" } else { $targetFull }
    $settingsAlreadyMatch = $true
    foreach ($candidatePath in $settingsPaths) {
        if (-not (Test-Path -LiteralPath $candidatePath -PathType Leaf)) {
            $settingsAlreadyMatch = $false
            break
        }
        try {
            $candidatePayload = Get-Content -LiteralPath $candidatePath -Raw -Encoding UTF8 | ConvertFrom-Json
            if ([string]$candidatePayload.custom_save_path -cne $expectedCustomSavePath) {
                $settingsAlreadyMatch = $false
                break
            }
        }
        catch {
            $settingsAlreadyMatch = $false
            break
        }
    }
    if ($settingsAlreadyMatch) { return $settingsPath }
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
    $payload["custom_save_path"] = $expectedCustomSavePath
    foreach ($targetPath in $settingsPaths) {
        Write-Utf8JsonFile $targetPath $payload
    }
    return $settingsPath
}

$appRoot = [System.IO.Path]::GetFullPath((Split-Path -Parent $MyInvocation.MyCommand.Path))
$managedAppRoot = if ([string]::IsNullOrWhiteSpace($ManagedInstallRoot)) {
    $appRoot
}
else {
    [System.IO.Path]::GetFullPath($ManagedInstallRoot)
}
$toolsDir = Join-Path $appRoot "tools"
$embeddedPythonHost = Join-Path $toolsDir "invoke_embedded_python.ps1"
$installPackScript = Join-Path $toolsDir "direct_sync_relay_install_pack.py"
$runnerScript = Join-Path $toolsDir "direct_sync_relay_runner.py"
$runnerExe = Join-Path $toolsDir "direct_sync_relay_runner.exe"
$registrationScript = Join-Path $toolsDir "register_label_match_worker_pc.py"
foreach ($requiredSource in @($embeddedPythonHost, $installPackScript, $runnerScript, $registrationScript)) {
    if (-not (Test-Path -LiteralPath $requiredSource -PathType Leaf)) {
        throw "In-process installer source is missing. Missing: $requiredSource"
    }
}
if (-not (Test-Path -LiteralPath $runnerExe -PathType Leaf)) {
    throw "Packaged scheduled relay runner is missing. Missing: $runnerExe"
}
. $embeddedPythonHost

function Invoke-InstallPackInProcess([string[]]$Arguments) {
    return [int](Invoke-KMTechEmbeddedPython `
        -AppRoot $appRoot `
        -ScriptPath $installPackScript `
        -Arguments $Arguments)
}
$reportDir = Join-Path $ProgramDataRoot "status"
$reportPath = Join-Path $reportDir "label_match_direct_sync_install.json"
$registrationReportPath = Join-Path $reportDir "label_match_worker_pc_registration.json"
$expectedInstallRoot = "C:\KMTech\Apps\Label_Match\current"
$expectedDirectSyncRoot = "C:\ProgramData\KMTech\DirectSync\label_match"
$expectedTaskName = "direct-sync-relay-label-match"
$expectedTaskLauncherPath = Join-Path $expectedDirectSyncRoot "bin\run_direct-sync-relay-label-match.vbs"
$expectedStateDbPath = Join-Path $expectedDirectSyncRoot "queue\direct_sync_relay.sqlite3"
$actualInstallRoot = $managedAppRoot
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
$commonProgramsRoot = "C:\ProgramData\Microsoft\Windows\Start Menu\Programs"
if (-not [string]::IsNullOrWhiteSpace($CommonProgramsRootForTest)) {
    if (-not $localTestOverrideEnabled) {
        throw "CommonProgramsRootForTest requires KMTECH_FACTORY_INSTALL_TEST_MODE=1."
    }
    $commonProgramsRoot = [System.IO.Path]::GetFullPath($CommonProgramsRootForTest)
}
$allUsersLauncherPath = Join-Path $commonProgramsRoot "KMTech\Label Match.lnk"
$expectedAppExecutable = Join-Path $managedAppRoot "Label_Match.exe"
foreach ($managedPathCheck in @(
    $managedAppRoot,
    $ProgramDataRoot,
    $ScanSourceDir,
    $LogisticsProfilePath,
    $allUsersLauncherPath
)) {
    Assert-NoReparsePath $managedPathCheck "managed installer path"
}
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
    expected_all_users_launcher_path = "C:\ProgramData\Microsoft\Windows\Start Menu\Programs\KMTech\Label Match.lnk"
    actual_all_users_launcher_path = $allUsersLauncherPath
}

if (-not $DryRun.IsPresent -and -not ($Uninstall.IsPresent -or $Rollback.IsPresent) -and -not $productionLayoutMatches -and -not $localTestOverrideEnabled) {
    $blockedPlan = [ordered]@{
        report_version = "label-match-direct-sync-install-pack-v2"
        status = "BLOCKED"
        blocked_reason = if ($AllowNoncanonicalLayoutForTest.IsPresent) { "noncanonical layout override requires KMTECH_FACTORY_INSTALL_TEST_MODE=1" } else { "production install requires the canonical Label_Match field layout" }
        apply = $true
        uninstall = $false
        field_layout_contract = $fieldLayoutContract
    }
    Write-Utf8JsonFile $reportPath $blockedPlan
    Write-Utf8JsonFile (Join-Path $reportDir "label_match_one_step_install_summary.json") ([ordered]@{
        installer_report_version = "label-match-direct-sync-one-step-install-v2"
        status = "BLOCKED"
        blocked_reason = $blockedPlan.blocked_reason
        exit_code = 2
        source_host_id = $resolvedSourceHostId
        field_layout_contract = $fieldLayoutContract
    })
    if ($null -ne $PublicWrapperExitCode) {
        $PublicWrapperExitCode.Value = 2
        return
    }
    exit 2
}

if ($Uninstall.IsPresent -and $Rollback.IsPresent) {
    throw "Uninstall and Rollback are mutually exclusive."
}

if ($Uninstall.IsPresent -or $Rollback.IsPresent) {
    if ($DryRun.IsPresent) { throw "Removal modes cannot be combined with DryRun." }
    $labelMatchProcesses = @(Get-Process -ErrorAction Stop | Where-Object { [string]$_.ProcessName -eq "Label_Match" })
    if ($labelMatchProcesses.Count -gt 0) {
        throw "Label_Match is still running; close the application before removal."
    }
    New-Item -ItemType Directory -Path $reportDir -Force | Out-Null
    $summaryPath = Join-Path $reportDir "label_match_one_step_install_summary.json"
    $installSummary = Read-RequiredInstallSummary `
        $summaryPath $SourceManifestSha256 $managedAppRoot $ProgramDataRoot $ScanSourceDir `
        $TaskName $allUsersLauncherPath $LogisticsProfilePath
    if ($Rollback.IsPresent) {
        if ([string]::IsNullOrWhiteSpace($EvidenceArchiveRoot) -or -not [System.IO.Path]::IsPathRooted($EvidenceArchiveRoot)) {
            throw "Rollback requires an absolute EvidenceArchiveRoot."
        }
        $EvidenceArchiveRoot = [System.IO.Path]::GetFullPath($EvidenceArchiveRoot)
        foreach ($managedPath in @($managedAppRoot, $ProgramDataRoot, $ScanSourceDir)) {
            if (
                (Test-SamePath $EvidenceArchiveRoot $managedPath) -or
                $EvidenceArchiveRoot.StartsWith([System.IO.Path]::GetFullPath($managedPath).TrimEnd('\') + '\', [System.StringComparison]::OrdinalIgnoreCase) -or
                [System.IO.Path]::GetFullPath($managedPath).StartsWith($EvidenceArchiveRoot.TrimEnd('\') + '\', [System.StringComparison]::OrdinalIgnoreCase)
            ) {
                throw "Rollback evidence must be outside managed paths."
            }
        }
        $freshResources = @(
            $installSummary.resources.app_root,
            $installSummary.resources.launcher,
            $installSummary.resources.scheduled_task,
            $installSummary.resources.direct_sync_root,
            $installSummary.resources.data_root,
            $installSummary.resources.machine_profile_root
        )
        foreach ($resource in $freshResources) {
            if ([string]$resource.prestate -cne "absent") {
                throw "Exact rollback is allowed only when every active resource records an absent prestate."
            }
        }
        if (Test-Path -LiteralPath $EvidenceArchiveRoot) {
            throw "Rollback EvidenceArchiveRoot must be a fresh absent path."
        }
        Assert-NoReparsePath $EvidenceArchiveRoot "rollback evidence root"
    }

    function Invoke-TaskRemovalPhase([string]$Phase, [string]$PhaseReportPath) {
        if (Test-Path -LiteralPath $PhaseReportPath) {
            throw "Typed scheduled-task $Phase report path must be absent before execution."
        }
        $phaseArguments = @(
            "--app-root", $managedAppRoot,
            "--program-data-root", $ProgramDataRoot,
            "--scan-source-dir", $ScanSourceDir,
            "--task-name", $TaskName,
            "--report-path", $PhaseReportPath,
            "--task-removal-phase", $Phase,
            "--apply"
        )
        $phaseArguments += if ($Rollback.IsPresent) { "--rollback" } else { "--uninstall" }
        if ($AllowNoncanonicalLayoutForTest.IsPresent) { $phaseArguments += "--allow-noncanonical-layout-for-test" }
        $phaseStartedUtc = [DateTime]::UtcNow
        $phaseExitCode = Invoke-InstallPackInProcess -Arguments $phaseArguments
        if ($phaseExitCode -ne 0) { throw "Typed scheduled-task $Phase phase failed." }
        if (-not (Test-Path -LiteralPath $PhaseReportPath -PathType Leaf)) {
            throw "Typed scheduled-task $Phase phase did not create its report."
        }
        if ((Get-Item -LiteralPath $PhaseReportPath).LastWriteTimeUtc -lt $phaseStartedUtc.AddSeconds(-2)) {
            throw "Typed scheduled-task $Phase report is not fresh."
        }
        $phaseReport = Get-Content -LiteralPath $PhaseReportPath -Raw -Encoding UTF8 | ConvertFrom-Json
        $expectedMode = if ($Rollback.IsPresent) { "exact_rollback_task_phase" } else { "safe_uninstall" }
        $lifecycle = $phaseReport.scheduled_task_lifecycle
        if (
            $phaseReport.report_version -cne "label-match-direct-sync-install-pack-v2" -or
            $phaseReport.status -cne "PASS" -or
            $phaseReport.apply -ne $true -or
            $phaseReport.rollback -ne $Rollback.IsPresent -or
            $phaseReport.uninstall -ne $Uninstall.IsPresent -or
            [string]$phaseReport.operation_mode -cne $expectedMode -or
            [string]$phaseReport.task_name -cne $TaskName -or
            [string]$phaseReport.task_removal_phase -cne $Phase -or
            $lifecycle.status -cne "PASS" -or
            [string]$lifecycle.task_name -cne $TaskName -or
            [string]$lifecycle.phase -cne $Phase
        ) {
            throw "Typed scheduled-task $Phase report is stale, mismatched, or not PASS."
        }
        if ($Phase -ceq "stop" -and $lifecycle.stopped_or_absent -ne $true -and $lifecycle.absence_proven -ne $true) {
            throw "Typed scheduled-task stop did not prove stopped-or-absent state."
        }
        if ($Phase -ceq "delete" -and $lifecycle.absence_proven -ne $true) {
            throw "Typed scheduled-task delete did not prove absence."
        }
        return $lifecycle
    }

    $taskStopReportPath = Join-Path $reportDir "label_match_task_stop_report.json"
    $taskStop = Invoke-TaskRemovalPhase "stop" $taskStopReportPath

    $evidence = $null
    if ($Rollback.IsPresent) {
        $evidence = Copy-BoundedRollbackEvidence $ScanSourceDir $ProgramDataRoot $EvidenceArchiveRoot
        if ($evidence.status -cne "PASS" -or $evidence.byte_parity_verified -ne $true) {
            throw "Rollback evidence preservation did not prove byte parity."
        }
    }

    $taskDeleteReportPath = Join-Path $reportDir "label_match_task_delete_report.json"
    $taskDelete = Invoke-TaskRemovalPhase "delete" $taskDeleteReportPath
    if ($taskDelete.absence_proven -ne $true) {
        throw "Scheduled-task absence was not proven after deletion."
    }

    $launcherResource = $installSummary.resources.launcher
    if ([string]$launcherResource.disposition -ceq "created") {
        if (Test-Path -LiteralPath $allUsersLauncherPath -PathType Leaf) {
            $properties = Get-ShortcutProperties $allUsersLauncherPath
            if (
                -not (Test-ShortcutContract $properties $expectedAppExecutable $managedAppRoot) -or
                (Get-FileSha256 $allUsersLauncherPath) -cne [string]$launcherResource.sha256
            ) {
                throw "Owned all-users launcher drifted; refusing removal."
            }
        }
    }
    $launcherDisposition = Remove-OwnedFile $allUsersLauncherPath $launcherResource "all-users launcher"

    $directSyncFileDispositions = @()
    foreach ($resource in @($installSummary.resources.direct_sync_owned_files)) {
        $directSyncFileDispositions += Remove-OwnedFile ([string]$resource.path) $resource "DirectSync owned file"
    }
    $machineProfileFileDispositions = @()
    foreach ($resource in @($installSummary.resources.machine_profile_files)) {
        $machineProfileFileDispositions += Remove-OwnedFile ([string]$resource.path) $resource "machine profile file"
    }

    $machineProfileRootDisposition = [ordered]@{
        path = [string]$installSummary.resources.machine_profile_root.path
        disposition = "preserved_preexisting"
        absence_proven = $false
    }
    if ([string]$installSummary.resources.machine_profile_root.prestate -ceq "absent") {
        $profileRoot = [string]$installSummary.resources.machine_profile_root.path
        if (Test-Path -LiteralPath $profileRoot -PathType Container) {
            Assert-NoReparsePath $profileRoot "machine profile cleanup root"
            foreach ($profileItem in Get-ChildItem -LiteralPath $profileRoot -Force -Recurse) {
                if (($profileItem.Attributes -band [System.IO.FileAttributes]::ReparsePoint) -ne 0) {
                    throw "Machine profile root contains a reparse point."
                }
            }
            if (@(Get-ChildItem -LiteralPath $profileRoot -Force -File -Recurse).Count -gt 0) {
                throw "Machine profile root contains an unclassified rollback remnant."
            }
            Get-ChildItem -LiteralPath $profileRoot -Force -Directory -Recurse |
                Sort-Object FullName -Descending |
                Remove-Item -Force -ErrorAction Stop
            Remove-Item -LiteralPath $profileRoot -Force -ErrorAction Stop
        }
        if (Test-Path -LiteralPath $profileRoot) { throw "Machine profile root remains after owned cleanup." }
        $machineProfileRootDisposition.disposition = "removed"
        $machineProfileRootDisposition.absence_proven = $true
    }
    $launcherParentRemovedPaths = @()

    $resourceReport = [ordered]@{
        report_version = if ($Rollback.IsPresent) { "label-match-exact-rollback-resources-v1" } else { "label-match-data-preserving-uninstall-v1" }
        status = "PASS"
        mode = if ($Rollback.IsPresent) { "EXACT_FRESH_TARGET_ROLLBACK" } else { "DATA_PRESERVING_UNINSTALL" }
        source_manifest_sha256 = $SourceManifestSha256
        task = [ordered]@{ stop = $taskStop; delete_and_absence = $taskDelete }
        resources = [ordered]@{
            app_root = [ordered]@{ path = $managedAppRoot; disposition = "pending_public_wrapper_removal"; absence_proven = $false }
            launcher = $launcherDisposition
            launcher_parent = [ordered]@{
                path = Split-Path -Parent $allUsersLauncherPath
                recorded_created_paths = @($installSummary.resources.created_directory_paths.launcher_parent)
                removed_paths = $launcherParentRemovedPaths
                disposition = if ($Rollback.IsPresent) { "pending_public_wrapper_ancestry_cleanup" } else { "preserved" }
            }
            direct_sync_owned_files = $directSyncFileDispositions
            machine_profile_files = $machineProfileFileDispositions
            machine_profile_root = $machineProfileRootDisposition
            data_root = [ordered]@{ path = [System.IO.Path]::GetFullPath($ScanSourceDir); disposition = "preserved" }
            direct_sync_business_evidence = [ordered]@{ path = [System.IO.Path]::GetFullPath($ProgramDataRoot); disposition = "preserved" }
        }
        evidence = $evidence
        pre_install_parity_claimed = $false
    }

    if ($Rollback.IsPresent) {
        foreach ($rootToRemove in @($ScanSourceDir, $ProgramDataRoot)) {
            if (Test-Path -LiteralPath $rootToRemove -PathType Container) {
                Assert-NoReparsePath $rootToRemove "rollback cleanup root"
                foreach ($item in Get-ChildItem -LiteralPath $rootToRemove -Force -Recurse) {
                    if (($item.Attributes -band [System.IO.FileAttributes]::ReparsePoint) -ne 0) {
                        throw "Refusing rollback cleanup through a reparse point: $($item.FullName)"
                    }
                }
                Remove-Item -LiteralPath $rootToRemove -Recurse -Force -ErrorAction Stop
            }
            if (Test-Path -LiteralPath $rootToRemove) { throw "Rollback resource remains: $rootToRemove" }
        }
        $dataParentRemovedPaths = @()
        $directSyncParentRemovedPaths = @()
        $machineProfileParentRemovedPaths = @()
        $resourceReport.resources.data_root.disposition = "removed_after_evidence"
        $resourceReport.resources.data_root.absence_proven = $true
        $resourceReport.resources.data_root.parent_paths_removed = $dataParentRemovedPaths
        $resourceReport.resources.direct_sync_business_evidence.disposition = "removed_after_evidence"
        $resourceReport.resources.direct_sync_business_evidence.absence_proven = $true
        $resourceReport.resources.direct_sync_business_evidence.parent_paths_removed = $directSyncParentRemovedPaths
        $resourceReport.resources.machine_profile_root.parent_paths_removed = $machineProfileParentRemovedPaths
        $resourceReport.pre_install_parity_claimed = $false
        $rollbackReportPath = Join-Path $EvidenceArchiveRoot "label_match_rollback_resources.json"
        Write-Utf8JsonFile $rollbackReportPath $resourceReport
    }
    else {
        $uninstallReportPath = Join-Path $reportDir "label_match_uninstall_report.json"
        Write-Utf8JsonFile $uninstallReportPath $resourceReport
    }
    if ($null -ne $PublicWrapperExitCode) {
        $PublicWrapperExitCode.Value = 0
        return
    }
    exit 0
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

$scanSourceExistedBefore = Test-Path -LiteralPath $ScanSourceDir
$directSyncRootExistedBefore = Test-Path -LiteralPath $ProgramDataRoot
$scanSourceCreatedDirectories = @(Get-MissingDirectoryChain $ScanSourceDir)
$directSyncCreatedDirectories = @(Get-MissingDirectoryChain $ProgramDataRoot)
$taskWrapperPath = Join-Path $ProgramDataRoot ("bin\run_{0}.ps1" -f $TaskName)
$taskLauncherPath = Join-Path $ProgramDataRoot ("bin\run_{0}.vbs" -f $TaskName)
$producerManifestPath = if ([string]::IsNullOrWhiteSpace($ExistingProducerManifestPath)) { Join-Path $ProgramDataRoot "producer_manifest.json" } else { $ExistingProducerManifestPath }
$credentialPath = if ([string]::IsNullOrWhiteSpace($ExistingCredentialPath)) { Join-Path $ProgramDataRoot "credential.json" } else { $ExistingCredentialPath }
$logisticsProfileRoot = Split-Path -Parent $LogisticsProfilePath
$logisticsProfileRootExistedBefore = Test-Path -LiteralPath $logisticsProfileRoot
$machineProfileCreatedDirectories = @(Get-MissingDirectoryChain $logisticsProfileRoot)
$launcherParentCreatedDirectories = @(Get-MissingDirectoryChain (Split-Path -Parent $allUsersLauncherPath))
$machineSecretPath = Join-Path $logisticsProfileRoot "secrets\bearer-token.dpapi"
$resourcePrestate = [ordered]@{
    task_wrapper = Test-Path -LiteralPath $taskWrapperPath -PathType Leaf
    task_launcher = Test-Path -LiteralPath $taskLauncherPath -PathType Leaf
    producer_manifest = Test-Path -LiteralPath $producerManifestPath -PathType Leaf
    credential = Test-Path -LiteralPath $credentialPath -PathType Leaf
    logistics_profile = Test-Path -LiteralPath $LogisticsProfilePath -PathType Leaf
    machine_secret = Test-Path -LiteralPath $machineSecretPath -PathType Leaf
    launcher = Test-Path -LiteralPath $allUsersLauncherPath -PathType Leaf
}
if (-not $DryRun.IsPresent -and $InstallPrestate -ceq "absent") {
    foreach ($property in $resourcePrestate.GetEnumerator()) {
        if ($reuseExistingIdentity -and $property.Key -in @("producer_manifest", "credential")) {
            continue
        }
        if ($property.Value) { throw "Fresh install found an unexpected pre-existing owned resource: $($property.Key)" }
    }
    $existingTasks = @(Get-ScheduledTask -ErrorAction Stop | Where-Object { [string]$_.TaskPath -eq '\' -and [string]$_.TaskName -eq $TaskName })
    if ($existingTasks.Count -ne 0) { throw "Fresh install found an unexpected same-named scheduled task." }
}
elseif (-not $DryRun.IsPresent -and $InstallPrestate -ceq "exact_reused") {
    $priorSummaryPath = Join-Path $reportDir "label_match_one_step_install_summary.json"
    $priorSummary = Read-RequiredInstallSummary `
        $priorSummaryPath $SourceManifestSha256 $managedAppRoot $ProgramDataRoot $ScanSourceDir `
        $TaskName $allUsersLauncherPath $LogisticsProfilePath
    $scanSourceCreatedDirectories = @($priorSummary.resources.created_directory_paths.data_root)
    $directSyncCreatedDirectories = @($priorSummary.resources.created_directory_paths.direct_sync_root)
    $machineProfileCreatedDirectories = @($priorSummary.resources.created_directory_paths.machine_profile_root)
    $launcherParentCreatedDirectories = @($priorSummary.resources.created_directory_paths.launcher_parent)
    $priorTasks = @(Get-ScheduledTask -ErrorAction Stop | Where-Object { [string]$_.TaskPath -eq '\' -and [string]$_.TaskName -eq $TaskName })
    $priorActions = if ($priorTasks.Count -eq 1) { @($priorTasks[0].Actions) } else { @() }
    $observedTaskAction = if ($priorActions.Count -eq 1) {
        ("{0} {1}" -f [string]$priorActions[0].Execute, [string]$priorActions[0].Arguments).Trim()
    }
    else {
        ""
    }
    if (
        $priorActions.Count -ne 1 -or
        [string]::IsNullOrWhiteSpace([string]$priorSummary.resources.scheduled_task.expected_action) -or
        $observedTaskAction -cne [string]$priorSummary.resources.scheduled_task.expected_action
    ) {
        throw "Existing scheduled task drifted from the owned install summary."
    }
    foreach ($resource in @($priorSummary.resources.direct_sync_owned_files) + @($priorSummary.resources.machine_profile_files)) {
        if (-not [string]::IsNullOrWhiteSpace([string]$resource.sha256)) {
            $resourceFileExists = Test-Path -LiteralPath ([string]$resource.path) -PathType Leaf
            if (-not $resourceFileExists) {
                throw "Existing installer-owned file drifted from the install summary: $($resource.path)"
            }
            if ((Get-FileSha256 ([string]$resource.path)) -cne [string]$resource.sha256) {
                throw "Existing installer-owned file drifted from the install summary: $($resource.path)"
            }
        }
    }
    $priorLauncher = $priorSummary.resources.launcher
    $priorLauncherExists = Test-Path -LiteralPath $allUsersLauncherPath -PathType Leaf
    if (-not $priorLauncherExists) {
        throw "Existing all-users launcher drifted from the owned install summary."
    }
    if (
        [string]::IsNullOrWhiteSpace([string]$priorLauncher.sha256) -or
        (Get-FileSha256 $allUsersLauncherPath) -cne [string]$priorLauncher.sha256 -or
        -not (Test-ShortcutContract (Get-ShortcutProperties $allUsersLauncherPath) $expectedAppExecutable $managedAppRoot)
    ) {
        throw "Existing all-users launcher drifted from the owned install summary."
    }
    foreach ($rootResource in @(
        $priorSummary.resources.direct_sync_root,
        $priorSummary.resources.data_root,
        $priorSummary.resources.machine_profile_root
    )) {
        if ([string]$rootResource.disposition -ceq "reused") {
            if (-not (Test-Path -LiteralPath ([string]$rootResource.path) -PathType Container)) {
                throw "Existing reused resource root disappeared after the owned install."
            }
        }
    }
}

New-Item -ItemType Directory -Path $ScanSourceDir -Force | Out-Null
New-Item -ItemType Directory -Path $reportDir -Force | Out-Null
$settingsPath = Set-LabelMatchSavePath -AppRoot $managedAppRoot -TargetSaveDir $ScanSourceDir

$arguments = @(
    "--app-root", $managedAppRoot,
    "--server-base-url", $ServerBaseUrl,
    "--program-data-root", $ProgramDataRoot,
    "--scan-source-dir", $ScanSourceDir,
    "--source-host-id", $resolvedSourceHostId,
    "--app-run-user", $AppRunUser,
    "--task-name", $TaskName,
    "--report-path", $reportPath,
    "--app-settings-path", $settingsPath,
    "--runner-exe", $runnerExe
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

$exitCode = Invoke-InstallPackInProcess -Arguments $arguments

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
$launcherRecord = [ordered]@{
    path = [System.IO.Path]::GetFullPath($allUsersLauncherPath)
    scope = "all_users"
    prestate = if ($resourcePrestate.launcher) { "exact_reused" } else { "absent" }
    disposition = "planned"
    target_path = [System.IO.Path]::GetFullPath($expectedAppExecutable)
    working_directory = [System.IO.Path]::GetFullPath($managedAppRoot)
    icon_path = [System.IO.Path]::GetFullPath($expectedAppExecutable)
    sha256 = $null
    verified = $false
}
if ($exitCode -eq 0 -and -not $DryRun.IsPresent) {
    try {
        Start-ScheduledTask -TaskName $TaskName -ErrorAction Stop
        $taskStartStatus = "STARTED"
        $launcherRecord = Ensure-AllUsersLauncher $allUsersLauncherPath $expectedAppExecutable $managedAppRoot
    }
    catch {
        $taskStartStatus = "FAILED"
        $taskStartError = $_.Exception.Message
        $exitCode = 1
        $cleanupReportPath = Join-Path $reportDir "label_match_failed_install_task_cleanup.json"
        $cleanupArguments = @(
            "--app-root", $managedAppRoot,
            "--program-data-root", $ProgramDataRoot,
            "--scan-source-dir", $ScanSourceDir,
            "--task-name", $TaskName,
            "--report-path", $cleanupReportPath,
            "--task-removal-phase", "full",
            "--apply",
            "--uninstall"
        )
        if ($AllowNoncanonicalLayoutForTest.IsPresent) { $cleanupArguments += "--allow-noncanonical-layout-for-test" }
        $cleanupExitCode = Invoke-InstallPackInProcess -Arguments $cleanupArguments
        if ($cleanupExitCode -ne 0) { $taskStartError += "; typed task cleanup failed" }
        if (-not $resourcePrestate.launcher) {
            if (Test-Path -LiteralPath $allUsersLauncherPath -PathType Leaf) {
                Remove-Item -LiteralPath $allUsersLauncherPath -Force -ErrorAction Stop
            }
        }
        try {
            Remove-NewMachineProfilesFromRegistrationReport $registrationReportPath $LogisticsProfilePath
        }
        catch {
            $taskStartError += "; machine profile rollback failed: $($_.Exception.Message)"
        }
    }
}

if ($exitCode -ne 0 -and -not $DryRun.IsPresent -and $taskStartStatus -cne "FAILED") {
    $cleanupReportPath = Join-Path $reportDir "label_match_failed_install_task_cleanup.json"
    $cleanupArguments = @(
        "--app-root", $managedAppRoot,
        "--program-data-root", $ProgramDataRoot,
        "--scan-source-dir", $ScanSourceDir,
        "--task-name", $TaskName,
        "--report-path", $cleanupReportPath,
        "--task-removal-phase", "full",
        "--apply",
        "--uninstall"
    )
    if ($AllowNoncanonicalLayoutForTest.IsPresent) { $cleanupArguments += "--allow-noncanonical-layout-for-test" }
    $cleanupExitCode = Invoke-InstallPackInProcess -Arguments $cleanupArguments
    if ($cleanupExitCode -ne 0) { $taskStartError = "typed task cleanup failed after installer failure" }
    try {
        Remove-NewMachineProfilesFromRegistrationReport $registrationReportPath $LogisticsProfilePath
    }
    catch {
        $taskStartError = "machine profile cleanup failed after installer failure: $($_.Exception.Message)"
    }
    foreach ($candidate in @(
        [ordered]@{ path = $taskWrapperPath; existed = [bool]$resourcePrestate.task_wrapper },
        [ordered]@{ path = $taskLauncherPath; existed = [bool]$resourcePrestate.task_launcher },
        [ordered]@{ path = $producerManifestPath; existed = [bool]$resourcePrestate.producer_manifest },
        [ordered]@{ path = $credentialPath; existed = [bool]$resourcePrestate.credential }
    )) {
        if (-not $candidate.existed) {
            if (Test-Path -LiteralPath $candidate.path -PathType Leaf) {
                Remove-Item -LiteralPath $candidate.path -Force -ErrorAction Stop
            }
        }
    }
}

$appInventory = Get-ImmutableAppInventoryIdentity $managedAppRoot $MutableAppRelativePaths
$directSyncOwnedFiles = @(
    Get-FileResourceRecord $taskWrapperPath ([bool]$resourcePrestate.task_wrapper)
    Get-FileResourceRecord $taskLauncherPath ([bool]$resourcePrestate.task_launcher)
    Get-FileResourceRecord $producerManifestPath ([bool]$resourcePrestate.producer_manifest)
    Get-FileResourceRecord $credentialPath ([bool]$resourcePrestate.credential)
)
$machineProfileFiles = @(
    Get-FileResourceRecord $LogisticsProfilePath ([bool]$resourcePrestate.logistics_profile)
    Get-FileResourceRecord $machineSecretPath ([bool]$resourcePrestate.machine_secret)
)
if ($InstallPrestate -ceq "exact_reused" -and $null -ne $priorSummary) {
    $launcherRecord.prestate = [string]$priorSummary.resources.launcher.prestate
    $launcherRecord.disposition = [string]$priorSummary.resources.launcher.disposition
    foreach ($current in @($directSyncOwnedFiles)) {
        $prior = @($priorSummary.resources.direct_sync_owned_files) | Where-Object {
            Test-SamePath ([string]$_.path) ([string]$current.path)
        } | Select-Object -First 1
        if ($null -ne $prior) {
            $current.prestate = [string]$prior.prestate
            $current.disposition = [string]$prior.disposition
        }
    }
    foreach ($current in @($machineProfileFiles)) {
        $prior = @($priorSummary.resources.machine_profile_files) | Where-Object {
            Test-SamePath ([string]$_.path) ([string]$current.path)
        } | Select-Object -First 1
        if ($null -ne $prior) {
            $current.prestate = [string]$prior.prestate
            $current.disposition = [string]$prior.disposition
        }
    }
}
$expectedTaskActionText = $null
if ($null -ne $installReport -and $null -ne $installReport.scheduled_task_create_command) {
    $taskCreateCommand = @($installReport.scheduled_task_create_command)
    $taskActionIndex = [Array]::IndexOf($taskCreateCommand, "/TR")
    if ($taskActionIndex -ge 0 -and $taskActionIndex + 1 -lt $taskCreateCommand.Count) {
        $expectedTaskActionText = [string]$taskCreateCommand[$taskActionIndex + 1]
    }
}

$summary = [ordered]@{
    installer_report_version = "label-match-direct-sync-one-step-install-v2"
    status = if ($exitCode -eq 0) { if ($DryRun.IsPresent) { "DRY_RUN" } else { "PASS" } } else { "FAILED" }
    blocked_reason = if ($taskStartStatus -eq "FAILED") { "scheduled task immediate start failed" } elseif ($null -ne $installReport) { $installReport.blocked_reason } else { $null }
    registration_blocked_reason = if ($null -ne $registrationSummary) { $registrationSummary.blocked_reason } else { $null }
    exit_code = $exitCode
    app_root = $managedAppRoot
    source_manifest_sha256 = $SourceManifestSha256
    settings_path = $settingsPath
    scan_source_dir = [System.IO.Path]::GetFullPath($ScanSourceDir)
    program_data_root = [System.IO.Path]::GetFullPath($ProgramDataRoot)
    logistics_profile_path = [System.IO.Path]::GetFullPath($LogisticsProfilePath)
    install_pack_report_path = [System.IO.Path]::GetFullPath($reportPath)
    enrollment_token_file_present = -not [string]::IsNullOrWhiteSpace($EnrollmentTokenFile)
    existing_identity_reused = $reuseExistingIdentity
    installer_execution_mode = "in_process_embedded_python"
    embedded_python_host_present = Test-Path -LiteralPath $embeddedPythonHost
    python_runner_script_present = Test-Path -LiteralPath $runnerScript
    python_registration_script_present = Test-Path -LiteralPath $registrationScript
    task_name = $TaskName
    field_layout_contract = if ($null -ne $installReport -and $null -ne $installReport.field_layout_contract) { $installReport.field_layout_contract } else { $fieldLayoutContract }
    scheduled_task_start = [ordered]@{
        status = $taskStartStatus
        error = $taskStartError
    }
    app_run_user = $AppRunUser
    app_runtime_acl = if ($null -ne $installReport) { $installReport.app_runtime_acl } else { $null }
    source_host_id = if ($null -ne $registrationSummary) { $registrationSummary.source_host_id } else { $resolvedSourceHostId }
    producer_install_id = if ($null -ne $registrationSummary) { $registrationSummary.producer_install_id } else { $null }
    producer_id = if ($null -ne $registrationSummary) { $registrationSummary.producer_id } else { $null }
    key_id = if ($null -ne $registrationSummary) { $registrationSummary.key_id } else { $null }
    manual_pc_approval_required = if ($null -ne $registrationSummary) { $registrationSummary.manual_pc_approval_required } else { $null }
    resources = [ordered]@{
        created_directory_paths = [ordered]@{
            data_root = @($scanSourceCreatedDirectories)
            direct_sync_root = @($directSyncCreatedDirectories)
            machine_profile_root = @($machineProfileCreatedDirectories)
            launcher_parent = @($launcherParentCreatedDirectories)
        }
        app_root = [ordered]@{
            path = [System.IO.Path]::GetFullPath($managedAppRoot)
            install_prestate = if ($InstallPrestate -ceq "exact_reused") { [string]$priorSummary.resources.app_root.install_prestate } else { $InstallPrestate }
            prestate = if ($InstallPrestate -ceq "exact_reused") { [string]$priorSummary.resources.app_root.prestate } else { $InstallPrestate }
            disposition = if ($InstallPrestate -ceq "exact_reused") { [string]$priorSummary.resources.app_root.disposition } else { "created" }
            inventory_contract = "label-match-app-immutable-inventory-v1"
            mutable_relative_paths = @($MutableAppRelativePaths)
            immutable_file_count = $appInventory.immutable_file_count
            immutable_inventory_sha256 = $appInventory.immutable_sha256
        }
        launcher = $launcherRecord
        scheduled_task = [ordered]@{
            name = $TaskName
            prestate = if ($InstallPrestate -ceq "absent") { "absent" } else { [string]$priorSummary.resources.scheduled_task.prestate }
            disposition = if ($DryRun.IsPresent) { "planned" } elseif ($exitCode -ne 0) { "cleanup_attempted" } elseif ($InstallPrestate -ceq "exact_reused") { [string]$priorSummary.resources.scheduled_task.disposition } else { "created" }
            immediate_start = $taskStartStatus
            expected_action = $expectedTaskActionText
        }
        direct_sync_root = [ordered]@{
            path = [System.IO.Path]::GetFullPath($ProgramDataRoot)
            prestate = if ($InstallPrestate -ceq "exact_reused") { [string]$priorSummary.resources.direct_sync_root.prestate } elseif ($directSyncRootExistedBefore) { "existing" } else { "absent" }
            disposition = if ($InstallPrestate -ceq "exact_reused") { [string]$priorSummary.resources.direct_sync_root.disposition } elseif ($directSyncRootExistedBefore) { "reused" } else { "created" }
        }
        direct_sync_owned_files = $directSyncOwnedFiles
        machine_profile_files = $machineProfileFiles
        machine_profile_root = [ordered]@{
            path = [System.IO.Path]::GetFullPath($logisticsProfileRoot)
            prestate = if ($InstallPrestate -ceq "exact_reused") { [string]$priorSummary.resources.machine_profile_root.prestate } elseif ($logisticsProfileRootExistedBefore) { "existing" } else { "absent" }
            disposition = if ($InstallPrestate -ceq "exact_reused") { [string]$priorSummary.resources.machine_profile_root.disposition } elseif ($logisticsProfileRootExistedBefore) { "reused" } else { "created" }
        }
        settings = Get-FileResourceRecord $settingsPath $true
        data_root = [ordered]@{
            path = [System.IO.Path]::GetFullPath($ScanSourceDir)
            prestate = if ($InstallPrestate -ceq "exact_reused") { [string]$priorSummary.resources.data_root.prestate } elseif ($scanSourceExistedBefore) { "existing" } else { "absent" }
            disposition = if ($InstallPrestate -ceq "exact_reused") { [string]$priorSummary.resources.data_root.disposition } elseif ($scanSourceExistedBefore) { "reused" } else { "created" }
            uninstall_disposition = "preserved"
        }
    }
    lifecycle_contract = [ordered]@{
        uninstall_mode = "DATA_PRESERVING_UNINSTALL"
        rollback_mode = "EXACT_FRESH_TARGET_ROLLBACK"
        rollback_requires_external_evidence = $true
        task_removal_order = @("stop", "delete", "absence")
        task_results_are_typed = $true
        bounded_evidence_maximum_files = 10000
        bounded_evidence_maximum_bytes = 2147483648
        no_plaintext_secrets_in_reports = $true
        fresh_evidence_root_required = $true
        reparse_points_rejected = $true
        directory_ancestry_tracked = $true
        typed_task_reports_bound_to_phase_and_identity = $true
        public_wrapper_finalizes_rollback_report = $true
        final_evidence_bytes_reverified = $true
        final_receipt_binds_evidence_hashes = $true
        app_inventory_contract = "label-match-app-immutable-inventory-v1"
        mutable_app_relative_paths = @($MutableAppRelativePaths)
        immutable_app_drift_rejected = $true
    }
}
$summaryPath = Join-Path $reportDir "label_match_one_step_install_summary.json"
Write-Utf8JsonFile $summaryPath $summary

if ($null -ne $PublicWrapperExitCode) {
    $PublicWrapperExitCode.Value = [int]$exitCode
    return
}
exit $exitCode
