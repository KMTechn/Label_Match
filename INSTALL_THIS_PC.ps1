param(
    [switch]$DryRun,
    [switch]$Uninstall,
    [switch]$Rollback,
    [string]$EvidenceArchiveRoot = "",
    [switch]$AllowNoncanonicalLayoutForTest,
    [string]$InstallRootForTest = "",
    [string]$CommonProgramsRootForTest = "",
    [string]$RollbackReceiptRootForTest = "",
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

$CanonicalInstallRoot = "C:\KMTech\Apps\Label_Match\current"
$DefaultProgramDataRoot = "C:\ProgramData\KMTech\DirectSync\label_match"
$TestModeEnvironmentName = "KMTECH_FACTORY_INSTALL_TEST_MODE"
$RequiredPackageMembers = @(
    "INSTALL_THIS_PC.ps1",
    "install_label_match_direct_sync.ps1",
    "Label_Match.exe",
    "build-manifest.json",
    "tools/direct_sync_relay_install_pack/direct_sync_relay_install_pack.exe",
    "tools/direct_sync_relay_runner.exe",
    "tools/register_label_match_worker_pc.exe"
)
$MutableAppRelativePaths = @("_internal/config/app_settings.json")

function ConvertTo-ElevationArgument([string]$Value) {
    if ([string]::IsNullOrEmpty($Value)) { return '""' }
    if ($Value -notmatch '[\s"]') { return $Value }
    $builder = New-Object System.Text.StringBuilder
    [void]$builder.Append('"')
    $slashCount = 0
    foreach ($character in $Value.ToCharArray()) {
        if ($character -eq '\') { $slashCount += 1; continue }
        if ($character -eq '"') {
            [void]$builder.Append((('\' * (($slashCount * 2) + 1)) -join ''))
            [void]$builder.Append('"')
            $slashCount = 0
            continue
        }
        if ($slashCount -gt 0) {
            [void]$builder.Append((('\' * $slashCount) -join ''))
            $slashCount = 0
        }
        [void]$builder.Append($character)
    }
    if ($slashCount -gt 0) {
        [void]$builder.Append((('\' * ($slashCount * 2)) -join ''))
    }
    [void]$builder.Append('"')
    return $builder.ToString()
}

function Invoke-SelfElevated([string]$ScriptPath, [System.Collections.IDictionary]$BoundParameters) {
    $identity = [Security.Principal.WindowsIdentity]::GetCurrent()
    $principal = New-Object Security.Principal.WindowsPrincipal($identity)
    if ($principal.IsInRole([Security.Principal.WindowsBuiltInRole]::Administrator)) { return }
    $powershellExe = Join-Path ([Environment]::SystemDirectory) 'WindowsPowerShell\v1.0\powershell.exe'
    $launchArguments = @('-NoProfile', '-ExecutionPolicy', 'Bypass', '-File', $ScriptPath)
    foreach ($entry in $BoundParameters.GetEnumerator()) {
        if ($entry.Value -is [System.Management.Automation.SwitchParameter]) {
            if ($entry.Value.IsPresent) { $launchArguments += "-$($entry.Key)" }
            continue
        }
        $launchArguments += "-$($entry.Key)"
        $launchArguments += [string]$entry.Value
    }
    $argumentLine = ($launchArguments | ForEach-Object { ConvertTo-ElevationArgument $_ }) -join ' '
    $process = Start-Process -FilePath $powershellExe -Verb RunAs -ArgumentList $argumentLine -Wait -PassThru -ErrorAction Stop
    exit $process.ExitCode
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

function Assert-NoReparseTree([string]$Root, [string]$Label) {
    if (-not (Test-Path -LiteralPath $Root -PathType Container)) { return }
    Assert-NoReparsePath $Root $Label
    foreach ($item in Get-ChildItem -LiteralPath $Root -Force -Recurse) {
        if (($item.Attributes -band [System.IO.FileAttributes]::ReparsePoint) -ne 0) {
            throw "$Label contains a reparse point: $($item.FullName)"
        }
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

function Remove-OwnedEmptyDirectoryPaths([object[]]$RecordedPaths, [object[]]$ManagedLeafDirectories) {
    $allowed = @{}
    foreach ($managedLeafDirectory in @($ManagedLeafDirectories)) {
        $cursor = [System.IO.Path]::GetFullPath([string]$managedLeafDirectory)
        while ($true) {
            $parent = [System.IO.Path]::GetDirectoryName($cursor.TrimEnd('\'))
            if ([string]::IsNullOrWhiteSpace($parent) -or $parent -eq $cursor) { break }
            $allowed[$cursor.ToLowerInvariant()] = $true
            $cursor = $parent
        }
    }
    $normalized = @()
    foreach ($recorded in @($RecordedPaths)) {
        $full = [System.IO.Path]::GetFullPath([string]$recorded)
        if (-not $allowed.ContainsKey($full.ToLowerInvariant())) {
            throw "Recorded installer-created directory escapes all managed ancestry: $full"
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

function Get-RelativeFilePath([string]$Root, [string]$Path) {
    $rootFull = [System.IO.Path]::GetFullPath($Root).TrimEnd('\')
    $pathFull = [System.IO.Path]::GetFullPath($Path)
    if (-not $pathFull.StartsWith($rootFull + '\', [System.StringComparison]::OrdinalIgnoreCase)) {
        throw "File escapes package root: $pathFull"
    }
    return $pathFull.Substring($rootFull.Length + 1).Replace('\', '/')
}

function Get-Sha256HexFromText([string]$Text) {
    $sha = [System.Security.Cryptography.SHA256]::Create()
    try {
        $bytes = [System.Text.Encoding]::UTF8.GetBytes($Text)
        return -join ($sha.ComputeHash($bytes) | ForEach-Object { $_.ToString("x2") })
    }
    finally {
        $sha.Dispose()
    }
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
    if (
        -not (Test-PathInside $mutableFull $rootFull) -or
        -not (Test-Path -LiteralPath $mutableFull -PathType Leaf)
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
                    sha256 = (Get-FileHash -LiteralPath $_.FullName -Algorithm SHA256).Hash.ToLowerInvariant()
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
        immutable_sha256 = Get-Sha256HexFromText $builder.ToString()
    }
}

function Assert-AppInventorySummaryContract($AppRootResource) {
    $mutablePaths = @($AppRootResource.mutable_relative_paths)
    if (
        [string]$AppRootResource.inventory_contract -cne "label-match-app-immutable-inventory-v1" -or
        $mutablePaths.Count -ne 1 -or
        [string]$mutablePaths[0] -cne "_internal/config/app_settings.json" -or
        [int]$AppRootResource.immutable_file_count -lt 1 -or
        [string]$AppRootResource.immutable_inventory_sha256 -cnotmatch '^[0-9a-f]{64}$'
    ) {
        throw "Install ownership summary app inventory contract is invalid."
    }
}

function Read-JsonFile([string]$Path, [string]$Label) {
    try {
        return Get-Content -LiteralPath $Path -Raw -Encoding UTF8 | ConvertFrom-Json
    }
    catch {
        throw "$Label is invalid JSON: $Path"
    }
}

function Write-Utf8JsonFile([string]$Path, $Payload) {
    $parent = Split-Path -Parent $Path
    if (-not [string]::IsNullOrWhiteSpace($parent)) {
        New-Item -ItemType Directory -Path $parent -Force | Out-Null
    }
    $json = $Payload | ConvertTo-Json -Depth 30
    $utf8NoBom = New-Object System.Text.UTF8Encoding($false)
    [System.IO.File]::WriteAllText($Path, $json + [System.Environment]::NewLine, $utf8NoBom)
}

function Confirm-BoundedRollbackEvidence([string]$EvidenceRoot, $EvidenceRecord, [string]$ResourceReportPath) {
    $rootFull = [System.IO.Path]::GetFullPath($EvidenceRoot)
    $inventoryPath = Join-Path $rootFull "evidence-inventory.json"
    if (
        -not (Test-Path -LiteralPath $inventoryPath -PathType Leaf) -or
        -not (Test-SamePath ([string]$EvidenceRecord.inventory_path) $inventoryPath) -or
        -not (Test-SamePath $ResourceReportPath (Join-Path $rootFull "label_match_rollback_resources.json"))
    ) {
        throw "Rollback evidence metadata paths are not exact."
    }
    Assert-NoReparseTree $rootFull "rollback evidence root"
    if ([long](Get-Item -LiteralPath $inventoryPath).Length -gt 16777216) {
        throw "Rollback evidence inventory metadata exceeds its bound."
    }
    $inventory = Read-JsonFile $inventoryPath "rollback evidence inventory"
    $files = @($inventory.files)
    if (
        $inventory.schema_version -cne "label-match-rollback-evidence-v1" -or
        $inventory.status -cne "PASS" -or
        [int]$inventory.limits.maximum_files -ne 10000 -or
        [long]$inventory.limits.maximum_bytes -ne 2147483648 -or
        [int]$inventory.file_count -ne $files.Count -or
        $files.Count -gt 10000 -or
        [long]$inventory.total_bytes -lt 0 -or
        [long]$inventory.total_bytes -gt 2147483648 -or
        $inventory.byte_parity_verified -ne $true -or
        [int]$EvidenceRecord.file_count -ne $files.Count -or
        [long]$EvidenceRecord.total_bytes -ne [long]$inventory.total_bytes -or
        $EvidenceRecord.byte_parity_verified -ne $true
    ) {
        throw "Rollback evidence inventory contract is invalid."
    }
    $expectedFiles = @{}
    $computedBytes = [long]0
    foreach ($entry in $files) {
        $entryKeys = @($entry.PSObject.Properties.Name | Sort-Object)
        if (($entryKeys -join ',') -cne 'path,sha256,size') {
            throw "Rollback evidence inventory entry fields differ from the exact contract."
        }
        $relative = [string]$entry.path
        Assert-SafeManifestRelativePath $relative
        if ($relative -cnotmatch '^(label-data|direct-sync)/') {
            throw "Rollback evidence inventory path is outside the allowed evidence namespaces."
        }
        $folded = $relative.ToLowerInvariant()
        if ($expectedFiles.ContainsKey($folded)) {
            throw "Rollback evidence inventory contains a case-colliding path."
        }
        if ([string]$entry.sha256 -cnotmatch '^[0-9a-f]{64}$' -or [long]$entry.size -lt 0) {
            throw "Rollback evidence inventory metadata is invalid."
        }
        $target = Join-Path $rootFull $relative.Replace('/', '\')
        if (-not (Test-PathInside $target $rootFull) -or -not (Test-Path -LiteralPath $target -PathType Leaf)) {
            throw "Rollback evidence inventory file is missing or escaped its root."
        }
        if ([long](Get-Item -LiteralPath $target).Length -ne [long]$entry.size) {
            throw "Rollback evidence file size changed after preservation."
        }
        if ((Get-FileHash -LiteralPath $target -Algorithm SHA256).Hash.ToLowerInvariant() -cne [string]$entry.sha256) {
            throw "Rollback evidence file hash changed after preservation."
        }
        $computedBytes += [long]$entry.size
        if ($computedBytes -gt 2147483648) { throw "Rollback evidence bytes exceed the fixed bound." }
        $expectedFiles[$folded] = $true
    }
    if ($computedBytes -ne [long]$inventory.total_bytes) {
        throw "Rollback evidence inventory total does not match preserved bytes."
    }
    $allowedMetadata = @(
        (Get-RelativeFilePath $rootFull $inventoryPath).ToLowerInvariant(),
        (Get-RelativeFilePath $rootFull $ResourceReportPath).ToLowerInvariant()
    )
    foreach ($actualFile in Get-ChildItem -LiteralPath $rootFull -Force -File -Recurse) {
        $relativeActual = (Get-RelativeFilePath $rootFull $actualFile.FullName).ToLowerInvariant()
        if (-not $expectedFiles.ContainsKey($relativeActual) -and $allowedMetadata -notcontains $relativeActual) {
            throw "Rollback evidence root contains an unexpected file."
        }
    }
    return [ordered]@{
        status = "PASS"
        inventory_path = $inventoryPath
        inventory_sha256 = (Get-FileHash -LiteralPath $inventoryPath -Algorithm SHA256).Hash.ToLowerInvariant()
        file_count = $files.Count
        total_bytes = $computedBytes
        archived_bytes_reverified = $true
    }
}

function Assert-SafeManifestRelativePath([string]$RelativePath) {
    if (
        [string]::IsNullOrWhiteSpace($RelativePath) -or
        [System.IO.Path]::IsPathRooted($RelativePath) -or
        $RelativePath.Contains('\') -or
        $RelativePath.Contains(':') -or
        $RelativePath -match '(^|/)(\.|\.\.)($|/)' -or
        $RelativePath.StartsWith('/') -or
        $RelativePath.EndsWith('/')
    ) {
        throw "Unsafe build-manifest path: $RelativePath"
    }
}

function Assert-PackageRoot([string]$Root) {
    $rootFull = [System.IO.Path]::GetFullPath($Root)
    $rootItem = Get-Item -LiteralPath $rootFull -Force -ErrorAction Stop
    if (($rootItem.Attributes -band [System.IO.FileAttributes]::ReparsePoint) -ne 0) {
        throw "Release package root is a reparse point: $rootFull"
    }
    $manifestPath = Join-Path $rootFull "build-manifest.json"
    if (-not (Test-Path -LiteralPath $manifestPath -PathType Leaf)) {
        throw "Release package is incomplete. Missing: $manifestPath"
    }
    foreach ($item in @(Get-ChildItem -LiteralPath $rootFull -Force -Recurse)) {
        if (($item.Attributes -band [System.IO.FileAttributes]::ReparsePoint) -ne 0) {
            throw "Release package contains a reparse point: $($item.FullName)"
        }
    }
    $manifest = Read-JsonFile $manifestPath "build manifest"
    if ($manifest.build_manifest_schema_version -ne 1 -or $manifest.payload_inventory -isnot [System.Array]) {
        throw "Build manifest does not use the supported payload inventory contract."
    }
    $expected = @{}
    foreach ($entry in @($manifest.payload_inventory)) {
        $entryKeys = @($entry.PSObject.Properties.Name | Sort-Object)
        if (($entryKeys -join ',') -cne 'path,sha256,size') {
            throw "Build manifest payload entry fields differ from the exact contract."
        }
        $relative = [string]$entry.path
        Assert-SafeManifestRelativePath $relative
        $folded = $relative.ToLowerInvariant()
        if ($expected.ContainsKey($folded)) { throw "Build manifest contains a case-colliding path: $relative" }
        if ([string]$entry.sha256 -cnotmatch '^[0-9a-f]{64}$' -or [long]$entry.size -lt 0) {
            throw "Build manifest payload metadata is invalid: $relative"
        }
        $expected[$folded] = $entry
        $target = Join-Path $rootFull $relative.Replace('/', '\')
        if (-not (Test-Path -LiteralPath $target -PathType Leaf)) { throw "Manifest-declared file is missing: $relative" }
        $targetFull = [System.IO.Path]::GetFullPath($target)
        if (-not (Test-PathInside $targetFull $rootFull)) { throw "Manifest-declared file escapes package root: $relative" }
        $item = Get-Item -LiteralPath $targetFull
        if ([long]$item.Length -ne [long]$entry.size) { throw "Manifest size mismatch: $relative" }
        $actualHash = (Get-FileHash -LiteralPath $targetFull -Algorithm SHA256).Hash.ToLowerInvariant()
        if ($actualHash -cne [string]$entry.sha256) { throw "Manifest hash mismatch: $relative" }
    }
    $actual = @(
        Get-ChildItem -LiteralPath $rootFull -Force -File -Recurse |
            ForEach-Object { Get-RelativeFilePath $rootFull $_.FullName } |
            Where-Object { $_ -cne 'build-manifest.json' }
    )
    if ($actual.Count -ne $expected.Count) { throw "Release package contains missing or unexpected payload files." }
    foreach ($relative in $actual) {
        if (-not $expected.ContainsKey($relative.ToLowerInvariant())) {
            throw "Release package contains an unexpected payload file: $relative"
        }
    }
    foreach ($required in $RequiredPackageMembers) {
        if (-not (Test-Path -LiteralPath (Join-Path $rootFull $required.Replace('/', '\')) -PathType Leaf)) {
            throw "Release package is incomplete. Missing required member: $required"
        }
    }
    return $manifest
}

function Copy-ManifestBoundPackage([string]$SourceRoot, [string]$CandidateRoot, $Manifest) {
    New-Item -ItemType Directory -Path $CandidateRoot -ErrorAction Stop | Out-Null
    [System.IO.File]::Copy((Join-Path $SourceRoot "build-manifest.json"), (Join-Path $CandidateRoot "build-manifest.json"), $false)
    foreach ($entry in @($Manifest.payload_inventory)) {
        $relativePlatform = ([string]$entry.path).Replace('/', '\')
        $source = Join-Path $SourceRoot $relativePlatform
        $target = Join-Path $CandidateRoot $relativePlatform
        $parent = Split-Path -Parent $target
        if (-not (Test-Path -LiteralPath $parent -PathType Container)) {
            New-Item -ItemType Directory -Path $parent -Force | Out-Null
        }
        [System.IO.File]::Copy($source, $target, $false)
    }
}

function Assert-OwnedInstalledTarget([string]$InstallRoot, [string]$SummaryPath, [string]$SourceManifestSha256) {
    if (-not (Test-Path -LiteralPath $SummaryPath -PathType Leaf)) {
        throw "Installed target ownership summary is missing: $SummaryPath"
    }
    $summary = Read-JsonFile $SummaryPath "install ownership summary"
    if (
        $summary.installer_report_version -cne "label-match-direct-sync-one-step-install-v2" -or
        $summary.status -cne "PASS" -or
        [string]$summary.source_manifest_sha256 -cne $SourceManifestSha256 -or
        -not (Test-SamePath ([string]$summary.resources.app_root.path) $InstallRoot)
    ) {
        throw "Installed target is not owned by the exact source manifest and install summary."
    }
    Assert-AppInventorySummaryContract $summary.resources.app_root
    if (Test-Path -LiteralPath $InstallRoot -PathType Container) {
        Assert-NoReparseTree $InstallRoot "installed app root"
        $identity = Get-ImmutableAppInventoryIdentity $InstallRoot $MutableAppRelativePaths
        if (
            [string]$summary.resources.app_root.immutable_inventory_sha256 -cne $identity.immutable_sha256 -or
            [int]$summary.resources.app_root.immutable_file_count -ne $identity.immutable_file_count
        ) {
            throw "Installed immutable app bytes drifted from the owned install summary."
        }
    }
    return $summary
}

$isDryRun = $DryRun.IsPresent
$isUninstall = $Uninstall.IsPresent
$isRollback = $Rollback.IsPresent
$allowTestLayout = $AllowNoncanonicalLayoutForTest.IsPresent
$testMode = $allowTestLayout -and [string]${env:KMTECH_FACTORY_INSTALL_TEST_MODE} -ceq "1"
if ($isUninstall -and $isRollback) { throw "-Uninstall and -Rollback are mutually exclusive." }
if ($isDryRun -and ($isUninstall -or $isRollback)) { throw "DryRun cannot be combined with removal modes." }

$installRootOverride = $InstallRootForTest
if (-not [string]::IsNullOrWhiteSpace($installRootOverride) -and -not $testMode) {
    throw "-InstallRootForTest requires -AllowNoncanonicalLayoutForTest and $TestModeEnvironmentName=1."
}
$installRoot = if ([string]::IsNullOrWhiteSpace($installRootOverride)) {
    $CanonicalInstallRoot
}
else {
    [System.IO.Path]::GetFullPath($installRootOverride)
}
$programDataRoot = if ([string]::IsNullOrWhiteSpace($ProgramDataRoot)) { $DefaultProgramDataRoot } else { $ProgramDataRoot }
$programDataRoot = [System.IO.Path]::GetFullPath($programDataRoot)
$scanSourceDir = $ScanSourceDir
$scanSourceDir = [System.IO.Path]::GetFullPath($scanSourceDir)
$commonProgramsRoot = if ([string]::IsNullOrWhiteSpace($CommonProgramsRootForTest)) { "C:\ProgramData\Microsoft\Windows\Start Menu\Programs" } else { $CommonProgramsRootForTest }
$receiptRoot = if ([string]::IsNullOrWhiteSpace($RollbackReceiptRootForTest)) { "C:\ProgramData\KMTech\InstallerReceipts\Label_Match" } else { $RollbackReceiptRootForTest }
if (-not [string]::IsNullOrWhiteSpace($CommonProgramsRootForTest) -or -not [string]::IsNullOrWhiteSpace($RollbackReceiptRootForTest)) {
    if (-not $testMode) { throw "Test root redirects require -AllowNoncanonicalLayoutForTest and KMTECH_FACTORY_INSTALL_TEST_MODE=1." }
}
$commonProgramsRoot = [System.IO.Path]::GetFullPath($commonProgramsRoot)
$receiptRoot = [System.IO.Path]::GetFullPath($receiptRoot)
$logisticsProfileFullPath = [System.IO.Path]::GetFullPath($LogisticsProfilePath)
$logisticsProfileRoot = Split-Path -Parent $logisticsProfileFullPath
$managedRoots = @($installRoot, $programDataRoot, $scanSourceDir, $logisticsProfileRoot)
for ($leftIndex = 0; $leftIndex -lt $managedRoots.Count; $leftIndex += 1) {
    for ($rightIndex = $leftIndex + 1; $rightIndex -lt $managedRoots.Count; $rightIndex += 1) {
        if (
            (Test-SamePath $managedRoots[$leftIndex] $managedRoots[$rightIndex]) -or
            (Test-PathInside $managedRoots[$leftIndex] $managedRoots[$rightIndex]) -or
            (Test-PathInside $managedRoots[$rightIndex] $managedRoots[$leftIndex])
        ) {
            throw "Managed install, DirectSync, Label data, and machine-profile roots must be disjoint."
        }
    }
}
$launcherDirectory = Join-Path $commonProgramsRoot "KMTech"
foreach ($auxiliaryRoot in @($launcherDirectory, $receiptRoot)) {
    foreach ($managedRoot in $managedRoots) {
        if (
            (Test-SamePath $auxiliaryRoot $managedRoot) -or
            (Test-PathInside $auxiliaryRoot $managedRoot) -or
            (Test-PathInside $managedRoot $auxiliaryRoot)
        ) {
            throw "Launcher and receipt roots must be disjoint from managed install/data roots."
        }
    }
}
foreach ($managedPathCheck in @(
    $installRoot,
    $programDataRoot,
    $scanSourceDir,
    $logisticsProfileFullPath,
    (Join-Path $commonProgramsRoot "KMTech\Label Match.lnk"),
    $receiptRoot
)) {
    Assert-NoReparsePath $managedPathCheck "managed installer path"
}
$evidenceArchiveRoot = $EvidenceArchiveRoot
if ($isRollback) {
    if ([string]::IsNullOrWhiteSpace($evidenceArchiveRoot) -or -not [System.IO.Path]::IsPathRooted($evidenceArchiveRoot)) {
        throw "-Rollback requires an absolute -EvidenceArchiveRoot outside managed paths."
    }
    $evidenceArchiveRoot = [System.IO.Path]::GetFullPath($evidenceArchiveRoot)
    foreach ($managed in @($managedRoots + @($launcherDirectory, $receiptRoot))) {
        if ((Test-SamePath $evidenceArchiveRoot $managed) -or (Test-PathInside $evidenceArchiveRoot $managed) -or (Test-PathInside $managed $evidenceArchiveRoot)) {
            throw "EvidenceArchiveRoot must be outside every managed install/data path."
        }
    }
    if (Test-Path -LiteralPath $evidenceArchiveRoot) {
        throw "EvidenceArchiveRoot must be a fresh absent path."
    }
    Assert-NoReparsePath $evidenceArchiveRoot "rollback evidence root"
}

# A test-only dry run exercises ordinary extraction and staging without UAC or task changes.
if (-not ($isDryRun -and $testMode)) {
    Invoke-SelfElevated $MyInvocation.MyCommand.Path $PSBoundParameters
}

$sourceRoot = [System.IO.Path]::GetFullPath((Split-Path -Parent $MyInvocation.MyCommand.Path))
Assert-NoReparsePath $sourceRoot "release package root"
$manifest = Assert-PackageRoot $sourceRoot
$sourceManifestSha256 = (Get-FileHash -LiteralPath (Join-Path $sourceRoot "build-manifest.json") -Algorithm SHA256).Hash.ToLowerInvariant()
$summaryPath = Join-Path $programDataRoot "status\label_match_one_step_install_summary.json"
$publicReportPath = Join-Path $receiptRoot "label_match_public_install_report.json"
$installPrestate = "absent"
$targetCreated = $false
$priorPublicReport = $null
$installParentCreatedPaths = @()
$receiptDirectoryCreatedPaths = @()
if (Test-Path -LiteralPath $publicReportPath -PathType Leaf) {
    $priorPublicReport = Read-JsonFile $publicReportPath "public install report"
    if (
        $priorPublicReport.report_version -cne "label-match-public-install-v2" -or
        [string]$priorPublicReport.source_manifest_sha256 -cne $sourceManifestSha256 -or
        $null -eq $priorPublicReport.staging -or
        $priorPublicReport.staging.PSObject.Properties.Name -notcontains "created_install_parent_paths" -or
        $priorPublicReport.staging.PSObject.Properties.Name -notcontains "created_receipt_directory_paths"
    ) {
        throw "Existing public install receipt is not owned by this exact source manifest."
    }
    $installParentCreatedPaths = @($priorPublicReport.staging.created_install_parent_paths)
    $receiptDirectoryCreatedPaths = @($priorPublicReport.staging.created_receipt_directory_paths)
}
elseif ($isUninstall -or $isRollback) {
    throw "Owned public install receipt is required for removal."
}
if (($isUninstall -or $isRollback) -and $priorPublicReport.status -cne "PASS") {
    throw "Removal requires an owned successful public install receipt."
}

if (-not ($isUninstall -or $isRollback)) {
    if (Test-Path -LiteralPath $installRoot) {
        if ($isDryRun) {
            throw "DryRun requires an absent isolated install target."
        }
        if (-not (Test-Path -LiteralPath $installRoot -PathType Container)) {
            throw "Canonical install target exists and is not a directory: $installRoot"
        }
        [void](Assert-OwnedInstalledTarget $installRoot $summaryPath $sourceManifestSha256)
        if ($null -eq $priorPublicReport -or $priorPublicReport.status -cne "PASS") {
            throw "Exact target reuse requires its owned successful public receipt."
        }
        $installPrestate = "exact_reused"
    }
    else {
        if ($null -ne $priorPublicReport -and $priorPublicReport.status -cne "FAILED") {
            throw "An owned successful public receipt exists while the installed target is absent."
        }
        $installParent = Split-Path -Parent $installRoot
        $installParentCreatedPaths = @($installParentCreatedPaths + @(Get-MissingDirectoryChain $installParent) | Select-Object -Unique)
        New-Item -ItemType Directory -Path $installParent -Force | Out-Null
        $candidate = Join-Path $installParent ((Split-Path -Leaf $installRoot) + ".candidate." + [guid]::NewGuid().ToString("N"))
        try {
            Copy-ManifestBoundPackage $sourceRoot $candidate $manifest
            [void](Assert-PackageRoot $candidate)
            $candidateManifestSha256 = (Get-FileHash -LiteralPath (Join-Path $candidate "build-manifest.json") -Algorithm SHA256).Hash.ToLowerInvariant()
            if ($candidateManifestSha256 -cne $sourceManifestSha256) {
                throw "Candidate manifest changed after source validation; refusing staging rename."
            }
            Move-Item -LiteralPath $candidate -Destination $installRoot -ErrorAction Stop
            $targetCreated = $true
        }
        finally {
            if (Test-Path -LiteralPath $candidate) {
                Remove-Item -LiteralPath $candidate -Recurse -Force -ErrorAction SilentlyContinue
            }
        }
    }
}
else {
    $ownedSummary = Assert-OwnedInstalledTarget $installRoot $summaryPath $sourceManifestSha256
    if ($isRollback -and [string]$ownedSummary.resources.app_root.install_prestate -cne "absent") {
        throw "Exact rollback is limited to a fresh target whose recorded prestate was absent."
    }
    if ($isRollback) {
        $recordedCreatedDirectoryPaths = @(
            @($priorPublicReport.staging.created_install_parent_paths) +
            @($priorPublicReport.staging.created_receipt_directory_paths) +
            @($ownedSummary.resources.created_directory_paths.data_root) +
            @($ownedSummary.resources.created_directory_paths.direct_sync_root) +
            @($ownedSummary.resources.created_directory_paths.machine_profile_root) +
            @($ownedSummary.resources.created_directory_paths.launcher_parent)
        )
        foreach ($recordedCreatedDirectoryPath in $recordedCreatedDirectoryPaths) {
            if ([string]::IsNullOrWhiteSpace([string]$recordedCreatedDirectoryPath)) {
                throw "Owned install receipt contains an empty created-directory ancestry path."
            }
            $recordedFullPath = [System.IO.Path]::GetFullPath([string]$recordedCreatedDirectoryPath)
            if (
                (Test-SamePath $evidenceArchiveRoot $recordedFullPath) -or
                (Test-PathInside $evidenceArchiveRoot $recordedFullPath) -or
                (Test-PathInside $recordedFullPath $evidenceArchiveRoot)
            ) {
                throw "EvidenceArchiveRoot overlaps an installer-created directory ancestry path."
            }
        }
    }
}

$receiptDirectoryCreatedPaths = @($receiptDirectoryCreatedPaths + @(Get-MissingDirectoryChain $receiptRoot) | Select-Object -Unique)

$publicReport = [ordered]@{
    report_version = "label-match-public-install-v2"
    status = "STAGED"
    source_manifest_sha256 = $sourceManifestSha256
    source_root = $sourceRoot
    install_root = $installRoot
    staging = [ordered]@{
        ordinary_extracted_root_supported = $true
        install_prestate = $installPrestate
        disposition = if ($targetCreated) { "created" } else { "exact_reused" }
        manifest_validated = $true
        manifest_hashes_and_sizes_verified = $true
        safe_relative_paths_verified = $true
        unexpected_payload_files_absent = $true
        same_volume_candidate_verified = $targetCreated
        candidate_byte_parity_verified = $targetCreated
        atomic_rename_used = $targetCreated
        exact_existing_target_summary_verified = -not $targetCreated
        nested_label_match_directory_absent = -not (Test-Path -LiteralPath (Join-Path $installRoot "Label_Match") -PathType Container)
        manifest_payload_file_count = @($manifest.payload_inventory).Count
        created_install_parent_paths = @($installParentCreatedPaths)
        created_receipt_directory_paths = @($receiptDirectoryCreatedPaths)
    }
    launcher_contract = [ordered]@{
        count = 1
        scope = "all_users"
        path = Join-Path $commonProgramsRoot "KMTech\Label Match.lnk"
        target = Join-Path $installRoot "Label_Match.exe"
        working_directory = $installRoot
        icon = Join-Path $installRoot "Label_Match.exe"
    }
    removal_contract = [ordered]@{
        uninstall = "DATA_PRESERVING_UNINSTALL"
        rollback = "EXACT_FRESH_TARGET_ROLLBACK"
        task_operations = @("stop", "delete", "absence")
        task_results_are_typed = $true
        rollback_evidence_external = $true
        evidence_maximum_files = 10000
        evidence_maximum_bytes = 2147483648
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
if (-not ($isUninstall -or $isRollback)) {
    if (Test-Path -LiteralPath $publicReportPath -PathType Leaf) {
        $priorPublicReport = Read-JsonFile $publicReportPath "public install report"
        if (
            $priorPublicReport.report_version -cne "label-match-public-install-v2" -or
            [string]$priorPublicReport.source_manifest_sha256 -cne $sourceManifestSha256 -or
            ($installPrestate -cne "exact_reused" -and $priorPublicReport.status -cne "FAILED")
        ) {
            throw "Refusing to overwrite an unowned public install receipt."
        }
    }
    Write-Utf8JsonFile $publicReportPath $publicReport
}

$installerRoot = if ($isUninstall -or $isRollback) { $sourceRoot } else { $installRoot }
$installer = Join-Path $installerRoot "install_label_match_direct_sync.ps1"
if (-not (Test-Path -LiteralPath $installer -PathType Leaf)) {
    throw "Release package is incomplete. Missing: $installer"
}

$nestedParameters = @{}
foreach ($entry in $PSBoundParameters.GetEnumerator()) {
    if ($entry.Key -in @("InstallRootForTest", "RollbackReceiptRootForTest")) {
        continue
    }
    $nestedParameters[$entry.Key] = $entry.Value
}
$nestedParameters["ManagedInstallRoot"] = $installRoot
$nestedParameters["SourceManifestSha256"] = $sourceManifestSha256
$nestedParameters["InstallPrestate"] = $installPrestate
$nestedExitCode = $null
$nestedParameters["PublicWrapperExitCode"] = [ref]$nestedExitCode

# Preserve the established tokenless self-enrollment path after manifest-bound staging.
try {
    & $installer @nestedParameters
    if ($null -eq $nestedExitCode) {
        throw "Nested installer did not return its typed exit code to the public wrapper."
    }
    $exitCode = [int]$nestedExitCode
}
catch {
    $exitCode = 1
    if (-not ($isUninstall -or $isRollback)) {
        $publicReport.status = "FAILED"
        $publicReport.nested_exit_code = $exitCode
        $publicReport["failure"] = $_.Exception.Message
        Write-Utf8JsonFile $publicReportPath $publicReport
    }
}

if ($exitCode -ne 0) {
    if (-not ($isUninstall -or $isRollback)) {
        $publicReport.status = "FAILED"
        $publicReport.nested_exit_code = $exitCode
        Write-Utf8JsonFile $publicReportPath $publicReport
    }
    if ($targetCreated -and (Test-Path -LiteralPath $installRoot -PathType Container)) {
        Remove-Item -LiteralPath $installRoot -Recurse -Force -ErrorAction SilentlyContinue
    }
    exit $exitCode
}

$nestedRemovalResourceReport = $null
$nestedRemovalResourceReportPath = ""
$nestedRemovalResourceReportSha256 = ""
$preFinalEvidenceVerification = $null
if ($isUninstall -or $isRollback) {
    $nestedRemovalResourceReportPath = if ($isRollback) {
        Join-Path $evidenceArchiveRoot "label_match_rollback_resources.json"
    }
    else {
        Join-Path $programDataRoot "status\label_match_uninstall_report.json"
    }
    if (-not (Test-Path -LiteralPath $nestedRemovalResourceReportPath -PathType Leaf)) {
        throw "Nested removal resource report is missing."
    }
    $nestedRemovalResourceReport = Read-JsonFile $nestedRemovalResourceReportPath "nested removal resource report"
    $expectedRemovalReportVersion = if ($isRollback) { "label-match-exact-rollback-resources-v1" } else { "label-match-data-preserving-uninstall-v1" }
    $expectedRemovalMode = if ($isRollback) { "EXACT_FRESH_TARGET_ROLLBACK" } else { "DATA_PRESERVING_UNINSTALL" }
    if (
        $nestedRemovalResourceReport.report_version -cne $expectedRemovalReportVersion -or
        $nestedRemovalResourceReport.status -cne "PASS" -or
        $nestedRemovalResourceReport.mode -cne $expectedRemovalMode -or
        [string]$nestedRemovalResourceReport.source_manifest_sha256 -cne $sourceManifestSha256 -or
        $nestedRemovalResourceReport.task.stop.status -cne "PASS" -or
        $nestedRemovalResourceReport.task.delete_and_absence.status -cne "PASS" -or
        $nestedRemovalResourceReport.task.delete_and_absence.absence_proven -ne $true -or
        -not (Test-SamePath ([string]$nestedRemovalResourceReport.resources.app_root.path) $installRoot) -or
        $nestedRemovalResourceReport.resources.app_root.disposition -cne "pending_public_wrapper_removal" -or
        $nestedRemovalResourceReport.resources.app_root.absence_proven -ne $false -or
        $nestedRemovalResourceReport.pre_install_parity_claimed -ne $false
    ) {
        throw "Nested removal resource report is stale, mismatched, or incomplete."
    }
    if (
        $isRollback -and
        (
            $nestedRemovalResourceReport.evidence.status -cne "PASS" -or
            $nestedRemovalResourceReport.evidence.byte_parity_verified -ne $true -or
            $nestedRemovalResourceReport.resources.launcher.absence_proven -ne $true -or
            $nestedRemovalResourceReport.resources.machine_profile_root.absence_proven -ne $true -or
            $nestedRemovalResourceReport.resources.data_root.absence_proven -ne $true -or
            $nestedRemovalResourceReport.resources.direct_sync_business_evidence.absence_proven -ne $true
        )
    ) {
        throw "Nested rollback report does not prove bounded external evidence parity."
    }
    if ($isRollback) {
        $preFinalEvidenceVerification = Confirm-BoundedRollbackEvidence `
            $evidenceArchiveRoot $nestedRemovalResourceReport.evidence $nestedRemovalResourceReportPath
    }
    if (
        $isUninstall -and
        (
            $nestedRemovalResourceReport.resources.data_root.disposition -cne "preserved" -or
            $nestedRemovalResourceReport.resources.direct_sync_business_evidence.disposition -cne "preserved"
        )
    ) {
        throw "Nested uninstall report does not preserve business evidence."
    }
    $nestedRemovalResourceReportSha256 = (Get-FileHash -LiteralPath $nestedRemovalResourceReportPath -Algorithm SHA256).Hash.ToLowerInvariant()
}

if ($isUninstall -or $isRollback) {
    if (Test-Path -LiteralPath $installRoot -PathType Container) {
        Assert-NoReparseTree $installRoot "installed app root"
        Assert-AppInventorySummaryContract $ownedSummary.resources.app_root
        $currentIdentity = Get-ImmutableAppInventoryIdentity $installRoot $MutableAppRelativePaths
        if (
            [string]$ownedSummary.resources.app_root.immutable_inventory_sha256 -cne $currentIdentity.immutable_sha256 -or
            [int]$ownedSummary.resources.app_root.immutable_file_count -ne $currentIdentity.immutable_file_count
        ) {
            throw "Installed immutable app content changed during removal; refusing recursive deletion."
        }
        Remove-Item -LiteralPath $installRoot -Recurse -Force -ErrorAction Stop
    }
    if (Test-Path -LiteralPath $installRoot) { throw "Installed app root remains after removal." }

    if ($isRollback) {
        $ownedPublicReport = $priorPublicReport
        if ($null -eq $ownedPublicReport -or -not (Test-Path -LiteralPath $publicReportPath -PathType Leaf)) {
            throw "Public install receipt ownership is not proven."
        }
        Remove-Item -LiteralPath $publicReportPath -Force -ErrorAction Stop
        $publicCreatedDirectoryPaths = @(
            @($ownedPublicReport.staging.created_install_parent_paths) +
            @($ownedPublicReport.staging.created_receipt_directory_paths) +
            @($ownedSummary.resources.created_directory_paths.data_root) +
            @($ownedSummary.resources.created_directory_paths.direct_sync_root) +
            @($ownedSummary.resources.created_directory_paths.machine_profile_root) +
            @($ownedSummary.resources.created_directory_paths.launcher_parent)
        )
        [void](Remove-OwnedEmptyDirectoryPaths $publicCreatedDirectoryPaths @(
            (Split-Path -Parent $installRoot),
            $receiptRoot,
            $scanSourceDir,
            $programDataRoot,
            $logisticsProfileRoot,
            $launcherDirectory
        ))
        $removedInstallParentPaths = @($ownedPublicReport.staging.created_install_parent_paths)
        $removedReceiptDirectoryPaths = @($ownedPublicReport.staging.created_receipt_directory_paths)
        if (-not (Test-Path -LiteralPath $evidenceArchiveRoot -PathType Container)) {
            throw "Rollback evidence root was not created by the nested rollback phase."
        }
        Assert-NoReparsePath $evidenceArchiveRoot "rollback evidence root"
        $rollbackResourceReportPath = $nestedRemovalResourceReportPath
        if ((Get-FileHash -LiteralPath $rollbackResourceReportPath -Algorithm SHA256).Hash.ToLowerInvariant() -cne $nestedRemovalResourceReportSha256) {
            throw "Rollback resource report changed after nested verification."
        }
        $rollbackResourceReport = $nestedRemovalResourceReport
        $rollbackResourceReport.resources.app_root.disposition = "removed"
        $rollbackResourceReport.resources.app_root.absence_proven = -not (Test-Path -LiteralPath $installRoot)
        $rollbackResourceReport.resources | Add-Member -NotePropertyName public_receipt_directories -NotePropertyValue ([pscustomobject][ordered]@{
            recorded_created_paths = @($ownedPublicReport.staging.created_receipt_directory_paths)
            removed_paths = $removedReceiptDirectoryPaths
        }) -Force
        $rollbackResourceReport.resources | Add-Member -NotePropertyName app_parent_directories -NotePropertyValue ([pscustomobject][ordered]@{
            recorded_created_paths = @($ownedPublicReport.staging.created_install_parent_paths)
            removed_paths = $removedInstallParentPaths
        }) -Force
        $rollbackResourceReport.resources.launcher_parent.removed_paths = @($ownedSummary.resources.created_directory_paths.launcher_parent)
        $rollbackResourceReport.resources.launcher_parent.disposition = "restored_to_recorded_prestate"
        $rollbackResourceReport.resources.data_root.parent_paths_removed = @($ownedSummary.resources.created_directory_paths.data_root)
        $rollbackResourceReport.resources.direct_sync_business_evidence.parent_paths_removed = @($ownedSummary.resources.created_directory_paths.direct_sync_root)
        $rollbackResourceReport.resources.machine_profile_root.parent_paths_removed = @($ownedSummary.resources.created_directory_paths.machine_profile_root)
        $rollbackResourceReport.evidence | Add-Member -NotePropertyName inventory_sha256 -NotePropertyValue $preFinalEvidenceVerification.inventory_sha256 -Force
        $rollbackResourceReport.evidence | Add-Member -NotePropertyName archived_bytes_reverified_by_public_wrapper -NotePropertyValue $true -Force
        $rollbackResourceReport.pre_install_parity_claimed = $true
        Write-Utf8JsonFile $rollbackResourceReportPath $rollbackResourceReport
        $finalEvidenceVerification = Confirm-BoundedRollbackEvidence `
            $evidenceArchiveRoot $rollbackResourceReport.evidence $rollbackResourceReportPath
        if ($finalEvidenceVerification.inventory_sha256 -cne $preFinalEvidenceVerification.inventory_sha256) {
            throw "Rollback evidence inventory changed during public finalization."
        }
        $finalResourceReportSha256 = (Get-FileHash -LiteralPath $rollbackResourceReportPath -Algorithm SHA256).Hash.ToLowerInvariant()
        $receiptPath = Join-Path $evidenceArchiveRoot "label_match_rollback_receipt.json"
        Write-Utf8JsonFile $receiptPath ([ordered]@{
            receipt_version = "label-match-exact-rollback-v1"
            status = "ROLLED_BACK"
            exact_fresh_target_parity = $true
            source_manifest_sha256 = $sourceManifestSha256
            app_root = [ordered]@{ path = $installRoot; disposition = "removed"; absence_proven = -not (Test-Path -LiteralPath $installRoot) }
            resource_report = $rollbackResourceReportPath
            resource_report_sha256 = $finalResourceReportSha256
            evidence_inventory = $finalEvidenceVerification.inventory_path
            evidence_inventory_sha256 = $finalEvidenceVerification.inventory_sha256
            archived_file_count = $finalEvidenceVerification.file_count
            archived_total_bytes = $finalEvidenceVerification.total_bytes
            archived_bytes_reverified = $true
            created_parent_paths_removed = $true
        })
        Write-Output "ROLLED_BACK evidence=$receiptPath"
    }
    else {
        $uninstallReportPath = $nestedRemovalResourceReportPath
        if ((Get-FileHash -LiteralPath $uninstallReportPath -Algorithm SHA256).Hash.ToLowerInvariant() -cne $nestedRemovalResourceReportSha256) {
            throw "Uninstall resource report changed after nested verification."
        }
        $uninstallReport = $nestedRemovalResourceReport
        $uninstallReport.resources.app_root.disposition = "removed"
        $uninstallReport.resources.app_root.absence_proven = -not (Test-Path -LiteralPath $installRoot)
        $uninstallReport.resources | Add-Member -NotePropertyName public_install_receipt -NotePropertyValue ([pscustomobject][ordered]@{
            path = $publicReportPath
            disposition = "preserved_uninstall_receipt"
        }) -Force
        Write-Utf8JsonFile $uninstallReportPath $uninstallReport
        Write-Output "UNINSTALLED_DATA_PRESERVED report=$uninstallReportPath"
    }
}
else {
    $publicReport.status = if ($isDryRun) { "DRY_RUN_STAGED" } else { "PASS" }
    $publicReport.nested_exit_code = 0
    $publicReport.install_summary = $summaryPath
    Write-Utf8JsonFile $publicReportPath $publicReport
    Write-Output "INSTALLED manifest=$sourceManifestSha256"
}
exit 0
