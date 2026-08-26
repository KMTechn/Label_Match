<#
.SYNOPSIS
Installs, removes, or rolls back the Label_Match release package.

.DESCRIPTION
The production server and machine-derived source identity remain the defaults.
For isolated non-production qualification, provide both ServerBaseUrl and
SourceHostId during installation, then set the matching process-scoped launch
environment variables before every launch or restart. Do not set these values
machine-wide.

.PARAMETER ServerBaseUrl
HTTPS base URL used for Direct Sync enrollment and upload.

.PARAMETER SourceHostId
Optional source identity override. Leave empty for the machine-derived default.

.EXAMPLE
$nonProductionServerBaseUrl = "<NON_PRODUCTION_SERVER_BASE_URL>"
$nonProductionSourceHostId = "<NON_PRODUCTION_SOURCE_HOST_ID>"
& .\INSTALL_THIS_PC.ps1 `
    -ServerBaseUrl $nonProductionServerBaseUrl `
    -SourceHostId $nonProductionSourceHostId

$env:LABEL_MATCH_DIRECT_SYNC_SERVER_BASE_URL = $nonProductionServerBaseUrl
$env:LABEL_MATCH_DIRECT_SYNC_SOURCE_HOST_ID = $nonProductionSourceHostId
& "C:\KMTech\Apps\Label_Match\current\Label_Match.exe"
#>
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
    [switch]$AllowInteractiveTaskForLocalTest
)

$ErrorActionPreference = "Stop"

$AppId = "Label_Match"
$AppExecutableName = "Label_Match.exe"
$OwnedScheduledTaskName = "direct-sync-relay-label-match"
$AllUsersShortcutName = "Label Match.lnk"
$CanonicalInstallRoot = "C:\KMTech\Apps\$AppId\current"
$DefaultProgramDataRoot = "C:\ProgramData\KMTech\DirectSync\label_match"
$TestModeEnvironmentName = "KMTECH_FACTORY_INSTALL_TEST_MODE"
$RequiredPackageMembers = @(
    "INSTALL_THIS_PC.ps1",
    "install_label_match_direct_sync.ps1",
    "Label_Match.exe",
    "build-manifest.json",
    "_internal/python312.dll",
    "_internal/base_library.zip",
    "tools/invoke_embedded_python.ps1",
    "tools/direct_sync_relay_install_pack.py",
    "tools/direct_sync_relay_runner.py",
    "tools/direct_sync_relay_runner.exe",
    "tools/register_label_match_worker_pc.py"
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

$AllowedInstallerFailureCodes = @(
    "CHILD_EXCEPTION",
    "CHILD_IMPORT_FAILED",
    "CHILD_NONZERO_EXIT",
    "CHILD_PROCESS_START_FAILED",
    "CHILD_PROCESS_TIMEOUT",
    "NESTED_INSTALLER_EXCEPTION",
    "NESTED_INSTALLER_NONZERO_EXIT",
    "PUBLIC_POSTCONDITION_FAILED",
    "SCHEDULED_TASK_START_FAILED"
)

function ConvertTo-BoundedDiagnosticText {
    param(
        [AllowNull()][object]$Value,
        [int]$MaximumLength = 512
    )
    $text = [string]$Value
    $text = [regex]::Replace($text, '[\x00-\x1F\x7F]+', ' ')
    $text = [regex]::Replace(
        $text,
        '(?i)\b([A-Za-z0-9_-]*(?:token|password|secret|authorization|cookie|api[_-]?key)[A-Za-z0-9_-]*)\b(\s*[:=]\s*)([^\s,;]+)',
        '$1$2[redacted]'
    )
    $text = [regex]::Replace($text, '(?i)\bbearer\s+[^\s,;]+', 'Bearer [redacted]')
    $text = [regex]::Replace($text.Trim(), '\s+', ' ')
    if ($text.Length -gt $MaximumLength) {
        return $text.Substring(0, $MaximumLength)
    }
    return $text
}

function Test-DiagnosticProperty([AllowNull()][object]$Value, [string]$Name) {
    if ($null -eq $Value) { return $false }
    if ($Value -is [System.Collections.IDictionary]) { return $Value.Contains($Name) }
    return $null -ne $Value.PSObject.Properties[$Name]
}

function Get-DiagnosticProperty([AllowNull()][object]$Value, [string]$Name) {
    if (-not (Test-DiagnosticProperty $Value $Name)) { return $null }
    if ($Value -is [System.Collections.IDictionary]) { return $Value[$Name] }
    return $Value.PSObject.Properties[$Name].Value
}

function New-InstallerFailureDiagnostic {
    param(
        [string]$CommandIdentity,
        [AllowNull()][object]$ChildExitCode,
        [string]$FailureCode,
        [AllowNull()][System.Exception]$Exception = $null
    )
    $identity = ConvertTo-BoundedDiagnosticText $CommandIdentity 96
    if ($identity -cnotmatch '^[A-Za-z0-9][A-Za-z0-9._:-]{0,95}$') {
        $identity = "child_process"
    }
    $code = if ($FailureCode -cin $AllowedInstallerFailureCodes) {
        $FailureCode
    }
    else {
        "CHILD_EXCEPTION"
    }
    $typedExitCode = $null
    if ($null -ne $ChildExitCode) {
        $parsedExitCode = 0
        if ([int]::TryParse([string]$ChildExitCode, [ref]$parsedExitCode)) {
            $typedExitCode = $parsedExitCode
        }
    }
    $diagnostic = [ordered]@{
        diagnostic_version = "label-match-child-failure-v1"
        command_identity = $identity
        child_exit_code = $typedExitCode
        failure_code = $code
    }
    if ($null -ne $Exception) {
        $inner = $Exception
        $innerDepth = 0
        while (
            $innerDepth -lt 8 -and
            $null -ne $inner.InnerException -and
            -not [object]::ReferenceEquals($inner, $inner.InnerException)
        ) {
            $inner = $inner.InnerException
            $innerDepth += 1
        }
        $diagnostic["inner_exception_type"] = ConvertTo-BoundedDiagnosticText ($inner.GetType().Name) 128
        $message = ConvertTo-BoundedDiagnosticText $inner.Message 512
        if (-not [string]::IsNullOrWhiteSpace($message)) {
            $diagnostic["inner_exception_message"] = $message
        }
    }
    return $diagnostic
}

function ConvertTo-InstallerFailureDiagnostic {
    param(
        [AllowNull()][object]$Candidate,
        [string]$FallbackCommandIdentity,
        [AllowNull()][object]$FallbackChildExitCode,
        [string]$FallbackFailureCode
    )
    $commandIdentity = $FallbackCommandIdentity
    if (Test-DiagnosticProperty $Candidate "command_identity") {
        $candidateIdentity = ConvertTo-BoundedDiagnosticText (Get-DiagnosticProperty $Candidate "command_identity") 96
        if ($candidateIdentity -cmatch '^[A-Za-z0-9][A-Za-z0-9._:-]{0,95}$') {
            $commandIdentity = $candidateIdentity
        }
    }
    $childExitCode = $FallbackChildExitCode
    if (Test-DiagnosticProperty $Candidate "child_exit_code") {
        $childExitCode = Get-DiagnosticProperty $Candidate "child_exit_code"
    }
    $failureCode = $FallbackFailureCode
    if (Test-DiagnosticProperty $Candidate "failure_code") {
        $candidateFailureCode = [string](Get-DiagnosticProperty $Candidate "failure_code")
        if ($candidateFailureCode -cin $AllowedInstallerFailureCodes) {
            $failureCode = $candidateFailureCode
        }
    }
    $diagnostic = New-InstallerFailureDiagnostic `
        $commandIdentity $childExitCode $failureCode
    if (Test-DiagnosticProperty $Candidate "inner_exception_type") {
        $exceptionType = ConvertTo-BoundedDiagnosticText (Get-DiagnosticProperty $Candidate "inner_exception_type") 128
        if ($exceptionType -cmatch '^[A-Za-z_][A-Za-z0-9_.]{0,127}$') {
            $diagnostic["inner_exception_type"] = $exceptionType
        }
    }
    if (Test-DiagnosticProperty $Candidate "inner_exception_message") {
        $exceptionMessage = ConvertTo-BoundedDiagnosticText (Get-DiagnosticProperty $Candidate "inner_exception_message") 512
        if (-not [string]::IsNullOrWhiteSpace($exceptionMessage)) {
            $diagnostic["inner_exception_message"] = $exceptionMessage
        }
    }
    return $diagnostic
}

function Confirm-BoundedRollbackEvidence([string]$EvidenceRoot, $EvidenceRecord, [string]$ResourceReportPath) {
    $rootFull = [System.IO.Path]::GetFullPath($EvidenceRoot)
    $inventoryPath = Join-Path $rootFull "evidence-inventory.json"
    $inventoryExists = Test-Path -LiteralPath $inventoryPath -PathType Leaf
    $inventoryPathMatches = Test-SamePath ([string]$EvidenceRecord.inventory_path) $inventoryPath
    $resourceReportPathMatches = Test-SamePath $ResourceReportPath (Join-Path $rootFull "label_match_rollback_resources.json")
    if (
        -not $inventoryExists -or
        -not $inventoryPathMatches -or
        -not $resourceReportPathMatches
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
        $targetInsideRoot = Test-PathInside $target $rootFull
        $targetFileExists = Test-Path -LiteralPath $target -PathType Leaf
        if (-not $targetInsideRoot -or -not $targetFileExists) {
            throw "Rollback evidence inventory file is missing or escaped its root."
        }
        if ([long](Get-Item -LiteralPath $target).Length -ne [long]$entry.size) {
            throw "Rollback evidence file size changed after preservation."
        }
        if ((Get-FileSha256 $target) -cne [string]$entry.sha256) {
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
        inventory_sha256 = Get-FileSha256 $inventoryPath
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
        $actualHash = Get-FileSha256 $targetFull
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

function Assert-OwnedInstalledTarget(
    [string]$InstallRoot,
    [string]$SummaryPath,
    [string]$SourceManifestSha256,
    [string]$ExpectedStatus = "PASS"
) {
    if (-not (Test-Path -LiteralPath $InstallRoot -PathType Container)) {
        throw "Installed target is missing: $InstallRoot"
    }
    if (-not (Test-Path -LiteralPath $SummaryPath -PathType Leaf)) {
        throw "Installed target ownership summary is missing: $SummaryPath"
    }
    $summary = Read-JsonFile $SummaryPath "install ownership summary"
    if (
        $summary.installer_report_version -cne "label-match-direct-sync-one-step-install-v2" -or
        $summary.status -cne $ExpectedStatus -or
        [string]$summary.source_manifest_sha256 -cne $SourceManifestSha256 -or
        -not (Test-SamePath ([string]$summary.resources.app_root.path) $InstallRoot)
    ) {
        throw "Installed target is not owned by the exact source manifest and install summary."
    }
    Assert-AppInventorySummaryContract $summary.resources.app_root
    Assert-NoReparseTree $InstallRoot "installed app root"
    $identity = Get-ImmutableAppInventoryIdentity $InstallRoot $MutableAppRelativePaths
    if (
        [string]$summary.resources.app_root.immutable_inventory_sha256 -cne $identity.immutable_sha256 -or
        [int]$summary.resources.app_root.immutable_file_count -ne $identity.immutable_file_count
    ) {
        throw "Installed immutable app bytes drifted from the owned install summary."
    }
    return $summary
}

function Assert-ManifestBoundInstalledExecutable([string]$InstallRoot, $Manifest) {
    $executableEntries = @(
        @($Manifest.payload_inventory) |
            Where-Object { [string]$_.path -ceq "Label_Match.exe" }
    )
    if ($executableEntries.Count -ne 1) {
        throw "Build manifest must bind exactly one canonical Label_Match.exe."
    }
    $entry = $executableEntries[0]
    $executablePath = Join-Path $InstallRoot "Label_Match.exe"
    if (-not (Test-Path -LiteralPath $executablePath -PathType Leaf)) {
        throw "Canonical installed executable is missing: $executablePath"
    }
    Assert-NoReparsePath $executablePath "canonical installed executable"
    $item = Get-Item -LiteralPath $executablePath -Force -ErrorAction Stop
    $actualSha256 = Get-FileSha256 $executablePath
    if ([long]$item.Length -ne [long]$entry.size -or $actualSha256 -cne [string]$entry.sha256) {
        throw "Canonical installed executable does not match the source manifest."
    }
    return [ordered]@{
        path = [System.IO.Path]::GetFullPath($executablePath)
        size = [long]$item.Length
        sha256 = $actualSha256
        source_manifest_path = "Label_Match.exe"
    }
}

function Get-OwnedAppProcesses([string]$InstallRoot, [string]$ExecutableName) {
    $expectedExecutablePath = [System.IO.Path]::GetFullPath((Join-Path $InstallRoot $ExecutableName))
    $escapedExecutableName = $ExecutableName.Replace("'", "''")
    return @(
        Get-CimInstance -ClassName Win32_Process -Filter ("Name = '{0}'" -f $escapedExecutableName) -ErrorAction Stop |
            Where-Object {
                -not [string]::IsNullOrWhiteSpace([string]$_.ExecutablePath) -and
                (Test-SamePath ([string]$_.ExecutablePath) $expectedExecutablePath)
            }
    )
}

function Get-OwnedScheduledTasks([string]$ScheduledTaskName) {
    return @(
        Get-ScheduledTask -ErrorAction Stop |
            Where-Object {
                [string]$_.TaskPath -ceq '\' -and
                [string]$_.TaskName -ceq $ScheduledTaskName
            }
    )
}

function Invoke-CommonUninstall(
    [string]$InstallRoot,
    [string]$ExecutableName,
    [string]$ScheduledTaskName,
    [string]$AllUsersShortcutPath,
    [string[]]$PreservedDataRoots
) {
    $preservedPathStates = @{}
    foreach ($preservedDataRoot in @($PreservedDataRoots | Select-Object -Unique)) {
        if ([string]::IsNullOrWhiteSpace([string]$preservedDataRoot)) { continue }
        $preservedFullPath = [System.IO.Path]::GetFullPath([string]$preservedDataRoot)
        if (
            (Test-SamePath $InstallRoot $preservedFullPath) -or
            (Test-PathInside $InstallRoot $preservedFullPath) -or
            (Test-PathInside $preservedFullPath $InstallRoot)
        ) {
            throw "Uninstall payload root overlaps a preserved data root: $preservedFullPath"
        }
        $preservedPathStates[$preservedFullPath] = Test-Path -LiteralPath $preservedFullPath
    }

    $ownedTasks = @(Get-OwnedScheduledTasks $ScheduledTaskName)
    foreach ($ownedTask in $ownedTasks) {
        if ([string]$ownedTask.State -in @("Running", "Queued")) {
            Stop-ScheduledTask -InputObject $ownedTask -ErrorAction Stop
        }
        Unregister-ScheduledTask -InputObject $ownedTask -Confirm:$false -ErrorAction Stop
    }

    $ownedProcesses = @(Get-OwnedAppProcesses $InstallRoot $ExecutableName)
    foreach ($ownedProcess in $ownedProcesses) {
        Stop-Process -Id ([int]$ownedProcess.ProcessId) -Force -ErrorAction SilentlyContinue
    }
    foreach ($ownedProcess in $ownedProcesses) {
        Wait-Process -Id ([int]$ownedProcess.ProcessId) -Timeout 10 -ErrorAction SilentlyContinue
    }

    if (Test-Path -LiteralPath $AllUsersShortcutPath) {
        Remove-Item -LiteralPath $AllUsersShortcutPath -Force -ErrorAction Stop
    }
    if (Test-Path -LiteralPath $InstallRoot) {
        if (-not (Test-Path -LiteralPath $InstallRoot -PathType Container)) {
            throw "Installed app payload root is not a directory: $InstallRoot"
        }
        Assert-NoReparseTree $InstallRoot "installed app payload root"
        Remove-Item -LiteralPath $InstallRoot -Recurse -Force -ErrorAction Stop
    }

    if (@(Get-OwnedAppProcesses $InstallRoot $ExecutableName).Count -ne 0) {
        throw "Owned app process remains after uninstall."
    }
    if (@(Get-OwnedScheduledTasks $ScheduledTaskName).Count -ne 0) {
        throw "Owned scheduled task remains after uninstall."
    }
    if (Test-Path -LiteralPath $AllUsersShortcutPath) {
        throw "Owned all-users shortcut remains after uninstall."
    }
    if (Test-Path -LiteralPath $InstallRoot) {
        throw "Replaceable app payload remains after uninstall."
    }
    foreach ($preservedFullPath in $preservedPathStates.Keys) {
        if ($preservedPathStates[$preservedFullPath] -and -not (Test-Path -LiteralPath $preservedFullPath)) {
            throw "Preserved data root was removed during uninstall: $preservedFullPath"
        }
    }

    Write-Output "uninstall_status=PASS_DATA_PRESERVED"
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
if ($isUninstall) {
    Invoke-SelfElevated $MyInvocation.MyCommand.Path $PSBoundParameters
    $preservedDataRoots = @($programDataRoot, $scanSourceDir, $logisticsProfileRoot, $receiptRoot)
    if (-not [string]::IsNullOrWhiteSpace([string]$env:LOCALAPPDATA)) {
        $preservedDataRoots += [System.IO.Path]::GetFullPath([string]$env:LOCALAPPDATA)
    }
    $allUsersShortcutPath = Join-Path $commonProgramsRoot ("KMTech\" + $AllUsersShortcutName)
    Invoke-CommonUninstall `
        $installRoot $AppExecutableName $OwnedScheduledTaskName $allUsersShortcutPath $preservedDataRoots
    exit 0
}
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
$sourceManifestSha256 = Get-FileSha256 (Join-Path $sourceRoot "build-manifest.json")
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
            $candidateManifestSha256 = Get-FileSha256 (Join-Path $candidate "build-manifest.json")
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
$nestedFailureDiagnostic = $null
$verifiedInstallSummary = $null
$verifiedInstalledExecutable = $null

# Preserve the established tokenless self-enrollment path after manifest-bound staging.
try {
    # A [ref] inside a splatted hashtable is dereferenced by PowerShell before
    # parameter binding. Pass both result references explicitly so the nested
    # script returns its exit and bounded diagnostic to this public wrapper.
    & $installer @nestedParameters `
        -PublicWrapperExitCode ([ref]$nestedExitCode) `
        -PublicWrapperFailureDiagnostic ([ref]$nestedFailureDiagnostic)
    if ($null -eq $nestedExitCode) {
        throw "Nested installer did not return its typed exit code to the public wrapper."
    }
    $exitCode = [int]$nestedExitCode
    if ($exitCode -eq 0 -and -not ($isUninstall -or $isRollback)) {
        $expectedSummaryStatus = if ($isDryRun) { "DRY_RUN" } else { "PASS" }
        $verifiedInstallSummary = Assert-OwnedInstalledTarget `
            $installRoot $summaryPath $sourceManifestSha256 $expectedSummaryStatus
        $verifiedInstalledExecutable = Assert-ManifestBoundInstalledExecutable $installRoot $manifest
    }
}
catch {
    $exitCode = 1
    $nestedFailureDiagnostic = New-InstallerFailureDiagnostic `
        "install_label_match_direct_sync.ps1" `
        $null `
        "NESTED_INSTALLER_EXCEPTION" `
        $_.Exception
    if (-not ($isUninstall -or $isRollback)) {
        $publicReport.status = "FAILED"
        $publicReport.nested_exit_code = $exitCode
        $publicReport["failure_diagnostic"] = $nestedFailureDiagnostic
        $publicReport["failure"] = if ($nestedFailureDiagnostic.Contains("inner_exception_message")) {
            $nestedFailureDiagnostic["inner_exception_message"]
        }
        else {
            "Nested installer invocation failed."
        }
        Write-Utf8JsonFile $publicReportPath $publicReport
    }
}

if ($exitCode -ne 0) {
    if (-not ($isUninstall -or $isRollback)) {
        $publicReport.status = "FAILED"
        $publicReport.nested_exit_code = $exitCode
        $publicReport["failure_diagnostic"] = ConvertTo-InstallerFailureDiagnostic `
            $nestedFailureDiagnostic `
            "install_label_match_direct_sync.ps1" `
            $exitCode `
            "NESTED_INSTALLER_NONZERO_EXIT"
        Write-Utf8JsonFile $publicReportPath $publicReport
    }
    if ($targetCreated) {
        if (Test-Path -LiteralPath $installRoot -PathType Container) {
            Remove-Item -LiteralPath $installRoot -Recurse -Force -ErrorAction SilentlyContinue
        }
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
    $nestedRemovalResourceReportSha256 = Get-FileSha256 $nestedRemovalResourceReportPath
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
        if ($null -eq $ownedPublicReport) {
            throw "Public install receipt ownership is not proven."
        }
        if (-not (Test-Path -LiteralPath $publicReportPath -PathType Leaf)) {
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
        if ((Get-FileSha256 $rollbackResourceReportPath) -cne $nestedRemovalResourceReportSha256) {
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
        $finalResourceReportSha256 = Get-FileSha256 $rollbackResourceReportPath
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
        if ((Get-FileSha256 $uninstallReportPath) -cne $nestedRemovalResourceReportSha256) {
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
    try {
        if ($null -eq $verifiedInstallSummary -or $null -eq $verifiedInstalledExecutable) {
            throw "Successful nested install lacks verified executable and ownership postconditions."
        }
        $publicReport.status = if ($isDryRun) { "DRY_RUN_STAGED" } else { "PASS" }
        $publicReport.nested_exit_code = 0
        $publicReport.install_summary = [ordered]@{
            path = [System.IO.Path]::GetFullPath($summaryPath)
            sha256 = Get-FileSha256 $summaryPath
            status = [string]$verifiedInstallSummary.status
            source_manifest_sha256 = [string]$verifiedInstallSummary.source_manifest_sha256
        }
        $publicReport.installed_executable = $verifiedInstalledExecutable
        Write-Utf8JsonFile $publicReportPath $publicReport

        $ownedPublicReport = Read-JsonFile $publicReportPath "public install report"
        $expectedPublicStatus = if ($isDryRun) { "DRY_RUN_STAGED" } else { "PASS" }
        if (
            $ownedPublicReport.report_version -cne "label-match-public-install-v2" -or
            $ownedPublicReport.status -cne $expectedPublicStatus -or
            [string]$ownedPublicReport.source_manifest_sha256 -cne $sourceManifestSha256 -or
            [string]$ownedPublicReport.install_summary.sha256 -cne (Get-FileSha256 $summaryPath) -or
            [string]$ownedPublicReport.installed_executable.sha256 -cne [string]$verifiedInstalledExecutable.sha256 -or
            [long]$ownedPublicReport.installed_executable.size -ne [long]$verifiedInstalledExecutable.size -or
            -not (Test-SamePath ([string]$ownedPublicReport.install_summary.path) $summaryPath) -or
            -not (Test-SamePath ([string]$ownedPublicReport.installed_executable.path) (Join-Path $installRoot "Label_Match.exe"))
        ) {
            throw "Public install receipt does not bind the verified install summary and executable."
        }
        [void](Assert-OwnedInstalledTarget $installRoot $summaryPath $sourceManifestSha256 $expectedSummaryStatus)
        [void](Assert-ManifestBoundInstalledExecutable $installRoot $manifest)
        Write-Output "INSTALLED manifest=$sourceManifestSha256"
    }
    catch {
        $postconditionDiagnostic = New-InstallerFailureDiagnostic `
            "INSTALL_THIS_PC.ps1" `
            $null `
            "PUBLIC_POSTCONDITION_FAILED" `
            $_.Exception
        $publicReport.status = "FAILED"
        $publicReport.nested_exit_code = 1
        $publicReport["failure_diagnostic"] = $postconditionDiagnostic
        $publicReport["failure"] = if ($postconditionDiagnostic.Contains("inner_exception_message")) {
            $postconditionDiagnostic["inner_exception_message"]
        }
        else {
            "Public install postcondition failed."
        }
        Write-Utf8JsonFile $publicReportPath $publicReport
        if ($targetCreated) {
            if (Test-Path -LiteralPath $installRoot -PathType Container) {
                Remove-Item -LiteralPath $installRoot -Recurse -Force -ErrorAction SilentlyContinue
            }
        }
        exit 1
    }
}
exit 0
