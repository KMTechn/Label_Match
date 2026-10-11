[CmdletBinding()]
param(
    [string]$SourceRoot = "",
    [string]$InstallRoot = "C:\KMTech\Apps\Label_Match\current",
    [string]$EvidencePath = "",
    [string]$ServerBaseUrl = "",
    [string]$ResumeRollbackAuditPath = "",
    [string]$ExpectedRollbackAuditSha256 = "",
    [switch]$PlanOnly,
    [switch]$AllowNoncanonicalLayoutForTest,
    [switch]$SkipSignatureValidationForTest
)

trap {
    $publicError = $_
    $publicExit = 1
    if ($publicError.Exception.Data.Contains('installation_preparation_exit_code')) {
        $publicExit = [int]$publicError.Exception.Data['installation_preparation_exit_code']
    }
    if ((Get-Command Get-LabelLaunchFailure -ErrorAction SilentlyContinue) -and
        (Get-Command ConvertTo-KmtechPublicRecord -ErrorAction SilentlyContinue)) {
        $publicFailure = Get-LabelLaunchFailure $publicError 'forward'
    } else {
        # The pinned helper may not have loaded yet. No error text is emitted.
        $publicFailure = [ordered]@{status='FAILED';reason_code='INSTALLER_FAILED';report_status='REPORT_UNAVAILABLE';exception_type=$publicError.Exception.GetBaseException().GetType().FullName;location_module='INSTALL_CANONICAL_PORTABLE.ps1';location_function='ScriptBlock';location_line=[int]$publicError.InvocationInfo.ScriptLineNumber}
    }
    if ($publicError.Exception.Data.Contains('label_failure_code') -and
        [string]$publicError.Exception.Data['label_failure_code'] -ceq 'SERVER_BASE_URL_INVALID') {
        $publicFailure.reason_code = 'SERVER_BASE_URL_INVALID'
        $publicFailure.guidance = '서버 주소는 인증정보가 없는 HTTPS 호스트 주소로 입력하세요.'
    }
    [Console]::Error.WriteLine('LABEL_MATCH_INSTALLER_FAILURE=' + ($publicFailure | ConvertTo-Json -Compress -Depth 5))
    exit $publicExit
}

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
$CanonicalTaskName = 'direct-sync-relay-label-match'
$testMode = $AllowNoncanonicalLayoutForTest -and
    [string]$env:KMTECH_FACTORY_INSTALL_TEST_MODE -ceq '1'
if ($SkipSignatureValidationForTest -and -not $testMode) {
    throw 'Signature bypass is test-only.'
}

function Get-RequiredExternalBoolean($Object, [string]$Name) {
    if ($null -eq $Object) { throw "External object is absent: $Name" }
    if ($Object -is [Collections.IDictionary]) {
        if (-not $Object.Contains($Name)) { throw "External boolean is absent: $Name" }
        $value = $Object[$Name]
    }
    else {
        $property = $Object.PSObject.Properties[$Name]
        if ($null -eq $property) { throw "External boolean is absent: $Name" }
        $value = $property.Value
    }
    if ($value -isnot [bool]) { throw "External boolean has invalid type: $Name" }
    return [bool]$value
}

function Test-RelayPersistentRetry($Relay) {
    return Get-RequiredExternalBoolean $Relay 'persistent_retry'
}

# This small trust bootstrap is intentionally local to both standalone loaders.
# Never use shared code (or start Python) to establish its own execution authority.
function Get-LabelSharedPortableLeafPath([string]$CodeRoot, [ValidateSet('portable.ps1','data_generation.ps1')][string]$Leaf = 'portable.ps1') {
    if ([string]::IsNullOrWhiteSpace($CodeRoot) -or -not [IO.Path]::IsPathRooted($CodeRoot) -or
        $CodeRoot.StartsWith('\\?\') -or $CodeRoot.StartsWith('\\.\')) {
        throw 'Shared PowerShell code root must be an ordinary absolute path.'
    }
    $root = [IO.Path]::GetFullPath($CodeRoot)
    # Inspect ancestors before probing app/, including links above the code root.
    $ancestor = $root
    while ($ancestor) {
        $item = Get-Item -LiteralPath $ancestor -Force
        if (($item.Attributes -band [IO.FileAttributes]::ReparsePoint) -ne 0) {
            throw "Shared PowerShell path contains a reparse point: $ancestor"
        }
        $ancestor = Split-Path -Path $ancestor -Parent
    }
    $appRoot = if (Test-Path -LiteralPath (Join-Path $root 'app') -PathType Container) {
        Join-Path $root 'app'
    } else { $root }
    foreach ($relative in @('', 'kmtech_shared', 'kmtech_shared\powershell',
        'kmtech_shared.lock.json', 'kmtech_shared.manifest.json', 'kmtech_shared\powershell\portable.ps1', 'kmtech_shared\powershell\data_generation.ps1')) {
        $path = if ($relative) { Join-Path $appRoot $relative } else { $appRoot }
        $item = Get-Item -LiteralPath $path -Force
        if (($item.Attributes -band [IO.FileAttributes]::ReparsePoint) -ne 0) {
            throw "Shared PowerShell path contains a reparse point: $path"
        }
    }
    $expectedManifestSha = 'de1aa3a5a2b072ec15ec2b3d60733881dcf4d5c87f14b15ed06691ba643ac918'
    $lockPath = Join-Path $appRoot 'kmtech_shared.lock.json'
    if ((Get-Item -LiteralPath $lockPath).Length -gt 65536) { throw 'Shared consumer lock is oversized.' }
    $lock = Get-Content -LiteralPath $lockPath -Raw -Encoding UTF8 | ConvertFrom-Json
    if ([string]$lock.version -cne '0.3.3' -or [string]$lock.manifest_sha256 -cne $expectedManifestSha) {
        throw 'Shared consumer manifest pin mismatch.'
    }
    $manifestPath = Join-Path $appRoot 'kmtech_shared.manifest.json'
    if ((Get-Item -LiteralPath $manifestPath).Length -gt 65536) { throw 'Shared manifest is oversized.' }
    [byte[]]$manifestBytes = [IO.File]::ReadAllBytes($manifestPath)
    $hash = [Security.Cryptography.SHA256]::Create()
    try {
        $actualManifestSha = ([BitConverter]::ToString($hash.ComputeHash($manifestBytes))).Replace('-', '').ToLowerInvariant()
    } finally { $hash.Dispose() }
    if ($actualManifestSha -cne $expectedManifestSha) { throw 'Shared manifest pin mismatch.' }
    $pinnedSharedManifest = (New-Object Text.UTF8Encoding($false, $true)).GetString($manifestBytes) | ConvertFrom-Json
    $sharedLeaf = Join-Path $appRoot ('kmtech_shared\powershell\' + $Leaf)
    $expectedLeafSha = $pinnedSharedManifest.files.('kmtech_shared/powershell/' + $Leaf)
    $stream = [IO.File]::OpenRead($sharedLeaf)
    $hash = [Security.Cryptography.SHA256]::Create()
    try {
        $actualLeafSha = ([BitConverter]::ToString($hash.ComputeHash($stream))).Replace('-', '').ToLowerInvariant()
    } finally { $hash.Dispose(); $stream.Dispose() }
    if ($actualLeafSha -cne $expectedLeafSha) {
        throw 'Shared PowerShell leaf pin mismatch.'
    }
    return $sharedLeaf
}

function Full([string]$Value, [string]$Purpose) {
    return ConvertTo-KmtechFullPath $Value $Purpose
}

function Same([string]$Left, [string]$Right) {
    return (Full $Left 'left path').Equals((Full $Right 'right path'), 'OrdinalIgnoreCase')
}

function Sha([string]$Path) {
    return Get-KmtechFileSha $Path
}

function ByteSha([byte[]]$Bytes) {
    $hash = [Security.Cryptography.SHA256]::Create()
    try {
        return ([BitConverter]::ToString($hash.ComputeHash($Bytes))).Replace('-', '').ToLowerInvariant()
    }
    finally { $hash.Dispose() }
}

function PinnedFileBytes([string]$Path, [string]$ExpectedSha256) {
    [byte[]]$bytes = [IO.File]::ReadAllBytes($Path)
    if ((ByteSha $bytes) -cne $ExpectedSha256) {
        throw "Pinned file bytes differ: $Path"
    }
    return ,$bytes
}

function UInt64BE([uint64]$Value) {
    [byte[]]$bytes = [BitConverter]::GetBytes($Value)
    if ([BitConverter]::IsLittleEndian) { [Array]::Reverse($bytes) }
    return $bytes
}

function HexBytes([string]$Value) {
    if ($Value -notmatch '^[0-9a-f]{64}$') { throw 'Inventory SHA-256 is invalid.' }
    [byte[]]$bytes = New-Object byte[] 32
    for ($index = 0; $index -lt 32; $index++) {
        $bytes[$index] = [Convert]::ToByte($Value.Substring($index * 2, 2), 16)
    }
    return $bytes
}

function PortableInventory([string]$Root) {
    $rootFull = Full $Root 'portable inventory root'
    $prefix = $rootFull + '\'
    $seen = @{}
    $records = @()
    foreach ($file in @(Get-ChildItem -LiteralPath $rootFull -File -Force -Recurse)) {
        if (($file.Attributes -band [IO.FileAttributes]::ReparsePoint) -ne 0) {
            throw "Portable inventory contains a reparse point: $($file.FullName)"
        }
        if (-not $file.FullName.StartsWith($prefix, [StringComparison]::OrdinalIgnoreCase)) {
            throw 'Portable inventory path escaped its root.'
        }
        $relative = $file.FullName.Substring($prefix.Length).Replace('\', '/')
        if ($relative.Equals('bootstrap-integrity.json', [StringComparison]::OrdinalIgnoreCase)) {
            continue
        }
        if ($seen.ContainsKey($relative)) {
            throw 'Portable inventory contains case-insensitive duplicate paths.'
        }
        $seen[$relative] = $true
        $records += [pscustomobject][ordered]@{
            path = $relative
            size = [uint64]$file.Length
            sha256 = Sha $file.FullName
        }
    }
    if ($records.Count -eq 0) { throw 'Portable inventory is empty.' }
    [object[]]$orderedRecords = @($records)
    [Array]::Sort(
        $orderedRecords,
        [System.Collections.Generic.Comparer[object]]::Create(
            [System.Comparison[object]]{
                param($left, $right)
                return [string]::CompareOrdinal([string]$left.path, [string]$right.path)
            }
        )
    )
    $records = $orderedRecords
    $stream = New-Object IO.MemoryStream
    $hash = [Security.Cryptography.SHA256]::Create()
    try {
        [byte[]]$domain = [Text.Encoding]::ASCII.GetBytes(
            'label-match-portable-full-inventory-v1'
        )
        $stream.Write($domain, 0, $domain.Length)
        $stream.WriteByte(0)
        [uint64]$byteCount = 0
        foreach ($record in $records) {
            [byte[]]$pathBytes = (New-Object Text.UTF8Encoding($false)).GetBytes(
                [string]$record.path
            )
            [byte[]]$pathLength = UInt64BE ([uint64]$pathBytes.Length)
            [byte[]]$sizeBytes = UInt64BE ([uint64]$record.size)
            [byte[]]$contentHash = HexBytes ([string]$record.sha256)
            $stream.Write($pathLength, 0, $pathLength.Length)
            $stream.Write($pathBytes, 0, $pathBytes.Length)
            $stream.Write($sizeBytes, 0, $sizeBytes.Length)
            $stream.Write($contentHash, 0, $contentHash.Length)
            $byteCount += [uint64]$record.size
        }
        $stream.Position = 0
        $aggregate = ([BitConverter]::ToString($hash.ComputeHash($stream))).Replace('-', '').ToLowerInvariant()
        $bootstrapLines = @(
            # Already ordinal UTF-16 path ordered; see tools/bootstrap_integrity.ps1.
            $records | ForEach-Object {
                "$($_.sha256) $($_.size) $($_.path)"
            }
        )
        [byte[]]$bootstrapBytes = (New-Object Text.UTF8Encoding($false)).GetBytes(
            ($bootstrapLines -join "`n") + "`n"
        )
        $bootstrapHash = [Security.Cryptography.SHA256]::Create()
        try {
            $bootstrapAggregate = ([BitConverter]::ToString(
                $bootstrapHash.ComputeHash($bootstrapBytes)
            )).Replace('-', '').ToLowerInvariant()
        }
        finally { $bootstrapHash.Dispose() }
        $criticalRecords = @{}
        foreach ($record in $records) { $criticalRecords[[string]$record.path] = $record }
        foreach ($criticalPath in @(
            'INSTALL_THIS_PC.ps1',
            'tools/bootstrap_integrity.ps1',
            'tools/label_writer_fence.ps1'
        )) {
            if (-not $criticalRecords.ContainsKey($criticalPath)) {
                throw "Portable inventory is missing critical file: $criticalPath"
            }
        }
        return [pscustomobject][ordered]@{
            schema_version = 'label-match-portable-full-inventory-v1'
            algorithm = 'sha256-domain-ordinal-path-length-path-size-content-digest-v1'
            file_count = [int]$records.Count
            byte_count = $byteCount
            sha256 = $aggregate
            bootstrap_aggregate_sha256 = $bootstrapAggregate
            critical_file_sha256 = [pscustomobject][ordered]@{
                placement_helper = [string]$criticalRecords['INSTALL_THIS_PC.ps1'].sha256
                bootstrap_integrity_helper = [string](
                    $criticalRecords['tools/bootstrap_integrity.ps1'].sha256
                )
                writer_fence_helper = [string](
                    $criticalRecords['tools/label_writer_fence.ps1'].sha256
                )
            }
        }
    }
    finally {
        $hash.Dispose()
        $stream.Dispose()
    }
}

function ReceiptSource([string]$Root, $ManifestValue) {
    $receiptPathValue = [string]$env:KMTECH_LABEL_CONFLICT_RESOLUTION_RECEIPT_PATH
    $receiptHashValue = [string]$env:KMTECH_LABEL_CONFLICT_RESOLUTION_RECEIPT_SHA256
    if (
        [string]::IsNullOrWhiteSpace($receiptPathValue) -or
        $receiptHashValue -cnotmatch '^[0-9a-f]{64}$'
    ) { throw 'Pinned conflict-resolution receipt is required.' }
    $receiptPath = Resolve-LabelGenerationInputPath $receiptPathValue $Root
    # ReceiptSource also runs in PLAN_ONLY and guarded helper paths. Admit its
    # exact selected parent before its first content read in every caller.
    $receiptSubjectSid = [Security.Principal.WindowsIdentity]::GetCurrent().User.Value
    $receiptMarkerPaths = @(
        (Join-Path (Split-Path -Parent $receiptPath) 'data-generation.json')
    )
    [void](Assert-LabelDataDowngrade -MarkerPaths $receiptMarkerPaths -SubjectSid $receiptSubjectSid `
        -TargetVersion (Get-LabelPortableDataVersion $Root $ManifestValue) `
        -TargetBuildId ([string]$ManifestValue.source_commit))
    if (-not (Test-Path -LiteralPath $receiptPath -PathType Leaf)) {
        throw 'Pinned conflict-resolution receipt is absent.'
    }
    [byte[]]$receiptBytes = [IO.File]::ReadAllBytes($receiptPath)
    if ($receiptBytes.Length -eq 0 -or $receiptBytes.Length -gt 1048576) {
        throw 'Pinned conflict-resolution receipt is oversized.'
    }
    $receiptHash = [Security.Cryptography.SHA256]::Create()
    try {
        $observedReceiptHash = ([BitConverter]::ToString(
            $receiptHash.ComputeHash($receiptBytes)
        )).Replace('-', '').ToLowerInvariant()
    }
    finally { $receiptHash.Dispose() }
    if ($observedReceiptHash -cne $receiptHashValue) {
        throw 'Pinned conflict-resolution receipt SHA-256 differs.'
    }
    $receiptText = (New-Object Text.UTF8Encoding($false, $true)).GetString(
        $receiptBytes
    )
    $receipt = $receiptText | ConvertFrom-Json
    $inventory = PortableInventory $Root
    if (
        [string]$receipt.schema_version -cne 'label-match-exact-clone-resolution-v2' -or
        [string]$receipt.status -cne 'RESOLVED' -or
        [string]$receipt.conflict_code -cne 'EXACT_CLONE_RUNTIME_CONFLICT' -or
        -not (Same ([string]$receipt.portable.root) $Root) -or
        [string]$receipt.portable.source_commit -cne [string]$ManifestValue.source_commit -or
        [string]$receipt.portable.source_tree -cne [string]$ManifestValue.source_tree -or
        [string]$receipt.portable.portable_manifest_sha256 -cne (Sha (Join-Path $Root 'portable-manifest.json')) -or
        [string]$receipt.portable.canonical_installer_sha256 -cne (Sha (Join-Path $Root 'INSTALL_CANONICAL_PORTABLE.ps1')) -or
        [string]$receipt.portable_inventory.schema_version -cne [string]$inventory.schema_version -or
        [string]$receipt.portable_inventory.algorithm -cne [string]$inventory.algorithm -or
        [int]$receipt.portable_inventory.file_count -ne [int]$inventory.file_count -or
        [uint64]$receipt.portable_inventory.byte_count -ne [uint64]$inventory.byte_count -or
        [string]$receipt.portable_inventory.sha256 -cne [string]$inventory.sha256 -or
        [string]$receipt.portable_inventory.critical_file_sha256.placement_helper -cne
            [string]$inventory.critical_file_sha256.placement_helper -or
        [string]$receipt.portable_inventory.critical_file_sha256.bootstrap_integrity_helper -cne
            [string]$inventory.critical_file_sha256.bootstrap_integrity_helper -or
        [string]$receipt.portable_inventory.critical_file_sha256.writer_fence_helper -cne
            [string]$inventory.critical_file_sha256.writer_fence_helper
    ) { throw 'Pinned receipt full portable source inventory differs.' }
    return $inventory
}

function Arg([string]$Value) {
    if ($Value.Contains('"')) { throw 'A command path contains a quote.' }
    if ($Value -match '\s') { return '"' + $Value + '"' }
    return $Value
}

function Command([string]$Root, [switch]$StartupUpdate) {
    if ($StartupUpdate) {
        return ('{0} -I -B {1} --bootstrap --role relay -- --label-match-user-relay' -f (Arg (Join-Path $Root 'runtime\pythonw.exe')), (Arg (Join-Path $Root 'app\auto_update.py')))
    }
    if (Test-Path -LiteralPath (Join-Path (Split-Path -Parent $Root) 'update-control\policy.json')) {
        return Command $Root -StartupUpdate
    }
    return ('{0} -I -B {1} --label-match-user-relay' -f
        (Arg (Join-Path $Root 'runtime\pythonw.exe')),
        (Arg (Join-Path $Root 'app\main.py')))
}

function RelayCodeRoot([string]$Root) {
    $control=Join-Path (Split-Path -Parent $Root) 'update-control'
    if (-not (Test-Path -LiteralPath (Join-Path $control 'policy.json'))) { return $Root }
    $active=Get-Content -LiteralPath (Join-Path $control 'active.json') -Raw -Encoding UTF8 | ConvertFrom-Json
    if ([string]$active.schema -cne 'label-match-startup-update-v1' -or [string]$active.sha -cnotmatch '^[0-9a-f]{40}$') { throw 'Startup update active code is invalid.' }
    $version=Join-Path (Join-Path (Split-Path -Parent $Root) 'versions') ([string]$active.sha)
    if (-not (Test-Path -LiteralPath (Join-Path $version 'app\main.py'))) { throw 'Startup update active code is absent.' }
    return $version
}

function ServerBaseUrlOrigin([string]$Value) {
    # Optional onboarding endpoint; empty keeps the product default. Only a
    # credential-free https://host[:port] origin is accepted so the value can never
    # carry a path, query, fragment, whitespace, or quoting into the product command.
    $origin = $Value.Trim()
    if ($origin.Length -eq 0) { return '' }
    if (
        $origin -notmatch ('^https://' +
            '(?<host>[A-Za-z0-9](?:[A-Za-z0-9-]{0,61}[A-Za-z0-9])?' +
            '(?:\.[A-Za-z0-9](?:[A-Za-z0-9-]{0,61}[A-Za-z0-9])?)*)' +
            '(?::(?<port>[1-9][0-9]{0,4}))?/?$')
    ) {
        $originFailure = [ArgumentException]::new('ServerBaseUrl must be a credential-free https://host[:port] origin.')
        $originFailure.Data['label_failure_code'] = 'SERVER_BASE_URL_INVALID'
        throw $originFailure
    }
    if ($Matches.ContainsKey('port') -and [int]$Matches['port'] -gt 65535) {
        $originFailure = [ArgumentException]::new('ServerBaseUrl port must be within 1-65535.')
        $originFailure.Data['label_failure_code'] = 'SERVER_BASE_URL_INVALID'
        throw $originFailure
    }
    return $origin.TrimEnd('/')
}

function OnboardingArguments([string]$ServerBaseUrlValue) {
    if ($ServerBaseUrlValue.Length -eq 0) { return ,[string[]]@() }
    return ,[string[]]@('--server-base-url', $ServerBaseUrlValue)
}

function Manifest([string]$Root, [bool]$UnsignedOk) {
    Assert-KmtechPortableTree $Root @(
        'portable-manifest.json',
        'runtime\python.exe',
        'runtime\pythonw.exe',
        'app\main.py',
        'launch-label-match.cmd',
        'INSTALL_CANONICAL_PORTABLE.ps1',
        'INSTALL_THIS_PC.ps1',
        'tools\bootstrap_integrity.ps1',
        'tools\label_writer_fence.ps1'
    )
    $value = Read-KmtechPortableManifest $Root
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
    Assert-KmtechCPythonSignature $Root $UnsignedOk
    return $value
}

function Invoke-LabelPinnedProbe([string]$Python, [byte[]]$ProbeBytes,
    [string]$ExpectedSha256, [string[]]$ProbeArguments = @(),
    [string]$FailureMessage = 'Pinned Python probe failed: ') {
    # Hash and execute the same bytes; no mutable temporary script path.
    $sha256 = [Security.Cryptography.SHA256]::Create()
    try { $actual = [BitConverter]::ToString($sha256.ComputeHash($ProbeBytes)).Replace('-', '').ToLowerInvariant() }
    finally { $sha256.Dispose() }
    if ($actual -cne $ExpectedSha256) { throw 'PINNED_PROBE_SHA256_MISMATCH' }
    $startInfo = New-Object Diagnostics.ProcessStartInfo
    $startInfo.FileName = $Python
    $startInfo.Arguments = '-I -B -'
    foreach ($argument in $ProbeArguments) { $startInfo.Arguments += ' ' + (Arg $argument) }
    $startInfo.UseShellExecute = $false
    $startInfo.CreateNoWindow = $true
    $startInfo.RedirectStandardInput = $true
    $startInfo.RedirectStandardOutput = $true
    $startInfo.RedirectStandardError = $true
    $process = [Diagnostics.Process]::Start($startInfo)
    try {
        # Drain both pipes while writing the program and waiting for its exit.
        Initialize-LabelChildOutputReader
        $stdout = $process.StandardOutput.ReadToEndAsync()
        $stderr = [LabelChildOutputReader]::Read($process.StandardError)
        try { $process.StandardInput.BaseStream.Write($ProbeBytes, 0, $ProbeBytes.Length) }
        finally { $process.StandardInput.Close() }
        $process.WaitForExit()
        $output = $stdout.GetAwaiter().GetResult()
        $errorOutput = ConvertTo-LabelFailureOutput ($stderr.GetAwaiter().GetResult())
        if ($process.ExitCode -ne 0) {
            $bytes = [Text.Encoding]::UTF8.GetBytes($output)
            $out = [ordered]@{text=(Protect-LabelFailureText $output);byte_count=$bytes.Length;truncated=($bytes.Length -gt 65536);limit_bytes=65536}
            throw (New-LabelChildFailure $FailureMessage $process.ExitCode $out $errorOutput)
        }
        return $output
    }
    finally { $process.Dispose() }
}

function Assert-WriterTransition([string]$Source, [string]$Installed) {
    # Run only the attested candidate's scanner. Never import installed code.
    # Changed semantics require an exact, reviewed directional release pair;
    # otherwise AST equality admits only comments/line movement.
    $probe = @'
import ast, hashlib, json, pathlib, re, sys
source, installed = map(pathlib.Path, sys.argv[1:3])
signed_generation_reverse_allowed = len(sys.argv) == 4 and sys.argv[3] == 'signed-compatible-downgrade'
sys.path.insert(0, str(source / 'app'))
from writer_sink_inventory import derive_writer_sink_inventory, writer_sink_inventory_sha256, _powershell_inventory

def identity(root):
    path = root / 'app/writer_session_fence.py'
    tree = ast.parse(path.read_text(encoding='utf-8-sig'))
    pins = [node for node in tree.body if isinstance(node, ast.Assign) and
            any(isinstance(target, ast.Name) and target.id == 'WRITER_INVENTORY_SHA256' for target in node.targets)]
    if len(pins) != 1 or not isinstance(pins[0].value, ast.Constant) or not isinstance(pins[0].value.value, str):
        raise ValueError('WRITER_TRANSITION_PIN_INVALID')
    pin = pins[0].value.value
    helper = (root / 'tools/label_writer_fence.ps1').read_text(encoding='utf-8-sig')
    helper_pins = re.findall(r"(?m)^\$Script:LabelWriterFenceInventorySha256 = '([0-9a-f]{64})'\s*$", helper)
    rows = derive_writer_sink_inventory(root / 'app')
    rows += _powershell_inventory(root / 'INSTALL_THIS_PC.ps1', root)
    rows.sort(key=lambda row: (row.source, row.source_path.casefold(), row.source_line, row.qualified_name))
    if helper_pins != [pin] or writer_sink_inventory_sha256(rows) != pin:
        raise ValueError('WRITER_TRANSITION_PIN_INVALID')
    membership = [(row.source, row.source_path, row.qualified_name, row.guard_kind) for row in rows]
    return pin, membership

def files(root, directory):
    return {path.relative_to(root).as_posix(): path for path in (root / directory).rglob('*') if path.is_file()}

def digest(path):
    with path.open('rb') as stream:
        return hashlib.file_digest(stream, 'sha256').digest()

def syntax(path, relative):
    tree = ast.parse(path.read_text(encoding='utf-8-sig'))
    if relative == 'app/writer_session_fence.py':
        # The only supported executable delta is this exact test-root isolation
        # branch. It is unreachable without the explicit test-mode/root pair;
        # ordinary production admission remains AST-identical to the preimage.
        test_guard = ast.parse('if (str((os.environ if environ is None else environ).get(TEST_MODE_ENV) or "") == "1" and str((os.environ if environ is None else environ).get(CONTROL_ROOT_OVERRIDE_ENV) or "").strip()):\n    return f"{WRITER_MUTEX_NAME}.{_sha256_text(selected)[:16]}"').body[0]
        for node in tree.body:
            if isinstance(node, ast.Assign) and any(isinstance(target, ast.Name) and target.id == 'WRITER_INVENTORY_SHA256' for target in node.targets):
                node.value = ast.Constant(value='validated-inventory-pin')
            if isinstance(node, ast.FunctionDef) and node.name == 'writer_admission_mutex_name' and len(node.body) > 1:
                if ast.dump(node.body[1], include_attributes=False) == ast.dump(test_guard, include_attributes=False):
                    del node.body[1]
    return ast.dump(tree, include_attributes=False)

# X13-B explicitly supports the 57f52e1 shared 0.2.0 preimage. Only these
# release-declared replacements and the read-only leaf addition may differ.
# Pins below hash Git LF text; CRLF checkout serialization is also supported.
shared_replacements = {
    'app/kmtech_shared/__init__.py': (
        'b1b16c26ef8cd85be89e13b24afda1ec7be1d4ce8d7632ab032981601f223455',
        '7714a7a02cda3da34c4972101c0e01cc052435a00d8d520b5c274a05aa9850e8'),
    'app/kmtech_shared.manifest.json': (
        '465eea8ad20f010e29be5bb98cbc32fdcd1d163d5ba7b76d9eac3b33c14874ee',
        'de1aa3a5a2b072ec15ec2b3d60733881dcf4d5c87f14b15ed06691ba643ac918'),
    'app/kmtech_shared.lock.json': (
        '6807ed97f7d734c5d8bdb0e23c141916cc5f013bd2392a46b98849d285d54fcd',
        '14ff6bbc353aab8fa9ce88297c79f2ca5504c554e0edb9671eb62d1766395e9f'),
    'app/kmtech_zero_pe.vendor.json': (
        'e7dac78746db6efbccedcba5c3b44b58b77bc44e8d25a2dbaea6dc1ac01e307c',
        'bf7ccf44e5abd14b3211bbea9b1e22d3b4a53525b4d4bc760c1893ca2da63a69'),
    'INSTALL_THIS_PC.ps1': (
        '7b9e32726e81185df274433d80177f3b59bf3b87291c641cebc51e4a6863250b',
        'bdc42c26a8b5464d8a65e880ba5474ef01a28c820997adcb5ddefa8daa9b022b'),
    'tools/bootstrap_integrity.ps1': (
        '7094e69137179c2fe66119a19358db66b375a4db45a8abdf55965bc75f7a37e2',
        '96101ce38befdf9dcca09ec42abd571aa441ad312f35bf3d14fd7effc97db963'),
}
shared_additions = {
    'app/kmtech_shared/powershell/portable.ps1':
        'fb2e506d2194eb54f526f4aa73515ebd226278a3f4f44cbc3854de91186781d1',
}

def release_digest(path):
    return hashlib.sha256(path.read_bytes().replace(b'\r\n', b'\n')).hexdigest()

# G-DECISIONS #2/#7: exact e89c188 preimage and three pinned leaves.
data_generation_replacements = {'app/Label_Match.py': ('65ab1913f9720b59b49ef6e5799891cf02af1fd3a413d5308ca568fa9a899145', '43c93e151623c6098ab6dc0b200d5d44c415cdbe12f2e3be97c3172c41d270e9'), 'app/current_user_onboarding.py': ('84fde787fe1bca5d3138c3cd243d9b13ffaf834ffc69a5414b39d0c2b81f905d', '93a4d7b67b9d740ddbf8bfa9546f01fc0c927ee0de985f4be4e0d83adf435ebd'), 'app/tools/direct_sync_relay_runner.py': ('c797ffeee38f5161621e526c4ef2f3844cbe848ea0124f875f3df05a1779af98', '9b8e7a9845c4e73edea64b4ac3d0b90e3f461b7d926e0e1db64bdb9197f734ac'), 'app/kmtech_shared/__init__.py': ('32f34fccf9a00e5d27e852a95fc7b776f18a5eccda991f47dc33cd8111d50295', '7714a7a02cda3da34c4972101c0e01cc052435a00d8d520b5c274a05aa9850e8'), 'app/kmtech_shared.manifest.json': ('feaed459688e915eb5f52f264501a4287261d9dcbdbc50e0c6f4bde59d8e7117', 'de1aa3a5a2b072ec15ec2b3d60733881dcf4d5c87f14b15ed06691ba643ac918'), 'app/kmtech_shared.lock.json': ('87cf641db77b8052ffbaeee86d0c2cbab84f07ce4c3748c796985be2eacc0cf9', '14ff6bbc353aab8fa9ce88297c79f2ca5504c554e0edb9671eb62d1766395e9f'), 'INSTALL_THIS_PC.ps1': ('8efe37d3203c94c71dff93a0761a674f400154b7b2d5c7bef8a91e2d09e5b702', 'bdc42c26a8b5464d8a65e880ba5474ef01a28c820997adcb5ddefa8daa9b022b'), 'tools/bootstrap_integrity.ps1': ('7cf77e9cc833093aed9ed7b3d79565f5396eed0288b1e7729e08959e82e12ff3', '96101ce38befdf9dcca09ec42abd571aa441ad312f35bf3d14fd7effc97db963'), 'app/writer_session_fence.py': ('26d4d4900f55cafbfad24099065ecfdd6b275327e60b93ac63d67186a05e09e5', '92419d83453e49cd225e79ccc1aa96230c7ba00636b7fd114ed02736e6c074e1'), 'tools/label_writer_fence.ps1': ('fbff47dd51d1d4503ef0dd0e9ec6796adaf943882f6771a19e85059d694de976', '44ad315d0ffe526f003e3d8d17ff0940338652e7e557ad22ed6e728551ce3d3f')}
data_generation_additions = {'app/label_data_generation.py': '31a8a2a16f037fe0bc7e1502ee8aeb725aafbbf4e72cd3c0f2af550d822a3b8f', 'app/kmtech_shared/data_generation.py': '14ffe4277dc60a32a6074abf3d56ec0b0dcf3d5accd372f6dd41f36de0e348c6', 'app/kmtech_shared/powershell/data_generation.ps1': 'b809e49985c73e0b1aedbe9dd3a8050177939f6fb088087e6d3e4c9bc05b5c96', 'app/failure_redaction.py': '58f77e71532747b439c9d5fcf8c52bbf620f974f51ca3b63ad0dcf764fea4af9', 'app/tools/failure_redaction.ps1': 'c81ce889aef0d216985a9dbaf439c2bfa11640d929e836d2bc7b68966d6d073f', 'app/tools/failure_redaction_rules.json': '092cc5071abb481602e746b08faba67a8fe43c7f5a59bc3e28181f76dc245b2d', 'tools/failure_redaction.ps1': 'c81ce889aef0d216985a9dbaf439c2bfa11640d929e836d2bc7b68966d6d073f', 'tools/failure_redaction_rules.json': '092cc5071abb481602e746b08faba67a8fe43c7f5a59bc3e28181f76dc245b2d'}
data_generation_transition = all(
    (source / relative).is_file() and release_digest(source / relative) == after
    for relative, (_, after) in data_generation_replacements.items()
) and all(
    not (installed / relative).exists() and (source / relative).is_file()
    and release_digest(source / relative) == expected
    for relative, expected in data_generation_additions.items()
)
data_generation_reverse = signed_generation_reverse_allowed and all(
    (source / relative).is_file() and (installed / relative).is_file()
    and release_digest(source / relative) == before and release_digest(installed / relative) == after
    for relative, (before, after) in data_generation_replacements.items()
) and all(
    not (source / relative).exists() and (installed / relative).is_file()
    and release_digest(installed / relative) == expected
    for relative, expected in data_generation_additions.items()
)

# Exact e89/a56 -> merged audit-index bytes, forward only. This does not
# change G's original ten-file declaration or its signed reverse decision.
data_generation_forward_companions = {'app/label_data_manager.py': ('4443b2e817e54e1ba1bbd9065f49123ef1d55b40a7e00f485bc2cda6d5d3d912', '2ddd2129d6de99073f3cd51d8947a4792ea56e371205f3ea70445301317f4e56'), 'app/label_recovery_schema.py': ('5ddc38a222acaa6a2ca4b3bf23fc09f173b1e2646a599a667062df245cbf5349', '4bf2ad6024efd6c3ac2f5965eef3e81fa149078f4bc22ff716272a5c2359ffa6')}
data_generation_companion_transition = all(
    (installed / relative).is_file() and (source / relative).is_file()
    and release_digest(installed / relative) == before
    and release_digest(source / relative) == after
    for relative, (before, after) in data_generation_forward_companions.items()
) and all(
    (source / relative).is_file() and release_digest(source / relative) == after
    for relative, (_, after) in data_generation_replacements.items()
) and all(
    (source / relative).is_file() and release_digest(source / relative) == expected
    for relative, expected in data_generation_additions.items()
)


# Exact committed inventory reader -> immutable source-admission inputs only.
# This reader adds no writer; every writer identity, runtime and other file
# still follows the existing directional release checks. No reverse exception.
source_admission_companions = {'app/label_exact_clone_resolution.py': ('0ff69b167d2b88e9be8d5a6bc068b6f160b4a19023a92b401881fc86cf7b24e4', 'f162f2c2680852cfa1ec52d4a355b275fcb87482561bb84aac89aa24e05696ce')}
source_admission_transition = all(
    (installed / relative).is_file() and (source / relative).is_file()
    and release_digest(installed / relative) == before
    and release_digest(source / relative) == after
    for relative, (before, after) in source_admission_companions.items()
)

# d0e504e -> 0cf5bf6 only: bootstrap v1 readers compare the same complete
# inventory independent of row order and validate its original-order aggregate.
# No persisted schema/writer changes need migration. The caller verifies the
# installed bootstrap record before this read-only probe, then retains the
# code/record preimage for transaction rollback. Downgrade is not admitted.
integrity_order_path = 'app/current_user_onboarding.py'
integrity_order_transition = (
    (installed / integrity_order_path).is_file() and
    (source / integrity_order_path).is_file() and
    release_digest(installed / integrity_order_path) ==
        'bfd59326ad76a400c70a62d78622b518a549e844c4bf4725f59eb7525c20a57f' and
    release_digest(source / integrity_order_path) ==
        '93a4d7b67b9d740ddbf8bfa9546f01fc0c927ee0de985f4be4e0d83adf435ebd'
)

shared_transition = all(
    (installed / relative).is_file() and (source / relative).is_file() and
    release_digest(installed / relative) == before and release_digest(source / relative) == after
    for relative, (before, after) in shared_replacements.items()
) and all(
    not (installed / relative).exists() and (source / relative).is_file() and
    release_digest(source / relative) == expected
    for relative, expected in shared_additions.items()
)

# 3f4574a -> reviewed w9scanonly source only: the legacy-label transition mode adds one leaf
# module and changes these modules. Writer membership, runtime, contracts and
# every other file stay checked. Persisted state gains only optional keys the
# preimage ignores, so no data migration; downgrade is not admitted.
legacy_label_replacements = {'app/label_data_manager.py': ('95887ca72e74aa258ba07584e1df14fc099f4b16998629d4639e4f5749ecc757', '2ddd2129d6de99073f3cd51d8947a4792ea56e371205f3ea70445301317f4e56'),
 'app/Label_Match.py': ('0481e687828da9e99aebad238307586c73f9cd0db8ea53e39603029fbcad921b',
                        '43c93e151623c6098ab6dc0b200d5d44c415cdbe12f2e3be97c3172c41d270e9'),
 'app/carrier_identity_port.py': ('08e470a49aef5213f458f352df2118cf79e01160ba2b0544091161358717584d',
                                  '3690500795de424c928570593debb4afcfc50d1175f2ccba77f44fb56c97979b'),
 'app/current_user_onboarding.py': ('323c4100d3ee0d32077232be2656209daa581f9e19ca018d3e840009d03dc26e',
                                    '93a4d7b67b9d740ddbf8bfa9546f01fc0c927ee0de985f4be4e0d83adf435ebd'),
 'app/deferred_intent_capture.py': ('e129488f303e001293431e8b45a0edc10bab22a7e9cb7e96782aa6d3df260561',
                                    'f1dc02ba1711847228945fe6a01c3881aef6e71441a85cbf5873dbf72700d72e'),
 'app/direct_sync_push.py': ('21bf3ba0500879209d543f64d2c7c63d74a17a0874e3508dfdf75f8f586290c7',
                             '7556f245d8724729d70e0639f6c5fcec06cc77e0f3e8b1f871278c585cd45666'),
 'app/label_completion.py': ('25fb17bff710478898ea1b16ab08538b2be74827d4aaac48ed7631364cfd7d3d',
                             'ed37d0733e62e375fcd85f873b2459f5fd6a3e7d68ced48f36200cb5e871c0ac'),
 'app/label_recovery_schema.py': ('52bd27f8c34dc4bbd343e3d695a8c8f2deabb19789989957eb9aaa0bbdc68d90',
                                  '4bf2ad6024efd6c3ac2f5965eef3e81fa149078f4bc22ff716272a5c2359ffa6'),
 'app/label_workbench_context.py': ('95b57100a5bf2014a8dc7685455342233fb8bcd9f78e04a92332ae2fab320422',
                                    '7139f01df5558e89718c00fb65a95d83840b0c473ae747643250872eb0511f69'),
 'app/label_workbench_history.py': ('214ee81fdc36cfc51d6b59c15ee69f4f5057ebfc899eced18f7b61c0daff50f8',
                                    '30743e22b77939bed5d79b1e490c6df76ab577a242327a2d281f164c93ef1ca5'),
 'app/logistics_runtime_profile.py': ('a5d6e73f0a64233c1dacd006d3b724da0b5a88eeb818b93d90f396af0664057f',
                                      'db92068a17237e47d3025bece8d8f479fbe5eca46fde86e90666e3cac49fab72'),
 'app/package_command_draft.py': ('7f296c95fbcdfdd117eb275068fcb59e3dcc91f63112fb0e7b8fc421a2a3e151',
                                  '8144882b47008ee3e2c35cbd62bdf70637d34d89a5e1ade7620ef405cc7423ac'),
 'app/package_logistics.py': ('818a1d7fa228e125c30c23f6950b6d579c69c22fda1bd9f2d9aded9e9b4201a1',
                              '732b2449cee55b15e9c687a542814f76c5dc0a0c49337ec018194caa7960b3c1'),
 'app/package_outbox.py': ('39b09189d4afddcb4bf937a8d300573391281092b6e62ac828ef41f1f72b6437',
                           'dc36a36e1697d1d3a42c5f0b4bf147791fa6687444a80d912778c83bd049f683'),
 'app/package_outbox_processor.py': ('4589f804c3d57e56ad93066d83d8c122b3d3eec9510aee9c2152375f8c0c3f79',
                                     'bc8cbc44c0b573df1a658fc09ec980e3965196680d8c7294daa52846b334ded1'),
 'app/sealed_transfer_exchange.py': ('f0452daf434ae11710a9e5cfa825478d9ceb552fadc804be269e5e0b2d26fca2',
                                     '1202d1fecf10ed6e6d6e7662ca9c87ff4aa2553d14e4f8fa67210f770b8ab755'),
 'app/terminal_operation_lease.py': ('4e30d014f82a2dea766ef735c39a3b9c0f4b9e4c521ce76b25c9ac8cfb3927b7',
                                     'a1c5a01061c87b482f49707995f85672279efa190ebcc97bea3ff74f32f77bfa'),
  'app/ui/workflow_snapshot_adapter.py': ('f325774fb1e6c61547c140a4611f5b52958dbe1ce1e89763ce3a534e0bb37411',
                                         '661d0167c8966d510dc85233e32b4ee5dda781ff3d9b4fca20a1f0c797a5a4d2'),
 'app/ui/workflow_view_state.py': ('6959c71768296a68d88de2d46a76b80f177ab7c5e2bff0fde0cddb3fa9ada718',
                                   'ca42f4a1a106b60a39fe07ee74f78b3fdc4b3935986ecd6b23140c7f4c768a68'),
 'app/writer_session_fence.py': ('8e3a3dcacca554f255ca41a0b70a208f237595b34dc8e7aa5db5b2c6fdf6c73c',
                                 '92419d83453e49cd225e79ccc1aa96230c7ba00636b7fd114ed02736e6c074e1'),
 'app/tools/install_logistics_runtime_profile.py': ('7be760a5e49054740b495e62d48da98259b7e40eb288d1a46604cf8201994122',
                                                    '7dde5875649e9ec9291a45e47191b9c6fcf6b32d66cafe70c0444390cefbab9e'),
 'app/label_match_single_instance.py': ('3c69b65c5091c9d55bd28bebacad2a1fae5801a663cc7f762dacf1c3c68043e7',
                                        'f16db6f775d66233f96e274ea9b75260b6694800f60152e4a9edb133e9de93ee'),
 'app/current_user_onboarding.py': ('323c4100d3ee0d32077232be2656209daa581f9e19ca018d3e840009d03dc26e',
                                    '93a4d7b67b9d740ddbf8bfa9546f01fc0c927ee0de985f4be4e0d83adf435ebd')}
legacy_label_additions = {'app/label_transition.py': '27c5952d804558211c7db05ac3489e4bd0422599a259af6b287fec5863f09279'}
legacy_label_transition = all(
    (installed / relative).is_file() and (source / relative).is_file() and
    release_digest(installed / relative) == before and release_digest(source / relative) == after
    for relative, (before, after) in legacy_label_replacements.items()
) and all(
    not (installed / relative).exists() and (source / relative).is_file() and
    release_digest(source / relative) == expected
    for relative, expected in legacy_label_additions.items()
)

# c83c72c (w9lmtransit), 817d393 (its w9lmscanmsg 1733-only cut, a TEST1
# candidate) or 92abdaa (w9lmscanmsg) -> w9lmp1fix only: scanner-only input
# fixes (w9scanflow), and a refused read-only label check that stops only its
# own label plus the 30 s lease clock tolerance (w9lmp1fix) in Label_Match.py
# and the exact pinned companion modules, identical in all three installs. No writer
# or other file changes; the capture edge guard is rewritten by the ordinary
# initialize. Everything else stays checked and downgrade is not admitted.
scan_input_path = 'app/Label_Match.py'
scan_input_pairs = {('27c03a98ca225d82f85e95bc546e0b57c984d390db73532f46191727e9b1f1b5',
  '43c93e151623c6098ab6dc0b200d5d44c415cdbe12f2e3be97c3172c41d270e9'),
 ('78968d1946644a76542bb011bea5881403bcdb4f6482de1497b4f7c0921a261a',
  '43c93e151623c6098ab6dc0b200d5d44c415cdbe12f2e3be97c3172c41d270e9'),
 ('b30d80d46cddd039ca2475ddeb1be0465fe6e3b67479e7ed1c8e8f0c4451813d',
  '43c93e151623c6098ab6dc0b200d5d44c415cdbe12f2e3be97c3172c41d270e9')}
scan_input_companions = {'app/label_data_manager.py': ('95887ca72e74aa258ba07584e1df14fc099f4b16998629d4639e4f5749ecc757', '2ddd2129d6de99073f3cd51d8947a4792ea56e371205f3ea70445301317f4e56'),
 'app/carrier_identity_port.py': ('08e470a49aef5213f458f352df2118cf79e01160ba2b0544091161358717584d',
                                  '3690500795de424c928570593debb4afcfc50d1175f2ccba77f44fb56c97979b'),
 'app/deferred_intent_capture.py': ('e129488f303e001293431e8b45a0edc10bab22a7e9cb7e96782aa6d3df260561',
                                    'f1dc02ba1711847228945fe6a01c3881aef6e71441a85cbf5873dbf72700d72e'),
 'app/label_completion.py': ('aaac26de64e050b07b2225723898c8c32e52f6526039202ffd8d2daa12f162b8',
                             'ed37d0733e62e375fcd85f873b2459f5fd6a3e7d68ced48f36200cb5e871c0ac'),
 'app/label_recovery_schema.py': ('e783a8c7ecac94dd415810f2891161967ec8f13a802d74d7da466e646986d462',
                                  '4bf2ad6024efd6c3ac2f5965eef3e81fa149078f4bc22ff716272a5c2359ffa6'),
 'app/label_transition.py': ('001001427e7352c308b351263770b2649aa96898b712396d8ae3ae4530186cf9',
                             '27c5952d804558211c7db05ac3489e4bd0422599a259af6b287fec5863f09279'),
 'app/label_workbench_history.py': ('214ee81fdc36cfc51d6b59c15ee69f4f5057ebfc899eced18f7b61c0daff50f8',
                                    '30743e22b77939bed5d79b1e490c6df76ab577a242327a2d281f164c93ef1ca5'),
 'app/package_command_draft.py': ('7f296c95fbcdfdd117eb275068fcb59e3dcc91f63112fb0e7b8fc421a2a3e151',
                                  '8144882b47008ee3e2c35cbd62bdf70637d34d89a5e1ade7620ef405cc7423ac'),
 'app/package_logistics.py': ('818a1d7fa228e125c30c23f6950b6d579c69c22fda1bd9f2d9aded9e9b4201a1',
                              '732b2449cee55b15e9c687a542814f76c5dc0a0c49337ec018194caa7960b3c1'),
 'app/package_outbox.py': ('39b09189d4afddcb4bf937a8d300573391281092b6e62ac828ef41f1f72b6437',
                           'dc36a36e1697d1d3a42c5f0b4bf147791fa6687444a80d912778c83bd049f683'),
 'app/package_outbox_processor.py': ('4589f804c3d57e56ad93066d83d8c122b3d3eec9510aee9c2152375f8c0c3f79',
                                     'bc8cbc44c0b573df1a658fc09ec980e3965196680d8c7294daa52846b334ded1'),
 'app/sealed_transfer_exchange.py': ('f0452daf434ae11710a9e5cfa825478d9ceb552fadc804be269e5e0b2d26fca2',
                                     '1202d1fecf10ed6e6d6e7662ca9c87ff4aa2553d14e4f8fa67210f770b8ab755'),
 'app/terminal_operation_lease.py': ('4e30d014f82a2dea766ef735c39a3b9c0f4b9e4c521ce76b25c9ac8cfb3927b7',
                                     'a1c5a01061c87b482f49707995f85672279efa190ebcc97bea3ff74f32f77bfa'),
 'app/ui/workflow_view_state.py': ('45066b34a856ca47c3aa7f10b44f72e948a2db5827a8f6da948eeb7d6a8a5e0a',
                                   'ca42f4a1a106b60a39fe07ee74f78b3fdc4b3935986ecd6b23140c7f4c768a68')}
scan_input_transition = (
    (installed / scan_input_path).is_file() and
    (source / scan_input_path).is_file() and
    (release_digest(installed / scan_input_path),
     release_digest(source / scan_input_path)) in scan_input_pairs
) and all(
    (installed / relative).is_file() and (source / relative).is_file() and
    release_digest(installed / relative) == before and release_digest(source / relative) == after
    for relative, (before, after) in scan_input_companions.items()
)

# e88011c -> reviewed w9scanonly source: scanner completion, recovery and
# delivery deltas below are exact. Writer membership, other app/runtime files
# and installer contracts stay checked. Optional local scanner state is read
# through the existing recovery schema. Downgrade is not admitted.
nextfix_label_path = 'app/Label_Match.py'
nextfix_label_pair = ('caac070ce1bfa2218c5780fbab575d7c77f62381701f35d0b21b3f959e21d938',
 '43c93e151623c6098ab6dc0b200d5d44c415cdbe12f2e3be97c3172c41d270e9')
nextfix_companions = {'app/label_data_manager.py': ('95887ca72e74aa258ba07584e1df14fc099f4b16998629d4639e4f5749ecc757', '2ddd2129d6de99073f3cd51d8947a4792ea56e371205f3ea70445301317f4e56'),
 'app/carrier_identity_port.py': ('08e470a49aef5213f458f352df2118cf79e01160ba2b0544091161358717584d',
                                  '3690500795de424c928570593debb4afcfc50d1175f2ccba77f44fb56c97979b'),
 'app/label_completion.py': ('d0fb68fe1d531d6ac7436c8a778c7c93d6d1dbd4da0918614d95414ee5861492',
                             'ed37d0733e62e375fcd85f873b2459f5fd6a3e7d68ced48f36200cb5e871c0ac'),
 'app/label_recovery_schema.py': ('e783a8c7ecac94dd415810f2891161967ec8f13a802d74d7da466e646986d462',
                                  '4bf2ad6024efd6c3ac2f5965eef3e81fa149078f4bc22ff716272a5c2359ffa6'),
 'app/label_transition.py': ('001001427e7352c308b351263770b2649aa96898b712396d8ae3ae4530186cf9',
                             '27c5952d804558211c7db05ac3489e4bd0422599a259af6b287fec5863f09279'),
 'app/label_workbench_history.py': ('214ee81fdc36cfc51d6b59c15ee69f4f5057ebfc899eced18f7b61c0daff50f8',
                                    '30743e22b77939bed5d79b1e490c6df76ab577a242327a2d281f164c93ef1ca5'),
 'app/package_command_draft.py': ('7f296c95fbcdfdd117eb275068fcb59e3dcc91f63112fb0e7b8fc421a2a3e151',
                                  '8144882b47008ee3e2c35cbd62bdf70637d34d89a5e1ade7620ef405cc7423ac'),
 'app/package_logistics.py': ('818a1d7fa228e125c30c23f6950b6d579c69c22fda1bd9f2d9aded9e9b4201a1',
                              '732b2449cee55b15e9c687a542814f76c5dc0a0c49337ec018194caa7960b3c1'),
 'app/package_outbox.py': ('39b09189d4afddcb4bf937a8d300573391281092b6e62ac828ef41f1f72b6437',
                           'dc36a36e1697d1d3a42c5f0b4bf147791fa6687444a80d912778c83bd049f683'),
 'app/package_outbox_processor.py': ('4589f804c3d57e56ad93066d83d8c122b3d3eec9510aee9c2152375f8c0c3f79',
                                     'bc8cbc44c0b573df1a658fc09ec980e3965196680d8c7294daa52846b334ded1'),
 'app/sealed_transfer_exchange.py': ('f0452daf434ae11710a9e5cfa825478d9ceb552fadc804be269e5e0b2d26fca2',
                                     '1202d1fecf10ed6e6d6e7662ca9c87ff4aa2553d14e4f8fa67210f770b8ab755'),
 'app/ui/workflow_view_state.py': ('45066b34a856ca47c3aa7f10b44f72e948a2db5827a8f6da948eeb7d6a8a5e0a',
                                   'ca42f4a1a106b60a39fe07ee74f78b3fdc4b3935986ecd6b23140c7f4c768a68')}
nextfix_label_transition = (
    (installed / nextfix_label_path).is_file() and
    (source / nextfix_label_path).is_file() and
    (release_digest(installed / nextfix_label_path),
     release_digest(source / nextfix_label_path)) == nextfix_label_pair
) and all((installed / relative).is_file() and (source / relative).is_file() and release_digest(installed / relative) == before and release_digest(source / relative) == after for relative, (before, after) in nextfix_companions.items())

# a20cd01 -> this scanner source: exact forward-only source pair, the same
# pinned companion deltas, and unchanged writer membership/runtime/contracts.
scanner_label_pair = ('7324ab7b111d940762c4acb20cdc9348ed6f176d8d7e4f7e52bd3d9c9d50db57', '43c93e151623c6098ab6dc0b200d5d44c415cdbe12f2e3be97c3172c41d270e9')
scanner_transition = (
    (installed / nextfix_label_path).is_file() and
    (source / nextfix_label_path).is_file() and
    (release_digest(installed / nextfix_label_path), release_digest(source / nextfix_label_path)) == scanner_label_pair
) and all((installed / relative).is_file() and (source / relative).is_file() and release_digest(installed / relative) == before and release_digest(source / relative) == after for relative, (before, after) in nextfix_companions.items())

# Reviewed current-user selection update on the same accepted preimages.
# All four modules must match the exact directional LF set. Other app/runtime
# files, writer membership and contracts retain their ordinary checks.
current_user_profile_replacements = {
    'app/enrollment_mutex.py': ('040294978e4341d9f06573f9117d99913f1dab8ef85de5f08a5296228fbb9787', 'ce77c0a1ab113326dffdb611a74c19308a48fc1b113626509960614fd3295981'),
    'app/writer_session_fence.py': ('825d5337385e3870ab4d83db1b63d9dea4d1fc9e9c528d7948415ca20256ae87', '92419d83453e49cd225e79ccc1aa96230c7ba00636b7fd114ed02736e6c074e1'),
    'app/tools/install_logistics_runtime_profile.py': ('7be760a5e49054740b495e62d48da98259b7e40eb288d1a46604cf8201994122', '7dde5875649e9ec9291a45e47191b9c6fcf6b32d66cafe70c0444390cefbab9e'),
    'app/label_match_single_instance.py': ('3c69b65c5091c9d55bd28bebacad2a1fae5801a663cc7f762dacf1c3c68043e7', 'f16db6f775d66233f96e274ea9b75260b6694800f60152e4a9edb133e9de93ee'),
    'app/current_user_onboarding.py': ('323c4100d3ee0d32077232be2656209daa581f9e19ca018d3e840009d03dc26e', '93a4d7b67b9d740ddbf8bfa9546f01fc0c927ee0de985f4be4e0d83adf435ebd'),
    'app/logistics_runtime_profile.py': ('2cd3fe00a34e79a6c12a995f74ba99515f88fff4e9bef50e844ab02dc59da1ed', 'db92068a17237e47d3025bece8d8f479fbe5eca46fde86e90666e3cac49fab72'),
}
# Exact writer bytes from the same accepted profile/scan preimages; their
# inventory pins differ. No additional release direction is admitted.
current_user_profile_writer_preimages = {
    '7e26bf0c1cddd87df8fdffb5921887f8034976269308c8d5920543c30ba38ce1',
    '825d5337385e3870ab4d83db1b63d9dea4d1fc9e9c528d7948415ca20256ae87',
    '96d32f3eab331cc14ccef610a5523d938ff34372f0ba1cfd9a5a6f9d779ee3ce',
    'b38cd124a736dfd2d020b0401b409f95049ee99444795cd5fe13f309bbaf42b7',
    'da8873b44fd4fc8c69cc21b17f7e53695630c807d70b48c4ab653a26e73d1a50',
}
current_user_profile_transition = all(
    (installed / relative).is_file() and (source / relative).is_file() and
    release_digest(installed / relative) in (
        current_user_profile_writer_preimages if relative == 'app/writer_session_fence.py' else {before}
    ) and release_digest(source / relative) == after
    for relative, (before, after) in current_user_profile_replacements.items()
)

# Transport lock release changes only these exact reviewed modules. Accept
# their directional bytes only when an existing release transition already
# matches; this table adds no release, writer identity, runtime or contract.
remote_transport_replacements = {'app/direct_sync_push.py': ({'21bf3ba0500879209d543f64d2c7c63d74a17a0874e3508dfdf75f8f586290c7',
                              '86c182b049dfffb9bc2256d140b8dbaa95aea631abcdf5b4f3da244b696643bd'},
                             '7556f245d8724729d70e0639f6c5fcec06cc77e0f3e8b1f871278c585cd45666'),
 'app/label_admin_pin.py': ({'8e5fb578366c38012792f5a3213310d6d442e4ff68202edf84e4983872f13bf0'},
                            '0c2818a8667bf680a2eb3b44f7b460732f0b25f6828ea151bb49dea5e814ad89'),
 'app/package_logistics.py': ({'818a1d7fa228e125c30c23f6950b6d579c69c22fda1bd9f2d9aded9e9b4201a1'},
                              '732b2449cee55b15e9c687a542814f76c5dc0a0c49337ec018194caa7960b3c1'),
 'app/package_outbox_processor.py': ({'4589f804c3d57e56ad93066d83d8c122b3d3eec9510aee9c2152375f8c0c3f79'},
                                     'bc8cbc44c0b573df1a658fc09ec980e3965196680d8c7294daa52846b334ded1'),
 'app/phs_label_workflow.py': ({'c1746fe550017bccdf8b8fd6b98ed1438cd49b5878d98328cceec7d6cb6edd7c'},
                               '72bb834611c5172e3cb9a0c517b49e4a664adcffef05006f90015a6384584ae0'),
 'app/producer_runtime_client.py': ({'c5c111e72d150395b825ac66e79208540560495703039d0ea4c4f215eb45b2f9'},
                                    'd575094842ab4bb126f16d5372074aad03e20e4c0163f2ab0ffc5b10f77e271c'),
 'app/sealed_transfer_exchange.py': ({'f0452daf434ae11710a9e5cfa825478d9ceb552fadc804be269e5e0b2d26fca2'},
                                     '1202d1fecf10ed6e6d6e7662ca9c87ff4aa2553d14e4f8fa67210f770b8ab755'),
 'app/user_relay.py': ({'07291437ab724a9512332b25666099f13170af2f12f5f62328ea6af54cad96e7'},
                       '1015c0d1026d69745d70bbe1d69ab0f5a97cec52eaf8831ecc95d0356d8f4d3b'),
 'app/writer_session_fence.py': ({'7e26bf0c1cddd87df8fdffb5921887f8034976269308c8d5920543c30ba38ce1', '825d5337385e3870ab4d83db1b63d9dea4d1fc9e9c528d7948415ca20256ae87', '8e3a3dcacca554f255ca41a0b70a208f237595b34dc8e7aa5db5b2c6fdf6c73c', '96d32f3eab331cc14ccef610a5523d938ff34372f0ba1cfd9a5a6f9d779ee3ce', 'b38cd124a736dfd2d020b0401b409f95049ee99444795cd5fe13f309bbaf42b7', 'da8873b44fd4fc8c69cc21b17f7e53695630c807d70b48c4ab653a26e73d1a50'},
                                 'a43bf51c06caf667a0a0e98849553ca667aa8cd2640b546d357d918d95ec13bb')}
remote_transport_transition = (
    legacy_label_transition or scan_input_transition or nextfix_label_transition
    or scanner_transition or current_user_profile_transition
) and all(
    (installed / relative).is_file() and (source / relative).is_file()
    and release_digest(installed / relative) in befores
    and release_digest(source / relative) == after
    for relative, (befores, after) in remote_transport_replacements.items()
)


# sub-333 / G 10-05 05:2x: exact forward admission repair only. Every
# accepted historical preimage stays explicit; neither reversal nor unknown
# membership/file bytes are admitted by this companion declaration.
quiesced_transport_replacements = {'app/producer_runtime_client.py': ({'c5c111e72d150395b825ac66e79208540560495703039d0ea4c4f215eb45b2f9', 'e47cd333cc3c743fe9cd30a6de4069929773e86bfbb23babdeaff287564e8f17'}, 'd575094842ab4bb126f16d5372074aad03e20e4c0163f2ab0ffc5b10f77e271c'), 'app/tools/register_label_match_worker_pc.py': ({'469c2e2ebdc9ddb45973e1423336bd6619b485e1604732d4d7bec55c41f0931f'}, 'fbb7397c227cf6845e01124119cafc0081b9a0aa4d50c44703d339fc5dd8271b')}
quiesced_transport_transition = all(
    (installed / relative).is_file() and (source / relative).is_file()
    and release_digest(installed / relative) in (befores | {after})
    and release_digest(source / relative) == after
    for relative, (befores, after) in quiesced_transport_replacements.items()
)
quiesced_transport_sources = [
    ('runtime_lease_network', 'producer_runtime_client.py', '_post_lease_request', 'decorator'),
    ('worker_enrollment_network', 'tools/register_label_match_worker_pc.py', '_enroll', 'decorator'),
    ('worker_partial_recovery_network', 'tools/register_label_match_worker_pc.py', '_partial_recovery_request', 'decorator'),
    ('worker_admin_recovery_network', 'tools/register_label_match_worker_pc.py', '_admin_recover', 'decorator'),
]

roster_additions = {'app/label_worker_roster.py': '63d8fef44564fa9666b4a5b7494339e0fd91692e94eea42e8cbbcbb81fe4b6bd'}
# This feature's exact reviewed branch baseline is independent of the older
# accepted release tables; never broaden their historical preimages.
roster_baseline_replacements = {'app/Label_Match.py': ('6a516d5aa46847547f0355f3505447bcd3ae86abd521640d0691d9ac3e962062',
                        '43c93e151623c6098ab6dc0b200d5d44c415cdbe12f2e3be97c3172c41d270e9'),
 'app/label_admin_pin.py': ('159b7f0b79c6bb54d56857140d8601424e0c2ef386bd9d16e08c784a8545ebb8',
                            '9be9247c439a99c696cb472f0bee0eee85b2fe1a98ee2a25aabe6395aff7a617'),
 'app/writer_session_fence.py': ('8d7ae33010ffce1645868060c0bb049ca1f554781c37de03a678dd712e5d8dd6',
                                 '92419d83453e49cd225e79ccc1aa96230c7ba00636b7fd114ed02736e6c074e1'),
 'tools/label_writer_fence.ps1': ('2795ccf12f76c7619fda186762303e02539714b7c33fbef49b65e89a1d06d67a',
                                  '44ad315d0ffe526f003e3d8d17ff0940338652e7e557ad22ed6e728551ce3d3f')}
# Exact additional runtime changes for the already accepted roster baseline.
roster_baseline_companions = {'app/label_completion.py': ('7650944f6d9f7431d8089b373a32aa649c324ecacfb8c24b40f13ccc2f2fdb55',
                             'ed37d0733e62e375fcd85f873b2459f5fd6a3e7d68ced48f36200cb5e871c0ac'),
 'app/label_data_manager.py': ('eb4f4bef03152c05c6ef160abb7738dfa8416c971bb79baac4092d52905c14ec',
                               '2ddd2129d6de99073f3cd51d8947a4792ea56e371205f3ea70445301317f4e56'),
 'app/label_recovery_schema.py': ('01fdfd2616bdc6a2a713ed89f32f23b73db9fe2d2e86d90482023e9950a178f7',
                                  '4bf2ad6024efd6c3ac2f5965eef3e81fa149078f4bc22ff716272a5c2359ffa6'),
 'app/package_command_draft.py': ('4936fdccd3af20e7f48c0fc0bdb3eb41e6baee073bf8dea90252adb487c60563',
                                  '8144882b47008ee3e2c35cbd62bdf70637d34d89a5e1ade7620ef405cc7423ac'),
 'app/package_outbox_processor.py': ('b7dc5263d0d41715b82c3e8c54e95951007eedbefcb9a4a90900aa3a7407a0e3',
                                     'bc8cbc44c0b573df1a658fc09ec980e3965196680d8c7294daa52846b334ded1'),
 'app/sealed_transfer_exchange.py': ('74c5d2c126537b316b2f9d8d5127853c1e4217448fe3a2656f551eaafe529d1c',
                                     '1202d1fecf10ed6e6d6e7662ca9c87ff4aa2553d14e4f8fa67210f770b8ab755'),
 'app/label_match_single_instance.py': ('64040b936adb201d21ecbf55d47536b545a8c10048200a4337635f0a7b00bb02',
                                        'f16db6f775d66233f96e274ea9b75260b6694800f60152e4a9edb133e9de93ee')}
roster_baseline_transition = all(
    (installed / relative).is_file() and (source / relative).is_file()
    and release_digest(installed / relative) == before
    and release_digest(source / relative) == after
    for relative, (before, after) in {**roster_baseline_replacements, **roster_baseline_companions}.items()
)
roster_transport_path = 'app/label_admin_pin.py'
roster_transport_befores, roster_transport_after = remote_transport_replacements[roster_transport_path]
roster_transport_befores = roster_transport_befores | {roster_baseline_replacements[roster_transport_path][0]}
roster_transport_pair = (installed / roster_transport_path).is_file() and (source / roster_transport_path).is_file()
roster_transport_forward = roster_transport_pair and (
    release_digest(installed / roster_transport_path) in roster_transport_befores
    and release_digest(source / roster_transport_path) == roster_transport_after
)
roster_transport_reverse = roster_transport_pair and (
    release_digest(installed / roster_transport_path) == roster_transport_after
    and release_digest(source / roster_transport_path) in roster_transport_befores
)
if roster_transport_reverse:
    # Refuse a downgrade before the new leaf's file-set difference obscures
    # the original directional Label_Match rejection.
    reverse_additions = set(files(source, 'app')) ^ set(files(installed, 'app'))
    if reverse_additions - set(roster_additions) or files(source, 'runtime').keys() != files(installed, 'runtime').keys():
        raise ValueError('WRITER_TRANSITION_SOURCE_SET_DIFFERS')
    reverse_path = 'app/Label_Match.py'
    if not all((root / reverse_path).is_file() for root in (source, installed)) or release_digest(source / reverse_path) == release_digest(installed / reverse_path):
        reverse_path = roster_transport_path
    raise ValueError('WRITER_TRANSITION_SEMANTICS_DIFFER: ' + reverse_path)
roster_transition = roster_transport_forward and all(
    not (installed / relative).exists() and (source / relative).is_file()
    and release_digest(source / relative) == expected
    for relative, expected in roster_additions.items()
)
if roster_transport_forward and not roster_transition:
    raise ValueError('WRITER_TRANSITION_SOURCE_SET_DIFFERS')



























# Every changed byte and added asset below is an exact committed release pair.
# Public trust rotation additionally requires this precise old/new configuration.
from typing import Any, Mapping
from urllib.parse import urlparse, parse_qsl
UPDATE_GITHUB_UPDATE_HOSTS = {"github.com", "githubusercontent.com", "github.io"}
UPDATE_SECRET_QUERY_KEYS = {
    "access_token",
    "api_key",
    "client_secret",
    "github_token",
    "pat",
    "private_key",
    "sig",
    "signature",
    "token",
}
UPDATE_SECRET_QUERY_PREFIXES = ("x_amz_", "x_goog_")
def _assert_https_update_url(url, *, require_zip=False):
    parsed = urlparse(str(url or ""))
    if parsed.scheme.lower() != "https" or not parsed.netloc:
        raise ValueError("Update URL must be HTTPS")
    if not parsed.hostname or not parsed.hostname.isascii():
        raise ValueError("업데이트 주소의 호스트는 ASCII IP 또는 도메인이어야 합니다.")
    parsed.port  # Reject a malformed port before requesting a URL.
    if parsed.username is not None or parsed.password is not None:
        raise ValueError("Update URL must not include userinfo")
    if parsed.fragment:
        raise ValueError("Update URL must not include fragments")
    if require_zip and not parsed.path.lower().endswith(".zip"):
        raise ValueError("Update artifact URL must point to a .zip file")
    for key, _value in parse_qsl(parsed.query, keep_blank_values=True):
        normalized_key = key.lower().replace("-", "_")
        if normalized_key in UPDATE_SECRET_QUERY_KEYS or normalized_key.startswith(UPDATE_SECRET_QUERY_PREFIXES):
            raise ValueError("Update URL must not contain raw token query parameters")
def _is_github_hosted_update_url(url):
    parsed = urlparse(str(url or ""))
    host = (parsed.hostname or "").lower().rstrip(".")
    return any(host == root or host.endswith("." + root) for root in UPDATE_GITHUB_UPDATE_HOSTS)
assert_https_update_url = _assert_https_update_url
def policy_from_settings(settings: Mapping[str, Any]) -> dict[str, Any]:

    if not isinstance(settings, Mapping):
        raise ValueError("auto-update settings must be an object")
    allowed = {"feed_url", "channel", "apply_during_work", "profile_auth"}
    if set(settings) - allowed:
        raise ValueError("auto-update settings contain unsupported fields")
    feed = settings.get("feed_url", "")
    if not isinstance(feed, str):
        raise ValueError("auto-update feed must be a string")
    if feed:
        assert_https_update_url(feed)
        if _is_github_hosted_update_url(feed):
            raise ValueError("daily update feed cannot use GitHub hosting")
    if settings.get("apply_during_work", False) is not False:
        raise ValueError("applying during work requires separate scanner acceptance")
    channel = settings.get("channel", "stable")
    if not isinstance(channel, str) or re.fullmatch(r"[a-z0-9_-]{1,32}", channel) is None:
        raise ValueError("auto-update channel is malformed")
    auth = settings.get("profile_auth", bool(feed))
    if feed and auth is not True:
        raise ValueError("company feed requires existing device authentication")
    if type(auth) is not bool:
        raise ValueError("profile authentication setting must be a boolean")
    return {"feed_url": feed, "channel": channel, "apply_during_work": False, "profile_auth": auth}
def canonical_startup_settings(settings: Mapping[str, Any]) -> bytes:
    return (json.dumps(policy_from_settings(settings), sort_keys=True, indent=2) + "\n").encode("utf-8")

startup_release_replacements={'3f4574a176f45253d746b397ad979896f266d062': {'INSTALL_THIS_PC.ps1': ('8efe37d3203c94c71dff93a0761a674f400154b7b2d5c7bef8a91e2d09e5b702', 'bdc42c26a8b5464d8a65e880ba5474ef01a28c820997adcb5ddefa8daa9b022b'), 'app/Label_Match.py': ('0481e687828da9e99aebad238307586c73f9cd0db8ea53e39603029fbcad921b', '43c93e151623c6098ab6dc0b200d5d44c415cdbe12f2e3be97c3172c41d270e9'), 'app/carrier_identity_port.py': ('08e470a49aef5213f458f352df2118cf79e01160ba2b0544091161358717584d', '3690500795de424c928570593debb4afcfc50d1175f2ccba77f44fb56c97979b'), 'app/config/app_settings.json': ('081e0ab6e932035312c9bc225db26589e4c694cd2c40170ca459443e1a5460e8', 'e65540f3325c70866753cd0abfe01c21edb89f6fa05cd1ddc1666a89af14c34b'), 'app/current_user_onboarding.py': ('323c4100d3ee0d32077232be2656209daa581f9e19ca018d3e840009d03dc26e', '93a4d7b67b9d740ddbf8bfa9546f01fc0c927ee0de985f4be4e0d83adf435ebd'), 'app/deferred_intent_capture.py': ('e129488f303e001293431e8b45a0edc10bab22a7e9cb7e96782aa6d3df260561', 'f1dc02ba1711847228945fe6a01c3881aef6e71441a85cbf5873dbf72700d72e'), 'app/direct_sync_push.py': ('21bf3ba0500879209d543f64d2c7c63d74a17a0874e3508dfdf75f8f586290c7', '7556f245d8724729d70e0639f6c5fcec06cc77e0f3e8b1f871278c585cd45666'), 'app/enrollment_mutex.py': ('040294978e4341d9f06573f9117d99913f1dab8ef85de5f08a5296228fbb9787', 'ce77c0a1ab113326dffdb611a74c19308a48fc1b113626509960614fd3295981'), 'app/label_completion.py': ('25fb17bff710478898ea1b16ab08538b2be74827d4aaac48ed7631364cfd7d3d', 'ed37d0733e62e375fcd85f873b2459f5fd6a3e7d68ced48f36200cb5e871c0ac'), 'app/label_data_manager.py': ('95887ca72e74aa258ba07584e1df14fc099f4b16998629d4639e4f5749ecc757', '2ddd2129d6de99073f3cd51d8947a4792ea56e371205f3ea70445301317f4e56'), 'app/label_match_product_host.py': ('bce7176d636a93c559340850d91894ffd02a4eef2a3f953d2ae05cd1cb0f9fbf', '2f68dda99e3bc9e0d6e1c325cb43d169845df3501050a354c624a44690097845'), 'app/label_match_single_instance.py': ('3c69b65c5091c9d55bd28bebacad2a1fae5801a663cc7f762dacf1c3c68043e7', 'f16db6f775d66233f96e274ea9b75260b6694800f60152e4a9edb133e9de93ee'), 'app/label_recovery_schema.py': ('52bd27f8c34dc4bbd343e3d695a8c8f2deabb19789989957eb9aaa0bbdc68d90', '4bf2ad6024efd6c3ac2f5965eef3e81fa149078f4bc22ff716272a5c2359ffa6'), 'app/label_workbench_context.py': ('95b57100a5bf2014a8dc7685455342233fb8bcd9f78e04a92332ae2fab320422', '7139f01df5558e89718c00fb65a95d83840b0c473ae747643250872eb0511f69'), 'app/label_workbench_history.py': ('214ee81fdc36cfc51d6b59c15ee69f4f5057ebfc899eced18f7b61c0daff50f8', '30743e22b77939bed5d79b1e490c6df76ab577a242327a2d281f164c93ef1ca5'), 'app/logistics_runtime_profile.py': ('a5d6e73f0a64233c1dacd006d3b724da0b5a88eeb818b93d90f396af0664057f', 'db92068a17237e47d3025bece8d8f479fbe5eca46fde86e90666e3cac49fab72'), 'app/main.py': ('520c2eaea01876f324288f01fee6f7600dd45d27829fb0e374969f6e2bf2bb6c', 'f85e46a1779eba99ac26609a172e8e4fad55e4e4115ee314bc5e1e66f4cf75ab'), 'app/package_command_draft.py': ('7f296c95fbcdfdd117eb275068fcb59e3dcc91f63112fb0e7b8fc421a2a3e151', '8144882b47008ee3e2c35cbd62bdf70637d34d89a5e1ade7620ef405cc7423ac'), 'app/package_logistics.py': ('818a1d7fa228e125c30c23f6950b6d579c69c22fda1bd9f2d9aded9e9b4201a1', '732b2449cee55b15e9c687a542814f76c5dc0a0c49337ec018194caa7960b3c1'), 'app/package_outbox.py': ('39b09189d4afddcb4bf937a8d300573391281092b6e62ac828ef41f1f72b6437', 'dc36a36e1697d1d3a42c5f0b4bf147791fa6687444a80d912778c83bd049f683'), 'app/package_outbox_processor.py': ('4589f804c3d57e56ad93066d83d8c122b3d3eec9510aee9c2152375f8c0c3f79', 'bc8cbc44c0b573df1a658fc09ec980e3965196680d8c7294daa52846b334ded1'), 'app/sealed_transfer_exchange.py': ('f0452daf434ae11710a9e5cfa825478d9ceb552fadc804be269e5e0b2d26fca2', '1202d1fecf10ed6e6d6e7662ca9c87ff4aa2553d14e4f8fa67210f770b8ab755'), 'app/terminal_operation_lease.py': ('4e30d014f82a2dea766ef735c39a3b9c0f4b9e4c521ce76b25c9ac8cfb3927b7', 'a1c5a01061c87b482f49707995f85672279efa190ebcc97bea3ff74f32f77bfa'), 'app/tools/install_logistics_runtime_profile.py': ('7be760a5e49054740b495e62d48da98259b7e40eb288d1a46604cf8201994122', '7dde5875649e9ec9291a45e47191b9c6fcf6b32d66cafe70c0444390cefbab9e'), 'app/ui/workflow_snapshot_adapter.py': ('f325774fb1e6c61547c140a4611f5b52958dbe1ce1e89763ce3a534e0bb37411', '661d0167c8966d510dc85233e32b4ee5dda781ff3d9b4fca20a1f0c797a5a4d2'), 'app/ui/workflow_view_state.py': ('6959c71768296a68d88de2d46a76b80f177ab7c5e2bff0fde0cddb3fa9ada718', 'ca42f4a1a106b60a39fe07ee74f78b3fdc4b3935986ecd6b23140c7f4c768a68'), 'app/user_relay.py': ('07291437ab724a9512332b25666099f13170af2f12f5f62328ea6af54cad96e7', '1015c0d1026d69745d70bbe1d69ab0f5a97cec52eaf8831ecc95d0356d8f4d3b'), 'app/writer_session_fence.py': ('8e3a3dcacca554f255ca41a0b70a208f237595b34dc8e7aa5db5b2c6fdf6c73c', '92419d83453e49cd225e79ccc1aa96230c7ba00636b7fd114ed02736e6c074e1'), 'tools/label_writer_fence.ps1': ('25f0f9f7b70d27e1b4e7f939720f694ae954670b94dc52c0a9c82d9f310a6794', '44ad315d0ffe526f003e3d8d17ff0940338652e7e557ad22ed6e728551ce3d3f'), 'tools/label_writer_fence_contract.json': ('bc95bd4ae6984f08120d7284f1ef73e7f1369e8fae078f6d60c66144a5cb24de', '8a07c41b748669e237fc7f35be75daede0fca9f86c2d1af07ff7a701a33126dd'), 'app/current_user_scheduled_task.py': ('5b339cae5a61c67bf3187f75ae254c38df4414bf7c0f5f3b8ed24f3cff52e870', 'ad9e95381b52e57cfdecd84373b61ae5449ac8edeb9fef33199432896d0b1417'), 'app/kmtech_shared.lock.json': ('87cf641db77b8052ffbaeee86d0c2cbab84f07ce4c3748c796985be2eacc0cf9', '14ff6bbc353aab8fa9ce88297c79f2ca5504c554e0edb9671eb62d1766395e9f'), 'app/kmtech_shared.manifest.json': ('feaed459688e915eb5f52f264501a4287261d9dcbdbc50e0c6f4bde59d8e7117', 'de1aa3a5a2b072ec15ec2b3d60733881dcf4d5c87f14b15ed06691ba643ac918'), 'app/label_admin_pin.py': ('8e5fb578366c38012792f5a3213310d6d442e4ff68202edf84e4983872f13bf0', '9be9247c439a99c696cb472f0bee0eee85b2fe1a98ee2a25aabe6395aff7a617'), 'app/phs_label_workflow.py': ('c1746fe550017bccdf8b8fd6b98ed1438cd49b5878d98328cceec7d6cb6edd7c', '72bb834611c5172e3cb9a0c517b49e4a664adcffef05006f90015a6384584ae0'), 'app/producer_runtime_client.py': ('c5c111e72d150395b825ac66e79208540560495703039d0ea4c4f215eb45b2f9', 'd575094842ab4bb126f16d5372074aad03e20e4c0163f2ab0ffc5b10f77e271c'), 'tools/bootstrap_integrity.ps1': ('7cf77e9cc833093aed9ed7b3d79565f5396eed0288b1e7729e08959e82e12ff3', '96101ce38befdf9dcca09ec42abd571aa441ad312f35bf3d14fd7effc97db963'), 'app/kmtech_shared/__init__.py': ('32f34fccf9a00e5d27e852a95fc7b776f18a5eccda991f47dc33cd8111d50295', '7714a7a02cda3da34c4972101c0e01cc052435a00d8d520b5c274a05aa9850e8'), 'app/tools/direct_sync_relay_runner.py': ('c797ffeee38f5161621e526c4ef2f3844cbe848ea0124f875f3df05a1779af98', '9b8e7a9845c4e73edea64b4ac3d0b90e3f461b7d926e0e1db64bdb9197f734ac'), 'app/tools/register_label_match_worker_pc.py': ('469c2e2ebdc9ddb45973e1423336bd6619b485e1604732d4d7bec55c41f0931f', 'fbb7397c227cf6845e01124119cafc0081b9a0aa4d50c44703d339fc5dd8271b'), 'app/tools/direct_sync_relay_install_pack.py': ('5a4368f2bb692dd37f406f449e607390024668a8101929577d88c549ad7093e5', '53cc1d41e90b0b9cda364ab744ce81abeaf720d222a7fe6263bd15728ec77d7f'), 'app/direct_sync_runtime.py': ('50ff189da613b1e1bec5bbb32061acc76e8d29ecae8808f5f591854a361be033', '0a4d8ddb463a8f5d52cbe60cfbfaca2aa22c06fa1c89831944cc5c54df854d04'), 'app/item_catalog_sync.py': ('41c33bf6a473cc147db6e522b1d20d4771cc9bd92c74fa3a8bd091a141907387', '7ba452c285d2f41fea202dedd68fedbe15d514a8d632bf6818f27ac54851c3d4'), 'app/label_guarded_runtime_reconcile.py': ('5f2d668622236950e307e5dc964303f78c7074d3f4bf2c2a37018c2fffe46016', 'dccab991a4caa38b7cefc59aa92357393219131232f3252c50d23dc7240a8a1e'), 'app/protected_admin.py': ('f4b1616dc152c84e368a32acb06b424b4b4cdab588635e383fa22e7ee6d3c58e', 'eadfb46474595075b840bbdf87b919910c228ac5481fe62721a3497c478d63f9'), 'app/registration_relay_recovery.py': ('25921c0e33df76ef1c52655d95f2482906d444409d5c4a4d602e3745ff8d721b', '013758e5ab7f9084eae87651fd088a667e240841384deae3140858d28998f144'), 'app/tools/direct_sync_relay_operator.py': ('ff97593c29669ad1f983d6f7be0fb1ebabf27adcf9661b797d36f7d583a0cedb', '3c990ebfd9ff356d5876b40d1098470d0602fae3a3ed838ccf0cbc644150032f'), 'app/tools/label_auth_recovery_canary.py': ('440a0ed5061b522cafa56408092e3ed361b42f1ae09569258b08005a04cd463b', '079dfe927bcf721f4db49779eeab96b66093e1e02c69b86da73ea069d5b7c9be'), 'app/tools/label_exact_clone_resolution_receipt.py': ('c77e6be851f32cad9058ee52892a9b2520232a15914379aba3c28ad09c581a9b', 'd1e0a8955eed3f056cb964d4f0058f082664740b03b98eefc8f5ea872ded987c'), 'app/tools/label_legacy_task_quiescence.py': ('02a422b3d5b2662a061ea79d88fd6d8da409d6daba92ffe22266e796da20dfe4', 'c9db34c66056e3e63eca603e3c1997f12367b58b6e748941b8d80a5fb7981b7e'), 'app/label_exact_clone_resolution.py': ('0ff69b167d2b88e9be8d5a6bc068b6f160b4a19023a92b401881fc86cf7b24e4', 'f162f2c2680852cfa1ec52d4a355b275fcb87482561bb84aac89aa24e05696ce'), 'app/label_workbench_activity.py': ('42ddafdfe2a45ab60f9eb0ffca4f73d4df2efbbc15f9eca1c12628f0e22c8706', 'b94ea10dbbfc75debb375386af285e43f87231ff4a14c42f1da845f6833fa152'), 'app/label_workbench_scan.py': ('5705832318e7954554b4bd94de77b1168e63622580d7bcc49fa5b4eb534ff271', '0d7201fabded66e0bbd620dc6fd7d98f2d8b747e0c7e17a4f8b224d66d049d52')}, '6479eca6c2c03f17d5f0fcf8b662a83045f96039': {'INSTALL_THIS_PC.ps1': ('8efe37d3203c94c71dff93a0761a674f400154b7b2d5c7bef8a91e2d09e5b702', 'bdc42c26a8b5464d8a65e880ba5474ef01a28c820997adcb5ddefa8daa9b022b'), 'app/Label_Match.py': ('b18a13700497a1064226271f2419b51acf34c008b3f82e69340ec2b64f8846f6', '43c93e151623c6098ab6dc0b200d5d44c415cdbe12f2e3be97c3172c41d270e9'), 'app/config/app_settings.json': ('081e0ab6e932035312c9bc225db26589e4c694cd2c40170ca459443e1a5460e8', 'e65540f3325c70866753cd0abfe01c21edb89f6fa05cd1ddc1666a89af14c34b'), 'app/current_user_onboarding.py': ('84fde787fe1bca5d3138c3cd243d9b13ffaf834ffc69a5414b39d0c2b81f905d', '93a4d7b67b9d740ddbf8bfa9546f01fc0c927ee0de985f4be4e0d83adf435ebd'), 'app/label_match_product_host.py': ('bce7176d636a93c559340850d91894ffd02a4eef2a3f953d2ae05cd1cb0f9fbf', '2f68dda99e3bc9e0d6e1c325cb43d169845df3501050a354c624a44690097845'), 'app/label_transition.py': ('a5927e064c0468257c54e408e3164c67512f225cb78dceb49c231ffd8df83c8b', '27c5952d804558211c7db05ac3489e4bd0422599a259af6b287fec5863f09279'), 'app/main.py': ('520c2eaea01876f324288f01fee6f7600dd45d27829fb0e374969f6e2bf2bb6c', 'f85e46a1779eba99ac26609a172e8e4fad55e4e4115ee314bc5e1e66f4cf75ab'), 'app/package_logistics.py': ('85a71ef78d268fae7f7aad9e21ae68920a6b23d5437e598a5822512fa45a6d89', '732b2449cee55b15e9c687a542814f76c5dc0a0c49337ec018194caa7960b3c1'), 'app/ui/workflow_view_state.py': ('39bfffcbd6819406ded9c62f8c298d53b5148c6448cd1143b8343363ca0d5136', 'ca42f4a1a106b60a39fe07ee74f78b3fdc4b3935986ecd6b23140c7f4c768a68'), 'app/user_relay.py': ('07291437ab724a9512332b25666099f13170af2f12f5f62328ea6af54cad96e7', '1015c0d1026d69745d70bbe1d69ab0f5a97cec52eaf8831ecc95d0356d8f4d3b'), 'app/writer_session_fence.py': ('2c78bd842bfcb8fe74793b84ea36cb06c2af3664faf2872ad0cbb888bb2361be', '92419d83453e49cd225e79ccc1aa96230c7ba00636b7fd114ed02736e6c074e1'), 'tools/label_writer_fence.ps1': ('3f94563728cd665a55c134eb34c5b5de0be4fcb1c0a31ce5525d74017f71a959', '44ad315d0ffe526f003e3d8d17ff0940338652e7e557ad22ed6e728551ce3d3f'), 'tools/label_writer_fence_contract.json': ('bc95bd4ae6984f08120d7284f1ef73e7f1369e8fae078f6d60c66144a5cb24de', '8a07c41b748669e237fc7f35be75daede0fca9f86c2d1af07ff7a701a33126dd'), 'app/current_user_scheduled_task.py': ('5b339cae5a61c67bf3187f75ae254c38df4414bf7c0f5f3b8ed24f3cff52e870', 'ad9e95381b52e57cfdecd84373b61ae5449ac8edeb9fef33199432896d0b1417'), 'app/direct_sync_push.py': ('86c182b049dfffb9bc2256d140b8dbaa95aea631abcdf5b4f3da244b696643bd', '7556f245d8724729d70e0639f6c5fcec06cc77e0f3e8b1f871278c585cd45666'), 'app/kmtech_shared.lock.json': ('87cf641db77b8052ffbaeee86d0c2cbab84f07ce4c3748c796985be2eacc0cf9', '14ff6bbc353aab8fa9ce88297c79f2ca5504c554e0edb9671eb62d1766395e9f'), 'app/kmtech_shared.manifest.json': ('feaed459688e915eb5f52f264501a4287261d9dcbdbc50e0c6f4bde59d8e7117', 'de1aa3a5a2b072ec15ec2b3d60733881dcf4d5c87f14b15ed06691ba643ac918'), 'app/label_admin_pin.py': ('8e5fb578366c38012792f5a3213310d6d442e4ff68202edf84e4983872f13bf0', '9be9247c439a99c696cb472f0bee0eee85b2fe1a98ee2a25aabe6395aff7a617'), 'app/label_completion.py': ('7650944f6d9f7431d8089b373a32aa649c324ecacfb8c24b40f13ccc2f2fdb55', 'ed37d0733e62e375fcd85f873b2459f5fd6a3e7d68ced48f36200cb5e871c0ac'), 'app/label_data_manager.py': ('eb4f4bef03152c05c6ef160abb7738dfa8416c971bb79baac4092d52905c14ec', '2ddd2129d6de99073f3cd51d8947a4792ea56e371205f3ea70445301317f4e56'), 'app/label_match_single_instance.py': ('64040b936adb201d21ecbf55d47536b545a8c10048200a4337635f0a7b00bb02', 'f16db6f775d66233f96e274ea9b75260b6694800f60152e4a9edb133e9de93ee'), 'app/label_recovery_schema.py': ('01fdfd2616bdc6a2a713ed89f32f23b73db9fe2d2e86d90482023e9950a178f7', '4bf2ad6024efd6c3ac2f5965eef3e81fa149078f4bc22ff716272a5c2359ffa6'), 'app/label_workbench_history.py': ('409aa2f5ae88e571aa599d30dc7766e6563829e5bfe29ba2fd0d0989aeabd524', '30743e22b77939bed5d79b1e490c6df76ab577a242327a2d281f164c93ef1ca5'), 'app/package_command_draft.py': ('4936fdccd3af20e7f48c0fc0bdb3eb41e6baee073bf8dea90252adb487c60563', '8144882b47008ee3e2c35cbd62bdf70637d34d89a5e1ade7620ef405cc7423ac'), 'app/package_outbox.py': ('00e9dbd0272b281dae2b38363a9683ff2de14f1abffed00c0b414a0fe5b7f786', 'dc36a36e1697d1d3a42c5f0b4bf147791fa6687444a80d912778c83bd049f683'), 'app/package_outbox_processor.py': ('bc57706c8edde0f5d2dd8e3d54a010979fc177e819d9aeb57662b52e3610075d', 'bc8cbc44c0b573df1a658fc09ec980e3965196680d8c7294daa52846b334ded1'), 'app/phs_label_workflow.py': ('c1746fe550017bccdf8b8fd6b98ed1438cd49b5878d98328cceec7d6cb6edd7c', '72bb834611c5172e3cb9a0c517b49e4a664adcffef05006f90015a6384584ae0'), 'app/producer_runtime_client.py': ('c5c111e72d150395b825ac66e79208540560495703039d0ea4c4f215eb45b2f9', 'd575094842ab4bb126f16d5372074aad03e20e4c0163f2ab0ffc5b10f77e271c'), 'app/sealed_transfer_exchange.py': ('a84adc6dc1b2064d0e37b3e49090dd06348603368de7babf83351a99e3f47e1f', '1202d1fecf10ed6e6d6e7662ca9c87ff4aa2553d14e4f8fa67210f770b8ab755'), 'tools/bootstrap_integrity.ps1': ('7cf77e9cc833093aed9ed7b3d79565f5396eed0288b1e7729e08959e82e12ff3', '96101ce38befdf9dcca09ec42abd571aa441ad312f35bf3d14fd7effc97db963'), 'app/kmtech_shared/__init__.py': ('32f34fccf9a00e5d27e852a95fc7b776f18a5eccda991f47dc33cd8111d50295', '7714a7a02cda3da34c4972101c0e01cc052435a00d8d520b5c274a05aa9850e8'), 'app/tools/direct_sync_relay_runner.py': ('c797ffeee38f5161621e526c4ef2f3844cbe848ea0124f875f3df05a1779af98', '9b8e7a9845c4e73edea64b4ac3d0b90e3f461b7d926e0e1db64bdb9197f734ac'), 'app/tools/register_label_match_worker_pc.py': ('469c2e2ebdc9ddb45973e1423336bd6619b485e1604732d4d7bec55c41f0931f', 'fbb7397c227cf6845e01124119cafc0081b9a0aa4d50c44703d339fc5dd8271b'), 'app/tools/direct_sync_relay_install_pack.py': ('5a4368f2bb692dd37f406f449e607390024668a8101929577d88c549ad7093e5', '53cc1d41e90b0b9cda364ab744ce81abeaf720d222a7fe6263bd15728ec77d7f'), 'app/direct_sync_runtime.py': ('50ff189da613b1e1bec5bbb32061acc76e8d29ecae8808f5f591854a361be033', '0a4d8ddb463a8f5d52cbe60cfbfaca2aa22c06fa1c89831944cc5c54df854d04'), 'app/item_catalog_sync.py': ('41c33bf6a473cc147db6e522b1d20d4771cc9bd92c74fa3a8bd091a141907387', '7ba452c285d2f41fea202dedd68fedbe15d514a8d632bf6818f27ac54851c3d4'), 'app/label_guarded_runtime_reconcile.py': ('5f2d668622236950e307e5dc964303f78c7074d3f4bf2c2a37018c2fffe46016', 'dccab991a4caa38b7cefc59aa92357393219131232f3252c50d23dc7240a8a1e'), 'app/logistics_runtime_profile.py': ('1d786cb74812b1b4588d122b333cc9b8724d28e7410f91c7c0f025d9fd1bd5ee', 'db92068a17237e47d3025bece8d8f479fbe5eca46fde86e90666e3cac49fab72'), 'app/protected_admin.py': ('f4b1616dc152c84e368a32acb06b424b4b4cdab588635e383fa22e7ee6d3c58e', 'eadfb46474595075b840bbdf87b919910c228ac5481fe62721a3497c478d63f9'), 'app/registration_relay_recovery.py': ('25921c0e33df76ef1c52655d95f2482906d444409d5c4a4d602e3745ff8d721b', '013758e5ab7f9084eae87651fd088a667e240841384deae3140858d28998f144'), 'app/tools/direct_sync_relay_operator.py': ('ff97593c29669ad1f983d6f7be0fb1ebabf27adcf9661b797d36f7d583a0cedb', '3c990ebfd9ff356d5876b40d1098470d0602fae3a3ed838ccf0cbc644150032f'), 'app/tools/install_logistics_runtime_profile.py': ('eb8650ace3df179a7c68f3247efe5eac86200543a43f4b2c80a47ccc95b336ef', '7dde5875649e9ec9291a45e47191b9c6fcf6b32d66cafe70c0444390cefbab9e'), 'app/tools/label_auth_recovery_canary.py': ('440a0ed5061b522cafa56408092e3ed361b42f1ae09569258b08005a04cd463b', '079dfe927bcf721f4db49779eeab96b66093e1e02c69b86da73ea069d5b7c9be'), 'app/tools/label_exact_clone_resolution_receipt.py': ('c77e6be851f32cad9058ee52892a9b2520232a15914379aba3c28ad09c581a9b', 'd1e0a8955eed3f056cb964d4f0058f082664740b03b98eefc8f5ea872ded987c'), 'app/tools/label_legacy_task_quiescence.py': ('02a422b3d5b2662a061ea79d88fd6d8da409d6daba92ffe22266e796da20dfe4', 'c9db34c66056e3e63eca603e3c1997f12367b58b6e748941b8d80a5fb7981b7e'), 'app/deferred_intent_capture.py': ('970c6565a3f5f589f15d9cc2fb0ff1e5980e772d977976152b93f3c7e6e41dbd', 'f1dc02ba1711847228945fe6a01c3881aef6e71441a85cbf5873dbf72700d72e'), 'app/label_exact_clone_resolution.py': ('0ff69b167d2b88e9be8d5a6bc068b6f160b4a19023a92b401881fc86cf7b24e4', 'f162f2c2680852cfa1ec52d4a355b275fcb87482561bb84aac89aa24e05696ce'), 'app/label_workbench_activity.py': ('42ddafdfe2a45ab60f9eb0ffca4f73d4df2efbbc15f9eca1c12628f0e22c8706', 'b94ea10dbbfc75debb375386af285e43f87231ff4a14c42f1da845f6833fa152'), 'app/label_workbench_context.py': ('32835bfbd588afd84fae494aabeab2add65f2ecb0b22aa0bb2478951107afe51', '7139f01df5558e89718c00fb65a95d83840b0c473ae747643250872eb0511f69'), 'app/label_workbench_scan.py': ('5705832318e7954554b4bd94de77b1168e63622580d7bcc49fa5b4eb534ff271', '0d7201fabded66e0bbd620dc6fd7d98f2d8b747e0c7e17a4f8b224d66d049d52')}, '731ca29b14a5a9f98ec4f4935382548891557d95': {'INSTALL_THIS_PC.ps1': ('8efe37d3203c94c71dff93a0761a674f400154b7b2d5c7bef8a91e2d09e5b702', 'bdc42c26a8b5464d8a65e880ba5474ef01a28c820997adcb5ddefa8daa9b022b'), 'app/Label_Match.py': ('cbb55194b55969017b22851b9645ed92d676071122f2fbac763161f05729fc45', '43c93e151623c6098ab6dc0b200d5d44c415cdbe12f2e3be97c3172c41d270e9'), 'app/config/app_settings.json': ('081e0ab6e932035312c9bc225db26589e4c694cd2c40170ca459443e1a5460e8', 'e65540f3325c70866753cd0abfe01c21edb89f6fa05cd1ddc1666a89af14c34b'), 'app/current_user_onboarding.py': ('84fde787fe1bca5d3138c3cd243d9b13ffaf834ffc69a5414b39d0c2b81f905d', '93a4d7b67b9d740ddbf8bfa9546f01fc0c927ee0de985f4be4e0d83adf435ebd'), 'app/label_match_product_host.py': ('bce7176d636a93c559340850d91894ffd02a4eef2a3f953d2ae05cd1cb0f9fbf', '2f68dda99e3bc9e0d6e1c325cb43d169845df3501050a354c624a44690097845'), 'app/main.py': ('520c2eaea01876f324288f01fee6f7600dd45d27829fb0e374969f6e2bf2bb6c', 'f85e46a1779eba99ac26609a172e8e4fad55e4e4115ee314bc5e1e66f4cf75ab'), 'app/user_relay.py': ('07291437ab724a9512332b25666099f13170af2f12f5f62328ea6af54cad96e7', '1015c0d1026d69745d70bbe1d69ab0f5a97cec52eaf8831ecc95d0356d8f4d3b'), 'app/writer_session_fence.py': ('1d55c96bff15a0ddba47fe65a39b033c67dd30c2ea95f19f41bd4fb4b0ecc253', '92419d83453e49cd225e79ccc1aa96230c7ba00636b7fd114ed02736e6c074e1'), 'tools/label_writer_fence.ps1': ('f5e36a8d7142a0e130ef712b35178052d1d11140853ac98310b591f9eb8e7f20', '44ad315d0ffe526f003e3d8d17ff0940338652e7e557ad22ed6e728551ce3d3f'), 'tools/label_writer_fence_contract.json': ('bc95bd4ae6984f08120d7284f1ef73e7f1369e8fae078f6d60c66144a5cb24de', '8a07c41b748669e237fc7f35be75daede0fca9f86c2d1af07ff7a701a33126dd'), 'app/current_user_scheduled_task.py': ('5b339cae5a61c67bf3187f75ae254c38df4414bf7c0f5f3b8ed24f3cff52e870', 'ad9e95381b52e57cfdecd84373b61ae5449ac8edeb9fef33199432896d0b1417'), 'app/direct_sync_push.py': ('86c182b049dfffb9bc2256d140b8dbaa95aea631abcdf5b4f3da244b696643bd', '7556f245d8724729d70e0639f6c5fcec06cc77e0f3e8b1f871278c585cd45666'), 'app/kmtech_shared.lock.json': ('87cf641db77b8052ffbaeee86d0c2cbab84f07ce4c3748c796985be2eacc0cf9', '14ff6bbc353aab8fa9ce88297c79f2ca5504c554e0edb9671eb62d1766395e9f'), 'app/kmtech_shared.manifest.json': ('feaed459688e915eb5f52f264501a4287261d9dcbdbc50e0c6f4bde59d8e7117', 'de1aa3a5a2b072ec15ec2b3d60733881dcf4d5c87f14b15ed06691ba643ac918'), 'app/label_admin_pin.py': ('8e5fb578366c38012792f5a3213310d6d442e4ff68202edf84e4983872f13bf0', '9be9247c439a99c696cb472f0bee0eee85b2fe1a98ee2a25aabe6395aff7a617'), 'app/label_completion.py': ('7650944f6d9f7431d8089b373a32aa649c324ecacfb8c24b40f13ccc2f2fdb55', 'ed37d0733e62e375fcd85f873b2459f5fd6a3e7d68ced48f36200cb5e871c0ac'), 'app/label_data_manager.py': ('eb4f4bef03152c05c6ef160abb7738dfa8416c971bb79baac4092d52905c14ec', '2ddd2129d6de99073f3cd51d8947a4792ea56e371205f3ea70445301317f4e56'), 'app/label_match_single_instance.py': ('64040b936adb201d21ecbf55d47536b545a8c10048200a4337635f0a7b00bb02', 'f16db6f775d66233f96e274ea9b75260b6694800f60152e4a9edb133e9de93ee'), 'app/label_recovery_schema.py': ('01fdfd2616bdc6a2a713ed89f32f23b73db9fe2d2e86d90482023e9950a178f7', '4bf2ad6024efd6c3ac2f5965eef3e81fa149078f4bc22ff716272a5c2359ffa6'), 'app/label_workbench_history.py': ('409aa2f5ae88e571aa599d30dc7766e6563829e5bfe29ba2fd0d0989aeabd524', '30743e22b77939bed5d79b1e490c6df76ab577a242327a2d281f164c93ef1ca5'), 'app/package_command_draft.py': ('4936fdccd3af20e7f48c0fc0bdb3eb41e6baee073bf8dea90252adb487c60563', '8144882b47008ee3e2c35cbd62bdf70637d34d89a5e1ade7620ef405cc7423ac'), 'app/package_logistics.py': ('552dc215f1a6fd22a3eb98959a05cb21ae086ac772f9cac4a71b6bef49d875b2', '732b2449cee55b15e9c687a542814f76c5dc0a0c49337ec018194caa7960b3c1'), 'app/package_outbox.py': ('00e9dbd0272b281dae2b38363a9683ff2de14f1abffed00c0b414a0fe5b7f786', 'dc36a36e1697d1d3a42c5f0b4bf147791fa6687444a80d912778c83bd049f683'), 'app/package_outbox_processor.py': ('bc57706c8edde0f5d2dd8e3d54a010979fc177e819d9aeb57662b52e3610075d', 'bc8cbc44c0b573df1a658fc09ec980e3965196680d8c7294daa52846b334ded1'), 'app/phs_label_workflow.py': ('c1746fe550017bccdf8b8fd6b98ed1438cd49b5878d98328cceec7d6cb6edd7c', '72bb834611c5172e3cb9a0c517b49e4a664adcffef05006f90015a6384584ae0'), 'app/producer_runtime_client.py': ('c5c111e72d150395b825ac66e79208540560495703039d0ea4c4f215eb45b2f9', 'd575094842ab4bb126f16d5372074aad03e20e4c0163f2ab0ffc5b10f77e271c'), 'app/sealed_transfer_exchange.py': ('a84adc6dc1b2064d0e37b3e49090dd06348603368de7babf83351a99e3f47e1f', '1202d1fecf10ed6e6d6e7662ca9c87ff4aa2553d14e4f8fa67210f770b8ab755'), 'tools/bootstrap_integrity.ps1': ('7cf77e9cc833093aed9ed7b3d79565f5396eed0288b1e7729e08959e82e12ff3', '96101ce38befdf9dcca09ec42abd571aa441ad312f35bf3d14fd7effc97db963'), 'app/kmtech_shared/__init__.py': ('32f34fccf9a00e5d27e852a95fc7b776f18a5eccda991f47dc33cd8111d50295', '7714a7a02cda3da34c4972101c0e01cc052435a00d8d520b5c274a05aa9850e8'), 'app/tools/direct_sync_relay_runner.py': ('c797ffeee38f5161621e526c4ef2f3844cbe848ea0124f875f3df05a1779af98', '9b8e7a9845c4e73edea64b4ac3d0b90e3f461b7d926e0e1db64bdb9197f734ac'), 'app/tools/register_label_match_worker_pc.py': ('469c2e2ebdc9ddb45973e1423336bd6619b485e1604732d4d7bec55c41f0931f', 'fbb7397c227cf6845e01124119cafc0081b9a0aa4d50c44703d339fc5dd8271b'), 'app/tools/direct_sync_relay_install_pack.py': ('5a4368f2bb692dd37f406f449e607390024668a8101929577d88c549ad7093e5', '53cc1d41e90b0b9cda364ab744ce81abeaf720d222a7fe6263bd15728ec77d7f'), 'app/direct_sync_runtime.py': ('50ff189da613b1e1bec5bbb32061acc76e8d29ecae8808f5f591854a361be033', '0a4d8ddb463a8f5d52cbe60cfbfaca2aa22c06fa1c89831944cc5c54df854d04'), 'app/item_catalog_sync.py': ('41c33bf6a473cc147db6e522b1d20d4771cc9bd92c74fa3a8bd091a141907387', '7ba452c285d2f41fea202dedd68fedbe15d514a8d632bf6818f27ac54851c3d4'), 'app/label_guarded_runtime_reconcile.py': ('5f2d668622236950e307e5dc964303f78c7074d3f4bf2c2a37018c2fffe46016', 'dccab991a4caa38b7cefc59aa92357393219131232f3252c50d23dc7240a8a1e'), 'app/logistics_runtime_profile.py': ('1d786cb74812b1b4588d122b333cc9b8724d28e7410f91c7c0f025d9fd1bd5ee', 'db92068a17237e47d3025bece8d8f479fbe5eca46fde86e90666e3cac49fab72'), 'app/protected_admin.py': ('f4b1616dc152c84e368a32acb06b424b4b4cdab588635e383fa22e7ee6d3c58e', 'eadfb46474595075b840bbdf87b919910c228ac5481fe62721a3497c478d63f9'), 'app/registration_relay_recovery.py': ('25921c0e33df76ef1c52655d95f2482906d444409d5c4a4d602e3745ff8d721b', '013758e5ab7f9084eae87651fd088a667e240841384deae3140858d28998f144'), 'app/tools/direct_sync_relay_operator.py': ('ff97593c29669ad1f983d6f7be0fb1ebabf27adcf9661b797d36f7d583a0cedb', '3c990ebfd9ff356d5876b40d1098470d0602fae3a3ed838ccf0cbc644150032f'), 'app/tools/install_logistics_runtime_profile.py': ('eb8650ace3df179a7c68f3247efe5eac86200543a43f4b2c80a47ccc95b336ef', '7dde5875649e9ec9291a45e47191b9c6fcf6b32d66cafe70c0444390cefbab9e'), 'app/tools/label_auth_recovery_canary.py': ('440a0ed5061b522cafa56408092e3ed361b42f1ae09569258b08005a04cd463b', '079dfe927bcf721f4db49779eeab96b66093e1e02c69b86da73ea069d5b7c9be'), 'app/tools/label_exact_clone_resolution_receipt.py': ('c77e6be851f32cad9058ee52892a9b2520232a15914379aba3c28ad09c581a9b', 'd1e0a8955eed3f056cb964d4f0058f082664740b03b98eefc8f5ea872ded987c'), 'app/tools/label_legacy_task_quiescence.py': ('02a422b3d5b2662a061ea79d88fd6d8da409d6daba92ffe22266e796da20dfe4', 'c9db34c66056e3e63eca603e3c1997f12367b58b6e748941b8d80a5fb7981b7e'), 'app/deferred_intent_capture.py': ('970c6565a3f5f589f15d9cc2fb0ff1e5980e772d977976152b93f3c7e6e41dbd', 'f1dc02ba1711847228945fe6a01c3881aef6e71441a85cbf5873dbf72700d72e'), 'app/label_exact_clone_resolution.py': ('0ff69b167d2b88e9be8d5a6bc068b6f160b4a19023a92b401881fc86cf7b24e4', 'f162f2c2680852cfa1ec52d4a355b275fcb87482561bb84aac89aa24e05696ce'), 'app/label_workbench_activity.py': ('42ddafdfe2a45ab60f9eb0ffca4f73d4df2efbbc15f9eca1c12628f0e22c8706', 'b94ea10dbbfc75debb375386af285e43f87231ff4a14c42f1da845f6833fa152'), 'app/label_workbench_context.py': ('32835bfbd588afd84fae494aabeab2add65f2ecb0b22aa0bb2478951107afe51', '7139f01df5558e89718c00fb65a95d83840b0c473ae747643250872eb0511f69'), 'app/label_workbench_scan.py': ('5705832318e7954554b4bd94de77b1168e63622580d7bcc49fa5b4eb534ff271', '0d7201fabded66e0bbd620dc6fd7d98f2d8b747e0c7e17a4f8b224d66d049d52'), 'app/ui/workflow_view_state.py': ('b914aece96760c7e149876d56ccd24b8efae2f33e68a23a3a5a183e1cdfc5242', 'ca42f4a1a106b60a39fe07ee74f78b3fdc4b3935986ecd6b23140c7f4c768a68')}, '817d393b7316735921c2aa6ede271a04cbea20fd': {'INSTALL_THIS_PC.ps1': ('8efe37d3203c94c71dff93a0761a674f400154b7b2d5c7bef8a91e2d09e5b702', 'bdc42c26a8b5464d8a65e880ba5474ef01a28c820997adcb5ddefa8daa9b022b'), 'app/Label_Match.py': ('78968d1946644a76542bb011bea5881403bcdb4f6482de1497b4f7c0921a261a', '43c93e151623c6098ab6dc0b200d5d44c415cdbe12f2e3be97c3172c41d270e9'), 'app/carrier_identity_port.py': ('08e470a49aef5213f458f352df2118cf79e01160ba2b0544091161358717584d', '3690500795de424c928570593debb4afcfc50d1175f2ccba77f44fb56c97979b'), 'app/config/app_settings.json': ('081e0ab6e932035312c9bc225db26589e4c694cd2c40170ca459443e1a5460e8', 'e65540f3325c70866753cd0abfe01c21edb89f6fa05cd1ddc1666a89af14c34b'), 'app/current_user_onboarding.py': ('323c4100d3ee0d32077232be2656209daa581f9e19ca018d3e840009d03dc26e', '93a4d7b67b9d740ddbf8bfa9546f01fc0c927ee0de985f4be4e0d83adf435ebd'), 'app/deferred_intent_capture.py': ('e129488f303e001293431e8b45a0edc10bab22a7e9cb7e96782aa6d3df260561', 'f1dc02ba1711847228945fe6a01c3881aef6e71441a85cbf5873dbf72700d72e'), 'app/enrollment_mutex.py': ('040294978e4341d9f06573f9117d99913f1dab8ef85de5f08a5296228fbb9787', 'ce77c0a1ab113326dffdb611a74c19308a48fc1b113626509960614fd3295981'), 'app/label_completion.py': ('aaac26de64e050b07b2225723898c8c32e52f6526039202ffd8d2daa12f162b8', 'ed37d0733e62e375fcd85f873b2459f5fd6a3e7d68ced48f36200cb5e871c0ac'), 'app/label_data_manager.py': ('95887ca72e74aa258ba07584e1df14fc099f4b16998629d4639e4f5749ecc757', '2ddd2129d6de99073f3cd51d8947a4792ea56e371205f3ea70445301317f4e56'), 'app/label_match_product_host.py': ('bce7176d636a93c559340850d91894ffd02a4eef2a3f953d2ae05cd1cb0f9fbf', '2f68dda99e3bc9e0d6e1c325cb43d169845df3501050a354c624a44690097845'), 'app/label_match_single_instance.py': ('3c69b65c5091c9d55bd28bebacad2a1fae5801a663cc7f762dacf1c3c68043e7', 'f16db6f775d66233f96e274ea9b75260b6694800f60152e4a9edb133e9de93ee'), 'app/label_recovery_schema.py': ('e783a8c7ecac94dd415810f2891161967ec8f13a802d74d7da466e646986d462', '4bf2ad6024efd6c3ac2f5965eef3e81fa149078f4bc22ff716272a5c2359ffa6'), 'app/label_transition.py': ('001001427e7352c308b351263770b2649aa96898b712396d8ae3ae4530186cf9', '27c5952d804558211c7db05ac3489e4bd0422599a259af6b287fec5863f09279'), 'app/label_workbench_history.py': ('214ee81fdc36cfc51d6b59c15ee69f4f5057ebfc899eced18f7b61c0daff50f8', '30743e22b77939bed5d79b1e490c6df76ab577a242327a2d281f164c93ef1ca5'), 'app/logistics_runtime_profile.py': ('2cd3fe00a34e79a6c12a995f74ba99515f88fff4e9bef50e844ab02dc59da1ed', 'db92068a17237e47d3025bece8d8f479fbe5eca46fde86e90666e3cac49fab72'), 'app/main.py': ('520c2eaea01876f324288f01fee6f7600dd45d27829fb0e374969f6e2bf2bb6c', 'f85e46a1779eba99ac26609a172e8e4fad55e4e4115ee314bc5e1e66f4cf75ab'), 'app/package_command_draft.py': ('7f296c95fbcdfdd117eb275068fcb59e3dcc91f63112fb0e7b8fc421a2a3e151', '8144882b47008ee3e2c35cbd62bdf70637d34d89a5e1ade7620ef405cc7423ac'), 'app/package_logistics.py': ('818a1d7fa228e125c30c23f6950b6d579c69c22fda1bd9f2d9aded9e9b4201a1', '732b2449cee55b15e9c687a542814f76c5dc0a0c49337ec018194caa7960b3c1'), 'app/package_outbox.py': ('39b09189d4afddcb4bf937a8d300573391281092b6e62ac828ef41f1f72b6437', 'dc36a36e1697d1d3a42c5f0b4bf147791fa6687444a80d912778c83bd049f683'), 'app/package_outbox_processor.py': ('4589f804c3d57e56ad93066d83d8c122b3d3eec9510aee9c2152375f8c0c3f79', 'bc8cbc44c0b573df1a658fc09ec980e3965196680d8c7294daa52846b334ded1'), 'app/sealed_transfer_exchange.py': ('f0452daf434ae11710a9e5cfa825478d9ceb552fadc804be269e5e0b2d26fca2', '1202d1fecf10ed6e6d6e7662ca9c87ff4aa2553d14e4f8fa67210f770b8ab755'), 'app/terminal_operation_lease.py': ('4e30d014f82a2dea766ef735c39a3b9c0f4b9e4c521ce76b25c9ac8cfb3927b7', 'a1c5a01061c87b482f49707995f85672279efa190ebcc97bea3ff74f32f77bfa'), 'app/tools/install_logistics_runtime_profile.py': ('7be760a5e49054740b495e62d48da98259b7e40eb288d1a46604cf8201994122', '7dde5875649e9ec9291a45e47191b9c6fcf6b32d66cafe70c0444390cefbab9e'), 'app/ui/workflow_view_state.py': ('45066b34a856ca47c3aa7f10b44f72e948a2db5827a8f6da948eeb7d6a8a5e0a', 'ca42f4a1a106b60a39fe07ee74f78b3fdc4b3935986ecd6b23140c7f4c768a68'), 'app/user_relay.py': ('07291437ab724a9512332b25666099f13170af2f12f5f62328ea6af54cad96e7', '1015c0d1026d69745d70bbe1d69ab0f5a97cec52eaf8831ecc95d0356d8f4d3b'), 'app/writer_session_fence.py': ('da8873b44fd4fc8c69cc21b17f7e53695630c807d70b48c4ab653a26e73d1a50', '92419d83453e49cd225e79ccc1aa96230c7ba00636b7fd114ed02736e6c074e1'), 'tools/label_writer_fence.ps1': ('c89453e181b94b7b018b1be78b1fad76d73d0fecd4f728ee6cf395fed6afbf27', '44ad315d0ffe526f003e3d8d17ff0940338652e7e557ad22ed6e728551ce3d3f'), 'tools/label_writer_fence_contract.json': ('bc95bd4ae6984f08120d7284f1ef73e7f1369e8fae078f6d60c66144a5cb24de', '8a07c41b748669e237fc7f35be75daede0fca9f86c2d1af07ff7a701a33126dd'), 'app/current_user_scheduled_task.py': ('5b339cae5a61c67bf3187f75ae254c38df4414bf7c0f5f3b8ed24f3cff52e870', 'ad9e95381b52e57cfdecd84373b61ae5449ac8edeb9fef33199432896d0b1417'), 'app/direct_sync_push.py': ('86c182b049dfffb9bc2256d140b8dbaa95aea631abcdf5b4f3da244b696643bd', '7556f245d8724729d70e0639f6c5fcec06cc77e0f3e8b1f871278c585cd45666'), 'app/kmtech_shared.lock.json': ('87cf641db77b8052ffbaeee86d0c2cbab84f07ce4c3748c796985be2eacc0cf9', '14ff6bbc353aab8fa9ce88297c79f2ca5504c554e0edb9671eb62d1766395e9f'), 'app/kmtech_shared.manifest.json': ('feaed459688e915eb5f52f264501a4287261d9dcbdbc50e0c6f4bde59d8e7117', 'de1aa3a5a2b072ec15ec2b3d60733881dcf4d5c87f14b15ed06691ba643ac918'), 'app/label_admin_pin.py': ('8e5fb578366c38012792f5a3213310d6d442e4ff68202edf84e4983872f13bf0', '9be9247c439a99c696cb472f0bee0eee85b2fe1a98ee2a25aabe6395aff7a617'), 'app/phs_label_workflow.py': ('c1746fe550017bccdf8b8fd6b98ed1438cd49b5878d98328cceec7d6cb6edd7c', '72bb834611c5172e3cb9a0c517b49e4a664adcffef05006f90015a6384584ae0'), 'app/producer_runtime_client.py': ('c5c111e72d150395b825ac66e79208540560495703039d0ea4c4f215eb45b2f9', 'd575094842ab4bb126f16d5372074aad03e20e4c0163f2ab0ffc5b10f77e271c'), 'tools/bootstrap_integrity.ps1': ('7cf77e9cc833093aed9ed7b3d79565f5396eed0288b1e7729e08959e82e12ff3', '96101ce38befdf9dcca09ec42abd571aa441ad312f35bf3d14fd7effc97db963'), 'app/kmtech_shared/__init__.py': ('32f34fccf9a00e5d27e852a95fc7b776f18a5eccda991f47dc33cd8111d50295', '7714a7a02cda3da34c4972101c0e01cc052435a00d8d520b5c274a05aa9850e8'), 'app/tools/direct_sync_relay_runner.py': ('c797ffeee38f5161621e526c4ef2f3844cbe848ea0124f875f3df05a1779af98', '9b8e7a9845c4e73edea64b4ac3d0b90e3f461b7d926e0e1db64bdb9197f734ac'), 'app/tools/register_label_match_worker_pc.py': ('469c2e2ebdc9ddb45973e1423336bd6619b485e1604732d4d7bec55c41f0931f', 'fbb7397c227cf6845e01124119cafc0081b9a0aa4d50c44703d339fc5dd8271b'), 'app/tools/direct_sync_relay_install_pack.py': ('5a4368f2bb692dd37f406f449e607390024668a8101929577d88c549ad7093e5', '53cc1d41e90b0b9cda364ab744ce81abeaf720d222a7fe6263bd15728ec77d7f'), 'app/direct_sync_runtime.py': ('50ff189da613b1e1bec5bbb32061acc76e8d29ecae8808f5f591854a361be033', '0a4d8ddb463a8f5d52cbe60cfbfaca2aa22c06fa1c89831944cc5c54df854d04'), 'app/item_catalog_sync.py': ('41c33bf6a473cc147db6e522b1d20d4771cc9bd92c74fa3a8bd091a141907387', '7ba452c285d2f41fea202dedd68fedbe15d514a8d632bf6818f27ac54851c3d4'), 'app/label_guarded_runtime_reconcile.py': ('5f2d668622236950e307e5dc964303f78c7074d3f4bf2c2a37018c2fffe46016', 'dccab991a4caa38b7cefc59aa92357393219131232f3252c50d23dc7240a8a1e'), 'app/protected_admin.py': ('f4b1616dc152c84e368a32acb06b424b4b4cdab588635e383fa22e7ee6d3c58e', 'eadfb46474595075b840bbdf87b919910c228ac5481fe62721a3497c478d63f9'), 'app/registration_relay_recovery.py': ('25921c0e33df76ef1c52655d95f2482906d444409d5c4a4d602e3745ff8d721b', '013758e5ab7f9084eae87651fd088a667e240841384deae3140858d28998f144'), 'app/tools/direct_sync_relay_operator.py': ('ff97593c29669ad1f983d6f7be0fb1ebabf27adcf9661b797d36f7d583a0cedb', '3c990ebfd9ff356d5876b40d1098470d0602fae3a3ed838ccf0cbc644150032f'), 'app/tools/label_auth_recovery_canary.py': ('440a0ed5061b522cafa56408092e3ed361b42f1ae09569258b08005a04cd463b', '079dfe927bcf721f4db49779eeab96b66093e1e02c69b86da73ea069d5b7c9be'), 'app/tools/label_exact_clone_resolution_receipt.py': ('c77e6be851f32cad9058ee52892a9b2520232a15914379aba3c28ad09c581a9b', 'd1e0a8955eed3f056cb964d4f0058f082664740b03b98eefc8f5ea872ded987c'), 'app/tools/label_legacy_task_quiescence.py': ('02a422b3d5b2662a061ea79d88fd6d8da409d6daba92ffe22266e796da20dfe4', 'c9db34c66056e3e63eca603e3c1997f12367b58b6e748941b8d80a5fb7981b7e'), 'app/label_exact_clone_resolution.py': ('0ff69b167d2b88e9be8d5a6bc068b6f160b4a19023a92b401881fc86cf7b24e4', 'f162f2c2680852cfa1ec52d4a355b275fcb87482561bb84aac89aa24e05696ce'), 'app/label_workbench_activity.py': ('42ddafdfe2a45ab60f9eb0ffca4f73d4df2efbbc15f9eca1c12628f0e22c8706', 'b94ea10dbbfc75debb375386af285e43f87231ff4a14c42f1da845f6833fa152'), 'app/label_workbench_context.py': ('32835bfbd588afd84fae494aabeab2add65f2ecb0b22aa0bb2478951107afe51', '7139f01df5558e89718c00fb65a95d83840b0c473ae747643250872eb0511f69'), 'app/label_workbench_scan.py': ('5705832318e7954554b4bd94de77b1168e63622580d7bcc49fa5b4eb534ff271', '0d7201fabded66e0bbd620dc6fd7d98f2d8b747e0c7e17a4f8b224d66d049d52')}, '92abdaa377b2b3255c25ff0e5e331c3a4f1ddfa4': {'INSTALL_THIS_PC.ps1': ('8efe37d3203c94c71dff93a0761a674f400154b7b2d5c7bef8a91e2d09e5b702', 'bdc42c26a8b5464d8a65e880ba5474ef01a28c820997adcb5ddefa8daa9b022b'), 'app/Label_Match.py': ('b30d80d46cddd039ca2475ddeb1be0465fe6e3b67479e7ed1c8e8f0c4451813d', '43c93e151623c6098ab6dc0b200d5d44c415cdbe12f2e3be97c3172c41d270e9'), 'app/carrier_identity_port.py': ('08e470a49aef5213f458f352df2118cf79e01160ba2b0544091161358717584d', '3690500795de424c928570593debb4afcfc50d1175f2ccba77f44fb56c97979b'), 'app/config/app_settings.json': ('081e0ab6e932035312c9bc225db26589e4c694cd2c40170ca459443e1a5460e8', 'e65540f3325c70866753cd0abfe01c21edb89f6fa05cd1ddc1666a89af14c34b'), 'app/current_user_onboarding.py': ('323c4100d3ee0d32077232be2656209daa581f9e19ca018d3e840009d03dc26e', '93a4d7b67b9d740ddbf8bfa9546f01fc0c927ee0de985f4be4e0d83adf435ebd'), 'app/deferred_intent_capture.py': ('e129488f303e001293431e8b45a0edc10bab22a7e9cb7e96782aa6d3df260561', 'f1dc02ba1711847228945fe6a01c3881aef6e71441a85cbf5873dbf72700d72e'), 'app/enrollment_mutex.py': ('040294978e4341d9f06573f9117d99913f1dab8ef85de5f08a5296228fbb9787', 'ce77c0a1ab113326dffdb611a74c19308a48fc1b113626509960614fd3295981'), 'app/label_completion.py': ('aaac26de64e050b07b2225723898c8c32e52f6526039202ffd8d2daa12f162b8', 'ed37d0733e62e375fcd85f873b2459f5fd6a3e7d68ced48f36200cb5e871c0ac'), 'app/label_data_manager.py': ('95887ca72e74aa258ba07584e1df14fc099f4b16998629d4639e4f5749ecc757', '2ddd2129d6de99073f3cd51d8947a4792ea56e371205f3ea70445301317f4e56'), 'app/label_match_product_host.py': ('bce7176d636a93c559340850d91894ffd02a4eef2a3f953d2ae05cd1cb0f9fbf', '2f68dda99e3bc9e0d6e1c325cb43d169845df3501050a354c624a44690097845'), 'app/label_match_single_instance.py': ('3c69b65c5091c9d55bd28bebacad2a1fae5801a663cc7f762dacf1c3c68043e7', 'f16db6f775d66233f96e274ea9b75260b6694800f60152e4a9edb133e9de93ee'), 'app/label_recovery_schema.py': ('e783a8c7ecac94dd415810f2891161967ec8f13a802d74d7da466e646986d462', '4bf2ad6024efd6c3ac2f5965eef3e81fa149078f4bc22ff716272a5c2359ffa6'), 'app/label_transition.py': ('001001427e7352c308b351263770b2649aa96898b712396d8ae3ae4530186cf9', '27c5952d804558211c7db05ac3489e4bd0422599a259af6b287fec5863f09279'), 'app/label_workbench_history.py': ('214ee81fdc36cfc51d6b59c15ee69f4f5057ebfc899eced18f7b61c0daff50f8', '30743e22b77939bed5d79b1e490c6df76ab577a242327a2d281f164c93ef1ca5'), 'app/logistics_runtime_profile.py': ('2cd3fe00a34e79a6c12a995f74ba99515f88fff4e9bef50e844ab02dc59da1ed', 'db92068a17237e47d3025bece8d8f479fbe5eca46fde86e90666e3cac49fab72'), 'app/main.py': ('520c2eaea01876f324288f01fee6f7600dd45d27829fb0e374969f6e2bf2bb6c', 'f85e46a1779eba99ac26609a172e8e4fad55e4e4115ee314bc5e1e66f4cf75ab'), 'app/package_command_draft.py': ('7f296c95fbcdfdd117eb275068fcb59e3dcc91f63112fb0e7b8fc421a2a3e151', '8144882b47008ee3e2c35cbd62bdf70637d34d89a5e1ade7620ef405cc7423ac'), 'app/package_logistics.py': ('818a1d7fa228e125c30c23f6950b6d579c69c22fda1bd9f2d9aded9e9b4201a1', '732b2449cee55b15e9c687a542814f76c5dc0a0c49337ec018194caa7960b3c1'), 'app/package_outbox.py': ('39b09189d4afddcb4bf937a8d300573391281092b6e62ac828ef41f1f72b6437', 'dc36a36e1697d1d3a42c5f0b4bf147791fa6687444a80d912778c83bd049f683'), 'app/package_outbox_processor.py': ('4589f804c3d57e56ad93066d83d8c122b3d3eec9510aee9c2152375f8c0c3f79', 'bc8cbc44c0b573df1a658fc09ec980e3965196680d8c7294daa52846b334ded1'), 'app/sealed_transfer_exchange.py': ('f0452daf434ae11710a9e5cfa825478d9ceb552fadc804be269e5e0b2d26fca2', '1202d1fecf10ed6e6d6e7662ca9c87ff4aa2553d14e4f8fa67210f770b8ab755'), 'app/terminal_operation_lease.py': ('4e30d014f82a2dea766ef735c39a3b9c0f4b9e4c521ce76b25c9ac8cfb3927b7', 'a1c5a01061c87b482f49707995f85672279efa190ebcc97bea3ff74f32f77bfa'), 'app/tools/install_logistics_runtime_profile.py': ('7be760a5e49054740b495e62d48da98259b7e40eb288d1a46604cf8201994122', '7dde5875649e9ec9291a45e47191b9c6fcf6b32d66cafe70c0444390cefbab9e'), 'app/ui/workflow_view_state.py': ('45066b34a856ca47c3aa7f10b44f72e948a2db5827a8f6da948eeb7d6a8a5e0a', 'ca42f4a1a106b60a39fe07ee74f78b3fdc4b3935986ecd6b23140c7f4c768a68'), 'app/user_relay.py': ('07291437ab724a9512332b25666099f13170af2f12f5f62328ea6af54cad96e7', '1015c0d1026d69745d70bbe1d69ab0f5a97cec52eaf8831ecc95d0356d8f4d3b'), 'app/writer_session_fence.py': ('b38cd124a736dfd2d020b0401b409f95049ee99444795cd5fe13f309bbaf42b7', '92419d83453e49cd225e79ccc1aa96230c7ba00636b7fd114ed02736e6c074e1'), 'tools/label_writer_fence.ps1': ('6525f38c350d1d875a6943c3b858b417cbeaac8a7b03e1f984f8584f8445d42f', '44ad315d0ffe526f003e3d8d17ff0940338652e7e557ad22ed6e728551ce3d3f'), 'tools/label_writer_fence_contract.json': ('bc95bd4ae6984f08120d7284f1ef73e7f1369e8fae078f6d60c66144a5cb24de', '8a07c41b748669e237fc7f35be75daede0fca9f86c2d1af07ff7a701a33126dd'), 'app/current_user_scheduled_task.py': ('5b339cae5a61c67bf3187f75ae254c38df4414bf7c0f5f3b8ed24f3cff52e870', 'ad9e95381b52e57cfdecd84373b61ae5449ac8edeb9fef33199432896d0b1417'), 'app/direct_sync_push.py': ('86c182b049dfffb9bc2256d140b8dbaa95aea631abcdf5b4f3da244b696643bd', '7556f245d8724729d70e0639f6c5fcec06cc77e0f3e8b1f871278c585cd45666'), 'app/kmtech_shared.lock.json': ('87cf641db77b8052ffbaeee86d0c2cbab84f07ce4c3748c796985be2eacc0cf9', '14ff6bbc353aab8fa9ce88297c79f2ca5504c554e0edb9671eb62d1766395e9f'), 'app/kmtech_shared.manifest.json': ('feaed459688e915eb5f52f264501a4287261d9dcbdbc50e0c6f4bde59d8e7117', 'de1aa3a5a2b072ec15ec2b3d60733881dcf4d5c87f14b15ed06691ba643ac918'), 'app/label_admin_pin.py': ('8e5fb578366c38012792f5a3213310d6d442e4ff68202edf84e4983872f13bf0', '9be9247c439a99c696cb472f0bee0eee85b2fe1a98ee2a25aabe6395aff7a617'), 'app/phs_label_workflow.py': ('c1746fe550017bccdf8b8fd6b98ed1438cd49b5878d98328cceec7d6cb6edd7c', '72bb834611c5172e3cb9a0c517b49e4a664adcffef05006f90015a6384584ae0'), 'app/producer_runtime_client.py': ('c5c111e72d150395b825ac66e79208540560495703039d0ea4c4f215eb45b2f9', 'd575094842ab4bb126f16d5372074aad03e20e4c0163f2ab0ffc5b10f77e271c'), 'tools/bootstrap_integrity.ps1': ('7cf77e9cc833093aed9ed7b3d79565f5396eed0288b1e7729e08959e82e12ff3', '96101ce38befdf9dcca09ec42abd571aa441ad312f35bf3d14fd7effc97db963'), 'app/kmtech_shared/__init__.py': ('32f34fccf9a00e5d27e852a95fc7b776f18a5eccda991f47dc33cd8111d50295', '7714a7a02cda3da34c4972101c0e01cc052435a00d8d520b5c274a05aa9850e8'), 'app/tools/direct_sync_relay_runner.py': ('c797ffeee38f5161621e526c4ef2f3844cbe848ea0124f875f3df05a1779af98', '9b8e7a9845c4e73edea64b4ac3d0b90e3f461b7d926e0e1db64bdb9197f734ac'), 'app/tools/register_label_match_worker_pc.py': ('469c2e2ebdc9ddb45973e1423336bd6619b485e1604732d4d7bec55c41f0931f', 'fbb7397c227cf6845e01124119cafc0081b9a0aa4d50c44703d339fc5dd8271b'), 'app/tools/direct_sync_relay_install_pack.py': ('5a4368f2bb692dd37f406f449e607390024668a8101929577d88c549ad7093e5', '53cc1d41e90b0b9cda364ab744ce81abeaf720d222a7fe6263bd15728ec77d7f'), 'app/direct_sync_runtime.py': ('50ff189da613b1e1bec5bbb32061acc76e8d29ecae8808f5f591854a361be033', '0a4d8ddb463a8f5d52cbe60cfbfaca2aa22c06fa1c89831944cc5c54df854d04'), 'app/item_catalog_sync.py': ('41c33bf6a473cc147db6e522b1d20d4771cc9bd92c74fa3a8bd091a141907387', '7ba452c285d2f41fea202dedd68fedbe15d514a8d632bf6818f27ac54851c3d4'), 'app/label_guarded_runtime_reconcile.py': ('5f2d668622236950e307e5dc964303f78c7074d3f4bf2c2a37018c2fffe46016', 'dccab991a4caa38b7cefc59aa92357393219131232f3252c50d23dc7240a8a1e'), 'app/protected_admin.py': ('f4b1616dc152c84e368a32acb06b424b4b4cdab588635e383fa22e7ee6d3c58e', 'eadfb46474595075b840bbdf87b919910c228ac5481fe62721a3497c478d63f9'), 'app/registration_relay_recovery.py': ('25921c0e33df76ef1c52655d95f2482906d444409d5c4a4d602e3745ff8d721b', '013758e5ab7f9084eae87651fd088a667e240841384deae3140858d28998f144'), 'app/tools/direct_sync_relay_operator.py': ('ff97593c29669ad1f983d6f7be0fb1ebabf27adcf9661b797d36f7d583a0cedb', '3c990ebfd9ff356d5876b40d1098470d0602fae3a3ed838ccf0cbc644150032f'), 'app/tools/label_auth_recovery_canary.py': ('440a0ed5061b522cafa56408092e3ed361b42f1ae09569258b08005a04cd463b', '079dfe927bcf721f4db49779eeab96b66093e1e02c69b86da73ea069d5b7c9be'), 'app/tools/label_exact_clone_resolution_receipt.py': ('c77e6be851f32cad9058ee52892a9b2520232a15914379aba3c28ad09c581a9b', 'd1e0a8955eed3f056cb964d4f0058f082664740b03b98eefc8f5ea872ded987c'), 'app/tools/label_legacy_task_quiescence.py': ('02a422b3d5b2662a061ea79d88fd6d8da409d6daba92ffe22266e796da20dfe4', 'c9db34c66056e3e63eca603e3c1997f12367b58b6e748941b8d80a5fb7981b7e'), 'app/label_exact_clone_resolution.py': ('0ff69b167d2b88e9be8d5a6bc068b6f160b4a19023a92b401881fc86cf7b24e4', 'f162f2c2680852cfa1ec52d4a355b275fcb87482561bb84aac89aa24e05696ce'), 'app/label_workbench_activity.py': ('42ddafdfe2a45ab60f9eb0ffca4f73d4df2efbbc15f9eca1c12628f0e22c8706', 'b94ea10dbbfc75debb375386af285e43f87231ff4a14c42f1da845f6833fa152'), 'app/label_workbench_context.py': ('32835bfbd588afd84fae494aabeab2add65f2ecb0b22aa0bb2478951107afe51', '7139f01df5558e89718c00fb65a95d83840b0c473ae747643250872eb0511f69'), 'app/label_workbench_scan.py': ('5705832318e7954554b4bd94de77b1168e63622580d7bcc49fa5b4eb534ff271', '0d7201fabded66e0bbd620dc6fd7d98f2d8b747e0c7e17a4f8b224d66d049d52')}, 'b00712c410770f1d023b09aa9a1de4743a81b1b6': {'INSTALL_THIS_PC.ps1': ('8efe37d3203c94c71dff93a0761a674f400154b7b2d5c7bef8a91e2d09e5b702', 'bdc42c26a8b5464d8a65e880ba5474ef01a28c820997adcb5ddefa8daa9b022b'), 'app/Label_Match.py': ('c6a122c91e2f6437bb8857986884899d4336469bfb83ee7731b83a81376e9aec', '43c93e151623c6098ab6dc0b200d5d44c415cdbe12f2e3be97c3172c41d270e9'), 'app/config/app_settings.json': ('081e0ab6e932035312c9bc225db26589e4c694cd2c40170ca459443e1a5460e8', 'e65540f3325c70866753cd0abfe01c21edb89f6fa05cd1ddc1666a89af14c34b'), 'app/current_user_onboarding.py': ('84fde787fe1bca5d3138c3cd243d9b13ffaf834ffc69a5414b39d0c2b81f905d', '93a4d7b67b9d740ddbf8bfa9546f01fc0c927ee0de985f4be4e0d83adf435ebd'), 'app/label_match_product_host.py': ('bce7176d636a93c559340850d91894ffd02a4eef2a3f953d2ae05cd1cb0f9fbf', '2f68dda99e3bc9e0d6e1c325cb43d169845df3501050a354c624a44690097845'), 'app/main.py': ('520c2eaea01876f324288f01fee6f7600dd45d27829fb0e374969f6e2bf2bb6c', 'f85e46a1779eba99ac26609a172e8e4fad55e4e4115ee314bc5e1e66f4cf75ab'), 'app/user_relay.py': ('07291437ab724a9512332b25666099f13170af2f12f5f62328ea6af54cad96e7', '1015c0d1026d69745d70bbe1d69ab0f5a97cec52eaf8831ecc95d0356d8f4d3b'), 'app/writer_session_fence.py': ('f333164e0306008d17b8eddaabb0da44bead475f9008181b97231839698bbbcc', '92419d83453e49cd225e79ccc1aa96230c7ba00636b7fd114ed02736e6c074e1'), 'tools/label_writer_fence.ps1': ('ed16a3a03ecdac3ae8c01ca41c124f6ddb4340d12834a8dd83ed8bad2f0049bd', '44ad315d0ffe526f003e3d8d17ff0940338652e7e557ad22ed6e728551ce3d3f'), 'tools/label_writer_fence_contract.json': ('bc95bd4ae6984f08120d7284f1ef73e7f1369e8fae078f6d60c66144a5cb24de', '8a07c41b748669e237fc7f35be75daede0fca9f86c2d1af07ff7a701a33126dd'), 'app/current_user_scheduled_task.py': ('5b339cae5a61c67bf3187f75ae254c38df4414bf7c0f5f3b8ed24f3cff52e870', 'ad9e95381b52e57cfdecd84373b61ae5449ac8edeb9fef33199432896d0b1417'), 'app/direct_sync_push.py': ('86c182b049dfffb9bc2256d140b8dbaa95aea631abcdf5b4f3da244b696643bd', '7556f245d8724729d70e0639f6c5fcec06cc77e0f3e8b1f871278c585cd45666'), 'app/kmtech_shared.lock.json': ('87cf641db77b8052ffbaeee86d0c2cbab84f07ce4c3748c796985be2eacc0cf9', '14ff6bbc353aab8fa9ce88297c79f2ca5504c554e0edb9671eb62d1766395e9f'), 'app/kmtech_shared.manifest.json': ('feaed459688e915eb5f52f264501a4287261d9dcbdbc50e0c6f4bde59d8e7117', 'de1aa3a5a2b072ec15ec2b3d60733881dcf4d5c87f14b15ed06691ba643ac918'), 'app/label_admin_pin.py': ('8e5fb578366c38012792f5a3213310d6d442e4ff68202edf84e4983872f13bf0', '9be9247c439a99c696cb472f0bee0eee85b2fe1a98ee2a25aabe6395aff7a617'), 'app/label_completion.py': ('7650944f6d9f7431d8089b373a32aa649c324ecacfb8c24b40f13ccc2f2fdb55', 'ed37d0733e62e375fcd85f873b2459f5fd6a3e7d68ced48f36200cb5e871c0ac'), 'app/label_data_manager.py': ('eb4f4bef03152c05c6ef160abb7738dfa8416c971bb79baac4092d52905c14ec', '2ddd2129d6de99073f3cd51d8947a4792ea56e371205f3ea70445301317f4e56'), 'app/label_match_single_instance.py': ('64040b936adb201d21ecbf55d47536b545a8c10048200a4337635f0a7b00bb02', 'f16db6f775d66233f96e274ea9b75260b6694800f60152e4a9edb133e9de93ee'), 'app/label_recovery_schema.py': ('01fdfd2616bdc6a2a713ed89f32f23b73db9fe2d2e86d90482023e9950a178f7', '4bf2ad6024efd6c3ac2f5965eef3e81fa149078f4bc22ff716272a5c2359ffa6'), 'app/label_workbench_history.py': ('409aa2f5ae88e571aa599d30dc7766e6563829e5bfe29ba2fd0d0989aeabd524', '30743e22b77939bed5d79b1e490c6df76ab577a242327a2d281f164c93ef1ca5'), 'app/package_command_draft.py': ('4936fdccd3af20e7f48c0fc0bdb3eb41e6baee073bf8dea90252adb487c60563', '8144882b47008ee3e2c35cbd62bdf70637d34d89a5e1ade7620ef405cc7423ac'), 'app/package_logistics.py': ('552dc215f1a6fd22a3eb98959a05cb21ae086ac772f9cac4a71b6bef49d875b2', '732b2449cee55b15e9c687a542814f76c5dc0a0c49337ec018194caa7960b3c1'), 'app/package_outbox.py': ('00e9dbd0272b281dae2b38363a9683ff2de14f1abffed00c0b414a0fe5b7f786', 'dc36a36e1697d1d3a42c5f0b4bf147791fa6687444a80d912778c83bd049f683'), 'app/package_outbox_processor.py': ('bc57706c8edde0f5d2dd8e3d54a010979fc177e819d9aeb57662b52e3610075d', 'bc8cbc44c0b573df1a658fc09ec980e3965196680d8c7294daa52846b334ded1'), 'app/phs_label_workflow.py': ('c1746fe550017bccdf8b8fd6b98ed1438cd49b5878d98328cceec7d6cb6edd7c', '72bb834611c5172e3cb9a0c517b49e4a664adcffef05006f90015a6384584ae0'), 'app/producer_runtime_client.py': ('c5c111e72d150395b825ac66e79208540560495703039d0ea4c4f215eb45b2f9', 'd575094842ab4bb126f16d5372074aad03e20e4c0163f2ab0ffc5b10f77e271c'), 'app/sealed_transfer_exchange.py': ('a84adc6dc1b2064d0e37b3e49090dd06348603368de7babf83351a99e3f47e1f', '1202d1fecf10ed6e6d6e7662ca9c87ff4aa2553d14e4f8fa67210f770b8ab755'), 'tools/bootstrap_integrity.ps1': ('7cf77e9cc833093aed9ed7b3d79565f5396eed0288b1e7729e08959e82e12ff3', '96101ce38befdf9dcca09ec42abd571aa441ad312f35bf3d14fd7effc97db963'), 'app/kmtech_shared/__init__.py': ('32f34fccf9a00e5d27e852a95fc7b776f18a5eccda991f47dc33cd8111d50295', '7714a7a02cda3da34c4972101c0e01cc052435a00d8d520b5c274a05aa9850e8'), 'app/tools/direct_sync_relay_runner.py': ('c797ffeee38f5161621e526c4ef2f3844cbe848ea0124f875f3df05a1779af98', '9b8e7a9845c4e73edea64b4ac3d0b90e3f461b7d926e0e1db64bdb9197f734ac'), 'app/tools/register_label_match_worker_pc.py': ('469c2e2ebdc9ddb45973e1423336bd6619b485e1604732d4d7bec55c41f0931f', 'fbb7397c227cf6845e01124119cafc0081b9a0aa4d50c44703d339fc5dd8271b'), 'app/tools/direct_sync_relay_install_pack.py': ('5a4368f2bb692dd37f406f449e607390024668a8101929577d88c549ad7093e5', '53cc1d41e90b0b9cda364ab744ce81abeaf720d222a7fe6263bd15728ec77d7f'), 'app/direct_sync_runtime.py': ('50ff189da613b1e1bec5bbb32061acc76e8d29ecae8808f5f591854a361be033', '0a4d8ddb463a8f5d52cbe60cfbfaca2aa22c06fa1c89831944cc5c54df854d04'), 'app/item_catalog_sync.py': ('41c33bf6a473cc147db6e522b1d20d4771cc9bd92c74fa3a8bd091a141907387', '7ba452c285d2f41fea202dedd68fedbe15d514a8d632bf6818f27ac54851c3d4'), 'app/label_guarded_runtime_reconcile.py': ('5f2d668622236950e307e5dc964303f78c7074d3f4bf2c2a37018c2fffe46016', 'dccab991a4caa38b7cefc59aa92357393219131232f3252c50d23dc7240a8a1e'), 'app/logistics_runtime_profile.py': ('1d786cb74812b1b4588d122b333cc9b8724d28e7410f91c7c0f025d9fd1bd5ee', 'db92068a17237e47d3025bece8d8f479fbe5eca46fde86e90666e3cac49fab72'), 'app/protected_admin.py': ('f4b1616dc152c84e368a32acb06b424b4b4cdab588635e383fa22e7ee6d3c58e', 'eadfb46474595075b840bbdf87b919910c228ac5481fe62721a3497c478d63f9'), 'app/registration_relay_recovery.py': ('25921c0e33df76ef1c52655d95f2482906d444409d5c4a4d602e3745ff8d721b', '013758e5ab7f9084eae87651fd088a667e240841384deae3140858d28998f144'), 'app/tools/direct_sync_relay_operator.py': ('ff97593c29669ad1f983d6f7be0fb1ebabf27adcf9661b797d36f7d583a0cedb', '3c990ebfd9ff356d5876b40d1098470d0602fae3a3ed838ccf0cbc644150032f'), 'app/tools/install_logistics_runtime_profile.py': ('eb8650ace3df179a7c68f3247efe5eac86200543a43f4b2c80a47ccc95b336ef', '7dde5875649e9ec9291a45e47191b9c6fcf6b32d66cafe70c0444390cefbab9e'), 'app/tools/label_auth_recovery_canary.py': ('440a0ed5061b522cafa56408092e3ed361b42f1ae09569258b08005a04cd463b', '079dfe927bcf721f4db49779eeab96b66093e1e02c69b86da73ea069d5b7c9be'), 'app/tools/label_exact_clone_resolution_receipt.py': ('c77e6be851f32cad9058ee52892a9b2520232a15914379aba3c28ad09c581a9b', 'd1e0a8955eed3f056cb964d4f0058f082664740b03b98eefc8f5ea872ded987c'), 'app/tools/label_legacy_task_quiescence.py': ('02a422b3d5b2662a061ea79d88fd6d8da409d6daba92ffe22266e796da20dfe4', 'c9db34c66056e3e63eca603e3c1997f12367b58b6e748941b8d80a5fb7981b7e'), 'app/deferred_intent_capture.py': ('970c6565a3f5f589f15d9cc2fb0ff1e5980e772d977976152b93f3c7e6e41dbd', 'f1dc02ba1711847228945fe6a01c3881aef6e71441a85cbf5873dbf72700d72e'), 'app/label_exact_clone_resolution.py': ('0ff69b167d2b88e9be8d5a6bc068b6f160b4a19023a92b401881fc86cf7b24e4', 'f162f2c2680852cfa1ec52d4a355b275fcb87482561bb84aac89aa24e05696ce'), 'app/label_workbench_activity.py': ('42ddafdfe2a45ab60f9eb0ffca4f73d4df2efbbc15f9eca1c12628f0e22c8706', 'b94ea10dbbfc75debb375386af285e43f87231ff4a14c42f1da845f6833fa152'), 'app/label_workbench_context.py': ('32835bfbd588afd84fae494aabeab2add65f2ecb0b22aa0bb2478951107afe51', '7139f01df5558e89718c00fb65a95d83840b0c473ae747643250872eb0511f69'), 'app/label_workbench_scan.py': ('5705832318e7954554b4bd94de77b1168e63622580d7bcc49fa5b4eb534ff271', '0d7201fabded66e0bbd620dc6fd7d98f2d8b747e0c7e17a4f8b224d66d049d52'), 'app/ui/workflow_view_state.py': ('b914aece96760c7e149876d56ccd24b8efae2f33e68a23a3a5a183e1cdfc5242', 'ca42f4a1a106b60a39fe07ee74f78b3fdc4b3935986ecd6b23140c7f4c768a68')}, 'c5e051b4b126aaab259d57d451be14f98d18d87e': {'INSTALL_THIS_PC.ps1': ('8efe37d3203c94c71dff93a0761a674f400154b7b2d5c7bef8a91e2d09e5b702', 'bdc42c26a8b5464d8a65e880ba5474ef01a28c820997adcb5ddefa8daa9b022b'), 'app/Label_Match.py': ('7324ab7b111d940762c4acb20cdc9348ed6f176d8d7e4f7e52bd3d9c9d50db57', '43c93e151623c6098ab6dc0b200d5d44c415cdbe12f2e3be97c3172c41d270e9'), 'app/carrier_identity_port.py': ('08e470a49aef5213f458f352df2118cf79e01160ba2b0544091161358717584d', '3690500795de424c928570593debb4afcfc50d1175f2ccba77f44fb56c97979b'), 'app/config/app_settings.json': ('081e0ab6e932035312c9bc225db26589e4c694cd2c40170ca459443e1a5460e8', 'e65540f3325c70866753cd0abfe01c21edb89f6fa05cd1ddc1666a89af14c34b'), 'app/current_user_onboarding.py': ('84fde787fe1bca5d3138c3cd243d9b13ffaf834ffc69a5414b39d0c2b81f905d', '93a4d7b67b9d740ddbf8bfa9546f01fc0c927ee0de985f4be4e0d83adf435ebd'), 'app/label_completion.py': ('d0fb68fe1d531d6ac7436c8a778c7c93d6d1dbd4da0918614d95414ee5861492', 'ed37d0733e62e375fcd85f873b2459f5fd6a3e7d68ced48f36200cb5e871c0ac'), 'app/label_data_manager.py': ('95887ca72e74aa258ba07584e1df14fc099f4b16998629d4639e4f5749ecc757', '2ddd2129d6de99073f3cd51d8947a4792ea56e371205f3ea70445301317f4e56'), 'app/label_match_product_host.py': ('bce7176d636a93c559340850d91894ffd02a4eef2a3f953d2ae05cd1cb0f9fbf', '2f68dda99e3bc9e0d6e1c325cb43d169845df3501050a354c624a44690097845'), 'app/label_recovery_schema.py': ('e783a8c7ecac94dd415810f2891161967ec8f13a802d74d7da466e646986d462', '4bf2ad6024efd6c3ac2f5965eef3e81fa149078f4bc22ff716272a5c2359ffa6'), 'app/label_transition.py': ('001001427e7352c308b351263770b2649aa96898b712396d8ae3ae4530186cf9', '27c5952d804558211c7db05ac3489e4bd0422599a259af6b287fec5863f09279'), 'app/label_workbench_history.py': ('214ee81fdc36cfc51d6b59c15ee69f4f5057ebfc899eced18f7b61c0daff50f8', '30743e22b77939bed5d79b1e490c6df76ab577a242327a2d281f164c93ef1ca5'), 'app/main.py': ('520c2eaea01876f324288f01fee6f7600dd45d27829fb0e374969f6e2bf2bb6c', 'f85e46a1779eba99ac26609a172e8e4fad55e4e4115ee314bc5e1e66f4cf75ab'), 'app/package_command_draft.py': ('7f296c95fbcdfdd117eb275068fcb59e3dcc91f63112fb0e7b8fc421a2a3e151', '8144882b47008ee3e2c35cbd62bdf70637d34d89a5e1ade7620ef405cc7423ac'), 'app/package_logistics.py': ('818a1d7fa228e125c30c23f6950b6d579c69c22fda1bd9f2d9aded9e9b4201a1', '732b2449cee55b15e9c687a542814f76c5dc0a0c49337ec018194caa7960b3c1'), 'app/package_outbox.py': ('39b09189d4afddcb4bf937a8d300573391281092b6e62ac828ef41f1f72b6437', 'dc36a36e1697d1d3a42c5f0b4bf147791fa6687444a80d912778c83bd049f683'), 'app/package_outbox_processor.py': ('4589f804c3d57e56ad93066d83d8c122b3d3eec9510aee9c2152375f8c0c3f79', 'bc8cbc44c0b573df1a658fc09ec980e3965196680d8c7294daa52846b334ded1'), 'app/sealed_transfer_exchange.py': ('f0452daf434ae11710a9e5cfa825478d9ceb552fadc804be269e5e0b2d26fca2', '1202d1fecf10ed6e6d6e7662ca9c87ff4aa2553d14e4f8fa67210f770b8ab755'), 'app/ui/workflow_view_state.py': ('45066b34a856ca47c3aa7f10b44f72e948a2db5827a8f6da948eeb7d6a8a5e0a', 'ca42f4a1a106b60a39fe07ee74f78b3fdc4b3935986ecd6b23140c7f4c768a68'), 'app/user_relay.py': ('07291437ab724a9512332b25666099f13170af2f12f5f62328ea6af54cad96e7', '1015c0d1026d69745d70bbe1d69ab0f5a97cec52eaf8831ecc95d0356d8f4d3b'), 'app/writer_session_fence.py': ('503dae7aa0250925287745c61a82b9268da59b2855f87d9ba1bd638b724b9cfb', '92419d83453e49cd225e79ccc1aa96230c7ba00636b7fd114ed02736e6c074e1'), 'tools/label_writer_fence.ps1': ('2406ca2a3c36016007f14a940ad5f432b7736c62e30d4414f8c1ead4de55d034', '44ad315d0ffe526f003e3d8d17ff0940338652e7e557ad22ed6e728551ce3d3f'), 'tools/label_writer_fence_contract.json': ('bc95bd4ae6984f08120d7284f1ef73e7f1369e8fae078f6d60c66144a5cb24de', '8a07c41b748669e237fc7f35be75daede0fca9f86c2d1af07ff7a701a33126dd'), 'app/current_user_scheduled_task.py': ('5b339cae5a61c67bf3187f75ae254c38df4414bf7c0f5f3b8ed24f3cff52e870', 'ad9e95381b52e57cfdecd84373b61ae5449ac8edeb9fef33199432896d0b1417'), 'app/direct_sync_push.py': ('86c182b049dfffb9bc2256d140b8dbaa95aea631abcdf5b4f3da244b696643bd', '7556f245d8724729d70e0639f6c5fcec06cc77e0f3e8b1f871278c585cd45666'), 'app/kmtech_shared.lock.json': ('87cf641db77b8052ffbaeee86d0c2cbab84f07ce4c3748c796985be2eacc0cf9', '14ff6bbc353aab8fa9ce88297c79f2ca5504c554e0edb9671eb62d1766395e9f'), 'app/kmtech_shared.manifest.json': ('feaed459688e915eb5f52f264501a4287261d9dcbdbc50e0c6f4bde59d8e7117', 'de1aa3a5a2b072ec15ec2b3d60733881dcf4d5c87f14b15ed06691ba643ac918'), 'app/label_admin_pin.py': ('8e5fb578366c38012792f5a3213310d6d442e4ff68202edf84e4983872f13bf0', '9be9247c439a99c696cb472f0bee0eee85b2fe1a98ee2a25aabe6395aff7a617'), 'app/label_match_single_instance.py': ('64040b936adb201d21ecbf55d47536b545a8c10048200a4337635f0a7b00bb02', 'f16db6f775d66233f96e274ea9b75260b6694800f60152e4a9edb133e9de93ee'), 'app/phs_label_workflow.py': ('c1746fe550017bccdf8b8fd6b98ed1438cd49b5878d98328cceec7d6cb6edd7c', '72bb834611c5172e3cb9a0c517b49e4a664adcffef05006f90015a6384584ae0'), 'app/producer_runtime_client.py': ('c5c111e72d150395b825ac66e79208540560495703039d0ea4c4f215eb45b2f9', 'd575094842ab4bb126f16d5372074aad03e20e4c0163f2ab0ffc5b10f77e271c'), 'tools/bootstrap_integrity.ps1': ('7cf77e9cc833093aed9ed7b3d79565f5396eed0288b1e7729e08959e82e12ff3', '96101ce38befdf9dcca09ec42abd571aa441ad312f35bf3d14fd7effc97db963'), 'app/kmtech_shared/__init__.py': ('32f34fccf9a00e5d27e852a95fc7b776f18a5eccda991f47dc33cd8111d50295', '7714a7a02cda3da34c4972101c0e01cc052435a00d8d520b5c274a05aa9850e8'), 'app/tools/direct_sync_relay_runner.py': ('c797ffeee38f5161621e526c4ef2f3844cbe848ea0124f875f3df05a1779af98', '9b8e7a9845c4e73edea64b4ac3d0b90e3f461b7d926e0e1db64bdb9197f734ac'), 'app/tools/register_label_match_worker_pc.py': ('469c2e2ebdc9ddb45973e1423336bd6619b485e1604732d4d7bec55c41f0931f', 'fbb7397c227cf6845e01124119cafc0081b9a0aa4d50c44703d339fc5dd8271b'), 'app/tools/direct_sync_relay_install_pack.py': ('5a4368f2bb692dd37f406f449e607390024668a8101929577d88c549ad7093e5', '53cc1d41e90b0b9cda364ab744ce81abeaf720d222a7fe6263bd15728ec77d7f'), 'app/direct_sync_runtime.py': ('50ff189da613b1e1bec5bbb32061acc76e8d29ecae8808f5f591854a361be033', '0a4d8ddb463a8f5d52cbe60cfbfaca2aa22c06fa1c89831944cc5c54df854d04'), 'app/item_catalog_sync.py': ('41c33bf6a473cc147db6e522b1d20d4771cc9bd92c74fa3a8bd091a141907387', '7ba452c285d2f41fea202dedd68fedbe15d514a8d632bf6818f27ac54851c3d4'), 'app/label_guarded_runtime_reconcile.py': ('5f2d668622236950e307e5dc964303f78c7074d3f4bf2c2a37018c2fffe46016', 'dccab991a4caa38b7cefc59aa92357393219131232f3252c50d23dc7240a8a1e'), 'app/logistics_runtime_profile.py': ('1d786cb74812b1b4588d122b333cc9b8724d28e7410f91c7c0f025d9fd1bd5ee', 'db92068a17237e47d3025bece8d8f479fbe5eca46fde86e90666e3cac49fab72'), 'app/protected_admin.py': ('f4b1616dc152c84e368a32acb06b424b4b4cdab588635e383fa22e7ee6d3c58e', 'eadfb46474595075b840bbdf87b919910c228ac5481fe62721a3497c478d63f9'), 'app/registration_relay_recovery.py': ('25921c0e33df76ef1c52655d95f2482906d444409d5c4a4d602e3745ff8d721b', '013758e5ab7f9084eae87651fd088a667e240841384deae3140858d28998f144'), 'app/tools/direct_sync_relay_operator.py': ('ff97593c29669ad1f983d6f7be0fb1ebabf27adcf9661b797d36f7d583a0cedb', '3c990ebfd9ff356d5876b40d1098470d0602fae3a3ed838ccf0cbc644150032f'), 'app/tools/install_logistics_runtime_profile.py': ('eb8650ace3df179a7c68f3247efe5eac86200543a43f4b2c80a47ccc95b336ef', '7dde5875649e9ec9291a45e47191b9c6fcf6b32d66cafe70c0444390cefbab9e'), 'app/tools/label_auth_recovery_canary.py': ('440a0ed5061b522cafa56408092e3ed361b42f1ae09569258b08005a04cd463b', '079dfe927bcf721f4db49779eeab96b66093e1e02c69b86da73ea069d5b7c9be'), 'app/tools/label_exact_clone_resolution_receipt.py': ('c77e6be851f32cad9058ee52892a9b2520232a15914379aba3c28ad09c581a9b', 'd1e0a8955eed3f056cb964d4f0058f082664740b03b98eefc8f5ea872ded987c'), 'app/tools/label_legacy_task_quiescence.py': ('02a422b3d5b2662a061ea79d88fd6d8da409d6daba92ffe22266e796da20dfe4', 'c9db34c66056e3e63eca603e3c1997f12367b58b6e748941b8d80a5fb7981b7e'), 'app/deferred_intent_capture.py': ('970c6565a3f5f589f15d9cc2fb0ff1e5980e772d977976152b93f3c7e6e41dbd', 'f1dc02ba1711847228945fe6a01c3881aef6e71441a85cbf5873dbf72700d72e'), 'app/label_exact_clone_resolution.py': ('0ff69b167d2b88e9be8d5a6bc068b6f160b4a19023a92b401881fc86cf7b24e4', 'f162f2c2680852cfa1ec52d4a355b275fcb87482561bb84aac89aa24e05696ce'), 'app/label_workbench_activity.py': ('42ddafdfe2a45ab60f9eb0ffca4f73d4df2efbbc15f9eca1c12628f0e22c8706', 'b94ea10dbbfc75debb375386af285e43f87231ff4a14c42f1da845f6833fa152'), 'app/label_workbench_context.py': ('32835bfbd588afd84fae494aabeab2add65f2ecb0b22aa0bb2478951107afe51', '7139f01df5558e89718c00fb65a95d83840b0c473ae747643250872eb0511f69'), 'app/label_workbench_scan.py': ('5705832318e7954554b4bd94de77b1168e63622580d7bcc49fa5b4eb534ff271', '0d7201fabded66e0bbd620dc6fd7d98f2d8b747e0c7e17a4f8b224d66d049d52')}, 'c83c72c26fd4352803500792abd27096b28c58de': {'INSTALL_THIS_PC.ps1': ('8efe37d3203c94c71dff93a0761a674f400154b7b2d5c7bef8a91e2d09e5b702', 'bdc42c26a8b5464d8a65e880ba5474ef01a28c820997adcb5ddefa8daa9b022b'), 'app/Label_Match.py': ('27c03a98ca225d82f85e95bc546e0b57c984d390db73532f46191727e9b1f1b5', '43c93e151623c6098ab6dc0b200d5d44c415cdbe12f2e3be97c3172c41d270e9'), 'app/carrier_identity_port.py': ('08e470a49aef5213f458f352df2118cf79e01160ba2b0544091161358717584d', '3690500795de424c928570593debb4afcfc50d1175f2ccba77f44fb56c97979b'), 'app/config/app_settings.json': ('081e0ab6e932035312c9bc225db26589e4c694cd2c40170ca459443e1a5460e8', 'e65540f3325c70866753cd0abfe01c21edb89f6fa05cd1ddc1666a89af14c34b'), 'app/current_user_onboarding.py': ('323c4100d3ee0d32077232be2656209daa581f9e19ca018d3e840009d03dc26e', '93a4d7b67b9d740ddbf8bfa9546f01fc0c927ee0de985f4be4e0d83adf435ebd'), 'app/deferred_intent_capture.py': ('e129488f303e001293431e8b45a0edc10bab22a7e9cb7e96782aa6d3df260561', 'f1dc02ba1711847228945fe6a01c3881aef6e71441a85cbf5873dbf72700d72e'), 'app/enrollment_mutex.py': ('040294978e4341d9f06573f9117d99913f1dab8ef85de5f08a5296228fbb9787', 'ce77c0a1ab113326dffdb611a74c19308a48fc1b113626509960614fd3295981'), 'app/label_completion.py': ('aaac26de64e050b07b2225723898c8c32e52f6526039202ffd8d2daa12f162b8', 'ed37d0733e62e375fcd85f873b2459f5fd6a3e7d68ced48f36200cb5e871c0ac'), 'app/label_data_manager.py': ('95887ca72e74aa258ba07584e1df14fc099f4b16998629d4639e4f5749ecc757', '2ddd2129d6de99073f3cd51d8947a4792ea56e371205f3ea70445301317f4e56'), 'app/label_match_product_host.py': ('bce7176d636a93c559340850d91894ffd02a4eef2a3f953d2ae05cd1cb0f9fbf', '2f68dda99e3bc9e0d6e1c325cb43d169845df3501050a354c624a44690097845'), 'app/label_match_single_instance.py': ('3c69b65c5091c9d55bd28bebacad2a1fae5801a663cc7f762dacf1c3c68043e7', 'f16db6f775d66233f96e274ea9b75260b6694800f60152e4a9edb133e9de93ee'), 'app/label_recovery_schema.py': ('e783a8c7ecac94dd415810f2891161967ec8f13a802d74d7da466e646986d462', '4bf2ad6024efd6c3ac2f5965eef3e81fa149078f4bc22ff716272a5c2359ffa6'), 'app/label_transition.py': ('001001427e7352c308b351263770b2649aa96898b712396d8ae3ae4530186cf9', '27c5952d804558211c7db05ac3489e4bd0422599a259af6b287fec5863f09279'), 'app/label_workbench_history.py': ('214ee81fdc36cfc51d6b59c15ee69f4f5057ebfc899eced18f7b61c0daff50f8', '30743e22b77939bed5d79b1e490c6df76ab577a242327a2d281f164c93ef1ca5'), 'app/logistics_runtime_profile.py': ('2cd3fe00a34e79a6c12a995f74ba99515f88fff4e9bef50e844ab02dc59da1ed', 'db92068a17237e47d3025bece8d8f479fbe5eca46fde86e90666e3cac49fab72'), 'app/main.py': ('520c2eaea01876f324288f01fee6f7600dd45d27829fb0e374969f6e2bf2bb6c', 'f85e46a1779eba99ac26609a172e8e4fad55e4e4115ee314bc5e1e66f4cf75ab'), 'app/package_command_draft.py': ('7f296c95fbcdfdd117eb275068fcb59e3dcc91f63112fb0e7b8fc421a2a3e151', '8144882b47008ee3e2c35cbd62bdf70637d34d89a5e1ade7620ef405cc7423ac'), 'app/package_logistics.py': ('818a1d7fa228e125c30c23f6950b6d579c69c22fda1bd9f2d9aded9e9b4201a1', '732b2449cee55b15e9c687a542814f76c5dc0a0c49337ec018194caa7960b3c1'), 'app/package_outbox.py': ('39b09189d4afddcb4bf937a8d300573391281092b6e62ac828ef41f1f72b6437', 'dc36a36e1697d1d3a42c5f0b4bf147791fa6687444a80d912778c83bd049f683'), 'app/package_outbox_processor.py': ('4589f804c3d57e56ad93066d83d8c122b3d3eec9510aee9c2152375f8c0c3f79', 'bc8cbc44c0b573df1a658fc09ec980e3965196680d8c7294daa52846b334ded1'), 'app/sealed_transfer_exchange.py': ('f0452daf434ae11710a9e5cfa825478d9ceb552fadc804be269e5e0b2d26fca2', '1202d1fecf10ed6e6d6e7662ca9c87ff4aa2553d14e4f8fa67210f770b8ab755'), 'app/terminal_operation_lease.py': ('4e30d014f82a2dea766ef735c39a3b9c0f4b9e4c521ce76b25c9ac8cfb3927b7', 'a1c5a01061c87b482f49707995f85672279efa190ebcc97bea3ff74f32f77bfa'), 'app/tools/install_logistics_runtime_profile.py': ('7be760a5e49054740b495e62d48da98259b7e40eb288d1a46604cf8201994122', '7dde5875649e9ec9291a45e47191b9c6fcf6b32d66cafe70c0444390cefbab9e'), 'app/ui/workflow_view_state.py': ('45066b34a856ca47c3aa7f10b44f72e948a2db5827a8f6da948eeb7d6a8a5e0a', 'ca42f4a1a106b60a39fe07ee74f78b3fdc4b3935986ecd6b23140c7f4c768a68'), 'app/user_relay.py': ('07291437ab724a9512332b25666099f13170af2f12f5f62328ea6af54cad96e7', '1015c0d1026d69745d70bbe1d69ab0f5a97cec52eaf8831ecc95d0356d8f4d3b'), 'app/writer_session_fence.py': ('7e26bf0c1cddd87df8fdffb5921887f8034976269308c8d5920543c30ba38ce1', '92419d83453e49cd225e79ccc1aa96230c7ba00636b7fd114ed02736e6c074e1'), 'tools/label_writer_fence.ps1': ('18204c0518d4e72d1443062546a7a03d2df47a78fc2a7d3cbb929540d05a55f9', '44ad315d0ffe526f003e3d8d17ff0940338652e7e557ad22ed6e728551ce3d3f'), 'tools/label_writer_fence_contract.json': ('bc95bd4ae6984f08120d7284f1ef73e7f1369e8fae078f6d60c66144a5cb24de', '8a07c41b748669e237fc7f35be75daede0fca9f86c2d1af07ff7a701a33126dd'), 'app/current_user_scheduled_task.py': ('5b339cae5a61c67bf3187f75ae254c38df4414bf7c0f5f3b8ed24f3cff52e870', 'ad9e95381b52e57cfdecd84373b61ae5449ac8edeb9fef33199432896d0b1417'), 'app/direct_sync_push.py': ('86c182b049dfffb9bc2256d140b8dbaa95aea631abcdf5b4f3da244b696643bd', '7556f245d8724729d70e0639f6c5fcec06cc77e0f3e8b1f871278c585cd45666'), 'app/kmtech_shared.lock.json': ('87cf641db77b8052ffbaeee86d0c2cbab84f07ce4c3748c796985be2eacc0cf9', '14ff6bbc353aab8fa9ce88297c79f2ca5504c554e0edb9671eb62d1766395e9f'), 'app/kmtech_shared.manifest.json': ('feaed459688e915eb5f52f264501a4287261d9dcbdbc50e0c6f4bde59d8e7117', 'de1aa3a5a2b072ec15ec2b3d60733881dcf4d5c87f14b15ed06691ba643ac918'), 'app/label_admin_pin.py': ('8e5fb578366c38012792f5a3213310d6d442e4ff68202edf84e4983872f13bf0', '9be9247c439a99c696cb472f0bee0eee85b2fe1a98ee2a25aabe6395aff7a617'), 'app/phs_label_workflow.py': ('c1746fe550017bccdf8b8fd6b98ed1438cd49b5878d98328cceec7d6cb6edd7c', '72bb834611c5172e3cb9a0c517b49e4a664adcffef05006f90015a6384584ae0'), 'app/producer_runtime_client.py': ('c5c111e72d150395b825ac66e79208540560495703039d0ea4c4f215eb45b2f9', 'd575094842ab4bb126f16d5372074aad03e20e4c0163f2ab0ffc5b10f77e271c'), 'tools/bootstrap_integrity.ps1': ('7cf77e9cc833093aed9ed7b3d79565f5396eed0288b1e7729e08959e82e12ff3', '96101ce38befdf9dcca09ec42abd571aa441ad312f35bf3d14fd7effc97db963'), 'app/kmtech_shared/__init__.py': ('32f34fccf9a00e5d27e852a95fc7b776f18a5eccda991f47dc33cd8111d50295', '7714a7a02cda3da34c4972101c0e01cc052435a00d8d520b5c274a05aa9850e8'), 'app/tools/direct_sync_relay_runner.py': ('c797ffeee38f5161621e526c4ef2f3844cbe848ea0124f875f3df05a1779af98', '9b8e7a9845c4e73edea64b4ac3d0b90e3f461b7d926e0e1db64bdb9197f734ac'), 'app/tools/register_label_match_worker_pc.py': ('469c2e2ebdc9ddb45973e1423336bd6619b485e1604732d4d7bec55c41f0931f', 'fbb7397c227cf6845e01124119cafc0081b9a0aa4d50c44703d339fc5dd8271b'), 'app/tools/direct_sync_relay_install_pack.py': ('5a4368f2bb692dd37f406f449e607390024668a8101929577d88c549ad7093e5', '53cc1d41e90b0b9cda364ab744ce81abeaf720d222a7fe6263bd15728ec77d7f'), 'app/direct_sync_runtime.py': ('50ff189da613b1e1bec5bbb32061acc76e8d29ecae8808f5f591854a361be033', '0a4d8ddb463a8f5d52cbe60cfbfaca2aa22c06fa1c89831944cc5c54df854d04'), 'app/item_catalog_sync.py': ('41c33bf6a473cc147db6e522b1d20d4771cc9bd92c74fa3a8bd091a141907387', '7ba452c285d2f41fea202dedd68fedbe15d514a8d632bf6818f27ac54851c3d4'), 'app/label_guarded_runtime_reconcile.py': ('5f2d668622236950e307e5dc964303f78c7074d3f4bf2c2a37018c2fffe46016', 'dccab991a4caa38b7cefc59aa92357393219131232f3252c50d23dc7240a8a1e'), 'app/protected_admin.py': ('f4b1616dc152c84e368a32acb06b424b4b4cdab588635e383fa22e7ee6d3c58e', 'eadfb46474595075b840bbdf87b919910c228ac5481fe62721a3497c478d63f9'), 'app/registration_relay_recovery.py': ('25921c0e33df76ef1c52655d95f2482906d444409d5c4a4d602e3745ff8d721b', '013758e5ab7f9084eae87651fd088a667e240841384deae3140858d28998f144'), 'app/tools/direct_sync_relay_operator.py': ('ff97593c29669ad1f983d6f7be0fb1ebabf27adcf9661b797d36f7d583a0cedb', '3c990ebfd9ff356d5876b40d1098470d0602fae3a3ed838ccf0cbc644150032f'), 'app/tools/label_auth_recovery_canary.py': ('440a0ed5061b522cafa56408092e3ed361b42f1ae09569258b08005a04cd463b', '079dfe927bcf721f4db49779eeab96b66093e1e02c69b86da73ea069d5b7c9be'), 'app/tools/label_exact_clone_resolution_receipt.py': ('c77e6be851f32cad9058ee52892a9b2520232a15914379aba3c28ad09c581a9b', 'd1e0a8955eed3f056cb964d4f0058f082664740b03b98eefc8f5ea872ded987c'), 'app/tools/label_legacy_task_quiescence.py': ('02a422b3d5b2662a061ea79d88fd6d8da409d6daba92ffe22266e796da20dfe4', 'c9db34c66056e3e63eca603e3c1997f12367b58b6e748941b8d80a5fb7981b7e'), 'app/label_exact_clone_resolution.py': ('0ff69b167d2b88e9be8d5a6bc068b6f160b4a19023a92b401881fc86cf7b24e4', 'f162f2c2680852cfa1ec52d4a355b275fcb87482561bb84aac89aa24e05696ce'), 'app/label_workbench_activity.py': ('42ddafdfe2a45ab60f9eb0ffca4f73d4df2efbbc15f9eca1c12628f0e22c8706', 'b94ea10dbbfc75debb375386af285e43f87231ff4a14c42f1da845f6833fa152'), 'app/label_workbench_context.py': ('32835bfbd588afd84fae494aabeab2add65f2ecb0b22aa0bb2478951107afe51', '7139f01df5558e89718c00fb65a95d83840b0c473ae747643250872eb0511f69'), 'app/label_workbench_scan.py': ('5705832318e7954554b4bd94de77b1168e63622580d7bcc49fa5b4eb534ff271', '0d7201fabded66e0bbd620dc6fd7d98f2d8b747e0c7e17a4f8b224d66d049d52')}, 'e88011c6fb6a773feb174ba29cad431edda54eeb': {'INSTALL_THIS_PC.ps1': ('8efe37d3203c94c71dff93a0761a674f400154b7b2d5c7bef8a91e2d09e5b702', 'bdc42c26a8b5464d8a65e880ba5474ef01a28c820997adcb5ddefa8daa9b022b'), 'app/Label_Match.py': ('caac070ce1bfa2218c5780fbab575d7c77f62381701f35d0b21b3f959e21d938', '43c93e151623c6098ab6dc0b200d5d44c415cdbe12f2e3be97c3172c41d270e9'), 'app/carrier_identity_port.py': ('08e470a49aef5213f458f352df2118cf79e01160ba2b0544091161358717584d', '3690500795de424c928570593debb4afcfc50d1175f2ccba77f44fb56c97979b'), 'app/config/app_settings.json': ('081e0ab6e932035312c9bc225db26589e4c694cd2c40170ca459443e1a5460e8', 'e65540f3325c70866753cd0abfe01c21edb89f6fa05cd1ddc1666a89af14c34b'), 'app/current_user_onboarding.py': ('323c4100d3ee0d32077232be2656209daa581f9e19ca018d3e840009d03dc26e', '93a4d7b67b9d740ddbf8bfa9546f01fc0c927ee0de985f4be4e0d83adf435ebd'), 'app/enrollment_mutex.py': ('040294978e4341d9f06573f9117d99913f1dab8ef85de5f08a5296228fbb9787', 'ce77c0a1ab113326dffdb611a74c19308a48fc1b113626509960614fd3295981'), 'app/label_completion.py': ('d0fb68fe1d531d6ac7436c8a778c7c93d6d1dbd4da0918614d95414ee5861492', 'ed37d0733e62e375fcd85f873b2459f5fd6a3e7d68ced48f36200cb5e871c0ac'), 'app/label_data_manager.py': ('95887ca72e74aa258ba07584e1df14fc099f4b16998629d4639e4f5749ecc757', '2ddd2129d6de99073f3cd51d8947a4792ea56e371205f3ea70445301317f4e56'), 'app/label_match_product_host.py': ('bce7176d636a93c559340850d91894ffd02a4eef2a3f953d2ae05cd1cb0f9fbf', '2f68dda99e3bc9e0d6e1c325cb43d169845df3501050a354c624a44690097845'), 'app/label_match_single_instance.py': ('3c69b65c5091c9d55bd28bebacad2a1fae5801a663cc7f762dacf1c3c68043e7', 'f16db6f775d66233f96e274ea9b75260b6694800f60152e4a9edb133e9de93ee'), 'app/label_recovery_schema.py': ('e783a8c7ecac94dd415810f2891161967ec8f13a802d74d7da466e646986d462', '4bf2ad6024efd6c3ac2f5965eef3e81fa149078f4bc22ff716272a5c2359ffa6'), 'app/label_transition.py': ('001001427e7352c308b351263770b2649aa96898b712396d8ae3ae4530186cf9', '27c5952d804558211c7db05ac3489e4bd0422599a259af6b287fec5863f09279'), 'app/label_workbench_history.py': ('214ee81fdc36cfc51d6b59c15ee69f4f5057ebfc899eced18f7b61c0daff50f8', '30743e22b77939bed5d79b1e490c6df76ab577a242327a2d281f164c93ef1ca5'), 'app/logistics_runtime_profile.py': ('2cd3fe00a34e79a6c12a995f74ba99515f88fff4e9bef50e844ab02dc59da1ed', 'db92068a17237e47d3025bece8d8f479fbe5eca46fde86e90666e3cac49fab72'), 'app/main.py': ('520c2eaea01876f324288f01fee6f7600dd45d27829fb0e374969f6e2bf2bb6c', 'f85e46a1779eba99ac26609a172e8e4fad55e4e4115ee314bc5e1e66f4cf75ab'), 'app/package_command_draft.py': ('7f296c95fbcdfdd117eb275068fcb59e3dcc91f63112fb0e7b8fc421a2a3e151', '8144882b47008ee3e2c35cbd62bdf70637d34d89a5e1ade7620ef405cc7423ac'), 'app/package_logistics.py': ('818a1d7fa228e125c30c23f6950b6d579c69c22fda1bd9f2d9aded9e9b4201a1', '732b2449cee55b15e9c687a542814f76c5dc0a0c49337ec018194caa7960b3c1'), 'app/package_outbox.py': ('39b09189d4afddcb4bf937a8d300573391281092b6e62ac828ef41f1f72b6437', 'dc36a36e1697d1d3a42c5f0b4bf147791fa6687444a80d912778c83bd049f683'), 'app/package_outbox_processor.py': ('4589f804c3d57e56ad93066d83d8c122b3d3eec9510aee9c2152375f8c0c3f79', 'bc8cbc44c0b573df1a658fc09ec980e3965196680d8c7294daa52846b334ded1'), 'app/sealed_transfer_exchange.py': ('f0452daf434ae11710a9e5cfa825478d9ceb552fadc804be269e5e0b2d26fca2', '1202d1fecf10ed6e6d6e7662ca9c87ff4aa2553d14e4f8fa67210f770b8ab755'), 'app/tools/install_logistics_runtime_profile.py': ('7be760a5e49054740b495e62d48da98259b7e40eb288d1a46604cf8201994122', '7dde5875649e9ec9291a45e47191b9c6fcf6b32d66cafe70c0444390cefbab9e'), 'app/ui/workflow_view_state.py': ('45066b34a856ca47c3aa7f10b44f72e948a2db5827a8f6da948eeb7d6a8a5e0a', 'ca42f4a1a106b60a39fe07ee74f78b3fdc4b3935986ecd6b23140c7f4c768a68'), 'app/user_relay.py': ('07291437ab724a9512332b25666099f13170af2f12f5f62328ea6af54cad96e7', '1015c0d1026d69745d70bbe1d69ab0f5a97cec52eaf8831ecc95d0356d8f4d3b'), 'app/writer_session_fence.py': ('96d32f3eab331cc14ccef610a5523d938ff34372f0ba1cfd9a5a6f9d779ee3ce', '92419d83453e49cd225e79ccc1aa96230c7ba00636b7fd114ed02736e6c074e1'), 'tools/label_writer_fence.ps1': ('d55c7eabc25a2260845893fe21da5df7d1075f2734db898632f9a46023a6a68e', '44ad315d0ffe526f003e3d8d17ff0940338652e7e557ad22ed6e728551ce3d3f'), 'tools/label_writer_fence_contract.json': ('bc95bd4ae6984f08120d7284f1ef73e7f1369e8fae078f6d60c66144a5cb24de', '8a07c41b748669e237fc7f35be75daede0fca9f86c2d1af07ff7a701a33126dd'), 'app/current_user_scheduled_task.py': ('5b339cae5a61c67bf3187f75ae254c38df4414bf7c0f5f3b8ed24f3cff52e870', 'ad9e95381b52e57cfdecd84373b61ae5449ac8edeb9fef33199432896d0b1417'), 'app/direct_sync_push.py': ('86c182b049dfffb9bc2256d140b8dbaa95aea631abcdf5b4f3da244b696643bd', '7556f245d8724729d70e0639f6c5fcec06cc77e0f3e8b1f871278c585cd45666'), 'app/kmtech_shared.lock.json': ('87cf641db77b8052ffbaeee86d0c2cbab84f07ce4c3748c796985be2eacc0cf9', '14ff6bbc353aab8fa9ce88297c79f2ca5504c554e0edb9671eb62d1766395e9f'), 'app/kmtech_shared.manifest.json': ('feaed459688e915eb5f52f264501a4287261d9dcbdbc50e0c6f4bde59d8e7117', 'de1aa3a5a2b072ec15ec2b3d60733881dcf4d5c87f14b15ed06691ba643ac918'), 'app/label_admin_pin.py': ('8e5fb578366c38012792f5a3213310d6d442e4ff68202edf84e4983872f13bf0', '9be9247c439a99c696cb472f0bee0eee85b2fe1a98ee2a25aabe6395aff7a617'), 'app/phs_label_workflow.py': ('c1746fe550017bccdf8b8fd6b98ed1438cd49b5878d98328cceec7d6cb6edd7c', '72bb834611c5172e3cb9a0c517b49e4a664adcffef05006f90015a6384584ae0'), 'app/producer_runtime_client.py': ('c5c111e72d150395b825ac66e79208540560495703039d0ea4c4f215eb45b2f9', 'd575094842ab4bb126f16d5372074aad03e20e4c0163f2ab0ffc5b10f77e271c'), 'tools/bootstrap_integrity.ps1': ('7cf77e9cc833093aed9ed7b3d79565f5396eed0288b1e7729e08959e82e12ff3', '96101ce38befdf9dcca09ec42abd571aa441ad312f35bf3d14fd7effc97db963'), 'app/kmtech_shared/__init__.py': ('32f34fccf9a00e5d27e852a95fc7b776f18a5eccda991f47dc33cd8111d50295', '7714a7a02cda3da34c4972101c0e01cc052435a00d8d520b5c274a05aa9850e8'), 'app/tools/direct_sync_relay_runner.py': ('c797ffeee38f5161621e526c4ef2f3844cbe848ea0124f875f3df05a1779af98', '9b8e7a9845c4e73edea64b4ac3d0b90e3f461b7d926e0e1db64bdb9197f734ac'), 'app/tools/register_label_match_worker_pc.py': ('469c2e2ebdc9ddb45973e1423336bd6619b485e1604732d4d7bec55c41f0931f', 'fbb7397c227cf6845e01124119cafc0081b9a0aa4d50c44703d339fc5dd8271b'), 'app/tools/direct_sync_relay_install_pack.py': ('5a4368f2bb692dd37f406f449e607390024668a8101929577d88c549ad7093e5', '53cc1d41e90b0b9cda364ab744ce81abeaf720d222a7fe6263bd15728ec77d7f'), 'app/direct_sync_runtime.py': ('50ff189da613b1e1bec5bbb32061acc76e8d29ecae8808f5f591854a361be033', '0a4d8ddb463a8f5d52cbe60cfbfaca2aa22c06fa1c89831944cc5c54df854d04'), 'app/item_catalog_sync.py': ('41c33bf6a473cc147db6e522b1d20d4771cc9bd92c74fa3a8bd091a141907387', '7ba452c285d2f41fea202dedd68fedbe15d514a8d632bf6818f27ac54851c3d4'), 'app/label_guarded_runtime_reconcile.py': ('5f2d668622236950e307e5dc964303f78c7074d3f4bf2c2a37018c2fffe46016', 'dccab991a4caa38b7cefc59aa92357393219131232f3252c50d23dc7240a8a1e'), 'app/protected_admin.py': ('f4b1616dc152c84e368a32acb06b424b4b4cdab588635e383fa22e7ee6d3c58e', 'eadfb46474595075b840bbdf87b919910c228ac5481fe62721a3497c478d63f9'), 'app/registration_relay_recovery.py': ('25921c0e33df76ef1c52655d95f2482906d444409d5c4a4d602e3745ff8d721b', '013758e5ab7f9084eae87651fd088a667e240841384deae3140858d28998f144'), 'app/tools/direct_sync_relay_operator.py': ('ff97593c29669ad1f983d6f7be0fb1ebabf27adcf9661b797d36f7d583a0cedb', '3c990ebfd9ff356d5876b40d1098470d0602fae3a3ed838ccf0cbc644150032f'), 'app/tools/label_auth_recovery_canary.py': ('440a0ed5061b522cafa56408092e3ed361b42f1ae09569258b08005a04cd463b', '079dfe927bcf721f4db49779eeab96b66093e1e02c69b86da73ea069d5b7c9be'), 'app/tools/label_exact_clone_resolution_receipt.py': ('c77e6be851f32cad9058ee52892a9b2520232a15914379aba3c28ad09c581a9b', 'd1e0a8955eed3f056cb964d4f0058f082664740b03b98eefc8f5ea872ded987c'), 'app/tools/label_legacy_task_quiescence.py': ('02a422b3d5b2662a061ea79d88fd6d8da409d6daba92ffe22266e796da20dfe4', 'c9db34c66056e3e63eca603e3c1997f12367b58b6e748941b8d80a5fb7981b7e'), 'app/deferred_intent_capture.py': ('970c6565a3f5f589f15d9cc2fb0ff1e5980e772d977976152b93f3c7e6e41dbd', 'f1dc02ba1711847228945fe6a01c3881aef6e71441a85cbf5873dbf72700d72e'), 'app/label_exact_clone_resolution.py': ('0ff69b167d2b88e9be8d5a6bc068b6f160b4a19023a92b401881fc86cf7b24e4', 'f162f2c2680852cfa1ec52d4a355b275fcb87482561bb84aac89aa24e05696ce'), 'app/label_workbench_activity.py': ('42ddafdfe2a45ab60f9eb0ffca4f73d4df2efbbc15f9eca1c12628f0e22c8706', 'b94ea10dbbfc75debb375386af285e43f87231ff4a14c42f1da845f6833fa152'), 'app/label_workbench_context.py': ('32835bfbd588afd84fae494aabeab2add65f2ecb0b22aa0bb2478951107afe51', '7139f01df5558e89718c00fb65a95d83840b0c473ae747643250872eb0511f69'), 'app/label_workbench_scan.py': ('5705832318e7954554b4bd94de77b1168e63622580d7bcc49fa5b4eb534ff271', '0d7201fabded66e0bbd620dc6fd7d98f2d8b747e0c7e17a4f8b224d66d049d52')}, 'e89c18879f5bf59ddcd550152bf26ffedc4d687a': {'INSTALL_THIS_PC.ps1': ('8efe37d3203c94c71dff93a0761a674f400154b7b2d5c7bef8a91e2d09e5b702', 'bdc42c26a8b5464d8a65e880ba5474ef01a28c820997adcb5ddefa8daa9b022b'), 'app/current_user_onboarding.py': ('84fde787fe1bca5d3138c3cd243d9b13ffaf834ffc69a5414b39d0c2b81f905d', '93a4d7b67b9d740ddbf8bfa9546f01fc0c927ee0de985f4be4e0d83adf435ebd'), 'app/current_user_scheduled_task.py': ('5b339cae5a61c67bf3187f75ae254c38df4414bf7c0f5f3b8ed24f3cff52e870', 'ad9e95381b52e57cfdecd84373b61ae5449ac8edeb9fef33199432896d0b1417'), 'app/kmtech_shared.lock.json': ('87cf641db77b8052ffbaeee86d0c2cbab84f07ce4c3748c796985be2eacc0cf9', '14ff6bbc353aab8fa9ce88297c79f2ca5504c554e0edb9671eb62d1766395e9f'), 'app/kmtech_shared.manifest.json': ('feaed459688e915eb5f52f264501a4287261d9dcbdbc50e0c6f4bde59d8e7117', 'de1aa3a5a2b072ec15ec2b3d60733881dcf4d5c87f14b15ed06691ba643ac918'), 'app/label_data_manager.py': ('4443b2e817e54e1ba1bbd9065f49123ef1d55b40a7e00f485bc2cda6d5d3d912', '2ddd2129d6de99073f3cd51d8947a4792ea56e371205f3ea70445301317f4e56'), 'app/Label_Match.py': ('65ab1913f9720b59b49ef6e5799891cf02af1fd3a413d5308ca568fa9a899145', '43c93e151623c6098ab6dc0b200d5d44c415cdbe12f2e3be97c3172c41d270e9'), 'app/label_match_product_host.py': ('bce7176d636a93c559340850d91894ffd02a4eef2a3f953d2ae05cd1cb0f9fbf', '2f68dda99e3bc9e0d6e1c325cb43d169845df3501050a354c624a44690097845'), 'app/label_recovery_schema.py': ('5ddc38a222acaa6a2ca4b3bf23fc09f173b1e2646a599a667062df245cbf5349', '4bf2ad6024efd6c3ac2f5965eef3e81fa149078f4bc22ff716272a5c2359ffa6'), 'app/main.py': ('520c2eaea01876f324288f01fee6f7600dd45d27829fb0e374969f6e2bf2bb6c', 'f85e46a1779eba99ac26609a172e8e4fad55e4e4115ee314bc5e1e66f4cf75ab'), 'app/user_relay.py': ('3b71cb47a1a4a3253ac0602804e5770e40e6b674004174838d3a00016c3e8487', '1015c0d1026d69745d70bbe1d69ab0f5a97cec52eaf8831ecc95d0356d8f4d3b'), 'app/writer_session_fence.py': ('26d4d4900f55cafbfad24099065ecfdd6b275327e60b93ac63d67186a05e09e5', '92419d83453e49cd225e79ccc1aa96230c7ba00636b7fd114ed02736e6c074e1'), 'tools/bootstrap_integrity.ps1': ('7cf77e9cc833093aed9ed7b3d79565f5396eed0288b1e7729e08959e82e12ff3', '96101ce38befdf9dcca09ec42abd571aa441ad312f35bf3d14fd7effc97db963'), 'tools/label_writer_fence.ps1': ('fbff47dd51d1d4503ef0dd0e9ec6796adaf943882f6771a19e85059d694de976', '44ad315d0ffe526f003e3d8d17ff0940338652e7e557ad22ed6e728551ce3d3f'), 'tools/label_writer_fence_contract.json': ('bc95bd4ae6984f08120d7284f1ef73e7f1369e8fae078f6d60c66144a5cb24de', '8a07c41b748669e237fc7f35be75daede0fca9f86c2d1af07ff7a701a33126dd'), 'app/config/app_settings.json': ('081e0ab6e932035312c9bc225db26589e4c694cd2c40170ca459443e1a5460e8', 'e65540f3325c70866753cd0abfe01c21edb89f6fa05cd1ddc1666a89af14c34b'), 'app/kmtech_shared/__init__.py': ('32f34fccf9a00e5d27e852a95fc7b776f18a5eccda991f47dc33cd8111d50295', '7714a7a02cda3da34c4972101c0e01cc052435a00d8d520b5c274a05aa9850e8'), 'app/tools/direct_sync_relay_runner.py': ('c797ffeee38f5161621e526c4ef2f3844cbe848ea0124f875f3df05a1779af98', '9b8e7a9845c4e73edea64b4ac3d0b90e3f461b7d926e0e1db64bdb9197f734ac'), 'app/tools/register_label_match_worker_pc.py': ('469c2e2ebdc9ddb45973e1423336bd6619b485e1604732d4d7bec55c41f0931f', 'fbb7397c227cf6845e01124119cafc0081b9a0aa4d50c44703d339fc5dd8271b'), 'app/tools/direct_sync_relay_install_pack.py': ('5a4368f2bb692dd37f406f449e607390024668a8101929577d88c549ad7093e5', '53cc1d41e90b0b9cda364ab744ce81abeaf720d222a7fe6263bd15728ec77d7f'), 'app/direct_sync_push.py': ('d9c21b3209dc35e380daeb045a51e84dd81fb99893b471e83711116ef9d06e6d', '7556f245d8724729d70e0639f6c5fcec06cc77e0f3e8b1f871278c585cd45666'), 'app/direct_sync_runtime.py': ('50ff189da613b1e1bec5bbb32061acc76e8d29ecae8808f5f591854a361be033', '0a4d8ddb463a8f5d52cbe60cfbfaca2aa22c06fa1c89831944cc5c54df854d04'), 'app/item_catalog_sync.py': ('41c33bf6a473cc147db6e522b1d20d4771cc9bd92c74fa3a8bd091a141907387', '7ba452c285d2f41fea202dedd68fedbe15d514a8d632bf6818f27ac54851c3d4'), 'app/label_admin_pin.py': ('25ff059f390f88166e74518a8027633ab75d6954ceb8f8ee74593725bf828f75', '9be9247c439a99c696cb472f0bee0eee85b2fe1a98ee2a25aabe6395aff7a617'), 'app/label_guarded_runtime_reconcile.py': ('5f2d668622236950e307e5dc964303f78c7074d3f4bf2c2a37018c2fffe46016', 'dccab991a4caa38b7cefc59aa92357393219131232f3252c50d23dc7240a8a1e'), 'app/logistics_runtime_profile.py': ('1d786cb74812b1b4588d122b333cc9b8724d28e7410f91c7c0f025d9fd1bd5ee', 'db92068a17237e47d3025bece8d8f479fbe5eca46fde86e90666e3cac49fab72'), 'app/package_logistics.py': ('6899fd407c99066ba52ac698af0f01dbe762a56b300a6c28f1c6e56958dd5ef3', '732b2449cee55b15e9c687a542814f76c5dc0a0c49337ec018194caa7960b3c1'), 'app/producer_runtime_client.py': ('e47cd333cc3c743fe9cd30a6de4069929773e86bfbb23babdeaff287564e8f17', 'd575094842ab4bb126f16d5372074aad03e20e4c0163f2ab0ffc5b10f77e271c'), 'app/protected_admin.py': ('f4b1616dc152c84e368a32acb06b424b4b4cdab588635e383fa22e7ee6d3c58e', 'eadfb46474595075b840bbdf87b919910c228ac5481fe62721a3497c478d63f9'), 'app/registration_relay_recovery.py': ('25921c0e33df76ef1c52655d95f2482906d444409d5c4a4d602e3745ff8d721b', '013758e5ab7f9084eae87651fd088a667e240841384deae3140858d28998f144'), 'app/tools/direct_sync_relay_operator.py': ('ff97593c29669ad1f983d6f7be0fb1ebabf27adcf9661b797d36f7d583a0cedb', '3c990ebfd9ff356d5876b40d1098470d0602fae3a3ed838ccf0cbc644150032f'), 'app/tools/install_logistics_runtime_profile.py': ('eb8650ace3df179a7c68f3247efe5eac86200543a43f4b2c80a47ccc95b336ef', '7dde5875649e9ec9291a45e47191b9c6fcf6b32d66cafe70c0444390cefbab9e'), 'app/tools/label_auth_recovery_canary.py': ('440a0ed5061b522cafa56408092e3ed361b42f1ae09569258b08005a04cd463b', '079dfe927bcf721f4db49779eeab96b66093e1e02c69b86da73ea069d5b7c9be'), 'app/tools/label_exact_clone_resolution_receipt.py': ('c77e6be851f32cad9058ee52892a9b2520232a15914379aba3c28ad09c581a9b', 'd1e0a8955eed3f056cb964d4f0058f082664740b03b98eefc8f5ea872ded987c'), 'app/tools/label_legacy_task_quiescence.py': ('02a422b3d5b2662a061ea79d88fd6d8da409d6daba92ffe22266e796da20dfe4', 'c9db34c66056e3e63eca603e3c1997f12367b58b6e748941b8d80a5fb7981b7e'), 'app/deferred_intent_capture.py': ('970c6565a3f5f589f15d9cc2fb0ff1e5980e772d977976152b93f3c7e6e41dbd', 'f1dc02ba1711847228945fe6a01c3881aef6e71441a85cbf5873dbf72700d72e'), 'app/label_exact_clone_resolution.py': ('0ff69b167d2b88e9be8d5a6bc068b6f160b4a19023a92b401881fc86cf7b24e4', 'f162f2c2680852cfa1ec52d4a355b275fcb87482561bb84aac89aa24e05696ce'), 'app/label_workbench_activity.py': ('42ddafdfe2a45ab60f9eb0ffca4f73d4df2efbbc15f9eca1c12628f0e22c8706', 'b94ea10dbbfc75debb375386af285e43f87231ff4a14c42f1da845f6833fa152'), 'app/label_workbench_context.py': ('32835bfbd588afd84fae494aabeab2add65f2ecb0b22aa0bb2478951107afe51', '7139f01df5558e89718c00fb65a95d83840b0c473ae747643250872eb0511f69'), 'app/label_workbench_scan.py': ('5705832318e7954554b4bd94de77b1168e63622580d7bcc49fa5b4eb534ff271', '0d7201fabded66e0bbd620dc6fd7d98f2d8b747e0c7e17a4f8b224d66d049d52'), 'app/ui/workflow_view_state.py': ('b914aece96760c7e149876d56ccd24b8efae2f33e68a23a3a5a183e1cdfc5242', 'ca42f4a1a106b60a39fe07ee74f78b3fdc4b3935986ecd6b23140c7f4c768a68')}, '0cf041446e6eb3cfecb64055c3affccb359c3685': {'INSTALL_THIS_PC.ps1': ('8efe37d3203c94c71dff93a0761a674f400154b7b2d5c7bef8a91e2d09e5b702', 'bdc42c26a8b5464d8a65e880ba5474ef01a28c820997adcb5ddefa8daa9b022b'), 'app/current_user_onboarding.py': ('84fde787fe1bca5d3138c3cd243d9b13ffaf834ffc69a5414b39d0c2b81f905d', '93a4d7b67b9d740ddbf8bfa9546f01fc0c927ee0de985f4be4e0d83adf435ebd'), 'app/current_user_scheduled_task.py': ('5b339cae5a61c67bf3187f75ae254c38df4414bf7c0f5f3b8ed24f3cff52e870', 'ad9e95381b52e57cfdecd84373b61ae5449ac8edeb9fef33199432896d0b1417'), 'app/kmtech_shared.lock.json': ('87cf641db77b8052ffbaeee86d0c2cbab84f07ce4c3748c796985be2eacc0cf9', '14ff6bbc353aab8fa9ce88297c79f2ca5504c554e0edb9671eb62d1766395e9f'), 'app/kmtech_shared.manifest.json': ('feaed459688e915eb5f52f264501a4287261d9dcbdbc50e0c6f4bde59d8e7117', 'de1aa3a5a2b072ec15ec2b3d60733881dcf4d5c87f14b15ed06691ba643ac918'), 'app/label_completion.py': ('7650944f6d9f7431d8089b373a32aa649c324ecacfb8c24b40f13ccc2f2fdb55', 'ed37d0733e62e375fcd85f873b2459f5fd6a3e7d68ced48f36200cb5e871c0ac'), 'app/label_data_manager.py': ('eb4f4bef03152c05c6ef160abb7738dfa8416c971bb79baac4092d52905c14ec', '2ddd2129d6de99073f3cd51d8947a4792ea56e371205f3ea70445301317f4e56'), 'app/Label_Match.py': ('5f16f57e6eaf9e73fc54c3b2f58eaf3f9f5841d9de2fbfd5a74a63c969e38c92', '43c93e151623c6098ab6dc0b200d5d44c415cdbe12f2e3be97c3172c41d270e9'), 'app/label_match_product_host.py': ('bce7176d636a93c559340850d91894ffd02a4eef2a3f953d2ae05cd1cb0f9fbf', '2f68dda99e3bc9e0d6e1c325cb43d169845df3501050a354c624a44690097845'), 'app/label_match_single_instance.py': ('64040b936adb201d21ecbf55d47536b545a8c10048200a4337635f0a7b00bb02', 'f16db6f775d66233f96e274ea9b75260b6694800f60152e4a9edb133e9de93ee'), 'app/label_recovery_schema.py': ('01fdfd2616bdc6a2a713ed89f32f23b73db9fe2d2e86d90482023e9950a178f7', '4bf2ad6024efd6c3ac2f5965eef3e81fa149078f4bc22ff716272a5c2359ffa6'), 'app/label_worker_roster.py': ('17b35efc3fb9f9a4589590d1cc8b0accaf2eba1cb9a482ffeaf669e12dc6ce32', '63d8fef44564fa9666b4a5b7494339e0fd91692e94eea42e8cbbcbb81fe4b6bd'), 'app/main.py': ('520c2eaea01876f324288f01fee6f7600dd45d27829fb0e374969f6e2bf2bb6c', 'f85e46a1779eba99ac26609a172e8e4fad55e4e4115ee314bc5e1e66f4cf75ab'), 'app/package_command_draft.py': ('4936fdccd3af20e7f48c0fc0bdb3eb41e6baee073bf8dea90252adb487c60563', '8144882b47008ee3e2c35cbd62bdf70637d34d89a5e1ade7620ef405cc7423ac'), 'app/package_outbox_processor.py': ('b7dc5263d0d41715b82c3e8c54e95951007eedbefcb9a4a90900aa3a7407a0e3', 'bc8cbc44c0b573df1a658fc09ec980e3965196680d8c7294daa52846b334ded1'), 'app/sealed_transfer_exchange.py': ('74c5d2c126537b316b2f9d8d5127853c1e4217448fe3a2656f551eaafe529d1c', '1202d1fecf10ed6e6d6e7662ca9c87ff4aa2553d14e4f8fa67210f770b8ab755'), 'app/user_relay.py': ('3b71cb47a1a4a3253ac0602804e5770e40e6b674004174838d3a00016c3e8487', '1015c0d1026d69745d70bbe1d69ab0f5a97cec52eaf8831ecc95d0356d8f4d3b'), 'app/writer_session_fence.py': ('e649873611c442930e8d635c5836752ea1203273676a3269443a9bf403755bca', '92419d83453e49cd225e79ccc1aa96230c7ba00636b7fd114ed02736e6c074e1'), 'app/config/app_settings.json': ('081e0ab6e932035312c9bc225db26589e4c694cd2c40170ca459443e1a5460e8', 'e65540f3325c70866753cd0abfe01c21edb89f6fa05cd1ddc1666a89af14c34b'), 'app/kmtech_shared/__init__.py': ('32f34fccf9a00e5d27e852a95fc7b776f18a5eccda991f47dc33cd8111d50295', '7714a7a02cda3da34c4972101c0e01cc052435a00d8d520b5c274a05aa9850e8'), 'app/tools/direct_sync_relay_runner.py': ('c797ffeee38f5161621e526c4ef2f3844cbe848ea0124f875f3df05a1779af98', '9b8e7a9845c4e73edea64b4ac3d0b90e3f461b7d926e0e1db64bdb9197f734ac'), 'app/tools/register_label_match_worker_pc.py': ('469c2e2ebdc9ddb45973e1423336bd6619b485e1604732d4d7bec55c41f0931f', 'fbb7397c227cf6845e01124119cafc0081b9a0aa4d50c44703d339fc5dd8271b'), 'tools/bootstrap_integrity.ps1': ('7cf77e9cc833093aed9ed7b3d79565f5396eed0288b1e7729e08959e82e12ff3', '96101ce38befdf9dcca09ec42abd571aa441ad312f35bf3d14fd7effc97db963'), 'tools/label_writer_fence.ps1': ('e3256e6129ab819075d38cb582559a25d312683c57995e1cfde92dbae66feb89', '44ad315d0ffe526f003e3d8d17ff0940338652e7e557ad22ed6e728551ce3d3f'), 'tools/label_writer_fence_contract.json': ('bc95bd4ae6984f08120d7284f1ef73e7f1369e8fae078f6d60c66144a5cb24de', '8a07c41b748669e237fc7f35be75daede0fca9f86c2d1af07ff7a701a33126dd'), 'app/tools/direct_sync_relay_install_pack.py': ('5a4368f2bb692dd37f406f449e607390024668a8101929577d88c549ad7093e5', '53cc1d41e90b0b9cda364ab744ce81abeaf720d222a7fe6263bd15728ec77d7f'), 'app/direct_sync_push.py': ('d9c21b3209dc35e380daeb045a51e84dd81fb99893b471e83711116ef9d06e6d', '7556f245d8724729d70e0639f6c5fcec06cc77e0f3e8b1f871278c585cd45666'), 'app/direct_sync_runtime.py': ('50ff189da613b1e1bec5bbb32061acc76e8d29ecae8808f5f591854a361be033', '0a4d8ddb463a8f5d52cbe60cfbfaca2aa22c06fa1c89831944cc5c54df854d04'), 'app/item_catalog_sync.py': ('41c33bf6a473cc147db6e522b1d20d4771cc9bd92c74fa3a8bd091a141907387', '7ba452c285d2f41fea202dedd68fedbe15d514a8d632bf6818f27ac54851c3d4'), 'app/label_admin_pin.py': ('25ff059f390f88166e74518a8027633ab75d6954ceb8f8ee74593725bf828f75', '9be9247c439a99c696cb472f0bee0eee85b2fe1a98ee2a25aabe6395aff7a617'), 'app/label_guarded_runtime_reconcile.py': ('5f2d668622236950e307e5dc964303f78c7074d3f4bf2c2a37018c2fffe46016', 'dccab991a4caa38b7cefc59aa92357393219131232f3252c50d23dc7240a8a1e'), 'app/logistics_runtime_profile.py': ('1d786cb74812b1b4588d122b333cc9b8724d28e7410f91c7c0f025d9fd1bd5ee', 'db92068a17237e47d3025bece8d8f479fbe5eca46fde86e90666e3cac49fab72'), 'app/package_logistics.py': ('6899fd407c99066ba52ac698af0f01dbe762a56b300a6c28f1c6e56958dd5ef3', '732b2449cee55b15e9c687a542814f76c5dc0a0c49337ec018194caa7960b3c1'), 'app/producer_runtime_client.py': ('e47cd333cc3c743fe9cd30a6de4069929773e86bfbb23babdeaff287564e8f17', 'd575094842ab4bb126f16d5372074aad03e20e4c0163f2ab0ffc5b10f77e271c'), 'app/protected_admin.py': ('f4b1616dc152c84e368a32acb06b424b4b4cdab588635e383fa22e7ee6d3c58e', 'eadfb46474595075b840bbdf87b919910c228ac5481fe62721a3497c478d63f9'), 'app/registration_relay_recovery.py': ('25921c0e33df76ef1c52655d95f2482906d444409d5c4a4d602e3745ff8d721b', '013758e5ab7f9084eae87651fd088a667e240841384deae3140858d28998f144'), 'app/tools/direct_sync_relay_operator.py': ('ff97593c29669ad1f983d6f7be0fb1ebabf27adcf9661b797d36f7d583a0cedb', '3c990ebfd9ff356d5876b40d1098470d0602fae3a3ed838ccf0cbc644150032f'), 'app/tools/install_logistics_runtime_profile.py': ('eb8650ace3df179a7c68f3247efe5eac86200543a43f4b2c80a47ccc95b336ef', '7dde5875649e9ec9291a45e47191b9c6fcf6b32d66cafe70c0444390cefbab9e'), 'app/tools/label_auth_recovery_canary.py': ('440a0ed5061b522cafa56408092e3ed361b42f1ae09569258b08005a04cd463b', '079dfe927bcf721f4db49779eeab96b66093e1e02c69b86da73ea069d5b7c9be'), 'app/tools/label_exact_clone_resolution_receipt.py': ('c77e6be851f32cad9058ee52892a9b2520232a15914379aba3c28ad09c581a9b', 'd1e0a8955eed3f056cb964d4f0058f082664740b03b98eefc8f5ea872ded987c'), 'app/tools/label_legacy_task_quiescence.py': ('02a422b3d5b2662a061ea79d88fd6d8da409d6daba92ffe22266e796da20dfe4', 'c9db34c66056e3e63eca603e3c1997f12367b58b6e748941b8d80a5fb7981b7e'), 'app/deferred_intent_capture.py': ('970c6565a3f5f589f15d9cc2fb0ff1e5980e772d977976152b93f3c7e6e41dbd', 'f1dc02ba1711847228945fe6a01c3881aef6e71441a85cbf5873dbf72700d72e'), 'app/label_exact_clone_resolution.py': ('0ff69b167d2b88e9be8d5a6bc068b6f160b4a19023a92b401881fc86cf7b24e4', 'f162f2c2680852cfa1ec52d4a355b275fcb87482561bb84aac89aa24e05696ce'), 'app/label_workbench_activity.py': ('42ddafdfe2a45ab60f9eb0ffca4f73d4df2efbbc15f9eca1c12628f0e22c8706', 'b94ea10dbbfc75debb375386af285e43f87231ff4a14c42f1da845f6833fa152'), 'app/label_workbench_context.py': ('32835bfbd588afd84fae494aabeab2add65f2ecb0b22aa0bb2478951107afe51', '7139f01df5558e89718c00fb65a95d83840b0c473ae747643250872eb0511f69'), 'app/label_workbench_scan.py': ('5705832318e7954554b4bd94de77b1168e63622580d7bcc49fa5b4eb534ff271', '0d7201fabded66e0bbd620dc6fd7d98f2d8b747e0c7e17a4f8b224d66d049d52'), 'app/ui/workflow_view_state.py': ('b914aece96760c7e149876d56ccd24b8efae2f33e68a23a3a5a183e1cdfc5242', 'ca42f4a1a106b60a39fe07ee74f78b3fdc4b3935986ecd6b23140c7f4c768a68')}, '4b7630f2ca25a08933a9482d8fed6946e79df688': {'INSTALL_THIS_PC.ps1': ('8efe37d3203c94c71dff93a0761a674f400154b7b2d5c7bef8a91e2d09e5b702', 'bdc42c26a8b5464d8a65e880ba5474ef01a28c820997adcb5ddefa8daa9b022b'), 'app/current_user_onboarding.py': ('84fde787fe1bca5d3138c3cd243d9b13ffaf834ffc69a5414b39d0c2b81f905d', '93a4d7b67b9d740ddbf8bfa9546f01fc0c927ee0de985f4be4e0d83adf435ebd'), 'app/current_user_scheduled_task.py': ('5b339cae5a61c67bf3187f75ae254c38df4414bf7c0f5f3b8ed24f3cff52e870', 'ad9e95381b52e57cfdecd84373b61ae5449ac8edeb9fef33199432896d0b1417'), 'app/kmtech_shared.lock.json': ('87cf641db77b8052ffbaeee86d0c2cbab84f07ce4c3748c796985be2eacc0cf9', '14ff6bbc353aab8fa9ce88297c79f2ca5504c554e0edb9671eb62d1766395e9f'), 'app/kmtech_shared.manifest.json': ('feaed459688e915eb5f52f264501a4287261d9dcbdbc50e0c6f4bde59d8e7117', 'de1aa3a5a2b072ec15ec2b3d60733881dcf4d5c87f14b15ed06691ba643ac918'), 'app/label_data_manager.py': ('4443b2e817e54e1ba1bbd9065f49123ef1d55b40a7e00f485bc2cda6d5d3d912', '2ddd2129d6de99073f3cd51d8947a4792ea56e371205f3ea70445301317f4e56'), 'app/Label_Match.py': ('40d1367865b7cc0c85db8db6b3e6ab61932dc8ed14673d8a785279580d283e70', '43c93e151623c6098ab6dc0b200d5d44c415cdbe12f2e3be97c3172c41d270e9'), 'app/label_match_product_host.py': ('bce7176d636a93c559340850d91894ffd02a4eef2a3f953d2ae05cd1cb0f9fbf', '2f68dda99e3bc9e0d6e1c325cb43d169845df3501050a354c624a44690097845'), 'app/label_match_single_instance.py': ('64040b936adb201d21ecbf55d47536b545a8c10048200a4337635f0a7b00bb02', 'f16db6f775d66233f96e274ea9b75260b6694800f60152e4a9edb133e9de93ee'), 'app/label_recovery_schema.py': ('5ddc38a222acaa6a2ca4b3bf23fc09f173b1e2646a599a667062df245cbf5349', '4bf2ad6024efd6c3ac2f5965eef3e81fa149078f4bc22ff716272a5c2359ffa6'), 'app/main.py': ('520c2eaea01876f324288f01fee6f7600dd45d27829fb0e374969f6e2bf2bb6c', 'f85e46a1779eba99ac26609a172e8e4fad55e4e4115ee314bc5e1e66f4cf75ab'), 'app/user_relay.py': ('3b71cb47a1a4a3253ac0602804e5770e40e6b674004174838d3a00016c3e8487', '1015c0d1026d69745d70bbe1d69ab0f5a97cec52eaf8831ecc95d0356d8f4d3b'), 'app/writer_session_fence.py': ('bd007a223dda4e53195d9af3ed3c876b72fe6f7d49447d38ce30a830104c6ed8', '92419d83453e49cd225e79ccc1aa96230c7ba00636b7fd114ed02736e6c074e1'), 'app/config/app_settings.json': ('081e0ab6e932035312c9bc225db26589e4c694cd2c40170ca459443e1a5460e8', 'e65540f3325c70866753cd0abfe01c21edb89f6fa05cd1ddc1666a89af14c34b'), 'app/kmtech_shared/__init__.py': ('32f34fccf9a00e5d27e852a95fc7b776f18a5eccda991f47dc33cd8111d50295', '7714a7a02cda3da34c4972101c0e01cc052435a00d8d520b5c274a05aa9850e8'), 'app/tools/direct_sync_relay_runner.py': ('c797ffeee38f5161621e526c4ef2f3844cbe848ea0124f875f3df05a1779af98', '9b8e7a9845c4e73edea64b4ac3d0b90e3f461b7d926e0e1db64bdb9197f734ac'), 'app/tools/register_label_match_worker_pc.py': ('469c2e2ebdc9ddb45973e1423336bd6619b485e1604732d4d7bec55c41f0931f', 'fbb7397c227cf6845e01124119cafc0081b9a0aa4d50c44703d339fc5dd8271b'), 'tools/bootstrap_integrity.ps1': ('7cf77e9cc833093aed9ed7b3d79565f5396eed0288b1e7729e08959e82e12ff3', '96101ce38befdf9dcca09ec42abd571aa441ad312f35bf3d14fd7effc97db963'), 'tools/label_writer_fence.ps1': ('500955366215a113f3c0ffd284d86d6364aadb86e00b097a4cefedb3cbb1ff7e', '44ad315d0ffe526f003e3d8d17ff0940338652e7e557ad22ed6e728551ce3d3f'), 'tools/label_writer_fence_contract.json': ('bc95bd4ae6984f08120d7284f1ef73e7f1369e8fae078f6d60c66144a5cb24de', '8a07c41b748669e237fc7f35be75daede0fca9f86c2d1af07ff7a701a33126dd'), 'app/tools/direct_sync_relay_install_pack.py': ('5a4368f2bb692dd37f406f449e607390024668a8101929577d88c549ad7093e5', '53cc1d41e90b0b9cda364ab744ce81abeaf720d222a7fe6263bd15728ec77d7f'), 'app/direct_sync_push.py': ('d9c21b3209dc35e380daeb045a51e84dd81fb99893b471e83711116ef9d06e6d', '7556f245d8724729d70e0639f6c5fcec06cc77e0f3e8b1f871278c585cd45666'), 'app/direct_sync_runtime.py': ('50ff189da613b1e1bec5bbb32061acc76e8d29ecae8808f5f591854a361be033', '0a4d8ddb463a8f5d52cbe60cfbfaca2aa22c06fa1c89831944cc5c54df854d04'), 'app/item_catalog_sync.py': ('41c33bf6a473cc147db6e522b1d20d4771cc9bd92c74fa3a8bd091a141907387', '7ba452c285d2f41fea202dedd68fedbe15d514a8d632bf6818f27ac54851c3d4'), 'app/label_admin_pin.py': ('25ff059f390f88166e74518a8027633ab75d6954ceb8f8ee74593725bf828f75', '9be9247c439a99c696cb472f0bee0eee85b2fe1a98ee2a25aabe6395aff7a617'), 'app/label_guarded_runtime_reconcile.py': ('5f2d668622236950e307e5dc964303f78c7074d3f4bf2c2a37018c2fffe46016', 'dccab991a4caa38b7cefc59aa92357393219131232f3252c50d23dc7240a8a1e'), 'app/logistics_runtime_profile.py': ('1d786cb74812b1b4588d122b333cc9b8724d28e7410f91c7c0f025d9fd1bd5ee', 'db92068a17237e47d3025bece8d8f479fbe5eca46fde86e90666e3cac49fab72'), 'app/package_logistics.py': ('6899fd407c99066ba52ac698af0f01dbe762a56b300a6c28f1c6e56958dd5ef3', '732b2449cee55b15e9c687a542814f76c5dc0a0c49337ec018194caa7960b3c1'), 'app/producer_runtime_client.py': ('e47cd333cc3c743fe9cd30a6de4069929773e86bfbb23babdeaff287564e8f17', 'd575094842ab4bb126f16d5372074aad03e20e4c0163f2ab0ffc5b10f77e271c'), 'app/protected_admin.py': ('f4b1616dc152c84e368a32acb06b424b4b4cdab588635e383fa22e7ee6d3c58e', 'eadfb46474595075b840bbdf87b919910c228ac5481fe62721a3497c478d63f9'), 'app/registration_relay_recovery.py': ('25921c0e33df76ef1c52655d95f2482906d444409d5c4a4d602e3745ff8d721b', '013758e5ab7f9084eae87651fd088a667e240841384deae3140858d28998f144'), 'app/tools/direct_sync_relay_operator.py': ('ff97593c29669ad1f983d6f7be0fb1ebabf27adcf9661b797d36f7d583a0cedb', '3c990ebfd9ff356d5876b40d1098470d0602fae3a3ed838ccf0cbc644150032f'), 'app/tools/install_logistics_runtime_profile.py': ('eb8650ace3df179a7c68f3247efe5eac86200543a43f4b2c80a47ccc95b336ef', '7dde5875649e9ec9291a45e47191b9c6fcf6b32d66cafe70c0444390cefbab9e'), 'app/tools/label_auth_recovery_canary.py': ('440a0ed5061b522cafa56408092e3ed361b42f1ae09569258b08005a04cd463b', '079dfe927bcf721f4db49779eeab96b66093e1e02c69b86da73ea069d5b7c9be'), 'app/tools/label_exact_clone_resolution_receipt.py': ('c77e6be851f32cad9058ee52892a9b2520232a15914379aba3c28ad09c581a9b', 'd1e0a8955eed3f056cb964d4f0058f082664740b03b98eefc8f5ea872ded987c'), 'app/tools/label_legacy_task_quiescence.py': ('02a422b3d5b2662a061ea79d88fd6d8da409d6daba92ffe22266e796da20dfe4', 'c9db34c66056e3e63eca603e3c1997f12367b58b6e748941b8d80a5fb7981b7e'), 'app/deferred_intent_capture.py': ('970c6565a3f5f589f15d9cc2fb0ff1e5980e772d977976152b93f3c7e6e41dbd', 'f1dc02ba1711847228945fe6a01c3881aef6e71441a85cbf5873dbf72700d72e'), 'app/label_exact_clone_resolution.py': ('0ff69b167d2b88e9be8d5a6bc068b6f160b4a19023a92b401881fc86cf7b24e4', 'f162f2c2680852cfa1ec52d4a355b275fcb87482561bb84aac89aa24e05696ce'), 'app/label_workbench_activity.py': ('42ddafdfe2a45ab60f9eb0ffca4f73d4df2efbbc15f9eca1c12628f0e22c8706', 'b94ea10dbbfc75debb375386af285e43f87231ff4a14c42f1da845f6833fa152'), 'app/label_workbench_context.py': ('32835bfbd588afd84fae494aabeab2add65f2ecb0b22aa0bb2478951107afe51', '7139f01df5558e89718c00fb65a95d83840b0c473ae747643250872eb0511f69'), 'app/label_workbench_scan.py': ('5705832318e7954554b4bd94de77b1168e63622580d7bcc49fa5b4eb534ff271', '0d7201fabded66e0bbd620dc6fd7d98f2d8b747e0c7e17a4f8b224d66d049d52'), 'app/ui/workflow_view_state.py': ('b914aece96760c7e149876d56ccd24b8efae2f33e68a23a3a5a183e1cdfc5242', 'ca42f4a1a106b60a39fe07ee74f78b3fdc4b3935986ecd6b23140c7f4c768a68')}, '640d770e62d41236fa51cf0e38112c331b023fe8': {'INSTALL_THIS_PC.ps1': ('8efe37d3203c94c71dff93a0761a674f400154b7b2d5c7bef8a91e2d09e5b702', 'bdc42c26a8b5464d8a65e880ba5474ef01a28c820997adcb5ddefa8daa9b022b'), 'app/current_user_onboarding.py': ('84fde787fe1bca5d3138c3cd243d9b13ffaf834ffc69a5414b39d0c2b81f905d', '93a4d7b67b9d740ddbf8bfa9546f01fc0c927ee0de985f4be4e0d83adf435ebd'), 'app/current_user_scheduled_task.py': ('5b339cae5a61c67bf3187f75ae254c38df4414bf7c0f5f3b8ed24f3cff52e870', 'ad9e95381b52e57cfdecd84373b61ae5449ac8edeb9fef33199432896d0b1417'), 'app/kmtech_shared.lock.json': ('87cf641db77b8052ffbaeee86d0c2cbab84f07ce4c3748c796985be2eacc0cf9', '14ff6bbc353aab8fa9ce88297c79f2ca5504c554e0edb9671eb62d1766395e9f'), 'app/kmtech_shared.manifest.json': ('feaed459688e915eb5f52f264501a4287261d9dcbdbc50e0c6f4bde59d8e7117', 'de1aa3a5a2b072ec15ec2b3d60733881dcf4d5c87f14b15ed06691ba643ac918'), 'app/label_data_manager.py': ('4443b2e817e54e1ba1bbd9065f49123ef1d55b40a7e00f485bc2cda6d5d3d912', '2ddd2129d6de99073f3cd51d8947a4792ea56e371205f3ea70445301317f4e56'), 'app/Label_Match.py': ('75fdd2d1422825f52320d3373acd12366b244e8ca00aa531a052983c09694227', '43c93e151623c6098ab6dc0b200d5d44c415cdbe12f2e3be97c3172c41d270e9'), 'app/label_match_product_host.py': ('bce7176d636a93c559340850d91894ffd02a4eef2a3f953d2ae05cd1cb0f9fbf', '2f68dda99e3bc9e0d6e1c325cb43d169845df3501050a354c624a44690097845'), 'app/label_match_single_instance.py': ('2929ffe042131ccc8cc905dbbad084be0fbab0585d70100252d3ee682c66346b', 'f16db6f775d66233f96e274ea9b75260b6694800f60152e4a9edb133e9de93ee'), 'app/label_recovery_schema.py': ('5ddc38a222acaa6a2ca4b3bf23fc09f173b1e2646a599a667062df245cbf5349', '4bf2ad6024efd6c3ac2f5965eef3e81fa149078f4bc22ff716272a5c2359ffa6'), 'app/main.py': ('520c2eaea01876f324288f01fee6f7600dd45d27829fb0e374969f6e2bf2bb6c', 'f85e46a1779eba99ac26609a172e8e4fad55e4e4115ee314bc5e1e66f4cf75ab'), 'app/user_relay.py': ('3b71cb47a1a4a3253ac0602804e5770e40e6b674004174838d3a00016c3e8487', '1015c0d1026d69745d70bbe1d69ab0f5a97cec52eaf8831ecc95d0356d8f4d3b'), 'app/writer_session_fence.py': ('5dc1377d481970f75c6a6330a2c2ea9987b016de4cea7f86f5788299a23b0f73', '92419d83453e49cd225e79ccc1aa96230c7ba00636b7fd114ed02736e6c074e1'), 'app/config/app_settings.json': ('081e0ab6e932035312c9bc225db26589e4c694cd2c40170ca459443e1a5460e8', 'e65540f3325c70866753cd0abfe01c21edb89f6fa05cd1ddc1666a89af14c34b'), 'app/kmtech_shared/__init__.py': ('32f34fccf9a00e5d27e852a95fc7b776f18a5eccda991f47dc33cd8111d50295', '7714a7a02cda3da34c4972101c0e01cc052435a00d8d520b5c274a05aa9850e8'), 'app/tools/direct_sync_relay_runner.py': ('c797ffeee38f5161621e526c4ef2f3844cbe848ea0124f875f3df05a1779af98', '9b8e7a9845c4e73edea64b4ac3d0b90e3f461b7d926e0e1db64bdb9197f734ac'), 'app/tools/register_label_match_worker_pc.py': ('469c2e2ebdc9ddb45973e1423336bd6619b485e1604732d4d7bec55c41f0931f', 'fbb7397c227cf6845e01124119cafc0081b9a0aa4d50c44703d339fc5dd8271b'), 'tools/bootstrap_integrity.ps1': ('7cf77e9cc833093aed9ed7b3d79565f5396eed0288b1e7729e08959e82e12ff3', '96101ce38befdf9dcca09ec42abd571aa441ad312f35bf3d14fd7effc97db963'), 'tools/label_writer_fence.ps1': ('89dd2e7e3ac09fb125905a00f49e6ce3b2c6f9d5420b0d47535124336f36ecf9', '44ad315d0ffe526f003e3d8d17ff0940338652e7e557ad22ed6e728551ce3d3f'), 'tools/label_writer_fence_contract.json': ('bc95bd4ae6984f08120d7284f1ef73e7f1369e8fae078f6d60c66144a5cb24de', '8a07c41b748669e237fc7f35be75daede0fca9f86c2d1af07ff7a701a33126dd'), 'app/tools/direct_sync_relay_install_pack.py': ('5a4368f2bb692dd37f406f449e607390024668a8101929577d88c549ad7093e5', '53cc1d41e90b0b9cda364ab744ce81abeaf720d222a7fe6263bd15728ec77d7f'), 'app/direct_sync_push.py': ('d9c21b3209dc35e380daeb045a51e84dd81fb99893b471e83711116ef9d06e6d', '7556f245d8724729d70e0639f6c5fcec06cc77e0f3e8b1f871278c585cd45666'), 'app/direct_sync_runtime.py': ('50ff189da613b1e1bec5bbb32061acc76e8d29ecae8808f5f591854a361be033', '0a4d8ddb463a8f5d52cbe60cfbfaca2aa22c06fa1c89831944cc5c54df854d04'), 'app/item_catalog_sync.py': ('41c33bf6a473cc147db6e522b1d20d4771cc9bd92c74fa3a8bd091a141907387', '7ba452c285d2f41fea202dedd68fedbe15d514a8d632bf6818f27ac54851c3d4'), 'app/label_admin_pin.py': ('25ff059f390f88166e74518a8027633ab75d6954ceb8f8ee74593725bf828f75', '9be9247c439a99c696cb472f0bee0eee85b2fe1a98ee2a25aabe6395aff7a617'), 'app/label_guarded_runtime_reconcile.py': ('5f2d668622236950e307e5dc964303f78c7074d3f4bf2c2a37018c2fffe46016', 'dccab991a4caa38b7cefc59aa92357393219131232f3252c50d23dc7240a8a1e'), 'app/logistics_runtime_profile.py': ('1d786cb74812b1b4588d122b333cc9b8724d28e7410f91c7c0f025d9fd1bd5ee', 'db92068a17237e47d3025bece8d8f479fbe5eca46fde86e90666e3cac49fab72'), 'app/package_logistics.py': ('6899fd407c99066ba52ac698af0f01dbe762a56b300a6c28f1c6e56958dd5ef3', '732b2449cee55b15e9c687a542814f76c5dc0a0c49337ec018194caa7960b3c1'), 'app/producer_runtime_client.py': ('e47cd333cc3c743fe9cd30a6de4069929773e86bfbb23babdeaff287564e8f17', 'd575094842ab4bb126f16d5372074aad03e20e4c0163f2ab0ffc5b10f77e271c'), 'app/protected_admin.py': ('f4b1616dc152c84e368a32acb06b424b4b4cdab588635e383fa22e7ee6d3c58e', 'eadfb46474595075b840bbdf87b919910c228ac5481fe62721a3497c478d63f9'), 'app/registration_relay_recovery.py': ('25921c0e33df76ef1c52655d95f2482906d444409d5c4a4d602e3745ff8d721b', '013758e5ab7f9084eae87651fd088a667e240841384deae3140858d28998f144'), 'app/tools/direct_sync_relay_operator.py': ('ff97593c29669ad1f983d6f7be0fb1ebabf27adcf9661b797d36f7d583a0cedb', '3c990ebfd9ff356d5876b40d1098470d0602fae3a3ed838ccf0cbc644150032f'), 'app/tools/install_logistics_runtime_profile.py': ('eb8650ace3df179a7c68f3247efe5eac86200543a43f4b2c80a47ccc95b336ef', '7dde5875649e9ec9291a45e47191b9c6fcf6b32d66cafe70c0444390cefbab9e'), 'app/tools/label_auth_recovery_canary.py': ('440a0ed5061b522cafa56408092e3ed361b42f1ae09569258b08005a04cd463b', '079dfe927bcf721f4db49779eeab96b66093e1e02c69b86da73ea069d5b7c9be'), 'app/tools/label_exact_clone_resolution_receipt.py': ('c77e6be851f32cad9058ee52892a9b2520232a15914379aba3c28ad09c581a9b', 'd1e0a8955eed3f056cb964d4f0058f082664740b03b98eefc8f5ea872ded987c'), 'app/tools/label_legacy_task_quiescence.py': ('02a422b3d5b2662a061ea79d88fd6d8da409d6daba92ffe22266e796da20dfe4', 'c9db34c66056e3e63eca603e3c1997f12367b58b6e748941b8d80a5fb7981b7e'), 'app/deferred_intent_capture.py': ('970c6565a3f5f589f15d9cc2fb0ff1e5980e772d977976152b93f3c7e6e41dbd', 'f1dc02ba1711847228945fe6a01c3881aef6e71441a85cbf5873dbf72700d72e'), 'app/label_exact_clone_resolution.py': ('0ff69b167d2b88e9be8d5a6bc068b6f160b4a19023a92b401881fc86cf7b24e4', 'f162f2c2680852cfa1ec52d4a355b275fcb87482561bb84aac89aa24e05696ce'), 'app/label_workbench_activity.py': ('42ddafdfe2a45ab60f9eb0ffca4f73d4df2efbbc15f9eca1c12628f0e22c8706', 'b94ea10dbbfc75debb375386af285e43f87231ff4a14c42f1da845f6833fa152'), 'app/label_workbench_context.py': ('32835bfbd588afd84fae494aabeab2add65f2ecb0b22aa0bb2478951107afe51', '7139f01df5558e89718c00fb65a95d83840b0c473ae747643250872eb0511f69'), 'app/label_workbench_scan.py': ('5705832318e7954554b4bd94de77b1168e63622580d7bcc49fa5b4eb534ff271', '0d7201fabded66e0bbd620dc6fd7d98f2d8b747e0c7e17a4f8b224d66d049d52'), 'app/ui/workflow_view_state.py': ('b914aece96760c7e149876d56ccd24b8efae2f33e68a23a3a5a183e1cdfc5242', 'ca42f4a1a106b60a39fe07ee74f78b3fdc4b3935986ecd6b23140c7f4c768a68')}, '134bab3c2059ce500f752a3ba09eb83a6d2c0b40': {'INSTALL_THIS_PC.ps1': ('8efe37d3203c94c71dff93a0761a674f400154b7b2d5c7bef8a91e2d09e5b702', 'bdc42c26a8b5464d8a65e880ba5474ef01a28c820997adcb5ddefa8daa9b022b'), 'app/current_user_onboarding.py': ('84fde787fe1bca5d3138c3cd243d9b13ffaf834ffc69a5414b39d0c2b81f905d', '93a4d7b67b9d740ddbf8bfa9546f01fc0c927ee0de985f4be4e0d83adf435ebd'), 'app/current_user_scheduled_task.py': ('5b339cae5a61c67bf3187f75ae254c38df4414bf7c0f5f3b8ed24f3cff52e870', 'ad9e95381b52e57cfdecd84373b61ae5449ac8edeb9fef33199432896d0b1417'), 'app/kmtech_shared.lock.json': ('87cf641db77b8052ffbaeee86d0c2cbab84f07ce4c3748c796985be2eacc0cf9', '14ff6bbc353aab8fa9ce88297c79f2ca5504c554e0edb9671eb62d1766395e9f'), 'app/kmtech_shared.manifest.json': ('feaed459688e915eb5f52f264501a4287261d9dcbdbc50e0c6f4bde59d8e7117', 'de1aa3a5a2b072ec15ec2b3d60733881dcf4d5c87f14b15ed06691ba643ac918'), 'app/label_admin_pin.py': ('159b7f0b79c6bb54d56857140d8601424e0c2ef386bd9d16e08c784a8545ebb8', '9be9247c439a99c696cb472f0bee0eee85b2fe1a98ee2a25aabe6395aff7a617'), 'app/label_completion.py': ('7650944f6d9f7431d8089b373a32aa649c324ecacfb8c24b40f13ccc2f2fdb55', 'ed37d0733e62e375fcd85f873b2459f5fd6a3e7d68ced48f36200cb5e871c0ac'), 'app/label_data_manager.py': ('eb4f4bef03152c05c6ef160abb7738dfa8416c971bb79baac4092d52905c14ec', '2ddd2129d6de99073f3cd51d8947a4792ea56e371205f3ea70445301317f4e56'), 'app/Label_Match.py': ('6a516d5aa46847547f0355f3505447bcd3ae86abd521640d0691d9ac3e962062', '43c93e151623c6098ab6dc0b200d5d44c415cdbe12f2e3be97c3172c41d270e9'), 'app/label_match_product_host.py': ('bce7176d636a93c559340850d91894ffd02a4eef2a3f953d2ae05cd1cb0f9fbf', '2f68dda99e3bc9e0d6e1c325cb43d169845df3501050a354c624a44690097845'), 'app/label_match_single_instance.py': ('64040b936adb201d21ecbf55d47536b545a8c10048200a4337635f0a7b00bb02', 'f16db6f775d66233f96e274ea9b75260b6694800f60152e4a9edb133e9de93ee'), 'app/label_recovery_schema.py': ('01fdfd2616bdc6a2a713ed89f32f23b73db9fe2d2e86d90482023e9950a178f7', '4bf2ad6024efd6c3ac2f5965eef3e81fa149078f4bc22ff716272a5c2359ffa6'), 'app/label_workbench_history.py': ('6f07e89f69d66bb979abaef58aa4ff514fb839756bc9f4b5b727b5c0cf4d2022', '30743e22b77939bed5d79b1e490c6df76ab577a242327a2d281f164c93ef1ca5'), 'app/main.py': ('520c2eaea01876f324288f01fee6f7600dd45d27829fb0e374969f6e2bf2bb6c', 'f85e46a1779eba99ac26609a172e8e4fad55e4e4115ee314bc5e1e66f4cf75ab'), 'app/package_command_draft.py': ('4936fdccd3af20e7f48c0fc0bdb3eb41e6baee073bf8dea90252adb487c60563', '8144882b47008ee3e2c35cbd62bdf70637d34d89a5e1ade7620ef405cc7423ac'), 'app/package_logistics.py': ('4585720051d266fa077005456b0da590b273ad8ab29bc5391cff2e6aa9f74224', '732b2449cee55b15e9c687a542814f76c5dc0a0c49337ec018194caa7960b3c1'), 'app/package_outbox_processor.py': ('b7dc5263d0d41715b82c3e8c54e95951007eedbefcb9a4a90900aa3a7407a0e3', 'bc8cbc44c0b573df1a658fc09ec980e3965196680d8c7294daa52846b334ded1'), 'app/phs_label_workflow.py': ('0d44faf4128286d49fc4285d26b9901d563c86eba0fe62d669c53a4a382c7b7d', '72bb834611c5172e3cb9a0c517b49e4a664adcffef05006f90015a6384584ae0'), 'app/sealed_transfer_exchange.py': ('74c5d2c126537b316b2f9d8d5127853c1e4217448fe3a2656f551eaafe529d1c', '1202d1fecf10ed6e6d6e7662ca9c87ff4aa2553d14e4f8fa67210f770b8ab755'), 'app/user_relay.py': ('3b71cb47a1a4a3253ac0602804e5770e40e6b674004174838d3a00016c3e8487', '1015c0d1026d69745d70bbe1d69ab0f5a97cec52eaf8831ecc95d0356d8f4d3b'), 'app/writer_session_fence.py': ('8d7ae33010ffce1645868060c0bb049ca1f554781c37de03a678dd712e5d8dd6', '92419d83453e49cd225e79ccc1aa96230c7ba00636b7fd114ed02736e6c074e1'), 'app/config/app_settings.json': ('081e0ab6e932035312c9bc225db26589e4c694cd2c40170ca459443e1a5460e8', 'e65540f3325c70866753cd0abfe01c21edb89f6fa05cd1ddc1666a89af14c34b'), 'app/kmtech_shared/__init__.py': ('32f34fccf9a00e5d27e852a95fc7b776f18a5eccda991f47dc33cd8111d50295', '7714a7a02cda3da34c4972101c0e01cc052435a00d8d520b5c274a05aa9850e8'), 'app/tools/direct_sync_relay_runner.py': ('c797ffeee38f5161621e526c4ef2f3844cbe848ea0124f875f3df05a1779af98', '9b8e7a9845c4e73edea64b4ac3d0b90e3f461b7d926e0e1db64bdb9197f734ac'), 'app/tools/register_label_match_worker_pc.py': ('469c2e2ebdc9ddb45973e1423336bd6619b485e1604732d4d7bec55c41f0931f', 'fbb7397c227cf6845e01124119cafc0081b9a0aa4d50c44703d339fc5dd8271b'), 'tools/bootstrap_integrity.ps1': ('7cf77e9cc833093aed9ed7b3d79565f5396eed0288b1e7729e08959e82e12ff3', '96101ce38befdf9dcca09ec42abd571aa441ad312f35bf3d14fd7effc97db963'), 'tools/label_writer_fence.ps1': ('2795ccf12f76c7619fda186762303e02539714b7c33fbef49b65e89a1d06d67a', '44ad315d0ffe526f003e3d8d17ff0940338652e7e557ad22ed6e728551ce3d3f'), 'tools/label_writer_fence_contract.json': ('bc95bd4ae6984f08120d7284f1ef73e7f1369e8fae078f6d60c66144a5cb24de', '8a07c41b748669e237fc7f35be75daede0fca9f86c2d1af07ff7a701a33126dd'), 'app/tools/direct_sync_relay_install_pack.py': ('5a4368f2bb692dd37f406f449e607390024668a8101929577d88c549ad7093e5', '53cc1d41e90b0b9cda364ab744ce81abeaf720d222a7fe6263bd15728ec77d7f'), 'app/direct_sync_push.py': ('d9c21b3209dc35e380daeb045a51e84dd81fb99893b471e83711116ef9d06e6d', '7556f245d8724729d70e0639f6c5fcec06cc77e0f3e8b1f871278c585cd45666'), 'app/direct_sync_runtime.py': ('50ff189da613b1e1bec5bbb32061acc76e8d29ecae8808f5f591854a361be033', '0a4d8ddb463a8f5d52cbe60cfbfaca2aa22c06fa1c89831944cc5c54df854d04'), 'app/item_catalog_sync.py': ('41c33bf6a473cc147db6e522b1d20d4771cc9bd92c74fa3a8bd091a141907387', '7ba452c285d2f41fea202dedd68fedbe15d514a8d632bf6818f27ac54851c3d4'), 'app/label_guarded_runtime_reconcile.py': ('5f2d668622236950e307e5dc964303f78c7074d3f4bf2c2a37018c2fffe46016', 'dccab991a4caa38b7cefc59aa92357393219131232f3252c50d23dc7240a8a1e'), 'app/logistics_runtime_profile.py': ('1d786cb74812b1b4588d122b333cc9b8724d28e7410f91c7c0f025d9fd1bd5ee', 'db92068a17237e47d3025bece8d8f479fbe5eca46fde86e90666e3cac49fab72'), 'app/producer_runtime_client.py': ('e47cd333cc3c743fe9cd30a6de4069929773e86bfbb23babdeaff287564e8f17', 'd575094842ab4bb126f16d5372074aad03e20e4c0163f2ab0ffc5b10f77e271c'), 'app/protected_admin.py': ('f4b1616dc152c84e368a32acb06b424b4b4cdab588635e383fa22e7ee6d3c58e', 'eadfb46474595075b840bbdf87b919910c228ac5481fe62721a3497c478d63f9'), 'app/registration_relay_recovery.py': ('25921c0e33df76ef1c52655d95f2482906d444409d5c4a4d602e3745ff8d721b', '013758e5ab7f9084eae87651fd088a667e240841384deae3140858d28998f144'), 'app/tools/direct_sync_relay_operator.py': ('ff97593c29669ad1f983d6f7be0fb1ebabf27adcf9661b797d36f7d583a0cedb', '3c990ebfd9ff356d5876b40d1098470d0602fae3a3ed838ccf0cbc644150032f'), 'app/tools/install_logistics_runtime_profile.py': ('eb8650ace3df179a7c68f3247efe5eac86200543a43f4b2c80a47ccc95b336ef', '7dde5875649e9ec9291a45e47191b9c6fcf6b32d66cafe70c0444390cefbab9e'), 'app/tools/label_auth_recovery_canary.py': ('440a0ed5061b522cafa56408092e3ed361b42f1ae09569258b08005a04cd463b', '079dfe927bcf721f4db49779eeab96b66093e1e02c69b86da73ea069d5b7c9be'), 'app/tools/label_exact_clone_resolution_receipt.py': ('c77e6be851f32cad9058ee52892a9b2520232a15914379aba3c28ad09c581a9b', 'd1e0a8955eed3f056cb964d4f0058f082664740b03b98eefc8f5ea872ded987c'), 'app/tools/label_legacy_task_quiescence.py': ('02a422b3d5b2662a061ea79d88fd6d8da409d6daba92ffe22266e796da20dfe4', 'c9db34c66056e3e63eca603e3c1997f12367b58b6e748941b8d80a5fb7981b7e'), 'app/deferred_intent_capture.py': ('970c6565a3f5f589f15d9cc2fb0ff1e5980e772d977976152b93f3c7e6e41dbd', 'f1dc02ba1711847228945fe6a01c3881aef6e71441a85cbf5873dbf72700d72e'), 'app/label_exact_clone_resolution.py': ('0ff69b167d2b88e9be8d5a6bc068b6f160b4a19023a92b401881fc86cf7b24e4', 'f162f2c2680852cfa1ec52d4a355b275fcb87482561bb84aac89aa24e05696ce'), 'app/label_workbench_activity.py': ('42ddafdfe2a45ab60f9eb0ffca4f73d4df2efbbc15f9eca1c12628f0e22c8706', 'b94ea10dbbfc75debb375386af285e43f87231ff4a14c42f1da845f6833fa152'), 'app/label_workbench_context.py': ('32835bfbd588afd84fae494aabeab2add65f2ecb0b22aa0bb2478951107afe51', '7139f01df5558e89718c00fb65a95d83840b0c473ae747643250872eb0511f69'), 'app/label_workbench_scan.py': ('5705832318e7954554b4bd94de77b1168e63622580d7bcc49fa5b4eb534ff271', '0d7201fabded66e0bbd620dc6fd7d98f2d8b747e0c7e17a4f8b224d66d049d52'), 'app/ui/workflow_view_state.py': ('b914aece96760c7e149876d56ccd24b8efae2f33e68a23a3a5a183e1cdfc5242', 'ca42f4a1a106b60a39fe07ee74f78b3fdc4b3935986ecd6b23140c7f4c768a68')}, '464830d78a305acb7aa01a7a7afc9e72a924a5c9': {'INSTALL_THIS_PC.ps1': ('7fa8fa3f6750b369f0dc3e21146b3d6fd0c4386a014ec389b90c92190db07744', 'bdc42c26a8b5464d8a65e880ba5474ef01a28c820997adcb5ddefa8daa9b022b'), 'app/auto_update.py': ('f1d79a5d1fd65cda3e1e61f6626031354d2cba5c238c66efc113732798b2e047', 'b59bf9443939f4cf634d732f3eb699bdf62785aa8a3f3baf85b8a4ab5bbc0804'), 'app/current_user_onboarding.py': ('3d3d99ee86594d648dbf0c0acc64a6ec10be718b49869bd7d8818e49f02095d5', '93a4d7b67b9d740ddbf8bfa9546f01fc0c927ee0de985f4be4e0d83adf435ebd'), 'app/current_user_scheduled_task.py': ('016e2e435a99a47ab401e6ece288a40694ed0166950a70e3d784327acb443065', 'ad9e95381b52e57cfdecd84373b61ae5449ac8edeb9fef33199432896d0b1417'), 'app/writer_session_fence.py': ('354044c03c5ec7cdad9b39b97a4fda35c582519080fb6aa786e257527a2c5684', '92419d83453e49cd225e79ccc1aa96230c7ba00636b7fd114ed02736e6c074e1'), 'tools/label_writer_fence.ps1': ('d67831f54723f7e0c566d6b435a6faeb47561bcbbaccc78c5f1b441831474c31', '44ad315d0ffe526f003e3d8d17ff0940338652e7e557ad22ed6e728551ce3d3f'), 'app/user_relay.py': ('b96a77cb6e5e8ddec423f554378f95d2f34559a41fbf1fb617da1d74feb84b2d', '1015c0d1026d69745d70bbe1d69ab0f5a97cec52eaf8831ecc95d0356d8f4d3b'), 'app/tools/direct_sync_relay_install_pack.py': ('5a4368f2bb692dd37f406f449e607390024668a8101929577d88c549ad7093e5', '53cc1d41e90b0b9cda364ab744ce81abeaf720d222a7fe6263bd15728ec77d7f'), 'app/direct_sync_push.py': ('d9c21b3209dc35e380daeb045a51e84dd81fb99893b471e83711116ef9d06e6d', '7556f245d8724729d70e0639f6c5fcec06cc77e0f3e8b1f871278c585cd45666'), 'app/direct_sync_runtime.py': ('50ff189da613b1e1bec5bbb32061acc76e8d29ecae8808f5f591854a361be033', '0a4d8ddb463a8f5d52cbe60cfbfaca2aa22c06fa1c89831944cc5c54df854d04'), 'app/item_catalog_sync.py': ('41c33bf6a473cc147db6e522b1d20d4771cc9bd92c74fa3a8bd091a141907387', '7ba452c285d2f41fea202dedd68fedbe15d514a8d632bf6818f27ac54851c3d4'), 'app/label_admin_pin.py': ('25ff059f390f88166e74518a8027633ab75d6954ceb8f8ee74593725bf828f75', '9be9247c439a99c696cb472f0bee0eee85b2fe1a98ee2a25aabe6395aff7a617'), 'app/label_data_generation.py': ('d0e097579337cfbb27fbecc267eef4b58116876ab6bdc34a9e9caadb7cc8537c', '31a8a2a16f037fe0bc7e1502ee8aeb725aafbbf4e72cd3c0f2af550d822a3b8f'), 'app/label_guarded_runtime_reconcile.py': ('5f2d668622236950e307e5dc964303f78c7074d3f4bf2c2a37018c2fffe46016', 'dccab991a4caa38b7cefc59aa92357393219131232f3252c50d23dc7240a8a1e'), 'app/Label_Match.py': ('2474a5ec27d4b59902a13a4456cbf51436b6e8ba088a75b7a665d7b0a1048914', '43c93e151623c6098ab6dc0b200d5d44c415cdbe12f2e3be97c3172c41d270e9'), 'app/label_match_product_host.py': ('599d12593d85ffa5a592fce3683cd58907be5d10b8a16af95456f661371af189', '2f68dda99e3bc9e0d6e1c325cb43d169845df3501050a354c624a44690097845'), 'app/logistics_runtime_profile.py': ('1d786cb74812b1b4588d122b333cc9b8724d28e7410f91c7c0f025d9fd1bd5ee', 'db92068a17237e47d3025bece8d8f479fbe5eca46fde86e90666e3cac49fab72'), 'app/package_logistics.py': ('6899fd407c99066ba52ac698af0f01dbe762a56b300a6c28f1c6e56958dd5ef3', '732b2449cee55b15e9c687a542814f76c5dc0a0c49337ec018194caa7960b3c1'), 'app/producer_runtime_client.py': ('e47cd333cc3c743fe9cd30a6de4069929773e86bfbb23babdeaff287564e8f17', 'd575094842ab4bb126f16d5372074aad03e20e4c0163f2ab0ffc5b10f77e271c'), 'app/protected_admin.py': ('f4b1616dc152c84e368a32acb06b424b4b4cdab588635e383fa22e7ee6d3c58e', 'eadfb46474595075b840bbdf87b919910c228ac5481fe62721a3497c478d63f9'), 'app/registration_relay_recovery.py': ('25921c0e33df76ef1c52655d95f2482906d444409d5c4a4d602e3745ff8d721b', '013758e5ab7f9084eae87651fd088a667e240841384deae3140858d28998f144'), 'app/tools/direct_sync_relay_operator.py': ('ff97593c29669ad1f983d6f7be0fb1ebabf27adcf9661b797d36f7d583a0cedb', '3c990ebfd9ff356d5876b40d1098470d0602fae3a3ed838ccf0cbc644150032f'), 'app/tools/direct_sync_relay_runner.py': ('f7c2aa827bd6b829af0df5f0e7ec7809e4092187b0ad04404bd35a0856fdcdf5', '9b8e7a9845c4e73edea64b4ac3d0b90e3f461b7d926e0e1db64bdb9197f734ac'), 'app/tools/install_logistics_runtime_profile.py': ('eb8650ace3df179a7c68f3247efe5eac86200543a43f4b2c80a47ccc95b336ef', '7dde5875649e9ec9291a45e47191b9c6fcf6b32d66cafe70c0444390cefbab9e'), 'app/tools/label_auth_recovery_canary.py': ('440a0ed5061b522cafa56408092e3ed361b42f1ae09569258b08005a04cd463b', '079dfe927bcf721f4db49779eeab96b66093e1e02c69b86da73ea069d5b7c9be'), 'app/tools/label_exact_clone_resolution_receipt.py': ('c77e6be851f32cad9058ee52892a9b2520232a15914379aba3c28ad09c581a9b', 'd1e0a8955eed3f056cb964d4f0058f082664740b03b98eefc8f5ea872ded987c'), 'app/tools/register_label_match_worker_pc.py': ('5734a13bfae8cfa18cdb64b3a67c3b04549a2b3fa5faa13319c7471b0f027521', 'fbb7397c227cf6845e01124119cafc0081b9a0aa4d50c44703d339fc5dd8271b'), 'tools/bootstrap_integrity.ps1': ('d5011d5a4d0809c3e61abb0a3c08d1b50bdea540f172d485b538854b1675406c', '96101ce38befdf9dcca09ec42abd571aa441ad312f35bf3d14fd7effc97db963'), 'app/label_recovery_schema.py': ('8717fe4428874933724f6b61dfb54c9fc82b49b7df1ef0cc130d7ddc7f1a3d3d', '4bf2ad6024efd6c3ac2f5965eef3e81fa149078f4bc22ff716272a5c2359ffa6'), 'tools/apply_prepared_update.ps1': ('5870315c17666ebec29cfda51a9f161c27a52a8894eb01e19bda36f00fa3bad1', 'dc7e30404d7bd4277692f875120aff4f7afd5c01b84301ed9e95a2c03e844c83'), 'app/tools/label_legacy_task_quiescence.py': ('02a422b3d5b2662a061ea79d88fd6d8da409d6daba92ffe22266e796da20dfe4', 'c9db34c66056e3e63eca603e3c1997f12367b58b6e748941b8d80a5fb7981b7e'), 'app/deferred_intent_capture.py': ('970c6565a3f5f589f15d9cc2fb0ff1e5980e772d977976152b93f3c7e6e41dbd', 'f1dc02ba1711847228945fe6a01c3881aef6e71441a85cbf5873dbf72700d72e'), 'app/label_exact_clone_resolution.py': ('0ff69b167d2b88e9be8d5a6bc068b6f160b4a19023a92b401881fc86cf7b24e4', 'f162f2c2680852cfa1ec52d4a355b275fcb87482561bb84aac89aa24e05696ce'), 'app/label_workbench_activity.py': ('42ddafdfe2a45ab60f9eb0ffca4f73d4df2efbbc15f9eca1c12628f0e22c8706', 'b94ea10dbbfc75debb375386af285e43f87231ff4a14c42f1da845f6833fa152'), 'app/label_workbench_context.py': ('32835bfbd588afd84fae494aabeab2add65f2ecb0b22aa0bb2478951107afe51', '7139f01df5558e89718c00fb65a95d83840b0c473ae747643250872eb0511f69'), 'app/label_workbench_scan.py': ('5705832318e7954554b4bd94de77b1168e63622580d7bcc49fa5b4eb534ff271', '0d7201fabded66e0bbd620dc6fd7d98f2d8b747e0c7e17a4f8b224d66d049d52'), 'app/ui/workflow_view_state.py': ('b914aece96760c7e149876d56ccd24b8efae2f33e68a23a3a5a183e1cdfc5242', 'ca42f4a1a106b60a39fe07ee74f78b3fdc4b3935986ecd6b23140c7f4c768a68')}, 'a56a58769c43d1b19abdd9881425f95a49dbd7f7': {'INSTALL_THIS_PC.ps1': ('5b43cc1c66b2e43aa855ce876e1c7515ca3cea403069002d3da82a597131a766', 'bdc42c26a8b5464d8a65e880ba5474ef01a28c820997adcb5ddefa8daa9b022b'), 'app/Label_Match.py': ('d0483a90b7c42923e4e638dbf45fbd3162768e6ca15fa2184fa7b71b0f84fda1', '43c93e151623c6098ab6dc0b200d5d44c415cdbe12f2e3be97c3172c41d270e9'), 'app/config/app_settings.json': ('081e0ab6e932035312c9bc225db26589e4c694cd2c40170ca459443e1a5460e8', 'e65540f3325c70866753cd0abfe01c21edb89f6fa05cd1ddc1666a89af14c34b'), 'app/current_user_onboarding.py': ('fb6f7a7db58874eb761d1a4fe88964be49c74d9ce871db9ab3b8d41e285a3017', '93a4d7b67b9d740ddbf8bfa9546f01fc0c927ee0de985f4be4e0d83adf435ebd'), 'app/current_user_scheduled_task.py': ('5b339cae5a61c67bf3187f75ae254c38df4414bf7c0f5f3b8ed24f3cff52e870', 'ad9e95381b52e57cfdecd84373b61ae5449ac8edeb9fef33199432896d0b1417'), 'app/direct_sync_push.py': ('d9c21b3209dc35e380daeb045a51e84dd81fb99893b471e83711116ef9d06e6d', '7556f245d8724729d70e0639f6c5fcec06cc77e0f3e8b1f871278c585cd45666'), 'app/direct_sync_runtime.py': ('50ff189da613b1e1bec5bbb32061acc76e8d29ecae8808f5f591854a361be033', '0a4d8ddb463a8f5d52cbe60cfbfaca2aa22c06fa1c89831944cc5c54df854d04'), 'app/item_catalog_sync.py': ('41c33bf6a473cc147db6e522b1d20d4771cc9bd92c74fa3a8bd091a141907387', '7ba452c285d2f41fea202dedd68fedbe15d514a8d632bf6818f27ac54851c3d4'), 'app/label_admin_pin.py': ('25ff059f390f88166e74518a8027633ab75d6954ceb8f8ee74593725bf828f75', '9be9247c439a99c696cb472f0bee0eee85b2fe1a98ee2a25aabe6395aff7a617'), 'app/label_data_generation.py': ('d0e097579337cfbb27fbecc267eef4b58116876ab6bdc34a9e9caadb7cc8537c', '31a8a2a16f037fe0bc7e1502ee8aeb725aafbbf4e72cd3c0f2af550d822a3b8f'), 'app/label_data_manager.py': ('4443b2e817e54e1ba1bbd9065f49123ef1d55b40a7e00f485bc2cda6d5d3d912', '2ddd2129d6de99073f3cd51d8947a4792ea56e371205f3ea70445301317f4e56'), 'app/label_guarded_runtime_reconcile.py': ('5f2d668622236950e307e5dc964303f78c7074d3f4bf2c2a37018c2fffe46016', 'dccab991a4caa38b7cefc59aa92357393219131232f3252c50d23dc7240a8a1e'), 'app/label_match_product_host.py': ('bce7176d636a93c559340850d91894ffd02a4eef2a3f953d2ae05cd1cb0f9fbf', '2f68dda99e3bc9e0d6e1c325cb43d169845df3501050a354c624a44690097845'), 'app/label_recovery_schema.py': ('5ddc38a222acaa6a2ca4b3bf23fc09f173b1e2646a599a667062df245cbf5349', '4bf2ad6024efd6c3ac2f5965eef3e81fa149078f4bc22ff716272a5c2359ffa6'), 'app/logistics_runtime_profile.py': ('1d786cb74812b1b4588d122b333cc9b8724d28e7410f91c7c0f025d9fd1bd5ee', 'db92068a17237e47d3025bece8d8f479fbe5eca46fde86e90666e3cac49fab72'), 'app/main.py': ('520c2eaea01876f324288f01fee6f7600dd45d27829fb0e374969f6e2bf2bb6c', 'f85e46a1779eba99ac26609a172e8e4fad55e4e4115ee314bc5e1e66f4cf75ab'), 'app/package_logistics.py': ('6899fd407c99066ba52ac698af0f01dbe762a56b300a6c28f1c6e56958dd5ef3', '732b2449cee55b15e9c687a542814f76c5dc0a0c49337ec018194caa7960b3c1'), 'app/producer_runtime_client.py': ('e47cd333cc3c743fe9cd30a6de4069929773e86bfbb23babdeaff287564e8f17', 'd575094842ab4bb126f16d5372074aad03e20e4c0163f2ab0ffc5b10f77e271c'), 'app/protected_admin.py': ('f4b1616dc152c84e368a32acb06b424b4b4cdab588635e383fa22e7ee6d3c58e', 'eadfb46474595075b840bbdf87b919910c228ac5481fe62721a3497c478d63f9'), 'app/registration_relay_recovery.py': ('25921c0e33df76ef1c52655d95f2482906d444409d5c4a4d602e3745ff8d721b', '013758e5ab7f9084eae87651fd088a667e240841384deae3140858d28998f144'), 'app/tools/direct_sync_relay_install_pack.py': ('5a4368f2bb692dd37f406f449e607390024668a8101929577d88c549ad7093e5', '53cc1d41e90b0b9cda364ab744ce81abeaf720d222a7fe6263bd15728ec77d7f'), 'app/tools/direct_sync_relay_operator.py': ('ff97593c29669ad1f983d6f7be0fb1ebabf27adcf9661b797d36f7d583a0cedb', '3c990ebfd9ff356d5876b40d1098470d0602fae3a3ed838ccf0cbc644150032f'), 'app/tools/direct_sync_relay_runner.py': ('f7c2aa827bd6b829af0df5f0e7ec7809e4092187b0ad04404bd35a0856fdcdf5', '9b8e7a9845c4e73edea64b4ac3d0b90e3f461b7d926e0e1db64bdb9197f734ac'), 'app/tools/install_logistics_runtime_profile.py': ('eb8650ace3df179a7c68f3247efe5eac86200543a43f4b2c80a47ccc95b336ef', '7dde5875649e9ec9291a45e47191b9c6fcf6b32d66cafe70c0444390cefbab9e'), 'app/tools/label_auth_recovery_canary.py': ('440a0ed5061b522cafa56408092e3ed361b42f1ae09569258b08005a04cd463b', '079dfe927bcf721f4db49779eeab96b66093e1e02c69b86da73ea069d5b7c9be'), 'app/tools/label_exact_clone_resolution_receipt.py': ('c77e6be851f32cad9058ee52892a9b2520232a15914379aba3c28ad09c581a9b', 'd1e0a8955eed3f056cb964d4f0058f082664740b03b98eefc8f5ea872ded987c'), 'app/tools/label_legacy_task_quiescence.py': ('02a422b3d5b2662a061ea79d88fd6d8da409d6daba92ffe22266e796da20dfe4', 'c9db34c66056e3e63eca603e3c1997f12367b58b6e748941b8d80a5fb7981b7e'), 'app/tools/register_label_match_worker_pc.py': ('469c2e2ebdc9ddb45973e1423336bd6619b485e1604732d4d7bec55c41f0931f', 'fbb7397c227cf6845e01124119cafc0081b9a0aa4d50c44703d339fc5dd8271b'), 'app/user_relay.py': ('3b71cb47a1a4a3253ac0602804e5770e40e6b674004174838d3a00016c3e8487', '1015c0d1026d69745d70bbe1d69ab0f5a97cec52eaf8831ecc95d0356d8f4d3b'), 'app/writer_session_fence.py': ('46731dc32ab372b721dcd59297764e0bc99cb4c2ca92ae5bf66157b3fb90593e', '92419d83453e49cd225e79ccc1aa96230c7ba00636b7fd114ed02736e6c074e1'), 'tools/bootstrap_integrity.ps1': ('60531b973d2c835c2b834136f34d61831e083180790aae28faaa96ba1d7a907c', '96101ce38befdf9dcca09ec42abd571aa441ad312f35bf3d14fd7effc97db963'), 'tools/label_writer_fence.ps1': ('6777bdfac69f830ac7800b4976e2ffc3b2b3cb0593f4e3c3a8cddd5855bb849b', '44ad315d0ffe526f003e3d8d17ff0940338652e7e557ad22ed6e728551ce3d3f'), 'tools/label_writer_fence_contract.json': ('bc95bd4ae6984f08120d7284f1ef73e7f1369e8fae078f6d60c66144a5cb24de', '8a07c41b748669e237fc7f35be75daede0fca9f86c2d1af07ff7a701a33126dd'), 'app/deferred_intent_capture.py': ('970c6565a3f5f589f15d9cc2fb0ff1e5980e772d977976152b93f3c7e6e41dbd', 'f1dc02ba1711847228945fe6a01c3881aef6e71441a85cbf5873dbf72700d72e'), 'app/label_exact_clone_resolution.py': ('0ff69b167d2b88e9be8d5a6bc068b6f160b4a19023a92b401881fc86cf7b24e4', 'f162f2c2680852cfa1ec52d4a355b275fcb87482561bb84aac89aa24e05696ce'), 'app/label_workbench_activity.py': ('42ddafdfe2a45ab60f9eb0ffca4f73d4df2efbbc15f9eca1c12628f0e22c8706', 'b94ea10dbbfc75debb375386af285e43f87231ff4a14c42f1da845f6833fa152'), 'app/label_workbench_context.py': ('32835bfbd588afd84fae494aabeab2add65f2ecb0b22aa0bb2478951107afe51', '7139f01df5558e89718c00fb65a95d83840b0c473ae747643250872eb0511f69'), 'app/label_workbench_scan.py': ('5705832318e7954554b4bd94de77b1168e63622580d7bcc49fa5b4eb534ff271', '0d7201fabded66e0bbd620dc6fd7d98f2d8b747e0c7e17a4f8b224d66d049d52'), 'app/ui/workflow_view_state.py': ('b914aece96760c7e149876d56ccd24b8efae2f33e68a23a3a5a183e1cdfc5242', 'ca42f4a1a106b60a39fe07ee74f78b3fdc4b3935986ecd6b23140c7f4c768a68')}, '2c46de16f2303117e88d4794bf3793f0d7de8c76': {'INSTALL_THIS_PC.ps1': ('5b43cc1c66b2e43aa855ce876e1c7515ca3cea403069002d3da82a597131a766', 'bdc42c26a8b5464d8a65e880ba5474ef01a28c820997adcb5ddefa8daa9b022b'), 'app/Label_Match.py': ('0b8dc236a221c941b423568353238453e1b7a6229647fc7882afc227bdeee702', '43c93e151623c6098ab6dc0b200d5d44c415cdbe12f2e3be97c3172c41d270e9'), 'app/config/app_settings.json': ('081e0ab6e932035312c9bc225db26589e4c694cd2c40170ca459443e1a5460e8', 'e65540f3325c70866753cd0abfe01c21edb89f6fa05cd1ddc1666a89af14c34b'), 'app/current_user_onboarding.py': ('cbe541306148f28a9e95317f6885cfca0e7b0e6744f81befd15b7727844109d1', '93a4d7b67b9d740ddbf8bfa9546f01fc0c927ee0de985f4be4e0d83adf435ebd'), 'app/current_user_scheduled_task.py': ('5b339cae5a61c67bf3187f75ae254c38df4414bf7c0f5f3b8ed24f3cff52e870', 'ad9e95381b52e57cfdecd84373b61ae5449ac8edeb9fef33199432896d0b1417'), 'app/direct_sync_push.py': ('d9c21b3209dc35e380daeb045a51e84dd81fb99893b471e83711116ef9d06e6d', '7556f245d8724729d70e0639f6c5fcec06cc77e0f3e8b1f871278c585cd45666'), 'app/direct_sync_runtime.py': ('03411c1c265188cd4ba4267b0db0444001799191c6a5ea1bb35690b319e04fc8', '0a4d8ddb463a8f5d52cbe60cfbfaca2aa22c06fa1c89831944cc5c54df854d04'), 'app/item_catalog_sync.py': ('41c33bf6a473cc147db6e522b1d20d4771cc9bd92c74fa3a8bd091a141907387', '7ba452c285d2f41fea202dedd68fedbe15d514a8d632bf6818f27ac54851c3d4'), 'app/label_admin_pin.py': ('25ff059f390f88166e74518a8027633ab75d6954ceb8f8ee74593725bf828f75', '9be9247c439a99c696cb472f0bee0eee85b2fe1a98ee2a25aabe6395aff7a617'), 'app/label_data_generation.py': ('c30153d3c0a7364f408efff5e62789271a49896dcdad4ad93a55d4ac8d985ef5', '31a8a2a16f037fe0bc7e1502ee8aeb725aafbbf4e72cd3c0f2af550d822a3b8f'), 'app/label_match_product_host.py': ('174895b001a0ba88dda6e877e6a742bec5a7690afd09104895d0e14c1886ee31', '2f68dda99e3bc9e0d6e1c325cb43d169845df3501050a354c624a44690097845'), 'app/label_recovery_schema.py': ('8717fe4428874933724f6b61dfb54c9fc82b49b7df1ef0cc130d7ddc7f1a3d3d', '4bf2ad6024efd6c3ac2f5965eef3e81fa149078f4bc22ff716272a5c2359ffa6'), 'app/logistics_runtime_profile.py': ('1d786cb74812b1b4588d122b333cc9b8724d28e7410f91c7c0f025d9fd1bd5ee', 'db92068a17237e47d3025bece8d8f479fbe5eca46fde86e90666e3cac49fab72'), 'app/main.py': ('520c2eaea01876f324288f01fee6f7600dd45d27829fb0e374969f6e2bf2bb6c', 'f85e46a1779eba99ac26609a172e8e4fad55e4e4115ee314bc5e1e66f4cf75ab'), 'app/package_logistics.py': ('43f7a81134e22a576e40b4313198ae5e86485449655398d21bf6cb309d523008', '732b2449cee55b15e9c687a542814f76c5dc0a0c49337ec018194caa7960b3c1'), 'app/producer_runtime_client.py': ('e47cd333cc3c743fe9cd30a6de4069929773e86bfbb23babdeaff287564e8f17', 'd575094842ab4bb126f16d5372074aad03e20e4c0163f2ab0ffc5b10f77e271c'), 'app/tools/direct_sync_relay_install_pack.py': ('4a1e96d6c8dff4f141fe23d3ea9e42d44507ce57438dca5da9a53cea91720fcf', '53cc1d41e90b0b9cda364ab744ce81abeaf720d222a7fe6263bd15728ec77d7f'), 'app/tools/direct_sync_relay_operator.py': ('8f7a22e4f180276dabe3c2635761d16c59ed41d6b35825e681ac510398d856ca', '3c990ebfd9ff356d5876b40d1098470d0602fae3a3ed838ccf0cbc644150032f'), 'app/tools/install_logistics_runtime_profile.py': ('9ff8ef6e353b215c1abe50b7809bd730e98a563e67aacaeeb88f632fc99b503d', '7dde5875649e9ec9291a45e47191b9c6fcf6b32d66cafe70c0444390cefbab9e'), 'app/tools/label_exact_clone_resolution_receipt.py': ('5d7e3836246a8e06e604bd7e776f31310e1c53006f80102cccd5c482dddc3302', 'd1e0a8955eed3f056cb964d4f0058f082664740b03b98eefc8f5ea872ded987c'), 'app/tools/label_legacy_task_quiescence.py': ('02a422b3d5b2662a061ea79d88fd6d8da409d6daba92ffe22266e796da20dfe4', 'c9db34c66056e3e63eca603e3c1997f12367b58b6e748941b8d80a5fb7981b7e'), 'app/tools/register_label_match_worker_pc.py': ('8bbc3f2e68d7217fbd554c229f043d8e047a02cf1e85b914402cf32d3e7703aa', 'fbb7397c227cf6845e01124119cafc0081b9a0aa4d50c44703d339fc5dd8271b'), 'app/user_relay.py': ('6edac91cef60878847ec1954e0b8c39978105a6df003984f056d421823e1262f', '1015c0d1026d69745d70bbe1d69ab0f5a97cec52eaf8831ecc95d0356d8f4d3b'), 'app/writer_session_fence.py': ('9376e9e2a998aca6f97fa99368276c3c87c18fa3b45625e1c1a226df68ebdb7b', '92419d83453e49cd225e79ccc1aa96230c7ba00636b7fd114ed02736e6c074e1'), 'tools/bootstrap_integrity.ps1': ('60531b973d2c835c2b834136f34d61831e083180790aae28faaa96ba1d7a907c', '96101ce38befdf9dcca09ec42abd571aa441ad312f35bf3d14fd7effc97db963'), 'tools/label_writer_fence.ps1': ('eff51cca49b3bc5e9104bcb2c3778c59dceecea003beb45390d5dc1eeeda6420', '44ad315d0ffe526f003e3d8d17ff0940338652e7e557ad22ed6e728551ce3d3f'), 'tools/label_writer_fence_contract.json': ('bc95bd4ae6984f08120d7284f1ef73e7f1369e8fae078f6d60c66144a5cb24de', '8a07c41b748669e237fc7f35be75daede0fca9f86c2d1af07ff7a701a33126dd'), 'app/deferred_intent_capture.py': ('970c6565a3f5f589f15d9cc2fb0ff1e5980e772d977976152b93f3c7e6e41dbd', 'f1dc02ba1711847228945fe6a01c3881aef6e71441a85cbf5873dbf72700d72e'), 'app/label_exact_clone_resolution.py': ('0ff69b167d2b88e9be8d5a6bc068b6f160b4a19023a92b401881fc86cf7b24e4', 'f162f2c2680852cfa1ec52d4a355b275fcb87482561bb84aac89aa24e05696ce'), 'app/label_workbench_activity.py': ('42ddafdfe2a45ab60f9eb0ffca4f73d4df2efbbc15f9eca1c12628f0e22c8706', 'b94ea10dbbfc75debb375386af285e43f87231ff4a14c42f1da845f6833fa152'), 'app/label_workbench_context.py': ('32835bfbd588afd84fae494aabeab2add65f2ecb0b22aa0bb2478951107afe51', '7139f01df5558e89718c00fb65a95d83840b0c473ae747643250872eb0511f69'), 'app/label_workbench_scan.py': ('5705832318e7954554b4bd94de77b1168e63622580d7bcc49fa5b4eb534ff271', '0d7201fabded66e0bbd620dc6fd7d98f2d8b747e0c7e17a4f8b224d66d049d52'), 'app/tools/direct_sync_relay_runner.py': ('ed4c169b6d057ff9d6927277068d0c477c2804984badc065cb6d5b7b40ced915', '9b8e7a9845c4e73edea64b4ac3d0b90e3f461b7d926e0e1db64bdb9197f734ac'), 'app/ui/workflow_view_state.py': ('b914aece96760c7e149876d56ccd24b8efae2f33e68a23a3a5a183e1cdfc5242', 'ca42f4a1a106b60a39fe07ee74f78b3fdc4b3935986ecd6b23140c7f4c768a68')}, '55cab723e215ab3ffd12ef7e3cc9e8d3c1569e8a': {'app/auto_update.py': ('d9d3b71657d90605d0bd7430ccda025723b0d70844741a0ca6a9e87cd84afed4', 'b59bf9443939f4cf634d732f3eb699bdf62785aa8a3f3baf85b8a4ab5bbc0804'), 'app/current_user_scheduled_task.py': ('f33a79ee136103a7ba688c97f099c4af0ad6b67d066f2b8d6b0a0c5902612667', 'ad9e95381b52e57cfdecd84373b61ae5449ac8edeb9fef33199432896d0b1417'), 'app/deferred_intent_capture.py': ('970c6565a3f5f589f15d9cc2fb0ff1e5980e772d977976152b93f3c7e6e41dbd', 'f1dc02ba1711847228945fe6a01c3881aef6e71441a85cbf5873dbf72700d72e'), 'app/label_admin_pin.py': ('be6ccfe4d74fd423d6dfcd7095ba1c92fa340c7be2ae4ade50127a1232f82d84', '9be9247c439a99c696cb472f0bee0eee85b2fe1a98ee2a25aabe6395aff7a617'), 'app/Label_Match.py': ('d3c85db9c46e73b2d0878145db967c9fde291d844d16fcada44d4ef947f2b4e9', '43c93e151623c6098ab6dc0b200d5d44c415cdbe12f2e3be97c3172c41d270e9'), 'app/writer_session_fence.py': ('3076b56b1dbf5c8023d4866942a91d782388845f3af5f83bae878fd2b72b0dab', '92419d83453e49cd225e79ccc1aa96230c7ba00636b7fd114ed02736e6c074e1'), 'INSTALL_THIS_PC.ps1': ('e1d2886238523c6374e698f51aa8c2d64441c452a6dc0469bd9934cf59ae2237', 'bdc42c26a8b5464d8a65e880ba5474ef01a28c820997adcb5ddefa8daa9b022b'), 'tools/label_writer_fence.ps1': ('5ad24d6d46f3f59d98497bb1147c9a0fc378428697b62c5621a9000bae4af81a', '44ad315d0ffe526f003e3d8d17ff0940338652e7e557ad22ed6e728551ce3d3f'), 'tools/apply_prepared_update.ps1': ('974ceb1b6b42cc6c3499c7f8732a6ec1854602b9e7ef777029590af358b4441a', 'dc7e30404d7bd4277692f875120aff4f7afd5c01b84301ed9e95a2c03e844c83')}}
startup_release_additions={'3f4574a176f45253d746b397ad979896f266d062': {'app/auto-update-settings.json': '7cf5b12cbea56e4f97707bf32f641c0e536ec42236c5d3c907f16facc2415bf9', 'app/auto_update.py': 'b59bf9443939f4cf634d732f3eb699bdf62785aa8a3f3baf85b8a4ab5bbc0804', 'app/config/update-manifest-key-config.json': 'd76e3d4f62aecd83d07e1b9b37f8463e1bc170bae6cd9bae965a17e6ca4d2671', 'app/label_transition.py': '27c5952d804558211c7db05ac3489e4bd0422599a259af6b287fec5863f09279', 'tools/apply_prepared_update.ps1': 'dc7e30404d7bd4277692f875120aff4f7afd5c01b84301ed9e95a2c03e844c83', 'app/label_data_generation.py': '31a8a2a16f037fe0bc7e1502ee8aeb725aafbbf4e72cd3c0f2af550d822a3b8f', 'app/label_worker_roster.py': '63d8fef44564fa9666b4a5b7494339e0fd91692e94eea42e8cbbcbb81fe4b6bd', 'app/kmtech_shared/data_generation.py': '14ffe4277dc60a32a6074abf3d56ec0b0dcf3d5accd372f6dd41f36de0e348c6', 'app/kmtech_shared/powershell/data_generation.ps1': 'b809e49985c73e0b1aedbe9dd3a8050177939f6fb088087e6d3e4c9bc05b5c96', 'app/failure_redaction.py': '58f77e71532747b439c9d5fcf8c52bbf620f974f51ca3b63ad0dcf764fea4af9', 'app/tools/failure_redaction.ps1': 'c81ce889aef0d216985a9dbaf439c2bfa11640d929e836d2bc7b68966d6d073f', 'app/tools/failure_redaction_rules.json': '092cc5071abb481602e746b08faba67a8fe43c7f5a59bc3e28181f76dc245b2d', 'tools/failure_redaction.ps1': 'c81ce889aef0d216985a9dbaf439c2bfa11640d929e836d2bc7b68966d6d073f', 'tools/failure_redaction_rules.json': '092cc5071abb481602e746b08faba67a8fe43c7f5a59bc3e28181f76dc245b2d'}, '6479eca6c2c03f17d5f0fcf8b662a83045f96039': {'app/auto-update-settings.json': '7cf5b12cbea56e4f97707bf32f641c0e536ec42236c5d3c907f16facc2415bf9', 'app/auto_update.py': 'b59bf9443939f4cf634d732f3eb699bdf62785aa8a3f3baf85b8a4ab5bbc0804', 'app/config/update-manifest-key-config.json': 'd76e3d4f62aecd83d07e1b9b37f8463e1bc170bae6cd9bae965a17e6ca4d2671', 'tools/apply_prepared_update.ps1': 'dc7e30404d7bd4277692f875120aff4f7afd5c01b84301ed9e95a2c03e844c83', 'app/label_data_generation.py': '31a8a2a16f037fe0bc7e1502ee8aeb725aafbbf4e72cd3c0f2af550d822a3b8f', 'app/label_worker_roster.py': '63d8fef44564fa9666b4a5b7494339e0fd91692e94eea42e8cbbcbb81fe4b6bd', 'app/kmtech_shared/data_generation.py': '14ffe4277dc60a32a6074abf3d56ec0b0dcf3d5accd372f6dd41f36de0e348c6', 'app/kmtech_shared/powershell/data_generation.ps1': 'b809e49985c73e0b1aedbe9dd3a8050177939f6fb088087e6d3e4c9bc05b5c96', 'app/failure_redaction.py': '58f77e71532747b439c9d5fcf8c52bbf620f974f51ca3b63ad0dcf764fea4af9', 'app/tools/failure_redaction.ps1': 'c81ce889aef0d216985a9dbaf439c2bfa11640d929e836d2bc7b68966d6d073f', 'app/tools/failure_redaction_rules.json': '092cc5071abb481602e746b08faba67a8fe43c7f5a59bc3e28181f76dc245b2d', 'tools/failure_redaction.ps1': 'c81ce889aef0d216985a9dbaf439c2bfa11640d929e836d2bc7b68966d6d073f', 'tools/failure_redaction_rules.json': '092cc5071abb481602e746b08faba67a8fe43c7f5a59bc3e28181f76dc245b2d'}, '731ca29b14a5a9f98ec4f4935382548891557d95': {'app/auto-update-settings.json': '7cf5b12cbea56e4f97707bf32f641c0e536ec42236c5d3c907f16facc2415bf9', 'app/auto_update.py': 'b59bf9443939f4cf634d732f3eb699bdf62785aa8a3f3baf85b8a4ab5bbc0804', 'app/config/update-manifest-key-config.json': 'd76e3d4f62aecd83d07e1b9b37f8463e1bc170bae6cd9bae965a17e6ca4d2671', 'tools/apply_prepared_update.ps1': 'dc7e30404d7bd4277692f875120aff4f7afd5c01b84301ed9e95a2c03e844c83', 'app/label_data_generation.py': '31a8a2a16f037fe0bc7e1502ee8aeb725aafbbf4e72cd3c0f2af550d822a3b8f', 'app/label_worker_roster.py': '63d8fef44564fa9666b4a5b7494339e0fd91692e94eea42e8cbbcbb81fe4b6bd', 'app/kmtech_shared/data_generation.py': '14ffe4277dc60a32a6074abf3d56ec0b0dcf3d5accd372f6dd41f36de0e348c6', 'app/kmtech_shared/powershell/data_generation.ps1': 'b809e49985c73e0b1aedbe9dd3a8050177939f6fb088087e6d3e4c9bc05b5c96', 'app/failure_redaction.py': '58f77e71532747b439c9d5fcf8c52bbf620f974f51ca3b63ad0dcf764fea4af9', 'app/tools/failure_redaction.ps1': 'c81ce889aef0d216985a9dbaf439c2bfa11640d929e836d2bc7b68966d6d073f', 'app/tools/failure_redaction_rules.json': '092cc5071abb481602e746b08faba67a8fe43c7f5a59bc3e28181f76dc245b2d', 'tools/failure_redaction.ps1': 'c81ce889aef0d216985a9dbaf439c2bfa11640d929e836d2bc7b68966d6d073f', 'tools/failure_redaction_rules.json': '092cc5071abb481602e746b08faba67a8fe43c7f5a59bc3e28181f76dc245b2d'}, '817d393b7316735921c2aa6ede271a04cbea20fd': {'app/auto-update-settings.json': '7cf5b12cbea56e4f97707bf32f641c0e536ec42236c5d3c907f16facc2415bf9', 'app/auto_update.py': 'b59bf9443939f4cf634d732f3eb699bdf62785aa8a3f3baf85b8a4ab5bbc0804', 'app/config/update-manifest-key-config.json': 'd76e3d4f62aecd83d07e1b9b37f8463e1bc170bae6cd9bae965a17e6ca4d2671', 'tools/apply_prepared_update.ps1': 'dc7e30404d7bd4277692f875120aff4f7afd5c01b84301ed9e95a2c03e844c83', 'app/label_data_generation.py': '31a8a2a16f037fe0bc7e1502ee8aeb725aafbbf4e72cd3c0f2af550d822a3b8f', 'app/label_worker_roster.py': '63d8fef44564fa9666b4a5b7494339e0fd91692e94eea42e8cbbcbb81fe4b6bd', 'app/kmtech_shared/data_generation.py': '14ffe4277dc60a32a6074abf3d56ec0b0dcf3d5accd372f6dd41f36de0e348c6', 'app/kmtech_shared/powershell/data_generation.ps1': 'b809e49985c73e0b1aedbe9dd3a8050177939f6fb088087e6d3e4c9bc05b5c96', 'app/failure_redaction.py': '58f77e71532747b439c9d5fcf8c52bbf620f974f51ca3b63ad0dcf764fea4af9', 'app/tools/failure_redaction.ps1': 'c81ce889aef0d216985a9dbaf439c2bfa11640d929e836d2bc7b68966d6d073f', 'app/tools/failure_redaction_rules.json': '092cc5071abb481602e746b08faba67a8fe43c7f5a59bc3e28181f76dc245b2d', 'tools/failure_redaction.ps1': 'c81ce889aef0d216985a9dbaf439c2bfa11640d929e836d2bc7b68966d6d073f', 'tools/failure_redaction_rules.json': '092cc5071abb481602e746b08faba67a8fe43c7f5a59bc3e28181f76dc245b2d'}, '92abdaa377b2b3255c25ff0e5e331c3a4f1ddfa4': {'app/auto-update-settings.json': '7cf5b12cbea56e4f97707bf32f641c0e536ec42236c5d3c907f16facc2415bf9', 'app/auto_update.py': 'b59bf9443939f4cf634d732f3eb699bdf62785aa8a3f3baf85b8a4ab5bbc0804', 'app/config/update-manifest-key-config.json': 'd76e3d4f62aecd83d07e1b9b37f8463e1bc170bae6cd9bae965a17e6ca4d2671', 'tools/apply_prepared_update.ps1': 'dc7e30404d7bd4277692f875120aff4f7afd5c01b84301ed9e95a2c03e844c83', 'app/label_data_generation.py': '31a8a2a16f037fe0bc7e1502ee8aeb725aafbbf4e72cd3c0f2af550d822a3b8f', 'app/label_worker_roster.py': '63d8fef44564fa9666b4a5b7494339e0fd91692e94eea42e8cbbcbb81fe4b6bd', 'app/kmtech_shared/data_generation.py': '14ffe4277dc60a32a6074abf3d56ec0b0dcf3d5accd372f6dd41f36de0e348c6', 'app/kmtech_shared/powershell/data_generation.ps1': 'b809e49985c73e0b1aedbe9dd3a8050177939f6fb088087e6d3e4c9bc05b5c96', 'app/failure_redaction.py': '58f77e71532747b439c9d5fcf8c52bbf620f974f51ca3b63ad0dcf764fea4af9', 'app/tools/failure_redaction.ps1': 'c81ce889aef0d216985a9dbaf439c2bfa11640d929e836d2bc7b68966d6d073f', 'app/tools/failure_redaction_rules.json': '092cc5071abb481602e746b08faba67a8fe43c7f5a59bc3e28181f76dc245b2d', 'tools/failure_redaction.ps1': 'c81ce889aef0d216985a9dbaf439c2bfa11640d929e836d2bc7b68966d6d073f', 'tools/failure_redaction_rules.json': '092cc5071abb481602e746b08faba67a8fe43c7f5a59bc3e28181f76dc245b2d'}, 'b00712c410770f1d023b09aa9a1de4743a81b1b6': {'app/auto-update-settings.json': '7cf5b12cbea56e4f97707bf32f641c0e536ec42236c5d3c907f16facc2415bf9', 'app/auto_update.py': 'b59bf9443939f4cf634d732f3eb699bdf62785aa8a3f3baf85b8a4ab5bbc0804', 'app/config/update-manifest-key-config.json': 'd76e3d4f62aecd83d07e1b9b37f8463e1bc170bae6cd9bae965a17e6ca4d2671', 'tools/apply_prepared_update.ps1': 'dc7e30404d7bd4277692f875120aff4f7afd5c01b84301ed9e95a2c03e844c83', 'app/label_data_generation.py': '31a8a2a16f037fe0bc7e1502ee8aeb725aafbbf4e72cd3c0f2af550d822a3b8f', 'app/label_worker_roster.py': '63d8fef44564fa9666b4a5b7494339e0fd91692e94eea42e8cbbcbb81fe4b6bd', 'app/kmtech_shared/data_generation.py': '14ffe4277dc60a32a6074abf3d56ec0b0dcf3d5accd372f6dd41f36de0e348c6', 'app/kmtech_shared/powershell/data_generation.ps1': 'b809e49985c73e0b1aedbe9dd3a8050177939f6fb088087e6d3e4c9bc05b5c96', 'app/failure_redaction.py': '58f77e71532747b439c9d5fcf8c52bbf620f974f51ca3b63ad0dcf764fea4af9', 'app/tools/failure_redaction.ps1': 'c81ce889aef0d216985a9dbaf439c2bfa11640d929e836d2bc7b68966d6d073f', 'app/tools/failure_redaction_rules.json': '092cc5071abb481602e746b08faba67a8fe43c7f5a59bc3e28181f76dc245b2d', 'tools/failure_redaction.ps1': 'c81ce889aef0d216985a9dbaf439c2bfa11640d929e836d2bc7b68966d6d073f', 'tools/failure_redaction_rules.json': '092cc5071abb481602e746b08faba67a8fe43c7f5a59bc3e28181f76dc245b2d'}, 'c5e051b4b126aaab259d57d451be14f98d18d87e': {'app/auto-update-settings.json': '7cf5b12cbea56e4f97707bf32f641c0e536ec42236c5d3c907f16facc2415bf9', 'app/auto_update.py': 'b59bf9443939f4cf634d732f3eb699bdf62785aa8a3f3baf85b8a4ab5bbc0804', 'app/config/update-manifest-key-config.json': 'd76e3d4f62aecd83d07e1b9b37f8463e1bc170bae6cd9bae965a17e6ca4d2671', 'tools/apply_prepared_update.ps1': 'dc7e30404d7bd4277692f875120aff4f7afd5c01b84301ed9e95a2c03e844c83', 'app/label_data_generation.py': '31a8a2a16f037fe0bc7e1502ee8aeb725aafbbf4e72cd3c0f2af550d822a3b8f', 'app/label_worker_roster.py': '63d8fef44564fa9666b4a5b7494339e0fd91692e94eea42e8cbbcbb81fe4b6bd', 'app/kmtech_shared/data_generation.py': '14ffe4277dc60a32a6074abf3d56ec0b0dcf3d5accd372f6dd41f36de0e348c6', 'app/kmtech_shared/powershell/data_generation.ps1': 'b809e49985c73e0b1aedbe9dd3a8050177939f6fb088087e6d3e4c9bc05b5c96', 'app/failure_redaction.py': '58f77e71532747b439c9d5fcf8c52bbf620f974f51ca3b63ad0dcf764fea4af9', 'app/tools/failure_redaction.ps1': 'c81ce889aef0d216985a9dbaf439c2bfa11640d929e836d2bc7b68966d6d073f', 'app/tools/failure_redaction_rules.json': '092cc5071abb481602e746b08faba67a8fe43c7f5a59bc3e28181f76dc245b2d', 'tools/failure_redaction.ps1': 'c81ce889aef0d216985a9dbaf439c2bfa11640d929e836d2bc7b68966d6d073f', 'tools/failure_redaction_rules.json': '092cc5071abb481602e746b08faba67a8fe43c7f5a59bc3e28181f76dc245b2d'}, 'c83c72c26fd4352803500792abd27096b28c58de': {'app/auto-update-settings.json': '7cf5b12cbea56e4f97707bf32f641c0e536ec42236c5d3c907f16facc2415bf9', 'app/auto_update.py': 'b59bf9443939f4cf634d732f3eb699bdf62785aa8a3f3baf85b8a4ab5bbc0804', 'app/config/update-manifest-key-config.json': 'd76e3d4f62aecd83d07e1b9b37f8463e1bc170bae6cd9bae965a17e6ca4d2671', 'tools/apply_prepared_update.ps1': 'dc7e30404d7bd4277692f875120aff4f7afd5c01b84301ed9e95a2c03e844c83', 'app/label_data_generation.py': '31a8a2a16f037fe0bc7e1502ee8aeb725aafbbf4e72cd3c0f2af550d822a3b8f', 'app/label_worker_roster.py': '63d8fef44564fa9666b4a5b7494339e0fd91692e94eea42e8cbbcbb81fe4b6bd', 'app/kmtech_shared/data_generation.py': '14ffe4277dc60a32a6074abf3d56ec0b0dcf3d5accd372f6dd41f36de0e348c6', 'app/kmtech_shared/powershell/data_generation.ps1': 'b809e49985c73e0b1aedbe9dd3a8050177939f6fb088087e6d3e4c9bc05b5c96', 'app/failure_redaction.py': '58f77e71532747b439c9d5fcf8c52bbf620f974f51ca3b63ad0dcf764fea4af9', 'app/tools/failure_redaction.ps1': 'c81ce889aef0d216985a9dbaf439c2bfa11640d929e836d2bc7b68966d6d073f', 'app/tools/failure_redaction_rules.json': '092cc5071abb481602e746b08faba67a8fe43c7f5a59bc3e28181f76dc245b2d', 'tools/failure_redaction.ps1': 'c81ce889aef0d216985a9dbaf439c2bfa11640d929e836d2bc7b68966d6d073f', 'tools/failure_redaction_rules.json': '092cc5071abb481602e746b08faba67a8fe43c7f5a59bc3e28181f76dc245b2d'}, 'e88011c6fb6a773feb174ba29cad431edda54eeb': {'app/auto-update-settings.json': '7cf5b12cbea56e4f97707bf32f641c0e536ec42236c5d3c907f16facc2415bf9', 'app/auto_update.py': 'b59bf9443939f4cf634d732f3eb699bdf62785aa8a3f3baf85b8a4ab5bbc0804', 'app/config/update-manifest-key-config.json': 'd76e3d4f62aecd83d07e1b9b37f8463e1bc170bae6cd9bae965a17e6ca4d2671', 'tools/apply_prepared_update.ps1': 'dc7e30404d7bd4277692f875120aff4f7afd5c01b84301ed9e95a2c03e844c83', 'app/label_data_generation.py': '31a8a2a16f037fe0bc7e1502ee8aeb725aafbbf4e72cd3c0f2af550d822a3b8f', 'app/label_worker_roster.py': '63d8fef44564fa9666b4a5b7494339e0fd91692e94eea42e8cbbcbb81fe4b6bd', 'app/kmtech_shared/data_generation.py': '14ffe4277dc60a32a6074abf3d56ec0b0dcf3d5accd372f6dd41f36de0e348c6', 'app/kmtech_shared/powershell/data_generation.ps1': 'b809e49985c73e0b1aedbe9dd3a8050177939f6fb088087e6d3e4c9bc05b5c96', 'app/failure_redaction.py': '58f77e71532747b439c9d5fcf8c52bbf620f974f51ca3b63ad0dcf764fea4af9', 'app/tools/failure_redaction.ps1': 'c81ce889aef0d216985a9dbaf439c2bfa11640d929e836d2bc7b68966d6d073f', 'app/tools/failure_redaction_rules.json': '092cc5071abb481602e746b08faba67a8fe43c7f5a59bc3e28181f76dc245b2d', 'tools/failure_redaction.ps1': 'c81ce889aef0d216985a9dbaf439c2bfa11640d929e836d2bc7b68966d6d073f', 'tools/failure_redaction_rules.json': '092cc5071abb481602e746b08faba67a8fe43c7f5a59bc3e28181f76dc245b2d'}, 'e89c18879f5bf59ddcd550152bf26ffedc4d687a': {'app/auto-update-settings.json': '7cf5b12cbea56e4f97707bf32f641c0e536ec42236c5d3c907f16facc2415bf9', 'app/auto_update.py': 'b59bf9443939f4cf634d732f3eb699bdf62785aa8a3f3baf85b8a4ab5bbc0804', 'app/label_data_generation.py': '31a8a2a16f037fe0bc7e1502ee8aeb725aafbbf4e72cd3c0f2af550d822a3b8f', 'tools/apply_prepared_update.ps1': 'dc7e30404d7bd4277692f875120aff4f7afd5c01b84301ed9e95a2c03e844c83', 'app/config/update-manifest-key-config.json': 'd76e3d4f62aecd83d07e1b9b37f8463e1bc170bae6cd9bae965a17e6ca4d2671', 'app/kmtech_shared/data_generation.py': '14ffe4277dc60a32a6074abf3d56ec0b0dcf3d5accd372f6dd41f36de0e348c6', 'app/kmtech_shared/powershell/data_generation.ps1': 'b809e49985c73e0b1aedbe9dd3a8050177939f6fb088087e6d3e4c9bc05b5c96', 'app/failure_redaction.py': '58f77e71532747b439c9d5fcf8c52bbf620f974f51ca3b63ad0dcf764fea4af9', 'app/tools/failure_redaction.ps1': 'c81ce889aef0d216985a9dbaf439c2bfa11640d929e836d2bc7b68966d6d073f', 'app/tools/failure_redaction_rules.json': '092cc5071abb481602e746b08faba67a8fe43c7f5a59bc3e28181f76dc245b2d', 'tools/failure_redaction.ps1': 'c81ce889aef0d216985a9dbaf439c2bfa11640d929e836d2bc7b68966d6d073f', 'tools/failure_redaction_rules.json': '092cc5071abb481602e746b08faba67a8fe43c7f5a59bc3e28181f76dc245b2d'}, '0cf041446e6eb3cfecb64055c3affccb359c3685': {'app/auto-update-settings.json': '7cf5b12cbea56e4f97707bf32f641c0e536ec42236c5d3c907f16facc2415bf9', 'app/auto_update.py': 'b59bf9443939f4cf634d732f3eb699bdf62785aa8a3f3baf85b8a4ab5bbc0804', 'app/label_data_generation.py': '31a8a2a16f037fe0bc7e1502ee8aeb725aafbbf4e72cd3c0f2af550d822a3b8f', 'app/config/update-manifest-key-config.json': 'd76e3d4f62aecd83d07e1b9b37f8463e1bc170bae6cd9bae965a17e6ca4d2671', 'app/kmtech_shared/data_generation.py': '14ffe4277dc60a32a6074abf3d56ec0b0dcf3d5accd372f6dd41f36de0e348c6', 'app/kmtech_shared/powershell/data_generation.ps1': 'b809e49985c73e0b1aedbe9dd3a8050177939f6fb088087e6d3e4c9bc05b5c96', 'tools/apply_prepared_update.ps1': 'dc7e30404d7bd4277692f875120aff4f7afd5c01b84301ed9e95a2c03e844c83', 'app/failure_redaction.py': '58f77e71532747b439c9d5fcf8c52bbf620f974f51ca3b63ad0dcf764fea4af9', 'app/tools/failure_redaction.ps1': 'c81ce889aef0d216985a9dbaf439c2bfa11640d929e836d2bc7b68966d6d073f', 'app/tools/failure_redaction_rules.json': '092cc5071abb481602e746b08faba67a8fe43c7f5a59bc3e28181f76dc245b2d', 'tools/failure_redaction.ps1': 'c81ce889aef0d216985a9dbaf439c2bfa11640d929e836d2bc7b68966d6d073f', 'tools/failure_redaction_rules.json': '092cc5071abb481602e746b08faba67a8fe43c7f5a59bc3e28181f76dc245b2d'}, '4b7630f2ca25a08933a9482d8fed6946e79df688': {'app/auto-update-settings.json': '7cf5b12cbea56e4f97707bf32f641c0e536ec42236c5d3c907f16facc2415bf9', 'app/auto_update.py': 'b59bf9443939f4cf634d732f3eb699bdf62785aa8a3f3baf85b8a4ab5bbc0804', 'app/label_data_generation.py': '31a8a2a16f037fe0bc7e1502ee8aeb725aafbbf4e72cd3c0f2af550d822a3b8f', 'app/config/update-manifest-key-config.json': 'd76e3d4f62aecd83d07e1b9b37f8463e1bc170bae6cd9bae965a17e6ca4d2671', 'app/kmtech_shared/data_generation.py': '14ffe4277dc60a32a6074abf3d56ec0b0dcf3d5accd372f6dd41f36de0e348c6', 'app/kmtech_shared/powershell/data_generation.ps1': 'b809e49985c73e0b1aedbe9dd3a8050177939f6fb088087e6d3e4c9bc05b5c96', 'tools/apply_prepared_update.ps1': 'dc7e30404d7bd4277692f875120aff4f7afd5c01b84301ed9e95a2c03e844c83', 'app/failure_redaction.py': '58f77e71532747b439c9d5fcf8c52bbf620f974f51ca3b63ad0dcf764fea4af9', 'app/tools/failure_redaction.ps1': 'c81ce889aef0d216985a9dbaf439c2bfa11640d929e836d2bc7b68966d6d073f', 'app/tools/failure_redaction_rules.json': '092cc5071abb481602e746b08faba67a8fe43c7f5a59bc3e28181f76dc245b2d', 'tools/failure_redaction.ps1': 'c81ce889aef0d216985a9dbaf439c2bfa11640d929e836d2bc7b68966d6d073f', 'tools/failure_redaction_rules.json': '092cc5071abb481602e746b08faba67a8fe43c7f5a59bc3e28181f76dc245b2d'}, '640d770e62d41236fa51cf0e38112c331b023fe8': {'app/auto-update-settings.json': '7cf5b12cbea56e4f97707bf32f641c0e536ec42236c5d3c907f16facc2415bf9', 'app/auto_update.py': 'b59bf9443939f4cf634d732f3eb699bdf62785aa8a3f3baf85b8a4ab5bbc0804', 'app/label_data_generation.py': '31a8a2a16f037fe0bc7e1502ee8aeb725aafbbf4e72cd3c0f2af550d822a3b8f', 'app/config/update-manifest-key-config.json': 'd76e3d4f62aecd83d07e1b9b37f8463e1bc170bae6cd9bae965a17e6ca4d2671', 'app/kmtech_shared/data_generation.py': '14ffe4277dc60a32a6074abf3d56ec0b0dcf3d5accd372f6dd41f36de0e348c6', 'app/kmtech_shared/powershell/data_generation.ps1': 'b809e49985c73e0b1aedbe9dd3a8050177939f6fb088087e6d3e4c9bc05b5c96', 'tools/apply_prepared_update.ps1': 'dc7e30404d7bd4277692f875120aff4f7afd5c01b84301ed9e95a2c03e844c83', 'app/failure_redaction.py': '58f77e71532747b439c9d5fcf8c52bbf620f974f51ca3b63ad0dcf764fea4af9', 'app/tools/failure_redaction.ps1': 'c81ce889aef0d216985a9dbaf439c2bfa11640d929e836d2bc7b68966d6d073f', 'app/tools/failure_redaction_rules.json': '092cc5071abb481602e746b08faba67a8fe43c7f5a59bc3e28181f76dc245b2d', 'tools/failure_redaction.ps1': 'c81ce889aef0d216985a9dbaf439c2bfa11640d929e836d2bc7b68966d6d073f', 'tools/failure_redaction_rules.json': '092cc5071abb481602e746b08faba67a8fe43c7f5a59bc3e28181f76dc245b2d'}, '134bab3c2059ce500f752a3ba09eb83a6d2c0b40': {'app/auto-update-settings.json': '7cf5b12cbea56e4f97707bf32f641c0e536ec42236c5d3c907f16facc2415bf9', 'app/auto_update.py': 'b59bf9443939f4cf634d732f3eb699bdf62785aa8a3f3baf85b8a4ab5bbc0804', 'app/label_data_generation.py': '31a8a2a16f037fe0bc7e1502ee8aeb725aafbbf4e72cd3c0f2af550d822a3b8f', 'app/label_worker_roster.py': '63d8fef44564fa9666b4a5b7494339e0fd91692e94eea42e8cbbcbb81fe4b6bd', 'app/config/update-manifest-key-config.json': 'd76e3d4f62aecd83d07e1b9b37f8463e1bc170bae6cd9bae965a17e6ca4d2671', 'app/kmtech_shared/data_generation.py': '14ffe4277dc60a32a6074abf3d56ec0b0dcf3d5accd372f6dd41f36de0e348c6', 'app/kmtech_shared/powershell/data_generation.ps1': 'b809e49985c73e0b1aedbe9dd3a8050177939f6fb088087e6d3e4c9bc05b5c96', 'tools/apply_prepared_update.ps1': 'dc7e30404d7bd4277692f875120aff4f7afd5c01b84301ed9e95a2c03e844c83', 'app/failure_redaction.py': '58f77e71532747b439c9d5fcf8c52bbf620f974f51ca3b63ad0dcf764fea4af9', 'app/tools/failure_redaction.ps1': 'c81ce889aef0d216985a9dbaf439c2bfa11640d929e836d2bc7b68966d6d073f', 'app/tools/failure_redaction_rules.json': '092cc5071abb481602e746b08faba67a8fe43c7f5a59bc3e28181f76dc245b2d', 'tools/failure_redaction.ps1': 'c81ce889aef0d216985a9dbaf439c2bfa11640d929e836d2bc7b68966d6d073f', 'tools/failure_redaction_rules.json': '092cc5071abb481602e746b08faba67a8fe43c7f5a59bc3e28181f76dc245b2d'}, '464830d78a305acb7aa01a7a7afc9e72a924a5c9': {'app/failure_redaction.py': '58f77e71532747b439c9d5fcf8c52bbf620f974f51ca3b63ad0dcf764fea4af9', 'app/tools/failure_redaction.ps1': 'c81ce889aef0d216985a9dbaf439c2bfa11640d929e836d2bc7b68966d6d073f', 'app/tools/failure_redaction_rules.json': '092cc5071abb481602e746b08faba67a8fe43c7f5a59bc3e28181f76dc245b2d', 'tools/failure_redaction.ps1': 'c81ce889aef0d216985a9dbaf439c2bfa11640d929e836d2bc7b68966d6d073f', 'tools/failure_redaction_rules.json': '092cc5071abb481602e746b08faba67a8fe43c7f5a59bc3e28181f76dc245b2d'}, 'a56a58769c43d1b19abdd9881425f95a49dbd7f7': {'app/auto-update-settings.json': '7cf5b12cbea56e4f97707bf32f641c0e536ec42236c5d3c907f16facc2415bf9', 'app/auto_update.py': 'b59bf9443939f4cf634d732f3eb699bdf62785aa8a3f3baf85b8a4ab5bbc0804', 'app/config/update-manifest-key-config.json': 'd76e3d4f62aecd83d07e1b9b37f8463e1bc170bae6cd9bae965a17e6ca4d2671', 'app/failure_redaction.py': '58f77e71532747b439c9d5fcf8c52bbf620f974f51ca3b63ad0dcf764fea4af9', 'app/tools/failure_redaction.ps1': 'c81ce889aef0d216985a9dbaf439c2bfa11640d929e836d2bc7b68966d6d073f', 'app/tools/failure_redaction_rules.json': '092cc5071abb481602e746b08faba67a8fe43c7f5a59bc3e28181f76dc245b2d', 'tools/apply_prepared_update.ps1': 'dc7e30404d7bd4277692f875120aff4f7afd5c01b84301ed9e95a2c03e844c83', 'tools/failure_redaction.ps1': 'c81ce889aef0d216985a9dbaf439c2bfa11640d929e836d2bc7b68966d6d073f', 'tools/failure_redaction_rules.json': '092cc5071abb481602e746b08faba67a8fe43c7f5a59bc3e28181f76dc245b2d'}, '2c46de16f2303117e88d4794bf3793f0d7de8c76': {'app/auto-update-settings.json': '7cf5b12cbea56e4f97707bf32f641c0e536ec42236c5d3c907f16facc2415bf9', 'app/auto_update.py': 'b59bf9443939f4cf634d732f3eb699bdf62785aa8a3f3baf85b8a4ab5bbc0804', 'app/config/update-manifest-key-config.json': 'd76e3d4f62aecd83d07e1b9b37f8463e1bc170bae6cd9bae965a17e6ca4d2671', 'app/failure_redaction.py': '58f77e71532747b439c9d5fcf8c52bbf620f974f51ca3b63ad0dcf764fea4af9', 'app/tools/failure_redaction.ps1': 'c81ce889aef0d216985a9dbaf439c2bfa11640d929e836d2bc7b68966d6d073f', 'app/tools/failure_redaction_rules.json': '092cc5071abb481602e746b08faba67a8fe43c7f5a59bc3e28181f76dc245b2d', 'tools/apply_prepared_update.ps1': 'dc7e30404d7bd4277692f875120aff4f7afd5c01b84301ed9e95a2c03e844c83', 'tools/failure_redaction.ps1': 'c81ce889aef0d216985a9dbaf439c2bfa11640d929e836d2bc7b68966d6d073f', 'tools/failure_redaction_rules.json': '092cc5071abb481602e746b08faba67a8fe43c7f5a59bc3e28181f76dc245b2d'}, '55cab723e215ab3ffd12ef7e3cc9e8d3c1569e8a': {}}
startup_public_key_pair = ('f570df63d4dd49f5ac5db2d414839fa0372a37605c7bdaddd385f684c948aae3', 'd76e3d4f62aecd83d07e1b9b37f8463e1bc170bae6cd9bae965a17e6ca4d2671')
startup_candidate_membership = [('auto_update_copy_prepared_file', 'auto_update.py', 'copy_prepared_file', 'decorator'), ('auto_update_create_download_stage', 'auto_update.py', 'create_download_stage', 'decorator'), ('auto_update_create_protected_stage', 'auto_update.py', 'create_protected_stage', 'decorator'), ('auto_update_extract_prepared_code', 'auto_update.py', 'extract_prepared_code', 'decorator'), ('auto_update_fixed_helper', 'auto_update.py', 'fixed_helper', 'decorator'), ('auto_update_launch_version', 'auto_update.py', 'launch_version', 'decorator'), ('auto_update_persist_switch', 'auto_update.py', 'persist_switch', 'decorator'), ('auto_update_preserve_candidate_version', 'auto_update.py', 'preserve_candidate_version', 'decorator'), ('auto_update_publish_download', 'auto_update.py', 'publish_download', 'decorator'), ('auto_update_request_apply_task', 'auto_update.py', 'request_apply_task', 'decorator'), ('auto_update_request_bootstrap', 'auto_update.py', 'request_bootstrap', 'decorator'), ('auto_update_restore_preimage', 'INSTALL_THIS_PC.ps1', 'Enter-LabelWriterDelegatedOperation', 'powershell_delegated_operation'), ('auto_update_run_download_attempt', 'auto_update.py', '_run_download_attempt', 'decorator'), ('auto_update_stage_prepared_copies', 'auto_update.py', 'stage_prepared_copies', 'decorator'), ('auto_update_supported_lifecycle_transition', 'auto_update.py', 'supported_lifecycle_transition', 'decorator'), ('auto_update_task_mutation', 'INSTALL_THIS_PC.ps1', 'Enter-LabelWriterDelegatedOperation', 'powershell_delegated_operation'), ('auto_update_write_download_bytes', 'auto_update.py', 'write_download_bytes', 'decorator'), ('auto_update_write_download_chunk', 'auto_update.py', 'write_download_chunk', 'decorator'), ('auto_update_write_json_durable', 'auto_update.py', 'write_json_durable', 'decorator'), ('canonical_placement', 'INSTALL_THIS_PC.ps1', 'Enter-LabelWriterDelegatedOperation', 'powershell_delegated_operation'), ('current_user_onboarding', 'current_user_onboarding.py', 'onboard_current_user', 'decorator'), ('current_user_setup_removal', 'current_user_onboarding.py', 'remove_current_user_setup', 'decorator'), ('data_generation_record', 'label_data_generation.py', 'record_user_data_open', 'decorator'), ('direct_sync_enqueue', 'direct_sync_runtime.py', 'enqueue_completed_source_file', 'decorator'), ('direct_sync_relay_cycle', 'direct_sync_runtime.py', 'run_relay_once', 'decorator'), ('direct_sync_upload', 'direct_sync_push.py', 'upload_source_file', 'decorator'), ('gui_deferred_capture_cancel', 'Label_Match.py', 'Label_Match._cancel_deferred_capture_for_set', 'decorator'), ('gui_direct_sync', 'Label_Match.py', '_label_match_run_and_record_session_direct_sync', 'decorator'), ('gui_label_recovery_hold', 'Label_Match.py', 'Label_Match._hold_label_recovery', 'decorator'), ('gui_package_cancel_enqueue', 'Label_Match.py', 'Label_Match._queue_authoritative_package_cancellation', 'decorator'), ('gui_package_enqueue', 'Label_Match.py', 'Label_Match._queue_authoritative_package', 'decorator'), ('gui_package_pin_changed_hold_recovery', 'Label_Match.py', 'Label_Match._resume_changed_pin_hold', 'decorator'), ('gui_package_pin_changed_restore', 'Label_Match.py', 'Label_Match._restore_changed_pin_file', 'decorator'), ('gui_package_pin_file_move_recovery', 'Label_Match.py', 'Label_Match._recover_package_pin_moves', 'decorator'), ('gui_package_recovery_audit', 'Label_Match.py', 'Label_Match._audit_package_recovery_action', 'decorator'), ('gui_package_recovery_hold', 'Label_Match.py', 'Label_Match._hold_package_recovery_set', 'decorator'), ('gui_shutdown_stall_notice', 'label_match_single_instance.py', '_set_shutdown_stall_notice', 'decorator'), ('label_admin_pin_attempt_audit', 'package_outbox.py', 'PackageOutbox.audit_pin_attempt', 'decorator'), ('label_admin_pin_changed_hold_prepare', 'label_admin_pin.py', 'AdminPinIntentStore.stage_changed_hold', 'decorator'), ('label_admin_pin_changed_hold_state', 'label_admin_pin.py', 'AdminPinIntentStore.finish_changed_hold', 'decorator'), ('label_admin_pin_file_move', 'label_admin_pin.py', 'AdminPinIntentStore.stage_file_move', 'decorator'), ('label_admin_pin_file_move_state', 'label_admin_pin.py', 'AdminPinIntentStore.finish_file_move', 'decorator'), ('label_admin_pin_intent_prepare', 'label_admin_pin.py', 'AdminPinIntentStore.prepare', 'decorator'), ('label_admin_pin_intent_transition', 'label_admin_pin.py', 'AdminPinIntentStore.transition', 'decorator'), ('label_admin_pin_request', 'label_admin_pin.py', 'AdminPinClient._post_path', 'decorator'), ('package_cancel', 'package_logistics.py', 'PackageLogisticsClient.cancel_package', 'decorator'), ('package_cancellation_drain', 'package_logistics.py', 'PackageCancellationOutboxProcessor.drain', 'decorator'), ('package_create', 'package_logistics.py', 'PackageLogisticsClient.create_package', 'decorator'), ('package_operation_lease', 'package_logistics.py', 'PackageLogisticsClient.issue_operation_lease', 'decorator'), ('package_outbox_drain', 'package_logistics.py', 'PackageOutboxProcessor.drain', 'decorator'), ('package_transfer_reseal', 'package_logistics.py', 'PackageLogisticsClient.replace_and_reseal_transfer', 'decorator'), ('persistent_relay_cycle', 'user_relay.py', 'run_persistent_relay_loop', 'bounded_context'), ('persistent_relay_status', 'user_relay.py', 'run_persistent_relay_loop', 'bounded_context'), ('phs_current_recovery', 'phs_label_workflow.py', 'PHSLabelExchangeCoordinator._resume_current', 'decorator'), ('phs_exchange_activate', 'package_logistics.py', 'PackageLogisticsClient.activate_phs_label_exchange', 'decorator'), ('phs_exchange_prepare', 'package_logistics.py', 'PackageLogisticsClient.prepare_phs_label_exchange', 'decorator'), ('phs_label_adopt', 'package_logistics.py', 'PackageLogisticsClient.adopt_phs_label', 'decorator'), ('phs_print_complete', 'package_logistics.py', 'PackageLogisticsClient.complete_phs_label_print', 'decorator'), ('phs_print_request', 'package_logistics.py', 'PackageLogisticsClient.request_phs_label_print', 'decorator'), ('phs_reconciliation_prepare', 'package_logistics.py', 'PackageLogisticsClient.prepare_phs_reconciliation_label_exchange', 'decorator'), ('phs_reconciliation_recovery', 'phs_label_workflow.py', 'PHSLabelExchangeCoordinator.recover_reconciliation', 'decorator'), ('raw_relay_runner', 'tools/direct_sync_relay_runner.py', 'main', 'decorator'), ('registration_relay_recovery', 'registration_relay_recovery.py', 'commit_relay_handoff', 'decorator'), ('relay_batch_claim', 'direct_sync_push.py', 'claim_next_relay_batch', 'decorator'), ('relay_batch_drain', 'direct_sync_push.py', 'drain_one_relay_batch', 'decorator'), ('relay_child_launch', 'user_relay.py', 'run_session_direct_sync_once', 'decorator'), ('relay_queue_schema', 'direct_sync_push.py', 'init_relay_queue_schema', 'decorator'), ('relay_spool_enqueue', 'direct_sync_push.py', 'enqueue_source_file_for_relay', 'decorator'), ('relay_stale_lease_reset', 'direct_sync_push.py', 'reset_stale_relay_leases', 'decorator'), ('runtime_lease_network', 'producer_runtime_client.py', '_post_lease_request', 'decorator'), ('scheduled_task_remove', 'current_user_scheduled_task.py', 'remove_current_user_scheduled_task', 'decorator'), ('sealed_transfer_attempt', 'sealed_transfer_exchange.py', 'SealedTransferExchangeCoordinator.attempt', 'decorator'), ('sealed_transfer_drain', 'sealed_transfer_exchange.py', 'SealedTransferExchangeCoordinator.drain_pending', 'decorator'), ('sealed_transfer_prepare', 'sealed_transfer_exchange.py', 'SealedTransferExchangeCoordinator.prepare', 'decorator'), ('user_relay_autostart_install', 'user_relay.py', 'install_user_relay_autostart', 'decorator'), ('user_relay_autostart_remove', 'user_relay.py', 'remove_user_relay_autostart', 'decorator'), ('user_relay_process_start', 'user_relay.py', 'start_user_relay_process', 'decorator'), ('user_relay_stop_release', 'user_relay.py', 'release_user_relay_stop_marker', 'decorator'), ('user_relay_stop_request', 'user_relay.py', 'request_user_relay_stop', 'decorator'), ('worker_admin_recovery_network', 'tools/register_label_match_worker_pc.py', '_admin_recover', 'decorator'), ('worker_enrollment_network', 'tools/register_label_match_worker_pc.py', '_enroll', 'decorator'), ('worker_partial_recovery_network', 'tools/register_label_match_worker_pc.py', '_partial_recovery_request', 'decorator')]
installed_manifest = json.loads((installed / 'portable-manifest.json').read_text(encoding='utf-8-sig'))
startup_from = installed_manifest.get('source_commit')
startup_changes = startup_release_replacements.get(startup_from)
if startup_changes is not None:
    startup_changes = dict(startup_changes)
    # Preserve the existing exact scanner Label preimage with its e88011c
    # companions; every other byte still follows the same committed declaration.
    startup_scanner = startup_from.startswith('e88011c') and release_digest(installed/'app/Label_Match.py') == scanner_label_pair[0]
    if startup_scanner:
        startup_changes['app/Label_Match.py'] = scanner_label_pair
        # Exact e880 companions with the already accepted scanner preimage and recomputed pins.
        startup_changes.update({'app/writer_session_fence.py': ('825d5337385e3870ab4d83db1b63d9dea4d1fc9e9c528d7948415ca20256ae87', '3076b56b1dbf5c8023d4866942a91d782388845f3af5f83bae878fd2b72b0dab'), 'tools/label_writer_fence.ps1': ('6ee3e545a1b3d9bf6e4722a2802e57bca8276d92593fe480c8990501a8db7b8a', '5ad24d6d46f3f59d98497bb1147c9a0fc378428697b62c5621a9000bae4af81a')})
    allowed_additions = dict(startup_release_additions[startup_from])
    settings_path=source/'app/auto-update-settings.json'
    settings_raw=settings_path.read_bytes()
    try:
        settings_canonical=canonical_startup_settings(json.loads(settings_raw))
        if settings_raw != settings_canonical:raise ValueError('noncanonical settings')
    except (ValueError,TypeError):
        raise ValueError('WRITER_TRANSITION_ADDED_ASSET_DIFFERS: startup settings') from None
    if 'app/auto-update-settings.json' in allowed_additions:
        allowed_additions['app/auto-update-settings.json']=hashlib.sha256(settings_canonical).hexdigest()
    all_directions = {**startup_changes}
    if source_admission_transition:
        all_directions.update(source_admission_companions)
    candidate_pin,candidate_sources=identity(source)
    if candidate_sources != startup_candidate_membership:
        raise ValueError('WRITER_TRANSITION_SOURCE_SET_DIFFERS')
    _, startup_installed_sources = identity(installed)
    generation_writer = ('data_generation_record', 'label_data_generation.py', 'record_user_data_open', 'decorator')
    startup_generation_delta = generation_writer in candidate_sources and generation_writer not in startup_installed_sources
    startup_addition_already_installed = any((installed / relative).exists() for relative in allowed_additions)
    def startup_delta_reason(relative, ordinary):
        # Preserve the approved G membership refusal for invalid code deltas.
        # This changes only the reason at an already-failing exact byte check.
        python_source = relative.startswith('app/') and relative.endswith('.py') and not relative.startswith('app/site-packages/')
        if startup_generation_delta and not startup_addition_already_installed:
            # Keep the historical e880 profile refusal at its onboarding boundary.
            if startup_from.startswith('e88011c') and ordinary == 'WRITER_TRANSITION_SEMANTICS_DIFFER' and relative in {
                'app/logistics_runtime_profile.py', 'app/tools/install_logistics_runtime_profile.py', 'app/enrollment_mutex.py',
            }:
                return 'WRITER_TRANSITION_SEMANTICS_DIFFER: app/current_user_onboarding.py'
            if python_source or relative == 'tools/label_writer_fence.ps1':
                return 'WRITER_TRANSITION_SOURCE_SET_DIFFERS'
        return ordinary + ': ' + relative
    key_path='app/update-manifest-key-config.json'
    old_key_digest=release_digest(installed/key_path)
    new_key_digest=release_digest(source/key_path)
    if old_key_digest != new_key_digest and (old_key_digest,new_key_digest) != startup_public_key_pair:
        raise ValueError('WRITER_TRANSITION_PUBLIC_KEY_PAIR_DIFFERS')
    if old_key_digest != new_key_digest:all_directions[key_path]=(old_key_digest,new_key_digest)
    for directory in ('app','runtime'):
        left,right=files(source,directory),files(installed,directory)
        expected_additions={p for p in allowed_additions if p.startswith(directory+'/')}
        if left.keys() != right.keys() | expected_additions:
            raise ValueError('WRITER_TRANSITION_SOURCE_SET_DIFFERS')
        for relative,path in left.items():
            if relative in expected_additions:
                if relative in right:raise ValueError('WRITER_TRANSITION_SEMANTICS_DIFFER: '+relative)
                if release_digest(path) != allowed_additions[relative]:raise ValueError(startup_delta_reason(relative, 'WRITER_TRANSITION_ADDED_ASSET_DIFFERS'))
            elif relative in all_directions:
                before,after=all_directions[relative]
                if release_digest(right[relative]) != before or release_digest(path) != after:raise ValueError(startup_delta_reason(relative, 'WRITER_TRANSITION_SEMANTICS_DIFFER'))
            elif digest(path) != digest(right[relative]):
                raise ValueError(startup_delta_reason(relative, 'WRITER_TRANSITION_SEMANTICS_DIFFER'))
    for relative in ('INSTALL_THIS_PC.ps1','launch-label-match.cmd','tools/bootstrap_integrity.ps1','tools/label_writer_fence.ps1','tools/label_writer_fence_contract.json','tools/apply_prepared_update.ps1'):
        if relative in allowed_additions:
            if (installed/relative).exists() or release_digest(source/relative)!=allowed_additions[relative]:raise ValueError('WRITER_TRANSITION_ADDED_ASSET_DIFFERS')
        elif relative in all_directions:
            before,after=all_directions[relative]
            if release_digest(installed/relative)!=before or release_digest(source/relative)!=after:raise ValueError(startup_delta_reason(relative, 'WRITER_TRANSITION_CONTRACT_DIFFERS'))
        elif digest(source/relative)!=digest(installed/relative):raise ValueError(startup_delta_reason(relative, 'WRITER_TRANSITION_CONTRACT_DIFFERS'))
    compatibility=('PINNED_DATA_GENERATION_G_CORRECTION' if startup_from in ('a56a58769c43d1b19abdd9881425f95a49dbd7f7', '2c46de16f2303117e88d4794bf3793f0d7de8c76') else 'PINNED_WORKER_RECORD_IDENTITY' if startup_from == '0cf041446e6eb3cfecb64055c3affccb359c3685' else 'PINNED_MAINLOOP_EXIT_CLEANUP' if startup_from == '4b7630f2ca25a08933a9482d8fed6946e79df688' else 'PINNED_MAINLOOP_EXIT_NOTICE' if startup_from == '640d770e62d41236fa51cf0e38112c331b023fe8' else 'PINNED_DATA_GENERATION_G' if startup_from == 'e89c18879f5bf59ddcd550152bf26ffedc4d687a' else 'PINNED_LEGACY_LABEL_TRANSITION_MODE' if startup_from.startswith('3f4574a') else 'PINNED_SCAN_INPUT_FIX' if startup_from.startswith(('c83c72c','817d393','92abdaa')) else 'PINNED_W9NEXTFIX_LABEL_RECOVERY' if startup_from.startswith('e88011c') else 'PINNED_STARTUP_UPDATE_BOOTSTRAP')
    if startup_scanner: compatibility='PINNED_W9SCANONLY_WORKFLOW'
    candidate_pin,candidate_sources=identity(source)
    installed_pin,installed_sources=identity(installed)
    print(json.dumps(dict(installed_inventory_sha256=installed_pin,candidate_inventory_sha256=candidate_pin,compatibility=compatibility)))
    sys.exit(0)

candidate_pin, candidate_sources = identity(source)
installed_pin, installed_sources = identity(installed)
if roster_transition:
    # This exact signed-transport pair moves the existing decorated request
    # into the common route helper. Every other writer identity stays equal;
    # executable deltas still require the complete reviewed release below.
    old_request = ('label_admin_pin_request', 'label_admin_pin.py', 'AdminPinClient.post', 'decorator')
    new_request = ('label_admin_pin_request', 'label_admin_pin.py', 'AdminPinClient._post_path', 'decorator')
    installed_sources = [new_request if row == old_request else row for row in installed_sources]
f5_retirement_replacements = {
    'app/phs_label_workflow.py': ({'c1746fe550017bccdf8b8fd6b98ed1438cd49b5878d98328cceec7d6cb6edd7c', '0d44faf4128286d49fc4285d26b9901d563c86eba0fe62d669c53a4a382c7b7d'}, '72bb834611c5172e3cb9a0c517b49e4a664adcffef05006f90015a6384584ae0'),
    'app/package_logistics.py': ({'4585720051d266fa077005456b0da590b273ad8ab29bc5391cff2e6aa9f74224', '818a1d7fa228e125c30c23f6950b6d579c69c22fda1bd9f2d9aded9e9b4201a1'}, '732b2449cee55b15e9c687a542814f76c5dc0a0c49337ec018194caa7960b3c1'),
    'app/label_workbench_history.py': ({'6f07e89f69d66bb979abaef58aa4ff514fb839756bc9f4b5b727b5c0cf4d2022', '214ee81fdc36cfc51d6b59c15ee69f4f5057ebfc899eced18f7b61c0daff50f8'}, '30743e22b77939bed5d79b1e490c6df76ab577a242327a2d281f164c93ef1ca5'),
}
f5_retirement_transition = all(
    (installed / relative).is_file() and (source / relative).is_file()
    and release_digest(installed / relative) in befores
    and release_digest(source / relative) == after
    for relative, (befores, after) in f5_retirement_replacements.items()
)
if f5_retirement_transition:
    # The exact reviewed workflow bytes retire only new F5 starts and move
    # the existing single recovery guard into its shared internal resume.
    # All remaining identities still require equality and reviewed bytes.
    retired_f5_sources = {
        ('phs_single_exchange', 'phs_label_workflow.py', 'PHSLabelExchangeCoordinator.execute_single', 'decorator'),
        ('phs_reconciliation_exchange', 'phs_label_workflow.py', 'PHSLabelExchangeCoordinator.execute_reconciliation', 'decorator'),
    }
    old_f5_recovery = ('phs_current_recovery', 'phs_label_workflow.py', 'PHSLabelExchangeCoordinator.recover_current', 'decorator')
    new_f5_recovery = ('phs_current_recovery', 'phs_label_workflow.py', 'PHSLabelExchangeCoordinator._resume_current', 'decorator')
    if (retired_f5_sources <= set(installed_sources)
            and old_f5_recovery in installed_sources
            and new_f5_recovery in candidate_sources):
        installed_sources = sorted(
            new_f5_recovery if row == old_f5_recovery else row
            for row in installed_sources if row not in retired_f5_sources
        )
if data_generation_transition:
    added_source = ('data_generation_record', 'label_data_generation.py', 'record_user_data_open', 'decorator')
    if added_source in candidate_sources and added_source not in installed_sources:
        installed_sources = sorted([*installed_sources, added_source])
if data_generation_reverse:
    added_source = ('data_generation_record', 'label_data_generation.py', 'record_user_data_open', 'decorator')
    if added_source not in installed_sources or added_source in candidate_sources:
        raise ValueError('WRITER_TRANSITION_SOURCE_SET_DIFFERS')
    installed_sources = [row for row in installed_sources if row != added_source]
# Only this exact forward repair may introduce these four guarded sinks.
if quiesced_transport_transition and all(row in candidate_sources for row in quiesced_transport_sources):
    installed_sources = sorted(set(installed_sources) | set(quiesced_transport_sources))
# Defer a reverse membership delta solely so the unchanged semantic checks
# report the original file-specific downgrade error; it never authorizes it.
quiesced_transport_reverse = all(row in installed_sources for row in quiesced_transport_sources) and all(
    row not in candidate_sources for row in quiesced_transport_sources)
if quiesced_transport_reverse:
    installed_sources = [row for row in installed_sources if row not in quiesced_transport_sources]
# Defer only the reviewed shutdown-notice membership delta. Exact release
# bytes still have to pass the ordinary directional file checks below, so
# re-pinning an altered module cannot admit it. Reverse deltas are deferred
# only to retain the existing semantic downgrade rejection.
shutdown_notice_source = ('gui_shutdown_stall_notice', 'label_match_single_instance.py', '_set_shutdown_stall_notice', 'decorator')
shutdown_notice_forward = (shutdown_notice_source not in installed_sources
    and candidate_sources == sorted(installed_sources + [shutdown_notice_source]))
shutdown_notice_reverse = (shutdown_notice_source not in candidate_sources
    and installed_sources == sorted(candidate_sources + [shutdown_notice_source]))
if candidate_sources != installed_sources and not (shutdown_notice_forward or shutdown_notice_reverse):
    raise ValueError('WRITER_TRANSITION_SOURCE_SET_DIFFERS')
# Exact stage-one 0cf0414 to stage-two bytes; state and pending stay additive.
worker_record_replacements = {'app/Label_Match.py': ('5f16f57e6eaf9e73fc54c3b2f58eaf3f9f5841d9de2fbfd5a74a63c969e38c92',
                        '43c93e151623c6098ab6dc0b200d5d44c415cdbe12f2e3be97c3172c41d270e9'),
 'app/label_completion.py': ('7650944f6d9f7431d8089b373a32aa649c324ecacfb8c24b40f13ccc2f2fdb55',
                             'ed37d0733e62e375fcd85f873b2459f5fd6a3e7d68ced48f36200cb5e871c0ac'),
 'app/label_data_manager.py': ('eb4f4bef03152c05c6ef160abb7738dfa8416c971bb79baac4092d52905c14ec',
                               '2ddd2129d6de99073f3cd51d8947a4792ea56e371205f3ea70445301317f4e56'),
 'app/label_recovery_schema.py': ('01fdfd2616bdc6a2a713ed89f32f23b73db9fe2d2e86d90482023e9950a178f7',
                                  '4bf2ad6024efd6c3ac2f5965eef3e81fa149078f4bc22ff716272a5c2359ffa6'),
 'app/label_worker_roster.py': ('17b35efc3fb9f9a4589590d1cc8b0accaf2eba1cb9a482ffeaf669e12dc6ce32',
                                '63d8fef44564fa9666b4a5b7494339e0fd91692e94eea42e8cbbcbb81fe4b6bd'),
 'app/package_command_draft.py': ('4936fdccd3af20e7f48c0fc0bdb3eb41e6baee073bf8dea90252adb487c60563',
                                  '8144882b47008ee3e2c35cbd62bdf70637d34d89a5e1ade7620ef405cc7423ac'),
 'app/package_outbox_processor.py': ('b7dc5263d0d41715b82c3e8c54e95951007eedbefcb9a4a90900aa3a7407a0e3',
                                     'bc8cbc44c0b573df1a658fc09ec980e3965196680d8c7294daa52846b334ded1'),
 'app/sealed_transfer_exchange.py': ('74c5d2c126537b316b2f9d8d5127853c1e4217448fe3a2656f551eaafe529d1c',
                                     '1202d1fecf10ed6e6d6e7662ca9c87ff4aa2553d14e4f8fa67210f770b8ab755'),
 'app/writer_session_fence.py': ('e649873611c442930e8d635c5836752ea1203273676a3269443a9bf403755bca',
                                 '92419d83453e49cd225e79ccc1aa96230c7ba00636b7fd114ed02736e6c074e1'),
 'tools/label_writer_fence.ps1': ('e3256e6129ab819075d38cb582559a25d312683c57995e1cfde92dbae66feb89',
                                  '44ad315d0ffe526f003e3d8d17ff0940338652e7e557ad22ed6e728551ce3d3f'),
 'app/label_match_single_instance.py': ('64040b936adb201d21ecbf55d47536b545a8c10048200a4337635f0a7b00bb02',
                                        'f16db6f775d66233f96e274ea9b75260b6694800f60152e4a9edb133e9de93ee')}
worker_record_transition = all(
    (installed / relative).is_file() and (source / relative).is_file()
    and release_digest(installed / relative) == before
    and release_digest(source / relative) == after
    for relative, (before, after) in worker_record_replacements.items()
)
# Exact 4b7630f shutdown repair; durable formats are unchanged.
mainloop_exit_replacements = {'app/Label_Match.py': ('40d1367865b7cc0c85db8db6b3e6ab61932dc8ed14673d8a785279580d283e70',
                        '43c93e151623c6098ab6dc0b200d5d44c415cdbe12f2e3be97c3172c41d270e9'),
 'app/label_match_single_instance.py': ('64040b936adb201d21ecbf55d47536b545a8c10048200a4337635f0a7b00bb02',
                                        'f16db6f775d66233f96e274ea9b75260b6694800f60152e4a9edb133e9de93ee'),
 'app/writer_session_fence.py': ('bd007a223dda4e53195d9af3ed3c876b72fe6f7d49447d38ce30a830104c6ed8',
                                 '92419d83453e49cd225e79ccc1aa96230c7ba00636b7fd114ed02736e6c074e1'),
 'tools/label_writer_fence.ps1': ('500955366215a113f3c0ffd284d86d6364aadb86e00b097a4cefedb3cbb1ff7e',
                                  '44ad315d0ffe526f003e3d8d17ff0940338652e7e557ad22ed6e728551ce3d3f')}
mainloop_exit_transition = all(
    (installed / relative).is_file() and (source / relative).is_file()
    and release_digest(installed / relative) == before
    and release_digest(source / relative) == after
    for relative, (before, after) in mainloop_exit_replacements.items()
)
# Exact prior cleanup candidate -> corrected notice-and-wait policy.
mainloop_exit_notice_replacements = {'app/Label_Match.py': ('75fdd2d1422825f52320d3373acd12366b244e8ca00aa531a052983c09694227',
                        '43c93e151623c6098ab6dc0b200d5d44c415cdbe12f2e3be97c3172c41d270e9'),
 'app/label_match_single_instance.py': ('2929ffe042131ccc8cc905dbbad084be0fbab0585d70100252d3ee682c66346b',
                                        'f16db6f775d66233f96e274ea9b75260b6694800f60152e4a9edb133e9de93ee'),
 'app/writer_session_fence.py': ('5dc1377d481970f75c6a6330a2c2ea9987b016de4cea7f86f5788299a23b0f73',
                                 '92419d83453e49cd225e79ccc1aa96230c7ba00636b7fd114ed02736e6c074e1'),
 'tools/label_writer_fence.ps1': ('89dd2e7e3ac09fb125905a00f49e6ce3b2c6f9d5420b0d47535124336f36ecf9',
                                  '44ad315d0ffe526f003e3d8d17ff0940338652e7e557ad22ed6e728551ce3d3f')}
mainloop_exit_notice_transition = all(
    (installed / relative).is_file() and (source / relative).is_file()
    and release_digest(installed / relative) == before
    and release_digest(source / relative) == after
    for relative, (before, after) in mainloop_exit_notice_replacements.items()
)
# Exact already-G forward correction; no reverse or membership exception.
data_generation_correction_replacements = {'app/Label_Match.py': ('d0483a90b7c42923e4e638dbf45fbd3162768e6ca15fa2184fa7b71b0f84fda1', '43c93e151623c6098ab6dc0b200d5d44c415cdbe12f2e3be97c3172c41d270e9'), 'app/current_user_onboarding.py': ('fb6f7a7db58874eb761d1a4fe88964be49c74d9ce871db9ab3b8d41e285a3017', '93a4d7b67b9d740ddbf8bfa9546f01fc0c927ee0de985f4be4e0d83adf435ebd'), 'app/label_data_generation.py': ('d0e097579337cfbb27fbecc267eef4b58116876ab6bdc34a9e9caadb7cc8537c', '31a8a2a16f037fe0bc7e1502ee8aeb725aafbbf4e72cd3c0f2af550d822a3b8f'), 'app/label_match_product_host.py': ('bce7176d636a93c559340850d91894ffd02a4eef2a3f953d2ae05cd1cb0f9fbf', '2f68dda99e3bc9e0d6e1c325cb43d169845df3501050a354c624a44690097845'), 'app/user_relay.py': ('3b71cb47a1a4a3253ac0602804e5770e40e6b674004174838d3a00016c3e8487', '1015c0d1026d69745d70bbe1d69ab0f5a97cec52eaf8831ecc95d0356d8f4d3b'), 'app/writer_session_fence.py': ('46731dc32ab372b721dcd59297764e0bc99cb4c2ca92ae5bf66157b3fb90593e', '92419d83453e49cd225e79ccc1aa96230c7ba00636b7fd114ed02736e6c074e1'), 'app/tools/direct_sync_relay_install_pack.py': ('5a4368f2bb692dd37f406f449e607390024668a8101929577d88c549ad7093e5', '53cc1d41e90b0b9cda364ab744ce81abeaf720d222a7fe6263bd15728ec77d7f'), 'app/tools/direct_sync_relay_operator.py': ('ff97593c29669ad1f983d6f7be0fb1ebabf27adcf9661b797d36f7d583a0cedb', '3c990ebfd9ff356d5876b40d1098470d0602fae3a3ed838ccf0cbc644150032f'), 'app/tools/direct_sync_relay_runner.py': ('f7c2aa827bd6b829af0df5f0e7ec7809e4092187b0ad04404bd35a0856fdcdf5', '9b8e7a9845c4e73edea64b4ac3d0b90e3f461b7d926e0e1db64bdb9197f734ac'), 'app/tools/install_logistics_runtime_profile.py': ('eb8650ace3df179a7c68f3247efe5eac86200543a43f4b2c80a47ccc95b336ef', '7dde5875649e9ec9291a45e47191b9c6fcf6b32d66cafe70c0444390cefbab9e'), 'app/tools/label_auth_recovery_canary.py': ('440a0ed5061b522cafa56408092e3ed361b42f1ae09569258b08005a04cd463b', '079dfe927bcf721f4db49779eeab96b66093e1e02c69b86da73ea069d5b7c9be'), 'app/tools/label_exact_clone_resolution_receipt.py': ('c77e6be851f32cad9058ee52892a9b2520232a15914379aba3c28ad09c581a9b', 'd1e0a8955eed3f056cb964d4f0058f082664740b03b98eefc8f5ea872ded987c'), 'app/tools/register_label_match_worker_pc.py': ('469c2e2ebdc9ddb45973e1423336bd6619b485e1604732d4d7bec55c41f0931f', 'fbb7397c227cf6845e01124119cafc0081b9a0aa4d50c44703d339fc5dd8271b'), 'tools/label_writer_fence.ps1': ('6777bdfac69f830ac7800b4976e2ffc3b2b3cb0593f4e3c3a8cddd5855bb849b', '44ad315d0ffe526f003e3d8d17ff0940338652e7e557ad22ed6e728551ce3d3f'), 'app/protected_admin.py': ('f4b1616dc152c84e368a32acb06b424b4b4cdab588635e383fa22e7ee6d3c58e', 'eadfb46474595075b840bbdf87b919910c228ac5481fe62721a3497c478d63f9'), 'app/package_logistics.py': ('6899fd407c99066ba52ac698af0f01dbe762a56b300a6c28f1c6e56958dd5ef3', '732b2449cee55b15e9c687a542814f76c5dc0a0c49337ec018194caa7960b3c1'), 'app/direct_sync_runtime.py': ('50ff189da613b1e1bec5bbb32061acc76e8d29ecae8808f5f591854a361be033', '0a4d8ddb463a8f5d52cbe60cfbfaca2aa22c06fa1c89831944cc5c54df854d04'), 'app/label_guarded_runtime_reconcile.py': ('5f2d668622236950e307e5dc964303f78c7074d3f4bf2c2a37018c2fffe46016', 'dccab991a4caa38b7cefc59aa92357393219131232f3252c50d23dc7240a8a1e'), 'app/registration_relay_recovery.py': ('25921c0e33df76ef1c52655d95f2482906d444409d5c4a4d602e3745ff8d721b', '013758e5ab7f9084eae87651fd088a667e240841384deae3140858d28998f144'), 'app/direct_sync_push.py': ('d9c21b3209dc35e380daeb045a51e84dd81fb99893b471e83711116ef9d06e6d', '7556f245d8724729d70e0639f6c5fcec06cc77e0f3e8b1f871278c585cd45666'), 'app/item_catalog_sync.py': ('41c33bf6a473cc147db6e522b1d20d4771cc9bd92c74fa3a8bd091a141907387', '7ba452c285d2f41fea202dedd68fedbe15d514a8d632bf6818f27ac54851c3d4'), 'app/label_admin_pin.py': ('25ff059f390f88166e74518a8027633ab75d6954ceb8f8ee74593725bf828f75', '9be9247c439a99c696cb472f0bee0eee85b2fe1a98ee2a25aabe6395aff7a617'), 'app/logistics_runtime_profile.py': ('1d786cb74812b1b4588d122b333cc9b8724d28e7410f91c7c0f025d9fd1bd5ee', 'db92068a17237e47d3025bece8d8f479fbe5eca46fde86e90666e3cac49fab72'), 'app/producer_runtime_client.py': ('e47cd333cc3c743fe9cd30a6de4069929773e86bfbb23babdeaff287564e8f17', 'd575094842ab4bb126f16d5372074aad03e20e4c0163f2ab0ffc5b10f77e271c')}
data_generation_correction_transition = all(
    (installed / relative).is_file() and (source / relative).is_file()
    and release_digest(installed / relative) == before
    and release_digest(source / relative) == after
    for relative, (before, after) in data_generation_correction_replacements.items()
)
# Compose G only with an existing exact directional pair. Each changed
# preimage still matches the baseline or that pair's historical declaration.

# Exact f6e4cf2 security-input correction; forward only.
data_generation_security_replacements = {'app/current_user_onboarding.py': ('3b0c37fff24747462f3bbb1a979c522a32f9cb20cd29f94dfbb87e4cb2e08d57', '93a4d7b67b9d740ddbf8bfa9546f01fc0c927ee0de985f4be4e0d83adf435ebd'), 'app/label_data_generation.py': ('ad5cd8415ce3334cbc06b2c8cb67bd0c914470d60344248cb29b9e94b881aa5a', '31a8a2a16f037fe0bc7e1502ee8aeb725aafbbf4e72cd3c0f2af550d822a3b8f'), 'app/protected_admin.py': ('f4b1616dc152c84e368a32acb06b424b4b4cdab588635e383fa22e7ee6d3c58e', 'eadfb46474595075b840bbdf87b919910c228ac5481fe62721a3497c478d63f9'), 'app/package_logistics.py': ('6899fd407c99066ba52ac698af0f01dbe762a56b300a6c28f1c6e56958dd5ef3', '732b2449cee55b15e9c687a542814f76c5dc0a0c49337ec018194caa7960b3c1'), 'app/writer_session_fence.py': ('d041b529535779ad5f35e0ebc790451e0432a34bc537ae7a38bf21dc0b87e8cb', '92419d83453e49cd225e79ccc1aa96230c7ba00636b7fd114ed02736e6c074e1'), 'tools/label_writer_fence.ps1': ('2182570ed8684627b75c743a13bc2bbc05018f29c714ed74f7ecda0925c1b813', '44ad315d0ffe526f003e3d8d17ff0940338652e7e557ad22ed6e728551ce3d3f'), 'app/Label_Match.py': ('18f08d739e4d5b64b402f38278dc29019c2701856eb18e34d8962f2602ca2724', '43c93e151623c6098ab6dc0b200d5d44c415cdbe12f2e3be97c3172c41d270e9'), 'app/direct_sync_runtime.py': ('50ff189da613b1e1bec5bbb32061acc76e8d29ecae8808f5f591854a361be033', '0a4d8ddb463a8f5d52cbe60cfbfaca2aa22c06fa1c89831944cc5c54df854d04'), 'app/label_guarded_runtime_reconcile.py': ('5f2d668622236950e307e5dc964303f78c7074d3f4bf2c2a37018c2fffe46016', 'dccab991a4caa38b7cefc59aa92357393219131232f3252c50d23dc7240a8a1e'), 'app/label_match_product_host.py': ('ce29df5585a2e1b0f1d075e05c04bc267e1ded9180454cb74197d7a6907cf580', '2f68dda99e3bc9e0d6e1c325cb43d169845df3501050a354c624a44690097845'), 'app/registration_relay_recovery.py': ('25921c0e33df76ef1c52655d95f2482906d444409d5c4a4d602e3745ff8d721b', '013758e5ab7f9084eae87651fd088a667e240841384deae3140858d28998f144'), 'app/tools/direct_sync_relay_operator.py': ('8396f45a0590eb6429bf7e152c102dfefb6aa46dfcac43b3b10fdf5b0104bf0c', '3c990ebfd9ff356d5876b40d1098470d0602fae3a3ed838ccf0cbc644150032f'), 'app/tools/direct_sync_relay_runner.py': ('bc9123fdef68cd477cad1225b9cefa88ec6809c48c27c61f2db5b6e5a22109f9', '9b8e7a9845c4e73edea64b4ac3d0b90e3f461b7d926e0e1db64bdb9197f734ac'), 'app/tools/label_auth_recovery_canary.py': ('4ff48178ed23fad2a8026f987d515f76658397252dabef971af6956490ce7b4e', '079dfe927bcf721f4db49779eeab96b66093e1e02c69b86da73ea069d5b7c9be'), 'app/user_relay.py': ('f9a28ff78d8c3d500cffe682676490deb72fba30db4318cddf8f67ae0e2b290b', '1015c0d1026d69745d70bbe1d69ab0f5a97cec52eaf8831ecc95d0356d8f4d3b'), 'app/direct_sync_push.py': ('d9c21b3209dc35e380daeb045a51e84dd81fb99893b471e83711116ef9d06e6d', '7556f245d8724729d70e0639f6c5fcec06cc77e0f3e8b1f871278c585cd45666'), 'app/item_catalog_sync.py': ('41c33bf6a473cc147db6e522b1d20d4771cc9bd92c74fa3a8bd091a141907387', '7ba452c285d2f41fea202dedd68fedbe15d514a8d632bf6818f27ac54851c3d4'), 'app/label_admin_pin.py': ('25ff059f390f88166e74518a8027633ab75d6954ceb8f8ee74593725bf828f75', '9be9247c439a99c696cb472f0bee0eee85b2fe1a98ee2a25aabe6395aff7a617'), 'app/logistics_runtime_profile.py': ('1d786cb74812b1b4588d122b333cc9b8724d28e7410f91c7c0f025d9fd1bd5ee', 'db92068a17237e47d3025bece8d8f479fbe5eca46fde86e90666e3cac49fab72'), 'app/producer_runtime_client.py': ('e47cd333cc3c743fe9cd30a6de4069929773e86bfbb23babdeaff287564e8f17', 'd575094842ab4bb126f16d5372074aad03e20e4c0163f2ab0ffc5b10f77e271c')}
data_generation_security_transition = all(
    (installed / relative).is_file() and (source / relative).is_file()
    and release_digest(installed / relative) == before
    and release_digest(source / relative) == after
    for relative, (before, after) in data_generation_security_replacements.items()
)


# Exact 223cda8 nested credential owner admission; forward only.
data_generation_nested_replacements = {'app/Label_Match.py': ('18f08d739e4d5b64b402f38278dc29019c2701856eb18e34d8962f2602ca2724', '43c93e151623c6098ab6dc0b200d5d44c415cdbe12f2e3be97c3172c41d270e9'), 'app/current_user_onboarding.py': ('a11885f411ea594bdbfd02576107df3428884c40cd77a17bafe8c4840df2ab08', '93a4d7b67b9d740ddbf8bfa9546f01fc0c927ee0de985f4be4e0d83adf435ebd'), 'app/direct_sync_runtime.py': ('50ff189da613b1e1bec5bbb32061acc76e8d29ecae8808f5f591854a361be033', '0a4d8ddb463a8f5d52cbe60cfbfaca2aa22c06fa1c89831944cc5c54df854d04'), 'app/label_data_generation.py': ('d8e6dd64c6cb9fd1ebb686235a5e97ea09139c310c3031284227fdbf11290a70', '31a8a2a16f037fe0bc7e1502ee8aeb725aafbbf4e72cd3c0f2af550d822a3b8f'), 'app/label_guarded_runtime_reconcile.py': ('5f2d668622236950e307e5dc964303f78c7074d3f4bf2c2a37018c2fffe46016', 'dccab991a4caa38b7cefc59aa92357393219131232f3252c50d23dc7240a8a1e'), 'app/label_match_product_host.py': ('ce29df5585a2e1b0f1d075e05c04bc267e1ded9180454cb74197d7a6907cf580', '2f68dda99e3bc9e0d6e1c325cb43d169845df3501050a354c624a44690097845'), 'app/registration_relay_recovery.py': ('25921c0e33df76ef1c52655d95f2482906d444409d5c4a4d602e3745ff8d721b', '013758e5ab7f9084eae87651fd088a667e240841384deae3140858d28998f144'), 'app/tools/direct_sync_relay_operator.py': ('8396f45a0590eb6429bf7e152c102dfefb6aa46dfcac43b3b10fdf5b0104bf0c', '3c990ebfd9ff356d5876b40d1098470d0602fae3a3ed838ccf0cbc644150032f'), 'app/tools/direct_sync_relay_runner.py': ('bc9123fdef68cd477cad1225b9cefa88ec6809c48c27c61f2db5b6e5a22109f9', '9b8e7a9845c4e73edea64b4ac3d0b90e3f461b7d926e0e1db64bdb9197f734ac'), 'app/tools/label_auth_recovery_canary.py': ('4ff48178ed23fad2a8026f987d515f76658397252dabef971af6956490ce7b4e', '079dfe927bcf721f4db49779eeab96b66093e1e02c69b86da73ea069d5b7c9be'), 'tools/label_writer_fence.ps1': ('517670a19a82713b53a30473bf2b8b94ab7083aec024a604ec3b568ff4afa349', '44ad315d0ffe526f003e3d8d17ff0940338652e7e557ad22ed6e728551ce3d3f'), 'app/user_relay.py': ('f9a28ff78d8c3d500cffe682676490deb72fba30db4318cddf8f67ae0e2b290b', '1015c0d1026d69745d70bbe1d69ab0f5a97cec52eaf8831ecc95d0356d8f4d3b'), 'app/writer_session_fence.py': ('1c70276cf1a6990771528bbc74628270a14000f624dc43d069ddff544d77cbc5', '92419d83453e49cd225e79ccc1aa96230c7ba00636b7fd114ed02736e6c074e1'), 'app/direct_sync_push.py': ('d9c21b3209dc35e380daeb045a51e84dd81fb99893b471e83711116ef9d06e6d', '7556f245d8724729d70e0639f6c5fcec06cc77e0f3e8b1f871278c585cd45666'), 'app/item_catalog_sync.py': ('41c33bf6a473cc147db6e522b1d20d4771cc9bd92c74fa3a8bd091a141907387', '7ba452c285d2f41fea202dedd68fedbe15d514a8d632bf6818f27ac54851c3d4'), 'app/label_admin_pin.py': ('25ff059f390f88166e74518a8027633ab75d6954ceb8f8ee74593725bf828f75', '9be9247c439a99c696cb472f0bee0eee85b2fe1a98ee2a25aabe6395aff7a617'), 'app/logistics_runtime_profile.py': ('1d786cb74812b1b4588d122b333cc9b8724d28e7410f91c7c0f025d9fd1bd5ee', 'db92068a17237e47d3025bece8d8f479fbe5eca46fde86e90666e3cac49fab72'), 'app/package_logistics.py': ('43f7a81134e22a576e40b4313198ae5e86485449655398d21bf6cb309d523008', '732b2449cee55b15e9c687a542814f76c5dc0a0c49337ec018194caa7960b3c1'), 'app/producer_runtime_client.py': ('e47cd333cc3c743fe9cd30a6de4069929773e86bfbb23babdeaff287564e8f17', 'd575094842ab4bb126f16d5372074aad03e20e4c0163f2ab0ffc5b10f77e271c')}
data_generation_pin_replacements = {'app/Label_Match.py': ('0b8dc236a221c941b423568353238453e1b7a6229647fc7882afc227bdeee702', '43c93e151623c6098ab6dc0b200d5d44c415cdbe12f2e3be97c3172c41d270e9'), 'app/direct_sync_push.py': ('d9c21b3209dc35e380daeb045a51e84dd81fb99893b471e83711116ef9d06e6d', '7556f245d8724729d70e0639f6c5fcec06cc77e0f3e8b1f871278c585cd45666'), 'app/direct_sync_runtime.py': ('03411c1c265188cd4ba4267b0db0444001799191c6a5ea1bb35690b319e04fc8', '0a4d8ddb463a8f5d52cbe60cfbfaca2aa22c06fa1c89831944cc5c54df854d04'), 'app/item_catalog_sync.py': ('41c33bf6a473cc147db6e522b1d20d4771cc9bd92c74fa3a8bd091a141907387', '7ba452c285d2f41fea202dedd68fedbe15d514a8d632bf6818f27ac54851c3d4'), 'app/label_admin_pin.py': ('25ff059f390f88166e74518a8027633ab75d6954ceb8f8ee74593725bf828f75', '9be9247c439a99c696cb472f0bee0eee85b2fe1a98ee2a25aabe6395aff7a617'), 'app/logistics_runtime_profile.py': ('1d786cb74812b1b4588d122b333cc9b8724d28e7410f91c7c0f025d9fd1bd5ee', 'db92068a17237e47d3025bece8d8f479fbe5eca46fde86e90666e3cac49fab72'), 'app/package_logistics.py': ('43f7a81134e22a576e40b4313198ae5e86485449655398d21bf6cb309d523008', '732b2449cee55b15e9c687a542814f76c5dc0a0c49337ec018194caa7960b3c1'), 'app/producer_runtime_client.py': ('e47cd333cc3c743fe9cd30a6de4069929773e86bfbb23babdeaff287564e8f17', 'd575094842ab4bb126f16d5372074aad03e20e4c0163f2ab0ffc5b10f77e271c'), 'tools/label_writer_fence.ps1': ('eff51cca49b3bc5e9104bcb2c3778c59dceecea003beb45390d5dc1eeeda6420', '44ad315d0ffe526f003e3d8d17ff0940338652e7e557ad22ed6e728551ce3d3f'), 'app/writer_session_fence.py': ('9376e9e2a998aca6f97fa99368276c3c87c18fa3b45625e1c1a226df68ebdb7b', '92419d83453e49cd225e79ccc1aa96230c7ba00636b7fd114ed02736e6c074e1')}
data_generation_pin_transition = all(
    (installed / relative).is_file() and (source / relative).is_file()
    and release_digest(installed / relative) == before
    and release_digest(source / relative) == after
    for relative, (before, after) in data_generation_pin_replacements.items()
)
data_generation_nested_transition = all(
    (installed / relative).is_file() and (source / relative).is_file()
    and release_digest(installed / relative) == before
    and release_digest(source / relative) == after
    for relative, (before, after) in data_generation_nested_replacements.items()
)

def data_generation_before_allowed(relative):
    if not (installed / relative).is_file():
        return False
    actual = release_digest(installed / relative)
    if actual == data_generation_replacements[relative][0]:
        return True
    if relative == 'app/Label_Match.py':
        scalar_pairs = [*(scan_input_pairs if scan_input_transition else []),
                        *([nextfix_label_pair] if nextfix_label_transition else []),
                        *([scanner_label_pair] if scanner_transition else [])]
        if any(actual == before for before, _ in scalar_pairs):
            return True
    for enabled, declarations in (
        (shared_transition, shared_replacements),
        (legacy_label_transition, legacy_label_replacements),
        (scan_input_transition, scan_input_companions),
        (nextfix_label_transition or scanner_transition, nextfix_companions),
        (current_user_profile_transition, current_user_profile_replacements),
        (remote_transport_transition, remote_transport_replacements),
        (roster_transition, {**roster_baseline_replacements, **roster_baseline_companions}),
        (worker_record_transition, worker_record_replacements),
        (mainloop_exit_transition, mainloop_exit_replacements),
        (mainloop_exit_notice_transition, mainloop_exit_notice_replacements),
    ):
        if enabled and relative in declarations:
            before = declarations[relative][0]
            if actual in (before if isinstance(before, set) else {before}):
                return True
    return False

for directory in ('app', 'runtime'):
    left, right = files(source, directory), files(installed, directory)
    additions = set(data_generation_additions) if data_generation_transition and directory == 'app' else set()
    additions |= set(shared_additions) if shared_transition and directory == 'app' else set()
    if legacy_label_transition and directory == 'app':
        additions |= set(legacy_label_additions)
    if roster_transition and directory == 'app':
        additions |= set(roster_additions)
    expected_left = right.keys() - set(data_generation_additions) if data_generation_reverse and directory == 'app' else right.keys() | additions
    if left.keys() != expected_left:
        raise ValueError('WRITER_TRANSITION_SOURCE_SET_DIFFERS')
    for relative in sorted(left, key=lambda path: (path != 'app/Label_Match.py', path)):
        path = left[relative]
        if source_admission_transition and relative in source_admission_companions:
            continue
        if data_generation_pin_transition and relative in data_generation_pin_replacements:
            continue
        if data_generation_nested_transition and relative in data_generation_nested_replacements:
            continue
        if data_generation_security_transition and relative in data_generation_security_replacements:
            continue
        if data_generation_correction_transition and relative in data_generation_correction_replacements:
            continue
        if data_generation_reverse and relative in data_generation_replacements:
            continue
        if data_generation_transition and relative in data_generation_additions:
            continue
        if data_generation_transition and relative in data_generation_replacements:
            if not data_generation_before_allowed(relative):
                raise ValueError('WRITER_TRANSITION_SEMANTICS_DIFFER: ' + relative)
            continue
        if data_generation_companion_transition and relative in data_generation_forward_companions:
            continue
        if shared_transition and relative in (shared_replacements.keys() | shared_additions.keys()):
            continue
        if integrity_order_transition and relative == integrity_order_path:
            continue
        if roster_transition and relative in roster_additions:
            continue
        if roster_baseline_transition and relative in roster_baseline_replacements:
            continue
        if roster_baseline_transition and relative in roster_baseline_companions:
            continue
        if quiesced_transport_transition and relative in quiesced_transport_replacements:
            continue
        if remote_transport_transition and relative in remote_transport_replacements:
            continue
        if mainloop_exit_notice_transition and relative in mainloop_exit_notice_replacements:
            continue
        if mainloop_exit_transition and relative in mainloop_exit_replacements:
            continue
        if worker_record_transition and relative in worker_record_replacements:
            continue
        if f5_retirement_transition and relative in f5_retirement_replacements:
            continue
        if current_user_profile_transition and relative in current_user_profile_replacements:
            continue
        if scan_input_transition and relative in ({scan_input_path} | scan_input_companions.keys()):
            continue
        if (nextfix_label_transition or scanner_transition) and relative in ({nextfix_label_path} | nextfix_companions.keys()):
            continue
        if legacy_label_transition and relative in (legacy_label_replacements.keys() | legacy_label_additions.keys()):
            continue
        other = right[relative]
        if digest(path) == digest(other):
            continue
        if directory == 'app' and path.suffix == '.py' and not relative.startswith('app/site-packages/'):
            if syntax(path, relative) == syntax(other, relative):
                continue
        raise ValueError('WRITER_TRANSITION_SEMANTICS_DIFFER: ' + relative)
for relative in ('INSTALL_THIS_PC.ps1', 'launch-label-match.cmd', 'tools/bootstrap_integrity.ps1', 'tools/label_writer_fence_contract.json'):
    if data_generation_reverse and relative in data_generation_replacements:
        continue
    if data_generation_transition and relative in data_generation_replacements:
        if not data_generation_before_allowed(relative):
            raise ValueError('WRITER_TRANSITION_CONTRACT_DIFFERS: ' + relative)
        continue
    if shared_transition and relative in shared_replacements:
        continue
    if (source / relative).read_bytes() != (installed / relative).read_bytes():
        raise ValueError('WRITER_TRANSITION_CONTRACT_DIFFERS: ' + relative)
if candidate_sources != installed_sources:
    # Only the complete committed shutdown repair may add this one sink.
    # Older reviewed release pairs remain checked above; their declarations
    # already pin the same candidate bytes. No other membership is waived.
    if not shutdown_notice_forward or not all(
        (source / relative).is_file() and release_digest(source / relative) == after
        for relative, (_before, after) in mainloop_exit_replacements.items()
    ):
        raise ValueError('WRITER_TRANSITION_SOURCE_SET_DIFFERS')
compatibility = ('SIGNED_COMPATIBLE_DATA_GENERATION_REVERSE' if data_generation_reverse else
                 'PINNED_DATA_GENERATION_G_CORRECTION' if (data_generation_correction_transition or data_generation_security_transition or data_generation_nested_transition or data_generation_pin_transition) else
                 'PINNED_MAINLOOP_EXIT_NOTICE' if mainloop_exit_notice_transition else
                 'PINNED_MAINLOOP_EXIT_CLEANUP' if mainloop_exit_transition else
                 'PINNED_WORKER_RECORD_IDENTITY' if worker_record_transition else
                 'X13B_PINNED_SHARED_LEAF_ADOPTION' if shared_transition else
                 'PINNED_BOOTSTRAP_INTEGRITY_ORDER_FIX' if integrity_order_transition else
                 'PINNED_LEGACY_LABEL_TRANSITION_MODE' if legacy_label_transition else
                 'PINNED_SCAN_INPUT_FIX' if scan_input_transition else
                 'PINNED_W9NEXTFIX_LABEL_RECOVERY' if nextfix_label_transition else
                 'PINNED_W9SCANONLY_WORKFLOW' if scanner_transition else
                 'PINNED_CURRENT_USER_PROFILE_SELECTION' if current_user_profile_transition else
                 'PINNED_DATA_GENERATION_G' if (data_generation_transition or data_generation_companion_transition) else 'UNCHANGED_PRODUCTION_AST_AND_CONTRACTS')
print(json.dumps(dict(installed_inventory_sha256=installed_pin, candidate_inventory_sha256=candidate_pin, compatibility=compatibility)))
'@
    $generationReverse = 'no-data-compatibility-override'
    $generationVariable = Get-Variable -Name generationDowngradeDecision -ErrorAction SilentlyContinue
    if ($null -ne $generationVariable) {
        $generationDecision = $generationVariable.Value
        if ($generationDecision.allowed -eq $true -and $null -ne $generationDecision.version -and
            $generationDecision.minimum_version -cne $generationDecision.version) {
            $generationReverse = 'signed-compatible-downgrade'
        }
    }
    # LF-normalized UTF-8 is the fixed probe identity on PS5.1 and PS7.
    $probeBytes = [Text.Encoding]::UTF8.GetBytes($probe.Replace("`r`n", "`n"))
    $stdout = Invoke-LabelPinnedProbe -Python (Join-Path $Source 'runtime\python.exe') `
        -ProbeBytes $probeBytes -ExpectedSha256 '6501412d068dab61579ad070ba4c15fa0fcc90ee9e5e48a1b5abf8efe38ba0c0' `
        -ProbeArguments @($Source, $Installed, $generationReverse) -FailureMessage 'Installed writer transition is unsupported: '
    return $stdout | ConvertFrom-Json
}

function ReplacementPreimage([string]$InstallRootValue, $ExpectedInventory, [string[]]$Before) {
    $newBackups = @(Get-ChildItem -LiteralPath (Split-Path -Parent $InstallRootValue) -Directory -Force |
        Where-Object { $_.Name -like '.current.rollback.*' -and $_.FullName -cnotin $Before })
    if ($newBackups.Count -eq 0) { return '' }
    if ($newBackups.Count -ne 1) { throw 'Replacement code preimage is ambiguous.' }
    $root = [string]$newBackups[0].FullName
    [void](Manifest $root $SkipSignatureValidationForTest)
    $inventory = PortableInventory $root
    if ([string]$inventory.sha256 -cne [string]$ExpectedInventory.sha256) {
        throw 'Replacement code preimage differs from the validated installed identity.'
    }
    return $root
}

function Open-LabelRunKey([string]$SubKey) {
    # OpenSubKey can hide native errors as null. Only Win32 code 2 is absence.
    if (-not ('KmtechLabelRunRegistryNative' -as [type])) {
        Add-Type -TypeDefinition @'
using System;
using System.Runtime.InteropServices;
public static class KmtechLabelRunRegistryNative {
    [DllImport("advapi32.dll", CharSet=CharSet.Unicode, ExactSpelling=true)]
    public static extern int RegOpenKeyExW(IntPtr root, string subKey, uint options, uint access, out IntPtr key);
}
'@
    }
    $handle = [IntPtr]::Zero
    $status = [KmtechLabelRunRegistryNative]::RegOpenKeyExW([IntPtr]::new(-2147483647), $SubKey, 0, 1, [ref]$handle)
    if ($status -eq 2) { return $null }
    if ($status -ne 0) { throw [ComponentModel.Win32Exception]::new($status) }
    if ($handle -eq [IntPtr]::Zero) { throw 'RUN_REGISTRY_HANDLE_INVALID' }
    $ownedHandle = [Microsoft.Win32.SafeHandles.SafeRegistryHandle]::new($handle, $true)
    try { return [Microsoft.Win32.RegistryKey]::FromHandle($ownedHandle) }
    catch { $ownedHandle.Dispose(); throw }
}

function Snapshot {
    $key = Open-LabelRunKey $RunKey
    if ($null -eq $key) { return [ordered]@{ exists = $false; kind = ''; data = '' } }
    try {
        if ($RunName -notin $key.GetValueNames()) { return [ordered]@{ exists = $false; kind = ''; data = '' } }
        $kind = [string]$key.GetValueKind($RunName)
        if ($kind -notin @('String', 'ExpandString')) { throw "Unsupported Run type: $kind" }
        $data = $key.GetValue(
            $RunName,
            $null,
            [Microsoft.Win32.RegistryValueOptions]::DoNotExpandEnvironmentNames
        )
        if ($null -eq $data) { throw 'Run value disappeared during snapshot.' }
        return [ordered]@{ exists = $true; kind = $kind; data = [string]$data }
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

function TextSha([string]$Value) {
    return ByteSha ((New-Object Text.UTF8Encoding($false)).GetBytes($Value))
}

function Import-LabelOfficialLegacyTaskFunctions([string]$Path) {
    $tokens=$null;$errors=$null
    $tree=[Management.Automation.Language.Parser]::ParseFile($Path,[ref]$tokens,[ref]$errors)
    if ($errors.Count) { throw 'LEGACY_RELAY_HELPER_PARSE_FAILED' }
    foreach ($name in @('Get-LabelOfficialLegacyTaskExpectation','Get-LabelOfficialLegacyPrincipalModes','Get-LabelOfficialLegacyTaskExpectations','ConvertTo-LabelLegacyTaskPath','Get-LabelOfficialLegacyTaskFingerprint','Get-LabelOfficialLegacyTaskSnapshot','Assert-LabelOfficialLegacyTaskLauncher','Assert-LabelOfficialLegacyTaskStopped','Invoke-LabelOfficialLegacyTaskTransition')) {
        $definitions=@($tree.FindAll({param($n)$n -is [Management.Automation.Language.FunctionDefinitionAst] -and $n.Name -ceq $name},$true))
        if ($definitions.Count -ne 1) { throw 'LEGACY_RELAY_HELPER_DEFINITION_FAILED' }
        . ([scriptblock]::Create($definitions[0].Extent.Text))
    }
}

function Get-LabelCurrentUserScheduledTasks {
    $official=Get-LabelOfficialLegacyTaskSnapshot
    return @(Get-ScheduledTask -ErrorAction Stop | Where-Object {
        [string]$_.TaskName -ieq $CanonicalTaskName -and [string]$_.TaskPath -ceq '\' -and
        ($null -eq $official -or [string]$official.name -ine [string]$_.TaskName)
    })
}

function ScheduledTaskSnapshot([string]$AuditRoot, [string]$RunId) {
    $task = Get-LabelCurrentUserScheduledTasks
    if ($null -eq $task) {
        return [pscustomobject][ordered]@{
            exists = $false
            enabled = $false
            xml_sha256 = ''
            backup_path = ''
        }
    }
    $xml = [string](Export-ScheduledTask -TaskName $CanonicalTaskName -TaskPath '\')
    if ([string]::IsNullOrWhiteSpace($xml)) { throw 'Scheduled task preimage is empty.' }
    $backup = Join-Path $AuditRoot "canonical-portable-$RunId-task-preimage.xml"
    [IO.File]::WriteAllText($backup, $xml, (New-Object Text.UTF8Encoding($false)))
    if ((TextSha ([IO.File]::ReadAllText($backup, (New-Object Text.UTF8Encoding($false, $true))))) -cne (TextSha $xml)) {
        throw 'Scheduled task preimage backup readback failed.'
    }
    return [pscustomobject][ordered]@{
        exists = $true
        enabled = [bool]$task.Settings.Enabled
        xml_sha256 = TextSha $xml
        backup_path = $backup
    }
}

function RestoreScheduledTask($Before) {
    $existing = Get-LabelCurrentUserScheduledTasks
    if (-not [bool]$Before.exists) {
        if ($null -ne $existing) {
            Unregister-ScheduledTask `
                -TaskName $CanonicalTaskName `
                -TaskPath '\' `
                -Confirm:$false
        }
        if ($null -ne (Get-LabelCurrentUserScheduledTasks)) {
            throw 'Scheduled task absence restore readback failed.'
        }
        return
    }
    $xml = [IO.File]::ReadAllText(
        [string]$Before.backup_path,
        (New-Object Text.UTF8Encoding($false, $true))
    )
    if ((TextSha $xml) -cne [string]$Before.xml_sha256) {
        throw 'Scheduled task preimage changed before restore.'
    }
    Register-ScheduledTask `
        -TaskName $CanonicalTaskName `
        -TaskPath '\' `
        -Xml $xml `
        -Force | Out-Null
    if ([bool]$Before.enabled) {
        Enable-ScheduledTask -TaskName $CanonicalTaskName -TaskPath '\' | Out-Null
    }
    else {
        Disable-ScheduledTask -TaskName $CanonicalTaskName -TaskPath '\' | Out-Null
    }
    Assert-LabelRollbackTaskPreimage $Before
}

function Assert-LabelRollbackTaskPreimage($Before) {
    $matches = @(Get-LabelCurrentUserScheduledTasks)
    if (-not [bool]$Before.exists) {
        if ($matches.Count -ne 0) { throw 'Scheduled task absence readback failed.' }
        return
    }
    if ($matches.Count -ne 1) { throw 'Scheduled task preimage readback is ambiguous.' }
    $restored = $matches[0]
    $restoredXml = [string](Export-ScheduledTask -TaskName $CanonicalTaskName -TaskPath '\')
    if (
        [bool]$restored.Settings.Enabled -ne [bool]$Before.enabled -or
        (TextSha $restoredXml) -cne [string]$Before.xml_sha256
    ) { throw 'Scheduled task exact restore readback failed.' }
}

function UnquiescedProductWriters {
    return @(Get-CimInstance Win32_Process -ErrorAction Stop | Where-Object {
        $command = [string]$_.CommandLine
        $executable = [string]$_.ExecutablePath
        ($executable -match '(?i)(pythonw?\.exe|Label_Match\.exe)$') -and
        (
            $command -like '*--label-match-user-relay*' -or
            $command -like '*--label-match-scheduled-relay*' -or
            $command -like '*--label-match-direct-sync-relay*' -or
            $command -like '*tools*direct_sync_relay_runner.py*' -or
            $command -like '*Label_Match*app*main.py*'
        )
    })
}

function Save([string]$Path, $Value) {
    New-Item -ItemType Directory -Path (Split-Path -Parent $Path) -Force | Out-Null
    $temp = "$Path.tmp.$PID"
    [IO.File]::WriteAllText(
        $temp,
        ((ConvertTo-KmtechRedactedSinkObject (ConvertTo-KmtechPublicDiagnosticTree $Value)) | ConvertTo-Json -Depth 30) + [Environment]::NewLine,
        (New-Object Text.UTF8Encoding($false))
    )
    Move-Item $temp $Path -Force
}

function Save-LabelMachineMetadata([string]$Path, $Value) {
    $json = ($Value | ConvertTo-Json -Depth 30) + [Environment]::NewLine
    Assert-KmtechMachineMetadataText -Text $json
    New-Item -ItemType Directory -Path (Split-Path -Parent $Path) -Force | Out-Null
    $temp = "$Path.tmp.$PID"
    [IO.File]::WriteAllText($temp, $json, (New-Object Text.UTF8Encoding($false)))
    Move-Item -LiteralPath $temp -Destination $Path -Force
}


function Archive-PretransactionFailure([string]$AuditRoot, [string]$RunId) {
    # Before writer authority/fence acquisition, another installer may publish
    # here. Retain only this run's literal snapshot entries outside the active
    # root; never classify an existing receipt or registration as pristine.
    $Script:LabelPretransactionActiveRootRetained = $false
    if ($RunId -cnotmatch '^[a-zA-Z0-9-]+$') { throw 'Pretransaction run identity is invalid.' }
    $auditRootPath = [IO.Path]::GetFullPath($AuditRoot).TrimEnd('\')
    if ((Split-Path -Leaf $auditRootPath) -cne 'install-audit') {
        throw 'Pretransaction audit root identity differs.'
    }
    if ((Get-Item -LiteralPath $auditRootPath).Attributes -band [IO.FileAttributes]::ReparsePoint) {
        throw 'Pretransaction audit root is a reparse point.'
    }
    $archiveParent = $auditRootPath + '-pretransaction-failures'
    if (Test-Path -LiteralPath $archiveParent) {
        $archiveItem = Get-Item -LiteralPath $archiveParent
        if (-not $archiveItem.PSIsContainer -or
            ($archiveItem.Attributes -band [IO.FileAttributes]::ReparsePoint)) {
            throw 'Pretransaction archive parent is not an ordinary directory.'
        }
    }
    $archiveRoot = Join-Path $archiveParent $RunId
    if (Test-Path -LiteralPath $archiveRoot) { throw 'Pretransaction archive already exists.' }
    [void][IO.Directory]::CreateDirectory($archiveParent)
    $prefix = 'canonical-portable-' + $RunId
    $allEntries = @(Get-ChildItem -LiteralPath $auditRootPath -Force)
    $entries = @($allEntries | Where-Object {
        $_.Name -ceq ($prefix + '.json') -or
            $_.Name.StartsWith($prefix + '.json.tmp.', [StringComparison]::Ordinal) -or
            $_.Name -cin @(($prefix + '-helper'), ($prefix + '-task-preimage.xml'),
                ($prefix + '-stop-preimage.json'), ($prefix + '-removal-preimage.json'),
                ($prefix + '-first-registration-attempt.json'))
    })
    foreach ($entry in $entries) {
        if ($entry.Attributes -band [IO.FileAttributes]::ReparsePoint) {
            throw 'Pretransaction artifact is a reparse point.'
        }
    }
    if ($entries.Count -eq 0) { throw 'Pretransaction artifacts are absent.' }
    [void][IO.Directory]::CreateDirectory($archiveRoot)
    foreach ($entry in $entries) {
        # The captured literal list belongs to this run. Another installer can
        # publish after enumeration; its active root and entries stay in place.
        $entryPath = [IO.Path]::GetFullPath($entry.FullName)
        if ([IO.Path]::GetDirectoryName($entryPath) -cne $auditRootPath -or
            [IO.Path]::GetFileName($entryPath) -cne $entry.Name) {
            throw 'Pretransaction artifact literal boundary differs.'
        }
        $currentEntry = Get-Item -LiteralPath $entryPath -ErrorAction Stop
        if ($currentEntry.Attributes -band [IO.FileAttributes]::ReparsePoint) {
            throw 'Pretransaction artifact became a reparse point.'
        }
        Move-Item -LiteralPath $entryPath -Destination (Join-Path $archiveRoot $entry.Name) -ErrorAction Stop
    }
    # Native nonrecursive deletion is atomic with respect to nonempty state
    # and open handles. A concurrent record or live handle keeps this root.
    if (Test-Path -LiteralPath $auditRootPath) {
        if ((Get-Item -LiteralPath $auditRootPath).Attributes -band [IO.FileAttributes]::ReparsePoint) {
            throw 'Pretransaction active audit root became a reparse point.'
        }
        try { [IO.Directory]::Delete($auditRootPath, $false) }
        catch [IO.IOException] {
            $Script:LabelPretransactionActiveRootRetained = $true
            [Console]::Error.WriteLine('Pretransaction active audit root retained: nonempty or open handle.')
        }
        catch [UnauthorizedAccessException] {
            $Script:LabelPretransactionActiveRootRetained = $true
            [Console]::Error.WriteLine('Pretransaction active audit root retained: native access denied.')
        }
    }
    return $archiveRoot
}

function FreezePlacementHelper(
    [string]$Source,
    [string]$AuditRoot,
    [string]$RunId,
    $ExpectedCriticalFileSha256,
    [string]$GuardedHelperOrigin = ''
) {
    if ([string]::IsNullOrEmpty($GuardedHelperOrigin)) { $GuardedHelperOrigin = $Source }
    $frozenRoot = Join-Path $AuditRoot "canonical-portable-$RunId-helper"
    $frozenTools = Join-Path $frozenRoot 'tools'
    New-Item -ItemType Directory -Path $frozenTools -Force | Out-Null
    $sourceHelper = Join-Path $GuardedHelperOrigin 'INSTALL_THIS_PC.ps1'
    $sourceIntegrity = Join-Path $GuardedHelperOrigin 'tools\bootstrap_integrity.ps1'
    $sourceWriterFence = Join-Path $Source 'tools\label_writer_fence.ps1'
    $frozenHelper = Join-Path $frozenRoot 'INSTALL_THIS_PC.ps1'
    $frozenIntegrity = Join-Path $frozenTools 'bootstrap_integrity.ps1'
    $frozenWriterFence = Join-Path $frozenTools 'label_writer_fence.ps1'
    $helperSha256 = [string]$ExpectedCriticalFileSha256.placement_helper
    $integritySha256 = [string]$ExpectedCriticalFileSha256.bootstrap_integrity_helper
    $writerFenceSha256 = [string]$ExpectedCriticalFileSha256.writer_fence_helper
    if (
        $helperSha256 -cnotmatch '^[0-9a-f]{64}$' -or
        $integritySha256 -cnotmatch '^[0-9a-f]{64}$' -or
        $writerFenceSha256 -cnotmatch '^[0-9a-f]{64}$'
    ) { throw 'Pinned critical helper hashes are invalid.' }
    [byte[]]$helperBytes = PinnedFileBytes $sourceHelper $helperSha256
    [byte[]]$integrityBytes = PinnedFileBytes $sourceIntegrity $integritySha256
    [byte[]]$writerFenceBytes = PinnedFileBytes $sourceWriterFence $writerFenceSha256
    [IO.File]::WriteAllBytes($frozenHelper, $helperBytes)
    [IO.File]::WriteAllBytes($frozenIntegrity, $integrityBytes)
    [IO.File]::WriteAllBytes($frozenWriterFence, $writerFenceBytes)
    if (
        (Sha $frozenHelper) -cne $helperSha256 -or
        (Sha $frozenIntegrity) -cne $integritySha256 -or
        (Sha $frozenWriterFence) -cne $writerFenceSha256
    ) { throw 'Frozen placement helper readback differs.' }

    $sourceSharedLeaf = Get-LabelSharedPortableLeafPath $GuardedHelperOrigin
    $sourceSharedApp = Split-Path (Split-Path (Split-Path $sourceSharedLeaf -Parent) -Parent) -Parent
    $frozenSharedPaths = @()
    foreach ($relative in @('kmtech_shared.lock.json', 'kmtech_shared.manifest.json', 'kmtech_shared\powershell\portable.ps1', 'kmtech_shared\powershell\data_generation.ps1')) {
        $target = Join-Path $frozenRoot $relative
        [void](New-Item -ItemType Directory -Path (Split-Path $target -Parent) -Force)
        [IO.File]::WriteAllBytes($target, [IO.File]::ReadAllBytes((Join-Path $sourceSharedApp $relative)))
        $frozenSharedPaths += $target
    }
    [void](Get-LabelSharedPortableLeafPath $frozenRoot)

    $userSid = [Security.Principal.WindowsIdentity]::GetCurrent().User
    $systemSid = New-Object Security.Principal.SecurityIdentifier('S-1-5-18')
    $adminSid = New-Object Security.Principal.SecurityIdentifier('S-1-5-32-544')
    $inheritance = [Security.AccessControl.InheritanceFlags]::ContainerInherit -bor
        [Security.AccessControl.InheritanceFlags]::ObjectInherit
    $propagation = [Security.AccessControl.PropagationFlags]::None
    $allow = [Security.AccessControl.AccessControlType]::Allow
    $deny = [Security.AccessControl.AccessControlType]::Deny
    $denyWrite = [Security.AccessControl.FileSystemRights]::WriteData -bor
        [Security.AccessControl.FileSystemRights]::AppendData -bor
        [Security.AccessControl.FileSystemRights]::WriteAttributes -bor
        [Security.AccessControl.FileSystemRights]::WriteExtendedAttributes -bor
        [Security.AccessControl.FileSystemRights]::Delete
    $acl = New-Object Security.AccessControl.DirectorySecurity
    $acl.SetOwner($userSid)
    $acl.SetAccessRuleProtection($true, $false)
    [void]$acl.AddAccessRule((New-Object Security.AccessControl.FileSystemAccessRule(
        $userSid,
        $denyWrite,
        $inheritance,
        $propagation,
        $deny
    )))
    [void]$acl.AddAccessRule((New-Object Security.AccessControl.FileSystemAccessRule(
        $userSid,
        ([Security.AccessControl.FileSystemRights]::ReadAndExecute -bor
            [Security.AccessControl.FileSystemRights]::Synchronize),
        $inheritance,
        $propagation,
        $allow
    )))
    foreach ($sid in @($systemSid, $adminSid)) {
        [void]$acl.AddAccessRule((New-Object Security.AccessControl.FileSystemAccessRule(
            $sid,
            [Security.AccessControl.FileSystemRights]::FullControl,
            $inheritance,
            $propagation,
            $allow
        )))
    }
    Set-Acl -LiteralPath $frozenRoot -AclObject $acl
    foreach ($frozenPath in (@($frozenHelper, $frozenIntegrity, $frozenWriterFence) + $frozenSharedPaths)) {
        $fileAcl = [IO.File]::GetAccessControl($frozenPath)
        [void]$fileAcl.AddAccessRule((New-Object Security.AccessControl.FileSystemAccessRule(
            $userSid,
            $denyWrite,
            $deny
        )))
        [IO.File]::SetAccessControl($frozenPath, $fileAcl)
        $writeProbe = $null
        try {
            $writeProbe = [IO.File]::Open(
                $frozenPath,
                [IO.FileMode]::Open,
                [IO.FileAccess]::Write,
                [IO.FileShare]::Read
            )
        }
        catch [UnauthorizedAccessException] {}
        finally {
            if ($null -ne $writeProbe) { $writeProbe.Dispose() }
        }
        if ($null -ne $writeProbe) { throw 'Frozen placement helper remains writable.' }
    }
    if (
        (Sha $frozenHelper) -cne $helperSha256 -or
        (Sha $frozenIntegrity) -cne $integritySha256 -or
        (Sha $frozenWriterFence) -cne $writerFenceSha256
    ) { throw 'Frozen placement helper changed while its ACL was applied.' }
    [void](Get-LabelSharedPortableLeafPath $frozenRoot)
    return [pscustomobject][ordered]@{
        root = $frozenRoot
        helper_path = $frozenHelper
        helper_sha256 = $helperSha256
        integrity_sha256 = $integritySha256
        writer_fence_path = $frozenWriterFence
        writer_fence_sha256 = $writerFenceSha256
        data_generation_path = Get-LabelSharedPortableLeafPath $frozenRoot 'data_generation.ps1'
        data_generation_sha256 = Sha (Get-LabelSharedPortableLeafPath $frozenRoot 'data_generation.ps1')
        current_user_writable = $false
    }
}

function InvokeFrozenIntegrityProbe($Frozen, [string]$Root) {
    $integrityPath = Join-Path ([string]$Frozen.root) 'tools\bootstrap_integrity.ps1'
    [void](PinnedFileBytes $integrityPath ([string]$Frozen.integrity_sha256))
    $rootBase64 = [Convert]::ToBase64String(
        (New-Object Text.UTF8Encoding($false)).GetBytes($Root)
    )
    # Keep the command below Windows' command-line limit as the pinned helper
    # grows. The child rechecks its frozen bytes before evaluating any helper.
    $probeScript = @'
$ErrorActionPreference = 'Stop'
[byte[]]$bytes = [IO.File]::ReadAllBytes('@@integrity-path@@')
$hash = [Security.Cryptography.SHA256]::Create()
try { $actual = ([BitConverter]::ToString($hash.ComputeHash($bytes))).Replace('-', '').ToLowerInvariant() }
finally { $hash.Dispose() }
if ($actual -cne '@@integrity-sha@@') { throw 'Frozen integrity helper byte pin differs.' }
$utf8 = New-Object Text.UTF8Encoding($false, $true)
. ([ScriptBlock]::Create(($utf8.GetString($bytes) -replace '^\uFEFF', ''))) -SharedCodeRoot '@@shared-root@@'
$probeRoot = $utf8.GetString([Convert]::FromBase64String('@@root-base64@@'))
[void](Assert-BootstrapIntegrityRecord $probeRoot)
'@.Replace('@@integrity-path@@', $integrityPath.Replace("'", "''")).Replace(
        '@@integrity-sha@@', [string]$Frozen.integrity_sha256).Replace(
        '@@shared-root@@', ([string]$Frozen.root).Replace("'", "''")).Replace(
        '@@root-base64@@', $rootBase64)
    $encodedProbe = [Convert]::ToBase64String(
        [Text.Encoding]::Unicode.GetBytes($probeScript)
    )
    $winps = Join-Path ([Environment]::SystemDirectory) 'WindowsPowerShell\v1.0\powershell.exe'
    & $winps `
        -NoLogo `
        -NoProfile `
        -NonInteractive `
        -ExecutionPolicy Bypass `
        -EncodedCommand $encodedProbe |
        Out-Null
    if ($LASTEXITCODE -ne 0) { throw 'Installed bootstrap integrity differs.' }
}

function Invoke-LabelDelegatedPlacementHelper($Frozen, [hashtable]$HelperParameters, $AuthorityLease, [switch]$RefreshDelegation) {
    $control = [string]$HelperParameters.WriterFenceControlRoot
    $admission = Enter-LabelWriterAdmission -ControlRoot $control -TimeoutMilliseconds 90000
    try {
        $active = Read-LabelWriterFence $control
        # As in Container_Audit, verify this thread's actual session lease;
        # HeldByOther is for the separate child. Balance only the recursion.
        if ($null -eq $AuthorityLease -or $AuthorityLease.acquired -isnot [bool] -or
            -not [bool]$AuthorityLease.acquired -or $AuthorityLease.mutex -isnot [Threading.Mutex] -or
            [string]$AuthorityLease.name -cne [string]$active.session_authority_mutex_name) {
            throw 'LABEL_PLACEMENT_CALLER_AUTHORITY_INVALID'
        }
        try { $owned = $AuthorityLease.mutex.WaitOne(0) }
        catch [Threading.AbandonedMutexException] { throw 'LABEL_PLACEMENT_CALLER_AUTHORITY_ABANDONED' }
        if (-not $owned) { throw 'LABEL_PLACEMENT_CALLER_AUTHORITY_NOT_OWNED' }
        $AuthorityLease.mutex.ReleaseMutex()
        $restorePreimage = $RefreshDelegation.IsPresent -and
            (($HelperParameters.ContainsKey('StartupUpdateRestorePreimage') -and
              -not [string]::IsNullOrEmpty([string]$HelperParameters.StartupUpdateRestorePreimage) -and
              [string]$HelperParameters.StartupUpdateRestoreSha256 -cmatch '^[0-9a-f]{64}$') -or
             ($HelperParameters.ContainsKey('LegacyRelayTransition') -and
              [string]$HelperParameters.LegacyRelayTransition -ceq 'Restore' -and
              [string]$HelperParameters.LegacyRelayPreimageSha256 -cmatch '^[0-9a-f]{64}$')) -and
            $HelperParameters.ReplaceExistingVerifiedPortable -is [bool] -and
            -not [bool]$HelperParameters.ReplaceExistingVerifiedPortable
        $delegatedSource = if ($restorePreimage) { 'auto_update_restore_preimage' } else { 'canonical_placement' }
        if ([string]$active.status -cne 'RESTORING' -or
            [string]$active.session_id -cne [string]$HelperParameters.WriterFenceSessionId -or
            [string]$active.attempt_id -cne [string]$HelperParameters.WriterFenceAttemptId -or
            [string]$active.replacement_transaction_id -cne [string]$HelperParameters.WriterFenceReplacementTransactionId -or
            [string]$active.delegation_sha256 -cne (Get-LabelWriterFenceStringSha256 ([string]$HelperParameters.WriterFenceDelegationToken)) -or
            $delegatedSource -cnotin @($active.delegated_sources) -or
            (-not $restorePreimage -and
                ((ConvertTo-LabelWriterFenceUtc ([string]$active.delegation_expires_at_utc)) -lt [DateTime]::UtcNow -or
                 -not $HelperParameters.ContainsKey('Uninstall') -or
                 $HelperParameters.Uninstall -isnot [bool] -or -not [bool]$HelperParameters.Uninstall))) {
            throw 'LABEL_PLACEMENT_CHILD_DELEGATION_INVALID'
        }
        if ($restorePreimage) {
            # Earlier code removal/inventory work may outlive a child grant.
            # Only its still-held, exactly bound parent authority can renew it.
            # Keep the same token and source set; ordinary writers remain fenced.
            [void](Set-LabelWriterFenceDelegation -ControlRoot $control -Status 'RESTORING' `
                -SessionId ([string]$HelperParameters.WriterFenceSessionId) `
                -AttemptId ([string]$HelperParameters.WriterFenceAttemptId) `
                -ReplacementTransactionId ([string]$HelperParameters.WriterFenceReplacementTransactionId) `
                -DelegationToken ([string]$HelperParameters.WriterFenceDelegationToken) `
                -DelegatedSources @($active.delegated_sources) -LifetimeSeconds 600)
        }
    }
    finally { Exit-LabelWriterAdmission $admission }
    # Keep the original session authority held across dispatch. The pinned
    # helper acquires admission itself and checks this exact delegation and
    # HeldByOther before mutation; ordinary writers still refuse the fence.
    return InvokeFrozenPlacementHelper $Frozen $HelperParameters
}

function Initialize-LabelPlacementSessionProtocol {
    if ('LabelPlacementChannel' -as [type]) { return }
    Add-Type -TypeDefinition @'
using System;
using System.IO;
using System.IO.Pipes;
using System.Diagnostics;
using System.Runtime.InteropServices;
using System.Text;
using Microsoft.Win32.SafeHandles;
public static class LabelPlacementChannel {
 public static bool IdleExpired;
 [StructLayout(LayoutKind.Sequential)]
 struct SecurityAttributes { public int length; public IntPtr descriptor; public int inherit; }
 [DllImport("kernel32.dll", CharSet=CharSet.Unicode, SetLastError=true)]
 static extern SafePipeHandle CreateNamedPipe(string name, uint openMode, uint pipeMode,
  uint instances, uint outSize, uint inSize, uint timeout, ref SecurityAttributes attributes);
 public static NamedPipeServerStream Server(string name, byte[] descriptor) {
  var pinned=GCHandle.Alloc(descriptor,GCHandleType.Pinned);
  try {
   var attributes=new SecurityAttributes {length=Marshal.SizeOf(typeof(SecurityAttributes)),descriptor=pinned.AddrOfPinnedObject(),inherit=0};
   // Reject remote connections at the pipe boundary. NETWORK is also present
   // in valid SSH logon tokens, so it must not be denied in the local ACL.
   var handle=CreateNamedPipe(@"\\.\pipe\"+name,0x40080003,0x00000008,1,65536,65536,0,ref attributes);
   if(handle.IsInvalid) {int error=Marshal.GetLastWin32Error();handle.Dispose();throw new System.ComponentModel.Win32Exception(error);}
   try {return new NamedPipeServerStream(PipeDirection.InOut,true,false,handle);}
   catch {handle.Dispose();throw;}
  } finally {pinned.Free();}
 }
 [DllImport("kernel32.dll", SetLastError=true)]
 static extern bool GetNamedPipeClientProcessId(SafePipeHandle pipe, out uint pid);
 [DllImport("kernel32.dll", SetLastError=true)]
 static extern bool GetNamedPipeServerProcessId(SafePipeHandle pipe, out uint pid);
 public static int Peer(SafePipeHandle pipe, bool server) {
  uint pid; bool ok=server ? GetNamedPipeClientProcessId(pipe,out pid) : GetNamedPipeServerProcessId(pipe,out pid);
  if(!ok) throw new System.ComponentModel.Win32Exception(Marshal.GetLastWin32Error());
  return checked((int)pid);
 }
 static void Exact(Stream stream, byte[] bytes, int timeout, Process parent) {
  var clock=Stopwatch.StartNew(); int count=0;
  while(count<bytes.Length) {
   var read=stream.ReadAsync(bytes,count,bytes.Length-count);
   while(!read.Wait(250)) {
    if(parent!=null && parent.HasExited) throw new InvalidOperationException("PLACEMENT_PARENT_EXITED");
    if(clock.ElapsedMilliseconds>=timeout) {
     if(parent!=null && clock.ElapsedMilliseconds<timeout+600000) IdleExpired=true;
     else throw new TimeoutException("PLACEMENT_CHANNEL_TIMEOUT");
    }
   }
   int n=read.GetAwaiter().GetResult(); if(n==0) throw new EndOfStreamException("PLACEMENT_CHANNEL_CLOSED");
   count+=n;
  }
 }
 public static string Read(Stream stream, int timeout, Process parent) {
  var header=new byte[4]; Exact(stream,header,timeout,parent); int length=BitConverter.ToInt32(header,0);
  if(length<1 || length>65536) throw new InvalidDataException("PLACEMENT_FRAME_SIZE");
  var body=new byte[length]; Exact(stream,body,timeout,parent); return new UTF8Encoding(false,true).GetString(body);
 }
 public static void Write(Stream stream, string json) {
  var bytes=new UTF8Encoding(false,true).GetBytes(json);
  if(bytes.Length<1 || bytes.Length>65536) throw new InvalidDataException("PLACEMENT_FRAME_SIZE");
  var header=BitConverter.GetBytes(bytes.Length);stream.Write(header,0,4);stream.Write(bytes,0,bytes.Length);stream.Flush();
 }
}
'@
}

function Read-LabelPlacementSessionRecord([string]$Path, [string]$ExpectedSha256, [string]$TransactionId, [int]$Sequence, [switch]$Seal, [switch]$Failure) {
    for ($ancestor=[IO.Path]::GetFullPath($Path); $ancestor; $ancestor=[IO.Path]::GetDirectoryName($ancestor)) {
        if ([IO.File]::GetAttributes($ancestor) -band [IO.FileAttributes]::ReparsePoint) { throw 'PLACEMENT_RESULT_REPARSE' }
    }
    $stream=[IO.File]::Open($Path,'Open','Read','Read')
    try {
        if ($stream.Length -lt 1 -or $stream.Length -gt 65536) { throw 'PLACEMENT_RESULT_SIZE' }
        [byte[]]$bytes=([IO.BinaryReader]::new($stream)).ReadBytes([int]$stream.Length)
        if ((ByteSha $bytes) -cne $ExpectedSha256) { throw 'PLACEMENT_RESULT_HASH' }
        $record=([Text.UTF8Encoding]::new($false,$true)).GetString($bytes) | ConvertFrom-Json
        $schema=if($Failure){'label-placement-session-failure-v1'}elseif($Seal){'label-placement-session-seal-v1'}else{'label-placement-session-result-v1'}
        if ([string]$record.schema -cne $schema -or
            [string]$record.transaction_id -cne $TransactionId -or
            ($record.sequence -isnot [int] -and $record.sequence -isnot [long]) -or $record.sequence -ne $Sequence) { throw 'PLACEMENT_RESULT_IDENTITY' }
        if($Failure){
            if($record.status -cne 'RECOVERY_REQUIRED' -or $record.reason_code -cnotin @('PARENT_EXITED','HELPER_WAIT_EXPIRED','HELPER_CHILD_UNPROVEN','GUARDIAN_FAILED') -or
               [string]$record.exception_type -cnotmatch '^[A-Za-z_][A-Za-z0-9_.]{0,127}$' -or
               [string]$record.location_module -cne 'INSTALL_CANONICAL_PORTABLE.ps1' -or
               [string]$record.location_function -cnotmatch '^[A-Za-z_][A-Za-z0-9_.-]{0,127}$' -or
               ($record.location_line -isnot [int] -and $record.location_line -isnot [long]) -or $record.location_line -le 0){throw 'PLACEMENT_FAILURE_IDENTITY'}
        }elseif(($record.exit_code -isnot [int] -and $record.exit_code -isnot [long]) -or
                $record.exit_code -lt [int]::MinValue -or $record.exit_code -gt [int]::MaxValue){throw 'PLACEMENT_RESULT_IDENTITY'}
        return $record
    } finally { $stream.Dispose() }
}

function Start-LabelPlacementSession([hashtable]$LaunchParameters, [string]$TransactionId, [string]$DiagnosticBase, [scriptblock]$TransportForTest) {
    Initialize-LabelPlacementSessionProtocol
    if ($TransactionId -cnotmatch '^[A-Za-z0-9-]{1,128}$') { throw 'PLACEMENT_SESSION_IDENTITY' }
    $pipeName='KMTech.LabelPlacement.'+[Guid]::NewGuid().ToString('N')
    $security=[IO.Pipes.PipeSecurity]::new()
    $security.SetAccessRuleProtection($true,$false)
    foreach ($sid in @([Security.Principal.WindowsIdentity]::GetCurrent().User,
        [Security.Principal.SecurityIdentifier]::new('S-1-5-18'),[Security.Principal.SecurityIdentifier]::new('S-1-5-32-544'))) {
        $security.AddAccessRule([IO.Pipes.PipeAccessRule]::new($sid,'FullControl','Allow'))
    }
    $pipe=[LabelPlacementChannel]::Server($pipeName,$security.GetSecurityDescriptorBinaryForm())
    $guardianPath=$DiagnosticBase+'-guardian-'+[Guid]::NewGuid().ToString('N')+'.ps1'
    $guardian=@'
param([string]$PipeName,[int]$ParentId,[long]$ParentBirth,[string]$ParentSid,[string]$TransactionId,[string]$ResultBase)
$ErrorActionPreference='Stop'
$channel=$null;$parent=$null;$journal=$null;$terminal='RECOVERY_REQUIRED';$sequence=0
function Publish([string]$Path,$Value) {
 $bytes=[Text.Encoding]::UTF8.GetBytes(($Value|ConvertTo-Json -Depth 8 -Compress)+[Environment]::NewLine)
 $stream=[IO.File]::Open($Path,'CreateNew','Write','Read')
 try {
  $acl=[Security.AccessControl.FileSecurity]::new();$acl.SetAccessRuleProtection($true,$false)
  foreach($sid in @([Security.Principal.SecurityIdentifier]::new('S-1-5-18'),[Security.Principal.SecurityIdentifier]::new('S-1-5-32-544'))){$acl.AddAccessRule([Security.AccessControl.FileSystemAccessRule]::new($sid,'FullControl','Allow'))}
  $acl.AddAccessRule([Security.AccessControl.FileSystemAccessRule]::new([Security.Principal.SecurityIdentifier]::new($ParentSid),'Read','Allow'))
  [IO.File]::SetAccessControl($Path,$acl)
  $stream.Write($bytes,0,$bytes.Length);$stream.Flush($true)
 } finally {$stream.Dispose()}
 # Pin the bytes we produced, never a reopened path that can be replaced.
 return (ByteSha $bytes)
}
function Append($Value) {
 $bytes=[Text.Encoding]::UTF8.GetBytes(($Value|ConvertTo-Json -Compress)+[Environment]::NewLine)
 $journal.Write($bytes,0,$bytes.Length);$journal.Flush($true)
}
try {
 Initialize-LabelPlacementSessionProtocol
 $parent=[Diagnostics.Process]::GetProcessById($ParentId)
 if($parent.StartTime.ToUniversalTime().ToFileTimeUtc() -ne $ParentBirth -or $parent.HasExited){throw 'PLACEMENT_PARENT_EXITED'}
 $channel=[IO.Pipes.NamedPipeClientStream]::new('.',$PipeName,'InOut','Asynchronous');$channel.Connect(60000)
 if([LabelPlacementChannel]::Peer($channel.SafePipeHandle,$false) -ne $ParentId){throw 'PLACEMENT_PARENT_IDENTITY'}
 $journalPath=$ResultBase+'.journal.jsonl'
 $journal=[IO.File]::Open($journalPath,'CreateNew','ReadWrite','Read')
 $acl=[Security.AccessControl.FileSecurity]::new();$acl.SetAccessRuleProtection($true,$false)
 foreach($sid in @([Security.Principal.SecurityIdentifier]::new('S-1-5-18'),[Security.Principal.SecurityIdentifier]::new('S-1-5-32-544'))){$acl.AddAccessRule([Security.AccessControl.FileSystemAccessRule]::new($sid,'FullControl','Allow'))}
 $acl.AddAccessRule([Security.AccessControl.FileSystemAccessRule]::new([Security.Principal.SecurityIdentifier]::new($ParentSid),'Read','Allow'))
 [IO.File]::SetAccessControl($journalPath,$acl)
 Append ([ordered]@{schema='label-placement-session-journal-v1';transaction_id=$TransactionId})
 Append ([ordered]@{phase='START';location_function='Start-LabelPlacementSession';location_line=1})
 [LabelPlacementChannel]::Write($channel,('{"status":"READY"}'))
 while($true) {
  $request=[LabelPlacementChannel]::Read($channel,720000,$parent)|ConvertFrom-Json
  if($parent.HasExited -or $parent.StartTime.ToUniversalTime().ToFileTimeUtc() -ne $ParentBirth){throw 'PLACEMENT_PARENT_EXITED'}
  if([string]$request.transaction_id -cne $TransactionId -or $request.sequence -isnot [int] -or $request.sequence -ne ($sequence+1)){throw 'PLACEMENT_COMMAND_IDENTITY'}
  $sequence=[int]$request.sequence
  if([string]$request.operation -cin @('CONFIRMED','REVERTED','ATTENTION')) {
   if([LabelPlacementChannel]::IdleExpired -and [string]$request.operation -ceq 'CONFIRMED'){throw 'PLACEMENT_WAIT_EXPIRED'}
   $terminal=[string]$request.operation;break
  }
  $verify=[string]$request.operation -ceq 'VERIFY'
  if([string]$request.operation -cnotin @('EXECUTE','VERIFY') -or
     (-not $verify -and [string]$request.command -cnotmatch '^[A-Za-z0-9+/]{1,16000}={0,2}$')){throw 'PLACEMENT_COMMAND_CONTRACT'}
  $expired=[LabelPlacementChannel]::IdleExpired -and ($request.rollback -isnot [bool] -or -not $request.rollback)
  $executionExpired=$false
  if($verify -or $expired){$exitCode=if($expired){70}else{0}}else{
  $start=[Diagnostics.ProcessStartInfo]::new()
  $start.FileName=Join-Path ([Environment]::SystemDirectory) 'WindowsPowerShell\v1.0\powershell.exe'
  $start.Arguments='-NoLogo -NoProfile -NonInteractive -ExecutionPolicy Bypass -EncodedCommand '+[string]$request.command
  $start.UseShellExecute=$false;$start.CreateNoWindow=$true
  $child=[Diagnostics.Process]::Start($start)
  try {
   # ExitCode belongs to the actual child. Runspace Completed is not success.
   try {$exitCode=Wait-LabelPlacementChild $child $parent}
   catch {
    $cause=$_.Exception.GetBaseException()
    if(-not $cause.Data.Contains('helper_child_exit_verified')){throw}
    $exitCode=[int]$cause.Data['helper_child_exit_code'];$executionExpired=$true
    [LabelPlacementChannel]::IdleExpired=$true
   }
  } finally {$child.Dispose()}
  }
  if($parent.HasExited){throw 'PLACEMENT_PARENT_EXITED'}
  $record=[ordered]@{schema='label-placement-session-result-v1';transaction_id=$TransactionId;sequence=$sequence;exit_code=$exitCode}
  if($executionExpired){
   $record.status='HELPER_EXECUTION_EXPIRED';$record.exception_type='System.TimeoutException'
   $record.location_module='INSTALL_CANONICAL_PORTABLE.ps1';$record.location_function='Wait-LabelPlacementChild'
   $record.location_line=$cause.Data['helper_failure_line']
  }
  $path=$ResultBase+'.'+$sequence+'.json';$sha=Publish $path $record
  $observed=[ordered]@{phase= $(if($verify -or $expired){'CONTROL_RESULT'}else{'CHILD_EXIT'});sequence=$sequence;exit_code=$exitCode;result_sha256=$sha}
  if($executionExpired){$observed.status='HELPER_EXECUTION_EXPIRED'}
  Append $observed
  [LabelPlacementChannel]::Write($channel,(@{transaction_id=$TransactionId;sequence=$sequence;sha256=$sha}|ConvertTo-Json -Compress))
 }
} catch {
 $cause=$_.Exception.GetBaseException()
 $reason=if($cause.Data.Contains('helper_child_unproven')){'HELPER_CHILD_UNPROVEN'}elseif($null -ne $parent -and $parent.HasExited){'PARENT_EXITED'}elseif($cause -is [TimeoutException]){'HELPER_WAIT_EXPIRED'}else{'GUARDIAN_FAILED'}
 $failure=[ordered]@{schema='label-placement-session-failure-v1';transaction_id=$TransactionId;sequence=$sequence;status='RECOVERY_REQUIRED';reason_code=$reason;exception_type=$cause.GetType().FullName;location_module='INSTALL_CANONICAL_PORTABLE.ps1';location_function='Start-LabelPlacementSession';location_line=$_.InvocationInfo.ScriptLineNumber}
 if($cause.Data.Contains('helper_failure_line')){$failure.location_function='Wait-LabelPlacementChild';$failure.location_line=$cause.Data['helper_failure_line']}
 try {
  $failureSha=Publish ($ResultBase+'.failure.json') $failure
  if($null -ne $journal){Append $failure}
  if($null -ne $channel){[LabelPlacementChannel]::Write($channel,(@{transaction_id=$TransactionId;sequence=$sequence;sha256=$failureSha;status='RECOVERY_REQUIRED'}|ConvertTo-Json -Compress))}
 }catch{}
} finally {
 if($null -ne $journal){
  Append ([ordered]@{phase='TERMINAL';status=$terminal;sequence=$sequence})
  # This is the last journal operation: no append after its digest is sealed.
  $journal.Position=0;$hash=[Security.Cryptography.SHA256]::Create()
  try {$digest=([BitConverter]::ToString($hash.ComputeHash($journal))).Replace('-','').ToLowerInvariant()}
  finally {$hash.Dispose();$journal.Dispose();$journal=$null}
  try {
   $sealExit=if($terminal -ceq 'RECOVERY_REQUIRED'){70}else{0}
   $sealSha=Publish ($ResultBase+'.seal.json') ([ordered]@{schema='label-placement-session-seal-v1';transaction_id=$TransactionId;sha256=$digest;status=$terminal;sequence=$sequence;exit_code=$sealExit})
   if($null -ne $channel){[LabelPlacementChannel]::Write($channel,(@{transaction_id=$TransactionId;sequence=$sequence;sha256=$sealSha}|ConvertTo-Json -Compress))}
  }catch{}
 }
 if($null -ne $channel){$channel.Dispose()};if($null -ne $parent){$parent.Dispose()}
}
if($terminal -ceq 'RECOVERY_REQUIRED'){exit 70};exit 0
'@
    $dependencies=''
    foreach($name in @('ByteSha','Initialize-LabelPlacementSessionProtocol','Stop-LabelRegistrationChild','Wait-LabelPlacementChild')){
        $dependencies+='function '+$name+' {'+(Get-Command $name).Definition+"}`n"
    }
    $guardian=$guardian.Replace("`n`$ErrorActionPreference='Stop'", "`n"+$dependencies+"`$ErrorActionPreference='Stop'")
    [byte[]]$bytes=[Text.UTF8Encoding]::new($false).GetBytes($guardian)
    $created=$false;$process=$null
    try {
        $stream=[IO.File]::Open($guardianPath,'CreateNew','Write','None');$created=$true
        try {
            $acl=[Security.AccessControl.FileSecurity]::new();$acl.SetAccessRuleProtection($true,$false)
            foreach($sid in @([Security.Principal.WindowsIdentity]::GetCurrent().User,[Security.Principal.SecurityIdentifier]::new('S-1-5-18'),[Security.Principal.SecurityIdentifier]::new('S-1-5-32-544'))){$acl.AddAccessRule([Security.AccessControl.FileSystemAccessRule]::new($sid,'ReadAndExecute,Delete','Allow'))}
            Set-Acl -LiteralPath $guardianPath -AclObject $acl -ErrorAction Stop
            $stream.Write($bytes,0,$bytes.Length);$stream.Flush($true)
        } finally {$stream.Dispose()}
        $birth=([Diagnostics.Process]::GetCurrentProcess()).StartTime.ToUniversalTime().ToFileTimeUtc()
        $resultBase=$guardianPath.Substring(0,$guardianPath.Length-4)
        $boot=@'
$ErrorActionPreference='Stop';$path='@@path@@'
for($p=$path;$p;$p=[IO.Path]::GetDirectoryName($p)){if([IO.File]::GetAttributes($p)-band [IO.FileAttributes]::ReparsePoint){throw 'PLACEMENT_GUARDIAN_REPARSE'}}
$s=[IO.File]::Open($path,'Open','Read','Read')
try {
 if($s.Length -gt 1048576){throw 'PLACEMENT_GUARDIAN_SIZE'}
 $b=([IO.BinaryReader]::new($s)).ReadBytes([int]$s.Length);$h=[Security.Cryptography.SHA256]::Create()
 try{$sha=([BitConverter]::ToString($h.ComputeHash($b))).Replace('-','').ToLowerInvariant()}finally{$h.Dispose()}
 if($sha -cne '@@sha@@'){throw 'PLACEMENT_GUARDIAN_HASH'}
 & ([ScriptBlock]::Create(([Text.UTF8Encoding]::new($false,$true)).GetString($b))) -PipeName '@@pipe@@' -ParentId @@pid@@ -ParentBirth @@birth@@ -ParentSid '@@parent-sid@@' -TransactionId '@@transaction@@' -ResultBase '@@result@@'
} finally {$s.Dispose()}
'@
        $boot=$boot.Replace('@@path@@',$guardianPath.Replace("'","''")).Replace('@@sha@@',(ByteSha $bytes)).Replace('@@pipe@@',$pipeName).Replace('@@pid@@',[string]$PID).Replace('@@birth@@',[string]$birth).Replace('@@transaction@@',$TransactionId).Replace('@@result@@',$resultBase.Replace("'","''"))
        $boot=$boot.Replace('@@parent-sid@@',[Security.Principal.WindowsIdentity]::GetCurrent().User.Value)
        $launch=$LaunchParameters.Clone();$launch.Wait=$false
        $launch.ArgumentList=@('-NoLogo','-NoProfile','-NonInteractive','-ExecutionPolicy','Bypass','-EncodedCommand',[Convert]::ToBase64String([Text.Encoding]::Unicode.GetBytes($boot)))
        $savedModulePath=$env:PSModulePath
        try {
            $env:PSModulePath=Join-Path ([Environment]::SystemDirectory) 'WindowsPowerShell\v1.0\Modules'
            if($null -ne $TransportForTest){$process=& $TransportForTest $launch}else{$process=Start-Process @launch}
        } finally {$env:PSModulePath=$savedModulePath}
        $connected=$pipe.BeginWaitForConnection($null,$null)
        if(-not $connected.AsyncWaitHandle.WaitOne(60000)){throw 'PLACEMENT_GUARDIAN_CONNECT_TIMEOUT'}
        $pipe.EndWaitForConnection($connected)
        if($process.HasExited -or [LabelPlacementChannel]::Peer($pipe.SafePipeHandle,$true) -ne $process.Id){throw 'PLACEMENT_GUARDIAN_IDENTITY'}
        $ready=[LabelPlacementChannel]::Read($pipe,60000,$null)|ConvertFrom-Json
        if([string]$ready.status -cne 'READY'){throw 'PLACEMENT_GUARDIAN_NOT_READY'}
        return [pscustomobject]@{pipe=$pipe;process=$process;transaction_id=$TransactionId;sequence=0;guardian_path=$guardianPath;result_base=$resultBase;closed=$false;rollback=$false;child_exit_unproven=$false}
    } catch {
        $pipe.Dispose();if($null -ne $process){$process.Dispose()}
        # A failed launch has no authority to start a second elevation.
        throw
    } finally {
        if($created -and $null -eq $process -and [IO.File]::Exists($guardianPath)){Remove-Item -LiteralPath $guardianPath}
    }
}

function Invoke-LabelPlacementSessionCommand($Session, [string]$EncodedCommand, [switch]$Verify) {
    # Only an authenticated normal response proves the native child finished.
    # A lost control channel must not let parent rollback race a live placement.
    try {
        if($Session.child_exit_unproven){throw 'HELPER_CHILD_UNPROVEN'}
        if($Session.closed -or $Session.process.HasExited){throw 'PLACEMENT_GUARDIAN_EXITED'}
        $Session.sequence++
        $operation=if($Verify){'VERIFY'}else{'EXECUTE'}
        [LabelPlacementChannel]::Write($Session.pipe,(@{operation=$operation;transaction_id=$Session.transaction_id;sequence=$Session.sequence;command=$EncodedCommand;rollback=[bool]$Session.rollback}|ConvertTo-Json -Compress))
        $response=[LabelPlacementChannel]::Read($Session.pipe,900000,$null)|ConvertFrom-Json
        if([string]$response.transaction_id -cne [string]$Session.transaction_id -or
            ($response.sequence -isnot [int] -and $response.sequence -isnot [long]) -or $response.sequence -ne $Session.sequence -or
            [string]$response.sha256 -cnotmatch '^[0-9a-f]{64}$'){throw 'PLACEMENT_RESPONSE_IDENTITY'}
        if($response.PSObject.Properties['status'] -and $response.status -cne 'RECOVERY_REQUIRED'){throw 'PLACEMENT_RESPONSE_IDENTITY'}
    } catch {
        $Session.child_exit_unproven=$true
        $_.Exception.Data['helper_child_unproven']=$true
        throw
    }
    if($response.PSObject.Properties['status'] -and $response.status -ceq 'RECOVERY_REQUIRED'){
        $Session.child_exit_unproven=$true
        try {
            $record=Read-LabelPlacementSessionRecord ($Session.result_base+'.failure.json') ([string]$response.sha256) $Session.transaction_id $Session.sequence -Failure
        } catch {
            # The authenticated recovery state survives a missing/forged
            # diagnostic file and is sealed in the parent's recovery audit.
            $_.Exception.Data['helper_child_unproven']=$true
            throw
        }
        $failure=[InvalidOperationException]::new([string]$record.reason_code)
        $failure.Data['helper_child_unproven']=$true
        $failure.Data['child_output']=[ordered]@{exception_type=[string]$record.exception_type;location_module=[string]$record.location_module;location_function=[string]$record.location_function;location_line=[int]$record.location_line;report_path=($Session.result_base+'.failure.json');report_sha256=[string]$response.sha256}
        throw $failure
    }
    $record=Read-LabelPlacementSessionRecord ($Session.result_base+'.'+$Session.sequence+'.json') ([string]$response.sha256) $Session.transaction_id $Session.sequence
    if($record.PSObject.Properties['status'] -and $record.status -ceq 'HELPER_EXECUTION_EXPIRED'){
        $failure=[TimeoutException]::new('HELPER_EXECUTION_EXPIRED')
        $failure.Data['helper_child_exit_verified']=$true
        $failure.Data['helper_child_exit_code']=[int]$record.exit_code
        $failure.Data['child_output']=[ordered]@{exception_type=[string]$record.exception_type;location_module=[string]$record.location_module;location_function=[string]$record.location_function;location_line=[int]$record.location_line;report_path=($Session.result_base+'.'+$Session.sequence+'.json');report_sha256=[string]$response.sha256}
        throw $failure
    }
    return [int]$record.exit_code
}

function Close-LabelPlacementSession($Session, [ValidateSet('CONFIRMED','REVERTED','ATTENTION')][string]$Outcome) {
    if($null -eq $Session -or $Session.closed){return}
    try {
        if($Session.process.HasExited){throw 'PLACEMENT_GUARDIAN_EXITED'}
        $Session.sequence++
        [LabelPlacementChannel]::Write($Session.pipe,(@{operation=$Outcome;transaction_id=$Session.transaction_id;sequence=$Session.sequence}|ConvertTo-Json -Compress))
        $receipt=[LabelPlacementChannel]::Read($Session.pipe,60000,$null)|ConvertFrom-Json
        if([string]$receipt.transaction_id -cne [string]$Session.transaction_id -or $receipt.sequence -ne $Session.sequence){throw 'PLACEMENT_JOURNAL_RECEIPT_DIFFERS'}
        if(-not $Session.process.WaitForExit(60000)){throw 'PLACEMENT_GUARDIAN_CLOSE_TIMEOUT'}
        if($Session.process.ExitCode -ne 0){throw 'PLACEMENT_GUARDIAN_CLOSE_FAILED'}
        $seal=Read-LabelPlacementSessionRecord ($Session.result_base+'.seal.json') ([string]$receipt.sha256) $Session.transaction_id $Session.sequence -Seal
        $journalPath=$Session.result_base+'.journal.jsonl'
        if([IO.File]::GetAttributes($journalPath)-band [IO.FileAttributes]::ReparsePoint){throw 'PLACEMENT_JOURNAL_REPARSE'}
        $journal=[IO.File]::Open($journalPath,'Open','Read','Read')
        try {
            if($journal.Length -gt 1048576){throw 'PLACEMENT_JOURNAL_SIZE'}
            $hash=[Security.Cryptography.SHA256]::Create()
            try{$journalSha=([BitConverter]::ToString($hash.ComputeHash($journal))).Replace('-','').ToLowerInvariant()}finally{$hash.Dispose()}
        } finally {$journal.Dispose()}
        if([string]$seal.transaction_id -cne [string]$Session.transaction_id -or [string]$seal.status -cne $Outcome -or
            [string]$seal.sha256 -cne $journalSha){throw 'PLACEMENT_JOURNAL_SEAL_DIFFERS'}
    } finally {
        $Session.closed=$true;$Session.pipe.Dispose();$Session.process.Dispose()
        if([IO.File]::Exists($Session.guardian_path)){Remove-Item -LiteralPath $Session.guardian_path}
    }
}

function Stop-LabelRegistrationChild([Diagnostics.Process]$Process, [long]$StartedAtFileTime) {
    # A PID is never ownership proof. Recheck the live chain and creation times
    # immediately before each stop, leaving unrelated/reused PIDs untouched.
    $deadline=[Diagnostics.Stopwatch]::StartNew()
    $rootId=$Process.Id
    function Get-LabelChildStopRemainingMilliseconds {
        $remaining=30000-$deadline.ElapsedMilliseconds
        if($remaining -le 0){throw 'REGISTRATION_CHILD_EXIT_UNPROVEN'}
        return [int]$remaining
    }
    function Get-LabelChildStopObservation([string]$Filter) {
        $remaining=Get-LabelChildStopRemainingMilliseconds
        $query=[PowerShell]::Create()
        $pending=$null;$discarded=$false
        try {
            # Preserve the caller's command resolution (including native test
            # observations) in this read-only, same-process runspace.
            $override=Get-Command Get-CimInstance -CommandType Function -ErrorAction SilentlyContinue
            if($null -ne $override){
                [void]$query.AddScript(('function Get-CimInstance {'+$override.Definition+'}')).AddStatement()
            }
            [void]$query.AddScript({param($filter,$seconds)
                Get-CimInstance Win32_Process -Filter $filter -OperationTimeoutSec $seconds -ErrorAction Stop
            }).AddArgument($Filter).AddArgument([uint32][Math]::Ceiling($remaining/1000.0))
            $pending=$query.BeginInvoke()
            $remaining=30000-$deadline.ElapsedMilliseconds
            if($remaining -le 0 -or -not $pending.AsyncWaitHandle.WaitOne([int]$remaining) -or
               $deadline.ElapsedMilliseconds -ge 30000){
                # Stop only the read-only query pipeline. Do not wait for CIM
                # cancellation or dispose a running pipeline after the deadline.
                $discarded=$true
                [void]$query.BeginStop($null,$null)
                throw 'REGISTRATION_CHILD_EXIT_UNPROVEN'
            }
            $rows=$query.EndInvoke($pending)
            [void](Get-LabelChildStopRemainingMilliseconds)
            if($query.HadErrors){throw $query.Streams.Error[0].Exception}
            return $rows
        } finally {
            if(-not $discarded){$query.Dispose()}
        }
    }
    $caller=[Diagnostics.Process]::GetCurrentProcess()
    $callerBirth=$caller.StartTime.ToUniversalTime()
    $owned=[Collections.Generic.List[object]]::new()
    try {
        $root=Get-LabelChildStopObservation "ProcessId = $rootId"
        if($Process.HasExited){return}
        if($null -eq $root -or $root.ParentProcessId -ne $PID -or
           $Process.StartTime.ToUniversalTime().ToFileTimeUtc() -ne $StartedAtFileTime -or
           $root.CreationDate.ToUniversalTime() -lt $callerBirth){throw 'REGISTRATION_CHILD_OWNERSHIP_UNPROVEN'}
        $owned.Add([pscustomobject]@{process=$Process;parent_id=$PID;birth=$Process.StartTime.ToUniversalTime();depth=0})
        for($index=0;$index -lt $owned.Count;$index++) {
            [void](Get-LabelChildStopRemainingMilliseconds)
            if($owned.Count -gt 128){throw 'REGISTRATION_CHILD_TREE_UNBOUNDED'}
            $node=$owned[$index]
            foreach($row in @(Get-LabelChildStopObservation ("ParentProcessId = "+$node.process.Id))) {
                [void](Get-LabelChildStopRemainingMilliseconds)
                try {$child=[Diagnostics.Process]::GetProcessById([int]$row.ProcessId)}catch [ArgumentException]{continue}
                $birth=$child.StartTime.ToUniversalTime()
                if($birth -lt $node.birth -or $birth -lt $callerBirth -or $row.CreationDate.ToUniversalTime() -lt $node.birth){$child.Dispose();throw 'REGISTRATION_CHILD_OWNERSHIP_UNPROVEN'}
                $owned.Add([pscustomobject]@{process=$child;parent_id=$node.process.Id;birth=$birth;depth=($node.depth+1)})
            }
        }
        foreach($node in @($owned | Sort-Object depth -Descending)) {
            [void](Get-LabelChildStopRemainingMilliseconds)
            if($node.process.HasExited){continue}
            $cursor=$node
            while($true) {
                [void](Get-LabelChildStopRemainingMilliseconds)
                if($cursor.process.HasExited -or $cursor.process.StartTime.ToUniversalTime() -ne $cursor.birth){throw 'REGISTRATION_CHILD_OWNERSHIP_UNPROVEN'}
                $live=Get-LabelChildStopObservation ("ProcessId = "+$cursor.process.Id)
                if($null -eq $live -or $live.ParentProcessId -ne $cursor.parent_id){throw 'REGISTRATION_CHILD_OWNERSHIP_UNPROVEN'}
                if($cursor.process.Id -eq $rootId){break}
                $parents=@($owned|Where-Object {$_.process.Id -eq $cursor.parent_id})
                if($parents.Count -ne 1 -or $cursor.birth -lt $parents[0].birth){throw 'REGISTRATION_CHILD_OWNERSHIP_UNPROVEN'}
                $cursor=$parents[0]
            }
            [void](Get-LabelChildStopRemainingMilliseconds)
            [void]$node.process.CloseMainWindow()
            $remaining=Get-LabelChildStopRemainingMilliseconds
            if(-not $node.process.WaitForExit([Math]::Min(250,$remaining))){
                # Kill through the already opened process handle, avoiding a
                # PID reuse race between the ownership check and termination.
                [void](Get-LabelChildStopRemainingMilliseconds)
                $node.process.Kill()
            }
        }
        foreach($node in $owned) {
            $remaining=Get-LabelChildStopRemainingMilliseconds
            if(-not $node.process.WaitForExit($remaining)){throw 'REGISTRATION_CHILD_EXIT_UNPROVEN'}
        }
        foreach($node in $owned){
            $survivors=@(Get-LabelChildStopObservation ("ParentProcessId = "+$node.process.Id) |
                Where-Object {$_.CreationDate.ToUniversalTime() -ge $node.birth})
            [void](Get-LabelChildStopRemainingMilliseconds)
            if($survivors.Count -gt 0){throw 'REGISTRATION_CHILD_EXIT_UNPROVEN'}
        }
    } finally {
        foreach($node in $owned){if($node.process.Id -ne $rootId){$node.process.Dispose()}}
        $caller.Dispose()
    }
}

function Wait-LabelPlacementChild([Diagnostics.Process]$Process, [Diagnostics.Process]$Parent, [int]$TimeoutMilliseconds=720000) {
    $birth=$Process.StartTime.ToUniversalTime().ToFileTimeUtc()
    $clock=[Diagnostics.Stopwatch]::StartNew()
    while(-not $Process.WaitForExit(250)) {
        if($Parent.HasExited){
            $lost=[InvalidOperationException]::new('PLACEMENT_PARENT_EXITED')
            $lost.Data['helper_failure_line']=$MyInvocation.ScriptLineNumber
            throw $lost
        }
        if($clock.ElapsedMilliseconds -ge $TimeoutMilliseconds){
            $failure=[TimeoutException]::new('HELPER_EXECUTION_EXPIRED')
            $failure.Data['helper_failure_line']=$MyInvocation.ScriptLineNumber
            try {
                Stop-LabelRegistrationChild $Process $birth
                if(-not $Process.HasExited){throw 'HELPER_CHILD_EXIT_UNPROVEN'}
                $failure.Data['helper_child_exit_verified']=$true
                $failure.Data['helper_child_exit_code']=[int]$Process.ExitCode
            } catch {$failure.Data['helper_child_unproven']=$true}
            throw $failure
        }
    }
    if($Parent.HasExited){
        $lost=[InvalidOperationException]::new('PLACEMENT_PARENT_EXITED')
        $lost.Data['helper_failure_line']=$MyInvocation.ScriptLineNumber
        throw $lost
    }
    return [int]$Process.ExitCode
}

function Wait-LabelRegistrationChild([Diagnostics.Process]$Process, [int]$TimeoutMilliseconds=600000) {
    $birth=$Process.StartTime.ToUniversalTime().ToFileTimeUtc()
    if($Process.WaitForExit($TimeoutMilliseconds)){return}
    $failure=[TimeoutException]::new('REGISTRATION_OBSERVATION_TIMEOUT')
    try {
        Stop-LabelRegistrationChild $Process $birth
        if(-not $Process.HasExited){throw 'REGISTRATION_CHILD_EXIT_UNPROVEN'}
        $failure.Data['registration_child_exit_verified']=$true
    } catch {
        $failure.Data['registration_child_unproven']=$true
        $failure.Data['registration_child_failure_type']=$_.Exception.GetType().FullName
    }
    throw $failure
}

function Confirm-LabelCanonicalPlacement($Onboarding, [string]$ExpectedRun, [string]$OnboardingPath,
    [DateTime]$StartedAt, [string]$TlsCaPath, [string]$ControlRoot, [string]$SessionId,
    [string]$AttemptId, [string]$TransactionId) {
    # This is T1: removing the active writer fence makes the prepared new code
    # usable. Every failure after attempting this write must preserve that code.
    $fresh=Get-Content -LiteralPath $OnboardingPath -Raw -Encoding UTF8|ConvertFrom-Json
    $run=Snapshot
    if([string]$fresh.status -cne 'READY' -or $fresh.server_registration_verified -isnot [bool] -or
       -not $fresh.server_registration_verified -or [string]$fresh.relay_autostart.command -cne $ExpectedRun -or
       -not $run.exists -or [string]$run.data -cne $ExpectedRun -or
       (Get-Item -LiteralPath $OnboardingPath).LastWriteTimeUtc -lt $StartedAt.AddSeconds(-1) -or
       [string]$Onboarding.status -cne 'READY'){throw 'CANONICAL_CONFIRMATION_READBACK_FAILED'}
    if(-not [string]::IsNullOrEmpty($TlsCaPath) -and
       ($null -eq $fresh.PSObject.Properties['state_readback'] -or
        $fresh.state_readback.tls_private_ca_configured -isnot [bool] -or
        -not $fresh.state_readback.tls_private_ca_configured)){throw 'CANONICAL_CONFIRMATION_TLS_FAILED'}
    $Script:LabelCanonicalConfirmation='UNATTEMPTED'
    # The guardian remains available until all confirmation checks have passed.
    if($null -ne $Script:LabelPlacementSession){
        if((Invoke-LabelPlacementSessionCommand $Script:LabelPlacementSession '' -Verify) -ne 0){throw 'PLACEMENT_WAIT_EXPIRED'}
        Close-LabelPlacementSession $Script:LabelPlacementSession 'CONFIRMED'
    }
    $Script:LabelCanonicalConfirmation='UNCERTAIN'
    [void](Stop-LabelWriterFence -ControlRoot $ControlRoot -SessionId $SessionId -AttemptId $AttemptId `
        -ReplacementTransactionId $TransactionId -TimeoutMilliseconds 90000)
    $Script:LabelCanonicalConfirmation='CONFIRMED'
}

function InvokeFrozenPlacementHelper($Frozen, [hashtable]$HelperParameters, [scriptblock]$TransportForTest = $null) {
    if ($null -ne $TransportForTest -and
        ($env:KMTECH_FACTORY_INSTALL_TEST_MODE -cne '1' -or
         -not $HelperParameters.ContainsKey('AllowNoncanonicalLayoutForTest') -or
         $HelperParameters.AllowNoncanonicalLayoutForTest -isnot [bool] -or
         -not [bool]$HelperParameters.AllowNoncanonicalLayoutForTest)) {
        throw 'Placement transport injection requires explicit factory test mode and layout.'
    }
    $Script:LabelPlacementChildFailure = $null
    $failureBase = [string]$HelperParameters.ElevationLogPath
    if ([string]::IsNullOrEmpty($failureBase)) {
        $failureBase = Join-Path ([IO.Path]::GetTempPath()) 'label-placement'
    }
    $failurePath = $failureBase + '-child-' + [Guid]::NewGuid().ToString('N') + '.json'
    for ($ancestor = [IO.Path]::GetDirectoryName($failurePath); $ancestor; $ancestor = [IO.Path]::GetDirectoryName($ancestor)) {
        if (([IO.File]::Exists($ancestor) -or [IO.Directory]::Exists($ancestor)) -and
            ([IO.File]::GetAttributes($ancestor) -band [IO.FileAttributes]::ReparsePoint)) {
            throw 'Placement diagnostic parent is a reparse point.'
        }
    }
    $payload = [ordered]@{
        failure_path = $failurePath
        helper_path = [string]$Frozen.helper_path
        helper_sha256 = [string]$Frozen.helper_sha256
        integrity_path = Join-Path ([string]$Frozen.root) 'tools\bootstrap_integrity.ps1'
        integrity_sha256 = [string]$Frozen.integrity_sha256
        writer_fence_path = [string]$Frozen.writer_fence_path
        writer_fence_sha256 = [string]$Frozen.writer_fence_sha256
        data_generation_path = [string]$Frozen.data_generation_path
        data_generation_sha256 = [string]$Frozen.data_generation_sha256
        parameters = $HelperParameters
    }
    $payloadJson = $payload | ConvertTo-Json -Depth 5 -Compress
    $launcher = @'
param([byte[]]$PayloadBytes)
$ErrorActionPreference = 'Stop'
$payloadText = (New-Object Text.UTF8Encoding($false, $true)).GetString($PayloadBytes)
$payload = $payloadText | ConvertFrom-Json
$placementPhase = 'PIN_VALIDATION'
trap {
    $childError = $_
    $childExit = 1
    $exception = $childError.Exception
    for ($depth = 0; $null -ne $exception -and $depth -lt 8; $depth++) {
        if ($exception.Data.Contains('installation_preparation_exit_code')) {
            $childExit = [int]$exception.Data['installation_preparation_exit_code']; break
        }
        $exception = $exception.InnerException
    }
    $sinkEnvironment = Get-KmtechSinkEnvironment
    $sinkEnvironment['KMTECH_LABEL_WRITER_DELEGATION_TOKEN'] = [string]$payload.parameters.WriterFenceDelegationToken
    $message = [string]$Script:KmtechRedactionRules.safe_messages[0]
    $trace = ConvertTo-KmtechRedactedSinkText -Text ($childError.Exception.ToString() + [Environment]::NewLine + [string]$childError.ScriptStackTrace) -Environment $sinkEnvironment
    $message = @($message -split '\r?\n' | Where-Object { $_.Trim() })[-1]
    if ($message.Length -gt 300) { $message = $message.Substring($message.Length-300) }
    $record = [ordered]@{
        schema = 'label-match-placement-child-failure-v1'; phase = $placementPhase; placement_code = 'PLACEMENT_CHILD_FAILED'
        exit_code = $childExit; exception_type = $childError.Exception.GetType().FullName
        hresult = $childError.Exception.HResult; message = $message
    }
    $nativeException=$childError.Exception
    while($null -ne $nativeException){
        if($nativeException.Data.Contains('label_legacy_native_failure')){
            $record.legacy_native_failure=$nativeException.Data['label_legacy_native_failure']
            $record.exception_type=$record.legacy_native_failure.exception_type
            $record.hresult=$record.legacy_native_failure.hresult;break
        }
        $nativeException=$nativeException.InnerException
    }
    try {
        $tracePath = [string]$payload.failure_path + '.trace.txt'
        $traceBytes = [Text.Encoding]::UTF8.GetBytes($trace)
        $truncated = $trace.Contains('[TRUNCATED:')
        $traceStream = [IO.File]::Open($tracePath, 'CreateNew', 'Write', 'Read')
        try {
            $traceAcl = [Security.AccessControl.FileSecurity]::new()
            $traceAcl.SetAccessRuleProtection($true,$false)
            foreach ($sid in @([Security.Principal.SecurityIdentifier]::new('S-1-5-18'),
                [Security.Principal.SecurityIdentifier]::new('S-1-5-32-544'))) {
                $traceAcl.AddAccessRule([Security.AccessControl.FileSystemAccessRule]::new($sid,'FullControl','Allow'))
            }
            $traceAcl.AddAccessRule([Security.AccessControl.FileSystemAccessRule]::new(
                [Security.Principal.WindowsIdentity]::GetCurrent().User,'Read','Allow'))
            Set-Acl -LiteralPath $tracePath -AclObject $traceAcl -ErrorAction Stop
            $traceStream.Write($traceBytes,0,$traceBytes.Length);$traceStream.Flush($true)
        } finally { $traceStream.Dispose() }
        $record.full_exception_trace = [ordered]@{path=$tracePath;sha256=(Get-FileHash -LiteralPath $tracePath -Algorithm SHA256).Hash.ToLowerInvariant();limit_bytes=65536;truncated=$truncated}
    } catch { $record.trace_failure_type = $_.Exception.GetType().Name }
    try {
        $stream = [IO.File]::Open([string]$payload.failure_path, 'CreateNew', 'Write', 'Read')
        try {
            $bytes = [Text.Encoding]::UTF8.GetBytes(((ConvertTo-KmtechRedactedSinkObject $record -Environment $sinkEnvironment) | ConvertTo-Json -Compress))
            $stream.Write($bytes, 0, $bytes.Length); $stream.Flush($true)
        } finally { $stream.Dispose() }
    } catch { [Console]::Error.WriteLine('Placement child diagnostic could not be saved.') }
    $publicFields = [ordered]@{status='FAILED';stage='SUPPORT';reason_code='INSTALLER_FAILED';placement_code=$record.placement_code;location_function=$placementPhase;exception_type=$record.exception_type;exit_code=$childExit;guidance=$message}
    if ($record.Contains('full_exception_trace')) {
        $publicFields.report_path=$record.full_exception_trace.path;$publicFields.report_sha256=$record.full_exception_trace.sha256
    } else { $publicFields.report_status='REPORT_UNAVAILABLE' }
    [Console]::Error.WriteLine('LABEL_MATCH_INSTALLER_FAILURE=' + ((ConvertTo-KmtechPublicRecord $publicFields) | ConvertTo-Json -Compress))
    exit $childExit
}
$pinnedStreams = [Collections.Generic.List[IO.Stream]]::new()
function PinnedBytes([string]$Path, [string]$Expected) {
    for ($item = [IO.Path]::GetFullPath($Path); $item; $item = [IO.Path]::GetDirectoryName($item)) {
        if ([IO.File]::GetAttributes($item) -band [IO.FileAttributes]::ReparsePoint) {
            throw "Elevated helper is a reparse point: $Path"
        }
    }
    $stream = [IO.File]::Open($Path, 'Open', 'Read', 'Read')
    $pinnedStreams.Add($stream)
    if ($stream.Length -gt 16777216) { throw 'Elevated helper is not bounded.' }
    [byte[]]$bytes = ([IO.BinaryReader]::new($stream)).ReadBytes([int]$stream.Length)
    $hash = [Security.Cryptography.SHA256]::Create()
    try {
        $actual = ([BitConverter]::ToString($hash.ComputeHash($bytes))).Replace('-', '').ToLowerInvariant()
    }
    finally { $hash.Dispose() }
    if ($actual -cne $Expected) { throw "Elevated helper byte pin differs: $Path" }
    return ,$bytes
}
function Get-RequiredExternalBoolean($Object, [string]$Name) {
    if ($null -eq $Object) { throw "External object is absent: $Name" }
    if ($Object -is [Collections.IDictionary]) {
        if (-not $Object.Contains($Name)) { throw "External boolean is absent: $Name" }
        $value = $Object[$Name]
    }
    else {
        $property = $Object.PSObject.Properties[$Name]
        if ($null -eq $property) { throw "External boolean is absent: $Name" }
        $value = $property.Value
    }
    if ($value -isnot [bool]) { throw "External boolean has invalid type: $Name" }
    return [bool]$value
}
function Get-RequiredExternalInteger($Object, [string]$Name) {
    if ($null -eq $Object) { throw "External object is absent: $Name" }
    if ($Object -is [Collections.IDictionary]) {
        if (-not $Object.Contains($Name)) { throw "External integer is absent: $Name" }
        $value = $Object[$Name]
    }
    else {
        $property = $Object.PSObject.Properties[$Name]
        if ($null -eq $property) { throw "External integer is absent: $Name" }
        $value = $property.Value
    }
    if ($value -isnot [int] -and $value -isnot [long]) {
        throw "External integer has invalid type: $Name"
    }
    return [int64]$value
}
[byte[]]$integrityBytes = PinnedBytes ([string]$payload.integrity_path) ([string]$payload.integrity_sha256)
[byte[]]$writerFenceBytes = PinnedBytes ([string]$payload.writer_fence_path) ([string]$payload.writer_fence_sha256)
[byte[]]$helperBytes = PinnedBytes ([string]$payload.helper_path) ([string]$payload.helper_sha256)
[byte[]]$dataGenerationBytes = PinnedBytes ([string]$payload.data_generation_path) ([string]$payload.data_generation_sha256)
$utf8 = New-Object Text.UTF8Encoding($false, $true)
$placementPhase = 'HELPER_LOADING'
. ([ScriptBlock]::Create(($utf8.GetString($integrityBytes) -replace '^\uFEFF', ''))) -SharedCodeRoot (Split-Path (Split-Path ([string]$payload.integrity_path) -Parent) -Parent)
. ([ScriptBlock]::Create(($utf8.GetString($writerFenceBytes) -replace '^\uFEFF', '')))
. ([ScriptBlock]::Create(($utf8.GetString($dataGenerationBytes) -replace '^\uFEFF', '')))
$helper = [ScriptBlock]::Create(($utf8.GetString($helperBytes) -replace '^\uFEFF', ''))
$placementPhase = 'PARAMETER_VALIDATION'
$parameterNames = @(
    'SourceRoot',
    'InstallRoot',
    'ElevationLogPath',
    'ExpectedBootstrapScriptSha256',
    'VerifiedBootstrapScriptPath',
    'BootstrapIntegrityPreloaded',
    'ExpectedSourceAggregateSha256',
    'ExpectedSourceFileCount',
    'ExpectedSourceByteCount',
    'WriterFenceFunctionsPreloaded',
    'WriterFenceControlRoot',
    'DataGenerationSubjectSid',
    'DataGenerationMarkerPaths',
    'WriterFenceSessionId',
    'WriterFenceAttemptId',
    'WriterFenceReplacementTransactionId',
    'WriterFenceDelegationToken',
    'AllowNoncanonicalLayoutForTest',
    'ReplaceExistingVerifiedPortable'
    'DryRun'
)
$actualParameterNames = @($payload.parameters.PSObject.Properties.Name)
if (
    @($parameterNames | Where-Object { $_ -notin $actualParameterNames }).Count -ne 0 -or
    @($actualParameterNames | Where-Object { $_ -notin ($parameterNames + @('Uninstall','StartupUpdateRestorePreimage','StartupUpdateRestoreSha256','ExpectedActiveCommit','InstallerRunId','InstallerOwnerProcessId','InstallerOwnerProcessStartedAtFileTime','LegacyRelayTransition','LegacyRelayPreimageSha256')) }).Count -ne 0
) { throw 'Elevated helper parameter contract differs.' }
$expectedSourceFileCount = Get-RequiredExternalInteger $payload.parameters 'ExpectedSourceFileCount'
$expectedSourceByteCount = Get-RequiredExternalInteger $payload.parameters 'ExpectedSourceByteCount'
if ($expectedSourceFileCount -lt 0 -or $expectedSourceFileCount -gt [int]::MaxValue) {
    throw 'External integer is outside the supported range: ExpectedSourceFileCount'
}
if ($expectedSourceByteCount -lt 0) {
    throw 'External integer is outside the supported range: ExpectedSourceByteCount'
}
$invokeParameters = @{
    SourceRoot = [string]$payload.parameters.SourceRoot
    InstallRoot = [string]$payload.parameters.InstallRoot
    ElevationLogPath = [string]$payload.parameters.ElevationLogPath
    ExpectedBootstrapScriptSha256 = [string]$payload.parameters.ExpectedBootstrapScriptSha256
    VerifiedBootstrapScriptPath = [string]$payload.parameters.VerifiedBootstrapScriptPath
    BootstrapIntegrityPreloaded = Get-RequiredExternalBoolean $payload.parameters 'BootstrapIntegrityPreloaded'
    ExpectedSourceAggregateSha256 = [string]$payload.parameters.ExpectedSourceAggregateSha256
    ExpectedSourceFileCount = [int]$expectedSourceFileCount
    ExpectedSourceByteCount = [uint64]$expectedSourceByteCount
    WriterFenceFunctionsPreloaded = Get-RequiredExternalBoolean $payload.parameters 'WriterFenceFunctionsPreloaded'
    WriterFenceControlRoot = [string]$payload.parameters.WriterFenceControlRoot
    DataGenerationSubjectSid = [string]$payload.parameters.DataGenerationSubjectSid
    DataGenerationMarkerPaths = @($payload.parameters.DataGenerationMarkerPaths | ForEach-Object { [string]$_ })
    WriterFenceSessionId = [string]$payload.parameters.WriterFenceSessionId
    WriterFenceAttemptId = [string]$payload.parameters.WriterFenceAttemptId
    WriterFenceReplacementTransactionId = [string]$payload.parameters.WriterFenceReplacementTransactionId
    WriterFenceDelegationToken = [string]$payload.parameters.WriterFenceDelegationToken
    AllowNoncanonicalLayoutForTest = Get-RequiredExternalBoolean $payload.parameters 'AllowNoncanonicalLayoutForTest'
    ReplaceExistingVerifiedPortable = Get-RequiredExternalBoolean $payload.parameters 'ReplaceExistingVerifiedPortable'
    DryRun = Get-RequiredExternalBoolean $payload.parameters 'DryRun'
}
foreach ($name in @('ExpectedActiveCommit','StartupUpdateRestorePreimage','StartupUpdateRestoreSha256','LegacyRelayTransition','LegacyRelayPreimageSha256')) {
    $property = $payload.parameters.PSObject.Properties[$name]
    $invokeParameters[$name] = if ($null -eq $property) { '' } else { [string]$property.Value }
}
foreach($n in @('InstallerRunId','InstallerOwnerProcessStartedAtFileTime')){$p=$payload.parameters.PSObject.Properties[$n];if($null -ne $p){$invokeParameters[$n]=[string]$p.Value}}
if($null -ne $payload.parameters.PSObject.Properties['InstallerOwnerProcessId']){$op=Get-RequiredExternalInteger $payload.parameters 'InstallerOwnerProcessId';if($op -le 0 -or $op -gt [int]::MaxValue){throw 'Invalid installer owner PID.'};$invokeParameters.InstallerOwnerProcessId=[int]$op}
$Script:LabelWriterFenceAdmissionProductionRoot = [string]$invokeParameters.WriterFenceControlRoot
if ($null -ne $payload.parameters.PSObject.Properties['Uninstall'] -and
    (Get-RequiredExternalBoolean $payload.parameters 'Uninstall')) {
    if ($invokeParameters.ReplaceExistingVerifiedPortable -or $invokeParameters.DryRun) {
        throw 'Absent-code rollback cannot replace or dry-run.'
    }
    [void](Assert-BootstrapIntegrityRecord -Root $invokeParameters.InstallRoot)
    $inventory = @(Get-BootstrapCodeInventory -Root $invokeParameters.InstallRoot)
    if ((Get-BootstrapInventoryAggregate -Inventory $inventory) -cne $invokeParameters.ExpectedSourceAggregateSha256 -or
        $inventory.Count -ne $invokeParameters.ExpectedSourceFileCount -or
        [uint64](($inventory | Measure-Object -Property size -Sum).Sum) -ne $invokeParameters.ExpectedSourceByteCount) {
        throw 'Absent-code rollback candidate inventory differs.'
    }
    $invokeParameters.Uninstall = $true
}
$placementPhase = 'HELPER_EXECUTION'
& $helper @invokeParameters
if (-not $?) { throw 'Placement helper returned an unsuccessful pipeline.' }
exit 0
'@
    $redactionPrelude = '$Script:KmtechRedactionRules = ' + "'" + (($Script:KmtechRedactionRules | ConvertTo-Json -Compress).Replace("'","''")) + "' | ConvertFrom-Json" + [Environment]::NewLine
    foreach ($name in @('Test-KmtechSecretEnvironmentValue','Get-KmtechSinkEnvironment','ConvertTo-KmtechRedactedSinkText','ConvertTo-KmtechRedactedSinkObject','ConvertTo-KmtechPublicRecord','Assert-KmtechMachineMetadataText')) {
        $redactionPrelude += 'function ' + $name + ' {' + (Get-Command $name).Definition + '}' + [Environment]::NewLine
    }
    # Keep the launcher's param block first; preceding statements turn param
    # into a command and lose the byte payload before the failure trap exists.
    $parameterHeader = 'param([byte[]]$PayloadBytes)'
    $parameterEnd = $launcher.IndexOf([char]10)
    if (-not $launcher.StartsWith($parameterHeader) -or $parameterEnd -lt 0) { throw 'Placement launcher parameter header differs.' }
    $launcher = $launcher.Insert($parameterEnd + 1, $redactionPrelude)
    $launcher = $launcher -replace '(?m)^[\t ]+', ''
    [byte[]]$launcherBytes = (New-Object Text.UTF8Encoding($false)).GetBytes($launcher)
    [byte[]]$payloadBytes = (New-Object Text.UTF8Encoding($false)).GetBytes($payloadJson)
    # Stay within the already supported diagnostic path length (MAX_PATH).
    $transportBase = $failurePath.Substring(0, $failurePath.Length - 5)
    $launcherPath = $transportBase + '.ps1'
    $payloadPath = $transportBase + '.dat'
    # The audit parent was checked above. Only these new, unpredictable files
    # are owned here. Publish private bytes after protecting their read access;
    # elevation trusts the in-memory hashes, never an adjacent manifest.
    $privateAcl = New-Object Security.AccessControl.FileSecurity
    $privateAcl.SetAccessRuleProtection($true, $false)
    foreach ($sid in @([Security.Principal.WindowsIdentity]::GetCurrent().User,
        [Security.Principal.SecurityIdentifier]::new('S-1-5-18'),
        [Security.Principal.SecurityIdentifier]::new('S-1-5-32-544'))) {
        [void]$privateAcl.AddAccessRule([Security.AccessControl.FileSystemAccessRule]::new(
            $sid, ([Security.AccessControl.FileSystemRights]::ReadAndExecute -bor
                [Security.AccessControl.FileSystemRights]::Delete), 'Allow'))
    }
    $createdPaths = @()
    try {
        foreach ($entry in @(@($launcherPath, $launcherBytes), @($payloadPath, $payloadBytes))) {
            $stream = [IO.File]::Open([string]$entry[0], 'CreateNew', 'Write', 'None')
            $createdPaths += [string]$entry[0]
            try {
                if ($PSVersionTable.PSVersion.Major -ge 6) {
                    [IO.FileSystemAclExtensions]::SetAccessControl([IO.FileInfo]::new([string]$entry[0]), $privateAcl)
                } else {
                    [IO.File]::SetAccessControl([string]$entry[0], $privateAcl)
                }
                $stream.Write([byte[]]$entry[1], 0, $entry[1].Length)
                $stream.Flush($true)
            } finally { $stream.Dispose() }
        }
    $bootstrap = @'
$ErrorActionPreference='Stop'
$streams=[Collections.Generic.List[IO.Stream]]::new()
function ReadPinned([string]$p,[string]$expected){
 for($a=[IO.Path]::GetFullPath($p);$a;$a=[IO.Path]::GetDirectoryName($a)){
  if([IO.File]::GetAttributes($a) -band [IO.FileAttributes]::ReparsePoint){throw 'Placement transport reparse point.'}
 }
 $s=[IO.File]::Open($p,'Open','Read','Read');$streams.Add($s)
 if($s.Length -gt 16777216){throw 'Placement transport is not bounded.'}
 [byte[]]$b=([IO.BinaryReader]::new($s)).ReadBytes([int]$s.Length)
 $h=[Security.Cryptography.SHA256]::Create()
 try{$actual=([BitConverter]::ToString($h.ComputeHash($b))).Replace('-','').ToLowerInvariant()}finally{$h.Dispose()}
 if($actual -cne $expected){throw 'Placement transport byte pin differs.'}
 return ,$b
}
$transportPhase='TRANSPORT_LAUNCHER'
try{
 [byte[]]$code=ReadPinned '@@launcher-path@@' '@@launcher-sha@@'
 $transportPhase='TRANSPORT_PAYLOAD'
 [byte[]]$data=ReadPinned '@@payload-path@@' '@@payload-sha@@'
 # Read handles deny write/delete until execution finishes. Execute only the
 # verified memory buffers, with no second read or path-based invocation.
 $transportPhase='TRANSPORT_EXECUTION'
 & ([ScriptBlock]::Create(([Text.UTF8Encoding]::new($false,$true)).GetString($code))) -PayloadBytes $data
}catch{
 # Before trusted code loads, retain only typed metadata. Never copy the
 # exception message, payload, command or environment into this fallback.
 $failurePath='@@failure-path@@'
 $record=[ordered]@{schema='label-match-placement-child-failure-v1';phase=$transportPhase;placement_code='PLACEMENT_TRANSPORT_REJECTED';exit_code=1;exception_type=$_.Exception.GetType().FullName;hresult=$_.Exception.HResult;message='@@safe-message@@'}
 try{
  $tracePath=$failurePath+'.trace.txt'
  $acl=[Security.AccessControl.FileSecurity]::new();$acl.SetAccessRuleProtection($true,$false)
  foreach($sid in @([Security.Principal.SecurityIdentifier]::new('S-1-5-18'),[Security.Principal.SecurityIdentifier]::new('S-1-5-32-544'))){$acl.AddAccessRule([Security.AccessControl.FileSystemAccessRule]::new($sid,'FullControl','Allow'))}
  $acl.AddAccessRule([Security.AccessControl.FileSystemAccessRule]::new([Security.Principal.WindowsIdentity]::GetCurrent().User,'Read','Allow'))
  $stream=[IO.File]::Open($tracePath,'CreateNew','Write','Read')
  try{Set-Acl -LiteralPath $tracePath -AclObject $acl -ErrorAction Stop;$bytes=[Text.Encoding]::UTF8.GetBytes(($record|ConvertTo-Json -Compress));$stream.Write($bytes,0,$bytes.Length);$stream.Flush($true)}finally{$stream.Dispose()}
  $record.full_exception_trace=[ordered]@{path=$tracePath;sha256=(Get-FileHash -LiteralPath $tracePath -Algorithm SHA256).Hash.ToLowerInvariant();limit_bytes=65536;truncated=$false}
 }catch{$record.trace_failure_type=$_.Exception.GetType().Name}
 try{
  $stream=[IO.File]::Open($failurePath,'CreateNew','Write','Read')
  try{$bytes=[Text.Encoding]::UTF8.GetBytes(($record|ConvertTo-Json -Compress));$stream.Write($bytes,0,$bytes.Length);$stream.Flush($true)}finally{$stream.Dispose()}
 }catch{}
 $public=[ordered]@{status='FAILED';stage='SUPPORT';reason_code='INSTALLER_FAILED';placement_code=$record.placement_code;location_function=$transportPhase;exit_code=1;exception_type=$record.exception_type;guidance=$record.message}
 if($record.Contains('full_exception_trace')){$public.report_path=$record.full_exception_trace.path;$public.report_sha256=$record.full_exception_trace.sha256}else{$public.report_status='REPORT_UNAVAILABLE'}
 [Console]::Error.WriteLine('LABEL_MATCH_INSTALLER_FAILURE='+($public|ConvertTo-Json -Compress))
 exit 1
}
finally{foreach($s in $streams){$s.Dispose()}}
'@
    $bootstrap = $bootstrap.Replace('@@launcher-path@@', $launcherPath.Replace("'", "''")).Replace(
        '@@launcher-sha@@', (ByteSha $launcherBytes)).Replace(
        '@@payload-path@@', $payloadPath.Replace("'", "''")).Replace(
        '@@payload-sha@@', (ByteSha $payloadBytes))
    $bootstrap = $bootstrap.Replace('@@failure-path@@', $failurePath.Replace("'", "''")).Replace('@@safe-message@@', ([string]$Script:KmtechRedactionRules.safe_messages[0]).Replace("'", "''"))
    $encodedLauncher = [Convert]::ToBase64String(
        [Text.Encoding]::Unicode.GetBytes($bootstrap)
    )
    if ($encodedLauncher.Length + 80 -gt 16000) {
        throw 'Placement launcher command is not bounded.'
    }
    $winps = Join-Path ([Environment]::SystemDirectory) 'WindowsPowerShell\v1.0\powershell.exe'
    # Test the current access token, not the account's group membership. A
    # filtered administrator token still needs consent; an elevated SSH or
    # shell token can start the same pinned helper directly without a dialog.
    $principal = [Security.Principal.WindowsPrincipal]::new(
        [Security.Principal.WindowsIdentity]::GetCurrent())
    $launchParameters = @{
        FilePath = $winps
        ArgumentList = @(
            '-NoLogo',
            '-NoProfile',
            '-NonInteractive',
            '-ExecutionPolicy',
            'Bypass',
            '-EncodedCommand',
            $encodedLauncher
        )
        WindowStyle = 'Hidden'
        Wait = $true
        PassThru = $true
    }
    if (-not $principal.IsInRole([Security.Principal.WindowsBuiltInRole]::Administrator)) {
        $launchParameters.Verb = 'RunAs'
    }
    # One test-only transport seam replaces ShellExecute with same-context
    # execution. Production cannot activate it through an environment alone.
    $persistent = Get-Variable -Name LabelPlacementSessionRequested -Scope Script -ErrorAction SilentlyContinue
    if ($null -ne $persistent -and $persistent.Value -is [bool] -and $persistent.Value) {
        if ($null -eq $Script:LabelPlacementSession) {
            if ($Script:LabelPlacementSessionLaunchAttempted) { throw 'PLACEMENT_GUARDIAN_UNAVAILABLE' }
            $Script:LabelPlacementSessionLaunchAttempted=$true
            # Publish no session until the one elevation has connected and its
            # native peer PID matches the actual Start-Process result.
            $Script:LabelPlacementSession = Start-LabelPlacementSession $launchParameters `
                ([string]$HelperParameters.WriterFenceReplacementTransactionId) $failureBase $TransportForTest
        }
        $childExit = Invoke-LabelPlacementSessionCommand $Script:LabelPlacementSession $encodedLauncher
    } else {
        $process = if ($null -ne $TransportForTest) { & $TransportForTest $launchParameters } else { Start-Process @launchParameters }
        $childExit = [int]$process.ExitCode
    }
    if (Test-Path -LiteralPath $failurePath -PathType Leaf) {
        $diagnosticItem = Get-Item -LiteralPath $failurePath
        if (($diagnosticItem.Attributes -band [IO.FileAttributes]::ReparsePoint) -or $diagnosticItem.Length -gt 65536) {
            throw 'Placement child failure evidence is not a bounded ordinary file.'
        }
        $record = [IO.File]::ReadAllText($failurePath) | ConvertFrom-Json
        if ([string]$record.schema -cne 'label-match-placement-child-failure-v1' -or
            [int]$record.exit_code -ne $childExit) { throw 'Placement child failure evidence differs.' }
        $Script:LabelPlacementChildFailure = $record
        if ($null -ne $record.PSObject.Properties['full_exception_trace']) {
            $tracePath = [string]$record.full_exception_trace.path
            if ($tracePath -cne ($failurePath + '.trace.txt')) { throw 'Placement trace path differs.' }
            $traceItem = Get-Item -LiteralPath $tracePath
            if (($traceItem.Attributes -band [IO.FileAttributes]::ReparsePoint) -or $traceItem.Length -gt 65536) { throw 'Placement trace is not a bounded ordinary file.' }
            $traceBytes = [IO.File]::ReadAllBytes($tracePath)
            if ((ByteSha $traceBytes) -cne [string]$record.full_exception_trace.sha256) { throw 'Placement trace bytes differ.' }
            $record | Add-Member -NotePropertyName full_exception_trace_text -NotePropertyValue ((New-Object Text.UTF8Encoding($false,$true)).GetString($traceBytes))
        }
    }
    if ($childExit -ne 0 -and $null -eq $Script:LabelPlacementChildFailure) {
        $fallback=[ordered]@{schema='label-match-placement-child-failure-v1';phase='CHILD_EXIT';placement_code='PLACEMENT_DIAGNOSTIC_UNAVAILABLE';exit_code=$childExit;exception_type='System.InvalidOperationException';hresult=0;message=[string]$Script:KmtechRedactionRules.safe_messages[0]}
        $reference=Write-KmtechProtectedFailureSidecar -Text ($fallback|ConvertTo-Json -Compress)
        $fallback.full_exception_trace=$reference
        $Script:LabelPlacementChildFailure=$fallback
    }
    return $childExit
    }
    catch {
        $originalPlacementFailure=$_
        if ($null -eq $Script:LabelPlacementChildFailure) {
            $reference=$null
            try { $reference=Write-KmtechProtectedFailureSidecar -Text (($originalPlacementFailure.Exception.ToString()+[Environment]::NewLine+[string]$originalPlacementFailure.ScriptStackTrace)) } catch { }
            $Script:LabelPlacementChildFailure=[ordered]@{schema='label-match-placement-child-failure-v1';phase='PARENT_LAUNCH';placement_code='PLACEMENT_LAUNCH_FAILED';exit_code=1;exception_type=$originalPlacementFailure.Exception.GetType().FullName;hresult=$originalPlacementFailure.Exception.HResult;message=[string]$Script:KmtechRedactionRules.safe_messages[0];full_exception_trace=$reference}
        }
        $originalPlacementFailure.Exception.Data['placement_child_failure']=$Script:LabelPlacementChildFailure
        throw $originalPlacementFailure
    }
    finally {
        foreach ($createdPath in $createdPaths) {
            if ([IO.File]::Exists($createdPath)) {
                Remove-Item -LiteralPath $createdPath -ErrorAction Stop
            }
        }
    }
}

function Relays {
    return @(Get-CimInstance Win32_Process -ErrorAction Stop | Where-Object {
        [string]$_.CommandLine -like '*--label-match-user-relay*' -and
        [string]$_.ExecutablePath -match '(?i)(pythonw?\.exe|Label_Match\.exe)$'
    })
}

function Normalize-RelayExecutableQuoting([string]$CommandLine, [string]$ExecutablePath) {
    # Windows startup may quote an executable that Arg would leave unquoted.
    # Strip only that exact, case-sensitive executable prefix; preserve all
    # argument bytes and required quotes around paths containing whitespace.
    $quoted = '"' + $ExecutablePath + '"'
    if ($ExecutablePath -notmatch '[\s"]' -and
        $CommandLine.StartsWith($quoted + ' ', [StringComparison]::Ordinal)) {
        return $ExecutablePath + $CommandLine.Substring($quoted.Length)
    }
    return $CommandLine
}

function Assert-RollbackRelayPreimage([object[]]$ExpectedRelays) {
    $actualRelays = @(Relays)
    if ($actualRelays.Count -ne $ExpectedRelays.Count) {
        throw 'rollback relay process-count readback failed'
    }
    foreach ($expected in $ExpectedRelays) {
        $matching = @($actualRelays | Where-Object {
            (Same ([string]$_.ExecutablePath) ([string]$expected.ExecutablePath)) -and
            (Normalize-RelayExecutableQuoting ([string]$_.CommandLine) ([string]$expected.ExecutablePath)) -ceq
                (Normalize-RelayExecutableQuoting ([string]$expected.CommandLine) ([string]$expected.ExecutablePath))
        })
        if ($matching.Count -ne 1) {
            throw 'rollback relay executable/command readback failed'
        }
    }
    return $actualRelays
}

function Protect-LabelFailureText([string]$Text) {
    if ($Text.TrimStart().StartsWith('{') -or $Text.TrimStart().StartsWith('[')) {
        try {
            $value = $Text | ConvertFrom-Json -ErrorAction Stop
            return ConvertTo-KmtechRedactedSinkText -Text ((ConvertTo-KmtechRedactedSinkObject $value) | ConvertTo-Json -Depth 30)
        } catch { }
    }
    return ConvertTo-KmtechRedactedSinkText -Text $Text
}

function Initialize-LabelChildOutputReader {
    # 64 KiB per stream covers a traceback/JSON report without unbounded audit growth.
    # On overflow retain the tail, where Python puts its lowest exception and cause.
    if ('LabelChildOutputReader' -as [type]) { [LabelChildOutputReader]::Configure((Get-KmtechSinkReplacements),$Script:KmtechRedactionRules.pattern,$Script:KmtechRedactionRules.path_suffix,$Script:KmtechRedactionRules.digest_suffix,$Script:KmtechRedactionRules.short_secret_digest,$Script:KmtechRedactionRules.field,$Script:KmtechRedactionRules.structured_delimiters); return }
    Add-Type -TypeDefinition @'
using System;
using System.IO;
using System.Text;
using System.Threading.Tasks;
using System.Collections.Generic;
using System.Linq;
using System.Text.RegularExpressions;

// Mask assignment and JSON secret values before bounded retention, including values spanning
// chunks. A truncated JSON document can no longer be parsed by the sink writer.
public sealed class LabelJsonFieldMasker {
 private readonly Regex secret, path, digest, shortSecret;
 public static string StructuredDelimiters;
 private readonly string marker, rawMarker, delimiters;
 private readonly StringBuilder token = new StringBuilder();
 private int state; private bool escaped, quoted, bareKey, malformed; private char quote, nestedQuote;
 private readonly Stack<char> closing = new Stack<char>();
 public LabelJsonFieldMasker(string pattern,string paths,string digests,string shorts,string replacement) {
  delimiters=StructuredDelimiters;
  if(String.IsNullOrEmpty(delimiters) || delimiters.Length%2!=0) throw new ArgumentException("Structured redaction grammar is invalid.");
  secret=new Regex(pattern+"|(?i)^(pin|otp|password)$");path=new Regex(paths);
  digest=new Regex(digests);shortSecret=new Regex(shorts);rawMarker=replacement;marker="\""+replacement+"\"";
 }
 private bool Sensitive(string key) {
  return secret.IsMatch(key) && !path.IsMatch(key) && !(digest.IsMatch(key) && !shortSecret.IsMatch(key));
 }
 public string Push(string text) {
  var output=new StringBuilder();
  foreach(char c in text) {
   bool again=true;
   while(again) {
    again=false;
    if(state==0) {
     if(c=='"' || c=='\'') {quote=c;token.Append(c);state=1;escaped=false;}
     else if(c=='_' || (c>='A' && c<='Z') || (c>='a' && c<='z')) {token.Append(c);state=8;}
     else output.Append(c);
    } else if(state==1) {
     token.Append(c);
     if(escaped) escaped=false;
     else if(c=='\\') escaped=true;
     else if(c==quote) state=2;
     // Long non-key strings remain ordinary text; secret values use state 4.
     if(token.Length>1024) {output.Append(token);token.Clear();state=7;}
    } else if(state==2) {
     if(Char.IsWhiteSpace(c)) {token.Append(c);}
     else if(c==':' || c=='=') {
      string raw=token.ToString().Trim();string key=raw.Substring(1,raw.Length-2);
      try {key=Regex.Unescape(key);} catch(ArgumentException) {}
      output.Append(token);output.Append(c);token.Clear();bareKey=false;state=Sensitive(key) ? 3 : 0;
     } else {output.Append(token);token.Clear();state=0;again=true;}
    } else if(state==3) {
     if(Char.IsWhiteSpace(c)) output.Append(c);
     else if(c=='"' || c=='\'') {quote=c;output.Append(quote+rawMarker+quote);state=4;escaped=false;}
     else if(c=='[') {token.Append(c);state=13;}
     else if(delimiters.IndexOf(c)>=0 && delimiters.IndexOf(c)%2==0) {output.Append(marker);state=5;closing.Clear();closing.Push(delimiters[delimiters.IndexOf(c)+1]);malformed=false;quoted=false;escaped=false;}
     else {token.Append(c);state=6;}
    } else if(state==4 || state==7) {
     if(state==7) output.Append(c);
     if(escaped) escaped=false;
     else if(c=='\\') escaped=true;
     else if(c==quote) state=0;
    } else if(state==5) {
     if(malformed) {if(c=='\r' || c=='\n') {output.Append(c);state=0;}}
     else if(quoted) {if(escaped) escaped=false;else if(c=='\\') escaped=true;else if(c==nestedQuote) quoted=false;}
     else if(c=='"' || c=='\'') {nestedQuote=c;quoted=true;}
     else if(delimiters.IndexOf(c)>=0 && delimiters.IndexOf(c)%2==0) closing.Push(delimiters[delimiters.IndexOf(c)+1]);
     else if(delimiters.IndexOf(c)>=0) {
      if(closing.Count==0 || closing.Peek()!=c) malformed=true;
      else {closing.Pop();if(closing.Count==0) state=0;}
     }
    } else if(state==6) {
     if(c==',' || c==';' || c=='}' || c==']' || Char.IsWhiteSpace(c)) {
      string value=token.ToString();output.Append(value=="true" || value=="false" || value=="null" ? value : (bareKey ? rawMarker : marker));
      token.Clear();state=0;again=true;
     } else if(token.Length<16) token.Append(c);
    } else if(state==8 || state==10) {
     bool keyChar=Char.IsLetterOrDigit(c) || c=='_' || c=='.' || c=='-';
     if(state==8 && keyChar) {
      token.Append(c);
      if(token.Length>1024) {output.Append(token);token.Clear();state=9;}
     } else if(Char.IsWhiteSpace(c)) {
      if(state==8) output.Append(token);
      output.Append(c);state=10;
     }
     else if(c==':' || c=='=') {
      string key=token.ToString().Trim();
      if(state==8) output.Append(token);
      output.Append(c);token.Clear();bareKey=true;
      bool metadata=path.IsMatch(key) || (digest.IsMatch(key) && !shortSecret.IsMatch(key));
      state=Sensitive(key) ? 3 : (metadata ? 11 : 0);
     } else {if(state==8) output.Append(token);token.Clear();state=0;again=true;}
    } else if(state==9) {
     if(Char.IsLetterOrDigit(c) || c=='_' || c=='.' || c=='-') output.Append(c);
     else {state=0;again=true;}
    } else if(state==11) {
     if(Char.IsWhiteSpace(c)) output.Append(c);
     else if(c=='"' || c=='\'') {quote=c;output.Append(c);state=7;escaped=false;}
     else {state=12;again=true;}
    } else if(state==12) {
     if(c==',' || c==';' || c=='}' || c==']' || Char.IsWhiteSpace(c)) {state=0;again=true;}
     else output.Append(c);
    } else if(state==13) {
     token.Append(c);string candidate=token.ToString();
     if("[REDACTED:".StartsWith(candidate,StringComparison.Ordinal)) {
      if(candidate=="[REDACTED:") {output.Append(token);token.Clear();state=14;}
     } else {
      output.Append(marker);token.Clear();state=5;closing.Clear();closing.Push(delimiters[delimiters.IndexOf('[')+1]);malformed=false;quoted=false;escaped=false;again=true;
     }
    } else if(state==14) {
     output.Append(c);if(c==']') state=0;
    }
   }
  }
  return output.ToString();
 }
 public string Finish() {
  string value=token.ToString();token.Clear();
  if(state==6) return value=="true" || value=="false" || value=="null" ? value : (bareKey ? rawMarker : marker);
  if(state==13) return marker;
  return state==1 || state==2 || state==8 ? value : "";
 }
}

public sealed class LabelChildOutput {
 public string Text; public long ByteCount; public bool Truncated;
 public Task Completion;
}
public static class LabelChildOutputReader {
 private static KeyValuePair<string,string>[] replacements = new KeyValuePair<string,string>[0];
 private static string pattern, paths, digests, shorts, field;
 public static void Configure(Dictionary<string,string> values,string patternValue,string pathValue,string digestValue,string shortValue,string fieldValue,string structuredValue) {
  LabelJsonFieldMasker.StructuredDelimiters=structuredValue;
  replacements = values.OrderByDescending(v => v.Key.Length).ThenBy(v => v.Key, StringComparer.Ordinal).ToArray();
  pattern=patternValue;paths=pathValue;digests=digestValue;shorts=shortValue;field=fieldValue;
 }
 public static LabelChildOutput Capture(StreamReader reader) {
  var result = new LabelChildOutput {Text = ""};
  result.Completion = Drain(reader, result, replacements); return result;
 }
 public static async Task<LabelChildOutput> Read(StreamReader reader) {
  var result = Capture(reader); await result.Completion.ConfigureAwait(false); return Snapshot(result);
 }
 public static LabelChildOutput Snapshot(LabelChildOutput result) {
  lock (result) {return new LabelChildOutput {Text=result.Text,ByteCount=result.ByteCount,Truncated=result.Truncated};}
 }
 private static string Clip(string value, int maximum, bool tail) {
  byte[] raw = Encoding.UTF8.GetBytes(value);
  if (raw.Length <= maximum) return value;
  if (tail) {
   int start=raw.Length-maximum; while ((raw[start]&0xC0)==0x80) start++;
   return Encoding.UTF8.GetString(raw,start,raw.Length-start);
  }
  int end=maximum; while ((raw[end]&0xC0)==0x80) end--;
  return Encoding.UTF8.GetString(raw,0,end);
 }
 private static async Task Drain(StreamReader reader, LabelChildOutput result, KeyValuePair<string,string>[] forms) {
  var buffer = new char[4096]; string pending="", head="", tail="";
  var masker = new LabelJsonFieldMasker(pattern,paths,digests,shorts,field);
  long rawCount=0, sanitizedCount=0; int count; bool headDone=false;
  int overlap=forms.Length==0 ? 2 : forms.Max(v=>v.Key.Length)+2;
  Action<string> emit = part => {
   int headRoom = 8192-Encoding.UTF8.GetByteCount(head);
   if (headRoom>0 && !headDone) {
    string prefix=Clip(part,headRoom,false); head+=prefix;part=part.Substring(prefix.Length);
    headDone=part.Length>0 || Encoding.UTF8.GetByteCount(head)==8192;
   }
   tail=Clip(tail+part,65536-Encoding.UTF8.GetByteCount(head),true);
   string output=head+tail;
   bool truncated=sanitizedCount>65536;
   if (truncated) {
    int tailBudget=65536-Encoding.UTF8.GetByteCount(head);
    string clipped=tail, marker;
    for(;;) {
     clipped=Clip(tail,tailBudget,true);
     long omitted=sanitizedCount-Encoding.UTF8.GetByteCount(head)-Encoding.UTF8.GetByteCount(clipped);
     marker="\n[TRUNCATED: "+omitted+" bytes omitted]\n";
     int next=65536-Encoding.UTF8.GetByteCount(head)-Encoding.UTF8.GetByteCount(marker);
     if(next>=tailBudget) break;tailBudget=next;
    }
    output=head+marker+clipped;
   }
   lock(result) {result.Text=output;result.ByteCount=rawCount;result.Truncated=truncated;}
  };
  try {
   while((count=await reader.ReadAsync(buffer,0,buffer.Length).ConfigureAwait(false))!=0) {
    rawCount+=Encoding.UTF8.GetByteCount(buffer,0,count);
    pending+=new string(buffer,0,count);
    foreach(var entry in forms) pending=pending.Replace(entry.Key,entry.Value);
    int split=Math.Max(0,pending.Length-overlap);
    if(split>0 && Char.IsHighSurrogate(pending[split-1])) split--;
    string part=masker.Push(pending.Substring(0,split));pending=pending.Substring(split);
    sanitizedCount+=Encoding.UTF8.GetByteCount(part);emit(part);
   }
  } catch(ObjectDisposedException) {} catch(IOException) {}
  foreach(var entry in forms) pending=pending.Replace(entry.Key,entry.Value);
  pending=masker.Push(pending)+masker.Finish();
  sanitizedCount+=Encoding.UTF8.GetByteCount(pending);emit(pending);
 }
}
'@
    [LabelChildOutputReader]::Configure((Get-KmtechSinkReplacements),$Script:KmtechRedactionRules.pattern,$Script:KmtechRedactionRules.path_suffix,$Script:KmtechRedactionRules.digest_suffix,$Script:KmtechRedactionRules.short_secret_digest,$Script:KmtechRedactionRules.field,$Script:KmtechRedactionRules.structured_delimiters)
}

function ConvertTo-LabelFailureOutput($Output) {
    return [ordered]@{text=(Protect-LabelFailureText ([string]$Output.Text));byte_count=$Output.ByteCount;truncated=$Output.Truncated;limit_bytes=65536}
}

function New-LabelChildFailure([string]$Mode, [int]$ExitCode, $Stdout, $Stderr) {
    $record = [ordered]@{stdout=$Stdout;stderr=$Stderr}
    $text = [string]$Stderr.text
    if ([string]::IsNullOrWhiteSpace($text)) { $text = [string]$Stdout.text }
    $types = [regex]::Matches($text, '(?m)^([A-Za-z_][A-Za-z0-9_.]*(?:Error|Exception))\s*:')
    $type = if ($types.Count) { $types[$types.Count - 1].Groups[1].Value } else { 'ChildNonzeroExit' }
    $last = [string]$Script:KmtechRedactionRules.safe_messages[0]
    $public = ConvertTo-KmtechPublicRecord ([ordered]@{status='FAILED';stage='CURRENT_USER_ONBOARDING';reason_code='CHILD_NONZERO_EXIT';exit_code=$ExitCode;exception_type=$type;guidance=$last;report_status='REPORT_UNAVAILABLE'})
    foreach ($stream in @($Stderr,$Stdout)) {
        foreach ($line in @([string]$stream.text -split '\r?\n')) {
            if ($line -cnotmatch '^LABEL_MATCH_INSTALLER_FAILURE=(\{[^\r\n]*\})$') { continue }
            try {
                $typed = $Matches[1] | ConvertFrom-Json -ErrorAction Stop
                $fields = [ordered]@{}
                foreach ($property in $typed.PSObject.Properties) { $fields[$property.Name]=$property.Value }
                $candidate = ConvertTo-KmtechPublicRecord $fields
                if ($candidate.Contains('public_record_code') -or -not $candidate.Contains('exception_type') -or -not $candidate.Contains('reason_code')) { continue }
                if ($candidate.Contains('report_path') -or $candidate.Contains('report_sha256')) {
                    if (-not $candidate.Contains('report_path') -or -not $candidate.Contains('report_sha256') -or
                        -not (Test-Path -LiteralPath $candidate.report_path -PathType Leaf) -or
                        (Get-FileHash -LiteralPath $candidate.report_path -Algorithm SHA256).Hash.ToLowerInvariant() -cne $candidate.report_sha256) {
                        $candidate.Remove('report_path'); $candidate.Remove('report_sha256')
                        $candidate.report_status='REPORT_UNAVAILABLE'
                    }
                }
                $public=$candidate; $public.exit_code=$ExitCode
                break
            } catch { }
        }
    }
    foreach ($streamName in @('stdout','stderr')) {
        $output = $record[$streamName]
        if ([string]::IsNullOrEmpty([string]$output.text)) { continue }
        try {
        $directory = Join-Path ([IO.Path]::GetTempPath()) ('label-product-diagnostic-'+[Guid]::NewGuid().ToString('N'))
        [void](New-Item -ItemType Directory -Path $directory -ErrorAction Stop)
        $acl = [Security.AccessControl.DirectorySecurity]::new()
        $acl.SetAccessRuleProtection($true,$false)
        foreach ($sid in @([Security.Principal.WindowsIdentity]::GetCurrent().User,
            [Security.Principal.SecurityIdentifier]::new('S-1-5-18'),
            [Security.Principal.SecurityIdentifier]::new('S-1-5-32-544'))) {
            $acl.AddAccessRule([Security.AccessControl.FileSystemAccessRule]::new($sid,'FullControl','ContainerInherit,ObjectInherit','None','Allow'))
        }
        Set-Acl -LiteralPath $directory -AclObject $acl
        $path = Join-Path $directory ($streamName+'.txt')
        $bytes = [Text.Encoding]::UTF8.GetBytes((Protect-LabelFailureText ([string]$output.text)))
        $file = [IO.File]::Open($path,'CreateNew','Write','Read')
        try { $file.Write($bytes,0,$bytes.Length);$file.Flush($true) } finally { $file.Dispose() }
        $hash = [Security.Cryptography.SHA256]::Create()
        try { $digest=([BitConverter]::ToString($hash.ComputeHash([IO.File]::ReadAllBytes($path)))).Replace('-','').ToLowerInvariant() }
        finally { $hash.Dispose() }
        $output.full_output = [ordered]@{path=$path;sha256=$digest;limit_bytes=65536;truncated=$output.truncated}
        } catch { $output.evidence_failure_type=$_.Exception.GetType().FullName }
        # The audit remains valid JSON and carries a pinned reference to the full stream.
        $output.text = ''
    }
    if (-not $public.Contains('report_path')) {
        foreach ($streamName in @('stderr','stdout')) {
            $output=$record[$streamName]
            if ($output.Contains('full_output')) {
                $public.Remove('report_status')
                $public.report_path=[string]$output.full_output.path
                $public.report_sha256=[string]$output.full_output.sha256
                break
            }
        }
    }
    $exception = [InvalidOperationException]::new('LABEL_MATCH_CHILD_FAILED')
    $exception.Data['child_output'] = $public
    return $exception
}

function Product([string]$Root, [string]$Mode, [string[]]$ExtraArguments = @(), [string]$LogicalAppRoot = '') {
    if ([string]::IsNullOrEmpty($LogicalAppRoot)) { $LogicalAppRoot = $Root }
    $args = '-I -B {0} {1} --app-root {2}' -f
        (Arg (Join-Path $Root 'app\main.py')),
        $Mode,
        (Arg $LogicalAppRoot)
    foreach ($extraArgument in $ExtraArguments) { $args += ' ' + (Arg $extraArgument) }
    Initialize-LabelChildOutputReader
    $startInfo = [Diagnostics.ProcessStartInfo]::new()
    $startInfo.FileName = Join-Path $Root 'runtime\pythonw.exe'
    $startInfo.Arguments = $args
    $startInfo.UseShellExecute = $false
    $startInfo.CreateNoWindow = $true
    $startInfo.RedirectStandardOutput = $true
    $startInfo.RedirectStandardError = $true
    $process = [Diagnostics.Process]::Start($startInfo)
    try {
        $stdoutCapture = [LabelChildOutputReader]::Capture($process.StandardOutput)
        $stderrCapture = [LabelChildOutputReader]::Capture($process.StandardError)
        # The registration budget is finite. A verified stopped child permits
        # rollback; uncertain ownership/exit preserves the new code and fence.
        if ($Mode -ceq '--onboard-current-user') { Wait-LabelRegistrationChild $process }
        else { $process.WaitForExit() }
        $exitCode = [int]$process.ExitCode
        # A persistent child can inherit these pipes. Capture its host's final
        # buffered output without waiting for that child's lifetime or stopping it.
        [void]$stdoutCapture.Completion.Wait(1000)
        [void]$stderrCapture.Completion.Wait(1000)
        $stdout = ConvertTo-LabelFailureOutput ([LabelChildOutputReader]::Snapshot($stdoutCapture))
        $stderr = ConvertTo-LabelFailureOutput ([LabelChildOutputReader]::Snapshot($stderrCapture))
        if ($exitCode -ne 0) {
            throw (New-LabelChildFailure $Mode $exitCode $stdout $stderr)
        }
    }
    finally {
        $process.Dispose()
    }
}


function Write-LabelPreparationFailure($Failure, [string]$Source,
    $Inventory, [string]$AuditRoot, [string]$ExternalEvidencePath) {
    $failureId = (Get-Date).ToUniversalTime().ToString('yyyyMMddTHHmmssfffZ') + '-' + [Guid]::NewGuid().ToString('N')
    $failurePath = Join-Path $AuditRoot ('canonical-portable-' + $failureId + '.json')
    $failureAudit = [ordered]@{
        schema='label-match-canonical-portable-install-v1'
        status='FAILED'; preparation_status='FAILED_PREPARATION_ADMISSION'; run_id=$failureId
        code_placement='NOT_STARTED'; source_root=$Source
        source_inventory_sha256=[string]$Inventory.sha256
        preparation_failure=(Get-LabelLaunchFailure $Failure 'preparation')
        rollback=[ordered]@{status='NOT_REQUIRED';available=$false;applied=$false;runtime_restored=$false}
    }
    try {
        Save $failurePath $failureAudit
        if ($ExternalEvidencePath) { Save (Full $ExternalEvidencePath 'EvidencePath') $failureAudit }
    } catch { }
    'install_status=FAILED'
    if (Test-Path -LiteralPath $failurePath -PathType Leaf) { "audit_path=$failurePath" }
}

function Invoke-LabelSourceDataAdmission([string]$ProductRoot, [string]$Source,
    $Inventory, [string]$AuditRoot, [string]$ExternalEvidencePath,
    [string[]]$OnboardingArgs = @()) {
    try {
        Product $ProductRoot '--onboard-current-user' `
            (@('--app-root', $Source, '--check-data-generation-only',
               '--installer-source-inventory-sha256', [string]$Inventory.sha256) + $OnboardingArgs)
    }
    catch {
        $admissionFailure = $_
        Write-LabelPreparationFailure $admissionFailure $Source $Inventory $AuditRoot $ExternalEvidencePath
        throw $admissionFailure
    }
}

function Get-LabelPortableDataVersion([string]$Root, $Manifest) {
    $manifestVersion = if ($Manifest -is [Collections.IDictionary]) { [string]$Manifest['version'] }
        elseif ($null -ne $Manifest -and $null -ne $Manifest.PSObject.Properties['version']) { [string]$Manifest.version }
        else { '' }
    if (-not [string]::IsNullOrWhiteSpace($manifestVersion)) { return $manifestVersion }
    # Older stock portable manifests omitted version. Read the verified source
    # literal; do not execute the old application to determine its identity.
    foreach ($relative in @('app\label_data_generation.py', 'app\Label_Match.py')) {
        $path = Join-Path $Root $relative
        if (Test-Path -LiteralPath $path -PathType Leaf) {
            if ((Get-Item -LiteralPath $path).Length -gt 10485760) { throw 'Release version source is oversized.' }
            $text = Get-Content -LiteralPath $path -Raw -Encoding UTF8
            $match = [regex]::Match($text, '(?m)^(?:CURRENT_VERSION|APP_VERSION)\s*=\s*["''](v\d+\.\d+\.\d+)["'']\s*$')
            if ($match.Success) { return $match.Groups[1].Value }
        }
    }
    return '' # The helper returns UNKNOWN; missing records remain ALLOW.
}

function Assert-LabelRollbackGeneration {
    # Before any rollback cleanup or old-code selection, retain the opened
    # generation. Code-only failures before data open still have no markers.
    if ($existingVerified) {
        $generationRollbackRoot = if ($replacementRollbackRoot) { $replacementRollbackRoot } else { $install }
        $generationRollbackManifest = Manifest $generationRollbackRoot $SkipSignatureValidationForTest
        $generationRollbackVersion = Get-LabelPortableDataVersion $generationRollbackRoot $generationRollbackManifest
        [void](Assert-LabelDataDowngrade -MarkerPaths $generationMarkerPaths -SubjectSid $generationSubjectSid `
            -TargetVersion $generationRollbackVersion -TargetBuildId ([string]$generationRollbackManifest.source_commit))
        # Both predicates are mandatory before any old-code/setup mutation.
        # A refused generation never starts the old target inventory walk.
        if ([string](PortableInventory $generationRollbackRoot).sha256 -cne [string]$installedPreimageInventory.sha256) {
            throw 'Exact rollback generation target is unproven; current code and data are preserved.'
        }
    }
    elseif (-not $existingVerified) {
        $generationAbsence = Get-KmtechDataDowngradeDecision -MarkerPaths $generationMarkerPaths `
            -TargetVersion 'v0.0.0' -TargetBuildId ('0' * 40) -AppId 'Label_Match' -SubjectSid $generationSubjectSid
        if ($generationAbsence.allowed -ne $true -or $null -ne $generationAbsence.version) {
            throw '새 판이 이 PC의 자료를 열어 설치 이전 코드 없음 상태로 되돌릴 수 없습니다. 현재 코드와 자료를 보존하고 관리자 지원 복구를 요청하세요.'
        }
    }
}

function Assert-LabelDataDowngrade(
    [string[]]$MarkerPaths, [string]$TargetVersion, [string]$SubjectSid, [string]$TargetBuildId
) {
    $decision = Get-KmtechDataDowngradeDecision -MarkerPaths $MarkerPaths `
        -TargetVersion $TargetVersion -AppId 'Label_Match' -SubjectSid $SubjectSid -TargetBuildId $TargetBuildId
    if ($decision.allowed -ne $true) { throw ([string]$decision.message) }
    return $decision
}

function Resolve-LabelGenerationInputPath([string]$Value, [string]$Root) {
    # Use the reader's resolver and the same FileSystem working directory as
    # Product's child. Do not interpret user path strings twice in two languages.
    $pathProbe = @'
import base64, sys
from pathlib import Path
sys.path.insert(0, str(Path(sys.argv[1]) / 'app'))
from label_data_generation import resolve_data_path
try:
    selected = resolve_data_path(base64.b64decode(sys.stdin.buffer.read()).decode('utf-8'))
    print(base64.b64encode(str(selected).encode('utf-8')).decode('ascii'))
except Exception:
    raise SystemExit(4)
'@
    # PS7 expands a native argv value beginning with ~ using its own HOME.
    # Byte stdin accepts the PS5 UTF-8 BOM and preserves non-ASCII paths.
    $pathInput = [Convert]::ToBase64String([Text.Encoding]::UTF8.GetBytes($Value))
    $resolved = @($pathInput | & (Join-Path $Root 'runtime\python.exe') -I -B -c $pathProbe $Root)
    if ($LASTEXITCODE -ne 0 -or $resolved.Count -ne 1 -or [string]::IsNullOrWhiteSpace([string]$resolved[0])) {
        throw '이 PC의 자료 경로를 확인할 수 없어 자료를 열 수 없습니다. 현재 코드와 자료를 보존하고 관리자에게 지원 복구를 요청하세요.'
    }
    try { $cooked = [Text.Encoding]::UTF8.GetString([Convert]::FromBase64String([string]$resolved[0])) }
    catch { throw '이 PC의 자료 경로를 확인할 수 없습니다. 현재 코드와 자료를 보존하고 관리자에게 지원 복구를 요청하세요.' }
    return Full $cooked 'Label generation input'
}

$WriterDelegationEnvironmentNames = @(
    'KMTECH_LABEL_WRITER_DELEGATION_TOKEN',
    'KMTECH_LABEL_WRITER_DELEGATION_SESSION_ID',
    'KMTECH_LABEL_WRITER_DELEGATION_ATTEMPT_ID',
    'KMTECH_LABEL_WRITER_DELEGATION_TRANSACTION_ID'
)

function SetWriterDelegationEnvironment(
    [string]$Token,
    [string]$SessionId,
    [string]$AttemptId,
    [string]$TransactionId
) {
    [Environment]::SetEnvironmentVariable($WriterDelegationEnvironmentNames[0], $Token, 'Process')
    [Environment]::SetEnvironmentVariable($WriterDelegationEnvironmentNames[1], $SessionId, 'Process')
    [Environment]::SetEnvironmentVariable($WriterDelegationEnvironmentNames[2], $AttemptId, 'Process')
    [Environment]::SetEnvironmentVariable($WriterDelegationEnvironmentNames[3], $TransactionId, 'Process')
}

function RestoreWriterDelegationEnvironment($Before) {
    for ($index = 0; $index -lt $WriterDelegationEnvironmentNames.Count; $index++) {
        [Environment]::SetEnvironmentVariable(
            $WriterDelegationEnvironmentNames[$index],
            $Before[$index],
            'Process'
        )
    }
}

function Initialize-CanonicalUserProcessApi {
    if ('LabelMatchCanonicalUserProcess' -as [type]) { return }
    Add-Type -TypeDefinition @'
using System;
using System.ComponentModel;
using System.Diagnostics;
using System.Runtime.InteropServices;
using System.Security.Principal;
using System.Text;
public static class LabelMatchCanonicalUserProcess {
    public sealed class Security { public string sid; public int session_id; public bool elevated; public int integrity_rid; }
    [StructLayout(LayoutKind.Sequential, CharSet=CharSet.Unicode)]
    struct Startup {
        public int cb; public string reserved, desktop, title;
        public int x,y,cx,cy,xChars,yChars,fill,flags; public short show,reservedBytes;
        public IntPtr reserved2,input,output,error;
    }
    [StructLayout(LayoutKind.Sequential)]
    struct ProcessInfo { public IntPtr process,thread; public int pid,tid; }
    [DllImport("advapi32.dll",SetLastError=true)] static extern bool OpenProcessToken(IntPtr process,uint access,out IntPtr token);
    [DllImport("advapi32.dll",SetLastError=true)] static extern bool GetTokenInformation(IntPtr token,int kind,IntPtr info,int length,out int needed);
    [DllImport("advapi32.dll",SetLastError=true)] static extern bool DuplicateTokenEx(IntPtr token,uint access,IntPtr attributes,int level,int type,out IntPtr copy);
    [DllImport("advapi32.dll",CharSet=CharSet.Unicode,SetLastError=true)]
    static extern bool CreateProcessWithTokenW(IntPtr token,uint logon,string application,StringBuilder command,uint flags,IntPtr environment,string directory,ref Startup startup,out ProcessInfo process);
    [DllImport("userenv.dll",SetLastError=true)] static extern bool CreateEnvironmentBlock(out IntPtr environment,IntPtr token,bool inherit);
    [DllImport("userenv.dll")] static extern bool DestroyEnvironmentBlock(IntPtr environment);
    [DllImport("kernel32.dll")] static extern bool CloseHandle(IntPtr handle);
    [DllImport("user32.dll")] static extern IntPtr GetShellWindow();
    [DllImport("user32.dll")] static extern uint GetWindowThreadProcessId(IntPtr window,out int pid);
    static void Check(bool success, string call) { if (!success) { var error=new Win32Exception(Marshal.GetLastWin32Error()); error.Data["call"]=call; throw error; } }
    static int Integer(IntPtr token,int kind) {
        IntPtr data=Marshal.AllocHGlobal(4);
        try { int needed; Check(GetTokenInformation(token,kind,data,4,out needed),"GetTokenInformation"); return Marshal.ReadInt32(data); }
        finally { Marshal.FreeHGlobal(data); }
    }
    [DllImport("advapi32.dll")] static extern IntPtr GetSidSubAuthorityCount(IntPtr sid);
    [DllImport("advapi32.dll")] static extern IntPtr GetSidSubAuthority(IntPtr sid,uint index);
    static int Integrity(IntPtr token) {
        int needed; GetTokenInformation(token,25,IntPtr.Zero,0,out needed);
        if(needed<=0 || needed>65536) throw new InvalidOperationException("Token integrity sizing differs.");
        IntPtr data=Marshal.AllocHGlobal(needed);
        try { Check(GetTokenInformation(token,25,data,needed,out needed),"GetTokenInformation:integrity");
            IntPtr sid=Marshal.ReadIntPtr(data); int count=Marshal.ReadByte(GetSidSubAuthorityCount(sid));
            if(count<=0) throw new InvalidOperationException("Token integrity SID differs.");
            return Marshal.ReadInt32(GetSidSubAuthority(sid,(uint)(count-1)));
        } finally { Marshal.FreeHGlobal(data); }
    }
    static Security Inspect(IntPtr token) {
        using (WindowsIdentity identity=new WindowsIdentity(token)) {
            return new Security {sid=identity.User.Value,session_id=Integer(token,12),elevated=Integer(token,20)!=0,integrity_rid=Integrity(token)};
        }
    }
    public static Security Read(int pid) {
        using (Process process=Process.GetProcessById(pid)) {
            IntPtr token; Check(OpenProcessToken(process.Handle,8,out token),"OpenProcessToken:child");
            try { return Inspect(token); } finally { CloseHandle(token); }
        }
    }
    public static int StartLimited(string command,string sid,int session) {
        IntPtr shell=GetShellWindow(); int shellPid;
        if (shell==IntPtr.Zero || GetWindowThreadProcessId(shell,out shellPid)==0)
            throw new InvalidOperationException("Canonical interactive shell is unavailable.");
        using (Process shellProcess=Process.GetProcessById(shellPid)) {
            IntPtr token; Check(OpenProcessToken(shellProcess.Handle,10,out token),"OpenProcessToken:shell");
            IntPtr primary=IntPtr.Zero,environment=IntPtr.Zero;
            try {
                Security security=Inspect(token);
                if (security.elevated || security.integrity_rid!=8192 || security.sid!=sid || security.session_id!=session || session<=0)
                    throw new InvalidOperationException("Canonical interactive shell security differs.");
                Check(DuplicateTokenEx(token,0x18B,IntPtr.Zero,2,1,out primary),"DuplicateTokenEx:primary");
                security=Inspect(primary);
                if (security.elevated || security.integrity_rid!=8192 || security.sid!=sid || security.session_id!=session)
                    throw new InvalidOperationException("Canonical normal-user token differs.");
                Check(CreateEnvironmentBlock(out environment,primary,false),"CreateEnvironmentBlock");
                Startup startup=new Startup(); startup.cb=Marshal.SizeOf(typeof(Startup)); startup.desktop="winsta0\\default";
                ProcessInfo child;
                Check(CreateProcessWithTokenW(primary,0,null,new StringBuilder(command),0x08000400,environment,null,ref startup,out child),"CreateProcessWithTokenW");
                try { return child.pid; } finally { CloseHandle(child.thread); CloseHandle(child.process); }
            } finally {
                if (environment!=IntPtr.Zero) DestroyEnvironmentBlock(environment);
                if (primary!=IntPtr.Zero) CloseHandle(primary);
                CloseHandle(token);
            }
        }
    }
}
'@
}
function Get-CanonicalRelaySecurity([int]$ProcessId) {
    Initialize-CanonicalUserProcessApi
    return [LabelMatchCanonicalUserProcess]::Read($ProcessId)
}
function Start-CanonicalLimitedRelay([string]$Line, [string]$UserSid, [int]$SessionId) {
    Initialize-CanonicalUserProcessApi
    return [LabelMatchCanonicalUserProcess]::StartLimited($Line, $UserSid, $SessionId)
}
function StartRaw([string]$Line) {
    $caller = Get-CanonicalRelaySecurity $PID
    if ([int]$caller.session_id -le 0) { throw 'Canonical relay requires an interactive user session.' }
    if ([bool]$caller.elevated) {
        # Reuse the same user's existing desktop token. Never fall back to the
        # installer token, another account, credentials, or a scheduled task.
        $newPid = Start-CanonicalLimitedRelay $Line ([string]$caller.sid) ([int]$caller.session_id)
    }
    else {
        $created = Invoke-CimMethod -ClassName Win32_Process -MethodName Create -Arguments @{CommandLine=$Line}
        if ([uint32]$created.ReturnValue -ne 0) { throw 'Rollback process start failed.' }
        $newPid = [int]$created.ProcessId
    }
    $security = Get-CanonicalRelaySecurity $newPid
    if ([bool]$security.elevated -or [int]$security.integrity_rid -ne 8192 -or [string]$security.sid -cne [string]$caller.sid -or
        [int]$security.session_id -ne [int]$caller.session_id) {
        throw 'Canonical relay normal-user security readback failed.'
    }
    return [int]$newPid
}

function Invoke-LabelRollbackStep([string]$Phase, [scriptblock]$Action) {
    # Dot-source this helper so successful restoration assignments remain in
    # the transaction scope. Each independent phase records its own failure.
    $phaseStack = Get-Variable -Name LabelRollbackPhaseStack -Scope Script -ErrorAction SilentlyContinue
    if ($null -eq $phaseStack -or $null -eq $phaseStack.Value) { $Script:LabelRollbackPhaseStack = [Collections.Generic.Stack[string]]::new() }
    $Script:LabelRollbackPhaseStack.Push($Phase)
    try {
        . $Action
        $Script:LabelRollbackSteps.Add([ordered]@{phase=$Script:LabelRollbackPhaseStack.Peek();status='PROVEN'})
    }
    catch {
        $Script:LabelRollbackErrors.Add($_)
        $failure = Get-LabelLaunchFailure $_ $Script:LabelRollbackPhaseStack.Peek()
        $Script:LabelRollbackFailures.Add($failure)
        $Script:LabelRollbackSteps.Add([ordered]@{phase=$Script:LabelRollbackPhaseStack.Peek();status='FAILED'})
    }
    finally { [void]$Script:LabelRollbackPhaseStack.Pop() }
}

function Get-LabelLaunchFailure($Failure, [string]$Phase) {
    $exception=$Failure.Exception; $childOutput=$null; $placementFailure=$null; $privateChain=@()
    while ($null -ne $exception) {
        if ($exception.Data.Contains('child_output')) { $childOutput=$exception.Data['child_output'] }
        if ($exception.Data.Contains('placement_child_failure')) { $placementFailure=$exception.Data['placement_child_failure'] }
        $privateData=[ordered]@{}
        foreach($key in @('call','placement_child_failure','label_legacy_native_failure','startup_restore_failures')) {
            if($exception.Data.Contains($key)) {$privateData[$key]=$exception.Data[$key]}
        }
        # Preserve a typed native call even when the free-text redactor treats
        # the colon in a Token API name as a secret assignment separator.
        if ($privateData.Contains('call') -and [string]$privateData.call -cmatch '\A([A-Za-z_][A-Za-z0-9_]{0,127}):([A-Za-z_0-9][A-Za-z0-9_]{0,127})\z') {
            $privateData.native_call_function=$Matches[1];$privateData.native_call_variant=$Matches[2]
        }
        $privateChain += [ordered]@{exception_type=$exception.GetType().FullName;native_error=$(if($exception -is [ComponentModel.Win32Exception]){$exception.NativeErrorCode}else{$null});details=$privateData}
        $exception=$exception.InnerException
    }
    $baseException=$Failure.Exception.GetBaseException()
    $locationFunction=$Phase;$locationLine=[int]$Failure.InvocationInfo.ScriptLineNumber
    if([string]$Failure.ScriptStackTrace -match '^at ([A-Za-z_][A-Za-z0-9_-]*), [^\r\n]+: line ([0-9]+)'){
        $locationFunction=$Matches[1];$locationLine=[int]$Matches[2]
    }
    $fields=[ordered]@{status='FAILED';stage='SUPPORT';reason_code='INSTALLER_FAILED';exception_type=$baseException.GetType().FullName;guidance=[string]$Script:KmtechRedactionRules.safe_messages[0];report_status='REPORT_UNAVAILABLE'}
    $fields.location_module='INSTALL_CANONICAL_PORTABLE.ps1';$fields.location_function=$locationFunction;$fields.location_line=$locationLine
    if ($null -ne $placementFailure) {
        $fields.placement_code=[string]$placementFailure.placement_code
        $fields.location_function=[string]$placementFailure.phase
        $placementTrace=$null
        if ($placementFailure -is [Collections.IDictionary]) { $placementTrace=$placementFailure['full_exception_trace'] }
        else {
            $traceProperty=$placementFailure.PSObject.Properties['full_exception_trace']
            if ($null -ne $traceProperty) { $placementTrace=$traceProperty.Value }
        }
        if ($null -ne $placementTrace) {
            $fields.Remove('report_status');$fields.report_path=[string]$placementTrace.path;$fields.report_sha256=[string]$placementTrace.sha256
        }
    }
    if ($null -ne $childOutput) {
        foreach ($key in $childOutput.Keys) { $fields[$key]=$childOutput[$key] }
        if ($fields.Contains('report_path') -and $fields.Contains('report_sha256')) {$fields.Remove('report_status')}
    }
    # Native failures retain their first cause in the existing private evidence.
    if (-not $fields.Contains('report_path')) {
        try {
            $private=[ordered]@{phase=$Phase;trace=$Failure.Exception.ToString();script_stack_trace=[string]$Failure.ScriptStackTrace;exception_chain=$privateChain}
            $reference=Write-KmtechProtectedFailureSidecar -Text ((ConvertTo-KmtechRedactedSinkObject $private) | ConvertTo-Json -Depth 20)
            $fields.Remove('report_status')
            $fields.report_path=[string]$reference.path; $fields.report_sha256=[string]$reference.sha256
        } catch { $fields.report_status='REPORT_UNAVAILABLE' }
    }
    return ConvertTo-KmtechPublicRecord $fields
}

function Wait-LabelRelayPreimageExit([object[]]$Expected) {
    $deadline=(Get-Date).AddSeconds(15)
    do {
        $remaining=@(UnquiescedProductWriters)
        foreach ($row in $remaining) {
            $matches=@($Expected | Where-Object {
                (Same ([string]$_.ExecutablePath) ([string]$row.ExecutablePath)) -and
                (Normalize-RelayExecutableQuoting ([string]$_.CommandLine) ([string]$_.ExecutablePath)) -ceq
                (Normalize-RelayExecutableQuoting ([string]$row.CommandLine) ([string]$row.ExecutablePath))
            })
            if ($matches.Count -ne 1) { throw 'Unexpected writer remains after normal removal.' }
        }
        if ($remaining.Count -eq 0) { return }
        Start-Sleep -Milliseconds 100
    } while ((Get-Date) -lt $deadline)
    throw 'Normal relay process exit remains unproven.'
}

function HealthyLifecycle([string]$Root, [string]$Installed, [string]$Mode = 'inspect', [string]$ExpectedStateSha256 = '') {
    # Only the attested candidate is imported. This path does not enroll, rotate
    # identities, reconcile authorities, or create conflict-resolution receipts.
    $probe = @'
import hashlib, json, pathlib, sys
root, installed = map(pathlib.Path, sys.argv[1:3])
mode = sys.argv[3]
expected = sys.argv[4] if len(sys.argv) > 4 else ''
sys.path[:0] = [str(root / 'app'), str(root / 'app/site-packages')]
import current_user_onboarding as onboarding
from label_exact_clone_resolution import client_authorities, read_bounded_json
from producer_runtime_client import _scope_values, _scope_key
from tools.register_label_match_worker_pc import _current_machine_guid, _current_user_sid, derive_path_independent_install_id
from user_relay import release_user_relay_stop_marker, user_relay_stop_path
from user_relay_stop_marker import read_stop_marker, _validated_node, canonical_marker_bytes
from writer_session_fence import writer_admission, active_fence

def inspect():
    paths = onboarding.resolve_current_user_onboarding_paths(installed)
    reference = read_bounded_json(paths.credential_path, label='producer credential reference')
    secret_ref = str(reference.get('secret_ref') or '')
    if (reference.get('secret') or not secret_ref.startswith('dpapi:') or
        pathlib.Path(reference.get('secret_data_dir') or paths.direct_sync_root).resolve() != paths.direct_sync_root):
        raise ValueError('healthy lifecycle requires canonical current-user DPAPI credentials')
    secret_name = secret_ref.split(':', 1)[1]
    if not secret_name or any(character in secret_name for character in '/\\:'):
        raise ValueError('healthy lifecycle credential reference is invalid')
    protected = [paths.identity_path, paths.producer_manifest_path, paths.credential_path,
                 paths.registration_report_path, paths.logistics_profile_path, paths.logistics_secret_path,
                 paths.direct_sync_root / 'secrets' / (secret_name + '.dpapi')]
    database = paths.direct_sync_root / 'queue/direct_sync_relay.sqlite3'
    for path in protected + [paths.direct_sync_root, paths.data_root, paths.settings_path, database]:
        for parent in (path, *path.parents):
            if parent.is_symlink() or (parent.exists() and parent.lstat().st_file_attributes & 0x400):
                raise ValueError('healthy lifecycle state contains a reparse point')
    state = onboarding.inspect_current_user_state(paths)
    if state['status'] != 'READY':
        raise ValueError('healthy lifecycle requires complete current-user identity and credential readback')
    identity = read_bounded_json(paths.identity_path, label='producer identity')
    local_install_id = derive_path_independent_install_id(machine_guid=_current_machine_guid(), user_sid=_current_user_sid())
    if identity['producer_install_id'] != local_install_id:
        raise ValueError('healthy lifecycle identity belongs to another machine or user')
    credential = onboarding.load_credentials_from_json(paths.credential_path)
    if credential.producer_id != identity['producer_id']:
        raise ValueError('healthy lifecycle credential identity differs')
    scope = _scope_values(credential, identity['producer_install_id'])
    if database.exists():
        for authority in client_authorities(database):
            if (authority['authority_scope'] != _scope_key(scope) or
                any(authority[name] != value for name, value in scope.items()) or
                authority['status'] not in {'ACTIVE', 'PENDING'} or authority['last_error_code']):
                raise ValueError('healthy lifecycle runtime authority is foreign, quarantined, or requires recovery')
    digest = hashlib.sha256()
    for path in protected:
        digest.update(str(path).encode('utf-8'))
        with path.open('rb') as stream:
            digest.update(hashlib.file_digest(stream, 'sha256').digest())
    state_hash = digest.hexdigest()
    if expected and state_hash != expected:
        raise ValueError('healthy lifecycle protected identity changed during replacement')
    result = dict(status='HEALTHY_CURRENT_USER', state_sha256=state_hash, marker_present=False)
    marker_path = user_relay_stop_path(paths.direct_sync_root)
    if marker_path.exists():
        marker, raw, marker_hash = read_stop_marker(marker_path)
        _validated_node(marker)
        if raw != canonical_marker_bytes(marker):
            raise ValueError('healthy lifecycle stop marker is not canonical')
        if mode in ('inspect_recovery', 'release_recovery') and expected:
            if marker_hash != expected:
                raise ValueError('recovery stop request differs')
            result.update(marker_present=True, request_id=marker['request_id'], marker_sha256=marker_hash)
            return paths, result
        removal = read_bounded_json(paths.removal_report_path, label='normal removal report')
        relay = removal.get('relay_process', {})
        if (removal.get('status') != 'PASS_DATA_PRESERVED' or removal.get('data_preserved') is not True or
            pathlib.Path(removal.get('machine_code_root', '')).resolve() != installed.resolve() or
            relay.get('status') != 'ABSENT' or relay.get('request_id') != marker['request_id'] or
            relay.get('stop_request_sha256') != marker_hash):
            raise ValueError('healthy lifecycle stop marker is not the exact normal removal marker')
        result.update(marker_present=True, request_id=marker['request_id'], marker_sha256=marker_hash)
    return paths, result

if mode in ('inspect', 'inspect_recovery'):
    _, result = inspect()
elif mode in ('release', 'release_recovery') and expected:
    # The canonical transaction still holds its active writer fence. Admission
    # validates the candidate identity and exact session delegation before the
    # marker is inspected or removed; ordinary onboarding retains its own guard.
    with writer_admission('user_relay_stop_release'):
        if active_fence() is None:
            raise ValueError('healthy lifecycle release requires an active replacement fence')
        paths, result = inspect()
        if not result['marker_present']:
            raise ValueError('normal removal marker disappeared before release')
        result['release'] = release_user_relay_stop_marker(paths.direct_sync_root,
            expected_request_id=result['request_id'], expected_sha256=result['marker_sha256'])
else:
    raise ValueError('unsupported healthy lifecycle operation')
print(json.dumps(result, sort_keys=True))
'@
    # LF-normalized UTF-8 is the fixed probe identity on PS5.1 and PS7.
    $probeBytes = [Text.Encoding]::UTF8.GetBytes($probe.Replace("`r`n", "`n"))
    $stdout = Invoke-LabelPinnedProbe -Python (Join-Path $Root 'runtime\python.exe') `
        -ProbeBytes $probeBytes -ExpectedSha256 '0b515260a4cf6dec9fdf8aaa935cfaaad2a0a5d92b0f4716c0e03c1ae96e8ff4' `
        -ProbeArguments @($Root, $Installed, $Mode, $ExpectedStateSha256) -FailureMessage 'Pinned conflict-resolution receipt source validation is required; healthy lifecycle validation failed: '
    return $stdout | ConvertFrom-Json
}

function ConvertTo-LabelTaskSid([string]$Identity) {
    if ([string]::IsNullOrWhiteSpace($Identity)) { throw 'TASK_USER_ID_UNRESOLVED' }
    if ($Identity -match '^S-1-') {
        return [Security.Principal.SecurityIdentifier]::new($Identity).Value
    }
    return [Security.Principal.NTAccount]::new($Identity).Translate([Security.Principal.SecurityIdentifier]).Value
}

function Assert-HealthyLifecycleOwnership($Run, $RelayValues, $Tasks, [string]$Installed) {
    if ($Run.exists -and ([string]$Run.kind -cne 'String' -or [string]$Run.data -cne (Command $Installed))) {
        throw 'Healthy lifecycle autostart belongs to another command.'
    }
    $relayRoot=RelayCodeRoot $Installed
    $userSid = [Security.Principal.WindowsIdentity]::GetCurrent().User.Value
    if (@($RelayValues).Count -gt 1) { throw 'Healthy lifecycle relay ownership is ambiguous.' }
    foreach ($relay in $RelayValues) {
        $owner = Invoke-CimMethod -InputObject $relay -MethodName GetOwnerSid -ErrorAction Stop
        if ($owner.ReturnValue -ne 0 -or [string]$owner.Sid -cne $userSid -or
            -not (Same ([string]$relay.ExecutablePath) (Join-Path $relayRoot 'runtime\pythonw.exe')) -or
            (Normalize-RelayExecutableQuoting ([string]$relay.CommandLine) (Join-Path $relayRoot 'runtime\pythonw.exe')) -cne (Command $relayRoot)) {
            throw 'Healthy lifecycle relay belongs to another owner or command.'
        }
    }
    foreach ($task in $Tasks) {
        $taskSid = ConvertTo-LabelTaskSid ([string]$task.Principal.UserId)
        if ($taskSid -cne $userSid -or [string]$task.Principal.RunLevel -cne 'Limited' -or
            [string]$task.Principal.LogonType -cne 'Interactive') {
            throw 'Healthy lifecycle task belongs to another principal.'
        }
        $actions = @($task.Actions)
        $expectedArguments = '-I -B ' + (Arg (Join-Path $Installed 'app\main.py')) +
            ' --label-match-scheduled-relay --app-root ' + (Arg $Installed) +
            ' --log-path ' + (Arg (Join-Path $defaultDirectSyncRoot 'logs\scheduled_direct_sync_relay.jsonl'))
        if ($actions.Count -ne 1 -or
            -not (Same ([string]$actions[0].Execute) (Join-Path $Installed 'runtime\python.exe')) -or
            [string]$actions[0].Arguments -cne $expectedArguments -or
            -not (Same ([string]$actions[0].WorkingDirectory) (Join-Path $Installed 'app'))) {
            throw 'Healthy lifecycle task belongs to another command.'
        }
    }
}

function Test-PristineInstallState(
    [string]$InstallRootValue,
    $RunSnapshotValue,
    [object[]]$RelaySnapshotValue,
    [object[]]$ScheduledTaskValues,
    [string[]]$ResiduePaths,
    [string]$BootstrapCaPath = "",
    [string]$PythonPath = "",
    [string]$AppRoot = ""
) {
    if (Test-Path -LiteralPath $InstallRootValue) { return $false }
    if ([bool]$RunSnapshotValue.exists) { return $false }
    if (@($RelaySnapshotValue).Count -ne 0) { return $false }
    if (@($ScheduledTaskValues).Count -ne 0) { return $false }
    # A public trust bundle is not producer registration. Admit only the
    # expected leaf in its otherwise empty directory, using the existing
    # bounded, certificate-only profile validator. Nothing is removed.
    $publicCaOnly = $false
    $caParent = ''
    if (-not [string]::IsNullOrWhiteSpace($BootstrapCaPath)) {
        $BootstrapCaPath = [IO.Path]::GetFullPath($BootstrapCaPath)
        $caParent = [IO.Path]::GetDirectoryName($BootstrapCaPath)
        $ancestor = $BootstrapCaPath
        while ($ancestor) {
            try { $item = Get-Item -LiteralPath $ancestor -Force -ErrorAction Stop }
            catch [Management.Automation.ItemNotFoundException] { $item = $null }
            if ($null -ne $item -and
                (($item.Attributes -band [IO.FileAttributes]::ReparsePoint) -ne 0)) {
                return $false
            }
            $ancestor = [IO.Path]::GetDirectoryName($ancestor)
        }
        if (Test-Path -LiteralPath $caParent) {
            if (-not (Test-Path -LiteralPath $caParent -PathType Container)) { return $false }
            $entries = @(Get-ChildItem -LiteralPath $caParent -Force -ErrorAction Stop)
            if ($entries.Count -ne 1 -or $entries[0].PSIsContainer -or
                -not [StringComparer]::OrdinalIgnoreCase.Equals($entries[0].FullName, $BootstrapCaPath) -or
                [string]::IsNullOrWhiteSpace($PythonPath) -or [string]::IsNullOrWhiteSpace($AppRoot)) {
                return $false
            }
            $certificateProbe = @'
import sys
sys.path.insert(0, sys.argv[1])
try:
    from tools.install_logistics_runtime_profile import _resolve_tls_ca_bundle
    _resolve_tls_ca_bundle(sys.argv[2], sys.argv[2])
except Exception:
    sys.exit(1)
'@
            $output = @(& $PythonPath -I -B -c $certificateProbe $AppRoot $BootstrapCaPath)
            if ($LASTEXITCODE -ne 0 -or $output.Count -ne 0) { return $false }
            $publicCaOnly = $true
        }
    }
    foreach ($path in $ResiduePaths) {
        if ([string]::IsNullOrWhiteSpace($path)) {
            throw 'Pristine-install residue path is empty.'
        }
        if ($publicCaOnly -and
            ([StringComparer]::OrdinalIgnoreCase.Equals([IO.Path]::GetFullPath($path), $BootstrapCaPath) -or
             [StringComparer]::OrdinalIgnoreCase.Equals([IO.Path]::GetFullPath($path), $caParent))) {
            continue
        }
        if (Test-Path -LiteralPath $path) { return $false }
    }
    return $true
}

function Test-LegacyFirstRegistrationState(
    [bool]$ExistingVerified,
    $WriterTransition,
    $RunSnapshotValue,
    [object[]]$RelaySnapshotValue,
    [object[]]$ScheduledTaskValues,
    [string[]]$ResiduePaths
) {
    # Only directions already admitted by Assert-WriterTransition qualify.
    # AST-equivalent/unknown code is not a pinned legacy release preimage.
    $pinnedDirections = @(
        'PINNED_DATA_GENERATION_G',
        'SIGNED_COMPATIBLE_DATA_GENERATION_REVERSE',
        'X13B_PINNED_SHARED_LEAF_ADOPTION',
        'PINNED_BOOTSTRAP_INTEGRITY_ORDER_FIX',
        'PINNED_LEGACY_LABEL_TRANSITION_MODE',
        'PINNED_SCAN_INPUT_FIX',
        'PINNED_W9NEXTFIX_LABEL_RECOVERY',
        'PINNED_CURRENT_USER_PROFILE_SELECTION',
        'PINNED_STARTUP_UPDATE_BOOTSTRAP'
    )
    if (-not $ExistingVerified -or
        [string]$WriterTransition.compatibility -cnotin $pinnedDirections -or
        [bool]$RunSnapshotValue.exists -or
        @($RelaySnapshotValue).Count -ne 0 -or @($ScheduledTaskValues).Count -ne 0) {
        return $false
    }
    foreach ($path in $ResiduePaths) {
        if ([string]::IsNullOrWhiteSpace($path)) { throw 'First-registration residue path is empty.' }
        # A directory at a credential filename, orphan secret/queue, or dangling
        # reparse point is partial state, never an absent credential.
        $ancestor = [IO.Path]::GetFullPath($path)
        while ($ancestor) {
            try { $item = Get-Item -LiteralPath $ancestor -Force -ErrorAction Stop }
            catch [Management.Automation.ItemNotFoundException] { $item = $null }
            if ($null -ne $item -and
                (($item.Attributes -band [IO.FileAttributes]::ReparsePoint) -ne 0)) {
                return $false
            }
            if ($ancestor -ceq [IO.Path]::GetFullPath($path) -and $null -ne $item) { return $false }
            $ancestor = [IO.Path]::GetDirectoryName($ancestor)
        }
    }
    return $true
}

function FirstRegistrationAbsence([string[]]$Paths) {
    foreach ($path in $Paths) {
        try {
            $item = Get-Item -LiteralPath $path -Force -ErrorAction Stop
            [ordered]@{path=$path; state='PRESENT'; kind=$(if ($item.PSIsContainer) { 'DIRECTORY' } else { 'FILE' })}
        }
        catch [Management.Automation.ItemNotFoundException] { [ordered]@{path=$path; state='ABSENT'} }
        catch { [ordered]@{path=$path; state='UNKNOWN'; failure_type=$_.Exception.GetType().Name} }
    }
}

function FirstRegistrationAbsentAncestors([string[]]$Paths) {
    $seen = [Collections.Generic.HashSet[string]]::new([StringComparer]::OrdinalIgnoreCase)
    foreach ($path in $Paths) {
        $parent = [IO.Path]::GetDirectoryName((Full $path 'first-registration path'))
        while ($parent) {
            try { [void](Get-Item -LiteralPath $parent -Force -ErrorAction Stop); break }
            catch [Management.Automation.ItemNotFoundException] {
                if ($seen.Add($parent)) { $parent }
            }
            $parent = [IO.Path]::GetDirectoryName($parent)
        }
    }
}

function Move-LegacyFirstRegistrationState(
    [string[]]$AbsentPaths, [string]$QuarantineRoot, [bool]$ServerRegistrationMayExist,
    [string[]]$DeferredPaths = @(), [string[]]$AbsentAncestors = @(), [string]$AuditRoot = ''
) {
    $quarantine = Full $QuarantineRoot 'first-registration quarantine'
    $resolved = @($AbsentPaths | ForEach-Object { Full $_ 'first-registration state preimage' } | Select-Object -Unique)
    foreach ($path in $resolved) {
        if ((Same $path $quarantine) -or
            $quarantine.StartsWith($path.TrimEnd('\') + '\', [StringComparison]::OrdinalIgnoreCase) -or
            $path.StartsWith($quarantine.TrimEnd('\') + '\', [StringComparison]::OrdinalIgnoreCase)) {
            throw 'First-registration quarantine overlaps live state.'
        }
    }
    foreach ($path in @($resolved) + @($quarantine)) {
        $ancestor = $path
        while ($ancestor) {
            try { $item = Get-Item -LiteralPath $ancestor -Force -ErrorAction Stop }
            catch [Management.Automation.ItemNotFoundException] { $item = $null }
            if ($null -ne $item -and (($item.Attributes -band [IO.FileAttributes]::ReparsePoint) -ne 0)) {
                throw 'First-registration rollback state contains a reparse point.'
            }
            $ancestor = [IO.Path]::GetDirectoryName($ancestor)
        }
    }
    $progressPath = Join-Path $quarantine 'state-moves.json'
    $initializeProgress = -not (Test-Path -LiteralPath $quarantine)
    if (Test-Path -LiteralPath $quarantine) {
        if (-not (Test-Path -LiteralPath $progressPath -PathType Leaf) -and
            @(Get-ChildItem -LiteralPath $quarantine -Force).Count -eq 0) {
            $initializeProgress = $true
        }
        elseif (-not (Test-Path -LiteralPath $progressPath -PathType Leaf) -or
            (Get-Item -LiteralPath $progressPath).Length -gt 65536) {
            throw 'First-registration quarantine progress is absent or oversized.'
        }
        if (-not $initializeProgress) {
            $previous = Get-Content -LiteralPath $progressPath -Raw -Encoding UTF8 | ConvertFrom-Json
            if ([string]$previous.schema -cne 'label-match-first-registration-quarantine-v1' -or
                -not (Same ([string]$previous.root) $quarantine) -or
                (@($previous.state_paths | ForEach-Object { $_.original_path }) -join "`n") -cne ($resolved -join "`n")) {
                throw 'First-registration quarantine belongs to another attempt.'
            }
        }
    }
    if ($initializeProgress) {
        if (-not (Test-Path -LiteralPath $quarantine)) { [void](New-Item -ItemType Directory -Path $quarantine -ErrorAction Stop) }
        $initialMap = @()
        for ($index = 0; $index -lt $resolved.Count; $index++) {
            $initialMap += [ordered]@{original_path=$resolved[$index]; quarantined_path=(Join-Path $quarantine ('state-' + $index)); preimage='ABSENT'; status='PENDING'; attempts=0; failure_type=''}
        }
        Save-LabelMachineMetadata $progressPath ([ordered]@{schema='label-match-first-registration-quarantine-v1'; root=$quarantine; state_paths=$initialMap; restored_absent=$false})
    }
    $moves = @()
    for ($index = 0; $index -lt $resolved.Count; $index++) {
        $path = $resolved[$index]
        $destination = Join-Path $quarantine ('state-' + $index)
        $present = Test-Path -LiteralPath $path
        $history = @(Get-ChildItem -LiteralPath $quarantine -Force | Where-Object {
            $_.Name -cmatch ('^state-' + $index + '(-[0-9a-f]{32})?$')
        } | ForEach-Object { $_.FullName })
        if ($present -and (Test-Path -LiteralPath $destination) -and $DeferredPaths -notcontains $path) {
            # A resumed removal can create its own new status/fence directories.
            # Append a new preserved copy; never overwrite an earlier identity.
            $destination += '-' + [Guid]::NewGuid().ToString('N')
        }
        elseif (-not $present -and $history.Count -ne 0) { $destination = $history[-1] }
        $entry = [ordered]@{original_path=$path; quarantined_path=$destination; quarantined_paths=$history; preimage='ABSENT'; status='ABSENT'; attempts=0; failure_type=''}
        if ($DeferredPaths -contains $path) { $entry.status = 'DEFERRED' }
        elseif ($present) {
            $entry.status = 'FAILED'
            for ($attempt = 1; $attempt -le 3; $attempt++) {
                $entry.attempts = $attempt
                try {
                    if (Test-Path -LiteralPath $destination) { throw 'Quarantine destination is occupied; originals preserved.' }
                    Move-Item -LiteralPath $path -Destination $destination -ErrorAction Stop
                    $entry.status = 'QUARANTINED'
                    $entry.quarantined_paths += $destination
                    $entry.failure_type = ''
                    break
                }
                catch { $entry.failure_type = $_.Exception.GetType().Name }
                if ($attempt -lt 3) { Start-Sleep -Milliseconds 100 }
            }
        }
        elseif (Test-Path -LiteralPath $destination) { $entry.status = 'QUARANTINED' }
        $moves += $entry
        # Keep the complete path map on every save so a stopped/failed move can
        # be resumed by the same command without renumbering destinations.
        $map = @($moves)
        for ($pending = $index + 1; $pending -lt $resolved.Count; $pending++) {
            $map += [ordered]@{original_path=$resolved[$pending]; quarantined_path=(Join-Path $quarantine ('state-' + $pending)); preimage='ABSENT'; status='PENDING'; attempts=0; failure_type=''}
        }
        Save-LabelMachineMetadata $progressPath ([ordered]@{schema='label-match-first-registration-quarantine-v1'; root=$quarantine; state_paths=$map; restored_absent=$false})
    }
    $ancestorMoves = @()
    if ($DeferredPaths.Count -eq 0) {
        foreach ($parent in @($AbsentAncestors | Sort-Object Length -Descending)) {
            $entry = [ordered]@{path=$parent; state='ABSENT'}
            if (Test-Path -LiteralPath $parent) {
                if ($AuditRoot -and ((Same $parent $AuditRoot) -or
                    $AuditRoot.StartsWith($parent.TrimEnd('\') + '\', [StringComparison]::OrdinalIgnoreCase))) {
                    $entry.state = 'RETAINED_FOR_ATTEMPT_AUDIT'
                }
                elseif (@(Get-ChildItem -LiteralPath $parent -Force -ErrorAction Stop).Count -eq 0) {
                    $destination = Join-Path $quarantine ('ancestor-' + $ancestorMoves.Count)
                    $entry.state = 'PRESENT'
                    for ($attempt = 1; $attempt -le 3; $attempt++) {
                        try { Move-Item -LiteralPath $parent -Destination $destination -ErrorAction Stop; $entry.state='ABSENT'; break }
                        catch { $entry.failure_type=$_.Exception.GetType().Name }
                        if ($attempt -lt 3) { Start-Sleep -Milliseconds 100 }
                    }
                }
                else { $entry.state = 'PRESENT_UNOWNED_CONTENT_PRESERVED' }
            }
            $ancestorMoves += $entry
        }
    }
    $readback = @(FirstRegistrationAbsence @($resolved | Where-Object { $DeferredPaths -notcontains $_ }))
    $remaining = @($readback | Where-Object { $_.state -cne 'ABSENT' })
    $remaining += @($ancestorMoves | Where-Object { $_.state -cnotin @('ABSENT', 'RETAINED_FOR_ATTEMPT_AUDIT') })
    $result = [ordered]@{schema='label-match-first-registration-quarantine-v1'; root=$quarantine; state_paths=$moves; ancestors=$ancestorMoves; absence_readback=$readback; remaining_paths=$remaining; restored_absent=($remaining.Count -eq 0); server_registration_may_exist=$ServerRegistrationMayExist; server_rollback_performed=$false}
    Save-LabelMachineMetadata $progressPath $result
    return $result
}

function Read-FirstRegistrationAttempt(
    [string]$AuditRoot, [string]$Install, [string]$SourceCommit, [string]$InstallerSha256,
    [string[]]$ExpectedPaths
) {
    if (-not (Test-Path -LiteralPath $AuditRoot)) { return $null }
    $pending = @()
    foreach ($file in @(Get-ChildItem -LiteralPath $AuditRoot -Filter 'canonical-portable-*-first-registration-attempt.json' -File)) {
        if (($file.Attributes -band [IO.FileAttributes]::ReparsePoint) -ne 0 -or $file.Length -gt 65536) {
            throw 'First-registration attempt record is unsafe or oversized.'
        }
        $record = Get-Content -LiteralPath $file.FullName -Raw -Encoding UTF8 | ConvertFrom-Json
        if ([string]$record.status -cin @('PASS', 'FAILED_ROLLED_BACK')) { continue }
        [void](Get-RequiredExternalBoolean $record 'state_verified')
        [void](Get-RequiredExternalBoolean $record 'registration_started')
        if ([string]$record.schema -cne 'label-match-first-registration-attempt-v1' -or
            [string]$record.status -cnotin @('PREFLIGHT_RECORDED', 'RUNNING', 'FAILED_ROLLBACK_INCOMPLETE') -or
            [string]$record.run_id -cnotmatch '^\d{8}T\d{9}Z-[0-9a-f]{32}$' -or
            $file.Name -cne ('canonical-portable-' + [string]$record.run_id + '-first-registration-attempt.json') -or
            -not (Same ([string]$record.install_root) $Install) -or
            [string]$record.source_commit -cne $SourceCommit -or
            [string]$record.orchestrator_sha256 -cne $InstallerSha256 -or
            -not (Same ([string]$record.quarantine_root) (Join-Path $AuditRoot ('canonical-portable-' + [string]$record.run_id + '-first-registration-quarantine'))) -or
            [string]$record.writer_session_id -cnotmatch '^[0-9a-f]{32}$' -or
            [string]$record.writer_attempt_id -cnotmatch '^[0-9a-f]{32}$' -or
            [string]$record.writer_transaction_id -cnotmatch '^[0-9a-f]{32}$' -or
            (@($record.absent_paths) -join "`n") -cne ($ExpectedPaths -join "`n")) {
            throw 'Pending first-registration cleanup requires the exact original installer and paths.'
        }
        foreach ($parent in @($record.absent_ancestors)) {
            $matches = @($ExpectedPaths | Where-Object {
                $_.StartsWith(([string]$parent).TrimEnd('\') + '\', [StringComparison]::OrdinalIgnoreCase)
            })
            $userRoot = Full ([string]$env:LOCALAPPDATA) 'current-user state boundary'
            if ($matches.Count -eq 0 -or -not [IO.Path]::IsPathRooted([string]$parent) -or
                -not ([string]$parent).StartsWith($userRoot.TrimEnd('\') + '\', [StringComparison]::OrdinalIgnoreCase)) {
                throw 'First-registration absent ancestor is outside its state paths.'
            }
        }
        foreach ($path in @($ExpectedPaths) + @($AuditRoot, [string]$record.quarantine_root)) {
            $ancestor = Full $path 'first-registration recovery path'
            while ($ancestor) {
                try { $item = Get-Item -LiteralPath $ancestor -Force -ErrorAction Stop }
                catch [Management.Automation.ItemNotFoundException] { $item = $null }
                if ($null -ne $item -and (($item.Attributes -band [IO.FileAttributes]::ReparsePoint) -ne 0)) {
                    throw 'First-registration recovery path contains a reparse point.'
                }
                $ancestor = [IO.Path]::GetDirectoryName($ancestor)
            }
        }
        $record.PSObject.Properties.Remove('record_path')
        $record | Add-Member -NotePropertyName record_path -NotePropertyValue $file.FullName
        $pending += $record
    }
    if ($pending.Count -gt 1) { throw 'Multiple pending first-registration attempts require supported recovery.' }
    if ($pending.Count -eq 1) { return $pending[0] }
    return $null
}

function Save-FirstRegistrationAttempt($Record, [string]$Path) {
    if ($null -ne $Record) { Save-LabelMachineMetadata $Path $Record }
}

function Save-LabelPlacementRecoveryPreimages {
    # Dot-source in the transaction scope. A rejected result can follow real
    # protected mutations, so capture actual verified recovery inputs on errors
    # as well as on a returned native exit code.
    $startupPreimage = Join-Path (Split-Path -Parent $install) ('.startup-update-install.' + $writerAttemptId)
    if (Test-Path -LiteralPath (Join-Path $startupPreimage 'preimage.json')) {
        $audit.startup_update_preimage = [ordered]@{path=$startupPreimage;sha256=(Sha (Join-Path $startupPreimage 'preimage.json'))}
    }
    if ($existingVerified -and $null -eq $startupLifecycle -and [string]::IsNullOrEmpty($replacementRollbackRoot)) {
        $replacementRollbackRoot = ReplacementPreimage $install $installedPreimageInventory $replacementBackupsBefore
        if ($legacyFirstRegistration) {
            $firstRegistrationAttempt.code_backup_root = $replacementRollbackRoot
            Save-FirstRegistrationAttempt $firstRegistrationAttempt $firstRegistrationAttemptPath
        }
    }
}

function Save-LabelRollbackContext {
    # Only restoration inputs are persisted; delegation is regenerated on resume.
    $values = [ordered]@{}
    foreach ($name in @('before','old','taskBefore','stopBefore','healthyRemovalReportBefore',
        'removalReportExisted','existingVerified','installedPreimageInventory','replacementRollbackRoot',
        'receiptSource','writerTransition','legacyFirstRegistration','healthyRemovedInstall',
        'firstRegistrationStateVerified','firstRegistrationStarted','firstRegistrationAttempt',
        'firstRegistrationAttemptPath','firstRegistrationStatePaths','firstRegistrationAbsentAncestors',
        'rollbackSources','restoreSources')) {
        $variable = Get-Variable -Name $name -Scope 1 -ErrorAction SilentlyContinue
        $values[$name] = if ($null -ne $variable) { $variable.Value } else { $null }
    }
    $audit.recovery_context = [ordered]@{operator_sid=$generationSubjectSid;source_root=$source;
        orchestrator_sha256=$writerOrchestratorSha256;writer_contract_sha256=$writerContractSha256;
        generation_marker_paths=@($generationMarkerPaths);values=$values}
}

function Invoke-LabelGenerationRefusalRecovery {
    $audit.rollback.generation_refused = $true
    $audit.rollback.guidance = '새 판이 이 PC 자료를 열어 옛 판으로 되돌릴 수 없습니다. 새 판과 자료를 보존합니다. 같은 새 판을 다시 시작하거나 호환 수정판을 설치하세요. 관리자 지원 재개: INSTALL_CANONICAL_PORTABLE.ps1 -ResumeRollbackAuditPath <감사 파일> -ExpectedRollbackAuditSha256 <감사 SHA256>.'
    $newVersionProven = $false
    . Invoke-LabelRollbackStep 'retained_candidate_readback' {
        if (-not $rollbackCustodyReady -or -not $rollbackProtectionReady) { throw 'Retained candidate protection is unproven.' }
        if ($audit.Contains('legacy_relay_task_preimage')) {
            $legacy=Get-LabelOfficialLegacyTaskSnapshot
            if ($null -eq $legacy -or $legacy.enabled) { throw 'Legacy relay stop is unproven.' }
        }
        $manifest = Manifest $install $SkipSignatureValidationForTest
        [void](Assert-LabelDataDowngrade -MarkerPaths $generationMarkerPaths -SubjectSid $generationSubjectSid `
            -TargetVersion (Get-LabelPortableDataVersion $install $manifest) -TargetBuildId ([string]$manifest.source_commit))
        InvokeFrozenIntegrityProbe $frozenPlacement $install
        if ([string](PortableInventory $install).sha256 -cne [string]$receiptSource.sha256) { throw 'Retained candidate code differs.' }
        $stopProof = $audit.rollback['new_relay_stop']
        $mode = if ($null -ne $stopProof) { 'inspect_recovery' } else { 'inspect' }
        $stopDigest = if ($null -ne $stopProof) { [string]$stopProof.stop_request_sha256 } else { '' }
        $state = HealthyLifecycle $source $install $mode $stopDigest
        if ([string]$state.status -cne 'HEALTHY_CURRENT_USER' -or ($state.marker_present -and $null -eq $stopProof)) { throw 'Retained candidate identity/stop state is unproven.' }
        $relay = Get-Content -LiteralPath $relayPath -Raw -Encoding UTF8 | ConvertFrom-Json
        if ([string]$relay.status -cne 'READY' -or -not (Test-RelayPersistentRetry $relay)) { throw 'Retained candidate relay readiness is unproven.' }
        $run = Snapshot
        if (-not $run.exists -or [string]$run.data -cne $wanted) { throw 'Retained candidate autostart differs.' }
        $newVersionProven = $true
    }
    if ($newVersionProven) {
        . Invoke-LabelRollbackStep 'retained_candidate_fence_release' {
            $stopProof = $audit.rollback['new_relay_stop']
            if ($null -ne $stopProof) {
                [void](Set-LabelWriterFenceDelegation -ControlRoot $writerFenceControlRoot -Status 'RESTORING' -SessionId $writerSessionId -AttemptId $writerAttemptId -ReplacementTransactionId $writerTransactionId -DelegationToken $writerDelegationToken -DelegatedSources @('user_relay_stop_release') -LifetimeSeconds 600)
                [void](HealthyLifecycle $source $install 'release_recovery' ([string]$stopProof.stop_request_sha256))
            }
            if ($writerFenceStarted) {
                [void](Stop-LabelWriterFence -ControlRoot $writerFenceControlRoot -SessionId $writerSessionId `
                    -AttemptId $writerAttemptId -ReplacementTransactionId $writerTransactionId -TimeoutMilliseconds 90000)
                $writerFenceStarted = $false
                $audit.writer_fence.status = 'RELEASED_FOR_RETAINED_CANDIDATE'
            }
            $audit.rollback.retained_candidate_ready = $true
        }
    } else {
        # Never reinstate an old launcher after G refuses. Remove only the exact
        # candidate Run command; a foreign persistence entry is retained/reported.
        . Invoke-LabelRollbackStep 'retained_candidate_autostart_stop' {
            $run = Snapshot
            if ($run.exists -and [string]$run.data -cne $wanted) { throw 'Autostart ownership is unproven; administrator recovery required.' }
            if ($run.exists) { Restore ([pscustomobject]@{exists=$false;kind='';data=''}) }
            if ((Snapshot).exists) { throw 'Candidate autostart absence is unproven.' }
            $audit.rollback.retained_candidate_ready = $false
        }
    }
}

function Invoke-LabelResumeRollback {
    if ($PlanOnly -or [string]::IsNullOrWhiteSpace($ExpectedRollbackAuditSha256)) { throw 'Supported rollback resume requires the exact audit SHA256.' }
    if (-not $testMode -and -not ([Security.Principal.WindowsPrincipal]::new([Security.Principal.WindowsIdentity]::GetCurrent())).IsInRole([Security.Principal.WindowsBuiltInRole]::Administrator)) {
        throw '지원 복구는 같은 작업 사용자의 관리자 PowerShell에서 실행하세요.'
    }
    $localAuditRoot = Join-Path $lad 'KMTech\Label_Match\install-audit'
    $auditPath = Full $ResumeRollbackAuditPath 'rollback audit'
    if (-not (Same (Split-Path -Parent $auditPath) $localAuditRoot)) { throw 'Rollback audit is outside the original operator audit root.' }
    $item = Get-Item -LiteralPath $auditPath
    if (($item.Attributes -band [IO.FileAttributes]::ReparsePoint) -or $item.Length -gt 1048576 -or
        $ExpectedRollbackAuditSha256 -cnotmatch '^[0-9a-f]{64}$' -or (Sha $auditPath) -cne $ExpectedRollbackAuditSha256) { throw 'Rollback audit pin/ordinary-file check failed.' }
    $saved = Get-Content -LiteralPath $auditPath -Raw -Encoding UTF8 | ConvertFrom-Json
    if ([string]$saved.schema -cne 'label-match-canonical-portable-install-v1' -or
        [string]$saved.status -cnotin @('ROLLBACK_FAILED','FAILED_ROLLBACK_INCOMPLETE','FAILED_ROLLED_BACK') -or
        -not (Same ([string]$saved.install_root) $install) -or [string]$saved.source_commit -cne [string]$sourceManifest.source_commit) { throw 'Rollback audit identity/status differs.' }
    $context = $saved.recovery_context
    if ($null -eq $context -or [string]$context.operator_sid -cne $generationSubjectSid -or
        [string]$context.orchestrator_sha256 -cne (Sha (Join-Path $source 'INSTALL_CANONICAL_PORTABLE.ps1'))) { throw 'Rollback context belongs to a different user or installer.' }
    $audit = [ordered]@{}
    foreach ($property in $saved.PSObject.Properties) { $audit[$property.Name] = $property.Value }
    foreach ($name in @('rollback','writer_fence')) {
        $map = [ordered]@{}
        foreach ($property in $audit[$name].PSObject.Properties) { $map[$property.Name] = $property.Value }
        $audit[$name] = $map
    }
    foreach ($name in @('before','old','taskBefore','stopBefore','healthyRemovalReportBefore',
        'removalReportExisted','existingVerified','installedPreimageInventory','replacementRollbackRoot',
        'writerTransition','legacyFirstRegistration','healthyRemovedInstall','firstRegistrationStateVerified',
        'firstRegistrationStarted','firstRegistrationAttempt','firstRegistrationAttemptPath',
        'firstRegistrationStatePaths','firstRegistrationAbsentAncestors','rollbackSources','restoreSources')) {
        if ($null -eq $context.values.PSObject.Properties[$name]) { throw 'Rollback context input is absent.' }
        Set-Variable -Name $name -Value $context.values.$name
    }
    $old = @($old)
    $generationMarkerPaths = @(@($generationMarkerPaths) + @($context.generation_marker_paths) | Select-Object -Unique)
    $receiptSource = ReceiptSource $source $sourceManifest
    $runId = [string]$audit.run_id
    $elevationLogPath = [string]$audit.elevation_log_path
    $recoveryRunId = $runId + '-resume-' + [Guid]::NewGuid().ToString('N')
    $frozenPlacement = FreezePlacementHelper $source $localAuditRoot $recoveryRunId $receiptSource.critical_file_sha256 $source
    . ([string]$frozenPlacement.writer_fence_path)
    $writerOrchestratorSha256 = [string]$context.orchestrator_sha256
    $writerContractSha256 = [string]$context.writer_contract_sha256
    if ($writerContractSha256 -cne [string]$frozenPlacement.writer_fence_sha256) { throw 'Recovery writer contract differs.' }
    $writerSessionId = [string]$audit.writer_fence.session_id
    $writerAttemptId = [string]$audit.writer_fence.attempt_id
    $writerTransactionId = [string]$audit.writer_fence.replacement_transaction_id
    $writerStartedAt = [string]$audit.writer_fence.session_started_at_utc
    $writerDelegationToken = [Guid]::NewGuid().ToString('N') + [Guid]::NewGuid().ToString('N')
    $Script:LabelWriterFenceInstalledIdentity = [pscustomobject]@{session_id=$writerSessionId;attempt_id=$writerAttemptId;
        replacement_transaction_id=$writerTransactionId;orchestrator_sha256=$writerOrchestratorSha256;
        writer_contract_sha256=$writerContractSha256;inventory_sha256=[string]$writerTransition.installed_inventory_sha256}
    $writerEnvironmentBefore = @($WriterDelegationEnvironmentNames | ForEach-Object { [Environment]::GetEnvironmentVariable($_, 'Process') })
    $writerAuthority = Enter-LabelWriterSessionAuthority -SessionId $writerSessionId -AttemptId $writerAttemptId `
        -OrchestratorSha256 $writerOrchestratorSha256 -ReplacementTransactionId $writerTransactionId -WriterContractSha256 $writerContractSha256
    try {
        $active = Read-LabelWriterFence $writerFenceControlRoot -AllowAbsent
        $writerFenceStarted = $null -ne $active
        if ($writerFenceStarted -and ([string]$active.session_id -cne $writerSessionId -or [string]$active.attempt_id -cne $writerAttemptId -or
            [string]$active.replacement_transaction_id -cne $writerTransactionId -or [string]$active.orchestrator_sha256 -cne $writerOrchestratorSha256 -or
            [string]$active.writer_contract_sha256 -cne $writerContractSha256)) { throw 'Held fence is not this recorded transaction.' }
        $helperParameters = @{InstallerRunId=$runId;InstallerOwnerProcessId=[int]$PID;
            InstallerOwnerProcessStartedAtFileTime=[Diagnostics.Process]::GetCurrentProcess().StartTime.ToUniversalTime().ToFileTimeUtc().ToString();
            SourceRoot=$source;InstallRoot=$install;ElevationLogPath=$elevationLogPath;
            ExpectedBootstrapScriptSha256=[string]$frozenPlacement.helper_sha256;VerifiedBootstrapScriptPath=[string]$frozenPlacement.helper_path;
            BootstrapIntegrityPreloaded=$true;WriterFenceFunctionsPreloaded=$true;WriterFenceControlRoot=$writerFenceControlRoot;
            DataGenerationSubjectSid=$generationSubjectSid;DataGenerationMarkerPaths=@($generationMarkerPaths);
            WriterFenceSessionId=$writerSessionId;WriterFenceAttemptId=$writerAttemptId;WriterFenceReplacementTransactionId=$writerTransactionId;
            WriterFenceDelegationToken=$writerDelegationToken;AllowNoncanonicalLayoutForTest=[bool]$testMode;ReplaceExistingVerifiedPortable=[bool]$existingVerified;DryRun=$false;
            ExpectedSourceAggregateSha256=[string]$receiptSource.bootstrap_aggregate_sha256;
            ExpectedSourceFileCount=[int]$receiptSource.file_count;ExpectedSourceByteCount=[uint64]$receiptSource.byte_count}
        . Invoke-LabelCanonicalRollback
        $audit.status = 'FAILED_ROLLED_BACK'
        $audit.rollback.supported_resume = 'PROVEN'
    } catch {
        $audit.status = 'ROLLBACK_FAILED'
        $audit.rollback.resume_failure = Get-LabelLaunchFailure $_ 'supported_resume'
        throw
    } finally {
        try {
            Save $auditPath $audit
            if ($EvidencePath) { Save (Full $EvidencePath 'EvidencePath') $audit }
        } finally {
            RestoreWriterDelegationEnvironment $writerEnvironmentBefore
            if ($null -ne $writerAuthority) { Exit-LabelWriterSessionAuthority $writerAuthority }
            $Script:LabelWriterFenceInstalledIdentity = $null
        }
    }
}

function Stop-LabelRollbackCandidateRelay {
    $audit.rollback.new_relay_stop = $null
    $live = @(Relays)
    if ($live.Count -eq 0) { return }
    foreach ($row in $live) {
        $security = Get-CanonicalRelaySecurity ([int]$row.ProcessId)
        if ([string]$security.sid -cne $generationSubjectSid) { throw 'Candidate relay owner is unproven.' }
    }
    $probe = @'
import json, pathlib, sys
root, state = map(pathlib.Path, sys.argv[1:3])
sys.path[:0] = [str(root / 'app'), str(root / 'app/site-packages')]
from user_relay import request_user_relay_stop
proof = request_user_relay_stop(state, timeout_seconds=15.0)
if proof['status'] != 'ABSENT':
    raise RuntimeError('candidate relay did not stop normally')
print(json.dumps(proof, sort_keys=True))
'@
    $bytes = [Text.Encoding]::UTF8.GetBytes($probe.Replace("`r`n","`n"))
    $output = Invoke-LabelPinnedProbe -Python (Join-Path $source 'runtime/python.exe') -ProbeBytes $bytes `
        -ExpectedSha256 'b0944e7a09135c3079da4c115fea3609434841a94739fa5615ab88e6254d4529' -ProbeArguments @($source,$defaultDirectSyncRoot) -FailureMessage 'Candidate relay protection failed: '
    $audit.rollback.new_relay_stop = $output | ConvertFrom-Json
    if (@(Relays).Count -ne 0) { throw 'Candidate relay process absence is unproven.' }
}

function Invoke-LabelCanonicalRollback {
        $audit.rollback.runtime_restored = $false
        $Script:LabelRollbackPhaseStack = [Collections.Generic.Stack[string]]::new()
        $Script:LabelRollbackErrors = [Collections.Generic.List[object]]::new()
        $Script:LabelRollbackFailures = [Collections.Generic.List[object]]::new()
        $Script:LabelRollbackSteps = [Collections.Generic.List[object]]::new()
        $rollbackCustodyReady = $false
        $rollbackQuiescent = $false
        $rollbackAuthenticationReady = -not $legacyFirstRegistration
        $rollbackUsesInstalled = $false
        $rollbackGenerationReady = $false
        $rollbackPreimageProven = $false
        $rollbackStartupProven = $false
        $rollbackRuntimeProven = $false
        $rollbackProtectionReady = $false
        . Invoke-LabelRollbackStep 'deployment_custody' {
            if (-not $writerFenceStarted) {
                # Final audit persistence can fail after the success path released
                # its fence. Reacquire the same session before stopping/restoring
                # any runtime or code; all existing phase checks still apply.
                if ($null -eq $writerAuthority) {
                    $writerAuthority = Enter-LabelWriterSessionAuthority `
                        -SessionId $writerSessionId `
                        -AttemptId $writerAttemptId `
                        -OrchestratorSha256 $writerOrchestratorSha256 `
                        -ReplacementTransactionId $writerTransactionId `
                        -WriterContractSha256 $writerContractSha256
                }
                [void](Start-LabelWriterFence `
                    -ControlRoot $writerFenceControlRoot `
                    -Status 'RESTORING' `
                    -SessionId $writerSessionId `
                    -AttemptId $writerAttemptId `
                    -ReplacementTransactionId $writerTransactionId `
                    -SessionStartedAtUtc $writerStartedAt `
                    -OrchestratorSha256 $writerOrchestratorSha256 `
                    -WriterContractSha256 $writerContractSha256 `
                    -AuthorityOwnedByCaller)
                $writerFenceStarted = $true
            }
            if ($writerFenceStarted) {
                [void](Set-LabelWriterFenceDelegation `
                    -ControlRoot $writerFenceControlRoot `
                    -Status 'RESTORING' `
                    -SessionId $writerSessionId `
                    -AttemptId $writerAttemptId `
                    -ReplacementTransactionId $writerTransactionId `
                    -DelegationToken $writerDelegationToken `
                    -DelegatedSources $rollbackSources `
                    -LifetimeSeconds 600)
                SetWriterDelegationEnvironment `
                    $writerDelegationToken `
                    $writerSessionId `
                    $writerAttemptId `
                    $writerTransactionId
            }
            $rollbackCustodyReady = $true
        }
        if ($rollbackCustodyReady) {
            . Invoke-LabelRollbackStep 'candidate_relay_stop' {
                Stop-LabelRollbackCandidateRelay
                $rollbackProtectionReady = $true
            }
        }
        . Invoke-LabelRollbackStep 'generation_before' {
            Assert-LabelRollbackGeneration
            $rollbackGenerationReady = $true
        }
        if ($rollbackGenerationReady -and $rollbackCustodyReady) {
            $rollbackGenerationReady = $false
            . Invoke-LabelRollbackStep 'generation_after' {
                Assert-LabelRollbackGeneration
                $rollbackGenerationReady = $true
            }
        }
        if ($rollbackGenerationReady -and $rollbackCustodyReady) {
            . Invoke-LabelRollbackStep 'current_user_removal' {
            $rollbackProductRoot = if (Test-Path (Join-Path $install 'runtime\pythonw.exe')) {
                $install
            }
            else { $source }
            $rollbackUsesInstalled = $false
            if (Same $rollbackProductRoot $install) {
                InvokeFrozenIntegrityProbe $frozenPlacement $install
                $rollbackInventory = PortableInventory $install
                if ($existingVerified -and [string]$rollbackInventory.sha256 -ceq [string]$installedPreimageInventory.sha256) {
                    $rollbackUsesInstalled = $true
                }
                elseif ([string]$rollbackInventory.sha256 -cne [string]$receiptSource.sha256) {
                    throw 'Rollback tree differs from both validated identities.'
                }
            }
            if ($writerFenceStarted) {
                [void](Set-LabelWriterFenceInstalledTree $writerFenceControlRoot $writerSessionId $writerAttemptId $writerTransactionId $rollbackUsesInstalled)
            }
            if (-not ($healthyRemovedInstall -and -not (Test-Path -LiteralPath $install))) {
                # No new setup has run while the exact legacy preimage remains
                # installed. Do not ask that old code to create removal state.
                if (-not ($legacyFirstRegistration -and $rollbackUsesInstalled)) {
                    Product $rollbackProductRoot '--remove-current-user-setup' -LogicalAppRoot $install
                }
            }
            }
            . Invoke-LabelRollbackStep 'writer_absence' {
                [void](Assert-RollbackRelayPreimage -ExpectedRelays @())
                $rollbackQuiescent = $true
            }
            if ($rollbackQuiescent) {
                . Invoke-LabelRollbackStep 'registration_preimage' {
            if ($legacyFirstRegistration) {
                # Detach authentication before restoring legacy code. Keep its
                # own active fence until the guarded placement helper completes.
                $firstRegistrationAttempt.status = 'FAILED_ROLLBACK_INCOMPLETE'
                Save-FirstRegistrationAttempt $firstRegistrationAttempt $firstRegistrationAttemptPath
                $stateMutationLease = Enter-LabelWriterAdmission -ControlRoot $writerFenceControlRoot -TimeoutMilliseconds 90000
                try {
                    $authPaths = @($firstRegistrationStatePaths | Where-Object { -not (Same $_ $defaultDirectSyncRoot) })
                    if (-not $firstRegistrationStateVerified -and
                        @(FirstRegistrationAbsence $authPaths | Where-Object { $_.state -cne 'ABSENT' }).Count -ne 0) {
                        throw 'Unowned state appeared before the first-registration recheck; preserved.'
                    }
                    $audit.rollback.first_registration_state = Move-LegacyFirstRegistrationState `
                        $firstRegistrationStatePaths ([string]$firstRegistrationAttempt.quarantine_root) `
                        $firstRegistrationStarted @($defaultDirectSyncRoot) $firstRegistrationAbsentAncestors $localAuditRoot
                    if (-not $audit.rollback.first_registration_state.restored_absent) {
                        throw 'First-registration authentication cleanup is incomplete; legacy code remains fenced.'
                    }
                }
                finally { Exit-LabelWriterAdmission $stateMutationLease }
            }
                    $rollbackAuthenticationReady = $true
                }
                if ($rollbackAuthenticationReady) {
                    . Invoke-LabelRollbackStep 'code_restore' {
            if ($existingVerified -and -not $rollbackUsesInstalled) {
                if ([string]::IsNullOrEmpty($replacementRollbackRoot)) {
                    throw 'Exact installed code preimage is unavailable for rollback.'
                }
                # Reuse the same pinned placement helper and its verified-tree
                # replacement contract, now with the already validated preimage.
                [void](Set-LabelWriterFenceInstalledTree $writerFenceControlRoot $writerSessionId $writerAttemptId $writerTransactionId $false)
                [void](Set-LabelWriterFenceDelegation `
                    -ControlRoot $writerFenceControlRoot `
                    -Status 'RESTORING' `
                    -SessionId $writerSessionId `
                    -AttemptId $writerAttemptId `
                    -ReplacementTransactionId $writerTransactionId `
                    -DelegationToken $writerDelegationToken `
                    -DelegatedSources @('canonical_placement','auto_update_task_mutation','auto_update_restore_preimage') `
                    -LifetimeSeconds 600)
                # INSTALL_THIS_PC consumes a release tree: its manifest metrics
                # exclude the installed bootstrap record. Reconstruct that exact
                # release view without changing or deleting the retained backup.
                $rollbackSource = Join-Path (Split-Path -Parent $auditPath) ($writerTransactionId + '-rollback-source')
                if ($legacyFirstRegistration) {
                    # A failed placement keeps its source as recovery evidence.
                    # A resumed rollback stages a new copy instead of colliding.
                    $rollbackSource += '-' + [Guid]::NewGuid().ToString('N')
                }
                if (Test-Path -LiteralPath $rollbackSource) { throw 'Rollback source staging already exists.' }
                [void](New-Item -ItemType Directory -Path $rollbackSource)
                $robocopy = Join-Path ([Environment]::SystemDirectory) 'robocopy.exe'
                & $robocopy $replacementRollbackRoot $rollbackSource /E /XJ /MT:16 /R:0 /W:0 /NFL /NDL /NJH /NJS /NP /XF (Join-Path $replacementRollbackRoot 'bootstrap-integrity.json') | Out-Null
                if ($LASTEXITCODE -lt 0 -or $LASTEXITCODE -ge 8) { throw "Rollback source copy failed: $LASTEXITCODE" }
                $restoreParameters = $helperParameters.Clone()
                $restoreParameters.SourceRoot = $rollbackSource
                $restoreParameters.ExpectedSourceAggregateSha256 = [string]$installedPreimageInventory.bootstrap_aggregate_sha256
                $restoreParameters.ExpectedSourceFileCount = [int]$installedPreimageInventory.file_count
                $restoreParameters.ExpectedSourceByteCount = [uint64]$installedPreimageInventory.byte_count
                $restoreExitCode = InvokeFrozenPlacementHelper $frozenPlacement $restoreParameters
                if ($restoreExitCode -ne 0) { throw "Exact code rollback failed: $restoreExitCode" }
                InvokeFrozenIntegrityProbe $frozenPlacement $install
                $restoredInventory = PortableInventory $install
                if ([string]$restoredInventory.sha256 -cne [string]$installedPreimageInventory.sha256) {
                    throw 'Restored code differs from the validated installed preimage.'
                }
                $audit.rollback.code_placement = 'RESTORED_PREIMAGE'
                [void](Set-LabelWriterFenceInstalledTree $writerFenceControlRoot $writerSessionId $writerAttemptId $writerTransactionId $true)
            }
            if (-not $existingVerified -and (Test-Path -LiteralPath $install)) {
                InvokeFrozenIntegrityProbe $frozenPlacement $install
                $rollbackInventory = PortableInventory $install
                if ([string]$rollbackInventory.sha256 -cne [string]$receiptSource.sha256) {
                    throw 'Absent-code rollback tree differs from the validated candidate.'
                }
                [void](Set-LabelWriterFenceDelegation `
                    -ControlRoot $writerFenceControlRoot `
                    -Status 'RESTORING' `
                    -SessionId $writerSessionId `
                    -AttemptId $writerAttemptId `
                    -ReplacementTransactionId $writerTransactionId `
                    -DelegationToken $writerDelegationToken `
                    -DelegatedSources @('canonical_placement','auto_update_task_mutation','auto_update_restore_preimage') `
                    -LifetimeSeconds 600)
                $removeParameters = $helperParameters.Clone()
                $removeParameters.ReplaceExistingVerifiedPortable = $false
                $removeParameters.Uninstall = $true
                $removeExit = Invoke-LabelDelegatedPlacementHelper $frozenPlacement $removeParameters $writerAuthority
                if ($removeExit -ne 0 -or (Test-Path -LiteralPath $install)) {
                    throw 'Absent-code rollback failed to restore code absence.'
                }
            }
            if (-not $existingVerified -and -not (Test-Path -LiteralPath $install)) {
                $audit.rollback.code_placement = 'RESTORED_ABSENCE'
            }
            $audit.rollback.code_restored = $existingVerified -or -not (Test-Path -LiteralPath $install)
            $audit.code_state = if (Test-Path -LiteralPath $install) { 'PRESENT' } else { 'ABSENT' }
                    }
                }
            }
        }
        if ($rollbackGenerationReady) {
            $rollbackGenerationReady = $false
            . Invoke-LabelRollbackStep 'generation_preimage' {
                Assert-LabelRollbackGeneration
                $rollbackGenerationReady = $true
            }
        }
        if ($rollbackGenerationReady) {
            . Invoke-LabelRollbackStep 'user_preimage_admission' {
        $ownerMutationLease = $null
        try {
            if ($writerFenceStarted) {
                $ownerMutationLease = Enter-LabelWriterAdmission `
                    -ControlRoot $writerFenceControlRoot `
                    -TimeoutMilliseconds 90000
            }
            . Invoke-LabelRollbackStep 'run_restore' { Restore $before }
            . Invoke-LabelRollbackStep 'task_restore' { RestoreScheduledTask $taskBefore }
            . Invoke-LabelRollbackStep 'stop_marker_restore' {
            if ([bool]$stopBefore.exists) {
                Copy-Item -LiteralPath ([string]$stopBefore.backup_path) -Destination $stop -Force
                if ((Sha $stop) -cne [string]$stopBefore.sha256) {
                    throw 'stop marker restore failed'
                }
            }
            elseif (Test-Path $stop) {
                Remove-Item $stop -Force
            }
            }
            . Invoke-LabelRollbackStep 'removal_report_restore' {
            if (Test-Path -LiteralPath $removalPath) {
                $removalProof=Join-Path $localAuditRoot ('canonical-portable-'+$runId+'-failed-removal-proof.json')
                if (-not (Test-Path -LiteralPath $removalProof)) { Copy-Item -LiteralPath $removalPath -Destination $removalProof }
                $audit.rollback.removal_failure_proof=[ordered]@{path=$removalProof;sha256=(Sha $removalProof)}
            }
            if ($null -ne $healthyRemovalReportBefore) {
                Copy-Item -LiteralPath $healthyRemovalReportBefore.backup_path -Destination $removalPath -Force
                if ((Sha $removalPath) -cne [string]$healthyRemovalReportBefore.sha256) {
                    throw 'Normal removal report preimage restoration failed.'
                }
                $audit.rollback.removal_report_restored = $true
            }
            elseif (-not $removalReportExisted -and (Test-Path -LiteralPath $removalPath)) {
                Remove-Item -LiteralPath $removalPath -Force
                if (Test-Path -LiteralPath $removalPath) { throw 'Removal report absence restoration failed.' }
                $audit.rollback.removal_report_restored = $true
            }
            }
        }
        finally {
            if ($null -ne $ownerMutationLease) {
                Exit-LabelWriterAdmission $ownerMutationLease
            }
        }
            }
            . Invoke-LabelRollbackStep 'official_legacy_task_restore' {
                if ($audit.Contains('legacy_relay_task_preimage')) {
                    $parameters=$helperParameters.Clone()
                    $parameters.LegacyRelayTransition='Restore'
                    $parameters.LegacyRelayPreimageSha256=[string]$audit.legacy_relay_task_preimage.sha256
                    $parameters.ReplaceExistingVerifiedPortable=$false
                    if ((Invoke-LabelDelegatedPlacementHelper $frozenPlacement $parameters $writerAuthority -RefreshDelegation) -ne 0) {
                        $failure=[InvalidOperationException]::new('LEGACY_RELAY_RESTORE_FAILED')
                        $failure.Data['placement_child_failure']=$Script:LabelPlacementChildFailure
                        throw $failure
                    }
                    $readback=Get-LabelOfficialLegacyTaskSnapshot
                    if ($null -eq $readback -or $readback.fingerprint.sha256 -cne [string]$audit.legacy_relay_task_preimage.fingerprint.sha256 -or
                        $readback.enabled -ne [bool]$audit.legacy_relay_task_preimage.enabled) { throw 'LEGACY_RELAY_RESTORE_READBACK_FAILED' }
                }
            }
            . Invoke-LabelRollbackStep 'protected_startup_restore' {
        if ($audit.Contains('startup_update_preimage')) {
            $restoreParameters=$helperParameters.Clone()
            $restoreParameters.StartupUpdateRestorePreimage=[string]$audit.startup_update_preimage.path
            $restoreParameters.StartupUpdateRestoreSha256=[string]$audit.startup_update_preimage.sha256
            $restoreParameters.ReplaceExistingVerifiedPortable=$false
            if ((Invoke-LabelDelegatedPlacementHelper $frozenPlacement $restoreParameters $writerAuthority -RefreshDelegation) -ne 0) {
                $failure=[InvalidOperationException]::new('Startup update preimage restoration failed.')
                $failure.Data['placement_child_failure']=$Script:LabelPlacementChildFailure
                throw $failure
            }
        }
        $rollbackStartupProven = $true
            }
            . Invoke-LabelRollbackStep 'preimage_readback' {
        $check = Snapshot
        if (
            [bool]$check.exists -ne [bool]$before.exists -or
            [string]$check.kind -cne [string]$before.kind -or
            [string]$check.data -cne [string]$before.data
        ) { throw 'registry restore failed' }
                if ($existingVerified) {
                    InvokeFrozenIntegrityProbe $frozenPlacement $install
                    if ([string](PortableInventory $install).sha256 -cne [string]$installedPreimageInventory.sha256) {
                        throw 'Rollback final code preimage differs.'
                    }
                } elseif (Test-Path -LiteralPath $install) { throw 'Rollback final code absence differs.' }
                Assert-LabelRollbackTaskPreimage $taskBefore
                if ($audit.Contains('legacy_relay_task_preimage')) {
                    $legacy=Get-LabelOfficialLegacyTaskSnapshot
                    if ($null -eq $legacy -or $legacy.fingerprint.sha256 -cne [string]$audit.legacy_relay_task_preimage.fingerprint.sha256 -or
                        $legacy.enabled -ne [bool]$audit.legacy_relay_task_preimage.enabled) { throw 'Legacy task final preimage differs.' }
                }
                if ([bool]$stopBefore.exists) {
                    if (-not (Test-Path -LiteralPath $stop -PathType Leaf) -or (Sha $stop) -cne [string]$stopBefore.sha256) { throw 'Rollback stop preimage differs.' }
                } elseif (Test-Path -LiteralPath $stop) { throw 'Rollback stop absence differs.' }
                if ($null -ne $healthyRemovalReportBefore) {
                    if ((Sha $removalPath) -cne [string]$healthyRemovalReportBefore.sha256) { throw 'Rollback removal preimage differs.' }
                } elseif (-not $removalReportExisted -and (Test-Path -LiteralPath $removalPath)) { throw 'Rollback removal absence differs.' }
                [void](Assert-RollbackRelayPreimage -ExpectedRelays @())
                $rollbackPreimageProven = $true
                $audit.rollback.code_restored = $true
            }
            # Keep protection when any prerequisite failed. Starting the old
            # relay before exact restoration and fence release is forbidden.
            $rollbackRuntimeProven = $rollbackCustodyReady -and $rollbackPreimageProven -and $rollbackStartupProven
            $rollbackReleaseGenerationProven = $false
            . Invoke-LabelRollbackStep 'generation_release' {
                Assert-LabelRollbackGeneration
                $rollbackReleaseGenerationProven = $true
            }
            $rollbackRuntimeProven = $rollbackRuntimeProven -and $rollbackReleaseGenerationProven
            if ($rollbackRuntimeProven) {
                . Invoke-LabelRollbackStep 'fence_release' {
        $firstRegistrationRollbackLease = $null
        try {
            if ($legacyFirstRegistration) {
                # Keep ordinary writers excluded while releasing the fence and
                # moving its own root. Session authority alone is a different mutex.
                $firstRegistrationRollbackLease = Enter-LabelWriterAdmission `
                    -ControlRoot $writerFenceControlRoot -TimeoutMilliseconds 90000
            }
            if ($writerFenceStarted) {
                $releasedFence = Stop-LabelWriterFence `
                    -ControlRoot $writerFenceControlRoot `
                    -SessionId $writerSessionId `
                    -AttemptId $writerAttemptId `
                    -ReplacementTransactionId $writerTransactionId `
                    -TimeoutMilliseconds 90000
                $writerFenceStarted = $false
                $audit.writer_fence.status = 'RELEASED_AFTER_ROLLBACK'
                $audit.writer_fence.writer_inventory_sha256 = [string]$releasedFence.writer_inventory_sha256
            }
            if ($legacyFirstRegistration) {
                $audit.rollback.first_registration_state = Move-LegacyFirstRegistrationState `
                    $firstRegistrationStatePaths ([string]$firstRegistrationAttempt.quarantine_root) `
                    $firstRegistrationStarted @() $firstRegistrationAbsentAncestors $localAuditRoot
                $audit.rollback.first_registration_state.retry_conflict_action = 'USE_SUPPORTED_ADMIN_RECOVERY'
                if (-not $audit.rollback.first_registration_state.restored_absent) {
                    # Retain custody across a final directory/ancestor move
                    # failure as well; a retry adopts only this exact session.
                    [void](Start-LabelWriterFence -ControlRoot $writerFenceControlRoot -Status 'RESTORING' `
                        -SessionId $writerSessionId -AttemptId $writerAttemptId `
                        -ReplacementTransactionId $writerTransactionId -SessionStartedAtUtc $writerStartedAt `
                        -OrchestratorSha256 $writerOrchestratorSha256 -WriterContractSha256 $writerContractSha256 `
                        -AuthorityOwnedByCaller)
                    $writerFenceStarted = $true
                    [void](Set-LabelWriterFenceInstalledTree $writerFenceControlRoot $writerSessionId $writerAttemptId $writerTransactionId $true)
                    throw 'First-registration final absence restoration is incomplete.'
                }
            }
        }
        finally {
            if ($null -ne $firstRegistrationRollbackLease) {
                Exit-LabelWriterAdmission $firstRegistrationRollbackLease
            }
        }
                }
                if (-not $writerFenceStarted) {
        foreach ($item in $old) {
            . Invoke-LabelRollbackStep 'relay_restart' {
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
        }
                }
            }
        }
        if (-not $rollbackGenerationReady) {
            . Invoke-LabelGenerationRefusalRecovery
        }
        if ($rollbackGenerationReady) {
        . Invoke-LabelRollbackStep 'final_readback' {
        [void](Assert-RollbackRelayPreimage -ExpectedRelays $old)
        $check = Snapshot
        if (
            [bool]$check.exists -ne [bool]$before.exists -or
            [string]$check.kind -cne [string]$before.kind -or
            [string]$check.data -cne [string]$before.data
        ) { throw 'registry restore failed' }
        }
        }
        $audit.rollback.runtime_restored = $rollbackRuntimeProven -and -not $writerFenceStarted
        if ($writerFenceStarted) {
            $audit.rollback.guidance = '복원 정합을 확인하지 못해 전송을 멈췄습니다. 자료와 복구 기록을 보존하세요. 같은 새 판을 다시 시작하거나 호환 수정판을 설치하려면 관리자 PowerShell에서 INSTALL_CANONICAL_PORTABLE.ps1 -ResumeRollbackAuditPath <감사 파일> -ExpectedRollbackAuditSha256 <감사 SHA256> 를 실행하세요.'
        }
        $audit.rollback.steps = $Script:LabelRollbackSteps.ToArray()
        $audit.rollback.step_failures = $Script:LabelRollbackFailures.ToArray()
        if ($Script:LabelRollbackErrors.Count -gt 0) { throw $Script:LabelRollbackErrors[0] }
}

if (-not $SourceRoot) { $SourceRoot = $PSScriptRoot }
. (Get-LabelSharedPortableLeafPath $PSScriptRoot)
. (Get-LabelSharedPortableLeafPath $PSScriptRoot 'data_generation.ps1')
$source = Full $SourceRoot 'SourceRoot'
function Read-CanonicalStartupLifecycle([string]$HostRoot, [string]$Root) {
    $parent = Split-Path -Parent $Root
    if (-not (Test-Path -LiteralPath (Join-Path $parent 'update-control')) -and
        -not (Test-Path -LiteralPath (Join-Path $parent 'removed.json'))) { return $null }
    $output = @(& (Join-Path $HostRoot 'runtime\python.exe') -I -B `
        (Join-Path $HostRoot 'app\main.py') --label-match-lifecycle-context --install-root $parent --operation install)
    if ($LASTEXITCODE -ne 0) { throw ('SUPPORTED_REMOVAL_REQUIRED:' + ($output -join "`n")) }
    $context = ($output -join "`n") | ConvertFrom-Json
    if ([string]$context.status -cne 'PASS') { throw 'SUPPORTED_REMOVAL_REQUIRED' }
    return $context
}
$install = Full $InstallRoot 'InstallRoot'
$startupLifecycle = $null
$lifecycleInstalled = $install

if (-not $testMode -and -not (Same $install $CanonicalRoot)) {
    throw 'InstallRoot is not canonical.'
}
$serverBaseUrl = ServerBaseUrlOrigin $ServerBaseUrl
$onboardingArguments = OnboardingArguments $serverBaseUrl
$serverBaseUrlSource = if ($serverBaseUrl) { 'explicit' } else { 'product_default' }
$sourceManifest = Manifest $source $SkipSignatureValidationForTest
. (Join-Path $source 'app/tools/failure_redaction.ps1')
. Import-LabelOfficialLegacyTaskFunctions (Join-Path $source 'INSTALL_THIS_PC.ps1')
if (-not $testMode -and -not $PlanOnly -and -not $ResumeRollbackAuditPath) {
    $startupLifecycle = Read-CanonicalStartupLifecycle $source $install
    if ($null -ne $startupLifecycle -and [string]$startupLifecycle.lifecycle_state -ceq 'INSTALLED') {
        $lifecycleInstalled = [string]$startupLifecycle.active_code_root
    }
}
$receiptSource = $null
$conflictReceiptSupplied = (
    -not [string]::IsNullOrWhiteSpace(
        [string]$env:KMTECH_LABEL_CONFLICT_RESOLUTION_RECEIPT_PATH
    ) -or
    -not [string]::IsNullOrWhiteSpace(
        [string]$env:KMTECH_LABEL_CONFLICT_RESOLUTION_RECEIPT_SHA256
    )
)
if ($conflictReceiptSupplied) {
    $receiptSource = ReceiptSource $source $sourceManifest
}
$wanted = Command $install -StartupUpdate:($sourceManifest.startup_update_protocol -eq 1)
if ($PlanOnly) {
    "install_status=PLAN_ONLY"
    "install_root=$install"
    "autostart_command=$wanted"
    "onboarding_server_base_url_source=$serverBaseUrlSource"
    if ($serverBaseUrl) { "onboarding_server_base_url=$serverBaseUrl" }
    "receipt_source_status=$(if ($null -eq $receiptSource) { 'NOT_REQUESTED' } else { 'PASS' })"
    'registry_changed=false'
    exit 0
}

$lad = Full $env:LOCALAPPDATA 'LOCALAPPDATA'
if (-not $testMode) {
    $profileLocal = [Environment]::GetFolderPath([Environment+SpecialFolder]::LocalApplicationData)
    if ([string]::IsNullOrWhiteSpace($profileLocal) -or -not (Same $lad (Full $profileLocal 'profile LOCALAPPDATA'))) {
        throw 'Current worker profile LOCALAPPDATA differs from the process environment.'
    }
}
$defaultDirectSyncRoot = Join-Path $lad 'KMTech\DirectSync\label_match'
$defaultDataRoot = Join-Path $lad 'KMTech\Label_Match\data'
$defaultSettingsPath = Join-Path $lad 'KMTech\Label_Match\config\app_settings.json'
$defaultProfilePath = Join-Path $lad 'KMTech\Logistics\profiles\Label_Match\runtime-profile.json'
$bootstrapCaPath = Join-Path $lad 'KMTech\Bootstrap\Label_Match\ca-bundle.pem'

$selectedDirectSyncRootValue = [string]$env:LABEL_MATCH_DIRECT_SYNC_ROOT
if ([string]::IsNullOrWhiteSpace($selectedDirectSyncRootValue)) {
    $selectedDirectSyncRootValue = [string]$env:LABEL_MATCH_DIRECT_SYNC_PROGRAM_DATA_ROOT
}
$selectedDirectSyncRoot = if ([string]::IsNullOrWhiteSpace($selectedDirectSyncRootValue)) {
    $defaultDirectSyncRoot
}
else { Resolve-LabelGenerationInputPath $selectedDirectSyncRootValue $source }
$selectedDataRoot = if ([string]::IsNullOrWhiteSpace([string]$env:LABEL_MATCH_SAVE_DIR)) {
    $defaultDataRoot
}
else { Resolve-LabelGenerationInputPath ([string]$env:LABEL_MATCH_SAVE_DIR) $source }
$selectedSettingsPath = if ([string]::IsNullOrWhiteSpace([string]$env:LABEL_MATCH_SETTINGS_PATH)) {
    $defaultSettingsPath
}
else { Resolve-LabelGenerationInputPath ([string]$env:LABEL_MATCH_SETTINGS_PATH) $source }
# The shared KM_LOGISTICS_PROFILE_PATH is not read: an inherited Machine value from the
# old PC-wide rollout points at a file other programs may use (TEST1 09-30 refused here).
$selectedProfilePath = if ([string]::IsNullOrWhiteSpace([string]$env:LABEL_MATCH_LOGISTICS_PROFILE_PATH)) {
    $defaultProfilePath
}
else { Resolve-LabelGenerationInputPath ([string]$env:LABEL_MATCH_LOGISTICS_PROFILE_PATH) $source }
$generationSecurityParents = @((Split-Path -Parent $bootstrapCaPath))
foreach ($generationInputName in @('LABEL_MATCH_ENROLLMENT_TLS_CA_BUNDLE_PATH',
    'LABEL_MATCH_LOGISTICS_TLS_CA_BUNDLE_PATH', 'LABEL_MATCH_PROTECTED_ADMIN_PROFILE')) {
    $generationInputValue = [Environment]::GetEnvironmentVariable($generationInputName, 'Process')
    if (-not [string]::IsNullOrWhiteSpace($generationInputValue)) {
        $generationSelectedInput = Resolve-LabelGenerationInputPath $generationInputValue $source
        $generationSecurityParents += Split-Path -Parent $generationSelectedInput
        [Environment]::SetEnvironmentVariable($generationInputName, $generationSelectedInput, 'Process')
    }
}
if ([string]::IsNullOrWhiteSpace([string]$env:LABEL_MATCH_PROTECTED_ADMIN_PROFILE)) {
    $generationSecurityParents += Join-Path $env:PROGRAMDATA 'KMTech\Label_Match\protected'
}

# Freeze the paths used by admission for every later product child as well.
$env:LABEL_MATCH_DIRECT_SYNC_ROOT = $selectedDirectSyncRoot
$env:LABEL_MATCH_SAVE_DIR = $selectedDataRoot
$env:LABEL_MATCH_SETTINGS_PATH = $selectedSettingsPath
$env:LABEL_MATCH_LOGISTICS_PROFILE_PATH = $selectedProfilePath

$statusRoot = Join-Path $defaultDirectSyncRoot 'status'
$stop = Join-Path $defaultDirectSyncRoot 'control\label_match_user_relay.stop.json'
$writerFenceControlRoot = Join-Path $defaultDirectSyncRoot 'control\writer-session'
$Script:LabelWriterFenceAdmissionProductionRoot = if ($testMode) { '' } else { $writerFenceControlRoot }
# Bind the original target user's roots before elevation; never use SYSTEM's profile.
$generationSubjectSid = [Security.Principal.WindowsIdentity]::GetCurrent().User.Value
$generationDataRoot = $selectedDataRoot
$generationMarkerPaths = @(
    (Join-Path $writerFenceControlRoot ('data-generation\' + $generationSubjectSid + '.json')),
    (Join-Path $selectedDataRoot 'data-generation.json'),
    (Join-Path $defaultDataRoot 'data-generation.json'),
    (Join-Path (Split-Path -Parent $selectedSettingsPath) 'data-generation.json'),
    (Join-Path (Split-Path -Parent $selectedProfilePath) 'data-generation.json'),
    (Join-Path $selectedDirectSyncRoot 'data-generation.json'),
    (Join-Path (Join-Path $selectedDirectSyncRoot 'queue') 'data-generation.json')
) | Select-Object -Unique
$generationMarkerPaths += @($generationSecurityParents | ForEach-Object { Join-Path $_ 'data-generation.json' })
if ($conflictReceiptSupplied) {
    $generationReceiptPath = Resolve-LabelGenerationInputPath ([string]$env:KMTECH_LABEL_CONFLICT_RESOLUTION_RECEIPT_PATH) $source
    $generationMarkerPaths += Join-Path (Split-Path -Parent $generationReceiptPath) 'data-generation.json'
}
$generationTargetVersion = Get-LabelPortableDataVersion $source $sourceManifest
$generationDowngradeDecision = Assert-LabelDataDowngrade -MarkerPaths $generationMarkerPaths -SubjectSid $generationSubjectSid `
    -TargetVersion $generationTargetVersion -TargetBuildId ([string]$sourceManifest.source_commit)
if (Test-Path -LiteralPath $selectedSettingsPath -PathType Leaf) {
    if ((Get-Item -LiteralPath $selectedSettingsPath).Length -gt 1048576) { throw 'User settings are oversized.' }
    $generationSettings = Get-Content -LiteralPath $selectedSettingsPath -Raw -Encoding UTF8 | ConvertFrom-Json
    if (-not [string]::IsNullOrWhiteSpace([string]$generationSettings.custom_save_path)) {
        $generationDataRoot = Resolve-LabelGenerationInputPath ([string]$generationSettings.custom_save_path) $source
    }
}
$generationMarkerPaths = @($generationMarkerPaths) + @(
    (Join-Path $generationDataRoot 'data-generation.json')
) | Select-Object -Unique
$onboardingPath = Join-Path $statusRoot 'current_user_onboarding.json'
$removalPath = Join-Path $statusRoot 'current_user_removal.json'
$relayPath = Join-Path $statusRoot 'label_match_user_relay.json'
if ($ResumeRollbackAuditPath) {
    . Invoke-LabelResumeRollback
    'rollback_resume_status=PROVEN'
    exit 0
}
$generationDowngradeDecision = Assert-LabelDataDowngrade -MarkerPaths $generationMarkerPaths -SubjectSid $generationSubjectSid `
    -TargetVersion $generationTargetVersion `
    -TargetBuildId ([string]$sourceManifest.source_commit)
$legacyRelayBefore=Get-LabelOfficialLegacyTaskSnapshot
Assert-LabelOfficialLegacyTaskLauncher $legacyRelayBefore
$before = Snapshot
# Use the verified guarded caller's existing product entrypoint. The target
# app-root stays the source release, including compatible pre-G source packets.
# Finish nested admission before audit/preimage writes, elevation, or any fence.
$generationProductRoot = Full $PSScriptRoot 'read-only admission product'
if (-not (Same $generationProductRoot $source)) {
    [void](Manifest $generationProductRoot $SkipSignatureValidationForTest)
    [void](PortableInventory $generationProductRoot)
}
if ($null -eq $receiptSource) { $receiptSource = PortableInventory $source }
Invoke-LabelSourceDataAdmission $generationProductRoot $source $receiptSource `
    (Join-Path $lad 'KMTech\Label_Match\install-audit') $EvidencePath $onboardingArguments
# Preparation must publish a known failure even before a preimage audit exists.
$audit = $null
$archiveRoot = ''
$retainedAuditPath = ''
try {
$old = @(Relays)
$scheduledTasksAtPreflight = @(
    Get-LabelCurrentUserScheduledTasks | Where-Object {
        [StringComparer]::OrdinalIgnoreCase.Equals(
            [string]$_.TaskName,
            $CanonicalTaskName
        ) -and [StringComparer]::OrdinalIgnoreCase.Equals([string]$_.TaskPath, '\')
    }
)
$pristineResiduePaths = @(
    # The whole DirectSync root catches credentials, identity, queue, status, and control residue.
    $defaultDirectSyncRoot,
    $selectedDirectSyncRoot,
    # These explicit paths keep stop-marker and credential absence visible in the predicate contract.
    $stop,
    (Join-Path $selectedDirectSyncRoot 'control\label_match_user_relay.stop.json'),
    (Join-Path $selectedDirectSyncRoot 'credential.json'),
    # Receipts/recovery evidence are existing state, even without a credential.
    (Join-Path $lad 'KMTech\Label_Match\install-audit'),
    # User data/settings are preserved by removal, so their presence still means the PC is not pristine.
    $defaultDataRoot,
    $selectedDataRoot,
    (Split-Path -Parent $defaultSettingsPath),
    (Split-Path -Parent $selectedSettingsPath),
    $defaultSettingsPath,
    $selectedSettingsPath,
    # Profile and DPAPI secret residue live outside the DirectSync root and must be checked separately.
    (Split-Path -Parent $defaultProfilePath),
    $defaultProfilePath,
    (Join-Path (Split-Path -Parent $defaultProfilePath) 'secrets\bearer-token.dpapi'),
    (Split-Path -Parent $selectedProfilePath),
    $selectedProfilePath,
    (Join-Path (Split-Path -Parent $selectedProfilePath) 'secrets\bearer-token.dpapi'),
    (Split-Path -Parent $bootstrapCaPath),
    $bootstrapCaPath
)
# Own authentication/recovery residue only. Business files and a public
# bootstrap CA do not manufacture a registration; legacy shared paths are not read.
$firstRegistrationResiduePaths = @(
    foreach ($root in @($defaultDirectSyncRoot, $selectedDirectSyncRoot)) {
        foreach ($relative in @('producer_identity.json', 'producer_manifest.json', 'credential.json',
            'secrets', 'queue', 'spool', 'upload_status', 'evidence', 'status', 'logs',
            'control\label_match_user_relay.stop.json')) {
            Join-Path $root $relative
        }
    }
    (Split-Path -Parent $defaultProfilePath)
    (Split-Path -Parent $selectedProfilePath)
    $selectedSettingsPath
    (Join-Path $defaultDataRoot 'package_logistics_outbox.sqlite3')
    (Join-Path $selectedDataRoot 'package_logistics_outbox.sqlite3')
)
# These entire roots/files must start absent so only this attempt's newly
# created state can be moved out on rollback. Existing business CSVs stay put.
$firstRegistrationQuarantinePaths = @(
    $defaultDirectSyncRoot
    (Split-Path -Parent $selectedProfilePath)
    $selectedSettingsPath
    foreach ($suffix in @('', '-wal', '-shm', '-journal')) {
        Join-Path $selectedDataRoot ('package_logistics_outbox.sqlite3' + $suffix)
    }
)
$firstRegistrationStatePaths = @(
    # The credential is detached first, before any other authentication state.
    (Join-Path $defaultDirectSyncRoot 'credential.json')
    $firstRegistrationResiduePaths | Where-Object { -not (Same $_ (Join-Path $defaultDirectSyncRoot 'credential.json')) }
    $firstRegistrationQuarantinePaths | Where-Object { -not (Same $_ $defaultDirectSyncRoot) }
    $defaultDirectSyncRoot
) | Select-Object -Unique
$pristineInstall = Test-PristineInstallState `
    $install `
    $before `
    $old `
    $scheduledTasksAtPreflight `
    $pristineResiduePaths `
    $bootstrapCaPath `
    (Join-Path $source 'runtime\python.exe') `
    (Join-Path $source 'app')
$healthyLifecycle = $null
if (-not $conflictReceiptSupplied -and -not $pristineInstall) {
    if (-not (Same $selectedDirectSyncRoot $defaultDirectSyncRoot) -or
        -not (Same $selectedDataRoot $defaultDataRoot) -or
        -not (Same $selectedSettingsPath $defaultSettingsPath) -or
        -not (Same $selectedProfilePath $defaultProfilePath)) {
        throw 'Pinned conflict-resolution receipt source validation is required. Noncanonical state is not eligible for healthy lifecycle.'
    }
    Assert-HealthyLifecycleOwnership $before $old $scheduledTasksAtPreflight $install
}
$runId = (Get-Date).ToUniversalTime().ToString('yyyyMMddTHHmmssfffZ') + '-' +
    [Guid]::NewGuid().ToString('N')
$placement = 'INSTALL_REQUIRED'
$existingVerified = $false
$installedPreimageInventory = $null
$candidate = $sourceManifest
if (Test-Path -LiteralPath $install) {
    $candidate = Manifest $lifecycleInstalled $SkipSignatureValidationForTest
    InvokeFrozenIntegrityProbe ([pscustomobject]@{
        root = $source
        integrity_sha256 = [string]$receiptSource.critical_file_sha256.bootstrap_integrity_helper
    }) $lifecycleInstalled
    $installedPreimageInventory = PortableInventory $lifecycleInstalled
    $existingVerified = $true
    if ([string]$candidate.source_commit -ceq [string]$sourceManifest.source_commit) {
        $placement = 'REUSED_VERIFIED'
    }
}
$writerTransition = Assert-WriterTransition $source $(if ($existingVerified) { $lifecycleInstalled } else { $source })
$legacyFirstRegistration = $false
$firstRegistrationStateVerified = $false
$firstRegistrationStarted = $false
$firstRegistrationAttempt = $null
$firstRegistrationAttemptPath = ''
$firstRegistrationAbsentAncestors = @()
$firstRegistrationResume = Read-FirstRegistrationAttempt `
    (Join-Path $env:LOCALAPPDATA 'KMTech\Label_Match\install-audit') $install `
    ([string]$sourceManifest.source_commit) (Sha (Join-Path $source 'INSTALL_CANONICAL_PORTABLE.ps1')) $firstRegistrationStatePaths
if (-not $conflictReceiptSupplied -and -not $pristineInstall) {
    $legacyFirstRegistration = Test-LegacyFirstRegistrationState `
        $existingVerified $writerTransition $before $old $scheduledTasksAtPreflight (@($firstRegistrationResiduePaths) + @($firstRegistrationQuarantinePaths))
    if ($legacyFirstRegistration -and @(UnquiescedProductWriters).Count -ne 0) {
        throw 'First registration requires normal shutdown of the installed Label product writer.'
    }
}
if ($null -ne $firstRegistrationResume) {
    # A retry performs only the recorded rollback, never another enrollment.
    $legacyFirstRegistration = $true
    if (@(UnquiescedProductWriters).Count -ne 0) {
        throw 'Pending first-registration recovery requires normal shutdown of the Label product writer.'
    }
}
elseif ($legacyFirstRegistration) {
    $firstRegistrationAbsentAncestors = @(FirstRegistrationAbsentAncestors $firstRegistrationStatePaths)
}
if ($null -eq $startupLifecycle -and -not $conflictReceiptSupplied -and -not $pristineInstall -and -not $legacyFirstRegistration) {
    $healthyLifecycle = HealthyLifecycle $source $install
}
$healthyWithoutCode = $null -ne $healthyLifecycle -and -not $existingVerified
$healthyRemovedInstall = $healthyWithoutCode -and $healthyLifecycle.marker_present
if ($healthyWithoutCode -and (
    ((Test-Path -LiteralPath $removalPath) -and -not $healthyLifecycle.marker_present) -or
    $before.exists -or $old.Count -ne 0 -or
    $scheduledTasksAtPreflight.Count -ne 0
)) { throw 'Healthy reinstall requires completed same-user removal and absent persistence.' }
# All compatibility checks precede snapshots on disk and the active fence: even
# publishing an undelegated fence can stop a resident preimage relay.
$localAuditRoot = Join-Path $lad 'KMTech\Label_Match\install-audit'
New-Item -ItemType Directory -Path $localAuditRoot -Force | Out-Null
$auditPath = Join-Path $localAuditRoot "canonical-portable-$runId.json"
$elevationLogPath = Join-Path $localAuditRoot "canonical-portable-$runId-elevated.jsonl"
$taskBefore = ScheduledTaskSnapshot $localAuditRoot $runId
$healthyRemovalReportBefore = $null
$removalReportExisted = Test-Path -LiteralPath $removalPath
$stopBefore = [ordered]@{ exists = $false; sha256 = ''; backup_path = '' }
if (Test-Path -LiteralPath $stop -PathType Leaf) {
    $stopBackup = Join-Path $localAuditRoot "canonical-portable-$runId-stop-preimage.json"
    Copy-Item -LiteralPath $stop -Destination $stopBackup -Force
    $stopBefore = [ordered]@{
        exists = $true
        sha256 = Sha $stop
        backup_path = $stopBackup
    }
    if ($null -ne $healthyLifecycle) {
        $reportBackup = Join-Path $localAuditRoot "canonical-portable-$runId-removal-preimage.json"
        Copy-Item -LiteralPath $removalPath -Destination $reportBackup
        $healthyRemovalReportBefore = [ordered]@{ backup_path=$reportBackup; sha256=(Sha $reportBackup) }
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
    code_preimage = if ($existingVerified) { 'PRESENT' } else { 'ABSENT' }
    current_user_registration_mode = if ($legacyFirstRegistration) { 'FIRST_REGISTRATION_OVER_PINNED_PREIMAGE' }
        elseif ($pristineInstall) { 'FIRST_INSTALL' } else { 'EXISTING_STATE' }
    first_registration_preimage = if ($legacyFirstRegistration) {
        @($firstRegistrationQuarantinePaths | ForEach-Object { [ordered]@{path=$_; state='ABSENT'} })
    } else { @() }
    first_registration_credential_preimage = if ($legacyFirstRegistration) {
        [ordered]@{path=(Join-Path $defaultDirectSyncRoot 'credential.json'); state='ABSENT'}
    } else { $null }
    code_state = if (Test-Path -LiteralPath $install) { 'PRESENT' } else { 'ABSENT' }
    source_commit = [string]$sourceManifest.source_commit
    runtime_pythonw_sha256 = Sha (Join-Path $source 'runtime\pythonw.exe')
    runtime_pythonw_signature = [string](
        Get-AuthenticodeSignature (Join-Path $source 'runtime\pythonw.exe')
    ).Status
    elevation_log_path = $elevationLogPath
    registry_value = $RunName
    onboarding_server_base_url = [ordered]@{
        source = $serverBaseUrlSource
        value = $serverBaseUrl
    }
    preimage = $before
    after = [ordered]@{ exists = $true; kind = 'String'; data = $wanted }
    relay_process_preimage_count = $old.Count
    scheduled_task_preimage = [ordered]@{
        exists = [bool]$taskBefore.exists
        enabled = [bool]$taskBefore.enabled
        xml_sha256 = [string]$taskBefore.xml_sha256
        backup_path = [string]$taskBefore.backup_path
    }
    stop_marker_path = $stop
    stop_marker_preimage = $stopBefore
    writer_transition = [ordered]@{
        compatibility = [string]$writerTransition.compatibility
        installed_inventory_sha256 = [string]$writerTransition.installed_inventory_sha256
        candidate_inventory_sha256 = [string]$writerTransition.candidate_inventory_sha256
        installed_source_commit = if ($existingVerified) { [string]$candidate.source_commit } else { '' }
        candidate_source_commit = [string]$sourceManifest.source_commit
        installed_portable_inventory_sha256 = if ($existingVerified) { [string]$installedPreimageInventory.sha256 } else { '' }
        candidate_portable_inventory_sha256 = [string]$receiptSource.sha256
    }
    rollback = [ordered]@{ available = $true; applied = $false; runtime_restored = $false }
}
Save $auditPath $audit
try {
if ($EvidencePath) { Save (Full $EvidencePath 'EvidencePath') $audit }
$guardedHelperOrigin = $source
$placementCriticalHashes = $receiptSource.critical_file_sha256
if (-not (Test-Path -LiteralPath (Join-Path $source 'app\kmtech_shared\powershell\data_generation.ps1') -PathType Leaf)) {
    # A compatible pre-G packet supplies code and its own writer inventory.
    # Placement/integrity/G helpers come from this separately verified G caller.
    $guardedHelperOrigin = Full $PSScriptRoot 'guarded installer packet'
    $guardedCallerManifest = Manifest $guardedHelperOrigin $SkipSignatureValidationForTest
    $guardedCallerReceipt = ReceiptSource $guardedHelperOrigin $guardedCallerManifest
    $placementCriticalHashes = [pscustomobject]@{
        placement_helper = [string]$guardedCallerReceipt.critical_file_sha256.placement_helper
        bootstrap_integrity_helper = [string]$guardedCallerReceipt.critical_file_sha256.bootstrap_integrity_helper
        writer_fence_helper = [string]$receiptSource.critical_file_sha256.writer_fence_helper
    }
}
$frozenPlacement = FreezePlacementHelper `
    $source `
    $localAuditRoot `
    $runId `
    $placementCriticalHashes `
    $guardedHelperOrigin
# The ordinary preflight uses only the already pinned/frozen integrity helper.
$preparationIntegrityBytes = PinnedFileBytes (Join-Path ([string]$frozenPlacement.root) 'tools\bootstrap_integrity.ps1') ([string]$frozenPlacement.integrity_sha256)
. ([ScriptBlock]::Create((([Text.UTF8Encoding]::new($false, $true)).GetString($preparationIntegrityBytes) -replace '^\uFEFF', ''))) -SharedCodeRoot ([string]$frozenPlacement.root)
if (-not $testMode) {
    try { Assert-LabelProtectedInstallPath (Split-Path -Parent $install) -AllowSupportedPreparation }
    catch {
        $admissionFailure = $_
        $audit.status = 'FAILED_PREPARATION_ADMISSION'
        $audit.preparation_failure = Get-LabelLaunchFailure $admissionFailure 'preparation'
        $audit.preparation_failure.exit_code = Get-InstallationPreparationExitCode $admissionFailure
        try { Save $auditPath $audit } catch { }
        throw $admissionFailure
    }
}
. ([string]$frozenPlacement.writer_fence_path)

$writerSessionId = [Guid]::NewGuid().ToString('N')
$writerAttemptId = [Guid]::NewGuid().ToString('N')
$writerTransactionId = [Guid]::NewGuid().ToString('N')
$writerDelegationToken = ([Guid]::NewGuid().ToString('N') + [Guid]::NewGuid().ToString('N'))
$writerStartedAt = [DateTime]::UtcNow.ToString('o')
$writerOwnerStartedAtFileTime = [Diagnostics.Process]::GetCurrentProcess().StartTime.ToUniversalTime().ToFileTimeUtc().ToString()
$writerOrchestratorSha256 = Sha (Join-Path $source 'INSTALL_CANONICAL_PORTABLE.ps1')
$writerContractSha256 = [string]$frozenPlacement.writer_fence_sha256
if ($null -ne $firstRegistrationResume) {
    $firstRegistrationAttempt = $firstRegistrationResume
    $firstRegistrationAttemptPath = [string]$firstRegistrationResume.record_path
    $firstRegistrationAbsentAncestors = @($firstRegistrationResume.absent_ancestors)
    $firstRegistrationStateVerified = [bool]$firstRegistrationResume.state_verified
    $firstRegistrationStarted = [bool]$firstRegistrationResume.registration_started
    if ([string]$firstRegistrationResume.writer_contract_sha256 -cne $writerContractSha256) {
        throw 'Pending first-registration writer contract differs.'
    }
    $originalInventory = $firstRegistrationResume.preimage_inventory
    if ($null -eq $originalInventory -or [string]$originalInventory.sha256 -notmatch '^[0-9a-f]{64}$') {
        throw 'Pending first-registration code preimage is invalid.'
    }
    $originalWriterTransition = $writerTransition
    if (-not $existingVerified -or [string]$installedPreimageInventory.sha256 -cne [string]$originalInventory.sha256) {
        $recoveryCodeRoot = Full ([string]$firstRegistrationResume.code_backup_root) 'first-registration code backup'
        $installParent = Split-Path -Parent $install
        if (-not $recoveryCodeRoot.StartsWith($installParent.TrimEnd('\') + '\', [StringComparison]::OrdinalIgnoreCase) -or
            (-not $testMode -and (Split-Path -Leaf $recoveryCodeRoot) -notlike '.current.rollback.*')) {
            throw 'Pending first-registration code backup is outside the retained preimage location.'
        }
        [void](Manifest $recoveryCodeRoot $SkipSignatureValidationForTest)
        InvokeFrozenIntegrityProbe $frozenPlacement $recoveryCodeRoot
        $recoveryInventory = PortableInventory $recoveryCodeRoot
        $recoveryTransition = Assert-WriterTransition $source $recoveryCodeRoot
        if ([string]$recoveryInventory.sha256 -cne [string]$originalInventory.sha256 -or
            -not (Test-LegacyFirstRegistrationState $true $recoveryTransition ([pscustomobject]@{exists=$false}) @() @() @())) {
            throw 'Pending first-registration code backup differs from its validated legacy release.'
        }
        $originalWriterTransition = $recoveryTransition
    }
    if (-not (Test-LegacyFirstRegistrationState $true $originalWriterTransition ([pscustomobject]@{exists=$false}) @() @() @()) -or
        [string]$firstRegistrationResume.installed_writer_inventory_sha256 -cne [string]$originalWriterTransition.installed_inventory_sha256) {
        throw 'Pending first-registration writer inventory differs from its validated legacy release.'
    }
    $existingVerified = $true
    $pristineInstall = $false
    $installedPreimageInventory = $originalInventory
    $before = [pscustomobject]@{exists=$false;kind='';data=''}
    $old = @()
    $taskBefore = [pscustomobject]@{exists=$false;enabled=$false;backup_path=''}
    $stopBefore = [pscustomobject]@{exists=$false}
    $removalReportExisted = $false
    $healthyRemovalReportBefore = $null
    $writerSessionId = [string]$firstRegistrationResume.writer_session_id
    $writerAttemptId = [string]$firstRegistrationResume.writer_attempt_id
    $writerTransactionId = [string]$firstRegistrationResume.writer_transaction_id
    $writerTransition.installed_inventory_sha256 = [string]$firstRegistrationResume.installed_writer_inventory_sha256
    $audit.resumed_first_registration_attempt = [string]$firstRegistrationResume.run_id
}
elseif ($legacyFirstRegistration) {
    $firstRegistrationAttemptPath = Join-Path $localAuditRoot ('canonical-portable-' + $runId + '-first-registration-attempt.json')
    $firstRegistrationAttempt = [ordered]@{
        schema='label-match-first-registration-attempt-v1'; run_id=$runId; status='PREFLIGHT_RECORDED'
        install_root=$install; source_commit=[string]$sourceManifest.source_commit
        orchestrator_sha256=$writerOrchestratorSha256; writer_contract_sha256=$writerContractSha256
        writer_session_id=$writerSessionId; writer_attempt_id=$writerAttemptId; writer_transaction_id=$writerTransactionId
        installed_writer_inventory_sha256=[string]$writerTransition.installed_inventory_sha256
        absent_paths=$firstRegistrationStatePaths; absent_ancestors=$firstRegistrationAbsentAncestors
        preimage_inventory=$installedPreimageInventory; code_backup_root=''; state_verified=$false; registration_started=$false
        quarantine_root=(Join-Path $localAuditRoot ('canonical-portable-' + $runId + '-first-registration-quarantine'))
        remaining_paths=@(); old_app_state='ABSENT'; server_rollback_performed=$false
    }
    Save-FirstRegistrationAttempt $firstRegistrationAttempt $firstRegistrationAttemptPath
}
$Script:LabelWriterFenceInstalledIdentity = [pscustomobject]@{
    session_id = $writerSessionId
    attempt_id = $writerAttemptId
    replacement_transaction_id = $writerTransactionId
    orchestrator_sha256 = $writerOrchestratorSha256
    writer_contract_sha256 = $writerContractSha256
    inventory_sha256 = [string]$writerTransition.installed_inventory_sha256
}
if ([string]$writerTransition.candidate_inventory_sha256 -cne $Script:LabelWriterFenceInventorySha256) {
    throw 'Candidate writer inventory differs from the frozen helper.'
}
$writerAuthority = $null
$writerFenceStarted = $false
$writerEnvironmentBefore = @(
    $WriterDelegationEnvironmentNames | ForEach-Object {
        [Environment]::GetEnvironmentVariable($_, 'Process')
    }
)
$restoreSources = @(
    'data_generation_record',
    'current_user_onboarding',
    'worker_enrollment_network',
    'direct_sync_enqueue',
    'direct_sync_relay_cycle',
    'direct_sync_upload',
    'persistent_relay_cycle',
    'persistent_relay_status',
    'raw_relay_runner',
    'relay_batch_claim',
    'relay_batch_drain',
    'relay_child_launch',
    'relay_queue_schema',
    'relay_spool_enqueue',
    'relay_stale_lease_reset',
    'scheduled_task_remove',
    'user_relay_autostart_install',
    'user_relay_process_start',
    'user_relay_stop_release',
    'user_relay_stop_request',
    'auto_update_task_mutation',
    'auto_update_restore_preimage',
    'auto_update_fixed_helper'
)
$rollbackSources = @(
    'current_user_setup_removal',
    'scheduled_task_remove',
    'user_relay_autostart_remove',
    'user_relay_stop_request',
    'auto_update_task_mutation',
    'auto_update_restore_preimage'
)
}
catch {
    $pretransactionFailure = $_
    $audit.status = 'FAILED_PRETRANSACTION'
    $audit.failure = Get-LabelLaunchFailure $pretransactionFailure 'pretransaction'
    $audit.recovery = [ordered]@{ transaction_started = $false; artifacts_archived = $false }
    try {
        Save $auditPath $audit
        $archiveRoot = Archive-PretransactionFailure $localAuditRoot $runId
        $audit.recovery.artifacts_archived = $true
        $audit.recovery.active_audit_root_retained = $Script:LabelPretransactionActiveRootRetained
        $audit.recovery.archive_root = $archiveRoot
        Save (Join-Path $archiveRoot (Split-Path -Leaf $auditPath)) $audit
        if ($EvidencePath) { Save (Full $EvidencePath 'EvidencePath') $audit }
    }
    catch {
        # A partial archive is retained, and no receipt/guard is relaxed to retry.
        # Report both failures while preserving the original ErrorRecord.
        $archiveFailure = $_
        $pretransactionFailure.Exception.Data['pretransaction_recovery_failure'] = $archiveFailure.Exception.Message
        $audit.recovery.failure = Get-LabelLaunchFailure $archiveFailure 'archive'
        $retainedAuditPath = Join-Path (Join-Path ($localAuditRoot + '-pretransaction-failures') $runId) (Split-Path -Leaf $auditPath)
        if (Test-Path -LiteralPath $auditPath) { $retainedAuditPath = $auditPath }
        try {
            Save $retainedAuditPath $audit
            if ($EvidencePath) { Save (Full $EvidencePath 'EvidencePath') $audit }
        } catch { }
    }
    $Script:LabelWriterFenceInstalledIdentity = $null
    throw $pretransactionFailure
}
$mutated = $false
$replacementRollbackRoot = if ($null -ne $firstRegistrationResume) { [string]$firstRegistrationResume.code_backup_root } else { '' }
$replacementBackupsBefore = @()
$placementAttempted = $false
}
catch {
    $preparationFailure = $_
    if ($null -ne $audit -and [string]$audit.status -ceq 'FAILED_PRETRANSACTION') {
        # The existing archive/recovery audit remains authoritative.
        'install_status=FAILED_PRETRANSACTION'
        $failureAuditPaths = @($retainedAuditPath)
        if ($archiveRoot) { $failureAuditPaths += Join-Path $archiveRoot (Split-Path -Leaf $auditPath) }
        $failureAuditPaths += $auditPath
        foreach ($failureAuditPath in $failureAuditPaths) {
            if ($failureAuditPath -and (Test-Path -LiteralPath $failureAuditPath -PathType Leaf)) {
                "audit_path=$failureAuditPath"
                break
            }
        }
    }
    else {
        Write-LabelPreparationFailure $preparationFailure $source $receiptSource `
            (Join-Path $lad 'KMTech\Label_Match\install-audit') $EvidencePath
    }
    throw $preparationFailure
}
$original = $null
$Script:LabelPlacementSessionRequested = $true
$Script:LabelPlacementSession = $null
$Script:LabelPlacementSessionLaunchAttempted = $false
$Script:LabelCanonicalConfirmation = 'UNATTEMPTED'
try {
    $writerAuthority = Enter-LabelWriterSessionAuthority `
        -SessionId $writerSessionId `
        -AttemptId $writerAttemptId `
        -OrchestratorSha256 $writerOrchestratorSha256 `
        -ReplacementTransactionId $writerTransactionId `
        -WriterContractSha256 $writerContractSha256
    if ($null -ne $firstRegistrationResume -and
        $null -ne (Read-LabelWriterFence $writerFenceControlRoot -AllowAbsent)) {
        # Session authority excludes a still-running original installer. Retire
        # only its exact orphan fence. An absent fence uses normal Start below.
        [void](Stop-LabelWriterFence -ControlRoot $writerFenceControlRoot `
            -SessionId $writerSessionId -AttemptId $writerAttemptId `
            -ReplacementTransactionId $writerTransactionId -TimeoutMilliseconds 90000)
    }
    if ($legacyFirstRegistration) {
        $mutated = $true
        $firstRegistrationAttempt.status = 'RUNNING'
        Save-FirstRegistrationAttempt $firstRegistrationAttempt $firstRegistrationAttemptPath
    }
    [void](Start-LabelWriterFence `
        -ControlRoot $writerFenceControlRoot `
        -Status 'QUIESCING' `
        -SessionId $writerSessionId `
        -AttemptId $writerAttemptId `
        -ReplacementTransactionId $writerTransactionId `
        -SessionStartedAtUtc $writerStartedAt `
        -OrchestratorSha256 $writerOrchestratorSha256 `
        -WriterContractSha256 $writerContractSha256 `
        -AuthorityOwnedByCaller)
    $writerFenceStarted = $true
    $mutated = $true
    $audit.writer_fence = [ordered]@{
        status = 'QUIESCING'
        owner_process_id = [int]$PID
        owner_process_started_at_filetime = $writerOwnerStartedAtFileTime
        orchestrator_sha256 = $writerOrchestratorSha256
        writer_contract_sha256 = $writerContractSha256
        session_started_at_utc = $writerStartedAt
        session_id = $writerSessionId
        attempt_id = $writerAttemptId
        replacement_transaction_id = $writerTransactionId
        writer_inventory_sha256 = $Script:LabelWriterFenceInventorySha256
        token_recorded = $false
    }
    Save $auditPath $audit
    if ($null -ne $firstRegistrationResume) {
        $helperParameters = @{
            InstallerRunId=$runId; InstallerOwnerProcessId=[int]$PID
            InstallerOwnerProcessStartedAtFileTime=$writerOwnerStartedAtFileTime
            SourceRoot=$source; InstallRoot=$install; ElevationLogPath=$elevationLogPath
            ExpectedBootstrapScriptSha256=[string]$frozenPlacement.helper_sha256
            VerifiedBootstrapScriptPath=[string]$frozenPlacement.helper_path; BootstrapIntegrityPreloaded=$true
            WriterFenceFunctionsPreloaded=$true; WriterFenceControlRoot=$writerFenceControlRoot
            DataGenerationSubjectSid=$generationSubjectSid; DataGenerationMarkerPaths=@($generationMarkerPaths)
            WriterFenceSessionId=$writerSessionId; WriterFenceAttemptId=$writerAttemptId
            WriterFenceReplacementTransactionId=$writerTransactionId; WriterFenceDelegationToken=$writerDelegationToken
            AllowNoncanonicalLayoutForTest=[bool]$testMode; ReplaceExistingVerifiedPortable=$true; DryRun=$false
        }
        throw 'RESUME_FIRST_REGISTRATION_ROLLBACK'
    }

    [void](Set-LabelWriterFenceDelegation `
        -ControlRoot $writerFenceControlRoot `
        -Status 'QUIESCING' `
        -SessionId $writerSessionId `
        -AttemptId $writerAttemptId `
        -ReplacementTransactionId $writerTransactionId `
        -DelegationToken $writerDelegationToken `
        -DelegatedSources $rollbackSources `
        -LifetimeSeconds 600)
    SetWriterDelegationEnvironment `
        $writerDelegationToken `
        $writerSessionId `
        $writerAttemptId `
        $writerTransactionId

    if ($legacyFirstRegistration) {
        # Repeat absence under writer admission before placement. Fence files
        # live in control/writer-session, outside the authentication residue set.
        $currentTasks = @(Get-ScheduledTask -ErrorAction Stop | Where-Object {
            [StringComparer]::OrdinalIgnoreCase.Equals([string]$_.TaskName, $CanonicalTaskName) -and
            [StringComparer]::OrdinalIgnoreCase.Equals([string]$_.TaskPath, '\')
        })
        if (-not (Test-LegacyFirstRegistrationState $existingVerified $writerTransition `
            (Snapshot) @(Relays) $currentTasks (@($firstRegistrationResiduePaths) + @($firstRegistrationQuarantinePaths | Where-Object { -not (Same $_ $defaultDirectSyncRoot) })))) {
            throw 'Current-user state changed before legacy first registration.'
        }
        $firstRegistrationStateVerified = $true
        $firstRegistrationAttempt.state_verified = $true
        Save-FirstRegistrationAttempt $firstRegistrationAttempt $firstRegistrationAttemptPath
    }
    if (-not $pristineInstall -and -not $legacyFirstRegistration) {
        if ($null -ne $healthyLifecycle) {
            [void](HealthyLifecycle $source $install 'inspect' ([string]$healthyLifecycle.state_sha256))
        }
        # Completed same-user removal remains valid after code uninstall. Its
        # canonical host is absent, so do not rerun removal from a staging root.
        if (-not $healthyWithoutCode) {
            $removalRoot = if ($existingVerified) { $install } else { $source }
            if ($existingVerified) {
                [void](Set-LabelWriterFenceInstalledTree $writerFenceControlRoot $writerSessionId $writerAttemptId $writerTransactionId $true)
            }
            $removalStarted = [DateTime]::UtcNow
            Product $removalRoot '--remove-current-user-setup'
            $removal = Get-Content $removalPath -Raw -Encoding UTF8 | ConvertFrom-Json
            if (
                (Snapshot).exists -or
                [string]$removal.status -cne 'PASS_DATA_PRESERVED' -or
                [string]$removal.relay_process.status -cne 'ABSENT' -or
                (Get-Item $removalPath).LastWriteTimeUtc -lt $removalStarted.AddSeconds(-1) -or
                $null -ne (Get-LabelCurrentUserScheduledTasks)
            ) { throw 'Removal readback failed.' }
        }
    }
    Wait-LabelRelayPreimageExit $old
    $unquiesced = @(UnquiescedProductWriters)
    if ($unquiesced.Count -ne 0) {
        throw 'Writer quiescence failed: a Label product writer process remains.'
    }
    $audit.writer_fence.status = 'QUIESCED'
    Save $auditPath $audit
    [void](Set-LabelWriterFenceInstalledTree $writerFenceControlRoot $writerSessionId $writerAttemptId $writerTransactionId $false)

    if ($placement -eq 'INSTALL_REQUIRED') {
        if ((Test-Path $install -PathType Container) -and -not $existingVerified) {
            throw 'Existing canonical tree is not eligible for verified replacement.'
        }
        if ($conflictReceiptSupplied) {
            $receiptSource = ReceiptSource $source $sourceManifest
        }
        if (
            [string]$frozenPlacement.helper_sha256 -cne
                [string]$placementCriticalHashes.placement_helper -or
            [string]$frozenPlacement.integrity_sha256 -cne
                [string]$placementCriticalHashes.bootstrap_integrity_helper -or
            [string]$frozenPlacement.writer_fence_sha256 -cne
                [string]$receiptSource.critical_file_sha256.writer_fence_helper
        ) { throw 'Frozen placement helper no longer matches the attested source.' }
        [void](Set-LabelWriterFenceDelegation `
            -ControlRoot $writerFenceControlRoot `
            -Status 'INSTALLING' `
            -SessionId $writerSessionId `
            -AttemptId $writerAttemptId `
            -ReplacementTransactionId $writerTransactionId `
            -DelegationToken $writerDelegationToken `
            -DelegatedSources @('canonical_placement','auto_update_task_mutation','auto_update_restore_preimage',`
            'auto_update_supported_lifecycle_transition','auto_update_persist_switch','auto_update_write_json_durable','auto_update_fixed_helper') `
            -LifetimeSeconds 600)
        $helperParameters = @{
            InstallerRunId = $runId
            InstallerOwnerProcessId = [int]$PID
            InstallerOwnerProcessStartedAtFileTime = $writerOwnerStartedAtFileTime
            ExpectedActiveCommit = $(if ($null -ne $startupLifecycle) { [string]$startupLifecycle.from_commit } else { "" })
            SourceRoot = $source
            InstallRoot = $install
            ElevationLogPath = $elevationLogPath
            ExpectedBootstrapScriptSha256 = [string]$frozenPlacement.helper_sha256
            VerifiedBootstrapScriptPath = [string]$frozenPlacement.helper_path
            BootstrapIntegrityPreloaded = $true
            ExpectedSourceAggregateSha256 = [string]$receiptSource.bootstrap_aggregate_sha256
            ExpectedSourceFileCount = [int]$receiptSource.file_count
            ExpectedSourceByteCount = [uint64]$receiptSource.byte_count
            WriterFenceFunctionsPreloaded = $true
            WriterFenceControlRoot = $writerFenceControlRoot
            DataGenerationSubjectSid = $generationSubjectSid
            DataGenerationMarkerPaths = @($generationMarkerPaths)
            WriterFenceSessionId = $writerSessionId
            WriterFenceAttemptId = $writerAttemptId
            WriterFenceReplacementTransactionId = $writerTransactionId
            WriterFenceDelegationToken = $writerDelegationToken
            AllowNoncanonicalLayoutForTest = [bool]$testMode
            ReplaceExistingVerifiedPortable = [bool]$existingVerified
            DryRun = $false
        }
        if ($existingVerified) {
            $replacementBackupsBefore = @(Get-ChildItem -LiteralPath (Split-Path -Parent $install) -Directory -Force |
                Where-Object { $_.Name -like '.current.rollback.*' } | ForEach-Object { $_.FullName })
        }
        if ($null -ne $legacyRelayBefore) {
            $legacyDirectory=Join-Path (Split-Path -Parent $install) ('.legacy-relay.'+$writerAttemptId)
            $audit.legacy_relay_task_preimage=[ordered]@{path=$legacyDirectory;sha256=$legacyRelayBefore.xml_sha256;enabled=$legacyRelayBefore.enabled;fingerprint=$legacyRelayBefore.fingerprint}
            Save $auditPath $audit
            $transitionParameters=$helperParameters.Clone()
            $transitionParameters.LegacyRelayTransition='Disable'
            $transitionParameters.LegacyRelayPreimageSha256=[string]$legacyRelayBefore.xml_sha256
            $transitionParameters.ReplaceExistingVerifiedPortable=$false
            if ((InvokeFrozenPlacementHelper $frozenPlacement $transitionParameters) -ne 0) { throw 'LEGACY_RELAY_TRANSITION_FAILED' }
            $readback=Get-LabelOfficialLegacyTaskSnapshot
            if ($null -eq $readback -or $readback.enabled -or $readback.fingerprint.sha256 -cne $legacyRelayBefore.fingerprint.sha256) { throw 'LEGACY_RELAY_DISABLED_READBACK_FAILED' }
            $audit.legacy_relay_task_preimage.transition='DISABLED_RETAINED'
        }
        $placementAttempted = $true
        $placementExitCode = InvokeFrozenPlacementHelper $frozenPlacement $helperParameters
        . Save-LabelPlacementRecoveryPreimages
        if ($placementExitCode -ne 0) {
            $audit.placement_child_failure = $Script:LabelPlacementChildFailure
            Save $auditPath $audit
            $preparationDiagnostic = Get-InstallationPreparationDiagnostic $placementExitCode
            if ($null -ne $preparationDiagnostic) {
                $failure = [InvalidOperationException]::new(($preparationDiagnostic.message + ' [' + $preparationDiagnostic.code + ']'))
                $failure.Data['installation_preparation_exit_code'] = $placementExitCode
                throw $failure
            }
            $failure = [InvalidOperationException]::new('Code placement failed.')
            $failure.Data['placement_child_failure'] = $Script:LabelPlacementChildFailure
            throw $failure

        }
        $placement = 'PASS'
    }
    $productInstall = $install
    if ($null -ne $startupLifecycle -and [string]$startupLifecycle.lifecycle_state -ceq 'INSTALLED') {
        $accepted = Read-CanonicalStartupLifecycle $source $install
        $productInstall = [string]$accepted.active_code_root
    }
    $installedManifest = Manifest $productInstall $SkipSignatureValidationForTest
    if ([string]$installedManifest.source_commit -cne [string]$sourceManifest.source_commit) {
        throw 'Installed identity differs.'
    }
    $installedInventory = PortableInventory $productInstall
    if (
        [string]$installedInventory.schema_version -cne [string]$receiptSource.schema_version -or
        [string]$installedInventory.algorithm -cne [string]$receiptSource.algorithm -or
        [int]$installedInventory.file_count -ne [int]$receiptSource.file_count -or
        [uint64]$installedInventory.byte_count -ne [uint64]$receiptSource.byte_count -or
        [string]$installedInventory.sha256 -cne [string]$receiptSource.sha256
    ) { throw 'Installed full portable inventory differs before Product execution.' }
    $startupPreimage = Join-Path (Split-Path -Parent $install) ('.startup-update-install.' + $writerAttemptId)
    if (Test-Path -LiteralPath (Join-Path $startupPreimage 'preimage.json')) {
        $audit.startup_update_preimage = [ordered]@{path=$startupPreimage;sha256=(Sha (Join-Path $startupPreimage 'preimage.json'))}
    }
    $audit.code_placement = $placement
    $audit.code_state = 'PRESENT'
    $audit.runtime_pythonw_sha256 = Sha (Join-Path $install 'runtime\pythonw.exe')
    $audit.runtime_pythonw_signature = [string](
        Get-AuthenticodeSignature (Join-Path $install 'runtime\pythonw.exe')
    ).Status
    Save $auditPath $audit
    if ($EvidencePath) { Save (Full $EvidencePath 'EvidencePath') $audit }

    [void](Set-LabelWriterFenceDelegation `
        -ControlRoot $writerFenceControlRoot `
        -Status 'RESTORING' `
        -SessionId $writerSessionId `
        -AttemptId $writerAttemptId `
        -ReplacementTransactionId $writerTransactionId `
        -DelegationToken $writerDelegationToken `
        -DelegatedSources $restoreSources `
        -LifetimeSeconds 600)
    SetWriterDelegationEnvironment `
        $writerDelegationToken `
        $writerSessionId `
        $writerAttemptId `
        $writerTransactionId

    $started = (Get-Date).ToUniversalTime()
    if ($null -ne $healthyLifecycle) {
        $healthyMode = if ($healthyWithoutCode -and -not $healthyRemovedInstall) { 'inspect' } else { 'release' }
        $audit.healthy_lifecycle = HealthyLifecycle $install $install $healthyMode ([string]$healthyLifecycle.state_sha256)
    }
    $onboardingFailure = $null
    if ($legacyFirstRegistration) {
        $firstRegistrationStarted = $true
        $firstRegistrationAttempt.registration_started = $true
        Save-FirstRegistrationAttempt $firstRegistrationAttempt $firstRegistrationAttemptPath
    }
    try { Product $productInstall '--onboard-current-user' @($onboardingArguments + '--defer-canonical-relay-start') }
    catch { $onboardingFailure = $_ }
    if ($null -ne $onboardingFailure -and -not (Test-Path -LiteralPath $onboardingPath)) {
        throw $onboardingFailure
    }
    $onboarding = Get-Content $onboardingPath -Raw -Encoding UTF8 | ConvertFrom-Json
    $after = Snapshot
    # Registration RECOVERY_REQUIRED is still a pre-confirmation failure.
    # Preserve its report and use the same guarded rollback, never activate it.
    if ($null -ne $onboardingFailure) { throw $onboardingFailure }
    if (
        [string]$onboarding.status -cne 'READY' -or
        [string]$onboarding.relay_autostart.command -cne $wanted -or
        -not $after.exists -or
        [string]$after.data -cne $wanted
    ) { throw 'Onboarding Run readback failed.' }
    if (Test-Path -LiteralPath $stop) { throw 'Relay stop marker survived onboarding.' }

    $officialBeforeRelease=Get-LabelOfficialLegacyTaskSnapshot
    if($null -ne $officialBeforeRelease -and $officialBeforeRelease.enabled){throw 'LEGACY_RELAY_DISABLED_READBACK_FAILED'}
    Confirm-LabelCanonicalPlacement $onboarding $wanted $onboardingPath $started `
        ([string]$env:LABEL_MATCH_ENROLLMENT_TLS_CA_BUNDLE_PATH) $writerFenceControlRoot `
        $writerSessionId $writerAttemptId $writerTransactionId
    $writerFenceStarted = $false
    Exit-LabelWriterSessionAuthority $writerAuthority
    $writerAuthority = $null

    if ([string]$onboarding.relay_start.status -cne 'DEFERRED_CANONICAL_INSTALL') { throw 'Canonical relay was not deferred.' }
    $activeCodeRoot = Join-Path (Join-Path (Split-Path -Parent $install) 'versions') ([string]$installedManifest.source_commit)
    $pidValue = StartRaw (Command $activeCodeRoot)
    $audit.relay_launch_security = Get-CanonicalRelaySecurity $pidValue
    Start-Sleep -Seconds 5
    $process = Get-CimInstance `
        Win32_Process `
        -Filter "ProcessId = $pidValue" `
        -ErrorAction SilentlyContinue
    if (
        $null -eq $process -or
        -not (Same ([string]$process.ExecutablePath) (Join-Path $activeCodeRoot 'runtime\pythonw.exe'))
    ) { throw 'Relay process proof failed.' }

    $deadline = (Get-Date).AddSeconds(75)
    $relay = $null
    $relayPersistentRetry = $false
    while ((Get-Date) -lt $deadline) {
        if (
            (Test-Path $relayPath) -and
            (Get-Item $relayPath).LastWriteTimeUtc -ge $started.AddSeconds(-1)
        ) {
            $relay = Get-Content $relayPath -Raw -Encoding UTF8 | ConvertFrom-Json
            $relayPersistentRetry = Test-RelayPersistentRetry $relay
            if ($relayPersistentRetry) { break }
        }
        Start-Sleep -Milliseconds 500
    }
    if ($null -eq $relay -or -not $relayPersistentRetry) {
        throw 'Fresh relay status proof failed.'
    }

    $officialFinal=Get-LabelOfficialLegacyTaskSnapshot
    if($null -ne $officialFinal -and $officialFinal.enabled){throw 'LEGACY_RELAY_DISABLED_READBACK_FAILED'}
    $audit.status = 'PASS'
    if ($legacyFirstRegistration) {
        $firstRegistrationAttempt.status = 'PASS'
        Save-FirstRegistrationAttempt $firstRegistrationAttempt $firstRegistrationAttemptPath
    }
    $audit.completed_at = (Get-Date).ToUniversalTime().ToString('o')
    $audit.stop_marker_absent = -not (Test-Path $stop)
    $audit.writer_fence.status = 'RELEASED_AFTER_RESTORE'
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
        persistent_retry = $relayPersistentRetry
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
    $forwardFailure = $_
    $unprovenRegistration = $false
    $unprovenHelper = $false
    for ($cause=$forwardFailure.Exception; $null -ne $cause; $cause=$cause.InnerException) {
        if ($cause.Data.Contains('registration_child_unproven')) { $unprovenRegistration=$true }
        if ($cause.Data.Contains('helper_child_unproven')) { $unprovenHelper=$true }
    }
    if ($Script:LabelCanonicalConfirmation -cin @('CONFIRMED','UNCERTAIN') -or $unprovenRegistration -or $unprovenHelper) {
        $audit.status='RECOVERY_REQUIRED'
        $audit.reason_code=if($unprovenHelper){'HELPER_CHILD_UNPROVEN'}elseif($unprovenRegistration){'REGISTRATION_CHILD_UNPROVEN'}elseif($Script:LabelCanonicalConfirmation -ceq 'CONFIRMED'){'POST_CONFIRMATION_FAILED'}else{'CONFIRMATION_UNCERTAIN'}
        $audit.failure_details=Get-LabelLaunchFailure $forwardFailure 'post_confirmation'
        $audit.code_state=if($unprovenHelper){'UNKNOWN'}else{'PRESENT'};$audit.rollback.applied=$false
        $audit.recovery_guidance='새 판과 자료를 보존했습니다. 보호 보고서를 확인하고 같은 판의 지원 복구를 실행하세요.'
        if ($Script:LabelCanonicalConfirmation -ceq 'CONFIRMED' -and -not $unprovenRegistration -and -not $unprovenHelper) {
            $retryPython=(Join-Path $install 'runtime\python.exe').Replace("'","''")
            $retryMain=(Join-Path $install 'app\main.py').Replace("'","''")
            $retryRoot=$install.Replace("'","''")
            $audit.recovery_command="& '$retryPython' -I -B '$retryMain' --onboard-current-user --app-root '$retryRoot'"
            $audit.recovery_guidance='새 판과 자료를 보존했습니다. 보고서의 원인을 해결한 뒤 같은 작업 사용자의 PowerShell에서 recovery_command로 같은 판의 사용자 준비를 재시도하세요. 성공 뒤 앱을 다시 시작하세요.'
        }
        Save $auditPath $audit
        if($EvidencePath){Save (Full $EvidencePath 'EvidencePath') $audit}
        'install_status=RECOVERY_REQUIRED'
        "audit_path=$auditPath"
        if ($audit.Contains('recovery_command')) { "recovery_command=$($audit.recovery_command)" }
        throw $forwardFailure
    }
    if($null -ne $Script:LabelPlacementSession){$Script:LabelPlacementSession.rollback=$true}
    'install_status=FAILED'
    if (-not $mutated -and $null -ne $startupLifecycle -and [string]$startupLifecycle.lifecycle_state -ceq 'INSTALLED') {
        $audit.status = 'FAILED_SUPPORTED_REMOVAL_REQUIRED'
        $audit.failure = $_.Exception.GetType().Name
        $audit.failure_details = Get-LabelLaunchFailure $_ 'forward'
        $audit.recovery_guidance = '자료와 등록 및 전송 대기를 보존하고 지원 제거를 먼저 실행한 뒤 다시 설치하세요.'
        Save $auditPath $audit
        throw ([string]$audit.failure_details.guidance)
    }
    $original = $_
    if ($placementAttempted) {
        try { . Save-LabelPlacementRecoveryPreimages }
        catch { $audit.rollback.preimage_capture_failure = Get-LabelLaunchFailure $_ 'preimage_capture' }
    }
    if ((Get-Variable -Name LabelPlacementChildFailure -Scope Script -ErrorAction SilentlyContinue) -and $null -ne $Script:LabelPlacementChildFailure) { $original.Exception.Data['placement_child_failure'] = $Script:LabelPlacementChildFailure }
    $audit.failure_type = $original.Exception.GetType().Name
    $audit.failure_details = Get-LabelLaunchFailure $original 'forward'
    Save-LabelRollbackContext
    try { Save $auditPath $audit }
    catch { $audit.rollback.initial_audit_failure = Get-LabelLaunchFailure $_ 'initial_audit_save' }
    if (-not $mutated) { throw ([string]$audit.failure_details.guidance) }
    $rollbackFailures = New-Object 'System.Collections.Generic.List[object]'
    $rollbackRuntimeVerified = $false
    $rollbackRuntimePreimageVerified = $false
    $audit.rollback.code_restored = $false
    try {
        . Invoke-LabelCanonicalRollback
        if($null -ne $Script:LabelPlacementSession){Close-LabelPlacementSession $Script:LabelPlacementSession 'REVERTED'}
        $audit.status = 'FAILED_ROLLED_BACK'
        if ($legacyFirstRegistration) {
            $firstRegistrationAttempt.status = 'FAILED_ROLLED_BACK'
            $firstRegistrationAttempt.remaining_paths = @()
            $firstRegistrationAttempt.old_app_state = 'ABSENT'
            Save-FirstRegistrationAttempt $firstRegistrationAttempt $firstRegistrationAttemptPath
        }
        $audit.rollback.applied = $mutated
        $audit.rollback.runtime_restored = $true
        $audit.failure_type = $original.Exception.GetType().Name
        $audit.failure_details = Get-LabelLaunchFailure $original 'forward'
        Save $auditPath $audit
        if ($EvidencePath) { Save (Full $EvidencePath 'EvidencePath') $audit }
    }
    catch {
        $rollbackFailure = $_
        try {
            $audit.status = if ($legacyFirstRegistration -and -not $rollbackRuntimeVerified) { 'FAILED_ROLLBACK_INCOMPLETE' } else { 'ROLLBACK_FAILED' }
            $audit.code_state = if ($null -ne $Script:LabelPlacementSession -and $Script:LabelPlacementSession.child_exit_unproven) { 'UNKNOWN' } elseif (Test-Path -LiteralPath $install) { 'PRESENT' } else { 'ABSENT' }
            $audit.rollback.applied = $mutated
            $audit.rollback.runtime_restored = $rollbackRuntimeProven -and -not $writerFenceStarted
            $audit.rollback.failure_type = $rollbackFailure.Exception.GetType().Name
            $audit.rollback.failure_details = Get-LabelLaunchFailure $rollbackFailure 'rollback'
            $runReadback=Snapshot
            if (-not $writerFenceStarted -and [bool]$runReadback.exists -eq [bool]$before.exists -and [string]$runReadback.kind -ceq [string]$before.kind -and [string]$runReadback.data -ceq [string]$before.data) {
                $audit.rollback.relay_recovery_status='RUN_RESTORED_RELAY_NEXT_LOGON'
                $audit.rollback.guidance='자동 시작 설정은 복원됐습니다. 다음 로그온 때 전송을 재개하며 설치 결과는 실패입니다.'
            }
            $audit.failure_type = $original.Exception.GetType().Name
        $audit.failure_details = Get-LabelLaunchFailure $original 'forward'
            if ($legacyFirstRegistration) {
                if (-not $writerFenceStarted) {
                    # A final cleanup can throw before returning its readback.
                    # Retain custody for exceptions as well as reported residue.
                    [void](Start-LabelWriterFence -ControlRoot $writerFenceControlRoot -Status 'RESTORING' `
                        -SessionId $writerSessionId -AttemptId $writerAttemptId `
                        -ReplacementTransactionId $writerTransactionId -SessionStartedAtUtc $writerStartedAt `
                        -OrchestratorSha256 $writerOrchestratorSha256 -WriterContractSha256 $writerContractSha256 `
                        -AuthorityOwnedByCaller)
                    $writerFenceStarted = $true
                    [void](Set-LabelWriterFenceInstalledTree $writerFenceControlRoot $writerSessionId $writerAttemptId $writerTransactionId $true)
                    $audit.writer_fence.status = 'RESTORING_AFTER_CLEANUP_EXCEPTION'
                }
                $readback = @(FirstRegistrationAbsence $firstRegistrationStatePaths)
                $audit.rollback.active_state_readback = $readback
                $remaining = @($readback | Where-Object { $_.state -cne 'ABSENT' })
                $ancestorReadback = @(FirstRegistrationAbsence @($firstRegistrationAbsentAncestors | Where-Object {
                    -not ((Same $_ $localAuditRoot) -or
                        $localAuditRoot.StartsWith(([string]$_).TrimEnd('\') + '\', [StringComparison]::OrdinalIgnoreCase))
                }))
                $audit.rollback.ancestor_readback = $ancestorReadback
                $remaining += @($ancestorReadback | Where-Object { $_.state -cne 'ABSENT' })
                $firstRegistrationAttempt.status = 'FAILED_ROLLBACK_INCOMPLETE'
                $firstRegistrationAttempt.remaining_paths = $remaining
                $core = @((Join-Path $defaultDirectSyncRoot 'producer_identity.json'),
                    (Join-Path $defaultDirectSyncRoot 'producer_manifest.json'),
                    (Join-Path $defaultDirectSyncRoot 'credential.json'),
                    (Join-Path $defaultDirectSyncRoot 'status\label_match_worker_pc_registration.json'),
                    $selectedProfilePath, (Join-Path (Split-Path -Parent $selectedProfilePath) 'secrets\bearer-token.dpapi'))
                $coreReadback = @(FirstRegistrationAbsence $core)
                $present = @($coreReadback | Where-Object { $_.state -ceq 'PRESENT' -and $_.kind -ceq 'FILE' }).Count
                $firstRegistrationAttempt.old_app_state = if (@($coreReadback | Where-Object { $_.state -ceq 'UNKNOWN' }).Count -ne 0) { 'UNPROVEN_STORAGE_READ_ERROR' }
                    elseif (@($coreReadback | Where-Object { $_.state -ceq 'PRESENT' -and $_.kind -cne 'FILE' }).Count -ne 0) { 'RECOVERY_REQUIRED' }
                    elseif ($present -eq 0) { 'ABSENT' }
                    elseif ($present -eq 1 -and $coreReadback[3].state -ceq 'PRESENT' -and $coreReadback[3].kind -ceq 'FILE') { 'UNPROVEN_REPORT_ONLY_STATE' }
                    elseif ($present -lt $core.Count) { 'RECOVERY_REQUIRED' } else { 'UNPROVEN_COMPLETE_STATE' }
                $audit.rollback.old_app_state = $firstRegistrationAttempt.old_app_state
                Save-FirstRegistrationAttempt $firstRegistrationAttempt $firstRegistrationAttemptPath
            }
            Save $auditPath $audit
            if ($EvidencePath) { Save (Full $EvidencePath 'EvidencePath') $audit }
        }
        catch {
            throw (
                'ROLLBACK_AUDIT_PERSISTENCE_FAILED: ' + $_.Exception.GetType().Name +
                '; rollback=' + $rollbackFailure.Exception.GetType().Name
            )
        }
        throw ([string]$audit.rollback.failure_details.guidance)
    }
    throw ([string]$audit.failure_details.guidance)
}
finally {
    # Keep the first cause and every attempted rollback phase in the same audit.
    try {
        if ($null -ne $original -and (Get-Variable -Name LabelRollbackSteps -Scope Script -ErrorAction SilentlyContinue)) {
            $audit.rollback.steps = $Script:LabelRollbackSteps.ToArray()
            $audit.rollback.step_failures = $Script:LabelRollbackFailures.ToArray()
            Save $auditPath $audit
        }
    } finally {
        if($null -ne $Script:LabelPlacementSession -and -not $Script:LabelPlacementSession.closed){
            try {Close-LabelPlacementSession $Script:LabelPlacementSession 'ATTENTION'} catch {
                $audit.guardian_failure=Get-LabelLaunchFailure $_ 'guardian_close'
                Save $auditPath $audit
            }
        }
        $Script:LabelPlacementSessionRequested=$false
        RestoreWriterDelegationEnvironment $writerEnvironmentBefore
        if ($null -ne $writerAuthority) {
            Exit-LabelWriterSessionAuthority $writerAuthority
            $writerAuthority = $null
        }
        $Script:LabelWriterFenceInstalledIdentity = $null
    }
}
