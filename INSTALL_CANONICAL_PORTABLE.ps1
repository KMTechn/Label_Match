[CmdletBinding()]
param(
    [string]$SourceRoot = "",
    [string]$InstallRoot = "C:\KMTech\Apps\Label_Match\current",
    [string]$EvidencePath = "",
    [string]$ServerBaseUrl = "",
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
function Get-LabelSharedPortableLeafPath([string]$CodeRoot) {
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
        'kmtech_shared.lock.json', 'kmtech_shared.manifest.json', 'kmtech_shared\powershell\portable.ps1')) {
        $path = if ($relative) { Join-Path $appRoot $relative } else { $appRoot }
        $item = Get-Item -LiteralPath $path -Force
        if (($item.Attributes -band [IO.FileAttributes]::ReparsePoint) -ne 0) {
            throw "Shared PowerShell path contains a reparse point: $path"
        }
    }
    $expectedManifestSha = 'feaed459688e915eb5f52f264501a4287261d9dcbdbc50e0c6f4bde59d8e7117'
    $lockPath = Join-Path $appRoot 'kmtech_shared.lock.json'
    if ((Get-Item -LiteralPath $lockPath).Length -gt 65536) { throw 'Shared consumer lock is oversized.' }
    $lock = Get-Content -LiteralPath $lockPath -Raw -Encoding UTF8 | ConvertFrom-Json
    if ([string]$lock.version -cne '0.3.1' -or [string]$lock.manifest_sha256 -cne $expectedManifestSha) {
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
    $sharedLeaf = Join-Path $appRoot 'kmtech_shared\powershell\portable.ps1'
    $expectedLeafSha = $pinnedSharedManifest.files.'kmtech_shared/powershell/portable.ps1'
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
    $receiptPath = Full $receiptPathValue 'conflict-resolution receipt path'
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

function Command([string]$Root) {
    return ('{0} -I -B {1} --label-match-user-relay' -f
        (Arg (Join-Path $Root 'runtime\pythonw.exe')),
        (Arg (Join-Path $Root 'app\main.py')))
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
    ) { throw 'ServerBaseUrl must be a credential-free https://host[:port] origin.' }
    if ($Matches.ContainsKey('port') -and [int]$Matches['port'] -gt 65535) {
        throw 'ServerBaseUrl port must be within 1-65535.'
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

function Assert-WriterTransition([string]$Source, [string]$Installed) {
    # Run only the attested candidate's scanner. Never import installed code.
    # Changed semantics require an exact, reviewed directional release pair;
    # otherwise AST equality admits only comments/line movement.
    $probe = @'
import ast, hashlib, json, pathlib, re, sys
source, installed = map(pathlib.Path, sys.argv[1:])
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
        '32f34fccf9a00e5d27e852a95fc7b776f18a5eccda991f47dc33cd8111d50295'),
    'app/kmtech_shared.manifest.json': (
        '465eea8ad20f010e29be5bb98cbc32fdcd1d163d5ba7b76d9eac3b33c14874ee',
        'feaed459688e915eb5f52f264501a4287261d9dcbdbc50e0c6f4bde59d8e7117'),
    'app/kmtech_shared.lock.json': (
        '6807ed97f7d734c5d8bdb0e23c141916cc5f013bd2392a46b98849d285d54fcd',
        '87cf641db77b8052ffbaeee86d0c2cbab84f07ce4c3748c796985be2eacc0cf9'),
    'app/kmtech_zero_pe.vendor.json': (
        'e7dac78746db6efbccedcba5c3b44b58b77bc44e8d25a2dbaea6dc1ac01e307c',
        'bf7ccf44e5abd14b3211bbea9b1e22d3b4a53525b4d4bc760c1893ca2da63a69'),
    'INSTALL_THIS_PC.ps1': (
        '7b9e32726e81185df274433d80177f3b59bf3b87291c641cebc51e4a6863250b',
        '2401e30f7d7c5965360bfcb1e1ebc3ef71fe30491eb8ee7e9f84dd2a8e1e5770'),
    'tools/bootstrap_integrity.ps1': (
        '7094e69137179c2fe66119a19358db66b375a4db45a8abdf55965bc75f7a37e2',
        '7cf77e9cc833093aed9ed7b3d79565f5396eed0288b1e7729e08959e82e12ff3'),
}
shared_additions = {
    'app/kmtech_shared/powershell/portable.ps1':
        'fb2e506d2194eb54f526f4aa73515ebd226278a3f4f44cbc3854de91186781d1',
}

def release_digest(path):
    return hashlib.sha256(path.read_bytes().replace(b'\r\n', b'\n')).hexdigest()

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
        '02bcbd5b2ed79b230dbc8a78add9d92457ef3f8076978c314fbda27b24a73ab7'
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
legacy_label_replacements = {'app/label_data_manager.py': ('95887ca72e74aa258ba07584e1df14fc099f4b16998629d4639e4f5749ecc757', 'eb4f4bef03152c05c6ef160abb7738dfa8416c971bb79baac4092d52905c14ec'),
 'app/Label_Match.py': ('0481e687828da9e99aebad238307586c73f9cd0db8ea53e39603029fbcad921b',
                        '7ea75f682a1fc2cf0ea2c1cc020e85d85bf20ab51a0fd0a4b2a42c4a3449d5a6'),
 'app/carrier_identity_port.py': ('08e470a49aef5213f458f352df2118cf79e01160ba2b0544091161358717584d',
                                  '3690500795de424c928570593debb4afcfc50d1175f2ccba77f44fb56c97979b'),
 'app/deferred_intent_capture.py': ('e129488f303e001293431e8b45a0edc10bab22a7e9cb7e96782aa6d3df260561',
                                    '970c6565a3f5f589f15d9cc2fb0ff1e5980e772d977976152b93f3c7e6e41dbd'),
 'app/direct_sync_push.py': ('21bf3ba0500879209d543f64d2c7c63d74a17a0874e3508dfdf75f8f586290c7',
                             'd9c21b3209dc35e380daeb045a51e84dd81fb99893b471e83711116ef9d06e6d'),
 'app/label_completion.py': ('25fb17bff710478898ea1b16ab08538b2be74827d4aaac48ed7631364cfd7d3d',
                             '7650944f6d9f7431d8089b373a32aa649c324ecacfb8c24b40f13ccc2f2fdb55'),
 'app/label_recovery_schema.py': ('52bd27f8c34dc4bbd343e3d695a8c8f2deabb19789989957eb9aaa0bbdc68d90',
                                  '01fdfd2616bdc6a2a713ed89f32f23b73db9fe2d2e86d90482023e9950a178f7'),
 'app/label_workbench_context.py': ('95b57100a5bf2014a8dc7685455342233fb8bcd9f78e04a92332ae2fab320422',
                                    '32835bfbd588afd84fae494aabeab2add65f2ecb0b22aa0bb2478951107afe51'),
 'app/label_workbench_history.py': ('214ee81fdc36cfc51d6b59c15ee69f4f5057ebfc899eced18f7b61c0daff50f8',
                                    '6f07e89f69d66bb979abaef58aa4ff514fb839756bc9f4b5b727b5c0cf4d2022'),
 'app/logistics_runtime_profile.py': ('a5d6e73f0a64233c1dacd006d3b724da0b5a88eeb818b93d90f396af0664057f',
                                      '1d786cb74812b1b4588d122b333cc9b8724d28e7410f91c7c0f025d9fd1bd5ee'),
 'app/package_command_draft.py': ('7f296c95fbcdfdd117eb275068fcb59e3dcc91f63112fb0e7b8fc421a2a3e151',
                                  '4936fdccd3af20e7f48c0fc0bdb3eb41e6baee073bf8dea90252adb487c60563'),
 'app/package_logistics.py': ('818a1d7fa228e125c30c23f6950b6d579c69c22fda1bd9f2d9aded9e9b4201a1',
                              '4585720051d266fa077005456b0da590b273ad8ab29bc5391cff2e6aa9f74224'),
 'app/package_outbox.py': ('39b09189d4afddcb4bf937a8d300573391281092b6e62ac828ef41f1f72b6437',
                           'dc36a36e1697d1d3a42c5f0b4bf147791fa6687444a80d912778c83bd049f683'),
 'app/package_outbox_processor.py': ('4589f804c3d57e56ad93066d83d8c122b3d3eec9510aee9c2152375f8c0c3f79',
                                     'b7dc5263d0d41715b82c3e8c54e95951007eedbefcb9a4a90900aa3a7407a0e3'),
 'app/sealed_transfer_exchange.py': ('f0452daf434ae11710a9e5cfa825478d9ceb552fadc804be269e5e0b2d26fca2',
                                     '74c5d2c126537b316b2f9d8d5127853c1e4217448fe3a2656f551eaafe529d1c'),
 'app/terminal_operation_lease.py': ('4e30d014f82a2dea766ef735c39a3b9c0f4b9e4c521ce76b25c9ac8cfb3927b7',
                                     'a1c5a01061c87b482f49707995f85672279efa190ebcc97bea3ff74f32f77bfa'),
 'app/ui/workflow_snapshot_adapter.py': ('f325774fb1e6c61547c140a4611f5b52958dbe1ce1e89763ce3a534e0bb37411',
                                         '661d0167c8966d510dc85233e32b4ee5dda781ff3d9b4fca20a1f0c797a5a4d2'),
 'app/ui/workflow_view_state.py': ('6959c71768296a68d88de2d46a76b80f177ab7c5e2bff0fde0cddb3fa9ada718',
                                   'b914aece96760c7e149876d56ccd24b8efae2f33e68a23a3a5a183e1cdfc5242'),
 'app/enrollment_mutex.py': ('040294978e4341d9f06573f9117d99913f1dab8ef85de5f08a5296228fbb9787',
                             'ce77c0a1ab113326dffdb611a74c19308a48fc1b113626509960614fd3295981'),
 'app/writer_session_fence.py': ('8e3a3dcacca554f255ca41a0b70a208f237595b34dc8e7aa5db5b2c6fdf6c73c',
                                 'f9c4bc88bad29655a30e17c5234835850fb32a76279036d47e52b1fb7edd30dc'),
 'app/tools/install_logistics_runtime_profile.py': ('7be760a5e49054740b495e62d48da98259b7e40eb288d1a46604cf8201994122',
                                                    'eb8650ace3df179a7c68f3247efe5eac86200543a43f4b2c80a47ccc95b336ef'),
 'app/label_match_single_instance.py': ('3c69b65c5091c9d55bd28bebacad2a1fae5801a663cc7f762dacf1c3c68043e7',
                                        '64040b936adb201d21ecbf55d47536b545a8c10048200a4337635f0a7b00bb02'),
 'app/current_user_onboarding.py': ('323c4100d3ee0d32077232be2656209daa581f9e19ca018d3e840009d03dc26e',
                                    '84fde787fe1bca5d3138c3cd243d9b13ffaf834ffc69a5414b39d0c2b81f905d')}
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
  '7ea75f682a1fc2cf0ea2c1cc020e85d85bf20ab51a0fd0a4b2a42c4a3449d5a6'),
 ('78968d1946644a76542bb011bea5881403bcdb4f6482de1497b4f7c0921a261a',
  '7ea75f682a1fc2cf0ea2c1cc020e85d85bf20ab51a0fd0a4b2a42c4a3449d5a6'),
 ('b30d80d46cddd039ca2475ddeb1be0465fe6e3b67479e7ed1c8e8f0c4451813d',
  '7ea75f682a1fc2cf0ea2c1cc020e85d85bf20ab51a0fd0a4b2a42c4a3449d5a6')}
scan_input_companions = {'app/label_data_manager.py': ('95887ca72e74aa258ba07584e1df14fc099f4b16998629d4639e4f5749ecc757', 'eb4f4bef03152c05c6ef160abb7738dfa8416c971bb79baac4092d52905c14ec'),
 'app/carrier_identity_port.py': ('08e470a49aef5213f458f352df2118cf79e01160ba2b0544091161358717584d',
                                  '3690500795de424c928570593debb4afcfc50d1175f2ccba77f44fb56c97979b'),
 'app/deferred_intent_capture.py': ('e129488f303e001293431e8b45a0edc10bab22a7e9cb7e96782aa6d3df260561',
                                    '970c6565a3f5f589f15d9cc2fb0ff1e5980e772d977976152b93f3c7e6e41dbd'),
 'app/label_completion.py': ('aaac26de64e050b07b2225723898c8c32e52f6526039202ffd8d2daa12f162b8',
                             '7650944f6d9f7431d8089b373a32aa649c324ecacfb8c24b40f13ccc2f2fdb55'),
 'app/label_recovery_schema.py': ('e783a8c7ecac94dd415810f2891161967ec8f13a802d74d7da466e646986d462',
                                  '01fdfd2616bdc6a2a713ed89f32f23b73db9fe2d2e86d90482023e9950a178f7'),
 'app/label_transition.py': ('001001427e7352c308b351263770b2649aa96898b712396d8ae3ae4530186cf9',
                             '27c5952d804558211c7db05ac3489e4bd0422599a259af6b287fec5863f09279'),
 'app/label_workbench_history.py': ('214ee81fdc36cfc51d6b59c15ee69f4f5057ebfc899eced18f7b61c0daff50f8',
                                    '6f07e89f69d66bb979abaef58aa4ff514fb839756bc9f4b5b727b5c0cf4d2022'),
 'app/package_command_draft.py': ('7f296c95fbcdfdd117eb275068fcb59e3dcc91f63112fb0e7b8fc421a2a3e151',
                                  '4936fdccd3af20e7f48c0fc0bdb3eb41e6baee073bf8dea90252adb487c60563'),
 'app/package_logistics.py': ('818a1d7fa228e125c30c23f6950b6d579c69c22fda1bd9f2d9aded9e9b4201a1',
                              '4585720051d266fa077005456b0da590b273ad8ab29bc5391cff2e6aa9f74224'),
 'app/package_outbox.py': ('39b09189d4afddcb4bf937a8d300573391281092b6e62ac828ef41f1f72b6437',
                           'dc36a36e1697d1d3a42c5f0b4bf147791fa6687444a80d912778c83bd049f683'),
 'app/package_outbox_processor.py': ('4589f804c3d57e56ad93066d83d8c122b3d3eec9510aee9c2152375f8c0c3f79',
                                     'b7dc5263d0d41715b82c3e8c54e95951007eedbefcb9a4a90900aa3a7407a0e3'),
 'app/sealed_transfer_exchange.py': ('f0452daf434ae11710a9e5cfa825478d9ceb552fadc804be269e5e0b2d26fca2',
                                     '74c5d2c126537b316b2f9d8d5127853c1e4217448fe3a2656f551eaafe529d1c'),
 'app/terminal_operation_lease.py': ('4e30d014f82a2dea766ef735c39a3b9c0f4b9e4c521ce76b25c9ac8cfb3927b7',
                                     'a1c5a01061c87b482f49707995f85672279efa190ebcc97bea3ff74f32f77bfa'),
 'app/ui/workflow_view_state.py': ('45066b34a856ca47c3aa7f10b44f72e948a2db5827a8f6da948eeb7d6a8a5e0a',
                                   'b914aece96760c7e149876d56ccd24b8efae2f33e68a23a3a5a183e1cdfc5242')}
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
 '7ea75f682a1fc2cf0ea2c1cc020e85d85bf20ab51a0fd0a4b2a42c4a3449d5a6')
nextfix_companions = {'app/label_data_manager.py': ('95887ca72e74aa258ba07584e1df14fc099f4b16998629d4639e4f5749ecc757', 'eb4f4bef03152c05c6ef160abb7738dfa8416c971bb79baac4092d52905c14ec'),
 'app/carrier_identity_port.py': ('08e470a49aef5213f458f352df2118cf79e01160ba2b0544091161358717584d',
                                  '3690500795de424c928570593debb4afcfc50d1175f2ccba77f44fb56c97979b'),
 'app/label_completion.py': ('d0fb68fe1d531d6ac7436c8a778c7c93d6d1dbd4da0918614d95414ee5861492',
                             '7650944f6d9f7431d8089b373a32aa649c324ecacfb8c24b40f13ccc2f2fdb55'),
 'app/label_recovery_schema.py': ('e783a8c7ecac94dd415810f2891161967ec8f13a802d74d7da466e646986d462',
                                  '01fdfd2616bdc6a2a713ed89f32f23b73db9fe2d2e86d90482023e9950a178f7'),
 'app/label_transition.py': ('001001427e7352c308b351263770b2649aa96898b712396d8ae3ae4530186cf9',
                             '27c5952d804558211c7db05ac3489e4bd0422599a259af6b287fec5863f09279'),
 'app/label_workbench_history.py': ('214ee81fdc36cfc51d6b59c15ee69f4f5057ebfc899eced18f7b61c0daff50f8',
                                    '6f07e89f69d66bb979abaef58aa4ff514fb839756bc9f4b5b727b5c0cf4d2022'),
 'app/package_command_draft.py': ('7f296c95fbcdfdd117eb275068fcb59e3dcc91f63112fb0e7b8fc421a2a3e151',
                                  '4936fdccd3af20e7f48c0fc0bdb3eb41e6baee073bf8dea90252adb487c60563'),
 'app/package_logistics.py': ('818a1d7fa228e125c30c23f6950b6d579c69c22fda1bd9f2d9aded9e9b4201a1',
                              '4585720051d266fa077005456b0da590b273ad8ab29bc5391cff2e6aa9f74224'),
 'app/package_outbox.py': ('39b09189d4afddcb4bf937a8d300573391281092b6e62ac828ef41f1f72b6437',
                           'dc36a36e1697d1d3a42c5f0b4bf147791fa6687444a80d912778c83bd049f683'),
 'app/package_outbox_processor.py': ('4589f804c3d57e56ad93066d83d8c122b3d3eec9510aee9c2152375f8c0c3f79',
                                     'b7dc5263d0d41715b82c3e8c54e95951007eedbefcb9a4a90900aa3a7407a0e3'),
 'app/sealed_transfer_exchange.py': ('f0452daf434ae11710a9e5cfa825478d9ceb552fadc804be269e5e0b2d26fca2',
                                     '74c5d2c126537b316b2f9d8d5127853c1e4217448fe3a2656f551eaafe529d1c'),
 'app/ui/workflow_view_state.py': ('45066b34a856ca47c3aa7f10b44f72e948a2db5827a8f6da948eeb7d6a8a5e0a',
                                   'b914aece96760c7e149876d56ccd24b8efae2f33e68a23a3a5a183e1cdfc5242')}
nextfix_label_transition = (
    (installed / nextfix_label_path).is_file() and
    (source / nextfix_label_path).is_file() and
    (release_digest(installed / nextfix_label_path),
     release_digest(source / nextfix_label_path)) == nextfix_label_pair
) and all((installed / relative).is_file() and (source / relative).is_file() and release_digest(installed / relative) == before and release_digest(source / relative) == after for relative, (before, after) in nextfix_companions.items())

# a20cd01 -> this scanner source: exact forward-only source pair, the same
# pinned companion deltas, and unchanged writer membership/runtime/contracts.
scanner_label_pair = ('7324ab7b111d940762c4acb20cdc9348ed6f176d8d7e4f7e52bd3d9c9d50db57', '7ea75f682a1fc2cf0ea2c1cc020e85d85bf20ab51a0fd0a4b2a42c4a3449d5a6')
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
    'app/writer_session_fence.py': ('825d5337385e3870ab4d83db1b63d9dea4d1fc9e9c528d7948415ca20256ae87', 'f9c4bc88bad29655a30e17c5234835850fb32a76279036d47e52b1fb7edd30dc'),
    'app/tools/install_logistics_runtime_profile.py': ('7be760a5e49054740b495e62d48da98259b7e40eb288d1a46604cf8201994122', 'eb8650ace3df179a7c68f3247efe5eac86200543a43f4b2c80a47ccc95b336ef'),
    'app/label_match_single_instance.py': ('3c69b65c5091c9d55bd28bebacad2a1fae5801a663cc7f762dacf1c3c68043e7', '64040b936adb201d21ecbf55d47536b545a8c10048200a4337635f0a7b00bb02'),
    'app/current_user_onboarding.py': ('323c4100d3ee0d32077232be2656209daa581f9e19ca018d3e840009d03dc26e', '84fde787fe1bca5d3138c3cd243d9b13ffaf834ffc69a5414b39d0c2b81f905d'),
    'app/logistics_runtime_profile.py': ('2cd3fe00a34e79a6c12a995f74ba99515f88fff4e9bef50e844ab02dc59da1ed', '1d786cb74812b1b4588d122b333cc9b8724d28e7410f91c7c0f025d9fd1bd5ee'),
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
                             'd9c21b3209dc35e380daeb045a51e84dd81fb99893b471e83711116ef9d06e6d'),
 'app/label_admin_pin.py': ({'8e5fb578366c38012792f5a3213310d6d442e4ff68202edf84e4983872f13bf0'},
                            '25ff059f390f88166e74518a8027633ab75d6954ceb8f8ee74593725bf828f75'),
 'app/package_logistics.py': ({'818a1d7fa228e125c30c23f6950b6d579c69c22fda1bd9f2d9aded9e9b4201a1'},
                              '4585720051d266fa077005456b0da590b273ad8ab29bc5391cff2e6aa9f74224'),
 'app/package_outbox_processor.py': ({'4589f804c3d57e56ad93066d83d8c122b3d3eec9510aee9c2152375f8c0c3f79'},
                                     'b7dc5263d0d41715b82c3e8c54e95951007eedbefcb9a4a90900aa3a7407a0e3'),
 'app/phs_label_workflow.py': ({'c1746fe550017bccdf8b8fd6b98ed1438cd49b5878d98328cceec7d6cb6edd7c'},
                               '0d44faf4128286d49fc4285d26b9901d563c86eba0fe62d669c53a4a382c7b7d'),
 'app/producer_runtime_client.py': ({'c5c111e72d150395b825ac66e79208540560495703039d0ea4c4f215eb45b2f9'},
                                    'e47cd333cc3c743fe9cd30a6de4069929773e86bfbb23babdeaff287564e8f17'),
 'app/sealed_transfer_exchange.py': ({'f0452daf434ae11710a9e5cfa825478d9ceb552fadc804be269e5e0b2d26fca2'},
                                     '74c5d2c126537b316b2f9d8d5127853c1e4217448fe3a2656f551eaafe529d1c'),
 'app/user_relay.py': ({'07291437ab724a9512332b25666099f13170af2f12f5f62328ea6af54cad96e7'},
                       '3b71cb47a1a4a3253ac0602804e5770e40e6b674004174838d3a00016c3e8487'),
 'app/writer_session_fence.py': ({'7e26bf0c1cddd87df8fdffb5921887f8034976269308c8d5920543c30ba38ce1', '825d5337385e3870ab4d83db1b63d9dea4d1fc9e9c528d7948415ca20256ae87', '8e3a3dcacca554f255ca41a0b70a208f237595b34dc8e7aa5db5b2c6fdf6c73c', '96d32f3eab331cc14ccef610a5523d938ff34372f0ba1cfd9a5a6f9d779ee3ce', 'b38cd124a736dfd2d020b0401b409f95049ee99444795cd5fe13f309bbaf42b7', 'da8873b44fd4fc8c69cc21b17f7e53695630c807d70b48c4ab653a26e73d1a50'},
                                 'f9c4bc88bad29655a30e17c5234835850fb32a76279036d47e52b1fb7edd30dc')}
remote_transport_transition = (
    legacy_label_transition or scan_input_transition or nextfix_label_transition
    or scanner_transition or current_user_profile_transition
) and all(
    (installed / relative).is_file() and (source / relative).is_file()
    and release_digest(installed / relative) in befores
    and release_digest(source / relative) == after
    for relative, (befores, after) in remote_transport_replacements.items()
)

roster_additions = {'app/label_worker_roster.py': '09e9621710cb4027b7c714ff6a105572a81eb25232954d0a900798bd86f0e7aa'}
# This feature's exact reviewed branch baseline is independent of the older
# accepted release tables; never broaden their historical preimages.
roster_baseline_replacements = {'app/Label_Match.py': ('6a516d5aa46847547f0355f3505447bcd3ae86abd521640d0691d9ac3e962062',
                        '7ea75f682a1fc2cf0ea2c1cc020e85d85bf20ab51a0fd0a4b2a42c4a3449d5a6'),
 'app/label_admin_pin.py': ('159b7f0b79c6bb54d56857140d8601424e0c2ef386bd9d16e08c784a8545ebb8',
                            '25ff059f390f88166e74518a8027633ab75d6954ceb8f8ee74593725bf828f75'),
 'app/writer_session_fence.py': ('8d7ae33010ffce1645868060c0bb049ca1f554781c37de03a678dd712e5d8dd6',
                                 'f9c4bc88bad29655a30e17c5234835850fb32a76279036d47e52b1fb7edd30dc'),
 'tools/label_writer_fence.ps1': ('2795ccf12f76c7619fda186762303e02539714b7c33fbef49b65e89a1d06d67a',
                                  '20df3336cbd6c1c86297716b725002c53e0d49d0a380eb0d4cf2261940e9cc5b')}
roster_baseline_transition = all(
    (installed / relative).is_file() and (source / relative).is_file()
    and release_digest(installed / relative) == before
    and release_digest(source / relative) == after
    for relative, (before, after) in roster_baseline_replacements.items()
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
candidate_pin, candidate_sources = identity(source)
installed_pin, installed_sources = identity(installed)
if roster_transition:
    # This exact signed-transport pair moves the existing decorated request
    # into the common route helper. Every other writer identity stays equal;
    # executable deltas still require the complete reviewed release below.
    old_request = ('label_admin_pin_request', 'label_admin_pin.py', 'AdminPinClient.post', 'decorator')
    new_request = ('label_admin_pin_request', 'label_admin_pin.py', 'AdminPinClient._post_path', 'decorator')
    installed_sources = [new_request if row == old_request else row for row in installed_sources]
if candidate_sources != installed_sources:
    raise ValueError('WRITER_TRANSITION_SOURCE_SET_DIFFERS')
for directory in ('app', 'runtime'):
    left, right = files(source, directory), files(installed, directory)
    additions = set(shared_additions) if shared_transition and directory == 'app' else set()
    if legacy_label_transition and directory == 'app':
        additions |= set(legacy_label_additions)
    if roster_transition and directory == 'app':
        additions |= set(roster_additions)
    if left.keys() != right.keys() | additions:
        raise ValueError('WRITER_TRANSITION_SOURCE_SET_DIFFERS')
    for relative in sorted(left, key=lambda path: (path != 'app/Label_Match.py', path)):
        path = left[relative]
        if shared_transition and relative in (shared_replacements.keys() | shared_additions.keys()):
            continue
        if integrity_order_transition and relative == integrity_order_path:
            continue
        if roster_transition and relative in roster_additions:
            continue
        if roster_baseline_transition and relative in roster_baseline_replacements:
            continue
        if remote_transport_transition and relative in remote_transport_replacements:
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
    if shared_transition and relative in shared_replacements:
        continue
    if (source / relative).read_bytes() != (installed / relative).read_bytes():
        raise ValueError('WRITER_TRANSITION_CONTRACT_DIFFERS: ' + relative)
compatibility = ('X13B_PINNED_SHARED_LEAF_ADOPTION' if shared_transition else
                 'PINNED_BOOTSTRAP_INTEGRITY_ORDER_FIX' if integrity_order_transition else
                 'PINNED_LEGACY_LABEL_TRANSITION_MODE' if legacy_label_transition else
                 'PINNED_SCAN_INPUT_FIX' if scan_input_transition else
                 'PINNED_W9NEXTFIX_LABEL_RECOVERY' if nextfix_label_transition else
                 'PINNED_W9SCANONLY_WORKFLOW' if scanner_transition else
                 'PINNED_CURRENT_USER_PROFILE_SELECTION' if current_user_profile_transition else
                 'UNCHANGED_PRODUCTION_AST_AND_CONTRACTS')
print(json.dumps(dict(installed_inventory_sha256=installed_pin, candidate_inventory_sha256=candidate_pin, compatibility=compatibility)))
'@
    $startInfo = New-Object Diagnostics.ProcessStartInfo
    $startInfo.FileName = Join-Path $Source 'runtime\python.exe'
    # Windows argv quoting; the probe is literal source, never shell input.
    $startInfo.Arguments = '-I -B - ' +
        (Arg $Source) + ' ' + (Arg $Installed)
    $startInfo.UseShellExecute = $false
    $startInfo.CreateNoWindow = $true
    $startInfo.RedirectStandardOutput = $true
    $startInfo.RedirectStandardError = $true
    $startInfo.RedirectStandardInput = $true
    $process = [Diagnostics.Process]::Start($startInfo)
    try {
        $probeBytes = [Text.Encoding]::UTF8.GetBytes($probe)
        $process.StandardInput.BaseStream.Write($probeBytes, 0, $probeBytes.Length)
        $process.StandardInput.Close()
        $stdout = $process.StandardOutput.ReadToEnd()
        $stderr = $process.StandardError.ReadToEnd()
        $process.WaitForExit()
        if ($process.ExitCode -ne 0) {
            throw ('Installed writer transition is unsupported: ' + $stderr.Trim())
        }
        return $stdout | ConvertFrom-Json
    }
    finally { $process.Dispose() }
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

function TextSha([string]$Value) {
    return ByteSha ((New-Object Text.UTF8Encoding($false)).GetBytes($Value))
}

function ScheduledTaskSnapshot([string]$AuditRoot, [string]$RunId) {
    $task = Get-ScheduledTask `
        -TaskName $CanonicalTaskName `
        -TaskPath '\' `
        -ErrorAction SilentlyContinue
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
    $existing = Get-ScheduledTask `
        -TaskName $CanonicalTaskName `
        -TaskPath '\' `
        -ErrorAction SilentlyContinue
    if (-not [bool]$Before.exists) {
        if ($null -ne $existing) {
            Unregister-ScheduledTask `
                -TaskName $CanonicalTaskName `
                -TaskPath '\' `
                -Confirm:$false
        }
        if ($null -ne (Get-ScheduledTask -TaskName $CanonicalTaskName -TaskPath '\' -ErrorAction SilentlyContinue)) {
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
    $restored = Get-ScheduledTask -TaskName $CanonicalTaskName -TaskPath '\'
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
        ($Value | ConvertTo-Json -Depth 8) + [Environment]::NewLine,
        (New-Object Text.UTF8Encoding($false))
    )
    Move-Item $temp $Path -Force
}

function FreezePlacementHelper(
    [string]$Source,
    [string]$AuditRoot,
    [string]$RunId,
    $ExpectedCriticalFileSha256
) {
    $frozenRoot = Join-Path $AuditRoot "canonical-portable-$RunId-helper"
    $frozenTools = Join-Path $frozenRoot 'tools'
    New-Item -ItemType Directory -Path $frozenTools -Force | Out-Null
    $sourceHelper = Join-Path $Source 'INSTALL_THIS_PC.ps1'
    $sourceIntegrity = Join-Path $Source 'tools\bootstrap_integrity.ps1'
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

    $sourceSharedLeaf = Get-LabelSharedPortableLeafPath $Source
    $sourceSharedApp = Split-Path (Split-Path (Split-Path $sourceSharedLeaf -Parent) -Parent) -Parent
    $frozenSharedPaths = @()
    foreach ($relative in @('kmtech_shared.lock.json', 'kmtech_shared.manifest.json', 'kmtech_shared\powershell\portable.ps1')) {
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
. ([ScriptBlock]::Create($utf8.GetString($bytes))) -SharedCodeRoot '@@shared-root@@'
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

function InvokeFrozenPlacementHelper($Frozen, [hashtable]$HelperParameters) {
    $payload = [ordered]@{
        helper_path = [string]$Frozen.helper_path
        helper_sha256 = [string]$Frozen.helper_sha256
        integrity_path = Join-Path ([string]$Frozen.root) 'tools\bootstrap_integrity.ps1'
        integrity_sha256 = [string]$Frozen.integrity_sha256
        writer_fence_path = [string]$Frozen.writer_fence_path
        writer_fence_sha256 = [string]$Frozen.writer_fence_sha256
        parameters = $HelperParameters
    }
    $payloadJson = $payload | ConvertTo-Json -Depth 5 -Compress
    $payloadBase64 = [Convert]::ToBase64String(
        (New-Object Text.UTF8Encoding($false)).GetBytes($payloadJson)
    )
    $launcher = @'
$ErrorActionPreference = 'Stop'
$payloadText = (New-Object Text.UTF8Encoding($false, $true)).GetString(
    [Convert]::FromBase64String('@@payload-base64@@')
)
$payload = $payloadText | ConvertFrom-Json
function PinnedBytes([string]$Path, [string]$Expected) {
    [byte[]]$bytes = [IO.File]::ReadAllBytes($Path)
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
$utf8 = New-Object Text.UTF8Encoding($false, $true)
. ([ScriptBlock]::Create($utf8.GetString($integrityBytes))) -SharedCodeRoot (Split-Path (Split-Path ([string]$payload.integrity_path) -Parent) -Parent)
. ([ScriptBlock]::Create($utf8.GetString($writerFenceBytes)))
$helper = [ScriptBlock]::Create($utf8.GetString($helperBytes))
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
    @($actualParameterNames | Where-Object { $_ -notin ($parameterNames + @('Uninstall')) }).Count -ne 0
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
    WriterFenceSessionId = [string]$payload.parameters.WriterFenceSessionId
    WriterFenceAttemptId = [string]$payload.parameters.WriterFenceAttemptId
    WriterFenceReplacementTransactionId = [string]$payload.parameters.WriterFenceReplacementTransactionId
    WriterFenceDelegationToken = [string]$payload.parameters.WriterFenceDelegationToken
    AllowNoncanonicalLayoutForTest = Get-RequiredExternalBoolean $payload.parameters 'AllowNoncanonicalLayoutForTest'
    ReplaceExistingVerifiedPortable = Get-RequiredExternalBoolean $payload.parameters 'ReplaceExistingVerifiedPortable'
    DryRun = Get-RequiredExternalBoolean $payload.parameters 'DryRun'
}
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
& $helper @invokeParameters
if (-not $?) { exit 4 }
exit 0
'@.Replace('@@payload-base64@@', $payloadBase64)
    $encodedLauncher = [Convert]::ToBase64String(
        [Text.Encoding]::Unicode.GetBytes($launcher)
    )
    $winps = Join-Path ([Environment]::SystemDirectory) 'WindowsPowerShell\v1.0\powershell.exe'
    $process = Start-Process `
        -FilePath $winps `
        -Verb RunAs `
        -ArgumentList @(
            '-NoLogo',
            '-NoProfile',
            '-NonInteractive',
            '-ExecutionPolicy',
            'Bypass',
            '-EncodedCommand',
            $encodedLauncher
        ) `
        -WindowStyle Hidden `
        -Wait `
        -PassThru
    return [int]$process.ExitCode
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

function Product([string]$Root, [string]$Mode, [string[]]$ExtraArguments = @()) {
    $args = '-I -B {0} {1} --app-root {2}' -f
        (Arg (Join-Path $Root 'app\main.py')),
        $Mode,
        (Arg $Root)
    foreach ($extraArgument in $ExtraArguments) { $args += ' ' + (Arg $extraArgument) }
    $stdoutPath = Join-Path ([IO.Path]::GetTempPath()) (
        'label-product-' + $PID + '-' + [Guid]::NewGuid().ToString('N') + '.out'
    )
    $stderrPath = Join-Path ([IO.Path]::GetTempPath()) (
        'label-product-' + $PID + '-' + [Guid]::NewGuid().ToString('N') + '.err'
    )
    try {
        $process = Start-Process `
            (Join-Path $Root 'runtime\pythonw.exe') `
            -ArgumentList $args `
            -WindowStyle Hidden `
            -PassThru `
            -RedirectStandardOutput $stdoutPath `
            -RedirectStandardError $stderrPath
        # Start-Process -Wait includes the persistent relay child; wait only for the product host.
        Wait-Process -InputObject $process
        $null = $process.HasExited
        $exitCode = [int]$process.ExitCode
        $stdout = ''
        $stderr = ''
        if (Test-Path -LiteralPath $stdoutPath) {
            $stdout = [IO.File]::ReadAllText($stdoutPath)
        }
        if (Test-Path -LiteralPath $stderrPath) {
            $stderr = [IO.File]::ReadAllText($stderrPath)
        }
        if ($exitCode -ne 0) {
            $detail = (($stderr + ' ' + $stdout) -replace '\s+', ' ').Trim()
            if ($detail.Length -gt 500) { $detail = $detail.Substring(0, 500) }
            throw "Product mode failed: $Mode/$exitCode $detail"
        }
    }
    finally {
        Remove-Item -LiteralPath $stdoutPath -Force -ErrorAction SilentlyContinue
        Remove-Item -LiteralPath $stderrPath -Force -ErrorAction SilentlyContinue
    }
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

function StartRaw([string]$Line) {
    $created = Invoke-CimMethod `
        -ClassName Win32_Process `
        -MethodName Create `
        -Arguments @{ CommandLine = $Line }
    if ([uint32]$created.ReturnValue -ne 0) { throw 'Rollback process start failed.' }
    return [int]$created.ProcessId
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
        removal = read_bounded_json(paths.removal_report_path, label='normal removal report')
        relay = removal.get('relay_process', {})
        if (removal.get('status') != 'PASS_DATA_PRESERVED' or removal.get('data_preserved') is not True or
            pathlib.Path(removal.get('machine_code_root', '')).resolve() != installed.resolve() or
            relay.get('status') != 'ABSENT' or relay.get('request_id') != marker['request_id'] or
            relay.get('stop_request_sha256') != marker_hash):
            raise ValueError('healthy lifecycle stop marker is not the exact normal removal marker')
        result.update(marker_present=True, request_id=marker['request_id'], marker_sha256=marker_hash)
    return paths, result

if mode == 'inspect':
    _, result = inspect()
elif mode == 'release' and expected:
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
    $startInfo = New-Object Diagnostics.ProcessStartInfo
    $startInfo.FileName = Join-Path $Root 'runtime\python.exe'
    $startInfo.Arguments = '-I -B -c "' + $probe.Replace('"', '\"') + '" ' +
        (Arg $Root) + ' ' + (Arg $Installed) + ' ' + (Arg $Mode) + ' ' + (Arg $ExpectedStateSha256)
    $startInfo.UseShellExecute = $false
    $startInfo.CreateNoWindow = $true
    $startInfo.RedirectStandardOutput = $true
    $startInfo.RedirectStandardError = $true
    $process = [Diagnostics.Process]::Start($startInfo)
    try {
        $stdout = $process.StandardOutput.ReadToEnd()
        $stderr = $process.StandardError.ReadToEnd()
        $process.WaitForExit()
        if ($process.ExitCode -ne 0) {
            throw ('Pinned conflict-resolution receipt source validation is required; healthy lifecycle validation failed: ' + $stderr.Trim())
        }
        return $stdout | ConvertFrom-Json
    }
    finally { $process.Dispose() }
}

function Assert-HealthyLifecycleOwnership($Run, $RelayValues, $Tasks, [string]$Installed) {
    if ($Run.exists -and ([string]$Run.kind -cne 'String' -or [string]$Run.data -cne (Command $Installed))) {
        throw 'Healthy lifecycle autostart belongs to another command.'
    }
    $userSid = [Security.Principal.WindowsIdentity]::GetCurrent().User.Value
    if (@($RelayValues).Count -gt 1) { throw 'Healthy lifecycle relay ownership is ambiguous.' }
    foreach ($relay in $RelayValues) {
        $owner = Invoke-CimMethod -InputObject $relay -MethodName GetOwnerSid -ErrorAction Stop
        if ($owner.ReturnValue -ne 0 -or [string]$owner.Sid -cne $userSid -or
            -not (Same ([string]$relay.ExecutablePath) (Join-Path $Installed 'runtime\pythonw.exe')) -or
            (Normalize-RelayExecutableQuoting ([string]$relay.CommandLine) (Join-Path $Installed 'runtime\pythonw.exe')) -cne (Command $Installed)) {
            throw 'Healthy lifecycle relay belongs to another owner or command.'
        }
    }
    foreach ($task in $Tasks) {
        $owner = New-Object Security.Principal.NTAccount([string]$task.Principal.UserId)
        $taskSid = if ([string]$task.Principal.UserId -match '^S-1-') {
            [string]$task.Principal.UserId
        } else { $owner.Translate([Security.Principal.SecurityIdentifier]).Value }
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
    [string[]]$ResiduePaths
) {
    if (Test-Path -LiteralPath $InstallRootValue) { return $false }
    if ([bool]$RunSnapshotValue.exists) { return $false }
    if (@($RelaySnapshotValue).Count -ne 0) { return $false }
    if (@($ScheduledTaskValues).Count -ne 0) { return $false }
    foreach ($path in $ResiduePaths) {
        if ([string]::IsNullOrWhiteSpace($path)) {
            throw 'Pristine-install residue path is empty.'
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
        'X13B_PINNED_SHARED_LEAF_ADOPTION',
        'PINNED_BOOTSTRAP_INTEGRITY_ORDER_FIX',
        'PINNED_LEGACY_LABEL_TRANSITION_MODE',
        'PINNED_SCAN_INPUT_FIX',
        'PINNED_W9NEXTFIX_LABEL_RECOVERY',
        'PINNED_CURRENT_USER_PROFILE_SELECTION'
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
        Save $progressPath ([ordered]@{schema='label-match-first-registration-quarantine-v1'; root=$quarantine; state_paths=$initialMap; restored_absent=$false})
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
        Save $progressPath ([ordered]@{schema='label-match-first-registration-quarantine-v1'; root=$quarantine; state_paths=$map; restored_absent=$false})
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
    Save $progressPath $result
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
    if ($null -ne $Record) { Save $Path $Record }
}

if (-not $SourceRoot) { $SourceRoot = $PSScriptRoot }
. (Get-LabelSharedPortableLeafPath $SourceRoot)
$source = Full $SourceRoot 'SourceRoot'
$install = Full $InstallRoot 'InstallRoot'
if (-not $testMode -and -not (Same $install $CanonicalRoot)) {
    throw 'InstallRoot is not canonical.'
}
$serverBaseUrl = ServerBaseUrlOrigin $ServerBaseUrl
$onboardingArguments = OnboardingArguments $serverBaseUrl
$serverBaseUrlSource = if ($serverBaseUrl) { 'explicit' } else { 'product_default' }
$sourceManifest = Manifest $source $SkipSignatureValidationForTest
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
$wanted = Command $install
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
else { Full $selectedDirectSyncRootValue 'Label current-user DirectSync root' }
$selectedDataRoot = if ([string]::IsNullOrWhiteSpace([string]$env:LABEL_MATCH_SAVE_DIR)) {
    $defaultDataRoot
}
else { Full ([string]$env:LABEL_MATCH_SAVE_DIR) 'Label current-user data root' }
$selectedSettingsPath = if ([string]::IsNullOrWhiteSpace([string]$env:LABEL_MATCH_SETTINGS_PATH)) {
    $defaultSettingsPath
}
else { Full ([string]$env:LABEL_MATCH_SETTINGS_PATH) 'Label current-user settings path' }
# The shared KM_LOGISTICS_PROFILE_PATH is not read: an inherited Machine value from the
# old PC-wide rollout points at a file other programs may use (TEST1 09-30 refused here).
$selectedProfilePath = if ([string]::IsNullOrWhiteSpace([string]$env:LABEL_MATCH_LOGISTICS_PROFILE_PATH)) {
    $defaultProfilePath
}
else { Full ([string]$env:LABEL_MATCH_LOGISTICS_PROFILE_PATH) 'Label logistics profile path' }

$statusRoot = Join-Path $defaultDirectSyncRoot 'status'
$stop = Join-Path $defaultDirectSyncRoot 'control\label_match_user_relay.stop.json'
$writerFenceControlRoot = Join-Path $defaultDirectSyncRoot 'control\writer-session'
$Script:LabelWriterFenceAdmissionProductionRoot = if ($testMode) { '' } else { $writerFenceControlRoot }
$onboardingPath = Join-Path $statusRoot 'current_user_onboarding.json'
$removalPath = Join-Path $statusRoot 'current_user_removal.json'
$relayPath = Join-Path $statusRoot 'label_match_user_relay.json'
$before = Snapshot
$old = @(Relays)
$scheduledTasksAtPreflight = @(
    Get-ScheduledTask -ErrorAction Stop | Where-Object {
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
    $pristineResiduePaths
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
if ($null -eq $receiptSource) { $receiptSource = PortableInventory $source }

$runId = (Get-Date).ToUniversalTime().ToString('yyyyMMddTHHmmssfffZ') + '-' +
    [Guid]::NewGuid().ToString('N')
$placement = 'INSTALL_REQUIRED'
$existingVerified = $false
$installedPreimageInventory = $null
$candidate = $sourceManifest
if (Test-Path -LiteralPath $install) {
    $candidate = Manifest $install $SkipSignatureValidationForTest
    InvokeFrozenIntegrityProbe ([pscustomobject]@{
        root = $source
        integrity_sha256 = [string]$receiptSource.critical_file_sha256.bootstrap_integrity_helper
    }) $install
    $installedPreimageInventory = PortableInventory $install
    $existingVerified = $true
    if ([string]$candidate.source_commit -ceq [string]$sourceManifest.source_commit) {
        $placement = 'REUSED_VERIFIED'
    }
}
$writerTransition = Assert-WriterTransition $source $(if ($existingVerified) { $install } else { $source })
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
if (-not $conflictReceiptSupplied -and -not $pristineInstall -and -not $legacyFirstRegistration) {
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
if ($EvidencePath) { Save (Full $EvidencePath 'EvidencePath') $audit }
$frozenPlacement = FreezePlacementHelper `
    $source `
    $localAuditRoot `
    $runId `
    $receiptSource.critical_file_sha256
. ([string]$frozenPlacement.writer_fence_path)

$writerSessionId = [Guid]::NewGuid().ToString('N')
$writerAttemptId = [Guid]::NewGuid().ToString('N')
$writerTransactionId = [Guid]::NewGuid().ToString('N')
$writerDelegationToken = ([Guid]::NewGuid().ToString('N') + [Guid]::NewGuid().ToString('N'))
$writerStartedAt = [DateTime]::UtcNow.ToString('o')
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
    'current_user_onboarding',
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
    'user_relay_stop_request'
)
$rollbackSources = @(
    'current_user_setup_removal',
    'scheduled_task_remove',
    'user_relay_autostart_remove',
    'user_relay_stop_request'
)
$mutated = $false
$replacementRollbackRoot = if ($null -ne $firstRegistrationResume) { [string]$firstRegistrationResume.code_backup_root } else { '' }
$replacementBackupsBefore = @()
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
        session_id = $writerSessionId
        attempt_id = $writerAttemptId
        replacement_transaction_id = $writerTransactionId
        writer_inventory_sha256 = $Script:LabelWriterFenceInventorySha256
        token_recorded = $false
    }
    Save $auditPath $audit
    if ($null -ne $firstRegistrationResume) {
        $helperParameters = @{
            SourceRoot=$source; InstallRoot=$install; ElevationLogPath=$elevationLogPath
            ExpectedBootstrapScriptSha256=[string]$frozenPlacement.helper_sha256
            VerifiedBootstrapScriptPath=[string]$frozenPlacement.helper_path; BootstrapIntegrityPreloaded=$true
            WriterFenceFunctionsPreloaded=$true; WriterFenceControlRoot=$writerFenceControlRoot
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
                $null -ne (Get-ScheduledTask -TaskName $CanonicalTaskName -TaskPath '\' -ErrorAction SilentlyContinue)
            ) { throw 'Removal readback failed.' }
        }
    }
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
                [string]$receiptSource.critical_file_sha256.placement_helper -or
            [string]$frozenPlacement.integrity_sha256 -cne
                [string]$receiptSource.critical_file_sha256.bootstrap_integrity_helper -or
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
            -DelegatedSources @('canonical_placement') `
            -LifetimeSeconds 600)
        $helperParameters = @{
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
        $placementExitCode = InvokeFrozenPlacementHelper $frozenPlacement $helperParameters
        if ($existingVerified) {
            $replacementRollbackRoot = ReplacementPreimage $install $installedPreimageInventory $replacementBackupsBefore
            if ($legacyFirstRegistration) {
                $firstRegistrationAttempt.code_backup_root = $replacementRollbackRoot
                Save-FirstRegistrationAttempt $firstRegistrationAttempt $firstRegistrationAttemptPath
            }
        }
        if ($placementExitCode -ne 0) { throw "Code placement failed: $placementExitCode" }
        $placement = 'PASS'
    }
    $installedManifest = Manifest $install $SkipSignatureValidationForTest
    if ([string]$installedManifest.source_commit -cne [string]$sourceManifest.source_commit) {
        throw 'Installed identity differs.'
    }
    $installedInventory = PortableInventory $install
    if (
        [string]$installedInventory.schema_version -cne [string]$receiptSource.schema_version -or
        [string]$installedInventory.algorithm -cne [string]$receiptSource.algorithm -or
        [int]$installedInventory.file_count -ne [int]$receiptSource.file_count -or
        [uint64]$installedInventory.byte_count -ne [uint64]$receiptSource.byte_count -or
        [string]$installedInventory.sha256 -cne [string]$receiptSource.sha256
    ) { throw 'Installed full portable inventory differs before Product execution.' }
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
    try { Product $install '--onboard-current-user' $onboardingArguments }
    catch { $onboardingFailure = $_ }
    if ($null -ne $onboardingFailure -and -not (Test-Path -LiteralPath $onboardingPath)) {
        throw $onboardingFailure
    }
    $onboarding = Get-Content $onboardingPath -Raw -Encoding UTF8 | ConvertFrom-Json
    $after = Snapshot
    if ($null -ne $onboardingFailure -and $pristineInstall -and
        [string]$onboarding.report_version -ceq 'label-match-current-user-onboarding-v1' -and
        [string]$onboarding.status -ceq 'RECOVERY_REQUIRED' -and
        [string]$onboarding.recovery_action -ceq 'ADMIN_RECOVERY_REQUIRED' -and
        $onboarding.server_registration_verified -ceq $false -and
        (Get-Item -LiteralPath $onboardingPath).LastWriteTimeUtc -ge $started.AddSeconds(-1)) {
        if ($after.exists -or @(Relays).Count -ne 0 -or
            @(Get-ScheduledTask -TaskName $CanonicalTaskName -TaskPath '\' -ErrorAction SilentlyContinue).Count -ne 0 -or
            (Test-Path -LiteralPath $stop)) {
            throw 'Recovery-required registration must leave persistence absent.'
        }
        [void](Stop-LabelWriterFence -ControlRoot $writerFenceControlRoot `
            -SessionId $writerSessionId -AttemptId $writerAttemptId `
            -ReplacementTransactionId $writerTransactionId -TimeoutMilliseconds 90000)
        $writerFenceStarted = $false
        Exit-LabelWriterSessionAuthority $writerAuthority
        $writerAuthority = $null
        $audit.status = 'RECOVERY_REQUIRED'
        $audit.completed_at = [DateTime]::UtcNow.ToString('o')
        $audit.after = $after
        $audit.writer_fence.status = 'RELEASED_FOR_ADMIN_RECOVERY'
        $audit.onboarding = [ordered]@{ status='RECOVERY_REQUIRED'; action='ADMIN_RECOVERY_REQUIRED' }
        $audit.recovery_guidance = 'Code is installed. Use the installed registration tool with the normal enrollment token and a separate administrator authorization, then run current-user onboarding again.'
        Save $auditPath $audit
        if ($EvidencePath) { Save (Full $EvidencePath 'EvidencePath') $audit }
        'install_status=RECOVERY_REQUIRED'
        "install_root=$install"
        "code_placement_status=$placement"
        'autostart_status=NOT_CONFIGURED'
        "next_action=$($audit.recovery_guidance)"
        "audit_path=$auditPath"
        return
    }
    if ($null -ne $onboardingFailure) { throw $onboardingFailure }
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

    [void](Stop-LabelWriterFence `
        -ControlRoot $writerFenceControlRoot `
        -SessionId $writerSessionId `
        -AttemptId $writerAttemptId `
        -ReplacementTransactionId $writerTransactionId `
        -TimeoutMilliseconds 90000)
    $writerFenceStarted = $false
    Exit-LabelWriterSessionAuthority $writerAuthority
    $writerAuthority = $null

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
    $original = $_
    if (-not $mutated) { throw $original }
    try {
        if ($mutated) {
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
                    Product $rollbackProductRoot '--remove-current-user-setup'
                }
            }
            [void](Assert-RollbackRelayPreimage -ExpectedRelays @())
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
                    -DelegatedSources @('canonical_placement') `
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
                    -DelegatedSources @('canonical_placement') `
                    -LifetimeSeconds 600)
                $removeParameters = $helperParameters.Clone()
                $removeParameters.ReplaceExistingVerifiedPortable = $false
                $removeParameters.Uninstall = $true
                # As with setup-preimage restoration below, the orchestrator
                # holds writer admission for the entire privileged mutation.
                $codeRemovalAdmission = Enter-LabelWriterAdmission -ControlRoot $writerFenceControlRoot -TimeoutMilliseconds 90000
                try { $removeExit = InvokeFrozenPlacementHelper $frozenPlacement $removeParameters }
                finally { Exit-LabelWriterAdmission $codeRemovalAdmission }
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
        $ownerMutationLease = $null
        try {
            if ($writerFenceStarted) {
                $ownerMutationLease = Enter-LabelWriterAdmission `
                    -ControlRoot $writerFenceControlRoot `
                    -TimeoutMilliseconds 90000
            }
            Restore $before
            RestoreScheduledTask $taskBefore
            if ([bool]$stopBefore.exists) {
                Copy-Item -LiteralPath ([string]$stopBefore.backup_path) -Destination $stop -Force
                if ((Sha $stop) -cne [string]$stopBefore.sha256) {
                    throw 'stop marker restore failed'
                }
            }
            elseif (Test-Path $stop) {
                Remove-Item $stop -Force
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
        finally {
            if ($null -ne $ownerMutationLease) {
                Exit-LabelWriterAdmission $ownerMutationLease
            }
        }
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
        [void](Assert-RollbackRelayPreimage -ExpectedRelays $old)
        $check = Snapshot
        if (
            [bool]$check.exists -ne [bool]$before.exists -or
            [string]$check.kind -cne [string]$before.kind -or
            [string]$check.data -cne [string]$before.data
        ) { throw 'registry restore failed' }
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
        Save $auditPath $audit
        if ($EvidencePath) { Save (Full $EvidencePath 'EvidencePath') $audit }
    }
    catch {
        $rollbackFailure = $_
        try {
            $audit.status = if ($legacyFirstRegistration) { 'FAILED_ROLLBACK_INCOMPLETE' } else { 'ROLLBACK_FAILED' }
            $audit.code_state = if (Test-Path -LiteralPath $install) { 'PRESENT' } else { 'ABSENT' }
            $audit.rollback.applied = $mutated
            $audit.rollback.runtime_restored = $false
            $audit.rollback.failure_type = $rollbackFailure.Exception.GetType().Name
            $audit.failure_type = $original.Exception.GetType().Name
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
        throw ("AUTOSTART_ROLLBACK_FAILED: " + $rollbackFailure.Exception.Message +
            '; original=' + $original.Exception.Message)
    }
    throw $original
}
finally {
    RestoreWriterDelegationEnvironment $writerEnvironmentBefore
    if ($null -ne $writerAuthority) {
        Exit-LabelWriterSessionAuthority $writerAuthority
        $writerAuthority = $null
    }
    $Script:LabelWriterFenceInstalledIdentity = $null
}
