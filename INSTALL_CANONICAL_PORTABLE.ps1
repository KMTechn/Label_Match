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
            $records | Sort-Object path | ForEach-Object {
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

function Manifest([string]$Root, [bool]$UnsignedOk) {
    foreach ($relative in @(
        'portable-manifest.json',
        'runtime\python.exe',
        'runtime\pythonw.exe',
        'app\main.py',
        'launch-label-match.cmd',
        'INSTALL_CANONICAL_PORTABLE.ps1',
        'INSTALL_THIS_PC.ps1',
        'tools\bootstrap_integrity.ps1',
        'tools\label_writer_fence.ps1'
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

    $userSid = [Security.Principal.WindowsIdentity]::GetCurrent().User
    $systemSid = New-Object Security.Principal.SecurityIdentifier('S-1-5-18')
    $adminSid = New-Object Security.Principal.SecurityIdentifier('S-1-5-32-544')
    $inheritance = [Security.AccessControl.InheritanceFlags]::ContainerInherit -bor
        [Security.AccessControl.InheritanceFlags]::ObjectInherit
    $propagation = [Security.AccessControl.PropagationFlags]::None
    $allow = [Security.AccessControl.AccessControlType]::Allow
    $acl = New-Object Security.AccessControl.DirectorySecurity
    $acl.SetOwner($userSid)
    $acl.SetAccessRuleProtection($true, $false)
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
    foreach ($frozenPath in @($frozenHelper, $frozenIntegrity, $frozenWriterFence)) {
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
    [byte[]]$integrityBytes = PinnedFileBytes $integrityPath ([string]$Frozen.integrity_sha256)
    $integrityText = (New-Object Text.UTF8Encoding($false, $true)).GetString(
        $integrityBytes
    )
    $rootBase64 = [Convert]::ToBase64String(
        (New-Object Text.UTF8Encoding($false)).GetBytes($Root)
    )
    $probeScript = $integrityText + "`n" +
        "`$probeRoot = (New-Object Text.UTF8Encoding(`$false, `$true)).GetString(" +
        "[Convert]::FromBase64String('$rootBase64'))`n" +
        "[void](Assert-BootstrapIntegrityRecord `$probeRoot)`n"
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
. ([ScriptBlock]::Create($utf8.GetString($integrityBytes)))
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
    @($actualParameterNames | Where-Object { $_ -notin $parameterNames }).Count -ne 0
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

function Assert-RollbackRelayPreimage([object[]]$ExpectedRelays) {
    $actualRelays = @(Relays)
    if ($actualRelays.Count -ne $ExpectedRelays.Count) {
        throw 'rollback relay process-count readback failed'
    }
    foreach ($expected in $ExpectedRelays) {
        $matching = @($actualRelays | Where-Object {
            (Same ([string]$_.ExecutablePath) ([string]$expected.ExecutablePath)) -and
            [string]$_.CommandLine -ceq [string]$expected.CommandLine
        })
        if ($matching.Count -ne 1) {
            throw 'rollback relay executable/command readback failed'
        }
    }
    return $actualRelays
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

if (-not $SourceRoot) { $SourceRoot = $PSScriptRoot }
$source = Full $SourceRoot 'SourceRoot'
$install = Full $InstallRoot 'InstallRoot'
if (-not $testMode -and -not (Same $install $CanonicalRoot)) {
    throw 'InstallRoot is not canonical.'
}
$sourceManifest = Manifest $source $SkipSignatureValidationForTest
$receiptSource = $null
if (
    -not [string]::IsNullOrWhiteSpace(
        [string]$env:KMTECH_LABEL_CONFLICT_RESOLUTION_RECEIPT_PATH
    ) -or
    -not [string]::IsNullOrWhiteSpace(
        [string]$env:KMTECH_LABEL_CONFLICT_RESOLUTION_RECEIPT_SHA256
    )
) {
    $receiptSource = ReceiptSource $source $sourceManifest
}
$wanted = Command $install
if ($PlanOnly) {
    "install_status=PLAN_ONLY"
    "install_root=$install"
    "autostart_command=$wanted"
    "receipt_source_status=$(if ($null -eq $receiptSource) { 'NOT_REQUESTED' } else { 'PASS' })"
    'registry_changed=false'
    exit 0
}
if ($null -eq $receiptSource) {
    throw 'Pinned conflict-resolution receipt source validation is required.'
}

$runId = (Get-Date).ToUniversalTime().ToString('yyyyMMddTHHmmssfffZ') + '-' +
    [Guid]::NewGuid().ToString('N')
$lad = Full $env:LOCALAPPDATA 'LOCALAPPDATA'
$localAuditRoot = Join-Path $lad 'KMTech\Label_Match\install-audit'
New-Item -ItemType Directory -Path $localAuditRoot -Force | Out-Null
$auditPath = Join-Path $localAuditRoot "canonical-portable-$runId.json"
$elevationLogPath = Join-Path $localAuditRoot "canonical-portable-$runId-elevated.jsonl"
$statusRoot = Join-Path $lad 'KMTech\DirectSync\label_match\status'
$stop = Join-Path $lad 'KMTech\DirectSync\label_match\control\label_match_user_relay.stop.json'
$writerFenceControlRoot = Join-Path $lad 'KMTech\DirectSync\label_match\control\writer-session'
$onboardingPath = Join-Path $statusRoot 'current_user_onboarding.json'
$removalPath = Join-Path $statusRoot 'current_user_removal.json'
$relayPath = Join-Path $statusRoot 'label_match_user_relay.json'
$before = Snapshot
$taskBefore = ScheduledTaskSnapshot $localAuditRoot $runId
$old = @(Relays)
$stopBefore = [ordered]@{ exists = $false; sha256 = ''; backup_path = '' }
if (Test-Path -LiteralPath $stop -PathType Leaf) {
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
    scheduled_task_preimage = [ordered]@{
        exists = [bool]$taskBefore.exists
        enabled = [bool]$taskBefore.enabled
        xml_sha256 = [string]$taskBefore.xml_sha256
        backup_path = [string]$taskBefore.backup_path
    }
    stop_marker_path = $stop
    stop_marker_preimage = $stopBefore
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

$placement = 'INSTALL_REQUIRED'
$existingVerified = $false
if (Test-Path $install -PathType Container) {
    try {
        $candidate = Manifest $install $SkipSignatureValidationForTest
        InvokeFrozenIntegrityProbe $frozenPlacement $install
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
$writerSessionId = [Guid]::NewGuid().ToString('N')
$writerAttemptId = [Guid]::NewGuid().ToString('N')
$writerTransactionId = [Guid]::NewGuid().ToString('N')
$writerDelegationToken = ([Guid]::NewGuid().ToString('N') + [Guid]::NewGuid().ToString('N'))
$writerStartedAt = [DateTime]::UtcNow.ToString('o')
$writerOrchestratorSha256 = Sha (Join-Path $source 'INSTALL_CANONICAL_PORTABLE.ps1')
$writerContractSha256 = [string]$frozenPlacement.writer_fence_sha256
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
    'scheduled_task_install',
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
try {
    $mutated = $true
    $writerAuthority = Enter-LabelWriterSessionAuthority `
        -SessionId $writerSessionId `
        -AttemptId $writerAttemptId `
        -OrchestratorSha256 $writerOrchestratorSha256 `
        -ReplacementTransactionId $writerTransactionId `
        -WriterContractSha256 $writerContractSha256
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
    $audit.writer_fence = [ordered]@{
        status = 'QUIESCING'
        session_id = $writerSessionId
        attempt_id = $writerAttemptId
        replacement_transaction_id = $writerTransactionId
        writer_inventory_sha256 = $Script:LabelWriterFenceInventorySha256
        token_recorded = $false
    }
    Save $auditPath $audit

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

    $removalRoot = if ($existingVerified) { $install } else { $source }
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
    $unquiesced = @(UnquiescedProductWriters)
    if ($unquiesced.Count -ne 0) {
        throw 'Writer quiescence failed: a Label product writer process remains.'
    }
    $audit.writer_fence.status = 'QUIESCED'
    Save $auditPath $audit

    if ($placement -eq 'INSTALL_REQUIRED') {
        if ((Test-Path $install -PathType Container) -and -not $existingVerified) {
            throw 'Existing canonical tree is not eligible for verified replacement.'
        }
        $receiptSource = ReceiptSource $source $sourceManifest
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
        $placementExitCode = InvokeFrozenPlacementHelper $frozenPlacement $helperParameters
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
    try {
        if ($mutated) {
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
            Product $rollbackProductRoot '--remove-current-user-setup'
            [void](Assert-RollbackRelayPreimage -ExpectedRelays @())
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
        }
        finally {
            if ($null -ne $ownerMutationLease) {
                Exit-LabelWriterAdmission $ownerMutationLease
            }
        }
        if ($writerFenceStarted) {
            [void](Stop-LabelWriterFence `
                -ControlRoot $writerFenceControlRoot `
                -SessionId $writerSessionId `
                -AttemptId $writerAttemptId `
                -ReplacementTransactionId $writerTransactionId `
                -TimeoutMilliseconds 90000)
            $writerFenceStarted = $false
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
        $audit.rollback.applied = $mutated
        $audit.rollback.runtime_restored = $true
        $audit.failure_type = $original.Exception.GetType().Name
        Save $auditPath $audit
        if ($EvidencePath) { Save (Full $EvidencePath 'EvidencePath') $audit }
    }
    catch {
        $rollbackFailure = $_
        try {
            $audit.status = 'ROLLBACK_FAILED'
            $audit.rollback.applied = $mutated
            $audit.rollback.runtime_restored = $false
            $audit.rollback.failure_type = $rollbackFailure.Exception.GetType().Name
            $audit.failure_type = $original.Exception.GetType().Name
            Save $auditPath $audit
            if ($EvidencePath) { Save (Full $EvidencePath 'EvidencePath') $audit }
        }
        catch {
            throw (
                'ROLLBACK_AUDIT_PERSISTENCE_FAILED: ' + $_.Exception.GetType().Name +
                '; rollback=' + $rollbackFailure.Exception.GetType().Name
            )
        }
        throw "AUTOSTART_ROLLBACK_FAILED: $($rollbackFailure.Exception.GetType().Name)"
    }
    throw $original
}
finally {
    RestoreWriterDelegationEnvironment $writerEnvironmentBefore
    if ($null -ne $writerAuthority) {
        Exit-LabelWriterSessionAuthority $writerAuthority
        $writerAuthority = $null
    }
}
