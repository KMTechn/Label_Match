# Original pure functions from Label_Match 57f52e1, retained for X13-B behavioral parity.

function Full([string]$Value, [string]$Purpose) {
    if (-not [IO.Path]::IsPathRooted($Value) -or $Value.StartsWith('\\?\')) {
        throw "$Purpose must be an ordinary absolute path."
    }
    $result = [IO.Path]::GetFullPath($Value).TrimEnd('\')
    if ($result -eq [IO.Path]::GetPathRoot($result)) { throw "$Purpose is too broad." }
    return $result
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

function Get-BootstrapStrictFullPath {
    param(
        [Parameter(Mandatory = $true)][string]$Path,
        [Parameter(Mandatory = $true)][string]$Purpose
    )
    if ([string]::IsNullOrWhiteSpace($Path) -or -not [IO.Path]::IsPathRooted($Path)) {
        throw "$Purpose must be an absolute path."
    }
    if ($Path.StartsWith('\\?\') -or $Path.StartsWith('\\.\')) {
        throw "$Purpose must not use a device path."
    }
    return [IO.Path]::GetFullPath($Path).TrimEnd([char[]]"\/")
}

function Get-BootstrapFileSha256 {
    param([Parameter(Mandatory = $true)][string]$Path)
    $stream = [IO.File]::OpenRead($Path)
    $sha = [Security.Cryptography.SHA256]::Create()
    try {
        return ([BitConverter]::ToString($sha.ComputeHash($stream))).Replace('-', '').ToLowerInvariant()
    }
    finally {
        $sha.Dispose()
        $stream.Dispose()
    }
}

function Get-BootstrapRelativeCodePath {
    param(
        [Parameter(Mandatory = $true)][string]$Root,
        [Parameter(Mandatory = $true)][string]$Path
    )
    $rootFull = (Get-BootstrapStrictFullPath $Root "inventory root") + '\'
    $pathFull = [IO.Path]::GetFullPath($Path)
    if (-not $pathFull.StartsWith($rootFull, [StringComparison]::OrdinalIgnoreCase)) {
        throw "Inventory path escaped its root."
    }
    return $pathFull.Substring($rootFull.Length).Replace('\', '/')
}

function Get-BootstrapInventoryAggregate {
    param([Parameter(Mandatory = $true)][object[]]$Inventory)
    $lines = @($Inventory | ForEach-Object { "$($_.sha256) $($_.size) $($_.path)" })
    $bytes = (New-Object Text.UTF8Encoding($false)).GetBytes(($lines -join "`n") + "`n")
    $sha = [Security.Cryptography.SHA256]::Create()
    try {
        return ([BitConverter]::ToString($sha.ComputeHash($bytes))).Replace('-', '').ToLowerInvariant()
    }
    finally {
        $sha.Dispose()
    }
}

function Sort-BootstrapInventory {
    param([Parameter(Mandatory = $true)][AllowEmptyCollection()][object[]]$Inventory)
    [object[]]$ordered = @($Inventory)
    [Array]::Sort(
        $ordered,
        [System.Collections.Generic.Comparer[object]]::Create(
            [System.Comparison[object]]{
                param($left, $right)
                return [StringComparer]::Ordinal.Compare([string]$left.path, [string]$right.path)
            }
        )
    )
    return $ordered
}
