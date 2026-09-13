# Shared read-only installer leaves. Windows PowerShell 5.1 / PowerShell 7.
# Dot-sourcing only defines functions; app schema, inventory order and lifecycle
# remain with the caller. See docs/powershell-equivalence.md before adoption.

function Get-KmtechFileSha([string]$Path) {
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

# Canonical Full contract in CA/IW/RW/LM; DI has additional rejection rules.
function ConvertTo-KmtechFullPath([string]$Value, [string]$Purpose) {
    if (-not [IO.Path]::IsPathRooted($Value) -or $Value.StartsWith('\\?\')) {
        throw "$Purpose must be an ordinary absolute path."
    }
    $result = [IO.Path]::GetFullPath($Value).TrimEnd('\')
    if ($result -eq [IO.Path]::GetPathRoot($result)) { throw "$Purpose is too broad." }
    return $result
}

# Bootstrap contract in DI/IW/RW/LM, distinct from canonical Full and CA.
function Get-KmtechBootstrapStrictFullPath {
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

function Get-KmtechBootstrapRelativeCodePath {
    param(
        [Parameter(Mandatory = $true)][string]$Root,
        [Parameter(Mandatory = $true)][string]$Path
    )
    $rootFull = (Get-KmtechBootstrapStrictFullPath $Root "inventory root") + '\'
    $pathFull = [IO.Path]::GetFullPath($Path)
    if (-not $pathFull.StartsWith($rootFull, [StringComparison]::OrdinalIgnoreCase)) {
        throw "Inventory path escaped its root."
    }
    return $pathFull.Substring($rootFull.Length).Replace('\', '/')
}

# Hash the supplied row order; do not sort, validate, or enumerate here.
function Get-KmtechBootstrapInventoryAggregate {
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

# RequiredFiles is the app's existing ordered list, including its writer helpers.
function Assert-KmtechPortableTree([string]$Root, [string[]]$RequiredFiles) {
    foreach ($relative in $RequiredFiles) {
        if (-not (Test-Path -LiteralPath (Join-Path $Root $relative) -PathType Leaf)) {
            throw "Portable tree is missing $relative."
        }
    }
    foreach ($item in @((Get-Item $Root -Force)) + @(Get-ChildItem $Root -Force -Recurse)) {
        if (($item.Attributes -band [IO.FileAttributes]::ReparsePoint) -ne 0) {
            throw "Portable tree contains a reparse point: $($item.FullName)"
        }
    }
}

# Bounded parsing only: callers must keep their schema/hash/native-closure checks.
# Preserve canonical installer provider-path and JSON error behavior.
function Read-KmtechPortableManifest([string]$Root) {
    $path = Join-Path $Root 'portable-manifest.json'
    if ((Get-Item $path).Length -gt 65536) { throw 'Portable manifest is oversized.' }
    $value = Get-Content $path -Raw -Encoding UTF8 | ConvertFrom-Json
    # Keep the parser's assigned shape across this extra function boundary.
    # The app's original schema/property checks must see arrays and null intact.
    return ,$value
}

function Assert-KmtechCPythonSignature([string]$Root, [bool]$UnsignedOk) {
    if (-not $UnsignedOk) {
        foreach ($relative in @('runtime\python.exe', 'runtime\pythonw.exe')) {
            if ([string](Get-AuthenticodeSignature (Join-Path $Root $relative)).Status -cne 'Valid') {
                throw "Signed CPython readback failed: $relative"
            }
        }
    }
}
