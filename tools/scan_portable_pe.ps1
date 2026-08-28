[CmdletBinding()]
param(
    [Parameter(Mandatory = $true)][string]$Root,
    [Parameter(Mandatory = $true)][string]$OutputJson,
    [ValidateRange(0, 100)][int]$ExpectedUnsigned = 0
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'
$resolvedRoot = (Resolve-Path -LiteralPath $Root -ErrorAction Stop).Path
if (-not (Test-Path -LiteralPath $resolvedRoot -PathType Container)) {
    throw "Portable root is not a directory: $resolvedRoot"
}

$rows = @()
foreach ($file in @(Get-ChildItem -LiteralPath $resolvedRoot -Recurse -File)) {
    if ($file.Length -lt 2) { continue }
    $stream = [IO.File]::OpenRead($file.FullName)
    try {
        $isPe = ($stream.ReadByte() -eq 0x4d -and $stream.ReadByte() -eq 0x5a)
    }
    finally {
        $stream.Dispose()
    }
    if (-not $isPe) { continue }
    $signature = Get-AuthenticodeSignature -LiteralPath $file.FullName
    $rows += [ordered]@{
        relative_path = $file.FullName.Substring($resolvedRoot.Length + 1).Replace('\', '/')
        bytes = [int64]$file.Length
        sha256 = (Get-FileHash -LiteralPath $file.FullName -Algorithm SHA256).Hash.ToLowerInvariant()
        status = [string]$signature.Status
        signer = if ($null -ne $signature.SignerCertificate) { [string]$signature.SignerCertificate.Subject } else { '' }
    }
}
$rows = @($rows | Sort-Object relative_path)
$unsigned = @($rows | Where-Object status -eq 'NotSigned')
$other = @($rows | Where-Object { $_.status -notin @('Valid', 'NotSigned') })
$result = [ordered]@{
    schema = 'label-match-portable-pe-inventory-v1'
    root = $resolvedRoot
    pe_count = $rows.Count
    valid_count = @($rows | Where-Object status -eq 'Valid').Count
    unsigned_count = $unsigned.Count
    other_status_count = $other.Count
    unsigned_paths = @($unsigned | ForEach-Object relative_path)
    other_status_paths = @($other | ForEach-Object relative_path)
    files = $rows
}
$json = $result | ConvertTo-Json -Depth 6
[IO.File]::WriteAllText(
    [IO.Path]::GetFullPath($OutputJson),
    $json + [Environment]::NewLine,
    [Text.UTF8Encoding]::new($false)
)
if ($other.Count -ne 0) {
    throw "Portable tree contains PE files with non-terminal signature status: $($result.other_status_paths -join ', ')"
}
if ($unsigned.Count -ne $ExpectedUnsigned) {
    throw "Portable tree unsigned PE count is $($unsigned.Count), expected $ExpectedUnsigned; files=$($result.unsigned_paths -join ', ')"
}
Write-Output ("PE={0} Valid={1} Unsigned={2}" -f $result.pe_count, $result.valid_count, $result.unsigned_count)
Write-Output ("UnsignedPaths={0}" -f ($result.unsigned_paths -join ','))
