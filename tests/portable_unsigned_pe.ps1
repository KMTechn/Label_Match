# Installer readback of portable-manifest.json allowed_unsigned_app_pe (docs/spec/operations.md#portable-native-files).
# Runs the real Manifest (INSTALL_CANONICAL_PORTABLE.ps1) and Assert-RequiredRelease (INSTALL_THIS_PC.ps1) on a
# disposable portable tree; the three pinned sha256 values are swapped for synthetic files' hashes.
param([string]$HostRoot, [string]$PortableRoot)
$ErrorActionPreference = 'Stop'
$script:Checks = 0
$IntegrityFileName = 'bootstrap-integrity.json'
function Equal($Actual, $Expected, [string]$Name) {
    if ($Actual -cne $Expected) { throw "FAIL $Name : expected [$Expected], got [$Actual]" }
    $script:Checks++
}
function Parse-Functions([string]$Path) {
    $tokens = $null; $errors = $null
    $ast = [Management.Automation.Language.Parser]::ParseFile($Path, [ref]$tokens, [ref]$errors)
    if ($errors.Count) { throw "Source parse failed: $Path" }
    $result = @{}
    foreach ($fn in $ast.FindAll({param($node) $node -is [Management.Automation.Language.FunctionDefinitionAst]}, $false)) {
        $result[$fn.Name] = $fn
    }
    return $result
}
. (Join-Path $HostRoot 'tools/bootstrap_integrity.ps1')
. (Join-Path $HostRoot 'kmtech_shared/powershell/portable.ps1')
$pins = [ordered]@{
    'site-packages/_cffi_backend.cp312-win_amd64.pyd' = '9af5e4f68a41f26b726f2e9424f9d58a4f5666189f6a48be95f406b257102aec'
    'site-packages/clr_loader/ffi/dlls/amd64/ClrLoader.dll' = '07f62bcb1b70320f221ffb2cfc5ce6133815195f3e7cdb8d833222506188c2ac'
    'site-packages/pythonnet/runtime/Python.Runtime.dll' = '2ebd4492e28442ef1f1af587afe5b3a090c2ebfa220758ded4f1450f2a27f13a'
}
$synthetic = @{}
$index = 0
foreach ($relative in $pins.Keys) {
    $path = Join-Path (Join-Path $PortableRoot 'app') $relative.Replace('/', '\')
    [void](New-Item -ItemType Directory -Force -Path (Split-Path -Parent $path))
    [IO.File]::WriteAllBytes($path, [Text.Encoding]::ASCII.GetBytes("MZ synthetic native $index"))
    $synthetic[$relative] = Get-BootstrapFileSha256 $path
    $index++
}
function Reference($Path, [string]$Name, [string[]]$Dependencies) {
    $functions = Parse-Functions $Path
    $text = @($Dependencies + $Name | ForEach-Object { $functions[$_].Extent.Text }) -join "`n"
    foreach ($relative in $pins.Keys) {
        if (-not $text.Contains($pins[$relative])) { throw "pin absent in $Name : $relative" }
        $text = $text.Replace($pins[$relative], $synthetic[$relative])
    }
    return [scriptblock]::Create($text + "`n" + $Name + ' @args')
}
$canonical = Reference (Join-Path $HostRoot 'INSTALL_CANONICAL_PORTABLE.ps1') 'Manifest' @('Sha')
$thisPc = Reference (Join-Path $HostRoot 'INSTALL_THIS_PC.ps1') 'Assert-RequiredRelease' @('Get-EarlyFileSha256', 'Get-FileSha256', 'Test-SamePath', 'Get-StrictFullPath')
$manifestPath = Join-Path $PortableRoot 'portable-manifest.json'
$original = [IO.File]::ReadAllText($manifestPath)
function Write-Manifest([object[]]$Listed) {
    $value = $original | ConvertFrom-Json
    $value.allowed_unsigned_app_pe = @($Listed)
    $count = [int64]0; $bytes = [int64]0
    Get-ChildItem -LiteralPath $PortableRoot -File -Force -Recurse | ForEach-Object {
        if ($_.Name -cne 'portable-manifest.json' -and $_.Name -cne $IntegrityFileName) { $count++; $bytes += $_.Length }
    }
    $value.file_count_before_manifest = $count
    $value.byte_count_before_manifest = $bytes
    [IO.File]::WriteAllText($manifestPath, ($value | ConvertTo-Json -Depth 8), (New-Object Text.UTF8Encoding($false)))
}
function Outcome([scriptblock]$Check, [object[]]$Arguments) {
    try { [void](& $Check @Arguments); return 'accepted' } catch { return $_.Exception.Message }
}
$three = @($pins.Keys)
$cases = [ordered]@{
    'none (older zero-PE release)' = @{ listed = @(); accept = $true }
    'exactly the pinned three' = @{ listed = $three; accept = $true }
    'a fourth unsigned PE' = @{ listed = $three + @('site-packages/webview/lib/WebBrowserInterop.x64.dll'); accept = $false }
    'one listed twice' = @{ listed = @($three[0], $three[0], $three[1]); accept = $false }
    'case variant path' = @{ listed = @($three[0], $three[1], $three[2].ToUpperInvariant()); accept = $false }
    'only two' = @{ listed = @($three[0], $three[1]); accept = $false }
}
foreach ($name in $cases.Keys) {
    Write-Manifest $cases[$name].listed
    $a = Outcome $canonical @($PortableRoot, $true)
    $b = Outcome $thisPc @($PortableRoot, $true)
    if ($cases[$name].accept) {
        Equal $a 'accepted' "canonical $name"
        Equal $b 'accepted' "this-pc $name"
    } else {
        Equal $a 'Portable manifest readback failed.' "canonical $name"
        Equal $b 'Portable release manifest contract is invalid.' "this-pc $name"
    }
}
# A listed file whose bytes differ from the pin, or that is absent, is refused.
$changed = Join-Path (Join-Path $PortableRoot 'app') $three[1].Replace('/', '\')
$saved = [IO.File]::ReadAllBytes($changed)
[IO.File]::WriteAllBytes($changed, [Text.Encoding]::ASCII.GetBytes('MZ changed'))
Write-Manifest $three
Equal (Outcome $canonical @($PortableRoot, $true)) 'Portable manifest readback failed.' 'canonical changed bytes'
Equal (Outcome $thisPc @($PortableRoot, $true)) 'Portable release manifest contract is invalid.' 'this-pc changed bytes'
Remove-Item -LiteralPath $changed
Write-Manifest $three
Equal (Outcome $canonical @($PortableRoot, $true)) 'Portable manifest readback failed.' 'canonical missing file'
Equal (Outcome $thisPc @($PortableRoot, $true)) 'Portable release manifest contract is invalid.' 'this-pc missing file'
[IO.File]::WriteAllBytes($changed, $saved)
Write-Output "PASS PS $($PSVersionTable.PSVersion.Major) assertions=$script:Checks"
