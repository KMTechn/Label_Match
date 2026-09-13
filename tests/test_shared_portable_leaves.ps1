param([Parameter(Mandatory = $true)][string]$WorkRoot, [switch]$ArrayManifestCase)
$ErrorActionPreference = 'Stop'
$repo = Split-Path $PSScriptRoot -Parent
[void](New-Item -ItemType Directory -Path $WorkRoot)
$source = Join-Path $WorkRoot 'source'
[void](New-Item -ItemType Directory -Path (Join-Path $source 'tools') -Force)
Copy-Item -LiteralPath (Join-Path $repo 'kmtech_shared') -Destination $source -Recurse
foreach ($name in @('kmtech_shared.lock.json', 'kmtech_shared.manifest.json', 'INSTALL_CANONICAL_PORTABLE.ps1', 'tools/bootstrap_integrity.ps1')) {
    Copy-Item -LiteralPath (Join-Path $repo $name) -Destination (Join-Path $source $name)
}
. (Join-Path $source 'tools/bootstrap_integrity.ps1') -SharedCodeRoot $source
$names = @('Full', 'Sha', 'Manifest', 'ByteSha', 'PinnedFileBytes', 'UInt64BE', 'HexBytes', 'PortableInventory')
$tokens = $null; $errors = $null
$ast = [Management.Automation.Language.Parser]::ParseFile((Join-Path $source 'INSTALL_CANONICAL_PORTABLE.ps1'), [ref]$tokens, [ref]$errors)
if ($errors.Count) { throw 'Canonical installer parse failed.' }
foreach ($name in ($names + @('InvokeFrozenIntegrityProbe'))) {
    $node = $ast.Find({param($n) $n -is [Management.Automation.Language.FunctionDefinitionAst] -and $n.Name -ceq $name}, $true)
    . ([ScriptBlock]::Create($node.Extent.Text))
}
$original = Get-Content -LiteralPath (Join-Path $PSScriptRoot 'fixtures/portable_leaves_before_x13b.ps1') -Raw -Encoding UTF8
$originalNames = $names + @('Get-BootstrapStrictFullPath', 'Get-BootstrapFileSha256', 'Get-BootstrapRelativeCodePath', 'Get-BootstrapInventoryAggregate', 'Sort-BootstrapInventory')
foreach ($name in $originalNames) { $original = [regex]::Replace($original, '\b' + [regex]::Escape($name) + '\b', ('Original' + $name)) }
. ([ScriptBlock]::Create($original))
$script:checks = 0
function Assert($Condition, [string]$Why) {
    if (-not $Condition) { throw "Assertion failed: $Why" }
    $script:checks++
}
function Result([string]$Name, [object[]]$Values) {
    try {
        $value = & $Name @Values
        return [ordered]@{ ok = $true; value = $value } | ConvertTo-Json -Depth 15 -Compress
    } catch {
        $message = $_.Exception.Message
        foreach ($originalName in $originalNames) { $message = $message.Replace(('Original' + $originalName), $originalName) }
        return [ordered]@{ ok = $false; type = $_.Exception.GetType().FullName; message = $message } | ConvertTo-Json -Compress
    }
}
function Parity([string]$Name, [object[]]$Values) {
    $before = Result ('Original' + $Name) $Values
    $after = Result $Name $Values
    Assert ($before -ceq $after) "$Name differs: $before / $after"
}
function Denied([ScriptBlock]$Action, [string]$Fragment) {
    $message = ''
    try { & $Action | Out-Null } catch { $message = $_.Exception.Message }
    Assert ($message.Contains($Fragment)) "Expected rejection '$Fragment', received '$message'"
}
$file = Join-Path $WorkRoot 'abc.txt'
[IO.File]::WriteAllText($file, 'abc', (New-Object Text.UTF8Encoding($false)))
Assert ((Sha $file) -ceq 'ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad') 'known SHA256'
foreach ($value in @($file, (Join-Path $WorkRoot 'missing'), $WorkRoot, '')) {
    Parity 'Sha' @($value)
    Parity 'Get-BootstrapFileSha256' @($value)
}
foreach ($value in @($WorkRoot, "$WorkRoot\a\..\b/", 'relative', '', ' ', 'C:\', '\\?\C:\x', '\\.\C:\x')) {
    Parity 'Full' @($value, 'test path')
    Parity 'Get-BootstrapStrictFullPath' @($value, 'test path')
}
foreach ($value in @($file, "$WorkRoot\nested\한글.txt", $WorkRoot, "$WorkRoot-other\abc.txt", "$WorkRoot\..\escaped")) {
    Parity 'Get-BootstrapRelativeCodePath' @($WorkRoot, $value)
}
$rows = @([pscustomobject]@{path='a.txt'; size=3; sha256=(Sha $file)}, [pscustomobject]@{path='Z.txt'; size=0; sha256=('0' * 64)})
Parity 'Get-BootstrapInventoryAggregate' @(,$rows)
[Array]::Reverse($rows)
Parity 'Get-BootstrapInventoryAggregate' @(,$rows)
Parity 'Get-BootstrapInventoryAggregate' @(,@())
Parity 'Get-BootstrapInventoryAggregate' @($null)

$tree = Join-Path $WorkRoot 'portable'
$required = @('portable-manifest.json', 'runtime/python.exe', 'runtime/pythonw.exe', 'app/main.py',
    'launch-label-match.cmd', 'INSTALL_CANONICAL_PORTABLE.ps1', 'INSTALL_THIS_PC.ps1', 'tools/bootstrap_integrity.ps1', 'tools/label_writer_fence.ps1')
foreach ($name in $required) {
    $target = Join-Path $tree $name
    [void](New-Item -ItemType Directory -Path (Split-Path $target -Parent) -Force)
    [IO.File]::WriteAllText($target, 'synthetic', (New-Object Text.UTF8Encoding($false)))
}
$manifestPath = Join-Path $tree 'portable-manifest.json'
$manifest = [ordered]@{schema='label-match-portable-tree-v1'; entrypoint='runtime/pythonw.exe app/main.py'; launcher='launch-label-match.cmd';
    allowed_unsigned_app_pe=@(); forbidden_package_roots=@(); runtime_pythonw_sha256=(Sha (Join-Path $tree 'runtime/pythonw.exe'));
    launcher_sha256=(Sha (Join-Path $tree 'launch-label-match.cmd'))}
$valid = $manifest | ConvertTo-Json -Compress
[IO.File]::WriteAllText($manifestPath, $valid)
if ($ArrayManifestCase) {
    # Preserve the original rejection; canonical 0.3.0 unwraps pipeline arrays.
    # The coordinator assigns its correction to the separate 0.3.1 bump lane.
    $wrapped = if ($PSVersionTable.PSVersion.Major -eq 5) { "[$valid]" } else { "[[$valid]]" }
    [IO.File]::WriteAllText($manifestPath, $wrapped)
    $originalRejection = Result 'OriginalManifest' @($tree, $true) | ConvertFrom-Json
    Assert (-not $originalRejection.ok -and $originalRejection.message -ceq 'Portable manifest readback failed.') 'original rejects array manifest'
    Parity 'Manifest' @($tree, $true)
    Write-Output "PASS PowerShell $($PSVersionTable.PSVersion): array manifest rejection"
    exit 0
}
Parity 'Manifest' @($tree, $true)
Parity 'Manifest' @($tree, $false) # Real Authenticode rejection of synthetic unsigned bytes.
foreach ($name in $required) {
    $path = Join-Path $tree $name
    Move-Item -LiteralPath $path -Destination "$path.saved"
    Parity 'Manifest' @($tree, $true)
    Move-Item -LiteralPath "$path.saved" -Destination $path
}
foreach ($text in @('{', (' ' * 65537), 'null', '{}', $valid.Replace('label-match-portable-tree-v1', 'wrong-schema'),
    $valid.Replace($manifest.runtime_pythonw_sha256, ('0' * 64)))) {
    [IO.File]::WriteAllText($manifestPath, $text)
    Parity 'Manifest' @($tree, $true)
}
[IO.File]::WriteAllText($manifestPath, $valid)
$link = Join-Path $tree 'linked'
[void](New-Item -ItemType Junction -Path $link -Target $source)
Parity 'Manifest' @($tree, $true)
(Get-Item -LiteralPath $link).Delete()
$script:signatureCalls = @(); $script:invalidSecond = $false
function Get-AuthenticodeSignature([string]$FilePath) {
    $script:signatureCalls += $FilePath
    $status = if ($script:invalidSecond -and $FilePath.EndsWith('pythonw.exe')) { 'NotSigned' } else { 'Valid' }
    return [pscustomobject]@{Status=$status}
}
Parity 'Manifest' @($tree, $false)
Assert ($script:signatureCalls.Count -eq 4 -and $script:signatureCalls[0].EndsWith('python.exe') -and $script:signatureCalls[1].EndsWith('pythonw.exe')) 'signature order'
$script:signatureCalls = @()
Parity 'Manifest' @($tree, $true)
Assert ($script:signatureCalls.Count -eq 0) 'unsigned bypass does not query signatures'
$script:invalidSecond = $true
Parity 'Manifest' @($tree, $false)
Parity 'PortableInventory' @($tree)
foreach ($name in @('A.txt', 'a.txt-other', '한글.txt', 'é.txt', 'z.txt')) { [IO.File]::WriteAllText((Join-Path $tree $name), $name) }
Parity 'PortableInventory' @($tree)
Parity 'Sort-BootstrapInventory' @(,$rows)

# Both source and portable roots must verify the consumer pin before leaf execution.
Assert ((Get-LabelSharedPortableLeafPath $source) -ceq (Join-Path $source 'kmtech_shared\powershell\portable.ps1')) 'source root'
$pack = Join-Path $WorkRoot 'package'
[void](New-Item -ItemType Directory -Path $pack)
Copy-Item -LiteralPath $source -Destination (Join-Path $pack 'app') -Recurse
Assert ((Get-LabelSharedPortableLeafPath $pack) -ceq (Join-Path $pack 'app\kmtech_shared\powershell\portable.ps1')) 'portable root'
$leaf = Join-Path $source 'kmtech_shared\powershell\portable.ps1'
$leafBytes = [IO.File]::ReadAllBytes($leaf)
[IO.File]::WriteAllText($leaf, "throw 'UNTRUSTED_LEAF_RAN'")
Denied { . (Get-LabelSharedPortableLeafPath $source) } 'Shared PowerShell leaf pin mismatch.'
$sharedManifestPath = Join-Path $source 'kmtech_shared.manifest.json'
$manifestBytes = [IO.File]::ReadAllBytes($sharedManifestPath)
$forged = Get-Content -LiteralPath $sharedManifestPath -Raw | ConvertFrom-Json
$forged.files.'kmtech_shared/powershell/portable.ps1' = Sha $leaf
[IO.File]::WriteAllText($sharedManifestPath, ($forged | ConvertTo-Json))
Denied { . (Get-LabelSharedPortableLeafPath $source) } 'Shared manifest pin mismatch.'
$lockPath = Join-Path $source 'kmtech_shared.lock.json'
$lockBytes = [IO.File]::ReadAllBytes($lockPath)
[IO.File]::WriteAllText($lockPath, ('{"version":"0.3.0","manifest_sha256":"' + (Sha $sharedManifestPath) + '"}'))
Denied { . (Get-LabelSharedPortableLeafPath $source) } 'Shared consumer manifest pin mismatch.'
[IO.File]::WriteAllBytes($lockPath, $lockBytes)
[IO.File]::WriteAllBytes($sharedManifestPath, $manifestBytes)
[IO.File]::WriteAllBytes($leaf, $leafBytes)
[IO.File]::WriteAllText($lockPath, (' ' * 65537))
Denied { . (Get-LabelSharedPortableLeafPath $source) } 'Shared consumer lock is oversized.'
[IO.File]::WriteAllBytes($lockPath, $lockBytes)
[IO.File]::WriteAllText($sharedManifestPath, (' ' * 65537))
Denied { . (Get-LabelSharedPortableLeafPath $source) } 'Shared manifest is oversized.'
[IO.File]::WriteAllBytes($sharedManifestPath, $manifestBytes)
Move-Item -LiteralPath $lockPath -Destination "$lockPath.saved"
Denied { . (Get-LabelSharedPortableLeafPath $source) } 'does not exist'
Move-Item -LiteralPath "$lockPath.saved" -Destination $lockPath
Move-Item -LiteralPath $leaf -Destination "$leaf.saved"
Denied { . (Get-LabelSharedPortableLeafPath $source) } 'does not exist'
Move-Item -LiteralPath "$leaf.saved" -Destination $leaf
$linkedRoot = Join-Path $WorkRoot 'linked-source'
[void](New-Item -ItemType Junction -Path $linkedRoot -Target $source)
Denied { . (Get-LabelSharedPortableLeafPath $linkedRoot) } 'reparse point'
Denied { . (Get-LabelSharedPortableLeafPath (Join-Path $linkedRoot 'tools')) } 'reparse point'
(Get-Item -LiteralPath $linkedRoot).Delete()
$packageDir = Join-Path $source 'kmtech_shared'
Move-Item -LiteralPath $packageDir -Destination "$packageDir.saved"
[void](New-Item -ItemType Junction -Path $packageDir -Target "$packageDir.saved")
Denied { . (Get-LabelSharedPortableLeafPath $source) } 'reparse point'
(Get-Item -LiteralPath $packageDir).Delete()
Move-Item -LiteralPath "$packageDir.saved" -Destination $packageDir
# The full helper no longer travels inside -EncodedCommand (Windows limit).
# Execute the real probe against a large pinned helper and a valid synthetic record.
[void](Write-BootstrapIntegrityRecord -Root $tree -CodeRoot $tree)
$integrityHelper = Join-Path $source 'tools/bootstrap_integrity.ps1'
[IO.File]::AppendAllText($integrityHelper, ("`n#" + ('x' * 40000) + "`n"))
$frozen = [pscustomobject]@{root=$source; integrity_sha256=(Sha $integrityHelper)}
InvokeFrozenIntegrityProbe $frozen $tree
Assert $true 'large frozen helper executes through bounded probe'
[IO.File]::AppendAllText($integrityHelper, "`nthrow 'UNTRUSTED_HELPER_RAN'`n")
Denied { InvokeFrozenIntegrityProbe $frozen $tree } 'Pinned file bytes differ:'
Write-Output "PASS PowerShell $($PSVersionTable.PSVersion): $script:checks assertions"
