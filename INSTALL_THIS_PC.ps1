[CmdletBinding()]
param(
    [switch]$DryRun,
    [switch]$Uninstall,
    [string]$SourceRoot = "",
    [string]$InstallRoot = "C:\KMTech\Apps\Label_Match\current",
    [string]$TlsCaBundlePath = "",
    [string]$OperatorLocalAppDataRoot = "",
    [string]$ElevationLogPath = "",
    [string]$ExpectedBootstrapScriptSha256 = "",
    [string]$VerifiedBootstrapScriptPath = "",
    [switch]$BootstrapIntegrityPreloaded,
    [string]$ExpectedSourceAggregateSha256 = "",
    [int]$ExpectedSourceFileCount = 0,
    [uint64]$ExpectedSourceByteCount = 0,
    [switch]$WriterFenceFunctionsPreloaded,
    [string]$WriterFenceControlRoot = "",
    [string]$DataGenerationSubjectSid = "",
    [string]$WriterFenceSessionId = "",
    [string]$WriterFenceAttemptId = "",
    [string]$WriterFenceReplacementTransactionId = "",
    [string]$WriterFenceDelegationToken = "",
    [string]$ExpectedActiveCommit = "",
    [string]$InstallerRunId = "",
    [int]$InstallerOwnerProcessId = 0,
    [string]$InstallerOwnerProcessStartedAtFileTime = "",
    [string]$StartupUpdateRestorePreimage = "",
    [string]$StartupUpdateRestoreSha256 = "",
    [string]$LegacyRelayTransition = "",
    [string]$LegacyRelayPreimageSha256 = "",
    [switch]$ReplaceExistingVerifiedPortable,
    [switch]$AllowNoncanonicalLayoutForTest,
    [switch]$ApplyHardenedAclForTest,
    [string[]]$DataGenerationMarkerPaths = @()
)

$ErrorActionPreference = "Stop"
$ExpectedInstallRoot = "C:\KMTech\Apps\Label_Match\current"
$IntegrityFileName = "bootstrap-integrity.json"
$IntegritySchema = "label-match-bootstrap-integrity-v1"

function Test-KmtechPrivilegedOwnerSid([string]$Sid) {
    return $Sid -cin @('S-1-5-18','S-1-5-32-544','S-1-5-80-956008885-3418522649-1831038044-1853292631-2271478464')
}

function Get-EarlyFileSha256([string]$Path) {
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

$BootstrapScriptPath = if (
    [string]::IsNullOrWhiteSpace($VerifiedBootstrapScriptPath)
) { $MyInvocation.MyCommand.Path } else { $VerifiedBootstrapScriptPath }
if (
    -not [string]::IsNullOrWhiteSpace($ExpectedBootstrapScriptSha256) -and
    (
        $ExpectedBootstrapScriptSha256 -cnotmatch '^[0-9a-f]{64}$' -or
        (Get-EarlyFileSha256 $BootstrapScriptPath) -cne $ExpectedBootstrapScriptSha256
    )
) { throw "Bootstrap script SHA-256 differs from its trusted caller pin." }
$earlyTestOverride = (
    $AllowNoncanonicalLayoutForTest.IsPresent -and
    [string]$env:KMTECH_FACTORY_INSTALL_TEST_MODE -ceq '1'
)
if (
    -not $DryRun.IsPresent -and
    -not $Uninstall.IsPresent -and
    -not $earlyTestOverride -and
    -not $BootstrapIntegrityPreloaded.IsPresent
) {
    throw (
        "Production placement requires integrity functions preloaded by the " +
        "canonical pinned in-memory launcher."
    )
}
$BootstrapIntegrityFunctions = ''
if (
    -not $BootstrapIntegrityPreloaded.IsPresent -and
    -not (-not $DryRun.IsPresent -and $Uninstall.IsPresent -and -not $earlyTestOverride)
) {
    $BootstrapIntegrityFunctions = Join-Path $PSScriptRoot "tools\bootstrap_integrity.ps1"
}
if ($BootstrapIntegrityPreloaded.IsPresent) {
    foreach ($functionName in @(
        'Get-BootstrapFileSha256',
        'Get-BootstrapCodeInventory',
        'Get-BootstrapInventoryAggregate',
        'Write-BootstrapIntegrityRecord',
        'Assert-BootstrapIntegrityRecord',
        'Protect-KmtechCodeAncestors',
        'Assert-LabelProtectedInstallPath',
        'Get-InstallationPreparationExitCode'
    )) {
        if (-not (Get-Command $functionName -CommandType Function -ErrorAction SilentlyContinue)) {
            throw "Preloaded bootstrap integrity producer is incomplete."
        }
    }
}
elseif (-not $DryRun.IsPresent -and $Uninstall.IsPresent -and -not $earlyTestOverride) {
    # Production code-only uninstall does not inspect or execute source-tree helpers.
}
elseif (-not (Test-Path -LiteralPath $BootstrapIntegrityFunctions -PathType Leaf)) {
    throw "Bootstrap integrity producer is unavailable."
}
else {
    . $BootstrapIntegrityFunctions -SharedCodeRoot $PSScriptRoot
    . (Get-LabelSharedPortableLeafPath $PSScriptRoot 'data_generation.ps1')
}
if (-not (Get-Command ConvertTo-KmtechRedactedSinkText -CommandType Function -ErrorAction SilentlyContinue) -and
    -not (-not $DryRun.IsPresent -and $Uninstall.IsPresent -and -not $earlyTestOverride)) {
    . (Join-Path $PSScriptRoot 'tools/failure_redaction.ps1')
}
if ($WriterFenceFunctionsPreloaded.IsPresent) {
    foreach ($functionName in @(
        'Enter-LabelWriterDelegatedOperation',
        'Exit-LabelWriterAdmission'
    )) {
        if (-not (Get-Command $functionName -CommandType Function -ErrorAction SilentlyContinue)) {
            throw "Preloaded writer fence helper is incomplete."
        }
    }
}
$LegacyRelayTaskName = "direct-sync-relay-label-match-current-pc"


function Get-PlacementDataVersion([string]$Root, $Manifest) {
    if (-not [string]::IsNullOrWhiteSpace([string]$Manifest.version)) { return [string]$Manifest.version }
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

function Assert-PlacementDataDowngrade([string]$Root) {
    if ($DataGenerationMarkerPaths.Count -eq 0 -and $testOverride) { return }
    if ($DataGenerationMarkerPaths.Count -eq 0 -or $DataGenerationSubjectSid -cnotmatch '^S-1-\d+(-\d+)+$') {
        throw '자료 판 확인 기록의 대상 사용자를 확인할 수 없습니다. 현재 코드와 자료를 보존하고 관리자 지원 복구를 요청하세요.'
    }
    $manifest = Get-Content -LiteralPath (Join-Path $Root 'portable-manifest.json') -Raw -Encoding UTF8 | ConvertFrom-Json
    $version = Get-PlacementDataVersion $Root $manifest
    $decision = Get-KmtechDataDowngradeDecision -MarkerPaths $DataGenerationMarkerPaths `
        -TargetVersion $version -TargetBuildId ([string]$manifest.source_commit) `
        -AppId 'Label_Match' -SubjectSid $DataGenerationSubjectSid
    if ($decision.allowed -ne $true) { throw ([string]$decision.message) }
}

function Enter-LabelCanonicalPlacement([int]$TimeoutMilliseconds = 5000) {
    # Placement and supported removal share the same exact admission source.
    return Enter-LabelWriterDelegatedOperation -ControlRoot $WriterFenceControlRoot `
        -SessionId $WriterFenceSessionId -AttemptId $WriterFenceAttemptId `
        -ReplacementTransactionId $WriterFenceReplacementTransactionId `
        -DelegationToken $WriterFenceDelegationToken -Source 'canonical_placement' `
        -TimeoutMilliseconds $TimeoutMilliseconds
}

function Get-StrictFullPath([string]$Path, [string]$Purpose) {
    if ([string]::IsNullOrWhiteSpace($Path) -or -not [IO.Path]::IsPathRooted($Path)) {
        throw "$Purpose must be an absolute path."
    }
    if ($Path.StartsWith('\\?\') -or $Path.StartsWith('\\.\')) {
        throw "$Purpose must not use a device namespace."
    }
    $full = [IO.Path]::GetFullPath($Path).TrimEnd('\')
    if ([string]::IsNullOrWhiteSpace($full) -or $full -eq [IO.Path]::GetPathRoot($full)) {
        throw "$Purpose must not be a filesystem root."
    }
    return $full
}

function Test-SamePath([string]$Left, [string]$Right) {
    try {
        $leftFull = Get-StrictFullPath $Left "left path"
        $rightFull = Get-StrictFullPath $Right "right path"
        return $leftFull.Equals($rightFull, [StringComparison]::OrdinalIgnoreCase)
    }
    catch {
        return $false
    }
}

function Assert-NoReparsePoint([string]$Path, [string]$Purpose) {
    if (-not (Test-Path -LiteralPath $Path)) { return }
    $items = @((Get-Item -LiteralPath $Path -Force))
    if ((Get-Item -LiteralPath $Path -Force).PSIsContainer) {
        $items += @(Get-ChildItem -LiteralPath $Path -Force -Recurse)
    }
    foreach ($item in $items) {
        if (($item.Attributes -band [IO.FileAttributes]::ReparsePoint) -ne 0) {
            throw "$Purpose must not contain a reparse point: $($item.FullName)"
        }
    }
}

function Get-FileSha256([string]$Path) {
    return Get-BootstrapFileSha256 $Path
}

function Install-CurrentUserTlsCaBootstrap([string]$SourcePath, [string]$LocalAppDataRoot) {
    if ([string]::IsNullOrWhiteSpace($SourcePath)) { return $null }
    $source = Get-StrictFullPath $SourcePath "TLS CA bundle source"; Assert-NoReparsePoint $source "TLS CA bundle source"
    if (-not (Test-Path -LiteralPath $source -PathType Leaf)) { throw "TLS CA bundle source is unavailable." }
    $sourceLength = (Get-Item -LiteralPath $source -Force).Length
    if ($sourceLength -le 0 -or $sourceLength -gt 131072) { throw "TLS CA bundle source size is invalid." }
    $userRoot = Get-StrictFullPath $LocalAppDataRoot "operator LOCALAPPDATA root"; $target = Join-Path $userRoot "KMTech\Bootstrap\Label_Match\ca-bundle.pem"
    $targetParent = Split-Path -Parent $target; New-Item -ItemType Directory -Path $targetParent -Force | Out-Null
    Assert-NoReparsePoint $targetParent "TLS CA bootstrap directory"
    Copy-Item -LiteralPath $source -Destination $target -Force; Assert-NoReparsePoint $target "TLS CA bootstrap target"
    if ((Get-FileSha256 $target) -cne (Get-FileSha256 $source)) { throw "TLS CA bootstrap exact readback failed." }
    return $target
}
function Assert-AlreadyElevated {
    $identity = [Security.Principal.WindowsIdentity]::GetCurrent()
    $principal = New-Object Security.Principal.WindowsPrincipal($identity)
    if (-not $principal.IsInRole([Security.Principal.WindowsBuiltInRole]::Administrator)) {
        throw (
            "Privileged placement must be launched by the canonical installer's " +
            "pinned in-memory elevation path."
        )
    }
}

function Write-ElevationLog([string]$Status, [string]$Message) {
    if ([string]::IsNullOrWhiteSpace($ElevationLogPath)) { return }
    $path = Get-StrictFullPath $ElevationLogPath "ElevationLogPath"
    New-Item -ItemType Directory -Path (Split-Path -Parent $path) -Force | Out-Null
    $entry = [ordered]@{
        captured_at = (Get-Date).ToUniversalTime().ToString('o')
        process_id = $PID
        elevated = $true
        status = $Status
        message = $Message
    }
    [IO.File]::AppendAllText(
        $path,
        ((ConvertTo-KmtechRedactedSinkObject $entry | ConvertTo-Json -Compress) + [Environment]::NewLine),
        (New-Object Text.UTF8Encoding($false))
    )
}

function Get-RelativeCodePath([string]$Root, [string]$Path) {
    return Get-BootstrapRelativeCodePath -Root $Root -Path $Path
}

function Get-CodeInventory([string]$Root) {
    return Get-BootstrapCodeInventory -Root $Root -IntegrityFileName $IntegrityFileName
}

function Get-InventoryAggregate([object[]]$Inventory) {
    return Get-BootstrapInventoryAggregate -Inventory $Inventory
}

function Write-Utf8Json([string]$Path, $Payload) {
    Write-BootstrapUtf8Json -Path $Path -Payload $Payload
}

function Assert-RequiredRelease([string]$Root, [bool]$AllowUnsignedPortableForTest) {
    $frozenFiles = @('Label_Match.exe', 'contract.lock.json')
    $portableFiles = @(
        'portable-manifest.json',
        'runtime\python.exe',
        'runtime\pythonw.exe',
        'app\main.py',
        'launch-label-match.cmd',
        'INSTALL_CANONICAL_PORTABLE.ps1',
        'INSTALL_THIS_PC.ps1',
        'tools\bootstrap_integrity.ps1'
    )
    $frozen = @($frozenFiles | Where-Object {
        Test-Path -LiteralPath (Join-Path $Root $_) -PathType Leaf
    }).Count -eq $frozenFiles.Count
    $portable = @($portableFiles | Where-Object {
        Test-Path -LiteralPath (Join-Path $Root $_) -PathType Leaf
    }).Count -eq $portableFiles.Count
    if ($frozen -eq $portable) {
        throw "Release layout must be exactly one of FROZEN_EXE or PORTABLE_CPYTHON."
    }
    if ($frozen) { return 'FROZEN_EXE' }

    $manifestPath = Join-Path $Root 'portable-manifest.json'
    if ((Get-Item -LiteralPath $manifestPath -Force).Length -gt 65536) {
        throw "Portable release manifest is oversized."
    }
    try {
        $manifest = Get-Content -LiteralPath $manifestPath -Raw -Encoding UTF8 |
            ConvertFrom-Json
    }
    catch {
        throw "Portable release manifest is invalid."
    }
    if (
        [string]$manifest.schema -cne 'label-match-portable-tree-v1' -or
        [string]$manifest.entrypoint -cne 'runtime/pythonw.exe app/main.py' -or
        [string]$manifest.launcher -cne 'launch-label-match.cmd' -or
        @($manifest.allowed_unsigned_app_pe).Count -ne 0 -or
        @($manifest.forbidden_package_roots).Count -ne 0
    ) {
        throw "Portable release manifest contract is invalid."
    }
    $pythonwPath = Join-Path $Root 'runtime\pythonw.exe'
    $launcherPath = Join-Path $Root 'launch-label-match.cmd'
    if (
        (Get-FileSha256 $pythonwPath) -cne
            ([string]$manifest.runtime_pythonw_sha256).ToLowerInvariant() -or
        (Get-FileSha256 $launcherPath) -cne
            ([string]$manifest.launcher_sha256).ToLowerInvariant()
    ) {
        throw "Portable release manifest hash readback failed."
    }
    # The root integrity record seals this manifest; counting it here would
    # create a manifest/integrity cycle. Its caller pin is checked separately.
    $integrityPath = Join-Path $Root 'bootstrap-integrity.json'
    $metricsBeforeManifest = (
        Get-ChildItem -LiteralPath $Root -File -Force -Recurse |
            Where-Object {
                -not (Test-SamePath $_.FullName $manifestPath) -and
                -not (Test-SamePath $_.FullName $integrityPath)
            } | Measure-Object -Property Length -Sum
    )
    if (
        [int64]$manifest.file_count_before_manifest -ne $metricsBeforeManifest.Count -or
        [int64]$manifest.byte_count_before_manifest -ne [int64]$metricsBeforeManifest.Sum
    ) {
        throw "Portable release tree metrics differ from the manifest."
    }
    if (-not $AllowUnsignedPortableForTest) {
        foreach ($relativePath in @('runtime\python.exe', 'runtime\pythonw.exe')) {
            $signature = Get-AuthenticodeSignature -LiteralPath (Join-Path $Root $relativePath)
            if ([string]$signature.Status -cne 'Valid') {
                throw "Portable CPython signature is not valid: $relativePath"
            }
        }
    }
    return 'PORTABLE_CPYTHON'
}

function ConvertTo-NormalizedAclRights([int64]$Rights) {
    $synchronize = [int64][System.Security.AccessControl.FileSystemRights]::Synchronize
    return $Rights -band (-bnot $synchronize)
}

function Assert-HardenedCodeAcl([string]$Path, [switch]$Recursive) {
    Assert-NoReparsePoint $Path "Hardened code ACL readback"
    $expected = @{
        'S-1-5-18' = [int64][System.Security.AccessControl.FileSystemRights]::FullControl
        'S-1-5-32-544' = [int64][System.Security.AccessControl.FileSystemRights]::FullControl
        'S-1-5-32-545' = [int64][System.Security.AccessControl.FileSystemRights]::ReadAndExecute
    }
    $targets = @((Get-Item -LiteralPath $Path -Force -ErrorAction Stop))
    if ($Recursive.IsPresent) {
        $targets += @(Get-ChildItem -LiteralPath $Path -Force -Recurse -ErrorAction Stop)
    }
    $expectedRootInheritance = [int](
        [System.Security.AccessControl.InheritanceFlags]::ContainerInherit -bor
        [System.Security.AccessControl.InheritanceFlags]::ObjectInherit
    )
    foreach ($target in $targets) {
        $isRoot = Test-SamePath $target.FullName $Path
        $acl = Get-Acl -LiteralPath $target.FullName -ErrorAction Stop
        $owner = $acl.GetOwner([System.Security.Principal.SecurityIdentifier])
        if (-not (Test-KmtechPrivilegedOwnerSid ([string]$owner.Value))) {
            throw "Hardened code ACL owner is not privileged: $($target.FullName)"
        }
        if ($isRoot -and -not $acl.AreAccessRulesProtected) {
            throw "Hardened code root still inherits access rules: $($target.FullName)"
        }
        if (-not $isRoot -and $acl.AreAccessRulesProtected) {
            throw "Hardened code descendant does not inherit the root DACL: $($target.FullName)"
        }
        $actual = @{}
        foreach ($rule in @($acl.GetAccessRules(
            $true,
            $true,
            [System.Security.Principal.SecurityIdentifier]
        ))) {
            $sid = [string]$rule.IdentityReference.Value
            if (
                [string]$rule.AccessControlType -cne 'Allow' -or
                -not $expected.ContainsKey($sid) -or
                ($isRoot -and $rule.IsInherited) -or
                (-not $isRoot -and -not $rule.IsInherited)
            ) {
                throw "Hardened code DACL contains an unexpected ACE for $sid on $($target.FullName)"
            }
            if (
                $isRoot -and (
                    [int]$rule.InheritanceFlags -ne $expectedRootInheritance -or
                    [string]$rule.PropagationFlags -cne 'None'
                )
            ) {
                throw "Hardened code root inheritance flags differ for $sid."
            }
            if (-not $actual.ContainsKey($sid)) { $actual[$sid] = [int64]0 }
            $actual[$sid] = [int64]$actual[$sid] -bor [int64]$rule.FileSystemRights
        }
        if ($actual.Count -ne $expected.Count) {
            throw "Hardened code DACL principal count differs: $($target.FullName)"
        }
        foreach ($sid in $expected.Keys) {
            if (
                -not $actual.ContainsKey($sid) -or
                (ConvertTo-NormalizedAclRights ([int64]$actual[$sid])) -ne
                (ConvertTo-NormalizedAclRights ([int64]$expected[$sid]))
            ) {
                throw "Hardened code DACL rights differ for $sid on $($target.FullName)"
            }
        }
    }
}

function Set-HardenedCodeAcl([string]$Path, [switch]$Recursive) {
    try {
        Assert-NoReparsePoint $Path "Hardened code ACL target"
        $icacls = Join-Path ([Environment]::SystemDirectory) 'icacls.exe'
        $ownerArgs = @($Path, '/setowner', '*S-1-5-32-544', '/L')
        $resetArgs = @($Path, '/reset', '/L')
        if ($Recursive.IsPresent) {
            $ownerArgs += '/T'
            $resetArgs += '/T'
        }
        & $icacls @ownerArgs | Out-Null
        if ($LASTEXITCODE -ne 0) { throw "Hardened code owner assignment failed: $Path" }
        & $icacls @resetArgs | Out-Null
        if ($LASTEXITCODE -ne 0) { throw "Hardened code DACL reset failed: $Path" }
        & $icacls $Path `
            '/inheritance:r' `
            '/grant:r' `
            '*S-1-5-18:(OI)(CI)F' `
            '*S-1-5-32-544:(OI)(CI)F' `
            '*S-1-5-32-545:(OI)(CI)RX' `
            '/L' | Out-Null
        if ($LASTEXITCODE -ne 0) { throw "Hardened code DACL installation failed: $Path" }
        Assert-HardenedCodeAcl $Path -Recursive:$Recursive.IsPresent
    }
    catch {
        Write-Output "acl_readback_status=UNKNOWN"
        throw
    }
}

function Assert-ProtectedCodeAncestors([string]$Path) {
    if (Get-Command Assert-LabelProtectedInstallPath -CommandType Function -ErrorAction SilentlyContinue) {
        Assert-LabelProtectedInstallPath $Path
        return
    }
    function Get-KmtechSharedAncestorAclState($Acl,[IO.FileAttributes]$Attributes,[switch]$RequireProtected) {
        # Pure admission rule. The helper-free removal copy is checked by AST.
        function Get-KmtechAncestorAclShapeKey($Value) {
            (@($Value.GetAccessRules($true,$true,[Security.Principal.SecurityIdentifier]) | ForEach-Object {
                [string]$_.IdentityReference.Value+'|'+[int64]$_.FileSystemRights+'|'+[string]$_.AccessControlType+'|'+[int]$_.InheritanceFlags+'|'+[int]$_.PropagationFlags+'|'+[int]$_.IsInherited
            } | Sort-Object) -join ';')
        }
        $reason=''
        if($Attributes -band [IO.FileAttributes]::ReparsePoint){$reason='폴더가 재분석 지점입니다'}
        $owner=$Acl.GetOwner([Security.Principal.SecurityIdentifier]).Value
        $rules=@($Acl.GetAccessRules($true,$true,[Security.Principal.SecurityIdentifier]))
        $shape=@(
            'S-1-5-32-544|2032127|Allow|3|0', 'S-1-5-18|2032127|Allow|3|0',
            'S-1-5-32-545|1179817|Allow|3|0', 'S-1-5-11|-536805376|Allow|3|2'
        )
        foreach($rule in $rules){if([string]$rule.AccessControlType -cne 'Allow'){$reason='거부 권한 항목이 있습니다'}}
        $actualKey=Get-KmtechAncestorAclShapeKey $Acl
        $defaultKey=(@(($shape+@('S-1-5-11|1245631|Allow|0|0')) | ForEach-Object {$_+'|1'} | Sort-Object) -join ';')
        $targetKey=(@(($shape+@('S-1-5-11|1179823|Allow|0|0')) | ForEach-Object {$_+'|0'} | Sort-Object) -join ';')
        $defaultShape=($rules.Count -eq 5 -and -not $Acl.AreAccessRulesProtected -and $actualKey -ceq $defaultKey)
        $targetShape=($rules.Count -eq 5 -and $Acl.AreAccessRulesProtected -and $actualKey -ceq $targetKey)
        if(-not(Test-KmtechPrivilegedOwnerSid $owner)){$reason='폴더 소유자가 예상과 다릅니다'}
        if(-not $reason -and -not $defaultShape -and -not $targetShape){$reason='권한 모양이 지원하는 기본값 또는 보호된 값과 다릅니다'}
        if(-not $reason -and $RequireProtected -and -not $targetShape){$reason='상위 폴더가 보호된 목표 모양이 아닙니다'}
        return [pscustomobject]@{reason=$reason;owner=$owner;shape_key=$actualKey;target_key=$targetKey;
            default_shape=$defaultShape;target_shape=$targetShape}
    }
    $full = [IO.Path]::GetFullPath($Path).TrimEnd('\')
    if($full -cmatch '^[A-Za-z]:$'){$full+='\'}
    $leaf = $true
    while ($full) {
        if($full -in @('C:\KMTech','C:\KMTech\Apps')) {
            # No installed/source helper is loaded at the standalone boundary.
            # Get-Item fails closed for missing/denied paths; admission is read-only.
            $entry=Get-Item -LiteralPath $full -ErrorAction Stop
            $acl=Get-Acl -LiteralPath $full -ErrorAction Stop
            $state=Get-KmtechSharedAncestorAclState $acl $entry.Attributes -RequireProtected
            if($state.reason){throw "설치 폴더 권한을 확인하세요: $full ($($state.reason)). 지원 담당자에게 문의하세요."}
        }
        elseif (Test-Path -LiteralPath $full) {
            $entry = Get-Item -LiteralPath $full -ErrorAction Stop
            if ($entry.Attributes -band [IO.FileAttributes]::ReparsePoint) { throw 'Unsafe update path component.' }
            $acl = Get-Acl -LiteralPath $full -ErrorAction Stop
            if (-not (Test-KmtechPrivilegedOwnerSid ($acl.GetOwner([Security.Principal.SecurityIdentifier]).Value))) {
                throw 'Update path component owner is not privileged.'
            }
            $mask = if ($leaf) { 0x500D0156 } else { 0x500D0150 }
            foreach ($rule in $acl.GetAccessRules($true,$true,[Security.Principal.SecurityIdentifier])) {
                if ($rule.AccessControlType -eq 'Allow' -and
                    -not ($rule.PropagationFlags -band [Security.AccessControl.PropagationFlags]::InheritOnly) -and
                    $rule.IdentityReference.Value -notin @('S-1-5-18','S-1-5-32-544') -and
                    ([int64]$rule.FileSystemRights -band $mask)) { throw 'Update path component is user mutable.' }
            }
        }
        if ($full -eq [IO.Path]::GetPathRoot($full)) { break }
        $full = [IO.Path]::GetDirectoryName($full)
        $leaf = $false
    }
}


function Initialize-LabelProtectedInstallBase([string]$InstallRoot) {
    $application = Split-Path -Parent $InstallRoot
    $Script:LabelAncestorPreparationEvents = [Collections.Generic.List[object]]::new()
    Assert-LabelProtectedInstallPath $application -AllowSupportedPreparation
    [void](Protect-KmtechCodeAncestors $application -BeforeAppCreation)
    try {
        if (-not (Test-Path -LiteralPath $application)) {
            [void](New-Item -ItemType Directory -Path $application -ErrorAction Stop)
            Set-HardenedCodeAcl $application
        }
    }
    catch {
        if ((Get-InstallationPreparationExitCode $_) -eq 66) {
            $_.Exception.Data['installation_preparation_exit_code'] = 64
        }
        throw
    }
    Assert-ProtectedCodeAncestors $application
}


function Protect-LabelInstallerOwnerFile([string]$Path) {
    & (Join-Path ([Environment]::SystemDirectory) 'icacls.exe') $Path '/setowner' '*S-1-5-32-544' '/L' | Out-Null
    if ($LASTEXITCODE -ne 0) { throw 'INSTALLER_OWNER_PROTECTION_FAILED' }
    Assert-ProtectedCodeAncestors $Path
}

function Write-LabelInstallerOwnerRecord([string]$InstallRoot) {
    if ($InstallerRunId -cnotmatch '^\d{8}T\d{9}Z-[0-9a-f]{32}$' -or
        $InstallerOwnerProcessId -le 0 -or $InstallerOwnerProcessStartedAtFileTime -cnotmatch '^[1-9][0-9]{0,18}$') {
        throw 'INSTALLER_OWNER_BINDING_INVALID'
    }
    $owner = [Diagnostics.Process]::GetProcessById($InstallerOwnerProcessId)
    try {
        if ($owner.HasExited -or $owner.StartTime.ToUniversalTime().ToFileTimeUtc().ToString() -cne $InstallerOwnerProcessStartedAtFileTime) {
            throw 'INSTALLER_OWNER_PROCESS_DIFFERS'
        }
    } finally { $owner.Dispose() }
    $lease = Enter-LabelCanonicalPlacement -TimeoutMilliseconds 15000
    try {
        $active = Read-LabelWriterFence $WriterFenceControlRoot
        if ([string]$active.session_id -cne $WriterFenceSessionId -or
            [string]$active.attempt_id -cne $WriterFenceAttemptId -or
            [string]$active.replacement_transaction_id -cne $WriterFenceReplacementTransactionId) {
            throw 'INSTALLER_OWNER_FENCE_DIFFERS'
        }
        $parent = Split-Path -Parent $InstallRoot
        Assert-ProtectedCodeAncestors $parent
        $path = Join-Path $parent ('.installer-owner-' + $WriterFenceSessionId + '-' + ($InstallerRunId -split '-')[-1] + '.json')
        Assert-NoReparsePoint $path 'installer owner record'
        $record = [ordered]@{schema='label-match-installer-owner-v1';run_id=$InstallerRunId;
            install_root=$InstallRoot;control_root=([IO.Path]::GetFullPath($WriterFenceControlRoot));
            owner_process_id=$InstallerOwnerProcessId;owner_process_started_at_filetime=$InstallerOwnerProcessStartedAtFileTime}
        foreach ($key in @('session_id','attempt_id','replacement_transaction_id','writer_inventory_sha256',
            'orchestrator_sha256','writer_contract_sha256','session_started_at_utc')) { $record[$key] = $active.$key }
        $json = $record | ConvertTo-Json -Depth 5 -Compress
        if (Test-Path -LiteralPath $path) {
            Assert-ProtectedCodeAncestors $path
            if ((Get-Item -LiteralPath $path).Length -gt 65536 -or
                ((Get-Content -LiteralPath $path -Raw -Encoding UTF8 | ConvertFrom-Json | ConvertTo-Json -Depth 5 -Compress) -cne $json)) {
                throw 'INSTALLER_OWNER_RECORD_DIFFERS'
            }
            return
        }
        $stream = [IO.File]::Open($path, 'CreateNew', 'Write', 'Read')
        try { $bytes=[Text.Encoding]::UTF8.GetBytes($json);$stream.Write($bytes,0,$bytes.Length);$stream.Flush($true) }
        finally { $stream.Dispose() }
        # Existing code-parent DACL supplies the protected inherited access.
        Protect-LabelInstallerOwnerFile $path
        if ((Get-Content -LiteralPath $path -Raw -Encoding UTF8) -cne $json) { throw 'INSTALLER_OWNER_READBACK_DIFFERS' }
    } finally { Exit-LabelWriterAdmission $lease }
}

function Assert-LabelStartupUpdateTrust([string]$Root) {
    Assert-ProtectedCodeAncestors (Split-Path -Parent $Root)
    Assert-HardenedCodeAcl $Root -Recursive
}

function Get-LabelStartupUpdateTaskSpec([string]$Root, [string]$Sid) {
    Assert-LabelStartupUpdateTrust $Root
    $runtime = Join-Path $Root 'runtime\python.exe'
    $implementation = Join-Path $Root 'app\auto_update.py'
    $output = & $runtime -I -B $implementation --print-logon-task --install-root $Root --operator-sid $Sid
    if ($LASTEXITCODE -ne 0) { throw 'STARTUP_UPDATE_TASK_SPEC_FAILED' }
    $spec = ($output -join "`n") | ConvertFrom-Json
    if ([string]$spec.name -cnotmatch '^KMTech\.LabelMatch\.StartupUpdate\.[0-9a-f]{24}$' -or
        $spec.xml -isnot [string]) { throw 'STARTUP_UPDATE_TASK_SPEC_INVALID' }
    return $spec
}


function Invoke-LabelStartupUpdateTaskMutation([scriptblock]$Action) {
    $lease=Enter-LabelWriterDelegatedOperation -ControlRoot $WriterFenceControlRoot -SessionId $WriterFenceSessionId -AttemptId $WriterFenceAttemptId -ReplacementTransactionId $WriterFenceReplacementTransactionId -DelegationToken $WriterFenceDelegationToken -Source 'auto_update_task_mutation'
    try { & $Action } finally { Exit-LabelWriterAdmission $lease }
}
function ConvertTo-LabelTaskSid([string]$Identity) {
    if ([string]::IsNullOrWhiteSpace($Identity)) { throw 'TASK_USER_ID_UNRESOLVED' }
    if ($Identity -match '^S-1-') {
        return [Security.Principal.SecurityIdentifier]::new($Identity).Value
    }
    return [Security.Principal.NTAccount]::new($Identity).Translate([Security.Principal.SecurityIdentifier]).Value
}

function Assert-LabelStartupUpdateTaskDefinition([xml]$Actual, [xml]$Expected) {
    # Bind identity on copies; preserve the raw preimage and all other XML.
    $Actual = $Actual.CloneNode($true)
    $Expected = $Expected.CloneNode($true)
    foreach ($document in @($Actual, $Expected)) {
        $identityNamespace = [Xml.XmlNamespaceManager]::new($document.NameTable)
        $identityNamespace.AddNamespace('t','http://schemas.microsoft.com/windows/2004/02/mit/task')
        # Task Scheduler omits these exact schema defaults on export. Remove
        # only a single, attribute-free literal true on copies; false, complex
        # or repeated nodes still differ. Keep all other ownership XML strict.
        foreach ($xpath in @('/t:Task/t:Triggers/t:LogonTrigger/t:Enabled','/t:Task/t:Settings/t:AllowStartOnDemand')) {
            $nodes = @($document.SelectNodes($xpath,$identityNamespace))
            if ($nodes.Count -eq 1 -and $nodes[0].Attributes.Count -eq 0 -and
                $nodes[0].ChildNodes.Count -eq 1 -and $nodes[0].FirstChild.NodeType -eq [Xml.XmlNodeType]::Text -and
                $nodes[0].get_InnerText() -ceq 'true') { [void]$nodes[0].ParentNode.RemoveChild($nodes[0]) }
        }
        foreach ($xpath in @('/t:Task/t:Principals/t:Principal/t:UserId','/t:Task/t:Triggers/t:LogonTrigger/t:UserId')) {
            $nodes = @($document.SelectNodes($xpath,$identityNamespace))
            if ($nodes.Count -ne 1 -or $nodes[0].ChildNodes.Count -ne 1 -or
                $nodes[0].FirstChild.NodeType -ne [Xml.XmlNodeType]::Text) { throw 'STARTUP_UPDATE_OWNED_TASK_DIFFERS' }
            $nodes[0].set_InnerText((ConvertTo-LabelTaskSid ([string]$nodes[0].get_InnerText())))
        }
    }
    $namespace=New-Object Xml.XmlNamespaceManager($Actual.NameTable)
    $namespace.AddNamespace('t','http://schemas.microsoft.com/windows/2004/02/mit/task')
    foreach ($pair in @(@('/t:Task/t:Actions/*',1),@('/t:Task/t:Triggers/*',1),@('/t:Task/t:Principals/*',1))) {
        if ($Actual.SelectNodes([string]$pair[0],$namespace).Count -ne [int]$pair[1]) { throw 'STARTUP_UPDATE_OWNED_TASK_DIFFERS' }
    }
    foreach ($xpath in @('/t:Task/t:Actions/@Context','/t:Task/t:Actions/t:Exec/t:Command','/t:Task/t:Actions/t:Exec/t:Arguments','/t:Task/t:Actions/t:Exec/t:WorkingDirectory',
        '/t:Task/t:Principals/t:Principal/@id','/t:Task/t:Principals/t:Principal/t:UserId','/t:Task/t:Principals/t:Principal/t:LogonType','/t:Task/t:Principals/t:Principal/t:RunLevel',
        '/t:Task/t:Triggers/t:LogonTrigger/t:UserId','/t:Task/t:Triggers/t:LogonTrigger/t:Enabled','/t:Task/t:Triggers/t:LogonTrigger/t:Delay',
        '/t:Task/t:Settings/t:MultipleInstancesPolicy','/t:Task/t:Settings/t:Hidden','/t:Task/t:Settings/t:ExecutionTimeLimit','/t:Task/t:Settings/t:AllowHardTerminate',
        '/t:Task/t:Settings/t:StartWhenAvailable','/t:Task/t:Settings/t:AllowStartOnDemand','/t:Task/t:Settings/t:DisallowStartIfOnBatteries','/t:Task/t:Settings/t:StopIfGoingOnBatteries')) {
        $a=@($Actual.SelectNodes($xpath,$namespace)); $b=@($Expected.SelectNodes($xpath,$namespace))
        if ($a.Count -ne $b.Count -or $a.Count -gt 1) { throw 'STARTUP_UPDATE_OWNED_TASK_DIFFERS' }
        if ($a.Count -eq 0) {
            if ($xpath -cnotin @('/t:Task/t:Actions/t:Exec/t:WorkingDirectory','/t:Task/t:Principals/t:Principal/t:LogonType',
                '/t:Task/t:Triggers/t:LogonTrigger/t:Enabled','/t:Task/t:Triggers/t:LogonTrigger/t:Delay','/t:Task/t:Settings/t:AllowStartOnDemand')) {
                throw 'STARTUP_UPDATE_OWNED_TASK_DIFFERS'
            }
            continue
        }
        # CLR getters avoid PowerShell's XML property adaptation; both engines
        # use the same typed node/count comparison, including attributes.
        if ([string]$a[0].get_InnerText() -cne [string]$b[0].get_InnerText()) { throw 'STARTUP_UPDATE_OWNED_TASK_DIFFERS' }
    }
    # Reject extra executable/trigger/principal properties, including repetition.
    foreach ($xpath in @('/t:Task/t:Actions','/t:Task/t:Triggers','/t:Task/t:Principals')) {
        $a=$Actual.SelectSingleNode($xpath,$namespace); $b=$Expected.SelectSingleNode($xpath,$namespace)
        if ($null -eq $a -or $null -eq $b -or [string]$a.get_InnerXml() -cne [string]$b.get_InnerXml()) { throw 'STARTUP_UPDATE_OWNED_TASK_DIFFERS' }
    }
}

function Get-LabelStartupUpdateTask([string]$Name) {
    # A denied/failed query is UNKNOWN, never an absent owned task.
    $matches=@(Get-ScheduledTask -ErrorAction Stop | Where-Object { [string]$_.TaskName -ceq $Name -and [string]$_.TaskPath -ceq '\' })
    if ($matches.Count -gt 1) { throw 'STARTUP_UPDATE_TASK_OWNERSHIP_AMBIGUOUS' }
    if ($matches.Count -eq 1) { return $matches[0] }
    return $null
}

function Get-LabelStartupUpdateTaskPreimage([string]$Root) {
    $spec = Get-LabelStartupUpdateTaskSpec $Root $OperatorSid
    $existing = Get-LabelStartupUpdateTask ([string]$spec.name)
    $result = [ordered]@{name=[string]$spec.name;exists=($null -ne $existing);xml='';sddl='';expected_xml=[string]$spec.xml}
    if ($null -ne $existing) {
        if ([string]$existing.State -ceq 'Running') { throw 'STARTUP_UPDATE_TASK_IS_RUNNING' }
        $service = New-Object -ComObject 'Schedule.Service'
        $service.Connect()
        $registered = $service.GetFolder('\').GetTask($spec.name)
        if ([int]$registered.GetInstances(0).Count -ne 0) { throw 'STARTUP_UPDATE_TASK_IS_RUNNING' }
        $actual = [xml]$registered.Xml
        $expected = [xml]$spec.xml
        Assert-LabelStartupUpdateTaskDefinition $actual $expected
        $result.xml = [string]$registered.Xml
        $result.sddl = [string]$registered.GetSecurityDescriptor(7)
    }
    return $result
}


function Write-LabelStartupUpdateRecord([string]$Path, $Payload) {
    if (-not ('KMTechStartupUpdateAtomicFile' -as [type])) {
        Add-Type -TypeDefinition @'
using System.Runtime.InteropServices;
public static class KMTechStartupUpdateAtomicFile {
 [DllImport("kernel32.dll", CharSet=CharSet.Unicode, SetLastError=true)]
 public static extern bool MoveFileExW(string source, string target, uint flags);
}
'@
    }
    Assert-NoReparsePoint $Path 'startup update record'
    $temporary = $Path + '.tmp.' + [guid]::NewGuid().ToString('N')
    $bytes = (New-Object Text.UTF8Encoding($false)).GetBytes(($Payload | ConvertTo-Json -Depth 12) + "`n")
    $stream = New-Object IO.FileStream($temporary,[IO.FileMode]::CreateNew,[IO.FileAccess]::Write,[IO.FileShare]::None,4096,[IO.FileOptions]::WriteThrough)
    try { $stream.Write($bytes,0,$bytes.Length); $stream.Flush($true) } finally { $stream.Dispose() }
    if (-not [KMTechStartupUpdateAtomicFile]::MoveFileExW($temporary,$Path,9)) { throw 'STARTUP_UPDATE_DURABLE_RECORD_FAILED' }
}


function Save-LabelStartupUpdatePreimage([string]$Root, [string]$Purpose) {
    Assert-LabelStartupUpdateTrust $Root
    $managed = Get-StrictFullPath (Split-Path -Parent $Root) 'startup update managed parent'
    $directory = Join-Path $managed ('.startup-update-' + $Purpose + '.' + $WriterFenceAttemptId)
    if (-not (($directory.StartsWith($managed+'\',[StringComparison]::OrdinalIgnoreCase))) -or (Test-Path -LiteralPath $directory)) { throw 'STARTUP_UPDATE_PREIMAGE_PATH_INVALID' }
    New-Item -ItemType Directory -Path $directory | Out-Null
    Set-HardenedCodeAcl $directory -Recursive
    $files = @()
    foreach ($name in @('policy.json','active.json','transition.json')) {
        $path = Join-Path $managed ('update-control\' + $name)
        Assert-NoReparsePoint $path 'startup update control preimage'
        $exists = Test-Path -LiteralPath $path -PathType Leaf
        $hash = ''
        if ($exists) {
            Copy-Item -LiteralPath $path -Destination (Join-Path $directory $name)
            $hash = Get-FileSha256 $path
            if ((Get-FileSha256 (Join-Path $directory $name)) -cne $hash) { throw 'STARTUP_UPDATE_PREIMAGE_READBACK_FAILED' }
        }
        $files += [ordered]@{name=$name;exists=[bool]$exists;sha256=$hash}
    }
    $task = Get-LabelStartupUpdateTaskPreimage $Root
    $payload = [ordered]@{schema='label-match-startup-update-install-preimage-v1';root=$Root;operator_sid=$OperatorSid;attempt_id=$WriterFenceAttemptId;files=$files;task=$task}
    Write-LabelStartupUpdateRecord (Join-Path $directory 'preimage.json') $payload
    return [pscustomobject]@{path=$directory;sha256=(Get-FileSha256 (Join-Path $directory 'preimage.json'))}
}


function Restore-LabelStartupUpdatePreimage([string]$Root, [string]$Directory, [string]$ExpectedSha256) {
    # The current tree may already have been removed or restored. This operation
    # executes only the pinned, preloaded helper and reads the hardened preimage.
    # Keep ancestor and preimage ACL/reparse checks without depending on current.
    Assert-ProtectedCodeAncestors (Split-Path -Parent $Root)
    $managed = Get-StrictFullPath (Split-Path -Parent $Root) 'startup update restore parent'
    $full = Get-StrictFullPath $Directory 'startup update preimage'
    if (-not (($full.StartsWith($managed+'\',[StringComparison]::OrdinalIgnoreCase))) -or (Split-Path -Leaf $full) -cnotmatch '^\.startup-update-(install|uninstall)\.[0-9a-f]{32}$') { throw 'STARTUP_UPDATE_RESTORE_PATH_INVALID' }
    Assert-NoReparsePoint $full 'startup update preimage'
    Assert-HardenedCodeAcl $full -Recursive
    $path = Join-Path $full 'preimage.json'
    if ($ExpectedSha256 -cnotmatch '^[0-9a-f]{64}$' -or (Get-FileSha256 $path) -cne $ExpectedSha256) { throw 'STARTUP_UPDATE_RESTORE_PREIMAGE_PIN_DIFFERS' }
    $saved = Get-Content -LiteralPath $path -Raw -Encoding UTF8 | ConvertFrom-Json
    if ([string]$saved.schema -cne 'label-match-startup-update-install-preimage-v1' -or
        -not (Test-SamePath ([string]$saved.root) $Root) -or [string]$saved.operator_sid -cne $OperatorSid -or
        (@($saved.files | ForEach-Object { [string]$_.name }) -join ',') -cne 'policy.json,active.json,transition.json') { throw 'STARTUP_UPDATE_RESTORE_BINDING_DIFFERS' }
    $currentTask = Get-LabelStartupUpdateTask ([string]$saved.task.name)
    if ($null -ne $currentTask -and [string]$currentTask.State -ceq 'Running') { throw 'STARTUP_UPDATE_TASK_IS_RUNNING' }
    if ($null -ne $currentTask) {
        Assert-LabelStartupUpdateTaskDefinition ([xml](Export-ScheduledTask -TaskName ([string]$saved.task.name))) ([xml]$saved.task.expected_xml)
        $service=New-Object -ComObject 'Schedule.Service'; $service.Connect()
        if ([int]$service.GetFolder('\').GetTask([string]$saved.task.name).GetInstances(0).Count -ne 0) { throw 'STARTUP_UPDATE_TASK_IS_RUNNING' }
    }

    $restoreErrors=[Collections.Generic.List[object]]::new()
    $control = Join-Path $managed 'update-control'
    foreach ($item in @($saved.files)) {
        try {
        $target = Join-Path $control ([string]$item.name)
        Assert-NoReparsePoint $target 'startup update restore target'
        if (Test-Path -LiteralPath $target) {
            Move-Item -LiteralPath $target -Destination (Join-Path $full ('failed-' + [guid]::NewGuid().ToString('N') + '-' + $item.name))
        }
        if ($item.exists -isnot [bool]) { throw 'STARTUP_UPDATE_RESTORE_EXISTENCE_INVALID' }
        if ($item.exists) {
            $original = Join-Path $full ([string]$item.name)
            if ((Get-FileSha256 $original) -cne [string]$item.sha256) { throw 'STARTUP_UPDATE_RESTORE_BYTES_DIFFER' }
            Copy-Item -LiteralPath $original -Destination $target
            if ((Get-FileSha256 $target) -cne [string]$item.sha256) { throw 'STARTUP_UPDATE_RESTORE_READBACK_FAILED' }
        }
        } catch { $restoreErrors.Add($_) }
    }
    try {
    Invoke-LabelStartupUpdateTaskMutation {
        $name = [string]$saved.task.name
        if ($name -cnotmatch '^KMTech\.LabelMatch\.StartupUpdate\.[0-9a-f]{24}$') { throw 'STARTUP_UPDATE_RESTORE_TASK_NAME_INVALID' }
        $existing = Get-LabelStartupUpdateTask $name
        if ($null -ne $existing -and [string]$existing.State -ceq 'Running') { throw 'STARTUP_UPDATE_TASK_IS_RUNNING' }
        if ($saved.task.exists -isnot [bool]) { throw 'STARTUP_UPDATE_RESTORE_TASK_EXISTENCE_INVALID' }
        if ($saved.task.exists) {
            Register-ScheduledTask -TaskName $name -Xml ([string]$saved.task.xml) -Force | Out-Null
            $service = New-Object -ComObject 'Schedule.Service'; $service.Connect()
            $registered = $service.GetFolder('\').GetTask($name)
            $registered.SetSecurityDescriptor([string]$saved.task.sddl,0)
            if ([string]$registered.GetSecurityDescriptor(7) -cne [string]$saved.task.sddl -or [string]$registered.Xml -cne [string]$saved.task.xml) { throw 'STARTUP_UPDATE_RESTORE_TASK_READBACK_FAILED' }
        } elseif ($null -ne $existing) {
            Unregister-ScheduledTask -TaskName $name -Confirm:$false
            if ($null -ne (Get-LabelStartupUpdateTask $name)) { throw 'STARTUP_UPDATE_RESTORE_TASK_REMOVAL_FAILED' }
        }
    } | Out-Null
    } catch { $restoreErrors.Add($_) }
    if($restoreErrors.Count){
        $errorRecord=$restoreErrors[0]
        $errorRecord.Exception.Data['startup_restore_failures']=@($restoreErrors | ForEach-Object {
            [ordered]@{type=$_.Exception.GetType().FullName;message=ConvertTo-KmtechRedactedSinkText $_.Exception.Message}
        })
        throw $errorRecord
    }
}


function Assert-LabelStartupUpdateSettings([object]$Settings, [string]$UpdatePolicyPath) {
    $names=@($Settings.PSObject.Properties.Name)
    $allowed=@('feed_url','channel','apply_during_work','profile_auth')
    if (@($names | Where-Object { $_ -cnotin $allowed }).Count -or @($allowed | Where-Object { $_ -cnotin $names }).Count) { throw 'STARTUP_UPDATE_SETTINGS_FIELDS_INVALID' }
    if ($Settings.feed_url -isnot [string] -or $Settings.channel -isnot [string] -or $Settings.channel -cnotmatch '^[a-z0-9_-]{1,32}$' -or $Settings.profile_auth -isnot [bool] -or $Settings.apply_during_work -isnot [bool] -or $Settings.apply_during_work) { throw 'STARTUP_UPDATE_SETTINGS_FORMAT_INVALID' }
    if ($Settings.feed_url) {
        $uri=$null
        if (-not [Uri]::TryCreate($Settings.feed_url,[UriKind]::Absolute,[ref]$uri) -or $uri.Scheme -cne 'https' -or -not $uri.Host -or $uri.UserInfo -or $uri.Fragment) { throw 'STARTUP_UPDATE_SETTINGS_HTTPS_REQUIRED' }
        $feedAuthority=[regex]::Match([string]$Settings.feed_url,'^https://([^/?#]+)',[Text.RegularExpressions.RegexOptions]::IgnoreCase).Groups[1].Value
        if ($feedAuthority.Contains('@')) { throw 'STARTUP_UPDATE_SETTINGS_HTTPS_REQUIRED' }
        if ($feedAuthority -match '[^\x00-\x7f]') { throw '업데이트 주소의 호스트는 ASCII IP 또는 도메인이어야 합니다.' }
        if ($uri.Query) {
            # Consume the runtime's literal policy; do not maintain a second list.
            $policy = Get-Content -LiteralPath $UpdatePolicyPath -Raw -Encoding UTF8
            $keys = [regex]::Match($policy, '(?ms)^UPDATE_SECRET_QUERY_KEYS = \{(?<body>.*?)^\}').Groups['body'].Value
            $prefixes = [regex]::Match($policy, '(?m)^UPDATE_SECRET_QUERY_PREFIXES = \((?<body>[^\r\n]*)\)\r?$').Groups['body'].Value
            $literal = '"([a-z_]+)"'
            if (-not $keys -or -not $prefixes -or [regex]::Replace($keys,$literal,'') -notmatch '^[,\s]*$' -or [regex]::Replace($prefixes,$literal,'') -notmatch '^[,\s]*$') { throw 'STARTUP_UPDATE_SETTINGS_QUERY_POLICY_INVALID' }
            $secretKeys = @([regex]::Matches($keys,$literal) | ForEach-Object { $_.Groups[1].Value })
            $secretPrefixes = @([regex]::Matches($prefixes,$literal) | ForEach-Object { $_.Groups[1].Value })
            $queryUrl = $Settings.feed_url -replace '[\r\n\t]', '' # urllib.parse URL preprocessing
            foreach ($part in (($queryUrl -split '\?',2)[1] -split '&')) {
                $name = [Uri]::UnescapeDataString(($part -split '=',2)[0].Replace('+',' ')).ToLowerInvariant().Replace('-','_')
                if ($name -cin $secretKeys -or @($secretPrefixes | Where-Object { $name.StartsWith($_,[StringComparison]::Ordinal) }).Count) { throw 'STARTUP_UPDATE_SETTINGS_TOKEN_QUERY_FORBIDDEN' }
            }
        }
        $hostName=$uri.DnsSafeHost.ToLowerInvariant().TrimEnd('.')
        if (@(@('github.com','githubusercontent.com','github.io') | Where-Object { $hostName -ceq $_ -or $hostName.EndsWith('.'+$_) }).Count) { throw 'STARTUP_UPDATE_SETTINGS_GITHUB_FORBIDDEN' }
        if (-not $Settings.profile_auth) { throw 'STARTUP_UPDATE_SETTINGS_AUTH_REQUIRED' }
    }
}
function Initialize-LabelStartupUpdate([string]$Root) {
    $settingsPath = Join-Path $Root 'app\auto-update-settings.json'
    if (-not (Test-Path -LiteralPath $settingsPath -ErrorAction Stop)) { return }
    if (-not (Test-Path -LiteralPath $settingsPath -PathType Leaf -ErrorAction Stop)) { throw 'STARTUP_UPDATE_SETTINGS_NOT_A_FILE' }
    if ($testOverride) { Write-Output 'startup_update_status=NOT_TESTED_HOST'; return }
    Assert-LabelStartupUpdateTrust $Root
    $settings = Get-Content -LiteralPath $settingsPath -Raw -Encoding UTF8 | ConvertFrom-Json
    Assert-LabelStartupUpdateSettings $settings (Join-Path $Root 'app\auto_update.py')
    $Script:StartupUpdatePreimage = Save-LabelStartupUpdatePreimage $Root 'install'
    $managed = Get-StrictFullPath (Split-Path -Parent $Root) 'startup update managed root'
    Assert-NoReparsePoint $managed 'startup update managed root'
    $control = Join-Path $managed 'update-control'
    $versions = Join-Path $managed 'versions'
    foreach ($directory in @($control,$versions)) {
        if (-not (Test-Path -LiteralPath $directory)) { New-Item -ItemType Directory -Path $directory | Out-Null }
        Assert-NoReparsePoint $directory 'startup update protected directory'
        Set-HardenedCodeAcl $directory
    }
    $settings = Get-Content -LiteralPath $settingsPath -Raw -Encoding UTF8 | ConvertFrom-Json
    $manifest = Get-Content -LiteralPath (Join-Path $Root 'portable-manifest.json') -Raw -Encoding UTF8 | ConvertFrom-Json
    if ([string]$manifest.source_commit -cnotmatch '^[0-9a-f]{40}$' -or $manifest.startup_update_protocol -ne 1) {
        throw 'STARTUP_UPDATE_INITIAL_VERSION_INVALID'
    }
    $version = Join-Path $versions ([string]$manifest.source_commit)
    if (-not (Test-Path -LiteralPath $version)) {
        Copy-Item -LiteralPath $Root -Destination $version -Recurse
        if ((Get-InventoryAggregate @(Get-CodeInventory $version)) -cne (Get-InventoryAggregate @(Get-CodeInventory $Root))) {
            throw 'STARTUP_UPDATE_INITIAL_VERSION_READBACK_FAILED'
        }
        [void](Write-BootstrapIntegrityRecord -Root $version -CodeRoot $version)
        Set-HardenedCodeAcl $version -Recursive
    } else {
        [void](Assert-BootstrapIntegrityRecord $version)
        if ((Get-InventoryAggregate @(Get-CodeInventory $version)) -cne (Get-InventoryAggregate @(Get-CodeInventory $Root))) {
            throw 'STARTUP_UPDATE_VERSION_ALREADY_HAS_DIFFERENT_BYTES'
        }
    }
    $hiberboot = $null
    $hiberbootReadStatus = 'UNPROVEN'
    try {
        $hiberboot = (Get-ItemProperty -LiteralPath 'HKLM:\SYSTEM\CurrentControlSet\Control\Session Manager\Power' -Name HiberbootEnabled -ErrorAction Stop).HiberbootEnabled
        $hiberbootReadStatus = 'READ'
    } catch { $hiberbootReadStatus = 'UNREADABLE' }
    $policy = [ordered]@{
        schema='label-match-startup-update-policy-v1'
        settings_sha256=(Get-FileSha256 $settingsPath)
        feed_url=[string]$settings.feed_url;channel=[string]$settings.channel
        profile_auth=[bool]$settings.profile_auth
        apply_during_work=$false
        operator_sid=$OperatorSid;operator_local_app_data=$OperatorLocalAppDataRoot
        staging_root=(Join-Path $OperatorLocalAppDataRoot 'KMTech\Label_Match\updates')
        hiberboot_enabled=$hiberboot;hiberboot_read_status=$hiberbootReadStatus
    }
    if ($settings.apply_during_work -isnot [bool] -or $settings.apply_during_work) { throw 'STARTUP_UPDATE_DURING_WORK_FORBIDDEN' }
    $policyPath = Join-Path $control 'policy.json'
    if (Test-Path -LiteralPath $policyPath) {
        $old = Get-Content -LiteralPath $policyPath -Raw -Encoding UTF8 | ConvertFrom-Json
        if ([string]$old.operator_sid -cne $OperatorSid -or -not (Test-SamePath ([string]$old.operator_local_app_data) $OperatorLocalAppDataRoot)) {
            throw 'STARTUP_UPDATE_OPERATOR_BINDING_DIFFERS'
        }
        Copy-Item -LiteralPath $policyPath -Destination (Join-Path $control ('policy-before-' + $WriterFenceAttemptId + '.json'))
    }
    $failedShas = @()
    $oldTransitionPath = Join-Path $Script:StartupUpdatePreimage.path 'transition.json'
    if (Test-Path -LiteralPath $oldTransitionPath) {
        $oldTransition = Get-Content -LiteralPath $oldTransitionPath -Raw -Encoding UTF8 | ConvertFrom-Json
        $failedShas = @($oldTransition.failed_shas)
    }
    Write-LabelStartupUpdateRecord (Join-Path $control 'transition.json') ([ordered]@{schema='label-match-startup-update-v1';phase='DEFERRED';deferred_at=(Get-Date).ToUniversalTime().ToString('o');reason='canonical installation';failed_shas=$failedShas})
    Write-LabelStartupUpdateRecord $policyPath $policy
    Write-LabelStartupUpdateRecord (Join-Path $control 'active.json') ([ordered]@{schema='label-match-startup-update-v1';sha=[string]$manifest.source_commit;transition_id='CANONICAL_INSTALL'})
    $spec = Get-LabelStartupUpdateTaskSpec $Root $OperatorSid
    Invoke-LabelStartupUpdateTaskMutation {
            $existing = Get-LabelStartupUpdateTask ([string]$spec.name)
            if ($null -ne $existing) {
                $oldXml = [xml](Export-ScheduledTask -TaskName $spec.name)
                $newXml = [xml]$spec.xml
                Assert-LabelStartupUpdateTaskDefinition $oldXml $newXml
                if ([string]$existing.State -ceq 'Running') { throw 'STARTUP_UPDATE_TASK_IS_RUNNING' }
            }
            Register-ScheduledTask -TaskName $spec.name -Xml $spec.xml -Force | Out-Null
            $service = New-Object -ComObject 'Schedule.Service'
            $service.Connect()
            $registered = $service.GetFolder('\').GetTask($spec.name)
            $registered.SetSecurityDescriptor(('D:P(A;;GA;;;SY)(A;;GA;;;BA)(A;;GRGX;;;' + $OperatorSid + ')'),0)
            $actual = [xml]$registered.Xml
            $expected = [xml]$spec.xml
            Assert-LabelStartupUpdateTaskDefinition $actual $expected
        } | Out-Null
    Write-Output 'startup_update_status=REGISTERED_LOGON_SYSTEM_CODE_ONLY'
    Write-Output ('startup_update_task=' + $spec.name)
    Write-Output ('hiberboot_enabled=' + $hiberboot)
    Write-Output ('hiberboot_read_status=' + $hiberbootReadStatus)
}


function Get-LegacyTaskByNameFailClosed([string]$Name) {
    try {
        $taskMatches = @(Get-ScheduledTask -ErrorAction Stop | Where-Object {
            ([string]$_.TaskName).Equals($Name, [StringComparison]::OrdinalIgnoreCase)
        })
    }
    catch {
        throw "Legacy scheduled task observation failed: $Name/$($_.Exception.GetType().Name)"
    }
    if ($taskMatches.Count -gt 1) {
        throw "Legacy scheduled task observation is non-unique: $Name"
    }
    return $taskMatches
}

function Get-LabelOfficialLegacyTaskExpectation {
    # Dynamic SYSTEM: public v2.0.61-63 wrapper:44-64 / pack:164-203,201-221.
    # Fixed SYSTEM: public v2.0.64/66 wrapper:44-65 / pack:164-211.
    # Earlier public releases have no SYSTEM default; same-operator modes are coordinator-approved.
    $pc = [regex]::Replace(([string]$env:COMPUTERNAME).Trim(),'[^A-Za-z0-9._-]+','-').Trim('.','-','_')
    if (-not $pc) { $pc = 'worker-pc' }
    if ($pc.Length -gt 96) { $pc = $pc.Substring(0,96).Trim('.','-','_') }
    try { $machine = [string](Get-ItemProperty -Path 'HKLM:\SOFTWARE\Microsoft\Cryptography' -Name MachineGuid -ErrorAction Stop).MachineGuid }
    catch { $machine = [string]$env:COMPUTERNAME + '|' + [string]$env:USERDOMAIN }
    $hash = [Security.Cryptography.SHA256]::Create()
    try { $suffix = ([BitConverter]::ToString($hash.ComputeHash([Text.Encoding]::UTF8.GetBytes($machine)))).Replace('-','').Substring(0,12).ToLowerInvariant() }
    finally { $hash.Dispose() }
    $hostId = ('label-match-'+$pc+'-'+$suffix).ToLowerInvariant()
    $name = 'direct-sync-relay-'+$hostId
    $wrapper = 'C:\ProgramData\KMTech\DirectSync\'+$hostId+'\bin\run_'+$name+'.vbs'
    return [ordered]@{
        'task.path'='\';'task.name'=$name;'action.count'='1';'action.type'='Exec'
        'action.command'=(Join-Path $env:SystemRoot 'System32\wscript.exe').ToLowerInvariant()
        'action.arguments'=('//B //NoLogo '+$wrapper.ToLowerInvariant());'action.workdir'=''
        'principal.user'='SYSTEM';'principal.logon_type'='ServiceAccount';'principal.run_level'='LeastPrivilege'
        'trigger.count'='1';'trigger.type'='TimeTrigger';'trigger.repetition_interval'='PT1M'
    }
}

function Get-LabelOfficialLegacyPrincipalModes {
    # One switch point: remove a mode if the guest proves enabled-only mutation unsafe.
    return @('SYSTEM','Password','InteractiveToken')
}

function Get-LabelOfficialLegacyTaskExpectations {
    $dynamic=Get-LabelOfficialLegacyTaskExpectation
    $fixed=[ordered]@{}
    foreach($key in $dynamic.Keys){$fixed[$key]=$dynamic[$key]}
    $fixed['task.name']='direct-sync-relay-label-match'
    $fixed['action.arguments']='//B //NoLogo c:\programdata\kmtech\directsync\label_match\bin\run_direct-sync-relay-label-match.vbs'
    $result=@()
    foreach($base in @($dynamic,$fixed)){
        foreach($mode in @(Get-LabelOfficialLegacyPrincipalModes)){
            $item=[ordered]@{}
            foreach($key in $base.Keys){$item[$key]=$base[$key]}
            if($mode -cne 'SYSTEM'){
                $item['principal.user']='CURRENT_USER'
                $item['principal.logon_type']=$mode
            }
            $result+=,$item
        }
    }
    return $result
}

function ConvertTo-LabelLegacyTaskPath([string]$Value) {
    if (-not $Value) { return '' }
    $text = [Environment]::ExpandEnvironmentVariables($Value.Trim().Trim('"')).Replace('/','\')
    if ($text -ieq 'wscript.exe') { $text = Join-Path $env:SystemRoot 'System32\wscript.exe' }
    return [IO.Path]::GetFullPath($text).TrimEnd('\').ToLowerInvariant()
}

function Get-LabelOfficialLegacyTaskFingerprint([string]$TaskName,[string]$TaskPath,[string]$Xml) {
    try {
    [xml]$node = $Xml
    $root = $node.DocumentElement
    $actions = @($root.SelectNodes("*[local-name()='Actions']/*"))
    $principals = @($root.SelectNodes("*[local-name()='Principals']/*"))
    $triggers = @($root.SelectNodes("*[local-name()='Triggers']/*"))
    if ($actions.Count -ne 1 -or $principals.Count -ne 1 -or $triggers.Count -ne 1) { throw 'Healthy lifecycle task belongs to another principal/command.' }
    $action = $actions[0];$principal=$principals[0];$trigger=$triggers[0]
    $sid = ConvertTo-LabelTaskSid ([string]$principal.UserId)
    $actor=[Security.Principal.WindowsIdentity]::GetCurrent().User.Value
    $user=if($sid -ceq 'S-1-5-18'){'SYSTEM'}elseif($sid -ceq $actor){'CURRENT_USER'}else{throw 'Healthy lifecycle task belongs to another principal/command.'}
    $expectations=@(Get-LabelOfficialLegacyTaskExpectations | Where-Object {
        [string]$_['task.name'] -ieq $TaskName -and [string]$_['principal.user'] -ceq $user -and
        [string]$_['principal.logon_type'] -ceq [string]$principal.LogonType
    })
    if($expectations.Count -ne 1){throw 'Healthy lifecycle task belongs to another principal/command.'}
    $expected=$expectations[0]
    $arguments = [regex]::Replace(([string]$action.Arguments).Trim(),'\s+',' ')
    if ($arguments -cnotmatch '^//B //NoLogo (.+)$') { throw 'Healthy lifecycle task belongs to another principal/command.' }
    $arguments = '//B //NoLogo '+(ConvertTo-LabelLegacyTaskPath $Matches[1])
    $actual = [ordered]@{
        'task.path'=($TaskPath.TrimEnd('\')+'\').ToLowerInvariant();'task.name'=$TaskName.ToLowerInvariant()
        'action.count'=[string]$actions.Count;'action.type'=$action.LocalName
        'action.command'=(ConvertTo-LabelLegacyTaskPath ([string]$action.Command))
        'action.arguments'=$arguments;'action.workdir'=(ConvertTo-LabelLegacyTaskPath ([string]$action.WorkingDirectory))
        'principal.user'=$user;'principal.logon_type'=[string]$principal.LogonType;'principal.run_level'=[string]$principal.RunLevel
        'trigger.count'=[string]$triggers.Count;'trigger.type'=$trigger.LocalName;'trigger.repetition_interval'=[string]$trigger.Repetition.Interval
    }
    $fields=[ordered]@{};$lines=@()
    $hash = [Security.Cryptography.SHA256]::Create()
    try {
        foreach ($key in @($expected.Keys | Sort-Object)) {
            if ([string]$actual[$key] -cne [string]$expected[$key]) { throw 'Healthy lifecycle task belongs to another principal/command.' }
            $line=$key+'='+[string]$actual[$key]+[string][char]10
            $fields[$key]=([BitConverter]::ToString($hash.ComputeHash([Text.Encoding]::UTF8.GetBytes($line)))).Replace('-','').ToLowerInvariant()
            $lines+=$line
        }
        $aggregate=([BitConverter]::ToString($hash.ComputeHash([Text.Encoding]::UTF8.GetBytes(($lines -join ''))))).Replace('-','').ToLowerInvariant()
    } finally { $hash.Dispose() }
    return [pscustomobject]@{fields=$fields;sha256=$aggregate;matched=$true}
    } catch { throw 'Healthy lifecycle task belongs to another principal/command.' }
}

function Get-LabelOfficialLegacyTaskSnapshot {
    try {
    $expected=Get-LabelOfficialLegacyTaskExpectation
    $tasks=@(Get-ScheduledTask -ErrorAction Stop | Where-Object {
        [string]$_.TaskName -like 'direct-sync-relay-label-match-*' -or
        ([string]$_.TaskName -ieq 'direct-sync-relay-label-match' -and
            @($_.Actions | Where-Object { [string]$_.Execute -match '(?i)wscript\.exe"?$' }).Count -gt 0)
    })
    if ($tasks.Count -gt 1) { throw 'Healthy lifecycle task belongs to another principal/command.' }
    if ($tasks.Count -eq 0) { return $null }
    $task=$tasks[0]
    $xml=[string](Export-ScheduledTask -TaskName ([string]$task.TaskName) -TaskPath ([string]$task.TaskPath) -ErrorAction Stop)
    $fingerprint=Get-LabelOfficialLegacyTaskFingerprint ([string]$task.TaskName) ([string]$task.TaskPath) $xml
    $hash=[Security.Cryptography.SHA256]::Create()
    try { $sha=([BitConverter]::ToString($hash.ComputeHash([Text.Encoding]::UTF8.GetBytes($xml)))).Replace('-','').ToLowerInvariant() }
    finally { $hash.Dispose() }
    return [pscustomobject]@{name=[string]$task.TaskName;path=[string]$task.TaskPath;xml=$xml;xml_sha256=$sha;enabled=[bool]$task.Settings.Enabled;fingerprint=$fingerprint}
    } catch { throw 'Healthy lifecycle task belongs to another principal/command.' }
}

function Assert-LabelOfficialLegacyTaskStopped([string]$Name) {
    $service=New-Object -ComObject 'Schedule.Service';$service.Connect()
    $task=$service.GetFolder('\').GetTask($Name)
    if ([int]$task.GetInstances(0).Count -ne 0) { throw 'LEGACY_RELAY_STOP_UNPROVEN' }
    $expectations=@(Get-LabelOfficialLegacyTaskExpectations | Where-Object { [string]$_['task.name'] -ieq $Name })
    if($expectations.Count -lt 1){throw 'Healthy lifecycle task belongs to another principal/command.'}
    $expected=$expectations[0]
    $wrapper=([string]$expected['action.arguments']).Substring('//B //NoLogo '.Length)
    $stateRoot=Split-Path -Parent (Split-Path -Parent $wrapper)
    $writers=@(Get-CimInstance Win32_Process -ErrorAction Stop | Where-Object {
        [string]$_.CommandLine -like ('*'+$stateRoot+'*') -and [string]$_.CommandLine -match 'direct[_-]sync[_-]relay'
    })
    if ($writers.Count) { throw 'LEGACY_RELAY_STOP_UNPROVEN' }
}

function Invoke-LabelOfficialLegacyTaskTransition([string]$Mode,[string]$Directory,[string]$ExpectedXmlSha256) {
    if ($Mode -cnotin @('Disable','Restore') -or $ExpectedXmlSha256 -cnotmatch '^[0-9a-f]{64}$') { throw 'LEGACY_RELAY_TRANSITION_INVALID' }
    $path=Join-Path $Directory 'preimage.xml'
    $expected=Get-LabelOfficialLegacyTaskExpectation
    if ($Mode -ceq 'Disable') {
        $before=Get-LabelOfficialLegacyTaskSnapshot
        if ($null -eq $before -or $before.xml_sha256 -cne $ExpectedXmlSha256) { throw 'LEGACY_RELAY_PREIMAGE_DIFFERS' }
        if (-not (Test-Path -LiteralPath $Directory)) { New-Item -ItemType Directory -Path $Directory | Out-Null }
        Set-HardenedCodeAcl $Directory -Recursive
        if (-not (Test-Path -LiteralPath $path)) {
            $stream=[IO.File]::Open($path,'CreateNew','Write','Read')
            try {$bytes=[Text.Encoding]::UTF8.GetBytes($before.xml);$stream.Write($bytes,0,$bytes.Length);$stream.Flush($true)}finally{$stream.Dispose()}
        }
        if ((Get-FileSha256 $path) -cne $ExpectedXmlSha256) { throw 'LEGACY_RELAY_PREIMAGE_DIFFERS' }
        $metadataPath=Join-Path $Directory 'preimage.json'
        if(-not (Test-Path -LiteralPath $metadataPath)){
            [IO.File]::WriteAllText($metadataPath,([ordered]@{task_name=$before.name;path=$before.path;enabled=$before.enabled;xml_sha256=$before.xml_sha256;fingerprint_sha256=$before.fingerprint.sha256}|ConvertTo-Json),[Text.UTF8Encoding]::new($false))
        }
        Assert-LabelOfficialLegacyTaskStopped $before.name
        Disable-ScheduledTask -TaskName $before.name -TaskPath $before.path -ErrorAction Stop | Out-Null
        Assert-LabelOfficialLegacyTaskStopped $before.name
        $after=Get-LabelOfficialLegacyTaskSnapshot
        if ($after.enabled -or $after.fingerprint.sha256 -cne $before.fingerprint.sha256) { throw 'LEGACY_RELAY_DISABLED_READBACK_FAILED' }
    } else {
        Assert-HardenedCodeAcl $Directory -Recursive
        Assert-NoReparsePoint $Directory 'legacy relay preimage'
        if ((Get-FileSha256 $path) -cne $ExpectedXmlSha256) { throw 'LEGACY_RELAY_PREIMAGE_DIFFERS' }
        $xml=[IO.File]::ReadAllText($path,[Text.UTF8Encoding]::new($false,$true))
        $metadata=Get-Content -LiteralPath (Join-Path $Directory 'preimage.json') -Raw -Encoding UTF8 | ConvertFrom-Json
        $name=[string]$metadata.task_name
        $fingerprint=Get-LabelOfficialLegacyTaskFingerprint $name '\' $xml
        if([string]$metadata.xml_sha256 -cne $ExpectedXmlSha256 -or $fingerprint.sha256 -cne [string]$metadata.fingerprint_sha256){throw 'LEGACY_RELAY_PREIMAGE_DIFFERS'}
        [xml]$original=$xml
        $enabled=[Convert]::ToBoolean([string]$original.Task.Settings.Enabled)
        $current=Get-LabelOfficialLegacyTaskSnapshot
        if($null -eq $current -or $current.fingerprint.sha256 -cne $fingerprint.sha256){throw 'LEGACY_RELAY_RESTORE_DEFINITION_UNPROVEN'}
        Assert-LabelOfficialLegacyTaskStopped $current.name
        # Enabled-only restoration preserves stored task credentials. No registration/deletion.
        if($current.enabled -ne $enabled){
            if($enabled){Enable-ScheduledTask -TaskName $name -TaskPath '\' -ErrorAction Stop | Out-Null}
            else{Disable-ScheduledTask -TaskName $name -TaskPath '\' -ErrorAction Stop | Out-Null}
        }
        $after=Get-LabelOfficialLegacyTaskSnapshot
        if ($after.enabled -ne $enabled -or $after.fingerprint.sha256 -cne $fingerprint.sha256) { throw 'LEGACY_RELAY_RESTORE_READBACK_FAILED' }
    }
}

function Get-OwnedLegacyTaskRetirementSnapshot([string]$Name) {
    if ($Name -ine 'direct-sync-relay-label-match-current-pc') {
        throw 'Unsupported historical Label task name.'
    }
    $taskMatches = @(Get-LegacyTaskByNameFailClosed $Name)
    if ($taskMatches.Count -eq 0) { return $null }
    $task = $taskMatches[0]
    $actions = @($task.Actions)
    $ownedVbsLauncher = 'C:\ProgramData\KMTech\DirectSync\label-match-margin-r2\bin\run_direct-sync-relay-label-match-current-pc.vbs'
    $ownedCmdLauncher = 'C:\ProgramData\KMTech\DirectSync\label-match-margin-r2\bin\run_direct-sync-relay-label-match-current-pc.ps1'
    if (
        $actions.Count -ne 1 -or [string]$task.TaskPath -cne '\' -or
        (ConvertTo-LabelTaskSid ([string]$task.Principal.UserId)) -cne 'S-1-5-18' -or
        [string]$task.Principal.LogonType -cne 'ServiceAccount' -or
        [string]$task.Principal.RunLevel -notin @('Limited', 'Highest') -or
        $null -eq $task.Settings.Enabled -or
        -not [string]::IsNullOrEmpty([string]$actions[0].WorkingDirectory)
    ) { throw "Refusing to remove a historical task with different ownership: $Name" }
    $execute = [string]$actions[0].Execute
    $arguments = [string]$actions[0].Arguments
    $ownedVbs = (
        $execute -in @('wscript.exe', (Join-Path $env:SystemRoot 'System32\wscript.exe')) -and
        $arguments -in @("//B //NoLogo $ownedVbsLauncher", ('//B //NoLogo "' + $ownedVbsLauncher + '"'))
    )
    $ownedPowerShell = (
        $execute -in @('powershell.exe', (Join-Path $env:SystemRoot 'System32\WindowsPowerShell\v1.0\powershell.exe')) -and
        $arguments -in @("-NoProfile -ExecutionPolicy Bypass -File $ownedCmdLauncher", ('-NoProfile -ExecutionPolicy Bypass -File "' + $ownedCmdLauncher + '"'))
    )
    if (-not ($ownedVbs -or $ownedPowerShell)) {
        throw "Refusing to remove a historical task with a different command: $Name"
    }
    [xml]$definition = Export-ScheduledTask -TaskName $Name -TaskPath '\' -ErrorAction Stop
    if ($null -eq $definition.Task.Settings.Enabled) {
        throw "Historical task enabled state is missing from its definition: $Name"
    }
    # Disabling is the sole permitted definition change during retirement.
    $definition.Task.Settings.Enabled = 'false'
    return [pscustomobject]@{ Enabled = [bool]$task.Settings.Enabled; Definition = $definition.OuterXml }
}

function Remove-OwnedLegacyTask([string]$Name) {
    $before = Get-OwnedLegacyTaskRetirementSnapshot $Name
    if ($null -eq $before) { return }
    $service = New-Object -ComObject 'Schedule.Service'
    $service.Connect()
    $folder = $service.GetFolder('\')
    if ($before.Enabled) {
        Disable-ScheduledTask -TaskName $Name -TaskPath '\' -ErrorAction Stop | Out-Null
    }
    $deadline = [DateTime]::UtcNow.AddSeconds(120)
    do {
        $current = Get-OwnedLegacyTaskRetirementSnapshot $Name
        if ($null -eq $current -or $current.Enabled -or $current.Definition -cne $before.Definition) {
            throw "Historical task changed during normal retirement: $Name"
        }
        $instances = $folder.GetTask($Name).GetInstances(0)
        if ($null -eq $instances -or $null -eq $instances.Count -or [int]$instances.Count -lt 0) {
            throw "Historical task instance absence is unknown: $Name"
        }
        if ([int]$instances.Count -eq 0) { break }
        if ([DateTime]::UtcNow -ge $deadline) { throw "Historical task did not finish naturally: $Name" }
        Start-Sleep -Milliseconds 200
    } while ($true)
    $final = Get-OwnedLegacyTaskRetirementSnapshot $Name
    if ($null -eq $final -or $final.Enabled -or $final.Definition -cne $before.Definition) {
        throw "Historical task changed after its last instance finished: $Name"
    }
    Unregister-ScheduledTask `
        -TaskName $Name `
        -TaskPath '\' `
        -Confirm:$false `
        -ErrorAction Stop
    if (@(Get-LegacyTaskByNameFailClosed $Name).Count -ne 0) {
        throw "Legacy scheduled task removal readback failed: $Name"
    }
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

function Test-CurrentUserRelayPersistencePresent {
    $runKey = 'Software\Microsoft\Windows\CurrentVersion\Run'
    $key = Open-LabelRunKey $runKey
    if ($null -eq $key) { return $false }
    try {
        if ('KMTech.LabelMatch.Relay' -notin $key.GetValueNames()) { return $false }
        $value = $key.GetValue('KMTech.LabelMatch.Relay', $null, [Microsoft.Win32.RegistryValueOptions]::DoNotExpandEnvironmentNames)
        if ([string]::IsNullOrWhiteSpace([string]$value)) { throw 'Current-user relay Run value is empty.' }
        return $true
    }
    finally { $key.Dispose() }
}

function Get-LabelSupportedStartupTasks([string]$Root) {
    $full = [IO.Path]::GetFullPath($Root)
    $tasks = @(Get-ScheduledTask -ErrorAction Stop | Where-Object {
        [string]$_.TaskName -cmatch '^KMTech\.LabelMatch\.StartupUpdate\.[0-9a-f]{24}$'
    })
    $owned = @()
    foreach ($task in $tasks) {
        $actions = @($task.Actions); $triggers = @($task.Triggers)
        if ($actions.Count -ne 1 -or $triggers.Count -ne 1) { throw 'SUPPORTED_TASK_OWNERSHIP_UNKNOWN' }
        $sid = ConvertTo-LabelTaskSid ([string]$triggers[0].UserId)
        if ($sid -cnotmatch '^S-1-5-21-(?:[0-9]+-){3}[0-9]+$') { throw 'SUPPORTED_TASK_USER_UNKNOWN' }
        $sha = [Security.Cryptography.SHA256]::Create()
        try { $digest = ([BitConverter]::ToString($sha.ComputeHash([Text.Encoding]::UTF8.GetBytes($full.ToLowerInvariant()+[char]10+$sid)))).Replace('-','').ToLowerInvariant() }
        finally { $sha.Dispose() }
        $expectedName = 'KMTech.LabelMatch.StartupUpdate.' + $digest.Substring(0,24)
        if ([string]$task.TaskName -cne $expectedName) { throw 'SUPPORTED_TASK_NAME_DIFFERS' }
        $expectedCommand = Join-Path ([Environment]::SystemDirectory) 'WindowsPowerShell\v1.0\powershell.exe'
        $helper = Join-Path $full 'tools\apply_prepared_update.ps1'
        $expectedArguments = '-NoLogo -NoProfile -NonInteractive -WindowStyle Hidden -File "' + $helper + '" -InstallRoot "' + $full + '"'
        if ([string]$task.TaskPath -cne '\' -or
            (ConvertTo-LabelTaskSid ([string]$task.Principal.UserId)) -cne 'S-1-5-18' -or
            [string]$task.Principal.LogonType -cne 'ServiceAccount' -or
            [string]$task.Principal.RunLevel -cne 'Highest' -or
            -not ([string]$actions[0].Execute).Equals($expectedCommand,[StringComparison]::OrdinalIgnoreCase) -or
            [string]$actions[0].Arguments -cne $expectedArguments -or
            -not [string]::IsNullOrEmpty([string]$actions[0].WorkingDirectory)) {
            throw 'SUPPORTED_TASK_ACTION_DIFFERS'
        }
        $owned += $task
    }
    return $owned
}

function Remove-LabelScheduledTasks($Folder, [string]$EvidenceRoot, [string]$Current) {
    try { $official=Get-LabelOfficialLegacyTaskSnapshot }
    catch { throw 'Refusing to remove a historical task with different ownership/command.' }
    if($null -ne $official){
        $directory=Join-Path $EvidenceRoot 'official-legacy-relay'
        Invoke-LabelOfficialLegacyTaskTransition 'Disable' $directory $official.xml_sha256
    }

    foreach ($task in @(Get-LabelSupportedStartupTasks $Current)) {
        $name = [string]$task.TaskName
        $native = $Folder.GetTask($name)
        Write-LabelStartupUpdateRecord -Path (Join-Path $EvidenceRoot ($name + '.json')) -Payload @{
            name=$name;xml=$native.Xml;sddl=$native.GetSecurityDescriptor(7)
        }
        $native.Stop(0)
        $Folder.DeleteTask($name,0)
        try { [void]$Folder.GetTask($name); throw 'REMOVAL_TASK_ABSENCE_NOT_PROVEN' }
        catch [Runtime.InteropServices.COMException] { if ($_.Exception.HResult -ne -2147024894) { throw } }
        catch [IO.FileNotFoundException] { if ($_.Exception.HResult -ne -2147024894) { throw } }
    }
    $legacy = Get-OwnedLegacyTaskRetirementSnapshot 'direct-sync-relay-label-match-current-pc'
    if ($null -ne $legacy) {
        Write-LabelStartupUpdateRecord -Path (Join-Path $EvidenceRoot 'legacy-task.json') -Payload $legacy
        Remove-OwnedLegacyTask 'direct-sync-relay-label-match-current-pc'
    }
    if (@(Get-LabelSupportedStartupTasks $Current).Count -ne 0) { throw 'REMOVAL_TASK_ABSENCE_NOT_PROVEN' }
}
function Get-LabelRemovalIoPath([string]$Path) {
    if ($Path.Length -lt 240 -or $Path.StartsWith('\\?\')) { return $Path }
    $native = $Path.Replace('/','\')
    if ($native.StartsWith('\\')) { return '\\?\UNC\' + $native.Substring(2) }
    return '\\?\' + $native
}

function Assert-LabelRemovalBoundary([string]$Path) {
    # Inspect ancestors, never enumerate opaque control/code children.
    $component = [IO.Path]::GetFullPath((Get-LabelRemovalIoPath $Path))
    while ($component) {
        if (Test-Path -LiteralPath (Get-LabelRemovalIoPath $component) -ErrorAction Stop) {
            $item = Get-Item -LiteralPath (Get-LabelRemovalIoPath $component) -Force -ErrorAction Stop
            if (($item.Attributes -band [IO.FileAttributes]::ReparsePoint) -ne 0) { throw 'REMOVAL_BOUNDARY_REDIRECTED' }
        }
        $component = [IO.Path]::GetDirectoryName($component)
    }
}

function Assert-LabelRemovalQuiescent([string]$ApplicationRoot, [int[]]$ExcludeProcessIds = @()) {
    $prefix = (Get-StrictFullPath $ApplicationRoot 'removal process root') + '\'
    $rows = @(Get-CimInstance Win32_Process -ErrorAction Stop)
    if ($rows.Count -eq 0) { throw 'REMOVAL_PROCESS_OBSERVATION_UNKNOWN' }
    foreach ($row in $rows) {
        if ($null -eq $row.ProcessId) { throw 'REMOVAL_PROCESS_OBSERVATION_UNKNOWN' }
        if ([int]$row.ProcessId -eq $PID -or [int]$row.ProcessId -in $ExcludeProcessIds) { continue }
        if (([string]$row.ExecutablePath).StartsWith($prefix, 'OrdinalIgnoreCase') -or
            ([string]$row.CommandLine).IndexOf($prefix, [StringComparison]::OrdinalIgnoreCase) -ge 0) {
            throw 'REMOVAL_REQUIRES_PRODUCT_PROCESS_STOP'
        }
    }
}

function Test-LabelRemovalFile([string]$Path, [string]$Sha256) {
    try {
        Assert-LabelRemovalBoundary $Path
        if (-not (Test-Path -LiteralPath (Get-LabelRemovalIoPath $Path) -ErrorAction Stop)) { return $false }
        $file = Get-Item -LiteralPath (Get-LabelRemovalIoPath $Path) -Force -ErrorAction Stop
        if ($file.PSIsContainer -or $Sha256 -cnotmatch '^[0-9a-f]{64}$') { return $false }
        $sha = [Security.Cryptography.SHA256]::Create()
        $reader = $null
        try {
            $reader = [IO.File]::OpenRead((Get-LabelRemovalIoPath $Path))
            return ([BitConverter]::ToString($sha.ComputeHash($reader))).Replace('-','').ToLowerInvariant() -ceq $Sha256
        } finally { if ($null -ne $reader) { $reader.Dispose() }; $sha.Dispose() }
    } catch {
        if ($_.Exception.Message -ceq 'REMOVAL_BOUNDARY_REDIRECTED') { return $false }
        # Unknown content cannot retire the inventory needed for the next try.
        throw "제거 미완 — 사용 중인 파일: $Path [REMOVAL_FILE_OBSERVATION_UNKNOWN]"
    }
}

function Get-LabelArtifactFilePlan([string]$Path, [switch]$Incoming) {
    # No registration census can prove that an opaque tree contains only code.
    # Read only the manifest and its declared files, never unknown subtrees.
    $empty = @{artifacts=@();hashes=@{};metadata_roots=@{};directories=@();retained_paths=@($Path);
        decisions=@(@{path=$Path;disposition='UNREADABLE_INVENTORY_RETAINED'})}
    try {
        Assert-LabelRemovalBoundary $Path
        $folder = Get-Item -LiteralPath (Get-LabelRemovalIoPath $Path) -Force -ErrorAction Stop
        if (-not $folder.PSIsContainer) { return $empty }
        $inventoryName = if ($Incoming) { 'manifest.json' } else { 'bootstrap-integrity.json' }
        $inventoryPath = Join-Path $Path $inventoryName
        Assert-LabelRemovalBoundary $inventoryPath
        $inventoryFile = Get-Item -LiteralPath (Get-LabelRemovalIoPath $inventoryPath) -Force -ErrorAction Stop
        if ($inventoryFile.PSIsContainer -or $inventoryFile.Length -gt 1048576) { return $empty }
        $raw = [IO.File]::ReadAllBytes((Get-LabelRemovalIoPath $inventoryPath))
        $sha = [Security.Cryptography.SHA256]::Create()
        try { $inventoryHash = ([BitConverter]::ToString($sha.ComputeHash($raw))).Replace('-','').ToLowerInvariant() }
        finally { $sha.Dispose() }
        $inventory = [Text.Encoding]::UTF8.GetString($raw).TrimStart([char]0xfeff) | ConvertFrom-Json -ErrorAction Stop
        if ($Incoming) {
            if ([string]$inventory.app_id -cne 'Label_Match') { return $empty }
            $packetHash = [string]$inventory.artifact.sha256
            if (-not $packetHash) { $packetHash = [string]$inventory.packet_sha256 }
            $rows = @(@{path='packet.zip';sha256=$packetHash})
        } elseif ([string]$inventory.schema_version -ceq 'label-match-bootstrap-integrity-v1' -and
            [string]$inventory.status -ceq 'PASS' -and $inventory.files -is [Array] -and $inventory.files.Count) {
            $rows = @($inventory.files)
        } else { return $empty }
        $hashes = @{}; $directories = @{}
        foreach ($row in $rows) {
            $relative = [string]$row.path; $hash = [string]$row.sha256
            if ($relative -cnotmatch '^[^:/\\]+(?:/[^:/\\]+)*$' -or
                @($relative.Split('/') | Where-Object { $_ -in @('.','..') -or $_.EndsWith('.') -or $_.EndsWith(' ') }).Count -or
                $hash -cnotmatch '^[0-9a-f]{64}$') { return $empty }
            $filePath = Join-Path $Path $relative
            if ($hashes.ContainsKey($filePath)) { return $empty }
            $hashes[$filePath] = $hash
            $parent = [IO.Path]::GetDirectoryName($filePath)
            while (-not (Test-SamePath $parent $Path)) {
                $directories[$parent] = $true
                $parent = [IO.Path]::GetDirectoryName($parent)
            }
        }
    } catch { return $empty }
    $owned = @($hashes.Keys | Sort-Object | Where-Object { Test-LabelRemovalFile $_ $hashes[$_] })
    # Explicit metadata exception: the readable list itself is retired last,
    # only while its bytes still match the exact snapshot used for this plan.
    # Unlisted signatures have no such authority and stay in place.
    $ordered = @($owned | Sort-Object { if ((Split-Path -Leaf $_) -cin @('portable-manifest.json','bootstrap-integrity.json','manifest.json')) { 1 } else { 0 } })
    $hashes[$inventoryPath] = $inventoryHash
    $ordered = @($ordered | Where-Object { -not (Test-SamePath $_ $inventoryPath) }) + @($inventoryPath)
    return @{artifacts=$ordered;hashes=$hashes;metadata_roots=@{$inventoryPath=$Path};directories=(@($directories.Keys) + @($Path));
        retained_paths=@($Path);decisions=@(@{path=$Path;disposition='HASH_MATCHED_FILES_AND_LAST_METADATA'})}
}

function Get-LabelRemovalArtifacts([string]$ApplicationRoot, [string[]]$RetainedArtifactRoots = @()) {
    $artifacts = @(); $hashes = @{}; $metadataRoots = @{}; $directories = @(); $retained = @(); $decisions = @()
    foreach ($name in @('current', 'versions', 'update-control')) {
        $parent = Join-Path $ApplicationRoot $name
        if (-not (Test-Path -LiteralPath $parent -ErrorAction Stop)) { continue }
        try {
            Assert-LabelRemovalBoundary $parent
            $container = Get-Item -LiteralPath $parent -Force -ErrorAction Stop
        } catch { $retained += $parent; continue }
        if (-not $container.PSIsContainer) { $retained += $parent; continue }
        if ($name -ceq 'current') {
            if ($parent -in $RetainedArtifactRoots) { $retained += $parent; continue }
            $plan = Get-LabelArtifactFilePlan $parent
            $artifacts += $plan.artifacts; $retained += $plan.retained_paths; $directories += $plan.directories
            $decisions += $plan.decisions
            foreach ($path in $plan.hashes.Keys) { $hashes[$path] = $plan.hashes[$path] }
            foreach ($path in $plan.metadata_roots.Keys) { $metadataRoots[$path] = $plan.metadata_roots[$path] }
            continue
        }
        foreach ($child in @(Get-ChildItem -LiteralPath $parent -Force -ErrorAction Stop)) {
            if ($child.Attributes -band [IO.FileAttributes]::ReparsePoint) { $retained += $child.FullName; continue }
            if ($name -ceq 'versions') {
                if ($child.Name -cmatch '^[0-9a-f]{40}$' -and $child.PSIsContainer) {
                    if ($child.FullName -in $RetainedArtifactRoots) { $retained += $child.FullName; continue }
                    $plan = Get-LabelArtifactFilePlan $child.FullName
                    $artifacts += $plan.artifacts; $retained += $plan.retained_paths; $directories += $plan.directories
                    $decisions += $plan.decisions
                    foreach ($path in $plan.hashes.Keys) { $hashes[$path] = $plan.hashes[$path] }
                    foreach ($path in $plan.metadata_roots.Keys) { $metadataRoots[$path] = $plan.metadata_roots[$path] }
                } else { $retained += $child.FullName }
            } elseif (-not $child.PSIsContainer) {
                if ($child.Name -cin @('policy.json','active.json','transition.json') -or
                    $child.Name -cmatch '^(?:policy-before|failure)-[0-9a-f]{32}\.json$' -or
                    $child.Name -cmatch '^failed-[0-9a-f]{32}-(?:policy|active)\.json$') {
                    $artifacts += $child.FullName
                } else { $retained += $child.FullName }
            } elseif ($child.Name -cmatch '^packet-[0-9a-f]{32}$') {
                if ($child.FullName -in $RetainedArtifactRoots) { $retained += $child.FullName; continue }
                foreach ($item in @(@{path=(Join-Path $child.FullName 'extracted\Label_Match');incoming=$false},
                                    @{path=$child.FullName;incoming=$true})) {
                    $plan = Get-LabelArtifactFilePlan $item.path -Incoming:([bool]$item.incoming)
                    $artifacts += $plan.artifacts; $retained += $plan.retained_paths; $directories += $plan.directories
                    $decisions += $plan.decisions
                    foreach ($path in $plan.hashes.Keys) { $hashes[$path] = $plan.hashes[$path] }
                    foreach ($path in $plan.metadata_roots.Keys) { $metadataRoots[$path] = $plan.metadata_roots[$path] }
                }
            } elseif ($child.Name -cin @('incoming','failures')) {
                foreach ($entry in @(Get-ChildItem -LiteralPath $child.FullName -Force -ErrorAction Stop)) {
                    if ($entry.Attributes -band [IO.FileAttributes]::ReparsePoint) { $retained += $entry.FullName; continue }
                    if ($child.Name -ceq 'incoming' -and $entry.PSIsContainer -and $entry.Name -cmatch '^[0-9a-f]{32}$') {
                        if ($entry.FullName -in $RetainedArtifactRoots) { $retained += $entry.FullName; continue }
                        $plan = Get-LabelArtifactFilePlan $entry.FullName -Incoming
                        $artifacts += $plan.artifacts; $retained += $plan.retained_paths; $directories += $plan.directories
                        $decisions += $plan.decisions
                        foreach ($path in $plan.hashes.Keys) { $hashes[$path] = $plan.hashes[$path] }
                        foreach ($path in $plan.metadata_roots.Keys) { $metadataRoots[$path] = $plan.metadata_roots[$path] }
                    } elseif ($child.Name -ceq 'failures' -and -not $entry.PSIsContainer -and $entry.Name -cmatch '^[0-9a-f]{32}\.json$') {
                        $artifacts += $entry.FullName
                    } else { $retained += $entry.FullName }
                }
            } else { $retained += $child.FullName }
        }
    }
    return @{artifacts=@($artifacts);hashes=$hashes;metadata_roots=$metadataRoots;directories=$directories;retained_paths=@($retained);decisions=@($decisions)}
}

function Remove-LabelEmptiedParents([string]$Path, [string[]]$Directories) {
    $parent = [IO.Path]::GetDirectoryName($Path)
    while ($parent -in $Directories) {
        try {
            Assert-LabelRemovalBoundary $parent
            $io = Get-LabelRemovalIoPath $parent
            if (-not (Test-Path -LiteralPath $io -ErrorAction Stop)) {
                $parent = [IO.Path]::GetDirectoryName($parent); continue
            }
            $folder = Get-Item -LiteralPath $io -Force -ErrorAction Stop
            if (-not $folder.PSIsContainer -or @(Get-ChildItem -LiteralPath $io -Force -ErrorAction Stop).Count) { return }
            # Only a proven file move authorizes this now-empty parent.
            # Never change attributes of a preexisting or nonempty directory.
            if ($folder.Attributes -band [IO.FileAttributes]::ReadOnly) {
                $folder.Attributes = $folder.Attributes -band (-bnot [IO.FileAttributes]::ReadOnly)
            }
            Remove-Item -LiteralPath $io -ErrorAction Stop
        } catch {
            # Empty-directory cleanup is best effort; report a surviving path.
            return $parent
        }
        $parent = [IO.Path]::GetDirectoryName($parent)
    }
}

function Invoke-LabelConvergentRemoval([string]$InstallRoot, $TaskFolder, [int[]]$ExcludeProcessIds = @()) {
    $current = Get-StrictFullPath $InstallRoot 'removal current'
    if ((Split-Path -Leaf $current) -cne 'current') { throw 'REMOVAL_CURRENT_PATH_INVALID' }
    $application = Get-StrictFullPath (Split-Path -Parent $current) 'removal application'
    Assert-LabelRemovalBoundary $application
    Assert-LabelRemovalQuiescent $application $ExcludeProcessIds
    $markerPath = Join-Path $application 'removed.json'
    if (Test-Path -LiteralPath $markerPath -ErrorAction Stop) {
        Assert-LabelRemovalBoundary $markerPath
        $markerFile = Get-Item -LiteralPath $markerPath -Force -ErrorAction Stop
        if ($markerFile.PSIsContainer) {
            throw "제거 표식을 만드는 경로와 자료가 겹칩니다. 자료를 보존하고 경로를 확인하세요: $markerPath"
        }
    }
    $evidenceParent = Join-Path $application 'removal-evidence'
    Assert-LabelRemovalBoundary $evidenceParent
    New-Item -ItemType Directory -Path $evidenceParent -Force | Out-Null
    $evidence = Join-Path $evidenceParent ([Guid]::NewGuid().ToString('N'))
    New-Item -ItemType Directory -Path $evidence | Out-Null
    if ($null -eq $TaskFolder -and -not $testOverride) { throw 'REMOVAL_TASK_OBSERVATION_UNKNOWN' }
    if ($null -ne $TaskFolder) { Remove-LabelScheduledTasks $TaskFolder $evidence $current }
    Assert-LabelRemovalQuiescent $application $ExcludeProcessIds
    $plan = Get-LabelRemovalArtifacts $application
    if (Test-Path -LiteralPath $markerPath) { $plan.artifacts += $markerPath }
    $changedMetadataRoots = @(); $cleanupRetained = @()
    foreach ($path in $plan.artifacts) {
        $prefix = $application.TrimEnd('\') + '\'
        if (-not $path.StartsWith($prefix, 'OrdinalIgnoreCase')) { throw 'REMOVAL_PATH_ESCAPED' }
        if ($plan.hashes.ContainsKey($path) -and -not (Test-LabelRemovalFile $path $plan.hashes[$path])) {
            if ($plan.metadata_roots.ContainsKey($path)) {
                $changedMetadataRoots += $plan.metadata_roots[$path]
                $plan.decisions += @{path=$plan.metadata_roots[$path];disposition='METADATA_CHANGED_RETAINED'}
            }
            continue
        }
        Assert-LabelRemovalBoundary $path
        if ((Get-Item -LiteralPath (Get-LabelRemovalIoPath $path) -Force -ErrorAction Stop).PSIsContainer) { throw 'REMOVAL_DIRECTORY_MOVE_FORBIDDEN' }
        $destination = Join-Path $evidence $path.Substring($prefix.Length)
        # Evidence adds a UUID segment; normal source paths can cross MAX_PATH
        # at the destination. The literal extended path stays inside evidence.
        $destinationIo = Get-LabelRemovalIoPath $destination
        New-Item -ItemType Directory -Path (Split-Path -Parent $destinationIo) -Force | Out-Null
        try {
            Move-Item -LiteralPath (Get-LabelRemovalIoPath $path) -Destination $destinationIo -ErrorAction Stop
            if (Test-Path -LiteralPath (Get-LabelRemovalIoPath $path) -ErrorAction Stop) { throw 'REMOVAL_PATH_ABSENCE_NOT_PROVEN' }
        } finally {
            # A rename may succeed before the caller receives its exception.
            # Clean only parents of that proven move, including on retry cuts.
            if ($plan.hashes.ContainsKey($path) -and
                -not (Test-Path -LiteralPath (Get-LabelRemovalIoPath $path) -ErrorAction Stop) -and
                (Test-LabelRemovalFile $destination $plan.hashes[$path])) {
                $cleanupRetained += @(Remove-LabelEmptiedParents $path $plan.directories)
            }
        }
    }
    $remaining = Get-LabelRemovalArtifacts $application -RetainedArtifactRoots $changedMetadataRoots
    if ($remaining.artifacts.Count) { throw 'REMOVAL_MACHINE_ARTIFACT_REMAINS' }
    foreach ($path in @($cleanupRetained | Sort-Object -Unique)) {
        if (Test-Path -LiteralPath (Get-LabelRemovalIoPath $path) -ErrorAction Stop) {
            $remaining.retained_paths += $path
            $plan.decisions += @{path=$path;disposition='EMPTY_OWNED_PARENT_RETAINED'}
        }
    }
    Write-LabelStartupUpdateRecord -Path $markerPath -Payload @{
        schema='label-match-supported-removal-v1'; lifecycle_state='REMOVED';
        evidence_root=$evidence; business_data_and_registration_preserved=$true
        retained_paths=@($remaining.retained_paths); artifact_dispositions=@($plan.decisions)
    }
    $marker = Get-Content -LiteralPath $markerPath -Raw -Encoding UTF8 | ConvertFrom-Json
    if ([string]$marker.lifecycle_state -cne 'REMOVED') { throw 'REMOVAL_MARKER_READBACK_FAILED' }
    return @{status='PASS';lifecycle_state='REMOVED';evidence_root=$evidence;retained_paths=@($remaining.retained_paths)}
}


$OperatorSid = [Security.Principal.WindowsIdentity]::GetCurrent().User.Value
$testOverride = $earlyTestOverride
if ([string]::IsNullOrWhiteSpace($OperatorLocalAppDataRoot)) {
    if ([string]::IsNullOrWhiteSpace($env:LOCALAPPDATA)) { throw "The invoking operator LOCALAPPDATA is unavailable." }
    $OperatorLocalAppDataRoot = [IO.Path]::GetFullPath($env:LOCALAPPDATA)
}
if ($ApplyHardenedAclForTest.IsPresent -and -not $testOverride) {
    throw "ApplyHardenedAclForTest requires the guarded noncanonical test layout."
}
if ($ReplaceExistingVerifiedPortable.IsPresent -and $Uninstall.IsPresent) {
    throw "ReplaceExistingVerifiedPortable cannot be combined with Uninstall."
}
$applyHardenedAcl = (-not $testOverride -or $ApplyHardenedAclForTest.IsPresent)
$aclReadbackStatus = if ($applyHardenedAcl) { 'UNKNOWN' } else { 'NOT_TESTED' }
$installRootFull = Get-StrictFullPath $InstallRoot "InstallRoot"
if (-not (Test-SamePath $installRootFull $ExpectedInstallRoot) -and -not $testOverride) {
    throw "InstallRoot must be the hardened Label_Match code root."
}
if (
    $Uninstall.IsPresent -and
    -not $DryRun.IsPresent -and
    -not $testOverride -and
    (Test-CurrentUserRelayPersistencePresent)
) {
    throw (
        "Run Label_Match.exe --remove-current-user-setup as the current user " +
        "before removing hardened code."
    )
}
if (
    -not $DryRun.IsPresent -and
    -not $Uninstall.IsPresent -and
    -not $testOverride -and
    (
        $ExpectedBootstrapScriptSha256 -cnotmatch '^[0-9a-f]{64}$' -or
        $ExpectedSourceAggregateSha256 -cnotmatch '^[0-9a-f]{64}$' -or
        $ExpectedSourceFileCount -lt 1 -or
        $ExpectedSourceByteCount -lt 1
    )
) { throw "Trusted portable source inventory pins are required." }
if (-not $DryRun.IsPresent -and -not $testOverride) {
    Assert-AlreadyElevated
    if (-not $Uninstall.IsPresent -and -not $StartupUpdateRestorePreimage -and -not $LegacyRelayTransition) {
        try {
            Initialize-LabelProtectedInstallBase $installRootFull
            Write-LabelInstallerOwnerRecord $installRootFull
            Write-ElevationLog 'PREPARATION' ($Script:LabelAncestorPreparationEvents.ToArray() | ConvertTo-Json -Depth 8 -Compress)
        }
        catch {
            # Keep the admission exception/code even if existing audit IO fails.
            $preparationFailure = $_
            $preparationFailure.Exception.Data['installation_preparation_exit_code'] = Get-InstallationPreparationExitCode $preparationFailure
            try {
                Write-ElevationLog 'PREPARATION_FAILED' ([ordered]@{
                    message = $preparationFailure.Exception.Message
                    events = $Script:LabelAncestorPreparationEvents.ToArray()
                } | ConvertTo-Json -Depth 8 -Compress)
            } catch { }
            throw $preparationFailure
        }
    }
    Write-ElevationLog 'STARTED' 'Elevated Label code placement started.'
}

if ($LegacyRelayTransition -or $StartupUpdateRestorePreimage) {
    if ($LegacyRelayTransition) {
        if ($DryRun -or $Uninstall -or $StartupUpdateRestorePreimage -or $WriterFenceAttemptId -cnotmatch '^[0-9a-f]{32}$') { throw 'LEGACY_RELAY_TRANSITION_INVALID' }
        Assert-ProtectedCodeAncestors (Split-Path -Parent $installRootFull)
        $directory=Join-Path (Split-Path -Parent $installRootFull) ('.legacy-relay.'+$WriterFenceAttemptId)
    } else {
        if ($testOverride -or $DryRun -or $Uninstall) { throw 'Startup update restore mode differs.' }
        $OperatorSid = [Security.Principal.WindowsIdentity]::GetCurrent().User.Value
    }
    # Both preimages use the same existing admission source; inventory stays 59.
    $lease = Enter-LabelWriterDelegatedOperation -ControlRoot $WriterFenceControlRoot -SessionId $WriterFenceSessionId -AttemptId $WriterFenceAttemptId -ReplacementTransactionId $WriterFenceReplacementTransactionId -DelegationToken $WriterFenceDelegationToken -Source 'auto_update_restore_preimage'
    try {
        if ($LegacyRelayTransition) {
            Invoke-LabelOfficialLegacyTaskTransition $LegacyRelayTransition $directory $LegacyRelayPreimageSha256
            Write-ElevationLog 'LEGACY_RELAY_TRANSITION' ('mode='+$LegacyRelayTransition+';preimage_sha256='+$LegacyRelayPreimageSha256)
        } else {
            Restore-LabelStartupUpdatePreimage $installRootFull $StartupUpdateRestorePreimage $StartupUpdateRestoreSha256
        }
    } finally { Exit-LabelWriterAdmission $lease }
    return
}
if ($Uninstall.IsPresent) {
    if ($DryRun.IsPresent) { 'uninstall_status=DRY_RUN_CODE_ONLY'; 'user_state_preserved=true'; exit 0 }
    $lease = $null
    # Standalone supported removal is elevated and does not load installed helpers.
    # A canonical caller retains its exact, preloaded deployment delegation.
    if (-not $testOverride -and $WriterFenceFunctionsPreloaded.IsPresent) {
        $lease = Enter-LabelCanonicalPlacement
    }
    try {
        $application = Split-Path -Parent $installRootFull
        if (-not (Test-Path -LiteralPath $application)) { New-Item -ItemType Directory -Path $application | Out-Null }
        if (-not $testOverride) { Assert-ProtectedCodeAncestors $application }
        $folder = $null
        if (-not $testOverride) { $scheduler=New-Object -ComObject 'Schedule.Service'; $scheduler.Connect(); $folder=$scheduler.GetFolder('\') }
        $removed = Invoke-LabelConvergentRemoval -InstallRoot $installRootFull -TaskFolder $folder
        'uninstall_status=PASS_CODE_REMOVED_STATE_PRESERVED'
        'application_root_status=RETAINED_FOR_EVIDENCE_AND_RETAINED_ITEMS'
        if ($testOverride) { 'system_task_status=NOT_TESTED' } else { 'system_task_status=ABSENT' }
        'user_state_preserved=true'
        "removal_evidence_root=$($removed.evidence_root)"
        foreach ($retained in $removed.retained_paths) { "removal_retained_path=$retained" }
    } finally { if ($null -ne $lease) { Exit-LabelWriterAdmission $lease } }
    exit 0
}

if ([string]::IsNullOrWhiteSpace($SourceRoot)) {
    $SourceRoot = Split-Path -Parent $MyInvocation.MyCommand.Path
}
$sourceRootFull = Get-StrictFullPath $SourceRoot "SourceRoot"
if (-not (Test-Path -LiteralPath $sourceRootFull -PathType Container)) {
    throw "SourceRoot does not exist."
}
if (Test-SamePath $sourceRootFull $installRootFull) {
    throw "SourceRoot and InstallRoot must differ."
}
Assert-NoReparsePoint $sourceRootFull "Frozen release"
$releaseLayout = Assert-RequiredRelease $sourceRootFull $testOverride
$sourceInventory = @(Get-CodeInventory $sourceRootFull)
if ($sourceInventory.Count -eq 0) {
    throw "Frozen release code inventory is empty."
}
$sourceAggregate = Get-InventoryAggregate $sourceInventory
$sourceByteCount = [uint64](
    ($sourceInventory | Measure-Object -Property size -Sum).Sum
)
if (
    -not [string]::IsNullOrWhiteSpace($ExpectedSourceAggregateSha256) -and
    (
        $sourceAggregate -cne $ExpectedSourceAggregateSha256 -or
        $sourceInventory.Count -ne $ExpectedSourceFileCount -or
        $sourceByteCount -ne $ExpectedSourceByteCount
    )
) { throw "Portable source inventory differs from its trusted caller pins." }
if ($DryRun.IsPresent) {
    Write-Output "bootstrap_status=DRY_RUN"
    Write-Output "code_root=$installRootFull"
    Write-Output "release_layout=$releaseLayout"
    Write-Output "file_count=$($sourceInventory.Count)"
    Write-Output "aggregate_sha256=$sourceAggregate"
    Write-Output "identity_profile_created=false"
    Write-Output "tls_ca_bootstrap_configured=$(-not [string]::IsNullOrWhiteSpace($TlsCaBundlePath))"
    Write-Output "elevation_points=1:code_placement"
    exit 0
}

$writerFenceLease = $null
if (-not $testOverride) {
    if (-not $WriterFenceFunctionsPreloaded.IsPresent) {
        throw "Production placement requires the preloaded writer fence helper."
    }
    $writerFenceLease = Enter-LabelCanonicalPlacement -TimeoutMilliseconds 15000
}

$applicationParent = Split-Path -Parent $installRootFull
$supportedContext = $null
if (-not $testOverride -and ((Test-Path -LiteralPath (Join-Path $applicationParent 'update-control')) -or
    (Test-Path -LiteralPath (Join-Path $applicationParent 'removed.json')))) {
    $healthOutput = @(& (Join-Path $sourceRootFull 'runtime\python.exe') -I -B `
        (Join-Path $sourceRootFull 'app\main.py') --label-match-lifecycle-context `
        --install-root $applicationParent --operation install)
    if ($LASTEXITCODE -ne 0) { throw ('자료와 등록 및 전송 대기를 보존하고 지원 제거를 먼저 실행한 뒤 다시 설치하세요. ' + ($healthOutput -join "`n")) }
    $supportedContext = ($healthOutput -join "`n") | ConvertFrom-Json
    if ([string]$supportedContext.status -cne 'PASS') { throw 'SUPPORTED_REMOVAL_REQUIRED' }
}
$versionedPlacement = ($null -ne $supportedContext -and [string]$supportedContext.lifecycle_state -ceq 'INSTALLED')
if ($versionedPlacement -and ([string]$ExpectedActiveCommit -cnotmatch '^[0-9a-f]{40}$' -or
    [string]$supportedContext.from_commit -cne $ExpectedActiveCommit)) { throw 'SUPPORTED_FROM_COMMIT_DIFFERS' }
$placementRoot = $installRootFull
if ($versionedPlacement) {
    $nextCommit = [string](Get-Content -LiteralPath (Join-Path $sourceRootFull 'portable-manifest.json') -Raw -Encoding UTF8 | ConvertFrom-Json).source_commit
    if ($nextCommit -cnotmatch '^[0-9a-f]{40}$') { throw 'SUPPORTED_TARGET_COMMIT_INVALID' }
    $placementRoot = Join-Path $applicationParent ('versions\' + $nextCommit)
    Assert-NoReparsePoint $placementRoot 'supported version destination'
    if (Test-Path -LiteralPath $placementRoot) {
        [void](Assert-BootstrapIntegrityRecord $placementRoot)
        if ((Get-InventoryAggregate @(Get-CodeInventory $placementRoot)) -cne $sourceAggregate) { throw 'SUPPORTED_TARGET_ALREADY_DIFFERS' }
    }
}
$stagingRoot = Join-Path $applicationParent ('.current.bootstrap.' + [Guid]::NewGuid().ToString('N'))
$replacementRollbackRoot = ''
$replacementApplied = $false
$Script:StartupUpdatePreimage = $null
$OperatorSid = [Security.Principal.WindowsIdentity]::GetCurrent().User.Value
New-Item -ItemType Directory -Path $applicationParent -Force | Out-Null
if ($applyHardenedAcl) {
    Set-HardenedCodeAcl $applicationParent
}
New-Item -ItemType Directory -Path $stagingRoot -Force | Out-Null
try {
    Assert-PlacementDataDowngrade $sourceRootFull
    $robocopy = Join-Path ([Environment]::SystemDirectory) 'robocopy.exe'
    & $robocopy $sourceRootFull $stagingRoot /E /XJ /MT:16 /R:0 /W:0 /NFL /NDL /NJH /NJS /NP /XF (Join-Path $sourceRootFull $IntegrityFileName) | Out-Null
    if ($LASTEXITCODE -lt 0 -or $LASTEXITCODE -ge 8) { throw "Portable staging copy failed: $LASTEXITCODE" }
    $stagedInventory = @(Get-CodeInventory $stagingRoot)
    $stagedAggregate = Get-InventoryAggregate $stagedInventory
    if ($stagedAggregate -cne $sourceAggregate) {
        throw "Staged code integrity readback differs from the frozen release."
    }
    $record = Write-BootstrapIntegrityRecord `
        -Root $stagingRoot `
        -CodeRoot $placementRoot `
        -Inventory $stagedInventory `
        -IntegrityFileName $IntegrityFileName `
        -IntegritySchema $IntegritySchema
    if ([string]$record.aggregate_sha256 -cne $stagedAggregate) {
        throw "Bootstrap integrity producer aggregate differs from staged inventory."
    }
    if ($applyHardenedAcl) {
        Set-HardenedCodeAcl $stagingRoot -Recursive
    }
    if (Test-Path -LiteralPath $placementRoot) {
        $existingRecordPath = Join-Path $placementRoot $IntegrityFileName
        $existingAggregate = ''
        $existingCodeAggregate = ''
        if (Test-Path -LiteralPath $existingRecordPath -PathType Leaf) {
            try {
                $existingAggregate = [string]((Get-Content -LiteralPath $existingRecordPath -Raw -Encoding UTF8 | ConvertFrom-Json).aggregate_sha256)
                $existingCodeAggregate = Get-InventoryAggregate @(Get-CodeInventory $placementRoot)
            }
            catch {
                $existingAggregate = ''
                $existingCodeAggregate = ''
            }
        }
        if (
            $existingAggregate -cne $sourceAggregate -or
            $existingCodeAggregate -cne $sourceAggregate
        ) {
            if ($versionedPlacement) { throw "SUPPORTED_TARGET_ALREADY_DIFFERS" }
            if (-not $ReplaceExistingVerifiedPortable.IsPresent) {
                throw "A different or damaged hardened code placement exists; remove it explicitly before replacement."
            }
            [void](Assert-BootstrapIntegrityRecord -Root $placementRoot)
            $existingManifestPath = Join-Path $placementRoot 'portable-manifest.json'
            if (-not (Test-Path -LiteralPath $existingManifestPath -PathType Leaf)) {
                throw "Verified replacement requires an existing portable manifest."
            }
            $existingManifest = Get-Content `
                -LiteralPath $existingManifestPath `
                -Raw `
                -Encoding UTF8 | ConvertFrom-Json
            if (
                [string]$existingManifest.schema -cne 'label-match-portable-tree-v1' -or
                [string]$existingManifest.source_commit -notmatch '^[0-9a-f]{40}$'
            ) {
                throw "Verified replacement existing portable identity is invalid."
            }
            $replacementRollbackRoot = Join-Path $applicationParent (
                '.current.rollback.' + [Guid]::NewGuid().ToString('N')
            )
            Move-Item -LiteralPath $placementRoot -Destination $replacementRollbackRoot
            try {
                Move-Item -LiteralPath $stagingRoot -Destination $placementRoot
                $replacementApplied = $true
                $bootstrapStatus = 'REPLACED_VERIFIED'
            }
            catch {
                $replacementTargetAbsent = -not (Test-Path -LiteralPath $placementRoot)
                $replacementRollbackPresent = Test-Path `
                    -LiteralPath $replacementRollbackRoot `
                    -PathType Container
                if ($replacementTargetAbsent -and $replacementRollbackPresent) {
                    Assert-PlacementDataDowngrade $replacementRollbackRoot
                    Move-Item -LiteralPath $replacementRollbackRoot -Destination $placementRoot
                }
                throw
            }
        }
        else {
            Remove-Item -LiteralPath $stagingRoot -Recurse -Force
            $bootstrapStatus = 'REUSED'
        }
    }
    else {
        Move-Item -LiteralPath $stagingRoot -Destination $placementRoot
        $bootstrapStatus = 'PASS'
    }
    if ($applyHardenedAcl) {
        Set-HardenedCodeAcl $placementRoot -Recursive
        $aclReadbackStatus = 'PASS'
    }
    $installedAggregate = Get-InventoryAggregate @(Get-CodeInventory $placementRoot)
    if ($installedAggregate -cne $sourceAggregate) {
        throw "Installed code integrity readback failed."
    }
    Write-Output "bootstrap_status=$bootstrapStatus"
    Write-Output "acl_readback_status=$aclReadbackStatus"
    if ($aclReadbackStatus -ceq 'PASS') {
        Write-Output "acl_owner_sid=S-1-5-32-544"
        Write-Output "dacl_normalized=true"
    }
    Write-Output "code_root=$placementRoot"
    Write-Output "release_layout=$releaseLayout"
    Write-Output "integrity_record=$(Join-Path $placementRoot $IntegrityFileName)"
    if ($versionedPlacement) {
        $activation = @(& (Join-Path $sourceRootFull 'runtime\python.exe') -I -B `
            (Join-Path $sourceRootFull 'app\main.py') --label-match-lifecycle-context `
            --install-root $applicationParent --expected-from-commit $ExpectedActiveCommit `
            --activate-supported-commit $nextCommit)
        if ($LASTEXITCODE -ne 0) { throw ('SUPPORTED_ACTIVATION_FAILED:' + ($activation -join "`n")) }
    } else { Initialize-LabelStartupUpdate $placementRoot }
    $tlsCaBootstrap = Install-CurrentUserTlsCaBootstrap $TlsCaBundlePath $OperatorLocalAppDataRoot
    if ($null -eq $tlsCaBootstrap) { Write-Output "tls_ca_bootstrap_status=ABSENT" }
    else { Write-Output "tls_ca_bootstrap_status=PASS"; Write-Output "tls_ca_bootstrap_path=$tlsCaBootstrap" }
    Write-Output "file_count=$($sourceInventory.Count)"
    Write-Output "aggregate_sha256=$sourceAggregate"
    Write-Output "identity_profile_created=false"
    Write-Output "elevation_points=1:code_placement"
    if ($replacementApplied) {
        Write-Output "replacement_rollback_status=PRESERVED"
        Write-Output "replacement_rollback_root=$replacementRollbackRoot"
    }
    if (-not $testOverride) {
        Write-ElevationLog 'PASS' "Elevated Label code placement completed: $bootstrapStatus."
    }
}
catch {
    if ($null -ne $Script:StartupUpdatePreimage) {
        Restore-LabelStartupUpdatePreimage $installRootFull $Script:StartupUpdatePreimage.path $Script:StartupUpdatePreimage.sha256
    }
    if (-not $testOverride) {
        Write-ElevationLog 'FAILED' ($_.Exception.GetType().Name)
    }
    if (Test-Path -LiteralPath $stagingRoot) {
        $stagingFull = Get-StrictFullPath $stagingRoot "bootstrap staging root"
        $parentFull = (Get-StrictFullPath $applicationParent "application parent") + '\'
        if (-not $stagingFull.StartsWith($parentFull, [StringComparison]::OrdinalIgnoreCase)) {
            throw "Bootstrap failed and staging cleanup target escaped its parent."
        }
        Remove-Item -LiteralPath $stagingFull -Recurse -Force -ErrorAction SilentlyContinue
    }
    if ($replacementApplied) {
        Assert-PlacementDataDowngrade $replacementRollbackRoot
        $failedRoot = Join-Path $applicationParent (
            '.current.failed.' + [Guid]::NewGuid().ToString('N')
        )
        if (Test-Path -LiteralPath $installRootFull -PathType Container) {
            Move-Item -LiteralPath $installRootFull -Destination $failedRoot
        }
        if (-not (Test-Path -LiteralPath $replacementRollbackRoot -PathType Container)) {
            throw "Verified replacement rollback source is unavailable."
        }
        Move-Item -LiteralPath $replacementRollbackRoot -Destination $installRootFull
        throw "Verified replacement failed and the prior canonical tree was restored."
    }
    throw
}
finally {
    if ($null -ne $writerFenceLease) {
        Exit-LabelWriterAdmission $writerFenceLease
    }
}
