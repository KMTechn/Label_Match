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
    [string]$WriterFenceSessionId = "",
    [string]$WriterFenceAttemptId = "",
    [string]$WriterFenceReplacementTransactionId = "",
    [string]$WriterFenceDelegationToken = "",
    [string]$StartupUpdateRestorePreimage = "",
    [string]$StartupUpdateRestoreSha256 = "",
    [switch]$ReplaceExistingVerifiedPortable,
    [switch]$AllowNoncanonicalLayoutForTest,
    [switch]$ApplyHardenedAclForTest
)

$ErrorActionPreference = "Stop"
$ExpectedInstallRoot = "C:\KMTech\Apps\Label_Match\current"
$IntegrityFileName = "bootstrap-integrity.json"
$IntegritySchema = "label-match-bootstrap-integrity-v1"

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
        'Assert-BootstrapIntegrityRecord'
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
        (($entry | ConvertTo-Json -Compress) + [Environment]::NewLine),
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
    $filesBeforeManifest = @(
        Get-ChildItem -LiteralPath $Root -File -Force -Recurse |
            Where-Object { -not (Test-SamePath $_.FullName $manifestPath) }
    )
    $bytesBeforeManifest = [int64](
        ($filesBeforeManifest | Measure-Object -Property Length -Sum).Sum
    )
    if (
        [int64]$manifest.file_count_before_manifest -ne $filesBeforeManifest.Count -or
        [int64]$manifest.byte_count_before_manifest -ne $bytesBeforeManifest
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
        if ([string]$owner.Value -cne 'S-1-5-32-544') {
            throw "Hardened code ACL owner is not BUILTIN\Administrators: $($target.FullName)"
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
    $full = [IO.Path]::GetFullPath($Path)
    $leaf = $true
    while ($full) {
        if (Test-Path -LiteralPath $full) {
            $entry = Get-Item -LiteralPath $full -ErrorAction Stop
            if ($entry.Attributes -band [IO.FileAttributes]::ReparsePoint) { throw 'Unsafe update path component.' }
            $acl = Get-Acl -LiteralPath $full -ErrorAction Stop
            if ($acl.GetOwner([Security.Principal.SecurityIdentifier]).Value -notin @('S-1-5-18','S-1-5-32-544')) {
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
function Assert-LabelStartupUpdateTaskDefinition([xml]$Actual, [xml]$Expected) {
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
        $a=$Actual.SelectSingleNode($xpath,$namespace); $b=$Expected.SelectSingleNode($xpath,$namespace)
        if ([string]$a.InnerText -cne [string]$b.InnerText) { throw 'STARTUP_UPDATE_OWNED_TASK_DIFFERS' }
    }
    # Reject extra executable/trigger/principal properties, including repetition.
    foreach ($xpath in @('/t:Task/t:Actions','/t:Task/t:Triggers','/t:Task/t:Principals')) {
        $a=$Actual.SelectSingleNode($xpath,$namespace); $b=$Expected.SelectSingleNode($xpath,$namespace)
        if ([string]$a.InnerXml -cne [string]$b.InnerXml) { throw 'STARTUP_UPDATE_OWNED_TASK_DIFFERS' }
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
    Assert-LabelStartupUpdateTrust $Root
    $managed = Get-StrictFullPath (Split-Path -Parent $Root) 'startup update restore parent'
    $full = Get-StrictFullPath $Directory 'startup update preimage'
    if (-not (($full.StartsWith($managed+'\',[StringComparison]::OrdinalIgnoreCase))) -or (Split-Path -Leaf $full) -cnotmatch '^\.startup-update-(install|uninstall)\.[0-9a-f]{32}$') { throw 'STARTUP_UPDATE_RESTORE_PATH_INVALID' }
    Assert-NoReparsePoint $full 'startup update preimage'
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

    $control = Join-Path $managed 'update-control'
    foreach ($item in @($saved.files)) {
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
    }
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
}


function Assert-LabelStartupUpdateSettings([object]$Settings) {
    $names=@($Settings.PSObject.Properties.Name)
    $allowed=@('feed_url','channel','apply_during_work','profile_auth')
    if (@($names | Where-Object { $_ -cnotin $allowed }).Count -or @($allowed | Where-Object { $_ -cnotin $names }).Count) { throw 'STARTUP_UPDATE_SETTINGS_FIELDS_INVALID' }
    if ($Settings.feed_url -isnot [string] -or $Settings.channel -isnot [string] -or $Settings.channel -cnotmatch '^[a-z0-9_-]{1,32}$' -or $Settings.profile_auth -isnot [bool] -or $Settings.apply_during_work -isnot [bool] -or $Settings.apply_during_work) { throw 'STARTUP_UPDATE_SETTINGS_FORMAT_INVALID' }
    if ($Settings.feed_url) {
        $uri=$null
        if (-not [Uri]::TryCreate($Settings.feed_url,[UriKind]::Absolute,[ref]$uri) -or $uri.Scheme -cne 'https' -or -not $uri.Host -or $uri.UserInfo -or $uri.Fragment) { throw 'STARTUP_UPDATE_SETTINGS_HTTPS_REQUIRED' }
        if ([regex]::Match([string]$Settings.feed_url,'^https://([^/?#]+)').Groups[1].Value -match '[^\x00-\x7f]') { throw '업데이트 주소의 호스트는 ASCII IP 또는 도메인이어야 합니다.' }
        $hostName=$uri.DnsSafeHost.ToLowerInvariant().TrimEnd('.')
        if (@(@('github.com','githubusercontent.com','github.io') | Where-Object { $hostName -ceq $_ -or $hostName.EndsWith('.'+$_) }).Count) { throw 'STARTUP_UPDATE_SETTINGS_GITHUB_FORBIDDEN' }
        if (-not $Settings.profile_auth) { throw 'STARTUP_UPDATE_SETTINGS_AUTH_REQUIRED' }
    }
}
function Initialize-LabelStartupUpdate([string]$Root) {
    $settingsPath = Join-Path $Root 'app\auto-update-settings.json'
    if (-not (Test-Path -LiteralPath $settingsPath -PathType Leaf)) { return }
    if ($testOverride) { Write-Output 'startup_update_status=NOT_TESTED_HOST'; return }
    Assert-LabelStartupUpdateTrust $Root
    $settings = Get-Content -LiteralPath $settingsPath -Raw -Encoding UTF8 | ConvertFrom-Json
    Assert-LabelStartupUpdateSettings $settings
    $Script:StartupUpdatePreimage = Save-LabelStartupUpdatePreimage $Root 'install'
    $managed = Get-StrictFullPath (Split-Path -Parent $Root) 'startup update managed root'
    Assert-NoReparsePoint $managed 'startup update managed root'
    $control = Join-Path $managed 'update-control'
    $versions = Join-Path $managed 'versions'
    foreach ($directory in @($control,$versions)) {
        if (-not (Test-Path -LiteralPath $directory)) { New-Item -ItemType Directory -Path $directory | Out-Null }
        Assert-NoReparsePoint $directory 'startup update protected directory'
        Set-HardenedCodeAcl $directory -Recursive
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
        [string]$task.Principal.UserId -notin @('SYSTEM', 'NT AUTHORITY\SYSTEM', 'S-1-5-18') -or
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

function Test-CurrentUserRelayPersistencePresent {
    $runKey = 'Registry::HKEY_CURRENT_USER\Software\Microsoft\Windows\CurrentVersion\Run'
    try {
        $value = Get-ItemPropertyValue `
            -LiteralPath $runKey `
            -Name 'KMTech.LabelMatch.Relay' `
            -ErrorAction Stop
        return -not [string]::IsNullOrWhiteSpace([string]$value)
    }
    catch [System.Management.Automation.ItemNotFoundException] {
        return $false
    }
    catch [System.Management.Automation.PSArgumentException] {
        return $false
    }
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
    Write-ElevationLog 'STARTED' 'Elevated Label code placement started.'
}

if ($StartupUpdateRestorePreimage) {
    if ($testOverride -or $DryRun -or $Uninstall) { throw 'Startup update restore mode differs.' }
    $OperatorSid = [Security.Principal.WindowsIdentity]::GetCurrent().User.Value
    $lease = Enter-LabelWriterDelegatedOperation -ControlRoot $WriterFenceControlRoot -SessionId $WriterFenceSessionId -AttemptId $WriterFenceAttemptId -ReplacementTransactionId $WriterFenceReplacementTransactionId -DelegationToken $WriterFenceDelegationToken -Source 'auto_update_restore_preimage'
    try { Restore-LabelStartupUpdatePreimage $installRootFull $StartupUpdateRestorePreimage $StartupUpdateRestoreSha256 }
    finally { Exit-LabelWriterAdmission $lease }
    return
}
function Assert-LabelStartupUpdateLifecycleOwnership([string]$Root) {
    $control = Join-Path (Split-Path -Parent $Root) 'update-control'
    if (Test-Path -LiteralPath (Join-Path $control 'policy.json')) {
        Assert-LabelStartupUpdateTrust $Root
        $active = Get-Content -LiteralPath (Join-Path $control 'active.json') -Raw -Encoding UTF8 | ConvertFrom-Json
        $installed = Get-Content -LiteralPath (Join-Path $Root 'portable-manifest.json') -Raw -Encoding UTF8 | ConvertFrom-Json
        $transition = Get-Content -LiteralPath (Join-Path $control 'transition.json') -Raw -Encoding UTF8 | ConvertFrom-Json
        if ([string]$active.sha -cnotmatch '^[0-9a-f]{40}$' -or [string]$installed.source_commit -cnotmatch '^[0-9a-f]{40}$' -or [string]$active.sha -cne [string]$installed.source_commit -or [string]$transition.phase -cnotin @('CONFIRMED','ROLLED_BACK','DEFERRED')) {
            Write-Output 'startup_update_lifecycle_status=RECOVERY_REQUIRED'
            Write-Output 'user_state_preserved=true'
            $failure = New-Object InvalidOperationException('자동 업데이트 후 수동 교체·제거는 아직 지원되지 않습니다. 데이터와 설치 상태를 보존하고 담당자에게 복구를 요청하세요.')
            $failure.Data['code'] = 'ACTIVE_CODE_REQUIRES_COORDINATED_RECOVERY'
            throw $failure
        }
        [void](Get-LabelStartupUpdateTaskPreimage $Root)
    }
}
if (-not $DryRun -and -not $testOverride) {
    Assert-LabelStartupUpdateLifecycleOwnership $installRootFull
}
if ($Uninstall.IsPresent) {
    if ($DryRun.IsPresent) {
        Write-Output "uninstall_status=DRY_RUN_CODE_ONLY"
        Write-Output "user_state_preserved=true"
        exit 0
    }
    [void](Get-StrictFullPath $installRootFull "uninstall target")
    Assert-NoReparsePoint $installRootFull "Label_Match code root"
    if (-not $testOverride) {
        Remove-OwnedLegacyTask $LegacyRelayTaskName
        $settings = Join-Path $installRootFull 'app\auto-update-settings.json'
        if (Test-Path -LiteralPath $settings) {
            $OperatorSid = [Security.Principal.WindowsIdentity]::GetCurrent().User.Value
            $Script:StartupUpdatePreimage=Save-LabelStartupUpdatePreimage $installRootFull 'uninstall'
            $task=$Script:StartupUpdatePreimage
            $spec=Get-LabelStartupUpdateTaskSpec $installRootFull $OperatorSid
            Invoke-LabelStartupUpdateTaskMutation {
                $existing=Get-LabelStartupUpdateTask ([string]$spec.name)
                if ($null -ne $existing) { Unregister-ScheduledTask -TaskName $spec.name -Confirm:$false }
                if ($null -ne (Get-LabelStartupUpdateTask ([string]$spec.name))) { throw 'Startup update task removal unproven.' }
            }
        }
    }
    if (Test-Path -LiteralPath $installRootFull) {
        Remove-Item -LiteralPath $installRootFull -Recurse -ErrorAction Stop
    }
    if (Test-Path -LiteralPath $installRootFull) {
        throw "Hardened code root removal postcondition failed."
    }
    Write-Output "uninstall_status=PASS_CODE_REMOVED_STATE_PRESERVED"
    Write-Output "application_root_status=ABSENT"
    Write-Output "system_task_status=ABSENT"
    Write-Output "user_state_preserved=true"
    Write-Output "current_user_setup_removal_command=Label_Match.exe --remove-current-user-setup"
    if (-not $testOverride) {
        Write-ElevationLog 'PASS' 'Elevated Label code removal completed.'
    }
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
    $writerFenceLease = Enter-LabelWriterDelegatedOperation `
        -ControlRoot $WriterFenceControlRoot `
        -SessionId $WriterFenceSessionId `
        -AttemptId $WriterFenceAttemptId `
        -ReplacementTransactionId $WriterFenceReplacementTransactionId `
        -DelegationToken $WriterFenceDelegationToken `
        -Source 'canonical_placement' `
        -TimeoutMilliseconds 15000
}

$applicationParent = Split-Path -Parent $installRootFull
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
        -CodeRoot $installRootFull `
        -Inventory $stagedInventory `
        -IntegrityFileName $IntegrityFileName `
        -IntegritySchema $IntegritySchema
    if ([string]$record.aggregate_sha256 -cne $stagedAggregate) {
        throw "Bootstrap integrity producer aggregate differs from staged inventory."
    }
    if ($applyHardenedAcl) {
        Set-HardenedCodeAcl $stagingRoot -Recursive
    }
    if (Test-Path -LiteralPath $installRootFull) {
        $existingRecordPath = Join-Path $installRootFull $IntegrityFileName
        $existingAggregate = ''
        $existingCodeAggregate = ''
        if (Test-Path -LiteralPath $existingRecordPath -PathType Leaf) {
            try {
                $existingAggregate = [string]((Get-Content -LiteralPath $existingRecordPath -Raw -Encoding UTF8 | ConvertFrom-Json).aggregate_sha256)
                $existingCodeAggregate = Get-InventoryAggregate @(Get-CodeInventory $installRootFull)
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
            if (-not $ReplaceExistingVerifiedPortable.IsPresent) {
                throw "A different or damaged hardened code placement exists; remove it explicitly before replacement."
            }
            [void](Assert-BootstrapIntegrityRecord -Root $installRootFull)
            $existingManifestPath = Join-Path $installRootFull 'portable-manifest.json'
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
            Move-Item -LiteralPath $installRootFull -Destination $replacementRollbackRoot
            try {
                Move-Item -LiteralPath $stagingRoot -Destination $installRootFull
                $replacementApplied = $true
                $bootstrapStatus = 'REPLACED_VERIFIED'
            }
            catch {
                $replacementTargetAbsent = -not (Test-Path -LiteralPath $installRootFull)
                $replacementRollbackPresent = Test-Path `
                    -LiteralPath $replacementRollbackRoot `
                    -PathType Container
                if ($replacementTargetAbsent -and $replacementRollbackPresent) {
                    Move-Item -LiteralPath $replacementRollbackRoot -Destination $installRootFull
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
        Move-Item -LiteralPath $stagingRoot -Destination $installRootFull
        $bootstrapStatus = 'PASS'
    }
    if ($applyHardenedAcl) {
        Set-HardenedCodeAcl $installRootFull -Recursive
        $aclReadbackStatus = 'PASS'
    }
    $installedAggregate = Get-InventoryAggregate @(Get-CodeInventory $installRootFull)
    if ($installedAggregate -cne $sourceAggregate) {
        throw "Installed code integrity readback failed."
    }
    Write-Output "bootstrap_status=$bootstrapStatus"
    Write-Output "acl_readback_status=$aclReadbackStatus"
    if ($aclReadbackStatus -ceq 'PASS') {
        Write-Output "acl_owner_sid=S-1-5-32-544"
        Write-Output "dacl_normalized=true"
    }
    Write-Output "code_root=$installRootFull"
    Write-Output "release_layout=$releaseLayout"
    Write-Output "integrity_record=$(Join-Path $installRootFull $IntegrityFileName)"
    Initialize-LabelStartupUpdate $installRootFull
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
