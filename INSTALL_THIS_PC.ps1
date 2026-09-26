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
    [string]$FreshMachineAction = '',
    [string]$FreshMachineJournal = '',
    [string]$FreshMachineJournalSha256 = '',
    [string]$FreshOperatorSid = '',
    [string]$WriterFenceControlRoot = "",
    [string]$WriterFenceSessionId = "",
    [string]$WriterFenceAttemptId = "",
    [string]$WriterFenceReplacementTransactionId = "",
    [string]$WriterFenceDelegationToken = "",
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

function Invoke-FreshMachinePreservation {
    if ($FreshMachineAction -cnotin @('Preserve', 'Restore') -or $DryRun -or $Uninstall -or
        -not $WriterFenceFunctionsPreloaded -or -not $BootstrapIntegrityPreloaded) {
        throw 'Fresh machine preservation requires the pinned canonical helper.'
    }
    # Bind the caller's inputs here: an empty delegation would skip the guard's
    # source/authority checks, and any other root or journal is not the
    # original user's transition.
    if ($WriterFenceDelegationToken.Length -lt 32 -or $FreshOperatorSid -cnotmatch '^S-1-5-21-[0-9-]+$') {
        throw 'Fresh machine preservation requires the live installer delegation for the original user.'
    }
    $operatorLocal = if ($testOverride) { Get-StrictFullPath $OperatorLocalAppDataRoot 'operator LOCALAPPDATA' } else {
        $profileKey = 'Registry::HKEY_LOCAL_MACHINE\SOFTWARE\Microsoft\Windows NT\CurrentVersion\ProfileList\' + $FreshOperatorSid
        Join-Path ([Environment]::ExpandEnvironmentVariables([string](
            Get-ItemProperty -LiteralPath $profileKey -Name ProfileImagePath -ErrorAction Stop).ProfileImagePath)) 'AppData\Local'
    }
    if (-not (Test-SamePath $WriterFenceControlRoot (Join-Path $operatorLocal 'KMTech\DirectSync\label_match\control\writer-session'))) {
        throw 'Machine preservation fence is not the original user canonical control root.'
    }
    $lease = Enter-LabelWriterDelegatedOperation -ControlRoot $WriterFenceControlRoot `
        -SessionId $WriterFenceSessionId -AttemptId $WriterFenceAttemptId `
        -ReplacementTransactionId $WriterFenceReplacementTransactionId `
        -DelegationToken $WriterFenceDelegationToken -Source 'fresh_machine_preservation'
    try {
        $journal = Get-StrictFullPath $FreshMachineJournal 'fresh journal'
        Assert-NoReparsePoint $journal 'fresh journal'
        if ($FreshMachineJournalSha256 -cnotmatch '^[0-9a-f]{64}$' -or
            (Get-FileSha256 $journal) -cne $FreshMachineJournalSha256 -or
            (Get-Item -LiteralPath $journal).Length -gt 16777216) { throw 'Fresh journal pin differs.' }
        $state = Get-Content -LiteralPath $journal -Raw -Encoding UTF8 | ConvertFrom-Json
        if ($state.schema -cne 'label-match-fresh-server-transition-v1' -or
            $state.transition_id -cnotmatch '^[0-9a-f]{32}$' -or
            $state.target.sid -cnotmatch '^S-1-5-21-[0-9-]+$' -or
            $state.phase -cnotin @('PLANNED','QUIESCED','ARCHIVE_VERIFIED','DETACHED') -or
            ($FreshMachineAction -ceq 'Preserve' -and $state.phase -ceq 'PLANNED')) {
            throw 'Machine preservation phase or user binding is invalid.'
        }
        if ($state.target.sid -cne $FreshOperatorSid -or
            $state.target.packet -cne (Get-FileSha256 (Join-Path $SourceRoot 'portable-manifest.json'))) {
            throw 'Machine preservation PC user/packet differs.'
        }
        $pointer = Get-Content -LiteralPath (Join-Path $operatorLocal 'KMTech\Label_Match\server-transition\active-transition.json') `
            -Raw -Encoding UTF8 | ConvertFrom-Json
        $attempts = @($state.attempts)
        if ([string]$pointer.schema -cne 'label-match-fresh-server-transition-v1' -or
            -not (Test-SamePath ([string]$pointer.journal) $journal) -or
            -not (Test-SamePath (Join-Path ([string]$state.paths.control_dir) 'fresh-server-transition.json') $journal) -or
            $attempts.Count -eq 0) {
            throw 'Fresh journal is not the original user current transition record.'
        }
        $attempt = $attempts[$attempts.Count - 1]
        $attemptKind = if ($null -ne $attempt.PSObject.Properties['kind']) { [string]$attempt.kind } else { '' }
        if ([string]$attempt.status -cne 'UNKNOWN' -or
            $attemptKind -cne $(if ($FreshMachineAction -ceq 'Restore') { 'restore' } else { '' }) -or
            [string]$attempt.authority.session_id -cne $WriterFenceSessionId -or
            [string]$attempt.authority.attempt_id -cne $WriterFenceAttemptId -or
            [string]$attempt.authority.replacement_transaction_id -cne $WriterFenceReplacementTransactionId) {
            throw 'Machine preservation is not bound to the current transition attempt.'
        }
        $machine = Get-StrictFullPath ([string]$state.machine_root) 'machine root'
        if (-not $testOverride -and -not (Test-SamePath $machine 'C:\ProgramData')) { throw 'Noncanonical machine root.' }
        $allowed = @('KMTech\Label_Match\data','KMTech\Label_Match\config\app_settings.json',
            'KMTech\Logistics\profiles\Label_Match','KMTech\DirectSync\label_match',
            'KMTech\DirectSync\label-match-margin-r2') | ForEach-Object { Join-Path $machine $_ }
        $archiveRoot = Get-StrictFullPath ([string]$state.archive_root) 'archive'
        if ($archiveRoot.StartsWith('\\') -or $archiveRoot -notmatch '\\KMTech\\server-transition\\Label_Match\\[^\\]+$') {
            throw 'Machine archive location differs.'
        }
        function Assert-FreshAncestors([string]$Path) {
            $cursor = $Path
            while ($cursor) {
                if (Test-Path -LiteralPath $cursor) {
                    if (((Get-Item -LiteralPath $cursor -Force).Attributes -band [IO.FileAttributes]::ReparsePoint) -ne 0) {
                        throw 'Machine preservation ancestor is a reparse point.'
                    }
                }
                $cursor = Split-Path -Parent $cursor
            }
        }
        function Assert-FreshSnapshot([string]$Path, $Snapshot, [bool]$Metadata) {
            Assert-FreshAncestors $Path
            Assert-NoReparsePoint $Path 'fresh state'
            $item = Get-Item -LiteralPath $Path -Force -ErrorAction Stop
            if ([bool]$item.PSIsContainer -ne [bool]$Snapshot.directory) { throw 'Machine state type differs.' }
            if ($Metadata -and ([int64]$item.Attributes -ne [int64]$Snapshot.attributes -or
                (Get-Acl -LiteralPath $Path).Sddl -cne [string]$Snapshot.sddl)) { throw 'Machine state metadata differs.' }
            if ($item.PSIsContainer) {
                $names = @($Snapshot.children.PSObject.Properties.Name)
                if (@(Get-ChildItem -LiteralPath $Path -Force).Count -ne $names.Count) { throw 'Machine membership differs.' }
                foreach ($name in $names) {
                    if ($name -in @('.', '..') -or $name.Contains('\') -or $name.Contains('/')) { throw 'Invalid snapshot leaf.' }
                    Assert-FreshSnapshot (Join-Path $Path $name) $Snapshot.children.$name $Metadata
                }
            } elseif ([int64]$item.Length -ne [int64]$Snapshot.size -or
                (Get-FileSha256 $Path) -cne [string]$Snapshot.sha256) { throw 'Machine state bytes differ.' }
        }
        function Set-FreshTreeAcl([string]$Path, $Snapshot, [bool]$Restore) {
            if ($Snapshot.directory) {
                foreach ($property in $Snapshot.children.PSObject.Properties) {
                    Set-FreshTreeAcl (Join-Path $Path $property.Name) $property.Value $Restore
                }
            }
            $sddl = if ($Restore) { [string]$Snapshot.sddl } else {
                'D:P(A;OICI;FA;;;SY)(A;OICI;FA;;;BA)(A;OICI;FA;;;' + [string]$state.target.sid + ')'
            }
            [LabelFreshDacl]::Set($Path, $sddl, ($Restore -and -not $sddl.Contains('D:P')))
            if ($Restore) { [IO.File]::SetAttributes($Path, [IO.FileAttributes][int]$Snapshot.attributes) }
        }
        # Set only the DACL. Set-Acl can request SACL privileges on a protected
        # file when used a second time; preserving data never requires those.
        Add-Type -TypeDefinition @'
using System;
using System.ComponentModel;
using System.Runtime.InteropServices;
public static class LabelFreshDacl {
    [DllImport("advapi32.dll", CharSet=CharSet.Unicode, SetLastError=true)]
    static extern bool ConvertStringSecurityDescriptorToSecurityDescriptor(string text, uint version, out IntPtr sd, IntPtr size);
    [DllImport("advapi32.dll", CharSet=CharSet.Unicode, SetLastError=true)]
    static extern bool SetFileSecurity(string path, uint flags, IntPtr sd);
    [DllImport("kernel32.dll")] static extern IntPtr LocalFree(IntPtr value);
    public static void Set(string path, string text, bool inherit) {
        IntPtr sd;
        if (!ConvertStringSecurityDescriptorToSecurityDescriptor(text, 1, out sd, IntPtr.Zero))
            throw new Win32Exception(Marshal.GetLastWin32Error());
        try {
            if (!SetFileSecurity(path, 4u | (inherit ? 0x20000000u : 0x80000000u), sd))
                throw new Win32Exception(Marshal.GetLastWin32Error());
        } finally { LocalFree(sd); }
    }
}
'@
        foreach ($entry in @($state.entries | Where-Object { $_.machine -eq $true })) {
            $original = Get-StrictFullPath ([string]$entry.source) 'machine source'
            if (@($allowed | Where-Object { Test-SamePath $_ $original }).Count -ne 1) { throw 'Unsupported machine state root.' }
            $inactive = Get-StrictFullPath ([string]$entry.inactive) 'machine inactive'
            $expectedInactive = Join-Path (Split-Path $original -Parent) ('.' + (Split-Path $original -Leaf) + '.fresh-' + $state.transition_id)
            if (-not (Test-SamePath $inactive $expectedInactive)) { throw 'Inactive location differs.' }
            $archive = Get-StrictFullPath ([string]$entry.archive) 'machine archive'
            if (-not $archive.StartsWith(($archiveRoot + '\items\'), [StringComparison]::OrdinalIgnoreCase) -or
                (Split-Path $archive -Leaf) -notmatch '^[0-9]+$') { throw 'Machine archive escaped its preservation root.' }
            foreach ($path in @($original,$inactive,$archive)) { Assert-FreshAncestors $path }
            if ($FreshMachineAction -ceq 'Restore') {
                if (Test-Path -LiteralPath $inactive) {
                    if (Test-Path -LiteralPath $original) { throw 'Active machine state already exists.' }
                    Assert-FreshSnapshot $archive $entry.snapshot $false
                    Assert-FreshSnapshot $inactive $entry.snapshot $false
                    Move-Item -LiteralPath $inactive -Destination $original -ErrorAction Stop
                    Set-FreshTreeAcl $original $entry.snapshot $true
                }
                # A retry after rename, before metadata restoration is safe only
                # for the original byte tree, and never overwrites a new file.
                Assert-FreshSnapshot $original $entry.snapshot $false
                Set-FreshTreeAcl $original $entry.snapshot $true
                Assert-FreshSnapshot $original $entry.snapshot $true
                continue
            }
            $from = if (Test-Path -LiteralPath $inactive) { $inactive } else { $original }
            $originalExists = Test-Path -LiteralPath $original
            if ($from -eq $inactive -and $originalExists) { throw 'Machine state has two active candidates.' }
            Assert-FreshSnapshot $from $entry.snapshot ($from -eq $original)
            if (-not (Test-Path -LiteralPath $archive)) {
                [void][IO.Directory]::CreateDirectory((Split-Path $archive -Parent))
                $staging = Join-Path (Split-Path $archive -Parent) ('.c-' + [Guid]::NewGuid().ToString('N').Substring(0,12))
                Copy-Item -LiteralPath $from -Destination $staging -Recurse -ErrorAction Stop
                $files = if ($entry.snapshot.directory) { @(Get-ChildItem -LiteralPath $staging -File -Recurse -Force) } else { @(Get-Item -LiteralPath $staging) }
                foreach ($file in $files) {
                    $stream = [IO.File]::Open($file.FullName, 'Open', 'ReadWrite', 'Read')
                    try { $stream.Flush($true) } finally { $stream.Dispose() }
                }
                Assert-FreshSnapshot $staging $entry.snapshot $false
                Move-Item -LiteralPath $staging -Destination $archive -ErrorAction Stop
            }
            Assert-FreshSnapshot $archive $entry.snapshot $false
            if ($from -eq $original) { Move-Item -LiteralPath $original -Destination $inactive -ErrorAction Stop }
            Set-FreshTreeAcl $inactive $entry.snapshot $false
            Assert-FreshSnapshot $inactive $entry.snapshot $false
        }
        Write-Output 'fresh_machine_preservation=PASS'
    }
    finally { Exit-LabelWriterAdmission $lease }
}

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

if ($FreshMachineAction) {
    Invoke-FreshMachinePreservation
    return
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
    }
    if (Test-Path -LiteralPath $installRootFull) {
        Remove-Item -LiteralPath $installRootFull -Recurse -Force -ErrorAction Stop
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
