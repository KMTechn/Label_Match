<#
Label_Match canonical zero-PE portable installer.

This product-owned installer is deliberately fail-closed and ships inside the
immutable portable root.  It implements the shared one-session v1 interface.

Current-user onboarding, HKCU Run, current-user Limited PT1M task registration,
identity/credential recovery, and relay launch stay in the product's
--onboard-current-user implementation.  This script owns only elevated code
placement and its exact code rollback so a parent orchestrator can use one UAC
session for several apps.
#>

[CmdletBinding()]
param(
    [string]$SourceRoot = '',
    [string]$InstallRoot = 'C:\KMTech\Apps\Label_Match\current',
    [string]$EvidencePath = '',
    [switch]$PlanOnly,
    [switch]$CodePlacementOnly,
    [switch]$Rollback
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'
$ProgressPreference = 'SilentlyContinue'

$Config = [ordered]@{
    contract_version = 'kmtech-canonical-portable-installer-v1'
    app_id = 'label_match'
    app_name = 'Label_Match'
    manifest_schema = 'label-match-portable-tree-v1'
    canonical_install_root = 'C:\KMTech\Apps\Label_Match\current'
    hkcu_run_name = 'KMTech.LabelMatch.Relay'
    persistent_relay_mode = '--label-match-user-relay'
    scheduled_task_name = 'direct-sync-relay-label-match'
    installed_owner_receipt_name = '.kmtech-canonical-install-owner.json'
    expected_pe_count = 46
    required_relative_paths = @(
        'portable-manifest.json',
        'runtime\python.exe',
        'runtime\pythonw.exe',
        'app\main.py',
        'launch-label-match.cmd',
        'INSTALL_CANONICAL_PORTABLE.ps1',
        'INSTALL_THIS_PC.ps1'
    )
}
function Assert-TemplateConfigured {
    foreach ($property in $Config.GetEnumerator()) {
        if ([string]$property.Value -match '__[A-Z0-9_]+__') {
            throw "Installer template is not configured: $($property.Key)"
        }
    }
    if ($InstallRoot -match '__[A-Z0-9_]+__') {
        throw 'Installer template InstallRoot is not configured.'
    }
}

function Get-FullPath([string]$Value, [string]$Purpose) {
    if ([string]::IsNullOrWhiteSpace($Value) -or -not [IO.Path]::IsPathRooted($Value)) {
        throw "$Purpose must be an absolute path."
    }
    if ($Value.StartsWith('\\?\') -or $Value.StartsWith('\\.\')) {
        throw "$Purpose must not use a device namespace."
    }
    $full = [IO.Path]::GetFullPath($Value).TrimEnd('\')
    if ($full -eq [IO.Path]::GetPathRoot($full)) {
        throw "$Purpose must not be a filesystem root."
    }
    return $full
}

function Test-SamePath([string]$Left, [string]$Right) {
    try {
        return (Get-FullPath $Left 'left path').Equals(
            (Get-FullPath $Right 'right path'),
            [StringComparison]::OrdinalIgnoreCase
        )
    }
    catch {
        return $false
    }
}

function Test-PathInside([string]$Candidate, [string]$Root) {
    $candidateFull = (Get-FullPath $Candidate 'candidate path') + '\'
    $rootFull = (Get-FullPath $Root 'root path') + '\'
    return $candidateFull.StartsWith($rootFull, [StringComparison]::OrdinalIgnoreCase)
}

function Assert-SafeOwnedWorkPath([string]$Path, [string]$Parent, [string]$Prefix) {
    $full = Get-FullPath $Path 'owned work path'
    $parentFull = Get-FullPath $Parent 'owned work parent'
    if (-not (Test-PathInside $full $parentFull)) {
        throw 'Owned work path escaped its canonical parent.'
    }
    if (-not ([IO.Path]::GetFileName($full)).StartsWith($Prefix, [StringComparison]::Ordinal)) {
        throw 'Owned work path has an unexpected name.'
    }
    return $full
}

function Get-Sha256([string]$Path) {
    return (Get-FileHash -LiteralPath $Path -Algorithm SHA256 -ErrorAction Stop).Hash.ToLowerInvariant()
}

function Write-JsonAtomic([string]$Path, $Payload, [switch]$AllowReplace) {
    $full = Get-FullPath $Path 'evidence path'
    if (-not $full.StartsWith('E:\', [StringComparison]::OrdinalIgnoreCase)) {
        throw 'Production evidence must be written to E:.'
    }
    if ((Test-Path -LiteralPath $full) -and -not $AllowReplace.IsPresent) {
        throw 'Refusing to overwrite an existing evidence path.'
    }
    $parent = Split-Path -Parent $full
    New-Item -ItemType Directory -Path $parent -Force -ErrorAction Stop | Out-Null
    $temporary = Join-Path $parent ('.{0}.{1}.{2}.tmp' -f ([IO.Path]::GetFileName($full)), $PID, [guid]::NewGuid().ToString('N'))
    try {
        $json = $Payload | ConvertTo-Json -Depth 30
        [IO.File]::WriteAllText($temporary, $json + [Environment]::NewLine, [Text.UTF8Encoding]::new($false))
        Move-Item -LiteralPath $temporary -Destination $full -Force:$AllowReplace.IsPresent -ErrorAction Stop
    }
    finally {
        Remove-Item -LiteralPath $temporary -Force -ErrorAction SilentlyContinue
    }
}

function Read-BoundedJson([string]$Path, [int64]$MaximumBytes = 1048576) {
    if (-not (Test-Path -LiteralPath $Path -PathType Leaf)) { throw "JSON file is absent: $Path" }
    $item = Get-Item -LiteralPath $Path -Force
    if ($item.Length -le 0 -or $item.Length -gt $MaximumBytes) { throw "JSON file size is invalid: $Path" }
    return Get-Content -LiteralPath $Path -Raw -Encoding UTF8 | ConvertFrom-Json
}

function Assert-NoReparsePoint([string]$Root) {
    $rootItem = Get-Item -LiteralPath $Root -Force -ErrorAction Stop
    foreach ($item in @($rootItem) + @(Get-ChildItem -LiteralPath $Root -Force -Recurse -ErrorAction Stop)) {
        if (($item.Attributes -band [IO.FileAttributes]::ReparsePoint) -ne 0) {
            throw "Reparse points are forbidden: $($item.FullName)"
        }
    }
}

function Get-RelativePath([string]$Root, [string]$Path) {
    $rootFull = (Get-FullPath $Root 'inventory root') + '\'
    $pathFull = Get-FullPath $Path 'inventory path'
    if (-not $pathFull.StartsWith($rootFull, [StringComparison]::OrdinalIgnoreCase)) {
        throw 'Inventory path escaped its root.'
    }
    return $pathFull.Substring($rootFull.Length).Replace('\', '/')
}

function Get-CodeInventory([string]$Root) {
    $receiptName = [string]$Config.installed_owner_receipt_name
    $rows = [Collections.Generic.List[object]]::new()
    foreach ($file in @(Get-ChildItem -LiteralPath $Root -File -Force -Recurse -ErrorAction Stop)) {
        $relative = Get-RelativePath $Root $file.FullName
        if ($relative -ceq $receiptName) { continue }
        $rows.Add([ordered]@{
            path = $relative
            bytes = [int64]$file.Length
            sha256 = Get-Sha256 $file.FullName
        })
    }
    return @($rows | Sort-Object path)
}

function Get-InventoryAggregate([object[]]$Rows) {
    $builder = [Text.StringBuilder]::new()
    foreach ($row in $Rows) {
        [void]$builder.Append([string]$row.path)
        [void]$builder.Append("`0")
        [void]$builder.Append([string]$row.bytes)
        [void]$builder.Append("`0")
        [void]$builder.Append([string]$row.sha256)
        [void]$builder.Append("`n")
    }
    $sha = [Security.Cryptography.SHA256]::Create()
    try {
        $bytes = [Text.Encoding]::UTF8.GetBytes($builder.ToString())
        return ([BitConverter]::ToString($sha.ComputeHash($bytes))).Replace('-', '').ToLowerInvariant()
    }
    finally {
        $sha.Dispose()
    }
}

function Get-PeInventory([string]$Root) {
    $rows = [Collections.Generic.List[object]]::new()
    foreach ($file in @(Get-ChildItem -LiteralPath $Root -File -Force -Recurse -ErrorAction Stop)) {
        if ($file.Length -lt 2) { continue }
        $stream = [IO.File]::Open($file.FullName, [IO.FileMode]::Open, [IO.FileAccess]::Read, [IO.FileShare]::Read)
        try {
            $isPe = ($stream.ReadByte() -eq 0x4d -and $stream.ReadByte() -eq 0x5a)
        }
        finally {
            $stream.Dispose()
        }
        if (-not $isPe) { continue }
        $signature = Get-AuthenticodeSignature -LiteralPath $file.FullName
        $rows.Add([ordered]@{
            path = Get-RelativePath $Root $file.FullName
            bytes = [int64]$file.Length
            sha256 = Get-Sha256 $file.FullName
            status = [string]$signature.Status
            signer = if ($null -ne $signature.SignerCertificate) { [string]$signature.SignerCertificate.Subject } else { '' }
        })
    }
    $sorted = @($rows | Sort-Object path)
    return [ordered]@{
        pe_count = $sorted.Count
        valid_count = @($sorted | Where-Object status -eq 'Valid').Count
        unsigned_count = @($sorted | Where-Object status -eq 'NotSigned').Count
        other_status_count = @($sorted | Where-Object { $_.status -notin @('Valid', 'NotSigned') }).Count
        files = $sorted
    }
}

function Assert-PeGate($Inventory) {
    if (
        [int]$Inventory.pe_count -ne [int]$Config.expected_pe_count -or
        [int]$Inventory.valid_count -ne [int]$Config.expected_pe_count -or
        [int]$Inventory.unsigned_count -ne 0 -or
        [int]$Inventory.other_status_count -ne 0
    ) {
        throw (
            'Portable PE inventory is not exact {0}/{0}/0/0; observed {1}/{2}/{3}/{4}.' -f
            $Config.expected_pe_count,
            $Inventory.pe_count,
            $Inventory.valid_count,
            $Inventory.unsigned_count,
            $Inventory.other_status_count
        )
    }
}

function Get-IsAdministrator {
    $identity = [Security.Principal.WindowsIdentity]::GetCurrent()
    $principal = [Security.Principal.WindowsPrincipal]::new($identity)
    return $principal.IsInRole([Security.Principal.WindowsBuiltInRole]::Administrator)
}

function Get-SidText($IdentityReference) {
    return [string]$IdentityReference.Translate([Security.Principal.SecurityIdentifier]).Value
}

function Assert-HardenedAcl([string]$Root) {
    $allowed = @('S-1-5-18', 'S-1-5-32-544', 'S-1-5-32-545')
    foreach ($item in @((Get-Item -LiteralPath $Root -Force)) + @(Get-ChildItem -LiteralPath $Root -Force -Recurse -ErrorAction Stop)) {
        $acl = Get-Acl -LiteralPath $item.FullName
        $ownerSid = Get-SidText ([Security.Principal.NTAccount]::new([string]$acl.Owner))
        if ($ownerSid -cne 'S-1-5-32-544') { throw "Code owner is not BUILTIN Administrators: $($item.FullName)" }
        if (-not $acl.AreAccessRulesProtected) { throw "Code DACL inherits from its parent: $($item.FullName)" }
        foreach ($rule in @($acl.Access)) {
            $sid = Get-SidText $rule.IdentityReference
            if ($rule.AccessControlType -eq [Security.AccessControl.AccessControlType]::Allow -and $allowed -notcontains $sid) {
                throw "Unexpected allow ACE on code: $sid $($item.FullName)"
            }
            if ($sid -ceq 'S-1-5-32-545' -and $rule.AccessControlType -eq [Security.AccessControl.AccessControlType]::Allow) {
                $writeMask = [Security.AccessControl.FileSystemRights]::Write -bor
                    [Security.AccessControl.FileSystemRights]::Modify -bor
                    [Security.AccessControl.FileSystemRights]::FullControl -bor
                    [Security.AccessControl.FileSystemRights]::Delete
                if (($rule.FileSystemRights -band $writeMask) -ne 0) {
                    throw "BUILTIN Users can modify code: $($item.FullName)"
                }
            }
        }
    }
}

function Set-HardenedAcl([string]$Root) {
    & icacls.exe $Root '/setowner' '*S-1-5-32-544' '/T' '/C' '/L' | Out-Null
    if ($LASTEXITCODE -ne 0) { throw 'Could not set code owner.' }
    & icacls.exe $Root `
        '/inheritance:r' `
        '/grant:r' `
        '*S-1-5-18:(OI)(CI)F' `
        '*S-1-5-32-544:(OI)(CI)F' `
        '*S-1-5-32-545:(OI)(CI)RX' `
        '/T' '/C' '/L' | Out-Null
    if ($LASTEXITCODE -ne 0) { throw 'Could not apply hardened code DACL.' }
    Assert-HardenedAcl $Root
}

function Get-DirectoryAclPreimage([string]$Path) {
    if (-not (Test-Path -LiteralPath $Path)) {
        return [ordered]@{ existed = $false; path = $Path }
    }
    if (-not (Test-Path -LiteralPath $Path -PathType Container)) {
        throw 'Canonical app parent exists but is not a directory.'
    }
    $item = Get-Item -LiteralPath $Path -Force
    if (($item.Attributes -band [IO.FileAttributes]::ReparsePoint) -ne 0) {
        throw 'Canonical app parent is a reparse point.'
    }
    $acl = Get-Acl -LiteralPath $Path
    $sections = [Security.AccessControl.AccessControlSections]::Access -bor
        [Security.AccessControl.AccessControlSections]::Owner -bor
        [Security.AccessControl.AccessControlSections]::Group
    return [ordered]@{
        existed = $true
        path = $Path
        sddl = $acl.GetSecurityDescriptorSddlForm($sections)
    }
}

function Restore-DirectoryAclPreimage($Preimage) {
    $path = Get-FullPath ([string]$Preimage.path) 'parent ACL preimage path'
    if ([bool]$Preimage.existed) {
        if (-not (Test-Path -LiteralPath $path -PathType Container)) {
            throw 'Cannot restore the original app-parent ACL because the directory is absent.'
        }
        $security = [Security.AccessControl.DirectorySecurity]::new()
        $security.SetSecurityDescriptorSddlForm([string]$Preimage.sddl)
        Set-Acl -LiteralPath $path -AclObject $security -ErrorAction Stop
        $sections = [Security.AccessControl.AccessControlSections]::Access -bor
            [Security.AccessControl.AccessControlSections]::Owner -bor
            [Security.AccessControl.AccessControlSections]::Group
        $readback = (Get-Acl -LiteralPath $path).GetSecurityDescriptorSddlForm($sections)
        if ($readback -cne [string]$Preimage.sddl) {
            throw 'App-parent ACL preimage restoration readback failed.'
        }
        return
    }
    if (Test-Path -LiteralPath $path -PathType Container) {
        if (@(Get-ChildItem -LiteralPath $path -Force).Count -ne 0) {
            throw 'A newly created app parent is not empty during rollback.'
        }
        Remove-Item -LiteralPath $path -Force -ErrorAction Stop
    }
    if (Test-Path -LiteralPath $path) { throw 'A newly created app parent survived rollback.' }
}

function Assert-RequiredSource([string]$Root) {
    foreach ($relative in @($Config.required_relative_paths)) {
        if (-not (Test-Path -LiteralPath (Join-Path $Root $relative) -PathType Leaf)) {
            throw "Portable source is missing $relative."
        }
    }
}

function Get-SourcePlan([string]$Root, [string]$Target) {
    Assert-NoReparsePoint $Root
    Assert-RequiredSource $Root
    $manifestPath = Join-Path $Root 'portable-manifest.json'
    $manifest = Read-BoundedJson $manifestPath 65536
    if (
        [string]$manifest.schema -cne [string]$Config.manifest_schema -or
        [string]$manifest.entrypoint -cne 'runtime/pythonw.exe app/main.py' -or
        [string]$manifest.launcher -cne 'launch-label-match.cmd' -or
        [string]$manifest.canonical_installer -cne 'INSTALL_CANONICAL_PORTABLE.ps1' -or
        [string]$manifest.source_commit -cnotmatch '^[0-9a-f]{40}$' -or
        [string]$manifest.source_tree -cnotmatch '^[0-9a-f]{40}$' -or
        @($manifest.allowed_unsigned_app_pe).Count -ne 0 -or
        @($manifest.forbidden_package_roots).Count -ne 0
    ) {
        throw 'Portable manifest contract is invalid.'
    }
    if ((Get-Sha256 (Join-Path $Root 'runtime\pythonw.exe')) -cne ([string]$manifest.runtime_pythonw_sha256).ToLowerInvariant()) {
        throw 'runtime/pythonw.exe differs from the portable manifest.'
    }
    if ((Get-Sha256 (Join-Path $Root 'runtime\python.exe')) -cne ([string]$manifest.runtime_python_sha256).ToLowerInvariant()) {
        throw 'runtime/python.exe differs from the portable manifest.'
    }
    if ((Get-Sha256 (Join-Path $Root 'launch-label-match.cmd')) -cne ([string]$manifest.launcher_sha256).ToLowerInvariant()) {
        throw 'The launcher differs from the portable manifest.'
    }
    if ((Get-Sha256 (Join-Path $Root 'INSTALL_CANONICAL_PORTABLE.ps1')) -cne ([string]$manifest.canonical_installer_sha256).ToLowerInvariant()) {
        throw 'The canonical installer differs from the portable manifest.'
    }
    $inventory = @(Get-CodeInventory $Root)
    $pe = Get-PeInventory $Root
    Assert-PeGate $pe
    $existing = Get-ExistingInstallPlan $Target
    return [ordered]@{
        source_root = $Root
        source_manifest_path = $manifestPath
        source_manifest_sha256 = Get-Sha256 $manifestPath
        source_commit = [string]$manifest.source_commit
        source_tree = [string]$manifest.source_tree
        source_file_count = $inventory.Count
        source_byte_count = [int64](($inventory | Measure-Object -Property bytes -Sum).Sum)
        source_inventory_sha256 = Get-InventoryAggregate $inventory
        installer_sha256 = Get-Sha256 (Join-Path $Root 'INSTALL_CANONICAL_PORTABLE.ps1')
        pe = $pe
        prestate = $existing
    }
}

function Get-ExistingInstallPlan([string]$Target) {
    if (-not (Test-Path -LiteralPath $Target)) {
        return [ordered]@{ disposition = 'absent' }
    }
    if (-not (Test-Path -LiteralPath $Target -PathType Container)) {
        throw 'Canonical target exists but is not a directory.'
    }
    Assert-NoReparsePoint $Target
    $receiptPath = Join-Path $Target ([string]$Config.installed_owner_receipt_name)
    $receipt = Read-BoundedJson $receiptPath 1048576
    $manifestPath = Join-Path $Target 'portable-manifest.json'
    if (
        [string]$receipt.schema -cne 'kmtech-canonical-installed-owner-v1' -or
        [string]$receipt.app_id -cne [string]$Config.app_id -or
        -not (Test-SamePath ([string]$receipt.install_root) $Target) -or
        (Get-Sha256 $manifestPath) -cne ([string]$receipt.installed_manifest_sha256).ToLowerInvariant()
    ) {
        throw 'Existing canonical target lacks an exact app-owned receipt.'
    }
    $inventory = @(Get-CodeInventory $Target)
    if ((Get-InventoryAggregate $inventory) -cne ([string]$receipt.installed_inventory_sha256).ToLowerInvariant()) {
        throw 'Existing canonical code inventory drifted from its owner receipt.'
    }
    $pe = Get-PeInventory $Target
    Assert-PeGate $pe
    Assert-HardenedAcl $Target
    return [ordered]@{
        disposition = 'owned_exact'
        owner_receipt_path = $receiptPath
        owner_receipt_sha256 = Get-Sha256 $receiptPath
        installed_inventory_sha256 = Get-InventoryAggregate $inventory
    }
}

function Test-HkcuRunValueAbsent {
    $path = 'Registry::HKEY_CURRENT_USER\Software\Microsoft\Windows\CurrentVersion\Run'
    try {
        $value = Get-ItemPropertyValue -LiteralPath $path -Name ([string]$Config.hkcu_run_name) -ErrorAction Stop
        return [string]::IsNullOrWhiteSpace([string]$value)
    }
    catch [Management.Automation.ItemNotFoundException] { return $true }
    catch [Management.Automation.PSArgumentException] { return $true }
}

function Test-CurrentUserTaskAbsent {
    $task = Get-ScheduledTask -TaskPath '\' -TaskName ([string]$Config.scheduled_task_name) -ErrorAction SilentlyContinue
    return $null -eq $task
}

function New-BaseEvidence([string]$Operation, [string]$Status, [string]$Source, [string]$Target) {
    $identity = [Security.Principal.WindowsIdentity]::GetCurrent()
    return [ordered]@{
        schema = [string]$Config.contract_version
        app_id = [string]$Config.app_id
        app_name = [string]$Config.app_name
        operation = $Operation
        status = $Status
        run_id = [guid]::NewGuid().ToString('N')
        captured_at_utc = [DateTime]::UtcNow.ToString('o')
        operator_sid = [string]$identity.User.Value
        elevated = Get-IsAdministrator
        source_root = $Source
        install_root = $Target
    }
}

Assert-TemplateConfigured
$modeCount = @(@($PlanOnly.IsPresent, $CodePlacementOnly.IsPresent, $Rollback.IsPresent) | Where-Object { $_ }).Count
if ($modeCount -ne 1) { throw 'Select exactly one of PlanOnly, CodePlacementOnly, or Rollback.' }

$targetRoot = Get-FullPath $InstallRoot 'InstallRoot'
if (-not (Test-SamePath $targetRoot ([string]$Config.canonical_install_root))) {
    throw 'InstallRoot is not this app canonical root.'
}
$evidenceFull = Get-FullPath $EvidencePath 'EvidencePath'
if (-not $evidenceFull.StartsWith('E:\', [StringComparison]::OrdinalIgnoreCase)) {
    throw 'EvidencePath must be on E:.'
}

if ($Rollback.IsPresent) {
    if (-not (Get-IsAdministrator)) { throw 'Rollback requires the parent orchestrator elevated session.' }
    $evidence = Read-BoundedJson $evidenceFull 1048576
    if (
        [string]$evidence.schema -cne [string]$Config.contract_version -or
        [string]$evidence.app_id -cne [string]$Config.app_id -or
        [string]$evidence.operation -cne 'CodePlacementOnly' -or
        [string]$evidence.status -cne 'PASS' -or
        -not (Test-SamePath ([string]$evidence.install_root) $targetRoot)
    ) {
        throw 'Rollback evidence does not own this installed target.'
    }
    if (-not (Test-HkcuRunValueAbsent)) {
        throw 'Remove current-user setup before rolling back canonical code.'
    }
    if (-not (Test-CurrentUserTaskAbsent)) {
        throw 'Remove the current-user scheduled task before rolling back canonical code.'
    }
    $ownerReceiptPath = Join-Path $targetRoot ([string]$Config.installed_owner_receipt_name)
    $ownerReceipt = Read-BoundedJson $ownerReceiptPath 1048576
    if (
        [string]$ownerReceipt.run_id -cne [string]$evidence.run_id -or
        [string]$ownerReceipt.installed_inventory_sha256 -cne [string]$evidence.installed.inventory_sha256
    ) {
        throw 'Installed code is no longer the exact code owned by this evidence.'
    }
    $parentRoot = Split-Path -Parent $targetRoot
    $rollbackRoot = [string]$evidence.rollback.code_backup_path
    if ([string]$evidence.rollback.prestate -ceq 'absent') {
        if (-not [string]::IsNullOrWhiteSpace($rollbackRoot)) { throw 'Absent prestate unexpectedly names a code backup.' }
        Remove-Item -LiteralPath $targetRoot -Recurse -Force -ErrorAction Stop
        if (Test-Path -LiteralPath $targetRoot) { throw 'New canonical target survived rollback.' }
    }
    elseif ([string]$evidence.rollback.prestate -ceq 'owned_exact') {
        [void](Assert-SafeOwnedWorkPath $rollbackRoot $parentRoot '.current.rollback.')
        if (-not (Test-Path -LiteralPath $rollbackRoot -PathType Container)) { throw 'Prior code backup is absent.' }
        Remove-Item -LiteralPath $targetRoot -Recurse -Force -ErrorAction Stop
        Move-Item -LiteralPath $rollbackRoot -Destination $targetRoot -ErrorAction Stop
        [void](Get-ExistingInstallPlan $targetRoot)
    }
    else { throw 'Rollback prestate is invalid.' }
    Restore-DirectoryAclPreimage $evidence.rollback.parent_preimage
    $evidence.status = 'ROLLED_BACK'
    $evidence.rollback.completed_at_utc = [DateTime]::UtcNow.ToString('o')
    Write-JsonAtomic $evidenceFull $evidence -AllowReplace
    Write-Output 'installer_status=ROLLED_BACK'
    Write-Output "installer_evidence=$evidenceFull"
    exit 0
}

$sourceRootFull = Get-FullPath $SourceRoot 'SourceRoot'
if (-not (Test-Path -LiteralPath $sourceRootFull -PathType Container)) { throw 'SourceRoot does not exist.' }
if (Test-SamePath $sourceRootFull $targetRoot -or (Test-PathInside $sourceRootFull $targetRoot) -or (Test-PathInside $targetRoot $sourceRootFull)) {
    throw 'SourceRoot and InstallRoot must not overlap.'
}
$plan = Get-SourcePlan $sourceRootFull $targetRoot

if ($PlanOnly.IsPresent) {
    $evidence = New-BaseEvidence 'PlanOnly' 'PLAN_READY' $sourceRootFull $targetRoot
    $evidence.source = $plan
    $evidence.rollback_feasible = $true
    $evidence.mutation_scope = 'evidence_only'
    $evidence.current_user_phase = [ordered]@{
        owner = 'runtime/python.exe -I -B app/main.py --onboard-current-user'
        hkcu_run_name = [string]$Config.hkcu_run_name
        scheduled_task_name = [string]$Config.scheduled_task_name
        persistent_relay_mode = [string]$Config.persistent_relay_mode
        mutation = 'DEFERRED_TO_UNELEVATED_PRODUCT_ONBOARDING'
        stop_marker = 'PRESERVED_UNTIL_CANONICAL_BINDING_AND_PINNED_EXACT_CLONE_CONFLICT_RESOLUTION'
        relay_survival = 'DEFERRED_TO_PARENT_NATURAL_TRIGGER_AND_PROCESS_POSTCHECK'
    }
    Write-JsonAtomic $evidenceFull $evidence
    Write-Output 'installer_status=PLAN_READY'
    Write-Output "installer_evidence=$evidenceFull"
    exit 0
}

if (-not (Get-IsAdministrator)) { throw 'CodePlacementOnly requires the parent orchestrator elevated session.' }
if (Test-Path -LiteralPath $evidenceFull) { throw 'CodePlacementOnly evidence path already exists.' }

$parentRoot = Split-Path -Parent $targetRoot
$parentAclPreimage = Get-DirectoryAclPreimage $parentRoot
$nonce = [guid]::NewGuid().ToString('N')
$stagingRoot = Assert-SafeOwnedWorkPath (Join-Path $parentRoot ('.current.staging.' + $nonce)) $parentRoot '.current.staging.'
$backupRoot = Assert-SafeOwnedWorkPath (Join-Path $parentRoot ('.current.rollback.' + $nonce)) $parentRoot '.current.rollback.'
$movedPrior = $false
$activatedNew = $false
$evidence = New-BaseEvidence 'CodePlacementOnly' 'STARTED' $sourceRootFull $targetRoot
$evidence.source = $plan
$evidence.rollback = [ordered]@{
    prestate = [string]$plan.prestate.disposition
    code_backup_path = if ([string]$plan.prestate.disposition -ceq 'owned_exact') { $backupRoot } else { '' }
    parent_preimage = $parentAclPreimage
    available = $true
}

try {
    New-Item -ItemType Directory -Path $parentRoot -Force -ErrorAction Stop | Out-Null
    Set-HardenedAcl $parentRoot
    Copy-Item -LiteralPath $sourceRootFull -Destination $stagingRoot -Recurse -Force -ErrorAction Stop
    Assert-NoReparsePoint $stagingRoot
    $stagedInventory = @(Get-CodeInventory $stagingRoot)
    $stagedPe = Get-PeInventory $stagingRoot
    Assert-PeGate $stagedPe
    if (
        $stagedInventory.Count -ne [int]$plan.source_file_count -or
        (Get-InventoryAggregate $stagedInventory) -cne [string]$plan.source_inventory_sha256
    ) { throw 'Staged code differs from source inventory.' }
    Set-HardenedAcl $stagingRoot

    if ([string]$plan.prestate.disposition -ceq 'owned_exact') {
        Move-Item -LiteralPath $targetRoot -Destination $backupRoot -ErrorAction Stop
        $movedPrior = $true
    }
    Move-Item -LiteralPath $stagingRoot -Destination $targetRoot -ErrorAction Stop
    $activatedNew = $true

    $installedInventory = @(Get-CodeInventory $targetRoot)
    $installedPe = Get-PeInventory $targetRoot
    Assert-PeGate $installedPe
    $installedAggregate = Get-InventoryAggregate $installedInventory
    if (
        $installedInventory.Count -ne [int]$plan.source_file_count -or
        $installedAggregate -cne [string]$plan.source_inventory_sha256
    ) { throw 'Installed code differs from source inventory.' }
    Assert-HardenedAcl $targetRoot

    $ownerReceiptPath = Join-Path $targetRoot ([string]$Config.installed_owner_receipt_name)
    $ownerReceipt = [ordered]@{
        schema = 'kmtech-canonical-installed-owner-v1'
        app_id = [string]$Config.app_id
        run_id = [string]$evidence.run_id
        install_root = $targetRoot
        source_commit = [string]$plan.source_commit
        source_tree = [string]$plan.source_tree
        installed_manifest_sha256 = Get-Sha256 (Join-Path $targetRoot 'portable-manifest.json')
        installed_inventory_sha256 = $installedAggregate
        installed_file_count = $installedInventory.Count
        installer_sha256 = [string]$plan.installer_sha256
        installed_at_utc = [DateTime]::UtcNow.ToString('o')
    }
    # The owner receipt is the one excluded mutable install metadata file.
    $ownerJson = $ownerReceipt | ConvertTo-Json -Depth 10
    [IO.File]::WriteAllText($ownerReceiptPath, $ownerJson + [Environment]::NewLine, [Text.UTF8Encoding]::new($false))
    Set-HardenedAcl $targetRoot

    $evidence.status = 'PASS'
    $evidence.installed = [ordered]@{
        inventory_sha256 = $installedAggregate
        file_count = $installedInventory.Count
        manifest_sha256 = Get-Sha256 (Join-Path $targetRoot 'portable-manifest.json')
        owner_receipt_path = $ownerReceiptPath
        owner_receipt_sha256 = Get-Sha256 $ownerReceiptPath
        pe_count = $installedPe.pe_count
        valid_count = $installedPe.valid_count
        unsigned_count = $installedPe.unsigned_count
        other_status_count = $installedPe.other_status_count
    }
    $evidence.completed_at_utc = [DateTime]::UtcNow.ToString('o')
    Write-JsonAtomic $evidenceFull $evidence
    Write-Output 'installer_status=PASS'
    Write-Output "installer_evidence=$evidenceFull"
    exit 0
}
catch {
    $failure = $_
    $rollbackError = ''
    try {
        if ($activatedNew -and (Test-Path -LiteralPath $targetRoot)) {
            Remove-Item -LiteralPath $targetRoot -Recurse -Force -ErrorAction Stop
        }
        if ($movedPrior -and (Test-Path -LiteralPath $backupRoot -PathType Container)) {
            Move-Item -LiteralPath $backupRoot -Destination $targetRoot -ErrorAction Stop
        }
        if (Test-Path -LiteralPath $stagingRoot) {
            Remove-Item -LiteralPath $stagingRoot -Recurse -Force -ErrorAction Stop
        }
        Restore-DirectoryAclPreimage $parentAclPreimage
    }
    catch { $rollbackError = $_.Exception.Message }
    $evidence.status = if ([string]::IsNullOrWhiteSpace($rollbackError)) { 'FAILED_ROLLED_BACK' } else { 'ROLLBACK_FAILED' }
    $evidence.failure = [ordered]@{
        error_type = $failure.Exception.GetType().FullName
        error_message = $failure.Exception.Message
        rollback_error = $rollbackError
    }
    try { Write-JsonAtomic $evidenceFull $evidence } catch { }
    throw $failure
}
