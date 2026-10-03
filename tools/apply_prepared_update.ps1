[CmdletBinding()]
param(
    [Parameter(Mandatory=$true)][string]$InstallRoot,
    [ValidateSet('Apply','Observe','Verify','Authority','ObserveLifecycle')][string]$Mode = 'Apply',
    [string]$CodeRoot = '',
    [switch]$SupportedLifecycle
)
$ErrorActionPreference = 'Stop'
$script:LabelStartupBootstrapLoaded=$false

function ConvertTo-LabelTaskSid([string]$Identity) {
    if ([string]::IsNullOrWhiteSpace($Identity)) { throw 'TASK_USER_ID_UNRESOLVED' }
    if ($Identity -match '^S-1-') {
        return [Security.Principal.SecurityIdentifier]::new($Identity).Value
    }
    return [Security.Principal.NTAccount]::new($Identity).Translate([Security.Principal.SecurityIdentifier]).Value
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
if ($SupportedLifecycle -and $Mode -cne 'ObserveLifecycle') { throw 'SUPPORTED_OBSERVATION_ONLY' }
if ($Mode -ceq 'ObserveLifecycle') {
    if (-not $SupportedLifecycle -or $CodeRoot -or
        -not ([IO.Path]::GetFullPath($InstallRoot)).Equals('C:\KMTech\Apps\Label_Match\current',[StringComparison]::OrdinalIgnoreCase)) {
        throw 'SUPPORTED_OBSERVATION_ROOT_DIFFERS'
    }
    $tasks = @(Get-LabelSupportedStartupTasks $InstallRoot)
    $legacy = @(Get-ScheduledTask -ErrorAction Stop | Where-Object {
        [string]$_.TaskName -ieq 'direct-sync-relay-label-match-current-pc'
    })
    @{task_exists=($tasks.Count -gt 0 -or $legacy.Count -gt 0)} | ConvertTo-Json -Compress
    exit 0
}
function Assert-Protected([string]$Path, [switch]$Recursive, [hashtable]$Verified = $null) {
    if ($null -eq $Verified) { $Verified = @{} }
    $full = [IO.Path]::GetFullPath($Path)
    if($full -ne [IO.Path]::GetPathRoot($full)){$full=$full.TrimEnd([char[]]'\/')}
    $leaf = $true
    while ($full) {
        if (-not $Verified.ContainsKey($full) -or ($leaf -and -not $Verified[$full])) {
        $entry = Get-Item -LiteralPath $full -ErrorAction Stop
        if ($entry.Attributes -band [IO.FileAttributes]::ReparsePoint) { throw 'Reparse point in update storage.' }
        $acl = Get-Acl -LiteralPath $full -ErrorAction Stop
        $owner = $acl.GetOwner([Security.Principal.SecurityIdentifier]).Value
        if ($owner -notin @('S-1-5-18','S-1-5-32-544')) { throw 'Update storage owner is not privileged.' }
        if($script:LabelStartupBootstrapLoaded -and $full -in @('C:\KMTech','C:\KMTech\Apps')){
            [void](Protect-KmtechCodeAncestors 'C:\KMTech\Apps\Label_Match' -AdmissionOnly -AncestorPath $full -RequireProtected)
            $Verified[$full]=$leaf
            $full=[IO.Path]::GetDirectoryName($full);$leaf=$false;continue
        }
        $mask = if ($leaf) { 0x500D0156 } else { 0x500D0150 }
        foreach ($rule in $acl.GetAccessRules($true,$true,[Security.Principal.SecurityIdentifier])) {
            if ($rule.AccessControlType -eq 'Allow' -and
                -not ($rule.PropagationFlags -band [Security.AccessControl.PropagationFlags]::InheritOnly) -and
                $rule.IdentityReference.Value -notin @('S-1-5-18','S-1-5-32-544') -and
                ([int64]$rule.FileSystemRights -band $mask)) { throw 'Update storage is user mutable.' }
        }
        $Verified[$full] = $leaf
        }
        if ($full -eq [IO.Path]::GetPathRoot($full)) { break }
        $full = [IO.Path]::GetDirectoryName($full)
        $leaf = $false
    }
    if ($Recursive) {
        foreach ($child in Get-ChildItem -LiteralPath $Path -Force) {
            # Reject redirected children before enumerating their contents.
            Assert-Protected $child.FullName -Recursive:($child.PSIsContainer) -Verified $Verified
        }
    }
}
# Validate every executable and data dependency before loading another helper.
$trustedBootstrap = [IO.Path]::GetFullPath((Join-Path $PSScriptRoot '..'))
$trustedApplication = Split-Path -Parent $trustedBootstrap
Assert-Protected $trustedApplication
Assert-Protected $trustedBootstrap -Recursive
Assert-Protected $PSCommandPath
foreach ($directory in @('update-control','versions')) {
    $trustedDirectory = Join-Path $trustedApplication $directory
    if (Test-Path -LiteralPath $trustedDirectory) { Assert-Protected $trustedDirectory -Recursive:($Mode -cne 'Verify') }
}
$IntegrityHelper = Join-Path $PSScriptRoot 'bootstrap_integrity.ps1'
. $IntegrityHelper
$script:LabelStartupBootstrapLoaded=$true
# Initial code-tree trust precedes loading the helper. Its exact, read-only
# shared-ancestor gate must then pass before shared code or daily operations.
[void](Protect-KmtechCodeAncestors $trustedApplication -AdmissionOnly -RequireProtected)
$SharedLeaf = Get-LabelSharedPortableLeafPath $InstallRoot
. $SharedLeaf

function Enter-LabelStartupUpdateAuthority {
    $root = Get-StrictFullPath $InstallRoot 'startup update bootstrap'
    Assert-BootstrapNoReparsePoint $root 'startup update bootstrap'
    if (-not (Test-BootstrapSamePath $PSScriptRoot (Join-Path $root 'tools'))) {
        throw 'STARTUP_UPDATE_FIXED_BOOTSTRAP_REQUIRED'
    }
    [void](Assert-BootstrapIntegrityRecord $root)
    $sid = [Security.Principal.WindowsIdentity]::GetCurrent().User.Value
    if ($Mode -cnotin @('Observe','Verify') -and $sid -cne 'S-1-5-18') { throw 'STARTUP_UPDATE_SYSTEM_REQUIRED' }
    $policyPath = Join-Path (Split-Path -Parent $root) 'update-control\policy.json'
    Assert-BootstrapNoReparsePoint $policyPath 'startup update policy'
    if ((Get-Item -LiteralPath $policyPath).Length -gt 65536) { throw 'STARTUP_UPDATE_POLICY_OVERSIZED' }
    $policy = Get-Content -LiteralPath $policyPath -Raw -Encoding UTF8 | ConvertFrom-Json
    if ($Mode -cin @('Observe','Verify') -and $sid -cne 'S-1-5-18' -and $sid -cne [string]$policy.operator_sid) {
        throw 'STARTUP_UPDATE_OPERATOR_DIFFERS'
    }
    return [pscustomobject]@{ root=$root; policy=$policy }
}

function Get-StartupUpdateProcesses([string]$Root) {
    # Query only app-related rows; do not log other applications' command lines.
    if (-not ('KMTechStartupUpdateToken' -as [type])) {
        Add-Type -TypeDefinition @'
using System;
using System.Runtime.InteropServices;
public static class KMTechStartupUpdateToken {
 [DllImport("kernel32.dll", SetLastError=true)] static extern IntPtr OpenProcess(uint rights, bool inherit, uint pid);
 [DllImport("kernel32.dll")] static extern bool CloseHandle(IntPtr handle);
 [DllImport("advapi32.dll", SetLastError=true)] static extern bool OpenProcessToken(IntPtr process, uint rights, out IntPtr token);
 [DllImport("advapi32.dll", SetLastError=true)] static extern bool GetTokenInformation(IntPtr token, int kind, IntPtr data, int size, out int needed);
 [DllImport("advapi32.dll")] static extern IntPtr GetSidSubAuthorityCount(IntPtr sid);
 [DllImport("advapi32.dll")] static extern IntPtr GetSidSubAuthority(IntPtr sid, uint index);
 public static bool Ordinary(uint pid) {
  IntPtr process=OpenProcess(0x1000,false,pid), token=IntPtr.Zero, data=IntPtr.Zero;
  if(process==IntPtr.Zero) throw new Exception("process token cannot be observed");
  try {
   if(!OpenProcessToken(process,8,out token)) throw new Exception("process token cannot be observed");
   int needed; data=Marshal.AllocHGlobal(4);
   if(!GetTokenInformation(token,20,data,4,out needed)) throw new Exception("process elevation cannot be observed");
   if(Marshal.ReadInt32(data)!=0) return false;
   Marshal.FreeHGlobal(data); data=IntPtr.Zero;
   GetTokenInformation(token,25,IntPtr.Zero,0,out needed); data=Marshal.AllocHGlobal(needed);
   if(!GetTokenInformation(token,25,data,needed,out needed)) throw new Exception("process integrity cannot be observed");
   IntPtr sid=Marshal.ReadIntPtr(data); byte count=Marshal.ReadByte(GetSidSubAuthorityCount(sid));
   if(count==0) throw new Exception("process integrity is malformed");
   int rid=Marshal.ReadInt32(GetSidSubAuthority(sid,(uint)(count-1)));
   return rid>=0x2000 && rid<0x3000;
  } finally { if(data!=IntPtr.Zero) Marshal.FreeHGlobal(data); if(token!=IntPtr.Zero) CloseHandle(token); CloseHandle(process); }
 }
}
'@
    }
    $managed = Split-Path -Parent $Root
    $rows = @()
    foreach ($process in @(Get-CimInstance Win32_Process -ErrorAction Stop)) {
        $image = [string]$process.ExecutablePath
        if (-not $image) {
            if ([string]$process.Name -match '^(pythonw?|Label_Match)\.exe$') {
                $rows += @{pid=[int]$process.ProcessId;observable=$false;bootstrap=$false}
            }
            continue
        }
        $insideManaged = $image.StartsWith($managed + '\',[StringComparison]::OrdinalIgnoreCase)
        $bundle = Split-Path -Parent (Split-Path -Parent $image)
        $command = [string]$process.CommandLine
        if (-not $insideManaged) {
            if ([string]$process.Name -ceq 'Label_Match.exe' -or $command -match 'Label_Match\.py(?:"|\s|$)') {
                $rows += @{pid=[int]$process.ProcessId;observable=$false;bootstrap=$false}
                continue
            }
            if ([string]$process.Name -notmatch '^pythonw?\.exe$' -or (Split-Path -Leaf (Split-Path -Parent $image)) -ine 'runtime') { continue }
            $outsideManifest = Join-Path $bundle 'portable-manifest.json'
            if (-not (Test-Path -LiteralPath $outsideManifest -PathType Leaf)) { continue }
            try {
                Assert-BootstrapNoReparsePoint $outsideManifest 'possible prior packet manifest'
                if ((Get-Item -LiteralPath $outsideManifest).Length -gt 65536) { throw 'prior packet manifest oversized' }
                $outside = Get-Content -LiteralPath $outsideManifest -Raw -Encoding UTF8 | ConvertFrom-Json
                if ([string]$outside.schema -cne 'label-match-portable-tree-v1') { continue }
            } catch {
                $rows += @{pid=[int]$process.ProcessId;observable=$false;bootstrap=$false}
                continue
            }
        }
        $bootstrapPattern = '^\s*"?' + [regex]::Escape((Join-Path $Root 'runtime\pythonw.exe')) + '"?\s+-I\s+-B\s+"?' + [regex]::Escape((Join-Path $Root 'app\auto_update.py')) + '"?\s+--bootstrap(?:\s|$)'
        $bootstrap = $insideManaged -and ($command -match $bootstrapPattern)
        try {
            $owner = Invoke-CimMethod -InputObject $process -MethodName GetOwnerSid -ErrorAction Stop
            if ($owner.ReturnValue -ne 0 -or -not $owner.Sid) { throw 'owner is unobservable' }
            $manifest = Get-Content -LiteralPath (Join-Path $bundle 'portable-manifest.json') -Raw -Encoding UTF8 | ConvertFrom-Json
            $role = 'writer'
            if ($command -match '--label-match-user-relay(?:\s|$)') { $role='relay' }
            elseif ($command -match ([regex]::Escape((Join-Path $bundle 'app\main.py')) + '"?(?:\s|$)') -and $command -notmatch '--[a-z-]+') { $role='gui' }
            # A GUI's original data-root/geometry options are also supported.
            elseif ($command -match ([regex]::Escape((Join-Path $bundle 'app\main.py')) + '"?(?:\s|$)') -and $command -notmatch '--label-match-|--onboard-|--remove-|--restore-') { $role='gui' }
            $rows += @{
                pid=[int]$process.ProcessId;observable=$true;bootstrap=[bool]$bootstrap
                sha=[string]$manifest.source_commit;sid=[string]$owner.Sid;role=$role
                ordinary_user=[KMTechStartupUpdateToken]::Ordinary([uint32]$process.ProcessId)
                created_at=([datetime]$process.CreationDate).ToUniversalTime().ToString('o')
            }
        } catch { $rows += @{pid=[int]$process.ProcessId;observable=$false;bootstrap=$false} }
    }
    return ,$rows
}

function Assert-StartupUpdateVersion([string]$Root, [string]$Candidate) {
    $managed = Split-Path -Parent $Root
    $full = Get-StrictFullPath $Candidate 'startup update version'
    Assert-Protected $full -Recursive
    Assert-BootstrapNoReparsePoint $full 'startup update version'
    $versionPrefix = Join-Path $managed 'versions'
    $packetPrefix = Join-Path $managed 'update-control'
    if (-not (Test-SamePath $full $Root) -and -not $full.StartsWith($versionPrefix + '\',[StringComparison]::OrdinalIgnoreCase) -and
        -not $full.StartsWith($packetPrefix + '\packet-',[StringComparison]::OrdinalIgnoreCase)) {
        throw 'STARTUP_UPDATE_VERSION_OUTSIDE_PROTECTED_ROOT'
    }
    Assert-KmtechPortableTree $full @('portable-manifest.json','runtime\python.exe','runtime\pythonw.exe','app\main.py')
    $manifest = Read-KmtechPortableManifest $full
    if ([string]$manifest.schema -cne 'label-match-portable-tree-v1' -or
        $manifest.startup_update_protocol -ne 1 -or
        [string]$manifest.source_commit -cnotmatch '^[0-9a-f]{40}$' -or
        [string]$manifest.entrypoint -cne 'runtime/pythonw.exe app/main.py' -or
        (Get-FileSha256 (Join-Path $full 'runtime\pythonw.exe')) -cne [string]$manifest.runtime_pythonw_sha256 -or
        @($manifest.allowed_unsigned_app_pe).Count -ne 0 -or @($manifest.forbidden_package_roots).Count -ne 0) {
        throw 'STARTUP_UPDATE_PORTABLE_MANIFEST_DIFFERS'
    }
    $pins = @{
        'launch-label-match.cmd'='launcher_sha256'
        'INSTALL_CANONICAL_PORTABLE.ps1'='canonical_installer_sha256'
        'INSTALL_THIS_PC.ps1'='helper_sha256'
        'tools\bootstrap_integrity.ps1'='integrity_helper_sha256'
        'tools\label_writer_fence.ps1'='writer_fence_helper_sha256'
        'tools\label_writer_fence_contract.json'='writer_session_contract_sha256'
        'tools\apply_prepared_update.ps1'='startup_update_helper_sha256'
        'app\update-manifest-key-config.json'='update_key_config_sha256'
    }
    foreach ($path in $pins.Keys) {
        if ((Get-FileSha256 (Join-Path $full $path)) -cne [string]$manifest.($pins[$path])) { throw 'STARTUP_UPDATE_FILE_PIN_DIFFERS' }
    }
    $pinSource = Get-Content -LiteralPath (Join-Path $full 'app\writer_session_fence.py') -Raw -Encoding UTF8
    $pinsFound = [regex]::Matches($pinSource,'(?m)^WRITER_INVENTORY_SHA256 = "([0-9a-f]{64})"')
    if ($pinsFound.Count -ne 1 -or $pinsFound[0].Groups[1].Value -cne [string]$manifest.writer_sink_inventory_contract_sha256) { throw 'STARTUP_UPDATE_WRITER_INVENTORY_DIFFERS' }

    Assert-KmtechCPythonSignature $full $false
    if ($full.StartsWith($packetPrefix + '\packet-',[StringComparison]::OrdinalIgnoreCase)) {
        if (Test-Path -LiteralPath (Join-Path $full 'bootstrap-integrity.json')) { throw 'STARTUP_UPDATE_PACKET_HAS_INSTALL_RECORD' }
        $finalIdentity = Join-Path $versionPrefix ([string]$manifest.source_commit)
        [void](Write-BootstrapIntegrityRecord -Root $full -CodeRoot $finalIdentity)
    } elseif (-not (Test-SamePath $full $Root) -and (Split-Path -Leaf $full) -cne [string]$manifest.source_commit) {
        throw 'STARTUP_UPDATE_VERSION_DIRECTORY_DIFFERS'
    } elseif (Test-Path -LiteralPath (Join-Path $full 'bootstrap-integrity.json') -PathType Leaf) {
        [void](Assert-BootstrapIntegrityRecord $full)
    } else { throw 'STARTUP_UPDATE_PREVIOUS_RECORD_MISSING' }
}

$authority = Enter-LabelStartupUpdateAuthority
if ($Mode -ceq 'Authority') { exit 0 }
if ($Mode -ceq 'Observe') {
    $rows = Get-StartupUpdateProcesses $authority.root
    ConvertTo-Json -InputObject @($rows) -Depth 8 -Compress
    exit 0
}
if ($Mode -ceq 'Verify') {
    Assert-StartupUpdateVersion $authority.root $CodeRoot
    exit 0
}
if ($CodeRoot) { throw 'STARTUP_UPDATE_APPLY_HAS_NO_USER_CODE_ARGUMENT' }
$runtime = Join-Path $authority.root 'runtime\python.exe'
$implementation = Join-Path $authority.root 'app\auto_update.py'
& $runtime -I -B $implementation --apply-prepared --control-root (Join-Path (Split-Path -Parent $authority.root) 'update-control')
exit $LASTEXITCODE
