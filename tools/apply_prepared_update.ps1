[CmdletBinding()]
param(
    [Parameter(Mandatory=$true)][string]$InstallRoot,
    [ValidateSet('Apply','Observe','Verify','Authority','ObserveLifecycle')][string]$Mode = 'Apply',
    [string]$CodeRoot = '',
    [switch]$SupportedLifecycle
)
Import-Module -Name (Join-Path $PSHOME 'Modules\Microsoft.PowerShell.Utility\Microsoft.PowerShell.Utility.psd1') -ErrorAction Stop
Import-Module -Name (Join-Path $PSHOME 'Modules\Microsoft.PowerShell.Security\Microsoft.PowerShell.Security.psd1') -ErrorAction Stop
$ErrorActionPreference = 'Stop'
$script:LabelStartupVerifyDiagnostic = [ordered]@{check='ARGUMENTS'}
function Write-LabelStartupVerifyFailure($Failure) {
    # Early ACL failures precede loading any dependency. Record only typed
    # check evidence here; arbitrary exception text requires the trusted redactor.
    $details=[ordered]@{schema='label-match-startup-verify-failure-v1';mode=$Mode;
        exception_type=$Failure.Exception.GetType().FullName;hresult=$Failure.Exception.HResult;
        location_line=$Failure.InvocationInfo.ScriptLineNumber;check=$script:LabelStartupVerifyDiagnostic}
    if (Get-Command Write-KmtechProtectedFailureSidecar -ErrorAction SilentlyContinue) {
        $details.trace=$Failure.Exception.ToString()
        return Write-KmtechProtectedFailureSidecar -Text ($details | ConvertTo-Json -Depth 8)
    }
    # Same native sidecar ownership/readback as failure_redaction.ps1. Never
    # execute that dependency while its containing code tree is still untrusted.
    $root=[IO.Path]::GetFullPath([IO.Path]::GetTempPath())
    for($ancestor=$root;$ancestor;$ancestor=[IO.Path]::GetDirectoryName($ancestor.TrimEnd('\'))){
        if([IO.File]::GetAttributes($ancestor) -band [IO.FileAttributes]::ReparsePoint){throw 'Diagnostic parent is redirected.'}
    }
    $directory=Join-Path $root ('label-startup-verify-'+[Guid]::NewGuid().ToString('N'))
    [void](New-Item -ItemType Directory -Path $directory -ErrorAction Stop)
    $acl=[Security.AccessControl.DirectorySecurity]::new();$acl.SetAccessRuleProtection($true,$false)
    foreach($sid in @([Security.Principal.WindowsIdentity]::GetCurrent().User,
        [Security.Principal.SecurityIdentifier]::new('S-1-5-18'),[Security.Principal.SecurityIdentifier]::new('S-1-5-32-544'))){
        $acl.AddAccessRule([Security.AccessControl.FileSystemAccessRule]::new($sid,'FullControl','ContainerInherit,ObjectInherit','None','Allow'))
    }
    Set-Acl -LiteralPath $directory -AclObject $acl -ErrorAction Stop
    $path=Join-Path $directory 'native-exception.txt'
    $bytes=[Text.Encoding]::UTF8.GetBytes(($details | ConvertTo-Json -Depth 8))
    $stream=[IO.File]::Open($path,'CreateNew','Write','Read')
    try{$stream.Write($bytes,0,$bytes.Length);$stream.Flush($true)}finally{$stream.Dispose()}
    $readback=[IO.File]::ReadAllBytes($path)
    $hash=[Security.Cryptography.SHA256]::Create()
    try{
        $sha=([BitConverter]::ToString($hash.ComputeHash($readback))).Replace('-','').ToLowerInvariant()
        $expected=([BitConverter]::ToString($hash.ComputeHash($bytes))).Replace('-','').ToLowerInvariant()
    }finally{$hash.Dispose()}
    if($readback.Length -ne $bytes.Length -or $sha -cne $expected){throw 'Diagnostic readback differs.'}
    return [ordered]@{path=$path;sha256=$sha}
}
trap {
    $failure=$_
    $record=[ordered]@{status='FAILED';reason_code='INSTALLER_FAILED';report_status='REPORT_UNAVAILABLE'}
    try{
        $reference=Write-LabelStartupVerifyFailure $failure
        $record.Remove('report_status');$record.report_path=$reference.path;$record.report_sha256=$reference.sha256
    }catch{}
    if (Get-Command ConvertTo-KmtechPublicRecord -ErrorAction SilentlyContinue) {
        $fields=[ordered]@{status='FAILED';stage='SUPPORT';reason_code='INSTALLER_FAILED';exception_type=$failure.Exception.GetType().FullName;guidance=[string]$Script:KmtechRedactionRules.safe_messages[0]}
        foreach($key in @('report_path','report_sha256','report_status')){if($record.Contains($key)){$fields[$key]=$record[$key]}}
        $record=ConvertTo-KmtechPublicRecord $fields
    }
    $publicJson = ($record | ConvertTo-Json -Compress)
    $publicAscii = [regex]::Replace($publicJson, '[^\u0000-\u007f]', { param($unit) '\u{0:x4}' -f [int][char]$unit.Value })
    [Console]::Error.WriteLine('LABEL_MATCH_INSTALLER_FAILURE=' + $publicAscii)
    exit 1
}
$script:LabelStartupBootstrapLoaded=$false

function Test-KmtechPrivilegedOwnerSid([string]$Sid) {
    return $Sid -cin @('S-1-5-18','S-1-5-32-544','S-1-5-80-956008885-3418522649-1831038044-1853292631-2271478464')
}

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
        $expectedArguments = '-NoLogo -NoProfile -NonInteractive -WindowStyle Hidden -ExecutionPolicy Bypass -File "' + $helper + '" -InstallRoot "' + $full + '"'
        $previousArguments = $expectedArguments -creplace '^(-NoLogo -NoProfile -NonInteractive -WindowStyle Hidden) -ExecutionPolicy Bypass(?= -File ")','$1'
        if ([string]$task.TaskPath -cne '\' -or
            (ConvertTo-LabelTaskSid ([string]$task.Principal.UserId)) -cne 'S-1-5-18' -or
            [string]$task.Principal.LogonType -cnotin @('ServiceAccount','5') -or
            [string]$task.Principal.RunLevel -cnotin @('Highest','HighestAvailable','1') -or
            -not ([string]$actions[0].Execute).Equals($expectedCommand,[StringComparison]::OrdinalIgnoreCase) -or
            ([string]$actions[0].Arguments -cne $expectedArguments -and [string]$actions[0].Arguments -cne $previousArguments) -or
            -not [string]::IsNullOrEmpty([string]$actions[0].WorkingDirectory)) {
            throw 'SUPPORTED_TASK_ACTION_DIFFERS'
        }
        $owned += $task
    }
    return $owned
}
function Invoke-LabelStartupReadOnlyQuery {
    param(
        [ValidateSet('Observe','ObserveLifecycle')][string]$QueryMode,
        [string]$Root,
        [string]$OperatorSid = '',
        [scriptblock]$NewProcess = { [Diagnostics.Process]::new() }
    )
    # Only these already loaded query functions enter the disposable child.
    # Authority/inventory validation, Verify and its audit writes remain here.
    $names = if ($QueryMode -ceq 'Observe') {
        @('Assert-LabelStartupNoReparsePoint','Get-StartupUpdateProcesses')
    } else { @('ConvertTo-LabelTaskSid','Get-LabelSupportedStartupTasks') }
    $query = "`$ErrorActionPreference='Stop';`n"
    foreach ($name in $names) {
        $body = (Get-Item -LiteralPath ('Function:\' + $name) -ErrorAction Stop).Definition
        $query += 'function ' + $name + ' {' + $body + "}`n"
    }
    $rootLiteral = "'" + $Root.Replace("'", "''") + "'"
    $sidLiteral = "'" + $OperatorSid.Replace("'", "''") + "'"
    if ($QueryMode -ceq 'Observe') {
        $query += 'ConvertTo-Json -InputObject (Get-StartupUpdateProcesses ' + $rootLiteral + ' ' + $sidLiteral + ') -Depth 8 -Compress'
    } else {
        $query += '$tasks=@(Get-LabelSupportedStartupTasks ' + $rootLiteral + ');'
        $query += '$legacy=@(Get-ScheduledTask -ErrorAction Stop | Where-Object {[string]$_.TaskName -ieq ''direct-sync-relay-label-match-current-pc''});'
        $query += '@{task_exists=($tasks.Count -gt 0 -or $legacy.Count -gt 0)} | ConvertTo-Json -Compress'
    }
    $process = & $NewProcess
    $started = $false
    $owned = $false
    try {
        $process.StartInfo.FileName = Join-Path ([Environment]::SystemDirectory) 'WindowsPowerShell\v1.0\powershell.exe'
        $process.StartInfo.Arguments = '-NoLogo -NoProfile -NonInteractive -WindowStyle Hidden -EncodedCommand ' + [Convert]::ToBase64String([Text.Encoding]::Unicode.GetBytes($query))
        $process.StartInfo.UseShellExecute = $false
        $process.StartInfo.CreateNoWindow = $true
        $process.StartInfo.RedirectStandardOutput = $true
        $process.StartInfo.RedirectStandardError = $true
        if (-not $process.Start()) { throw 'STARTUP_QUERY_OWNERSHIP_UNKNOWN' }
        $started = $true
        # Pin the process handle before recording its PID and birth generation;
        # never reopen a PID or terminate a product/other process by name.
        $handle = $process.Handle
        $processId = $process.Id
        $created = $process.StartTime.ToUniversalTime().Ticks
        if ($processId -le 0 -or $handle -eq [IntPtr]::Zero -or $created -le 0) { throw 'STARTUP_QUERY_OWNERSHIP_UNKNOWN' }
        $owned = $true
        $output = $process.StandardOutput.ReadToEndAsync()
        $errors = $process.StandardError.ReadToEndAsync()
        # Reuse the official app-stop snapshot's maximum ten-second query wait.
        $clock = [Diagnostics.Stopwatch]::StartNew()
        if (-not $process.WaitForExit(10000)) {
            throw 'STARTUP_QUERY_TIMEOUT_UNKNOWN'
        }
        if ($process.ExitCode -ne 0) { throw 'STARTUP_QUERY_FAILED_UNKNOWN' }
        if (-not $output.Wait([Math]::Max(0, 10000 - [int]$clock.ElapsedMilliseconds))) {
            throw 'STARTUP_QUERY_INCOMPLETE_UNKNOWN'
        }
        $text = $output.GetAwaiter().GetResult()
        if ([string]::IsNullOrWhiteSpace($text)) { throw 'STARTUP_QUERY_INCOMPLETE_UNKNOWN' }
        return $text.Trim()
    } finally {
        if ($started -and -not $process.HasExited) {
            if (-not $owned -or $process.Id -ne $processId -or $process.StartTime.ToUniversalTime().Ticks -ne $created -or $process.Handle -ne $handle) {
                throw 'STARTUP_QUERY_OWNERSHIP_UNKNOWN'
            }
            $process.Kill()
            # Do not let the supervisor disappear while its query is alive.
            # OS termination itself has no claimed hard wall-clock bound.
            $process.WaitForExit()
        }
        $process.Dispose()
    }
}

if ($SupportedLifecycle -and $Mode -cne 'ObserveLifecycle') { throw 'SUPPORTED_OBSERVATION_ONLY' }
if ($Mode -ceq 'ObserveLifecycle') {
    if (-not $SupportedLifecycle -or $CodeRoot -or
        -not ([IO.Path]::GetFullPath($InstallRoot)).Equals('C:\KMTech\Apps\Label_Match\current',[StringComparison]::OrdinalIgnoreCase)) {
        throw 'SUPPORTED_OBSERVATION_ROOT_DIFFERS'
    }
    Invoke-LabelStartupReadOnlyQuery -QueryMode ObserveLifecycle -Root $InstallRoot
    exit 0
}
function Assert-Protected([string]$Path, [switch]$Recursive, [hashtable]$Verified = $null) {
    if ($null -eq $Verified) { $Verified = @{} }
    $full = [IO.Path]::GetFullPath($Path)
    if($full -ne [IO.Path]::GetPathRoot($full)){$full=$full.TrimEnd([char[]]'\/')}
    $leaf = $true
    while ($full) {
        if (-not $Verified.ContainsKey($full) -or ($leaf -and -not $Verified[$full])) {
        $script:LabelStartupVerifyDiagnostic=[ordered]@{check='ACL';leaf=$leaf;observation='PENDING'}
        $entry = Get-Item -Force -LiteralPath $full -ErrorAction Stop
        if ($entry.Attributes -band [IO.FileAttributes]::ReparsePoint) { throw 'Reparse point in update storage.' }
        $acl = Get-Acl -LiteralPath $full -ErrorAction Stop
        $owner = $acl.GetOwner([Security.Principal.SecurityIdentifier]).Value
        $pathHash=[Security.Cryptography.SHA256]::Create()
        try{$pathIdentity=([BitConverter]::ToString($pathHash.ComputeHash([Text.Encoding]::UTF8.GetBytes($full)))).Replace('-','').ToLowerInvariant()}
        finally{$pathHash.Dispose()}
        $script:LabelStartupVerifyDiagnostic=[ordered]@{check='ACL';path_sha256=$pathIdentity;leaf=$leaf;
            owner_sid=$owner;inheritance_protected=$acl.AreAccessRulesProtected;expected_owner='SYSTEM_ADMINISTRATORS_TRUSTED_INSTALLER'}
        if (-not (Test-KmtechPrivilegedOwnerSid $owner)) { throw 'Update storage owner is not privileged.' }
        if($script:LabelStartupBootstrapLoaded -and $full -in @('C:\KMTech','C:\KMTech\Apps')){
            [void](Protect-KmtechCodeAncestors 'C:\KMTech\Apps\Label_Match' -AdmissionOnly -AncestorPath $full -RequireProtected)
            $Verified[$full]=$leaf
            $full=[IO.Path]::GetDirectoryName($full);$leaf=$false;continue
        }
        $mask = if ($leaf) { 0x500D0156 } else { 0x500D0150 }
        $script:LabelStartupVerifyDiagnostic.forbidden_mask=$mask
        foreach ($rule in $acl.GetAccessRules($true,$true,[Security.Principal.SecurityIdentifier])) {
            $script:LabelStartupVerifyDiagnostic.rule=[ordered]@{sid=$rule.IdentityReference.Value;
                rights=[int64]$rule.FileSystemRights;access_type=[string]$rule.AccessControlType;
                inheritance_flags=[int]$rule.InheritanceFlags;propagation_flags=[int]$rule.PropagationFlags;inherited=$rule.IsInherited}
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
# The ACL gate has admitted the complete dependency tree. Preserve native
# exceptions from the subsequent integrity/manifest/version checks privately.
. (Join-Path $trustedBootstrap 'app\tools\failure_redaction.ps1')

function Get-LabelStartupStrictFullPath([string]$Path,[string]$Purpose) {
    $full=Get-BootstrapStrictFullPath $Path $Purpose
    if ([string]::IsNullOrWhiteSpace($full) -or $full -eq [IO.Path]::GetPathRoot($full)) {
        throw "$Purpose must not be a filesystem root."
    }
    return $full
}

function Test-LabelStartupSamePath([string]$Left,[string]$Right) {
    try {
        $leftFull=Get-LabelStartupStrictFullPath $Left 'left path'
        $rightFull=Get-LabelStartupStrictFullPath $Right 'right path'
        return $leftFull.Equals($rightFull,[StringComparison]::OrdinalIgnoreCase)
    } catch { return $false }
}

function Assert-LabelStartupNoReparsePoint([string]$Path,[string]$Purpose) {
    for($item=[IO.Path]::GetFullPath($Path);$item;$item=[IO.Path]::GetDirectoryName($item)) {
        if((Test-Path -LiteralPath $item) -and
            ((Get-Item -Force -LiteralPath $item -ErrorAction Stop).Attributes -band [IO.FileAttributes]::ReparsePoint)) {
            throw "$Purpose contains a reparse point."
        }
    }
}

function Assert-LabelStartupIntegrity([string]$Root) {
    $script:LabelStartupVerifyDiagnostic=[ordered]@{check='BOOTSTRAP_INTEGRITY';root=$Root}
    try { return Assert-BootstrapIntegrityRecord $Root }
    catch {
        $failure=$_
        # Keep the validator's original exception. These values are a separate
        # bounded readback after failure, never a substitute acceptance check.
        $details=[ordered]@{check='BOOTSTRAP_INTEGRITY';observation='POST_FAILURE_READBACK';root=$Root;
            expected_schema=$BootstrapIntegritySchema;expected_status='PASS';record_present=$false}
        $script:LabelStartupVerifyDiagnostic=$details
        try {
            $path=Join-Path $Root 'bootstrap-integrity.json'
            $details.record_present=Test-Path -LiteralPath $path -PathType Leaf
            if($details.record_present){
                $details.record_bytes=(Get-Item -LiteralPath $path).Length
                $details.diagnostic_read_limit_bytes=1048576
                if($details.record_bytes -le $details.diagnostic_read_limit_bytes){
                    $record=Get-Content -LiteralPath $path -Raw -Encoding UTF8|ConvertFrom-Json
                    $details.actual_schema=[string]$record.schema_version;$details.actual_status=[string]$record.status
                    $details.declared_code_root=[string]$record.code_root
                    $details.expected_file_count=$record.file_count;$details.recorded_rows=@($record.files).Count
                    $inventory=@(Get-BootstrapCodeInventory -Root $Root)
                    $details.actual_file_count=$inventory.Count
                    $details.expected_aggregate_sha256=[string]$record.aggregate_sha256
                    $details.actual_aggregate_sha256=Get-BootstrapInventoryAggregate -Inventory $inventory
                    if($failure.Exception.Message -match 'index ([0-9]+)'){
                        $index=[int]$Matches[1];$details.failed_index=$index
                        if($index -lt @($record.files).Count){$expected=@($record.files)[$index];$details.expected_file=@{path=[string]$expected.path;bytes=$expected.size;sha256=[string]$expected.sha256}}
                        if($index -lt $inventory.Count){$actual=$inventory[$index];$details.actual_file=@{path=[string]$actual.path;bytes=$actual.size;sha256=[string]$actual.sha256}}
                    }
                }
            }
        } catch {$details.readback_exception_type=$_.Exception.GetType().FullName}
        throw $failure
    }
}

function Enter-LabelStartupUpdateAuthority {
    $script:LabelStartupVerifyDiagnostic=[ordered]@{check='AUTHORITY_PATH';expected_helper_directory='tools'}
    $root = Get-LabelStartupStrictFullPath $InstallRoot 'startup update bootstrap'
    Assert-LabelStartupNoReparsePoint $root 'startup update bootstrap'
    if (-not (Test-LabelStartupSamePath $PSScriptRoot (Join-Path $root 'tools'))) {
        throw 'STARTUP_UPDATE_FIXED_BOOTSTRAP_REQUIRED'
    }
    [void](Assert-LabelStartupIntegrity $root)
    $sid = [Security.Principal.WindowsIdentity]::GetCurrent().User.Value
    if ($Mode -cnotin @('Observe','Verify') -and $sid -cne 'S-1-5-18') { throw 'STARTUP_UPDATE_SYSTEM_REQUIRED' }
    $policyPath = Join-Path (Split-Path -Parent $root) 'update-control\policy.json'
    $script:LabelStartupVerifyDiagnostic=[ordered]@{check='STARTUP_POLICY';mode=$Mode;caller_sid=$sid;maximum_bytes=65536}
    Assert-LabelStartupNoReparsePoint $policyPath 'startup update policy'
    $script:LabelStartupVerifyDiagnostic.actual_bytes=(Get-Item -LiteralPath $policyPath).Length
    if ($script:LabelStartupVerifyDiagnostic.actual_bytes -gt 65536) { throw 'STARTUP_UPDATE_POLICY_OVERSIZED' }
    $policy = Get-Content -LiteralPath $policyPath -Raw -Encoding UTF8 | ConvertFrom-Json
    $script:LabelStartupVerifyDiagnostic.expected_operator_sid=[string]$policy.operator_sid
    if ($Mode -cin @('Observe','Verify') -and $sid -cne 'S-1-5-18' -and $sid -cne [string]$policy.operator_sid) {
        throw 'STARTUP_UPDATE_OPERATOR_DIFFERS'
    }
    return [pscustomobject]@{ root=$root; policy=$policy }
}

function Get-StartupUpdateProcesses([string]$Root, [string]$OperatorSid = '') {
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
        if (-not $image -and [string]$process.Name -match '^(pythonw?|Label_Match)\.exe$') {
            # A terminating process may disappear between the list and path
            # read. Retry only this identity, briefly; never wait indefinitely.
            for ($attempt = 0; $attempt -lt 3 -and -not $image; $attempt++) {
                if ($attempt -gt 0) { Start-Sleep -Milliseconds 100 }
                $again = @(Get-CimInstance Win32_Process -Filter ("ProcessId=" + [int]$process.ProcessId) -ErrorAction Stop)
                if ($again.Count -eq 0) { $process = $null; break }
                $process = $again[0]
                $image = [string]$process.ExecutablePath
            }
            if ($null -eq $process) { continue }
            if (-not $image -and $OperatorSid) {
                try {
                    $owner = Invoke-CimMethod -InputObject $process -MethodName GetOwnerSid -ErrorAction Stop
                    if ($owner.ReturnValue -eq 0 -and $owner.Sid -and [string]$owner.Sid -cne $OperatorSid) { continue }
                } catch { } # Ambiguous ownership remains fail closed below.
            }
        }
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
                Assert-LabelStartupNoReparsePoint $outsideManifest 'possible prior packet manifest'
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
        if ($bootstrap) { continue }
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
    $script:LabelStartupVerifyDiagnostic=[ordered]@{check='VERSION_PATH';observation='PENDING'}
    $full = Get-LabelStartupStrictFullPath $Candidate 'startup update version'
    Assert-Protected $full -Recursive
    Assert-LabelStartupNoReparsePoint $full 'startup update version'
    $versionPrefix = Join-Path $managed 'versions'
    $packetPrefix = Join-Path $managed 'update-control'
    $script:LabelStartupVerifyDiagnostic=[ordered]@{check='VERSION_PATH';candidate=$full;bootstrap=$Root;version_prefix=$versionPrefix;packet_prefix=$packetPrefix}
    if (-not (Test-LabelStartupSamePath $full $Root) -and -not $full.StartsWith($versionPrefix + '\',[StringComparison]::OrdinalIgnoreCase) -and
        -not $full.StartsWith($packetPrefix + '\packet-',[StringComparison]::OrdinalIgnoreCase)) {
        throw 'STARTUP_UPDATE_VERSION_OUTSIDE_PROTECTED_ROOT'
    }
    Assert-KmtechPortableTree $full @('portable-manifest.json','runtime\python.exe','runtime\pythonw.exe','app\main.py')
    $script:LabelStartupVerifyDiagnostic=[ordered]@{check='MANIFEST';observation='READ_PENDING'}
    $manifest = Read-KmtechPortableManifest $full
    $script:LabelStartupVerifyDiagnostic=[ordered]@{check='MANIFEST_RUNTIME_PIN';relative_path='runtime/pythonw.exe';expected_sha256=[string]$manifest.runtime_pythonw_sha256;observation='READ_PENDING'}
    $runtimePythonwSha256=Get-BootstrapFileSha256 (Join-Path $full 'runtime\pythonw.exe')
    # These fixed pins are checked against the builder and both installers by
    # the release gate. Older zero-PE packets retain their empty declaration.
    $unsignedPe = @{
        'site-packages/_cffi_backend.cp312-win_amd64.pyd' = '9af5e4f68a41f26b726f2e9424f9d58a4f5666189f6a48be95f406b257102aec'
        'site-packages/clr_loader/ffi/dlls/amd64/ClrLoader.dll' = '07f62bcb1b70320f221ffb2cfc5ce6133815195f3e7cdb8d833222506188c2ac'
        'site-packages/pythonnet/runtime/Python.Runtime.dll' = '2ebd4492e28442ef1f1af587afe5b3a090c2ebfa220758ded4f1450f2a27f13a'
    }
    $listedPe = @(@($manifest.allowed_unsigned_app_pe) | ForEach-Object { [string]$_ })
    $unsignedPeOk = $listedPe.Count -eq 0 -or (
        $listedPe.Count -eq $unsignedPe.Count -and
        @($listedPe | Sort-Object -Unique -CaseSensitive).Count -eq $unsignedPe.Count -and
        @($listedPe | Where-Object {
            $_ -cin @($unsignedPe.Keys) -and
            (Test-Path -LiteralPath (Join-Path (Join-Path $full 'app') $_.Replace('/', '\')) -PathType Leaf) -and
            (Get-BootstrapFileSha256 (Join-Path (Join-Path $full 'app') $_.Replace('/', '\'))) -ceq $unsignedPe[$_]
        }).Count -eq $unsignedPe.Count
    )
    $script:LabelStartupVerifyDiagnostic=[ordered]@{check='MANIFEST';expected_schema='label-match-portable-tree-v1';
        actual_schema=[string]$manifest.schema;schema_matches=([string]$manifest.schema -ceq 'label-match-portable-tree-v1');expected_protocol=1;protocol=$manifest.startup_update_protocol;
        commit_valid=([string]$manifest.source_commit -cmatch '^[0-9a-f]{40}$');
        entrypoint_matches=([string]$manifest.entrypoint -ceq 'runtime/pythonw.exe app/main.py');
        expected_runtime_pythonw_sha256=[string]$manifest.runtime_pythonw_sha256;actual_runtime_pythonw_sha256=$runtimePythonwSha256;
        allowed_unsigned_count=@($manifest.allowed_unsigned_app_pe).Count;forbidden_package_count=@($manifest.forbidden_package_roots).Count}
    if ([string]$manifest.schema -cne 'label-match-portable-tree-v1' -or
        $manifest.startup_update_protocol -ne 1 -or
        [string]$manifest.source_commit -cnotmatch '^[0-9a-f]{40}$' -or
        [string]$manifest.entrypoint -cne 'runtime/pythonw.exe app/main.py' -or
        $runtimePythonwSha256 -cne [string]$manifest.runtime_pythonw_sha256 -or
        -not $unsignedPeOk -or @($manifest.forbidden_package_roots).Count -ne 0) {
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
        $script:LabelStartupVerifyDiagnostic=[ordered]@{check='MANIFEST_FILE_PIN';relative_path=$path;expected_sha256=[string]$manifest.($pins[$path]);observation='READ_PENDING'}
        $actual=Get-BootstrapFileSha256 (Join-Path $full $path)
        $script:LabelStartupVerifyDiagnostic=[ordered]@{check='MANIFEST_FILE_PIN';relative_path=$path;
            expected_sha256=[string]$manifest.($pins[$path]);actual_sha256=$actual}
        if ($actual -cne [string]$manifest.($pins[$path])) { throw 'STARTUP_UPDATE_FILE_PIN_DIFFERS' }
    }
    $pinSource = Get-Content -LiteralPath (Join-Path $full 'app\writer_session_fence.py') -Raw -Encoding UTF8
    $pinsFound = [regex]::Matches($pinSource,'(?m)^WRITER_INVENTORY_SHA256 = "([0-9a-f]{64})"')
    $script:LabelStartupVerifyDiagnostic=[ordered]@{check='MANIFEST_WRITER_INVENTORY';declaration_count=$pinsFound.Count;
        expected_sha256=[string]$manifest.writer_sink_inventory_contract_sha256;
        actual_sha256=if($pinsFound.Count -eq 1){$pinsFound[0].Groups[1].Value}else{''}}
    if ($pinsFound.Count -ne 1 -or $pinsFound[0].Groups[1].Value -cne [string]$manifest.writer_sink_inventory_contract_sha256) { throw 'STARTUP_UPDATE_WRITER_INVENTORY_DIFFERS' }

    $script:LabelStartupVerifyDiagnostic=[ordered]@{check='RUNTIME_SIGNATURE';target='runtime/pythonw.exe'}
    Assert-KmtechCPythonSignature $full $false
    $script:LabelStartupVerifyDiagnostic=[ordered]@{check='VERSION';is_bootstrap=(Test-LabelStartupSamePath $full $Root);
        directory_matches=((Split-Path -Leaf $full) -ceq [string]$manifest.source_commit);
        integrity_record_present=(Test-Path -LiteralPath (Join-Path $full 'bootstrap-integrity.json') -PathType Leaf)}
    if ($full.StartsWith($packetPrefix + '\packet-',[StringComparison]::OrdinalIgnoreCase)) {
        if (Test-Path -LiteralPath (Join-Path $full 'bootstrap-integrity.json')) { throw 'STARTUP_UPDATE_PACKET_HAS_INSTALL_RECORD' }
        $finalIdentity = Join-Path $versionPrefix ([string]$manifest.source_commit)
        [void](Write-BootstrapIntegrityRecord -Root $full -CodeRoot $finalIdentity)
    } elseif (-not (Test-LabelStartupSamePath $full $Root) -and (Split-Path -Leaf $full) -cne [string]$manifest.source_commit) {
        throw 'STARTUP_UPDATE_VERSION_DIRECTORY_DIFFERS'
    } elseif (Test-Path -LiteralPath (Join-Path $full 'bootstrap-integrity.json') -PathType Leaf) {
        [void](Assert-LabelStartupIntegrity $full)
    } else { throw 'STARTUP_UPDATE_PREVIOUS_RECORD_MISSING' }
}

$authority = Enter-LabelStartupUpdateAuthority
if ($Mode -ceq 'Authority') { exit 0 }
if ($Mode -ceq 'Observe') {
    Invoke-LabelStartupReadOnlyQuery -QueryMode Observe -Root $authority.root -OperatorSid ([string]$authority.policy.operator_sid)
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
