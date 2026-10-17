param([string]$Repo,[string]$Case,[string]$Root,[string[]]$KitArguments)
$ErrorActionPreference='Stop'
Set-StrictMode -Version Latest
trap {
    [IO.File]::WriteAllText((Join-Path $Root 'l2-result.json'),(@{case=$Case;error_type=$_.Exception.GetType().FullName;error_id=$_.FullyQualifiedErrorId;line=$_.InvocationInfo.ScriptLineNumber}|ConvertTo-Json))
    throw 'L2_CASE_FAILED'
}
$tokens=$null;$errors=$null
$ast=[Management.Automation.Language.Parser]::ParseFile((Join-Path $Repo 'INSTALL_THIS_PC.ps1'),[ref]$tokens,[ref]$errors)
if($errors.Count){throw 'SOURCE_PARSE_FAILED'}
foreach($function in $ast.FindAll({param($node) $node -is [Management.Automation.Language.FunctionDefinitionAst]},$false)){
    . ([scriptblock]::Create($function.Extent.Text))
}
$application=Join-Path $Root 'application'
$current=Join-Path $application 'current'
$profile=Join-Path $Root 'worker-profile'
$sid='S-1-5-21-'+('111-222-333-1001')
$otherSid='S-1-5-21-'+('111-222-333-1002')
$script:LocalUserFixture=[Security.Principal.SecurityIdentifier]::new($otherSid)
if($Case -eq 'domain'){$sid='S-1-5-21-'+('999-888-777-1001')}
$script:WorkerSidFixture=$sid
$script:OtherSidFixture=$otherSid
$script:WorkerProfileRoot=$profile
$script:WorkerAccount=''
$script:SourceRoot=$Repo
$script:InstallerOwnerProcessId=0
$script:InstallerOwnerProcessStartedAtFileTime=''
$script:BootstrapIntegrityPreloaded=[Management.Automation.SwitchParameter]$false
$script:WriterFenceFunctionsPreloaded=[Management.Automation.SwitchParameter]$false
function Assert-ProtectedCodeAncestors([string]$Path) {}

if($Case -in @('refresh_blank_birth','refresh_blank_parent','same_birth','broken_top','newer_top','blank_birth','blank_parent','above_unrecognized','syswow64','cwd_only','ancestor','outsider','runtime_ancestor','wrong_engine','reused_pid','other_argument','peer_root','normalized_root','canonical_owner','canonical_packet_differs','canonical_unknown_argument','parent_ended','four_shell_chain')){
    $birth=[datetime]::UtcNow.AddMinutes(-1)
    $engine=Join-Path ([Environment]::SystemDirectory) 'WindowsPowerShell\v1.0\powershell.exe'
    $parentId=($PID+10000)
    $parentCommand='powershell -NoProfile -Command "& packet\INSTALL_THIS_PC.ps1 -Uninstall -InstallRoot '''+$current+'''"'
    $parentEngine=$engine
    if($Case -eq 'runtime_ancestor'){$parentEngine=Join-Path $current 'runtime\python.exe'}
    if($Case -eq 'wrong_engine'){$parentEngine=Join-Path $Root 'powershell.exe'}
    if($Case -eq 'other_argument'){$parentCommand+=' -Other "'+(Join-Path $current 'app\main.py')+'"'}
    if($Case -eq 'peer_root'){$parentCommand='powershell -Command "& '''+($application+'-peer\app\main.py')+'''"'}
    if($Case -eq 'normalized_root'){$parentCommand+=' -Other "'+(Join-Path $current '..\current\app\main.py').Replace('\','/')+'"'}
    if($Case -in @('canonical_owner','canonical_packet_differs','canonical_unknown_argument')){
        $script:SourceRoot=Join-Path $Root 'packet'
        [void][IO.Directory]::CreateDirectory($script:SourceRoot)
        $canonical=Join-Path $script:SourceRoot 'INSTALL_CANONICAL_PORTABLE.ps1'
        [IO.File]::WriteAllText($canonical,'param([string]$EvidencePath)')
        $hash=(Get-FileHash -LiteralPath $canonical -Algorithm SHA256).Hash.ToLowerInvariant()
        if($Case -eq 'canonical_packet_differs'){$hash='0'*64}
        [IO.File]::WriteAllText((Join-Path $script:SourceRoot 'portable-manifest.json'),(@{canonical_installer_sha256=$hash}|ConvertTo-Json))
        $script:BootstrapIntegrityPreloaded=[Management.Automation.SwitchParameter]$true
        $script:WriterFenceFunctionsPreloaded=[Management.Automation.SwitchParameter]$true
        $script:InstallerOwnerProcessId=$parentId
        $script:InstallerOwnerProcessStartedAtFileTime=$birth.ToFileTimeUtc().ToString()
        $parentCommand='powershell -File "'+$canonical+'" -EvidencePath "'+(Join-Path $current 'audit.json')+'"'
        if($Case -eq 'canonical_unknown_argument'){$parentCommand+=' -Unknown value'}
        function Get-BootstrapFileSha256([string]$Path){return (Get-FileHash -LiteralPath $Path -Algorithm SHA256).Hash.ToLowerInvariant()}
    }
    $script:Rows=@(
        [pscustomobject]@{ProcessId=$PID;ParentProcessId=$parentId;CreationDate=$birth.AddSeconds(10);ExecutablePath=$engine;CommandLine='powershell -File packet\INSTALL_THIS_PC.ps1'},
        [pscustomobject]@{ProcessId=$parentId;ParentProcessId=0;CreationDate=$birth;ExecutablePath=$parentEngine;CommandLine=$parentCommand}
    )
    if($Case -eq 'outsider'){$script:Rows += [pscustomobject]@{ProcessId=($PID+20000);ParentProcessId=0;CreationDate=$birth;ExecutablePath=$engine;CommandLine='powershell -File "'+(Join-Path $current 'app\main.py')+'"'}}
    if($Case -eq 'four_shell_chain'){
        $script:Rows[1].ParentProcessId=$parentId+1
        $script:Rows += [pscustomobject]@{ProcessId=($parentId+1);ParentProcessId=($parentId+2);CreationDate=$birth.AddSeconds(-1);ExecutablePath=(Join-Path ([Environment]::SystemDirectory) 'cmd.exe');CommandLine='cmd /c powershell -File packet\INSTALL_THIS_PC.ps1 -Uninstall -InstallRoot "'+$current+'"'}
        $script:Rows += [pscustomobject]@{ProcessId=($parentId+2);ParentProcessId=0;CreationDate=$birth.AddSeconds(-2);ExecutablePath=(Join-Path ([Environment]::SystemDirectory) 'OpenSSH\sshd.exe');CommandLine='sshd -D'}
    }
    if($Case -in @('broken_top','newer_top','above_unrecognized')){
        $script:Rows[1].ParentProcessId=$parentId+1
        if($Case -eq 'newer_top'){
            $script:Rows += [pscustomobject]@{ProcessId=($parentId+1);ParentProcessId=0;CreationDate=$birth.AddSeconds(5);ExecutablePath=$engine;CommandLine='powershell.exe'}
        }
        if($Case -eq 'above_unrecognized'){
            $script:Rows += [pscustomobject]@{ProcessId=($parentId+1);ParentProcessId=($parentId+2);CreationDate=$birth.AddSeconds(-1);ExecutablePath=(Join-Path $Root 'explorer.exe');CommandLine='explorer.exe'}
            $script:Rows += [pscustomobject]@{ProcessId=($parentId+2);ParentProcessId=0;CreationDate=$birth.AddSeconds(-2);ExecutablePath=$engine;CommandLine='powershell.exe -File "'+(Join-Path $current 'held.ps1')+'"'}
        }
    }
    if($Case -eq 'same_birth'){$script:Rows[1].CreationDate=$script:Rows[0].CreationDate}
    if($Case -eq 'blank_birth'){$script:Rows[1].CreationDate=$null}
    if($Case -eq 'blank_parent'){$script:Rows[1].ParentProcessId=$null}
    if($Case -eq 'syswow64'){$script:Rows[1].ExecutablePath=Join-Path (Split-Path ([Environment]::SystemDirectory) -Parent) 'SysWOW64\WindowsPowerShell\v1.0\powershell.exe'}
    if($Case -eq 'cwd_only'){$script:Rows[1].CommandLine='powershell.exe';$script:Rows[1] | Add-Member NoteProperty WorkingDirectory $current}
    function Get-CimInstance {param($ClassName,$Filter,$ErrorAction)
        if($Filter){
            $id=[int]($Filter -replace '[^0-9]','')
            if($Case -eq 'parent_ended' -and $id -eq $parentId){return @()}
            if($Case -in @('refresh_blank_birth','refresh_blank_parent') -and $id -eq $parentId){
                $copy=@($script:Rows | Where-Object ProcessId -eq $id)[0].PSObject.Copy()
                if($Case -eq 'refresh_blank_birth'){$copy.CreationDate=$null}else{$copy.ParentProcessId=''}
                return $copy
            }
            return @($script:Rows | Where-Object {$_.ProcessId -eq $id})
        }
        return $script:Rows
    }
    function Get-LabelRemovalNativeProcessStartTime([int]$ProcessId){
        $row=@($script:Rows | Where-Object {$_.ProcessId -eq $ProcessId})[0]
        if($Case -eq 'reused_pid' -and $ProcessId -eq $parentId){return $row.CreationDate.AddSeconds(1)}
        return $row.CreationDate
    }
    $rejected=$false
    try { Assert-LabelRemovalQuiescent $application }
    catch {if($Case -in @('refresh_blank_birth','refresh_blank_parent','blank_birth','blank_parent')){if($_.Exception.Message -cne 'REMOVAL_PROCESS_OBSERVATION_UNKNOWN'){throw}}elseif($_.Exception.Message -cne 'REMOVAL_REQUIRES_PRODUCT_PROCESS_STOP'){throw};$rejected=$true}
    $expectedReject=$Case -notin @('broken_top','newer_top','cwd_only','ancestor','peer_root','canonical_owner','four_shell_chain')
    if($rejected -ne $expectedReject){throw 'PROCESS_ADMISSION_DIFFERS'}
} else {
    [void][IO.Directory]::CreateDirectory((Join-Path $application 'update-control'))
    [void][IO.Directory]::CreateDirectory($current)
    [void][IO.Directory]::CreateDirectory($profile)
    $marker=Join-Path $application 'update-control\worker-pending.json'
    $markerValue=@{schema='label-match-machine-worker-pending-v1';target_sid=$sid;install_id='20261017T000000000Z-'+('a'*32);source_commit=('b'*40);version='2.0.110';inventory_sha256=('c'*64)}
    [IO.File]::WriteAllText($marker,($markerValue|ConvertTo-Json))
    $script:Hive=@{loaded=$true;completed=$false;run_present=$false}
    $script:Account='ENABLED'
    $script:ProfileExists=$true
    if($Case -eq 'hive_unloaded'){$script:Hive.loaded=$false}
    if($Case -eq 'completed'){$script:Hive.completed=$true}
    if($Case -eq 'pending_run'){$script:Hive.run_present=$true}
    if($Case -eq 'pending_run_lower'){$script:Hive.run_present=$true}
    if($Case -eq 'completed_lower'){$script:Hive.completed=$true}
    if($Case -in @('large_receipt','receipt','wrong_sid','wrong_status','wrong_root','old_receipt','receipt_reparse','receipt_hidden','fallback_receipt','array_sid','array_status','array_root')){$script:Hive.completed=$true}
    if($Case -eq 'disabled'){$script:Hive.completed=$true;$script:Hive.run_present=$true;$script:Account='DISABLED'}
    if($Case -eq 'deleted'){$script:Hive.loaded=$false;$script:Account='ABSENT'}
    if($Case -eq 'domain'){$script:Hive.loaded=$false;$script:Account='DOMAIN'}
    if($Case -eq 'lookup_error'){$script:Hive.loaded=$false;$script:Account='ERROR'}
    if($Case -in @('locked','expired')){$script:Hive.loaded=$false}
    if($Case -eq 'unmapped'){$script:Hive.loaded=$false;$script:Account='UNMAPPED'}
    if($Case -eq 'profile_absent'){$script:ProfileExists=$false}
    if($Case -in @('large_receipt','missing_receipt','entry_missing_receipt')){$script:Hive.completed=$true}
    if($Case -in @('fallback_receipt','missing_binding','fallback_no_profile')){[IO.File]::Delete($marker)}
    if($Case -in @('fallback_receipt','fallback_no_profile')){$script:WorkerAccount='fixture-worker'}
    if($Case -eq 'fallback_no_profile'){$script:ProfileExists=$false}
    if($Case -eq 'binding_differs'){$script:WorkerAccount='different-worker'}
    function Resolve-LabelRemovalAccountSid([string]$Account){if($Account -eq 'different-worker'){return $script:OtherSidFixture};return $script:WorkerSidFixture}
    function New-L2RegistryKey([string[]]$Names=@()){
        $key=[pscustomobject]@{Names=$Names}
        $key | Add-Member ScriptMethod GetValueNames {return $this.Names}
        $key | Add-Member ScriptMethod GetValue {param($Name,$Default,$Options) return $script:WorkerProfileRoot}
        $key | Add-Member ScriptMethod Dispose {}
        return $key
    }
    function Open-LabelRemovalRegistryKey([string]$Hive,[string]$SubKey){
        if($Hive -eq 'LocalMachine'){if(-not $script:ProfileExists){return $null};return (New-L2RegistryKey)}
        if(-not $script:Hive.loaded){return $null}
        if($SubKey.EndsWith('\Software\KMTech\Label_Match')){if($script:Hive.completed){return (New-L2RegistryKey @($(if($Case -eq 'completed_lower'){'workerinstallcompleted'}else{'WorkerInstallCompleted'})))};return $null}
        if($SubKey.EndsWith('\Software\Microsoft\Windows\CurrentVersion\Run')){if($script:Hive.run_present){return (New-L2RegistryKey @($(if($Case -eq 'pending_run_lower'){'kmtech.labelmatch.relay'}else{'KMTech.LabelMatch.Relay'})))};return $null}
        return (New-L2RegistryKey)
    }
    function Get-LocalUser {param($SID,$ErrorAction)
        if($script:Account -eq 'ERROR'){throw 'OS_LOOKUP_FAILED'}
        if($null -eq $SID){return [pscustomobject]@{SID=$script:LocalUserFixture;Enabled=$true}}
        if($script:Account -eq 'ABSENT'){return $null}
        if($script:Account -eq 'UNMAPPED'){throw [Security.Principal.IdentityNotMappedException]::new()}
        return [pscustomobject]@{SID=$SID;Enabled=($script:Account -ne 'DISABLED');Locked=($Case -eq 'locked');Expired=($Case -eq 'expired')}
    }
    $receipt=Join-Path $profile 'AppData\Local\KMTech\DirectSync\label_match\status\current_user_removal.json'
    if($Case -in @('large_receipt','receipt','wrong_sid','wrong_status','wrong_root','old_receipt','receipt_reparse','receipt_hidden','fallback_receipt','array_sid','array_status','array_root')){
        [void][IO.Directory]::CreateDirectory((Split-Path -Parent $receipt))
        $value=@{operator_sid=$sid;status='PASS_DATA_PRESERVED';machine_code_root=$current;report_version=1;data_preserved=$true}
        if($Case -eq 'wrong_sid'){$value.operator_sid=$otherSid}
        if($Case -eq 'wrong_status'){$value.status='pass_data_preserved'}
        if($Case -eq 'wrong_root'){$value.machine_code_root=$current+'-peer'}
        if($Case -eq 'old_receipt'){$value.Remove('operator_sid')}
        if($Case -eq 'array_sid'){$value.operator_sid=@($sid)}
        if($Case -eq 'array_status'){$value.status=@('PASS_DATA_PRESERVED')}
        if($Case -eq 'array_root'){$value.machine_code_root=@($current)}
        [IO.File]::WriteAllText($receipt,($value|ConvertTo-Json))
        if($Case -eq 'receipt_hidden'){[IO.File]::SetAttributes($receipt,[IO.FileAttributes]::Hidden)}
        if($Case -eq 'receipt_reparse'){
            $target=Join-Path $Root 'unrelated-status';[void][IO.Directory]::CreateDirectory($target)
            [IO.File]::Copy($receipt,(Join-Path $target 'current_user_removal.json'))
            [IO.File]::Delete($receipt)
            [IO.Directory]::Delete((Split-Path -Parent $receipt))
            [void](New-Item -ItemType Junction -Path (Split-Path -Parent $receipt) -Target $target)
        }
    }
    if($Case -eq 'large_marker'){$markerValue.padding='x'*70000;[IO.File]::WriteAllText($marker,($markerValue|ConvertTo-Json))}
    if($Case -eq 'large_receipt'){$value.padding='x'*70000;[IO.File]::WriteAllText($receipt,($value|ConvertTo-Json))}
    if($Case -eq 'kit_missing_marker'){[IO.File]::Delete($marker)}
    if($Case -eq 'kit_invalid_marker'){[IO.File]::WriteAllText($marker,'{}')}
    if($Case -like 'kit_*'){
        $engine=Join-Path ([Environment]::SystemDirectory) 'WindowsPowerShell\v1.0\powershell.exe'
        $birth=[datetime]::UtcNow.AddMinutes(-1);$parentId=$PID+10000
        $packetScript=Join-Path $Root 'packet\INSTALL_THIS_PC.ps1'
        $kitArgv='"'+$engine+'" -File "'+$packetScript+'" '+($KitArguments -join ' ')
        if($kitArgv -match '-WorkerSid\b|-WorkerAccount\b'){throw 'KIT_ARGV_DIFFERS'}
        $script:Rows=@(
            [pscustomobject]@{ProcessId=$PID;ParentProcessId=$parentId;CreationDate=$birth.AddSeconds(1);ExecutablePath=$engine;CommandLine=$kitArgv},
            [pscustomobject]@{ProcessId=$parentId;ParentProcessId=0;CreationDate=$birth;ExecutablePath=$engine;CommandLine=$kitArgv}
        )
        function Get-CimInstance {param($ClassName,$Filter,$ErrorAction)
            if($Filter){$id=[int]($Filter -replace '[^0-9]','');return @($script:Rows | Where-Object ProcessId -eq $id)}
            return $script:Rows
        }
        function Get-LabelRemovalNativeProcessStartTime([int]$ProcessId){return @($script:Rows | Where-Object ProcessId -eq $ProcessId)[0].CreationDate}
        Assert-LabelRemovalQuiescent $application
    }
    $proof=$null
    $removalWorkerPartProof=$null
    $before=@(Get-ChildItem -LiteralPath $application -Recurse | ForEach-Object {$_.FullName}) -join '|'
    $rejected=$false
    try {
        if($Case -in @('entry_missing_receipt','entry_delegated','entry_raw_switch','kit_bound','kit_missing_marker','kit_invalid_marker')){
            $Uninstall=[Management.Automation.SwitchParameter]$true
            $DryRun=[Management.Automation.SwitchParameter]$false
            $testOverride=$false
            $installRootFull=$current
            $WriterFenceFunctionsPreloaded=[Management.Automation.SwitchParameter]($Case -in @('entry_delegated','entry_raw_switch'))
            function Enter-LabelCanonicalPlacement {if($Case -eq 'entry_raw_switch'){throw 'ATTESTATION_REFUSED'};return [pscustomobject]@{admitted=$true}}
            function Exit-LabelWriterAdmission($Lease) {}
            function Test-CurrentUserRelayPersistencePresent {return $false}
            $guards=@($ast.FindAll({param($node) $node -is [Management.Automation.Language.IfStatementAst]},$true) | Where-Object {
                $_ -is [Management.Automation.Language.IfStatementAst] -and
                ($_.Extent.Text.Contains('Assert-LabelMachineRemovalWorkerPart -InstallRoot') -or $_.Extent.Text.Contains('Test-CurrentUserRelayPersistencePresent'))
            })
            if($guards.Count -eq 0){throw 'ENTRY_GUARD_NOT_FOUND'}
            $guard=@($guards | Sort-Object {$_.Extent.Text.Length})[0]
            . ([scriptblock]::Create($guard.Extent.Text))
            $proof=$removalWorkerPartProof
        } else {$proof=Assert-LabelMachineRemovalWorkerPart -InstallRoot $current -WorkerAccount $script:WorkerAccount}
    }
    catch {if($Case -eq 'entry_raw_switch'){if($_.Exception.Message -cne 'ATTESTATION_REFUSED'){throw};$rejected=$true}else{if($_.Exception.Data['label_installer_reason_code'] -cne 'MACHINE_REMOVAL_WORKER_PART_PENDING'){throw};$rejected=$true}}
    $expectedReject=$Case -notin @('never_started','large_marker','large_receipt','kit_bound','entry_delegated','pending','receipt','disabled','deleted','fallback_receipt','profile_absent','receipt_hidden','unmapped')
    if($rejected -ne $expectedReject){throw 'WORKER_PART_ADMISSION_DIFFERS'}
    if($null -ne $proof){
        if($proof.proof -cnotin @('NEVER_STARTED','PROFILE_ABSENT','USER_RECEIPT','ACCOUNT_UNAVAILABLE')){throw 'PROOF_VOCABULARY_DIFFERS'}
        $assignment=@($ast.FindAll({param($node) $node -is [Management.Automation.Language.AssignmentStatementAst] -and $node.Extent.Text.StartsWith('$plan.decisions += ') -and $node.Extent.Text.Contains('WORKER_PART_')},$true))
        if($assignment.Count -ne 1){throw 'AUDIT_ASSIGNMENT_MISSING'}
        $plan=@{decisions=@()};$WorkerPartProof=$proof
        . ([scriptblock]::Create($assignment[0].Extent.Text))
        $row=$plan.decisions[0]
        if($row.path -cne 'application' -or $row.disposition -cne ('WORKER_PART_'+$proof.proof) -or -not $row.ContainsKey('worker_run_present') -or $row.worker_run_present -ne $proof.run_present){throw 'AUDIT_ROW_DIFFERS'}
    }
    $after=@(Get-ChildItem -LiteralPath $application -Recurse | ForEach-Object {$_.FullName}) -join '|'
    if($before -cne $after){throw 'REFUSAL_CHANGED_MACHINE_FILES'}
    if($Case -eq 'disabled' -and $proof.run_present -ne $true){throw 'RUN_AUDIT_MISSING'}
    if($Case -eq 'receipt_hidden'){[IO.File]::SetAttributes($receipt,[IO.FileAttributes]::Normal)}
}
Write-Output 'PASS_L2'
[IO.File]::WriteAllText((Join-Path $Root 'l2-result.json'),(@{case=$Case;passed=$true}|ConvertTo-Json))
