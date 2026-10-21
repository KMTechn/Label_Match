param([string]$Repo,[string]$Seed,[string]$Output)
$ErrorActionPreference='Stop'
Set-StrictMode -Version Latest
$ast=[Management.Automation.Language.Parser]::ParseFile((Join-Path $Repo 'INSTALL_CANONICAL_PORTABLE.ps1'),[ref]$null,[ref]$null)
$names=@('Full','Same','ByteSha','PinnedFileBytes','UInt64BE','HexBytes','PortableInventory','Save','Invoke-LabelResumeRollback')
foreach($node in $ast.FindAll({param($n) $n -is [Management.Automation.Language.FunctionDefinitionAst]},$true)) {
    if($node.Name -in $names -or $node.Name -like '*-LabelOrphan*') {
        . ([scriptblock]::Create($node.Extent.Text))
    }
}
. (Join-Path $Repo 'tools/label_writer_fence.ps1')
function Sha([string]$Path){Get-FileHash -LiteralPath $Path -Algorithm SHA256|ForEach-Object {$_.Hash.ToLowerInvariant()}}
function Get-KmtechFileSha([string]$Path){Sha $Path}
function ConvertTo-KmtechPublicDiagnosticTree($Value){return $Value}
function ConvertTo-KmtechRedactedSinkObject($Value){return $Value}
function Assert-LabelOrphanProtectedPath([string]$Path,[string]$MachineRoot=''){
    if($script:CaseMode -ceq 'protected_unreadable'){throw 'ORPHAN_TEST_PROTECTED_UNREADABLE'}
}
function Get-LabelOrphanProcessRows {
    if(-not [OrphanTestMutexProbe]::CanAcquire((Get-LabelWriterAdmissionMutexName $fenceControl))){throw 'TEST_CENSUS_HELD_WRITER_ADMISSION'}
    if($script:CaseMode -ceq 'census_failure'){throw 'ORPHAN_TEST_CIM_FAILURE'}
    return $script:CensusRows
}
function Snapshot {
    if($script:CaseMode -cin @('unknown_artifacts','known_run_unreadable')){throw 'ORPHAN_TEST_RUN_UNREADABLE'}
    return $script:RunForTest
}
function Get-LabelCurrentUserScheduledTasks {
    if($script:CaseMode -cin @('unknown_artifacts','known_task_unreadable')){throw 'ORPHAN_TEST_TASK_UNREADABLE'}
    return $null
}
function ReceiptSource($Source,$Manifest){return [pscustomobject]@{critical_file_sha256=[pscustomobject]@{writer_fence_helper=(Sha (Join-Path $Source 'tools/label_writer_fence.ps1'))}}}
$script:ReplayCount=0
$script:RealReleaseSave=(Get-Command Save-LabelOrphanRelease).ScriptBlock
Add-Type -TypeDefinition @'
using System.Threading;
using System.Threading.Tasks;
public static class OrphanTestMutexProbe {
 public static bool CanAcquire(string name) {
  return Task.Run(() => { using(var m=new Mutex(false,name)) {
    bool acquired=m.WaitOne(0); if(acquired)m.ReleaseMutex(); return acquired;
  }}).GetAwaiter().GetResult();
 }
}
'@
$script:RealOrphanCode=(Get-Command Get-LabelOrphanCode).ScriptBlock
function Get-LabelOrphanCode($Audit){
    if(-not [OrphanTestMutexProbe]::CanAcquire((Get-LabelWriterAdmissionMutexName $fenceControl))){throw 'TEST_INVENTORY_HELD_WRITER_ADMISSION'}
    & $script:RealOrphanCode $Audit
}
function Save-LabelOrphanRelease([string]$Path,$Record){
    $script:ReleaseEvents+=('SAVE_'+$Record.state)
    if($script:CaseMode -ceq 'save_before_failure' -and $Record.state -ceq 'RELEASING'){throw 'ORPHAN_TEST_SAVE_FAILED'}
    if($script:CaseMode -cin @('final_save_once','minimal_final_save_once') -and $Record.state -ceq 'RELEASED' -and -not $script:FaultInjected){
        $script:FaultInjected=$true;throw 'ORPHAN_TEST_FINAL_SAVE_FAILED'
    }
    $published=$Record
    if($script:CaseMode -cin @('minimal_record_stop_once','minimal_final_save_once') -and $Record.state -ceq 'RELEASING' -and -not $script:MinimalPublished){
        $published=[ordered]@{}
        foreach($name in $Record.Keys){if($name -cnotin @('observations','observed_fence_sha256')){$published[$name]=$Record[$name]}}
        $script:MinimalPublished=$true
    }
    & $script:RealReleaseSave $Path $published
    if($Record.state -ceq 'RELEASING'){
        $script:WriterRaceRefused=-not [OrphanTestMutexProbe]::CanAcquire((Get-LabelWriterAdmissionMutexName $fenceControl))
        if($script:CaseMode -cin @('record_stop_once','minimal_record_stop_once') -and -not $script:FaultInjected){$script:FaultInjected=$true;throw 'ORPHAN_TEST_AFTER_RECORD'}
        if($script:CaseMode -ceq 'operator_edit'){$script:RunForTest=[pscustomobject]@{exists=$true;kind='String';data='operator-edited-command'}}
        if($script:CaseMode -ceq 'fence_swap'){
            $replacement=Read-LabelOrphanJson $script:ActivePathForTest
            $replacement.attempt_id=[Guid]::NewGuid().ToString('N')
            $replacement.session_authority_mutex_name=Get-LabelWriterSessionAuthorityMutexName $replacement.session_id $replacement.attempt_id $replacement.orchestrator_sha256 $replacement.replacement_transaction_id $replacement.writer_contract_sha256
            [IO.File]::WriteAllText($script:ActivePathForTest,($replacement|ConvertTo-Json -Depth 10),[Text.UTF8Encoding]::new($false))
            $script:InjectedFenceHash=Sha $script:ActivePathForTest
        }
    }
}
function Remove-Item([string]$LiteralPath,[string]$ErrorAction){
    if($script:TestingPublicRelease -and $LiteralPath -ceq $script:ActivePathForTest){
        $script:ReleaseEvents+='DELETE_FENCE'
        $record=Read-LabelOrphanJson $script:ReleasePathForTest
        if($record.state -cne 'RELEASING'){throw 'ORPHAN_TEST_DELETE_BEFORE_RECORD'}
        if($script:CaseMode -ceq 'clear_once' -and -not $script:FaultInjected){$script:FaultInjected=$true;throw 'ORPHAN_TEST_CLEAR_FAILED'}
    }
    Microsoft.PowerShell.Management\Remove-Item -LiteralPath $LiteralPath -ErrorAction Stop
}
foreach($name in @('Invoke-LabelCanonicalRollback','FreezePlacementHelper','StartRaw','Start-LabelPlacementSession','RestoreScheduledTask','Restore')) {
    Set-Item -LiteralPath ('function:'+ $name) -Value {$script:ReplayCount++;throw 'ORPHAN_REPLAY_FORBIDDEN'}
}
$jsonArgs=@{}
if((Get-Command ConvertFrom-Json).Parameters.ContainsKey('DateKind')){$jsonArgs.DateKind='String'}
$seeds=Get-Content -LiteralPath $Seed -Raw -Encoding UTF8|ConvertFrom-Json @jsonArgs
$results=@()
$originalProgramData=$env:PROGRAMDATA
foreach($seedCase in $seeds){
    $script:CaseMode=[string](Get-LabelOrphanValue $seedCase 'mode')
    $script:CensusRows=@();$script:ReleaseEvents=@();$script:FaultInjected=$false;$script:InjectedFenceHash=''
    $script:WriterRaceRefused=$true;$script:TestingPublicRelease=$false
    $script:MinimalPublished=$false;$script:RunForTest=[pscustomobject]@{exists=$false;kind='';data=''}
    $observerMutex=$null
    $root=[string]$seedCase.case_root
    $install=[string]$seedCase.audit.install_root
    $source=$Repo
    $sourceManifest=[pscustomobject]@{source_commit=[string]$seedCase.audit.source_commit}
    $lad=Join-Path $root 'local'
    $localAuditRoot=Join-Path $lad 'KMTech/Label_Match/install-audit'
    [void][IO.Directory]::CreateDirectory($localAuditRoot)
    $writerFenceControlRoot=Join-Path $root 'control'
    [void][IO.Directory]::CreateDirectory($writerFenceControlRoot)
    # A machine install keeps its only original audit and its fence under the fixed machine
    # ProgramData root; the audit is not copied into the operator LocalAppData root.
    $env:PROGRAMDATA=$originalProgramData
    $layout=[string](Get-LabelOrphanValue $seedCase 'layout')
    $fenceControl=$writerFenceControlRoot
    $machineAuditRoot=''
    if($layout -ceq 'machine'){
        $env:PROGRAMDATA=Join-Path $root 'pd'
        $machineAuditRoot=Join-Path $env:PROGRAMDATA 'KMTech/Label_Match/install-audit'
        $localAuditRoot=$machineAuditRoot
        [void][IO.Directory]::CreateDirectory($localAuditRoot)
        $fenceControl=Join-Path $machineAuditRoot ('machine-writer-'+[string]$seedCase.audit.run_id)
        [void][IO.Directory]::CreateDirectory($fenceControl)
    }
    if($script:CaseMode -ceq 'machine_audit_operator_fence'){$fenceControl=$writerFenceControlRoot}
    if($script:CaseMode -ceq 'operator_audit_machine_fence'){
        $env:PROGRAMDATA=Join-Path $root 'pd'
        $machineAuditRoot=Join-Path $env:PROGRAMDATA 'KMTech/Label_Match/install-audit'
        $fenceControl=Join-Path $machineAuditRoot ('machine-writer-'+[string]$seedCase.audit.run_id)
        [void][IO.Directory]::CreateDirectory($fenceControl)
    }
    $stop=Join-Path $root 'relay.stop.json'
    $CanonicalTaskName='direct-sync-relay-label-match'
    $testMode=$true;$PlanOnly=$false;$generationSubjectSid='S-1-5-21-1'
    $generationMarkerPaths=@()
    $Script:LabelWriterFenceAdmissionProductionRoot=$fenceControl
    $codeClass=[string](Get-LabelOrphanValue $seedCase 'code_class')
    if(-not $codeClass){$codeClass='CANDIDATE'}
    $inventory=if(Test-Path -LiteralPath $install){(PortableInventory $install).sha256}else{''}
    $candidateHash=$inventory;$installedHash=''
    if($codeClass -ceq 'INSTALLED'){
        $installedHash=$inventory;$candidateHash='b'*64
        $seedCase.audit.code_preimage='PRESENT';$seedCase.audit.rollback.applied=$true
        $seedCase.audit.rollback|Add-Member -NotePropertyName code_restored -NotePropertyValue $true -Force
    }
    if($script:CaseMode -cin @('equal_candidate','equal_installed','equal_unknown')){
        $candidateHash=$inventory;$installedHash=$inventory;$seedCase.audit.code_preimage='PRESENT'
        if($script:CaseMode -ceq 'equal_candidate'){$seedCase.audit.code_state='PRESENT'}
    }
    $seedCase.audit|Add-Member -NotePropertyName writer_transition -NotePropertyValue ([pscustomobject]@{
        installed_portable_inventory_sha256=$installedHash;candidate_portable_inventory_sha256=$candidateHash}) -Force
    $ResumeRollbackAuditPath=Join-Path $localAuditRoot ('canonical-portable-'+$seedCase.audit.run_id+'.json')
    Save $ResumeRollbackAuditPath $seedCase.audit
    $ExpectedRollbackAuditSha256=Sha $ResumeRollbackAuditPath
    $activePath=Join-Path $fenceControl 'active.json'
    $script:ActivePathForTest=$activePath
    [IO.File]::WriteAllText($activePath,($seedCase.active|ConvertTo-Json -Depth 10),[Text.UTF8Encoding]::new($false))
    $owner=[ordered]@{schema='label-match-installer-owner-v1';run_id=[string]$seedCase.audit.run_id;
        install_root=$install;control_root=$fenceControl}
    foreach($field in @('session_id','attempt_id','replacement_transaction_id','writer_inventory_sha256','orchestrator_sha256','writer_contract_sha256','session_started_at_utc','owner_process_id','owner_process_started_at_filetime')){
        $owner[$field]=$seedCase.audit.writer_fence.$field
    }
    $ownerPath=Join-Path (Split-Path -Parent $install) ('.installer-owner-'+$owner.session_id+'-'+($owner.run_id -split '-')[-1]+'.json')
    [IO.File]::WriteAllText($ownerPath,($owner|ConvertTo-Json -Depth 10),[Text.UTF8Encoding]::new($false))
    $script:ReleasePathForTest=Join-Path $localAuditRoot ('orphan-fence-release-'+$owner.attempt_id+'.json')
    if($script:CaseMode -ceq 'free_authority_handle'){
        $observerMutex=[Threading.Mutex]::new($false,([string]$seedCase.active.session_authority_mutex_name))
    }
    if($script:CaseMode -like 'tuple_*'){
        $field=$script:CaseMode.Substring(6)
        $seedCase.active.$field=if($field -ceq 'session_started_at_utc'){'2026-01-01T00:00:00Z'}else{('f'*([string]$seedCase.active.$field).Length)}
        [IO.File]::WriteAllText($activePath,($seedCase.active|ConvertTo-Json -Depth 10),[Text.UTF8Encoding]::new($false))
    }
    if($script:CaseMode -cin @('owner_live','owner_birth')){
        $owner.owner_process_id=$PID
        $owner.owner_process_started_at_filetime=([Diagnostics.Process]::GetCurrentProcess().StartTime.ToUniversalTime().ToFileTimeUtc()-$(if($script:CaseMode -ceq 'owner_birth'){1}else{0})).ToString()
        $seedCase.audit.writer_fence.owner_process_id=$owner.owner_process_id
        $seedCase.audit.writer_fence.owner_process_started_at_filetime=$owner.owner_process_started_at_filetime
        [IO.File]::WriteAllText($ownerPath,($owner|ConvertTo-Json -Depth 10),[Text.UTF8Encoding]::new($false))
    }
    if($script:CaseMode -ceq 'owner_other_control'){
        $owner.control_root=Join-Path (Split-Path -Parent $fenceControl) ('machine-writer-20260101T000000000Z-'+[Guid]::NewGuid().ToString('N'))
        [IO.File]::WriteAllText($ownerPath,($owner|ConvertTo-Json -Depth 10),[Text.UTF8Encoding]::new($false))
    }
    if($script:CaseMode -ceq 'owner_other_tuple'){
        $owner.attempt_id=[Guid]::NewGuid().ToString('N')
        [IO.File]::WriteAllText($ownerPath,($owner|ConvertTo-Json -Depth 10),[Text.UTF8Encoding]::new($false))
    }
    if($script:CaseMode -ceq 'fence_other_control'){
        $operatorActive=Join-Path $writerFenceControlRoot 'active.json'
        [IO.File]::WriteAllText($operatorActive,($seedCase.active|ConvertTo-Json -Depth 10),[Text.UTF8Encoding]::new($false))
        Remove-Item -LiteralPath $activePath -ErrorAction Stop
        $activePath=$operatorActive
    }
    if($script:CaseMode -ceq 'machine_no_fence'){Remove-Item -LiteralPath $activePath -ErrorAction Stop}
    if($script:CaseMode -ceq 'audit_other_root'){
        $localAuditRoot=Join-Path $root 'other-audit'
        [void][IO.Directory]::CreateDirectory($localAuditRoot)
        $ResumeRollbackAuditPath=Join-Path $localAuditRoot ('canonical-portable-'+$seedCase.audit.run_id+'.json')
        $script:ReleasePathForTest=Join-Path $localAuditRoot ('orphan-fence-release-'+$owner.attempt_id+'.json')
    }
    if($script:CaseMode -ceq 'install_other'){$seedCase.audit.install_root=Join-Path $root 'managed/other-current'}
    if($script:CaseMode -cin @('census_helper','census_bootstrap','census_unreadable','census_encoded','census_nested','census_self','census_unrelated','census_descendant',
        'census_spelling','census_spelling_unrelated','census_nested_unquoted','census_undecodable','census_command')){
        $command=if($script:CaseMode -ceq 'census_unrelated'){'unrelated maintenance'}else{'-File "'+$install+'\helper.ps1"'}
        if($script:CaseMode -ceq 'census_command'){$command=[string]$seedCase.command_line}
        if($script:CaseMode -ceq 'census_encoded'){$command='-EncodedCommand '+[Convert]::ToBase64String([Text.Encoding]::Unicode.GetBytes($command))}
        if($script:CaseMode -ceq 'census_nested'){
            $nested=[Convert]::ToBase64String([Text.Encoding]::UTF8.GetBytes(('{"attempt_id":"'+$owner.attempt_id+'"}')))
            $wrapped='$payload='''+$nested+''';[Text.Encoding]::UTF8.GetString([Convert]::FromBase64String($payload))'
            $command='-EncodedCommand '+[Convert]::ToBase64String([Text.Encoding]::Unicode.GetBytes($wrapped))
        }
        if($script:CaseMode -ceq 'census_spelling'){$command=[string]$seedCase.spelling+' '+[Convert]::ToBase64String([Text.Encoding]::Unicode.GetBytes($command))}
        if((Get-LabelOrphanValue $seedCase 'quote_payload')){$parts=$command -split ' ',2;$command=$parts[0]+' '+[char]34+$parts[1]+[char]34}
        if($script:CaseMode -ceq 'census_spelling_unrelated'){$command=[string]$seedCase.spelling+' '+[Convert]::ToBase64String([Text.Encoding]::Unicode.GetBytes('unrelated maintenance'))}
        if($script:CaseMode -ceq 'census_undecodable'){$command=[string]$seedCase.spelling+' ABC'}
        if($script:CaseMode -ceq 'census_nested_unquoted'){
            $inner=[Convert]::ToBase64String([Text.Encoding]::Unicode.GetBytes($command))
            $wrapped='powershell.exe -NoProfile '+[string]$seedCase.spelling+' '+$inner
            $command=[string]$seedCase.spelling+' '+[Convert]::ToBase64String([Text.Encoding]::Unicode.GetBytes($wrapped))
        }
        $script:CensusRows=@([pscustomobject]@{ProcessId=$(if($script:CaseMode -ceq 'census_self'){$PID}else{60001});ParentProcessId=$(if($script:CaseMode -ceq 'census_descendant'){$owner.owner_process_id}else{1});
            Name=$(if($script:CaseMode -ceq 'census_bootstrap'){'python.exe'}else{'powershell.exe'});ExecutablePath='C:\Windows\System32\WindowsPowerShell\v1.0\powershell.exe';
            CommandLine=$command;CreationDate=$(if($script:CaseMode -ceq 'census_self'){[Diagnostics.Process]::GetCurrentProcess().StartTime}else{[DateTime]::UtcNow})})
        if($script:CaseMode -ceq 'census_unreadable'){$script:CensusRows[0].CommandLine=$null}
    }
    if($script:CaseMode -ceq 'census_idle'){$script:CensusRows=@([pscustomobject]@{ProcessId=0;ParentProcessId=0;Name='System Idle Process';ExecutablePath=$null;CommandLine=$null;CreationDate=$null})}
    if($script:CaseMode -ceq 'inventory_changed'){[IO.File]::AppendAllText((Join-Path $install 'app/payload.txt'),'different')}
    if($script:CaseMode -ceq 'known_absent'){$seedCase.audit.code_state='ABSENT'}
    if($script:CaseMode -ceq 'absent_present'){$seedCase.audit.code_state='PRESENT'}
    if($script:CaseMode -ceq 'candidate_restored'){$seedCase.audit.rollback.applied=$true;$seedCase.audit.rollback|Add-Member -NotePropertyName code_restored -NotePropertyValue $true -Force}
    if($script:CaseMode -ceq 'installed_retained'){$seedCase.audit.code_state='PRESENT';$seedCase.audit.rollback.applied=$false}
    if($script:CaseMode -ceq 'preimage_changed'){$seedCase.audit|Add-Member -NotePropertyName recovery_context -NotePropertyValue ([pscustomobject]@{values=[pscustomobject]@{installedPreimageInventory=[pscustomobject]@{sha256=('c'*64)}}}) -Force}
    if($script:CaseMode -ceq 'run_changed'){$seedCase.audit|Add-Member -NotePropertyName after -NotePropertyValue ([pscustomobject]@{exists=$true;kind='String';data='different-command'}) -Force}
    if($script:CaseMode -ceq 'stop_changed'){$seedCase.audit|Add-Member -NotePropertyName stop_marker_absent -NotePropertyValue $false -Force}
    if($script:CaseMode -ceq 'task_changed'){$seedCase.audit|Add-Member -NotePropertyName scheduled_task_preimage -NotePropertyValue ([pscustomobject]@{exists=$true;enabled=$true;xml_sha256=('c'*64)}) -Force}
    if($script:CaseMode -ceq 'pending_changed'){$seedCase.audit|Add-Member -NotePropertyName worker_pending -NotePropertyValue ([pscustomobject]@{path=(Join-Path (Split-Path -Parent $install) 'update-control/worker-pending.json');install_id=$owner.run_id}) -Force}
    if($script:CaseMode -ceq 'shortcut_changed'){$seedCase.audit|Add-Member -NotePropertyName machine_shortcut -NotePropertyValue ([pscustomobject]@{path=(Join-Path $install 'app/payload.txt');sha256=('c'*64)}) -Force}
    if($script:CaseMode -ceq 'known_run_unreadable'){$seedCase.audit|Add-Member -NotePropertyName after -NotePropertyValue ([pscustomobject]@{exists=$false;kind='';data=''}) -Force}
    if($script:CaseMode -ceq 'known_task_unreadable'){$seedCase.audit|Add-Member -NotePropertyName scheduled_task_preimage -NotePropertyValue ([pscustomobject]@{exists=$false;enabled=$false;xml_sha256=''}) -Force}
    if($script:CaseMode -ceq 'unknown_artifacts'){
        [void][IO.Directory]::CreateDirectory($stop)
        [void][IO.Directory]::CreateDirectory((Join-Path (Split-Path -Parent $install) 'update-control/worker-pending.json'))
    }
    if($script:CaseMode -ceq 'legacy_no_fence'){
        Remove-Item -LiteralPath $activePath -ErrorAction Stop
        $values=[ordered]@{}
        foreach($name in @('before','old','taskBefore','stopBefore','healthyRemovalReportBefore','removalReportExisted','existingVerified',
            'installedPreimageInventory','replacementRollbackRoot','writerTransition','legacyFirstRegistration','healthyRemovedInstall',
            'firstRegistrationStateVerified','firstRegistrationStarted','firstRegistrationAttempt','firstRegistrationAttemptPath',
            'firstRegistrationStatePaths','firstRegistrationAbsentAncestors','rollbackSources','restoreSources')){$values[$name]=$null}
        $values.existingVerified=$false
        $seedCase.audit|Add-Member -NotePropertyName recovery_context -NotePropertyValue ([pscustomobject]@{operator_sid=$generationSubjectSid;
            orchestrator_sha256=(Sha (Join-Path $source 'INSTALL_CANONICAL_PORTABLE.ps1'));values=[pscustomobject]$values;generation_marker_paths=@()}) -Force
        $seedCase.audit|Add-Member -NotePropertyName elevation_log_path -NotePropertyValue '' -Force
    }
    if($script:CaseMode -cin @('census_saved_descendant','census_owner_unreadable_birth')){
        $parentId=$owner.owner_process_id
        if($script:CaseMode -ceq 'census_saved_descendant'){
            $parentId=4000000011L
            $seedCase.audit|Add-Member -NotePropertyName retained_child -NotePropertyValue ([pscustomobject]@{
                child_process_id=$parentId;child_process_started_at_filetime=$owner.owner_process_started_at_filetime}) -Force
        }
        $script:CensusRows=@([pscustomobject]@{ProcessId=4000000012L;ParentProcessId=$parentId;Name='neutral.exe';
            ExecutablePath='C:/neutral/neutral.exe';CommandLine='neutral';
            CreationDate=$(if($script:CaseMode -ceq 'census_owner_unreadable_birth'){$null}else{[DateTime]::UtcNow})})
    }
    Save $ResumeRollbackAuditPath $seedCase.audit
    if($script:CaseMode -ceq 'audit_corrupt'){[IO.File]::WriteAllText($ResumeRollbackAuditPath,'{invalid')}
    if($script:CaseMode -ceq 'owner_corrupt'){[IO.File]::WriteAllText($ownerPath,'{invalid')}
    if($script:CaseMode -ceq 'release_corrupt'){[IO.File]::WriteAllText($script:ReleasePathForTest,'{invalid')}
    if($script:CaseMode -ceq 'duplicate_owner'){
        $duplicateRun=($owner.run_id -replace '[0-9a-f]{32}$',[Guid]::NewGuid().ToString('N'))
        $duplicate=[ordered]@{};foreach($key in $owner.Keys){$duplicate[$key]=$owner[$key]};$duplicate.run_id=$duplicateRun
        $duplicatePath=Join-Path (Split-Path -Parent $install) ('.installer-owner-'+$owner.session_id+'-'+($duplicateRun -split '-')[-1]+'.json')
        [IO.File]::WriteAllText($duplicatePath,($duplicate|ConvertTo-Json -Depth 10),[Text.UTF8Encoding]::new($false))
    }
    if($script:CaseMode -like 'child_result_*'){
        $childPath=Join-Path $localAuditRoot ('canonical-portable-'+$owner.run_id+'-guardian-'+[Guid]::NewGuid().ToString('N')+'.1.json')
        $child=[ordered]@{schema='label-placement-session-result-v1';transaction_id=$owner.replacement_transaction_id;sequence=1;exit_code=0;
            child_process_id=$(if($script:CaseMode -ceq 'child_result_absent'){4294967294}else{$PID});
            child_process_started_at_filetime=[Diagnostics.Process]::GetCurrentProcess().StartTime.ToUniversalTime().ToFileTimeUtc().ToString()}
        if($script:CaseMode -ceq 'child_result_mismatch'){$child.transaction_id=[Guid]::NewGuid().ToString('N')}
        if($script:CaseMode -ceq 'child_result_reused'){$child.child_process_started_at_filetime=([long]$child.child_process_started_at_filetime-1).ToString()}
        [IO.File]::WriteAllText($childPath,($child|ConvertTo-Json -Depth 5),[Text.UTF8Encoding]::new($false))
        if($script:CaseMode -ceq 'child_result_corrupt'){[IO.File]::WriteAllText($childPath,'{invalid')}
    }
    $ExpectedRollbackAuditSha256=Sha $ResumeRollbackAuditPath
    $originalFenceHash=if(Test-Path -LiteralPath $activePath){Sha $activePath}else{''}
    $item=[ordered]@{site=$seedCase.row.site;status=$seedCase.row.status;fence_status=$seedCase.row.fence_status;
        released=$false;reinstall_open=$false;idempotent=$false;control=$fenceControl;error_type='';error_line=0;reason='';
        mode=$script:CaseMode;first_failure_proven=$false;observed_code_class='';subsequent_fence_preserved=$false}
    $item['layout']=$(if($layout){$layout}else{'user'})
    $item['audit_in_machine_root']=[bool]$machineAuditRoot -and (Same (Split-Path -Parent $ResumeRollbackAuditPath) $machineAuditRoot)
    $item['audit_in_operator_root']=Same (Split-Path -Parent $ResumeRollbackAuditPath) (Join-Path $lad 'KMTech/Label_Match/install-audit')
    if($script:CaseMode -ceq 'legacy_no_fence'){
        try{Invoke-LabelResumeRollback}catch{}
        $item['legacy_path_reached']=$script:ReplayCount -gt 0
        $item['audit_unchanged']=(Sha $ResumeRollbackAuditPath) -ceq $ExpectedRollbackAuditSha256
        $item['replay_count']=$script:ReplayCount
        $results += [pscustomobject]$item
        continue
    }
    try {
        $script:TestingPublicRelease=$true
        if($script:CaseMode -cin @('record_stop_once','clear_once','final_save_once','minimal_record_stop_once','minimal_final_save_once')){
            try{Invoke-LabelResumeRollback;throw 'ORPHAN_TEST_FAULT_NOT_REACHED'}catch{
                $pendingRelease=Read-LabelOrphanJson $script:ReleasePathForTest
                $item.first_failure_proven=$pendingRelease.state -ceq 'RELEASING' -and
                    $(if($script:CaseMode -cin @('final_save_once','minimal_final_save_once')){-not(Test-Path -LiteralPath $activePath)}else{(Sha $activePath) -ceq $originalFenceHash})
            }
        }
        Invoke-LabelResumeRollback
        $releasePath=Join-Path $localAuditRoot ('orphan-fence-release-'+$owner.attempt_id+'.json')
        $release=Get-Content -LiteralPath $releasePath -Raw -Encoding UTF8|ConvertFrom-Json @jsonArgs
        $item.released=(-not(Test-Path -LiteralPath $activePath)) -and $release.state -ceq 'RELEASED'
        $item.observed_code_class=$release.observed_code_class
        if($script:CaseMode -ceq 'minimal_released'){
            $release.PSObject.Properties.Remove('observations');$release.PSObject.Properties.Remove('observed_fence_sha256')
            [IO.File]::WriteAllText($releasePath,($release|ConvertTo-Json -Depth 20),[Text.UTF8Encoding]::new($false))
        }
        $releaseHash=Sha $releasePath
        Invoke-LabelResumeRollback
        $item.idempotent=(Sha $releasePath) -ceq $releaseHash -and -not(Test-Path -LiteralPath $activePath)
        $script:TestingPublicRelease=$false
        $new=[Guid]::NewGuid().ToString('N')
        [void](Start-LabelWriterFence -ControlRoot $fenceControl -Status 'QUIESCING' -SessionId $new -AttemptId $new `
            -ReplacementTransactionId $new -SessionStartedAtUtc ([DateTime]::UtcNow.ToString('o')) `
            -OrchestratorSha256 $owner.orchestrator_sha256 -WriterContractSha256 $owner.writer_contract_sha256 -AuthorityOwnedByCaller)
        $item.reinstall_open=Test-Path -LiteralPath $activePath
        $subsequentHash=Sha $activePath
        try{Invoke-LabelResumeRollback}catch{$item.subsequent_fence_preserved=(Sha $activePath) -ceq $subsequentHash}
        Remove-Item -LiteralPath $activePath -ErrorAction Stop
    } catch {
        $item.error_type=$_.Exception.GetType().Name;$item.error_line=$_.InvocationInfo.ScriptLineNumber
        if($_.Exception.Message -cmatch '^ORPHAN_[A-Z_]+$'){$item.reason=$_.Exception.Message}
        if($_.Exception.Message -ceq 'Rollback audit is outside the original operator audit root.'){$item.reason='AUDIT_ROOT_REJECTED'}
    }
    $script:TestingPublicRelease=$false
    $item['fence_unchanged']=(Test-Path -LiteralPath $activePath) -and (Sha $activePath) -ceq $originalFenceHash
    $item['foreign_fence_preserved']=$script:InjectedFenceHash -and (Test-Path -LiteralPath $activePath) -and (Sha $activePath) -ceq $script:InjectedFenceHash
    $item['writer_race_refused']=$script:WriterRaceRefused
    $item['events']=$script:ReleaseEvents
    $item['operator_edit_preserved']=$script:RunForTest.data -ceq 'operator-edited-command'
    $item['unknown_artifacts_preserved']=(Test-Path -LiteralPath $stop -PathType Container) -and (Test-Path -LiteralPath (Join-Path (Split-Path -Parent $install) 'update-control/worker-pending.json') -PathType Container)
    $item['audit_unchanged']=(Sha $ResumeRollbackAuditPath) -ceq $ExpectedRollbackAuditSha256
    $item['replay_count']=$script:ReplayCount
    if($null -ne $observerMutex){$observerMutex.Dispose()}
    $results += [pscustomobject]$item
}
$env:PROGRAMDATA=$originalProgramData
[IO.File]::WriteAllText($Output,($results|ConvertTo-Json -Depth 15),[Text.UTF8Encoding]::new($false))
