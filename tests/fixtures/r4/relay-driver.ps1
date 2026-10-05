param([string]$Repo,[string]$Output,[string]$InputPath)
$ErrorActionPreference='Stop'
. (Join-Path $Repo 'tools/failure_redaction.ps1')
$ExtractedFunctionNames=@('Get-LabelOfficialLegacyTaskExpectation','Get-LabelOfficialLegacyTaskExpectations',
    'Get-LabelOfficialLegacyPrincipalModes','Get-LabelOfficialLegacyTaskFingerprint',
    'Get-LabelOfficialLegacyTaskSnapshot','Assert-LabelOfficialLegacyTaskStopped','Invoke-LabelOfficialLegacyTaskTransition')
$inputRecord=Get-Content -LiteralPath $InputPath -Raw -Encoding UTF8 | ConvertFrom-Json
$tokens=$null;$errors=$null
$ast=[Management.Automation.Language.Parser]::ParseFile((Join-Path $Repo 'INSTALL_THIS_PC.ps1'),[ref]$tokens,[ref]$errors)
if($errors.Count){throw 'R4 product parse failed'}
$functions=@{}
foreach($node in $ast.FindAll({param($n)$n -is [Management.Automation.Language.FunctionDefinitionAst]},$true)){$functions[$node.Name]=$node.Extent.Text}
$missing=@($ExtractedFunctionNames | Where-Object {-not $functions.ContainsKey($_)})
if($missing.Count){
    [ordered]@{seam_available=$false;missing=$missing} | ConvertTo-Json -Depth 8 | Set-Content -LiteralPath $Output -Encoding UTF8
    exit 0
}
foreach($definition in $functions.Values){. ([scriptblock]::Create($definition))}
$env:COMPUTERNAME='R4-FIXTURE-PC';$env:SystemRoot='C:\Windows'
$script:events=[Collections.Generic.List[string]]::new()
$script:registerCalls=0;$script:deleteCalls=0;$script:newRelayEnabled=$false
$script:tasks=@()
$script:root=[IO.Path]::GetDirectoryName([IO.Path]::GetFullPath($InputPath))
$directory=Join-Path $script:root 'transition'
foreach($record in @($inputRecord.tasks)){
    [xml]$xml=[string]$record.xml
    $actions=@($xml.Task.Actions.ChildNodes | Where-Object {$_.NodeType -eq 'Element'} | ForEach-Object {
        [pscustomobject]@{Execute=[string]$_.Command;Arguments=[string]$_.Arguments;WorkingDirectory=[string]$_.WorkingDirectory}
    })
    $script:tasks += [pscustomobject]@{TaskName=$record.name;TaskPath=$record.path;Xml=[string]$record.xml;
        State=$record.state;Settings=[pscustomobject]@{Enabled=[bool]$record.enabled};Actions=$actions;
        Principal=[pscustomobject]@{UserId=[string]$xml.Task.Principals.Principal.UserId;
            LogonType=[string]$xml.Task.Principals.Principal.LogonType;RunLevel=[string]$xml.Task.Principals.Principal.RunLevel};
        Triggers=@([pscustomobject]@{Repetition=[pscustomobject]@{Interval=[string]$xml.Task.Triggers.TimeTrigger.Repetition.Interval}})}
}
function Get-ScheduledTask {
    param($TaskName,$TaskPath,$ErrorAction)
    $script:events.Add('query')
    if($inputRecord.query_failure){throw 'R4 synthetic scheduler query failure'}
    return @($script:tasks | Where-Object {(-not $TaskName -or $_.TaskName -ieq $TaskName) -and (-not $TaskPath -or $_.TaskPath -ieq $TaskPath)})
}
function Export-ScheduledTask {
    param($TaskName,$TaskPath,$ErrorAction)
    $script:events.Add('xml_read')
    return [string](@($script:tasks | Where-Object {$_.TaskName -ieq $TaskName})[0].Xml)
}
function Set-R4Enabled($TaskName,[bool]$Enabled){
    $preimage=Join-Path $directory 'preimage.xml'
    if(-not (Test-Path -LiteralPath $preimage)){throw 'R4 mutation preceded persisted preimage'}
    foreach($task in @($script:tasks | Where-Object {$_.TaskName -ieq $TaskName})){
        $task.Settings.Enabled=$Enabled
        $pattern='(?s)(?<before><(?:\w+:)?Settings\b[^>]*>.*?<(?:\w+:)?Enabled>)[^<]*(?<after></(?:\w+:)?Enabled>)'
        $task.Xml=[regex]::Replace($task.Xml,$pattern,[Text.RegularExpressions.MatchEvaluator]{
            param($m) return $m.Groups['before'].Value+$Enabled.ToString().ToLowerInvariant()+$m.Groups['after'].Value
        })
    }
}
function Disable-ScheduledTask {param($TaskName,$TaskPath,$ErrorAction) $script:events.Add('disable');Set-R4Enabled $TaskName $false}
function Enable-ScheduledTask {param($TaskName,$TaskPath,$ErrorAction) $script:events.Add('enable');Set-R4Enabled $TaskName $true}
function Register-ScheduledTask {$script:registerCalls++;throw 'R4 re-registration/password access forbidden'}
function Unregister-ScheduledTask {$script:deleteCalls++;throw 'R4 official task deletion forbidden'}
function Stop-ScheduledTask {throw 'R4 force-stop forbidden'}
function Start-Process {throw 'R4 native process execution forbidden'}
function Product {throw 'R4 real product execution forbidden'}
function StartRaw {
    if(@($script:tasks | Where-Object {$_.Settings.Enabled}).Count){throw 'R4 old/new enabled overlap'}
    $script:newRelayEnabled=$true;$script:events.Add('new_relay_start');return 1
}
function Command {return 'R4 synthetic new relay command'}
function Get-CimInstance {return @()}
function Get-ItemProperty {return [pscustomobject]@{MachineGuid='R4_SYNTHETIC_MACHINE'}}
function ConvertTo-LabelTaskSid {
    param($Identity)
    if([string]$Identity -ceq [string]$inputRecord.same_user_sid){
        # Internal identity binding only; never record the actual SID.
        return [Security.Principal.WindowsIdentity]::GetCurrent().User.Value
    }
    return [string]$Identity
}
function Set-HardenedCodeAcl {param($Path,[switch]$Recursive) $script:events.Add('protect_preimage')}
function Assert-HardenedCodeAcl {param($Path,[switch]$Recursive)}
function Assert-NoReparsePoint {param($Path,$Purpose)
    if(-not ([IO.Path]::GetFullPath($Path)).StartsWith($script:root+'\',[StringComparison]::OrdinalIgnoreCase)){throw 'R4 escaped input root'}
}
function Get-LabelSupportedStartupTasks {return @()}
function Get-FileSha256 {param($Path)
    Assert-NoReparsePoint $Path 'hash';return (Get-FileHash -LiteralPath $Path -Algorithm SHA256).Hash.ToLowerInvariant()
}
function New-Object {
    param($ComObject,$TypeName,$ArgumentList)
    if($ComObject -cne 'Schedule.Service'){throw 'R4 unmocked native object forbidden'}
    $service=[pscustomobject]@{};$service | Add-Member ScriptMethod Connect { }
    $service | Add-Member ScriptMethod GetFolder {
        param($Path)
        $folder=[pscustomobject]@{}
        $folder | Add-Member ScriptMethod GetTask {
            param($Name)
            $row=@($script:tasks | Where-Object {$_.TaskName -ieq $Name})[0]
            $task=[pscustomobject]@{Row=$row;Enabled=[bool]$row.Settings.Enabled;Xml=$row.Xml}
            $task | Add-Member ScriptMethod GetInstances {
                param($Flags)
                $script:events.Add('stop_readback')
                return [pscustomobject]@{Count=$(if($this.Row.State -ceq 'Running'){1}else{0})}
            }
            return $task
        }
        return $folder
    }
    return $service
}
$reason='';$identityAudit=$null;$transition=$null;$fingerprint=$null
try{
    if([string]$inputRecord.mode -ceq 'remove'){
        # Actual common removal helper, with unrelated current startup tasks absent.
        $directory=Join-Path $script:root 'official-legacy-relay'
        $folder=(New-Object -ComObject 'Schedule.Service').GetFolder('\')
        Remove-LabelScheduledTasks $folder $script:root (Join-Path $script:root 'code')
        $directory=Join-Path $script:root 'official-legacy-relay'
        $snapshot=Get-LabelOfficialLegacyTaskSnapshot
        $fingerprint=$snapshot.fingerprint
        $identityAudit=[ordered]@{matched=$fingerprint.matched;full_fingerprint=$fingerprint.sha256;field_fingerprints=$fingerprint.fields}
    }else{
        $snapshot=Get-LabelOfficialLegacyTaskSnapshot
        if($null -eq $snapshot){throw 'Healthy lifecycle task belongs to another command.'}
        $fingerprint=$snapshot.fingerprint
        $identityAudit=[ordered]@{matched=$fingerprint.matched;full_fingerprint=$fingerprint.sha256;field_fingerprints=$fingerprint.fields}
        if([string]$inputRecord.mode -ceq 'preflight'){
            $canonical=[Management.Automation.Language.Parser]::ParseFile((Join-Path $Repo 'INSTALL_CANONICAL_PORTABLE.ps1'),[ref]$tokens,[ref]$errors)
            $nodes=@($canonical.FindAll({param($n)$n -is [Management.Automation.Language.AssignmentStatementAst] -and $n.Left.Extent.Text -ceq '$legacyRelayBefore'},$true))
            if($nodes.Count -ne 1){throw 'R4 canonical legacy preflight assignment missing'}
            . ([scriptblock]::Create($nodes[0].Extent.Text))
        }else{
            Invoke-LabelOfficialLegacyTaskTransition 'Disable' $directory $snapshot.xml_sha256
            if([string]$inputRecord.mode -ceq 'rollback' -and [bool]$inputRecord.generation_allowed){
                Invoke-LabelOfficialLegacyTaskTransition 'Restore' $directory $snapshot.xml_sha256
            }elseif([string]$inputRecord.mode -ceq 'install'){
                # Read the actual canonical audit assignment, rather than fabricate its fingerprints.
                $legacyRelayBefore=$snapshot;$legacyDirectory=$directory;$audit=[ordered]@{}
                $canonical=[Management.Automation.Language.Parser]::ParseFile((Join-Path $Repo 'INSTALL_CANONICAL_PORTABLE.ps1'),[ref]$tokens,[ref]$errors)
                $auditNodes=@($canonical.FindAll({param($n)$n -is [Management.Automation.Language.AssignmentStatementAst] -and $n.Left.Extent.Text -ceq '$audit.legacy_relay_task_preimage'},$true))
                if($auditNodes.Count -ne 1){throw 'R4 canonical legacy identity audit assignment missing'}
                . ([scriptblock]::Create($auditNodes[0].Extent.Text))
                $auditFingerprint=$audit.legacy_relay_task_preimage.fingerprint
                $identityAudit=[ordered]@{matched=$auditFingerprint.matched;full_fingerprint=$auditFingerprint.sha256;field_fingerprints=$auditFingerprint.fields}
                # Actual canonical launch statement, after real isolated Disable.
                $nodes=@($canonical.FindAll({param($n)$n -is [Management.Automation.Language.AssignmentStatementAst] -and $n.Left.Extent.Text -ceq '$pidValue' -and $n.Right.Extent.Text.Contains('StartRaw')},$true))
                if($nodes.Count -ne 1){throw 'R4 canonical launch assignment missing'}
                $activeCodeRoot=Join-Path $script:root 'code';$install=$activeCodeRoot
                . ([scriptblock]::Create($nodes[0].Extent.Text))
            }
        }
    }
}catch{$reason=[string]$_.Exception.Message}
$afterFingerprint=$null
try{if($script:tasks.Count -eq 1){$afterFingerprint=Get-LabelOfficialLegacyTaskFingerprint $script:tasks[0].TaskName $script:tasks[0].TaskPath $script:tasks[0].Xml}}catch{}
$preimageSaved=Test-Path -LiteralPath (Join-Path $directory 'preimage.xml')
$metadata=$null;$preimageSha=$null
if($preimageSaved){
    $preimageSha=Get-FileSha256 (Join-Path $directory 'preimage.xml')
    if(Test-Path -LiteralPath (Join-Path $directory 'preimage.json')){
        $metadata=Get-Content -LiteralPath (Join-Path $directory 'preimage.json') -Raw -Encoding UTF8 | ConvertFrom-Json
    }
}
$transition=[ordered]@{preimage_saved=[bool]$preimageSaved;stop_confirmed=$script:events.Contains('stop_readback');
    metadata_fingerprint=$(if($null -ne $metadata){$metadata.fingerprint_sha256}else{$null});
    preimage_enabled=$(if($null -ne $metadata){[bool]$metadata.enabled}else{$null});preimage_xml_sha256=$preimageSha;
    full_fingerprint=$(if($null -ne $afterFingerprint){$afterFingerprint.sha256}else{$null});guidance_present=$false}
[ordered]@{seam_available=$true;reason_code=$reason;identity_audit=$identityAudit;transition=$transition;
    task_count=$script:tasks.Count;observed_count=$(if($null -ne $legacyRelayBefore){1}else{0});
    scheduler_queries=@($script:events | Where-Object {$_ -ceq 'query'}).Count;
    old_enabled=@($script:tasks | ForEach-Object {[bool]$_.Settings.Enabled});
    new_relay_enabled=$script:newRelayEnabled;events=$script:events.ToArray();
    register_calls=$script:registerCalls;delete_calls=$script:deleteCalls} |
    ConvertTo-Json -Depth 16 | Set-Content -LiteralPath $Output -Encoding UTF8
