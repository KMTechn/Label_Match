param([string]$SharedCodeRoot = "")
Import-Module -Name (Join-Path $PSHOME 'Modules\Microsoft.PowerShell.Utility\Microsoft.PowerShell.Utility.psd1') -ErrorAction Stop
Import-Module -Name (Join-Path $PSHOME 'Modules\Microsoft.PowerShell.Security\Microsoft.PowerShell.Security.psd1') -ErrorAction Stop

function Get-LabelEmergencyFullPath([string]$Path,[string]$Label) {
    if(-not [IO.Path]::IsPathRooted($Path)){throw 'Emergency shortcut path must be absolute.'}
    return [IO.Path]::GetFullPath($Path).TrimEnd('\')
}

function Assert-LabelEmergencyPath([string]$Path,[string]$Label,[switch]$PathOnly) {
    $cursor=Get-LabelEmergencyFullPath $Path $Label
    while($cursor){
        if((Test-Path -LiteralPath $cursor) -and ((Get-Item -LiteralPath $cursor -Force).Attributes -band [IO.FileAttributes]::ReparsePoint)){
            throw 'Emergency shortcut path contains a reparse point.'
        }
        $cursor=[IO.Path]::GetDirectoryName($cursor)
    }
}

function Get-LabelEmergencyShortcutPath([string]$InstallRoot) {
    $root=Get-LabelEmergencyFullPath $InstallRoot 'emergency install root'
    $parent=Get-LabelEmergencyFullPath (Split-Path -Parent $root) 'emergency application folder'
    return Join-Path (Join-Path $parent '관리용') '포장 옛 화면 (비상).lnk'
}

function Test-LabelEmergencyShortcutDirectory([string]$Path) {
    $directory=Split-Path -Parent $Path
    Assert-LabelEmergencyPath $directory
    if(-not (Test-Path -LiteralPath $directory)){return $true}
    $item=Get-Item -LiteralPath $directory -Force -ErrorAction Stop
    if(-not $item.PSIsContainer -or ($item.Attributes -band [IO.FileAttributes]::ReparsePoint)){return $false}
    # Even an owned link is left alone when this directory contains foreign data.
    foreach($entry in @(Get-ChildItem -LiteralPath $directory -Force -ErrorAction Stop)){
        if($entry.FullName -ine $Path -or $entry.PSIsContainer -or
            ($entry.Attributes -band [IO.FileAttributes]::ReparsePoint)){return $false}
    }
    return $true
}

function Test-LabelEmergencyShortcutOwner($Link,[string]$Launcher) {
    return ([string]$Link.TargetPath -ieq $Launcher -and [string]$Link.Arguments -ceq '--ui tk' -and
        [string]$Link.Description -ceq 'KMTech.LabelMatch.EmergencyTk.v1')
}

function Install-LabelEmergencyShortcut([string]$InstallRoot) {
    $root=Get-LabelEmergencyFullPath $InstallRoot 'emergency install root'
    $launcher=Join-Path $root 'launch-label-match.cmd'
    $icon=(Join-Path $root 'app\assets\logo.ico')+',0'
    $path=Get-LabelEmergencyShortcutPath $root
    Assert-LabelEmergencyPath $root 'emergency shortcut root' -PathOnly
    if(-not [IO.File]::Exists($launcher)){throw 'Emergency shortcut entrypoint is absent.'}
    Assert-LabelEmergencyPath $launcher 'emergency shortcut entrypoint' -PathOnly
    if(-not (Test-LabelEmergencyShortcutDirectory $path)){
        Write-Warning '포장 비상 바로 가기나 관리용 폴더에 다른 파일이 있어 그대로 둡니다.'
        return [pscustomobject]@{status='PRESERVED_FOREIGN'}
    }
    $directory=Split-Path -Parent $path
    $shell=New-Object -ComObject WScript.Shell
    try{
        if(Test-Path -LiteralPath $path){
            Assert-LabelEmergencyPath $path 'emergency shortcut prestate' -PathOnly
            try{$link=$shell.CreateShortcut($path)}catch{
                Write-Warning '포장 비상 바로 가기는 다른 파일이어서 그대로 둡니다.'
                return [pscustomobject]@{status='PRESERVED_FOREIGN'}
            }
            if(-not (Test-LabelEmergencyShortcutOwner $link $launcher)){
                Write-Warning '포장 비상 바로 가기는 다른 파일이어서 그대로 둡니다.'
                return [pscustomobject]@{status='PRESERVED_FOREIGN'}
            }
            if([string]$link.WorkingDirectory -ieq $root -and [string]$link.IconLocation -ieq $icon -and $link.WindowStyle -eq 7){
                return [pscustomobject]@{status='RETAINED'}
            }
            $status='UPDATED'
        }else{
            $status='CREATED'
            [void][IO.Directory]::CreateDirectory($directory)
            Assert-LabelEmergencyPath $directory 'emergency shortcut directory' -PathOnly
            $link=$shell.CreateShortcut($path)
        }
        $link.TargetPath=$launcher;$link.Arguments='--ui tk';$link.WorkingDirectory=$root
        $link.IconLocation=$icon
        $link.WindowStyle=7;$link.Description='KMTech.LabelMatch.EmergencyTk.v1';$link.Save()
        $readback=$shell.CreateShortcut($path)
        if(-not [IO.File]::Exists($path) -or [string]$readback.TargetPath -ine $launcher -or
            [string]$readback.Arguments -cne '--ui tk' -or [string]$readback.WorkingDirectory -ine $root -or
            [string]$readback.IconLocation -ine $icon -or $readback.WindowStyle -ne 7 -or
            [string]$readback.Description -cne 'KMTech.LabelMatch.EmergencyTk.v1'){
            throw 'Emergency shortcut readback failed.'
        }
        return [pscustomobject]@{status=$status}
    }finally{[void][Runtime.InteropServices.Marshal]::FinalReleaseComObject($shell)}
}

function Remove-LabelEmergencyShortcut([string]$InstallRoot) {
    $root=Get-LabelEmergencyFullPath $InstallRoot 'emergency install root'
    $path=Get-LabelEmergencyShortcutPath $root
    if(-not (Test-LabelEmergencyShortcutDirectory $path)){
        Write-Warning '포장 비상 바로 가기나 관리용 폴더에 다른 파일이 있어 그대로 둡니다.';return
    }
    $directory=Split-Path -Parent $path
    if(-not (Test-Path -LiteralPath $path)){
        if([IO.Directory]::Exists($directory)){Remove-Item -LiteralPath $directory -ErrorAction Stop}
        return
    }
    Assert-LabelEmergencyPath $path 'emergency shortcut removal' -PathOnly
    $shell=New-Object -ComObject WScript.Shell
    try{
        try{$link=$shell.CreateShortcut($path)}catch{
            Write-Warning '포장 비상 바로 가기는 다른 파일이어서 그대로 둡니다.';return
        }
        if(-not (Test-LabelEmergencyShortcutOwner $link (Join-Path $root 'launch-label-match.cmd'))){
            Write-Warning '포장 비상 바로 가기는 다른 파일이어서 그대로 둡니다.';return
        }
        Remove-Item -LiteralPath $path -ErrorAction Stop
        if(Test-Path -LiteralPath $path){throw 'Emergency shortcut removal readback failed.'}
        if(@(Get-ChildItem -LiteralPath $directory -Force -ErrorAction Stop).Count -eq 0){Remove-Item -LiteralPath $directory -ErrorAction Stop}
    }finally{[void][Runtime.InteropServices.Marshal]::FinalReleaseComObject($shell)}
}


function Test-KmtechPrivilegedOwnerSid([string]$Sid) {
    return $Sid -cin @('S-1-5-18','S-1-5-32-544','S-1-5-80-956008885-3418522649-1831038044-1853292631-2271478464')
}

function Write-LabelAncestorPreparationEvent($Payload) {
    if ($null -eq $Script:LabelAncestorPreparationEvents) {
        $Script:LabelAncestorPreparationEvents = [Collections.Generic.List[object]]::new()
    }
    $Script:LabelAncestorPreparationEvents.Add($Payload)
}

function Get-InstallationPreparationExitCode($Failure) {
    $exception=if($Failure -is [Exception]){$Failure}else{$Failure.Exception}
    for($depth=0;$null -ne $exception -and $depth -lt 8;$depth++){
        if($exception.Data.Contains('installation_preparation_exit_code')){return [int]$exception.Data['installation_preparation_exit_code']}
        $exception=$exception.InnerException
    }
    return 66
}

function Get-InstallationPreparationDiagnostic([int]$ExitCode) {
    $diagnostics=@{
        61=@('UNSUPPORTED_ACL','상위 폴더 권한이 지원하는 기본값 또는 보호된 값과 다릅니다.')
        62=@('REPARSE_POINT','설치 경로에 재분석 지점이 있어 바탕을 준비할 수 없습니다.')
        63=@('UNTRUSTED_OWNER','설치 폴더 소유자를 신뢰할 수 없어 바탕을 준비할 수 없습니다.')
        64=@('APP_PARENT_CREATE','보호된 앱 폴더를 만들지 못했습니다.')
        65=@('CONTROL_CREATE','보호된 설치 제어·증거 폴더를 만들지 못했습니다.')
        66=@('OTHER','설치 바탕 준비 중 오류가 발생했습니다.')
    }
    $entry=$diagnostics[$ExitCode]
    if($null -eq $entry){return $null}
    return [pscustomobject]@{code=('INSTALLATION_PREPARATION_'+$entry[0]);message=($entry[1]+' 지원 담당자에게 문의하세요.');exit_code=$ExitCode}
}

function Protect-KmtechCodeAncestors([string]$CodeParent,[switch]$AdmissionOnly,[switch]$BeforeAppCreation,[string]$AncestorPath,[switch]$RequireProtected) {
    # Deliberately persistent, non-recursive protection of these two shared paths.
    # Never restore their old permissions as part of an app rollback/uninstall.
    if (-not ([IO.Path]::GetDirectoryName([IO.Path]::GetFullPath($CodeParent))).Equals('C:\KMTech\Apps',[StringComparison]::OrdinalIgnoreCase)) {
        throw 'Code parent must be a direct application directory under C:\KMTech\Apps.'
    }
    if(($AncestorPath -and $AncestorPath -notin @('C:\KMTech','C:\KMTech\Apps')) -or
        (($AncestorPath -or $RequireProtected) -and -not $AdmissionOnly)){
        throw 'Selected ancestor admission requires a literal shared path and read-only admission.'
    }
    $records=@(); $plans=@()
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
    $ancestorPaths=if($AncestorPath){@($AncestorPath)}else{@('C:\KMTech','C:\KMTech\Apps')}
    foreach ($path in $ancestorPaths) {
        $reason=''
        if (-not (Test-Path -LiteralPath $path)) {
            if($AdmissionOnly){
                $failure=[InvalidOperationException]::new("설치 폴더 권한을 확인하세요: $path (상위 폴더가 없습니다). 지원 담당자에게 문의하세요.")
                $failure.Data['installation_preparation_exit_code']=61;throw $failure
            }
            $ancestor=Split-Path -Parent $path
            $entry=Get-Item -Force -LiteralPath $ancestor -ErrorAction Stop
            if ($entry.Attributes -band [IO.FileAttributes]::ReparsePoint) { $reason='상위 폴더가 재분석 지점입니다' }
            else { [void][IO.Directory]::CreateDirectory($path) }
        }
        $entry=Get-Item -Force -LiteralPath $path -ErrorAction Stop
        if ($entry.Attributes -band [IO.FileAttributes]::ReparsePoint) { $reason='폴더가 재분석 지점입니다' }
        $acl=Get-Acl -LiteralPath $path -ErrorAction Stop
        $before=$acl.GetSecurityDescriptorSddlForm([Security.AccessControl.AccessControlSections]::Owner -bor [Security.AccessControl.AccessControlSections]::Group -bor [Security.AccessControl.AccessControlSections]::Access)
        $state=Get-KmtechSharedAncestorAclState $acl $entry.Attributes -RequireProtected:$RequireProtected
        $owner=$state.owner; $actualKey=$state.shape_key; $targetKey=$state.target_key
        $defaultShape=$state.default_shape; $targetShape=$state.target_shape
        if($state.reason){$reason=$state.reason}
        if($reason) {
            Write-LabelAncestorPreparationEvent ([ordered]@{event='ANCESTOR_ACL_REJECTED';path=$path;reason=$reason;before_sddl=$before;changed=$false})
            $failure=[InvalidOperationException]::new("설치 폴더 권한을 확인하세요: $path ($reason). 지원 담당자에게 문의하세요.")
            $failure.Data['canonical_code']='LABEL_PLACEMENT_ANCESTOR_TRUST_FAILED'
            $failure.Data['ancestor_trust_failure']=$failure.Message
            $failure.Data['installation_preparation_exit_code']=if($entry.Attributes -band [IO.FileAttributes]::ReparsePoint){62}elseif(-not(Test-KmtechPrivilegedOwnerSid $owner)){63}else{61}
            throw $failure
        }
        $plans+=@{path=$path;shape_key=$actualKey;owner=$owner;group=$acl.GetGroup([Security.Principal.SecurityIdentifier]).Value;
            protected=$acl.AreAccessRulesProtected;
            default_shape=$defaultShape;target_shape=$targetShape}
    }
    if($AdmissionOnly){return}
    # Both existing shapes are admitted before the first ACL write.
    foreach($plan in $plans) {
        $path=$plan.path
        $entry=Get-Item -Force -LiteralPath $path -ErrorAction Stop
        if($entry.Attributes -band [IO.FileAttributes]::ReparsePoint){throw 'Ancestor became a reparse point before protection.'}
        $fresh=Get-Acl -LiteralPath $path -ErrorAction Stop
        $freshSddl=$fresh.GetSecurityDescriptorSddlForm([Security.AccessControl.AccessControlSections]::Owner -bor [Security.AccessControl.AccessControlSections]::Group -bor [Security.AccessControl.AccessControlSections]::Access)
        # Our preceding parent write can reorder this child's inherited ACEs.
        # Reject changed permissions/identity/flags, then transform the fresh ACL.
        if((Get-KmtechSharedAncestorAclState $fresh $entry.Attributes).shape_key -cne $plan.shape_key -or
            $fresh.GetOwner([Security.Principal.SecurityIdentifier]).Value -cne $plan.owner -or
            $fresh.GetGroup([Security.Principal.SecurityIdentifier]).Value -cne $plan.group -or
            $fresh.AreAccessRulesProtected -ne $plan.protected){throw 'Ancestor ACL changed during protection admission.'}
        $acl=$fresh; $before=$freshSddl
        Write-LabelAncestorPreparationEvent ([ordered]@{event='ANCESTOR_ACL_BEFORE';path=$path;before_sddl=$before;already_target=$plan.target_shape;intentional_persistent_change=$true})
        if($plan.default_shape) {
            $acl.SetAccessRuleProtection($true,$true)
            # .NET can retain the inherited flag until persistence. Convert each
            # copied ACE explicitly without remapping its generic/inherit-only mask.
            $raw=[Security.AccessControl.RawSecurityDescriptor]::new($acl.GetSecurityDescriptorBinaryForm(),0)
            foreach($ace in $raw.DiscretionaryAcl){$ace.AceFlags=[Security.AccessControl.AceFlags]([int]$ace.AceFlags -band (-bnot [int][Security.AccessControl.AceFlags]::Inherited))}
            $binary=[byte[]]::new($raw.BinaryLength);$raw.GetBinaryForm($binary,0)
            $acl.SetSecurityDescriptorBinaryForm($binary,[Security.AccessControl.AccessControlSections]::Access)
            $self=@($acl.GetAccessRules($true,$true,[Security.Principal.SecurityIdentifier]) | Where-Object {
                $_.IdentityReference.Value -ceq 'S-1-5-11' -and [int64]$_.FileSystemRights -eq 1245631 -and [int]$_.InheritanceFlags -eq 0 -and [int]$_.PropagationFlags -eq 0
            })
            if($self.Count -ne 1){throw 'Ancestor ACL copied self-entry readback failed.'}
            $acl.RemoveAccessRuleSpecific($self[0])
            $acl.AddAccessRule([Security.AccessControl.FileSystemAccessRule]::new(
                [Security.Principal.SecurityIdentifier]::new('S-1-5-11'),
                [Security.AccessControl.FileSystemRights]1179823,'None','None','Allow'))
            Set-Acl -LiteralPath $path -AclObject $acl -ErrorAction Stop
        }
        $afterAcl=Get-Acl -LiteralPath $path -ErrorAction Stop
        $after=$afterAcl.GetSecurityDescriptorSddlForm([Security.AccessControl.AccessControlSections]::Owner -bor [Security.AccessControl.AccessControlSections]::Group -bor [Security.AccessControl.AccessControlSections]::Access)
        $afterRules=@($afterAcl.GetAccessRules($true,$true,[Security.Principal.SecurityIdentifier]))
        $afterKey=(Get-KmtechSharedAncestorAclState $afterAcl $entry.Attributes).shape_key
        if(-not(Test-KmtechPrivilegedOwnerSid ($afterAcl.GetOwner([Security.Principal.SecurityIdentifier]).Value)) -or $afterAcl.GetOwner([Security.Principal.SecurityIdentifier]).Value -cne $plan.owner -or -not $afterAcl.AreAccessRulesProtected -or $afterRules.Count -ne 5 -or $afterKey -cne $targetKey){throw 'Ancestor ACL protected readback failed.'}
        $record=[ordered]@{path=$path;before_sddl=$before;after_sddl=$after;changed=$plan.default_shape;intentional_persistent_change=$true}
        $records+=$record
        Write-LabelAncestorPreparationEvent ([ordered]@{event='ANCESTOR_ACL_AFTER';record=$record})
    }
    # The app directory is the guarded leaf; Apps/KMTech are higher ancestors,
    # where direct creation remains allowed. This also rejects user-owned squatters.
    try { if(-not $BeforeAppCreation){Assert-LabelProtectedInstallPath $CodeParent} }
    catch {
        # Preserve the original trust exception as InnerException.
        $failure=[InvalidOperationException]::new("설치 폴더 권한을 확인하세요: $CodeParent (코드 경로의 신뢰 재검사가 실패했습니다). 지원 담당자에게 문의하세요.",$_.Exception)
        $failure.Data['canonical_code']='LABEL_PLACEMENT_ANCESTOR_TRUST_FAILED'
        $failure.Data['ancestor_trust_failure']=$failure.Message
        throw $failure
    }
    return [pscustomobject]@{status='VERIFIED';records=$records;rollback_restores_ancestor_permissions=$false}
}

function Get-LabelMachineProgramDataRoot {
    return [IO.Path]::GetFullPath([Environment]::GetFolderPath('CommonApplicationData')).TrimEnd('\')
}

function Get-LabelMachineFencePathScope([string]$MachineFenceRoot) {
    $programData=Get-LabelMachineProgramDataRoot
    $boundary=[IO.Path]::GetFullPath($MachineFenceRoot).TrimEnd('\')
    $expected=Join-Path $programData 'KMTech\Label_Match\install-audit'
    if([IO.Path]::GetDirectoryName($boundary) -ine $expected -or
        [IO.Path]::GetFileName($boundary) -cnotmatch '^machine-writer-[0-9]{8}T[0-9]{9}Z-[0-9a-f]{32}$'){
        throw 'MACHINE_FENCE_PATH_INVALID'
    }
    return @{program_data=$programData;root=$boundary}
}

function Initialize-LabelMachineWriterRoot([string]$ControlRoot) {
    $scope=Get-LabelMachineFencePathScope $ControlRoot
    $parent=[IO.Path]::GetDirectoryName($scope.root)
    Assert-LabelProtectedInstallPath $parent -MachineFenceRoot $scope.root
    if(Test-Path -LiteralPath $scope.root){throw 'MACHINE_FENCE_ROOT_ALREADY_EXISTS'}
    [void](New-Item -ItemType Directory -Path $scope.root -ErrorAction Stop)
    $acl=Get-Acl -LiteralPath $scope.root -ErrorAction Stop
    if(-not(Test-KmtechPrivilegedOwnerSid ($acl.GetOwner([Security.Principal.SecurityIdentifier]).Value))){throw 'MACHINE_FENCE_OWNER_INVALID'}
    $protected=[Security.AccessControl.DirectorySecurity]::new()
    $protected.SetOwner($acl.GetOwner([Security.Principal.SecurityIdentifier]))
    $protected.SetAccessRuleProtection($true,$false)
    foreach($sid in @('S-1-5-18','S-1-5-32-544')){
        $protected.AddAccessRule([Security.AccessControl.FileSystemAccessRule]::new(
            [Security.Principal.SecurityIdentifier]::new($sid),'FullControl','ContainerInherit,ObjectInherit','None','Allow'))
    }
    Set-Acl -LiteralPath $scope.root -AclObject $protected -ErrorAction Stop
    if(@(Get-ChildItem -Force -LiteralPath $scope.root -ErrorAction Stop).Count){throw 'MACHINE_FENCE_ROOT_NOT_EMPTY'}
    Assert-LabelProtectedInstallPath $scope.root -MachineFenceRoot $scope.root
}

function Assert-LabelProtectedInstallPath([string]$Path,[switch]$AllowSupportedPreparation,[string]$MachineFenceRoot='') {
    $machineScope=if($MachineFenceRoot){Get-LabelMachineFencePathScope $MachineFenceRoot}else{$null}
    $full=[IO.Path]::GetFullPath($Path);$leaf=$true
    if($null -ne $machineScope -and $full.TrimEnd('\') -ine $machineScope.root -and
        -not $full.StartsWith(([string]$machineScope.root)+'\',[StringComparison]::OrdinalIgnoreCase) -and
        -not ([string]$machineScope.root).StartsWith($full.TrimEnd('\')+'\',[StringComparison]::OrdinalIgnoreCase)){
        throw 'MACHINE_FENCE_PATH_INVALID'
    }
    if($full -ne [IO.Path]::GetPathRoot($full)){$full=$full.TrimEnd([char[]]'\/')}
    while($full) {
        $shared=$full -in @('C:\KMTech','C:\KMTech\Apps')
        if($shared){
            # Ordinary preparation can inspect an absent base without creating it;
            # every actual admission of an existing ancestor uses the same shape.
            if(-not ($AllowSupportedPreparation -and -not(Test-Path -LiteralPath $full))){
                [void](Protect-KmtechCodeAncestors 'C:\KMTech\Apps\Label_Match' -AdmissionOnly -AncestorPath $full -RequireProtected:(-not $AllowSupportedPreparation))
            }
            $full=[IO.Path]::GetDirectoryName($full);$leaf=$false;continue
        }
        if($AllowSupportedPreparation -and -not(Test-Path -LiteralPath $full) -and
            $full -ieq 'C:\KMTech\Apps\Label_Match'){
            $full=[IO.Path]::GetDirectoryName($full);$leaf=$false;continue
        }
        $entry=Get-Item -Force -LiteralPath $full -ErrorAction Stop
        if($entry.Attributes -band [IO.FileAttributes]::ReparsePoint){
            $failure=[InvalidOperationException]::new('설치 경로 에 재분석 지점이 있습니다.')
            $failure.Data['installation_preparation_exit_code']=62;throw $failure
        }
        $acl=Get-Acl -LiteralPath $full -ErrorAction Stop
        if(-not(Test-KmtechPrivilegedOwnerSid ($acl.GetOwner([Security.Principal.SecurityIdentifier]).Value))){
            $failure=[InvalidOperationException]::new('설치 경로 소유자를 신뢰할 수 없습니다.')
            $failure.Data['installation_preparation_exit_code']=63;throw $failure
        }
        $mask=if($leaf){0x500D0156}else{0x500D0150}
        if($null -ne $machineScope){
            $aboveProgramData=([string]$machineScope.program_data).StartsWith($full.TrimEnd('\')+'\',[StringComparison]::OrdinalIgnoreCase)
            $insideFence=$full.StartsWith(([string]$machineScope.root)+'\',[StringComparison]::OrdinalIgnoreCase)
            if($full -ieq $machineScope.program_data -or $aboveProgramData){$mask=0}
            elseif($full -ieq $machineScope.root -or $insideFence){
                $mask=if($leaf -or $full -ieq $machineScope.root){0x500D0156}else{0x500D0150}
                if($full -ieq $machineScope.root -and -not $acl.AreAccessRulesProtected){throw 'MACHINE_FENCE_DACL_UNPROTECTED'}
            }else{$mask=0x500D0040}
        }
        foreach($rule in $acl.GetAccessRules($true,$true,[Security.Principal.SecurityIdentifier])) {
            if($rule.AccessControlType -eq 'Allow' -and
                -not($rule.PropagationFlags -band [Security.AccessControl.PropagationFlags]::InheritOnly) -and
                $rule.IdentityReference.Value -cnotin @('S-1-5-18','S-1-5-32-544') -and
                ([int64]$rule.FileSystemRights -band $mask)){
                $failure=[InvalidOperationException]::new('설치 경로 권한에 일반 사용자 쓰기가 있습니다.')
                $failure.Data['installation_preparation_exit_code']=61;throw $failure
            }
        }
        if($full -eq [IO.Path]::GetPathRoot($full)){break}
        $full=[IO.Path]::GetDirectoryName($full);$leaf=$false
    }
}

$BootstrapIntegrityFileName = "bootstrap-integrity.json"
$BootstrapIntegritySchema = "label-match-bootstrap-integrity-v1"
$BootstrapPortableCodeRoot = "."

# Bootstrap v1 ordering/aggregate contract (also consumed by Python onboarding
# and the canonical installer's expected bootstrap digest): relative POSIX paths,
# case-sensitive ordinal UTF-16 code-unit order, without culture/case folding or
# Unicode normalization. Hash UTF-8 (no BOM) rows: "sha256 size path\n", including
# the final LF. The stored row order must be this same canonical order.
function Sort-BootstrapInventory {
    param([Parameter(Mandatory = $true)][AllowEmptyCollection()][object[]]$Inventory)
    [object[]]$ordered = @($Inventory)
    [Array]::Sort(
        $ordered,
        [System.Collections.Generic.Comparer[object]]::Create(
            [System.Comparison[object]]{
                param($left, $right)
                return [StringComparer]::Ordinal.Compare([string]$left.path, [string]$right.path)
            }
        )
    )
    return $ordered
}

function Get-RequiredExternalInteger($Object, [string]$Name) {
    if ($null -eq $Object) { throw "External object is absent: $Name" }
    if ($Object -is [Collections.IDictionary]) {
        if (-not $Object.Contains($Name)) { throw "External integer is absent: $Name" }
        $value = $Object[$Name]
    }
    else {
        $property = $Object.PSObject.Properties[$Name]
        if ($null -eq $property) { throw "External integer is absent: $Name" }
        $value = $property.Value
    }
    if ($value -isnot [int] -and $value -isnot [long]) {
        throw "External integer has invalid type: $Name"
    }
    return [int64]$value
}

function Get-BootstrapStrictFullPath {
    param(
        [Parameter(Mandatory = $true)][string]$Path,
        [Parameter(Mandatory = $true)][string]$Purpose
    )
    return Get-KmtechBootstrapStrictFullPath $Path $Purpose
}

function Get-BootstrapFileSha256 {
    param([Parameter(Mandatory = $true)][string]$Path)
    $stream = [IO.File]::OpenRead($Path)
    $sha = [Security.Cryptography.SHA256]::Create()
    try {
        return ([BitConverter]::ToString($sha.ComputeHash($stream))).Replace('-', '').ToLowerInvariant()
    }
    finally {
        $sha.Dispose()
        $stream.Dispose()
    }
}

function Get-BootstrapRelativeCodePath {
    param(
        [Parameter(Mandatory = $true)][string]$Root,
        [Parameter(Mandatory = $true)][string]$Path
    )
    return Get-KmtechBootstrapRelativeCodePath $Root $Path
}

function Get-BootstrapCodeInventory {
    param(
        [Parameter(Mandatory = $true)][string]$Root,
        [string]$IntegrityFileName = $BootstrapIntegrityFileName
    )
    $rootFull = Get-BootstrapStrictFullPath $Root "code root"
    $result = @()
    foreach ($file in @(Get-ChildItem -LiteralPath $rootFull -File -Force -Recurse)) {
        $relative = Get-BootstrapRelativeCodePath $rootFull $file.FullName
        if ($relative.Equals($IntegrityFileName, [StringComparison]::OrdinalIgnoreCase)) {
            continue
        }
        $result += [pscustomobject][ordered]@{
            path = $relative
            size = [int64]$file.Length
            sha256 = Get-BootstrapFileSha256 $file.FullName
        }
    }
    return Sort-BootstrapInventory -Inventory $result
}

function Get-BootstrapInventoryAggregate {
    param([Parameter(Mandatory = $true)][object[]]$Inventory)
    return Get-KmtechBootstrapInventoryAggregate -Inventory $Inventory
}

function Write-BootstrapUtf8Json {
    param(
        [Parameter(Mandatory = $true)][string]$Path,
        [Parameter(Mandatory = $true)]$Payload
    )
    $temporary = "$Path.tmp.$PID"
    $json = $Payload | ConvertTo-Json -Depth 8
    if (-not (Get-Command Assert-KmtechMachineMetadataText -ErrorAction SilentlyContinue)) {
        . (Join-Path $PSScriptRoot 'failure_redaction.ps1')
    }
    Assert-KmtechMachineMetadataText -Text ($json + [Environment]::NewLine)
    [IO.File]::WriteAllText($temporary, $json + [Environment]::NewLine, (New-Object Text.UTF8Encoding($false)))
    if (-not (Get-Command Invoke-LabelPinnedAtomicReplace -ErrorAction SilentlyContinue)) {
        . (Join-Path $PSScriptRoot 'label_writer_fence.ps1')
    }
    try {
        if ([IO.File]::Exists($Path)) { Invoke-LabelPinnedAtomicReplace $temporary $Path -Mode Replace }
        else { Invoke-LabelPinnedAtomicReplace $temporary $Path -Mode Move }
    } finally {
        if ([IO.File]::Exists($temporary)) { [IO.File]::Delete($temporary) }
    }
}

function Write-BootstrapIntegrityRecord {
    param(
        [Parameter(Mandatory = $true)][string]$Root,
        [Parameter(Mandatory = $true)][string]$CodeRoot,
        [object[]]$Inventory = @(),
        [string]$InstalledAt = "",
        [string]$IntegrityFileName = $BootstrapIntegrityFileName,
        [string]$IntegritySchema = $BootstrapIntegritySchema
    )
    $rootFull = Get-BootstrapStrictFullPath $Root "code root"
    if ($CodeRoot -cne $BootstrapPortableCodeRoot -and -not [IO.Path]::IsPathRooted($CodeRoot)) {
        throw "bootstrap integrity code root must be absolute or the portable package marker."
    }
    $selectedInventory = @($Inventory)
    if ($selectedInventory.Count -eq 0) {
        $selectedInventory = @(Get-BootstrapCodeInventory -Root $rootFull -IntegrityFileName $IntegrityFileName)
    }
    if ($selectedInventory.Count -eq 0) {
        throw "Frozen release code inventory is empty."
    }
    $selectedInventory = @(Sort-BootstrapInventory -Inventory $selectedInventory)
    $aggregate = Get-BootstrapInventoryAggregate -Inventory $selectedInventory
    $record = [ordered]@{
        schema_version = $IntegritySchema
        status = 'PASS'
        code_root = $CodeRoot
        installed_at = if ([string]::IsNullOrWhiteSpace($InstalledAt)) {
            (Get-Date).ToUniversalTime().ToString('o')
        } else {
            $InstalledAt
        }
        file_count = $selectedInventory.Count
        aggregate_sha256 = $aggregate
        files = $selectedInventory
        identity_profile_created = $false
        state_scope = 'current_user_first_run'
    }
    Write-BootstrapUtf8Json -Path (Join-Path $rootFull $IntegrityFileName) -Payload $record
    return [pscustomobject]$record
}

function Assert-BootstrapIntegrityRecord {
    param(
        [Parameter(Mandatory = $true)][string]$Root,
        [string]$IntegrityFileName = $BootstrapIntegrityFileName,
        [string]$IntegritySchema = $BootstrapIntegritySchema
    )
    $rootFull = Get-BootstrapStrictFullPath $Root "bootstrap integrity root"
    $recordPath = Join-Path $rootFull $IntegrityFileName
    if (-not (Test-Path -LiteralPath $recordPath -PathType Leaf)) {
        throw "Bootstrap integrity record is absent."
    }
    $record = Get-Content -LiteralPath $recordPath -Raw -Encoding UTF8 | ConvertFrom-Json
    if (
        [string]$record.schema_version -cne $IntegritySchema -or
        [string]$record.status -cne 'PASS'
    ) {
        throw "Bootstrap integrity record schema or status is invalid."
    }
    if ([string]$record.code_root -ceq $BootstrapPortableCodeRoot) {
        $declaredCodeRoot = $rootFull
    }
    else {
        $declaredCodeRoot = Get-BootstrapStrictFullPath `
            ([string]$record.code_root) `
            "bootstrap integrity code root"
    }
    if (-not $declaredCodeRoot.Equals($rootFull, [StringComparison]::OrdinalIgnoreCase)) {
        throw "Bootstrap integrity record code root is invalid."
    }
    $inventory = @(
        Get-BootstrapCodeInventory -Root $rootFull -IntegrityFileName $IntegrityFileName
    )
    $recordFileCount = Get-RequiredExternalInteger $record 'file_count'
    if (
        $recordFileCount -ne $inventory.Count -or
        @($record.files).Count -ne $inventory.Count
    ) {
        throw "Bootstrap integrity record file count is invalid."
    }
    $actualByPath = @{}
    foreach ($actual in $inventory) {
        $actualPath = [string]$actual.path
        if ($actualByPath.ContainsKey($actualPath)) {
            throw "Bootstrap integrity actual inventory contains duplicate paths."
        }
        $actualByPath[$actualPath] = $actual
    }
    for ($index = 0; $index -lt @($record.files).Count; $index += 1) {
        $expected = @($record.files)[$index]
        $expectedPath = [string]$expected.path
        if ($expectedPath -cne [string]$inventory[$index].path) {
            throw "Bootstrap integrity inventory order is invalid at index $index."
        }
        if (-not $actualByPath.ContainsKey($expectedPath)) {
            throw "Bootstrap integrity inventory is missing the recorded path at index $index."
        }
        $actual = $actualByPath[$expectedPath]
        if (
            [string]$expected.path -cne [string]$actual.path -or
            [int64]$expected.size -ne [int64]$actual.size -or
            [string]$expected.sha256 -cne [string]$actual.sha256
        ) {
            throw "Bootstrap integrity inventory differs at index $index."
        }
        $actualByPath.Remove($expectedPath)
    }
    if ($actualByPath.Count -ne 0) {
        throw "Bootstrap integrity actual inventory contains unrecorded paths."
    }
    $aggregate = Get-BootstrapInventoryAggregate -Inventory @($record.files)
    if ([string]$record.aggregate_sha256 -cne $aggregate) {
        throw "Bootstrap integrity aggregate is invalid."
    }
    $frozenMainCount = @(
        $inventory | Where-Object { [string]$_.path -ieq 'Label_Match.exe' }
    ).Count
    $portablePythonCount = @(
        $inventory | Where-Object { [string]$_.path -ieq 'runtime/pythonw.exe' }
    ).Count
    $portableMainCount = @(
        $inventory | Where-Object { [string]$_.path -ieq 'app/main.py' }
    ).Count
    $frozenLayout = $frozenMainCount -eq 1
    $portableLayout = $portablePythonCount -eq 1 -and $portableMainCount -eq 1
    if ($frozenLayout -eq $portableLayout) {
        throw "Bootstrap integrity record does not identify exactly one supported release layout."
    }
    return [pscustomobject]@{
        status = 'PASS'
        record_path = $recordPath
        file_count = $inventory.Count
        aggregate_sha256 = $aggregate
    }
}

# This small trust bootstrap is intentionally local to both standalone loaders.
# Never use shared code (or start Python) to establish its own execution authority.
function Get-LabelSharedPortableLeafPath([string]$CodeRoot, [ValidateSet('portable.ps1','data_generation.ps1')][string]$Leaf = 'portable.ps1') {
    if ([string]::IsNullOrWhiteSpace($CodeRoot) -or -not [IO.Path]::IsPathRooted($CodeRoot) -or
        $CodeRoot.StartsWith('\\?\') -or $CodeRoot.StartsWith('\\.\')) {
        throw 'Shared PowerShell code root must be an ordinary absolute path.'
    }
    $root = [IO.Path]::GetFullPath($CodeRoot)
    # Inspect ancestors before probing app/, including links above the code root.
    $ancestor = $root
    while ($ancestor) {
        $item = Get-Item -LiteralPath $ancestor -Force
        if (($item.Attributes -band [IO.FileAttributes]::ReparsePoint) -ne 0) {
            throw "Shared PowerShell path contains a reparse point: $ancestor"
        }
        $ancestor = Split-Path -Path $ancestor -Parent
    }
    $appRoot = if (Test-Path -LiteralPath (Join-Path $root 'app') -PathType Container) {
        Join-Path $root 'app'
    } else { $root }
    foreach ($relative in @('', 'kmtech_shared', 'kmtech_shared\powershell',
        'kmtech_shared.lock.json', 'kmtech_shared.manifest.json', ('kmtech_shared\powershell\' + $Leaf))) {
        $path = if ($relative) { Join-Path $appRoot $relative } else { $appRoot }
        $item = Get-Item -LiteralPath $path -Force
        if (($item.Attributes -band [IO.FileAttributes]::ReparsePoint) -ne 0) {
            throw "Shared PowerShell path contains a reparse point: $path"
        }
    }
    $expectedManifestSha = '3ff5ea08f684f0c35e008ab1b3d816b03d69e3b6d4b72e34b70ba0db11e765a7'
    $lockPath = Join-Path $appRoot 'kmtech_shared.lock.json'
    if ((Get-Item -LiteralPath $lockPath).Length -gt 65536) { throw 'Shared consumer lock is oversized.' }
    $lock = Get-Content -LiteralPath $lockPath -Raw -Encoding UTF8 | ConvertFrom-Json
    if ([string]$lock.manifest_sha256 -cne $expectedManifestSha) {
        throw 'Shared consumer manifest pin mismatch.'
    }
    $manifestPath = Join-Path $appRoot 'kmtech_shared.manifest.json'
    if ((Get-Item -LiteralPath $manifestPath).Length -gt 65536) { throw 'Shared manifest is oversized.' }
    [byte[]]$manifestBytes = [IO.File]::ReadAllBytes($manifestPath)
    $hash = [Security.Cryptography.SHA256]::Create()
    try {
        $actualManifestSha = ([BitConverter]::ToString($hash.ComputeHash($manifestBytes))).Replace('-', '').ToLowerInvariant()
    } finally { $hash.Dispose() }
    if ($actualManifestSha -cne $expectedManifestSha) { throw 'Shared manifest pin mismatch.' }
    $pinnedSharedManifest = (New-Object Text.UTF8Encoding($false, $true)).GetString($manifestBytes) | ConvertFrom-Json
    if ([string]$lock.version -cne [string]$pinnedSharedManifest.version) {
        throw 'Shared consumer manifest pin mismatch.'
    }
    $sharedLeaf = Join-Path $appRoot ('kmtech_shared\powershell\' + $Leaf)
    $expectedLeafSha = $pinnedSharedManifest.files.('kmtech_shared/powershell/' + $Leaf)
    $stream = [IO.File]::OpenRead($sharedLeaf)
    $hash = [Security.Cryptography.SHA256]::Create()
    try {
        $actualLeafSha = ([BitConverter]::ToString($hash.ComputeHash($stream))).Replace('-', '').ToLowerInvariant()
    } finally { $hash.Dispose(); $stream.Dispose() }
    if ($actualLeafSha -cne $expectedLeafSha) {
        throw 'Shared PowerShell leaf pin mismatch.'
    }
    return $sharedLeaf
}

# The native early SHA remains usable without module auto-loading. Full inventory
# consumers explicitly provide their trusted helper closure's code root.
if ($SharedCodeRoot) { . (Get-LabelSharedPortableLeafPath $SharedCodeRoot) }
