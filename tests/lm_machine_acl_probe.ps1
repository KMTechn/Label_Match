param([string]$Repo,[string]$Root,[string]$Case)
$ErrorActionPreference='Stop'
trap {
    [IO.File]::WriteAllText((Join-Path $Root 'acl-probe-failure.json'),(@{type=$_.Exception.GetType().Name;line=$_.InvocationInfo.ScriptLineNumber;locations=@([regex]::Matches($_.ScriptStackTrace,'line ([0-9]+)')|ForEach-Object{[int]$_.Groups[1].Value})}|ConvertTo-Json -Compress))
    exit 1
}
. (Join-Path $Repo 'tools/bootstrap_integrity.ps1') -SharedCodeRoot $Repo
. (Join-Path $Repo 'tests/lm_machine_acl_fixture.ps1')
$Script:testProgramData=Join-Path $Root 'ProgramData'
$Script:testAnchor=Join-Path $Root 'trusted-anchor'
Set-TestMachineAcl $Script:testProgramData -ProgramData
Set-TestMachineAcl $Script:testAnchor
$parent=Join-Path $Script:testProgramData 'KMTech\Label_Match\install-audit'
[void][IO.Directory]::CreateDirectory($parent)
$fence=Join-Path $parent ('machine-writer-20261015T000000000Z-'+('a'*32))
Initialize-LabelMachineWriterRoot $fence
$leaf=Join-Path $fence 'active.json'
[IO.File]::WriteAllText($leaf,'{}')
$users=[Security.Principal.SecurityIdentifier]::new('S-1-5-32-545')
$nativeAcl=Microsoft.PowerShell.Security\Get-Acl -LiteralPath $Script:testProgramData
$rules=@($nativeAcl.GetAccessRules($true,$true,[Security.Principal.SecurityIdentifier])|Where-Object{$_.IdentityReference.Value -ceq $users.Value})
$fixtureRules=(@($rules|Where-Object{([int64]$_.FileSystemRights -band 0x116) -eq 0x116 -and [int]$_.InheritanceFlags -eq 1 -and [int]$_.PropagationFlags -eq 0}).Count -eq 1 -and
    @($rules|Where-Object{[int64]$_.FileSystemRights -eq 1179817 -and [int]$_.InheritanceFlags -eq 3 -and [int]$_.PropagationFlags -eq 0}).Count -eq 1)
if(-not $fixtureRules){throw 'PROGRAM_DATA_FIXTURE_DIFFERS'}
$shared=Join-Path $Script:testProgramData 'KMTech'
if($Case -like '*-owner-class'){
    $path=switch($Case){'programdata-owner-class'{$Script:testProgramData};'shared-owner-class'{$shared};'root-owner-class'{$fence};default{throw 'ACL_CASE_INVALID'}}
    $Script:testUntrustedOwnerPath=$path
}elseif($Case -eq 'root-inheritance'){
    $acl=Microsoft.PowerShell.Security\Get-Acl -LiteralPath $fence
    $acl.SetAccessRuleProtection($false,$true);Set-Acl -LiteralPath $fence -AclObject $acl
}elseif($Case -ne 'default'){
    $path=$shared
    $mask=switch($Case){
        'shared-delete'{0x10000};'shared-delete-child'{0x40};'shared-write-dac'{0x40000};'shared-write-owner'{0x80000}
        'root-write'{$path=$fence;2};'leaf-write'{$path=$leaf;2}
        default{throw 'ACL_CASE_INVALID'}
    }
    $acl=Microsoft.PowerShell.Security\Get-Acl -LiteralPath $path
    $acl.AddAccessRule([Security.AccessControl.FileSystemAccessRule]::new($users,[Security.AccessControl.FileSystemRights]$mask,'Allow'))
    Set-Acl -LiteralPath $path -AclObject $acl
}
$accepted=$false;$defaultDenied=$false
try{Assert-LabelProtectedInstallPath $leaf -MachineFenceRoot $fence;$accepted=$true}catch{}
try{Assert-LabelProtectedInstallPath $leaf}catch{$defaultDenied=$true}
[IO.File]::WriteAllText((Join-Path $Root 'acl-fixture.json'),(@{program_data=$Script:testProgramData;anchor=$Script:testAnchor;root=$fence;leaf=$leaf;untrusted_owner_path=$Script:testUntrustedOwnerPath;accepted=$accepted;default_denied=$defaultDenied;fixture_rule_match=$fixtureRules}|ConvertTo-Json -Compress))
