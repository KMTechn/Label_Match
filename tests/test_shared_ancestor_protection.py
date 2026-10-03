"""Exercise the two-folder operation with memory admission and native inheritance."""
import os
from pathlib import Path
import uuid

import pytest

import shutil
import subprocess

def run_functions(tmp_path, source, names, script, *, values=None, engine='powershell.exe'):
    executable = shutil.which(engine)
    if not executable:
        pytest.skip(f'{engine} is required')
    driver = tmp_path / 'contract.ps1'
    driver.write_text(r"""
$ErrorActionPreference='Stop'
[Console]::OutputEncoding=[Text.UTF8Encoding]::new($false)
$tokens=$null;$errors=$null
$ast=[Management.Automation.Language.Parser]::ParseFile($env:LM_CONTRACT_SOURCE,[ref]$tokens,[ref]$errors)
if($errors.Count){throw 'Product PowerShell parse failed.'}
foreach($dependency in @('ConvertTo-LabelTaskSid')){
 $definitions=@($ast.FindAll({param($n) $n -is [Management.Automation.Language.FunctionDefinitionAst] -and $n.Name -ceq $dependency},$true))
 if($definitions.Count -eq 1){. ([scriptblock]::Create($definitions[0].Extent.Text))}
}
foreach($name in $env:LM_CONTRACT_NAMES.Split(',')){
 $definitions=@($ast.FindAll({param($n) $n -is [Management.Automation.Language.FunctionDefinitionAst] -and $n.Name -ceq $name},$true))
 if($definitions.Count -ne 1){throw ('Product function missing: '+$name)}
 . ([scriptblock]::Create($definitions[0].Extent.Text))
}
""" + script, encoding='utf-8-sig')
    environment = os.environ.copy()
    environment.update({'LM_CONTRACT_SOURCE': str(source), 'LM_CONTRACT_NAMES': ','.join(names)})
    environment.update(values or {})
    return subprocess.run([executable, '-NoLogo', '-NoProfile', '-NonInteractive', '-File', str(driver)],
                          cwd=tmp_path, env=environment, capture_output=True, encoding='utf-8',
                          creationflags=getattr(subprocess, 'CREATE_NO_WINDOW', 0))

ROOT = Path(__file__).resolve().parents[1]


def test_shipped_installers_keep_utf8_bom_for_windows_powershell_korean_errors():
    for relative in ('INSTALL_THIS_PC.ps1', 'INSTALL_CANONICAL_PORTABLE.ps1', 'tools/bootstrap_integrity.ps1'):
        assert (ROOT / relative).read_bytes().startswith(b'\xef\xbb\xbf'), relative


def test_windows_powershell_decodes_actual_shared_rejection_literal(tmp_path):
    result = run_functions(tmp_path, ROOT/'tools/bootstrap_integrity.ps1', ['Protect-KmtechCodeAncestors'], r'''
$tokens=$null;$errors=$null
$ast=[Management.Automation.Language.Parser]::ParseFile($env:LM_REAL_HELPER,[ref]$tokens,[ref]$errors)
if($errors.Count){throw 'Actual shipped helper parse failed.'}
$functions=@($ast.FindAll({param($node)$node -is [Management.Automation.Language.FunctionDefinitionAst] -and $node.Name -ceq 'Protect-KmtechCodeAncestors'},$true))
if($functions.Count -ne 1){throw 'Actual shipped rejection function missing.'}
. ([scriptblock]::Create($functions[0].Extent.Text))
function Test-Path {param($LiteralPath) return $true}
function Get-Item {param($LiteralPath,$ErrorAction) return [pscustomobject]@{Attributes=[IO.FileAttributes]::Directory}}
function Get-Acl {param($LiteralPath,$ErrorAction)
 $acl=[Security.AccessControl.DirectorySecurity]::new()
 $acl.SetSecurityDescriptorSddlForm('O:S-1-5-21-1-2-3-1000G:BAD:AI(A;OICIID;FA;;;BA)(A;OICIID;FA;;;SY)(A;OICIID;0x1200a9;;;BU)(A;ID;0x1301bf;;;AU)(A;OICIIOID;0xe0010000;;;AU)')
 return $acl
}
function Set-Acl {throw 'Rejected shape performed an ACL write.'}
function Write-LabelAncestorPreparationEvent {param($Payload)}
$rejected=$false
try{[void](Protect-KmtechCodeAncestors 'C:\KMTech\Apps\Label_Match')}
catch{
 $rejected=$true
 $expected='설치 폴더 권한을 확인하세요: C:\KMTech (폴더 소유자가 예상과 다릅니다). 지원 담당자에게 문의하세요.'
 if($_.Exception.Message -cne $expected){throw 'Actual PS5 file decoding changed the Korean rejection.'}
}
if(-not $rejected){throw 'User-owned ancestor was admitted.'}
'PASS'
''', engine='powershell.exe', values={'LM_REAL_HELPER': str(ROOT/'tools/bootstrap_integrity.ps1')})
    assert result.returncode == 0, result.stdout + result.stderr


@pytest.mark.parametrize('engine', ('powershell.exe', 'pwsh.exe'))
@pytest.mark.parametrize('scenario', ('both_default', 'parent_target', 'child_target', 'changed_set',
                                      'user_owner', 'deny', 'extra_sid', 'partial', 'reparse'))
def test_shared_protection_with_native_ntfs_inheritance(tmp_path, engine, scenario):
    # The task may keep native ACL inputs on D: while the repository runner uses
    # its short-path basetemp. No shared/installed folder is an input or a target.
    native_parent = Path(os.environ.get('LM_NTFS_TEST_ROOT', str(tmp_path)))
    native_root = native_parent / ('n-' + uuid.uuid4().hex[:8])
    result = run_functions(tmp_path, ROOT/'tools/bootstrap_integrity.ps1', ['Protect-KmtechCodeAncestors'], r'''
$ErrorActionPreference='Stop'
$script:nativeRoot=[IO.Path]::GetFullPath($env:LM_NATIVE_ROOT)
$script:mapped=@{'C:\KMTech'=Join-Path $script:nativeRoot 'KMTech';'C:\KMTech\Apps'=Join-Path $script:nativeRoot 'KMTech\Apps'}
$script:writes=@();$script:events=@();$script:raceInjected=$false
$script:propagatedChildSddl=$null
function Native-Path([string]$Path){
 if(-not $script:mapped.ContainsKey($Path)){throw 'Unmapped production path in native ACL test.'}
 return $script:mapped[$Path]
}
function Write-LabelAncestorPreparationEvent {param($Payload) $script:events+=,$Payload}
function Write-InstallationCaughtFailure {param($Failure)}
function Test-Path {param($LiteralPath) Microsoft.PowerShell.Management\Test-Path -LiteralPath (Native-Path $LiteralPath)}
function Get-Item {param($LiteralPath,$ErrorAction) Microsoft.PowerShell.Management\Get-Item -LiteralPath (Native-Path $LiteralPath) -ErrorAction Stop}
function Get-Acl {param($LiteralPath,$ErrorAction)
 $native=Native-Path $LiteralPath
 if($env:LM_NATIVE_SCENARIO -ceq 'changed_set' -and $LiteralPath -ceq 'C:\KMTech\Apps' -and $script:writes.Count -eq 1 -and -not $script:raceInjected){
  $changed=Microsoft.PowerShell.Security\Get-Acl -LiteralPath $native
  $changed.AddAccessRule([Security.AccessControl.FileSystemAccessRule]::new([Security.Principal.SecurityIdentifier]::new('S-1-1-0'),'Read','Allow'))
  Microsoft.PowerShell.Security\Set-Acl -LiteralPath $native -AclObject $changed
  $script:raceInjected=$true
 }
 $acl=Microsoft.PowerShell.Security\Get-Acl -LiteralPath $native
 # The worker is not elevated. Only owner admission is translated; the DACL,
 # ACE order, inheritance flags and persistence are the real NTFS values.
 $ownerSid=if($env:LM_NATIVE_SCENARIO -ceq 'user_owner' -and $LiteralPath -ceq 'C:\KMTech\Apps'){'S-1-5-21-1-2-3-1000'}else{'S-1-5-32-544'}
 $acl.SetOwner([Security.Principal.SecurityIdentifier]::new($ownerSid))
 return $acl
}
function Set-Acl {param($LiteralPath,$AclObject,$ErrorAction)
 $native=Native-Path $LiteralPath
 $owner=(Microsoft.PowerShell.Security\Get-Acl -LiteralPath $native).GetOwner([Security.Principal.SecurityIdentifier])
 $AclObject.SetOwner($owner)
 Microsoft.PowerShell.Security\Set-Acl -LiteralPath $native -AclObject $AclObject -ErrorAction Stop
 $script:writes+=,$LiteralPath
 if($LiteralPath -ceq 'C:\KMTech'){$script:propagatedChildSddl=(Microsoft.PowerShell.Security\Get-Acl -LiteralPath $script:mapped['C:\KMTech\Apps']).Sddl}
}
function Effective-Key([string]$Path){
 $acl=Microsoft.PowerShell.Security\Get-Acl -LiteralPath $Path
 $rights=@{}
 foreach($rule in $acl.GetAccessRules($true,$true,[Security.Principal.SecurityIdentifier])){
  if($rule.PropagationFlags -band [Security.AccessControl.PropagationFlags]::InheritOnly){continue}
  $key=$rule.IdentityReference.Value+'|'+$rule.AccessControlType
  $rights[$key]=[int64]$rights[$key] -bor [int64]$rule.FileSystemRights
 }
 return ($acl.GetOwner([Security.Principal.SecurityIdentifier]).Value+';'+$acl.GetGroup([Security.Principal.SecurityIdentifier]).Value+';'+(@($rights.Keys | Sort-Object | ForEach-Object {$_+'|'+$rights[$_]}) -join ';'))
}
# Apply the lab C:\ inheritable ACEs BEFORE creating its descendants. Creating
# them first would pre-propagate the reordered ACEs and conceal the defect.
# The current owner has self-only FullControl on the artificial volume root.
[void][IO.Directory]::CreateDirectory($script:nativeRoot)
$rootAcl=Microsoft.PowerShell.Security\Get-Acl -LiteralPath $script:nativeRoot
$sid=$rootAcl.GetOwner([Security.Principal.SecurityIdentifier]).Value
# CommonSecurityDescriptor sorts explicit allow ACEs. Persist the raw root
# descriptor instead so its inherited order is the one actually seen in C:\.
Add-Type -TypeDefinition @'
using System; using System.Runtime.InteropServices;
public static class NativeAncestorFixture {
 [DllImport("advapi32.dll", CharSet=CharSet.Unicode, SetLastError=true)]
 [return: MarshalAs(UnmanagedType.Bool)]
 public static extern bool SetFileSecurityW(string path, uint information, byte[] descriptor);
}
'@
$rawRoot=[Security.AccessControl.RawSecurityDescriptor]::new(('O:'+$sid+'G:BAD:P(A;;FA;;;'+$sid+')(A;OICI;FA;;;BA)(A;OICI;FA;;;SY)(A;OICI;0x1200a9;;;BU)(A;OICIIO;SDGXGWGR;;;AU)'))
$rootBytes=[byte[]]::new($rawRoot.BinaryLength);$rawRoot.GetBinaryForm($rootBytes,0)
if(-not [NativeAncestorFixture]::SetFileSecurityW($script:nativeRoot,2147483652,$rootBytes)){throw [ComponentModel.Win32Exception]::new([Runtime.InteropServices.Marshal]::GetLastWin32Error())}
foreach($relative in @('KMTech\Apps\Legacy\Nested','KMTech\Sibling')){[void][IO.Directory]::CreateDirectory((Join-Path $script:nativeRoot $relative))}
$preserved=@('KMTech\Apps\Legacy','KMTech\Apps\Legacy\Nested','KMTech\Apps\Legacy\Nested\old.txt','KMTech\Sibling','KMTech\Sibling\sibling.txt') | ForEach-Object {Join-Path $script:nativeRoot $_}
[IO.File]::WriteAllText($preserved[2],'old');[IO.File]::WriteAllText($preserved[4],'sibling')
foreach($path in $script:mapped.Values){
 $rules=@((Microsoft.PowerShell.Security\Get-Acl -LiteralPath $path).GetAccessRules($true,$true,[Security.Principal.SecurityIdentifier]))
 if($rules.Count -ne 5 -or @($rules | Where-Object {-not $_.IsInherited}).Count){throw 'Native fixture did not produce the lab inherited five-ACE shape.'}
}
$before=@{};foreach($path in $preserved){$before[$path]=Effective-Key $path}
$initialChildSddl=(Microsoft.PowerShell.Security\Get-Acl -LiteralPath $script:mapped['C:\KMTech\Apps']).Sddl
$nativeRefusal=$env:LM_NATIVE_SCENARIO -in @('user_owner','deny','extra_sid','partial','reparse')
if($env:LM_NATIVE_SCENARIO -in @('deny','extra_sid','partial')){
 $acl=Microsoft.PowerShell.Security\Get-Acl -LiteralPath $script:mapped['C:\KMTech\Apps']
 $sid=if($env:LM_NATIVE_SCENARIO -ceq 'extra_sid'){'S-1-1-0'}else{'S-1-5-11'}
 $type=if($env:LM_NATIVE_SCENARIO -ceq 'deny'){'Deny'}else{'Allow'}
 $acl.AddAccessRule([Security.AccessControl.FileSystemAccessRule]::new([Security.Principal.SecurityIdentifier]::new($sid),'WriteData',$type))
 Microsoft.PowerShell.Security\Set-Acl -LiteralPath $script:mapped['C:\KMTech\Apps'] -AclObject $acl
}
if($env:LM_NATIVE_SCENARIO -ceq 'reparse'){
 $saved=Join-Path $script:nativeRoot 'KMTech\SavedApps'
 [IO.Directory]::Move($script:mapped['C:\KMTech\Apps'],$saved)
 [void](Microsoft.PowerShell.Management\New-Item -ItemType Junction -Path $script:mapped['C:\KMTech\Apps'] -Target $saved)
}
$refusalBefore=@{}
foreach($literal in $script:mapped.Keys){$refusalBefore[$literal]=(Microsoft.PowerShell.Security\Get-Acl -LiteralPath (Native-Path $literal)).Sddl}
if($env:LM_NATIVE_SCENARIO -in @('parent_target','child_target')){
 $literal=if($env:LM_NATIVE_SCENARIO -ceq 'parent_target'){'C:\KMTech'}else{'C:\KMTech\Apps'}
 $acl=Get-Acl -LiteralPath $literal
 $acl.SetSecurityDescriptorSddlForm('D:P(A;OICI;FA;;;BA)(A;OICI;FA;;;SY)(A;OICI;0x1200a9;;;BU)(A;;0x1200af;;;AU)(A;OICIIO;SDGXGWGR;;;AU)',[Security.AccessControl.AccessControlSections]::Access)
 Set-Acl -LiteralPath $literal -AclObject $acl
 $script:writes=@()
}
$failed=$false
try{$result=Protect-KmtechCodeAncestors 'C:\KMTech\Apps\Label_Match' -BeforeAppCreation}
catch{
 $failed=$true
 if($nativeRefusal){
  if(-not $_.Exception.Message.Contains('설치 폴더 권한을 확인하세요')){throw 'Native rejection lacks Korean folder/reason.'}
  $expectedCode=if($env:LM_NATIVE_SCENARIO -ceq 'user_owner'){63}elseif($env:LM_NATIVE_SCENARIO -ceq 'reparse'){62}else{61}
  if($_.Exception.Data['installation_preparation_exit_code'] -ne $expectedCode){throw 'Native rejection code differs.'}
 }elseif($env:LM_NATIVE_SCENARIO -cne 'changed_set' -or -not $_.Exception.Message.Contains('Ancestor ACL changed during protection admission.')){throw}
}
if($nativeRefusal){
 if(-not $failed -or $script:writes.Count){throw 'Native rejected shape was written/admitted.'}
 foreach($literal in $script:mapped.Keys){if($refusalBefore[$literal] -cne (Microsoft.PowerShell.Security\Get-Acl -LiteralPath (Native-Path $literal)).Sddl){throw 'Rejected native ACL changed.'}}
}elseif($env:LM_NATIVE_SCENARIO -ceq 'changed_set'){
 if(-not $failed -or -not $script:raceInjected -or $script:writes.Count -ne 1){throw 'External permission change was not rejected before the child write.'}
}else{
 if($failed -or $result.status -cne 'VERIFIED'){throw 'Native two-folder protection did not succeed.'}
 $expectedWrites=if($env:LM_NATIVE_SCENARIO -ceq 'both_default'){2}else{1}
 if($script:writes.Count -ne $expectedWrites){throw 'Intermediate-state protection wrote the wrong folders.'}
 if($env:LM_NATIVE_SCENARIO -ceq 'both_default' -and $initialChildSddl -ceq $script:propagatedChildSddl){throw 'Native fixture did not reproduce inherited ACE reordering.'}
 foreach($path in $preserved){if($before[$path] -cne (Effective-Key $path)){throw ('Descendant effective permissions changed: '+$path)}}
 # Real filesystem writes remain possible for the same normal token.
 [IO.File]::AppendAllText($preserved[2],'-kept')
 $oldFolder=Join-Path $script:nativeRoot 'KMTech\Apps\Legacy\Nested'
 [IO.Directory]::Move($oldFolder,($oldFolder+'-renamed'))
 $firstCount=$script:writes.Count
 $again=Protect-KmtechCodeAncestors 'C:\KMTech\Apps\Label_Match' -BeforeAppCreation
 if($script:writes.Count -ne $firstCount -or @($again.records | Where-Object {$_.changed}).Count){throw 'Native repeat changed a protected ACL.'}
 if(@($script:events | Where-Object {$_.event -ceq 'ANCESTOR_ACL_BEFORE'}).Count -ne 4){throw 'Before/after SDDL evidence is missing.'}
}
[IO.File]::WriteAllText((Join-Path $script:nativeRoot 'proof.json'),([ordered]@{scenario=$env:LM_NATIVE_SCENARIO;initial_child_sddl=$initialChildSddl;propagated_child_sddl=$script:propagatedChildSddl;writes=$script:writes;race_rejected=$failed;owner_seam=$true;events=$script:events} | ConvertTo-Json -Depth 12))
'PASS'
''', engine=engine, values={'LM_NATIVE_ROOT': str(native_root), 'LM_NATIVE_SCENARIO': scenario})
    assert result.returncode == 0, result.stdout + result.stderr


@pytest.mark.parametrize('engine', ('powershell.exe', 'pwsh.exe'))
@pytest.mark.parametrize('scenario', ('default', 'target', 'lab_intermediate', 'user_owner', 'deny', 'extra_sid', 'partial', 'reparse', 'post_guard_failure'))
def test_shared_protection_admits_only_reviewed_shapes_and_keeps_creation(tmp_path, engine, scenario):
    result = run_functions(tmp_path, ROOT/'tools/bootstrap_integrity.ps1', ['Protect-KmtechCodeAncestors'], r'''
$ErrorActionPreference='Stop'
$script:events=@();$script:writes=@();$script:acls=@{}
function Write-LabelAncestorPreparationEvent {param($Payload) $script:events+=,$Payload}
function Write-InstallationCaughtFailure {param($Failure)}
function Test-Path {param($LiteralPath) return $true}
function Get-Item {param($LiteralPath,$ErrorAction) return [pscustomobject]@{Attributes=if($env:LM_SHAPE_SCENARIO -ceq 'reparse' -and $LiteralPath -ceq 'C:\KMTech\Apps'){[IO.FileAttributes]::ReparsePoint}else{[IO.FileAttributes]::Directory}}}
function Get-Acl {param($LiteralPath,$ErrorAction)
 $copy=[Security.AccessControl.DirectorySecurity]::new()
 $copy.SetSecurityDescriptorBinaryForm($script:acls[$LiteralPath].GetSecurityDescriptorBinaryForm())
 return $copy
}
function Set-Acl {param($LiteralPath,$AclObject,$ErrorAction)
 if($LiteralPath -notin @('C:\KMTech','C:\KMTech\Apps')){throw 'Nonliteral target was written.'}
 $script:writes+=,$LiteralPath;$script:acls[$LiteralPath]=$AclObject
}
function Assert-LabelProtectedInstallPath {param($Path)
 if($Path -cne 'C:\KMTech\Apps\Label_Match'){throw 'Wrong guarded leaf.'}
 if($env:LM_SHAPE_SCENARIO -ceq 'post_guard_failure'){throw 'injected actual guard rejection'}
 foreach($acl in $script:acls.Values){
  foreach($rule in $acl.GetAccessRules($true,$true,[Security.Principal.SecurityIdentifier])){
   if($rule.IdentityReference.Value -notin @('S-1-5-18','S-1-5-32-544') -and -not ($rule.PropagationFlags -band 2) -and ([int64]$rule.FileSystemRights -band 0x500D0150)){throw 'Guard-denied bits remain.'}
  }
 }
}
foreach($path in @('C:\KMTech','C:\KMTech\Apps')){
 $acl=[Security.AccessControl.DirectorySecurity]::new()
 $sddl='O:BAG:BAD:AI(A;OICIID;FA;;;BA)(A;OICIID;FA;;;SY)(A;OICIID;0x1200a9;;;BU)(A;ID;0x1301bf;;;AU)(A;OICIIOID;0xe0010000;;;AU)'
 if($env:LM_SHAPE_SCENARIO -ceq 'target'){$sddl='O:BAG:BAD:P(A;OICI;FA;;;BA)(A;OICI;FA;;;SY)(A;OICI;0x1200a9;;;BU)(A;;0x1200af;;;AU)(A;OICIIO;0xe0010000;;;AU)'}
 # Exact before/after SDDLs from sub-121's native failure: protected parent,
 # inherited child with unchanged five ACEs in the propagated order.
 if($env:LM_SHAPE_SCENARIO -ceq 'lab_intermediate'){
  $sddl=if($path -ceq 'C:\KMTech'){'O:BAG:S-1-5-21-2506378611-1465194981-818886254-513D:PAI(A;OICIIO;SDGXGWGR;;;AU)(A;;0x1200af;;;AU)(A;OICI;FA;;;SY)(A;OICI;FA;;;BA)(A;OICI;0x1200a9;;;BU)'}else{'O:BAG:S-1-5-21-2506378611-1465194981-818886254-513D:AI(A;ID;0x1301bf;;;AU)(A;OICIIOID;SDGXGWGR;;;AU)(A;OICIID;FA;;;SY)(A;OICIID;FA;;;BA)(A;OICIID;0x1200a9;;;BU)'}
 }
 if($path -ceq 'C:\KMTech\Apps'){
  switch($env:LM_SHAPE_SCENARIO){
   'user_owner' {$sddl=$sddl.Replace('O:BA','O:S-1-5-21-1-2-3-1000')}
   'deny' {$sddl=$sddl.Replace('D:AI','D:AI(D;;WD;;;AU)')}
   'extra_sid' {$sddl += '(A;;WD;;;WD)'}
   'partial' {$sddl=$sddl.Replace('0x1301bf','0x1200af')}
  }
 }
 $acl.SetSecurityDescriptorSddlForm($sddl);$script:acls[$path]=$acl
}
$before=@{};foreach($path in $script:acls.Keys){$before[$path]=$script:acls[$path].GetSecurityDescriptorSddlForm([Security.AccessControl.AccessControlSections]::All)}
$failed=$false
try{$result=Protect-KmtechCodeAncestors 'C:\KMTech\Apps\Label_Match'}catch{$failed=$true;if(-not $_.Exception.Message.Contains('설치 폴더 권한을 확인하세요')){throw ('Plain-language rejection missing: '+$_.Exception.Message)}}
$expectedFail=$env:LM_SHAPE_SCENARIO -notin @('default','target','lab_intermediate')
if($failed -ne $expectedFail){throw 'Shape admission result differs.'}
if($env:LM_SHAPE_SCENARIO -in @('user_owner','deny','extra_sid','partial','reparse')){
 if($script:writes.Count){throw 'Unexpected second-folder shape changed an earlier folder.'}
 foreach($path in $script:acls.Keys){if($before[$path] -cne $script:acls[$path].GetSecurityDescriptorSddlForm([Security.AccessControl.AccessControlSections]::All)){throw 'Rejected ACL changed in memory.'}}
} elseif($env:LM_SHAPE_SCENARIO -ceq 'target'){
 if($script:writes.Count){throw 'Idempotent target was rewritten.'}
} else {
 $expectedWrites=if($env:LM_SHAPE_SCENARIO -ceq 'lab_intermediate'){1}else{2}
 if($script:writes.Count -ne $expectedWrites){throw 'Expected only the admitted default folders to be written.'}
 foreach($acl in $script:acls.Values){
  if(-not $acl.AreAccessRulesProtected){throw 'Ancestor inheritance remains active.'}
  $rules=@($acl.GetAccessRules($true,$true,[Security.Principal.SecurityIdentifier]))
  $self=@($rules | Where-Object {$_.IdentityReference.Value -ceq 'S-1-5-11' -and [int]$_.PropagationFlags -eq 0})
  $child=@($rules | Where-Object {$_.IdentityReference.Value -ceq 'S-1-5-11' -and [int]$_.PropagationFlags -eq 2})
  if($rules.Count -ne 5 -or $self.Count -ne 1 -or [int64]$self[0].FileSystemRights -ne 1179823 -or ([int64]$self[0].FileSystemRights -band 6) -ne 6 -or $child.Count -ne 1 -or [int64]$child[0].FileSystemRights -ne -536805376){throw 'B rights or child inheritance changed.'}
 }
 if($env:LM_SHAPE_SCENARIO -in @('default','lab_intermediate')){
  $second=Protect-KmtechCodeAncestors 'C:\KMTech\Apps\Label_Match'
  if($script:writes.Count -ne $expectedWrites -or @($second.records | Where-Object {$_.changed}).Count){throw 'Repeated B operation changed permissions.'}
 }
}
'PASS'
''', engine=engine, values={'LM_SHAPE_SCENARIO': scenario})
    assert result.returncode == 0, result.stdout + result.stderr
