"""Actual placement/daily admission on owned NTFS; no installer or task execution."""
import json
import os
from pathlib import Path
import uuid

import pytest

from tests.test_shared_ancestor_protection import run_functions

ROOT = Path(__file__).resolve().parents[1]


@pytest.mark.parametrize('engine', ('powershell.exe', 'pwsh.exe'))
@pytest.mark.parametrize('entry,scenario', [
    (entry, scenario)
    for entry in ('shared_parent', 'shared_apps', 'placement', 'startup', 'daily',
                  'standalone', 'standalone_parent', 'standalone_apps')
    for scenario in ('target', 'default', 'extra_write', 'extra_read', 'user_owner', 'reparse', 'system_owner', 'trusted_owner')
] + [('other_leaf', 'target'), ('admission', 'missing_parent'),
     ('admission', 'missing_apps'), ('selected_admission', 'missing_apps'),
     ('standalone_other', 'target'), ('standalone', 'missing_apps'),
     ('standalone_parent', 'missing_parent')])
def test_shared_ancestor_admission_actual_calls(tmp_path, engine, entry, scenario):
    native_root = Path(os.environ.get('LM_NTFS_TEST_ROOT', str(tmp_path))) / ('a-'+uuid.uuid4().hex[:8])
    result = run_functions(tmp_path, ROOT/'INSTALL_THIS_PC.ps1',
                           ['Assert-ProtectedCodeAncestors', 'Assert-LabelStartupUpdateTrust'], r'''
$helperAst=[Management.Automation.Language.Parser]::ParseFile($env:LM_ADMISSION_HELPER,[ref]$tokens,[ref]$errors)
if($errors.Count){throw 'Bootstrap parse failed.'}
foreach($name in $(if($env:LM_ADMISSION_ENTRY -like 'standalone*'){@()}else{@('Protect-KmtechCodeAncestors','Assert-LabelProtectedInstallPath','Write-LabelAncestorPreparationEvent')})){
 $definition=@($helperAst.FindAll({param($n)$n -is [Management.Automation.Language.FunctionDefinitionAst] -and $n.Name -ceq $name},$true))
 if($definition.Count -ne 1){throw 'Expected actual ancestor predicate.'}
 $body=$definition[0].Extent.Text
 if($name -ceq 'Protect-KmtechCodeAncestors'){
  # A missing-path regression must create only in the owned fixture, and fail
  # the directory-set check. Translate the .NET creation argument as well as
  # cmdlet paths; otherwise that regression could touch the real C: base.
  if(-not $body.Contains('[IO.Directory]::CreateDirectory($path)')){throw 'Native creation path seam changed.'}
  $body=$body.Replace('[IO.Directory]::CreateDirectory($path)','[IO.Directory]::CreateDirectory((Native-Path $path))')
 }
 . ([scriptblock]::Create($body))
}
$dailyAst=[Management.Automation.Language.Parser]::ParseFile($env:LM_ADMISSION_DAILY,[ref]$tokens,[ref]$errors)
if($errors.Count){throw 'Daily parse failed.'}
$definition=@($dailyAst.FindAll({param($n)$n -is [Management.Automation.Language.FunctionDefinitionAst] -and $n.Name -ceq 'Assert-Protected'},$true))
. ([scriptblock]::Create($definition[0].Extent.Text))
$nativeRoot=[IO.Path]::GetFullPath($env:LM_ADMISSION_ROOT)
$application='C:\KMTech\Apps\Label_Match'
$current=$application+'\current'
$testedAncestor=if($env:LM_ADMISSION_ENTRY -in @('shared_parent','standalone_parent')){'C:\KMTech'}else{'C:\KMTech\Apps'}
$mapped=@{'C:\'=$nativeRoot;'C:\KMTech'=Join-Path $nativeRoot 'KMTech';
 'C:\KMTech\Apps'=Join-Path $nativeRoot 'KMTech\Apps';
 'C:\KMTech\Apps\Label_Match'=Join-Path $nativeRoot 'KMTech\Apps\Label_Match';
 'D:\SameShape'=Join-Path $nativeRoot 'SameShape';'D:\'=Join-Path $nativeRoot 'OtherRoot'}
[void][IO.Directory]::CreateDirectory($nativeRoot)
$missing=$env:LM_ADMISSION_SCENARIO -like 'missing_*'
if(-not $missing){foreach($path in $mapped.Values){[void][IO.Directory]::CreateDirectory($path)}}
elseif($env:LM_ADMISSION_SCENARIO -ceq 'missing_apps'){[void][IO.Directory]::CreateDirectory($mapped['C:\KMTech'])}
if($env:LM_ADMISSION_SCENARIO -ceq 'reparse'){
 $redirected=$mapped[$testedAncestor];[IO.Directory]::Move($redirected,($redirected+'-original'))
 Microsoft.PowerShell.Management\New-Item -ItemType Junction -Path $redirected -Target ($redirected+'-original') | Out-Null
}
Add-Type -TypeDefinition @'
using System; using System.Runtime.InteropServices;
public static class LabelAdmissionFixture {
 [DllImport("advapi32.dll", CharSet=CharSet.Unicode, SetLastError=true)]
 [return: MarshalAs(UnmanagedType.Bool)]
 public static extern bool SetFileSecurityW(string path, uint info, byte[] descriptor);
}
'@
$originalAcls=@()
foreach($native in @($mapped.Values | Sort-Object -Unique)){
 if(-not [IO.Directory]::Exists($native)){continue}
 $acl=Microsoft.PowerShell.Security\Get-Acl -LiteralPath $native -ErrorAction Stop
 $originalAcls+=@{path=$native;binary=$acl.GetSecurityDescriptorBinaryForm();sddl=$acl.Sddl;
  protected=$acl.AreAccessRulesProtected;
  owner=$acl.GetOwner([Security.Principal.SecurityIdentifier]).Value;
  group=$acl.GetGroup([Security.Principal.SecurityIdentifier]).Value}
}
try{
$fixtureSid=[Security.Principal.WindowsIdentity]::GetCurrent().User.Value
$target='D:P(A;OICI;FA;;;BA)(A;OICI;FA;;;SY)(A;OICI;0x1200a9;;;BU)(A;;0x1200af;;;AU)(A;OICIIO;SDGXGWGR;;;AU)'
$leaf='D:P(A;OICI;FA;;;BA)(A;OICI;FA;;;SY)(A;OICI;0x1200a9;;;BU)'
foreach($literal in @('C:\','C:\KMTech','C:\KMTech\Apps',$application,'D:\SameShape','D:\')){
 $native=$mapped[$literal]
 if(-not [IO.Directory]::Exists($native)){continue}
 $sddl=if($literal -in @('C:\KMTech','C:\KMTech\Apps','D:\SameShape')){$target}else{$leaf}
 if($literal -in @('C:\','D:\')){$sddl+='(A;;FA;;;'+$fixtureSid+')'}
 $information=[uint32]2147483652
 if($literal -ceq $testedAncestor){
  switch($env:LM_ADMISSION_SCENARIO){
   'default' {$sddl='D:AI(A;OICIID;FA;;;BA)(A;OICIID;FA;;;SY)(A;OICIID;0x1200a9;;;BU)(A;ID;0x1301bf;;;AU)(A;OICIIOID;SDGXGWGR;;;AU)';$information=[uint32]536870916}
   'extra_write' {$sddl+='(A;;WD;;;WD)'}
   'extra_read' {$sddl+='(A;;RC;;;WD)'}
  }
 }
 $raw=[Security.AccessControl.RawSecurityDescriptor]::new($sddl)
 $binary=[byte[]]::new($raw.BinaryLength);$raw.GetBinaryForm($binary,0)
 if(-not [LabelAdmissionFixture]::SetFileSecurityW($native,$information,$binary)){
  throw [ComponentModel.Win32Exception]::new([Runtime.InteropServices.Marshal]::GetLastWin32Error())
 }
}
function Native-Path([string]$Path){
 $key=$Path.TrimEnd('\');if($key -in @('C:','D:')){$key+='\'}
 if(-not $mapped.ContainsKey($key)){throw ('Unmapped production path: '+$Path)}
 return $mapped[$key]
}
function Test-Path {param($LiteralPath) Microsoft.PowerShell.Management\Test-Path -LiteralPath (Native-Path $LiteralPath)}
function Get-Item {param($LiteralPath,$ErrorAction) Microsoft.PowerShell.Management\Get-Item -LiteralPath (Native-Path $LiteralPath) -ErrorAction Stop}
function Get-Acl {param($LiteralPath,$ErrorAction)
 $acl=Microsoft.PowerShell.Security\Get-Acl -LiteralPath (Native-Path $LiteralPath)
 if($LiteralPath -in @('C:\','D:\')){
  foreach($rule in @($acl.GetAccessRules($true,$false,[Security.Principal.SecurityIdentifier]))){
   if($rule.IdentityReference.Value -ceq $fixtureSid -and [int]$rule.InheritanceFlags -eq 0){$acl.RemoveAccessRuleSpecific($rule)}
  }
 }
 # Only owner reading is translated: actual owner/DACL persistence is preserved.
 $owner=switch($env:LM_ADMISSION_SCENARIO){
 'user_owner' {if($LiteralPath.TrimEnd('\') -ceq $testedAncestor){'S-1-5-21-1-2-3-1000'}else{'S-1-5-32-544'}}
 'system_owner' {'S-1-5-18'}
 'trusted_owner' {'S-1-5-80-956008885-3418522649-1831038044-1853292631-2271478464'}
 default {'S-1-5-32-544'}
}
 $acl.SetOwner([Security.Principal.SecurityIdentifier]::new($owner));return $acl
}
function Set-Acl {throw 'Admission attempted an ACL write.'}
function New-Item {throw 'Admission attempted directory creation.'}
$script:strictCodeChecks=0
function Assert-HardenedCodeAcl {param($Path,[switch]$Recursive)
 if($Path -cne $current -or -not $Recursive){throw 'Startup code-tree check changed.'}
 $script:strictCodeChecks++
}
$before=@{}
foreach($literal in $mapped.Keys){if([IO.Directory]::Exists($mapped[$literal])){$before[$literal]=(Microsoft.PowerShell.Security\Get-Acl -LiteralPath $mapped[$literal]).Sddl}}
$beforePaths=@(Microsoft.PowerShell.Management\Get-ChildItem -LiteralPath $nativeRoot -Recurse | ForEach-Object {$_.FullName})
$failed=$false;$failure=$null;$calls=0
try{
 switch($env:LM_ADMISSION_ENTRY){
  {$_ -like 'standalone*'} {
   foreach($name in @('Protect-KmtechCodeAncestors','Assert-LabelProtectedInstallPath','Write-LabelAncestorPreparationEvent')){
    if(Get-Command $name -CommandType Function -ErrorAction SilentlyContinue){throw 'Standalone loaded a helper.'}
   }
   $path=switch($env:LM_ADMISSION_ENTRY){
    'standalone_parent' {'C:\KMTech'}
    'standalone_apps' {'C:\KMTech\Apps\'}
    'standalone_other' {'D:\SameShape'}
    default {$application}
   }
   Assert-ProtectedCodeAncestors $path;$calls=1
  }
  'shared_parent' {Assert-LabelProtectedInstallPath 'C:\KMTech';$calls=1}
  'shared_apps' {Assert-LabelProtectedInstallPath 'C:\KMTech\Apps\';$calls=1}
  'placement' {
   # Run the actual final trust statement from the installation preparation body.
   $initialize=@($ast.FindAll({param($n)$n -is [Management.Automation.Language.FunctionDefinitionAst] -and $n.Name -ceq 'Initialize-LabelProtectedInstallBase'},$true))
   $call=@($initialize[0].FindAll({param($n)$n -is [Management.Automation.Language.CommandAst] -and $n.GetCommandName() -ceq 'Assert-ProtectedCodeAncestors'},$true))
   if($call.Count -ne 1){throw 'Placement call coverage changed.'}
   . ([scriptblock]::Create($call[0].Extent.Text));$calls=1
  }
  'startup' {Assert-LabelStartupUpdateTrust $current;$calls=1}
  'daily' {
   # The initial unmodified strict code trust precedes the validated helper load.
   $script:LabelStartupBootstrapLoaded=$true
   $gates=@($dailyAst.EndBlock.Statements | Where-Object {$_.Extent.Text -cmatch '^\[void\]\(Protect-KmtechCodeAncestors \$trustedApplication -AdmissionOnly -RequireProtected\)$'})
   $trustedApplication=$application
   if($gates.Count){. ([scriptblock]::Create($gates[0].Extent.Text))}
   # Run the real top-level application call as well as its actual function.
   $call=@($dailyAst.EndBlock.Statements | Where-Object {$_.Extent.Text -ceq 'Assert-Protected $trustedApplication'})
   if($call.Count -ne 1){throw 'Actual daily trust call changed.'}
   . ([scriptblock]::Create($call[0].Extent.Text));$calls=1
  }
  'other_leaf' {Assert-LabelProtectedInstallPath 'D:\SameShape';$calls=1}
  'admission' {Protect-KmtechCodeAncestors $application -AdmissionOnly;$calls=1}
  'selected_admission' {Protect-KmtechCodeAncestors $application -AdmissionOnly -AncestorPath 'C:\KMTech\Apps' -RequireProtected;$calls=1}
 }
}catch{$failed=$true;$failure=$_}
$expectedFailure=$env:LM_ADMISSION_SCENARIO -notin @('target','system_owner','trusted_owner') -or $env:LM_ADMISSION_ENTRY -in @('other_leaf','standalone_other')
if($failed -ne $expectedFailure){throw ('Actual caller admitted/rejected wrong NTFS shape: '+$env:LM_ADMISSION_ENTRY+'/'+$env:LM_ADMISSION_SCENARIO+' failure='+$failure)}
if($expectedFailure -and $script:strictCodeChecks){throw 'Rejected ancestor reached startup operations.'}
if($expectedFailure -and $env:LM_ADMISSION_ENTRY -eq 'selected_admission' -and $failure.Exception.Message -match 'parameter cannot be found'){throw 'Missing admission was rejected only by a missing test parameter.'}
foreach($literal in $before.Keys){if($before[$literal] -cne (Microsoft.PowerShell.Security\Get-Acl -LiteralPath $mapped[$literal]).Sddl){throw 'Admission changed native owner/DACL.'}}
$afterPaths=@(Microsoft.PowerShell.Management\Get-ChildItem -LiteralPath $nativeRoot -Recurse | ForEach-Object {$_.FullName})
if(($beforePaths -join '|') -cne ($afterPaths -join '|')){throw 'Read-only admission created an absent path.'}
[IO.File]::WriteAllText((Microsoft.PowerShell.Management\Join-Path $nativeRoot 'proof.json'),(@{entry=$env:LM_ADMISSION_ENTRY;scenario=$env:LM_ADMISSION_SCENARIO;calls=$calls;rejected=$failed;before=$before;owner_read_seam=$true;actual_ntfs=$true;directory_set_unchanged=$true} | ConvertTo-Json -Depth 5))
'PASS'
}finally{
 $restoreErrors=@()
 # Restore parents first, then each saved child with its original inheritance.
 foreach($original in $originalAcls){
  try{
   $acl=Microsoft.PowerShell.Security\Get-Acl -LiteralPath $original.path -ErrorAction Stop
   $information=if($original.protected){[uint32]2147483652}else{[uint32]536870916}
   if($acl.GetOwner([Security.Principal.SecurityIdentifier]).Value -cne $original.owner){$information=$information -bor 1}
   if($acl.GetGroup([Security.Principal.SecurityIdentifier]).Value -cne $original.group){$information=$information -bor 2}
   if(-not [LabelAdmissionFixture]::SetFileSecurityW($original.path,$information,$original.binary)){
    throw [ComponentModel.Win32Exception]::new([Runtime.InteropServices.Marshal]::GetLastWin32Error())
   }
   $restored=Microsoft.PowerShell.Security\Get-Acl -LiteralPath $original.path -ErrorAction Stop
   if($restored.Sddl -cne $original.sddl){throw 'Admission fixture security was not restored.'}
  }catch{$restoreErrors+=$_.Exception}
 }
 if($restoreErrors.Count){throw [AggregateException]::new('Admission fixture security restore failed.',[Exception[]]$restoreErrors)}
}
''', engine=engine, values={
        'LM_ADMISSION_HELPER': str(ROOT/'tools/bootstrap_integrity.ps1'),
        'LM_ADMISSION_DAILY': str(ROOT/'tools/apply_prepared_update.ps1'),
        'LM_ADMISSION_ROOT': str(native_root), 'LM_ADMISSION_ENTRY': entry,
        'LM_ADMISSION_SCENARIO': scenario,
    })
    assert result.returncode == 0, result.stdout + result.stderr
    proof = json.loads((native_root/'proof.json').read_text(encoding='utf-8'))
    assert proof['actual_ntfs'] and proof['directory_set_unchanged']
    assert proof['rejected'] == (scenario not in ('target', 'system_owner', 'trusted_owner')
                                 or entry in ('other_leaf', 'standalone_other'))


@pytest.mark.parametrize('engine', ('powershell.exe', 'pwsh.exe'))
def test_standalone_uses_exact_pure_shared_predicate(tmp_path, engine):
    result = run_functions(tmp_path, ROOT/'INSTALL_THIS_PC.ps1', ['Assert-ProtectedCodeAncestors'], r'''
$helper=[Management.Automation.Language.Parser]::ParseFile($env:LM_ADMISSION_HELPER,[ref]$tokens,[ref]$errors)
if($errors.Count){throw 'Helper parse failed.'}
$predicate='Get-KmtechSharedAncestorAclState'
$shared=@($helper.FindAll({param($n)$n -is [Management.Automation.Language.FunctionDefinitionAst] -and $n.Name -ceq $predicate},$true))
$standalone=@($ast.FindAll({param($n)$n -is [Management.Automation.Language.FunctionDefinitionAst] -and $n.Name -ceq $predicate},$true))
if($shared.Count -ne 1 -or $standalone.Count -ne 1){throw 'Expected one predicate per trust boundary.'}
if($shared[0].Body.Extent.Text -cne $standalone[0].Body.Extent.Text){throw 'Standalone/shared admission rules drifted.'}
# The complete copied predicate and its nested shape key have no IO/mutation commands.
$commands=@($shared[0].FindAll({param($n)$n -is [Management.Automation.Language.CommandAst]},$true) | ForEach-Object {$_.GetCommandName()})
foreach($name in $commands){if($name -notin @('ForEach-Object','Sort-Object','Get-KmtechAncestorAclShapeKey','Test-KmtechPrivilegedOwnerSid')){throw ('Predicate gained side effects: '+$name)}}
'PASS'
''', engine=engine, values={'LM_ADMISSION_HELPER': str(ROOT/'tools/bootstrap_integrity.ps1')})
    assert result.returncode == 0, result.stdout + result.stderr
