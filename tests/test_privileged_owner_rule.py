"""Privileged owner reads over owned D NTFS ACLs; privileged owners are not set."""
from pathlib import Path
import os
import uuid

import pytest

from tests.test_shared_ancestor_protection import run_functions

ROOT = Path(__file__).resolve().parents[1]


@pytest.mark.parametrize('engine', ('powershell.exe', 'pwsh.exe'))
@pytest.mark.parametrize('source,call', (
    ('INSTALL_THIS_PC.ps1', 'Assert-HardenedCodeAcl'),
    ('tools/apply_prepared_update.ps1', 'Assert-Protected')))
@pytest.mark.parametrize('owner', ('S-1-5-18', 'S-1-5-32-544',
    'S-1-5-80-956008885-3418522649-1831038044-1853292631-2271478464',
    'S-1-5-21-1-2-3-1000'))
@pytest.mark.parametrize('shape', ('protected', 'world_write'))
def test_native_code_trust_preserves_acl_masks_for_owner_set(tmp_path, engine, source, call, owner, shape):
    native = Path(os.environ.get('LM_NTFS_TEST_ROOT', str(tmp_path))) / ('o-' + uuid.uuid4().hex[:8])
    names = ['ConvertTo-NormalizedAclRights', call] if call == 'Assert-HardenedCodeAcl' else [call]
    result = run_functions(tmp_path, ROOT / source, names, r'''
$native=$env:LM_OWNER_NATIVE_ROOT
[void][IO.Directory]::CreateDirectory($native)
Add-Type -TypeDefinition @'
using System; using System.Runtime.InteropServices;
public static class LabelOwnerFixture {
 [DllImport("advapi32.dll", CharSet=CharSet.Unicode, SetLastError=true)]
 [return: MarshalAs(UnmanagedType.Bool)]
 public static extern bool SetFileSecurityW(string path,uint info,byte[] descriptor);
}
'@
$sddl='D:P(A;OICI;FA;;;BA)(A;OICI;FA;;;SY)(A;OICI;0x1200a9;;;BU)'
if($env:LM_OWNER_SHAPE -ceq 'world_write'){$sddl+='(A;;WD;;;WD)'}
$raw=[Security.AccessControl.RawSecurityDescriptor]::new($sddl)
$binary=[byte[]]::new($raw.BinaryLength);$raw.GetBinaryForm($binary,0)
if(-not [LabelOwnerFixture]::SetFileSecurityW($native,[uint32]2147483652,$binary)){
 throw [ComponentModel.Win32Exception]::new([Runtime.InteropServices.Marshal]::GetLastWin32Error())
}
$before=(Microsoft.PowerShell.Security\Get-Acl -LiteralPath $native).Sddl
function Native-Path($LiteralPath){
 if($LiteralPath -notin @('C:\Owned','C:\',$native)){throw 'Unmapped host ACL target'}
 return $native
}
function Get-Item {param($LiteralPath,[switch]$Force,$ErrorAction) Microsoft.PowerShell.Management\Get-Item -LiteralPath (Native-Path $LiteralPath) -ErrorAction Stop}
function Get-Acl {param($LiteralPath,$ErrorAction)
 $acl=Microsoft.PowerShell.Security\Get-Acl -LiteralPath (Native-Path $LiteralPath) -ErrorAction Stop
 $acl.SetOwner([Security.Principal.SecurityIdentifier]::new($env:LM_OWNER_SID));return $acl
}
function Assert-NoReparsePoint {param($Path,$Context) [void](Native-Path $Path)}
function Test-SamePath {param($Left,$Right) return $Left -ceq (Native-Path $Right)}
function Set-Acl {throw 'Owner admission attempted native mutation'}
$failed=$false;$failure=$null
try{
 if($env:LM_OWNER_CALL -ceq 'Assert-HardenedCodeAcl'){Assert-HardenedCodeAcl 'C:\Owned'}
 else{Assert-Protected 'C:\Owned'}
}catch{$failed=$true;$failure=$_}
$shouldFail=$env:LM_OWNER_SID -ceq 'S-1-5-21-1-2-3-1000' -or $env:LM_OWNER_SHAPE -ceq 'world_write'
if($failed -ne $shouldFail){throw ('Native owner/mask verdict differs: '+$failure)}
if((Microsoft.PowerShell.Security\Get-Acl -LiteralPath $native).Sddl -cne $before){throw 'Native owner/DACL changed'}
'PASS'
''', engine=engine, values={'LM_OWNER_NATIVE_ROOT': str(native), 'LM_OWNER_SID': owner,
    'LM_OWNER_SHAPE': shape, 'LM_OWNER_CALL': call})
    assert result.returncode == 0, result.stdout + result.stderr


@pytest.mark.parametrize('engine', ('powershell.exe', 'pwsh.exe'))
def test_owner_policy_boundary_copies_are_identical_and_pure(tmp_path, engine):
    result = run_functions(tmp_path, ROOT / 'INSTALL_THIS_PC.ps1', ['Assert-ProtectedCodeAncestors'], r'''
$bodies=@()
foreach($file in @('INSTALL_THIS_PC.ps1','tools/bootstrap_integrity.ps1','tools/apply_prepared_update.ps1')){
 $tree=[Management.Automation.Language.Parser]::ParseFile((Join-Path $env:LM_OWNER_REPO $file),[ref]$tokens,[ref]$errors)
 if($errors.Count){throw 'Owner boundary parse failed'}
 $definitions=@($tree.FindAll({param($n)$n -is [Management.Automation.Language.FunctionDefinitionAst] -and $n.Name -ceq 'Test-KmtechPrivilegedOwnerSid'},$true))
 if($definitions.Count -ne 1){throw 'Owner policy missing/duplicated'}
 $bodies+=,$definitions[0].Body.Extent.Text
 if(@($definitions[0].FindAll({param($n)$n -is [Management.Automation.Language.CommandAst]},$true)).Count){throw 'Owner policy gained commands'}
}
foreach($body in $bodies){if($body -cne $bodies[0]){throw 'Owner policy boundary drift'}}
'PASS'
''', engine=engine, values={'LM_OWNER_REPO': str(ROOT)})
    assert result.returncode == 0, result.stdout + result.stderr
