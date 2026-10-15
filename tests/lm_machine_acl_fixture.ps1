$Script:testUntrustedOwnerPath=''
function Set-TestMachineAcl([string]$Path,[switch]$ProgramData) {
    [void][IO.Directory]::CreateDirectory($Path)
    $acl=[Security.AccessControl.DirectorySecurity]::new();$acl.SetAccessRuleProtection($true,$false)
    $acl.SetOwner([Security.Principal.SecurityIdentifier]::new('S-1-5-32-544'))
    foreach($sid in @('S-1-5-18','S-1-5-32-544')){
        $acl.AddAccessRule([Security.AccessControl.FileSystemAccessRule]::new([Security.Principal.SecurityIdentifier]::new($sid),'FullControl','ObjectInherit,ContainerInherit','None','Allow'))
    }
    if($ProgramData){
        $acl.SetSecurityDescriptorSddlForm('O:BAG:BAD:P(A;OICI;FA;;;SY)(A;OICI;FA;;;BA)(A;OICI;0x1200a9;;;BU)(A;CI;0x116;;;BU)')
    }
    Set-Acl -LiteralPath $Path -AclObject $acl
}
function Get-LabelMachineProgramDataRoot {return $Script:testProgramData}
function Get-Acl {
    [CmdletBinding()]param([string]$LiteralPath)
    $selected=[IO.Path]::GetFullPath($LiteralPath)
    $inside=$selected -ieq $Script:testProgramData -or $selected.StartsWith($Script:testProgramData+'\',[StringComparison]::OrdinalIgnoreCase)
    $observed=if($inside){$selected}else{$Script:testAnchor}
    $acl=Microsoft.PowerShell.Security\Get-Acl -LiteralPath $observed -ErrorAction Stop
    $user=[Security.Principal.WindowsIdentity]::GetCurrent().User
    $sddl=$acl.GetSecurityDescriptorSddlForm([Security.AccessControl.AccessControlSections]::Owner -bor [Security.AccessControl.AccessControlSections]::Group -bor [Security.AccessControl.AccessControlSections]::Access)
    $sddl=[regex]::Replace($sddl,'\([^()]*;;;'+[regex]::Escape($user.Value)+'\)','')
    $copy=if($acl -is [Security.AccessControl.DirectorySecurity]){[Security.AccessControl.DirectorySecurity]::new()}else{[Security.AccessControl.FileSecurity]::new()}
    $copy.SetSecurityDescriptorSddlForm($sddl);$acl=$copy
    $owner=if($selected -ieq $Script:testUntrustedOwnerPath){'S-1-5-32-545'}else{'S-1-5-32-544'}
    $acl.SetOwner([Security.Principal.SecurityIdentifier]::new($owner))
    return $acl
}
function Set-Acl {
    [CmdletBinding()]param([string]$LiteralPath,$AclObject)
    $selected=[IO.Path]::GetFullPath($LiteralPath)
    if($selected -ine $Script:testProgramData -and -not $selected.StartsWith($Script:testProgramData+'\',[StringComparison]::OrdinalIgnoreCase) -and $selected -ine $Script:testAnchor){throw 'ACL_FIXTURE_SCOPE_INVALID'}
    $directory=([IO.File]::GetAttributes($selected) -band [IO.FileAttributes]::Directory) -ne 0
    $copy=if($directory){[Security.AccessControl.DirectorySecurity]::new()}else{[Security.AccessControl.FileSecurity]::new()}
    $copy.SetSecurityDescriptorSddlForm($AclObject.GetSecurityDescriptorSddlForm([Security.AccessControl.AccessControlSections]::Access),[Security.AccessControl.AccessControlSections]::Access)
    $user=[Security.Principal.WindowsIdentity]::GetCurrent().User
    $rule=if($directory){[Security.AccessControl.FileSystemAccessRule]::new($user,'FullControl','ObjectInherit,ContainerInherit','None','Allow')}else{[Security.AccessControl.FileSystemAccessRule]::new($user,'FullControl','Allow')}
    $copy.AddAccessRule($rule)
    if(-not ('Sub194FixtureDacl' -as [type])){
        Add-Type -TypeDefinition @'
using System;
using System.Runtime.InteropServices;
public static class Sub194FixtureDacl {
 [DllImport("advapi32.dll",CharSet=CharSet.Unicode,SetLastError=true)]
 static extern uint SetNamedSecurityInfo(string path,int kind,uint information,IntPtr owner,IntPtr group,IntPtr dacl,IntPtr sacl);
 [DllImport("advapi32.dll",SetLastError=true)]
 static extern bool GetSecurityDescriptorDacl(IntPtr descriptor,out bool present,out IntPtr dacl,out bool defaulted);
 public static uint Apply(string path,uint information,byte[] descriptor) {
  var pinned=GCHandle.Alloc(descriptor,GCHandleType.Pinned);
  try {
   bool present,defaulted;IntPtr dacl;
   if(!GetSecurityDescriptorDacl(pinned.AddrOfPinnedObject(),out present,out dacl,out defaulted) || !present)
    return (uint)Marshal.GetLastWin32Error();
   return SetNamedSecurityInfo(path,1,information,IntPtr.Zero,IntPtr.Zero,dacl,IntPtr.Zero);
  } finally {pinned.Free();}
 }
}
'@
    }
    [uint32]$information=if($copy.AreAccessRulesProtected){2147483652}else{536870916}
    $errorCode=[Sub194FixtureDacl]::Apply($selected,$information,$copy.GetSecurityDescriptorBinaryForm())
    if($errorCode -ne 0){throw [ComponentModel.Win32Exception]::new([int]$errorCode)}
}
