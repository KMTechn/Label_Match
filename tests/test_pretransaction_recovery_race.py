from pathlib import Path
import pytest
INSTALLER = Path(__file__).resolve().parents[1] / 'INSTALL_CANONICAL_PORTABLE.ps1'

@pytest.mark.parametrize("engine", ("powershell.exe", "pwsh.exe"))
def test_pretransaction_archive_preserves_run_published_after_listing(tmp_path, engine):
    from tests.test_shared_ancestor_protection import run_functions

    result = run_functions(tmp_path, INSTALLER, ["Archive-PretransactionFailure"], r'''
$auditRoot=Join-Path $env:LM_ARCHIVE_ROOT 'install-audit'
[void][IO.Directory]::CreateDirectory($auditRoot)
$own=Join-Path $auditRoot 'canonical-portable-race-a.json'
$other=Join-Path $auditRoot 'canonical-portable-race-b.json'
[IO.File]::WriteAllText($own,'first run evidence')
$script:published=$false
function Get-ChildItem {
 param($LiteralPath,[switch]$Force)
 $snapshot=@(Microsoft.PowerShell.Management\Get-ChildItem -LiteralPath $LiteralPath -Force:$Force)
 if($LiteralPath -ceq $auditRoot -and -not $script:published){
  $script:published=$true
  [IO.File]::WriteAllText($other,'live second run evidence')
 }
 return $snapshot
}
$archive=Archive-PretransactionFailure $auditRoot 'race-a'
if(-not (Test-Path -LiteralPath $other)){throw 'Live other-run record moved.'}
if([IO.File]::ReadAllText($other) -cne 'live second run evidence'){throw 'Other-run bytes changed.'}
if([IO.File]::ReadAllText((Join-Path $archive (Split-Path -Leaf $own))) -cne 'first run evidence'){throw 'Own evidence lost.'}
if(Test-Path -LiteralPath (Join-Path $archive (Split-Path -Leaf $other))){throw 'Other-run record entered own archive.'}
'PASS'
''', engine=engine, values={"LM_ARCHIVE_ROOT": str(tmp_path)})
    assert result.returncode == 0, result.stdout + result.stderr


@pytest.mark.parametrize("engine", ("powershell.exe", "pwsh.exe"))
@pytest.mark.parametrize("case", ("late-record", "open-handle"))
def test_pretransaction_empty_root_cleanup_preserves_new_record_or_handle(tmp_path, engine, case):
    from tests.test_shared_ancestor_protection import run_functions

    result = run_functions(tmp_path, INSTALLER, ["Archive-PretransactionFailure"], r"""
$auditRoot=Join-Path $env:LM_ARCHIVE_ROOT 'install-audit'
[void][IO.Directory]::CreateDirectory($auditRoot)
$own=Join-Path $auditRoot 'canonical-portable-race-a.json'
$other=Join-Path $auditRoot 'canonical-portable-race-b.json'
[IO.File]::WriteAllText($own,'first run evidence')
$handle=[IntPtr]::Zero
if($env:LM_ARCHIVE_CASE -eq 'open-handle'){
 Add-Type -TypeDefinition @'
using System;
using System.Runtime.InteropServices;
public static class AuditHandle {
 [DllImport("kernel32.dll", CharSet=CharSet.Unicode, SetLastError=true)]
 public static extern IntPtr CreateFile(string path,uint access,uint share,IntPtr security,uint mode,uint flags,IntPtr template);
 [DllImport("kernel32.dll")] public static extern bool CloseHandle(IntPtr h);
}
'@
 $handle=[AuditHandle]::CreateFile($auditRoot,[uint32]2147483648,3,[IntPtr]::Zero,3,0x02000000,[IntPtr]::Zero)
 if($handle -eq [IntPtr](-1)){throw 'Native isolated directory handle could not be opened.'}
}else{
 function Invoke-LabelPinnedAtomicReplace {param($Source,$Destination,$Mode)
  [IO.File]::Move($Source,$Destination)
  [IO.File]::WriteAllText($other,'live second run evidence')
 }
}
try {
 $archive=Archive-PretransactionFailure $auditRoot 'race-a'
 if(-not (Test-Path -LiteralPath $auditRoot)){throw 'Shared live root deleted.'}
 if(-not $Script:LabelPretransactionActiveRootRetained){throw 'Retention outcome not recorded.'}
 if($env:LM_ARCHIVE_CASE -eq 'late-record' -and [IO.File]::ReadAllText($other) -cne 'live second run evidence'){throw 'Late record lost.'}
 if([IO.File]::ReadAllText((Join-Path $archive (Split-Path -Leaf $own))) -cne 'first run evidence'){throw 'Own evidence lost.'}
 'PASS'
} finally {if($handle -ne [IntPtr]::Zero){[void][AuditHandle]::CloseHandle($handle)}}
""", engine=engine, values={"LM_ARCHIVE_ROOT": str(tmp_path), "LM_ARCHIVE_CASE": case})
    assert result.returncode == 0, result.stdout + result.stderr
