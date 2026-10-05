"""Read-only launcher admission before any canonical preparation mutation."""
import hashlib
import json
from pathlib import Path
import xml.etree.ElementTree as ET

import pytest

from tests.test_shared_ancestor_protection import run_functions

ROOT = Path(__file__).resolve().parents[1]


def test_canonical_launcher_admission_precedes_first_audit_write():
    source = (ROOT / "INSTALL_CANONICAL_PORTABLE.ps1").read_text(encoding="utf-8-sig")
    snapshot = source.index("$legacyRelayBefore=Get-LabelOfficialLegacyTaskSnapshot")
    admission = source.index("Assert-LabelOfficialLegacyTaskLauncher $legacyRelayBefore", snapshot)
    first_write = source.index("New-Item -ItemType Directory -Path $localAuditRoot -Force", snapshot)
    assert snapshot < admission < first_write


@pytest.mark.parametrize("engine", ["powershell.exe", "pwsh.exe"])
@pytest.mark.parametrize("case", ["no-task", "missing-vbs", "missing-wrapper", "foreign-binding",
                                  "changed-content", "ps1-consistent", "cmd-consistent",
                                  "ps1-official-crcrlf", "changed-content-crcrlf",
                                  "foreign-binding-crcrlf", "extra-blank-line", "mixed-crcrlf"])
def test_actual_launcher_admission_has_zero_writes_on_refusal(tmp_path, engine, case):
    from tools.direct_sync_relay_install_pack import _task_launcher_content
    launcher = tmp_path / "bin/run_owned.vbs"
    wrapper = launcher.with_suffix(".cmd" if case == "cmd-consistent" else ".ps1")
    launcher.parent.mkdir()
    protected = tmp_path / "protected-base"
    protected.mkdir()
    if case not in {"no-task", "missing-vbs"}:
        selected = tmp_path / "foreign.ps1" if case.startswith("foreign-binding") else wrapper
        text = _task_launcher_content(selected)
        if case == "cmd-consistent":
            text = '\r\n'.join([
                'Set shell = CreateObject("WScript.Shell")',
                'comspec = shell.ExpandEnvironmentStrings("%ComSpec%")',
                'runner = "' + str(wrapper) + '"',
                'command = """" & comspec & """ /d /c """ & runner & """"',
                'exitCode = shell.Run(command, 0, True)', 'WScript.Quit exitCode', ''])
        if case.startswith("changed-content"):
            text = text.replace('shell.Run(command, 0, True)', 'shell.Run(command, 1, False)')
        if case == "extra-blank-line":
            text = text.replace('wrapper = ', '\r\nwrapper = ')
        if case == "mixed-crcrlf":
            text = text.replace('\r\n', '\n', 1)
        # The official v2.0.38 writer already returns CRLF; its text writer's
        # CRLF translation produces CR CR LF on Windows (six separators).
        launcher.write_text(text, encoding="ascii", newline="\r\n" if case.endswith("crcrlf") else "")
        if case.endswith("crcrlf"):
            assert launcher.read_bytes().count(b"\r\r\n") == (5 if case == "mixed-crcrlf" else 6)
    if case not in {"no-task", "missing-wrapper"}:
        wrapper.write_text("exit 0\n", encoding="ascii")
    task = ET.Element("Task")
    action = ET.SubElement(ET.SubElement(task, "Actions"), "Exec")
    ET.SubElement(action, "Arguments").text = '//B //NoLogo "' + str(launcher) + '"'
    config = tmp_path / "case.json"
    config.write_text(json.dumps({"xml": ET.tostring(task, encoding="unicode"),
                                 "no_task": case == "no-task", "protected": str(protected),
                                 "mutation": str(tmp_path / "preparation/evidence")}), encoding="utf-8")
    before = {p.relative_to(tmp_path): hashlib.sha256(p.read_bytes()).hexdigest()
              for p in tmp_path.rglob("*") if p.is_file()}
    result = run_functions(tmp_path, ROOT / "INSTALL_THIS_PC.ps1",
                           ["Assert-LabelOfficialLegacyTaskLauncher"], r'''
Set-StrictMode -Version Latest
$case=Get-Content -LiteralPath $env:LM_LAUNCHER_CASE -Raw | ConvertFrom-Json
$beforeAcl=Get-Acl -LiteralPath $case.protected
$snapshot=if($case.no_task){$null}else{[pscustomobject]@{xml=$case.xml;fingerprint=[pscustomobject]@{matched=$true}}}
$admitted=$false
try {
 Assert-LabelOfficialLegacyTaskLauncher $snapshot
 $admitted=$true
 [void][IO.Directory]::CreateDirectory($case.mutation)
}catch{
 if($_.Exception.Message -cne 'Healthy lifecycle task belongs to another principal/command.'){
  throw 'Existing refusal guidance changed'
 }
}
$afterAcl=Get-Acl -LiteralPath $case.protected
if($beforeAcl.Owner -cne $afterAcl.Owner -or $beforeAcl.Sddl -cne $afterAcl.Sddl){throw 'Protected fixture owner or effective ACL changed'}
if($admitted){'ADMITTED'}else{'REFUSED'}
''', engine=engine, values={"LM_LAUNCHER_CASE": str(config)})
    (tmp_path / "driver.stdout").write_text(result.stdout, encoding="utf-8")
    (tmp_path / "driver.stderr").write_text(result.stderr, encoding="utf-8")
    assert result.returncode == 0, "isolated launcher driver failed; child evidence retained"
    allowed = case in {"no-task", "ps1-consistent", "cmd-consistent", "ps1-official-crcrlf"}
    assert result.stdout.strip() == ("ADMITTED" if allowed else "REFUSED")
    assert (tmp_path / "preparation/evidence").exists() is allowed
    assert all(hashlib.sha256((tmp_path / p).read_bytes()).hexdigest() == digest
               for p, digest in before.items())
    if not allowed:
        assert not (tmp_path / "preparation").exists()
