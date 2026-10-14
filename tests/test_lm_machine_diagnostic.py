import json
from pathlib import Path
import shutil
import subprocess

import pytest

ROOT = Path(__file__).resolve().parents[1]


@pytest.mark.parametrize('engine', ['powershell','pwsh'])
@pytest.mark.parametrize('child_exit', [73,74])
@pytest.mark.parametrize('writer_fails', [False,True])
def test_machine_placement_preserves_child_failure_in_audit_and_outer_trap(tmp_path, engine, child_exit, writer_fails):
    command=[shutil.which(engine),'-NoProfile','-NonInteractive','-ExecutionPolicy','Bypass','-File',
        str(ROOT / 'tests/lm_machine_diagnostic.ps1'),'-HostRoot',str(ROOT),
        '-FixtureRoot',str(tmp_path),'-ChildExit',str(child_exit)]
    assert command[0] is not None
    if writer_fails:
        command.append('-WriterFails')
    child=subprocess.run(command,capture_output=True,creationflags=subprocess.CREATE_NO_WINDOW)
    assert child.returncode == child_exit
    assert not child.stdout.strip()
    prefix=b'LABEL_MATCH_INSTALLER_FAILURE='
    assert child.stderr.strip().startswith(prefix)
    public=json.loads(child.stderr.strip()[len(prefix):])
    audit=json.loads((tmp_path / 'audit.json').read_bytes())
    assert audit['status'] == 'RECOVERY_REQUIRED'
    assert audit['placement_child_failure']['exit_code'] == child_exit
    assert public['placement_code'] == audit['failure']['placement_code'] == 'PLACEMENT_DIAGNOSTIC_UNAVAILABLE'
    if writer_fails:
        assert public['report_status'] == audit['failure']['report_status'] == 'REPORT_UNAVAILABLE'
    else:
        assert public['report_path'] == audit['failure']['report_path'] == 'fixture-reference'
        assert public['report_sha256'] == audit['failure']['report_sha256'] == '0' * 64


def test_required_diagnostic_engine_absence_is_a_failure(tmp_path, monkeypatch):
    from tests.test_lm_placement_diagnostic import _parse
    monkeypatch.setattr(shutil,'which',lambda name: None)
    with pytest.raises(pytest.fail.Exception):
        _parse(tmp_path,'powershell',{})
