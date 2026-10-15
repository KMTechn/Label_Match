"""Run the complete standalone script and its trap with native proofs isolated."""
from pathlib import Path
import json
import re
import shutil
import subprocess

import pytest

ROOT = Path(__file__).resolve().parents[1]


@pytest.mark.parametrize("engine", ["powershell.exe", "pwsh.exe"])
@pytest.mark.parametrize("scenario", ["arguments", "persistence"])
def test_standalone_refusal_reaches_the_public_failure_record(tmp_path, engine, scenario):
    executable = shutil.which(engine)
    assert executable is not None
    package = tmp_path / "packet"
    package.mkdir()
    (package / "tools").mkdir()
    for name in ("failure_redaction.ps1", "failure_redaction_rules.json"):
        shutil.copy2(ROOT / "tools" / name, package / "tools" / name)
    # Replace only native privilege and registry reads. Parameter binding, all
    # remaining top-level code, both refusal branches and the trap are unchanged.
    source = (ROOT / "INSTALL_THIS_PC.ps1").read_text(encoding="utf-8-sig")
    source, count = re.subn(r"(?ms)^function Assert-AlreadyElevated \{.*?^\}",
        "function Assert-AlreadyElevated {}", source)
    assert count == 1
    source, count = re.subn(r"(?ms)^function Test-CurrentUserRelayPersistencePresent \{.*?^\}",
        "function Test-CurrentUserRelayPersistencePresent { return $true }", source)
    assert count == 1
    script = package / "INSTALL_THIS_PC.ps1"
    script.write_bytes(b"\xef\xbb\xbf" + source.encode())
    args = [executable, "-NoLogo", "-NoProfile", "-NonInteractive", "-File", str(script), "-Uninstall"]
    if scenario == "arguments":
        args += ["-WorkerSid", "invalid-worker-qualification", "-DataGenerationSubjectSid", "mismatch-qualification"]
    child = subprocess.run(args, capture_output=True, text=True, creationflags=subprocess.CREATE_NO_WINDOW)
    records = [line.split("=", 1)[1] for line in child.stderr.splitlines()
        if line.startswith("LABEL_MATCH_INSTALLER_FAILURE=")]
    assert child.returncode == 1 and len(records) == 1
    public = json.loads(records[0])
    assert public["reason_code"] == ("UNSUPPORTED_WORKER_ARGUMENTS" if scenario == "arguments"
        else "CURRENT_USER_RELAY_SETUP_PRESENT")
    guidance = public["guidance"]
    assert "-Uninstall" in guidance and "-WorkerSid 없이" in guidance
    if scenario == "persistence":
        assert guidance.index("--remove-current-user-setup") < guidance.index("-Uninstall")
    else:
        assert "WorkerSid 형식" in guidance and "작업자 SID 대조" in guidance
    assert not any(value in records[0] for value in (
        "invalid-worker-qualification", "mismatch-qualification", str(tmp_path)))
    assert re.search(r"S-1-5-21-(?:\d+-){3}\d+", records[0]) is None
