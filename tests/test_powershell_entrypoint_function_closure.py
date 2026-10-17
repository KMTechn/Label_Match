"""G62: parse each independent PowerShell entry without executing product code."""

import json
from pathlib import Path
import shutil
import subprocess

import pytest


ROOT = Path(__file__).resolve().parents[1]

# Libraries bootstrap_integrity, failure_redaction, invoke_embedded_python and
# label_writer_fence are reached through their actual dot-source sites below.
ENTRYPOINTS = (
    "INSTALL_THIS_PC.ps1",
    "INSTALL_CANONICAL_PORTABLE.ps1",
    "install_label_match_direct_sync.ps1",
    "tools/apply_prepared_update.ps1",
    "tools/build_frozen_release_candidate.ps1",
    "tools/label_worker_shortcuts.ps1",
    "tools/prepublish_release_gate.ps1",
    "tools/provision_protected_admin_acl.ps1",
    "tools/run_frozen_release_candidate_once.ps1",
    "tools/sign_release_executables.ps1",
)


def test_independent_powershell_entries_have_function_definitions(tmp_path):
    powershell = shutil.which("powershell.exe")
    if powershell is None:
        pytest.skip("G62 requires the Windows PowerShell 5.1 parser")
    result_path = tmp_path / "closure.json"
    entries_path = tmp_path / "entries.json"
    entries_path.write_text(json.dumps(ENTRYPOINTS), encoding="utf-8")
    completed = subprocess.run(
        [powershell, "-NoLogo", "-NoProfile", "-NonInteractive", "-File",
         str(ROOT / "tests/powershell_entrypoint_function_closure.ps1"),
         "-Root", str(ROOT), "-EntriesPath", str(entries_path),
         "-ResultPath", str(result_path)],
        cwd=ROOT, capture_output=True,
        creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0),
    )
    # Never expose parser extents, source literals or native stderr on failure.
    assert completed.returncode == 0, "G62 parser harness failed (source suppressed)"
    result = json.loads(result_path.read_text(encoding="utf-8-sig"))
    assert result["self_check"], "G62 did not reject an undefined fixture command"
    problems = result["problems"]
    assert not problems, "G62 unresolved commands/loads:\n" + "\n".join(problems)
    assert len(result["entries"]) == len(ENTRYPOINTS)
