"""U1–U9: generation, not READY failure alone, selects the next executable."""
import json
from pathlib import Path
import subprocess
import sys
import shutil

import pytest
import auto_update as updates
from tests.update_trial_generation_child import OLD, NEW
from kmtech_shared import data_generation as generation


def child(root, scenario, role, mode="run"):
    result = subprocess.run([sys.executable, "-B", str(Path(__file__).with_name("update_trial_generation_child.py")),
        mode, scenario, role, str(root)], capture_output=True, text=True,
        creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0))
    assert result.returncode == (19 if mode == "interrupt" else 0), result.stderr[-2000:]
    return json.loads(result.stdout) if result.stdout else None


@pytest.mark.parametrize("role", ["gui", "relay"])
@pytest.mark.parametrize("scenario", ["before", "shared", "owner", "unknown", "compatible", "unknown-before"])
def test_failed_trial_selects_code_from_actual_generation(tmp_path, role, scenario):
    result = child(tmp_path / "trial", scenario, role)
    if scenario == "before":
        assert result["selected_sha"] == OLD
        assert result["previous_status"] == "ALLOW"
        assert result["phase"] == "ROLLBACK_PENDING_READY"
    else:
        assert result["selected_sha"] == NEW
        assert result["phase"] == "DEFERRED"
        assert result["decision"]["selected_sha"] == NEW
        if scenario in {"unknown", "unknown-before"}:
            assert result["decision"]["code"] == "TRIAL_GENERATION_UNKNOWN"
            assert result["candidate_status"] == "UNKNOWN"
        else:
            assert result["candidate_status"] == "ALLOW"
            assert result["previous_status"] == ("ALLOW" if scenario == "compatible" else "REFUSE")
    if scenario == "shared":
        assert result["shared_build"] == NEW and result["owner_build"] == OLD
        assert result["business"] == "original business data"
    if scenario == "owner":
        assert result["owner_build"] == NEW
        assert result["business"] == "candidate data preserved"


@pytest.mark.parametrize("scenario", ["before", "shared", "owner", "unknown"])
def test_interrupted_durable_decision_replays_in_a_fresh_process(tmp_path, scenario):
    root = tmp_path / "trial"
    child(root, scenario, "gui", "interrupt")
    before = updates.read_json(root / "control/transition.json")
    assert updates.read_json(root / "control/active.json")["sha"] == NEW
    result = child(root, scenario, "gui", "resume")
    assert result["selected_sha"] == (OLD if scenario == "before" else NEW)
    assert updates.read_json(root / "control/transition.json")["failure_decision"] == before["failure_decision"]


def test_unobserved_legacy_pending_failure_cannot_authorize_rollback():
    from tests.test_auto_update import trial
    pending = updates.request_rollback(trial(), reason="candidate startup failed")
    assert updates.recovery_sha(pending, pointer_sha=NEW) == NEW
    with pytest.raises(ValueError, match="observed unchanged"):
        updates.rollback_transition(pending, writers_absent=True, previous_verified=True,
            durable_rollback=True, nonce="8" * 32, now=updates.now_utc())


def test_unknown_generation_next_attempt_keeps_support_guidance_and_all_evidence(tmp_path):
    root = tmp_path / "trial"
    child(root, "unknown", "relay")
    evidence = {path: path.read_bytes() for path in root.rglob("*") if path.is_file()}
    for _attempt in range(2):
        admission = child(root, "unknown", "relay", "admission")
        assert admission == {"status": "UNKNOWN", "message": generation.UNKNOWN_MESSAGE,
                             "selected_sha": NEW}
        assert {path: path.read_bytes() for path in root.rglob("*") if path.is_file()} == evidence


@pytest.mark.parametrize("role", ["gui", "relay"])
def test_successful_ready_still_confirms_candidate_after_generation(tmp_path, role):
    result = child(tmp_path / "trial", "ready", role)
    assert result["phase"] == "CONFIRMED"
    assert result["selected_sha"] == NEW and result["candidate_status"] == "ALLOW"
    assert result["shared_build"] == result["owner_build"] == NEW
    assert result["decision"] is None


@pytest.mark.parametrize("shell", ["powershell", "pwsh"])
def test_powershell_file_child_retains_candidate_after_generation(tmp_path, shell):
    executable = shutil.which(shell)
    assert executable, "the assigned Windows host requires PS5.1 and PS7"
    literal = lambda value: "'" + str(value).replace("'", "''") + "'"
    script = tmp_path / "trial.ps1"
    script.write_text("& " + literal(sys.executable) + " -B " +
        literal(Path(__file__).with_name("update_trial_generation_child.py")) +
        " run owner relay " + literal(tmp_path / "trial") + "; exit $LASTEXITCODE\n", encoding="utf-8")
    result = subprocess.run([executable, "-NoProfile", "-File", str(script)],
        capture_output=True, text=True, creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0))
    assert result.returncode == 0, result.stderr[-2000:]
    observed = json.loads(result.stdout)
    assert observed["selected_sha"] == NEW
    assert observed["candidate_status"] == "ALLOW" and observed["previous_status"] == "REFUSE"
