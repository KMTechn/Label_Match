"""A completed fresh transition is final only after its own writer fence is released."""

from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess
import uuid

import pytest

import fresh_server_transition as transition
import writer_session_fence as fence
from tests.test_writer_session_fence import _environment, _payload, _write_active
from tools.register_label_match_worker_pc import _current_user_sid


ROOT = Path(__file__).resolve().parents[1]
INSTALLER = ROOT / "INSTALL_CANONICAL_PORTABLE.ps1"
POWERSHELL = r"C:\Windows\System32\WindowsPowerShell\v1.0\powershell.exe"
KIND = {"ACTIVATED": "activation", "RESTORED": "restore"}


def _sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


@pytest.fixture
def crashed(tmp_path, monkeypatch):
    """The owner saved its completion record, then died before Stop-LabelWriterFence."""
    local = tmp_path / "local"
    control = local / "KMTech/DirectSync/label_match/control/writer-session"
    env = _environment(control)
    env.update(LOCALAPPDATA=str(local), KMTECH_FACTORY_INSTALL_TEST_MODE="1",
               PSModulePath=r"C:\Windows\System32\WindowsPowerShell\v1.0\Modules")
    for name in (fence.TEST_MODE_ENV, fence.CONTROL_ROOT_OVERRIDE_ENV):
        monkeypatch.setenv(name, env[name])
    packet = tmp_path / "packet"
    (packet / "tools").mkdir(parents=True)
    (packet / "portable-manifest.json").write_bytes(b'{"fixture":"fresh-terminal-packet"}')
    shutil.copy2(ROOT / "tools/label_writer_fence.ps1", packet / "tools/label_writer_fence.ps1")
    source_input = tmp_path / "transition-input.json"
    source_input.write_bytes(b'{"fixture":"fresh-terminal-input"}')
    payload = _payload(delegated_sources=["fresh_server_transition", "user_relay_process_start"], token="t" * 64)
    payload.update(session_id=uuid.uuid4().hex, attempt_id=uuid.uuid4().hex, replacement_transaction_id=uuid.uuid4().hex)
    payload["session_authority_mutex_name"] = fence.session_authority_mutex_name(
        payload["session_id"], payload["attempt_id"], payload["orchestrator_sha256"],
        payload["replacement_transaction_id"], payload["writer_contract_sha256"])
    _write_active(control, payload)
    owner = {"schema": "label-fresh-installer-owner-v1", "sid": _current_user_sid(),
             "install_root": str(tmp_path / "installed"), "packet": _sha(packet / "portable-manifest.json"),
             "input_sha256": _sha(source_input), "session_id": payload["session_id"],
             "attempt_id": payload["attempt_id"], "transaction_id": payload["replacement_transaction_id"],
             "status": "UNKNOWN", "started_at": datetime.now(timezone.utc).isoformat()}
    audit = local / "KMTech/Label_Match/server-transition"
    audit.mkdir(parents=True)
    (audit / "installer-owner.json").write_text(json.dumps(owner), encoding="utf-8")
    authority = {name: payload[name] for name in ("session_id", "attempt_id", "replacement_transaction_id")}
    return {"tmp": tmp_path, "env": env, "control": control, "packet": packet, "input": source_input,
            "payload": payload, "owner": audit / "installer-owner.json", "authority": authority}


def _state(phase, authority):
    return {"phase": phase, "attempts": [{"id": uuid.uuid4().hex, "kind": KIND[phase], "authority": authority,
                                          "status": "PASS"}]}


def _resume(crashed, phase, action, summary=None):
    """Run the real installer function; the product only answers the read-only plan."""
    state = _state(phase, crashed["authority"])
    summary = summary or {"phase": phase, "restore_possible": phase == "RESTORED",
                          "archive_root": str(crashed["tmp"] / "archive"), **transition._completion(state)}
    tmp = crashed["tmp"]
    (tmp / "summary.json").write_text(json.dumps(summary, ensure_ascii=False), encoding="utf-8")
    source = INSTALLER.read_text(encoding="utf-8")
    definitions = source[:source.index("if (-not $SourceRoot) { $SourceRoot = $PSScriptRoot }")]
    quoted = {name: str(value).replace("'", "''") for name, value in {
        "root": ROOT, "packet": crashed["packet"], "install": tmp / "installed",
        "summary": tmp / "summary.json", "calls": tmp / "product-calls.log"}.items()}
    harness = tmp / "terminal-resume.ps1"
    harness.write_text(definitions + f"""
. (Get-LabelSharedPortableLeafPath '{quoted["root"]}')
$source = '{quoted["packet"]}'
$install = '{quoted["install"]}'
function PortableInventory([string]$Root) {{
    [pscustomobject]@{{critical_file_sha256=[pscustomobject]@{{writer_fence_helper=(Sha (Join-Path $Root 'tools\\label_writer_fence.ps1'))}}}}
}}
function Product([string]$Root, [string]$Mode, [string[]]$ExtraArguments = @()) {{
    $action = $ExtraArguments[[Array]::IndexOf($ExtraArguments, '--action') + 1]
    Add-Content -LiteralPath '{quoted["calls"]}' -Value $action
    if ($action -cnotin @('plan', 'status')) {{ throw "unexpected mutating product call: $action" }}
    Copy-Item -LiteralPath '{quoted["summary"]}' -Destination $ExtraArguments[[Array]::IndexOf($ExtraArguments, '--result-path') + 1]
}}
Invoke-LabelFreshServerTransition
""", encoding="utf-8-sig")
    result = subprocess.run([POWERSHELL, "-NoProfile", "-NonInteractive", "-ExecutionPolicy", "Bypass", "-File",
                             str(harness), "-TransitionInputPath", str(crashed["input"]),
                             "-TransitionArchiveVolume", str(tmp / "archive"), "-FreshTransitionAction", action,
                             "-AllowNoncanonicalLayoutForTest", "-SkipSignatureValidationForTest"],
                            env=crashed["env"], capture_output=True, text=True, timeout=60,
                            creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0))
    calls = (tmp / "product-calls.log").read_text(encoding="utf-8").split() if (tmp / "product-calls.log").exists() else []
    return result, calls, state


def _ordinary_writer(crashed):
    try:
        with fence.writer_admission("package_outbox_enqueue", environ=_environment(crashed["control"])):
            return "ADMITTED"
    except fence.WriterFencedError as exc:
        return exc.code


@pytest.mark.parametrize("phase,action", [("ACTIVATED", "Resume"), ("RESTORED", "Restore"), ("RESTORED", "Resume")])
def test_supported_resume_releases_only_its_ended_owner_fence(crashed, phase, action):
    state = _state(phase, crashed["authority"])
    assert transition._completion(state)["completion"] == "FENCE_RELEASE_PENDING"
    assert _ordinary_writer(crashed) == "ACTIVE_WRITER_FENCE"
    result, calls, state = _resume(crashed, phase, action)
    assert result.returncode == 0, result.stderr
    assert "fresh_transition_status=PASS" in result.stdout
    assert calls == ["plan"]
    assert not (crashed["control"] / "active.json").exists()
    assert _ordinary_writer(crashed) == "ADMITTED"
    assert transition._completion(state)["completion"] == "COMPLETE"


def test_live_owner_keeps_the_fence_and_reports_waiting(crashed):
    authority = fence._acquire_named_mutex(crashed["payload"]["session_authority_mutex_name"], 0)  # noqa: SLF001
    assert authority is not None
    try:
        # The plan read in this thread cannot see its own mutex; the installer rechecks.
        result, calls, _ = _resume(crashed, "ACTIVATED", "Resume")
    finally:
        authority.release()
    assert result.returncode != 0
    assert (crashed["control"] / "active.json").exists()
    assert calls == ["plan"]
    assert _ordinary_writer(crashed) == "ACTIVE_WRITER_FENCE"


@pytest.mark.parametrize("binding", ["other_transition", "owner_input", "owner_absent"])
def test_unbound_or_other_transition_fence_is_kept(crashed, binding):
    before = (crashed["control"] / "active.json").read_bytes()
    authority = dict(crashed["authority"])
    if binding == "other_transition":
        authority["attempt_id"] = uuid.uuid4().hex
    elif binding == "owner_input":
        owner = json.loads(crashed["owner"].read_text(encoding="utf-8"))
        owner["input_sha256"] = "0" * 64
        crashed["owner"].write_text(json.dumps(owner), encoding="utf-8")
    else:
        crashed["owner"].unlink()
    state = _state("ACTIVATED", authority)
    summary = {"phase": "ACTIVATED", "restore_possible": False, "archive_root": "", **transition._completion(state)}
    if binding == "other_transition":
        assert summary["completion"] == "FENCE_BLOCKED" and summary["next_action"]
    result, calls, _ = _resume(crashed, "ACTIVATED", "Resume", summary)
    assert result.returncode != 0
    assert (crashed["control"] / "active.json").read_bytes() == before
    assert calls == ["plan"]
    assert _ordinary_writer(crashed) == "ACTIVE_WRITER_FENCE"


def test_prepare_never_releases_or_starts_a_fence_for_a_completed_record(crashed):
    before = (crashed["control"] / "active.json").read_bytes()
    result, calls, _ = _resume(crashed, "ACTIVATED", "Prepare")
    assert result.returncode != 0
    assert (crashed["control"] / "active.json").read_bytes() == before
    assert calls == ["plan"]


@pytest.mark.parametrize("phase,action", [("ACTIVATED", "Resume"), ("RESTORED", "Restore")])
def test_completed_record_without_fence_is_complete_without_new_fence(crashed, phase, action):
    (crashed["control"] / "active.json").unlink()
    result, calls, state = _resume(crashed, phase, action)
    assert result.returncode == 0, result.stderr
    assert "fresh_transition_status=PASS" in result.stdout
    assert calls == ["plan"]
    assert not (crashed["control"] / "active.json").exists()
    assert transition._completion(state)["completion"] == "COMPLETE"


def test_restored_record_is_not_reused_by_prepare(crashed):
    (crashed["control"] / "active.json").unlink()
    result, calls, _ = _resume(crashed, "RESTORED", "Prepare")
    assert result.returncode != 0
    assert calls == ["plan"]
    assert not (crashed["control"] / "active.json").exists()
