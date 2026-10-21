"""Real terminal Save sites and the worker-side fence boundary.

The serialization harness retains the legacy rollback-context probe only.
The administrator exit itself is exercised in test_terminal_orphan_admin_release.
Workers keep these fences until that administrator recovery finishes.
"""

import ast
from contextlib import contextmanager
import copy
import json
import os
from pathlib import Path
import re
import shutil
import subprocess

import pytest

import current_user_onboarding as onboarding
import startup_check
import writer_session_fence as fence
from tests.test_dead_installer_fence_entries import carried, fence_only_removal
from tests.test_failed_deployment_removal import CANONICAL, _remove, failed_install


ROOT = Path(__file__).resolve().parents[1]
INSTALLER = ROOT / "INSTALL_CANONICAL_PORTABLE.ps1"

# Primary audit saves only; optional EvidencePath copies share the same state.
# The final two machine saves apply when release was skipped or failed.
SITES = (
    (6685, "machine", ("RECOVERY_REQUIRED", "ROLLBACK_FAILED")),
    (6711, "machine", ("ROLLBACK_FAILED",)),
    (6722, "machine", ("RECOVERY_REQUIRED", "ROLLBACK_FAILED")),
    (6726, "machine", ("ROLLBACK_FAILED",)),
    (6734, "machine", ("RECOVERY_REQUIRED", "ROLLBACK_FAILED")),
    (6756, "machine", ("RECOVERY_REQUIRED", "ROLLBACK_FAILED")),
    (6764, "machine", ("ROLLBACK_FAILED",)),
    (7903, "user", ("RECOVERY_REQUIRED",)),
    (8010, "user", ("ROLLBACK_FAILED", "FAILED_ROLLBACK_INCOMPLETE")),
    (8029, "user", ("ROLLBACK_FAILED", "FAILED_ROLLBACK_INCOMPLETE")),
    (8035, "user", ("RECOVERY_REQUIRED", "ROLLBACK_FAILED", "FAILED_ROLLBACK_INCOMPLETE")),
    (5550, "user", ("ROLLBACK_FAILED",)),
)


def _scenes():
    for line, lane, statuses in SITES:
        states = ("INSTALLING", "RESTORING") if lane == "machine" else ("QUIESCING", "INSTALLING", "RESTORING")
        for status in statuses:
            for state in states:
                # Forward-only RECOVERY_REQUIRED saves at these sites can follow
                # an unproven placement child or registration child respectively.
                yield dict(site=line, lane=lane, status=status, fence_status=state)


def _quote(value):
    return "'" + str(value).replace("'", "''") + "'"


def _powershell_batch(tmp_path, engine, audit, control, record):
    """Use the actual Save function and entry guards, never the script body."""
    executable = shutil.which(engine)
    assert executable is not None, "Required PowerShell engine is unavailable"
    seed = tmp_path / "seed.json"
    seed.write_text(json.dumps(record), encoding="utf-8")
    scenarios = tmp_path / "scenes.json"
    scenarios.write_text(json.dumps(list(_scenes())), encoding="utf-8")
    driver = tmp_path / "terminal-audit.ps1"
    driver.write_text("\n".join((
        "function Test-LabelOrphanReleaseCandidate { return $false }",
        "$ErrorActionPreference='Stop'",
        f"$source={_quote(ROOT)}; $auditFile={_quote(audit)}; $control={_quote(control)}",
        f"$scratch={_quote(tmp_path)}; $seed={_quote(seed)}; $scenes={_quote(scenarios)}",
        f"$originalStartedAt={_quote(record['writer_fence']['session_started_at_utc'])}",
        r"""
trap {@{code=$_.FullyQualifiedErrorId;line=$_.InvocationInfo.ScriptLineNumber} | ConvertTo-Json -Compress | Set-Content -LiteralPath (Join-Path $scratch 'ps-error-summary.json');exit 92}
$tokens=$null;$errors=$null
$tree=[Management.Automation.Language.Parser]::ParseFile((Join-Path $source 'INSTALL_CANONICAL_PORTABLE.ps1'),[ref]$tokens,[ref]$errors)
if($errors.Count){throw 'INSTALLER_PARSE_FAILED'}
foreach($name in @('Save','Full','Same','Sha','Invoke-LabelResumeRollback')){
 $defs=@($tree.FindAll({param($node) $node -is [Management.Automation.Language.FunctionDefinitionAst] -and $node.Name -ceq $name},$true))
 if($defs.Count -ne 1){throw 'FUNCTION_EXTRACTION_FAILED'}
 . ([ScriptBlock]::Create($defs[0].Extent.Text))
}
. (Join-Path $source 'tools\label_writer_fence.ps1')
$shared=[Management.Automation.Language.Parser]::ParseFile((Join-Path $source 'kmtech_shared\powershell\portable.ps1'),[ref]$tokens,[ref]$errors)
$shaFunction=$shared.Find({param($node) $node -is [Management.Automation.Language.FunctionDefinitionAst] -and $node.Name -ceq 'Get-KmtechFileSha'},$true)
. ([ScriptBlock]::Create($shaFunction.Extent.Text))
# The synthetic records contain no secrets. Keep the actual Save formatter and
# atomic publisher, while avoiding ambient redaction/environment discovery.
function ConvertTo-KmtechPublicDiagnosticTree($Value){return $Value}
function ConvertTo-KmtechRedactedSinkObject($Value){return $Value}
function ReceiptSource {throw 'SUB441_RESUME_ACCEPTED_CONTEXT'}
function Assert-LabelDeadUnfinishedInstaller {throw 'SUB441_UNEXPECTED_UNFINISHED'}
$testMode=$true;$PlanOnly=$false
$lad=Split-Path -Parent (Split-Path -Parent (Split-Path -Parent (Split-Path -Parent $auditFile)))
$sourceManifest=[pscustomobject]@{source_commit=('a'*40)}
$generationSubjectSid='SUB441_OPERATOR'
$EvidencePath=''
$generationMarkerPaths=@()
$results=@();$index=0
foreach($scene in (Get-Content -LiteralPath $scenes -Raw | ConvertFrom-Json)){
 $record=Get-Content -LiteralPath $seed -Raw | ConvertFrom-Json
 # ConvertFrom-Json on PS7 otherwise turns the seed timestamp into a DateTime.
 # The installer creates this field as a string; preserve those actual bytes.
 $record.writer_fence.session_started_at_utc=$originalStartedAt
 $record.status=$scene.status
 $record | Add-Member -Force NoteProperty source_commit ('a'*40)
 $record | Add-Member -Force NoteProperty rollback ([pscustomobject]@{})
 $values=[ordered]@{}
 foreach($name in @('before','old','taskBefore','stopBefore','healthyRemovalReportBefore','removalReportExisted','existingVerified','installedPreimageInventory','replacementRollbackRoot','writerTransition','legacyFirstRegistration','healthyRemovedInstall','firstRegistrationStateVerified','firstRegistrationStarted','firstRegistrationAttempt','firstRegistrationAttemptPath','firstRegistrationStatePaths','firstRegistrationAbsentAncestors','rollbackSources','restoreSources')){$values[$name]=$null}
 $values.existingVerified=$false
 $context=if($scene.lane -ceq 'machine'){@{kind='MACHINE_WORKER_DEFERRED';target_sid='SUB441_WORKER'}}else{@{operator_sid=$generationSubjectSid;orchestrator_sha256=(Sha (Join-Path $source 'INSTALL_CANONICAL_PORTABLE.ps1'));values=$values;generation_marker_paths=@()}}
 $record | Add-Member -Force NoteProperty recovery_context $context
 $audit=$record;$auditPath=$auditFile
 $checkpoint=@($tree.FindAll({param($node) $node -is [Management.Automation.Language.CommandAst] -and $node.GetCommandName() -ceq 'Save' -and $node.Extent.StartLineNumber -eq [int]$scene.site -and $node.CommandElements[1].Extent.Text -cin @('$auditFile','$auditPath')},$true))
 if($checkpoint.Count -ne 1){throw 'SAVE_CHECKPOINT_EXTRACTION_FAILED'}
 . ([ScriptBlock]::Create($checkpoint[0].Extent.Text))
 $copy=Join-Path $scratch ('saved-'+$index+'.json')
 [IO.File]::Copy($auditFile,$copy)
 $start='UNEXPECTED_START_RETURN'
 try{[void](Start-LabelWriterFence -ControlRoot $control -SessionId 'sub441-next-session' -AttemptId 'sub441-next-attempt' -ReplacementTransactionId 'sub441-next-transaction' -SessionStartedAtUtc ([DateTime]::UtcNow.ToString('o')) -OrchestratorSha256 ('b'*64) -WriterContractSha256 ('c'*64) -AuthorityOwnedByCaller)}
 catch{if($_.Exception.Message -ceq 'LABEL_WRITER_FENCE_ALREADY_ACTIVE'){$start='LABEL_WRITER_FENCE_ALREADY_ACTIVE'}else{throw}}
 $install=[string]$record.install_root
 $ResumeRollbackAuditPath=$auditFile;$ExpectedRollbackAuditSha256=Sha $auditFile
 $resume='UNEXPECTED_RESUME_RETURN'
 try{Invoke-LabelResumeRollback}
 catch{
  $message=$_.Exception.Message
  if($message -ceq 'Rollback audit identity/status differs.'){$resume='RESUME_STATUS_REJECTED'}
  elseif($message -ceq 'Rollback context belongs to a different user or installer.'){$resume='RESUME_CONTEXT_REJECTED'}
  elseif($message -ceq 'SUB441_RESUME_ACCEPTED_CONTEXT'){$resume='RESUME_ACCEPTED_CONTEXT'}
  elseif($_.FullyQualifiedErrorId -like 'PropertyNotFoundStrict*'){$resume='RESUME_CONTEXT_PROPERTY_MISSING'}
  else{throw}
 }
 $results+=@{site=[int]$scene.site;lane=$scene.lane;status=$scene.status;fence_status=$scene.fence_status;copy=$copy;reinstall=$start;resume=$resume}
 $index++
}
$results | ConvertTo-Json -Depth 5 | Set-Content -LiteralPath (Join-Path $scratch 'ps-results.json') -Encoding UTF8
""",
    )), encoding="utf-8-sig")
    completed = subprocess.run(
        [executable, "-NoProfile", "-NonInteractive", "-ExecutionPolicy", "Bypass", "-File", str(driver)],
        capture_output=True, creationflags=subprocess.CREATE_NO_WINDOW, timeout=75,
    )
    # Never expose a raw PowerShell diagnostic or a synthetic record on failure.
    diagnostic = tmp_path / "ps-error-summary.json"
    detail = json.loads(diagnostic.read_text(encoding="utf-8-sig")) if diagnostic.exists() else {}
    assert completed.returncode == 0, f"PowerShell harness failed rc={completed.returncode}; code={detail.get('code')}; line={detail.get('line')}"
    return json.loads((tmp_path / "ps-results.json").read_text(encoding="utf-8-sig"))


def _startup_fence_gate():
    """Run the unchanged nested startup fence probe without other OS probes."""
    tree = ast.parse(Path(startup_check.__file__).read_text(encoding="utf-8-sig"))
    check = next(node for node in tree.body if isinstance(node, ast.FunctionDef) and node.name == "check_startup")
    probe = next(node for node in check.body if isinstance(node, ast.FunctionDef) and node.name == "fences")
    reasons = []
    namespace = {"fence": fence, "installation_root": lambda code: None, "code": ROOT, "reasons": reasons}
    exec(compile(ast.fix_missing_locations(ast.Module(body=[probe], type_ignores=[])), str(startup_check.__file__), "exec"), namespace)
    namespace["fences"]()
    return "ACTIVE_WRITER_FENCE" if "ACTIVE_WRITER_FENCE" in reasons else "ACCEPTED"


@pytest.mark.parametrize("engine", ("powershell.exe", "pwsh.exe"), ids=("PS51", "PS7"))
def test_worker_terminal_fences_require_administrator_release_after_owner_exits(carried, tmp_path, monkeypatch, engine, record_property):
    control, payload, audit, original_record = carried
    monkeypatch.setattr(fence, "_installer_machine_idle", lambda *args: True)

    @contextmanager
    def no_lifecycle(app_root, **kwargs):
        yield

    monkeypatch.setattr(fence, "_onboarding_lifecycle_scope", no_lifecycle)
    rows = _powershell_batch(tmp_path, engine, audit, control, original_record)
    findings = []
    for row in rows:
        raw = Path(row.pop("copy")).read_bytes()
        # This assertion rules out json.dumps/ConvertTo-Json -Compress fixtures.
        assert b'\n' in raw and re.search(rb'"status"\s*:\s+"', raw)
        saved = json.loads(raw.decode("utf-8-sig"))
        assert saved["writer_fence"] == original_record["writer_fence"]
        audit.write_bytes(raw)
        active = copy.deepcopy(payload)
        active["status"] = row["fence_status"]
        (control / fence.ACTIVE_FILENAME).write_text(json.dumps(active), encoding="utf-8")
        before = (control / fence.ACTIVE_FILENAME).read_bytes()
        row["startup"] = _startup_fence_gate()
        events = []
        try:
            onboarding.onboard_current_user(CANONICAL)
        except fence.WriterFencedError as error:
            row["first_setup"] = error.code
        else:
            pytest.fail("Unproven first setup reached the onboarding body")
        try:
            _remove(events)
        except fence.WriterFencedError as error:
            row["remove"] = error.code
        else:
            row["remove"] = "ACCEPTED_FENCE_RETAINED" if (control / fence.ACTIVE_FILENAME).exists() else "ACCEPTED_FENCE_SETTLED"
        row["resume_death_proof"] = fence.prove_dead_unfinished_installer(str(audit))
        # Entry acceptance alone is not an exit: RF/FRI previously kept active.
        remaining = (control / fence.ACTIVE_FILENAME).exists()
        row['fence_released'] = not remaining
        row['startup_after'] = _startup_fence_gate()
        row['reinstall_after'] = 'BLOCKED'
        if remaining:
            assert (control / fence.ACTIVE_FILENAME).read_bytes() == before
        else:
            # Exercise the official PS fence-start guard after settlement, then
            # retire only the new test fence before checking the app gate again.
            from tests.test_dead_installer_fence_entries import _start_new_fence
            row['reinstall_after'] = _start_new_fence(tmp_path, control)
            (control / fence.ACTIVE_FILENAME).unlink()
            assert audit.read_bytes() == raw
        row['result'] = 'P' if (row['fence_released'] and
                               row['startup_after'] != 'ACTIVE_WRITER_FENCE' and
                               row['reinstall_after'] == 'STARTED') else 'F'
        findings.append(row)
    counts = {kind: sum(row["result"] == kind for row in findings) for kind in ("F", "P")}
    record_property("terminal_audit_entry_matrix", json.dumps(findings, separators=(",", ":")))
    record_property("terminal_audit_entry_counts", json.dumps(counts))
    (tmp_path / "entry-matrix.json").write_text(json.dumps(findings, indent=2), encoding="utf-8")
    assert counts["F"] == 49, f"Terminal-audit orphan fence RED: {counts['F']} trapped / {counts['P']} accepted entry gates"


def test_terminal_save_inventory_is_bound_to_the_candidate_source():
    lines = INSTALLER.read_text(encoding="utf-8-sig").splitlines()
    for line, lane, statuses in SITES:
        assert "Save $audit" in lines[line - 1], f"Save checkpoint moved: {line}"
    assert "ConvertTo-Json -Depth 30" in lines[1770]
    assert "Stop-LabelWriterFence" in lines[6742]
    assert "unproven child" in lines[6738]
