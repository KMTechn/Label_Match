from __future__ import annotations

import hashlib
import io
import json
import os
from pathlib import Path
import re
import shutil
import subprocess
import sys
import tarfile
import time

import pytest

from tools import build_portable_release_candidate as builder
from writer_sink_inventory import (
    _powershell_inventory,
    derive_writer_sink_inventory,
    writer_sink_inventory_sha256,
)


ROOT = Path(__file__).resolve().parents[1]
INSTALLER = ROOT / "INSTALL_CANONICAL_PORTABLE.ps1"
NATIVE = ROOT / "tests/_writer_transition_native.py"


def _quote(value: object) -> str:
    return "'" + str(value).replace("'", "''") + "'"


def _definitions() -> str:
    source = INSTALLER.read_text(encoding="utf-8")
    return source[: source.index("if (-not $SourceRoot) { $SourceRoot = $PSScriptRoot }")] + "\n. (Get-LabelSharedPortableLeafPath " + _quote(ROOT) + ")\n"


def _environment(root: Path) -> dict[str, str]:
    env = os.environ.copy()
    for name in ("LOCALAPPDATA", "APPDATA", "PROGRAMDATA", "TEMP", "TMP"):
        path = root / name
        path.mkdir(parents=True, exist_ok=True)
        env[name] = str(path)
    state = Path(env["TEMP"]) / "native-state"
    state.mkdir()
    env.update({
        "PYTHONDONTWRITEBYTECODE": "1",
        "KMTECH_FACTORY_INSTALL_TEST_MODE": "1",
        "KMTECH_LABEL_WRITER_TEST_MODE": "1",
        "KMTECH_LABEL_WRITER_CONTROL_ROOT": str(root / "control"),
        "LM_TRANSITION_NATIVE_STATE": str(state),
        "LM_TRANSITION_NATIVE_ADAPTER": str(NATIVE),
        "LABEL_MATCH_DIRECT_SYNC_ROOT": str(root / "direct-sync"),
        "LABEL_MATCH_SAVE_DIR": str(root / "data"),
        "LABEL_MATCH_SETTINGS_PATH": str(root / "settings.json"),
        "KM_LOGISTICS_PROFILE_PATH": str(root / "profile.json"),
    })
    for key in tuple(env):
        if key.startswith("KMTECH_LABEL_WRITER_DELEGATION_"):
            env.pop(key)
    return env


def _ps(root: Path, code: str, env: dict[str, str], *, timeout: int = 120, engine: str = "powershell.exe"):
    script = root / ("probe-" + str(time.time_ns()) + ".ps1")
    script.write_text(code, encoding="utf-8-sig")
    stdout = script.with_suffix(".stdout.log")
    stderr = script.with_suffix(".stderr.log")
    # A persistent restored relay can inherit duplicate pipe handles from
    # PowerShell. Files let us wait for the installer alone, as Product does.
    with stdout.open("w", encoding="utf-8") as out, stderr.open("w", encoding="utf-8") as err:
        completed = subprocess.run(
            [engine, "-NoProfile", "-NonInteractive", "-ExecutionPolicy", "Bypass", "-File", str(script)],
            env=env, cwd=root, stdout=out, stderr=err, text=True, timeout=timeout,
        )
    assert stdout.stat().st_size < 1024 * 1024 and stderr.stat().st_size < 1024 * 1024
    completed.stdout = stdout.read_text(encoding="utf-8", errors="replace")
    completed.stderr = stderr.read_text(encoding="utf-8", errors="replace")
    return completed


def _pin(tree: Path) -> str:
    rows = derive_writer_sink_inventory(tree / "app")
    rows += _powershell_inventory(tree / "INSTALL_THIS_PC.ps1", tree)
    rows.sort(key=lambda row: (row.source, row.source_path.casefold(), row.source_line, row.qualified_name))
    pin = writer_sink_inventory_sha256(rows)
    path = tree / "app/writer_session_fence.py"
    text = path.read_text(encoding="utf-8")
    text = re.sub(r'(?m)^WRITER_INVENTORY_SHA256 = "[0-9a-f]{64}"', f'WRITER_INVENTORY_SHA256 = "{pin}"', text)
    path.write_text(text, encoding="utf-8", newline="\n")
    path = tree / "tools/label_writer_fence.ps1"
    text = re.sub(r"(?m)^\$Script:LabelWriterFenceInventorySha256 = '[0-9a-f]{64}'", f"$Script:LabelWriterFenceInventorySha256 = '{pin}'", path.read_text(encoding="utf-8"))
    path.write_text(text, encoding="utf-8", newline="\n")
    return pin


def _manifest(tree: Path, commit: str) -> None:
    files = [path for path in tree.rglob("*") if path.is_file() and path.name not in {"portable-manifest.json", "bootstrap-integrity.json"}]
    value = {
        "schema": "label-match-portable-tree-v1", "source_commit": commit,
        "source_tree": commit, "entrypoint": "runtime/pythonw.exe app/main.py",
        "launcher": "launch-label-match.cmd", "allowed_unsigned_app_pe": [],
        "forbidden_package_roots": [], "runtime_pythonw_sha256": hashlib.sha256((tree / "runtime/pythonw.exe").read_bytes()).hexdigest(),
        "launcher_sha256": hashlib.sha256((tree / "launch-label-match.cmd").read_bytes()).hexdigest(),
        "file_count_before_manifest": len(files),
        "byte_count_before_manifest": sum(path.stat().st_size for path in files),
    }
    (tree / "portable-manifest.json").write_text(json.dumps(value), encoding="utf-8")


@pytest.fixture(scope="module")
def portable_pair(tmp_path_factory):
    root = tmp_path_factory.mktemp("writer-transition-pair")
    old = root / "old"
    old.mkdir()
    builder._copy_application(ROOT, old / "app")
    # Isolated installer children use the shipped dependency layout, even when
    # pytest itself runs in a venv whose base interpreter has no packages.
    site_packages = old / "app/site-packages"
    site_packages.mkdir()
    builder._copy_third_party(site_packages)
    runtime = old / "runtime"
    runtime.mkdir()
    home = Path(sys.base_prefix)
    for name in builder.RUNTIME_ROOT_FILES:
        shutil.copy2(home / name, runtime / name)
    # A fixture venv borrows the existing stdlib and writes under pytest's
    # assigned temporary root; application dependencies above are copied.
    (runtime / "pyvenv.cfg").write_text(f"home = {home}\ninclude-system-site-packages = true\n", encoding="utf-8")
    for relative in ("INSTALL_CANONICAL_PORTABLE.ps1", "INSTALL_THIS_PC.ps1", "tools/bootstrap_integrity.ps1", "tools/label_writer_fence.ps1", "tools/label_writer_fence_contract.json"):
        target = old / relative
        target.parent.mkdir(exist_ok=True)
        shutil.copy2(ROOT / relative, target)
    shutil.copy2(ROOT / "portable/launch-label-match.cmd", old / "launch-label-match.cmd")
    main = old / "app/main.py"
    text = main.read_text(encoding="utf-8")
    text = text.replace('if __name__ == "__main__":', 'if os.environ.get("LM_TRANSITION_NATIVE_ADAPTER"):\n    import runpy\n    runpy.run_path(os.environ["LM_TRANSITION_NATIVE_ADAPTER"])\n\nif __name__ == "__main__":')
    main.write_text(text, encoding="utf-8", newline="\n")
    old_pin = _pin(old)
    _manifest(old, "1" * 40)
    candidate = root / "candidate"
    shutil.copytree(old, candidate)
    path = candidate / "app/current_user_onboarding.py"
    # A genuine comment-only fixture revision changes source_sha256 without
    # changing membership/AST. No production source is padded or repinned.
    path.write_text(path.read_text(encoding="utf-8") + "\n# Comment-only release fixture.\n", encoding="utf-8", newline="\n")
    new_pin = _pin(candidate)
    assert old_pin != new_pin
    _manifest(candidate, "2" * 40)
    return old, candidate, old_pin, new_pin


def test_validated_transition_accepts_only_matching_derived_pins_and_unchanged_semantics(tmp_path, portable_pair):
    old, candidate, old_pin, new_pin = portable_pair
    env = _environment(tmp_path)
    result = _ps(tmp_path, _definitions() + f"\nAssert-WriterTransition {_quote(candidate)} {_quote(old)} | ConvertTo-Json -Compress", env)
    assert result.returncode == 0, result.stderr
    payload = json.loads(result.stdout)
    assert payload == {"installed_inventory_sha256": old_pin, "candidate_inventory_sha256": new_pin, "compatibility": "UNCHANGED_PRODUCTION_AST_AND_CONTRACTS"}


@pytest.fixture(scope="module")
def shared_upgrade_pair(tmp_path_factory):
    """Recorded X13-B before/after bytes with a borrowed synthetic runtime.

    Later application additions/refactors belong to portable_pair, not to the
    exact historical shared-leaf adoption declared by the installer.
    """
    accepted = "a6253d1a63cc42723216baa021040af3f6e9c4a1"
    root = tmp_path_factory.mktemp("shared-upgrade")
    candidate = root / "candidate"
    builder._copy_application(ROOT, candidate / "app")
    (candidate / "runtime").mkdir()
    for name in builder.RUNTIME_ROOT_FILES:
        shutil.copy2(Path(sys.base_prefix) / name, candidate / "runtime" / name)
    (candidate / "runtime/pyvenv.cfg").write_text(
        f"home = {sys.base_prefix}\ninclude-system-site-packages = true\n", encoding="utf-8")
    source_paths = {"app/" + p.relative_to(candidate / "app").as_posix():
                    p.relative_to(candidate / "app").as_posix()
                    for p in (candidate / "app").rglob("*") if p.is_file()}
    source_paths["app/main.py"] = "portable/main.py"
    for name in ("INSTALL_CANONICAL_PORTABLE.ps1", "INSTALL_THIS_PC.ps1",
                 "tools/bootstrap_integrity.ps1", "tools/label_writer_fence.ps1",
                 "tools/label_writer_fence_contract.json", "launch-label-match.cmd"):
        source_paths[name] = "portable/" + name if name.endswith(".cmd") else name
        target = candidate / name
        target.parent.mkdir(exist_ok=True)
        target.write_bytes((ROOT / source_paths[name]).read_bytes())
    accepted_files = set(subprocess.check_output([
        "git", "-C", str(ROOT), "ls-tree", "-r", "--name-only", accepted,
    ], text=True).splitlines())
    for relative, name in list(source_paths.items()):
        if name not in accepted_files:
            (candidate / relative).unlink()
            del source_paths[relative]
    addition = "app/kmtech_shared/powershell/portable.ps1"
    old = root / "old"
    shutil.copytree(candidate, old)
    (old / addition).unlink()
    for tree, revision in ((old, "57f52e10f3f1f740f88020ebb3e45b718a587a5b"), (candidate, accepted)):
        paths = {relative: name for relative, name in source_paths.items()
                 if tree == candidate or relative != addition}
        recorded = subprocess.check_output([
            "git", "-C", str(ROOT), "archive", "--format=tar", revision, "--", *paths.values(),
        ])
        with tarfile.open(fileobj=io.BytesIO(recorded)) as archive:
            for relative, name in paths.items():
                member = archive.getmember(name)
                assert member.isfile()
                (tree / relative).write_bytes(archive.extractfile(member).read())
    _manifest(old, "57f52e10f3f1f740f88020ebb3e45b718a587a5b")
    _manifest(candidate, accepted)
    return old, candidate


def _upgrade_preflight(candidate: Path, installed: Path) -> str:
    source = INSTALLER.read_text(encoding="utf-8")
    start = source.index("$placement = 'INSTALL_REQUIRED'")
    end = source.index("if (-not $conflictReceiptSupplied -and -not $pristineInstall)", start)
    return f"""
$source = {_quote(candidate)}
$install = {_quote(installed)}
$SkipSignatureValidationForTest = $true
$sourceManifest = Manifest $source $true
$receiptSource = PortableInventory $source
{source[start:end]}
"""


@pytest.mark.parametrize("engine", ["powershell.exe", "pwsh.exe"])
def test_shared_leaf_upgrade_preflight_and_replacement_integrity(tmp_path, shared_upgrade_pair, engine):
    old, candidate = shared_upgrade_pair
    installed = tmp_path / "installed"
    shutil.copytree(old, installed)
    env = _environment(tmp_path)
    code = _definitions() + f"""
. (Join-Path {_quote(candidate)} 'tools/bootstrap_integrity.ps1') -SharedCodeRoot {_quote(candidate)}
[void](Write-BootstrapIntegrityRecord -Root {_quote(installed)} -CodeRoot {_quote(installed)})
$before = PortableInventory {_quote(installed)}
$oldSelf = Assert-WriterTransition {_quote(installed)} {_quote(installed)}
$newSelf = Assert-WriterTransition {_quote(candidate)} {_quote(candidate)}
if ($oldSelf.compatibility -cne 'UNCHANGED_PRODUCTION_AST_AND_CONTRACTS' -or
    $newSelf.compatibility -cne 'UNCHANGED_PRODUCTION_AST_AND_CONTRACTS') {{ throw 'Invalid same-version control' }}
{_upgrade_preflight(candidate, installed)}
if ($writerTransition.compatibility -cne 'X13B_PINNED_SHARED_LEAF_ADOPTION') {{ throw 'Missing declared transition' }}
if ($before.sha256 -cnotmatch '^[0-9a-f]{{64}}$' -or
    (PortableInventory {_quote(installed)}).sha256 -cne $before.sha256) {{ throw 'Preflight mutated installed bytes' }}
$writerTransition | ConvertTo-Json -Compress
"""
    result = _ps(tmp_path, code, env, engine=engine)
    assert result.returncode == 0, result.stdout + result.stderr
    # Synthetic replacement only; no native installation or lifecycle actions.
    replacement = tmp_path / "replacement"
    shutil.copytree(candidate, replacement)
    result = _ps(tmp_path, _definitions() + f"""
. (Join-Path {_quote(candidate)} 'tools/bootstrap_integrity.ps1') -SharedCodeRoot {_quote(candidate)}
[void](Write-BootstrapIntegrityRecord -Root {_quote(replacement)} -CodeRoot {_quote(replacement)})
{_upgrade_preflight(candidate, replacement)}
if ($writerTransition.compatibility -cne 'UNCHANGED_PRODUCTION_AST_AND_CONTRACTS') {{ throw 'Invalid replacement transition' }}
if (-not (Test-Path -LiteralPath {_quote(replacement / 'app/kmtech_shared/powershell/portable.ps1')})) {{ throw 'Missing replacement leaf' }}
Write-Output 'PASS replaced full tree'
""", env, engine=engine)
    assert result.returncode == 0, result.stdout + result.stderr
    assert "PASS replaced full tree" in result.stdout


@pytest.mark.parametrize("engine", ["powershell.exe", "pwsh.exe"])
@pytest.mark.parametrize("change", ["extra", "missing", "tampered", "declared-tampered", "new-leaf-missing", "link"])
def test_shared_leaf_upgrade_rejects_unrelated_or_invalid_trees(tmp_path, shared_upgrade_pair, engine, change):
    old, candidate = shared_upgrade_pair
    installed = tmp_path / "installed"
    shutil.copytree(candidate if change == "new-leaf-missing" else old, installed)
    if change == "extra":
        (installed / "app/unrelated.txt").write_text("unexpected", encoding="utf-8")
    elif change == "missing":
        (installed / "app/assets/Item.csv").unlink()
    elif change == "tampered":
        with (installed / "app/assets/Item.csv").open("ab") as stream:
            stream.write(b"tampered")
    elif change == "declared-tampered":
        with (installed / "app/kmtech_shared.lock.json").open("ab") as stream:
            stream.write(b" ")
    elif change == "new-leaf-missing":
        (installed / "app/kmtech_shared/powershell/portable.ps1").unlink()
    env = _environment(tmp_path)
    link = installed / "app/linked"
    link_setup = f"[void](New-Item -ItemType Junction -Path {_quote(link)} -Target {_quote(old / 'app/assets')})" if change == "link" else ""
    code = _definitions() + f"""
. (Join-Path {_quote(candidate)} 'tools/bootstrap_integrity.ps1') -SharedCodeRoot {_quote(candidate)}
[void](Write-BootstrapIntegrityRecord -Root {_quote(installed)} -CodeRoot {_quote(installed)})
{link_setup}
{_upgrade_preflight(candidate, installed)}
"""
    try:
        result = _ps(tmp_path, code, env, engine=engine)
        assert result.returncode != 0, result.stdout
        reason = "reparse point" if change == "link" else (
            "WRITER_TRANSITION_SEMANTICS_DIFFER" if change == "tampered" else "WRITER_TRANSITION_SOURCE_SET_DIFFERS")
        assert reason in result.stderr, result.stdout + result.stderr
    finally:
        if link.exists():
            link.rmdir()  # Remove the junction itself, preserving its target.


@pytest.mark.parametrize("change,reason", [
    ("forged", "WRITER_TRANSITION_PIN_INVALID"),
    ("source-set", "WRITER_TRANSITION_SOURCE_SET_DIFFERS"),
    ("semantics", "WRITER_TRANSITION_SEMANTICS_DIFFER"),
])
def test_unsupported_transition_rejects_without_fence_or_product_mutation(tmp_path, portable_pair, change, reason):
    old, candidate, _, _ = portable_pair
    altered = tmp_path / "altered"
    shutil.copytree(old, altered)
    if change == "forged":
        path = altered / "app/writer_session_fence.py"
        path.write_text(re.sub(r'(?m)^WRITER_INVENTORY_SHA256 = "[0-9a-f]{64}"', 'WRITER_INVENTORY_SHA256 = "' + "a" * 64 + '"', path.read_text(encoding="utf-8")), encoding="utf-8")
    elif change == "source-set":
        (altered / "app/new_writer.py").write_text("from writer_session_fence import writer_sink\n@writer_sink('new_transition_sink')\ndef mutate():\n    pass\n", encoding="utf-8")
        _pin(altered)
    else:
        path = altered / "app/current_user_onboarding.py"
        path.write_text(path.read_text(encoding="utf-8").replace('"data_preserved": True', '"data_preserved": False'), encoding="utf-8")
        _pin(altered)
    env = _environment(tmp_path)
    state = Path(env["LM_TRANSITION_NATIVE_STATE"])
    stop = Path(env["LABEL_MATCH_DIRECT_SYNC_ROOT"]) / "control/label_match_user_relay.stop.json"
    source = INSTALLER.read_text(encoding="utf-8")
    preflight = source[source.index("$placement = 'INSTALL_REQUIRED'"):source.index("$localAuditRoot = Join-Path $lad")]
    code = _definitions() + f"""
$source = {_quote(candidate)}
$install = {_quote(altered)}
$SkipSignatureValidationForTest = $true
$sourceManifest = Manifest $source $true
$receiptSource = PortableInventory $source
. (Join-Path $source 'tools/bootstrap_integrity.ps1') -SharedCodeRoot $source
[void](Write-BootstrapIntegrityRecord -Root $install -CodeRoot $install)
{preflight}
"""
    with (tmp_path / "preflight-relay.log").open("w", encoding="utf-8") as log:
        process = subprocess.Popen([str(altered / "runtime/pythonw.exe"), "-I", "-B", str(altered / "app/main.py"), "--label-match-user-relay"], env=env, stdout=log, stderr=log)
        try:
            deadline = time.monotonic() + 15
            while not (state / "relay-tick.json").exists() and time.monotonic() < deadline:
                assert process.poll() is None
                time.sleep(0.05)
            assert (state / "relay-tick.json").exists()
            result = _ps(tmp_path, code, env)
            assert result.returncode != 0 and reason in result.stderr, result.stdout + result.stderr
            assert process.poll() is None
            assert not Path(env["KMTECH_LABEL_WRITER_CONTROL_ROOT"]).exists()
            assert not stop.exists()
            assert not list(tmp_path.rglob("current_user_removal.json"))
        finally:
            stop.parent.mkdir(parents=True, exist_ok=True)
            stop.write_text("{}", encoding="utf-8")
            try:
                process.wait(timeout=10)
            finally:
                if process.poll() is None:
                    process.terminate()
                    process.wait(timeout=5)


def _session_setup(candidate: Path, old: Path) -> str:
    source = INSTALLER.read_text(encoding="utf-8")
    initialization = source[source.index("$writerSessionId ="): source.index("$mutated = $false")]
    return f"""
$source = {_quote(candidate)}
$install = {_quote(old)}
$writerFenceControlRoot = $env:KMTECH_LABEL_WRITER_CONTROL_ROOT
. (Join-Path $source 'tools/label_writer_fence.ps1')
$writerTransition = Assert-WriterTransition $source $install
$frozenPlacement = [pscustomobject]@{{root=$source; writer_fence_sha256=(Sha (Join-Path $source 'tools/label_writer_fence.ps1')); integrity_sha256=(Sha (Join-Path $source 'tools/bootstrap_integrity.ps1'))}}
{initialization}
"""


def test_real_product_old_candidate_and_third_identity_admission(tmp_path, portable_pair):
    old, candidate, old_pin, new_pin = portable_pair
    env = _environment(tmp_path)
    code = _definitions() + _session_setup(candidate, old) + r"""
$authority = Enter-LabelWriterSessionAuthority $writerSessionId $writerAttemptId $writerOrchestratorSha256 $writerTransactionId $writerContractSha256
try {
    [void](Start-LabelWriterFence -ControlRoot $writerFenceControlRoot -SessionId $writerSessionId -AttemptId $writerAttemptId -ReplacementTransactionId $writerTransactionId -SessionStartedAtUtc $writerStartedAt -OrchestratorSha256 $writerOrchestratorSha256 -WriterContractSha256 $writerContractSha256 -AuthorityOwnedByCaller)
    [void](Set-LabelWriterFenceDelegation $writerFenceControlRoot 'QUIESCING' $writerSessionId $writerAttemptId $writerTransactionId $writerDelegationToken $rollbackSources 600)
    SetWriterDelegationEnvironment $writerDelegationToken $writerSessionId $writerAttemptId $writerTransactionId
    $oldDenied = $false
    try { Product $install '--remove-current-user-setup' } catch { $oldDenied = $_.Exception.Message -like '*FENCE_BINDING_INVALID*' }
    if (-not $oldDenied) { throw 'old Product was not denied under candidate pin' }
    $report = Join-Path $env:LABEL_MATCH_DIRECT_SYNC_ROOT 'status/current_user_removal.json'
    if (Test-Path $report) { throw 'denied Product wrote a removal report' }
    [void](Set-LabelWriterFenceInstalledTree $writerFenceControlRoot $writerSessionId $writerAttemptId $writerTransactionId $true)
    $env:KMTECH_LABEL_WRITER_DELEGATION_TOKEN = ''
    $undelegatedDenied = $false
    try { Product $install '--remove-current-user-setup' } catch { $undelegatedDenied = $_.Exception.Message -like '*ACTIVE_WRITER_FENCE*' }
    if (-not $undelegatedDenied) { throw 'undelegated old Product was admitted' }
    SetWriterDelegationEnvironment $writerDelegationToken $writerSessionId $writerAttemptId $writerTransactionId
    Product $install '--remove-current-user-setup'
    $oldReport = Get-Content $report -Raw | ConvertFrom-Json
    if ($oldReport.status -cne 'PASS_DATA_PRESERVED') { throw 'old removal failed' }
    $candidateDenied = $false
    try { Product $source '--remove-current-user-setup' } catch { $candidateDenied = $_.Exception.Message -like '*FENCE_BINDING_INVALID*' }
    if (-not $candidateDenied) { throw 'candidate admitted in old phase' }
    [void](Set-LabelWriterFenceInstalledTree $writerFenceControlRoot $writerSessionId $writerAttemptId $writerTransactionId $false)
    Product $source '--remove-current-user-setup'
    $candidateReport = Get-Content $report -Raw | ConvertFrom-Json
    $activePath = Get-LabelWriterFenceActivePath $writerFenceControlRoot
    $before = [IO.File]::ReadAllText($activePath)
    $third = $before | ConvertFrom-Json
    $third.writer_inventory_sha256 = 'a' * 64
    [IO.File]::WriteAllText($activePath, ($third | ConvertTo-Json -Depth 10))
    $thirdDenied = $false
    try { [void](Read-LabelWriterFence $writerFenceControlRoot) } catch { $thirdDenied = $_.Exception.Message -ceq 'LABEL_WRITER_FENCE_BINDING_INVALID' }
    if (-not $thirdDenied) { throw 'third identity admitted' }
    $reportHash = Sha $report
    $thirdProductDenied = $false
    try { Product $source '--remove-current-user-setup' } catch { $thirdProductDenied = $_.Exception.Message -like '*FENCE_BINDING_INVALID*' }
    if (-not $thirdProductDenied -or (Sha $report) -cne $reportHash) { throw 'third identity mutated product state' }
    [IO.File]::WriteAllText($activePath, $before)
    [void](Stop-LabelWriterFence $writerFenceControlRoot $writerSessionId $writerAttemptId $writerTransactionId)
    @{old_denied=$oldDenied; candidate_denied=$candidateDenied; third_denied=$thirdDenied; old_status=$oldReport.status; candidate_status=$candidateReport.status; fence_absent=(-not (Test-Path $activePath))} | ConvertTo-Json -Compress
}
finally { Exit-LabelWriterSessionAuthority $authority }
"""
    result = _ps(tmp_path, code, env)
    assert result.returncode == 0, result.stderr + result.stdout
    assert json.loads(result.stdout.splitlines()[-1]) == {
        "old_denied": True, "candidate_denied": True, "third_denied": True,
        "old_status": "PASS_DATA_PRESERVED", "candidate_status": "PASS_DATA_PRESERVED", "fence_absent": True,
    }


def test_compatibility_rejection_precedes_all_persistent_preimage_and_fence_effects():
    source = INSTALLER.read_text(encoding="utf-8")
    compatibility = source.index("$writerTransition = Assert-WriterTransition")
    assert source.index("InvokeFrozenIntegrityProbe ([pscustomobject]") < compatibility
    assert compatibility < source.index("New-Item -ItemType Directory -Path $localAuditRoot")
    assert compatibility < source.index("$taskBefore = ScheduledTaskSnapshot")
    assert compatibility < source.index("[void](Start-LabelWriterFence")


@pytest.mark.parametrize("failure", ["removal", "quiesce", "placement-move", "after-placement", "after-release"])
def test_real_removal_placement_failure_and_rollback_release_restore_relay(tmp_path, portable_pair, failure):
    old, candidate, old_pin, new_pin = portable_pair
    install = tmp_path / "canonical/current"
    shutil.copytree(old, install)
    env = _environment(tmp_path)
    if failure == "removal":
        env["LM_TRANSITION_FAIL_REMOVAL_ONCE"] = "1"
    if failure == "after-release":
        env["LM_TRANSITION_ACTIVATION_FIXTURE"] = "1"
    state = Path(env["LM_TRANSITION_NATIVE_STATE"])
    old_command = subprocess.list2cmdline([str(install / "runtime/pythonw.exe"), "-I", "-B", str(install / "app/main.py"), "--label-match-user-relay"])
    (state / "registry.txt").write_text(old_command, encoding="utf-8")
    (state / "task.txt").write_text("preimage-task", encoding="utf-8")
    logs = (tmp_path / "initial-relay.log").open("w", encoding="utf-8")
    process = subprocess.Popen([str(install / "runtime/pythonw.exe"), "-I", "-B", str(install / "app/main.py"), "--label-match-user-relay"], env=env, stdout=logs, stderr=logs)
    stop_path = Path(env["LABEL_MATCH_DIRECT_SYNC_ROOT"]) / "control/label_match_user_relay.stop.json"
    try:
        deadline = time.monotonic() + 15
        while not (state / "relay-tick.json").exists() and time.monotonic() < deadline:
            assert process.poll() is None, (tmp_path / "initial-relay.log").read_text(encoding="utf-8")
            time.sleep(0.05)
        assert (state / "relay-tick.json").exists()
        # The production helper runs in a child and owns actual staging/rename/
        # integrity recovery. Only the single chosen rename is forced to fail.
        helper_script = tmp_path / "placement-child.ps1"
        helper_script.write_text(f"""
param([string]$RestoreSource, [string]$ExpectedAggregate, [int]$ExpectedCount, [uint64]$ExpectedBytes)
$ErrorActionPreference = 'Stop'
. {_quote(candidate / 'tools/label_writer_fence.ps1')}
$lease = Enter-LabelWriterDelegatedOperation -ControlRoot $env:KMTECH_LABEL_WRITER_CONTROL_ROOT -SessionId $env:KMTECH_LABEL_WRITER_DELEGATION_SESSION_ID -AttemptId $env:KMTECH_LABEL_WRITER_DELEGATION_ATTEMPT_ID -ReplacementTransactionId $env:KMTECH_LABEL_WRITER_DELEGATION_TRANSACTION_ID -DelegationToken $env:KMTECH_LABEL_WRITER_DELEGATION_TOKEN -Source 'canonical_placement'
function Move-Item {{
    [CmdletBinding()] param([string]$LiteralPath, [string]$Destination, [switch]$Force)
    if ({_quote(failure)} -ceq 'placement-move' -and (Split-Path $LiteralPath -Leaf) -like '.current.bootstrap.*' -and $Destination -ceq {_quote(install)}) {{ throw 'injected placement rename failure' }}
    Microsoft.PowerShell.Management\\Move-Item @PSBoundParameters
}}
try {{
    & {_quote(candidate / 'INSTALL_THIS_PC.ps1')} -SourceRoot $RestoreSource -InstallRoot {_quote(install)} -AllowNoncanonicalLayoutForTest -ReplaceExistingVerifiedPortable -ExpectedSourceAggregateSha256 $ExpectedAggregate -ExpectedSourceFileCount $ExpectedCount -ExpectedSourceByteCount $ExpectedBytes
}}
finally {{ Exit-LabelWriterAdmission $lease }}
""", encoding="utf-8-sig")
        source = INSTALLER.read_text(encoding="utf-8")
        transaction = source[source.index("$mutated = $false"):]
        launcher = tmp_path / "restore-relay.py"
        launcher.write_text(
            "import json, pathlib, subprocess, sys\n"
            "command = json.loads(pathlib.Path(sys.argv[1]).read_text(encoding='utf-8-sig'))\n"
            "process = subprocess.Popen(command, stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, close_fds=True)\n"
            "print(process.pid)\n",
            encoding="utf-8",
        )
        code = _definitions() + _session_setup(candidate, install) + f"""
$state = {_quote(state)}
$testMode = $true
$SkipSignatureValidationForTest = $true
$EvidencePath = ''
$pristineInstall = $false
$healthyLifecycle = $null
$healthyRemovedInstall = $false
$healthyWithoutCode = $false
$removalReportExisted = $false
$healthyRemovalReportBefore = $null
$existingVerified = $true
$placement = 'INSTALL_REQUIRED'
$conflictReceiptSupplied = $false
$sourceManifest = Manifest $source $true
$receiptSource = PortableInventory $source
$frozenPlacement | Add-Member helper_sha256 ([string]$receiptSource.critical_file_sha256.placement_helper)
$frozenPlacement | Add-Member helper_path (Join-Path $source 'INSTALL_THIS_PC.ps1')
. (Join-Path $source 'tools/bootstrap_integrity.ps1') -SharedCodeRoot $source
[void](Write-BootstrapIntegrityRecord -Root $install -CodeRoot $install)
$installedPreimageInventory = PortableInventory $install
$before = [ordered]@{{exists=$true; kind='String'; data={_quote(old_command)}}}
$taskBefore = [ordered]@{{exists=$true; enabled=$true; data='preimage-task'}}
$stopBefore = [ordered]@{{exists=$false; sha256=''; backup_path=''}}
$stop = {_quote(stop_path)}
$statusRoot = Join-Path $env:LABEL_MATCH_DIRECT_SYNC_ROOT 'status'
$removalPath = Join-Path $statusRoot 'current_user_removal.json'
$onboardingPath = Join-Path $statusRoot 'current_user_onboarding.json'
$relayPath = Join-Path $statusRoot 'label_match_user_relay.json'
$auditPath = {_quote(tmp_path / 'audit.json')}
$elevationLogPath = {_quote(tmp_path / 'placement.log')}
$audit = [ordered]@{{status='PREIMAGE_SAVED'; rollback=[ordered]@{{available=$true; applied=$false; runtime_restored=$false}}}}
$wanted = Command $install
$onboardingArguments = @()
function Snapshot {{
    $path = Join-Path $state 'registry.txt'
    if (Test-Path $path) {{ return [ordered]@{{exists=$true; kind='String'; data=[IO.File]::ReadAllText($path)}} }}
    return [ordered]@{{exists=$false; kind=''; data=''}}
}}
function Restore($Before) {{ [IO.File]::WriteAllText((Join-Path $state 'registry.txt'), [string]$Before.data) }}
function RestoreScheduledTask($Before) {{ [IO.File]::WriteAllText((Join-Path $state 'task.txt'), [string]$Before.data) }}
function Get-ScheduledTask {{ param($TaskName, $TaskPath, $ErrorAction) return @() }}
function Get-AuthenticodeSignature {{ param($FilePath) return @{{Status='Valid'}} }}
function Relays {{
    return @(Get-CimInstance Win32_Process -Filter "Name='pythonw.exe'" | Where-Object {{ $_.ExecutablePath -ceq (Join-Path $install 'runtime\\pythonw.exe') -and $_.CommandLine -like '*--label-match-user-relay*' }})
}}
$old = @(Relays)
if ($old.Count -ne 1) {{ throw 'fixture relay preimage absent' }}
function UnquiescedProductWriters {{
    if ({_quote(failure)} -ceq 'quiesce') {{ throw 'injected quiescence query failure after real removal' }}
    return @(Relays)
}}
function InvokeFrozenPlacementHelper($Frozen, $HelperParameters) {{
    & powershell.exe -NoProfile -NonInteractive -ExecutionPolicy Bypass -File {_quote(helper_script)} -RestoreSource $HelperParameters.SourceRoot -ExpectedAggregate $HelperParameters.ExpectedSourceAggregateSha256 -ExpectedCount $HelperParameters.ExpectedSourceFileCount -ExpectedBytes $HelperParameters.ExpectedSourceByteCount >> {_quote(tmp_path / 'placement.stdout.log')} 2>> {_quote(tmp_path / 'placement.stderr.log')}
    return $LASTEXITCODE
}}
$script:realProduct = ${{function:Product}}
function Product([string]$Root, [string]$Mode, [string[]]$ExtraArguments = @()) {{
    if ($Mode -ceq '--onboard-current-user' -and {_quote(failure)} -cne 'after-release') {{ throw 'injected failure after successful placement before enrollment' }}
    & $script:realProduct $Root $Mode $ExtraArguments
}}
$script:realSave = ${{function:Save}}
$script:auditFaultConsumed = $false
function Save([string]$Path, $Value) {{
    if ({_quote(failure)} -ceq 'after-release' -and $Value.status -ceq 'PASS' -and -not $script:auditFaultConsumed) {{
        if ($writerFenceStarted -or $null -ne $writerAuthority) {{ throw 'fixture did not reach the real release boundary' }}
        $script:auditFaultConsumed = $true
        throw 'injected final audit persistence failure after actual fence release'
    }}
    & $script:realSave $Path $Value
}}
function StartRaw([string]$Line) {{
    if (Test-Path (Get-LabelWriterFenceActivePath $writerFenceControlRoot)) {{ throw 'relay restarted under active fence' }}
    foreach ($name in $WriterDelegationEnvironmentNames) {{ [Environment]::SetEnvironmentVariable($name, '', 'Process') }}
    [IO.File]::WriteAllText({_quote(tmp_path / 'restore-command.json')}, ($Line | ConvertTo-Json -Compress))
    $pidText = & (Join-Path $source 'runtime/python.exe') -I -B {_quote(launcher)} {_quote(tmp_path / 'restore-command.json')}
    if ($LASTEXITCODE -ne 0) {{ throw 'raw fixture relay creation failed' }}
    return [int]$pidText
}}
{transaction}
"""
        result = _ps(tmp_path, code, env, timeout=180)
        assert result.returncode != 0
        audit = json.loads((tmp_path / "audit.json").read_text(encoding="utf-8-sig"))
        assert audit["status"] == "FAILED_ROLLED_BACK", result.stderr[-3000:] + result.stdout[-1000:]
        assert audit["rollback"]["runtime_restored"] is True
        assert not (Path(env["KMTECH_LABEL_WRITER_CONTROL_ROOT"]) / "active.json").exists()
        assert not stop_path.exists()
        assert (state / "registry.txt").read_text(encoding="utf-8") == old_command
        assert (state / "task.txt").read_text(encoding="utf-8") == "preimage-task"
        restored_pid = int((state / "relay.pid").read_text(encoding="ascii"))
        tick = json.loads((state / "relay-tick.json").read_text(encoding="utf-8"))
        assert restored_pid != process.pid and tick["pid"] == restored_pid
        installed_manifest = json.loads((install / "portable-manifest.json").read_text(encoding="utf-8"))
        assert installed_manifest["source_commit"] == "1" * 40
        assert audit["rollback"]["code_restored"] is True
        if failure in {"after-placement", "after-release"}:
            assert audit["rollback"]["code_placement"] == "RESTORED_PREIMAGE"
    finally:
        stop_path.parent.mkdir(parents=True, exist_ok=True)
        stop_path.write_text("{}", encoding="utf-8")
        try:
            process.wait(timeout=10)
        finally:
            if process.poll() is None:
                process.terminate()
                process.wait(timeout=5)
            logs.close()
            # Only the exact fixture executable can be cleaned up here.
            _ps(tmp_path, f"$ErrorActionPreference='Stop'\nGet-CimInstance Win32_Process -Filter \"Name='pythonw.exe'\" | Where-Object {{ $_.ExecutablePath -ceq {_quote(install / 'runtime/pythonw.exe')} }} | ForEach-Object {{ Stop-Process -Id $_.ProcessId -ErrorAction SilentlyContinue }}", env)
