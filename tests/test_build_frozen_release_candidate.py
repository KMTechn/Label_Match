from __future__ import annotations

from pathlib import Path
import os
import re
import shutil
import subprocess
import sys

import pytest


ROOT = Path(__file__).resolve().parents[1]
SCRIPT_PATH = ROOT / "tools" / "build_frozen_release_candidate.ps1"
CONTRACT_PATH = ROOT / "RELEASE_GATE_CONTRACT.md"


def _source() -> str:
    return SCRIPT_PATH.read_text(encoding="utf-8")


def test_script_has_valid_powershell_ast():
    powershell = shutil.which("pwsh") or shutil.which("powershell")
    if not powershell:
        pytest.skip("PowerShell is unavailable")
    command = r"""
$errors = $null
$tokens = $null
[System.Management.Automation.Language.Parser]::ParseFile(
  $env:LABEL_MATCH_CANDIDATE_SCRIPT,
  [ref]$tokens,
  [ref]$errors
) | Out-Null
if ($errors.Count -ne 0) {
  $errors | ForEach-Object { [Console]::Error.WriteLine($_.ToString()) }
  exit 1
}
"""
    completed = subprocess.run(
        [powershell, "-NoProfile", "-NonInteractive", "-Command", command],
        check=False,
        capture_output=True,
        text=True,
        encoding="utf-8",
        env={**os.environ, "LABEL_MATCH_CANDIDATE_SCRIPT": str(SCRIPT_PATH)},
    )
    assert completed.returncode == 0, completed.stderr


def test_script_requires_fresh_external_output_and_exact_offline_toolchain():
    source = _source()

    assert "[string]$OutputRoot" in source
    assert '[string]$Tag = "v2.0.67"' in source
    assert "[string]$PythonPath" in source
    assert "[string]$Wheelhouse" in source
    assert "[string]$MirrorRoot" in source
    assert "MirrorRoot must be an existing prepared local bare mirror" in source
    assert "OutputRoot must be a dedicated directory outside the release work clone and local bare mirror" in source
    assert "OutputRoot must be fresh and must not already exist" in source
    assert '[IO.FileMode]::CreateNew' in source
    assert '$ExpectedPythonVersion = "3.12.10"' in source
    assert '$ExpectedPyInstallerVersion = "6.20.0"' in source
    assert "--no-index" in source
    assert "--find-links $resolvedWheelhouse" in source
    assert "--only-binary=:all:" in source
    assert "--require-hashes" in source
    assert "--no-deps" in source
    assert "$env:PIP_NO_INDEX = \"1\"" in source


def test_script_enforces_clean_work_clone_and_exact_local_bare_mirror_topology():
    source = _source()

    assert '"rev-parse", "--is-inside-work-tree"' in source
    assert '"rev-parse", "--is-bare-repository"' in source
    assert "The builder must run from a non-bare isolated release work clone" in source
    assert "MirrorRoot must be a bare Git repository" in source
    assert "Get-NormalizedLocalOriginPath" in source
    assert "Prepared clone origin must be an absolute local path or file URI" in source
    assert "Prepared release work clone origin must be the exact supplied local bare mirror" in source
    assert '"symbolic-ref", "--quiet", "HEAD"' in source
    assert '"HEAD^{commit}"' in source
    assert '"refs/heads/main^{commit}"' in source
    assert '"refs/remotes/origin/main^{commit}"' in source
    assert '"status", "--porcelain=v1", "--untracked-files=all"' in source
    assert '$headRef -cne "refs/heads/main"' in source
    assert "$headCommit -cne $localMainCommit" in source
    assert "$headCommit -cne $originMainCommit" in source
    assert "$headCommit -cne $mirrorMainCommit" in source
    assert "HEAD, local main, origin/main, and local bare mirror main" in source
    assert "$finalTagObject -cne $mirrorTagObject" in source
    assert '$tagObjectType -cne "tag"' in source
    assert '$mirrorTagObjectType -cne "tag"' in source
    assert "$tagCommit -cne $headCommit" in source
    assert "$mirrorTagCommit -cne $headCommit" in source
    assert "verify_release_tag_attestation.py" in source
    assert "verify_release_identity.py" in source
    assert '--reviewed-ref "refs/remotes/origin/main"' in source
    assert source.index("verify_release_tag_attestation.py") < source.index(
        "verify_release_identity.py"
    )
    assert source.index("verify_release_tag_attestation.py") < source.index(
        '"--name", "Label_Match"'
    )
    assert "$postBuildTagObject -cne $finalTagObject" in source
    assert "$postBuildMirrorTagObject -cne $finalTagObject" in source
    assert '$postBuildMirrorTagType -cne "tag"' in source
    assert "$postBuildMirrorTagCommit -cne $headCommit" in source
    assert "$postBuildOriginMain -cne $headCommit" in source
    assert "FINAL tag object/type/peel" in source

    forbidden_mutations = (
        r"\bgit\s+(?:-C\s+\S+\s+)?fetch\b",
        r"\bgit\s+(?:-C\s+\S+\s+)?push\b",
        r"\bgit\s+(?:-C\s+\S+\s+)?tag\b",
        r"\bgh\s+(?:api|release)\b",
        r"\bInvoke-WebRequest\b",
        r"\bInvoke-RestMethod\b",
        r"\bcurl(?:\.exe)?\b",
    )
    for pattern in forbidden_mutations:
        assert re.search(pattern, source, flags=re.IGNORECASE) is None, pattern


def test_script_rejects_a_clean_clone_whose_origin_is_not_the_supplied_mirror(
    tmp_path,
):
    powershell = shutil.which("pwsh") or shutil.which("powershell")
    git = shutil.which("git")
    if not powershell or not git:
        pytest.skip("PowerShell and Git are required")

    expected_mirror = tmp_path / "expected-mirror.git"
    wrong_mirror = tmp_path / "wrong-mirror.git"
    work_clone = tmp_path / "work-clone"
    wheelhouse = tmp_path / "wheelhouse"
    output_root = tmp_path / "candidate-output"
    wheelhouse.mkdir()

    def run_git(*arguments: str, cwd: Path | None = None) -> None:
        completed = subprocess.run(
            [git, *arguments],
            cwd=cwd,
            check=False,
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
        )
        assert completed.returncode == 0, completed.stderr

    run_git("init", "--bare", str(expected_mirror))
    run_git("init", "--bare", str(wrong_mirror))
    run_git("init", "--initial-branch=main", str(work_clone))
    copied_script = work_clone / "tools" / SCRIPT_PATH.name
    copied_script.parent.mkdir()
    shutil.copy2(SCRIPT_PATH, copied_script)
    (work_clone / ".git" / "info" / "exclude").write_bytes(b"/tools/\n")
    run_git("remote", "add", "origin", str(wrong_mirror.resolve()), cwd=work_clone)

    completed = subprocess.run(
        [
            powershell,
            "-NoProfile",
            "-NonInteractive",
            "-File",
            str(copied_script),
            "-OutputRoot",
            str(output_root),
            "-Tag",
            "v2.0.67",
            "-PythonPath",
            sys.executable,
            "-Wheelhouse",
            str(wheelhouse),
            "-MirrorRoot",
            str(expected_mirror),
        ],
        check=False,
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
    )

    assert completed.returncode != 0
    assert (
        "Prepared release work clone origin must be the exact supplied local bare mirror"
        in completed.stdout + completed.stderr
    )
    assert not output_root.exists()


def test_script_moves_authoritative_build_and_package_gates_outside_repo():
    source = _source()

    assert '$venvRoot = Join-Path $resolvedOutputRoot "venv"' in source
    assert '$workRoot = Join-Path $resolvedOutputRoot "work"' in source
    assert '$distRoot = Join-Path $resolvedOutputRoot "dist"' in source
    assert "kmtech_factory_contracts.build_cli prepare" in source
    assert '--name", "Label_Match"' in source
    assert '"--onedir"' in source
    assert '"--windowed"' in source
    assert '"--noupx"' in source
    assert "build_release_cli_tools.py" in source
    for executable in (
        "KMTech_Logistics_Profile_Install",
        "KMTech_Logistics_Profile_Check",
        "Label_Match_Protected_Admin_Install",
        "KMTechActiveWorkProbe",
    ):
        assert executable in source
    assert source.count("-Mode build-identity") == 4
    assert "[Linq.Enumerable]::SequenceEqual" in source
    assert "verify_staged_release_installer.py" in source
    assert "tests\\test_staged_release_installer.py" in source
    assert "kmtech_factory_contracts.build_cli manifest" in source
    assert "kmtech_factory_contracts.build_cli verify" in source
    assert "--built-at-utc $builtAtUtc" in source
    assert "build_release_archive.py" in source
    assert "--source-epoch $sourceEpoch" in source


def test_script_freezes_github_stable_without_authenticode_or_private_feed():
    source = _source()

    assert 'provider = "github"' in source
    assert 'channel = "stable"' in source
    assert '($updateFields -join ",") -cne "provider,channel"' in source
    assert '$archiveReport.release_trust -cne "internal_unsigned"' in source
    assert '$archiveReport.tag_signature_verified -ne $false' in source
    assert '$archiveReport.authenticode_required -ne $false' in source
    assert "Get-AuthenticodeSignature" not in source
    assert "sign_release_executables" not in source
    assert "WINDOWS_CODE_SIGNING" not in source
    assert "PRIVATE_UPDATE_" not in source
    assert "COMPANY_UPDATE_UPLOAD" not in source


def test_script_emits_preserved_pre_push_receipt_without_tag_recreation_cycle():
    source = _source()

    for suffix in (
        ".zip",
        ".sha256",
        ".archive-verification.json",
        ".phase1-qualification.json",
        ".final-tag-identity.json",
        ".release-notes.txt",
    ):
        assert suffix in source
    assert 'canonical_tag_message = "Release $Tag"' in source
    assert 'schema_version = "label-match-pre-push-qualification-v2"' in source
    assert 'phase = "phase_b_pre_push_frozen_candidate"' in source
    assert "tag_recorded_before_release_identity_and_build = $true" in source
    assert "tag_object = $finalTagObject" in source
    assert "tag_peeled_commit = $headCommit" in source
    assert "external_post_download_parity_required = $true" in source
    for note_line in (
        '"Internal prerelease; not production-ready."',
        '"Tag: $Tag"',
        '"Commit: $headCommit"',
        '"Tree: $headTree"',
        '"Artifact: $archiveName"',
        '"Artifact-SHA256: $($archiveReport.archive_sha256)"',
        '"Artifact-Size: $($archiveReport.archive_size)"',
        '"Main-EXE-SHA256: $($archiveReport.main_exe_sha256)"',
        '"Factory-Contract-SHA256: $FactoryContractSha256"',
        '"Status: QUARANTINED_PENDING_FACTORY_QUALIFICATION"',
    ):
        assert note_line in source
    assert 'release_title = "Release $Tag"' in source
    assert 'network_used = $false' in source
    assert 'publication_mutated = $false' in source
    assert 'tag_mutated = $false' in source
    assert "provisional" not in source.lower()
    assert "Qualified-ZIP-SHA256" not in source
    assert ".tag-message.txt" not in source
    assert "replace only the local" not in source
    assert "preserve this receipt and frozen pair" in source
    assert "compare the externally downloaded ZIP/checksum" in source


def test_release_contract_prepares_and_invokes_the_governing_mirror_topology():
    contract = CONTRACT_PATH.read_text(encoding="utf-8")

    assert "existing isolated local bare mirror" in contract
    assert "separate clean,\n   non-bare release work clone" in contract
    assert "absolute path (or `file://` URI)" in contract
    assert "`refs/remotes/origin/main`" in contract
    assert "mirror `refs/heads/main`" in contract
    assert "A GitHub/HTTPS origin is not accepted here" in contract
    assert ".\\tools\\build_frozen_release_candidate.ps1" in contract
    assert "-MirrorRoot <ABSOLUTE-LOCAL-BARE-MIRROR>" in contract
    assert "performs no fetch, push, tag mutation, or" in contract
    assert "never\n   retarget the release work clone's local `origin`" in contract
