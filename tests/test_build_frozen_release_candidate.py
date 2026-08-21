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
TAG_BURNER_PATH = ROOT / "tools" / "burn_local_release_tag_once.py"
REQUIREMENTS_PATH = ROOT / "requirements-release.txt"

PANDAS_LIVE_SERVER_CLOSURE = {
    "numpy": "numpy==2.3.3 --hash=sha256:497d7cad08e7092dba36e3d296fe4c97708c93daf26643a1ae4b03f6294d30eb",
    "pandas": "pandas==2.3.2 --hash=sha256:8c13b81a9347eb8c7548f53fd9a4f08d4dfe996836543f805c987bafa03317ae",
    "python-dateutil": "python-dateutil==2.9.0.post0 --hash=sha256:a8b2bc7bffae282281c8140a97d3aa9c14da0b136dfe83f850eea9a5f7470427",
    "pytz": "pytz==2026.2 --hash=sha256:04156e608bee23d3792fd45c94ae47fae1036688e75032eea2e3bf0323d1f126",
    "six": "six==1.17.0 --hash=sha256:4721f391ed90541fddacab5acf947aa0d3dc7d27b2e1e8eda2be8970586c3274",
    "tzdata": "tzdata==2026.3 --hash=sha256:dc096730c87af6cab1b171c9d532be840741ff5d459015e7f6947bd7d7e54931",
}


def _source() -> str:
    return SCRIPT_PATH.read_text(encoding="utf-8")


def _create_subst_alias(target: Path) -> tuple[str, str]:
    if os.name != "nt":
        pytest.skip("SUBST path identity is Windows-specific")
    subst = shutil.which("subst.exe") or shutil.which("subst")
    if not subst:
        pytest.skip("subst.exe is unavailable")
    for letter in "STUVWXYZQPNMLKJIHGF":
        drive = f"{letter}:"
        if Path(f"{drive}\\").exists():
            continue
        completed = subprocess.run(
            [subst, drive, str(target)],
            check=False,
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
        )
        if completed.returncode == 0:
            return subst, drive
    pytest.skip("No unused DOS drive letter was available for SUBST")


def _remove_subst_alias(subst: str, drive: str) -> None:
    completed = subprocess.run(
        [subst, drive, "/D"],
        check=False,
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
    )
    assert completed.returncode == 0, completed.stderr
    assert not Path(f"{drive}\\").exists()


def _path_identity_prelude() -> str:
    source = _source()
    start = source.index("function Test-FullyQualifiedLocalDosPath")
    end = source.index("function Write-NewUtf8File")
    return source[start:end]


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
    assert '[string]$Tag = "v2.0.75"' in source
    assert "[string]$PythonPath" in source
    assert "[string]$Wheelhouse" in source
    assert "[string]$MirrorRoot" in source
    assert "OutputRoot must be a dedicated directory outside the release work clone and local bare mirror" in source
    assert "OutputRoot must be fresh and must not already exist" in source
    assert "LabelMatchReleasePathIdentityV1" in source
    assert "GetFileInformationByHandleEx" in source
    assert "QueryDosDeviceW" in source
    assert "Get-ProspectiveCanonicalDirectoryPath" in source
    assert 'schema_version = "label-match-release-path-identity-v1"' in source
    assert "filesystem_reparse_points_allowed = $false" in source
    assert '[IO.FileMode]::CreateNew' in source
    assert '$ExpectedPythonVersion = "3.12.10"' in source
    assert '$ExpectedPyInstallerVersion = "6.20.0"' in source
    assert "--no-index" in source
    assert "--find-links $resolvedWheelhouse" in source
    assert "--only-binary=:all:" in source
    assert "--require-hashes" in source
    assert "--no-deps" in source
    assert "$env:PIP_NO_INDEX = \"1\"" in source


def test_hash_locked_requirements_include_exact_live_server_pandas_closure():
    requirement_lines = [
        line.strip()
        for line in REQUIREMENTS_PATH.read_text(encoding="utf-8").splitlines()
        if line.strip() and not line.lstrip().startswith("#")
    ]

    for project_name, exact_line in PANDAS_LIVE_SERVER_CLOSURE.items():
        matching_lines = [
            line
            for line in requirement_lines
            if line.partition("==")[0].lower() == project_name
        ]
        assert matching_lines == [exact_line]


def test_contract_requires_exactly_once_local_tag_burner_before_builder():
    contract = CONTRACT_PATH.read_text(encoding="utf-8")

    burner_command = "python -I .\\tools\\burn_local_release_tag_once.py"
    builder_command = "pwsh -NoProfile -File .\\tools\\build_frozen_release_candidate.ps1"
    assert TAG_BURNER_PATH.is_file()
    assert burner_command in contract
    assert contract.index(burner_command) < contract.index(builder_command)
    assert "--tag v2.0.75" in contract
    assert "--expected-commit <EXACT-CANDIDATE-COMMIT>" in contract
    assert "--expected-tree <EXACT-CANDIDATE-TREE>" in contract
    assert "do not retry it" in contract
    assert "b7ec6cb414742449672bf41a3c16af8f4f756d8057cf94c6942423a5a630b7f0" in contract


def test_script_runs_only_the_static_staged_installer_gate_without_elevation():
    source = _source()

    assert "WindowsBuiltInRole]::Administrator" not in source
    assert "Frozen release candidate qualification requires an elevated" not in source
    verifier = source.index("verify_staged_release_installer.py")
    manifest = source.index("kmtech_factory_contracts.build_cli manifest")
    sealed_test = source.index('"tests\\test_staged_release_installer.py"')
    archive = source.index("build_release_archive.py")
    assert verifier < manifest < sealed_test < archive
    assert 'LABEL_MATCH_REQUIRE_STAGED_INSTALLER_TEST = "1"' in source
    assert "Run sealed staged installer release gate" in source


def test_script_enforces_clean_work_clone_and_exact_local_bare_mirror_topology():
    source = _source()

    assert '"rev-parse", "--is-inside-work-tree"' in source
    assert '"rev-parse", "--is-bare-repository"' in source
    assert "The builder must run from a non-bare isolated release work clone" in source
    assert "MirrorRoot must be a bare Git repository" in source
    assert "Get-NormalizedLocalOriginPath" in source
    assert "Prepared clone origin must be an absolute local path or file URI" in source
    assert "Prepared release work clone origin must be the exact supplied local bare mirror" in source
    assert '"rev-parse", "--show-toplevel"' in source
    assert '"rev-parse", "--show-prefix"' in source
    assert "Assert-SameReleaseDirectoryIdentity" in source
    assert "Get-ReleaseDirectoryIdentity $localOriginPath" in source
    assert "Get-ReleaseDirectoryIdentity $postBuildOriginPath" in source
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
            "v2.0.75",
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


def test_script_accepts_subst_worktree_and_mirror_identity_until_python_gate(tmp_path):
    if os.name != "nt":
        pytest.skip("SUBST path identity is Windows-specific")
    powershell = shutil.which("pwsh")
    git = shutil.which("git")
    if not powershell or not git:
        pytest.skip("PowerShell 7 and Git are required")

    release_root = tmp_path / "release"
    work_clone = release_root / "work"
    mirror = release_root / "mirror.git"
    wheelhouse = tmp_path / "wheelhouse"
    fake_python = tmp_path / "fake-python.cmd"
    temp_root = tmp_path / "temp"
    work_clone.mkdir(parents=True)
    wheelhouse.mkdir()
    temp_root.mkdir()
    fake_python.write_bytes(
        b'@echo {"version":"0.0.0","system":"Windows","machine":"AMD64","bits":"64bit"}\r\n'
    )

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

    run_git("init", "--initial-branch=main", str(work_clone))
    copied_script = work_clone / "tools" / SCRIPT_PATH.name
    copied_script.parent.mkdir()
    shutil.copy2(SCRIPT_PATH, copied_script)
    run_git("add", "tools", cwd=work_clone)
    run_git(
        "-c",
        "user.name=Label Match Test",
        "-c",
        "user.email=label-match-test@example.invalid",
        "commit",
        "-m",
        "Add builder fixture",
        cwd=work_clone,
    )
    run_git("init", "--bare", str(mirror))
    run_git("remote", "add", "origin", str(mirror.resolve()), cwd=work_clone)
    run_git("push", "-u", "origin", "main", cwd=work_clone)
    run_git(
        "-c",
        "user.name=Label Match Test",
        "-c",
        "user.email=label-match-test@example.invalid",
        "tag",
        "-a",
        "v9.9.9",
        "-m",
        "Release v9.9.9",
        cwd=work_clone,
    )
    run_git("push", "origin", "refs/tags/v9.9.9", cwd=work_clone)
    run_git(
        "fetch",
        "origin",
        "+refs/heads/main:refs/remotes/origin/main",
        cwd=work_clone,
    )

    subst, drive = _create_subst_alias(release_root)
    try:
        alias_script = Path(f"{drive}\\work\\tools\\{SCRIPT_PATH.name}")
        alias_mirror = Path(f"{drive}\\mirror.git")
        alias_output = Path(f"{drive}\\candidate-output")
        completed = subprocess.run(
            [
                powershell,
                "-NoProfile",
                "-NonInteractive",
                "-File",
                str(alias_script),
                "-OutputRoot",
                str(alias_output),
                "-Tag",
                "v9.9.9",
                "-PythonPath",
                str(fake_python),
                "-Wheelhouse",
                str(wheelhouse),
                "-MirrorRoot",
                str(alias_mirror),
            ],
            check=False,
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
            env={**os.environ, "TEMP": str(temp_root), "TMP": str(temp_root)},
        )

        combined = completed.stdout + completed.stderr
        assert completed.returncode != 0
        assert "Release Python must be exact Windows x64 CPython 3.12.10" in combined
        assert "exact isolated release work clone root" not in combined
        assert "exact supplied local bare mirror" not in combined
        assert not (release_root / "candidate-output").exists()
    finally:
        _remove_subst_alias(subst, drive)


def test_path_identity_rejects_different_directory_and_reparse_alias(tmp_path):
    if os.name != "nt":
        pytest.skip("Win32 directory identity is Windows-specific")
    powershell = shutil.which("pwsh") or shutil.which("powershell")
    cmd = shutil.which("cmd.exe") or shutil.which("cmd")
    if not powershell or not cmd:
        pytest.skip("PowerShell and cmd.exe are required")

    left = tmp_path / "left"
    right = tmp_path / "right"
    junction = tmp_path / "junction"
    temp_root = tmp_path / "temp"
    left.mkdir()
    right.mkdir()
    temp_root.mkdir()
    linked = subprocess.run(
        [cmd, "/d", "/c", "mklink", "/J", str(junction), str(left)],
        check=False,
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
    )
    assert linked.returncode == 0, linked.stderr

    mismatch_probe = tmp_path / "different-identity.ps1"
    mismatch_probe.write_text(
        _path_identity_prelude()
        + "\n$left = Get-ReleaseDirectoryIdentity $env:LEFT_PATH 'left path'\n"
        + "$right = Get-ReleaseDirectoryIdentity $env:RIGHT_PATH 'right path'\n"
        + "Assert-SameReleaseDirectoryIdentity $left $right 'DIFFERENT_DIRECTORY_REJECTED'\n",
        encoding="utf-8",
    )
    mismatch = subprocess.run(
        [powershell, "-NoProfile", "-NonInteractive", "-File", str(mismatch_probe)],
        check=False,
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
        env={
            **os.environ,
            "LEFT_PATH": str(left),
            "RIGHT_PATH": str(right),
            "TEMP": str(temp_root),
            "TMP": str(temp_root),
        },
    )
    assert mismatch.returncode != 0
    assert "DIFFERENT_DIRECTORY_REJECTED" in mismatch.stdout + mismatch.stderr

    reparse_probe = tmp_path / "reparse-identity.ps1"
    reparse_probe.write_text(
        _path_identity_prelude()
        + "\n[void](Get-ReleaseDirectoryIdentity $env:REPARSE_PATH 'reparse path')\n",
        encoding="utf-8",
    )
    reparse = subprocess.run(
        [powershell, "-NoProfile", "-NonInteractive", "-File", str(reparse_probe)],
        check=False,
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
        env={
            **os.environ,
            "REPARSE_PATH": str(junction),
            "TEMP": str(temp_root),
            "TMP": str(temp_root),
        },
    )
    assert reparse.returncode != 0
    assert "crosses a filesystem reparse point" in reparse.stdout + reparse.stderr
    junction.rmdir()
    assert left.is_dir()


@pytest.mark.parametrize(
    "bad_path",
    (r"relative\path", r"C:drive-relative", r"\\server\share", r"\\?\C:\device"),
)
def test_path_identity_rejects_nonlocal_or_nonqualified_paths(tmp_path, bad_path):
    powershell = shutil.which("pwsh") or shutil.which("powershell")
    if not powershell:
        pytest.skip("PowerShell is required")
    temp_root = tmp_path / "temp"
    temp_root.mkdir()
    probe = tmp_path / "invalid-path.ps1"
    probe.write_text(
        _path_identity_prelude()
        + "\n[void](ConvertTo-NormalizedDirectoryPath $env:BAD_PATH 'bad path')\n",
        encoding="utf-8",
    )
    completed = subprocess.run(
        [powershell, "-NoProfile", "-NonInteractive", "-File", str(probe)],
        check=False,
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
        env={
            **os.environ,
            "BAD_PATH": bad_path,
            "TEMP": str(temp_root),
            "TMP": str(temp_root),
        },
    )
    assert completed.returncode != 0
    assert "must be a fully qualified local directory path" in (
        completed.stdout + completed.stderr
    )


def test_script_rejects_alias_output_that_physically_enters_worktree(tmp_path):
    if os.name != "nt":
        pytest.skip("SUBST path identity is Windows-specific")
    powershell = shutil.which("pwsh")
    if not powershell:
        pytest.skip("PowerShell 7 is required")

    work_clone = tmp_path / "work"
    mirror = tmp_path / "mirror.git"
    wheelhouse = tmp_path / "wheelhouse"
    temp_root = tmp_path / "temp"
    copied_script = work_clone / "tools" / SCRIPT_PATH.name
    copied_script.parent.mkdir(parents=True)
    shutil.copy2(SCRIPT_PATH, copied_script)
    mirror.mkdir()
    wheelhouse.mkdir()
    temp_root.mkdir()

    subst, drive = _create_subst_alias(work_clone)
    try:
        alias_output = Path(f"{drive}\\ignored\\candidate")
        completed = subprocess.run(
            [
                powershell,
                "-NoProfile",
                "-NonInteractive",
                "-File",
                str(copied_script),
                "-OutputRoot",
                str(alias_output),
                "-Tag",
                "v9.9.9",
                "-PythonPath",
                sys.executable,
                "-Wheelhouse",
                str(wheelhouse),
                "-MirrorRoot",
                str(mirror),
            ],
            check=False,
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
            env={**os.environ, "TEMP": str(temp_root), "TMP": str(temp_root)},
        )

        assert completed.returncode != 0
        assert "OutputRoot must be a dedicated directory outside" in (
            completed.stdout + completed.stderr
        )
        assert not (work_clone / "ignored" / "candidate").exists()
    finally:
        _remove_subst_alias(subst, drive)


def test_script_rejects_changed_tracked_bytes_before_output_creation(tmp_path):
    powershell = shutil.which("pwsh") or shutil.which("powershell")
    git = shutil.which("git")
    if not powershell or not git:
        pytest.skip("PowerShell and Git are required")

    work_clone = tmp_path / "work"
    mirror = tmp_path / "mirror.git"
    wheelhouse = tmp_path / "wheelhouse"
    output_root = tmp_path / "candidate-output"
    temp_root = tmp_path / "temp"
    work_clone.mkdir()
    wheelhouse.mkdir()
    temp_root.mkdir()

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

    run_git("init", "--initial-branch=main", str(work_clone))
    copied_script = work_clone / "tools" / SCRIPT_PATH.name
    copied_script.parent.mkdir()
    shutil.copy2(SCRIPT_PATH, copied_script)
    sentinel = work_clone / "tracked-sentinel.txt"
    sentinel.write_text("reviewed bytes\n", encoding="utf-8")
    run_git("add", "tools", "tracked-sentinel.txt", cwd=work_clone)
    run_git(
        "-c",
        "user.name=Label Match Test",
        "-c",
        "user.email=label-match-test@example.invalid",
        "commit",
        "-m",
        "Add clean builder fixture",
        cwd=work_clone,
    )
    run_git("init", "--bare", str(mirror))
    sentinel.write_text("changed bytes\n", encoding="utf-8")

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
            "v9.9.9",
            "-PythonPath",
            sys.executable,
            "-Wheelhouse",
            str(wheelhouse),
            "-MirrorRoot",
            str(mirror),
        ],
        check=False,
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
        env={**os.environ, "TEMP": str(temp_root), "TMP": str(temp_root)},
    )

    assert completed.returncode != 0
    assert "must be clean before the one-shot build" in completed.stdout + completed.stderr
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
    assert 'schema_version = "label-match-release-path-identity-v1"' in source
    assert "revalidated_after_build = $true" in source
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
