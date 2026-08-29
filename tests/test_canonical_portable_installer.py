from pathlib import Path
import re

from tools import build_portable_release_candidate as portable_builder


ROOT = Path(__file__).resolve().parents[1]
INSTALLER = ROOT / "INSTALL_CANONICAL_PORTABLE.ps1"
HELPER = ROOT / "INSTALL_THIS_PC.ps1"
INTEGRITY_HELPER = ROOT / "tools" / "bootstrap_integrity.ps1"


def _source(path: Path = INSTALLER) -> str:
    return path.read_text(encoding="utf-8")


def test_installer_exposes_inspection_equivalent_v2_interface() -> None:
    source = _source()
    parameter_block = source[
        source.index("param(") : source.index(")", source.index("param("))
    ]

    for name in (
        "SourceRoot",
        "InstallRoot",
        "EvidencePath",
        "PlanOnly",
        "AllowNoncanonicalLayoutForTest",
        "SkipSignatureValidationForTest",
    ):
        assert re.search(rf"\${name}\b", parameter_block)
    assert not re.search(r"\$CodePlacementOnly\b", parameter_block)
    assert not re.search(r"\$Rollback\b", parameter_block)
    assert not re.search(r"__[A-Z0-9_]+__", source)


def test_top_level_installer_owns_current_user_lifecycle_and_preimage() -> None:
    source = _source()

    for token in (
        "C:\\KMTech\\Apps\\Label_Match\\current",
        "KMTech.LabelMatch.Relay",
        "--label-match-user-relay",
        "label-match-portable-tree-v1",
        "label-match-canonical-portable-install-v1",
        "INSTALL_THIS_PC.ps1",
        "Product $install '--remove-current-user-setup'",
        "Product $install '--onboard-current-user'",
        "PREIMAGE_SAVED",
        "FAILED_ROLLED_BACK",
        "stop_marker_preimage",
        "REUSED_VERIFIED",
    ):
        assert token in source
    assert "(Arg $Root)" in source
    assert "Register-ScheduledTask" not in source
    assert "Start-ScheduledTask" not in source
    assert "Stop-ScheduledTask" not in source
    assert "schtasks /run" not in source.lower()


def test_code_helper_owns_privileged_placement_and_exact_rollback() -> None:
    source = _source(HELPER)

    for name in (
        "DryRun",
        "Uninstall",
        "SourceRoot",
        "InstallRoot",
        "ElevationLogPath",
        "ReplaceExistingVerifiedPortable",
    ):
        parameter_block = source[
            source.index("param(") : source.index(")", source.index("param("))
        ]
        assert re.search(rf"\${name}\b", parameter_block)
    for token in (
        "tools\\bootstrap_integrity.ps1",
        ".current.rollback.",
        "REPLACED_VERIFIED",
        "replacement_rollback_status=PRESERVED",
        "Write-ElevationLog",
    ):
        assert token in source
    assert "Register-ScheduledTask" not in source
    assert INTEGRITY_HELPER.is_file()


def test_portable_builder_packages_v2_installer_helper_and_integrity_tool() -> None:
    source = _source(ROOT / "tools" / "build_portable_release_candidate.py")

    assert portable_builder.CANONICAL_INSTALLER_FILENAME == INSTALLER.name
    assert portable_builder.LEGACY_INSTALLER_FILENAME == HELPER.name
    assert portable_builder.BOOTSTRAP_INTEGRITY_HELPER.as_posix() == (
        "tools/bootstrap_integrity.ps1"
    )
    assert "canonical_installer_sha256" in source
    assert "runtime_python_sha256" in source
    assert "shutil.copy2(installer_source" in source
    assert "shutil.copy2(legacy_installer_source" in source
    assert "shutil.copy2(bootstrap_helper_source" in source


def test_plan_only_contract_is_stdout_only_and_non_mutating() -> None:
    source = _source()
    plan_block = source[source.index("if ($PlanOnly)") : source.index("$runId =")]

    assert "install_status=PLAN_ONLY" in plan_block
    assert "registry_changed=false" in plan_block
    assert "Save " not in plan_block
    assert "Start-Process" not in plan_block
    assert "INSTALL_THIS_PC.ps1" not in plan_block
