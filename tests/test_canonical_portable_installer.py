from pathlib import Path
import re

from tools import build_portable_release_candidate as portable_builder

ROOT = Path(__file__).resolve().parents[1]
INSTALLER = ROOT / "INSTALL_CANONICAL_PORTABLE.ps1"


def _source() -> str:
    return INSTALLER.read_text(encoding="utf-8")


def test_installer_exposes_exact_one_session_fail_closed_interface() -> None:
    source = _source()
    parameter_block = source[
        source.index("param(") : source.index(")", source.index("param("))
    ]

    for name in (
        "SourceRoot",
        "InstallRoot",
        "EvidencePath",
        "PlanOnly",
        "CodePlacementOnly",
        "Rollback",
    ):
        assert re.search(rf"\${name}\b", parameter_block)
    assert "Select exactly one of PlanOnly, CodePlacementOnly, or Rollback" in source
    assert "EvidencePath must be on E:" in source
    assert "FAILED_ROLLED_BACK" in source
    assert "ROLLBACK_FAILED" in source
    assert not re.search(r"__[A-Z0-9_]+__", source)


def test_installer_is_code_only_and_defers_current_user_bindings() -> None:
    source = _source()

    assert "app_id = 'label_match'" in source
    assert "canonical_install_root = 'C:\\KMTech\\Apps\\Label_Match\\current'" in source
    assert "hkcu_run_name = 'KMTech.LabelMatch.Relay'" in source
    assert "scheduled_task_name = 'direct-sync-relay-label-match'" in source
    assert "DEFERRED_TO_UNELEVATED_PRODUCT_ONBOARDING" in source
    assert (
        "PRESERVED_UNTIL_CANONICAL_BINDING_AND_PINNED_EXACT_CLONE_CONFLICT_RESOLUTION"
        in source
    )
    assert "Register-ScheduledTask" not in source
    assert "Unregister-ScheduledTask" not in source
    assert "New-ScheduledTask" not in source
    assert "Start-ScheduledTask" not in source
    assert "schtasks /run" not in source.lower()
    assert "Stop-Process" not in source
    assert "Stop-ScheduledTask" not in source


def test_installer_proves_zero_pe_provenance_and_owned_rollback() -> None:
    source = _source()

    for token in (
        "runtime_python_sha256",
        "runtime_pythonw_sha256",
        "launcher_sha256",
        "canonical_installer_sha256",
        "expected_pe_count = 46",
        ".kmtech-canonical-install-owner.json",
        "installed_inventory_sha256",
        "Assert-HardenedAcl",
        "Assert-SafeOwnedWorkPath",
    ):
        assert token in source
    assert "Remove current-user setup before rolling back canonical code" in source
    assert (
        "Remove the current-user scheduled task before rolling back canonical code"
        in source
    )


def test_portable_builder_packages_and_hashes_owner_installers() -> None:
    source = (ROOT / "tools" / "build_portable_release_candidate.py").read_text(
        encoding="utf-8"
    )

    assert portable_builder.CANONICAL_INSTALLER_FILENAME == INSTALLER.name
    assert portable_builder.LEGACY_INSTALLER_FILENAME == "INSTALL_THIS_PC.ps1"
    assert "canonical_installer_sha256" in source
    assert "runtime_python_sha256" in source
    assert "shutil.copy2(installer_source" in source
    assert "shutil.copy2(legacy_installer_source" in source
