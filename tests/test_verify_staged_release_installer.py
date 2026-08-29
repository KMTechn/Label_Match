import importlib.util
import json
from pathlib import Path
import sys

import pytest


MODULE_PATH = (
    Path(__file__).resolve().parents[1]
    / "tools"
    / "verify_staged_release_installer.py"
)
SPEC = importlib.util.spec_from_file_location(
    "verify_staged_release_installer_for_tests", MODULE_PATH
)
assert SPEC and SPEC.loader
verifier = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = verifier
SPEC.loader.exec_module(verifier)


def _package(tmp_path: Path) -> Path:
    root = tmp_path / "Label_Match"
    internal = root / "_internal" / "config"
    internal.mkdir(parents=True)
    installer_source = Path(__file__).resolve().parents[1] / "INSTALL_THIS_PC.ps1"
    helper_source = (
        Path(__file__).resolve().parents[1] / "tools" / "bootstrap_integrity.ps1"
    )
    (root / "INSTALL_THIS_PC.ps1").write_text(
        installer_source.read_text(encoding="utf-8-sig"),
        encoding="utf-8",
    )
    (root / "tools").mkdir()
    (root / "tools" / "bootstrap_integrity.ps1").write_text(
        helper_source.read_text(encoding="utf-8-sig"),
        encoding="utf-8",
    )
    (root / "Label_Match.exe").write_bytes(b"onedir product host")
    (root / "contract.lock.json").write_text("{}\n", encoding="utf-8")
    (internal / "app_settings.json").write_text("{}\n", encoding="utf-8")
    (root / "_internal" / "python312.dll").write_bytes(b"runtime")
    return root


def test_verifier_proves_code_only_current_user_topology(tmp_path):
    package = _package(tmp_path)

    report = verifier.verify_staged_package(package)

    assert report["status"] == "PASS"
    assert report["schema_version"] == verifier.REPORT_SCHEMA
    assert report["runtime_host"]["package_layout"] == "onedir"
    assert report["runtime_host"]["relay_execution_boundary"] == "product_host"
    assert report["bootstrap_contract"]["elevation_points"] == ["code_placement"]
    assert report["bootstrap_contract"]["identity_profile_created"] is False
    assert report["state_contract"]["relay_persistence"] == "HKCU_RUN"
    assert report["state_contract"]["relay_port_contract"] == 18456
    assert report["state_contract"]["source_host_override_required"] is False
    assert report["legacy_authority_contract"]["system_scheduled_task_supported"] is False
    assert report["legacy_authority_contract"]["forbidden_package_members_absent"] is True
    assert report["system_python_required"] is False
    assert report["original_package_unchanged"] is True


@pytest.mark.parametrize(
    "relative", sorted(verifier.FORBIDDEN_ACTIVE_AUTHORITY_MEMBERS)
)
def test_verifier_rejects_every_retired_active_authority_member(tmp_path, relative):
    package = _package(tmp_path)
    target = package / relative
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_bytes(b"retired active authority")

    with pytest.raises(
        verifier.StagedInstallerVerificationError,
        match="retired active task-authority",
    ):
        verifier.verify_staged_package(package)


def test_verifier_rejects_task_creation_reintroduced_into_public_bootstrap(tmp_path):
    package = _package(tmp_path)
    installer = package / "INSTALL_THIS_PC.ps1"
    installer.write_text(
        installer.read_text(encoding="utf-8") + "\nRegister-ScheduledTask -TaskName bad\n",
        encoding="utf-8",
    )

    with pytest.raises(
        verifier.StagedInstallerVerificationError,
        match="retired enrollment/task authority",
    ):
        verifier.verify_staged_package(package)


def test_verifier_requires_onedir_runtime(tmp_path):
    package = _package(tmp_path)
    (package / "_internal" / "config" / "app_settings.json").unlink()

    with pytest.raises(
        verifier.StagedInstallerVerificationError,
        match="required staged members",
    ):
        verifier.verify_staged_package(package)


def test_cli_writes_exact_report(tmp_path):
    package = _package(tmp_path)
    report_path = tmp_path / "report.json"

    assert verifier.main(
        ["--package-root", str(package), "--report", str(report_path)]
    ) == 0
    payload = json.loads(report_path.read_text(encoding="utf-8"))
    assert payload["status"] == "PASS"
    assert payload["runtime_host"]["path"] == "Label_Match.exe"
