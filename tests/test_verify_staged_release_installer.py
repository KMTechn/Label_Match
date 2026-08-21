from __future__ import annotations

import ctypes
import importlib.util
import json
from pathlib import Path
import shutil
import sys

import pytest


MODULE_PATH = Path(__file__).resolve().parents[1] / "tools" / "verify_staged_release_installer.py"
SPEC = importlib.util.spec_from_file_location("verify_staged_release_installer_for_tests", MODULE_PATH)
assert SPEC and SPEC.loader
verifier = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = verifier
SPEC.loader.exec_module(verifier)


@pytest.mark.skipif(verifier.os.name != "nt", reason="Windows-only path alias behavior")
def test_same_path_accepts_8dot3_alias_but_rejects_a_different_file(tmp_path):
    target = tmp_path / "long-staged-installer-directory" / "Label_Match" / "_internal"
    target.mkdir(parents=True)
    settings = target / "app_settings.json"
    settings.write_text("{}\n", encoding="utf-8")
    other = target / "other_settings.json"
    other.write_text("{}\n", encoding="utf-8")
    buffer = ctypes.create_unicode_buffer(32768)
    length = ctypes.windll.kernel32.GetShortPathNameW(str(settings), buffer, len(buffer))
    if not length:
        pytest.skip("8.3 aliases are disabled on this volume")
    short_alias = buffer.value
    if verifier.os.path.normcase(verifier.os.path.abspath(short_alias)) == verifier.os.path.normcase(
        verifier.os.path.abspath(settings)
    ):
        pytest.skip("Windows returned the long spelling instead of an 8.3 alias")

    assert verifier._same_path(short_alias, settings)
    assert not verifier._same_path(short_alias, other)


def _write_manifest(root: Path) -> None:
    inventory = verifier._inventory(root)
    manifest = {
        "build_manifest_schema_version": 1,
        "payload_inventory": sorted(inventory, key=lambda item: str(item["path"])),
        "payload_inventory_sha256": verifier._inventory_digest(
            sorted(inventory, key=lambda item: str(item["path"]))
        ),
    }
    (root / "build-manifest.json").write_text(
        json.dumps(manifest, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )


def _package(tmp_path: Path) -> Path:
    root = tmp_path / "Label_Match"
    (root / "tools/direct_sync_relay_install_pack/_internal").mkdir(parents=True)
    (root / "config").mkdir(parents=True)
    (root / "_internal/config").mkdir(parents=True)
    (root / "INSTALL_THIS_PC.ps1").write_text("# public fixture\n", encoding="utf-8")
    (root / "install_label_match_direct_sync.ps1").write_text("# nested fixture\n", encoding="utf-8")
    (root / "Label_Match.exe").write_bytes(b"app")
    (root / "config/app_settings.json").write_text("{}\n", encoding="utf-8")
    (root / "_internal/config/app_settings.json").write_text("{}\n", encoding="utf-8")
    (root / "tools/direct_sync_relay_install_pack/direct_sync_relay_install_pack.exe").write_bytes(b"install")
    (root / "tools/direct_sync_relay_install_pack/_internal/python312.dll").write_bytes(b"runtime")
    (root / "tools/direct_sync_relay_runner.exe").write_bytes(b"runner")
    (root / "tools/register_label_match_worker_pc.exe").write_bytes(b"register")
    _write_manifest(root)
    return root


class _Completed:
    returncode = 0


def _fake_run_with_reports(command, **kwargs):
    command = [str(part) for part in command]
    extracted_root = Path(command[command.index("-File") + 1]).parent
    install_root = Path(command[command.index("-InstallRootForTest") + 1])
    program_data = Path(command[command.index("-ProgramDataRoot") + 1])
    scan_source = Path(command[command.index("-ScanSourceDir") + 1])
    common_programs = Path(command[command.index("-CommonProgramsRootForTest") + 1])
    receipt_root = Path(command[command.index("-RollbackReceiptRootForTest") + 1])
    shutil.copytree(extracted_root, install_root)
    runner = install_root / "tools/direct_sync_relay_runner.exe"
    registration = install_root / "tools/register_label_match_worker_pc.exe"
    settings = install_root / "_internal/config/app_settings.json"
    settings.write_text(json.dumps({"custom_save_path": str(scan_source)}), encoding="utf-8")
    manifest_sha = verifier._sha256(extracted_root / "build-manifest.json")
    public_report = {
        "report_version": "label-match-public-install-v2",
        "status": "DRY_RUN_STAGED",
        "source_manifest_sha256": manifest_sha,
        "install_root": str(install_root),
        "staging": {
            "ordinary_extracted_root_supported": True,
            "manifest_validated": True,
            "manifest_hashes_and_sizes_verified": True,
            "safe_relative_paths_verified": True,
            "unexpected_payload_files_absent": True,
            "same_volume_candidate_verified": True,
            "candidate_byte_parity_verified": True,
            "atomic_rename_used": True,
            "nested_label_match_directory_absent": True,
            "created_install_parent_paths": [str(install_root.parent)],
            "created_receipt_directory_paths": [str(receipt_root)],
        },
        "launcher_contract": {
            "count": 1,
            "scope": "all_users",
            "path": str(common_programs / "KMTech/Label Match.lnk"),
            "target": str(install_root / "Label_Match.exe"),
            "working_directory": str(install_root),
            "icon": str(install_root / "Label_Match.exe"),
        },
        "removal_contract": {
            "uninstall": "DATA_PRESERVING_UNINSTALL",
            "rollback": "EXACT_FRESH_TARGET_ROLLBACK",
            "task_operations": ["stop", "delete", "absence"],
            "task_results_are_typed": True,
            "rollback_evidence_external": True,
            "evidence_maximum_files": 10000,
            "evidence_maximum_bytes": 2147483648,
            "no_plaintext_secrets_in_reports": True,
            "fresh_evidence_root_required": True,
            "reparse_points_rejected": True,
            "directory_ancestry_tracked": True,
            "typed_task_reports_bound_to_phase_and_identity": True,
            "public_wrapper_finalizes_rollback_report": True,
            "final_evidence_bytes_reverified": True,
            "final_receipt_binds_evidence_hashes": True,
            "app_inventory_contract": verifier.APP_INVENTORY_CONTRACT,
            "mutable_app_relative_paths": verifier.MUTABLE_APP_RELATIVE_PATHS,
            "immutable_app_drift_rejected": True,
        },
    }
    receipt_root.mkdir(parents=True)
    (receipt_root / "label_match_public_install_report.json").write_text(
        json.dumps(public_report), encoding="utf-8"
    )
    install_report = {
        "status": "DRY_RUN",
        "field_layout_contract": {
            "expected_install_root": verifier.CANONICAL_INSTALL_ROOT,
            "expected_direct_sync_root": verifier.CANONICAL_DIRECT_SYNC_ROOT,
            "expected_task_name": verifier.CANONICAL_TASK_NAME,
            "expected_task_launcher_path": verifier.CANONICAL_TASK_LAUNCHER_PATH,
            "expected_state_db_path": verifier.CANONICAL_STATE_DB_PATH,
            "local_test_override_enabled": True,
        },
        "runner_exe": str(runner),
        "runner_command": [str(runner), "--help"],
        "app_settings_path": str(settings),
        "self_enrollment": {
            "registration_command_mode": "bundled_executable",
            "registration_executable": str(registration),
        },
    }
    status = program_data / "status"
    status.mkdir(parents=True)
    (status / "label_match_direct_sync_install.json").write_text(
        json.dumps(install_report), encoding="utf-8"
    )
    (status / "label_match_one_step_install_summary.json").write_text(
        json.dumps(
            {
                "installer_report_version": "label-match-direct-sync-one-step-install-v2",
                "lifecycle_contract": {
                    "task_removal_order": ["stop", "delete", "absence"],
                    "fresh_evidence_root_required": True,
                    "reparse_points_rejected": True,
                    "directory_ancestry_tracked": True,
                    "typed_task_reports_bound_to_phase_and_identity": True,
                    "public_wrapper_finalizes_rollback_report": True,
                    "final_evidence_bytes_reverified": True,
                    "final_receipt_binds_evidence_hashes": True,
                    "app_inventory_contract": verifier.APP_INVENTORY_CONTRACT,
                    "mutable_app_relative_paths": verifier.MUTABLE_APP_RELATIVE_PATHS,
                    "immutable_app_drift_rejected": True,
                },
                "resources": {
                    "app_root": {
                        "inventory_contract": verifier.APP_INVENTORY_CONTRACT,
                        "mutable_relative_paths": verifier.MUTABLE_APP_RELATIVE_PATHS,
                        "immutable_file_count": 7,
                        "immutable_inventory_sha256": "a" * 64,
                    },
                    "created_directory_paths": {
                        "data_root": [str(scan_source)],
                        "direct_sync_root": [str(program_data)],
                        "machine_profile_root": [],
                        "launcher_parent": [str(common_programs / "KMTech")],
                    }
                },
            }
        ),
        encoding="utf-8",
    )
    kwargs["stdout"].write(b"dry run\n")
    kwargs["stdout"].flush()
    return _Completed()


@pytest.mark.skipif(verifier.os.name != "nt", reason="Windows-only staged installer verifier")
def test_verify_staged_installer_proves_public_staging_and_lifecycle_contract(tmp_path, monkeypatch):
    package = _package(tmp_path)
    before = verifier._inventory(package)
    monkeypatch.setattr(
        verifier.shutil,
        "which",
        lambda _name: "C:/Windows/System32/WindowsPowerShell/v1.0/powershell.exe",
    )
    monkeypatch.setattr(verifier.subprocess, "run", _fake_run_with_reports)

    report = verifier.verify_staged_installer(package)

    assert report["status"] == "PASS"
    assert report["schema_version"].endswith("v2")
    assert report["proof_classification"] == "STATIC_ISOLATED_DRY_RUN"
    assert report["dynamic_qualification"] == "NOT_TESTED"
    assert report["staging_contract"]["ordinary_extracted_root"] is True
    assert report["launcher_contract"]["scope"] == "all_users"
    assert report["removal_contract"]["task_operations"] == ["stop", "delete", "absence"]
    assert report["removal_contract"]["uninstall_preserves_business_data"] is True
    assert report["removal_contract"]["directory_ancestry_tracked"] is True
    assert report["removal_contract"]["mutable_app_relative_paths"] == [
        "_internal/config/app_settings.json"
    ]
    assert verifier._inventory(package) == before


@pytest.mark.skipif(verifier.os.name != "nt", reason="Windows-only staged installer verifier")
def test_verify_staged_installer_rejects_python_runner_fallback(tmp_path, monkeypatch):
    package = _package(tmp_path)
    monkeypatch.setattr(
        verifier.shutil,
        "which",
        lambda _name: "C:/Windows/System32/WindowsPowerShell/v1.0/powershell.exe",
    )

    def fake_python_report(command, **kwargs):
        completed = _fake_run_with_reports(command, **kwargs)
        command = [str(part) for part in command]
        program_data = Path(command[command.index("-ProgramDataRoot") + 1])
        report_path = program_data / "status/label_match_direct_sync_install.json"
        payload = json.loads(report_path.read_text(encoding="utf-8"))
        payload["runner_command"][0] = "C:/Python312/python.exe"
        report_path.write_text(json.dumps(payload), encoding="utf-8")
        return completed

    monkeypatch.setattr(verifier.subprocess, "run", fake_python_report)
    with pytest.raises(verifier.StagedInstallerVerificationError, match="bundled runtime"):
        verifier.verify_staged_installer(package)


@pytest.mark.parametrize("unsafe", ["../escape", "C:/escape", "file:stream", "a\\b"])
def test_manifest_rejects_windows_unsafe_paths(tmp_path, unsafe):
    package = _package(tmp_path)
    manifest_path = package / "build-manifest.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    manifest["payload_inventory"][0]["path"] = unsafe
    manifest_path.write_text(json.dumps(manifest), encoding="utf-8")

    with pytest.raises(verifier.StagedInstallerVerificationError, match="unsafe"):
        verifier._validate_manifest_payload(package)


def test_manifest_rejects_tamper_and_unexpected_file(tmp_path):
    package = _package(tmp_path / "tamper")
    (package / "Label_Match.exe").write_bytes(b"changed")
    with pytest.raises(verifier.StagedInstallerVerificationError, match="byte mismatch"):
        verifier._validate_manifest_payload(package)

    package = _package(tmp_path / "extra")
    (package / "rogue.txt").write_text("rogue", encoding="utf-8")
    with pytest.raises(verifier.StagedInstallerVerificationError, match="unexpected"):
        verifier._validate_manifest_payload(package)


@pytest.mark.skipif(verifier.os.name != "nt", reason="Windows-only staged installer verifier")
def test_verify_staged_installer_rejects_output_overflow(tmp_path, monkeypatch):
    package = _package(tmp_path)
    monkeypatch.setattr(
        verifier.shutil,
        "which",
        lambda _name: "C:/Windows/System32/WindowsPowerShell/v1.0/powershell.exe",
    )

    def fake_overflow(command, **kwargs):
        kwargs["stdout"].write(b"x" * (verifier.MAXIMUM_OUTPUT_BYTES + 1))
        kwargs["stdout"].flush()
        return _Completed()

    monkeypatch.setattr(verifier.subprocess, "run", fake_overflow)
    with pytest.raises(verifier.StagedInstallerVerificationError, match="exceeded"):
        verifier.verify_staged_installer(package)
