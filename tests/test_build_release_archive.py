from __future__ import annotations

import importlib.util
import json
from pathlib import Path
import sys
import zipfile

import pytest


MODULE_PATH = Path(__file__).resolve().parents[1] / "tools" / "build_release_archive.py"
SPEC = importlib.util.spec_from_file_location("build_release_archive_for_tests", MODULE_PATH)
assert SPEC and SPEC.loader
archive_builder = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = archive_builder
SPEC.loader.exec_module(archive_builder)
APPROVED_SIGNER = "A" * 40
TIMESTAMP_SIGNER = "B" * 40
_REAL_BUILD_RELEASE_ARCHIVE = archive_builder.build_release_archive


@pytest.fixture(autouse=True)
def _approved_live_authenticode(monkeypatch):
    monkeypatch.setattr(
        archive_builder,
        "_read_authenticode_signature",
        lambda _path: {
            "status": "Valid",
            "signer_thumbprint": APPROVED_SIGNER,
            "timestamp_thumbprint": TIMESTAMP_SIGNER,
        },
    )

    def build_with_approved_signer(*args, **kwargs):
        kwargs.setdefault("expected_signer_thumbprint", APPROVED_SIGNER)
        return _REAL_BUILD_RELEASE_ARCHIVE(*args, **kwargs)

    monkeypatch.setattr(archive_builder, "build_release_archive", build_with_approved_signer)


def _write(path: Path, payload: bytes) -> dict[str, object]:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(payload)
    return {
        "size": path.stat().st_size,
        "sha256": archive_builder._sha256(path),
    }


def _refresh_staged_inventory(root: Path) -> None:
    staged_path = root / "staged-installer-verification.json"
    staged = json.loads(staged_path.read_text(encoding="utf-8"))
    staged_path.unlink()
    inventory = archive_builder._inventory(root)
    staged["original_package_inventory"] = inventory
    staged["original_package_inventory_sha256"] = archive_builder._inventory_digest(inventory)
    staged["original_package_file_count"] = len(inventory)
    staged_path.write_text(json.dumps(staged), encoding="utf-8")


def _package(tmp_path: Path) -> Path:
    root = tmp_path / "dist" / "Label_Match"
    tools = root / "tools"
    signer = "A" * 40
    timestamp = "B" * 40
    signed_paths = {
        "Label_Match.exe": b"main signed exe",
        "tools/direct_sync_relay_runner.exe": b"runner signed exe",
        "tools/direct_sync_relay_install_pack/direct_sync_relay_install_pack.exe": b"installer signed exe",
        "tools/register_label_match_worker_pc.exe": b"registration signed exe",
    }
    signed_entries = []
    for relative, payload in signed_paths.items():
        target = root / relative
        details = _write(target, payload)
        signed_entries.append(
            {
                "path": relative,
                **details,
                "status": "Valid",
                "signer_thumbprint": signer,
                "timestamp_thumbprint": timestamp,
            }
        )
    runtime = root / "tools/direct_sync_relay_install_pack/_internal/python312.dll"
    runtime_details = _write(runtime, b"runtime")
    installer = root / "install_label_match_direct_sync.ps1"
    installer.write_text("# installer\n", encoding="utf-8")
    (root / "_internal/config").mkdir(parents=True)
    (root / "_internal/config/app_settings.json").write_text("{}\n", encoding="utf-8")
    for source_name in (
        "direct_sync_relay_runner.py",
        "direct_sync_relay_operator.py",
        "direct_sync_relay_install_pack.py",
        "direct_sync_phase_g_label_match_runtime_report.py",
        "register_label_match_worker_pc.py",
    ):
        (tools / source_name).write_text("# fixture\n", encoding="utf-8")
    (tools / "enrollment_token.txt.template").write_text(
        "Tokenless self-enrollment is the production default.\n",
        encoding="utf-8",
    )

    pre = tools / "release_cli_tools_manifest.json"
    pre.write_text(
        json.dumps(
            {
                "status": "PASS",
                "artifact_phase": "unsigned_pre_sign",
                "tools": [
                    {"name": "direct_sync_relay_runner", "mode": "onefile"},
                    {"name": "direct_sync_relay_install_pack", "mode": "onedir"},
                    {"name": "register_label_match_worker_pc", "mode": "onefile"},
                ],
            }
        ),
        encoding="utf-8",
    )
    authenticode = root / "authenticode-manifest.json"
    authenticode.write_text(
        json.dumps(
            {
                "status": "PASS",
                "signer_thumbprint": signer,
                "executables": signed_entries,
            }
        ),
        encoding="utf-8",
    )
    post = {
        "schema_version": "label-match-release-cli-tools-v1",
        "status": "PASS",
        "artifact_phase": "signed_post_sign",
        "commit": "a" * 40,
        "tree": "b" * 40,
        "app_version": "v2.0.36",
        "pre_sign_manifest": pre.name,
        "pre_sign_manifest_sha256": archive_builder._sha256(pre),
        "authenticode_manifest": authenticode.name,
        "authenticode_manifest_sha256": archive_builder._sha256(authenticode),
        "authenticode_verification": signed_entries,
        "probe_policy": {
            "probe_count": 3,
            "help_timeout_seconds": 15.0,
            "fresh_copy_per_probe": True,
            "isolated_environment_per_probe": True,
            "residual_process_policy": "fail_closed_new_exact_executable_path_with_baseline",
        },
        "tools": [
            {
                "name": "direct_sync_relay_runner",
                "mode": "onefile",
                "payload_inventory": [
                    {
                        "path": "direct_sync_relay_runner.exe",
                        **{
                            key: signed_entries[1][key]
                            for key in ("size", "sha256")
                        },
                    }
                ],
            },
            {
                "name": "direct_sync_relay_install_pack",
                "mode": "onedir",
                "payload_inventory": [
                    {
                        "path": "direct_sync_relay_install_pack.exe",
                        **{
                            key: signed_entries[2][key]
                            for key in ("size", "sha256")
                        },
                    },
                    {"path": "_internal/python312.dll", **runtime_details},
                ],
            },
            {
                "name": "register_label_match_worker_pc",
                "mode": "onefile",
                "payload_inventory": [
                    {
                        "path": "register_label_match_worker_pc.exe",
                        **{
                            key: signed_entries[3][key]
                            for key in ("size", "sha256")
                        },
                    }
                ],
            },
        ],
    }
    signed_by_path = {entry["path"]: entry for entry in signed_entries}
    for tool in post["tools"]:
        executable_path = (
            f"tools/{tool['name']}/{tool['name']}.exe"
            if tool["mode"] == "onedir"
            else f"tools/{tool['name']}.exe"
        )
        executable = signed_by_path[executable_path]
        tool.update(
            {
                "executable_sha256": executable["sha256"],
                "executable_size": executable["size"],
                "archive_verification": {
                    "status": "PASS",
                    "viewer_stdout_sha256": "e" * 64,
                    "viewer_stderr_bytes": 0,
                },
                "help_runs": [
                    {
                        "run": run_no,
                        "status": "PASS",
                        "returncode": 0,
                        "stdout_bytes": 100,
                        "elapsed_ms": 500,
                        "stderr_bytes": 0,
                        "residual_process_count": 0,
                        "probe_executable_sha256": executable["sha256"],
                    }
                    for run_no in range(1, 4)
                ],
            }
        )
    (tools / "release_cli_tools_post_sign_manifest.json").write_text(
        json.dumps(post),
        encoding="utf-8",
    )
    (root / "staged-installer-verification.json").write_text(
        json.dumps(
            {
                "schema_version": "label-match-staged-installer-verification-v1",
                "status": "PASS",
                "installer_status": "DRY_RUN",
                "system_python_required": False,
                "app_settings_path": "_internal/config/app_settings.json",
                "app_save_path_matches_relay_scan_source": True,
                "original_package_unchanged": True,
                "stdout_bytes": 100,
                "stderr_bytes": 0,
                "installer": {
                    "path": "install_label_match_direct_sync.ps1",
                    "sha256": archive_builder._sha256(installer),
                },
                "install_helper": {
                    "path": "tools/direct_sync_relay_install_pack/direct_sync_relay_install_pack.exe",
                    "sha256": signed_entries[2]["sha256"],
                },
                "runner": {
                    "path": "tools/direct_sync_relay_runner.exe",
                    "sha256": signed_entries[1]["sha256"],
                    "selected": True,
                },
                "registration": {
                    "path": "tools/register_label_match_worker_pc.exe",
                    "sha256": signed_entries[3]["sha256"],
                    "selected": True,
                },
            }
        ),
        encoding="utf-8",
    )
    (root / "release-identity.json").write_text(
        json.dumps(
            {
                "schema_version": "label-match-release-identity-v2",
                "status": "PASS",
                "tag": "v2.0.36",
                "app_version": "v2.0.36",
                "commit": "a" * 40,
                "tree": "b" * 40,
                "clean_checkout": True,
                "annotated_tag": True,
                "tag_signature_verified": True,
                "reviewed_main_ancestor": True,
                "reviewed_ref_exact": True,
            }
        ),
        encoding="utf-8",
    )
    _refresh_staged_inventory(root)
    return root


def test_build_release_archive_is_deterministic_and_byte_exact(tmp_path):
    package = _package(tmp_path)
    first = tmp_path / "one.zip"
    second = tmp_path / "two.zip"

    first_report = archive_builder.build_release_archive(package, first, source_epoch=1_700_000_000)
    second_report = archive_builder.build_release_archive(package, second, source_epoch=1_700_000_000)

    assert first_report["status"] == "PASS"
    assert first_report["byte_parity"] is True
    assert first_report["exact_membership"] is True
    assert first_report["install_onedir_runtime_file_count"] == 1
    assert first_report["signed_executable_count"] == 4
    assert first_report["archive_sha256"] == second_report["archive_sha256"]
    assert first.read_bytes() == second.read_bytes()
    with zipfile.ZipFile(first) as archive:
        names = set(archive.namelist())
    assert "Label_Match/tools/direct_sync_relay_install_pack/_internal/python312.dll" in names
    assert len(names) == first_report["package_file_count"]


def test_build_release_archive_rejects_changed_onedir_runtime(tmp_path):
    package = _package(tmp_path)
    runtime = package / "tools/direct_sync_relay_install_pack/_internal/python312.dll"
    runtime.write_bytes(b"changed")

    with pytest.raises(archive_builder.ReleaseArchiveError, match="packaged helper payload mismatch"):
        archive_builder.build_release_archive(package, tmp_path / "bad.zip", source_epoch=1_700_000_000)


def test_build_release_archive_rejects_extra_onedir_file_or_missing_installer(tmp_path):
    package = _package(tmp_path)
    rogue = package / "tools/direct_sync_relay_install_pack/_internal/rogue.dll"
    rogue.write_bytes(b"rogue")
    with pytest.raises(archive_builder.ReleaseArchiveError, match="onedir membership mismatch"):
        archive_builder.build_release_archive(package, tmp_path / "rogue.zip", source_epoch=1_700_000_000)

    rogue.unlink()
    (package / "install_label_match_direct_sync.ps1").unlink()
    _refresh_staged_inventory(package)
    with pytest.raises(archive_builder.ReleaseArchiveError, match="installer hash mismatch"):
        archive_builder.build_release_archive(package, tmp_path / "missing.zip", source_epoch=1_700_000_000)


def test_build_release_archive_never_overwrites_existing_archive(tmp_path):
    package = _package(tmp_path)
    archive = tmp_path / "existing.zip"
    archive.write_bytes(b"keep")

    with pytest.raises(archive_builder.ReleaseArchiveError, match="already exists"):
        archive_builder.build_release_archive(package, archive, source_epoch=1_700_000_000)
    assert archive.read_bytes() == b"keep"


def test_build_release_archive_rejects_identity_post_sign_provenance_mismatch(tmp_path):
    package = _package(tmp_path)
    identity_path = package / "release-identity.json"
    identity = json.loads(identity_path.read_text(encoding="utf-8"))
    identity["commit"] = "c" * 40
    identity_path.write_text(json.dumps(identity), encoding="utf-8")
    _refresh_staged_inventory(package)

    with pytest.raises(archive_builder.ReleaseArchiveError, match="post-sign commit differ"):
        archive_builder.build_release_archive(package, tmp_path / "wrong-id.zip", source_epoch=1_700_000_000)


@pytest.mark.parametrize(
    ("field", "value", "message"),
    [
        ("pre_sign_manifest", "../outside.json", "predecessor manifest name"),
        ("authenticode_manifest", "../outside.json", "Authenticode manifest name"),
    ],
)
def test_build_release_archive_rejects_external_manifest_binding(tmp_path, field, value, message):
    package = _package(tmp_path)
    post_path = package / "tools/release_cli_tools_post_sign_manifest.json"
    post = json.loads(post_path.read_text(encoding="utf-8"))
    post[field] = value
    post_path.write_text(json.dumps(post), encoding="utf-8")

    with pytest.raises(archive_builder.ReleaseArchiveError, match=message):
        archive_builder.build_release_archive(package, tmp_path / f"bad-{field}.zip", source_epoch=1_700_000_000)


def test_build_release_archive_rejects_failed_staged_installer_claim(tmp_path):
    package = _package(tmp_path)
    staged_path = package / "staged-installer-verification.json"
    staged = json.loads(staged_path.read_text(encoding="utf-8"))
    staged["runner"]["selected"] = False
    staged_path.write_text(json.dumps(staged), encoding="utf-8")

    with pytest.raises(archive_builder.ReleaseArchiveError, match="runner was not selected"):
        archive_builder.build_release_archive(package, tmp_path / "bad-staged.zip", source_epoch=1_700_000_000)


def test_build_release_archive_rejects_missing_post_sign_probe_evidence(tmp_path):
    package = _package(tmp_path)
    post_path = package / "tools/release_cli_tools_post_sign_manifest.json"
    post = json.loads(post_path.read_text(encoding="utf-8"))
    post["tools"][0]["help_runs"][1]["residual_process_count"] = 1
    post_path.write_text(json.dumps(post), encoding="utf-8")

    with pytest.raises(archive_builder.ReleaseArchiveError, match="residual_process_count must equal 0"):
        archive_builder.build_release_archive(package, tmp_path / "bad-probe.zip", source_epoch=1_700_000_000)


def test_build_release_archive_rejects_deleted_probe_field_and_accepts_zero_byte_payload(tmp_path):
    package = _package(tmp_path)
    post_path = package / "tools/release_cli_tools_post_sign_manifest.json"
    post = json.loads(post_path.read_text(encoding="utf-8"))
    del post["tools"][0]["help_runs"][0]["residual_process_count"]
    post_path.write_text(json.dumps(post), encoding="utf-8")
    with pytest.raises(archive_builder.ReleaseArchiveError, match="residual_process_count must be an integer"):
        archive_builder.build_release_archive(package, tmp_path / "missing-field.zip", source_epoch=1_700_000_000)

    package = _package(tmp_path / "zero")
    zero_file = package / "tools/direct_sync_relay_install_pack/_internal/zero.marker"
    zero_file.write_bytes(b"")
    post_path = package / "tools/release_cli_tools_post_sign_manifest.json"
    post = json.loads(post_path.read_text(encoding="utf-8"))
    post["tools"][1]["payload_inventory"].append(
        {
            "path": "_internal/zero.marker",
            "size": 0,
            "sha256": archive_builder._sha256(zero_file),
        }
    )
    post_path.write_text(json.dumps(post), encoding="utf-8")
    _refresh_staged_inventory(package)
    report = archive_builder.build_release_archive(
        package,
        tmp_path / "zero.zip",
        source_epoch=1_700_000_000,
    )
    assert report["status"] == "PASS"


def test_build_release_archive_rejects_final_live_authenticode_mismatch(tmp_path, monkeypatch):
    package = _package(tmp_path)
    monkeypatch.setattr(
        archive_builder,
        "_read_authenticode_signature",
        lambda _path: {
            "status": "HashMismatch",
            "signer_thumbprint": APPROVED_SIGNER,
            "timestamp_thumbprint": TIMESTAMP_SIGNER,
        },
    )

    with pytest.raises(archive_builder.ReleaseArchiveError, match="final live Authenticode"):
        archive_builder.build_release_archive(package, tmp_path / "bad-live.zip", source_epoch=1_700_000_000)
