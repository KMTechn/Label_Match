from __future__ import annotations

import importlib.util
import hashlib
import json
from pathlib import Path
import re
import sys
import zipfile

import pytest


MODULE_PATH = Path(__file__).resolve().parents[1] / "tools" / "build_release_archive.py"
SPEC = importlib.util.spec_from_file_location("build_release_archive_for_tests", MODULE_PATH)
assert SPEC and SPEC.loader
archive_builder = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = archive_builder
SPEC.loader.exec_module(archive_builder)

IDENTITY_MODULE_PATH = MODULE_PATH.with_name("verify_release_identity.py")
IDENTITY_SPEC = importlib.util.spec_from_file_location(
    "verify_release_identity_for_archive_integration_tests", IDENTITY_MODULE_PATH
)
assert IDENTITY_SPEC and IDENTITY_SPEC.loader
identity_verifier = importlib.util.module_from_spec(IDENTITY_SPEC)
IDENTITY_SPEC.loader.exec_module(identity_verifier)

BUILDER_PATH = MODULE_PATH.with_name("build_frozen_release_candidate.ps1")

TAG = "v2.0.92"
COMMIT = "1" * 40
TREE = "2" * 40


def _builder_reviewed_ref() -> str:
    source = BUILDER_PATH.read_text(encoding="utf-8")
    invocation = source.split("tools\\verify_release_identity.py", maxsplit=1)[1]
    invocation = invocation.split(
        'Assert-LastExitCode "Verify unsigned release identity"', maxsplit=1
    )[0]
    matches = re.findall(r'--reviewed-ref\s+"([^"]+)"', invocation)
    assert matches == ["refs/remotes/origin/main"]
    return matches[0]


def _generate_builder_release_identity(tmp_path: Path, monkeypatch) -> dict[str, object]:
    (tmp_path / "Label_Match.py").write_text(
        f'APP_VERSION = "{TAG}"\n', encoding="utf-8"
    )
    reviewed_ref = _builder_reviewed_ref()
    replies = {
        ("rev-parse", "HEAD"): COMMIT,
        ("cat-file", "-t", f"refs/tags/{TAG}"): "tag",
        ("rev-parse", f"refs/tags/{TAG}^{{commit}}"): COMMIT,
        ("rev-parse", "--verify", reviewed_ref): COMMIT,
        ("status", "--porcelain=v1", "--untracked-files=all"): "",
        ("rev-parse", "HEAD^{tree}"): TREE,
    }
    monkeypatch.setattr(
        identity_verifier, "_git", lambda _root, *args: replies[args]
    )
    return identity_verifier.verify_release_identity(
        tmp_path,
        expected_tag=TAG,
        expected_sha=COMMIT,
        reviewed_ref=reviewed_ref,
    )


def _write(path: Path, payload: bytes | str) -> dict[str, object]:
    path.parent.mkdir(parents=True, exist_ok=True)
    if isinstance(payload, str):
        path.write_text(payload, encoding="utf-8", newline="\n")
    else:
        path.write_bytes(payload)
    return {"size": path.stat().st_size, "sha256": archive_builder._sha256(path)}


def _write_json(path: Path, payload: dict[str, object]) -> None:
    _write(path, json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n")


def _tool_report(root: Path, name: str, source: str, mode: str) -> dict[str, object]:
    tools = root / "tools"
    if mode == "onefile":
        executable = tools / f"{name}.exe"
        _write(executable, f"{name} executable".encode())
        payload_root = tools
        payload_paths = [executable]
    else:
        payload_root = tools / name
        executable = payload_root / f"{name}.exe"
        runtime = payload_root / "_internal" / "python312.dll"
        _write(executable, f"{name} executable".encode())
        _write(runtime, b"python runtime")
        payload_paths = [runtime, executable]
    inventory = [
        {
            "path": path.relative_to(payload_root).as_posix(),
            "size": path.stat().st_size,
            "sha256": archive_builder._sha256(path),
        }
        for path in sorted(
            payload_paths,
            key=lambda path: path.relative_to(payload_root).as_posix().casefold(),
        )
    ]
    executable_sha = archive_builder._sha256(executable)
    return {
        "name": name,
        "source": source,
        "mode": mode,
        "executable_sha256": executable_sha,
        "executable_size": executable.stat().st_size,
        "payload_inventory": inventory,
        "help_runs": [
            {
                "run": run,
                "status": "PASS",
                "elapsed_ms": 100,
                "returncode": 0,
                "stdout_bytes": 64,
                "stderr_bytes": 0,
                "probe_executable_sha256": executable_sha,
                "residual_process_count": 0,
            }
            for run in range(1, 4)
        ],
        "archive_verification": {
            "status": "PASS",
            "viewer_stdout_sha256": "3" * 64,
            "viewer_stderr_bytes": 0,
        },
    }


def _package(tmp_path: Path) -> Path:
    root = tmp_path / "dist" / "Label_Match"
    tools = root / "tools"
    required_payloads = {
        "CENTRAL_LOGISTICS_PC_ROLLOUT.md": b"rollout",
        "INSTALL_THIS_PC.ps1": b"# installer wrapper\n",
        "Label_Match.exe": b"main executable",
        "Label_Match_Protected_Admin_Install.exe": b"protected admin",
        "PROTECTED_ADMIN_PROVISIONING.md": b"protected admin docs",
        "PROVISION_PROTECTED_ADMIN_ACL.ps1": b"# acl wrapper\n",
        "KMTech_Logistics_Profile_Check.exe": b"profile check",
        "KMTech_Logistics_Profile_Install.exe": b"profile install",
        "config/app_settings.json": b'{"update_settings":{"provider":"github","channel":"stable"}}\n',
        "_internal/config/app_settings.json": b'{"update_settings":{"provider":"github","channel":"stable"}}\n',
        "_internal/python312.dll": b"embedded runtime",
        "_internal/base_library.zip": b"embedded library",
        "direct_sync_operator.py": b"# operator\n",
        "direct_sync_push.py": b"# push\n",
        "direct_sync_runtime.py": b"# runtime\n",
        "logistics_runtime_profile.py": b"# profile\n",
        "producer_runtime_client.py": b"# producer\n",
        "tools/check_logistics_runtime_profile.py": b"# check profile\n",
        "tools/install_logistics_runtime_profile.py": b"# install profile\n",
        "tools/direct_sync_relay_runner.py": b"# runner source\n",
        "tools/register_label_match_worker_pc.py": b"# registration source\n",
        "tools/direct_sync_relay_operator.py": b"# relay operator\n",
    }
    for relative, payload in required_payloads.items():
        _write(root / relative, payload)

    release_identity = {
        "schema_version": archive_builder.RELEASE_IDENTITY_SCHEMA,
        "status": "PASS",
        "tag": TAG,
        "app_version": TAG,
        "commit": COMMIT,
        "tree": TREE,
        "clean_checkout": True,
        "release_trust": "internal_unsigned",
        "tag_object_type": "tag",
        "annotated_tag": True,
        "tag_signature_verified": False,
        "reviewed_ref": "refs/remotes/origin/main",
        "reviewed_ref_commit": COMMIT,
        "reviewed_main_ancestor": True,
        "reviewed_ref_exact": True,
    }
    _write_json(root / "release-identity.json", release_identity)

    contract_lock = archive_builder.EXPECTED_CONTRACT_LOCK
    _write_json(root / "contract.lock.json", contract_lock)
    dependency = {"kind": "none", "commit": None, "sha256": None}
    build_identity = {
        "build_identity_schema_version": 1,
        "app_id": "label_match",
        "app_version": TAG,
        "source_commit": COMMIT,
        "source_tree": TREE,
        "dirty": False,
        "contract_bundle_version": "1.0.3",
        "contract_bundle_corrective_revision": 1,
        "python_version": archive_builder.PYTHON_VERSION,
        "pyinstaller_version": archive_builder.PYINSTALLER_VERSION,
        "contract_bundle_sha256": archive_builder.CONTRACT_BUNDLE_SHA256,
        "db_schema": {"current": 0, "minimum": 0, "maximum": 0},
        "server_api_contract_version": "logistics-v1",
        "event_contract_version": "common-event-envelope-v1",
        "manifest_contract_version": "producer-onboarding-manifest-v1",
        "dependency_lock_sha256": archive_builder._canonical_sha256(contract_lock),
        "dependency": dependency,
        "builder": {
            "name": "kmtech_factory_contracts.build_cli",
            "version": "1.0.3",
        },
        "build_compatibility_sha256": "0" * 64,
    }
    binding_identity = dict(build_identity)
    binding_identity.pop("build_compatibility_sha256")
    compatibility = {
        "matrix_schema_version": 1,
        "app_id": "label_match",
        "app_version": TAG,
        "source_commit": COMMIT,
        "source_tree": TREE,
        "build_identity_sha256": archive_builder._canonical_sha256(binding_identity),
        "contract_bundle_sha256": archive_builder.CONTRACT_BUNDLE_SHA256,
        "contract_bundle_corrective_revision": 1,
        "dependency": dependency,
        "minimum_installer_version": "1.0.3.4",
        "resources": archive_builder.EXPECTED_FACTORY_RESOURCES,
    }
    build_identity["build_compatibility_sha256"] = archive_builder._canonical_sha256(
        compatibility
    )
    _write_json(root / "build-identity.json", build_identity)
    _write_json(root / "build-compatibility.json", compatibility)

    probe = root / "KMTechActiveWorkProbe.exe"
    _write(probe, b"active work probe")
    probe_sha = archive_builder._sha256(probe)
    for filename, mode, apps in (
        (
            "KMTechActiveWorkProbe.independent.build-identity.json",
            "independent",
            ["Label_Match"],
        ),
        (
            "KMTechActiveWorkProbe.integrated.build-identity.json",
            "integrated",
            archive_builder.ALL_PROBE_APPS,
        ),
    ):
        _write_json(
            root / filename,
            {
                "schema_version": archive_builder.PROBE_SCHEMA,
                "probe_name": archive_builder.PROBE_NAME,
                "probe_version": archive_builder.PROBE_VERSION,
                "probe_artifact_sha256": probe_sha,
                "probe_source_commit": COMMIT,
                "workflow_mode": mode,
                "supported_apps": apps,
            },
        )

    predecessor_inventory = archive_builder._inventory(
        root, excluded={"build-manifest.json"}, casefold=False
    )
    _write_json(
        root / "build-manifest.json",
        {
            "build_manifest_schema_version": 1,
            "payload_inventory": predecessor_inventory,
            "payload_inventory_sha256": archive_builder._canonical_sha256(
                predecessor_inventory
            ),
        },
    )
    staged_inventory = archive_builder._inventory(
        root, excluded={"build-manifest.json"}
    )
    public_entrypoint = root / "INSTALL_THIS_PC.ps1"
    staged_report = {
        "schema_version": archive_builder.STAGED_INSTALLER_SCHEMA,
        "status": "PASS",
        "proof_classification": "STATIC_ISOLATED_DRY_RUN",
        "dynamic_qualification": "NOT_TESTED",
        "public_entrypoint": {
            "path": "INSTALL_THIS_PC.ps1",
            "sha256": archive_builder._sha256(public_entrypoint),
        },
        "runtime_host": {
            "path": "Label_Match.exe",
            "sha256": archive_builder._sha256(root / "Label_Match.exe"),
            "package_layout": "onedir",
            "relay_execution_boundary": "product_host",
            "current_user_relay_mode": "--label-match-user-relay",
            "direct_sync_relay_mode": "--label-match-direct-sync-relay",
        },
        "bootstrap_contract": {
            "canonical_code_root": r"C:\KMTech\Apps\Label_Match\current",
            "elevation_points": ["code_placement"],
            "identity_profile_created": False,
            "state_scope": "current_user_first_run",
            "exact_inventory_readback": True,
            "onedir_required": True,
        },
        "state_contract": {
            "identity_scope": "current_user_per_pc",
            "profile_scope": "current_user",
            "credential_scope": "current_user_dpapi",
            "ledger_scope": "current_user",
            "operation_lease_store": "AUTHORITATIVE_SNAPSHOT_PRESERVED",
            "relay_persistence": "HKCU_RUN",
            "relay_port_contract": 18456,
            "source_host_override_required": False,
        },
        "legacy_authority_contract": {
            "system_scheduled_task_supported": False,
            "task_creation_tokens_absent": True,
            "legacy_owned_task_cleanup_only": True,
            "forbidden_package_members_absent": True,
            "forbidden_members": sorted(archive_builder.RETIRED_HELPER_EXECUTABLES),
        },
        "manifest_contract": {
            "path": "build-manifest.json",
            "sha256": archive_builder._sha256(root / "build-manifest.json"),
            "payload_file_count": len(predecessor_inventory),
            "payload_inventory_sha256": archive_builder._canonical_sha256(
                predecessor_inventory
            ),
            "preseal_isolated_manifest": True,
        },
        "system_python_required": False,
        "original_package_file_count": len(staged_inventory),
        "original_package_inventory": staged_inventory,
        "original_package_inventory_sha256": archive_builder._inventory_digest(
            staged_inventory
        ),
        "original_package_unchanged": True,
        "output_bound_bytes": 64 * 1024,
        "timeout_seconds": 120,
        "stdout_bytes": 100,
        "stderr_bytes": 0,
    }
    _write_json(root / "staged-installer-verification.json", staged_report)

    factory_inventory = archive_builder._inventory(
        root, excluded={"build-manifest.json"}, casefold=False
    )
    manifest = {
        "build_manifest_schema_version": 1,
        "app_id": "label_match",
        "app_version": TAG,
        "identity_path": "build-identity.json",
        "identity_sha256": archive_builder._canonical_sha256(build_identity),
        "build_compatibility_path": "build-compatibility.json",
        "contract_bundle_corrective_revision": 1,
        "contract_bundle_sha256": archive_builder.CONTRACT_BUNDLE_SHA256,
        "dependency": dependency,
        "build_compatibility_sha256": archive_builder._canonical_sha256(compatibility),
        "payload_inventory": factory_inventory,
        "payload_inventory_sha256": archive_builder._canonical_sha256(factory_inventory),
        "expected_files": sorted(archive_builder.FACTORY_EXPECTED_FILES),
        "built_at_utc": "2023-11-14T22:13:20Z",
    }
    _write_json(root / "build-manifest.json", manifest)
    return root


def test_archive_accepts_identity_generated_with_exact_builder_reviewed_ref(
    tmp_path, monkeypatch
):
    generated = _generate_builder_release_identity(tmp_path, monkeypatch)
    _write_json(tmp_path / "release-identity.json", generated)

    assert archive_builder._validate_release_identity(tmp_path, expected_tag=TAG) == {
        "tag": TAG,
        "commit": COMMIT,
        "tree": TREE,
    }


def test_archive_rejects_wrong_ref_in_builder_generated_identity(tmp_path, monkeypatch):
    generated = _generate_builder_release_identity(tmp_path, monkeypatch)
    generated["reviewed_ref"] = "refs/heads/main"
    _write_json(tmp_path / "release-identity.json", generated)

    with pytest.raises(archive_builder.ReleaseArchiveError, match="internal_unsigned v3"):
        archive_builder._validate_release_identity(tmp_path, expected_tag=TAG)


def test_build_release_archive_is_deterministic_unsigned_and_byte_exact(tmp_path):
    package = _package(tmp_path)
    first = tmp_path / "one.zip"
    second = tmp_path / "two.zip"

    first_report = archive_builder.build_release_archive(
        package, first, source_epoch=1_700_000_000, expected_tag=TAG
    )
    second_report = archive_builder.build_release_archive(
        package, second, source_epoch=1_700_000_000, expected_tag=TAG
    )

    assert first_report["status"] == "PASS"
    assert first_report["release_trust"] == "internal_unsigned"
    assert first_report["release_identity_schema"] == "label-match-release-identity-v3"
    assert first_report["annotated_tag"] is True
    assert first_report["tag_signature_verified"] is False
    assert first_report["authenticode_required"] is False
    assert first_report["python_version"] == "3.12.10"
    assert first_report["pyinstaller_version"] == "6.20.0"
    assert first_report["retired_helper_executables_absent"] is True
    assert first_report["staged_installer_verified"] is True
    assert first_report["code_only_bootstrap_verified"] is True
    assert first_report["onedir_product_host_verified"] is True
    assert first_report["current_user_state_contract_verified"] is True
    assert first_report["hkcu_relay_contract_verified"] is True
    assert first_report["system_task_authority_absent"] is True
    assert first_report["authoritative_snapshot_lease_preserved"] is True
    assert first_report["dynamic_install_qualification"] == "NOT_TESTED"
    assert first_report["factory_manifest_verified"] is True
    assert first_report["archive_sha256"] == second_report["archive_sha256"]
    assert first.read_bytes() == second.read_bytes()
    with zipfile.ZipFile(first) as archive:
        assert archive.testzip() is None
        names = set(archive.namelist())
        bootstrap_bytes = archive.read("Label_Match/bootstrap-integrity.json")
        bootstrap = json.loads(bootstrap_bytes.decode("utf-8"))
    assert "Label_Match/build-manifest.json" in names
    assert "Label_Match/bootstrap-integrity.json" in names
    assert first_report["bootstrap_integrity_verified"] is True
    assert "Label_Match/tools/direct_sync_relay_runner.py" in names
    assert "Label_Match/install_label_match_direct_sync.ps1" not in names
    assert "Label_Match/tools/direct_sync_relay_install_pack.py" not in names
    assert "Label_Match/tools/direct_sync_relay_runner/direct_sync_relay_runner.exe" not in names
    assert not any(name.endswith("register_label_match_worker_pc.exe") for name in names)
    assert len(names) == first_report["package_file_count"]
    assert not (package / "bootstrap-integrity.json").exists()
    assert bootstrap["schema_version"] == "label-match-bootstrap-integrity-v2"
    assert bootstrap["status"] == "PASS"
    assert bootstrap["code_root"] == "."
    assert bootstrap["inventory_algorithm"] == "sha256-file-hash-size-utf8-path-v1"
    assert bootstrap["package_layout"] == "onedir"
    assert "files" not in bootstrap
    inventory = archive_builder._inventory(package)
    assert bootstrap["file_count"] == len(inventory)
    assert bootstrap["root_sha256"] == archive_builder._bootstrap_root_sha256(
        inventory
    )
    assert hashlib.sha256(bootstrap_bytes).hexdigest() == first_report[
        "bootstrap_integrity_sha256"
    ]


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("release_trust", "public_signed"),
        ("tag_signature_verified", True),
        ("annotated_tag", False),
        ("schema_version", "label-match-release-identity-v2"),
    ],
)
def test_archive_rejects_noncanonical_unsigned_v3_identity(tmp_path, field, value):
    package = _package(tmp_path)
    path = package / "release-identity.json"
    payload = json.loads(path.read_text(encoding="utf-8"))
    payload[field] = value
    _write_json(path, payload)

    with pytest.raises(archive_builder.ReleaseArchiveError, match="internal_unsigned v3"):
        archive_builder.build_release_archive(
            package, tmp_path / "bad.zip", source_epoch=1_700_000_000, expected_tag=TAG
        )


def test_archive_contract_retires_active_system_task_authority():
    assert archive_builder.RETIRED_HELPER_EXECUTABLES == {
        "install_label_match_direct_sync.ps1",
        "tools/direct_sync_relay_install_pack.py",
        "tools/direct_sync_relay_install_pack/direct_sync_relay_install_pack.exe",
        "tools/direct_sync_relay_install_pack.exe",
        "tools/direct_sync_relay_runner/direct_sync_relay_runner.exe",
        "tools/register_label_match_worker_pc.exe",
        "tools/invoke_embedded_python.ps1",
    }
    assert "tools/direct_sync_relay_runner.py" in archive_builder.REQUIRED_PACKAGE_MEMBERS
    assert not (
        archive_builder.RETIRED_HELPER_EXECUTABLES
        & archive_builder.REQUIRED_PACKAGE_MEMBERS
    )


def test_archive_rejects_staged_installer_probe_or_factory_current_drift(tmp_path):
    package = _package(tmp_path / "staged")
    staged_path = package / "staged-installer-verification.json"
    staged = json.loads(staged_path.read_text(encoding="utf-8"))
    staged["runtime_host"]["relay_execution_boundary"] = "scheduled_task"
    _write_json(staged_path, staged)
    with pytest.raises(archive_builder.ReleaseArchiveError, match="product-host binding"):
        archive_builder.build_release_archive(
            package, tmp_path / "staged.zip", source_epoch=1_700_000_000
        )

    package = _package(tmp_path / "probe")
    probe_path = package / "KMTechActiveWorkProbe.integrated.build-identity.json"
    probe = json.loads(probe_path.read_text(encoding="utf-8"))
    probe["supported_apps"] = ["Label_Match"]
    _write_json(probe_path, probe)
    with pytest.raises(archive_builder.ReleaseArchiveError, match="probe identity mismatch"):
        archive_builder.build_release_archive(
            package, tmp_path / "probe.zip", source_epoch=1_700_000_000
        )

    package = _package(tmp_path / "current")
    _write(package / "rogue.txt", b"rogue")
    with pytest.raises(archive_builder.ReleaseArchiveError, match="current package differs"):
        archive_builder.build_release_archive(
            package, tmp_path / "current.zip", source_epoch=1_700_000_000
        )


@pytest.mark.parametrize(
    ("path", "value", "message"),
    [
        (("schema_version",), "label-match-staged-installer-verification-v2", "staged installer"),
        (("bootstrap_contract", "onedir_required"), False, "bootstrap contract"),
        (("state_contract", "source_host_override_required"), True, "state contract"),
        (("legacy_authority_contract", "system_scheduled_task_supported"), True, "task-authority contract"),
    ],
)
def test_archive_rejects_abbreviated_current_topology_contract(
    tmp_path, path, value, message
):
    package = _package(tmp_path)
    staged_path = package / "staged-installer-verification.json"
    staged = json.loads(staged_path.read_text(encoding="utf-8"))
    target = staged
    for key in path[:-1]:
        target = target[key]
    target[path[-1]] = value
    _write_json(staged_path, staged)

    with pytest.raises(archive_builder.ReleaseArchiveError, match=message):
        archive_builder.build_release_archive(
            package, tmp_path / "invalid-staged.zip", source_epoch=1_700_000_000
        )


@pytest.mark.parametrize("path", ["C:/escape", "file:stream", "../escape", "a\\b"])
def test_archive_rejects_windows_unsafe_relative_paths(path):
    with pytest.raises(archive_builder.ReleaseArchiveError, match="canonical POSIX"):
        archive_builder._safe_relative(path, label="fixture")


def test_build_release_archive_never_overwrites_existing_archive(tmp_path):
    package = _package(tmp_path)
    archive = tmp_path / "existing.zip"
    archive.write_bytes(b"keep")

    with pytest.raises(archive_builder.ReleaseArchiveError, match="already exists"):
        archive_builder.build_release_archive(
            package, archive, source_epoch=1_700_000_000, expected_tag=TAG
        )
    assert archive.read_bytes() == b"keep"


def test_cli_exclusive_creates_report_after_archive(tmp_path):
    package = _package(tmp_path)
    archive = tmp_path / f"Label_Match-{TAG}.zip"
    report = tmp_path / "report.json"
    assert (
        archive_builder.main(
            [
                "--package-root",
                str(package),
                "--archive",
                str(archive),
                "--source-epoch",
                "1700000000",
                "--expected-tag",
                TAG,
                "--report",
                str(report),
            ]
        )
        == 0
    )
    payload = json.loads(report.read_text(encoding="utf-8"))
    assert payload["archive_sha256"] == archive_builder._sha256(archive)
    assert payload["main_exe_sha256"] == archive_builder._sha256(
        package / "Label_Match.exe"
    )
