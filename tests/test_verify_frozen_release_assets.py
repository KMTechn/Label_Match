from __future__ import annotations

import hashlib
import importlib.util
import json
from pathlib import Path
import stat
import sys
import zipfile

import pytest

from tools import verify_frozen_release_assets as verifier


TAG = "v2.0.94"
COMMIT = "1" * 40
TREE = "2" * 40
TAG_OBJECT = "4" * 40
SOURCE_EPOCH = 1_700_000_000

BUILD_TEST_PATH = Path(__file__).with_name("test_build_release_archive.py")
BUILD_TEST_SPEC = importlib.util.spec_from_file_location(
    "_label_match_build_archive_test_fixture", BUILD_TEST_PATH
)
assert BUILD_TEST_SPEC and BUILD_TEST_SPEC.loader
build_fixture = importlib.util.module_from_spec(BUILD_TEST_SPEC)
sys.modules[BUILD_TEST_SPEC.name] = build_fixture
BUILD_TEST_SPEC.loader.exec_module(build_fixture)


def _qualification_receipt(path: Path, fixture: dict[str, object]) -> Path:
    archive = fixture["archive"]
    checksum = fixture["checksum"]
    assert isinstance(archive, Path)
    assert isinstance(checksum, Path)
    payload = {
        "schema_version": "label-match-pre-push-qualification-v2",
        "status": "PASS",
        "phase": "phase_b_pre_push_frozen_candidate",
        "release_title": f"Release {TAG}",
        "release_trust": "internal_unsigned",
        "tag": TAG,
        "tag_object": TAG_OBJECT,
        "tag_object_type": "tag",
        "tag_peeled_commit": COMMIT,
        "canonical_tag_message": f"Release {TAG}",
        "tag_identity_report": f"Label_Match-{TAG}.final-tag-identity.json",
        "tag_identity_report_sha256": "5" * 64,
        "tag_recorded_before_release_identity_and_build": True,
        "commit": COMMIT,
        "tree": TREE,
        "tag_signature_verified": False,
        "python_version": "3.12.10",
        "pyinstaller_version": "6.20.0",
        "native_free_low_risk": {
            "pygame_paths": [],
            "pillow_paths": [],
            "charset_normalizer_native_paths": [],
            "charset_normalizer_mode": "pure-python-source-override",
            "audio_backend": "stdlib-winsound",
        },
        "source_epoch": SOURCE_EPOCH,
        "path_identity": {"schema_version": "label-match-release-path-identity-v1"},
        "archive": archive.name,
        "archive_sha256": fixture["archive_sha256"],
        "archive_size": fixture["archive_size"],
        "checksum": checksum.name,
        "checksum_sha256": verifier._sha256_file(checksum),
        "main_exe_sha256": fixture["main_exe_sha256"],
        "factory_contract_sha256": verifier.CONTRACT_BUNDLE_SHA256,
        "update_provider": "github",
        "update_channel": "stable",
        "frozen_bytes": True,
        "network_used": False,
        "publication_mutated": False,
        "tag_mutated": False,
        "external_post_download_parity_required": True,
    }
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return path


def _fixture(tmp_path: Path) -> dict[str, object]:
    package = build_fixture._package(tmp_path)
    archive = tmp_path / f"Label_Match-{TAG}.zip"
    report = build_fixture.archive_builder.build_release_archive(
        package,
        archive,
        source_epoch=SOURCE_EPOCH,
        expected_tag=TAG,
    )
    checksum = archive.with_name(archive.name + ".sha256")
    checksum.write_bytes(
        f"{report['archive_sha256']}  {archive.name}\n".encode("ascii")
    )
    fixture: dict[str, object] = {
        "archive": archive,
        "checksum": checksum,
        "archive_sha256": report["archive_sha256"],
        "archive_size": report["archive_size"],
        "main_exe_sha256": report["main_exe_sha256"],
    }
    fixture["receipt"] = _qualification_receipt(
        tmp_path / "preserved-phase1-qualification.json", fixture
    )
    return fixture


def _verify(
    fixture: dict[str, object],
    *,
    use_receipt: bool = False,
) -> dict[str, object]:
    archive = fixture["archive"]
    checksum = fixture["checksum"]
    assert isinstance(archive, Path)
    assert isinstance(checksum, Path)
    receipt = fixture["receipt"] if use_receipt else None
    assert receipt is None or isinstance(receipt, Path)
    return verifier.verify_frozen_release_assets(
        archive,
        checksum,
        expected_tag=TAG,
        expected_commit=COMMIT,
        expected_tree=TREE,
        expected_tag_object=TAG_OBJECT,
        expected_source_epoch=SOURCE_EPOCH,
        expected_archive_sha256=(
            "" if use_receipt else str(fixture["archive_sha256"])
        ),
        expected_archive_size=(0 if use_receipt else int(fixture["archive_size"])),
        expected_main_exe_sha256=(
            "" if use_receipt else str(fixture["main_exe_sha256"])
        ),
        qualification_receipt_path=receipt,
    )


def _read_archive(archive: Path) -> dict[str, tuple[zipfile.ZipInfo, bytes]]:
    with zipfile.ZipFile(archive, "r") as source:
        return {
            info.filename.removeprefix("Label_Match/"): (info, source.read(info))
            for info in source.infolist()
        }


def _canonical_sha256(value: object) -> str:
    payload = json.dumps(
        value, ensure_ascii=True, sort_keys=True, separators=(",", ":")
    ).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


def _json_bytes(value: object) -> bytes:
    return (json.dumps(value, indent=2, sort_keys=True) + "\n").encode("utf-8")


def _refresh_staged_inventory(
    entries: dict[str, tuple[zipfile.ZipInfo, bytes]],
) -> None:
    name = "staged-installer-verification.json"
    info, payload = entries[name]
    staged = json.loads(payload)
    payload_inventory = [
        {
            "path": relative,
            "size": len(member_payload),
            "sha256": hashlib.sha256(member_payload).hexdigest(),
        }
        for relative, (_member_info, member_payload) in sorted(entries.items())
        if relative
        not in {name, "build-manifest.json", "bootstrap-integrity.json"}
    ]
    predecessor_manifest = _json_bytes(
        {
            "build_manifest_schema_version": 1,
            "payload_inventory": payload_inventory,
            "payload_inventory_sha256": _canonical_sha256(payload_inventory),
        }
    )
    inventory = sorted(
        payload_inventory,
        key=lambda item: (str(item["path"]).casefold(), str(item["path"])),
    )
    staged["original_package_file_count"] = len(inventory)
    staged["original_package_inventory"] = inventory
    staged["original_package_inventory_sha256"] = _canonical_sha256(inventory)
    staged["manifest_contract"]["sha256"] = hashlib.sha256(
        predecessor_manifest
    ).hexdigest()
    staged["manifest_contract"]["payload_file_count"] = len(payload_inventory)
    staged["manifest_contract"]["payload_inventory_sha256"] = _canonical_sha256(
        payload_inventory
    )
    entries[name] = (info, _json_bytes(staged))


def _refresh_build_manifest(
    entries: dict[str, tuple[zipfile.ZipInfo, bytes]],
) -> None:
    name = "build-manifest.json"
    info, payload = entries[name]
    manifest = json.loads(payload)
    inventory = [
        {
            "path": relative,
            "size": len(member_payload),
            "sha256": hashlib.sha256(member_payload).hexdigest(),
        }
        for relative, (_member_info, member_payload) in sorted(entries.items())
        if relative not in {name, "bootstrap-integrity.json"}
    ]
    manifest["payload_inventory"] = inventory
    manifest["payload_inventory_sha256"] = _canonical_sha256(inventory)
    entries[name] = (info, _json_bytes(manifest))


def _rewrite_archive(
    fixture: dict[str, object],
    entries: dict[str, tuple[zipfile.ZipInfo, bytes]],
) -> None:
    archive = fixture["archive"]
    checksum = fixture["checksum"]
    assert isinstance(archive, Path)
    assert isinstance(checksum, Path)
    bootstrap_name = "bootstrap-integrity.json"
    if bootstrap_name in entries:
        bootstrap_info, _old_payload = entries[bootstrap_name]
        inventory = [
            {
                "path": relative,
                "size": len(member_payload),
                "sha256": hashlib.sha256(member_payload).hexdigest(),
            }
            for relative, (_member_info, member_payload) in sorted(entries.items())
            if relative != bootstrap_name
        ]
        _record, bootstrap_payload = (
            build_fixture.archive_builder._bootstrap_integrity_payload(
                inventory,
                source_epoch=SOURCE_EPOCH,
            )
        )
        entries[bootstrap_name] = (bootstrap_info, bootstrap_payload)
    archive.unlink()
    with zipfile.ZipFile(archive, "x") as output:
        for _relative, (info, payload) in entries.items():
            output.writestr(info, payload)
    archive_sha = verifier._sha256_file(archive)
    checksum.write_bytes(f"{archive_sha}  {archive.name}\n".encode("ascii"))
    fixture["archive_sha256"] = archive_sha
    fixture["archive_size"] = archive.stat().st_size


def test_verifier_accepts_exact_frozen_archive_and_shared_evidence_contract(tmp_path):
    fixture = _fixture(tmp_path)

    result = _verify(fixture)

    assert result["status"] == "PASS"
    assert result["exact_membership"] is True
    assert result["manifest_byte_parity"] is True
    assert result["embedded_identities_verified"] is True
    assert result["staged_installer_verified"] is True
    assert result["factory_manifest_verified"] is True
    assert result["retired_helper_executables_absent"] is True
    assert result["qualification_receipt_status"] == "NOT_TESTED_EXTERNAL_REQUIRED"


def test_verifier_rejects_archive_without_bootstrap_integrity_record(tmp_path):
    fixture = _fixture(tmp_path)
    archive = fixture["archive"]
    assert isinstance(archive, Path)
    entries = _read_archive(archive)
    entries.pop("bootstrap-integrity.json")
    _rewrite_archive(fixture, entries)

    with pytest.raises(
        verifier.FrozenReleaseError,
        match="bootstrap integrity record is missing or unverified",
    ):
        _verify(fixture)


def test_verifier_compares_downloaded_bytes_to_preserved_phase1_receipt(tmp_path):
    fixture = _fixture(tmp_path)

    result = _verify(fixture, use_receipt=True)

    assert result["status"] == "PASS"
    assert result["qualification_receipt_status"] == "PASS"
    assert result["qualification_receipt_sha256"] == verifier._sha256_file(
        fixture["receipt"]
    )


def test_verifier_rejects_checksum_mismatch(tmp_path):
    fixture = _fixture(tmp_path)
    checksum = fixture["checksum"]
    assert isinstance(checksum, Path)
    archive = fixture["archive"]
    assert isinstance(archive, Path)
    checksum.write_bytes(f"{'0' * 64}  {archive.name}\n".encode("ascii"))

    with pytest.raises(verifier.FrozenReleaseError, match="checksum does not match"):
        _verify(fixture)


def test_verifier_rejects_extra_archive_member(tmp_path):
    fixture = _fixture(tmp_path)
    archive = fixture["archive"]
    assert isinstance(archive, Path)
    entries = _read_archive(archive)
    template = next(iter(entries.values()))[0]
    rogue = zipfile.ZipInfo("Label_Match/rogue.txt", date_time=template.date_time)
    rogue.compress_type = zipfile.ZIP_DEFLATED
    rogue.create_system = 3
    rogue.external_attr = (stat.S_IFREG | 0o644) << 16
    entries["rogue.txt"] = (rogue, b"rogue")
    _rewrite_archive(fixture, entries)

    with pytest.raises(verifier.FrozenReleaseError, match="manifest"):
        _verify(fixture)


def test_verifier_rejects_unix_symbolic_link_member(tmp_path):
    fixture = _fixture(tmp_path)
    archive = fixture["archive"]
    assert isinstance(archive, Path)
    with zipfile.ZipFile(archive, "a") as output:
        link = zipfile.ZipInfo("Label_Match/unsafe-link")
        link.create_system = 3
        link.external_attr = (stat.S_IFLNK | 0o777) << 16
        output.writestr(link, "Label_Match.exe")
    fixture["archive_sha256"] = verifier._sha256_file(archive)
    fixture["archive_size"] = archive.stat().st_size
    checksum = fixture["checksum"]
    assert isinstance(checksum, Path)
    checksum.write_bytes(
        f"{fixture['archive_sha256']}  {archive.name}\n".encode("ascii")
    )

    with pytest.raises(verifier.FrozenReleaseError, match="symbolic-link"):
        _verify(fixture)


def test_abbreviated_staged_installer_report_cannot_pass(tmp_path):
    fixture = _fixture(tmp_path)
    archive = fixture["archive"]
    assert isinstance(archive, Path)
    entries = _read_archive(archive)
    name = "staged-installer-verification.json"
    info, payload = entries[name]
    staged = json.loads(payload)
    entries[name] = (
        info,
        _json_bytes(
            {
                "schema_version": staged["schema_version"],
                "status": "PASS",
                "original_package_unchanged": True,
            }
        ),
    )
    _refresh_build_manifest(entries)
    _rewrite_archive(fixture, entries)

    with pytest.raises(verifier.FrozenReleaseError, match="staged installer evidence fields"):
        _verify(fixture)


def test_forged_source_host_override_requirement_cannot_pass_after_reseal(tmp_path):
    fixture = _fixture(tmp_path)
    archive = fixture["archive"]
    assert isinstance(archive, Path)
    entries = _read_archive(archive)
    name = "staged-installer-verification.json"
    info, payload = entries[name]
    staged = json.loads(payload)
    staged["state_contract"]["source_host_override_required"] = True
    entries[name] = (info, _json_bytes(staged))
    _refresh_build_manifest(entries)
    _rewrite_archive(fixture, entries)

    with pytest.raises(verifier.FrozenReleaseError, match="state contract"):
        _verify(fixture)


def test_abbreviated_product_host_binding_evidence_cannot_pass(tmp_path):
    fixture = _fixture(tmp_path)
    archive = fixture["archive"]
    assert isinstance(archive, Path)
    entries = _read_archive(archive)
    name = "staged-installer-verification.json"
    info, payload = entries[name]
    staged = json.loads(payload)
    staged["runtime_host"].pop("relay_execution_boundary")
    entries[name] = (info, _json_bytes(staged))
    _refresh_build_manifest(entries)
    _rewrite_archive(fixture, entries)

    with pytest.raises(verifier.FrozenReleaseError, match="product-host binding"):
        _verify(fixture)


def test_embedded_host_without_runtime_payload_cannot_pass(tmp_path):
    fixture = _fixture(tmp_path)
    archive = fixture["archive"]
    assert isinstance(archive, Path)
    entries = _read_archive(archive)
    runtime = "_internal/python312.dll"
    entries.pop(runtime)
    _refresh_staged_inventory(entries)
    _refresh_build_manifest(entries)
    _rewrite_archive(fixture, entries)

    with pytest.raises(verifier.FrozenReleaseError, match="required release package members"):
        _verify(fixture)


def test_probe_identity_with_abbreviated_fields_cannot_pass(tmp_path):
    fixture = _fixture(tmp_path)
    archive = fixture["archive"]
    assert isinstance(archive, Path)
    entries = _read_archive(archive)
    name = "KMTechActiveWorkProbe.independent.build-identity.json"
    info, payload = entries[name]
    identity = json.loads(payload)
    identity.pop("probe_version")
    entries[name] = (info, _json_bytes(identity))
    _refresh_staged_inventory(entries)
    _refresh_build_manifest(entries)
    _rewrite_archive(fixture, entries)

    with pytest.raises(verifier.FrozenReleaseError, match="probe identity fields differ"):
        _verify(fixture)


def test_archive_cannot_reintroduce_separate_scheduled_runner(tmp_path):
    fixture = _fixture(tmp_path)
    archive = fixture["archive"]
    assert isinstance(archive, Path)
    entries = _read_archive(archive)
    source_info, _source_payload = entries["tools/direct_sync_relay_runner.py"]
    retired_info = zipfile.ZipInfo(
        "Label_Match/tools/direct_sync_relay_runner/direct_sync_relay_runner.exe",
        date_time=source_info.date_time,
    )
    retired_info.compress_type = source_info.compress_type
    retired_info.external_attr = source_info.external_attr
    retired_info.create_system = source_info.create_system
    entries["tools/direct_sync_relay_runner/direct_sync_relay_runner.exe"] = (
        retired_info,
        b"retired scheduled runner",
    )
    _refresh_staged_inventory(entries)
    _refresh_build_manifest(entries)
    _rewrite_archive(fixture, entries)

    with pytest.raises(verifier.FrozenReleaseError, match="retired helper"):
        _verify(fixture)
