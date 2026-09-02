from __future__ import annotations

import hashlib
from io import BytesIO
import json
import os
from pathlib import Path
import subprocess
import sys

import pytest
from PIL import Image

from tools import capture_label_operator_ui as capture
from tools import publish_outline_user_manual as publisher


APP_COMMIT = "a" * 40
APP_TREE = "b" * 40
TOOL_COMMIT = "d" * 40
TOOL_BLOB_SHA256 = "e" * 64
GENERATED_AT = "2026-09-03T01:02:03Z"
BUNDLE_ID = "Label_Match__aaaaaaaaaaaa__20260903T010203Z__1234abcd"
ARTIFACT_FILE = "artifacts/Label_Match.zip"
ARTIFACT_BYTES = b"synthetic portable Label_Match artifact\n"
ARTIFACT_SHA256 = hashlib.sha256(ARTIFACT_BYTES).hexdigest()
VALIDATOR = Path(
    "E:/KMTech/production-readiness-20260830/HANDOVER/tools/"
    "validate_capture_bundle_v1.py"
)


def _png_bytes(size: tuple[int, int] = (2, 2)) -> bytes:
    stream = BytesIO()
    Image.new("RGB", size, "white").save(stream, format="PNG")
    return stream.getvalue()


def _capture_inputs(
    payload: bytes, *, bundle_id: str = BUNDLE_ID
) -> list[dict[str, object]]:
    return [
        {
            "state_id": state_id,
            "image_file": (
                f"Label_Match/{bundle_id}/captures/"
                f"{state_id}__2x2__96dpi.png"
            ),
            "image_bytes": payload,
            "viewport": {"width_px": 2, "height_px": 2},
            "dpi": 96,
            "generated_at": GENERATED_AT,
        }
        for state_id in capture.M7_REQUIRED_STATE_IDS
    ]


def _materialize(
    tmp_path: Path,
    *,
    app_specific: dict[str, object] | None = None,
    captures: list[dict[str, object]] | None = None,
    forbidden_name: str | None = None,
    approver: str = capture.M7_APPROVAL_PLACEHOLDER,
    custodian: str = capture.M7_APPROVAL_PLACEHOLDER,
    custody_location: str = capture.M7_APPROVAL_PLACEHOLDER,
    retention_period: str = capture.M7_APPROVAL_PLACEHOLDER,
) -> dict[str, object]:
    evidence_root = tmp_path / "evidence"
    artifact = evidence_root / ARTIFACT_FILE
    artifact.parent.mkdir(parents=True)
    artifact.write_bytes(ARTIFACT_BYTES)
    if forbidden_name is not None:
        (evidence_root / forbidden_name).write_text("mutable alias\n", encoding="utf-8")
    return capture.materialize_m7_external_capture_bundle(
        evidence_root=evidence_root,
        bundle_id=BUNDLE_ID,
        app_commit=APP_COMMIT,
        app_tree=APP_TREE,
        portable_artifact_file=ARTIFACT_FILE,
        portable_artifact_sha256=ARTIFACT_SHA256,
        capture_tool_commit=TOOL_COMMIT,
        capture_tool_blob_sha256=TOOL_BLOB_SHA256,
        captures=captures if captures is not None else _capture_inputs(_png_bytes()),
        app_specific=app_specific,
        approver=approver,
        custodian=custodian,
        custody_location=custody_location,
        retention_period=retention_period,
        index_generated_at=capture.dt.datetime(
            2026, 9, 3, 1, 2, 4, tzinfo=capture.dt.timezone.utc
        ),
        index_nonce="87654321",
    )


def _json_bytes(value: object) -> bytes:
    return (
        json.dumps(value, ensure_ascii=False, indent=2).encode("utf-8") + b"\n"
    )


def _sha256_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def test_capture_bundle_writer_materializes_exact_canonical_document_topology(
    tmp_path,
):
    result = _materialize(
        tmp_path, app_specific={"diagnostic": "kept below root"}
    )
    evidence_root = result["evidence_root"]
    bundle_root = result["bundle_root"]
    manifest = result["manifest"]
    capture_set = json.loads(
        result["capture_set_path"].read_text(encoding="utf-8")
    )

    assert set(manifest) == {
        "schema",
        "app",
        "app_source",
        "portable_artifact",
        "capture_tool",
        "captures",
        "approval",
        "app_specific",
    }
    assert manifest["schema"] == capture.M7_EXTERNAL_CAPTURE_BUNDLE_SCHEMA
    assert manifest["app"] == "Label_Match"
    assert manifest["app_source"] == {"commit": APP_COMMIT, "tree": APP_TREE}
    assert manifest["portable_artifact"] == {
        "file": ARTIFACT_FILE,
        "sha256": ARTIFACT_SHA256,
    }
    assert manifest["capture_tool"] == {
        "path": capture.M7_CAPTURE_TOOL_PATH,
        "commit": TOOL_COMMIT,
        "blob_sha256": TOOL_BLOB_SHA256,
    }
    assert len(capture.M7_MANIFEST_FIELD_GROUPS) == 8
    assert [entry["state_id"] for entry in manifest["captures"]] == list(
        capture.M7_REQUIRED_STATE_IDS
    )
    assert all(
        set(entry)
        == {
            "state_id",
            "viewport",
            "dpi",
            "generated_at",
            "image_file",
            "image_sha256",
            "state_manifest_file",
            "state_manifest_sha256",
        }
        and entry["viewport"] == {"width_px": 2, "height_px": 2}
        and entry["dpi"] == 96
        and entry["generated_at"] == GENERATED_AT
        for entry in manifest["captures"]
    )
    assert manifest["approval"]["approver"] == capture.M7_APPROVAL_PLACEHOLDER
    assert all(
        len(manifest["approval"][key]) == 64
        for key in ("approval_receipt_sha256", "custody_receipt_sha256")
    )
    assert manifest["app_specific"] == {"diagnostic": "kept below root"}
    assert "bundle_id" not in manifest
    assert "schema_version" not in manifest
    assert set(capture_set) == {
        "schema",
        "app",
        "bundle_id",
        "app_source",
        "portable_artifact",
        "capture_tool",
        "captures",
        "app_specific",
    }
    assert capture_set["bundle_id"] == BUNDLE_ID
    for key in (
        "schema",
        "app",
        "app_source",
        "portable_artifact",
        "capture_tool",
        "captures",
        "app_specific",
    ):
        assert capture_set[key] == manifest[key]

    for item in manifest["captures"]:
        state_path = evidence_root.joinpath(*item["state_manifest_file"].split("/"))
        state = json.loads(state_path.read_text(encoding="utf-8"))
        assert set(state) == {
            "schema",
            "app",
            "bundle_id",
            "state_id",
            "viewport",
            "dpi",
            "generated_at",
            "image_file",
            "image_sha256",
        }
        assert state["bundle_id"] == BUNDLE_ID
        assert state["state_id"] == item["state_id"]
        assert _sha256_bytes(state_path.read_bytes()) == item[
            "state_manifest_sha256"
        ]

    approval_receipt = json.loads(
        result["approval_receipt_path"].read_text(encoding="utf-8")
    )
    custody_receipt = json.loads(
        result["custody_receipt_path"].read_text(encoding="utf-8")
    )
    assert set(approval_receipt) == {
        "schema",
        "app",
        "bundle_id",
        "capture_set_file",
        "capture_set_sha256",
        "approver",
    }
    assert set(custody_receipt) == {
        "schema",
        "app",
        "bundle_id",
        "capture_set_file",
        "capture_set_sha256",
        "approval_receipt_file",
        "approval_receipt_sha256",
        "custodian",
        "custody_location",
        "retention_period",
    }
    capture_set_sha256 = _sha256_bytes(result["capture_set_path"].read_bytes())
    assert approval_receipt["capture_set_sha256"] == capture_set_sha256
    assert custody_receipt["capture_set_sha256"] == capture_set_sha256
    assert custody_receipt["approval_receipt_sha256"] == _sha256_bytes(
        result["approval_receipt_path"].read_bytes()
    )
    assert manifest["approval"]["custody_receipt_sha256"] == _sha256_bytes(
        result["custody_receipt_path"].read_bytes()
    )

    index = json.loads(result["index_path"].read_text(encoding="utf-8"))
    assert result["index_path"].name == (
        "handover-index__20260903T010204Z__87654321.json"
    )
    assert set(index) == {"schema", "manifests"}
    assert index["manifests"] == [
        {
            "app": "Label_Match",
            "manifest_file": f"Label_Match/{BUNDLE_ID}/manifest.json",
            "manifest_sha256": _sha256_bytes(result["manifest_path"].read_bytes()),
        }
    ]
    assert {path.name for path in bundle_root.iterdir()} == {
        "capture-set.json",
        "manifest.json",
        "captures",
        "states",
        "approval",
    }


def test_capture_manifest_builder_requires_portable_artifact_identity_before_rendering():
    with pytest.raises(
        capture.CaptureBundleContractError, match="PORTABLE_ARTIFACT_REQUIRED"
    ):
        capture.build_m7_external_capture_manifest(
            bundle_id=BUNDLE_ID,
            app_commit=APP_COMMIT,
            app_tree=APP_TREE,
            portable_artifact_file="",
            portable_artifact_sha256="",
            capture_tool_commit=TOOL_COMMIT,
            capture_tool_blob_sha256=TOOL_BLOB_SHA256,
            captures=(),
        )


def test_capture_bundle_writer_seals_receipts_before_final_manifest_and_index(
    monkeypatch, tmp_path
):
    writes: list[str] = []
    evidence_root = (tmp_path / "evidence").resolve()
    real_bytes_writer = capture._write_create_new_bytes
    real_json_writer = capture._write_create_new_json

    def record(path: Path) -> None:
        writes.append(path.resolve().relative_to(evidence_root).as_posix())

    def write_bytes(path: Path, payload: bytes) -> None:
        record(path)
        real_bytes_writer(path, payload)

    def write_json(path: Path, value) -> None:
        record(path)
        real_json_writer(path, value)

    monkeypatch.setattr(capture, "_write_create_new_bytes", write_bytes)
    monkeypatch.setattr(capture, "_write_create_new_json", write_json)

    result = _materialize(tmp_path)

    image_writes = [
        f"Label_Match/{BUNDLE_ID}/captures/{state_id}__2x2__96dpi.png"
        for state_id in capture.M7_REQUIRED_STATE_IDS
    ]
    state_writes = [
        f"Label_Match/{BUNDLE_ID}/states/{state_id}__2x2__96dpi.json"
        for state_id in capture.M7_REQUIRED_STATE_IDS
    ]
    assert writes[:9] == image_writes
    assert writes[9:18] == state_writes
    assert writes[18:] == [
        f"Label_Match/{BUNDLE_ID}/capture-set.json",
        f"Label_Match/{BUNDLE_ID}/approval/approval-receipt.json",
        f"Label_Match/{BUNDLE_ID}/approval/custody-receipt.json",
        f"Label_Match/{BUNDLE_ID}/manifest.json",
        "indexes/handover-index__20260903T010204Z__87654321.json",
    ]
    assert result["manifest_path"].is_file()


def test_capture_bundle_writer_rejects_forbidden_latest_before_bundle_creation(
    tmp_path,
):
    with pytest.raises(
        capture.CaptureBundleContractError,
        match="INVALID_EVIDENCE_ROOT_TOPOLOGY",
    ):
        _materialize(tmp_path, forbidden_name="latest.json")

    assert not (tmp_path / "evidence" / "Label_Match").exists()


def test_capture_bundle_writer_checks_lexical_evidence_root_before_resolving(
    monkeypatch, tmp_path
):
    evidence_root = (tmp_path / "evidence").resolve()
    real_redirect_check = capture._is_reparse_or_symlink

    def redirect_check(path: Path) -> bool:
        return Path(path) == evidence_root or real_redirect_check(path)

    monkeypatch.setattr(capture, "_is_reparse_or_symlink", redirect_check)

    with pytest.raises(
        capture.CaptureBundleContractError, match="INVALID_BUNDLE_LOCATION"
    ):
        _materialize(tmp_path)

    assert not (evidence_root / "Label_Match").exists()


def test_capture_bundle_writer_rejects_incomplete_state_set_before_bundle_creation(
    tmp_path,
):
    incomplete = _capture_inputs(_png_bytes())[:-1]

    with pytest.raises(
        capture.CaptureBundleContractError, match="INVALID_CAPTURE_STATE_SET"
    ):
        _materialize(tmp_path, captures=incomplete)

    assert not (tmp_path / "evidence" / "Label_Match").exists()


def test_capture_bundle_writer_never_seals_final_manifest_after_receipt_io_failure(
    monkeypatch, tmp_path
):
    real_json_writer = capture._write_create_new_json

    def fail_custody(path: Path, value) -> None:
        if path.name == "custody-receipt.json":
            raise OSError("synthetic custody write failure")
        real_json_writer(path, value)

    monkeypatch.setattr(capture, "_write_create_new_json", fail_custody)

    with pytest.raises(capture.CaptureBundleContractError, match="BUNDLE_WRITE_FAILED"):
        _materialize(tmp_path)

    bundle_root = tmp_path / "evidence" / "Label_Match" / BUNDLE_ID
    assert (bundle_root / "capture-set.json").is_file()
    assert (bundle_root / "approval" / "approval-receipt.json").is_file()
    assert not (bundle_root / "approval" / "custody-receipt.json").exists()
    assert not (bundle_root / "manifest.json").exists()
    assert not (tmp_path / "evidence" / "indexes").exists()


def test_synthetic_png_bundle_passes_canonical_validator_as_approval_pending(
    tmp_path, capsys
):
    assert VALIDATOR.is_file(), f"canonical validator is missing: {VALIDATOR}"
    result = _materialize(tmp_path)
    env = dict(os.environ)
    env["PYTHONDONTWRITEBYTECODE"] = "1"

    completed = subprocess.run(
        [
            sys.executable,
            "-B",
            str(VALIDATOR),
            str(result["index_path"]),
            "--app",
            "Label_Match",
        ],
        check=False,
        capture_output=True,
        text=True,
        encoding="utf-8",
        env=env,
    )
    print(completed.stdout, end="")

    report = json.loads(completed.stdout)
    assert completed.stderr == ""
    assert completed.returncode == 3
    assert report["result"] == "APPROVAL_PENDING"
    assert report["exit_code"] == 3
    assert report["summary"]["FAIL"] == 0
    assert report["summary"]["APPROVAL_PENDING"] == 5
    assert {
        check["reason_code"]
        for check in report["checks"]
        if check["status"] == "APPROVAL_PENDING"
    } == {"ORGANIZATION_PLACEHOLDER"}


def test_capture_describe_path_matches_canonical_contract_without_rendering(capsys):
    assert capture.main(["--describe-m7-contract"]) == 0

    described = json.loads(capsys.readouterr().out)
    assert described == capture.build_m7_external_capture_bundle_contract()
    assert set(described) == {
        "schema",
        "app",
        "required_state_ids",
        "app_specific",
    }
    assert "app_id" not in described
    assert described["schema"] == "M7 external capture bundle v1"
    assert described["app"] == "Label_Match"
    assert len(described["app_specific"]["manifest_required_field_groups"]) == 8
    assert all(
        path.startswith("Label_Match/<bundle-id>/")
        for path in described["app_specific"]["bundle_layout"].values()
    )


def test_capture_cli_rejects_missing_portable_artifact_before_rendering(capsys):
    assert capture.main([]) == 3

    error = json.loads(capsys.readouterr().err)
    assert error["status"] == "FAIL"
    assert error["error_code"] == "PORTABLE_ARTIFACT_REQUIRED"


def test_capture_identity_measurement_uses_git_and_exact_tool_blob(tmp_path):
    repo = tmp_path / "Label_Match"
    tool = repo / capture.M7_CAPTURE_TOOL_PATH
    tool.parent.mkdir(parents=True)
    tool.write_text("VALUE = 1\n", encoding="utf-8")
    subprocess.run(["git", "init", "-q", str(repo)], check=True)
    subprocess.run(
        ["git", "-C", str(repo), "config", "user.name", "Test"], check=True
    )
    subprocess.run(
        ["git", "-C", str(repo), "config", "user.email", "test@example.invalid"],
        check=True,
    )
    subprocess.run(["git", "-C", str(repo), "add", "."], check=True)
    subprocess.run(["git", "-C", str(repo), "commit", "-qm", "fixture"], check=True)

    measured = capture.measure_m7_capture_identities(repo, tool_root=repo)

    head = subprocess.check_output(
        ["git", "-C", str(repo), "rev-parse", "HEAD"], text=True
    ).strip()
    tree = subprocess.check_output(
        ["git", "-C", str(repo), "rev-parse", "HEAD^{tree}"], text=True
    ).strip()
    assert measured["app_source"] == {"commit": head, "tree": tree}
    assert measured["capture_tool"] == {
        "path": capture.M7_CAPTURE_TOOL_PATH,
        "commit": head,
        "blob_sha256": hashlib.sha256(tool.read_bytes()).hexdigest(),
    }


def _write_approved_bundle(tmp_path: Path) -> tuple[Path, str, bytes]:
    payload = _png_bytes()
    result = _materialize(
        tmp_path,
        captures=_capture_inputs(payload),
        approver="품질 리더",
        custodian="증거 보관 담당자",
        custody_location="승인된 외부 증거 보관소",
        retention_period="7 years",
    )
    manifest_path = result["manifest_path"]
    manifest_bytes = manifest_path.read_bytes()
    return manifest_path, _sha256_bytes(manifest_bytes), payload


def _rewrite_index_manifest_digest(manifest_path: Path, digest: str) -> None:
    evidence_root = manifest_path.parents[2]
    index_paths = sorted((evidence_root / "indexes").glob("handover-index__*.json"))
    assert len(index_paths) == 1
    index = json.loads(index_paths[0].read_text(encoding="utf-8"))
    matching = [
        entry
        for entry in index["manifests"]
        if entry["app"] == "Label_Match"
        and entry["manifest_file"]
        == f"Label_Match/{manifest_path.parent.name}/manifest.json"
    ]
    assert len(matching) == 1
    matching[0]["manifest_sha256"] = digest
    index_paths[0].write_bytes(_json_bytes(index))


def _rewrite_manifest(manifest_path: Path, manifest: dict) -> str:
    payload = _json_bytes(manifest)
    manifest_path.write_bytes(payload)
    digest = _sha256_bytes(payload)
    _rewrite_index_manifest_digest(manifest_path, digest)
    return digest


def _replace_approval_receipt_with_arbitrary_json(manifest_path: Path) -> str:
    approval_path = manifest_path.parent / "approval" / "approval-receipt.json"
    approval_payload = _json_bytes({"arbitrary": "receipt fixture"})
    approval_path.write_bytes(approval_payload)
    approval_digest = _sha256_bytes(approval_payload)

    custody_path = manifest_path.parent / "approval" / "custody-receipt.json"
    custody = json.loads(custody_path.read_text(encoding="utf-8"))
    custody["approval_receipt_sha256"] = approval_digest
    custody_payload = _json_bytes(custody)
    custody_path.write_bytes(custody_payload)

    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    manifest["approval"]["approval_receipt_sha256"] = approval_digest
    manifest["approval"]["custody_receipt_sha256"] = _sha256_bytes(custody_payload)
    return _rewrite_manifest(manifest_path, manifest)


def test_publisher_validates_an_approved_canonical_bundle(tmp_path):
    manifest_path, digest, _payload = _write_approved_bundle(tmp_path)

    report = publisher.validate_external_bundle_manifest(manifest_path, digest)

    assert report["schema"] == publisher.M7_EXTERNAL_CAPTURE_BUNDLE_SCHEMA
    assert report["app"] == "Label_Match"
    assert report["approval_status"] == "APPROVED"
    assert report["verified_state_ids"] == list(publisher.M7_REQUIRED_STATE_IDS)
    assert len(report["verified_image_files"]) == 9
    assert len(report["verified_receipt_files"]) == 2
    assert all(
        path.startswith(f"Label_Match/{BUNDLE_ID}/")
        for path in (
            *report["verified_image_files"],
            *report["verified_receipt_files"],
        )
    )
    assert report["canonical_validator_result"] == "PASS"
    assert report["canonical_validator_exit_code"] == 0
    assert report["canonical_validator_summary"]["FAIL"] == 0
    assert report["canonical_validator_summary"]["APPROVAL_PENDING"] == 0


@pytest.mark.parametrize(
    ("defect", "reason_code"),
    (
        ("schema", "SCHEMA_VIOLATION"),
        ("image_digest", "IMAGE_DIGEST_MISMATCH"),
    ),
)
def test_publisher_rejects_canonical_validator_failures(tmp_path, defect, reason_code):
    manifest_path, _digest, _payload = _write_approved_bundle(tmp_path)
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    if defect == "schema":
        manifest["schema"] = "not canonical"
    elif defect == "image_digest":
        manifest["captures"][0]["image_sha256"] = "0" * 64
    digest = _rewrite_manifest(manifest_path, manifest)

    with pytest.raises(publisher.ExternalBundleValidationError) as caught:
        publisher.validate_external_bundle_manifest(manifest_path, digest)
    assert caught.value.code == "CANONICAL_VALIDATOR_REJECTED"
    assert reason_code in str(caught.value)


def test_publisher_rejects_empty_states_directory(tmp_path):
    manifest_path, digest, _payload = _write_approved_bundle(tmp_path)
    states_dir = manifest_path.parent / "states"
    for state_path in states_dir.iterdir():
        state_path.unlink()
    assert not list(states_dir.iterdir())

    with pytest.raises(publisher.ExternalBundleValidationError) as caught:
        publisher.validate_external_bundle_manifest(manifest_path, digest)
    assert caught.value.code == "CANONICAL_VALIDATOR_REJECTED"
    assert "FILE_MISSING" in str(caught.value)


def test_publisher_rejects_arbitrary_receipt_with_resealed_digest_chain(tmp_path):
    manifest_path, _digest, _payload = _write_approved_bundle(tmp_path)
    digest = _replace_approval_receipt_with_arbitrary_json(manifest_path)

    with pytest.raises(publisher.ExternalBundleValidationError) as caught:
        publisher.validate_external_bundle_manifest(manifest_path, digest)
    assert caught.value.code == "CANONICAL_VALIDATOR_REJECTED"
    assert "SCHEMA_VIOLATION" in str(caught.value)


def test_publisher_rejects_canonical_approval_pending_exit_3(tmp_path):
    result = _materialize(tmp_path)
    manifest_path = result["manifest_path"]
    digest = _sha256_bytes(manifest_path.read_bytes())

    with pytest.raises(publisher.ExternalBundleValidationError) as caught:
        publisher.validate_external_bundle_manifest(manifest_path, digest)
    assert caught.value.code == "APPROVAL_PENDING"
    assert "canonical validator exit 3" in str(caught.value)
    assert "ORGANIZATION_PLACEHOLDER" in str(caught.value)


def test_publisher_rejects_expected_manifest_digest_mismatch(tmp_path):
    manifest_path, _digest, _payload = _write_approved_bundle(tmp_path)

    with pytest.raises(
        publisher.ExternalBundleValidationError, match="MANIFEST_DIGEST_MISMATCH"
    ):
        publisher.validate_external_bundle_manifest(manifest_path, "0" * 64)


def test_publisher_dry_run_verifies_bundle_without_network_or_file_writes(
    monkeypatch, tmp_path, capsys
):
    manifest_path, digest, _payload = _write_approved_bundle(tmp_path)
    report_path = tmp_path / "must-not-exist.json"
    monkeypatch.setattr(
        publisher,
        "_load_outline_config",
        lambda _args: (publisher.DEFAULT_OUTLINE_URL, ""),
    )

    result = publisher.main(
        [
            "--external-bundle-manifest",
            str(manifest_path),
            "--expected-manifest-sha256",
            digest,
            "--report-path",
            str(report_path),
            "--dry-run",
        ]
    )

    report = json.loads(capsys.readouterr().out)
    assert result == 0
    assert report["status"] == "PASS"
    assert report["network_writes"] == 0
    assert report["file_writes"] == 0
    assert report["verified_state_ids"] == list(publisher.M7_REQUIRED_STATE_IDS)
    assert report["canonical_validator_result"] == "PASS"
    assert report["document_reference_only"] is True
    assert not report_path.exists()


def test_publisher_live_path_updates_reference_text_without_uploading_raster(
    monkeypatch, tmp_path, capsys
):
    manifest_path, digest, _payload = _write_approved_bundle(tmp_path)

    class FakeOutlineClient:
        instance = None

        def __init__(self, *_args, **_kwargs):
            self.base_url = publisher.DEFAULT_OUTLINE_URL
            self.updated_text = ""
            self.calls = []
            FakeOutlineClient.instance = self

        def upload_image(self, *_args, **_kwargs):
            raise AssertionError("external-bundle publishing must not upload raster")

        def api(self, method, payload):
            self.calls.append((method, payload))
            if method == "documents.update":
                self.updated_text = payload["text"]
                return {"ok": True}
            if method == "documents.info":
                return {
                    "ok": True,
                    "data": {
                        "document": {"text": self.updated_text, "url": "/doc/label"}
                    },
                }
            raise AssertionError(method)

    monkeypatch.setattr(publisher, "OutlineClient", FakeOutlineClient)
    monkeypatch.setattr(
        publisher,
        "_load_outline_config",
        lambda _args: (publisher.DEFAULT_OUTLINE_URL, "token"),
    )

    result = publisher.main(
        [
            "--external-bundle-manifest",
            str(manifest_path),
            "--expected-manifest-sha256",
            digest,
        ]
    )

    report = json.loads(capsys.readouterr().out)
    assert result == 0
    assert report["status"] == "PASS"
    assert report["unique_images_uploaded"] == 0
    assert report["document_markdown_image_refs"] == 0
    assert report["document_digest_values"] == 0
    assert [name for name, _payload in FakeOutlineClient.instance.calls] == [
        "documents.update",
        "documents.info",
    ]


def test_legacy_cli_names_the_external_bundle_replacement(capsys):
    result = publisher.main(["--dry-run"])

    error = capsys.readouterr().err
    assert result == 1
    assert "LEGACY_TRACKED_IMAGE_PUBLISHING_DEPRECATED" in error
    assert "--external-bundle-manifest" in error
    assert "--expected-manifest-sha256" in error


def test_publisher_cannot_write_reports_into_canonical_handover():
    forbidden = publisher.M7_CANONICAL_HANDOVER_DIR / "publisher-result.json"

    with pytest.raises(
        publisher.ExternalBundleValidationError, match="HANDOVER_WRITE_FORBIDDEN"
    ):
        publisher._write_report(str(forbidden), {"status": "PASS"})
