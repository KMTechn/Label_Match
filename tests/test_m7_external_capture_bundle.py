from __future__ import annotations

import hashlib
from io import BytesIO
import json
from pathlib import Path
import subprocess

import pytest
from PIL import Image

from tools import capture_label_operator_ui as capture
from tools import publish_outline_user_manual as publisher


APP_COMMIT = "a" * 40
APP_TREE = "b" * 40
ARTIFACT_SHA256 = "c" * 64
TOOL_COMMIT = "d" * 40
TOOL_BLOB_SHA256 = "e" * 64
GENERATED_AT = "2026-09-03T01:02:03Z"


def _png_bytes(size: tuple[int, int] = (2, 2)) -> bytes:
    stream = BytesIO()
    Image.new("RGB", size, "white").save(stream, format="PNG")
    return stream.getvalue()


def _capture_inputs(payload: bytes) -> list[dict[str, object]]:
    return [
        {
            "state_id": state_id,
            "image_file": f"captures/{state_id}__2x2__96dpi.png",
            "image_bytes": payload,
            "viewport": {"width_px": 2, "height_px": 2},
            "dpi": 96,
            "generated_at": GENERATED_AT,
        }
        for state_id in capture.M7_REQUIRED_STATE_IDS
    ]


def _manifest(payload: bytes, *, approval=None, app_specific=None):
    return capture.build_m7_external_capture_manifest(
        app_commit=APP_COMMIT,
        app_tree=APP_TREE,
        portable_artifact_file="portable/Label_Match.zip",
        portable_artifact_sha256=ARTIFACT_SHA256,
        capture_tool_commit=TOOL_COMMIT,
        capture_tool_blob_sha256=TOOL_BLOB_SHA256,
        captures=_capture_inputs(payload),
        approval=approval,
        app_specific=app_specific,
    )


def _json_bytes(value: object) -> bytes:
    return (
        json.dumps(value, ensure_ascii=False, indent=2).encode("utf-8") + b"\n"
    )


def _sha256_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def test_capture_manifest_builder_uses_all_eight_canonical_groups_from_png_bytes():
    payload = _png_bytes()
    manifest = _manifest(payload, app_specific={"diagnostic": "kept below root"})

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
        "file": "portable/Label_Match.zip",
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
        entry["image_sha256"] == _sha256_bytes(payload)
        and entry["viewport"] == {"width_px": 2, "height_px": 2}
        and entry["dpi"] == 96
        and entry["generated_at"] == GENERATED_AT
        for entry in manifest["captures"]
    )
    assert manifest["approval"]["approver"] == capture.M7_APPROVAL_PLACEHOLDER
    assert manifest["app_specific"] == {"diagnostic": "kept below root"}
    assert "schema_version" not in manifest


def test_capture_manifest_builder_requires_portable_artifact_identity_before_rendering():
    with pytest.raises(
        capture.CaptureBundleContractError, match="PORTABLE_ARTIFACT_REQUIRED"
    ):
        capture.build_m7_external_capture_manifest(
            app_commit=APP_COMMIT,
            app_tree=APP_TREE,
            portable_artifact_file="",
            portable_artifact_sha256="",
            capture_tool_commit=TOOL_COMMIT,
            capture_tool_blob_sha256=TOOL_BLOB_SHA256,
            captures=_capture_inputs(_png_bytes()),
        )


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
    bundle_id = "Label_Match__aaaaaaaaaaaa__20260903T010203Z__1234abcd"
    bundle_root = tmp_path / "Label_Match" / bundle_id
    for name in ("captures", "states", "approval"):
        (bundle_root / name).mkdir(parents=True, exist_ok=True)
    approval_receipt = _json_bytes(
        {"schema": "approval receipt fixture", "capture_set": "capture-set.json"}
    )
    custody_receipt = _json_bytes(
        {
            "schema": "custody receipt fixture",
            "capture_set": "capture-set.json",
            "approval_receipt": "approval/approval-receipt.json",
        }
    )
    (bundle_root / "approval" / "approval-receipt.json").write_bytes(
        approval_receipt
    )
    (bundle_root / "approval" / "custody-receipt.json").write_bytes(
        custody_receipt
    )
    approval = {
        "approver": "품질 리더",
        "approval_receipt_file": "approval/approval-receipt.json",
        "approval_receipt_sha256": _sha256_bytes(approval_receipt),
        "custody_receipt_file": "approval/custody-receipt.json",
        "custody_receipt_sha256": _sha256_bytes(custody_receipt),
    }
    manifest = _manifest(payload, approval=approval)
    for entry in manifest["captures"]:
        (bundle_root / entry["image_file"]).write_bytes(payload)
    capture_set = {
        key: manifest[key]
        for key in (
            "schema",
            "app",
            "app_source",
            "portable_artifact",
            "capture_tool",
            "captures",
        )
    }
    (bundle_root / "capture-set.json").write_bytes(_json_bytes(capture_set))
    manifest_bytes = _json_bytes(manifest)
    manifest_path = bundle_root / "manifest.json"
    manifest_path.write_bytes(manifest_bytes)
    return manifest_path, _sha256_bytes(manifest_bytes), payload


def _rewrite_manifest(manifest_path: Path, manifest: dict) -> str:
    payload = _json_bytes(manifest)
    manifest_path.write_bytes(payload)
    return _sha256_bytes(payload)


def test_publisher_validates_an_approved_canonical_bundle(tmp_path):
    manifest_path, digest, _payload = _write_approved_bundle(tmp_path)

    report = publisher.validate_external_bundle_manifest(manifest_path, digest)

    assert report["schema"] == publisher.M7_EXTERNAL_CAPTURE_BUNDLE_SCHEMA
    assert report["app"] == "Label_Match"
    assert report["approval_status"] == "APPROVED"
    assert report["verified_state_ids"] == list(publisher.M7_REQUIRED_STATE_IDS)
    assert len(report["verified_image_files"]) == 9
    assert len(report["verified_receipt_files"]) == 2


@pytest.mark.parametrize(
    ("defect", "error_code"),
    (
        ("schema", "SCHEMA_MISMATCH"),
        ("state", "REQUIRED_STATE_SET_MISMATCH"),
        ("image_digest", "IMAGE_DIGEST_MISMATCH"),
        ("approval", "APPROVAL_PENDING"),
    ),
)
def test_publisher_rejects_canonical_bundle_defects(tmp_path, defect, error_code):
    manifest_path, _digest, _payload = _write_approved_bundle(tmp_path)
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    if defect == "schema":
        manifest["schema"] = "not canonical"
    elif defect == "state":
        manifest["captures"].pop()
    elif defect == "image_digest":
        manifest["captures"][0]["image_sha256"] = "0" * 64
    elif defect == "approval":
        manifest["approval"]["approver"] = publisher.M7_APPROVAL_PLACEHOLDER
    digest = _rewrite_manifest(manifest_path, manifest)

    with pytest.raises(publisher.ExternalBundleValidationError, match=error_code):
        publisher.validate_external_bundle_manifest(manifest_path, digest)


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
