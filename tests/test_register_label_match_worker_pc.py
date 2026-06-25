import importlib.util
import json
from pathlib import Path


def load_registration_module():
    module_path = Path(__file__).resolve().parents[1] / "tools" / "register_label_match_worker_pc.py"
    spec = importlib.util.spec_from_file_location("register_label_match_worker_pc_for_tests", module_path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_label_match_registration_dry_run_derives_per_pc_identity_without_secret(tmp_path):
    module = load_registration_module()
    report_path = tmp_path / "registration-dry-run.json"
    result = module.main(
        [
            "--dry-run",
            "--server-base-url",
            "https://worker.example.invalid",
            "--pc-id",
            "PACKING-PC-01",
            "--machine-guid",
            "machine-guid-one",
            "--sync-dir",
            str(tmp_path / "Label_Match" / "data"),
            "--data-dir",
            str(tmp_path / "DirectSync" / "label_match"),
            "--report-path",
            str(report_path),
        ]
    )

    assert result == 0
    report = json.loads(report_path.read_text(encoding="utf-8-sig"))
    report_text = report_path.read_text(encoding="utf-8-sig")
    assert report["status"] == "DRY_RUN"
    assert report["manual_pc_approval_required"] is False
    assert report["source_host_id"].startswith("label-match-packing-pc-01-")
    assert report["producer_id"] == f"producer-{report['source_host_id']}"
    assert report["key_id"] == f"key-{report['source_host_id']}"
    assert report["endpoint_url"] == "https://worker.example.invalid/api/producer-ingest/v1/source-file"
    assert report["server_registration_verified"] is False
    assert report["secret_bootstrap_verified"] is False
    assert "machine-guid-one" not in report_text
    assert "secret" not in report.get("secret_ref", "")


def test_label_match_registration_manifest_includes_runtime_event_contract(tmp_path):
    module = load_registration_module()
    args = type(
        "Args",
        (),
        {
            "data_dir": str(tmp_path / "DirectSync" / "label_match"),
            "endpoint_url": "",
            "key_id": "",
            "machine_guid": "machine-guid-contract",
            "pc_id": "PACKING-PC-CONTRACT",
            "producer_id": "",
            "producer_install_id": "",
            "secret_ref_target": "",
            "server_base_url": "https://worker.example.invalid",
            "source_host_id": "",
            "sync_dir": str(tmp_path / "Label_Match" / "data"),
            "dry_run": True,
            "enrollment_url": "",
        },
    )()

    manifest, _credential, _report = module.build_payloads(args)
    raw_event_names = set(manifest["streams"][0]["raw_event_names"])

    assert {
        "APP_START",
        "APP_CLOSE",
        "SCAN_ATTEMPT",
        "SCAN_OK",
        "TRAY_COMPLETE",
        "SET_CANCELLED",
        "SET_DELETED",
        "SET_RESTORED",
        "TRAY_COMPLETION_CANCELLED",
        "UI_ERROR",
        "ERROR_INPUT",
        "ERROR_MISMATCH",
        "BASE64_DECODED",
    }.issubset(raw_event_names)


def test_label_match_registration_apply_writes_manifest_credential_and_receipt_without_raw_secret(
    tmp_path,
    monkeypatch,
):
    module = load_registration_module()
    data_dir = tmp_path / "DirectSync" / "label_match"
    sync_dir = tmp_path / "Label_Match" / "data"
    report_path = data_dir / "status" / "registration.json"
    secret = "server-issued-secret"

    def fake_enroll(payload, *, enrollment_url, enrollment_token, timeout_seconds):
        assert enrollment_token == "install-token"
        assert payload["contract_version"] == module.ENROLLMENT_CONTRACT_VERSION
        assert payload["manifest"]["streams"][0]["stream_name"] == "label_match_events"
        return {
            "status": "enrolled",
            "producer_id": payload["producer_id"],
            "key_id": payload["key_id"],
            "secret": secret,
            "secret_fingerprint_sha256": module._fingerprint(secret),
            "client_receipt": {
                "receipt_schema_version": "producer-self-enrollment-client-receipt-v1",
                "status": "enrolled",
                "producer_id": payload["producer_id"],
                "key_id": payload["key_id"],
            },
            "server_binding": {
                "producer_manifest_path": "/srv/producers/label/producer_manifest.json",
                "registry_path": "/srv/producers/label/source_registry.json",
            },
        }

    monkeypatch.setattr(module, "_enroll", fake_enroll)
    monkeypatch.setattr(module, "_write_dpapi_secret", lambda data_dir, target, secret_text: Path(data_dir) / "secrets" / f"{target}.dpapi")
    monkeypatch.setattr(module, "_verify_dpapi_secret", lambda data_dir, target, secret_text: secret_text == secret)

    result = module.main(
        [
            "--apply",
            "--server-base-url",
            "https://worker.example.invalid",
            "--enrollment-token",
            "install-token",
            "--pc-id",
            "PACKING-PC-02",
            "--machine-guid",
            "machine-guid-two",
            "--sync-dir",
            str(sync_dir),
            "--data-dir",
            str(data_dir),
            "--report-path",
            str(report_path),
        ]
    )

    assert result == 0
    manifest_path = data_dir / "producer_manifest.json"
    credential_path = data_dir / "credential.json"
    receipt_path = data_dir / "evidence" / "producer_self_enrollment_receipt.json"
    assert manifest_path.is_file()
    assert credential_path.is_file()
    assert receipt_path.is_file()

    manifest = json.loads(manifest_path.read_text(encoding="utf-8-sig"))
    credential = json.loads(credential_path.read_text(encoding="utf-8-sig"))
    report = json.loads(report_path.read_text(encoding="utf-8-sig"))
    combined_text = "\n".join(
        [
            manifest_path.read_text(encoding="utf-8-sig"),
            credential_path.read_text(encoding="utf-8-sig"),
            receipt_path.read_text(encoding="utf-8-sig"),
            report_path.read_text(encoding="utf-8-sig"),
        ]
    )

    assert manifest["identity_registry"]["status"] == "self_enrolled"
    assert manifest["sync"]["sync_transport"] == "http_push"
    assert manifest["sync"]["sync_dir"] == str(sync_dir.resolve())
    assert credential["secret_ref"].startswith("dpapi:")
    assert credential["secret_data_dir"] == str(data_dir.resolve())
    assert "secret" not in credential
    assert report["status"] == "SELF_ENROLLMENT_REGISTERED"
    assert report["server_registration_verified"] is True
    assert report["secret_bootstrap_verified"] is True
    assert report["manual_pc_approval_required"] is False
    assert "server-issued-secret" not in combined_text
    assert "install-token" not in combined_text


def test_label_match_registration_apply_can_use_ip_allowlisted_server_without_token(tmp_path, monkeypatch):
    module = load_registration_module()
    data_dir = tmp_path / "DirectSync" / "label_match"
    sync_dir = tmp_path / "Label_Match" / "data"
    report_path = data_dir / "status" / "registration.json"
    secret = "server-issued-secret"

    def fake_enroll(payload, *, enrollment_url, enrollment_token, timeout_seconds):
        assert enrollment_token == ""
        return {
            "status": "enrolled",
            "producer_id": payload["producer_id"],
            "key_id": payload["key_id"],
            "secret": secret,
            "secret_fingerprint_sha256": module._fingerprint(secret),
            "client_receipt": {
                "receipt_schema_version": "producer-self-enrollment-client-receipt-v1",
                "status": "enrolled",
                "producer_id": payload["producer_id"],
                "key_id": payload["key_id"],
            },
            "server_binding": {
                "producer_manifest_path": "/srv/producers/label/producer_manifest.json",
                "registry_path": "/srv/producers/label/source_registry.json",
            },
        }

    monkeypatch.setattr(module, "_enroll", fake_enroll)
    monkeypatch.setattr(module, "_write_dpapi_secret", lambda data_dir, target, secret_text: Path(data_dir) / "secrets" / f"{target}.dpapi")
    monkeypatch.setattr(module, "_verify_dpapi_secret", lambda data_dir, target, secret_text: secret_text == secret)

    result = module.main(
        [
            "--apply",
            "--server-base-url",
            "https://worker.example.invalid",
            "--enrollment-token-env",
            "",
            "--sync-dir",
            str(sync_dir),
            "--data-dir",
            str(data_dir),
            "--report-path",
            str(report_path),
        ]
    )

    assert result == 0
    report = json.loads(report_path.read_text(encoding="utf-8-sig"))
    assert report["status"] == "SELF_ENROLLMENT_REGISTERED"
    assert report["token_source"] == "ip_allowlist"
    assert report["manual_pc_approval_required"] is False
    assert report["raw_secret_written"] is False


def test_label_match_registration_does_not_auto_load_adjacent_token_file(tmp_path, monkeypatch):
    module = load_registration_module()
    fake_tool_dir = tmp_path / "tools"
    fake_tool_dir.mkdir()
    fake_module_path = fake_tool_dir / "register_label_match_worker_pc.py"
    fake_module_path.write_text("# test fixture\n", encoding="utf-8")
    (fake_tool_dir / "enrollment_token.txt").write_text("should-not-be-used", encoding="utf-8")
    monkeypatch.setattr(module, "__file__", str(fake_module_path))

    args = type(
        "Args",
        (),
        {
            "enrollment_token": "",
            "enrollment_token_file": "",
            "enrollment_token_env": "",
        },
    )()

    assert module._token_from_sources(args) == ("ip_allowlist", "")
