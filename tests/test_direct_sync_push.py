import hashlib
import json
from pathlib import Path

from direct_sync_push import (
    DEFAULT_ENDPOINT_PATH,
    ProducerCredentials,
    build_source_file_plan,
    canonical_json,
    canonical_request_string,
    count_csv_data_rows,
    manifest_hash,
    signed_headers,
    upload_source_file,
)


class FakeResponse:
    def __init__(self, status_code, payload):
        self.status_code = status_code
        self._payload = payload

    def json(self):
        return self._payload


class FakeSession:
    def __init__(self, response):
        self.response = response
        self.calls = []

    def post(self, url, *, data, files, headers, timeout):
        file_name, file_handle, content_type = files["file"]
        self.calls.append(
            {
                "url": url,
                "metadata": data["metadata"],
                "headers": dict(headers),
                "timeout": timeout,
                "file_name": file_name,
                "file_bytes": file_handle.read(),
                "content_type": content_type,
            }
        )
        return self.response


def make_manifest(tmp_path):
    manifest = {
        "schema_version": "producer-onboarding-manifest-v1",
        "pc_identity": {
            "pc_id": "LABEL-PC01",
            "source_host_id": "label-host-1",
            "producer_install_id": "install-label-1",
        },
        "apps": ["LabelMatch"],
        "streams": [
            {
                "stream_name": "label_match_events",
                "source_system": "label_match",
                "source_transport": "legacy_packaging_csv",
            }
        ],
    }
    path = tmp_path / "producer_manifest.json"
    path.write_text(json.dumps(manifest, ensure_ascii=False), encoding="utf-8")
    return manifest, path


def write_csv(tmp_path):
    path = tmp_path / "패키징작업이벤트로그_fixture_20260621.csv"
    path.write_text(
        "timestamp,worker_name,event,details\n"
        "2026-06-21T00:00:00,worker,LABEL_MATCHED,\"{ \"\"product_barcode\"\": \"\"BC-1\"\" }\"\n",
        encoding="utf-8",
    )
    return path


def make_credentials():
    return ProducerCredentials(
        producer_id="producer-label",
        key_id="key-label",
        secret="label-secret",
        endpoint_url="https://worker.example.invalid/api/producer-ingest/v1/source-file",
    )


def test_build_plan_uses_label_match_stream_and_csv_rows(tmp_path):
    manifest, manifest_path = make_manifest(tmp_path)
    csv_path = write_csv(tmp_path)
    credentials = make_credentials()

    plan = build_source_file_plan(
        source_file_path=csv_path,
        producer_manifest_path=manifest_path,
        credentials=credentials,
    )

    content = csv_path.read_bytes()
    assert plan.content_sha256 == hashlib.sha256(content).hexdigest()
    assert plan.byte_length == len(content)
    assert count_csv_data_rows(csv_path) == 1
    assert plan.metadata["manifest_hash"] == manifest_hash(manifest)
    assert plan.metadata["stream_name"] == "label_match_events"
    assert plan.metadata["source_system"] == "label_match"
    assert plan.metadata["source_transport"] == "legacy_packaging_csv"
    assert plan.metadata["relative_path"] == f"legacy_csv/{csv_path.name}"
    assert plan.metadata["row_count"] == 1
    assert plan.metadata["first_row_number"] == 2
    assert plan.metadata["last_row_number"] == 2


def test_signed_headers_match_server_contract(tmp_path):
    _manifest, manifest_path = make_manifest(tmp_path)
    csv_path = write_csv(tmp_path)
    credentials = make_credentials()
    plan = build_source_file_plan(
        source_file_path=csv_path,
        producer_manifest_path=manifest_path,
        credentials=credentials,
    )

    headers = signed_headers(
        credentials,
        plan.metadata,
        timestamp="2026-06-21T00:00:00Z",
        nonce="nonce-label-1",
    )
    canonical = canonical_request_string(
        method="POST",
        path=DEFAULT_ENDPOINT_PATH,
        query_string="",
        timestamp="2026-06-21T00:00:00Z",
        nonce="nonce-label-1",
        producer_id=credentials.producer_id,
        key_id=credentials.key_id,
        metadata=plan.metadata,
        content_sha256=plan.metadata["content_sha256"],
        byte_length=plan.metadata["byte_length"],
        content_type="multipart/form-data",
    )
    import hmac

    expected = hmac.new(b"label-secret", canonical.encode("utf-8"), hashlib.sha256).hexdigest()
    assert headers["X-Producer-Signature"] == expected


def test_upload_writes_status_without_storing_secret(tmp_path):
    _manifest, manifest_path = make_manifest(tmp_path)
    csv_path = write_csv(tmp_path)
    credentials = make_credentials()
    plan = build_source_file_plan(
        source_file_path=csv_path,
        producer_manifest_path=manifest_path,
        credentials=credentials,
    )
    session = FakeSession(
        FakeResponse(
            200,
            {
                "request_id": "request-label-1",
                "client_batch_id": plan.metadata["client_batch_id"],
                "server_source_file_id": "label-host-1/label_match_events/legacy_csv/file.csv",
                "committed": True,
                "status": "accepted",
                "totals": {"inserted": 1, "replayed": 0, "quarantined": 0, "errors": 0},
            },
        )
    )

    result = upload_source_file(plan, credentials, session=session, status_dir=tmp_path / "status")

    assert result.success is True
    assert result.committed is True
    assert Path(result.status_path).is_file()
    assert json.loads(session.calls[0]["metadata"]) == plan.metadata
    assert session.calls[0]["metadata"] == canonical_json(plan.metadata)
    assert session.calls[0]["file_bytes"] == csv_path.read_bytes()
    status_text = Path(result.status_path).read_text(encoding="utf-8")
    assert "label-secret" not in status_text
    assert "X-Producer-Signature" not in status_text
