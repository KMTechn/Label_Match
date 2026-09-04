"""Real-child and real-HTTP regressions for Label_Match data-plane mocks.

These tests exist so a no-op subprocess or a no-op HTTP client turns the
suite red. They must not mock subprocess, requests.Session, or urlopen.
Product code is not changed here; a failure is a finding.
"""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from email import policy
from email.parser import BytesParser
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from ipaddress import IPv4Address
import json
import os
from pathlib import Path
import socket
import ssl
import subprocess
import sys
import threading
from typing import Any, Iterator, Mapping
from urllib.parse import urlsplit

from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import ec
from cryptography.x509.oid import ExtendedKeyUsageOID, NameOID
import pytest

import package_logistics as package_module
import producer_runtime_client as runtime_client
import user_relay
import writer_session_fence as fence
from current_user_onboarding import resolve_current_user_onboarding_paths
from direct_sync_push import (
    RELAY_STATUS_ACKED,
    ProducerCredentials,
    build_source_file_plan,
    drain_one_relay_batch,
    enqueue_source_file_for_relay,
    relay_queue_status,
    upload_source_file,
)
from logistics_runtime_profile import protect_current_user_secret
from package_logistics import PackageClientConfig, PackageLogisticsClient


ROOT = Path(__file__).resolve().parents[1]
TESTS_DIR = Path(__file__).resolve().parent
LOOPBACK_HOST = "label-match-loopback.test"
LOOPBACK_HOST_ENV = "KMTECH_TEST_LOOPBACK_HOST"
INGEST_PATH = "/api/producer-ingest/v1/source-file"
LEASE_PATH = "/api/producer-ingest/v1/runtime-lease"
PACKAGE_PATH = "/logistics/api/v1/packages"
CAPABILITIES_PATH = "/logistics/api/v1/capabilities"
LEASE_EXPIRES_AT = "2099-01-01T00:00:00Z"
BARCODE = "LM-REAL-HTTP-BC-1"
CSV_NAME = "포장실작업이벤트로그_real_http_20260904.csv"


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )


def _write_packaging_csv(directory: Path, *, barcode: str = BARCODE) -> Path:
    directory.mkdir(parents=True, exist_ok=True)
    path = directory / CSV_NAME
    path.write_text(
        "timestamp,worker_name,event,details\n"
        f"2026-09-04T00:00:00,worker,TRAY_COMPLETE,"
        f"\"{{ \"\"product_barcode\"\": \"\"{barcode}\"\" }}\"\n",
        encoding="utf-8",
    )
    return path


def _write_manifest(path: Path) -> dict[str, Any]:
    manifest = {
        "schema_version": "producer-onboarding-manifest-v1",
        "pc_identity": {
            "pc_id": "LABEL-REAL-HTTP-PC",
            "source_host_id": "label-real-http-host",
            "producer_install_id": "label-real-http-install",
        },
        "apps": ["LabelMatch"],
        "streams": [
            {
                "producer_role": "label_match",
                "stream_name": "label_match_events",
                "source_system": "label_match",
                "source_transport": "legacy_packaging_csv",
            }
        ],
    }
    _write_json(path, manifest)
    return manifest


def _write_credential(path: Path, *, endpoint_url: str) -> None:
    _write_json(
        path,
        {
            "producer_id": "producer-label-real-http",
            "key_id": "key-label-real-http",
            "secret": "label-real-http-secret",
            "endpoint_url": endpoint_url,
            "runtime_lease_mode": "enforce",
        },
    )


def _write_tls_material(directory: Path, hostname: str) -> tuple[Path, Path, Path]:
    directory.mkdir(parents=True, exist_ok=True)
    now = _now()
    ca_key = ec.generate_private_key(ec.SECP256R1())
    ca_name = x509.Name(
        [x509.NameAttribute(NameOID.COMMON_NAME, "Label Match Loopback Test CA")]
    )
    ca_cert = (
        x509.CertificateBuilder()
        .subject_name(ca_name)
        .issuer_name(ca_name)
        .public_key(ca_key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(now - timedelta(minutes=1))
        .not_valid_after(now + timedelta(days=1))
        .add_extension(x509.BasicConstraints(ca=True, path_length=0), critical=True)
        .add_extension(
            x509.KeyUsage(
                digital_signature=True,
                content_commitment=False,
                key_encipherment=False,
                data_encipherment=False,
                key_agreement=False,
                key_cert_sign=True,
                crl_sign=True,
                encipher_only=False,
                decipher_only=False,
            ),
            critical=True,
        )
        .sign(ca_key, hashes.SHA256())
    )
    leaf_key = ec.generate_private_key(ec.SECP256R1())
    leaf_name = x509.Name(
        [x509.NameAttribute(NameOID.COMMON_NAME, hostname)]
    )
    leaf_cert = (
        x509.CertificateBuilder()
        .subject_name(leaf_name)
        .issuer_name(ca_name)
        .public_key(leaf_key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(now - timedelta(minutes=1))
        .not_valid_after(now + timedelta(days=1))
        .add_extension(x509.BasicConstraints(ca=False, path_length=None), critical=True)
        .add_extension(
            x509.SubjectAlternativeName(
                [
                    x509.DNSName(hostname),
                    x509.IPAddress(IPv4Address("127.0.0.1")),
                ]
            ),
            critical=False,
        )
        .add_extension(
            x509.ExtendedKeyUsage([ExtendedKeyUsageOID.SERVER_AUTH]),
            critical=False,
        )
        .sign(ca_key, hashes.SHA256())
    )
    ca_path = directory / "ca-bundle.pem"
    cert_path = directory / "server.pem"
    key_path = directory / "server.key"
    ca_path.write_bytes(ca_cert.public_bytes(serialization.Encoding.PEM))
    cert_path.write_bytes(leaf_cert.public_bytes(serialization.Encoding.PEM))
    key_path.write_bytes(
        leaf_key.private_bytes(
            encoding=serialization.Encoding.PEM,
            format=serialization.PrivateFormat.TraditionalOpenSSL,
            encryption_algorithm=serialization.NoEncryption(),
        )
    )
    return ca_path, cert_path, key_path


def _install_loopback_host_alias(monkeypatch: pytest.MonkeyPatch, hostname: str) -> None:
    alias = hostname.rstrip(".").lower()
    original_getaddrinfo = socket.getaddrinfo

    def getaddrinfo(host, port, family=0, type=0, proto=0, flags=0):
        selected = str(host or "").strip().rstrip(".").lower()
        if selected == alias:
            return original_getaddrinfo(
                "127.0.0.1",
                port,
                socket.AF_INET,
                type,
                proto,
                flags,
            )
        return original_getaddrinfo(host, port, family, type, proto, flags)

    monkeypatch.setattr(socket, "getaddrinfo", getaddrinfo)


def _isolate_writer_and_user_roots(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> dict[str, Path]:
    control_root = tmp_path / "writer-control"
    local_app_data = tmp_path / "LocalAppData"
    program_data = tmp_path / "ProgramData"
    temp_root = tmp_path / "tmp"
    control_root.mkdir(parents=True)
    local_app_data.mkdir(parents=True)
    program_data.mkdir(parents=True)
    temp_root.mkdir(parents=True)
    monkeypatch.setenv(fence.TEST_MODE_ENV, "1")
    monkeypatch.setenv(fence.CONTROL_ROOT_OVERRIDE_ENV, str(control_root))
    monkeypatch.setenv("LOCALAPPDATA", str(local_app_data))
    monkeypatch.setenv("PROGRAMDATA", str(program_data))
    monkeypatch.setenv("TEMP", str(temp_root))
    monkeypatch.setenv("TMP", str(temp_root))
    monkeypatch.setenv("PYTHONDONTWRITEBYTECODE", "1")
    monkeypatch.delenv("APP_ENV", raising=False)
    monkeypatch.delenv("ENV", raising=False)
    monkeypatch.delenv("LABEL_MATCH_PRODUCTION", raising=False)
    monkeypatch.delenv("DIRECT_SYNC_PRODUCTION", raising=False)
    child_pythonpath = [str(TESTS_DIR)]
    inherited = str(os.environ.get("PYTHONPATH") or "").strip()
    if inherited:
        child_pythonpath.append(inherited)
    monkeypatch.setenv("PYTHONPATH", os.pathsep.join(child_pythonpath))
    monkeypatch.setenv(LOOPBACK_HOST_ENV, LOOPBACK_HOST)
    return {
        "control_root": control_root,
        "local_app_data": local_app_data,
        "program_data": program_data,
        "temp_root": temp_root,
    }


def _child_environment(control_root: Path, local_app_data: Path) -> dict[str, str]:
    values = os.environ.copy()
    values[fence.TEST_MODE_ENV] = "1"
    values[fence.CONTROL_ROOT_OVERRIDE_ENV] = str(control_root)
    values["LOCALAPPDATA"] = str(local_app_data)
    values["PYTHONPATH"] = os.pathsep.join(
        [str(TESTS_DIR), *filter(None, str(values.get("PYTHONPATH") or "").split(os.pathsep))]
    )
    values[LOOPBACK_HOST_ENV] = LOOPBACK_HOST
    values["PYTHONDONTWRITEBYTECODE"] = "1"
    values.pop("PYTHONSAFEPATH", None)
    return values


def _runtime_token(sequence: int) -> str:
    return f"{int(sequence):043d}"


def _lease_grant(request: Mapping[str, Any]) -> dict[str, Any]:
    next_sequence = int(request.get("runtime_request_sequence") or 0) + 1
    return {
        "ok": True,
        "status": "ACTIVE",
        "contract_version": runtime_client.CONTRACT_VERSION,
        "operation": "renewed" if "runtime_fence" in request else "issued",
        "lease_id": "lease-label-real-http",
        "producer_install_id": request.get("producer_install_id")
        or "label-real-http-install",
        "runtime_instance_id": request["runtime_instance_id"],
        "public_jwk_thumbprint": runtime_client._jwk_thumbprint(request["public_jwk"]),
        "issue_idempotency_key": request["issue_idempotency_key"],
        "fence": int(request.get("runtime_fence") or 1),
        "issued_at": "2026-09-04T00:00:00Z",
        "expires_at": LEASE_EXPIRES_AT,
        "next_request_token": _runtime_token(next_sequence),
        "next_request_sequence": next_sequence,
    }


def _source_file_receipt(metadata: dict[str, Any]) -> dict[str, Any]:
    row_count = int(metadata.get("row_count") or 0)
    payload: dict[str, Any] = {
        "request_id": f"request-{metadata.get('client_batch_id')}",
        "client_batch_id": metadata.get("client_batch_id"),
        "server_source_file_id": (
            f"{metadata['source_host_id']}/{metadata['producer_role']}/"
            f"{metadata['stream_name']}/{metadata['relative_path']}"
        ),
        "committed": True,
        "status": "accepted",
        "projection_disposition": "COMPLETE",
        "retryable": False,
        "next_retry_after": None,
        "totals": {
            "inserted": row_count,
            "replayed": 0,
            "quarantined": 0,
            "errors": 0,
        },
        "producer_install_id": metadata.get("producer_install_id"),
    }
    if "runtime_request_sequence" in metadata:
        next_sequence = int(metadata["runtime_request_sequence"]) + 1
        payload["runtime_lease"] = {
            "contract_version": runtime_client.CONTRACT_VERSION,
            "validation_status": "consumed",
            "lease_id": "lease-label-real-http",
            "fence": metadata["runtime_fence"],
            "next_request_token": _runtime_token(next_sequence),
            "next_request_sequence": next_sequence,
            "expires_at": LEASE_EXPIRES_AT,
        }
    return payload


def _parse_multipart(headers: Any, body: bytes) -> tuple[dict[str, Any] | None, bytes, str]:
    content_type = headers.get("Content-Type") or headers.get("content-type") or ""
    envelope = (
        f"MIME-Version: 1.0\r\nContent-Type: {content_type}\r\n\r\n".encode("ascii")
        + body
    )
    parsed = BytesParser(policy=policy.default).parsebytes(envelope)
    metadata = None
    file_bytes = b""
    filename = ""
    if not parsed.is_multipart():
        return metadata, file_bytes, filename
    for part in parsed.iter_parts():
        name = part.get_param("name", header="content-disposition")
        payload = part.get_payload(decode=True) or b""
        if name == "metadata":
            metadata = json.loads(payload.decode("utf-8"))
        elif name == "file":
            file_bytes = payload if isinstance(payload, (bytes, bytearray)) else b""
            filename = str(
                part.get_param("filename", header="content-disposition") or ""
            )
    return metadata, bytes(file_bytes), filename


class _LoopbackServer(ThreadingHTTPServer):
    def __init__(self, server_address, request_handler_class):
        super().__init__(server_address, request_handler_class)
        self.lock = threading.Lock()
        self.lease_requests: list[dict[str, Any]] = []
        self.source_file_requests: list[dict[str, Any]] = []
        self.package_requests: list[dict[str, Any]] = []
        self.capability_requests: list[dict[str, Any]] = []


class _LoopbackHandler(BaseHTTPRequestHandler):
    server: _LoopbackServer

    def log_message(self, format, *args) -> None:  # noqa: A003 - stdlib signature
        return

    def _read_body(self) -> bytes:
        length = int(self.headers.get("Content-Length") or 0)
        return self.rfile.read(length) if length else b""

    def _json(self, status: int, payload: dict[str, Any]) -> None:
        raw = json.dumps(payload, ensure_ascii=False, sort_keys=True).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(raw)))
        self.send_header("Connection", "close")
        self.end_headers()
        self.wfile.write(raw)

    def do_GET(self) -> None:  # noqa: N802 - stdlib
        path = urlsplit(self.path).path
        if path == CAPABILITIES_PATH:
            with self.server.lock:
                self.server.capability_requests.append(
                    {"headers": dict(self.headers), "path": path}
                )
            self._json(
                200,
                {
                    "ok": True,
                    "data": {
                        "program": "Label_Match",
                        "package_create": True,
                    },
                },
            )
            return
        self._json(404, {"ok": False, "error": {"code": "NOT_FOUND"}})

    def do_POST(self) -> None:  # noqa: N802 - stdlib
        path = urlsplit(self.path).path
        body = self._read_body()
        if path == LEASE_PATH:
            request = json.loads(body.decode("utf-8"))
            grant = _lease_grant(request)
            grant["producer_install_id"] = "label-real-http-install"
            with self.server.lock:
                self.server.lease_requests.append(request)
            self._json(200, grant)
            return
        if path == INGEST_PATH:
            metadata, file_bytes, filename = _parse_multipart(self.headers, body)
            assert metadata is not None
            with self.server.lock:
                self.server.source_file_requests.append(
                    {
                        "headers": dict(self.headers),
                        "metadata": metadata,
                        "file_bytes": file_bytes,
                        "filename": filename,
                    }
                )
            self._json(200, _source_file_receipt(metadata))
            return
        if path == PACKAGE_PATH:
            with self.server.lock:
                self.server.package_requests.append(
                    {
                        "headers": dict(self.headers),
                        "body": body,
                    }
                )
            self._json(
                200,
                {
                    "ok": True,
                    "data": {
                        "receipt_id": "receipt-label-real-http",
                        "status": "COMMITTED",
                        "command_type": "CREATE_PACKAGE",
                    },
                },
            )
            return
        self._json(404, {"ok": False, "error": {"code": "NOT_FOUND"}})


@contextmanager
def _loopback_https_server(cert_path: Path, key_path: Path) -> Iterator[_LoopbackServer]:
    httpd = _LoopbackServer(("127.0.0.1", 0), _LoopbackHandler)
    context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    context.load_cert_chain(certfile=str(cert_path), keyfile=str(key_path))
    httpd.socket = context.wrap_socket(httpd.socket, server_side=True)
    thread = threading.Thread(target=httpd.serve_forever, daemon=True)
    thread.start()
    try:
        yield httpd
    finally:
        httpd.shutdown()
        httpd.server_close()
        thread.join(timeout=5)


def _endpoint_url(port: int) -> str:
    return f"https://{LOOPBACK_HOST}:{port}{INGEST_PATH}"


def _package_base_url(port: int) -> str:
    return f"https://{LOOPBACK_HOST}:{port}"


def _relay_command(
    *,
    direct_sync_root: Path,
    scan_dir: Path,
    ca_path: Path,
    worker_id: str,
) -> list[str]:
    return [
        sys.executable,
        "-B",
        str(ROOT / "Label_Match.py"),
        user_relay.DIRECT_SYNC_RELAY_MODE,
        "--db-path",
        str(direct_sync_root / "queue" / "direct_sync_relay.sqlite3"),
        "--spool-dir",
        str(direct_sync_root / "spool"),
        "--producer-manifest-path",
        str(direct_sync_root / "producer_manifest.json"),
        "--credential-path",
        str(direct_sync_root / "credential.json"),
        "--upload-status-dir",
        str(direct_sync_root / "upload_status"),
        "--runtime-status-path",
        str(direct_sync_root / "status" / "direct_sync_relay_status.json"),
        "--log-path",
        str(direct_sync_root / "logs" / "direct_sync_relay.jsonl"),
        "--operator-pause-path",
        str(direct_sync_root / "control" / "pause.json"),
        "--scan-source-dir",
        str(scan_dir),
        "--source-glob",
        user_relay.LABEL_MATCH_SOURCE_GLOB,
        "--worker-id",
        worker_id,
        "--timeout-seconds",
        "15",
        "--min-free-bytes",
        "0",
        "--tls-ca-bundle-path",
        str(ca_path),
    ]


def _prepare_direct_sync_root(
    root: Path, *, endpoint_url: str, scan_dir: Path
) -> Path:
    csv_path = _write_packaging_csv(scan_dir)
    _write_manifest(root / "producer_manifest.json")
    _write_credential(root / "credential.json", endpoint_url=endpoint_url)
    for name in ("queue", "spool", "upload_status", "status", "logs", "control"):
        (root / name).mkdir(parents=True, exist_ok=True)
    assert csv_path.is_file()
    return csv_path


def _assert_ingest_received_barcode(server: _LoopbackServer, barcode: str) -> dict[str, Any]:
    assert server.lease_requests, (
        "producer runtime-lease HTTP was not received on the loopback stub"
    )
    assert server.source_file_requests, (
        "producer ingest HTTP was not received on the loopback stub; "
        "drain/upload is a no-op from this test's point of view"
    )
    posted = server.source_file_requests[0]
    file_bytes = posted["file_bytes"]
    assert barcode.encode("utf-8") in file_bytes
    metadata = posted["metadata"]
    assert metadata["stream_name"] == "label_match_events"
    assert metadata["source_system"] == "label_match"
    assert str(posted["headers"].get("X-Producer-Signature") or "").strip()
    return posted


def test_hosted_relay_child_posts_packaging_csv_to_loopback_https(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    roots = _isolate_writer_and_user_roots(tmp_path, monkeypatch)
    _install_loopback_host_alias(monkeypatch, LOOPBACK_HOST)
    tls_dir = tmp_path / "tls"
    ca_path, cert_path, key_path = _write_tls_material(tls_dir, LOOPBACK_HOST)
    direct_sync_root = tmp_path / "direct-sync"
    scan_dir = tmp_path / "label-data"
    with _loopback_https_server(cert_path, key_path) as server:
        port = int(server.server_address[1])
        _prepare_direct_sync_root(
            direct_sync_root,
            endpoint_url=_endpoint_url(port),
            scan_dir=scan_dir,
        )
        completed = subprocess.run(
            _relay_command(
                direct_sync_root=direct_sync_root,
                scan_dir=scan_dir,
                ca_path=ca_path,
                worker_id="direct-sync-relay-label-match-real-http-child",
            ),
            cwd=ROOT,
            env=_child_environment(roots["control_root"], roots["local_app_data"]),
            capture_output=True,
            text=True,
            timeout=45,
            check=False,
        )

    assert completed.returncode == 0, completed.stderr or completed.stdout
    assert "direct_sync_relay_status=acked" in completed.stdout
    posted = _assert_ingest_received_barcode(server, BARCODE)
    db_path = direct_sync_root / "queue" / "direct_sync_relay.sqlite3"
    assert relay_queue_status(db_path)["counts"][RELAY_STATUS_ACKED] == 1
    assert posted["metadata"]["row_count"] == 1


def test_parent_writer_admission_blocks_the_same_real_relay_child(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    roots = _isolate_writer_and_user_roots(tmp_path, monkeypatch)
    _install_loopback_host_alias(monkeypatch, LOOPBACK_HOST)
    tls_dir = tmp_path / "tls"
    ca_path, cert_path, key_path = _write_tls_material(tls_dir, LOOPBACK_HOST)
    direct_sync_root = tmp_path / "direct-sync"
    scan_dir = tmp_path / "label-data"
    with _loopback_https_server(cert_path, key_path) as server:
        port = int(server.server_address[1])
        _prepare_direct_sync_root(
            direct_sync_root,
            endpoint_url=_endpoint_url(port),
            scan_dir=scan_dir,
        )
        command = _relay_command(
            direct_sync_root=direct_sync_root,
            scan_dir=scan_dir,
            ca_path=ca_path,
            worker_id="direct-sync-relay-label-match-real-http-blocked",
        )
        with fence.writer_admission("relay_child_launch"):
            blocked = subprocess.run(
                command,
                cwd=ROOT,
                env=_child_environment(roots["control_root"], roots["local_app_data"]),
                capture_output=True,
                text=True,
                timeout=20,
                check=False,
            )

    assert blocked.returncode != 0, blocked.stdout
    assert not server.source_file_requests
    status_path = direct_sync_root / "status" / "direct_sync_relay_status.json"
    if status_path.is_file():
        status = json.loads(status_path.read_text(encoding="utf-8"))
        assert status.get("error_code") == "hosted_relay_unhandled_exception"
        assert "WriterFencedError" in str(status.get("error_message") or "")


def test_run_session_direct_sync_once_posts_packaging_csv_to_loopback_https(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    roots = _isolate_writer_and_user_roots(tmp_path, monkeypatch)
    _install_loopback_host_alias(monkeypatch, LOOPBACK_HOST)
    tls_dir = tmp_path / "tls"
    ca_path, cert_path, key_path = _write_tls_material(tls_dir, LOOPBACK_HOST)
    direct_sync_root = tmp_path / "direct-sync"
    scan_dir = tmp_path / "label-data"
    with _loopback_https_server(cert_path, key_path) as server:
        port = int(server.server_address[1])
        _prepare_direct_sync_root(
            direct_sync_root,
            endpoint_url=_endpoint_url(port),
            scan_dir=scan_dir,
        )
        result = user_relay.run_session_direct_sync_once(
            app_root=ROOT,
            direct_sync_root=direct_sync_root,
            scan_source_dir=scan_dir,
            reason="REAL_CHILD_HTTP_REGRESSION",
            timeout_seconds=45,
            tls_ca_bundle_path=ca_path,
            worker_id="direct-sync-relay-label-match-real-http-session",
        )
        status_path = (
            direct_sync_root / "status" / "direct_sync_relay_status.json"
        )
        runtime_status = (
            json.loads(status_path.read_text(encoding="utf-8"))
            if status_path.is_file()
            else None
        )

    assert result["status"] == "PASS", {
        **result,
        "runtime_status": runtime_status,
        "ingest_posts": len(server.source_file_requests),
        "lease_posts": len(server.lease_requests),
    }
    assert result["returncode"] == 0
    assert "direct_sync_relay_status=acked" in result["stdout_tail"]
    _assert_ingest_received_barcode(server, BARCODE)
    assert roots["control_root"].is_dir()


def test_drain_one_relay_batch_uses_real_requests_session_against_loopback_https(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _isolate_writer_and_user_roots(tmp_path, monkeypatch)
    _install_loopback_host_alias(monkeypatch, LOOPBACK_HOST)
    tls_dir = tmp_path / "tls"
    ca_path, cert_path, key_path = _write_tls_material(tls_dir, LOOPBACK_HOST)
    csv_path = _write_packaging_csv(tmp_path / "csv")
    manifest = _write_manifest(tmp_path / "producer_manifest.json")
    db_path = tmp_path / "relay.sqlite3"
    with _loopback_https_server(cert_path, key_path) as server:
        port = int(server.server_address[1])
        credentials = ProducerCredentials(
            producer_id="producer-label-real-http",
            key_id="key-label-real-http",
            secret="label-real-http-secret",
            endpoint_url=_endpoint_url(port),
            runtime_lease_mode="enforce",
        )
        enqueue_source_file_for_relay(
            db_path=db_path,
            spool_dir=tmp_path / "spool",
            source_file_path=csv_path,
            producer_manifest_path=tmp_path / "producer_manifest.json",
            credentials=credentials,
        )
        result = drain_one_relay_batch(
            db_path=db_path,
            credentials=credentials,
            worker_id="direct-sync-relay-label-match-real-http-drain",
            status_dir=tmp_path / "status",
            timeout=15,
            tls_ca_bundle_path=str(ca_path),
        )

    assert result is not None
    assert result.success is True, result.error_message
    assert result.committed is True
    _assert_ingest_received_barcode(server, BARCODE)
    assert relay_queue_status(db_path)["counts"][RELAY_STATUS_ACKED] == 1
    plan = build_source_file_plan(
        source_file_path=csv_path,
        producer_manifest_path=tmp_path / "producer_manifest.json",
        credentials=credentials,
    )
    assert plan.metadata["source_host_id"] == manifest["pc_identity"]["source_host_id"]
    assert plan.metadata["producer_install_id"] == (
        manifest["pc_identity"]["producer_install_id"]
    )
    assert plan.metadata["stream_name"] == "label_match_events"


def test_package_logistics_urlopen_posts_and_gets_loopback_https(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _isolate_writer_and_user_roots(tmp_path, monkeypatch)
    _install_loopback_host_alias(monkeypatch, LOOPBACK_HOST)
    tls_dir = tmp_path / "tls"
    ca_path, cert_path, key_path = _write_tls_material(tls_dir, LOOPBACK_HOST)
    body = json.dumps(
        {
            "command_type": "CREATE_PACKAGE",
            "idempotency_key": "label-real-http-package",
        },
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")
    with _loopback_https_server(cert_path, key_path) as server:
        port = int(server.server_address[1])
        posted = package_module._default_transport(
            "POST",
            _package_base_url(port) + PACKAGE_PATH,
            {
                "Accept": "application/json",
                "Content-Type": "application/json; charset=utf-8",
                "Authorization": "Bearer label-real-http-token",
                "X-KMTech-Client": "Label_Match",
            },
            body,
            5.0,
            tls_ca_bundle_path=str(ca_path),
        )
        client = PackageLogisticsClient(
            PackageClientConfig(
                base_url=_package_base_url(port),
                token="label-real-http-token",
                authority_scope_id="SCOPE-LABEL-REAL-HTTP",
                source_host_id="label-real-http-host",
                device_id="label-real-http-device",
                timeout_seconds=5.0,
                authoritative_required=False,
                tls_ca_bundle_path=str(ca_path),
            )
        )
        capabilities = client.get_capabilities()

    assert posted["ok"] is True
    assert posted["data"]["command_type"] == "CREATE_PACKAGE"
    assert server.package_requests, (
        "package logistics urlopen POST was not received on the loopback stub"
    )
    assert server.package_requests[0]["body"] == body
    assert (
        server.package_requests[0]["headers"].get("Authorization")
        == "Bearer label-real-http-token"
    )
    assert capabilities["package_create"] is True
    assert server.capability_requests, (
        "package logistics urlopen GET was not received on the loopback stub"
    )


@pytest.mark.skipif(os.name != "nt", reason="Windows DETACHED_PROCESS user-relay launch")
def test_start_user_relay_process_keeps_real_child_alive_long_enough_to_post(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    roots = _isolate_writer_and_user_roots(tmp_path, monkeypatch)
    _install_loopback_host_alias(monkeypatch, LOOPBACK_HOST)
    local_app_data = roots["local_app_data"]
    paths = resolve_current_user_onboarding_paths(ROOT)
    profile_dir = paths.logistics_profile_path.parent
    tls_dir = profile_dir / "tls"
    ca_path, cert_path, key_path = _write_tls_material(tls_dir, LOOPBACK_HOST)
    process_id = 0
    with _loopback_https_server(cert_path, key_path) as server:
        port = int(server.server_address[1])
        _prepare_direct_sync_root(
            paths.direct_sync_root,
            endpoint_url=_endpoint_url(port),
            scan_dir=paths.data_root,
        )
        secret_path = profile_dir / "secrets" / "bearer-token.dpapi"
        secret_path.parent.mkdir(parents=True, exist_ok=True)
        secret_path.write_bytes(protect_current_user_secret("label-real-http-token"))
        _write_json(
            paths.logistics_profile_path,
            {
                "contract_version": "km-logistics-runtime-profile-v1",
                "base_url": f"https://{LOOPBACK_HOST}:{port}",
                "authority_scope": "SCOPE-LABEL-REAL-HTTP",
                "authority_epoch": 1,
                "authority_plane": "AUTHORITATIVE",
                "ledger_plane": "AUTHORITATIVE",
                "plane_epoch": 1,
                "device_id": "label-real-http-device",
                "source_host_id": "label-real-http-host",
                "bearer_token_ref": "dpapi:secrets/bearer-token.dpapi",
                "timeout_seconds": 8,
                "tls_ca_bundle_path": str(ca_path),
            },
        )
        try:
            report = user_relay.start_user_relay_process(
                ROOT,
                survival_seconds=4.0,
            )
            process_id = int(report["process_id"])
            assert report["status"] == "ALIVE"
            assert process_id > 0
            waited = 0.0
            while waited < 20.0 and not server.source_file_requests:
                threading.Event().wait(0.25)
                waited += 0.25
            _assert_ingest_received_barcode(server, BARCODE)
        finally:
            if process_id:
                subprocess.run(
                    ["taskkill", "/PID", str(process_id), "/T", "/F"],
                    check=False,
                    capture_output=True,
                    text=True,
                    timeout=15,
                )
    assert local_app_data.is_dir()


def test_upload_source_file_real_session_fails_closed_when_loopback_is_down(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _isolate_writer_and_user_roots(tmp_path, monkeypatch)
    _install_loopback_host_alias(monkeypatch, LOOPBACK_HOST)
    tls_dir = tmp_path / "tls"
    ca_path, cert_path, key_path = _write_tls_material(tls_dir, LOOPBACK_HOST)
    csv_path = _write_packaging_csv(tmp_path / "csv")
    _write_manifest(tmp_path / "producer_manifest.json")
    with _loopback_https_server(cert_path, key_path) as server:
        port = int(server.server_address[1])
    credentials = ProducerCredentials(
        producer_id="producer-label-real-http",
        key_id="key-label-real-http",
        secret="label-real-http-secret",
        endpoint_url=_endpoint_url(port),
        runtime_lease_mode="observe",
    )
    plan = build_source_file_plan(
        source_file_path=csv_path,
        producer_manifest_path=tmp_path / "producer_manifest.json",
        credentials=credentials,
    )
    result = upload_source_file(
        plan,
        credentials,
        timeout=2,
        tls_ca_bundle_path=str(ca_path),
    )
    assert result.success is False
    assert result.retryable is True
    assert result.error_code == "transport_error"
    assert not server.source_file_requests
