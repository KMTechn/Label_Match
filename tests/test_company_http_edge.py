"""Company edge responses through real codecs and offline transport doubles."""
import gzip
import io
import json
from types import SimpleNamespace
import urllib.error
import zlib

import pytest
import requests
from requests.adapters import HTTPAdapter
from urllib3.response import HTTPResponse

import label_admin_pin as pin
import package_logistics as packages
import producer_runtime_client as runtime
from tests.test_item_catalog_sync import CATALOG, _no_logistics_runtime_profile
from tests.test_auto_update_publish_download import fake_feed
from tests.test_register_label_match_worker_pc import load_registration_module


URL = "https://example.invalid/api/producer-ingest"
HTML = b"<html>Cloudflare: error code: 1010</html>"


def encoded(payload, encoding):
    if encoding == "gzip":
        return gzip.compress(payload)
    if encoding == "deflate":
        return zlib.compress(payload)
    if encoding == "raw-deflate":
        return zlib.compress(payload)[2:-4]
    return payload


class UrllibResponse(io.BytesIO):
    def __init__(self, payload, *, status=200, encoding="", url=URL):
        super().__init__(encoded(payload, encoding))
        self.status, self.url = status, url
        self.headers = {"Content-Encoding": "deflate" if encoding == "raw-deflate" else encoding}

    def geturl(self):
        return self.url


def requests_response(payload, *, status=200, encoding="", url=URL):
    request = requests.Request("GET", url).prepare()
    raw = HTTPResponse(body=io.BytesIO(encoded(payload, encoding)), status=status,
                       headers={"Content-Encoding": encoding} if encoding else {},
                       preload_content=False, decode_content=False)
    return HTTPAdapter().build_response(request, raw)


def pin_client(response):
    from tests.test_direct_sync_push import make_credentials
    client = pin.AdminPinClient(make_credentials(), transport=lambda *args: (200, b"{}"))
    client.transport = None
    response.url = client.origin + "/api/admin-pin/v1/redemptions"
    def open_response(*args, **kwargs):
        if response.status >= 400:
            raise urllib.error.HTTPError(response.url, response.status, "edge refusal", response.headers, response)
        return response
    client.opener = SimpleNamespace(open=open_response)
    return client


@pytest.mark.parametrize("encoding", ["gzip", "deflate"])
def test_pin_accepts_exact_decoded_limit_and_larger_roster(encoding):
    raw = b'{"ok":true}'
    client = pin_client(UrllibResponse(raw.ljust(8192), encoding=encoding))
    assert client.post("redemptions", {}) == {"ok": True}
    response = UrllibResponse(raw.ljust(262144), encoding=encoding)
    client = pin_client(response)
    response.url = client.origin + "/api/worker-roster/v1/query"
    assert client.query_roster("scope", None) == {"ok": True}


@pytest.mark.parametrize("encoding", ["gzip", "deflate"])
def test_logistics_rejects_compression_bomb_before_json(monkeypatch, encoding):
    response = UrllibResponse(b" " * (8 * 1024 * 1024 + 1), encoding=encoding)
    monkeypatch.setattr(packages, "urlopen", lambda *args, **kwargs: response)
    with pytest.raises(packages.PackageTransportError, match="oversized"):
        packages._default_transport("GET", URL, {}, None, 8)


@pytest.mark.parametrize("encoding", ["gzip", "deflate"])
def test_truncated_compression_never_becomes_success(encoding, monkeypatch):
    response = UrllibResponse(b'{"ok":true}', encoding=encoding)
    damaged = response.getvalue()[:-3]
    response = UrllibResponse(damaged)
    response.headers["Content-Encoding"] = encoding
    client = pin_client(response)
    with pytest.raises(pin.AdminPinError) as caught:
        client.post("redemptions", {})
    assert caught.value.unknown
    response = UrllibResponse(damaged)
    response.headers["Content-Encoding"] = encoding
    monkeypatch.setattr(packages, "urlopen", lambda *args, **kwargs: response)
    with pytest.raises(packages.PackageTransportError):
        packages._default_transport("GET", URL, {}, None, 8)


def test_pin_compressed_json_refusal_preserves_contract_code():
    raw = b'{"ok":false,"error":{"code":"PIN_INVALID"}}'
    client = pin_client(UrllibResponse(raw, status=403, encoding="gzip"))
    with pytest.raises(pin.AdminPinError) as caught:
        client.post("redemptions", {})
    assert caught.value.code == "PIN_INVALID" and not caught.value.unknown
    assert caught.value.http_status == 403
    # operations.md company HTTP: retain refusal and per-item hold guidance.
    # operations.md:110; hold suffix introduced by 95a0fe8 (HTTP cause: 2796fca).
    assert pin.operator_message(caught.value.code, http_status=caught.value.http_status) == (
        pin.FAILURE_MESSAGE + ' 이 건만 보류하고 다른 작업을 계속하세요.'
    )


def test_pin_html_after_redemption_recovers_same_key_receipt():
    import uuid
    from tests.test_direct_sync_push import make_credentials
    verification, operation = str(uuid.uuid4()), str(uuid.uuid4())
    calls = []
    def transport(path, body, headers):
        calls.append((path, json.loads(body)))
        if path.endswith("/redemptions"):
            return 503, HTML
        return 200, json.dumps({"ok": True, "verification_id": verification,
            "operation_key": operation, "status": "CONSUMED",
            "redemption_id": str(uuid.uuid4()), "consumed_at": 1}).encode()
    client = pin.AdminPinClient(make_credentials(), transport=transport)
    client.redeem(verification_id=verification, proof="", operation_key=operation,
                  action="LABEL.RECHECK", target={}, operator={})
    assert len(calls) == 2 and calls[1][0].endswith("/status")
    assert {call[1]["operation_key"] for call in calls} == {operation}


@pytest.mark.parametrize("encoding", ["", "gzip", "deflate", "raw-deflate"])
def test_pin_reads_encoded_json_and_keeps_decoded_size_limit(encoding):
    answer = {"ok": True, "message": "정상"}
    raw = json.dumps(answer, ensure_ascii=False).encode()
    client = pin_client(UrllibResponse(raw, encoding=encoding))
    assert client.post("redemptions", {}) == answer
    client = pin_client(UrllibResponse(b" " * 9000 + raw, encoding=encoding))
    with pytest.raises(pin.AdminPinError) as caught:
        client.post("redemptions", {})
    assert caught.value.unknown


@pytest.mark.parametrize("status", [403, 502, 503])
def test_pin_html_has_operator_http_cause_and_keeps_post_unknown(status):
    client = pin_client(UrllibResponse(HTML, status=status))
    with pytest.raises(pin.AdminPinError) as caught:
        client.post("redemptions", {})
    assert caught.value.code == "VERIFY_UNAVAILABLE"
    assert caught.value.http_status == status
    assert caught.value.unknown  # An edge response cannot prove POST application.
    message = pin.operator_message(caught.value.code, http_status=caught.value.http_status)
    assert "회사 서버" in message and str(status) in message
    assert "알 수 없" not in message


@pytest.mark.parametrize("status", [403, 503])
def test_pin_html_refusal_records_hold_and_allows_explicit_park(tmp_path, monkeypatch, status):
    import hashlib
    from tests.test_label_admin_pin import _pin_app, SOURCE_A, SOURCE_B

    app, journal, _ = _pin_app(tmp_path, monkeypatch)
    requests_seen = []
    def refuse(request, **kwargs):
        requests_seen.append(request.full_url)
        response = UrllibResponse(HTML, status=status, url=request.full_url)
        raise urllib.error.HTTPError(request.full_url, status, "edge refusal", response.headers, response)
    app._admin_pin_client.transport = None
    app._admin_pin_client.opener = SimpleNamespace(open=refuse)
    digest = hashlib.sha256(journal.path.read_bytes()).hexdigest()
    item = "F5:" + digest

    assert app._run_package_pin_action("LABEL.F5_HOLD", item) is False
    assert len(requests_seen) == 2  # The existing unknown verification retry remains.
    pending = app._admin_pin_store.pending("label_f5_recovery", digest)
    assert pending is not None and pending["state"] == "UNVERIFIED_OPERATOR_HOLD"
    assert pending["error_code"] == "VERIFY_UNAVAILABLE" and pending["verification_id"] == ""
    reason = "회사 서버 거절" if status == 403 else "회사 서버 오류"
    assert f"{reason}({status})" in app._admin_pin_last_message
    assert journal.path.exists() and app.package_outbox.get_label_exchange_hold(digest) is None

    assert app._park_unverified_pin_item(item) is True
    assert len(requests_seen) == 2  # Explicit unverified parking needs no new PIN request.
    assert not journal.path.exists()
    assert app._label_recovery_source_is_held(SOURCE_A) is True
    assert app._label_recovery_source_is_held(SOURCE_B) is False
    assert app._active_label_recovery_state() == {}


@pytest.mark.parametrize("encoding", ["", "gzip", "deflate", "raw-deflate"])
def test_logistics_reads_encoded_json(monkeypatch, encoding):
    raw = json.dumps({"ok": True, "data": {"test": "정상"}}, ensure_ascii=False).encode()
    monkeypatch.setattr(packages, "urlopen", lambda *args, **kwargs: UrllibResponse(raw, encoding=encoding))
    assert packages._default_transport("GET", URL, {}, None, 8)["data"]["test"] == "정상"


@pytest.mark.parametrize("status", [403, 502, 503])
def test_logistics_html_keeps_http_cause_and_no_committed_claim(monkeypatch, status):
    def fail(*args, **kwargs):
        raise urllib.error.HTTPError(URL, status, "edge refusal", {}, io.BytesIO(HTML))
    monkeypatch.setattr(packages, "urlopen", fail)
    with pytest.raises(packages.PackageApiError) as caught:
        packages._default_transport("POST", URL, {}, b"{}", 8)
    assert caught.value.code == f"HTTP_{status}"
    assert "회사 서버" in caught.value.message and str(status) in caught.value.message
    assert caught.value.committed is None


def test_runtime_sends_product_agent_and_requests_decodes_gzip():
    from tests.test_direct_sync_push import make_credentials
    calls = []
    def post(url, **kwargs):
        calls.append(kwargs)
        return requests_response(b'{"ok":true}', encoding="gzip")
    answer, failure = runtime._post_lease_request(request_value={}, credentials=make_credentials(),
        session=SimpleNamespace(post=post), timeout=8)
    assert answer == {"ok": True} and failure is None
    assert calls[0]["headers"]["User-Agent"].startswith("KMTech-")


def test_catalog_sends_product_agent_and_requests_decodes_gzip(tmp_path):
    import item_catalog_sync as catalog
    calls = []
    bundle = tmp_path / "bundle.csv"
    bundle.write_bytes(CATALOG)
    def get(url, **kwargs):
        calls.append(kwargs)
        return requests_response(CATALOG, encoding="gzip")
    result = catalog.refresh_item_catalog(bundle, cache_path=tmp_path / "cache.csv", url=URL, get=get)
    assert result.read_bytes() == CATALOG
    assert calls[0]["headers"]["User-Agent"].startswith("KMTech-")


@pytest.mark.parametrize("status", [200, 403, 503])
def test_enrollment_sends_product_agent_and_keeps_http_classification(monkeypatch, status):
    module = load_registration_module()
    calls = []
    monkeypatch.setattr(module, "require_enrollment_mutex_owned", lambda: None)
    def post(url, **kwargs):
        calls.append(kwargs)
        return requests_response(b'{"ok":true}' if status == 200 else HTML, status=status, encoding="gzip")
    monkeypatch.setattr(module.requests, "post", post)
    if status == 200:
        assert module._enroll({}, enrollment_url=URL, enrollment_token="", timeout_seconds=8) == {"ok": True}
    else:
        with pytest.raises(module.ProducerEnrollmentHTTPError) as caught:
            module._enroll({}, enrollment_url=URL, enrollment_token="", timeout_seconds=8)
        assert caught.value.status_code == status
        assert "회사 서버" in str(caught.value) and str(status) in str(caught.value)
    assert calls[0]["headers"]["User-Agent"].startswith("KMTech-")


@pytest.mark.parametrize("status", [200, 403, 503])
def test_partial_recovery_sends_product_agent_and_classifies_html(monkeypatch, status):
    module = load_registration_module()
    calls = []
    monkeypatch.setattr(module, "require_enrollment_mutex_owned", lambda: None)
    def post(url, **kwargs):
        calls.append(kwargs)
        return requests_response(json.dumps({"contract_version": "producer-admin-recovery-status-response-v1"}).encode()
                                 if status == 200 else HTML, status=status)
    session = SimpleNamespace(post=post)
    signer = SimpleNamespace(sign_es256=lambda body: bytes(64))
    if status == 200:
        module._partial_recovery_request(session, URL, "status", {}, signer, "", 8)
    else:
        with pytest.raises(module.ProducerEnrollmentHTTPError) as caught:
            module._partial_recovery_request(session, URL, "status", {}, signer, "", 8)
        assert caught.value.status_code == status
        assert "회사 서버" in str(caught.value)
    assert calls[0]["headers"]["User-Agent"].startswith("KMTech-")


@pytest.mark.parametrize("status", [200, 403, 503])
def test_feed_sends_product_agent_and_classifies_http(monkeypatch, tmp_path, status):
    import auto_update as updates
    calls = []
    with pytest.MonkeyPatch.context() as patch:
        fake_feed(patch, tmp_path, "normal", [])
        session = requests.Session()
        def get(url, **kwargs):
            calls.append(kwargs)
            return requests_response(b"{}", status=status, encoding="gzip", url=url)
        session.get = get
        patch.setattr(requests, "Session", lambda: session)
        if status == 200:
            assert updates.prepare_feed_packet(tmp_path)["status"] == "NO_UPDATE"
        else:
            with pytest.raises(ValueError) as caught:
                updates.prepare_feed_packet(tmp_path)
            assert "회사 서버" in str(caught.value) and str(status) in str(caught.value)
        assert calls[0]["headers"]["User-Agent"].startswith("KMTech-")


@pytest.mark.parametrize("status", [403, 503])
def test_upload_html_retains_status_message_without_false_ack(tmp_path, status):
    from tests.test_direct_sync_push import make_credentials, make_manifest, write_csv
    import direct_sync_push as push
    _, manifest_path = make_manifest(tmp_path)
    plan = push.build_source_file_plan(source_file_path=write_csv(tmp_path),
                                      producer_manifest_path=manifest_path,
                                      credentials=make_credentials())
    result = push._upload_response_result(plan, requests_response(HTML, status=status),
                                        credentials=make_credentials(), headers={})
    assert not result.success and not result.committed
    assert result.status_code == status
    assert "회사 서버" in result.error_message and str(status) in result.error_message


@pytest.mark.parametrize("status", [403, 503])
def test_read_only_canary_keeps_http_cause_without_reading_body(tmp_path, status):
    from tests.test_direct_sync_push import make_credentials, make_manifest, write_csv
    import direct_sync_push as push
    from tools.label_auth_recovery_canary import probe_authentication
    _, manifest_path = make_manifest(tmp_path)
    credentials = make_credentials()
    plan = push.build_source_file_plan(source_file_path=write_csv(tmp_path),
        producer_manifest_path=manifest_path, credentials=credentials)
    response = requests_response(HTML, status=status)
    calls = []
    def get(url, **kwargs):
        calls.append(kwargs)
        return response
    result = probe_authentication(credentials=credentials, metadata=plan.metadata,
                                  session=SimpleNamespace(get=get))
    assert result.evidence["http_error_code"] == f"HTTP_{status}"
    assert "회사 서버" in result.evidence["http_error_message"]
    assert result.evidence["body_read"] is False
    assert calls[0]["headers"]["User-Agent"].startswith("KMTech-")


@pytest.mark.parametrize("status", [200, 403, 503])
def test_guarded_runtime_keeps_source_binding_and_http_cause(monkeypatch, status):
    from label_guarded_runtime_reconcile import _SourceCommitBoundSession, GuardedRuntimeReconcileError
    calls = []
    commit = "1" * 40
    def post(url, **kwargs):
        calls.append(kwargs)
        return SimpleNamespace(status_code=status, headers={"X-KMTech-Source-Commit": commit} if status == 200 else {})
    monkeypatch.setattr(requests, "Session", lambda: SimpleNamespace(post=post))
    session = _SourceCommitBoundSession(expected_binding={}, expected_source_commit=commit,
        endpoint_validator=lambda: {}, endpoint_validator_kwargs={},
        pre_dispatch_guard=lambda: {"status": "PASS"})
    if status == 200:
        assert session.post(URL).status_code == 200
    else:
        with pytest.raises(GuardedRuntimeReconcileError, match=f"HTTP {status}"):
            session.post(URL)
    assert len(calls) == 1 and calls[0]["headers"]["User-Agent"].startswith("KMTech-")
    with pytest.raises(GuardedRuntimeReconcileError, match="exactly one"):
        session.post(URL)


@pytest.mark.parametrize("status", [403, 503])
def test_guarded_runtime_http_cause_survives_lease_transport(monkeypatch, status):
    from label_guarded_runtime_reconcile import _SourceCommitBoundSession
    from tests.test_direct_sync_push import make_credentials
    monkeypatch.setattr(requests, "Session", lambda: SimpleNamespace(
        post=lambda *args, **kwargs: SimpleNamespace(status_code=status, headers={})))
    session = _SourceCommitBoundSession(expected_binding={}, expected_source_commit="1" * 40,
        endpoint_validator=lambda: {}, endpoint_validator_kwargs={},
        pre_dispatch_guard=lambda: {"status": "PASS"})
    answer, failure = runtime._post_lease_request(request_value={}, credentials=make_credentials(),
                                                session=session, timeout=8)
    assert answer is None and failure.status_code == status
    assert failure.error_code == f"HTTP_{status}"
    assert failure.retryable is True
    assert "회사 서버" in failure.error_message and str(status) in failure.error_message
