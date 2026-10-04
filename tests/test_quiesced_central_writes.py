"""Central writes must reject the active deployment fence before transport."""
import copy
from pathlib import Path
from types import SimpleNamespace

import pytest

import writer_session_fence as fence
from tests.test_writer_session_fence import _payload, _write_active
from tests.test_partial_registration_recovery import recovery, put_residue


@pytest.fixture
def quiesced(monkeypatch, tmp_path):
    root = tmp_path / "control"
    monkeypatch.setenv(fence.TEST_MODE_ENV, "1")
    monkeypatch.setenv(fence.CONTROL_ROOT_OVERRIDE_ENV, str(root))
    payload = _payload(delegated_sources=[])
    payload["status"] = "INSTALLING"
    _write_active(root, payload)
    assert fence.active_fence(root)["status"] == "INSTALLING"
    return root / fence.ACTIVE_FILENAME


def test_quiesced_runtime_authority_sends_nothing_and_keeps_pending_key(monkeypatch, tmp_path, quiesced):
    from tests.test_producer_runtime_client import _credentials, _LeaseSession
    import producer_runtime_client as client

    session = _LeaseSession()
    arguments = dict(db_path=tmp_path / "relay.sqlite3", credentials=_credentials(),
                     producer_install_id="install-test", session=session, now="2026-08-06T00:00:00Z")
    with pytest.raises(fence.WriterFencedError):
        client.ensure_runtime_authority(**arguments)
    assert session.calls == []
    # The pending lease request stays in the real SQLite state for retry.
    import sqlite3
    with sqlite3.connect(arguments["db_path"]) as connection:
        before = connection.execute("SELECT pending_request_json FROM direct_sync_runtime_authority").fetchone()[0]
    quiesced.unlink()
    assert client.ensure_runtime_authority(**arguments).error_code == ""
    assert len(session.calls) == 1
    import json
    assert session.calls[0][1] == json.loads(before)


def test_quiesced_registration_http_is_denied_before_transport(monkeypatch, tmp_path, quiesced):
    from enrollment_mutex import EnrollmentMutex
    from tools import register_label_match_worker_pc as module

    calls = []
    response = SimpleNamespace(status_code=200, json=lambda: {"ok": True})
    monkeypatch.setattr(module.requests, "post", lambda *a, **k: calls.append((a, k)) or response)
    payload = {"producer_id": "test-producer"}
    original = copy.deepcopy(payload)
    with EnrollmentMutex(1):
        with pytest.raises(fence.WriterFencedError):
            module._enroll(payload, enrollment_url="https://example.invalid/enroll",
                           enrollment_token="", timeout_seconds=1)
    assert calls == []
    assert payload == original
    quiesced.unlink()
    with EnrollmentMutex(1):
        assert module._enroll(payload, enrollment_url="https://example.invalid/enroll",
                              enrollment_token="", timeout_seconds=1) == {"ok": True}
    assert len(calls) == 1
    assert calls[0][1]["json"] == original


@pytest.mark.parametrize("unknown_commit", [False, True])
def test_quiesced_public_registration_recovery_preserves_residue_and_unknown(
    recovery, quiesced, unknown_commit, isolated_dpapi
):
    r = recovery
    isolated_dpapi(r.m)
    path, original = put_residue(r, "identity")
    receipt = quiesced.read_bytes()
    if unknown_commit:
        quiesced.unlink()
        r.state["fault"] = "commit"
        with pytest.raises(r.m.requests.Timeout):
            r.m.apply_registration(r.args, r.manifest, r.credential, r.report)
        quiesced.write_bytes(receipt)
    calls = list(r.state["calls"])
    originals = {p: p.read_bytes() for p in Path(r.args.data_dir).rglob("*") if p.is_file()}
    with pytest.raises(fence.WriterFencedError):
        r.m.apply_registration(r.args, r.manifest, r.credential, r.report)
    assert r.state["calls"] == calls
    assert all(p.read_bytes() == content for p, content in originals.items())
    if not unknown_commit:
        assert path.read_bytes() == original
    quiesced.unlink()
    result = r.m.apply_registration(r.args, r.manifest, r.credential, r.report)
    assert result["status"] == "ADMIN_RECOVERY_REGISTERED"
    assert r.state["commits"] == 1


@pytest.mark.parametrize("action", ["prepare", "status", "commit"])
def test_quiesced_partial_recovery_transport_is_fenced(quiesced, action):
    from enrollment_mutex import EnrollmentMutex
    from tools import register_label_match_worker_pc as module

    calls = []
    value = {"contract_version": f"producer-admin-recovery-{action}-response-v1"}
    session = SimpleNamespace(post=lambda *a, **kw: calls.append((a, kw)) or
                              SimpleNamespace(status_code=200, json=lambda: value))
    signer = SimpleNamespace(sign_es256=lambda value: b"s" * 64)
    arguments = (session, "https://example.invalid/recover", action, {"producer_id": "test"}, signer, "", 1)
    with EnrollmentMutex(1), pytest.raises(fence.WriterFencedError):
        module._partial_recovery_request(*arguments)
    assert calls == []
    quiesced.unlink()
    with EnrollmentMutex(1):
        assert module._partial_recovery_request(*arguments) == value
    assert len(calls) == 1


@pytest.mark.parametrize("method", [
    "issue_operation_lease", "replace_and_reseal_transfer", "adopt_phs_label",
    "prepare_phs_label_exchange", "prepare_phs_reconciliation_label_exchange",
    "request_phs_label_print", "complete_phs_label_print",
    "activate_phs_label_exchange", "create_package", "cancel_package",
])
def test_quiesced_business_and_print_commands_deny_before_body(quiesced, method):
    from package_logistics import PackageClientConfig, PackageLogisticsClient

    calls = []
    client = PackageLogisticsClient(
        PackageClientConfig("https://example.invalid", "synthetic-config", "scope", "host", "device"),
        transport=lambda *args: calls.append(args),
    )
    # Admission precedes even argument validation, therefore no command/PIN
    # preparation or transport can run while this owner fence is active.
    with pytest.raises(fence.WriterFencedError):
        getattr(client, method)()
    assert calls == []


@pytest.mark.parametrize("route", ["verifications", "redemptions", "status"])
def test_quiesced_pin_preserves_wire_body_and_retries(quiesced, route):
    from tests.test_producer_runtime_client import _credentials
    from label_admin_pin import AdminPinClient
    import json

    calls = []
    client = AdminPinClient(_credentials(), transport=lambda *args: calls.append(args) or
                            (200, b'{"ok":true}'))
    body = {"request_id": "same-request", "action": "test"}
    before = copy.deepcopy(body)
    with pytest.raises(fence.WriterFencedError):
        client.post(route, body)
    assert calls == []
    assert body == before
    quiesced.unlink()
    assert client.post(route, body) == {"ok": True}
    assert json.loads(calls[0][1]) == before
