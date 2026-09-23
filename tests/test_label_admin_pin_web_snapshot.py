"""The committed Web admin-pin.v1 server accepts Label's signed client."""

import importlib.util
import os
from pathlib import Path
from types import SimpleNamespace
import uuid

import pytest

from label_admin_pin import AdminPinClient, operator_local_id


def test_web_383e6c4_in_process(monkeypatch, tmp_path):
    snapshot = os.environ.get("LABEL_ADMIN_PIN_WEB_SNAPSHOT", "")
    if not snapshot:
        pytest.skip("set LABEL_ADMIN_PIN_WEB_SNAPSHOT to the git archive of Web 383e6c4")
    root = Path(snapshot)
    assert (root / "admin_pin.py").is_file()
    monkeypatch.syspath_prepend(str(root))
    spec = importlib.util.spec_from_file_location(
        "web_383e6c4_admin_pin_tests", root / "tests" / "test_admin_pin.py"
    )
    web_tests = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(web_tests)
    app, principal = web_tests._http_app(monkeypatch, tmp_path)
    store = app.extensions["admin_pin_store"]
    store.upsert_link(
        admin_id="A1", principal_id=principal["principal_id"],
        app_scopes=["LABEL"], actor_id="test", reason="isolated Label scope",
    )
    store.set_pin(principal["principal_id"], "582714")
    monkeypatch.setenv("PRODUCER_INGEST_TEST_STREAM", "label_match_events")
    flask_client = app.test_client()

    def transport(path, body, headers):
        response = flask_client.post(
            path, data=body, headers=headers, content_type="application/json",
            base_url="https://localhost",
        )
        return response.status_code, response.data

    credentials = SimpleNamespace(
        producer_id="test-producer", key_id="test-key",
        secret="machine-secret-A8r9P2", endpoint_url="https://localhost/api/producer-ingest",
    )
    client = AdminPinClient(credentials, transport=transport)
    target = {"kind": "label_f5_recovery", "id": "journal-1", "state_fingerprint": "a" * 64}
    operator = {"local_id": operator_local_id("홍길동"), "login_epoch": uuid.uuid4().hex}
    issued = client.verify(
        request_id=str(uuid.uuid4()), admin_id="A1", pin="582714",
        action="LABEL.F5_HOLD", target=target, operator=operator,
    )
    key = "label-pin-operation-1"
    client.redeem(
        verification_id=issued["verification_id"], proof=issued["proof"],
        operation_key=key, action="LABEL.F5_HOLD", target=target, operator=operator,
    )
    assert client.status(issued["verification_id"], key) == "CONSUMED"
    import admin_pin as web_pin
    for name in ("worker-12", "홍길동", "가" * 63):
        assert web_pin._operator({
            "local_id": operator_local_id(name), "login_epoch": "epoch-1",
        })
    assert operator_local_id("가") == operator_local_id("\u1100\u1161")
