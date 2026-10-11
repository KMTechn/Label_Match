"""Packaging Stage B: local durability, signed retries and pinned tray identity."""
import copy
import json
import queue
import threading
import time
import uuid
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import pytest
import Label_Match as lm
import label_worker_roster as worker_roster
from label_admin_pin import AdminPinClient, AdminPinError
from protected_admin import PROTECTED_ADMIN_OPERATOR_ID, PROTECTED_ADMIN_DISPLAY_NAME
from label_worker_roster import (WorkerRoster, CONTRACT_VERSION_V2, STAGE_A,
                                normalize_registration_name, registration_enabled, worker_choice_label)
from tests.test_label_worker_roster import app_with_roster, answer, SCOPE, BOB
from tests.test_w9scanonly_packaging import packaging, products, scan, completions
from tests.test_legacy_label_transition import MASTER

def make_client(monkeypatch, transport):
    credentials = SimpleNamespace(endpoint_url="https://roster-test.example.invalid/api/ingest",
                                  producer_id="synthetic-producer", key_id="synthetic-key",
                                  secret=uuid.uuid4().hex * 2)
    return AdminPinClient(credentials, transport=transport)


def application(packaging, tmp_path):
    app, dialogs = packaging
    app.app_settings_path = str(tmp_path / "settings" / "app_settings.json")
    app.app_settings = {"worker_name": app.worker_name}
    app.package_logistics_client = SimpleNamespace(config=SimpleNamespace(authority_scope_id="site-1"))
    app._init_worker_roster()
    app._request_worker_roster_refresh = lambda: None
    app.open_settings_window = lambda: None
    return app, dialogs


def select_local(app, row):
    app.worker_id, app.worker_name = row["worker_id"], row["display_name"]
    app.app_settings.update(worker_id=app.worker_id, worker_name=app.worker_name)


def finish_refresh(app):
    app._roster_thread.join(5)
    assert not app._roster_thread.is_alive()
    app._poll_worker_roster(schedule=False)


@pytest.mark.parametrize("gate", [False, True])
def test_local_names_are_selectable_offline_but_wire_identity_requires_signal(packaging, tmp_path, gate):
    app, _ = application(packaging, tmp_path)
    row = app._register_worker_name("새 이름")
    select_local(app, row)
    app._worker_identity_enabled = gate
    app._worker_roster_stage_b = gate
    assert app._roster_worker_allowed()
    scan(app, MASTER)
    identity, enabled = app._worker_record_context()
    assert identity == {"worker_id": row["worker_id"], "worker_name_snapshot": "새 이름"}
    assert enabled is gate
    assert worker_choice_label(row) == "새 이름 #임시1"


def test_fsync_failure_keeps_input_unselectable_and_other_names_usable(packaging, tmp_path, monkeypatch):
    app, _ = application(packaging, tmp_path)
    known = worker("웹 이름")
    app._apply_worker_roster_answer(0, "site-1", snapshot(known))
    before = app._worker_roster_choices()
    with monkeypatch.context() as context:
        context.setattr(worker_roster.os, "fsync", lambda fd: (_ for _ in ()).throw(OSError("disk write failed")))
        with pytest.raises(OSError):
            app._register_worker_name("새 이름")
    assert app._worker_roster_choices() == before
    assert app._roster_b.local["sequence"] == 0
    assert not app._roster_b.pending_requests()
    assert app._register_worker_name("새 이름")["local_sequence"] == 1


def test_numbered_picker_saves_only_name_and_identity_and_keeps_admin_separate(packaging, tmp_path):
    app, _ = application(packaging, tmp_path)
    row = app._register_worker_name("새 이름")
    app.scale_factor, app.tree_font_size = 1.0, 14
    app.unique_id = "PC-ROSTER"
    app._has_background_work = lambda: False
    app._block_view_only_action = lambda *a, **k: False
    app._block_duplicate_history_load = lambda *a, **k: False
    app._release_shortage_confirmation = lambda: True
    app._update_save_directory = lambda: None
    app._destroy_modal_and_refocus = lambda *a: None
    app.title = lambda *a: None
    class Tree:
        def __getitem__(self, key): return []
    app.summary_tree = app.history_tree = Tree()
    app._save_settings_and_close(object(), worker_choice_label(row), _protected_admin_login=True)
    assert app.worker_name != "새 이름"
    app._save_settings_and_close(object(), worker_choice_label(row))
    settings = json.loads(Path(app.app_settings_path).read_text(encoding="utf-8"))
    assert app.worker_name == settings["worker_name"] == "새 이름"
    assert settings["worker_id"] == row["worker_id"]
    assert "#" not in settings["worker_history"][0]["name"]


def test_malformed_v2_cache_preserves_original_without_accepting_missing_fields(tmp_path):
    path = tmp_path / "roster.json"
    damaged = json.dumps({**snapshot(worker()), "sha256": "0" * 64}).encode()
    path.write_bytes(damaged)
    assert worker_roster.load_cache(path, "site-1") is None
    preserved, = tmp_path.glob("roster.json.invalid-*")
    assert preserved.read_bytes() == damaged


@pytest.mark.parametrize("fault", ["deep-json", "quarantine-unavailable"])
def test_damaged_registration_never_blocks_web_names_or_recreates_ids(tmp_path, monkeypatch, fault):
    known = worker("웹 이름")
    store = roster(tmp_path, known)
    # Use a separate v1 cache so existing names remain available after an unreadable envelope.
    worker_roster.save_cache(store.path, {**answer(), "authority_scope_id": "site-1"}, "site-1")
    damaged = (b"[" * 2000 + b"0" + b"]" * 2000) if fault == "deep-json" else b"{broken"
    store.local_path.write_bytes(damaged)
    if fault == "quarantine-unavailable":
        monkeypatch.setattr(WorkerRoster, "_preserve_bad", lambda *a: (_ for _ in ()).throw(OSError("cannot quarantine")))
    restarted = WorkerRoster(store.path, "site-1")
    assert restarted.local_blocked and restarted.active_workers()
    with pytest.raises(ValueError):
        restarted.register_local("또 다른 이름")
    assert not restarted.local["workers"] and restarted.local["sequence"] == 0
    preserved = store.local_path if store.local_path.exists() else next(tmp_path.glob("roster.registrations.json.bad-*"))
    assert preserved.read_bytes() == damaged


def test_temporary_disabled_name_cannot_reappear_as_legacy_local_history(packaging, tmp_path):
    app, _ = application(packaging, tmp_path)
    row = app._register_worker_name("새 이름")
    select_local(app, row)
    body, = app._roster_b.pending_requests()
    reply = acknowledgement(body)
    app._roster_b.apply_registration(body, reply)
    app._roster_b.register_local("새 이름", existing_worker_id=reply["worker"]["worker_id"])
    returned, = app._roster_b.pending_requests()
    app._roster_b.apply_registration(returned, error=AdminPinError("WORKER_NOT_FOUND", status=404))
    app._worker_roster = app._roster_b.snapshot
    app._apply_worker_roster_answer(0, "site-1", snapshot(version=3))
    assert not any(choice["display_name"] == "새 이름" for choice in app._worker_roster_choices())
    assert not app._roster_worker_allowed()


@pytest.mark.parametrize("scenario", ["failure", "mapping", "different"])
def test_registration_dialog_preserves_input_and_one_same_person_decision(packaging, tmp_path, monkeypatch, scenario):
    app, _ = application(packaging, tmp_path)
    app._roster_selection_window = object()
    app._refresh_worker_roster_selection = lambda: None
    if scenario != "failure":
        app._register_worker_name("동명")
    variables, widgets = [], []
    class Variable:
        def __init__(self, value=""):
            self.value = value
            variables.append(self)
        def get(self): return self.value
        def set(self, value): self.value = value
    class Widget:
        def __init__(self, *args, **kwargs):
            self.options, self.destroyed, self.value = kwargs, False, ""
            widgets.append(self)
        def get(self): return self.value
        def set(self, value): self.value = value
        def configure(self, **kwargs): self.options.update(kwargs)
        def destroy(self): self.destroyed = True
        def winfo_exists(self): return not self.destroyed
        def __getattr__(self, name): return lambda *a, **k: None
    monkeypatch.setattr(lm.tk, "StringVar", Variable)
    monkeypatch.setattr(lm.tk, "Toplevel", Widget)
    for name in ("Frame", "Label", "Entry", "Combobox", "Button"):
        monkeypatch.setattr(lm.ttk, name, Widget)
    if scenario == "failure":
        app._register_worker_name = lambda *a, **k: (_ for _ in ()).throw(OSError("synthetic save failure"))
    app._open_worker_registration()
    count = len(widgets)
    app._open_worker_registration()
    assert len(widgets) == count
    variables[0].set("새 이름" if scenario == "failure" else "동명")
    button = next(row for row in widgets if row.options.get("text") == "등록")
    button.options["command"]()
    if scenario == "failure":
        assert variables[0].get() == "새 이름" and not widgets[0].destroyed
        assert any("입력은 남아" in row.options.get("text", "") for row in widgets)
    elif scenario == "mapping":
        request, = app._roster_b.pending_requests()
        reply = acknowledgement(request, name="동명_1")
        app._roster_b.apply_registration(request, reply)
        next(row for row in widgets if row.options.get("text") == "같은 사람").options["command"]()
        returned, = app._roster_b.pending_requests()
        assert returned["mode"] == "return" and returned["existing_worker_id"] == reply["worker"]["worker_id"]
        assert len(app._roster_b.local["workers"]) == 1 and widgets[0].destroyed
    else:
        next(row for row in widgets if row.options.get("text") == "다른 사람").options["command"]()
        button.options["command"]()
        assert not widgets[0].destroyed
        variables[0].set("동명B")
        button.options["command"]()
        assert len(app._roster_b.local["workers"]) == 2 and widgets[0].destroyed


@pytest.mark.parametrize("status,code,count", [(400, "INVALID_REQUEST", 2), (409, "TEMP_ID_CONFLICT", 2),
    (500, "INTERNAL_ERROR", 2), (401, "AUTH_REQUIRED", 1), (403, "ACTION_NOT_ALLOWED", 1),
    (429, "RATE_LIMITED", 1), (503, "ROSTER_V2_REGISTER_DISABLED", 1)])
def test_item_errors_continue_and_installation_errors_stop_only_this_round(packaging, tmp_path, monkeypatch, status, code, count):
    app, _ = application(packaging, tmp_path)
    app._register_worker_name("첫 이름")
    app._register_worker_name("다음 이름")
    requests = app._roster_b.pending_requests()
    calls = []
    def transport(path, raw, headers):
        if path.endswith("/query"):
            return 200, json.dumps(snapshot()).encode()
        body = json.loads(raw)
        calls.append(body)
        if len(calls) == 1:
            return status, json.dumps({"ok": False, "error": {"code": code}}).encode()
        return 201, json.dumps(acknowledgement(body)).encode()
    client = make_client(monkeypatch, transport)
    app._package_pin_client = lambda: client
    app.package_logistics_client.get_capabilities = lambda: {"capability_ids": ["worker_roster_v2_stage_b"]}
    app._request_worker_roster_refresh = lm.Label_Match._request_worker_roster_refresh.__get__(app)
    app._request_worker_roster_refresh()
    finish_refresh(app)
    assert len(calls) == count
    assert app._roster_b.local["requests"][0]["body"] == requests[0]
    assert {"첫 이름", "다음 이름"} <= {row["display_name"] for row in app._worker_roster_choices()}
    assert app._worker_roster_stage_b is (status != 503)


def test_gate_off_never_calls_register_and_keeps_web_names_and_local_queue(packaging, tmp_path):
    app, _ = application(packaging, tmp_path)
    app._register_worker_name("이 PC 이름")
    requests = app._roster_b.pending_requests()
    known = worker("웹 이름")
    app.package_logistics_client.get_capabilities = lambda: {"capabilities": {"worker_roster_v2": {"stage_b": False}}}
    app._worker_roster_query = lambda *a: snapshot(known)
    app._package_pin_client = lambda: (_ for _ in ()).throw(AssertionError("off gate requested registration client"))
    app._request_worker_roster_refresh = lm.Label_Match._request_worker_roster_refresh.__get__(app)
    app._request_worker_roster_refresh()
    finish_refresh(app)
    assert {row["display_name"] for row in app._worker_roster_choices()} == {"웹 이름", "이 PC 이름"}
    assert app._roster_b.pending_requests() == requests
    assert "이 PC에 저장됨" in app._worker_roster_notice_text()


def test_v2_html_404_falls_back_to_v1(packaging, tmp_path, monkeypatch):
    app, _ = application(packaging, tmp_path)
    paths = []
    def transport(path, raw, headers):
        paths.append(path)
        if "/v2/" in path:
            return 404, b"<html>not found</html>"
        return 200, json.dumps({**answer(), "authority_scope_id": "site-1"}).encode()
    app._package_pin_client = lambda: make_client(monkeypatch, transport)
    assert app._worker_roster_query("site-1", None)["contract_version"] == worker_roster.CONTRACT_VERSION
    assert paths == ["/api/worker-roster/v2/query", "/api/worker-roster/v1/query"]


def test_stale_login_or_store_result_cannot_apply_ack_or_gate(packaging, tmp_path):
    app, _ = application(packaging, tmp_path)
    row = app._register_worker_name("새 이름")
    body, = app._roster_b.pending_requests()
    app._roster_result_queue.put((0, "site-1", None, True, True, [(body, acknowledgement(body), None)],
                                 app._roster_b, "old-login"))
    app._admin_pin_login_epoch = "new-login"
    app._poll_worker_roster(schedule=False)
    assert not app._worker_roster_stage_b and app._roster_b.pending_requests() == [body]
    assert app._roster_b.resolved_id(row["worker_id"]) == row["worker_id"]


def test_stage_b_201_mapping_affects_only_next_tray_and_slow_registration_preserves_scan_order(packaging, tmp_path, monkeypatch, lock_remote):
    app, dialogs = application(packaging, tmp_path)
    row = app._register_worker_name("새 이름")
    select_local(app, row)
    body, = app._roster_b.pending_requests()
    reply = acknowledgement(body, name="새 이름_1")
    entered, release = threading.Event(), threading.Event()
    def transport(path, raw, headers):
        lock_remote.transport("roster B fake HTTP")
        if path.endswith("/query"):
            return 200, json.dumps(snapshot()).encode()
        assert json.loads(raw) == body
        entered.set()
        assert release.wait(5)
        return 201, json.dumps(reply).encode()
    client = make_client(monkeypatch, transport)
    app._package_pin_client = lambda: client
    app.package_logistics_client.get_capabilities = lambda: {"capabilities": {
        "worker_roster_v2": {"stage_b": True, "temporary_identity_ingest": True, "stage_a": False}}}
    app._request_worker_roster_refresh = lm.Label_Match._request_worker_roster_refresh.__get__(app)
    app._request_worker_roster_refresh()
    try:
        assert entered.wait(2)
        values = [MASTER, *products()]
        started = time.monotonic()
        for value in values[:2]:
            scan(app, value)
        old = copy.deepcopy(app.current_set_info)
        release.set()
        finish_refresh(app)
        assert app.current_set_info == old
        for value in values[2:]:
            scan(app, value)
        assert len(completions(app)) == 1
        assert time.monotonic() - started < 3
        assert app._worker_identity_enabled and app._worker_roster_stage_b
        assert app._roster_b.resolved_id(row["worker_id"]) == reply["worker"]["worker_id"]
        scan(app, MASTER)
        assert app.current_set_info["worker_id"] == reply["worker"]["worker_id"]
        assert app.current_set_info["worker_name_snapshot"] == "새 이름_1"
        assert app.current_set_info["raw"] == [MASTER]
        assert not dialogs
    finally:
        release.set()
        app._roster_thread.join(5)


@pytest.mark.parametrize("gate", [False, True])
def test_registration_mapping_and_new_gate_never_rewrite_saved_package_command(tmp_path, gate):
    from tests.test_package_logistics import _draft, _projection, _receipt, SCOPE as LOGISTICS_SCOPE
    from package_command_draft import PackageCommandDraft
    from package_logistics import PackageLogisticsClient, PackageClientConfig, PackageOutbox, PackageOutboxProcessor
    from package_errors import PackageApiError, PackageTransportError
    store = roster(tmp_path / "roster")
    local = store.register_local("새 이름")
    body, = store.pending_requests()
    original_identity = {"worker_id": local["worker_id"], "worker_name_snapshot": local["display_name"]}
    draft = PackageCommandDraft.from_dict({**_draft().to_dict(), "worker_identity": original_identity})
    outbox = PackageOutbox(tmp_path / "commands.sqlite3")
    row = outbox.enqueue(draft)
    outbox.mark_local_completion_committed(row["idempotency_key"])
    bodies = []
    signal = [gate]
    def server(method, url, headers, raw, duration):
        if url.endswith("/capabilities"):
            return {"ok": True, "data": {"capabilities": {"worker_record_identity_v1": {"enabled": True},
                "worker_roster_v2": {"stage_b": signal[0], "temporary_identity_ingest": signal[0]}}}}
        if method == "GET":
            raise PackageApiError(404, "RECEIPT_NOT_FOUND", "not yet visible")
        bodies.append(raw)
        if len(bodies) == 1:
            raise PackageTransportError("ACK unavailable")
        return {"ok": True, "data": _receipt(draft)}
    client = PackageLogisticsClient(PackageClientConfig("https://logistics.test", uuid.uuid4().hex,
        LOGISTICS_SCOPE, "test-host", "test-device"), transport=server)
    client._label_roster_b = store
    client.get_bundle = lambda *a, **k: _projection()
    assert PackageOutboxProcessor(outbox, client).drain(limit=1) == {"acked": 0, "retry": 1, "conflict": 0}
    saved = outbox.get_by_set_id(draft.set_id)["command_json"]
    payload = json.loads(saved)["payload"]
    assert ("worker_identity" in payload) is gate
    if gate:
        assert payload["worker_identity"] == original_identity
    store.apply_registration(body, acknowledgement(body))
    signal[0] = not gate
    restarted = PackageOutbox(tmp_path / "commands.sqlite3")
    with restarted._connect() as conn:
        conn.execute("UPDATE package_command_outbox SET retry_after_at='2000-01-01T00:00:00Z'")
    assert PackageOutboxProcessor(restarted, client).drain(limit=1) == {"acked": 1, "retry": 0, "conflict": 0}
    assert bodies[0] == bodies[1]
    assert restarted.get_by_set_id(draft.set_id)["command_json"] == saved
    assert json.loads(restarted.get_by_set_id(draft.set_id)["draft_json"])["worker_identity"] == original_identity

def worker(name='tester', *, worker_id=None, number=1, status='ACTIVE'):
    return {'worker_id': worker_id or str(uuid.uuid4()), 'display_name': name,
            'worker_number': number, 'status': status}


def snapshot(*workers, version=1, links=()):
    return {'ok': True, 'contract_version': CONTRACT_VERSION_V2, 'authority_scope_id': 'site-1',
            'version': version, 'unchanged': False, 'workers': list(workers), 'temp_links': list(links)}


def roster(tmp_path, *workers):
    result = WorkerRoster(tmp_path / 'roster.json', 'site-1')
    if workers:
        result.apply(snapshot(*workers), requested_version=None)
    return result


def acknowledgement(body, *, name=None, version=2, worker_id=None):
    row = worker(name or body['display_name'], worker_id=worker_id or body.get('existing_worker_id'))
    link = {'temp_uuid': body['temp_uuid'], 'worker_id': row['worker_id'], 'worker_number': 1} if body['mode'] == 'create' else None
    return {'ok': True, 'contract_version': CONTRACT_VERSION_V2, 'authority_scope_id': 'site-1',
            'request_id': body['request_id'], 'version': version, 'worker': row, 'temp_link': link}


@pytest.mark.parametrize('value,expected', [
    ('  김  ', '김'), (' Nguyen\u00a0  Van ', 'Nguyen Van'), ('e\u0301', 'é'),
    ('김' * 64, '김' * 64), ('😀' * 64, '😀' * 64),
])
def test_registration_normalizes_nfc_spaces_and_accepts_64_codepoints(value, expected):
    assert normalize_registration_name(value) == expected


@pytest.mark.parametrize('value', ['', '  ', 'x' * 65, '123456', PROTECTED_ADMIN_OPERATOR_ID, PROTECTED_ADMIN_DISPLAY_NAME,
    '=a', '+a', '-a', '@a', 'a/b', 'a\\b', 'a\x00b', 'a\nb', 'a\u200bb', 'a\ud800b', 'a\u2028b'])
def test_registration_rejects_unsafe_or_protected_admin_input(tmp_path, value):
    store = roster(tmp_path)
    with pytest.raises(ValueError):
        store.register_local(value)
    assert store.local['sequence'] == 0 and not store.local_path.exists()


@pytest.mark.parametrize('answer,expected', [
    ({}, False), ({'capabilities': {'worker_roster_v2': {'stage_b': True}}}, False),
    ({'capabilities': {'worker_roster_v2': {'stage_b': True, 'temporary_identity_ingest': True}}}, True),
    ({'capabilities': {'worker_roster_v2': {'stage_b': 1, 'temporary_identity_ingest': True}}}, False),
    ({'data': {'capabilities': {}, 'capability_ids': ['worker_roster_v2_stage_b']}}, True),
])
def test_register_gate_accepts_both_true_signals_or_id(answer, expected):
    assert registration_enabled(answer) is expected
    assert STAGE_A is False


def test_local_worker_request_and_sequence_survive_restart_together(tmp_path):
    store = roster(tmp_path)
    local = store.register_local(' 새 이름 ')
    body, = store.pending_requests()
    envelope = json.loads(store.local_path.read_text(encoding='utf-8'))
    assert envelope['sequence'] == local['local_sequence'] == 1
    assert body['temp_uuid'] == local['worker_id']
    assert set(body) == {'mode', 'authority_scope_id', 'request_id', 'display_name', 'action_at', 'temp_uuid'}
    assert worker_choice_label(local) == '새 이름 #임시1'
    restarted = WorkerRoster(store.path, 'site-1')
    assert restarted.pending_requests() == [body]
    assert restarted.active_workers() == store.active_workers()


def test_two_roster_instances_preserve_each_others_registration_and_ack(tmp_path):
    first = roster(tmp_path)
    second = WorkerRoster(first.path, 'site-1')
    one = first.register_local('one')
    two = second.register_local('two')
    requests = second.pending_requests()
    assert [row['local_sequence'] for row in second.local['workers']] == [1, 2]
    reply_one = acknowledgement(requests[0])
    reply_two = acknowledgement(requests[1])
    reply_two['worker']['worker_number'] = reply_two['temp_link']['worker_number'] = 2
    first.apply_registration(requests[0], reply_one)
    second.apply_registration(requests[1], reply_two)
    restarted = WorkerRoster(first.path, 'site-1')
    assert restarted.local['sequence'] == 2 and not restarted.pending_requests()
    assert [item['body'] for item in restarted.local['requests']] == requests
    assert restarted.same_person(one['worker_id'], reply_one['worker']['worker_id'])
    assert restarted.same_person(two['worker_id'], reply_two['worker']['worker_id'])


def test_damaged_envelope_is_preserved_and_never_replaced_with_fresh_ids(tmp_path):
    store = roster(tmp_path, worker())
    store.register_local('new')
    damaged = b'{broken registration bytes'
    store.local_path.write_bytes(damaged)
    restarted = WorkerRoster(store.path, 'site-1')
    assert restarted.local_blocked
    preserved, = store.path.parent.glob(store.local_path.name + '.bad-*')
    assert preserved.read_bytes() == damaged
    with pytest.raises(ValueError):
        restarted.register_local('another')
    assert not restarted.local_path.exists()
    assert WorkerRoster(store.path, 'site-1').local_blocked
    known = worker('web name')
    restarted.apply(snapshot(known, version=3), requested_version=None)
    assert restarted.active_workers() == [known]
    assert WorkerRoster(store.path, 'site-1').active_workers() == [known]


def test_signed_v2_queries_and_replays_use_original_body_and_fresh_nonce(tmp_path, monkeypatch):
    store = roster(tmp_path)
    store.register_local('tester')
    body, = store.pending_requests()
    reply = acknowledgement(body)
    calls = []
    def transport(path, raw, headers):
        calls.append((path, json.loads(raw), headers['X-Producer-Nonce']))
        return (200, json.dumps(snapshot()).encode()) if path.endswith('/query') else (201, json.dumps(reply).encode())
    client = make_client(monkeypatch, transport)
    client.query_roster_v2('site-1')
    client.register_roster(body)
    client.register_roster(body)
    assert calls[0][:2] == ('/api/worker-roster/v2/query', {'authority_scope_id': 'site-1'})
    assert calls[1][1] == calls[2][1] == body
    assert calls[1][2] != calls[2][2]
    for field in ('current_pin', 'approved', 'producer_install_id'):
        with pytest.raises(AdminPinError):
            client.register_roster({**body, field: True})
    assert len(calls) == 3


@pytest.mark.parametrize('status,code', [(503, 'ROSTER_V2_REGISTER_DISABLED'), (429, 'RATE_LIMITED'), (500, 'INTERNAL_ERROR')])
def test_roster_client_preserves_unknown_error_code_status_and_outbox(tmp_path, monkeypatch, status, code):
    store = roster(tmp_path)
    store.register_local('tester')
    body, = store.pending_requests()
    client = make_client(monkeypatch, lambda *args: (status, json.dumps({'ok': False, 'error': {'code': code}}).encode()))
    with pytest.raises(AdminPinError) as caught:
        client.register_roster(body)
    assert caught.value.status == status and caught.value.code == code and caught.value.unknown
    store.apply_registration(body, error=caught.value)
    assert store.pending_requests() == [body]
    assert WorkerRoster(store.path, 'site-1').pending_requests() == [body]


def test_ack_save_failure_and_lost_ack_recover_mapping_without_regenerating_request(tmp_path, monkeypatch):
    store = roster(tmp_path)
    local = store.register_local('tester')
    body, = store.pending_requests()
    reply = acknowledgement(body, name='tester_1')
    with monkeypatch.context() as patcher:
        patcher.setattr('label_worker_roster._atomic_write', lambda *args, **kwargs: (_ for _ in ()).throw(OSError('lost save')))
        with pytest.raises(OSError):
            store.apply_registration(body, reply)
    assert store.pending_requests() == [body]
    assert store.resolved_id(local['worker_id']) == local['worker_id']
    restarted = WorkerRoster(store.path, 'site-1')
    restarted.apply(snapshot(reply['worker'], links=[reply['temp_link']], version=2), requested_version=None)
    assert not restarted.pending_requests()
    assert restarted.resolved_id(local['worker_id']) == reply['worker']['worker_id']
    assert worker_choice_label(restarted.active_workers()[0]) == 'tester_1 #1'
    assert restarted.local['requests'][0]['body'] == body


def test_same_pending_name_keeps_one_create_and_durable_dependent_return(tmp_path):
    store = roster(tmp_path)
    first = store.register_local('tester')
    create, = store.pending_requests()
    same = store.register_local('tester', existing_worker_id=first['worker_id'])
    assert same['worker_id'] == first['worker_id']
    assert store.pending_requests() == [create]
    intent = copy.deepcopy(store.local['requests'][1])
    assert intent['body'] is None and intent['depends_on'] == create['request_id']
    restarted = WorkerRoster(store.path, 'site-1')
    reply = acknowledgement(create)
    restarted.apply_registration(create, reply)
    returned, = restarted.pending_requests()
    assert returned['mode'] == 'return' and 'temp_uuid' not in returned
    assert returned['existing_worker_id'] == reply['worker']['worker_id']
    assert returned['request_id'] == intent['request_id']
    assert returned['action_at'] == intent['intent']['action_at']
    assert WorkerRoster(store.path, 'site-1').pending_requests() == [returned]


def test_same_central_name_returns_existing_uuid_and_different_name_creates(tmp_path):
    original = worker('tester')
    store = roster(tmp_path, original)
    assert store.duplicate_workers(' tester ') == [original]
    with pytest.raises(ValueError):
        store.register_local('tester')
    same = store.register_local('tester', existing_worker_id=original['worker_id'])
    returned, = store.pending_requests()
    assert same['worker_id'] == returned['existing_worker_id'] == original['worker_id']
    assert returned['mode'] == 'return' and 'temp_uuid' not in returned
    different = store.register_local('testerB')
    assert different['worker_id'] != original['worker_id']
    assert len(store.active_workers()) == 2


@pytest.mark.parametrize('code', ['REQUEST_CONFLICT', 'TEMP_ID_CONFLICT'])
def test_conflict_never_reissues_identity_and_mapping_preserves_review_until_name_matches(tmp_path, code):
    store = roster(tmp_path)
    local = store.register_local('tester')
    body, = store.pending_requests()
    store.apply_registration(body, error=AdminPinError(code, status=409))
    assert not store.pending_requests() and store.active_worker(worker_id=local['worker_id'])
    reply = acknowledgement(body, name='different')
    store.apply(snapshot(reply['worker'], links=[reply['temp_link']], version=3), requested_version=None)
    assert store.local['requests'][0]['state'] == 'REVIEW'
    assert store.local['requests'][0]['error'] == code
    assert store.local['requests'][0]['body'] == body
    store.apply(snapshot({**reply['worker'], 'display_name': 'tester'}, links=[reply['temp_link']], version=4), requested_version=3)
    assert store.local['requests'][0]['state'] == 'ACKED'
    assert store.local['requests'][0]['error'] == code


def test_new_server_disabled_status_wins_over_pending_return_and_picker_hidden_is_ignored(tmp_path):
    original = worker('tester')
    store = roster(tmp_path, {**original, 'picker_hidden': True})
    assert store.active_workers() == [original] and STAGE_A is False
    store.register_local('tester', existing_worker_id=original['worker_id'])
    store.apply(snapshot({**original, 'status': 'DISABLED'}, version=2), requested_version=1)
    assert store.local['requests'][0]['state'] == 'TERMINAL'
    assert not store.pending_requests() and not store.active_workers()


@pytest.mark.parametrize('code,status', [('WORKER_DISABLED', 403), ('WORKER_NOT_FOUND', 404)])
def test_rejected_return_is_terminal_and_not_selectable(tmp_path, code, status):
    original = worker()
    store = roster(tmp_path, original)
    store.register_local('tester', existing_worker_id=original['worker_id'])
    request, = store.pending_requests()
    store.apply_registration(request, error=AdminPinError(code, status=status))
    assert store.local['requests'][0]['state'] == 'TERMINAL'
    assert not store.active_workers() and not store.pending_requests()
    store.apply(snapshot(original, version=2), requested_version=1)
    assert store.active_workers() == [original]
    assert store.local['requests'][0]['state'] == 'TERMINAL' and not store.pending_requests()


def test_unsafe_central_name_never_resurrects_through_temporary_alias(tmp_path):
    store = roster(tmp_path)
    local = store.register_local('tester')
    request, = store.pending_requests()
    reply = acknowledgement(request)
    store.apply(snapshot({**reply['worker'], 'display_name': 'unsafe/name'}, links=[reply['temp_link']], version=2), requested_version=None)
    assert store.same_person(local['worker_id'], reply['worker']['worker_id'])
    assert not store.active_workers()
    assert not WorkerRoster(store.path, 'site-1').active_workers()


def test_older_query_keeps_new_status_but_saves_immutable_temp_mapping(tmp_path):
    store = roster(tmp_path)
    local = store.register_local('tester')
    body, = store.pending_requests()
    reply = acknowledgement(body)
    store.apply(snapshot({**reply['worker'], 'status': 'DISABLED'}, version=4), requested_version=None)
    store.apply(snapshot(reply['worker'], links=[reply['temp_link']], version=2), requested_version=None)
    assert store.known_version == 4 and not store.active_workers()
    assert store.same_person(local['worker_id'], reply['worker']['worker_id'])
    original = store.local_path.read_bytes()
    with pytest.raises(ValueError):
        store.apply(snapshot(links=[{**reply['temp_link'], 'worker_id': str(uuid.uuid4())}], version=5), requested_version=4)
    assert store.local_path.read_bytes() == original


@pytest.mark.parametrize('future', [False, True])
def test_invalid_request_is_item_review_except_future_action_at(tmp_path, future):
    store = roster(tmp_path)
    if future:
        import datetime
        real = datetime.datetime
        class FutureClock(real):
            @classmethod
            def now(cls, tz=None):
                return real.now(tz) + datetime.timedelta(minutes=10)
        with patch('label_worker_roster.dt.datetime', FutureClock):
            store.register_local('tester')
    else:
        store.register_local('tester')
    request, = store.pending_requests()
    store.apply_registration(request, error=AdminPinError('INVALID_REQUEST', status=400))
    assert bool(store.pending_requests()) is future
    assert store.local['requests'][0]['body'] == request
