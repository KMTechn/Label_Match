"""Stage-one selection, offline integrity, and background query regressions."""
import copy
import csv
import hashlib
import hmac
import json
import uuid
import queue
import threading
import time
import uuid
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

import Label_Match as module
import label_worker_roster as roster
from tests.test_w9scanonly_packaging import packaging, products, scan, completions
from tests.test_legacy_label_transition import MASTER
from label_admin_pin import AdminPinClient, AdminPinError
from direct_sync_push import canonical_request_string

SCOPE = "roster-test-site"
ALICE = "00000000-0000-0000-0000-000000000001"
BOB = "00000000-0000-0000-0000-000000000002"


def answer(version=1, disabled=False, name="Nguyễn Thị Mai"):
    return {"ok": True, "contract_version": roster.CONTRACT_VERSION,
            "authority_scope_id": SCOPE, "version": version, "unchanged": False,
            "workers": [
                {"worker_id": ALICE, "display_name": name,
                 "status": "DISABLED" if disabled else "ACTIVE"},
                {"worker_id": BOB, "display_name": "김철", "status": "ACTIVE"}]}


def app_with_roster(tmp_path, settings=None, response=None):
    app = module.Label_Match.__new__(module.Label_Match)
    app.app_settings_path = str(tmp_path / "app_settings.json")
    app.app_settings = settings or {}
    app.worker_name = app.app_settings.get("worker_name", "")
    app.package_logistics_client = SimpleNamespace(config=SimpleNamespace(authority_scope_id=SCOPE))
    app.current_set_info = {}
    app._init_worker_roster()
    if response is not None:
        app._apply_worker_roster_answer(0, SCOPE, response)
    return app


def test_query_versions_cache_offline_restart_and_recent_identity(tmp_path):
    app = app_with_roster(tmp_path, {"worker_name": "김철", "recent_worker_ids": [BOB]}, answer())
    assert [w["worker_id"] for w in app._worker_roster_choices()] == [BOB, ALICE]
    assert app._roster_worker_allowed()
    assert app.worker_id == BOB and app.app_settings["worker_id"] == BOB
    before = Path(app._roster_cache_path).read_bytes()
    unchanged = {"ok": True, "contract_version": roster.CONTRACT_VERSION,
                 "authority_scope_id": SCOPE, "version": 1, "unchanged": True}
    app._apply_worker_roster_answer(0, SCOPE, unchanged)
    assert Path(app._roster_cache_path).read_bytes() == before
    restored = app_with_roster(tmp_path, {"worker_id": BOB, "worker_name": "김철"})
    assert restored._roster_worker_allowed()
    restored._apply_worker_roster_answer(0, SCOPE, answer(2, disabled=True))
    assert [w["worker_id"] for w in restored._worker_roster_choices()] == [BOB]


@pytest.mark.parametrize("fault", ["digest", "scope", "duplicate", "shape", "json", "deep-json"])
def test_damaged_cache_preserves_original_and_uses_good_copy(tmp_path, fault):
    path = tmp_path / "roster.json"
    roster.save_cache(path, answer(), SCOPE)
    payload = json.loads(path.read_text(encoding="utf-8"))
    if fault == "digest":
        payload["workers"][0]["display_name"] = "Changed"
    elif fault == "scope":
        payload["authority_scope_id"] = "other-site"
    elif fault == "duplicate":
        payload["workers"][1]["worker_id"] = ALICE
    elif fault == "shape":
        payload["version"] = True
    damaged = b"{broken" if fault == "json" else json.dumps(payload).encode()
    if fault == "deep-json":
        damaged = b'{"workers":' + b'[' * 2000 + b'0' + b']' * 2000 + b'}'
    path.write_bytes(damaged)
    assert roster.load_cache(path, SCOPE)["workers"] == answer()["workers"]
    preserved = list(tmp_path.glob("roster.json.invalid-*"))
    assert len(preserved) == 1 and preserved[0].read_bytes() == damaged


def test_no_roster_recent_names_and_remembered_name_continue_without_entry(tmp_path):
    app = app_with_roster(tmp_path, {"worker_name": "Mai", "worker_history": ["김철", "Mai"]})
    assert app._roster_worker_allowed()
    assert {w["display_name"] for w in app._worker_roster_choices()} == {"Mai", "김철"}
    assert all(w["worker_id"] is None for w in app._worker_roster_choices())
    empty = app_with_roster(tmp_path / "empty")
    assert not empty._roster_worker_allowed() and empty._worker_roster_choices() == []


def test_disabled_missing_and_renamed_identity_require_choice_at_next_start(tmp_path):
    app = app_with_roster(tmp_path, {"worker_id": ALICE, "worker_name": "Nguyễn Thị Mai"}, answer())
    assert app._roster_worker_allowed()
    app._apply_worker_roster_answer(0, SCOPE, answer(2, disabled=True))
    assert not app._roster_worker_allowed()
    app.current_set_info = {"id": "in-progress", "raw": [MASTER]}
    assert app._roster_worker_allowed(), "the original set can finish or recover"
    app.current_set_info = {}
    assert not app._roster_worker_allowed()
    app._apply_worker_roster_answer(0, SCOPE, answer(3, name="Mai New"))
    assert not app._roster_worker_allowed(), "do not relabel the remembered identity silently"


def test_query_failure_and_late_response_do_not_replace_cache(tmp_path):
    app = app_with_roster(tmp_path, response=answer(2))
    original = Path(app._roster_cache_path).read_bytes()
    with pytest.raises(ValueError):
        app._apply_worker_roster_answer(0, SCOPE, {"ok": False})
    app._roster_request_serial = 1
    assert app._apply_worker_roster_answer(0, SCOPE, answer(3, disabled=True)) is False
    assert app._apply_worker_roster_answer(1, "other-site", answer(3)) is False
    assert Path(app._roster_cache_path).read_bytes() == original
    with pytest.raises(ValueError):
        app._apply_worker_roster_answer(1, SCOPE, answer(1))
    with pytest.raises(ValueError):
        app._apply_worker_roster_answer(1, SCOPE, answer(2, disabled=True))


@pytest.mark.parametrize("status", [403, 503])
def test_background_http_refusal_shows_code_and_preserves_choices(tmp_path, status):
    from label_admin_pin import AdminPinError
    app = app_with_roster(tmp_path, response=answer(2))
    before = Path(app._roster_cache_path).read_bytes()
    notes = []
    app._roster_selection_window = object()
    app._roster_choice_widget = SimpleNamespace(configure=lambda **kwargs: None)
    app._roster_choice_note = SimpleNamespace(configure=lambda **kwargs: notes.append(kwargs["text"]))
    app.worker_name_var = SimpleNamespace(get=lambda: "", set=lambda value: None)
    def query(*args):
        raise AdminPinError(f"HTTP_{status}", unknown=True)
    app._worker_roster_query = query
    app._request_worker_roster_refresh()
    app._roster_thread.join(2)
    assert not app._roster_thread.is_alive()
    app._poll_worker_roster(schedule=False)
    assert notes and "회사 서버" in notes[-1] and str(status) in notes[-1]
    assert Path(app._roster_cache_path).read_bytes() == before
    assert app._worker_roster["version"] == 2


def test_stale_roster_http_refusal_does_not_replace_current_notice(tmp_path):
    app = app_with_roster(tmp_path, response=answer(2))
    app._roster_request_serial = 1
    app._roster_result_queue.put((0, SCOPE, None, False, "HTTP_403"))
    app._poll_worker_roster(schedule=False)
    assert app.__dict__.get("_roster_http_error_code", "") == ""


def test_first_launch_does_not_treat_packaged_template_as_local_history(tmp_path, monkeypatch):
    template = tmp_path / "template.json"
    template.write_text(json.dumps({"worker_name": "template worker", "worker_history": ["Template"],
                                    "ui_settings": {"base_font_size": 14}}), encoding="utf-8")
    target = tmp_path / "user" / "app_settings.json"
    monkeypatch.setattr(module, "_default_label_match_settings_path", lambda: str(target))
    monkeypatch.setattr(module, "resource_path", lambda *_: str(template))
    app = module.Label_Match.__new__(module.Label_Match)
    app._setup_paths()
    settings = app._load_app_settings()
    assert "worker_name" not in settings and "worker_history" not in settings
    assert settings["ui_settings"] == {"base_font_size": 14}
    target.write_text(json.dumps({"worker_name": "Mai", "worker_history": ["Mai"]}), encoding="utf-8")
    app._setup_paths()
    assert app._load_app_settings()["worker_name"] == "Mai", "existing PC names are retained"


@pytest.mark.parametrize("name", ["김철", "김민수", "Li", "Nguyễn Thị Mai", "長い名前" * 80])
def test_names_have_no_length_or_language_limit(name):
    assert roster.valid_name(name)


@pytest.mark.parametrize("name", ["", " \t ", "Mai\nNew", "Mai\x00", "Mai\x85"])
def test_empty_and_control_character_names_are_rejected(name):
    assert not roster.valid_name(name)


def test_delayed_background_query_does_not_hold_scan_writer(packaging, tmp_path, lock_remote):
    app, _dialogs = packaging
    selected_name = "Nguyễn Thị Mai"
    app.app_settings = {"worker_name": selected_name, "worker_id": ALICE}
    app.worker_name = selected_name
    app.data_manager.close(timeout=None)
    app.data_manager = module.DataManager(app.save_directory, app.Worker.PACKAGING, selected_name, "PC-LT")
    app.data_manager._state_lock = lock_remote.wrap_lock(app.data_manager._state_lock, label="scan checkpoint")
    app.app_settings_path = str(tmp_path / "settings" / "app_settings.json")
    app.package_logistics_client = SimpleNamespace(config=SimpleNamespace(authority_scope_id=SCOPE))
    app._init_worker_roster()
    app._apply_worker_roster_answer(0, SCOPE, answer())
    started = threading.Event()
    def query(scope, known_version):
        lock_remote.transport("roster fake query")
        assert scope == SCOPE and known_version == 1
        started.set()
        time.sleep(6)
        raise OSError("offline")
    app._worker_roster_query = query
    before = Path(app._roster_cache_path).read_bytes()
    app._request_worker_roster_refresh()
    assert started.wait(2)
    begin = time.monotonic()
    for value in [MASTER, *products()]:
        scan(app, value)
    assert len(completions(app)) == 1
    rows = []
    for path in Path(app.save_directory).glob("*PC-LT*.csv"):
        assert path.read_bytes().startswith(b"\xef\xbb\xbftimestamp,worker_name,event,details\r\n")
        with path.open(encoding="utf-8-sig", newline="") as handle:
            rows.extend(csv.DictReader(handle))
    assert rows and all(row["worker_name"] == selected_name for row in rows)
    assert all(ALICE not in row["details"] for row in rows)
    assert time.monotonic() - begin < 3, "local durable completion waited for roster"
    assert app._roster_thread.is_alive()
    app._roster_thread.join(8)
    app._poll_worker_roster(schedule=False)
    assert Path(app._roster_cache_path).read_bytes() == before


def test_roster_reuses_signed_transport_and_preserves_admin_route_and_size(lock_remote):
    credentials = SimpleNamespace(endpoint_url="https://roster-test.example.invalid/api/ingest",
                                  producer_id="roster-test-producer", key_id=uuid.uuid4().hex,
                                  secret=uuid.uuid4().hex)
    requests = []
    result = answer()
    def server(path, body, headers):
        lock_remote.transport("authenticated roster fake")
        payload = json.loads(body)
        requests.append((path, payload))
        canonical = canonical_request_string(
            method="POST", path=path, query_string="",
            timestamp=headers["X-Producer-Timestamp"], nonce=headers["X-Producer-Nonce"],
            producer_id=credentials.producer_id, key_id=credentials.key_id,
            metadata=payload, content_sha256=hashlib.sha256(body).hexdigest(),
            byte_length=len(body), content_type="application/json")
        assert hmac.compare_digest(headers["X-Producer-Signature"],
            hmac.new(credentials.secret.encode(), canonical.encode(), hashlib.sha256).hexdigest())
        return 200, json.dumps(result).encode()
    client = AdminPinClient(credentials, transport=server)
    assert client.query_roster(SCOPE, None)["workers"] == answer()["workers"]
    assert requests[-1] == ("/api/worker-roster/v1/query", {"authority_scope_id": SCOPE, "known_version": None})
    client.post("verifications", {"contract_version": "admin-pin.v1"})
    assert requests[-1][0] == "/api/admin-pin/v1/verifications"
    result = {"ok": True, "padding": "x" * 9000}
    assert client.query_roster(SCOPE, 1)["ok"] is True
    with pytest.raises(AdminPinError):
        client.post("verifications", {})
    result = {"ok": True, "padding": "x" * 262144}
    with pytest.raises(AdminPinError):
        client.query_roster(SCOPE, 1)


def test_save_accepts_only_current_choice_and_keeps_admin_separate(tmp_path, monkeypatch):
    app = app_with_roster(tmp_path, {"worker_name": "김철", "worker_id": BOB}, answer())
    app.run_tests = True
    app.initialized_successfully = True
    app.save_directory = str(tmp_path / "data")
    app.unique_id = "PC-CHOICE"
    app.scale_factor = 1.2
    app.tree_font_size = 14
    app._has_background_work = lambda: False
    app._block_view_only_action = lambda *a, **k: False
    app._block_duplicate_history_load = lambda *a, **k: False
    app._release_shortage_confirmation = lambda: True
    app._update_save_directory = lambda: None
    app.title = lambda *a: None
    app._destroy_modal_and_refocus = lambda *a: None
    class Tree:
        def __getitem__(self, name):
            return []
    app.summary_tree = app.history_tree = Tree()
    app.data_manager = module.DataManager(app.save_directory, app.Worker.PACKAGING, app.worker_name, app.unique_id)
    try:
        app._save_settings_and_close(object(), "Unlisted")
        assert app.worker_name == "김철" and app.worker_id == BOB
        app._save_settings_and_close(object(), "Nguyễn Thị Mai", _protected_admin_login=True)
        assert app.worker_name == "김철", "administrator entry cannot accept an ordinary name"
        app._save_settings_and_close(object(), "Nguyễn Thị Mai")
        assert app.worker_name == "Nguyễn Thị Mai" and app.worker_id == ALICE
        settings = json.loads(Path(app.app_settings_path).read_text(encoding="utf-8"))
        assert settings["worker_name"] == "Nguyễn Thị Mai" and settings["worker_id"] == ALICE
        assert settings["recent_worker_ids"][0] == ALICE
    finally:
        app.data_manager.close(timeout=None)


def test_worker_ui_has_only_readonly_selection():
    import ast
    tree = ast.parse(Path(module.__file__).read_text(encoding="utf-8-sig"))
    method = next(n for n in ast.walk(tree) if isinstance(n, ast.FunctionDef) and n.name == "open_settings_window")
    combos = [n for n in ast.walk(method) if isinstance(n, ast.Call)
              and isinstance(n.func, ast.Attribute) and n.func.attr == "Combobox"]
    assert len(combos) == 1
    assert any(k.arg == "state" and isinstance(k.value, ast.Constant)
               and k.value.value == "readonly" for k in combos[0].keywords)
    assert not any(isinstance(n, ast.Call) and isinstance(n.func, ast.Attribute)
                   and n.func.attr == "Entry" for n in ast.walk(method))
    assert any(isinstance(n, ast.Call) and isinstance(n.func, ast.Attribute)
               and n.func.attr == "Button" and any(k.arg == "text" and isinstance(k.value, ast.Constant)
                   and k.value.value == "새 이름 등록" for k in n.keywords) for n in ast.walk(method))


def test_headless_selector_refreshes_active_choices_and_offline_notice(tmp_path, monkeypatch):
    app = app_with_roster(tmp_path, {"worker_name": "김철", "recent_worker_ids": [BOB]}, answer())
    app.colors = {}
    app.default_font_name = "Malgun Gothic"
    app.run_tests = True
    app._request_worker_roster_refresh = lambda: None
    app._center_child_window = lambda *a: None
    app._dialog_size = lambda *a: (600, 300)
    class Variable:
        def __init__(self, value=""):
            self.value = value
        def get(self):
            return self.value
        def set(self, value):
            self.value = value
    class Widget:
        def __init__(self, *a, **kwargs):
            self.options = kwargs
        def configure(self, **kwargs):
            self.options.update(kwargs)
        config = configure
        def __getattr__(self, name):
            return lambda *a, **k: None
    monkeypatch.setattr(module.tk, "Toplevel", Widget)
    monkeypatch.setattr(module.tk, "StringVar", Variable)
    for name in ("Frame", "Label", "Button", "Combobox"):
        monkeypatch.setattr(module.ttk, name, Widget)
    app.open_settings_window()
    assert app._roster_choice_widget.options["state"] == "readonly"
    assert app._roster_choice_widget.options["values"] == ["김철", "Nguyễn Thị Mai"]
    app.worker_name_var.set("Nguyễn Thị Mai")
    app._apply_worker_roster_answer(0, SCOPE, answer(2, disabled=True))
    assert app.worker_name_var.get() == ""
    assert app._roster_choice_widget.options["values"] == ["김철"]
    app._worker_roster = None
    app._refresh_worker_roster_selection()
    assert "명단을 받지 못해 이 PC 에 저장된 이름을 씁니다" in app._roster_choice_note.options["text"]


def test_old_settings_ignore_malformed_recent_identity_entries(tmp_path):
    app = app_with_roster(tmp_path, {"worker_name": "김철", "recent_worker_ids": [{}, None, BOB]}, answer())
    assert app._roster_worker_allowed()
    assert [worker["worker_id"] for worker in app._worker_roster_choices()] == [BOB, ALICE]


@pytest.mark.parametrize("invalid", ["", " \u3000 ", "\u0001", "이\u200b", "이\ud800", "이\u2028", "이\u2029"])
def test_invalid_name_row_keeps_valid_worker_through_cache_restart(tmp_path, caplog, invalid):
    app = app_with_roster(tmp_path, {"worker_name": "김철"}, answer(name=invalid))
    assert app._roster_worker_allowed() and app.worker_id == BOB
    assert [worker["display_name"] for worker in app._worker_roster_choices()] == ["김철"]
    assert any("invalid-name rows: 1" in record.message for record in caplog.records)
    stored = json.loads(Path(app._roster_cache_path).read_text(encoding="utf-8"))
    assert set(stored) == {"contract_version", "authority_scope_id", "version", "workers", "fetched_at", "sha256"}
    assert stored["workers"] == answer(name=invalid)["workers"]
    app._init_worker_roster()
    assert app._roster_worker_allowed()
    assert [worker["worker_id"] for worker in app._worker_roster_choices()] == [BOB]


def test_trimmed_name_binds_remembered_worker_without_changing_received_version(tmp_path):
    app = app_with_roster(tmp_path, {"worker_name": "Nguyễn Thị Mai"}, answer(name="  Nguyễn Thị Mai\u3000"))
    assert app._roster_worker_allowed() and app.worker_id == ALICE
    assert app._worker_roster_choices()[0]["display_name"] == "Nguyễn Thị Mai"
    before = Path(app._roster_cache_path).read_bytes()
    assert app._apply_worker_roster_answer(0, SCOPE, answer(name="  Nguyễn Thị Mai\u3000"))
    assert Path(app._roster_cache_path).read_bytes() == before
    with pytest.raises(ValueError, match="changed workers"):
        app._apply_worker_roster_answer(0, SCOPE, answer(name="Nguyễn Thị Mai"))
    app._init_worker_roster()
    assert app._roster_worker_allowed()


@pytest.mark.parametrize("restart", [False, True])
@pytest.mark.parametrize("remembered", [False, True])
def test_zero_received_rows_allow_local_choice_and_notice(tmp_path, restart, remembered):
    settings = {"worker_history": [{"name": "김철"}], "worker_name": "김철" if remembered else ""}
    response = answer()
    response["workers"] = []
    app = app_with_roster(tmp_path, settings, response)
    if restart:
        app._init_worker_roster()
    assert app._roster_worker_allowed() is remembered
    assert app._worker_roster_choices() == [{"worker_id": None, "display_name": "김철", "legacy_local": True}]
    notes = {}
    app._roster_selection_window = object()
    app._roster_choice_widget = SimpleNamespace(configure=lambda **values: notes.update(values))
    app._roster_choice_note = SimpleNamespace(configure=lambda **values: notes.update(values))
    app.worker_name_var = SimpleNamespace(get=lambda: "", set=lambda value: None)
    app._refresh_worker_roster_selection()
    assert "이 PC 에 저장된 이름을 씁니다" in notes["text"]


@pytest.mark.parametrize("restart", [False, True])
@pytest.mark.parametrize("invalid_names", [False, True])
def test_nonempty_unusable_roster_never_allows_local_fallback(tmp_path, restart, invalid_names):
    response = answer()
    for worker in response["workers"]:
        if invalid_names:
            worker["display_name"] = "\u200b"
        else:
            worker["status"] = "DISABLED"
    app = app_with_roster(tmp_path, {"worker_name": "김철"}, response)
    if restart:
        app._init_worker_roster()
    assert app._worker_roster_choices() == [] and not app._roster_worker_allowed()
    notes = {}
    app._roster_selection_window = object()
    app._roster_choice_widget = SimpleNamespace(configure=lambda **values: notes.update(values))
    app._roster_choice_note = SimpleNamespace(configure=lambda **values: notes.update(values))
    app.worker_name_var = SimpleNamespace(get=lambda: "김철", set=lambda value: None)
    app._refresh_worker_roster_selection()
    assert notes["text"] == "이름을 고르거나 새 이름을 등록하세요."


def test_dropped_name_does_not_hide_duplicate_identity_or_cache_damage(tmp_path):
    response = answer(name="\u200b")
    response["workers"][0]["worker_id"] = BOB
    with pytest.raises(ValueError):
        roster.apply_response(response, None, SCOPE)
    app = app_with_roster(tmp_path, {"worker_name": "김철"}, answer(name="\u200b"))
    path = Path(app._roster_cache_path)
    damaged = json.loads(path.read_text(encoding="utf-8"))
    damaged["workers"][0]["display_name"] = "\u200c"
    original = json.dumps(damaged).encode("utf-8")
    path.write_bytes(original)
    app._init_worker_roster()
    assert app._roster_worker_allowed()
    assert next(path.parent.glob("roster.json.invalid-*")).read_bytes() == original


@pytest.mark.parametrize("name", ["김철", "김민수", "Li", "Nguyễn Thị Mai", "長い名前" * 80])
def test_full_name_survives_csv_header_display_and_safe_filename(tmp_path, name):
    manager = module.DataManager(str(tmp_path), "포장실", name, "PC-NAME")
    try:
        manager.log_event(module.Label_Match.Events.SCAN_OK, {"message": "roster name"})
        assert manager.flush(timeout=5)
        assert module._label_match_operator_context(name) == "작업자 " + name
        paths = list(tmp_path.rglob("*.csv"))
        assert paths and all(len(path.name) < 240 for path in paths)
        with paths[0].open(encoding="utf-8-sig", newline="") as handle:
            rows = list(csv.DictReader(handle))
        assert rows and all(row["worker_name"] == name for row in rows)
        assert all(set(row) == {"timestamp", "worker_name", "event", "details"} for row in rows)
    finally:
        manager.close(timeout=None)


@pytest.mark.parametrize("name", ["A" * 65, *["Name" + char for char in '\\/:*?"<>|'], *[char + "Name" for char in "=+-@"]])
def test_server_name_limits_drop_only_the_bad_row_online_and_offline(tmp_path, name):
    payload = answer(name=name)
    invalid = payload["workers"][0]
    valid = dict(invalid, worker_id=str(uuid.uuid4()), display_name="정상")
    payload["workers"] = [invalid, valid]
    scope = payload["authority_scope_id"]
    result = roster.apply_response(payload, None, scope)
    assert result["workers"] == [valid]
    assert not roster.uses_legacy_names(result)
    path = tmp_path / "roster.json"
    roster.save_cache(path, result, scope)
    cached = roster.load_cache(path, scope)
    assert cached["workers"] == [valid]
    assert not roster.uses_legacy_names(cached)


@pytest.mark.parametrize("name", ["A" * 64, "  " + "A" * 64 + "  ", "김찬", "José", "A+B-@"])
def test_server_name_boundary_language_and_interior_operators_remain_usable(name):
    payload = answer(name=name)
    result = roster.apply_response(payload, None, payload["authority_scope_id"])
    assert result["workers"][0]["display_name"] == name.strip()


@pytest.mark.parametrize("name", ["A" * 65, "A" * 128, "Name/Old", "=Old", "Old\u200bName"])
def test_legacy_local_name_validation_keeps_the_baseline_rules(name):
    assert roster.valid_name(name)
    assert roster.choices(None, [], [name])[0]["display_name"] == name


def test_disabled_worker_finishes_actual_package_then_next_scan_requests_selection(packaging, tmp_path):
    app, dialogs = packaging
    app.app_settings = {"worker_name": app.worker_name, "worker_id": ALICE}
    app.app_settings_path = str(tmp_path / "settings" / "app_settings.json")
    app.package_logistics_client = SimpleNamespace(config=SimpleNamespace(authority_scope_id=SCOPE))
    app._init_worker_roster()
    app._apply_worker_roster_answer(0, SCOPE, answer(name=app.worker_name))
    values = [MASTER, *products()]
    for value in values[:2]:
        scan(app, value)
    app._apply_worker_roster_answer(0, SCOPE, answer(2, disabled=True, name=app.worker_name))
    for value in values[2:]:
        scan(app, value)
    assert len(completions(app)) == 1
    selected = []
    app.open_settings_window = lambda: selected.append(True)
    app._show_scanner_notice = Mock()
    scan(app, MASTER)
    app._show_scanner_notice.assert_called_once_with(
        "작업자 선택", "이름을 고르거나 '새 이름 등록'에서 이름을 등록한 뒤 다시 스캔하세요.")
    assert app.current_set_info["raw"] == [] and selected == [True]
    assert len(completions(app)) == 1 and dialogs == []
