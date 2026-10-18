"""D-05: the name left in settings is only a 'last time' hint, never this run's worker."""
import ast
import csv
import json
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

import Label_Match as module
import label_screen_bridge as bridge
from label_admin_pin import AdminPinError
from tests.test_label_worker_roster import ALICE, BOB, SCOPE, answer
from tests.test_w9scanonly_packaging import packaging, products, scan, completions  # noqa: F401
from tests.test_legacy_label_transition import MASTER

ROOT = Path(module.__file__).resolve().parent


def init_assignments():
    tree = ast.parse((ROOT / "Label_Match.py").read_text(encoding="utf-8"))
    cls = next(n for n in tree.body if isinstance(n, ast.ClassDef) and n.name == "Label_Match")
    init = next(n for n in cls.body if isinstance(n, ast.FunctionDef) and n.name == "__init__")
    found = {}
    for node in ast.walk(init):
        if isinstance(node, ast.Assign) and len(node.targets) == 1:
            target = node.targets[0]
            if (isinstance(target, ast.Attribute) and isinstance(target.value, ast.Name)
                    and target.value.id == "self"):
                found.setdefault(target.attr, ast.unparse(node.value))
    return found, init


def test_real_init_starts_with_nobody_chosen():
    found, init = init_assignments()
    assert found["worker_name"] == "''"
    assert found["_worker_pick_pending"] == "True"
    assert "app_settings.get('worker_name'" in found["_last_worker_name"]
    assert "WORKER_UNDECIDED_LABEL" in ast.unparse(init), "DataManager must not open with a blank owner"


def pending_start(app, tmp_path, settings, *, current=None):
    """Same state __init__ leaves (pinned by the AST test above); real roster init after it."""
    found, _ = init_assignments()
    assert found["_worker_pick_pending"] == "True"
    app.app_settings = dict(settings)
    app.app_settings_path = str(tmp_path / "settings" / "app_settings.json")
    app._last_worker_name = str(settings.get("worker_name", "")).strip()
    app.worker_name = ""
    app._worker_pick_pending = True
    app.current_set_info = current if current is not None else {}
    app.package_logistics_client = SimpleNamespace(config=SimpleNamespace(authority_scope_id=SCOPE))
    app._init_worker_roster()
    app.opened_settings = []
    app.open_settings_window = lambda: app.opened_settings.append(True)
    app._apply_worker_roster_answer(0, SCOPE, answer())


def enable_save(app, tmp_path):
    app.run_tests = True
    app.initialized_successfully = True
    app.save_directory = str(tmp_path / "data")
    app.unique_id = "PC-D05"
    app.scale_factor, app.tree_font_size = 1.2, 14
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

        def column(self, *a):
            return 10

    app.summary_tree = app.history_tree = Tree()
    app.data_manager = module.DataManager(app.save_directory, app.Worker.PACKAGING,
                                          module.WORKER_UNDECIDED_LABEL, app.unique_id)


def rows(app, event):
    assert app.data_manager.flush(timeout=5)
    out = []
    for path in sorted(Path(app.save_directory).glob("*.csv")):
        with path.open(encoding="utf-8-sig", newline="") as stream:
            out.extend(r for r in csv.DictReader(stream) if r["event"] == event)
    return out


def test_start_leaves_worker_undecided_and_requires_picking(packaging, tmp_path):
    app, _ = packaging
    pending_start(app, tmp_path, {"worker_name": "김철", "worker_id": BOB, "recent_worker_ids": [BOB]})
    assert app.worker_name == "" and app.worker_id is None
    assert app.opened_settings == [True], "settings window opens at start"
    assert not app._roster_worker_allowed()
    assert [w["worker_id"] for w in app._worker_roster_choices()][0] == BOB
    state = bridge._settings(SimpleNamespace(app=app, module=module, _register=None))
    assert state["required"] is True and state["selected"] == "김철"
    assert app.app_settings["worker_name"] == "김철"


def test_scan_before_picking_is_visibly_refused_and_new_screen_says_bad(packaging, tmp_path):
    app, _ = packaging
    pending_start(app, tmp_path, {"worker_name": app.worker_name, "worker_id": ALICE})
    app._play_sound = Mock()
    opened_before = len(app.opened_settings)
    scan(app, MASTER)
    app._play_sound.assert_any_call("fail")
    assert len(app.opened_settings) == opened_before + 1 and not app.current_set_info.get("raw") and not completions(app)
    notice = app.__dict__.get("_workflow_notice") or app.__dict__.get("_scanner_notice")
    view = SimpleNamespace(current_stage="idle", notice=notice, completion_kind=None, qa_completed=False)
    processor = SimpleNamespace(app=app, _flash=None, _sound=None, _serial=1)
    result = bridge._result(processor, view, {"raw": []})
    assert result["kind"] == "bad" and "작업자" in result["text"]


def test_restored_set_opens_picker_refuses_scan_allows_first_pick(packaging, tmp_path):
    app, _ = packaging
    restored = {"id": "SET-D05", "raw": [MASTER], "parsed": [MASTER],
                "worker_name_snapshot": "원작업자", "worker_id": None}
    pending_start(app, tmp_path, {"worker_name": "김철", "worker_id": BOB}, current=restored)
    assert app.opened_settings == [True]
    assert not app._roster_worker_allowed(), "a restored set no longer skips the picker"
    app._play_sound = Mock()
    scan(app, products()[0])
    app._play_sound.assert_any_call("fail")
    assert app.current_set_info["raw"] == [MASTER]
    assert app._record_owner_name() == "원작업자"
    app.current_set_info = {"id": "SET-OLD", "raw": [MASTER]}
    assert app._record_owner_name() == module.WORKER_UNDECIDED_LABEL
    app.current_set_info = restored
    with pytest.raises(AdminPinError):
        app._package_pin_operator()
    enable_save(app, tmp_path)
    try:
        app._save_settings_and_close(object(), "")
        assert app._worker_pick_pending and app.worker_name == ""
        app._save_settings_and_close(object(), "Nguyễn Thị Mai")  # first pick with an active set
        assert app.worker_name == "Nguyễn Thị Mai" and app.worker_id == ALICE
        assert app.current_set_info["id"] == "SET-D05"
        assert app.current_set_info["worker_name_snapshot"] == "원작업자"
        assert app._roster_worker_allowed()
        assert app._package_pin_operator()["local_id"]
        app._save_settings_and_close(object(), "김철")  # any later change stays blocked
        assert app.worker_name == "Nguyễn Thị Mai"
        app.data_manager.log_event(app.Events.SET_RESTORED,
                                   {"set_id": "SET-D05", "continued_by": app._record_owner_name()})
        # set-bound records keep the set's own owner; the detail names who continues
        got = rows(app, app.Events.SET_RESTORED)
        assert [r["worker_name"] for r in got] == ["원작업자"]
        assert json.loads(got[0]["details"])["continued_by"] == "Nguyễn Thị Mai"
    finally:
        app.data_manager.close(timeout=None)


def test_pin_action_before_picking_says_pick_first(packaging, tmp_path):
    app, _ = packaging
    pending_start(app, tmp_path, {"worker_name": "김철", "worker_id": BOB},
                  current={"id": "S1", "raw": [MASTER], "worker_name_snapshot": "원작업자"})
    blocked, _prepared = app._prepare_package_worker_action("LABEL.SET_HOLD", "S1", "")
    assert blocked is True and app._admin_pin_last_message == "작업자를 먼저 고르세요"


def test_after_picking_records_belong_to_the_chosen_person_and_settings_keep_hint(packaging, tmp_path):
    app, _ = packaging
    pending_start(app, tmp_path, {"worker_name": "김철", "worker_id": BOB})
    enable_save(app, tmp_path)
    try:
        app._save_settings_and_close(object(), "Nguyễn Thị Mai")
        assert app.worker_name == "Nguyễn Thị Mai" and app.worker_id == ALICE
        app.data_manager.log_event(app.Events.SET_RESTORED, {"set_id": "X"})
        assert [r["worker_name"] for r in rows(app, app.Events.SET_RESTORED)] == ["Nguyễn Thị Mai"]
        settings = json.loads(Path(app.app_settings_path).read_text(encoding="utf-8"))
        assert settings["worker_name"] == "Nguyễn Thị Mai" and settings["worker_id"] == ALICE
    finally:
        app.data_manager.close(timeout=None)


def test_unpicked_save_keeps_last_name_in_settings(packaging, tmp_path):
    app, _ = packaging
    pending_start(app, tmp_path, {"worker_name": "김철", "worker_id": BOB})
    enable_save(app, tmp_path)
    try:
        app._save_app_settings()
        assert app.app_settings["worker_name"] == "김철" and app.app_settings.get("worker_id") == BOB
    finally:
        app.data_manager.close(timeout=None)
