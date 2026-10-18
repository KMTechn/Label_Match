"""D-05: the name left in settings is only a 'last time' hint, never this run's worker."""
import json
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

import Label_Match as module
import label_screen_bridge as bridge
from tests.test_label_worker_roster import ALICE, BOB, SCOPE, answer
from tests.test_w9scanonly_packaging import packaging, products, scan, completions  # noqa: F401
from tests.test_legacy_label_transition import MASTER


def fresh_start(app, tmp_path, settings):
    """Re-run only the startup name logic the way __init__ does."""
    app.app_settings = dict(settings)
    app.app_settings_path = str(tmp_path / "settings" / "app_settings.json")
    app._last_worker_name = str(settings.get("worker_name", "")).strip()
    app.worker_name = ""
    app._worker_pick_pending = True
    app.package_logistics_client = SimpleNamespace(config=SimpleNamespace(authority_scope_id=SCOPE))
    app._init_worker_roster()
    app.opened_settings = []
    app.open_settings_window = lambda: app.opened_settings.append(True)
    app._apply_worker_roster_answer(0, SCOPE, answer())


def test_start_leaves_worker_undecided_and_requires_picking(packaging, tmp_path):
    app, _ = packaging
    fresh_start(app, tmp_path, {"worker_name": "김철", "worker_id": BOB, "recent_worker_ids": [BOB]})
    assert app.worker_name == "" and app.worker_id is None
    assert app.opened_settings == [True], "settings window opens at start"
    assert not app._roster_worker_allowed()
    # last-time ordering stays, and the new screen only marks it as 'last'
    assert [w["worker_id"] for w in app._worker_roster_choices()][0] == BOB
    state = bridge._settings(SimpleNamespace(app=app, module=module, _register=None))
    assert state["required"] is True and state["selected"] == "김철"
    assert app.app_settings["worker_name"] == "김철", "settings value is not erased"


def test_scan_before_picking_is_visibly_refused_and_changes_nothing(packaging, tmp_path):
    app, dialogs = packaging
    fresh_start(app, tmp_path, {"worker_name": app.worker_name, "worker_id": ALICE})
    opened = []
    app.open_settings_window = lambda: opened.append(True)
    app._show_scanner_notice = Mock()
    app._play_sound = Mock()
    scan(app, MASTER)
    app._show_scanner_notice.assert_called_once()
    assert app._show_scanner_notice.call_args.args[0] == "작업자 선택"
    app._play_sound.assert_any_call("fail")
    assert opened == [True] and app.current_set_info["raw"] == [] and not completions(app)


def test_after_picking_records_belong_to_the_chosen_person(packaging, tmp_path):
    app, _ = packaging
    fresh_start(app, tmp_path, {"worker_name": "김철", "worker_id": BOB})
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
    app.summary_tree = app.history_tree = Tree()
    app.data_manager = module.DataManager(app.save_directory, app.Worker.PACKAGING, "", app.unique_id)
    try:
        # Enter on an empty combo saves nothing and the worker stays undecided.
        app._save_settings_and_close(object(), "")
        assert app.worker_name == "" and app._worker_pick_pending
        app._save_settings_and_close(object(), "Nguyễn Thị Mai")
        assert app.worker_name == "Nguyễn Thị Mai" and app.worker_id == ALICE
        assert app._roster_worker_allowed() and not app._worker_pick_pending
        settings = json.loads(Path(app.app_settings_path).read_text(encoding="utf-8"))
        assert settings["worker_name"] == "Nguyễn Thị Mai" and settings["worker_id"] == ALICE
    finally:
        app.data_manager.close(timeout=None)


def test_unsaved_start_keeps_last_name_in_settings_file(packaging, tmp_path):
    app, _ = packaging
    fresh_start(app, tmp_path, {"worker_name": "김철", "worker_id": BOB})
    app.initialized_successfully = True
    app.scale_factor, app.tree_font_size = 1.2, 14
    class Tree:
        def __getitem__(self, name):
            return []
        def column(self, *a):
            return 10
    app.summary_tree = app.history_tree = Tree()
    app._save_app_settings()
    assert app.app_settings["worker_name"] == "김철" and app.app_settings.get("worker_id") == BOB
