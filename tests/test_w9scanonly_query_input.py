"""DESIGN §1.7: a scanner Entry returns from lookup panes to the work."""
import pytest

from tests.test_w9scanonly_packaging import packaging, scan  # noqa: F401
from tests.test_legacy_label_transition import MASTER
from tests.test_w9scanonly_failure_input import live_presenter as _live_presenter
from tests.test_label_match_core import _FakeEntry
from tests.test_label_operator_action_gates import FakeTree


def live_presenter(app):
    _live_presenter(app)
    app.history_tree = FakeTree()


class Panes:
    def __init__(self):
        self.selected = "history"

    def select(self, pane=None):
        if pane is not None:
            self.selected = str(pane)
        return self.selected


@pytest.mark.parametrize("past_date", [False, True])
@pytest.mark.parametrize("pane", ["history", "progress"])
def test_query_pane_scan_returns_to_work_and_starts_the_original_label(packaging, past_date, pane):
    app, dialogs = packaging
    live_presenter(app)
    app.operator_notebook = Panes()
    app.operator_notebook.selected = pane
    app.session_tab = "work"
    app.history_card = "history"
    app.history_view_updates_active_state = not past_date
    app.global_scanned_set.add("ALREADY-PACKAGED-SYNTHETIC")
    if past_date:
        app.history_load_generation = 7
        app.history_load_pending = True
        app.history_tree.insert("", "end", iid="past", values=("", "past row"), tags=("success",))
    scan(app, MASTER)
    assert app.operator_notebook.select() == "work"
    assert app.history_view_updates_active_state
    assert app.current_set_info["raw"] == [MASTER]
    assert app.entry.get() == "" and app.entry.cget("state") == "normal"
    assert "ALREADY-PACKAGED-SYNTHETIC" in app.global_scanned_set
    assert app._render_operator_workbench().scan_input_enabled
    if past_date:
        assert app.history_load_generation == 8 and not app.history_load_pending
        assert "past" not in app.history_tree.get_children()
    assert dialogs == []


@pytest.mark.parametrize("gate", [None, "active_read", "active_error"])
def test_query_focus_accepts_scanner_but_keeps_active_work_safety_gates(packaging, gate):
    app, _dialogs = packaging
    live_presenter(app)

    class FocusEntry(_FakeEntry):
        focused = False

        def focus_set(self):
            self.focused = True

    app.entry = FocusEntry("")
    app.history_view_updates_active_state = False
    if gate == "active_read":
        app.history_active_load_pending = True
    elif gate == "active_error":
        app._history_active_load_error = {"error": "synthetic current-day read failure"}
    focused = app._focus_scan_entry_if_available()
    assert focused is (gate is None)
    assert app.entry.focused is (gate is None)
    assert app.entry.cget("state") == ("normal" if gate is None else "disabled")


@pytest.mark.parametrize("gate", ["active_read", "active_error"])
def test_query_entry_scan_cannot_clear_an_active_history_safety_gate(packaging, gate):
    app, _dialogs = packaging
    live_presenter(app)
    app.history_view_updates_active_state = False
    app.history_load_generation = 7
    app.history_load_pending = True
    app.global_scanned_set.add("ALREADY-PACKAGED-SYNTHETIC")
    if gate == "active_read":
        app.history_active_load_pending = True
    else:
        app._history_active_load_error = {"error": "synthetic current-day read failure"}
    scan(app, MASTER)
    assert app.current_set_info["raw"] == []
    assert "ALREADY-PACKAGED-SYNTHETIC" in app.global_scanned_set
    assert not app._render_operator_workbench().scan_input_enabled
    if gate == "active_read":
        assert app.history_active_load_pending and app.history_load_generation == 7
    else:
        assert app._history_active_load_error["error"] == "synthetic current-day read failure"


def test_past_query_snapshot_keeps_unpainted_rows_and_drops_cancelled_rows(packaging):
    import queue
    app, _dialogs = packaging
    live_presenter(app)
    row = {"values": ("", "synthetic row"), "tags": ("success",)}

    class Tree:
        def get_children(self):
            return ("keep",)

        def item(self, iid, option):
            assert iid == "keep"
            return row[option]

    class GateReached(RuntimeError):
        pass

    def stop_after_gate():
        raise GateReached()

    app.history_tree = Tree()
    app.history_queue = queue.Queue()
    app.history_load_generation = 7
    app.history_row_details_map = {"keep": {"id": "keep"}, "unpainted": {"id": "unpainted"}}
    app._scanner_active_history_view = {
        "sorted_sets": [(key, row) for key in ("keep", "cancelled", "unpainted")],
    }
    app._history_load_updates_active_state = lambda _date: False
    app._render_operator_workbench = stop_after_gate
    with pytest.raises(GateReached):
        app._load_history_and_rebuild_summary()
    snapshot = app._scanner_active_history_view
    assert {iid for iid, _ in snapshot["sorted_sets"]} == {"keep", "unpainted"}
    assert snapshot["active_row_details"] == app.history_row_details_map
    assert not app.history_view_updates_active_state and app.history_load_pending
