from __future__ import annotations

from copy import deepcopy
import inspect
import queue
import re

import pytest

from Label_Match import Label_Match
from ui.workflow_view_state import WorkflowNotice, WorkflowSnapshot, present_workflow


class FakeWidget:
    def __init__(self):
        self.options = {}
        self.mapped = True
        self.focused = False

    def configure(self, **kwargs):
        self.options.update(kwargs)

    config = configure

    def grid(self, **kwargs):
        self.mapped = True

    def grid_remove(self):
        self.mapped = False

    def focus_set(self):
        self.focused = True

    def __setitem__(self, key, value):
        self.options[key] = value


class FakeTree(FakeWidget):
    def __init__(self):
        super().__init__()
        self.rows = {}
        self.order = []

    def get_children(self, item=""):
        return tuple(self.order)

    def delete(self, *items):
        for item in items:
            key = str(item)
            self.rows.pop(key, None)
            if key in self.order:
                self.order.remove(key)

    def insert(self, parent, index, iid=None, **kwargs):
        row_id = str(iid if iid is not None else f"row-{len(self.order)}")
        self.rows[row_id] = dict(kwargs)
        self.order.append(row_id)
        return row_id


class FakeNotebook:
    def __init__(self, *tabs, selected=None):
        self._tabs = [str(tab) for tab in tabs]
        self.selected = str(selected) if selected is not None else None
        self.tab_options = {}

    def tabs(self):
        return tuple(self._tabs)

    def add(self, frame, **kwargs):
        frame_id = str(frame)
        if frame_id not in self._tabs:
            self._tabs.append(frame_id)
        self.tab_options.setdefault(frame_id, {}).update(kwargs)

    def hide(self, frame):
        frame_id = str(frame)
        if frame_id in self._tabs:
            self._tabs.remove(frame_id)

    def select(self, frame):
        self.selected = str(frame)

    def tab(self, frame, **kwargs):
        frame_id = str(frame)
        self.tab_options.setdefault(frame_id, {}).update(kwargs)


def _current_state(raw=(), parsed=(), **overrides):
    state = {
        "id": "set-1" if raw else None,
        "raw": list(raw),
        "parsed": list(parsed),
        "start_time": None,
        "error_count": 0,
        "has_error_or_reset": False,
        "phase": "A",
        "item_name_override": None,
        "production_date": None,
        "sealed_transfer": None,
        "exact_rescan_active": False,
        "exact_rescan_complete": False,
        "exact_rescan_target_count": 0,
        "exact_rescan_source_bundle_id": "",
        "exact_rescan_barcodes": [],
    }
    state.update(overrides)
    return state


def _render_app(raw=(), parsed=(), **current_overrides):
    app = Label_Match.__new__(Label_Match)
    app.current_set_info = _current_state(raw, parsed, **current_overrides)
    app.operator_workbench_ready = True
    app.initialized_successfully = True
    app.history_view_updates_active_state = True
    app.history_active_load_pending = False
    app.items_data = {"ITEM-001": {"Item Name": "테스트 품목", "Spec": "S"}}
    app.colors = {
        "primary": "#2563EB",
        "success": "#047857",
        "danger": "#B91C1C",
        "text": "#111827",
        "text_subtle": "#6B7280",
    }

    app._workflow_completion_kind = None
    app._workflow_display_scans = ()
    app._workflow_display_parsed_scans = ()
    app._workflow_last_normal_override = None
    app._workflow_blocking_notice = None
    app._workflow_notice = None
    app._workflow_recovered = False
    app._workflow_item_snapshot = None

    app.view_mode_label = FakeWidget()
    app.big_display_label = FakeWidget()
    app.progress_bar = FakeWidget()
    app.qa_scan_tree = FakeTree()
    app.exact_rescan_tree = FakeTree()
    app.qa_scan_frame = "qa"
    app.exact_rescan_frame = "exact"
    app.live_scan_notebook = FakeNotebook("qa", "exact", selected="exact")
    app.entry = FakeWidget()
    app.manual_complete_button = FakeWidget()
    app.exact_rescan_button = FakeWidget()
    app.reset_button = FakeWidget()
    app.cancel_tray_button = FakeWidget()
    return app


def _slot_values(view):
    return tuple(slot.value for slot in view.slots if slot.filled)


def _rendered_tree_values(app):
    return tuple(app.qa_scan_tree.rows[iid]["values"][1] for iid in app.qa_scan_tree.order)


def test_live_center_slots_keep_raw_snapshot_but_render_compact_values_without_mutation():
    raw = (
        "RAW-MASTER|CLC=ITEM-001|PHS=A",
        "RAW-PRODUCT-0001-ITEM-001",
        "RAW-PRODUCT-0002-ITEM-001",
    )
    parsed = ("ITEM-001", "ITEM-001", "ITEM-001")
    app = _render_app(raw, parsed)
    before = deepcopy(app.current_set_info)

    view = app._render_operator_workbench()

    assert _slot_values(view) == raw
    rendered = _rendered_tree_values(app)[: len(raw)]
    assert rendered[0] == "ITEM-001"
    assert all(value.startswith("ITEM-001 · ID #") for value in rendered[1:])
    assert all(value not in raw for value in rendered)
    assert tuple(
        app._qa_scan_detail_rows[f"qa-slot-{index}"]["raw"]
        for index in range(1, len(raw) + 1)
    ) == raw
    assert not any(value in parsed for value in _slot_values(view))
    assert app.current_set_info == before


def test_completion_snapshot_keeps_raw_display_and_parsed_business_identity_separate():
    raw = (
        "RAW-MASTER|CLC=ITEM-001|PHS=A",
        "RAW-PRODUCT-0001-ITEM-001",
        "RAW-PRODUCT-0002-ITEM-001",
        "RAW-PRODUCT-0003-ITEM-001",
        "RAW-FINAL-LABEL-ITEM-001-6D20260715",
    )
    parsed = ("ITEM-001",) * 5
    app = _render_app(raw, parsed)
    before = deepcopy(app.current_set_info)

    assert app._publish_workflow_completion("full") == "full"

    assert app.current_set_info == before
    assert app._workflow_display_scans == raw
    assert app._workflow_display_parsed_scans == parsed
    assert app._workflow_item_snapshot["item_code"] == "ITEM-001"

    # Business finalization resets current_set_info after publishing.  The
    # detached view-only snapshot must keep accepted raw detail while the list
    # renders compact values, without writing either back into the new set.
    app.current_set_info = _current_state()
    reset_before = deepcopy(app.current_set_info)
    view = app._render_operator_workbench()

    assert _slot_values(view) == raw
    rendered = _rendered_tree_values(app)
    assert rendered[0] == "ITEM-001"
    assert all(value.startswith("ITEM-001 · ID #") for value in rendered[1:])
    assert all(value not in raw for value in rendered)
    assert tuple(
        app._qa_scan_detail_rows[f"qa-slot-{index}"]["raw"]
        for index in range(1, len(raw) + 1)
    ) == raw
    assert app.current_set_info == reset_before


def _action_app(view):
    app = Label_Match.__new__(Label_Match)
    app._last_workflow_view = view
    app.current_set_info = {"sentinel": ["unchanged"]}
    app.calls = []
    app._reset_current_set = lambda **kwargs: app.calls.append(("f1", kwargs))
    app._prompt_and_cancel_completed_tray = lambda: app.calls.append(("f2", {}))
    app._prompt_manual_complete = lambda: app.calls.append(("f3", {}))
    app._prompt_exact_rescan = lambda: app.calls.append(("f4", {}))
    return app


@pytest.mark.parametrize(
    "blocked_view",
    [
        present_workflow(
            WorkflowSnapshot(
                qa_scans=("M", "P1", "P2", "P3", "F"),
                blocking_notice=WorkflowNotice(
                    "중앙 제출 보류",
                    "서버 확인 전에는 현재 세트를 유지합니다.",
                    kind="submission_blocked",
                ),
            )
        ),
        present_workflow(
            WorkflowSnapshot(qa_scans=("M", "P1"), has_error=True)
        ),
        present_workflow(
            WorkflowSnapshot(qa_scans=("M", "P1"), history_readonly=True)
        ),
    ],
    ids=("submission_blocked", "error", "history_readonly"),
)
@pytest.mark.parametrize("action", ("f1", "f2", "f3", "f4"))
def test_shortcut_handler_blocks_every_action_when_presenter_disables_it(
    blocked_view,
    action,
):
    app = _action_app(blocked_view)
    before = deepcopy(app.current_set_info)

    handled = Label_Match._handle_workflow_shortcut(app, action)

    assert handled == "break"
    assert app.calls == []
    assert app.current_set_info == before


@pytest.mark.parametrize(
    ("action", "view", "expected"),
    [
        (
            "f1",
            present_workflow(WorkflowSnapshot(qa_scans=("M", "P1"))),
            ("f1", {"full_reset": True}),
        ),
        (
            "f2",
            present_workflow(WorkflowSnapshot()),
            ("f2", {}),
        ),
        (
            "f3",
            present_workflow(WorkflowSnapshot(qa_scans=("M", "P1"))),
            ("f3", {}),
        ),
        (
            "f4",
            present_workflow(WorkflowSnapshot(qa_scans=("M",))),
            ("f4", {}),
        ),
    ],
)
def test_shortcut_handler_calls_only_the_presenter_enabled_action(action, view, expected):
    app = _action_app(view)

    handled = Label_Match._handle_workflow_shortcut(app, action)

    assert handled == "break"
    assert app.calls == [expected]


@pytest.mark.parametrize("sequence", ("<Return>", "<KP_Enter>"))
def test_notice_action_button_binds_both_enter_keys_to_acknowledgement(sequence):
    source = inspect.getsource(Label_Match._create_widgets)
    callback_pattern = re.compile(
        rf"workflow_notice_action_button\.bind\(\s*['\"]{re.escape(sequence)}['\"]"
        rf"\s*,\s*self\._acknowledge_workflow_notice\s*\)",
        re.MULTILINE,
    )

    assert callback_pattern.search(source), f"missing {sequence} acknowledgement binding"


def test_f4_complete_keeps_completed_exact_list_visible_in_center():
    app = _render_app(
        ("RAW-MASTER",),
        ("ITEM-001",),
        exact_rescan_complete=True,
        exact_rescan_target_count=2,
        exact_rescan_barcodes=["EXACT-1", "EXACT-2"],
    )
    before = deepcopy(app.current_set_info)

    view = app._render_operator_workbench()

    assert view.exact_rescan.status == "complete"
    assert "exact" in app.live_scan_notebook.tabs()
    assert app.live_scan_notebook.selected == "exact"
    assert app.current_set_info == before


def test_f4_complete_returns_to_qa_list_after_next_qa_scan_is_accepted():
    app = _render_app(
        ("RAW-MASTER", "RAW-PRODUCT-1"),
        ("ITEM-001", "ITEM-001"),
        exact_rescan_complete=True,
        exact_rescan_target_count=2,
        exact_rescan_barcodes=["EXACT-1", "EXACT-2"],
    )
    before = deepcopy(app.current_set_info)

    view = app._render_operator_workbench()

    assert view.exact_rescan.status == "complete"
    assert "exact" in app.live_scan_notebook.tabs()
    assert app.live_scan_notebook.selected == "qa"
    assert view.last_normal_scan == "RAW-PRODUCT-1"
    assert app.current_set_info == before


def test_f4_active_selects_the_separate_exact_rescan_tab():
    app = _render_app(
        ("RAW-MASTER",),
        ("ITEM-001",),
        exact_rescan_active=True,
        exact_rescan_target_count=2,
        exact_rescan_barcodes=["EXACT-1"],
    )

    view = app._render_operator_workbench()

    assert view.exact_rescan.status == "active"
    assert app.live_scan_notebook.selected == "exact"
    assert _slot_values(view) == ("RAW-MASTER",)


def _fresh_gate_app(*, cached_view, fresh_view):
    app = _action_app(cached_view)
    app.operator_workbench_ready = True
    app.initialized_successfully = True
    app.entry = FakeWidget()
    app.render_calls = []

    def render():
        app.render_calls.append("render")
        app._last_workflow_view = fresh_view
        return fresh_view

    app._render_operator_workbench = render
    return app


@pytest.mark.parametrize(
    "fresh_view",
    (
        present_workflow(
            WorkflowSnapshot(qa_scans=("M",), history_loading=True)
        ),
        present_workflow(
            WorkflowSnapshot(qa_scans=("M",), history_readonly=True)
        ),
    ),
    ids=("history_loading", "history_readonly"),
)
@pytest.mark.parametrize(
    ("action", "cached_enabled"),
    (
        ("f1", present_workflow(WorkflowSnapshot(qa_scans=("M",)))),
        ("f2", present_workflow(WorkflowSnapshot(qa_scans=("M",)))),
        ("f3", present_workflow(WorkflowSnapshot(qa_scans=("M", "P1")))),
        ("f4", present_workflow(WorkflowSnapshot(qa_scans=("M",)))),
    ),
)
def test_shortcut_handler_refreshes_stale_enabled_view_before_action(
    fresh_view,
    action,
    cached_enabled,
):
    app = _fresh_gate_app(cached_view=cached_enabled, fresh_view=fresh_view)
    before = deepcopy(app.current_set_info)

    handled = Label_Match._handle_workflow_shortcut(app, action)

    assert handled == "break"
    assert app.render_calls == ["render"]
    assert app.calls == []
    assert app.current_set_info == before


@pytest.mark.parametrize(
    "fresh_view",
    (
        present_workflow(
            WorkflowSnapshot(qa_scans=("M",), history_loading=True)
        ),
        present_workflow(
            WorkflowSnapshot(qa_scans=("M",), history_readonly=True)
        ),
    ),
    ids=("history_loading", "history_readonly"),
)
def test_scan_enter_refreshes_stale_enabled_view_before_processing(fresh_view):
    cached_enabled = present_workflow(WorkflowSnapshot(qa_scans=("M",)))
    app = _fresh_gate_app(cached_view=cached_enabled, fresh_view=fresh_view)
    app.process_calls = []
    app.process_input = lambda event=None: app.process_calls.append(event)

    handled = Label_Match._handle_scan_enter(app, event="scan-enter")

    assert handled == "break"
    assert app.render_calls == ["render"]
    assert app.process_calls == []


@pytest.mark.parametrize(
    "fresh_view",
    (
        present_workflow(
            WorkflowSnapshot(qa_scans=("M",), history_loading=True)
        ),
        present_workflow(
            WorkflowSnapshot(qa_scans=("M",), history_readonly=True)
        ),
    ),
    ids=("history_loading", "history_readonly"),
)
def test_focus_scan_entry_refreshes_stale_enabled_view_before_focus(fresh_view):
    cached_enabled = present_workflow(WorkflowSnapshot(qa_scans=("M",)))
    app = _fresh_gate_app(cached_view=cached_enabled, fresh_view=fresh_view)

    focused = Label_Match._focus_scan_entry_if_available(app)

    assert focused is False
    assert app.render_calls == ["render"]
    assert app.entry.focused is False


def test_history_load_renders_gate_immediately_after_pending_flags_are_set():
    class StopAfterRender(RuntimeError):
        pass

    app = Label_Match.__new__(Label_Match)
    app.history_load_generation = 0
    app.history_queue = queue.Queue()
    app.operator_workbench_ready = True
    app._history_load_updates_active_state = lambda target_date=None: True
    observed = []

    def stop_render():
        observed.append(
            (
                app.history_load_pending,
                app.history_active_load_pending,
                app.history_view_updates_active_state,
            )
        )
        raise StopAfterRender

    app._render_operator_workbench = stop_render

    with pytest.raises(StopAfterRender):
        Label_Match._load_history_and_rebuild_summary(app)

    assert observed == [(True, True, True)]
