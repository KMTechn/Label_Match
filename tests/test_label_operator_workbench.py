from __future__ import annotations

import copy
import inspect
import os
import textwrap

import pytest

import Label_Match as label_match_module
from Label_Match import Label_Match


class FakeWidget:
    """Small Tk-shaped recorder; it never creates a Tcl interpreter."""

    def __init__(self, master=None, *args, kind="Widget", **kwargs):
        self.master = master
        self.kind = kind
        self.options = dict(kwargs)
        self.children = []
        self.grid_options = {}
        self.pack_options = {}
        self.place_options = {}
        self.grid_rows = {}
        self.grid_columns = {}
        self.bindings = []
        self.notebook_pages = []
        self.rows = {}
        self._row_order = []
        self._mapped = False
        self._next_iid = 0
        self._selection = []
        self._focus_iid = ""
        self.focused = False
        if isinstance(master, FakeWidget):
            master.children.append(self)

    def grid(self, **kwargs):
        self.grid_options.update(kwargs)
        self._mapped = True

    def grid_configure(self, **kwargs):
        self.grid_options.update(kwargs)

    def grid_remove(self):
        self._mapped = False

    grid_forget = grid_remove

    def pack(self, **kwargs):
        self.pack_options.update(kwargs)
        self._mapped = True

    def place(self, **kwargs):
        self.place_options.update(kwargs)
        self._mapped = True

    def place_configure(self, **kwargs):
        self.place_options.update(kwargs)

    def grid_rowconfigure(self, row, **kwargs):
        self.grid_rows.setdefault(row, {}).update(kwargs)

    rowconfigure = grid_rowconfigure

    def grid_columnconfigure(self, column, **kwargs):
        self.grid_columns.setdefault(column, {}).update(kwargs)

    columnconfigure = grid_columnconfigure

    def configure(self, **kwargs):
        self.options.update(kwargs)

    config = configure

    def cget(self, key):
        return self.options.get(key, "")

    def bind(self, sequence, callback, add=None):
        self.bindings.append((sequence, callback, add))

    def add(self, child, **kwargs):
        if child not in self.children:
            self.children.append(child)
        self.notebook_pages.append((child, dict(kwargs)))

    def heading(self, column, **kwargs):
        self.options.setdefault("headings", {})[column] = dict(kwargs)

    def column(self, column, option=None, **kwargs):
        columns = self.options.setdefault("columns_config", {})
        columns.setdefault(column, {}).update(kwargs)
        if option is not None:
            return columns[column].get(option, 100)
        return columns[column]

    def insert(self, parent, index=None, iid=None, **kwargs):
        # Entry/Text calls use ``insert(index, value)``.  Their contents are
        # irrelevant to these layout assertions, but accepting the shape keeps
        # the fake useful for the existing monolithic widget builder.
        if not isinstance(kwargs, dict) or (not kwargs and iid is None):
            self.options["inserted"] = index
            return None
        row_id = str(iid) if iid is not None else f"row-{self._next_iid}"
        self._next_iid += 1
        self.rows[row_id] = dict(kwargs)
        self._row_order.append(row_id)
        return row_id

    def delete(self, *items):
        if not items:
            return
        if len(items) >= 2 and items[-1] in ("end", label_match_module.tk.END):
            self.rows.clear()
            self._row_order.clear()
            return
        for item in items:
            key = str(item)
            self.rows.pop(key, None)
            if key in self._row_order:
                self._row_order.remove(key)

    def get_children(self, item=""):
        return tuple(self._row_order)

    def item(self, iid, option=None, **kwargs):
        row = self.rows.setdefault(str(iid), {})
        row.update(kwargs)
        if option is not None:
            return row.get(option, ())
        return dict(row)

    def set(self, *args):
        self.options["scroll"] = args

    def yview(self, *args):
        return args

    def xview(self, *args):
        return args

    def yview_moveto(self, fraction):
        self.options["yview_fraction"] = fraction

    def selection(self):
        return tuple(self._selection)

    def selection_set(self, *items):
        self._selection = [str(item) for item in items]

    def selection_remove(self, *items):
        removed = {
            str(item)
            for value in items
            for item in (value if isinstance(value, (tuple, list)) else (value,))
        }
        self._selection = [item for item in self._selection if item not in removed]

    def focus(self, iid=None):
        if iid is not None:
            self._focus_iid = str(iid)
        return self._focus_iid

    def focus_set(self):
        self.focused = True

    def see(self, iid):
        self.options["seen"] = str(iid)

    def tag_configure(self, *args, **kwargs):
        self.options.setdefault("tags", []).append((args, kwargs))

    def add_command(self, **kwargs):
        self.options.setdefault("menu_commands", []).append(kwargs)

    def add_separator(self):
        self.options.setdefault("menu_commands", []).append({"separator": True})

    def winfo_exists(self):
        return True

    def winfo_ismapped(self):
        return self._mapped

    def winfo_width(self):
        return 900

    def winfo_height(self):
        return 900

    def winfo_children(self):
        return list(self.children)

    def __getitem__(self, key):
        return self.options.get(key)

    def __setitem__(self, key, value):
        self.options[key] = value


def _factory(kind):
    def create(master=None, *args, **kwargs):
        return FakeWidget(master, *args, kind=kind, **kwargs)

    return create


def _walk(widget):
    yield widget
    for child in widget.children:
        yield from _walk(child)


def _is_descendant(widget, ancestor):
    current = widget
    while isinstance(current, FakeWidget):
        if current is ancestor:
            return True
        current = current.master
    return False


def _required_widget(app, name):
    assert name in app.__dict__, f"operator workbench must expose self.{name}"
    return app.__dict__[name]


def test_profile_bootstrap_ignores_unrealized_one_pixel_root():
    app = Label_Match.__new__(Label_Match)
    app.winfo_width = lambda: 1
    app.winfo_height = lambda: 1
    app.winfo_screenwidth = lambda: 2560
    app.winfo_screenheight = lambda: 1392
    app._screen_diagonal_inches = lambda: None

    name, profile = app._select_ui_profile()

    assert name == "standard"
    assert profile is Label_Match.UI_PROFILES["standard"]


@pytest.fixture
def operator_workbench(monkeypatch):
    for name in ("Frame", "Label", "Text", "Menu", "Listbox"):
        monkeypatch.setattr(label_match_module.tk, name, _factory(f"tk.{name}"))
    for name in (
        "Frame",
        "Label",
        "Button",
        "Entry",
        "Progressbar",
        "PanedWindow",
        "Scrollbar",
        "Treeview",
        "Notebook",
    ):
        monkeypatch.setattr(label_match_module.ttk, name, _factory(f"ttk.{name}"))

    app = Label_Match.__new__(Label_Match)
    app.ui_profile = dict(Label_Match.UI_PROFILES["standard"])
    app.default_font_name = "Malgun Gothic"
    app.colors = {
        "background": "#f3f4f6",
        "card_background": "#ffffff",
        "text": "#111827",
        "text_subtle": "#6b7280",
        "primary": "#2563eb",
        "success": "#15803d",
        "success_light": "#dcfce7",
        "danger": "#b91c1c",
        "border": "#d1d5db",
    }
    app.hist_proportions = {"Set": 4, **{f"Input{i}": 10 for i in range(1, 6)}, "Result": 8, "Timestamp": 14}
    app.summary_proportions = {"Code": 70, "Phase": 12, "Count": 18}
    app.current_set_info = {
        "id": None,
        "raw": [],
        "parsed": [],
        "has_error_or_reset": False,
        "sealed_transfer": None,
        "exact_rescan_active": False,
        "exact_rescan_complete": False,
        "exact_rescan_target_count": 0,
        "exact_rescan_barcodes": [],
    }
    app.initialized_successfully = True
    app.history_view_updates_active_state = True
    app.history_active_load_pending = False
    app.run_tests = True
    app._root_bindings = []
    app.bind = lambda sequence, callback, add=None: app._root_bindings.append((sequence, callback, add))
    app.after = lambda delay, callback=None, *args: "after-fixture"
    app.winfo_width = lambda: 1440
    app.winfo_height = lambda: 900
    app.winfo_screenwidth = lambda: 1440
    app._apply_responsive_layout = lambda: None

    app._create_widgets()
    return app


def test_workbench_keeps_three_panes_persistent_and_scan_input_in_center(operator_workbench):
    app = operator_workbench
    workbench = _required_widget(app, "operator_workbench_frame")
    left = _required_widget(app, "operator_left_pane")
    center = _required_widget(app, "operator_center_pane")
    right = _required_widget(app, "operator_right_pane")
    notice = _required_widget(app, "workflow_notice_frame")
    qa_tree = _required_widget(app, "qa_scan_tree")
    exact_frame = _required_widget(app, "exact_rescan_frame")
    exact_tree = _required_widget(app, "exact_rescan_tree")
    exact_detail = _required_widget(app, "exact_rescan_detail_frame")
    panes = (left, center, right)

    assert workbench.master is app.main_frame
    assert {pane.master for pane in panes} == {workbench}
    assert all(pane.winfo_ismapped() for pane in panes)
    assert _is_descendant(app.entry, center)
    assert _is_descendant(notice, center)
    assert _is_descendant(qa_tree, center)
    assert _is_descendant(exact_frame, center)
    assert _is_descendant(exact_tree, exact_frame)
    assert _is_descendant(exact_detail, exact_frame)


def test_center_has_one_five_step_rail_and_separate_conditional_f4_list(operator_workbench):
    app = operator_workbench
    center = _required_widget(app, "operator_center_pane")
    qa_tree = _required_widget(app, "qa_scan_tree")
    exact_frame = _required_widget(app, "exact_rescan_frame")
    exact_tree = _required_widget(app, "exact_rescan_tree")

    assert len(app.step_labels) == app.TOTAL_SCAN_COUNT == 5
    assert all(_is_descendant(label, center) for label in app.step_labels)
    assert qa_tree is not exact_tree
    assert qa_tree.master is not exact_tree.master
    assert exact_tree.master is exact_frame
    assert not _is_descendant(app.history_tree, center)
    assert not _is_descendant(app.summary_tree, center)


def test_right_notebook_preserves_session_history_and_summary_on_same_screen(operator_workbench):
    app = operator_workbench
    right = _required_widget(app, "operator_right_pane")
    notebook = _required_widget(app, "operator_notebook")
    session_tab = _required_widget(app, "operator_session_tab")
    history_tab = _required_widget(app, "operator_history_tab")
    summary_tab = _required_widget(app, "operator_summary_tab")

    assert _is_descendant(notebook, right)
    assert [page for page, _ in notebook.notebook_pages] == [
        session_tab,
        history_tab,
        summary_tab,
    ]
    tab_text = [options.get("text", "") for _, options in notebook.notebook_pages]
    assert tab_text == ["이번 세션", "스캔 기록", "통과 요약"]
    assert _is_descendant(app.history_tree, history_tab)
    assert _is_descendant(app.summary_tree, summary_tab)


def test_four_existing_actions_form_a_two_by_two_grid_in_right_pane(operator_workbench):
    app = operator_workbench
    action_frame = _required_widget(app, "operator_action_frame")
    right = _required_widget(app, "operator_right_pane")
    buttons = (
        app.manual_complete_button,
        app.exact_rescan_button,
        app.reset_button,
        app.cancel_tray_button,
    )

    assert {button.master for button in buttons} == {action_frame}
    assert _is_descendant(action_frame, right)
    assert [(button.grid_options.get("row"), button.grid_options.get("column")) for button in buttons] == [
        (0, 0),
        (0, 1),
        (1, 0),
        (1, 1),
    ]


def test_workbench_renderer_uses_snapshot_adapter_then_pure_presenter():
    source = textwrap.dedent(inspect.getsource(Label_Match._render_operator_workbench))

    assert "adapt_workflow_snapshot(" in source
    assert "present_workflow(" in source
    assert source.index("adapt_workflow_snapshot(") < source.index("present_workflow(")


def test_responsive_layout_does_not_drain_tk_events_from_configure_callback():
    source = inspect.getsource(Label_Match._apply_operator_responsive_layout)

    assert "update_idletasks" not in source
    assert ".update()" not in source


def test_renderer_populates_actual_accepted_qa_rows_and_keeps_f4_rows_separate(operator_workbench):
    app = operator_workbench
    assert hasattr(Label_Match, "_render_operator_workbench")
    qa_scans = ["MASTER-001", "PRODUCT-001", "PRODUCT-002"]
    exact_scans = ["EXACT-001", "EXACT-002"]
    app.current_set_info.update(
        {
            "raw": list(qa_scans),
            "parsed": list(qa_scans),
            "exact_rescan_active": False,
            "exact_rescan_complete": True,
            "exact_rescan_target_count": 2,
            "exact_rescan_barcodes": list(exact_scans),
        }
    )

    app._render_operator_workbench()

    qa_text = repr(app.qa_scan_tree.rows)
    exact_text = repr(app.exact_rescan_tree.rows)
    assert all(scan in qa_text for scan in qa_scans)
    assert all(scan not in qa_text for scan in exact_scans)
    assert all(scan in exact_text for scan in exact_scans)
    assert app.qa_scan_tree.winfo_ismapped() is True
    assert app.exact_rescan_frame.winfo_ismapped() is True


def test_live_qa_list_exposes_readonly_wrapped_selected_raw_detail(operator_workbench):
    app = operator_workbench
    detail_frame = _required_widget(app, "qa_scan_detail_frame")
    detail_text = _required_widget(app, "qa_scan_detail_text")
    detail_scrollbar = _required_widget(app, "qa_scan_detail_scrollbar")

    assert _is_descendant(detail_frame, app.operator_center_pane)
    assert _is_descendant(detail_text, detail_frame)
    assert _is_descendant(detail_scrollbar, detail_frame)
    assert detail_text.cget("wrap") == "char"
    assert detail_text.cget("takefocus") == 0
    assert detail_text.cget("state") == "disabled"
    assert any(
        sequence == "<<TreeviewSelect>>"
        for sequence, _callback, _add in app.qa_scan_tree.bindings
    )


def test_selected_qa_detail_keeps_full_raw_value_and_selection_across_rerender(
    operator_workbench,
):
    app = operator_workbench
    master = "MASTER|CLC=ITEM-001|PHS=A"
    long_product = "PRODUCT|" + "X" * 240 + "\x1d6D20260715|END"
    app.current_set_info.update(
        {
            "raw": [master, long_product],
            "parsed": ["ITEM-001", "ITEM-001"],
        }
    )

    app._render_operator_workbench()

    assert app.qa_scan_tree.selection() == ("qa-slot-2",)
    displayed_product = app.qa_scan_tree.item("qa-slot-2", "values")[1]
    assert displayed_product != long_product
    assert "..." in displayed_product
    assert "단계: 2. 제품1" in app.qa_scan_detail_metadata_label.cget("text")
    assert "상태: 완료" in app.qa_scan_detail_metadata_label.cget("text")
    assert app.qa_scan_detail_text.options["inserted"] == long_product

    app.qa_scan_tree.selection_set("qa-slot-1")
    app._on_qa_scan_selection_changed()
    assert app.qa_scan_detail_text.options["inserted"] == master
    assert app.entry.focused is False

    product_2 = "PRODUCT-2|" + "Y" * 180
    app.current_set_info["raw"].append(product_2)
    app.current_set_info["parsed"].append("ITEM-001")
    app._render_operator_workbench()

    assert app.qa_scan_tree.selection() == ("qa-slot-1",)
    assert app.qa_scan_detail_text.options["inserted"] == master

    # Treeview emits the same virtual event for mouse selection and arrow-key
    # navigation, so the complete raw value follows keyboard row movement too.
    app.qa_scan_tree.selection_set("qa-slot-3")
    app._on_qa_scan_selection_changed()
    assert app.qa_scan_detail_text.options["inserted"] == product_2


def test_f4_list_exposes_selected_full_raw_without_stealing_scan_focus(
    operator_workbench,
):
    app = operator_workbench
    detail_frame = _required_widget(app, "exact_rescan_detail_frame")
    detail_text = _required_widget(app, "exact_rescan_detail_text")
    detail_scrollbar = _required_widget(app, "exact_rescan_detail_scrollbar")
    first = "PHS|F4|FIRST|" + "A" * 240
    second = "PHS|F4|SECOND|" + "B" * 240
    app.entry.focused = True
    focus_before = app.entry.focused

    assert _is_descendant(detail_frame, app.exact_rescan_frame)
    assert _is_descendant(detail_text, detail_frame)
    assert _is_descendant(detail_scrollbar, detail_frame)
    assert detail_text.cget("wrap") == "char"
    assert detail_text.cget("takefocus") == 0
    assert detail_text.cget("state") == "disabled"
    assert any(
        sequence == "<<TreeviewSelect>>"
        for sequence, _callback, _add in app.exact_rescan_tree.bindings
    )

    app.current_set_info.update(
        {
            "exact_rescan_active": True,
            "exact_rescan_complete": False,
            "exact_rescan_target_count": 2,
            "exact_rescan_barcodes": [first, second],
        }
    )
    business_state_before = copy.deepcopy(app.current_set_info)
    app._render_operator_workbench()

    first_display = app.exact_rescan_tree.rows["exact-slot-1"]["values"][1]
    second_display = app.exact_rescan_tree.rows["exact-slot-2"]["values"][1]
    assert "..." in first_display and first_display != first
    assert "..." in second_display and second_display != second
    assert app.exact_rescan_tree.selection() == ("exact-slot-2",)
    assert app.exact_rescan_detail_metadata_label.cget("text") == "순서: 2"
    assert detail_text.options["inserted"] == second
    assert app.entry.focused == focus_before
    assert app.current_set_info == business_state_before

    app.exact_rescan_tree.selection_set("exact-slot-1")
    app._on_exact_rescan_selection_changed()
    assert app.exact_rescan_detail_metadata_label.cget("text") == "순서: 1"
    assert detail_text.options["inserted"] == first
    assert app.entry.focused == focus_before
    assert app.current_set_info == business_state_before

    app._render_operator_workbench()
    assert app.exact_rescan_tree.selection() == ("exact-slot-1",)
    assert detail_text.options["inserted"] == first
    assert app.entry.focused == focus_before
    assert app.current_set_info == business_state_before


def test_operator_notice_compacts_to_reason_key_value_and_next_action(
    operator_workbench,
):
    app = operator_workbench
    message = (
        "현품표와 제품이 불일치합니다.\n\n"
        "- 현품표: MASTER-001\n"
        "- 스캔 제품: PRODUCT-999\n\n"
        "→ 제품을 제거하고 새 현품표부터 다시 스캔하세요."
    )

    compact = app._compact_operator_notice_message(message)

    assert compact.splitlines() == [
        "현품표와 제품이 불일치합니다.",
        "- 스캔 제품: PRODUCT-999",
        "→ 제품 제거 후 확인 → 새 현품표 스캔",
    ]
    assert "MASTER-001" not in compact


def test_qa_detail_handles_empty_completion_and_recovery_views(operator_workbench):
    app = operator_workbench
    app._render_operator_workbench()

    assert app.qa_scan_tree.selection() == ()
    assert app.qa_scan_detail_metadata_label.cget("text") == "단계: -  |  상태: -"
    assert "행을 선택" in app.qa_scan_detail_text.options["inserted"]

    completed = tuple(f"RAW-{index}-" + "Z" * 80 for index in range(1, 6))
    app._workflow_completion_kind = "full"
    app._workflow_display_scans = completed
    app._workflow_display_parsed_scans = ("ITEM-001",) * 5
    app._render_operator_workbench()
    assert app.qa_scan_tree.selection() == ("qa-slot-5",)
    assert app.qa_scan_detail_text.options["inserted"] == completed[-1]

    app._workflow_completion_kind = None
    app._workflow_display_scans = ()
    app._workflow_display_parsed_scans = ()
    app._workflow_recovered = True
    app.current_set_info.update(
        {"raw": ["RECOVERED-MASTER"], "parsed": ["ITEM-001"]}
    )
    app.qa_scan_tree.selection_remove(app.qa_scan_tree.selection())
    app._render_operator_workbench()
    assert app.qa_scan_tree.selection() == ("qa-slot-1",)
    assert app.qa_scan_detail_text.options["inserted"] == "RECOVERED-MASTER"


def test_f4_list_hides_reversibly_without_replacing_live_qa_rows(operator_workbench):
    app = operator_workbench
    assert hasattr(Label_Match, "_render_operator_workbench")
    qa_scans = ["MASTER-001"]
    app.current_set_info.update({"raw": list(qa_scans), "parsed": list(qa_scans)})

    app._render_operator_workbench()
    before = repr(app.qa_scan_tree.rows)
    assert app.exact_rescan_frame.winfo_ismapped() is False

    app.current_set_info.update(
        {
            "exact_rescan_active": True,
            "exact_rescan_target_count": 2,
            "exact_rescan_barcodes": ["EXACT-001"],
        }
    )
    app._render_operator_workbench()
    assert app.exact_rescan_frame.winfo_ismapped() is True

    app.current_set_info.update(
        {
            "exact_rescan_active": False,
            "exact_rescan_complete": False,
            "exact_rescan_target_count": 0,
            "exact_rescan_barcodes": [],
        }
    )
    app._render_operator_workbench()

    assert app.exact_rescan_frame.winfo_ismapped() is False
    assert repr(app.qa_scan_tree.rows) == before


@pytest.mark.skipif(os.name != "nt", reason="Label Match is a Windows Tk application")
def test_live_submission_retry_keeps_full_server_error_and_five_scan_rows(
    tmp_path,
    monkeypatch,
):
    """Guard the real retry notice geometry, not the action-free capture fixture."""

    from tools.capture_label_operator_ui import (
        _apply_scale,
        _configure_size,
        _make_capture_app,
        _wait_until_ready,
        build_isolated_app_settings,
        pump_tk,
    )

    data_root = tmp_path / "label_match_live_submission"
    temp_root = data_root / "temp"
    temp_root.mkdir(parents=True)
    guards = {
        "LABEL_MATCH_SAVE_DIR": str(data_root),
        "LABEL_MATCH_AUTOMATED_TEST": "1",
        "LABEL_MATCH_AUDIO_ENABLED": "off",
        "LABEL_MATCH_DIRECT_SYNC_BOOTSTRAP": "off",
        "LABEL_MATCH_SESSION_SYNC_TRIGGER": "off",
        "LABEL_MATCH_UPDATE_PROVIDER": "off",
        "KMTECH_TEST_SILENT_AUDIO": "1",
        "SDL_AUDIODRIVER": "dummy",
        "PYGAME_HIDE_SUPPORT_PROMPT": "1",
        "PYTHONDONTWRITEBYTECODE": "1",
        "TEMP": str(temp_root),
        "TMP": str(temp_root),
    }
    for key, value in guards.items():
        monkeypatch.setenv(key, value)
    for key in tuple(os.environ):
        if key.startswith("LABEL_MATCH_LOGISTICS_") or key.startswith(
            "WORKER_ANALYSIS_LOGISTICS_"
        ):
            monkeypatch.delenv(key, raising=False)

    settings = build_isolated_app_settings(data_root, 1.4)
    app = None
    try:
        app = _make_capture_app(label_match_module, settings)
        _wait_until_ready(app)
        _apply_scale(app, 1.4)
        _configure_size(app, (1366, 768))
        app.entry.focus_set()
        pump_tk(app, 120)
        assert app.focus_get() == app.entry

        scans = (
            "AAA2270730100 · 현품표",
            "AAA2270730100 · 제품 1",
            "AAA2270730100 · 제품 2",
            "AAA2270730100 · 제품 3",
            "AAA2270730100 · 최종 라벨",
        )
        app.current_set_info.update(
            {
                "id": "live-submission-set",
                "raw": list(scans),
                "parsed": list(scans),
                "has_error_or_reset": False,
                "exact_rescan_active": False,
                "exact_rescan_complete": False,
                "exact_rescan_target_count": 0,
                "exact_rescan_barcodes": [],
            }
        )
        server_error = (
            "HTTP 503 Service Unavailable: "
            "중앙 포장 API 연결 시간이 초과되었습니다."
        )

        app._publish_submission_block(server_error)
        pump_tk(app, 260)

        assert bool(app.workflow_notice_action_button.winfo_ismapped()) is True
        assert "제출 재시도" in app.workflow_notice_action_button.cget("text")

        rows = tuple(app.qa_scan_tree.get_children())
        assert len(rows) == 5
        assert all(scan in repr(app.qa_scan_tree.item(row, "values")) for row, scan in zip(rows, scans))
        final_row_box = app.qa_scan_tree.bbox(rows[-1])
        assert final_row_box
        assert final_row_box[1] + final_row_box[3] <= app.qa_scan_tree.winfo_height()

        retry_tree_box = (
            app.qa_scan_tree.winfo_x(),
            app.qa_scan_tree.winfo_y(),
            app.qa_scan_tree.winfo_width(),
            app.qa_scan_tree.winfo_height(),
        )
        retry_notice_height = app.workflow_notice_frame.winfo_height()

        detail_actual = (
            app.qa_scan_detail_text.winfo_width(),
            app.qa_scan_detail_text.winfo_height(),
        )
        detail_requested_height = app.qa_scan_detail_text.winfo_reqheight()
        assert detail_actual[0] > 300
        assert detail_actual[1] >= detail_requested_height
        assert (
            app.qa_scan_detail_text.winfo_y()
            + app.qa_scan_detail_text.winfo_height()
            <= app.qa_scan_detail_frame.winfo_height()
        )

        for button in (
            app.manual_complete_button,
            app.exact_rescan_button,
            app.reset_button,
            app.cancel_tray_button,
        ):
            assert button.winfo_height() >= button.winfo_reqheight()
        action_row_heights = (
            app.operator_action_frame.grid_bbox(0, 0)[3],
            app.operator_action_frame.grid_bbox(0, 1)[3],
        )
        assert all(86 <= height <= 104 for height in action_row_heights)
        assert app.operator_status_frame.winfo_height() <= 32

        notice_text = str(app.workflow_notice_label.cget("text"))
        assert server_error in notice_text
        actual = (
            app.workflow_notice_label.winfo_width(),
            app.workflow_notice_label.winfo_height(),
        )
        requested = (
            app.workflow_notice_label.winfo_reqwidth(),
            app.workflow_notice_label.winfo_reqheight(),
        )
        assert actual[0] >= requested[0] and actual[1] >= requested[1], (
            "retry notice clips the server error: "
            f"actual={actual}, requested={requested}, text={notice_text!r}"
        )

        long_master = "CLC|MASTER|" + "M" * 72
        long_product = "PHS|PRODUCT|" + "P" * 72
        displayed_master = app._middle_ellipsis(long_master, 48)
        displayed_product = app._middle_ellipsis(long_product, 48)
        mismatch_message = (
            "현품표와 제품이 불일치합니다.\n\n"
            f"- 현품표: {displayed_master}\n"
            f"- 스캔 제품: {displayed_product}\n\n"
            "→ 이 세트는 오류 처리됩니다. 제품을 제거하고 확인 후 새 현품표부터 다시 스캔하세요."
        )
        app._present_inline_workflow_error(
            "[제품 불일치]",
            mismatch_message,
            app.Results.FAIL_MISMATCH,
            long_product,
        )
        pump_tk(app, 260)

        mismatch_text = str(app.workflow_notice_label.cget("text"))
        mismatch_lines = tuple(
            line for line in mismatch_text.splitlines() if line.strip()
        )
        assert len(mismatch_lines) <= 3
        assert "스캔 제품" in mismatch_text
        assert "새 현품표" in mismatch_text
        assert (
            app.workflow_notice_label.winfo_width()
            >= app.workflow_notice_label.winfo_reqwidth()
        )
        assert (
            app.workflow_notice_label.winfo_height()
            >= app.workflow_notice_label.winfo_reqheight()
        ), (
            "real mismatch notice clips at 1366x768 / 1.4x: "
            f"actual={app.workflow_notice_label.winfo_height()}, "
            f"requested={app.workflow_notice_label.winfo_reqheight()}, "
            f"text={mismatch_text!r}"
        )
        assert app.workflow_notice_frame.winfo_height() == retry_notice_height
        mismatch_tree_box = (
            app.qa_scan_tree.winfo_x(),
            app.qa_scan_tree.winfo_y(),
            app.qa_scan_tree.winfo_width(),
            app.qa_scan_tree.winfo_height(),
        )
        assert all(
            abs(before - after) <= 2
            for before, after in zip(retry_tree_box, mismatch_tree_box)
        )
        assert app.focus_get() == app.workflow_notice_action_button

        app._acknowledge_workflow_notice()
        pump_tk(app, 260)
        assert app.focus_get() == app.entry
    finally:
        if app is not None:
            app.destroy()
