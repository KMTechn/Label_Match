from __future__ import annotations

import copy
import inspect
import os
import subprocess
import sys
import textwrap
from pathlib import Path

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

    def pack_info(self):
        return dict(self.pack_options)

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
    notice_source = inspect.getsource(Label_Match._fit_operator_notice_geometry)

    assert "update_idletasks" not in source
    assert ".update()" not in source
    assert "update_idletasks" not in notice_source
    assert ".update()" not in notice_source


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


def test_exact_complete_keeps_full_f4_raw_in_detail_not_notice(
    operator_workbench,
):
    app = operator_workbench
    master = "PHS|MASTER|" + "M" * 160
    exact_values = tuple(
        f"PHS|F4|EXACT-{index}|" + chr(64 + index) * 180
        for index in range(1, 4)
    )
    app.current_set_info.update(
        {
            "raw": [master],
            "parsed": [master],
            "exact_rescan_active": False,
            "exact_rescan_complete": True,
            "exact_rescan_target_count": len(exact_values),
            "exact_rescan_barcodes": list(exact_values),
        }
    )
    app._workflow_last_normal_override = exact_values[-1]

    app._render_operator_workbench()

    notice_text = str(app.workflow_notice_label.cget("text"))
    assert exact_values[-1] not in notice_text
    assert notice_text == "2/5 제품 1 스캔"
    assert app.exact_rescan_tree.selection() == ("exact-slot-3",)
    assert app.exact_rescan_detail_text.options["inserted"] == exact_values[-1]
    assert app._exact_rescan_detail_rows["exact-slot-3"]["raw"] == exact_values[-1]


def test_tree_cell_fit_uses_effective_stretched_column_width(monkeypatch):
    class FixedFont:
        @staticmethod
        def measure(value):
            return len(str(value)) * 8

    class FixedStyle:
        @staticmethod
        def lookup(_style_name, option):
            assert option == "font"
            return ("Consolas", 12)

    monkeypatch.setattr(
        label_match_module.tkFont,
        "Font",
        lambda *args, **kwargs: FixedFont(),
    )
    app = Label_Match.__new__(Label_Match)
    app.style = FixedStyle()
    tree = FakeWidget(
        kind="ttk.Treeview",
        columns=("Stage", "Value", "State"),
        style="Operator.Treeview",
    )
    tree.column("Stage", width=120, stretch=False)
    tree.column("Value", width=100, stretch=True)
    tree.column("State", width=80, stretch=False)
    tree.winfo_width = lambda: 900
    value = "V" * 80

    assert app._fit_operator_tree_cell_text(tree, "Value", value) == value

    tree.column("Value", stretch=False)
    fixed_result = app._fit_operator_tree_cell_text(tree, "Value", value)
    assert fixed_result != value
    assert "..." in fixed_result


def test_history_normal_control_width_probe_is_cached_by_font_signature():
    app = Label_Match.__new__(Label_Match)
    app.default_font_name = "Malgun Gothic"
    app._current_font_size = 18
    calls = []
    style_state = {"compact": True}

    def configure_controls(compact=False):
        calls.append(bool(compact))
        style_state["compact"] = bool(compact)

    app._configure_history_control_buttons = configure_controls
    control_frame = FakeWidget()
    # Reproduce Tk's delayed parent request: it still reports compact width
    # immediately after the children switch to normal styling.
    control_frame.winfo_reqwidth = lambda: 228
    app.tk = type("FakeTk", (), {"splitlist": staticmethod(lambda value: str(value).split())})()
    button_specs = (
        ("today_button", 144, (0, 5), "오늘"),
        ("date_search_button", 144, (0, 15), "조회"),
        ("decrease_font_button", 88, 0, "-"),
        ("increase_font_button", 88, 0, "+"),
    )
    for name, normal_width, padx, text in button_specs:
        button = FakeWidget(text=text)
        button.pack(padx=padx)
        button.winfo_reqwidth = (
            lambda width=normal_width: 52
            if style_state["compact"]
            else width
        )
        setattr(app, name, button)
    date_button = app.date_search_button

    first = app._normal_history_control_requested_width(
        control_frame,
        date_button,
        24,
    )
    second = app._normal_history_control_requested_width(
        control_frame,
        date_button,
        24,
    )

    assert control_frame.winfo_reqwidth() == 228
    assert first == second == 484
    assert calls == [False]
    assert date_button.cget("text") == "날짜 조회"

    app._current_font_size = 20
    assert app._normal_history_control_requested_width(
        control_frame,
        date_button,
        24,
    ) == 484
    assert calls == [False, False]


@pytest.mark.parametrize(
    ("notebook_width", "realized_width", "stale_width"),
    ((887, 883, 653), (1281, 1277, 1484)),
)
def test_tree_cell_fit_uses_realized_sibling_instead_of_hidden_page_width(
    monkeypatch,
    notebook_width,
    realized_width,
    stale_width,
):
    class FixedFont:
        @staticmethod
        def measure(value):
            return len(str(value)) * 8

    class FixedStyle:
        @staticmethod
        def lookup(_style_name, option):
            assert option == "font"
            return ("Consolas", 12)

    monkeypatch.setattr(
        label_match_module.tkFont,
        "Font",
        lambda *args, **kwargs: FixedFont(),
    )
    app = Label_Match.__new__(Label_Match)
    app.style = FixedStyle()
    notebook = FakeWidget()
    notebook.winfo_width = lambda: notebook_width
    qa_tree = FakeWidget()
    qa_tree.winfo_width = lambda: realized_width
    qa_tree._mapped = True
    exact_tree = FakeWidget(
        kind="ttk.Treeview",
        columns=("Order", "Value"),
        style="Operator.Treeview",
    )
    exact_tree.column("Order", width=80, stretch=False)
    exact_tree.column("Value", width=100, stretch=True)
    exact_tree.winfo_width = lambda: stale_width
    exact_tree._mapped = False
    app.live_scan_notebook = notebook
    app.qa_scan_tree = qa_tree
    app.exact_rescan_tree = exact_tree
    value = "F4-EXACT-" + "V" * 220 + "-VALUE-END"

    fitted = app._fit_operator_tree_cell_text(exact_tree, "Value", value)

    app.live_scan_notebook = None
    app.qa_scan_tree = None
    exact_tree.winfo_width = lambda: realized_width
    expected = app._fit_operator_tree_cell_text(exact_tree, "Value", value)
    exact_tree.winfo_width = lambda: stale_width
    stale = app._fit_operator_tree_cell_text(exact_tree, "Value", value)

    assert fitted == expected
    assert fitted != stale


def test_tree_cell_fit_prefers_mapped_sibling_when_hidden_width_is_closer(
    monkeypatch,
):
    class FixedFont:
        @staticmethod
        def measure(value):
            return len(str(value)) * 8

    class FixedStyle:
        @staticmethod
        def lookup(_style_name, option):
            assert option == "font"
            return ("Consolas", 12)

    monkeypatch.setattr(
        label_match_module.tkFont,
        "Font",
        lambda *args, **kwargs: FixedFont(),
    )
    app = Label_Match.__new__(Label_Match)
    app.style = FixedStyle()
    notebook = FakeWidget()
    notebook.winfo_width = lambda: 887
    qa_tree = FakeWidget()
    qa_tree.winfo_width = lambda: 883
    qa_tree._mapped = True
    exact_tree = FakeWidget(
        kind="ttk.Treeview",
        columns=("Order", "Value"),
        style="Operator.Treeview",
    )
    exact_tree.column("Order", width=80, stretch=False)
    exact_tree.column("Value", width=100, stretch=True)
    exact_tree.winfo_width = lambda: 886
    exact_tree._mapped = False
    app.live_scan_notebook = notebook
    app.qa_scan_tree = qa_tree
    app.exact_rescan_tree = exact_tree
    value = "F4-EXACT-" + "V" * 220 + "-VALUE-END"

    fitted = app._fit_operator_tree_cell_text(exact_tree, "Value", value)

    app.live_scan_notebook = None
    app.qa_scan_tree = None
    exact_tree.winfo_width = lambda: 883
    expected = app._fit_operator_tree_cell_text(exact_tree, "Value", value)

    assert fitted == expected


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
        TARGET_DISPLAY_DPI,
        _apply_scale,
        _configure_size,
        _make_capture_app,
        _pending_after_ids,
        _wait_until_ready,
        apply_state_fixture,
        build_isolated_app_settings,
        build_state_fixtures,
        pump_tk,
        settle_responsive_layout,
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
        app = _make_capture_app(
            label_match_module,
            settings,
            target_dpi=TARGET_DISPLAY_DPI[0],
        )
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
        assert (
            app.workflow_notice_label.winfo_y()
            + app.workflow_notice_label.winfo_height()
            <= app.workflow_notice_frame.winfo_height()
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

        _apply_scale(app, 1.0)
        _configure_size(app, (2560, 1392))
        error_fixture = next(
            fixture
            for fixture in build_state_fixtures()
            if fixture.state_id == "error"
        )
        apply_state_fixture(app, error_fixture)
        settle_responsive_layout(app)
        pump_tk(app, 260)

        assert (
            app.workflow_notice_label.winfo_height()
            >= app.workflow_notice_label.winfo_reqheight()
        )
        assert (
            app.workflow_notice_label.winfo_y()
            + app.workflow_notice_label.winfo_height()
            <= app.workflow_notice_frame.winfo_height()
        )
        wide_rows = tuple(app.qa_scan_tree.get_children())
        assert len(wide_rows) == 5
        wide_last_row_box = app.qa_scan_tree.bbox(wide_rows[-1])
        assert wide_last_row_box
        assert (
            wide_last_row_box[1] + wide_last_row_box[3]
            <= app.qa_scan_tree.winfo_height()
        )

        import _tkinter

        history_fixture = next(
            fixture
            for fixture in build_state_fixtures()
            if fixture.state_id == "history_readonly"
        )
        session_fixture = next(
            fixture
            for fixture in build_state_fixtures()
            if fixture.state_id == "recovery"
        )

        def drain_tk_events(limit=256):
            for after_id in _pending_after_ids(app):
                app.after_cancel(after_id)
            app._operator_layout_settle_after_id = None
            app._responsive_after_id = None
            for event_count in range(1, limit + 1):
                if not app.tk.dooneevent(
                    _tkinter.ALL_EVENTS | _tkinter.DONT_WAIT
                ):
                    assert _pending_after_ids(app) == ()
                    return event_count
            pytest.fail("history tab layout keeps generating Tk idle events")

        def widget_box(widget):
            return (
                widget.winfo_x(),
                widget.winfo_y(),
                widget.winfo_width(),
                widget.winfo_height(),
            )

        def root_box(widget):
            return (
                widget.winfo_rootx(),
                widget.winfo_rooty(),
                widget.winfo_rootx() + widget.winfo_width(),
                widget.winfo_rooty() + widget.winfo_height(),
            )

        def history_layout_signature():
            grid_keys = ("row", "column", "columnspan", "sticky", "pady")
            label_grid = app.hist_header_label.grid_info()
            controls_grid = app.hist_control_frame.grid_info()
            return (
                app._history_controls_compact,
                app._history_controls_stacked,
                tuple(str(label_grid.get(key, "")) for key in grid_keys),
                tuple(str(controls_grid.get(key, "")) for key in grid_keys),
                widget_box(app.hist_header_frame),
                widget_box(app.hist_header_label),
                widget_box(app.hist_control_frame),
                tuple(
                    (
                        button.cget("text"),
                        button.cget("style"),
                        str(button.pack_info().get("padx", "")),
                    )
                    for button in (
                        app.today_button,
                        app.date_search_button,
                        app.decrease_font_button,
                        app.increase_font_button,
                    )
                ),
                tuple(
                    app.history_tree.heading(column, "text")
                    for column in app.HISTORY_HEADING_LABELS
                ),
                str(app.style.lookup("Treeview.Heading", "font")),
            )

        center_widgets = (
            app.workflow_notice_frame,
            app.live_scan_notebook,
            app.qa_scan_tree,
            app.qa_scan_detail_frame,
        )
        center_before_history = tuple(map(widget_box, center_widgets))

        apply_state_fixture(app, history_fixture)
        assert drain_tk_events() < 256
        first_history_signature = history_layout_signature()
        assert app.operator_notebook.select() == str(app.history_card)
        assert bool(app.history_tree.winfo_ismapped()) is True
        assert bool(app.session_tree.winfo_ismapped()) is False
        assert bool(app.hist_header_frame.winfo_ismapped()) is True
        assert bool(app.hist_header_label.winfo_ismapped()) is True
        assert bool(app.hist_control_frame.winfo_ismapped()) is True
        assert all(
            bool(button.winfo_ismapped()) is True
            for button in (
                app.today_button,
                app.date_search_button,
                app.decrease_font_button,
                app.increase_font_button,
            )
        )
        assert all(
            abs(before - after) <= 2
            for before_box, after_box in zip(
                center_before_history,
                map(widget_box, center_widgets),
            )
            for before, after in zip(before_box, after_box)
        )

        label_rect = root_box(app.hist_header_label)
        controls_rect = root_box(app.hist_control_frame)
        assert (
            app.hist_control_frame.winfo_x()
            + app.hist_control_frame.winfo_width()
            <= app.hist_header_frame.winfo_width()
        )
        assert (
            app.hist_control_frame.winfo_y()
            + app.hist_control_frame.winfo_height()
            <= app.hist_header_frame.winfo_height()
        )
        assert (
            label_rect[2] <= controls_rect[0]
            or controls_rect[2] <= label_rect[0]
            or label_rect[3] <= controls_rect[1]
            or controls_rect[3] <= label_rect[1]
        )
        for button in (
            app.today_button,
            app.date_search_button,
            app.decrease_font_button,
            app.increase_font_button,
        ):
            assert button.winfo_x() >= 0 and button.winfo_y() >= 0
            assert button.winfo_width() >= button.winfo_reqwidth()
            assert button.winfo_height() >= button.winfo_reqheight()

        apply_state_fixture(app, history_fixture)
        assert drain_tk_events() < 256
        assert history_layout_signature() == first_history_signature

        apply_state_fixture(app, session_fixture)
        assert drain_tk_events() < 256
        assert app.operator_notebook.select() == str(app.session_tab)
        assert bool(app.session_tree.winfo_ismapped()) is True
        assert bool(app.history_tree.winfo_ismapped()) is False

        apply_state_fixture(app, history_fixture)
        assert drain_tk_events() < 256
        assert history_layout_signature() == first_history_signature
        assert bool(app.history_tree.winfo_ismapped()) is True
        assert bool(app.session_tree.winfo_ismapped()) is False

        history_rows = tuple(app.qa_scan_tree.get_children())
        assert len(history_rows) == 5
        history_last_row_box = app.qa_scan_tree.bbox(history_rows[-1])
        assert history_last_row_box
        assert (
            history_last_row_box[1] + history_last_row_box[3]
            <= app.qa_scan_tree.winfo_height()
        )

        def history_signature_at(size):
            _configure_size(app, size)
            apply_state_fixture(app, history_fixture)
            settle_responsive_layout(app)
            pump_tk(app, 180)
            assert drain_tk_events() < 256
            assert (
                app.hist_header_frame.winfo_x()
                + app.hist_header_frame.winfo_width()
                <= app.history_card.winfo_width()
            )
            assert (
                app.hist_control_frame.winfo_x()
                + app.hist_control_frame.winfo_width()
                <= app.hist_header_frame.winfo_width()
            )
            assert (
                app._history_control_buttons_requested_width()
                == app.hist_control_frame.winfo_reqwidth()
            )
            return history_layout_signature()

        compact_history_before = history_signature_at((1366, 768))
        history_signature_at((2560, 1392))
        compact_history_after = history_signature_at((1366, 768))
        assert compact_history_after == compact_history_before

    finally:
        if app is not None:
            app.destroy()


@pytest.mark.skipif(os.name != "nt", reason="Label Match is a Windows Tk application")
def test_display2_1366_scale100_keeps_operator_content_inside_its_regions(
    tmp_path,
    monkeypatch,
):
    """Fail closed unless the real 1366x768 window is proven on DISPLAY2."""

    child_guard = "LABEL_MATCH_DISPLAY2_LAYOUT_CHILD"
    if os.environ.get(child_guard) != "1":
        # A long full-suite process has already created and destroyed another
        # Tk interpreter.  Python 3.12/Tk 8.6 on Windows can then intermittently
        # lose its Tcl library commands while constructing a second root.  Run
        # this independent live geometry proof in a fresh interpreter; the
        # child still uses the same fail-closed DISPLAY2 placement contract.
        child_env = os.environ.copy()
        child_env[child_guard] = "1"
        node_id = (
            "tests/test_label_operator_workbench.py::"
            "test_display2_1366_scale100_keeps_operator_content_inside_its_regions"
        )
        result = subprocess.run(
            [sys.executable, "-B", "-m", "pytest", "-q", node_id],
            cwd=os.fspath(Path(__file__).resolve().parents[1]),
            env=child_env,
            capture_output=True,
            text=True,
            timeout=180,
            check=False,
        )
        assert result.returncode == 0, result.stdout + result.stderr
        return

    from tools.capture_label_operator_ui import (
        TARGET_DISPLAY_DEVICE,
        TARGET_DISPLAY_WORK_AREA,
        _apply_scale,
        _configure_size,
        _make_capture_app,
        _pending_after_ids,
        _wait_until_ready,
        apply_state_fixture,
        build_isolated_app_settings,
        build_state_fixtures,
        pump_tk,
        resolve_capture_monitor,
        settle_responsive_layout,
    )

    data_root = tmp_path / "label_match_display2_1366_scale100"
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

    monitor_target = resolve_capture_monitor(
        TARGET_DISPLAY_DEVICE,
        TARGET_DISPLAY_WORK_AREA,
    )
    assert monitor_target["device"].casefold() == TARGET_DISPLAY_DEVICE.casefold()
    assert monitor_target["is_primary"] is False
    assert tuple(monitor_target["work_rect"][:2]) != (0, 0)

    def root_box(widget):
        left = int(widget.winfo_rootx())
        top = int(widget.winfo_rooty())
        return (
            left,
            top,
            left + int(widget.winfo_width()),
            top + int(widget.winfo_height()),
        )

    def contains(parent, child, tolerance=2):
        parent_box = root_box(parent)
        child_box = root_box(child)
        return (
            child_box[0] >= parent_box[0] - tolerance
            and child_box[1] >= parent_box[1] - tolerance
            and child_box[2] <= parent_box[2] + tolerance
            and child_box[3] <= parent_box[3] + tolerance
        )

    def boxes_overlap(first, second):
        a = root_box(first)
        b = root_box(second)
        return not (
            a[2] <= b[0]
            or b[2] <= a[0]
            or a[3] <= b[1]
            or b[3] <= a[1]
        )

    settings = build_isolated_app_settings(data_root, 1.0)
    app = None
    try:
        app = _make_capture_app(
            label_match_module,
            settings,
            target_dpi=int(monitor_target["dpi"][0]),
        )
        _wait_until_ready(app)
        _apply_scale(app, 1.0)
        placement = _configure_size(
            app,
            (1366, 768),
            monitor_target,
        )
        assert placement["status"] == "PASS"
        assert placement["monitor"]["device"].casefold() == TARGET_DISPLAY_DEVICE.casefold()
        assert placement["monitor"]["is_primary"] is False

        fixtures = {
            fixture.state_id: fixture for fixture in build_state_fixtures()
        }
        for state_id in (
            "waiting",
            "error",
            "qa_progress",
            "exact_first",
            "exact_complete",
        ):
            fixture = fixtures[state_id]
            apply_state_fixture(app, fixture)
            settle_responsive_layout(app)
            pump_tk(app, 220)

            assert contains(app.operator_center_pane, app.live_scan_notebook), state_id
            if state_id in {"exact_first", "exact_complete"}:
                active_frame = app.exact_rescan_frame
                active_tree = app.exact_rescan_tree
                active_detail_frame = app.exact_rescan_detail_frame
                active_detail_text = app.exact_rescan_detail_text
                expected_row_count = len(fixture.exact_barcodes)
            else:
                active_frame = app.qa_scan_frame
                active_tree = app.qa_scan_tree
                active_detail_frame = app.qa_scan_detail_frame
                active_detail_text = app.qa_scan_detail_text
                expected_row_count = 5
            assert contains(active_frame, active_detail_frame), state_id
            assert contains(active_detail_frame, active_detail_text), state_id
            assert (
                active_detail_text.winfo_height()
                >= active_detail_text.winfo_reqheight()
            ), state_id

            rows = tuple(active_tree.get_children())
            assert len(rows) == expected_row_count, state_id
            assert bool(active_tree.winfo_ismapped()) is True, state_id
            assert (
                root_box(active_tree)[1]
                >= root_box(app.operator_input_frame)[3] - 1
            ), state_id
            if state_id == "exact_first":
                displayed = str(active_tree.item(rows[0], "values")[1])
                assert displayed == app._fit_operator_tree_cell_text(
                    active_tree,
                    "Value",
                    fixture.exact_barcodes[0],
                )
            final_row_box = active_tree.bbox(rows[-1])
            assert final_row_box, state_id
            assert (
                final_row_box[1] + final_row_box[3]
                <= active_tree.winfo_height()
            ), state_id

            assert (
                app.workflow_notice_label.winfo_height()
                >= app.workflow_notice_label.winfo_reqheight()
            ), state_id
            assert contains(
                app.workflow_notice_frame,
                app.workflow_notice_label,
            ), state_id
            assert (
                app.workflow_notice_frame.winfo_height()
                >= app._operator_notice_required_height
            ), state_id

            assert contains(app.main_frame, app.operator_status_frame), state_id
            assert contains(
                app.operator_status_frame,
                app.operator_footer_label,
            ), state_id
            assert (
                root_box(app.live_scan_notebook)[3]
                <= root_box(app.operator_center_pane)[3] + 2
            ), state_id
            assert (
                root_box(app.operator_center_pane)[3]
                <= root_box(app.operator_status_frame)[1]
            ), state_id

            for button in (
                app.manual_complete_button,
                app.exact_rescan_button,
                app.reset_button,
                app.cancel_tray_button,
            ):
                assert 86 <= button.winfo_height() <= 104, (
                    state_id,
                    button.cget("text"),
                    button.winfo_height(),
                )
                assert contains(app.operator_action_frame, button), state_id

        exact_fixture = fixtures["exact_complete"]
        pump_tk(app, 160)
        assert app.live_scan_notebook.select() == str(app.exact_rescan_frame)
        assert contains(app.live_scan_notebook, app.exact_rescan_frame)
        assert contains(app.exact_rescan_frame, app.exact_rescan_detail_frame)
        assert contains(
            app.exact_rescan_detail_frame,
            app.exact_rescan_detail_text,
        )
        assert (
            app.exact_rescan_detail_text.winfo_height()
            >= app.exact_rescan_detail_text.winfo_reqheight()
        )
        latest_exact_iid = f"exact-slot-{len(exact_fixture.exact_barcodes)}"
        app.exact_rescan_tree.selection_set(latest_exact_iid)
        app._render_exact_rescan_detail(latest_exact_iid)
        assert (
            app.exact_rescan_detail_text.get("1.0", "end-1c")
            == exact_fixture.exact_barcodes[-1]
        )
        assert exact_fixture.exact_barcodes[-1] not in str(
            app.workflow_notice_label.cget("text")
        )

        def relative_box(widget):
            root_left = int(app.winfo_rootx())
            root_top = int(app.winfo_rooty())
            box = root_box(widget)
            return (
                box[0] - root_left,
                box[1] - root_top,
                box[2] - root_left,
                box[3] - root_top,
            )

        def prove_exact_first_transition(size):
            resized = _configure_size(app, size, monitor_target)
            assert resized["status"] == "PASS"
            assert resized["monitor"]["is_primary"] is False
            apply_state_fixture(app, fixtures["qa_master"])
            settle_responsive_layout(app)
            pump_tk(app, 180)
            assert bool(app.qa_scan_tree.winfo_ismapped()) is True

            exact_first = fixtures["exact_first"]
            apply_state_fixture(app, exact_first)
            settle_responsive_layout(app)
            pump_tk(app, 180)
            assert app.live_scan_notebook.select() == str(app.exact_rescan_frame)
            assert bool(app.exact_rescan_tree.winfo_ismapped()) is True
            rows = tuple(app.exact_rescan_tree.get_children())
            assert rows == ("exact-slot-1",)
            displayed = str(app.exact_rescan_tree.item(rows[0], "values")[1])
            assert displayed == app._fit_operator_tree_cell_text(
                app.exact_rescan_tree,
                "Value",
                exact_first.exact_barcodes[0],
            )
            assert (
                app.exact_rescan_detail_text.get("1.0", "end-1c")
                == exact_first.exact_barcodes[0]
            )
            last_box = app.exact_rescan_tree.bbox(rows[-1])
            assert last_box
            assert (
                last_box[1] + last_box[3]
                <= app.exact_rescan_tree.winfo_height()
            )
            assert (
                root_box(app.exact_rescan_tree)[1]
                >= root_box(app.operator_input_frame)[3] - 1
            )
            assert _pending_after_ids(app) == ()
            return tuple(
                relative_box(widget)
                for widget in (
                    app.live_scan_notebook,
                    app.exact_rescan_tree,
                    app.exact_rescan_detail_frame,
                    app.exact_rescan_detail_text,
                )
            )

        compact_before = prove_exact_first_transition((1366, 768))
        prove_exact_first_transition((2560, 1392))
        compact_after = prove_exact_first_transition((1366, 768))
        assert all(
            abs(before - after) <= 2
            for before_box, after_box in zip(compact_before, compact_after)
            for before, after in zip(before_box, after_box)
        )

        history_placement = _configure_size(
            app,
            (1920, 1080),
            monitor_target,
        )
        assert history_placement["status"] == "PASS"
        assert history_placement["monitor"]["is_primary"] is False
        app.operator_notebook.select(app.history_card)
        settle_responsive_layout(app)
        pump_tk(app, 220)
        assert contains(app.right_activity_card, app.hist_header_frame)
        assert contains(app.operator_history_notebook, app.hist_header_frame)
        assert contains(app.hist_header_frame, app.hist_header_label)
        assert contains(app.hist_header_frame, app.hist_control_frame)
        assert (
            app._history_control_buttons_requested_width()
            == app.hist_control_frame.winfo_reqwidth()
        )
        assert not boxes_overlap(app.hist_header_label, app.hist_control_frame)
        history_buttons = (
            app.today_button,
            app.date_search_button,
            app.decrease_font_button,
            app.increase_font_button,
        )
        assert all(
            contains(app.hist_control_frame, button)
            for button in history_buttons
        )
        assert all(
            button.winfo_width() >= button.winfo_reqwidth()
            and button.winfo_height() >= button.winfo_reqheight()
            for button in history_buttons
        )
        assert all(
            not boxes_overlap(first, second)
            for index, first in enumerate(history_buttons)
            for second in history_buttons[index + 1 :]
        )
        widths = tuple(button.winfo_width() for button in history_buttons)
        heights = tuple(button.winfo_height() for button in history_buttons)
        assert abs(widths[0] - widths[1]) <= 2
        assert abs(widths[2] - widths[3]) <= 2
        assert max(heights) - min(heights) <= 2
    finally:
        if app is not None:
            app.destroy()
