"""Recovery remains reachable without occupying the scanner's work controls."""
from dataclasses import replace
import ast
import inspect
import textwrap
from types import SimpleNamespace

import pytest

from deferred_intent_capture import DeferredOperatorStatusGroup
from tests.test_label_operator_workbench import operator_workbench, _required_widget, _walk  # noqa: F401
from tests.test_label_operator_workbench import FakeWidget
import Label_Match as lm
from tests.test_deferred_intent_capture import _store


def test_recovery_entry_is_only_in_header_administrator_menu(operator_workbench):
    app = operator_workbench
    button = _required_widget(app, "administrator_menu_button")
    assert button.master is app.settings_button.master
    assert button.pack_options["before"] is app.about_button
    assert button.cget("text") == "관리자"
    commands = app.package_recovery_menu.options["menu_commands"]
    assert commands == [{"label": "멈춘 작업 확인 (관리자)", "command": app._show_package_recovery_workbench}]
    assert "package_recovery_button" not in app.__dict__
    assert all(widget.cget("text") != "건별 복구" for widget in _walk(app.operator_left_pane))


@pytest.mark.parametrize("status", ["CONFLICT", "PENDING"])
def test_recovery_notice_reuses_the_menu_callback_and_existing_availability(operator_workbench, status):
    app = operator_workbench
    app._set_active_package_submission_notice({"status": status})
    command = app.package_recovery_menu.options["menu_commands"][0]["command"]
    assert command is app._workflow_notice_action
    assert command == app._show_package_recovery_workbench
    assert app.workflow_notice_action_button.winfo_ismapped()
    assert app.workflow_notice_action_button.cget("text") == "멈춘 작업 확인 (관리자)"
    assert app.workflow_notice_action_button.cget("command") == app._acknowledge_workflow_notice
    assert app.administrator_menu_button.winfo_ismapped()
    assert app.workflow_notice_action_button.cget("state") != "disabled"
    assert app.administrator_menu_button.cget("state") != "disabled"
    invoked = []
    app._workflow_notice_action = lambda: invoked.append(command)
    app.workflow_notice_action_button.cget("command")()
    assert invoked == [command]


@pytest.mark.parametrize("modal", [False, True])
def test_recovery_window_close_preserves_partial_scan_and_returns_focus_once(operator_workbench, monkeypatch, modal):
    app = operator_workbench
    class Window(FakeWidget):
        def title(self, title):
            self.options["title"] = title
        def transient(self, owner):
            self.options["transient"] = owner
        def winfo_exists(self):
            return not self.options.get("destroyed", False)
        def destroy(self):
            self.options["destroyed"] = True
            for sequence, callback, _add in self.bindings:
                if sequence == "<Destroy>":
                    callback(SimpleNamespace(widget=self))
    monkeypatch.setattr(lm.tk, "Toplevel", Window)
    monkeypatch.setattr(lm.tk, "StringVar", lambda **kw: SimpleNamespace(set=lambda value: None))
    app._package_recovery_candidates = lambda: []
    app.run_tests = False
    app.entry.options["input"] = "PHS=2|HALF-"
    app.entry.get = lambda: app.entry.options["input"]
    focused = []
    app.entry.focus_set = lambda: focused.append("scan")
    app.grab_current = lambda: object() if modal else None
    window = app.package_recovery_menu.options["menu_commands"][0]["command"]()
    assert window.options["title"] == "멈춘 작업 확인 (관리자)"
    assert window.options["transient"] is app
    assert app.entry.get() == "PHS=2|HALF-" and not focused
    window.destroy()
    assert len(app._idle_callbacks) == 1 and not focused
    app._idle_callbacks.pop(0)()
    assert focused == ([] if modal else ["scan"])
    assert app.entry.get() == "PHS=2|HALF-" and not app._idle_callbacks


@pytest.mark.parametrize("count", [0, 2])
def test_administrator_footer_is_only_present_for_review(operator_workbench, tmp_path, count):
    app = operator_workbench
    _, _, store = _store(tmp_path)
    group = DeferredOperatorStatusGroup("admin_review", "관리자 확인", ("OPERATOR_REVIEW",), count, "", 0)
    readback = replace(store.status_readback(), operator_groups=(group,), nonterminal_count=count)
    before = app.entry.focused
    app._render_deferred_observability(readback)
    assert app.operator_footer_label.cget("text") == (f"관리자 확인 필요 {count}건" if count else "회사 서버 확인 중")
    assert app.entry.focused == before


def test_all_four_saved_recovery_notice_actions_use_the_same_menu_target():
    tree = ast.parse(textwrap.dedent(inspect.getsource(lm.Label_Match._load_current_set_state_checked)))
    labels, callbacks = [], []
    for node in ast.walk(tree):
        if not isinstance(node, ast.Assign):
            continue
        for target in node.targets:
            if isinstance(target, ast.Attribute) and target.attr == "_workflow_notice_action_text":
                labels.append(node.value)
            if isinstance(target, ast.Attribute) and target.attr == "_workflow_notice_action" and isinstance(node.value, ast.Call):
                callbacks.append(node.value)
    assert not any(isinstance(value, ast.Constant) and value.value == "건별 복구" for value in labels)
    assert len(labels) == 5
    assert sum(isinstance(value, ast.Name) and value.id == "LABEL_MATCH_PACKAGE_RECOVERY_TEXT" for value in labels) == 4
    assert [value.value for value in labels if isinstance(value, ast.Constant)] == ["저장 재시도"]
    expected = ast.parse("self.__dict__.setdefault('_package_recovery_callback', self._show_package_recovery_workbench)", mode="eval").body
    assert len(callbacks) == 4
    assert all(ast.dump(value, include_attributes=False) == ast.dump(expected, include_attributes=False) for value in callbacks)


def test_idle_focus_return_defers_to_another_toplevel_without_erasing_scan(operator_workbench):
    app = operator_workbench
    app.entry.options["input"] = "PHS=2|HALF-"
    app.entry.get = lambda: app.entry.options["input"]
    app.entry.delete = lambda *args: pytest.fail("focus return erased a scan")
    app.entry.selection_range = lambda *args: pytest.fail("focus return selected a scan")
    focused = []
    app.entry.focus_set = lambda: focused.append("scan")
    other = object()
    app.focus_get = lambda: SimpleNamespace(winfo_toplevel=lambda: other)
    app.grab_current = lambda: None
    first = app._schedule_scan_entry_focus_return()
    assert app._schedule_scan_entry_focus_return() == first
    assert len(app._idle_callbacks) == 1
    app._idle_callbacks.pop(0)()
    assert not focused and not app._idle_callbacks
    assert app.entry.get() == "PHS=2|HALF-"
