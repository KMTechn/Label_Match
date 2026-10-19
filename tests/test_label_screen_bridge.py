"""포장 새 화면 이음(label_screen_bridge) — 창 · Tk 없이 셸 Bridge(owner='external')와 가짜 앱.

HOST-USE §7: state 꼴 = ui/parts/app/lm/contract.json · scan → 처리 → push 차례 · 같은 seq 처리 한 번 ·
예외 → processing_error · admin 인수(PIN)는 셸 기록에 없음. 손질 셋(intake_scan · process_input raw · PIN 인수)은
진짜 Label_Match 메서드를 가짜 self 로 부른다. PIN 은 시험마다 무작위로 만들어 원천에 값이 없다.
"""
from __future__ import annotations

import json
import secrets
import subprocess
import sys
import threading
import types
from pathlib import Path
from unittest import mock

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "shell"))

import Label_Match  # noqa: E402
from bridge import Bridge  # noqa: E402
from label_screen_bridge import LabelMatchScreenProcessor, present_lm  # noqa: E402
from ui.workflow_view_state import WorkflowNotice, WorkflowSnapshot, present_workflow  # noqa: E402
from tests.label_screen_entry_double import fresh_screen_launch

CONTRACT = json.loads((ROOT / "ui" / "parts" / "app" / "lm" / "contract.json").read_text(encoding="utf-8"))
MASTER = "PHS=2|SRC=KMTECH_INPUT_TAG|ITG=A110|CLC=C0325|LBL=L0325|HSH=0f3a9c2b7d1e4a56"


def schema_errors(schema, value, path="state"):
    """contract 의 JSON Schema 부분집합(type · required · properties · additionalProperties · enum · const · items · minimum · maxItems)."""
    errors = []
    if "const" in schema and value != schema["const"]:
        errors.append(f"{path}: const")
    if "enum" in schema and value not in schema["enum"]:
        errors.append(f"{path}: {value!r} not in enum")
    kinds = schema.get("type")
    if kinds is not None:
        kinds = [kinds] if isinstance(kinds, str) else kinds
        names = {"object": dict, "array": list, "string": str, "boolean": bool, "null": type(None)}
        if not any((k == "integer" and isinstance(value, int) and not isinstance(value, bool))
                   or (k in names and isinstance(value, names[k]) and not (k != "boolean" and isinstance(value, bool)))
                   for k in kinds):
            return errors + [f"{path}: type {kinds} != {type(value).__name__}"]
    if isinstance(value, dict):
        errors += [f"{path}: missing {key}" for key in schema.get("required", []) if key not in value]
        for key, item in value.items():
            if key in schema.get("properties", {}):
                errors += schema_errors(schema["properties"][key], item, f"{path}.{key}")
            elif schema.get("additionalProperties") is False:
                errors.append(f"{path}: extra {key}")
    if isinstance(value, list):
        if "maxItems" in schema and len(value) > schema["maxItems"]:
            errors.append(f"{path}: maxItems")
        for index, item in enumerate(value):
            errors += schema_errors(schema.get("items", {}), item, f"{path}[{index}]")
    if "minimum" in schema and isinstance(value, int) and not isinstance(value, bool) and value < schema["minimum"]:
        errors.append(f"{path}: minimum")
    return errors


class FakeTree:
    def __init__(self, rows):
        self.rows = rows

    def get_children(self):
        return tuple(iid for iid, _values in self.rows)

    def item(self, iid, option):
        return dict(self.rows)[iid]


class FakeApp:
    """Label_Match 의 이음이 읽는 속성 · 메서드만 — 처리 규칙은 진짜 앱 몫(여기서는 차례만 흉내)."""

    TOTAL_SCAN_COUNT = 5

    def __init__(self):
        self.initialized_successfully = True
        self.worker_name = "작업자 12"
        self.worker_id = "w12"
        self.current_set_info = {"raw": [], "parsed": []}
        self.items_data = {"A110": {"Item Name": "커버 어셈블리", "Spec": "LHD"}}
        self.calls = []
        self.idle = []
        self.clipboard = []
        self.history_tree = FakeTree([("r1", ("1", "A110", "x1", "x2", "x3", "x4", "통과", "2026-10-13 08:12:00"))])
        self.history_row_details_map = {str(iid): {"scanned_product_barcodes": list(values[1:1 + self.TOTAL_SCAN_COUNT])} for iid, values in self.history_tree.rows}
        self.summary_tree = FakeTree([("s1", ("A110", "1차", "4"))])
        self._render_operator_workbench()

    # 숨긴 Tk 화면이 하는 일 — 지금 세트로 present_workflow
    def _workflow_view_source(self):
        return dict(self.current_set_info)

    def _render_operator_workbench(self):
        self._last_workflow_view = present_workflow(WorkflowSnapshot(qa_scans=tuple(self.current_set_info["raw"])))
        return self._last_workflow_view

    def _render_logistics_connection_footer(self):
        return None

    def _deferred_capture_pending_notice(self, view):
        return None

    def _deferred_capture_failure_notice(self):
        return None

    def _compact_operator_notice_message(self, message):
        return " ".join(str(message).split())

    def _standard_phs2_workflow_expected(self, source=None):
        return True

    def _operator_package_quantity(self, source):
        return 24 if source.get("raw") else None

    def _worker_roster_choices(self):
        return [{"worker_id": "w12", "display_name": "작업자 12"}, {"worker_id": "w07", "display_name": "작업자 7"}]

    def _roster_worker_allowed(self):
        return bool(self.worker_name)

    def _operator_session_time_text(self, value):
        return str(value)[11:16]

    def after_idle(self, callback):
        self.idle.append(callback)

    def flush_idle(self):
        while self.idle:
            self.idle.pop(0)()

    def clipboard_clear(self):
        self.clipboard.clear()

    def clipboard_append(self, text):
        self.clipboard.append(text)

    def _play_sound(self, sound_key, block=False):
        self.calls.append(("sound", sound_key))

    def update_big_display(self, text, color=""):
        self.calls.append(("big", text, color))

    def open_settings_window(self):
        self.calls.append(("settings_window",))

    # 입구 — 진짜 앱은 처리 층 규칙으로 판정; 여기선 받은 차례만 남김
    def intake_scan(self, text, *, event=None, from_entry=False):
        self.calls.append(("intake", text, from_entry))
        if text == "BOOM":
            raise RuntimeError("processing failed")
        if text:
            parsed = ["A110"] if not self.current_set_info["raw"] else []
            self.current_set_info = {"raw": self.current_set_info["raw"] + [text],
                                     "parsed": (self.current_set_info["parsed"] or parsed) + ([] if parsed else [text])}
            self._play_sound("scan_master")
            self._render_operator_workbench()

    def _handle_workflow_shortcut(self, action, event=None):
        self.calls.append(("shortcut", action))
        if action == "f2":
            label = Label_Match.simpledialog.askstring("완료된 트레이 취소", "취소할 트레이의 현품표를 스캔하거나 입력하세요:")
            self.calls.append(("f2_label", label))
            if label == "L0325":
                notice = WorkflowNotice("완료 트레이 취소", "현품표 L0325 트레이를 취소합니다.", kind="pointer_confirmation", tone="warning")
                reject = mock.Mock(name="refuse")
                self._pointer_confirmation = (notice, reject, lambda: self.calls.append(("cancelled", label)))
                self._workflow_notice_action = reject
                self._workflow_notice_action_text = "취소하기"
            else:
                Label_Match.messagebox.showwarning("입력 오류", "그 현품표의 완료 트레이가 없습니다.", parent=self)

    def _acknowledge_workflow_notice(self, event=None):
        self.calls.append(("ack",))

    def _handle_workflow_escape(self, event=None):
        self.calls.append(("escape",))

    def _submit_package_pin_action(self, action, set_id, physical_qr="", *, window, finish, fail):
        # 진짜 앱: 준비 → _prompt_package_admin_pin() → 적용. 여기선 PIN 묻는 자리만 진짜 메서드로.
        credentials = Label_Match.Label_Match._prompt_package_admin_pin(self)
        self.calls.append(("pin_action", action, set_id, physical_qr, credentials is not None, window.winfo_exists()))
        self._admin_pin_result_outcome = "success"
        finish("다시 확인했습니다")
        return True

    run_tests = True

    def _package_recovery_candidates(self):
        return [{"set_id": "S-0325", "package": {"label": "L0325", "item_code": "A110"}}]

    def on_closing(self, _confirmed=False, _pointer_confirmed=False):
        self.calls.append(("on_closing", _confirmed))


@pytest.fixture
def screen(tmp_path):
    app = FakeApp()
    bridge = Bridge(tmp_path / "shell", "rw", sound=mock.Mock(), owner="external")
    processor = LabelMatchScreenProcessor(app, bridge, module=Label_Match)
    bridge.attach_owner(processor)
    pushes = []
    bridge.set_notifier(pushes.append)
    yield types.SimpleNamespace(app=app, bridge=bridge, processor=processor, pushes=pushes, data=tmp_path / "shell")
    processor.uninstall()
    bridge.close()


def meta(seq):
    """받이(scan.js)가 보내는 꼴 — 셸은 evidence.pedal_pressed(bool)가 없으면 invalid_evidence 로 거절."""
    return {"seq": seq, "source": "scanner", "evidence": {"source": "webview_event_order", "pedal_pressed": False}}


def call(screen, function, *args):
    """다른 스레드(js_api 자리)에서 부르고 주인 스레드(이 시험)가 pump — 셸과 같은 길."""
    box = {}
    worker = threading.Thread(target=lambda: box.setdefault("value", function(*args)))
    worker.start()
    while worker.is_alive():
        screen.bridge.pump()
        screen.app.flush_idle()
        worker.join(0.005)
    screen.bridge.pump()
    return box["value"]


def wait_pushes(screen, count):
    for _ in range(400):
        screen.app.flush_idle()
        screen.bridge.pump()
        if len(screen.pushes) >= count:
            return
        threading.Event().wait(0.005)


def test_actual_shell_state_uses_the_unmodified_receipted_contract(screen):
    old_commit = "ac980126f2d3976be65c8cee9f8347e9f27883b2"

    def old_json(relative):
        result = subprocess.run(
            ["git", "-C", str(ROOT), "show", f"{old_commit}:{relative}"],
            capture_output=True, check=True,
            creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0),
        )
        return json.loads(result.stdout)

    assert old_json("ui/parts/MANIFEST.json")["commit"] == "c7e51fc19d74611ff9a7b6b6340208ae57d7b8a5"
    old_schema = old_json("ui/parts/app/lm/contract.json")["state"]
    assert "events_dropped" not in old_schema["properties"]
    before = screen.bridge.events.dropped
    with mock.patch.object(screen.bridge.events.fileio, "append_bytes", side_effect=OSError):
        screen.bridge.events.record("test", "contract_probe")
    state = call(screen, screen.bridge.get_state)
    assert type(state["events_dropped"]) is int and state["events_dropped"] == before + 1
    old_errors = schema_errors(old_schema, state)
    assert any('events_dropped' in error for error in old_errors)
    # Validate the actual response against the receipt, without editing either.
    assert schema_errors(CONTRACT["state"], state) == []
    assert CONTRACT["state"]["properties"]["events_dropped"] == {"type": "integer", "minimum": 0}
    for invalid in (-1, 1.5, True):
        errors = schema_errors(CONTRACT["state"], {**state, "events_dropped": invalid})
        assert errors and all("events_dropped" in error for error in errors)
    assert schema_errors(CONTRACT["state"], {**state, "unlisted_shell_field": None})


def test_state_through_the_shell_matches_the_contract(screen):
    state_schema = CONTRACT["state"]
    empty = call(screen, screen.bridge.state)
    assert schema_errors(state_schema, empty) == []
    assert empty["work"]["next"] == "현품표를 스캔하세요" and empty["work"]["scan_left"] == 5
    for seq, text in enumerate([MASTER, "A110LHD2610134821", "A110LHD2610134822"], 1):
        call(screen, screen.bridge.scan, text, meta(seq))
    state = call(screen, screen.bridge.state)
    assert schema_errors(state_schema, state) == []
    work = state["work"]
    assert (work["scan_done"], work["scan_total"], work["scan_left"]) == (3, 5, 2)
    assert work["next"] == '표본 확인 — 제품 3번째를 스캔하세요'
    assert [slot["role"] for slot in work["slots"]] == ["master_label", "product", "product", "product", "last_product"]
    assert work["slots"][-1]["key"] == "final_label"
    assert work["result"]["kind"] == "ok" and work["result"]["sound"] == "scan_master"
    assert work["tag"]["code"] == "A110" and work["quantity"] == "제품 24개"
    assert state["today"]["rows"][0]["kind"] == "ok" and state["today"]["summary"] == [{"item": "A110 1차", "count": 4}]


def test_scan_goes_through_the_one_door_then_pushes_state(screen):
    reply = call(screen, screen.bridge.scan, MASTER, meta(1))
    assert reply["accepted"] is True and reply["seq"] == 1
    assert reply["sound"] is None  # 포장 처리 층이 소리를 냈다 — 셸 소리 0
    screen.bridge._sound.play.assert_not_called()
    assert ("intake", MASTER, False) in screen.app.calls
    wait_pushes(screen, 2)
    kinds = [push["kind"] for push in screen.pushes]
    assert kinds[0] == "scan_result" and "state" in kinds[1:]  # 셸의 스캔 알림 → 처리 층이 바뀐 state
    state_push = next(push for push in screen.pushes if push["kind"] == "state")
    assert state_push["state"]["work"]["scan_done"] == 1


def test_same_seq_is_processed_once(screen):
    first = call(screen, screen.bridge.scan, MASTER, meta(1))
    again = call(screen, screen.bridge.scan, MASTER, meta(1))
    assert [entry for entry in screen.app.calls if entry[0] == "intake"] == [("intake", MASTER, False)]
    assert again.get("duplicate") is True and again["seq"] == first["seq"]


def test_processing_exception_answers_processing_error_and_next_scan_runs(screen):
    failed = call(screen, screen.bridge.scan, "BOOM", meta(1))
    assert failed["kind"] == "processing_error" and failed["accepted"] is False
    ok = call(screen, screen.bridge.scan, MASTER, meta(2))
    assert ok["accepted"] is True


def test_admin_pin_reaches_the_prompt_once_and_is_never_recorded(screen):
    pin = "".join(secrets.choice("0123456789") for _ in range(6))
    call(screen, screen.bridge.command, "recovery_list", {})
    reply = call(screen, screen.bridge.admin, "LABEL.RECHECK", {"admin_id": "admin-7", "pin": pin, "target": "S-0325"})
    assert reply["ok"] is True and reply["result"]["ok"] is True
    action = [entry for entry in screen.app.calls if entry[0] == "pin_action"]
    assert action == [("pin_action", "LABEL.RECHECK", "S-0325", "", True, True)]
    assert "_screen_admin_credentials" not in screen.app.__dict__
    state = call(screen, screen.bridge.state)
    assert state["admin"]["result"]["text"] == "다시 확인했습니다"
    written = "".join(path.read_text(encoding="utf-8") for path in screen.data.glob("*.jsonl"))
    assert pin not in written and pin not in json.dumps(state, ensure_ascii=False)


def test_admin_needs_id_pin_and_target(screen):
    reply = call(screen, screen.bridge.admin, "LABEL.RECHECK", {"admin_id": "", "pin": "", "target": "S-0325"})
    assert reply["result"]["ok"] is False
    assert not [entry for entry in screen.app.calls if entry[0] == "pin_action"]


def test_approval_runs_only_with_its_own_id(screen):
    reply = call(screen, screen.bridge.command, "cancel_completed", {"label": "L0325"})
    assert reply["result"] == {"ok": True}
    approval = reply["state"]["approval"]
    assert approval["title"] == "완료 트레이 취소" and approval["confirm"] == "취소하기"
    wrong = call(screen, screen.bridge.command, "approve", {"id": "pc-0"})
    assert wrong["result"]["ok"] is False
    assert ("cancelled", "L0325") not in screen.app.calls
    right = call(screen, screen.bridge.command, "approve", {"id": approval["id"]})
    assert right["result"] == {"ok": True}
    assert screen.app.calls.count(("cancelled", "L0325")) == 1
    assert right["state"]["approval"] is None


def test_f2_label_answers_the_prompt_and_a_miss_shows_inline(screen):
    reply = call(screen, screen.bridge.command, "cancel_completed", {"label": "L9999"})
    assert ("f2_label", "L9999") in screen.app.calls
    assert reply["result"]["ok"] is False
    assert "그 현품표의 완료 트레이가 없습니다" in reply["result"]["result"]["text"]


def test_dialogs_become_bands_and_uninstall_restores_the_module(screen):
    original = screen.processor._installed["module"]["messagebox"]
    Label_Match.messagebox.showwarning("작업 중 경고", "현재 스캔 작업이 진행 중입니다.")
    state = call(screen, screen.bridge.state)
    assert {"kind": "warn", "code": None, "text": "작업 중 경고 — 현재 스캔 작업이 진행 중입니다."} in state["alerts"]
    assert Label_Match.messagebox.askyesno("시연 모드 시작", "시연하시겠습니까?") is False
    screen.processor.uninstall()
    assert Label_Match.messagebox is original
    assert "_play_sound" not in screen.app.__dict__


def test_commands_use_the_same_entries_as_the_tk_buttons(screen):
    for name, expected in (("undo", ("shortcut", "undo")), ("exchange", ("shortcut", "f4")),
                           ("cancel_current", ("shortcut", "f1")), ("ack", ("ack",)), ("escape", ("escape",)),
                           ("empty_enter", ("intake", "", False))):
        reply = call(screen, screen.bridge.command, name, {})
        assert reply["ok"] is True, name
        assert expected in screen.app.calls, name
    unknown = call(screen, screen.bridge.command, "delete_record", {"id": "r1"})
    assert unknown["result"]["ok"] is False


def test_copy_raw_copies_that_row(screen):
    reply = call(screen, screen.bridge.command, "copy_raw", {"id": "r1"})
    assert reply["result"]["ok"] is True
    assert screen.app.clipboard == ["A110\nx1\nx2\nx3\nx4"]


def test_window_close_asks_the_app_and_keeps_the_window(screen):
    assert call(screen, screen.bridge.ask_closing) is False
    assert ("on_closing", False) in screen.app.calls


# ── 손질 셋: 진짜 Label_Match 메서드를 가짜 self 로 ──
def door(entry_text=""):
    self = types.SimpleNamespace()
    self.entry = mock.Mock()
    self.entry.get.return_value = entry_text
    self.operator_workbench_ready = False
    for name in ("_clear_scanner_notice", "_return_to_scanner_workbench", "_acknowledge_workflow_notice",
                 "_render_operator_workbench", "process_input", "_ui_lane_is_busy", "update_big_display", "_play_sound"):
        setattr(self, name, mock.Mock(name=name))
    self._reject_residual_tag_input = types.MethodType(Label_Match.Label_Match._reject_residual_tag_input, self)
    self._consume_scan_entry = mock.Mock(
        side_effect=lambda: setattr(self.entry.get, "return_value", ""))
    self.initial_entry_text = entry_text
    self._partial_submit_confirm_state = lambda: None
    self._clear_partial_submit_confirmation = lambda: None
    self.intake_scan = types.MethodType(Label_Match.Label_Match.intake_scan, self)
    return self


def test_tk_entry_and_screen_text_share_one_scan_door():
    tk_self = door(" A110 ")
    Label_Match.Label_Match._handle_scan_enter(tk_self)
    tk_self.process_input.assert_called_once_with(None, raw=tk_self.initial_entry_text.strip())  # 입력 칸 길: 지금 부름 그대로(raw 없음) — process_input 이 Entry 를 읽음
    screen_self = door()
    screen_self.intake_scan("A110", event=None)
    screen_self.process_input.assert_called_once_with(None, raw="A110")
    screen_self.entry.get.assert_not_called()


def test_both_doors_stamp_the_enter_time_and_clear_the_notice_once():
    # 바탕: Enter 시각 · 안내 지우기가 입력 칸 읽기보다 먼저 — 이음은 읽기 뒤 intake_scan 에서(차이 0 확인)
    for self, enter in ((door(" A110 "), lambda s: Label_Match.Label_Match._handle_scan_enter(s)),
                        (door(), lambda s: s.intake_scan("A110"))):
        self._last_scan_enter_at = None
        enter(self)
        assert isinstance(self._last_scan_enter_at, float)
        self._clear_scanner_notice.assert_called_once_with()
        self.process_input.assert_called_once()

def test_pending_error_is_acknowledged_by_either_door():
    self = door()
    self._pending_workflow_error = {"result": "FAIL"}
    self.intake_scan("NEXT")
    self._acknowledge_workflow_notice.assert_called_once()
    self.process_input.assert_not_called()


def test_screen_scan_never_reads_or_clears_the_entry():
    self = types.SimpleNamespace(entry=mock.Mock(), _scans_behind_completion=None)
    self._ui_lane_is_busy = mock.Mock(return_value=True)
    self._completion_scan_queue_open = mock.Mock(return_value=True)
    self._hold_scan_behind_completion = mock.Mock()
    self._reject_residual_tag_input = types.MethodType(Label_Match.Label_Match._reject_residual_tag_input, self)
    self._consume_scan_entry = mock.Mock()
    Label_Match.Label_Match.process_input(self, raw=" A110LHD2610134821 ")
    self._hold_scan_behind_completion.assert_called_once_with("A110LHD2610134821")
    assert self.entry.mock_calls == []
    self.entry.get.return_value = "FROM-ENTRY"
    Label_Match.Label_Match.process_input(self)
    self._hold_scan_behind_completion.assert_called_with("FROM-ENTRY")


def test_admin_prompt_returns_the_screen_pair_once():
    pair = ("admin-7", "".join(secrets.choice("0123456789") for _ in range(6)))
    self = types.SimpleNamespace(run_tests=True, _screen_admin_credentials=pair)
    assert Label_Match.Label_Match._prompt_package_admin_pin(self) == pair
    assert "_screen_admin_credentials" not in self.__dict__
    assert Label_Match.Label_Match._prompt_package_admin_pin(self) is None


def test_new_screen_flag_is_off_by_default():
    assert Label_Match.LABEL_MATCH_SCREEN_HOST == {"requested": False, "active": False, "run": None,
                                               "catalog_unconfirmed": False}
    assert Label_Match.LABEL_MATCH_NEW_SCREEN_ARGUMENT == "--new-screen"


def test_screen_page_and_every_part_it_loads_sit_under_the_http_root():
    import re
    import label_match_screen

    root = label_match_screen.PARTS.resolve()
    page = label_match_screen.PAGE.resolve()
    assert page.is_file() and root in page.parents
    refs = re.findall(r'(?:href|src)="([^"]+)"', page.read_text(encoding="utf-8"))
    assert refs and all(not ref.startswith(("http:", "https:", "//")) for ref in refs)
    for ref in refs:
        target = (page.parent / ref).resolve()
        assert target.is_file() and root in target.parents, ref


def _start_gate(monkeypatch, *, catalog):
    calls = []
    screen = types.ModuleType("label_match_screen")
    screen.screen_runner = lambda module: (lambda: calls.append("window") or 0)
    screen.ScreenUnavailable = type("ScreenUnavailable", (RuntimeError,), {})
    monkeypatch.setitem(sys.modules, "label_match_screen", screen)
    monkeypatch.setitem(Label_Match.LABEL_MATCH_SCREEN_HOST, "requested", True)
    monkeypatch.setattr(Label_Match, "prepare_startup_item_catalog", catalog)
    monkeypatch.setattr(Label_Match, "offline_catalog_startable", lambda **_kwargs: False)
    monkeypatch.setattr(Label_Match, "_offer_item_catalog_startup_retry", lambda error: calls.append("retry offered") and False)
    monkeypatch.setattr(Label_Match, "legacy_label_transition_enabled", lambda: False)
    monkeypatch.setattr(Label_Match, "_label_match_startup_trace", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(Label_Match, "Label_Match", lambda: calls.append("tk app") or pytest.fail("Tk app on the main thread"))
    return calls


def test_new_screen_window_opens_only_after_the_catalog_gate_fails_closed(monkeypatch):
    def refused():
        raise Label_Match.ItemCatalogSyncError("catalog unavailable", cause_code=Label_Match.SNAPSHOT_UNAVAILABLE_AFTER_VERIFY)
    calls = _start_gate(monkeypatch, catalog=refused)
    with pytest.raises(Label_Match.ItemCatalogSyncError):  # main() 이 지금 안내 · 종료 코드 그대로
        Label_Match._run_label_match_application()
    assert calls == ["retry offered"]  # 창 0 · Tk 앱 0


def test_new_screen_window_opens_once_after_the_gates_pass(monkeypatch):
    calls = _start_gate(monkeypatch, catalog=lambda: None)
    assert Label_Match._run_label_match_application() == 0
    assert calls == ["window"]


def test_owner_that_cannot_build_the_app_ends_the_start_wait_without_a_window():
    import label_match_screen

    def refused():
        raise RuntimeError("start gate refused")
    module = types.SimpleNamespace(LABEL_MATCH_SCREEN_HOST={"active": False, "run": None},
                                   _run_label_match_application=refused)
    bridge = mock.Mock()
    bridge.fail_owner.side_effect = [None, RuntimeError("fail_owner: external owner only, before attach")]
    label_match_screen.owner_main_for(module)(bridge)
    assert str(bridge.fail_owner.call_args_list[0].args[0]) == "start gate refused"
    bridge.attach_owner.assert_not_called()


def test_screen_runner_passes_bounded_before_exit_and_release_version(monkeypatch, tmp_path):
    import label_match_screen
    passed = {}
    fake = types.ModuleType("host")
    fake.run_app = lambda *args, **kwargs: passed.update(kwargs) or 'web'
    from tests.test_screen_precreation_recovery import shell_runtime
    shell_runtime.__wrapped__(monkeypatch, tmp_path)
    monkeypatch.setitem(sys.modules, "host", fake)
    waits = []
    monkeypatch.setattr(label_match_screen, "close_and_wait_writers", lambda seconds: waits.append(seconds) or True)
    assert label_match_screen.screen_runner(Label_Match)() == 0
    assert passed["version"] == Label_Match.APP_VERSION
    assert passed["before_exit"]("SHELL_START_FAILED", 240) is True
    assert passed["before_exit"]("SHELL_START_FAILED", 0) is True
    assert waits == [120, 0]
    assert passed["root"] == label_match_screen.PARTS
