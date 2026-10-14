"""포장 새 화면 이음 — 셸 host(v3.8)의 처리 층 객체와 새 화면 state 모으기.

꼴은 ui/parts/app/lm/contract.json(lm-state/1, 웹 저장소 static/ui/app/lm 을 내보낸 사본)이다.
띄우기는 label_match_screen.py(HOST-USE §4 나: owner_main)가 한다.

- 메서드는 모두 주인 스레드(포장 앱 Tk 스레드)에서만 불린다. 셸 bridge 가 js_api 를 접수만 하고 pump 로 넘긴다.
- 숨긴 Tk 화면은 그대로 산다. 처리 규칙 · 저장 · 대기열 · 동기화와 단추 · 입력 칸 gate 는 지금 코드가 계산하고,
  이음은 그 값을 읽어 모은다(같은 앱의 두 화면이 같은 진실).
- 새 화면 판에서 길이 바뀌는 곳은 APP-SCREENS-1013 불변식 1 의 손질 셋이다.
  ① 스캔 글 = Label_Match.intake_scan 인수(입력 칸 아님).
  ② 관리자 PIN = admin 인수(_screen_admin_credentials, Tk 창 아님).
  ③ 작업 흐름 안 messagebox · 설정 창 = 띠 · 판(아래 받이).
"""
from __future__ import annotations

import json
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

STATE_CONTRACT = "lm-state/1"
PARTS_DIR = Path(__file__).resolve().parent / "ui" / "parts"
SOUNDS = frozenset({"scan_master", "scan_1", "pass", "fail", "error"})
BAND = {"danger": "bad", "warning": "warn", "info": "quiet", "muted": "quiet", "success": "quiet", "primary": "quiet"}
FLASH = {"red": "bad", "green": "ok", "primary": "idle"}
HIDDEN_BADGES = frozenset({"중앙 멤버십", "PHS2 서버 멤버십"})
SET_ACTIONS = frozenset({"LABEL.RECHECK", "LABEL.SET_HOLD", "LABEL.F5_HOLD", "LABEL.SUPPORT_HANDOFF"})
HISTORY_LIMIT = 100  # state 는 push 마다 화면으로 감 — 오늘 기록은 최근 100줄(4 GB PC)
DIALOG_LIMIT = 2


def _parts_fingerprint():
    try:
        return str(json.loads((PARTS_DIR / "MANIFEST.json").read_text(encoding="utf-8")).get("parts_fingerprint") or "") or None
    except (OSError, ValueError):
        return None


def _text(value):
    return " ".join(str(value or "").split())


class _ScreenWindow:
    """Tk Toplevel 자리(관리자 요청 대기 · 설정 저장) — 열려 있는 동안 winfo_exists() 가 True."""

    def __init__(self):
        self.alive = True

    def winfo_exists(self):
        return self.alive

    def destroy(self):
        self.alive = False

    def grab_release(self):
        return None


class _ScreenDialogs:
    """새 화면 판의 앱 모듈 messagebox · simpledialog 자리 — Tk 창 대신 띠 한 줄(물음은 「아니요」)."""

    def __init__(self, processor):
        self._processor = processor

    def showerror(self, title=None, message=None, **_options):
        self._processor._dialog("bad", title, message)
        return "ok"

    def showwarning(self, title=None, message=None, **_options):
        self._processor._dialog("warn", title, message)
        return "ok"

    def showinfo(self, title=None, message=None, **_options):
        self._processor._dialog("quiet", title, message)
        return "ok"

    def askyesno(self, title=None, message=None, **_options):
        self._processor._dialog("warn", title, message)
        return False

    askokcancel = askretrycancel = askyesno

    def askyesnocancel(self, title=None, message=None, **_options):
        self._processor._dialog("warn", title, message)
        return None

    def askstring(self, title=None, prompt=None, **_options):
        return self._processor._take_answer()


class LabelMatchScreenProcessor:
    """셸 bridge 의 처리 층 객체(HOST-USE §3) — state · scan · command · admin · closing."""

    def __init__(self, app, bridge=None, module=None):
        self.app = app
        self.bridge = bridge
        self.module = module or sys.modules[type(app).__module__]
        self._installed = {}
        self._serial = 0
        self._flash = None
        self._sound = None
        self._dialogs = []
        self._answers = []
        self._recovery = None
        self._pending = None
        self._admin_result = None
        self._admin_outcome = None
        self._register = None
        self._push_scheduled = False
        self._parts = _parts_fingerprint()
        self._install()

    # ── 받이: 앱 객체에 붙이는 것(처리 규칙은 그대로, 보이는 길만) ──
    def _install(self):
        app = self.app
        dialogs = _ScreenDialogs(self)
        self._installed = {"module": {name: getattr(self.module, name) for name in ("messagebox", "simpledialog")},
                           "app": [name for name in ("_play_sound", "update_big_display", "_render_operator_workbench",
                                                     "_render_logistics_connection_footer", "open_settings_window")
                                   if name not in app.__dict__]}
        self.module.messagebox = dialogs
        self.module.simpledialog = dialogs
        play, big = app._play_sound, app.update_big_display
        render, footer = app._render_operator_workbench, app._render_logistics_connection_footer

        def play_sound(sound_key, block=False):
            self._sound = (self._serial, str(sound_key))
            return play(sound_key, block)

        def update_big_display(text, color=""):
            self._flash = (self._serial, str(text or ""), str(color or ""))
            return big(text, color)

        def render_operator_workbench():
            view = render()
            self._changed("workbench")
            return view

        def render_connection_footer():
            result = footer()
            self._changed("connection")
            return result

        def open_settings_window():  # 작업자 고르기 = 판(state.settings.required 가 엶)
            refresh = getattr(app, "_request_worker_roster_refresh", None)
            if callable(refresh):
                refresh()
            self._changed("settings")

        app._play_sound = play_sound
        app.update_big_display = update_big_display
        app._render_operator_workbench = render_operator_workbench
        app._render_logistics_connection_footer = render_connection_footer
        app.open_settings_window = open_settings_window
        self._installed["prompt"] = app.__dict__.get("_prompt_package_admin_pin")

        def prompt_package_admin_pin():
            credentials = app.__dict__.pop("_screen_admin_credentials", None)
            if credentials is not None and all(credentials):
                return credentials
            app._admin_pin_last_message = "관리자 인증이 다시 필요합니다. 관리자 ID와 PIN을 입력해 다시 조회하세요."
            app._admin_pin_result_outcome = "unknown"
            return None

        app._prompt_package_admin_pin = prompt_package_admin_pin

    def uninstall(self):
        """받이를 되돌림(시험 · 화면 판을 끝낼 때) — 모듈 messagebox · simpledialog 와 앱 메서드."""
        if not self._installed:
            return
        for name, value in self._installed.get("module", {}).items():
            setattr(self.module, name, value)
        for name in self._installed.get("app", []):
            self.app.__dict__.pop(name, None)
        original_prompt = self._installed.get("prompt")
        if original_prompt is None:
            self.app.__dict__.pop("_prompt_package_admin_pin", None)
        else:
            self.app._prompt_package_admin_pin = original_prompt
        self._installed = {}

    def _completion_outcome(self, value):
        if value is True:
            outcome = "success"
        elif value is False:
            outcome = "failure"
        elif self.app.__dict__.get("_admin_pin_result_outcome") == "success":
            outcome = "success"
        elif isinstance(value, str) and value:
            outcome = "failure"
        else:
            outcome = "unknown"
        self._admin_outcome = outcome
        return outcome == "success"

    def _changed(self, reason):
        if self.bridge is None or self._push_scheduled:
            return
        self._push_scheduled = True

        def push():
            self._push_scheduled = False
            self.bridge.push_state(reason=reason)

        try:
            self.app.after_idle(push)
        except Exception:
            push()

    def _dialog(self, kind, title, message):
        title, message = _text(title), _text(message)
        text = f"{title} — {message}" if title and message else title or message
        if not text:
            return
        self._dialogs = (self._dialogs + [{"kind": kind, "code": None, "text": text}])[-DIALOG_LIMIT:]
        self._changed("dialog")

    def _take_answer(self):
        return self._answers.pop(0) if self._answers else None

    # ── 셸이 부르는 넷 ──
    def state(self):
        return present_lm(self)

    def scan(self, text, info):
        self._serial += 1
        self._dialogs = []
        self.app.intake_scan(text)
        result = _result(self, *_view_and_source(self.app))
        # 포장 처리 층이 소리를 이미 냈다(_play_sound) — 셸 소리는 0.
        return {"accepted": True, "verdict": None, "kind": "scan_result", "sound": None,
                "message": result["text"], "result": {"kind": result["kind"]}}

    def command(self, name, args):
        handler = getattr(self, f"_command_{name}", None)
        if not callable(handler):
            return {"ok": False, "result": {"kind": "warn", "text": "이 화면에서 쓰지 않는 명령입니다"}}
        self._serial += 1
        return handler(dict(args or {})) or {"ok": True}

    def admin(self, name, args):
        args = dict(args or {})
        admin_id, pin = str(args.get("admin_id") or "").strip(), str(args.get("pin") or "")
        if name not in SET_ACTIONS and name != "LABEL.PHS_LOOKUP":
            return {"ok": False, "result": {"kind": "warn", "text": "이 화면에서 쓰지 않는 관리자 동작입니다"}}
        if not admin_id or not pin:
            return {"ok": False, "result": {"kind": "warn", "text": "관리자 ID 와 PIN 6자리를 넣으세요"}}
        target = str(args.get("target") or "")
        physical_qr = str(args.get("physical_qr") or "").strip()
        if name in SET_ACTIONS and not target:
            return {"ok": False, "result": {"kind": "warn", "text": "멈춘 작업 확인 목록에서 고른 트레이가 없습니다"}}
        if name == "LABEL.PHS_LOOKUP" and not physical_qr:
            return {"ok": False, "result": {"kind": "warn", "text": "실물 현품표를 찍으세요"}}
        action = name
        if name in {"LABEL.SET_HOLD", "LABEL.F5_HOLD"}:  # 지금 창과 같게 F5: 대상은 F5 보류(:11788)
            action = "LABEL.F5_HOLD" if target.startswith("F5:") else "LABEL.SET_HOLD"
        set_id = target if name in SET_ACTIONS else "PHS_LOOKUP"
        app, window = self.app, _ScreenWindow()

        def settle(value, ok):
            app.__dict__.pop("_screen_admin_credentials", None)
            window.destroy()
            text = _text(value) if value not in (None, False, True) else _text(app.__dict__.get("_admin_pin_last_message"))
            self._admin_result = {"action": action, "ok": bool(ok), "text": text or ("했습니다" if ok else "관리자 확인을 마치지 못했습니다")}
            self._load_recovery()
            self._changed("admin")

        app._admin_pin_result_outcome = "unknown"
        app._screen_admin_credentials = (admin_id, pin)
        try:
            accepted = app._submit_package_pin_action(
                action, set_id, physical_qr, window=window,
                finish=lambda value: settle(value, self._completion_outcome(value)),
                fail=lambda error: settle(None, False),
            )
        except Exception:
            app.__dict__.pop("_screen_admin_credentials", None)
            raise
        if not accepted:
            app.__dict__.pop("_screen_admin_credentials", None)
            return {"ok": False, "result": {"kind": "warn", "text": "지금은 관리자 요청을 받지 못했습니다 — 잠시 뒤 다시 하세요"}}
        return {"ok": True, "result": {"kind": "saved", "text": "회사 서버에서 확인하는 중입니다",
                                       "sub": "결과는 멈춘 작업 확인 판 맨 위에 나옵니다 — 창을 닫아도 끝까지 진행됩니다"}}

    def closing(self):
        """창 X(셸 v3.7) — 지금 Tk 의 끝내기 길: 하던 트레이가 있으면 마우스 승인으로 묻고 닫기는 막음."""
        app = self.app
        app.on_closing()
        if app.__dict__.get("_pointer_confirmation") is not None or not app.__dict__.get("_tk_shutdown_requested", False):
            self._changed("closing")
            return False
        return True

    # ── 명령(지금 Tk 단추 · 단축키와 같은 입구) ──
    def _command_start_retry(self, _args):
        if not self.app._retry_initial_load():
            return {"ok": False, "result": {"kind": "warn", "text": "지금은 다시 시도할 수 없습니다"}}
        self._changed("start_retry")

    def _command_undo(self, _args):
        self.app._handle_workflow_shortcut("undo")

    def _command_exchange(self, _args):
        self.app._handle_workflow_shortcut("f4")

    def _command_cancel_current(self, _args):
        self.app._handle_workflow_shortcut("f1")

    def _command_cancel_completed(self, args):
        label = str(args.get("label") or "").strip()
        before = len(self._dialogs)
        self._answers = [label]
        try:
            self.app._handle_workflow_shortcut("f2")
        finally:
            self._answers = []
        if self.app.__dict__.get("_pointer_confirmation") is not None:
            return {"ok": True}
        if len(self._dialogs) > before or self._dialogs:
            return {"ok": False, "result": {"kind": "warn", "text": self._dialogs[-1]["text"]}}
        return {"ok": True}

    def _command_empty_enter(self, _args):  # 받이 버퍼가 빈 Enter — Tk 입력 칸의 빈 Enter 와 같은 문
        self.app.intake_scan("")

    def _command_ack(self, _args):
        self.app._acknowledge_workflow_notice()

    def _command_escape(self, _args):
        self._dialogs = []
        self.app._handle_workflow_escape()

    def _command_approve(self, args):
        app = self.app
        pending = _pointer_pending(app)
        if pending is None or _approval_id(pending) != str(args.get("id") or ""):
            return {"ok": False, "result": {"kind": "warn", "text": "승인할 일이 바뀌었습니다 — 다시 확인하세요"}}
        # _confirm_pointer_action(:21451) 의 실행 줄 — 단추 안 누름 · 뗌 확인은 화면(pointerdown/up)이 했다.
        app._pointer_confirmation = None
        app._workflow_notice_action = None
        app._workflow_notice_action_text = "확인"
        app._phs_label_guidance_notice = None
        app._render_operator_workbench()
        pending[2]()
        return {"ok": True}

    def _command_dismiss(self, args):
        app = self.app
        pending = _pointer_pending(app)
        if pending is None or _approval_id(pending) != str(args.get("id") or ""):
            return {"ok": True}
        app._pointer_confirmation = None
        app._workflow_notice_action = None
        app._workflow_notice_action_text = "확인"
        app._phs_label_guidance_notice = None
        app._render_operator_workbench()
        return {"ok": True}

    def _command_close(self, _args):
        self.closing()
        return {"ok": True}

    def _command_select_worker(self, args):
        name = str(args.get("id") or "").strip()
        before = len(self._dialogs)
        self.app._save_settings_and_close(_ScreenWindow(), name)
        if len(self._dialogs) > before:
            return {"ok": False, "result": {"kind": "warn", "text": self._dialogs[-1]["text"]}}
        return {"ok": True}

    def _command_register_worker(self, args):
        app, roster_module = self.app, self.module.label_worker_roster
        try:
            name = roster_module.normalize_registration_name(args.get("name"))
            if self._register and self._register.get("different_from") == name:
                raise ValueError("다른 분이면 구분할 이름을 적어 주세요. 예: 이름B")
            roster = app.__dict__.get("_roster_b")
            duplicates = roster.duplicate_workers(name) if roster is not None else []
            if duplicates:  # 같은 이름이 있음 — 이 판이 후보 줄로 바뀜(같은 사람 · 다른 사람, :7416–7425)
                self._register = {"name": name, "candidates": list(duplicates), "different_from": None}
                return {"ok": True, "sheet": {"kind": "list", "title": "새 이름 등록"}}
            app._register_worker_name(name, existing_worker_id="")
        except ValueError as exc:
            return {"ok": False, "result": {"kind": "warn", "text": str(exc)}}
        except (OSError, self.module.WriterFenceError):
            return {"ok": False, "result": {"kind": "warn", "text": "이름을 저장하지 못했습니다. 입력은 남아 있습니다. 다시 등록하세요."}}
        self._register = None
        return {"ok": True}

    def _command_register_same(self, args):
        app, register = self.app, self._register or {}
        chosen = next((row for row in register.get("candidates", []) if str(row.get("worker_id")) == str(args.get("id"))), None)
        roster = app.__dict__.get("_roster_b")
        if chosen is None or roster is None:
            return {"ok": False, "result": {"kind": "warn", "text": "이름이 바뀌었습니다. 새 이름 등록을 다시 하세요."}}
        try:
            current = roster.duplicate_workers(register["name"])
        except ValueError:
            current = []
        if not any(roster.same_person(row["worker_id"], chosen["worker_id"]) for row in current):
            return {"ok": False, "result": {"kind": "warn", "text": "이름이 바뀌었습니다. 새 이름 등록을 다시 하세요."}}
        try:
            app._register_worker_name(register["name"], existing_worker_id=chosen["worker_id"])
        except (OSError, ValueError, self.module.WriterFenceError):
            return {"ok": False, "result": {"kind": "warn", "text": "이름을 저장하지 못했습니다. 다시 등록하세요."}}
        self._register = None
        return {"ok": True}

    def _command_register_other(self, _args):
        register = self._register or {}
        self._register = {"name": register.get("name", ""), "candidates": [], "different_from": register.get("name")}
        return {"ok": True}

    def _command_history(self, args):
        if args:
            return {"ok": False, "code": "HISTORY_TODAY_ONLY",
                    "result": {"kind": "warn", "text": "지난 날짜 기록은 웹 관리 화면에서 확인하세요."}}
        self.app._load_history_and_rebuild_summary(None)

    def _command_scan_cancel(self, args):
        app = self.app

        def reply(ok, code, deleted=0):
            return {"ok": ok, "code": code, "deleted": deleted,
                    "result": {"kind": "saved" if ok else "warn",
                               "text": "선택한 스캔 기록을 확인하세요." if ok else "기록을 새로 확인하고 다시 선택하세요."}}

        rows = args.get("rows")
        if (set(args) != {"rows"} or not isinstance(rows, list)
                or not 0 < len(rows) <= HISTORY_LIMIT
                or any(not isinstance(row, str) or not row for row in rows)
                or len(rows) != len(set(rows))):
            return reply(False, "SCAN_CANCEL_BAD_INPUT")
        if _pointer_pending(app) is not None:
            return reply(False, "SCAN_CANCEL_CONFIRMATION_PENDING")
        if not app.__dict__.get("history_view_updates_active_state", True):
            return reply(False, "SCAN_CANCEL_READ_ONLY")
        if (app.__dict__.get("_app_close_in_progress") or app._ui_lane_is_busy()
                or app.__dict__.get("history_active_load_pending")
                or app.__dict__.get("_history_active_load_error")):
            return reply(False, "SCAN_CANCEL_BUSY")
        visible = {row["row"] for row in _history_rows(app)}
        if any(row not in visible for row in rows):
            return reply(False, "SCAN_CANCEL_TARGET_CHANGED")
        before = len(self._dialogs)
        app.history_tree.selection_set(rows)
        try:
            app._delete_selected_row()
        except Exception:
            deleted = sum(not app.history_tree.exists(row) for row in rows)
            self._changed("scan_cancel")
            return reply(False, "SCAN_CANCEL_FAILED", deleted)
        deleted = sum(not app.history_tree.exists(row) for row in rows)
        self._changed("scan_cancel")
        if _pointer_pending(app) is not None:
            return reply(True, "SCAN_CANCEL_CONFIRMATION_REQUIRED")
        if any(row["kind"] == "bad" for row in self._dialogs[before:]):
            return reply(False, "SCAN_CANCEL_FAILED", deleted)
        return reply(bool(deleted), "SCAN_CANCEL_APPLIED" if deleted else "SCAN_CANCEL_NOT_APPLIED", deleted)

    def _command_refresh_progress(self, _args):
        self.app._refresh_logistics_progress()

    def _command_recovery_list(self, _args):
        self._admin_result = None
        self._load_recovery()

    def _command_recovery_park(self, args):
        app = self.app
        target = str(args.get("target") or "")
        item = next((row for row in app._package_recovery_candidates() if str(row["set_id"]) == target), None)
        if item is None or target in app.__dict__.get("_package_pin_pending_targets", ()) or app._ui_lane_is_busy():
            return {"ok": False, "result": {"kind": "warn", "text": "지금은 이 건을 보류할 수 없습니다"}}
        parked = app._park_unverified_pin_item(target)
        self._admin_result = {"action": "recovery_park", "ok": bool(parked),
                              "text": _text(app.__dict__.get("_admin_pin_last_message") or "보류하지 못했습니다")}
        self._load_recovery()
        self._changed("recovery_park")
        return {"ok": bool(parked)}

    def _command_recovery_retry_pin(self, args):
        app = self.app
        target = str(args.get("target") or "")
        item = next((row for row in app._package_recovery_candidates() if str(row["set_id"]) == target), None)
        if item is None or not item.get("pin_intent"):
            return {"ok": False, "result": {"kind": "warn", "text": "다시 확인할 관리자 요청이 없습니다"}}
        admin_id, pin = str(args.get("admin_id") or "").strip(), str(args.get("pin") or "")
        if not admin_id or not pin:
            return {"ok": False, "result": {"kind": "warn", "text": "관리자 ID 와 PIN 6자리를 넣으세요"}}
        app._admin_pin_result_outcome = "unknown"
        app._screen_admin_credentials = (admin_id, pin)
        window = _ScreenWindow()

        def settle(value, ok):
            app.__dict__.pop("_screen_admin_credentials", None)
            window.destroy()
            self._admin_result = {"action": "recovery_retry_pin", "ok": bool(ok),
                                  "text": _text(value or app.__dict__.get("_admin_pin_last_message") or "결과를 확인하지 못했습니다")}
            self._load_recovery()
            self._changed("recovery_retry_pin")

        try:
            accepted = app._retry_package_pin_item(item, window=window, physical_qr=str(args.get("physical_qr") or "").strip(),
                                                  finish=lambda value: settle(value, self._completion_outcome(value)),
                                                  fail=lambda _error: settle(None, False))
        except Exception:
            app.__dict__.pop("_screen_admin_credentials", None)
            window.destroy()
            raise
        if not accepted:
            app.__dict__.pop("_screen_admin_credentials", None)
            window.destroy()
        return {"ok": bool(accepted)}

    def _command_pending_status(self, _args):
        app = self.app
        self._pending = {"rows": [
            {"kind": "회사 서버", "count": int(app.__dict__.get("_logistics_pending_transmissions") or 0), "text": "회사 서버로 보낼 것"},
            {"kind": "관리자", "count": int(app.__dict__.get("_logistics_admin_review_count") or 0), "text": "관리자 확인 필요"},
        ]}

    def _command_copy_raw(self, args):
        row = next((item for item in _history_rows(self.app) if item["row"] == str(args.get("id") or "")), None)
        if row is None:
            return {"ok": False, "result": {"kind": "warn", "text": "그 기록을 찾지 못했습니다"}}
        self.app.clipboard_clear()
        self.app.clipboard_append("\n".join(row["raw"]))
        return {"ok": True, "result": {"kind": "ok", "text": "복사했습니다"}}

    def _load_recovery(self):
        try:
            candidates = self.app._package_recovery_candidates()
        except Exception:
            self._recovery = {"loading": False, "rows": []}
            self._admin_result = {"action": "recovery_list", "ok": False, "text": "복구 목록을 확인하지 못했습니다. 저장 폴더를 확인하고 다시 시도하세요."}
            return
        self._recovery = {"loading": False, "rows": [_recovery_row(self.app, item) for item in candidates]}


# ── state 모으기(부작용 0 — 숨긴 Tk 화면이 이미 계산한 값을 읽음) ──
def _pointer_pending(app):
    pending = app.__dict__.get("_pointer_confirmation")
    if pending and app.__dict__.get("_workflow_notice_action") is pending[1]:
        return pending
    return None


def _approval_id(pending):
    return f"pc-{id(pending):x}"


def _view_and_source(app):
    return app.__dict__.get("_last_workflow_view"), app._workflow_view_source()


def _display_notice(app, view):
    if view is None:
        return None
    deferred = app._deferred_capture_pending_notice(view) or app._deferred_capture_failure_notice()
    return (deferred or view.notice or app.__dict__.get("_package_create_review_notice")
            or app.__dict__.get("_package_cancellation_review_notice")
            or app.__dict__.get("_phs_label_guidance_notice") or app.__dict__.get("_ui_lane_review_notice"))


def _result(processor, view, source):
    """방금 결과 — 지금 Tk 결과 줄(:22093–22107)과 같은 갈래 + 이번 차례의 큰 글 · 소리."""
    app = processor.app
    flash = processor._flash if processor._flash and processor._flash[0] == processor._serial else None
    sound = processor._sound[1] if processor._sound and processor._sound[0] == processor._serial else None
    sound = "scan_master" if sound in {"scan_2", "scan_3"} else sound if sound in SOUNDS else None
    notice = _display_notice(app, view)
    if view is None:
        return {"kind": "idle", "text": "준비 중입니다", "sub": None, "sound": None, "code": None}
    rejected = bool(view.current_stage in {"error", "completion_failed"} or (
        notice is not None and (notice.tone == "danger" or notice.kind == "scanner_notice")))
    if flash and FLASH.get(flash[2]) == "bad":
        return {"kind": "bad", "text": _text(flash[1]), "sub": _text(notice.message) if rejected and notice else None, "sound": sound, "code": None}
    if rejected:
        title = _text(notice.title) if notice is not None else "틀림"
        return {"kind": "bad", "text": title, "sub": _text(app._compact_operator_notice_message(notice.message)) if notice is not None else None,
                "sound": sound, "code": None}
    if view.completion_kind == "full":
        return {"kind": "ok", "text": "트레이 완료 · 이 PC에 저장됨", "sub": None, "sound": sound, "code": None}
    if flash and flash[1]:
        return {"kind": FLASH.get(flash[2], "idle"), "text": _text(flash[1]), "sub": None, "sound": sound, "code": None}
    if view.qa_completed or view.completion_kind in {"full", "partial"}:
        last = str(view.last_normal_scan or "")
        return {"kind": "ok", "text": "맞음", "sub": f"…{last[-4:]}" if last else None, "sound": sound, "code": None}
    return {"kind": "idle", "text": "아직 찍은 것이 없습니다", "sub": None, "sound": sound, "code": None}


def _enabled(app, name, fallback):
    widget = app.__dict__.get(name)
    if widget is None:
        return bool(fallback)
    try:
        return str(widget.cget("state")) != "disabled"
    except Exception:
        return bool(fallback)


def _tag(app, source):
    scans = list(source.get("parsed") or [])
    item_code = str(scans[0] if scans else "")
    snapshot = app.__dict__.get("_workflow_item_snapshot") or {}
    if not item_code and snapshot:
        item_code = str(snapshot.get("item_code") or "")
    if not item_code:
        return None
    info = (app.__dict__.get("items_data") or {}).get(item_code, {})
    return {"code": item_code,
            "item": str(source.get("item_name_override") or snapshot.get("item_name_override") or info.get("Item Name") or "-"),
            "spec": str(info.get("Spec") or snapshot.get("spec") or ""),
            "phase": str(source.get("phase") or snapshot.get("phase") or "")}


def _quantity(app, view, source):
    if app._standard_phs2_workflow_expected(source):
        count = app._operator_package_quantity(source)
        return f"제품 {count:,}개" if count is not None else "제품 수량 확인 중" if source.get("raw") else None
    if view.exact_rescan.status in {"active", "complete"}:
        return None
    return None


def _legacy(app, module):
    if not app.__dict__.get("_legacy_label_transition_enabled", False):
        return {"enabled": False, "text": None}
    label = app.__dict__.get("legacy_transition_label")
    try:
        text = _text(label.cget("text")) if label is not None else ""
    except Exception:
        text = ""
    return {"enabled": True, "text": text or module.LEGACY_LABEL_TRANSITION_BANNER}


def _next(app, view, source, notice):
    raw = source.get("raw") or []
    exchange = source.get("scanner_exchange_phase")
    if exchange == "target":
        return "교환할 불량 제품을 스캔하세요", [{"text": "불량 제품", "mark": "on"}, {"text": " · ", "mark": "plain"}, {"text": "대체 제품", "mark": "off"}]
    if exchange == "replacement":
        return "검사실의 대체 제품을 스캔하세요", [{"text": "불량 제품", "mark": "off"}, {"text": " · ", "mark": "plain"}, {"text": "대체 제품", "mark": "on"}]
    total = view.qa_total
    if not raw:
        return "현품표를 스캔하세요", [{"text": f"현품표 1번 + 상단 제품 {total - 1}번 = {total}번 찍으면 저절로 끝납니다", "mark": "plain"}]
    if view.notice is None and 0 < view.qa_completed < total and view.exact_rescan.status != "active":
        return f"상단 제품 {view.qa_completed}번째를 스캔하세요", [{"text": f"{total}번째를 찍으면 저절로 끝납니다", "mark": "plain"}]
    sub = [{"text": _text(view.notice.message), "mark": "plain"}] if view.notice is not None and view.completion_kind else []
    return _text(view.next_action), sub


def _slots(view, source):
    target = source.get("scanner_exchange_target")
    total = len(view.slots)
    rows = []
    for slot in view.slots:
        state = slot.state
        if target and slot.filled and slot.value == target:
            state = "swap"
        role = "master_label" if slot.index == 1 else "last_product" if slot.index == total else "product"
        rows.append({"index": slot.index, "key": slot.key, "label": slot.label, "value": str(slot.value or ""),
                     "filled": bool(slot.filled), "state": state, "role": role})
    return rows


def _conn(app, module):
    words = module.LABEL_MATCH_STATUS_LINE_TEXT
    observation = app.__dict__.get("_logistics_connection_observation")
    connected = observation[1] if observation else None
    count = app.__dict__.get("_logistics_pending_transmissions")
    review = app.__dict__.get("_logistics_admin_review_count", 0)
    oldest = app.__dict__.get("_logistics_oldest_pending_at")
    age = max(0, int((datetime.now(timezone.utc) - oldest).total_seconds())) if oldest else 0
    age = max(age, app.__dict__.get("_logistics_oldest_pending_age", 0))
    suffix = words["send_count"].format(count=count) if count is not None else ""
    latest = app.__dict__.get("_logistics_last_transmission")
    last_ok = latest.astimezone().strftime("%H:%M") if latest is not None else None
    if review:
        state, label = "review", words["review"][0].format(review=review) + suffix
    elif connected is False:
        state, label = "offline", words["offline"][0] + suffix
    elif connected is True and count and age > 600:
        state, label = "late", words["accumulating"][0].format(count=count) + (words["last_send"].format(stamp=last_ok) if last_ok else "")
    elif connected is None:
        state, label = "first", words["checking"][0]
    else:
        state, label = "ok", words["normal"][0] + suffix
    return {"state": state, "label": label, "last_ok_at": last_ok}


def _alerts(processor, view, notice):
    app, module = processor.app, processor.module
    alerts = []
    pointer = _pointer_pending(app)
    if notice is not None and not (pointer is not None and notice is pointer[0]):
        message = _text(app._compact_operator_notice_message(notice.message))
        band = {"kind": BAND.get(str(notice.tone or "danger"), "warn"), "code": None,
                "text": f"{_text(notice.title)} — {message}" if message else _text(notice.title)}
        if (app.__dict__.get("_pending_workflow_error") or app.__dict__.get("_workflow_pending_error")
                or callable(app.__dict__.get("_workflow_notice_action"))):
            action_text = _text(app.__dict__.get("_workflow_notice_action_text") or "확인")
            band["choices"] = [{"text": f"{action_text} (Esc)", "go": "command:ack", "default": True}]
        alerts.append(band)
    if app.__dict__.get("_logistics_relay_state") == "stopped":
        alerts.append({"kind": "warn", "code": None, "text": module.LABEL_MATCH_RELAY_SCREEN_TEXT["stopped"]})
    alerts.extend(processor._dialogs)
    return alerts[:4]


def _history_rows(app):
    tree = app.__dict__.get("history_tree")
    if tree is None:
        return []
    total = int(getattr(app, "TOTAL_SCAN_COUNT", 5))
    rows = []
    try:
        children = [iid for iid in tree.get_children() if str(iid) != "loading"]
        for number, iid in enumerate(children, 1):
            if number <= len(children) - HISTORY_LIMIT:
                continue
            values = [str(value) for value in (tree.item(iid, "values") or ())]
            if len(values) < total + 3:
                continue
            scans = [value for value in values[1:1 + total] if value]
            result = _text(values[1 + total])
            # 결과 글 = 지금 표 그대로(통과 · 불일치 · 입력오류 · 진행중..., Label_Match.Results)
            kind = "ok" if result.startswith("통과") else "warn" if result.startswith("진행") or "부족" in result else "bad"
            rows.append({"row": str(iid), "no": int(values[0]) if values[0].isdigit() else number,
                         "time": _text(app._operator_session_time_text(values[2 + total])),
                         "tag": _text(values[1]), "item": "", "result": result, "kind": kind, "scans": len(scans), "raw": scans})
    except Exception:
        return []
    return rows[::-1]  # 새것 먼저


def _summary_rows(app):
    tree = app.__dict__.get("summary_tree")
    if tree is None:
        return []
    rows = []
    try:
        for iid in tree.get_children():
            values = [str(value) for value in (tree.item(iid, "values") or ())]
            if len(values) >= 3:
                rows.append({"item": _text(f"{values[0]} {values[1]}"), "count": int(str(values[2]).replace(",", "") or 0)})
    except Exception:
        return []
    return rows


def _progress(app):
    data = app.__dict__.get("_logistics_progress_snapshot")
    connected = app.__dict__.get("_logistics_progress_connected")
    today = datetime.now().strftime("%Y-%m-%d")
    if data is None:
        return {"state": "offline" if connected is False else "none", "date": today, "fetched_at": None, "rows": []}
    rows = []
    for row in data.get("items", [])[:200]:
        rows.append({"item": str(row.get("name") or ""), "plan": int(row.get("plan_qty") or 0), "packed": int(row.get("packaged_qty") or 0),
                     "left": int(row.get("remaining_qty") or 0),
                     "left_boxes": None if row.get("remaining_boxes") is None else int(row.get("remaining_boxes"))})
    stamp = str(app.__dict__.get("_logistics_progress_last_success") or "")
    return {"state": "ok", "date": str(data.get("date") or today), "fetched_at": stamp[11:16] or None, "rows": rows}


def _settings(processor):
    app, roster_module = processor.app, processor.module.label_worker_roster
    choices = app._worker_roster_choices()
    names = [roster_module.worker_choice_label(row) for row in choices]
    current = next((roster_module.worker_choice_label(row) for row in choices if row["worker_id"] == app.__dict__.get("worker_id")),
                   processor.module.display_operator_name(app.__dict__.get("worker_name")))
    register = processor._register
    screen_register = None
    if register and register.get("candidates"):
        screen_register = {"name": register["name"], "text": "같은 이름이 있습니다. 같은 분인가요?",
                           "candidates": [{"id": str(row["worker_id"]), "label": roster_module.worker_choice_label(row)} for row in register["candidates"]]}
    note = "다른 분이면 구분할 이름을 적어 주세요. 예: 이름B" if register and register.get("different_from") else (
        "현재 트레이를 끝내거나 취소한 뒤 바꿀 수 있습니다" if (app.__dict__.get("current_set_info") or {}).get("id") else None)
    return {"workers": [{"name": name} for name in names], "selected": current if current in names else None,
            "required": not app._roster_worker_allowed(), "note": note, "register": screen_register}


def _recovery_row(app, item):
    """멈춘 작업 확인 줄 — 지금 창의 까닭 글(:11734–11742)과 같은 고르기."""
    held = item.get("held") or {}
    reason = "이 건의 상태를 다시 확인하거나 보류로 두세요."
    if held:
        reason = "보류한 건입니다. 다시 확인하거나 보류로 두고 다른 작업을 계속하세요."
    if (item.get("unverified") or item.get("orphan_path")
            or app.__dict__.get("_package_recovery_file_issues", {}).get(item["set_id"])):
        reason = "저장 파일을 확인하지 못했습니다. 이 건만 보류하고 다시 확인하세요."
    if item.get("prior_pin_unknown"):
        reason = "관리자 요청 결과를 아직 모릅니다. 관리자 확인을 다시 조회하거나 지원 인계하세요."
    if item.get("changed_hold"):
        reason = "보류하는 동안 파일 내용이 바뀌었습니다. 원본을 확인한 뒤 복원하거나 보류로 두세요."
    set_id = str(item["set_id"])
    hold = "LABEL.F5_HOLD" if set_id.startswith("F5:") else "LABEL.SET_HOLD"
    package = item.get("package") or {}
    return {"set_id": set_id, "tag": _text(package.get("label") or package.get("phs_label") or "트레이"),
            "item": _text(package.get("item_code") or ""), "status": reason,
            "actions": ["LABEL.RECHECK", hold, "LABEL.SUPPORT_HANDOFF"]}


def present_lm(processor):
    app, module = processor.app, processor.module
    view, source = _view_and_source(app)
    notice = _display_notice(app, view)
    pointer = _pointer_pending(app)
    start = None
    if app.__dict__.get("_startup_catalog_error") is not None:
        start = {"phase": "catalog_failed", "title": "품목 목록 확인 실패", "text": "연결을 확인하고 다시 시도하세요", "retry": True}
    elif view is None or not app.__dict__.get("initialized_successfully", False):
        start = {"phase": "loading", "title": "시작 준비 중", "text": "포장 프로그램을 준비하고 있습니다", "retry": False}
    elif app.__dict__.get("_startup_update_pending", False):
        start = {"phase": "update_check", "title": "새 판 확인 중", "text": "업데이트 시험을 준비하고 있습니다 — 잠시 기다리세요", "retry": False}
    work = _work(processor, view, source, notice) if view is not None else _empty_work()
    worker = app.__dict__.get("worker_name")
    return {
        "contract": STATE_CONTRACT,
        "app": "label_match",
        "version": module.APP_VERSION,
        "worker": {"name": str(worker), "label": _text(module.display_operator_name(worker))} if worker else None,
        "start": start,
        "work": work,
        "approval": None if pointer is None else {
            "id": _approval_id(pointer), "action": _text(app.__dict__.get("_workflow_notice_action_text") or "확인"),
            "title": _text(pointer[0].title), "message": str(pointer[0].message or "").strip(),
            "confirm": _text(app.__dict__.get("_workflow_notice_action_text") or "확인"), "cancel": "그만두기"},
        "conn": _conn(app, module),
        "queue": {"pending": int(app.__dict__.get("_logistics_pending_transmissions") or 0),
                  "failed": int(app.__dict__.get("_logistics_admin_review_count") or 0)},
        "update": {"state": "none", "version": None},
        "alerts": _alerts(processor, view, notice),
        "today": {"date": datetime.now().strftime("%Y-%m-%d"), "is_today": bool(app.__dict__.get("history_view_updates_active_state", True)),
                  "loading": bool(app.__dict__.get("history_active_load_pending", False) or app.__dict__.get("history_load_pending", False)),
                  "rows": _history_rows(app), "summary": _summary_rows(app)},
        "progress": _progress(app),
        "settings": _settings(processor),
        "admin": {"recovery": processor._recovery, "pending": processor._pending, "result": processor._admin_result},
        "about": {"built_at": None, "parts": processor._parts},
    }


def _empty_work():
    total = 5
    return {"stage": "initializing", "stage_label": "초기화 중", "next": "잠시 기다리세요", "next_sub": [], "tag": None, "quantity": None,
            "legacy": {"enabled": False, "text": None}, "top_scan_count": total - 1, "scan_total": total, "scan_done": 0, "scan_left": total,
            "slots": [], "result": {"kind": "idle", "text": "준비 중입니다", "sub": None, "sound": None, "code": None}, "completion": None,
            "badges": [], "readonly": False, "exchange": {"phase": None, "target": None}, "short": {"pending": False, "remaining_s": None},
            "exact_rescan": {"status": "not_started", "completed": 0, "target": 0}, "held_scans": 0,
            "top_scan_setting": {"value": total - 1, "source": "default", "version": 0, "updated_at": None, "fetched_at": None},
            "next_set_top_scan_count": None, "manual_complete_hint": "", "allow": {"undo": False, "exchange": False, "f1": False, "f2": False},
            "scan_enabled": False}


def _work(processor, view, source, notice):
    app, module = processor.app, processor.module
    total = view.qa_total
    done = total if view.completion_kind == "full" else view.qa_completed  # 지금 Tk 수 줄(:22088)과 같게
    next_text, next_sub = _next(app, view, source, notice)
    deadline = app.__dict__.get("_shortage_confirmation_deadline")
    exchange = source.get("scanner_exchange_phase")
    return {
        "stage": str(view.current_stage), "stage_label": _text(view.current_stage_label),
        "next": next_text, "next_sub": next_sub,
        "tag": _tag(app, source), "quantity": _quantity(app, view, source),
        "legacy": _legacy(app, module),
        "top_scan_count": total - 1, "scan_total": total, "scan_done": done, "scan_left": max(total - done, 0),
        "slots": _slots(view, source),
        "result": _result(processor, view, source),
        "completion": view.completion_kind,
        "badges": [badge for badge in view.badges if badge not in HIDDEN_BADGES],
        "readonly": bool(view.readonly),
        "exchange": {"phase": exchange if exchange in {"target", "replacement"} else None,
                     "target": str(source.get("scanner_exchange_target")) if source.get("scanner_exchange_target") else None},
        "short": {"pending": deadline is not None,
                  "remaining_s": max(0, int(deadline - time.monotonic())) if deadline is not None else None},
        "exact_rescan": {"status": view.exact_rescan.status, "completed": view.exact_rescan.completed, "target": view.exact_rescan.target},
        "held_scans": len(app.__dict__.get("_scans_behind_completion") or ()),
        # 상단 확인 수 웹 설정(UI-SHAPE-LM-TOPSCAN-1016)은 sub-151 처리 층이 들어오면 그 값 — 이 바탕(9de2c05)은 4 고정.
        "top_scan_setting": {"value": total - 1, "source": "default", "version": 0, "updated_at": None, "fetched_at": None},
        "next_set_top_scan_count": None,
        "manual_complete_hint": _text(view.f3_hint),
        "allow": {"undo": _enabled(app, "manual_complete_button", view.cancel_current_enabled),
                  "exchange": _enabled(app, "exact_rescan_button", view.f4_enabled),
                  "f1": _enabled(app, "reset_button", view.cancel_current_enabled),
                  "f2": _enabled(app, "cancel_tray_button", view.cancel_completed_enabled)},
        "scan_enabled": _enabled(app, "entry", view.scan_input_enabled),
    }
