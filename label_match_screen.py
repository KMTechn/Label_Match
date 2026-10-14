"""포장 새 화면 띄우기 — 셸 host(v3.16) run_app 의 owner_main 꼴(HOST-USE §4 나).

`Label_Match.py --new-screen` 이 main() 의 같은 시작 확인(업데이트 · 한 개만 · 설치 확인)을 거친 뒤 여기로 온다.
포장 앱(Tk)은 주인 스레드 하나에서 숨긴 채 만들고 돌며(root.after 로 bridge.pump), 보이는 화면은 셸 창(WebView2)이다.
닫기: 창 X 는 처리 층 closing() 이 지금 끝내기 길(하던 트레이면 마우스 승인)로 묻고 창을 붙잡는다.
앱이 저장을 다 끝내고 스스로 닫히면(mainloop 끝) request_exit 로 창을 닫는다 — 저장 중에 창이 먼저 닫히지 않는다.
"""
from __future__ import annotations

from contextlib import contextmanager
import os
import sys
import threading
from pathlib import Path

from writer_session_fence import WriterFenceError, writer_admission

APP_ROOT = Path(__file__).resolve().parent
SHELL_DIR = APP_ROOT / "shell"
PARTS = APP_ROOT / "ui" / "parts"  # 화면 꾸러미 꼭대기 = http 뿌리(셸 v3.8 root) — ../frame.js · ../../core 가 닿음
PAGE = PARTS / "app" / "lm" / "index.html"
PUMP_MS = 10


@contextmanager
def shell_write_guard(op, path, kind):
    """셸의 한 번 쓰기만 기존 포장 쓰기 허가 안에서 실행한다."""
    with writer_admission("shell_fileio"):
        yield


def shell_data_dir(*, environ=None):
    environment = os.environ if environ is None else environ
    local_app_data = environment.get("LOCALAPPDATA", "").strip()
    if not local_app_data:
        raise RuntimeError("LOCALAPPDATA is required for the Label_Match screen shell")
    return Path(local_app_data) / "KMTech" / "Label_Match" / "screen-shell"


class ScreenUnavailable(RuntimeError):
    """A recoverable preparation failure before any owner or window exists."""
    def __init__(self, code):
        super().__init__(code)
        self.code = code


def _prepare_screen():
    # Resolve dependencies before run_app starts the processing Tk owner.
    try:
        from host import run_app
        import webview
    except Exception as error:
        raise ScreenUnavailable("SHELL_IMPORT_FAILED") from error
    try:
        from webview.guilib import initialize
        backend = initialize("edgechromium")
        if backend.renderer != "edgechromium":
            raise RuntimeError("WebView2 is unavailable")
        from Microsoft.Web.WebView2.Core import CoreWebView2Environment
        if not CoreWebView2Environment.GetAvailableBrowserVersionString(webview.settings["WEBVIEW2_RUNTIME_PATH"]):
            raise RuntimeError("WebView2 is unavailable")
    except Exception as error:
        raise ScreenUnavailable("SHELL_WEBVIEW2_UNAVAILABLE") from error
    try:
        import shell_fileio
        data = shell_data_dir()
        shell_fileio.set_write_guard(shell_write_guard)
        shell_fileio.make_dirs(data / "webview2", "udf")
    except WriterFenceError:
        raise
    except Exception as error:
        raise ScreenUnavailable("SHELL_DATA_DIR_FAILED") from error
    return run_app, webview, data


class _ScreenAttempt:
    def __init__(self):
        self.window_created = False
        self.owner_started = False
        self.fallback = threading.Event()
        self.owner_done = threading.Event()
        self.owner_error = None


def owner_main_for(module, *, app_holder=None, attempt=None):
    """주인 스레드 본문 — 앱 만들기(지금 시작 길 그대로) · 처리 층 붙이기 · pump."""
    from label_screen_bridge import LabelMatchScreenProcessor

    def owner_main(bridge):
        if attempt is not None:
            attempt.owner_started = True
        host = module.LABEL_MATCH_SCREEN_HOST
        host["active"] = True
        attached = []
        host["run"] = lambda app: run_on_owner(app, bridge, attached)
        try:
            report = host.get("relay_reuse_report")
            return (module._run_label_match_application(relay_reuse_report=report)
                    if report is not None else module._run_label_match_application())
        except BaseException as error:
            if attached:
                if attempt is not None:
                    attempt.owner_error = error
                raise
            bridge.fail_owner(error)  # 처리 층을 못 만듦 — 창 0, run_app 이 이 오류를 다시 던짐(셸 v3.11)
        finally:
            if not attached:
                try:
                    bridge.fail_owner(RuntimeError("Label_Match screen owner ended before attach"))
                except RuntimeError:
                    pass  # 이미 끝남(위 fail_owner)
            if attempt is not None:
                attempt.owner_done.set()

    def run_on_owner(app, bridge, attached):
        if app_holder is not None:
            app_holder.append(app)
        # The processing Tk root is hidden; the shell's ready receipt proves visibility.
        app._startup_screen_visible = lambda: bridge._view_count > 0
        processor = LabelMatchScreenProcessor(app, bridge)
        bridge.attach_owner(processor)
        attached.append(True)

        def pump():
            if app.__dict__.get("_tk_destroy_complete") or app.__dict__.get("_tk_shutdown_requested"):
                return
            if attempt is not None and attempt.fallback.is_set():
                # Stay on this Tk's owner thread and reuse the processing layer.
                processor.uninstall()
                bridge.close()
                module.LABEL_MATCH_SCREEN_HOST.update(requested=False, active=False, run=None)
                app._startup_screen_visible = lambda: bool(app.winfo_viewable())
                app.deiconify()
                return
            if bridge.pump():
                app.after(PUMP_MS, pump)
            else:  # 창이 먼저 닫힘(셸 closing 무답 등) — 지금 끝내기 길로 저장을 마치고 닫음
                app.on_closing(_confirmed=True)

        app.after(PUMP_MS, pump)
        try:
            app.mainloop()
        except BaseException as error:
            if attempt is not None:
                attempt.owner_error = error
            raise
        finally:
            try:
                app._finish_after_mainloop()
            finally:
                bridge.request_exit()

    return owner_main


def screen_runner(module):
    def run():
        if str(SHELL_DIR) not in sys.path:
            sys.path.insert(0, str(SHELL_DIR))
        run_app, webview, data = _prepare_screen()
        attempt = _ScreenAttempt()
        app_holder = []

        def on_force_exit():
            if app_holder:
                app_holder[0].save_current_set_on_force_exit()

        create_window = webview.create_window
        previous_windows = tuple(webview.windows)

        def create_once(*args, **kwargs):
            window = create_window(*args, **kwargs)
            attempt.window_created = True
            return window

        webview.create_window = create_once
        try:
            return run_app("rw", data, root=PARTS, page=PAGE, title="포장", owner_main=owner_main_for(module, app_holder=app_holder, attempt=attempt),
                           on_force_exit=on_force_exit, pedal=False, browser_preset_name="none", write_guard=shell_write_guard)
        except WriterFenceError:
            raise
        except Exception as error:
            if attempt.window_created or tuple(webview.windows) != previous_windows:
                raise
            if app_holder and not attempt.owner_done.is_set():
                module._label_match_startup_trace("SHELL_WINDOW_FAILED")
                attempt.fallback.set()
                attempt.owner_done.wait()
                if attempt.owner_error is not None:
                    raise attempt.owner_error
                return 0
            if attempt.owner_started:
                raise  # Existing application/startup refusal remains fail closed.
            raise ScreenUnavailable("SHELL_WINDOW_FAILED") from error
        finally:
            webview.create_window = create_window

    return run
