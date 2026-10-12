"""포장 새 화면 띄우기 — 셸 host(v3.8) run_app 의 owner_main 꼴(HOST-USE §4 나).

`Label_Match.py --new-screen` 이 main() 의 같은 시작 확인(업데이트 · 한 개만 · 설치 확인)을 거친 뒤 여기로 온다.
포장 앱(Tk)은 주인 스레드 하나에서 숨긴 채 만들고 돌며(root.after 로 bridge.pump), 보이는 화면은 셸 창(WebView2)이다.
닫기: 창 X 는 처리 층 closing() 이 지금 끝내기 길(하던 트레이면 마우스 승인)로 묻고 창을 붙잡는다.
앱이 저장을 다 끝내고 스스로 닫히면(mainloop 끝) request_exit 로 창을 닫는다 — 저장 중에 창이 먼저 닫히지 않는다.
"""
from __future__ import annotations

import os
import sys
from pathlib import Path

APP_ROOT = Path(__file__).resolve().parent
SHELL_DIR = APP_ROOT / "shell"
PARTS = APP_ROOT / "ui" / "parts"  # 화면 꾸러미 꼭대기 = http 뿌리(셸 v3.8 root) — ../frame.js · ../../core 가 닿음
PAGE = PARTS / "app" / "lm" / "index.html"
PUMP_MS = 10


def shell_data_dir():
    local_app_data = os.environ.get("LOCALAPPDATA", "").strip()
    if not local_app_data:
        raise RuntimeError("LOCALAPPDATA is required for the Label_Match screen shell")
    return Path(local_app_data) / "KMTech" / "Label_Match" / "screen-shell"


def owner_main_for(module):
    """주인 스레드 본문 — 앱 만들기(지금 시작 길 그대로) · 처리 층 붙이기 · pump."""
    from label_screen_bridge import LabelMatchScreenProcessor

    def owner_main(bridge):
        module.LABEL_MATCH_SCREEN_HOST["active"] = True
        app = module._create_label_match_application()
        bridge.attach_owner(LabelMatchScreenProcessor(app, bridge))

        def pump():
            if app.__dict__.get("_tk_destroy_complete") or app.__dict__.get("_tk_shutdown_requested"):
                return
            if bridge.pump():
                app.after(PUMP_MS, pump)
            else:  # 창이 먼저 닫힘(셸 closing 무답 등) — 지금 끝내기 길로 저장을 마치고 닫음
                app.on_closing(_confirmed=True)

        app.after(PUMP_MS, pump)
        try:
            app.mainloop()
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
        from host import run_app

        return run_app("rw", shell_data_dir(), root=PARTS, page=PAGE, title="포장", owner_main=owner_main_for(module),
                       pedal=False, browser_preset_name="none")

    return run
