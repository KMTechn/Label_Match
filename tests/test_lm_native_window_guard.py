"""No native windows from defaults, bypasses, or alternate application call sites."""
import ctypes
from pathlib import Path
import re
import subprocess

import pytest

import label_match_instance_win32 as native


def test_default_window_actions_are_autouse_recordings(request):
    assert 'native_window_calls' in request.fixturenames
    recording = request.getfixturevalue('native_window_calls')
    assert native.activate_pid(1234) == native.ActivationResult(False)
    assert native.set_foreground_window(8) is True
    assert native.flash_window(8) is True
    native.timed_notice(closing=None)
    assert recording.calls == [('activate_pid', (1234,)), ('SetForegroundWindow', (8,)), ('FlashWindowEx', (8,)),
        ('MessageBoxTimeoutW', (None, '프로그램이 이미 켜져 있습니다 · 잠시 뒤 다시 누르세요', '포장', 0x10000, 0, 5000))]


@pytest.mark.parametrize('bypass', ['notice', 'foreground', 'flash', 'windll_notice', 'flash_seam'])
def test_real_user32_bypass_fails_before_a_native_window_call(native_window_calls, bypass):
    with pytest.raises(AssertionError, match='NATIVE_WINDOW_CALL_BLOCKED'):
        if bypass == 'notice':
            native_window_calls.original.notice()
        elif bypass == 'foreground':
            native_window_calls.original.foreground(8)
        elif bypass == 'flash':
            ctypes.WinDLL('user32').FlashWindowEx(None)
        elif bypass == 'flash_seam':
            native_window_calls.original.flash(8)
        else:
            ctypes.windll.user32.MessageBoxTimeoutW(None, None, None, 0, 0, 5000)


def test_window_calls_exist_only_in_the_application_native_seam():
    root = Path(__file__).resolve().parents[1]
    paths = subprocess.check_output(['git', '-C', str(root), 'ls-files', '-z', '*.py']).decode().split('\0')
    pattern = re.compile(r'\.\s*(?:SetForegroundWindow|FlashWindowEx|MessageBoxTimeoutW)\s*\('
                         r'|getattr\([^\n]+[\"\x27](?:SetForegroundWindow|FlashWindowEx|MessageBoxTimeoutW)[\"\x27]\s*\)\s*\(')
    offenders = []
    for relative in paths:
        if not relative or relative == 'label_match_instance_win32.py' or relative.startswith(('tests/', 'shell/')):
            continue  # Tests use explicit doubles; shell/ is the byte-exact shared implementation.
        if pattern.search((root / relative).read_text(encoding='utf-8-sig')):
            offenders.append(relative)
    assert offenders == []
