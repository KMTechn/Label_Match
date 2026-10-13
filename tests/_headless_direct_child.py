"""Fail closed on host UI and native state before direct-entry product imports."""
from __future__ import annotations

import ctypes
import importlib.util
import os
from pathlib import Path
import socket
import subprocess
import sys


def _blocked(*_args, **_kwargs):
    raise AssertionError("native host UI/state is outside this isolated test")


def install(*, fake_startup_notice=True):
    """Explicit loading also protects interpreters started with -I or -E."""
    boundary_path = os.environ.get("KMTECH_TEST_WRITE_BOUNDARY_SHIM")
    if boundary_path and not getattr(sys, "_kmtech_ca_write_boundary", False):
        spec = importlib.util.spec_from_file_location("_direct_write_boundary", boundary_path)
        boundary = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(boundary)

    import _tkinter
    import tkinter
    from tkinter import messagebox

    _tkinter.create = _blocked
    tkinter.Tk.__init__ = _blocked
    tkinter.Toplevel.__init__ = _blocked
    tkinter.NoDefaultRoot()
    messagebox._show = lambda *_args, **_kwargs: "ok"
    for name in ("showinfo", "showwarning", "showerror"):
        setattr(messagebox, name, lambda *_args, **_kwargs: "ok")
    for name in ("askokcancel", "askyesno", "askretrycancel"):
        setattr(messagebox, name, lambda *_args, **_kwargs: False)
    messagebox.askquestion = lambda *_args, **_kwargs: "no"
    messagebox.askyesnocancel = lambda *_args, **_kwargs: None
    for name in ("connect", "connect_ex", "bind", "listen"):
        setattr(socket.socket, name, _blocked)

    if os.name == "nt":
        exports = {
            "shell32": ("SHGetKnownFolderPath", "SHGetFolderPathW", "ShellExecuteW",
                        "ShellExecuteA", "ShellExecuteExW", "ShellExecuteExA"),
            "advapi32": ("CredReadW", "CredEnumerateW"),
            "crypt32": ("CryptUnprotectData",),
            "user32": ("MessageBoxW", "MessageBoxA", "CreateWindowExW", "CreateWindowExA",
                       "DialogBoxParamW", "DialogBoxParamA"),
        }

        def guard_library(library, name):
            selected = str(name).replace("\\", "/").rsplit("/", 1)[-1].casefold().removesuffix(".dll")
            for export in exports.get(selected, ()):
                setattr(library, export, _blocked)
            return library

        for name in exports:
            guard_library(getattr(ctypes.windll, name), name)
        original_library = ctypes.WinDLL

        def isolated_library(name, *args, **kwargs):
            return guard_library(original_library(name, *args, **kwargs), name)

        ctypes.WinDLL = isolated_library
        original_popen = subprocess.Popen

        class HiddenPopen(original_popen):
            _headless_creationflags = subprocess.CREATE_NO_WINDOW

            def __init__(self, *args, **kwargs):
                kwargs["creationflags"] = kwargs.get("creationflags", 0) | subprocess.CREATE_NO_WINDOW
                super().__init__(*args, **kwargs)

        subprocess.Popen = HiddenPopen

    if fake_startup_notice:
        import startup_observation
        startup_observation.show_startup_blocked_notice = lambda: None

    return {
        "tk_blocked": _tkinter.create is _blocked,
        "notice_faked": fake_startup_notice,
        "known_folder_blocked": os.name != "nt" or ctypes.windll.shell32.SHGetKnownFolderPath is _blocked,
        "hidden_popen": os.name != "nt" or bool(subprocess.Popen._headless_creationflags & subprocess.CREATE_NO_WINDOW),
        "write_boundary": bool(getattr(sys, "_kmtech_ca_write_boundary", False)),
    }


if __name__ == "__main__":
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
    proof = install()
    import _tkinter
    from tkinter import messagebox
    try:
        _tkinter.create()
    except AssertionError:
        pass
    else:
        raise AssertionError("Tk factory was not blocked")
    if os.name == "nt":
        try:
            ctypes.windll.shell32.SHGetKnownFolderPath()
        except AssertionError:
            pass
        else:
            raise AssertionError("known-folder API was not blocked")
    assert messagebox.showerror() == "ok"
    assert all(proof.values())
    print("child shim verified: Tk/native folders blocked; startup notice faked; Popen CREATE_NO_WINDOW; write boundary active")
