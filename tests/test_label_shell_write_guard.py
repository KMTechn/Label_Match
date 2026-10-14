"""Pinned shell writes use the existing Label_Match writer admission."""
from contextlib import contextmanager
import hashlib
import json
from pathlib import Path
import sys
import types

import pytest

import label_match_screen
import writer_session_fence as fence
from writer_sink_inventory import derive_writer_sink_inventory, WriterSinkInventoryError
from tests.test_label_screen_bridge import call, meta, screen
from tests.test_screen_precreation_recovery import shell_runtime

ROOT = Path(__file__).resolve().parents[1]
ACTUAL_WRITE_GUARD = label_match_screen.shell_write_guard
READY_SHA256 = "8c3b35d4740a86481e2d011630bf78271316c38ca5f1e137de8109f320e1eb88"


def test_shell_copy_matches_v3162_ready():
    ready_path = ROOT / "shell/SHELL-READY.json"
    assert ready_path.is_file()
    raw = ready_path.read_bytes()
    assert hashlib.sha256(raw).hexdigest() == READY_SHA256
    ready = json.loads(raw)
    assert ready["version"] == "v3.16.2"
    for entry in ready["files"]:
        path = ROOT / entry["path"]
        assert path.stat().st_size == entry["bytes"]
        assert hashlib.sha256(path.read_bytes()).hexdigest() == entry["sha256"]
    for entry in ready["kit_only_files"]:
        assert not (ROOT / entry["path"]).exists()


def test_screen_runner_installs_the_admission_guard(monkeypatch, tmp_path, shell_runtime):
    captured = {}

    def run_app(profile, data, *, write_guard, **options):
        captured["guard"] = write_guard
        return 0

    monkeypatch.setitem(sys.modules, "host", types.SimpleNamespace(run_app=run_app))
    monkeypatch.setattr(label_match_screen, "shell_data_dir", lambda: tmp_path)
    assert label_match_screen.screen_runner(types.SimpleNamespace(APP_VERSION=__import__('Label_Match').APP_VERSION))() == 0
    assert captured["guard"] is label_match_screen.shell_write_guard


@pytest.mark.parametrize("operation", ["make_dirs", "append_bytes", "create_new", "truncate", "replace_atomic"])
def test_each_shell_write_is_inside_app_admission(monkeypatch, tmp_path, operation):
    import shell_fileio

    path = tmp_path / "target"
    if operation == "truncate":
        path.write_bytes(b"before")
    entered = []
    inside = []

    @contextmanager
    def admission(source):
        entered.append(source)
        inside.append(True)
        try:
            yield
        finally:
            inside.pop()

    original_guard = shell_fileio._guard

    @contextmanager
    def observed_guard(op, target, kind, *, owner=None):
        with original_guard(op, target, kind, owner=owner):
            assert inside == [True]
            yield

    monkeypatch.setattr(label_match_screen, "writer_admission", admission)
    fileio = shell_fileio.set_write_guard(label_match_screen.shell_write_guard)
    monkeypatch.setattr(shell_fileio, "_guard", observed_guard)
    args = {"make_dirs": (), "append_bytes": (b"after",), "create_new": (b"after",),
            "truncate": (2,), "replace_atomic": (b"after",)}[operation]
    getattr(fileio, operation)(path, *args, kind="test_control")
    assert entered == ["shell_fileio"]
    assert inside == []
    assert path.is_dir() if operation == "make_dirs" else path.read_bytes() == (b"be" if operation == "truncate" else b"after")


def test_real_admission_denial_precedes_any_shell_mutation(monkeypatch, tmp_path):
    import shell_fileio

    target = tmp_path / "not-created"
    fileio = shell_fileio.set_write_guard(label_match_screen.shell_write_guard)
    monkeypatch.setattr(fence, "_acquire_named_mutex", lambda *args: None)
    with pytest.raises(fence.WriterFencedError):
        fileio.replace_atomic(target, b"after", "test_control")
    assert not target.exists()
    assert not target.with_name(target.name + ".tmp").exists()


def test_real_admission_allows_a_bounded_shell_write(monkeypatch, tmp_path):
    import shell_fileio

    target = tmp_path / "admitted"
    fileio = shell_fileio.set_write_guard(label_match_screen.shell_write_guard)
    fileio.replace_atomic(target, b"after", "test_control")
    assert target.read_bytes() == b"after"
    assert getattr(fence._WRITER_LOCAL, "depth", 0) == 0


def test_initial_profile_directory_is_not_created_when_admission_is_denied(monkeypatch, tmp_path, shell_runtime):
    import bridge
    monkeypatch.setitem(sys.modules, 'shell_fileio', bridge.shell_fileio)
    monkeypatch.setattr(label_match_screen, 'shell_write_guard', ACTUAL_WRITE_GUARD)
    monkeypatch.setattr(fence, '_acquire_named_mutex', lambda *_a: None)
    with pytest.raises(fence.WriterFencedError):
        label_match_screen._prepare_screen(__import__('Label_Match').APP_VERSION)
    assert not (tmp_path / 'screen').exists()


def test_denied_shell_writes_keep_the_screen_and_next_scan_running(screen, monkeypatch):
    import shell_fileio

    before = {path.name: path.read_bytes() for path in screen.data.glob("*") if path.is_file()}
    monkeypatch.setattr(screen.bridge.fileio, "factory", label_match_screen.shell_write_guard)
    monkeypatch.setattr(fence, "_acquire_named_mutex", lambda *args: None)
    first = call(screen, lambda: screen.bridge.scan("first", meta(1)))
    second = call(screen, lambda: screen.bridge.scan("second", meta(2)))
    repeated = call(screen, lambda: screen.bridge.scan("second", meta(2)))
    state = call(screen, screen.bridge.state)
    assert first["accepted"] and second["accepted"]
    assert repeated["duplicate"] is True
    assert repeated == {**second, "text": "second", "duplicate": True}
    assert state["events_dropped"] > 0
    assert not state["journal_uncertain"]
    assert {path.name: path.read_bytes() for path in screen.data.glob("*") if path.is_file()} == before


def test_inventory_declares_one_shell_fileio_sink():
    rows = derive_writer_sink_inventory(ROOT)
    shell_rows = [row for row in rows if row.source == "shell_fileio"]
    assert len(shell_rows) == 1
    assert shell_rows[0].source_path == "label_match_screen.py"
    assert shell_rows[0].qualified_name == "shell_write_guard"
    assert shell_rows[0].guard_kind == "bounded_context"


@pytest.mark.parametrize("drift", ["shell_bytes", "missing_guard", "missing_admission"])
def test_inventory_rejects_shell_drift(tmp_path, drift):
    import shutil

    shutil.copytree(ROOT / "shell", tmp_path / "shell")
    entry = (ROOT / "label_match_screen.py").read_text(encoding="utf-8")
    if drift == "shell_bytes":
        path = tmp_path / "shell/bridge.py"
        path.write_bytes(path.read_bytes() + b"\n# drift\n")
    elif drift == "missing_guard":
        assert "write_guard=shell_write_guard" in entry
        entry = entry.replace("write_guard=shell_write_guard", "write_guard=None")
    else:
        assert 'writer_admission("shell_fileio")' in entry
        entry = entry.replace('writer_admission("shell_fileio")', "__import__('contextlib').nullcontext()")
    (tmp_path / "label_match_screen.py").write_text(entry, encoding="utf-8")
    with pytest.raises(WriterSinkInventoryError):
        derive_writer_sink_inventory(tmp_path)
