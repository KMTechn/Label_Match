import json
import os
from pathlib import Path
import subprocess
import sys
from tools.build_portable_release_candidate import _copy_application

import pytest

import auto_update as updates
import writer_session_fence as fence

ROOT = Path(__file__).resolve().parents[1]
BASE = "e78023199d99e1878a5927139595dcf25088fdfa"
NOWIN = getattr(subprocess, "CREATE_NO_WINDOW", 0)
SCRIPT_PYTHON = os.environ.get("KMTECH_LM_SCRIPT_TEST_RUNTIME", sys.executable)


@pytest.fixture(scope="module")
def historical_repo(tmp_path_factory):
    repository = tmp_path_factory.mktemp("historical") / "repo.git"
    subprocess.run(["git", "init", "--bare", str(repository)], check=True, capture_output=True, creationflags=NOWIN)
    subprocess.run(["git", "-C", str(repository), "fetch", "--no-tags", "--depth=1", str(ROOT), BASE],
                   check=True, capture_output=True, creationflags=NOWIN, timeout=45)
    assert not (repository / "objects/info/alternates").exists()
    return repository


def _packet(tmp_path, old, historical_repo=None):
    managed = tmp_path / ("red" if old else "green")
    app = managed / "current/app"
    _copy_application(ROOT, app)
    files = ("auto_update.py", "writer_session_fence.py", "failure_redaction.py", "tools/failure_redaction_rules.json")
    for name in files:
        raw = (subprocess.check_output(["git", "-C", str(historical_repo), "show", f"{BASE}:{name}"], creationflags=NOWIN)
               if old else (ROOT / name).read_bytes())
        target = app / name
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(raw)
    helper = Path(__file__).with_name("_script_run_admission_probe.py")
    with (app / "writer_session_fence.py").open("a", encoding="utf-8") as output:
        output.write("\n# Private test observer; product body above remains byte-identical.\n")
        output.write(f"import importlib.util as _probe_util\n_probe_spec = _probe_util.spec_from_file_location('_script_probe', {str(helper)!r})\n")
        output.write("_probe_module = _probe_util.module_from_spec(_probe_spec)\n_probe_spec.loader.exec_module(_probe_module)\n_probe_module.install(Path(__file__).resolve().parent)\n")
    return managed, app


@pytest.mark.parametrize("mode", ["system", "relay", "request_bootstrap"])
def test_script_entry_red_to_green_until_first_admission(monkeypatch, tmp_path, historical_repo, mode):
    from contextlib import nullcontext
    for old in (True, False):
        managed, app = _packet(tmp_path, old, historical_repo)
        if mode == "system":
            args = ["--apply-prepared", "--control-root", str(managed / "update-control")]
        elif mode == "relay":
            args = ["--bootstrap", "--role", "relay", "--", "--label-match-user-relay"]
        else:
            commands = []
            monkeypatch.setattr(fence, "writer_admission", lambda *a, **kw: nullcontext())
            monkeypatch.setattr(updates, "start_ordinary_process", lambda cmd, **kw: commands.append((cmd, kw)))
            updates.request_bootstrap(managed / "update-control", ["--test-gui-argument"], role="gui")
            emitted, options = commands[0]
            assert emitted[:4] == [str(managed / "current/runtime/pythonw.exe"), "-I", "-B", str(app / "auto_update.py")]
            assert options == {"cwd": str(managed / "current")}
            args = emitted[4:]
        command = [SCRIPT_PYTHON, "-I", "-B", str(app / "auto_update.py"), *args]
        child = subprocess.run(command, capture_output=True, creationflags=NOWIN, timeout=45)
        evidence = json.loads((managed / "probe-evidence.json").read_text())
        assert child.returncode == 1  # RED refusal, or GREEN deliberate BaseException stop.
        assert evidence["socket_requests"] == evidence["child_requests"] == 0
        if old:
            assert evidence["definition_rejections"] == ["WRITER_PROBE_ONLY_SINK_NOT_ALLOWED"]
            assert not evidence["main_entered"]
            assert evidence["first_admission"] is None
        else:
            assert evidence["definition_rejections"] == []
            assert evidence["main_entered"]
            assert evidence["first_admission"] == {"source": "auto_update_fixed_helper" if mode == "system" else "auto_update_launch_version",
                                                   "depth": 0, "acquired": False}
            for name in ("write_download_bytes", "write_download_chunk", "stage_prepared_copies", "_run_download_attempt"):
                assert evidence["script_sinks"][name] == f"auto_update.{name}"


def test_current_junction_script_uses_app_identity_and_rejects_inherited_depth(tmp_path):
    managed, app = _packet(tmp_path, False)
    original = managed / "current"
    target = managed / "version-bundle"
    assert original.resolve().is_relative_to(tmp_path.resolve())
    assert target.resolve().is_relative_to(tmp_path.resolve()) and not target.exists()
    original.rename(target)
    # Native PowerShell creates only this task-owned junction; no cmd cleanup.
    link_literal = str(original).replace("'", "''")
    target_literal = str(target).replace("'", "''")
    script = f"$ErrorActionPreference='Stop'; New-Item -ItemType Junction -Path '{link_literal}' -Target '{target_literal}' | Out-Null"
    made = subprocess.run(["powershell.exe", "-NoProfile", "-NonInteractive", "-Command", script],
                          capture_output=True, creationflags=NOWIN)
    assert made.returncode == 0
    try:
        assert app.resolve() == target / "app"
        installed = managed / "not-installed"
        child = subprocess.run([SCRIPT_PYTHON, "-I", "-B", str(app / "auto_update.py"), "--print-logon-task",
                                "--install-root", str(installed), "--operator-sid", "S-1-5-21-101-202-303-1001"],
                               capture_output=True, creationflags=NOWIN, timeout=45)
        assert child.returncode == 0
        task = json.loads(child.stdout)
        assert task["name"].startswith("KMTech.LabelMatch.StartupUpdate.")
        evidence = json.loads((managed / "probe-evidence.json").read_text())
        assert evidence["staging_rejections"] == ["STAGING_WRITER_ADMISSION_HELD"] * 4
        assert evidence["first_admission"] is None
        assert not installed.exists()
    finally:
        assert original.resolve() == target.resolve()
        original.rmdir()  # Unlink only; preserve the target and all original bytes.


def test_main_allowlisted_relay_without_probe_flag_releases_before_body(monkeypatch):
    from contextlib import contextmanager
    events = []
    @contextmanager
    def admission(source):
        events.append(("enter", source))
        try:
            yield
        finally:
            events.append(("exit", source))
    monkeypatch.setattr(fence, "writer_admission", admission)
    monkeypatch.setattr(fence._WRITER_LOCAL, "depth", 0, raising=False)
    namespace = {"__name__": "__main__", "__file__": str(ROOT / "user_relay.py"),
                 "writer_sink": fence.writer_sink, "events": events}
    exec(compile('@writer_sink("relay_child_launch")\ndef run_session_direct_sync_once():\n    events.append(("body", "relay_child_launch"))\n',
                 str(ROOT / "user_relay.py"), "exec"), namespace)
    namespace["run_session_direct_sync_once"]()
    assert events == [("enter", "relay_child_launch"), ("exit", "relay_child_launch"), ("body", "relay_child_launch")]


def test_packet_installer_task_child_red_to_green(tmp_path, historical_repo):
    import re
    import xml.etree.ElementTree as ET
    for old in (True, False):
        managed, app = _packet(tmp_path, old, historical_repo)
        installed = managed / "not-installed"
        child = subprocess.run([SCRIPT_PYTHON, "-I", "-B", str(app / "auto_update.py"), "--print-logon-task",
                                "--install-root", str(installed), "--operator-sid", "S-1-5-21-101-202-303-1001"],
                               capture_output=True, creationflags=NOWIN, timeout=45)
        evidence = json.loads((managed / "probe-evidence.json").read_text())
        assert not installed.exists()
        if old:
            assert child.returncode == 1
            assert evidence["definition_rejections"] == ["WRITER_PROBE_ONLY_SINK_NOT_ALLOWED"]
        else:
            assert child.returncode == 0
            spec = json.loads(child.stdout)
            assert re.fullmatch(r"KMTech\.LabelMatch\.StartupUpdate\.[0-9a-f]{24}", spec["name"])
            ET.fromstring(spec["xml"])
            assert evidence["definition_rejections"] == []
            assert evidence["first_admission"] is None
