"""G100b packaging: first setup relay waits share one value; trees are read once per process."""
import ast
import re
import time
from pathlib import Path
from types import SimpleNamespace

import pytest

import auto_update
import current_user_onboarding as onboarding

ROOT = Path(__file__).resolve().parents[1]
INSTALLER = ROOT / "INSTALL_CANONICAL_PORTABLE.ps1"


def test_bootstrap_waits_for_relay_ready_at_76_seconds(monkeypatch, tmp_path):
    """A relay that becomes ready 76 s after launch must not be missed."""
    clock = {"now": 1000.0}
    monkeypatch.setattr(auto_update.time, "monotonic", lambda: clock["now"])
    monkeypatch.setattr(auto_update.time, "sleep", lambda s: clock.__setitem__("now", clock["now"] + s))
    seen = {}
    monkeypatch.setattr(auto_update, "support_record", lambda *a, **k: {})
    monkeypatch.setattr(auto_update, "write_json_durable", lambda *a, **k: None)
    monkeypatch.setattr(auto_update, "assert_ordinary_recovery", lambda *a, **k: None)
    monkeypatch.setattr(auto_update, "emit_support_record", lambda *a, **k: None)
    monkeypatch.setattr(auto_update, "_launch_bootstrap_version",
                        lambda control, sha, arguments, *, deadline, **k: seen.setdefault("deadline", deadline) and 4321)
    ready_at = 1000.0 + 76

    def wait(control, policy, sha, *, started_at, deadline, pid=None):
        while clock["now"] < deadline:
            if clock["now"] >= ready_at:
                return True
            clock["now"] += 1
        return False
    monkeypatch.setattr(auto_update, "wait_current_relay", wait)
    result = auto_update.start_current(tmp_path, {"staging_root": str(tmp_path)}, None, {"sha": "a" * 40},
                                       ["--label-match-user-relay"], code="X")
    assert result == 0


def test_every_relay_wait_uses_the_one_value():
    text = (ROOT / "auto_update.py").read_text(encoding="utf-8")
    assert auto_update.RELAY_PREPARE_SECONDS == 600
    assert not re.search(r"monotonic\(\)\s*\+\s*75\b", text)

    bootstrap = next(node for node in ast.parse(text).body
                     if isinstance(node, ast.FunctionDef) and node.name == "_bootstrap")
    assignments = [node for node in ast.walk(bootstrap) if isinstance(node, ast.Assign)
                   and any(isinstance(target, ast.Name) and target.id == "relay_deadline"
                           for target in node.targets)]
    assert len(assignments) == 1
    assert assignments[0] in list(ast.walk(bootstrap.body[0]))
    calls = [node for node in ast.walk(bootstrap)
             if isinstance(node, ast.Call) and isinstance(node.func, ast.Name)]
    current_waits = [call for call in calls
                     if call.func.id in {"start_current", "recovery_writer_result", "wait_current_relay"}]
    assert len(current_waits) == 22  # 14 starts, seven recovery paths, one rollback readback.
    for call in current_waits:
        deadline = next(keyword.value for keyword in call.keywords if keyword.arg == "deadline")
        assert isinstance(deadline, ast.Name) and deadline.id == "relay_deadline"
    trial_starts = [call for call in calls if call.func.id == "_launch_bootstrap_version"]
    assert len(trial_starts) == 2
    for call in trial_starts:
        deadline = next(keyword.value for keyword in call.keywords if keyword.arg == "deadline")
        assert ast.unparse(deadline) == "relay_deadline.bounded_by(deadline)"
    deadline_class = next(node for node in ast.parse(text).body
                          if isinstance(node, ast.ClassDef) and node.name == "_RelayPreparationDeadline")
    bound = next(node for node in deadline_class.body
                 if isinstance(node, ast.FunctionDef) and node.name == "bounded_by")
    assert "min(self, deadline)" in ast.unparse(bound)
    assert "parent_pid=self.parent_pid" in ast.unparse(bound)
    assert "parent_identity=self.parent_identity" in ast.unparse(bound)
    launch = next(node for node in ast.parse(text).body
                  if isinstance(node, ast.FunctionDef) and node.name == "launch_version")
    spawn_index = next(index for index, statement in enumerate(launch.body)
                       if isinstance(statement, ast.Assign) and isinstance(statement.value, ast.Call)
                       and isinstance(statement.value.func, ast.Name)
                       and statement.value.func.id == "start_ordinary_process")
    final_guard = launch.body[spawn_index - 1]
    assert isinstance(final_guard, ast.If)
    assert ast.unparse(final_guard.test) == "relay_deadline is not None"
    assert "_relay_prepare_failure(relay_deadline)" in ast.unparse(final_guard)
    assert "raise _RelayPreparationUnavailable(failure)" in ast.unparse(final_guard)
    admission = next(node for node in ast.parse(text).body
                     if isinstance(node, ast.FunctionDef) and node.name == "_launch_bootstrap_version")
    version_start = next(node for node in ast.walk(admission)
                         if isinstance(node, ast.Call) and isinstance(node.func, ast.Name)
                         and node.func.id == "launch_version")
    shared = next(keyword.value for keyword in version_start.keywords if keyword.arg == "relay_deadline")
    assert ast.unparse(shared) == "deadline"


def test_installer_and_user_relay_use_the_same_value():
    import user_relay
    value = auto_update.RELAY_PREPARE_SECONDS
    ps = INSTALLER.read_text(encoding="utf-8")
    assert f"AddSeconds({value})" in ps and f"{value}-([DateTime]::UtcNow-$relayStartedAt)" in ps
    import inspect
    src = Path(user_relay.__file__).read_text(encoding="utf-8")
    for function in (user_relay.start_user_relay_process, user_relay.reuse_or_start_user_relay_process):
        assert inspect.signature(function).parameters["bootstrap_readiness_seconds"].default == value
    parent = next(node for node in ast.parse(src).body
                  if isinstance(node, ast.FunctionDef) and node.name == "start_user_relay_process")
    deadlines = [node for node in ast.walk(parent) if isinstance(node, ast.Assign)
                 and any(isinstance(target, ast.Name) and target.id == "parent_deadline" for target in node.targets)]
    assert len(deadlines) == 1
    assert isinstance(deadlines[0].value, ast.BinOp) and isinstance(deadlines[0].value.op, ast.Add)
    assert ast.unparse(deadlines[0].value.left) == "time.monotonic()"
    assert "min(float(RELAY_PREPARE_SECONDS), float(bootstrap_readiness_seconds))" in ast.unparse(deadlines[0])
    spawn = next(node for node in ast.walk(parent) if isinstance(node, ast.Call)
                 and isinstance(node.func, ast.Name) and node.func.id == "start_ordinary_process")
    assert deadlines[0].lineno < spawn.lineno
    loop = next(node for node in ast.walk(parent) if isinstance(node, ast.While))
    assert ast.unparse(loop.test) == "time.monotonic() < parent_deadline"
    child_source = (ROOT / "auto_update.py").read_text(encoding="utf-8")
    child = next(node for node in ast.parse(child_source).body
                 if isinstance(node, ast.FunctionDef) and node.name == "_relay_preparation_deadline")
    assert auto_update.RELAY_PARENT_RESULT_RESERVE_SECONDS == 10
    assert "min(independent, received - RELAY_PARENT_RESULT_RESERVE_SECONDS)" in ast.unparse(child)
    assert "independent = entry + RELAY_PREPARE_SECONDS" in ast.unparse(child)


def _tree(root):
    (root / "app").mkdir(parents=True)
    (root / "app" / "a.py").write_bytes(b"x=1\n")
    (root / "app" / "b.py").write_bytes(b"y=2\n")
    rec = root / "bootstrap-integrity.json"
    rec.write_text("{}")
    return SimpleNamespace(app_root=root / "app", bootstrap_integrity_path=rec)


def test_second_integrity_check_does_not_reread_the_tree(monkeypatch, tmp_path):
    paths = _tree(tmp_path)
    calls = []
    monkeypatch.setattr(onboarding, "_verify_bootstrap_integrity_uncached",
                        lambda p, *, required: calls.append(1) or {"status": "PASS"})
    reads = []
    original = Path.read_bytes
    monkeypatch.setattr(Path, "read_bytes", lambda self: reads.append(self) or original(self))
    onboarding._BOOTSTRAP_INTEGRITY_PASS.clear()
    assert onboarding.verify_bootstrap_integrity(paths, required=True)["status"] == "PASS"
    first = len(reads)
    assert first >= 2
    for _ in range(3):
        assert onboarding.verify_bootstrap_integrity(paths, required=True)["status"] == "PASS"
    assert len(reads) == first and len(calls) == 1


def test_changed_file_after_pass_is_still_rejected_or_reverified(monkeypatch, tmp_path):
    paths = _tree(tmp_path)
    results = [{"status": "PASS"}, {"status": "FAIL"}]
    monkeypatch.setattr(onboarding, "_verify_bootstrap_integrity_uncached",
                        lambda p, *, required: results.pop(0))
    onboarding._BOOTSTRAP_INTEGRITY_PASS.clear()
    assert onboarding.verify_bootstrap_integrity(paths, required=True)["status"] == "PASS"
    (paths.app_root / "a.py").write_bytes(b"x=99\n")
    assert onboarding.verify_bootstrap_integrity(paths, required=True)["status"] == "FAIL"


def test_same_size_file_with_a_new_mtime_is_reverified_and_rejected(monkeypatch, tmp_path):
    import os
    paths = _tree(tmp_path)
    results = [{"status": "PASS"}, {"status": "FAIL"}]
    monkeypatch.setattr(onboarding, "_verify_bootstrap_integrity_uncached",
                        lambda p, *, required: results.pop(0))
    onboarding._BOOTSTRAP_INTEGRITY_PASS.clear()
    target = paths.app_root / "a.py"
    assert onboarding.verify_bootstrap_integrity(paths, required=True)["status"] == "PASS"
    stat = target.lstat()
    target.write_bytes(b"x=2\n")  # same size as b"x=1\n"
    os.utime(target, ns=(stat.st_atime_ns, stat.st_mtime_ns + 1_000_000_000))
    assert target.lstat().st_size == stat.st_size
    assert onboarding.verify_bootstrap_integrity(paths, required=True)["status"] == "FAIL"
    assert results == []


def test_second_writer_inventory_does_not_reread_the_tree(monkeypatch, tmp_path):
    import writer_sink_inventory
    bundle = tmp_path / "b"
    (bundle / "app").mkdir(parents=True)
    (bundle / "app" / "m.py").write_bytes(b"z=1\n")
    (bundle / "INSTALL_THIS_PC.ps1").write_bytes(b"#\n")
    monkeypatch.setattr(writer_sink_inventory, "_runtime_files",
                        lambda root, suffix: sorted(root.glob("*" + suffix)))
    monkeypatch.setattr(auto_update, "_version_writer_inventory_sha256", lambda b: "h" * 64)
    auto_update._VERSION_WRITER_INVENTORY_PASS.clear()
    reads = []
    original = Path.read_bytes
    monkeypatch.setattr(Path, "read_bytes", lambda self: reads.append(self) or original(self))
    assert auto_update.version_writer_inventory_sha256(bundle) == "h" * 64
    first = len(reads)
    assert auto_update.version_writer_inventory_sha256(bundle) == "h" * 64
    assert len(reads) == first
    (bundle / "app" / "m.py").write_bytes(b"z=2\n")
    auto_update.version_writer_inventory_sha256(bundle)
    assert len(reads) > first


def test_known_limit_second_check_does_not_read_bytes_of_a_same_size_same_mtime_change(monkeypatch, tmp_path):
    """Documented limit (operations.md 「한 번 읽기」 한계, BACKLOG G100b), not a fix.

    The second check trusts size + mtime_ns of the tree this process already
    verified. A file put back with the same size and the same mtime_ns is not
    seen: the second check reads no bytes and answers the cached PASS. The first
    check (changes while it runs) and any size/mtime change are still caught.
    """
    import os
    paths = _tree(tmp_path)
    monkeypatch.setattr(onboarding, "_verify_bootstrap_integrity_uncached",
                        lambda p, *, required: {"status": "PASS"})
    onboarding._BOOTSTRAP_INTEGRITY_PASS.clear()
    target = paths.app_root / "a.py"
    assert onboarding.verify_bootstrap_integrity(paths, required=True)["status"] == "PASS"
    stat = target.lstat()
    target.write_bytes(b"x=2\n")  # same size as b"x=1\n"
    os.utime(target, ns=(stat.st_atime_ns, stat.st_mtime_ns))
    assert target.lstat().st_size == stat.st_size and target.lstat().st_mtime_ns == stat.st_mtime_ns
    reads = []
    original = Path.read_bytes
    monkeypatch.setattr(Path, "read_bytes", lambda self: reads.append(self) or original(self))
    assert onboarding.verify_bootstrap_integrity(paths, required=True)["status"] == "PASS"
    assert reads == []  # the second check did not read the changed bytes


def test_registration_child_cap_equals_the_relay_wait_and_other_ps_caps_are_other_meanings():
    """Table check (operations.md 「고정 상한 판정표」): only values with the relay-prepare
    meaning are tied to RELAY_PREPARE_SECONDS; the rest are pinned as different caps."""
    ps = INSTALLER.read_text(encoding="utf-8")
    ms = auto_update.RELAY_PREPARE_SECONDS * 1000
    # Outer cap of the first-setup registration child, which itself waits up to
    # RELAY_PREPARE_SECONDS for the relay: it must not be smaller (else it kills it).
    assert f"function Wait-LabelRegistrationChild([Diagnostics.Process]$Process, [int]$TimeoutMilliseconds={ms})" in ps
    assert "Wait-LabelRegistrationChild $process" in ps
    # Different meanings, left alone on purpose.
    assert "function Wait-LabelPlacementChild([Diagnostics.Process]$Process, [Diagnostics.Process]$Parent, [int]$TimeoutMilliseconds=720000)" in ps
    assert ps.count("-TimeoutMilliseconds 90000") >= 6          # writer-admission / fence waits
    assert "$process.WaitForExit(120000)" in ps                  # dead-installer proof child
    assert "Start-Sleep -Seconds 3" in ps                        # rollback relay restart settle
    host = (ROOT / "shell" / "host.py").read_text(encoding="utf-8")
    assert "CLOSING_WAIT_SECONDS = 120" in host                  # window closing dialog
