"""Concurrent recovery requests use the real named update mutex and launcher."""
import copy
import hashlib
import inspect
import json
import os
import re
import subprocess
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

import pytest

import auto_update as updates
from test_recovery_gui_bootstrap import GUI_CASES


_MUTEX_BODY = inspect.unwrap(updates.update_lock)
_MUTEX_SOURCE, _MUTEX_FIRST_LINE = inspect.getsourcelines(_MUTEX_BODY)
_MUTEX_WAIT_LINE = _MUTEX_FIRST_LINE + next(
    index for index, line in enumerate(_MUTEX_SOURCE)
    if "result = kernel.WaitForSingleObject(handle," in line
)


def _write_record(path, record):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(record), encoding="utf-8")


def _packet(tmp_path):
    """Create real policy, journal and inventory inputs without runnable code."""
    control = tmp_path / "managed/update-control"
    stage = tmp_path / "stage"
    stage.mkdir()
    sha = "a" * 40
    bundle = updates.version_bundle(control, sha)
    app = bundle / "app"
    for name in (*updates.CONTRACT_FILES,
                 *("app/" + name for name in updates.DATA_API_FILES),
                 "app/main.py", "app/label_match_relay_entry.py",
                 "INSTALL_THIS_PC.ps1", "runtime/pythonw.exe"):
        path = bundle / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(b"")
    (app / "Label_Match.py").write_text('APP_VERSION = "v2.0.110"\n', encoding="utf-8")
    (app / "fixture_sink.py").write_text(
        '@writer_sink("recovery_fixture")\ndef fixture_sink():\n    pass\n',
        encoding="utf-8",
    )
    _write_record(bundle / "portable-manifest.json", {
        "schema": "label-match-portable-tree-v1", "startup_update_protocol": 1,
        "version": "v2.0.110", "source_commit": sha,
        "writer_sink_inventory_contract_sha256": updates.version_writer_inventory_sha256(bundle),
    })
    settings = updates.policy_from_settings({})
    raw_settings = updates.canonical_startup_settings(settings)
    settings_path = control.parent / "current/app/auto-update-settings.json"
    settings_path.parent.mkdir(parents=True)
    settings_path.write_bytes(raw_settings)
    sid = updates.current_process_sid()
    policy = {**settings, "operator_sid": sid, "staging_root": str(stage),
              "operator_local_app_data": str(tmp_path / "operator-local"),
              "settings_sha256": hashlib.sha256(raw_settings).hexdigest()}
    _write_record(control / "policy.json", policy)
    _write_record(control / "active.json", {"sha": sha})
    # This is a retained claim fixture, not permission to create a new trial.
    _write_record(stage / "trial.json", {"pid": 700001, "role": "relay"})
    assert updates.ordinary_user_context(sid), "ordinary host test identity required"
    assert updates.load_policy(control) == policy
    assert updates.current_generation_status(control, policy, sha) == "ALLOW"
    return SimpleNamespace(control=control, stage=stage, sha=sha, bundle=bundle,
                           app=app, sid=sid)


def _observed_role(argv, *, current, bundle):
    """Observe's bootstrap/relay/GUI predicates, applied to recorded real argv."""
    command = subprocess.list2cmdline(argv)
    bootstrap = (r'^\s*"?' + re.escape(str(current / "runtime/pythonw.exe"))
                 + r'"?\s+-I\s+-B\s+"?' + re.escape(str(current / "app/auto_update.py"))
                 + r'"?\s+--bootstrap(?:\s|$)')
    if re.search(bootstrap, command, re.IGNORECASE):
        return "bootstrap"
    if re.search(r"--label-match-user-relay(?:\s|$)", command, re.IGNORECASE):
        return "relay"
    main = re.escape(str(bundle / "app/main.py")) + r'"?(?:\s|$)'
    if re.search(main, command, re.IGNORECASE):
        if not re.search(r"--[a-z-]+", command, re.IGNORECASE):
            return "gui"
        if not re.search(r"--label-match-|--onboard-|--remove-|--restore-", command,
                         re.IGNORECASE):
            return "gui"
    return "writer"


def _waiting_on_real_mutex(thread_id):
    frame = sys._current_frames().get(thread_id)
    while frame is not None:
        if frame.f_code is _MUTEX_BODY.__code__ and frame.f_lineno == _MUTEX_WAIT_LINE:
            # A zero-time native probe from this third thread must see ownership
            # held by the first request while the second is in the native wait.
            return frame.f_locals["kernel"].WaitForSingleObject(
                frame.f_locals["handle"], 0
            ) == 0x102
        frame = frame.f_back
    return False


@pytest.mark.parametrize("phase,claim", GUI_CASES)
def test_concurrent_relay_only_gui_requests_share_real_update_mutex(tmp_path, monkeypatch,
                                                                  phase, claim):
    packet = _packet(tmp_path)
    state = {"schema": updates.SCHEMA, "phase": phase}
    if phase == "DEFERRED":
        state["recovery_pending"] = True
    _write_record(packet.control / "transition.json", state)
    if claim == "ABSENT":
        _write_record(packet.control / "apply-claim.json", {
            "schema": "code-update-apply-claim-v1", "status": "RUNNING",
            "pid": 2147483647, "creation_identity": "absent-fixture",
        })
    assert updates.apply_claim_status(packet.control) == claim
    assert updates.gui_recovery_state(state, claim)
    retained = (packet.control / "transition.json", packet.stage / "trial.json",
                packet.control / "active.json", packet.control / "policy.json")
    if claim == "ABSENT":
        retained += (packet.control / "apply-claim.json",)
    before = {path: path.read_bytes() for path in retained}
    boundary = (datetime.now(timezone.utc) - timedelta(minutes=1)).isoformat()
    rows, launches, observations = [], [], []
    launch_entered, release_launch, second_started = (threading.Event() for _ in range(3))
    first_thread, second_thread = [], []
    current = packet.control.parent / "current"

    def record(argv, *, created_at):
        role = _observed_role(argv, current=current, bundle=packet.bundle)
        row = dict(pid=os.getpid() + 100000 + len(rows), observable=True, bootstrap=role == "bootstrap",
                   role=role, sha=packet.sha, sid=packet.sid, ordinary_user=True,
                   created_at=created_at)
        rows.append(row)
        observations.append(copy.deepcopy(rows))
        return row

    relay_argv = [str(packet.bundle / "runtime/pythonw.exe"), "-I", "-B",
                  *updates.headless_product_arguments(packet.bundle, ["--label-match-user-relay"])]
    record(relay_argv, created_at=boundary)
    for _ in range(2):
        record([str(current / "runtime/pythonw.exe"), "-I", "-B",
                str(current / "app/auto_update.py"), "--bootstrap", "--role", "gui"],
               created_at=boundary)
    assert [row["role"] for row in rows] == ["relay", "bootstrap", "bootstrap"]

    def snapshot(control):
        assert control == packet.control
        observations.append(copy.deepcopy(rows))
        # PowerShell Observe excludes fixed bootstrap commands before identity
        # lookup; classify them from argv above rather than assign their roles.
        return copy.deepcopy([row for row in rows if not row["bootstrap"]])

    def spawn(argv, *, cwd, environment):
        launches.append((threading.get_ident(), list(argv)))
        assert cwd == str(packet.bundle)
        assert environment.get("KMTECH_LM_UPDATE_DEFERRED_AT") == boundary
        assert "KMTECH_LM_UPDATE_TRIAL_NONCE" not in environment
        if threading.get_ident() == first_thread[0]:
            launch_entered.set()
            assert release_launch.wait(4), "first process boundary was not released"
        row = record(argv, created_at=datetime.now(timezone.utc).isoformat())
        return SimpleNamespace(pid=row["pid"])

    monkeypatch.setattr(updates, "process_snapshot", snapshot)
    monkeypatch.setattr(updates, "start_ordinary_process", spawn)

    def request_first():
        first_thread.append(threading.get_ident())
        return updates._bootstrap(packet.app, [], role="gui")

    def request_second():
        second_thread.append(threading.get_ident())
        second_started.set()
        return updates._bootstrap(packet.app, [], role="gui")

    waited = False
    with ThreadPoolExecutor(max_workers=2) as pool:
        first = pool.submit(request_first)
        try:
            assert launch_entered.wait(3), "first request did not reach process boundary"
            second = pool.submit(request_second)
            assert second_started.wait(1)
            wait_deadline = time.monotonic() + 2
            while time.monotonic() < wait_deadline and not second.done():
                if _waiting_on_real_mutex(second_thread[0]):
                    waited = True
                    break
                time.sleep(0.005)
        finally:
            release_launch.set()
        results = [first.result(timeout=5), second.result(timeout=5)]

    assert waited, "second request must actually wait in the named update mutex"
    assert results == [0, 0]
    expected = [str(packet.bundle / "runtime/pythonw.exe"), "-I", "-B",
                str(packet.bundle / "app/main.py")]
    assert launches == [(first_thread[0], expected)]
    assert all(sum(row["role"] == "relay" for row in snapshot) == 1
               for snapshot in observations)
    assert all(sum(row["role"] == "gui" for row in snapshot) <= 1
               for snapshot in observations)
    # The relay is retained separately; neither bootstrap request may add a
    # second GUI writer to the cohort selected by the real writer filter.
    assert all(sum(row.get("role") == "gui" for row in updates.writer_processes(
        snapshot, own_pid=os.getpid())) <= 1 for snapshot in observations)
    assert sum(row["role"] == "gui" for row in rows) == 1
    assert sum(row["role"] == "relay" for row in rows) == 1
    assert {path: path.read_bytes() for path in retained} == before
