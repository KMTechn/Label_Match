"""LM113 stop preflight and read-only rollback GUI recovery."""
import builtins
import copy
import hashlib
import json
import sqlite3
import types
from contextlib import nullcontext

import pytest

import auto_update as updates
from kmtech_shared import data_generation as generation
from test_recovery_gui_bootstrap import memory_world, system
from user_relay import release_user_relay_stop_marker, user_relay_stop_path
from user_relay_stop_marker import read_stop_marker
from test_user_relay_stop_marker import _root_marker, _write_marker


class PastStopPreflight(Exception):
    pass


def apply_until_preflight(control):
    try:
        return updates._apply_prepared_once(control)
    except PastStopPreflight:
        return None


def disk_world(tmp_path, monkeypatch, phase="PREPARED"):
    w = memory_world(phase, "NONE", writer=None)
    control = tmp_path / "control"
    control.mkdir()
    policy = {**w.policy, "operator_local_app_data": str(tmp_path / "operator")}
    if phase == "DEFERRED":
        w.state["recovery_pending"] = False
    policy["staging_root"] = str(tmp_path / "staging")
    (tmp_path / "staging").mkdir()
    (control / "active.json").write_text(json.dumps({"sha": w.previous}))
    (control / "transition.json").write_text(json.dumps(w.state))
    monkeypatch.setattr(updates, "load_policy", lambda *_: policy)
    monkeypatch.setattr(updates, "fixed_helper", lambda *a, **kw: None)
    monkeypatch.setattr(updates, "update_lock", lambda *a, **kw: nullcontext())
    monkeypatch.setattr(updates, "new_apply_claim", lambda: {"schema": "test-claim"})
    monkeypatch.setattr(updates, "process_snapshot", lambda *_: [])
    monkeypatch.setattr(updates, "record_confirmed_transition", lambda _, state: state)
    marker = user_relay_stop_path(
        tmp_path / "operator" / "KMTech" / "DirectSync" / "label_match")
    marker.parent.mkdir(parents=True)
    _write_marker(marker, _root_marker())
    return w, control, policy, marker


@pytest.mark.parametrize("phase", ["PREPARED", "CONFIRMED", "DEFERRED"])
def test_stop_marker_defers_system_without_switch_or_marker_change(tmp_path, monkeypatch, phase):
    w, control, policy, marker = disk_world(tmp_path, monkeypatch, phase)
    monkeypatch.setattr(updates, "prepared_pending", lambda *_: True)
    def past_preflight(*args, **kwargs):
        if len(args) < 2 or args[1] != "Authority":
            raise PastStopPreflight
    monkeypatch.setattr(updates, "fixed_helper", past_preflight)
    monkeypatch.setattr(updates, "authorize_prepared_replay", past_preflight)
    before = (control / "active.json").read_bytes(), marker.read_bytes()
    assert apply_until_preflight(control) == 0
    deferred = updates.read_json(control / "transition.json")
    assert deferred["phase"] == "DEFERRED"
    assert "recovery_pending" not in deferred
    assert deferred.get("retained_candidate") is False
    assert deferred["reason_code"] == "UPDATE_RELAY_STOP_REQUESTED"
    assert ((control / "active.json").read_bytes(), marker.read_bytes()) == before
    state_bytes = (control / "transition.json").read_bytes()
    assert updates._apply_prepared_once(control) == 0
    assert (control / "transition.json").read_bytes() == state_bytes


@pytest.mark.parametrize("role", ["gui", "relay"])
def test_bootstrap_stop_preflight_opens_current_role_without_request(tmp_path, monkeypatch, role):
    w, control, policy, marker = disk_world(tmp_path, monkeypatch)
    monkeypatch.setattr(updates, "control_root", lambda *_: control)
    monkeypatch.setattr(updates, "ordinary_user_context", lambda *_: True)
    monkeypatch.setattr(updates, "apply_claim_status", lambda *_: "NONE")
    monkeypatch.setattr(updates, "prepared_pending", lambda *_: True)
    monkeypatch.setattr(updates, "request_apply_task", lambda *_: pytest.fail("request"))
    import current_user_onboarding
    monkeypatch.setattr(current_user_onboarding, "_machine_worker_context", lambda *_: None)
    monkeypatch.setattr(updates, "current_generation_status", lambda *_: "ALLOW")
    launches = []
    resident_status, resident_rows = {}, []
    if role == "gui":
        from user_relay import user_relay_status_path
        direct = tmp_path / "resident"
        status_path = user_relay_status_path(direct)
        optional_record = updates.optional_record
        native_identity = updates.native_process_identity
        resident_pid = updates.os.getpid() + 100000
        monkeypatch.setattr(current_user_onboarding, "resolve_current_user_onboarding_paths",
                            lambda *_: types.SimpleNamespace(direct_sync_root=direct))
        monkeypatch.setattr(updates, "optional_record", lambda path:
                            dict(resident_status) if path == status_path else optional_record(path))
        monkeypatch.setattr(updates, "native_process_identity", lambda pid:
                            {"status": "LIVE", "creation_identity": "resident-generation"}
                            if pid == resident_pid else native_identity(pid))
        monkeypatch.setattr(updates, "process_snapshot", lambda *_: list(resident_rows))

    def launch(*args, **kwargs):
        launches.append((args, kwargs))
        if role == "gui":
            at = updates.now_utc()
            resident_status.update(status="RUNNING", source_commit=w.previous,
                                   ordinary_user=True, process_id=resident_pid, updated_at=at,
                                   code_root=str(updates.version_bundle(control, w.previous) / "app"))
            resident_rows.append(dict(pid=resident_pid, role="relay", observable=True,
                                      ordinary_user=True, sha=w.previous,
                                      sid=policy["operator_sid"], created_at=at))
        return 12345

    monkeypatch.setattr(updates, "_launch_bootstrap_version", launch)
    monkeypatch.setattr(updates, "wait_current_relay", lambda *a, **kw: True)
    writes = []
    write = updates.write_json_durable
    def record(path, value):
        writes.append(path.name)
        return write(path, value)
    monkeypatch.setattr(updates, "write_json_durable", record)
    before = {p: p.read_bytes() for p in (control / "transition.json", control / "active.json", marker)}
    arguments = [] if role == "gui" else ["--label-match-user-relay"]
    if role == "relay":
        from user_relay import StartupStopRequested
        with pytest.raises(StartupStopRequested):
            updates._bootstrap(control, arguments, role=role)
        assert updates.bootstrap(control, arguments, role=role) == 0
        assert launches == []
        assert writes == []
    else:
        assert updates._bootstrap(control, arguments, role=role) == 0
        assert writes == ["startup-support.json"]
        assert updates.read_json(tmp_path / "staging/startup-support.json")["reason_code"] == "UPDATE_RELAY_STOP_REQUESTED"
        assert launches[0][0][1:3] == (w.previous, arguments)
    assert all(p.read_bytes() == value for p, value in before.items())


def test_existing_onboarding_release_allows_next_system_apply(tmp_path, monkeypatch):
    w, control, policy, marker = disk_world(tmp_path, monkeypatch)
    monkeypatch.setattr(updates, "prepared_pending", lambda *_: True)
    monkeypatch.setattr(updates, "authorize_prepared_replay", lambda *_: (_ for _ in ()).throw(PastStopPreflight()))
    assert apply_until_preflight(control) == 0
    request, _, digest = read_stop_marker(marker)
    released = release_user_relay_stop_marker(marker.parent.parent,
        expected_request_id=request["request_id"], expected_sha256=digest,
        marker_lease_factory=lambda *_: types.SimpleNamespace(close=lambda: None))
    assert released["status"] == "RELEASED" and not marker.exists()
    reached = []

    def verify_current(*args, **kwargs):
        reached.append("verify")
        raise RuntimeError("authenticated-apply-boundary")

    monkeypatch.setattr(updates, "fixed_helper", verify_current)
    # Authority is the first fixed helper call; permit it before checking replay.
    def helper(*args, **kwargs):
        if args[1] != "Authority":
            return verify_current(*args, **kwargs)

    monkeypatch.setattr(updates, "fixed_helper", helper)
    monkeypatch.setattr(updates, "settle_trial_failure", lambda *_: pytest.fail("not a failed trial"))
    with pytest.raises(RuntimeError, match="authenticated-apply-boundary"):
        updates._apply_prepared_once(control)
    assert reached == ["verify"]
    assert updates.read_json(control / "active.json")["sha"] == w.previous


def real_generation_world(tmp_path, claim):
    w = memory_world("ROLLBACK_PENDING", claim)
    w.policy["operator_sid"] = updates.current_process_sid()
    for row in w.rows:
        row["sid"] = w.policy["operator_sid"]
    previous = w.state["generation_previous_release"]
    marker = tmp_path / "generation.json"
    record = generation.record_generation(marker, previous["version"], app_id="Label_Match",
        subject_sid=w.policy["operator_sid"], build_id=w.previous)
    w.generation = {"status": "READ", "marker_path": str(marker), "record": record}
    w.state["generation_before"] = copy.deepcopy(w.generation)
    original_import = w.ns["__builtins__"]["__import__"]

    def importer(name, *args, **kwargs):
        if name == "kmtech_shared":
            return builtins.__import__(name, *args, **kwargs)
        return original_import(name, *args, **kwargs)

    w.ns["__builtins__"]["__import__"] = importer
    w.ns["current_generation_status"] = types.FunctionType(updates.current_generation_status.__code__, w.ns)
    w.ns["version_manifest"] = lambda path: previous
    return w, marker


@pytest.mark.parametrize("claim", ["NONE", "ABSENT"])
def test_pending_gui_business_writes_survive_real_generation_settlement(tmp_path, claim):
    w, marker = real_generation_world(tmp_path, claim)
    before = copy.deepcopy(w.files)
    assert w.bootstrap()["rc"] == 0
    assert [row["role"] for row in w.launches] == ["gui"]
    assert w.files == before
    assert not w.gui_environments[0].get("KMTECH_LM_UPDATE_TRIAL_NONCE")
    # A normal active-version write leaves the generation unchanged, but changes
    # durable business data. Settlement must retain those new ledger quantities.
    database = tmp_path / "business.sqlite3"
    with sqlite3.connect(database) as connection:
        connection.execute("CREATE TABLE ledger (item TEXT PRIMARY KEY, quantity INTEGER)")
        connection.execute("INSERT INTO ledger VALUES (?, ?)", ("tray", 4))
    generation.record_generation(marker, w.state["generation_previous_release"]["version"],
        app_id="Label_Match", subject_sid=w.policy["operator_sid"], build_id=w.previous)
    fingerprint = hashlib.sha256(database.read_bytes()).hexdigest(), marker.read_bytes()
    assert system(w)["rc"] == 20
    assert w.state == before[str(w.control / "transition.json")]
    w.rows = []
    w.auto_ready = True
    assert system(w)["exception"] is None
    assert w.state["phase"] == "ROLLED_BACK" and w.active == w.previous
    assert (hashlib.sha256(database.read_bytes()).hexdigest(), marker.read_bytes()) == fingerprint
    with sqlite3.connect(database) as connection:
        assert connection.execute("SELECT item, quantity FROM ledger").fetchall() == [("tray", 4)]


@pytest.mark.parametrize("claim", ["NONE", "ABSENT", "LIVE", "UNKNOWN"])
@pytest.mark.parametrize("bad", ["missing", "two", "generation", "active"])
def test_pending_gui_incomplete_conditions_preserve_block(tmp_path, claim, bad):
    w, marker = real_generation_world(tmp_path, claim)
    if bad == "missing":
        w.rows = []
    elif bad == "two":
        w.rows.append({**w.rows[0], "pid": w.rows[0]["pid"] + 1})
    elif bad == "generation":
        marker.write_bytes(b"unreadable generation")
    else:
        w.files[str(w.control / "active.json")]["sha"] = w.candidate
        w.rows[0]["sha"] = w.candidate
    w.ns["request_apply_task"] = lambda *_: None
    before = copy.deepcopy(w.files)
    result = w.bootstrap()
    if claim == "ABSENT" or (claim == "NONE" and bad != "missing"):
        assert result["rc"] == 20
    else:
        # Preserve the existing live/unknown-owner wait and read-only refusal.
        assert result["rc"] == 20 or result["exception"] == "ValueError"
    assert not w.launches
    assert w.files == before


def test_stop_arriving_during_prepared_verification_prevents_switch(tmp_path, monkeypatch):
    w, control, policy, marker = disk_world(tmp_path, monkeypatch)
    raw = marker.read_bytes()
    marker.unlink()
    monkeypatch.setattr(updates, "authorize_prepared_replay", lambda *_: marker.write_bytes(raw))
    switched = []
    def switch(*args):
        switched.append(args)
        raise PastStopPreflight
    monkeypatch.setattr(updates, "persist_switch", switch)
    assert apply_until_preflight(control) == 0
    assert not switched and marker.read_bytes() == raw
    assert updates.read_json(control / "active.json")["sha"] == w.previous
    assert updates.read_json(control / "transition.json")["reason_code"] == "UPDATE_RELAY_STOP_REQUESTED"


def test_stop_preflight_uses_policy_operator_root_not_ambient_overrides(tmp_path, monkeypatch):
    _, _, policy, marker = disk_world(tmp_path, monkeypatch)
    monkeypatch.setenv("LABEL_MATCH_DIRECT_SYNC_ROOT", str(tmp_path / "unrelated"))
    assert updates.relay_stop_requested(policy) is True
    marker.unlink()
    assert updates.relay_stop_requested(policy) is False
    policy["operator_local_app_data"] = "relative-operator"
    with pytest.raises(ValueError):
        updates.relay_stop_requested(policy)


@pytest.mark.parametrize("phase", ["PREPARED", "CONFIRMED", "DEFERRED"])
def test_stop_deferred_record_remains_readable_by_existing_onboarding_gate(tmp_path, monkeypatch, phase):
    _, control, policy, _ = disk_world(tmp_path, monkeypatch, phase)
    monkeypatch.setattr(updates, "prepared_pending", lambda *_: True)
    assert updates._apply_prepared_once(control) == 0
    active = updates.read_json(control / "active.json")["sha"]
    state = updates.read_json(control / "transition.json")
    # resolve_lifecycle_context(register) calls this exact consistency proof
    # whenever a deferred record retains its candidate identity.
    assert updates.recovery_sha(state, pointer_sha=active) == active


@pytest.mark.parametrize("pointer", ["previous", "candidate", "other"])
@pytest.mark.parametrize("retained", [False, True, "invalid"])
@pytest.mark.parametrize("pending", ["absent", False, True])
def test_stop_deferred_recovery_sha_contract(pointer, retained, pending):
    w = memory_world()
    state = {**w.state, "phase": "DEFERRED", "reason_code": "UPDATE_RELAY_STOP_REQUESTED",
             "retained_candidate": retained}
    state.pop("recovery_pending", None)
    if pending != "absent":
        state["recovery_pending"] = pending
    active = {"previous": w.previous, "candidate": w.candidate, "other": "c" * 40}[pointer]
    if retained is False and pending == "absent" and pointer != "other":
        assert updates.recovery_sha(state, pointer_sha=active) == active
    else:
        with pytest.raises(ValueError):
            updates.recovery_sha(state, pointer_sha=active)


@pytest.mark.parametrize("phase", ["FAILED", "UNKNOWN", "ROLLBACK_PENDING"])
def test_trial_failure_stop_keeps_existing_system_recovery_request(phase):
    w = memory_world(phase, "NONE", writer=None)
    w.prepared = True
    w.ns["relay_stop_requested"] = lambda *_: True
    requested = []
    class RecoveryRequested(Exception):
        pass
    def request(*_):
        requested.append(True)
        raise RecoveryRequested
    w.ns["request_apply_task"] = request
    outcome = w.bootstrap()
    assert requested == [True]
