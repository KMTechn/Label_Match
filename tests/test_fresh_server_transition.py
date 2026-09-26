"""Fresh-server transitions preserve old bytes without exposing them to consumers."""

import hashlib
import importlib
import json
from pathlib import Path
import sqlite3
import re

import pytest

from current_user_onboarding import inspect_current_user_state, resolve_current_user_onboarding_paths
from tests.test_current_user_onboarding import _credential_loader, _profile_loader, _ready_state
from tests._possession_fixture import fake_possession_descriptor


SID = "S-1-5-21-100-200-300-1001"
GUID = "00112233-4455-6677-8899-aabbccddeeff"


def write_json(path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value), encoding="utf-8")


@pytest.fixture
def fresh(tmp_path, monkeypatch):
    transition = importlib.import_module("fresh_server_transition")
    env = {"LOCALAPPDATA": str(tmp_path / "local"), "PROGRAMDATA": str(tmp_path / "machine"),
           "COMPUTERNAME": "LABEL-PC01"}
    paths = resolve_current_user_onboarding_paths(tmp_path / "packet", environ=env)
    write_json(paths.app_root / "portable-manifest.json", {"source_commit": "c" * 40})
    acceptance = tmp_path / "server-accept.json"
    write_json(acceptance, {"phase": "ACCEPTED", "ready": True, "client_writes_blocked": False,
                           "snapshot_rollback_allowed": False, "source_commit": "c" * 40})
    target = {"schema": "kmtech.fresh-transition.v1", "origin": "https://fresh.example.invalid",
              "scope": "TEST1-FRESH", "deployment_id": "/srv/kmtech/releases/fresh-20260926",
              "pc": "LABEL-PC01", "sid": SID,
              "packet": hashlib.sha256((paths.app_root / "portable-manifest.json").read_bytes()).hexdigest(),
              "acceptance": {"record_path": str(acceptance),
                             "record_sha256": hashlib.sha256(acceptance.read_bytes()).hexdigest(),
                             "source_commit": "c" * 40, "accepted_by": "site-administrator"}}
    target_path = tmp_path / "transition-input.json"
    write_json(target_path, target)
    monkeypatch.setattr(transition, "_runtime_identity", lambda: ("LABEL-PC01", SID, GUID))
    monkeypatch.setattr(transition, "_possession_descriptor", lambda: fake_possession_descriptor(created=False))
    monkeypatch.setattr(transition, "_protect_directory", lambda p, sid: p.mkdir(parents=True, exist_ok=True))
    monkeypatch.setattr(transition, "_protect_inactive", lambda *a: None)
    monkeypatch.setattr(transition, "_set_security", lambda *a, **kw: None)
    monkeypatch.setattr(transition, "_reject_machine_environment_anchors", lambda: None)
    monkeypatch.setattr(transition, "_require_quiescence", lambda *a, **kw: {"status": "QUIESCED"})
    monkeypatch.setattr(transition, "_require_authority", lambda: {
        "session_id": "1" * 32, "attempt_id": "2" * 32, "replacement_transaction_id": "3" * 32})
    monkeypatch.setattr(transition, "_capture_persistence", lambda: {"run": {"exists": False}, "tasks": []})
    monkeypatch.setattr(transition, "_disable_persistence", lambda *a: None)
    monkeypatch.setattr(transition, "_restore_persistence", lambda *a: None)
    monkeypatch.setattr(transition, "_server_preflight", lambda *a: {
        "ready_source_commit": "c" * 40, "initial_challenge_status": 404,
        "initial_challenge_code": "producer_identity_not_found"})
    monkeypatch.setattr(transition, "_candidate", lambda *a: {
        "producer_id": "producer-label-pc01", "producer_install_id": "install-label-pc01",
        "source_host_id": "label-pc01", "manifest_hash": "e" * 64})
    return transition, env, paths, target_path, tmp_path / "preservation"


def test_interrupted_copy_preserves_partial_attempt_and_resume(fresh, monkeypatch):
    transition, env, paths, target, archive = fresh
    _ready_state(paths)
    real_copy = transition._copy_tree
    failed = []

    def interrupted(source, dest, snapshot):
        if "items" in dest.parts and not failed:
            dest.parent.mkdir(parents=True, exist_ok=True)
            dest.write_bytes(b"partial-copy")
            failed.append(dest)
            raise OSError("copy boundary")
        return real_copy(source, dest, snapshot)

    monkeypatch.setattr(transition, "_copy_tree", interrupted)
    with pytest.raises(OSError, match="copy boundary"):
        transition.prepare_fresh_server_registration(paths, target, archive, environ=env, confirm=True, confirm_unsent=True)
    original = paths.identity_path.read_bytes()
    result = transition.prepare_fresh_server_registration(paths, target, archive, environ=env, confirm=True, confirm_unsent=True)
    assert result["phase"] == "DETACHED"
    assert failed[0].read_bytes() == b"partial-copy"
    assert transition.archived_path(result, str(paths.identity_path)).read_bytes() == original


@pytest.mark.parametrize("boundary", ["before_copy", "after_detach"])
def test_restore_before_registration_preserves_original_bytes_and_metadata(fresh, monkeypatch, boundary):
    transition, env, paths, target, archive = fresh
    for key, value in env.items():
        monkeypatch.setenv(key, value)
    _ready_state(paths)
    paths.data_root.mkdir(parents=True, exist_ok=True)
    (paths.data_root / "session_backup.json").write_bytes(b"original-session")
    roots = transition._state_roots(paths, env)
    expected = {str(root): transition._snapshot(root) for root in roots}
    if boundary == "before_copy":
        monkeypatch.setattr(transition, "_server_preflight", lambda *a: (_ for _ in ()).throw(OSError("peer unavailable")))
        with pytest.raises(OSError):
            transition.prepare_fresh_server_registration(paths, target, archive, environ=env, confirm=True, confirm_unsent=True)
    else:
        transition.prepare_fresh_server_registration(paths, target, archive, environ=env, confirm=True, confirm_unsent=True)
    result = transition.restore_fresh_server(paths, confirm_old_server_ready=True)
    assert result["phase"] == "RESTORED"
    assert all(transition._snapshot(Path(root)) == snapshot for root, snapshot in expected.items())


@pytest.mark.parametrize("phase", ["REGISTERING", "REGISTERED", "ACTIVATED"])
def test_restore_refuses_any_possible_server_registration(fresh, monkeypatch, phase):
    transition, env, paths, target, archive = fresh
    for key, value in env.items():
        monkeypatch.setenv(key, value)
    _ready_state(paths)
    state = transition.prepare_fresh_server_registration(paths, target, archive, environ=env, confirm=True, confirm_unsent=True)
    state["phase"] = phase
    transition._save(paths, state)
    with pytest.raises(transition.FreshTransitionError, match="등록이 시작됐을 수"):
        transition.restore_fresh_server(paths, confirm_old_server_ready=True)
    assert not paths.identity_path.exists()


def test_machine_roots_require_helper_before_any_rename(fresh):
    transition, env, paths, target, archive = fresh
    _ready_state(paths)
    machine = Path(env["PROGRAMDATA"])
    legacy = machine / "KMTech/DirectSync/label-match-margin-r2/queue/old.db"
    shared = machine / "KMTech/Logistics/runtime-profile.json"
    other = machine / "KMTech/Logistics/profiles/OtherApp/runtime-profile.json"
    for file in (legacy, shared, other):
        file.parent.mkdir(parents=True, exist_ok=True)
        file.write_bytes(b"preserved")
    state = transition.prepare_fresh_server_registration(paths, target, archive, environ=env,
                confirm=True, confirm_unsent=True, defer_machine=True)
    assert state["phase"] == "QUIESCED"
    assert paths.identity_path.exists() and legacy.exists()
    assert shared.read_bytes() == other.read_bytes() == b"preserved"
    assert [e["source"] for e in state["entries"] if e["machine"]] == [str(legacy.parent.parent)]


def test_publication_that_would_land_in_a_new_work_root_is_refused_before_detaching(fresh):
    from dataclasses import replace
    transition, env, paths, target, archive = fresh
    _ready_state(paths)
    profiles = Path(env["LOCALAPPDATA"]) / "KMTech/Logistics/profiles"
    (profiles / "old.csv").write_bytes(b"old-business")
    paths = replace(paths, data_root=profiles)
    identity = paths.identity_path.read_bytes()
    with pytest.raises(transition.FreshTransitionError, match="구분할 수 없"):
        transition.prepare_fresh_server_registration(paths, target, archive, environ=env, plan_only=True)
    assert paths.identity_path.read_bytes() == identity
    assert (profiles / "old.csv").read_bytes() == b"old-business"
    assert not archive.exists()


def test_normal_onboarding_and_registration_block_during_transition(fresh, monkeypatch):
    import current_user_onboarding as onboarding
    from tools import register_label_match_worker_pc as registration
    transition, env, paths, target, archive = fresh
    for key, value in env.items():
        monkeypatch.setenv(key, value)
    _ready_state(paths)
    state = transition.prepare_fresh_server_registration(paths, target, archive, environ=env, confirm=True, confirm_unsent=True)
    before = transition._snapshot(Path(state["archive_root"]))
    with pytest.raises(onboarding.CurrentUserOnboardingError, match="fresh|전환"):
        onboarding.onboard_current_user(paths.app_root, environ=env)
    with pytest.raises(registration.DirectSyncPushError, match="fresh server transition"):
        registration._assert_no_pending_fresh_transition()
    assert transition._snapshot(Path(state["archive_root"])) == before


def test_hklm_anchor_is_rejected_without_printing_value(monkeypatch):
    import fresh_server_transition as transition
    import logistics_runtime_profile as profile
    monkeypatch.setattr(profile, "_machine_environment_value", lambda name: "secret-machine-anchor")
    with pytest.raises(transition.FreshTransitionError, match="HKLM") as error:
        transition._reject_machine_environment_anchors()
    assert "secret-machine-anchor" not in str(error.value)


def test_quiescence_does_not_claim_another_apps_shared_relay_runner(fresh):
    transition, env, paths, target, archive = fresh
    pattern = transition._owned_process_pattern(paths, [])
    assert re.search(pattern, 'python.exe C:/OtherApp/tools/direct_sync_relay_runner.py --manifest C:/OtherApp/manifest.json') is None
    assert re.search(pattern, 'python.exe C:/Runner/direct_sync_relay_runner.py --data-dir "' + str(paths.direct_sync_root) + '"')
    assert re.search(pattern, 'C:/KMTech/Apps/Label_Match/current/Label_Match.exe')


@pytest.mark.parametrize("old_state", ["ready", "legacy", "partial"])
def test_old_state_is_archived_and_identity_becomes_absent(fresh, old_state):
    transition, env, paths, target, archive = fresh
    if old_state == "ready":
        _ready_state(paths)
        assert inspect_current_user_state(paths, profile_loader=_profile_loader,
                                          credential_loader=_credential_loader)["status"] == "READY"
    else:
        write_json(paths.identity_path, {"schema_version": "legacy", "producer_id": "old"})
        if old_state == "legacy":
            write_json(paths.producer_manifest_path, {"apps": ["LabelMatch"]})
    paths.data_root.mkdir(parents=True, exist_ok=True)
    original = paths.data_root / "포장실작업이벤트로그_old_20260925.csv"
    original.write_bytes(b"old-event,unacknowledged\r\n")
    keyring = paths.data_root / "package_operation_lease_keyring.json"
    keyring.write_bytes(b'{"old-lease":"must-not-be-reused"}')
    expected = {str(p): p.read_bytes() for p in (original, keyring, paths.identity_path)}
    result = transition.prepare_fresh_server_registration(
        paths, target, archive, environ=env, confirm=True, confirm_unsent=True)
    assert result["phase"] == "DETACHED"
    assert inspect_current_user_state(paths)["status"] == "ABSENT"
    for source, raw in expected.items():
        assert not Path(source).exists()
        assert transition.archived_path(result, source).read_bytes() == raw
    assert result["old_uploads_permitted"] is False


def test_unsent_or_unknown_requires_separate_confirmation(fresh):
    transition, env, paths, target, archive = fresh
    paths.data_root.mkdir(parents=True)
    db = paths.data_root / "package_logistics_outbox.sqlite3"
    with sqlite3.connect(db) as conn:
        conn.execute("CREATE TABLE package_outbox (status TEXT)")
        conn.execute("INSERT INTO package_outbox VALUES ('UNKNOWN')")
    original = db.read_bytes()
    with pytest.raises(transition.FreshTransitionError, match="미전송|UNKNOWN"):
        transition.prepare_fresh_server_registration(paths, target, archive, environ=env, confirm=True)
    assert db.read_bytes() == original


def test_split_custom_legacy_machine_roots_and_sidecars_are_all_preserved(fresh):
    transition, env, paths, target, archive = fresh
    custom = paths.data_root.parent / "custom"
    write_json(paths.settings_path, {"custom_save_path": str(custom)})
    old_ledger = paths.ledger_path
    old_ledger.parent.mkdir(parents=True, exist_ok=True)
    old_ledger.write_bytes(b"old-onboarding-ledger")
    write_json(paths.onboarding_report_path, {"ledger_path": str(old_ledger)})
    paths = resolve_current_user_onboarding_paths(paths.app_root, environ=env)
    assert paths.ledger_path == old_ledger and paths.data_root == custom
    machine = Path(env["PROGRAMDATA"]) / "KMTech" / "Label_Match" / "data"
    expected = {}
    for root in (custom, old_ledger.parent, machine, paths.queue_dir):
        root.mkdir(parents=True, exist_ok=True)
        for name in ("old.sqlite3", "old.sqlite3-wal", "old.sqlite3-shm", "session_backup.json"):
            path = root / name
            path.write_bytes((str(root) + name).encode())
            expected[str(path)] = path.read_bytes()
    result = transition.prepare_fresh_server_registration(
        paths, target, archive, environ=env, confirm=True, confirm_unsent=True)
    for source, raw in expected.items():
        assert not Path(source).exists()
        assert transition.archived_path(result, source).read_bytes() == raw
    resumed_paths = resolve_current_user_onboarding_paths(paths.app_root, environ=env)
    assert resumed_paths.ledger_path.parent == resumed_paths.data_root


def test_interrupted_rename_resumes_without_overwriting_archive(fresh, monkeypatch):
    transition, env, paths, target, archive = fresh
    _ready_state(paths)
    original = paths.identity_path.read_bytes()
    rename = transition._detach_entry
    calls = []

    def crash_after_rename(entry):
        rename(entry)
        calls.append(entry)
        if len(calls) == 1:
            raise OSError("injected crash after rename")

    monkeypatch.setattr(transition, "_detach_entry", crash_after_rename)
    with pytest.raises(OSError, match="injected crash"):
        transition.prepare_fresh_server_registration(
            paths, target, archive, environ=env, confirm=True, confirm_unsent=True)
    monkeypatch.setattr(transition, "_detach_entry", rename)
    result = transition.prepare_fresh_server_registration(
        paths, target, archive, environ=env, confirm=True, confirm_unsent=True)
    assert result["phase"] == "DETACHED"
    assert transition.archived_path(result, str(paths.identity_path)).read_bytes() == original
    assert len(result["attempts"]) == 2


@pytest.mark.parametrize("field,value", [("sid", "S-1-5-21-100-200-300-1002"),
                                         ("pc", "OTHER-PC"), ("packet", "d" * 64),
                                         ("origin", "https://fresh.example.invalid/other")])
def test_target_binding_fails_before_any_old_file_changes(fresh, field, value):
    transition, env, paths, target, archive = fresh
    _ready_state(paths)
    raw = paths.identity_path.read_bytes()
    payload = json.loads(target.read_text())
    payload[field] = value
    write_json(target, payload)
    with pytest.raises(transition.FreshTransitionError):
        transition.prepare_fresh_server_registration(
            paths, target, archive, environ=env, confirm=True, confirm_unsent=True)
    assert paths.identity_path.read_bytes() == raw
    assert not archive.exists()
