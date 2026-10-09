import json
import pytest

import auto_update as updates


def test_installer_logon_task_child_runs_as_an_isolated_script(tmp_path):
    from pathlib import Path
    import re
    import subprocess
    import sys
    import xml.etree.ElementTree as ET

    sid = "S-1-5-21-101-202-303-1001"
    install_root = tmp_path / "installed"
    result = subprocess.run(
        [sys.executable, "-I", "-B", str(Path(updates.__file__).resolve()),
         "--print-logon-task", "--install-root", str(install_root), "--operator-sid", sid],
        capture_output=True, text=True, check=False,
        creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0),
    )
    assert result.returncode == 0, result.stderr
    spec = json.loads(result.stdout)
    assert re.fullmatch(r"KMTech\.LabelMatch\.StartupUpdate\.[0-9a-f]{24}", spec["name"])
    task = ET.fromstring(spec["xml"])
    ns = {"t": "http://schemas.microsoft.com/windows/2004/02/mit/task"}
    assert task.find("t:Triggers/t:LogonTrigger/t:UserId", ns).text == sid
    assert task.find("t:Actions/t:Exec/t:Command", ns).text.lower().endswith("powershell.exe")
    assert "apply_prepared_update.ps1" in task.find("t:Actions/t:Exec/t:Arguments", ns).text
    assert not install_root.exists()


NOW = "2026-10-01T04:00:00+00:00"
LATER = "2026-10-01T04:00:01+00:00"
OLD = "1" * 40
NEW = "2" * 40



def bind_test_packet_settings(control, policy):
    import hashlib
    settings=updates.policy_from_settings({key:policy[key] for key in ('feed_url','channel','apply_during_work','profile_auth') if key in policy})
    raw=updates.canonical_startup_settings(settings)
    path=control.parent/'current/app/auto-update-settings.json'
    path.parent.mkdir(parents=True,exist_ok=True);path.write_bytes(raw)
    policy.update(settings,settings_sha256=hashlib.sha256(raw).hexdigest())


def prepared():
    return updates.prepare_transition(
        previous_sha=OLD, candidate_sha=NEW, packet_sha256="3" * 64,
        forward_pin="4" * 64, rollback_pin="5" * 64,
        transition_id="6" * 32, nonce="7" * 32, now=NOW,
    )


def trial():
    state = updates.apply_transition(prepared(), writers_absent=True, packet_verified=True, durable_prepared=True, now=NOW)
    return updates.bind_trial(state, sha=NEW, pid=42, ordinary_user=True, now=NOW)


def ready(state):
    return {
        **{check: True for check in updates.READY_CHECKS},
        "pid": state["trial_pid"], "sha": NEW,
        "nonce": state["nonce"], "transition_id": state["transition_id"],
        "ready_at": LATER,
    }


def test_candidate_is_seen_before_system_has_created_a_marker():
    assert updates.startup_action(None, prepared_present=True) == "REQUEST_APPLY"
    assert updates.startup_action(None, prepared_present=False) == "START_CURRENT"


@pytest.mark.parametrize("missing", ["writers_absent", "packet_verified", "durable_prepared"])
@pytest.mark.parametrize("value", [False, None, 1])
def test_apply_does_not_authorize_switch_before_durable_write_or_verified_absence(missing, value):
    checks = dict(writers_absent=True, packet_verified=True, durable_prepared=True)
    checks[missing] = value
    before = prepared()
    with pytest.raises(ValueError):
        updates.apply_transition(before, now=NOW, **checks)
    assert before["phase"] == "PREPARED"


@pytest.mark.parametrize("change", [
    {"nonce": "8" * 32}, {"sha": OLD}, {"pid": 43}, {"pid": True},
    {"transition_id": "8" * 32}, {"ready_at": "2026-09-30T00:00:00+00:00"},
    {"ready_at": "2026-10-02T00:00:00+00:00"}, {"ready_at": "2026-10-01T04:00:01"},
    *[{check: False} for check in updates.READY_CHECKS],
])
def test_stale_or_partial_ready_does_not_confirm_a_new_version(change):
    state = trial()
    receipt = {**ready(state), **change}
    with pytest.raises(ValueError):
        updates.confirm_transition(state, receipt, process_matches=True, relay_matches=True, now=LATER)


def test_live_pid_and_heartbeat_are_insufficient_and_trial_only_runs_once():
    state = trial()
    assert not updates.ready_matches(state, {"pid": 42}, process_matches=True, relay_matches=True, now=LATER)
    assert not updates.ready_matches(state, ready(state), process_matches=True, relay_matches=False, now=LATER)
    with pytest.raises(ValueError):
        updates.bind_trial(state, sha=NEW, pid=43, ordinary_user=True, now=LATER)


def test_startup_failure_returns_to_previous_code_only_after_writer_absence():
    state = updates.request_rollback(trial(), reason="new application failed to initialize")
    # U1: the pure rollback fixture models the observed unchanged-G branch.
    state = {**state, "failure_decision": {"selected_sha": OLD}}
    unknown = updates.rollback_transition(state, writers_absent=None, previous_verified=True, durable_rollback=True, nonce="8" * 32, now=LATER)
    assert unknown["phase"] == "UNKNOWN"
    with pytest.raises(ValueError):
        updates.recovery_sha(unknown, pointer_sha=NEW)
    rollback = updates.rollback_transition(state, writers_absent=True, previous_verified=True, durable_rollback=True, nonce="8" * 32, now=LATER)
    assert rollback["trial_sha"] is None
    assert rollback["trial_started_at"] is None
    # D-16/E6: the already-confirmed previous code needs no second READY.
    assert updates.recovery_sha(rollback, pointer_sha=NEW) == OLD
    result = rollback
    assert result["phase"] == "ROLLED_BACK"
    assert result["failed_shas"] == [NEW]
    with pytest.raises(ValueError, match="failed version"):
        updates.prepare_transition(previous_sha=OLD, candidate_sha=NEW, packet_sha256="3" * 64, forward_pin="4" * 64, rollback_pin="5" * 64, transition_id="6" * 32, nonce="7" * 32, now=LATER, failed_shas=result["failed_shas"])


def test_power_loss_between_record_and_pointer_write_replays_recorded_code():
    before = prepared()
    assert updates.recovery_sha(before, pointer_sha=OLD) == OLD
    with pytest.raises(ValueError, match="before the applied record"):
        updates.recovery_sha(before, pointer_sha=NEW)
    applied = updates.apply_transition(before, writers_absent=True, packet_verified=True, durable_prepared=True, now=NOW)
    assert updates.recovery_sha(applied, pointer_sha=OLD) == NEW
    rollback = updates.request_rollback(applied, reason="startup failed")
    # U1: the pure rollback fixture models the observed unchanged-G branch.
    rollback = {**rollback, "failure_decision": {"selected_sha": OLD}}
    assert updates.recovery_sha(rollback, pointer_sha=NEW) == OLD
    assert updates.recovery_sha(rollback, pointer_sha=OLD) == OLD
    with pytest.raises(ValueError):
        updates.recovery_sha(rollback, pointer_sha="9" * 40)


def signed_pair():
    forward, reverse = pins()
    return dict(from_commit=OLD, to_commit=NEW,
                forward_contract_sha256=updates.directional_pin([forward], **forward),
                rollback_contract_sha256=updates.directional_pin([reverse], **reverse))


def pins():
    return [
        dict(zip(updates.PIN_FIELDS, (OLD, NEW, "a" * 64, "b" * 64))),
        dict(zip(updates.PIN_FIELDS, (NEW, OLD, "b" * 64, "a" * 64))),
    ]


def test_forward_approval_does_not_grant_reverse_or_another_contract():
    forward, reverse = pins()
    assert updates.directional_pin([forward], **forward)
    with pytest.raises(ValueError, match="no exact approval"):
        updates.directional_pin([forward], **reverse)
    with pytest.raises(ValueError, match="no exact approval"):
        updates.directional_pin([forward], **{**forward, "to_contract_sha256": "c" * 64})
    with pytest.raises(ValueError, match="no exact approval"):
        updates.directional_pin([], **forward)


@pytest.mark.parametrize("change", [{"from_commit": "*"}, {"to_commit": "2" * 39}, {"extra": True}])
def test_approval_rejects_wildcards_and_malformed_entries(change):
    forward = pins()[0]
    with pytest.raises(ValueError):
        updates.directional_pin([{**forward, **change}], **forward)


def test_both_reviewed_directions_authorize_even_when_consumer_bytes_change():
    current = dict(source_commit=OLD, contract_sha256="a" * 64, writer_sink_inventory_contract_sha256="a" * 64, data_api_contract_sha256="c" * 64)
    candidate = dict(source_commit=NEW, contract_sha256="b" * 64, writer_sink_inventory_contract_sha256="b" * 64, data_api_contract_sha256="c" * 64)
    assert len(updates.bidirectional_pins(pins(), current=current, candidate=candidate)) == 2
    with pytest.raises(ValueError, match="no exact approval"):
        updates.bidirectional_pins(pins()[:1], current=current, candidate=candidate)
    assert len(updates.bidirectional_pins(pins(), current=current, candidate={**candidate, "data_api_contract_sha256": "d" * 64})) == 2


def test_feed_defaults_and_work_time_switch_are_closed():
    assert updates.policy_from_settings({}) == dict(feed_url="", channel="stable", apply_during_work=False, profile_auth=False)
    for settings in ({"apply_during_work": True}, {"feed_url": "http://example.test/update"}, {"profile_auth": "true"}):
        with pytest.raises(ValueError):
            updates.policy_from_settings(settings)


def test_logon_task_has_fixed_system_action_and_operator_trigger(monkeypatch, tmp_path):
    import xml.etree.ElementTree as xml

    monkeypatch.setattr(updates, "system_executable", lambda relative: "C:/Windows/System32/" + relative)
    root = tmp_path / "current"
    sid = "S-1-5-21-111-222-333-1001"
    task = updates.install_task_spec(root, sid)
    document = xml.fromstring(task["xml"])
    ns = {"t": "http://schemas.microsoft.com/windows/2004/02/mit/task"}
    assert document.find("t:Triggers/t:LogonTrigger/t:UserId", ns).text == sid
    assert document.find("t:Principals/t:Principal/t:UserId", ns).text == "S-1-5-18"
    assert document.find("t:Settings/t:Hidden", ns).text == "true"
    assert document.find("t:Settings/t:AllowHardTerminate", ns).text == "false"
    action = document.find("t:Actions/t:Exec", ns)
    assert action.find("t:Command", ns).text.startswith("C:/Windows/System32/")
    assert "apply_prepared_update.ps1" in action.find("t:Arguments", ns).text
    assert str(root) in action.find("t:Arguments", ns).text
    assert updates.task_name(root, sid) != updates.task_name(root, sid.replace("1001", "1002"))


def test_pid_reuse_cannot_confirm_a_readiness_receipt():
    state = trial()
    row = dict(pid=42, observable=True, sha=NEW, sid="operator", role="gui", ordinary_user=True, created_at=LATER)
    assert not updates.process_matches_trial(row, state=state, sha=NEW, sid="operator", role="gui")
    assert updates.process_matches_trial({**row, "created_at": NOW}, state=state, sha=NEW, sid="operator", role="gui")


@pytest.mark.parametrize("phase", ["PREPARED", "UNKNOWN", "ROLLBACK_PENDING"])
def test_old_tools_cannot_write_during_a_transition(monkeypatch, tmp_path, phase):
    control = tmp_path / "update-control"
    monkeypatch.setattr(updates, "control_root", lambda _root: control)
    monkeypatch.setattr(updates, "ordinary_user_context", lambda _sid=None: True)
    monkeypatch.setattr(updates, "runtime_matches", lambda _root: True)
    monkeypatch.setattr(updates, "optional_record", lambda _path: {**prepared(), "phase": phase})
    monkeypatch.setattr(updates, "read_json", lambda path: {"operator_sid": "operator"} if path.name == "policy.json" else {"source_commit": OLD, "sha": OLD})
    monkeypatch.setattr(updates, "load_policy", lambda _control: {"operator_sid": "operator"})
    with pytest.raises(ValueError, match="startup transition"):
        updates.assert_version_writer(tmp_path / "app", source="csv", environ={})


def test_switch_record_is_flushed_before_pointer_and_failure_preserves_old_pointer(monkeypatch, tmp_path):
    calls = []
    state = trial()
    def write(path, payload):
        calls.append(path.name)
        if path.name == "active.json":
            raise OSError("injected pointer flush failure")
    monkeypatch.setattr(updates, "write_json_durable", write)
    with pytest.raises(OSError):
        updates.persist_switch(tmp_path, state, NEW)
    assert calls == ["transition.json", "active.json"]
    assert updates.recovery_sha(state, pointer_sha=OLD) == NEW


def test_failed_preparation_defers_but_failed_activation_remains_unknown(monkeypatch, tmp_path):
    from contextlib import nullcontext

    pointer = {"sha": OLD}
    state = {"schema": updates.SCHEMA, "phase": "DEFERRED"}
    writes = []
    monkeypatch.setattr(updates, "fixed_helper", lambda *_args, **_kwargs: "")
    monkeypatch.setattr(updates, "update_lock", lambda *_args, **_kwargs: nullcontext())
    monkeypatch.setattr(updates, "read_json", lambda path: {"operator_sid": "operator"} if path.name == "policy.json" else dict(pointer))
    monkeypatch.setattr(updates, "load_policy", lambda _control: {"operator_sid": "operator"})
    monkeypatch.setattr(updates, "optional_record", lambda _path: state)
    monkeypatch.setattr(updates, "write_json_durable", lambda path, payload: writes.append((path.name, payload)))
    def fail(_control):
        raise ValueError("injected verification failure")
    monkeypatch.setattr(updates, "_apply_prepared", fail)
    assert updates.apply_prepared(tmp_path) == 0
    assert writes[-1][1]["phase"] == "DEFERRED"
    state = {**trial(), "phase": "APPLIED_PENDING_READY"}
    assert updates.apply_prepared(tmp_path) == 20
    assert writes[-1][1]["phase"] == "UNKNOWN"


@pytest.mark.parametrize("failure_mode", ["exit", "launch_error"])
def test_engine_candidate_exit_restarts_previous_version_without_reverting_data(monkeypatch, tmp_path, failure_mode):
    """Exercise the apply loop/journal with synthetic process and crypto adapters."""
    import json
    import time
    from contextlib import nullcontext
    import Label_Match as update_service

    managed = tmp_path / "managed"
    control = managed / "update-control"
    stage = tmp_path / "operator" / "updates"
    (managed / "current/app").mkdir(parents=True)
    control.mkdir()
    stage.mkdir(parents=True)
    key = {"manifest_public_key": "key-adapter"}
    (managed / "current/app/update-manifest-key-config.json").write_text(json.dumps(key))
    policy = dict(feed_url="https://example.test/feed", channel="stable", operator_sid="S-1-5-21-455", operator_local_app_data=str(tmp_path / "operator"), staging_root=str(stage))
    bind_test_packet_settings(control, policy)
    # U1: retain the actual apply loop and all prior proof/data assertions.
    from kmtech_shared import data_generation as generation
    from writer_session_fence import canonical_control_root
    marker = canonical_control_root({"LOCALAPPDATA": policy["operator_local_app_data"]}) / "data-generation" / (policy["operator_sid"] + ".json")
    marker.parent.mkdir(parents=True)
    generation.record_generation(marker, "v2.0.98", app_id="Label_Match", subject_sid=policy["operator_sid"], build_id=OLD)
    updates.write_json_durable(control / "policy.json", policy)
    updates.write_json_durable(control / "active.json", {"schema": updates.SCHEMA, "sha": OLD})
    updates.write_json_durable(stage / "prepared.json", {"source_commit": NEW})
    for name in ("manifest.json", "manifest.sig", "packet.zip"):
        (stage / name).write_bytes(b"synthetic adapter input")
    business = tmp_path / "ledger-and-unfinished.bin"
    business.write_bytes(b"original business state must not be restored or deleted")
    protected_key_hash = "e" * 64
    verified = []
    monkeypatch.setenv("LOCALAPPDATA", str(tmp_path / "operator"))
    monkeypatch.setattr(updates, "update_lock", lambda *_args, **_kwargs: nullcontext())
    monkeypatch.setattr(updates, "fixed_helper", lambda _control, mode, **kwargs: verified.append((mode, kwargs.get("code_root"))) or "")
    monkeypatch.setattr(updates, "now_utc", lambda: LATER)
    monkeypatch.setattr(updates, "version_manifest", lambda bundle: dict(source_commit=OLD if bundle.name == OLD else NEW, version="v2.0.98" if bundle.name == OLD else "v2.0.99", writer_sink_inventory_contract_sha256="a" * 64 if bundle.name == OLD else "b" * 64, contract_sha256="a" * 64 if bundle.name == OLD else "b" * 64, data_api_contract_sha256="c" * 64 if bundle.name == OLD else "d" * 64))
    monkeypatch.setattr(updates, "verify_prepared_packet", lambda *_args, **_kwargs: ({"source_commit": NEW, "version": "v2.0.99", "approved_transitions": pins(), "auto_update": signed_pair()}, {"archive": {}, "sha256": "3" * 64}))
    monkeypatch.setattr(updates, "copy_prepared_file", lambda source, target, **_kwargs: target.write_bytes(source.read_bytes()))
    monkeypatch.setattr(updates, "preserve_candidate_version", lambda *_args: None)
    monkeypatch.setattr(updates, "extract_prepared_code", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(update_service, "_sha256_file", lambda _path: protected_key_hash)
    candidate_seen = False
    nonces = []
    def snapshot(_control):
        nonlocal candidate_seen
        state = updates.optional_record(control / "transition.json")
        if not state or state["phase"] == "PREPARED":
            return []
        if state["phase"] == "APPLIED_PENDING_READY":
            if candidate_seen:
                return []  # New GUI exited normally before READY.
            candidate_seen = True
            nonces.append(state["nonce"])
            if failure_mode == "launch_error":
                updates.write_json_durable(stage / "trial-failure.json", {
                    "nonce": state["nonce"], "transition_id": state["transition_id"], "sha": NEW,
                    "failed_role": "gui", "launch_failure_details": [{"type": "builtins.OSError", "win32_error": 5}],
                })
                return []
            updates.write_json_durable(stage / "trial.json", {"nonce": state["nonce"], "transition_id": state["transition_id"], "pid": 42})
            return [dict(pid=42, observable=True, sha=NEW, sid="S-1-5-21-455", role="gui", ordinary_user=True, created_at=LATER)]
        if state["phase"] == "ROLLBACK_PENDING_READY":
            raise AssertionError("E6 must not launch another previous-version trial")
        return []  # Candidate writers are absent before rollback.
    monkeypatch.setattr(updates, "process_snapshot", snapshot)
    monkeypatch.setattr(time, "sleep", lambda _seconds: None)
    original_policy = (control / "policy.json").read_bytes()
    original_marker = marker.read_bytes()
    assert updates.apply_prepared(control) == 0
    result = updates.read_json(control / "transition.json")
    assert result["phase"] == "ROLLED_BACK"
    assert result["failed_shas"] == [NEW]
    assert updates.read_json(control / "active.json")["sha"] == OLD
    assert len(nonces) == 1
    assert result["trial_sha"] is None
    assert marker.read_bytes() == original_marker
    assert result["data_api_evidence"] == {"previous_data_api_sha256":"c"*64,"candidate_data_api_sha256":"d"*64,"consumer_bytes_equal":False}
    assert business.read_bytes() == b"original business state must not be restored or deleted"
    assert (control / "policy.json").read_bytes() == original_policy
    assert any(mode == "Verify" and path.name == OLD for mode, path in verified)




def test_relay_logon_launches_gui_and_trial_relay_with_existing_lm_user_roots(monkeypatch, tmp_path):
    from contextlib import nullcontext
    import time

    control = tmp_path / "update-control"
    stage = tmp_path / "stage"
    control.mkdir(); stage.mkdir()
    policy = dict(operator_sid="operator", staging_root=str(stage))
    state = updates.apply_transition(prepared(), writers_absent=True, packet_verified=True, durable_prepared=True, now=NOW)
    bind_test_packet_settings(control, policy)
    updates.write_json_durable(control / "policy.json", policy)
    updates.write_json_durable(control / "active.json", {"sha": NEW})
    updates.write_json_durable(control / "transition.json", state)
    monkeypatch.setattr(updates, "control_root", lambda _root: control)
    monkeypatch.setattr(updates, "ordinary_user_context", lambda _sid=None: True)
    monkeypatch.setattr(updates, "update_lock", lambda *_args, **_kwargs: nullcontext())
    monkeypatch.setattr(updates, "prepared_pending", lambda *_args: False)
    # E1 requests supervision before a resumed trial; this is the OS task boundary.
    monkeypatch.setattr(updates, "request_apply_task", lambda *_args: None)
    launched = []
    monkeypatch.setattr(updates, "launch_version", lambda _control, sha, args, **kwargs: launched.append((sha, args, kwargs)) or 42)
    def complete(_seconds):
        updates.write_json_durable(control / "transition.json", {**state, "phase": "CONFIRMED"})
    monkeypatch.setattr(time, "sleep", complete)
    assert updates.bootstrap(tmp_path / "app", ["--label-match-user-relay"], role="relay") == 0
    assert [args for _, args, _ in launched] == [[], ["--label-match-user-relay"]]
    assert all(sha == NEW and settings["nonce"] == state["nonce"] for sha, _, settings in launched)


def test_portable_relay_gate_does_not_require_the_gui_pid(monkeypatch, tmp_path):
    from contextlib import nullcontext

    control = tmp_path / "control"
    state = trial()
    monkeypatch.setattr(updates, "control_root", lambda _root: control)
    monkeypatch.setattr(updates, "ordinary_user_context", lambda _sid=None: True)
    monkeypatch.setattr(updates, "update_lock", lambda *_args, **_kwargs: nullcontext())
    monkeypatch.setattr(updates, "optional_record", lambda path: state if path.name == "transition.json" else {"nonce": state["nonce"], "transition_id": state["transition_id"], "pid": 42})
    monkeypatch.setattr(updates, "read_json", lambda path: {"operator_sid": "operator", "staging_root": str(tmp_path)} if path.name == "policy.json" else {"sha": NEW})
    monkeypatch.setattr(updates, "load_policy", lambda _control: {"operator_sid": "operator", "staging_root": str(tmp_path)})
    monkeypatch.setattr(updates, "version_manifest", lambda _root: {"source_commit": NEW})
    monkeypatch.setenv("KMTECH_LM_UPDATE_TRIAL_NONCE", state["nonce"])
    assert updates.startup_gate(tmp_path / "app", ["--label-match-user-relay"]) is None


def manifests():
    current = dict(source_commit=OLD, contract_sha256="a" * 64, writer_sink_inventory_contract_sha256="a" * 64, data_api_contract_sha256="c" * 64)
    candidate = dict(source_commit=NEW, contract_sha256="b" * 64, writer_sink_inventory_contract_sha256="b" * 64, data_api_contract_sha256="c" * 64)
    return current, candidate


def test_signed_manifest_pairs_use_existing_es256_and_reject_other_key_and_tamper():
    import json
    import Label_Match as update_service
    from kmtech_zero_pe.cng_p256 import P256KeyPair

    manifest = dict(schema_version="kmtech-private-update-manifest-v1", manifest_version=1, signature_version="es256-v1", app_id="Label_Match", approved_transitions=pins())
    # Existing migration fixture uses ephemeral CNG keys; none are persisted.
    with P256KeyPair.generate() as key:
        signature = key.sign_es256(update_service._canonical_manifest_bytes(manifest))
        public = json.dumps(key.public_jwk, sort_keys=True)
    update_service._verify_update_manifest_signature(manifest, signature, public)
    current, candidate = manifests()
    assert updates.manifest_transition_pins(manifest, current=current, candidate=candidate)[3] == "SIGNED_EXACT_PAIR"
    with P256KeyPair.generate() as other:
        wrong_public = json.dumps(other.public_jwk, sort_keys=True)
    with pytest.raises(ValueError):
        update_service._verify_update_manifest_signature(manifest, signature, wrong_public)
    with pytest.raises(ValueError):
        update_service._verify_update_manifest_signature({**manifest, "approved_transitions": []}, signature, public)


@pytest.mark.parametrize("approvals", [None, [], [*pins(), {**pins()[0], "from_commit": "9" * 40}], [{**pins()[0], "from_commit": "*"}, pins()[1]], [{**pins()[0], "from_commit": "9" * 40}, pins()[1]]])
def test_unsigned_or_foreign_pair_cannot_authorize_changed_contract(approvals):
    current, candidate = manifests()
    manifest = {} if approvals is None else {"approved_transitions": approvals}
    with pytest.raises(ValueError):
        updates.manifest_transition_pins(manifest, current=current, candidate=candidate)




def test_reverse_audit_stays_in_transition_and_policy_is_unchanged(monkeypatch, tmp_path):
    updates.write_json_durable(tmp_path / "policy.json", {"feed_url": "https://example.test/feed"})
    original = (tmp_path / "policy.json").read_bytes()
    state = {**trial(), "rollback_pair": pins()[1], "pin_authority": "SIGNED_EXACT_PAIR"}
    updates.record_confirmed_transition(tmp_path, state)
    assert (tmp_path / "policy.json").read_bytes() == original
    confirmed = {**state, "phase": "CONFIRMED"}
    updates.write_json_durable(tmp_path / "transition.json", confirmed)
    result = updates.record_confirmed_transition(tmp_path, confirmed)
    assert (tmp_path / "policy.json").read_bytes() == original
    assert updates.read_json(tmp_path / "transition.json")["rollback_pair"] == pins()[1]
    assert result["confirmation_audit_applied"] is True


def test_prepared_handle_rejects_hardlink_and_foreign_owner(tmp_path):
    import os

    source = tmp_path / "source"
    source.write_bytes(b"owned packet")
    with pytest.raises(ValueError, match="owned"):
        with updates.unredirected_source(source, expected_owner_sid="S-1-5-21-0-0-0-65535"):
            pass
    os.link(source, tmp_path / "alias")
    with pytest.raises(ValueError, match="hardlinks"):
        with updates.unredirected_source(source):
            pass


def test_prepared_handle_holds_ancestors_and_bytes_until_copy_finishes(tmp_path):
    directory = tmp_path / "stage"
    directory.mkdir()
    source = directory / "packet.zip"
    source.write_bytes(b"authenticated original")
    with updates.unredirected_source(source) as incoming:
        with pytest.raises(OSError):
            source.write_bytes(b"concurrent replacement")
        with pytest.raises(OSError):
            directory.rename(tmp_path / "renamed")
        assert incoming.read() == b"authenticated original"
    source.write_bytes(b"new packet after readers finish")


@pytest.mark.parametrize("queue_state", ["healthy", "missing", "corrupt"])
def test_ready_reads_existing_ledger_and_queue_without_repair(monkeypatch, tmp_path, queue_state):
    import sqlite3
    from types import SimpleNamespace
    import current_user_onboarding
    import user_relay

    paths = SimpleNamespace(data_root=tmp_path, events_dir=tmp_path / "events", queue_dir=tmp_path / "queue",
                            ledger_path=tmp_path / "ledger.db", direct_sync_root=tmp_path / "direct_sync")
    paths.events_dir.mkdir()
    paths.queue_dir.mkdir()
    with sqlite3.connect(paths.ledger_path) as connection:
        connection.execute("CREATE TABLE preserved(value TEXT)")
    queue = paths.direct_sync_root / "queue/direct_sync_relay.sqlite3"
    queue.parent.mkdir(parents=True)
    if queue_state == "healthy":
        with sqlite3.connect(queue) as connection:
            connection.execute("CREATE TABLE preserved(value TEXT)")
    elif queue_state == "corrupt":
        queue.write_bytes(b"original corrupt evidence")
    before = queue.read_bytes() if queue.exists() else None
    state = trial()
    monkeypatch.setattr(updates, "trial_context", lambda _root: (tmp_path, {"staging_root": str(tmp_path)}, state))
    monkeypatch.setattr(updates, "ordinary_user_context", lambda *_args: True)
    monkeypatch.setattr(current_user_onboarding, "resolve_current_user_onboarding_paths", lambda _root: paths)
    monkeypatch.setattr(current_user_onboarding, "inspect_current_user_state", lambda _paths: {"status": "READY"})
    monkeypatch.setattr(user_relay, "user_relay_status_path", lambda _root: tmp_path / "relay.json")
    monkeypatch.setattr(updates, "optional_record", lambda _path: {"status": "RUNNING", "source_commit": NEW,
                                                                "update_nonce": state["nonce"], "ordinary_user": True})
    monkeypatch.setattr(updates, "version_manifest", lambda _root: {"source_commit": NEW})
    assert updates.publish_gui_ready(tmp_path / "app") is (queue_state == "healthy")
    assert (queue.read_bytes() if queue.exists() else None) == before
    assert (tmp_path / "ready.json").exists() is (queue_state == "healthy")



def test_unapproved_identical_contract_is_still_not_a_release_approval():
    current={"source_commit":OLD,"writer_sink_inventory_contract_sha256":"a"*64,"data_api_contract_sha256":"b"*64}
    candidate={**current,"source_commit":NEW}
    with pytest.raises(ValueError):
        updates.bidirectional_pins([],current=current,candidate=candidate)

def test_lm_trial_blocks_scan_before_reading_entry():
    import Label_Match
    app=object.__new__(Label_Match.Label_Match)
    app.__dict__["_startup_update_pending"]=True
    assert app.process_input() is None

@pytest.mark.parametrize("child",[
    {"sid":"foreign"},{"session_id":2},{"elevated":True},{"integrity_rid":12288},
])
def test_user_relay_launch_rejects_foreign_or_elevated_token(monkeypatch,child):
    from types import SimpleNamespace
    caller={"sid":"operator","session_id":1,"elevated":True,"integrity_rid":12288}
    normal={"sid":"operator","session_id":1,"elevated":False,"integrity_rid":8192}
    rows=iter([caller,{**normal,**child}])
    monkeypatch.setattr(updates,"process_security",lambda pid:next(rows))
    monkeypatch.setattr(updates,"_start_with_shell_token",lambda *a,**k:SimpleNamespace(pid=42))
    with pytest.raises(ValueError):updates.start_ordinary_process(["fixed.exe"],cwd="fixed")

def test_elevated_relay_uses_same_user_shell_and_checks_child(monkeypatch):
    from types import SimpleNamespace
    caller={"sid":"operator","session_id":1,"elevated":True,"integrity_rid":12288}
    normal={**caller,"elevated":False,"integrity_rid":8192}
    rows=iter([caller,normal]);calls=[]
    monkeypatch.setattr(updates,"process_security",lambda pid:next(rows))
    monkeypatch.setattr(updates,"_start_with_shell_token",lambda command,**kw:calls.append((command,kw)) or SimpleNamespace(pid=42))
    assert updates.start_ordinary_process(["fixed.exe"],cwd="fixed").pid==42
    assert calls[0][1]["expected"]==caller

def test_system_cannot_launch_operator_relay(monkeypatch):
    monkeypatch.setattr(updates,"process_security",lambda pid:{"sid":"SYSTEM","session_id":0,"elevated":True,"integrity_rid":16384})
    with pytest.raises(ValueError):updates.start_ordinary_process(["fixed.exe"],cwd="fixed")


@pytest.mark.parametrize("phase,pointer,expected", [
    ("APPLIED_PENDING_READY", NEW, False), ("CONFIRMED", OLD, False),
    ("CONFIRMED", NEW, True), ("ROLLED_BACK", NEW, True), ("UNKNOWN", NEW, False),
])
def test_ready_does_not_unlock_gui_before_protected_confirmation(monkeypatch,tmp_path,phase,pointer,expected):
    monkeypatch.setattr(updates,"control_root",lambda _root:tmp_path)
    monkeypatch.setattr(updates,"optional_record",lambda _path:{"phase":phase,"trial_sha":NEW})
    monkeypatch.setattr(updates,"read_json",lambda _path:{"sha":pointer})
    monkeypatch.setattr(updates,"version_manifest",lambda _root:{"source_commit":NEW})
    assert updates.confirmed_trial(tmp_path/"app") is expected


def test_unreadable_unfinished_state_cannot_publish_ready_or_change_evidence(monkeypatch,tmp_path):
    import sqlite3
    from types import SimpleNamespace
    import current_user_onboarding
    import user_relay
    paths=SimpleNamespace(data_root=tmp_path,ledger_path=tmp_path/"ledger.db",direct_sync_root=tmp_path/"sync")
    queue=paths.direct_sync_root/"queue/direct_sync_relay.sqlite3";queue.parent.mkdir(parents=True)
    for database in (paths.ledger_path,queue):
        with sqlite3.connect(database) as c:c.execute("CREATE TABLE preserved(value TEXT)")
    unfinished=tmp_path/"operator/worker_current_set_state_packaging.json";unfinished.parent.mkdir()
    original=b'original unreadable failure evidence';unfinished.write_bytes(original)
    state=trial()
    monkeypatch.setattr(updates,"trial_context",lambda _root:(tmp_path,{"staging_root":str(tmp_path)},state))
    monkeypatch.setattr(updates,"ordinary_user_context",lambda *_args:True)
    monkeypatch.setattr(current_user_onboarding,"resolve_current_user_onboarding_paths",lambda _root:paths)
    monkeypatch.setattr(current_user_onboarding,"inspect_current_user_state",lambda _paths:{"status":"READY"})
    monkeypatch.setattr(user_relay,"user_relay_status_path",lambda _root:tmp_path/"relay.json")
    assert updates.publish_gui_ready(tmp_path/"app") is False
    assert unfinished.read_bytes()==original
    assert not (tmp_path/"ready.json").exists()


def test_bootstrap_trial_relay_uses_verified_user_launch_and_bound_nonce(monkeypatch,tmp_path):
    from types import SimpleNamespace
    bundle=tmp_path/"versions"/NEW
    calls=[]
    monkeypatch.setattr(updates,"version_bundle",lambda _root,_sha:bundle)
    monkeypatch.setattr(updates,"version_manifest",lambda _root:{"source_commit":NEW})
    monkeypatch.setattr(updates,"start_ordinary_process",lambda command,**kwargs:calls.append((command,kwargs)) or SimpleNamespace(pid=42))
    nonce="1"*32
    monkeypatch.setenv('LABEL_MATCH_SAVE_DIR',str(tmp_path/'preserved-business-root'))
    assert updates.launch_version(tmp_path,NEW,["--label-match-user-relay"],nonce=nonce)==42
    assert calls[0][0][-1]=="--label-match-user-relay"
    assert calls[0][1]["environment"]["KMTECH_LM_UPDATE_TRIAL_NONCE"]==nonce
    assert calls[0][1]["cwd"]==str(bundle)
    assert calls[0][1]['environment']['LABEL_MATCH_SAVE_DIR']==str(tmp_path/'preserved-business-root')


def test_reviewed_gui_hotfix_changes_raw_consumer_evidence_without_weakening_exact_pair(monkeypatch,tmp_path):
    from writer_sink_inventory import derive_writer_sink_inventory,writer_sink_inventory_sha256
    monkeypatch.setattr(updates,'DATA_API_FILES',('Label_Match.py',))
    versions=[]
    for name,commit,ui in [('old',OLD,'old wording'),('new',NEW,'hotfix wording')]:
        root=tmp_path/name;root.mkdir()
        (root/'Label_Match.py').write_text(f'APP_VERSION="v2.0.94"\nUI_TEXT={ui!r}\n@writer_sink("fixture_write")\ndef write(): return {{"api": 1}}\n',encoding='utf-8')
        versions.append(dict(source_commit=commit,contract_sha256=writer_sink_inventory_sha256(derive_writer_sink_inventory(root)),writer_sink_inventory_contract_sha256=writer_sink_inventory_sha256(derive_writer_sink_inventory(root)),data_api_contract_sha256=updates.data_api_contract_sha256(root)))
    current,candidate=versions
    pair=dict(zip(updates.PIN_FIELDS,(OLD,NEW,current['writer_sink_inventory_contract_sha256'],candidate['writer_sink_inventory_contract_sha256'])))
    reverse=dict(zip(updates.PIN_FIELDS,(NEW,OLD,candidate['writer_sink_inventory_contract_sha256'],current['writer_sink_inventory_contract_sha256'])))
    assert current['data_api_contract_sha256'] != candidate['data_api_contract_sha256']
    assert updates.manifest_transition_pins({'approved_transitions':[pair,reverse]},current=current,candidate=candidate)[3]=='SIGNED_EXACT_PAIR'
    with pytest.raises(ValueError):updates.manifest_transition_pins({'approved_transitions':[]},current=current,candidate=candidate)
    with pytest.raises(ValueError):updates.manifest_transition_pins({'approved_transitions':[pair]},current=current,candidate=candidate)
    with pytest.raises(ValueError):updates.manifest_transition_pins({'approved_transitions':[pair,reverse]},current={**current,'source_commit':'9'*40},candidate=candidate)


def test_packet_writer_pin_is_recomputed_from_actual_bytes_without_importing_candidate(monkeypatch,tmp_path):
    app=tmp_path/'app';app.mkdir()
    source=app/'Label_Match.py'
    source.write_text('APP_VERSION="v2.0.94"\nraise RuntimeError("candidate must never execute")\n@writer_sink("fixture_write")\ndef write(): return 1\n',encoding='utf-8')
    (tmp_path/'INSTALL_THIS_PC.ps1').write_text('# pure fixture\n',encoding='utf-8')
    monkeypatch.setattr(updates,'DATA_API_FILES',('Label_Match.py',))
    monkeypatch.setattr(updates,'CONTRACT_FILES',('app/Label_Match.py',))
    pin=updates.version_writer_inventory_sha256(tmp_path)
    (tmp_path/'portable-manifest.json').write_text(json.dumps({'schema':'label-match-portable-tree-v1','version':'v2.0.94','startup_update_protocol':1,'source_commit':NEW,'writer_sink_inventory_contract_sha256':pin}),encoding='utf-8')
    assert updates.version_manifest(tmp_path)['writer_sink_inventory_contract_sha256']==pin
    source.write_text(source.read_text().replace('return 1','return 2'),encoding='utf-8')
    with pytest.raises(ValueError,match='actual source bytes'):updates.version_manifest(tmp_path)


def feed_versions():
    current,candidate=manifests()
    return {**current,'version':'v2.0.22'}, {**candidate,'version':'v2.0.24','data_api_contract_sha256':'d'*64}


def test_signed_future_pair_needs_no_install_time_allowlist():
    current,candidate=feed_versions()
    policy=updates.policy_from_settings({'feed_url':'https://company.test/lm'})
    assert 'approved_transitions' not in policy
    manifest={'source_commit':NEW,'version':candidate['version'],'approved_transitions':pins(),'auto_update':signed_pair()}
    result=updates.authorize_feed_transition(manifest,current=current,candidate=candidate)
    assert result[3]=='SIGNED_EXACT_PAIR'
    assert result[4]=={'previous_data_api_sha256':'c'*64,'candidate_data_api_sha256':'d'*64,'consumer_bytes_equal':False}


@pytest.mark.parametrize('change',['same_version','lower_version','reverse_feed','wrong_from','wrong_contract','no_pair','one_direction','target_identity','same_commit'])
def test_feed_authority_rejects_replay_downgrade_and_actual_identity_mismatch(change):
    current,candidate=feed_versions()
    manifest={'source_commit':NEW,'version':candidate['version'],'approved_transitions':pins(),'auto_update':signed_pair()}
    if change in {'same_version','lower_version'}:
        candidate={**candidate,'version':current['version'] if change=='same_version' else 'v2.0.21'}
        manifest['version']=candidate['version']
    elif change=='reverse_feed':
        current,candidate=candidate,current
        manifest.update(source_commit=candidate['source_commit'],version=candidate['version'])
    elif change=='wrong_from':current={**current,'source_commit':'9'*40}
    elif change=='wrong_contract':candidate={**candidate,'contract_sha256':'e'*64}
    elif change=='no_pair':manifest['approved_transitions']=[]
    elif change=='one_direction':manifest['approved_transitions']=pins()[:1]
    elif change=='target_identity':manifest['source_commit']='9'*40
    elif change=='same_commit':
        candidate={**candidate,'source_commit':OLD};manifest['source_commit']=OLD
        manifest['approved_transitions']=[{**pins()[0],'to_commit':OLD},{**pins()[1],'from_commit':OLD}]
    with pytest.raises(ValueError):updates.authorize_feed_transition(manifest,current=current,candidate=candidate)


def test_old_feed_cannot_raise_its_version_only_in_portable_metadata(monkeypatch,tmp_path):
    app=tmp_path/'app';app.mkdir()
    (app/'Label_Match.py').write_text('APP_VERSION="v2.0.22"\n@writer_sink("fixture_write")\ndef write(): return 1\n',encoding='utf-8')
    (tmp_path/'INSTALL_THIS_PC.ps1').write_text('# pure fixture\n',encoding='utf-8')
    monkeypatch.setattr(updates,'DATA_API_FILES',('Label_Match.py',))
    monkeypatch.setattr(updates,'CONTRACT_FILES',('app/Label_Match.py',))
    pin=updates.version_writer_inventory_sha256(tmp_path)
    (tmp_path/'portable-manifest.json').write_text(json.dumps({'schema':'label-match-portable-tree-v1','startup_update_protocol':1,'source_commit':OLD,'writer_sink_inventory_contract_sha256':pin,'version':'v9.0.0'}),encoding='utf-8')
    with pytest.raises(ValueError,match='actual application literal'):updates.version_manifest(tmp_path)


@pytest.mark.parametrize('damage',[None,'signature','zip'])
def test_future_feed_uses_existing_es256_packet_verification_before_authority(tmp_path,damage):
    import hashlib
    import zipfile
    import Label_Match
    from kmtech_zero_pe.cng_p256 import P256KeyPair
    from tests.test_update_manifest import canonical_manifest
    manifest=canonical_manifest();manifest.update(source_commit=NEW,signature_version='es256-v1',approved_transitions=pins(),auto_update=signed_pair())
    packet=tmp_path/'packet.zip'
    with zipfile.ZipFile(packet,'w') as archive:
        for name in manifest['archive']['required_files']:archive.writestr(name,b'never executed fixture')
    manifest['artifact'].update(sha256=hashlib.sha256(packet.read_bytes()).hexdigest(),size_bytes=packet.stat().st_size)
    with P256KeyPair.generate() as key:
        signature=key.sign_es256(Label_Match._canonical_manifest_bytes(manifest))
        public=json.dumps(key.public_jwk,sort_keys=True)
    (tmp_path/'manifest.json').write_text(json.dumps(manifest),encoding='utf-8')
    (tmp_path/'manifest.sig').write_bytes(signature if damage!='signature' else b'bad signature')
    if damage=='zip':packet.write_bytes(packet.read_bytes()+b'corrupt')
    if damage:
        with pytest.raises(ValueError):updates.verify_prepared_packet(tmp_path,key_config=public,current_version='v2.0.22',channel='stable')
    else:
        verified,_=updates.verify_prepared_packet(tmp_path,key_config=public,current_version='v2.0.22',channel='stable')
        current,candidate=feed_versions()
        assert updates.authorize_feed_transition(verified,current=current,candidate=candidate)[3]=='SIGNED_EXACT_PAIR'


@pytest.mark.parametrize('mode',['--remove-current-user-setup','--onboard-current-user','--restore-current-user-lifecycle-after-replacement'])
@pytest.mark.parametrize('phase',['CONFIRMED','APPLIED_PENDING_READY'])
def test_manual_user_lifecycle_after_auto_update_refuses_before_mutation(monkeypatch,tmp_path,capsys,mode,phase):
    import writer_session_fence as fence
    current=tmp_path/'current'; (current/'app').mkdir(parents=True)
    control=tmp_path/'update-control';control.mkdir()
    for path,value in [(current/'portable-manifest.json',{'source_commit':OLD}),
                       (control/'policy.json',{}),(control/'active.json',{'sha':NEW}),
                       (control/'transition.json',{'phase':phase})]:
        path.write_text(json.dumps(value),encoding='utf-8')
    data=tmp_path/'data';data.mkdir();(data/'unfinished.json').write_bytes(b'unique recovery data')
    policy={};bind_test_packet_settings(control,policy)
    (control/'policy.json').write_text(json.dumps(policy),encoding='utf-8')
    before={p.relative_to(tmp_path):p.read_bytes() for p in tmp_path.rglob('*') if p.is_file()}
    monkeypatch.setattr(fence,'canonical_control_root',lambda:tmp_path/'isolated-fence')
    monkeypatch.setattr(fence,'active_fence',lambda *_args,**_kwargs:None)
    if mode == '--remove-current-user-setup':
        assert updates.startup_gate(current/'app',[mode]) is None
        assert not capsys.readouterr().out
    else:
        assert updates.startup_gate(current/'app',[mode])==20
        result=json.loads(capsys.readouterr().out)
        assert result['status']=='RECOVERY_REQUIRED' and result['data_preserved'] is True
        assert result['code']=='SUPPORTED_REMOVAL_REQUIRED'
        assert '지원 제거' in result['message']
    assert before=={p.relative_to(tmp_path):p.read_bytes() for p in tmp_path.rglob('*') if p.is_file()}


def test_manual_lifecycle_exact_existing_delegation_is_preserved(monkeypatch,tmp_path):
    import writer_session_fence as fence
    current=tmp_path/'current';current.mkdir();control=tmp_path/'update-control';control.mkdir()
    for path,value in [(current/'portable-manifest.json',{'source_commit':OLD}),
                       (control/'active.json',{'sha':NEW}),(control/'transition.json',{'phase':'CONFIRMED'})]:
        path.write_text(json.dumps(value),encoding='utf-8')
    monkeypatch.setattr(fence,'canonical_control_root',lambda:tmp_path/'isolated-fence')
    monkeypatch.setattr(fence,'active_fence',lambda *_args,**_kwargs:{'existing':'authority'})
    seen=[]
    monkeypatch.setattr(fence,'_delegation_matches',lambda *_args,**kw:seen.append(kw['source']) or True)
    assert updates.manual_lifecycle_gate(control,'--remove-current-user-setup') is None
    assert seen==[]  # User removal no longer interprets machine deployment state.


@pytest.mark.parametrize('settings',[
    {'feed_url':'http://company.test/feed'}, {'feed_url':'https://github.com/org/feed'},
    {'feed_url':'https://raw.githubusercontent.com/org/feed'}, {'unexpected':False},
    {'apply_during_work':True}, {'apply_during_work':0}, {'profile_auth':'true'},
    {'feed_url':123}, {'channel':[]}, {'feed_url':'https://company.test/feed','profile_auth':False},
])
def test_startup_settings_reject_unapproved_address_fields_and_formats(settings):
    with pytest.raises(ValueError):updates.canonical_startup_settings(settings)


@pytest.mark.parametrize('damage',[None,'file','whitespace','policy'])
def test_installed_canonical_settings_are_bound_to_protected_policy(tmp_path,damage):
    control=tmp_path/'update-control';control.mkdir()
    policy={'feed_url':'https://company.test/lm/feed','channel':'stable','profile_auth':True,'apply_during_work':False}
    bind_test_packet_settings(control,policy)
    updates.write_json_durable(control/'policy.json',policy)
    path=control.parent/'current/app/auto-update-settings.json'
    if damage=='file':path.write_bytes(updates.canonical_startup_settings({**policy_from_keys(policy),'feed_url':'https://company.test/other'}))
    elif damage=='whitespace':path.write_bytes(path.read_bytes()+b' ')
    elif damage=='policy':updates.write_json_durable(control/'policy.json',{**policy,'feed_url':'https://company.test/other'})
    before={p:p.read_bytes() for p in tmp_path.rglob('*') if p.is_file()}
    if damage:
        with pytest.raises(ValueError):updates.load_policy(control)
    else:
        assert updates.load_policy(control)==policy
    assert before=={p:p.read_bytes() for p in before}


def policy_from_keys(policy):
    return {k:policy[k] for k in ('feed_url','channel','profile_auth','apply_during_work')}


def test_legacy_previous_pending_ready_record_still_confirms():
    """E6 removes new old-code trials while preserving durable legacy records."""
    state = updates.request_rollback(trial(), reason="legacy startup failure")
    state = {**state, "failure_decision": {"selected_sha": OLD}}
    settled = updates.rollback_transition(state, writers_absent=True,
        previous_verified=True, durable_rollback=True, nonce="8" * 32, now=LATER)
    legacy = {**settled, "phase": "ROLLBACK_PENDING_READY", "nonce": "8" * 32,
              "trial_sha": None, "trial_pid": None, "trial_started_at": None}
    legacy = updates.bind_trial(legacy, sha=OLD, pid=43, ordinary_user=True, now=LATER)
    receipt = {**ready(legacy), "sha": OLD}
    result = updates.confirm_transition(legacy, receipt, process_matches=True,
        relay_matches=True, now=LATER)
    assert result["phase"] == "ROLLED_BACK"
    assert result["failed_shas"] == [NEW]
