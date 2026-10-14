"""Explicit lane qualification: unmodified stock runtimes, manifests and app bytes."""
import json
import os
from pathlib import Path
from types import SimpleNamespace
import zipfile

import pytest

import auto_update as updates
import current_user_onboarding
import user_relay
from tests.test_relay_trial_roles import pending

STOCK = (
    ("104", "e7b9075b2383889e77f524be7a1f0f80d7630033"),
    ("105", "4272c5601e035f6a111e493a4acfa5d8d182978f"),
)


@pytest.fixture(scope="module")
def copied_stock():
    config = Path(os.environ["IWRUN_CONFIG"])
    managed = config.parent / "stock-qualification"
    control = managed / "update-control"
    control.mkdir(parents=True)
    # A deliberately incomplete machine policy refuses in _machine_worker_context,
    # before the stock relay can open credentials or ordinary-user data.
    (control / "policy.json").write_text("{}", encoding="utf-8")
    result = {}
    for version, sha in STOCK:
        packet = Path("D:/KMTech/w9firstupd-evidence/stock-1013") / (
            "stock-packet-lm-v2.0." + version + "-" + sha[:7] + ".zip")
        assert packet.is_file(), "The approved stock input is required for this qualification"
        bundle = updates.version_bundle(control, sha)
        with zipfile.ZipFile(packet) as archive:
            for member in archive.infolist():
                target = bundle / member.filename
                assert target.resolve().is_relative_to(bundle.resolve())
                if member.is_dir():
                    target.mkdir(parents=True, exist_ok=True)
                else:
                    target.parent.mkdir(parents=True, exist_ok=True)
                    target.write_bytes(archive.read(member))
        identity = updates.version_manifest(bundle)
        assert identity["source_commit"] == sha
        assert identity["version"] == "v2.0." + version
        result[version] = sha
    return control, result


@pytest.mark.parametrize("target", ["104", "105"])
@pytest.mark.parametrize("phase", ["ROLLBACK_PENDING_READY", "FAILED", "APPLIED_PENDING_READY"])
def test_real_copied_versions_refuse_recovery_success_without_target_relay_ready(
        copied_stock, target, phase, monkeypatch):
    control, versions = copied_stock
    sha = versions[target]
    state = {**pending(), "phase": phase, "previous_sha": versions["104"],
             "candidate_sha": versions["105"]}
    active = {"sha": sha}
    (control / "active.json").write_text(json.dumps(active), encoding="utf-8")
    policy = {"operator_sid": "qualification-worker", "staging_root": str(control.parent / "stage")}
    policy_path = control / "policy.json"
    assert policy_path.read_text(encoding="utf-8") == "{}"
    monkeypatch.setattr(current_user_onboarding, "resolve_current_user_onboarding_paths",
                        lambda _: SimpleNamespace(direct_sync_root=control.parent / "isolated-worker"))
    monkeypatch.setattr(updates, "assert_ordinary_recovery", lambda *args: None)
    monkeypatch.setattr(updates, "emit_support_record", lambda *args, **kwargs: None)
    actual_launch = updates._launch_bootstrap_version
    child_ids = []
    def observe_launch(*args, **kwargs):
        pid = actual_launch(*args, **kwargs)
        child_ids.append(pid)
        return pid
    monkeypatch.setattr(updates, "_launch_bootstrap_version", observe_launch)
    result = updates.start_current(control, policy, state, active, [user_relay.USER_RELAY_MODE],
        code="UPDATE_TERMINAL_RECOVERED", deadline=updates.time.monotonic() + 2)
    assert result != 0
    assert len(child_ids) == 1 and child_ids[0] > 0
    assert not (Path(policy["staging_root"]) / "ready.json").exists()
    assert updates.version_manifest(updates.version_bundle(control, sha))["source_commit"] == sha


def test_copied_stock_gui_contract_cannot_be_confirmed_by_a_relay_receipt(copied_stock):
    control, versions = copied_stock
    state = {**pending(), "previous_sha": versions["104"], "candidate_sha": versions["105"]}
    state = updates.bind_trial(state, sha=versions["105"], pid=713, ordinary_user=True,
                               now=state["applied_at"], role="gui")
    receipt = {**{name: True for name in updates.RELAY_READY_CHECKS},
               "pid": 713, "sha": versions["105"], "role": "relay", "nonce": state["nonce"],
               "transition_id": state["transition_id"], "ready_at": updates.now_utc()}
    assert not updates.ready_matches(state, receipt, process_matches=True, relay_matches=True,
                                     now=updates.now_utc())
    for sha in versions.values():
        assert updates.version_manifest(updates.version_bundle(control, sha))["source_commit"] == sha
