"""Headless installed first setup with separate bootstrap and relay processes."""
import json
import hashlib
import os
from pathlib import Path

import pytest

from tests import _first_setup_host as host


@pytest.mark.skipif(os.name != "nt", reason="real Windows update mutex and Observe")
def test_clean_first_setup_reaches_ready_with_slow_real_observe(tmp_path):
    packet = os.environ.get("LABEL_MATCH_HOST_TEST_PACKET")
    if not packet:
        pytest.skip("a previously authenticated portable packet is required")
    config = host.prepare(tmp_path, Path(__file__).resolve().parents[1], Path(packet), feed_shape="test-pc")
    try:
        host.ready(config)
        proof = json.loads((tmp_path / "slow-observe-proof.json").read_text())
        assert proof["real_query_child"] and proof["real_update_mutex"]
        assert proof["elapsed_seconds"] >= 15
        host.restart(config)
    finally:
        host.stop_owned_relays(config)


@pytest.mark.skipif(os.name != "nt", reason="real Windows update mutex and Observe")
def test_preserved_previous_user_state_and_stop_marker_reach_ready(tmp_path):
    packet = os.environ.get("LABEL_MATCH_HOST_TEST_PREVIOUS_PACKET") or os.environ.get("LABEL_MATCH_HOST_TEST_PACKET")
    if not packet:
        pytest.skip("an authenticated previous portable packet is required")
    config = host.prepare(tmp_path, Path(__file__).resolve().parents[1], Path(packet), preserved=True, feed_shape="test-pc")
    seed = json.loads((tmp_path / "seed-result.json").read_text())
    assert seed["status"] == "PRESERVED_STOP_PROVEN"
    value = json.loads(config.read_text())
    current = json.loads((Path(value["current"]) / "portable-manifest.json").read_bytes())
    assert seed["old_version"] != current["version"]
    try:
        result = host.ready(config)
        assert result["status"] == "READY"
        assert result["registration_action"] == "REUSED"
        assert result["marker_was_present"] is True
        assert result["stop_release_status"] == "RELEASED"
        protected = json.loads((tmp_path / "preserved-bytes.json").read_text())
        assert all(hashlib.sha256(Path(path).read_bytes()).hexdigest() == digest for path, digest in protected.items())
        proof = json.loads((tmp_path / "slow-observe-proof.json").read_text())
        assert proof["elapsed_seconds"] >= 15 and proof["real_query_child"]
        host.restart(config)
    finally:
        host.stop_owned_relays(config)


@pytest.mark.skipif(os.name != "nt", reason="real Windows update mutex and Observe")
def test_relay_auto_update_124_to_125_reaches_running_and_owner_readback(tmp_path):
    from kmtech_zero_pe import P256KeyPair
    import user_relay
    packet = os.environ.get("LABEL_MATCH_HOST_TEST_PACKET")
    if not packet:
        pytest.skip("an authenticated portable packet is required")
    with P256KeyPair.generate() as signer:
        slow_observe = int(os.environ.get("LABEL_MATCH_HOST_TEST_SLOW_OBSERVE_SECONDS", "15"))
        assert slow_observe in {15, 30}
        config = host.prepare(tmp_path, Path(__file__).resolve().parents[1], Path(packet), signer=signer,
                              slow_observe=slow_observe)
        prepared_target = host.prepare_update(config, signer)
        try:
            host.ready(config)
            target = host.offer_update(config)
            assert target == prepared_target
            control = tmp_path / "machine/update-control"

            def confirmed():
                pointer = json.loads((control / "active.json").read_text())
                state = json.loads((control / "transition.json").read_text())
                return pointer.get("sha") == target and state.get("phase") == "CONFIRMED"

            host.wait_for(config, lambda: (tmp_path / "pending-prepared-ui-proof.json").exists())
            pending = json.loads((tmp_path / "pending-prepared-ui-proof.json").read_bytes())
            assert pending["phase"] == "DEFERRED"
            assert pending["screen_warning_decision"] is False
            assert pending["runtime_error_code_present"] is False
            assert pending["runtime_error_message_present"] is False
            state = json.loads((control / "transition.json").read_bytes())
            assert state["phase"] == "DEFERRED"
            host.restart(config, "update-restart")
            state = json.loads((control / "transition.json").read_bytes())
            deferred = state.get("phase") == "DEFERRED"
            if deferred:
                host.restart(config, "update-deferred-next")
            state = json.loads((control / "transition.json").read_bytes())
            host.write(tmp_path / "deferred-next-start-proof.json", {
                "deferred_after_trial": deferred,
                "next_start_entered": deferred,
                "confirmed_after_next_start": deferred and state.get("phase") == "CONFIRMED",
            })
            host.wait_for(config, confirmed)
            status_path = user_relay.user_relay_status_path(tmp_path / "local/KMTech/DirectSync/label_match")
            status = json.loads(status_path.read_text())
            assert status["status"] == "RUNNING" and status["source_commit"] == target
            owner = host.launch(config, "read-owner")
            assert owner.wait() == 0
            result = json.loads((tmp_path / "update-owner-result.json").read_text())
            assert result["status"] == "ALIVE" and result["existing_relay_reused"] is True
            assert result["process_id"] == status["process_id"]
            host.prove_next_cycle(config, name="updated", process_id=status["process_id"],
                                  accept_initial_pass=True)
            declared = json.loads((tmp_path / "machine/versions" / target / "portable-manifest.json").read_bytes())
            assert declared["version"] == "v2.0.125"
        finally:
            host.stop_owned_relays(config)


@pytest.mark.skipif(os.name != "nt", reason="real Windows update mutex and Observe")
def test_legacy_stable_manifest_is_log_only_and_relay_stays_ready(tmp_path):
    packet = os.environ.get("LABEL_MATCH_HOST_TEST_PACKET")
    if not packet:
        pytest.skip("an authenticated portable packet is required")
    config = host.prepare(tmp_path, Path(__file__).resolve().parents[1], Path(packet), feed_shape="legacy")
    try:
        host.ready(config)
        assert list((tmp_path / "preparation-frames").glob("*.json"))
        host.restart(config)
    finally:
        host.stop_owned_relays(config)
