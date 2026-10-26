"""Preparation failures may blacklist only an authenticated target identity."""
from contextlib import nullcontext
from pathlib import Path

import pytest

import auto_update as updates
from tests.test_auto_update_ready_retry import NEW, OLD, _native_error, _preparation_case

SIGNED = "4" * 40
REAL_APPLY = updates._apply_prepared


@pytest.mark.parametrize("error", [_native_error(112), OSError(28, "simulated full disk")])
def test_full_disk_keeps_the_same_bounded_retry(tmp_path, monkeypatch, error):
    case = _preparation_case(tmp_path, monkeypatch, error)
    assert case.run() == 0
    assert case.state().get("failed_shas", []) == []
    assert case.state()["retry_shas"][NEW]["attempts"] == 1
    case.clock[0] = 900
    assert updates.prepared_pending(case.control, case.policy, case.state())


@pytest.mark.parametrize("authenticated", [None, SIGNED, "invalid"])
def test_unsigned_marker_cannot_choose_the_permanent_sha(tmp_path, monkeypatch, authenticated):
    error = updates._preparation_rejection("simulated validation rejection")
    if authenticated is not None:
        error._preparation_signed_sha = authenticated
    case = _preparation_case(tmp_path, monkeypatch, error)
    assert case.run() == 0
    expected = [SIGNED] if authenticated == SIGNED else []
    assert case.state().get("failed_shas", []) == expected
    assert NEW not in case.state().get("failed_shas", [])


def preparation_entry(tmp_path, monkeypatch):
    case = _preparation_case(tmp_path, monkeypatch, RuntimeError())
    case.policy["channel"] = "stable"
    case.files[case.control.parent / "current/app/update-manifest-key-config.json"] = {"manifest_public_key": None}
    case.policy["operator_local_app_data"] = str(tmp_path / "profile")
    monkeypatch.setattr(updates, "_apply_prepared", REAL_APPLY)
    lock = "_initial_ready_lock" if hasattr(updates, "_initial_ready_lock") else "_ready_window_update_lock"
    monkeypatch.setattr(updates, lock, lambda *a, **k: nullcontext())
    monkeypatch.setattr(updates, "prepared_pending", lambda *a: True)
    monkeypatch.setattr(updates, "process_snapshot", lambda *a: [])
    monkeypatch.setattr(updates, "new_apply_claim", lambda: {})
    monkeypatch.setattr(updates, "defer_stopped_relay", lambda *a: False)
    return case


def test_installed_manifest_failure_does_not_reject_the_candidate(tmp_path, monkeypatch):
    case = preparation_entry(tmp_path, monkeypatch)
    def unreadable(*a, **k):
        raise ValueError("installed manifest unreadable")
    monkeypatch.setattr(updates, "version_manifest", unreadable)
    assert case.run() == 0
    assert case.state().get("failed_shas", []) == []
    assert case.state()["retry_shas"][NEW]["attempts"] == 1


def test_resumed_installed_manifest_failure_keeps_the_candidate_retryable(tmp_path, monkeypatch):
    case = preparation_entry(tmp_path, monkeypatch)
    case.files[case.control / "transition.json"] = {
        "phase": "PREPARED", "previous_sha": OLD, "candidate_sha": SIGNED,
        "protected_packet": "packet-" + "1" * 32,
        "prepared_packet_root": str(case.control / ("packet-" + "1" * 32))}
    def unreadable(*a, **k):
        raise ValueError("installed manifest unreadable")
    monkeypatch.setattr(updates, "version_manifest", unreadable)
    assert case.run() == 0
    assert case.state().get("failed_shas", []) == []
    assert case.state()["retry_shas"][NEW]["attempts"] == 1


@pytest.mark.parametrize("failure", ["archive", "candidate_manifest", "target_mismatch", "pins"])
def test_authenticated_candidate_rejection_uses_signed_identity(tmp_path, monkeypatch, failure):
    case = preparation_entry(tmp_path, monkeypatch)
    case.files[case.control.parent / "current/app/update-manifest-key-config.json"] = {"manifest_public_key": "unused"}
    def manifest(path, **kw):
        if "extracted" in Path(path).parts:
            if failure == "candidate_manifest":
                raise ValueError("candidate manifest invalid")
            return {"version": "2.0.0", "source_commit": NEW if failure == "target_mismatch" else SIGNED}
        return {"version": "1.0.0", "source_commit": OLD}
    def invalid(*a, **k):
        raise ValueError("candidate validation failed")
    monkeypatch.setattr(updates, "version_manifest", manifest)
    monkeypatch.setattr(updates, "create_protected_stage", lambda *a: None)
    monkeypatch.setattr(updates, "copy_prepared_file", lambda *a, **k: None)
    monkeypatch.setattr(updates, "verify_prepared_packet", lambda *a, **k: (
        {"source_commit": SIGNED, "version": "2.0.0"}, {"archive_policy": {}, "archive": {}}))
    monkeypatch.setattr(updates, "extract_prepared_code", invalid if failure == "archive" else lambda *a: None)
    import Label_Match
    monkeypatch.setattr(Label_Match, "_sha256_file", lambda *a: "same")
    if hasattr(updates, "file_sha256"):
        monkeypatch.setattr(updates, "file_sha256", lambda *a: "same")
    pins = "authorize_feed_transition" if hasattr(updates, "authorize_feed_transition") else "manifest_transition_pins"
    monkeypatch.setattr(updates, pins, invalid)
    assert case.run() == 0
    assert case.state().get("failed_shas", []) == [SIGNED]
    assert NEW not in case.state()["failed_shas"]


@pytest.mark.parametrize("signature_valid", [False, True])
def test_packet_hash_rejection_carries_identity_only_after_signature(tmp_path, monkeypatch, signature_valid):
    import Label_Match as verifier
    manifest = {"source_commit": SIGNED, "artifact": {"size_bytes": 1}}
    (tmp_path / "manifest.sig").write_bytes(b"fixture")
    (tmp_path / "packet.zip").write_bytes(b"different size")
    monkeypatch.setattr(updates, "read_json", lambda *a: manifest)
    monkeypatch.setattr(updates, "_assert_no_reparse_components", lambda *a: None)
    def signature(*a):
        if not signature_valid:
            raise ValueError("signature invalid")
    monkeypatch.setattr(verifier, "_verify_update_manifest_signature", signature)
    monkeypatch.setattr(verifier, "_update_candidate_from_manifest", lambda *a, **k: {
        "archive": {"layout": "canonical_portable"}, "sha256": "0" * 64})
    with pytest.raises(ValueError) as caught:
        updates.verify_prepared_packet(tmp_path, key_config="unused", current_version="1.0.0", channel="stable")
    assert getattr(caught.value, "_preparation_signed_sha", None) == (SIGNED if signature_valid else None)
