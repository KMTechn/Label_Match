"""Regression checks for the process-chain harness, without starting a product."""
import json
from pathlib import Path
import sys
from types import SimpleNamespace

import pytest

from tests import _first_setup_host as host


def test_host_private_compatibility_uses_the_product_packaged_key_loader(tmp_path, monkeypatch):
    import Label_Match as product
    import label_data_generation as generation
    from kmtech_zero_pe import P256KeyPair
    from tests.test_update_manifest import canonical_manifest
    monkeypatch.setattr(product, '__file__', str(tmp_path / 'app/Label_Match.py'))
    monkeypatch.setattr(product, '_load_update_settings', lambda: {})
    monkeypatch.delenv(product.UPDATE_MANIFEST_PUBLIC_KEY_ENV, raising=False)
    with P256KeyPair.generate() as signer:
        host.write(tmp_path / 'app/config/app_settings.json', {'update_settings': {}})
        host._write_test_update_key_config(tmp_path, signer.public_jwk)
        manifest = canonical_manifest()
        manifest.update(signature_version='es256-v1', version='v2.0.125', source_commit='a' * 40,
                        data_compatible_min_version='v2.0.124')
        signature = signer.sign_es256(json.dumps(manifest, sort_keys=True,
            separators=(',', ':'), ensure_ascii=True).encode())
        identity = generation._verified_generation_compatibility(
            {'version': 'v2.0.125', 'build_id': 'a' * 40},
            {'data_generation_signed_manifest': manifest, 'data_generation_signature_hex': signature.hex()})
    assert identity['compatibility_verified'] is True
    assert identity['data_compatible_min_version'] == 'v2.0.124'


def test_host_private_compatibility_preserves_real_configured_key_priority(tmp_path, monkeypatch):
    import Label_Match as product
    import label_data_generation as generation
    from kmtech_zero_pe import P256KeyPair
    from tests.test_update_manifest import canonical_manifest
    monkeypatch.setattr(product, '__file__', str(tmp_path / 'app/Label_Match.py'))
    monkeypatch.delenv(product.UPDATE_MANIFEST_PUBLIC_KEY_ENV, raising=False)
    with P256KeyPair.generate() as stale, P256KeyPair.generate() as signer:
        settings = {'preserved_fixture_value': True, 'update_settings': {
            'channel': 'preserved', 'manifest_public_key': json.dumps(stale.public_jwk)}}
        host.write(tmp_path / 'app/config/app_settings.json', settings)
        host._write_test_update_key_config(tmp_path, signer.public_jwk)
        manifest = canonical_manifest()
        manifest.update(signature_version='es256-v1', version='v2.0.125', source_commit='a' * 40,
                        data_compatible_min_version='v2.0.124')
        signature = signer.sign_es256(json.dumps(manifest, sort_keys=True,
            separators=(',', ':'), ensure_ascii=True).encode())
        identity = generation._verified_generation_compatibility(
            {'version': 'v2.0.125', 'build_id': 'a' * 40},
            {'data_generation_signed_manifest': manifest, 'data_generation_signature_hex': signature.hex()})
    assert identity['compatibility_verified'] is True
    assert identity['data_compatible_min_version'] == 'v2.0.124'
    preserved = json.loads((tmp_path / 'app/config/app_settings.json').read_bytes())
    assert preserved['preserved_fixture_value'] is True
    assert preserved['update_settings']['channel'] == 'preserved'


@pytest.mark.parametrize("updated", [False, True])
def test_setup_launch_metadata_uses_the_code_selected_for_that_start(tmp_path, monkeypatch, updated):
    current = tmp_path / "machine/current"
    control = tmp_path / "machine/update-control"
    active = "b" * 40 if updated else "a" * 40
    host.write(current / "portable-manifest.json", {"source_commit": "a" * 40})
    host.write(control / "active.json", {"sha": active})
    selected = tmp_path / "machine/versions" / active
    (selected / "app").mkdir(parents=True)
    config = tmp_path / "config.json"
    host.write(config, {"current": str(current), "control": str(control)})
    monkeypatch.setattr(host, "_native_process_times", lambda _: {"creation_identity": 10000000})
    host._register_native_launch(config, "keep-relay", (), SimpleNamespace(pid=321))
    row = json.loads((tmp_path / "processes/321.json").read_bytes())
    expected = selected if updated else current
    assert row["ExecutablePath"] == str(expected / "runtime/pythonw.exe")
    assert str(expected / "app/label_match_relay_entry.py") in row["CommandLine"]
    assert str(expected) in row["CommandLine"]


def test_setup_root_does_not_substitute_current_for_a_missing_active_version(tmp_path):
    current = tmp_path / "machine/current"
    control = tmp_path / "machine/update-control"
    host.write(current / "portable-manifest.json", {"source_commit": "a" * 40})
    host.write(control / "active.json", {"sha": "b" * 40})
    value = {"current": str(current), "control": str(control)}
    assert host._setup_app(value) == tmp_path / "machine/versions" / ("b" * 40) / "app"


@pytest.mark.parametrize("failure,reason", [
    ("ordinary", "ORDINARY_CONTEXT_AND_PID"),
    ("build", "ACTIVE_CODE_BINDING"),
    ("claim", "RELAY_CLAIM_BINDING"),
    ("native", "SINGLE_NATIVE_RELAY_AND_CLAIM_TIME"),
])
def test_ready_observer_records_original_rejection_without_extra_state_reads(tmp_path, monkeypatch, failure, reason):
    import auto_update as updates
    from tests._first_setup_ready_observer import observed_receipt
    state = {"phase": "APPLIED_PENDING_READY", "candidate_sha": "a" * 40,
             "nonce": "fixture", "transition_id": "fixture", "applied_at": "2026-10-25T00:00:00Z"}
    claim = {"role": "relay", "pid": 321, "nonce": "fixture", "transition_id": "fixture"}
    calls = []

    def ordinary_user_context():
        calls.append("ordinary")
        return failure != "ordinary"

    def version_manifest(_path):
        calls.append("manifest")
        return {"source_commit": ("b" if failure == "build" else "a") * 40}

    def read_json(_path):
        calls.append("active")
        return {"sha": "a" * 40}

    def optional_record(path):
        calls.append(path.name)
        return state if path.name == "transition.json" else None if failure == "claim" else claim

    monkeypatch.setattr(updates, "ordinary_user_context", ordinary_user_context)
    monkeypatch.setattr(updates, "control_root", lambda _: tmp_path)
    monkeypatch.setattr(updates, "load_policy", lambda _: {"staging_root": str(tmp_path), "operator_sid": "fixture"})
    monkeypatch.setattr(updates, "version_manifest", version_manifest)
    monkeypatch.setattr(updates, "read_json", read_json)
    monkeypatch.setattr(updates, "optional_record", optional_record)
    monkeypatch.setattr(updates, "process_snapshot", lambda _: [])
    assert updates._relay_ready_receipt(tmp_path / "app", 321) is False
    unobserved_calls = list(calls)
    calls.clear()
    records = []
    observe = observed_receipt(updates._relay_ready_receipt, updates, records.append)
    assert observe(tmp_path / "app", 321) is False
    assert calls == unobserved_calls
    assert records[0]["outcome"] == "REJECTED"
    assert records[0]["rejections"][-1]["reason_code"] == reason
    assert all(type(value) is bool for value in records[0]["checks"].values())


@pytest.mark.parametrize("raises", [False, True])
def test_ready_observer_preserves_result_exception_and_existing_trace(monkeypatch, raises):
    import auto_update as updates
    from tests._first_setup_ready_observer import observed_receipt
    value = (Path("unwritten-ready.json"), {"unexported": object()})
    error = ValueError("fixture")
    calls, events, records = [], [], []

    def original():
        calls.append(True)
        if raises:
            raise error
        return value

    def previous(frame, event, argument):
        if frame.f_code is original.__code__:
            events.append(event)
        return None

    prior = sys.gettrace()
    sys.settrace(previous)
    try:
        observe = observed_receipt(original, updates, records.append)
        if raises:
            with pytest.raises(ValueError) as raised:
                observe()
            assert raised.value is error
        else:
            assert observe() is value
        assert sys.gettrace() is previous
    finally:
        sys.settrace(prior)
    assert calls == [True] and events == ["call"]
    assert len(records) == 1
    assert "unexported" not in json.dumps(records)
