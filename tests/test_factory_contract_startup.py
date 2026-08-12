import json
from pathlib import Path

import pytest

import Label_Match as app
from kmtech_factory_contracts import CONTRACT_BUNDLE_SHA256, FactoryContractError


ROOT = Path(__file__).resolve().parents[1]


def test_factory_contract_startup_accepts_synced_lock_and_bundle():
    lock = app.verify_factory_contract_startup()

    assert lock["app_id"] == "label_match"
    assert lock["contract_bundle_version"] == "1.0.3"
    assert lock["contract_bundle_sha256"] == CONTRACT_BUNDLE_SHA256


@pytest.mark.parametrize(
    ("field", "value", "error_code"),
    [
        ("contract_bundle_sha256", "0" * 64, "CONTRACT_HASH_MISMATCH"),
        ("contract_bundle_version", "999.0.0", "CONTRACT_VERSION_MISMATCH"),
    ],
)
def test_factory_contract_startup_rejects_hash_or_version_tamper(
    tmp_path,
    field,
    value,
    error_code,
):
    payload = json.loads((ROOT / "contract.lock.json").read_text(encoding="utf-8"))
    payload[field] = value
    tampered_lock = tmp_path / "contract.lock.json"
    tampered_lock.write_text(json.dumps(payload), encoding="utf-8")

    with pytest.raises(FactoryContractError) as caught:
        app.verify_factory_contract_startup(tampered_lock)

    assert caught.value.code == error_code


def test_main_fails_before_runtime_when_factory_contract_gate_fails(monkeypatch):
    failure = FactoryContractError("CONTRACT_TEST_FAILURE", "fixture rejection")

    def reject_contract():
        raise failure

    monkeypatch.setattr(app, "verify_factory_contract_startup", reject_contract)

    with pytest.raises(FactoryContractError) as caught:
        app.main()

    assert caught.value is failure


def test_frozen_release_verifier_requires_factory_contract_data():
    verifier = (ROOT / "tools" / "verify_frozen_release_assets.py").read_text(
        encoding="utf-8"
    )

    assert '"build-identity.json"' in verifier
    assert '"build-compatibility.json"' in verifier
    assert '"build-manifest.json"' in verifier
    assert '"contract.lock.json"' in verifier
    assert CONTRACT_BUNDLE_SHA256 in verifier
