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


def test_authoritative_pyinstaller_build_includes_factory_contract_data():
    workflow = (ROOT / ".github" / "workflows" / "release.yml").read_text(
        encoding="utf-8"
    )

    assert (
        '--add-data "kmtech_factory_contracts/bundle;kmtech_factory_contracts/bundle"'
        in workflow
    )
    assert '--add-data "contract.lock.json;."' in workflow
    assert "kmtech_factory_contracts.build_cli prepare" in workflow
    assert '--add-data "build/factory_contract_identity/build-identity.json;."' in workflow
    assert '--add-data "build/factory_contract_identity/build-compatibility.json;."' in workflow
    assert "kmtech_factory_contracts.build_cli manifest" in workflow
    assert "kmtech_factory_contracts.build_cli verify" in workflow
