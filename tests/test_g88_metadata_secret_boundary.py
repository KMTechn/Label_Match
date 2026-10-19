"""G88: a secret-shaped process variable must not block unrelated machine metadata writes.

All values are synthetic and created at run time; none is printed.
"""
import importlib
import json
import uuid

import pytest

NAME = "G88_METADATA_TOKEN"
REFUSED = "^PRODUCT_METADATA_SECRET_VALUE_REFUSED$"


@pytest.mark.parametrize("length", [1, 2, 3])
def test_g88_short_environment_value_does_not_block_ready_metadata_overwrite(tmp_path, monkeypatch, length):
    import auto_update
    monkeypatch.setenv(NAME, "1234"[:length])
    target = tmp_path / "transition.json"
    target.write_text(json.dumps({"status": "READY"}), encoding="utf-8")
    payload = {"status": "READY", "enabled": True, "disabled": False, "optional": None,
               "count": True.real, "build": "1234" * 2}
    auto_update.write_json_durable(target, payload)
    assert json.loads(target.read_text(encoding="utf-8")) == payload


def test_g88_supplied_environment_does_not_read_process_environment(monkeypatch):
    import failure_redaction
    marker = uuid.uuid4().hex
    monkeypatch.setenv(NAME, marker)
    failure_redaction.assert_machine_metadata_text(json.dumps({"data_root": marker}), {})


def test_g88_real_secret_does_not_replace_ready_metadata(tmp_path, monkeypatch):
    import auto_update
    marker = uuid.uuid4().hex
    monkeypatch.setenv(NAME, marker)
    folder = tmp_path / "metadata"
    folder.mkdir()
    target = folder / "transition.json"
    before = json.dumps({"status": "READY"}).encode()
    target.write_bytes(before)
    with pytest.raises(ValueError, match=REFUSED):
        auto_update.write_json_durable(target, {"status": marker})
    assert target.read_bytes() == before
    assert [path.name for path in folder.iterdir()] == [target.name]


def test_g88_complete_numeric_secret_of_four_or_more_digits_is_refused():
    import failure_redaction
    numeric = str(10 ** 5 + uuid.uuid4().int % (10 ** 5))
    with pytest.raises(ValueError, match=REFUSED):
        failure_redaction.assert_machine_metadata_text(json.dumps({"value": numeric}), {"G88_ADMIN_PIN": numeric})


def test_g88_registration_credential_writer_is_outside_metadata_guard(tmp_path, monkeypatch):
    from tools.register_label_match_worker_pc import _write_json as write_credential
    marker = uuid.uuid4().hex
    monkeypatch.setenv(NAME, marker)
    target = tmp_path / "credential.json"
    target.write_text(json.dumps({"status": "READY"}), encoding="utf-8")
    payload = {"api_token": marker, "schema_version": True.real}
    write_credential(target, payload)
    assert json.loads(target.read_text(encoding="utf-8")) == payload


WRITERS = [
    ("auto_update", "write_json_durable"),
    ("current_user_onboarding", "_write_machine_json_atomic"),
    ("user_relay", "_write_machine_json_atomic"),
]


@pytest.mark.parametrize("module_name,function_name", WRITERS, ids=lambda value: value)
def test_g88_every_metadata_writer_still_refuses_a_process_environment_secret(
        tmp_path, monkeypatch, module_name, function_name):
    """Omitting the environment means empty inside the check, so each writer must pass its own."""
    module = importlib.import_module(module_name)
    writer = getattr(module, function_name)
    implementation = getattr(writer, "__wrapped__", writer)
    synthetic = "synthetic-only-machine-secret-" + uuid.uuid4().hex
    monkeypatch.setenv("CUSTOM_MACHINE_SECRET", synthetic)
    target = tmp_path / "missing-parent" / "machine.json"
    with pytest.raises(ValueError, match=REFUSED):
        implementation(target, {"value": synthetic})
    assert not target.parent.exists()
    assert not list(tmp_path.rglob("*tmp*"))


def test_g88_settings_writer_resolves_a_missing_environment_to_the_process_one(tmp_path, monkeypatch):
    import current_user_onboarding as module
    synthetic = "synthetic-only-machine-secret-" + uuid.uuid4().hex
    monkeypatch.setenv("CUSTOM_MACHINE_SECRET", synthetic)
    with pytest.raises(ValueError, match=REFUSED):
        module._write_machine_json_atomic(tmp_path / "settings.json", {"save_path": synthetic})
    monkeypatch.setenv("CUSTOM_MACHINE_SECRET", "s3!")
    module._write_machine_json_atomic(tmp_path / "settings.json", {"save_path": "s3!"})
    assert json.loads((tmp_path / "settings.json").read_text(encoding="utf-8")) == {"save_path": "s3!"}
