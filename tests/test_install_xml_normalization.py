"""Measured XML defaults and explicit variants share the official identity."""
import json
import xml.etree.ElementTree as ET

import pytest

from tests.test_r4_official_relay import FAMILIES, MANIFEST, NS, vector_input
from tests.test_r4_support import engine, fingerprint, run_driver


@pytest.mark.parametrize("family", sorted(FAMILIES))
@pytest.mark.parametrize("mode", ["install", "rollback"])
def test_measured_missing_elements_match_explicit_fingerprint(tmp_path, engine, family, mode):
    row = FAMILIES[family]
    payload = vector_input({"family": family, "mode": mode})
    tree = ET.fromstring(payload["tasks"][0]["xml"])
    principal = tree.find("t:Principals/t:Principal", NS)
    principal.remove(principal.find("t:RunLevel", NS))
    if row["identity"]["principal.user"] == "SYSTEM":
        principal.remove(principal.find("t:LogonType", NS))
    payload["tasks"][0]["xml"] = ET.tostring(tree, encoding="unicode")
    path = tmp_path / "input.json"
    path.write_text(json.dumps(payload), encoding="utf-8")
    record = run_driver(tmp_path, engine, "relay-driver.ps1", InputPath=path)
    full, fields = fingerprint(row["identity"])
    assert not record["reason_code"]
    assert record["identity_audit"]["full_fingerprint"] == full
    assert record["identity_audit"]["field_fingerprints"] == fields
    assert record["register_calls"] == record["delete_calls"] == 0
    assert record["old_enabled"] == [mode == "rollback"]
    assert not (any(record["old_enabled"]) and record["new_relay_enabled"])
    if mode == "rollback":
        assert record["transition"]["full_fingerprint"] == full


@pytest.mark.parametrize("case", ["user-logon-absent", "unknown-run-level", "unknown-logon", "duplicate-run-level"])
def test_unmeasured_or_unknown_enum_is_rejected_before_mutation(tmp_path, engine, case):
    family = next(k for k, row in FAMILIES.items() if row["identity"]["principal.logon_type"] == "Password")
    payload = vector_input({"family": family, "mode": "install"})
    tree = ET.fromstring(payload["tasks"][0]["xml"])
    principal = tree.find("t:Principals/t:Principal", NS)
    if case == "user-logon-absent":
        principal.remove(principal.find("t:LogonType", NS))
    elif case == "unknown-run-level":
        principal.find("t:RunLevel", NS).text = "UNKNOWN_LEVEL"
    elif case == "unknown-logon":
        principal.find("t:LogonType", NS).text = "UNKNOWN_LOGON"
    else:
        ET.SubElement(principal, "{" + NS["t"] + "}RunLevel").text = "LeastPrivilege"
    payload["tasks"][0]["xml"] = ET.tostring(tree, encoding="unicode")
    path = tmp_path / "input.json"
    path.write_text(json.dumps(payload), encoding="utf-8")
    record = run_driver(tmp_path, engine, "relay-driver.ps1", InputPath=path)
    assert record["reason_code"].startswith("Healthy lifecycle task belongs to another ")
    assert record["old_enabled"] == [True]
    assert not record["new_relay_enabled"]
    assert record["register_calls"] == record["delete_calls"] == 0
    assert not any(event in record["events"] for event in ("disable", "enable", "new_relay_start"))
    assert not record["transition"]["preimage_saved"]
