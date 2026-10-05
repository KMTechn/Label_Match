"""Measured XML defaults and explicit variants share the official identity."""
import json
import xml.etree.ElementTree as ET

import pytest

from tests.test_r4_official_relay import FAMILIES, MANIFEST, NS, vector_input
from tests.test_r4_support import FIXTURES, engine, fingerprint, run_driver


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


@pytest.mark.parametrize("enabled", [True, False])
@pytest.mark.parametrize("mode", ["install", "rollback"])
def test_xml_enabled_absent_preserves_actual_scheduler_state(tmp_path, engine, enabled, mode):
    family = next(iter(FAMILIES))
    payload = vector_input({"family": family, "mode": mode})
    tree = ET.fromstring(payload["tasks"][0]["xml"])
    settings = tree.find("t:Settings", NS)
    settings.remove(settings.find("t:Enabled", NS))
    payload["tasks"][0].update(xml=ET.tostring(tree, encoding="unicode"), enabled=enabled)
    path = tmp_path / "input.json"
    path.write_text(json.dumps(payload), encoding="utf-8")
    record = run_driver(tmp_path, engine, "relay-driver.ps1", InputPath=path)
    full, fields = fingerprint(FAMILIES[family]["identity"])
    assert not record["reason_code"]
    assert record["identity_audit"]["full_fingerprint"] == full
    assert record["identity_audit"]["field_fingerprints"] == fields
    assert record["transition"]["preimage_enabled"] is enabled
    assert record["old_enabled"] == [enabled if mode == "rollback" else False]
    assert record["register_calls"] == record["delete_calls"] == 0
    assert not (any(record["old_enabled"]) and record["new_relay_enabled"])


@pytest.mark.parametrize("case", ["false-object-true-xml", "true-object-false-xml", "unknown-xml", "duplicate-xml"])
def test_explicit_enabled_inconsistency_is_rejected_before_mutation(tmp_path, engine, case):
    payload = vector_input({"family": next(iter(FAMILIES)), "mode": "install"})
    tree = ET.fromstring(payload["tasks"][0]["xml"])
    settings = tree.find("t:Settings", NS)
    node = settings.find("t:Enabled", NS)
    if case == "false-object-true-xml":
        payload["tasks"][0]["enabled"] = False
        node.text = "true"
    elif case == "true-object-false-xml":
        node.text = "false"
    elif case == "unknown-xml":
        node.text = "UNKNOWN"
    else:
        ET.SubElement(settings, "{" + NS["t"] + "}Enabled").text = "true"
    payload["tasks"][0]["xml"] = ET.tostring(tree, encoding="unicode")
    path = tmp_path / "input.json"
    path.write_text(json.dumps(payload), encoding="utf-8")
    record = run_driver(tmp_path, engine, "relay-driver.ps1", InputPath=path)
    assert record["reason_code"].startswith("Healthy lifecycle task belongs to another ")
    assert not record["transition"]["preimage_saved"]
    assert not any(event in record["events"] for event in ("disable", "enable", "new_relay_start"))
    assert record["register_calls"] == record["delete_calls"] == 0


@pytest.mark.parametrize("state", ["$null", "'false'", "0", "[pscustomobject]@{}"])
def test_unreadable_or_non_boolean_scheduler_state_is_rejected(tmp_path, engine, state):
    payload = vector_input({"family": next(iter(FAMILIES)), "mode": "install"})
    path = tmp_path / "input.json"
    path.write_text(json.dumps(payload), encoding="utf-8")
    # Reuse the isolated scheduler seam before its test body, then inject an
    # invalid object value without its input adapter's boolean coercion.
    prefix = (FIXTURES / "relay-driver.ps1").read_text(encoding="utf-8-sig").split("$reason='';")[0]
    driver = tmp_path / "invalid-enabled.ps1"
    driver.write_text(prefix + "\n$script:tasks[0].Settings.Enabled=" + state + "\n"
                      "$reason='';try {Get-LabelOfficialLegacyTaskSnapshot | Out-Null} catch {$reason=$_.Exception.Message}\n"
                      "[ordered]@{reason_code=$reason;events=$script:events.ToArray();register_calls=$script:registerCalls;"
                      "delete_calls=$script:deleteCalls} | ConvertTo-Json | Set-Content -LiteralPath $Output -Encoding UTF8\n",
                      encoding="utf-8-sig")
    record = run_driver(tmp_path, engine, driver, InputPath=path)
    assert record["reason_code"].startswith("Healthy lifecycle task belongs to another ")
    assert not any(event in record["events"] for event in ("disable", "enable", "new_relay_start"))
    assert record["register_calls"] == record["delete_calls"] == 0
