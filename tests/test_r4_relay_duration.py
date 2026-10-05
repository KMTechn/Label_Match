"""SCHEMA duration equivalence at the real product parser; scheduler doubles only."""
import json
import xml.etree.ElementTree as ET

import pytest

from tests.test_r4_official_relay import FAMILIES, FIELD_PATHS, NS, TABLE, vector_input
from tests.test_r4_support import engine, fingerprint, run_driver


@pytest.mark.parametrize("interval,equivalent", [("PT1M", True), ("PT60S", True), ("PT0H1M0S", True), ("PT2M", False), ("invalid", False)])
def test_legacy_relay_duration_is_normalized_without_changing_identity(tmp_path, engine, interval, equivalent):
    vector = next(row for row in TABLE["vectors"] if row["mode"] == "install" and row["proceed"] and not row.get("mutation") and FAMILIES[row["family"]]["identity"]["trigger.repetition_interval"] == "PT1M")
    payload = vector_input(vector)
    tree = ET.fromstring(payload["tasks"][0]["xml"])
    tree.find(FIELD_PATHS["trigger.repetition_interval"], NS).text = interval
    payload["tasks"][0]["xml"] = ET.tostring(tree, encoding="unicode")
    input_path = tmp_path / "input.json"
    input_path.write_text(json.dumps(payload), encoding="utf-8")
    record = run_driver(tmp_path, engine, "relay-driver.ps1", InputPath=input_path)
    assert record["seam_available"]
    assert record["register_calls"] == record["delete_calls"] == 0
    if equivalent:
        full, fields = fingerprint(FAMILIES[vector["family"]]["identity"])
        assert not record["reason_code"]
        assert record["identity_audit"]["full_fingerprint"] == full
        assert record["identity_audit"]["field_fingerprints"] == fields
        assert record["old_enabled"] == [False] and record["new_relay_enabled"]
    else:
        assert record["reason_code"].startswith("Healthy lifecycle task belongs to another ")
        assert not record["transition"]["preimage_saved"]
        assert record["old_enabled"] == [True] and not record["new_relay_enabled"]
        assert not any(event in record["events"] for event in ("disable", "enable", "new_relay_start"))
