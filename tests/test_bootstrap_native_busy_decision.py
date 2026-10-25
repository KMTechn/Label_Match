"""The actual native mutex wait returns to the extended bootstrap guard."""
import json

import pytest

from tests.test_bootstrap_apply_decision_wait import _decision_world


@pytest.mark.parametrize("claim", ["UNKNOWN", "ABSENT"])
def test_native_busy_returns_to_live_claim_check(tmp_path, claim):
    facts = _decision_world(decision_at=None, boundary_at=256, boundary_claim=claim, native_busy=True)
    (tmp_path / "native-busy-proof.json").write_text(json.dumps(facts), encoding="utf-8")
    assert facts["native_waits"] and any(row["busy"] for row in facts["native_waits"])
    assert all(row["milliseconds"] == 5000 for row in facts["native_waits"])
    assert 256 <= facts["exit_clock"] < 262
    assert facts["returncode"] == 20 and facts["error_type"] is None
    assert facts["ordinary_popen"] == facts["trial_popen"] == 0
