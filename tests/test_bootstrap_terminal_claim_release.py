"""Review578: terminal publication and SYSTEM claim settlement are separate."""
import json

import pytest

from tests.test_bootstrap_apply_decision_wait import _decision_world


@pytest.mark.parametrize("root_busy", [True, False], ids=["busy", "available"])
@pytest.mark.parametrize("claim", ["LIVE", "UNKNOWN"])
def test_terminal_waits_for_claim_settlement_under_original_extension(tmp_path, root_busy, claim):
    facts = _decision_world(root_busy=root_busy, terminal_claim=claim)
    (tmp_path / "terminal-claim-release-proof.json").write_text(json.dumps(facts), encoding="utf-8")
    assert facts["terminal_claim_reads"][0]["status"] == claim
    assert facts["claim_settled"][0] > facts["terminal_claim_reads"][0]["at"]
    assert facts["returncode"] == 0 and facts["ordinary_popen"] == 1 and facts["trial_popen"] == 0
    assert facts["support_reason"] == "UPDATE_TERMINAL_RECOVERED"
    assert facts["starts"] == [dict(code="UPDATE_TERMINAL_RECOVERED", owned=True)]
    assert facts["retained"] == [dict(owned=True)]
    assert any(row["status"] == "NONE" for row in facts["terminal_claim_reads"])
