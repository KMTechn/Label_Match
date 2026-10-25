"""Review569: terminal decisions retain their protected startup paths."""
import json

import pytest

from tests.test_bootstrap_apply_decision_wait import _decision_world


@pytest.mark.parametrize("root_busy", [True, False], ids=["busy", "available"])
def test_extended_terminal_preserves_support_and_retained_ownership(tmp_path, root_busy):
    facts = _decision_world(root_busy=root_busy)
    (tmp_path / "terminal-decision-proof.json").write_text(json.dumps(facts), encoding="utf-8")
    assert facts["returncode"] == 0 and facts["ordinary_popen"] == 1 and facts["trial_popen"] == 0
    assert facts["support_reason"] == "UPDATE_TERMINAL_RECOVERED"
    assert facts["starts"] == [dict(code="UPDATE_TERMINAL_RECOVERED", owned=True)]
    assert facts["retained"] == [dict(owned=True)]


def test_extended_terminal_reuses_the_existing_relay_without_popen(tmp_path):
    facts = _decision_world(existing_relay=True)
    (tmp_path / "terminal-relay-reuse-proof.json").write_text(json.dumps(facts), encoding="utf-8")
    assert facts["returncode"] == 0 and facts["error_type"] is None
    assert facts["ordinary_popen"] == facts["trial_popen"] == 0
    assert facts["remaining_pids"] == [7001]
    assert facts["starts"] == [dict(code="UPDATE_TERMINAL_RECOVERED", owned=True)]
    assert facts["retained"] == [dict(owned=True)]
    assert any(row["owned"] and row.get("result") == 0 for row in facts["recovery"])
