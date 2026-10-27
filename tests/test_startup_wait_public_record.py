"""Preserve the declared wait result while rejecting undeclared diagnostics."""
import json

import auto_update as updates


def test_wait_expiry_code_roundtrips_but_undeclared_code_is_withheld():
    accepted = json.loads(json.dumps(updates.support_record({
        "reason_code": "UPDATE_LOCK_WAIT_EXPIRED", "exit_code": 0,
    }, stage="update_apply")))
    rejected = updates.support_record({
        "reason_code": "UPDATE_LOCK_WAIT_EXPIRED_UNAPPROVED", "exit_code": 0,
    }, stage="update_apply")
    assert accepted["reason_code"] == "UPDATE_LOCK_WAIT_EXPIRED"
    assert "reason_code" not in rejected
    assert accepted["exit_code"] == rejected["exit_code"] == 0
