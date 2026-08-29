import json
from pathlib import Path

from current_user_scheduled_task import (
    LEGACY_TASK_NAME,
    evaluate_legacy_task_quiescence,
)
from tools import label_legacy_task_quiescence as gate


def test_cli_writes_pass_for_absent_legacy_task(tmp_path: Path):
    report_path = (tmp_path / "absent.json").resolve()
    report = evaluate_legacy_task_quiescence(
        {"exists": False, "name": LEGACY_TASK_NAME}
    )

    exit_code = gate.main(
        ["--report-path", str(report_path)], reader=lambda: report
    )

    written = json.loads(report_path.read_text(encoding="utf-8"))
    assert exit_code == 0
    assert written["status"] == "PASS"
    assert written["reason_code"] == "LEGACY_TASK_ABSENT"


def test_cli_negative_control_writes_actionable_fail_for_present_enabled_task(
    tmp_path: Path,
):
    report_path = (tmp_path / "present-enabled.json").resolve()
    report = evaluate_legacy_task_quiescence(
        {
            "exists": True,
            "name": LEGACY_TASK_NAME,
            "state": "Ready",
            "principal_user_id": "SYSTEM",
            "principal_logon_type": "ServiceAccount",
            "principal_run_level": "Highest",
        }
    )

    exit_code = gate.main(
        ["--report-path", str(report_path)], reader=lambda: report
    )

    written = json.loads(report_path.read_text(encoding="utf-8"))
    assert exit_code == 4
    assert written["status"] == "FAIL"
    assert written["reason_code"] == "LEGACY_TASK_PRESENT_ENABLED"
    assert "Disable or remove" in written["remediation"]
    assert r"\direct-sync-relay-label-match-current-pc" in written["remediation"]
    assert written["task_or_process_mutated"] is False
