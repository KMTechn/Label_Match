"""Headless routing of an actual writer-fence error to the existing admin UI."""
import ast
import os
from pathlib import Path
import sys

import pytest

from current_user_onboarding import CurrentUserOnboardingError, ONBOARDING_EXIT_CODE
from label_data_generation import DataGenerationError
from writer_session_fence import WriterFenceError, WriterFencedError


@pytest.mark.parametrize("failure_stage", ["onboarding", "application"])
def test_startup_fence_error_keeps_app_stopped_and_explains_recovery(
    tmp_path, capsys, failure_stage,
):
    source = Path(__file__).parents[1] / "Label_Match.py"
    tree = ast.parse(source.read_text(encoding="utf-8"))
    selected = {
        "main", "_show_installation_recovery_required",
    }
    functions = [n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name in selected]
    assert len(functions) == len(selected)
    message = next(
        ast.literal_eval(n.value) for n in tree.body
        if isinstance(n, ast.Assign)
        and any(isinstance(t, ast.Name) and t.id == "INSTALLATION_RECOVERY_REQUIRED_MESSAGE" for t in n.targets)
    )
    exit_code = ONBOARDING_EXIT_CODE
    reports, traces, calls = [], [], []

    def denied():
        raise WriterFencedError("ACTIVE_WRITER_FENCE", "writer denied by the active deployment fence")

    def onboard(*args, **kwargs):
        calls.append("onboarding")
        if failure_stage == "onboarding":
            denied()
        return {"bootstrap_integrity": {"status": "PASS"}}

    def run_application(*args, **kwargs):
        calls.append("application")
        denied()

    namespace = {
        "os": os, "sys": sys,
        "WriterFenceError": WriterFenceError,
        "CurrentUserOnboardingError": CurrentUserOnboardingError,
        "DataGenerationError": DataGenerationError,
        "ONBOARDING_EXIT_CODE": exit_code,
        "INSTALLATION_RECOVERY_REQUIRED_MESSAGE": message,
        "dispatch_product_mode": lambda args: None,
        "verify_factory_contract_startup": lambda: None,
        "check_user_data_open": lambda root: None,
        "_label_match_startup_trace": lambda event, **kw: traces.append((event, kw)),
        "_first_run_onboarding_enabled": lambda: True,
        "_label_match_runtime_app_root": lambda: tmp_path,
        "onboard_current_user": onboard,
        "requires_bootstrap_integrity": lambda root: True,
        "LABEL_MATCH_DIRECT_SYNC_SERVER_BASE_URL_ENV": "LABEL_MATCH_DIRECT_SYNC_SERVER_BASE_URL",
        "LABEL_MATCH_DIRECT_SYNC_DEFAULT_SERVER_BASE_URL": "https://example.invalid",
        "_show_first_run_onboarding_error": reports.append,
        "_default_label_match_settings_path": lambda: tmp_path / "settings.json",
        "resolve_data_scope": lambda **kw: str(tmp_path),
        "resource_path": lambda path: path,
        "run_guarded_entrypoint": run_application,
        "_run_label_match_application": lambda: None,
    }
    exec(compile(ast.Module(body=functions, type_ignores=[]), str(source), "exec"), namespace)
    assert namespace["main"]([]) == exit_code
    assert calls == (["onboarding"] if failure_stage == "onboarding" else ["onboarding", "application"])
    assert len(reports) == 1
    assert reports[0].status == "BLOCKED"
    assert reports[0].cause_code == "INSTALLATION_RECOVERY_REQUIRED"
    assert "자료와 복구 기록을 보존" in str(reports[0])
    assert "관리자에게 지원 복구를 요청" in capsys.readouterr().err
    notice = [kw for event, kw in traces if event == "installation_recovery_required"]
    assert len(notice) == 1
    assert notice[0]["guard_code"] == "ACTIVE_WRITER_FENCE"
    assert notice[0]["user_message"] == message
    assert not list(tmp_path.iterdir())
