from pathlib import Path

import pytest

from tools.classify_hosted_ui_scope import (
    is_process_only_path,
    requires_hosted_ui_evidence,
)


@pytest.mark.parametrize(
    "path",
    [
        ".github/workflows/ci.yml",
        "docs/verification.md",
        "tests/test_release_workflow.py",
        "RELEASE_GATE_CONTRACT.md",
        "README.txt",
        ".gitignore",
    ],
)
def test_explicit_process_only_paths_skip_hosted_ui_evidence(path):
    assert is_process_only_path(path) is True
    assert requires_hosted_ui_evidence([path]) is False


@pytest.mark.parametrize(
    "path",
    [
        "Label_Match.py",
        "protected_admin.py",
        "logistics_runtime_profile.py",
        "requirements-release.txt",
        "config/app_settings.json",
        "assets/logo.ico",
        "tools/capture_label_operator_ui.py",
        "tools/classify_hosted_ui_scope.py",
        "tests/test_label_operator_workbench.py",
        "unknown/new_runtime.py",
    ],
)
def test_runtime_dependency_and_unknown_paths_require_hosted_ui_evidence(path):
    assert is_process_only_path(path) is False
    assert requires_hosted_ui_evidence([path]) is True


def test_empty_or_mixed_change_sets_fail_closed():
    assert requires_hosted_ui_evidence([]) is True
    assert requires_hosted_ui_evidence(
        ["docs/verification.md", "unknown/new_runtime.py"]
    ) is True


def test_ci_workflow_uses_the_fail_closed_classifier():
    workflow = Path(".github/workflows/ci.yml").read_text(encoding="utf-8")
    assert "python -I -S tools/classify_hosted_ui_scope.py" in workflow
    assert "Hosted UI scope classifier failed closed." in workflow
    assert "Label_Match\\.py$|" not in workflow
