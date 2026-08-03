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
    classifier_start = workflow.index("- name: Classify hosted UI evidence scope")
    classifier_end = workflow.index("\n      - name:", classifier_start + 1)
    classifier_step = workflow[classifier_start:classifier_end]
    consumer_start = workflow.index(
        "- name: Run hosted UI retry geometry when UI changed"
    )
    consumer_end = workflow.find("\n      - name:", consumer_start + 1)
    consumer_step = workflow[
        consumer_start : consumer_end if consumer_end != -1 else len(workflow)
    ]

    assert "id: ui_scope" in classifier_step
    assert "actions: read" in workflow
    assert "BEFORE_SHA" not in workflow
    assert "actions/workflows/ci.yml/runs" in classifier_step
    assert "-f branch=main -f event=push -f status=success" in classifier_step
    assert "git merge-base --is-ancestor $candidate $env:GITHUB_SHA" in classifier_step
    assert "python -I -S tools/classify_hosted_ui_scope.py" in classifier_step
    assert '--baseline "$baseline"' in classifier_step
    assert '@("true", "false") -cnotcontains $required[0]' in classifier_step
    assert '"required=$($required[0])"' in classifier_step
    assert "$env:GITHUB_OUTPUT" in classifier_step
    assert "Hosted UI scope classifier failed closed." in classifier_step
    assert "if: steps.ui_scope.outputs.required == 'true'" in consumer_step
    assert "Label_Match\\.py$|" not in workflow
