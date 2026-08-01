import ast
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
INSTALLER_NAME = "Label_Match_Protected_Admin_Install.exe"
ACL_SCRIPT_NAME = "PROVISION_PROTECTED_ADMIN_ACL.ps1"
PROVISIONING_DOC_NAME = "PROTECTED_ADMIN_PROVISIONING.md"


def _app_version() -> str:
    tree = ast.parse((ROOT / "Label_Match.py").read_text(encoding="utf-8"))
    values = [
        node.value.value
        for node in ast.walk(tree)
        if isinstance(node, (ast.Assign, ast.AnnAssign))
        for target in (
            node.targets if isinstance(node, ast.Assign) else [node.target]
        )
        if isinstance(target, ast.Name)
        and target.id == "APP_VERSION"
        and isinstance(node.value, ast.Constant)
        and isinstance(node.value.value, str)
    ]
    assert len(values) == 1
    return values[0]


def test_release_workflow_contains_source_free_protected_admin_bundle() -> None:
    workflow = (ROOT / ".github" / "workflows" / "release.yml").read_text(
        encoding="utf-8"
    )
    assert f'--name "{INSTALLER_NAME.removesuffix(".exe")}" --onefile --console' in workflow
    assert "tools/install_protected_admin.py" in workflow
    assert f"dist/Label_Match/{INSTALLER_NAME} --help" in workflow
    assert f"dist/Label_Match/{INSTALLER_NAME} --dry-run" in workflow
    assert f"dist/Label_Match/{ACL_SCRIPT_NAME} -DryRun" in workflow
    assert "tools/provision_protected_admin_acl.ps1" in workflow
    assert "docs/PROTECTED_ADMIN_PROVISIONING.md" in workflow
    assert workflow.count(f'"Label_Match/{INSTALLER_NAME}"') == 1
    assert workflow.count(f'"Label_Match/{ACL_SCRIPT_NAME}"') == 1
    assert workflow.count(f'"Label_Match/{PROVISIONING_DOC_NAME}"') == 1
    internal_required = workflow[
        workflow.index("required = {") : workflow.index("files = sorted(")
    ]
    assert f'"{INSTALLER_NAME}"' in internal_required
    assert f'"{ACL_SCRIPT_NAME}"' in internal_required
    assert f'"{PROVISIONING_DOC_NAME}"' in internal_required
    assert "install_protected_admin.py" not in internal_required


def test_acl_wrapper_never_accepts_or_transports_the_protected_code() -> None:
    script = (ROOT / "tools" / "provision_protected_admin_acl.ps1").read_text(
        encoding="utf-8"
    )
    lowered = script.casefold()
    assert INSTALLER_NAME in script
    assert "--reader-principal" in script
    assert "--profile-path" in script
    assert "--dry-run" in script
    assert "start-transcript" not in lowered
    assert "protected_admin_code" not in lowered
    assert "--code" not in lowered
    parameter_block = script.split(")", 1)[0].casefold()
    assert all(
        marker not in parameter_block
        for marker in ("code", "credential", "password", "secret")
    )


def test_provisioning_document_and_version_contract_are_current() -> None:
    document = (ROOT / "docs" / "PROTECTED_ADMIN_PROVISIONING.md").read_text(
        encoding="utf-8"
    )
    assert INSTALLER_NAME in document
    assert ACL_SCRIPT_NAME in document
    assert "-DryRun" in document
    assert "-ReaderPrincipal" in document
    assert "명령행 인자" in document
    assert "환경 변수" in document
    assert "PowerShell transcript" in document
    assert _app_version() == "v2.0.56"
