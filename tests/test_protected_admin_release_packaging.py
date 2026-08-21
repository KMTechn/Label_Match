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


def test_frozen_release_verifier_requires_source_free_protected_admin_bundle() -> None:
    verifier = (ROOT / "tools" / "verify_frozen_release_assets.py").read_text(
        encoding="utf-8"
    )
    assert f'"{INSTALLER_NAME}"' in verifier
    assert f'"{ACL_SCRIPT_NAME}"' in verifier
    assert f'"{PROVISIONING_DOC_NAME}"' in verifier
    assert "install_protected_admin.py" not in verifier


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
    assert _app_version() == "v2.0.74"


def test_release_records_hosted_ci_factually_without_recompiling_imported_modules() -> None:
    release = (ROOT / ".github" / "workflows" / "release.yml").read_text(
        encoding="utf-8"
    )
    ci = (ROOT / ".github" / "workflows" / "ci.yml").read_text(encoding="utf-8")
    lease_tests = (ROOT / "tests" / "test_terminal_operation_lease.py").read_text(
        encoding="utf-8"
    )

    assert "py_compile" not in release
    assert "py_compile" not in ci
    assert "Record exact-SHA Hosted CI status without making it a release gate" in release
    assert "hosted_ci=PASS_NON_GATING" in release
    assert release.count("hosted_ci=WAIVED_NOT_TESTED") == 2
    assert "Require successful exact-SHA main Full CI" not in release
    assert "python -m pytest -q --deselect" in ci
    assert "from terminal_operation_lease import" in lease_tests
