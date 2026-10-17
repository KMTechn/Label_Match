"""Focused reproductions for the L3 audit scenarios; no installation or GUI."""
from pathlib import Path

import pytest

import current_user_onboarding as onboarding
from tests.test_current_user_onboarding import (
    _environment,
    _write_bootstrap_inventory_record,
    _write_bootstrap_root_record,
)
from tests.test_shared_ancestor_protection import run_functions


ROOT = Path(__file__).resolve().parents[1]


@pytest.mark.parametrize("engine", ("powershell", "pwsh"))
@pytest.mark.parametrize("representation", ("dictionary", "object"))
def test_g31_strictmode_old_child_failure_omits_optional_diagnostic_fields(
    tmp_path, engine, representation
):
    # Get-LabelLaunchFailure accepts old placement records without the optional
    # location/report fields (aedcbca; INSTALL_CANONICAL_PORTABLE.ps1:3952-3966).
    redaction = str(ROOT / "tools/failure_redaction.ps1").replace("'", "''")
    script = ". '" + redaction + "'\n" + r"""
Set-StrictMode -Version Latest
$ErrorActionPreference='Stop'
$old=@{placement_code='INSTALLATION_FAILED'}
if($env:LM_G31_REPRESENTATION -ceq 'object'){$old=[pscustomobject]$old}
$failure=[InvalidOperationException]::new('old child failure fixture')
$failure.Data['placement_child_failure']=$old
try { throw $failure }
catch { $public=Get-LabelLaunchFailure $_ 'placement' }
if($public.status -cne 'FAILED' -or $public.reason_code -cne 'INSTALLER_FAILED'){
    throw 'Old child failure did not preserve the public failure.'
}
if([string]::IsNullOrWhiteSpace([string]$public.guidance)){
    throw 'Old child failure lost recovery guidance.'
}
"""
    result = run_functions(
        tmp_path, ROOT / "INSTALL_CANONICAL_PORTABLE.ps1",
        ["Get-LabelLaunchFailure"], script, engine=engine,
        values={"LM_G31_REPRESENTATION": representation},
    )
    assert result.returncode == 0, result.stdout + result.stderr


@pytest.mark.parametrize("record_format", ("inventory", "root"))
@pytest.mark.parametrize("mutation", ("ancestor_sibling", "code_content"))
def test_g44_bootstrap_check_distinguishes_ancestor_activity_from_code_change(
    tmp_path, monkeypatch, record_format, mutation
):
    # G44's event occurs during the actual startup integrity hash, rather than
    # before it. A sibling outside the install root must not invalidate code.
    parent = tmp_path / "install-parent"
    app_root = parent / "installation"
    (app_root / "runtime").mkdir(parents=True)
    (app_root / "app").mkdir()
    if record_format == "root":
        (app_root / "_internal").mkdir()
        (app_root / "Label_Match.exe").write_bytes(b"fixture executable")
    runtime = app_root / "runtime/pythonw.exe"
    runtime.write_bytes(b"fixture runtime")
    (app_root / "app/main.py").write_text("pass\n", encoding="utf-8")
    writer = (_write_bootstrap_inventory_record if record_format == "inventory"
              else _write_bootstrap_root_record)
    record = writer(app_root)
    paths = onboarding.resolve_current_user_onboarding_paths(
        app_root, environ=_environment(tmp_path)
    )
    preserved = {path: path.read_bytes() for path in app_root.rglob("*") if path.is_file()}
    original_hash = onboarding._file_sha256
    injected = []

    def mutate_during_hash(path):
        if not injected:
            injected.append(path)
            if mutation == "ancestor_sibling":
                (parent / "concurrent-sibling").mkdir()
            else:
                path.write_bytes(path.read_bytes() + b" changed during startup")
        return original_hash(path)

    monkeypatch.setattr(onboarding, "_file_sha256", mutate_during_hash)
    if mutation == "code_content":
        with pytest.raises(ValueError):
            onboarding.verify_bootstrap_integrity(paths, required=True)
        assert injected and record.read_bytes() == preserved[record]
    else:
        result = onboarding.verify_bootstrap_integrity(paths, required=True)
        assert result["status"] == "PASS"
        assert injected and (parent / "concurrent-sibling").is_dir()
        assert all(path.read_bytes() == raw for path, raw in preserved.items())
