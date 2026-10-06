import importlib.util
import json
from pathlib import Path
import sys

import pytest
from tests._shared_portable_fixture import shared_bootstrap_files


MODULE_PATH = (
    Path(__file__).resolve().parents[1]
    / "tools"
    / "verify_staged_release_installer.py"
)
SPEC = importlib.util.spec_from_file_location(
    "verify_staged_release_installer_for_tests", MODULE_PATH
)
assert SPEC and SPEC.loader
verifier = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = verifier
SPEC.loader.exec_module(verifier)


def _package(tmp_path: Path) -> Path:
    root = tmp_path / "Label_Match"
    internal = root / "_internal" / "config"
    internal.mkdir(parents=True)
    installer_source = Path(__file__).resolve().parents[1] / "INSTALL_THIS_PC.ps1"
    helper_source = (
        Path(__file__).resolve().parents[1] / "tools" / "bootstrap_integrity.ps1"
    )
    (root / "INSTALL_THIS_PC.ps1").write_text(
        installer_source.read_text(encoding="utf-8-sig"),
        encoding="utf-8",
    )
    (root / "tools").mkdir()
    (root / "tools" / "bootstrap_integrity.ps1").write_text(
        helper_source.read_text(encoding="utf-8-sig"),
        encoding="utf-8",
    )
    (root / "Label_Match.exe").write_bytes(b"onedir product host")
    (root / "contract.lock.json").write_text("{}\n", encoding="utf-8")
    (internal / "app_settings.json").write_text("{}\n", encoding="utf-8")
    (root / "_internal" / "python312.dll").write_bytes(b"runtime")
    for name, data in shared_bootstrap_files().items():
        target = root / name
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(data)
    return root


def test_verifier_proves_code_only_current_user_topology(tmp_path):
    package = _package(tmp_path)

    report = verifier.verify_staged_package(package)

    assert report["status"] == "PASS"
    assert report["schema_version"] == verifier.REPORT_SCHEMA
    assert report["runtime_host"]["package_layout"] == "onedir"
    assert report["runtime_host"]["relay_execution_boundary"] == "product_host"
    assert report["bootstrap_contract"]["elevation_points"] == ["code_placement"]
    assert (
        report["bootstrap_contract"]["elevation_authority"]
        == "canonical_pinned_in_memory_caller"
    )
    assert report["bootstrap_contract"]["identity_profile_created"] is False
    assert report["state_contract"]["relay_persistence"] == "HKCU_RUN"
    assert report["state_contract"]["relay_port_contract"] == 18456
    assert report["state_contract"]["source_host_override_required"] is False
    assert report["legacy_authority_contract"]["system_scheduled_task_supported"] is False
    assert report["legacy_authority_contract"]["forbidden_package_members_absent"] is True
    assert report["system_python_required"] is False
    assert report["original_package_unchanged"] is True


def test_verifier_refuses_inline_task_command_in_integrity_helper(tmp_path):
    package = _package(tmp_path)
    helper = package / 'tools/bootstrap_integrity.ps1'
    with helper.open('a', encoding='utf-8') as stream:
        stream.write("\nif ($false) { Register-ScheduledTask -TaskName 'unexpected' }\n")
    with pytest.raises(verifier.StagedInstallerVerificationError) as caught:
        verifier._assert_bootstrap_source(package / 'INSTALL_THIS_PC.ps1', helper)
    assert caught.value.legacy_authority_contract['task_creation_tokens_absent'] is False


@pytest.mark.parametrize("location", ("helper", "installer"))
@pytest.mark.parametrize("case,command", (
    ("definition_only", "# Register-ScheduledTask is a prohibited operation\n"),
    ("direct", "Register-ScheduledTask -TaskName ReviewOnly -Xml $xml\n"),
    ("quoted_call", "& 'Register-ScheduledTask' -TaskName ReviewOnly -Xml $xml\n"),
    ("module_qualified", "ScheduledTasks\\Register-ScheduledTask -TaskName ReviewOnly -Xml $xml\n"),
    ("variable_call", "$reviewCommand = 'Register-ScheduledTask'; & $reviewCommand -TaskName ReviewOnly -Xml $xml\n"),
    ("expression_call", "& ('Register-' + 'ScheduledTask') -TaskName ReviewOnly -Xml $xml\n"),
    ("nested_direct", "function ReviewOnly { Register-ScheduledTask -TaskName ReviewOnly -Xml $xml }\n"),
    ("module_qualified_quoted", "& 'ScheduledTasks\\Register-ScheduledTask' -TaskName ReviewOnly -Xml $xml\n"),
    ("alias", "Set-Alias -Name ReviewRegistration -Value Register-ScheduledTask; ReviewRegistration -TaskName ReviewOnly -Xml $xml\n"),
    ("direct_splat", '$reviewParameters=@{TaskName="ReviewOnly";Xml=$xml}; Register-ScheduledTask @reviewParameters\n'),
    ("module_splat", '$reviewParameters=@{TaskName="ReviewOnly";Xml=$xml}; ScheduledTasks\\Register-ScheduledTask @reviewParameters\n'),
    ("escaped_direct", "Register-Schedu`ledTask -TaskName ReviewOnly -Xml $xml\n"),
    ("child_command_literal", 'powershell.exe -NoProfile -Command "Register-ScheduledTask -TaskName ReviewOnly -Xml $xml"\n'),
    ("invoke_expression", 'Invoke-Expression "Register-ScheduledTask -TaskName ReviewOnly -Xml $xml"\n'),
))
def test_bootstrap_source_task_command_variants(tmp_path, location, case, command):
    # Parse source only: none of these task-creation commands is executed.
    package = _package(tmp_path)
    installer = package / "INSTALL_THIS_PC.ps1"
    helper = package / "tools/bootstrap_integrity.ps1"
    target = helper if location == "helper" else installer
    with target.open("a", encoding="utf-8") as stream:
        stream.write("\n" + command)
    if case == "definition_only":
        verifier._assert_bootstrap_source(installer, helper)
    else:
        with pytest.raises(verifier.StagedInstallerVerificationError,
                           match="retired enrollment/task authority") as caught:
            verifier._assert_bootstrap_source(installer, helper)
        assert caught.value.legacy_authority_contract["task_creation_tokens_absent"] is False


@pytest.mark.parametrize("location", ("helper", "installer"))
def test_bootstrap_source_allows_task_name_definition_strings(tmp_path, location):
    package = _package(tmp_path)
    installer = package / "INSTALL_THIS_PC.ps1"
    helper = package / "tools/bootstrap_integrity.ps1"
    target = helper if location == "helper" else installer
    with target.open("a", encoding="utf-8") as stream:
        stream.write("\n$documentation = 'Register-ScheduledTask'; $example = 'Invoke-Expression'\n")
    verifier._assert_bootstrap_source(installer, helper)


@pytest.mark.parametrize("command", (
    "$reviewCommand = 'Register-ScheduledTask'; . $reviewCommand -TaskName ReviewOnly",
    "New-Alias ReviewRegistration Register-ScheduledTask; ReviewRegistration -TaskName ReviewOnly",
    "sal ReviewRegistration Register-ScheduledTask; ReviewRegistration -TaskName ReviewOnly",
    "iex 'Register-ScheduledTask -TaskName ReviewOnly'",
    "Microsoft.PowerShell.Utility\\Invoke-Expression 'Register-ScheduledTask -TaskName ReviewOnly'",
    "pwsh -NoProfile -Command 'Register-ScheduledTask -TaskName ReviewOnly'",
    "& 'ScheduledTasks\\REGISTER-SCHEDULEDTASK' -TaskName ReviewOnly",
    "& { Register-ScheduledTask -TaskName ReviewOnly }",
))
def test_bootstrap_source_refuses_execution_alias_family(tmp_path, command):
    package = _package(tmp_path)
    installer = package / "INSTALL_THIS_PC.ps1"
    helper = package / "tools/bootstrap_integrity.ps1"
    with helper.open("a", encoding="utf-8") as stream:
        stream.write("\n" + command + "\n")
    with pytest.raises(verifier.StagedInstallerVerificationError,
                       match="retired enrollment/task authority") as caught:
        verifier._assert_bootstrap_source(installer, helper)
    assert caught.value.legacy_authority_contract["task_creation_tokens_absent"] is False


@pytest.mark.parametrize("before,after", (
    ("$BootstrapIntegrityFunctions = Join-Path $PSScriptRoot \"tools\\bootstrap_integrity.ps1\"",
     "$BootstrapIntegrityFunctions = 'Register-ScheduledTask'"),
    ("$icacls = Join-Path ([Environment]::SystemDirectory) 'icacls.exe'",
     "$icacls = 'Register-ScheduledTask'"),
    ("$runtime = Join-Path $Root 'runtime\\python.exe'",
     "$runtime = 'powershell.exe'"),
    ("$implementation = Join-Path $Root 'app\\auto_update.py'",
     "$implementation = '-Command Register-ScheduledTask'"),
    ("$robocopy = Join-Path ([Environment]::SystemDirectory) 'robocopy.exe'",
     "$robocopy = 'Register-ScheduledTask'"),
    ("try { & $Action }", "try { $local:Action = { Register-ScheduledTask }; & $Action }"),
    ("& $icacls @ownerArgs", "$local:icacls = 'Register-ScheduledTask'; & $icacls @ownerArgs"),
    ("& $icacls @ownerArgs", "& $icacls @ownerArgs; & $icacls @ownerArgs"),
    ("([scriptblock]$Action)", "([string]$Action)"),
    ("Invoke-LabelStartupUpdateTaskMutation {", "Invoke-LabelStartupUpdateTaskMutation $unresolved; & {"),
))
def test_bootstrap_source_binds_existing_indirect_targets(tmp_path, before, after):
    package = _package(tmp_path)
    installer = package / "INSTALL_THIS_PC.ps1"
    helper = package / "tools/bootstrap_integrity.ps1"
    source = installer.read_text(encoding="utf-8")
    assert source.count(before) >= 1
    # Preserve line numbers, so the existing two-registration guard cannot
    # account for rejection of an indirect binding/count/delegation change.
    changed = source.replace(before, after, 1)
    assert changed.count("\n") == source.count("\n")
    installer.write_text(changed, encoding="utf-8")
    with pytest.raises(verifier.StagedInstallerVerificationError,
                       match="retired enrollment/task authority") as caught:
        verifier._assert_bootstrap_source(installer, helper)
    assert caught.value.legacy_authority_contract["task_creation_tokens_absent"] is False


def _archive_consumer():
    path = MODULE_PATH.with_name("build_release_archive.py")
    spec = importlib.util.spec_from_file_location("staged_archive_consumer_for_tests", path)
    assert spec and spec.loader
    consumer = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = consumer
    spec.loader.exec_module(consumer)
    return consumer


def _write_staged_report(package, report):
    (package / "staged-installer-verification.json").write_text(
        json.dumps(report, indent=2) + "\n", encoding="utf-8"
    )


def test_real_staged_producer_report_is_accepted_by_archive_consumer(tmp_path):
    package = _package(tmp_path)
    report = verifier.verify_staged_package(package)
    _write_staged_report(package, report)
    consumer = _archive_consumer()
    consumer._validate_staged_installer(package)


@pytest.mark.parametrize("damage", ("missing", "wrong"))
def test_archive_consumer_refuses_missing_or_wrong_elevation_authority(tmp_path, damage):
    package = _package(tmp_path)
    report = verifier.verify_staged_package(package)
    if damage == "missing":
        del report["bootstrap_contract"]["elevation_authority"]
    else:
        report["bootstrap_contract"]["elevation_authority"] = "unreviewed"
    _write_staged_report(package, report)
    consumer = _archive_consumer()
    with pytest.raises(consumer.ReleaseArchiveError, match="code-only bootstrap contract"):
        consumer._validate_staged_installer(package)


@pytest.mark.parametrize("damage", ("third-call", "changed-arguments"))
def test_invalid_task_creation_is_false_and_refused_by_archive_consumer(tmp_path, damage):
    package = _package(tmp_path)
    report = verifier.verify_staged_package(package)
    installer = package / "INSTALL_THIS_PC.ps1"
    source = installer.read_text(encoding="utf-8")
    if damage == "third-call":
        source += "\nRegister-ScheduledTask -TaskName bad\n"
    else:
        source = source.replace(
            "Register-ScheduledTask -TaskName $spec.name -Xml $spec.xml -Force",
            "Register-ScheduledTask -TaskName $spec.name -Xml $spec.xml -Force -User bad", 1
        )
    installer.write_text(source, encoding="utf-8")
    with pytest.raises(verifier.StagedInstallerVerificationError,
                       match="retired enrollment/task authority") as caught:
        verifier.verify_staged_package(package)
    assert caught.value.legacy_authority_contract["task_creation_tokens_absent"] is False
    report["legacy_authority_contract"].update(caught.value.legacy_authority_contract)
    _write_staged_report(package, report)
    consumer = _archive_consumer()
    with pytest.raises(consumer.ReleaseArchiveError, match="retired task-authority contract"):
        consumer._validate_staged_installer(package)


@pytest.mark.parametrize("damage", ("missing", "wrong-path", "wrong-hash"))
def test_archive_consumer_refuses_invalid_integrity_helper_binding(tmp_path, damage):
    package = _package(tmp_path)
    report = verifier.verify_staged_package(package)
    entrypoint = report["public_entrypoint"]
    if damage == "missing":
        del entrypoint["integrity_helper_sha256"]
    elif damage == "wrong-path":
        entrypoint["integrity_helper_path"] = "tools/unreviewed.ps1"
    else:
        entrypoint["integrity_helper_sha256"] = "0" * 64
    _write_staged_report(package, report)
    consumer = _archive_consumer()
    with pytest.raises(consumer.ReleaseArchiveError, match="public bootstrap binding"):
        consumer._validate_staged_installer(package)


@pytest.mark.parametrize(
    "relative", sorted(verifier.FORBIDDEN_ACTIVE_AUTHORITY_MEMBERS)
)
def test_verifier_rejects_every_retired_active_authority_member(tmp_path, relative):
    package = _package(tmp_path)
    target = package / relative
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_bytes(b"retired active authority")

    with pytest.raises(
        verifier.StagedInstallerVerificationError,
        match="retired active task-authority",
    ):
        verifier.verify_staged_package(package)


def test_verifier_rejects_task_creation_reintroduced_into_public_bootstrap(tmp_path):
    package = _package(tmp_path)
    installer = package / "INSTALL_THIS_PC.ps1"
    installer.write_text(
        installer.read_text(encoding="utf-8") + "\nRegister-ScheduledTask -TaskName bad\n",
        encoding="utf-8",
    )

    with pytest.raises(
        verifier.StagedInstallerVerificationError,
        match="retired enrollment/task authority",
    ):
        verifier.verify_staged_package(package)


def test_verifier_requires_onedir_runtime(tmp_path):
    package = _package(tmp_path)
    (package / "_internal" / "config" / "app_settings.json").unlink()

    with pytest.raises(
        verifier.StagedInstallerVerificationError,
        match="required staged members",
    ):
        verifier.verify_staged_package(package)


@pytest.mark.parametrize("damage", ("task-name", "unguarded", "helper", "inline-helper"))
def test_verifier_refuses_registration_outside_exact_startup_task_contract(tmp_path, damage):
    package = _package(tmp_path)
    installer = package / "INSTALL_THIS_PC.ps1"
    source = installer.read_text(encoding="utf-8")
    if damage == "task-name":
        source = source.replace("Register-ScheduledTask -TaskName $spec.name -Xml $spec.xml -Force",
                                "Register-ScheduledTask -TaskName bad -Xml $spec.xml -Force", 1)
    elif damage == "unguarded":
        source = source.replace("Invoke-LabelStartupUpdateTaskMutation {", "& {", 1)
    else:
        helper = package / "tools/bootstrap_integrity.ps1"
        extra = "\nRegister-ScheduledTask -TaskName bad\n" if damage == "helper" else "\nfunction Forbidden { Register-ScheduledTask -TaskName bad }\n"
        helper.write_text(helper.read_text(encoding="utf-8") + extra,
                          encoding="utf-8")
    installer.write_text(source, encoding="utf-8")
    with pytest.raises(verifier.StagedInstallerVerificationError,
                       match="retired enrollment/task authority"):
        verifier.verify_staged_package(package)


@pytest.mark.parametrize("relative", sorted(shared_bootstrap_files()))
def test_verifier_requires_complete_shared_bootstrap_closure(tmp_path, relative):
    package = _package(tmp_path)
    (package / relative).unlink()
    with pytest.raises(verifier.StagedInstallerVerificationError, match="required staged members"):
        verifier.verify_staged_package(package)


def test_cli_writes_exact_report(tmp_path):
    package = _package(tmp_path)
    report_path = tmp_path / "report.json"

    assert verifier.main(
        ["--package-root", str(package), "--report", str(report_path)]
    ) == 0
    payload = json.loads(report_path.read_text(encoding="utf-8"))
    assert payload["status"] == "PASS"
    assert payload["runtime_host"]["path"] == "Label_Match.exe"
