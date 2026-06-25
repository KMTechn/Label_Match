import shutil
import subprocess
import sys
import zipfile
from pathlib import Path


def test_release_workflow_packages_direct_sync_relay_tools():
    workflow = Path(".github/workflows/release.yml").read_text(encoding="utf-8")

    assert 'pyinstaller --name "direct_sync_relay_runner" --onefile --console --paths "." tools/direct_sync_relay_runner.py' in workflow
    assert 'pyinstaller --name "direct_sync_relay_install_pack" --onefile --console --paths "." tools/direct_sync_relay_install_pack.py' in workflow
    assert 'pyinstaller --name "register_label_match_worker_pc" --onefile --console --paths "." tools/register_label_match_worker_pc.py' in workflow
    assert "LABEL_MATCH_ENROLLMENT_TOKEN" not in workflow
    assert "Normal installs use PRODUCER_SELF_ENROLL_ALLOWED_IPS" in workflow

    include_step = workflow.index("- name: Include direct-sync relay tools")
    zip_step = workflow.index("- name: Zip the build folder")
    packaging_block = workflow[include_step:zip_step]

    assert include_step < zip_step
    assert "New-Item -ItemType Directory -Force -Path dist/Label_Match/tools" in packaging_block
    assert "Copy-Item install_label_match_direct_sync.ps1 -Destination dist/Label_Match" in packaging_block
    assert "Copy-Item direct_sync_push.py,direct_sync_runtime.py,direct_sync_operator.py -Destination dist/Label_Match" in packaging_block
    assert (
        "Copy-Item tools/direct_sync_relay_runner.py,tools/direct_sync_relay_operator.py,"
        "tools/direct_sync_relay_install_pack.py,tools/direct_sync_phase_g_label_match_runtime_report.py,"
        "tools/register_label_match_worker_pc.py "
        "-Destination dist/Label_Match/tools"
    ) in packaging_block
    assert (
        "Copy-Item dist/direct_sync_relay_runner.exe,dist/direct_sync_relay_install_pack.exe,"
        "dist/register_label_match_worker_pc.exe -Destination dist/Label_Match/tools"
    ) in packaging_block
    assert "tools/register_label_match_worker_pc.py" in packaging_block


def test_staged_release_relay_files_are_importable_and_archived(tmp_path):
    source_root = Path.cwd()
    staged_root = tmp_path / "dist" / "Label_Match"
    staged_tools = staged_root / "tools"
    staged_tools.mkdir(parents=True)

    shutil.copy2(source_root / "install_label_match_direct_sync.ps1", staged_root / "install_label_match_direct_sync.ps1")
    for filename in ["direct_sync_push.py", "direct_sync_runtime.py", "direct_sync_operator.py"]:
        shutil.copy2(source_root / filename, staged_root / filename)
    for filename in [
        "direct_sync_relay_runner.py",
        "direct_sync_relay_operator.py",
        "direct_sync_relay_install_pack.py",
        "direct_sync_phase_g_label_match_runtime_report.py",
        "register_label_match_worker_pc.py",
    ]:
        shutil.copy2(source_root / "tools" / filename, staged_tools / filename)
    for filename in [
        "direct_sync_relay_runner.exe",
        "direct_sync_relay_install_pack.exe",
        "register_label_match_worker_pc.exe",
    ]:
        (staged_tools / filename).write_bytes(b"fixture exe")

    completed = subprocess.run(
        [sys.executable, str(staged_tools / "direct_sync_relay_runner.py"), "--help"],
        check=False,
        capture_output=True,
        text=True,
    )

    assert completed.returncode == 0
    assert "Label_Match direct-sync relay runner" in completed.stdout

    zip_path = tmp_path / "Label_Match-test.zip"
    with zipfile.ZipFile(zip_path, "w") as archive:
        for path in staged_root.rglob("*"):
            if path.is_file():
                archive.write(path, path.relative_to(tmp_path / "dist").as_posix())

    with zipfile.ZipFile(zip_path) as archive:
        names = set(archive.namelist())
    assert {
        "Label_Match/direct_sync_push.py",
        "Label_Match/direct_sync_runtime.py",
        "Label_Match/direct_sync_operator.py",
        "Label_Match/install_label_match_direct_sync.ps1",
        "Label_Match/tools/direct_sync_relay_runner.py",
        "Label_Match/tools/direct_sync_relay_operator.py",
        "Label_Match/tools/direct_sync_relay_install_pack.py",
        "Label_Match/tools/direct_sync_phase_g_label_match_runtime_report.py",
        "Label_Match/tools/register_label_match_worker_pc.py",
        "Label_Match/tools/direct_sync_relay_runner.exe",
        "Label_Match/tools/direct_sync_relay_install_pack.exe",
        "Label_Match/tools/register_label_match_worker_pc.exe",
    }.issubset(names)


def test_one_step_installer_uses_bundled_tools_ip_allowlist_and_programdata_paths():
    script = Path("install_label_match_direct_sync.ps1").read_text(encoding="utf-8")

    assert "direct_sync_relay_install_pack.exe" in script
    assert "direct_sync_relay_runner.exe" in script
    assert "register_label_match_worker_pc.exe" in script
    assert "EnrollmentTokenFile" in script
    assert "enrollment_token.txt" not in script
    assert "Test-Path -LiteralPath $tokenFile" not in script
    assert "Get-MachineStableSuffix" in script
    assert "label-match-{0}-{1}" in script
    assert "C:\\ProgramData\\KMTech\\DirectSync\\$sourceHostId" in script
    assert "direct-sync-relay-$sourceHostId" in script
    assert "C:\\ProgramData\\KMTech\\DirectSync\\label_match" not in script
    assert "C:\\ProgramData\\KMTech\\Label_Match\\data" in script
    assert "custom_save_path" in script
    assert "--self-enroll" in script
    assert "--runner-exe" in script
    assert "--registration-exe" in script
    assert "--confirm-production-install" in script
