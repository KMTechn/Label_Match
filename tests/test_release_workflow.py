import re
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


def test_release_workflow_generates_private_update_manifest():
    workflow = Path(".github/workflows/release.yml").read_text(encoding="utf-8")
    uses_values = re.findall(r"(?m)^\s+uses:\s+([^\s#]+)", workflow)

    assert uses_values
    assert all(re.search(r"@[0-9a-f]{40}$", value) for value in uses_values)
    assert "softprops/action-gh-release@v2" not in workflow
    assert "- name: Generate private update manifest" in workflow
    assert "- name: Generate release SHA256 checksum" in workflow
    assert "Get-FileHash -Algorithm SHA256" in workflow
    assert "kmtech-private-update-manifest-v1" in workflow
    assert "app_id = \"Label_Match\"" in workflow
    assert "PRIVATE_UPDATE_ARTIFACT_BASE_URL" in workflow
    assert "PRIVATE_UPDATE_ARTIFACT_BASE_URL must use HTTPS." in workflow
    assert "PRIVATE_UPDATE_ARTIFACT_BASE_URL must not include userinfo." in workflow
    assert "PRIVATE_UPDATE_ARTIFACT_BASE_URL must not include fragments." in workflow
    assert "PRIVATE_UPDATE_ARTIFACT_BASE_URL must not contain query strings." in workflow
    assert "not GitHub release storage" in workflow
    assert ".githubusercontent.com" in workflow
    assert "PRIVATE_UPDATE_ROLLOUT_PERCENTAGE" in workflow
    assert "PRIVATE_UPDATE_ROLLOUT_PERCENTAGE must be an integer from 0 to 100." in workflow
    assert "$artifactUrl = \"$baseUrl/$zipPath\"" in workflow
    assert "\"$hash  $zipPath\" | Set-Content -Encoding utf8NoBOM \"$zipPath.sha256\"" in workflow
    assert "releases/download" not in workflow
    assert "percentage = $rolloutPercentage" in workflow
    assert "Label_Match-${{ github.ref_name }}.manifest.json" in workflow
    assert "Label_Match-${{ github.ref_name }}.zip" in workflow
    assert "- name: Sign private update manifest" in workflow
    assert "PRIVATE_UPDATE_MANIFEST_SIGNING_KEY" in workflow
    assert "- name: Publish private update feed" in workflow
    assert "COMPANY_UPDATE_UPLOAD_TOKEN" in workflow
    assert "COMPANY_UPDATE_UPLOAD_ORIGIN_IP" in workflow
    assert "--resolve" in workflow
    assert "PRIVATE_UPDATE_APP_SLUG: label_match" in workflow
    assert "curl.exe" in workflow
    assert "- name: Attach install update settings" in workflow
    assert "dist/Label_Match/config/app_settings.json" in workflow
    assert "PRIVATE_UPDATE_MANIFEST_URL" in workflow
    assert "PRIVATE_UPDATE_MANIFEST_PUBLIC_KEY" in workflow
    assert 'provider = "private_manifest"' in workflow
    assert 'provider = "github"' in workflow
    assert "PRIVATE_UPDATE_MANIFEST_URL and PRIVATE_UPDATE_MANIFEST_PUBLIC_KEY must be set together." in workflow

    manifest_step = workflow.index("- name: Generate private update manifest")
    release_step = workflow.index("- name: Create Release and Upload Asset")
    attach_step = workflow.index("- name: Attach install update settings")
    zip_step = workflow.index("- name: Zip the build folder")
    upload_block = workflow[release_step:]

    assert attach_step < zip_step
    assert manifest_step < release_step
    assert "files: |" in upload_block
    assert "Label_Match-${{ github.ref_name }}.zip" in upload_block
    assert "Label_Match-${{ github.ref_name }}.zip.sha256" in upload_block
    assert "Label_Match-${{ github.ref_name }}.manifest.json" not in upload_block


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
    assert "--confirm-production-install" not in script
