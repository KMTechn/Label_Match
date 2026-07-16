import base64
import importlib.util
import json
import os
import re
import shutil
import subprocess
import sys
import textwrap
import zipfile
from pathlib import Path

from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey


def test_release_requirements_are_exact_hash_locked_for_windows_cp312():
    lines = [
        line.strip()
        for line in Path("requirements-release.txt").read_text(encoding="utf-8").splitlines()
        if line.strip() and not line.lstrip().startswith("#")
    ]

    assert len(lines) == 24
    assert all("==" in line for line in lines)
    assert all(" --hash=sha256:" in line for line in lines)
    assert all(line.count("--hash=sha256:") == 1 for line in lines)
    assert any(line.startswith("pyinstaller==6.20.0 ") for line in lines)
    assert any(line.startswith("pytest==9.1.1 ") for line in lines)


def test_release_workflow_packages_direct_sync_relay_tools():
    workflow = Path(".github/workflows/release.yml").read_text(encoding="utf-8")

    assert 'python tools/build_release_cli_tools.py --destination "dist/Label_Match/tools" --help-timeout-seconds 15 --probe-count 3' in workflow
    assert 'python tools/build_release_cli_tools.py --destination "dist/Label_Match/tools" --verify-existing --help-timeout-seconds 15 --probe-count 3' in workflow
    assert 'pyinstaller --name "direct_sync_relay_runner"' not in workflow.lower()
    assert 'pyinstaller --name "direct_sync_relay_install_pack"' not in workflow.lower()
    assert 'pyinstaller --name "register_label_match_worker_pc"' not in workflow.lower()
    release_requirements = Path("requirements-release.txt").read_text(encoding="utf-8")
    assert "pyinstaller==6.20.0" in release_requirements
    assert "pytest==" in release_requirements
    assert "tools/verify_release_identity.py" in workflow
    identity_block = workflow[
        workflow.index("- name: Validate release identity") : workflow.index("- name: Install dependencies")
    ]
    assert "github.ref_name" not in identity_block
    assert "github.sha" not in identity_block
    assert "GITHUB_REF_NAME" not in identity_block
    assert '--expected-tag "$env:LABEL_MATCH_RELEASE_TAG"' in identity_block
    assert '--expected-sha "$env:GITHUB_SHA"' in identity_block
    assert "repository_dispatch:" in workflow
    assert "types: [label-match-release]" in workflow
    assert "push:" not in workflow
    assert "github.ref_name" not in workflow
    assert "requirements-release.txt" in workflow
    assert "--require-hashes --no-deps" in workflow
    assert "python-version: '3.12.10'" in workflow
    assert "runs-on: [self-hosted, Windows, X64, label-match-signing]" in workflow
    assert "environment: label-match-production-signing" in workflow
    assert "python -I -S tools/verify_release_identity.py" in workflow
    assert "LABEL_MATCH_RELEASE_TAG_SIGNER_FINGERPRINT" in identity_block
    assert "python -I -m venv" in workflow
    assert "PYTHONNOUSERSITE" in workflow
    assert "PYTEST_DISABLE_PLUGIN_AUTOLOAD" in workflow
    assert "Clean release virtual environment" in workflow
    assert workflow.count('$PSNativeCommandUseErrorActionPreference = $true') >= 4
    assert "fetch-depth: 0" in workflow
    assert "persist-credentials: false" in workflow
    assert "LABEL_MATCH_ENROLLMENT_TOKEN" not in workflow
    assert "Normal installs use PRODUCER_SELF_ENROLL_ALLOWED_IPS" in workflow

    include_step = workflow.index("- name: Include direct-sync relay tools")
    identity_step = workflow.index("- name: Validate release identity")
    test_step = workflow.index("- name: Run tests")
    helper_build_step = workflow.index("- name: Build direct-sync CLI tools")
    zip_step = workflow.index("- name: Build and verify deterministic release archive")
    packaging_block = workflow[include_step:zip_step]

    assert identity_step < test_step < helper_build_step < include_step < zip_step
    assert "New-Item -ItemType Directory -Force -Path dist/Label_Match/tools" in packaging_block
    assert "Copy-Item install_label_match_direct_sync.ps1 -Destination dist/Label_Match" in packaging_block
    assert "Copy-Item direct_sync_push.py,direct_sync_runtime.py,direct_sync_operator.py -Destination dist/Label_Match" in packaging_block
    assert (
        "Copy-Item tools/direct_sync_relay_runner.py,tools/direct_sync_relay_operator.py,"
        "tools/direct_sync_relay_install_pack.py,tools/direct_sync_phase_g_label_match_runtime_report.py,"
        "tools/register_label_match_worker_pc.py "
        "-Destination dist/Label_Match/tools"
    ) in packaging_block
    assert "Copy-Item dist/direct_sync_relay_runner.exe" not in packaging_block
    assert 'Copy-Item "$env:RUNNER_TEMP\\label-match-release-identity.json" -Destination dist/Label_Match/release-identity.json' in packaging_block
    assert "tools/register_label_match_worker_pc.py" in packaging_block
    assert "direct_sync_relay_install_pack/direct_sync_relay_install_pack.exe" in workflow
    assert "Label_Match/tools/release_cli_tools_manifest.json" in workflow
    assert "Label_Match/tools/release_cli_tools_post_sign_manifest.json" in workflow


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
    assert "Label_Match-${{ env.LABEL_MATCH_RELEASE_TAG }}.manifest.json" in workflow
    assert "Label_Match-${{ env.LABEL_MATCH_RELEASE_TAG }}.zip" in workflow
    assert "- name: Sign private update manifest" in workflow
    assert "PRIVATE_UPDATE_MANIFEST_SIGNING_KEY" in workflow
    assert "- name: Publish private update feed" in workflow
    assert "COMPANY_UPDATE_UPLOAD_TOKEN" in workflow
    assert "COMPANY_UPDATE_UPLOAD_ORIGIN_IP" in workflow
    assert "--resolve" in workflow
    assert "PRIVATE_UPDATE_APP_SLUG: label_match" in workflow
    assert "curl.exe" in workflow
    assert "curl.exe --config -" in workflow
    assert '"-H", "Authorization: Bearer' not in workflow
    assert "- name: Attach install update settings" in workflow
    assert "dist/Label_Match/config/app_settings.json" in workflow
    assert "PRIVATE_UPDATE_MANIFEST_URL" in workflow
    assert "PRIVATE_UPDATE_MANIFEST_PUBLIC_KEY" in workflow
    assert 'provider = "private_manifest"' in workflow
    assert 'provider = "github"' in workflow
    assert "PRIVATE_UPDATE_MANIFEST_URL and PRIVATE_UPDATE_MANIFEST_PUBLIC_KEY must be set together." in workflow
    assert "WINDOWS_CODE_SIGNING_PFX_BASE64" not in workflow
    assert "WINDOWS_CODE_SIGNING_PFX_PASSWORD" not in workflow
    assert "WINDOWS_CODE_SIGNING_CERT_THUMBPRINT" in workflow
    assert "WINDOWS_CODE_SIGNING_TIMESTAMP_URL" in workflow
    assert "tools/sign_release_executables.ps1" in workflow
    assert "Label_Match/authenticode-manifest.json" in workflow
    assert "tools/verify_staged_release_installer.py" in workflow
    assert "tests/test_staged_release_installer.py" in workflow
    assert "tools/build_release_archive.py" in workflow
    assert "archive-verification.json" in workflow
    assert "Compress-Archive" not in workflow

    manifest_step = workflow.index("- name: Generate private update manifest")
    release_step = workflow.index("- name: Create Release and Upload Asset")
    attach_step = workflow.index("- name: Attach install update settings")
    sign_step = workflow.index("- name: Sign and verify release executables")
    signed_cli_step = workflow.index("- name: Reverify signed CLI tools")
    installer_step = workflow.index("- name: Verify staged installer without system Python")
    zip_step = workflow.index("- name: Build and verify deterministic release archive")
    upload_block = workflow[release_step:]

    assert attach_step < zip_step
    assert attach_step < sign_step < signed_cli_step < installer_step < zip_step
    assert manifest_step < release_step
    assert "files: |" in upload_block
    assert "Label_Match-${{ env.LABEL_MATCH_RELEASE_TAG }}.zip" in upload_block
    assert "Label_Match-${{ env.LABEL_MATCH_RELEASE_TAG }}.zip.sha256" in upload_block
    assert "Label_Match-${{ env.LABEL_MATCH_RELEASE_TAG }}.archive-verification.json" in upload_block
    assert "Label_Match-${{ env.LABEL_MATCH_RELEASE_TAG }}.manifest.json" not in upload_block
    assert "tag_name: ${{ env.LABEL_MATCH_RELEASE_TAG }}" in upload_block


def test_release_workflow_self_verifies_private_manifest_signature_before_publish(tmp_path):
    workflow = Path(".github/workflows/release.yml").read_text(encoding="utf-8")

    sign_step = workflow.index("- name: Sign private update manifest")
    publish_step = workflow.index("- name: Publish private update feed")
    sign_block = workflow[sign_step:publish_step]
    next_step = workflow.index("\n      - name:", sign_step + 1)
    sign_step_block = workflow[sign_step:next_step]

    assert sign_step < publish_step
    assert (
        "PRIVATE_UPDATE_MANIFEST_PUBLIC_KEY: "
        "${{ vars.PRIVATE_UPDATE_MANIFEST_PUBLIC_KEY }}"
    ) in sign_block
    verify_expression = (
        "Ed25519PublicKey.from_public_bytes(public_key).verify(signature, canonical)"
    )
    write_expression = (
        'manifest_path.with_name(manifest_path.name + ".sig").write_bytes(signature)'
    )
    assert verify_expression in sign_block
    assert write_expression in sign_block
    assert sign_block.index(verify_expression) < sign_block.index(write_expression)
    assert "signature does not match" in sign_block
    assert "if ($LASTEXITCODE -ne 0)" in sign_block
    assert 'throw "Private update manifest signature self-test failed."' in sign_block
    assert "[string]::IsNullOrWhiteSpace($env:PRIVATE_UPDATE_MANIFEST_PUBLIC_KEY)" in sign_block
    assert 'throw "PRIVATE_UPDATE_MANIFEST_PUBLIC_KEY variable is required."' in sign_block
    assert "if ($sig.Length -ne 64)" in sign_block
    assert 'throw "Private update manifest signature must be 64 bytes."' in sign_block
    embedded_script = textwrap.dedent(
        sign_block.split("$script = @'", 1)[1].split("'@", 1)[0]
    ).strip()
    powershell_script = textwrap.dedent(
        sign_step_block.split("        run: |\n", 1)[1]
    ).strip()

    manifest = {
        "schema_version": "kmtech-private-update-manifest-v1",
        "version": "v-test-한글",
        "artifact": {
            "name": "라벨-✓.zip",
            "sha256": "a" * 64,
            "metadata": {"labels": ["정상", "검증"], "enabled": True},
        },
    }
    manifest_path = tmp_path / "Label_Match-v-test.manifest.json"
    manifest_path.write_text(
        json.dumps(manifest, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    signing_key = Ed25519PrivateKey.generate()
    private_key_hex = signing_key.private_bytes_raw().hex()
    public_key_hex = signing_key.public_key().public_bytes_raw().hex()

    env = os.environ.copy()
    env["PRIVATE_UPDATE_MANIFEST_PATH"] = str(manifest_path)
    env["PRIVATE_UPDATE_MANIFEST_SIGNING_KEY"] = private_key_hex
    env["PRIVATE_UPDATE_MANIFEST_PUBLIC_KEY"] = public_key_hex
    matching = subprocess.run(
        [sys.executable, "-c", embedded_script],
        capture_output=True,
        text=True,
        env=env,
        timeout=30,
        check=False,
    )
    matching_output = matching.stdout + matching.stderr
    assert matching.returncode == 0, matching_output
    assert private_key_hex not in matching_output
    assert public_key_hex not in matching_output
    generated_signature = manifest_path.with_name(manifest_path.name + ".sig").read_bytes()
    assert len(generated_signature) == 64
    parsed_manifest = json.loads(manifest_path.read_text(encoding="utf-8-sig"))

    module_path = Path(__file__).resolve().parents[1] / "Label_Match.py"
    spec = importlib.util.spec_from_file_location("label_match_release_workflow_test", module_path)
    assert spec is not None and spec.loader is not None
    label_match_module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(label_match_module)
    label_match_module._verify_update_manifest_signature(
        parsed_manifest,
        generated_signature,
        public_key_hex,
    )
    known_good_signature = generated_signature
    mutated_manifest = dict(manifest)
    mutated_manifest["version"] = "v-test-변경"
    manifest_path.write_text(
        json.dumps(mutated_manifest, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )

    cases = (
        (
            Ed25519PrivateKey.generate().public_key().public_bytes_raw().hex(),
            "signature does not match",
        ),
        ("not-hex", "must be hex"),
        ("00" * 31, "must decode to 32 bytes"),
    )
    for configured_public_key, expected_error in cases:
        env["PRIVATE_UPDATE_MANIFEST_PUBLIC_KEY"] = configured_public_key
        rejected = subprocess.run(
            [sys.executable, "-c", embedded_script],
            capture_output=True,
            text=True,
            env=env,
            timeout=30,
            check=False,
        )
        rejected_output = rejected.stdout + rejected.stderr
        assert rejected.returncode != 0
        assert expected_error in rejected_output
        assert private_key_hex not in rejected_output
        assert configured_public_key not in rejected_output
        assert manifest_path.with_name(manifest_path.name + ".sig").read_bytes() == known_good_signature

    manifest_path.write_text(
        json.dumps(manifest, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )

    powershell = next(
        (
            executable
            for name in ("pwsh", "powershell", "powershell.exe")
            if (executable := shutil.which(name))
        ),
        None,
    )
    if powershell:
        encoded_command = base64.b64encode(
            powershell_script.encode("utf-16le")
        ).decode("ascii")
        command = [
            powershell,
            "-NoProfile",
            "-NonInteractive",
            "-EncodedCommand",
            encoded_command,
        ]
        env["PRIVATE_UPDATE_MANIFEST_PUBLIC_KEY"] = public_key_hex
        wrapper_matching = subprocess.run(
            command,
            capture_output=True,
            text=True,
            env=env,
            timeout=30,
            check=False,
        )
        wrapper_matching_output = wrapper_matching.stdout + wrapper_matching.stderr
        assert wrapper_matching.returncode == 0, wrapper_matching_output
        assert private_key_hex not in wrapper_matching_output
        assert public_key_hex not in wrapper_matching_output
        wrapper_signature = manifest_path.with_name(manifest_path.name + ".sig").read_bytes()
        label_match_module._verify_update_manifest_signature(
            parsed_manifest,
            wrapper_signature,
            public_key_hex,
        )
        wrapper_good_signature = wrapper_signature
        manifest_path.write_text(
            json.dumps(mutated_manifest, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )

        for configured_public_key, expected_error in cases:
            env["PRIVATE_UPDATE_MANIFEST_PUBLIC_KEY"] = configured_public_key
            wrapper_rejected = subprocess.run(
                command,
                capture_output=True,
                text=True,
                env=env,
                timeout=30,
                check=False,
            )
            wrapper_output = wrapper_rejected.stdout + wrapper_rejected.stderr
            assert wrapper_rejected.returncode != 0
            assert expected_error in wrapper_output
            assert "self-test failed" in wrapper_output
            assert private_key_hex not in wrapper_output
            assert configured_public_key not in wrapper_output
            assert (
                manifest_path.with_name(manifest_path.name + ".sig").read_bytes()
                == wrapper_good_signature
            )

        for missing_value in (None, ""):
            missing_env = env.copy()
            if missing_value is None:
                missing_env.pop("PRIVATE_UPDATE_MANIFEST_PUBLIC_KEY", None)
            else:
                missing_env["PRIVATE_UPDATE_MANIFEST_PUBLIC_KEY"] = missing_value
            wrapper_missing = subprocess.run(
                command,
                capture_output=True,
                text=True,
                env=missing_env,
                timeout=30,
                check=False,
            )
            missing_output = wrapper_missing.stdout + wrapper_missing.stderr
            assert wrapper_missing.returncode != 0
            assert "PRIVATE_UPDATE_MANIFEST_PUBLIC_KEY variable is required" in missing_output
            assert private_key_hex not in missing_output


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
    for filename in ["direct_sync_relay_runner.exe", "register_label_match_worker_pc.exe"]:
        (staged_tools / filename).write_bytes(b"fixture exe")
    install_payload = staged_tools / "direct_sync_relay_install_pack"
    install_payload.mkdir()
    (install_payload / "direct_sync_relay_install_pack.exe").write_bytes(b"fixture exe")
    (staged_tools / "release_cli_tools_manifest.json").write_text("{}\n", encoding="utf-8")

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
        "Label_Match/tools/direct_sync_relay_install_pack/direct_sync_relay_install_pack.exe",
        "Label_Match/tools/register_label_match_worker_pc.exe",
        "Label_Match/tools/release_cli_tools_manifest.json",
    }.issubset(names)


def test_one_step_installer_uses_bundled_tools_ip_allowlist_and_programdata_paths():
    script = Path("install_label_match_direct_sync.ps1").read_text(encoding="utf-8")

    assert '"direct_sync_relay_install_pack\\direct_sync_relay_install_pack.exe"' in script
    assert "direct_sync_relay_runner.exe" in script
    assert "direct_sync_relay_runner.py" in script
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
    assert "--python-exe" in script
    assert '--runner-exe", $runnerExe' in script
    assert "--registration-exe" in script
    assert "--app-settings-path" in script
    assert '"_internal"' in script
    assert "--confirm-production-install" not in script
