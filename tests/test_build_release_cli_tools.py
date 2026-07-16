from __future__ import annotations

import importlib.util
import json
from pathlib import Path
import subprocess
import sys

import pytest


MODULE_PATH = Path(__file__).resolve().parents[1] / "tools" / "build_release_cli_tools.py"
SPEC = importlib.util.spec_from_file_location("build_release_cli_tools_for_tests", MODULE_PATH)
assert SPEC and SPEC.loader
builder = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = builder
SPEC.loader.exec_module(builder)


def _built_tool(tmp_path: Path, spec=None):
    spec = spec or builder.TOOL_SPECS[0]
    executable = tmp_path / spec.executable_name
    executable.write_bytes(b"candidate")
    return builder.BuiltTool(
        spec=spec,
        payload_root=executable,
        executable=executable,
        executable_sha256=builder._sha256(executable),
        executable_size=executable.stat().st_size,
        payload_inventory=builder._payload_inventory(executable),
        help_runs=[{"run": run, "status": "PASS"} for run in range(1, 4)],
        archive_verification={"status": "PASS", "viewer_stdout_sha256": "a" * 64, "viewer_stderr_bytes": 0},
    )


def test_tool_contract_is_exact_and_install_pack_uses_onedir():
    assert [(spec.name, spec.source_rel, spec.mode) for spec in builder.TOOL_SPECS] == [
        ("direct_sync_relay_runner", "tools/direct_sync_relay_runner.py", "onefile"),
        ("direct_sync_relay_install_pack", "tools/direct_sync_relay_install_pack.py", "onedir"),
        ("register_label_match_worker_pc", "tools/register_label_match_worker_pc.py", "onefile"),
    ]
    assert all(spec.help_marker for spec in builder.TOOL_SPECS)


def test_pyinstaller_commands_are_clean_noupx_and_use_disjoint_roots(tmp_path):
    commands = []
    roots = []
    for index, spec in enumerate(builder.TOOL_SPECS):
        tool_root = tmp_path / f"tool-{index}"
        work = tool_root / "work"
        dist = tool_root / "dist"
        spec_path = tool_root / "spec"
        roots.append({work, dist, spec_path})
        command = builder._pyinstaller_command(
            spec,
            repo_root=tmp_path,
            source=tmp_path / spec.source_rel,
            work_path=work,
            dist_path=dist,
            spec_path=spec_path,
        )
        commands.append(command)
        assert "--clean" in command
        assert "--noupx" in command
        assert "--noconfirm" in command
        assert ("--onedir" in command) is (spec.mode == "onedir")
        assert ("--onefile" in command) is (spec.mode == "onefile")
    assert all(left.isdisjoint(right) for i, left in enumerate(roots) for right in roots[i + 1 :])


def test_verify_pe_rejects_fixture_bytes_and_accepts_pe_header(tmp_path):
    executable = tmp_path / "tool.exe"
    executable.write_bytes(b"fixture exe")
    with pytest.raises(builder.ReleaseCliBuildError, match="not a Windows PE"):
        builder._verify_pe(executable)

    content = bytearray(132)
    content[:2] = b"MZ"
    content[0x3C:0x40] = (128).to_bytes(4, "little")
    content[128:132] = b"PE\0\0"
    executable.write_bytes(content)
    builder._verify_pe(executable)


def test_verify_carchive_requires_structure_script_and_onefile_pyz(tmp_path, monkeypatch):
    spec = builder.TOOL_SPECS[0]
    executable = tmp_path / spec.executable_name
    executable.write_bytes(b"candidate")
    valid = (
        f"Options in '{spec.executable_name}' (PKG/CArchive):\n"
        f"Contents of '{spec.executable_name}' (PKG/CArchive):\n"
        f"0, 1, 1, 0, 's', '{spec.name}'\n"
        "1, 1, 1, 0, 'z', 'PYZ.pyz'\n"
    ).encode()
    monkeypatch.setattr(
        builder,
        "_run_checked",
        lambda *_args, **_kwargs: subprocess.CompletedProcess([], 0, valid, b""),
    )
    assert builder._verify_carchive(spec, executable, repo_root=tmp_path)["status"] == "PASS"

    monkeypatch.setattr(
        builder,
        "_run_checked",
        lambda *_args, **_kwargs: subprocess.CompletedProcess([], 0, valid.replace(b"PYZ.pyz", b"missing"), b""),
    )
    with pytest.raises(builder.ReleaseCliBuildError, match="PYZ payload missing"):
        builder._verify_carchive(spec, executable, repo_root=tmp_path)


def test_isolated_help_requires_expected_usage_stderr_zero_and_no_residual(tmp_path, monkeypatch):
    built = _built_tool(tmp_path)

    class FakeProcess:
        pid = 123
        returncode = 0

        def communicate(self, timeout):
            assert timeout == 15
            return f"usage: tool\n{built.spec.help_marker}\n".encode(), b""

        def poll(self):
            return self.returncode

    monkeypatch.setattr(builder.subprocess, "Popen", lambda *args, **kwargs: FakeProcess())
    monkeypatch.setattr(
        builder,
        "_process_entries",
        lambda: [(77, built.spec.executable_name), (88, "unrelated.exe")],
    )
    observed = {}

    def fake_terminate(_executable, _root_pid, *, baseline_pids):
        observed["baseline_pids"] = list(baseline_pids)
        return []

    monkeypatch.setattr(builder, "_terminate_and_verify_zero", fake_terminate)

    result = builder._run_isolated_help(
        built,
        run_no=1,
        timeout_seconds=15,
        smoke_root=tmp_path / "smoke",
    )
    assert result["status"] == "PASS"
    assert result["residual_process_count"] == 0
    assert observed["baseline_pids"] == [77]


def test_one_timeout_is_always_deny_even_after_cleanup(tmp_path, monkeypatch):
    built = _built_tool(tmp_path)

    class TimeoutProcess:
        pid = 456
        returncode = None

        def communicate(self, timeout):
            raise subprocess.TimeoutExpired("tool", timeout)

        def poll(self):
            return None

    monkeypatch.setattr(builder.subprocess, "Popen", lambda *args, **kwargs: TimeoutProcess())
    monkeypatch.setattr(builder, "_process_entries", lambda: [])
    monkeypatch.setattr(builder, "_terminate_and_verify_zero", lambda *_args, **_kwargs: [456])

    with pytest.raises(builder.ReleaseCliBuildError, match="timed out"):
        builder._run_isolated_help(
            built,
            run_no=2,
            timeout_seconds=15,
            smoke_root=tmp_path / "smoke",
        )


def test_build_is_fully_sequential_and_does_not_publish_after_failure(tmp_path, monkeypatch):
    repo_root = tmp_path / "repo"
    repo_root.mkdir()
    destination_parent = tmp_path / "dist" / "Label_Match"
    destination_parent.mkdir(parents=True)
    destination = destination_parent / "tools"
    events = []

    def fake_build(spec, *, repo_root, tool_root):
        events.append(("build", spec.name))
        tool_root.mkdir(parents=True)
        return _built_tool(tool_root, spec)

    def fake_help(built, *, run_no, timeout_seconds, smoke_root):
        events.append(("help", built.spec.name, run_no))
        if built.spec.name == "register_label_match_worker_pc" and run_no == 2:
            raise builder.ReleaseCliBuildError("blocked third helper")
        return {"run": run_no, "status": "PASS"}

    monkeypatch.setattr(builder, "_build_candidate", fake_build)
    monkeypatch.setattr(builder, "_run_isolated_help", fake_help)
    monkeypatch.setattr(builder, "_verify_clean_checkout", lambda _root: None)

    with pytest.raises(builder.ReleaseCliBuildError, match="blocked third helper"):
        builder.build_release_cli_tools(destination, repo_root=repo_root)

    assert not destination.exists()
    assert events[:4] == [
        ("build", "direct_sync_relay_runner"),
        ("help", "direct_sync_relay_runner", 1),
        ("help", "direct_sync_relay_runner", 2),
        ("help", "direct_sync_relay_runner", 3),
    ]
    assert events[4][0:2] == ("build", "direct_sync_relay_install_pack")


def test_atomic_publish_preserves_exact_payload_hashes(tmp_path, monkeypatch):
    repo_root = tmp_path / "repo"
    repo_root.mkdir()
    (repo_root / "Label_Match.py").write_text('APP_VERSION = "v2.0.36"\n', encoding="utf-8")
    destination_parent = tmp_path / "dist" / "Label_Match"
    destination_parent.mkdir(parents=True)
    destination = destination_parent / "tools"
    built_tools = []
    for spec in builder.TOOL_SPECS:
        root = tmp_path / spec.name
        if spec.mode == "onefile":
            root.mkdir()
            built = _built_tool(root, spec)
        else:
            payload = root / spec.name
            internal = payload / "_internal"
            internal.mkdir(parents=True)
            executable = payload / spec.executable_name
            executable.write_bytes(b"onedir executable")
            (internal / "python312.dll").write_bytes(b"runtime")
            built = builder.BuiltTool(
                spec=spec,
                payload_root=payload,
                executable=executable,
                executable_sha256=builder._sha256(executable),
                executable_size=executable.stat().st_size,
                payload_inventory=builder._payload_inventory(payload),
                help_runs=[{"run": run, "status": "PASS"} for run in range(1, 4)],
                archive_verification={"status": "PASS", "viewer_stdout_sha256": "b" * 64, "viewer_stderr_bytes": 0},
            )
        built_tools.append(built)
    monkeypatch.setattr(builder, "_git_value", lambda _root, *args: "a" * 40 if args[-1] == "HEAD" else "b" * 40)

    report = builder._publish_atomically(built_tools, destination=destination, repo_root=repo_root)

    assert report["status"] == "PASS"
    assert report["probe_policy"]["probe_count"] == 3
    assert report["probe_policy"]["help_timeout_seconds"] == 15.0
    assert all(tool["archive_verification"]["status"] == "PASS" for tool in report["tools"])
    assert (destination / "direct_sync_relay_runner.exe").is_file()
    assert (destination / "direct_sync_relay_install_pack" / "direct_sync_relay_install_pack.exe").is_file()
    assert (destination / "register_label_match_worker_pc.exe").is_file()
    assert (destination / builder.REPORT_NAME).is_file()
    assert not list(destination_parent.glob(".tools.release-stage-*"))


def test_existing_destination_is_never_merged_or_deleted(tmp_path):
    repo_root = tmp_path / "repo"
    repo_root.mkdir()
    destination = tmp_path / "dist" / "Label_Match" / "tools"
    destination.mkdir(parents=True)
    marker = destination / "keep.txt"
    marker.write_text("user", encoding="utf-8")

    with pytest.raises(builder.ReleaseCliBuildError, match="already exists"):
        builder._publish_atomically([], destination=destination, repo_root=repo_root)

    assert marker.read_text(encoding="utf-8") == "user"


def test_clean_checkout_is_required(monkeypatch, tmp_path):
    monkeypatch.setattr(builder, "_git_value", lambda *_args: " M user-owned.py")
    with pytest.raises(builder.ReleaseCliBuildError, match="clean Git checkout"):
        builder._verify_clean_checkout(tmp_path)


def test_authenticode_manifest_must_bind_exact_four_signed_executables(tmp_path, monkeypatch):
    package_root = tmp_path / "Label_Match"
    destination = package_root / "tools"
    destination.mkdir(parents=True)
    built_tools = []
    signer = "A" * 40
    timestamp = "B" * 40
    main_executable = package_root / "Label_Match.exe"
    main_executable.write_bytes(b"signed main executable")
    entries = [
        {
            "path": "Label_Match.exe",
            "size": main_executable.stat().st_size,
            "sha256": builder._sha256(main_executable),
            "status": "Valid",
            "signer_thumbprint": signer,
            "timestamp_thumbprint": timestamp,
        }
    ]
    for spec in builder.TOOL_SPECS:
        if spec.mode == "onefile":
            executable = destination / spec.executable_name
            executable.write_bytes(spec.name.encode())
            payload = executable
            relative = f"tools/{spec.executable_name}"
        else:
            payload = destination / spec.name
            payload.mkdir()
            executable = payload / spec.executable_name
            executable.write_bytes(spec.name.encode())
            relative = f"tools/{spec.name}/{spec.executable_name}"
        built = builder.BuiltTool(
            spec=spec,
            payload_root=payload,
            executable=executable,
            executable_sha256=builder._sha256(executable),
            executable_size=executable.stat().st_size,
            payload_inventory=builder._payload_inventory(payload),
            help_runs=[],
        )
        built_tools.append(built)
        entries.append(
            {
                "path": relative,
                "size": executable.stat().st_size,
                "sha256": built.executable_sha256,
                "status": "Valid",
                "signer_thumbprint": signer,
                "timestamp_thumbprint": timestamp,
            }
        )
    manifest = package_root / "authenticode-manifest.json"
    manifest.write_text(
        json.dumps({"status": "PASS", "signer_thumbprint": signer, "executables": entries}),
        encoding="utf-8",
    )
    monkeypatch.setattr(
        builder,
        "_read_authenticode_signature",
        lambda _path: {
            "status": "Valid",
            "signer_thumbprint": signer,
            "timestamp_thumbprint": timestamp,
        },
    )

    path, digest, verification = builder._verify_authenticode_manifest(
        destination,
        built_tools,
        expected_signer_thumbprint=signer,
    )
    assert path == manifest
    assert digest == builder._sha256(manifest)
    assert {entry["path"] for entry in verification} == {
        "Label_Match.exe",
        "tools/direct_sync_relay_runner.exe",
        "tools/direct_sync_relay_install_pack/direct_sync_relay_install_pack.exe",
        "tools/register_label_match_worker_pc.exe",
    }

    entries[0]["sha256"] = "f" * 64
    manifest.write_text(
        json.dumps({"status": "PASS", "signer_thumbprint": signer, "executables": entries}),
        encoding="utf-8",
    )
    with pytest.raises(builder.ReleaseCliBuildError, match="post-sign hash mismatch"):
        builder._verify_authenticode_manifest(
            destination,
            built_tools,
            expected_signer_thumbprint=signer,
        )


def test_authenticode_manifest_rejects_duplicate_paths_and_live_signature_mismatch(tmp_path, monkeypatch):
    package_root = tmp_path / "Label_Match"
    destination = package_root / "tools"
    destination.mkdir(parents=True)
    main_executable = package_root / "Label_Match.exe"
    main_executable.write_bytes(b"main")
    signer = "A" * 40
    timestamp = "B" * 40
    entries = [
        {
            "path": "Label_Match.exe",
            "size": 4,
            "sha256": builder._sha256(main_executable),
            "status": "Valid",
            "signer_thumbprint": signer,
            "timestamp_thumbprint": timestamp,
        }
    ]
    manifest = package_root / "authenticode-manifest.json"
    manifest.write_text(
        json.dumps({"status": "PASS", "signer_thumbprint": signer, "executables": entries + entries}),
        encoding="utf-8",
    )
    with pytest.raises(builder.ReleaseCliBuildError, match="duplicate path"):
        builder._verify_authenticode_manifest(destination, [], expected_signer_thumbprint=signer)

    manifest.write_text(
        json.dumps({"status": "PASS", "signer_thumbprint": signer, "executables": entries}),
        encoding="utf-8",
    )
    monkeypatch.setattr(
        builder,
        "_read_authenticode_signature",
        lambda _path: {
            "status": "HashMismatch",
            "signer_thumbprint": signer,
            "timestamp_thumbprint": timestamp,
        },
    )
    with pytest.raises(builder.ReleaseCliBuildError, match="live Authenticode status"):
        builder._verify_authenticode_manifest(destination, [], expected_signer_thumbprint=signer)

    with pytest.raises(builder.ReleaseCliBuildError, match="approved thumbprint"):
        builder._verify_authenticode_manifest(
            destination,
            [],
            expected_signer_thumbprint="C" * 40,
        )


def test_signing_script_uses_thumbprint_store_timestamp_and_four_exact_targets():
    script = (Path(__file__).resolve().parents[1] / "tools" / "sign_release_executables.ps1").read_text(
        encoding="utf-8"
    )
    assert "Import-PfxCertificate" not in script
    assert "PfxBase64" not in script
    assert "PfxPassword" not in script
    assert "Cert:\\CurrentUser\\My" in script
    assert "protected_current_user_certificate_store" in script
    assert "/sha1 $certificate.Thumbprint /s My /fd SHA256 /td SHA256 /tr $TimestampUrl" in script
    assert "Get-AuthenticodeSignature" in script
    assert "SignatureStatus]::Valid" in script
    assert "TimeStamperCertificate" in script
    assert "1.3.6.1.5.5.7.3.3" in script
    assert "ExpectedThumbprint" in script
    assert "direct_sync_relay_install_pack\\direct_sync_relay_install_pack.exe" in script
    assert "authenticode-manifest.json" in script
