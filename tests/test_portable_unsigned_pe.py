import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess

import pytest

from tools import build_portable_release_candidate as builder

ROOT = Path(__file__).resolve().parents[1]


def test_native_transition_digest_rejects_line_ending_changes_in_binary_bytes():
    from tools.refresh_first_update_declarations import digest
    original, altered = b"MZ\r\nreviewed", b"MZ\nreviewed"
    assert digest(original) != digest(altered)
    assert digest(original) == hashlib.sha256(original).hexdigest()
    assert digest(b"text\r\n") == digest(b"text\n")


@pytest.mark.parametrize("engine", ["powershell.exe", "pwsh.exe"])
def test_installers_accept_only_the_pinned_unsigned_natives(tmp_path, engine):
    host, portable = tmp_path / "host", tmp_path / "portable"
    (host / "tools").mkdir(parents=True)
    for name in ("tools/bootstrap_integrity.ps1", "INSTALL_CANONICAL_PORTABLE.ps1", "INSTALL_THIS_PC.ps1"):
        shutil.copyfile(ROOT / name, host / name)
    shutil.copytree(ROOT / "kmtech_shared", host / "kmtech_shared",
                    ignore=shutil.ignore_patterns("__pycache__", "*.pyc"))
    for name in ("kmtech_shared.manifest.json", "kmtech_shared.lock.json"):
        shutil.copyfile(ROOT / name, host / name)
    files = ("runtime/python.exe", "runtime/pythonw.exe", "runtime/python312.dll",
             "runtime/vcruntime140.dll", "runtime/vcruntime140_1.dll", "app/main.py",
             "INSTALL_CANONICAL_PORTABLE.ps1", "INSTALL_THIS_PC.ps1", "tools/bootstrap_integrity.ps1",
             "tools/label_writer_fence.ps1",
             "launch-label-match.cmd")
    for name in files:
        p = portable / name
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_bytes(b"fixture")
    manifest = {
        "schema": builder.PORTABLE_SCHEMA, "version": "v2.0.100", "source_commit": "a" * 40,
        "entrypoint": "runtime/pythonw.exe app/main.py", "launcher": "launch-label-match.cmd",
        "runtime_pythonw_sha256": hashlib.sha256(b"fixture").hexdigest(),
        "launcher_sha256": hashlib.sha256(b"fixture").hexdigest(),
        "allowed_unsigned_app_pe": [], "forbidden_package_roots": [],
        "file_count_before_manifest": len(files), "byte_count_before_manifest": len(files) * len(b"fixture"),
    }
    (portable / "portable-manifest.json").write_text(json.dumps(manifest), encoding="utf-8")
    executable = shutil.which(engine)
    assert executable
    result = subprocess.run([executable, "-NoLogo", "-NoProfile", "-NonInteractive", "-WindowStyle", "Hidden",
                             "-ExecutionPolicy", "Bypass", "-File", str(ROOT / "tests/portable_unsigned_pe.ps1"),
                             "-HostRoot", str(host), "-PortableRoot", str(portable)],
                            capture_output=True, text=True, creationflags=subprocess.CREATE_NO_WINDOW)
    evidence = tmp_path.parent.parent
    (evidence / (engine + ".unsigned-pe.stdout.log")).write_text(result.stdout, encoding="utf-8")
    (evidence / (engine + ".unsigned-pe.stderr.log")).write_text(result.stderr, encoding="utf-8")
    assert result.returncode == 0


def test_builder_fails_when_installer_builder_and_packet_unsigned_pins_disagree(tmp_path, monkeypatch):
    output, app = tmp_path / "out", tmp_path / "out/app"
    pins = {}
    for index, path in enumerate(sorted(builder.APP_NATIVE_UNSIGNED)):
        data = f"MZ synthetic {index}".encode()
        (app / path).parent.mkdir(parents=True, exist_ok=True)
        (app / path).write_bytes(data)
        pins[path] = (len(data), hashlib.sha256(data).hexdigest())
    monkeypatch.setattr(builder, "APP_NATIVE_ALLOWLIST", {**builder.APP_NATIVE_ALLOWLIST, **pins})
    block = "".join(f"    '{path}' = '{sha}'\n" for path, (_size, sha) in pins.items())
    for name in ("INSTALL_CANONICAL_PORTABLE.ps1", "INSTALL_THIS_PC.ps1"):
        (output / name).write_text("$unsignedPe = @{\n" + block + "}\n", encoding="utf-8-sig")
    builder._assert_installer_unsigned_pins(output, app)
    first = sorted(pins)[0]
    source = (output / "INSTALL_THIS_PC.ps1").read_text(encoding="utf-8-sig")
    (output / "INSTALL_THIS_PC.ps1").write_text(source.replace(pins[first][1], "0" * 64), encoding="utf-8-sig")
    with pytest.raises(builder.PortableBuildError):
        builder._assert_installer_unsigned_pins(output, app)
    (output / "INSTALL_THIS_PC.ps1").write_text("$unsignedPe = @{\n" + block + "}\n", encoding="utf-8-sig")
    (app / first).write_bytes(b"MZ changed")
    with pytest.raises(builder.PortableBuildError):
        builder._assert_installer_unsigned_pins(output, app)


def test_native_inventory_rejects_added_missing_and_changed_pe(tmp_path, monkeypatch):
    pins = {}
    for index, path in enumerate(builder.APP_NATIVE_ALLOWLIST):
        data = f"MZ native {index}".encode()
        (tmp_path / path).parent.mkdir(parents=True, exist_ok=True)
        (tmp_path / path).write_bytes(data)
        pins[path] = (len(data), hashlib.sha256(data).hexdigest())
    monkeypatch.setattr(builder, "APP_NATIVE_ALLOWLIST", pins)
    assert set(builder._app_native_inventory(tmp_path)) == set(pins)
    extra = tmp_path / "other.dll"
    extra.write_bytes(b"MZ unreviewed")
    with pytest.raises(builder.PortableBuildError):
        builder._app_native_inventory(tmp_path)
    extra.unlink()
    first = tmp_path / next(iter(pins))
    original = first.read_bytes()
    first.write_bytes(original + b"changed")
    with pytest.raises(builder.PortableBuildError):
        builder._app_native_inventory(tmp_path)
    first.unlink()
    with pytest.raises(builder.PortableBuildError):
        builder._app_native_inventory(tmp_path)
