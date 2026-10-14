"""The runtime keeps QR encoding while excluding development payloads."""
from pathlib import Path
import sys

from tools import build_portable_release_candidate as builder


def test_third_party_copy_excludes_qrcode_tests_and_keeps_encoder(tmp_path, monkeypatch):
    provider = tmp_path / "provider"
    package = provider / "qrcode"
    (package / "tests").mkdir(parents=True)
    (package / "tests/test_main.py").write_text("", encoding="utf-8")
    (package / "main.py").write_text("", encoding="utf-8")
    metadata = provider / "qrcode.dist-info"
    metadata.mkdir()
    monkeypatch.setattr(builder, "THIRD_PARTY", {"qrcode": ("8.2", ("qrcode",))})
    monkeypatch.setattr(builder, "SHELL_RUNTIME_SITE_PACKAGES", provider)
    monkeypatch.setattr(builder, "_distribution_root", lambda *a: (provider, metadata))
    target = tmp_path / "site-packages"
    builder._copy_third_party(target)
    assert (target / "qrcode/main.py").is_file()
    assert not (target / "qrcode/tests").exists()


def test_runtime_copy_excludes_venv_and_keeps_stdlib(tmp_path, monkeypatch):
    provider = tmp_path / "python"
    (provider / "DLLs").mkdir(parents=True)
    (provider / "tcl").mkdir()
    (provider / "Lib/venv/scripts").mkdir(parents=True)
    (provider / "Lib/venv/__init__.py").write_text("", encoding="utf-8")
    (provider / "Lib/json").mkdir()
    (provider / "Lib/json/__init__.py").write_text("", encoding="utf-8")
    for name in builder.RUNTIME_ROOT_FILES:
        (provider / name).write_bytes(b"")
    monkeypatch.setattr(builder, "EXPECTED_PYTHON", tuple(sys.version_info[:3]))
    monkeypatch.setattr(builder.platform, "architecture", lambda: ("64bit", ""))
    target = tmp_path / "runtime"
    builder._runtime_source(provider, target)
    assert (target / "Lib/json/__init__.py").is_file()
    assert not (target / "Lib/venv").exists()
