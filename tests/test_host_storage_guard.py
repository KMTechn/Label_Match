"""A recreated marker and a native known-folder lookup cannot escape tests."""

import os
from pathlib import Path
import subprocess
import sys

import pytest

from tests import _host_storage_guard as guard


def test_unchanged_host_snapshot_has_no_changes(tmp_path):
    marker = tmp_path / "data-generation.json"
    marker.write_bytes(b"existing evidence")
    before = guard.storage_snapshot(tmp_path)
    assert guard.storage_changes(before, guard.storage_snapshot(tmp_path)) == {
        "new": [], "changed": [], "removed": []}


@pytest.mark.parametrize("change", ["new", "same-bytes-recreated"])
def test_host_snapshot_detects_a_new_or_recreated_marker(tmp_path, change):
    marker = tmp_path / "data-generation.json"
    if change != "new":
        marker.write_bytes(b"existing evidence")
    before = guard.storage_snapshot(tmp_path)
    if marker.exists():
        marker.unlink()
    marker.write_bytes(b"existing evidence")
    stamp = marker.stat().st_mtime_ns + 1000000000
    os.utime(marker, ns=(stamp, stamp))
    differences = guard.storage_changes(before, guard.storage_snapshot(tmp_path))
    assert differences["new" if change == "new" else "changed"]


def test_python_host_write_is_blocked_before_existing_bytes_change(tmp_path):
    marker = tmp_path / "data-generation.json"
    marker.write_bytes(b"existing evidence")
    code = """
import sys
from pathlib import Path
from tests._host_storage_guard import host_write_blocker
root = Path(sys.argv[1])
sys.addaudithook(host_write_blocker(root))
try:
    (root / 'data-generation.json').write_bytes(b'replacement')
except PermissionError:
    print('BLOCKED')
else:
    raise SystemExit(1)
"""
    result = subprocess.run([sys.executable, "-B", "-c", code, str(tmp_path)],
        capture_output=True, text=True, check=False,
        creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0))
    assert result.returncode == 0 and result.stdout.strip() == "BLOCKED"
    assert marker.read_bytes() == b"existing evidence"


@pytest.mark.parametrize("folder", ["CommonApplicationData", "LocalApplicationData", "Startup", "DesktopDirectory"])
@pytest.mark.parametrize("syntax", ["quoted", "enum", "quoted-option", "enum-option"])
def test_native_known_folder_lookup_uses_an_owned_test_root(tmp_path, monkeypatch, folder, syntax):
    monkeypatch.setattr(guard, "OWN_ROOT", tmp_path)
    expected = tmp_path / "native-data"
    quoted = str(expected).replace("'", "''")
    argument = ("[Environment+SpecialFolder]::" + folder if syntax.startswith("enum")
                else "'" + folder + "'")
    if syntax.endswith("option"):
        argument += ", [Environment+SpecialFolderOption]::DoNotVerify"
    code = ("$actual = [Environment]::GetFolderPath(" + argument + ")\n"
            "$expected = '" + quoted + "'\n"
            "if ($actual -ne $expected) { throw 'FOLDER_NOT_ISOLATED' }\n"
            "Write-Output 'ISOLATED'")
    environ = {"PROGRAMDATA": str(expected), "LOCALAPPDATA": str(expected)}
    if folder in {"Startup", "DesktopDirectory"}:
        expected = tmp_path / "known-folders" / folder.lower()
        code = code.replace(quoted, str(expected).replace("'", "''"))
    isolated = guard.isolate_known_folders(code, tmp_path, environ)
    script = tmp_path / "known-folder.ps1"
    script.write_text(isolated, encoding="utf-8-sig")
    result = subprocess.run(["powershell.exe", "-NoProfile", "-NonInteractive", "-File", str(script)],
        capture_output=True, text=True, check=False,
        creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0))
    assert result.returncode == 0 and result.stdout.strip() == "ISOLATED"
    assert "GetFolderPath" not in isolated


def test_unknown_native_folder_is_rejected_before_execution(tmp_path, monkeypatch):
    monkeypatch.setattr(guard, "OWN_ROOT", tmp_path)
    with pytest.raises(ValueError, match="Unreviewed native known folder"):
        guard.isolate_known_folders("[Environment]::GetFolderPath('UnknownFolder')", tmp_path, {})


def test_known_folder_cannot_select_another_root(tmp_path, monkeypatch):
    monkeypatch.setattr(guard, "OWN_ROOT", tmp_path)
    with pytest.raises(ValueError, match="escaped"):
        guard.isolate_known_folders("[Environment]::GetFolderPath('CommonApplicationData')",
            tmp_path, {"PROGRAMDATA": str(tmp_path.parent / "outside")})
