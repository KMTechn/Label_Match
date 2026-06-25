import importlib.util
import zipfile
from pathlib import Path

import pytest


def load_label_match_module():
    module_path = Path(__file__).resolve().parents[1] / "Label_Match.py"
    spec = importlib.util.spec_from_file_location("label_match_update_test", module_path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def write_zip(path, members):
    with zipfile.ZipFile(path, "w") as archive:
        for name, content in members:
            archive.writestr(name, content)


def test_safe_extract_update_zip_accepts_nested_relative_members(tmp_path):
    module = load_label_match_module()
    zip_path = tmp_path / "update.zip"
    destination = tmp_path / "extract"
    write_zip(zip_path, [("Label_Match/app.exe", b"binary")])

    with zipfile.ZipFile(zip_path, "r") as archive:
        module._safe_extract_update_zip(archive, destination)

    assert (destination / "Label_Match" / "app.exe").read_bytes() == b"binary"


@pytest.mark.parametrize(
    "member_name",
    [
        "../outside.txt",
        "Label_Match/../../outside.txt",
        "Label_Match\\..\\outside.txt",
        "/absolute/outside.txt",
        "C:/outside.txt",
    ],
)
def test_safe_extract_update_zip_rejects_path_traversal_members(tmp_path, member_name):
    module = load_label_match_module()
    zip_path = tmp_path / "update.zip"
    destination = tmp_path / "extract"
    write_zip(zip_path, [(member_name, b"evil")])

    with zipfile.ZipFile(zip_path, "r") as archive:
        with pytest.raises(ValueError, match="Unsafe update archive member"):
            module._safe_extract_update_zip(archive, destination)

    assert not (tmp_path / "outside.txt").exists()
    assert not (destination / "outside.txt").exists()


def test_safe_extract_update_zip_rejects_symlink_members(tmp_path):
    module = load_label_match_module()
    zip_path = tmp_path / "update.zip"
    info = zipfile.ZipInfo("Label_Match/link")
    info.external_attr = 0o120777 << 16
    with zipfile.ZipFile(zip_path, "w") as archive:
        archive.writestr(info, "target")

    with zipfile.ZipFile(zip_path, "r") as archive:
        with pytest.raises(ValueError, match="Unsafe update archive member"):
            module._safe_extract_update_zip(archive, tmp_path / "extract")
