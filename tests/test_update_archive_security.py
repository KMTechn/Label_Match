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


def test_safe_extract_update_zip_enforces_manifest_archive_policy(tmp_path):
    module = load_label_match_module()
    zip_path = tmp_path / "update.zip"
    destination = tmp_path / "extract"
    policy = {"top_level": "App", "required_files": ["App/App.exe"]}
    write_zip(zip_path, [("App/App.exe", b"binary"), ("App/config.json", b"{}")])

    with zipfile.ZipFile(zip_path, "r") as archive:
        module._safe_extract_update_zip(archive, destination, archive_policy=policy)

    assert (destination / "App" / "App.exe").read_bytes() == b"binary"


@pytest.mark.parametrize(
    ("members", "policy", "match"),
    [
        ([("Other/App.exe", b"binary")], {"top_level": "App", "required_files": ["App/App.exe"]}, "outside manifest top_level"),
        ([("App/readme.txt", b"notes")], {"top_level": "App", "required_files": ["App/App.exe"]}, "missing required file"),
        ([("App/App.exe", b"one"), ("app/app.exe", b"two")], {"required_files": ["App/App.exe"]}, "duplicate"),
        (
            [("App/App.exe", b"binary"), ("App/data", b"x"), ("App/data/file.txt", b"y")],
            {"top_level": "App", "required_files": ["App/App.exe"]},
            "collision",
        ),
        (
            [("App/App.exe", b"binary"), ("App/data/file.txt:Zone.Identifier", b"evil")],
            {"top_level": "App", "required_files": ["App/App.exe"]},
            "Unsafe update archive member",
        ),
        (
            [("App/App.exe", b"binary"), ("App/CON.txt", b"evil")],
            {"top_level": "App", "required_files": ["App/App.exe"]},
            "Unsafe update archive member",
        ),
        (
            [("App/App.exe", b"binary"), ("App/bad.", b"evil")],
            {"top_level": "App", "required_files": ["App/App.exe"]},
            "Unsafe update archive member",
        ),
        (
            [("App/App.exe", b"binary"), ("App/bad ", b"evil")],
            {"top_level": "App", "required_files": ["App/App.exe"]},
            "Unsafe update archive member",
        ),
        (
            [("App/App.exe", b"binary"), ("App/bad\tname.txt", b"evil")],
            {"top_level": "App", "required_files": ["App/App.exe"]},
            "Unsafe update archive member",
        ),
    ],
)
def test_safe_extract_update_zip_rejects_manifest_archive_policy_violations(tmp_path, members, policy, match):
    module = load_label_match_module()
    zip_path = tmp_path / "update.zip"
    write_zip(zip_path, members)

    with zipfile.ZipFile(zip_path, "r") as archive:
        with pytest.raises(ValueError, match=match):
            module._safe_extract_update_zip(archive, tmp_path / "extract", archive_policy=policy)
