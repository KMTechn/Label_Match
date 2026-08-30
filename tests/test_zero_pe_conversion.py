from __future__ import annotations

import ast
import hashlib
from pathlib import Path
import sys

import pytest

from kmtech_zero_pe import RasterImage
from phs_label_workflow import PHSLabelRenderer
from tools import build_portable_release_candidate as portable_builder


ROOT = Path(__file__).resolve().parents[1]
FORBIDDEN_ROOTS = {
    "PIL",
    "cffi",
    "charset_normalizer",
    "cryptography",
    "pygame",
}


def _sha256(relative_path: str) -> str:
    return hashlib.sha256((ROOT / relative_path).read_bytes()).hexdigest()


def _production_imports(roots: set[str]) -> list[tuple[str, str]]:
    paths = list(ROOT.glob("*.py"))
    for package in ("kmtech_factory_contracts", "kmtech_zero_pe", "ui"):
        paths.extend((ROOT / package).rglob("*.py"))
    matches: list[tuple[str, str]] = []
    for path in sorted(paths):
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                names = [alias.name for alias in node.names]
            elif isinstance(node, ast.ImportFrom) and node.module:
                names = [node.module]
            else:
                continue
            for name in names:
                if name.split(".", 1)[0] in roots:
                    matches.append((path.relative_to(ROOT).as_posix(), name))
    return matches


def test_shared_zero_pe_vendor_files_are_byte_pinned() -> None:
    assert _sha256("kmtech_zero_pe/cng_p256.py") == (
        "bd792c05e9f9c288469c92ecbdcdc088cc21dcfd7760c82ddcaa89ea48fc770b"
    )
    assert _sha256("kmtech_zero_pe/gdi_print.py") == (
        "48453e70a4bdd2008c2e4565bf647a852f319322458f9dc5a094a064274faece"
    )
    assert _sha256("kmtech_zero_pe/raster.py") == (
        "1296fc461e349cc02c1379b09096559203d2ec22cdc27c780958a05006d97c48"
    )
    assert _sha256("kmtech_zero_pe/release_signature.py") == (
        "ac21e2bca45899cd1161d89d4d2b6261ccb624bef745f88f5357c402e151cf1e"
    )


def test_native_package_imports_are_absent_from_production() -> None:
    assert _production_imports(FORBIDDEN_ROOTS) == []


def test_low_difficulty_native_dependencies_are_absent_from_runtime_requirements() -> None:
    requirements = [
        line.strip().lower()
        for line in (ROOT / "requirements.txt").read_text(encoding="utf-8").splitlines()
        if line.strip() and not line.lstrip().startswith("#")
    ]
    release = [
        line.strip().lower()
        for line in (ROOT / "requirements-release.txt")
        .read_text(encoding="utf-8")
        .splitlines()
        if line.strip() and not line.lstrip().startswith("#")
    ]
    for forbidden in ("cffi", "cryptography", "pygame", "pillow"):
        assert all(not line.startswith(forbidden) for line in requirements)
    assert any(line.startswith("charset-normalizer==3.4.9") for line in release)


def test_portable_builder_requires_an_empty_native_application_closure() -> None:
    assert portable_builder.EXPECTED_PYTHON == (3, 12, 10)
    assert portable_builder.ALLOWED_APP_NATIVE_NAMES == set()
    assert portable_builder.EXTERNAL_TOOL_MODULES == (
        "tools.label_auth_recovery_canary",
        "tools.label_legacy_task_quiescence",
    )
    for forbidden in ("cffi", "cryptography", "pillow", "pygame", "pycparser"):
        assert forbidden not in portable_builder.THIRD_PARTY


def test_portable_builder_derives_complete_tool_dependency_closure() -> None:
    sources = portable_builder._discover_portable_tool_sources(ROOT)
    relative = {path.relative_to(ROOT).as_posix() for path in sources}
    assert relative == {
        "tools/direct_sync_relay_runner.py",
        "tools/install_logistics_runtime_profile.py",
        "tools/label_auth_recovery_canary.py",
        "tools/label_legacy_task_quiescence.py",
        "tools/register_label_match_worker_pc.py",
    }
    native_imports: list[tuple[str, str]] = []
    for path in sources:
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                names = [alias.name for alias in node.names]
            elif isinstance(node, ast.ImportFrom) and node.module:
                names = [node.module]
            else:
                continue
            for name in names:
                if name.split(".", 1)[0] in FORBIDDEN_ROOTS:
                    native_imports.append((path.relative_to(ROOT).as_posix(), name))
    assert native_imports == []


def test_portable_tool_dependency_closure_is_recursive_and_fail_closed(
    tmp_path: Path,
) -> None:
    tools = tmp_path / "tools"
    tools.mkdir()
    entrypoint = tmp_path / "main.py"
    first = tools / "first.py"
    second = tools / "second.py"
    entrypoint.write_text("from tools import first\n", encoding="utf-8")
    first.write_text("from tools import second\n", encoding="utf-8")
    second.write_text("VALUE = 1\n", encoding="utf-8")

    discovered = portable_builder._discover_portable_tool_sources(
        tmp_path,
        initial_sources=[entrypoint],
        external_modules=(),
    )
    assert [path.name for path in discovered] == ["first.py", "second.py"]

    second.unlink()
    with pytest.raises(
        portable_builder.PortableBuildError,
        match="required portable tool module is missing: tools.second",
    ):
        portable_builder._discover_portable_tool_sources(
            tmp_path,
            initial_sources=[entrypoint],
            external_modules=(),
        )


def test_portable_runtime_import_probe_fails_closed_on_missing_module(
    tmp_path: Path,
) -> None:
    output = tmp_path / "packet"
    app = output / "app"
    tools = app / "tools"
    tools.mkdir(parents=True)
    (app / "current_user_onboarding.py").write_text("VALUE = 1\n", encoding="utf-8")
    (app / "label_match_product_host.py").write_text("VALUE = 1\n", encoding="utf-8")
    source_tools = tmp_path / "source" / "tools"
    source_tools.mkdir(parents=True)
    first = source_tools / "first.py"
    first.write_text("from tools import missing\n", encoding="utf-8")
    (tools / "first.py").write_bytes(first.read_bytes())

    with pytest.raises(
        portable_builder.PortableBuildError,
        match="portable runtime import closure failed",
    ):
        portable_builder._assert_portable_import_closure(
            output,
            tmp_path / "source",
            [first],
            python_executable=Path(sys.executable),
        )

    missing = source_tools / "missing.py"
    missing.write_text("VALUE = 1\n", encoding="utf-8")
    (tools / "missing.py").write_bytes(missing.read_bytes())
    portable_builder._assert_portable_import_closure(
        output,
        tmp_path / "source",
        [first, missing],
        python_executable=Path(sys.executable),
    )
    assert list(output.rglob("__pycache__")) == []


def test_portable_launcher_uses_pythonw_source_entrypoint() -> None:
    launcher = (ROOT / "portable" / "launch-label-match.cmd").read_text(
        encoding="utf-8"
    )
    assert "runtime\\pythonw.exe" in launcher
    assert "app\\main.py" in launcher
    assert "--focus" not in launcher


def test_frozen_builder_forces_source_only_charset_and_excludes_removed_packages() -> None:
    source = (ROOT / "tools" / "build_frozen_release_candidate.ps1").read_text(
        encoding="utf-8"
    )
    hook = (ROOT / "tools" / "pyinstaller_hooks" / "hook-charset_normalizer.py")
    assert "Initialize-NativeFreeOverrides" in source
    assert "Assert-LowRiskNativeFreePackage" in source
    assert '"--additional-hooks-dir", $nativeFreeOverrides.hook_root' in source
    assert '"--exclude-module", "PIL"' in source
    assert '"--exclude-module", "pygame"' in source
    assert '"--exclude-module", "charset_normalizer.md__mypyc"' in source
    assert '$mainArguments = @($nativeFreePyInstallerArguments) + @(' in source
    assert '"--exclude-module", "rpds"' in source
    assert source.count('"--exclude-module", "rpds"') == 1
    main_arguments = source.index(
        '$mainArguments = @($nativeFreePyInstallerArguments) + @('
    )
    rpds_exclusion = source.index('"--exclude-module", "rpds"')
    assert main_arguments < rpds_exclusion < source.index(
        '"--name", "Label_Match"', main_arguments
    )
    assert '"--hidden-import", "pygame"' not in source
    assert '"--hidden-import", "PIL"' not in source
    assert hook.read_text(encoding="utf-8").count("hiddenimports: list[str] = []") == 1


def test_gdi_phs_label_retains_legacy_pixel_dimensions(tmp_path: Path) -> None:
    rendered = PHSLabelRenderer(tmp_path).render(
        {
            "parsed": ["AAA2270730200"],
            "item_name_override": "ZERO PE TEST",
            "package_source_snapshot": {"member_count": 2},
        },
        {
            "label_id": "LBL-ZERO-PE-TEST",
            "qr_payload": (
                "PHS=2|SRC=KMTECH_INPUT_TAG|ITG=ITAG-ZERO-PE-TEST|"
                "CLC=AAA2270730200|LBL=LBL-ZERO-PE-TEST|HSH=dddddddddddddddd"
            ),
            "business_date": "2026-08-28",
            "worker_code": "2270730200-1",
            "item_id": "AAA2270730200",
            "member_count": 2,
        },
    )
    image = RasterImage.from_png(rendered.path)
    assert (image.width, image.height) == (1100, 600)
    assert rendered.sha256 == hashlib.sha256(Path(rendered.path).read_bytes()).hexdigest()
