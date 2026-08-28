from __future__ import annotations

import ast
import hashlib
from pathlib import Path

from kmtech_zero_pe import RasterImage
from phs_label_workflow import PHSLabelRenderer


ROOT = Path(__file__).resolve().parents[1]
REMOVED_ROOTS = {"PIL", "pygame", "charset_normalizer"}


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


def test_removed_native_package_imports_are_absent_from_production() -> None:
    assert _production_imports(REMOVED_ROOTS) == []


def test_cryptography_surface_is_preserved_for_the_followup_lane() -> None:
    assert _production_imports({"cryptography"}) == [
        ("Label_Match.py", "cryptography.exceptions"),
        ("Label_Match.py", "cryptography.hazmat.primitives.asymmetric.ed25519"),
        ("producer_runtime_client.py", "cryptography.hazmat.primitives.asymmetric"),
        ("terminal_operation_lease.py", "cryptography.exceptions"),
        ("terminal_operation_lease.py", "cryptography.hazmat.primitives"),
        ("terminal_operation_lease.py", "cryptography.hazmat.primitives.asymmetric"),
        (
            "terminal_operation_lease.py",
            "cryptography.hazmat.primitives.asymmetric.utils",
        ),
    ]


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
    for forbidden in ("pygame", "charset-normalizer"):
        assert all(not line.startswith(forbidden) for line in requirements)
        assert all(not line.startswith(forbidden) for line in release)
    assert any(line.startswith("chardet") for line in requirements)
    assert any(line.startswith("chardet==5.2.0") for line in release)


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
