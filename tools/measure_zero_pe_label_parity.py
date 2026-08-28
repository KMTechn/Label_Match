#!/usr/bin/env python
"""Compare the pre-conversion Pillow label with the shared GDI renderer."""

from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
import subprocess
import sys
import types


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from kmtech_zero_pe import RasterImage  # noqa: E402
from phs_label_workflow import PHSLabelRenderer  # noqa: E402


TARGET_QR = (
    "PHS=2|SRC=KMTECH_INPUT_TAG|ITG=ITAG-ZERO-PE-LABEL|"
    "CLC=AAA2270730200|LBL=LBL-ZERO-PE|HSH=bbbbbbbbbbbbbbbb"
)


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _content_metrics(path: Path) -> dict[str, object]:
    image = RasterImage.from_png(path)
    xs: list[int] = []
    ys: list[int] = []
    nonwhite = 0
    for y in range(image.height):
        for x in range(image.width):
            offset = (y * image.width + x) * 4
            blue, green, red, _alpha = image.bgra[offset : offset + 4]
            if (red, green, blue) != (255, 255, 255):
                xs.append(x)
                ys.append(y)
                nonwhite += 1
    return {
        "width_px": image.width,
        "height_px": image.height,
        "nonwhite_pixels": nonwhite,
        "content_bbox_px": [min(xs), min(ys), max(xs) + 1, max(ys) + 1],
        "sha256": _sha256(path),
        "bytes": path.stat().st_size,
    }


def _baseline_renderer(ref: str):
    completed = subprocess.run(
        ["git", "show", f"{ref}:phs_label_workflow.py"],
        cwd=ROOT,
        capture_output=True,
        timeout=30,
        check=False,
    )
    if completed.returncode != 0:
        raise RuntimeError(f"baseline source is unavailable at {ref}")
    module_name = "_label_match_pillow_baseline"
    module = types.ModuleType(module_name)
    module.__file__ = f"git:{ref}:phs_label_workflow.py"
    sys.modules[module_name] = module
    exec(compile(completed.stdout, module.__file__, "exec"), module.__dict__)
    return module.PHSLabelRenderer


def measure(output_root: Path, baseline_ref: str) -> dict[str, object]:
    current_set = {
        "parsed": ["AAA2270730200"],
        "item_name_override": "ZERO PE DIMENSION FIXTURE",
        "package_source_snapshot": {"member_count": 2},
    }
    target = {
        "label_id": "LBL-ZERO-PE",
        "qr_payload": TARGET_QR,
        "business_date": "2026-08-28",
        "worker_code": "2270730200-1",
        "item_id": "AAA2270730200",
        "member_count": 2,
    }
    pillow_path = Path(
        _baseline_renderer(baseline_ref)(output_root / "pillow").render(
            current_set, target
        ).path
    )
    gdi_path = Path(PHSLabelRenderer(output_root / "gdi").render(current_set, target).path)
    pillow = _content_metrics(pillow_path)
    gdi = _content_metrics(gdi_path)
    dimensions_equal = (
        pillow["width_px"] == gdi["width_px"] == 1100
        and pillow["height_px"] == gdi["height_px"] == 600
    )
    if not dimensions_equal:
        raise RuntimeError(f"label dimensions differ: pillow={pillow}, gdi={gdi}")
    return {
        "schema": "label-match-zero-pe-label-parity-v1",
        "baseline_ref": baseline_ref,
        "pillow": {**pillow, "path": str(pillow_path)},
        "gdi": {**gdi, "path": str(gdi_path)},
        "dimensions_equal": dimensions_equal,
        "expected_dimensions_px": [1100, 600],
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output-root", type=Path, required=True)
    parser.add_argument("--report", type=Path, required=True)
    parser.add_argument("--baseline-ref", default="HEAD")
    args = parser.parse_args()
    result = measure(args.output_root.resolve(), args.baseline_ref)
    args.report.parent.mkdir(parents=True, exist_ok=True)
    args.report.write_text(
        json.dumps(result, ensure_ascii=True, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
        newline="\n",
    )
    print(json.dumps(result, ensure_ascii=True, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
