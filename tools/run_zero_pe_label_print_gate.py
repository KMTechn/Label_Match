#!/usr/bin/env python
"""Print one real Label_Match PHS label through the shared GDI adapter."""

from __future__ import annotations

import argparse
from dataclasses import asdict
import json
from pathlib import Path
import re
import sys


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from kmtech_zero_pe import (  # noqa: E402
    GdiPrinter,
    MarginsMM,
    PaperSpec,
    PrintSpec,
    RasterImage,
)
from phs_label_workflow import PHSLabelRenderer  # noqa: E402


TARGET_QR = (
    "PHS=2|SRC=KMTECH_INPUT_TAG|ITG=ITAG-ZERO-PE-PRINT|"
    "CLC=AAA2270730200|LBL=LBL-ZERO-PE-PRINT|HSH=cccccccccccccccc"
)
_MEDIA_BOX = re.compile(
    rb"/MediaBox\s*\[\s*([-+0-9.]+)\s+([-+0-9.]+)\s+"
    rb"([-+0-9.]+)\s+([-+0-9.]+)\s*\]"
)


def _pdf_page_mm(path: Path) -> list[float]:
    if path.stat().st_size > 10 * 1024 * 1024:
        raise RuntimeError("print evidence PDF exceeds the 10 MiB inspection bound")
    with path.open("rb") as handle:
        prefix = handle.read(256 * 1024)
    match = _MEDIA_BOX.search(prefix)
    if match is None:
        raise RuntimeError("print evidence PDF has no bounded MediaBox")
    x0, y0, x1, y1 = (float(value) for value in match.groups())
    return [round((x1 - x0) * 25.4 / 72.0, 3), round((y1 - y0) * 25.4 / 72.0, 3)]


def run(output_root: Path, printer_name: str) -> dict[str, object]:
    rendered = PHSLabelRenderer(output_root / "rendered").render(
        {
            "parsed": ["AAA2270730200"],
            "item_name_override": "ZERO PE PRINT FIXTURE",
            "package_source_snapshot": {"member_count": 2},
        },
        {
            "label_id": "LBL-ZERO-PE-PRINT",
            "qr_payload": TARGET_QR,
            "business_date": "2026-08-28",
            "worker_code": "2270730200-1",
            "item_id": "AAA2270730200",
            "member_count": 2,
        },
    )
    image_path = Path(rendered.path)
    image = RasterImage.from_png(image_path)
    pdf_path = output_root / "label-gdi-print.pdf"
    receipt = GdiPrinter(printer_name).print_png(
        image_path,
        PrintSpec(
            document_name="Label_Match zero-PE print gate",
            paper=PaperSpec(
                orientation="landscape",
                paper_size_code=9,
                driver_scale_percent=100,
            ),
            margins_mm=MarginsMM(left=12.0, top=12.0, right=12.0, bottom=12.0),
            content_scale_percent=100,
            output_path=pdf_path,
            overwrite_output=True,
        ),
    )
    page_mm = _pdf_page_mm(pdf_path)
    if not receipt.default_printer_unchanged or not receipt.default_devmode_unchanged:
        raise RuntimeError("printer defaults changed during the job")
    if receipt.copied_scanlines != image.height:
        raise RuntimeError("GDI did not copy every source scanline")
    if any(abs(actual - expected) > 0.6 for actual, expected in zip(page_mm, (297.0, 210.0))):
        raise RuntimeError(f"PDF is not A4 landscape: {page_mm}")
    return {
        "schema": "label-match-zero-pe-print-gate-v1",
        "rendered_png": str(image_path),
        "rendered_png_sha256": rendered.sha256,
        "source_dimensions_px": [image.width, image.height],
        "pdf_path": str(pdf_path),
        "pdf_page_mm": page_mm,
        "receipt": asdict(receipt),
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output-root", type=Path, required=True)
    parser.add_argument("--report", type=Path, required=True)
    parser.add_argument("--printer", default="Microsoft Print to PDF")
    args = parser.parse_args()
    args.output_root.mkdir(parents=True, exist_ok=True)
    result = run(args.output_root.resolve(), args.printer)
    args.report.parent.mkdir(parents=True, exist_ok=True)
    args.report.write_text(
        json.dumps(result, ensure_ascii=True, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
        newline="\n",
    )
    summary = {
        "printer": result["receipt"]["printer_name"],
        "job_id": result["receipt"]["job_id"],
        "source_dimensions_px": result["source_dimensions_px"],
        "pdf_page_mm": result["pdf_page_mm"],
        "defaults_unchanged": result["receipt"]["default_printer_unchanged"]
        and result["receipt"]["default_devmode_unchanged"],
    }
    print(json.dumps(summary, ensure_ascii=True, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
