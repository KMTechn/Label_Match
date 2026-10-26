"""Offline contracts from scanner keys to the existing barcode decoders."""
import ast
import json
from pathlib import Path
import re
import shutil
import subprocess


ROOT = Path(__file__).resolve().parents[1]
APP = "lm" if (ROOT / "Label_Match.py").exists() else "ca"
UI = ROOT / ("ui/parts" if APP == "lm" else "ui")


def node_result(*arguments):
    node = shutil.which("node")
    assert node, "the offline scanner contract requires Node"
    result = subprocess.run(
        [node, str(ROOT / "tests/check_scan_controls.mjs"), str(UI / "app/scan.js"), *arguments],
        capture_output=True, text=True, encoding="utf-8", check=False,
        creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0),
    )
    assert result.returncode == 0, "scanner VM contract failed"
    return json.loads(result.stdout)


def test_scanner_preserves_control_chords_and_native_editing():
    checks = node_result()
    assert checks and all(checks.values()), "a scanner control contract failed"


def test_scanner_output_reaches_existing_decoder():
    samples = node_result("--decoder-samples")
    assert len(samples) == 3
    if APP == "ca":
        from label_qr import parse_new_format_qr
        # CA has no GS/RS date decoder. Its current PHS2 decoder keeps internal bytes.
        for value in samples:
            parsed = parse_new_format_qr("PHS=2|NOTE=" + value)
            assert parsed is not None and parsed["NOTE"] == value
    else:
        from carrier_identity_port import parse_legacy_production_date
        tree = ast.parse((ROOT / "Label_Match.py").read_text(encoding="utf-8-sig"))
        nodes = [node for node in tree.body
                 if isinstance(node, ast.FunctionDef) and node.name == "_label_match_is_delivery_label_scan"
                 or isinstance(node, ast.Assign) and any(
                     isinstance(target, ast.Name) and target.id == "_LABEL_MATCH_DELIVERY_LABEL_HEADER"
                     for target in node.targets)]
        namespace = {"re": re}
        exec(compile(ast.Module(body=nodes, type_ignores=[]), "<delivery-detector>", "exec"), namespace)
        for value in samples:
            assert namespace["_label_match_is_delivery_label_scan"](value)
            assert parse_legacy_production_date(value) == "2026-10-26"
