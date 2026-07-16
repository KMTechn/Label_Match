from __future__ import annotations

import copy
import csv
import json
import os
from pathlib import Path

import pytest
from PIL import Image, ImageDraw

from tools import label_match_actual_input_walkthrough as walkthrough


def test_synthetic_fixture_is_real_five_step_parser_input():
    fixture = walkthrough.synthetic_fixture("UNIT-MARKER")

    for scenario in ("happy", "duplicate", "mismatch", "recovery"):
        values = fixture[scenario]
        assert len(values) == 5
        master = values[0]
        assert master.startswith("VALID-")
        assert all(master in value for value in values[1:])
        assert "<GS>6D" in values[-1]

    assert fixture["mismatch"][0] not in fixture["wrong_product"]
    assert len(fixture["wrong_product"]) > len(fixture["mismatch"][0])


def test_child_environment_removes_integrations_and_redirects_runtime_roots(tmp_path):
    data_dir = tmp_path / "isolated_data"
    original = {
        "PATH": os.environ.get("PATH", ""),
        "LABEL_MATCH_LOGISTICS_TOKEN": "secret",
        "LABEL_MATCH_DIRECT_SYNC_SERVER_BASE_URL": "https://production.invalid",
        "LABEL_MATCH_UPDATE_TOKEN": "update-secret",
        "WORKER_ANALYSIS_LOGISTICS_URL": "https://production.invalid",
        "WORKER_ANALYSIS_SERVER_URL": "https://production.invalid",
        "PYTEST_CURRENT_TEST": "must-not-leak",
    }

    child = walkthrough.build_child_environment(original, data_dir, "ENV-TEST")

    assert original["LABEL_MATCH_LOGISTICS_TOKEN"] == "secret"
    assert "LABEL_MATCH_LOGISTICS_TOKEN" not in child
    assert "LABEL_MATCH_UPDATE_TOKEN" not in child
    assert "WORKER_ANALYSIS_LOGISTICS_URL" not in child
    assert "WORKER_ANALYSIS_SERVER_URL" not in child
    assert "PYTEST_CURRENT_TEST" not in child
    assert child["LABEL_MATCH_AUDIO_ENABLED"] == "off"
    assert child["LABEL_MATCH_DIRECT_SYNC_BOOTSTRAP"] == "off"
    assert child["LABEL_MATCH_SESSION_SYNC_TRIGGER"] == "off"
    assert child["LABEL_MATCH_UPDATE_PROVIDER"] == "off"
    assert Path(child["LABEL_MATCH_SAVE_DIR"]).resolve() == data_dir.resolve()
    for key in ("ProgramData", "LOCALAPPDATA", "APPDATA", "TEMP", "TMP"):
        assert Path(child[key]).resolve().is_relative_to(data_dir.resolve())


def _valid_aggregate_report(tmp_path: Path) -> dict:
    screenshots = []
    for index, screenshot_name in enumerate(walkthrough.EXPECTED_SCREENSHOT_NAMES):
        path = tmp_path / f"screen-{index}.png"
        path.write_bytes(f"synthetic screenshot {index}".encode("utf-8"))
        screenshots.append(
            {
                "name": screenshot_name,
                "path": str(path),
                "sha256": walkthrough._sha256_file(path),
                "hwnd_pid_matches_process": True,
                "capture_pixels_valid": True,
                "blank_suspected": False,
                "excess_black_suspected": False,
                "edge_black_stripe_suspected": False,
                "contiguous_black_stripe_suspected": False,
                "black_tile_suspected": False,
                "uniform_low_variance_suspected": False,
                "near_black_ratio": 0.01,
                "width": 1366,
                "height": 768,
                "pixel_size_matches": True,
            }
        )

    phases = {}
    for phase_name, pid in (("phase_one", 101), ("phase_two", 202)):
        phases[phase_name] = {
            "status": "PASS",
            "pid": pid,
            "checks": [
                {"name": name, "status": "PASS"}
                for name in sorted(walkthrough.REQUIRED_PHASE_CHECKS[phase_name])
            ],
        }
    return {
        **phases,
        "process_input_call_count": 15,
        "event_counts": dict(walkthrough.EXPECTED_EVENT_COUNTS),
        "tray_result_counts": dict(walkthrough.EXPECTED_TRAY_RESULT_COUNTS),
        "state_file_exists_after_phase_two": False,
        "git_status_unchanged": True,
        "all_artifacts_below_output": True,
        "screenshots": screenshots,
        "geometry": "1366x768+0+0",
        "expected_screenshot_names": list(walkthrough.EXPECTED_SCREENSHOT_NAMES),
        "expected_screenshot_size": [1366, 768],
    }


def _remove_phase_check(report: dict, phase_name: str, check_name: str) -> None:
    report[phase_name]["checks"] = [
        item
        for item in report[phase_name]["checks"]
        if item["name"] != check_name
    ]


def test_aggregate_report_verifier_accepts_complete_contract(tmp_path):
    report = _valid_aggregate_report(tmp_path)

    assert walkthrough.report_issue_codes(report) == []


@pytest.mark.parametrize(
    ("mutation", "expected_issue"),
    [
        (
            lambda report: report["phase_two"].update(
                {"pid": report["phase_one"]["pid"]}
            ),
            "RESTART_PID_NOT_CHANGED",
        ),
        (
            lambda report: report.update({"process_input_call_count": 14}),
            "PROCESS_INPUT_CALL_COUNT_MISMATCH",
        ),
        (
            lambda report: report.update(
                {"tray_result_counts": {"통과": 4}}
            ),
            "TRAY_RESULT_SEMANTICS_MISMATCH",
        ),
        (
            lambda report: _remove_phase_check(
                report,
                "phase_one",
                "partial_state_contains_two_real_scans",
            ),
            "REQUIRED_CHECK_PHASE_ONE_PARTIAL_STATE_CONTAINS_TWO_REAL_SCANS_MISSING_OR_FAILED",
        ),
        (
            lambda report: _remove_phase_check(
                report,
                "phase_two",
                "external_runtime_trace_unchanged",
            ),
            "REQUIRED_CHECK_PHASE_TWO_EXTERNAL_RUNTIME_TRACE_UNCHANGED_MISSING_OR_FAILED",
        ),
        (
            lambda report: report.update({"all_artifacts_below_output": False}),
            "ARTIFACT_ESCAPED_OUTPUT_ROOT",
        ),
        (
            lambda report: report["screenshots"][3].update(
                {"blank_suspected": True}
            ),
            "SCREENSHOT_INVALID_04_MISMATCH_WARNING",
        ),
        (
            lambda report: report["screenshots"][3].update(
                {"name": "unexpected-name"}
            ),
            "SCREENSHOT_NAME_SET_MISMATCH",
        ),
        (
            lambda report: report["screenshots"][3].update(
                {"sha256": report["screenshots"][0]["sha256"]}
            ),
            "SCREENSHOT_SHA256_NOT_UNIQUE",
        ),
        (
            lambda report: report["screenshots"][3].update({"width": 1280}),
            "SCREENSHOT_INVALID_04_MISMATCH_WARNING",
        ),
    ],
)
def test_aggregate_report_verifier_fails_closed(tmp_path, mutation, expected_issue):
    report = _valid_aggregate_report(tmp_path)
    mutation(report)

    assert expected_issue in walkthrough.report_issue_codes(report)


def test_tray_result_counts_distinguish_pass_input_error_and_mismatch(tmp_path):
    log_path = tmp_path / "포장실작업이벤트로그_SYNTH_20260716.csv"
    with log_path.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=("timestamp", "worker_name", "event", "details"),
        )
        writer.writeheader()
        for result in ("통과", "입력오류", "불일치", "통과"):
            writer.writerow(
                {
                    "timestamp": "2026-07-16T09:00:00",
                    "worker_name": "synthetic",
                    "event": "TRAY_COMPLETE",
                    "details": json.dumps({"final_result": result}, ensure_ascii=False),
                }
            )

    assert walkthrough._tray_result_counts(tmp_path) == {
        "불일치": 1,
        "입력오류": 1,
        "통과": 2,
    }


def _valid_ui_like_image() -> Image.Image:
    image = Image.new("RGB", (480, 320), "white")
    draw = ImageDraw.Draw(image)
    draw.rectangle((12, 12, 150, 60), fill="#3B82F6")
    draw.rectangle((170, 12, 310, 60), fill="#FEE2E2", outline="#B91C1C")
    draw.rectangle((12, 85, 468, 305), fill="#F3F4F6", outline="#D1D5DB")
    draw.line((30, 115, 440, 115), fill="#6B7280", width=2)
    return image


def test_capture_pixel_gate_accepts_ui_like_frame():
    metrics = walkthrough.analyze_capture_image(
        _valid_ui_like_image(), expected_size=(480, 320)
    )

    assert metrics["capture_pixels_valid"] is True
    assert metrics["near_black_ratio"] < walkthrough.NEAR_BLACK_FAILURE_RATIO
    assert metrics["black_tile_suspected"] is False
    assert metrics["edge_black_stripe_suspected"] is False


def test_capture_pixel_gate_rejects_thirty_percent_edge_stripe():
    image = _valid_ui_like_image()
    ImageDraw.Draw(image).rectangle((0, 0, int(image.width * 0.30), image.height), fill="black")

    metrics = walkthrough.analyze_capture_image(image, expected_size=image.size)

    assert metrics["capture_pixels_valid"] is False
    assert metrics["excess_black_suspected"] is True
    assert metrics["edge_black_stripe_suspected"] is True
    assert metrics["longest_near_black_column_run_ratio"] >= 0.30


def test_capture_pixel_gate_rejects_local_black_tile_without_global_excess():
    image = _valid_ui_like_image()
    tile_width = image.width // walkthrough.TILE_COLUMNS
    tile_height = image.height // walkthrough.TILE_ROWS
    ImageDraw.Draw(image).rectangle(
        (tile_width * 5, tile_height * 3, tile_width * 6 - 1, tile_height * 4 - 1),
        fill="black",
    )

    metrics = walkthrough.analyze_capture_image(image, expected_size=image.size)

    assert metrics["near_black_ratio"] < walkthrough.NEAR_BLACK_FAILURE_RATIO
    assert metrics["black_tile_suspected"] is True
    assert metrics["capture_pixels_valid"] is False


def test_capture_pixel_gate_rejects_uniform_gray_frame():
    metrics = walkthrough.analyze_capture_image(
        Image.new("RGB", (480, 320), "#808080"),
        expected_size=(480, 320),
    )

    assert metrics["blank_suspected"] is True
    assert metrics["uniform_low_variance_suspected"] is True
    assert metrics["capture_pixels_valid"] is False


@pytest.mark.skipif(os.name != "nt", reason="Label_Match is a Windows Tk application")
def test_tk_return_event_is_delivered_to_the_bound_entry():
    import tkinter as tk
    from tkinter import ttk

    root = tk.Tk()
    calls = []
    try:
        root.geometry("320x120+0+0")
        entry = ttk.Entry(root)
        entry.pack(fill="x")

        def process_input(event):
            calls.append(
                {
                    "widget": event.widget,
                    "value": entry.get(),
                    "event_type": str(event.type),
                }
            )
            entry.delete(0, "end")
            return "break"

        entry.bind("<Return>", process_input)
        root.update()
        entry.focus_force()
        entry.insert(0, "SYNTHETIC-BARCODE")
        entry.event_generate("<Return>", when="tail")
        walkthrough._pump(root, 120)

        assert calls == [
            {
                "widget": entry,
                "value": "SYNTHETIC-BARCODE",
                "event_type": "2",
            }
        ]
        assert entry.get() == ""
    finally:
        root.destroy()
