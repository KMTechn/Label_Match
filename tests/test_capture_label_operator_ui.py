from __future__ import annotations

import argparse
import ctypes
import importlib
import importlib.abc
import importlib.machinery
import importlib.util
import os
import py_compile
import subprocess
import sys
from dataclasses import asdict
from pathlib import Path
from types import SimpleNamespace

import pytest
from PIL import Image

from tools import capture_label_operator_ui as capture
from tools.capture_label_operator_ui import (
    AUTHORITATIVE_CAPTURE_SOURCE,
    CANCEL_BUTTON_ALIASES,
    CANCELLATION_CONFLICT_COUNT,
    CANCELLATION_CONFLICT_MESSAGE,
    CANCELLATION_CONFLICT_TITLE,
    CANCELLATION_SURFACE_CAPTURE_CONTRACT,
    CAPTURE_MANIFEST_SCHEMA_VERSION,
    DEFAULT_SCALE,
    DEFAULT_SIZES,
    DEFAULT_STATE_IDS,
    MAX_SCALE,
    MIN_SCALE,
    REQUIRED_WIDGET_ATTRS,
    TARGET_DISPLAY_DEVICE,
    TARGET_DISPLAY_DPI,
    TARGET_DISPLAY_MONITOR_AREA,
    TARGET_DISPLAY_WORK_AREA,
    analyze_image,
    apply_state_fixture,
    apply_cross_capture_contracts,
    assert_descendant,
    build_parser,
    build_presenter_view,
    build_state_fixtures,
    compare_layout_signatures,
    evaluate_capture,
    evaluate_clipping_proxy,
    evaluate_text_clipping_proxy,
    evaluate_tree_text_fit_proxy,
    evaluate_tree_detail_partition,
    evaluate_middle_ellipsis_fit,
    expected_scan_tree_mapping,
    expected_presenter_rows,
    parse_scale,
    parse_sizes,
    parse_states,
    parse_work_area,
    resolve_capture_monitor,
    settle_responsive_layout,
    validate_exact_rows,
    validate_live_contract,
    validate_presenter_rows,
    validate_capture_matrix_request,
    validate_cancellation_surface_capture_contract,
    validate_qa_detail_contract,
    validate_window_capture_pair,
    validate_root_only_toplevels,
)


def test_default_capture_matrix_covers_required_sizes_states_and_scale():
    assert DEFAULT_SIZES == (
        (1366, 768),
        (1440, 900),
        (1920, 1080),
        (2560, 1080),
        (2560, 1392),
    )
    assert DEFAULT_STATE_IDS == (
        "waiting",
        "qa_master",
        "exact_first",
        "exact_active",
        "exact_complete",
        "qa_progress",
        "qa_product_2",
        "qa_product_3",
        "cancellation_conflict",
        "sealed",
        "error",
        "full_complete",
        "partial_complete",
        "recovery",
        "history_readonly",
        "submission_blocked",
    )
    assert DEFAULT_SCALE == 1.0


def test_cli_parsers_validate_deduplicate_and_keep_korean_multiplication_mark():
    assert parse_sizes("1366×768,1440x900,1366x768") == (
        (1366, 768),
        (1440, 900),
    )
    assert parse_states("waiting,error,waiting") == ("waiting", "error")
    assert parse_scale(str(MIN_SCALE)) == MIN_SCALE
    assert parse_scale(str(MAX_SCALE)) == MAX_SCALE
    args = build_parser().parse_args([])
    assert args.scale == DEFAULT_SCALE
    assert args.display_device == TARGET_DISPLAY_DEVICE
    assert args.work_area == TARGET_DISPLAY_WORK_AREA
    assert args.output_root.resolve().is_relative_to(capture.CAPTURE_OUTPUT_BASE)
    assert not args.output_root.resolve().is_relative_to(capture.ROOT)
    assert parse_work_area("693,-1440,3253,-48") == TARGET_DISPLAY_WORK_AREA

    for value in ("800x600", "wide", "1366x"):
        with pytest.raises(argparse.ArgumentTypeError):
            parse_sizes(value)
    with pytest.raises(argparse.ArgumentTypeError):
        parse_states("not-a-state")
    for value in ("0.69", "2.51", "nan", "inf", "large", True):
        with pytest.raises(argparse.ArgumentTypeError):
            parse_scale(value)
    for value in ("0,0,800,600", "693,-1440,3253", "bad"):
        with pytest.raises(argparse.ArgumentTypeError):
            parse_work_area(value)


def test_programmatic_matrix_requires_all_five_sizes_all_sixteen_states_once():
    sizes, states = validate_capture_matrix_request(
        tuple(reversed(DEFAULT_SIZES)), tuple(reversed(DEFAULT_STATE_IDS))
    )
    assert set(sizes) == set(DEFAULT_SIZES)
    assert set(states) == set(DEFAULT_STATE_IDS)

    with pytest.raises(RuntimeError, match="sizes contain programmatic duplicates"):
        validate_capture_matrix_request(
            (*DEFAULT_SIZES[:-1], DEFAULT_SIZES[0]), DEFAULT_STATE_IDS
        )
    with pytest.raises(RuntimeError, match="states contain programmatic duplicates"):
        validate_capture_matrix_request(
            DEFAULT_SIZES, (*DEFAULT_STATE_IDS[:-1], DEFAULT_STATE_IDS[0])
        )
    with pytest.raises(RuntimeError, match="every DEFAULT_SIZES"):
        validate_capture_matrix_request(DEFAULT_SIZES[:-1], DEFAULT_STATE_IDS)
    with pytest.raises(RuntimeError, match="every DEFAULT_STATE_IDS"):
        validate_capture_matrix_request(DEFAULT_SIZES, DEFAULT_STATE_IDS[:-1])


def test_manifest_contract_captures_only_persistent_cancellation_conflict():
    cancellation = validate_cancellation_surface_capture_contract(
        DEFAULT_STATE_IDS
    )
    assert cancellation == {
        "status": "PASS",
        "persistent_capture_states": ["cancellation_conflict"],
        "modal_only_excluded_states": [
            "cancellation_acked",
            "cancellation_pending",
        ],
        "extra_visible_toplevels_allowed": False,
    }
    for metadata in CANCELLATION_SURFACE_CAPTURE_CONTRACT[
        "modal_only_exclusions"
    ].values():
        assert "nonpersistent" in metadata["runtime_surface"]
        assert "messagebox" in metadata["runtime_surface"]
        assert "root-only PrintWindow" in metadata["reason"]
    assert CAPTURE_MANIFEST_SCHEMA_VERSION == 4
    with pytest.raises(RuntimeError, match="cannot be persistent captures"):
        validate_cancellation_surface_capture_contract(
            (*DEFAULT_STATE_IDS, "cancellation_pending")
        )


def test_state_fixtures_preserve_qa_exact_and_last_normal_contracts():
    fixtures = {fixture.state_id: fixture for fixture in build_state_fixtures()}

    assert tuple(fixtures) == DEFAULT_STATE_IDS
    assert fixtures["waiting"].qa_scans == ()
    assert len(fixtures["qa_master"].qa_scans) == 1
    assert len(fixtures["qa_progress"].qa_scans) == 2
    assert len(fixtures["qa_product_2"].qa_scans) == 3
    assert len(fixtures["qa_product_3"].qa_scans) == 4
    assert (
        fixtures["cancellation_conflict"].qa_scans
        == fixtures["qa_product_3"].qa_scans
    )
    assert fixtures["cancellation_conflict"].selected_qa_index == 4
    assert (
        fixtures["cancellation_conflict"].last_normal_scan
        == fixtures["qa_product_3"].last_normal_scan
    )
    assert fixtures["error"].qa_scans == fixtures["qa_product_3"].qa_scans
    assert fixtures["error"].last_normal_scan == fixtures["qa_product_3"].last_normal_scan
    assert fixtures["error"].has_error is True
    assert len(fixtures["exact_first"].exact_barcodes) == 1
    assert fixtures["exact_active"].exact_active is True
    assert len(fixtures["exact_active"].exact_barcodes) < fixtures["exact_active"].exact_target
    assert fixtures["exact_complete"].exact_complete is True
    assert len(fixtures["exact_complete"].exact_barcodes) == fixtures["exact_complete"].exact_target
    assert fixtures["exact_complete"].exact_target == 3
    assert fixtures["qa_progress"].exact_complete is True
    assert (
        fixtures["qa_progress"].exact_barcodes
        == fixtures["exact_complete"].exact_barcodes
    )
    assert fixtures["sealed"].sealed_transfer is True
    assert fixtures["full_complete"].completion_kind == "full"
    assert fixtures["partial_complete"].completion_kind == "partial"
    assert fixtures["recovery"].recovered is True
    assert fixtures["history_readonly"].history_readonly is True
    assert fixtures["submission_blocked"].notice_title
    assert fixtures["submission_blocked"].qa_scans == fixtures["full_complete"].qa_scans
    assert all(
        "PHS=" in raw and len(raw) >= 160
        for raw in fixtures["full_complete"].qa_scans
    )
    assert len(
        [line for line in fixtures["error"].error_message.splitlines() if line.strip()]
    ) == 4


def test_only_the_state_selected_live_scan_tree_is_mapping_critical():
    fixtures = {fixture.state_id: fixture for fixture in build_state_fixtures()}
    app = SimpleNamespace(current_set_info={})

    assert expected_scan_tree_mapping(fixtures["qa_progress"], app) == {
        "current_set_tree": True,
        "exact_rescan_tree": False,
    }
    assert expected_scan_tree_mapping(fixtures["exact_active"], app) == {
        "current_set_tree": False,
        "exact_rescan_tree": True,
    }
    assert expected_scan_tree_mapping(fixtures["exact_complete"], app) == {
        "current_set_tree": False,
        "exact_rescan_tree": True,
    }


def test_apply_error_fixture_sets_and_clears_all_renderer_error_aliases():
    app = SimpleNamespace(
        current_set_info={},
        _refresh_operator_workbench=lambda: None,
    )
    fixtures = {fixture.state_id: fixture for fixture in build_state_fixtures()}

    apply_state_fixture(app, fixtures["error"])
    assert app._pending_workflow_error == fixtures["error"].error_message
    assert app._workflow_pending_error == fixtures["error"].error_message
    assert app._workflow_error_message == fixtures["error"].error_message

    apply_state_fixture(app, fixtures["waiting"])
    assert app._pending_workflow_error is None
    assert app._workflow_pending_error is None
    assert app._workflow_error_message == ""


def test_apply_conflict_fixture_uses_real_nonblocking_review_renderer():
    from Label_Match import Label_Match

    app = SimpleNamespace(
        current_set_info={},
        operator_workbench_ready=False,
        _workflow_widgets_ready=False,
        _refresh_operator_workbench=lambda: None,
    )
    app._refresh_package_cancellation_review_notice = lambda: (
        Label_Match._refresh_package_cancellation_review_notice(app)
    )
    fixtures = {fixture.state_id: fixture for fixture in build_state_fixtures()}

    view, _method = apply_state_fixture(app, fixtures["cancellation_conflict"])

    assert view.notice is None
    assert view.scan_input_enabled is True
    assert view.cancel_current_enabled is True
    assert view.cancel_completed_enabled is True
    assert view.f3_enabled is True
    assert view.f4_enabled is False
    assert app._workflow_blocking_notice is None
    assert len(app._package_cancellation_review_rows) == CANCELLATION_CONFLICT_COUNT
    notice = app._package_cancellation_review_notice
    assert notice.title == CANCELLATION_CONFLICT_TITLE
    assert notice.message == CANCELLATION_CONFLICT_MESSAGE
    assert notice.kind == "package_cancellation_review"
    assert notice.tone == "danger"

    apply_state_fixture(app, fixtures["waiting"])
    assert app._package_cancellation_review_notice is None
    assert app._package_cancellation_review_rows == ()


def test_apply_fixture_selects_history_only_for_readonly_and_restores_session():
    class FakeNotebook:
        def __init__(self):
            self.selections = []

        def select(self, target):
            self.selections.append(target)

    notebook = FakeNotebook()
    session_tab = object()
    history_tab = object()
    app = SimpleNamespace(
        current_set_info={},
        operator_history_notebook=notebook,
        operator_session_tab=session_tab,
        operator_history_tab=history_tab,
        _refresh_operator_workbench=lambda: None,
    )
    fixtures = {fixture.state_id: fixture for fixture in build_state_fixtures()}

    apply_state_fixture(app, fixtures["history_readonly"])
    assert notebook.selections[-1] is history_tab

    for state_id in DEFAULT_STATE_IDS:
        if state_id == "history_readonly":
            continue
        apply_state_fixture(app, fixtures[state_id])
        assert notebook.selections[-1] is session_tab


@pytest.mark.parametrize("state_id", DEFAULT_STATE_IDS)
def test_fixtures_are_accepted_by_the_real_workflow_presenter(state_id):
    fixture = next(
        fixture for fixture in build_state_fixtures() if fixture.state_id == state_id
    )
    view = build_presenter_view(fixture)

    assert len(view.slots) == 5
    assert view.qa_completed == len(fixture.qa_scans)
    assert view.last_normal_scan == fixture.last_normal_scan
    if state_id == "exact_active":
        assert view.exact_rescan.status == "active"
        assert view.exact_rescan.completed == len(fixture.exact_barcodes)
    if state_id == "history_readonly":
        assert view.readonly is True
        assert view.scan_input_enabled is False
    if state_id in {"error", "submission_blocked"}:
        assert view.scan_input_enabled is False
    if state_id == "cancellation_conflict":
        assert view.notice is None
        assert view.scan_input_enabled is True
        assert view.cancel_current_enabled is True
        assert view.cancel_completed_enabled is True
        assert view.f3_enabled is True


def test_live_contract_reports_missing_widgets_and_never_false_passes_legacy_ui():
    legacy = SimpleNamespace(step_labels=[object()] * 5)

    issues = validate_live_contract(legacy)

    assert "missing_widget:workbench_frame" in issues
    assert "missing_widget:current_set_tree" in issues
    assert "missing_widget:exact_rescan_tree" in issues
    assert "missing_widget:exact_rescan_detail_text" in issues
    assert "missing_widget:cancel_button" in issues
    assert "missing_presenter_refresh_method" in issues


def test_live_contract_accepts_complete_distinct_widget_protocol():
    app = SimpleNamespace()
    for name in REQUIRED_WIDGET_ATTRS:
        setattr(app, name, object())
    setattr(app, CANCEL_BUTTON_ALIASES[0], object())
    app.step_labels = [object() for _ in range(5)]
    app._refresh_operator_workbench = lambda: None

    assert validate_live_contract(app) == []


def test_live_contract_rejects_reused_tree_widgets():
    app = SimpleNamespace()
    for name in REQUIRED_WIDGET_ATTRS:
        setattr(app, name, object())
    reused = object()
    app.current_set_tree = reused
    app.exact_rescan_tree = reused
    app.cancel_button = object()
    app.step_labels = [object() for _ in range(5)]
    app._refresh_workflow_view = lambda: None

    assert "tree_widgets_must_be_distinct" in validate_live_contract(app)


def test_exact_detail_contract_fails_closed_when_required_widgets_are_absent():
    app = SimpleNamespace(exact_rescan_tree=object())
    fixture = next(
        item for item in build_state_fixtures() if item.state_id == "exact_active"
    )

    result = capture.collect_exact_detail_contract(app, fixture)

    assert result["available"] is False
    assert result["passed"] is False
    assert "missing_widget:exact_rescan_detail_text" in result["issues"]


def test_image_analysis_flags_blank_black_and_wrong_sized_captures():
    white = Image.new("RGB", (32, 24), "white")
    white_metrics = analyze_image(white, (32, 24))
    assert white_metrics["pixel_size_matches"] is True
    assert white_metrics["near_black_ratio"] == 0
    assert white_metrics["blank_suspected"] is True

    mixed = Image.new("RGB", (10, 10), "white")
    for x in range(5):
        for y in range(10):
            mixed.putpixel((x, y), (0, 0, 0))
    mixed_metrics = analyze_image(mixed, (11, 10))
    assert mixed_metrics["pixel_size_matches"] is False
    assert mixed_metrics["near_black_ratio"] == pytest.approx(0.5)
    assert mixed_metrics["blank_suspected"] is False
    assert mixed_metrics["capture_pixels_valid"] is False
    assert mixed_metrics["excess_black_suspected"] is True


def _ui_like_image(width=480, height=320):
    image = Image.new("RGB", (width, height), "white")
    for x in range(20, width - 20):
        image.putpixel((x, 40), (59, 130, 246))
        image.putpixel((x, 120), (209, 213, 219))
    return image


def test_image_analysis_rejects_edge_stripes_and_local_black_tiles():
    valid = _ui_like_image()
    assert analyze_image(valid, valid.size)["capture_pixels_valid"] is True

    stripe = _ui_like_image()
    for x in range(int(stripe.width * 0.18)):
        for y in range(stripe.height):
            stripe.putpixel((x, y), (0, 0, 0))
    stripe_metrics = analyze_image(stripe, stripe.size)
    assert stripe_metrics["capture_pixels_valid"] is False
    assert stripe_metrics["edge_black_stripe_suspected"] is True

    tile = _ui_like_image()
    tile_width = tile.width // capture.TILE_COLUMNS
    tile_height = tile.height // capture.TILE_ROWS
    for x in range(tile_width * 5, tile_width * 6):
        for y in range(tile_height * 3, tile_height * 4):
            tile.putpixel((x, y), (0, 0, 0))
    tile_metrics = analyze_image(tile, tile.size)
    assert tile_metrics["capture_pixels_valid"] is False
    assert tile_metrics["black_tile_suspected"] is True


def test_image_analysis_accepts_only_the_os_frame_black_outside_client_roi():
    outer = _ui_like_image()
    client_bbox = (8, 0, outer.width - 8, outer.height - 8)

    # PrintWindow includes the invisible Win32 resize border in GetWindowRect.
    # It is black on the locked DISPLAY2 even though the complete app client is
    # rendered.  Keep the full outer image as evidence, but judge app pixels in
    # the independently attested client rectangle.
    for x in range(outer.width):
        for y in range(outer.height - 8, outer.height):
            outer.putpixel((x, y), (0, 0, 0))
    for x in range(8):
        for y in range(outer.height):
            outer.putpixel((x, y), (0, 0, 0))
    for x in range(outer.width - 8, outer.width):
        for y in range(outer.height):
            outer.putpixel((x, y), (0, 0, 0))

    full_outer = analyze_image(outer, outer.size)
    client = analyze_image(outer, outer.size, content_bbox=client_bbox)

    assert full_outer["capture_pixels_valid"] is False
    assert full_outer["edge_black_stripe_suspected"] is True
    assert client["capture_pixels_valid"] is True
    assert client["edge_black_stripe_suspected"] is False
    assert client["analysis_region"] == "window_client"
    assert client["analysis_bbox"] == list(client_bbox)
    assert client["analysis_pixel_size"] == [
        client_bbox[2] - client_bbox[0],
        client_bbox[3] - client_bbox[1],
    ]
    # Pixel-size attestation remains tied to the uncropped outer evidence.
    assert client["pixel_size"] == list(outer.size)
    assert client["pixel_size_matches"] is True


def test_image_analysis_still_rejects_black_stripe_inside_client_roi():
    outer = _ui_like_image()
    client_bbox = (8, 0, outer.width - 8, outer.height - 8)
    client_width = client_bbox[2] - client_bbox[0]
    stripe_right = client_bbox[0] + int(client_width * 0.18)
    for x in range(client_bbox[0], stripe_right):
        for y in range(client_bbox[1], client_bbox[3]):
            outer.putpixel((x, y), (0, 0, 0))

    metrics = analyze_image(outer, outer.size, content_bbox=client_bbox)

    assert metrics["capture_pixels_valid"] is False
    assert metrics["edge_black_stripe_suspected"] is True
    assert metrics["analysis_region"] == "window_client"


@pytest.mark.parametrize(
    "content_bbox",
    (
        (0, 0, 10),
        (-1, 0, 10, 10),
        (0, -1, 10, 10),
        (5, 0, 5, 10),
        (0, 6, 10, 6),
        (0, 0, 33, 24),
        (0, 0, 32, 25),
    ),
)
def test_image_analysis_rejects_invalid_client_bbox(content_bbox):
    image = _ui_like_image(width=32, height=24)

    with pytest.raises(ValueError, match="content_bbox"):
        analyze_image(image, image.size, content_bbox=content_bbox)


def _geometry_record(
    name: str,
    bbox: list[int],
    *,
    mapped: bool = True,
    critical: bool = True,
    requested_width: int | None = None,
    requested_height: int | None = None,
    check_requested_width: bool = False,
    check_requested_height: bool = False,
):
    width = bbox[2] - bbox[0]
    height = bbox[3] - bbox[1]
    return {
        "name": name,
        "mapped": mapped,
        "critical": critical,
        "bbox": bbox,
        "size": [width, height],
        "requested_size": [
            width if requested_width is None else requested_width,
            height if requested_height is None else requested_height,
        ],
        "check_requested_width": check_requested_width,
        "check_requested_height": check_requested_height,
        "text_measurement_source": "tk",
    }


def test_clipping_proxy_detects_bounds_visibility_compression_overlap_and_containment():
    records = [
        _geometry_record("card", [0, 0, 100, 100]),
        _geometry_record("entry", [10, 10, 90, 40]),
        _geometry_record("list", [10, 30, 90, 80]),
        _geometry_record("outside", [80, 80, 110, 110]),
        {
            **_geometry_record(
                "compressed",
                [10, 82, 70, 98],
                requested_width=80,
                requested_height=24,
                check_requested_width=True,
                check_requested_height=True,
            ),
            "text": "overflowing label",
            "wraplength": 0,
            "text_pixel_width": 80,
        },
        _geometry_record("hidden", [0, 0, 1, 1], mapped=False),
        _geometry_record("inactive_tab", [0, 0, 1, 1], mapped=False, critical=False),
    ]

    result = evaluate_clipping_proxy(
        records,
        (100, 100),
        overlap_pairs=(("entry", "list"),),
        containment_pairs=(("outside", "card"),),
    )

    assert result["suspected"] is True
    assert result["clipped_or_zero_sized_widgets"] == ["outside"]
    assert result["unmapped_critical_widgets"] == ["hidden"]
    assert result["width_compressed_widgets"] == ["compressed"]
    assert result["height_compressed_widgets"] == ["compressed"]
    assert result["overlaps"] == [["entry", "list"]]
    assert result["outside_containers"] == [
        {"widget": "outside", "container": "card"}
    ]
    assert result["issue_count"] == 6


def test_containment_gate_rejects_three_pixel_overflow_and_ignores_hidden_page():
    records = [
        _geometry_record("card", [0, 0, 100, 100]),
        _geometry_record("active_page", [0, 0, 100, 103]),
        _geometry_record(
            "hidden_page",
            [0, 0, 1, 1],
            mapped=False,
            critical=False,
        ),
    ]

    result = evaluate_clipping_proxy(
        records,
        (100, 103),
        containment_pairs=(
            ("active_page", "card"),
            ("hidden_page", "card"),
        ),
    )

    assert result["outside_containers"] == [
        {"widget": "active_page", "container": "card"}
    ]
    assert result["suspected"] is True


def test_history_control_pairwise_gate_rejects_overlap_but_ignores_hidden_inactive_tab():
    names = (
        "history_today_button",
        "history_date_search_button",
        "history_decrease_font_button",
        "history_increase_font_button",
    )
    pairs = capture._pairwise_widget_names(names)
    records = [
        _geometry_record(names[0], [0, 0, 50, 30]),
        _geometry_record(names[1], [40, 0, 100, 30]),
        _geometry_record(names[2], [110, 0, 140, 30]),
        _geometry_record(names[3], [150, 0, 180, 30]),
        _geometry_record(
            "inactive_history_control",
            [0, 0, 1, 1],
            mapped=False,
            critical=False,
        ),
    ]

    result = evaluate_clipping_proxy(
        records,
        (200, 50),
        overlap_pairs=pairs,
    )

    assert len(pairs) == 6
    assert result["overlaps"] == [[names[0], names[1]]]
    assert result["unmapped_critical_widgets"] == []
    assert result["suspected"] is True


def test_text_clipping_proxy_checks_wrap_width_and_requested_geometry():
    records = [
        {
            **_geometry_record(
                "single_line",
                [0, 0, 80, 20],
                requested_width=100,
                requested_height=24,
            ),
            "text": "잘리는 한 줄",
            "wraplength": 0,
        },
        {
            **_geometry_record(
                "wrapped",
                [0, 25, 90, 55],
                requested_width=90,
                requested_height=42,
            ),
            "text": "두 줄 안내",
            "wraplength": 120,
            "text_pixel_width": 150,
            "text_line_height": 20,
        },
    ]

    result = evaluate_text_clipping_proxy(records)

    assert result["suspected"] is True
    assert result["width_compressed_text_widgets"] == ["single_line"]
    assert result["height_compressed_text_widgets"] == ["single_line", "wrapped"]
    assert result["wraplength_exceeds_widget"] == ["wrapped"]


def test_text_clipping_proxy_trusts_fully_realized_natural_label_geometry():
    records = [
        {
            **_geometry_record(
                "natural_tlabel",
                [0, 0, 100, 24],
                requested_width=100,
                requested_height=24,
            ),
            "text": "정상 라벨",
            "widget_class": "TLabel",
            "wraplength": 0,
            "text_pixel_width": 96,
            "text_available_width": 96,
        },
        {
            **_geometry_record(
                "natural_wrapped_label",
                [0, 25, 90, 67],
                requested_width=90,
                requested_height=42,
            ),
            "text": "두 줄로 정상 배치되는 안내 문구",
            "widget_class": "TLabel",
            "wraplength": 120,
            "text_pixel_width": 150,
            "text_line_pixel_widths": [150],
            "text_line_height": 20,
            "text_available_width": 86,
            "text_natural_geometry_authoritative": True,
        },
    ]

    result = evaluate_text_clipping_proxy(records)

    assert result["suspected"] is False
    assert result["width_compressed_text_widgets"] == []
    assert result["height_compressed_text_widgets"] == []
    assert result["wraplength_exceeds_widget"] == []


def test_text_clipping_proxy_rejects_explicit_width_even_when_request_fits():
    records = [
        {
            **_geometry_record(
                "fixed_single_line",
                [0, 0, 90, 24],
                requested_width=60,
                requested_height=24,
            ),
            "text": "명시 폭보다 긴 단일 행 안내",
            "widget_class": "TLabel",
            "wraplength": 0,
            "text_pixel_width": 290,
            "text_available_width": 86,
            "text_natural_geometry_authoritative": False,
        },
        {
            **_geometry_record(
                "fixed_wrapped",
                [0, 25, 90, 67],
                requested_width=60,
                requested_height=42,
            ),
            "text": "명시 폭에서 잘릴 수 있는 줄바꿈 안내",
            "widget_class": "TLabel",
            "wraplength": 120,
            "text_pixel_width": 150,
            "text_line_pixel_widths": [150],
            "text_line_height": 20,
            "text_available_width": 86,
            "text_natural_geometry_authoritative": False,
        },
    ]

    result = evaluate_text_clipping_proxy(records)

    assert result["width_compressed_text_widgets"] == ["fixed_single_line"]
    assert result["wraplength_exceeds_widget"] == ["fixed_wrapped"]
    assert result["suspected"] is True


def test_font_metrics_prefers_direct_widget_font_and_measures_multiline_by_line():
    class FakeTk:
        def __init__(self):
            self.calls = []

        def call(self, *args):
            self.calls.append(args)
            if args[:2] == ("font", "measure"):
                assert args[2] == "ResponsiveDirectFont"
                return {"long first line": 137, "short": 41}[args[3]]
            if args[:2] == ("font", "metrics"):
                assert args[2] == "ResponsiveDirectFont"
                assert args[3] == "-linespace"
                return 29
            if args[:2] == ("ttk::style", "lookup"):
                raise AssertionError("direct widget font must win over ttk style")
            raise AssertionError(f"unexpected Tk call: {args!r}")

    class Widget:
        def __init__(self):
            self.tk = FakeTk()

        @staticmethod
        def cget(option):
            return {
                "font": "ResponsiveDirectFont",
                "style": "Wrong.Header.TLabel",
            }[option]

        @staticmethod
        def winfo_class():
            return "TLabel"

    widget = Widget()

    width, linespace, source = capture._tk_font_metrics_with_source(
        widget,
        "long first line\nshort",
    )

    assert (width, linespace, source) == (137, 29, "tk")
    assert ("font", "measure", "ResponsiveDirectFont", "long first line") in widget.tk.calls
    assert ("font", "measure", "ResponsiveDirectFont", "short") in widget.tk.calls
    assert not any(call[:2] == ("ttk::style", "lookup") for call in widget.tk.calls)


def test_font_metrics_marks_unresolved_default_font_non_authoritative():
    class FakeTk:
        @staticmethod
        def call(*args):
            if args[:2] == ("font", "measure"):
                assert args[2] == "TkDefaultFont"
                return 40
            if args[:2] == ("font", "metrics"):
                return 16
            raise AssertionError(args)

    class Widget:
        tk = FakeTk()

        @staticmethod
        def cget(_option):
            raise RuntimeError("no direct font or ttk style")

        @staticmethod
        def winfo_class():
            return "Unknown"

    assert capture._tk_font_metrics_with_source(Widget(), "text") == (
        40,
        16,
        "tk-unresolved-default",
    )


def test_compact_button_request_can_shrink_only_while_actual_text_still_fits():
    safe = {
        **_geometry_record(
            "compact_plus",
            [0, 0, 30, 31],
            requested_width=106,
            requested_height=31,
            check_requested_width=True,
        ),
        "text": "+",
        "widget_class": "TButton",
        "wraplength": 0,
        "text_pixel_width": 9,
        "text_line_height": 17,
        "text_explicit_line_count": 1,
    }

    geometry = evaluate_clipping_proxy([safe], (200, 100))
    text = evaluate_text_clipping_proxy([safe])

    assert geometry["suspected"] is False
    assert geometry["width_compressed_widgets"] == []
    assert text["suspected"] is False

    overflow = {
        **safe,
        "name": "overflowing_button",
        "text": "실제 넘침",
        "text_pixel_width": 33,
    }
    geometry = evaluate_clipping_proxy([overflow], (200, 100))
    text = evaluate_text_clipping_proxy([overflow])

    assert geometry["width_compressed_widgets"] == ["overflowing_button"]
    assert geometry["suspected"] is True
    assert text["width_compressed_text_widgets"] == ["overflowing_button"]
    assert text["suspected"] is True

    padding_edge = {
        **safe,
        "name": "padding_edge_button",
        "text": "경계",
        # 24 px fits the 30 px outer box but not the conservative 22 px
        # button content area after border/padding.
        "text_pixel_width": 24,
    }
    geometry = evaluate_clipping_proxy([padding_edge], (200, 100))
    text = evaluate_text_clipping_proxy([padding_edge])

    assert geometry["width_compressed_widgets"] == ["padding_edge_button"]
    assert text["width_compressed_text_widgets"] == ["padding_edge_button"]


def test_requested_width_compression_remains_fail_closed_for_empty_entry():
    entry = {
        **_geometry_record(
            "scan_entry",
            [0, 0, 80, 32],
            requested_width=120,
            check_requested_width=True,
        ),
        "widget_class": "TEntry",
        "text": "",
        "wraplength": 0,
        "text_pixel_width": 0,
    }

    result = evaluate_clipping_proxy([entry], (200, 100))

    assert result["width_compressed_widgets"] == ["scan_entry"]
    assert result["suspected"] is True


def test_text_clipping_proxy_rejects_non_authoritative_font_measurement():
    record = {
        **_geometry_record("notice", [0, 0, 200, 40]),
        "text": "측정 엔진이 필요한 안내",
        "wraplength": 180,
        "text_pixel_width": 120,
        "text_line_pixel_widths": [120],
        "text_line_height": 18,
        "text_measurement_source": "headless-approximation",
    }

    result = evaluate_text_clipping_proxy([record])

    assert result["non_authoritative_text_measurements"] == ["notice"]
    assert result["suspected"] is True


def test_active_tree_detail_partition_rejects_exact_overlap():
    frame = _geometry_record("exact_frame", [0, 0, 600, 400])
    tree = _geometry_record("exact_tree", [10, 10, 590, 300])
    detail = _geometry_record("exact_detail", [10, 280, 590, 390])

    result = evaluate_tree_detail_partition(tree, detail, frame)

    assert result["passed"] is False
    assert "tree_detail_overlap" in result["issues"]
    assert "tree_detail_vertical_order_invalid" in result["issues"]


def test_tree_text_fit_proxy_allows_value_overflow_but_not_fixed_columns():
    records = [
        {
            "name": "row:stage",
            "visible": True,
            "width": 80,
            "height": 30,
            "text_width": 92,
            "line_height": 18,
            "allow_overflow": False,
        },
        {
            "name": "row:value",
            "visible": True,
            "width": 180,
            "height": 30,
            "text_width": 420,
            "line_height": 18,
            "allow_overflow": True,
        },
        {
            "name": "row:hidden",
            "visible": False,
            "width": 0,
            "height": 0,
            "text_width": 10,
            "line_height": 18,
            "allow_overflow": False,
        },
    ]

    result = evaluate_tree_text_fit_proxy(records)

    assert result["overflowing_fixed_text"] == ["row:stage"]
    assert result["invisible_cells"] == ["row:hidden"]
    assert "row:value" not in result["overflowing_fixed_text"]


def test_tree_text_fit_proxy_rejects_non_authoritative_nonblank_measurement():
    result = evaluate_tree_text_fit_proxy(
        [
            {
                "name": "tree:heading:Stage",
                "visible": True,
                "width": 120,
                "height": 30,
                "text_width": 40,
                "line_height": 18,
                "text_nonblank": True,
                "measurement_source": "headless-approximation",
                "allow_overflow": False,
            }
        ]
    )

    assert result["non_authoritative_text_measurements"] == [
        "tree:heading:Stage"
    ]
    assert result["suspected"] is True


def test_middle_ellipsis_requires_start_end_and_pixel_fit():
    raw = "PHS|CLC=AAA2270730100|" + "X" * 240 + "|6D=20260716|END"
    displayed = raw[:38] + "..." + raw[-16:]
    assert evaluate_middle_ellipsis_fit(
        raw,
        displayed,
        measured_width=420,
        available_width=421,
    ) == []

    assert "middle_ellipsis_start_not_preserved" in evaluate_middle_ellipsis_fit(
        raw,
        "WRONG..." + raw[-16:],
        measured_width=300,
        available_width=421,
    )
    assert "middle_ellipsis_end_not_preserved" in evaluate_middle_ellipsis_fit(
        raw,
        raw[:38] + "...WRONG",
        measured_width=300,
        available_width=421,
    )
    assert "display_text_exceeds_value_column" in evaluate_middle_ellipsis_fit(
        raw,
        displayed,
        measured_width=422,
        available_width=421,
    )


def test_operator_scan_summary_contract_rejects_full_raw_and_requires_short_id():
    item_code = "AAA2270730200"
    master = f"CLC={item_code}|PHS=2|ITG=MASTER-1"
    product = f"CLC={item_code}|PHS=2|SERIAL=260717000002"
    valid = (item_code, f"{item_code} · S/N 260717000002")

    assert capture.validate_operator_scan_summaries(
        (master, product),
        (item_code, item_code),
        (1, 2),
        valid,
    ) == []

    issues = capture.validate_operator_scan_summaries(
        (master, product),
        (item_code, item_code),
        (1, 2),
        (master, item_code),
    )
    assert "summary_1:raw_delimiter_exposed" in issues
    assert "summary_1:full_raw_exposed" in issues
    assert "summary_1:master_must_equal_item_code" in issues
    assert "summary_2:short_identifier_missing" in issues

    for empty_identifier in (
        f"{item_code} · ",
        f"{item_code} · ID ",
    ):
        assert "summary_1:short_identifier_missing" in (
            capture.validate_operator_scan_summaries(
                (product,),
                (item_code,),
                (2,),
                (empty_identifier,),
            )
        )

    same_raw_issues = capture.validate_operator_scan_summaries(
        (item_code,),
        (item_code,),
        (2,),
        (item_code,),
    )
    assert "summary_1:short_identifier_missing" in same_raw_issues

    prefix_bypass_issues = capture.validate_operator_scan_summaries(
        (product,),
        (item_code,),
        (2,),
        (f"{item_code}X · ID TOKEN",),
    )
    assert "summary_1:item_code_missing" in prefix_bypass_issues


def test_scan_display_contract_requires_authoritative_tk_font_measurement():
    class Tree:
        def __init__(self, tk):
            self.tk = tk

        def get_children(self, _root):
            return ("row-1",)

        def cget(self, key):
            return ("Value",) if key == "columns" else "Operator.Treeview"

        def column(self, _column, _option):
            return 100

        def item(self, _iid, _option):
            return ("ABC",)

    class BrokenTk:
        def call(self, *_args):
            raise RuntimeError("no font engine")

    headless = capture.collect_scan_display_contract(
        Tree(BrokenTk()),
        ("ABC",),
        value_column="Value",
        expected_display_values=("ABC",),
    )
    assert headless["passed"] is False
    assert "row_1:non_authoritative_text_measurement" in headless["issues"]
    assert headless["rows"][0]["measurement_source"] == "headless-approximation"

    class RealTk:
        def call(self, *args):
            if args[:3] == ("ttk::style", "lookup", "Operator.Treeview"):
                return "OperatorFont"
            if args[:2] == ("font", "measure"):
                return 24
            if args[:2] == ("font", "metrics"):
                return 16
            raise AssertionError(args)

    authoritative = capture.collect_scan_display_contract(
        Tree(RealTk()),
        ("ABC",),
        value_column="Value",
        expected_display_values=("ABC",),
    )
    assert authoritative["passed"] is True
    assert authoritative["rows"][0]["measurement_source"] == "tk"


def test_capture_contract_uses_same_effective_stretched_tree_width_as_app():
    class Tree:
        widths = {"Stage": 120, "Value": 100, "State": 80}

        @staticmethod
        def cget(option):
            assert option == "columns"
            return ("Stage", "Value", "State")

        @classmethod
        def column(cls, column, option):
            if option == "width":
                return cls.widths[column]
            if option == "stretch":
                return column == "Value"
            raise AssertionError(option)

        @staticmethod
        def winfo_width():
            return 900

    tree = Tree()

    assert capture.effective_tree_column_width(tree, "Value") == 700
    assert capture.effective_tree_column_width(tree, "Stage") == 120
    tree.winfo_width = lambda: 1
    assert capture.effective_tree_column_width(
        tree,
        "Value",
        viewport_width=900,
    ) == 700
    tree.winfo_width = lambda: 1484
    assert capture.effective_tree_column_width(
        tree,
        "Value",
        viewport_width=1277,
    ) == 1077


def test_hidden_tree_display_contract_uses_shared_viewport_and_rejects_one_pixel_overflow():
    class RealTk:
        def __init__(self, measured_width):
            self.measured_width = measured_width

        def call(self, *args):
            if args[:3] == ("ttk::style", "lookup", "Operator.Treeview"):
                return "OperatorFont"
            if args[:2] == ("font", "measure"):
                return self.measured_width
            if args[:2] == ("font", "metrics"):
                return 16
            raise AssertionError(args)

    class HiddenTree:
        widths = {"Stage": 120, "Value": 100, "State": 80}

        def __init__(self, measured_width):
            self.tk = RealTk(measured_width)

        @staticmethod
        def get_children(_root):
            return ("row-1",)

        @staticmethod
        def cget(option):
            if option == "columns":
                return ("Stage", "Value", "State")
            if option == "style":
                return "Operator.Treeview"
            raise AssertionError(option)

        @classmethod
        def column(cls, column, option):
            if option == "width":
                return cls.widths[column]
            if option == "stretch":
                return column == "Value"
            raise AssertionError(option)

        @staticmethod
        def item(_iid, _option):
            return ("1", "ABC", "OK")

        @staticmethod
        def winfo_width():
            return 1

    passing = capture.collect_scan_display_contract(
        HiddenTree(680),
        ("ABC",),
        value_column="Value",
        expected_display_values=("ABC",),
        viewport_width=900,
    )
    overflowing = capture.collect_scan_display_contract(
        HiddenTree(681),
        ("ABC",),
        value_column="Value",
        expected_display_values=("ABC",),
        viewport_width=900,
    )
    missing_item_prefix = capture.collect_scan_display_contract(
        HiddenTree(24),
        ("RAW-ABC",),
        value_column="Value",
        expected_display_values=("ABC",),
        viewport_width=900,
        display_source_values=("ABC",),
        required_prefix_values=("ITEM-001",),
    )

    assert passing["passed"] is True
    assert passing["rows"][0]["available_width"] == 680
    assert overflowing["passed"] is False
    assert "row_1:display_text_exceeds_value_column" in overflowing["issues"]
    assert missing_item_prefix["passed"] is False
    assert (
        "row_1:item_code_not_preserved_in_display"
        in missing_item_prefix["issues"]
    )


def test_qa_detail_contract_requires_mapping_and_selected_text_raw_parity():
    raws = ("PHS-MASTER-LONG", "PHS-PRODUCT-LONG")
    details = {
        "qa-slot-1": {"raw": raws[0]},
        "qa-slot-2": {"raw": raws[1]},
    }
    selected = {"qa-slot-1": raws[0], "qa-slot-2": raws[1]}
    assert validate_qa_detail_contract(raws, details, selected) == []

    details["qa-slot-2"]["raw"] = "TRUNCATED"
    selected["qa-slot-1"] = "WRONG"
    issues = validate_qa_detail_contract(raws, details, selected)
    assert "qa_detail_1_selected_text_mismatch" in issues
    assert "qa_detail_2_raw_parity_mismatch" in issues


def test_display2_monitor_contract_rejects_primary_wrong_work_area_and_plus_zero():
    inventory = [
        {
            "device": TARGET_DISPLAY_DEVICE,
            "is_primary": False,
            "monitor_rect": list(TARGET_DISPLAY_MONITOR_AREA),
            "work_rect": list(TARGET_DISPLAY_WORK_AREA),
            "dpi": list(TARGET_DISPLAY_DPI),
        }
    ]
    resolved = resolve_capture_monitor(
        TARGET_DISPLAY_DEVICE,
        TARGET_DISPLAY_WORK_AREA,
        inventory=inventory,
    )
    assert resolved["device"] == TARGET_DISPLAY_DEVICE
    assert resolved["is_primary"] is False
    assert resolved["work_size"] == [2560, 1392]

    primary = [{**inventory[0], "is_primary": True}]
    with pytest.raises(RuntimeError, match="non-primary"):
        resolve_capture_monitor(
            TARGET_DISPLAY_DEVICE,
            TARGET_DISPLAY_WORK_AREA,
            inventory=primary,
        )
    with pytest.raises(RuntimeError, match="work area is locked"):
        resolve_capture_monitor(
            TARGET_DISPLAY_DEVICE,
            (694, -1440, 3254, -48),
            inventory=inventory,
        )
    with pytest.raises(RuntimeError, match="locked"):
        resolve_capture_monitor(
            r"\\.\DISPLAY3",
            TARGET_DISPLAY_WORK_AREA,
            inventory=inventory,
        )
    with pytest.raises(RuntimeError, match=r"\+0\+0"):
        resolve_capture_monitor(
            TARGET_DISPLAY_DEVICE,
            (0, 0, 2560, 1392),
            inventory=inventory,
        )


def test_direct_responsive_settle_cancels_timer_and_flushes_windows_paint():
    calls = []

    class QueueTk:
        def __init__(self):
            self.pending = ["after-1", "after-2"]

        def call(self, *args):
            assert args == ("after", "info")
            return tuple(self.pending)

        def splitlist(self, value):
            return tuple(value)

    class FakeApp:
        def __init__(self):
            self.__dict__["_operator_layout_settle_after_id"] = "after-1"
            self.__dict__["_responsive_after_id"] = "after-2"
            self.tk = QueueTk()

        def after_cancel(self, value):
            calls.append(("cancel", value))
            self.tk.pending.remove(value)

        def _apply_operator_responsive_layout(self, *, settle):
            assert self._operator_layout_settle_after_id is None
            assert self._responsive_after_id is None
            calls.append(("layout", settle))
            self.tk.pending.append("after-layout")

        def update_idletasks(self):
            calls.append(("idle",))

        def update(self):
            assert self.tk.pending == []
            calls.append(("full-update",))

    result = settle_responsive_layout(
        FakeApp(),
        hwnd=101,
        update_window=lambda hwnd: calls.append(("update", hwnd)) or 1,
        dwm_flush=lambda: calls.append(("dwm",)) or 0,
        invalidate_rect=lambda hwnd, _rect, _erase: calls.append(
            ("invalidate", hwnd)
        )
        or 1,
    )

    assert result["status"] == "PASS"
    assert calls == [
        ("cancel", "after-1"),
        ("cancel", "after-2"),
        ("layout", True),
        ("cancel", "after-layout"),
        ("idle",),
        ("full-update",),
        ("idle",),
        ("invalidate", 101),
        ("update", 101),
        ("dwm",),
        ("idle",),
    ]
    assert result["full_app_update_called"] is True
    assert result["pending_after_full_update"] == 0
    assert result["responsive_callback_cancellation"] == {
        "status": "PASS",
        "attributes_cleared": [
            "_operator_layout_settle_after_id",
            "_responsive_after_id",
        ],
        "cancelled_ids": ["after-1", "after-2"],
    }
    assert result["scheduled_job_quiescence"]["remaining_after"] == 0
    assert result["scheduled_job_quiescence"]["cancelled"] == 1
    assert result["invalidate_rect_result"] == 1
    assert result["update_window_result"] == 1


def test_responsive_settle_clears_both_ids_before_aborting_failed_cancel():
    calls = []

    class FakeApp:
        def __init__(self):
            self._operator_layout_settle_after_id = "after-1"
            self._responsive_after_id = "after-2"

        def after_cancel(self, after_id):
            calls.append(("cancel", after_id))
            if after_id == "after-1":
                raise RuntimeError("stuck")

        def _apply_operator_responsive_layout(self, *, settle):
            calls.append(("layout", settle))

    app = FakeApp()
    with pytest.raises(RuntimeError, match="cancellation failed"):
        settle_responsive_layout(app)

    assert calls == [("cancel", "after-1"), ("cancel", "after-2")]
    assert app._operator_layout_settle_after_id is None
    assert app._responsive_after_id is None


def test_prepare_state_for_capture_settles_before_reading_rendered_state(
    monkeypatch,
):
    calls = []
    app = object()
    fixture = SimpleNamespace(state_id="qa_progress")
    view = object()
    settle_evidence = {"status": "PASS"}
    rendered_state = {"state": "qa_progress"}

    def apply_fixture(actual_app, actual_fixture):
        assert actual_app is app
        assert actual_fixture is fixture
        calls.append("apply")
        return view, "refresh"

    def settle_layout(actual_app):
        assert actual_app is app
        calls.append("settle")
        return settle_evidence

    def collect_state(actual_app, actual_fixture, actual_view):
        assert actual_app is app
        assert actual_fixture is fixture
        assert actual_view is view
        calls.append("rendered")
        return rendered_state

    monkeypatch.setattr(capture, "apply_state_fixture", apply_fixture)
    monkeypatch.setattr(capture, "settle_responsive_layout", settle_layout)
    monkeypatch.setattr(capture, "collect_rendered_state", collect_state)
    monkeypatch.setattr(
        capture,
        "pump_tk",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            AssertionError("per-state pump must not run")
        ),
    )

    result = capture.prepare_state_for_capture(app, fixture)

    assert calls == ["apply", "settle", "rendered"]
    assert result == (view, "refresh", settle_evidence, rendered_state)


def test_responsive_settle_fails_closed_when_update_window_does_not_paint():
    class EmptyTk:
        def call(self, *_args):
            return ()

        def splitlist(self, value):
            return tuple(value)

    class FakeApp:
        tk = EmptyTk()

        def _apply_operator_responsive_layout(self, *, settle):
            assert settle is True

        def update_idletasks(self):
            pass

        def update(self):
            pass

    with pytest.raises(RuntimeError, match="UpdateWindow failed"):
        settle_responsive_layout(
            FakeApp(),
            hwnd=101,
            invalidate_rect=lambda *_args: 1,
            update_window=lambda _hwnd: 0,
            dwm_flush=lambda: 0,
        )


def test_scheduled_job_query_and_cancel_fail_closed_and_success_is_rechecked():
    class BrokenTk:
        def call(self, *_args):
            raise RuntimeError("tcl unavailable")

    with pytest.raises(RuntimeError, match="cannot query Tcl scheduled jobs"):
        capture._pending_after_ids(SimpleNamespace(tk=BrokenTk()))

    class QueueTk:
        def __init__(self):
            self.pending = ["after-1"]

        def call(self, *_args):
            return tuple(self.pending)

        def splitlist(self, value):
            return tuple(value)

    queue = QueueTk()
    failing = SimpleNamespace(
        tk=queue,
        after_cancel=lambda _after_id: (_ for _ in ()).throw(RuntimeError("no")),
        update_idletasks=lambda: None,
    )
    with pytest.raises(RuntimeError, match="scheduled job quiescence failed"):
        capture.quiesce_scheduled_jobs(failing)

    def cancel(after_id):
        queue.pending.remove(after_id)

    passing = SimpleNamespace(
        tk=queue,
        after_cancel=cancel,
        update_idletasks=lambda: None,
    )
    assert capture.quiesce_scheduled_jobs(passing)["remaining_after"] == 0


def test_capture_rechecks_after_queue_after_its_full_update(monkeypatch):
    root = SimpleNamespace(update_idletasks=lambda: None, update=lambda: None)
    monkeypatch.setattr(capture, "_pending_after_ids", lambda _root: ("after-9",))
    monkeypatch.setattr(
        capture,
        "_capture_outer_with_print_window",
        lambda _root: (_ for _ in ()).throw(AssertionError("must not capture")),
    )

    with pytest.raises(RuntimeError, match="pre-capture full update"):
        capture.capture_tk_client(root)


def test_window_capture_pair_requires_stable_outer_client_pid_windows_and_pixels():
    snapshot = {
        "status": "PASS",
        "current_pid": 9001,
        "root_hwnd": 101,
        "window_rect": [693, -1440, 2059, -672],
        "window_size": [1366, 768],
        "client_rect": [701, -1409, 2051, -680],
        "client_size": [1350, 729],
        "client_offset_in_window": [8, 31],
        "visible_pid_toplevels": [
            {
                "hwnd": 101,
                "rect": [693, -1440, 2059, -672],
                "contained_on_display2": True,
            }
        ],
        "all_visible_pid_toplevels_contained": True,
    }
    result = validate_window_capture_pair(
        snapshot,
        dict(snapshot),
        requested_outer_size=(1366, 768),
        captured_pixel_size=(1366, 768),
    )
    assert result["requested_size_semantics"] == "outer-window-pixels"

    moved = dict(snapshot)
    moved["client_rect"] = [702, -1409, 2052, -680]
    with pytest.raises(RuntimeError, match="client_rect"):
        validate_window_capture_pair(
            snapshot,
            moved,
            requested_outer_size=(1366, 768),
            captured_pixel_size=(1350, 729),
        )


def test_root_only_printwindow_contract_rejects_contained_dialog_toplevel():
    validate_root_only_toplevels(101, [101])
    with pytest.raises(RuntimeError, match="rejects extra visible PID toplevels"):
        validate_root_only_toplevels(101, [101, 202])


def test_constructor_toplevel_is_never_deiconified_and_is_rejected():
    events = []

    class FakeTk:
        def __init__(self, *_args, **_kwargs):
            self.visible = False

        def withdraw(self):
            self.visible = False
            events.append("root-withdraw")

        def deiconify(self):
            self.visible = True
            events.append("root-deiconify-visible")

        def state(self, new_state=None):
            if new_state in {"normal", "zoomed"}:
                self.visible = True
            return "normal" if self.visible else "withdrawn"

        def destroy(self):
            events.append("root-destroy")

    class FakeToplevel:
        def __init__(self, _master=None):
            self.visible = False
            events.append("toplevel-init")
            self.deiconify()

        def withdraw(self):
            self.visible = False
            events.append("toplevel-withdraw")

        def deiconify(self):
            self.visible = True
            events.append("toplevel-deiconify-visible")

        def state(self, new_state=None):
            if new_state in {"normal", "zoomed"}:
                self.visible = True
            return "normal" if self.visible else "withdrawn"

    module = SimpleNamespace()
    module.tk = SimpleNamespace(Tk=FakeTk, Toplevel=FakeToplevel)
    original_toplevel_init = FakeToplevel.__init__

    class FakeLabel(FakeTk):
        def __init__(self, run_tests=False):
            super().__init__()
            assert run_tests is True
            self.deiconify()
            module.last_toplevel = module.tk.Toplevel(self)

    module.Label_Match = FakeLabel

    with pytest.raises(RuntimeError, match="forbidden extra Toplevels"):
        capture._make_capture_app(module, {})

    assert "toplevel-init" in events
    assert "toplevel-withdraw" in events
    assert "toplevel-deiconify-visible" not in events
    assert "root-deiconify-visible" not in events
    assert module.last_toplevel.visible is False
    assert module.tk.Toplevel.__init__ is original_toplevel_init


def _presenter_rendered_rows(view):
    rows = []
    for expected in expected_presenter_rows(view):
        rows.append(
            {
                "text": f"{expected['index']}. {expected['label']}",
                "values": [expected["value"]],
                "tags": [expected["state"]],
            }
        )
    return rows


def test_presenter_row_validation_requires_all_five_labels_values_and_state_tags():
    fixture = next(
        fixture for fixture in build_state_fixtures() if fixture.state_id == "qa_progress"
    )
    view = build_presenter_view(fixture)
    expected = expected_presenter_rows(view)
    rendered = _presenter_rendered_rows(view)
    displays = [str(row["value"] or "") for row in expected]

    assert validate_presenter_rows(rendered, expected, displays) == []

    rendered[0]["values"] = [""]
    rendered[1]["tags"] = ["pending"]
    issues = validate_presenter_rows(rendered, expected, displays)
    assert "qa_row_1_missing_presenter_value" in issues
    assert "qa_row_2_missing_presenter_state_tag" in issues
    assert validate_presenter_rows(rendered[:-1], expected, displays) == [
        "qa_row_count_mismatch:4!=5"
    ]


def test_exact_rescan_validation_requires_separate_exact_membership_rows():
    exact = ("EXACT-1", "EXACT-2")
    rows = [
        {"text": "1", "values": ["EXACT-1"], "tags": ["complete"]},
        {"text": "2", "values": ["EXACT-2"], "tags": ["complete"]},
    ]

    assert validate_exact_rows(rows, exact, exact) == []
    assert validate_exact_rows(rows[:1], exact, exact) == ["exact_row_count_mismatch:1!=2"]
    rows[1]["values"] = ["WRONG"]
    assert validate_exact_rows(rows, exact, exact) == ["exact_row_2_missing_barcode"]


def test_long_pixel_fitted_cells_validate_without_raw_substring_false_failures():
    fixture = next(
        item for item in build_state_fixtures() if item.state_id == "qa_master"
    )
    view = build_presenter_view(fixture)
    expected = expected_presenter_rows(view)
    raw = fixture.qa_scans[0]
    fitted = raw[:42] + "..." + raw[-18:]
    displays = [fitted, "", "", "", ""]
    rendered = _presenter_rendered_rows(view)
    rendered[0]["values"] = [fitted]

    assert raw not in fitted
    assert validate_presenter_rows(rendered, expected, displays) == []
    assert evaluate_middle_ellipsis_fit(
        raw,
        fitted,
        measured_width=400,
        available_width=400,
    ) == []

    exact_raw = next(
        item for item in build_state_fixtures() if item.state_id == "exact_active"
    ).exact_barcodes[0]
    exact_fitted = exact_raw[:40] + "..." + exact_raw[-16:]
    exact_rows = [{"text": "1", "values": [exact_fitted]}]
    assert validate_exact_rows(
        exact_rows, (exact_raw,), (exact_fitted,)
    ) == []

    qa_capture = _valid_capture_record("qa_master")
    qa_capture["rendered_state"]["current_set_rows"][0]["values"] = [fitted]
    qa_capture["rendered_state"]["expected_qa_display_values"][0] = fitted
    qa_issues = evaluate_capture(qa_capture)
    assert "qa_row_1_missing_presenter_value" not in qa_issues

    exact_capture = _valid_capture_record("exact_active")
    exact_capture["rendered_state"]["exact_rescan_rows"][0]["values"] = [
        exact_fitted
    ]
    exact_capture["rendered_state"]["expected_exact_display_values"][0] = (
        exact_fitted
    )
    exact_issues = evaluate_capture(exact_capture)
    assert "exact_row_1_missing_barcode" not in exact_issues


def test_long_last_normal_uses_raw_detail_and_fitted_cell_and_rejects_duplicates():
    fixture = next(
        item for item in build_state_fixtures() if item.state_id == "qa_master"
    )
    raw = fixture.last_normal_scan
    fitted = raw[:36] + "..." + raw[-16:]
    qa_rows = [{"values": ["1. 현품표", fitted, "완료"]}]
    qa_detail = {
        "detail_rows": {"qa-slot-1": {"raw": raw}},
        "selected_texts": {"qa-slot-1": raw},
    }
    result = capture.build_last_normal_scan_contract(
        fixture,
        qa_rows,
        [],
        qa_detail,
        {"detail_rows": {}, "selected_texts": {}},
        (fitted,),
        (),
    )
    assert result["passed"] is True
    assert result["raw_detail_exact_count"] == 1
    assert result["fitted_cell_exact_count"] == 1

    duplicate = capture.StateFixture(
        "duplicate",
        "duplicate",
        qa_scans=(raw, raw),
        last_normal_scan=raw,
    )
    duplicate_result = capture.build_last_normal_scan_contract(
        duplicate,
        qa_rows * 2,
        [],
        qa_detail,
        {"detail_rows": {}, "selected_texts": {}},
        (fitted, fitted),
        (),
    )
    assert duplicate_result["passed"] is False
    assert "last_normal_fixture_source_count:2!=1" in duplicate_result["issues"]


def _valid_capture_record(state_id: str = "qa_progress"):
    fixture = next(
        fixture for fixture in build_state_fixtures() if fixture.state_id == state_id
    )
    view = build_presenter_view(fixture)
    exact_mode = bool(
        fixture.exact_active
        or (fixture.exact_complete and len(fixture.qa_scans) <= 1)
    )
    rendered_rows = _presenter_rendered_rows(view)
    exact_rows = [
        {"text": str(index), "values": [barcode], "tags": ["complete"]}
        for index, barcode in enumerate(fixture.exact_barcodes, 1)
    ]
    notice = view.notice
    display_notice = (
        SimpleNamespace(
            title=CANCELLATION_CONFLICT_TITLE,
            message=CANCELLATION_CONFLICT_MESSAGE,
            kind="package_cancellation_review",
            tone="danger",
        )
        if state_id == "cancellation_conflict"
        else notice
    )
    selected_iid = (
        f"qa-slot-{fixture.selected_qa_index}"
        if fixture.selected_qa_index
        else None
    )
    selected_raw = (
        fixture.qa_scans[fixture.selected_qa_index - 1]
        if fixture.selected_qa_index
        else ""
    )
    return {
        "state": state_id,
        "requested_size": [1366, 768],
        "capture_source": AUTHORITATIVE_CAPTURE_SOURCE,
        "window_capture_contract": {
            "status": "PASS",
            "before": {
                "client_size": [1350, 729],
                "client_offset_in_window": [8, 0],
            },
            "after": {
                "client_size": [1350, 729],
                "client_offset_in_window": [8, 0],
            },
        },
        "client_outer_bbox": [8, 0, 1358, 729],
        "sha256": f"raw-{state_id}",
        "workbench_sha256": f"workbench-{state_id}",
        "requested_scale": 1.0,
        "applied_scale_factor": 1.0,
        "fixture": asdict(fixture),
        "image_analysis": {
            "analysis_region": "window_client",
            "analysis_bbox": [8, 0, 1358, 729],
            "analysis_pixel_size": [1350, 729],
            "pixel_size_matches": True,
            "capture_pixels_valid": True,
            "blank_suspected": False,
            "near_black_ratio": 0.01,
            "excess_black_suspected": False,
            "edge_black_stripe_suspected": False,
            "contiguous_black_stripe_suspected": False,
            "black_tile_suspected": False,
            "uniform_low_variance_suspected": False,
        },
        "ui_geometry": {
            "clipping_proxy": {"suspected": False},
            "text_clipping_proxy": {"suspected": False},
            "tree_text_clipping_suspected": False,
            "structure": {
                "three_distinct_cards": True,
                "current_and_exact_trees_are_distinct": True,
                "center_current_list_below_scan_input": True,
                "active_tree_detail_partition": {"passed": True, "issues": []},
                "detail_text_bottom_within_frame": True,
                "detail_text_requested_height_fits": True,
                "exact_detail_available": True,
                "exact_detail_text_bottom_within_frame": True,
                "exact_detail_text_requested_height_fits": True,
                "right_action_height_contract_86_to_104": True,
                "status_footer_height_contract_max_32": True,
                "notice_message_reqheight_fits": True,
                "mismatch_notice_4_to_3_line_contract": True,
                "mapped_workflow_notice_frame_count": 1,
                "center_list_signature": {
                    "path": ".center.current",
                    "master_path": ".center",
                    "mapped": not exact_mode,
                    "bbox": [300, 300, 900, 580],
                    "grid": {"row": 5, "column": 0},
                },
                "active_scan_tree_signature": {
                    "mode": "f4" if exact_mode else "qa",
                    "tree_path": ".center.active",
                    "tree_bbox": [300, 300, 900, 580],
                    "detail_bbox": [300, 582, 900, 600],
                    "tree_mapped": True,
                    "logical_frame_path": ".center.live.frame",
                    "logical_bbox": [300, 280, 900, 600],
                    "logical_frame_mapped": True,
                    "notebook_bbox": [290, 270, 910, 610],
                },
            },
        },
        "rendered_state": {
            "current_set_rows": rendered_rows,
            "exact_rescan_rows": exact_rows,
            "presenter_rows": expected_presenter_rows(view),
            "expected_qa_display_values": [
                str(row.get("values", [""])[0]) for row in rendered_rows
            ],
            "expected_exact_display_values": [
                str(row.get("values", [""])[0]) for row in exact_rows
            ],
            "presenter_stage_label": view.current_stage_label,
            "presenter_next_action": view.next_action,
            "presenter_last_normal_scan": view.last_normal_scan,
            "presenter_notice": (
                {
                    "title": notice.title,
                    "message": notice.message,
                    "kind": notice.kind,
                    "tone": notice.tone,
                }
                if notice
                else None
            ),
            "display_notice": (
                {
                    "title": display_notice.title,
                    "message": display_notice.message,
                    "kind": display_notice.kind,
                    "tone": display_notice.tone,
                }
                if display_notice
                else None
            ),
            "presenter_action_gates": {
                "scan_input_enabled": bool(view.scan_input_enabled),
                "f1_cancel_current_enabled": bool(view.cancel_current_enabled),
                "f2_cancel_completed_enabled": bool(view.cancel_completed_enabled),
                "f3_enabled": bool(view.f3_enabled),
                "f4_enabled": bool(view.f4_enabled),
            },
            "button_states": {
                "reset_button": "normal" if view.cancel_current_enabled else "disabled",
                "cancel_button": "normal" if view.cancel_completed_enabled else "disabled",
                "manual_complete_button": "normal" if view.f3_enabled else "disabled",
                "exact_rescan_button": "normal" if view.f4_enabled else "disabled",
            },
            "notice_title_occurrences": 1 if display_notice else 0,
            "notice_message_occurrences": 1 if display_notice else 0,
            "notice_display_contract": {
                "passed": True,
                "issues": [],
                "title_occurrences": 1 if display_notice else 0,
                "message_occurrences": 1 if display_notice else 0,
            },
            "last_normal_occurrences_on_screen": 1 if fixture.last_normal_scan else 0,
            "last_normal_occurrences_in_center": 1 if fixture.last_normal_scan else 0,
            "last_normal_occurrences_in_actual_list": (
                1 if fixture.last_normal_scan else 0
            ),
            "last_normal_occurrences_in_right": 0,
            "center_visible_texts": [view.current_stage_label, view.next_action],
            "notice_action_mapped": state_id in {"error", "submission_blocked"},
            "notice_action_text": (
                "제출 재시도" if state_id == "submission_blocked" else "확인"
            ),
            "entry_state": "disabled"
            if state_id in {"error", "history_readonly", "submission_blocked"}
            else "normal",
            "history_tree_mapped": state_id == "history_readonly",
            "session_tree_mapped": state_id != "history_readonly",
            "current_tree_mapped": not exact_mode,
            "exact_tree_mapped": exact_mode,
            "qa_detail_contract": {
                "passed": True,
                "issues": [],
                "selected_iid": selected_iid,
                "selected_raw": selected_raw,
                "selected_detail_text": selected_raw,
            },
            "qa_summary_contract": {"passed": True, "issues": []},
            "exact_summary_contract": {"passed": True, "issues": []},
            "qa_display_contract": {"passed": True, "issues": []},
            "exact_display_contract": {"passed": True, "issues": []},
            "exact_detail_contract": {"passed": True, "issues": []},
            "last_normal_contract": {
                "passed": True,
                "issues": [],
            },
        },
        "issues": [],
        "passed": True,
    }


@pytest.mark.parametrize("state_id", DEFAULT_STATE_IDS)
def test_capture_evaluation_accepts_complete_synthetic_contract(state_id):
    record = _valid_capture_record(state_id)

    assert evaluate_capture(record) == []


def _shift_both_recorded_client_boxes(record):
    shifted = [0, 0, 1350, 729]
    record["client_outer_bbox"] = shifted
    record["image_analysis"]["analysis_bbox"] = shifted


@pytest.mark.parametrize(
    ("mutation", "expected_issue"),
    (
        (
            lambda record: record["image_analysis"].update(
                analysis_region="full_outer_window"
            ),
            "image_analysis_region_not_window_client",
        ),
        (
            lambda record: record["image_analysis"].update(
                analysis_bbox=[0, 0, 1366, 768]
            ),
            "image_analysis_bbox_not_attested_client",
        ),
        (
            lambda record: record["image_analysis"].update(
                analysis_pixel_size=[1366, 768]
            ),
            "image_analysis_size_not_attested_client",
        ),
        (
            _shift_both_recorded_client_boxes,
            "client_outer_bbox_not_attested_client",
        ),
    ),
)
def test_capture_evaluation_binds_image_analysis_to_attested_client_roi(
    mutation,
    expected_issue,
):
    record = _valid_capture_record("waiting")
    mutation(record)

    assert expected_issue in evaluate_capture(record)


def test_exact_complete_keeps_completed_f4_tree_and_last_raw_visible():
    record = _valid_capture_record("exact_complete")

    assert record["rendered_state"]["current_tree_mapped"] is False
    assert record["rendered_state"]["exact_tree_mapped"] is True
    assert len(record["rendered_state"]["exact_rescan_rows"]) == len(
        record["fixture"]["exact_barcodes"]
    )
    assert evaluate_capture(record) == []

    record["rendered_state"]["exact_rescan_rows"].pop()
    assert "exact_row_count_mismatch:2!=3" in evaluate_capture(record)


def test_capture_evaluation_rejects_stale_notice_action_on_active_state():
    record = _valid_capture_record("waiting")
    record["rendered_state"]["notice_action_mapped"] = True
    record["rendered_state"]["notice_action_text"] = "제출 재시도"

    assert "notice_action_mapping_mismatch" in evaluate_capture(record)


def test_cancellation_conflict_evaluation_is_exact_and_nonblocking():
    record = _valid_capture_record("cancellation_conflict")
    assert evaluate_capture(record) == []

    record["rendered_state"]["display_notice"]["message"] = "미확인 3건"
    record["rendered_state"]["presenter_action_gates"]["f3_enabled"] = False
    record["rendered_state"]["qa_detail_contract"]["selected_detail_text"] = (
        "abbreviated"
    )
    record["rendered_state"]["current_set_rows"].pop()

    issues = evaluate_capture(record)
    assert "cancellation_conflict_notice_mismatch" in issues
    assert "cancellation_conflict_action_gates_changed" in issues
    assert "cancellation_conflict_selected_raw_detail_changed" in issues
    assert "cancellation_conflict_five_row_scan_list_missing" in issues


def test_capture_evaluation_fails_closed_when_scan_summary_contract_is_missing_or_failed():
    missing = _valid_capture_record("qa_progress")
    missing["rendered_state"].pop("qa_summary_contract")
    assert "qa_summary_contract_missing_or_failed" in evaluate_capture(missing)

    failed = _valid_capture_record("exact_first")
    failed["rendered_state"]["exact_summary_contract"] = {
        "passed": False,
        "issues": ["summary_1:short_identifier_missing"],
    }
    assert (
        "exact_summary_contract:summary_1:short_identifier_missing"
        in evaluate_capture(failed)
    )


def test_compact_mismatch_notice_semantics_pass_and_duplicate_widget_fails():
    record = _valid_capture_record("error")
    assert len(
        [
            line
            for line in record["rendered_state"]["presenter_notice"]["message"].splitlines()
            if line.strip()
        ]
    ) == 4
    assert not any(
        issue.startswith("notice_display_contract:")
        for issue in evaluate_capture(record)
    )

    record["rendered_state"]["notice_display_contract"] = {
        "passed": False,
        "issues": ["notice_message_occurrence_count:2!=1"],
    }
    assert (
        "notice_display_contract:notice_message_occurrence_count:2!=1"
        in evaluate_capture(record)
    )


def test_capture_evaluation_rejects_wrong_live_scan_tab_mapping():
    active = _valid_capture_record("exact_active")
    active["rendered_state"]["current_tree_mapped"] = True
    active["rendered_state"]["exact_tree_mapped"] = False

    issues = evaluate_capture(active)

    assert "exact_rescan_tree_mapping_mismatch" in issues
    assert "current_set_tree_mapping_mismatch" in issues


def test_capture_evaluation_combines_pixel_geometry_notice_and_preservation_failures():
    record = _valid_capture_record("submission_blocked")
    record["image_analysis"]["blank_suspected"] = True
    record["ui_geometry"]["clipping_proxy"]["suspected"] = True
    record["ui_geometry"]["structure"]["mapped_workflow_notice_frame_count"] = 2
    record["rendered_state"]["last_normal_contract"] = {
        "passed": False,
        "issues": ["last_normal_fixture_source_count:2!=1"],
    }
    record["rendered_state"]["notice_display_contract"] = {
        "passed": False,
        "issues": ["notice_title_occurrence_count:2!=1"],
    }
    record["rendered_state"]["entry_state"] = "normal"

    issues = evaluate_capture(record)

    assert "blank_image_suspected" in issues
    assert "clipping_or_overlap_suspected" in issues
    assert "workflow_notice_frame_not_single" in issues
    assert any(issue.startswith("last_normal_contract:") for issue in issues)
    assert any(issue.startswith("notice_display_contract:") for issue in issues)
    assert "blocked_state_scan_entry_enabled" in issues


def test_capture_evaluation_fails_new_detail_action_footer_and_text_gates():
    record = _valid_capture_record("qa_product_3")
    record["ui_geometry"]["text_clipping_proxy"]["suspected"] = True
    record["ui_geometry"]["tree_text_clipping_suspected"] = True
    structure = record["ui_geometry"]["structure"]
    structure["detail_text_bottom_within_frame"] = False
    structure["detail_text_requested_height_fits"] = False
    structure["right_action_height_contract_86_to_104"] = False
    structure["status_footer_height_contract_max_32"] = False
    structure["center_current_list_below_scan_input"] = False

    issues = evaluate_capture(record)

    assert "requested_vs_actual_text_clipping_suspected" in issues
    assert "tree_text_clipping_suspected" in issues
    assert "detail_text_overruns_detail_frame" in issues
    assert "detail_text_height_compressed" in issues
    assert "right_action_height_outside_86_to_104" in issues
    assert "status_or_footer_height_exceeds_32" in issues
    assert "current_scan_list_not_below_input" in issues


def test_cross_capture_contract_preserves_center_geometry_and_scan_values():
    qa = _valid_capture_record("qa_product_3")
    conflict = _valid_capture_record("cancellation_conflict")
    error = _valid_capture_record("error")
    completed = _valid_capture_record("full_complete")
    blocked = _valid_capture_record("submission_blocked")
    captures = [qa, conflict, error, completed, blocked]

    apply_cross_capture_contracts(captures)
    assert all(capture["passed"] for capture in captures)

    error["rendered_state"]["current_set_rows"][0]["values"] = ["LOST"]
    blocked["ui_geometry"]["structure"]["center_list_signature"] = {
        "path": ".replacement",
        "master_path": ".other",
        "mapped": True,
        "bbox": [0, 0, 10, 10],
        "grid": {"row": 1},
    }
    apply_cross_capture_contracts(captures)

    assert "last_normal_qa_rows_not_preserved" in error["issues"]
    assert "center_scan_list_geometry_changed_across_states" in blocked["issues"]
    assert error["passed"] is False
    assert blocked["passed"] is False


def test_cross_capture_conflict_preserves_selected_raw_and_fkey_gates():
    qa = _valid_capture_record("qa_product_3")
    conflict = _valid_capture_record("cancellation_conflict")
    apply_cross_capture_contracts([qa, conflict])
    assert conflict["passed"] is True

    conflict["rendered_state"]["qa_detail_contract"]["selected_raw"] = "changed"
    conflict["rendered_state"]["button_states"]["manual_complete_button"] = (
        "disabled"
    )
    apply_cross_capture_contracts([qa, conflict])

    assert "cancellation_conflict_selected_raw_changed" in conflict["issues"]
    assert (
        "cancellation_conflict_fkey_button_states_changed" in conflict["issues"]
    )


def test_cross_capture_qa_preservation_ignores_status_values_and_tags():
    qa = _valid_capture_record("qa_product_3")
    error = _valid_capture_record("error")
    for row in qa["rendered_state"]["current_set_rows"]:
        row["values"].append("정상 상태")
    for row in error["rendered_state"]["current_set_rows"]:
        row["values"].append("오류 상태")
        row["tags"] = ["error"]

    apply_cross_capture_contracts([qa, error])

    assert "last_normal_qa_rows_not_preserved" not in error["issues"]
    assert error["passed"] is True


def test_cross_capture_geometry_skips_hidden_f4_but_rejects_vertical_resize():
    qa = _valid_capture_record("qa_product_3")
    error = _valid_capture_record("error")
    exact = _valid_capture_record("exact_active")
    error_signature = error["ui_geometry"]["structure"]["center_list_signature"]
    error_signature["bbox"] = [300, 360, 900, 540]
    exact_signature = exact["ui_geometry"]["structure"]["center_list_signature"]
    exact_signature.update(
        {
            "mapped": False,
            "path": ".hidden.relayout",
            "master_path": ".hidden.parent",
            "bbox": [0, 0, 1, 1],
            "grid": {},
        }
    )

    apply_cross_capture_contracts([qa, error, exact])

    assert "center_scan_list_geometry_changed_across_states" in error["issues"]
    assert "center_scan_list_geometry_changed_across_states" not in exact["issues"]


def test_cross_capture_geometry_allows_two_pixel_live_list_tolerance():
    qa = _valid_capture_record("qa_product_3")
    error = _valid_capture_record("error")
    error["ui_geometry"]["structure"]["center_list_signature"]["bbox"] = [
        302,
        302,
        902,
        582,
    ]

    apply_cross_capture_contracts([qa, error])

    assert "center_scan_list_geometry_changed_across_states" not in error["issues"]


def test_cross_capture_rejects_reused_raw_and_workbench_hashes():
    first = _valid_capture_record("qa_progress")
    second = _valid_capture_record("qa_product_2")
    second["sha256"] = first["sha256"]
    second["workbench_sha256"] = first["workbench_sha256"]

    apply_cross_capture_contracts([first, second])

    assert any("raw_sha256_reused_across_states" in issue for issue in first["issues"])
    assert any(
        "workbench_sha256_reused_across_states" in issue
        for issue in second["issues"]
    )
    assert first["passed"] is False
    assert second["passed"] is False


def _signature(*, offset: int = 0, parent: str = ".workbench"):
    return {
        "center_card": {
            "path": ".workbench.center",
            "master_path": parent,
            "bbox": [200 + offset, 10, 900 + offset, 700],
            "grid": {"row": 0, "column": 1},
        },
        "current_set_tree": {
            "path": ".workbench.center.current",
            "master_path": ".workbench.center",
            "bbox": [220 + offset, 300, 880 + offset, 570],
            "grid": {"row": 5, "column": 0},
        },
    }


def test_compact_wide_compact_signature_detects_accumulation_parent_and_grid_changes():
    before = _signature()
    assert compare_layout_signatures(before, _signature(offset=2)) == []

    geometry_issues = compare_layout_signatures(before, _signature(offset=4))
    assert "center_card:geometry_accumulated" in geometry_issues
    assert "current_set_tree:geometry_accumulated" in geometry_issues

    changed = _signature(parent=".replacement")
    changed["current_set_tree"]["grid"] = {"row": 4, "column": 0}
    issues = compare_layout_signatures(before, changed)
    assert "center_card:parent_changed" in issues
    assert "current_set_tree:grid_changed" in issues


def test_output_isolation_rejects_parent_and_sibling_paths(tmp_path):
    allowed = tmp_path / "tmp"
    allowed.mkdir()

    child = assert_descendant(allowed / "capture" / "data", allowed, label="data")
    assert child == (allowed / "capture" / "data").resolve()
    with pytest.raises(RuntimeError):
        assert_descendant(allowed, allowed, label="data")
    with pytest.raises(RuntimeError):
        assert_descendant(tmp_path / "other", allowed, label="data")


def test_external_output_stays_clean_before_and_after_files_exist(tmp_path):
    source = tmp_path / "source"
    (source / "ui").mkdir(parents=True)
    (source / "Label_Match.py").write_text("VALUE = 1\n", encoding="utf-8")
    (source / "ui" / "__init__.py").write_text("", encoding="utf-8")

    def git(*args: str) -> str:
        completed = subprocess.run(
            ["git", "-C", str(source), *args],
            check=True,
            capture_output=True,
            text=True,
            encoding="utf-8",
        )
        return completed.stdout.strip()

    git("init", "--quiet")
    git("config", "user.email", "capture-test@example.invalid")
    git("config", "user.name", "Capture Test")
    git("add", "Label_Match.py", "ui/__init__.py")
    git("commit", "--quiet", "-m", "fixture")
    commit = git("rev-parse", "HEAD")
    tree = git("rev-parse", "HEAD^{tree}")

    before = capture.verify_source_identity(
        source,
        expected_commit=commit,
        expected_tree=tree,
    )
    output_base = tmp_path / "external-captures"
    output = capture.assert_external_capture_descendant(
        output_base / "capture-1",
        output_base,
        source,
        label="output root",
    )
    output.mkdir(parents=True)
    (output / "manifest.json").write_text("{}\n", encoding="utf-8")
    assert output.exists()

    after = capture.verify_source_identity(
        source,
        expected_commit=commit,
        expected_tree=tree,
    )
    assert before["worktree_clean"] is True
    assert after["worktree_clean"] is True
    assert git("status", "--porcelain=v1", "--untracked-files=all") == ""

    with pytest.raises(RuntimeError, match="outside source root"):
        capture.assert_external_capture_descendant(
            source / "tmp" / "capture-2",
            source / "tmp",
            source,
            label="output root",
        )


def test_source_identity_requires_exact_clean_commit_and_tree(monkeypatch, tmp_path):
    source = tmp_path / "exact-source"
    (source / "ui").mkdir(parents=True)
    (source / "Label_Match.py").write_text("APP_VERSION = 'v2.0.36'\n", encoding="utf-8")
    values = {
        ("rev-parse", "--show-toplevel"): str(source.resolve()),
        ("rev-parse", "HEAD"): "commit-1",
        ("rev-parse", "HEAD^{tree}"): "tree-1",
        ("status", "--porcelain=v1", "--untracked-files=all"): "",
    }
    monkeypatch.setattr(
        capture,
        "_git_text",
        lambda _root, *args: values[tuple(args)],
    )

    identity = capture.verify_source_identity(
        source,
        expected_commit="commit-1",
        expected_tree="tree-1",
    )

    assert identity["worktree_clean"] is True
    with pytest.raises(RuntimeError, match="must be supplied explicitly"):
        capture.verify_source_identity(
            source,
            expected_commit="",
            expected_tree="",
        )
    values[("status", "--porcelain=v1", "--untracked-files=all")] = " M ui/x.py"
    with pytest.raises(RuntimeError, match="must be clean"):
        capture.verify_source_identity(
            source,
            expected_commit="commit-1",
            expected_tree="tree-1",
        )


def test_actual_unchecked_hash_poisoned_pyc_is_rejected_before_source_import(
    tmp_path,
):
    source = tmp_path / "source"
    source.mkdir()
    app_path = source / "Label_Match.py"
    app_path.write_text("PAYLOAD = 'POISONED_PYC'\n", encoding="utf-8")
    py_compile.compile(
        str(app_path),
        doraise=True,
        invalidation_mode=py_compile.PycInvalidationMode.UNCHECKED_HASH,
    )
    app_path.write_text("PAYLOAD = 'clean-source'\n", encoding="utf-8")

    previous_path = tuple(sys.path)
    previous_module = sys.modules.pop("Label_Match", None)
    try:
        sys.path.insert(0, str(source))
        importlib.invalidate_caches()
        poisoned = importlib.import_module("Label_Match")
        assert poisoned.PAYLOAD == "POISONED_PYC"
    finally:
        sys.modules.pop("Label_Match", None)
        if previous_module is not None:
            sys.modules["Label_Match"] = previous_module
        sys.path[:] = list(previous_path)
        importlib.invalidate_caches()

    with pytest.raises(RuntimeError, match="forbidden Python bytecode"):
        capture.verify_no_bytecode_artifacts(source)
    with pytest.raises(RuntimeError, match="forbidden Python bytecode"):
        capture.import_label_match_from_source(source)


def test_external_pycache_prefix_poison_is_ignored_and_restored(
    monkeypatch,
    tmp_path,
):
    source = tmp_path / "source"
    (source / "ui").mkdir(parents=True)
    app_path = source / "Label_Match.py"
    ui_path = source / "ui" / "__init__.py"
    app_path.write_text(
        "import ui\nPAYLOAD = 'EXTERNAL_POISON'\n",
        encoding="utf-8",
    )
    ui_path.write_text("ORIGIN = 'disk'\n", encoding="utf-8")
    external_cache = tmp_path / "external-pycache"
    monkeypatch.setattr(sys, "pycache_prefix", str(external_cache))
    poisoned_cache_path = Path(
        importlib.util.cache_from_source(str(app_path))
    )
    poisoned_cache_path.parent.mkdir(parents=True, exist_ok=True)
    py_compile.compile(
        str(app_path),
        cfile=str(poisoned_cache_path),
        doraise=True,
        invalidation_mode=py_compile.PycInvalidationMode.UNCHECKED_HASH,
    )
    app_path.write_text("import ui\nPAYLOAD = 'clean-source'\n", encoding="utf-8")
    subprocess.run(
        ["git", "init", "--quiet", str(source)],
        check=True,
        capture_output=True,
    )
    for arguments in (
        ("config", "user.email", "capture-test@example.invalid"),
        ("config", "user.name", "Capture Test"),
        ("add", "Label_Match.py", "ui/__init__.py"),
        ("commit", "--quiet", "-m", "fixture"),
    ):
        subprocess.run(
            ["git", "-C", str(source), *arguments],
            check=True,
            capture_output=True,
        )
    assert poisoned_cache_path.is_file()
    assert capture.verify_no_bytecode_artifacts(source)["status"] == "PASS"

    prefix_before = sys.pycache_prefix
    dont_write_before = sys.dont_write_bytecode
    module, origins, isolation = capture.import_label_match_from_source(source)
    try:
        assert module.PAYLOAD == "clean-source"
        assert origins["Label_Match"]["loader_source_exact"] is True
        assert sys.pycache_prefix is None
        assert sys.dont_write_bytecode is True
    finally:
        restored = isolation.restore()

    assert restored["pycache_prefix_restored"] is True
    assert restored["dont_write_bytecode_restored"] is True
    assert sys.pycache_prefix == prefix_before
    assert sys.dont_write_bytecode == dont_write_before
    assert capture.verify_no_bytecode_artifacts(source)["status"] == "PASS"


def test_import_origins_are_tracked_and_match_head_after_filters(monkeypatch, tmp_path):
    source = tmp_path / "source"
    (source / "ui").mkdir(parents=True)
    app_file = source / "Label_Match.py"
    ui_file = source / "ui" / "__init__.py"
    app_file.write_text("VALUE = 1\n", encoding="utf-8")
    ui_file.write_text("", encoding="utf-8")
    modules = __import__("sys").modules
    for name in tuple(modules):
        if name in {"Label_Match", "ui", "core", "package_logistics"} or any(
            name.startswith(f"{prefix}.")
            for prefix in ("Label_Match", "ui", "core", "package_logistics")
        ):
            monkeypatch.delitem(modules, name, raising=False)
    app_loader = importlib.machinery.SourceFileLoader(
        "Label_Match", str(app_file)
    )
    app_spec = importlib.util.spec_from_file_location(
        "Label_Match", app_file, loader=app_loader
    )
    ui_loader = importlib.machinery.SourceFileLoader("ui", str(ui_file))
    ui_spec = importlib.util.spec_from_file_location(
        "ui",
        ui_file,
        loader=ui_loader,
        submodule_search_locations=[str(ui_file.parent)],
    )
    monkeypatch.setitem(
        modules,
        "Label_Match",
        SimpleNamespace(
            __file__=str(app_file),
            __spec__=app_spec,
            __loader__=app_loader,
        ),
    )
    monkeypatch.setitem(
        modules,
        "ui",
        SimpleNamespace(
            __file__=str(ui_file),
            __spec__=ui_spec,
            __loader__=ui_loader,
        ),
    )

    def git_text(_root, *args):
        if args[:2] == ("ls-files", "--error-unmatch"):
            return args[-1]
        if args[0] == "rev-parse":
            return "blob-1"
        if args[0] == "hash-object":
            return "blob-1"
        raise AssertionError(args)

    monkeypatch.setattr(capture, "_git_text", git_text)
    origins = capture.verify_import_origins(source)
    assert origins["Label_Match"]["head_blob_matches_filtered_worktree"] is True
    assert origins["Label_Match"]["loader_source_exact"] is True
    assert origins["ui"]["tracked"] is True

    bad_loader = object()
    modules["Label_Match"].__spec__ = SimpleNamespace(
        origin=str(app_file), loader=bad_loader
    )
    modules["Label_Match"].__loader__ = bad_loader
    with pytest.raises(RuntimeError, match="exact SourceFileLoader"):
        capture.verify_import_origins(source)
    modules["Label_Match"].__spec__ = app_spec
    modules["Label_Match"].__loader__ = app_loader

    monkeypatch.setattr(
        capture,
        "_git_text",
        lambda _root, *args: (
            args[-1]
            if args[:2] == ("ls-files", "--error-unmatch")
            else "worktree-blob"
            if args[0] == "hash-object"
            else "head-blob"
        ),
    )
    with pytest.raises(RuntimeError, match="differs from HEAD after Git filters"):
        capture.verify_import_origins(source)


def test_source_import_purges_same_path_preloaded_payload_and_restores_it(
    monkeypatch, tmp_path
):
    source = tmp_path / "source"
    (source / "ui").mkdir(parents=True)
    app_path = source / "Label_Match.py"
    ui_path = source / "ui" / "__init__.py"
    app_path.write_text("import ui\nPAYLOAD = 'disk'\n", encoding="utf-8")
    ui_path.write_text("ORIGIN = 'disk'\n", encoding="utf-8")
    subprocess.run(
        ["git", "init", "--quiet", str(source)],
        check=True,
        capture_output=True,
    )
    for arguments in (
        ("config", "user.email", "capture-test@example.invalid"),
        ("config", "user.name", "Capture Test"),
        ("add", "Label_Match.py", "ui/__init__.py"),
        ("commit", "--quiet", "-m", "fixture"),
    ):
        subprocess.run(
            ["git", "-C", str(source), *arguments],
            check=True,
            capture_output=True,
        )
    malicious = SimpleNamespace(__file__=str(app_path), PAYLOAD="memory")
    malicious_ui = SimpleNamespace(__file__=str(ui_path), ORIGIN="memory")
    monkeypatch.setitem(__import__("sys").modules, "Label_Match", malicious)
    monkeypatch.setitem(__import__("sys").modules, "ui", malicious_ui)
    monkeypatch.setattr(sys, "dont_write_bytecode", True)

    meta_calls: list[str] = []
    path_hook_calls: list[str] = []
    cache_calls: list[str] = []

    class PayloadLoader(importlib.abc.Loader):
        def exec_module(self, module):
            module.__file__ = str(app_path)
            module.PAYLOAD = "meta-path"

    class PayloadMetaFinder(importlib.abc.MetaPathFinder):
        def find_spec(self, fullname, path=None, target=None):
            if fullname == "Label_Match":
                meta_calls.append(fullname)
                return importlib.util.spec_from_loader(
                    fullname,
                    PayloadLoader(),
                    origin=str(app_path),
                )
            return None

    class PayloadPathFinder:
        def find_spec(self, fullname, target=None):
            if fullname == "Label_Match":
                cache_calls.append(fullname)
                return importlib.util.spec_from_loader(
                    fullname,
                    PayloadLoader(),
                    origin=str(app_path),
                )
            return None

    cached_finder = PayloadPathFinder()

    def payload_path_hook(path):
        path_hook_calls.append(str(path))
        if str(Path(path).resolve()) == str(source.resolve()):
            return cached_finder
        raise ImportError(path)

    monkeypatch.setattr(
        sys,
        "meta_path",
        [PayloadMetaFinder(), *sys.meta_path],
    )
    monkeypatch.setattr(
        sys,
        "path_hooks",
        [payload_path_hook, *sys.path_hooks],
    )
    monkeypatch.setattr(
        sys,
        "path_importer_cache",
        {**sys.path_importer_cache, str(source.resolve()): cached_finder},
    )
    path_before = tuple(sys.path)
    meta_path_object_before = sys.meta_path
    meta_path_before = tuple(sys.meta_path)
    path_hooks_object_before = sys.path_hooks
    path_hooks_before = tuple(sys.path_hooks)
    importer_cache_object_before = sys.path_importer_cache
    importer_cache_before = dict(sys.path_importer_cache)

    module, origins, isolation = capture.import_label_match_from_source(source)
    try:
        assert module.PAYLOAD == "disk"
        assert module is not malicious
        assert sys.modules["ui"] is not malicious_ui
        assert origins["Label_Match"]["loader_source_exact"] is True
        assert origins["ui"]["loader_source_exact"] is True
        assert meta_calls == []
        assert path_hook_calls == []
        assert cache_calls == []
    finally:
        restored = isolation.restore()
        assert restored["status"] == "PASS"
        assert restored["meta_path_restored"] is True
        assert restored["path_hooks_restored"] is True
        assert restored["path_importer_cache_restored"] is True

    assert sys.modules["Label_Match"] is malicious
    assert sys.modules["ui"] is malicious_ui
    assert tuple(sys.path) == path_before
    assert sys.meta_path is meta_path_object_before
    assert tuple(sys.meta_path) == meta_path_before
    assert all(
        current is previous
        for current, previous in zip(sys.meta_path, meta_path_before)
    )
    assert sys.path_hooks is path_hooks_object_before
    assert tuple(sys.path_hooks) == path_hooks_before
    assert all(
        current is previous
        for current, previous in zip(sys.path_hooks, path_hooks_before)
    )
    assert sys.path_importer_cache is importer_cache_object_before
    assert set(sys.path_importer_cache) == set(importer_cache_before)
    assert all(
        sys.path_importer_cache[key] is value
        for key, value in importer_cache_before.items()
    )


def test_harness_attestation_requires_clean_head_bound_files(monkeypatch, tmp_path):
    for relative in capture.HARNESS_ATTESTED_PATHS:
        path = tmp_path / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(relative + "\n", encoding="utf-8")

    def git_text(_root, *args):
        if args == ("rev-parse", "--show-toplevel"):
            return str(tmp_path.resolve())
        if args[:3] == ("status", "--porcelain=v1", "--untracked-files=all"):
            return ""
        if args[:2] == ("ls-files", "--error-unmatch"):
            return args[-1]
        if args[0] == "hash-object":
            return "blob"
        if args[0] == "rev-parse" and str(args[1]).startswith("HEAD:"):
            return "blob"
        if args == ("rev-parse", "HEAD"):
            return "commit"
        if args == ("rev-parse", "HEAD^{tree}"):
            return "tree"
        raise AssertionError(args)

    monkeypatch.setattr(capture, "_git_text", git_text)
    assert capture.verify_harness_identity(tmp_path)["status"] == "PASS"

    monkeypatch.setattr(
        capture,
        "_git_text",
        lambda _root, *args: (
            " M tools/capture_label_operator_ui.py"
            if args[:3] == ("status", "--porcelain=v1", "--untracked-files=all")
            else git_text(_root, *args)
        ),
    )
    with pytest.raises(RuntimeError, match="must be clean"):
        capture.verify_harness_identity(tmp_path)


def test_execution_harness_must_be_same_root_and_same_expected_head(tmp_path):
    identity = {
        "status": "PASS",
        "attested_paths_clean": True,
        "commit": "commit-new",
        "tree": "tree-new",
    }
    result = capture.validate_execution_source_binding(
        tmp_path,
        tmp_path,
        identity,
        expected_commit="commit-new",
        expected_tree="tree-new",
    )
    assert result["harness_root_equals_source_root"] is True

    old_harness = {**identity, "commit": "commit-old", "tree": "tree-old"}
    with pytest.raises(RuntimeError, match="harness_commit"):
        capture.validate_execution_source_binding(
            tmp_path,
            tmp_path,
            old_harness,
            expected_commit="commit-new",
            expected_tree="tree-new",
        )
    other_source = tmp_path / "other-source"
    with pytest.raises(RuntimeError, match="harness_root"):
        capture.validate_execution_source_binding(
            tmp_path,
            other_source,
            identity,
            expected_commit="commit-new",
            expected_tree="tree-new",
        )


def test_dpi_awareness_is_independently_observed_as_two():
    class FakeShcore:
        def SetProcessDpiAwareness(self, requested):
            assert requested == 2
            return -2147024891

        def GetProcessDpiAwareness(self, _process, pointer):
            ctypes.cast(pointer, ctypes.POINTER(ctypes.c_int)).contents.value = 2
            return 0

    result = capture.enable_per_monitor_dpi_awareness(shcore=FakeShcore())
    assert result["observed"] == 2
    assert result["status"] == "PASS"


def test_target_tk_scaling_is_configured_and_independently_attested():
    class FakeTkInterpreter:
        def __init__(self):
            self.scaling = 2.0

        def call(self, *args):
            assert args[:2] == ("tk", "scaling")
            if len(args) == 2:
                return self.scaling
            self.scaling = float(args[2])
            return ""

    class FakeApp:
        def __init__(self):
            self.tk = FakeTkInterpreter()

        def winfo_fpixels(self, value):
            assert value == "1i"
            return self.tk.scaling * 72.0

    class FakeUser32:
        @staticmethod
        def GetDpiForWindow(hwnd):
            assert hwnd == 101
            return 96

    app = FakeApp()
    configured = capture.configure_target_tk_scaling(app, 96)
    observed = capture.observe_target_tk_scaling(
        app,
        96,
        hwnd=101,
        user32=FakeUser32(),
    )

    assert configured["before_tk_scaling"] == 2.0
    assert configured["configured_before_widget_creation"] is True
    assert configured["observed_tk_scaling"] == pytest.approx(96 / 72)
    assert observed["pixels_per_inch"] == pytest.approx(96.0)
    assert observed["window_dpi"] == 96


def test_capture_app_pins_target_tk_scaling_before_subclass_widgets(
    monkeypatch,
):
    events = []

    class FakeTkInterpreter:
        def __init__(self):
            self.scaling = 2.0

        def call(self, *args):
            assert args[:2] == ("tk", "scaling")
            if len(args) == 2:
                return self.scaling
            self.scaling = float(args[2])
            events.append(("scaling", self.scaling))
            return ""

    class FakeTk:
        def __init__(self, *_args, **_kwargs):
            self.tk = FakeTkInterpreter()
            self.visible = False

        def winfo_fpixels(self, value):
            assert value == "1i"
            return self.tk.scaling * 72.0

        def withdraw(self):
            self.visible = False

        def deiconify(self):
            self.visible = True

        def state(self, new_state=None):
            if new_state in {"normal", "zoomed"}:
                self.visible = True
            return "normal" if self.visible else "withdrawn"

        def destroy(self):
            pass

    class FakeToplevel(FakeTk):
        pass

    module = SimpleNamespace()
    module.tk = SimpleNamespace(Tk=FakeTk, Toplevel=FakeToplevel)

    class FakeLabel(FakeTk):
        def __init__(self, run_tests=False):
            super().__init__()
            assert run_tests is True
            events.append(("create-widgets", self.tk.scaling))

    module.Label_Match = FakeLabel
    monkeypatch.setattr(capture, "_window_root_hwnd", lambda *_args, **_kwargs: 987654)

    app = capture._make_capture_app(module, {}, target_dpi=96)
    try:
        assert events == [
            ("scaling", pytest.approx(96 / 72)),
            ("create-widgets", pytest.approx(96 / 72)),
        ]
        assert app._capture_constructor_tk_scaling[
            "configured_before_widget_creation"
        ] is True
    finally:
        capture.release_previsible_toplevel_guard(app, reject_created=False)


def test_environment_isolation_restores_and_redacts_real_host_values(
    monkeypatch, tmp_path
):
    source_root = tmp_path / "source"
    output_base = tmp_path / "external-captures"
    source_root.mkdir()
    monkeypatch.setenv("PROGRAMDATA", r"C:\RealProgramData")
    monkeypatch.setenv("LOCALAPPDATA", r"C:\Users\real\AppData\Local")
    monkeypatch.setenv("COMPUTERNAME", "REAL-HOST-77")
    monkeypatch.setenv("LABEL_MATCH_LOGISTICS_TOKEN", "secret")
    isolation = capture.prepare_isolated_environment(
        output_base / "run" / "_isolated_data",
        output_base=output_base,
        source_root=source_root,
    )
    assert os.environ["COMPUTERNAME"] == "CAPTURE-DISPLAY2"
    assert "LABEL_MATCH_LOGISTICS_TOKEN" not in os.environ
    sanitized, labels = capture.redact_sensitive_manifest_values(
        {
            "host": "REAL-HOST-77",
            "path": r"C:\Users\real\AppData\Local\x",
            "REAL-HOST-77-key": "value",
        },
        isolation.sensitive_values,
    )
    assert "REAL-HOST-77" not in repr(sanitized)
    assert all("REAL-HOST-77" not in key for key in sanitized)
    assert "COMPUTERNAME" in labels
    assert isolation.restore()["status"] == "PASS"
    assert os.environ["COMPUTERNAME"] == "REAL-HOST-77"
    assert os.environ["LABEL_MATCH_LOGISTICS_TOKEN"] == "secret"


def test_privacy_failure_manifest_discards_original_sensitive_keys_and_values():
    minimal = capture.minimal_privacy_failure_manifest(
        RuntimeError("REAL-HOST-77 C:\\Users\\real")
    )
    serialized = repr(minimal)
    assert "REAL-HOST-77" not in serialized
    assert "Users" not in serialized
    assert minimal["privacy_contract"]["original_manifest_discarded"] is True
    assert minimal["summary"]["passed"] is False
