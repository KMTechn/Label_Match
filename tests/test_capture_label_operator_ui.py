from __future__ import annotations

import argparse
from dataclasses import asdict
from types import SimpleNamespace

import pytest
from PIL import Image

from tools.capture_label_operator_ui import (
    CANCEL_BUTTON_ALIASES,
    DEFAULT_SCALE,
    DEFAULT_SIZES,
    DEFAULT_STATE_IDS,
    MAX_SCALE,
    MIN_SCALE,
    REQUIRED_WIDGET_ATTRS,
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
    expected_scan_tree_mapping,
    expected_presenter_rows,
    parse_scale,
    parse_sizes,
    parse_states,
    validate_exact_rows,
    validate_live_contract,
    validate_presenter_rows,
)


def test_default_capture_matrix_covers_required_sizes_states_and_scale():
    assert DEFAULT_SIZES == (
        (1366, 768),
        (1440, 900),
        (1920, 1080),
        (2560, 1080),
    )
    assert DEFAULT_STATE_IDS == (
        "waiting",
        "qa_progress",
        "exact_active",
        "exact_complete",
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
    assert build_parser().parse_args([]).scale == DEFAULT_SCALE

    for value in ("800x600", "wide", "1366x"):
        with pytest.raises(argparse.ArgumentTypeError):
            parse_sizes(value)
    with pytest.raises(argparse.ArgumentTypeError):
        parse_states("not-a-state")
    for value in ("0.69", "2.51", "nan", "inf", "large", True):
        with pytest.raises(argparse.ArgumentTypeError):
            parse_scale(value)


def test_state_fixtures_preserve_qa_exact_and_last_normal_contracts():
    fixtures = {fixture.state_id: fixture for fixture in build_state_fixtures()}

    assert tuple(fixtures) == DEFAULT_STATE_IDS
    assert fixtures["waiting"].qa_scans == ()
    assert len(fixtures["qa_progress"].qa_scans) == 2
    assert fixtures["error"].qa_scans == fixtures["qa_progress"].qa_scans
    assert fixtures["error"].last_normal_scan == fixtures["qa_progress"].last_normal_scan
    assert fixtures["error"].has_error is True
    assert fixtures["exact_active"].exact_active is True
    assert len(fixtures["exact_active"].exact_barcodes) < fixtures["exact_active"].exact_target
    assert fixtures["exact_complete"].exact_complete is True
    assert len(fixtures["exact_complete"].exact_barcodes) == fixtures["exact_complete"].exact_target
    assert fixtures["sealed"].sealed_transfer is True
    assert fixtures["full_complete"].completion_kind == "full"
    assert fixtures["partial_complete"].completion_kind == "partial"
    assert fixtures["recovery"].recovered is True
    assert fixtures["history_readonly"].history_readonly is True
    assert fixtures["submission_blocked"].notice_title
    assert fixtures["submission_blocked"].qa_scans == fixtures["full_complete"].qa_scans


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
        "current_set_tree": True,
        "exact_rescan_tree": False,
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


def test_live_contract_reports_missing_widgets_and_never_false_passes_legacy_ui():
    legacy = SimpleNamespace(step_labels=[object()] * 5)

    issues = validate_live_contract(legacy)

    assert "missing_widget:workbench_frame" in issues
    assert "missing_widget:current_set_tree" in issues
    assert "missing_widget:exact_rescan_tree" in issues
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
    }


def test_clipping_proxy_detects_bounds_visibility_compression_overlap_and_containment():
    records = [
        _geometry_record("card", [0, 0, 100, 100]),
        _geometry_record("entry", [10, 10, 90, 40]),
        _geometry_record("list", [10, 30, 90, 80]),
        _geometry_record("outside", [80, 80, 110, 110]),
        _geometry_record(
            "compressed",
            [10, 82, 70, 98],
            requested_width=80,
            requested_height=24,
            check_requested_width=True,
            check_requested_height=True,
        ),
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

    assert validate_presenter_rows(rendered, expected) == []

    rendered[0]["values"] = [""]
    rendered[1]["tags"] = ["pending"]
    issues = validate_presenter_rows(rendered, expected)
    assert "qa_row_1_missing_presenter_value" in issues
    assert "qa_row_2_missing_presenter_state_tag" in issues
    assert validate_presenter_rows(rendered[:-1], expected) == [
        "qa_row_count_mismatch:4!=5"
    ]


def test_exact_rescan_validation_requires_separate_exact_membership_rows():
    exact = ("EXACT-1", "EXACT-2")
    rows = [
        {"text": "1", "values": ["EXACT-1"], "tags": ["complete"]},
        {"text": "2", "values": ["EXACT-2"], "tags": ["complete"]},
    ]

    assert validate_exact_rows(rows, exact) == []
    assert validate_exact_rows(rows[:1], exact) == ["exact_row_count_mismatch:1!=2"]
    rows[1]["values"] = ["WRONG"]
    assert validate_exact_rows(rows, exact) == ["exact_row_2_missing_barcode"]


def _valid_capture_record(state_id: str = "qa_progress"):
    fixture = next(
        fixture for fixture in build_state_fixtures() if fixture.state_id == state_id
    )
    view = build_presenter_view(fixture)
    rendered_rows = _presenter_rendered_rows(view)
    exact_rows = [
        {"text": str(index), "values": [barcode], "tags": ["complete"]}
        for index, barcode in enumerate(fixture.exact_barcodes, 1)
    ]
    notice = view.notice
    return {
        "state": state_id,
        "requested_size": [1366, 768],
        "requested_scale": 1.0,
        "applied_scale_factor": 1.0,
        "fixture": asdict(fixture),
        "image_analysis": {
            "pixel_size_matches": True,
            "blank_suspected": False,
            "near_black_ratio": 0.01,
        },
        "ui_geometry": {
            "clipping_proxy": {"suspected": False},
            "structure": {
                "three_distinct_cards": True,
                "current_and_exact_trees_are_distinct": True,
                "center_current_list_below_scan_input": True,
                "mapped_workflow_notice_frame_count": 1,
                "center_list_signature": {
                    "path": ".center.current",
                    "master_path": ".center",
                    "mapped": state_id != "exact_active",
                    "bbox": [300, 300, 900, 580],
                    "grid": {"row": 5, "column": 0},
                },
            },
        },
        "rendered_state": {
            "current_set_rows": rendered_rows,
            "exact_rescan_rows": exact_rows,
            "presenter_rows": expected_presenter_rows(view),
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
            "notice_title_occurrences": 1 if notice else 0,
            "notice_message_occurrences": 1 if notice else 0,
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
            "current_tree_mapped": state_id != "exact_active",
            "exact_tree_mapped": state_id == "exact_active",
        },
        "issues": [],
        "passed": True,
    }


@pytest.mark.parametrize("state_id", DEFAULT_STATE_IDS)
def test_capture_evaluation_accepts_complete_synthetic_contract(state_id):
    record = _valid_capture_record(state_id)

    assert evaluate_capture(record) == []


def test_exact_complete_returns_to_qa_tree_but_preserves_hidden_exact_rows():
    record = _valid_capture_record("exact_complete")

    assert record["rendered_state"]["current_tree_mapped"] is True
    assert record["rendered_state"]["exact_tree_mapped"] is False
    assert len(record["rendered_state"]["exact_rescan_rows"]) == len(
        record["fixture"]["exact_barcodes"]
    )
    assert evaluate_capture(record) == []

    record["rendered_state"]["exact_rescan_rows"].pop()
    assert "exact_row_count_mismatch:3!=4" in evaluate_capture(record)


def test_capture_evaluation_rejects_stale_notice_action_on_active_state():
    record = _valid_capture_record("waiting")
    record["rendered_state"]["notice_action_mapped"] = True
    record["rendered_state"]["notice_action_text"] = "제출 재시도"

    assert "notice_action_mapping_mismatch" in evaluate_capture(record)


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
    record["rendered_state"]["last_normal_occurrences_on_screen"] = 0
    record["rendered_state"]["notice_title_occurrences"] = 2
    record["rendered_state"]["entry_state"] = "normal"

    issues = evaluate_capture(record)

    assert "blank_image_suspected" in issues
    assert "clipping_or_overlap_suspected" in issues
    assert "workflow_notice_frame_not_single" in issues
    assert "last_normal_scan_missing_or_duplicated_on_screen" in issues
    assert "notice_title_missing_or_duplicated" in issues
    assert "blocked_state_scan_entry_enabled" in issues


def test_cross_capture_contract_preserves_center_geometry_and_scan_values():
    qa = _valid_capture_record("qa_progress")
    error = _valid_capture_record("error")
    completed = _valid_capture_record("full_complete")
    blocked = _valid_capture_record("submission_blocked")
    captures = [qa, error, completed, blocked]

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


def test_cross_capture_qa_preservation_ignores_status_values_and_tags():
    qa = _valid_capture_record("qa_progress")
    error = _valid_capture_record("error")
    for row in qa["rendered_state"]["current_set_rows"]:
        row["values"].append("정상 상태")
    for row in error["rendered_state"]["current_set_rows"]:
        row["values"].append("오류 상태")
        row["tags"] = ["error"]

    apply_cross_capture_contracts([qa, error])

    assert "last_normal_qa_rows_not_preserved" not in error["issues"]
    assert error["passed"] is True


def test_cross_capture_geometry_skips_hidden_f4_current_tree_and_vertical_resize():
    qa = _valid_capture_record("qa_progress")
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

    assert "center_scan_list_geometry_changed_across_states" not in error["issues"]
    assert "center_scan_list_geometry_changed_across_states" not in exact["issues"]


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
