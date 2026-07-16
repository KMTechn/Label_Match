from dataclasses import FrozenInstanceError

import pytest

from ui.operator_layout import (
    build_operator_layout,
    center_row_metrics,
    pane_metrics,
    select_layout_profile,
)


@pytest.mark.parametrize(
    ("width", "height", "profile", "gap", "left", "center", "right"),
    [
        (1366, 768, "compact", 22, 232, 668, 422),
        (1440, 900, "standard", 26, 248, 704, 436),
        (1920, 1080, "wide", 34, 300, 980, 572),
    ],
)
def test_reference_three_column_workbench_is_exact(
    width,
    height,
    profile,
    gap,
    left,
    center,
    right,
):
    layout = build_operator_layout(width, height)

    assert layout.profile.name == profile
    assert layout.panes.gap == gap
    assert (
        layout.panes.left_width,
        layout.panes.center_width,
        layout.panes.right_width,
    ) == (left, center, right)
    assert layout.panes.occupied_width == width


@pytest.mark.parametrize(
    ("width", "height", "profile"),
    [
        (1280, 1024, "small"),
        (1366, 768, "compact"),
        (1440, 900, "standard"),
        (1920, 1080, "wide"),
        (2560, 1080, "wide"),
        (2560, 1392, "wide"),
    ],
)
@pytest.mark.parametrize("scale", [1.0, 1.4])
def test_supported_sizes_never_overrun_actual_content(width, height, profile, scale):
    layout = build_operator_layout(width, height, scale)

    if scale == 1.0:
        assert layout.profile.name == profile
    assert layout.panes.occupied_width == width
    assert layout.panes.left_width > 0
    assert layout.panes.center_width > 0
    assert layout.panes.right_width > 0
    assert layout.center.reserved_height <= height
    assert layout.center.live_list_min_height >= 112
    assert layout.center.actions.rows == 2
    assert layout.center.actions.columns == 2
    assert layout.center.actions.total_height == (
        layout.center.actions.button_height * 2 + layout.center.actions.row_gap
    )


def test_center_contract_keeps_live_scan_list_below_input_and_above_detail():
    metrics = center_row_metrics(668, 768, profile="compact")

    assert metrics.headline_height > metrics.notice_height
    assert metrics.stage_rail_height >= 42
    assert metrics.notice_height >= 40
    assert metrics.scan_input_height >= 48
    assert metrics.live_list_min_height > metrics.detail_height
    assert metrics.actions.button_height >= 40


def test_large_text_short_display_preserves_all_fixed_rows_and_actions():
    layout = build_operator_layout(1366, 768, 1.4)
    center = layout.center

    assert center.reserved_height <= 768
    assert center.live_list_min_height >= 140
    assert center.headline_height >= 48
    assert center.stage_rail_height >= 42
    assert center.notice_height >= 40
    assert center.scan_input_height >= 48
    assert center.detail_height >= 34
    assert center.actions.button_height >= 50


def test_compact_wide_compact_round_trip_is_pure_and_reversible():
    compact_before = build_operator_layout(1366, 768, 1.0)
    wide = build_operator_layout(2560, 1392, 1.0)
    compact_after = build_operator_layout(1366, 768, 1.0)

    assert compact_before == compact_after
    assert wide != compact_before
    assert wide.profile.name == "wide"
    assert compact_after.profile.name == "compact"


def test_metrics_are_frozen_values_not_mutable_resize_state():
    layout = build_operator_layout(1440, 900)

    with pytest.raises(FrozenInstanceError):
        layout.panes.center_width = 1


@pytest.mark.parametrize(
    ("width", "height", "scale", "expected"),
    [
        (1280, 1024, 1.0, "small"),
        (1366, 768, 1.0, "compact"),
        (1440, 900, 1.0, "standard"),
        (1920, 1080, 1.0, "wide"),
        (1440, 900, 1.4, "small"),
        (1920, 1080, 1.4, "compact"),
        (2560, 1392, 1.4, "standard"),
    ],
)
def test_profile_uses_effective_content_size(width, height, scale, expected):
    assert select_layout_profile(width, height, scale).name == expected


@pytest.mark.parametrize("height", [1120, 1170, 1220])
def test_scaled_ultrawide_short_work_area_stays_standard(height):
    profile = select_layout_profile(2520, height, 1.2)

    assert profile.name == "standard"
    assert profile.effective_width == pytest.approx(2100)
    assert profile.effective_height < 1040


def test_wide_effective_height_boundary_is_stable():
    assert select_layout_profile(1920, 1039, 1.0).name == "standard"
    assert select_layout_profile(1920, 1040, 1.0).name == "wide"


@pytest.mark.parametrize("width", [1920, 2560])
def test_1080_high_work_area_remains_wide_at_default_scale(width):
    assert select_layout_profile(width, 1080, 1.0).name == "wide"


def test_invalid_dimensions_and_scale_fail_fast():
    with pytest.raises(TypeError, match="content_width"):
        build_operator_layout("1366", 768)
    with pytest.raises(ValueError, match="scale"):
        build_operator_layout(1366, 768, float("nan"))
    with pytest.raises(ValueError, match="unknown layout profile"):
        pane_metrics(1366, 768, profile="television")
