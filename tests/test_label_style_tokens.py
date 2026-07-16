from dataclasses import FrozenInstanceError
from pathlib import Path

import pytest

from ui.style_tokens import (
    DEFAULT_FONT_FAMILY,
    build_style_tokens,
)


@pytest.mark.parametrize("profile", ["small", "compact", "standard", "wide"])
@pytest.mark.parametrize("scale", [1.0, 1.4])
def test_fonts_spacing_and_buttons_are_scaled_immutable_tokens(profile, scale):
    tokens = build_style_tokens(profile, scale)

    assert tokens.profile == profile
    assert tokens.scale == scale
    assert tokens.fonts.family == DEFAULT_FONT_FAMILY
    assert tokens.fonts.headline >= 24
    assert tokens.fonts.scan_input >= 16
    assert tokens.spacing.xxs < tokens.spacing.xxl
    assert tokens.buttons.min_height >= 40
    assert tokens.buttons.border_width >= 2
    with pytest.raises(FrozenInstanceError):
        tokens.fonts.body = 99


def test_large_text_increases_fonts_targets_and_spacing_without_mutation():
    normal = build_style_tokens("compact", 1.0)
    large = build_style_tokens("compact", 1.4)
    normal_again = build_style_tokens("compact", 1.0)

    assert normal == normal_again
    assert large.fonts.body > normal.fonts.body
    assert large.fonts.headline > normal.fonts.headline
    assert large.spacing.md > normal.spacing.md
    assert large.buttons.min_height > normal.buttons.min_height


def test_statuses_have_text_labels_and_distinct_semantic_borders():
    statuses = build_style_tokens().statuses

    assert statuses.waiting.label == "대기"
    assert statuses.active.label == "스캔 진행 중"
    assert statuses.success.label == "완료"
    assert statuses.warning.label == "주의"
    assert statuses.error.label == "오류"
    assert statuses.recovered.label == "복구됨"
    assert statuses.readonly.label == "조회 전용"
    assert len(
        {
            statuses.waiting.border,
            statuses.active.border,
            statuses.success.border,
            statuses.warning.border,
            statuses.error.border,
            statuses.recovered.border,
            statuses.readonly.border,
        }
    ) == 7


def test_status_and_surface_colors_are_hex_rgb_values():
    tokens = build_style_tokens("wide")
    colors = [
        tokens.surfaces.canvas,
        tokens.surfaces.card,
        tokens.surfaces.text,
        tokens.surfaces.primary,
    ]
    for status in (
        tokens.statuses.waiting,
        tokens.statuses.active,
        tokens.statuses.success,
        tokens.statuses.warning,
        tokens.statuses.error,
        tokens.statuses.recovered,
        tokens.statuses.readonly,
    ):
        colors.extend((status.foreground, status.background, status.border))

    assert all(color.startswith("#") and len(color) == 7 for color in colors)


def test_modules_do_not_import_tk_or_another_application_runtime():
    repo_root = Path(__file__).resolve().parents[1]
    source = "\n".join(
        (repo_root / "ui" / name).read_text(encoding="utf-8")
        for name in ("operator_layout.py", "style_tokens.py")
    ).lower()

    assert "tkinter" not in source
    assert "inspection_worker" not in source
    assert "container_audit" not in source
    assert "rework_worker" not in source


def test_unknown_profile_fails_fast():
    with pytest.raises(ValueError, match="unknown style profile"):
        build_style_tokens("giant")
