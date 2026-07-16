"""Immutable, Tk-independent visual tokens for the Label Match workbench."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Final

from .operator_layout import (
    DEFAULT_UI_SCALE,
    LayoutProfileName,
    normalize_scale,
)


DEFAULT_FONT_FAMILY: Final[str] = "Malgun Gothic"


@dataclass(frozen=True, slots=True)
class SurfaceColorTokens:
    canvas: str = "#F9FAFB"
    sidebar: str = "#FFFFFF"
    card: str = "#FFFFFF"
    surface_alt: str = "#F3F4F6"
    input: str = "#FFFFFF"
    text: str = "#111827"
    text_subtle: str = "#6B7280"
    border: str = "#D1D5DB"
    border_strong: str = "#9CA3AF"
    primary: str = "#3B82F6"
    primary_active: str = "#2563EB"
    disabled: str = "#E5E7EB"


@dataclass(frozen=True, slots=True)
class StatusColor:
    """A semantic state remains identifiable through its Korean label."""

    label: str
    foreground: str
    background: str
    border: str


@dataclass(frozen=True, slots=True)
class StatusColorTokens:
    waiting: StatusColor
    active: StatusColor
    success: StatusColor
    warning: StatusColor
    error: StatusColor
    recovered: StatusColor
    readonly: StatusColor


@dataclass(frozen=True, slots=True)
class FontTokens:
    family: str
    caption: int
    body: int
    sidebar_body: int
    button: int
    section_title: int
    headline: int
    stage_rail: int
    scan_input: int
    live_list: int
    detail: int


@dataclass(frozen=True, slots=True)
class SpacingTokens:
    xxs: int
    xs: int
    sm: int
    md: int
    lg: int
    xl: int
    xxl: int


@dataclass(frozen=True, slots=True)
class ButtonStyleTokens:
    min_height: int
    horizontal_padding: int
    border_width: int


@dataclass(frozen=True, slots=True)
class StyleTokens:
    profile: LayoutProfileName
    scale: float
    surfaces: SurfaceColorTokens
    statuses: StatusColorTokens
    fonts: FontTokens
    spacing: SpacingTokens
    buttons: ButtonStyleTokens


@dataclass(frozen=True, slots=True)
class _StyleBase:
    fonts: tuple[int, int, int, int, int, int, int, int, int, int]
    spacing: tuple[int, int, int, int, int, int, int]
    button_height: int
    button_padding: int


_STYLE_BASES: Final[dict[LayoutProfileName, _StyleBase]] = {
    "small": _StyleBase(
        (10, 12, 12, 12, 15, 28, 13, 18, 12, 11),
        (2, 4, 6, 8, 12, 16, 24),
        44,
        12,
    ),
    "compact": _StyleBase(
        (10, 13, 13, 13, 16, 32, 14, 20, 13, 12),
        (2, 4, 7, 9, 14, 20, 28),
        46,
        14,
    ),
    "standard": _StyleBase(
        (11, 14, 14, 14, 18, 36, 15, 22, 14, 13),
        (3, 5, 8, 12, 18, 24, 32),
        50,
        16,
    ),
    "wide": _StyleBase(
        (12, 15, 15, 15, 20, 40, 16, 24, 15, 14),
        (4, 6, 10, 14, 20, 28, 38),
        54,
        18,
    ),
}


DEFAULT_SURFACES: Final[SurfaceColorTokens] = SurfaceColorTokens()


def _status_colors() -> StatusColorTokens:
    return StatusColorTokens(
        waiting=StatusColor("대기", "#4B5563", "#F3F4F6", "#9CA3AF"),
        active=StatusColor("스캔 진행 중", "#1D4ED8", "#DBEAFE", "#3B82F6"),
        success=StatusColor("완료", "#047857", "#D1FAE5", "#059669"),
        warning=StatusColor("주의", "#92400E", "#FEF3C7", "#D97706"),
        error=StatusColor("오류", "#B91C1C", "#FEE2E2", "#DC2626"),
        recovered=StatusColor("복구됨", "#5B21B6", "#EDE9FE", "#7C3AED"),
        readonly=StatusColor("조회 전용", "#374151", "#E5E7EB", "#6B7280"),
    )


DEFAULT_STATUS_COLORS: Final[StatusColorTokens] = _status_colors()


def normalize_profile(profile: LayoutProfileName | str) -> LayoutProfileName:
    normalized = str(profile).strip().lower()
    if normalized not in _STYLE_BASES:
        choices = ", ".join(_STYLE_BASES)
        raise ValueError(f"unknown style profile {profile!r}; expected one of: {choices}")
    return normalized  # type: ignore[return-value]


def _scaled(value: int, scale: float, *, minimum: int = 1) -> int:
    return max(minimum, int(round(value * scale)))


def build_style_tokens(
    profile: LayoutProfileName | str = "standard",
    scale: object = DEFAULT_UI_SCALE,
    *,
    surfaces: SurfaceColorTokens = DEFAULT_SURFACES,
    statuses: StatusColorTokens = DEFAULT_STATUS_COLORS,
) -> StyleTokens:
    """Build a fresh, immutable token set for one content-size profile."""

    normalized_profile = normalize_profile(profile)
    normalized_scale = normalize_scale(scale)
    base = _STYLE_BASES[normalized_profile]
    (
        caption,
        body,
        sidebar_body,
        button,
        section_title,
        headline,
        stage_rail,
        scan_input,
        live_list,
        detail,
    ) = base.fonts

    fonts = FontTokens(
        family=DEFAULT_FONT_FAMILY,
        caption=_scaled(caption, normalized_scale, minimum=9),
        body=_scaled(body, normalized_scale, minimum=11),
        sidebar_body=_scaled(sidebar_body, normalized_scale, minimum=11),
        button=_scaled(button, normalized_scale, minimum=11),
        section_title=_scaled(section_title, normalized_scale, minimum=14),
        headline=_scaled(headline, normalized_scale, minimum=24),
        stage_rail=_scaled(stage_rail, normalized_scale, minimum=12),
        scan_input=_scaled(scan_input, normalized_scale, minimum=16),
        live_list=_scaled(live_list, normalized_scale, minimum=11),
        detail=_scaled(detail, normalized_scale, minimum=10),
    )
    spacing = SpacingTokens(
        *(_scaled(value, normalized_scale) for value in base.spacing)
    )
    buttons = ButtonStyleTokens(
        min_height=_scaled(base.button_height, normalized_scale, minimum=40),
        horizontal_padding=_scaled(base.button_padding, normalized_scale, minimum=10),
        border_width=_scaled(2, normalized_scale, minimum=2),
    )
    return StyleTokens(
        profile=normalized_profile,
        scale=normalized_scale,
        surfaces=surfaces,
        statuses=statuses,
        fonts=fonts,
        spacing=spacing,
        buttons=buttons,
    )


DEFAULT_STYLE_TOKENS: Final[StyleTokens] = build_style_tokens()


__all__ = [
    "ButtonStyleTokens",
    "DEFAULT_FONT_FAMILY",
    "DEFAULT_STATUS_COLORS",
    "DEFAULT_STYLE_TOKENS",
    "DEFAULT_SURFACES",
    "FontTokens",
    "SpacingTokens",
    "StatusColor",
    "StatusColorTokens",
    "StyleTokens",
    "SurfaceColorTokens",
    "build_style_tokens",
    "normalize_profile",
]
