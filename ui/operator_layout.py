"""Pure responsive metrics for the Label Match operator workbench.

The module deliberately has no Tk dependency.  It converts the *current
content size* into an immutable three-column layout, so every resize is
calculated from fresh input instead of mutating the previous geometry.

The center column keeps one stable vertical sequence::

    headline -> five-stage rail -> one notice -> scan input
             -> live scan list -> detail -> 2 x 2 actions

That sequence is shared by normal, error, completion, and recovery states.
Only the presenter text and semantic state change between those states.
"""

from __future__ import annotations

from dataclasses import dataclass
import math
from typing import Final, Literal, TypeAlias


MIN_UI_SCALE: Final[float] = 0.7
MAX_UI_SCALE: Final[float] = 2.5
DEFAULT_UI_SCALE: Final[float] = 1.0

LayoutProfileName: TypeAlias = Literal["small", "compact", "standard", "wide"]
_PROFILE_NAMES: Final[frozenset[str]] = frozenset(
    {"small", "compact", "standard", "wide"}
)


@dataclass(frozen=True, slots=True)
class LayoutProfile:
    """Profile selected from usable content size and requested UI scale."""

    name: LayoutProfileName
    content_width: int
    content_height: int
    scale: float
    effective_width: float
    effective_height: float


@dataclass(frozen=True, slots=True)
class PaneMetrics:
    """Widths of the persistent left / center / right operator areas."""

    profile: LayoutProfileName
    content_width: int
    content_height: int
    scale: float
    gap: int
    left_width: int
    center_width: int
    right_width: int
    compressed: bool

    @property
    def occupied_width(self) -> int:
        """Physical width consumed by the three panes and two gaps."""

        return self.left_width + self.center_width + self.right_width + self.gap * 2


@dataclass(frozen=True, slots=True)
class ActionGridMetrics:
    """The four operator actions always use a readable 2 by 2 grid."""

    rows: int
    columns: int
    button_height: int
    row_gap: int
    column_gap: int
    total_height: int


@dataclass(frozen=True, slots=True)
class CenterRowMetrics:
    """Vertical budget for the primary scan workflow in the center pane."""

    profile: LayoutProfileName
    content_height: int
    scale: float
    vertical_padding: int
    row_gap: int
    headline_height: int
    stage_rail_height: int
    notice_height: int
    scan_input_height: int
    live_list_min_height: int
    detail_height: int
    actions: ActionGridMetrics
    reserved_height: int
    compressed: bool


@dataclass(frozen=True, slots=True)
class OperatorLayoutMetrics:
    """Complete Tk-independent geometry contract for the workbench."""

    profile: LayoutProfile
    panes: PaneMetrics
    center: CenterRowMetrics


@dataclass(frozen=True, slots=True)
class _PaneRule:
    gap: int
    left_ratio: float
    right_ratio: float
    left_min: int
    left_max: int
    right_min: int
    right_max: int
    center_min: int


@dataclass(frozen=True, slots=True)
class _CenterBase:
    vertical_padding: int
    row_gap: int
    headline: int
    stage_rail: int
    notice: int
    scan_input: int
    live_list: int
    detail: int
    button: int
    action_gap: int


# The three supplied design anchors are intentionally exact at scale 1.0:
# 1366: 232 + 22 + 668 + 22 + 422
# 1440: 248 + 26 + 704 + 26 + 436
# 1920: 300 + 34 + 980 + 34 + 572
_PANE_RULES: Final[dict[LayoutProfileName, _PaneRule]] = {
    "small": _PaneRule(18, 0.170, 0.300, 190, 240, 330, 410, 540),
    "compact": _PaneRule(
        22,
        232 / 1366,
        422 / 1366,
        210,
        280,
        370,
        480,
        600,
    ),
    "standard": _PaneRule(
        26,
        248 / 1440,
        436 / 1440,
        230,
        360,
        390,
        560,
        650,
    ),
    "wide": _PaneRule(
        34,
        300 / 1920,
        572 / 1920,
        260,
        420,
        470,
        720,
        760,
    ),
}


_CENTER_BASES: Final[dict[LayoutProfileName, _CenterBase]] = {
    "small": _CenterBase(12, 8, 56, 48, 42, 52, 170, 42, 44, 8),
    "compact": _CenterBase(14, 9, 64, 52, 46, 56, 190, 44, 46, 9),
    "standard": _CenterBase(16, 10, 72, 58, 50, 60, 220, 48, 50, 10),
    "wide": _CenterBase(18, 12, 80, 64, 54, 64, 250, 52, 54, 12),
}


def _finite_number(value: object, *, name: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise TypeError(f"{name} must be a finite number")
    numeric = float(value)
    if not math.isfinite(numeric):
        raise ValueError(f"{name} must be finite")
    return numeric


def _dimension(value: object, *, name: str, minimum: int = 1) -> int:
    numeric = _finite_number(value, name=name)
    return max(minimum, int(round(numeric)))


def normalize_scale(value: object = DEFAULT_UI_SCALE) -> float:
    """Validate and clamp an operator text/UI scale."""

    numeric = _finite_number(value, name="scale")
    return max(MIN_UI_SCALE, min(MAX_UI_SCALE, numeric))


def select_layout_profile(
    content_width: object,
    content_height: object,
    scale: object = DEFAULT_UI_SCALE,
) -> LayoutProfile:
    """Choose a profile from the current usable content rectangle.

    Dividing by the requested scale prevents a large-text layout from being
    mistaken for a roomy layout.  Width remains the primary signal while the
    height guards the short 768 px operator display.
    """

    width = _dimension(content_width, name="content_width")
    height = _dimension(content_height, name="content_height")
    normalized_scale = normalize_scale(scale)
    effective_width = width / normalized_scale
    effective_height = height / normalized_scale

    if effective_width < 1300 or effective_height < 660:
        name: LayoutProfileName = "small"
    elif effective_width < 1420 or effective_height < 800:
        name = "compact"
    elif effective_width < 1840 or effective_height < 960:
        name = "standard"
    else:
        name = "wide"

    return LayoutProfile(
        name=name,
        content_width=width,
        content_height=height,
        scale=normalized_scale,
        effective_width=effective_width,
        effective_height=effective_height,
    )


def _profile_name(
    profile: LayoutProfile | LayoutProfileName | None,
    *,
    width: int,
    height: int,
    scale: float,
) -> LayoutProfileName:
    if profile is None:
        return select_layout_profile(width, height, scale).name
    if isinstance(profile, LayoutProfile):
        return profile.name
    if isinstance(profile, str) and profile in _PROFILE_NAMES:
        return profile  # type: ignore[return-value]
    raise ValueError(f"unknown layout profile: {profile!r}")


def pane_metrics(
    content_width: object,
    content_height: object,
    scale: object = DEFAULT_UI_SCALE,
    *,
    profile: LayoutProfile | LayoutProfileName | None = None,
) -> PaneMetrics:
    """Return non-overlapping widths for the persistent three-column desk."""

    width = _dimension(content_width, name="content_width", minimum=3)
    height = _dimension(content_height, name="content_height")
    normalized_scale = normalize_scale(scale)
    name = _profile_name(profile, width=width, height=height, scale=normalized_scale)
    rule = _PANE_RULES[name]

    # Gaps grow modestly for large text, without multiplying away the scan
    # area's useful width.  Scale 1.0 preserves all three design anchors.
    gap_scale = 1.0 + max(0.0, min(0.25, (normalized_scale - 1.0) * 0.625))
    gap = max(1, int(round(rule.gap * gap_scale)))
    if gap * 2 >= width:
        gap = max(0, (width - 3) // 2)
    available = max(3, width - gap * 2)

    left = max(rule.left_min, min(rule.left_max, int(round(width * rule.left_ratio))))
    right = max(rule.right_min, min(rule.right_max, int(round(width * rule.right_ratio))))

    # Keep the center at least as large as its content contract permits.  When
    # the physical width is smaller than all preferred minima, preserve a
    # center majority and share the remaining width between the side panes.
    center_floor = min(rule.center_min, max(1, int(round(available * 0.48))))
    side_budget = max(2, available - center_floor)
    requested_side = left + right
    compressed = requested_side > side_budget
    if compressed:
        left_share = left / requested_side if requested_side else 0.36
        left = max(1, min(side_budget - 1, int(round(side_budget * left_share))))
        right = max(1, side_budget - left)

    center = available - left - right
    if center < 1:
        side_budget = available - 1
        left = max(1, int(round(side_budget * 0.36)))
        right = max(1, side_budget - left)
        center = available - left - right
        compressed = True

    return PaneMetrics(
        profile=name,
        content_width=width,
        content_height=height,
        scale=normalized_scale,
        gap=gap,
        left_width=left,
        center_width=center,
        right_width=right,
        compressed=compressed,
    )


def _layout_scale(scale: float) -> float:
    """Let rows grow for large text without blindly scaling every spacer."""

    return max(0.85, min(1.65, 1.0 + (scale - 1.0) * 0.65))


def _scaled(value: int, scale: float, *, minimum: int = 1) -> int:
    return max(minimum, int(round(value * scale)))


def action_grid_metrics(
    profile: LayoutProfile | LayoutProfileName,
    scale: object = DEFAULT_UI_SCALE,
) -> ActionGridMetrics:
    """Return the stable 2 by 2 action-button footprint."""

    normalized_scale = normalize_scale(scale)
    if isinstance(profile, LayoutProfile):
        name = profile.name
    elif isinstance(profile, str) and profile in _PROFILE_NAMES:
        name = profile  # type: ignore[assignment]
    else:
        raise ValueError(f"unknown layout profile: {profile!r}")
    base = _CENTER_BASES[name]
    growth = _layout_scale(normalized_scale)
    button_height = _scaled(base.button, growth, minimum=40)
    row_gap = _scaled(base.action_gap, growth, minimum=6)
    column_gap = _scaled(base.action_gap, growth, minimum=6)
    return ActionGridMetrics(
        rows=2,
        columns=2,
        button_height=button_height,
        row_gap=row_gap,
        column_gap=column_gap,
        total_height=button_height * 2 + row_gap,
    )


def _reserved_height(
    *,
    padding: int,
    gap: int,
    headline: int,
    rail: int,
    notice: int,
    scan_input: int,
    live_list: int,
    detail: int,
    actions: ActionGridMetrics,
) -> int:
    # Seven vertical blocks have six gaps between them.
    return (
        padding * 2
        + gap * 6
        + headline
        + rail
        + notice
        + scan_input
        + live_list
        + detail
        + actions.total_height
    )


def center_row_metrics(
    center_width: object,
    content_height: object,
    scale: object = DEFAULT_UI_SCALE,
    *,
    profile: LayoutProfile | LayoutProfileName | None = None,
) -> CenterRowMetrics:
    """Calculate the center workflow's row heights from fresh dimensions."""

    width = _dimension(center_width, name="center_width")
    height = _dimension(content_height, name="content_height")
    normalized_scale = normalize_scale(scale)
    name = _profile_name(profile, width=width, height=height, scale=normalized_scale)
    base = _CENTER_BASES[name]
    growth = _layout_scale(normalized_scale)
    actions = action_grid_metrics(name, normalized_scale)

    padding = _scaled(base.vertical_padding, growth, minimum=8)
    gap = _scaled(base.row_gap, growth, minimum=5)
    headline = _scaled(base.headline, growth, minimum=48)
    rail = _scaled(base.stage_rail, growth, minimum=42)
    notice = _scaled(base.notice, growth, minimum=40)
    scan_input = _scaled(base.scan_input, growth, minimum=48)
    live_list = _scaled(base.live_list, growth, minimum=112)
    detail = _scaled(base.detail, growth, minimum=34)

    desired_reserved = _reserved_height(
        padding=padding,
        gap=gap,
        headline=headline,
        rail=rail,
        notice=notice,
        scan_input=scan_input,
        live_list=live_list,
        detail=detail,
        actions=actions,
    )
    compressed = desired_reserved > height
    overflow = max(0, desired_reserved - height)

    # Preserve all fixed workflow rows and the physical action targets.  The
    # five-row live list is the flexible region; it shrinks first but remains
    # visible.  Detail and decorative whitespace yield only if still needed.
    list_floor = _scaled(112, min(growth, 1.25), minimum=100)
    reduction = min(overflow, max(0, live_list - list_floor))
    live_list -= reduction
    overflow -= reduction

    detail_floor = _scaled(34, min(growth, 1.20), minimum=30)
    reduction = min(overflow, max(0, detail - detail_floor))
    detail -= reduction
    overflow -= reduction

    minimum_gap = 4
    reduction_per_gap = min(max(0, gap - minimum_gap), math.ceil(overflow / 6))
    gap -= reduction_per_gap
    overflow = max(0, overflow - reduction_per_gap * 6)

    minimum_padding = 6
    reduction_per_edge = min(max(0, padding - minimum_padding), math.ceil(overflow / 2))
    padding -= reduction_per_edge

    reserved = _reserved_height(
        padding=padding,
        gap=gap,
        headline=headline,
        rail=rail,
        notice=notice,
        scan_input=scan_input,
        live_list=live_list,
        detail=detail,
        actions=actions,
    )

    return CenterRowMetrics(
        profile=name,
        content_height=height,
        scale=normalized_scale,
        vertical_padding=padding,
        row_gap=gap,
        headline_height=headline,
        stage_rail_height=rail,
        notice_height=notice,
        scan_input_height=scan_input,
        live_list_min_height=live_list,
        detail_height=detail,
        actions=actions,
        reserved_height=reserved,
        compressed=compressed,
    )


def build_operator_layout(
    content_width: object,
    content_height: object,
    scale: object = DEFAULT_UI_SCALE,
) -> OperatorLayoutMetrics:
    """Build the full operator layout from actual content size."""

    profile = select_layout_profile(content_width, content_height, scale)
    panes = pane_metrics(
        profile.content_width,
        profile.content_height,
        profile.scale,
        profile=profile,
    )
    center = center_row_metrics(
        panes.center_width,
        profile.content_height,
        profile.scale,
        profile=profile,
    )
    return OperatorLayoutMetrics(profile=profile, panes=panes, center=center)


__all__ = [
    "ActionGridMetrics",
    "CenterRowMetrics",
    "DEFAULT_UI_SCALE",
    "LayoutProfile",
    "LayoutProfileName",
    "MAX_UI_SCALE",
    "MIN_UI_SCALE",
    "OperatorLayoutMetrics",
    "PaneMetrics",
    "action_grid_metrics",
    "build_operator_layout",
    "center_row_metrics",
    "normalize_scale",
    "pane_metrics",
    "select_layout_profile",
]
