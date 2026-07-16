"""Read-only adapter from Label Match runtime state to presentation state.

Only already-accepted scan state is adapted here.  This module intentionally
does not parse barcodes or import Tk, persistence, logistics, ledger, or API
code.  Callers keep ownership of the mutable runtime mappings and lists; the
returned :class:`WorkflowSnapshot` contains detached immutable tuples.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any

from ui.workflow_view_state import WorkflowNotice, WorkflowSnapshot


QA_TOTAL = 5


def adapt_workflow_snapshot(
    current_set_info: Mapping[str, Any],
    *,
    initialized: bool = True,
    loading: bool = False,
    loading_message: str = "",
    history_readonly: bool = False,
    history_loading: bool = False,
    recovered: bool = False,
    completion_kind: str | None = None,
    blocking_notice: WorkflowNotice | Mapping[str, Any] | None = None,
    last_normal_scan_override: str | None = None,
    has_error: bool | None = None,
    error_message: str = "",
) -> WorkflowSnapshot:
    """Copy runtime and transient UI state into an immutable snapshot.

    ``raw`` and ``parsed`` are the two representations of the same accepted QA
    scans.  A count mismatch, or a count beyond the five-step workflow, is a
    corrupt state boundary and is rejected instead of being hidden in the UI.
    The presenter receives ``raw`` values because this surface is the operator's
    actual scan list; ``parsed`` remains the business-side comparison value and
    is used here only to verify the accepted-scan boundary.

    ``has_error`` may be supplied for a transient error that has not been
    written to ``current_set_info`` yet.  It can add an error but cannot hide
    an error already recorded by ``has_error_or_reset``.
    """

    if not isinstance(current_set_info, Mapping):
        raise TypeError("current_set_info must be a Mapping")

    raw_scans = _detached_sequence(current_set_info.get("raw"), field="raw")
    parsed_scans = _detached_sequence(
        current_set_info.get("parsed"),
        field="parsed",
    )
    _validate_accepted_scan_counts(raw_scans, parsed_scans)

    exact_barcodes = _detached_sequence(
        current_set_info.get("exact_rescan_barcodes"),
        field="exact_rescan_barcodes",
    )
    exact_target = _nonnegative_int(
        current_set_info.get("exact_rescan_target_count", 0),
        field="exact_rescan_target_count",
    )
    notice = _detached_notice(blocking_notice)
    error_flag = bool(
        current_set_info.get("has_error_or_reset", False)
        or (has_error if has_error is not None else False)
    )

    return WorkflowSnapshot(
        qa_scans=raw_scans,
        initialized=bool(initialized),
        loading=bool(loading),
        loading_message=loading_message,
        has_error=error_flag,
        error_message=error_message,
        completion_kind=completion_kind,
        blocking_notice=notice,
        last_normal_scan_override=last_normal_scan_override,
        recovered=bool(recovered),
        history_readonly=bool(history_readonly),
        history_loading=bool(history_loading),
        sealed_transfer=bool(current_set_info.get("sealed_transfer")),
        exact_rescan_active=bool(
            current_set_info.get("exact_rescan_active", False)
        ),
        exact_rescan_complete=bool(
            current_set_info.get("exact_rescan_complete", False)
        ),
        exact_rescan_target=exact_target,
        exact_rescan_barcodes=exact_barcodes,
    )


def _detached_sequence(value: Any, *, field: str) -> tuple[Any, ...]:
    if value is None:
        return ()
    if isinstance(value, (str, bytes, bytearray)) or not isinstance(value, Sequence):
        raise TypeError(f"{field} must be a sequence of scan values")
    return tuple(value)


def _validate_accepted_scan_counts(
    raw_scans: tuple[Any, ...],
    parsed_scans: tuple[Any, ...],
) -> None:
    raw_count = len(raw_scans)
    parsed_count = len(parsed_scans)
    if raw_count != parsed_count:
        raise ValueError(
            "accepted scan count mismatch: "
            f"raw={raw_count}, parsed={parsed_count}"
        )
    if raw_count > QA_TOTAL:
        raise ValueError(
            f"accepted scan count cannot exceed {QA_TOTAL}: {raw_count}"
        )


def _nonnegative_int(value: Any, *, field: str) -> int:
    try:
        normalized = int(value or 0)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{field} must be a nonnegative integer") from exc
    if normalized < 0:
        raise ValueError(f"{field} must be a nonnegative integer")
    return normalized


def _detached_notice(
    value: WorkflowNotice | Mapping[str, Any] | None,
) -> WorkflowNotice | None:
    if value is None:
        return None
    if isinstance(value, WorkflowNotice):
        return value
    if not isinstance(value, Mapping):
        raise TypeError("blocking_notice must be one WorkflowNotice or Mapping")
    return WorkflowNotice(
        title=value.get("title", ""),
        message=value.get("message", ""),
        kind=value.get("kind", "blocking"),
        tone=value.get("tone", "danger"),
    )
