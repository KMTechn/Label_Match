from __future__ import annotations

from pathlib import Path


CONTRACT_CANDIDATE_EVENT_TYPES = frozenset(
    {
        "PHS_LABEL_ACTIVE_RESOLVED",
        "PHS_LABEL_EXCHANGE_RESULT",
        "PHS_RECONCILIATION_EXCHANGE_RESULT",
        "SEALED_TRANSFER_EXCHANGE_APPLIED",
        "SEALED_TRANSFER_EXCHANGE_ACKED",
    }
)


LOCAL_ONLY_EVENT_TYPES = frozenset(
    {
        "EXACT_RESCAN_STARTED",
        "EXACT_RESCAN_OK",
        "EXACT_RESCAN_COMPLETED",
    }
)


FORBIDDEN_EVENT_TYPES = frozenset()


AUDITED_OUT_OF_CATALOG_EVENT_TYPES = frozenset(
    CONTRACT_CANDIDATE_EVENT_TYPES
    | LOCAL_ONLY_EVENT_TYPES
    | FORBIDDEN_EVENT_TYPES
)


def local_only_event_log_path(
    contract_log_file_path: str | Path,
    *,
    local_events_dir: str | Path,
) -> Path:
    """Return the matching file in the non-relayed local event stream."""

    contract_path = Path(contract_log_file_path)
    filename = contract_path.name
    marker = "작업이벤트로그_"
    if marker in filename:
        filename = filename.replace(marker, "로컬이벤트로그_", 1)
    else:
        filename = f"local-only-{filename}"
    return Path(local_events_dir) / filename


__all__ = [
    "AUDITED_OUT_OF_CATALOG_EVENT_TYPES",
    "CONTRACT_CANDIDATE_EVENT_TYPES",
    "FORBIDDEN_EVENT_TYPES",
    "LOCAL_ONLY_EVENT_TYPES",
    "local_only_event_log_path",
]
