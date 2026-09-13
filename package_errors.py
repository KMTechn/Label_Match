"""Shared package failure types and bounded retry metadata."""

from __future__ import annotations

import math
from typing import Any

MAX_RETRY_AFTER_SECONDS = 1800.0


class PackageLogisticsError(RuntimeError):
    pass


class PackageTransportError(PackageLogisticsError):
    pass


class PackageApiError(PackageLogisticsError):
    def __init__(
        self,
        status_code: int,
        code: str,
        message: str,
        *,
        retryable: bool | None = None,
        committed: bool | None = None,
        retry_after_seconds: float | None = None,
    ):
        normalized_code = str(code or "PACKAGE_API_ERROR")
        normalized_message = str(message or "package command rejected")
        super().__init__(f"{normalized_code}: {normalized_message}")
        self.status_code = int(status_code)
        self.code = normalized_code
        self.message = normalized_message
        self.retryable = retryable if isinstance(retryable, bool) else None
        self.committed = committed if isinstance(committed, bool) else None
        self.retry_after_seconds = _bounded_retry_after_seconds(
            retry_after_seconds
        )


def _bounded_retry_after_seconds(value: Any) -> float | None:
    if value is None or isinstance(value, bool):
        return None
    try:
        parsed = float(value)
    except (TypeError, ValueError, OverflowError):
        return None
    if not math.isfinite(parsed):
        return None
    return min(MAX_RETRY_AFTER_SECONDS, max(0.0, parsed))
