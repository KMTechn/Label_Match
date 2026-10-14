"""Shared package failure types and bounded retry metadata."""

from __future__ import annotations

import math
from typing import Any

MAX_RETRY_AFTER_SECONDS = 1800.0


class PackageLogisticsError(RuntimeError):
    pass


class PackageRecoveryEvidenceError(PackageLogisticsError):
    """A fixed startup refusal; stored receipt contents never enter the message."""

    code = 'PACKAGE_RECOVERY_STORED_EVIDENCE_INVALID'
    message = '저장된 포장 회수 증거를 확인하지 못했습니다. 자료를 보존하고 관리자에게 복구를 요청하세요.'

    def __init__(self):
        super().__init__(self.code + ': ' + self.message)


class PackageRecoveryStorageError(PackageLogisticsError):
    """A storage read failure does not classify the stored evidence as damaged."""

    code = 'PACKAGE_RECOVERY_STORAGE_UNAVAILABLE'
    message = '관리자에게 알리세요'

    def __init__(self):
        super().__init__(self.code + ': ' + self.message)


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
