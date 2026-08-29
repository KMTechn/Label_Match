"""Secret-safe result contract for Label_Match post-deploy canaries.

Application code owns the authentication, runtime-authority, and recovery
probes.  This module owns only the exact three-valued result model and the
machine-readable report boundary, so an absent target can never become PASS.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import re
from typing import Any, Iterable, Mapping, Sequence


CANARY_CONTRACT_VERSION = "kmtech-auth-recovery-canary.v1"
CANARY_STATUSES = frozenset({"PASS", "FAIL", "UNKNOWN"})
MAX_EVIDENCE_TEXT_BYTES = 512
MAX_JSON_INPUT_BYTES = 1024 * 1024
_FORBIDDEN_EVIDENCE_KEY = re.compile(
    r"(?:secret|password|authorization|signature|cookie|request_token)",
    re.IGNORECASE,
)


class CanaryContractError(ValueError):
    """Raised when an adapter attempts to emit an unsafe or invalid report."""


@dataclass(frozen=True)
class CanaryCheck:
    """One required canary observation with an explicit three-valued status."""

    name: str
    status: str
    reason_code: str
    evidence: Mapping[str, Any]

    def as_dict(self) -> dict[str, Any]:
        name = str(self.name or "").strip()
        status = str(self.status or "").strip().upper()
        reason_code = str(self.reason_code or "").strip()
        if not name or not reason_code or status not in CANARY_STATUSES:
            raise CanaryContractError("canary check identity or status is invalid")
        evidence = _safe_json_value(dict(self.evidence), path=f"checks.{name}.evidence")
        if not isinstance(evidence, dict):
            raise CanaryContractError("canary evidence must be a JSON object")
        return {
            "name": name,
            "status": status,
            "reason_code": reason_code,
            "evidence": evidence,
        }


def utc_now_text() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def aggregate_status(checks: Sequence[CanaryCheck]) -> str:
    """FAIL dominates UNKNOWN; PASS is possible only when every check passes."""

    statuses = [str(check.status or "").strip().upper() for check in checks]
    if not statuses or any(status not in CANARY_STATUSES for status in statuses):
        raise CanaryContractError("canary checks are absent or contain an invalid status")
    if "FAIL" in statuses:
        return "FAIL"
    if "UNKNOWN" in statuses:
        return "UNKNOWN"
    return "PASS"


def build_canary_report(
    *,
    app_id: str,
    checks: Sequence[CanaryCheck],
    started_at_utc: str,
    completed_at_utc: str,
    duration_ms: int,
    required_check_names: Sequence[str],
) -> dict[str, Any]:
    """Build an exact report after validating names, statuses, and evidence."""

    app_id = str(app_id or "").strip()
    if not app_id:
        raise CanaryContractError("app_id is required")
    rows = [check.as_dict() for check in checks]
    names = [row["name"] for row in rows]
    required = [str(name or "").strip() for name in required_check_names]
    if not required or any(not name for name in required):
        raise CanaryContractError("required check names are invalid")
    if len(set(names)) != len(names) or names != required:
        raise CanaryContractError("canary check set or order differs from the exact contract")
    summary = {
        status: sum(1 for row in rows if row["status"] == status)
        for status in ("PASS", "FAIL", "UNKNOWN")
    }
    report = {
        "schema": CANARY_CONTRACT_VERSION,
        "app_id": app_id,
        "status": aggregate_status(checks),
        "started_at_utc": str(started_at_utc or ""),
        "completed_at_utc": str(completed_at_utc or ""),
        "duration_ms": max(0, int(duration_ms)),
        "checks": rows,
        "summary": summary,
        "secret_material_recorded": False,
    }
    return _safe_json_value(report, path="report")


def assert_forbidden_values_absent(
    report: Mapping[str, Any], forbidden_values: Iterable[str | bytes]
) -> None:
    """Reject a report containing any runtime credential value."""

    def string_leaves(value: Any) -> Iterable[str]:
        if isinstance(value, str):
            yield value
            return
        if isinstance(value, Mapping):
            for key, child in value.items():
                yield str(key)
                yield from string_leaves(child)
            return
        if isinstance(value, (list, tuple)):
            for child in value:
                yield from string_leaves(child)

    leaves = tuple(string_leaves(report))
    for value in forbidden_values:
        if isinstance(value, bytes):
            try:
                text = value.decode("utf-8")
            except UnicodeDecodeError:
                text = value.hex()
        else:
            text = str(value or "")
        if text and any(text in leaf for leaf in leaves):
            raise CanaryContractError("runtime credential material reached canary output")


def write_json_atomic(path: str | os.PathLike[str], payload: Mapping[str, Any]) -> None:
    """Write one bounded JSON object without following a report-path symlink."""

    requested_path = Path(path).expanduser()
    if requested_path.is_symlink():
        raise CanaryContractError("report path is not a regular file")
    report_path = requested_path.resolve()
    report_path.parent.mkdir(parents=True, exist_ok=True)
    if report_path.exists() and not report_path.is_file():
        raise CanaryContractError("report path is not a regular file")
    safe_payload = _safe_json_value(dict(payload), path="report")
    encoded = (
        json.dumps(safe_payload, ensure_ascii=False, sort_keys=True, indent=2) + "\n"
    ).encode("utf-8")
    temporary = report_path.with_name(f".{report_path.name}.tmp.{os.getpid()}")
    try:
        with temporary.open("xb") as handle:
            handle.write(encoded)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, report_path)
    finally:
        try:
            temporary.unlink()
        except FileNotFoundError:
            pass


def read_bounded_json_object(
    path: str | os.PathLike[str], *, maximum_bytes: int = MAX_JSON_INPUT_BYTES
) -> dict[str, Any]:
    """Read one small regular JSON file and require a top-level object."""

    requested_path = Path(path).expanduser()
    if requested_path.is_symlink():
        raise CanaryContractError("JSON input is absent or is not a regular file")
    selected = requested_path.resolve()
    if not selected.is_file():
        raise CanaryContractError("JSON input is absent or is not a regular file")
    size = selected.stat().st_size
    if size < 2 or size > int(maximum_bytes):
        raise CanaryContractError("JSON input size is outside the bounded contract")
    try:
        payload = json.loads(selected.read_text(encoding="utf-8-sig"))
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise CanaryContractError("JSON input is unreadable") from exc
    if not isinstance(payload, dict):
        raise CanaryContractError("JSON input must contain one object")
    return payload


def _safe_json_value(value: Any, *, path: str) -> Any:
    if value is None or isinstance(value, (bool, int)):
        return value
    if isinstance(value, float):
        if value != value or value in {float("inf"), float("-inf")}:
            raise CanaryContractError(f"{path} contains a non-finite number")
        return value
    if isinstance(value, str):
        if len(value.encode("utf-8")) > MAX_EVIDENCE_TEXT_BYTES:
            raise CanaryContractError(f"{path} contains oversized text")
        if "\r" in value or "\n" in value:
            raise CanaryContractError(f"{path} contains multiline text")
        return value
    if isinstance(value, Mapping):
        result: dict[str, Any] = {}
        for raw_key, child in value.items():
            key = str(raw_key or "").strip()
            if not key or (".evidence" in path and _FORBIDDEN_EVIDENCE_KEY.search(key)):
                raise CanaryContractError(f"{path} contains a forbidden evidence key")
            result[key] = _safe_json_value(child, path=f"{path}.{key}")
        return result
    if isinstance(value, (list, tuple)):
        return [
            _safe_json_value(child, path=f"{path}[{index}]")
            for index, child in enumerate(value)
        ]
    raise CanaryContractError(f"{path} contains a non-JSON value")
