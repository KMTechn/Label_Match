"""Bounded, auditable successor lineage for the Label relay stop marker.

The marker is a safety fence, not an authentication token.  A canonical
installer may legitimately replace it while retiring old persistence.  The v2
shape therefore embeds the exact predecessor marker and its byte hash so a
receipt bound to an older marker can prove a continuous, tamper-evident chain.

Only four successor hops are accepted.  That covers the initial canonical
install and a small number of retry/removal cycles while forcing a fresh
operator receipt instead of trusting an indefinitely growing local history.
"""

from __future__ import annotations

from datetime import datetime
import hashlib
import json
from pathlib import Path
import re
from typing import Any, Mapping


STOP_MARKER_V1 = "label-match-user-relay-stop-v1"
STOP_MARKER_V2 = "label-match-user-relay-stop-v2"
MAX_SUCCESSOR_DEPTH = 4
MAX_MARKER_BYTES = 64 * 1024

_REQUEST_ID = re.compile(r"^[0-9a-f]{32}$")
_SHA256 = re.compile(r"^[0-9a-f]{64}$")


class StopMarkerLineageError(ValueError):
    """Raised when a stop-marker lineage is missing, malformed, or untrusted."""


def canonical_marker_bytes(marker: Mapping[str, Any]) -> bytes:
    """Return the exact UTF-8 representation used by the atomic marker writer."""

    return (
        json.dumps(
            dict(marker),
            ensure_ascii=False,
            indent=2,
            sort_keys=True,
        )
        + "\n"
    ).encode("utf-8")


def marker_sha256(marker: Mapping[str, Any]) -> str:
    return hashlib.sha256(canonical_marker_bytes(marker)).hexdigest()


def _required_request_id(value: Any, label: str) -> str:
    text = str(value or "")
    if _REQUEST_ID.fullmatch(text) is None:
        raise StopMarkerLineageError(f"{label} request_id is invalid")
    return text


def _required_time(value: Any, label: str) -> str:
    text = str(value or "")
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError as exc:
        raise StopMarkerLineageError(f"{label} requested_at is invalid") from exc
    if parsed.tzinfo is None:
        raise StopMarkerLineageError(f"{label} requested_at has no timezone")
    return text


def _validated_node(
    marker: Mapping[str, Any],
    *,
    seen_request_ids: set[str] | None = None,
) -> tuple[dict[str, Any], int]:
    value = dict(marker)
    seen = set() if seen_request_ids is None else seen_request_ids
    schema = value.get("schema_version")
    request_id = _required_request_id(value.get("request_id"), "stop marker")
    _required_time(value.get("requested_at"), "stop marker")
    if request_id in seen:
        raise StopMarkerLineageError("stop marker lineage repeats a request_id")
    seen.add(request_id)

    if schema == STOP_MARKER_V1:
        if set(value) != {"schema_version", "request_id", "requested_at"}:
            raise StopMarkerLineageError("v1 stop marker fields differ")
        return value, 0

    if schema != STOP_MARKER_V2:
        raise StopMarkerLineageError("stop marker schema is unsupported")
    expected = {
        "schema_version",
        "request_id",
        "requested_at",
        "lineage_depth",
        "predecessor_sha256",
        "predecessor_marker",
    }
    if set(value) != expected:
        raise StopMarkerLineageError("v2 stop marker fields differ")
    predecessor = value.get("predecessor_marker")
    if not isinstance(predecessor, Mapping):
        raise StopMarkerLineageError("stop marker predecessor is invalid")
    predecessor_hash = str(value.get("predecessor_sha256") or "").lower()
    if _SHA256.fullmatch(predecessor_hash) is None:
        raise StopMarkerLineageError("stop marker predecessor hash is invalid")
    predecessor_value, predecessor_depth = _validated_node(
        predecessor,
        seen_request_ids=seen,
    )
    if marker_sha256(predecessor_value) != predecessor_hash:
        raise StopMarkerLineageError("stop marker predecessor hash differs")
    depth = value.get("lineage_depth")
    if (
        isinstance(depth, bool)
        or not isinstance(depth, int)
        or depth != predecessor_depth + 1
        or not 1 <= depth <= MAX_SUCCESSOR_DEPTH
    ):
        raise StopMarkerLineageError("stop marker lineage depth is invalid")
    return value, depth


def read_stop_marker(path: str | Path) -> tuple[dict[str, Any], bytes, str]:
    marker_path = Path(path).expanduser().resolve()
    if not marker_path.is_file():
        raise StopMarkerLineageError("relay stop marker is absent")
    size = marker_path.stat().st_size
    if size <= 0 or size > MAX_MARKER_BYTES:
        raise StopMarkerLineageError("relay stop marker size is invalid")
    raw = marker_path.read_bytes()
    try:
        value = json.loads(raw.decode("utf-8-sig"))
    except (UnicodeError, json.JSONDecodeError) as exc:
        raise StopMarkerLineageError("relay stop marker JSON is invalid") from exc
    if not isinstance(value, dict):
        raise StopMarkerLineageError("relay stop marker is not an object")
    return value, raw, hashlib.sha256(raw).hexdigest()


def build_successor_marker(
    path: str | Path,
    *,
    request_id: str,
    requested_at: str,
) -> dict[str, Any]:
    """Build, but do not write, a strict successor of the marker at ``path``."""

    predecessor, raw, predecessor_hash = read_stop_marker(path)
    predecessor, predecessor_depth = _validated_node(predecessor)
    if raw != canonical_marker_bytes(predecessor):
        raise StopMarkerLineageError(
            "existing stop marker is not canonical and cannot be inherited"
        )
    if predecessor_depth >= MAX_SUCCESSOR_DEPTH:
        raise StopMarkerLineageError(
            "stop marker lineage limit reached; a fresh receipt is required"
        )
    _required_request_id(request_id, "successor stop marker")
    _required_time(requested_at, "successor stop marker")
    successor = {
        "schema_version": STOP_MARKER_V2,
        "request_id": request_id,
        "requested_at": requested_at,
        "lineage_depth": predecessor_depth + 1,
        "predecessor_sha256": predecessor_hash,
        "predecessor_marker": predecessor,
    }
    _validated_node(successor)
    return successor


def validate_marker_successor_lineage(
    path: str | Path,
    *,
    anchor_request_id: str,
    anchor_sha256: str,
) -> dict[str, Any]:
    """Prove that the live marker is the anchor or a bounded valid successor."""

    marker, raw, current_hash = read_stop_marker(path)
    anchor_id = _required_request_id(anchor_request_id, "receipt stop marker")
    anchor_hash = str(anchor_sha256 or "").lower()
    if _SHA256.fullmatch(anchor_hash) is None:
        raise StopMarkerLineageError("receipt stop marker hash is invalid")

    if current_hash == anchor_hash and marker.get("request_id") == anchor_id:
        return {
            "status": "EXACT",
            "current_request_id": anchor_id,
            "current_sha256": current_hash,
            "successor_hops": 0,
            "max_successor_hops": MAX_SUCCESSOR_DEPTH,
        }

    marker, depth = _validated_node(marker)
    if raw != canonical_marker_bytes(marker):
        raise StopMarkerLineageError("successor stop marker is not canonical")
    matches = 0
    hops = 0
    current: dict[str, Any] = marker
    current_node_hash = current_hash
    anchor_hops = -1
    while True:
        if (
            current.get("request_id") == anchor_id
            and current_node_hash == anchor_hash
        ):
            matches += 1
            anchor_hops = hops
        if current.get("schema_version") != STOP_MARKER_V2:
            break
        predecessor = dict(current["predecessor_marker"])
        current_node_hash = str(current["predecessor_sha256"])
        current = predecessor
        hops += 1
    if matches != 1 or anchor_hops < 1:
        raise StopMarkerLineageError(
            "live stop marker is not a verified successor of the receipt marker"
        )
    return {
        "status": "SUCCESSOR",
        "current_request_id": str(marker["request_id"]),
        "current_sha256": current_hash,
        "successor_hops": anchor_hops,
        "lineage_depth": depth,
        "max_successor_hops": MAX_SUCCESSOR_DEPTH,
    }


__all__ = [
    "MAX_SUCCESSOR_DEPTH",
    "STOP_MARKER_V1",
    "STOP_MARKER_V2",
    "StopMarkerLineageError",
    "build_successor_marker",
    "canonical_marker_bytes",
    "marker_sha256",
    "read_stop_marker",
    "validate_marker_successor_lineage",
]
