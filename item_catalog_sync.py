"""Central-authority startup sync for the shared four-column item catalog."""

from __future__ import annotations

import csv
import datetime as dt
import hashlib
import hmac
import io
import json
import logging
import os
import tempfile
from http.client import responses as HTTP_STATUS_REASONS
from pathlib import Path
from typing import Any, Callable, Mapping
from urllib.parse import urlsplit

import requests


logger = logging.getLogger(__name__)

CATALOG_PATH = "/inbound/api/item-catalog.csv"
DEFAULT_SERVER_BASE_URL = "https://worker.kmtecherp.com"
REQUIRED_HEADER = ("Item Code", "Item Name", "Spec", "Tray Image")
ACTIVE_PATH_ENV = "KMTECH_ITEM_CATALOG_ACTIVE_PATH"
URL_ENV = "KMTECH_ITEM_CATALOG_URL"
AUTHENTICATED_CATALOG_HOST = "worker.kmtecherp.com"
AUTHENTICATED_CATALOG_AUTHORITIES = (
    AUTHENTICATED_CATALOG_HOST,
    f"{AUTHENTICATED_CATALOG_HOST}:443",
)
LOGISTICS_PROGRAM = "Label_Match"
CACHE_AUTHORITY_SCHEMA = "kmtech.item-catalog.authority.v2"
CACHE_RECOVERY_SCHEMA = "kmtech.item-catalog.recovery.v1"
CACHE_HMAC_KEY_LABEL = b"kmtech:item-catalog-cache:v2:key"
CACHE_HMAC_DOMAIN = b"kmtech:item-catalog-cache:v2:record\0"
CATALOG_DIAGNOSTIC_SCHEMA = "kmtech.label-match.item-catalog-startup-diagnostic.v1"
PROFILE_LOAD_FAILED = "PROFILE_LOAD_FAILED"
PROFILE_INCOMPLETE = "PROFILE_INCOMPLETE"
URL_NOT_TRUSTED = "URL_NOT_TRUSTED"
REQUEST_FAILED_NO_CACHE = "REQUEST_FAILED_NO_CACHE"
SNAPSHOT_UNAVAILABLE_AFTER_VERIFY = "SNAPSHOT_UNAVAILABLE_AFTER_VERIFY"
SNAPSHOT_PARSE_FAILED = "SNAPSHOT_PARSE_FAILED"
ITEM_CATALOG_CAUSE_CODES = frozenset(
    {
        PROFILE_LOAD_FAILED,
        PROFILE_INCOMPLETE,
        URL_NOT_TRUSTED,
        REQUEST_FAILED_NO_CACHE,
        SNAPSHOT_UNAVAILABLE_AFTER_VERIFY,
        SNAPSHOT_PARSE_FAILED,
    }
)
BASE_URL_ENV_NAMES = (
    "WORKER_ANALYSIS_SERVER_URL",
    "WORKER_ANALYSIS_LOGISTICS_API_BASE_URL",
    "CONTAINER_AUDIT_DIRECT_SYNC_SERVER_BASE_URL",
    "LABEL_MATCH_DIRECT_SYNC_SERVER_BASE_URL",
    "DEFECT_DIRECT_SYNC_SERVER_BASE_URL",
    "INSPECTION_DIRECT_SYNC_SERVER_BASE_URL",
    "REWORK_DIRECT_SYNC_SERVER_BASE_URL",
    "DEFECT_SERVER_BASE_URL",
    "KMTECH_SERVER_BASE_URL",
)

_VERIFIED_CATALOG_SNAPSHOTS: dict[str, bytes] = {}
_REQUIRED_VERIFIED_CATALOG_SNAPSHOT_PATHS: set[str] = set()
_CATALOG_ATTEMPT_CONTEXT: dict[str, object] = {}
_CATALOG_SOURCES = frozenset(
    {"UNKNOWN", "CENTRAL_REFRESH", "VERIFIED_CACHE", "LOCAL_CACHE", "BUNDLED"}
)
_CACHE_STATES = frozenset(
    {
        "NOT_CHECKED",
        "ABSENT",
        "CATALOG_INVALID",
        "AUTHORITY_MISSING",
        "AUTHORITY_OR_HMAC_INVALID",
        "RECOVERY_INVALID",
        "VALID_AUTHENTICATED",
        "VALID_AUTHENTICATED_RECOVERY",
        "VALID_UNAUTHENTICATED",
    }
)


def _catalog_url_components(
    url: str,
    *,
    redacted_values: tuple[str, ...] = (),
) -> dict[str, object]:
    def clean(value: object) -> str:
        text = str(value or "")
        for secret in redacted_values:
            if secret:
                text = text.replace(secret, "[REDACTED]")
        return text

    try:
        parsed = urlsplit(str(url or ""))
        try:
            port = parsed.port
        except ValueError:
            port = None
        return {
            "scheme": clean(parsed.scheme.lower()),
            "host": clean((parsed.hostname or "").lower()),
            "port": port,
            "path": clean(parsed.path),
        }
    except ValueError:
        return {"scheme": "", "host": "", "port": None, "path": ""}


def _empty_catalog_attempt_context() -> dict[str, object]:
    return {
        "catalog_url": _catalog_url_components(""),
        "request_sent": False,
        "http_status_code": None,
        "http_reason_phrase": "",
        "pre_send_rejection_code": None,
        "central_enrolled": False,
        "profile_present": False,
        "qualification_authority_id_present": False,
        "tls_ca_bundle_configured": False,
        "exception_type": "",
        "catalog_source": "UNKNOWN",
        "cache_path": "",
        "cache_catalog_present": False,
        "cache_authority_present": False,
        "cache_recovery_present": False,
        "legacy_cache_present": False,
        "cache_state": "NOT_CHECKED",
        "cache_used": False,
        "cache_last_modified_utc": "UNKNOWN",
    }


def _reset_catalog_attempt_context(url: str = "") -> None:
    global _CATALOG_ATTEMPT_CONTEXT
    _CATALOG_ATTEMPT_CONTEXT = _empty_catalog_attempt_context()
    _CATALOG_ATTEMPT_CONTEXT["catalog_url"] = _catalog_url_components(url)


def _update_catalog_attempt_context(**values: object) -> None:
    _CATALOG_ATTEMPT_CONTEXT.update(values)


def get_catalog_attempt_context() -> dict[str, object]:
    context = dict(_CATALOG_ATTEMPT_CONTEXT or _empty_catalog_attempt_context())
    context["catalog_url"] = dict(context["catalog_url"])
    return context


def _profile_secret_values(profile: object) -> tuple[str, ...]:
    return tuple(
        value
        for value in (
            str(getattr(profile, "bearer_token", "") or "").strip(),
            str(getattr(profile, "device_id", "") or "").strip(),
            str(getattr(profile, "source_host_id", "") or "").strip(),
            str(
                getattr(profile, "isolated_qualification_authority_id", "") or ""
            ).strip(),
        )
        if value
    )


def _bounded_http_reason_phrase(status_code: int | None, value: object) -> str:
    if status_code is None:
        return ""
    canonical = str(HTTP_STATUS_REASONS.get(int(status_code), "") or "")
    candidate = " ".join(str(value or "").split())
    if canonical and candidate.casefold() == canonical.casefold():
        return canonical
    return "UNAVAILABLE"


def _bounded_exception_type(value: object) -> str:
    text = str(value or "")
    return "".join(
        character
        for character in text[:200]
        if character.isalnum() or character in "._"
    )


def _bounded_timestamp(value: object) -> str:
    text = str(value or "").strip()
    if text == "UNKNOWN":
        return text
    try:
        parsed = dt.datetime.fromisoformat(text)
    except ValueError:
        return "UNKNOWN"
    if parsed.tzinfo is None:
        return "UNKNOWN"
    return parsed.astimezone(dt.timezone.utc).isoformat()


def _sanitized_catalog_attempt_context(
    context: Mapping[str, object],
) -> dict[str, object]:
    raw_url = context.get("catalog_url")
    url = dict(raw_url) if isinstance(raw_url, Mapping) else {}
    try:
        port = int(url["port"]) if url.get("port") is not None else None
    except (TypeError, ValueError):
        port = None
    if port is not None and not 1 <= port <= 65535:
        port = None
    try:
        status_code = (
            int(context["http_status_code"])
            if context.get("http_status_code") is not None
            else None
        )
    except (TypeError, ValueError):
        status_code = None
    if status_code is not None and not 100 <= status_code <= 599:
        status_code = None
    rejection_code = str(context.get("pre_send_rejection_code") or "")
    if rejection_code not in ITEM_CATALOG_CAUSE_CODES:
        rejection_code = ""
    catalog_source = str(context.get("catalog_source") or "UNKNOWN")
    if catalog_source not in _CATALOG_SOURCES:
        catalog_source = "UNKNOWN"
    cache_state = str(context.get("cache_state") or "NOT_CHECKED")
    if cache_state not in _CACHE_STATES:
        cache_state = "NOT_CHECKED"
    return {
        "catalog_url": {
            "scheme": str(url.get("scheme") or "")[:32],
            "host": str(url.get("host") or "")[:253],
            "port": port,
            "path": str(url.get("path") or "")[:2048],
        },
        "request_sent": bool(context.get("request_sent")),
        "http_status_code": status_code,
        "http_reason_phrase": _bounded_http_reason_phrase(
            status_code,
            context.get("http_reason_phrase"),
        ),
        "pre_send_rejection_code": rejection_code or None,
        "central_enrolled": bool(context.get("central_enrolled")),
        "profile_present": bool(context.get("profile_present")),
        "qualification_authority_id_present": bool(
            context.get("qualification_authority_id_present")
        ),
        "tls_ca_bundle_configured": bool(
            context.get("tls_ca_bundle_configured")
        ),
        "exception_type": _bounded_exception_type(context.get("exception_type")),
        "catalog_source": catalog_source,
        "cache_path": str(context.get("cache_path") or "")[:4096],
        "cache_catalog_present": bool(context.get("cache_catalog_present")),
        "cache_authority_present": bool(context.get("cache_authority_present")),
        "cache_recovery_present": bool(context.get("cache_recovery_present")),
        "legacy_cache_present": bool(context.get("legacy_cache_present")),
        "cache_state": cache_state,
        "cache_used": bool(context.get("cache_used")),
        "cache_last_modified_utc": _bounded_timestamp(
            context.get("cache_last_modified_utc")
        ),
    }


class ItemCatalogSyncError(RuntimeError):
    """Raised when an enrolled PC cannot establish a central catalog baseline."""

    def __init__(
        self,
        message: str,
        *,
        cause_code: str,
        diagnostic_context: Mapping[str, object] | None = None,
    ) -> None:
        if cause_code not in ITEM_CATALOG_CAUSE_CODES:
            raise ValueError(f"unsupported item catalog cause code: {cause_code}")
        super().__init__(message)
        context = dict(diagnostic_context or get_catalog_attempt_context())
        context["catalog_url"] = dict(
            context.get("catalog_url") or _catalog_url_components("")
        )
        if not str(context.get("exception_type") or ""):
            context["exception_type"] = type(self).__name__
        self.cause_code = cause_code
        self.diagnostic_context = context

    def diagnostic_payload(self) -> dict[str, object]:
        return {
            "schema": CATALOG_DIAGNOSTIC_SCHEMA,
            "status": "FAIL",
            "recorded_at_utc": dt.datetime.now(dt.timezone.utc).isoformat(),
            "cause_code": self.cause_code,
            **get_catalog_attempt_context_from_error(self),
        }


def get_catalog_attempt_context_from_error(
    error: ItemCatalogSyncError,
) -> dict[str, object]:
    return _sanitized_catalog_attempt_context(error.diagnostic_context)


def write_item_catalog_failure_diagnostic(
    path: str | Path,
    error: ItemCatalogSyncError,
) -> Path:
    destination = Path(path)
    payload = json.dumps(
        error.diagnostic_payload(),
        ensure_ascii=True,
        sort_keys=True,
        separators=(",", ":"),
    )
    _atomic_write(destination, (payload + "\n").encode("utf-8"))
    return destination


def write_item_catalog_startup_diagnostic(path: str | Path) -> Path:
    """Persist the latest successful central-refresh or verified-cache decision."""

    context = _sanitized_catalog_attempt_context(get_catalog_attempt_context())
    payload = json.dumps(
        {
            "schema": CATALOG_DIAGNOSTIC_SCHEMA,
            "status": "DEGRADED_CACHE" if context["cache_used"] else "READY",
            "recorded_at_utc": dt.datetime.now(dt.timezone.utc).isoformat(),
            "cause_code": None,
            **context,
        },
        ensure_ascii=True,
        sort_keys=True,
        separators=(",", ":"),
    )
    destination = Path(path)
    _atomic_write(destination, (payload + "\n").encode("utf-8"))
    return destination


def _catalog_snapshot_key(path: str | Path) -> str:
    try:
        resolved = Path(path).resolve(strict=False)
    except OSError:
        resolved = Path(os.path.abspath(os.fspath(path)))
    return os.path.normcase(str(resolved))


def _forget_verified_catalog_snapshot(path: str | Path) -> None:
    _VERIFIED_CATALOG_SNAPSHOTS.pop(_catalog_snapshot_key(path), None)


def _remember_verified_catalog_snapshot(path: str | Path, payload: bytes) -> None:
    _VERIFIED_CATALOG_SNAPSHOTS[_catalog_snapshot_key(path)] = bytes(payload)


def get_verified_catalog_snapshot(path: str | Path) -> bytes | None:
    """Return the exact immutable bytes accepted by the central authority check."""

    return _VERIFIED_CATALOG_SNAPSHOTS.get(_catalog_snapshot_key(path))


def requires_verified_catalog_snapshot(path: str | Path) -> bool:
    """Return whether this path was selected during a centrally enrolled refresh."""

    return _catalog_snapshot_key(path) in _REQUIRED_VERIFIED_CATALOG_SNAPSHOT_PATHS


def default_cache_path(environ: Mapping[str, str] | None = None) -> Path:
    values = os.environ if environ is None else environ
    local_app_data = str(values.get("LOCALAPPDATA") or "").strip()
    root = Path(local_app_data) if local_app_data else Path.home() / ".kmtech"
    return root / "KMTech" / "ItemCatalog" / LOGISTICS_PROGRAM / "Item.csv"


def resolve_catalog_url(environ: Mapping[str, str] | None = None) -> str:
    values = os.environ if environ is None else environ
    override = str(values.get(URL_ENV) or "").strip()
    if override:
        return override
    base_url = next(
        (
            str(values.get(name) or "").strip()
            for name in BASE_URL_ENV_NAMES
            if str(values.get(name) or "").strip()
        ),
        DEFAULT_SERVER_BASE_URL,
    )
    return base_url.rstrip("/") + CATALOG_PATH


def _load_item_catalog_logistics_profile() -> Any | None:
    from logistics_runtime_profile import load_logistics_runtime_profile

    profile = load_logistics_runtime_profile(required=None)
    if profile is None:
        return None
    qualification_authority_id_present = bool(
        str(
            getattr(profile, "isolated_qualification_authority_id", "") or ""
        ).strip()
    )
    secrets = _profile_secret_values(profile)
    profile_base_url = str(getattr(profile, "base_url", "") or "").strip()
    _update_catalog_attempt_context(
        catalog_url=_catalog_url_components(
            profile_base_url.rstrip("/") + CATALOG_PATH if profile_base_url else "",
            redacted_values=secrets,
        ),
        central_enrolled=True,
        profile_present=True,
        qualification_authority_id_present=qualification_authority_id_present,
        tls_ca_bundle_configured=bool(
            str(getattr(profile, "tls_ca_bundle_path", "") or "").strip()
        ),
    )
    if not all(
        (
            str(profile.bearer_token or "").strip(),
            str(profile.source_host_id or "").strip(),
            str(profile.device_id or "").strip(),
            str(profile.base_url or "").strip(),
        )
    ):
        _update_catalog_attempt_context(
            pre_send_rejection_code=PROFILE_INCOMPLETE,
            exception_type=ItemCatalogSyncError.__name__,
        )
        raise ItemCatalogSyncError(
            "central item catalog profile is incomplete",
            cause_code=PROFILE_INCOMPLETE,
        )
    return profile


def _profile_bound_catalog_urls(base_url: str) -> frozenset[str]:
    origin = str(base_url or "").rstrip("/")
    approved = {origin + CATALOG_PATH}
    parsed = urlsplit(origin)
    if parsed.scheme == "https" and parsed.port is None and parsed.hostname:
        approved.add(f"https://{parsed.netloc}:443{CATALOG_PATH}")
    return frozenset(approved)


def _is_trusted_authenticated_catalog_url(url: str, profile: Any | None = None) -> bool:
    try:
        parsed = urlsplit(url)
        production_trusted = (
            parsed.scheme == "https"
            and parsed.netloc in AUTHENTICATED_CATALOG_AUTHORITIES
            and parsed.hostname == AUTHENTICATED_CATALOG_HOST
            and parsed.port in (None, 443)
            and parsed.username is None
            and parsed.password is None
            and parsed.path == CATALOG_PATH
            and not parsed.query
            and not parsed.fragment
        )
        if profile is not None:
            return url in _profile_bound_catalog_urls(str(profile.base_url))
        return production_trusted
    except ValueError:
        return False


def validate_catalog_bytes(payload: bytes) -> None:
    if payload.startswith(b"\xef\xbb\xbf"):
        raise ValueError("item catalog must be UTF-8 without BOM")
    rows = list(csv.reader(io.StringIO(payload.decode("utf-8"), newline="")))
    if not rows or tuple(rows[0]) != REQUIRED_HEADER:
        raise ValueError("item catalog header mismatch")
    if len(rows) < 2:
        raise ValueError("item catalog has no data rows")
    item_codes: list[str] = []
    for row in rows[1:]:
        if len(row) != len(REQUIRED_HEADER):
            raise ValueError("item catalog row must contain exactly four columns")
        item_code = row[0].strip()
        if not item_code:
            raise ValueError("item catalog contains an empty item code")
        item_codes.append(item_code)
    if len(item_codes) != len(set(item_codes)):
        raise ValueError("item catalog contains duplicate item codes")
    if item_codes != sorted(item_codes):
        raise ValueError("item catalog item codes are not sorted")


def _is_valid_catalog(path: Path) -> bool:
    try:
        validate_catalog_bytes(path.read_bytes())
        return True
    except (OSError, UnicodeError, ValueError, csv.Error):
        return False


def _atomic_write(path: Path, payload: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.is_file() and path.read_bytes() == payload:
        return
    temporary_path: Path | None = None
    try:
        with tempfile.NamedTemporaryFile(
            mode="wb",
            dir=path.parent,
            prefix=f".{path.name}.",
            suffix=".tmp",
            delete=False,
        ) as handle:
            handle.write(payload)
            handle.flush()
            os.fsync(handle.fileno())
            temporary_path = Path(handle.name)
        os.replace(temporary_path, path)
        temporary_path = None
    finally:
        if temporary_path is not None:
            temporary_path.unlink(missing_ok=True)


def _cache_authority_path(cache: Path) -> Path:
    return cache.with_name(f"{cache.name}.authority.json")


def _last_good_cache_path(cache: Path) -> Path:
    return cache.with_name(f"{cache.name}.last-good")


def _cache_recovery_path(cache: Path) -> Path:
    return cache.with_name(f"{cache.name}.recovery.json")


def _legacy_cache_path_for(cache: Path) -> Path | None:
    if (
        cache.name == "Item.csv"
        and cache.parent.name == LOGISTICS_PROGRAM
        and cache.parent.parent.name == "ItemCatalog"
    ):
        return cache.parent.parent / "Item.csv"
    return None


def _path_modified_utc(path: Path) -> str:
    try:
        return dt.datetime.fromtimestamp(
            path.stat().st_mtime,
            tz=dt.timezone.utc,
        ).isoformat()
    except OSError:
        return "UNKNOWN"


def _initial_cache_state(cache: Path) -> str:
    if not cache.is_file():
        return "ABSENT"
    if not _is_valid_catalog(cache):
        return "CATALOG_INVALID"
    if not _cache_authority_path(cache).is_file():
        return "AUTHORITY_MISSING"
    return "AUTHORITY_OR_HMAC_INVALID"


def _update_cache_attempt_context(cache: Path) -> None:
    legacy_cache = _legacy_cache_path_for(cache)
    _update_catalog_attempt_context(
        cache_path=str(cache),
        cache_catalog_present=cache.is_file(),
        cache_authority_present=_cache_authority_path(cache).is_file(),
        cache_recovery_present=_cache_recovery_path(cache).is_file(),
        legacy_cache_present=bool(legacy_cache and legacy_cache.is_file()),
        cache_state=_initial_cache_state(cache),
        cache_used=False,
        cache_last_modified_utc="UNKNOWN",
    )


def _mark_catalog_source(
    path: Path,
    *,
    source: str,
    cache_state: str,
    cache_used: bool,
) -> None:
    _update_catalog_attempt_context(
        catalog_source=source,
        cache_state=cache_state,
        cache_used=cache_used,
        cache_last_modified_utc=_path_modified_utc(path),
    )


def _cache_authority_record(
    payload: bytes,
    *,
    url: str,
    source_host_id: str,
    device_id: str,
) -> dict[str, object]:
    return {
        "schema": CACHE_AUTHORITY_SCHEMA,
        "catalog_sha256": hashlib.sha256(payload).hexdigest(),
        "url": (
            DEFAULT_SERVER_BASE_URL + CATALOG_PATH
            if _is_trusted_authenticated_catalog_url(url)
            else url
        ),
        "source_host_id": source_host_id,
        "device_id": device_id,
        "program": LOGISTICS_PROGRAM,
    }


def _same_catalog_cache_authority(stored_url: object, current_url: object) -> bool:
    """Allow a verified cache to survive an enrolled endpoint port change.

    The catalog remains bound to HTTPS, host, and the one catalog path. The
    port is deliberately excluded because an enrolled endpoint can move ports
    (or be pointed at a closed same-host port during a network-outage drill)
    without changing the authority that issued the HMAC-protected snapshot.
    """

    try:
        stored = urlsplit(str(stored_url or ""))
        current = urlsplit(str(current_url or ""))
        # Accessing ``port`` also rejects malformed or out-of-range values.
        stored.port
        current.port
    except ValueError:
        return False
    for parsed in (stored, current):
        if (
            parsed.scheme.lower() != "https"
            or not parsed.hostname
            or parsed.netloc.endswith(":")
            or parsed.username is not None
            or parsed.password is not None
            or parsed.path != CATALOG_PATH
            or parsed.query
            or parsed.fragment
        ):
            return False
    return stored.hostname.lower() == current.hostname.lower()


def _write_authenticated_cache(
    cache: Path,
    payload: bytes,
    *,
    url: str,
    source_host_id: str,
    device_id: str,
    bearer_token: str,
) -> None:
    authority = _cache_authority_record(
        payload,
        url=url,
        source_host_id=source_host_id,
        device_id=device_id,
    )
    authority["cache_hmac_sha256"] = _cache_authority_hmac(
        payload,
        authority,
        bearer_token=bearer_token,
    )
    authority_bytes = (_canonical_json(authority) + "\n").encode("utf-8")
    recovery = {
        "schema": CACHE_RECOVERY_SCHEMA,
        "authority": authority,
        "catalog_utf8": payload.decode("utf-8"),
    }
    _atomic_write(
        _cache_recovery_path(cache),
        (_canonical_json(recovery) + "\n").encode("utf-8"),
    )
    _atomic_write(cache, payload)
    _atomic_write(_cache_authority_path(cache), authority_bytes)
    last_good = _last_good_cache_path(cache)
    last_good.unlink(missing_ok=True)
    _cache_authority_path(last_good).unlink(missing_ok=True)


def _read_authenticated_cache_payload(
    cache: Path,
    *,
    url: str,
    source_host_id: str,
    device_id: str,
    bearer_token: str,
) -> bytes | None:
    try:
        payload = cache.read_bytes()
        validate_catalog_bytes(payload)
        authority = json.loads(_cache_authority_path(cache).read_text(encoding="utf-8"))
    except (OSError, UnicodeError, ValueError, TypeError, json.JSONDecodeError, csv.Error):
        return None
    if not _is_valid_authenticated_payload(
        payload,
        authority,
        url=url,
        source_host_id=source_host_id,
        device_id=device_id,
        bearer_token=bearer_token,
    ):
        return None
    return payload


def _is_valid_authenticated_payload(
    payload: bytes,
    authority: object,
    *,
    url: str,
    source_host_id: str,
    device_id: str,
    bearer_token: str,
) -> bool:
    if not isinstance(authority, dict):
        return False
    unsigned_authority = dict(authority)
    supplied_hmac = unsigned_authority.pop("cache_hmac_sha256", None)
    expected = _cache_authority_record(
        payload,
        url=url,
        source_host_id=source_host_id,
        device_id=device_id,
    )
    stored_url = unsigned_authority.pop("url", None)
    expected_url = expected.pop("url")
    if (
        unsigned_authority != expected
        or not _same_catalog_cache_authority(stored_url, expected_url)
        or not isinstance(supplied_hmac, str)
        or len(supplied_hmac) != 64
        or any(char not in "0123456789abcdef" for char in supplied_hmac)
    ):
        return False
    signed_authority = dict(unsigned_authority)
    signed_authority["url"] = stored_url
    expected_hmac = _cache_authority_hmac(
        payload,
        signed_authority,
        bearer_token=bearer_token,
    )
    return hmac.compare_digest(supplied_hmac, expected_hmac)


def _read_authenticated_recovery_payload(
    cache: Path,
    *,
    url: str,
    source_host_id: str,
    device_id: str,
    bearer_token: str,
) -> bytes | None:
    try:
        recovery = json.loads(_cache_recovery_path(cache).read_text(encoding="utf-8"))
        if not isinstance(recovery, dict) or set(recovery) != {
            "schema",
            "authority",
            "catalog_utf8",
        }:
            return None
        if recovery["schema"] != CACHE_RECOVERY_SCHEMA:
            return None
        catalog_text = recovery["catalog_utf8"]
        if not isinstance(catalog_text, str):
            return None
        payload = catalog_text.encode("utf-8")
        validate_catalog_bytes(payload)
    except (OSError, UnicodeError, ValueError, TypeError, json.JSONDecodeError, csv.Error):
        return None
    if not _is_valid_authenticated_payload(
        payload,
        recovery["authority"],
        url=url,
        source_host_id=source_host_id,
        device_id=device_id,
        bearer_token=bearer_token,
    ):
        return None
    return payload


def _recover_authenticated_cache(
    cache: Path,
    *,
    url: str,
    source_host_id: str,
    device_id: str,
    bearer_token: str,
) -> Path | None:
    payload = _read_authenticated_recovery_payload(
        cache,
        url=url,
        source_host_id=source_host_id,
        device_id=device_id,
        bearer_token=bearer_token,
    )
    if payload is None:
        return None
    last_good = _last_good_cache_path(cache)
    try:
        _atomic_write(last_good, payload)
    except OSError:
        try:
            materialized = last_good.is_file() and last_good.read_bytes() == payload
        except OSError:
            return None
        if not materialized:
            return None
    _remember_verified_catalog_snapshot(last_good, payload)
    return last_good


def _canonical_json(payload: dict[str, object]) -> str:
    return json.dumps(
        payload,
        ensure_ascii=True,
        sort_keys=True,
        separators=(",", ":"),
    )


def _cache_authority_hmac(
    payload: bytes,
    authority: dict[str, object],
    *,
    bearer_token: str,
) -> str:
    token_bytes = bearer_token.encode("utf-8")
    if not token_bytes:
        raise ValueError("central item catalog token is empty")
    key = hmac.new(token_bytes, CACHE_HMAC_KEY_LABEL, hashlib.sha256).digest()
    authority_bytes = _canonical_json(authority).encode("utf-8")
    message = (
        CACHE_HMAC_DOMAIN
        + len(authority_bytes).to_bytes(8, "big")
        + authority_bytes
        + len(payload).to_bytes(8, "big")
        + payload
    )
    return hmac.new(key, message, hashlib.sha256).hexdigest()


def _hardened_get(url: str, **kwargs: object) -> object:
    """Send credentials without inheriting process proxy or CA overrides."""

    with requests.Session() as session:
        session.trust_env = False
        return session.get(url, **kwargs)


def refresh_item_catalog(
    bundled_path: str | Path,
    *,
    cache_path: str | Path | None = None,
    url: str | None = None,
    timeout_seconds: float = 2.0,
    get: Callable[..., object] | None = None,
) -> Path:
    bundled = Path(bundled_path)
    cache = Path(cache_path) if cache_path is not None else default_cache_path()
    _reset_catalog_attempt_context(url or "")
    _update_cache_attempt_context(cache)
    last_good = _last_good_cache_path(cache)
    _forget_verified_catalog_snapshot(cache)
    _forget_verified_catalog_snapshot(last_good)
    _REQUIRED_VERIFIED_CATALOG_SNAPSHOT_PATHS.discard(_catalog_snapshot_key(cache))
    _REQUIRED_VERIFIED_CATALOG_SNAPSHOT_PATHS.discard(
        _catalog_snapshot_key(last_good)
    )
    fallback = cache if _is_valid_catalog(cache) else bundled
    try:
        profile = _load_item_catalog_logistics_profile()
    except ItemCatalogSyncError:
        raise
    except Exception as exc:  # noqa: BLE001 - classify without exposing secrets.
        _update_catalog_attempt_context(
            pre_send_rejection_code=PROFILE_LOAD_FAILED,
            exception_type=type(exc).__name__,
        )
        raise ItemCatalogSyncError(
            "central item catalog profile could not be loaded",
            cause_code=PROFILE_LOAD_FAILED,
        ) from None
    central_enrolled = profile is not None
    if central_enrolled:
        _REQUIRED_VERIFIED_CATALOG_SNAPSHOT_PATHS.update(
            {_catalog_snapshot_key(cache), _catalog_snapshot_key(last_good)}
        )
    profile_catalog_url = (
        f"{str(profile.base_url).rstrip('/')}{CATALOG_PATH}"
        if profile is not None
        else ""
    )
    effective_url = url or profile_catalog_url or resolve_catalog_url()
    profile_secrets = _profile_secret_values(profile) if profile is not None else ()
    _update_catalog_attempt_context(
        catalog_url=_catalog_url_components(
            effective_url,
            redacted_values=profile_secrets,
        ),
        central_enrolled=central_enrolled,
        profile_present=profile is not None,
        qualification_authority_id_present=bool(
            profile is not None
            and str(
                getattr(profile, "isolated_qualification_authority_id", "") or ""
            ).strip()
        ),
        tls_ca_bundle_configured=bool(
            profile is not None
            and str(getattr(profile, "tls_ca_bundle_path", "") or "").strip()
        ),
    )
    if central_enrolled and not _is_trusted_authenticated_catalog_url(
        effective_url, profile
    ):
        _update_catalog_attempt_context(
            pre_send_rejection_code=URL_NOT_TRUSTED,
            exception_type=ItemCatalogSyncError.__name__,
        )
        raise ItemCatalogSyncError(
            "central item catalog URL is not trusted",
            cause_code=URL_NOT_TRUSTED,
        )
    try:
        request_kwargs: dict[str, object] = {
            "timeout": timeout_seconds,
            "allow_redirects": False,
        }
        if profile is not None:
            request_kwargs["headers"] = {
                "Authorization": f"Bearer {profile.bearer_token}",
                "X-Logistics-Source-Host-Id": profile.source_host_id,
                "X-Logistics-Device-Id": profile.device_id,
                "X-Logistics-Program": LOGISTICS_PROGRAM,
            }
            tls_ca_bundle_path = str(
                getattr(profile, "tls_ca_bundle_path", "") or ""
            ).strip()
            if tls_ca_bundle_path:
                request_kwargs["verify"] = tls_ca_bundle_path
        transport = get or (_hardened_get if central_enrolled else requests.get)
        _update_catalog_attempt_context(request_sent=True)
        response = transport(effective_url, **request_kwargs)
        status_code = getattr(response, "status_code", None)
        try:
            observed_status_code = (
                int(status_code) if status_code is not None else None
            )
        except (TypeError, ValueError):
            observed_status_code = None
        _update_catalog_attempt_context(
            http_status_code=observed_status_code,
            http_reason_phrase=_bounded_http_reason_phrase(
                observed_status_code,
                getattr(response, "reason", ""),
            ),
        )
        if status_code is not None and 300 <= int(status_code) < 400:
            raise ValueError("item catalog redirects are not allowed")
        response.raise_for_status()
        payload = bytes(response.content)
        validate_catalog_bytes(payload)
        if profile is not None:
            _write_authenticated_cache(
                cache,
                payload,
                url=effective_url,
                source_host_id=str(profile.source_host_id),
                device_id=str(profile.device_id),
                bearer_token=str(profile.bearer_token).strip(),
            )
            _remember_verified_catalog_snapshot(cache, payload)
            _update_cache_attempt_context(cache)
            _mark_catalog_source(
                cache,
                source="CENTRAL_REFRESH",
                cache_state="VALID_AUTHENTICATED",
                cache_used=False,
            )
        else:
            _cache_authority_path(cache).unlink(missing_ok=True)
            _cache_recovery_path(cache).unlink(missing_ok=True)
            last_good.unlink(missing_ok=True)
            _cache_authority_path(last_good).unlink(missing_ok=True)
            _atomic_write(cache, payload)
            _update_cache_attempt_context(cache)
            _mark_catalog_source(
                cache,
                source="CENTRAL_REFRESH",
                cache_state="VALID_UNAUTHENTICATED",
                cache_used=False,
            )
        return cache
    except Exception as exc:  # noqa: BLE001 - persist type, never exception text.
        _update_catalog_attempt_context(exception_type=type(exc).__name__)
        recovered = cache if _is_valid_catalog(cache) else None
        if central_enrolled:
            assert profile is not None
            bearer_token = str(profile.bearer_token).strip()
            cache_payload = _read_authenticated_cache_payload(
                cache,
                url=effective_url,
                source_host_id=str(profile.source_host_id),
                device_id=str(profile.device_id),
                bearer_token=bearer_token,
            )
            if cache_payload is not None:
                _remember_verified_catalog_snapshot(cache, cache_payload)
                authenticated_cache = cache
            else:
                authenticated_cache = _recover_authenticated_cache(
                    cache,
                    url=effective_url,
                    source_host_id=str(profile.source_host_id),
                    device_id=str(profile.device_id),
                    bearer_token=bearer_token,
                )
            if authenticated_cache is not None:
                _update_cache_attempt_context(cache)
                _mark_catalog_source(
                    authenticated_cache,
                    source="VERIFIED_CACHE",
                    cache_state=(
                        "VALID_AUTHENTICATED"
                        if authenticated_cache == cache
                        else "VALID_AUTHENTICATED_RECOVERY"
                    ),
                    cache_used=True,
                )
                logger.warning(
                    "Central item catalog refresh failed; using the last central cache"
                )
                return authenticated_cache
            _update_cache_attempt_context(cache)
            if (
                _cache_recovery_path(cache).is_file()
                and get_catalog_attempt_context()["cache_state"] == "ABSENT"
            ):
                _update_catalog_attempt_context(cache_state="RECOVERY_INVALID")
            raise ItemCatalogSyncError(
                "central item catalog is unavailable and no last central cache exists",
                cause_code=REQUEST_FAILED_NO_CACHE,
            ) from None
        fallback = recovered or fallback
        _mark_catalog_source(
            fallback,
            source="LOCAL_CACHE" if fallback == cache else "BUNDLED",
            cache_state=(
                "VALID_UNAUTHENTICATED" if fallback == cache else "ABSENT"
            ),
            cache_used=fallback == cache,
        )
        logger.warning("Item catalog sync skipped; using %s", fallback)
        return fallback


def is_shared_catalog_cache(path: str | Path) -> bool:
    try:
        candidate = Path(path).resolve(strict=False)
        default = default_cache_path().resolve(strict=False)
        return candidate in {default, _last_good_cache_path(default)}
    except OSError:
        return False
