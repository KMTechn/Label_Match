"""Central-authority startup sync for the shared four-column item catalog."""

from __future__ import annotations

import csv
import hashlib
import hmac
import io
import json
import logging
import os
import tempfile
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


class ItemCatalogSyncError(RuntimeError):
    """Raised when an enrolled PC cannot establish a central catalog baseline."""


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
    if not all(
        (
            str(profile.bearer_token or "").strip(),
            str(profile.source_host_id or "").strip(),
            str(profile.device_id or "").strip(),
        )
    ):
        raise ItemCatalogSyncError("central item catalog profile is incomplete")
    return profile


def _is_trusted_authenticated_catalog_url(url: str) -> bool:
    try:
        parsed = urlsplit(url)
        return (
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
    if (
        unsigned_authority != expected
        or not isinstance(supplied_hmac, str)
        or len(supplied_hmac) != 64
        or any(char not in "0123456789abcdef" for char in supplied_hmac)
    ):
        return False
    expected_hmac = _cache_authority_hmac(
        payload,
        expected,
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
    except Exception:  # noqa: BLE001 - never expose profile details or secrets.
        raise ItemCatalogSyncError(
            "central item catalog profile could not be loaded"
        ) from None
    central_enrolled = profile is not None
    if central_enrolled:
        _REQUIRED_VERIFIED_CATALOG_SNAPSHOT_PATHS.update(
            {_catalog_snapshot_key(cache), _catalog_snapshot_key(last_good)}
        )
    effective_url = url or resolve_catalog_url()
    if central_enrolled and not _is_trusted_authenticated_catalog_url(effective_url):
        raise ItemCatalogSyncError("central item catalog URL is not trusted")
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
        transport = get or (_hardened_get if central_enrolled else requests.get)
        response = transport(effective_url, **request_kwargs)
        status_code = getattr(response, "status_code", None)
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
        else:
            _cache_authority_path(cache).unlink(missing_ok=True)
            _cache_recovery_path(cache).unlink(missing_ok=True)
            last_good.unlink(missing_ok=True)
            _cache_authority_path(last_good).unlink(missing_ok=True)
            _atomic_write(cache, payload)
        return cache
    except Exception:  # noqa: BLE001 - log only a generic, non-secret status.
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
                logger.warning(
                    "Central item catalog refresh failed; using the last central cache"
                )
                return authenticated_cache
            raise ItemCatalogSyncError(
                "central item catalog is unavailable and no last central cache exists"
            ) from None
        fallback = recovered or fallback
        logger.warning("Item catalog sync skipped; using %s", fallback)
        return fallback


def is_shared_catalog_cache(path: str | Path) -> bool:
    try:
        candidate = Path(path).resolve(strict=False)
        default = default_cache_path().resolve(strict=False)
        return candidate in {default, _last_good_cache_path(default)}
    except OSError:
        return False
