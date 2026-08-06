"""Best-effort startup sync for the shared four-column item catalog."""

from __future__ import annotations

import csv
import io
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


def default_cache_path(environ: Mapping[str, str] | None = None) -> Path:
    values = os.environ if environ is None else environ
    local_app_data = str(values.get("LOCALAPPDATA") or "").strip()
    root = Path(local_app_data) if local_app_data else Path.home() / ".kmtech"
    return root / "KMTech" / "ItemCatalog" / "Item.csv"


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
    try:
        from logistics_runtime_profile import load_logistics_runtime_profile

        return load_logistics_runtime_profile(required=None)
    except Exception:  # noqa: BLE001
        logger.warning(
            "Item catalog logistics credentials are unavailable; "
            "continuing without authorization"
        )
        return None


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


def refresh_item_catalog(
    bundled_path: str | Path,
    *,
    cache_path: str | Path | None = None,
    url: str | None = None,
    timeout_seconds: float = 2.0,
    get: Callable[..., object] = requests.get,
) -> Path:
    bundled = Path(bundled_path)
    cache = Path(cache_path) if cache_path is not None else default_cache_path()
    fallback = cache if _is_valid_catalog(cache) else bundled
    try:
        effective_url = url or resolve_catalog_url()
        request_kwargs: dict[str, object] = {
            "timeout": timeout_seconds,
            "allow_redirects": False,
        }
        profile = _load_item_catalog_logistics_profile()
        if profile is not None:
            if not _is_trusted_authenticated_catalog_url(effective_url):
                raise ValueError("authenticated item catalog URL is not trusted")
            request_kwargs["headers"] = {
                "Authorization": f"Bearer {profile.bearer_token}",
                "X-Logistics-Source-Host-Id": profile.source_host_id,
                "X-Logistics-Device-Id": profile.device_id,
                "X-Logistics-Program": LOGISTICS_PROGRAM,
            }
        response = get(effective_url, **request_kwargs)
        status_code = getattr(response, "status_code", None)
        if status_code is not None and 300 <= int(status_code) < 400:
            raise ValueError("item catalog redirects are not allowed")
        response.raise_for_status()
        payload = bytes(response.content)
        validate_catalog_bytes(payload)
        _atomic_write(cache, payload)
        return cache
    except Exception:  # noqa: BLE001
        recovered = cache if _is_valid_catalog(cache) else fallback
        logger.warning("Item catalog sync skipped; using %s", recovered)
        return recovered


def is_shared_catalog_cache(path: str | Path) -> bool:
    try:
        return Path(path).resolve(strict=False) == default_cache_path().resolve(strict=False)
    except OSError:
        return False
