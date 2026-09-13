"""Item.csv v1 validation and authenticated-cache framing, without app state.

URL policy, identity/profile loading, paths, writer admission and durable
recovery orchestration belong to the caller. See docs/spec/contracts.md.
"""
from __future__ import annotations

import csv
import hashlib
import hmac
import io
import json
from pathlib import Path
from typing import Callable

CATALOG_SCHEMA_VERSION = 1
REQUIRED_HEADER = ("Item Code", "Item Name", "Spec", "Tray Image")
CACHE_AUTHORITY_SCHEMA = "kmtech.item-catalog.authority.v2"
CACHE_RECOVERY_SCHEMA = "kmtech.item-catalog.recovery.v1"
CACHE_HMAC_KEY_LABEL = b"kmtech:item-catalog-cache:v2:key"
CACHE_HMAC_DOMAIN = b"kmtech:item-catalog-cache:v2:record\0"



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


def cache_authority_path(cache: Path) -> Path:
    return cache.with_name(f"{cache.name}.authority.json")


def last_good_cache_path(cache: Path) -> Path:
    return cache.with_name(f"{cache.name}.last-good")


def cache_recovery_path(cache: Path) -> Path:
    return cache.with_name(f"{cache.name}.recovery.json")


def canonical_json(payload: dict[str, object]) -> str:
    return json.dumps(
        payload,
        ensure_ascii=True,
        sort_keys=True,
        separators=(",", ":"),
    )


def cache_authority_hmac(
    payload: bytes,
    authority: dict[str, object],
    *,
    bearer_token: str,
) -> str:
    token_bytes = bearer_token.encode("utf-8")
    if not token_bytes:
        raise ValueError("central item catalog token is empty")
    key = hmac.new(token_bytes, CACHE_HMAC_KEY_LABEL, hashlib.sha256).digest()
    authority_bytes = canonical_json(authority).encode("utf-8")
    message = (
        CACHE_HMAC_DOMAIN
        + len(authority_bytes).to_bytes(8, "big")
        + authority_bytes
        + len(payload).to_bytes(8, "big")
        + payload
    )
    return hmac.new(key, message, hashlib.sha256).hexdigest()


def cache_authority_record(
    payload: bytes,
    *,
    url: str,
    program: str,
    canonicalize_url: Callable[[str], str],
    source_host_id: str,
    device_id: str,
) -> dict[str, object]:
    return {
        "schema": CACHE_AUTHORITY_SCHEMA,
        "catalog_sha256": hashlib.sha256(payload).hexdigest(),
        "url": canonicalize_url(url),
        "source_host_id": source_host_id,
        "device_id": device_id,
        "program": program,
    }


def is_valid_authenticated_payload(
    payload: bytes,
    authority: object,
    *,
    url: str,
    program: str,
    canonicalize_url: Callable[[str], str],
    source_host_id: str,
    device_id: str,
    bearer_token: str,
    same_authority: Callable[[object, object], bool],
) -> bool:
    if not isinstance(authority, dict):
        return False
    unsigned_authority = dict(authority)
    supplied_hmac = unsigned_authority.pop("cache_hmac_sha256", None)
    expected = cache_authority_record(
        payload,
        url=url,
        program=program,
        canonicalize_url=canonicalize_url,
        source_host_id=source_host_id,
        device_id=device_id,
    )
    stored_url = unsigned_authority.pop("url", None)
    expected_url = expected.pop("url")
    if (
        unsigned_authority != expected
        or not same_authority(stored_url, expected_url)
        or not isinstance(supplied_hmac, str)
        or len(supplied_hmac) != 64
        or any(char not in "0123456789abcdef" for char in supplied_hmac)
    ):
        return False
    signed_authority = dict(unsigned_authority)
    signed_authority["url"] = stored_url
    expected_hmac = cache_authority_hmac(
        payload,
        signed_authority,
        bearer_token=bearer_token,
    )
    return hmac.compare_digest(supplied_hmac, expected_hmac)
