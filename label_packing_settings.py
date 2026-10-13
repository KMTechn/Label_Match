"""Read-only packing "top scan count" from the company web (packing-settings-v1).

The web setting is the only source.  The app reads it at start and when a
work session starts, keeps the last received value on this PC, and never
writes it back.  A failed or invalid read never stops packing: the last
received value (or the default 4) is used and a warning band is shown.

Each packing set freezes the count and the received version when it opens;
the helpers below read that frozen value from a set record.  Records written
before this feature carry no value and mean count 4 / version 0, so the
default is stored as an absent key and old records stay byte-identical.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import sys
import uuid
from typing import Any, Callable, Mapping
from urllib.parse import urlsplit

from writer_session_fence import writer_sink


CONTRACT = "packing-settings-v1"
SETTINGS_PATH = "/inbound/api/packing-settings.json"
DEFAULT_TOP_SCAN_COUNT = 4
DEFAULT_VERSION = 0
MIN_TOP_SCAN_COUNT = 1
MAX_TOP_SCAN_COUNT = 6
UNAVAILABLE = "PACKING_SETTINGS_UNAVAILABLE"
INVALID = "PACKING_SETTINGS_INVALID"
VERSION_INVALID = "PACKING_SETTINGS_VERSION_INVALID"
SOURCE_SERVER = "server"
SOURCE_LAST_SAVED = "last_saved"
SOURCE_DEFAULT = "default"
SET_COUNT_KEY = "top_scan_count"
SET_VERSION_KEY = "packing_settings_version"
STORE_FILENAME = "packing-settings.json"
_STORE_SCHEMA = "kmtech.label-match.packing-settings.v1"
_PROGRAM = "Label_Match"


class PackingSettingsReadError(Exception):
    """A settings read that the app replaces with its fallback value."""

    def __init__(self, code: str, message: str = "") -> None:
        super().__init__(message or code)
        self.code = code


def valid_count(value: Any) -> bool:
    return type(value) is int and MIN_TOP_SCAN_COUNT <= value <= MAX_TOP_SCAN_COUNT


def valid_version(value: Any) -> bool:
    return type(value) is int and 0 <= value <= 2**63 - 1


def parse_payload(payload: Any) -> dict[str, Any]:
    """Validate one 200 body; any deviation is PACKING_SETTINGS_INVALID."""

    if isinstance(payload, (bytes, bytearray)):
        try:
            payload = json.loads(bytes(payload).decode("utf-8"))
        except (UnicodeDecodeError, ValueError) as exc:
            raise PackingSettingsReadError(INVALID, "settings body is not JSON") from exc
    if not isinstance(payload, Mapping):
        raise PackingSettingsReadError(INVALID, "settings body is not an object")
    if payload.get("contract") != CONTRACT:
        raise PackingSettingsReadError(INVALID, "settings contract differs")
    count = payload.get("top_scan_count")
    version = payload.get("version")
    updated_at = payload.get("updated_at")
    if not valid_count(count):
        raise PackingSettingsReadError(INVALID, "top_scan_count out of range")
    if not valid_version(version):
        raise PackingSettingsReadError(INVALID, "version is invalid")
    if updated_at is not None:
        try:
            if type(updated_at) is not str or datetime.fromisoformat(updated_at).tzinfo is None:
                raise ValueError
        except ValueError as exc:
            raise PackingSettingsReadError(INVALID, "updated_at is invalid") from exc
    return {"top_scan_count": count, "version": version, "updated_at": updated_at}


def _load_profile():
    from logistics_runtime_profile import load_logistics_runtime_profile

    return load_logistics_runtime_profile(required=None)


def _profile_settings_url(profile: Any) -> str:
    base = str(getattr(profile, "base_url", "") or "").strip().rstrip("/")
    return base + SETTINGS_PATH if base else ""


def _trusted_settings_url(url: str, profile: Any) -> bool:
    """The item-catalog trust rule: only the enrolled profile's own origin."""

    try:
        parsed = urlsplit(url)
        origin = urlsplit(str(getattr(profile, "base_url", "") or "").strip().rstrip("/"))
    except ValueError:
        return False
    return bool(
        parsed.scheme == "https"
        and parsed.hostname
        and parsed.username is None
        and parsed.password is None
        and (parsed.scheme, parsed.netloc) == (origin.scheme, origin.netloc)
        and parsed.path == SETTINGS_PATH
        and not parsed.query
        and not parsed.fragment
    )


def fetch(
    *,
    get: Callable[..., Any] | None = None,
    profile_loader: Callable[[], Any] | None = None,
    timeout_seconds: float = 2.0,
) -> dict[str, Any]:
    """One network read with the item-catalog profile and headers.

    Runs off the Tk thread.  Raises :class:`PackingSettingsReadError` with
    UNAVAILABLE (no profile, untrusted URL, transport, non-200) or INVALID.
    """

    try:
        profile = (profile_loader or _load_profile)()
    except Exception as exc:  # noqa: BLE001 - classify without detail
        raise PackingSettingsReadError(UNAVAILABLE, "profile unavailable") from exc
    if profile is None or not all(
        str(getattr(profile, name, "") or "").strip()
        for name in ("bearer_token", "source_host_id", "device_id", "base_url")
    ):
        raise PackingSettingsReadError(UNAVAILABLE, "profile unavailable")
    url = _profile_settings_url(profile)
    if not _trusted_settings_url(url, profile):
        raise PackingSettingsReadError(UNAVAILABLE, "settings URL is not trusted")
    kwargs: dict[str, Any] = {
        "timeout": timeout_seconds,
        "allow_redirects": False,
        "headers": {
            "User-Agent": "KMTech-LabelMatch/1.0",
            "Authorization": f"Bearer {profile.bearer_token}",
            "X-Logistics-Source-Host-Id": profile.source_host_id,
            "X-Logistics-Device-Id": profile.device_id,
            "X-Logistics-Program": _PROGRAM,
        },
    }
    try:
        ca_path = str(getattr(profile, "tls_ca_bundle_path", "") or "").strip()
        if ca_path:
            from label_data_generation import check_user_data_open, resolve_data_path

            ca_path = str(resolve_data_path(ca_path))
            app_root = (
                Path(sys.executable).resolve().parent
                if getattr(sys, "frozen", False)
                else Path(__file__).resolve().parent
            )
            check_user_data_open(app_root, (Path(ca_path).parent,))
            kwargs["verify"] = ca_path
        if get is None:
            from item_catalog_sync import _hardened_get as get
        response = get(url, **kwargs)
        status = int(getattr(response, "status_code", 0) or 0)
    except PackingSettingsReadError:
        raise
    except Exception as exc:  # noqa: BLE001 - transport detail stays local
        raise PackingSettingsReadError(UNAVAILABLE, type(exc).__name__) from exc
    if status != 200:
        # 400 UNSUPPORTED_QUERY_PARAMETER, 401/403, 503 PACKING_SETTINGS_UNAVAILABLE
        # and redirects are all "could not read" for the operator.
        raise PackingSettingsReadError(UNAVAILABLE, f"HTTP {status}")
    return parse_payload(getattr(response, "content", b""))


@dataclass(frozen=True)
class PackingSettingsState:
    value: int
    source: str
    version: int
    updated_at: str | None = None
    fetched_at: str | None = None
    alert_code: str | None = None

    @property
    def alert_text(self) -> str:
        if self.alert_code is None:
            return ""
        using = (
            f"마지막 값({self.value}개)"
            if self.source == SOURCE_LAST_SAVED
            else f"기본값({DEFAULT_TOP_SCAN_COUNT}개)"
        )
        if self.alert_code == INVALID:
            return f"회사 서버 설정 값이 올바르지 않아 {using}으로 작업합니다 — 관리자에게 알려 주세요"
        return f"회사 서버 설정을 받지 못해 {using}으로 작업합니다"

    def view(self) -> dict[str, Any]:
        return {
            "value": self.value,
            "source": self.source,
            "version": self.version,
            "updated_at": self.updated_at,
            "fetched_at": self.fetched_at,
        }


def default_state() -> PackingSettingsState:
    return PackingSettingsState(DEFAULT_TOP_SCAN_COUNT, SOURCE_DEFAULT, DEFAULT_VERSION)


def state_from_saved(saved: Mapping[str, Any] | None) -> PackingSettingsState:
    if not saved:
        return default_state()
    return PackingSettingsState(
        saved["top_scan_count"], SOURCE_LAST_SAVED, saved["version"],
        saved.get("updated_at"), saved.get("fetched_at"),
    )


def state_after_read(
    result: Mapping[str, Any] | None,
    error_code: str | None,
    saved: Mapping[str, Any] | None,
) -> PackingSettingsState:
    """The value to use after one read: received, last saved, or 4."""

    if error_code is None and result is not None:
        return PackingSettingsState(
            result["top_scan_count"], SOURCE_SERVER, result["version"],
            result.get("updated_at"), result.get("fetched_at"),
        )
    fallback = state_from_saved(saved)
    return PackingSettingsState(
        fallback.value, fallback.source, fallback.version,
        fallback.updated_at, fallback.fetched_at,
        INVALID if error_code == INVALID else UNAVAILABLE,
    )


def load_saved(path: str | os.PathLike[str]) -> dict[str, Any] | None:
    """The last value received from the server on this PC, or None."""

    try:
        value = json.loads(Path(path).read_text(encoding="utf-8"))
        if not isinstance(value, dict) or value.get("schema") != _STORE_SCHEMA:
            return None
        parsed = parse_payload({
            "contract": CONTRACT,
            "top_scan_count": value.get("top_scan_count"),
            "version": value.get("version"),
            "updated_at": value.get("updated_at"),
        })
        fetched_at = value.get("fetched_at")
        parsed["fetched_at"] = fetched_at if type(fetched_at) is str else None
        return parsed
    except (OSError, ValueError, PackingSettingsReadError):
        return None


@writer_sink("label_packing_settings_save")
def save_received(path: str | os.PathLike[str], result: Mapping[str, Any]) -> dict[str, Any]:
    """Keep the received value as this PC's last value (atomic replace)."""

    record = {
        "schema": _STORE_SCHEMA,
        "top_scan_count": result["top_scan_count"],
        "version": result["version"],
        "updated_at": result.get("updated_at"),
        "fetched_at": result.get("fetched_at"),
    }
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    temporary = target.with_name(target.name + ".tmp-" + uuid.uuid4().hex)
    try:
        with temporary.open("w", encoding="utf-8", newline="\n") as handle:
            json.dump(record, handle, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, target)
    finally:
        if temporary.exists():
            temporary.unlink()
    return dict(record)


def utc_now_text() -> str:
    return datetime.now(timezone.utc).isoformat()


def set_count(record: Mapping[str, Any] | None) -> int:
    """The count a set opened with (absent or damaged: 4, today's flow)."""

    value = (record or {}).get(SET_COUNT_KEY)
    return value if valid_count(value) else DEFAULT_TOP_SCAN_COUNT


def set_version(record: Mapping[str, Any] | None) -> int:
    value = (record or {}).get(SET_VERSION_KEY)
    return value if valid_version(value) else DEFAULT_VERSION


def stamp_set(record: dict[str, Any], state: PackingSettingsState) -> None:
    """Freeze the count/version on a set that is opening.

    The default (4, version 0) is the meaning of an absent key, so it is not
    written and today's set records keep their exact bytes.
    """

    record.pop(SET_COUNT_KEY, None)
    record.pop(SET_VERSION_KEY, None)
    if (state.value, state.version) != (DEFAULT_TOP_SCAN_COUNT, DEFAULT_VERSION):
        record[SET_COUNT_KEY] = state.value
        record[SET_VERSION_KEY] = state.version


def step_names(scan_total: int) -> tuple[str, ...]:
    """Position 1 is the master label, position p is product p-1."""

    return ("현품표", *(f"제품{index}" for index in range(1, max(1, int(scan_total)))))


_ORDINALS = {2: "두", 3: "세", 4: "네", 5: "다섯", 6: "여섯", 7: "일곱"}


def ordinal_text(position: int) -> str:
    return f"{_ORDINALS.get(int(position), str(position))} 번째"


__all__ = [
    "CONTRACT", "DEFAULT_TOP_SCAN_COUNT", "INVALID", "MAX_TOP_SCAN_COUNT",
    "MIN_TOP_SCAN_COUNT", "PackingSettingsReadError", "PackingSettingsState",
    "SETTINGS_PATH", "SET_COUNT_KEY", "SET_VERSION_KEY", "SOURCE_DEFAULT",
    "SOURCE_LAST_SAVED", "SOURCE_SERVER", "STORE_FILENAME", "UNAVAILABLE",
    "VERSION_INVALID", "default_state", "fetch", "load_saved", "ordinal_text",
    "parse_payload", "save_received", "set_count", "set_version",
    "state_after_read", "state_from_saved", "stamp_set", "step_names",
    "utc_now_text",
]
