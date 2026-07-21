"""Secure machine-scoped runtime profile for the authoritative logistics API."""

from __future__ import annotations

import ctypes
import ipaddress
import json
import math
import os
import stat
from ctypes import wintypes
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable, Mapping
from urllib.parse import urlsplit, urlunsplit


PROFILE_CONTRACT_VERSION = "km-logistics-runtime-profile-v1"
PROFILE_PATH_ENV = "KM_LOGISTICS_PROFILE_PATH"
REQUIRED_ENV = "KM_LOGISTICS_REQUIRED"
DPAPI_REFERENCE_PREFIX = "dpapi:"
DEFAULT_TOKEN_REF = "dpapi:secrets/bearer-token.dpapi"
DPAPI_ENTROPY = b"KMTech Logistics Runtime Profile v1"
DEFAULT_PROFILE_RELATIVE_PATH = Path("KMTech") / "Logistics" / "runtime-profile.json"
SUPPORTED_LEDGER_PLANES = frozenset({"AUTHORITATIVE", "SHADOW_CANDIDATE"})
MAX_PROFILE_BYTES = 64 * 1024
MAX_SECRET_BYTES = 64 * 1024
_MACHINE_ENVIRONMENT_KEY = (
    r"SYSTEM\CurrentControlSet\Control\Session Manager\Environment"
)


class LogisticsRuntimeConfigurationError(RuntimeError):
    pass


@dataclass(frozen=True)
class LogisticsRuntimeProfile:
    base_url: str
    authority_scope: str
    authority_epoch: int
    authority_plane: str
    ledger_plane: str
    plane_epoch: int
    device_id: str
    source_host_id: str
    bearer_token: str = field(repr=False)
    timeout_seconds: float = 10.0
    profile_path: str = ""
    required: bool = False

    def redacted_summary(self) -> dict[str, Any]:
        return {
            "contract_version": PROFILE_CONTRACT_VERSION,
            "base_url": self.base_url,
            "authority_scope": self.authority_scope,
            "authority_epoch": self.authority_epoch,
            "authority_plane": self.authority_plane,
            "ledger_plane": self.ledger_plane,
            "plane_epoch": self.plane_epoch,
            "device_id": self.device_id,
            "source_host_id": self.source_host_id,
            "timeout_seconds": self.timeout_seconds,
            "profile_path": self.profile_path,
            "bearer_token_present": bool(self.bearer_token),
            "required": self.required,
        }


class _DataBlob(ctypes.Structure):
    _fields_ = (("cbData", wintypes.DWORD), ("pbData", ctypes.POINTER(ctypes.c_byte)))


def _blob(value: bytes) -> tuple[_DataBlob, Any]:
    buffer = ctypes.create_string_buffer(value)
    return _DataBlob(len(value), ctypes.cast(buffer, ctypes.POINTER(ctypes.c_byte))), buffer


def unprotect_machine_secret(value: bytes) -> str:
    if os.name != "nt":
        raise LogisticsRuntimeConfigurationError(
            "DPAPI machine profile can only be decrypted on Windows"
        )
    encrypted, encrypted_buffer = _blob(bytes(value))
    entropy, entropy_buffer = _blob(DPAPI_ENTROPY)
    decrypted = _DataBlob()
    crypt32 = ctypes.windll.crypt32
    kernel32 = ctypes.windll.kernel32
    if not crypt32.CryptUnprotectData(
        ctypes.byref(encrypted), None, ctypes.byref(entropy), None, None, 0x1, ctypes.byref(decrypted)
    ):
        del encrypted_buffer, entropy_buffer
        raise LogisticsRuntimeConfigurationError("DPAPI bearer token could not be decrypted")
    try:
        raw = ctypes.string_at(decrypted.pbData, decrypted.cbData)
    finally:
        kernel32.LocalFree(decrypted.pbData)
    try:
        return raw.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise LogisticsRuntimeConfigurationError("DPAPI bearer token is not valid UTF-8") from exc


def protect_machine_secret(value: str) -> bytes:
    if os.name != "nt":
        raise LogisticsRuntimeConfigurationError(
            "DPAPI machine profile can only be installed on Windows"
        )
    token = str(value or "").strip()
    if (
        not token
        or len(token) > 16_384
        or any(character.isspace() for character in token)
    ):
        raise LogisticsRuntimeConfigurationError("bearer token is empty or invalid")
    raw = token.encode("utf-8")
    clear, clear_buffer = _blob(raw)
    entropy, entropy_buffer = _blob(DPAPI_ENTROPY)
    encrypted = _DataBlob()
    crypt32 = ctypes.windll.crypt32
    kernel32 = ctypes.windll.kernel32
    if not crypt32.CryptProtectData(
        ctypes.byref(clear),
        None,
        ctypes.byref(entropy),
        None,
        None,
        0x1 | 0x4,
        ctypes.byref(encrypted),
    ):
        del clear_buffer, entropy_buffer
        raise LogisticsRuntimeConfigurationError("DPAPI bearer token could not be protected")
    try:
        return ctypes.string_at(encrypted.pbData, encrypted.cbData)
    finally:
        kernel32.LocalFree(encrypted.pbData)


def _parse_bool(value: Any, *, field_name: str) -> bool:
    normalized = str(value or "").strip().lower()
    if normalized in {"", "0", "false", "no", "off"}:
        return False
    if normalized in {"1", "true", "yes", "on"}:
        return True
    raise LogisticsRuntimeConfigurationError(f"{field_name} must be an explicit boolean")


def logistics_runtime_required(environ: Mapping[str, str] | None = None) -> bool:
    values = _runtime_environment(environ)
    return _parse_bool(values.get(REQUIRED_ENV), field_name=REQUIRED_ENV)


def _machine_environment_value(name: str) -> str:
    if os.name != "nt":
        return ""
    try:
        import winreg

        with winreg.OpenKey(winreg.HKEY_LOCAL_MACHINE, _MACHINE_ENVIRONMENT_KEY) as key:
            try:
                value, _kind = winreg.QueryValueEx(key, name)
            except FileNotFoundError:
                return ""
    except FileNotFoundError:
        return ""
    except OSError as exc:
        raise LogisticsRuntimeConfigurationError(
            "Windows Machine logistics environment could not be read"
        ) from exc
    return str(value or "").strip()


def _runtime_environment(environ: Mapping[str, str] | None) -> Mapping[str, str]:
    if environ is not None or os.name != "nt":
        return os.environ if environ is None else environ
    machine_path = _machine_environment_value(PROFILE_PATH_ENV)
    machine_required = _machine_environment_value(REQUIRED_ENV)
    if machine_path or machine_required:
        return {
            PROFILE_PATH_ENV: machine_path,
            REQUIRED_ENV: machine_required,
            "PROGRAMDATA": r"C:\ProgramData",
        }
    return os.environ


def default_logistics_profile_path(environ: Mapping[str, str] | None = None) -> Path:
    values = _runtime_environment(environ)
    program_data = str(values.get("PROGRAMDATA") or r"C:\ProgramData").strip()
    return Path(program_data) / DEFAULT_PROFILE_RELATIVE_PATH


default_profile_path = default_logistics_profile_path


def _safe_text(value: Any, field_name: str) -> str:
    text = str(value or "").strip()
    if (
        not text
        or len(text) > 200
        or any(ord(character) < 32 for character in text)
    ):
        raise LogisticsRuntimeConfigurationError(f"{field_name} is required")
    return text


def _positive_int(value: Any, field_name: str) -> int:
    if isinstance(value, bool):
        raise LogisticsRuntimeConfigurationError(f"{field_name} must be a positive integer")
    try:
        parsed = int(value)
    except (TypeError, ValueError) as exc:
        raise LogisticsRuntimeConfigurationError(f"{field_name} must be a positive integer") from exc
    if parsed < 1:
        raise LogisticsRuntimeConfigurationError(f"{field_name} must be a positive integer")
    return parsed


def _selected_ledger_plane(
    values: Mapping[str, Any], authority_plane: str
) -> str:
    if "ledger_plane" not in values:
        return authority_plane
    ledger_plane = _safe_text(values.get("ledger_plane"), "ledger_plane").upper()
    if ledger_plane not in SUPPORTED_LEDGER_PLANES:
        raise LogisticsRuntimeConfigurationError(
            "machine logistics ledger_plane must be AUTHORITATIVE or SHADOW_CANDIDATE"
        )
    return ledger_plane


def _https_base_url(value: Any) -> str:
    url = _safe_text(value, "base_url").rstrip("/")
    if any(character.isspace() for character in url) or "\\" in url:
        raise LogisticsRuntimeConfigurationError("base_url contains unsafe characters")
    try:
        parsed = urlsplit(url)
        port = parsed.port
    except ValueError as exc:
        raise LogisticsRuntimeConfigurationError("base_url is not a valid URL") from exc
    if (
        parsed.scheme.lower() != "https"
        or not parsed.hostname
        or parsed.username
        or parsed.password
        or parsed.path not in {"", "/"}
        or parsed.query
        or parsed.fragment
        or (port is not None and not 1 <= port <= 65535)
    ):
        raise LogisticsRuntimeConfigurationError(
            "base_url must be credential-free HTTPS without query or fragment"
        )
    hostname = parsed.hostname.rstrip(".").lower()
    is_loopback = hostname == "localhost"
    try:
        is_loopback = is_loopback or ipaddress.ip_address(hostname).is_loopback
    except ValueError:
        pass
    if is_loopback:
        raise LogisticsRuntimeConfigurationError(
            "machine logistics base_url must not use a loopback origin"
        )
    return urlunsplit(("https", parsed.netloc, "", "", ""))


def assert_path_has_no_reparse_components(
    value: str | os.PathLike[str], *, label: str
) -> Path:
    path = Path(os.path.abspath(os.path.expanduser(os.fspath(value))))
    current = Path(path.anchor)
    for part in path.parts[1:]:
        current /= part
        try:
            metadata = os.lstat(current)
        except FileNotFoundError:
            continue
        except OSError as exc:
            raise LogisticsRuntimeConfigurationError(
                f"{label} path could not be inspected"
            ) from exc
        attributes = int(getattr(metadata, "st_file_attributes", 0) or 0)
        if stat.S_ISLNK(metadata.st_mode) or attributes & 0x400:
            raise LogisticsRuntimeConfigurationError(
                f"{label} path must not contain a symlink or junction"
            )
    return path


def _read_profile(path: Path) -> dict[str, Any]:
    def reject_duplicates(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
        result: dict[str, Any] = {}
        for key, value in pairs:
            if key in result:
                raise LogisticsRuntimeConfigurationError(
                    f"machine logistics profile has duplicate field: {key}"
                )
            result[key] = value
        return result

    try:
        raw = path.read_bytes()
    except OSError as exc:
        raise LogisticsRuntimeConfigurationError("machine logistics profile could not be read") from exc
    if not raw or len(raw) > MAX_PROFILE_BYTES:
        raise LogisticsRuntimeConfigurationError("machine logistics profile size is invalid")
    try:
        value = json.loads(
            raw.decode("utf-8-sig"), object_pairs_hook=reject_duplicates
        )
    except LogisticsRuntimeConfigurationError:
        raise
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise LogisticsRuntimeConfigurationError(
            "machine logistics profile is not valid UTF-8 JSON"
        ) from exc
    if not isinstance(value, dict):
        raise LogisticsRuntimeConfigurationError("machine logistics profile must be an object")
    return value


def _resolve_secret_path(profile_path: Path, reference: Any) -> Path:
    value = _safe_text(reference, "bearer_token_ref")
    if not value.lower().startswith(DPAPI_REFERENCE_PREFIX):
        raise LogisticsRuntimeConfigurationError("bearer_token_ref must use dpapi:")
    relative = value[len(DPAPI_REFERENCE_PREFIX) :].strip().replace("/", os.sep)
    candidate = Path(relative)
    if not relative or candidate.is_absolute() or ".." in candidate.parts:
        raise LogisticsRuntimeConfigurationError("bearer_token_ref must stay inside the profile directory")
    root = assert_path_has_no_reparse_components(
        profile_path.parent, label="runtime profile directory"
    )
    unresolved = assert_path_has_no_reparse_components(
        root / candidate, label="DPAPI secret"
    )
    resolved = unresolved.resolve()
    try:
        resolved.relative_to(root)
    except ValueError as exc:
        raise LogisticsRuntimeConfigurationError("bearer_token_ref escapes the profile directory") from exc
    return resolved


def load_logistics_runtime_profile(
    required: bool | None = None,
    profile_path: str | os.PathLike[str] | None = None,
    *,
    environ: Mapping[str, str] | None = None,
    decryptor: Callable[[bytes], str] | None = None,
) -> LogisticsRuntimeProfile | None:
    values = _runtime_environment(environ)
    required_value = logistics_runtime_required(values) if required is None else bool(required)
    explicit_path = profile_path or str(values.get(PROFILE_PATH_ENV) or "").strip()
    path = assert_path_has_no_reparse_components(
        Path(explicit_path) if explicit_path else default_logistics_profile_path(values),
        label="runtime profile",
    )
    if not path.exists():
        if required_value or explicit_path:
            raise LogisticsRuntimeConfigurationError("machine logistics profile is missing")
        return None
    profile = _read_profile(path)
    if "bearer_token" in profile:
        raise LogisticsRuntimeConfigurationError(
            "plaintext bearer_token is forbidden in the machine profile"
        )
    if profile.get("contract_version") != PROFILE_CONTRACT_VERSION:
        raise LogisticsRuntimeConfigurationError("machine logistics profile contract_version is invalid")
    secret_path = _resolve_secret_path(path, profile.get("bearer_token_ref"))
    try:
        encrypted = secret_path.read_bytes()
    except OSError as exc:
        raise LogisticsRuntimeConfigurationError("DPAPI bearer token file could not be read") from exc
    if not encrypted or len(encrypted) > MAX_SECRET_BYTES:
        raise LogisticsRuntimeConfigurationError("DPAPI bearer token file size is invalid")
    token = str((decryptor or unprotect_machine_secret)(encrypted) or "").strip()
    if (
        not token
        or len(token) > 16_384
        or any(character.isspace() for character in token)
    ):
        raise LogisticsRuntimeConfigurationError("DPAPI bearer token is empty or invalid")
    authority_plane = _safe_text(
        profile.get("authority_plane"), "authority_plane"
    ).upper()
    if authority_plane != "AUTHORITATIVE":
        raise LogisticsRuntimeConfigurationError(
            "machine logistics authority_plane must be AUTHORITATIVE"
        )
    ledger_plane = _selected_ledger_plane(profile, authority_plane)
    try:
        timeout = float(profile.get("timeout_seconds", 10.0))
    except (TypeError, ValueError) as exc:
        raise LogisticsRuntimeConfigurationError("timeout_seconds is invalid") from exc
    if not math.isfinite(timeout) or not 0.1 <= timeout <= 60.0:
        raise LogisticsRuntimeConfigurationError("timeout_seconds must be between 0.1 and 60")
    return LogisticsRuntimeProfile(
        base_url=_https_base_url(profile.get("base_url")),
        authority_scope=_safe_text(profile.get("authority_scope"), "authority_scope"),
        authority_epoch=_positive_int(profile.get("authority_epoch"), "authority_epoch"),
        authority_plane=authority_plane,
        ledger_plane=ledger_plane,
        plane_epoch=_positive_int(profile.get("plane_epoch"), "plane_epoch"),
        device_id=_safe_text(profile.get("device_id"), "device_id"),
        source_host_id=_safe_text(profile.get("source_host_id"), "source_host_id"),
        bearer_token=token,
        timeout_seconds=timeout,
        profile_path=str(path.resolve()),
        required=required_value,
    )


def profile_from_values(
    values: Mapping[str, Any],
    *,
    profile_path: str | os.PathLike[str],
    bearer_token: str,
    required: bool = True,
) -> LogisticsRuntimeProfile:
    if values.get("contract_version") != PROFILE_CONTRACT_VERSION:
        raise LogisticsRuntimeConfigurationError("machine logistics profile contract_version is invalid")
    if values.get("bearer_token_ref") != DEFAULT_TOKEN_REF:
        raise LogisticsRuntimeConfigurationError("bearer_token_ref must use the standard DPAPI path")
    token = str(bearer_token or "").strip()
    if (
        not token
        or len(token) > 16_384
        or any(character.isspace() for character in token)
    ):
        raise LogisticsRuntimeConfigurationError("bearer token is empty or invalid")
    authority_plane = _safe_text(
        values.get("authority_plane"), "authority_plane"
    ).upper()
    if authority_plane != "AUTHORITATIVE":
        raise LogisticsRuntimeConfigurationError("machine logistics authority_plane must be AUTHORITATIVE")
    ledger_plane = _selected_ledger_plane(values, authority_plane)
    try:
        timeout = float(values.get("timeout_seconds", 10.0))
    except (TypeError, ValueError) as exc:
        raise LogisticsRuntimeConfigurationError("timeout_seconds is invalid") from exc
    if not math.isfinite(timeout) or not 0.1 <= timeout <= 60.0:
        raise LogisticsRuntimeConfigurationError("timeout_seconds must be between 0.1 and 60")
    return LogisticsRuntimeProfile(
        base_url=_https_base_url(values.get("base_url")),
        authority_scope=_safe_text(values.get("authority_scope"), "authority_scope"),
        authority_epoch=_positive_int(values.get("authority_epoch"), "authority_epoch"),
        authority_plane=authority_plane,
        ledger_plane=ledger_plane,
        plane_epoch=_positive_int(values.get("plane_epoch"), "plane_epoch"),
        device_id=_safe_text(values.get("device_id"), "device_id"),
        source_host_id=_safe_text(values.get("source_host_id"), "source_host_id"),
        bearer_token=token,
        timeout_seconds=timeout,
        profile_path=str(
            assert_path_has_no_reparse_components(
                profile_path, label="runtime profile"
            ).resolve()
        ),
        required=bool(required),
    )


protect_bearer_token = protect_machine_secret


def assert_logistics_runtime_ready(
    required: bool | None = None,
    profile_path: str | os.PathLike[str] | None = None,
    **kwargs: Any,
) -> LogisticsRuntimeProfile | None:
    return load_logistics_runtime_profile(required, profile_path, **kwargs)


__all__ = [
    "DPAPI_ENTROPY",
    "DEFAULT_TOKEN_REF",
    "LogisticsRuntimeConfigurationError",
    "LogisticsRuntimeProfile",
    "PROFILE_CONTRACT_VERSION",
    "PROFILE_PATH_ENV",
    "REQUIRED_ENV",
    "assert_logistics_runtime_ready",
    "assert_path_has_no_reparse_components",
    "default_logistics_profile_path",
    "default_profile_path",
    "load_logistics_runtime_profile",
    "logistics_runtime_required",
    "profile_from_values",
    "protect_bearer_token",
    "protect_machine_secret",
    "unprotect_machine_secret",
]
