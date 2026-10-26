"""Data-generation high-water marks; callers own release trust and writer admission.

No business database is opened. Serialize writes with the application's existing
writer/deployment admission, and supply literal, already-created marker parents.
Only a verified signed release declaration may lower the compatibility floor.
"""
from __future__ import annotations

import json
import os
from pathlib import Path
import re
import stat
import time
import uuid
from typing import Iterable

SCHEMA = "kmtech-data-generation-v2"
MAX_BYTES = 4096
ATOMIC_REPLACE_RETRY_SECONDS = 1.0
REFUSAL_MESSAGE = "이 PC의 자료는 더 새 판에서 이미 열려 이 판으로는 열 수 없습니다.\n현장 관리자에게 알리세요."
UNKNOWN_MESSAGE = "이 PC의 자료 판을 확인하지 못해 프로그램을 열 수 없습니다.\n현장 관리자에게 알리세요."
_VERSION = re.compile(r"v?(0|[1-9][0-9]{0,8})\.(0|[1-9][0-9]{0,8})\.(0|[1-9][0-9]{0,8})")
_FIELDS = frozenset({"schema", "app_id", "subject_sid", "version", "data_compatible_min_version", "build_id", "compatibility_verified"})
_CAUSE_IDENTIFIER = re.compile(r"[A-Za-z_<][A-Za-z0-9_<>]*(\.[A-Za-z_<][A-Za-z0-9_<>]*)*")


class DataGenerationError(RuntimeError):
    """A marker cannot be safely observed or recorded."""


class DecisionResult(dict):
    """Public decision fields with an optional sanitized in-memory cause.

    The cause is an attribute, absent from dict/JSON/repr/equality projections.
    copy.copy and pickle may carry the attribute; they are not public projections.
    """

    def __init__(self, *args, private_cause: dict[str, object] | None = None, **kwargs):
        super().__init__(*args, **kwargs)
        self.private_cause = private_cause


def version_key(version: str) -> tuple[int, int, int]:
    match = _VERSION.fullmatch(version) if isinstance(version, str) else None
    if match is None:
        raise DataGenerationError("data generation version is not comparable")
    return tuple(int(part) for part in match.groups())


def _canonical_version(version: str) -> str:
    return "v" + ".".join(str(part) for part in version_key(version))


def _identity(app_id: str, subject_sid: str) -> None:
    if not isinstance(app_id, str) or not re.fullmatch(r"[A-Za-z0-9_.-]{1,64}", app_id):
        raise DataGenerationError("data generation application identity is invalid")
    if not isinstance(subject_sid, str) or len(subject_sid) > 256 or not re.fullmatch(r"S-1-[0-9]+(?:-[0-9]+){1,15}", subject_sid):
        raise DataGenerationError("data generation user identity is invalid")


def _build_identity(build_id: str) -> None:
    if not isinstance(build_id, str) or not re.fullmatch(r"[0-9a-f]{40}|[0-9a-f]{64}", build_id):
        raise DataGenerationError("data generation build identity is invalid")


def _literal_path(path: Path | str) -> Path:
    path = Path(path)
    if not path.is_absolute() or str(path).startswith(("\\\\?\\", "\\\\.\\")):
        raise DataGenerationError("data generation path must be literal and absolute")
    path = Path(os.path.abspath(path))
    for component in (*reversed(path.parents), path):
        try:
            info = component.lstat()
        except FileNotFoundError:
            continue
        if stat.S_ISLNK(info.st_mode) or getattr(info, "st_file_attributes", 0) & 0x400:
            raise DataGenerationError("data generation path contains a reparse point")
        if component != path and not stat.S_ISDIR(info.st_mode):
            raise DataGenerationError("data generation ancestor is not a directory")
    return path


def _decode(raw: bytes, app_id: str, subject_sid: str) -> dict[str, object]:
    def unique(pairs):
        value = {}
        for key, item in pairs:
            if key in value:
                raise DataGenerationError("duplicate data generation field")
            value[key] = item
        return value

    record = json.loads(raw.decode("utf-8"), object_pairs_hook=unique)
    if not isinstance(record, dict) or set(record) != _FIELDS:
        raise DataGenerationError("data generation fields differ")
    if record["schema"] != SCHEMA or record["app_id"] != app_id or record["subject_sid"] != subject_sid:
        raise DataGenerationError("data generation identity differs")
    _build_identity(record["build_id"])
    if type(record["compatibility_verified"]) is not bool:
        raise DataGenerationError("data generation compatibility trust differs")
    if version_key(record["data_compatible_min_version"]) > version_key(record["version"]):
        raise DataGenerationError("data generation compatibility floor is invalid")
    if not record["compatibility_verified"] and version_key(record["data_compatible_min_version"]) != version_key(record["version"]):
        raise DataGenerationError("unverified data generation compatibility floor")
    return record


def _read(path: Path | str, app_id: str, subject_sid: str) -> dict[str, object] | None:
    path = _literal_path(path)
    try:
        info = path.lstat()
    except FileNotFoundError:
        return None
    if not stat.S_ISREG(info.st_mode) or not 0 < info.st_size <= MAX_BYTES:
        raise DataGenerationError("data generation marker size or type is invalid")
    with path.open("rb") as handle:
        raw = handle.read(MAX_BYTES + 1)
    if len(raw) > MAX_BYTES:
        raise DataGenerationError("data generation marker is oversized")
    _literal_path(path)
    return _decode(raw, app_id, subject_sid)


def _cause_identifier(value: object) -> str:
    if type(value) is str and len(value) <= 200 and _CAUSE_IDENTIFIER.fullmatch(value):
        return value
    return "<invalid>"


def probe_downgrade(marker_paths: Iterable[Path | str], target_version: str, *, app_id: str, subject_sid: str, target_build_id: str) -> DecisionResult:
    """ALLOW/REFUSE/UNKNOWN; UNKNOWN denies, even if the other slot is readable."""
    try:
        _identity(app_id, subject_sid)
        _build_identity(target_build_id)
        target = version_key(target_version)
        paths = list(marker_paths)
        if not paths:
            raise DataGenerationError("data generation marker locations are absent")
        records = [record for path in paths if (record := _read(path, app_id, subject_sid)) is not None]
        if not records:
            return DecisionResult({"status": "ALLOW", "allowed": True, "message_key": "no_generation", "message": "", "version": None, "minimum_version": None})
        latest = max(version_key(record["version"]) for record in records)
        newest = [record for record in records if version_key(record["version"]) == latest]
        if target == latest and any(record["build_id"] != target_build_id and not record["compatibility_verified"] for record in newest):
            raise DataGenerationError("same version has a different unverified build")
        floor = max(version_key(record["data_compatible_min_version"]) for record in newest)
        allowed = target >= floor
        return DecisionResult({"status": "ALLOW" if allowed else "REFUSE", "allowed": allowed,
                "message_key": "compatible_generation" if allowed else "data_generation_downgrade_refused",
                "message": "" if allowed else REFUSAL_MESSAGE,
                "version": "v" + ".".join(map(str, latest)), "minimum_version": "v" + ".".join(map(str, floor))})
    except (OSError, ValueError, TypeError, DataGenerationError) as error:
        try:
            raised_at = seen_in = None
            traceback = error.__traceback__
            while traceback is not None:
                line = traceback.tb_lineno
                location = {"module": _cause_identifier(traceback.tb_frame.f_globals["__name__"]),
                            "function": _cause_identifier(traceback.tb_frame.f_code.co_name),
                            "line": line if type(line) is int and 1 <= line <= 10_000_000 else None}
                raised_at = location
                if location["module"] == __name__:
                    seen_in = location
                traceback = traceback.tb_next
            cause_type = type(error)
            private_cause = {"type": _cause_identifier(cause_type.__qualname__),
                             "type_module": _cause_identifier(cause_type.__module__),
                             "raised_at": raised_at, "seen_in": seen_in}
        except Exception:
            private_cause = None
        return DecisionResult({"status": "UNKNOWN", "allowed": False, "message_key": "data_generation_unknown", "message": UNKNOWN_MESSAGE, "version": None, "minimum_version": None},
                              private_cause=private_cause)


def record_generation(marker_path: Path | str, version: str, *, app_id: str, subject_sid: str, build_id: str,
                      data_compatible_min_version: str | None = None, compatibility_verified: bool = False) -> dict[str, object]:
    """Atomically record a monotonic generation, before opening user data.

compatibility_verified=True asserts an EXPLICIT compatibility floor in a signed
manifest binding version/build/floor. Missing floors are not compatibility grants.
Probe all slots before opening data; recording alone does not authorize admission.
"""
    _identity(app_id, subject_sid)
    _build_identity(build_id)
    if type(compatibility_verified) is not bool:
        raise DataGenerationError("data generation compatibility trust differs")
    compatibility_verified = compatibility_verified and data_compatible_min_version is not None
    version = _canonical_version(version)
    floor = _canonical_version(data_compatible_min_version) if compatibility_verified and data_compatible_min_version is not None else version
    if version_key(floor) > version_key(version):
        raise DataGenerationError("data generation compatibility floor exceeds release")
    path = _literal_path(marker_path)
    if not path.parent.is_dir():
        raise DataGenerationError("data generation parent must already exist")
    previous = _read(path, app_id, subject_sid)
    if previous is not None:
        previous_version = version_key(previous["version"])
        if previous_version > version_key(version) or (previous_version == version_key(version) and previous["build_id"] == build_id):
            return previous
        if previous_version == version_key(version):
            # An admitted different build must not inherit its predecessor's
            # compatibility grant. Keep the stricter floor at this maximum.
            compatibility_verified = compatibility_verified and previous["compatibility_verified"]
            floor = max((floor, previous["data_compatible_min_version"]), key=version_key) if compatibility_verified else version
    record = {"schema": SCHEMA, "app_id": app_id, "subject_sid": subject_sid,
              "version": version, "data_compatible_min_version": floor,
              "build_id": build_id, "compatibility_verified": compatibility_verified}
    raw = (json.dumps(record, ensure_ascii=True, sort_keys=True, separators=(",", ":")) + "\n").encode("utf-8")
    temporary = path.with_name(path.name + ".tmp." + uuid.uuid4().hex)
    try:
        _literal_path(temporary)
        with temporary.open("xb") as handle:
            handle.write(raw)
            handle.flush()
            os.fsync(handle.fileno())
        _literal_path(path)
        if os.name != "nt":
            os.replace(temporary, path)
        else:
            # Windows lock errors 5/32 (scanner, indexer) are retried for one second.
            deadline = time.monotonic() + ATOMIC_REPLACE_RETRY_SECONDS
            while True:
                try:
                    os.replace(temporary, path)
                    break
                except OSError as error:
                    if getattr(error, "winerror", None) not in {5, 32}:
                        raise
                    remaining = deadline - time.monotonic()
                    if remaining <= 0:
                        raise
                    time.sleep(min(0.05, remaining))
                    if time.monotonic() >= deadline:
                        raise
        if _read(path, app_id, subject_sid) != record:
            raise DataGenerationError("data generation durable readback differs")
    finally:
        if temporary.exists():
            temporary.unlink()
    return record
