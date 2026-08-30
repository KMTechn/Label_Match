"""First-run current-user state onboarding for Label_Match."""

from __future__ import annotations

import argparse
from dataclasses import dataclass
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import stat
import sys
from typing import Any, Callable, Mapping, MutableMapping
import uuid

from current_user_scheduled_task import (
    LEGACY_TASK_QUIESCENCE_VERSION,
    LEGACY_TASK_REQUIRED_STATE,
    install_current_user_scheduled_task,
    read_legacy_system_task_quiescence,
    remove_current_user_scheduled_task,
)
from direct_sync_push import manifest_hash
from enrollment_mutex import EnrollmentMutex, EnrollmentMutexError
from direct_sync_runtime import load_credentials_from_json
from label_exact_clone_resolution import read_pinned_json, validate_resolution_receipt
from logistics_runtime_profile import (
    PROFILE_PATH_ENV,
    REQUIRED_ENV,
    load_logistics_runtime_profile,
    unprotect_current_user_secret,
)
from user_relay import (
    install_user_relay_autostart,
    release_user_relay_stop_marker,
    remove_user_relay_autostart,
    request_user_relay_stop,
    start_user_relay_process,
    user_relay_stop_path,
)
from writer_session_fence import writer_sink

DEFAULT_SERVER_BASE_URL = "https://worker.kmtecherp.com"
ONBOARDING_REPORT_VERSION = "label-match-current-user-onboarding-v1"
REMOVAL_REPORT_VERSION = "label-match-current-user-removal-v1"
BOOTSTRAP_INTEGRITY_VERSION = "label-match-bootstrap-integrity-v2"
BOOTSTRAP_INVENTORY_ALGORITHM = "sha256-file-hash-size-utf8-path-v1"
BOOTSTRAP_ROOT_HASH_DOMAIN = b"label-match-code-root-v1\n"
ONBOARDING_JSON_MAX_BYTES = 1024 * 1024
BOOTSTRAP_INTEGRITY_MAX_BYTES = 8 * 1024 * 1024
ENROLLMENT_TLS_CA_BUNDLE_PATH_ENV = "LABEL_MATCH_ENROLLMENT_TLS_CA_BUNDLE_PATH"
ONBOARDING_EXIT_CODE = 4
LABEL_MATCH_DATA_ROOT_ENV = "LABEL_MATCH_SAVE_DIR"
LABEL_MATCH_SETTINGS_PATH_ENV = "LABEL_MATCH_SETTINGS_PATH"
LABEL_MATCH_DIRECT_SYNC_ROOT_ENV = "LABEL_MATCH_DIRECT_SYNC_ROOT"
LEGACY_DIRECT_SYNC_ROOT_ENV = "LABEL_MATCH_DIRECT_SYNC_PROGRAM_DATA_ROOT"
CANONICAL_PORTABLE_ROOT = Path(r"C:\KMTech\Apps\Label_Match\current")
PORTABLE_BOOTSTRAP_INTEGRITY_VERSION = "label-match-bootstrap-integrity-v1"
CONFLICT_RECEIPT_PATH_ENV = "KMTECH_LABEL_CONFLICT_RESOLUTION_RECEIPT_PATH"
CONFLICT_RECEIPT_SHA256_ENV = "KMTECH_LABEL_CONFLICT_RESOLUTION_RECEIPT_SHA256"


class CurrentUserOnboardingError(RuntimeError):
    def __init__(
        self,
        message: str,
        *,
        report_path: Path,
        status: str = "FAILED",
    ) -> None:
        super().__init__(message)
        self.report_path = Path(report_path)
        self.status = status


def _portable_stop_marker_release_preflight(
    paths: CurrentUserOnboardingPaths,
    *,
    environ: Mapping[str, str] | None = None,
) -> dict[str, Any]:
    marker = user_relay_stop_path(paths.direct_sync_root)
    if not marker.exists():
        return {"status": "NOT_REQUIRED", "marker_present": False}
    if os.path.normcase(str(paths.app_root)) != os.path.normcase(
        str(CANONICAL_PORTABLE_ROOT)
    ):
        raise ValueError(
            "relay stop marker is a safety fence until the canonical portable root is installed"
        )
    manifest_path = paths.app_root / "portable-manifest.json"
    installer_path = paths.app_root / "INSTALL_CANONICAL_PORTABLE.ps1"
    integrity_path = paths.app_root / "bootstrap-integrity.json"
    if not all(
        path.is_file() for path in (manifest_path, installer_path, integrity_path)
    ) or manifest_path.stat().st_size > 64 * 1024:
        raise ValueError(
            "relay stop marker cannot be removed before canonical install readback"
        )
    manifest = _read_json(manifest_path, "canonical portable manifest")
    source_commit = str(manifest.get("source_commit") or "").lower()
    source_tree = str(manifest.get("source_tree") or "").lower()
    runtime = paths.app_root / "runtime" / "pythonw.exe"
    expected_runtime_hash = str(manifest.get("runtime_pythonw_sha256") or "").lower()
    if (
        manifest.get("schema") != "label-match-portable-tree-v1"
        or manifest.get("entrypoint") != "runtime/pythonw.exe app/main.py"
        or len(source_commit) != 40
        or any(character not in "0123456789abcdef" for character in source_commit)
        or len(source_tree) != 40
        or any(character not in "0123456789abcdef" for character in source_tree)
        or manifest.get("allowed_unsigned_app_pe") != []
        or manifest.get("forbidden_package_roots") != []
        or manifest.get("canonical_installer") != "INSTALL_CANONICAL_PORTABLE.ps1"
        or _file_sha256(installer_path)
        != str(manifest.get("canonical_installer_sha256") or "").lower()
        or not runtime.is_file()
        or _file_sha256(runtime) != expected_runtime_hash
    ):
        raise ValueError(
            "relay stop marker cannot be removed because canonical install identity differs"
        )
    integrity = _read_json(integrity_path, "canonical bootstrap integrity record")
    rows: list[dict[str, Any]] = []
    for file_path in sorted(
        (path for path in paths.app_root.rglob("*") if path.is_file()),
        key=lambda path: path.relative_to(paths.app_root).as_posix().casefold(),
    ):
        relative = file_path.relative_to(paths.app_root).as_posix()
        if relative.casefold() == integrity_path.name.casefold():
            continue
        file_stat = file_path.lstat()
        if file_path.is_symlink() or (
            getattr(file_stat, "st_file_attributes", 0)
            & getattr(stat, "FILE_ATTRIBUTE_REPARSE_POINT", 0x400)
        ):
            raise ValueError("canonical portable install contains a reparse point")
        rows.append(
            {
                "path": relative,
                "size": file_stat.st_size,
                "sha256": _file_sha256(file_path),
            }
        )
    aggregate_payload = "".join(
        f"{row['sha256']} {row['size']} {row['path']}\n" for row in rows
    ).encode("utf-8")
    aggregate = hashlib.sha256(aggregate_payload).hexdigest()
    if (
        integrity.get("schema_version") != PORTABLE_BOOTSTRAP_INTEGRITY_VERSION
        or integrity.get("status") != "PASS"
        or str(integrity.get("code_root") or "") not in {".", str(paths.app_root)}
        or integrity.get("file_count") != len(rows)
        or integrity.get("files") != rows
        or str(integrity.get("aggregate_sha256") or "").lower() != aggregate
        or integrity.get("identity_profile_created") is not False
        or integrity.get("state_scope") != "current_user_first_run"
    ):
        raise ValueError("canonical bootstrap integrity readback differs")

    values = os.environ if environ is None else environ
    receipt_value = str(values.get(CONFLICT_RECEIPT_PATH_ENV) or "").strip()
    expected_receipt_hash = str(
        values.get(CONFLICT_RECEIPT_SHA256_ENV) or ""
    ).strip().lower()
    if not receipt_value or len(expected_receipt_hash) != 64 or any(
        character not in "0123456789abcdef" for character in expected_receipt_hash
    ):
        raise ValueError(
            "relay stop marker requires a pinned EXACT_CLONE_RUNTIME_CONFLICT receipt"
        )
    receipt_path = _resolved(receipt_value)
    try:
        receipt = read_pinned_json(
            receipt_path,
            expected_receipt_hash,
            label="EXACT_CLONE_RUNTIME_CONFLICT receipt",
            maximum_bytes=ONBOARDING_JSON_MAX_BYTES,
        )
    except ValueError:
        raise ValueError("EXACT_CLONE_RUNTIME_CONFLICT receipt pin differs")

    receipt_readback = validate_resolution_receipt(
        receipt,
        client_db_path=paths.direct_sync_root
        / "queue"
        / "direct_sync_relay.sqlite3",
        identity_path=paths.identity_path,
        credential_path=paths.credential_path,
        stop_marker_path=marker,
        portable_root=paths.app_root,
        allow_portable_relocation=True,
    )
    return {
        "status": "CANONICAL_INSTALL_PROVEN",
        "marker_present": True,
        "manifest_path": str(manifest_path),
        "bootstrap_integrity_path": str(integrity_path),
        "bootstrap_integrity_sha256": _file_sha256(integrity_path),
        "conflict_resolution_receipt_path": str(receipt_path),
        "conflict_resolution_receipt_sha256": expected_receipt_hash,
        "conflict_resolution_receipt_readback": receipt_readback,
        "current_stop_marker_request_id": receipt_readback[
            "stop_marker_lineage"
        ]["current_request_id"],
        "current_stop_marker_sha256": receipt_readback["stop_marker_lineage"][
            "current_sha256"
        ],
        "source_commit": source_commit,
        "source_tree": source_tree,
        "conflict_resolution_authority": (
            "pinned path plus exact SHA-256 plus local authority readback"
        ),
    }


@dataclass(frozen=True)
class CurrentUserOnboardingPaths:
    app_root: Path
    data_root: Path
    settings_path: Path
    direct_sync_root: Path
    queue_dir: Path
    spool_dir: Path
    upload_status_dir: Path
    status_dir: Path
    logs_dir: Path
    control_dir: Path
    identity_path: Path
    producer_manifest_path: Path
    credential_path: Path
    registration_receipt_path: Path
    registration_report_path: Path
    onboarding_report_path: Path
    removal_report_path: Path
    logistics_profile_path: Path
    logistics_secret_path: Path
    bootstrap_tls_ca_bundle_path: Path
    ledger_path: Path
    bootstrap_integrity_path: Path


def _now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def _resolved(path: str | os.PathLike[str]) -> Path:
    return Path(path).expanduser().resolve(strict=False)


def _is_legacy_sync_path(path: Path) -> bool:
    candidate = os.path.normcase(str(_resolved(path)))
    legacy = os.path.normcase(str(_resolved(r"C:\Sync")))
    return candidate == legacy or candidate.startswith(legacy + os.sep)


def resolve_current_user_onboarding_paths(
    app_root: str | os.PathLike[str],
    *,
    environ: Mapping[str, str] | None = None,
) -> CurrentUserOnboardingPaths:
    values = os.environ if environ is None else environ
    selected_app_root = _resolved(app_root)
    local_app_data = str(values.get("LOCALAPPDATA") or "").strip()
    explicit_data_root = str(values.get(LABEL_MATCH_DATA_ROOT_ENV) or "").strip()
    if explicit_data_root:
        data_root = _resolved(explicit_data_root)
    elif local_app_data:
        data_root = _resolved(local_app_data) / "KMTech" / "Label_Match" / "data"
    else:
        raise CurrentUserOnboardingError(
            "LOCALAPPDATA is unavailable for current-user onboarding",
            report_path=(
                selected_app_root / "current-user-onboarding-unavailable.json"
            ),
            status="UNKNOWN",
        )

    explicit_settings = str(values.get(LABEL_MATCH_SETTINGS_PATH_ENV) or "").strip()
    if explicit_settings:
        settings_path = _resolved(explicit_settings)
    elif local_app_data:
        settings_path = (
            _resolved(local_app_data)
            / "KMTech"
            / "Label_Match"
            / "config"
            / "app_settings.json"
        )
    else:
        settings_path = data_root.parent / "config" / "app_settings.json"

    explicit_direct_sync_root = str(
        values.get(LABEL_MATCH_DIRECT_SYNC_ROOT_ENV)
        or values.get(LEGACY_DIRECT_SYNC_ROOT_ENV)
        or ""
    ).strip()
    if explicit_direct_sync_root:
        direct_sync_root = _resolved(explicit_direct_sync_root)
    elif local_app_data:
        direct_sync_root = (
            _resolved(local_app_data) / "KMTech" / "DirectSync" / "label_match"
        )
    else:
        direct_sync_root = data_root.parent / "direct_sync"

    explicit_profile = str(values.get(PROFILE_PATH_ENV) or "").strip()
    if explicit_profile:
        logistics_profile_path = _resolved(explicit_profile)
    elif local_app_data:
        logistics_profile_path = (
            _resolved(local_app_data)
            / "KMTech"
            / "Logistics"
            / "profiles"
            / "Label_Match"
            / "runtime-profile.json"
        )
    else:
        logistics_profile_path = (
            data_root.parent / "logistics-profile" / "runtime-profile.json"
        )
    bootstrap_tls_ca_bundle_path = (
        _resolved(local_app_data)
        / "KMTech"
        / "Bootstrap"
        / "Label_Match"
        / "ca-bundle.pem"
        if local_app_data
        else data_root.parent / "Bootstrap" / "Label_Match" / "ca-bundle.pem"
    )

    for candidate in (
        data_root,
        settings_path,
        direct_sync_root,
        logistics_profile_path,
    ):
        if _is_legacy_sync_path(candidate):
            raise CurrentUserOnboardingError(
                "current-user onboarding state must not use the legacy Sync root",
                report_path=(
                    direct_sync_root / "status" / "current_user_onboarding.json"
                ),
            )

    status_dir = direct_sync_root / "status"
    return CurrentUserOnboardingPaths(
        app_root=selected_app_root,
        data_root=data_root,
        settings_path=settings_path,
        direct_sync_root=direct_sync_root,
        queue_dir=direct_sync_root / "queue",
        spool_dir=direct_sync_root / "spool",
        upload_status_dir=direct_sync_root / "upload_status",
        status_dir=status_dir,
        logs_dir=direct_sync_root / "logs",
        control_dir=direct_sync_root / "control",
        identity_path=direct_sync_root / "producer_identity.json",
        producer_manifest_path=direct_sync_root / "producer_manifest.json",
        credential_path=direct_sync_root / "credential.json",
        registration_receipt_path=(
            direct_sync_root / "evidence" / "producer_self_enrollment_receipt.json"
        ),
        registration_report_path=(
            status_dir / "label_match_worker_pc_registration.json"
        ),
        onboarding_report_path=status_dir / "current_user_onboarding.json",
        removal_report_path=status_dir / "current_user_removal.json",
        logistics_profile_path=logistics_profile_path,
        logistics_secret_path=(
            logistics_profile_path.parent / "secrets" / "bearer-token.dpapi"
        ),
        bootstrap_tls_ca_bundle_path=bootstrap_tls_ca_bundle_path,
        ledger_path=data_root / "package_logistics_outbox.sqlite3",
        bootstrap_integrity_path=selected_app_root / "bootstrap-integrity.json",
    )


def _write_json_atomic(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.{uuid.uuid4().hex}.tmp")
    try:
        with temporary.open("w", encoding="utf-8", newline="\n") as handle:
            json.dump(
                dict(payload), handle, ensure_ascii=False, indent=2, sort_keys=True
            )
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def _read_json(
    path: Path,
    purpose: str,
    *,
    maximum_bytes: int = ONBOARDING_JSON_MAX_BYTES,
) -> dict[str, Any]:
    try:
        with path.open("rb") as handle:
            raw = handle.read(maximum_bytes + 1)
    except OSError as exc:
        raise ValueError(f"{purpose} is absent") from exc
    if not raw or len(raw) > maximum_bytes:
        raise ValueError(f"{purpose} size is invalid")
    try:
        value = json.loads(raw.decode("utf-8-sig"))
    except (UnicodeError, json.JSONDecodeError) as exc:
        raise ValueError(f"{purpose} is not valid UTF-8 JSON") from exc
    if not isinstance(value, dict):
        raise ValueError(f"{purpose} must be a JSON object")
    return value


def _file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _is_reparse_point(path: Path) -> bool:
    try:
        attributes = int(getattr(path.lstat(), "st_file_attributes", 0))
    except OSError as exc:
        raise ValueError(f"bootstrap code path cannot be inspected: {path}") from exc
    return path.is_symlink() or bool(attributes & 0x400)


def _calculate_code_root_hash(
    app_root: Path,
    *,
    integrity_record_name: str,
) -> tuple[int, str, bool]:
    """Hash the exact code file set without serializing the per-file inventory."""

    canonical_entries: list[bytes] = []
    main_executable_present = False
    try:
        candidates = app_root.rglob("*")
        for candidate in candidates:
            if _is_reparse_point(candidate):
                raise ValueError(f"bootstrap code path is redirected: {candidate}")
            if not candidate.is_file():
                continue
            relative_path = candidate.relative_to(app_root).as_posix()
            if relative_path.casefold() == integrity_record_name.casefold():
                continue
            try:
                before = candidate.stat()
                content_hash = _file_sha256(candidate)
                after = candidate.stat()
            except OSError as exc:
                raise ValueError(
                    f"bootstrap code file cannot be inspected: {relative_path}"
                ) from exc
            if (
                before.st_size != after.st_size
                or before.st_mtime_ns != after.st_mtime_ns
            ):
                raise ValueError(
                    f"bootstrap code file changed during verification: {relative_path}"
                )
            canonical_entries.append(
                (
                    f"{content_hash} {after.st_size} "
                    f"{relative_path.encode('utf-8').hex()}\n"
                ).encode("ascii")
            )
            if relative_path.casefold() == "label_match.exe":
                main_executable_present = True
    except OSError as exc:
        raise ValueError("bootstrap code inventory cannot be enumerated") from exc

    canonical_entries.sort()
    digest = hashlib.sha256()
    digest.update(BOOTSTRAP_ROOT_HASH_DOMAIN)
    for entry in canonical_entries:
        digest.update(entry)
    return len(canonical_entries), digest.hexdigest(), main_executable_present


def verify_bootstrap_integrity(
    paths: CurrentUserOnboardingPaths,
    *,
    required: bool,
) -> dict[str, Any]:
    if not required:
        return {"status": "NOT_TESTED", "reason": "source-mode onboarding"}
    try:
        record_stat = paths.bootstrap_integrity_path.lstat()
    except FileNotFoundError:
        return {
            "status": "ABSENT",
            "reason": "bootstrap integrity record is absent",
            "warning": True,
            "record_path": str(paths.bootstrap_integrity_path),
        }
    except OSError as exc:
        raise ValueError("bootstrap integrity record cannot be inspected") from exc
    if not stat.S_ISREG(record_stat.st_mode):
        raise ValueError("bootstrap integrity record is not a regular file")
    record = _read_json(
        paths.bootstrap_integrity_path,
        "bootstrap integrity record",
        maximum_bytes=BOOTSTRAP_INTEGRITY_MAX_BYTES,
    )
    if record.get("schema_version") == PORTABLE_BOOTSTRAP_INTEGRITY_VERSION:
        rows: list[dict[str, Any]] = []
        frozen_main_present = False
        portable_pythonw_present = False
        portable_main_present = False
        for candidate in sorted(
            paths.app_root.rglob("*"),
            key=lambda path: path.relative_to(paths.app_root).as_posix().casefold(),
        ):
            if _is_reparse_point(candidate):
                raise ValueError(f"bootstrap code path is redirected: {candidate}")
            if not candidate.is_file():
                continue
            relative_path = candidate.relative_to(paths.app_root).as_posix()
            if (
                relative_path.casefold()
                == paths.bootstrap_integrity_path.name.casefold()
            ):
                continue
            before = candidate.stat()
            content_hash = _file_sha256(candidate)
            after = candidate.stat()
            if before.st_size != after.st_size or before.st_mtime_ns != after.st_mtime_ns:
                raise ValueError(
                    f"bootstrap code file changed during verification: {relative_path}"
                )
            rows.append(
                {
                    "path": relative_path,
                    "size": after.st_size,
                    "sha256": content_hash,
                }
            )
            folded = relative_path.casefold()
            frozen_main_present = frozen_main_present or folded == "label_match.exe"
            portable_pythonw_present = (
                portable_pythonw_present or folded == "runtime/pythonw.exe"
            )
            portable_main_present = portable_main_present or folded == "app/main.py"
        aggregate = hashlib.sha256(
            "".join(
                f"{row['sha256']} {row['size']} {row['path']}\n" for row in rows
            ).encode("utf-8")
        ).hexdigest()
        code_root = str(record.get("code_root") or "").strip()
        resolved_code_root = (
            paths.app_root if code_root == "." else _resolved(code_root)
        )
        portable_layout = portable_pythonw_present and portable_main_present
        if (
            record.get("status") != "PASS"
            or resolved_code_root != paths.app_root
            or type(record.get("file_count")) is not int
            or record.get("file_count") != len(rows)
            or record.get("files") != rows
            or str(record.get("aggregate_sha256") or "").lower() != aggregate
            or record.get("identity_profile_created") is not False
            or record.get("state_scope") != "current_user_first_run"
            or frozen_main_present == portable_layout
        ):
            raise ValueError("bootstrap inventory integrity record is invalid")
        return {
            "status": "PASS",
            "schema_version": PORTABLE_BOOTSTRAP_INTEGRITY_VERSION,
            "record_path": str(paths.bootstrap_integrity_path),
            "code_root": str(paths.app_root),
            "file_count": len(rows),
            "aggregate_sha256": aggregate,
            "package_layout": "onedir" if frozen_main_present else "portable_cpython",
        }
    if record.get("schema_version") != BOOTSTRAP_INTEGRITY_VERSION:
        raise ValueError("bootstrap integrity record schema is invalid")
    if record.get("status") != "PASS":
        raise ValueError("bootstrap integrity record is not PASS")
    if record.get("package_layout") != "onedir":
        raise ValueError("bootstrap integrity record lost the onedir package layout")
    code_root_value = str(record.get("code_root") or "").strip()
    record_code_root = (
        _resolved(paths.bootstrap_integrity_path.parent)
        if code_root_value == "."
        else _resolved(code_root_value)
    )
    if record_code_root != paths.app_root:
        raise ValueError("bootstrap integrity record code root is invalid")
    if record.get("inventory_algorithm") != BOOTSTRAP_INVENTORY_ALGORITHM:
        raise ValueError("bootstrap integrity record algorithm is invalid")
    file_count = record.get("file_count")
    if type(file_count) is not int or file_count <= 0:
        raise ValueError("bootstrap integrity record file count is invalid")
    expected_root_hash = str(record.get("root_sha256") or "").strip().lower()
    if (
        len(expected_root_hash) != 64
        or any(character not in "0123456789abcdef" for character in expected_root_hash)
        or "files" in record
    ):
        raise ValueError("bootstrap integrity record root hash is invalid")
    actual_count, actual_root_hash, main_executable_present = _calculate_code_root_hash(
        paths.app_root,
        integrity_record_name=paths.bootstrap_integrity_path.name,
    )
    if actual_count != file_count or actual_root_hash != expected_root_hash:
        raise ValueError("bootstrap code root integrity failed")
    if not main_executable_present:
        raise ValueError("bootstrap integrity record does not identify Label_Match.exe")
    if not (paths.app_root / "Label_Match.exe").is_file():
        raise ValueError("hardened Label_Match executable is absent")
    if not (paths.app_root / "_internal").is_dir():
        raise ValueError("hardened Label_Match onedir runtime is absent")
    return {
        "status": "PASS",
        "record_path": str(paths.bootstrap_integrity_path),
        "code_root": str(paths.app_root),
        "file_count": actual_count,
        "root_sha256": actual_root_hash,
        "inventory_algorithm": BOOTSTRAP_INVENTORY_ALGORITHM,
        "package_layout": "onedir",
    }


def _default_profile_loader(path: Path) -> Any:
    return load_logistics_runtime_profile(
        required=True,
        profile_path=path,
        decryptor=unprotect_current_user_secret,
    )


def inspect_current_user_state(
    paths: CurrentUserOnboardingPaths,
    *,
    profile_loader: Callable[[Path], Any] = _default_profile_loader,
    credential_loader: Callable[[Path], Any] = load_credentials_from_json,
) -> dict[str, Any]:
    state_paths = {
        "identity": paths.identity_path,
        "producer_manifest": paths.producer_manifest_path,
        "credential": paths.credential_path,
        "registration_report": paths.registration_report_path,
        "logistics_profile": paths.logistics_profile_path,
        "logistics_secret": paths.logistics_secret_path,
    }
    present = {name: path.is_file() for name, path in state_paths.items()}
    if not any(present.values()):
        return {"status": "ABSENT", "present": present}
    if present["registration_report"] and sum(present.values()) == 1:
        report = _read_json(paths.registration_report_path, "registration report")
        if str(report.get("status") or "") in {"BLOCKED", "FAILED", "UNKNOWN"}:
            return {"status": "ABSENT_RETRYABLE", "present": present}
    if not all(present.values()):
        return {
            "status": "RECOVERY_REQUIRED",
            "present": present,
            "reason": "current-user onboarding state is partial",
        }
    try:
        identity = _read_json(paths.identity_path, "producer identity")
        manifest = _read_json(paths.producer_manifest_path, "producer manifest")
        credential = _read_json(paths.credential_path, "producer credential")
        registration = _read_json(paths.registration_report_path, "registration report")
        profile_payload = _read_json(paths.logistics_profile_path, "logistics profile")
        required_identity = {
            field: str(identity.get(field) or "").strip()
            for field in ("producer_id", "source_host_id", "producer_install_id")
        }
        if not all(required_identity.values()):
            raise ValueError("producer identity is incomplete")
        pc_identity = manifest.get("pc_identity")
        if not isinstance(pc_identity, Mapping):
            raise ValueError("producer manifest identity is absent")
        if (
            str(pc_identity.get("source_host_id") or "")
            != required_identity["source_host_id"]
            or str(pc_identity.get("producer_install_id") or "")
            != required_identity["producer_install_id"]
        ):
            raise ValueError("producer identity and manifest binding differ")
        expected_manifest_hash = str(registration.get("manifest_hash") or "").lower()
        if (
            registration.get("server_registration_verified") is not True
            or registration.get("manifest_hash_verified") is not True
            or registration.get("persisted_manifest_hash_verified") is not True
            or len(expected_manifest_hash) != 64
            or manifest_hash(manifest) != expected_manifest_hash
        ):
            raise ValueError("server-authorized manifest readback is incomplete")
        if str(credential.get("dpapi_scope") or "") != "current_user":
            raise ValueError("producer credential is not current-user scoped")
        if str(profile_payload.get("credential_scope") or "") != "current_user":
            raise ValueError("logistics profile is not current-user scoped")
        resolved_credential = credential_loader(paths.credential_path)
        resolved_profile = profile_loader(paths.logistics_profile_path)
        if resolved_credential is None or resolved_profile is None:
            raise ValueError("credential/profile readback returned no value")
        if (
            str(getattr(resolved_profile, "source_host_id", ""))
            != required_identity["source_host_id"]
        ):
            raise ValueError("logistics profile identity binding differs")
        if (
            str(getattr(resolved_profile, "authority_plane", "")).upper()
            != "AUTHORITATIVE"
        ):
            raise ValueError("Label_Match requires AUTHORITATIVE logistics authority")
    except Exception as exc:
        return {
            "status": "RECOVERY_REQUIRED",
            "present": present,
            "reason": str(exc),
            "error_type": exc.__class__.__name__,
        }
    return {
        "status": "READY",
        "present": present,
        "source_host_id": required_identity["source_host_id"],
        "producer_install_id": required_identity["producer_install_id"],
        "manifest_hash": expected_manifest_hash,
        "authority_plane": "AUTHORITATIVE",
        "tls_private_ca_configured": bool(
            getattr(resolved_profile, "tls_ca_bundle_path", "")
        ),
    }


def _configured_tls_ca_bundle_source(
    paths: CurrentUserOnboardingPaths,
    environ: Mapping[str, str] | None = None,
) -> str:
    values = os.environ if environ is None else environ
    explicit = str(values.get(ENROLLMENT_TLS_CA_BUNDLE_PATH_ENV) or "").strip()
    if explicit:
        return explicit
    if paths.bootstrap_tls_ca_bundle_path.is_file():
        return str(paths.bootstrap_tls_ca_bundle_path)
    return ""


def _registration_runner(
    paths: CurrentUserOnboardingPaths,
    *,
    server_base_url: str,
    environ: Mapping[str, str] | None = None,
) -> int:
    from tools import register_label_match_worker_pc

    arguments = [
        "--apply",
        "--server-base-url",
        server_base_url,
        "--data-dir",
        str(paths.direct_sync_root),
        "--sync-dir",
        str(paths.data_root),
        "--require-machine-credential-bundle",
        "--credential-scope",
        "current_user",
        "--logistics-profile-path",
        str(paths.logistics_profile_path),
        "--identity-path",
        str(paths.identity_path),
        "--manifest-path",
        str(paths.producer_manifest_path),
        "--credential-path",
        str(paths.credential_path),
        "--receipt-path",
        str(paths.registration_receipt_path),
        "--report-path",
        str(paths.registration_report_path),
    ]
    tls_ca_source = _configured_tls_ca_bundle_source(paths, environ)
    if tls_ca_source:
        arguments.extend(["--tls-ca-bundle-path", tls_ca_source])
    return int(register_label_match_worker_pc.main(arguments))


def _create_ledger(path: Path) -> None:
    from package_logistics import PackageOutbox
    from terminal_operation_lease import OperationLeaseStore

    PackageOutbox(path)
    OperationLeaseStore(path)


def _ensure_user_settings(paths: CurrentUserOnboardingPaths) -> dict[str, Any]:
    if paths.settings_path.is_file():
        _read_json(paths.settings_path, "current-user settings")
        return {"status": "REUSED", "path": str(paths.settings_path)}
    candidates = (
        paths.app_root / "_internal" / "config" / "app_settings.json",
        paths.app_root / "config" / "app_settings.json",
    )
    template = next((path for path in candidates if path.is_file()), None)
    payload = _read_json(template, "packaged settings template") if template else {}
    _write_json_atomic(paths.settings_path, payload)
    return {
        "status": "CREATED",
        "path": str(paths.settings_path),
        "template": str(template) if template else "empty_source_mode_default",
    }


def apply_current_user_runtime_environment(
    paths: CurrentUserOnboardingPaths,
    *,
    environ: MutableMapping[str, str] | None = None,
) -> None:
    values = os.environ if environ is None else environ
    values[LABEL_MATCH_DATA_ROOT_ENV] = str(paths.data_root)
    values[LABEL_MATCH_SETTINGS_PATH_ENV] = str(paths.settings_path)
    values[LABEL_MATCH_DIRECT_SYNC_ROOT_ENV] = str(paths.direct_sync_root)
    values[LEGACY_DIRECT_SYNC_ROOT_ENV] = str(paths.direct_sync_root)
    values[PROFILE_PATH_ENV] = str(paths.logistics_profile_path)
    values[REQUIRED_ENV] = "1"


@writer_sink("current_user_onboarding")
def onboard_current_user(
    app_root: str | os.PathLike[str],
    *,
    environ: MutableMapping[str, str] | None = None,
    server_base_url: str = DEFAULT_SERVER_BASE_URL,
    require_bootstrap_integrity: bool | None = None,
    registration_runner: Callable[[CurrentUserOnboardingPaths], Any] | None = None,
    profile_loader: Callable[[Path], Any] = _default_profile_loader,
    credential_loader: Callable[[Path], Any] = load_credentials_from_json,
    ledger_factory: Callable[[Path], None] = _create_ledger,
    settings_factory: Callable[
        [CurrentUserOnboardingPaths], Mapping[str, Any]
    ] = _ensure_user_settings,
    autostart_installer: Callable[
        [str | os.PathLike[str]], Mapping[str, Any]
    ] = install_user_relay_autostart,
    scheduled_task_installer: Callable[
        [str | os.PathLike[str]], Mapping[str, Any]
    ] = install_current_user_scheduled_task,
    legacy_task_quiescence_reader: Callable[
        [], Mapping[str, Any]
    ] = read_legacy_system_task_quiescence,
    relay_launcher: Callable[
        [str | os.PathLike[str]], Mapping[str, Any]
    ] = start_user_relay_process,
) -> dict[str, Any]:
    paths = resolve_current_user_onboarding_paths(app_root, environ=environ)
    tls_ca_source = _configured_tls_ca_bundle_source(paths, environ)
    for directory in (
        paths.data_root,
        paths.settings_path.parent,
        paths.direct_sync_root,
        paths.queue_dir,
        paths.spool_dir,
        paths.upload_status_dir,
        paths.status_dir,
        paths.logs_dir,
        paths.control_dir,
        paths.logistics_profile_path.parent,
    ):
        directory.mkdir(parents=True, exist_ok=True)
    require_integrity = (
        bool(getattr(sys, "frozen", False))
        if require_bootstrap_integrity is None
        else bool(require_bootstrap_integrity)
    )
    report: dict[str, Any] = {
        "report_version": ONBOARDING_REPORT_VERSION,
        "status": "UNKNOWN",
        "action": "UNKNOWN",
        "captured_at": _now(),
        "state_scope": "current_user",
        "elevation_required": False,
        "data_root": str(paths.data_root),
        "settings_path": str(paths.settings_path),
        "direct_sync_root": str(paths.direct_sync_root),
        "logistics_profile_path": str(paths.logistics_profile_path),
        "ledger_path": str(paths.ledger_path),
        "tls_ca_bundle_source_configured": bool(tls_ca_source),
        "server_registration_verified": False,
        "failure": "",
    }
    stop_marker_released = False

    def restore_stop_marker_fence() -> None:
        nonlocal stop_marker_released
        if not stop_marker_released:
            return
        try:
            stop_path = user_relay_stop_path(paths.direct_sync_root)
            if stop_path.exists():
                report["stop_marker_refence"] = {"status": "ALREADY_PRESENT"}
            else:
                report["stop_marker_refence"] = dict(
                    request_user_relay_stop(paths.direct_sync_root, timeout_seconds=0)
                )
        except Exception as refence_error:
            report["stop_marker_refence"] = {
                "status": "FAILED",
                "failure": str(refence_error)[:500],
                "error_type": refence_error.__class__.__name__,
            }
        stop_marker_released = False
    try:
        report["legacy_task_quiescence"] = dict(legacy_task_quiescence_reader())
        legacy_quiescence = report["legacy_task_quiescence"]
        if (
            legacy_quiescence.get("schema") != LEGACY_TASK_QUIESCENCE_VERSION
            or legacy_quiescence.get("required_state")
            != LEGACY_TASK_REQUIRED_STATE
            or legacy_quiescence.get("read_only") is not True
            or legacy_quiescence.get("task_or_process_mutated") is not False
        ):
            raise ValueError("legacy scheduled-task quiescence evidence is invalid")
        if legacy_quiescence.get("status") != "PASS":
            reason = str(
                legacy_quiescence.get("reason_code")
                or "LEGACY_TASK_QUIESCENCE_FAILED"
            )
            remediation = str(
                legacy_quiescence.get("remediation")
                or "disable or remove the legacy Label scheduled task"
            )
            raise ValueError(f"{reason}: {remediation}")
        report["bootstrap_integrity"] = verify_bootstrap_integrity(
            paths,
            required=require_integrity,
        )
        state = inspect_current_user_state(
            paths,
            profile_loader=profile_loader,
            credential_loader=credential_loader,
        )
        report["initial_state"] = state
        if state["status"] == "READY" and tls_ca_source:
            from tools.install_logistics_runtime_profile import (
                install_tls_ca_bundle_for_existing_profile,
            )

            report["tls_ca_bundle_upgrade"] = (
                install_tls_ca_bundle_for_existing_profile(
                    profile_path=paths.logistics_profile_path,
                    tls_ca_bundle_path=tls_ca_source,
                    credential_scope="current_user",
                )
            )
            state = inspect_current_user_state(
                paths,
                profile_loader=profile_loader,
                credential_loader=credential_loader,
            )
        if state["status"] == "RECOVERY_REQUIRED":
            raise ValueError(str(state.get("reason") or "partial current-user state"))
        if state["status"] in {"ABSENT", "ABSENT_RETRYABLE"}:
            with EnrollmentMutex() as mutex_receipt:
                report["enrollment_mutex"] = mutex_receipt
                # The first inspection occurs before mutex ownership.  Repeat
                # it while owned so a simultaneous onboarding winner cannot
                # be followed by a second enrollment attempt.
                state = inspect_current_user_state(
                    paths,
                    profile_loader=profile_loader,
                    credential_loader=credential_loader,
                )
                if state["status"] == "RECOVERY_REQUIRED":
                    raise ValueError(
                        str(state.get("reason") or "partial current-user state")
                    )
                if state["status"] in {"ABSENT", "ABSENT_RETRYABLE"}:
                    if registration_runner is None:
                        return_code = _registration_runner(
                            paths,
                            server_base_url=server_base_url,
                            environ=environ,
                        )
                    else:
                        return_code = registration_runner(paths)
                    if type(return_code) is not int:
                        raise CurrentUserOnboardingError(
                            "registration result is UNKNOWN because no exit code was returned",
                            report_path=paths.onboarding_report_path,
                            status="UNKNOWN",
                        )
                    if return_code != 0:
                        raise ValueError(
                            f"current-user registration failed with exit code {return_code}"
                        )
                    state = inspect_current_user_state(
                        paths,
                        profile_loader=profile_loader,
                        credential_loader=credential_loader,
                    )
                    if state["status"] != "READY":
                        raise ValueError(
                            "registration returned success without complete current-user readback"
                        )
                    report["action"] = "CREATED"
                elif state["status"] == "READY":
                    report["action"] = "REUSED_AFTER_MUTEX_WAIT"
                else:
                    raise ValueError(
                        "current-user state became indeterminate under the enrollment mutex"
                    )
        else:
            report["action"] = "REUSED"
        if tls_ca_source and not state.get("tls_private_ca_configured"):
            raise ValueError(
                "configured TLS CA bundle was not persisted in the logistics profile"
            )

        ledger_factory(paths.ledger_path)
        if not paths.ledger_path.is_file():
            raise ValueError("current-user business ledger readback failed")
        report["settings"] = dict(settings_factory(paths))
        settings_status = str(report["settings"].get("status") or "")
        if settings_status not in {"CREATED", "REUSED"}:
            raise ValueError("current-user settings placement was not proven")

        stop_path = user_relay_stop_path(paths.direct_sync_root)
        report["stop_marker_release"] = _portable_stop_marker_release_preflight(
            paths,
            environ=environ,
        )
        report["relay_autostart"] = dict(autostart_installer(paths.app_root))
        autostart_status = str(report["relay_autostart"].get("status") or "")
        if autostart_status in {"", "UNKNOWN"}:
            raise CurrentUserOnboardingError(
                "current-user relay autostart result is UNKNOWN",
                report_path=paths.onboarding_report_path,
                status="UNKNOWN",
            )
        if autostart_status != "PASS":
            raise ValueError("current-user relay autostart was not proven")
        report["scheduled_task"] = dict(scheduled_task_installer(paths.app_root))
        scheduled_task_status = str(report["scheduled_task"].get("status") or "")
        if scheduled_task_status in {"", "UNKNOWN"}:
            raise CurrentUserOnboardingError(
                "current-user scheduled-task result is UNKNOWN",
                report_path=paths.onboarding_report_path,
                status="UNKNOWN",
            )
        if scheduled_task_status != "PASS":
            raise ValueError("current-user scheduled task was not proven")
        # The marker is a safety fence.  It is released only after canonical
        # install ownership, HKCU, and Limited PT1M task bindings all read back.
        if report["stop_marker_release"].get("marker_present"):
            release = release_user_relay_stop_marker(
                paths.direct_sync_root,
                expected_request_id=report["stop_marker_release"][
                    "current_stop_marker_request_id"
                ],
                expected_sha256=report["stop_marker_release"][
                    "current_stop_marker_sha256"
                ],
            )
            if release.get("status") != "RELEASED":
                raise ValueError("relay stop marker release was not proven")
            stop_marker_released = True
        else:
            release = {"status": "NOT_REQUIRED"}
        report["stop_marker_release"]["release"] = release
        report["relay_start"] = dict(relay_launcher(paths.app_root))
        relay_start_status = str(report["relay_start"].get("status") or "")
        if relay_start_status in {"", "UNKNOWN"}:
            raise CurrentUserOnboardingError(
                "current-user relay launch result is UNKNOWN",
                report_path=paths.onboarding_report_path,
                status="UNKNOWN",
            )
        if relay_start_status != "ALIVE":
            raise ValueError("current-user relay survival was not proven")
        apply_current_user_runtime_environment(paths, environ=environ)
        report.update(
            {
                "status": "READY",
                "state_readback": state,
                "server_registration_verified": True,
                "ledger_status": "READY",
                "operation_lease_store": "AUTHORITATIVE_SNAPSHOT_PRESERVED",
                "persistent_relay_principal": "current_user",
                "system_scheduled_task_required": False,
                "current_user_scheduled_task_required": True,
                "completed_at": _now(),
            }
        )
        _write_json_atomic(paths.onboarding_report_path, report)
        return report
    except EnrollmentMutexError as exc:
        restore_stop_marker_fence()
        report["status"] = exc.report_status
        report["failure"] = exc.reason_code
        report["error_type"] = exc.__class__.__name__
        report["recovery_action"] = exc.recovery_action
        report["enrollment_mutex"] = dict(exc.mutex_report)
        _write_json_atomic(paths.onboarding_report_path, report)
        raise CurrentUserOnboardingError(
            f"Label_Match enrollment mutex blocked onboarding: {exc.reason_code}",
            report_path=paths.onboarding_report_path,
            status=exc.report_status,
        ) from exc
    except CurrentUserOnboardingError as exc:
        restore_stop_marker_fence()
        report["status"] = exc.status
        report["failure"] = str(exc)
        report["error_type"] = exc.__class__.__name__
        _write_json_atomic(paths.onboarding_report_path, report)
        raise
    except Exception as exc:
        restore_stop_marker_fence()
        report["status"] = "FAILED"
        report["failure"] = str(exc)[:500]
        report["error_type"] = exc.__class__.__name__
        _write_json_atomic(paths.onboarding_report_path, report)
        raise CurrentUserOnboardingError(
            f"Label_Match first-run onboarding failed: {exc}",
            report_path=paths.onboarding_report_path,
            status="FAILED",
        ) from exc


@writer_sink("current_user_setup_removal")
def remove_current_user_setup(
    app_root: str | os.PathLike[str],
    *,
    environ: Mapping[str, str] | None = None,
    autostart_remover: Callable[[], Mapping[str, Any]] = remove_user_relay_autostart,
    scheduled_task_remover: Callable[
        [str | os.PathLike[str]], Mapping[str, Any]
    ] = remove_current_user_scheduled_task,
    relay_stopper: Callable[
        [str | os.PathLike[str]], Mapping[str, Any]
    ] = request_user_relay_stop,
) -> dict[str, Any]:
    paths = resolve_current_user_onboarding_paths(app_root, environ=environ)
    paths.status_dir.mkdir(parents=True, exist_ok=True)
    report: dict[str, Any] = {
        "report_version": REMOVAL_REPORT_VERSION,
        "status": "UNKNOWN",
        "captured_at": _now(),
        "state_scope": "current_user",
        "data_preserved": True,
        "preserved_paths": [
            str(paths.data_root),
            str(paths.settings_path),
            str(paths.direct_sync_root),
            str(paths.logistics_profile_path.parent),
        ],
        "machine_code_root": str(paths.app_root),
        "machine_code_removal_requires_elevation": True,
        "failure": "",
    }
    try:
        report["relay_autostart"] = dict(autostart_remover())
        report["scheduled_task"] = dict(scheduled_task_remover(paths.app_root))
        report["relay_process"] = dict(relay_stopper(paths.direct_sync_root))
        autostart_status = str(report["relay_autostart"].get("status") or "")
        scheduled_task_status = str(report["scheduled_task"].get("status") or "")
        relay_status = str(report["relay_process"].get("status") or "")
        if (
            autostart_status in {"", "UNKNOWN"}
            or scheduled_task_status in {"", "UNKNOWN"}
            or relay_status in {"", "UNKNOWN"}
        ):
            raise CurrentUserOnboardingError(
                "current-user removal result is UNKNOWN",
                report_path=paths.removal_report_path,
                status="UNKNOWN",
            )
        if autostart_status != "ABSENT":
            raise ValueError("HKCU relay persistence absence was not proven")
        if scheduled_task_status != "ABSENT":
            raise ValueError("current-user task absence was not proven")
        if relay_status != "ABSENT":
            raise ValueError("current-user relay process absence is UNKNOWN")
        report.update({"status": "PASS_DATA_PRESERVED", "completed_at": _now()})
        _write_json_atomic(paths.removal_report_path, report)
        return report
    except CurrentUserOnboardingError as exc:
        report.update(
            {
                "status": exc.status,
                "failure": str(exc)[:500],
                "error_type": exc.__class__.__name__,
            }
        )
        _write_json_atomic(paths.removal_report_path, report)
        raise
    except Exception as exc:
        report.update(
            {
                "status": "FAILED",
                "failure": str(exc)[:500],
                "error_type": exc.__class__.__name__,
            }
        )
        _write_json_atomic(paths.removal_report_path, report)
        raise CurrentUserOnboardingError(
            f"Label_Match current-user removal failed: {exc}",
            report_path=paths.removal_report_path,
        ) from exc


def _default_app_root() -> Path:
    if getattr(sys, "frozen", False):
        return Path(sys.executable).resolve().parent
    return Path(__file__).resolve().parent


def onboarding_main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Onboard Label_Match for the current user"
    )
    parser.add_argument("--app-root", default=str(_default_app_root()))
    parser.add_argument("--server-base-url", default=DEFAULT_SERVER_BASE_URL)
    args = parser.parse_args(argv)
    try:
        report = onboard_current_user(
            args.app_root,
            server_base_url=args.server_base_url,
            require_bootstrap_integrity=bool(getattr(sys, "frozen", False)),
        )
    except CurrentUserOnboardingError as exc:
        print(f"onboarding_status={exc.status}")
        print(f"onboarding_report={exc.report_path}")
        return ONBOARDING_EXIT_CODE
    print(f"onboarding_status={report['status']}")
    print(f"onboarding_action={report['action']}")
    print(
        "onboarding_report="
        f"{resolve_current_user_onboarding_paths(args.app_root).onboarding_report_path}"
    )
    return 0


def removal_main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Remove Label_Match current-user setup"
    )
    parser.add_argument("--app-root", default=str(_default_app_root()))
    args = parser.parse_args(argv)
    try:
        report = remove_current_user_setup(args.app_root)
    except CurrentUserOnboardingError as exc:
        print(f"current_user_removal_status={exc.status}")
        print(f"current_user_removal_report={exc.report_path}")
        return ONBOARDING_EXIT_CODE
    paths = resolve_current_user_onboarding_paths(args.app_root)
    print(f"current_user_removal_status={report['status']}")
    print("data_preserved=true")
    print(f"current_user_removal_report={paths.removal_report_path}")
    print("machine_code_removal_command=INSTALL_THIS_PC.ps1 -Uninstall")
    return 0


if __name__ == "__main__":
    raise SystemExit(onboarding_main())
