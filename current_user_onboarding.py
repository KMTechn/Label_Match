"""First-run current-user state onboarding for Label_Match."""

from __future__ import annotations

import argparse
from dataclasses import dataclass, replace
from datetime import datetime, timezone
import hashlib
import json
import logging
import os
import re
from pathlib import Path
import stat
import sys
from typing import Any, Callable, Mapping, MutableMapping
from urllib.parse import urlsplit
import uuid

from current_user_scheduled_task import (
    LEGACY_TASK_QUIESCENCE_VERSION,
    LEGACY_TASK_REQUIRED_STATE,
    read_legacy_system_task_quiescence,
    remove_current_user_scheduled_task,
)
from direct_sync_push import manifest_hash
from enrollment_mutex import EnrollmentMutex, EnrollmentMutexError
from direct_sync_runtime import load_credentials_from_json, prepare_credentials_from_json
from label_exact_clone_resolution import read_pinned_json, validate_resolution_receipt
from label_match_product_host import _default_product_root, requires_bootstrap_integrity
from label_match_single_instance import SingleInstanceError, resolve_data_scope
from logistics_runtime_profile import (
    APP_PROFILE_PATH_ENV as LABEL_MATCH_LOGISTICS_PROFILE_PATH_ENV,
    LogisticsRuntimeConfigurationError,
    REQUIRED_ENV,
    assert_path_has_no_reparse_components,
    default_logistics_profile_path,
    load_logistics_runtime_profile,
    unprotect_current_user_secret,
)
from user_relay import (
    install_user_relay_autostart,
    release_user_relay_stop_marker,
    remove_user_relay_autostart,
    request_user_relay_stop,
    reuse_or_start_user_relay_process,
    user_relay_stop_path,
)
from writer_session_fence import writer_admission, writer_sink
from label_data_generation import DataGenerationError, check_user_data_open, record_user_data_open

DEFAULT_SERVER_BASE_URL = "https://worker.kmtecherp.com"
ONBOARDING_REPORT_VERSION = "label-match-current-user-onboarding-v1"
# The inventory declares this sink once; each bounded phase uses its source.
ONBOARDING_WRITER_SOURCE = "current_user_onboarding"
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


from label_atomic_replace import atomic_replace

class CurrentUserOnboardingError(RuntimeError):
    def __init__(
        self,
        message: str,
        *,
        report_path: Path,
        status: str = "FAILED",
        cause_code: str = "SETUP_FAILED",
        recovery_command: str = "",
    ) -> None:
        super().__init__(message)
        self.report_path = Path(report_path)
        self.status = status
        self.cause_code = cause_code
        self.recovery_command = recovery_command


def _verified_bootstrap_inventory_aggregate(
    record: Mapping[str, Any], rows: list[dict[str, Any]]
) -> str | None:
    recorded_rows = record.get("files")
    if not isinstance(recorded_rows, list) or any(
        not isinstance(row, dict)
        or not isinstance(row.get("path"), str)
        or type(row.get("size")) is not int
        or not isinstance(row.get("sha256"), str)
        for row in recorded_rows
    ):
        return None
    # Match the installer's ordinal UTF-16 order without folding path identity.
    # Keep a list so duplicate rows cannot disappear during comparison.
    if sorted(recorded_rows, key=lambda row: row["path"].encode("utf-16-be")) != rows:
        return None
    # Legacy records bind the aggregate to their original serialized row order.
    aggregate = hashlib.sha256("".join(
        f"{row['sha256']} {row['size']} {row['path']}\n" for row in recorded_rows
    ).encode("utf-8")).hexdigest()
    if str(record.get("aggregate_sha256") or "").lower() != aggregate:
        return None
    if recorded_rows != rows:
        logging.getLogger(__name__).warning("bootstrap_integrity_inventory_order_differs")
    return aggregate


def _portable_stop_marker_release_preflight(
    paths: CurrentUserOnboardingPaths,
    *,
    environ: Mapping[str, str] | None = None,
    credential_input=None,
) -> dict[str, Any]:
    from label_native_policy import unsigned_native_manifest_valid
    marker = user_relay_stop_path(paths.direct_sync_root)
    if not marker.exists():
        return {"status": "NOT_REQUIRED", "marker_present": False}
    from auto_update import installation_root, resolve_lifecycle_context
    owner_root = paths.app_root
    managed = installation_root(paths.app_root)
    if managed is not None and (managed / "update-control").exists():
        from writer_session_fence import onboarding_lifecycle_context
        context = onboarding_lifecycle_context(paths.app_root)
        if context is None:
            context = resolve_lifecycle_context(managed, operation="register")
        if _resolved(context["active_code_root"]) != paths.app_root:
            raise ValueError("relay resume active code changed during registration")
        owner_root = _resolved(context["fixed_root"])
    owner_paths = replace(paths, app_root=owner_root,
        bootstrap_integrity_path=owner_root / "bootstrap-integrity.json")
    if os.path.normcase(str(owner_root)) != os.path.normcase(
        str(CANONICAL_PORTABLE_ROOT)
    ):
        raise ValueError(
            "relay stop marker is a safety fence until the canonical portable root is installed"
        )
    manifest_path = owner_root / "portable-manifest.json"
    installer_path = owner_root / "INSTALL_CANONICAL_PORTABLE.ps1"
    integrity_path = owner_root / "bootstrap-integrity.json"
    if not all(
        path.is_file() for path in (manifest_path, installer_path, integrity_path)
    ) or manifest_path.stat().st_size > 64 * 1024:
        raise ValueError(
            "relay stop marker cannot be removed before canonical install readback"
        )
    manifest = _read_json(manifest_path, "canonical portable manifest")
    source_commit = str(manifest.get("source_commit") or "").lower()
    source_tree = str(manifest.get("source_tree") or "").lower()
    runtime = owner_root / "runtime" / "pythonw.exe"
    expected_runtime_hash = str(manifest.get("runtime_pythonw_sha256") or "").lower()
    if (
        manifest.get("schema") != "label-match-portable-tree-v1"
        or manifest.get("entrypoint") != "runtime/pythonw.exe app/main.py"
        or len(source_commit) != 40
        or any(character not in "0123456789abcdef" for character in source_commit)
        or len(source_tree) != 40
        or any(character not in "0123456789abcdef" for character in source_tree)
        or not unsigned_native_manifest_valid(owner_root, manifest.get("allowed_unsigned_app_pe"))
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
        (path for path in owner_root.rglob("*") if path.is_file()),
        key=lambda path: path.relative_to(owner_root).as_posix().encode("utf-16-be"),
    ):
        relative = file_path.relative_to(owner_root).as_posix()
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
    aggregate = _verified_bootstrap_inventory_aggregate(integrity, rows)
    if (
        integrity.get("schema_version") != PORTABLE_BOOTSTRAP_INTEGRITY_VERSION
        or integrity.get("status") != "PASS"
        or str(integrity.get("code_root") or "") not in {".", str(owner_root)}
        or integrity.get("file_count") != len(rows)
        or aggregate is None
        or integrity.get("identity_profile_created") is not False
        or integrity.get("state_scope") != "current_user_first_run"
    ):
        raise ValueError("canonical bootstrap integrity readback differs")

    values = os.environ if environ is None else environ
    receipt_value = str(values.get(CONFLICT_RECEIPT_PATH_ENV) or "").strip()
    expected_receipt_hash = str(
        values.get(CONFLICT_RECEIPT_SHA256_ENV) or ""
    ).strip().lower()
    if not receipt_value and not expected_receipt_hash:
        from registration_relay_recovery import recovered_stop_release, _normal_stop
        recovered = (recovered_stop_release(owner_paths, credential_input=credential_input)
                     if credential_input is not None else recovered_stop_release(owner_paths))
        if recovered is not None:
            return recovered
        if paths.removal_report_path.is_file():
            normal = _normal_stop(paths.direct_sync_root, owner_root)
            return {"status": "SUPPORTED_USER_REMOVAL_PROVEN", "marker_present": True,
                "current_stop_marker_request_id": normal["request_id"],
                "current_stop_marker_sha256": normal["sha256"]}
    if not receipt_value or len(expected_receipt_hash) != 64 or any(
        character not in "0123456789abcdef" for character in expected_receipt_hash
    ):
        raise ValueError(
            "relay stop marker requires a pinned EXACT_CLONE_RUNTIME_CONFLICT receipt"
        )
    from label_data_generation import resolve_data_path
    receipt_path = resolve_data_path(receipt_value)
    check_user_data_open(paths.app_root, (receipt_path.parent,), environ=environ)
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
        portable_root=owner_root,
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
    explicit_settings = str(values.get(LABEL_MATCH_SETTINGS_PATH_ENV) or "").strip()
    if not local_app_data and not str(
        values.get(LABEL_MATCH_LOGISTICS_PROFILE_PATH_ENV) or ""
    ).strip():
        for anchor in (explicit_data_root, explicit_settings):
            if anchor:
                assert_path_has_no_reparse_components(
                    anchor, label="current-user profile state root"
                )
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
    elif explicit_data_root:
        settings_path = _resolved(explicit_data_root).parent / "config" / "app_settings.json"
    else:
        raise CurrentUserOnboardingError(
            "LOCALAPPDATA is unavailable for current-user onboarding",
            report_path=selected_app_root / "current-user-onboarding-unavailable.json",
            status="UNKNOWN",
        )

    templates = (
        selected_app_root / "_internal" / "config" / "app_settings.json",
        selected_app_root / "config" / "app_settings.json",
    )
    try:
        data_root = Path(resolve_data_scope(
            environment=values,
            settings_path=settings_path,
            settings_template_path=next((path for path in templates if path.is_file()), None),
            for_onboarding=True,
        ))
    except SingleInstanceError as exc:
        raise CurrentUserOnboardingError(
            str(exc), report_path=selected_app_root / "current-user-onboarding-unavailable.json",
            status="UNKNOWN",
        ) from exc
    # Keep old state anchors independently of the business root.  Source setups
    # without LOCALAPPDATA historically anchor them beside the environment root.
    default_data_root = (
        _resolved(explicit_data_root) if explicit_data_root else
        _resolved(local_app_data) / "KMTech" / "Label_Match" / "data" if local_app_data else
        data_root
    )

    explicit_direct_sync_root = str(
        values.get(LABEL_MATCH_DIRECT_SYNC_ROOT_ENV)
        or values.get(LEGACY_DIRECT_SYNC_ROOT_ENV)
        or ""
    ).strip()
    if explicit_direct_sync_root:
        direct_sync_root = Path(explicit_direct_sync_root)
    elif local_app_data:
        direct_sync_root = (
            Path(local_app_data) / "KMTech" / "DirectSync" / "label_match"
        )
    else:
        direct_sync_root = default_data_root.parent / "direct_sync"
    direct_sync_root = assert_path_has_no_reparse_components(
        direct_sync_root, label="current-user DirectSync root"
    ).resolve(strict=False)

    logistics_profile_path = default_logistics_profile_path(
        values, default_data_root=default_data_root
    )
    bootstrap_tls_ca_bundle_path = (
        _resolved(local_app_data)
        / "KMTech"
        / "Bootstrap"
        / "Label_Match"
        / "ca-bundle.pem"
        if local_app_data
        else default_data_root.parent / "Bootstrap" / "Label_Match" / "ca-bundle.pem"
    )

    for candidate in (
        data_root,
        default_data_root,
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
    ledger_path = data_root / "package_logistics_outbox.sqlite3"
    legacy_ledger = default_data_root / ledger_path.name
    previous_report = status_dir / "current_user_onboarding.json"
    if previous_report.is_file():
        try:
            previous = _read_json(previous_report, "previous onboarding report")
            recorded = Path(str(previous.get("ledger_path") or ""))
            if recorded.is_absolute() and recorded.name == ledger_path.name and recorded.is_file():
                legacy_ledger = _resolved(recorded)
        except ValueError:
            logging.getLogger(__name__).warning("storage_root_previous_report_unreadable")
    if legacy_ledger != ledger_path and legacy_ledger.is_file():
        # Preserve the existing onboarding ledger even when GUI CSV/outbox
        # already live in a custom root.  Never move or merge either store.
        ledger_path = legacy_ledger
        logging.getLogger(__name__).warning(
            "storage_root_split_preserved: data_root=%s onboarding_ledger=%s",
            data_root, ledger_path,
        )
    if _is_legacy_sync_path(ledger_path):
        raise CurrentUserOnboardingError(
            "current-user onboarding state must not use the legacy Sync root",
            report_path=status_dir / "current_user_onboarding.json",
        )
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
        ledger_path=ledger_path,
        bootstrap_integrity_path=selected_app_root / "bootstrap-integrity.json",
    )


def _write_json_atomic(path: Path, payload: Mapping[str, Any]) -> dict[str, str]:
    from failure_redaction import redact_json_for_sink
    serialized = json.dumps(redact_json_for_sink(dict(payload)), ensure_ascii=False,
                            indent=2, sort_keys=True) + "\n"
    return _write_json_bytes_atomic(path, serialized)


def _write_machine_json_atomic(path: Path, payload: Mapping[str, Any]) -> None:
    from failure_redaction import assert_machine_metadata_text
    serialized = json.dumps(dict(payload), ensure_ascii=False, indent=2, sort_keys=True) + "\n"
    assert_machine_metadata_text(serialized)
    _write_json_bytes_atomic(path, serialized)


def _write_json_bytes_atomic(path: Path, serialized: str) -> dict[str, str]:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.{uuid.uuid4().hex}.tmp")
    try:
        with temporary.open("w", encoding="utf-8", newline="\n") as handle:
            handle.write(serialized)
            handle.flush()
            os.fsync(handle.fileno())
        atomic_replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)
    return {"path": str(path), "sha256": hashlib.sha256(serialized.encode("utf-8")).hexdigest()}


def _publish_failure_report(path: Path, report: Mapping[str, Any], error: BaseException) -> None:
    from failure_redaction import protected_failure_details
    error._failure_report_ref = None
    try:
        with writer_admission(ONBOARDING_WRITER_SOURCE):
            error._failure_report_ref = _write_json_atomic(path, {
                **report, **protected_failure_details(error),
            })
    except Exception:
        # A secondary publication failure must not replace the original cause.
        error._failure_report_ref = None


def _emit_installer_failure(error: BaseException, stage: str) -> None:
    from failure_redaction import emit_public_failure
    emit_public_failure(error, stage)


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


def validate_portable_bootstrap_inventory(record, rows, app_root: Path) -> dict[str, Any]:
    """Validate the installer's inventory; callers choose which bytes to observe."""
    aggregate = _verified_bootstrap_inventory_aggregate(record, rows)
    names = {row["path"].casefold() for row in rows}
    portable_layout = {"runtime/pythonw.exe", "app/main.py"} <= names
    code_root = str(record.get("code_root") or "").strip()
    resolved = app_root if code_root == "." else _resolved(code_root)
    if (record.get("schema_version") != PORTABLE_BOOTSTRAP_INTEGRITY_VERSION
            or record.get("status") != "PASS" or resolved != app_root
            or type(record.get("file_count")) is not int or record["file_count"] != len(rows)
            or aggregate is None or record.get("identity_profile_created") is not False
            or record.get("state_scope") != "current_user_first_run"
            or ("label_match.exe" in names) == portable_layout):
        raise ValueError("bootstrap inventory integrity record is invalid")
    return {"status": "PASS", "schema_version": PORTABLE_BOOTSTRAP_INTEGRITY_VERSION,
            "code_root": str(app_root), "file_count": len(rows), "aggregate_sha256": aggregate,
            "package_layout": "portable_cpython" if portable_layout else "onedir"}


_BOOTSTRAP_INTEGRITY_PASS = {}
_WINDOW_FIRST_START = None


def verify_bootstrap_integrity(paths: CurrentUserOnboardingPaths, *, required: bool) -> dict[str, Any]:
    from writer_session_fence import remote_request
    if not required or not paths.bootstrap_integrity_path.exists():
        return _verify_bootstrap_integrity_uncached(paths, required=required)
    def inputs():
        rows = []
        for candidate in sorted(paths.app_root.rglob("*")):
            if _is_reparse_point(candidate):
                raise ValueError("bootstrap code path is redirected")
            if candidate.is_file():
                rows.append((candidate.relative_to(paths.app_root).as_posix(), candidate.read_bytes()))
        return tuple(rows)
    with remote_request():
        observed = inputs()
        key = (paths.app_root.absolute(), paths.bootstrap_integrity_path.absolute())
        cached = _BOOTSTRAP_INTEGRITY_PASS.get(key)
        if cached is not None and cached[0] == observed:
            return dict(cached[1])
        result = _verify_bootstrap_integrity_uncached(paths, required=required)
        if inputs() != observed:
            raise ValueError("bootstrap code changed during verification")
        if result.get("status") == "PASS":
            _BOOTSTRAP_INTEGRITY_PASS[key] = (observed, dict(result))
        return result


def _verify_bootstrap_integrity_uncached(
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
        for candidate in sorted(
            paths.app_root.rglob("*"),
            # Ordering contract: tools/bootstrap_integrity.ps1. Big-endian UTF-16
            # preserves .NET ordinal code-unit order, including non-BMP names.
            key=lambda path: path.relative_to(paths.app_root).as_posix().encode("utf-16-be"),
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
        return {**validate_portable_bootstrap_inventory(record, rows, paths.app_root),
                "record_path": str(paths.bootstrap_integrity_path)}
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


def _server_origin(value: Any) -> tuple[str, str, int] | None:
    if not isinstance(value, str) or not value or any(
        character.isspace() or ord(character) < 32 for character in value
    ):
        return None
    try:
        parsed = urlsplit(value)
        if (parsed.scheme not in {"http", "https"} or not parsed.hostname
                or parsed.username is not None or parsed.password is not None):
            return None
        if not re.fullmatch(r"[a-zA-Z0-9.:-]+", parsed.hostname):
            return None
        port = parsed.port
        if port is not None and port <= 0:
            return None
        return parsed.scheme, parsed.hostname.lower(), port or (443 if parsed.scheme == "https" else 80)
    except ValueError:
        return None


def require_install_server_origin_matches(
    paths: CurrentUserOnboardingPaths, *, credential: Mapping[str, Any] | None = None,
) -> None:
    # The machine marker survives HKCU completion. Never substitute environment
    # overrides or a server-returned logistics profile for either bound origin.
    from auto_update import installation_root
    try:
        managed = installation_root(paths.app_root)
        if managed is None:
            return
        marker = managed / "update-control" / "worker-pending.json"
        assert_path_has_no_reparse_components(marker, label="worker installation marker")
        pending = _read_json(marker, "worker installation marker", maximum_bytes=16384)
        if pending.get("schema") != "label-match-machine-worker-pending-v1":
            return
        installed = pending.get("server_base_url")
        if not _pending_server_origin(installed):
            return
        if credential is None:
            credential = _read_json(paths.credential_path, "relay endpoint")
        installed_origin = _server_origin(installed)
        registered_origin = _server_origin(credential.get("endpoint_url"))
    except (OSError, ValueError, TypeError, AttributeError, LogisticsRuntimeConfigurationError):
        # Missing legacy fields and unreadable inputs add no new refusal.
        return
    if installed_origin is not None and registered_origin is not None and installed_origin != registered_origin:
        raise CurrentUserOnboardingError(
            "설치 주소가 바뀌었습니다. 관리자에게 알려 주세요.",
            report_path=paths.onboarding_report_path, status="RECOVERY_REQUIRED",
            cause_code="INSTALL_SERVER_ORIGIN_CHANGED",
        )


def inspect_current_user_state(
    paths: CurrentUserOnboardingPaths,
    *,
    profile_loader: Callable[[Path], Any] = _default_profile_loader,
    credential_loader: Callable[[Path], Any] = load_credentials_from_json,
    credential_input=None,
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
        if (
            report.get("status") == "ADMIN_RECOVERY_REQUIRED"
            and report.get("recovery_action") == "ADMIN_RECOVERY_REQUIRED"
            and report.get("secret_material_persisted") is False
        ):
            return {"status": "ADMIN_RECOVERY_REQUIRED", "present": present}
    if not all(present.values()):
        return {
            "status": "RECOVERY_REQUIRED",
            "present": present,
            "reason": "current-user onboarding state is partial",
        }
    try:
        identity = _read_json(paths.identity_path, "producer identity")
        manifest = _read_json(paths.producer_manifest_path, "producer manifest")
        if credential_input is not None and credential_input.error is not None:
            raise credential_input.error
        credential = (credential_input.payload if credential_input is not None else
                      _read_json(paths.credential_path, "producer credential"))
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
        resolved_credential = (credential_loader(paths.credential_path, credential_input=credential_input)
            if credential_input is not None and credential_loader is load_credentials_from_json else
            credential_loader(paths.credential_path))
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
        require_install_server_origin_matches(paths, credential=credential)
    except Exception as exc:
        return {
            "status": "RECOVERY_REQUIRED",
            "present": present,
            "reason": str(exc),
            "error_type": exc.__class__.__name__,
            **({"cause_code": exc.cause_code} if isinstance(exc, CurrentUserOnboardingError) else {}),
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
        from label_data_generation import resolve_data_path
        return str(resolve_data_path(explicit))
    if paths.bootstrap_tls_ca_bundle_path.is_file():
        return str(paths.bootstrap_tls_ca_bundle_path)
    return ""


def _registration_runner(
    paths: CurrentUserOnboardingPaths,
    *,
    server_base_url: str,
    environ: Mapping[str, str] | None = None,
    tls_ca_source: str | None = None,
    defer_canonical_data_open: bool = False,
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
    if tls_ca_source is None:
        tls_ca_source = _configured_tls_ca_bundle_source(paths, environ)
    if tls_ca_source:
        arguments.extend(["--tls-ca-bundle-path", tls_ca_source])
    if defer_canonical_data_open:
        return int(register_label_match_worker_pc.main(arguments, defer_canonical_data_open=True))
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
    _write_machine_json_atomic(paths.settings_path, payload)
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
    values[LABEL_MATCH_LOGISTICS_PROFILE_PATH_ENV] = str(paths.logistics_profile_path)
    values[REQUIRED_ENV] = "1"


def prepare_current_user_data_open(
    app_root: str | os.PathLike[str],
    *,
    environ: Mapping[str, str] | None = None,
    installer_admission: bool = False,
    source_inventory_sha256: str | None = None,
):
    """Admit existing onboarding inputs without recording or changing state."""
    admit = check_user_data_open
    if installer_admission:
        from label_data_generation import check_installer_user_data_open
        admit = lambda *args, **kwargs: check_installer_user_data_open(
            *args, source_inventory_sha256=source_inventory_sha256, **kwargs)
    try:
        admit(Path(app_root), environ=environ)
    except DataGenerationError as exc:
        raise CurrentUserOnboardingError(str(exc), report_path=Path(app_root),
            status="UNKNOWN", cause_code="DATA_GENERATION_REFUSED") from exc
    paths = resolve_current_user_onboarding_paths(app_root, environ=environ)
    try:
        admit(paths.app_root, (paths.data_root, paths.ledger_path.parent,
            paths.direct_sync_root, paths.queue_dir, paths.logistics_profile_path.parent), environ=environ)
        tls_ca_source = _configured_tls_ca_bundle_source(paths, environ)
        if tls_ca_source:
            admit(paths.app_root, (Path(tls_ca_source).parent,), environ=environ)
        credential_input = (prepare_credentials_from_json(paths.credential_path,
            app_root=paths.app_root, environ=environ,
            **({"installer_admission": True, "source_inventory_sha256": source_inventory_sha256}
               if installer_admission else {}))
            if paths.credential_path.is_file() else None)
    except DataGenerationError as exc:
        raise CurrentUserOnboardingError(str(exc), report_path=paths.onboarding_report_path,
            status="UNKNOWN", cause_code="DATA_GENERATION_REFUSED") from exc
    return paths, tls_ca_source, credential_input


def _worker_install_completion() -> str:
    import winreg
    try:
        with winreg.OpenKey(winreg.HKEY_CURRENT_USER, r"Software\KMTech\Label_Match", 0, winreg.KEY_QUERY_VALUE) as key:
            value, kind = winreg.QueryValueEx(key, "WorkerInstallCompleted")
    except FileNotFoundError:
        return ""
    if kind != winreg.REG_SZ or not isinstance(value, str):
        raise ValueError("worker installation completion registry type differs")
    return value


_PENDING_SERVER_ORIGIN = re.compile(
    r"https://[A-Za-z0-9](?:[A-Za-z0-9-]{0,61}[A-Za-z0-9])?"
    r"(?:\.[A-Za-z0-9](?:[A-Za-z0-9-]{0,61}[A-Za-z0-9])?)*(?::([1-9][0-9]{0,4}))?",
    re.IGNORECASE | re.ASCII,  # case-insensitive like the installer -match, ASCII only
)


def _pending_server_origin(value: Any) -> bool:
    # Mirrors the installer ServerBaseUrlOrigin result: credential-free https://host[:port].
    match = _PENDING_SERVER_ORIGIN.fullmatch(value) if isinstance(value, str) else None
    return match is not None and (match.group(1) is None or int(match.group(1)) <= 65535)


def validate_pending_worker_install(app_root: str | os.PathLike[str]) -> dict[str, Any] | None:
    """Read the machine marker before any business-data or current-user write."""
    from auto_update import installation_root, current_process_sid
    root = _resolved(app_root)
    managed = installation_root(root)
    if managed is None:
        return None
    path = managed / "update-control" / "worker-pending.json"
    try:
        assert_path_has_no_reparse_components(path, label="worker installation marker")
        try:
            size = path.stat().st_size
        except FileNotFoundError:
            return None
        if not 0 < size <= 65536:
            raise ValueError("worker installation marker size differs")
        with path.open("rb") as stream:
            raw = stream.read(65537)
        if len(raw) > 65536:
            raise ValueError("worker installation marker is oversized")
        pending = json.loads(raw.decode("utf-8-sig"))
        if not isinstance(pending, dict) or pending.get("schema") != "label-match-machine-worker-pending-v1":
            raise ValueError("worker installation marker schema differs")
        if pending.get("target_sid") != current_process_sid():
            raise CurrentUserOnboardingError(
                "설치할 때 지정한 작업자 계정으로 프로그램을 실행하세요.", report_path=path,
                cause_code="PENDING_WORKER_SID_MISMATCH",
            )
        install_id, commit = pending.get("install_id"), pending.get("source_commit")
        if (not isinstance(install_id, str) or not re.fullmatch(r"\d{8}T\d{9}Z-[0-9a-f]{32}", install_id)
                or not isinstance(commit, str) or not re.fullmatch(r"[0-9a-f]{40}", commit)
                or not isinstance(pending.get("inventory_sha256"), str)
                or not re.fullmatch(r"[0-9a-f]{64}", pending["inventory_sha256"])):
            raise ValueError("worker installation marker identity differs")
        # An installer -ServerBaseUrl travels here; a malformed one refuses rather
        # than falling back to the production default.
        if "server_base_url" in pending and not _pending_server_origin(pending["server_base_url"]):
            raise ValueError("worker installation marker server origin differs")
        completion = install_id + ":" + commit
        if _worker_install_completion() == completion:
            return None
        if root.name.casefold() == "app":
            root = root.parent
        active_path = managed / "update-control" / "active.json"
        if active_path.exists():
            assert_path_has_no_reparse_components(active_path, label="worker installation active code")
            active = _read_json(active_path, "worker installation active code")
            if active.get("sha") != commit:
                raise ValueError("worker installation active release differs")
            if root == managed / "current" and (managed / "versions" / commit).is_dir():
                root = managed / "versions" / commit
        assert_path_has_no_reparse_components(root / "portable-manifest.json", label="worker installation manifest")
        manifest = _read_json(root / "portable-manifest.json", "worker installation manifest")
        if manifest.get("source_commit") != commit or manifest.get("version") != pending.get("version"):
            raise ValueError("worker installation marker release differs")
        pending["marker_path"] = path
        pending["marker_sha256"] = hashlib.sha256(raw).hexdigest()
        pending["completion"] = completion
        return pending
    except CurrentUserOnboardingError:
        raise
    except Exception as exc:
        raise CurrentUserOnboardingError(
            "작업자 설치 준비 기록을 확인하지 못했습니다. 관리자에게 설치 기록을 확인해 달라고 요청하세요.",
            report_path=path, cause_code="PENDING_WORKER_INSTALL_INVALID",
        ) from exc


def _complete_worker_install(pending: Mapping[str, Any]) -> None:
    # Called inside the existing onboarding admission, after READY report readback.
    import winreg
    path = Path(pending["marker_path"])
    assert_path_has_no_reparse_components(path, label="worker installation marker")
    with path.open("rb") as stream:
        raw = stream.read(65537)
    if hashlib.sha256(raw).hexdigest() != pending["marker_sha256"]:
        raise ValueError("worker installation changed during onboarding")
    with winreg.CreateKeyEx(winreg.HKEY_CURRENT_USER, r"Software\KMTech\Label_Match", 0,
                           winreg.KEY_SET_VALUE | winreg.KEY_QUERY_VALUE) as key:
        winreg.SetValueEx(key, "WorkerInstallCompleted", 0, winreg.REG_SZ, pending["completion"])
        value, kind = winreg.QueryValueEx(key, "WorkerInstallCompleted")
        if value != pending["completion"] or kind != winreg.REG_SZ:
            raise ValueError("worker installation completion readback differs")


def window_first_release_identity(app_root):
    report = _WINDOW_FIRST_START
    if report is None or _resolved(app_root) != report["paths"].app_root:
        return None
    for path, raw in report["inputs"].items():
        if path.read_bytes() != raw:
            raise DataGenerationError("startup verification input changed")
    return dict(report["identity"])


def try_window_first_start(app_root, *, environ=None, arguments=()):
    """Read steady local evidence, then atomically hold all new DirectSync sends."""
    global _WINDOW_FIRST_START
    from auto_update import (control_root, load_policy, read_json, ordinary_user_context,
                             prepared_pending, _sha, recovery_sha)
    from startup_send_gate import read_startup_send_hold
    from user_relay import _existing_user_relay_owner, _reuse_existing_user_relay
    from label_data_generation import portable_release_identity
    from writer_session_fence import WriterFenceError
    if arguments:
        return None
    try:
        root = _resolved(app_root)
        if root.name.casefold() == "app":
            root = root.parent
        control = control_root(root / "app")
        if control is None or root.parent != control.parent / "versions":
            return None
        policy = load_policy(control)
        if not ordinary_user_context(policy["operator_sid"]):
            return None
        active, state = read_json(control / "active.json"), read_json(control / "transition.json")
        if (active.get("schema") != "label-match-startup-update-v1"
                or state.get("schema") != "label-match-startup-update-v1"
                or state.get("phase") not in {"CONFIRMED", "ROLLED_BACK", "DEFERRED"}
                or (control / "apply-claim.json").exists()
                or prepared_pending(control, policy, state)):
            return None
        manifest = _read_json(root / "portable-manifest.json", "startup manifest")
        identity = portable_release_identity(manifest)
        if (manifest.get("startup_update_protocol") != 1 or identity["build_id"] != _sha(active["sha"])
                or root.name != active["sha"]
                or (state.get("candidate_sha") is not None and recovery_sha(state, pointer_sha=active["sha"]) != active["sha"])):
            return None
        paths = resolve_current_user_onboarding_paths(root, environ=environ)
        if (user_relay_stop_path(paths.direct_sync_root).exists()
                or read_startup_send_hold(paths.direct_sync_root) is not None):
            return None
        previous = _read_json(paths.onboarding_report_path, "onboarding report")
        if previous.get("status") != "READY" or previous.get("state_scope") != "current_user":
            return None
        for field in ("data_root", "direct_sync_root", "ledger_path", "settings_path", "logistics_profile_path"):
            if _resolved(previous.get(field) or "") != getattr(paths, field):
                return None
        state_paths = (paths.identity_path, paths.producer_manifest_path, paths.credential_path,
                       paths.registration_report_path, paths.logistics_profile_path, paths.logistics_secret_path)
        if not all(path.is_file() for path in (*state_paths, paths.ledger_path, paths.settings_path)):
            return None
        registration = _read_json(paths.registration_report_path, "registration report")
        if registration.get("server_registration_verified") is not True:
            return None
        registered_origin = registration.get("server_base_url")
        if registered_origin:
            profile = _read_json(paths.logistics_profile_path, "logistics profile")
            if str(profile.get("base_url") or "").rstrip("/") != str(registered_origin).rstrip("/"):
                return None
        observed = {}
        for path in (*state_paths, root / "portable-manifest.json", *(control / name for name in ("policy.json", "active.json", "transition.json"))):
            assert_path_has_no_reparse_components(path, label="startup verification input")
            if path.stat().st_size > BOOTSTRAP_INTEGRITY_MAX_BYTES:
                return None
            observed[path] = path.read_bytes()
        owner = _existing_user_relay_owner(root, paths.direct_sync_root, is_bootstrap=True,
                                          allow_fast_path=True, fast_only=True)
        if owner is None or owner.get("fast_path") is not True:
            return None
        relay = _reuse_existing_user_relay(lambda: owner, 0, lambda _: None)
        report = {"status": "READY", "start_path": "FAST_WINDOW_FIRST", "app_root": root,
                  "paths": paths, "inputs": observed, "identity": identity,
                  "direct_sync_root": str(paths.direct_sync_root), "report_path": paths.onboarding_report_path,
                  "relay_start": relay, "existing_relay_reused": True, "fast_path": True,
                  "hold": None, "server_base_url": registered_origin or DEFAULT_SERVER_BASE_URL}
        import threading
        report["window_visible"] = threading.Event()
        _WINDOW_FIRST_START = report
        apply_current_user_runtime_environment(paths, environ=environ)
        return report
    except (OSError, ValueError, TypeError, KeyError, CurrentUserOnboardingError, DataGenerationError, WriterFenceError):
        return None


def admit_window_first_start(report):
    """Only the single-instance owner may pause this user's new sends."""
    from startup_send_gate import create_startup_send_hold
    window_first_release_identity(report["app_root"])
    hold = create_startup_send_hold(report["paths"].direct_sync_root)
    if hold is None:
        raise CurrentUserOnboardingError("서버 보내기 확인 중입니다. 잠시 뒤 다시 시도하세요.",
            report_path=report["report_path"], cause_code="STARTUP_VERIFICATION_PENDING")
    report["hold"] = hold


def complete_window_first_verification(report):
    global _WINDOW_FIRST_START
    from startup_send_gate import release_startup_send_hold
    from user_relay import verify_reused_user_relay
    try:
        window_first_release_identity(report["paths"].app_root)
        result = onboard_current_user(report["app_root"], server_base_url=report["server_base_url"],
                                      start_path="FAST_WINDOW_FIRST", relay_launcher=lambda _root: dict(report["relay_start"]))
        if result.get("status") != "READY":
            raise ValueError("startup verification did not finish")
        verify_reused_user_relay(report["app_root"], report["relay_start"])
        window_first_release_identity(report["paths"].app_root)
        if not release_startup_send_hold(report["paths"].direct_sync_root, report["hold"], verified=True):
            raise ValueError("startup verification hold changed")
        _WINDOW_FIRST_START = None
    except CurrentUserOnboardingError:
        raise
    except Exception as error:
        failure = CurrentUserOnboardingError("서버 보내기 멈춤 — 관리자에게 알려 주세요",
            report_path=report["report_path"], cause_code="STARTUP_VERIFICATION_FAILED")
        raise failure from error


@writer_sink("current_user_onboarding", probe_only=True)
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
    scheduled_task_remover: Callable[
        [str | os.PathLike[str]], Mapping[str, Any]
    ] = remove_current_user_scheduled_task,
    legacy_task_quiescence_reader: Callable[
        [], Mapping[str, Any]
    ] = read_legacy_system_task_quiescence,
    relay_launcher: Callable[
        [str | os.PathLike[str]], Mapping[str, Any]
    ] = reuse_or_start_user_relay_process,
    defer_canonical_relay_start: bool = False,
    defer_machine_worker_relay_start: bool = False,
    _startup: bool = False,
    start_path: str = "FULL",
) -> dict[str, Any]:
    from auto_update import installation_root, assert_registration_storage
    from writer_session_fence import onboarding_lifecycle_context
    pending_worker = validate_pending_worker_install(app_root)
    if defer_machine_worker_relay_start:
        from auto_update import process_security
        security = process_security(os.getpid())
        if (pending_worker is None or security.get("elevated") is not True
                or security.get("sid") != pending_worker["target_sid"]):
            raise ValueError("deferred worker relay requires its elevated pending worker")
    managed = installation_root(Path(app_root))
    setup_root = Path(app_root)
    if managed is not None and (managed / "update-control").exists():
        context = onboarding_lifecycle_context(app_root)
        if context is None:
            raise ValueError("onboarding lifecycle observation is absent")
        app_root = context["active_code_root"]
        setup_root = Path(context["fixed_root"])
    if defer_canonical_relay_start:
        from writer_session_fence import active_fence, canonical_control_root, _delegation_matches
        values = os.environ if environ is None else environ
        fence = active_fence(canonical_control_root(values), environ=values)
        if fence is None or not _delegation_matches(fence, source="current_user_onboarding", environ=values):
            raise ValueError("deferred data open and relay start require exact canonical installer ownership")
    paths, tls_ca_source, credential_input = prepare_current_user_data_open(app_root, environ=environ)
    assert_registration_storage(Path(app_root), (paths.data_root, paths.settings_path,
        paths.direct_sync_root, paths.logistics_profile_path, paths.ledger_path))
    with writer_admission(ONBOARDING_WRITER_SOURCE):
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
        requires_bootstrap_integrity(paths.app_root)
        if require_bootstrap_integrity is None
        else bool(require_bootstrap_integrity)
    )
    report: dict[str, Any] = {
        "report_version": ONBOARDING_REPORT_VERSION,
        "start_path": start_path,
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
        "storage_root_compatibility": (
            "SPLIT_PRESERVED" if paths.ledger_path.parent != paths.data_root else "UNIFIED"
        ),
        "tls_ca_bundle_source_configured": bool(tls_ca_source),
        "server_registration_verified": False,
        "failure": "",
    }
    stop_marker_released = False
    business_open_in_progress = False

    def require_registration_recovery(state: Mapping[str, Any]) -> None:
        if state.get("cause_code") == "INSTALL_SERVER_ORIGIN_CHANGED":
            raise CurrentUserOnboardingError(
                str(state["reason"]), report_path=paths.onboarding_report_path,
                status="RECOVERY_REQUIRED", cause_code="INSTALL_SERVER_ORIGIN_CHANGED",
            )
        if state.get("status") != "ADMIN_RECOVERY_REQUIRED":
            return
        report["action"] = "ADMIN_RECOVERY_REQUIRED"
        report["recovery_action"] = "ADMIN_RECOVERY_REQUIRED"
        raise CurrentUserOnboardingError(
            "이 PC의 기존 등록을 복구해야 합니다. 관리자는 설치된 등록 도구에서 "
            "일반 등록 토큰과 별도의 관리자 복구 승인을 사용한 뒤 초기 설정을 다시 실행하세요.",
            report_path=paths.onboarding_report_path,
            status="RECOVERY_REQUIRED",
        )

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
        try:
            report["bootstrap_integrity"] = verify_bootstrap_integrity(
                paths,
                required=require_integrity,
            )
        except ValueError as exc:
            raise CurrentUserOnboardingError(
                str(exc), report_path=paths.onboarding_report_path,
                cause_code="BOOTSTRAP_INTEGRITY_INVALID",
            ) from exc
        state = inspect_current_user_state(
            paths,
            profile_loader=profile_loader,
            credential_loader=credential_loader, credential_input=credential_input,
        )
        report["initial_state"] = state
        if state["status"] in {"ABSENT", "ABSENT_RETRYABLE"}:
            report["start_path"] = "FIRST_RUN"
        require_registration_recovery(state)
        if state["status"] == "READY" and tls_ca_source:
            from tools.install_logistics_runtime_profile import (
                install_tls_ca_bundle_for_existing_profile,
            )

            with writer_admission(ONBOARDING_WRITER_SOURCE):
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
                credential_loader=credential_loader, credential_input=credential_input,
            )
            require_registration_recovery(state)
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
                    credential_loader=credential_loader, credential_input=credential_input,
                )
                require_registration_recovery(state)
                if state["status"] == "RECOVERY_REQUIRED":
                    raise ValueError(
                        str(state.get("reason") or "partial current-user state")
                    )
                if state["status"] in {"ABSENT", "ABSENT_RETRYABLE"}:
                    with writer_admission(ONBOARDING_WRITER_SOURCE):
                        if registration_runner is None:
                            return_code = _registration_runner(
                                paths,
                                server_base_url=server_base_url,
                                environ=environ,
                                tls_ca_source=tls_ca_source,
                                defer_canonical_data_open=defer_canonical_relay_start,
                            )
                        else:
                            return_code = registration_runner(paths)
                    # Registration can create/replace this locator; admit that new
                    # input once before its readback and keep it for later loaders.
                    credential_input = (prepare_credentials_from_json(paths.credential_path,
                        app_root=paths.app_root, environ=environ) if paths.credential_path.is_file() else None)
                    if type(return_code) is not int:
                        raise CurrentUserOnboardingError(
                            "registration result is UNKNOWN because no exit code was returned",
                            report_path=paths.onboarding_report_path,
                            status="UNKNOWN",
                        )
                    if return_code != 0:
                        require_registration_recovery(inspect_current_user_state(
                            paths, profile_loader=profile_loader,
                            credential_loader=credential_loader, credential_input=credential_input,
                        ))
                        if paths.registration_report_path.is_file():
                            registration_failure = _read_json(
                                paths.registration_report_path, "registration failure report"
                            )
                            if (
                                registration_failure.get("report_version")
                                == "label-match-worker-pc-registration-v1"
                                and registration_failure.get("status") == "BLOCKED"
                                and registration_failure.get("failure_category")
                                == "NETWORK_OR_SERVER_UNAVAILABLE"
                            ):
                                raise CurrentUserOnboardingError(
                                    "current-user registration network or server is unavailable",
                                    report_path=paths.onboarding_report_path,
                                    cause_code="NETWORK_OR_SERVER_UNAVAILABLE",
                                )
                        raise ValueError(
                            f"current-user registration failed with exit code {return_code}"
                        )
                    state = inspect_current_user_state(
                        paths,
                        profile_loader=profile_loader,
                        credential_loader=credential_loader, credential_input=credential_input,
                    )
                    require_registration_recovery(state)
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

        # Keep the read-only downgrade preflight above. Canonical preparation
        # waits until T1; normal startup records READY before business data open.
        if defer_canonical_relay_start:
            # Canonical preparation is still reversible. The normal startup
            # after T1 performs these idempotent opens under its writer guard.
            report["ledger_status"] = "DEFERRED_CANONICAL_INSTALL"
            report["settings"] = {"status": "DEFERRED_CANONICAL_INSTALL"}
        else:
            try:
                with writer_admission(ONBOARDING_WRITER_SOURCE):
                    record_user_data_open(paths.app_root, (paths.data_root, paths.ledger_path.parent,
                                                           paths.direct_sync_root, paths.queue_dir,
                                                           paths.logistics_profile_path.parent))
            except DataGenerationError as exc:
                raise CurrentUserOnboardingError(str(exc), report_path=paths.onboarding_report_path,
                    status="UNKNOWN", cause_code="DATA_GENERATION_REFUSED") from exc
            business_open_in_progress = True
            with writer_admission(ONBOARDING_WRITER_SOURCE):
                ledger_factory(paths.ledger_path)
            if not paths.ledger_path.is_file():
                raise ValueError("current-user business ledger readback failed")
            report["ledger_status"] = "READY"
            with writer_admission(ONBOARDING_WRITER_SOURCE):
                report["settings"] = dict(settings_factory(paths))
            settings_status = str(report["settings"].get("status") or "")
            if settings_status not in {"CREATED", "REUSED"}:
                raise ValueError("current-user settings placement was not proven")
            business_open_in_progress = False

        stop_path = user_relay_stop_path(paths.direct_sync_root)
        report['relay_resume_required'] = stop_path.exists()
        report["stop_marker_release"] = _portable_stop_marker_release_preflight(
            paths,
            environ=environ,
            credential_input=credential_input,
        )
        with writer_admission(ONBOARDING_WRITER_SOURCE):
            report["relay_autostart"] = dict(autostart_installer(setup_root))
        autostart_status = str(report["relay_autostart"].get("status") or "")
        if autostart_status in {"", "UNKNOWN"}:
            raise CurrentUserOnboardingError(
                "current-user relay autostart result is UNKNOWN",
                report_path=paths.onboarding_report_path,
                status="UNKNOWN",
            )
        if autostart_status != "PASS":
            raise ValueError("current-user relay autostart was not proven")
        with writer_admission(remove_current_user_scheduled_task.__label_writer_sink__):
            report["scheduled_task_migration"] = dict(scheduled_task_remover(setup_root))
        migration_status = str(report["scheduled_task_migration"].get("status") or "")
        if migration_status in {"", "UNKNOWN"}:
            raise CurrentUserOnboardingError(
                "historical scheduled-task retirement result is UNKNOWN",
                report_path=paths.onboarding_report_path,
                status="UNKNOWN",
            )
        if migration_status not in {"ABSENT", "DISABLED_RETAINED_OFFICIAL"}:
            raise ValueError("historical current-user scheduled task retirement was not proven")
        # The marker is a safety fence.  It is released only after canonical
        # install ownership, HKCU, and historical task absence all read back.
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
        if defer_canonical_relay_start or defer_machine_worker_relay_start:
            report["relay_start"] = {"status": "DEFERRED_WORKER_SESSION" if defer_machine_worker_relay_start else "DEFERRED_CANONICAL_INSTALL"}
        else:
            from startup_send_gate import read_startup_send_hold
            if relay_launcher is reuse_or_start_user_relay_process and (
                (_startup and start_path != "FAST_WINDOW_FIRST")
                or read_startup_send_hold(paths.direct_sync_root) is not None
            ):
                report["relay_start"] = dict(relay_launcher(paths.app_root, allow_recent_reuse=False))
            else:
                report["relay_start"] = dict(relay_launcher(paths.app_root))
        relay_start_status = str(report["relay_start"].get("status") or "")
        report["existing_relay_reused"] = report["relay_start"].get("existing_relay_reused") is True
        report["fast_path"] = report["relay_start"].get("fast_path") is True
        if relay_start_status in {"", "UNKNOWN"}:
            raise CurrentUserOnboardingError(
                "current-user relay launch result is UNKNOWN",
                report_path=paths.onboarding_report_path,
                status="UNKNOWN",
            )
        expected_relay_status = ("DEFERRED_WORKER_SESSION" if defer_machine_worker_relay_start else
                                 "DEFERRED_CANONICAL_INSTALL" if defer_canonical_relay_start else "ALIVE")
        if relay_start_status != expected_relay_status:
            raise ValueError("current-user relay survival was not proven")
        # Helpers, child launch and survival ran without admission. Reacquire
        # and recheck the fence before applying the environment or READY.
        with writer_admission(ONBOARDING_WRITER_SOURCE):
            apply_current_user_runtime_environment(paths, environ=environ)
            report.update(
                {
                    "status": "WORKER_SESSION_PENDING" if defer_machine_worker_relay_start else "READY",
                    "state_readback": state,
                    "server_registration_verified": True,
                    "operation_lease_store": "AUTHORITATIVE_SNAPSHOT_PRESERVED",
                    "persistent_relay_principal": "current_user",
                    "system_scheduled_task_required": False,
                    "current_user_scheduled_task_required": False,
                    "completed_at": _now(),
                }
            )
            _write_json_atomic(paths.onboarding_report_path, report)
            if pending_worker is not None and not (defer_machine_worker_relay_start or defer_canonical_relay_start):
                _complete_worker_install(pending_worker)
        return report
    except EnrollmentMutexError as exc:
        restore_stop_marker_fence()
        report["status"] = exc.report_status
        report["failure"] = exc.reason_code
        report["error_type"] = exc.__class__.__name__
        report["recovery_action"] = exc.recovery_action
        report["enrollment_mutex"] = dict(exc.mutex_report)
        failure = CurrentUserOnboardingError(
            f"Label_Match enrollment mutex blocked onboarding: {exc.reason_code}",
            report_path=paths.onboarding_report_path,
            status=exc.report_status,
        )
        failure.__cause__ = exc
        _publish_failure_report(paths.onboarding_report_path, report, failure)
        raise failure from exc
    except CurrentUserOnboardingError as exc:
        restore_stop_marker_fence()
        if report.get('relay_resume_required'):
            exc.cause_code = 'RELAY_RESUME_REQUIRED'
        report["status"] = exc.status
        report["failure"] = str(exc)
        report["error_type"] = exc.__class__.__name__
        report["cause_code"] = exc.cause_code
        _publish_failure_report(paths.onboarding_report_path, report, exc)
        raise
    except Exception as exc:
        restore_stop_marker_fence()
        report["status"] = "FAILED"
        report["failure"] = str(exc)[:500]
        report["error_type"] = exc.__class__.__name__
        from auto_update import launch_failure_details
        report["launch_failure_details"] = launch_failure_details(exc)
        from requests.exceptions import ConnectionError as RequestsConnectionError, Timeout
        cause_code = (
            'BUSINESS_DATA_OPEN_FAILED' if business_open_in_progress and managed is not None else
            'RELAY_RESUME_REQUIRED' if report.get('relay_resume_required') else
            "NETWORK_OR_SERVER_UNAVAILABLE"
            if isinstance(exc, (ConnectionError, TimeoutError, RequestsConnectionError, Timeout))
            else "SETUP_FAILED"
        )
        report["cause_code"] = cause_code
        recovery_command = ""
        if cause_code == "BUSINESS_DATA_OPEN_FAILED":
            def quoted(path):
                return "'" + str(path).replace("'", "''") + "'"
            recovery_command = ("& " + quoted(setup_root / "runtime" / "python.exe") +
                " -I -B " + quoted(setup_root / "app" / "main.py") +
                " --onboard-current-user --app-root " + quoted(setup_root))
            report["recovery_command"] = recovery_command
            report["recovery_guidance"] = (
                "새 판과 자료를 보존했습니다. 원인을 해결한 뒤 같은 작업 사용자의 "
                "PowerShell에서 recovery_command로 사용자 준비를 재시도하고 앱을 다시 시작하세요.")
        failure = CurrentUserOnboardingError(
            f"Label_Match first-run onboarding failed: {exc}",
            report_path=paths.onboarding_report_path,
            status="FAILED",
            cause_code=cause_code,
            recovery_command=recovery_command,
        )
        failure.__cause__ = exc
        _publish_failure_report(paths.onboarding_report_path, report, failure)
        raise failure from exc


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
    from auto_update import installation_root
    managed = installation_root(Path(app_root))
    if managed is not None:
        # User removal must work even when machine control cannot be decoded.
        # Its report and persistence belong to the stable owner, not active B.
        app_root = managed / "current"
    from label_data_generation import _authenticated_support_removal_source
    paths = None
    try:
        with _authenticated_support_removal_source(Path(app_root)) as support_check:
            admit = support_check or check_user_data_open
            admit(Path(app_root), environ=environ)
            paths = resolve_current_user_onboarding_paths(app_root, environ=environ)
            admit(paths.app_root, (paths.data_root, paths.ledger_path.parent,
                paths.direct_sync_root, paths.queue_dir, paths.logistics_profile_path.parent), environ=environ)
    except DataGenerationError as exc:
        raise CurrentUserOnboardingError(str(exc),
            report_path=paths.removal_report_path if paths is not None else Path(app_root),
            status="UNKNOWN", cause_code="DATA_GENERATION_REFUSED") from exc
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
        failures = []
        first_failure = None
        for step, key, callback, arguments in (
            ("run", "relay_autostart", autostart_remover, ()),
            ("relay", "relay_process", relay_stopper, (paths.direct_sync_root,)),
            ("task", "scheduled_task", scheduled_task_remover, (paths.app_root,)),
        ):
            if step == "task" and report.get("relay_process", {}).get("status") != "ABSENT":
                report[key] = {"status": "NOT_TESTED_RELAY_UNPROVEN"}
                continue
            try:
                report[key] = dict(callback(*arguments))
                status = str(report[key].get("status") or "")
                if status in {"", "UNKNOWN"}:
                    raise CurrentUserOnboardingError(
                        f"current-user {step} removal result is UNKNOWN",
                        report_path=paths.removal_report_path, status="UNKNOWN",
                    )
                if status != "ABSENT":
                    raise ValueError(f"current-user {step} absence was not proven")
            except Exception as exc:
                from failure_redaction import protected_failure_details
                if first_failure is None:
                    first_failure = exc
                failures.append({"step": step, "error_type": type(exc).__name__,
                                 "failure": str(exc)[:500],
                                 "status": getattr(exc, "status", "FAILED"),
                                 **protected_failure_details(exc, environ)})
        if failures:
            report["removal_failures"] = failures
            status = "UNKNOWN" if any(item["status"] == "UNKNOWN" for item in failures) else "FAILED"
            raise CurrentUserOnboardingError(
                "current-user removal remains incomplete: " + "; ".join(
                    item["step"] + ": " + item["failure"] for item in failures),
                report_path=paths.removal_report_path, status=status,
            ) from first_failure
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
        _publish_failure_report(paths.removal_report_path, report, exc)
        raise
    except Exception as exc:
        report.update(
            {
                "status": "FAILED",
                "failure": str(exc)[:500],
                "error_type": exc.__class__.__name__,
            }
        )
        failure = CurrentUserOnboardingError(
            f"Label_Match current-user removal failed: {exc}",
            report_path=paths.removal_report_path,
        )
        failure.__cause__ = exc
        _publish_failure_report(paths.removal_report_path, report, failure)
        raise failure from exc


def _default_app_root() -> Path:
    if getattr(sys, "frozen", False):
        return Path(sys.executable).resolve().parent
    return _default_product_root()


def onboarding_main(argv: list[str] | None = None) -> int:
    parser = __import__("failure_redaction").InstallerArgumentParser(
        description="Onboard Label_Match for the current user"
    )
    parser.add_argument("--app-root", default=str(_default_app_root()))
    parser.add_argument("--server-base-url", default=DEFAULT_SERVER_BASE_URL)
    parser.add_argument("--check-data-generation-only", action="store_true")
    parser.add_argument("--installer-source-inventory-sha256", default=None, help=__import__('argparse').SUPPRESS)
    parser.add_argument("--defer-canonical-relay-start", action="store_true")
    parser.add_argument("--defer-machine-worker-relay-start", action="store_true")
    args = parser.parse_args(argv)
    if args.installer_source_inventory_sha256 is not None and not args.check_data_generation_only:
        parser.error("source inventory is only accepted for read-only installation admission")
    try:
        if args.check_data_generation_only:
            prepare_current_user_data_open(args.app_root, installer_admission=True,
                source_inventory_sha256=args.installer_source_inventory_sha256)
            print("onboarding_data_generation_status=PASS")
            return 0
        report = onboard_current_user(
            args.app_root,
            defer_canonical_relay_start=args.defer_canonical_relay_start,
            defer_machine_worker_relay_start=args.defer_machine_worker_relay_start,
            server_base_url=args.server_base_url,
            require_bootstrap_integrity=requires_bootstrap_integrity(args.app_root),
        )
    except CurrentUserOnboardingError as exc:
        print(f"onboarding_status={exc.status}")
        print(f"onboarding_report={exc.report_path}")
        _emit_installer_failure(exc, "CURRENT_USER_ONBOARDING")
        return ONBOARDING_EXIT_CODE
    except (RuntimeError, ValueError, OSError) as exc:
        # Lifecycle Verify precedes current-user path/TLS preparation. Preserve
        # its child reference without claiming enrollment or exposing a traceback.
        _emit_installer_failure(exc, "CURRENT_USER_ONBOARDING")
        return ONBOARDING_EXIT_CODE
    print(f"onboarding_status={report['status']}")
    print(f"onboarding_action={report['action']}")
    print(
        "onboarding_report="
        f"{resolve_current_user_onboarding_paths(args.app_root).onboarding_report_path}"
    )
    return 0


def removal_main(argv: list[str] | None = None) -> int:
    parser = __import__("failure_redaction").InstallerArgumentParser(
        description="Remove Label_Match current-user setup"
    )
    parser.add_argument("--app-root", default=str(_default_app_root()))
    args = parser.parse_args(argv)
    try:
        report = remove_current_user_setup(args.app_root)
    except CurrentUserOnboardingError as exc:
        print(f"current_user_removal_status={exc.status}")
        print(f"current_user_removal_report={exc.report_path}")
        _emit_installer_failure(exc, "CURRENT_USER_REMOVAL")
        return ONBOARDING_EXIT_CODE
    paths = resolve_current_user_onboarding_paths(args.app_root)
    print(f"current_user_removal_status={report['status']}")
    print("data_preserved=true")
    print(f"current_user_removal_report={paths.removal_report_path}")
    print("machine_code_removal_command=INSTALL_THIS_PC.ps1 -Uninstall")
    return 0


if __name__ == "__main__":
    raise SystemExit(onboarding_main())
