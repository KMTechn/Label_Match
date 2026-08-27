"""First-run current-user state onboarding for Label_Match."""

from __future__ import annotations

import argparse
from dataclasses import dataclass
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import sys
from typing import Any, Callable, Mapping, MutableMapping
import uuid

from direct_sync_push import manifest_hash
from direct_sync_runtime import load_credentials_from_json
from logistics_runtime_profile import (
    PROFILE_PATH_ENV,
    REQUIRED_ENV,
    load_logistics_runtime_profile,
    unprotect_current_user_secret,
)
from user_relay import (
    install_user_relay_autostart,
    remove_user_relay_autostart,
    request_user_relay_stop,
    start_user_relay_process,
    user_relay_stop_path,
)


DEFAULT_SERVER_BASE_URL = "https://worker.kmtecherp.com"
ONBOARDING_REPORT_VERSION = "label-match-current-user-onboarding-v1"
REMOVAL_REPORT_VERSION = "label-match-current-user-removal-v1"
BOOTSTRAP_INTEGRITY_VERSION = "label-match-bootstrap-integrity-v1"
ONBOARDING_EXIT_CODE = 4
LABEL_MATCH_DATA_ROOT_ENV = "LABEL_MATCH_SAVE_DIR"
LABEL_MATCH_SETTINGS_PATH_ENV = "LABEL_MATCH_SETTINGS_PATH"
LABEL_MATCH_DIRECT_SYNC_ROOT_ENV = "LABEL_MATCH_DIRECT_SYNC_ROOT"
LEGACY_DIRECT_SYNC_ROOT_ENV = "LABEL_MATCH_DIRECT_SYNC_PROGRAM_DATA_ROOT"


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
            direct_sync_root
            / "evidence"
            / "producer_self_enrollment_receipt.json"
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
        ledger_path=data_root / "package_logistics_outbox.sqlite3",
        bootstrap_integrity_path=selected_app_root / "bootstrap-integrity.json",
    )


def _write_json_atomic(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.{uuid.uuid4().hex}.tmp")
    try:
        with temporary.open("w", encoding="utf-8", newline="\n") as handle:
            json.dump(dict(payload), handle, ensure_ascii=False, indent=2, sort_keys=True)
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def _read_json(path: Path, purpose: str) -> dict[str, Any]:
    try:
        size = path.stat().st_size
    except OSError as exc:
        raise ValueError(f"{purpose} is absent") from exc
    if size <= 0 or size > 1024 * 1024:
        raise ValueError(f"{purpose} size is invalid")
    try:
        value = json.loads(path.read_text(encoding="utf-8-sig"))
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
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


def verify_bootstrap_integrity(
    paths: CurrentUserOnboardingPaths,
    *,
    required: bool,
) -> dict[str, Any]:
    if not required:
        return {"status": "NOT_TESTED", "reason": "source-mode onboarding"}
    record = _read_json(paths.bootstrap_integrity_path, "bootstrap integrity record")
    if record.get("schema_version") != BOOTSTRAP_INTEGRITY_VERSION:
        raise ValueError("bootstrap integrity record schema is invalid")
    if record.get("status") != "PASS":
        raise ValueError("bootstrap integrity record is not PASS")
    if record.get("package_layout") != "onedir":
        raise ValueError("bootstrap integrity record lost the onedir package layout")
    if _resolved(str(record.get("code_root") or "")) != paths.app_root:
        raise ValueError("bootstrap integrity record code root is invalid")
    files = record.get("files")
    if (
        not isinstance(files, list)
        or not files
        or record.get("file_count") != len(files)
    ):
        raise ValueError("bootstrap integrity record file inventory is invalid")
    normalized: list[tuple[str, int, str]] = []
    declared_paths: set[str] = set()
    for item in files:
        if not isinstance(item, Mapping):
            raise ValueError("bootstrap integrity inventory entry is invalid")
        relative_text = str(item.get("path") or "").replace("\\", "/")
        parts = relative_text.split("/")
        if (
            not relative_text
            or relative_text.startswith("/")
            or any(part in {"", ".", ".."} for part in parts)
            or ":" in parts[0]
        ):
            raise ValueError("bootstrap integrity inventory path is unsafe")
        folded = relative_text.casefold()
        if folded in declared_paths:
            raise ValueError("bootstrap integrity inventory path is duplicated")
        declared_paths.add(folded)
        try:
            expected_size = int(item.get("size"))
        except (TypeError, ValueError) as exc:
            raise ValueError("bootstrap integrity inventory size is invalid") from exc
        expected_hash = str(item.get("sha256") or "").strip().lower()
        if expected_size < 0 or len(expected_hash) != 64:
            raise ValueError("bootstrap integrity inventory metadata is invalid")
        target = paths.app_root.joinpath(*parts)
        if target.is_symlink() or not target.is_file():
            raise ValueError(
                f"bootstrap code file is absent or redirected: {relative_text}"
            )
        if target.stat().st_size != expected_size or _file_sha256(target) != expected_hash:
            raise ValueError(f"bootstrap code file integrity failed: {relative_text}")
        normalized.append((expected_hash, expected_size, relative_text))
    actual_paths = set()
    for candidate in paths.app_root.rglob("*"):
        if not candidate.is_file():
            continue
        relative = candidate.relative_to(paths.app_root).as_posix().casefold()
        if relative == paths.bootstrap_integrity_path.name.casefold():
            continue
        actual_paths.add(relative)
    if actual_paths != declared_paths:
        raise ValueError("bootstrap code inventory exact readback failed")
    aggregate_payload = "".join(
        f"{sha256} {size} {relative_path}\n"
        for sha256, size, relative_path in normalized
    ).encode("utf-8")
    aggregate = hashlib.sha256(aggregate_payload).hexdigest()
    if aggregate != str(record.get("aggregate_sha256") or "").strip().lower():
        raise ValueError("bootstrap integrity aggregate is invalid")
    main_entries = [
        item
        for item in files
        if isinstance(item, Mapping)
        and str(item.get("path") or "").replace("\\", "/").casefold()
        == "label_match.exe"
    ]
    if len(main_entries) != 1:
        raise ValueError("bootstrap integrity record does not identify Label_Match.exe")
    if not (paths.app_root / "Label_Match.exe").is_file():
        raise ValueError("hardened Label_Match executable is absent")
    if not (paths.app_root / "_internal").is_dir():
        raise ValueError("hardened Label_Match onedir runtime is absent")
    return {
        "status": "PASS",
        "record_path": str(paths.bootstrap_integrity_path),
        "code_root": str(paths.app_root),
        "file_count": len(files),
        "aggregate_sha256": aggregate,
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
        if str(getattr(resolved_profile, "authority_plane", "")).upper() != "AUTHORITATIVE":
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
    }


def _registration_runner(
    paths: CurrentUserOnboardingPaths,
    *,
    server_base_url: str,
) -> int:
    from tools import register_label_match_worker_pc

    return int(
        register_label_match_worker_pc.main(
            [
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
        )
    )


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
    settings_factory: Callable[[CurrentUserOnboardingPaths], Mapping[str, Any]] = _ensure_user_settings,
    autostart_installer: Callable[[str | os.PathLike[str]], Mapping[str, Any]] = install_user_relay_autostart,
    relay_launcher: Callable[[str | os.PathLike[str]], Mapping[str, Any]] = start_user_relay_process,
) -> dict[str, Any]:
    paths = resolve_current_user_onboarding_paths(app_root, environ=environ)
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
        "server_registration_verified": False,
        "failure": "",
    }
    try:
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
        if state["status"] == "RECOVERY_REQUIRED":
            raise ValueError(str(state.get("reason") or "partial current-user state"))
        if state["status"] in {"ABSENT", "ABSENT_RETRYABLE"}:
            if registration_runner is None:
                return_code = _registration_runner(
                    paths,
                    server_base_url=server_base_url,
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
        else:
            report["action"] = "REUSED"

        ledger_factory(paths.ledger_path)
        if not paths.ledger_path.is_file():
            raise ValueError("current-user business ledger readback failed")
        report["settings"] = dict(settings_factory(paths))
        settings_status = str(report["settings"].get("status") or "")
        if settings_status not in {"CREATED", "REUSED"}:
            raise ValueError("current-user settings placement was not proven")

        stop_path = user_relay_stop_path(paths.direct_sync_root)
        stop_path.unlink(missing_ok=True)
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
        report["relay_start"] = dict(relay_launcher(paths.app_root))
        relay_start_status = str(report["relay_start"].get("status") or "")
        if relay_start_status in {"", "UNKNOWN"}:
            raise CurrentUserOnboardingError(
                "current-user relay launch result is UNKNOWN",
                report_path=paths.onboarding_report_path,
                status="UNKNOWN",
            )
        if relay_start_status != "START_REQUESTED":
            raise ValueError("current-user relay launch was not requested")
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
                "completed_at": _now(),
            }
        )
        _write_json_atomic(paths.onboarding_report_path, report)
        return report
    except CurrentUserOnboardingError as exc:
        report["status"] = exc.status
        report["failure"] = str(exc)
        report["error_type"] = exc.__class__.__name__
        _write_json_atomic(paths.onboarding_report_path, report)
        raise
    except Exception as exc:
        report["status"] = "FAILED"
        report["failure"] = str(exc)[:500]
        report["error_type"] = exc.__class__.__name__
        _write_json_atomic(paths.onboarding_report_path, report)
        raise CurrentUserOnboardingError(
            f"Label_Match first-run onboarding failed: {exc}",
            report_path=paths.onboarding_report_path,
            status="FAILED",
        ) from exc


def remove_current_user_setup(
    app_root: str | os.PathLike[str],
    *,
    environ: Mapping[str, str] | None = None,
    autostart_remover: Callable[[], Mapping[str, Any]] = remove_user_relay_autostart,
    relay_stopper: Callable[[str | os.PathLike[str]], Mapping[str, Any]] = request_user_relay_stop,
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
        report["relay_process"] = dict(relay_stopper(paths.direct_sync_root))
        autostart_status = str(report["relay_autostart"].get("status") or "")
        relay_status = str(report["relay_process"].get("status") or "")
        if autostart_status in {"", "UNKNOWN"} or relay_status in {"", "UNKNOWN"}:
            raise CurrentUserOnboardingError(
                "current-user removal result is UNKNOWN",
                report_path=paths.removal_report_path,
                status="UNKNOWN",
            )
        if autostart_status != "ABSENT":
            raise ValueError("HKCU relay persistence absence was not proven")
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
