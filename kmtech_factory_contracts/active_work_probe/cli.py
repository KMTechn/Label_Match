"""PowerShell-compatible CLI for the broker-owned active-work probe."""

from __future__ import annotations

import argparse
import hashlib
import os
import re
import sys
from collections import OrderedDict
from datetime import datetime
from pathlib import Path
from typing import Any, Callable, Mapping, Sequence

from .adapters import BLOCKER_KIND_CATALOG, create_adapter
from .core import (
    BUILD_IDENTITY_SCHEMA_VERSION,
    EXIT_ACTIVE,
    EXIT_CLEAR,
    EXIT_ERROR,
    FileSnapshot,
    ProbeAdapter,
    ProbeBinding,
    ProbeError,
    ProbeRequest,
    build_active_diagnostic,
    build_clear_evidence,
    build_error_diagnostic,
    file_sha256,
    observe_adapter,
    ordered_json_bytes,
    require_lower_hex,
    require_trusted_path_ancestry,
    snapshot_path,
    strict_json_bytes,
)


PROBE_ARTIFACT_FILENAME = "KMTechActiveWorkProbe.exe"
BUILD_IDENTITY_FILENAMES = {
    "independent": "KMTechActiveWorkProbe.independent.build-identity.json",
    "integrated": "KMTechActiveWorkProbe.integrated.build-identity.json",
}
ALL_APPS = (
    "Inspection_worker",
    "Rework_worker",
    "Defect_Inspection",
    "Container_Audit",
    "Label_Match",
)
APP_IDS = {
    "Inspection_worker": "inspection_worker",
    "Rework_worker": "rework_worker",
    "Defect_Inspection": "defect_inspection",
    "Container_Audit": "container_audit",
    "Label_Match": "label_match",
}
_SAFE_ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.:-]{0,255}$")


class _ArgumentParser(argparse.ArgumentParser):
    def error(self, message: str) -> None:
        raise ProbeError("CLI_ARGUMENT_INVALID", message)


def _parser() -> argparse.ArgumentParser:
    parser = _ArgumentParser(allow_abbrev=False, add_help=True)
    parser.add_argument("-Mode", required=True)
    parser.add_argument("-OutputPath")
    parser.add_argument("-TargetPc")
    parser.add_argument("-App")
    parser.add_argument("-ReleaseRunId")
    parser.add_argument("-CanaryRunId")
    parser.add_argument("-QualificationRunId")
    parser.add_argument("-ProbeBuildIdentityPath")
    parser.add_argument("-ProbeBuildIdentitySha256")
    parser.add_argument("-ProbeArtifactPath")
    parser.add_argument("-ProbeSourceCommit")
    parser.add_argument("-WorkflowMode")
    parser.add_argument("-SupportedApps")
    parser.add_argument("-ProbeName", default="KMTechActiveWorkProbe")
    parser.add_argument("-ProbeVersion", default="v1.0.3.4")
    return parser


def _required(namespace: argparse.Namespace, *names: str) -> None:
    missing = [name for name in names if not str(getattr(namespace, name, "") or "").strip()]
    if missing:
        raise ProbeError("CLI_ARGUMENT_INVALID", f"required arguments missing: {missing!r}")


def _artifact_path(explicit: Path | None = None) -> Path:
    if explicit is not None:
        if not explicit.is_absolute():
            raise ProbeError("PROBE_ARTIFACT_PATH_INVALID", "artifact path must be absolute")
        return require_trusted_path_ancestry(explicit)
    if bool(getattr(sys, "frozen", False)):
        value = Path(os.path.abspath(sys.executable))
    else:
        value = Path(os.path.abspath(sys.argv[0]))
    return require_trusted_path_ancestry(value)


def _create_new_fsynced(path: Path, value: Mapping[str, Any]) -> None:
    if not path.is_absolute():
        raise ProbeError("OUTPUT_PATH_INVALID", "output path must be absolute")
    output = require_trusted_path_ancestry(path)
    try:
        parent_stat = os.lstat(output.parent)
    except OSError as exc:
        raise ProbeError("OUTPUT_PATH_INVALID", repr(exc)) from exc
    reparse = bool(int(getattr(parent_stat, "st_file_attributes", 0) or 0) & 0x400)
    if not output.parent.is_dir() or output.parent.is_symlink() or reparse:
        raise ProbeError("OUTPUT_PATH_INVALID", "output parent is absent")
    payload = ordered_json_bytes(value)
    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL
    flags |= int(getattr(os, "O_BINARY", 0))
    descriptor: int | None = None
    try:
        descriptor = os.open(output, flags, 0o600)
        offset = 0
        while offset < len(payload):
            written = os.write(descriptor, payload[offset:])
            if written <= 0:
                raise OSError("short report write")
            offset += written
        os.fsync(descriptor)
    except FileExistsError as exc:
        raise ProbeError("OUTPUT_ALREADY_EXISTS", "CreateNew report path exists") from exc
    except OSError as exc:
        raise ProbeError("OUTPUT_WRITE_FAILED", repr(exc)) from exc
    finally:
        if descriptor is not None:
            os.close(descriptor)


def _validate_build_identity(
    identity_path: Path,
    expected_identity_sha256: str,
    artifact_path: Path,
    app: str,
) -> tuple[ProbeBinding, FileSnapshot]:
    if not identity_path.is_absolute():
        raise ProbeError("PROBE_BUILD_IDENTITY_PATH_INVALID", "identity path must be absolute")
    require_lower_hex(
        expected_identity_sha256,
        64,
        code="PROBE_BUILD_IDENTITY_HASH_INVALID",
    )
    identity_before = snapshot_path(identity_path)
    if not identity_before.exists or identity_before.kind != "regular" or identity_before.size <= 0:
        raise ProbeError("PROBE_BUILD_IDENTITY_INVALID", "build identity is absent")
    if identity_before.size > 64 * 1024 or identity_before.sha256 != expected_identity_sha256:
        raise ProbeError("PROBE_BUILD_IDENTITY_INVALID", "build identity hash or size differs")
    try:
        raw = identity_path.read_bytes()
    except OSError as exc:
        raise ProbeError("PROBE_BUILD_IDENTITY_INVALID", repr(exc)) from exc
    if (
        hashlib.sha256(raw).hexdigest() != expected_identity_sha256
        or file_sha256(identity_path) != expected_identity_sha256
    ):
        raise ProbeError("PROBE_BUILD_IDENTITY_CHANGED", "build identity changed during read")
    value = strict_json_bytes(raw)
    expected_fields = {
        "schema_version",
        "probe_name",
        "probe_version",
        "probe_artifact_sha256",
        "probe_source_commit",
        "workflow_mode",
        "supported_apps",
    }
    if not isinstance(value, Mapping) or set(value) != expected_fields:
        raise ProbeError("PROBE_BUILD_IDENTITY_INVALID", "build identity fields differ")
    if value.get("schema_version") != BUILD_IDENTITY_SCHEMA_VERSION:
        raise ProbeError("PROBE_BUILD_IDENTITY_INVALID", "build identity schema differs")
    for field_name in ("probe_name", "probe_version"):
        field_value = str(value.get(field_name) or "")
        if not _SAFE_ID_RE.fullmatch(field_value):
            raise ProbeError("PROBE_BUILD_IDENTITY_INVALID", f"{field_name} is invalid")
    artifact_snapshot = snapshot_path(artifact_path)
    if not artifact_snapshot.exists or artifact_snapshot.kind != "regular":
        raise ProbeError("PROBE_ARTIFACT_INVALID", "probe artifact is absent")
    if artifact_path.name != PROBE_ARTIFACT_FILENAME:
        raise ProbeError("PROBE_ARTIFACT_FILENAME_INVALID", "probe artifact filename is nonstandard")
    artifact_sha256 = require_lower_hex(
        str(value.get("probe_artifact_sha256") or ""),
        64,
        code="PROBE_BUILD_IDENTITY_INVALID",
    )
    if artifact_snapshot.sha256 != artifact_sha256:
        raise ProbeError("PROBE_ARTIFACT_IDENTITY_MISMATCH", "artifact hash differs")
    source_commit = require_lower_hex(
        str(value.get("probe_source_commit") or ""),
        40,
        code="PROBE_BUILD_IDENTITY_INVALID",
    )
    workflow_mode = str(value.get("workflow_mode") or "")
    supported = value.get("supported_apps")
    if (
        workflow_mode not in BUILD_IDENTITY_FILENAMES
        or not isinstance(supported, list)
        or not supported
        or any(not isinstance(item, str) or item not in ALL_APPS for item in supported)
        or len(supported) != len(set(supported))
    ):
        raise ProbeError("PROBE_BUILD_IDENTITY_INVALID", "workflow scope is invalid")
    if workflow_mode == "independent":
        if supported != [app]:
            raise ProbeError("PROBE_BUILD_IDENTITY_SCOPE_MISMATCH", "independent scope differs")
    elif tuple(supported) != ALL_APPS:
        raise ProbeError("PROBE_BUILD_IDENTITY_SCOPE_MISMATCH", "integrated scope differs")
    if identity_path.name != BUILD_IDENTITY_FILENAMES[workflow_mode]:
        raise ProbeError("PROBE_BUILD_IDENTITY_INVALID", "build identity filename is nonstandard")
    if snapshot_path(identity_path) != identity_before:
        raise ProbeError("PROBE_BUILD_IDENTITY_CHANGED", "build identity changed")
    return (
        ProbeBinding(
            build_identity_sha256=expected_identity_sha256,
            artifact_sha256=artifact_sha256,
            source_commit=source_commit,
            artifact_path=str(artifact_path),
            workflow_mode=workflow_mode,
            supported_apps=tuple(supported),
        ),
        identity_before,
    )


def _request(namespace: argparse.Namespace) -> ProbeRequest:
    _required(namespace, "TargetPc", "App", "ReleaseRunId")
    app = str(namespace.App)
    app_id = APP_IDS.get(app)
    if app_id is None:
        raise ProbeError("APP_UNSUPPORTED", "requested app is unsupported")
    has_canary = bool(str(namespace.CanaryRunId or "").strip())
    has_qualification = bool(str(namespace.QualificationRunId or "").strip())
    if has_canary == has_qualification:
        raise ProbeError("REQUEST_IDENTITY_INVALID", "exactly one run identity is required")
    if str(namespace.TargetPc) == "TEST1" and not has_canary:
        raise ProbeError("REQUEST_IDENTITY_INVALID", "TEST1 requires canary run identity")
    if str(namespace.TargetPc) != "TEST1" and not has_qualification:
        raise ProbeError("REQUEST_IDENTITY_INVALID", "qualification target requires qualification run")
    return ProbeRequest(
        release_run_id=str(namespace.ReleaseRunId),
        run_id_name="canary_run_id" if has_canary else "qualification_run_id",
        run_id=str(namespace.CanaryRunId if has_canary else namespace.QualificationRunId),
        target_pc=str(namespace.TargetPc),
        app_id=app_id,
        app=app,
    )


def _fallback_binding(
    namespace: argparse.Namespace,
    artifact_path: Path,
    parsed_identity: Mapping[str, Any] | None = None,
) -> ProbeBinding:
    try:
        artifact_hash = file_sha256(artifact_path)
    except ProbeError:
        artifact_hash = "0" * 64
    build_hash = str(namespace.ProbeBuildIdentitySha256 or "")
    if len(build_hash) != 64 or any(character not in "0123456789abcdef" for character in build_hash):
        build_hash = "0" * 64
    source_commit = str((parsed_identity or {}).get("probe_source_commit") or "")
    if len(source_commit) != 40 or any(character not in "0123456789abcdef" for character in source_commit):
        source_commit = "0" * 40
    return ProbeBinding(
        build_identity_sha256=build_hash,
        artifact_sha256=artifact_hash,
        source_commit=source_commit,
        artifact_path=str(artifact_path),
        workflow_mode="independent",
        supported_apps=(str(namespace.App or "unknown"),),
    )


AdapterFactory = Callable[[str, str], ProbeAdapter]


def _run_active_work(
    namespace: argparse.Namespace,
    *,
    adapter_factory: AdapterFactory,
    artifact_path: Path,
    now: datetime | None,
) -> tuple[int, OrderedDict[str, Any]]:
    _required(
        namespace,
        "OutputPath",
        "ProbeBuildIdentityPath",
        "ProbeBuildIdentitySha256",
        "WorkflowMode",
    )
    request = _request(namespace)
    fallback_binding = _fallback_binding(namespace, artifact_path)
    try:
        request.validate()
        identity_path = Path(str(namespace.ProbeBuildIdentityPath))
        if not identity_path.is_absolute():
            raise ProbeError("PROBE_BUILD_IDENTITY_PATH_INVALID", "identity path must be absolute")
        binding, identity_before = _validate_build_identity(
            identity_path,
            str(namespace.ProbeBuildIdentitySha256),
            artifact_path,
            request.app,
        )
        fallback_binding = binding
        if str(namespace.WorkflowMode) != binding.workflow_mode:
            raise ProbeError("PROBE_BUILD_IDENTITY_SCOPE_MISMATCH", "workflow mode differs")
        if binding.workflow_mode == "integrated" and request.target_pc != "TEST1":
            raise ProbeError("PROBE_BUILD_IDENTITY_SCOPE_MISMATCH", "integrated scope requires TEST1")
        artifact_before = snapshot_path(artifact_path)
        adapter = adapter_factory(request.app, request.target_pc)
        observed = observe_adapter(adapter)
        observed_kinds = {
            str(row["kind"]) for row in observed.observation.sanitized_blockers()
        }
        if not observed_kinds.issubset(BLOCKER_KIND_CATALOG):
            raise ProbeError("BLOCKER_KIND_UNKNOWN", "adapter emitted an undeclared blocker kind")
        if snapshot_path(artifact_path) != artifact_before:
            raise ProbeError("PROBE_ARTIFACT_CHANGED", "probe artifact changed during observation")
        if snapshot_path(identity_path) != identity_before:
            raise ProbeError("PROBE_BUILD_IDENTITY_CHANGED", "build identity changed after observation")
        if observed.observation.active:
            return (
                EXIT_ACTIVE,
                build_active_diagnostic(request, binding, observed.observation, now=now),
            )
        return EXIT_CLEAR, build_clear_evidence(request, binding, observed, now=now)
    except ProbeError as exc:
        return EXIT_ERROR, build_error_diagnostic(request, fallback_binding, exc, now=now)
    except Exception as exc:  # no raw exception text leaves the protected receipt
        error = ProbeError("UNEXPECTED_PROBE_ERROR", repr(exc))
        return EXIT_ERROR, build_error_diagnostic(request, fallback_binding, error, now=now)


def _parse_supported_apps(value: str) -> tuple[str, ...]:
    parts = str(value or "").split(",")
    if any(not part.strip() for part in parts):
        raise ProbeError("BUILD_IDENTITY_SCOPE_INVALID", "empty supported app token")
    rows = tuple(part.strip() for part in parts)
    if not rows or len(rows) != len(set(rows)) or any(row not in ALL_APPS for row in rows):
        raise ProbeError("BUILD_IDENTITY_SCOPE_INVALID", "supported app list is invalid")
    return rows


def _run_build_identity(namespace: argparse.Namespace) -> OrderedDict[str, Any]:
    _required(
        namespace,
        "OutputPath",
        "ProbeArtifactPath",
        "ProbeSourceCommit",
        "WorkflowMode",
        "SupportedApps",
    )
    workflow_mode = str(namespace.WorkflowMode)
    if workflow_mode not in BUILD_IDENTITY_FILENAMES:
        raise ProbeError("BUILD_IDENTITY_SCOPE_INVALID", "workflow mode is invalid")
    output_path = Path(str(namespace.OutputPath))
    if not output_path.is_absolute():
        raise ProbeError("OUTPUT_PATH_INVALID", "output path must be absolute")
    if output_path.name != BUILD_IDENTITY_FILENAMES[workflow_mode]:
        raise ProbeError("BUILD_IDENTITY_FILENAME_INVALID", "build identity filename is nonstandard")
    artifact_path = Path(str(namespace.ProbeArtifactPath))
    if not artifact_path.is_absolute():
        raise ProbeError("PROBE_ARTIFACT_PATH_INVALID", "artifact path must be absolute")
    if artifact_path.name != PROBE_ARTIFACT_FILENAME:
        raise ProbeError("PROBE_ARTIFACT_FILENAME_INVALID", "probe artifact filename is nonstandard")
    artifact = snapshot_path(artifact_path)
    if not artifact.exists or artifact.kind != "regular" or artifact.size <= 0:
        raise ProbeError("PROBE_ARTIFACT_INVALID", "probe artifact is absent")
    source_commit = require_lower_hex(
        str(namespace.ProbeSourceCommit),
        40,
        code="PROBE_SOURCE_COMMIT_INVALID",
    )
    supported = _parse_supported_apps(str(namespace.SupportedApps))
    if not _SAFE_ID_RE.fullmatch(str(namespace.ProbeName or "")) or not _SAFE_ID_RE.fullmatch(
        str(namespace.ProbeVersion or "")
    ):
        raise ProbeError("BUILD_IDENTITY_ID_INVALID", "probe name or version is invalid")
    if workflow_mode == "independent" and len(supported) != 1:
        raise ProbeError("BUILD_IDENTITY_SCOPE_INVALID", "independent build requires one app")
    if workflow_mode == "integrated" and supported != ALL_APPS:
        raise ProbeError("BUILD_IDENTITY_SCOPE_INVALID", "integrated build requires exact app order")
    identity = OrderedDict(
        (
            ("schema_version", BUILD_IDENTITY_SCHEMA_VERSION),
            ("probe_name", str(namespace.ProbeName)),
            ("probe_version", str(namespace.ProbeVersion)),
            ("probe_artifact_sha256", artifact.sha256),
            ("probe_source_commit", source_commit),
            ("workflow_mode", workflow_mode),
            ("supported_apps", list(supported)),
        )
    )
    if snapshot_path(artifact_path) != artifact:
        raise ProbeError("PROBE_ARTIFACT_CHANGED", "probe artifact changed during identity generation")
    return identity


def main(
    argv: Sequence[str] | None = None,
    *,
    adapter_factory: AdapterFactory = create_adapter,
    artifact_path: Path | None = None,
    now: datetime | None = None,
) -> int:
    try:
        namespace = _parser().parse_args(list(argv) if argv is not None else None)
        _required(namespace, "OutputPath")
        output_path = Path(str(namespace.OutputPath))
        if not output_path.is_absolute():
            raise ProbeError("OUTPUT_PATH_INVALID", "output path must be absolute")
        if namespace.Mode == "build-identity":
            identity = _run_build_identity(namespace)
            _create_new_fsynced(output_path, identity)
            return EXIT_CLEAR
        if namespace.Mode != "active-work-evidence":
            raise ProbeError("CLI_MODE_INVALID", "unsupported probe mode")
        code, receipt = _run_active_work(
            namespace,
            adapter_factory=adapter_factory,
            artifact_path=_artifact_path(artifact_path),
            now=now,
        )
        _create_new_fsynced(output_path, receipt)
        return code
    except ProbeError:
        return EXIT_ERROR
    except Exception:
        return EXIT_ERROR


if __name__ == "__main__":  # pragma: no cover - exercised through __main__
    raise SystemExit(main())


__all__ = [
    "ALL_APPS",
    "BUILD_IDENTITY_FILENAMES",
    "PROBE_ARTIFACT_FILENAME",
    "main",
]
