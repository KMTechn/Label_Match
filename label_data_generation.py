"""Record the release that actually opens this user's business data."""
from __future__ import annotations

import json
import os
from pathlib import Path
import re
import subprocess
import sys
from types import SimpleNamespace
from typing import Mapping

from kmtech_shared.data_generation import probe_downgrade, record_generation, version_key
from writer_session_fence import canonical_control_root, writer_sink

CURRENT_VERSION = "v2.0.95"
APP_ID = "Label_Match"
UNKNOWN_MESSAGE = (
    "이 PC의 자료 판을 확인할 수 없어 자료를 열거나 옛 판으로 되돌릴 수 없습니다. "
    "현재 코드와 자료·확인 기록을 보존하고 관리자에게 지원 복구를 요청하세요."
)


class DataGenerationError(RuntimeError):
    pass


def release_identity(app_root: Path) -> dict[str, object]:
    """Use the existing code-root verifier, never a user-supplied build string."""
    root = Path(app_root).resolve()
    manifest_path = root / "portable-manifest.json"
    if manifest_path.exists() or getattr(sys, "frozen", False):
        from current_user_onboarding import (
            verify_bootstrap_integrity,
        )
        integrity = verify_bootstrap_integrity(
            SimpleNamespace(app_root=root, bootstrap_integrity_path=root / "bootstrap-integrity.json"),
            required=True,
        )
        if integrity.get("status") != "PASS":
            raise DataGenerationError(UNKNOWN_MESSAGE)
        if manifest_path.exists():
            if manifest_path.stat().st_size > 65536:
                raise DataGenerationError(UNKNOWN_MESSAGE)
            manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
            valid = (manifest.get("schema") == "label-match-portable-tree-v1"
                     and manifest.get("version") == CURRENT_VERSION)
        else:
            # The frozen builder already packages this exact factory identity;
            # the existing code-root verifier above binds its onedir bytes.
            manifest_path = root / "build-identity.json"
            if manifest_path.stat().st_size > 65536:
                raise DataGenerationError(UNKNOWN_MESSAGE)
            manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
            valid = (manifest.get("build_identity_schema_version") == 1
                     and manifest.get("app_id") == "label_match"
                     and manifest.get("app_version") == CURRENT_VERSION)
        commit = manifest.get("source_commit")
        if (not valid or not isinstance(commit, str) or not re.fullmatch(r"[0-9a-f]{40}", commit)):
            raise DataGenerationError(UNKNOWN_MESSAGE)
    else:
        # Supported developer entry: require a committed, unchanged source tree.
        # Portable and frozen releases cannot borrow an unrelated checkout's HEAD.
        if root != Path(__file__).resolve().parent:
            raise DataGenerationError(UNKNOWN_MESSAGE)
        try:
            commit = subprocess.check_output(
                ["git", "rev-parse", "--verify", "HEAD^{commit}"], cwd=root,
                timeout=15, stderr=subprocess.DEVNULL, text=True,
            ).strip()
            unchanged = subprocess.run(
                ["git", "diff", "--quiet", "HEAD"],
                cwd=root, timeout=15, capture_output=True, check=False,
            ).returncode == 0
            untracked = subprocess.check_output(
                ["git", "ls-files", "--others", "--exclude-standard"], cwd=root,
                timeout=15, stderr=subprocess.DEVNULL,
            )
        except (OSError, subprocess.SubprocessError) as exc:
            raise DataGenerationError(UNKNOWN_MESSAGE) from exc
        if not unchanged or untracked or not re.fullmatch(r"[0-9a-f]{40}", commit):
            raise DataGenerationError(UNKNOWN_MESSAGE)
    return {"version": CURRENT_VERSION, "build_id": commit,
            "data_compatible_min_version": CURRENT_VERSION,
            "compatibility_verified": False}


def verified_private_compatibility(
    manifest: dict, signature: bytes, public_key: object, *, version: str, build_id: str,
) -> dict[str, object]:
    # This is the existing private-update ES256 verification entrypoint. No
    # portable metadata, current-file hash, or CPython signer grants compatibility.
    from Label_Match import _verify_update_manifest_signature
    _verify_update_manifest_signature(manifest, signature, public_key)
    if (manifest.get("signature_version") != "es256-v1"
            or manifest.get("schema_version") != "kmtech-private-update-manifest-v1"
            or manifest.get("manifest_version") != 1
            or manifest.get("app_id") != APP_ID or manifest.get("package_id") != APP_ID
            or version_key(manifest.get("version")) != version_key(version)
            or manifest.get("source_commit") != build_id):
        raise DataGenerationError(UNKNOWN_MESSAGE)
    floor = manifest.get("data_compatible_min_version", version)
    if version_key(floor) > version_key(version):
        raise DataGenerationError(UNKNOWN_MESSAGE)
    return {"version": version, "build_id": build_id,
            "data_compatible_min_version": floor,
            "compatibility_verified": "data_compatible_min_version" in manifest}


def _installed_identity(app_root: Path) -> dict[str, object]:
    from label_match_product_host import is_portable_product_root
    root = Path(app_root).resolve()
    if root.name.casefold() == "app" and is_portable_product_root(root.parent):
        root = root.parent
    identity = release_identity(root)
    metadata_path = root / "portable-manifest.json"
    if not metadata_path.is_file() and getattr(sys, "frozen", False):
        metadata_path = root / "build-identity.json"
    if not metadata_path.is_file():
        return identity
    if metadata_path.stat().st_size > 65536:
        raise DataGenerationError(UNKNOWN_MESSAGE)
    metadata = json.loads(metadata_path.read_text(encoding="utf-8"))
    proof = metadata.get("data_generation_signed_manifest")
    signature_hex = metadata.get("data_generation_signature_hex")
    if proof is not None or signature_hex is not None:
        from Label_Match import _get_update_manifest_public_key
        if (not isinstance(proof, dict) or not isinstance(signature_hex, str)
                or not re.fullmatch(r"[0-9a-f]{128}", signature_hex)):
            raise DataGenerationError(UNKNOWN_MESSAGE)
        identity = verified_private_compatibility(
            proof, bytes.fromhex(signature_hex),
            _get_update_manifest_public_key(), version=str(identity["version"]),
            build_id=str(identity["build_id"]),
        )
    return identity


def marker_paths(data_roots, subject_sid: str, *, environ: Mapping[str, str] | None = None):
    if not re.fullmatch(r"S-1-\d+(?:-\d+)+", subject_sid):
        raise DataGenerationError(UNKNOWN_MESSAGE)
    return [canonical_control_root(environ) / "data-generation" / (subject_sid + ".json"),
            *(Path(root).absolute() / "data-generation.json" for root in dict.fromkeys(data_roots))]


def check_user_data_open(app_root: Path, data_roots=(), *, environ=None):
    """Read-only admission before resolving settings or changing lifecycle state."""
    from protected_admin import current_process_sid
    values = os.environ if environ is None else environ
    roots = list(data_roots)
    local = values.get("LOCALAPPDATA")
    if local:
        roots.extend((Path(local) / "KMTech" / "Label_Match" / "data",
                      Path(local) / "KMTech" / "DirectSync" / "label_match",
                      Path(local) / "KMTech" / "DirectSync" / "label_match" / "queue"))
    for name in ("LABEL_MATCH_SAVE_DIR", "LABEL_MATCH_DIRECT_SYNC_ROOT",
                 "LABEL_MATCH_DIRECT_SYNC_PROGRAM_DATA_ROOT"):
        if values.get(name):
            roots.extend((Path(values[name]), Path(values[name]) / "queue"))
    try:
        sid = current_process_sid()
        identity = _installed_identity(app_root)
        decision = probe_downgrade(marker_paths(roots, sid), str(identity["version"]),
            app_id=APP_ID, subject_sid=sid, target_build_id=str(identity["build_id"]))
        if not decision["allowed"]:
            raise DataGenerationError(str(decision["message"]))
        return identity
    except DataGenerationError:
        raise
    except (OSError, ValueError, TypeError) as exc:
        raise DataGenerationError(UNKNOWN_MESSAGE) from exc


@writer_sink("data_generation_record")
def record_user_data_open(app_root: Path, data_roots, *, environ=None, opener=None):
    """Called immediately before a business constructor, including direct entry."""
    from protected_admin import current_process_sid
    try:
        sid = current_process_sid()
        identity = _installed_identity(app_root)
        # Admission and its SID record must use the same observed process root.
        # The optional onboarding map selects data paths, not another fence.
        paths = marker_paths(data_roots, sid)
        decision = probe_downgrade(paths, str(identity["version"]), app_id=APP_ID,
                                  subject_sid=sid, target_build_id=str(identity["build_id"]))
        if not decision["allowed"]:
            raise DataGenerationError(str(decision["message"]))
        for path in paths:
            from logistics_runtime_profile import assert_path_has_no_reparse_components
            assert_path_has_no_reparse_components(path, label="data generation marker")
            path.parent.mkdir(parents=True, exist_ok=True)
            record_generation(path, str(identity["version"]), app_id=APP_ID, subject_sid=sid,
                              build_id=str(identity["build_id"]),
                              data_compatible_min_version=str(identity["data_compatible_min_version"]),
                              compatibility_verified=bool(identity["compatibility_verified"]))
        return opener() if opener is not None else identity
    except DataGenerationError:
        raise
    except (OSError, ValueError, TypeError) as exc:
        raise DataGenerationError(UNKNOWN_MESSAGE) from exc
