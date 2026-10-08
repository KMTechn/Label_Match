"""Record the release that actually opens this user's business data."""
from __future__ import annotations

import json
import hashlib
import os
from pathlib import Path
import re
import subprocess
import sys
from types import SimpleNamespace
from typing import Mapping

from kmtech_shared.data_generation import probe_downgrade, record_generation, version_key
from writer_session_fence import canonical_control_root, writer_sink

CURRENT_VERSION = "v2.0.96"
APP_ID = "Label_Match"
_RELEASE_IDENTITY_PASS: dict[tuple[Path, Path], dict[str, object]] = {}
UNKNOWN_MESSAGE = (
    "이 PC의 자료 판을 확인할 수 없어 자료를 열거나 옛 판으로 되돌릴 수 없습니다. "
    "현재 코드와 자료·확인 기록을 보존하고 관리자에게 지원 복구를 요청하세요."
)


class DataGenerationError(RuntimeError):
    pass


def release_identity(app_root: Path) -> dict[str, object]:
    """Use the existing code-root verifier, never a user-supplied build string."""
    root = Path(app_root).expanduser().resolve()
    manifest_path = root / "portable-manifest.json"
    cache_key = None
    if manifest_path.exists() or getattr(sys, "frozen", False):
        entrypoint = Path(
            sys.executable if getattr(sys, "frozen", False)
            else (sys.argv[0] if sys.argv else __file__)
        ).expanduser().resolve()
        cache_key = (root, entrypoint)
        cached = _RELEASE_IDENTITY_PASS.get(cache_key)
        if cached is not None:
            return cached.copy()
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
                creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0),
            ).strip()
            unchanged = subprocess.run(
                ["git", "diff", "--quiet", "HEAD"],
                cwd=root, timeout=15, capture_output=True, check=False,
                creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0),
            ).returncode == 0
            untracked = subprocess.check_output(
                ["git", "ls-files", "--others", "--exclude-standard"], cwd=root,
                timeout=15, stderr=subprocess.DEVNULL,
                creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0),
            )
        except (OSError, subprocess.SubprocessError) as exc:
            raise DataGenerationError(UNKNOWN_MESSAGE) from exc
        if not unchanged or untracked or not re.fullmatch(r"[0-9a-f]{40}", commit):
            raise DataGenerationError(UNKNOWN_MESSAGE)
    identity = {"version": CURRENT_VERSION, "build_id": commit,
                "data_compatible_min_version": CURRENT_VERSION,
                "compatibility_verified": False}
    if cache_key is not None:
        # Remember only the fully validated installed identity, never failures
        # or developer clean-Git checks. Callers cannot mutate the stored PASS.
        _RELEASE_IDENTITY_PASS[cache_key] = identity.copy()
    return identity.copy()


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
    root = Path(app_root).expanduser().resolve()
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
    return _verified_generation_compatibility(identity, metadata)


def _verified_generation_compatibility(identity, metadata):
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


def resolve_data_path(value) -> Path:
    """Use the existing reader's expansion and no-reparse path contract."""
    from logistics_runtime_profile import assert_path_has_no_reparse_components
    try:
        if value is None or not str(value).strip():
            raise DataGenerationError(UNKNOWN_MESSAGE)
        return assert_path_has_no_reparse_components(value, label="data generation input").resolve(strict=False)
    except (OSError, ValueError, TypeError, RuntimeError) as exc:
        raise DataGenerationError(UNKNOWN_MESSAGE) from exc


def normalize_data_arguments(args, names):
    """Admission and subsequent readers consume the same selected arguments."""
    for name in names:
        value = getattr(args, name, None)
        if value is not None and str(value).strip():
            path = resolve_data_path(value)
            setattr(args, name, path if isinstance(value, Path) else str(path))


def marker_paths(data_roots, subject_sid: str, *, environ: Mapping[str, str] | None = None):
    if not re.fullmatch(r"S-1-\d+(?:-\d+)+", subject_sid):
        raise DataGenerationError(UNKNOWN_MESSAGE)
    return [canonical_control_root(environ) / "data-generation" / (subject_sid + ".json"),
            *(root / "data-generation.json" for root in dict.fromkeys(resolve_data_path(p) for p in data_roots))]


def _installer_source_identity(app_root: Path, *, source_inventory_sha256=None) -> dict[str, object]:
    """Bind read-only installer admission to the verified source's own bytes."""
    root = resolve_data_path(app_root)
    manifest_path = root / "portable-manifest.json"
    if not manifest_path.exists():
        return _installed_identity(root)
    if source_inventory_sha256 is not None:
        # Canonical supplies its already verified ReceiptSource inventory. This
        # read-only entry accepts no version/build override and creates no record.
        if not isinstance(source_inventory_sha256, str) or not re.fullmatch(r"[0-9a-f]{64}", source_inventory_sha256):
            raise DataGenerationError(UNKNOWN_MESSAGE)
        from label_exact_clone_resolution import portable_inventory_binding
        bound = {}
        for relative, maximum in (("portable-manifest.json", 65536),
                ("app/label_data_generation.py" if (root / "app/label_data_generation.py").is_file()
                 else "app/Label_Match.py", 10485760)):
            path = root / relative
            if path.stat().st_size > maximum:
                raise DataGenerationError(UNKNOWN_MESSAGE)
            bound[relative] = path.read_bytes()
            if len(bound[relative]) > maximum:
                raise DataGenerationError(UNKNOWN_MESSAGE)
        if portable_inventory_binding(root, bound_files=bound)["sha256"] != source_inventory_sha256:
            raise DataGenerationError(UNKNOWN_MESSAGE)
        files = {relative: {"size": len(raw), "sha256": hashlib.sha256(raw).hexdigest()}
                 for relative, raw in bound.items()}
    else:
        from current_user_onboarding import verify_bootstrap_integrity, BOOTSTRAP_INTEGRITY_MAX_BYTES
        record_path = root / "bootstrap-integrity.json"
        if record_path.stat().st_size > BOOTSTRAP_INTEGRITY_MAX_BYTES:
            raise DataGenerationError(UNKNOWN_MESSAGE)
        record_bytes = record_path.read_bytes()
        integrity = verify_bootstrap_integrity(
            SimpleNamespace(app_root=root, bootstrap_integrity_path=record_path), required=True)
        if integrity.get("status") != "PASS" or record_path.read_bytes() != record_bytes:
            raise DataGenerationError(UNKNOWN_MESSAGE)
        record = json.loads(record_bytes.decode("utf-8-sig"))
        files = {row["path"]: row for row in record.get("files", [])}

    def bound_bytes(relative: str, maximum: int) -> bytes:
        if source_inventory_sha256 is not None:
            raw = bound[relative]
        else:
            path = root / relative
            if path.stat().st_size > maximum:
                raise DataGenerationError(UNKNOWN_MESSAGE)
            raw = path.read_bytes()
        row = files.get(relative)
        if (not isinstance(row, dict) or row.get("size") != len(raw)
                or row.get("sha256") != hashlib.sha256(raw).hexdigest()):
            raise DataGenerationError(UNKNOWN_MESSAGE)
        return raw

    manifest = json.loads(bound_bytes("portable-manifest.json", 65536).decode("utf-8-sig"))
    if manifest.get("schema") != "label-match-portable-tree-v1":
        raise DataGenerationError(UNKNOWN_MESSAGE)
    commit = manifest.get("source_commit")
    if not isinstance(commit, str) or not re.fullmatch(r"[0-9a-f]{40}", commit):
        raise DataGenerationError(UNKNOWN_MESSAGE)
    relative = ("app/label_data_generation.py" if (root / "app/label_data_generation.py").is_file()
                else "app/Label_Match.py")
    source = bound_bytes(relative, 10485760).decode("utf-8-sig")
    versions = re.findall(r'(?m)^(?:CURRENT_VERSION|APP_VERSION)\s*=\s*["\'](v\d+\.\d+\.\d+)["\']\s*$', source)
    if len(versions) != 1:
        raise DataGenerationError(UNKNOWN_MESSAGE)
    version = versions[0]
    if manifest.get("version") not in (None, "", version):
        raise DataGenerationError(UNKNOWN_MESSAGE)
    identity = {"version": version, "build_id": commit,
                "data_compatible_min_version": version, "compatibility_verified": False}
    if source_inventory_sha256 is not None:
        if portable_inventory_binding(root)["sha256"] != source_inventory_sha256:
            raise DataGenerationError(UNKNOWN_MESSAGE)
        return _verified_generation_compatibility(identity, manifest)
    if version == CURRENT_VERSION and manifest.get("version") == CURRENT_VERSION:
        # Keep the ordinary signed-compatibility checks for current packets.
        return _installed_identity(root)
    return {"version": version, "build_id": commit,
            "data_compatible_min_version": version, "compatibility_verified": False}


def check_user_data_open(app_root: Path, data_roots=(), *, environ=None):
    return _check_user_data_open(app_root, data_roots, environ=environ,
                                identity_resolver=_installed_identity)


def check_installer_user_data_open(app_root: Path, data_roots=(), *, environ=None, source_inventory_sha256=None):
    """Read-only installation admission; no caller-supplied version or build."""
    return _check_user_data_open(app_root, data_roots, environ=environ,
                                identity_resolver=lambda root: _installer_source_identity(
                                    root, source_inventory_sha256=source_inventory_sha256))


def _check_user_data_open(app_root: Path, data_roots=(), *, environ=None, identity_resolver):
    """Read-only admission before resolving settings or changing lifecycle state."""
    from protected_admin import current_process_sid
    values = os.environ if environ is None else environ
    try:
        roots = list(data_roots)
        local = str(values.get("LOCALAPPDATA") or "").strip()
        save = str(values.get("LABEL_MATCH_SAVE_DIR") or "").strip()
        if local:
            local_root = resolve_data_path(local)
            roots.extend((local_root / "KMTech" / "Label_Match" / "data",
                          local_root / "KMTech" / "Label_Match" / "config",
                          local_root / "KMTech" / "DirectSync" / "label_match",
                          local_root / "KMTech" / "DirectSync" / "label_match" / "queue",
                          local_root / "KMTech" / "Bootstrap" / "Label_Match"))
        elif save:
            roots.append(resolve_data_path(save).parent / "Bootstrap" / "Label_Match")
        for name in ("LABEL_MATCH_SAVE_DIR", "LABEL_MATCH_DIRECT_SYNC_ROOT",
                     "LABEL_MATCH_DIRECT_SYNC_PROGRAM_DATA_ROOT"):
            value = str(values.get(name) or "").strip()
            if value:
                root = resolve_data_path(value)
                roots.extend((root, root / "queue"))
        settings = str(values.get("LABEL_MATCH_SETTINGS_PATH") or "").strip()
        if settings:
            roots.append(resolve_data_path(settings).parent)
        # These are read-only security inputs, including the normal default
        # administrator verifier. Refuse before an entry records any generation.
        for name in ("LABEL_MATCH_ENROLLMENT_TLS_CA_BUNDLE_PATH",
                     "LABEL_MATCH_LOGISTICS_TLS_CA_BUNDLE_PATH",
                     "KMTECH_LABEL_CONFLICT_RESOLUTION_RECEIPT_PATH"):
            value = str(values.get(name) or "").strip()
            if value:
                roots.append(resolve_data_path(value).parent)
        from protected_admin import default_protected_admin_profile_path
        roots.append(resolve_data_path(default_protected_admin_profile_path(environ=values)).parent)
        if local or save or str(values.get("LABEL_MATCH_LOGISTICS_PROFILE_PATH") or "").strip():
            from logistics_runtime_profile import default_logistics_profile_path
            roots.append(default_logistics_profile_path(values,
                default_data_root=resolve_data_path(save) if save else None).parent)
        sid = current_process_sid()
        identity = identity_resolver(app_root)
        decision = probe_downgrade(marker_paths(roots, sid), str(identity["version"]),
            app_id=APP_ID, subject_sid=sid, target_build_id=str(identity["build_id"]))
        if not decision["allowed"]:
            raise DataGenerationError(str(decision["message"]))
        return identity
    except DataGenerationError:
        raise
    except (OSError, ValueError, TypeError, RuntimeError) as exc:
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
    except (OSError, ValueError, TypeError, RuntimeError) as exc:
        raise DataGenerationError(UNKNOWN_MESSAGE) from exc
