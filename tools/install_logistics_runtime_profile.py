"""Install one non-secret logistics profile and machine-scope DPAPI token."""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import re
import subprocess
import sys
from typing import Any


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from logistics_runtime_profile import (  # noqa: E402
    DEFAULT_TOKEN_REF,
    PROFILE_CONTRACT_VERSION,
    assert_path_has_no_reparse_components,
    default_profile_path,
    load_logistics_runtime_profile,
    profile_from_values,
    protect_bearer_token,
)


DEFAULT_INSTALL_TOKEN_ENV = "KM_LOGISTICS_INSTALL_BEARER_TOKEN"


def _secure_profile_directory(path: Path, reader_principal: str) -> None:
    reader = str(reader_principal or "").strip()
    if not reader or not re.fullmatch(r"[A-Za-z0-9가-힣 _.-]+(?:\\[A-Za-z0-9가-힣 _.$-]+)?", reader):
        raise ValueError("reader_principal is required and must be a safe account name")
    if os.name != "nt":
        raise RuntimeError("machine profile ACL installation requires Windows")
    path.mkdir(parents=True, exist_ok=True)
    subprocess.run(
        [
            "icacls", str(path), "/inheritance:r", "/grant:r",
            "*S-1-5-18:(OI)(CI)F", "*S-1-5-32-544:(OI)(CI)F",
            f"{reader}:(OI)(CI)R",
        ],
        check=True,
        capture_output=True,
        text=True,
        timeout=30,
    )


def _atomic_write(path: Path, data: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    try:
        temporary.write_bytes(data)
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def install_runtime_profile(
    *,
    profile_path: str | os.PathLike[str],
    base_url: str,
    authority_scope: str,
    authority_epoch: int,
    authority_plane: str,
    plane_epoch: int,
    device_id: str,
    source_host_id: str,
    bearer_token: str,
    timeout_seconds: float = 10.0,
    dry_run: bool = False,
    replace: bool = False,
    reader_principal: str = "",
) -> dict[str, Any]:
    target = assert_path_has_no_reparse_components(
        profile_path, label="runtime profile"
    )
    values = {
        "contract_version": PROFILE_CONTRACT_VERSION,
        "base_url": base_url,
        "authority_scope": authority_scope,
        "authority_epoch": authority_epoch,
        "authority_plane": authority_plane,
        "plane_epoch": plane_epoch,
        "device_id": device_id,
        "source_host_id": source_host_id,
        "bearer_token_ref": DEFAULT_TOKEN_REF,
        "timeout_seconds": timeout_seconds,
    }
    validated = profile_from_values(
        values,
        profile_path=target,
        bearer_token=bearer_token,
        required=True,
    )
    summary = validated.redacted_summary()
    summary["status"] = "dry-run" if dry_run else "installed"
    if dry_run:
        return summary
    if target.exists() and not replace:
        raise FileExistsError(
            "runtime profile already exists; use --replace for an intentional rotation"
        )
    _secure_profile_directory(target.parent, reader_principal)
    protected = protect_bearer_token(bearer_token)
    secret_relative = DEFAULT_TOKEN_REF.split(":", 1)[1].replace("/", os.sep)
    secret_path = (target.parent / secret_relative).resolve()
    secret_path.relative_to(target.parent.resolve())
    _atomic_write(secret_path, protected)
    _atomic_write(
        target,
        (json.dumps(values, ensure_ascii=False, sort_keys=True, indent=2) + "\n").encode(
            "utf-8"
        ),
    )
    readback = load_logistics_runtime_profile(required=True, profile_path=target)
    if readback is None or readback != validated:
        raise RuntimeError("runtime profile exact readback failed")
    return summary


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Install the shared KMTech logistics PC profile.")
    parser.add_argument("--profile-path", default=str(default_profile_path()))
    parser.add_argument("--base-url", required=True)
    parser.add_argument("--authority-scope", required=True)
    parser.add_argument("--authority-epoch", type=int, required=True)
    parser.add_argument("--authority-plane", default="AUTHORITATIVE")
    parser.add_argument("--plane-epoch", type=int, required=True)
    parser.add_argument("--device-id", required=True)
    parser.add_argument("--source-host-id", required=True)
    parser.add_argument("--timeout-seconds", type=float, default=10.0)
    parser.add_argument("--token-env", default=DEFAULT_INSTALL_TOKEN_ENV)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--replace", action="store_true")
    parser.add_argument("--reader-principal", help="Windows account allowed to read the DPAPI blob")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    token = str(os.environ.get(args.token_env) or "").strip()
    if not token:
        print("BLOCKED: bearer token environment value is missing", file=sys.stderr)
        return 2
    try:
        report = install_runtime_profile(
            profile_path=args.profile_path,
            base_url=args.base_url,
            authority_scope=args.authority_scope,
            authority_epoch=args.authority_epoch,
            authority_plane=args.authority_plane,
            plane_epoch=args.plane_epoch,
            device_id=args.device_id,
            source_host_id=args.source_host_id,
            bearer_token=token,
            timeout_seconds=args.timeout_seconds,
            dry_run=args.dry_run,
            replace=args.replace,
            reader_principal=args.reader_principal or "",
        )
    except Exception as exc:
        print(f"BLOCKED: {exc.__class__.__name__}: {exc}", file=sys.stderr)
        return 2
    print(json.dumps(report, ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
