"""Provision a machine-local protected-administrator verifier for Label Match."""

from __future__ import annotations

import argparse
import getpass
import hmac
import json
import os
from pathlib import Path
import re
import subprocess
import sys
import tempfile
from typing import Any


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from logistics_runtime_profile import assert_path_has_no_reparse_components  # noqa: E402
from protected_admin import (  # noqa: E402
    MAX_PROTECTED_ADMIN_PROFILE_BYTES,
    PROTECTED_ADMIN_PROFILE_SCHEMA,
    PROTECTED_ADMIN_ROLE,
    ProtectedAdminProfileError,
    build_protected_admin_profile,
    default_protected_admin_profile_path,
    load_protected_admin_profile,
)

__all__ = [
    "build_parser",
    "install_protected_admin_profile",
    "load_installed_profile",
    "main",
]


_QUALIFIED_ACCOUNT_RE = re.compile(
    r"^(?:\.|[A-Za-z0-9][A-Za-z0-9_.-]{0,62})"
    r"\\[A-Za-z0-9][A-Za-z0-9_.@-]{0,127}$"
)
_UPN_RE = re.compile(
    r"^[A-Za-z0-9][A-Za-z0-9_.-]{0,63}"
    r"@[A-Za-z0-9][A-Za-z0-9.-]{0,190}$"
)
_SID_RE = re.compile(r"^S-1-[0-9]+(?:-[0-9]+)+$")
_READ_AND_EXECUTE_WITH_SYNCHRONIZE = 1_179_817
_WINDOWS_POWERSHELL_SECURITY_BOOTSTRAP = r"""
$trustedModuleRoot = [System.IO.Path]::GetFullPath(
    $env:KMTECH_PROTECTED_POWERSHELL_MODULE_ROOT
).TrimEnd('\')
if (-not [System.IO.Directory]::Exists($trustedModuleRoot)) {
    throw 'trusted in-box Windows PowerShell module root is missing'
}
$env:PSModulePath = $trustedModuleRoot
$securityManifest = [System.IO.Path]::GetFullPath(
    $env:KMTECH_PROTECTED_POWERSHELL_SECURITY_MANIFEST
)
if (-not [System.IO.File]::Exists($securityManifest)) {
    throw 'trusted in-box Microsoft.PowerShell.Security manifest is missing'
}
Import-Module -Name $securityManifest -Force -ErrorAction Stop
$securityModules = @(Get-Module -Name Microsoft.PowerShell.Security)
if ($securityModules.Count -ne 1) {
    throw 'trusted in-box Microsoft.PowerShell.Security module is ambiguous'
}
$actualManifest = [System.IO.Path]::GetFullPath($securityModules[0].Path)
if (-not [string]::Equals(
    $actualManifest,
    $securityManifest,
    [System.StringComparison]::OrdinalIgnoreCase
)) {
    throw 'Microsoft.PowerShell.Security resolved outside the trusted in-box module root'
}
$aclCommands = @(Get-Command -Name Get-Acl -CommandType Cmdlet -ErrorAction Stop)
if (
    $aclCommands.Count -ne 1 -or
    $aclCommands[0].ModuleName -cne 'Microsoft.PowerShell.Security'
) {
    throw 'Get-Acl does not resolve to the trusted in-box security module'
}
"""
_BROAD_READER_NAMES = frozenset(
    {
        "anonymous logon",
        "administrators",
        "authenticated users",
        "builtin\\administrators",
        "builtin\\guests",
        "builtin\\users",
        "creator owner",
        "domain admins",
        "domain computers",
        "domain guests",
        "domain users",
        "enterprise admins",
        "everyone",
        "guests",
        "interactive",
        "local service",
        "network",
        "network service",
        "service",
        "system",
        "users",
    }
)

_APPLY_EXACT_ACL_SCRIPT = r"""
$ErrorActionPreference = 'Stop'
$target = [System.IO.Path]::GetFullPath($env:KMTECH_PROTECTED_ACL_TARGET)
$reader = New-Object System.Security.Principal.NTAccount($env:KMTECH_PROTECTED_ACL_READER)
$readerSid = $reader.Translate([System.Security.Principal.SecurityIdentifier])
$readerSidValue = $readerSid.Value
$blockedSids = @(
    'S-1-1-0',
    'S-1-5-2',
    'S-1-5-4',
    'S-1-5-6',
    'S-1-5-7',
    'S-1-5-11',
    'S-1-5-18',
    'S-1-5-19',
    'S-1-5-20',
    'S-1-5-32-544',
    'S-1-5-32-545',
    'S-1-5-32-546'
)
if (
    $blockedSids -contains $readerSidValue -or
    $readerSidValue -match '-(512|513|514|515|519)$'
) {
    throw 'reader principal resolves to a broad or privileged identity'
}
$escapedReaderSid = $readerSidValue.Replace("'", "''")
$readerUsers = @(
    Get-CimInstance -ClassName Win32_UserAccount -Filter "SID='$escapedReaderSid'"
)
if ($readerUsers.Count -ne 1) {
    throw 'reader principal must resolve to exactly one Windows user account'
}

$systemSid = New-Object System.Security.Principal.SecurityIdentifier('S-1-5-18')
$administratorsSid = New-Object System.Security.Principal.SecurityIdentifier('S-1-5-32-544')
$allow = [System.Security.AccessControl.AccessControlType]::Allow
$fullControl = [System.Security.AccessControl.FileSystemRights]::FullControl
$readOnly = [System.Security.AccessControl.FileSystemRights]::ReadAndExecute
$noPropagation = [System.Security.AccessControl.PropagationFlags]::None
$isDirectory = $env:KMTECH_PROTECTED_ACL_DIRECTORY -eq '1'

if ($isDirectory) {
    $security = New-Object System.Security.AccessControl.DirectorySecurity
    $inheritance = (
        [System.Security.AccessControl.InheritanceFlags]::ContainerInherit -bor
        [System.Security.AccessControl.InheritanceFlags]::ObjectInherit
    )
    $security.SetAccessRuleProtection($true, $false)
    foreach ($identity in @($systemSid, $administratorsSid)) {
        $security.AddAccessRule(
            [System.Security.AccessControl.FileSystemAccessRule]::new(
                $identity, $fullControl, $inheritance, $noPropagation, $allow
            )
        )
    }
    $security.AddAccessRule(
        [System.Security.AccessControl.FileSystemAccessRule]::new(
            $readerSid, $readOnly, $inheritance, $noPropagation, $allow
        )
    )
    [System.IO.Directory]::SetAccessControl($target, $security)
} else {
    $security = New-Object System.Security.AccessControl.FileSecurity
    $security.SetAccessRuleProtection($true, $false)
    foreach ($identity in @($systemSid, $administratorsSid)) {
        $security.AddAccessRule(
            [System.Security.AccessControl.FileSystemAccessRule]::new(
                $identity, $fullControl, $allow
            )
        )
    }
    $security.AddAccessRule(
        [System.Security.AccessControl.FileSystemAccessRule]::new(
            $readerSid, $readOnly, $allow
        )
    )
    [System.IO.File]::SetAccessControl($target, $security)
}
$readerSidValue
"""

_READ_EXACT_ACL_SCRIPT = r"""
$ErrorActionPreference = 'Stop'
$target = [System.IO.Path]::GetFullPath($env:KMTECH_PROTECTED_ACL_TARGET)
$acl = Get-Acl -LiteralPath $target
$entries = @(
    $acl.Access | ForEach-Object {
        [PSCustomObject]@{
            sid = $_.IdentityReference.Translate(
                [System.Security.Principal.SecurityIdentifier]
            ).Value
            access_type = $_.AccessControlType.ToString()
            rights = [int64]$_.FileSystemRights
            inheritance = [int]$_.InheritanceFlags
            propagation = [int]$_.PropagationFlags
            inherited = [bool]$_.IsInherited
        }
    }
)
[PSCustomObject]@{
    protected = [bool]$acl.AreAccessRulesProtected
    entries = $entries
} | ConvertTo-Json -Compress -Depth 4
"""


def _checked_path(value: str | os.PathLike[str], *, label: str) -> Path:
    try:
        return assert_path_has_no_reparse_components(value, label=label)
    except Exception as exc:
        raise ProtectedAdminProfileError(f"{label} path is unsafe") from exc


def _validate_reader_principal(reader_principal: object) -> str:
    principal = str(reader_principal or "").strip()
    if not principal or not (
        _QUALIFIED_ACCOUNT_RE.fullmatch(principal) or _UPN_RE.fullmatch(principal)
    ):
        raise ValueError("reader_principal must be one qualified narrow account")
    normalized = principal.casefold()
    leaf = normalized.rsplit("\\", 1)[-1].split("@", 1)[0]
    if normalized in _BROAD_READER_NAMES or leaf in _BROAD_READER_NAMES:
        raise ValueError("reader_principal must not be broad or privileged")
    return principal


def _trusted_windows_powershell_authority() -> tuple[Path, Path, Path, Path]:
    if os.name != "nt":
        raise ProtectedAdminProfileError(
            "trusted Windows PowerShell authority is available only on Windows"
        )
    try:
        import ctypes

        buffer = ctypes.create_unicode_buffer(32_768)
        length = int(
            ctypes.windll.kernel32.GetSystemDirectoryW(buffer, len(buffer))
        )
    except (AttributeError, OSError, TypeError, ValueError) as exc:
        raise ProtectedAdminProfileError(
            "trusted Windows system directory could not be resolved"
        ) from exc
    if length <= 0 or length >= len(buffer):
        raise ProtectedAdminProfileError(
            "trusted Windows system directory could not be resolved"
        )
    system_directory = Path(buffer.value)
    if (
        not system_directory.is_absolute()
        or system_directory.name.casefold() != "system32"
        or not system_directory.is_dir()
    ):
        raise ProtectedAdminProfileError(
            "trusted Windows system directory is invalid"
        )
    windows_root = system_directory.parent
    powershell_home = system_directory / "WindowsPowerShell" / "v1.0"
    powershell_executable = powershell_home / "powershell.exe"
    module_root = powershell_home / "Modules"
    security_manifest = (
        module_root
        / "Microsoft.PowerShell.Security"
        / "Microsoft.PowerShell.Security.psd1"
    )
    if (
        not powershell_executable.is_file()
        or not module_root.is_dir()
        or not security_manifest.is_file()
    ):
        raise ProtectedAdminProfileError(
            "trusted in-box Windows PowerShell security authority is missing"
        )
    return (
        powershell_executable,
        module_root,
        security_manifest,
        windows_root,
    )


def _run_powershell(
    script: str,
    *,
    path: Path,
    reader_principal: str = "",
    is_directory: bool = False,
) -> str:
    if os.name != "nt":
        raise ProtectedAdminProfileError(
            "protected administrator ACL hardening is available only on Windows"
        )
    (
        powershell_executable,
        module_root,
        security_manifest,
        windows_root,
    ) = _trusted_windows_powershell_authority()
    environment = os.environ.copy()
    environment["SystemRoot"] = str(windows_root)
    environment["WINDIR"] = str(windows_root)
    environment["PSModulePath"] = str(module_root)
    environment["KMTECH_PROTECTED_ACL_TARGET"] = str(path)
    environment["KMTECH_PROTECTED_ACL_READER"] = reader_principal
    environment["KMTECH_PROTECTED_ACL_DIRECTORY"] = "1" if is_directory else "0"
    environment["KMTECH_PROTECTED_POWERSHELL_SECURITY_MANIFEST"] = str(
        security_manifest
    )
    environment["KMTECH_PROTECTED_POWERSHELL_MODULE_ROOT"] = str(module_root)
    trusted_script = _WINDOWS_POWERSHELL_SECURITY_BOOTSTRAP + "\n" + script
    try:
        completed = subprocess.run(
            [
                str(powershell_executable),
                "-NoProfile",
                "-NonInteractive",
                "-ExecutionPolicy",
                "Bypass",
                "-Command",
                trusted_script,
            ],
            check=False,
            capture_output=True,
            text=True,
            timeout=20,
            env=environment,
            creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0),
        )
    except Exception as exc:
        raise ProtectedAdminProfileError(
            "protected administrator ACL operation failed"
        ) from exc
    if completed.returncode != 0:
        raise ProtectedAdminProfileError(
            "protected administrator ACL operation failed"
        )
    output = str(completed.stdout or "").strip()
    if len(output) > 32 * 1024:
        raise ProtectedAdminProfileError(
            "protected administrator ACL readback was unexpectedly large"
        )
    return output


def _apply_exact_acl(
    path: Path,
    reader_principal: str,
    *,
    is_directory: bool,
) -> str:
    principal = _validate_reader_principal(reader_principal)
    target = _checked_path(path, label="protected administrator ACL target")
    reader_sid = _run_powershell(
        _APPLY_EXACT_ACL_SCRIPT,
        path=target,
        reader_principal=principal,
        is_directory=is_directory,
    )
    if not _SID_RE.fullmatch(reader_sid):
        raise ProtectedAdminProfileError(
            "protected administrator reader SID resolution failed"
        )
    return reader_sid


def _assert_exact_acl(
    path: Path,
    reader_sid: str,
    *,
    is_directory: bool,
) -> None:
    target = _checked_path(path, label="protected administrator ACL target")
    raw = _run_powershell(
        _READ_EXACT_ACL_SCRIPT,
        path=target,
        is_directory=is_directory,
    )
    try:
        payload = json.loads(raw)
        entries = payload["entries"]
    except (KeyError, TypeError, json.JSONDecodeError) as exc:
        raise ProtectedAdminProfileError(
            "protected administrator ACL readback is invalid"
        ) from exc
    if payload.get("protected") is not True or not isinstance(entries, list):
        raise ProtectedAdminProfileError(
            "protected administrator ACL inheritance is not disabled"
        )

    inheritance = 3 if is_directory else 0
    expected = {
        "S-1-5-18": (2032127, inheritance),
        "S-1-5-32-544": (2032127, inheritance),
        reader_sid: (_READ_AND_EXECUTE_WITH_SYNCHRONIZE, inheritance),
    }
    actual: dict[str, tuple[int, int]] = {}
    for entry in entries:
        if not isinstance(entry, dict):
            raise ProtectedAdminProfileError(
                "protected administrator ACL entry is invalid"
            )
        sid = entry.get("sid")
        if (
            not isinstance(sid, str)
            or sid in actual
            or entry.get("access_type") != "Allow"
            or entry.get("inherited") is not False
            or type(entry.get("rights")) is not int
            or type(entry.get("inheritance")) is not int
            or entry.get("propagation") != 0
        ):
            raise ProtectedAdminProfileError(
                "protected administrator ACL contains an unexpected entry"
            )
        actual[sid] = (entry["rights"], entry["inheritance"])
    if actual != expected:
        raise ProtectedAdminProfileError(
            "protected administrator ACL does not match the exact allow-list"
        )


def _harden_profile_directory(path: Path, reader_principal: str) -> None:
    if os.name != "nt":
        raise ProtectedAdminProfileError(
            "protected administrator ACL hardening is available only on Windows"
        )
    principal = _validate_reader_principal(reader_principal)
    directory = _checked_path(
        path,
        label="protected administrator profile directory",
    )
    try:
        directory.mkdir(parents=True, exist_ok=True)
    except OSError as exc:
        raise ProtectedAdminProfileError(
            "protected administrator profile directory could not be created"
        ) from exc
    directory = _checked_path(
        directory,
        label="protected administrator profile directory",
    )
    if not directory.is_dir():
        raise ProtectedAdminProfileError(
            "protected administrator profile directory is invalid"
        )
    reader_sid = _apply_exact_acl(
        directory,
        principal,
        is_directory=True,
    )
    _checked_path(directory, label="protected administrator profile directory")
    _assert_exact_acl(directory, reader_sid, is_directory=True)


def _harden_profile_file(path: Path, reader_principal: str) -> None:
    if os.name != "nt":
        raise ProtectedAdminProfileError(
            "protected administrator ACL hardening is available only on Windows"
        )
    principal = _validate_reader_principal(reader_principal)
    target = _checked_path(path, label="protected administrator profile file")
    if not target.is_file():
        raise ProtectedAdminProfileError(
            "protected administrator profile file is invalid"
        )
    reader_sid = _apply_exact_acl(target, principal, is_directory=False)
    _checked_path(target, label="protected administrator profile file")
    _assert_exact_acl(target, reader_sid, is_directory=False)


def _new_empty_hardened_temp(path: Path, reader_principal: str) -> Path:
    target = _checked_path(path, label="protected administrator profile")
    try:
        descriptor, temporary_name = tempfile.mkstemp(
            prefix=f".{target.name}.",
            suffix=".tmp",
            dir=str(target.parent),
        )
        os.close(descriptor)
    except OSError as exc:
        raise ProtectedAdminProfileError(
            "protected administrator temporary file creation failed"
        ) from exc
    temporary = _checked_path(
        temporary_name,
        label="protected administrator temporary file",
    )
    try:
        if temporary.stat().st_size != 0:
            raise ProtectedAdminProfileError(
                "protected administrator temporary file was not empty"
            )
        _harden_profile_file(temporary, reader_principal)
        return temporary
    except Exception:
        try:
            temporary.unlink(missing_ok=True)
        except OSError:
            pass
        raise


def _prepare_hardened_temp(
    path: Path,
    data: bytes,
    reader_principal: str,
) -> Path:
    temporary = _new_empty_hardened_temp(path, reader_principal)
    try:
        _checked_path(temporary, label="protected administrator temporary file")
        with open(temporary, "r+b") as handle:
            handle.truncate(0)
            handle.write(data)
            handle.flush()
            os.fsync(handle.fileno())
        _checked_path(temporary, label="protected administrator temporary file")
        return temporary
    except Exception:
        try:
            temporary.unlink(missing_ok=True)
        except OSError:
            pass
        raise


def _atomic_write(path: Path, data: bytes, reader_principal: str) -> None:
    temporary = _prepare_hardened_temp(path, data, reader_principal)
    try:
        _checked_path(path, label="protected administrator profile")
        _checked_path(temporary, label="protected administrator temporary file")
        os.replace(temporary, path)
        _checked_path(path, label="protected administrator profile")
    finally:
        try:
            temporary.unlink(missing_ok=True)
        except OSError:
            pass


def load_installed_profile(
    path: str | os.PathLike[str],
    *,
    expected: dict[str, object] | None = None,
) -> dict[str, object]:
    payload = load_protected_admin_profile(path)
    if expected is not None and payload != expected:
        raise ProtectedAdminProfileError(
            "protected administrator profile exact readback failed"
        )
    return payload


def _read_exact_profile_bytes(path: Path, *, expected_size: int) -> bytes:
    target = _checked_path(path, label="protected administrator profile")
    if expected_size < 1 or expected_size > MAX_PROTECTED_ADMIN_PROFILE_BYTES:
        raise ProtectedAdminProfileError(
            "protected administrator expected profile size is invalid"
        )
    try:
        with target.open("rb") as handle:
            data = handle.read(expected_size + 1)
    except OSError as exc:
        raise ProtectedAdminProfileError(
            "protected administrator profile byte readback failed"
        ) from exc
    _checked_path(target, label="protected administrator profile")
    if len(data) != expected_size:
        raise ProtectedAdminProfileError(
            "protected administrator profile byte readback differs"
        )
    return data


def _remove_or_invalidate_failed_profile(
    target: Path,
    reader_principal: str,
) -> None:
    """Ensure a failed first install cannot leave an authenticating profile."""
    checked = _checked_path(target, label="protected administrator profile")
    try:
        checked.unlink(missing_ok=True)
    except OSError:
        tombstone = _new_empty_hardened_temp(checked, reader_principal)
        try:
            with tombstone.open("r+b") as handle:
                handle.truncate(0)
                handle.flush()
                os.fsync(handle.fileno())
            os.replace(tombstone, checked)
            _checked_path(checked, label="protected administrator profile")
            if checked.stat().st_size != 0:
                raise ProtectedAdminProfileError(
                    "protected administrator fail-closed marker is invalid"
                )
        finally:
            try:
                tombstone.unlink(missing_ok=True)
            except OSError:
                pass
    if checked.exists():
        try:
            load_installed_profile(checked)
        except ProtectedAdminProfileError:
            return
        raise ProtectedAdminProfileError(
            "protected administrator failed profile remains usable"
        )


def install_protected_admin_profile(
    candidate: object | None = None,
    *,
    profile_path: str | os.PathLike[str] | None = None,
    reader_principal: str = "",
    dry_run: bool = False,
    replace: bool = False,
) -> dict[str, Any]:
    target = _checked_path(
        profile_path or default_protected_admin_profile_path(),
        label="protected administrator profile",
    )
    if reader_principal or not dry_run:
        _validate_reader_principal(reader_principal)
    summary: dict[str, Any] = {
        "status": "dry-run" if dry_run else "installed",
        "schema_version": PROTECTED_ADMIN_PROFILE_SCHEMA,
        "role": PROTECTED_ADMIN_ROLE,
        "profile_path": str(target),
    }
    if dry_run:
        if candidate is not None:
            build_protected_admin_profile(candidate)
        return summary
    profile = build_protected_admin_profile(candidate)
    if target.exists() and not replace:
        raise FileExistsError(
            "protected administrator profile already exists; use --replace for intentional reprovisioning"
        )

    profile_data = (
        json.dumps(
            profile,
            ensure_ascii=False,
            sort_keys=True,
            indent=2,
        )
        + "\n"
    ).encode("utf-8")
    existing_valid_data: bytes | None = None
    existing_valid_profile: dict[str, object] | None = None

    backup: Path | None = None
    replaced_target = False
    try:
        _harden_profile_directory(target.parent, reader_principal)
        target = _checked_path(
            target,
            label="protected administrator profile",
        )
        if target.exists():
            if not replace:
                raise FileExistsError(
                    "protected administrator profile appeared during provisioning"
                )
            _harden_profile_file(target, reader_principal)
            try:
                existing_valid_profile = load_installed_profile(target)
                existing_valid_data = _read_exact_profile_bytes(
                    target,
                    expected_size=target.stat().st_size,
                )
                if load_installed_profile(target) != existing_valid_profile:
                    raise ProtectedAdminProfileError(
                        "protected administrator existing profile changed during backup"
                    )
            except (OSError, ProtectedAdminProfileError):
                existing_valid_data = None
                existing_valid_profile = None
        if existing_valid_data is not None:
            backup = _prepare_hardened_temp(
                target,
                existing_valid_data,
                reader_principal,
            )
            if existing_valid_profile is None:
                raise ProtectedAdminProfileError(
                    "protected administrator backup verification failed"
                )
            load_installed_profile(backup, expected=existing_valid_profile)
            if _read_exact_profile_bytes(
                backup,
                expected_size=len(existing_valid_data),
            ) != existing_valid_data:
                raise ProtectedAdminProfileError(
                    "protected administrator backup byte verification failed"
                )
        replaced_target = True
        _atomic_write(target, profile_data, reader_principal)
        _harden_profile_file(target, reader_principal)
        installed = load_installed_profile(target, expected=profile)
        if _read_exact_profile_bytes(
            target,
            expected_size=len(profile_data),
        ) != profile_data:
            raise ProtectedAdminProfileError(
                "protected administrator profile byte readback differs"
            )
    except Exception as exc:
        restore_error: Exception | None = None
        if replaced_target and backup is not None and backup.exists():
            try:
                os.replace(backup, target)
                backup = None
                _harden_profile_file(target, reader_principal)
                restored = load_installed_profile(target)
                if existing_valid_data is None or _read_exact_profile_bytes(
                    target,
                    expected_size=len(existing_valid_data),
                ) != existing_valid_data:
                    raise ProtectedAdminProfileError(
                        "protected administrator previous profile content was not restored"
                    )
                if restored is None:
                    raise ProtectedAdminProfileError(
                        "protected administrator previous profile was not restored"
                    )
            except Exception as restoration_exc:
                try:
                    _remove_or_invalidate_failed_profile(
                        target,
                        reader_principal,
                    )
                except Exception as invalidation_exc:
                    restore_error = invalidation_exc
                else:
                    restore_error = restoration_exc
        elif replaced_target:
            try:
                _remove_or_invalidate_failed_profile(target, reader_principal)
            except Exception as cleanup_exc:
                restore_error = cleanup_exc
        if restore_error is not None:
            raise ProtectedAdminProfileError(
                "protected administrator reprovisioning failed and restoration could not be verified"
            ) from exc
        raise
    finally:
        if backup is not None:
            try:
                backup.unlink(missing_ok=True)
            except OSError:
                pass
    return summary


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Install a machine-local offline administrator verifier.",
        allow_abbrev=False,
    )
    parser.add_argument("--profile-path", default=default_protected_admin_profile_path())
    parser.add_argument("--reader-principal", default="")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--replace", action="store_true")
    return parser


def main(argv: list[str] | None = None) -> int:
    args, unknown = build_parser().parse_known_args(argv)
    if unknown:
        print("BLOCKED: unsupported command-line argument", file=sys.stderr)
        return 2
    if args.dry_run:
        try:
            report = install_protected_admin_profile(
                None,
                profile_path=args.profile_path,
                reader_principal=args.reader_principal,
                dry_run=True,
                replace=args.replace,
            )
        except Exception as exc:
            print(f"BLOCKED: {exc.__class__.__name__}", file=sys.stderr)
            return 2
        print(json.dumps(report, ensure_ascii=False, sort_keys=True))
        return 0
    if not args.dry_run and not str(args.reader_principal or "").strip():
        print("BLOCKED: --reader-principal is required for an actual install", file=sys.stderr)
        return 2

    first_entry = ""
    confirmation = ""
    try:
        try:
            first_entry = getpass.getpass("Protected administrator code: ")
            confirmation = getpass.getpass("Confirm protected administrator code: ")
        except (AttributeError, EOFError, KeyboardInterrupt):
            print("BLOCKED: protected administrator hidden input failed", file=sys.stderr)
            return 2
        entries_match = hmac.compare_digest(
            first_entry.encode("utf-8"),
            confirmation.encode("utf-8"),
        )
        if not entries_match:
            print("BLOCKED: credential confirmation mismatch", file=sys.stderr)
            return 2
        try:
            report = install_protected_admin_profile(
                first_entry,
                profile_path=args.profile_path,
                reader_principal=args.reader_principal,
                dry_run=args.dry_run,
                replace=args.replace,
            )
        except Exception as exc:
            print(f"BLOCKED: {exc.__class__.__name__}", file=sys.stderr)
            return 2
    finally:
        first_entry = ""
        confirmation = ""
    print(json.dumps(report, ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
