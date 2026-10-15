"""Read-only installed startup observation; never import the GUI entrypoint."""
from __future__ import annotations

import ctypes
from datetime import datetime, timedelta, timezone
import hashlib
import io
import json
import os
from pathlib import Path
import re
import sys
import time
from contextlib import redirect_stderr, redirect_stdout

sys.dont_write_bytecode = True
if __name__ == "__main__":
    sys.path[:0] = [str(Path(__file__).absolute().parent),
                    str(Path(__file__).absolute().parent / "site-packages")]

REASON_CATEGORIES = {code: category for category, codes in (
    ("UPDATE_PENDING", ("UPDATE_PREPARED", "UPDATE_RECOVERY", "UPDATE_PHASE")),
    ("DEPLOY_FENCE", ("ACTIVE_WRITER_FENCE", "MACHINE_DEPLOY_FENCE")),
    ("INSTALL_MISMATCH", ("INSTALL_MANIFEST_MISMATCH", "INSTALL_VERSION_MISMATCH",
                          "ACTIVE_BUILD_MISMATCH", "INSTALL_RECORD_MISSING", "RELAY_AUTOSTART_MISMATCH")),
    ("DATA_GUARD", ("DATA_GENERATION_REFUSED",)),
    ("ACCOUNT_MISMATCH", ("OPERATOR_ACCOUNT_MISMATCH", "FIRST_RUN_SETUP_REQUIRED",
                          "POSSESSION_PROOF_REATTACH_REQUIRED", "ADMIN_RECOVERY_ACTION",
                          "PENDING_WORKER_SID_MISMATCH", "PENDING_WORKER_INSTALL_INVALID")),
    ("CATALOG_OFFLINE", ("CATALOG_OFFLINE_UNAVAILABLE",)),
    ("ALREADY_RUNNING", ("ALREADY_RUNNING",)),
    ("UNKNOWN", tuple("UNKNOWN_" + name for name in
                      ("INSTALL", "UPDATE", "FENCE", "DATA", "ACCOUNT", "CATALOG", "RELAY", "PROCESS", "CHECK", "SLOW_CHECK"))),
) for code in codes}


def startup_result(reasons=(), *, version="", build_sha=""):
    # Test-only observation override. Installers and updates never set this.
    forced = next((code for code, category in REASON_CATEGORIES.items()
                   if category == os.environ.get("KMTECH_START_CHECK_TEST_CATEGORY")), None)
    if forced is not None:
        reasons = (forced,)
    codes = list(dict.fromkeys(code if code in REASON_CATEGORIES else "UNKNOWN_CHECK" for code in reasons))
    blocking = [code for code in codes if REASON_CATEGORIES[code] != "ALREADY_RUNNING"]
    return {"schema": "app_startup_check.v1", "app": "LABEL",
            "version": version if re.fullmatch(r"v\d+\.\d+\.\d+", str(version)) else "",
            "build_sha": build_sha if re.fullmatch(r"[0-9a-f]{40}", str(build_sha)) else "",
            "startable": not blocking, "reasons": codes,
            "status": blocking[0] if blocking else "READY",
            "category": REASON_CATEGORIES[blocking[0]] if blocking else "READY",
            "categories": list(dict.fromkeys(REASON_CATEGORIES[code] for code in codes)),
            "checked_at": datetime.now(timezone(timedelta(hours=9))).isoformat(timespec="seconds")}


class SlowCheck(RuntimeError):
    pass


def _read(path, *, maximum=1024 * 1024):
    from auto_update import _assert_no_reparse_components, unique_object
    _assert_no_reparse_components(path)
    before = path.stat()
    if not 0 < before.st_size <= maximum:
        raise ValueError("bounded startup input size differs")
    with path.open("rb") as stream:
        raw = stream.read(maximum + 1)
    after = path.stat()
    if len(raw) != before.st_size or (before.st_size, before.st_mtime_ns) != (after.st_size, after.st_mtime_ns):
        raise ValueError("startup input changed")
    return json.loads(raw.decode("utf-8-sig"), object_pairs_hook=unique_object)


def _installed_identity(root, deadline):
    from current_user_onboarding import BOOTSTRAP_INTEGRITY_MAX_BYTES, validate_portable_bootstrap_inventory
    from label_data_generation import portable_release_identity
    from auto_update import _assert_no_reparse_components
    record = _read(root / "bootstrap-integrity.json", maximum=BOOTSTRAP_INTEGRITY_MAX_BYTES)
    rows = sorted(record["files"], key=lambda row: row["path"].encode("utf-16-be"))
    validate_portable_bootstrap_inventory(record, rows, root)
    files = {row["path"]: row for row in rows}
    if len(files) != len(rows):
        raise ValueError("duplicate inventory paths")
    # Compare the existing installed inventory and bounded startup files. Never
    # rehash the runtime DLLs or the complete package on an HDD startup probe.
    selected = ("portable-manifest.json", "app/main.py", "app/startup_check.py",
                "app/label_data_generation.py", "app/auto_update.py",
                "app/current_user_onboarding.py", "app/logistics_runtime_profile.py",
                "app/item_catalog_sync.py", "app/writer_session_fence.py",
                "app/user_relay.py", "app/startup_observation.py")
    manifest = None
    for name in selected:
        if time.monotonic() >= deadline:
            raise SlowCheck()
        row = files[name]
        path = root / name
        _assert_no_reparse_components(path)
        if type(row["size"]) is not int or not 0 <= row["size"] <= 256 * 1024:
            raise SlowCheck()
        before = path.stat()
        with path.open("rb") as stream:
            raw = stream.read(256 * 1024 + 1)
        after = path.stat()
        if (len(raw) != row["size"] or hashlib.sha256(raw).hexdigest() != row["sha256"]
                or (before.st_size, before.st_mtime_ns) != (after.st_size, after.st_mtime_ns)):
            raise ValueError("installed startup file differs")
        if name == "portable-manifest.json":
            from auto_update import unique_object
            manifest = json.loads(raw.decode("utf-8-sig"), object_pairs_hook=unique_object)
    return portable_release_identity(manifest)


def _bounded_inputs(paths):
    from auto_update import _assert_no_reparse_components
    for path in paths:
        _assert_no_reparse_components(path)
        try:
            if path.stat().st_size > 1024 * 1024:
                raise SlowCheck()
            with path.open("rb") as stream:
                stream.read(1)  # A swallowed cache-reader error must remain UNKNOWN.
        except FileNotFoundError:
            pass


def _already_running(root):
    """Read process images/command lines through Win32, without opening a mutex."""
    from ctypes import wintypes as w
    kernel = ctypes.WinDLL("kernel32", use_last_error=True)
    psapi = ctypes.WinDLL("psapi", use_last_error=True)
    query = ctypes.WinDLL("ntdll").NtQueryInformationProcess
    kernel.OpenProcess.argtypes = [w.DWORD, w.BOOL, w.DWORD]
    kernel.OpenProcess.restype = w.HANDLE
    kernel.CloseHandle.argtypes = [w.HANDLE]
    kernel.QueryFullProcessImageNameW.argtypes = [w.HANDLE, w.DWORD, w.LPWSTR, ctypes.POINTER(w.DWORD)]
    query.argtypes = [w.HANDLE, w.ULONG, w.LPVOID, w.ULONG, ctypes.POINTER(w.ULONG)]
    query.restype = ctypes.c_long
    class UnicodeString(ctypes.Structure):
        _fields_ = [("length", w.USHORT), ("maximum", w.USHORT), ("buffer", w.LPVOID)]
    pids, used = (w.DWORD * 4096)(), w.DWORD()
    psapi.EnumProcesses.argtypes = [ctypes.POINTER(w.DWORD), w.DWORD, ctypes.POINTER(w.DWORD)]
    if not psapi.EnumProcesses(pids, ctypes.sizeof(pids), ctypes.byref(used)) or used.value == ctypes.sizeof(pids):
        raise OSError("process list unavailable")
    expected = {os.path.normcase(str(root / "runtime" / name)) for name in ("python.exe", "pythonw.exe")}
    for pid in pids[:used.value // ctypes.sizeof(w.DWORD)]:
        if pid == os.getpid():
            continue
        handle = kernel.OpenProcess(0x1000, False, pid)
        if not handle:
            continue
        try:
            image, length = ctypes.create_unicode_buffer(32768), w.DWORD(32768)
            if not kernel.QueryFullProcessImageNameW(handle, 0, image, ctypes.byref(length)) or os.path.normcase(image.value) not in expected:
                continue
            buffer, required = ctypes.create_string_buffer(65536), w.ULONG()
            if query(handle, 60, buffer, len(buffer), ctypes.byref(required)) < 0:
                raise OSError("product command line unavailable")
            text = UnicodeString.from_buffer(buffer)
            command = ctypes.wstring_at(text.buffer, text.length // 2)
            if any(str(root / "app" / entry).casefold() in command.casefold() for entry in ("main.py", "Label_Match.py")) and "--label-match-" not in command:
                return True
        finally:
            kernel.CloseHandle(handle)
    return False


def check_startup(app_root=None):
    from auto_update import control_root, installation_root, load_policy, optional_record, prepared_pending, read_json, ordinary_user_context, startup_bootstrap_reason
    from current_user_onboarding import inspect_current_user_state, resolve_current_user_onboarding_paths, validate_pending_worker_install
    from label_data_generation import CURRENT_VERSION, DataGenerationError, UNKNOWN_MESSAGE, _check_user_data_open
    from logistics_runtime_profile import load_logistics_runtime_profile, legacy_label_transition_enabled
    import item_catalog_sync as catalog
    import writer_session_fence as fence
    from user_relay import _registry_get, user_relay_command_line
    root = Path(app_root) if app_root is not None else Path(__file__).absolute().parent.parent
    root = root.absolute()
    deadline, reasons, identity = time.monotonic() + 8, [], {}
    def probe(stage, action):
        if time.monotonic() >= deadline:
            reasons.append("UNKNOWN_SLOW_CHECK")
            return None
        try:
            value = action()
            if time.monotonic() >= deadline:
                raise SlowCheck()
            return value
        except SlowCheck:
            reasons.append("UNKNOWN_SLOW_CHECK")
        except Exception:
            reasons.append("UNKNOWN_" + stage)
        return None
    def install():
        nonlocal identity
        try:
            identity = _installed_identity(root, deadline)
        except DataGenerationError:
            reasons.append("INSTALL_VERSION_MISMATCH")
        except (ValueError, KeyError, TypeError):
            reasons.append("INSTALL_MANIFEST_MISMATCH")
    probe("INSTALL", install)
    code = root / "app"
    control = probe("UPDATE", lambda: control_root(code))
    def update():
        if control is None:
            reasons.append("INSTALL_RECORD_MISSING")
            return
        policy = load_policy(control)
        if not ordinary_user_context(policy["operator_sid"]):
            reasons.append("OPERATOR_ACCOUNT_MISMATCH")
        if identity:
            active, state = read_json(control / "active.json"), optional_record(control / "transition.json")
            reason = startup_bootstrap_reason(str(identity["build_id"]), active, state, prepared_pending(control, policy, state))
            if read_json(control / "active.json") != active or optional_record(control / "transition.json") != state:
                raise ValueError("startup transition changed")
            if reason:
                reasons.append(reason)
    probe("UPDATE", update)
    def fences():
        if fence.active_fence() is not None:
            reasons.append("ACTIVE_WRITER_FENCE")
        managed = installation_root(code)
        if managed:
            for path in managed.glob(".installer-owner-*.json"):
                owner = _read(path)
                if fence._normalized_control_root(owner["control_root"]) == fence._normalized_control_root(fence.canonical_control_root()) and fence.active_fence(owner["control_root"]) is not None:
                    reasons.append("MACHINE_DEPLOY_FENCE")
    probe("FENCE", fences)
    paths = probe("ACCOUNT", lambda: resolve_current_user_onboarding_paths(root))
    def admit(_root, roots=(), *, environ=None):
        if not identity:
            raise ValueError("installed identity unavailable")
        return _check_user_data_open(root, roots, environ=environ, identity_resolver=lambda _root: identity)
    if paths and identity:
        def data():
            try:
                admit(root, (paths.data_root, paths.ledger_path.parent, paths.direct_sync_root, paths.queue_dir, paths.logistics_profile_path.parent))
            except DataGenerationError as error:
                reasons.append("DATA_GENERATION_REFUSED")
                if str(error) == UNKNOWN_MESSAGE or error.private_cause is not None or error.__cause__ is not None:
                    reasons.append("UNKNOWN_DATA")
        probe("DATA", data)
    profile = probe("CATALOG", lambda: load_logistics_runtime_profile(data_admission=admit)) if identity else None
    def account():
        try:
            if validate_pending_worker_install(root) is not None:
                reasons.append("FIRST_RUN_SETUP_REQUIRED")
        except Exception as error:
            reasons.append(getattr(error, "cause_code", "UNKNOWN_ACCOUNT"))
        if paths:
            _bounded_inputs((paths.settings_path, paths.identity_path, paths.producer_manifest_path,
                             paths.credential_path, paths.registration_report_path, paths.logistics_secret_path))
            if paths.settings_path.is_file():
                if not isinstance(_read(paths.settings_path), dict):
                    raise ValueError("settings are not an object")
        if not paths or not paths.settings_path.is_file() or inspect_current_user_state(paths, profile_loader=lambda _path: profile)["status"] != "READY":
            reasons.append("FIRST_RUN_SETUP_REQUIRED")
    probe("ACCOUNT", account)
    def offline():
        cache = catalog.default_cache_path()
        _bounded_inputs((cache, catalog._cache_authority_path(cache), catalog._cache_recovery_path(cache), root / "app/assets/Item.csv"))
        authenticated = False
        if profile is not None:
            url = str(profile.base_url).rstrip("/") + catalog.CATALOG_PATH
            if catalog._is_trusted_authenticated_catalog_url(url, profile):
                args = dict(url=url, source_host_id=str(profile.source_host_id), device_id=str(profile.device_id), bearer_token=str(profile.bearer_token).strip())
                authenticated = catalog._read_authenticated_cache_payload(cache, **args) is not None or catalog._read_authenticated_recovery_payload(cache, **args) is not None
        legacy = legacy_label_transition_enabled()
        local = catalog._is_valid_catalog(cache) or catalog._is_valid_catalog(root / "app/assets/Item.csv")
        if not catalog.offline_catalog_startable(authenticated=authenticated, enrolled=profile is not None, legacy=legacy) or not (authenticated or local or legacy):
            reasons.append("CATALOG_OFFLINE_UNAVAILABLE")
    probe("CATALOG", offline)
    probe("RELAY", lambda: reasons.append("RELAY_AUTOSTART_MISMATCH") if _registry_get() != user_relay_command_line(root) else None)
    if probe("PROCESS", lambda: _already_running(root)):
        reasons.append("ALREADY_RUNNING")
    return startup_result(reasons, version=CURRENT_VERSION, build_sha=identity.get("build_id", ""))


def main(arguments=None):
    try:
        with redirect_stdout(io.StringIO()), redirect_stderr(io.StringIO()):
            result = check_startup() if list(sys.argv[1:] if arguments is None else arguments) == ["--json"] else startup_result(["UNKNOWN_CHECK"])
    except Exception:
        result = startup_result(["UNKNOWN_CHECK"])
    print(json.dumps(result, ensure_ascii=True, separators=(",", ":")))
    return 0 if result["startable"] else 20 if "UNKNOWN" in result["categories"] else 10


if __name__ == "__main__":
    raise SystemExit(main())
