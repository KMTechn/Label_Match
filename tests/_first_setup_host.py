"""Private OS adapters for the headless first-setup process-chain tests.

Product policy, manifests, writer admission, update mutex, onboarding, relay
cycles, and owner readback are executed from the isolated packet. Only native
identity, external helpers/tasks, credential protection, and HTTP are virtual.
"""
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import time
from types import SimpleNamespace
import zipfile

ORIGIN = "https://setup.test.invalid"
SID = "S-1-5-21-100-200-300-1001"


def write(path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, sort_keys=True), encoding="utf-8")


def _observation_path(root, pid):
    return root / "relay-lifecycle-observations" / (hashlib.sha256(str(pid).encode()).hexdigest() + ".json")


def _write_observation(path, value):
    import uuid
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(path.name + "." + uuid.uuid4().hex + ".tmp")
    temporary.write_text(json.dumps(value, sort_keys=True), encoding="utf-8")
    os.replace(temporary, path)


def _run_with_trace(callback, trace, *args, **kwargs):
    previous = sys.gettrace()
    sys.settrace(trace)
    try:
        return callback(*args, **kwargs)
    finally:
        sys.settrace(previous)


def _host_load_snapshot(config):
    try:
        selected = json.loads(Path(config).read_bytes()).get("host_load_snapshot")
        if not selected:
            return {"available": False}
        path = Path(selected)
        if path.stat().st_size > 16384:
            return {"available": False, "error_type": "OversizedObservation"}
        value = json.loads(path.read_bytes())
        value["snapshot_age_seconds"] = round(time.monotonic() - value.pop("monotonic"), 3)
        return {"available": True, **value}
    except Exception as error:
        return {"available": False, "error_type": type(error).__name__}


def _claim_update_observe_delay(root, state, active, claim, seconds):
    """Claim one delayed read of an existing applied-candidate trial."""
    if (not state or state.get("phase") != "APPLIED_PENDING_READY" or not claim
            or not state.get("nonce") or not state.get("transition_id")
            or not state.get("candidate_sha") or not state.get("applied_at")
            or claim.get("nonce") != state.get("nonce")
            or claim.get("transition_id") != state.get("transition_id")
            or claim.get("role") not in {"relay", "gui"}
            or not isinstance(claim.get("pid"), int) or isinstance(claim["pid"], bool)
            or claim["pid"] <= 0 or not active
            or active.get("sha") != state.get("candidate_sha")):
        return None
    try:
        with (Path(root) / "update-slow-observe-started").open("x", encoding="utf-8"):
            pass
    except FileExistsError:
        return None
    return dict(phase=state["phase"], candidate_pid=claim["pid"], requested_seconds=seconds,
                applied_at=state["applied_at"], nonce_match=True,
                transition_id_match=True, active_candidate_match=True)


def _wait_observed_sync_child(process, config, timeout):
    """Observe the existing wait without changing its timeout or child lifetime."""
    import threading
    import uuid
    path = Path(config).parent / "direct-sync-child-observations" / (uuid.uuid4().hex + ".json")
    began = time.monotonic()
    record = {"timeout_seconds": timeout, "host_before": _host_load_snapshot(config)}
    try:
        creation = _native_process_times(process)["creation_identity"]
    except Exception as error:
        creation = None
        record["native_identity_error_type"] = type(error).__name__
    try:
        result = process.wait(timeout=timeout)
        record["wait_return_code"] = result
        return result
    except BaseException as error:
        record["wait_error_type"] = type(error).__name__
        raise
    finally:
        ended_ns = time.time_ns()
        record.update(elapsed_seconds=round(time.monotonic() - began, 6),
                      host_after=_host_load_snapshot(config))
        try:
            record["native_at_wait_end"] = _native_guard_process(
                process, creation, process.pid, ended_ns)
            _write_observation(path, record)
            if record["native_at_wait_end"].get("native_alive"):
                def observe_exit():
                    # Keep the original Popen handle; never signal or reap a child.
                    while True:
                        try:
                            native = _native_guard_process(process, creation, process.pid, ended_ns)
                            if not native.get("native_alive"):
                                record.update(native_after_wait=native,
                                              host_at_later_exit=_host_load_snapshot(config),
                                              last_fixed_child_log=_fixed_child_log(process))
                                _write_observation(path, record)
                                return
                        except Exception:
                            return
                        time.sleep(0.25)
                threading.Thread(target=observe_exit, daemon=True).start()
        except Exception:
            pass


def _native_process_times(process):
    """Query only the original Popen handle, including after its child exits."""
    import ctypes
    from ctypes import wintypes
    kernel = ctypes.WinDLL("kernel32", use_last_error=True)
    handle = wintypes.HANDLE(int(process._handle))
    kernel.GetProcessId.argtypes = [wintypes.HANDLE]
    kernel.GetProcessId.restype = wintypes.DWORD
    kernel.GetProcessTimes.argtypes = [wintypes.HANDLE, *([ctypes.POINTER(wintypes.FILETIME)] * 4)]
    kernel.GetProcessTimes.restype = wintypes.BOOL
    kernel.GetExitCodeProcess.argtypes = [wintypes.HANDLE, ctypes.POINTER(wintypes.DWORD)]
    kernel.GetExitCodeProcess.restype = wintypes.BOOL
    kernel.WaitForSingleObject.argtypes = [wintypes.HANDLE, wintypes.DWORD]
    kernel.WaitForSingleObject.restype = wintypes.DWORD
    born, exited, kernel_time, user_time = (wintypes.FILETIME() for _ in range(4))
    if not kernel.GetProcessTimes(handle, ctypes.byref(born), ctypes.byref(exited),
                                  ctypes.byref(kernel_time), ctypes.byref(user_time)):
        raise ctypes.WinError(ctypes.get_last_error())
    pid = kernel.GetProcessId(handle)
    if not pid:
        raise ctypes.WinError(ctypes.get_last_error())
    wait = kernel.WaitForSingleObject(handle, 0)
    if wait not in (0, 258):
        raise ctypes.WinError(ctypes.get_last_error())
    code = wintypes.DWORD()
    if not kernel.GetExitCodeProcess(handle, ctypes.byref(code)):
        raise ctypes.WinError(ctypes.get_last_error())
    ticks = lambda value: (value.dwHighDateTime << 32) | value.dwLowDateTime
    birth_ns = ticks(born) * 100 - 11644473600000000000
    exit_ns = None if wait == 258 else ticks(exited) * 100 - 11644473600000000000
    return {"pid": pid, "creation_identity": ticks(born), "alive": wait == 258,
            "exit_code": None if wait == 258 else code.value,
            "exit_time_unix_ns": exit_ns,
            "cpu_kernel_seconds": ticks(kernel_time) / 10_000_000,
            "cpu_user_seconds": ticks(user_time) / 10_000_000,
            "cpu_seconds": (ticks(kernel_time) + ticks(user_time)) / 10_000_000,
            "wall_seconds": ((time.time_ns() if exit_ns is None else exit_ns) - birth_ns) / 1_000_000_000}


def _native_guard_process(process, expected_creation, expected_pid, guard_ns):
    state = _native_process_times(process)
    return {"native_alive": state["alive"],
            "native_pid_creation_matches": state["pid"] == expected_pid
                and state["creation_identity"] == expected_creation,
            "native_exit_code": state["exit_code"],
            "native_cpu_seconds": state["cpu_seconds"],
            "native_cpu_kernel_seconds": state["cpu_kernel_seconds"],
            "native_cpu_user_seconds": state["cpu_user_seconds"],
            "native_wall_seconds": state["wall_seconds"],
            "native_exit_guard_seconds": None if state["alive"] else
                round((state["exit_time_unix_ns"] - guard_ns) / 1_000_000_000, 6)}


def _fixed_child_log(process):
    import re
    path = process.host_test_output
    size = path.stat().st_size
    with path.open("rb") as stream:
        stream.seek(max(0, size - 65536))
        raw = stream.read(65536)
    text = raw.decode("utf-8", errors="replace")
    marker = None
    for line in text.splitlines():
        try:
            record = json.loads(line)
        except ValueError:
            continue
        code = record.get("reason_code") if isinstance(record, dict) else None
        if isinstance(code, str) and re.fullmatch(r"[A-Z][A-Z0-9_]{0,95}", code):
            marker = code
    frames = [{"file": Path(file).name, "function": function, "line": int(line)}
              for file, line, function in re.findall(
                  r'File "([^"\r\n]+)", line (\d+), in ([A-Za-z_][A-Za-z_0-9]*|<module>)', text)
              if Path(file).name in {"auto_update.py", "user_relay.py", "label_match_relay_entry.py",
                  "label_match_product_host.py", "writer_session_fence.py", "label_data_generation.py"}]
    return {"last_fixed_marker": marker, "product_frames": frames,
            "log_tail_truncated": size > len(raw), "log_tail_sha256": hashlib.sha256(raw).hexdigest()}


def inventory(root):
    rows = []
    for path in sorted(root.rglob("*"),
                       key=lambda item: item.relative_to(root).as_posix().encode("utf-16-be")):
        if not path.is_file() or path == root / "bootstrap-integrity.json":
            continue
        rows.append({"path": path.relative_to(root).as_posix(), "size": path.stat().st_size,
                     "sha256": hashlib.file_digest(path.open("rb"), "sha256").hexdigest()})
    aggregate = hashlib.sha256("".join(
        f"{row['sha256']} {row['size']} {row['path']}\n" for row in rows).encode()).hexdigest()
    target = root / "bootstrap-integrity.json"
    write(target, {"schema_version": "label-match-bootstrap-integrity-v1", "status": "PASS",
                   "code_root": str(root.resolve()), "installed_at": datetime.now(timezone.utc).isoformat(),
                   "file_count": len(rows), "files": rows,
                   "aggregate_sha256": aggregate, "identity_profile_created": False,
                   "state_scope": "current_user_first_run"})
    return target


def relocate_ancestor_boundary(bundle, root):
    """Map only the OS's canonical ancestor names into the isolated fixture."""
    path = bundle / "tools/bootstrap_integrity.ps1"
    raw = path.read_text(encoding="utf-8-sig")
    first = raw.index("function Protect-KmtechCodeAncestors(")
    last = raw.index("\nfunction ", first + 1)
    body = raw[first:last]
    assert "C:\\KMTech\\Apps" in body
    body = body.replace("C:\\KMTech\\Apps", str(root))
    body = body.replace("C:\\KMTech", str(root.parent))
    path.write_text(raw[:first] + body + raw[last:], encoding="utf-8-sig")
    manifest = json.loads((bundle / "portable-manifest.json").read_bytes())
    manifest["integrity_helper_sha256"] = hashlib.sha256(path.read_bytes()).hexdigest()
    helper = bundle / "tools/apply_prepared_update.ps1"
    source = helper.read_text(encoding="utf-8-sig")
    native_identity = "$sid = [Security.Principal.WindowsIdentity]::GetCurrent().User.Value"
    assert source.count(native_identity) == 1
    source = source.replace(native_identity,
        "$sid = if ($env:LABEL_MATCH_HOST_TEST_SYSTEM -ceq '1') { 'S-1-5-18' } else { '" + SID + "' }")
    helper.write_text(source, encoding="utf-8-sig")
    manifest["startup_update_helper_sha256"] = hashlib.sha256(helper.read_bytes()).hexdigest()
    write(bundle / "portable-manifest.json", manifest)


def native_acl(root):
    text = r'''
function Get-Acl {
    param($LiteralPath)
    $acl=[Security.AccessControl.DirectorySecurity]::new()
    $acl.SetOwner([Security.Principal.SecurityIdentifier]::new('S-1-5-32-544'))
    $acl.SetGroup([Security.Principal.SecurityIdentifier]::new('S-1-5-32-544'))
    if ([IO.Path]::GetFullPath($LiteralPath) -in @('ROOT','PARENT')) {
        $acl.SetSecurityDescriptorSddlForm('O:BAG:BAD:P(A;OICI;FA;;;SY)(A;OICI;FA;;;BA)(A;OICI;0x1200a9;;;BU)(A;OICIIO;0xe0010000;;;AU)(A;;0x1200af;;;AU)')
        return $acl
    }
    $acl.SetAccessRuleProtection($true,$false)
    foreach($sid in @('S-1-5-18','S-1-5-32-544')) {
        $acl.AddAccessRule([Security.AccessControl.FileSystemAccessRule]::new(
            [Security.Principal.SecurityIdentifier]::new($sid),'FullControl','Allow'))
    }
    return $acl
}
function Set-Acl { param($LiteralPath,$AclObject) }
Export-ModuleMember -Function Get-Acl,Set-Acl
'''
    (root / "native-boundary.psm1").write_text(
        text.replace("'ROOT'", "'" + str(root).replace("'", "''") + "'")
            .replace("'PARENT'", "'" + str(root.parent).replace("'", "''") + "'"), encoding="utf-8-sig")


def observe_script(helper, current, processes, *, diagnostic=None, target_pid=None):
    source = helper.read_text(encoding="utf-8-sig")
    supervisor = source[source.index("function Invoke-LabelStartupReadOnlyQuery {"):
                        source.index("if ($SupportedLifecycle -and")]
    query = source[source.index("function Get-StartupUpdateProcesses("):
                   source.index("function Assert-StartupUpdateVersion(")]
    adapters = r'''
    Add-Type 'public static class KMTechStartupUpdateToken { public static System.Collections.Generic.HashSet<uint> Elevated = new System.Collections.Generic.HashSet<uint>(); public static bool Ordinary(uint pid) { return !Elevated.Contains(pid); } }'
    $script:testProcesses = Get-Content -LiteralPath 'PROCESSES' -Raw | ConvertFrom-Json
    foreach ($item in $script:testProcesses) {
        if ($item.OrdinaryUser -ceq $false) { [void][KMTechStartupUpdateToken]::Elevated.Add([uint32]$item.ProcessId) }
    }
    function Get-CimInstance {
        $diagnostic = @{input_row_present=$false;native_live=$false;native_generation_matches=$false;emitted_row_present=$false}
        foreach ($item in $script:testProcesses) {
            $isTarget = [int]$item.ProcessId -eq DIAGNOSTICPID
            if ($isTarget) { $diagnostic.input_row_present=$true }
            $item.CreationDate = [datetime]$item.CreationDate
            $native=$null
            try {
                $native=[Diagnostics.Process]::GetProcessById([int]$item.ProcessId)
                $difference=$native.StartTime.ToUniversalTime().Ticks-$item.CreationDate.ToUniversalTime().Ticks
                if ($isTarget) {
                    $diagnostic.native_live=-not $native.HasExited
                    $diagnostic.native_generation_matches=$difference -ge 0 -and $difference -lt 10
                }
                if ($isTarget -and -not $native.HasExited -and $difference -ge 0 -and $difference -lt 10) { $diagnostic.emitted_row_present=$true }
                if (-not $native.HasExited -and $difference -ge 0 -and $difference -lt 10) { $item }
            } catch { if ($isTarget) { $diagnostic.native_error_type=$_.Exception.GetType().Name } } finally { if ($null -ne $native) { $native.Dispose() } }
        }
        try { [IO.File]::WriteAllText('DIAGNOSTICPATH', ($diagnostic | ConvertTo-Json -Compress)) } catch {}
    }
    function Invoke-CimMethod {
        param($InputObject,$MethodName)
        $sid=if ($InputObject.Sid) { [string]$InputObject.Sid } else { 'WORKER' }
        return @{ReturnValue=0;Sid=$sid}
    }
'''.replace("PROCESSES", str(processes).replace("'", "''")).replace("WORKER", SID)
    adapters = adapters.replace("DIAGNOSTICPID", str(target_pid or 0)).replace(
        "DIAGNOSTICPATH", str(diagnostic or processes.with_suffix(".diagnostic.json")).replace("'", "''"))
    query = query.replace("    # Query only app-related rows;",
                          adapters + "\n    # Query only app-related rows;", 1)
    return (supervisor + "\nfunction Assert-LabelStartupNoReparsePoint {}\n" + query
            + "\nInvoke-LabelStartupReadOnlyQuery -QueryMode Observe -Root '"
            + str(current).replace("'", "''") + "' -OperatorSid '" + SID + "'\n")


def copy_release_sources(source, bundle):
    for path in source.glob("*.py"):
        shutil.copyfile(path, bundle / "app" / path.name)
    for name in ("INSTALL_CANONICAL_PORTABLE.ps1", "INSTALL_THIS_PC.ps1", "tools/label_writer_fence.ps1",
                 "tools/failure_redaction_rules.json"):
        if (bundle / name).exists():
            shutil.copyfile(source / name, bundle / name)
        if (bundle / "app" / name).exists():
            shutil.copyfile(source / name, bundle / "app" / name)


def refresh_file_pins(bundle, manifest):
    pins = {"launch-label-match.cmd": "launcher_sha256",
            "INSTALL_CANONICAL_PORTABLE.ps1": "canonical_installer_sha256", "INSTALL_THIS_PC.ps1": "helper_sha256",
            "tools/bootstrap_integrity.ps1": "integrity_helper_sha256",
            "tools/label_writer_fence.ps1": "writer_fence_helper_sha256",
            "tools/label_writer_fence_contract.json": "writer_session_contract_sha256",
            "tools/apply_prepared_update.ps1": "startup_update_helper_sha256",
            "app/update-manifest-key-config.json": "update_key_config_sha256"}
    for name, field in pins.items():
        manifest[field] = hashlib.sha256((bundle / name).read_bytes()).hexdigest()


def _write_test_update_key_config(bundle, public):
    from Label_Match import UPDATE_PACKAGED_KEY_CONFIG_SCHEMA
    settings_path = bundle / "app/config/app_settings.json"
    settings = json.loads(settings_path.read_text(encoding="utf-8"))
    if not isinstance(settings, dict) or not isinstance(settings.get("update_settings"), dict):
        raise ValueError("test update settings must contain an update_settings object")
    configured = json.dumps(public)
    settings["update_settings"]["manifest_public_key"] = configured
    write(settings_path, settings)
    write(bundle / "app/update-manifest-key-config.json", {
        "schema": UPDATE_PACKAGED_KEY_CONFIG_SCHEMA,
        "manifest_public_key": configured,
    })


def prepare(root, source, packet, *, preserved=False, signer=None, feed_shape="signed", slow_observe=15):
    """Make a test-only installed tree from authenticated, already built stock."""
    current = root / "machine/current"
    current.mkdir(parents=True)
    with zipfile.ZipFile(packet) as archive:
        assert archive.testzip() is None
        for info in archive.infolist():
            target = (current / info.filename).resolve()
            assert target.is_relative_to(current.resolve())
        archive.extractall(current)
    if not preserved:
        copy_release_sources(source, current)
    settings = {"feed_url": ORIGIN + "/feed/latest.json", "channel": "stable",
                "profile_auth": True, "apply_during_work": False}
    raw = (json.dumps(settings, sort_keys=True, separators=(",", ":")) + "\n").encode()
    # Use the production canonical encoder rather than imposing a test format.
    import auto_update as updates
    raw = updates.canonical_startup_settings(settings)
    if feed_shape == "test-pc":
        raw = (current / "app/auto-update-settings.json").read_bytes()
        settings = json.loads(raw)
    (current / "app/auto-update-settings.json").write_bytes(raw)
    manifest = json.loads((current / "portable-manifest.json").read_bytes())
    manifest["version"] = updates.source_version_literal((current / "app/Label_Match.py").read_bytes())
    manifest["data_compatible_min_version"] = manifest["version"]
    from tests.test_update_manifest import canonical_manifest, signed_es256_manifest_payload
    feed = canonical_manifest()
    feed.update(version=manifest["version"], source_commit=manifest["source_commit"])
    artifact_name = "Label_Match-" + manifest["version"] + ".zip"
    feed["artifact"].update(name=artifact_name, url=ORIGIN + "/artifacts/" + artifact_name)
    if signer is None:
        public, signature = signed_es256_manifest_payload(feed)
    else:
        feed["signature_version"] = "es256-v1"
        public = signer.public_jwk
        signature = signer.sign_es256(json.dumps(feed, sort_keys=True, separators=(",", ":"), ensure_ascii=True).encode())
    if feed_shape == "legacy":
        # README's July shape; no production values, keys or network access.
        feed.update(version="v2.0.66")
        feed.pop("signature_version", None)
        feed["artifact"].update(name="Label_Match-v2.0.66.zip", url=ORIGIN + "/artifacts/old.zip")
    write(root / "feed.json", feed)
    (root / "feed.sig").write_bytes(signature)
    _write_test_update_key_config(current, public)
    refresh_file_pins(current, manifest)
    manifest["writer_sink_inventory_contract_sha256"] = updates.version_writer_inventory_sha256(current)
    write(current / "portable-manifest.json", manifest)
    relocate_ancestor_boundary(current, root)
    inventory(current)
    selected = root / "machine/versions" / manifest["source_commit"]
    shutil.copytree(current, selected)
    inventory(selected)
    policy = {**settings, "schema": "label-match-startup-update-policy-v1",
              "operator_sid": SID, "operator_local_app_data": str(root / "local"),
              "settings_sha256": hashlib.sha256(raw).hexdigest(), "staging_root": str(root / "stage"),
              "hiberboot_enabled": None, "hiberboot_read_status": "UNPROVEN"}
    control = root / "machine/update-control"
    write(control / "policy.json", policy)
    write(control / "active.json", {"schema": updates.SCHEMA, "sha": manifest["source_commit"],
                                        "transition_id": "CANONICAL_INSTALL"})
    write(control / "transition.json", {"schema": updates.SCHEMA, "phase": "DEFERRED",
                                        "deferred_at": updates.now_utc(), "reason": "canonical installation",
                                        "failed_shas": []})
    (root / "local").mkdir()
    (root / "stage").mkdir()
    config = root / "host-config.json"
    write(config, {"root": str(root), "repo": str(source), "current": str(current),
                   "selected": str(selected), "control": str(control), "slow_observe": slow_observe,
                   "feed_shape": feed_shape,
                   "host_load_snapshot": os.environ.get("LABEL_MATCH_HOST_TEST_LOAD_SNAPSHOT", "")})
    native_acl(root)
    if preserved:
        process = launch(config, "seed")
        assert process.wait() == 0, "inspect isolated seed output and state metadata"
        overlay_release(config, source)
    return config


def overlay_release(config, source):
    """Reinstall code while retaining the prior user state and its stop proof."""
    import auto_update as updates
    value = json.loads(config.read_text())
    current, root = Path(value["current"]), config.parent
    manifest = json.loads((current / "portable-manifest.json").read_bytes())
    previous = manifest["version"]
    copy_release_sources(source, current)
    version = updates.source_version_literal((current / "app/Label_Match.py").read_bytes())
    test_sha = hashlib.sha1(b"".join(path.read_bytes() for path in sorted(source.glob("*.py")))).hexdigest()
    manifest.update(version=version, data_compatible_min_version=version, source_commit=test_sha)
    refresh_file_pins(current, manifest)
    manifest["writer_sink_inventory_contract_sha256"] = updates.version_writer_inventory_sha256(current)
    write(current / "portable-manifest.json", manifest)
    inventory(current)
    selected = current.parent / "versions" / test_sha
    assert not selected.exists()
    shutil.copytree(current, selected)
    inventory(selected)
    control = Path(value["control"])
    write(control / "active.json", {"schema": updates.SCHEMA, "sha": test_sha,
                                        "transition_id": "CANONICAL_INSTALL"})
    write(control / "transition.json", {"schema": updates.SCHEMA, "phase": "DEFERRED",
                                        "deferred_at": updates.now_utc(), "reason": "canonical installation",
                                        "failed_shas": []})
    value.update(selected=str(selected), forbid_enroll=True, preserved_release=previous)
    write(config, value)
    # Retain a correctly authenticated no-update feed for the reinstalled code.
    from tests.test_update_manifest import canonical_manifest, signed_es256_manifest_payload
    feed = canonical_manifest()
    feed.update(version=version, source_commit=test_sha)
    artifact_name = "Label_Match-" + version + ".zip"
    feed["artifact"].update(name=artifact_name, url=ORIGIN + "/artifacts/" + artifact_name)
    public, signature = signed_es256_manifest_payload(feed)
    write(root / "feed.json", feed)
    (root / "feed.sig").write_bytes(signature)
    for bundle in (current, selected):
        _write_test_update_key_config(bundle, public)
        declared = json.loads((bundle / "portable-manifest.json").read_bytes())
        declared["update_key_config_sha256"] = hashlib.sha256(
            (bundle / "app/update-manifest-key-config.json").read_bytes()).hexdigest()
        write(bundle / "portable-manifest.json", declared)
        inventory(bundle)


def prepare_update(config, signer, *, candidate_version="v2.0.125", base_bundle=None,
                   offer_name="offer", include_compatibility=True, compatibility_floor=None):
    """Prepare a signed fixture before any installed relay is alive."""
    import auto_update as updates
    from tests.test_update_manifest import canonical_manifest
    value = json.loads(config.read_text())
    root = config.parent
    current = Path(base_bundle) if base_bundle is not None else Path(value["selected"])
    before = updates.version_manifest(current)
    assert (before["version"], candidate_version) in {
        ("v2.0.124", "v2.0.125"), ("v2.0.125", "v2.0.126")}
    assert offer_name in {"offer", "offer126"}
    offer = root / offer_name
    candidate = offer / "Label_Match"
    assert not candidate.exists()
    shutil.copytree(current, candidate)
    receipt = (candidate / "bootstrap-integrity.json").resolve()
    assert receipt.is_relative_to(offer.resolve())
    receipt.unlink(missing_ok=base_bundle is not None)
    # The release builder requires a clean committed tree and packages Git
    # blobs (build_portable_release_candidate.py:558-572,650-710). A scratch
    # release commit is outside this test's authority, so use the approved
    # source-copy route with the same existing declaration generators.
    source = Path(value["repo"])
    suffix = candidate_version.rsplit(".", 1)[1]
    source125 = root / ("source" + suffix)
    with (root / ("source" + suffix + ".private.log")).open("xb") as stream:
        subprocess.run(["git", "-c", "core.autocrlf=false", "clone", "--shared",
                        str(source), str(source125)], check=True, stdout=stream,
                       stderr=subprocess.STDOUT, creationflags=subprocess.CREATE_NO_WINDOW)
    tracked = subprocess.check_output(["git", "ls-files", "-z"], cwd=source,
                                      creationflags=subprocess.CREATE_NO_WINDOW)
    for name in filter(None, tracked.decode().split("\0")):
        target = source125 / name
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(source / name, target)
    for name, constant in (("Label_Match.py", "APP_VERSION"),
                           ("label_data_generation.py", "CURRENT_VERSION")):
        path = source125 / name
        raw = path.read_bytes()
        old = (constant + ' = "v2.0.124"').encode()
        assert raw.count(old) == 1
        path.write_bytes(raw.replace(old, (constant + ' = "' + candidate_version + '"').encode(), 1))
    declarations = []
    # Historical layouts add long relative paths. Keep their private work root
    # beside the pytest case, below the Windows copytree path limit.
    declaration_work = root.parent / ("d" + suffix + "-" + hashlib.sha256(str(root).encode()).hexdigest()[:8])
    assert not declaration_work.exists()
    write(root / ("source" + suffix + "-work-root.json"), {"path": str(declaration_work)})
    tools = [("refresh_admin_worker_declarations.py", []),
             ("refresh_first_update_declarations.py",
              ["--work-root", str(declaration_work)]),
             ("refresh_atomic_replace_declarations.py", [])]
    for tool, extra in tools:
        for mode in ("write", "check"):
            args = [sys.executable, "-s", "-B", str(source125 / "tools" / tool), *extra]
            if mode == "check":
                args.append("--check")
            log_prefix = "" if suffix == "125" else "source" + suffix + "-"
            with (root / (log_prefix + tool + "-" + mode + ".private.log")).open("xb") as stream:
                result = subprocess.run(args, cwd=source125, stdout=stream,
                                        stderr=subprocess.STDOUT,
                                        creationflags=subprocess.CREATE_NO_WINDOW)
            declarations.append({"tool": tool, "mode": mode, "returncode": result.returncode})
            write(root / ("source" + suffix + "-declaration-proof.json"), declarations)
            assert result.returncode == 0, "inspect isolated declaration result"
    copy_release_sources(source125, candidate)
    hot = hashlib.sha1((before["source_commit"] + ":host" + suffix).encode()).hexdigest()
    manifest = json.loads((candidate / "portable-manifest.json").read_bytes())
    manifest.update(version=candidate_version, source_commit=hot)
    # Exercise the release builder's signed private-compatibility metadata path.
    # An unsigned floor alone intentionally cannot authorize the 124 reader.
    floor = compatibility_floor or before["version"]
    if include_compatibility:
        private = canonical_manifest()
        private.update(signature_version="es256-v1", source_commit=hot, version=candidate_version,
                       data_compatible_min_version=floor)
        private_signature = signer.sign_es256(json.dumps(private, sort_keys=True,
            separators=(",", ":"), ensure_ascii=True).encode())
        from label_data_generation import verified_private_compatibility
        compatibility = verified_private_compatibility(private, private_signature, signer.public_jwk,
            version=candidate_version, build_id=hot)
        assert compatibility["compatibility_verified"] is True
        assert compatibility["data_compatible_min_version"] == floor
        manifest.update(data_compatible_min_version=floor,
            data_generation_signed_manifest=private, data_generation_signature_hex=private_signature.hex())
    else:
        manifest.pop("data_generation_signed_manifest", None)
        manifest.pop("data_generation_signature_hex", None)
        manifest["data_compatible_min_version"] = candidate_version
    write(root / ("source" + suffix + "-compatibility-proof.json"), {
        "private_signature_verified": include_compatibility,
        "floor_matches_starting_release": include_compatibility and floor == before["version"],
        "version": candidate_version, "data_compatible_min_version": floor if include_compatibility else candidate_version,
        "official_signature_used": False,
    })
    refresh_file_pins(candidate, manifest)
    manifest["writer_sink_inventory_contract_sha256"] = updates.version_writer_inventory_sha256(candidate)
    write(candidate / "portable-manifest.json", manifest)
    assert updates.version_manifest(candidate)["version"] == candidate_version
    after = updates.version_identity(lambda name: (candidate / name).read_bytes())
    forward = {"from_commit": before["source_commit"], "to_commit": hot,
               "from_contract_sha256": before["contract_sha256"], "to_contract_sha256": after["contract_sha256"]}
    reverse = {"from_commit": hot, "to_commit": before["source_commit"],
               "from_contract_sha256": after["contract_sha256"], "to_contract_sha256": before["contract_sha256"]}
    pin = lambda record: hashlib.sha256(json.dumps(record, sort_keys=True, separators=(",", ":")).encode()).hexdigest()
    packet = offer / ("Label_Match-" + candidate_version + ".zip")
    with zipfile.ZipFile(packet, "x", compression=zipfile.ZIP_DEFLATED) as archive:
        for path in sorted(candidate.rglob("*")):
            if path.is_file():
                archive.write(path, "Label_Match/" + path.relative_to(candidate).as_posix())
    feed = canonical_manifest()
    feed.update(signature_version="es256-v1", source_commit=hot, version=candidate_version,
        approved_transitions=[forward, reverse],
        auto_update={"from_commit": before["source_commit"], "to_commit": hot,
                     "forward_contract_sha256": pin(forward), "rollback_contract_sha256": pin(reverse)})
    url = ORIGIN + "/artifacts/Label_Match-" + candidate_version + ".zip"
    with packet.open("rb") as stream:
        digest = hashlib.file_digest(stream, "sha256").hexdigest()
    feed["artifact"].update(name="Label_Match-" + candidate_version + ".zip", url=url,
                            size_bytes=packet.stat().st_size, sha256=digest)
    write(offer / "feed.json", feed)
    (offer / "feed.sig").write_bytes(signer.sign_es256(
        json.dumps(feed, sort_keys=True, separators=(",", ":"), ensure_ascii=True).encode()))
    write(root / (offer_name + "-info.json"), {"source_commit": hot, "artifact_url": url,
                                     "packet": str(packet), "from_commit": before["source_commit"]})
    return hot


def offer_update(config, offer_name="offer"):
    """Publish only the already validated fixture after 124 reaches READY."""
    assert offer_name in {"offer", "offer126"}
    info = json.loads((config.parent / (offer_name + "-info.json")).read_bytes())
    (config.parent / (offer_name + "-ready")).touch()
    return info["source_commit"]


def _setup_app(value):
    """Keep first setup on current; subsequent starts use the active version."""
    current = Path(value["current"])
    active = json.loads((Path(value["control"]) / "active.json").read_bytes())["sha"]
    initial = json.loads((current / "portable-manifest.json").read_bytes())["source_commit"]
    return (current if active == initial else current.parent / "versions" / active) / "app"


def _register_native_launch(config, entry, arguments, process):
    from datetime import datetime, timedelta, timezone
    config = Path(config)
    value = json.loads(config.read_bytes())
    setup = entry in {"onboard", "seed", "keep-relay", "read-owner", "stop"}
    app = _setup_app(value) if setup else Path(entry).parent
    identity = _native_process_times(process)
    created = datetime(1601, 1, 1, tzinfo=timezone.utc) + timedelta(
        microseconds=int(identity["creation_identity"]) // 10)
    product_entry = app / "label_match_relay_entry.py" if setup else Path(entry)
    product_arguments = ["--onboard-current-user", "--app-root", str(app.parent)] if setup else arguments
    system = "--apply-prepared" in arguments
    _write_observation(config.parent / "processes" / (str(process.pid) + ".json"), {
        "ProcessId": process.pid, "Name": "pythonw.exe",
        "ExecutablePath": str(app.parent / "runtime/pythonw.exe"),
        "CommandLine": subprocess.list2cmdline([str(app.parent / "runtime/pythonw.exe"),
                                               "-I", "-B", str(product_entry), *product_arguments]),
        "CreationDate": created.isoformat(), "CreationIdentity": identity["creation_identity"],
        "Sid": "S-1-5-18" if system else SID, "OrdinaryUser": not system,
    })


def launch(config, entry="onboard", arguments=(), *, environment=None):
    config = Path(config)
    value = json.loads(config.read_text())
    code = ("import sys;sys.path.insert(0,sys.argv[1]);"
            "from tests._first_setup_host import child_main;"
            "raise SystemExit(child_main(sys.argv[2],sys.argv[3],sys.argv[4:]))")
    command = [sys.executable, "-B", "-c", code, value["repo"], str(config), entry, *arguments]
    output = config.parent / (Path(entry).name + "-" + str(time.time_ns()) + ".private.log")
    with output.open("wb") as stream:
        native = getattr(subprocess.Popen, "native", subprocess.Popen)
        process = native(command, env=environment, stdin=subprocess.DEVNULL, stdout=stream,
                         stderr=subprocess.STDOUT, creationflags=subprocess.CREATE_NO_WINDOW)
    process.host_test_output = output
    _register_native_launch(config, entry, arguments, process)
    if os.environ.get("LABEL_MATCH_HOST_TEST_LOCK_TIMELINE") == "1":
        from tests._first_setup_lock_observer import observe_launch
        observe_launch(config, entry, arguments, process, _native_process_times, _fixed_child_log)
    return process


def install_os(config, app):
    import auto_update as updates
    import current_user_onboarding as onboarding
    import direct_sync_runtime
    import logistics_runtime_profile as profiles
    import tools.install_logistics_runtime_profile as profile_installer
    from tools import register_label_match_worker_pc as registration
    import requests
    import protected_admin
    import user_relay
    import writer_session_fence as fence
    from tests._possession_fixture import fake_possession_descriptor
    from tests.test_first_setup_relay_parent_cohort import _query

    value = json.loads(config.read_text())
    root, current, selected = (Path(value[k]) for k in ("root", "current", "selected"))
    # Observe actual published state without changing its content or timing.
    actual_write = user_relay._write_json_atomic
    def observed_write(path, payload):
        result = actual_write(path, payload)
        if payload.get("report_version") == "label-match-user-relay-v1":
            cycle = payload.get("last_cycle") or {}
            event = {"at": time.monotonic(), "time_ns": time.time_ns(), "pid": os.getpid(),
                     "status": payload.get("status"), "cycle": payload.get("cycle_count"),
                     "reported_process_id": payload.get("process_id"),
                     "source_commit": payload.get("source_commit"),
                     "update_nonce_present": bool(payload.get("update_nonce")),
                     "process_status": cycle.get("process_status"),
                     "process_returncode": cycle.get("process_returncode"),
                     "relay_status": cycle.get("relay_status")}
            if payload.get("status") == "RUNNING":
                # Execute the actual screen's read-only decision functions;
                # no toolkit, window, tray or renderer is instantiated.
                import Label_Match as product
                direct = root / "local/KMTech/DirectSync/label_match"
                runtime = direct / "status/direct_sync_relay_status.json"
                runtime_payload = json.loads(runtime.read_bytes()) if runtime.exists() else {}
                durable = product._label_match_relay_durable_state(
                    str(direct / "queue/direct_sync_relay.sqlite3"))
                screen = product._label_match_relay_screen_state(runtime_payload, durable)
                event.update(screen_state=screen, screen_warning_decision=screen in {"retrying", "stopped"})
            target = root / "cycle-observations" / (str(os.getpid()) + ".jsonl")
            target.parent.mkdir(exist_ok=True)
            with target.open("a", encoding="utf-8") as stream:
                stream.write(json.dumps(event) + "\n")
        return result
    user_relay._write_json_atomic = observed_write
    actual_durable_write = updates.write_json_durable
    def observed_durable_write(path, payload):
        result = actual_durable_write(path, payload)
        if Path(path) == root / "stage/prepared.json":
            import Label_Match as product
            direct = root / "local/KMTech/DirectSync/label_match"
            runtime = direct / "status/direct_sync_relay_status.json"
            state = json.loads(runtime.read_bytes()) if runtime.exists() else {}
            durable = product._label_match_relay_durable_state(
                str(direct / "queue/direct_sync_relay.sqlite3"))
            screen = product._label_match_relay_screen_state(state, durable)
            transition = json.loads((root / "machine/update-control/transition.json").read_bytes())
            write(root / "pending-prepared-ui-proof.json", {
                "phase": transition["phase"], "screen_state": screen,
                "screen_warning_decision": screen in {"retrying", "stopped"},
                "runtime_error_code_present": bool(state.get("error_code")),
                "runtime_error_message_present": bool(state.get("error_message"))})
        return result
    updates.write_json_durable = observed_durable_write
    def observe_http(kind):
        with (root / "http-observations.jsonl").open("a", encoding="utf-8") as stream:
            stream.write(json.dumps({"at": time.monotonic(), "kind": kind}) + "\n")
    os.environ["LOCALAPPDATA"] = str(root / "local")
    os.environ["APPDATA"] = str(root / "roaming")
    os.environ["LABEL_MATCH_SAVE_DIR"] = str(root / "state/data")
    os.environ[onboarding.LABEL_MATCH_SETTINGS_PATH_ENV] = str(root / "local/KMTech/Label_Match/config/app_settings.json")
    os.environ[onboarding.LABEL_MATCH_DIRECT_SYNC_ROOT_ENV] = str(root / "local/KMTech/DirectSync/label_match")
    profile_path = str(root / "local/KMTech/Label_Match/logistics/runtime_profile.json")
    os.environ[profiles.APP_PROFILE_PATH_ENV] = profile_path
    os.environ[profiles.PROFILE_PATH_ENV] = profile_path
    os.environ[registration.DEFAULT_ENROLLMENT_TOKEN_ENV] = ""
    os.environ["LABEL_MATCH_DIRECT_SYNC_SERVER_BASE_URL"] = ORIGIN
    os.environ["KMTECH_LABEL_WRITER_TEST_MODE"] = "1"
    os.environ["KMTECH_LABEL_WRITER_CONTROL_ROOT"] = str(root / "local" / fence.CONTROL_RELATIVE_PATH)
    assert os.environ["LABEL_MATCH_DIRECT_SYNC_SERVER_BASE_URL"] == ORIGIN
    policy = updates.load_policy(Path(value["control"]))
    assert value["feed_shape"] == "test-pc" or policy["feed_url"].startswith(ORIGIN + "/")
    write(root / "test-origin-proof.json", {"default_origin_is_test": True, "http_is_fake": True})
    onboarding.CANONICAL_PORTABLE_ROOT = current
    run_value = root / "hkcu-run.json"
    user_relay._registry_set = lambda command: write(run_value, {"command": command})
    user_relay._registry_get = lambda: json.loads(run_value.read_bytes())["command"] if run_value.exists() else ""
    import inspect
    inspect.unwrap(onboarding.onboard_current_user).__kwdefaults__.update(
        autostart_installer=user_relay.install_user_relay_autostart,
        scheduled_task_remover=lambda *_: {"status": "ABSENT"},
        legacy_task_quiescence_reader=lambda: {
            "schema": "label-match-legacy-task-quiescence-v1", "status": "PASS",
            "required_state": "ABSENT_OR_DISABLED", "read_only": True, "task_or_process_mutated": False})
    actual_prepare = updates.prepare_feed_packet
    def observed_prepare(*args, **kwargs):
        try:
            return actual_prepare(*args, **kwargs)
        except Exception as error:
            import traceback
            write(root / "preparation-frames" / (str(os.getpid()) + "-" + str(time.time_ns()) + ".json"), {
                "type": type(error).__name__, "frames": [
                    {"file": Path(frame.filename).name, "line": frame.lineno, "function": frame.name}
                    for frame in traceback.extract_tb(error.__traceback__)]})
            raise
    updates.prepare_feed_packet = observed_prepare
    last_observe = {}
    guard_observations = {}

    def collect_guard(frame):
        local = frame.f_locals
        claim = local.get("claim") or {}
        state, active = local.get("state") or {}, local.get("active") or {}
        pid = claim.get("pid")
        guard_ns = time.time_ns()
        proof = {"at_python_guard_exception": True,
                 "active_is_candidate": active.get("sha") == state.get("candidate_sha"),
                 "phase_is_rollback_pending_ready": state.get("phase") == "ROLLBACK_PENDING_READY",
                 "metadata_row_present": (root / "processes" / (str(pid) + ".json")).is_file(),
                 "cim_input_row_present": pid in last_observe.get("input_pids", ()),
                 "observe_output_row_present": pid in last_observe.get("output_pids", ()),
                 "observe_query_target_matches_claim": last_observe.get("target_pid") == pid,
                 "cim_adapter": last_observe.get("cim", {}),
                 "observe_output_guard_seconds": round((last_observe["time_ns"] - guard_ns) / 1e9, 6)
                    if "time_ns" in last_observe else None}
        process = next((child for child in children if child.pid == pid), None)
        proof["held_child_found"] = process is not None
        if process is not None:
            try:
                proof.update(_native_guard_process(process,
                    getattr(process, "host_test_creation_identity", None), pid, guard_ns))
                if proof["native_alive"]:
                    security = native_process_security(pid)
                    proof["native_sid_matches_observer_account"] = security.get("sid") == native_sid
            except Exception as error:
                proof["native_error_type"] = type(error).__name__
            try:
                proof.update(_fixed_child_log(process))
            except Exception as error:
                proof["log_error_type"] = type(error).__name__
        observation = _observation_path(root, pid)
        proof["relay_entry_observation_present"] = observation.is_file()
        if observation.is_file():
            try:
                proof["relay_entry"] = json.loads(observation.read_text())
            except Exception as error:
                proof["relay_entry_error_type"] = type(error).__name__
        return proof

    actual_failure_details = updates.launch_failure_details
    def observed_failure_details(error, *args, **kwargs):
        # Preserve the failed trial's actual local predicates before rollback
        # replaces its claim. Never copy locals, identifiers or credentials.
        if isinstance(error, ValueError) and str(error) == "existing trial process identity or creation time differs":
            try:
                import uuid
                observation = guard_observations.get(id(error))
                if observation is not None:
                    _write_observation(root / "trial-process-guards" / (uuid.uuid4().hex + ".json"), observation)
                trace = error.__traceback__
                while trace is not None:
                    frame = trace.tb_frame
                    if frame.f_code.co_name == "_bootstrap":
                        local = frame.f_locals
                        claim, state = local.get("claim") or {}, local.get("state") or {}
                        row, active = local.get("row"), local.get("active") or {}
                        policy, role = local.get("policy") or {}, local.get("claimed_role")
                        proof = {"phase_is_applied_pending_ready": state.get("phase") == "APPLIED_PENDING_READY",
                            "phase_is_rollback_pending_ready": state.get("phase") == "ROLLBACK_PENDING_READY",
                            "row_present": row is not None,
                            "transition_matches": claim.get("transition_id") == state.get("transition_id"),
                            "nonce_matches": claim.get("nonce") == state.get("nonce"),
                            "role_valid": role in {"gui", "relay"},
                            "active_is_candidate": active.get("sha") == state.get("candidate_sha")}
                        if row is not None and role in {"gui", "relay"}:
                            proof.update(observable=row.get("observable") is True,
                                sha_matches=row.get("sha") == active.get("sha"),
                                sid_matches=row.get("sid") == policy.get("operator_sid"),
                                role_matches=row.get("role") == role,
                                row_role_is_writer=row.get("role") == "writer",
                                row_role_is_gui=row.get("role") == "gui",
                                row_role_is_relay=row.get("role") == "relay",
                                pid_valid=type(row.get("pid")) is int and row["pid"] > 0,
                                pid_matches_claim=row.get("pid") == claim.get("pid"),
                                ordinary_user=row.get("ordinary_user") is True)
                            checks = {
                                "process_matches_trial": lambda: updates.process_matches_trial(row,
                                    state=state, sha=active.get("sha"), sid=policy.get("operator_sid"), role=role),
                                "created_after_claim": lambda: updates._utc(row.get("created_at")) > updates._utc(claim.get("claimed_at")),
                                "created_after_required_boundary": lambda: updates._utc(row.get("created_at")) >= updates._utc(
                                    state["applied_at"] if role == "gui" and state.get("phase") == "APPLIED_PENDING_READY"
                                    else state.get("rollback_at", state["applied_at"])),
                            }
                            if (role == state.get("trial_role", "gui") and state.get("trial_pid") == row.get("pid")
                                    and state.get("trial_started_at")):
                                checks["bound_creation_matches"] = lambda: updates._utc(row.get("created_at")) == updates._utc(state["trial_started_at"])
                            def matches_native_creation():
                                from datetime import datetime, timedelta, timezone
                                identity = updates.native_process_identity(row["pid"])
                                created = datetime(1601, 1, 1, tzinfo=timezone.utc) + timedelta(
                                    microseconds=int(identity["creation_identity"]) // 10)
                                return updates._utc(row.get("created_at")) == created
                            checks["row_created_matches_native_identity"] = matches_native_creation
                            for name, check in checks.items():
                                try:
                                    proof[name] = bool(check())
                                except Exception as check_error:
                                    proof[name + "_error_type"] = type(check_error).__name__
                        write(root / "trial-identity-predicates" / (str(os.getpid()) + "-" + str(time.time_ns()) + ".json"), proof)
                        del local
                    del frame
                    trace = trace.tb_next
            except Exception as observation_error:
                try:
                    write(root / "trial-identity-observation-error.json", {"type": type(observation_error).__name__})
                except OSError:
                    pass
        return actual_failure_details(error, *args, **kwargs)
    updates.launch_failure_details = observed_failure_details
    def stop_snapshot(**_kwargs):
        rows = []
        for path in (root / "processes").glob("*.json"):
            row = json.loads(path.read_bytes())
            identity = updates.native_process_identity(row["ProcessId"])
            if identity.get("status") == "LIVE" and identity.get("creation_identity") == row["CreationIdentity"]:
                rows.append({"pid": row["ProcessId"], "parent_pid": 0, "name": row["Name"],
                    "image": row["ExecutablePath"], "command": row["CommandLine"],
                    "sid": row["Sid"], "session_id": 1, "created_at": row["CreationDate"]})
        return rows
    user_relay._app_stop_process_snapshot = stop_snapshot
    native_sid = updates.current_process_sid()
    native_process_security = updates.process_security
    native_lock = updates.update_lock
    updates.update_lock = lambda control, _sid, **kw: native_lock(control, native_sid, **kw)
    native_source = updates.unredirected_source
    def source_identity(path, *, expected_owner_sid=None):
        assert Path(path).resolve().is_relative_to(root.resolve())
        assert expected_owner_sid in (None, SID)
        return native_source(path, expected_owner_sid=native_sid if expected_owner_sid else None)
    updates.unredirected_source = source_identity
    system = "--apply-prepared" in sys.argv
    os.environ["LABEL_MATCH_HOST_TEST_SYSTEM"] = "1" if system else "0"
    virtual_sid = "S-1-5-18" if system else SID
    updates.current_process_sid = lambda: virtual_sid
    protected_admin.current_process_sid = lambda: virtual_sid

    def security(pid):
        if pid == os.getpid():
            elevated = system
        else:
            metadata = root / "processes" / (str(pid) + ".json")
            if not metadata.is_file():
                raise ValueError("test process identity is outside the owned cohort")
            elevated = json.loads(metadata.read_text()).get("OrdinaryUser") is False
        return {"sid": "S-1-5-18" if elevated else SID, "session_id": 0 if elevated else 1,
                "elevated": elevated, "integrity_rid": 16384 if elevated else 8192}

    updates.process_security = security
    original_relay_run = user_relay._run_user_relay
    def observed_relay_run(args, app_root, argv, **kwargs):
        proof = {"app_root_argument_present": "--app-root" in (argv or []),
                 "resolved_app_root_is_entry_app": Path(app_root).resolve() == app.resolve(),
                 "resolved_app_root_is_entry_bundle": Path(app_root).resolve() == app.parent.resolve(),
                 "relay_module_is_entry_app": Path(user_relay.__file__).resolve().parent == app.resolve(),
                 "entered_product_relay": True}
        def save():
            try:
                _write_observation(_observation_path(root, os.getpid()), proof)
            except Exception:
                pass
        save()
        try:
            result = original_relay_run(args, app_root, argv, **kwargs)
        except BaseException as error:
            proof["product_error_type"] = type(error).__name__
            save()
            raise
        proof["product_return_code"] = result if type(result) is int else None
        save()
        return result
    user_relay._run_user_relay = observed_relay_run
    updates.ordinary_user_context = lambda expected=None: not system and expected in (None, SID)
    updates.runtime_matches = lambda *_: True
    registration._current_user_sid = lambda: SID
    registration._current_machine_guid = lambda: "00112233-4455-6677-8899-aabbccddeeff"

    # The test's DPAPI bytes have no relationship to the host credential store.
    protect = lambda text: b"host-test-dpapi:" + text.encode()
    unprotect = lambda raw: raw.removeprefix(b"host-test-dpapi:").decode()
    registration._dpapi_protect_current_user = protect
    registration._dpapi_unprotect_current_user = unprotect
    direct_sync_runtime._dpapi_unprotect_current_user = lambda raw: unprotect(raw).encode()
    profiles.protect_current_user_secret = protect
    profiles.unprotect_current_user_secret = unprotect
    onboarding.unprotect_current_user_secret = unprotect
    profile_installer.protect_current_user_secret = protect
    profile_installer.unprotect_current_user_secret = unprotect

    class Key:
        @classmethod
        def provision_initial(cls, **_kw):
            return cls()
        open_existing = provision_initial
        def __enter__(self):
            return self
        def __exit__(self, *_):
            pass
        def descriptor(self):
            return SimpleNamespace(as_dict=lambda: fake_possession_descriptor())
    registration.PersistentPossessionKey = Key

    def post(url, **kwargs):
        from tests.test_register_label_match_worker_pc import fake_v2_enrollment_response
        from tests.test_logistics_runtime_profile import _machine_enrollment_response
        observe_http("enroll" if url == ORIGIN + "/api/producer-ingest/v2/enroll" else "unmodelled")
        assert not value.get("forbid_enroll"), "preserved registration must be reused"
        assert url.startswith(ORIGIN + "/")
        assert url.endswith("/api/producer-ingest/v2/enroll")
        assert "X-Producer-Enrollment-Token" not in kwargs.get("headers", {})
        payload = kwargs["json"]
        response = fake_v2_enrollment_response(registration, payload, secret=os.urandom(32).hex())
        bundle = _machine_enrollment_response(10)["machine_credential_bundle"]
        identity = payload["manifest"]["pc_identity"]
        bundle["bindings"].update(app=registration.LABEL_MATCH_APP,
            source_host_id=identity["source_host_id"], device_id=identity["pc_id"])
        bundle["credentials"]["producer_ingest"].update(key_id=response["key_id"], secret=response["secret"])
        bundle["profiles"]["logistics"].update(base_url=ORIGIN, source_host_id=identity["source_host_id"],
                                               device_id=identity["pc_id"])
        response["machine_credential_bundle"] = bundle
        return SimpleNamespace(status_code=200, json=lambda: response)
    requests.post = post

    class Response:
        def __init__(self, url, path):
            self.url, self.path, self.status_code = url, path, 200
            self.headers = {"Content-Length": str(path.stat().st_size)}
        @property
        def content(self):
            return self.path.read_bytes()
        def iter_content(self, chunk_size=64 * 1024, **_kwargs):
            with self.path.open("rb") as stream:
                while chunk := stream.read(chunk_size):
                    yield chunk
        def json(self):
            return json.loads(self.content)
        def __enter__(self):
            return self
        def __exit__(self, *_):
            pass
        def close(self):
            pass

    class Session:
        def __init__(self):
            self.cookies = SimpleNamespace(clear=lambda: None)
            self.headers = {}
            self.trust_env = False
            self.offered = (root / "offer-ready").exists()
            self.offer_name = "offer126" if (root / "offer126-ready").exists() else "offer"
            self.leases = None
        def get(self, url, **_kwargs):
            assert url.startswith(ORIGIN + "/"), "test HTTP origin differs"
            if value["feed_shape"] == "test-pc":
                observe_http("feed_404")
                response = Response(url, root / "feed.json")
                response.status_code = 404
                return response
            feed_root = root / self.offer_name if self.offered else root
            paths = {ORIGIN + "/feed/latest.json": feed_root / "feed.json",
                     ORIGIN + "/feed/latest.json.sig": feed_root / "feed.sig"}
            if self.offered:
                offered = json.loads((root / (self.offer_name + "-info.json")).read_text())
                paths[offered["artifact_url"]] = Path(offered["packet"])
            observe_http("update_read" if url in paths else "unmodelled")
            assert url in paths, "unmodelled test HTTP route"
            return Response(url, paths[url])
        def post(self, url, **kwargs):
            import producer_runtime_client
            from tests.test_producer_runtime_client import _LeaseSession
            expected = producer_runtime_client._runtime_endpoint(ORIGIN + "/api/producer-ingest/v1/source-file")
            observe_http("runtime_lease" if url == expected else "unmodelled")
            assert url == expected, "unmodelled test HTTP route"
            if self.leases is None:
                self.leases = _LeaseSession()
            response = self.leases.post(url, **kwargs)
            paths = onboarding.resolve_current_user_onboarding_paths(app.parent)
            identity = json.loads(paths.identity_path.read_text())
            response.payload["producer_install_id"] = identity["producer_install_id"]
            return response
        def __enter__(self):
            return self
        def __exit__(self, *_):
            pass
        def close(self):
            pass
    requests.Session = Session

    actual_run = subprocess.run
    def run(command, *args, **kwargs):
        if any(str(part).endswith("apply_prepared_update.ps1") for part in command):
            mode = command[command.index("-Mode") + 1]
            if mode == "Observe":
                import inspect
                target_pid = None
                trial_context = None
                frame = inspect.currentframe()
                while frame is not None:
                    if frame.f_code.co_name == "_bootstrap":
                        target_pid = (frame.f_locals.get("claim") or {}).get("pid")
                        trial_context = (frame.f_locals.get("state"),
                                         frame.f_locals.get("active"), frame.f_locals.get("claim"))
                        break
                    frame = frame.f_back
                del frame
                rows = []
                for path in (root / "processes").glob("*.json"):
                    row = json.loads(path.read_text())
                    if updates.native_process_identity(row["ProcessId"]).get("status") == "LIVE":
                        rows.append(row)
                processes = root / ("query-" + str(os.getpid()) + ".json")
                write(processes, rows)
                diagnostic = root / ("query-source-" + str(os.getpid()) + ".json")
                script = observe_script(current / "tools/apply_prepared_update.ps1", current, processes,
                                        diagnostic=diagnostic, target_pid=target_pid)
                import user_relay
                status = user_relay.user_relay_status_path(
                    onboarding.resolve_current_user_onboarding_paths(selected).direct_sync_root)
                slow = app == current / "app" and status.exists() and not (root / "slow-observe-started").exists()
                update_slow = (_claim_update_observe_delay(root, *trial_context, value["slow_observe"])
                               if not slow and trial_context is not None else None)
                if slow:
                    (root / "slow-observe-started").touch()
                if slow or update_slow:
                    script = "Start-Sleep -Seconds " + str(value["slow_observe"]) + "\n" + script
                target = root / ("query-" + str(os.getpid()) + ".ps1")
                target.write_text(script, encoding="utf-8-sig")
                before = time.monotonic()
                output = _query(target)
                try:
                    observed_rows = json.loads(output)
                    last_observe.update(input_pids=[row["ProcessId"] for row in rows],
                        output_pids=[row.get("pid") for row in observed_rows], target_pid=target_pid,
                        cim=json.loads(diagnostic.read_text()), time_ns=time.time_ns())
                except Exception as error:
                    last_observe.update(diagnostic_error_type=type(error).__name__)
                if slow:
                    write(root / "slow-observe-proof.json", {"elapsed_seconds": time.monotonic() - before,
                        "real_query_child": True, "real_update_mutex": True})
                if update_slow:
                    write(root / "update-slow-observe-proof.json", {
                        **update_slow, "elapsed_seconds": time.monotonic() - before,
                        "real_query_child": True, "real_update_mutex": True})
            elif mode == "ObserveLifecycle":
                output = json.dumps({"task_exists": True})
            elif mode in {"Verify", "Authority"}:
                quote = lambda path: "'" + str(path).replace("'", "''") + "'"
                selected_code = command[command.index("-CodeRoot") + 1] if "-CodeRoot" in command else None
                driver = root / ("verify-" + str(os.getpid()) + ".ps1")
                driver.write_text("$ErrorActionPreference='Stop'\nImport-Module "
                    + quote(root / "native-boundary.psm1") + "\n& "
                    + quote(current / "tools/apply_prepared_update.ps1")
                    + " -InstallRoot " + quote(current) + " -Mode " + mode
                    + (" -CodeRoot " + quote(selected_code) if selected_code is not None else "")
                    + "\nexit $LASTEXITCODE\n", encoding="utf-8-sig")
                completed = actual_run([updates.system_executable("WindowsPowerShell/v1.0/powershell.exe"),
                    "-NoLogo", "-NoProfile", "-NonInteractive", "-WindowStyle", "Hidden", "-File", str(driver)],
                    capture_output=True, text=True, creationflags=subprocess.CREATE_NO_WINDOW)
                if completed.returncode:
                    driver.with_suffix(".private.log").write_text(completed.stderr, encoding="utf-8")
                return subprocess.CompletedProcess(command, completed.returncode, completed.stdout, completed.stderr)
            else:
                raise AssertionError("unmodelled external helper mode: " + mode)
            return subprocess.CompletedProcess(command, 0, output, "")
        if command and str(command[0]).casefold() in {"icacls", "icacls.exe"}:
            return subprocess.CompletedProcess(command, 0, b"", b"")
        if command and Path(command[0]).name.casefold() == "schtasks.exe":
            assert command[1:3] == ["/Run", "/TN"]
            assert command[3] == updates.task_name(current, SID)
            process = launch(config, str(current / "app/auto_update.py"),
                             ["--apply-prepared", "--control-root", str(Path(value["control"]))])
            children.append(process)
            return subprocess.CompletedProcess(command, 0, b"", b"")
        entries = [(index, item) for index, item in enumerate(command)
                   if str(item).endswith("label_match_relay_entry.py")]
        if entries:
            import user_relay as child_relay
            index, entry = entries[0]
            process = launch(config, str(entry), command[index + 1:])
            if child_relay.DIRECT_SYNC_RELAY_MODE in command[index + 1:]:
                rc = _wait_observed_sync_child(process, config, kwargs.get("timeout"))
            else:
                rc = process.wait(timeout=kwargs.get("timeout"))
            raw = process.host_test_output.read_bytes()
            return subprocess.CompletedProcess(command, rc, raw, b"")
        return actual_run(command, *args, **kwargs)
    subprocess.run = run

    children = []
    def start(command, *, cwd, environment=None):
        entry_index = next(i for i, item in enumerate(command) if str(item).endswith(".py"))
        write(root / "launches" / (str(os.getpid()) + "-" + str(time.time_ns()) + ".json"), {
            "entry": Path(command[entry_index]).name, "arguments": command[entry_index + 1:],
            "isolated_product_flags": command[1:entry_index], "os_process_adapter": True})
        process = launch(config, str(command[entry_index]), command[entry_index + 1:], environment=environment)
        try:
            process.host_test_creation_identity = _native_process_times(process)["creation_identity"]
        except Exception as error:
            process.host_test_creation_error_type = type(error).__name__
        children.append(process)
        return process
    updates.start_ordinary_process = start
    original_bootstrap = updates._bootstrap
    def observed_bootstrap(*arguments, **options):
        prior_trace = sys.gettrace()
        def guard_trace(frame, event, argument):
            previous_local = prior_trace(frame, event, argument) if prior_trace is not None else None
            if frame.f_code is not original_bootstrap.__code__:
                return previous_local
            if previous_local is None:
                frame.f_trace_lines = False
            def local_trace(frame, event, argument):
                nonlocal previous_local
                if previous_local is not None:
                    previous_local = previous_local(frame, event, argument)
                if event == "exception":
                    error = argument[1]
                    if isinstance(error, ValueError) and str(error) == "existing trial process identity or creation time differs":
                        try:
                            guard_observations[id(error)] = collect_guard(frame)
                        except Exception as observation_error:
                            guard_observations[id(error)] = {"observation_error_type": type(observation_error).__name__}
                return local_trace
            return local_trace
        return _run_with_trace(original_bootstrap, guard_trace, *arguments, **options)
    updates._bootstrap = observed_bootstrap
    if os.environ.get("LABEL_MATCH_HOST_TEST_LOCK_TIMELINE") == "1":
        from tests._first_setup_lock_observer import install
        install(root, updates, fence, native_lock, native_sid, app)
        from tests._first_setup_ready_observer import install as install_ready_observer
        install_ready_observer(root, updates)
    return children


def child_main(config, entry, arguments):
    config = Path(config)
    value = json.loads(config.read_text())
    setup = entry in {"onboard", "seed", "keep-relay", "read-owner", "stop"}
    app = _setup_app(value) if setup else Path(entry).parent
    sys.path[:0] = [str(app), str(app / "site-packages"), "E:/KMTech/shared-tools/prodblock"]
    sys.argv = [str(app / "label_match_relay_entry.py") if setup else entry, *arguments]
    # -I product launches are translated solely at the process/token OS boundary.
    children = install_os(config, app)
    import auto_update as updates
    import current_user_onboarding as onboarding
    import user_relay
    root = config.parent
    from datetime import datetime, timedelta, timezone
    identity = updates.native_process_identity(os.getpid())
    created = datetime(1601, 1, 1, tzinfo=timezone.utc) + timedelta(
        microseconds=int(identity["creation_identity"]) // 10)
    process_entry = app / "label_match_relay_entry.py" if setup else Path(entry)
    product_arguments = ["--onboard-current-user", "--app-root", str(app.parent)] if setup else arguments
    _write_observation(root / "processes" / (str(os.getpid()) + ".json"), {
        "ProcessId": os.getpid(), "Name": "pythonw.exe",
        "ExecutablePath": str(app.parent / "runtime/pythonw.exe"),
        "CommandLine": subprocess.list2cmdline([str(app.parent / "runtime/pythonw.exe"),
                                               "-I", "-B", str(process_entry), *product_arguments]),
        "CreationDate": created.isoformat(),
        "CreationIdentity": identity["creation_identity"],
        "Sid": updates.current_process_sid(), "OrdinaryUser": "--apply-prepared" not in arguments,
    })
    if entry == "seed":
        import writer_session_fence as fence
        paths = onboarding.resolve_current_user_onboarding_paths(app.parent)
        assert onboarding._registration_runner(paths, server_base_url=ORIGIN) == 0
        assert onboarding.inspect_current_user_state(paths)["status"] == "READY"
        with fence.writer_admission("current_user_onboarding"):
            onboarding.record_user_data_open(paths.app_root, (paths.data_root, paths.direct_sync_root))
            onboarding._create_ledger(paths.ledger_path)
            onboarding._ensure_user_settings(paths)
            (paths.data_root / "operator-kept.txt").write_bytes(b"preserve this user-owned file\n")
        removal = onboarding.remove_current_user_setup(app.parent,
            autostart_remover=lambda: {"status": "ABSENT"},
            scheduled_task_remover=lambda *_: {"status": "ABSENT"})
        assert removal["status"] == "PASS_DATA_PRESERVED"
        protected = (paths.identity_path, paths.producer_manifest_path, paths.credential_path,
                     paths.registration_report_path, paths.logistics_profile_path,
                     paths.logistics_secret_path, paths.data_root / "operator-kept.txt")
        write(root / "preserved-bytes.json", {str(path): hashlib.sha256(path.read_bytes()).hexdigest()
                                            for path in protected})
        write(root / "seed-result.json", {"status": "PRESERVED_STOP_PROVEN",
                                         "old_version": updates.version_manifest(app.parent)["version"]})
        return 0
    if entry == "read-owner":
        owner = user_relay.start_user_relay_process(app.parent, survival_seconds=0)
        write(root / "update-owner-result.json", owner)
        return 0
    if entry == "stop":
        paths = onboarding.resolve_current_user_onboarding_paths(app.parent)
        report = user_relay.request_app_stop(paths.direct_sync_root, app_root=app.parent,
                                             webview_root=root / "unused-webview", timeout_seconds=150)
        write(root / "official-stop-result.json", {"status": report["status"],
              "remaining_process_count": report["remaining_process_count"]})
        return 0 if report["status"] == "STOPPED" else 1
    if not setup:
        if Path(entry).name == "auto_update.py":
            return updates.main(arguments)
        import label_match_relay_entry
        return label_match_relay_entry.main(arguments)
    paths = onboarding.resolve_current_user_onboarding_paths(app.parent)
    ready = False
    try:
        import label_match_product_host
        rc = label_match_product_host.dispatch_product_mode([
            "--onboard-current-user", "--app-root", str(app.parent), "--server-base-url", ORIGIN])
        assert rc == 0, "inspect isolated onboarding report and fixed diagnostics"
        report = json.loads(paths.onboarding_report_path.read_bytes())
        write(root / "result.json", {"status": report["status"], "relay_start": report["relay_start"],
            "registration_action": report["action"],
            "marker_was_present": report["stop_marker_release"]["marker_present"],
            "stop_release_status": report["stop_marker_release"]["release"]["status"]})
        ready = True
        return 0
    except BaseException as error:
        import traceback
        chain, seen, current_error = [], set(), error
        while current_error is not None and id(current_error) not in seen:
            seen.add(id(current_error))
            chain.append({"type": type(current_error).__name__,
                "update_mutex_busy": isinstance(current_error, RuntimeError)
                    and str(current_error) == "update mutex ownership is busy",
                "frames": [{"file": Path(f.filename).name, "function": f.name, "line": f.lineno}
                           for f in traceback.extract_tb(current_error.__traceback__)]})
            current_error = current_error.__cause__ or current_error.__context__
        write(root / "error.json", {"type": type(error).__name__,
            "cause_type": type(error.__cause__).__name__ if error.__cause__ else None,
            "frames": [{"file": Path(f.filename).name, "function": f.name, "line": f.lineno}
                       for f in traceback.extract_tb(error.__traceback__)], "chain": chain})
        raise
    finally:
        if entry != "keep-relay" or not ready:
            user_relay.request_user_relay_stop(paths.direct_sync_root, timeout_seconds=0)
        for child in children:
            child.wait()


def wait_for(config, predicate, *, seconds=600):
    """Only this test controller waits; product deadlines are never patched."""
    root = Path(config).parent
    deadline = time.monotonic() + seconds
    observed = None
    progress_at = time.monotonic()
    extended = False
    while time.monotonic() < deadline:
        result = predicate()
        if result:
            return result
        paths = [root / "machine/update-control/transition.json",
                 root / "machine/update-control/active.json", root / "stage/prepared.json"]
        shape = tuple((path.stat().st_size, path.stat().st_mtime_ns) if path.exists() else None for path in paths)
        if shape != observed:
            progress_at, observed = time.monotonic(), shape
        if (not extended and deadline - time.monotonic() < 30
                and time.monotonic() - progress_at < 60 and owned_processes_alive(config)):
            deadline += seconds
            extended = True
        time.sleep(min(30, max(0, deadline - time.monotonic())))
    raise TimeoutError("inspect bounded controller progress before another run")


def owned_processes_alive(config):
    import auto_update as updates
    for path in (Path(config).parent / "processes").glob("*.json"):
        row = json.loads(path.read_text())
        identity = updates.native_process_identity(row["ProcessId"])
        if (identity.get("status") == "LIVE"
                and identity.get("creation_identity") == row.get("CreationIdentity")):
            return True
    return False


def stop_owned_relays(config):
    """Normal product stop, scoped exclusively to this fixture's own root."""
    import auto_update as updates
    import user_relay
    config = Path(config)
    root = config.parent.resolve()
    direct = root / "local/KMTech/DirectSync/label_match"
    assert direct.resolve().is_relative_to(root)
    user_relay.request_user_relay_stop(direct, timeout_seconds=0)

    wait_for(config, lambda: not owned_processes_alive(config), seconds=120)


def ready(config, name="initial"):
    """The official headless product-host onboarding entry, with OS adapters."""
    config = Path(config)
    process = launch(config, "keep-relay")
    assert process.wait() == 0, "inspect isolated onboarding report and fixed frames"
    result = json.loads((config.parent / "result.json").read_bytes())
    assert result["status"] == "READY" and result["relay_start"]["status"] == "ALIVE"
    write(config.parent / (name + "-setup-result.json"), result)
    prove_next_cycle(config, name=name, process_id=result["relay_start"]["process_id"], accept_initial_pass=True)
    return result


def official_stop(config, name="restart"):
    config = Path(config)
    process = launch(config, "stop")
    assert process.wait() == 0, "official preserving app stop did not prove STOPPED"
    result = json.loads((config.parent / "official-stop-result.json").read_bytes())
    assert result["status"] == "STOPPED" and result["remaining_process_count"] == 0
    write(config.parent / (name + "-stop-result.json"), result)
    wait_for(config, lambda: not owned_processes_alive(config), seconds=120)


def session_end(config, name="restart"):
    """ANSWER-3: end only this birth-proven relay tree, without a stop marker."""
    import ctypes
    from ctypes import wintypes as W
    import auto_update as updates
    import user_relay
    config = Path(config)
    root = config.parent.resolve()
    direct = root / "local/KMTech/DirectSync/label_match"
    assert not user_relay.user_relay_stop_path(direct).exists()
    status = json.loads(user_relay.user_relay_status_path(direct).read_bytes())
    pid = status["process_id"]
    recorded = json.loads((root / "processes" / (str(pid) + ".json")).read_bytes())
    assert "--label-match-user-relay" in recorded["CommandLine"]
    kernel = ctypes.WinDLL("kernel32", use_last_error=True)
    kernel.OpenProcess.argtypes = [W.DWORD, W.BOOL, W.DWORD]
    kernel.OpenProcess.restype = W.HANDLE
    kernel.IsProcessInJob.argtypes = [W.HANDLE, W.HANDLE, ctypes.POINTER(W.BOOL)]
    kernel.CloseHandle.argtypes = [W.HANDLE]
    # Retain the process object through taskkill, preventing PID reuse.
    handle = kernel.OpenProcess(0x1000 | 0x100000, False, pid)
    assert handle
    try:
        identity = updates.native_process_identity(pid)
        assert identity.get("status") == "LIVE" and identity["creation_identity"] == recorded["CreationIdentity"]
        contained = W.BOOL()
        assert kernel.IsProcessInJob(handle, None, ctypes.byref(contained)) and contained.value
        result = subprocess.run([updates.system_executable("taskkill.exe"), "/PID", str(pid), "/T", "/F"],
            capture_output=True, creationflags=subprocess.CREATE_NO_WINDOW)
        assert result.returncode in (0, 128)
    finally:
        kernel.CloseHandle(handle)
    wait_for(config, lambda: not owned_processes_alive(config), seconds=120)
    assert not user_relay.user_relay_stop_path(direct).exists()
    write(root / (name + "-session-end.json"), {"native_birth_proven": True,
          "job_contained": True, "owned_tree_ended": True, "stop_marker_present": False})


def _autorun_trial_record(config):
    path = Path(config).parent / "machine/update-control/transition.json"
    try:
        assert path.stat().st_size < 65536
        value = json.loads(path.read_bytes())
    except FileNotFoundError:
        return None
    assert isinstance(value, dict), "bootstrap trial journal is malformed"
    return value


def _wait_for_autorun_trial(config, process, returncode):
    """Keep the candidate alive while the existing SYSTEM trial settles."""
    from datetime import datetime
    from auto_update import READY_WINDOW_SECONDS

    root = Path(config).parent
    state = _autorun_trial_record(config)
    assert state, "nonzero bootstrap has no SYSTEM trial"
    def stamp(value):
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        assert parsed.tzinfo is not None, "trial clock must include its timezone"
        return parsed.timestamp()
    applied = stamp(state["applied_at"])
    binding = tuple(state.get(k) for k in ("transition_id", "candidate_sha", "applied_at"))
    assert all(binding), "nonzero bootstrap lacks its original trial binding"
    deadline = time.monotonic() + max(0.0, applied + READY_WINDOW_SECONDS + 60 - time.time())
    began = time.monotonic()
    terminal = {"CONFIRMED", "ROLLED_BACK", "DEFERRED"}
    timed_out = False
    while state.get("phase") not in terminal:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            timed_out = True
            break
        time.sleep(min(30, remaining))
        state = _autorun_trial_record(config)
        same = bool(state) and tuple(state.get(k) for k in ("transition_id", "candidate_sha", "applied_at")) == binding
        assert same, "SYSTEM trial changed while observing bootstrap outcome"
    log = Path(process.host_test_output)
    assert log.stat().st_size < 1024 ** 2
    text = log.read_text(encoding="utf-8", errors="replace")
    known_tail = (returncode == 1 and "in _bootstrap" in text
                  and "in assert_ordinary_recovery" in text
                  and "ValueError: an earlier or unrelated writer prevents ordinary recovery" in text)
    confirmed_seconds = None
    if state.get("confirmed_at"):
        confirmed_seconds = stamp(state["confirmed_at"]) - applied
    proof = dict(bootstrap_return_code=returncode, known_p3_tail=known_tail,
                 phase=state.get("phase"), wait_seconds=time.monotonic()-began,
                 observation_upper_seconds=READY_WINDOW_SECONDS+60,
                 confirmed_after_applied_seconds=confirmed_seconds, timed_out=timed_out,
                 candidate_stopped_by_observer=False)
    write(root / "bootstrap-outcome-proof.json", proof)
    assert not timed_out, "SYSTEM trial did not settle by its original applied deadline plus60"
    assert known_tail, "bootstrap failed outside the allowed secondary P3 tail"
    assert state.get("phase") == "CONFIRMED", "SYSTEM did not confirm this trial"
    assert confirmed_seconds is not None and 0 <= confirmed_seconds <= READY_WINDOW_SECONDS, "SYSTEM confirmation missed the original READY window"


def autorun(config):
    """Parse and execute the exact value the real installer put in virtual HKCU."""
    import ctypes
    from ctypes import wintypes as W
    config = Path(config)
    command = json.loads((config.parent / "hkcu-run.json").read_bytes())["command"]
    shell = ctypes.WinDLL("shell32", use_last_error=True)
    shell.CommandLineToArgvW.argtypes = [W.LPCWSTR, ctypes.POINTER(ctypes.c_int)]
    shell.CommandLineToArgvW.restype = ctypes.POINTER(W.LPWSTR)
    kernel = ctypes.WinDLL("kernel32", use_last_error=True)
    kernel.LocalFree.argtypes = [ctypes.c_void_p]
    count = ctypes.c_int()
    argv = shell.CommandLineToArgvW(command, ctypes.byref(count))
    assert argv
    try:
        parts = [argv[index] for index in range(count.value)]
    finally:
        kernel.LocalFree(argv)
    assert parts[1:3] == ["-I", "-B"] and Path(parts[3]).name == "auto_update.py"
    assert parts[4:7] == ["--bootstrap", "--role", "relay"]
    write(config.parent / "autorun-proof.json", {"registered_command_exact": True,
          "isolated_python_flags": parts[1:3], "entry": Path(parts[3]).name})
    process = launch(config, parts[3], parts[4:])
    returncode = process.wait()
    if returncode != 0:
        _wait_for_autorun_trial(config, process, returncode)


def restart(config, name="restart"):
    session_end(config, name)
    autorun(config)
    result = ready(config, name)
    assert not result["marker_was_present"]
    return result


def prove_next_cycle(config, *, name="initial", process_id=None, accept_initial_pass=False):
    """ANSWER-1: observe the unchanged relay, at most two further cycles/180s."""
    import user_relay
    config = Path(config)
    root = config.parent
    deadline = time.monotonic() + 180
    result = {"same_relay_pass": False, "proof": name}
    while time.monotonic() < deadline:
        events = []
        for path in (root / "cycle-observations").glob("*.jsonl"):
            events += [json.loads(line) for line in path.read_text().splitlines() if line.strip()]
        running = sorted((e for e in events if e["status"] == "RUNNING"
                          and (process_id is None or e["pid"] == process_id)), key=lambda e: e["at"])
        failed = next((e for e in running if e["process_returncode"] not in (None, 0)), None)
        if not failed and accept_initial_pass and running:
            first = running[0]
            if first["process_status"] == "PASS" and first["process_returncode"] == 0:
                result.update(same_relay_pass=True, first_cycle_refused=False,
                              elapsed_seconds=0, further_cycles=0)
                break
        if failed:
            good = next((e for e in running if e["pid"] == failed["pid"]
                         and e["at"] > failed["at"] and e["process_status"] == "PASS"
                         and e["process_returncode"] == 0), None)
            elapsed = (good["at"] if good else time.monotonic()) - failed["at"]
            result.update(first_cycle_refused=True, elapsed_seconds=elapsed,
                          further_cycles=(good or running[-1])["cycle"] - failed["cycle"])
            if good:
                result["same_relay_pass"] = elapsed <= 180 and result["further_cycles"] <= 2
                break
            if elapsed >= 180 or running[-1]["cycle"] - failed["cycle"] >= 2:
                break
        time.sleep(1)
    direct = root / "local/KMTech/DirectSync/label_match"
    result["stop_marker_present"] = user_relay.user_relay_stop_path(direct).exists()
    lingering = []
    support_codes = {}
    rules = json.loads((Path(value_path(config, "repo")) / "tools/failure_redaction_rules.json").read_bytes())
    allowed_codes = set(rules["allowed_codes"])
    non_allowlisted = 0
    paths = [*(root / "local").rglob("*.json"), *(root / "local").rglob("*.jsonl")]
    support = root / "stage/startup-support.json"
    if support.exists():
        paths.append(support)
    for path in paths:
        if path.stat().st_size > 65536:
            continue
        try:
            payload = ([json.loads(line) for line in path.read_text().splitlines() if line.strip()]
                       if path.suffix == ".jsonl" else json.loads(path.read_bytes()))
        except (ValueError, OSError):
            continue
        def inspect(value):
            nonlocal non_allowlisted
            if isinstance(value, dict):
                for key, child in value.items():
                    if key in {"status", "cause_code", "reason_code", "code"} and isinstance(child, str):
                        if child in {"RECOVERY_REQUIRED", "RELAY_RESUME_REQUIRED"}:
                            lingering.append({"file": path.name, "field": key, "code": child})
                        if key in {"reason_code", "cause_code", "code"} and child:
                            if child in allowed_codes:
                                support_codes[child] = support_codes.get(child, 0) + 1
                            else:
                                non_allowlisted += 1
                    inspect(child)
            elif isinstance(value, list):
                for child in value:
                    inspect(child)
        inspect(payload)
    result.update(lingering_recovery=lingering, support_code_counts=support_codes,
                  non_allowlisted_code_count=non_allowlisted)
    result["screen_decisions_observed"] = all("screen_warning_decision" in event for event in running)
    result["screen_warning_decisions"] = sum(event.get("screen_warning_decision") is True for event in running)
    runtime = direct / "status/direct_sync_relay_status.json"
    runtime_state = json.loads(runtime.read_bytes()) if runtime.exists() else {}
    result.update(runtime_status_is_idle=runtime_state.get("status") == "idle",
                  runtime_error_code_present=bool(runtime_state.get("error_code")),
                  runtime_error_message_present=bool(runtime_state.get("error_message")))
    http_path = root / "http-observations.jsonl"
    http = [json.loads(line) for line in http_path.read_text().splitlines()] if http_path.exists() else []
    result["http_request_counts"] = {kind: sum(row["kind"] == kind for row in http)
                                     for kind in ("enroll", "runtime_lease", "update_read", "unmodelled")}
    write(root / (name + "-cycle-proof.json"), result)
    assert result["same_relay_pass"], "ANSWER-1 condition 1 failed; retain scene and stop for lane design"
    assert not result["stop_marker_present"] and not lingering
    assert not result["runtime_error_code_present"] and not result["runtime_error_message_present"]
    assert result["http_request_counts"]["unmodelled"] == 0
    assert result["screen_decisions_observed"] and result["screen_warning_decisions"] == 0
    return result


def value_path(config, name):
    return json.loads(Path(config).read_text())[name]
