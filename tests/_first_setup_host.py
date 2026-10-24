"""Private OS adapters for the headless first-setup process-chain tests.

Product policy, manifests, writer admission, update mutex, onboarding, relay
cycles, and owner readback are executed from the isolated packet. Only native
identity, external helpers/tasks, credential protection, and HTTP are virtual.
"""
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
                   "code_root": str(root.resolve()), "file_count": len(rows), "files": rows,
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


def observe_script(helper, current, processes):
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
        foreach ($item in $script:testProcesses) {
            $item.CreationDate = [datetime]$item.CreationDate
            $native=$null
            try {
                $native=[Diagnostics.Process]::GetProcessById([int]$item.ProcessId)
                $difference=$native.StartTime.ToUniversalTime().Ticks-$item.CreationDate.ToUniversalTime().Ticks
                if (-not $native.HasExited -and $difference -ge 0 -and $difference -lt 10) { $item }
            } catch {} finally { if ($null -ne $native) { $native.Dispose() } }
        }
    }
    function Invoke-CimMethod {
        param($InputObject,$MethodName)
        $sid=if ($InputObject.Sid) { [string]$InputObject.Sid } else { 'WORKER' }
        return @{ReturnValue=0;Sid=$sid}
    }
'''.replace("PROCESSES", str(processes).replace("'", "''")).replace("WORKER", SID)
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


def prepare(root, source, packet, *, preserved=False, signer=None, feed_shape="signed"):
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
    write(current / "app/update-manifest-key-config.json", {"manifest_public_key": json.dumps(public)})
    refresh_file_pins(current, manifest)
    manifest["writer_sink_inventory_contract_sha256"] = updates.version_writer_inventory_sha256(current)
    write(current / "portable-manifest.json", manifest)
    relocate_ancestor_boundary(current, root)
    inventory(current)
    selected = root / "machine/versions" / manifest["source_commit"]
    shutil.copytree(current, selected)
    inventory(selected)
    policy = {**settings, "operator_sid": SID, "operator_local_app_data": str(root / "local"),
              "settings_sha256": hashlib.sha256(raw).hexdigest(), "staging_root": str(root / "stage")}
    control = root / "machine/update-control"
    write(control / "policy.json", policy)
    write(control / "active.json", {"schema": updates.SCHEMA, "sha": manifest["source_commit"]})
    write(control / "transition.json", {"schema": updates.SCHEMA, "phase": "DEFERRED",
                                        "deferred_at": updates.now_utc()})
    (root / "local").mkdir()
    (root / "stage").mkdir()
    config = root / "host-config.json"
    write(config, {"root": str(root), "repo": str(source), "current": str(current),
                   "selected": str(selected), "control": str(control), "slow_observe": 15,
                   "feed_shape": feed_shape})
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
    write(control / "active.json", {"schema": updates.SCHEMA, "sha": test_sha})
    write(control / "transition.json", {"schema": updates.SCHEMA, "phase": "DEFERRED",
                                        "deferred_at": updates.now_utc()})
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
        write(bundle / "app/update-manifest-key-config.json", {"manifest_public_key": json.dumps(public)})
        declared = json.loads((bundle / "portable-manifest.json").read_bytes())
        declared["update_key_config_sha256"] = hashlib.sha256(
            (bundle / "app/update-manifest-key-config.json").read_bytes()).hexdigest()
        write(bundle / "portable-manifest.json", declared)
        inventory(bundle)


def offer_update(config, signer):
    """Use the existing signed canonical update-fixture format for 124 → 125."""
    import auto_update as updates
    from tests.test_update_manifest import canonical_manifest
    value = json.loads(config.read_text())
    root, current = config.parent, Path(value["selected"])
    before = updates.version_manifest(current)
    assert before["version"] == "v2.0.124"
    candidate = root / "offer/Label_Match"
    assert not candidate.exists()
    shutil.copytree(current, candidate)
    receipt = (candidate / "bootstrap-integrity.json").resolve()
    assert receipt.is_relative_to((root / "offer").resolve())
    receipt.unlink()
    label = candidate / "app/Label_Match.py"
    raw = label.read_bytes()
    assert b'APP_VERSION = "v2.0.124"' in raw
    label.write_bytes(raw.replace(b'APP_VERSION = "v2.0.124"', b'APP_VERSION = "v2.0.125"', 1))
    # As in test_auto_update_endpoints, this is only an application-version
    # copy. Data format/constants, code paths, and the trusted key stay intact.
    hot = hashlib.sha1((before["source_commit"] + ":host125").encode()).hexdigest()
    manifest = json.loads((candidate / "portable-manifest.json").read_bytes())
    manifest.update(version="v2.0.125", source_commit=hot)
    write(candidate / "portable-manifest.json", manifest)
    after = updates.version_identity(lambda name: (candidate / name).read_bytes())
    forward = {"from_commit": before["source_commit"], "to_commit": hot,
               "from_contract_sha256": before["contract_sha256"], "to_contract_sha256": after["contract_sha256"]}
    reverse = {"from_commit": hot, "to_commit": before["source_commit"],
               "from_contract_sha256": after["contract_sha256"], "to_contract_sha256": before["contract_sha256"]}
    pin = lambda record: hashlib.sha256(json.dumps(record, sort_keys=True, separators=(",", ":")).encode()).hexdigest()
    packet = root / "offer/Label_Match-v2.0.125.zip"
    with zipfile.ZipFile(packet, "x", compression=zipfile.ZIP_DEFLATED) as archive:
        for path in sorted(candidate.rglob("*")):
            if path.is_file():
                archive.write(path, "Label_Match/" + path.relative_to(candidate).as_posix())
    feed = canonical_manifest()
    feed.update(signature_version="es256-v1", source_commit=hot, version="v2.0.125",
        approved_transitions=[forward, reverse],
        auto_update={"from_commit": before["source_commit"], "to_commit": hot,
                     "forward_contract_sha256": pin(forward), "rollback_contract_sha256": pin(reverse)})
    url = ORIGIN + "/artifacts/Label_Match-v2.0.125.zip"
    with packet.open("rb") as stream:
        digest = hashlib.file_digest(stream, "sha256").hexdigest()
    feed["artifact"].update(name="Label_Match-v2.0.125.zip", url=url,
                            size_bytes=packet.stat().st_size, sha256=digest)
    write(root / "offer/feed.json", feed)
    (root / "offer/feed.sig").write_bytes(signer.sign_es256(
        json.dumps(feed, sort_keys=True, separators=(",", ":"), ensure_ascii=True).encode()))
    write(root / "offer-info.json", {"source_commit": hot, "artifact_url": url,
                                     "packet": str(packet), "from_commit": before["source_commit"]})
    (root / "offer-ready").touch()
    return hot


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
            event = {"at": time.monotonic(), "pid": os.getpid(),
                     "status": payload.get("status"), "cycle": payload.get("cycle_count"),
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
            self.leases = None
        def get(self, url, **_kwargs):
            assert url.startswith(ORIGIN + "/"), "test HTTP origin differs"
            if value["feed_shape"] == "test-pc":
                observe_http("feed_404")
                response = Response(url, root / "feed.json")
                response.status_code = 404
                return response
            feed_root = root / "offer" if self.offered else root
            paths = {ORIGIN + "/feed/latest.json": feed_root / "feed.json",
                     ORIGIN + "/feed/latest.json.sig": feed_root / "feed.sig"}
            if self.offered:
                offered = json.loads((root / "offer-info.json").read_text())
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
                rows = []
                for path in (root / "processes").glob("*.json"):
                    row = json.loads(path.read_text())
                    if updates.native_process_identity(row["ProcessId"]).get("status") == "LIVE":
                        rows.append(row)
                processes = root / ("query-" + str(os.getpid()) + ".json")
                write(processes, rows)
                script = observe_script(current / "tools/apply_prepared_update.ps1", current, processes)
                import user_relay
                status = user_relay.user_relay_status_path(
                    onboarding.resolve_current_user_onboarding_paths(selected).direct_sync_root)
                slow = app == current / "app" and status.exists() and not (root / "slow-observe-started").exists()
                if slow:
                    (root / "slow-observe-started").touch()
                    script = "Start-Sleep -Seconds " + str(value["slow_observe"]) + "\n" + script
                target = root / ("query-" + str(os.getpid()) + ".ps1")
                target.write_text(script, encoding="utf-8-sig")
                before = time.monotonic()
                output = _query(target)
                if slow:
                    write(root / "slow-observe-proof.json", {"elapsed_seconds": time.monotonic() - before,
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
            index, entry = entries[0]
            process = launch(config, str(entry), command[index + 1:])
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
        children.append(process)
        return process
    updates.start_ordinary_process = start
    return children


def child_main(config, entry, arguments):
    config = Path(config)
    value = json.loads(config.read_text())
    setup = entry in {"onboard", "seed", "keep-relay", "read-owner", "stop"}
    app = Path(value["current"]) / "app" if setup else Path(entry).parent
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
    write(root / "processes" / (str(os.getpid()) + ".json"), {
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
        paths = onboarding.resolve_current_user_onboarding_paths(Path(value["current"]))
        assert onboarding._registration_runner(paths, server_base_url=ORIGIN) == 0
        assert onboarding.inspect_current_user_state(paths)["status"] == "READY"
        with fence.writer_admission("current_user_onboarding"):
            onboarding.record_user_data_open(paths.app_root, (paths.data_root, paths.direct_sync_root))
            onboarding._create_ledger(paths.ledger_path)
            onboarding._ensure_user_settings(paths)
            (paths.data_root / "operator-kept.txt").write_bytes(b"preserve this user-owned file\n")
        removal = onboarding.remove_current_user_setup(Path(value["current"]),
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
        owner = user_relay.start_user_relay_process(Path(value["current"]), survival_seconds=0)
        write(root / "update-owner-result.json", owner)
        return 0
    if entry == "stop":
        paths = onboarding.resolve_current_user_onboarding_paths(Path(value["current"]))
        report = user_relay.request_app_stop(paths.direct_sync_root, app_root=Path(value["current"]),
                                             webview_root=root / "unused-webview", timeout_seconds=150)
        write(root / "official-stop-result.json", {"status": report["status"],
              "remaining_process_count": report["remaining_process_count"]})
        return 0 if report["status"] == "STOPPED" else 1
    if not setup:
        if Path(entry).name == "auto_update.py":
            return updates.main(arguments)
        import label_match_relay_entry
        return label_match_relay_entry.main(arguments)
    paths = onboarding.resolve_current_user_onboarding_paths(Path(value["current"]))
    ready = False
    try:
        import label_match_product_host
        rc = label_match_product_host.dispatch_product_mode([
            "--onboard-current-user", "--app-root", str(Path(value["current"])), "--server-base-url", ORIGIN])
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
    assert process.wait() == 0, "actual registered autostart did not complete its bootstrap"


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
