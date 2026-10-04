"""Run one real Python process with synthetic identity and lifecycle stand-ins."""
import contextlib
import importlib
import io
import json
import os
from pathlib import Path
import sys
import sqlite3
from types import SimpleNamespace

repo, root, entry, state, output = sys.argv[1:]
repo, root = Path(repo), Path(root)
sys.path.insert(0, str(repo))
for name, relative in {"LOCALAPPDATA": "local", "APPDATA": "roaming", "PROGRAMDATA": "program",
                       "LABEL_MATCH_SAVE_DIR": "data", "LABEL_MATCH_DIRECT_SYNC_ROOT": "relay",
                       "KMTECH_LABEL_WRITER_CONTROL_ROOT": "control"}.items():
    os.environ[name] = str(root / relative)
os.environ["KMTECH_LABEL_WRITER_TEST_MODE"] = "1"
os.environ["LABEL_MATCH_SETTINGS_PATH"] = str(root / "settings.json")
import protected_admin
import label_data_generation as generation
from kmtech_shared.data_generation import record_generation
SID = "S-1-5-21-900-901-902-1001"
protected_admin.current_process_sid = lambda: SID
generation._installed_identity = lambda _root: {
    "version": generation.CURRENT_VERSION, "build_id": "b" * 40,
    "data_compatible_min_version": generation.CURRENT_VERSION, "compatibility_verified": False,
}
root.mkdir(parents=True, exist_ok=True)
for marker in generation.marker_paths((root / "data", root / "relay"), SID):
    marker.parent.mkdir(parents=True, exist_ok=True)
    record_generation(marker, "v3.0.0" if state == "old" else generation.CURRENT_VERSION,
                      app_id=generation.APP_ID, subject_sid=SID, build_id="a" * 40)
    if state == "corrupt":
        marker.write_text('{"broken":true}', encoding="utf-8")
for relative in ("settings.json", "relay/producer_identity.json", "relay/credential.json",
                 "relay/producer_manifest.json", "relay/status/current_user_onboarding.json",
                 "relay/status/user_relay.json", "relay/control/user-relay-stop.json",
                 "run-state.json", "task-state.json"):
    path = root / relative
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text('{"preserved":true}', encoding="utf-8")
identity = {"schema_version": "label-match-producer-identity-v1", "producer_id": "synthetic-producer",
            "source_host_id": "synthetic-host", "producer_install_id": "synthetic-install", "pc_id": "synthetic-pc"}
(root / "relay/producer_identity.json").write_text(json.dumps(identity), encoding="utf-8")
database = root / "relay/queue/direct_sync_relay.sqlite3"
database.parent.mkdir(parents=True, exist_ok=True)
with sqlite3.connect(database) as connection:
    connection.execute("CREATE TABLE relay_batches (status TEXT)")
    connection.execute("INSERT INTO relay_batches VALUES ('pending')")
events = {"identity_reads": 0, "data_reads": 0, "registration_calls": 0, "mutations": 0}
def mutate(*_args, **_kwargs):
    events["mutations"] += 1
    return {"status": "ALIVE"}
def network(*_args, **_kwargs):
    events["registration_calls"] += 1
    raise RuntimeError("isolated network trap")
import requests
requests.sessions.Session.request = network
import current_user_onboarding as onboarding
import user_relay
onboarding.read_legacy_system_task_quiescence = lambda: {
    "schema": onboarding.LEGACY_TASK_QUIESCENCE_VERSION,
    "required_state": onboarding.LEGACY_TASK_REQUIRED_STATE, "read_only": True,
    "task_or_process_mutated": False, "status": "PASS",
}
onboarding.verify_bootstrap_integrity = lambda *_args, **_kwargs: {"status": "PASS"}
user_relay.remove_user_relay_autostart = mutate
user_relay.install_user_relay_autostart = mutate
user_relay.request_user_relay_stop = mutate
onboarding.remove_current_user_scheduled_task = mutate
onboarding.start_user_relay_process = mutate
def relay_profile(*_args, **_kwargs):
    (root / "relay/credential.json").read_text()
    return SimpleNamespace(tls_ca_bundle_path="")
import logistics_runtime_profile
logistics_runtime_profile.load_logistics_runtime_profile = relay_profile
import winreg
winreg.SetValueEx = mutate
winreg.DeleteValue = mutate
winreg.CreateKeyEx = mutate
before = {str(p.relative_to(root)): p.read_bytes().hex() for p in root.rglob("*") if p.is_file()}
original_open = Path.open
def observed_open(path, mode="r", *args, **kwargs):
    if "r" in mode and path.name in {"producer_identity.json", "credential.json", "producer_manifest.json", "settings.json"}:
        events["identity_reads"] += 1
    return original_open(path, mode, *args, **kwargs)
Path.open = observed_open
original_connect = sqlite3.connect
def observed_connect(*args, **kwargs):
    events["data_reads"] += 1
    return original_connect(*args, **kwargs)
sqlite3.connect = observed_connect
module_name, function_name = entry.rsplit(":", 1)
module = importlib.import_module(module_name)
common = ["--app-root", str(repo)]
argv = {
    "registrar": ["--apply", "--sync-dir", str(root / "data"), "--data-dir", str(root / "relay"),
                  "--enrollment-token-env", "", "--machine-guid", "synthetic-guid"],
    "registrar-dry": ["--dry-run", "--sync-dir", str(root / "data"), "--data-dir", str(root / "relay"),
                      "--enrollment-token-env", "", "--machine-guid", "synthetic-guid"],
    "onboarding": common,
    "removal": common,
    "relay": [*common, "--once"],
    "main-app": [],
    "persistent": [],
    "hosted-relay": [*common, "--once"],
    "hosted-raw": [],
    "registrar-admin": [],
    "registrar-partial": [],
    "check-profile": ["--profile-path", str(root / "relay/logistics_runtime_profile.json")],
    "install-profile": ["--profile-path", str(root / "relay/logistics_runtime_profile.json"),
        "--base-url", "https://example.invalid", "--authority-scope", "synthetic", "--authority-epoch", "1",
        "--plane-epoch", "1", "--device-id", "synthetic", "--source-host-id", "synthetic", "--dry-run"],
    "install-admin": ["--profile-path", str(root / "admin.json"), "--dry-run"],
    "install-pack": [*common, "--program-data-root", str(root / "relay"),
        "--scan-source-dir", str(root / "data"), "--report-path", str(root / "install.json"), "--self-enroll"],
    "exact": ["capture", "--client-db", str(root / "relay/queue/direct_sync_relay.sqlite3"),
        "--server-db", str(root / "server.sqlite3"), "--identity", str(root / "relay/producer_identity.json"),
        "--credential", str(root / "relay/credential.json"), "--portable-root", str(repo), "--output", str(root / "receipt.json")],
    "reconcile": sum((["--" + name, str(root / name)] for name in (
        "preimage", "client-backup", "live-client-proof", "server-initializer-proof", "server-source-root",
        "server-launcher", "server-executable", "client-db", "server-db", "identity", "credential",
        "stop-marker", "portable-root", "receipt-output", "evidence-output")), [])
        + sum((["--" + name, "a" * 64] for name in (
        "client-backup-sha256", "live-client-proof-sha256", "server-initializer-proof-sha256",
        "server-launcher-sha256", "server-executable-sha256", "tls-ca-bundle-sha256")), [])
        + ["--server-source-commit", "a" * 40],
    "operator": ["status", "--db-path", str(root / "relay/queue/direct_sync_relay.sqlite3")],
    "operator-pause": ["pause", "--operator-pause-path", str(root / "relay/control/pause.json"),
        "--operator-id", "synthetic", "--reason", "synthetic"],
    "operator-retry": ["retry-dead", "--db-path", str(database), "--relay-id", "synthetic",
        "--operator-id", "synthetic", "--reason", "synthetic"],
    "canary": ["--work-dir", str(root / "canary"), "--report-path", str(root / "canary/report.json"),
               "--credential-path", str(root / "relay/credential.json"), "--live-root", str(root / "data")],
    "raw": ["--db-path", str(root / "relay/queue/direct_sync_relay.sqlite3"),
            "--spool-dir", str(root / "relay/spool"), "--producer-manifest-path", str(root / "relay/producer_manifest.json"),
            "--credential-path", str(root / "relay/credential.json"), "--upload-status-dir", str(root / "relay/upload_status"),
            "--runtime-status-path", str(root / "relay/status/raw.json"), "--log-path", str(root / "relay/logs/raw.log")],
}
kind = os.environ["KMTECH_DATA_GENERATION_ENTRY_KIND"]
argv["hosted-raw"] = argv["raw"]
argv["registrar-admin"] = [*argv["registrar"], "--admin-recovery-secret-file", str(root / "authorization.json"),
    "--pc-id", "synthetic-pc", "--source-host-id", "synthetic-host", "--producer-install-id", "synthetic-install",
    "--producer-id", "synthetic-producer",
    "--expected-active-manifest-hash", "a" * 64]
argv["registrar-partial"] = [*argv["registrar-admin"], "--recover-partial-local-state"]
if kind.startswith("registrar"):
    module.apply_registration = network
stdout, stderr = io.StringIO(), io.StringIO()
error = ""
with contextlib.redirect_stdout(stdout), contextlib.redirect_stderr(stderr):
    try:
        if kind == "onboarding":
            # Function defaults were bound at import; pass explicit stand-ins.
            module.onboard_current_user(repo, legacy_task_quiescence_reader=onboarding.read_legacy_system_task_quiescence,
                autostart_installer=mutate, scheduled_task_remover=mutate, relay_launcher=mutate)
            exit_code = 0
        elif kind == "persistent":
            user_relay.run_persistent_relay_loop(
                lambda: generation.record_user_data_open(repo, (root / "data",)),
                status_path=root / "relay/status/user_relay.json", max_cycles=1)
            exit_code = 0
        elif kind == "hosted-relay":
            exit_code = module.dispatch_product_mode([module.USER_RELAY_MODE, *argv[kind]])
        elif kind == "hosted-raw":
            exit_code = module.dispatch_product_mode([module.DIRECT_SYNC_RELAY_MODE, *argv[kind]])
        else:
            exit_code = getattr(module, function_name)(argv[kind])
    except Exception as exc:
        error = str(exc)
        exit_code = 2
Path.open = original_open
sqlite3.connect = original_connect
after = {str(p.relative_to(root)): p.read_bytes().hex() for p in root.rglob("*") if p.is_file()}
changed = sorted(name for name in before if after.get(name) != before[name])
added = sorted(name for name in after if name not in before and not name.startswith("control/"))
message = stdout.getvalue() + stderr.getvalue() + error
result = {"entry": kind, "state": state, **events, "exit_code": exit_code,
          "changed": changed, "added": added, "korean_guidance": "자료" in message and "복구" in message,
          "passed": events == {"identity_reads": 0, "data_reads": 0, "registration_calls": 0, "mutations": 0}
                    and not changed and not [p for p in added if "startup-trace" not in p]
                    and exit_code != 0 and "자료" in message and "복구" in message}
Path(output).write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")
print(json.dumps(result, ensure_ascii=False))
