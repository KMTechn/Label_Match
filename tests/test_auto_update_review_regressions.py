"""Review regressions using real functions; no installer, relay or GUI starts."""
from pathlib import Path
import hashlib
import json
import subprocess
import time
from contextlib import nullcontext
from types import SimpleNamespace

import pytest
import auto_update as updates


ROOT = Path(__file__).resolve().parents[1]

FEED_URL_CASES = [
    ("https://company.test/feed", True), ("https://updates.factory/feed", True),
    ("https://192.0.2.10:8443/feed", True), ("https://[2001:db8::1]:8443/feed", True),
    ("https://xn--bcher-kva.test/feed", True), ("HTTPS://COMPANY.TEST/feed", True),
    ("hTtPs://CoMpAnY.TeSt/feed", True),
    ("https://bücher.test/feed", False), ("https://@company.test/feed", False),
    ("https://:password@company.test/feed", False), ("https://company.test:bad/feed", False),
    ("https://company.test:65536/feed", False), ("http://company.test/feed", False),
    ("https://company.test/feed#fragment", False), ("https://github.com/feed", False),
    ("https://alias.github.com/feed", False), ("https://raw.githubusercontent.com/feed", False),
    ("https://alias.github.io./feed", False),
]


@pytest.mark.parametrize("url,accepted", FEED_URL_CASES)
def test_installer_embedded_and_runtime_feed_url_contract(url, accepted):
    source = (ROOT / "INSTALL_CANONICAL_PORTABLE.ps1").read_text(encoding="utf-8-sig")
    pure = source.split("from typing import Any, Mapping", 1)[1].split("startup_release_replacements =", 1)[0]
    namespace = {"Any": object, "Mapping": dict, "re": __import__("re"), "json": json}
    exec(pure, namespace)
    for validator in (updates.policy_from_settings, namespace["policy_from_settings"]):
        if accepted:
            assert validator({"feed_url": url})["feed_url"] == url
        else:
            with pytest.raises(ValueError):
                validator({"feed_url": url})


def test_pc_installer_feed_url_contract_without_installing(tmp_path):
    from tests.test_startup_update_installer import functions
    cases = tmp_path / "urls.json"
    cases.write_text(json.dumps([url for url, _accepted in FEED_URL_CASES]), encoding="utf-8")
    body = functions("INSTALL_THIS_PC.ps1", ["Assert-LabelStartupUpdateSettings"])
    result = powershell(tmp_path, body + f"""
$urls=Get-Content -LiteralPath {quote(cases)} -Raw -Encoding UTF8 | ConvertFrom-Json
foreach ($url in $urls) {{
    try {{ Assert-LabelStartupUpdateSettings ([pscustomobject]@{{feed_url=$url;channel='stable';apply_during_work=$false;profile_auth=$true}}); 'ACCEPT' }}
    catch {{ 'REJECT' }}
}}
""")
    assert result.splitlines() == ["ACCEPT" if accepted else "REJECT" for _url, accepted in FEED_URL_CASES]


def quote(value):
    return "'" + str(value).replace("'", "''") + "'"


@pytest.mark.parametrize("scheme", ["HTTPS", "hTtPs"])
@pytest.mark.parametrize("authority", ["@company.test", "bücher.test"])
def test_feed_invalid_authority_rejected_across_scheme_case(tmp_path, scheme, authority):
    from tests.test_startup_update_installer import functions

    url = f"{scheme}://{authority}/feed"
    test_installer_embedded_and_runtime_feed_url_contract(url, False)
    settings = tmp_path / "settings.json"
    settings.write_text(json.dumps({"feed_url": url, "channel": "stable",
        "apply_during_work": False, "profile_auth": True}), encoding="utf-8")
    body = functions("INSTALL_THIS_PC.ps1", ["Assert-LabelStartupUpdateSettings"])
    result = powershell(tmp_path, body + f"""
$settings=Get-Content -LiteralPath {quote(settings)} -Raw -Encoding UTF8 | ConvertFrom-Json
try {{ Assert-LabelStartupUpdateSettings $settings; 'ACCEPT' }} catch {{ 'REJECT' }}
""")
    assert result == "REJECT", f"PC/runtime authority mismatch for {url}: {result}"


def powershell(tmp_path, code):
    script = tmp_path / "actual-functions.ps1"
    script.write_text("$ErrorActionPreference='Stop'\n" + code, encoding="utf-8-sig")
    result = subprocess.run(["powershell.exe", "-NoProfile", "-NonInteractive", "-File", str(script)],
                            cwd=tmp_path, capture_output=True, text=True,
                            creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0))
    assert result.returncode == 0, result.stdout + result.stderr
    return result.stdout.strip()


@pytest.mark.parametrize("trial", [False, True])
def test_ordinary_user_reopens_native_update_mutex(tmp_path, trial):
    if not updates.ordinary_user_context():
        pytest.skip("native reopening requires an ordinary interactive caller")
    # The unique tmp path hashes to a fresh object; no product mutex is touched.
    sid = updates.current_process_sid()
    with updates.update_lock(tmp_path, sid, trial=trial):
        with updates.update_lock(tmp_path, sid, trial=trial):
            pass


@pytest.mark.parametrize("existing_version", [False, True])
def test_initialize_uses_complete_placement_function_bundle(tmp_path, existing_version):
    current = tmp_path / "current"
    (current / "app").mkdir(parents=True)
    raw = updates.canonical_startup_settings({})
    (current / "app/auto-update-settings.json").write_bytes(raw)
    (current / "portable-manifest.json").write_text(json.dumps({
        "source_commit": "1" * 40, "startup_update_protocol": 1,
    }), encoding="utf-8")
    if existing_version:
        (tmp_path / "versions" / ("1" * 40)).mkdir(parents=True)
    # Load every real top-level function, with the same two preloaded scripts
    # as InvokeFrozenPlacementHelper. No canonical wrapper/Sha function exists.
    output = powershell(tmp_path, f"""
. {quote(ROOT/'tools/bootstrap_integrity.ps1')} -SharedCodeRoot {quote(ROOT)}
. {quote(ROOT/'tools/label_writer_fence.ps1')}
$tokens=$null; $errors=$null
$tree=[Management.Automation.Language.Parser]::ParseFile({quote(ROOT/'INSTALL_THIS_PC.ps1')},[ref]$tokens,[ref]$errors)
if($errors.Count){{throw 'Actual placement parse failed'}}
$bundle=@($tree.EndBlock.Statements | Where-Object {{ $_ -is [Management.Automation.Language.FunctionDefinitionAst] }})
if($bundle.Count -lt 20){{throw 'Incomplete placement function bundle'}}
. ([ScriptBlock]::Create(($bundle.Extent.Text -join [Environment]::NewLine)))
if(Get-Command Sha -ErrorAction SilentlyContinue){{throw 'Canonical wrapper leaked into placement scope'}}
$testOverride=$false; $OperatorSid='S-1-5-21-1-2-3-1001'; $OperatorLocalAppDataRoot={quote(tmp_path/'local')}
$WriterFenceAttemptId='regression'
function Assert-LabelStartupUpdateTrust($Root){{}}
function Save-LabelStartupUpdatePreimage($Root,$Purpose){{return [pscustomobject]@{{path={quote(tmp_path/'preimage')}}}}}
function Set-HardenedCodeAcl($Path,[switch]$Recursive){{}}
function Assert-BootstrapIntegrityRecord($Root){{}}
function Write-BootstrapIntegrityRecord($Root,$CodeRoot){{}}
function Get-CodeInventory($Root){{return @()}}
function Get-InventoryAggregate($Items){{return 'same'}}
function Get-ItemProperty{{return [pscustomobject]@{{HiberbootEnabled=1}}}}
function Write-LabelStartupUpdateRecord($Path,$Payload){{
 if([IO.Path]::GetFileName($Path) -eq 'policy.json'){{$script:settingsDigest=$Payload.settings_sha256}}
}}
function Get-LabelStartupUpdateTaskSpec($Root,$Sid){{return [pscustomobject]@{{name='isolated-regression'}}}}
function Invoke-LabelStartupUpdateTaskMutation($Action){{}}
Initialize-LabelStartupUpdate {quote(current)}
if($script:settingsDigest -cne '{hashlib.sha256(raw).hexdigest()}'){{throw 'Policy did not hash actual packet settings'}}
'INITIALIZED'
""")
    assert output.splitlines() == [
        "startup_update_status=REGISTERED_LOGON_SYSTEM_CODE_ONLY",
        "startup_update_task=isolated-regression", "hiberboot_enabled=1",
        "hiberboot_read_status=READ", "INITIALIZED",
    ]


def bootstrap_fixture(monkeypatch, tmp_path):
    old, new = "1" * 40, "2" * 40
    stamp = "2026-10-02T00:00:00+00:00"
    state = updates.apply_transition(updates.prepare_transition(
        previous_sha=old, candidate_sha=new, packet_sha256="3" * 64,
        forward_pin="4" * 64, rollback_pin="5" * 64,
        transition_id="6" * 32, nonce="7" * 32, now=stamp,
    ), writers_absent=True, packet_verified=True, durable_prepared=True, now=stamp)
    policy = {"operator_sid": "operator", "staging_root": str(tmp_path)}
    records = {"transition.json": state, "active.json": {"sha": new}}
    monkeypatch.setattr(updates, "control_root", lambda _: tmp_path)
    monkeypatch.setattr(updates, "load_policy", lambda _: policy)
    monkeypatch.setattr(updates, "ordinary_user_context", lambda *_: True)
    monkeypatch.setattr(updates, "update_lock", lambda *a, **k: nullcontext())
    monkeypatch.setattr(updates, "prepared_pending", lambda *_: False)
    monkeypatch.setattr(updates, "optional_record", lambda p: records.get(p.name))
    monkeypatch.setattr(updates, "read_json", lambda p: records[p.name])
    monkeypatch.setattr(updates, "write_json_durable", lambda p, s: records.__setitem__(p.name, dict(s)))
    monkeypatch.setattr(updates, "request_apply_task", lambda *_: None)
    tick = iter(range(1000))
    monkeypatch.setattr(time, "monotonic", lambda: next(tick))
    return old, new, state, records


@pytest.mark.parametrize("failed_role", ["gui", "relay"])
def test_trial_launch_failure_waits_and_starts_previous_code(monkeypatch, tmp_path, failed_role):
    old, new, original, records = bootstrap_fixture(monkeypatch, tmp_path)
    calls = []
    def launch(control, sha, args, **kwargs):
        role = "relay" if args == ["--label-match-user-relay"] else "gui"
        calls.append((sha, role))
        if sha == new and role == failed_role:
            raise OSError(5, "injected trial launch failure")
        return 42 if role == "gui" else 43
    monkeypatch.setattr(updates, "launch_version", launch)
    def system_recovery(_):
        if records["transition.json"]["phase"] == "APPLIED_PENDING_READY":
            failure = records["trial-failure.json"]
            assert failure["nonce"] == original["nonce"]
            assert failure["transition_id"] == original["transition_id"]
            assert failure["failed_role"] == failed_role
            rollback = updates.rollback_transition(
                updates.request_rollback(original, reason="injected child start failed"),
                writers_absent=True, previous_verified=True, durable_rollback=True,
                nonce="8" * 32, now="2026-10-02T00:00:01+00:00",
            )
            records["transition.json"] = rollback
            records["active.json"] = {"sha": old}
        else:
            records["transition.json"] = {**records["transition.json"], "phase": "ROLLED_BACK"}
    monkeypatch.setattr(time, "sleep", system_recovery)
    assert updates.bootstrap(tmp_path / "app", [], role="gui") == 0
    assert calls[-2:] == [(old, "gui"), (old, "relay")]
    assert calls.count((new, failed_role)) == 1


def test_failed_trial_keeps_unknown_and_never_launches_previous_code(monkeypatch, tmp_path):
    old, new, _, records = bootstrap_fixture(monkeypatch, tmp_path)
    calls = []
    def fail(control, sha, args, **kwargs):
        calls.append(sha)
        raise OSError(5, "injected child start failure")
    monkeypatch.setattr(updates, "launch_version", fail)
    monkeypatch.setattr(time, "sleep", lambda _: records["transition.json"].update(phase="UNKNOWN"))
    assert updates.bootstrap(tmp_path / "app", [], role="gui") == 20
    assert calls == [new]
    assert records["active.json"]["sha"] == new
    assert records["trial-failure.json"]["launch_failure_details"][0]["win32_error"] == 5


@pytest.mark.parametrize("change", ["missing", "reused_pid", "wrong_sid", "wrong_sha", "wrong_transition", "missing_relay"])
def test_existing_trial_claim_requires_live_bound_processes(monkeypatch, tmp_path, change):
    _, new, state, records = bootstrap_fixture(monkeypatch, tmp_path)
    records["trial.json"] = {"pid": 42, "nonce": state["nonce"],
                             "transition_id": state["transition_id"], "claimed_at": state["applied_at"]}
    gui = dict(pid=42, observable=True, sha=new, sid="operator", role="gui",
               ordinary_user=True, created_at=state["applied_at"])
    relay = {**gui, "pid": 43, "role": "relay"}
    if change == "reused_pid": gui["created_at"] = "2026-10-02T00:00:01+00:00"
    if change == "wrong_sid": gui["sid"] = "another-user"
    if change == "wrong_sha": gui["sha"] = "9" * 40
    if change == "wrong_transition": records["trial.json"]["transition_id"] = "9" * 32
    rows = [] if change == "missing" else [gui] if change == "missing_relay" else [gui, relay]
    monkeypatch.setattr(updates, "process_snapshot", lambda _: rows)
    monkeypatch.setattr(updates, "launch_version", lambda *a, **k: pytest.fail("stale claim launched code"))
    monkeypatch.setattr(time, "sleep", lambda _: records["transition.json"].update(phase="UNKNOWN"))
    assert updates.bootstrap(tmp_path / "app", [], role="gui") == 20
    assert records["trial-failure.json"]["sha"] == new


@pytest.mark.parametrize("caller", ["helper", "task", "relay_cycle"])
def test_finite_child_failure_preserves_exit_and_original_streams(monkeypatch, tmp_path, caller):
    import user_relay
    monkeypatch.setattr(updates, "system_executable", lambda _: "fixed-probe")
    monkeypatch.setattr(subprocess, "run", lambda *a, **k: SimpleNamespace(
        returncode=7, stdout=b"original-stdout-sentinel", stderr=b"original-stderr-sentinel",
    ))
    if caller == "relay_cycle":
        result = user_relay._run_command(["fixed-probe"], 10)
        evidence = result.get("child_output", {})
    else:
        with pytest.raises(RuntimeError) as caught:
            if caller == "helper": updates.fixed_helper(tmp_path, "Verify")
            else: updates.request_apply_task(tmp_path, {"operator_sid": "S-1-5-21-1-2-3-1001"})
        evidence = getattr(caught.value, "child_output", {})
    assert evidence == {"returncode": 7, "stdout": "original-stdout-sentinel", "stderr": "original-stderr-sentinel"}


def test_failed_apply_persists_original_exception_chain(monkeypatch, tmp_path):
    _, _, _, records = bootstrap_fixture(monkeypatch, tmp_path)
    records["transition.json"] = {"schema": updates.SCHEMA, "phase": "DEFERRED"}
    monkeypatch.setattr(updates, "fixed_helper", lambda *a, **k: "")
    def fail(_):
        try: raise OSError(5, "DuplicateTokenEx:primary")
        except OSError as original: raise RuntimeError("original-outer-sentinel") from original
    monkeypatch.setattr(updates, "_apply_prepared", fail)
    assert updates.apply_prepared(tmp_path) == 0
    chain = records["transition.json"].get("launch_failure_details", [])
    assert chain[0]["message"] == "original-outer-sentinel"
    assert chain[1]["call"] == "DuplicateTokenEx:primary"
    assert chain[1]["win32_error"] == 5


def test_prepare_failure_reaches_relay_status_with_original_cause(monkeypatch, tmp_path):
    import user_relay
    def fail(_): raise OSError(5, "original-prepare-sentinel")
    monkeypatch.setattr(updates, "prepare_feed_packet", fail)
    monkeypatch.setattr(user_relay, "run_session_direct_sync_once", lambda **k: {"status": "PASS", "returncode": 0})
    monkeypatch.setattr(user_relay, "_read_json", lambda _: {"status": "RUNNING"})
    result = user_relay._runtime_cycle(app_root=tmp_path, direct_sync_root=tmp_path, scan_source_dir=tmp_path)
    chain = result["auto_update"].get("launch_failure_details", [])
    assert chain[0]["win32_error"] == 5
    path = Path(result["auto_update"]["failure_evidence_path"])
    evidence = json.loads(path.read_text(encoding="utf-8"))
    assert "original-prepare-sentinel" in evidence["launch_failure_details"][0]["message"]
    assert "original-prepare-sentinel" not in json.dumps(result)


def test_failure_diagnostics_mask_quoted_authority_and_bound_original_text():
    value = 'original-message-sentinel {"authorization":"Bearer synthetic-value","credential":"synthetic-credential"} token=synthetic-token'
    chain = updates.launch_failure_details(RuntimeError(value + "x" * 20000), include_original=True)
    message = chain[0].get("message", "")
    assert "original-message-sentinel" in message
    assert all(secret not in message for secret in ("synthetic-value", "synthetic-credential", "synthetic-token"))
    assert len(message) <= 8192


def test_diagnostic_storage_failure_preserves_original_relay_outcome(monkeypatch, tmp_path):
    import user_relay
    original = user_relay._write_json_atomic
    def unavailable(path, value):
        if path.name.startswith(("cycle-failure-", "update-preparation-failure-")):
            raise OSError(28, "diagnostic storage unavailable")
        original(path, value)
    monkeypatch.setattr(user_relay, "_write_json_atomic", unavailable)
    def fail(): raise ValueError("original cycle failure")
    result = user_relay.run_persistent_relay_loop(fail, status_path=tmp_path/"status.json", max_cycles=1)
    cycle = result["last_cycle"]
    assert cycle["status"] == "UNKNOWN"
    assert cycle["error_type"] == "ValueError"
    assert cycle["failure_evidence_status"] == "FAILED"
    assert cycle["failure_evidence_error_details"][0]["win32_error"] == 28

GITHUB_HOSTS = ["kmtechn.github.io", "gist.github.com", "gist.githubusercontent.com",
                "github.io", "githubusercontent.com", "sub.github.com", "KMTechn.GitHub.IO."]


@pytest.mark.parametrize("host", GITHUB_HOSTS)
def test_github_feed_policy_matches_python_installer_and_embedded_gate(tmp_path, host):
    settings = updates.policy_from_settings({})
    settings.update(feed_url=f"https://{host}/feed.json", profile_auth=True)
    with pytest.raises(ValueError, match="GitHub"):
        updates.policy_from_settings(settings)
    # Execute the actual embedded pure gate used by release-pair validation.
    source = (ROOT / "INSTALL_CANONICAL_PORTABLE.ps1").read_text(encoding="utf-8-sig")
    pure = source.split("from typing import Any, Mapping", 1)[1].split("startup_release_replacements =", 1)[0]
    namespace = {"Any": object, "Mapping": dict, "re": __import__("re"), "json": json}
    exec(pure, namespace)
    with pytest.raises(ValueError, match="GitHub"):
        namespace["policy_from_settings"](settings)
    settings_path = tmp_path / "settings.json"
    settings_path.write_text(json.dumps(settings), encoding="utf-8")
    output = powershell(tmp_path, f"""
$tokens=$null; $errors=$null
$tree=[Management.Automation.Language.Parser]::ParseFile({quote(ROOT/'INSTALL_THIS_PC.ps1')},[ref]$tokens,[ref]$errors)
$bundle=@($tree.EndBlock.Statements | Where-Object {{ $_ -is [Management.Automation.Language.FunctionDefinitionAst] }})
. ([ScriptBlock]::Create(($bundle.Extent.Text -join [Environment]::NewLine)))
$settings=Get-Content -LiteralPath {quote(settings_path)} -Raw -Encoding UTF8 | ConvertFrom-Json
try {{Assert-LabelStartupUpdateSettings $settings; 'ACCEPTED'}} catch {{if($_.Exception.Message -cne 'STARTUP_UPDATE_SETTINGS_GITHUB_FORBIDDEN'){{throw}}; 'GITHUB_REFUSED'}}
""")
    assert output == "GITHUB_REFUSED"


@pytest.mark.parametrize("host", ["company.test", "github.io.company.test", "evilgithub.io"])
def test_github_suffix_filter_keeps_company_and_distinct_hosts(tmp_path, host):
    settings = updates.policy_from_settings({"feed_url": f"https://{host}/feed.json"})
    assert settings["feed_url"] == f"https://{host}/feed.json"
    path = tmp_path / "settings.json"
    path.write_text(json.dumps(settings), encoding="utf-8")
    output = powershell(tmp_path, f"""
$tokens=$null; $errors=$null
$tree=[Management.Automation.Language.Parser]::ParseFile({quote(ROOT/'INSTALL_THIS_PC.ps1')},[ref]$tokens,[ref]$errors)
$bundle=@($tree.EndBlock.Statements | Where-Object {{ $_ -is [Management.Automation.Language.FunctionDefinitionAst] }})
. ([ScriptBlock]::Create(($bundle.Extent.Text -join [Environment]::NewLine)))
Assert-LabelStartupUpdateSettings (Get-Content -LiteralPath {quote(path)} -Raw -Encoding UTF8 | ConvertFrom-Json)
'COMPANY_ACCEPTED'
""")
    assert output == "COMPANY_ACCEPTED"

@pytest.mark.parametrize("name,phrase", [
    ("INSTALL_THIS_PC.ps1", "지원 제거"),
    ("INSTALL_CANONICAL_PORTABLE.ps1", "지원 제거"),
])
def test_powershell51_decodes_actual_korean_installer_with_bom(tmp_path, name, phrase):
    raw = (ROOT / name).read_bytes()
    if not raw.startswith(b"\xef\xbb\xbf"):
        pytest.fail("Korean installer requires UTF-8 BOM")
    expected = raw.decode("utf-8-sig")
    assert phrase in expected
    output = powershell(tmp_path, f"""
if($PSVersionTable.PSVersion.Major -ne 5){{throw 'Expected Windows PowerShell 5.1'}}
$tokens=$null; $errors=$null
$tree=[Management.Automation.Language.Parser]::ParseFile({quote(ROOT/name)},[ref]$tokens,[ref]$errors)
if($errors.Count){{throw 'Actual Korean installer parse failed'}}
$phrases=@($tree.FindAll({{param($node) $node -is [Management.Automation.Language.StringConstantExpressionAst]}},$true) | ForEach-Object {{ $_.Value }} | Where-Object {{ $_.Contains({quote(phrase)}) }})
if($phrases.Count -eq 0){{throw 'Actual Korean text did not survive PS5.1 decoding'}}
[IO.File]::WriteAllText({quote(tmp_path/'decoded.txt')},$tree.Extent.Text,(New-Object Text.UTF8Encoding($false)))
'KOREAN_INTACT'
""")
    assert output == "KOREAN_INTACT"
    # Compare actual decoded bytes: read_text would normalize CRLF to LF.
    assert hashlib.sha256((tmp_path / "decoded.txt").read_bytes()).hexdigest() == hashlib.sha256(expected.encode("utf-8")).hexdigest()
