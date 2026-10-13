"""Real generation/onboarding checks for record-free support removal packets."""
import json
from pathlib import Path
import shutil
import subprocess

import pytest

import current_user_onboarding as onboarding
import label_data_generation as generation
import protected_admin
import writer_session_fence as fence
from kmtech_shared.data_generation import record_generation
from tests.test_current_user_onboarding import _write_bootstrap_inventory_record


@pytest.fixture
def packets(tmp_path, monkeypatch):
    source = tmp_path / "support"
    installed = tmp_path / "installed"
    files = {
        "app/main.py": b"# synthetic portable entry\n",
        "runtime/pythonw.exe": b"synthetic runtime; never executed",
        "INSTALL_THIS_PC.ps1": b"# synthetic placement helper\n",
        "tools/bootstrap_integrity.ps1": b"# synthetic integrity helper\n",
        "tools/label_writer_fence.ps1": b"# synthetic fence helper\n",
        "app/label_data_generation.py": Path(generation.__file__).read_bytes(),
        "portable-manifest.json": json.dumps({
            "schema": "label-match-portable-tree-v1",
            "version": generation.CURRENT_VERSION,
            "source_commit": "a" * 40,
        }).encode(),
    }
    for relative, raw in files.items():
        path = source / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(raw)
    shutil.copytree(source, installed)
    _write_bootstrap_inventory_record(installed)
    monkeypatch.setattr(generation, "__file__", str(source / "app/label_data_generation.py"))
    monkeypatch.setattr(protected_admin, "current_process_sid", lambda: "S-1-5-21-" + "134")
    generation._RELEASE_IDENTITY_PASS.clear()
    environment = {"LOCALAPPDATA": str(tmp_path / "local"),
                   "LABEL_MATCH_SAVE_DIR": str(tmp_path / "data")}
    data = tmp_path / "data"
    data.mkdir()
    (data / "work.csv").write_bytes(b"preserved business data\n")
    yield source, installed, environment
    generation._RELEASE_IDENTITY_PASS.clear()


def _remove(installed, environment, calls):
    def completed(name):
        calls.append(name)
        return {"status": "ABSENT"}
    return onboarding.remove_current_user_setup(
        installed, environ=environment,
        autostart_remover=lambda: completed("run"),
        relay_stopper=lambda _root: completed("relay"),
        scheduled_task_remover=lambda _root: completed("task"),
    )


@pytest.mark.parametrize("target", ["root", "app", "managed-current"])
def test_same_release_support_removal_and_rollback_preserve_data(packets, monkeypatch, target):
    source, installed, environment = packets
    if target == "managed-current":
        managed = installed.parent / "managed"
        managed.mkdir()
        installed.rename(managed / "current")
        installed = managed / "current"
        _write_bootstrap_inventory_record(installed)
    target_root = installed / "app" if target == "app" else installed
    calls = []
    before = (Path(environment["LABEL_MATCH_SAVE_DIR"]) / "work.csv").read_bytes()
    # Every complete proof and final readback must run outside writer admission.
    import label_exact_clone_resolution as exact
    for module, name in ((onboarding, "verify_bootstrap_integrity"),
                         (exact, "portable_inventory_binding")):
        original = getattr(module, name)
        def unlocked(*args, _original=original, **kwargs):
            assert not getattr(fence._WRITER_LOCAL, "depth", 0)
            return _original(*args, **kwargs)
        monkeypatch.setattr(module, name, unlocked)
    report = _remove(target_root, environment, calls)
    assert report["status"] == "PASS_DATA_PRESERVED"
    assert calls == ["run", "relay", "task"]
    assert (Path(environment["LABEL_MATCH_SAVE_DIR"]) / "work.csv").read_bytes() == before
    assert not (source / "bootstrap-integrity.json").exists()
    assert not list(Path(environment["LABEL_MATCH_SAVE_DIR"]).glob("data-generation*.json"))
    with pytest.raises(generation.DataGenerationError):
        generation.check_user_data_open(source, environ=environment)


@pytest.mark.parametrize("mutation", [
    "build", "version", "runtime", "added", "missing", "installed-tampered",
    "record-absent", "record-forged", "source-record-forged", "signature-forged",
])
def test_unproven_support_packet_refuses_before_removal(packets, mutation):
    source, installed, environment = packets
    if mutation in {"build", "version", "signature-forged"}:
        manifest_path = source / "portable-manifest.json"
        manifest = json.loads(manifest_path.read_bytes())
        if mutation == "build":
            manifest["source_commit"] = "b" * 40
        elif mutation == "version":
            manifest["version"] = "v2.0.98"
        else:
            manifest["data_generation_signed_manifest"] = {}
            manifest["data_generation_signature_hex"] = "a" * 128
        manifest_path.write_text(json.dumps(manifest), encoding="utf-8")
        if mutation == "signature-forged":
            shutil.copy2(manifest_path, installed / manifest_path.name)
            _write_bootstrap_inventory_record(installed)
    elif mutation == "runtime":
        (source / "runtime/pythonw.exe").write_bytes(b"changed runtime")
    elif mutation == "added":
        (source / "app/extra.py").write_bytes(b"# unexpected file")
    elif mutation == "missing":
        (source / "app/main.py").unlink()
    elif mutation == "installed-tampered":
        (installed / "app/main.py").write_bytes(b"# tampered installed code")
    elif mutation == "record-absent":
        (installed / "bootstrap-integrity.json").unlink()
    elif mutation == "source-record-forged":
        (source / "bootstrap-integrity.json").write_text("{}", encoding="utf-8")
    else:
        record_path = installed / "bootstrap-integrity.json"
        record = json.loads(record_path.read_bytes())
        record["aggregate_sha256"] = "a" * 64
        record_path.write_text(json.dumps(record), encoding="utf-8")
    calls = []
    with pytest.raises(onboarding.CurrentUserOnboardingError) as caught:
        _remove(installed, environment, calls)
    assert caught.value.status == "UNKNOWN"
    assert caught.value.cause_code == "DATA_GENERATION_REFUSED"
    assert not calls
    assert not (Path(environment["LOCALAPPDATA"]) / "KMTech/Bootstrap/Label_Match").exists()


@pytest.mark.parametrize("change", ["source", "installed", "record", "source-record-added"])
def test_final_proof_change_refuses_before_removal(packets, monkeypatch, change):
    source, installed, environment = packets
    original = generation.probe_downgrade
    mutated = False
    def probe(*args, **kwargs):
        nonlocal mutated
        result = original(*args, **kwargs)
        if not mutated:
            mutated = True
            if change == "source-record-added":
                (source / "bootstrap-integrity.json").write_text("{}", encoding="utf-8")
            elif change == "record":
                path = installed / "bootstrap-integrity.json"
                path.write_bytes(path.read_bytes() + b"\n")
            else:
                root = source if change == "source" else installed
                (root / "app/main.py").write_bytes(b"# changed during generation probe")
        return result
    monkeypatch.setattr(generation, "probe_downgrade", probe)
    calls = []
    with pytest.raises(onboarding.CurrentUserOnboardingError) as caught:
        _remove(installed, environment, calls)
    assert caught.value.status == "UNKNOWN"
    assert not calls


@pytest.mark.parametrize("marker", ["newer", "same-version-other-build", "corrupt"])
def test_support_proof_preserves_generation_refusal(packets, marker):
    _source, installed, environment = packets
    path = Path(environment["LABEL_MATCH_SAVE_DIR"]) / "data-generation.json"
    if marker == "corrupt":
        path.write_bytes(b"invalid generation record")
    else:
        record_generation(path, "v9.0.0" if marker == "newer" else generation.CURRENT_VERSION,
                          app_id=generation.APP_ID, subject_sid="S-1-5-21-" + "134",
                          build_id="b" * 40)
    before = path.read_bytes()
    calls = []
    with pytest.raises(onboarding.CurrentUserOnboardingError):
        _remove(installed, environment, calls)
    assert not calls
    assert path.read_bytes() == before


def test_installed_packet_removal_keeps_normal_checks(packets, monkeypatch):
    _source, installed, environment = packets
    monkeypatch.setattr(generation, "__file__", str(installed / "app/label_data_generation.py"))
    checks = []
    actual = onboarding.check_user_data_open
    def check(*args, **kwargs):
        checks.append(args[0])
        return actual(*args, **kwargs)
    monkeypatch.setattr(onboarding, "check_user_data_open", check)
    assert _remove(installed, environment, [])["status"] == "PASS_DATA_PRESERVED"
    assert checks == [installed, installed]


def test_same_generation_support_removal_passes_without_changing_marker(packets):
    _source, installed, environment = packets
    marker = Path(environment["LABEL_MATCH_SAVE_DIR"]) / "data-generation.json"
    record_generation(marker, generation.CURRENT_VERSION, app_id=generation.APP_ID,
                      subject_sid="S-1-5-21-" + "134", build_id="a" * 40)
    before = marker.read_bytes()
    assert _remove(installed, environment, [])["status"] == "PASS_DATA_PRESERVED"
    assert marker.read_bytes() == before


def test_valid_other_release_code_and_manifest_still_refuse(packets):
    source, installed, environment = packets
    code = source / "app/label_data_generation.py"
    code.write_bytes(code.read_bytes().replace(generation.CURRENT_VERSION.encode(), b"v2.0.98"))
    manifest_path = source / "portable-manifest.json"
    manifest = json.loads(manifest_path.read_bytes())
    manifest.update(version="v2.0.98", source_commit="b" * 40)
    manifest_path.write_text(json.dumps(manifest), encoding="utf-8")
    calls = []
    with pytest.raises(onboarding.CurrentUserOnboardingError):
        _remove(installed, environment, calls)
    assert not calls


def test_generation_advanced_during_final_inventory_still_refuses(packets, monkeypatch):
    source, installed, environment = packets
    import label_exact_clone_resolution as exact
    actual = exact.portable_inventory_binding
    source_reads = 0
    def inventory(root, **kwargs):
        nonlocal source_reads
        result = actual(root, **kwargs)
        if Path(root) == source:
            source_reads += 1
            if source_reads == 3:
                assert not getattr(fence._WRITER_LOCAL, "depth", 0)
                marker = Path(environment["LABEL_MATCH_SAVE_DIR"]) / "data-generation.json"
                record_generation(marker, "v9.0.0", app_id=generation.APP_ID,
                                  subject_sid="S-1-5-21-" + "134", build_id="b" * 40)
        return result
    monkeypatch.setattr(exact, "portable_inventory_binding", inventory)
    calls = []
    with pytest.raises(onboarding.CurrentUserOnboardingError):
        _remove(installed, environment, calls)
    assert source_reads == 3
    assert not calls


def test_fence_resume_error_retains_original_type(packets, monkeypatch):
    _source, installed, environment = packets
    actual = fence._admitted_writer_sources
    observations = 0
    def admitted(*args, **kwargs):
        nonlocal observations
        observations += 1
        result = actual(*args, **kwargs)
        return frozenset({"unrelated-source"}) if observations > 1 else result
    monkeypatch.setattr(fence, "_admitted_writer_sources", admitted)
    calls = []
    with pytest.raises(fence.WriterFencedError) as caught:
        _remove(installed, environment, calls)
    assert caught.value.code == "ACTIVE_WRITER_FENCE"
    assert not calls


@pytest.mark.parametrize("shell", [name for name in ("powershell.exe", "pwsh.exe") if shutil.which(name)])
@pytest.mark.parametrize("installed_verified", [False, True])
def test_canonical_removal_passes_installed_logical_root(tmp_path, shell, installed_verified):
    # Execute only the existing removal branch with host effects stubbed. No
    # installer/product entrypoint, registry, task or native credential runs.
    repo = Path(__file__).resolve().parents[1]
    text = (repo / "INSTALL_CANONICAL_PORTABLE.ps1").read_text(encoding="utf-8-sig")
    anchor = text.index("    $removalRoot = if ($existingVerified)")
    start = text.rfind("    if (-not $pristineInstall", 0, anchor)
    end = text.index("    $unquiesced = @(UnquiescedProductWriters)", anchor)
    block = text[start:end]
    report = tmp_path / "removal.json"
    report.write_text(json.dumps({"status": "PASS_DATA_PRESERVED",
                                  "relay_process": {"status": "ABSENT"}}), encoding="utf-8")
    script = tmp_path / "removal-branch.ps1"
    script.write_text("""
$ErrorActionPreference='Stop'
$pristineInstall=$false; $legacyFirstRegistration=$false; $healthyWithoutCode=$false
$healthyLifecycle=$null; $healthyRemovedInstall=$false
$old=@(); $source='support'; $install='installed'; $lifecycleInstalled=$install
$existingVerified=VERIFIED
$writerFenceControlRoot=''; $writerSessionId=''; $writerAttemptId=''; $writerTransactionId=''
$removalPath='REPORT'
function Product([string]$Root,[string]$Mode,[string[]]$ExtraArguments=@(),[string]$LogicalAppRoot='') {
    if (-not $LogicalAppRoot) {$LogicalAppRoot=$Root}
    $script:called=@{execution=$Root;logical=$LogicalAppRoot;mode=$Mode}
}
function Snapshot {return @{exists=$false}}
function Get-LabelCurrentUserScheduledTasks {return @()}
function Get-LabelOfficialLegacyTaskSnapshot {return $null}
function Wait-LabelRelayPreimageExit {return}
function Set-LabelWriterFenceInstalledTree {return $null}
BLOCK
$script:called | ConvertTo-Json -Compress
""".replace("VERIFIED", "$true" if installed_verified else "$false")
        .replace("REPORT", str(report).replace("'", "''")).replace("BLOCK", block), encoding="utf-8-sig")
    completed = subprocess.run([shell, "-NoLogo", "-NoProfile", "-NonInteractive",
        "-ExecutionPolicy", "Bypass", "-File", str(script)], capture_output=True,
        creationflags=subprocess.CREATE_NO_WINDOW)
    assert completed.returncode == 0, "definition-only removal branch failed"
    result = json.loads(completed.stdout.decode("utf-8-sig"))
    assert result == {"execution": "installed" if installed_verified else "support",
                      "logical": "installed", "mode": "--remove-current-user-setup"}
