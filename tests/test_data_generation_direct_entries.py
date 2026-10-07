"""Every supported data entry refuses before identity reads or lifecycle writes."""
import json
import os
from pathlib import Path
import subprocess
import sys

import pytest


ENTRIES = {
    "registrar": "tools.register_label_match_worker_pc:main",
    "registrar-dry": "tools.register_label_match_worker_pc:main",
    "registrar-admin": "tools.register_label_match_worker_pc:main",
    "registrar-partial": "tools.register_label_match_worker_pc:main",
    "onboarding": "current_user_onboarding:onboarding_main",
    "removal": "current_user_onboarding:removal_main",
    "relay": "user_relay:main",
    "raw": "tools.direct_sync_relay_runner:main",
    "hosted-relay": "label_match_product_host:dispatch_product_mode",
    "hosted-raw": "label_match_product_host:dispatch_product_mode",
    "persistent": "user_relay:main",
    "main-app": "Label_Match:main",
    "operator": "tools.direct_sync_relay_operator:main",
    "operator-pause": "tools.direct_sync_relay_operator:main",
    "operator-retry": "tools.direct_sync_relay_operator:main",
    "canary": "tools.label_auth_recovery_canary:main",
    "check-profile": "tools.check_logistics_runtime_profile:main",
    "install-profile": "tools.install_logistics_runtime_profile:main",
    "install-admin": "tools.install_protected_admin:main",
    "install-pack": "tools.direct_sync_relay_install_pack:main",
    "exact": "tools.label_exact_clone_resolution_receipt:main",
    "exact-receipt": "tools.label_exact_clone_resolution_receipt:main",
    "reconcile": "tools.label_guarded_runtime_reconcile:main",
}


@pytest.mark.parametrize("entry", ENTRIES)
@pytest.mark.parametrize("state", ["old", "other-build", "corrupt"])
def test_direct_process_refuses_without_identity_or_lifecycle_changes(entry, state, tmp_path):
    repo = Path(__file__).resolve().parents[1]
    output = tmp_path / "result.json"
    environment = os.environ.copy()
    environment["KMTECH_DATA_GENERATION_ENTRY_KIND"] = entry
    completed = subprocess.run([
        sys.executable, "-B", str(repo / "tests/_data_generation_direct_child.py"),
        str(repo), str(tmp_path / "isolated-user"), ENTRIES[entry], state, str(output),
    ], env=environment, capture_output=True, text=True, encoding="utf-8", errors="replace",
       creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0))
    assert completed.returncode == 0, completed.stderr[-1600:]
    result = json.loads(output.read_text(encoding="utf-8"))
    assert result["passed"], result


@pytest.mark.parametrize("entry", ENTRIES)
@pytest.mark.parametrize("state", ["external-tilde", "external-relative", "external-case", "external-link"])
def test_external_input_marker_without_control_marker_refuses_before_any_read(entry, state, tmp_path):
    test_direct_process_refuses_without_identity_or_lifecycle_changes(entry, state, tmp_path)
    assert json.loads((tmp_path / "result.json").read_text(encoding="utf-8"))["control_marker_absent_before"]


@pytest.mark.parametrize("state", ["external-blank-profile", "external-default-profile"])
def test_empty_or_default_profile_selects_actual_external_marker_before_loading(state, tmp_path):
    test_direct_process_refuses_without_identity_or_lifecycle_changes("check-profile", state, tmp_path)
    assert json.loads((tmp_path / "result.json").read_text(encoding="utf-8"))["control_marker_absent_before"]


@pytest.mark.parametrize("entry", ["hosted-raw", "hosted-relay"])
@pytest.mark.parametrize("state", ["external-duplicate", "external-abbreviated"])
def test_host_uses_the_child_parser_path_before_bootstrap_diagnostics(entry, state, tmp_path):
    test_direct_process_refuses_without_identity_or_lifecycle_changes(entry, state, tmp_path)
    assert json.loads((tmp_path / "result.json").read_text(encoding="utf-8"))["control_marker_absent_before"]


@pytest.mark.parametrize("argument", ["", "~/external/logistics_runtime_profile.json"])
def test_allowed_profile_reader_uses_the_same_selected_path_as_generation_guard(argument, monkeypatch, tmp_path):
    import label_data_generation as generation
    from tools import check_logistics_runtime_profile as check
    monkeypatch.setenv("USERPROFILE", str(tmp_path))
    actual = tmp_path / "external/logistics_runtime_profile.json"
    monkeypatch.setenv("LABEL_MATCH_LOGISTICS_PROFILE_PATH", str(actual))
    admitted, loaded = [], []
    monkeypatch.setattr(generation, "check_user_data_open", lambda _root, roots=(): admitted.extend(roots))
    monkeypatch.setattr(generation, "record_user_data_open", lambda _root, roots: admitted.extend(roots))
    def loader(*, required, profile_path):
        loaded.append(Path(profile_path))
        return None
    monkeypatch.setattr(check, "load_logistics_runtime_profile", loader)
    assert check.main(["--optional", "--profile-path", argument]) == 0
    assert loaded == [actual.resolve()]
    assert admitted == [actual.parent.resolve(), actual.parent.resolve()]


def test_allow_preflight_is_read_only_and_normal_relay_still_persists(monkeypatch, tmp_path):
    import label_data_generation as generation
    import protected_admin
    import user_relay
    monkeypatch.setattr(protected_admin, "current_process_sid", lambda: "S-1-5-21-900-901-902-1001")
    monkeypatch.setattr(generation, "_installed_identity", lambda _root: {
        "version": generation.CURRENT_VERSION, "build_id": "a" * 40,
        "data_compatible_min_version": generation.CURRENT_VERSION, "compatibility_verified": False,
    })
    data = tmp_path / "data"
    generation.check_user_data_open(tmp_path, (data,))
    assert not data.exists()
    status = tmp_path / "relay-status.json"
    report = user_relay.run_persistent_relay_loop(lambda: {"status": "PASS"},
        status_path=status, max_cycles=1)
    assert report["status"] == "COMPLETED"
    assert json.loads(status.read_text())["last_cycle"]["status"] == "PASS"


def test_registration_preflights_generation_without_committing_before_identity_read(monkeypatch, tmp_path):
    import label_data_generation as generation
    import protected_admin
    from tools import register_label_match_worker_pc as registrar
    monkeypatch.setattr(protected_admin, "current_process_sid", lambda: "S-1-5-21-900-901-902-1001")
    monkeypatch.setattr(generation, "_installed_identity", lambda _root: {
        "version": generation.CURRENT_VERSION, "build_id": "a" * 40,
        "data_compatible_min_version": generation.CURRENT_VERSION, "compatibility_verified": False,
    })
    reached = []
    data = tmp_path / "data"
    relay = tmp_path / "relay"
    def payloads(_args):
        assert not (data / "data-generation.json").exists()
        assert not (relay / "data-generation.json").exists()
        reached.append("identity-read")
        raise RuntimeError("isolated end of the allowed identity boundary")
    monkeypatch.setattr(registrar, "build_payloads", payloads)
    assert registrar.main(["--dry-run", "--sync-dir", str(data), "--data-dir", str(relay),
                           "--enrollment-token-env", ""]) == 2
    assert reached == ["identity-read"]
    assert not (data / "data-generation.json").exists()
    assert not (relay / "data-generation.json").exists()


def test_installed_tool_uses_packet_identity_without_resolving_user_settings(monkeypatch, tmp_path):
    import current_user_onboarding
    import label_data_generation as generation
    root = tmp_path / "packet"
    (root / "app").mkdir(parents=True)
    (root / "runtime").mkdir()
    (root / "app/main.py").write_text("# synthetic code")
    (root / "runtime/python.exe").write_text("synthetic")
    (root / "runtime/pythonw.exe").write_text("synthetic")
    (root / "portable-manifest.json").write_text(json.dumps({
        "schema": "label-match-portable-tree-v1", "version": generation.CURRENT_VERSION,
        "source_commit": "a" * 40,
    }))
    observed = []
    def verify(paths, *, required):
        assert required is True
        assert paths.app_root == root
        assert paths.bootstrap_integrity_path == root / "bootstrap-integrity.json"
        observed.append(paths.app_root)
        return {"status": "PASS"}
    def forbidden(*_args, **_kwargs):
        pytest.fail("release identity read user settings before G")
    monkeypatch.setattr(current_user_onboarding, "verify_bootstrap_integrity", verify)
    monkeypatch.setattr(current_user_onboarding, "resolve_current_user_onboarding_paths", forbidden)
    assert generation._installed_identity(root / "app")["build_id"] == "a" * 40
    assert observed == [root]


@pytest.mark.parametrize("operation", ["check_user_data_open", "record_user_data_open"])
def test_unavailable_process_sid_is_generation_unknown_before_release_or_data_read(
        operation, monkeypatch, tmp_path):
    import label_data_generation as generation
    import protected_admin
    def unavailable():
        raise RuntimeError("synthetic process SID lookup failure")
    def forbidden(_root):
        pytest.fail("release/data lookup followed an unavailable SID")
    monkeypatch.setattr(protected_admin, "current_process_sid", unavailable)
    monkeypatch.setattr(generation, "_installed_identity", forbidden)
    data = tmp_path / "unopened-data"
    with pytest.raises(generation.DataGenerationError, match="자료.*복구"):
        getattr(generation, operation)(tmp_path, (data,))
    assert not data.exists()
