"""Wave 3 review fixtures; expected roots are compared with e45ec3e in the review probe."""

import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys
from unittest.mock import patch

import pytest

import current_user_onboarding as onboarding
from label_match_single_instance import resolve_data_scope
from user_relay import _resolve_scan_source_dir


def test_legacy_registration_matches_baseline_through_environment_and_restart(tmp_path):
    # Run the reviewer reproduction unchanged, with its work directory supplied
    # by pytest. Only registration/OS/transport dependencies are fixture doubles.
    script = r'''
import json, runpy, sys, types
from pathlib import Path
review_run = types.ModuleType("review_run")
review_run.ROOT = Path(sys.argv[1])
review_run.REPO = Path(sys.argv[2])
sys.modules["review_run"] = review_run
runpy.run_path(str(review_run.REPO / "tests/_storage_root_registration_probe.py"))
results = json.loads((review_run.ROOT / "registration-comparison.json").read_text(encoding="utf-8"))
for row in results.values():
    assert row["report_status"] == "READY"
    for key in ("registration_argument", "report_data_root", "ledger", "fresh_restart_ledger"):
        assert row[key] == "A", (key, row)

# Exercise the actual GUI method after applying the READY report environment,
# and in a fresh process environment with only registration state on disk.
import os
from unittest.mock import patch
import Label_Match
from current_user_onboarding import apply_current_user_runtime_environment, resolve_current_user_onboarding_paths
root = review_run.ROOT / "registration-fixtures/head"
environment = {"LOCALAPPDATA": str(root / "local"), "ProgramData": str(root / "program")}
paths = resolve_current_user_onboarding_paths(root / "app", environ=environment)
for runtime in (dict(environment), environment):
    if runtime is environment:
        apply_current_user_runtime_environment(paths, environ=runtime)
    with patch.dict(os.environ, runtime, clear=True):
        app = object.__new__(Label_Match.Label_Match)
        app.app_settings = {}
        assert Path(app._resolve_configured_save_path()) == paths.data_root
'''
    repo = Path(__file__).resolve().parents[1]
    result = subprocess.run(
        [sys.executable, "-B", "-c", script, str(tmp_path), str(repo)], cwd=tmp_path,
        env=dict(os.environ, PYTHONPATH=str(repo)), capture_output=True, text=True, timeout=60,
    )
    assert result.returncode == 0, result.stdout + result.stderr


@pytest.mark.parametrize("case", [
    "standalone", "onboarded", "custom-env-a", "custom-env-b", "env",
    "split-defaults", "onboarded-with-stale-programdata",
])
def test_review_path_fixtures_preserve_roots_and_bytes(tmp_path, case):
    import Label_Match

    def write(path, content):
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(content if isinstance(content, bytes) else json.dumps(content).encode("utf-8"))

    # Keep the seven probe_paths.py fixture inputs, including synthetic pending
    # ledger bytes and both stores' CSVs, unchanged from the rejected review.
    root = tmp_path / "path-fixtures" / case
    local = root / "local"
    program = root / "program"
    custom = root / "custom-c"
    la = local / "KMTech/Label_Match/data"
    pd = program / "KMTech/Label_Match/data"
    sync = local / "KMTech/DirectSync/label_match"
    settings = local / "KMTech/Label_Match/config/app_settings.json"
    env = {"LOCALAPPDATA": str(local), "ProgramData": str(program)}
    payload = {}
    if case in ["custom-env-a", "custom-env-b"]:
        payload = {"custom_save_path": str(custom)}
        env["LABEL_MATCH_SAVE_DIR"] = str(root / ("env-a" if case.endswith("-a") else "env-b"))
    if case == "env":
        env["LABEL_MATCH_SAVE_DIR"] = str(root / "env-a")
    if case in ["standalone", "split-defaults", "onboarded-with-stale-programdata"]:
        write(pd / "old.csv", b"existing ProgramData business CSV")
    if case in ["onboarded", "split-defaults", "onboarded-with-stale-programdata"]:
        write(sync / "producer_identity.json", {"producer_id": "synthetic-existing"})
        write(la / "package_logistics_outbox.sqlite3", b"existing current-user pending ledger")
        write(sync / "status/current_user_onboarding.json", {
            "data_root": str(la), "ledger_path": str(la / "package_logistics_outbox.sqlite3"),
        })
    if case in ["onboarded", "onboarded-with-stale-programdata"]:
        write(la / "current.csv", b"active current-user completion CSV")
    write(settings, payload)
    before = {
        str(p.relative_to(root)): hashlib.sha256(p.read_bytes()).hexdigest()
        for p in root.rglob("*") if p.is_file()
    }

    runtime = dict(env)
    if case == "standalone":
        # Before entering onboarding, a direct standalone guard still uses P.
        assert Path(resolve_data_scope(environment=runtime, settings_path=settings)) == pd
    paths = onboarding.resolve_current_user_onboarding_paths(root / "app", environ=runtime)
    onboarding.apply_current_user_runtime_environment(paths, environ=runtime)
    with patch.dict(os.environ, runtime, clear=True):
        app = object.__new__(Label_Match.Label_Match)
        app.app_settings = payload
        actual = {
            "onboarding": paths.data_root,
            "ledger": paths.ledger_path,
            "gui": Path(app._resolve_configured_save_path()),
            "guard": Path(resolve_data_scope(environment=runtime, settings_path=settings)),
            "relay": _resolve_scan_source_dir("", data_root=paths.data_root, settings_path=settings),
        }
    # e45ec3e: onboarded/default-split roots and ledger are A; env-only is E;
    # GUI/guard/relay custom is C. W3 intentionally aligns new custom ledgers
    # E -> C. Entering onboarding selects A even with legacy ProgramData files.
    expected = (
        custom if case.startswith("custom-") else
        root / "env-a" if case == "env" else
        la
    )
    assert actual == {
        "onboarding": expected,
        "ledger": expected / "package_logistics_outbox.sqlite3",
        "gui": expected, "guard": expected, "relay": expected,
    }
    after = {
        str(p.relative_to(root)): hashlib.sha256(p.read_bytes()).hexdigest()
        for p in root.rglob("*") if p.is_file()
    }
    assert after == before


def test_storage_diagnostics_emit_at_unchanged_startup_logging_level(tmp_path):
    # Like probe_diagnostic.py, import the real app in a fresh interpreter and
    # attach a handler without enabling INFO or changing any logger's level.
    script = r'''
import io, json, logging, sys
from pathlib import Path
import Label_Match
from current_user_onboarding import resolve_current_user_onboarding_paths
from label_match_single_instance import resolve_data_scope

root = Path(sys.argv[1])
env = {"LOCALAPPDATA": str(root / "local"), "ProgramData": str(root / "program")}
ledger = root / "local/KMTech/Label_Match/data/package_logistics_outbox.sqlite3"
ledger.parent.mkdir(parents=True)
ledger.write_bytes(b"existing current-user pending ledger")
settings = root / "local/KMTech/Label_Match/config/app_settings.json"
settings.parent.mkdir(parents=True)
custom = root / "custom-c"
settings.write_text(json.dumps({"custom_save_path": str(custom)}), encoding="utf-8")
buffer = io.StringIO()
handler = logging.StreamHandler(buffer)
logger = logging.getLogger("label_match_single_instance")
split_logger = logging.getLogger("current_user_onboarding")
levels = [logging.getLogger().level, logger.level, split_logger.level]
logging.getLogger().addHandler(handler)
try:
    selected = resolve_data_scope(environment={"LABEL_MATCH_SAVE_DIR": str(root / "env-a")})
    paths = resolve_current_user_onboarding_paths(root / "app", environ=env)
finally:
    logging.getLogger().removeHandler(handler)
assert logger.getEffectiveLevel() == logging.WARNING
assert split_logger.getEffectiveLevel() == logging.WARNING
assert not logger.isEnabledFor(logging.INFO)
assert levels == [logging.getLogger().level, logger.level, split_logger.level]
assert paths.data_root == custom and paths.ledger_path == ledger
assert ledger.read_bytes() == b"existing current-user pending ledger"
lines = buffer.getvalue().splitlines()
assert lines == [
    f"storage_root_selected: rule=environment.LABEL_MATCH_SAVE_DIR data_root={selected}",
    f"storage_root_selected: rule=settings.custom_save_path data_root={custom}",
    f"storage_root_split_preserved: data_root={custom} onboarding_ledger={ledger}",
]
print("PASS: default WARNING startup channel records selection and preserved split")
'''
    environment = dict(os.environ, PYTHONPATH=str(Path(__file__).resolve().parents[1]))
    result = subprocess.run(
        [sys.executable, "-B", "-c", script, str(tmp_path)], cwd=tmp_path,
        env=environment, capture_output=True, text=True, timeout=30,
    )
    assert result.returncode == 0, result.stdout + result.stderr


@pytest.mark.parametrize("state_file", [
    "producer_identity.json", "producer_manifest.json",
    "status/current_user_onboarding.json", "status/label_match_worker_pc_registration.json",
])
def test_current_user_state_does_not_inspect_stale_programdata(monkeypatch, tmp_path, state_file):
    local = tmp_path / "local"
    state = local / "KMTech/DirectSync/label_match" / state_file
    state.parent.mkdir(parents=True)
    state.write_text("{}", encoding="utf-8")

    def unexpected_scan(_path):
        pytest.fail("onboarded installations must not inspect stale ProgramData")

    monkeypatch.setattr(os, "scandir", unexpected_scan)
    assert resolve_data_scope(environment={
        "LOCALAPPDATA": str(local), "ProgramData": str(tmp_path / "unreadable-program"),
    }) == str(local / "KMTech/Label_Match/data")
