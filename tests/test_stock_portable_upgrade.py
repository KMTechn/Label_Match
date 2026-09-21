"""Historical stock packages, with OS/credential adapters outside their bytes."""
from __future__ import annotations

from datetime import datetime, timedelta
import hashlib
import json
from pathlib import Path
import shutil
import subprocess
import sys

import pytest

from tools import build_portable_release_candidate as builder
from tests.test_healthy_lifecycle import _adapters, _fixture, _hashes, _stop_fixture
from tests.test_writer_transition import ROOT, _definitions, _environment, _ps, _quote
from user_relay_stop_marker import canonical_marker_bytes, read_stop_marker


@pytest.fixture(scope="module")
def stock_packages(tmp_path_factory):
    root = tmp_path_factory.mktemp("stock")
    packages = []
    for revision in ("d0e504e", "66c623a"):
        source = root / (revision + "-source")
        subprocess.run(["git", "clone", "--quiet", "--no-hardlinks", "--no-checkout",
                        "-c", "core.autocrlf=false", str(ROOT), str(source)],
                       check=True, capture_output=True)
        subprocess.run(["git", "-C", str(source), "reset", "--hard", revision],
                       check=True, capture_output=True)
        if revision == "66c623a":
            # Reproduce the clean Windows checkout that failed on capture8.
            subprocess.run(["git", "-C", str(source), "config", "core.autocrlf", "true"], check=True)
            for relative in ("assets/Item.csv", "config/app_settings.json"):
                path = source / relative
                path.write_bytes(path.read_bytes().replace(b"\n", b"\r\n"))
            subprocess.run(["git", "-C", str(source), "add", "--renormalize",
                            "assets/Item.csv", "config/app_settings.json"], check=True)
        package = root / (revision + "-portable")
        if revision == "d0e504e":
            # Run the historical stock builder, not today's selective fixture.
            subprocess.run([sys.executable, "-I", "-B",
                            str(source / "tools/build_portable_release_candidate.py"),
                            "--output", str(package), "--update-manifest-public-key-config", "11" * 32],
                           check=True, capture_output=True, timeout=180)
        else:
            builder.build(source, Path(sys.base_prefix), package,
                          update_manifest_public_key_config="11" * 32)
        packages.append((source, package))
    return packages


def test_stock_package_preserves_every_committed_source_byte(stock_packages):
    source, package = stock_packages[1]
    checked = 0
    for path in package.rglob("*"):
        if not path.is_file():
            continue
        relative = path.relative_to(package).as_posix()
        if relative.startswith(("runtime/", "app/site-packages/")) or relative in {
            "portable-manifest.json", "app/update-manifest-key-config.json",
        }:
            continue
        original = {
            "app/main.py": "portable/main.py",
            "launch-label-match.cmd": "portable/launch-label-match.cmd",
        }.get(relative, relative.removeprefix("app/"))
        expected = subprocess.check_output(["git", "-C", str(source), "show", "HEAD:" + original])
        assert path.read_bytes() == expected, relative
        checked += 1
    assert checked > 100


@pytest.mark.parametrize("relative", ["app/assets/Item.csv", "app/config/app_settings.json"])
def test_stock_transition_rejects_changed_data(tmp_path, stock_packages, relative):
    old, candidate = (entry[1] for entry in stock_packages)
    altered = tmp_path / "altered"
    shutil.copytree(candidate, altered)
    with (altered / relative).open("ab") as stream:
        stream.write(b"unreviewed data change\n")
    result = _ps(tmp_path, _definitions() + f"\nAssert-WriterTransition {_quote(altered)} {_quote(old)}",
                 _environment(tmp_path))
    assert result.returncode != 0
    assert "WRITER_TRANSITION_SEMANTICS_DIFFER: " + relative in result.stderr


def test_stock_d0_upgrade_preserves_data_and_two_restart_integrity_checks(tmp_path_factory, stock_packages):
    _stock_d0_upgrade(tmp_path_factory, stock_packages)


def test_stock_d0_abnormal_stop_marker_removal_then_upgrade(tmp_path_factory, stock_packages):
    _stock_d0_upgrade(tmp_path_factory, stock_packages, abnormal_marker=True)


def _stock_d0_upgrade(tmp_path_factory, stock_packages, *, abnormal_marker=False):
    tmp_path = tmp_path_factory.mktemp("e2e")
    old, candidate = (entry[1] for entry in stock_packages)
    empty = tmp_path / "empty"
    empty.mkdir()
    install, candidate, env, paths = _fixture(tmp_path, (empty, candidate))
    install.rmdir()  # _fixture seeds user state; the stock helper installs code.
    result = _ps(tmp_path, f"& {_quote(old / 'INSTALL_THIS_PC.ps1')} -SourceRoot {_quote(old)} "
                 f"-InstallRoot {_quote(install)} -AllowNoncanonicalLayoutForTest\nexit $LASTEXITCODE", env, timeout=180)
    assert result.returncode == 0, result.stderr[-2400:]
    old_record = (install / "bootstrap-integrity.json").read_bytes()
    old_catalog = (install / "app/assets/Item.csv").read_bytes()
    # A local catalog and its authority evidence are user data, unlike the bundle.
    cache = Path(env["LOCALAPPDATA"]) / "KMTech/ItemCatalog/Label_Match"
    cache.mkdir(parents=True)
    (cache / "Item.csv").write_bytes(b"LOCAL,CATALOG,USER,VALUE\r\n")
    (cache / "Item.csv.authority.json").write_bytes(b'{"fixture":"preserve"}\n')
    cache_before = {p.name: p.read_bytes() for p in cache.iterdir()}
    preserved = {}
    for relative in ("spool/pending.csv", "upload_status/pending.json",
                     "logs/existing.log", "evidence/existing-receipt.json"):
        path = paths.direct_sync_root / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(b'{"fixture":"preserve"}\n')
        preserved[path] = path.read_bytes()
    before = _hashes(paths)
    adapter = Path(__file__).with_name("_stock_upgrade_native.py")
    env["LM_STOCK_NATIVE_ADAPTER"] = str(adapter)
    source = (candidate / "INSTALL_CANONICAL_PORTABLE.ps1").read_text(encoding="utf-8")
    anchor = "if (-not $SourceRoot) { $SourceRoot = $PSScriptRoot }"
    source = source.replace(anchor, _adapters(install, candidate, env, tmp_path) + "\n" + anchor, 1)
    source = source.replace("$writerFenceControlRoot = Join-Path $defaultDirectSyncRoot 'control\\writer-session'",
                            "$writerFenceControlRoot = $env:KMTECH_LABEL_WRITER_CONTROL_ROOT")
    # Run real product modes through an external OS adapter, leaving both
    # attested package trees and all transition/integrity code untouched.
    source = source.replace("(Arg (Join-Path $Root 'app\\main.py')),", f"(Arg {_quote(adapter)}),", 1)
    source = source.replace("import current_user_onboarding as onboarding\n",
                            "import current_user_onboarding as onboarding\n"
                            "import os, runpy\nrunpy.run_path(os.environ['LM_STOCK_NATIVE_ADAPTER'], "
                            "init_globals={'lifecycle_only': True, 'app_root': root})\n", 1)
    script = tmp_path / "canonical-stock.ps1"
    script.write_text(source, encoding="utf-8-sig")
    try:
        if abnormal_marker:
            # The adapter replaces only OS/credential boundaries and calls the
            # installed stock app/main.py dispatcher with the public mode.
            arguments = subprocess.list2cmdline([
                "-I", "-B", str(adapter), "--remove-current-user-setup", "--app-root", str(install),
            ])
            removal_command = (
                "$ErrorActionPreference='Stop'\n"
                f"$p = Start-Process -FilePath {_quote(install / 'runtime/python.exe')} "
                f"-ArgumentList {_quote(arguments)} -WindowStyle Hidden -Wait -PassThru "
                f"-RedirectStandardOutput {_quote(tmp_path / 'removal.stdout.log')} "
                f"-RedirectStandardError {_quote(tmp_path / 'removal.stderr.log')}\n"
                "exit $p.ExitCode"
            )
            initial = _ps(tmp_path, removal_command, env)
            assert initial.returncode == 0, initial.stderr
            previous_report = paths.removal_report_path.read_bytes()
            previous = json.loads(previous_report)
            marker_path = paths.control_dir / "label_match_user_relay.stop.json"
            # Reproduce a valid later stop request, unrelated to the last
            # successful removal. Synthetic input only; never repair a marker.
            later = datetime.fromisoformat(previous["completed_at"]) + timedelta(seconds=1)
            stopped = {"schema_version": "label-match-user-relay-stop-v1",
                       "request_id": "b" * 32, "requested_at": later.isoformat()}
            marker_path.write_bytes(canonical_marker_bytes(stopped))
            stopped_raw = marker_path.read_bytes()
            stopped_hash = hashlib.sha256(stopped_raw).hexdigest()
            assert previous["relay_process"]["request_id"] != stopped["request_id"]
            assert previous["relay_process"]["stop_request_sha256"] != stopped_hash
            paths.onboarding_report_path.write_text(json.dumps({
                "status": "FAILED", "failure": "canonical bootstrap integrity readback differs",
            }), encoding="utf-8")
            denied = _ps(tmp_path, f"& {_quote(script)} -SourceRoot {_quote(candidate)} "
                         f"-InstallRoot {_quote(install)} -EvidencePath {_quote(tmp_path / 'denied.json')} "
                         "-AllowNoncanonicalLayoutForTest -SkipSignatureValidationForTest\nexit $LASTEXITCODE",
                         env, timeout=180)
            assert denied.returncode != 0
            assert "healthy lifecycle stop marker is not the exact normal removal marker" in denied.stderr
            assert paths.removal_report_path.read_bytes() == previous_report
            assert marker_path.read_bytes() == stopped_raw
            assert (install / "bootstrap-integrity.json").read_bytes() == old_record
            assert _hashes(paths) == before

            removed = _ps(tmp_path, removal_command, env)
            assert removed.returncode == 0, removed.stderr
            # PS 5.1 must retain diagnostic stderr without promoting it into a
            # terminating NativeCommandError before the native exit is read.
            assert "storage_root_selected" in (tmp_path / "removal.stderr.log").read_text()
            report = json.loads(paths.removal_report_path.read_bytes())
            successor, _, successor_hash = read_stop_marker(marker_path)
            assert report["status"] == "PASS_DATA_PRESERVED"
            assert report["data_preserved"] is True
            assert Path(report["machine_code_root"]) == install
            assert report["relay_process"]["status"] == "ABSENT"
            assert report["relay_process"]["request_id"] == successor["request_id"]
            assert report["relay_process"]["stop_request_sha256"] == successor_hash
            assert successor["predecessor_marker"] == stopped
            assert successor["predecessor_sha256"] == stopped_hash
            assert successor["lineage_depth"] == 1
            assert _hashes(paths) == before
            assert all(path.read_bytes() == value for path, value in preserved.items())
            (tmp_path / "recovery-removal.json").write_bytes(paths.removal_report_path.read_bytes())
            (tmp_path / "recovery-marker.json").write_bytes(marker_path.read_bytes())
        result = _ps(tmp_path, f"& {_quote(script)} -SourceRoot {_quote(candidate)} -InstallRoot {_quote(install)} "
                     f"-EvidencePath {_quote(tmp_path / 'audit.json')} -AllowNoncanonicalLayoutForTest "
                     "-SkipSignatureValidationForTest\nexit $LASTEXITCODE", env, timeout=240)
        assert result.returncode == 0, result.stderr[-3600:] + result.stdout[-1000:]
        audit = json.loads((tmp_path / "audit.json").read_text(encoding="utf-8-sig"))
        assert audit["status"] == "PASS"
        assert not (paths.control_dir / "label_match_user_relay.stop.json").exists()
        assert json.loads((install / "portable-manifest.json").read_text())["source_commit"] == (
            json.loads((candidate / "portable-manifest.json").read_text())["source_commit"]
        )
        backups = list(install.parent.glob(".current.rollback.*"))
        assert len(backups) == 1
        assert (backups[0] / "bootstrap-integrity.json").read_bytes() == old_record
        assert (backups[0] / "app/assets/Item.csv").read_bytes() == old_catalog
        assert (install / "app/assets/Item.csv").read_bytes() == old_catalog
        record = (install / "bootstrap-integrity.json").read_bytes()
        probe = """import json,pathlib,sys
root=pathlib.Path(sys.argv[1])
sys.path[:0]=[str(root/'app'),str(root/'app/site-packages')]
import current_user_onboarding as onboarding
print(json.dumps(onboarding.verify_bootstrap_integrity(onboarding.resolve_current_user_onboarding_paths(root),required=True)))
"""
        for attempt in range(2):
            restarted = subprocess.run([str(install / "runtime/python.exe"), "-I", "-B", "-c", probe, str(install)],
                                       env=env, capture_output=True, text=True, timeout=90)
            assert restarted.returncode == 0, restarted.stderr
            assert json.loads(restarted.stdout)["status"] == "PASS"
            assert (install / "bootstrap-integrity.json").read_bytes() == record
            assert _hashes(paths) == before
            assert all(path.read_bytes() == value for path, value in preserved.items())
            assert {p.name: p.read_bytes() for p in cache.iterdir()} == cache_before
            (tmp_path / f"restart-{attempt + 1}.json").write_text(restarted.stdout, encoding="utf-8")
    finally:
        _stop_fixture(tmp_path, install, env)
