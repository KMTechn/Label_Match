"""Historical stock packages, with OS/credential adapters outside their bytes."""
from __future__ import annotations

import json
from pathlib import Path
import shutil
import subprocess
import sys

import pytest

from tools import build_portable_release_candidate as builder
from tests.test_healthy_lifecycle import _adapters, _fixture, _hashes, _stop_fixture
from tests.test_writer_transition import ROOT, _definitions, _environment, _ps, _quote


@pytest.fixture(scope="module")
def stock_packages(tmp_path_factory):
    root = tmp_path_factory.mktemp("stock")
    packages = []
    for revision in ("d0e504e", "aa4d59b"):
        source = root / (revision + "-source")
        subprocess.run(["git", "clone", "--quiet", "--no-hardlinks", "--no-checkout",
                        "-c", "core.autocrlf=false", str(ROOT), str(source)],
                       check=True, capture_output=True)
        subprocess.run(["git", "-C", str(source), "reset", "--hard", revision],
                       check=True, capture_output=True)
        if revision == "aa4d59b":
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
        result = _ps(tmp_path, f"& {_quote(script)} -SourceRoot {_quote(candidate)} -InstallRoot {_quote(install)} "
                     f"-EvidencePath {_quote(tmp_path / 'audit.json')} -AllowNoncanonicalLayoutForTest "
                     "-SkipSignatureValidationForTest\nexit $LASTEXITCODE", env, timeout=240)
        assert result.returncode == 0, result.stderr[-3600:] + result.stdout[-1000:]
        audit = json.loads((tmp_path / "audit.json").read_text(encoding="utf-8-sig"))
        assert audit["status"] == "PASS"
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
            assert {p.name: p.read_bytes() for p in cache.iterdir()} == cache_before
            (tmp_path / f"restart-{attempt + 1}.json").write_text(restarted.stdout, encoding="utf-8")
    finally:
        _stop_fixture(tmp_path, install, env)
