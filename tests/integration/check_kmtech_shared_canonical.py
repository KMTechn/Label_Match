"""Explicit integration node; the filename excludes it from default discovery."""
import importlib.util
import inspect
import json
import subprocess
import sys

from qualification import check_kmtech_shared as local


def test_canonical_shared_source_matches_app_checker():
    canonical_root = local.ROOT.parent / "kmtech_shared"
    checker = canonical_root / "manifest/sync_shared.py"
    assert checker.is_file(), "Explicit integration check requires the canonical kmtech_shared checkout"
    spec = importlib.util.spec_from_file_location("canonical_shared_checker", checker)
    canonical = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(canonical)
    assert local.FILES == canonical.FILES
    assert local.SCHEMA == canonical.SCHEMA
    for name in ("digest", "inventory", "package_version", "check_manifest"):
        assert inspect.getsource(getattr(local, name)) == inspect.getsource(getattr(canonical, name)), name
    assert (local.ROOT / "kmtech_shared.manifest.json").read_bytes() == (
        canonical_root / "manifest/shared-source.json").read_bytes()
    lock = json.loads((local.ROOT / "kmtech_shared.lock.json").read_bytes())
    result = subprocess.run(
        [sys.executable, "-I", "-B", str(checker), "--check", "--root", str(local.ROOT),
         "--manifest", str(local.ROOT / "kmtech_shared.manifest.json"),
         "--expected-sha256", lock["manifest_sha256"]],
        capture_output=True, text=True, check=False, timeout=30,
    )
    assert result.returncode == 0, result.stdout + result.stderr
