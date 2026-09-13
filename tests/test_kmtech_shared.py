"""Pinned shared inputs and Label_Match compatibility facades."""
from contextlib import contextmanager
import hashlib
import inspect
import json
from pathlib import Path
import shutil
import subprocess
import sys

import pytest

from kmtech_shared import raster as core
from kmtech_zero_pe import raster as facade
from tools import build_portable_release_candidate as builder


ROOT = Path(__file__).resolve().parents[1]
CHECKER = ROOT / "qualification/check_kmtech_shared.py"


def _check(root):
    return subprocess.run(
        [sys.executable, "-I", "-B", str(CHECKER), "--check", "--root", str(root)],
        capture_output=True, text=True, check=False, timeout=30,
    )


def test_pinned_shared_source_and_packaging_inputs():
    result = _check(ROOT)
    assert result.returncode == 0, result.stdout + result.stderr
    # The tracked frozen builder generates its spec; the legacy local .spec is ignored.
    frozen = (ROOT / "tools/build_frozen_release_candidate.ps1").read_text(encoding="utf-8")
    for module in ("kmtech_shared.catalog", "kmtech_shared.raster", "kmtech_shared.runtime"):
        assert f'"--hidden-import", "{module}"' in frozen
    for name in ("kmtech_shared.manifest.json", "kmtech_shared.lock.json"):
        assert f'"--add-data", "$(Join-Path $repoRoot \'{name}\');."' in frozen
        assert f'--expected-file {name} `' in frozen
    provenance = json.loads((ROOT / "kmtech_zero_pe.vendor.json").read_bytes())
    for name, digest in provenance["files"].items():
        assert hashlib.sha256((ROOT / name).read_bytes()).hexdigest() == digest


@pytest.mark.parametrize("mutation", ["missing", "extra", "changed", "manifest"])
def test_local_checker_rejects_shared_source_drift(tmp_path, mutation):
    shutil.copytree(ROOT / "kmtech_shared", tmp_path / "kmtech_shared")
    for name in ("kmtech_shared.manifest.json", "kmtech_shared.lock.json"):
        shutil.copyfile(ROOT / name, tmp_path / name)
    if mutation == "missing":
        (tmp_path / "kmtech_shared/catalog.py").unlink()
    elif mutation == "extra":
        (tmp_path / "kmtech_shared/extra.py").write_text("", encoding="utf-8")
    elif mutation == "changed":
        with (tmp_path / "kmtech_shared/catalog.py").open("ab") as stream:
            stream.write(b"\n# unexpected drift\n")
    else:
        path = tmp_path / "kmtech_shared.manifest.json"
        path.write_bytes(path.read_bytes() + b"\n")
    result = _check(tmp_path)
    assert result.returncode == 1
    assert "FAIL:" in result.stderr


@pytest.mark.parametrize("mutation", ["version", "hash", "fields", "malformed"])
def test_local_checker_rejects_invalid_lock(tmp_path, mutation):
    shutil.copytree(ROOT / "kmtech_shared", tmp_path / "kmtech_shared")
    shutil.copyfile(ROOT / "kmtech_shared.manifest.json", tmp_path / "kmtech_shared.manifest.json")
    lock = json.loads((ROOT / "kmtech_shared.lock.json").read_bytes())
    if mutation == "version":
        lock["version"] = "0.0.0"
    elif mutation == "hash":
        lock["manifest_sha256"] = "0" * 64
    elif mutation == "fields":
        lock.pop("manifest_sha256")
    path = tmp_path / "kmtech_shared.lock.json"
    path.write_text("{" if mutation == "malformed" else json.dumps(lock), encoding="utf-8")
    result = _check(tmp_path)
    assert result.returncode == 1
    assert "FAIL:" in result.stderr


def test_local_checker_tests_pass_without_sibling_checkout(tmp_path):
    app = tmp_path / "app"
    builder._copy_application(ROOT, app)
    for name in ("tests/test_kmtech_shared.py", "tests/integration/check_kmtech_shared_canonical.py",
                 "qualification/check_kmtech_shared.py", "tools/build_portable_release_candidate.py",
                 "tools/build_frozen_release_candidate.ps1"):
        target = app / name
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(ROOT / name, target)
    assert not (app.parent / "kmtech_shared").exists()
    code = """
import pathlib, sys
app = pathlib.Path(sys.argv[1]).resolve()
sys.path.insert(0, str(app))
def guard(event, args):
    if event in {'socket.connect', 'socket.bind'}:
        raise RuntimeError('Standalone pin test forbids network')
sys.addaudithook(guard)
import tkinter
def deny(*args, **kwargs):
    raise RuntimeError('Standalone pin test forbids GUI')
tkinter.Tk.__init__ = deny
tkinter.Toplevel.__init__ = deny
import pytest
result = pytest.main(sys.argv[2:])
for name in ('kmtech_shared', 'kmtech_zero_pe', 'tools.build_portable_release_candidate'):
    assert pathlib.Path(sys.modules[name].__file__).resolve().is_relative_to(app), name
raise SystemExit(result)
"""
    command = [sys.executable, "-I", "-B", "-c", code, str(app),
               "-q", "-p", "no:cacheprovider", "--tb=short", "--rootdir", str(app),
               "--basetemp", str(tmp_path / "child-pytest")]
    test = str(app / "tests/test_kmtech_shared.py")
    result = subprocess.run(
        [*command, test + "::test_pinned_shared_source_and_packaging_inputs",
         test + "::test_local_checker_rejects_shared_source_drift"],
        cwd=app, capture_output=True, text=True, check=False, timeout=120,
    )
    assert result.returncode == 0, result.stdout + result.stderr
    assert "5 passed" in result.stdout
    collected = subprocess.run(
        [*command, "--collect-only", "tests"], cwd=app,
        capture_output=True, text=True, check=False, timeout=120,
    )
    assert collected.returncode == 0, collected.stdout + collected.stderr
    assert "test_pinned_shared_source_and_packaging_inputs" in collected.stdout
    assert "test_canonical_shared_source_matches_app_checker" not in collected.stdout


def test_every_image_factory_retains_label_class_signature_and_png_bytes(tmp_path):
    path = tmp_path / "source.png"
    image = facade.RasterImage.solid(6, 4, (10, 20, 30))
    path.write_bytes(image.to_png_bytes())
    images = [image, facade.RasterImage.from_png(path),
              facade.RasterImage.from_png_bytes(path.read_bytes()),
              image.resized(6, 4), image.resized(3, 2),
              image.resized(3, 2, resample="nearest"),
              image.contain((30, 20)), image.contain((3, 2))]
    with facade.RasterCanvas(6, 4, background=(10, 20, 30)) as canvas:
        images.extend([canvas.snapshot(), canvas.snapshot().resized(3, 2)])
    assert str(inspect.signature(facade.RasterImage.save_png)) == (
        "(self, path: 'str | os.PathLike[str]', *, dpi: 'tuple[int, int] | None' = None, "
        "atomic: 'bool' = True) -> 'dict[str, object]'"
    )
    for index, result in enumerate(images):
        assert type(result) is facade.RasterImage
        expected = core.RasterImage(result.width, result.height, result.bgra)
        for atomic in (True, False):
            target = tmp_path / f"saved-{index}.png"
            receipt = result.save_png(target, dpi=(300, 300), atomic=atomic)
            assert target.read_bytes() == expected.to_png_bytes(dpi=(300, 300))
            assert receipt["sha256"] == hashlib.sha256(target.read_bytes()).hexdigest()


def test_label_exchange_writer_denial_precedes_renderer_and_png_write(tmp_path, monkeypatch):
    import writer_session_fence as fence
    from phs_label_workflow import PHSLabelExchangeCoordinator, PHSLabelRenderer

    @contextmanager
    def denied(source, **kwargs):
        assert source == "phs_single_exchange"
        raise fence.WriterFencedError("TEST_DENIED", "synthetic admission denied")
        yield

    def forbidden(*args, **kwargs):
        pytest.fail("writer denial must precede render/save")

    monkeypatch.setattr(fence, "writer_admission", denied)
    monkeypatch.setattr(PHSLabelRenderer, "render", forbidden)
    monkeypatch.setattr(facade.RasterImage, "save_png", forbidden)
    with pytest.raises(fence.WriterFencedError, match="synthetic admission denied"):
        PHSLabelExchangeCoordinator.execute_single(object(), {}, {}, persist_current_set=lambda: True)
    assert list(tmp_path.iterdir()) == []


def test_portable_copy_imports_one_shared_module_identity(tmp_path):
    app = tmp_path / "app"
    sources = builder._copy_application(ROOT, app)
    builder._assert_portable_import_closure(tmp_path, ROOT, sources, Path(sys.executable))
    checked = _check(app)
    assert checked.returncode == 0, checked.stdout + checked.stderr
    code = """
import pathlib, sys
sys.path.insert(0, sys.argv[1])
from kmtech_shared import raster, catalog
from kmtech_zero_pe import raster as facade, gdi_print, RasterImage, RasterCanvas
import Label_Match, phs_label_workflow, item_catalog_sync
assert item_catalog_sync._catalog_core is catalog
assert facade.RasterImage.__bases__ == (raster.RasterImage,)
assert facade.RasterCanvas.__bases__ == (raster.RasterCanvas,)
assert RasterImage is gdi_print.RasterImage is facade.RasterImage
assert Label_Match.RasterCanvas is RasterCanvas is phs_label_workflow.RasterCanvas is facade.RasterCanvas
for module in (catalog, raster):
    origin = pathlib.Path(module.__file__).resolve()
    assert origin.is_relative_to(pathlib.Path(sys.argv[1]).resolve())
    identities = [name for name, loaded in list(sys.modules.items())
                  if getattr(loaded, '__file__', None)
                  and pathlib.Path(loaded.__file__).resolve() == origin]
    assert identities == [module.__name__], identities
print('PASS: portable import closure and single shared module identity')
"""
    result = subprocess.run(
        [sys.executable, "-I", "-B", "-c", code, str(app)], cwd=tmp_path,
        capture_output=True, text=True, check=False, timeout=60,
    )
    assert result.returncode == 0, result.stdout + result.stderr


def test_pre_adoption_v2_sidecars_read_write_and_recover_unchanged(tmp_path, monkeypatch):
    import item_catalog_sync as sync

    # Captured from 164b5da before adoption, using only a synthetic identity/token.
    fixture = json.loads((ROOT / "tests/fixtures/item_catalog_v2_label.json").read_bytes())
    payload = fixture["catalog_utf8"].encode("utf-8")
    binding = fixture["binding"]
    monkeypatch.setattr(sync, "_load_item_catalog_logistics_profile", lambda: None)
    cache = tmp_path / "Item.csv"
    cache.write_bytes(payload)
    authority_path = tmp_path / "Item.csv.authority.json"
    recovery_path = tmp_path / "Item.csv.recovery.json"
    authority_path.write_bytes(fixture["authority_json"].encode("utf-8"))
    recovery_path.write_bytes(fixture["recovery_json"].encode("utf-8"))
    assert sync._read_authenticated_cache_payload(cache, **binding) == payload
    rotated = dict(binding, bearer_token="rotated-synthetic-token")
    assert sync._read_authenticated_cache_payload(cache, **rotated) is None
    assert sync._read_authenticated_recovery_payload(cache, **rotated) is None
    sync._write_authenticated_cache(cache, payload, **binding)
    assert cache.read_bytes() == payload
    assert authority_path.read_bytes() == fixture["authority_json"].encode("utf-8")
    assert recovery_path.read_bytes() == fixture["recovery_json"].encode("utf-8")
    cache.write_bytes(b"interrupted primary write")
    assert sync._read_authenticated_cache_payload(cache, **binding) is None
    recovered = sync._recover_authenticated_cache(cache, **binding)
    assert recovered == tmp_path / "Item.csv.last-good"
    assert recovered.read_bytes() == payload
    assert sync.get_verified_catalog_snapshot(recovered) == payload
    assert cache.read_bytes() == b"interrupted primary write"
