"""3f4574a -> legacy-label transition upgrade through the real writer guard.

Both sides are stock packages from their own commits. Only the installer's
read-only preflight runs (manifest, installed bootstrap integrity, inventory,
writer transition); native/UAC placement is the lab guest's.
"""
from __future__ import annotations

import ast
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys

import pytest

from tests.test_writer_transition import (
    INSTALLER, ROOT, _definitions, _environment, _pin, _ps, _quote, _upgrade_preflight,
)


BASE = "3f4574a"
PINNED = "PINNED_LEGACY_LABEL_TRANSITION_MODE"
CHANGED_LINE = "\nUNREVIEWED_RUNTIME_CHANGE = True\n"


def _release_digest(data: bytes) -> str:
    return hashlib.sha256(data.replace(b"\r\n", b"\n")).hexdigest()


def _declared_pair():
    source = INSTALLER.read_text(encoding="utf-8")
    start = source.index("$probe = @'") + len("$probe = @'")
    tree = ast.parse(source[start:source.index("\n'@", start)].lstrip())
    values = {node.targets[0].id: ast.literal_eval(node.value) for node in tree.body
              if isinstance(node, ast.Assign) and isinstance(node.targets[0], ast.Name)
              and node.targets[0].id in {"legacy_label_replacements", "legacy_label_additions"}}
    return values["legacy_label_replacements"], values["legacy_label_additions"]


@pytest.fixture(scope="module")
def legacy_label_packages(tmp_path_factory):
    root = tmp_path_factory.mktemp("legacy-label-stock")
    head = subprocess.check_output(["git", "-C", str(ROOT), "rev-parse", "HEAD"], text=True).strip()
    packages = []
    for name, revision in (("old", BASE), ("candidate", head)):
        source = root / (name + "-source")
        subprocess.run(["git", "clone", "--quiet", "--no-hardlinks", "--no-checkout",
                        "-c", "core.autocrlf=false", str(ROOT), str(source)], check=True, capture_output=True)
        subprocess.run(["git", "-C", str(source), "reset", "--hard", revision], check=True, capture_output=True)
        package = root / (name + "-portable")
        # Each side's own stock builder, as installed and as shipped.
        subprocess.run([sys.executable, "-I", "-B", str(source / "tools/build_portable_release_candidate.py"),
                        "--repo-root", str(source), "--output", str(package),
                        "--update-manifest-public-key-config", "11" * 32],
                       check=True, capture_output=True, timeout=600)
        packages.append(package)
    return tuple(packages)


def _tree(package: Path, target: Path, rewritten=()) -> Path:
    """Hard-link a package; every file a case rewrites becomes a private copy."""
    shutil.copytree(package, target, copy_function=os.link)
    for relative in (*rewritten, "app/writer_session_fence.py", "tools/label_writer_fence.ps1"):
        (target / relative).unlink()
        shutil.copy2(package / relative, target / relative)
    return target


def _seed_base_work(env) -> dict[Path, bytes]:
    """What a 3f4574a PC can leave behind: an open set, a held 5/5 set, rows
    and a queue not yet sent. The guard neither reads nor changes it."""
    data, sync = Path(env["LABEL_MATCH_SAVE_DIR"]), Path(env["LABEL_MATCH_DIRECT_SYNC_ROOT"])
    open_set = {"set_id": "OPEN-1", "scans": ["MASTER-A", "PRODUCT-1"]}
    held_set = {"set_id": "HELD-1", "scans": ["MASTER-B", "P-1", "P-2", "P-3", "FINAL-B"],
                "package_logistics": {"status": "PENDING"}}
    files = {
        data / "작업자_current_set_state_packaging.json": json.dumps(open_set, ensure_ascii=False).encode(),
        data / "lane" / "held_set_state.json": json.dumps(held_set, ensure_ascii=False).encode(),
        data / "포장실작업이벤트로그_PC01_20260929.csv": "timestamp,event,details\r\n".encode("utf-8-sig"),
        data / "package_logistics_outbox.sqlite3": b"SQLite format 3\x00fixture outbox",
        sync / "spool/pending.csv": b"pending,row\r\n",
        sync / "upload_status/pending.json": b'{"fixture":"pending"}\n',
    }
    for path, value in files.items():
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(value)
    return files


def test_declared_pair_is_exactly_3f4574a_and_this_tree():
    replacements, additions = _declared_pair()
    for relative, (before, after) in replacements.items():
        name = relative.removeprefix("app/")
        assert _release_digest(subprocess.check_output(["git", "-C", str(ROOT), "show", f"{BASE}:{name}"])) == before, relative
        assert _release_digest((ROOT / name).read_bytes()) == after, relative
    for relative, after in additions.items():
        name = relative.removeprefix("app/")
        assert subprocess.run(["git", "-C", str(ROOT), "cat-file", "-e", f"{BASE}:{name}"],
                              capture_output=True).returncode != 0, relative
        assert _release_digest((ROOT / name).read_bytes()) == after, relative
    assert set(additions) == {"app/label_transition.py"}


@pytest.mark.parametrize("base_work", [False, True], ids=["no-work", "open-and-held-sets"])
def test_stock_3f4574a_upgrade_passes_the_writer_guard(tmp_path, legacy_label_packages, base_work):
    old, candidate = legacy_label_packages
    installed = _tree(old, tmp_path / "installed")
    env = _environment(tmp_path)
    work = _seed_base_work(env) if base_work else {}
    result = _ps(tmp_path, _definitions() + f"""
. (Join-Path {_quote(candidate)} 'tools/bootstrap_integrity.ps1') -SharedCodeRoot {_quote(candidate)}
[void](Write-BootstrapIntegrityRecord -Root {_quote(installed)} -CodeRoot {_quote(installed)})
$before = PortableInventory {_quote(installed)}
$recordBefore = Sha (Join-Path {_quote(installed)} 'bootstrap-integrity.json')
{_upgrade_preflight(candidate, installed)}
if ((PortableInventory {_quote(installed)}).sha256 -cne $before.sha256 -or
    (Sha (Join-Path {_quote(installed)} 'bootstrap-integrity.json')) -cne $recordBefore) {{ throw 'Preflight changed the preimage' }}
$writerTransition | ConvertTo-Json -Compress
""", env, timeout=300)
    assert result.returncode == 0, result.stdout + result.stderr
    payload = json.loads(result.stdout)
    assert payload["compatibility"] == PINNED
    assert payload["installed_inventory_sha256"] != payload["candidate_inventory_sha256"]
    assert all(path.read_bytes() == value for path, value in work.items())
    assert not Path(env["KMTECH_LABEL_WRITER_CONTROL_ROOT"]).exists()


@pytest.mark.parametrize("change,reason", [
    ("reverse", "WRITER_TRANSITION_SOURCE_SET_DIFFERS"),
    ("other-preimage", "WRITER_TRANSITION_SOURCE_SET_DIFFERS"),
    ("other-candidate", "WRITER_TRANSITION_SOURCE_SET_DIFFERS"),
    ("second-addition", "WRITER_TRANSITION_SOURCE_SET_DIFFERS"),
    ("addition-already-installed", "WRITER_TRANSITION_SEMANTICS_DIFFER"),
    ("unrelated-module", "WRITER_TRANSITION_SEMANTICS_DIFFER: app/user_relay.py"),
    ("data", "WRITER_TRANSITION_SEMANTICS_DIFFER: app/assets/Item.csv"),
])
def test_stock_3f4574a_guard_still_refuses_everything_else(tmp_path, legacy_label_packages, change, reason):
    old, candidate = legacy_label_packages
    source, installed = (old, candidate) if change == "reverse" else (candidate, old)
    if change == "other-preimage":
        installed = _tree(old, tmp_path / "installed", ["app/Label_Match.py"])
        with (installed / "app/Label_Match.py").open("a", encoding="utf-8") as stream:
            stream.write(CHANGED_LINE)
        _pin(installed)
    elif change == "addition-already-installed":
        installed = _tree(old, tmp_path / "installed")
        shutil.copy2(candidate / "app/label_transition.py", installed / "app/label_transition.py")
        _pin(installed)
    elif change in {"other-candidate", "unrelated-module"}:
        name = "app/user_relay.py" if change == "unrelated-module" else "app/Label_Match.py"
        source = _tree(candidate, tmp_path / "candidate", [name])
        with (source / name).open("a", encoding="utf-8") as stream:
            stream.write(CHANGED_LINE)
        _pin(source)
    elif change == "second-addition":
        source = _tree(candidate, tmp_path / "candidate")
        (source / "app/unreviewed_leaf.py").write_text("VALUE = 1\n", encoding="utf-8")
        _pin(source)
    elif change == "data":
        source = _tree(candidate, tmp_path / "candidate", ["app/assets/Item.csv"])
        with (source / "app/assets/Item.csv").open("ab") as stream:
            stream.write(b"unreviewed data change\n")
    env = _environment(tmp_path)
    result = _ps(tmp_path, _definitions() + f"\nAssert-WriterTransition {_quote(source)} {_quote(installed)}", env)
    assert result.returncode != 0, result.stdout
    assert reason in result.stderr, result.stdout + result.stderr
