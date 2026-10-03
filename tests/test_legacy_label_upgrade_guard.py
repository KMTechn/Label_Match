"""3f4574a -> legacy-label transition upgrade through the real writer guard.

Both sides are stock packages from their own commits. Only the installer's
read-only preflight runs (manifest, installed bootstrap integrity, inventory,
writer transition); native/UAC placement is the lab guest's.
"""
from __future__ import annotations

import ast
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import zipfile

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
    return _stock_label_packages(tmp_path_factory, "legacy-label-stock", BASE)


def _stock_label_packages(tmp_path_factory, prefix, baseline, candidate=None):
    root = tmp_path_factory.mktemp(prefix)
    head = subprocess.check_output(["git", "-C", str(ROOT), "rev-parse", "HEAD"], text=True).strip()
    packages = []
    revisions = [("old", baseline)] + ([("candidate", head)] if candidate is None else [])
    for name, revision in revisions:
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
    return tuple(packages) if candidate is None else (packages[0], candidate)


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


def test_declared_scan_input_pairs_are_exactly_c83c72c_817d393_and_92abdaa_to_this_tree():
    """c83c72c (w9lmtransit), 817d393 (the 1733-only TEST1 candidate) and
    92abdaa (w9lmscanmsg) may be installed; each upgrades to this tree, never
    the other way. The companions include the reviewed scanner audit storage
    changes; all three accepted releases share the same preimages."""
    source = INSTALLER.read_text(encoding="utf-8")
    start = source.index("$probe = @'") + len("$probe = @'")
    tree = ast.parse(source[start:source.index("\n'@", start)].lstrip())
    values = {node.targets[0].id: ast.literal_eval(node.value) for node in tree.body
              if isinstance(node, ast.Assign) and isinstance(node.targets[0], ast.Name)
              and node.targets[0].id in {"scan_input_pairs", "scan_input_companions"}}
    pairs, companions = values["scan_input_pairs"], values["scan_input_companions"]
    installs = ("c83c72c", "817d393", "92abdaa")
    here = _release_digest((ROOT / "Label_Match.py").read_bytes())
    befores = {_release_digest(subprocess.check_output(["git", "-C", str(ROOT), "show", f"{commit}:Label_Match.py"]))
               for commit in installs}
    assert pairs == {(before, here) for before in befores}
    assert not any((here, before) in pairs for before in befores)
    assert set(companions) == {
        "app/carrier_identity_port.py", "app/deferred_intent_capture.py", "app/label_data_manager.py",
        "app/label_completion.py", "app/label_recovery_schema.py", "app/label_transition.py",
        "app/label_workbench_history.py", "app/package_command_draft.py", "app/package_logistics.py",
        "app/package_outbox.py", "app/package_outbox_processor.py", "app/sealed_transfer_exchange.py",
        "app/terminal_operation_lease.py", "app/ui/workflow_view_state.py",
    }
    for relative, (before, after) in companions.items():
        name = relative.removeprefix("app/")
        assert {_release_digest(subprocess.check_output(["git", "-C", str(ROOT), "show", f"{commit}:{name}"]))
                for commit in installs} == {before}, relative
        assert _release_digest((ROOT / name).read_bytes()) == after != before, relative


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
    # Re-pinning a writer module now disables the exact release set before
    # the legacy leaf addition can be admitted.
    ("unrelated-module", "WRITER_TRANSITION_SOURCE_SET_DIFFERS"),
    ("roster-addition-changed", "WRITER_TRANSITION_SOURCE_SET_DIFFERS"),
    ("roster-addition-missing", "WRITER_TRANSITION_SOURCE_SET_DIFFERS"),
    ("roster-transport-changed", "WRITER_TRANSITION_SOURCE_SET_DIFFERS"),
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
    elif change in {"other-candidate", "unrelated-module", "roster-transport-changed"}:
        name = ("app/label_admin_pin.py" if change == "roster-transport-changed" else
                "app/user_relay.py" if change == "unrelated-module" else "app/Label_Match.py")
        source = _tree(candidate, tmp_path / "candidate", [name])
        with (source / name).open("a", encoding="utf-8") as stream:
            stream.write(CHANGED_LINE)
        _pin(source)
    elif change in {"roster-addition-changed", "roster-addition-missing"}:
        source = _tree(candidate, tmp_path / "candidate", ["app/label_worker_roster.py"])
        leaf = source / "app/label_worker_roster.py"
        if change == "roster-addition-missing":
            leaf.unlink()
        else:
            with leaf.open("a", encoding="utf-8") as stream:
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


@pytest.fixture(scope='module')
def worker_record_packages(tmp_path_factory, legacy_label_packages):
    return _stock_label_packages(tmp_path_factory, 'worker-record-stock', '0cf0414', legacy_label_packages[1])


def test_worker_record_declaration_matches_stage_one_and_candidate():
    pairs = _declared_current_user_profile_pairs('worker_record_replacements')
    expected = {'app/Label_Match.py', 'app/label_completion.py', 'app/label_data_manager.py',
        'app/label_recovery_schema.py', 'app/label_worker_roster.py', 'app/package_command_draft.py',
        'app/package_outbox_processor.py', 'app/sealed_transfer_exchange.py',
        'app/writer_session_fence.py', 'tools/label_writer_fence.ps1'}
    assert set(pairs) == expected
    for relative, (before, after) in pairs.items():
        path = relative.removeprefix('app/')
        original = subprocess.check_output(['git', '-C', str(ROOT), 'show', f'0cf0414:{path}'])
        assert before == _release_digest(original)
        assert after == _release_digest((ROOT / path).read_bytes()) != before


def test_stage_one_stock_upgrade_preserves_current_and_pending(tmp_path, worker_record_packages):
    old, candidate = worker_record_packages
    installed = _tree(old, tmp_path / 'installed')
    environment = _environment(tmp_path)
    work = _seed_base_work(environment)
    result = _ps(tmp_path, _definitions() + f"""
. (Join-Path {_quote(candidate)} 'tools/bootstrap_integrity.ps1') -SharedCodeRoot {_quote(candidate)}
[void](Write-BootstrapIntegrityRecord -Root {_quote(installed)} -CodeRoot {_quote(installed)})
$before = PortableInventory {_quote(installed)}
{_upgrade_preflight(candidate, installed)}
if ((PortableInventory {_quote(installed)}).sha256 -cne $before.sha256) {{ throw 'Preflight changed the preimage' }}
$writerTransition | ConvertTo-Json -Compress
""", environment, timeout=300)
    assert result.returncode == 0, result.stdout + result.stderr
    assert json.loads(result.stdout)['compatibility'] == 'PINNED_WORKER_RECORD_IDENTITY'
    assert all(path.read_bytes() == raw for path, raw in work.items())


@pytest.mark.parametrize('change', ['reverse', 'app/label_worker_roster.py', 'app/label_data_manager.py', 'app/package_command_draft.py'])
def test_worker_record_upgrade_refuses_reverse_and_unreviewed_bytes(tmp_path, worker_record_packages, change):
    old, candidate = worker_record_packages
    if change == 'reverse':
        source, installed = old, candidate
    else:
        source = _tree(candidate, tmp_path / 'candidate', [change])
        with (source / change).open('ab') as stream:
            stream.write(CHANGED_LINE.encode('utf-8'))
        _pin(source)
        installed = old
    result = _ps(tmp_path, _definitions() + f'\nAssert-WriterTransition {_quote(source)} {_quote(installed)}', _environment(tmp_path))
    assert result.returncode != 0
    assert 'WRITER_TRANSITION_' in result.stderr


NEXTFIX_BASE = "e88011c"
NEXTFIX_PINNED = "PINNED_W9NEXTFIX_LABEL_RECOVERY"


def _declared_current_user_profile_pairs(variable="current_user_profile_replacements"):
    source = INSTALLER.read_text(encoding="utf-8")
    start = source.index("$probe = @'") + len("$probe = @'")
    tree = ast.parse(source[start:source.index("\n'@", start)].lstrip())
    return next(ast.literal_eval(node.value) for node in tree.body
                if isinstance(node, ast.Assign) and isinstance(node.targets[0], ast.Name)
                and node.targets[0].id == variable)


def test_declared_current_user_profile_pair_matches_reviewed_preimage_and_tree():
    pairs = _declared_current_user_profile_pairs()
    assert set(pairs) == {
        "app/current_user_onboarding.py", "app/logistics_runtime_profile.py",
        "app/label_match_single_instance.py",
        "app/tools/install_logistics_runtime_profile.py",
        "app/enrollment_mutex.py", "app/writer_session_fence.py",
    }
    for relative, (before, after) in pairs.items():
        name = relative.removeprefix("app/")
        assert before == _release_digest(subprocess.check_output(
            ["git", "-C", str(ROOT), "show", f"a20cd01:{name}"]))
        assert after == _release_digest((ROOT / name).read_bytes())
        assert before != after


def test_writer_preimages_cover_only_the_accepted_profile_releases():
    preimages = _declared_current_user_profile_pairs("current_user_profile_writer_preimages")
    revisions = ("a20cd01", "e88011c", "c83c72c", "817d393", "92abdaa", "614369b")
    expected = {_release_digest(subprocess.check_output(
        ["git", "-C", str(ROOT), "show", f"{revision}:writer_session_fence.py"]))
        for revision in revisions}
    after = _release_digest((ROOT / "writer_session_fence.py").read_bytes())
    assert preimages == expected
    assert after not in preimages
    assert len(preimages) == 5


def test_declared_nextfix_pair_is_exactly_e88011c_and_this_tree():
    source = INSTALLER.read_text(encoding="utf-8")
    start = source.index("$probe = @'") + len("$probe = @'")
    tree = ast.parse(source[start:source.index("\n'@", start)].lstrip())
    values = {node.targets[0].id: ast.literal_eval(node.value) for node in tree.body
              if isinstance(node, ast.Assign) and isinstance(node.targets[0], ast.Name)
              and node.targets[0].id in {"nextfix_label_pair", "nextfix_label_path"}}
    pair = values["nextfix_label_pair"]
    assert values["nextfix_label_path"] == "app/Label_Match.py"
    before = _release_digest(subprocess.check_output(["git", "-C", str(ROOT), "show",
                                                      f"{NEXTFIX_BASE}:Label_Match.py"]))
    after = _release_digest((ROOT / "Label_Match.py").read_bytes())
    assert pair == (before, after)
    assert before != after


@pytest.fixture(scope="module")
def nextfix_archive_layouts(tmp_path_factory):
    root = tmp_path_factory.mktemp("nextfix-source-archives")
    head = subprocess.check_output(["git", "-C", str(ROOT), "rev-parse", "HEAD"], text=True).strip()
    layouts = []
    for name, revision in (("old", NEXTFIX_BASE), ("candidate", head)):
        archive = root / (name + ".zip")
        subprocess.run(["git", "-C", str(ROOT), "archive", "--format=zip", "-o", str(archive), revision],
                       check=True, capture_output=True)
        source = root / (name + "-source")
        with zipfile.ZipFile(archive) as stream:
            stream.extractall(source)
        spec = importlib.util.spec_from_file_location(name + "_nextfix_builder",
                                                     source / "tools/build_portable_release_candidate.py")
        builder = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(builder)
        layout = root / (name + "-layout")
        builder._copy_application(source, layout / "app")
        (layout / "runtime").mkdir()
        for relative in ("INSTALL_THIS_PC.ps1", "launch-label-match.cmd", "tools/bootstrap_integrity.ps1",
                         "tools/label_writer_fence.ps1", "tools/label_writer_fence_contract.json"):
            target = layout / relative
            target.parent.mkdir(exist_ok=True)
            origin = "portable/launch-label-match.cmd" if relative == "launch-label-match.cmd" else relative
            target.write_bytes((source / origin).read_bytes())
        layouts.append(layout)
    return tuple(layouts)


def _nextfix_probe(tmp_path, source, installed):
    """Real read-only installer probe; source archives, no runtime/packet attestation."""
    installer = INSTALLER.read_text(encoding="utf-8")
    start = installer.index("$probe = @'") + len("$probe = @'")
    probe = tmp_path / "writer-transition-probe.py"
    probe.write_text(installer[start:installer.index("\n'@", start)].lstrip(), encoding="utf-8")
    return subprocess.run([sys.executable, "-B", str(probe), str(source), str(installed)],
                          text=True, capture_output=True)


def test_e88011c_archive_upgrade_passes_nextfix_writer_guard(tmp_path, nextfix_archive_layouts):
    old, candidate = nextfix_archive_layouts
    before = (old / "app/Label_Match.py").read_bytes()
    result = _nextfix_probe(tmp_path, candidate, old)
    assert result.returncode == 0, result.stdout + result.stderr
    payload = json.loads(result.stdout)
    assert payload["compatibility"] == NEXTFIX_PINNED
    assert payload["installed_inventory_sha256"] != payload["candidate_inventory_sha256"]
    assert (old / "app/Label_Match.py").read_bytes() == before


def test_reviewed_current_user_pair_preserves_e88011c_upgrade(tmp_path, nextfix_archive_layouts):
    old, candidate = nextfix_archive_layouts
    for relative, (before, after) in _declared_current_user_profile_pairs().items():
        installed_digest = _release_digest((old / relative).read_bytes())
        if relative == "app/writer_session_fence.py":
            assert installed_digest in _declared_current_user_profile_pairs("current_user_profile_writer_preimages")
        else:
            assert installed_digest == before
        assert _release_digest((candidate / relative).read_bytes()) == after
    result = _nextfix_probe(tmp_path, candidate, old)
    assert result.returncode == 0, result.stdout + result.stderr
    assert json.loads(result.stdout)["compatibility"] == NEXTFIX_PINNED


@pytest.mark.parametrize("relative", [
    "app/current_user_onboarding.py", "app/logistics_runtime_profile.py",
    "app/label_match_single_instance.py",
    "app/tools/install_logistics_runtime_profile.py",
    "app/enrollment_mutex.py", "app/writer_session_fence.py",
])
def test_current_user_pair_refuses_changed_candidate(tmp_path, nextfix_archive_layouts, relative):
    old, candidate = nextfix_archive_layouts
    before, after = _declared_current_user_profile_pairs()[relative]
    installed_digest = _release_digest((old / relative).read_bytes())
    if relative == "app/writer_session_fence.py":
        assert installed_digest in _declared_current_user_profile_pairs("current_user_profile_writer_preimages")
    else:
        assert installed_digest == before
    assert _release_digest((candidate / relative).read_bytes()) == after
    changed = _tree(candidate, tmp_path / "changed", [relative])
    with (changed / relative).open("a", encoding="utf-8") as stream:
        stream.write(CHANGED_LINE)
    _pin(changed)
    assert _release_digest((changed / relative).read_bytes()) != after
    result = _nextfix_probe(tmp_path, changed, old)
    assert result.returncode != 0
    # Any mutation disables the atomic module set; the archive layout's
    # first ordinary AST mismatch is onboarding, including a profile mutation.
    assert "WRITER_TRANSITION_SEMANTICS_DIFFER: app/current_user_onboarding.py" in result.stderr


@pytest.mark.parametrize("relative", ["app/enrollment_mutex.py", "app/writer_session_fence.py"])
def test_mutex_pair_refuses_changed_installed_preimage(tmp_path, nextfix_archive_layouts, relative):
    old, candidate = nextfix_archive_layouts
    changed = _tree(old, tmp_path / "changed-installed", [relative])
    with (changed / relative).open("a", encoding="utf-8") as stream:
        stream.write(CHANGED_LINE)
    _pin(changed)
    result = _nextfix_probe(tmp_path, candidate, changed)
    assert result.returncode != 0
    assert "WRITER_TRANSITION_SEMANTICS_DIFFER" in result.stderr


def test_declared_scanner_pair_and_companions_match_reviewed_sources():
    source = INSTALLER.read_text(encoding="utf-8")
    start = source.index("$probe = @'") + len("$probe = @'")
    tree = ast.parse(source[start:source.index("\n'@", start)].lstrip())
    values = {node.targets[0].id: ast.literal_eval(node.value) for node in tree.body
              if isinstance(node, ast.Assign) and isinstance(node.targets[0], ast.Name)
              and node.targets[0].id in {"scanner_label_pair", "nextfix_companions"}}
    assert values["scanner_label_pair"] == (
        _release_digest(subprocess.check_output(["git", "-C", str(ROOT), "show", "a20cd01:Label_Match.py"])),
        _release_digest((ROOT / "Label_Match.py").read_bytes()),
    )
    for relative, (before, after) in values["nextfix_companions"].items():
        name = relative.removeprefix("app/")
        assert before == _release_digest(subprocess.check_output(["git", "-C", str(ROOT), "show", f"a20cd01:{name}"]))
        assert after == _release_digest((ROOT / name).read_bytes()) != before


def test_baseline_archive_upgrades_only_forward_to_scanner_source(tmp_path, nextfix_archive_layouts):
    old, candidate = nextfix_archive_layouts
    baseline = _tree(old, tmp_path / "baseline", ["app/Label_Match.py"])
    (baseline / "app/Label_Match.py").write_bytes(subprocess.check_output(["git", "-C", str(ROOT), "show", "a20cd01:Label_Match.py"]))
    _pin(baseline)
    preserved = (baseline / "app/Label_Match.py").read_bytes()
    result = _nextfix_probe(tmp_path, candidate, baseline)
    assert result.returncode == 0, result.stdout + result.stderr
    assert json.loads(result.stdout)["compatibility"] == "PINNED_W9SCANONLY_WORKFLOW"
    assert (baseline / "app/Label_Match.py").read_bytes() == preserved
    reverse = _nextfix_probe(tmp_path, baseline, candidate)
    assert reverse.returncode != 0
    assert "WRITER_TRANSITION_SEMANTICS_DIFFER" in reverse.stderr


@pytest.mark.parametrize("change,reason", [
    ("membership", "WRITER_TRANSITION_SOURCE_SET_DIFFERS"),
    # The relay mutation invalidates the atomic profile set. A changed or
    # reversed scanner source fails its directional Label_Match pair first.
    ("other-app", "WRITER_TRANSITION_SEMANTICS_DIFFER: app/current_user_onboarding.py"),
    ("other-label", "WRITER_TRANSITION_SEMANTICS_DIFFER: app/Label_Match.py"),
    ("reverse", "WRITER_TRANSITION_SEMANTICS_DIFFER: app/Label_Match.py"),
    ("contract", "WRITER_TRANSITION_CONTRACT_DIFFERS: launch-label-match.cmd"),
])
def test_nextfix_archive_guard_refuses_other_changes(tmp_path, nextfix_archive_layouts, change, reason):
    old, candidate = nextfix_archive_layouts
    if change == "reverse":
        source, installed = old, candidate
    else:
        relative = {"membership": "app/user_relay.py", "other-app": "app/user_relay.py", "other-label": "app/Label_Match.py",
                    "contract": "launch-label-match.cmd"}[change]
        source = _tree(candidate, tmp_path / "candidate", [relative])
        installed = old
        with (source / relative).open("ab") as stream:
            stream.write(b'\n@writer_sink("unreviewed_membership")\ndef unreviewed_writer():\n    return None\n'
                         if change == "membership" else
                         CHANGED_LINE.encode() if change == "other-label" else b"X")
        _pin(source)  # Refusal is semantic/contract evidence, rather than a stale-pin failure.
    result = _nextfix_probe(tmp_path, source, installed)
    assert result.returncode != 0, result.stdout
    assert reason in result.stderr, result.stdout + result.stderr


ROSTER_BASELINE = "134bab3c2059ce500f752a3ba09eb83a6d2c0b40"


@pytest.fixture(scope="module")
def roster_baseline_packages(tmp_path_factory, legacy_label_packages):
    return _stock_label_packages(tmp_path_factory, "roster-baseline-stock",
                                 ROSTER_BASELINE, legacy_label_packages[1])


def test_roster_baseline_declares_only_exact_changed_files():
    pairs = _declared_current_user_profile_pairs("roster_baseline_replacements")
    assert set(pairs) == {"app/Label_Match.py", "app/label_admin_pin.py",
                          "app/writer_session_fence.py", "tools/label_writer_fence.ps1"}
    for relative, (before, after) in pairs.items():
        name = relative.removeprefix("app/")
        original = subprocess.check_output(["git", "-C", str(ROOT), "show", f"{ROSTER_BASELINE}:{name}"])
        assert _release_digest(original) == before
        assert _release_digest((ROOT / name).read_bytes()) == after != before


def test_roster_baseline_stage_two_companions_match_only_changed_runtime():
    pairs = _declared_current_user_profile_pairs('roster_baseline_companions')
    assert set(pairs) == {'app/label_completion.py', 'app/label_data_manager.py',
        'app/label_recovery_schema.py', 'app/package_command_draft.py',
        'app/package_outbox_processor.py', 'app/sealed_transfer_exchange.py'}
    for relative, (before, after) in pairs.items():
        name = relative.removeprefix('app/')
        original = subprocess.check_output(['git', '-C', str(ROOT), 'show', f'{ROSTER_BASELINE}:{name}'])
        assert before == _release_digest(original)
        assert after == _release_digest((ROOT / name).read_bytes()) != before


@pytest.mark.parametrize('relative', ['app/label_completion.py', 'app/label_data_manager.py',
    'app/label_recovery_schema.py', 'app/package_command_draft.py',
    'app/package_outbox_processor.py', 'app/sealed_transfer_exchange.py'])
def test_roster_baseline_stage_two_companions_refuse_unreviewed_bytes(tmp_path, roster_baseline_packages, relative):
    old, candidate = roster_baseline_packages
    source = _tree(candidate, tmp_path / 'candidate', [relative])
    with (source / relative).open('ab') as stream:
        stream.write(CHANGED_LINE.encode('utf-8'))
    _pin(source)
    result = _ps(tmp_path, _definitions() + f'\nAssert-WriterTransition {_quote(source)} {_quote(old)}', _environment(tmp_path))
    assert result.returncode != 0
    assert 'WRITER_TRANSITION_' in result.stderr


def test_roster_baseline_stock_upgrades_forward_and_refuses_reverse(tmp_path, roster_baseline_packages):
    old, candidate = roster_baseline_packages
    env = _environment(tmp_path)
    before = _seed_base_work(env)
    result = _ps(tmp_path, _definitions() + f"\nAssert-WriterTransition {_quote(candidate)} {_quote(old)}", env)
    assert result.returncode == 0, result.stdout + result.stderr
    assert all(path.read_bytes() == raw for path, raw in before.items())
    reverse = _ps(tmp_path, _definitions() + f"\nAssert-WriterTransition {_quote(old)} {_quote(candidate)}", env)
    assert reverse.returncode != 0
    assert "WRITER_TRANSITION_SEMANTICS_DIFFER: app/Label_Match.py" in reverse.stderr
    assert all(path.read_bytes() == raw for path, raw in before.items())


@pytest.mark.parametrize("relative,reason", [
    ("app/Label_Match.py", "WRITER_TRANSITION_SEMANTICS_DIFFER"),
    ("app/label_admin_pin.py", "WRITER_TRANSITION_SOURCE_SET_DIFFERS"),
    ("app/writer_session_fence.py", "WRITER_TRANSITION_SEMANTICS_DIFFER"),
    ("tools/label_writer_fence.ps1", "WRITER_TRANSITION_SEMANTICS_DIFFER"),
    ("app/label_worker_roster.py", "WRITER_TRANSITION_SOURCE_SET_DIFFERS"),
    ("roster-missing", "WRITER_TRANSITION_SOURCE_SET_DIFFERS"),
])
def test_roster_baseline_refuses_unreviewed_bytes(tmp_path, roster_baseline_packages, relative, reason):
    old, candidate = roster_baseline_packages
    name = "app/label_worker_roster.py" if relative == "roster-missing" else relative
    source = _tree(candidate, tmp_path / "candidate", [name])
    if relative == "roster-missing":
        (source / name).unlink()
    else:
        with (source / name).open("ab") as stream:
            stream.write(b"\n$UnreviewedRuntimeChange = $true\n" if name.endswith(".ps1") else CHANGED_LINE.encode())
    _pin(source)
    result = _ps(tmp_path, _definitions() + f"\nAssert-WriterTransition {_quote(source)} {_quote(old)}", _environment(tmp_path))
    assert result.returncode != 0, result.stdout
    assert reason in result.stderr, result.stdout + result.stderr
