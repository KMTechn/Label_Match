"""Transport changes admit exact bytes only within existing release directions."""
import ast
import hashlib
import re
import subprocess

import pytest

from tests.test_legacy_label_upgrade_guard import (
    CHANGED_LINE, INSTALLER, ROOT, _nextfix_probe, _pin, _release_digest, _tree,
    nextfix_archive_layouts,
)

REVISIONS = ('3f4574a', 'a20cd01', 'e88011c', 'c83c72c', '817d393', '92abdaa', '614369b')
MODULES = ('direct_sync_push.py', 'label_admin_pin.py', 'package_logistics.py',
    'package_outbox_processor.py', 'phs_label_workflow.py', 'producer_runtime_client.py',
    'sealed_transfer_exchange.py', 'user_relay.py', 'writer_session_fence.py')


def test_remote_transport_pairs_cover_only_existing_releases_and_this_tree():
    source = INSTALLER.read_text(encoding='utf-8')
    start = source.index("$probe = @'") + len("$probe = @'")
    tree = ast.parse(source[start:source.index("\n'@", start)].lstrip())
    pairs = next(ast.literal_eval(node.value) for node in tree.body
        if isinstance(node, ast.Assign) and isinstance(node.targets[0], ast.Name)
        and node.targets[0].id == 'remote_transport_replacements')
    assert set(pairs) == {'app/' + name for name in MODULES}
    for relative, (befores, after) in pairs.items():
        name = relative.removeprefix('app/')
        expected = {_release_digest(subprocess.check_output(
            ['git', '-C', str(ROOT), 'show', revision + ':' + name])) for revision in REVISIONS}
        assert befores == expected
        assert after == _release_digest((ROOT / name).read_bytes())
        assert after not in befores


@pytest.mark.parametrize('side', ['candidate', 'installed'])
@pytest.mark.parametrize('name', MODULES)
def test_transport_guard_refuses_changed_bytes_on_either_side(tmp_path, nextfix_archive_layouts, side, name):
    old, candidate = nextfix_archive_layouts
    changed = _tree(candidate if side == 'candidate' else old, tmp_path / 'changed', ['app/' + name])
    with (changed / 'app' / name).open('a', encoding='utf-8') as stream:
        stream.write(CHANGED_LINE)
    _pin(changed)
    source = changed if side == 'candidate' else candidate
    installed = changed if side == 'installed' else old
    before = {(tree, path.relative_to(tree)): hashlib.sha256(path.read_bytes()).digest()
              for tree in (source, installed) for path in tree.rglob('*') if path.is_file()}
    result = _nextfix_probe(tmp_path, source, installed)
    assert result.returncode == 1
    assert {(tree, path.relative_to(tree)): hashlib.sha256(path.read_bytes()).digest()
            for tree in (source, installed) for path in tree.rglob('*') if path.is_file()} == before
    # A changed signed-transport pair cannot authorize the roster's exact
    # post -> _post_path writer identity mapping; identity validation now
    # rejects either side before the per-file semantics comparison.
    # F5 retirement also needs the exact logistics/workflow pair. A mutation
    # on either side invalidates that source-identity adapter, so these two
    # modules now fail the same identity check before ordinary AST comparison.
    # The quiesced runtime adapter likewise requires the exact client pair.
    reason = ('WRITER_TRANSITION_SOURCE_SET_DIFFERS' if name in {'label_admin_pin.py', 'package_logistics.py', 'phs_label_workflow.py', 'producer_runtime_client.py'}
              else 'WRITER_TRANSITION_SEMANTICS_DIFFER')
    assert set(re.findall(r'\bWRITER_TRANSITION_[A-Z_]+\b', result.stderr)) == {reason}
