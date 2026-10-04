"""New maintenance guards have exact forward bytes, including after re-pinning."""
import ast
import hashlib
import subprocess

import pytest

from tests.test_data_generation_upgrade_pair import ROOT, baseline_source, pair, probe
from tests.test_writer_transition import _pin


def test_quiesced_transport_declaration_has_only_the_reviewed_preimages():
    text = (ROOT / "INSTALL_CANONICAL_PORTABLE.ps1").read_text(encoding="utf-8-sig")
    start = text.index("$probe = @'") + len("$probe = @'")
    tree = ast.parse(text[start:text.index("\n'@", start)])
    declarations = {
        node.targets[0].id: ast.literal_eval(node.value)
        for node in tree.body if isinstance(node, ast.Assign)
        and isinstance(node.targets[0], ast.Name)
        and node.targets[0].id in {"quiesced_transport_replacements", "quiesced_transport_sources"}
    }
    paths = {"app/producer_runtime_client.py", "app/tools/register_label_match_worker_pc.py"}
    assert set(declarations["quiesced_transport_replacements"]) == paths
    for relative, (befores, after) in declarations["quiesced_transport_replacements"].items():
        local = relative.removeprefix("app/")
        expected = {
            hashlib.sha256(subprocess.check_output(
                ["git", "-C", str(ROOT), "show", revision + ":" + local]
            ).replace(b"\r\n", b"\n")).hexdigest()
            for revision in ("3f4574a", "e89c188", "ac745f0")
        }
        assert befores == expected
        assert after == hashlib.sha256((ROOT / local).read_bytes()).hexdigest()
        assert after not in befores
    assert len(declarations["quiesced_transport_sources"]) == 4
    assert {row[0] for row in declarations["quiesced_transport_sources"]} == {
        "runtime_lease_network", "worker_enrollment_network",
        "worker_partial_recovery_network", "worker_admin_recovery_network",
    }


@pytest.mark.parametrize("relative", [
    "app/producer_runtime_client.py", "app/tools/register_label_match_worker_pc.py",
])
@pytest.mark.parametrize("side", ["candidate", "installed"])
def test_transport_pair_rejects_re_pinned_unreviewed_bytes(pair, relative, side):
    old, new = pair
    target_root = new if side == "candidate" else old
    target = target_root / relative
    target.write_bytes(target.read_bytes() + b"\nUNREVIEWED_BYTE = True\n")
    _pin(target_root)
    before = target.read_bytes()
    result = probe(new, old)
    assert result.returncode != 0
    assert "WRITER_TRANSITION_" in result.stderr
    assert target.read_bytes() == before
