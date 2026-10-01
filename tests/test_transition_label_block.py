"""P3 freezes the shared UTF-8 block from CA ca6ab8fe, including its wording."""
import hashlib
from pathlib import Path


def test_transition_rules_match_committed_ca_block_byte_for_byte():
    root = Path(__file__).resolve().parents[1]
    # read_text normalizes checkout CRLF; Git's canonical source uses LF.
    source = (root / "label_transition.py").read_text(encoding="utf-8")
    start = "# ---- Transition label rules shared"
    end = "# ---- End of the shared transition label rules."
    actual = source[source.index(start):source.index(end) + len(end)].encode("utf-8")
    expected = (root / "tests/transition_label_rules_ca6ab8fe.txt").read_text(encoding="utf-8").encode("utf-8")
    assert len(expected) == 7765
    assert hashlib.sha256(expected).hexdigest() == "2a2eea19f68844d6c1e305e76113ad210260766c554c673c774efc4006bfe5b2"
    assert actual == expected
