from pathlib import Path
import re
import shutil

import pytest

from tools import build_portable_release_candidate as builder

ROOT = Path(__file__).resolve().parents[1]
SOURCES = {
    "INSTALL_THIS_PC.ps1": "INSTALL_THIS_PC.ps1",
    "INSTALL_CANONICAL_PORTABLE.ps1": "INSTALL_CANONICAL_PORTABLE.ps1",
    "tools/bootstrap_integrity.ps1": "tools/bootstrap_integrity.ps1",
    "tools/failure_redaction.ps1": "tools/failure_redaction.ps1",
    "tools/failure_redaction_rules.json": "tools/failure_redaction_rules.json",
    "app/kmtech_shared.manifest.json": "kmtech_shared.manifest.json",
}


def packet(tmp_path):
    for target, source in SOURCES.items():
        (tmp_path / target).parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(ROOT / source, tmp_path / target)
    return tmp_path


def test_packet_file_pins_match_committed_bytes(tmp_path):
    builder._assert_packet_file_pins(packet(tmp_path))


@pytest.mark.parametrize("name, holder, pattern, target", builder.PACKET_FILE_PINS)
def test_build_rejects_packet_with_wrong_file_pin(tmp_path, name, holder, pattern, target):
    root = packet(tmp_path)
    text = (root / holder).read_text(encoding="utf-8-sig")
    pin = re.search(pattern, text)
    (root / holder).write_text(text[:pin.start(1)] + "0" * 64 + text[pin.end(1):], encoding="utf-8", newline="")
    with pytest.raises(builder.PortableBuildError) as raised:
        builder._assert_packet_file_pins(root)
    assert str(raised.value).endswith(": " + name)
