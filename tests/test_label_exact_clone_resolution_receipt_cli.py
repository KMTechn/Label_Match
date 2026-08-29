import json
from pathlib import Path

import pytest

from label_exact_clone_resolution import (
    ExactCloneResolutionError,
    write_new_json,
)
from tools import label_exact_clone_resolution_receipt as receipt_cli


def test_write_new_json_publishes_exclusively(tmp_path):
    output = tmp_path / "evidence.json"
    write_new_json(output, {"status": "FIRST"})
    first_bytes = output.read_bytes()

    with pytest.raises(ExactCloneResolutionError, match="refusing to overwrite"):
        write_new_json(output, {"status": "SECOND"})

    assert output.read_bytes() == first_bytes
    assert json.loads(first_bytes) == {"status": "FIRST"}
    assert list(tmp_path.glob(".*.tmp")) == []


def _rebind_arguments(tmp_path: Path) -> list[str]:
    return [
        "rebind",
        "--server-db",
        str(tmp_path / "server.sqlite3"),
        "--portable-root",
        str(tmp_path / "portable"),
        "--output",
        str(tmp_path / "receipt.json"),
        "--preimage",
        str(tmp_path / "preimage.json"),
        "--preimage-sha256",
        "1" * 64,
        "--predecessor-receipt",
        str(tmp_path / "predecessor.json"),
        "--predecessor-receipt-sha256",
        "2" * 64,
        "--repo-root",
        str(tmp_path / "repo"),
        "--expected-successor-commit",
        "3" * 40,
        "--expected-successor-tree",
        "4" * 40,
        "--expected-successor-manifest-sha256",
        "5" * 64,
        "--expected-successor-installer-sha256",
        "6" * 64,
        "--expected-changed-paths-sha256",
        "7" * 64,
        "--rebind-evidence-output",
        str(tmp_path / "rebind-evidence.json"),
    ]


def test_rebind_cli_publishes_lineage_evidence_before_receipt(
    monkeypatch, tmp_path
):
    events = []
    real_write = receipt_cli.write_new_json

    def fake_rebind(**_kwargs):
        return (
            {
                "schema_version": "label-match-exact-clone-resolution-v1",
                "status": "RESOLVED",
            },
            {
                "schema_version": "label-match-portable-successor-rebind-v1",
                "status": "PASS",
            },
        )

    def recording_write(path, value):
        events.append(Path(path).name)
        return real_write(path, value)

    monkeypatch.setattr(receipt_cli, "create_portable_successor_receipt", fake_rebind)
    monkeypatch.setattr(receipt_cli, "write_new_json", recording_write)

    assert receipt_cli.main(_rebind_arguments(tmp_path)) == 0
    assert events == ["rebind-evidence.json", "receipt.json"]
    evidence = json.loads((tmp_path / "rebind-evidence.json").read_text())
    assert evidence["successor_receipt"]["path"] == str(
        (tmp_path / "receipt.json").resolve()
    )
    assert (tmp_path / "receipt.json").is_file()


def test_rebind_cli_evidence_collision_leaves_no_consumable_receipt(
    monkeypatch, tmp_path
):
    evidence_output = tmp_path / "rebind-evidence.json"
    evidence_output.mkdir()

    def fake_rebind(**_kwargs):
        return (
            {
                "schema_version": "label-match-exact-clone-resolution-v1",
                "status": "RESOLVED",
            },
            {
                "schema_version": "label-match-portable-successor-rebind-v1",
                "status": "PASS",
            },
        )

    monkeypatch.setattr(receipt_cli, "create_portable_successor_receipt", fake_rebind)

    assert receipt_cli.main(_rebind_arguments(tmp_path)) == 4
    assert not (tmp_path / "receipt.json").exists()
