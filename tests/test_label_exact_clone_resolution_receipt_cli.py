import json
from pathlib import Path

import pytest

from label_exact_clone_resolution import (
    ExactCloneResolutionError,
    write_new_json,
)
from tools import label_exact_clone_resolution_receipt as receipt_cli


@pytest.fixture(autouse=True)
def _stub_live_receipt_revalidation(monkeypatch):
    monkeypatch.setattr(
        receipt_cli,
        "validate_resolution_receipt",
        lambda *_args, **_kwargs: {"status": "RESOLVED"},
    )


def test_write_new_json_publishes_exclusively(tmp_path):
    output = tmp_path / "evidence.json"
    write_new_json(output, {"status": "FIRST"})
    first_bytes = output.read_bytes()

    with pytest.raises(ExactCloneResolutionError, match="refusing to overwrite"):
        write_new_json(output, {"status": "SECOND"})

    assert output.read_bytes() == first_bytes
    assert json.loads(first_bytes) == {"status": "FIRST"}
    assert list(tmp_path.glob(".*.tmp")) == []


def _rebind_arguments(
    tmp_path: Path,
    *,
    output: Path | None = None,
    evidence_output: Path | None = None,
    portable_root: Path | None = None,
) -> list[str]:
    return [
        "rebind",
        "--client-db",
        str(tmp_path / "client.sqlite3"),
        "--server-db",
        str(tmp_path / "server.sqlite3"),
        "--identity",
        str(tmp_path / "producer-identity.json"),
        "--credential",
        str(tmp_path / "credential.json"),
        "--stop-marker",
        str(tmp_path / "stop-marker.json"),
        "--portable-root",
        str(portable_root if portable_root is not None else tmp_path / "portable"),
        "--output",
        str(output if output is not None else tmp_path / "receipt.json"),
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
        "--expected-successor-inventory-sha256",
        "7" * 64,
        "--expected-successor-inventory-file-count",
        "11",
        "--expected-successor-inventory-byte-count",
        "12345",
        "--expected-changed-paths-sha256",
        "8" * 64,
        "--rebind-evidence-output",
        str(
            evidence_output
            if evidence_output is not None
            else tmp_path / "rebind-evidence.json"
        ),
    ]


def test_rebind_cli_publishes_lineage_evidence_before_receipt(
    monkeypatch, tmp_path
):
    events = []
    forwarded = {}
    validations = []
    pinned_reads = []
    real_write = receipt_cli.write_new_json
    real_read = receipt_cli.read_pinned_json

    def fake_rebind(**kwargs):
        forwarded.update(kwargs)
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

    def recording_validation(payload, **kwargs):
        validations.append((payload, kwargs))
        return {"status": "RESOLVED"}

    def recording_read(path, expected_sha256, *, label):
        pinned_reads.append((path, expected_sha256, label))
        return real_read(path, expected_sha256, label=label)

    monkeypatch.setattr(receipt_cli, "create_portable_successor_receipt", fake_rebind)
    monkeypatch.setattr(receipt_cli, "write_new_json", recording_write)
    monkeypatch.setattr(receipt_cli, "read_pinned_json", recording_read)
    monkeypatch.setattr(
        receipt_cli,
        "validate_resolution_receipt",
        recording_validation,
    )

    assert receipt_cli.main(_rebind_arguments(tmp_path)) == 0
    assert events == ["rebind-evidence.json", "receipt.json"]
    assert len(validations) == 3
    assert all(
        call[1]["portable_root"] == (tmp_path / "portable").resolve()
        for call in validations
    )
    assert forwarded == {
        "preimage_path": tmp_path / "preimage.json",
        "preimage_sha256": "1" * 64,
        "predecessor_receipt_path": tmp_path / "predecessor.json",
        "predecessor_receipt_sha256": "2" * 64,
        "repo_root": tmp_path / "repo",
        "expected_successor_commit": "3" * 40,
        "expected_successor_tree": "4" * 40,
        "expected_successor_manifest_sha256": "5" * 64,
        "expected_successor_installer_sha256": "6" * 64,
        "expected_successor_inventory_sha256": "7" * 64,
        "expected_successor_inventory_file_count": 11,
        "expected_successor_inventory_byte_count": 12345,
        "expected_changed_paths_sha256": "8" * 64,
        "client_db_path": tmp_path / "client.sqlite3",
        "server_db_path": tmp_path / "server.sqlite3",
        "identity_path": tmp_path / "producer-identity.json",
        "credential_path": tmp_path / "credential.json",
        "stop_marker_path": tmp_path / "stop-marker.json",
        "portable_root": tmp_path / "portable",
    }
    evidence = json.loads((tmp_path / "rebind-evidence.json").read_text())
    published_receipt = json.loads((tmp_path / "receipt.json").read_text())
    assert pinned_reads == [
        (
            (tmp_path / "receipt.json").resolve(),
            evidence["successor_receipt"]["sha256"],
            "published portable successor receipt",
        )
    ]
    assert validations[-1][0] == published_receipt
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


@pytest.mark.parametrize(
    ("receipt_relative", "evidence_relative"),
    [
        ("receipt.json", "receipt.json/rebind-evidence.json"),
        ("rebind-evidence.json/receipt.json", "rebind-evidence.json"),
    ],
)
def test_rebind_cli_rejects_ancestor_descendant_output_paths(
    monkeypatch,
    tmp_path,
    capsys,
    receipt_relative,
    evidence_relative,
):
    def unexpected_rebind(**_kwargs):
        pytest.fail("rebind validation must not run for nested output paths")

    monkeypatch.setattr(
        receipt_cli,
        "create_portable_successor_receipt",
        unexpected_rebind,
    )

    assert (
        receipt_cli.main(
            _rebind_arguments(
                tmp_path,
                output=tmp_path / receipt_relative,
                evidence_output=tmp_path / evidence_relative,
            )
        )
        == 4
    )
    assert "ancestor/descendant relationship" in capsys.readouterr().err
    assert not (tmp_path / receipt_relative).exists()
    assert not (tmp_path / evidence_relative).exists()


@pytest.mark.parametrize(
    ("selected_output", "relative_path"),
    [
        ("receipt", "."),
        ("receipt", "receipts/receipt.json"),
        ("evidence", "."),
        ("evidence", "evidence/rebind.json"),
    ],
)
def test_rebind_cli_rejects_outputs_at_or_below_portable_root_before_scan(
    monkeypatch,
    tmp_path,
    capsys,
    selected_output,
    relative_path,
):
    portable_root = tmp_path / "portable"
    selected_path = portable_root / relative_path
    output = selected_path if selected_output == "receipt" else tmp_path / "receipt.json"
    evidence_output = (
        selected_path
        if selected_output == "evidence"
        else tmp_path / "rebind-evidence.json"
    )

    def unexpected_rebind(**_kwargs):
        pytest.fail("portable inventory scan must not run for an in-tree output")

    def unexpected_write(_path, _value):
        pytest.fail("publication must not run for an in-tree output")

    monkeypatch.setattr(
        receipt_cli,
        "create_portable_successor_receipt",
        unexpected_rebind,
    )
    monkeypatch.setattr(receipt_cli, "write_new_json", unexpected_write)

    assert (
        receipt_cli.main(
            _rebind_arguments(
                tmp_path,
                output=output,
                evidence_output=evidence_output,
                portable_root=portable_root,
            )
        )
        == 4
    )
    assert "must be outside the resolved portable root" in capsys.readouterr().err


def test_rebind_cli_receipt_collision_preserves_evidence_and_competing_receipt(
    monkeypatch,
    tmp_path,
    capsys,
):
    real_write = receipt_cli.write_new_json
    competing_receipt = b"competing receipt\n"

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

    def collide_on_receipt(path, value):
        selected = Path(path)
        if selected.name == "receipt.json":
            selected.write_bytes(competing_receipt)
        return real_write(path, value)

    monkeypatch.setattr(receipt_cli, "create_portable_successor_receipt", fake_rebind)
    monkeypatch.setattr(receipt_cli, "write_new_json", collide_on_receipt)

    assert receipt_cli.main(_rebind_arguments(tmp_path)) == 4
    stderr = capsys.readouterr().err
    assert "preserved the rebind evidence and any receipt path state" in stderr
    assert "blocked/indeterminate" in stderr
    evidence = json.loads((tmp_path / "rebind-evidence.json").read_text())
    assert evidence["status"] == "PASS"
    assert (tmp_path / "receipt.json").read_bytes() == competing_receipt


def test_rebind_cli_raw_receipt_publication_error_preserves_evidence(
    monkeypatch,
    tmp_path,
    capsys,
):
    real_write = receipt_cli.write_new_json
    output = tmp_path / "blocked-parent" / "receipt.json"
    blocking_parent = output.parent
    blocking_bytes = b"not a directory\n"

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

    def block_receipt_parent(path, value):
        if Path(path) == output:
            blocking_parent.write_bytes(blocking_bytes)
        return real_write(path, value)

    monkeypatch.setattr(receipt_cli, "create_portable_successor_receipt", fake_rebind)
    monkeypatch.setattr(receipt_cli, "write_new_json", block_receipt_parent)

    assert (
        receipt_cli.main(_rebind_arguments(tmp_path, output=output))
        == 4
    )
    stderr = capsys.readouterr().err
    assert "preserved the rebind evidence and any receipt path state" in stderr
    assert "blocked/indeterminate" in stderr
    evidence = json.loads((tmp_path / "rebind-evidence.json").read_text())
    assert evidence["status"] == "PASS"
    assert blocking_parent.read_bytes() == blocking_bytes


def test_rebind_cli_post_publication_revalidation_preserves_both_outputs(
    monkeypatch,
    tmp_path,
    capsys,
):
    validation_count = 0

    def fake_rebind(**_kwargs):
        return (
            {
                "schema_version": "label-match-exact-clone-resolution-v2",
                "status": "RESOLVED",
            },
            {
                "schema_version": "label-match-portable-successor-rebind-v1",
                "status": "PASS",
            },
        )

    def changing_validation(*_args, **_kwargs):
        nonlocal validation_count
        validation_count += 1
        if validation_count == 3:
            raise ExactCloneResolutionError("portable changed after publication")
        return {"status": "RESOLVED"}

    monkeypatch.setattr(receipt_cli, "create_portable_successor_receipt", fake_rebind)
    monkeypatch.setattr(
        receipt_cli,
        "validate_resolution_receipt",
        changing_validation,
    )

    assert receipt_cli.main(_rebind_arguments(tmp_path)) == 4
    evidence = json.loads((tmp_path / "rebind-evidence.json").read_text())
    receipt = json.loads((tmp_path / "receipt.json").read_text())
    assert evidence["status"] == "PASS"
    assert receipt["status"] == "RESOLVED"
    stderr = capsys.readouterr().err
    assert "preserved the rebind evidence and any receipt path state" in stderr
    assert "blocked/indeterminate" in stderr


def test_rebind_cli_pinned_receipt_read_failure_preserves_both_outputs(
    monkeypatch,
    tmp_path,
    capsys,
):
    def fake_rebind(**_kwargs):
        return (
            {
                "schema_version": "label-match-exact-clone-resolution-v2",
                "status": "RESOLVED",
            },
            {
                "schema_version": "label-match-portable-successor-rebind-v1",
                "status": "PASS",
            },
        )

    def fail_pinned_read(*_args, **_kwargs):
        raise ExactCloneResolutionError("published receipt SHA-256 differs")

    monkeypatch.setattr(receipt_cli, "create_portable_successor_receipt", fake_rebind)
    monkeypatch.setattr(receipt_cli, "read_pinned_json", fail_pinned_read)

    assert receipt_cli.main(_rebind_arguments(tmp_path)) == 4
    evidence = json.loads((tmp_path / "rebind-evidence.json").read_text())
    receipt = json.loads((tmp_path / "receipt.json").read_text())
    assert evidence["status"] == "PASS"
    assert receipt["status"] == "RESOLVED"
    stderr = capsys.readouterr().err
    assert "preserved the rebind evidence and any receipt path state" in stderr
    assert "published receipt SHA-256 differs" in stderr


def test_rebind_cli_resolves_and_reuses_canonical_output_paths_once(
    monkeypatch,
    tmp_path,
    capsys,
):
    raw_portable = tmp_path / "portable-parent" / ".." / "portable"
    raw_receipt = tmp_path / "receipt-parent" / ".." / "receipt.json"
    raw_evidence = tmp_path / "evidence-parent" / ".." / "rebind-evidence.json"
    canonical_portable = raw_portable.resolve(strict=False)
    canonical_receipt = raw_receipt.resolve(strict=False)
    canonical_evidence = raw_evidence.resolve(strict=False)
    original_resolve = Path.resolve
    original_exists = Path.exists
    resolve_calls = []
    exists_calls = []
    write_calls = []
    pinned_reads = []
    forwarded = {}

    def recording_resolve(path, strict=False):
        resolve_calls.append(path)
        return original_resolve(path, strict=strict)

    def recording_exists(path):
        if path in {canonical_receipt, canonical_evidence}:
            exists_calls.append(path)
            return False
        return original_exists(path)

    def fake_rebind(**kwargs):
        forwarded.update(kwargs)
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
        write_calls.append((path, value))
        return path

    def recording_read(path, expected_sha256, *, label):
        pinned_reads.append((path, expected_sha256, label))
        return write_calls[-1][1]

    monkeypatch.setattr(Path, "resolve", recording_resolve)
    monkeypatch.setattr(Path, "exists", recording_exists)
    monkeypatch.setattr(receipt_cli, "create_portable_successor_receipt", fake_rebind)
    monkeypatch.setattr(receipt_cli, "write_new_json", recording_write)
    monkeypatch.setattr(receipt_cli, "read_pinned_json", recording_read)

    assert (
        receipt_cli.main(
            _rebind_arguments(
                tmp_path,
                output=raw_receipt,
                evidence_output=raw_evidence,
                portable_root=raw_portable,
            )
        )
        == 0
    )
    assert resolve_calls == [raw_portable, raw_receipt, raw_evidence]
    assert exists_calls == [canonical_receipt, canonical_evidence]
    assert [path for path, _value in write_calls] == [
        canonical_evidence,
        canonical_receipt,
    ]
    assert pinned_reads == [
        (
            canonical_receipt,
            write_calls[0][1]["successor_receipt"]["sha256"],
            "published portable successor receipt",
        )
    ]
    assert forwarded["portable_root"] == canonical_portable
    assert write_calls[0][1]["successor_receipt"]["path"] == str(
        canonical_receipt
    )
    summary = json.loads(capsys.readouterr().out)
    assert summary["output"] == str(canonical_receipt)
    assert summary["rebind_evidence"] == str(canonical_evidence)
