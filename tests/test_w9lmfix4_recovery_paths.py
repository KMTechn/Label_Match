"""Real Windows link probes for package recovery and F5 output."""

import os
import subprocess

import pytest

import label_safe_path
from label_data_manager import read_recovery_file
from phs_label_workflow import PHSLabelRenderer
from label_recovery_schema import valid_recovery_archive_path, valid_recovery_rendered_path
from tests.test_w9nb_label_recovery import _recovery_app, _storage_recovery_app


PHS2 = ("PHS=2|SRC=KMTECH_INPUT_TAG|ITG=ITG-PATH|CLC=AAA2270730100|"
        "LBL=LBL-PATH|HSH=0123456789abcdef")
VERSION = "label-match-phs-label-exchange-v1"


def _junction(path, target):
    result = subprocess.run(["cmd", "/c", "mklink", "/J", str(path), str(target)],
                            capture_output=True, text=True)
    assert result.returncode == 0, result.stderr
    assert path.is_junction()


def test_current_hardlink_is_quarantined_as_entry_and_other_work_can_continue(
        tmp_path, monkeypatch):
    outside = tmp_path.parent / (tmp_path.name + "-outside")
    outside.mkdir()
    raw = b'{"current_set_info":{"id":"SET-LINK","raw":[]}}'
    original = outside / "current.json"
    original.write_bytes(raw)
    app, _journal, current = _storage_recovery_app(tmp_path, monkeypatch)
    os.link(original, current)
    reads = []
    actual_open = label_safe_path.msvcrt.open_osfhandle
    monkeypatch.setattr(label_safe_path.msvcrt, "open_osfhandle",
                        lambda *args: (reads.append(args), actual_open(*args))[1])
    evidence = read_recovery_file(current, current_state=True)
    assert evidence["reason"] == "ValueError" and evidence["raw"] is None
    assert reads == []
    app._load_current_set_state()
    assert app._workflow_blocking_notice is not None
    hold_id = "CURRENT:" + evidence["sha256"]
    assert hold_id in {row["set_id"] for row in app._package_recovery_candidates()}
    assert app._hold_package_recovery_set(hold_id, manager_code="admin") is True
    archive = current.with_name(current.name + ".held-" + evidence["sha256"])
    assert not current.exists() and archive.exists()
    assert archive.stat().st_nlink == 2 and original.read_bytes() == raw
    assert app.package_outbox.get_workbench_hold(hold_id) is not None
    assert app.package_outbox.has_workbench_audit(
        set_id=hold_id, action="FILE_UNVERIFIED", observed="ValueError"
    )
    assert app.__dict__.get("_workflow_blocking_notice") is None


def test_f5_hardlink_is_held_without_reading_outside_and_next_journal_runs(
        tmp_path, monkeypatch):
    outside = tmp_path.parent / (tmp_path.name + "-outside")
    outside.mkdir()
    app, journal = _recovery_app(tmp_path, monkeypatch, {
        "status": "PREPARE_PENDING", "scan_payload": PHS2,
        "prepare_idempotency_key": "KEY-PATH",
    })
    raw = journal.path.read_bytes()
    original = outside / "journal.json"
    os.replace(journal.path, original)
    os.link(original, journal.path)
    evidence = read_recovery_file(journal.path, journal_schema="label-match-phs-label-exchange-v1")
    assert evidence["raw"] is None and evidence["reason"] == "ValueError"
    hold_id = evidence["sha256"]
    assert "F5:" + hold_id in {row["set_id"] for row in app._package_recovery_candidates()}
    assert app._hold_label_recovery(hold_id, manager_code="admin") is True
    archive = journal.path.with_name(journal.path.name + ".held-" + hold_id)
    assert not journal.path.exists() and archive.stat().st_nlink == 2
    assert original.read_bytes() == raw
    assert app.package_outbox.get_workbench_hold("F5:" + hold_id) is not None
    assert app.package_outbox.has_workbench_audit(
        set_id="F5:" + hold_id, action="FILE_UNVERIFIED", observed="ValueError"
    )
    held_row = next(row for row in app._package_recovery_candidates()
                    if row["set_id"] == "F5:" + hold_id)
    assert held_row["unverified"] == "ValueError"
    assert "파일 항목" in app._recheck_package_recovery_set(
        "F5:" + hold_id, manager_code="admin"
    )
    assert "다른 F5" in app._recheck_package_recovery_physical(
        "F5:" + hold_id, PHS2, manager_code="admin"
    )
    assert app._label_recovery_source_is_held(PHS2) is False
    journal.save({"status": "PREPARE_PENDING", "scan_payload": PHS2,
                  "prepare_idempotency_key": "KEY-NEXT"})
    assert journal.load()["prepare_idempotency_key"] == "KEY-NEXT"


def test_f5_archive_hardlink_is_reported_unverified_without_outside_read(
        tmp_path, monkeypatch):
    outside = tmp_path.parent / (tmp_path.name + "-outside")
    outside.mkdir()
    app, journal = _recovery_app(tmp_path, monkeypatch, {
        "status": "PREPARED", "scan_payload": PHS2,
        "prepare_idempotency_key": "KEY-ARCHIVE", "exchange_id": "EX-ARCHIVE",
    })
    digest = app._label_recovery_hold_id()
    assert app._hold_label_recovery(digest, manager_code="admin") is True
    archive = journal.path.with_name(journal.path.name + ".held-" + digest)
    raw = archive.read_bytes()
    archive.unlink()
    original = outside / "archive.json"
    original.write_bytes(raw)
    os.link(original, archive)
    assert app._finalize_label_recovery_holds() is True
    assert app._package_recovery_file_issues["F5:" + digest]
    assert original.read_bytes() == raw and archive.stat().st_nlink == 2


def test_junction_parent_blocks_recovery_read_and_png_write(tmp_path, monkeypatch):
    outside = tmp_path.parent / (tmp_path.name + "-outside")
    outside.mkdir()
    raw = b'{"current_set_info":{"id":"SET-A","raw":[]}}'
    (outside / "current.json").write_bytes(raw)
    junction = tmp_path / "linked-parent"
    _junction(junction, outside)
    try:
        evidence = read_recovery_file(junction / "current.json", current_state=True)
        assert evidence["raw"] is None and evidence["reason"] == "ValueError"
    finally:
        os.rmdir(junction)

    output_root = tmp_path / "output"
    folder = output_root / "2026-09-23"
    folder.mkdir(parents=True)
    external_pngs = outside / "pngs"
    external_pngs.mkdir()
    junction = folder / "phs_label_exchange"
    _junction(junction, external_pngs)
    try:
        from kmtech_shared.raster import RasterImage
        import phs_label_workflow

        class Canvas:
            def __init__(self, *_args, **_kwargs): pass
            def __enter__(self): return self
            def __exit__(self, *_args): return False
            def rectangle(self, *_args, **_kwargs): pass
            def qr(self, *_args, **_kwargs): pass
            def text(self, *_args, **_kwargs): pass
            def snapshot(self): return RasterImage.solid(1, 1)

        monkeypatch.setattr(phs_label_workflow, "RasterCanvas", Canvas)
        with pytest.raises(phs_label_workflow.PHSPhysicalPrintError):
            PHSLabelRenderer(output_root).render({}, {
                "label_id": "LBL-PATH", "qr_payload": PHS2,
                "business_date": "2026-09-23", "worker_code": "WORKER",
            })
        assert list(external_pngs.iterdir()) == []
    finally:
        os.rmdir(junction)


def test_checked_write_and_replace_reject_dangling_final_link(tmp_path):
    missing = tmp_path.parent / (tmp_path.name + "-outside") / "missing.png"
    target = tmp_path / "label.png"
    target.symlink_to(missing)
    source = tmp_path / "source.png"
    source.write_bytes(b"source")
    try:
        with pytest.raises(ValueError):
            label_safe_path.write_checked_bytes(
                target, b"new", allowed_root=tmp_path
            )
        with pytest.raises(ValueError):
            label_safe_path.replace_checked(
                source, target, allowed_root=tmp_path
            )
        assert target.is_symlink() and not missing.exists()
        assert source.read_bytes() == b"source"
    finally:
        target.unlink()


@pytest.mark.parametrize("slot", ["current", "journal", "archive", "png"])
@pytest.mark.parametrize("entry_kind", ["inside_symlink", "outside_symlink",
                                        "inside_hardlink", "outside_hardlink",
                                        "directory", "parent_junction"])
def test_recovery_and_output_slots_reject_untrusted_entries(
        tmp_path, slot, entry_kind):
    outside = tmp_path.parent / (tmp_path.name + "-outside")
    outside.mkdir()
    inside = tmp_path / "inside"
    inside.mkdir()
    names = {"current": "current.json", "journal": "journal.json",
             "archive": "journal.json.held-" + "a" * 64, "png": "LBL-PATH.png"}
    folder = tmp_path / ("2026-09-23/phs_label_exchange" if slot == "png" else "recovery")
    folder.mkdir(parents=True)
    name = names[slot]
    if slot == "archive":
        assert valid_recovery_archive_path(str(folder / name), "a" * 64)
    if slot == "png":
        assert valid_recovery_rendered_path(str(folder / name))
    target = folder / name
    payload = (b'{"current_set_info":{"id":"SET-A","raw":[]}}'
               if slot == "current" else
               (b'{"schema_version":"' + VERSION.encode() +
                b'","state":{"status":"PREPARED","scan_payload":"' +
                PHS2.encode() + b'"}}') if slot != "png" else b"sentinel")
    entry = target
    if entry_kind == "parent_junction":
        os.rmdir(folder)
        external_parent = outside / "directory"
        external_parent.mkdir()
        (external_parent / name).write_bytes(payload)
        _junction(folder, external_parent)
        original = external_parent / name
    elif entry_kind == "directory":
        entry.mkdir()
        original = None
    else:
        original = ((inside if entry_kind.startswith("inside") else outside)
                    / (slot + "-original"))
        original.write_bytes(payload)
        if entry_kind.endswith("symlink"):
            entry.symlink_to(original)
        else:
            os.link(original, entry)
    try:
        if slot == "png":
            with pytest.raises((ValueError, OSError)):
                label_safe_path.write_checked_bytes(
                    entry, b"new-png", allowed_root=tmp_path
                )
        else:
            evidence = read_recovery_file(
                entry, current_state=slot == "current",
                journal_schema=VERSION if slot != "current" else None,
            )
            assert evidence["raw"] is None and not evidence["verified"]
            assert evidence["reason"] != "missing"
        if original is not None:
            assert original.read_bytes() == payload
    finally:
        if entry_kind == "parent_junction":
            os.rmdir(folder)
        elif entry_kind.endswith("symlink"):
            entry.unlink()


@pytest.mark.parametrize("slot", ["current", "journal", "archive"])
@pytest.mark.parametrize("raw", [pytest.param(b"", id="zero"),
                                 pytest.param(b"\x00" * (8 * 1024 * 1024 + 1),
                                              id="oversized")])
def test_recovery_slots_keep_zero_and_oversized_bytes_unverified(tmp_path, slot, raw):
    name = "current.json" if slot == "current" else "journal.json"
    if slot == "archive":
        name += ".held-" + "a" * 64
    path = tmp_path / name
    path.write_bytes(raw)
    evidence = read_recovery_file(
        path, current_state=slot == "current",
        journal_schema=VERSION if slot != "current" else None,
    )
    assert evidence["raw"] == raw and not evidence["verified"]
    assert evidence["reason"] == ("oversized" if raw else "JSONDecodeError")
