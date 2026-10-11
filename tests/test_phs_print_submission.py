from __future__ import annotations

import copy
import ctypes
import json
import os
from pathlib import Path
from types import SimpleNamespace
import threading

import pytest

import phs_label_workflow as flow
import writer_session_fence as fence
from tests._f5_recovery_fixture import restore_saved_f5_journal
from tests.test_phs_label_workflow import _Client, _Renderer, _current_set
from tests.test_tk_serial_ui_lane import FakeTkRoot, _close


@pytest.fixture
def gdi(monkeypatch):
    device = SimpleNamespace(printer_name="TEST_LABEL_PRINTER", starts=0, validations=0)
    def validate(_paper):
        device.validations += 1
    def start(_image, spec):
        device.starts += 1
        return SimpleNamespace(default_printer_unchanged=True,
                               default_devmode_unchanged=True,
                               printer_name=device.printer_name, job_id=17,
                               document_name=spec.document_name)
    device.validate_paper, device.print_image = validate, start
    monkeypatch.setattr(flow, "GdiPrinter", lambda: device)
    monkeypatch.setattr(flow, "read_checked_bytes", lambda *_args, **_kwargs: b"png")
    monkeypatch.setattr(flow, "RasterImage", SimpleNamespace(from_png_bytes=lambda _data: object()))
    monkeypatch.setattr(flow.WindowsGDIPhysicalLabelPrinter, "_printer_profile",
                        staticmethod(lambda _name: ("USB001", "TEST_LABEL_DRIVER")))
    return device


@pytest.mark.parametrize("marker", flow._NON_LABEL_PRINTER_MARKERS)
@pytest.mark.parametrize("field", ["name", "port", "driver"])
def test_virtual_or_file_printer_never_starts_document(tmp_path, monkeypatch, gdi, marker, field):
    profile = ["USB001", "TEST_LABEL_DRIVER"]
    if field == "name":
        gdi.printer_name = marker.upper()
    else:
        profile[0 if field == "port" else 1] = marker.upper()
    monkeypatch.setattr(flow.WindowsGDIPhysicalLabelPrinter, "_printer_profile",
                        staticmethod(lambda _name: tuple(profile)))
    printer = flow.WindowsGDIPhysicalLabelPrinter(tmp_path)
    with pytest.raises(flow.PHSPhysicalPrintError) as caught:
        printer.print_png(str(tmp_path / "label.png"), document_name="label")
    assert gdi.starts == gdi.validations == 0
    assert caught.value.operator_message == (
        f"기본 프린터가 라벨 프린터가 아닙니다({gdi.printer_name}). 관리자에게 알리세요.")
    assert str(caught.value) == "기본 프린터가 라벨 프린터가 아닙니다. 관리자에게 알리세요."


def test_paper_refusal_before_document_uses_same_item_hold(tmp_path, gdi):
    def refuse(_paper):
        raise RuntimeError("paper refused")
    gdi.validate_paper = refuse
    with pytest.raises(flow.PHSPhysicalPrintError) as caught:
        flow.WindowsGDIPhysicalLabelPrinter(tmp_path).print_png(
            str(tmp_path / "label.png"), document_name="label")
    assert gdi.starts == 0
    assert gdi.printer_name in caught.value.operator_message
    assert gdi.printer_name not in str(caught.value)


def test_real_printer_submission_retains_existing_evidence(tmp_path, gdi):
    result = flow.WindowsGDIPhysicalLabelPrinter(tmp_path).print_png(
        str(tmp_path / "label.png"), document_name="label")
    assert gdi.starts == gdi.validations == 1
    assert result.spool_job_id == 17
    assert result.printer_name == gdi.printer_name


@pytest.mark.parametrize("read_ok", [True, False])
def test_native_profile_reads_driver_and_port_and_always_closes(monkeypatch, read_ok):
    port, driver = ctypes.create_unicode_buffer("USB001"), ctypes.create_unicode_buffer("TEST_DRIVER")
    info = flow._PrinterInfo2W()
    info.pPortName = ctypes.cast(port, ctypes.c_void_p).value
    info.pDriverName = ctypes.cast(driver, ctypes.c_void_p).value
    closed = []
    def open_printer(_name, handle, _defaults):
        handle._obj.value = 41
        return True
    def get_printer(_handle, level, buffer, _size, needed):
        assert level == 2
        needed._obj.value = ctypes.sizeof(info)
        if buffer is None or not read_ok:
            return False
        ctypes.memmove(buffer, ctypes.byref(info), ctypes.sizeof(info))
        return True
    spool = SimpleNamespace(OpenPrinterW=open_printer, GetPrinterW=get_printer,
                            ClosePrinter=lambda handle: closed.append(handle.value))
    monkeypatch.setattr(flow.ctypes, "WinDLL", lambda *_args, **_kwargs: spool)
    if read_ok:
        assert flow.WindowsGDIPhysicalLabelPrinter._printer_profile("label") == (port.value, driver.value)
    else:
        with pytest.raises(flow.PHSPhysicalPrintError):
            flow.WindowsGDIPhysicalLabelPrinter._printer_profile("label")
    assert closed == [41]


def _key(path):
    return os.path.normcase(os.path.abspath(path))


def test_timeout_blocks_same_key_until_late_submission_is_recorded(tmp_path, monkeypatch):
    monkeypatch.setattr(flow, "PRINT_SUBMIT_TIMEOUT_SECONDS", 0.02)
    printer = flow.WindowsGDIPhysicalLabelPrinter(tmp_path)
    gate, entered = threading.Event(), threading.Event()
    path = str(tmp_path / "label.png")
    records, submissions = [], []
    def submit(filepath, **_kwargs):
        submissions.append(filepath)
        if filepath == path:
            entered.set()
            assert gate.wait(2)
        return "submitted"
    monkeypatch.setattr(flow.WindowsGDIPhysicalLabelPrinter, "_submit_print_png",
                        lambda self, filepath, **kwargs: submit(filepath, **kwargs))
    try:
        with pytest.raises(flow.PHSPhysicalPrintError) as first:
            printer.print_png(path, document_name="label", late_record=records.append)
        assert entered.is_set() and first.value.code == "LOCAL_PRINT_UNKNOWN"
        worker = printer._submissions[_key(path)]
        assert worker.daemon and worker.is_alive()
        with pytest.raises(flow.PHSPhysicalPrintError) as duplicate:
            flow.WindowsGDIPhysicalLabelPrinter(tmp_path).print_png(path, document_name="label")
        assert duplicate.value.operator_message == (
            "이전 인쇄가 아직 프린터 대기열에 있습니다 — 프린터 대기열을 확인한 뒤 다시 누르세요")
        assert len(submissions) == 1
        assert printer.print_png(str(tmp_path / "other.png"), document_name="other") == "submitted"
        gate.set()
        worker.join(2)
        assert not worker.is_alive()
        assert len(records) == 1 and records[0]["result"] == "SUCCESS"
        assert _key(path) not in printer._submissions
        assert printer.print_png(path, document_name="label") == "submitted"
    finally:
        gate.set()
        if "worker" in locals():
            worker.join(2)


def test_unknown_keeps_lane_live_and_close_does_not_join_driver(tmp_path, monkeypatch):
    from tk_serial_ui_lane import LaneTask, TkSerialUiLane
    monkeypatch.setattr(flow, "PRINT_SUBMIT_TIMEOUT_SECONDS", 0.02)
    gate = threading.Event()
    path = str(tmp_path / "label.png")
    printer = flow.WindowsGDIPhysicalLabelPrinter(tmp_path)
    def submit(*_args, **_kwargs):
        assert gate.wait(2)
        return "submitted"
    monkeypatch.setattr(printer, "_submit_print_png", submit)
    root, results = FakeTkRoot(), []
    lane = TkSerialUiLane(root, poll_ms=1)
    def work():
        try:
            printer.print_png(path, document_name="label", late_record=lambda _record: None)
        except flow.PHSPhysicalPrintError as exc:
            return exc.code
    try:
        assert lane.submit(LaneTask("print", 1, work, results.append,
                                   lambda exc: pytest.fail(type(exc).__name__))).accepted
        root.run_until(lambda: len(results) == 1 and not lane.is_busy())
        assert results == ["LOCAL_PRINT_UNKNOWN"]
        worker = printer._submissions[_key(path)]
        assert worker.is_alive()
        assert lane.submit(LaneTask("scan", 1, lambda: "next", results.append,
                                   lambda exc: pytest.fail(type(exc).__name__))).accepted
        root.run_until(lambda: len(results) == 2 and not lane.is_busy())
        _close(root, lane)
        assert str(lane.state) == "CLOSED" and worker.is_alive()
        assert results[-1] == "next"
    finally:
        gate.set()
        if "worker" in locals():
            worker.join(2)
        if str(lane.state) != "CLOSED":
            root.run_until(lambda: not lane.is_busy())
            _close(root, lane)


def test_submission_releases_and_revalidates_outer_writer_admission(tmp_path):
    journal = flow.PHSLabelExchangeJournal(tmp_path / "exchange.json")
    observations = []
    with fence.writer_admission("phs_current_recovery"):
        lease = fence._WRITER_LOCAL.lease
        class Printer:
            def print_png(self, *_args, **_kwargs):
                observations.append((lease.owned, fence._WRITER_LOCAL.depth))
                return "submitted"
        assert flow._submit_physical_print(Printer(), "label.png", "label", journal, {}) == "submitted"
        assert lease.owned and fence._WRITER_LOCAL.depth > 0
    assert observations == [(False, 0)]


@pytest.mark.parametrize("kind", ["virtual", "paper"])
def test_nonlabel_or_paper_failure_holds_only_item_and_hides_name_in_records(tmp_path, gdi, kind):
    if kind == "virtual":
        gdi.printer_name = "TEST_FILE_PRINTER_PDF"
    else:
        def refuse(_paper):
            raise RuntimeError("paper refused")
        gdi.validate_paper = refuse
    current = _current_set()
    before = copy.deepcopy(current)
    client = _Client()
    journal = flow.PHSLabelExchangeJournal(tmp_path / "exchange.json")
    coordinator = flow.PHSLabelExchangeCoordinator(client, journal,
        _Renderer(tmp_path / "label.png"), flow.WindowsGDIPhysicalLabelPrinter(tmp_path))
    restore_saved_f5_journal(coordinator, "single")
    result = coordinator.recover_current(current, persist_current_set=lambda: True)
    assert not result.success and gdi.starts == 0
    assert current == before
    assert gdi.printer_name in result.operator_message
    assert gdi.printer_name not in result.message
    assert gdi.printer_name not in journal.path.read_text(encoding="utf-8")
    assert not any(name == "activate" for name, *_rest in client.calls)
    assert not any(name == "complete_print" and value is True
                   for name, value in client.calls)


def test_unknown_records_late_outcome_without_mutating_current_journal_or_label(tmp_path, monkeypatch):
    monkeypatch.setattr(flow, "PRINT_SUBMIT_TIMEOUT_SECONDS", 0.02)
    gate = threading.Event()
    printer = flow.WindowsGDIPhysicalLabelPrinter(tmp_path)
    def submit(*_args, **_kwargs):
        assert gate.wait(2)
        return flow.PhysicalPrintEvidence("TEST_LABEL_PRINTER", 17, "label", "test-time")
    monkeypatch.setattr(printer, "_submit_print_png", submit)
    current, client = _current_set(), _Client()
    before = copy.deepcopy(current)
    journal = flow.PHSLabelExchangeJournal(tmp_path / "exchange.json")
    coordinator = flow.PHSLabelExchangeCoordinator(client, journal,
        _Renderer(tmp_path / "label.png"), printer)
    restore_saved_f5_journal(coordinator, "single")
    try:
        result = coordinator.recover_current(current, persist_current_set=lambda: True)
        assert not result.success and result.error_code == "LOCAL_PRINT_UNKNOWN"
        assert current == before
        assert journal.load()["status"] == "LOCAL_PRINT_STARTING"
        preserved = journal.path.read_bytes()
        assert not any(name in ("activate", "complete_print") for name, *_rest in client.calls)
        worker = next(worker for worker in printer._submissions.values() if worker.is_alive())
        gate.set()
        worker.join(2)
        assert not worker.is_alive()
        assert journal.path.read_bytes() == preserved and current == before
        late = list(tmp_path.glob("exchange.json.late-print-*.json"))
        assert len(late) == 1
        recorded = flow.PHSLabelExchangeJournal(late[0]).load()
        assert recorded["status"] == "PRINT_OUTCOME_UNKNOWN"
        assert json.loads(recorded["print_error_message"])["result"] == "SUCCESS"
        assert "TEST_LABEL_PRINTER" not in late[0].read_text(encoding="utf-8")
    finally:
        gate.set()
        if "worker" in locals():
            worker.join(2)


@pytest.mark.parametrize("kind", ["SINGLE", "BATCH", "SPLIT", "MERGE"])
def test_reconciliation_unknown_holds_target_and_never_activates(tmp_path, monkeypatch, kind):
    from tests.test_phs_reconciliation_workflow import (
        _Client as BatchClient, _resolution, _current_set as batch_current, _coordinator)
    monkeypatch.setattr(flow, "PRINT_SUBMIT_TIMEOUT_SECONDS", 0.02)
    gate = threading.Event()
    printer = flow.WindowsGDIPhysicalLabelPrinter(tmp_path)
    def submit(*_args, **_kwargs):
        assert gate.wait(2)
        return flow.PhysicalPrintEvidence("TEST_LABEL_PRINTER", 17, "label", "test-time")
    monkeypatch.setattr(printer, "_submit_print_png", submit)
    resolution = _resolution(kind)
    client, current = BatchClient(resolution), batch_current(resolution)
    before = copy.deepcopy(current)
    coordinator = _coordinator(tmp_path, client, printer)
    try:
        result = coordinator.recover_reconciliation(current_set=current, persist_current_set=lambda: True)
        assert not result.success and result.error_code == "LOCAL_PRINT_UNKNOWN"
        assert current == before
        assert not any(name == "activate" for name, *_rest in client.calls)
        state = coordinator.journal.load()
        assert any(value["status"] == "LOCAL_PRINT_STARTING" for value in state["target_prints"].values())
        preserved = coordinator.journal.path.read_bytes()
        worker = next(worker for worker in printer._submissions.values() if worker.is_alive())
        gate.set()
        worker.join(2)
        assert not worker.is_alive()
        assert coordinator.journal.path.read_bytes() == preserved and current == before
        assert len(list(tmp_path.glob("*.late-print-*.json"))) == 1
    finally:
        gate.set()
        if "worker" in locals():
            worker.join(2)
