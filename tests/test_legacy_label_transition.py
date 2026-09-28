"""Administrator transition mode: every set is accepted and classified.

Local storage (event CSV, package outbox) is real; the central client may only
expose its read-only config, so any central call fails the test.
"""

import base64
import copy
import csv
import json
import sqlite3
import sys
import threading
from collections import defaultdict
from contextlib import closing
from datetime import datetime, timedelta
from pathlib import Path
from types import SimpleNamespace

import pytest

import label_transition
from package_logistics import PackageOutbox
from tests.test_label_match_core import (
    _B1_KEY,
    _B1_RAW,
    _b1_app,
    _b1_assert_completion,
    _b1_close,
    _b1_rows,
    _FakeEntry,
    _FakeHistoryTree,
    _FakeLabel,
    _FakeProgressBar,
    load_label_match_module,
)
from tests.test_deferred_lease_clock_retry import _first_scan, clock_case  # noqa: F401
from tests.test_label_operator_action_gates import FakeWidget, _render_app

MASTER = "AAA2270730100"
TODAY = datetime.now().strftime("%Y%m%d")
LOCAL_STATUS = "TRANSITION_LOCAL_ONLY"
OLD_QR = f"CLC={MASTER}|SPC=과도기 품목|PHS=1"
BND_LABEL = f"CLC={MASTER}|SPC=구조화 품목|PHS=1|BND=TRANSFER-BND-1"
PHS2_LABEL = (
    f"PHS=2|SRC=KMTECH_INPUT_TAG|ITG=ITG-OFF-1|CLC={MASTER}|LBL=LBL-OFF-1|HSH=0123456789abcdef"
)


def _legacy_set(number, master=MASTER):
    return [
        master,
        f"{MASTER}-P{number}-1",
        f"{MASTER}-P{number}-2",
        f"{MASTER}-P{number}-3",
        f"{MASTER}-FINAL-LABEL-{number:04d}<GS>6D{TODAY}",
    ]


def _transition(details):
    return {key: details.get(key) for key in (
        "transition_class", "transition_reasons", "transition_duplicate",
    )}


class _CentralClientMustNotBeCalled:
    config = SimpleNamespace(tls_ca_bundle_path="")

    def __getattr__(self, name):
        raise AssertionError(f"legacy transition reached the central client: {name}")


def _fresh_set():
    return {
        "id": None, "raw": [], "parsed": [], "start_time": None,
        "error_count": 0, "has_error_or_reset": False, "phase": None,
        "item_name_override": None, "production_date": None,
    }


def _packaging_app(module, tmp_path, monkeypatch, *, registered, transition):
    monkeypatch.setattr(module, "logistics_runtime_required", lambda: registered)
    monkeypatch.setattr(
        module, "_label_match_direct_sync_context",
        lambda *a, **k: {
            "status_dir": str(tmp_path / "sync-status"),
            "source_host_id": "label-match-transition-test",
            "scan_source_dir": str(tmp_path),
        },
    )
    syncs = []
    monkeypatch.setattr(
        module, "_label_match_start_session_direct_sync",
        lambda context, *, reason: syncs.append(reason) or SimpleNamespace(is_alive=lambda: False),
    )
    app = object.__new__(module.Label_Match)
    app.tk = SimpleNamespace()  # No Tcl interpreter in this storage fixture.
    app.current_set_info = _fresh_set()
    app.initialized_successfully = True
    app.is_running_simulation = False
    app.run_tests = False
    app.is_blinking = False
    app.worker_name = "worker-lt"
    app.save_directory = str(tmp_path)
    app.data_manager = module.DataManager(str(tmp_path), "포장실", "worker-lt", "PC-LT")
    app.package_outbox = PackageOutbox(tmp_path / "package_logistics_outbox.sqlite3")
    app.package_logistics_client = _CentralClientMustNotBeCalled() if registered else None
    app._logistics_authoritative_required = registered
    app._legacy_label_transition_enabled = transition
    app.items_data = {MASTER: {"Item Name": "과도기 품목", "Spec": "규격"}}
    app.scan_count = defaultdict(lambda: defaultdict(int))
    app.global_scanned_set = set()
    app.set_details_map = {}
    app.history_row_details_map = {}
    app.history_tree = _FakeHistoryTree()
    app.history_view_updates_active_state = True
    app.history_active_load_pending = False
    app.save_status_label = _FakeLabel()
    app.status_label = _FakeLabel()
    app.progress_bar = _FakeProgressBar()
    app.update_big_display = lambda *a, **k: None
    app._update_status_label = lambda: None
    app._update_history_tree_in_progress = lambda: None
    app._update_manual_complete_button_state = lambda: None
    app._render_operator_workbench = lambda: None
    app._save_current_set_state = lambda: True
    app.after = lambda *a: None
    app._play_sound = lambda *a: None
    app._update_summary_tree = lambda: None
    app.errors = []
    app._present_inline_workflow_error = (
        lambda title, message, result, details: app.errors.append((title, message))
    )
    app._trigger_modal_error = (
        lambda title, message, result, details: app.errors.append((title, message))
    )
    app.blocks = []
    app._publish_durable_commit_block = lambda error, **k: app.blocks.append(error) or False
    app.warnings = []
    monkeypatch.setattr(
        module.messagebox, "showwarning",
        lambda title, message, **k: app.warnings.append((title, message)),
    )

    def idle():
        app.current_set_info = _fresh_set()
        return True

    app._return_to_idle_after_finalized_set = idle
    return app, syncs


def _scan(module, app, value):
    app.entry = _FakeEntry(value)
    module.Label_Match.process_input(app)


def _events(tmp_path, event):
    rows = []
    for path in sorted(Path(tmp_path).glob("포장실작업이벤트로그_PC-LT_*.csv")):
        with path.open(encoding="utf-8-sig", newline="") as stream:
            rows.extend(
                (row, json.loads(row["details"]))
                for row in csv.DictReader(stream)
                if row["event"] == event
            )
    return rows


def _outbox_rows(tmp_path):
    with closing(sqlite3.connect(
        (Path(tmp_path) / "package_logistics_outbox.sqlite3").as_uri() + "?mode=ro", uri=True,
    )) as conn:
        return conn.execute("SELECT set_id FROM package_command_outbox").fetchall()


def _close(app):
    app.data_manager.flush(timeout=5)
    app.data_manager.close(timeout=5)


def test_switch_reads_only_the_machine_environment_value_one(monkeypatch):
    from logistics_runtime_profile import legacy_label_transition_enabled

    opened = []

    class FakeKey:
        def __enter__(self):
            return self

        def __exit__(self, *exc):
            return False

    def fake_winreg(values):
        def query(key, name):
            if name not in values:
                raise FileNotFoundError(name)
            return values[name], 1

        return SimpleNamespace(
            HKEY_LOCAL_MACHINE="HKLM",
            OpenKey=lambda hive, path: opened.append((hive, path)) or FakeKey(),
            QueryValueEx=query,
        )

    name = "KMTECH_LEGACY_LABEL_TRANSITION"
    monkeypatch.setenv(name, "1")  # process (or user) scope never enables it
    monkeypatch.setitem(sys.modules, "winreg", fake_winreg({}))
    assert legacy_label_transition_enabled() is False
    for value, expected in (("1", True), (" 1 ", True), ("0", False), ("", False),
                            ("true", False), ("yes", False), ("2", False)):
        monkeypatch.setitem(sys.modules, "winreg", fake_winreg({name: value}))
        assert legacy_label_transition_enabled() is expected, value
    assert set(opened) == {
        ("HKLM", r"SYSTEM\CurrentControlSet\Control\Session Manager\Environment")
    }
    assert legacy_label_transition_enabled(lambda requested: "1" if requested == name else "") is True
    assert legacy_label_transition_enabled(lambda requested: "") is False


def test_start_label_classes_keep_new_system_keys_apart_from_legacy():
    import label_transition

    module = load_label_match_module()

    def classify(raw):
        return label_transition.classify_start_label(
            raw, parse_sealed=module._label_match_parse_sealed_transfer_qr,
        )

    assert classify(MASTER) == ("LEGACY", (), "")
    assert classify(OLD_QR)[:2] == ("LEGACY", ())
    # An old phase QR (PHS=2 without any new-system key) stays legacy.
    assert classify(f"CLC={MASTER}|SPC=옛 품목|PHS=2")[:2] == ("LEGACY", ())
    assert classify(PHS2_LABEL) == ("PHS2", (), MASTER)
    assert classify(BND_LABEL) == ("STRUCTURED", (), MASTER)
    cases = {
        f"CLC={MASTER}|SPC=Product|PHS=1|BND=TRANSFER-REAL-1|BND=": ("DUPLICATE_KEY", "PHS_EMPTY"),
        f"CLC={MASTER}|SPC=Product|PHS=1|BND=": ("PHS_EMPTY", "LINEAGE_MISSING"),
        f"CLC={MASTER}|SPC=Product|BND=TRANSFER-REAL-1": ("PHS_MISSING",),
        PHS2_LABEL.replace("HSH=0123456789abcdef", "HSH=0123"): ("PHS2_FORMAT_INVALID",),
        PHS2_LABEL.replace("PHS=2|", ""): ("PHS_MISSING", "PHS2_FORMAT_INVALID"),
        PHS2_LABEL.replace("ITG=ITG-OFF-1", "ITG="): ("PHS_EMPTY", "LINEAGE_MISSING", "PHS2_FORMAT_INVALID"),
        PHS2_LABEL.replace(f"CLC={MASTER}|", ""): ("PHS2_FORMAT_INVALID", "ITEM_UNCONFIRMED"),
        f"TRF=1|BND=TRANSFER-1|CLC={MASTER}|QT=4": ("SEALED_QR_INVALID",),
    }
    for raw, reasons in cases.items():
        assert classify(raw)[:2] == ("MALFORMED", reasons), raw
    assert classify(f"TRF=1|BND=TRANSFER-1|CLC={MASTER}|QT=4")[2] == MASTER
    assert label_transition.fields("PHS2_LOCAL", ["a b", "OK_CODE", "OK_CODE"], 1) == {
        "transition_class": "PHS2_LOCAL",
        "transition_reasons": ["REASON_CODE_REDACTED", "OK_CODE"],
        "transition_duplicate": True,
    }


def test_on_registered_five_scan_set_completes_locally_without_outbox(tmp_path, monkeypatch):
    module = load_label_match_module()
    app, syncs = _packaging_app(module, tmp_path, monkeypatch, registered=True, transition=True)
    try:
        for value in _legacy_set(1):
            _scan(module, app, value)
    finally:
        _close(app)

    assert app.blocks == [] and app.errors == [] and app.warnings == []
    [(row, details)] = _events(tmp_path, "TRAY_COMPLETE")
    assert details["package_logistics"] == {
        "status": LOCAL_STATUS, "sample_barcodes_are_membership": False,
    }
    assert _transition(details) == {
        "transition_class": "LEGACY", "transition_reasons": [], "transition_duplicate": False,
    }
    assert details["scanned_product_barcodes"] == _legacy_set(1)
    assert details["final_result"] == "통과" and details["scan_count"] == 5
    assert details["packaging_set_identity"].startswith("label_match|PC-LT|")
    assert row["worker_name"] == "worker-lt" and row["timestamp"]
    assert _outbox_rows(tmp_path) == []
    assert syncs == ["TRAY_COMPLETE"]
    assert app.current_set_info["raw"] == []
    assert app.save_status_label.kwargs["text"].startswith("✓ 기록됨 · 과도기 옛 방식(원장 제외)")


def test_on_registered_partial_f3_completes_locally_without_outbox(tmp_path, monkeypatch):
    module = load_label_match_module()
    app, _syncs = _packaging_app(module, tmp_path, monkeypatch, registered=True, transition=True)
    try:
        for value in _legacy_set(2)[:3]:
            _scan(module, app, value)
        module.Label_Match._finalize_set(app, app.Results.PASS, is_manual_complete=True)
        for value in [BND_LABEL, *_legacy_set(12)[1:3]]:
            _scan(module, app, value)
        module.Label_Match._finalize_set(app, app.Results.PASS, is_manual_complete=True)
    finally:
        _close(app)

    assert app.blocks == []
    [(_row, legacy), (_row2, structured)] = _events(tmp_path, "TRAY_COMPLETE")
    assert legacy["package_logistics"]["status"] == LOCAL_STATUS
    assert legacy["transition_class"] == "LEGACY"
    assert legacy["is_partial_submission"] is True
    assert legacy["packaging_set_count"] == 0
    assert structured["package_logistics"]["status"] == LOCAL_STATUS
    assert _transition(structured) == {
        "transition_class": "PHS2_LOCAL", "transition_reasons": ["PARTIAL_PACKAGE"],
        "transition_duplicate": False,
    }
    assert _outbox_rows(tmp_path) == []


def test_on_legacy_completion_cancels_locally(tmp_path, monkeypatch):
    module = load_label_match_module()
    app, _syncs = _packaging_app(module, tmp_path, monkeypatch, registered=True, transition=True)
    shown = []
    monkeypatch.setattr(module.messagebox, "askyesno", lambda *a, **k: True)
    for kind in ("showinfo", "showwarning", "showerror"):
        monkeypatch.setattr(
            module.messagebox, kind, lambda *a, _kind=kind, **k: shown.append(_kind)
        )
    try:
        for value in _legacy_set(3):
            _scan(module, app, value)
        module.Label_Match._cancel_completed_tray_by_label(app, MASTER)
    finally:
        _close(app)

    assert shown == ["showinfo"]
    [(_row, cancelled)] = _events(tmp_path, "TRAY_COMPLETION_CANCELLED")
    assert "package_cancellation" not in cancelled
    assert app.set_details_map == {}
    assert _outbox_rows(tmp_path) == []


def test_on_completion_classes_survive_restart_and_banner_counts_them(tmp_path, monkeypatch):
    module = load_label_match_module()
    app, _syncs = _packaging_app(module, tmp_path, monkeypatch, registered=True, transition=True)
    malformed = f"CLC={MASTER}|SPC=Product|PHS=1|BND=TRANSFER-REAL-1|BND="
    try:
        for value in _legacy_set(4):
            _scan(module, app, value)
        for value in [malformed, *_legacy_set(13)[1:]]:
            _scan(module, app, value)
    finally:
        _close(app)

    restarted = object.__new__(module.Label_Match)
    restarted.data_manager = SimpleNamespace(
        _get_log_filepath=app.data_manager._get_log_filepath
    )
    results = __import__("queue").Queue()
    module.Label_Match._async_load_history_task(restarted, results)
    loaded = results.get_nowait()
    classes = sorted(
        (details["transition_class"], tuple(details["transition_reasons"]))
        for details in loaded["set_details_map"].values()
    )
    assert classes == [("LEGACY", ()), ("PHS2_MALFORMED", ("DUPLICATE_KEY", "PHS_EMPTY"))]
    assert set(_legacy_set(4)[1:]) <= loaded["global_scanned_set"]

    restarted.set_details_map = loaded["set_details_map"]
    restarted.legacy_transition_label = FakeWidget()
    restarted._legacy_label_transition_enabled = True
    restarted._render_legacy_transition_banner()
    assert restarted.legacy_transition_label.options["text"] == (
        "과도기 모드 — 옛 방식 포장 받음 · 오늘 옛 방식 1 · 중앙 0 · 로컬 0 · 형식 오류 1 · 중복 0"
    )
    assert restarted.legacy_transition_label.mapped is True

    class Tomorrow(datetime):
        @classmethod
        def now(cls, tz=None):
            return datetime.now(tz) + timedelta(days=1)

    monkeypatch.setattr(module, "datetime", Tomorrow)
    restarted.winfo_exists = lambda: True
    restarted.initialized_successfully = True
    restarted.clock_label = FakeWidget()
    restarted.after = lambda *a: None
    restarted._update_clock()  # the per-second clock starts "오늘" again at midnight
    assert restarted.legacy_transition_label.options["text"].endswith(
        "오늘 옛 방식 0 · 중앙 0 · 로컬 0 · 형식 오류 0 · 중복 0"
    )
    restarted._legacy_label_transition_enabled = False
    restarted._render_legacy_transition_banner()
    assert restarted.legacy_transition_label.mapped is False


def test_on_server_rejection_or_offline_never_blocks_the_next_set(tmp_path, monkeypatch):
    module = load_label_match_module()
    app, _syncs = _packaging_app(module, tmp_path, monkeypatch, registered=True, transition=True)
    (tmp_path / "sync-status").mkdir()
    outcomes = iter([
        {"status": "FAIL", "error": "HTTP 422 rejected by server"},
        {"status": "FAIL", "error": "connection refused (offline)"},
    ])
    monkeypatch.setattr(
        module, "_label_match_run_session_direct_sync_once",
        lambda context, *, reason, **k: {**next(outcomes), "reason": reason},
    )
    started = []

    def start(context, *, reason):
        thread = threading.Thread(
            target=module._label_match_run_and_record_session_direct_sync,
            args=(context,), kwargs={"reason": reason}, daemon=True,
        )
        thread.start()
        started.append(thread)
        return thread

    monkeypatch.setattr(module, "_label_match_start_session_direct_sync", start)
    try:
        for number in (5, 6):
            for value in _legacy_set(number):
                _scan(module, app, value)
            started[-1].join(10)
            report = json.loads(
                (tmp_path / "sync-status" / "label_match_session_direct_sync_trigger.json")
                .read_text(encoding="utf-8")
            )
            assert report["result"]["status"] == "FAIL"
    finally:
        _close(app)

    assert app.blocks == [] and app.errors == []
    assert len(_events(tmp_path, "TRAY_COMPLETE")) == 2
    assert _outbox_rows(tmp_path) == []


@pytest.mark.parametrize(("malformed", "reasons"), [
    (f"CLC={MASTER}|SPC=Product|PHS=1|BND=TRANSFER-REAL-1|BND=", ["DUPLICATE_KEY", "PHS_EMPTY"]),
    (f"CLC={MASTER}|SPC=Product|PHS=1|BND=", ["PHS_EMPTY", "LINEAGE_MISSING"]),
])
def test_on_damaged_bnd_label_is_malformed_not_legacy(tmp_path, monkeypatch, malformed, reasons):
    module = load_label_match_module()
    app, _syncs = _packaging_app(module, tmp_path, monkeypatch, registered=True, transition=True)
    try:
        for value in [malformed, *_legacy_set(14)[1:]]:
            _scan(module, app, value)
    finally:
        _close(app)

    assert app.blocks == [] and app.errors == []
    assert [title for title, _message in app.warnings] == ["현품표 형식 오류 · 과도기 기록"]
    [(_row, details)] = _events(tmp_path, "TRAY_COMPLETE")
    assert _transition(details) == {
        "transition_class": "PHS2_MALFORMED", "transition_reasons": reasons,
        "transition_duplicate": False,
    }
    assert details["scanned_product_barcodes"][0] == malformed  # the raw label value
    assert details["package_logistics"]["status"] == LOCAL_STATUS
    assert _outbox_rows(tmp_path) == []


def test_off_damaged_bnd_label_is_refused_at_first_product_as_malformed(tmp_path, monkeypatch):
    module = load_label_match_module()
    app, _syncs = _packaging_app(module, tmp_path, monkeypatch, registered=True, transition=False)
    malformed = f"CLC={MASTER}|SPC=Product|PHS=1|BND=TRANSFER-REAL-1|BND="
    try:
        _scan(module, app, malformed)
        _scan(module, app, f"{MASTER}-P15-1")
    finally:
        _close(app)

    [(title, message)] = app.errors
    assert title == "[현품표 형식 오류]"
    assert "옛 방식" not in message and "디스크" not in message and "관리자" in message
    assert _events(tmp_path, "TRAY_COMPLETE") == []


@pytest.mark.parametrize("transition", [False, True])
def test_malformed_phs2_label_is_classified_on_and_refused_off(tmp_path, monkeypatch, transition):
    module = load_label_match_module()
    app, _syncs = _packaging_app(
        module, tmp_path, monkeypatch, registered=True, transition=transition,
    )
    damaged = PHS2_LABEL.replace("HSH=0123456789abcdef", "HSH=0123")
    try:
        for value in [damaged, *_legacy_set(16)[1:]]:
            _scan(module, app, value)
    finally:
        _close(app)

    if not transition:
        assert app.errors[0][0] == "[PHS2 현품표 오류]"  # base first-scan refusal
        assert _events(tmp_path, "TRAY_COMPLETE") == []
        return
    assert app.errors == [] and app.blocks == []
    [(_row, details)] = _events(tmp_path, "TRAY_COMPLETE")
    assert _transition(details) == {
        "transition_class": "PHS2_MALFORMED", "transition_reasons": ["PHS2_FORMAT_INVALID"],
        "transition_duplicate": False,
    }
    assert details["scan_count"] == 5 and details["scanned_product_barcodes"][0] == damaged
    assert _outbox_rows(tmp_path) == []


@pytest.mark.parametrize("transition", [False, True])
def test_duplicate_label_is_warned_and_reaches_the_ledger_once(tmp_path, monkeypatch, transition):
    module = load_label_match_module()
    app, _syncs = _packaging_app(
        module, tmp_path, monkeypatch, registered=True, transition=transition,
    )
    try:
        for value in [BND_LABEL, *_legacy_set(17)[1:]]:
            _scan(module, app, value)
        for value in [BND_LABEL, *_legacy_set(18)[1:]]:
            _scan(module, app, value)
    finally:
        _close(app)

    assert len(_outbox_rows(tmp_path)) == 1  # the package ledger sees the label once
    completions = [details for _row, details in _events(tmp_path, "TRAY_COMPLETE")]
    if not transition:
        assert app.errors[0][0] == "[현품표 중복 스캔]"
        assert len(completions) == 1 and "transition_class" not in completions[0]
        return
    assert app.errors == [] and app.blocks == []
    assert [title for title, _message in app.warnings] == ["중복 현품표 · 과도기 기록"]
    assert [_transition(details) for details in completions] == [
        {"transition_class": "PHS2_CENTRAL", "transition_reasons": [], "transition_duplicate": False},
        {"transition_class": "PHS2_LOCAL", "transition_reasons": ["DUPLICATE_LABEL"],
         "transition_duplicate": True},
    ]
    assert completions[1]["package_logistics"]["status"] == LOCAL_STATUS


def test_on_duplicate_old_qr_label_stays_legacy_with_duplicate_mark(tmp_path, monkeypatch):
    module = load_label_match_module()
    app, _syncs = _packaging_app(module, tmp_path, monkeypatch, registered=True, transition=True)
    try:
        for number in (19, 20):
            for value in [OLD_QR, *_legacy_set(number)[1:]]:
                _scan(module, app, value)
    finally:
        _close(app)

    assert [_transition(details) for _row, details in _events(tmp_path, "TRAY_COMPLETE")] == [
        {"transition_class": "LEGACY", "transition_reasons": [], "transition_duplicate": False},
        {"transition_class": "LEGACY", "transition_reasons": ["DUPLICATE_LABEL"],
         "transition_duplicate": True},
    ]
    assert _outbox_rows(tmp_path) == []


def test_on_duplicate_malformed_label_keeps_its_class_with_duplicate_mark(tmp_path, monkeypatch):
    module = load_label_match_module()
    app, _syncs = _packaging_app(module, tmp_path, monkeypatch, registered=True, transition=True)
    malformed = f"CLC={MASTER}|SPC=Product|PHS=1|BND="
    try:
        for number in (24, 25):
            for value in [malformed, *_legacy_set(number)[1:]]:
                _scan(module, app, value)
    finally:
        _close(app)

    assert [title for title, _message in app.warnings] == [
        "현품표 형식 오류 · 과도기 기록", "중복 현품표 · 과도기 기록",
    ]
    assert [_transition(details) for _row, details in _events(tmp_path, "TRAY_COMPLETE")] == [
        {"transition_class": "PHS2_MALFORMED", "transition_reasons": ["PHS_EMPTY", "LINEAGE_MISSING"],
         "transition_duplicate": False},
        {"transition_class": "PHS2_MALFORMED",
         "transition_reasons": ["PHS_EMPTY", "LINEAGE_MISSING", "DUPLICATE_LABEL"],
         "transition_duplicate": True},
    ]


def test_on_failed_set_row_keeps_its_label_class(tmp_path, monkeypatch):
    module = load_label_match_module()
    app, _syncs = _packaging_app(module, tmp_path, monkeypatch, registered=True, transition=True)
    wrong = "BBB2270730100-WRONG-ITEM-1"
    try:
        for value in _legacy_set(26)[:2]:
            _scan(module, app, value)
        _scan(module, app, wrong)
        assert [title for title, _message in app.errors] == ["[제품 불일치]"]
        module.Label_Match._finalize_set(app, app.Results.FAIL_MISMATCH, wrong)
    finally:
        _close(app)

    [(_row, details)] = _events(tmp_path, "TRAY_COMPLETE")
    assert details["final_result"] == app.Results.FAIL_MISMATCH
    assert details["packaging_set_count"] == 0
    assert _transition(details) == {
        "transition_class": "LEGACY", "transition_reasons": [], "transition_duplicate": False,
    }
    assert "package_logistics" not in details and _outbox_rows(tmp_path) == []


@pytest.mark.parametrize("transition", [False, True])
def test_central_refusal_at_f3_is_kept_as_phs2_local(tmp_path, monkeypatch, transition):
    module = load_label_match_module()
    app, actions, _clock = _b1_app(module, tmp_path, monkeypatch)
    app._legacy_label_transition_enabled = transition

    def refuse(*_args, **_kwargs):
        raise module.OperationLeaseError(
            "OPERATION_LEASE_REQUIRED", "a verified operation lease is required",
        )

    monkeypatch.setattr(module, "_label_match_package_draft", refuse)
    try:
        app._begin_central_package_submission()
    finally:
        _b1_close(app.data_manager)
    commands, events = _b1_rows(tmp_path)
    assert commands == []
    if not transition:
        assert events == [] and actions == ["a verified operation lease is required"]
        return
    [(_row, details)] = events
    assert _transition(details) == {
        "transition_class": "PHS2_LOCAL", "transition_reasons": ["OPERATION_LEASE_REQUIRED"],
        "transition_duplicate": False,
    }
    assert details["package_logistics"]["status"] == LOCAL_STATUS
    assert actions == ["sound:pass", "summary", "idle"]  # no outbox drain


class _OfflineCaptureStore:
    """Only the capture seam; the overlay, cancel call and set flow are real."""

    def __init__(self):
        self.cancelled = []

    def capture_label_package_source(self, *, local_work_identity, physical_qr_payload, item_code):
        self.capture = (local_work_identity, physical_qr_payload, item_code)
        return SimpleNamespace(
            intent_id="INTENT-OFFLINE-1", pending_count=1, oldest_age_seconds=0, reason_code="",
        )

    def get_owned_capture(self, *, intent_id, local_work_identity, physical_qr_payload):
        return {"intent_id": intent_id, "state": "RETRY_WAIT_VALIDATION", "row_version": 2,
                "authority_scope_id": "SCOPE-LT"}

    def cancel_unsubmitted(self, **kwargs):
        self.cancelled.append(kwargs)
        return {"state": "CANCELLED"}

    def validation_status(self, intent_id):
        return None


@pytest.mark.parametrize("transition", [False, True])
def test_offline_phs2_lookup_is_kept_as_a_local_five_scan_set(tmp_path, monkeypatch, transition):
    module = load_label_match_module()
    app, _syncs = _packaging_app(
        module, tmp_path, monkeypatch, registered=True, transition=transition,
    )
    app.package_logistics_client.config = SimpleNamespace(
        tls_ca_bundle_path="", authority_scope_id="SCOPE-LT",
        device_id="PC-LT", source_host_id="HOST-LT",
    )
    store = _OfflineCaptureStore()
    app.deferred_intent_capture = store
    app._prepare_deferred_label_validation = lambda capture: module.DeferredValidationResult(
        intent_id=capture.intent_id, state="RETRY_WAIT_VALIDATION",
        outcome="RETRYABLE_UNAVAILABLE", reason_code="PACKAGE_TRANSPORT_UNAVAILABLE",
        observed_at="2026-09-29T00:00:00Z",
    )
    try:
        for value in [PHS2_LABEL, *_legacy_set(21)[1:]]:
            _scan(module, app, value)
    finally:
        _close(app)

    assert store.capture[1:] == (PHS2_LABEL, MASTER)
    if not transition:
        assert store.cancelled == []
        assert app._deferred_capture_ui["status"] == "저장됨-검증대기"
        assert _events(tmp_path, "TRAY_COMPLETE") == []
        return
    [cancel] = store.cancelled
    assert cancel["intent_id"] == "INTENT-OFFLINE-1" and cancel["physical_qr_payload"] == PHS2_LABEL
    assert [title for title, _message in app.warnings] == ["중앙 확인 불가 · 과도기 로컬 기록"]
    [(_row, details)] = _events(tmp_path, "TRAY_COMPLETE")
    assert _transition(details) == {
        "transition_class": "PHS2_LOCAL", "transition_reasons": ["PACKAGE_TRANSPORT_UNAVAILABLE"],
        "transition_duplicate": False,
    }
    assert details["scan_count"] == 5 and details["packaging_scan_mode"] == "LEGACY_QA_SAMPLES"
    assert _outbox_rows(tmp_path) == []


def test_on_real_capture_store_cancels_the_offline_phs2_before_the_local_set(clock_case, monkeypatch):
    """Production capture store and cancellation; only the read is offline."""

    from unittest.mock import Mock

    from package_logistics import PackageTransportError
    from tests.test_deferred_intent_capture import _row

    case = clock_case
    app = case.app
    app.package_logistics_client.resolve_package_source_evidence = Mock(
        side_effect=PackageTransportError("offline read")
    )
    intent_id = _first_scan(case)  # the base first scan: captured, read failed
    outcome = app.deferred_intent_capture.validation_status(intent_id)
    assert outcome.state == "RETRY_WAIT_VALIDATION" and app.current_set_info["raw"] == []

    events = []
    app.tk = SimpleNamespace()  # No Tcl interpreter in this storage fixture.
    app.history_view_updates_active_state = True
    app.data_manager = SimpleNamespace(log_event=lambda *event: events.append(event))
    app.run_tests = False
    app.is_running_simulation = False
    app._legacy_label_transition_enabled = True
    app.worker_name = "worker-lt"
    app.progress_bar = _FakeProgressBar()
    for name in ("update_big_display", "_update_status_label", "_update_history_tree_in_progress",
                 "_render_operator_workbench", "_play_sound", "_clear_workflow_completion"):
        setattr(app, name, lambda *a, **k: None)
    app._save_current_set_state = lambda: True
    warnings = []
    module = load_label_match_module()
    monkeypatch.setattr(module.messagebox, "showwarning", lambda title, *a, **k: warnings.append(title))

    assert app._transition_accept_blocked_phs2(
        outcome, case.group["scan_payload"], case.group["item_id"],
    ) is True
    assert _row(case.database, intent_id)["state"] == "CANCELLED"
    assert app.current_set_info["raw"] == [case.group["scan_payload"]]
    assert "deferred_intent_id" not in app.current_set_info
    assert _transition(app.current_set_info) == {
        "transition_class": "PHS2_LOCAL", "transition_reasons": ["PACKAGE_TRANSPORT_UNAVAILABLE"],
        "transition_duplicate": False,
    }
    assert app._central_inherit_all_active() is False  # five scans follow
    assert warnings == ["중앙 확인 불가 · 과도기 로컬 기록"]
    with closing(sqlite3.connect(case.database)) as conn:
        assert conn.execute("SELECT COUNT(*) FROM package_command_outbox").fetchone()[0] == 0
        assert conn.execute("SELECT COUNT(*) FROM package_operation_leases").fetchone()[0] == 0


@pytest.mark.parametrize("transition", [False, True])
def test_f3_refusal_closes_the_validated_capture_before_a_local_completion(
    clock_case, monkeypatch, transition,
):
    """An open VALIDATED capture would hold later captures in its FIFO."""

    from tests.test_deferred_intent_capture import _row

    case = clock_case
    app = case.app
    intent_id = _first_scan(case)  # validated online at the first scan
    assert _row(case.database, intent_id)["state"] == "VALIDATED"
    import Label_Match as module

    def refuse(*_args, **_kwargs):  # refused before any F3 lease is issued
        raise module.PackageLogisticsError("PHS2 package draft is refused")

    monkeypatch.setattr(module, "_label_match_package_draft", refuse)
    app.run_tests = False
    app.worker_name = "worker-lt"
    app._legacy_label_transition_enabled = transition
    if not transition:
        with pytest.raises(module.PackageLogisticsError):
            app._queue_authoritative_package(item_code=case.group["item_id"], is_manual_complete=False)
        assert _row(case.database, intent_id)["state"] == "VALIDATED"
        return
    package = app._queue_authoritative_package(
        item_code=case.group["item_id"], is_manual_complete=False,
    )
    assert package == {
        "status": LOCAL_STATUS, "sample_barcodes_are_membership": False,
        "transition_class": "PHS2_LOCAL", "transition_reasons": ["PACKAGE_CENTRAL_BLOCKED"],
        "transition_duplicate": False,
    }
    assert _row(case.database, intent_id)["state"] == "CANCELLED"
    assert case.calls == []  # no F3 lease was issued
    assert app.deferred_intent_capture.next_materialization_candidate() is None
    with closing(sqlite3.connect(case.database)) as conn:
        assert conn.execute("SELECT COUNT(*) FROM package_command_outbox").fetchone()[0] == 0


def test_on_held_phs2_label_is_kept_local(tmp_path, monkeypatch):
    module = load_label_match_module()
    app, _syncs = _packaging_app(module, tmp_path, monkeypatch, registered=True, transition=True)
    app._recover_unknown_package_hold_identities = lambda: None
    app.package_outbox.workbench_hold_for_source = lambda *a: {"set_id": "held-before"}
    try:
        for value in [PHS2_LABEL, *_legacy_set(22)[1:]]:
            _scan(module, app, value)
    finally:
        _close(app)

    [(_row, details)] = _events(tmp_path, "TRAY_COMPLETE")
    assert _transition(details) == {
        "transition_class": "PHS2_LOCAL", "transition_reasons": ["PACKAGE_WORKBENCH_HOLD"],
        "transition_duplicate": False,
    }
    assert _outbox_rows(tmp_path) == []


def test_on_flush_retry_resends_the_one_completion_row_unchanged(tmp_path, monkeypatch):
    module = load_label_match_module()
    app, _syncs = _packaging_app(module, tmp_path, monkeypatch, registered=True, transition=True)
    real_flush = module.Label_Match._flush_data_manager_if_supported
    glitched = []

    def flush(timeout=5.0):
        real_flush(app, timeout=timeout)
        if not glitched and _events(tmp_path, "TRAY_COMPLETE"):  # right after the append
            glitched.append(timeout)
            raise TimeoutError("Log writer did not flush before timeout")

    app._flush_data_manager_if_supported = flush
    try:
        for value in _legacy_set(23):
            _scan(module, app, value)
        assert [type(error) for error in app.blocks] == [TimeoutError]
        assert app.current_set_info["raw"] == _legacy_set(23)  # still the same set
        [log_path] = Path(tmp_path).glob("포장실작업이벤트로그_PC-LT_*.csv")
        first = log_path.read_bytes()
        [(first_row, first_details)] = _events(tmp_path, "TRAY_COMPLETE")

        module.Label_Match._finalize_set(app, app.Results.PASS)  # operator retry
        app.data_manager.flush(timeout=5)
        assert log_path.read_bytes() == first  # same row, same detail, same time
    finally:
        _close(app)

    assert len(app.blocks) == 1
    [(row, details)] = _events(tmp_path, "TRAY_COMPLETE")
    assert (row, details) == (first_row, first_details)
    assert details["transition_class"] == "LEGACY"
    assert app.current_set_info["raw"] == []


@pytest.mark.parametrize(
    ("error_text", "title"),
    [
        (
            "central packaging requires a sealed transfer QR, structured PHS BND/ITG "
            "lineage, or FULL EXACT_RESCAN; three product samples are not membership",
            "5/5 유지 · 옛 방식 현품표 완료 불가",
        ),
        (
            "AUTHORITATIVE_LOGISTICS_REQUIRED: manual packaging completion is disabled",
            "5/5 유지 · F3 소량 완료 불가",
        ),
    ],
)
def test_policy_block_names_its_cause_instead_of_disk_storage(error_text, title):
    module = load_label_match_module()
    app = object.__new__(module.Label_Match)
    app.current_set_info = {"raw": _legacy_set(9), "parsed": [MASTER] * 5}
    rendered = []
    app._render_operator_workbench = lambda: rendered.append(app._workflow_blocking_notice)
    error = module.PackageLogisticsError(error_text)
    lane_failure = module.Failure.from_exception(
        error, safe_operator_code=module._label_match_package_policy_block_code(error),
    )

    for failure in (error, lane_failure):
        module.Label_Match._publish_durable_commit_block(app, failure)
        notice = rendered[-1]
        assert notice.title == title
        assert "디스크" not in notice.message and "관리자" in notice.message
        assert app._workflow_notice_action_text == "다시 확인"

    module.Label_Match._publish_durable_commit_block(app, OSError("disk full"))
    assert "디스크" in rendered[-1].message
    assert app._workflow_notice_action_text == "저장 재시도"


def test_off_registered_legacy_label_is_explained_at_first_scan(tmp_path, monkeypatch):
    module = load_label_match_module()
    app, _syncs = _packaging_app(module, tmp_path, monkeypatch, registered=True, transition=False)
    try:
        _scan(module, app, MASTER)
        assert app.current_set_info["raw"] == [MASTER]  # kept for the base F4 path
        _scan(module, app, f"{MASTER}-P7-1")
    finally:
        _close(app)

    assert app.current_set_info["raw"] == [MASTER]
    [(title, message)] = app.errors
    assert title == "[옛 방식 포장 불가]"
    assert "과도기 모드가 꺼져" in message and "관리자" in message
    assert "디스크" not in message
    assert _events(tmp_path, "TRAY_COMPLETE") == []
    [(_row, rejected)] = _events(tmp_path, "ERROR_INPUT")
    assert rejected["scan_pos"] == 2


def test_off_first_scan_view_names_cause_and_keeps_f1_and_base_f4():
    app = _render_app((MASTER,), (MASTER,))
    app.run_tests = False
    app.package_logistics_client = object()
    app._legacy_label_transition_enabled = False

    view = app._render_operator_workbench()

    assert view.current_stage == "legacy_label_blocked"
    assert view.notice.title == "옛 방식 현품표 · 5단계 포장 불가"
    assert "과도기 모드가 꺼져" in view.notice.message and "F4" in view.notice.message
    assert "디스크" not in view.notice.message
    assert app.big_display_label.options["text"] == "옛 방식 현품표"
    assert view.f4_enabled and view.cancel_current_enabled and view.scan_input_enabled

    app._legacy_label_transition_enabled = True
    assert app._render_operator_workbench().current_stage == "product_1"
    app._legacy_label_transition_enabled = False
    app.current_set_info["exact_rescan_active"] = True
    assert app._render_operator_workbench().current_stage == "exact_rescan"


def test_off_f4_exact_rescan_path_is_not_gated(tmp_path, monkeypatch):
    module = load_label_match_module()
    app, _syncs = _packaging_app(module, tmp_path, monkeypatch, registered=True, transition=False)
    try:
        _scan(module, app, MASTER)
        app.current_set_info.update(
            exact_rescan_active=True, exact_rescan_complete=False,
            exact_rescan_target_count=1, exact_rescan_source_bundle_id="TRANSFER-REAL-1",
            exact_rescan_barcodes=[],
        )
        _scan(module, app, f"{MASTER}-P8-X")
        assert app.current_set_info["exact_rescan_complete"] is True
        _scan(module, app, f"{MASTER}-P8-1")
    finally:
        _close(app)

    assert app.errors == []
    assert app.current_set_info["raw"] == [MASTER, f"{MASTER}-P8-1"]


@pytest.mark.parametrize("transition", [False, True])
def test_central_phs2_completion_is_identical_in_both_modes(tmp_path, monkeypatch, transition):
    module = load_label_match_module()
    app, actions, _clock = _b1_app(module, tmp_path, monkeypatch)
    app._legacy_label_transition_enabled = transition
    try:
        assert app._begin_central_package_submission() is True
        command, row = _b1_assert_completion(tmp_path)
        assert command["idempotency_key"] == _B1_KEY
        assert actions == ["sound:pass", "drain", "summary", "idle"]
    finally:
        _b1_close(app.data_manager)
    details = json.loads(row["details"])
    assert details["scanned_product_barcodes"] == [_B1_RAW]
    if transition:
        assert _transition(details) == {
            "transition_class": "PHS2_CENTRAL", "transition_reasons": [],
            "transition_duplicate": False,
        }
    else:
        assert "transition_class" not in details


@pytest.mark.parametrize("transition", [False, True])
def test_structured_bnd_label_keeps_the_central_outbox_path(tmp_path, monkeypatch, transition):
    module = load_label_match_module()
    app, _syncs = _packaging_app(
        module, tmp_path, monkeypatch, registered=True, transition=transition,
    )
    values = [BND_LABEL, *_legacy_set(10)[1:]]
    try:
        for value in values:
            _scan(module, app, value)
    finally:
        _close(app)

    assert app.errors == []
    [(_row, details)] = _events(tmp_path, "TRAY_COMPLETE")
    assert details["package_logistics"]["status"] != LOCAL_STATUS
    assert details["package_logistics"]["idempotency_key"]
    assert details.get("transition_class") == ("PHS2_CENTRAL" if transition else None)
    assert len(_outbox_rows(tmp_path)) == 1


@pytest.mark.parametrize("transition", [False, True])
def test_unregistered_legacy_set_is_unchanged_in_both_modes(tmp_path, monkeypatch, transition):
    module = load_label_match_module()
    app, _syncs = _packaging_app(
        module, tmp_path, monkeypatch, registered=False, transition=transition,
    )
    try:
        for value in _legacy_set(11):
            _scan(module, app, value)
    finally:
        _close(app)

    assert app.errors == [] and app.blocks == []
    [(_row, details)] = _events(tmp_path, "TRAY_COMPLETE")
    assert details["package_logistics"] == {
        "status": "LEGACY_DIRECT_SYNC_ONLY", "sample_barcodes_are_membership": False,
    }
    assert "transition_class" not in details
    assert _outbox_rows(tmp_path) == []


# --- Cross-check sub-02 (Codex) findings and the 09-29 coordinator decisions ---
# Ported from work/Label_Match/w9lmtransit/sub-02/test_crosscheck2.py; the
# sealed start label keeps the approved refusal, so its case checks the notice.


def _app_for_recovery(module, tmp_path, monkeypatch):
    app, _ = _packaging_app(module, tmp_path, monkeypatch, registered=True, transition=True)
    del app.__dict__["_save_current_set_state"]  # the real current-state file
    app._recover_pending_package_pin_moves = lambda: None
    app._finalize_label_recovery_holds = lambda: True
    app.sealed_transfer_exchange_store = SimpleNamespace(blocking_rows=lambda **k: [])
    app._reconcile_pending_sealed_transfer_exchanges = lambda **k: None
    app._reconcile_active_package_submission = lambda: None
    for name in ("askyesno", "askyesnocancel"):
        monkeypatch.setattr(module.messagebox, name, lambda *a, **k: True)
    for name in ("showinfo", "showerror"):
        monkeypatch.setattr(module.messagebox, name, lambda *a, **k: None)
    return app


def _load_history(module, app):
    pending = __import__("queue").Queue()
    module.Label_Match._async_load_history_task(app, pending)
    result = pending.get_nowait()
    assert "error" not in result, result
    for key in ("scan_count", "global_scanned_set", "set_details_map"):
        setattr(app, key, result[key])
    return result


def _select_completed_rows(app):
    from tests.test_label_match_core import _RecordingTree

    class SelectableTree(_RecordingTree):
        def selection(self):
            return tuple(self.rows)

        def item(self, iid, option=None):
            return self.rows[iid]["values"] if option == "values" else self.rows[iid]

    app.history_tree = SelectableTree()
    for set_id in app.set_details_map:
        app.history_tree.insert("", "end", iid=set_id,
                                values=(set_id, MASTER, "p1", "p2", "p3", "final", "통과", "05:00:00"))
    app._render_history_detail = lambda *a, **k: None


@pytest.mark.parametrize("encoded", [False, True])
def test_duplicate_src_keeps_the_new_label_marker(tmp_path, monkeypatch, encoded):
    import base64

    module = load_label_match_module()
    raw = f"CLC={MASTER}|SPC=Product|PHS=2|SRC=KMTECH_INPUT_TAG|SRC=OLD"
    if encoded:
        raw = base64.b64encode(raw.encode()).decode()
    app, _ = _packaging_app(module, tmp_path, monkeypatch, registered=True, transition=True)
    try:
        for value in [raw, *_legacy_set(101)[1:]]:
            _scan(module, app, value)
    finally:
        _close(app)

    [(_row, details)] = _events(tmp_path, "TRAY_COMPLETE")
    assert details["final_result"] == app.Results.PASS and _outbox_rows(tmp_path) == []
    assert details["transition_class"] == "PHS2_MALFORMED"
    assert "DUPLICATE_KEY" in details["transition_reasons"]


@pytest.mark.parametrize("raw", [
    BND_LABEL + "|PHS=1", BND_LABEL + "|BND=", PHS2_LABEL + "|ITG=", PHS2_LABEL + "|SRC=OLD",
])
def test_detectable_duplicate_keys_keep_the_malformed_class(raw):
    import label_transition

    module = load_label_match_module()
    shape, reasons, _item = label_transition.classify_start_label(
        raw, parse_sealed=module._label_match_parse_sealed_transfer_qr,
    )
    assert shape == "MALFORMED" and "DUPLICATE_KEY" in reasons


@pytest.mark.parametrize("quantity", ["abc", "0", "-1"])
@pytest.mark.parametrize("kind", ["BND", "SEALED", "COMPACT"])
def test_malformed_quantity_is_accepted_outside_the_ledger(tmp_path, monkeypatch, quantity, kind):
    module = load_label_match_module()
    if kind == "BND":
        raw = BND_LABEL + "|QT=" + quantity
    elif kind == "COMPACT":
        raw = PHS2_LABEL + "|QT=" + quantity
    else:
        raw = (f"TRF=1|BND=T-QTY|AUTH_SCOPE=S-QTY|CLC={MASTER}|QT={quantity}"
               f"|HSH={'a' * 64}|EPOCH=1|PLANE=AUTHORITATIVE|PE=1")
    app, _ = _packaging_app(module, tmp_path, monkeypatch, registered=True, transition=True)
    try:
        for value in [raw, *_legacy_set(102)[1:]]:
            _scan(module, app, value)
    finally:
        _close(app)

    [(_row, details)] = _events(tmp_path, "TRAY_COMPLETE")
    assert details["final_result"] == app.Results.PASS
    assert details["transition_class"] == "PHS2_MALFORMED"
    assert "QT_INVALID" in details["transition_reasons"]
    assert _outbox_rows(tmp_path) == []


@pytest.mark.parametrize("transition", [False, True])
def test_valid_sealed_start_label_names_the_cause_and_next_step(tmp_path, monkeypatch, transition):
    module = load_label_match_module()
    raw = (f"TRF=1|BND=T-SEALED|AUTH_SCOPE=S-SEALED|CLC={MASTER}|QT=4"
           f"|HSH={'b' * 64}|EPOCH=1|PLANE=AUTHORITATIVE|PE=1")
    app, _ = _packaging_app(module, tmp_path, monkeypatch, registered=True, transition=transition)
    try:
        _scan(module, app, raw)
    finally:
        _close(app)

    [(title, message)] = app.errors  # approved: not a start label in either mode
    assert title == "[PHS2 현품표 필요]" and app.current_set_info["raw"] == []
    assert "이적 봉인 QR" in message and "원본 PHS2 현품표를 스캔" in message
    assert "관리자" in message and "디스크" not in message


def test_malformed_inspection_alias_keeps_the_resolved_item(tmp_path, monkeypatch):
    module = load_label_match_module()
    raw = f"CLC=INSPECTION|ITEM={MASTER}|SPC=Product|PHS=1|BND="
    app, _ = _packaging_app(module, tmp_path, monkeypatch, registered=True, transition=True)
    try:
        for value in [raw, *_legacy_set(103)[1:]]:
            _scan(module, app, value)
    finally:
        _close(app)

    assert app.errors == []
    [(_row, details)] = _events(tmp_path, "TRAY_COMPLETE")
    assert details["transition_class"] == "PHS2_MALFORMED" and details["item_code"] == MASTER


@pytest.mark.parametrize("raw", [BND_LABEL + "|BND=", PHS2_LABEL + "|QT=abc"])
def test_malformed_current_set_survives_a_real_save_and_load(tmp_path, monkeypatch, raw):
    module = load_label_match_module()
    app = _app_for_recovery(module, tmp_path, monkeypatch)
    try:
        for value in [raw, _legacy_set(104)[1]]:
            _scan(module, app, value)
        saved = copy.deepcopy(app.current_set_info)
        assert app._save_current_set_state()
    finally:
        _close(app)
    restored = _app_for_recovery(module, tmp_path, monkeypatch)
    try:
        restored._load_current_set_state()
        assert restored.current_set_info["id"] == saved["id"]
        assert restored.current_set_info["raw"] == saved["raw"]
        assert restored.current_set_info["transition_class"] == "PHS2_MALFORMED"
        assert not restored._central_inherit_all_active()
    finally:
        _close(restored)


def test_second_cycle_of_a_label_survives_restore_and_counts_separately(tmp_path, monkeypatch):
    module = load_label_match_module()
    app = _app_for_recovery(module, tmp_path, monkeypatch)
    try:
        for value in [OLD_QR, *_legacy_set(105)[1:]]:
            _scan(module, app, value)
        first_id = next(iter(app.set_details_map))
        for value in [OLD_QR, _legacy_set(106)[1]]:
            _scan(module, app, value)
        second_id = app.current_set_info["id"]
        assert second_id != first_id and app.current_set_info["transition_duplicate"]
    finally:
        _close(app)
    restored = _app_for_recovery(module, tmp_path, monkeypatch)
    try:
        _load_history(module, restored)
        restored._load_current_set_state()
        assert restored.current_set_info["id"] == second_id
        assert len(restored.current_set_info["raw"]) == 2
        for value in _legacy_set(106)[2:]:
            _scan(module, restored, value)
        restored.data_manager.flush(timeout=5)
        rows = _load_history(module, restored)["set_details_map"]
        assert set(rows) == {first_id, second_id}
        assert sum(bool(details["transition_duplicate"]) for details in rows.values()) == 1
    finally:
        _close(restored)


def test_local_phs2_current_set_is_kept_after_midnight(tmp_path, monkeypatch):
    module = load_label_match_module()
    app = _app_for_recovery(module, tmp_path, monkeypatch)
    try:
        app.global_scanned_set.add(PHS2_LABEL)
        for value in [PHS2_LABEL, _legacy_set(107)[1]]:
            _scan(module, app, value)
        saved = copy.deepcopy(app.current_set_info)
        assert saved["transition_class"] == "PHS2_LOCAL"
    finally:
        _close(app)

    class Tomorrow(datetime):
        @classmethod
        def now(cls, tz=None):
            return datetime.now(tz) + timedelta(days=1)

    monkeypatch.setattr(module, "datetime", Tomorrow)
    restored = _app_for_recovery(module, tmp_path, monkeypatch)
    try:
        restored._load_current_set_state()
        assert restored.current_set_info["raw"] == saved["raw"]
        assert Path(restored._package_current_state_path()).exists()
    finally:
        _close(restored)


def test_started_transition_cycle_keeps_its_class_after_switch_off(tmp_path, monkeypatch):
    module = load_label_match_module()
    app = _app_for_recovery(module, tmp_path, monkeypatch)
    try:
        app.global_scanned_set.add(PHS2_LABEL)
        for value in [PHS2_LABEL, _legacy_set(112)[1]]:
            _scan(module, app, value)
        assert app.current_set_info["transition_class"] == "PHS2_LOCAL"
    finally:
        _close(app)
    restored = _app_for_recovery(module, tmp_path, monkeypatch)
    restored._legacy_label_transition_enabled = False
    try:
        restored._load_current_set_state()
        for value in _legacy_set(112)[2:]:
            _scan(module, restored, value)
    finally:
        _close(restored)

    [(_row, details)] = _events(tmp_path, "TRAY_COMPLETE")
    assert details["final_result"] == restored.Results.PASS and _outbox_rows(tmp_path) == []
    assert details["transition_class"] == "PHS2_LOCAL" and details["transition_duplicate"] is True
    assert details["package_logistics"]["status"] == LOCAL_STATUS


def test_started_legacy_cycle_is_not_refused_after_switch_off(tmp_path, monkeypatch):
    module = load_label_match_module()
    app = _app_for_recovery(module, tmp_path, monkeypatch)
    try:
        for value in _legacy_set(113)[:2]:
            _scan(module, app, value)
        assert app.current_set_info["transition_class"] == "LEGACY"
    finally:
        _close(app)
    restored = _app_for_recovery(module, tmp_path, monkeypatch)
    restored._legacy_label_transition_enabled = False
    try:
        restored._load_current_set_state()
        for value in _legacy_set(113)[2:]:
            _scan(module, restored, value)
    finally:
        _close(restored)

    assert restored.errors == []
    [(_row, details)] = _events(tmp_path, "TRAY_COMPLETE")
    assert details["transition_class"] == "LEGACY" and _outbox_rows(tmp_path) == []


@pytest.mark.parametrize("event", ["TRAY_COMPLETE", "TRAY_COMPLETION_CANCELLED", "SET_DELETED"])
def test_transition_business_events_carry_a_stable_event_key(tmp_path, monkeypatch, event):
    module = load_label_match_module()
    app, _ = _packaging_app(module, tmp_path, monkeypatch, registered=True, transition=True)
    monkeypatch.setattr(module.messagebox, "askyesno", lambda *a, **k: True)
    for name in ("showinfo", "showerror"):
        monkeypatch.setattr(module.messagebox, name, lambda *a, **k: None)
    try:
        for value in _legacy_set(108):
            _scan(module, app, value)
        set_id = next(iter(app.set_details_map))
        if event == "SET_DELETED":
            _select_completed_rows(app)
            app._delete_selected_row()
        elif event == "TRAY_COMPLETION_CANCELLED":
            app._cancel_completed_tray_by_label(MASTER)
    finally:
        _close(app)

    [(_row, details)] = _events(tmp_path, event)
    key = details["idempotency_key"]
    assert isinstance(key, str) and 0 < len(key) <= 128
    # Both cancel routes share the one cancellation ID of the completed set.
    assert key == (
        module.label_transition.event_key("PC-LT", set_id, event)
        if event == "TRAY_COMPLETE"
        else module.label_transition.cancellation_key("PC-LT", set_id)
    )
    assert details["app_version"] == module.APP_VERSION


@pytest.mark.parametrize(("event", "retry"), [
    ("SET_DELETED", "SET_DELETED"),
    ("TRAY_COMPLETION_CANCELLED", "TRAY_COMPLETION_CANCELLED"),
    # sub-04 P1: the retry may come through the other cancel route.
    ("SET_DELETED", "TRAY_COMPLETION_CANCELLED"),
    ("TRAY_COMPLETION_CANCELLED", "SET_DELETED"),
])
def test_cancellation_retry_reuses_the_original_csv_row(tmp_path, monkeypatch, event, retry):
    module = load_label_match_module()
    app, _ = _packaging_app(module, tmp_path, monkeypatch, registered=True, transition=True)
    monkeypatch.setattr(module.messagebox, "askyesno", lambda *a, **k: True)
    for name in ("showinfo", "showerror"):
        monkeypatch.setattr(module.messagebox, name, lambda *a, **k: None)
    real_flush = app._flush_data_manager_if_supported
    glitched = []

    def flush(timeout=5):
        real_flush(timeout=timeout)
        if not glitched and _events(tmp_path, event):
            glitched.append(1)
            raise TimeoutError("cancellation append succeeded, flush status unavailable")

    try:
        for value in _legacy_set(111):
            _scan(module, app, value)
        _select_completed_rows(app)
        routes = {
            "SET_DELETED": app._delete_selected_row,
            "TRAY_COMPLETION_CANCELLED": lambda: app._cancel_completed_tray_by_label(MASTER),
        }
        app._flush_data_manager_if_supported = flush
        routes[event]()
        assert app.set_details_map, "the uncertain first cancellation keeps the target"
        first = _events(tmp_path, event)
        assert len(first) == 1
        routes[retry]()
        app.data_manager.flush(timeout=5)
        # The first row stays the one row (same timestamp), whichever route retried.
        assert _events(tmp_path, "SET_DELETED") + _events(tmp_path, "TRAY_COMPLETION_CANCELLED") == first
    finally:
        _close(app)


def test_failed_completion_stays_out_of_the_banner_after_restart(tmp_path, monkeypatch):
    module = load_label_match_module()
    app, _ = _packaging_app(module, tmp_path, monkeypatch, registered=True, transition=True)
    try:
        for value in _legacy_set(109)[:2]:
            _scan(module, app, value)
        app._finalize_set(app.Results.FAIL_MISMATCH, "wrong item")
        app.data_manager.flush(timeout=5)
        app.legacy_transition_label = FakeWidget()
        app._render_legacy_transition_banner()
        before = app.legacy_transition_label.options["text"]
        _load_history(module, app)
        app._render_legacy_transition_banner()
        assert app.legacy_transition_label.options["text"] == before
        assert before.endswith("오늘 옛 방식 0 · 중앙 0 · 로컬 0 · 형식 오류 0 · 중복 0")
    finally:
        _close(app)


@pytest.mark.parametrize("failure_stage", ["lookup", "apply"])
@pytest.mark.parametrize("transition", [False, True])
def test_f3_source_refresh_refusal_completes_locally_before_any_command(
    tmp_path, monkeypatch, failure_stage, transition,
):
    module = load_label_match_module()
    app, actions, _clock = _b1_app(module, tmp_path, monkeypatch)
    app._legacy_label_transition_enabled = transition
    app.current_set_info.pop("package_source_snapshot", None)
    app.ui_lane = object()
    submitted = []
    app._submit_ui_lane_task = lambda **task: submitted.append(task) or SimpleNamespace(accepted=True)
    app._publish_submission_block = lambda error: actions.append("preflight:" + str(error))

    def refuse(*_args, **_kwargs):
        raise module.PackageLogisticsError("PACKAGE_PREVIEW_MISMATCH")

    if failure_stage == "lookup":
        app._resolve_central_phs2_seal_for_exchange = refuse
    else:
        app._resolve_central_phs2_seal_for_exchange = lambda *a: ({}, {}, {})
        app._apply_resolved_central_phs2_seal = refuse
    try:
        assert app._begin_central_package_submission()
        [task] = submitted[:1]
        try:
            value = task["work"]()
        except Exception as error:  # the base lane delivers it to fail()
            task["fail"](error)
        else:
            task["finish"](value)
        if len(submitted) > 1:  # the local completion itself runs on the lane
            completion = submitted[1]
            completion["finish"](completion["work"]())
    finally:
        _b1_close(app.data_manager)
    commands, rows = _b1_rows(tmp_path)
    assert commands == []
    if not transition:
        assert rows == [] and any("PACKAGE_PREVIEW_MISMATCH" in str(item) for item in actions)
        return
    [(_row, details)] = rows
    assert _transition(details) == {
        "transition_class": "PHS2_LOCAL", "transition_reasons": ["PACKAGE_PREVIEW_MISMATCH"],
        "transition_duplicate": False,
    }


def test_f3_source_refresh_refusal_keeps_a_submitted_package_out_of_local(tmp_path, monkeypatch):
    module = load_label_match_module()
    app, actions, _clock = _b1_app(module, tmp_path, monkeypatch)
    app._legacy_label_transition_enabled = True
    app.current_set_info.pop("package_source_snapshot", None)
    submitted_row = {"set_id": "b1-first", "status": "UNKNOWN"}
    monkeypatch.setattr(app.package_outbox, "get_by_set_id",
                        lambda set_id: submitted_row if set_id == "b1-first" else None)
    app.ui_lane = object()
    submitted = []
    app._submit_ui_lane_task = lambda **task: submitted.append(task) or SimpleNamespace(accepted=True)
    app._publish_submission_block = lambda error: actions.append("preflight:" + str(error))

    def refuse(*_args, **_kwargs):
        raise module.PackageLogisticsError("PACKAGE_PREVIEW_MISMATCH")

    app._resolve_central_phs2_seal_for_exchange = refuse
    try:
        assert app._begin_central_package_submission()
        [task] = submitted
        task["finish"](task["work"]())
    finally:
        _b1_close(app.data_manager)
    assert _b1_rows(tmp_path)[1] == []  # no local completion row
    assert app.current_set_info.get("transition_class") != "PHS2_LOCAL"
    assert any("PACKAGE_PREVIEW_MISMATCH" in str(item) for item in actions)


def test_emitted_transition_reasons_are_upper_case_codes(tmp_path, monkeypatch):
    import re

    module = load_label_match_module()
    app, _ = _packaging_app(module, tmp_path, monkeypatch, registered=True, transition=True)
    try:
        for value in [BND_LABEL + "|BND=", *_legacy_set(110)[1:]]:
            _scan(module, app, value)
    finally:
        _close(app)
    [(_row, details)] = _events(tmp_path, "TRAY_COMPLETE")
    assert details["transition_reasons"]
    assert all(re.fullmatch(r"[A-Z0-9_]+", code) for code in details["transition_reasons"])


def test_local_f3_decision_never_becomes_central_on_a_flush_retry(tmp_path, monkeypatch):
    module = load_label_match_module()
    app, actions, clock = _b1_app(module, tmp_path, monkeypatch)
    app._legacy_label_transition_enabled = True
    real_draft = module._label_match_package_draft
    draft_calls = []

    def draft(*args, **kwargs):
        draft_calls.append(1)
        if len(draft_calls) == 1:
            raise module.OperationLeaseError("OPERATION_LEASE_REQUIRED", "first lookup failure")
        return real_draft(*args, **kwargs)

    monkeypatch.setattr(module, "_label_match_package_draft", draft)
    real_flush = app._flush_data_manager_if_supported
    glitched = []

    def flush(timeout=5):
        real_flush(timeout=timeout)
        if not glitched and _b1_rows(tmp_path)[1]:
            glitched.append(1)
            raise TimeoutError("completion append succeeded, flush status unavailable")

    app._flush_data_manager_if_supported = flush
    try:
        app._begin_central_package_submission()
        commands, first_rows = _b1_rows(tmp_path)
        assert commands == [] and first_rows[0][1]["transition_class"] == "PHS2_LOCAL"
        clock.instant += timedelta(seconds=3)
        app._finalize_set(app.Results.PASS)  # operator retry; the draft would pass now
        app.data_manager.flush(timeout=5)
        commands, rows = _b1_rows(tmp_path)
        assert rows == first_rows
        assert commands == [], "a pinned PHS2_LOCAL completion stays outside the package outbox"
    finally:
        _b1_close(app.data_manager)


@pytest.mark.parametrize("field,bad_value", [
    ("transition_class", []), ("transition_reasons", "free text"), ("transition_duplicate", "false"),
])
def test_current_state_transition_fields_keep_their_types(field, bad_value):
    from label_recovery_schema import validate_recovery_record

    state = {"current_set_info": {"id": "set-type-check", "raw": [MASTER], field: bad_value}}
    assert validate_recovery_record("current", state) is False
    good = {"current_set_info": {"id": "set-type-check", "raw": [MASTER],
                                 "transition_class": "LEGACY", "transition_reasons": ["QT_INVALID"],
                                 "transition_duplicate": False}}
    assert validate_recovery_record("current", good) is True


@pytest.mark.parametrize("live_claim", [False, True])
def test_transition_capture_cancellation_preserves_an_unsafe_state(tmp_path, monkeypatch, live_claim):
    from tests.test_deferred_intent_capture import _capture, _row, _store

    module = load_label_match_module()
    db_path, outbox, store = _store(tmp_path)
    captured = _capture(store, set_id="SET-TRANSITION-CAPTURE", scan=PHS2_LABEL)
    if live_claim:
        assert store.claim_validation(captured.intent_id, worker_id="still-validating")
    before = _row(db_path, captured.intent_id)
    app, _ = _packaging_app(module, tmp_path, monkeypatch, registered=True, transition=True)
    app.deferred_intent_capture = store
    app.package_outbox = outbox
    app.current_set_info.update(id=before["local_work_identity"], deferred_intent_id=captured.intent_id)
    app._operation_lease_request_context = lambda scan: (store.binding.authority_scope_id, "")
    try:
        accepted = app._transition_accept_blocked_phs2(
            module.PackageLogisticsError("read-only source unavailable"), PHS2_LABEL, MASTER)
        after = _row(db_path, captured.intent_id)
        if live_claim:
            assert not accepted and before == after and app.current_set_info["raw"] == []
        else:
            assert accepted and after["state"] == "CANCELLED"
            assert app.current_set_info["transition_class"] == "PHS2_LOCAL"
            assert "deferred_intent_id" not in app.current_set_info
            second = _capture(store, set_id="SET-NEXT", scan=PHS2_LABEL + "-NEXT")
            assert store.next_validation_candidate() == second.intent_id
        assert _outbox_rows(tmp_path) == []
    finally:
        _close(app)


def test_documented_powershell_measurement_counts_a_real_csv(tmp_path, monkeypatch):
    import re
    import subprocess

    module = load_label_match_module()
    app, _ = _packaging_app(module, tmp_path, monkeypatch, registered=True, transition=True)
    monkeypatch.setattr(module.messagebox, "askyesno", lambda *a, **k: True)
    monkeypatch.setattr(module.messagebox, "showinfo", lambda *a, **k: None)
    try:
        for value in _legacy_set(114):
            _scan(module, app, value)
        app._cancel_completed_tray_by_label(MASTER)
        for value in _legacy_set(115)[:2]:
            _scan(module, app, value)
        app._finalize_set(app.Results.FAIL_MISMATCH, "wrong item")
    finally:
        _close(app)
    operations = (Path(__file__).resolve().parents[1] / "docs/spec/operations.md").read_text(encoding="utf-8")
    section = operations.split('<a id="legacy-label-transition"></a>', 1)[1].split("<a id=", 1)[0]
    [documented] = [block for block in re.findall(r"```powershell\n(.*?)```", section, re.S)
                    if "Group-Object" in block]
    script = documented.replace(
        r"$env:LOCALAPPDATA\KMTech\Label_Match\data\포장실작업이벤트로그_*_$d.csv",
        str(tmp_path / "포장실작업이벤트로그_*_$d.csv"),
    )
    script = ("[Console]::OutputEncoding = [Text.UTF8Encoding]::new($false)\n" + script.rstrip()
              + " | Select-Object Count,Name | ConvertTo-Json -Compress\n")
    path = tmp_path / "measurement.ps1"
    path.write_text(script, encoding="utf-8-sig")
    result = subprocess.run(["powershell", "-NoProfile", "-NonInteractive", "-File", str(path)],
                            capture_output=True, text=True, encoding="utf-8", timeout=120)
    assert result.returncode == 0, result.stderr
    measured = json.loads(result.stdout.strip())
    # The documented count includes the later-cancelled and the failed row.
    assert measured["Count"] == 2 and measured["Name"].startswith("LEGACY")


def _transition_relay_batch(tmp_path, rows):
    from tests.test_direct_sync_push import enqueue_source_file_for_relay, make_credentials, make_manifest

    _manifest, manifest_path = make_manifest(tmp_path)
    path = tmp_path / "transition.csv"
    with path.open("w", encoding="utf-8-sig", newline="") as stream:
        writer = csv.writer(stream)
        writer.writerow(["timestamp", "worker_name", "event", "details"])
        for event, details in rows:
            writer.writerow(["2026-09-29T05:00:00", "worker", event, json.dumps(details)])
    return enqueue_source_file_for_relay(
        db_path=tmp_path / "relay.sqlite3", spool_dir=tmp_path / "spool",
        source_file_path=path, producer_manifest_path=manifest_path,
        credentials=make_credentials(),
    )


def _duplicate_row(**changes):
    """A duplicate completion row this PC (manifest source_host_id label-host-1) wrote."""

    details = {
        "set_id": "set-dup", "transition_class": "PHS2_LOCAL", "transition_reasons": ["DUPLICATE_LABEL"],
        "transition_duplicate": True, "transition_source_host_id": "label-host-1",
        "packaging_set_identity": "label_match|PC-LT|set-dup",
        "idempotency_key": label_transition.event_key("PC-LT", "set-dup", "TRAY_COMPLETE"),
    }
    details.update(changes)
    return "TRAY_COMPLETE", {key: value for key, value in details.items() if value is not None}


_DUPLICATE_ROW = _duplicate_row()


def _cancellation_row(event, **changes):
    """A transition cancellation row this PC (label-host-1) wrote."""

    details = {
        ("set_id" if event == "SET_DELETED" else "cancelled_set_id"): "set-dup",
        "affected_completed_packaging_set_identity": "label_match|PC-LT|set-dup",
        "idempotency_key": label_transition.cancellation_key("PC-LT", "set-dup"),
        "transition_source_host_id": "label-host-1",
    }
    details.update(changes)
    return event, {key: value for key, value in details.items() if value is not None}


@pytest.mark.parametrize(("rows", "reason", "accepted"), [
    pytest.param([_DUPLICATE_ROW], "TRANSITION_DUPLICATE_OBSERVED", True, id="duplicate-completion"),
    pytest.param([("APP_CLOSE", {"message": "closed"}), _DUPLICATE_ROW],
                 "TRANSITION_DUPLICATE_OBSERVED", True, id="lifecycle-and-duplicate"),
    pytest.param([_DUPLICATE_ROW], "NO_STAGE1_REDUCER", False, id="wrong-reason"),
    pytest.param([("TRAY_COMPLETE", {"set_id": "set-base"})], "TRANSITION_DUPLICATE_OBSERVED",
                 False, id="base-completion"),
    # sub-03 P1 (3): only a row this PC wrote takes its raw acknowledgement.
    pytest.param([_duplicate_row(transition_source_host_id="other-host")], "TRANSITION_DUPLICATE_OBSERVED",
                 False, id="other-host"),
    pytest.param([_duplicate_row(transition_source_host_id=None)], "TRANSITION_DUPLICATE_OBSERVED",
                 False, id="no-host"),
    # detail.source_host_id is Web's own lineage key, never this binding.
    pytest.param([_duplicate_row(transition_source_host_id=None, source_host_id="label-host-1")],
                 "TRANSITION_DUPLICATE_OBSERVED", False, id="web-lineage-key"),
    # The PC binding is transition_source_host_id (07:4x); Web judges the rest
    # of the row (08:50: set identity, set count, event ID derivation).
    pytest.param([_duplicate_row(packaging_set_identity="label_match|OTHER-PC|set-dup")],
                 "TRANSITION_DUPLICATE_OBSERVED", True, id="this-host-other-writer"),
    pytest.param([_duplicate_row(idempotency_key="LM-TRAY_COMPLETE-" + "a" * 40)],
                 "TRANSITION_DUPLICATE_OBSERVED", True, id="this-host-other-key"),
    pytest.param([_duplicate_row(idempotency_key=None)], "TRANSITION_DUPLICATE_OBSERVED", False,
                 id="completion-without-event-id"),
    pytest.param([_duplicate_row(
        transition_source_host_id="other-host", packaging_set_identity="label_match|OTHER-PC|set-dup",
        idempotency_key=label_transition.event_key("OTHER-PC", "set-dup", "TRAY_COMPLETE"),
    )], "TRANSITION_DUPLICATE_OBSERVED", False, id="other-pc"),
    # The published Web receipt table (w9webrecv RECEIPT-CODES.md, d928087).
    pytest.param([_duplicate_row(transition_duplicate=False, packaging_set_count="x")],
                 "TRANSITION_NOT_PROJECTABLE", True, id="completion-bad-set-count"),
    pytest.param([_duplicate_row(set_id=None, packaging_set_identity=None)],
                 "TRANSITION_NOT_PROJECTABLE", True, id="completion-without-set-identity"),
    pytest.param([_cancellation_row("SET_DELETED")], "TRANSITION_DUPLICATE_OBSERVED", True,
                 id="deletion-duplicate"),
    pytest.param([_cancellation_row("TRAY_COMPLETION_CANCELLED")], "TRANSITION_DUPLICATE_OBSERVED", True,
                 id="cancellation-duplicate"),
    pytest.param([_cancellation_row("SET_DELETED", set_id=None,
                                    affected_completed_packaging_set_identity=None)],
                 "TRANSITION_NOT_PROJECTABLE", True, id="deletion-without-set-identity"),
    pytest.param([_cancellation_row("TRAY_COMPLETION_CANCELLED", cancelled_set_id=None,
                                    affected_completed_packaging_set_identity=None)],
                 "TRANSITION_NOT_PROJECTABLE", True, id="cancellation-without-set-identity"),
    pytest.param([("APP_CLOSE", {"message": "closed"}), _cancellation_row("TRAY_COMPLETION_CANCELLED")],
                 "TRANSITION_NOT_PROJECTABLE", True, id="raw-and-cancellation"),
    pytest.param([_cancellation_row("SET_DELETED", transition_source_host_id="other-host")],
                 "TRANSITION_DUPLICATE_OBSERVED", False, id="cancellation-other-host"),
    pytest.param([_cancellation_row("SET_DELETED", idempotency_key=None)],
                 "TRANSITION_NOT_PROJECTABLE", False, id="cancellation-without-event-id"),
    # COORD-INBOX-0850: the five Web re-review combinations.
    pytest.param([_duplicate_row(transition_duplicate=False, packaging_set_count="1")],
                 "TRANSITION_NOT_PROJECTABLE", True, id="0850-completion-string-count"),
    pytest.param([_cancellation_row("SET_DELETED", idempotency_key="LM-CANCELLATION-" + "b" * 40)],
                 "TRANSITION_DUPLICATE_OBSERVED", True, id="0850-deletion-same-set-new-id"),
    pytest.param([_cancellation_row("TRAY_COMPLETION_CANCELLED", idempotency_key="LM-CANCELLATION-" + "c" * 40)],
                 "TRANSITION_DUPLICATE_OBSERVED", True, id="0850-cancellation-same-set-new-id"),
    pytest.param([_DUPLICATE_ROW], "TRANSITION_UNPUBLISHED_REASON", False, id="unpublished-reason"),
])
def test_relay_accepts_a_raw_transition_duplicate_receipt_only_exactly(
    tmp_path, monkeypatch, rows, reason, accepted,
):
    from collections import Counter

    import direct_sync_push
    from tests.test_direct_sync_push import (
        FakeResponse, FakeSession, RuntimePreparation, _raw_lifecycle_receipt, make_credentials,
    )

    # As tests/test_direct_sync_push.py isolates relay receipts from runtime leases.
    monkeypatch.setattr(direct_sync_push, "prepare_runtime_metadata",
                        lambda **kwargs: RuntimePreparation(metadata=dict(kwargs["metadata"])))
    monkeypatch.setattr(direct_sync_push, "client_runtime_lease_mode", lambda _credentials: "observe")
    drain_one_relay_batch = direct_sync_push.drain_one_relay_batch

    row = _transition_relay_batch(tmp_path, rows)
    names = [event for event, _details in rows]
    receipt = _raw_lifecycle_receipt(row, tuple(names))
    for entry in receipt["projection_observation"]["event_classifications"]:
        if entry["raw_event_name"] in {"TRAY_COMPLETE", "SET_DELETED", "TRAY_COMPLETION_CANCELLED"}:
            entry["raw_only_reason_code"] = reason
    assert sum(entry["count"] for entry in receipt["projection_observation"]["event_classifications"]) == len(rows)
    assert Counter(names) == Counter({entry["raw_event_name"]: entry["count"]
                                      for entry in receipt["projection_observation"]["event_classifications"]})
    result = drain_one_relay_batch(
        db_path=tmp_path / "relay.sqlite3", credentials=make_credentials(),
        session=FakeSession(FakeResponse(200, receipt)), status_dir=tmp_path / "status",
    )
    assert result.success is accepted
    if not accepted:
        assert result.error_code == "producer_projection_incomplete"


# sub-03 interim P1s (07:0x, 07:1x); ported from sub-03/test_recheck3.py.


class _PowerLoss(BaseException):
    """Stop a call at a persistence boundary without ending the process."""


def _captured_transition_app(case, module, tmp_path, monkeypatch, transition_class):
    app = _app_for_recovery(module, tmp_path, monkeypatch)
    app.deferred_intent_capture = case.open_capture()
    app.package_outbox = case.app.package_outbox
    app.package_logistics_client = case.app.package_logistics_client
    app._operation_lease_request_context = case.app._operation_lease_request_context
    app.current_set_info = copy.deepcopy(case.app.current_set_info)
    app.current_set_info.update(label_transition.fields(transition_class))
    app.current_set_info["start_time"] = datetime.now()
    return app


def _restored_app(case, module, tmp_path, monkeypatch):
    restored = _app_for_recovery(module, tmp_path, monkeypatch)
    restored.deferred_intent_capture = case.open_capture()
    restored.package_outbox = case.app.package_outbox
    restored._load_current_set_state()
    return restored


@pytest.mark.parametrize("failure", ["save-false", "interruption"])
def test_failed_local_decision_save_keeps_the_saved_set_and_its_capture(
    clock_case, tmp_path, monkeypatch, failure,
):
    import label_completion
    from tests.test_deferred_intent_capture import _row

    intent_id = _first_scan(clock_case)
    module = load_label_match_module()
    app = _captured_transition_app(clock_case, module, tmp_path, monkeypatch, "PHS2_CENTRAL")
    raw = list(app.current_set_info["raw"])
    path = Path(app._package_current_state_path())
    assert app._save_current_set_state()
    saved = path.read_bytes()

    def fail_save(_current):
        if failure == "interruption":
            raise _PowerLoss()
        return False

    try:
        try:
            decided = label_completion._decide_transition_local(
                app, app.current_set_info, "PHS2_LOCAL", ["PACKAGE_PREVIEW_MISMATCH"], False,
                persist_current_state=fail_save,
            )
        except _PowerLoss:
            decided = None
        assert not decided and path.read_bytes() == saved
        # The capture stays open until the decision is durable.
        assert _row(clock_case.database, intent_id)["state"] == "VALIDATED"
    finally:
        _close(app)
    restored = _restored_app(clock_case, module, tmp_path, monkeypatch)
    try:
        assert path.exists() and restored.current_set_info["raw"] == raw
    finally:
        _close(restored)


def test_recovery_keeps_a_pinned_local_set_whose_capture_was_closed(clock_case, tmp_path, monkeypatch):
    from tests.test_deferred_intent_capture import _row

    intent_id = _first_scan(clock_case)
    module = load_label_match_module()
    app = _captured_transition_app(clock_case, module, tmp_path, monkeypatch, "PHS2_LOCAL")
    raw = list(app.current_set_info["raw"])
    try:
        # A stop between closing the capture and the save that drops its id.
        assert app._save_current_set_state()
        app._cancel_deferred_capture_for_set(app.current_set_info)
        assert _row(clock_case.database, intent_id)["state"] == "CANCELLED"
    finally:
        _close(app)
    restored = _restored_app(clock_case, module, tmp_path, monkeypatch)
    try:
        assert Path(restored._package_current_state_path()).exists()
        assert restored.current_set_info["raw"] == raw
        assert restored.current_set_info["transition_class"] == "PHS2_LOCAL"
        assert "deferred_intent_id" not in restored.current_set_info
    finally:
        _close(restored)


def test_lane_marker_save_failure_writes_no_row_until_the_marker_is_durable(tmp_path, monkeypatch):
    """Ported from sub-04/test_recheck4_boundaries.py: a set pinned without the
    start marker (an older build's shape) must save its row marker first."""

    module = load_label_match_module()
    app, _actions, _clock = _b1_app(module, tmp_path, monkeypatch)
    app.current_set_info.update(label_transition.fields("PHS2_LOCAL", ["PACKAGE_TRANSPORT_UNAVAILABLE"]))
    app._legacy_label_transition_enabled = True
    app.ui_lane = object()
    pending = []
    app._submit_ui_lane_task = lambda **task: pending.append(task) or SimpleNamespace(accepted=True)
    real_save = app._persist_ui_lane_current_set_snapshot
    rejected = []

    def save(snapshot):
        if snapshot.get("transition_row_key"):
            rejected.append(True)
            return False
        return real_save(snapshot)

    app._persist_ui_lane_current_set_snapshot = save
    try:
        for _attempt in range(2):
            assert app._begin_central_package_submission()
            task = pending.pop(0)
            with pytest.raises(module.PackageLogisticsError):
                task["work"]()
            assert _b1_rows(tmp_path) == ([], [])
        assert len(rejected) == 2
        app._persist_ui_lane_current_set_snapshot = real_save
        assert app._begin_central_package_submission()
        task = pending.pop(0)
        task["finish"](task["work"]())
        commands, rows = _b1_rows(tmp_path)
        assert commands == [] and [details["transition_class"] for _row, details in rows] == ["PHS2_LOCAL"]
    finally:
        _b1_close(app.data_manager)


def test_declining_restore_closes_a_pinned_local_capture(clock_case, tmp_path, monkeypatch):
    """A stop after the local decision was saved but before its capture closed,
    then the operator declines the restore: the cycle stays local."""

    import label_completion
    from tests.test_deferred_intent_capture import _row

    intent_id = _first_scan(clock_case)
    module = load_label_match_module()
    app = _captured_transition_app(clock_case, module, tmp_path, monkeypatch, "PHS2_CENTRAL")
    assert app._save_current_set_state()

    def stop_before_close(_current):
        raise _PowerLoss()

    app._cancel_deferred_capture_for_set = stop_before_close
    try:
        with pytest.raises(_PowerLoss):
            label_completion._decide_transition_local(
                app, app.current_set_info, "PHS2_LOCAL", ["PACKAGE_TRANSPORT_UNAVAILABLE"], False,
            )
        assert _row(clock_case.database, intent_id)["state"] == "VALIDATED"
    finally:
        _close(app)
    restored = _app_for_recovery(module, tmp_path, monkeypatch)
    restored.deferred_intent_capture = clock_case.open_capture()
    restored.package_outbox = clock_case.app.package_outbox
    restored.package_logistics_client = clock_case.app.package_logistics_client
    restored._operation_lease_request_context = clock_case.app._operation_lease_request_context
    monkeypatch.setattr(module.messagebox, "askyesno", lambda *a, **k: False)  # declined
    attempts = []
    restored._prepare_deferred_intent_validation = lambda identity: attempts.append(identity)
    try:
        restored._load_current_set_state()
        assert not Path(restored._package_current_state_path()).exists()
        assert _row(clock_case.database, intent_id)["state"] == "CANCELLED"
        restored._build_deferred_validation_lane_task().work()
        assert attempts == [], "a declined local cycle never returns to central validation"
    finally:
        _close(restored)


def test_retry_of_a_completion_saved_before_the_marker_reuses_its_row(tmp_path, monkeypatch):
    """A set saved by a build without transition_row_key (3f4574a, the lab's
    preserved state) whose row was appended with an unknown flush result."""

    module = load_label_match_module()
    app, _ = _packaging_app(module, tmp_path, monkeypatch, registered=False, transition=False)
    del app.__dict__["_save_current_set_state"]  # the real current-state file
    real_flush = app._flush_data_manager_if_supported
    glitched = []

    def flush(timeout=5):
        real_flush(timeout=timeout)
        if not glitched and _events(tmp_path, "TRAY_COMPLETE"):
            glitched.append(True)
            raise TimeoutError("completion append succeeded, flush status unavailable")

    app._flush_data_manager_if_supported = flush
    try:
        for value in _legacy_set(321):
            _scan(module, app, value)
        first = _events(tmp_path, "TRAY_COMPLETE")
        assert len(first) == 1 and "transition_class" not in first[0][1]
        assert app.current_set_info["raw"] and "transition_row_key" not in app.current_set_info
    finally:
        _close(app)
    upgraded = _app_for_recovery(module, tmp_path, monkeypatch)  # registered, switch on
    try:
        upgraded._load_current_set_state()
        assert upgraded.current_set_info["raw"] == _legacy_set(321)
        upgraded._finalize_set(upgraded.Results.PASS)
        upgraded.data_manager.flush(timeout=5)
        assert _events(tmp_path, "TRAY_COMPLETE") == first  # the one row, unchanged
    finally:
        _close(upgraded)


@pytest.mark.parametrize("restart", [False, True])
def test_local_completion_retry_reuses_its_row_from_any_earlier_day(tmp_path, monkeypatch, restart):
    module = load_label_match_module()

    class Clock(datetime):
        instant = datetime(2026, 9, 27, 23, 59, 0)

        @classmethod
        def now(cls, tz=None):
            return cls.instant

    monkeypatch.setattr(module, "datetime", Clock)
    app = _app_for_recovery(module, tmp_path, monkeypatch)
    real_flush = app._flush_data_manager_if_supported
    glitched = []

    def flush(timeout=5):
        real_flush(timeout=timeout)
        if not glitched and _events(tmp_path, "TRAY_COMPLETE"):
            glitched.append(True)
            raise TimeoutError("completion append succeeded, flush status unavailable")

    try:
        for value in _legacy_set(301)[:4]:  # started 9/27
            _scan(module, app, value)
        Clock.instant = datetime(2026, 9, 28, 0, 1, 0)  # completed 9/28
        app._flush_data_manager_if_supported = flush
        _scan(module, app, f"{MASTER}-FINAL-LABEL-0301<GS>6D20260927")
        first = _events(tmp_path, "TRAY_COMPLETE")
        assert len(first) == 1 and app.current_set_info["raw"]
        Clock.instant = datetime(2026, 9, 29, 8, 0, 0)  # retried 9/29
        if restart:
            _close(app)
            app = _app_for_recovery(module, tmp_path, monkeypatch)
            app._load_current_set_state()
        app._finalize_set(app.Results.PASS)
        app.data_manager.flush(timeout=5)
        assert _events(tmp_path, "TRAY_COMPLETE") == first  # the same row and content
    finally:
        _close(app)


@pytest.mark.parametrize("cancel", ["SET_DELETED", "TRAY_COMPLETION_CANCELLED"])
def test_transition_rows_name_the_registered_pc_for_the_relay(tmp_path, monkeypatch, cancel):
    import direct_sync_push

    module = load_label_match_module()
    app, _ = _packaging_app(module, tmp_path, monkeypatch, registered=True, transition=True)
    app.package_logistics_client.config = SimpleNamespace(tls_ca_bundle_path="", source_host_id="label-host-1")
    monkeypatch.setattr(module.messagebox, "askyesno", lambda *a, **k: True)
    for name in ("showinfo", "showerror"):
        monkeypatch.setattr(module.messagebox, name, lambda *a, **k: None)
    try:
        for value in _legacy_set(311):
            _scan(module, app, value)
        _select_completed_rows(app)
        if cancel == "SET_DELETED":
            app._delete_selected_row()
        else:
            app._cancel_completed_tray_by_label(MASTER)
    finally:
        _close(app)

    [(completion, details)] = _events(tmp_path, "TRAY_COMPLETE")
    [(cancellation, cancelled)] = _events(tmp_path, cancel)
    assert details["transition_class"] == "LEGACY"
    for row, row_details in ((completion, details), (cancellation, cancelled)):
        assert row_details["transition_source_host_id"] == "label-host-1"
        assert "source_host_id" not in row_details
        assert direct_sync_push._is_this_pc_transition_row(row, "label-host-1")
        assert not direct_sync_push._is_this_pc_transition_row(row, "other-host")


@pytest.mark.parametrize("quantity", ["abc", "0", "-1", "", "2|QTY=0"])
def test_inspection_quantity_alias_is_checked_before_the_ledger(tmp_path, monkeypatch, quantity):
    module = load_label_match_module()
    raw = f"CLC=INSPECTION|ITEM={MASTER}|SPC=Product|PHS=1|BND=TRANSFER-ALIAS|QTY={quantity}"
    # The package flow reads QTY as QT.
    assert module._label_match_parse_new_format_fields(raw).get("QT", "") == quantity.split("QTY=")[-1]
    app, _ = _packaging_app(module, tmp_path, monkeypatch, registered=True, transition=True)
    try:
        for value in [raw, *_legacy_set(312)[1:]]:
            _scan(module, app, value)
    finally:
        _close(app)

    [(_row, details)] = _events(tmp_path, "TRAY_COMPLETE")
    assert details["transition_class"] == "PHS2_MALFORMED"
    assert "QT_INVALID" in details["transition_reasons"]
    assert _outbox_rows(tmp_path) == []


@pytest.mark.parametrize("encoded", [False, True], ids=["plain", "base64"])
@pytest.mark.parametrize("transition", [False, True])
def test_valid_inspection_name_alias_stays_central(tmp_path, monkeypatch, transition, encoded):
    module = load_label_match_module()
    raw = f"CLC=INSPECTION|ITEM={MASTER}|ITEM_NAME=Product|PHS=1|BND=TRANSFER-ALIAS|QT=4"
    if encoded:
        raw = base64.b64encode(raw.encode()).decode()
    app, _ = _packaging_app(module, tmp_path, monkeypatch, registered=True, transition=transition)
    try:
        for value in [raw, *_legacy_set(313)[1:]]:
            _scan(module, app, value)
    finally:
        _close(app)

    [(_row, details)] = _events(tmp_path, "TRAY_COMPLETE")
    assert details.get("transition_class") == ("PHS2_CENTRAL" if transition else None)
    assert len(_outbox_rows(tmp_path)) == 1


@pytest.mark.parametrize(("raw", "reasons"), [
    pytest.param(f"CLC=INSPECTION|ITEM={MASTER}|ITEM_NAME=Product|PHS=1|BND=B-1|QT=4", (), id="item-name"),
    pytest.param(f"CLC=INSPECTION|ITEM_CODE={MASTER}|PHS=1|BND=B-1|QTY=4", (), id="item-code-qty"),
    pytest.param(f"CLC=INSPECTION|ITEM={MASTER}|SPC=P|PHASE=1|BND=B-1|QT=4", (), id="phase"),
    pytest.param(f"CLC=INSPECTION|ITEM={MASTER}|SPC=P|PHS=1|BND=B-1|QT=4|QTY=4",
                 ("DUPLICATE_KEY",), id="qt-and-qty"),
    pytest.param(f"CLC=INSPECTION|ITEM={MASTER}|SPC=P|PHS=1|BND=B-1|QTY=",
                 ("PHS_EMPTY", "QT_INVALID"), id="empty-qty"),
    pytest.param(f"CLC=INSPECTION|ITEM={MASTER}|SPC=P|PHASE=1|PHASE=2|BND=B-1|QT=4",
                 ("DUPLICATE_KEY",), id="repeated-phase"),
    pytest.param(f"CLC=INSPECTION|ITEM={MASTER}|ITEM_NAME=P|ITEM_NAME=Q|PHS=1|BND=B-1|QT=4",
                 ("DUPLICATE_KEY",), id="repeated-item-name"),
    # The carrier parser reads no item from ITEM without CLC=INSPECTION.
    pytest.param(f"ITEM={MASTER}|SPC=P|PHS=1|BND=B-1|QT=4", ("FORMAT_INVALID",), id="no-clc"),
])
def test_structured_label_aliases_are_judged_as_the_carrier_parser_reads_them(raw, reasons):
    from carrier_identity_port import parse_legacy_fields

    shape, found, item = label_transition.classify_start_label(raw, parse_sealed=lambda _raw: None)
    assert (shape, found, item) == ("MALFORMED" if reasons else "STRUCTURED", reasons, MASTER)
    if not reasons:
        assert parse_legacy_fields(raw)["CLC"] == MASTER  # the package flow reads each one


def test_bnd_label_the_carrier_parser_cannot_read_completes_as_malformed(tmp_path, monkeypatch):
    module = load_label_match_module()
    raw = f"ITEM={MASTER}|SPC=Product|PHS=1|BND=T-ALIAS|QT=4"
    assert module._label_match_parse_new_format_fields(raw) is None
    app, _ = _packaging_app(module, tmp_path, monkeypatch, registered=True, transition=True)
    try:
        for value in [raw, *_legacy_set(306)[1:]]:
            _scan(module, app, value)
    finally:
        _close(app)

    assert app.errors == []
    [(_row, details)] = _events(tmp_path, "TRAY_COMPLETE")
    assert _transition(details) == {
        "transition_class": "PHS2_MALFORMED", "transition_reasons": ["FORMAT_INVALID"],
        "transition_duplicate": False,
    }
    assert _outbox_rows(tmp_path) == []


def test_central_completion_retry_waits_for_its_queued_row(tmp_path, monkeypatch):
    module = load_label_match_module()
    app, _actions, clock = _b1_app(module, tmp_path, monkeypatch)
    app._legacy_label_transition_enabled = True
    entered, release = threading.Event(), threading.Event()
    real_open = app.data_manager._open_file

    class HeldWriter:
        def __init__(self, handle):
            self.handle = handle

        def __enter__(self):
            self.handle.__enter__()
            return self

        def __exit__(self, *args):
            return self.handle.__exit__(*args)

        def __getattr__(self, name):
            return getattr(self.handle, name)

        def write(self, value):
            if "TRAY_COMPLETE" in value:
                entered.set()
                assert release.wait(10), "the test did not release its writer barrier"
            return self.handle.write(value)

    def open_file(path, mode, **kwargs):
        handle = real_open(path, mode, **kwargs)
        return HeldWriter(handle) if mode == "a" else handle

    app.data_manager._open_file = open_file
    appended, timed_out = [], []
    real_log_event = app.data_manager.log_event
    real_flush = app._flush_data_manager_if_supported

    def log_event(event, *args, **kwargs):
        if "TRAY_COMPLETE" in str(event):
            appended.append(event)
        return real_log_event(event, *args, **kwargs)

    def flush(timeout=5):
        if appended and not timed_out:  # the completion row is still in the writer queue
            timed_out.append(True)
            assert entered.wait(5)
            raise TimeoutError("flush timed out with the completion row still queued")
        if timed_out:
            release.set()
        return real_flush(timeout=timeout)

    app.data_manager.log_event = log_event
    app._flush_data_manager_if_supported = flush
    try:
        app._begin_central_package_submission()
        assert entered.is_set() and _b1_rows(tmp_path)[1] == []
        clock.instant += timedelta(seconds=2)
        app._finalize_set(app.Results.PASS)  # operator retry
        app.data_manager.flush(timeout=5)
        [(_row, details)] = _b1_rows(tmp_path)[1]
        assert details["transition_class"] == "PHS2_CENTRAL"
    finally:
        release.set()
        _b1_close(app.data_manager)


# The server's LabelMatch stream catalog (w9webrecv 6e27c99) without its three
# business events; the server answers each with RAW_LEGITIMATE/NO_STAGE1_REDUCER.
_SERVER_RAW_ONLY_EVENTS = (
    "APP_CLOSE", "APP_START", "BASE64_DECODED", "ERROR_INPUT", "ERROR_MISMATCH", "LABEL_MATCHED",
    "PACKAGING_WAITING_OBSERVED", "PHS_LABEL_ACTIVE_RESOLVED", "PHS_LABEL_EXCHANGE_RESULT",
    "PHS_RECONCILIATION_EXCHANGE_RESULT", "PHS_REPLACEMENT_WAITING_MARKED", "POST_REVIEW_REQUIRED",
    "SCAN_ATTEMPT", "SCAN_OK", "SEALED_TRANSFER_EXCHANGE_ACKED", "SEALED_TRANSFER_EXCHANGE_APPLIED",
    "SET_CANCELLED", "SET_RESTORED", "SHIPPING_WAITING_OBSERVED", "UI_ERROR",
)


@pytest.mark.parametrize("event", [
    *_SERVER_RAW_ONLY_EVENTS, "TRAY_COMPLETE", "SET_DELETED", "TRAY_COMPLETION_CANCELLED",
])
def test_relay_takes_a_raw_receipt_for_every_server_raw_only_event(tmp_path, monkeypatch, event):
    import direct_sync_push
    from tests.test_direct_sync_push import (
        FakeResponse, FakeSession, RuntimePreparation, _raw_lifecycle_receipt, make_credentials,
    )

    monkeypatch.setattr(direct_sync_push, "prepare_runtime_metadata",
                        lambda **kwargs: RuntimePreparation(metadata=dict(kwargs["metadata"])))
    monkeypatch.setattr(direct_sync_push, "client_runtime_lease_mode", lambda _credentials: "observe")
    row = _transition_relay_batch(tmp_path, [(event, {"set_id": "set-raw"})])
    result = direct_sync_push.drain_one_relay_batch(
        db_path=tmp_path / "relay.sqlite3", credentials=make_credentials(),
        session=FakeSession(FakeResponse(200, _raw_lifecycle_receipt(row, (event,)))),
        status_dir=tmp_path / "status",
    )
    # A business event never takes a raw-only acknowledgement.
    assert result.success is (event in _SERVER_RAW_ONLY_EVENTS)
