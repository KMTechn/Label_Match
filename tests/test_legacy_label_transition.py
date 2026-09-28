"""Administrator transition mode: every set is accepted and classified.

Local storage (event CSV, package outbox) is real; the central client may only
expose its read-only config, so any central call fails the test.
"""

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
    calls = []

    def flush(timeout=5.0):
        real_flush(app, timeout=timeout)
        calls.append(timeout)
        if len(calls) == 2:  # right after the TRAY_COMPLETE append
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
