import base64
import csv
import importlib.util
import json
import queue
from collections import defaultdict
from datetime import datetime
from pathlib import Path

import pytest


def load_label_match_module():
    module_path = Path(__file__).resolve().parents[1] / "Label_Match.py"
    spec = importlib.util.spec_from_file_location("label_match_app_for_tests", module_path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_new_format_label_requires_non_empty_required_fields():
    module = load_label_match_module()
    parse = module.Label_Match._parse_new_format_label

    assert parse(None, "CLC=AAA2270730100|SPC=Product|PHS=1") == {
        "CLC": "AAA2270730100",
        "SPC": "Product",
        "PHS": "1",
    }
    assert parse(None, "CLC= |SPC=Product|PHS=1") is None
    assert parse(None, "CLC=AAA2270730100|SPC= |PHS=1") is None
    assert parse(None, "CLC=AAA2270730100|SPC=Product|PHS= ") is None
    assert parse(None, "CLC=AAA2270730100|SPC=Product") is None


def test_extract_production_date_accepts_real_dates_only():
    module = load_label_match_module()
    extract = module.Label_Match._extract_production_date

    assert extract(None, "FINAL_LABEL\x1D6D20260228") == "2026-02-28"
    assert extract(None, "FINAL_LABEL\x1D6D20240229") == "2024-02-29"
    assert extract(None, "FINAL_LABEL\x1D6D20260231") is None
    assert extract(None, "FINAL_LABEL\x1D6D20261301") is None
    assert extract(None, "FINAL_LABEL\x1D6D2026AB01") is None
    assert extract(None, "FINAL_LABEL_WITHOUT_DATE") is None


def test_enriched_tray_complete_preserves_label_match_contract():
    module = load_label_match_module()

    enriched = module._enrich_label_match_event(
        "TRAY_COMPLETE",
        {
            "set_id": "set-1",
            "scanned_product_barcodes": [
                "CLC=AAA2270730100|SPC=Product|PHS=1",
                "PRODUCT_AAA2270730100_1",
                "PRODUCT_AAA2270730100_2",
                "PRODUCT_AAA2270730100_3",
                "FINAL_LABEL_AAA2270730100\x1D6D20260228",
            ],
            "parsed_product_barcodes": ["AAA2270730100"] * 5,
            "final_result": "통과",
        },
        "LABEL-PC01",
    )

    assert enriched["source_system"] == "label_match"
    assert enriched["source_transport_or_dataset"] == "legacy_packaging_csv"
    assert enriched["dispatch_key"] == "label_match|legacy_packaging_csv|TRAY_COMPLETE"
    assert enriched["packaging_set_identity"] == "label_match|LABEL-PC01|set-1"
    assert enriched["final_result"] == "통과"
    assert enriched["scan_contract_version"] == "label_match_current_v1"
    assert enriched["quantity_basis"] == "PACKAGING_SET"
    assert enriched["packaging_set_count"] == 1
    assert enriched["downstream_count_excluded"] is False
    assert enriched["product_sample_barcodes"] == [
        "PRODUCT_AAA2270730100_1",
        "PRODUCT_AAA2270730100_2",
        "PRODUCT_AAA2270730100_3",
    ]


def test_partial_manual_tray_complete_is_excluded_from_downstream_set_count():
    module = load_label_match_module()

    enriched = module._enrich_label_match_event(
        "TRAY_COMPLETE",
        {
            "set_id": "partial-set",
            "scanned_product_barcodes": ["MASTER1", "PRODUCT_MASTER1_1"],
            "parsed_product_barcodes": ["MASTER1", "MASTER1"],
            "final_result": "통과",
            "is_partial_submission": True,
        },
        "LABEL-PC01",
    )

    assert enriched["quantity_basis"] == "PARTIAL_SUBMISSION"
    assert enriched["measure_code"] == "PACKAGING_SET_COUNT"
    assert enriched["packaging_set_count"] == 0
    assert enriched["downstream_count_excluded"] is True
    assert enriched["downstream_count_exclusion_reason"] == "PARTIAL_MANUAL_COMPLETION"


def test_tray_complete_result_helper_prefers_explicit_result_with_legacy_fallback():
    module = load_label_match_module()

    assert module._label_match_tray_complete_result({
        "final_result": "통과",
        "has_error_or_reset": True,
    }) == "통과"
    assert module._label_match_tray_complete_passed({
        "final_result": "통과",
        "has_error_or_reset": True,
    }) is True
    assert module._label_match_tray_complete_result({
        "final_result": "입력오류",
        "has_error_or_reset": True,
    }) == "입력오류"
    assert module._label_match_tray_complete_passed({
        "final_result": "입력오류",
        "has_error_or_reset": True,
    }) is False
    assert module._label_match_tray_complete_result({"has_error_or_reset": False}) == "통과"
    assert module._label_match_tray_complete_result({"has_error_or_reset": True}) == "불일치"


def test_manual_complete_policy_requires_clean_partial_product_scan():
    module = load_label_match_module()
    reason = module._label_match_manual_complete_block_reason
    allowed = module._label_match_manual_complete_allowed

    assert reason({"raw": []}) == "manual_complete_requires_product_scan"
    assert reason({"raw": ["MASTER"]}) == "manual_complete_requires_product_scan"
    assert allowed({"raw": ["MASTER", "PRODUCT_1"], "error_count": 0, "has_error_or_reset": False}) is True
    assert allowed({"raw": ["MASTER", "PRODUCT_1", "PRODUCT_2", "PRODUCT_3"]}) is True
    assert reason({"raw": ["MASTER", "P1", "P2", "P3", "FINAL"]}) == "manual_complete_only_for_partial_sets"
    assert reason({"raw": ["MASTER", "PRODUCT_1"], "has_error_or_reset": True}) == "manual_complete_blocked_after_error"
    assert reason({"raw": ["MASTER", "PRODUCT_1"], "error_count": 1}) == "manual_complete_blocked_after_error"


def test_history_display_keeps_master_label_full_text_when_narrow():
    module = load_label_match_module()
    app = object.__new__(module.Label_Match)
    app.ui_profile_name = "small"
    app.tree_font_size = 13

    class NarrowHistoryTree:
        def column(self, column, option=None, **kwargs):
            if option == "width":
                return {"Input1": 70, "Input2": 70}.get(column, 90)
            return None

    app.history_tree = NarrowHistoryTree()
    product_barcode = "PRODUCT-AAA2270730100-001-LONG-BARCODE"

    display_values = module.Label_Match._history_values_for_display(app, (
        1,
        "AAA2270730100",
        product_barcode,
        "",
        "",
        "",
        "통과",
        "08:00:00",
    ))

    assert display_values[1] == "AAA2270730100"
    assert display_values[2] != product_barcode
    assert "..." in display_values[2]


def test_partial_manual_pass_updates_duplicates_without_summary_count():
    module = load_label_match_module()
    app = object.__new__(module.Label_Match)
    app.Results = module.Label_Match.Results
    app.current_set_info = {
        "id": "partial-set",
        "raw": ["MASTER1", "PRODUCT_MASTER1_1", "PRODUCT_MASTER1_2"],
        "parsed": ["MASTER1", "MASTER1", "MASTER1"],
        "start_time": datetime(2026, 6, 22, 10, 0, 0),
        "error_count": 0,
        "has_error_or_reset": False,
        "phase": "-",
        "item_name_override": None,
        "production_date": None,
    }
    app.items_data = {"MASTER1": {"Item Name": "Item 1", "Spec": "Spec"}}
    app.scan_count = defaultdict(lambda: defaultdict(int))
    app.global_scanned_set = set()
    app.set_details_map = {}
    app.data_manager = _FakeLoggingDataManager()
    app.history_tree = _FakeHistoryTree()
    app.save_status_label = _FakeLabel()
    app.is_running_simulation = True
    app.initialized_successfully = True
    app._play_sound = lambda sound_key: None
    app._update_summary_tree = lambda: None
    app._reset_current_set = lambda **kwargs: None
    app.after = lambda delay, callback: None

    module.Label_Match._finalize_set(app, app.Results.PASS, is_manual_complete=True)

    assert app.scan_count == {}
    assert app.global_scanned_set == {"PRODUCT_MASTER1_1", "PRODUCT_MASTER1_2"}
    assert app.data_manager.events[0][0] == module.Label_Match.Events.TRAY_COMPLETE
    details = app.data_manager.events[0][1]
    assert details["is_partial_submission"] is True
    assert details["final_result"] == "통과"
    assert details["production_date"] is None
    assert details["set_id"] in app.set_details_map


def test_legacy_pass_normalizes_missing_phase_to_dash():
    module = load_label_match_module()
    app = object.__new__(module.Label_Match)
    app.Results = module.Label_Match.Results
    app.Events = module.Label_Match.Events
    app.current_set_info = {
        "id": "legacy-set",
        "raw": [
            "AAA2270730100",
            "PROD-AAA2270730100-1",
            "PROD-AAA2270730100-2",
            "PROD-AAA2270730100-3",
            "FINAL-AAA2270730100\x1D6D20260623",
        ],
        "parsed": ["AAA2270730100"] * 5,
        "start_time": datetime(2026, 6, 23, 10, 0, 0),
        "error_count": 0,
        "has_error_or_reset": False,
        "phase": None,
        "item_name_override": None,
        "production_date": "2026-06-23",
    }
    app.items_data = {"AAA2270730100": {"Item Name": "L07", "Spec": "KMC_LHD"}}
    app.scan_count = defaultdict(lambda: defaultdict(int))
    app.global_scanned_set = set()
    app.set_details_map = {}
    app.history_row_details_map = {}
    app.data_manager = _FakeLoggingDataManager()
    app.history_tree = _FakeHistoryTree()
    app.save_status_label = _FakeLabel()
    app.is_running_simulation = True
    app.initialized_successfully = True
    app.run_tests = True
    app._play_sound = lambda sound_key: None
    app._update_summary_tree = lambda: None
    app._reset_current_set = lambda **kwargs: None
    app.after = lambda delay, callback: None

    module.Label_Match._finalize_set(app, app.Results.PASS)

    assert app.scan_count["2026-06-23"][("AAA2270730100", "-")] == 1
    details = app.data_manager.events[0][1]
    assert details["phase"] == "-"


def test_mark_current_set_error_persists_active_set_only():
    module = load_label_match_module()
    app = object.__new__(module.Label_Match)
    app.current_set_info = {
        "raw": ["MASTER"],
        "parsed": ["MASTER"],
        "error_count": 0,
        "has_error_or_reset": False,
    }
    saved = []
    app._update_manual_complete_button_state = lambda: None
    app._save_current_set_state = lambda: saved.append(dict(app.current_set_info))

    module.Label_Match._mark_current_set_error(app)

    assert app.current_set_info["error_count"] == 1
    assert app.current_set_info["has_error_or_reset"] is True
    assert saved == [{
        "raw": ["MASTER"],
        "parsed": ["MASTER"],
        "error_count": 1,
        "has_error_or_reset": True,
    }]

    app.current_set_info = {"raw": [], "parsed": [], "error_count": 0, "has_error_or_reset": False}
    module.Label_Match._mark_current_set_error(app)

    assert len(saved) == 1


def test_first_scan_error_event_and_finalization_share_set_id():
    module = load_label_match_module()
    app = object.__new__(module.Label_Match)
    app.current_set_info = {
        "id": None,
        "raw": [],
        "parsed": [],
        "start_time": None,
        "error_count": 0,
        "has_error_or_reset": False,
        "phase": None,
        "item_name_override": None,
        "production_date": None,
    }
    app.items_data = {}
    app.data_manager = _FakeLoggingDataManager()
    app.history_tree = _FakeHistoryTree()
    app.save_status_label = _FakeLabel()
    app.status_label = _FakeLabel()
    app.is_running_simulation = True
    app.run_tests = True
    app.initialized_successfully = True
    app.update_big_display = lambda text, color="": None
    app._update_manual_complete_button_state = lambda: None
    app._play_sound = lambda sound_key: None
    app._update_summary_tree = lambda: None
    app._reset_current_set = lambda **kwargs: None
    app.after = lambda delay, callback: None

    module.Label_Match._handle_input_error(app, "BAD-MASTER", reason="bad")

    assert [event for event, _details in app.data_manager.events] == [
        module.Label_Match.Events.ERROR_INPUT,
        module.Label_Match.Events.TRAY_COMPLETE,
    ]
    error_details = app.data_manager.events[0][1]
    final_details = app.data_manager.events[1][1]
    assert error_details["set_id"]
    assert error_details["set_id"] == final_details["set_id"]
    assert final_details["final_result"] == "입력오류"


def test_data_manager_close_flushes_queue_using_event_timestamp_date(tmp_path):
    module = load_label_match_module()
    manager = module.DataManager(str(tmp_path), "포장실", "worker-a", "PC01")
    manager.log_queue.put(["2026-06-22T23:59:59", "worker-a", "TEST_EVENT", "{}"])

    manager.close()

    log_path = tmp_path / "포장실작업이벤트로그_PC01_20260622.csv"
    assert log_path.is_file()
    with log_path.open("r", encoding="utf-8-sig", newline="") as file:
        rows = list(csv.reader(file))
    assert rows[0] == ["timestamp", "worker_name", "event", "details"]
    assert rows[1] == ["2026-06-22T23:59:59", "worker-a", "TEST_EVENT", "{}"]
    assert manager.log_thread.is_alive() is False


def test_data_manager_close_timeout_raises_when_writer_does_not_stop():
    module = load_label_match_module()

    class NeverStopsThread:
        def __init__(self):
            self.join_timeout = None

        def is_alive(self):
            return True

        def join(self, timeout=None):
            self.join_timeout = timeout

    manager = object.__new__(module.DataManager)
    manager.log_queue = queue.Queue()
    manager.log_thread = NeverStopsThread()
    manager._close_lock = module.threading.Lock()
    manager._close_requested = False

    with pytest.raises(TimeoutError):
        module.DataManager.close(manager, timeout=0.01)

    assert manager.log_queue.get_nowait() is None
    assert manager.log_thread.join_timeout == 0.01


def test_data_manager_close_raises_writer_error(tmp_path, monkeypatch):
    module = load_label_match_module()

    def failing_open(*args, **kwargs):
        raise OSError("forced write failure")

    monkeypatch.setattr(module, "open", failing_open, raising=False)
    manager = module.DataManager(str(tmp_path), "포장실", "worker-a", "PC01")
    manager.log_event("TEST_EVENT", {"value": 1})

    with pytest.raises(RuntimeError, match="forced write failure"):
        manager.close(timeout=5.0)


def test_on_closing_replaces_closed_data_manager_after_close_failure(monkeypatch):
    module = load_label_match_module()
    app = object.__new__(module.Label_Match)
    failed_manager = _RecoverableCloseFailingDataManager()
    replacement_manager = _FakeLoggingDataManager()
    app.initialized_successfully = True
    app.is_running_simulation = False
    app.is_generating_test_logs = False
    app.run_tests = False
    app.is_blinking = True
    app.data_manager = failed_manager
    app._save_app_settings = lambda: (_ for _ in ()).throw(AssertionError("settings should not save"))
    app.destroy = lambda: (_ for _ in ()).throw(AssertionError("window should not close"))
    monkeypatch.setattr(module, "DataManager", lambda *args, **kwargs: replacement_manager)
    monkeypatch.setattr(module.messagebox, "askokcancel", lambda *args, **kwargs: True)
    monkeypatch.setattr(module.messagebox, "showerror", lambda *args, **kwargs: None)

    module.Label_Match.on_closing(app)

    assert failed_manager.closed is True
    assert app.data_manager is replacement_manager
    app.data_manager.log_event("AFTER_FAILED_CLOSE", {})
    assert app.data_manager.events[-1][0] == "AFTER_FAILED_CLOSE"


def test_on_closing_does_not_destroy_when_log_close_fails(monkeypatch):
    module = load_label_match_module()
    app = object.__new__(module.Label_Match)
    app.initialized_successfully = True
    app.is_running_simulation = False
    app.is_generating_test_logs = False
    app.run_tests = False
    app.is_blinking = True
    app.data_manager = _CloseFailingDataManager()
    app._save_app_settings = lambda: (_ for _ in ()).throw(AssertionError("settings should not save"))
    app.destroy = lambda: (_ for _ in ()).throw(AssertionError("window should not close"))
    monkeypatch.setattr(module.messagebox, "askokcancel", lambda *args, **kwargs: True)
    errors = []
    monkeypatch.setattr(module.messagebox, "showerror", lambda *args, **kwargs: errors.append((args, kwargs)))

    module.Label_Match.on_closing(app)

    assert errors


def test_settings_save_replaces_closed_data_manager_after_close_failure(monkeypatch):
    module = load_label_match_module()
    app = object.__new__(module.Label_Match)
    failed_manager = _RecoverableCloseFailingDataManager()
    replacement_manager = _FakeLoggingDataManager()
    app.current_set_info = {"id": None}
    app.is_running_simulation = False
    app.is_generating_test_logs = False
    app.run_tests = False
    app.worker_name = "old-worker"
    app.save_directory = "C:\\Sync\\old-worker"
    app.data_manager = failed_manager
    app._save_app_settings = lambda: (_ for _ in ()).throw(AssertionError("settings should not save"))
    app._update_save_directory = lambda: (_ for _ in ()).throw(AssertionError("save directory should not update"))
    app.title = lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("title should not change"))
    window = _FakeWindow()
    monkeypatch.setattr(module, "DataManager", lambda *args, **kwargs: replacement_manager)
    monkeypatch.setattr(module.messagebox, "showerror", lambda *args, **kwargs: None)

    module.Label_Match._save_settings_and_close(app, window, "new-worker")

    assert app.worker_name == "old-worker"
    assert app.save_directory == "C:\\Sync\\old-worker"
    assert app.data_manager is replacement_manager
    assert window.destroyed is False


def test_settings_save_blocks_while_viewing_past_history(monkeypatch):
    module = load_label_match_module()
    app = object.__new__(module.Label_Match)
    app.current_set_info = {"id": None}
    app.is_running_simulation = False
    app.is_generating_test_logs = False
    app.history_view_updates_active_state = False
    app.history_load_pending = False
    app.history_active_load_pending = False
    app.run_tests = False
    app.worker_name = "old-worker"
    app.status_label = _FakeLabel()
    app.data_manager = _FailingDataManager()
    app._save_app_settings = lambda: (_ for _ in ()).throw(AssertionError("settings should not save"))
    app._update_save_directory = lambda: (_ for _ in ()).throw(AssertionError("save directory should not update"))
    window = _FakeWindow()
    warnings = []
    monkeypatch.setattr(module.messagebox, "showwarning", lambda *args, **kwargs: warnings.append((args, kwargs)))

    module.Label_Match._save_settings_and_close(app, window, "new-worker")

    assert app.worker_name == "old-worker"
    assert "과거 기록 조회 중" in app.status_label.kwargs["text"]
    assert warnings
    assert window.destroyed is False


def test_settings_save_blocks_while_history_load_is_pending(monkeypatch):
    module = load_label_match_module()
    app = object.__new__(module.Label_Match)
    app.current_set_info = {"id": None}
    app.is_running_simulation = False
    app.is_generating_test_logs = False
    app.history_view_updates_active_state = True
    app.history_load_pending = True
    app.history_active_load_pending = True
    app.run_tests = False
    app.worker_name = "old-worker"
    app.status_label = _FakeLabel()
    app.data_manager = _FailingDataManager()
    app._save_app_settings = lambda: (_ for _ in ()).throw(AssertionError("settings should not save"))
    app._update_save_directory = lambda: (_ for _ in ()).throw(AssertionError("save directory should not update"))
    window = _FakeWindow()
    warnings = []
    monkeypatch.setattr(module.messagebox, "showwarning", lambda *args, **kwargs: warnings.append((args, kwargs)))

    module.Label_Match._save_settings_and_close(app, window, "new-worker")

    assert app.worker_name == "old-worker"
    assert "오늘 기록을 불러오는 중" in app.status_label.kwargs["text"]
    assert warnings
    assert window.destroyed is False


def test_settings_save_blocks_while_active_history_load_flag_is_pending(monkeypatch):
    module = load_label_match_module()
    app = object.__new__(module.Label_Match)
    app.current_set_info = {"id": None}
    app.is_running_simulation = False
    app.is_generating_test_logs = False
    app.history_view_updates_active_state = True
    app.history_load_pending = False
    app.history_active_load_pending = True
    app.run_tests = False
    app.worker_name = "old-worker"
    app.status_label = _FakeLabel()
    app.data_manager = _FailingDataManager()
    app._save_app_settings = lambda: (_ for _ in ()).throw(AssertionError("settings should not save"))
    app._update_save_directory = lambda: (_ for _ in ()).throw(AssertionError("save directory should not update"))
    window = _FakeWindow()
    warnings = []
    monkeypatch.setattr(module.messagebox, "showwarning", lambda *args, **kwargs: warnings.append((args, kwargs)))

    module.Label_Match._save_settings_and_close(app, window, "new-worker")

    assert app.worker_name == "old-worker"
    assert "오늘 기록을 불러오는 중" in app.status_label.kwargs["text"]
    assert warnings
    assert window.destroyed is False


def test_settings_save_blocks_while_current_set_is_active(monkeypatch):
    module = load_label_match_module()
    app = object.__new__(module.Label_Match)
    app.current_set_info = {"id": "active-set"}
    app.is_running_simulation = False
    app.run_tests = False
    app.worker_name = "old-worker"
    app.data_manager = _FailingDataManager()
    app._save_app_settings = lambda: (_ for _ in ()).throw(AssertionError("settings should not save"))
    app._update_save_directory = lambda: (_ for _ in ()).throw(AssertionError("save directory should not update"))
    window = _FakeWindow()
    warnings = []
    monkeypatch.setattr(module.messagebox, "showwarning", lambda *args, **kwargs: warnings.append((args, kwargs)))

    module.Label_Match._save_settings_and_close(app, window, "new-worker")

    assert app.worker_name == "old-worker"
    assert warnings
    assert window.destroyed is False


def test_settings_save_does_not_persist_or_switch_worker_when_log_close_fails(monkeypatch):
    module = load_label_match_module()
    app = object.__new__(module.Label_Match)
    old_manager = _CloseFailingDataManager()
    app.current_set_info = {"id": None}
    app.is_running_simulation = False
    app.is_generating_test_logs = False
    app.run_tests = False
    app.worker_name = "old-worker"
    app.save_directory = "C:\\Sync\\old-worker"
    app.data_manager = old_manager
    app._save_app_settings = lambda: (_ for _ in ()).throw(AssertionError("settings should not save"))
    app._update_save_directory = lambda: (_ for _ in ()).throw(AssertionError("save directory should not update"))
    app.title = lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("title should not change"))
    window = _FakeWindow()
    errors = []
    monkeypatch.setattr(module.messagebox, "showerror", lambda *args, **kwargs: errors.append((args, kwargs)))

    module.Label_Match._save_settings_and_close(app, window, "new-worker")

    assert app.worker_name == "old-worker"
    assert app.save_directory == "C:\\Sync\\old-worker"
    assert app.data_manager is old_manager
    assert window.destroyed is False
    assert errors


def test_duplicate_index_helper_and_rebuild_preserve_shared_barcodes():
    module = load_label_match_module()
    passed_one = {
        "final_result": "통과",
        "scanned_product_barcodes": ["MASTER1", "SHARED_PRODUCT", "ONLY_ONE"],
    }
    passed_two = {
        "final_result": "통과",
        "scanned_product_barcodes": ["MASTER2", "SHARED_PRODUCT", "ONLY_TWO"],
    }
    unique_master = {
        "final_result": "통과",
        "is_unique_master_label": True,
        "scanned_product_barcodes": ["UNIQUE_MASTER", "UNIQUE_PRODUCT"],
    }
    failed = {
        "final_result": "입력오류",
        "scanned_product_barcodes": ["FAILED_MASTER", "FAILED_PRODUCT"],
    }

    assert module._label_match_duplicate_index_barcodes(unique_master) == {"UNIQUE_MASTER", "UNIQUE_PRODUCT"}
    assert module._label_match_duplicate_index_barcodes(failed) == set()

    app = object.__new__(module.Label_Match)
    app.set_details_map = {"one": passed_one, "two": passed_two, "failed": failed}
    module.Label_Match._rebuild_global_scanned_set_from_details(app)

    assert app.global_scanned_set == {"SHARED_PRODUCT", "ONLY_ONE", "ONLY_TWO"}

    del app.set_details_map["one"]
    module.Label_Match._rebuild_global_scanned_set_from_details(app)

    assert app.global_scanned_set == {"SHARED_PRODUCT", "ONLY_TWO"}


def test_new_format_unique_master_duplicate_blocks_base64_and_decoded_equivalents():
    module = load_label_match_module()
    decoded_master = "CLC=ITEM1|SPC=Product|PHS=A"
    base64_master = base64.b64encode(decoded_master.encode("utf-8")).decode("utf-8")
    completed_details = _completed_details(
        set_id="unique",
        master_code="ITEM1",
        end_time="2026-06-23T10:00:00",
        raw_scans=[base64_master, "UNIQUE_PRODUCT"],
        item_name_override="Product",
        phase="A",
    )
    app = object.__new__(module.Label_Match)
    app.current_set_info = {
        "id": None,
        "raw": [],
        "parsed": [],
        "error_count": 0,
        "has_error_or_reset": False,
        "phase": None,
        "item_name_override": None,
        "production_date": None,
    }
    app.entry = _FakeEntry(decoded_master)
    app.status_label = _FakeLabel()
    app.data_manager = _FakeLoggingDataManager()
    app.global_scanned_set = module._label_match_duplicate_index_barcodes(completed_details)
    app.history_view_updates_active_state = True
    app.history_active_load_pending = False
    app.run_tests = True
    app.is_blinking = False
    app.is_running_simulation = False
    app.initialized_successfully = True
    app.items_data = {}
    app.update_big_display = lambda *args, **kwargs: None
    app._update_manual_complete_button_state = lambda: None
    app._save_current_set_state = lambda: None

    module.Label_Match.process_input(app)

    assert app.current_set_info["raw"] == []
    assert "중복" in app.status_label.kwargs["text"]
    assert [event for event, _details in app.data_manager.events] == [
        module.Label_Match.Events.SCAN_ATTEMPT,
        module.Label_Match.Events.ERROR_INPUT,
    ]


def test_legacy_base64_new_format_without_metadata_is_indexed_as_unique_master():
    module = load_label_match_module()
    decoded_master = "CLC=ITEM1|SPC=Product|PHS=A"
    base64_master = base64.b64encode(decoded_master.encode("utf-8")).decode("utf-8")
    legacy_details = _completed_details(
        set_id="legacy",
        master_code="ITEM1",
        end_time="2026-06-23T10:00:00",
        raw_scans=[base64_master, "UNIQUE_PRODUCT"],
        phase="A",
    )

    indexed = module._label_match_duplicate_index_barcodes(legacy_details)

    assert module._label_match_first_scan_is_unique_master(legacy_details) is True
    assert base64_master in indexed
    assert decoded_master in indexed
    assert "UNIQUE_PRODUCT" in indexed


def test_new_format_master_identity_survives_finalize_and_history_reload(tmp_path):
    module = load_label_match_module()
    base64_master = "Q0xDPUlURU0xfFNQQz1Qcm9kdWN0fFBIUz1B"
    app = object.__new__(module.Label_Match)
    app.Results = module.Label_Match.Results
    app.current_set_info = {
        "id": "new-format-set",
        "raw": [base64_master, "PRODUCT_ITEM1_1", "PRODUCT_ITEM1_2", "FINAL_LABEL_ITEM1\x1D6D20260622"],
        "parsed": ["ITEM1", "ITEM1", "ITEM1", "ITEM1"],
        "start_time": datetime(2026, 6, 22, 10, 0, 0),
        "error_count": 0,
        "has_error_or_reset": False,
        "phase": "A",
        "item_name_override": "Product",
        "production_date": "2026-06-22",
    }
    app.items_data = {}
    app.scan_count = defaultdict(lambda: defaultdict(int))
    app.global_scanned_set = set()
    app.set_details_map = {}
    app.data_manager = _FakeLoggingDataManager()
    app.history_tree = _FakeHistoryTree()
    app.save_status_label = _FakeLabel()
    app.is_running_simulation = True
    app.initialized_successfully = True
    app._play_sound = lambda sound_key: None
    app._update_summary_tree = lambda: None
    app._reset_current_set = lambda **kwargs: None
    app.after = lambda delay, callback: None

    module.Label_Match._finalize_set(app, app.Results.PASS)
    details = app.data_manager.events[0][1]
    assert details["item_name_override"] == "Product"
    assert details["is_unique_master_label"] is True

    log_path = tmp_path / "events.csv"
    with log_path.open("w", newline="", encoding="utf-8-sig") as file:
        writer = csv.DictWriter(file, fieldnames=["timestamp", "worker_name", "event", "details"])
        writer.writeheader()
        writer.writerow(_event_row(module, "2026-06-22T10:01:00", details))

    result_queue = queue.Queue()
    reload_app = object.__new__(module.Label_Match)
    reload_app.data_manager = _FakeDataManager(log_path)

    module.Label_Match._async_load_history_task(reload_app, result_queue)
    result = result_queue.get_nowait()

    assert base64_master in result["global_scanned_set"]
    assert result["scan_count"]["2026-06-22"][("ITEM1", "A")] == 1


def test_history_reload_indexes_legacy_base64_new_format_without_metadata(tmp_path):
    module = load_label_match_module()
    decoded_master = "CLC=ITEM1|SPC=Product|PHS=A"
    base64_master = base64.b64encode(decoded_master.encode("utf-8")).decode("utf-8")
    details = _completed_details(
        set_id="legacy-new-format",
        master_code="ITEM1",
        end_time="2026-06-22T10:01:00",
        raw_scans=[base64_master, "PRODUCT_ITEM1_1"],
        phase="A",
    )
    log_path = tmp_path / "events.csv"
    with log_path.open("w", newline="", encoding="utf-8-sig") as file:
        writer = csv.DictWriter(file, fieldnames=["timestamp", "worker_name", "event", "details"])
        writer.writeheader()
        writer.writerow(_event_row(module, "2026-06-22T10:01:00", details))
    result_queue = queue.Queue()
    app = object.__new__(module.Label_Match)
    app.data_manager = _FakeDataManager(log_path)

    module.Label_Match._async_load_history_task(app, result_queue)
    result = result_queue.get_nowait()

    assert base64_master in result["global_scanned_set"]
    assert decoded_master in result["global_scanned_set"]


def test_history_rebuild_uses_final_result_for_display_counts_and_pass_map(tmp_path):
    module = load_label_match_module()
    log_path = tmp_path / "events.csv"
    rows = [
        _event_row(
            module,
            "2026-06-22T10:00:00",
            {
                "set_id": "set-pass-after-error",
                "item_code": "ITEM1",
                "master_label_code": "ITEM1",
                "production_date": "2026-06-22",
                "phase": "A",
                "has_error_or_reset": True,
                "final_result": "통과",
                "item_name_override": "Product",
                "end_time": "2026-06-22T10:00:00",
                "scanned_product_barcodes": [
                    "Q0xDPUlURU0xfFNQQz1Qcm9kdWN0fFBIUz1B",
                    "PRODUCT_ITEM1_1",
                    "PRODUCT_ITEM1_2",
                ],
                "parsed_product_barcodes": ["ITEM1", "ITEM1", "ITEM1"],
            },
        ),
        _event_row(
            module,
            "2026-06-22T10:01:00",
            {
                "set_id": "legacy-failed",
                "item_code": "ITEM2",
                "master_label_code": "ITEM2",
                "production_date": "2026-06-22",
                "phase": "B",
                "has_error_or_reset": True,
                "end_time": "2026-06-22T10:01:00",
                "scanned_product_barcodes": ["ITEM2", "PRODUCT_ITEM2_1"],
                "parsed_product_barcodes": ["ITEM2", "ITEM2"],
            },
        ),
        _event_row(
            module,
            "2026-06-22T10:02:00",
            {
                "set_id": "explicit-input-error",
                "item_code": "ITEM3",
                "master_label_code": "ITEM3",
                "production_date": "2026-06-22",
                "phase": "C",
                "has_error_or_reset": True,
                "final_result": "입력오류",
                "end_time": "2026-06-22T10:02:00",
                "scanned_product_barcodes": ["ITEM3", "PRODUCT_ITEM3_1"],
                "parsed_product_barcodes": ["ITEM3", "ITEM3"],
            },
        ),
    ]
    with log_path.open("w", newline="", encoding="utf-8-sig") as file:
        writer = csv.DictWriter(file, fieldnames=["timestamp", "worker_name", "event", "details"])
        writer.writeheader()
        writer.writerows(rows)

    result_queue = queue.Queue()
    app = object.__new__(module.Label_Match)
    app.data_manager = _FakeDataManager(log_path)

    module.Label_Match._async_load_history_task(app, result_queue)
    result = result_queue.get_nowait()

    by_set_id = {set_id: data for set_id, data in result["sorted_sets"]}
    assert by_set_id["set-pass-after-error"]["values"][6] == "통과"
    assert by_set_id["legacy-failed"]["values"][6] == "불일치"
    assert by_set_id["explicit-input-error"]["values"][6] == "입력오류"

    assert result["scan_count"]["2026-06-22"][("ITEM1", "A")] == 1
    assert ("ITEM2", "B") not in result["scan_count"]["2026-06-22"]
    assert ("ITEM3", "C") not in result["scan_count"]["2026-06-22"]
    assert result["set_details_map"].keys() == {"set-pass-after-error", "legacy-failed", "explicit-input-error"}
    assert "PRODUCT_ITEM1_1" in result["global_scanned_set"]
    assert "Q0xDPUlURU0xfFNQQz1Qcm9kdWN0fFBIUz1B" in result["global_scanned_set"]
    assert "PRODUCT_ITEM2_1" not in result["global_scanned_set"]
    assert "PRODUCT_ITEM3_1" not in result["global_scanned_set"]


def test_view_only_history_load_does_not_replace_live_scan_state():
    module = load_label_match_module()
    app = object.__new__(module.Label_Match)
    app.run_tests = True
    app.initialized_successfully = True
    app.history_tree = _RecordingTree()
    app.summary_tree = _RecordingTree()
    app.history_queue = queue.Queue()
    app.scan_count = defaultdict(lambda: defaultdict(int))
    app.scan_count["2026-06-23"][("TODAY", "-")] = 1
    app.global_scanned_set = {"TODAY_PRODUCT"}
    app.set_details_map = {"today-set": {"item_code": "TODAY"}}
    app.history_view_updates_active_state = True
    app.history_load_pending = True
    app.after = lambda delay, callback: None

    view_scan_count = defaultdict(lambda: defaultdict(int))
    view_scan_count["2026-06-22"][("PAST", "-")] = 2
    app.history_queue.put({
        "sorted_sets": [
            (
                "past-set",
                {
                    "values": ("past-set", "PAST", "", "", "", "", "통과", "10:00:00"),
                    "tags": ("success",),
                    "details": {"set_id": "past-set"},
                },
            )
        ],
        "scan_count": view_scan_count,
        "global_scanned_set": {"PAST_PRODUCT"},
        "set_details_map": {"past-set": {"item_code": "PAST"}},
        "updates_active_state": False,
    })

    module.Label_Match._process_history_queue(app)

    assert app.history_view_updates_active_state is False
    assert app.history_load_pending is False
    assert app.global_scanned_set == {"TODAY_PRODUCT"}
    assert app.set_details_map == {"today-set": {"item_code": "TODAY"}}
    assert app.scan_count["2026-06-23"][("TODAY", "-")] == 1
    assert "2026-06-22" not in app.scan_count
    assert app.history_tree.rows["past-set"]["values"][0] == 1
    assert ("PAST", "-", 2) in [row["values"] for row in app.summary_tree.rows.values()]


def test_summary_display_moves_date_to_header_and_shows_code_through_product_name():
    module = load_label_match_module()
    app = object.__new__(module.Label_Match)
    app.initialized_successfully = True
    app.summary_tree = _RecordingTree()
    app.summary_date_label = _FakeLabel()
    app.items_data = {"AAA2270730100-LONG-CODE": {"Item Name": "LONG-PRODUCT"}}
    long_code = "AAA2270730100-LONG-CODE"
    new_format_code = "CLC=BBB3370830123|SPC=PACKING-LABEL-FULL-TEXT|PHS=2"
    scan_count = defaultdict(lambda: defaultdict(int))
    scan_count["2026-06-22"][(long_code, "A")] = 1
    scan_count["2026-06-23"][(long_code, "A")] = 2
    scan_count["2026-06-23"][(new_format_code, "2")] = 1

    module.Label_Match._render_summary_tree(app, scan_count)

    assert [row["values"] for row in app.summary_tree.rows.values()] == [
        ("AAA2270730100-LONG-CODE | LONG-PRODUCT", "A", 3),
        ("BBB3370830123 | PACKING-LABEL-FULL-TEXT", "2", 1),
    ]
    assert app.summary_date_label.kwargs["text"] == "기간 2026-06-22 ~ 2026-06-23"


def test_stale_history_generation_is_ignored_before_current_result():
    module = load_label_match_module()
    app = object.__new__(module.Label_Match)
    app.run_tests = True
    app.initialized_successfully = True
    app.history_tree = _RecordingTree()
    app.summary_tree = _RecordingTree()
    app.history_tree.insert("", "end", iid="loading", values=())
    app.history_queue = queue.Queue()
    app.history_load_generation = 2
    app.history_view_updates_active_state = True
    app.history_load_pending = True
    app.history_active_load_pending = True
    app.scan_count = defaultdict(lambda: defaultdict(int))
    app.global_scanned_set = set()
    app.set_details_map = {}
    app.after = lambda delay, callback: None

    stale_counts = defaultdict(lambda: defaultdict(int))
    stale_counts["2026-06-22"][("STALE", "-")] = 1
    current_counts = defaultdict(lambda: defaultdict(int))
    current_counts["2026-06-23"][("CURRENT", "-")] = 1
    app.history_queue.put({
        "sorted_sets": [("stale", {"values": ("stale", "", "", "", "", "", "통과", "09:00:00"), "tags": ("success",)})],
        "scan_count": stale_counts,
        "global_scanned_set": {"STALE_PRODUCT"},
        "set_details_map": {"stale": {}},
        "updates_active_state": False,
        "load_generation": 1,
    })
    app.history_queue.put({
        "sorted_sets": [("current", {"values": ("current", "", "", "", "", "", "통과", "10:00:00"), "tags": ("success",)})],
        "scan_count": current_counts,
        "global_scanned_set": {"CURRENT_PRODUCT"},
        "set_details_map": {"current": {}},
        "updates_active_state": True,
        "load_generation": 2,
    })

    module.Label_Match._process_history_queue(app)

    assert "stale" not in app.history_tree.rows
    assert "current" in app.history_tree.rows
    assert app.global_scanned_set == {"CURRENT_PRODUCT"}
    assert app.set_details_map == {"current": {}}
    assert app.scan_count["2026-06-23"][("CURRENT", "-")] == 1
    assert app.history_load_pending is False
    assert app.history_active_load_pending is False


def test_view_only_history_poll_reschedules_while_async_load_pending():
    module = load_label_match_module()
    app = object.__new__(module.Label_Match)
    app.history_queue = queue.Queue()
    app.history_load_pending = True
    app.history_active_load_pending = False
    scheduled = []
    app.after = lambda delay, callback: scheduled.append((delay, callback))

    module.Label_Match._process_history_queue(app)

    assert scheduled
    assert scheduled[0][0] == 100


def test_orphan_history_poll_does_not_reschedule_after_load_completed():
    module = load_label_match_module()
    app = object.__new__(module.Label_Match)
    app.history_queue = queue.Queue()
    app.history_load_pending = False
    app.history_active_load_pending = False
    scheduled = []
    app.after = lambda delay, callback: scheduled.append((delay, callback))

    module.Label_Match._process_history_queue(app)

    assert scheduled == []


def test_history_load_updates_active_state_for_today_date(monkeypatch):
    module = load_label_match_module()

    class FixedDateTime(module.datetime):
        @classmethod
        def now(cls, tz=None):
            return cls(2026, 6, 23, 9, 0, 0)

    monkeypatch.setattr(module, "datetime", FixedDateTime)
    app = object.__new__(module.Label_Match)

    assert module.Label_Match._history_load_updates_active_state(app, None) is True
    assert module.Label_Match._history_load_updates_active_state(app, FixedDateTime(2026, 6, 23)) is True
    assert module.Label_Match._history_load_updates_active_state(app, FixedDateTime(2026, 6, 22)) is False


def test_process_input_blocks_scans_while_viewing_past_history():
    module = load_label_match_module()
    app = object.__new__(module.Label_Match)
    app.entry = _FakeEntry("PRODUCT_SHOULD_NOT_SCAN")
    app.status_label = _FakeLabel()
    app.data_manager = _FailingDataManager()
    app.is_blinking = False
    app.initialized_successfully = True
    app.history_view_updates_active_state = False
    app.run_tests = True

    module.Label_Match.process_input(app)

    assert "과거 기록 조회 중" in app.status_label.kwargs["text"]


def test_process_input_blocks_scans_while_active_history_load_is_pending():
    module = load_label_match_module()
    app = object.__new__(module.Label_Match)
    app.entry = _FakeEntry("PRODUCT_SHOULD_NOT_SCAN")
    app.status_label = _FakeLabel()
    app.data_manager = _FailingDataManager()
    app.is_blinking = False
    app.initialized_successfully = True
    app.history_view_updates_active_state = True
    app.history_active_load_pending = True
    app.run_tests = True

    module.Label_Match.process_input(app)

    assert "오늘 기록을 불러오는 중" in app.status_label.kwargs["text"]


def test_process_input_blocks_hidden_auto_test_while_viewing_past_history():
    module = load_label_match_module()
    app = object.__new__(module.Label_Match)
    app.entry = _FakeEntry("_RUN_AUTO_TEST_")
    app.status_label = _FakeLabel()
    app.is_blinking = False
    app.initialized_successfully = True
    app.history_view_updates_active_state = False
    app.history_active_load_pending = False
    app.run_tests = True
    app._run_auto_test_simulation = lambda: (_ for _ in ()).throw(AssertionError("auto test should not start"))

    module.Label_Match.process_input(app)

    assert "과거 기록 조회 중" in app.status_label.kwargs["text"]


def test_process_input_blocks_hidden_auto_test_while_active_history_load_is_pending():
    module = load_label_match_module()
    app = object.__new__(module.Label_Match)
    app.entry = _FakeEntry("_RUN_AUTO_TEST_")
    app.status_label = _FakeLabel()
    app.is_blinking = False
    app.initialized_successfully = True
    app.history_view_updates_active_state = True
    app.history_active_load_pending = True
    app.run_tests = True
    app._run_auto_test_simulation = lambda: (_ for _ in ()).throw(AssertionError("auto test should not start"))

    module.Label_Match.process_input(app)

    assert "오늘 기록을 불러오는 중" in app.status_label.kwargs["text"]


def test_reset_current_set_blocks_cancel_while_viewing_past_history():
    module = load_label_match_module()
    app = object.__new__(module.Label_Match)
    active_set = {
        "id": "active-set",
        "raw": ["MASTER", "PRODUCT1"],
        "parsed": ["MASTER", "MASTER"],
        "start_time": datetime(2026, 6, 23, 10, 0, 0),
        "error_count": 0,
        "has_error_or_reset": False,
    }
    app.current_set_info = dict(active_set)
    app.history_view_updates_active_state = False
    app.is_blinking = False
    app.run_tests = True
    app.status_label = _FakeLabel()
    app.data_manager = _FailingDataManager()
    app.history_tree = _FailingHistoryTree()
    app._delete_current_set_state = lambda: (_ for _ in ()).throw(AssertionError("state file should not be deleted"))

    module.Label_Match._reset_current_set(app, full_reset=True)

    assert app.current_set_info == active_set
    assert "과거 기록 조회 중" in app.status_label.kwargs["text"]


def test_reset_current_set_blocks_cancel_while_active_history_load_is_pending():
    module = load_label_match_module()
    app = object.__new__(module.Label_Match)
    active_set = {
        "id": "active-set",
        "raw": ["MASTER", "PRODUCT1"],
        "parsed": ["MASTER", "MASTER"],
        "start_time": datetime(2026, 6, 23, 10, 0, 0),
        "error_count": 0,
        "has_error_or_reset": False,
    }
    app.current_set_info = dict(active_set)
    app.history_view_updates_active_state = True
    app.history_active_load_pending = True
    app.is_blinking = False
    app.run_tests = True
    app.status_label = _FakeLabel()
    app.data_manager = _FailingDataManager()
    app.history_tree = _FailingHistoryTree()
    app._delete_current_set_state = lambda: (_ for _ in ()).throw(AssertionError("state file should not be deleted"))

    module.Label_Match._reset_current_set(app, full_reset=True)

    assert app.current_set_info == active_set
    assert "오늘 기록을 불러오는 중" in app.status_label.kwargs["text"]


def test_auto_test_reset_step_respects_blocked_reset_without_clearing_state():
    module = load_label_match_module()
    app = object.__new__(module.Label_Match)
    app.is_running_simulation = True
    app.current_scenario_index = 0
    app.current_step_index = 0
    app.simulation_scenarios = [{"name": "blocked reset", "steps": [("reset", None)]}]
    app.current_set_info = {"id": "active-set", "raw": ["MASTER"], "parsed": ["MASTER"]}
    app.history_view_updates_active_state = False
    app.history_active_load_pending = False
    app.is_blinking = False
    app.run_tests = True
    app.initialized_successfully = True
    app.status_label = _FakeLabel()
    app.data_manager = _FailingDataManager()
    app.history_tree = _RecordingTree()
    app.summary_tree = _RecordingTree()
    app.history_tree.insert("", "end", iid="existing-history", values=("keep",))
    app.summary_tree.insert("", "end", iid="existing-summary", values=("keep",))
    app.scan_count = defaultdict(lambda: defaultdict(int))
    app.scan_count["2026-06-23"][("ITEM1", "-")] = 1
    app.global_scanned_set = {"PRODUCT1"}
    app.set_details_map = {"set-1": {"item_code": "ITEM1"}}
    app.entry = _FakeWidget()
    app._delete_current_set_state = lambda: (_ for _ in ()).throw(AssertionError("state file should not be deleted"))
    app._truncate_string = lambda value, max_len=50: value
    app.after = lambda *args, **kwargs: None

    module.Label_Match._execute_test_step(app)

    assert app.is_running_simulation is False
    assert "existing-history" in app.history_tree.rows
    assert "existing-summary" in app.summary_tree.rows
    assert app.scan_count["2026-06-23"][("ITEM1", "-")] == 1
    assert app.global_scanned_set == {"PRODUCT1"}
    assert app.set_details_map == {"set-1": {"item_code": "ITEM1"}}


def test_on_closing_blocks_background_work_without_closing_data_manager(monkeypatch):
    module = load_label_match_module()
    app = object.__new__(module.Label_Match)
    app.initialized_successfully = True
    app.is_running_simulation = False
    app.is_generating_test_logs = True
    app.run_tests = False
    app.data_manager = _FailingDataManager()
    app.destroy = lambda: (_ for _ in ()).throw(AssertionError("window should not close"))
    warnings = []
    monkeypatch.setattr(module.messagebox, "showwarning", lambda *args, **kwargs: warnings.append((args, kwargs)))

    module.Label_Match.on_closing(app)

    assert warnings


def test_reload_today_history_blocks_during_background_work():
    module = load_label_match_module()
    app = object.__new__(module.Label_Match)
    app.is_running_simulation = True
    app.is_generating_test_logs = False
    app.run_tests = True
    app.status_label = _FakeLabel()
    app._load_history_and_rebuild_summary = lambda *args: (_ for _ in ()).throw(AssertionError("history should not reload"))
    app._process_history_queue = lambda: (_ for _ in ()).throw(AssertionError("history queue should not process"))

    module.Label_Match._reload_today_history(app)

    assert "기록을 다시 불러올 수 없습니다" in app.status_label.kwargs["text"]


def test_prompt_for_date_reload_blocks_during_background_work(monkeypatch):
    module = load_label_match_module()
    app = object.__new__(module.Label_Match)
    app.initialized_successfully = True
    app.is_running_simulation = False
    app.is_generating_test_logs = True
    app.run_tests = False
    app.status_label = _FakeLabel()
    app._load_history_and_rebuild_summary = lambda *args: (_ for _ in ()).throw(AssertionError("history should not reload"))
    app._process_history_queue = lambda: (_ for _ in ()).throw(AssertionError("history queue should not process"))
    monkeypatch.setattr(module.messagebox, "showwarning", lambda *args, **kwargs: None)
    monkeypatch.setattr(module, "CalendarWindow", lambda parent: (_ for _ in ()).throw(AssertionError("calendar should not open")))

    module.Label_Match._prompt_for_date_and_reload(app)

    assert "기록을 다시 불러올 수 없습니다" in app.status_label.kwargs["text"]


def test_reload_today_history_blocks_duplicate_active_history_load():
    module = load_label_match_module()
    app = object.__new__(module.Label_Match)
    app.is_running_simulation = False
    app.is_generating_test_logs = False
    app.history_active_load_pending = True
    app.run_tests = True
    app.status_label = _FakeLabel()
    app._load_history_and_rebuild_summary = lambda *args: (_ for _ in ()).throw(AssertionError("history should not reload"))
    app._process_history_queue = lambda: (_ for _ in ()).throw(AssertionError("history queue should not process"))

    module.Label_Match._reload_today_history(app)

    assert "오늘 기록을 불러오는 중" in app.status_label.kwargs["text"]


def test_prompt_for_date_reload_blocks_duplicate_active_history_load(monkeypatch):
    module = load_label_match_module()
    app = object.__new__(module.Label_Match)
    app.initialized_successfully = True
    app.is_running_simulation = False
    app.is_generating_test_logs = False
    app.history_active_load_pending = True
    app.run_tests = False
    app.status_label = _FakeLabel()
    app._load_history_and_rebuild_summary = lambda *args: (_ for _ in ()).throw(AssertionError("history should not reload"))
    app._process_history_queue = lambda: (_ for _ in ()).throw(AssertionError("history queue should not process"))
    monkeypatch.setattr(module.messagebox, "showwarning", lambda *args, **kwargs: None)
    monkeypatch.setattr(module, "CalendarWindow", lambda parent: (_ for _ in ()).throw(AssertionError("calendar should not open")))

    module.Label_Match._prompt_for_date_and_reload(app)

    assert "오늘 기록을 불러오는 중" in app.status_label.kwargs["text"]


def test_test_log_generation_failure_clears_background_flag_and_restores_input():
    module = load_label_match_module()

    class FailingLogManager:
        def log_event(self, event_type, details):
            raise RuntimeError("disk full")

    app = object.__new__(module.Label_Match)
    app.is_generating_test_logs = True
    app.items_data = {"ITEM1": {"Item Name": "Product", "Spec": "Spec"}}
    app.data_manager = FailingLogManager()
    app.scan_count = defaultdict(lambda: defaultdict(int))
    app.set_details_map = {}
    app.global_scanned_set = set()
    app.entry = _FakeWidget()
    app.status_label = _FakeLabel()
    app.run_tests = True
    app.winfo_exists = lambda: True
    app.update_big_display = lambda *args, **kwargs: None
    app.after = lambda delay, callback, *args: callback(*args)

    module.Label_Match._execute_test_simulation(app, "ITEM1", 1)

    assert app.is_generating_test_logs is False
    assert app.entry.kwargs["state"] == "normal"
    assert "테스트 데이터 생성 실패" in app.status_label.kwargs["text"]


def test_cancel_completed_tray_uses_latest_for_reused_regular_master_label():
    module = load_label_match_module()
    app = object.__new__(module.Label_Match)
    old_details = _completed_details(
        set_id="old",
        master_code="ITEM1",
        end_time="2026-06-23T10:00:00",
        raw_scans=["ITEM1", "SHARED_PRODUCT", "OLD_ONLY"],
    )
    new_details = _completed_details(
        set_id="new",
        master_code="ITEM1",
        end_time="2026-06-23T11:00:00",
        raw_scans=["ITEM1", "SHARED_PRODUCT", "NEW_ONLY"],
    )
    app.set_details_map = {"old": old_details, "new": new_details}
    app.scan_count = defaultdict(lambda: defaultdict(int))
    app.scan_count["2026-06-23"][("ITEM1", "-")] = 2
    app.global_scanned_set = {"SHARED_PRODUCT", "OLD_ONLY", "NEW_ONLY"}
    app.history_tree = _RecordingTree()
    app.history_tree.insert("", "end", iid="old", values=())
    app.history_tree.insert("", "end", iid="new", values=())
    app.data_manager = _FakeLoggingDataManager()
    app.run_tests = True
    app._update_summary_tree = lambda: None

    module.Label_Match._cancel_completed_tray_by_label(app, "ITEM1")

    assert app.data_manager.events[0][1]["cancelled_set_id"] == "new"
    assert set(app.set_details_map) == {"old"}
    assert app.global_scanned_set == {"SHARED_PRODUCT", "OLD_ONLY"}
    assert app.scan_count["2026-06-23"][("ITEM1", "-")] == 1


def test_cancel_completed_tray_uses_datetime_end_time_candidates():
    module = load_label_match_module()
    app = object.__new__(module.Label_Match)
    old_details = _completed_details(
        set_id="old",
        master_code="ITEM1",
        end_time="2026-06-23T10:00:00",
        raw_scans=["ITEM1", "OLD_ONLY"],
    )
    new_details = _completed_details(
        set_id="new",
        master_code="ITEM1",
        end_time=datetime(2026, 6, 23, 11, 0, 0),
        raw_scans=["ITEM1", "NEW_ONLY"],
    )
    app.set_details_map = {"old": old_details, "new": new_details}
    app.scan_count = defaultdict(lambda: defaultdict(int))
    app.scan_count["2026-06-23"][("ITEM1", "-")] = 2
    app.global_scanned_set = {"OLD_ONLY", "NEW_ONLY"}
    app.history_tree = _RecordingTree()
    app.history_tree.insert("", "end", iid="old", values=())
    app.history_tree.insert("", "end", iid="new", values=())
    app.data_manager = _FakeLoggingDataManager()
    app.run_tests = True
    app._update_summary_tree = lambda: None

    module.Label_Match._cancel_completed_tray_by_label(app, "ITEM1")

    assert app.data_manager.events[0][1]["cancelled_set_id"] == "new"
    assert set(app.set_details_map) == {"old"}
    assert app.global_scanned_set == {"OLD_ONLY"}


def test_cancel_completed_tray_finds_all_datetime_end_time_candidates():
    module = load_label_match_module()
    app = object.__new__(module.Label_Match)
    old_details = _completed_details(
        set_id="old",
        master_code="ITEM1",
        end_time=datetime(2026, 6, 23, 10, 0, 0),
        raw_scans=["ITEM1", "OLD_ONLY"],
    )
    new_details = _completed_details(
        set_id="new",
        master_code="ITEM1",
        end_time=datetime(2026, 6, 23, 11, 0, 0),
        raw_scans=["ITEM1", "NEW_ONLY"],
    )
    app.set_details_map = {"old": old_details, "new": new_details}
    app.scan_count = defaultdict(lambda: defaultdict(int))
    app.scan_count["2026-06-23"][("ITEM1", "-")] = 2
    app.global_scanned_set = {"OLD_ONLY", "NEW_ONLY"}
    app.history_tree = _RecordingTree()
    app.history_tree.insert("", "end", iid="old", values=())
    app.history_tree.insert("", "end", iid="new", values=())
    app.data_manager = _FakeLoggingDataManager()
    app.run_tests = True
    app._update_summary_tree = lambda: None

    module.Label_Match._cancel_completed_tray_by_label(app, "ITEM1")

    assert app.data_manager.events[0][1]["cancelled_set_id"] == "new"
    assert set(app.set_details_map) == {"old"}


def test_cancel_completed_tray_uses_raw_first_scan_for_unique_master_label():
    module = load_label_match_module()
    app = object.__new__(module.Label_Match)
    unique_raw = "CLC=ITEM1|SPC=Product|PHS=A"
    unique_details = _completed_details(
        set_id="unique",
        master_code="ITEM1",
        end_time="2026-06-23T10:00:00",
        raw_scans=[unique_raw, "UNIQUE_PRODUCT"],
        item_name_override="Product",
        phase="A",
    )
    regular_details = _completed_details(
        set_id="regular",
        master_code="ITEM1",
        end_time="2026-06-23T11:00:00",
        raw_scans=["ITEM1", "REGULAR_PRODUCT"],
    )
    app.set_details_map = {"unique": unique_details, "regular": regular_details}
    app.scan_count = defaultdict(lambda: defaultdict(int))
    app.scan_count["2026-06-23"][("ITEM1", "A")] = 1
    app.scan_count["2026-06-23"][("ITEM1", "-")] = 1
    app.global_scanned_set = {unique_raw, "UNIQUE_PRODUCT", "REGULAR_PRODUCT"}
    app.history_tree = _RecordingTree()
    app.history_tree.insert("", "end", iid="unique", values=())
    app.history_tree.insert("", "end", iid="regular", values=())
    app.data_manager = _FakeLoggingDataManager()
    app.run_tests = True
    app._update_summary_tree = lambda: None

    module.Label_Match._cancel_completed_tray_by_label(app, unique_raw)

    assert app.data_manager.events[0][1]["cancelled_set_id"] == "unique"
    assert set(app.set_details_map) == {"regular"}
    assert app.global_scanned_set == {"REGULAR_PRODUCT"}


def test_cancel_completed_tray_finds_unique_master_by_base64_or_decoded_equivalent():
    module = load_label_match_module()
    app = object.__new__(module.Label_Match)
    decoded_master = "CLC=ITEM1|SPC=Product|PHS=A"
    base64_master = base64.b64encode(decoded_master.encode("utf-8")).decode("utf-8")
    unique_details = _completed_details(
        set_id="unique",
        master_code="ITEM1",
        end_time="2026-06-23T10:00:00",
        raw_scans=[base64_master, "UNIQUE_PRODUCT"],
        item_name_override="Product",
        phase="A",
    )
    app.set_details_map = {"unique": unique_details}
    app.scan_count = defaultdict(lambda: defaultdict(int))
    app.scan_count["2026-06-23"][("ITEM1", "A")] = 1
    app.global_scanned_set = module._label_match_duplicate_index_barcodes(unique_details)
    app.history_tree = _RecordingTree()
    app.history_tree.insert("", "end", iid="unique", values=())
    app.data_manager = _FakeLoggingDataManager()
    app.run_tests = True
    app._update_summary_tree = lambda: None

    module.Label_Match._cancel_completed_tray_by_label(app, decoded_master)

    assert app.data_manager.events[0][1]["cancelled_set_id"] == "unique"
    assert app.set_details_map == {}
    assert app.global_scanned_set == set()
    assert app.scan_count == {}


def test_cancel_completed_tray_finds_legacy_base64_unique_master_without_metadata():
    module = load_label_match_module()
    app = object.__new__(module.Label_Match)
    decoded_master = "CLC=ITEM1|SPC=Product|PHS=A"
    base64_master = base64.b64encode(decoded_master.encode("utf-8")).decode("utf-8")
    legacy_details = _completed_details(
        set_id="legacy",
        master_code="ITEM1",
        end_time="2026-06-23T10:00:00",
        raw_scans=[base64_master, "UNIQUE_PRODUCT"],
        phase="A",
    )
    app.set_details_map = {"legacy": legacy_details}
    app.scan_count = defaultdict(lambda: defaultdict(int))
    app.scan_count["2026-06-23"][("ITEM1", "A")] = 1
    app.global_scanned_set = module._label_match_duplicate_index_barcodes(legacy_details)
    app.history_tree = _RecordingTree()
    app.history_tree.insert("", "end", iid="legacy", values=())
    app.data_manager = _FakeLoggingDataManager()
    app.run_tests = True
    app._update_summary_tree = lambda: None

    module.Label_Match._cancel_completed_tray_by_label(app, decoded_master)

    assert app.data_manager.events[0][1]["cancelled_set_id"] == "legacy"
    assert app.set_details_map == {}
    assert app.global_scanned_set == set()


def test_delete_selected_row_handles_string_iid_against_numeric_detail_key():
    module = load_label_match_module()

    class SelectableTree(_RecordingTree):
        def __init__(self):
            super().__init__()
            self._selection = ()

        def selection(self):
            return self._selection

        def item(self, iid, option=None):
            if option == "values":
                return self.rows[iid]["values"]
            return self.rows[iid]

    details = _completed_details(
        set_id=123,
        master_code="VALID-MASTER1",
        end_time="2026-06-23T08:00:00",
        raw_scans=["MASTER", "PRODUCT-1"],
    )
    app = object.__new__(module.Label_Match)
    app.Results = module.Label_Match.Results
    app.Events = module.Label_Match.Events
    app.run_tests = True
    app.initialized_successfully = True
    app.history_view_updates_active_state = True
    app.history_load_pending = False
    app.history_active_load_pending = False
    app.history_tree = SelectableTree()
    app.history_tree.insert("", "end", iid="123", values=(1, "MASTER", "PRODUCT-1", "", "", "", "통과", "08:00:00"))
    app.history_tree._selection = ("123",)
    app.set_details_map = {123: details}
    app.history_row_details_map = {"123": details}
    app.scan_count = defaultdict(lambda: defaultdict(int))
    app.scan_count["2026-06-23"][("VALID-MASTER1", "-")] = 1
    app.global_scanned_set = {"PRODUCT-1"}
    app.data_manager = _FakeLoggingDataManager()
    app._update_summary_tree = lambda: None
    app._render_history_detail = lambda *args, **kwargs: None

    module.Label_Match._delete_selected_row(app)

    assert "123" not in app.history_tree.rows
    assert app.set_details_map == {}
    assert app.history_row_details_map == {}
    assert "2026-06-23" not in app.scan_count
    assert app.global_scanned_set == set()
    assert app.data_manager.events[0][0] == module.Label_Match.Events.SET_DELETED
    assert app.data_manager.events[0][1]["set_id"] == "123"


def test_delete_selected_row_keeps_row_when_delete_log_write_fails(monkeypatch):
    module = load_label_match_module()

    class SelectableTree(_RecordingTree):
        def __init__(self):
            super().__init__()
            self._selection = ()

        def selection(self):
            return self._selection

        def item(self, iid, option=None):
            if option == "values":
                return self.rows[iid]["values"]
            return self.rows[iid]

    class DeleteLogFailingDataManager:
        def log_event(self, event_type, details):
            raise RuntimeError("disk full")

    app = object.__new__(module.Label_Match)
    app.Results = module.Label_Match.Results
    app.Events = module.Label_Match.Events
    app.run_tests = False
    app.initialized_successfully = True
    app.history_view_updates_active_state = True
    app.history_load_pending = False
    app.history_active_load_pending = False
    app.history_tree = SelectableTree()
    app.history_tree.insert("", "end", iid="set-1", values=(1, "MASTER", "PRODUCT-1", "", "", "", "통과", "08:00:00"))
    app.history_tree._selection = ("set-1",)
    app.set_details_map = {
        "set-1": _completed_details(
            set_id="set-1",
            master_code="VALID-MASTER1",
            end_time="2026-06-23T08:00:00",
            raw_scans=["MASTER", "PRODUCT-1"],
        )
    }
    app.history_row_details_map = {}
    app.scan_count = defaultdict(lambda: defaultdict(int))
    app.global_scanned_set = {"PRODUCT-1"}
    app.data_manager = DeleteLogFailingDataManager()
    app.status_label = _FakeLabel()
    app._update_summary_tree = lambda: (_ for _ in ()).throw(AssertionError("summary should not update"))
    app._render_history_detail = lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("detail should not update"))
    monkeypatch.setattr(module.messagebox, "askyesno", lambda *args, **kwargs: True)
    errors = []
    monkeypatch.setattr(module.messagebox, "showerror", lambda *args, **kwargs: errors.append((args, kwargs)))

    module.Label_Match._delete_selected_row(app)

    assert "set-1" in app.history_tree.rows
    assert "set-1" in app.set_details_map
    assert errors
    assert errors[0][0][0] == "삭제 실패"
    assert "disk full" in errors[0][0][1]
    assert app.status_label.kwargs["text"] == "❌ 기록 삭제 실패"


def test_test_log_generation_blocks_settings_save(monkeypatch):
    module = load_label_match_module()
    app = object.__new__(module.Label_Match)
    app.entry = _FakeWidget()
    app.progress_bar = _FakeProgressBar()
    app.is_generating_test_logs = False
    app.is_running_simulation = False
    app.run_tests = False
    app.update_big_display = lambda *args, **kwargs: None
    fake_thread = _FakeThread()
    monkeypatch.setattr(module.threading, "Thread", lambda *args, **kwargs: fake_thread)

    module.Label_Match.run_test_log_simulation(app, "ITEM1", 100)

    assert app.is_generating_test_logs is True
    assert fake_thread.started is True

    app.current_set_info = {"id": None}
    app.worker_name = "old-worker"
    app.data_manager = _FailingDataManager()
    app._save_app_settings = lambda: (_ for _ in ()).throw(AssertionError("settings should not save"))
    app._update_save_directory = lambda: (_ for _ in ()).throw(AssertionError("save directory should not update"))
    window = _FakeWindow()
    warnings = []
    monkeypatch.setattr(module.messagebox, "showwarning", lambda *args, **kwargs: warnings.append((args, kwargs)))

    module.Label_Match._save_settings_and_close(app, window, "new-worker")

    assert app.worker_name == "old-worker"
    assert warnings
    assert window.destroyed is False


def _event_row(module, timestamp, details):
    return {
        "timestamp": timestamp,
        "worker_name": "tester",
        "event": module.Label_Match.Events.TRAY_COMPLETE,
        "details": json.dumps(details, ensure_ascii=False, cls=module.DateTimeEncoder),
    }


def _completed_details(set_id, master_code, end_time, raw_scans, item_name_override=None, phase="-"):
    return {
        "set_id": set_id,
        "final_result": "통과",
        "master_label_code": master_code,
        "item_code": master_code,
        "item_name": item_name_override or "Product",
        "spec": "",
        "scanned_product_barcodes": raw_scans,
        "parsed_product_barcodes": [master_code] * len(raw_scans),
        "end_time": end_time,
        "production_date": "2026-06-23",
        "phase": phase,
        "item_name_override": item_name_override,
        "is_unique_master_label": bool(item_name_override),
    }


class _FakeDataManager:
    def __init__(self, log_path):
        self.log_path = log_path

    def _get_log_filepath(self, target_date=None):
        return str(self.log_path)


class _FakeLoggingDataManager:
    def __init__(self):
        self.events = []

    def log_event(self, event_type, details):
        self.events.append((event_type, details))


class _FakeHistoryTree:
    def exists(self, iid):
        return False


class _FailingHistoryTree:
    def exists(self, iid):
        raise AssertionError("history tree should not be touched")

    def delete(self, *iids):
        raise AssertionError("history tree should not be touched")


class _FakeLabel:
    def config(self, **kwargs):
        self.kwargs = kwargs

    def configure(self, **kwargs):
        self.config(**kwargs)


class _RecordingTree:
    def __init__(self):
        self.rows = {}
        self.next_id = 0

    def exists(self, iid):
        return iid in self.rows

    def delete(self, *iids):
        for iid in iids:
            self.rows.pop(iid, None)

    def get_children(self):
        return tuple(self.rows.keys())

    def insert(self, parent, index, iid=None, values=(), tags=()):
        row_id = iid or f"row-{self.next_id}"
        self.next_id += 1
        self.rows[row_id] = {"values": tuple(values), "tags": tuple(tags)}
        return row_id


class _FakeEntry:
    def __init__(self, text):
        self.text = text
        self.deleted = False

    def get(self):
        return self.text

    def delete(self, start, end):
        self.deleted = True


class _FailingDataManager:
    def log_event(self, event_type, details):
        raise AssertionError("log_event should not be called")

    def close(self, timeout=None):
        raise AssertionError("close should not be called")


class _CloseFailingDataManager:
    def log_event(self, event_type, details):
        self.logged = (event_type, details)

    def close(self, timeout=None):
        raise RuntimeError("forced close failure")


class _StoppedThread:
    def is_alive(self):
        return False


class _RecoverableCloseFailingDataManager:
    def __init__(self):
        self.save_directory = "C:\\Sync\\old-worker"
        self.process_name = "포장실"
        self.worker_name = "old-worker"
        self.unique_id = "PC01"
        self._close_requested = False
        self.log_thread = _StoppedThread()
        self.closed = False
        self.events = []

    def log_event(self, event_type, details):
        self.events.append((event_type, details))

    def close(self, timeout=None):
        self.closed = True
        self._close_requested = True
        raise RuntimeError("forced close failure")


class _FakeWindow:
    def __init__(self):
        self.destroyed = False

    def destroy(self):
        self.destroyed = True


class _FakeWidget:
    def __init__(self):
        self.kwargs = {}

    def config(self, **kwargs):
        self.kwargs.update(kwargs)

    def focus_set(self):
        self.focused = True


class _FakeProgressBar(dict):
    pass


class _FakeThread:
    def __init__(self):
        self.started = False

    def start(self):
        self.started = True
