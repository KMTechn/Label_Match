import csv
import inspect
import json
import threading
from types import SimpleNamespace

import Label_Match as label_module
from package_logistics import PackageOutbox


class _Entry:
    def __init__(self, value):
        self.value = value

    def get(self):
        return self.value


def _replacement_resolution():
    return {
        "scan": {
            "replacement_required": True,
            "scanned_label_id": "LBL-OLD-INTERNAL",
            "active_label_id": "LBL-NEW-INTERNAL",
        }
    }


def _replacement_app(manager, outbox):
    app = label_module.Label_Match.__new__(label_module.Label_Match)
    app.current_set_info = {
        "id": "SET-1",
        "source_session_id": "ITG-REPLACEMENT-1",
        "raw": ["PHS2"],
        "parsed": ["ITEM"],
    }
    app.data_manager = manager
    app.package_outbox = outbox
    app._phs_replacement_notice_pairs = set()
    app._phs_label_guidance_notice = None
    app._render_operator_workbench = lambda: None
    return app


def _replacement_csv_events(root):
    events = []
    for path in root.glob("*.csv"):
        with path.open("r", encoding="utf-8-sig", newline="") as handle:
            for row in csv.DictReader(handle):
                if row.get("event") == "PHS_REPLACEMENT_WAITING_MARKED":
                    events.append(json.loads(row["details"]))
    return events


def test_worker_summary_hides_internal_instruction_and_label_ids():
    resolution = {
        "actions": [
            {
                "action_id": "ACTION-INTERNAL-1",
                "action_type": "SPLIT",
                "sources": [
                    {
                        "source_label_id": "LABEL-INTERNAL-1",
                        "instruction_id": "INSTRUCTION-INTERNAL-1",
                        "business_date": "2026-07-27",
                        "item_daily_ordinal": 3,
                        "worker_code": "7월27일-3",
                        "qty_pcs": 4,
                    }
                ],
                "targets": [
                    {
                        "instruction_id": "INSTRUCTION-INTERNAL-2",
                        "business_date": "2026-07-28",
                        "item_daily_ordinal": 1,
                        "worker_code": "7월28일-1",
                        "qty_pcs": 2,
                    },
                    {
                        "instruction_id": "INSTRUCTION-INTERNAL-3",
                        "business_date": "2026-07-28",
                        "item_daily_ordinal": 2,
                        "worker_code": "7월28일-2",
                        "qty_pcs": 2,
                    },
                ],
            }
        ]
    }

    text = "\n".join(
        label_module._label_match_phs_reconciliation_display_lines(
            resolution
        )
    )

    assert "현품표 분할" in text
    assert "3번째 현품표" in text
    assert "7월28일-2" in text
    assert "ACTION-INTERNAL" not in text
    assert "LABEL-INTERNAL" not in text
    assert "INSTRUCTION-INTERNAL" not in text


def test_topology_refresh_blocks_package_complete_without_losing_progress():
    current = {
        "central_inherit_all": True,
        "raw": ["PHS2"],
        "parsed": ["ITEM"],
        "progress_marker": "KEEP",
        "phs_label_topology_refresh_required": True,
    }

    assert (
        label_module._label_match_manual_complete_block_reason(current)
        == "phs_label_topology_refresh_required"
    )
    assert current["raw"] == ["PHS2"]
    assert current["parsed"] == ["ITEM"]
    assert current["progress_marker"] == "KEEP"


def test_f5_scan_and_background_exchange_restore_scanner_focus():
    scan_source = inspect.getsource(
        label_module.Label_Match._show_phs_reconciliation_scan_window
    )
    exchange_source = inspect.getsource(
        label_module.Label_Match._start_phs_reconciliation_exchange
    )

    assert 'bind("<Return>", submit)' in scan_source
    assert "scan_entry.focus_set()" in scan_source
    assert "threading.Thread" in exchange_source
    assert "_focus_scan_entry_if_available()" in exchange_source
    assert "messagebox.showinfo" not in exchange_source


def test_replacement_action_window_is_non_modal_and_uses_operator_language():
    source = inspect.getsource(
        label_module.Label_Match._show_phs_reconciliation_action_window
    )

    assert ".grab_set(" not in source
    assert "popup.focus_set()" not in source
    assert "_focus_scan_entry_if_available()" in source
    assert "ACTIVE successor" not in source
    assert "ACTIVE로 전환" not in source
    assert "새 현품표가 출력되면 기존 표를 교체해 주세요." in source


def test_replacement_required_notice_is_yellow_and_once_per_pair(tmp_path):
    expected = (
        "현품표 교체 필요. 작업은 계속할 수 있습니다. "
        "현재 현품표를 교체 대기로 분리해 주세요."
    )
    renders = []
    app = label_module.Label_Match.__new__(label_module.Label_Match)
    app.current_set_info = {
        "id": "SET-1",
        "raw": ["PHS2", "PRODUCT-1"],
        "parsed": ["ITEM", "PRODUCT-1"],
        "progress_marker": "KEEP",
    }
    app.entry = _Entry("UNSUBMITTED-INPUT")
    app._phs_replacement_notice_pairs = set()
    app._phs_label_guidance_notice = None
    app._render_operator_workbench = lambda: renders.append("render")
    app.package_outbox = PackageOutbox(tmp_path / "replacement.sqlite3")
    replacement_intents = []
    flushes = []
    app.data_manager = SimpleNamespace(
        save_directory=str(tmp_path),
        process_name="포장실",
        unique_id="PACK-PC-1",
        log_event=lambda event, details: replacement_intents.append(
            (event, details)
        ),
        flush=lambda timeout=None: flushes.append(timeout),
    )
    before = {
        "raw": list(app.current_set_info["raw"]),
        "parsed": list(app.current_set_info["parsed"]),
        "progress_marker": app.current_set_info["progress_marker"],
        "entry": app.entry.get(),
    }

    assert (
        app._show_phs_replacement_required_notice_once(
            _replacement_resolution()
        )
        is True
    )
    assert (
        app._show_phs_replacement_required_notice_once(
            _replacement_resolution()
        )
        is False
    )

    notice = app._phs_label_guidance_notice
    assert notice.message == expected
    assert notice.tone == "warning"
    assert renders == ["render"]
    assert [event for event, _details in replacement_intents] == [
        "PHS_REPLACEMENT_WAITING_MARKED"
    ]
    intent = replacement_intents[0][1]
    assert intent["set_id"] == "SET-1"
    assert intent["old_label_id"] == "LBL-OLD-INTERNAL"
    assert intent["new_label_id"] == "LBL-NEW-INTERNAL"
    assert intent["process"] == "PACKAGING"
    assert intent["location"] == "PACKAGING"
    assert intent["dedupe_key"]
    assert flushes == [5.0]
    assert app.current_set_info["raw"] == before["raw"]
    assert app.current_set_info["parsed"] == before["parsed"]
    assert app.current_set_info["progress_marker"] == before["progress_marker"]
    assert app.entry.get() == before["entry"]
    assert not any(
        marker in notice.message
        for marker in (
            "ACTIVE successor",
            "OVERLAY_REPLACED",
            "LBL-",
            "UUID",
            "hash",
        )
    )


def test_replacement_waiting_event_is_exact_once_across_restart_and_concurrency(
    tmp_path,
):
    db_path = tmp_path / "replacement.sqlite3"
    manager = label_module.DataManager(
        str(tmp_path), "포장실", "worker", "PACK-PC-1"
    )
    barrier = threading.Barrier(8)
    results = []
    apps = [
        _replacement_app(manager, PackageOutbox(db_path)) for _ in range(8)
    ]

    def record(app):
        barrier.wait()
        results.append(
            app._show_phs_replacement_required_notice_once(
                _replacement_resolution()
            )
        )

    threads = [threading.Thread(target=record, args=(app,)) for app in apps]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=10)
    assert all(not thread.is_alive() for thread in threads)
    assert len(results) == len(threads)
    assert all(results)

    restarted = _replacement_app(manager, PackageOutbox(db_path))
    assert restarted._show_phs_replacement_required_notice_once(
        _replacement_resolution()
    )
    manager.close(timeout=5)

    events = _replacement_csv_events(tmp_path)
    assert len(events) == 1
    assert events[0]["process"] == "PACKAGING"
    assert events[0]["location"] == "PACKAGING"
    row = PackageOutbox(db_path).get_replacement_waiting_event(
        events[0]["dedupe_key"]
    )
    assert row["local_csv_committed"] == 1


def test_replacement_waiting_csv_fsync_crash_recovers_without_duplicate(tmp_path):
    db_path = tmp_path / "replacement-crash.sqlite3"
    manager = label_module.DataManager(
        str(tmp_path), "포장실", "worker", "PACK-PC-1"
    )

    class CrashAfterProjectionOutbox(PackageOutbox):
        def commit_replacement_waiting_csv_projection(self, key, projector):
            def crash_after_fsync(payload):
                projector(payload)
                raise RuntimeError("simulated crash after CSV fsync")

            return super().commit_replacement_waiting_csv_projection(
                key, crash_after_fsync
            )

    crashed = _replacement_app(manager, CrashAfterProjectionOutbox(db_path))
    durable_blocks = []
    crashed._publish_durable_commit_block = (
        lambda error, **_kwargs: durable_blocks.append(str(error)) or False
    )
    assert not crashed._show_phs_replacement_required_notice_once(
        _replacement_resolution()
    )
    assert durable_blocks == ["simulated crash after CSV fsync"]

    recovered = _replacement_app(manager, PackageOutbox(db_path))
    assert recovered._show_phs_replacement_required_notice_once(
        _replacement_resolution()
    )
    manager.close(timeout=5)

    events = _replacement_csv_events(tmp_path)
    assert len(events) == 1
    assert events[0]["process"] == "PACKAGING"
    assert events[0]["location"] == "PACKAGING"
    row = PackageOutbox(db_path).get_replacement_waiting_event(
        events[0]["dedupe_key"]
    )
    assert row["local_csv_committed"] == 1


def test_exchange_execute_rechecks_lookup_capture_before_starting_worker():
    calls = []
    app = label_module.Label_Match.__new__(label_module.Label_Match)
    app.current_set_info = {
        "id": "SET-1",
        "raw": ["PHS2"],
        "parsed": ["ITEM"],
        "package_source_snapshot": {"version": 1},
    }
    captured = label_module._label_match_capture_current_set(
        app.current_set_info
    )
    app.current_set_info["raw"].append("PRODUCT-1")
    app._phs_label_exchange_pending = False
    app._phs_label_guidance_notice = None
    app._render_operator_workbench = lambda: calls.append("render")
    app._focus_scan_entry_if_available = lambda: calls.append("focus")

    started = app._start_phs_reconciliation_exchange(
        _replacement_resolution(),
        expected_current_set=captured,
    )

    assert started is False
    assert app.current_set_info["raw"] == ["PHS2", "PRODUCT-1"]
    assert app.current_set_info["parsed"] == ["ITEM"]
    assert calls == ["render", "focus"]
    assert app._phs_label_guidance_notice.tone == "warning"
    assert "포장 상태가 바뀌어" in app._phs_label_guidance_notice.message


def test_direct_replaced_label_scan_uses_exact_operator_notice(tmp_path):
    expected = (
        "현품표 교체 필요. 작업은 계속할 수 있습니다. "
        "현재 현품표를 교체 대기로 분리해 주세요."
    )
    old_qr = (
        "PHS=2|SRC=KMTECH_INPUT_TAG|ITG=ITG-1|CLC=ITEM-1|"
        "LBL=LBL-OLD-INTERNAL|HSH=aaaaaaaaaaaaaaaa"
    )
    evidence = SimpleNamespace(
        replaced_scan=True,
        physical_scanned_qr_payload=old_qr,
        canonical_input_tag_qr=old_qr,
        active_label_qr_payload=old_qr.replace(
            "LBL-OLD-INTERNAL", "LBL-NEW-INTERNAL"
        ),
        active_label_id="LBL-NEW-INTERNAL",
        active_label_business_date="2026-08-01",
        active_label_worker_code="8월1일-1",
        active_label_resolution="OVERLAY_REPLACED",
        item_id="ITEM-1",
        member_count=2,
        membership_hash="b" * 64,
        state_fields=lambda: {
            "active_label_id": "LBL-NEW-INTERNAL",
            "phs_label_replaced_scan": True,
        },
    )
    renders = []
    app = label_module.Label_Match.__new__(label_module.Label_Match)
    app.current_set_info = {"id": "SET-1", "raw": [], "parsed": []}
    app._phs_replacement_notice_pairs = set()
    app._phs_label_guidance_notice = None
    app.package_outbox = PackageOutbox(tmp_path / "replacement.sqlite3")
    app._update_on_success_scan = lambda *_args, **_kwargs: None
    app.data_manager = SimpleNamespace(
        save_directory=str(tmp_path),
        process_name="포장실",
        unique_id="PACK-PC-1",
        log_event=lambda *_args, **_kwargs: None,
        flush=lambda timeout=None: True,
    )
    app._save_current_set_state = lambda: True
    app._render_operator_workbench = lambda: renders.append("render")
    app._focus_scan_entry_if_available = lambda: None

    assert app._accept_resolved_central_phs2_scan(
        evidence,
        {"bundle_id": "BUNDLE-1"},
        None,
    ) is True

    notice = app._phs_label_guidance_notice
    assert notice.message == expected
    assert notice.tone == "warning"
    assert "LBL-" not in notice.message
    assert renders
