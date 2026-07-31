import inspect

import Label_Match as label_module


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
    assert "ACTIVE successor" not in source
    assert "ACTIVE로 전환" not in source
    assert "새 현품표가 출력되면 기존 표를 교체해 주세요." in source
