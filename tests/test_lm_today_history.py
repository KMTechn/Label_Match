from datetime import datetime, timedelta
from unittest import mock

import pytest

import Label_Match as lm
from label_screen_bridge import HISTORY_LIMIT, LabelMatchScreenProcessor, _history_rows
from tests.test_label_operator_workbench import _walk, operator_workbench
from tests.test_label_screen_bridge import FakeApp


@pytest.mark.parametrize('args', [{'date': '2025-01-01'}, {'selected_date': '2025-01-01'}, {'target_date': '2025-01-01'}])
def test_web_history_rejects_past_date_queries(args):
    app = FakeApp()
    app._load_history_and_rebuild_summary = mock.Mock()
    processor = LabelMatchScreenProcessor(app, module=lm)
    try:
        result = processor.command('history', args)
        assert result['ok'] is False
        assert result['code'] == 'HISTORY_TODAY_ONLY'
        app._load_history_and_rebuild_summary.assert_not_called()
    finally:
        processor.uninstall()


def test_web_history_refreshes_only_today_without_a_date_picker():
    app = FakeApp()
    app._load_history_and_rebuild_summary = mock.Mock()
    processor = LabelMatchScreenProcessor(app, module=lm)
    try:
        assert processor.command('history', {})['ok'] is True
        app._load_history_and_rebuild_summary.assert_called_once_with(None)
    finally:
        processor.uninstall()


def test_tk_history_has_today_refresh_and_web_report_notice_without_date_picker(operator_workbench):
    app = operator_workbench
    assert app.today_button.cget("command") == app._reload_today_history
    assert "date_search_button" not in app.__dict__
    assert not hasattr(lm, "CalendarWindow")
    assert not hasattr(lm.Label_Match, "_prompt_for_date_and_reload")
    widgets = list(_walk(app.history_card))
    assert app.history_report_notice in widgets
    assert app.history_report_notice.cget("text") == "지난 기록은 회사 웹 리포트에서 봅니다"
    assert not any(widget.cget("text") in {"조회", "날짜 조회"} for widget in widgets)
    assert any(entry.get("command") == app._delete_selected_row
               for values in app.history_context_menu.options.values() if isinstance(values, list)
               for entry in values if isinstance(entry, dict))


@pytest.mark.parametrize("day_offset", [-1, 1])
def test_tk_history_refuses_other_dates_before_mutating_or_starting_loader(day_offset):
    app = lm.Label_Match.__new__(lm.Label_Match)
    app.history_load_generation = 7
    with mock.patch.object(lm.threading, "Thread") as loader:
        with pytest.raises(ValueError, match="HISTORY_TODAY_ONLY"):
            app._load_history_and_rebuild_summary(datetime.now() + timedelta(days=day_offset))
    assert app.history_load_generation == 7
    loader.assert_not_called()


def test_tk_today_history_limit_matches_web_without_losing_working_indexes(operator_workbench):
    app = operator_workbench
    app._history_values_for_display = lambda values, **kwargs: values
    rows = [(str(number), {"values": (number, "tray", "", "", "", "", "통과", "12:00:00"),
                           "tags": ("success",)}) for number in range(HISTORY_LIMIT + 5)]
    details = {iid: {"set_id": iid} for iid, _ in rows}
    app.set_details_map = details
    app.history_row_details_map = dict(details)
    app.global_scanned_set = {"retained-product"}
    app.scan_count = {}
    result = {"sorted_sets": rows, "set_details_map": details, "scan_count": {}, "summary_items": []}
    list(app._history_display_steps(result))
    expected = [iid for iid, _ in rows[-HISTORY_LIMIT:]]
    assert list(app.history_tree.get_children()) == expected
    assert [row["row"] for row in _history_rows(app)] == expected[::-1]
    assert len(app.set_details_map) == HISTORY_LIMIT + 5
    assert app.global_scanned_set == {"retained-product"}

    app.current_set_info = {"id": "live-tray", "parsed": ["tray"]}
    app._details_for_current_set = lambda: {"set_id": "live-tray"}
    app._update_history_tree_in_progress()
    assert len(app.history_tree.get_children()) == HISTORY_LIMIT
    assert app.history_tree.exists("live-tray")
    assert not app.history_tree.exists(expected[0])
    assert len(app.set_details_map) == HISTORY_LIMIT + 5
    assert len(_history_rows(app)) == HISTORY_LIMIT
