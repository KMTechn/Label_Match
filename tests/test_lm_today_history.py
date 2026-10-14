from unittest import mock

import pytest

import Label_Match as lm
from label_screen_bridge import LabelMatchScreenProcessor
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
