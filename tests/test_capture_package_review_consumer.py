"""Exercise the unchanged capture consumer with real review methods, no Tk."""
from types import MethodType, SimpleNamespace

import pytest

from tests.test_label_match_core import load_label_match_module
from tools import capture_label_operator_ui as capture


@pytest.mark.parametrize('state', ['waiting', 'cancellation_conflict'])
def test_capture_fixture_accepts_production_review_refresh(state):
    module = load_label_match_module()
    app = SimpleNamespace(
        current_set_info={}, operator_workbench_ready=False, run_tests=True,
        history_tree=None, session_tree=None,
        after=lambda *args: 'scheduled',
        _poll_package_outbox_drain=lambda: None,
        _ui_lane_is_busy=lambda: False,
        _render_operator_workbench=lambda: None,
        _refresh_operator_workbench=lambda: None,
    )
    for name in ['_refresh_package_cancellation_review_notice', '_start_package_outbox_drain',
                 '_package_review_context', '_read_package_review_snapshot', '_schedule_package_outbox_poll']:
        method = getattr(module.Label_Match, name, None)
        if method is not None:
            setattr(app, name, MethodType(method, app))
    fixture = next(value for value in capture.build_state_fixtures() if value.state_id == state)
    try:
        _, method = capture.apply_state_fixture(app, fixture)
        assert method == '_refresh_operator_workbench'
        assert not app.package_outbox_thread.is_alive()
        assert app._package_review_snapshot.context == app._package_review_context()
        assert len(app._package_cancellation_review_rows) == len(capture._fixture_cancellation_conflict_rows(fixture))
    finally:
        worker = getattr(app, 'package_outbox_thread', None)
        if worker is not None:
            worker.join(5)
            assert not worker.is_alive()
