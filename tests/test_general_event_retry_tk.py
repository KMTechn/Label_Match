"""Headless Tk renderer and visible-footer structure checks (lane ANSWER-1)."""
import ast
import inspect
from pathlib import Path
from types import SimpleNamespace

import pytest

import Label_Match as module
import label_workbench_context
from tests.test_label_operator_workbench import operator_workbench  # noqa: F401


@pytest.mark.parametrize('relay_state', ['normal', 'stopped'])
def test_retry_notice_uses_real_workbench_renderer_and_clears_after_save(operator_workbench, relay_state):
    app = operator_workbench
    app.data_manager = SimpleNamespace(has_pending_log_retries=True)
    app._render_relay_screen_status(relay_state)
    label = app.save_status_label
    assert label.master is app.operator_status_frame
    assert label.winfo_ismapped()
    assert label.cget('text') == module.LABEL_MATCH_LOG_RETRY_NOTICE
    assert label.cget('style') == 'Status.TLabel'
    assert not app.relay_screen_status_label.winfo_ismapped()
    # Run the actual renderer used by save/copy toast timers while pending.
    app._render_save_status('saved-toast')
    assert 'saved-toast' in label.cget('text')
    assert module.LABEL_MATCH_LOG_RETRY_NOTICE in label.cget('text')
    app._render_relay_screen_status(relay_state)
    assert 'saved-toast' in label.cget('text')
    app._render_save_status('')
    assert label.cget('text') == module.LABEL_MATCH_LOG_RETRY_NOTICE
    app.data_manager.has_pending_log_retries = False
    app._render_relay_screen_status(relay_state)
    assert label.cget('text') == ''
    assert label.winfo_ismapped()
    assert label.cget('style') == 'Save.Success.TLabel'


def test_copy_toast_and_its_actual_timer_keep_pending_retry_notice(operator_workbench):
    app = operator_workbench
    app.data_manager = SimpleNamespace(has_pending_log_retries=True)
    timers = []
    app.after = lambda delay, callback: timers.append((delay, callback))
    app._selected_history_iid = lambda: 'selected-row'
    app._history_details_for_iid = lambda iid: {'barcode': 'fixture-barcode'}
    app._barcode_detail_text = lambda details: details['barcode']
    app.clipboard_clear = lambda: None
    app.clipboard_append = lambda text: None
    app._copy_selected_history_barcodes()
    assert '바코드 원문 복사됨' in app.save_status_label.cget('text')
    assert module.LABEL_MATCH_LOG_RETRY_NOTICE in app.save_status_label.cget('text')
    app._render_relay_screen_status('normal')
    assert '바코드 원문 복사됨' in app.save_status_label.cget('text')
    app.data_manager.has_pending_log_retries = False
    app._render_relay_screen_status('normal')
    assert '바코드 원문 복사됨' in app.save_status_label.cget('text')
    assert module.LABEL_MATCH_LOG_RETRY_NOTICE not in app.save_status_label.cget('text')
    app.data_manager.has_pending_log_retries = True
    assert timers[0][0] == 2500
    timers[0][1]()
    assert app.save_status_label.cget('text') == module.LABEL_MATCH_LOG_RETRY_NOTICE


def test_save_notice_footer_is_gridded_and_has_no_hide_or_destroy_path():
    sources = [ast.parse(Path(path).read_text(encoding='utf-8-sig'))
        for path in (module.__file__, label_workbench_context.__file__)]
    footer = next(node for node in sources[1].body
        if isinstance(node, ast.FunctionDef) and node.name == 'create_status_footer')
    grid_calls = [ast.unparse(node.func) for node in ast.walk(footer) if isinstance(node, ast.Call)]
    assert 'self.save_status_label.grid' in grid_calls
    assert 'self.operator_status_frame.grid' in grid_calls
    builder = inspect.getsource(module.Label_Match._create_widgets)
    assert 'label_workbench_context.create_status_footer(' in builder
    forbidden = {'grid_remove', 'grid_forget', 'pack_forget', 'place_forget', 'destroy'}
    for tree in sources:
        for node in ast.walk(tree):
            if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute):
                assert not (node.func.attr in forbidden and ast.unparse(node.func.value)
                    in {'self.save_status_label', 'self.operator_status_frame'})
    # Both existing success notifications must route their timer through the renderer.
    for method in (module.Label_Match._copy_selected_history_barcodes,
                   module.Label_Match._finalize_set):
        source = inspect.getsource(method)
        assert 'self._render_save_status(' in source
        assert 'lambda: self._render_save_status("")' in source
