"""Freeze reviewed B01 consumers, including callbacks, overrides and dynamic bindings.

If this fails, review Thread/snapshot use at the changed callers before updating
the explicit inventory; line numbers and repeated calls do not affect the set.
"""
import ast
import subprocess
from pathlib import Path


def test_package_review_api_consumer_set_requires_review():
    repo = Path(__file__).resolve().parents[1]
    apis = {
        'ExchangeReviewSnapshot',
        'PackageReviewSnapshot',
        '_package_review_context',
        '_package_review_result',
        '_package_review_snapshot',
        '_poll_package_outbox_drain',
        '_read_package_review_snapshot',
        '_refresh_package_cancellation_review_notice',
        '_run_scheduled_package_outbox_drain',
        '_schedule_package_outbox_poll',
        '_start_package_outbox_drain',
    }
    expected = {
        'Label_Match.py:': 'ExchangeReviewSnapshot,PackageReviewSnapshot',
        'Label_Match.py:Label_Match.__init__': '_start_package_outbox_drain',
        'Label_Match.py:Label_Match._cancel_completed_tray_by_label': '_start_package_outbox_drain',
        'Label_Match.py:Label_Match._delete_selected_row': '_start_package_outbox_drain',
        'Label_Match.py:Label_Match._finalize_set': '_start_package_outbox_drain',
        'Label_Match.py:Label_Match._package_review_context': '_package_review_context',
        'Label_Match.py:Label_Match._poll_package_outbox_drain': '_package_review_result,_poll_package_outbox_drain,_refresh_package_cancellation_review_notice,_run_scheduled_package_outbox_drain,_schedule_package_outbox_poll',
        'Label_Match.py:Label_Match._read_package_review_snapshot': 'ExchangeReviewSnapshot,PackageReviewSnapshot,_package_review_context,_read_package_review_snapshot',
        'Label_Match.py:Label_Match._refresh_package_cancellation_review_notice': '_package_review_context,_package_review_snapshot,_refresh_package_cancellation_review_notice,_start_package_outbox_drain',
        'Label_Match.py:Label_Match._render_operator_workbench': '_package_review_context,_package_review_snapshot,_refresh_package_cancellation_review_notice',
        'Label_Match.py:Label_Match._reset_current_set': '_refresh_package_cancellation_review_notice',
        'Label_Match.py:Label_Match._run_scheduled_package_outbox_drain': '_run_scheduled_package_outbox_drain,_start_package_outbox_drain',
        'Label_Match.py:Label_Match._schedule_package_outbox_poll': '_poll_package_outbox_drain,_schedule_package_outbox_poll',
        'Label_Match.py:Label_Match._start_package_outbox_drain': '_package_review_context,_schedule_package_outbox_poll,_start_package_outbox_drain',
        'Label_Match.py:Label_Match._start_package_outbox_drain.worker': '_package_review_result,_read_package_review_snapshot',
        'Label_Match.py:Label_Match.on_closing': '_start_package_outbox_drain',
        'tests/test_capture_label_operator_ui.py:test_apply_conflict_fixture_uses_real_nonblocking_review_renderer': '_package_review_context,_poll_package_outbox_drain,_read_package_review_snapshot,_refresh_package_cancellation_review_notice,_schedule_package_outbox_poll,_start_package_outbox_drain',
        'tests/test_capture_package_review_consumer.py:test_capture_fixture_accepts_production_review_refresh': '_package_review_context,_package_review_snapshot,_poll_package_outbox_drain,_read_package_review_snapshot,_refresh_package_cancellation_review_notice,_schedule_package_outbox_poll,_start_package_outbox_drain',
        'tests/test_deferred_intent_capture.py:test_validated_materializer_flows_through_f3_durable_completion': '_start_package_outbox_drain',
        'tests/test_label_match_actual_input_walkthrough.py:test_walkthrough_workbench_can_render_before_review_snapshot': 'PackageReviewSnapshot,_package_review_snapshot,_start_package_outbox_drain',
        'tests/test_label_match_core.py:_b1_app': '_start_package_outbox_drain',
        'tests/test_label_match_core.py:test_existing_cancellation_conflicts_refresh_without_configured_client': '_poll_package_outbox_drain,_refresh_package_cancellation_review_notice,_start_package_outbox_drain',
        'tests/test_label_match_core.py:test_finalize_set_waits_for_durable_log_before_mutating_active_state': '_start_package_outbox_drain',
        'tests/test_label_match_core.py:test_historical_package_review_reconciliation_never_blocks_scanning': '_read_package_review_snapshot,_refresh_package_cancellation_review_notice',
        'tests/test_label_match_core.py:test_locally_committed_package_conflict_preserves_success_and_hides_internals': '_read_package_review_snapshot,_refresh_package_cancellation_review_notice',
        'tests/test_label_match_core.py:test_on_closing_replaces_closed_data_manager_after_close_failure': '_start_package_outbox_drain',
        'tests/test_label_match_core.py:test_package_review_query_failure_retains_warning_until_confirmed_refresh': '_read_package_review_snapshot,_refresh_package_cancellation_review_notice',
        'tests/test_label_match_core.py:test_package_worker_never_calls_tk_after_from_background_thread': '_refresh_package_cancellation_review_notice,_start_package_outbox_drain',
        'tests/test_label_match_core.py:test_recoverable_conflict_reset_preserves_evidence_and_dismisses_warning': '_poll_package_outbox_drain',
        'tests/test_label_match_core.py:test_terminal_cancellation_conflict_stays_in_separate_operator_review_notice': '_read_package_review_snapshot,_refresh_package_cancellation_review_notice',
        'tests/test_label_responsiveness.py:test_package_review_snapshot_ignores_stale_context': '_read_package_review_snapshot,_refresh_package_cancellation_review_notice',
        'tests/test_label_responsiveness.py:test_package_review_writer_lock_does_not_block_tk_callback': '_package_review_snapshot,_poll_package_outbox_drain,_refresh_package_cancellation_review_notice',
        'tests/test_label_responsiveness.py:test_workbench_exchange_notice_uses_snapshot_but_action_guard_reads_store': '_package_review_snapshot,_read_package_review_snapshot',
        'tests/test_label_ui_lane_integration.py:test_close_cancel_rearm_rejection_relatches_and_preserves_resume_flag': '_start_package_outbox_drain',
        'tests/test_label_ui_lane_integration.py:test_close_cancel_rearms_deferred_validation_through_real_scheduler': '_start_package_outbox_drain',
        'tests/test_label_ui_lane_integration.py:test_close_cancel_restore_failure_stays_fail_closed_and_retries': '_start_package_outbox_drain',
        'tests/test_label_ui_lane_integration.py:test_close_failure_compensates_durable_app_close_before_resume': '_start_package_outbox_drain',
        'tests/test_label_ui_lane_integration.py:test_close_failure_keeps_lane_alive_for_resumed_f5_submit': '_start_package_outbox_drain',
        'tests/test_label_ui_lane_integration.py:test_close_retries_manager_recovery_before_rolling_back': '_start_package_outbox_drain',
        'tests/test_label_ui_lane_integration.py:test_f3_lease_outbox_and_flush_run_off_tk_before_ui_apply': '_start_package_outbox_drain',
        'tools/capture_label_operator_ui.py:apply_state_fixture': '_package_review_result,_refresh_package_cancellation_review_notice',
        'tools/label_match_actual_input_walkthrough.py:_make_app.WalkthroughLabelMatch._start_package_outbox_drain': '_start_package_outbox_drain',
    }
    observed = {}

    class References(ast.NodeVisitor):
        def __init__(self, path):
            self.path, self.scope = path, []

        def record(self, name):
            if name in apis:
                key = f"{self.path}:{'.'.join(self.scope)}"
                observed.setdefault(key, set()).add(name)

        def visit_ClassDef(self, node):
            self.scope.append(node.name)
            self.generic_visit(node)
            self.scope.pop()

        def visit_FunctionDef(self, node):
            self.scope.append(node.name)
            self.record(node.name)
            self.generic_visit(node)
            self.scope.pop()

        visit_AsyncFunctionDef = visit_FunctionDef

        def visit_Attribute(self, node):
            self.record(node.attr)
            self.generic_visit(node)

        def visit_Name(self, node):
            self.record(node.id)

        def visit_keyword(self, node):
            self.record(node.arg)
            self.generic_visit(node)

        def visit_Constant(self, node):
            if isinstance(node.value, str):
                self.record(node.value)

    paths = subprocess.check_output(
        ["git", "ls-files", "--cached", "--others", "--exclude-standard", "-z", "--", "*.py"],
        cwd=repo,
    ).decode().split("\0")
    for path in filter(None, paths):
        source = repo / path
        if source.resolve() != Path(__file__).resolve():
            References(path).visit(ast.parse(source.read_text(encoding="utf-8-sig")))
    actual = {key: ",".join(sorted(names)) for key, names in observed.items()}
    assert actual == expected, "Review changed Thread/snapshot callers before updating the inventory"
