from __future__ import annotations

from copy import deepcopy

import pytest

import Label_Match as label_match_module
from Label_Match import Label_Match
from package_logistics import PackageLogisticsError


class FakeLabel:
    def __init__(self):
        self.options = {}

    def configure(self, **kwargs):
        self.options.update(kwargs)

    config = configure

    def cget(self, name):
        return self.options.get(name, "")

    def focus_set(self):
        self.options["focused"] = True

    focus_force = focus_set


class RecordingDataManager:
    def __init__(self):
        self.events = []

    def log_event(self, event, details):
        self.events.append((event, details))


def _current(*scans):
    return {
        "id": "set-1",
        "raw": list(scans),
        "parsed": list(scans),
        "start_time": None,
        "error_count": 0,
        "has_error_or_reset": False,
        "phase": None,
        "item_name_override": None,
        "production_date": None,
        "sealed_transfer": None,
        "exact_rescan_active": False,
        "exact_rescan_complete": False,
        "exact_rescan_target_count": 0,
        "exact_rescan_source_bundle_id": "",
        "exact_rescan_barcodes": [],
    }


def _error_app(*scans, workbench_ready=True):
    app = Label_Match.__new__(Label_Match)
    app.current_set_info = _current(*scans)
    app.data_manager = RecordingDataManager()
    app.run_tests = False
    app.is_running_simulation = False
    app.is_blinking = False
    app.status_label = FakeLabel()
    app.big_display_label = FakeLabel()
    app.workflow_notice_frame = object() if workbench_ready else None
    app.workflow_notice_label = FakeLabel() if workbench_ready else None
    app.operator_workbench_ready = workbench_ready
    app._render_calls = []
    app._render_operator_workbench = lambda *args, **kwargs: app._render_calls.append((args, kwargs))
    app._modal_calls = []
    app._trigger_modal_error = lambda *args, **kwargs: app._modal_calls.append((args, kwargs))
    app._play_sound = lambda *args, **kwargs: None
    app._save_current_set_state = lambda: None
    app._update_manual_complete_button_state = lambda: None
    app.update_big_display = lambda text, color="": app.big_display_label.configure(
        text=text,
        color=color,
    )
    return app


@pytest.mark.parametrize(
    ("method_name", "args", "expected_text"),
    [
        ("_handle_input_error", ("BAD-SCAN",), "입력 오류"),
        ("_handle_mismatch", ("WRONG-PRODUCT", "MASTER-001"), "제품 불일치"),
    ],
)
def test_error_is_inline_when_workbench_is_ready_and_keeps_accepted_scans(
    method_name,
    args,
    expected_text,
):
    app = _error_app("MASTER-001", "PRODUCT-001", workbench_ready=True)
    accepted_before = deepcopy(app.current_set_info["parsed"])

    getattr(Label_Match, method_name)(app, *args)

    assert app._modal_calls == []
    assert app.current_set_info["parsed"] == accepted_before
    assert app.current_set_info["raw"] == accepted_before
    assert app.current_set_info["error_count"] == 1
    assert app.current_set_info["has_error_or_reset"] is True
    assert expected_text in app.workflow_notice_label.cget("text")
    assert app._render_calls


def test_error_uses_legacy_modal_only_before_workbench_exists():
    app = _error_app("MASTER-001", workbench_ready=False)

    Label_Match._handle_input_error(
        app,
        "BAD-SCAN",
        title="[현품표 형식 오류]",
        reason="잘못된 형식",
    )

    assert len(app._modal_calls) == 1
    assert app._render_calls == []


@pytest.mark.parametrize("key", ["Return", "Escape"])
def test_enter_and_escape_acknowledge_inline_error_before_failed_set_is_reset(key):
    app = _error_app("MASTER-001", "PRODUCT-001", workbench_ready=True)
    app._pending_workflow_error = {
        "result": app.Results.FAIL_INPUT_ERROR,
        "error_details": "BAD-SCAN",
    }
    app._workflow_notice = object()
    app._finalize_calls = []
    app._finalize_set = lambda result, error_details="", **kwargs: app._finalize_calls.append(
        (result, error_details, kwargs)
    )
    accepted_before = deepcopy(app.current_set_info["parsed"])
    event = type("FakeEvent", (), {"keysym": key})()

    handled = Label_Match._acknowledge_workflow_notice(app, event)

    assert handled == "break"
    assert app.current_set_info["parsed"] == accepted_before
    assert app._finalize_calls == [(app.Results.FAIL_INPUT_ERROR, "BAD-SCAN", {})]


def test_full_and_partial_completion_publish_distinct_view_kinds():
    app = _error_app(
        "MASTER-001",
        "PRODUCT-001",
        "PRODUCT-002",
        "PRODUCT-003",
        "FINAL-001",
    )
    published = []
    app._publish_workflow_completion = lambda kind: published.append(kind)

    Label_Match._publish_finalize_completion(app, is_manual_complete=False)
    Label_Match._publish_finalize_completion(app, is_manual_complete=True)

    assert published == ["full", "partial"]


def test_submission_block_keeps_all_five_scans_and_offers_retry_without_reset(monkeypatch):
    scans = ["MASTER-001", "PRODUCT-001", "PRODUCT-002", "PRODUCT-003", "FINAL-001"]
    app = _error_app(*scans, workbench_ready=True)
    app.items_data = {}
    app._queue_authoritative_package = lambda **kwargs: (_ for _ in ()).throw(
        PackageLogisticsError("서버 ACK 확인 필요")
    )
    app._finalize_calls = []
    monkeypatch.setattr(label_match_module.messagebox, "showerror", lambda *args, **kwargs: pytest.fail("workbench error must stay inline"))

    completed = Label_Match._finalize_set(app, app.Results.PASS)

    assert completed is False
    assert app.current_set_info["raw"] == scans
    assert app.current_set_info["parsed"] == scans
    assert "5/5 유지" in app._workflow_blocking_notice.title
    assert "서버 ACK 확인 필요" in app.workflow_notice_label.cget("text")
    assert app._workflow_notice_action_text == "제출 재시도"
    assert callable(app._workflow_notice_action)

    original = deepcopy(app.current_set_info)
    app._finalize_set = lambda result, error_details="", **kwargs: app._finalize_calls.append(
        (result, error_details, kwargs)
    )
    handled = Label_Match._retry_blocked_submission(app)

    assert handled is True
    assert app.current_set_info == original
    assert app._finalize_calls == [(app.Results.PASS, "", {})]
