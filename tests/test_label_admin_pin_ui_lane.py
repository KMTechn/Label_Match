"""Recovery PIN callbacks use the real lane with injected transport and fake Tk."""

import hashlib
import threading
from types import SimpleNamespace

import pytest

import Label_Match as app_module
from tests.test_label_admin_pin import _active_set_pin_app, _intent, _pin_app
from tests.test_tk_serial_ui_lane import FakeTkRoot
from tk_serial_ui_lane import Admission, LaneState, TkSerialUiLane


@pytest.fixture
def pin_lane(tmp_path, monkeypatch, lock_remote):
    app, journal, server = _pin_app(tmp_path, monkeypatch)
    root = FakeTkRoot()
    app._ui_lane_generation = 0
    app.operator_workbench_ready = False
    app._focus_scan_entry_if_available = lambda: None
    app.after = root.after
    app.winfo_exists = lambda: not root.destroyed
    renders, rejected = [], []

    def render():
        assert threading.get_ident() == root.owner_thread_id
        renders.append(1)

    app._render_operator_workbench = render
    app._show_ui_lane_rejection = rejected.append
    app.ui_lane = TkSerialUiLane(
        root, poll_ms=1, generation_provider=lambda: app._ui_lane_generation,
        on_runner_fault=app._handle_ui_lane_fault,
    )
    window = SimpleNamespace(alive=True)
    window.winfo_exists = lambda: window.alive
    item = "F5:" + hashlib.sha256(journal.path.read_bytes()).hexdigest()
    finished, errors = [], []
    original_prompt = app._prompt_package_admin_pin
    prompts = []

    def prompt():
        assert threading.get_ident() == root.owner_thread_id
        assert app.ui_lane.state is LaneState.IDLE
        prompts.append(1)
        return original_prompt()

    app._prompt_package_admin_pin = prompt
    context = SimpleNamespace(
        app=app, root=root, journal=journal, server=server, window=window,
        item=item, finished=finished, errors=errors, renders=renders,
        rejected=rejected, prompts=prompts,
    )

    def submit(action="LABEL.F5_HOLD", finish=None):
        return app._submit_package_pin_action(
            action, context.item, window=window,
            finish=finished.append if finish is None else finish,
            fail=errors.append,
        )

    context.submit = submit
    yield context
    if app.ui_lane.is_busy():
        root.run_until(lambda: not app.ui_lane.is_busy())
    app.ui_lane.close_idle()


@pytest.mark.parametrize("boundary", ["verify", "redeem", "status", "central"])
def test_pin_transport_wait_keeps_ui_callbacks_and_rejects_duplicate(pin_lane, monkeypatch, boundary):
    p = pin_lane
    entered, release = threading.Event(), threading.Event()
    worker_calls = []
    action = "LABEL.F5_HOLD"

    def block():
        worker_calls.append(threading.get_ident())
        assert threading.get_ident() == p.app.ui_lane.worker_thread_id
        import writer_session_fence
        assert not getattr(writer_session_fence._WRITER_LOCAL, "depth", 0)
        entered.set()
        assert release.wait(2)

    method = {"verify": "verify", "redeem": "redeem", "status": "status"}.get(boundary)
    if method:
        original = getattr(p.app._admin_pin_client, method)
        def blocked(*args, **kwargs):
            block()
            return original(*args, **kwargs)
        monkeypatch.setattr(p.app._admin_pin_client, method, blocked)
    if boundary == "status":
        # Retain a consumed intent whose local effect is already durable.
        original_transition = p.app._admin_pin_store.transition
        def interrupted(key, state, **kwargs):
            if state == "APPLIED":
                raise OSError("cut before applied marker")
            return original_transition(key, state, **kwargs)
        monkeypatch.setattr(p.app._admin_pin_store, "transition", interrupted)
        assert p.app._run_package_pin_action("LABEL.F5_HOLD", p.item) is False
        monkeypatch.setattr(p.app._admin_pin_store, "transition", original_transition)
        p.prompts.clear()
    if boundary == "central":
        state = p.journal.load()
        state["exchange_id"] = "exchange-test"
        p.journal.save(state)
        p.item = "F5:" + hashlib.sha256(p.journal.path.read_bytes()).hexdigest()
        def query(exchange_id, **kwargs):
            block()
            return {"exchange": {"exchange_id": exchange_id, "state": "PREPARED"}}
        p.app.phs_label_exchange_coordinator.client = SimpleNamespace(get_phs_label_exchange=query)
        action = "LABEL.RECHECK"
    try:
        assert p.submit(action)
        p.root.run_until(entered.is_set)
        callbacks = []
        p.root.after(0, lambda: callbacks.append(threading.get_ident()))
        p.root.run_until(lambda: bool(callbacks))
        assert callbacks == [p.root.owner_thread_id]
        assert not p.submit()
        assert p.rejected == ["busy"]
        assert len(worker_calls) == 1
        assert p.prompts == ([] if boundary == "status" else [1])
    finally:
        release.set()
    p.root.run_until(lambda: not p.app._package_pin_pending_targets)
    assert p.finished and not p.errors
    assert p.server.verify_calls == p.server.redeem_calls == 1
    assert _intent(p.app)[2] == "APPLIED"


@pytest.mark.parametrize("stale", [False, True])
def test_closed_or_stale_pin_window_keeps_durable_apply_without_render(pin_lane, monkeypatch, stale):
    p = pin_lane
    raw = p.journal.path.read_bytes()
    entered, release = threading.Event(), threading.Event()
    original = p.app._admin_pin_client.redeem

    def redeem(**kwargs):
        entered.set()
        assert release.wait(2)
        return original(**kwargs)

    monkeypatch.setattr(p.app._admin_pin_client, "redeem", redeem)
    try:
        assert p.submit()
        p.root.run_until(entered.is_set)
        if stale:
            p.app._advance_ui_lane_generation()
        else:
            p.window.alive = False
    finally:
        release.set()
    p.root.run_until(lambda: not p.app._package_pin_pending_targets)
    assert _intent(p.app)[2] == "APPLIED"
    assert bytes(p.app.package_outbox.get_label_exchange_hold(p.item[3:])["journal_bytes"]) == raw
    assert not p.journal.path.exists()
    assert not p.finished and not p.errors and not p.renders


@pytest.mark.parametrize("result", [None, "", {}])
def test_empty_completed_apply_uses_existing_fallback_and_one_effect(pin_lane, monkeypatch, result):
    p = pin_lane
    original = p.app._package_pin_apply
    def empty_apply(*args):
        original(*args)
        return result
    monkeypatch.setattr(p.app, "_package_pin_apply", empty_apply)
    assert p.submit("LABEL.RECHECK")
    p.root.run_until(lambda: not p.app._package_pin_pending_targets)
    assert p.finished == [result] and not p.errors
    assert p.app._admin_pin_last_message == "관리자 확인을 마치지 못했습니다."
    assert _intent(p.app)[2] == "APPLIED"
    key = _intent(p.app)[0]
    rows = p.app.package_outbox.pin_effect_rows(key)
    assert sum(row["action"] == "RECHECK" for row in rows) == 1
    assert p.server.verify_calls == p.server.redeem_calls == 1


@pytest.mark.parametrize("failure", ["cancel", "prompt", "prepare", "apply", "finish", "submit"])
def test_pending_pin_flag_is_released_on_every_exit(pin_lane, monkeypatch, failure):
    p = pin_lane
    with monkeypatch.context() as patch:
        def raises(*args, **kwargs):
            raise RuntimeError("injected UI or lane failure")
        if failure == "cancel":
            patch.setattr(p.app, "_prompt_package_admin_pin", lambda: None)
        elif failure == "prompt":
            patch.setattr(p.app, "_prompt_package_admin_pin", raises)
        elif failure == "prepare":
            patch.setattr(p.app._admin_pin_store, "pending", raises)
        elif failure == "apply":
            patch.setattr(p.app, "_execute_package_pin_action", raises)
        elif failure == "submit":
            patch.setattr(p.app, "_submit_ui_lane_task", raises)
        if failure == "submit":
            with pytest.raises(RuntimeError):
                p.submit()
        else:
            assert p.submit(finish=raises if failure == "finish" else None)
            p.root.run_until(lambda: not p.app._package_pin_pending_targets)
    assert not p.app._package_pin_pending_targets
    p.finished.clear()
    assert p.submit()
    p.root.run_until(lambda: not p.app._package_pin_pending_targets)
    assert p.finished == [True]
    assert _intent(p.app)[2] == "APPLIED"


def test_preparation_own_generation_change_continues_same_click_to_apply(pin_lane, monkeypatch):
    p = pin_lane
    original = p.app._prepare_package_pin_action

    def prepare(*args):
        assert threading.get_ident() == p.app.ui_lane.worker_thread_id
        preparation = original(*args)
        assert preparation[0] is False
        # Local cleanup fences queued scans from the previous screen. That
        # fence must still allow this request's idle PIN/apply continuation.
        p.app._advance_ui_lane_generation()
        return preparation

    monkeypatch.setattr(p.app, "_prepare_package_pin_action", prepare)
    assert p.submit()
    p.root.run_until(lambda: not p.app._package_pin_pending_targets)
    assert p.app._ui_lane_generation == 1
    assert p.prompts == [1] and p.finished == [True] and not p.errors
    assert p.server.verify_calls == p.server.redeem_calls == 1
    assert _intent(p.app)[2] == "APPLIED"


@pytest.mark.parametrize("result", [None, "", {}])
@pytest.mark.parametrize("target_kind", ["set", "f5"])
def test_consumed_replay_empty_apply_finishes_same_intent_without_new_pin(pin_lane, tmp_path, monkeypatch, result, target_kind):
    p = pin_lane
    if target_kind == "f5":
        app, server, item = p.app, p.server, p.item
    else:
        app, _journal, _current, server, item = _active_set_pin_app(
            tmp_path / "replay", monkeypatch, linked=False,
        )
    app._ui_lane_generation = 0
    app.operator_workbench_ready = False
    app._focus_scan_entry_if_available = lambda: None
    app._render_operator_workbench = p.app._render_operator_workbench
    app.after = p.root.after
    app.winfo_exists = p.app.winfo_exists
    app.ui_lane = p.app.ui_lane
    app.ui_lane._generation_provider = lambda: app._ui_lane_generation
    original = app._package_pin_apply

    def interrupted(*args):
        raise OSError("cut after redemption before local apply")

    with monkeypatch.context() as patch:
        patch.setattr(app, "_package_pin_apply", interrupted)
        assert app._run_package_pin_action("LABEL.RECHECK", item) is False
    key = _intent(app)[0]

    def unexpected_prompt():
        raise AssertionError("consumed replay must not request a fresh PIN")

    def empty_apply(*args):
        original(*args)
        return result

    monkeypatch.setattr(app, "_prompt_package_admin_pin", unexpected_prompt)
    monkeypatch.setattr(app, "_package_pin_apply", empty_apply)
    assert app._submit_package_pin_action(
        "LABEL.RECHECK", item, window=p.window,
        finish=p.finished.append, fail=p.errors.append,
    )
    p.root.run_until(lambda: not app._package_pin_pending_targets)
    assert p.finished == [result] and not p.errors
    assert _intent(app)[0] == key and _intent(app)[2] == "APPLIED"
    assert app._admin_pin_last_message == "관리자 확인을 마치지 못했습니다."
    assert server.status_calls == server.verify_calls == server.redeem_calls == 1
    rows = app.package_outbox.pin_effect_rows(key)
    assert sum(row["action"] == "RECHECK" for row in rows) == 1


@pytest.mark.parametrize("closed", [False, True])
def test_durable_block_keeps_model_on_worker_and_notice_label_on_ui(pin_lane, monkeypatch, closed):
    p = pin_lane
    updates = []

    class Notice:
        def configure(self, **kwargs):
            assert threading.get_ident() == p.root.owner_thread_id
            updates.append(kwargs["text"])

    p.app.workflow_notice_label = Notice()
    p.app.current_set_info = {"id": None, "raw": []}
    p.app._workflow_total_scan_count = lambda: 1
    original = p.app._package_pin_apply

    def apply(*args):
        assert threading.get_ident() == p.app.ui_lane.worker_thread_id
        result = original(*args)
        p.app._publish_durable_commit_block(OSError("injected durable failure"))
        assert p.app._workflow_blocking_notice is not None
        assert not updates
        p.window.alive = not closed
        return result

    monkeypatch.setattr(p.app, "_package_pin_apply", apply)
    assert p.submit()
    p.root.run_until(lambda: not p.app._package_pin_pending_targets)
    assert _intent(p.app)[2] == "APPLIED" and not p.errors
    assert len(updates) == (0 if closed else 1)
    assert p.finished == ([] if closed else [True])


def test_second_stage_rejection_clears_pending_without_verifying(pin_lane, monkeypatch):
    p = pin_lane
    original = p.app._submit_ui_lane_task
    calls = []
    def submit(**kwargs):
        calls.append(kwargs["name"])
        return original(**kwargs) if len(calls) == 1 else Admission(False, reason="busy")
    monkeypatch.setattr(p.app, "_submit_ui_lane_task", submit)
    assert p.submit()
    p.root.run_until(lambda: not p.app._package_pin_pending_targets)
    assert len(calls) == 2
    assert p.server.verify_calls == p.server.redeem_calls == 0
    assert not p.finished and not p.errors


def _workbench(pin_lane, monkeypatch):
    p = pin_lane
    widgets, values = [], []
    class Widget:
        def __init__(self, *args, **kwargs):
            assert threading.get_ident() == p.root.owner_thread_id
            self.options, self.rows, self.bindings = kwargs, [], {}
            self.alive, self.refreshes = True, 0
            widgets.append(self)
        def title(self, *args):
            pass
        def transient(self, *args):
            pass
        def pack(self, **kwargs):
            pass
        def bind(self, name, callback):
            self.bindings[name] = callback
        def delete(self, *args):
            assert threading.get_ident() == p.root.owner_thread_id
            self.rows.clear()
            self.refreshes += 1
        def insert(self, *args):
            assert threading.get_ident() == p.root.owner_thread_id
            self.rows.append(args[-1])
        def configure(self, **kwargs):
            assert threading.get_ident() == p.root.owner_thread_id
            self.options.update(kwargs)
        def yview(self, *args):
            pass
        def set(self, *args):
            pass
        def curselection(self):
            return (0,) if self.rows else ()
        def winfo_exists(self):
            assert threading.get_ident() == p.root.owner_thread_id
            return self.alive
    class Value:
        def __init__(self, value=""):
            self.value, self.updates = value, []
            values.append(self)
        def set(self, value):
            assert threading.get_ident() == p.root.owner_thread_id
            self.value = value
            self.updates.append(value)
    for toolkit, names in (
        (app_module.tk, ("Toplevel", "Listbox", "Text")),
        (app_module.ttk, ("Frame", "Label", "Button", "Scrollbar")),
    ):
        for name in names:
            monkeypatch.setattr(toolkit, name, Widget)
    monkeypatch.setattr(app_module.tk, "StringVar", Value)
    p.app.run_tests = False
    window = p.app._show_package_recovery_workbench()
    listing = next(w for w in widgets if w.options.get("height") == 8)
    buttons = {w.options["text"]: w.options["command"] for w in widgets if "command" in w.options and "text" in w.options}
    return window, listing, values[0], buttons


@pytest.mark.parametrize("closed", [False, True])
def test_real_workbench_callback_keeps_one_request_and_existing_hold_text(pin_lane, monkeypatch, closed):
    p = pin_lane
    window, listing, status, buttons = _workbench(p, monkeypatch)
    raw = p.journal.path.read_bytes()
    entered, release = threading.Event(), threading.Event()
    original = p.app._admin_pin_client.redeem
    def redeem(**kwargs):
        entered.set()
        assert release.wait(2)
        return original(**kwargs)
    monkeypatch.setattr(p.app._admin_pin_client, "redeem", redeem)
    refreshes = listing.refreshes
    try:
        buttons["보류 후 계속"]()
        p.root.run_until(entered.is_set)
        p.root.after(0, lambda: setattr(window, "alive", not closed))
        p.root.run_one()
        if closed:
            window.alive = False
        else:
            buttons["보류 후 계속"]()
            assert p.rejected == ["busy"]
        assert p.server.redeem_calls == 0
    finally:
        release.set()
    p.root.run_until(lambda: not p.app._package_pin_pending_targets)
    assert p.server.verify_calls == p.server.redeem_calls == 1
    assert _intent(p.app)[2] == "APPLIED"
    assert bytes(p.app.package_outbox.get_label_exchange_hold(p.item[3:])["journal_bytes"]) == raw
    if closed:
        assert not status.updates and listing.refreshes == refreshes
    else:
        assert status.updates == ["해당 트레이의 원본·요청 키를 보류하고 다른 트레이 작업을 계속할 수 있습니다."]
        assert listing.refreshes == refreshes + 1


def test_empty_result_in_real_workbench_refreshes_once_with_fallback(pin_lane, monkeypatch):
    p = pin_lane
    _window, listing, status, buttons = _workbench(p, monkeypatch)
    original = p.app._package_pin_apply
    def empty_apply(*args):
        original(*args)
        return None
    monkeypatch.setattr(p.app, "_package_pin_apply", empty_apply)
    refreshes = listing.refreshes
    buttons["같은 요청 다시 확인"]()
    p.root.run_until(lambda: not p.app._package_pin_pending_targets)
    assert status.updates == ["관리자 확인을 마치지 못했습니다."]
    assert listing.refreshes == refreshes + 1 and _intent(p.app)[2] == "APPLIED"


def test_closed_preparation_does_not_open_pin_or_verify(pin_lane, monkeypatch):
    p = pin_lane
    entered, release = threading.Event(), threading.Event()
    original = p.app._prepare_package_pin_action
    def prepare(*args):
        entered.set()
        assert release.wait(2)
        return original(*args)
    monkeypatch.setattr(p.app, "_prepare_package_pin_action", prepare)
    try:
        assert p.submit()
        p.root.run_until(entered.is_set)
        p.window.alive = False
    finally:
        release.set()
    p.root.run_until(lambda: not p.app._package_pin_pending_targets)
    assert not p.prompts and not p.finished and not p.errors
    assert p.server.verify_calls == p.server.redeem_calls == 0
    assert p.journal.path.exists() and not p.app._admin_pin_store.path.exists()


def test_destroy_event_releases_window_reservation_while_apply_continues(pin_lane, monkeypatch):
    p = pin_lane
    window, _listing, status, buttons = _workbench(p, monkeypatch)
    p.app._schedule_scan_entry_focus_return = lambda: None
    entered, release = threading.Event(), threading.Event()
    original = p.app._admin_pin_client.redeem
    def redeem(**kwargs):
        entered.set()
        assert release.wait(2)
        return original(**kwargs)
    monkeypatch.setattr(p.app._admin_pin_client, "redeem", redeem)
    try:
        buttons["보류 후 계속"]()
        p.root.run_until(entered.is_set)
        window.alive = False
        window.bindings["<Destroy>"](SimpleNamespace(widget=window))
        assert not p.app._package_pin_pending_targets
        assert p.app.ui_lane.is_busy()
    finally:
        release.set()
    p.root.run_until(lambda: not p.app.ui_lane.is_busy())
    p.root.run_one()
    assert _intent(p.app)[2] == "APPLIED" and not status.updates
    assert p.server.verify_calls == p.server.redeem_calls == 1


@pytest.mark.parametrize("phase", [1, 2])
def test_ui_idle_schedule_failure_releases_pending_and_preserves_effect(pin_lane, monkeypatch, phase):
    p = pin_lane
    original = p.app.after
    calls = []
    def after(*args):
        calls.append(1)
        if len(calls) == phase:
            raise RuntimeError("UI idle scheduling failed")
        return original(*args)
    monkeypatch.setattr(p.app, "after", after)
    assert p.submit()
    p.root.run_until(lambda: p.app.ui_lane.state is LaneState.BROKEN)
    assert not p.app._package_pin_pending_targets
    if phase == 1:
        assert p.server.verify_calls == p.server.redeem_calls == 0
    else:
        assert _intent(p.app)[2] == "APPLIED"
        assert p.server.verify_calls == p.server.redeem_calls == 1


@pytest.mark.parametrize("closed,stale", [(False, False), (True, False), (False, True)])
def test_hold_resets_local_tray_on_worker_and_progress_only_on_ui(pin_lane, tmp_path, monkeypatch, closed, stale):
    p = pin_lane
    app, journal, current, server, item = _active_set_pin_app(tmp_path / "active", monkeypatch, linked=False)
    # Use the real reset rather than the old synchronous fixture's reset double.
    del app._reset_current_set
    app._ui_lane_generation = 0
    app.operator_workbench_ready = False
    app.initialized_successfully = False
    app._focus_scan_entry_if_available = lambda: None
    app.after = p.root.after
    app.winfo_exists = p.app.winfo_exists
    progress = []
    class Progress:
        def __setitem__(self, name, value):
            assert threading.get_ident() == p.root.owner_thread_id
            progress.append((name, value))
    app.progress_bar = Progress()
    app.ui_lane = p.app.ui_lane
    app.ui_lane._generation_provider = lambda: app._ui_lane_generation
    entered, release = threading.Event(), threading.Event()
    original = app._admin_pin_client.redeem
    def redeem(**kwargs):
        entered.set()
        assert release.wait(2)
        return original(**kwargs)
    monkeypatch.setattr(app._admin_pin_client, "redeem", redeem)
    completed = []
    try:
        assert app._submit_package_pin_action(
            "LABEL.SET_HOLD", item, window=p.window,
            finish=completed.append, fail=p.errors.append,
        )
        p.root.run_until(entered.is_set)
        p.window.alive = not closed
        if stale:
            app._advance_ui_lane_generation()
    finally:
        release.set()
    p.root.run_until(lambda: not app._package_pin_pending_targets)
    assert app.current_set_info["id"] is None and not current.exists()
    assert _intent(app)[2] == "APPLIED" and not p.errors
    assert completed == ([] if closed or stale else [True])
    assert progress == ([] if closed or stale else [("value", 0)])
    assert server.verify_calls == server.redeem_calls == 1
