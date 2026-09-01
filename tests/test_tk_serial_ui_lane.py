from __future__ import annotations

from collections import deque
import importlib
import threading
import time

import pytest


class FakeTkRoot:
    def __init__(self) -> None:
        self.owner_thread_id = threading.get_ident()
        self.jobs = deque()
        self.cancelled: set[str] = set()
        self.after_threads: list[int] = []
        self.destroyed = False
        self._next_job = 0

    def after(self, delay_ms, callback, *args):
        thread_id = threading.get_ident()
        self.after_threads.append(thread_id)
        if thread_id != self.owner_thread_id:
            raise AssertionError("worker called Tk.after")
        self._next_job += 1
        job_id = f"job-{self._next_job}"
        self.jobs.append((job_id, int(delay_ms), callback, args))
        return job_id

    def after_cancel(self, job_id):
        if threading.get_ident() != self.owner_thread_id:
            raise AssertionError("worker called Tk.after_cancel")
        self.cancelled.add(str(job_id))

    def destroy(self):
        assert threading.get_ident() == self.owner_thread_id
        self.destroyed = True

    def run_one(self) -> bool:
        while self.jobs:
            job_id, _delay, callback, args = self.jobs.popleft()
            if job_id in self.cancelled:
                continue
            callback(*args)
            return True
        return False

    def run_until(self, predicate, *, timeout=2.0) -> None:
        deadline = time.monotonic() + timeout
        while not predicate():
            ran = self.run_one()
            if not ran:
                threading.Event().wait(0.002)
            if time.monotonic() >= deadline:
                raise AssertionError("fake Tk pump did not reach the expected state")


def _symbols():
    module = importlib.import_module("tk_serial_ui_lane")
    return module, module.LaneTask, module.TkSerialUiLane


def _close(root, lane) -> None:
    if str(getattr(lane, "state", "")) == "CLOSED":
        return
    lane.close_idle()
    root.run_until(lambda: str(lane.state) == "CLOSED")


def test_blocked_work_does_not_block_tk_pump():
    module, LaneTask, TkSerialUiLane = _symbols()
    assert module.UI_LANE_SPEC == "kmtech-tk-ui-lane-v1"
    root = FakeTkRoot()
    lane = TkSerialUiLane(root, poll_ms=1)
    gate = threading.Event()
    started = threading.Event()
    heartbeats = []

    def work():
        started.set()
        assert gate.wait(timeout=2.0)
        return "done"

    started_at = time.perf_counter()
    admission = lane.submit(
        LaneTask("blocked", 1, work, lambda _value: None, lambda exc: pytest.fail(str(exc)))
    )
    elapsed = time.perf_counter() - started_at
    root.after(0, lambda: heartbeats.append(threading.get_ident()))
    root.run_until(lambda: bool(heartbeats))

    assert admission.accepted is True
    assert elapsed < 0.1
    assert started.wait(timeout=1.0)
    assert heartbeats == [root.owner_thread_id]
    assert lane.worker_thread_id != root.owner_thread_id
    gate.set()
    root.run_until(lambda: not lane.is_busy())
    _close(root, lane)


def test_exactly_one_worker_and_no_overlap():
    _module, LaneTask, TkSerialUiLane = _symbols()
    root = FakeTkRoot()
    lane = TkSerialUiLane(root, poll_ms=1)
    active = 0
    max_active = 0
    worker_ids = []

    def work():
        nonlocal active, max_active
        active += 1
        max_active = max(max_active, active)
        worker_ids.append(threading.get_ident())
        active -= 1
        return len(worker_ids)

    for index in range(2):
        task = LaneTask(
            f"op-{index}", index, work, lambda _value: None, pytest.fail
        )
        assert lane.submit(task).accepted
        root.run_until(lambda: not lane.is_busy())

    assert len(set(worker_ids)) == 1
    assert worker_ids[0] == lane.worker_thread_id
    assert max_active == 1
    _close(root, lane)


def test_worker_never_calls_tk_or_after():
    _module, LaneTask, TkSerialUiLane = _symbols()
    root = FakeTkRoot()
    lane = TkSerialUiLane(root, poll_ms=1)
    work_threads = []
    finish_threads = []
    assert lane.submit(
        LaneTask(
            "thread-affinity",
            1,
            lambda: work_threads.append(threading.get_ident()),
            lambda _value: finish_threads.append(threading.get_ident()),
            pytest.fail,
        )
    ).accepted
    root.run_until(lambda: not lane.is_busy())

    assert work_threads == [lane.worker_thread_id]
    assert finish_threads == [root.owner_thread_id]
    assert set(root.after_threads) == {root.owner_thread_id}
    _close(root, lane)


def test_success_finish_runs_once_on_tk():
    _module, LaneTask, TkSerialUiLane = _symbols()
    root = FakeTkRoot()
    lane = TkSerialUiLane(root, poll_ms=1)
    finished = []
    failed = []
    lane.submit(
        LaneTask(
            "success",
            1,
            lambda: 42,
            lambda value: finished.append((value, threading.get_ident())),
            failed.append,
        )
    )
    root.run_until(lambda: not lane.is_busy())
    for _index in range(3):
        root.run_one()

    assert finished == [(42, root.owner_thread_id)]
    assert failed == []
    _close(root, lane)


def test_exception_reaches_fail_once_on_tk():
    _module, LaneTask, TkSerialUiLane = _symbols()
    root = FakeTkRoot()
    lane = TkSerialUiLane(root, poll_ms=1)
    finished = []
    failed = []

    def work():
        raise ValueError("secret detail must not reach the operator")

    lane.submit(
        LaneTask(
            "failure",
            1,
            work,
            finished.append,
            lambda exc: failed.append((type(exc), threading.get_ident())),
        )
    )
    root.run_until(lambda: not lane.is_busy())
    for _index in range(3):
        root.run_one()

    assert finished == []
    assert failed == [(ValueError, root.owner_thread_id)]
    _close(root, lane)


def test_finish_failure_breaks_lane():
    _module, LaneTask, TkSerialUiLane = _symbols()
    root = FakeTkRoot()
    faults = []
    lane = TkSerialUiLane(
        root,
        poll_ms=1,
        on_runner_fault=lambda exc: faults.append(type(exc)),
    )

    def broken_finish(_value):
        raise RuntimeError("render failed")

    lane.submit(
        LaneTask("broken-finish", 1, lambda: "ok", broken_finish, pytest.fail)
    )
    root.run_until(lambda: str(lane.state) == "BROKEN")
    rejected = lane.submit(
        LaneTask("must-reject", 2, lambda: None, lambda _value: None, pytest.fail)
    )

    assert rejected.accepted is False
    assert rejected.reason == "broken"
    assert faults == [RuntimeError]
    lane.close_idle()
    root.run_until(lambda: str(lane.state) == "CLOSED")


def test_busy_reject_preserves_input():
    _module, LaneTask, TkSerialUiLane = _symbols()
    root = FakeTkRoot()
    lane = TkSerialUiLane(root, poll_ms=1)
    gate = threading.Event()
    raw_input = {"value": "PRODUCT-B"}
    first = lane.submit(
        LaneTask(
            "first",
            1,
            lambda: gate.wait(timeout=2.0),
            lambda _value: None,
            pytest.fail,
        )
    )
    second = lane.submit(
        LaneTask(
            "second",
            1,
            lambda: raw_input.clear(),
            lambda _value: None,
            pytest.fail,
        )
    )

    assert first.accepted is True
    assert second.accepted is False
    assert second.reason == "busy"
    assert raw_input == {"value": "PRODUCT-B"}
    gate.set()
    root.run_until(lambda: not lane.is_busy())
    _close(root, lane)


def test_fifo_ui_call_before_final_result():
    _module, LaneTask, TkSerialUiLane = _symbols()
    root = FakeTkRoot()
    lane = TkSerialUiLane(root, poll_ms=1)
    order = []

    def work():
        value = lane.call_ui_sync(
            lambda: order.append("checkpoint") or "prepared"
        )
        order.append(f"worker:{value}")
        return "final"

    lane.submit(
        LaneTask(
            "checkpoint",
            1,
            work,
            lambda value: order.append(value),
            pytest.fail,
        )
    )
    root.run_until(lambda: not lane.is_busy())

    assert order == ["checkpoint", "worker:prepared", "final"]
    _close(root, lane)


def test_on_idle_barrier_order():
    _module, LaneTask, TkSerialUiLane = _symbols()
    root = FakeTkRoot()
    lane = TkSerialUiLane(root, poll_ms=1)
    order = []
    nested_admissions = []

    def on_idle():
        order.append(("idle", lane.is_busy()))
        nested_admissions.append(
            lane.submit(
                LaneTask(
                    "too-early",
                    1,
                    lambda: None,
                    lambda _value: None,
                    pytest.fail,
                )
            )
        )

    lane.submit(
        LaneTask(
            "barrier",
            1,
            lambda: order.append("work") or "ok",
            lambda _value: order.append("finish"),
            pytest.fail,
            on_idle=on_idle,
        )
    )
    root.run_until(lambda: not lane.is_busy() and bool(nested_admissions))

    assert order == ["work", "finish", ("idle", False)]
    assert nested_admissions[0].accepted is False
    assert nested_admissions[0].reason == "busy"
    _close(root, lane)


def test_generation_fence_settles_but_skips_stale_render():
    _module, LaneTask, TkSerialUiLane = _symbols()
    root = FakeTkRoot()
    generation = {"value": 2}
    settled = []
    rendered = []
    lane = TkSerialUiLane(
        root,
        poll_ms=1,
        generation_provider=lambda: generation["value"],
    )

    lane.submit(
        LaneTask(
            "stale",
            1,
            lambda: "receipt",
            rendered.append,
            pytest.fail,
            settle=lambda value, error: settled.append((value, error)),
        )
    )
    root.run_until(lambda: not lane.is_busy())

    assert settled == [("receipt", None)]
    assert rendered == []
    _close(root, lane)


def test_timeout_is_typed_and_ui_stays_live():
    _module, LaneTask, TkSerialUiLane = _symbols()
    root = FakeTkRoot()
    lane = TkSerialUiLane(root, poll_ms=1)
    gate = threading.Event()
    heartbeat = []
    failures = []

    class TypedTimeout(TimeoutError):
        commit_state = "unknown"
        retryable = True

    def work():
        assert gate.wait(timeout=2.0)
        raise TypedTimeout("safe timeout")

    lane.submit(LaneTask("timeout", 1, work, pytest.fail, failures.append))
    root.after(0, lambda: heartbeat.append("alive"))
    root.run_until(lambda: heartbeat == ["alive"])
    gate.set()
    root.run_until(lambda: not lane.is_busy())

    assert len(failures) == 1
    assert isinstance(failures[0], TypedTimeout)
    assert failures[0].commit_state == "unknown"
    _close(root, lane)


def test_close_stops_admission_and_drains_before_destroy():
    _module, LaneTask, TkSerialUiLane = _symbols()
    root = FakeTkRoot()
    lane = TkSerialUiLane(root, poll_ms=1)
    gate = threading.Event()
    order = []

    def work():
        assert gate.wait(timeout=2.0)
        order.append("work")
        return "done"

    lane.submit(
        LaneTask(
            "mutation",
            1,
            work,
            lambda _value: order.append("finish"),
            pytest.fail,
        )
    )

    def after_drain():
        order.append("recovery-save")
        assert lane.worker_thread.is_alive() is False
        order.append("worker-close")
        root.destroy()
        order.append("destroy")

    lane.drain_then(after_drain)
    rejected = lane.submit(
        LaneTask("late", 1, lambda: None, lambda _value: None, pytest.fail)
    )
    assert rejected.accepted is False
    assert rejected.reason == "closing"
    assert root.destroyed is False
    gate.set()
    root.run_until(lambda: root.destroyed)

    assert order == ["work", "finish", "recovery-save", "worker-close", "destroy"]
    assert str(lane.state) == "CLOSED"


def test_call_ui_sync_round_trip_and_exception():
    _module, LaneTask, TkSerialUiLane = _symbols()
    root = FakeTkRoot()
    lane = TkSerialUiLane(root, poll_ms=1)
    round_trip = []
    failures = []

    def work():
        round_trip.append(
            lane.call_ui_sync(lambda left, right: left + right, 20, 22)
        )

        def fail_checkpoint():
            raise LookupError("checkpoint failed")

        lane.call_ui_sync(fail_checkpoint)

    lane.submit(LaneTask("ui-call", 1, work, pytest.fail, failures.append))
    root.run_until(lambda: not lane.is_busy())

    assert round_trip == [42]
    assert len(failures) == 1
    assert isinstance(failures[0], LookupError)
    _close(root, lane)


def test_periodic_trigger_coalesces_while_busy():
    module, LaneTask, TkSerialUiLane = _symbols()
    root = FakeTkRoot()
    lane = TkSerialUiLane(root, poll_ms=1)
    calls = []
    gates = [threading.Event(), threading.Event()]

    def task_factory():
        index = len(calls)

        def work():
            assert gates[index].wait(timeout=2.0)
            return "done"

        return LaneTask(
            "poll",
            1,
            work,
            lambda _value: calls.append("finish"),
            pytest.fail,
        )

    trigger = module.CoalescingTrigger(lane, task_factory)
    first = trigger.trigger()
    second = trigger.trigger()
    third = trigger.trigger()

    assert first.accepted is True
    assert second.accepted is False
    assert third.accepted is False
    assert trigger.pending_count == 1
    gates[0].set()
    root.run_until(lambda: calls == ["finish"] and lane.is_busy())
    gates[1].set()
    root.run_until(lambda: calls == ["finish", "finish"])
    assert trigger.pending_count == 0
    _close(root, lane)
