from __future__ import annotations

import threading

import tk_serial_ui_lane as lane_module
from tests.test_tk_serial_ui_lane import FakeTkRoot


def test_broken_lane_closes_with_admitted_task_still_waiting_for_worker():
    release_worker = threading.Event()
    work_calls = []
    finish_calls = []
    close_calls = []
    faults = []

    class HeldWorkerLane(lane_module.TkSerialUiLane):
        def _worker_main(self):
            # Control the admission-to-dequeue boundary without sleep-based races.
            self._worker_thread_id = threading.get_ident()
            self._worker_started.set()
            release_worker.wait()
            super()._worker_main()

    root = FakeTkRoot()
    lane = HeldWorkerLane(
        root, worker_join_timeout=0.1, on_runner_fault=faults.append
    )
    task = lane_module.LaneTask(
        "accepted-before-break",
        0,
        lambda: work_calls.append("accepted"),
        finish_calls.append,
        finish_calls.append,
    )
    try:
        assert lane.submit(task).accepted is True
        busy = lane.submit(task)
        assert busy.accepted is False and busy.reason == "busy"
        assert work_calls == []

        fault = RuntimeError("forced timeout before worker dequeue")
        assert lane.break_for_shutdown_timeout(fault) is True
        rejected = lane.submit(task)
        assert rejected.accepted is False and rejected.reason == "broken"

        # Stop must fit alongside the already accepted work, without discarding it.
        lane.drain_then(lambda: close_calls.append("closed"))
        assert lane.state is lane_module.LaneState.BROKEN
        assert close_calls == []
        assert faults == [fault]
        assert work_calls == []

        release_worker.set()
        lane.worker_thread.join(timeout=2.0)
        assert not lane.worker_thread.is_alive()
        lane.close_idle()
        lane.close_idle()
        assert lane.state is lane_module.LaneState.CLOSED
        assert work_calls == ["accepted"]
        assert finish_calls == []
        assert close_calls == ["closed"]
        assert faults == [fault]
    finally:
        # The unchanged implementation fails before signaling stop. Clean up its
        # non-daemon worker so this regression reports a failure instead of hanging.
        release_worker.set()
        lane.worker_thread.join(timeout=0.2)
        if lane.worker_thread.is_alive():
            lane._task_queue.put(lane_module._STOP, timeout=2.0)
            lane.worker_thread.join(timeout=2.0)
        assert not lane.worker_thread.is_alive()
