"""Repository-local Tk-safe serial worker lane.

The lane owns execution and Tk result delivery only.  Label durable intents,
outboxes, and current-set state remain application-owned domain state.
"""

from __future__ import annotations

from dataclasses import dataclass, replace
from enum import Enum
import itertools
import queue
import threading
import time
from typing import Any, Callable, Optional


UI_LANE_SPEC = "kmtech-tk-ui-lane-v1"

DRAIN_TO_TERMINAL = "DRAIN_TO_TERMINAL"
DRAIN_TO_DURABLE_HANDOFF = "DRAIN_TO_DURABLE_HANDOFF"
_SHUTDOWN_POLICIES = {
    DRAIN_TO_TERMINAL,
    DRAIN_TO_DURABLE_HANDOFF,
}


class LaneState(str, Enum):
    IDLE = "IDLE"
    BUSY = "BUSY"
    DRAINING = "DRAINING"
    BROKEN = "BROKEN"
    CLOSED = "CLOSED"

    def __str__(self) -> str:
        return self.value


@dataclass(frozen=True)
class Admission:
    accepted: bool
    op_id: Optional[int] = None
    reason: str = ""

    def __bool__(self) -> bool:
        return self.accepted


@dataclass(frozen=True)
class LaneTask:
    name: str
    generation: int
    work: Callable[[], Any]
    finish: Callable[[Any], None]
    fail: Callable[[BaseException], None]
    on_idle: Optional[Callable[[], None]] = None
    cancel_safe: bool = False
    shutdown_policy: str = DRAIN_TO_TERMINAL
    settle: Optional[
        Callable[[Any, Optional[BaseException]], None]
    ] = None

    def __post_init__(self) -> None:
        if not str(self.name or "").strip():
            raise ValueError("lane task name is required")
        if self.shutdown_policy not in _SHUTDOWN_POLICIES:
            raise ValueError("unsupported lane shutdown policy")
        for callback in (self.work, self.finish, self.fail):
            if not callable(callback):
                raise TypeError("lane task callbacks must be callable")


@dataclass(frozen=True)
class _TaskEnvelope:
    op_id: int
    task: LaneTask


@dataclass
class _UiCallEnvelope:
    sequence: int
    op_id: int
    generation: int
    callback: Callable[..., Any]
    args: tuple[Any, ...]
    kwargs: dict[str, Any]
    event: threading.Event
    value: Any = None
    error: Optional[BaseException] = None


@dataclass(frozen=True)
class _ResultEnvelope:
    sequence: int
    kind: str
    op_id: int
    generation: int
    task: LaneTask
    value: Any = None
    error: Optional[BaseException] = None
    elapsed: float = 0.0


_STOP = object()


class TkSerialUiLane:
    """Execute one blocking foreground command and apply its result on Tk."""

    def __init__(
        self,
        root: Any,
        *,
        poll_ms: int = 15,
        max_results_per_tick: int = 32,
        pump_budget_ms: float = 8.0,
        generation_provider: Optional[Callable[[], int]] = None,
        on_runner_fault: Optional[Callable[[BaseException], None]] = None,
        on_fault: Optional[Callable[[BaseException], None]] = None,
        worker_name: str = "label-match-tk-ui-lane",
        worker_join_timeout: float = 2.0,
    ) -> None:
        self._root = root
        self._owner_thread_id = threading.get_ident()
        self._poll_ms = max(1, int(poll_ms))
        self._max_results_per_tick = max(1, int(max_results_per_tick))
        self._pump_budget_seconds = max(
            0.001,
            float(pump_budget_ms) / 1000.0,
        )
        self._generation_provider = generation_provider
        self._on_fault = on_runner_fault or on_fault
        self._worker_join_timeout = max(0.1, float(worker_join_timeout))
        self._state_lock = threading.RLock()
        self._sequence_lock = threading.Lock()
        self._state = LaneState.IDLE
        self._active: Optional[_TaskEnvelope] = None
        self._idle_barrier = False
        self._fault_presented = False
        self._stop_sent = False
        self._task_queue: queue.Queue[Any] = queue.Queue(maxsize=1)
        self._result_queue: queue.Queue[Any] = queue.Queue()
        self._op_ids = itertools.count(1)
        self._sequences = itertools.count(1)
        self._drain_callbacks: list[Callable[[], None]] = []
        self._idle_waiters: list[Callable[[], None]] = []
        self._after_id: Any = None
        self._worker_thread_id: Optional[int] = None
        self._worker_started = threading.Event()
        self.worker_thread = threading.Thread(
            target=self._worker_main,
            name=str(worker_name or "label-match-tk-ui-lane"),
            daemon=False,
        )
        self.worker_thread.start()
        if not self._worker_started.wait(timeout=2.0):
            raise RuntimeError("Tk UI lane worker did not start")
        self._schedule_pump()

    @property
    def state(self) -> LaneState:
        with self._state_lock:
            return self._state

    @property
    def worker_thread_id(self) -> Optional[int]:
        return self._worker_thread_id

    @property
    def owner_thread_id(self) -> int:
        return self._owner_thread_id

    @property
    def current_task_name(self) -> str:
        with self._state_lock:
            return self._active.task.name if self._active is not None else ""

    def _assert_owner(self) -> None:
        if threading.get_ident() != self._owner_thread_id:
            raise RuntimeError(
                "TkSerialUiLane owner method called outside the Tk owner"
            )

    def _next_sequence(self) -> int:
        with self._sequence_lock:
            return next(self._sequences)

    def submit(self, task: LaneTask) -> Admission:
        self._assert_owner()
        if not isinstance(task, LaneTask):
            raise TypeError("submit requires LaneTask")
        with self._state_lock:
            if self._state is LaneState.BROKEN:
                return Admission(False, reason="broken")
            if self._state in {LaneState.DRAINING, LaneState.CLOSED}:
                return Admission(False, reason="closing")
            if self._active is not None or self._idle_barrier:
                return Admission(False, reason="busy")
            envelope = _TaskEnvelope(next(self._op_ids), task)
            self._active = envelope
            self._state = LaneState.BUSY
        try:
            self._task_queue.put_nowait(envelope)
        except queue.Full as exc:
            self._break_lane(exc)
            return Admission(False, reason="broken")
        return Admission(True, op_id=envelope.op_id)

    def is_busy(self) -> bool:
        with self._state_lock:
            return self._active is not None

    def call_ui_sync(
        self,
        callback: Callable[..., Any],
        *args: Any,
        **kwargs: Any,
    ) -> Any:
        if threading.get_ident() == self._owner_thread_id:
            return callback(*args, **kwargs)
        with self._state_lock:
            if self._state in {LaneState.BROKEN, LaneState.CLOSED}:
                raise RuntimeError(
                    f"Tk UI lane is {self._state.value.lower()}"
                )
            active = self._active
            if active is None:
                raise RuntimeError("Tk UI call has no active lane task")
            envelope = _UiCallEnvelope(
                sequence=self._next_sequence(),
                op_id=active.op_id,
                generation=active.task.generation,
                callback=callback,
                args=tuple(args),
                kwargs=dict(kwargs),
                event=threading.Event(),
            )
            self._result_queue.put_nowait(envelope)
        envelope.event.wait()
        if envelope.error is not None:
            raise envelope.error
        return envelope.value

    def stop_accepting(self) -> None:
        self._assert_owner()
        with self._state_lock:
            if self._state in {LaneState.BROKEN, LaneState.CLOSED}:
                return
            self._state = LaneState.DRAINING

    def drain_then(self, callback: Callable[[], None]) -> None:
        self._assert_owner()
        if not callable(callback):
            raise TypeError("drain callback must be callable")
        with self._state_lock:
            if self._state is LaneState.CLOSED:
                callback()
                return
            self._drain_callbacks.append(callback)
            if self._state is not LaneState.BROKEN:
                self._state = LaneState.DRAINING
            ready = self._active is None and not self._idle_barrier
        if ready:
            self._complete_close()

    def close_idle(self) -> None:
        self._assert_owner()
        with self._state_lock:
            if self._state is LaneState.CLOSED:
                return
            if self._active is not None or self._idle_barrier:
                raise RuntimeError("close_idle requires an idle lane")
            if self._state is not LaneState.BROKEN:
                self._state = LaneState.DRAINING
        self._complete_close()

    def defer_until_idle(self, callback: Callable[[], None]) -> None:
        self._assert_owner()
        if not callable(callback):
            raise TypeError("idle callback must be callable")
        with self._state_lock:
            if self._state in {
                LaneState.BROKEN,
                LaneState.CLOSED,
                LaneState.DRAINING,
            }:
                return
            if self._active is None and not self._idle_barrier:
                schedule_now = True
            else:
                self._idle_waiters.append(callback)
                schedule_now = False
        if schedule_now:
            self._root.after(0, callback)

    def _worker_main(self) -> None:
        self._worker_thread_id = threading.get_ident()
        self._worker_started.set()
        try:
            while True:
                queued = self._task_queue.get()
                try:
                    if queued is _STOP:
                        return
                    if not isinstance(queued, _TaskEnvelope):
                        raise RuntimeError("invalid Tk UI lane task envelope")
                    started = time.monotonic()
                    try:
                        value = queued.task.work()
                    except BaseException as exc:
                        result = _ResultEnvelope(
                            self._next_sequence(),
                            "failure",
                            queued.op_id,
                            queued.task.generation,
                            queued.task,
                            error=exc,
                            elapsed=time.monotonic() - started,
                        )
                    else:
                        result = _ResultEnvelope(
                            self._next_sequence(),
                            "success",
                            queued.op_id,
                            queued.task.generation,
                            queued.task,
                            value=value,
                            elapsed=time.monotonic() - started,
                        )
                    self._result_queue.put_nowait(result)
                finally:
                    self._task_queue.task_done()
        except BaseException as exc:
            self._result_queue.put_nowait(
                _ResultEnvelope(
                    self._next_sequence(),
                    "runner_fault",
                    0,
                    0,
                    LaneTask(
                        "runner.fault",
                        0,
                        lambda: None,
                        lambda _value: None,
                        lambda _error: None,
                    ),
                    error=exc,
                )
            )

    def _schedule_pump(self) -> None:
        self._assert_owner()
        with self._state_lock:
            if self._state is LaneState.CLOSED or self._after_id is not None:
                return
        try:
            self._after_id = self._root.after(self._poll_ms, self._pump)
        except BaseException as exc:
            self._break_lane(exc)

    def _pump(self) -> None:
        self._assert_owner()
        self._after_id = None
        deadline = time.monotonic() + self._pump_budget_seconds
        processed = 0
        while (
            processed < self._max_results_per_tick
            and time.monotonic() <= deadline
        ):
            try:
                envelope = self._result_queue.get_nowait()
            except queue.Empty:
                break
            try:
                if isinstance(envelope, _UiCallEnvelope):
                    self._apply_ui_call(envelope)
                elif isinstance(envelope, _ResultEnvelope):
                    self._apply_result(envelope)
                else:
                    self._break_lane(
                        RuntimeError("invalid Tk UI lane result envelope")
                    )
            finally:
                self._result_queue.task_done()
            processed += 1
            if self.state is LaneState.CLOSED:
                break
        if self.state is not LaneState.CLOSED:
            self._schedule_pump()

    def _apply_ui_call(self, envelope: _UiCallEnvelope) -> None:
        try:
            with self._state_lock:
                active = self._active
            if active is None or active.op_id != envelope.op_id:
                raise RuntimeError("stale Tk UI lane checkpoint")
            envelope.value = envelope.callback(
                *envelope.args,
                **envelope.kwargs,
            )
        except BaseException as exc:
            envelope.error = exc
        finally:
            envelope.event.set()

    def _generation_is_current(self, generation: int) -> bool:
        if self._generation_provider is None:
            return True
        return int(self._generation_provider()) == int(generation)

    def _apply_result(self, envelope: _ResultEnvelope) -> None:
        if envelope.kind == "runner_fault":
            self._break_lane(
                envelope.error or RuntimeError("Tk UI lane worker fault")
            )
            return
        with self._state_lock:
            active = self._active
        if (
            active is None
            or active.op_id != envelope.op_id
            or active.task is not envelope.task
        ):
            self._break_lane(
                RuntimeError("Tk UI lane result does not match active task")
            )
            return
        task = active.task
        try:
            if task.settle is not None:
                task.settle(envelope.value, envelope.error)
            if self._generation_is_current(envelope.generation):
                if envelope.kind == "success":
                    task.finish(envelope.value)
                else:
                    task.fail(
                        envelope.error
                        or RuntimeError(
                            "Tk UI lane task failed without an exception"
                        )
                    )
        except BaseException as exc:
            self._break_lane(exc)
            return

        with self._state_lock:
            draining = self._state is LaneState.DRAINING
            self._active = None
            self._state = (
                LaneState.DRAINING if draining else LaneState.IDLE
            )
            self._idle_barrier = True
        try:
            if task.on_idle is not None:
                task.on_idle()
        except BaseException as exc:
            self._break_lane(exc)
            return
        finally:
            with self._state_lock:
                self._idle_barrier = False
        if draining:
            self._complete_close()
            return
        with self._state_lock:
            waiters, self._idle_waiters = self._idle_waiters, []
        for callback in waiters:
            self._root.after(0, callback)

    def _present_fault(self, exc: BaseException) -> None:
        if self._fault_presented:
            return
        self._fault_presented = True
        if self._on_fault is not None:
            try:
                self._on_fault(exc)
            except BaseException:
                pass

    def _break_lane(self, exc: BaseException) -> None:
        with self._state_lock:
            self._state = LaneState.BROKEN
            self._active = None
            self._idle_barrier = False
            self._idle_waiters.clear()
        self._release_pending_ui_calls(
            RuntimeError("Tk UI lane is broken")
        )
        self._present_fault(exc)

    def _release_pending_ui_calls(self, error: BaseException) -> None:
        retained: list[Any] = []
        while True:
            try:
                envelope = self._result_queue.get_nowait()
            except queue.Empty:
                break
            if isinstance(envelope, _UiCallEnvelope):
                envelope.error = error
                envelope.event.set()
            else:
                retained.append(envelope)
        for envelope in retained:
            self._result_queue.put_nowait(envelope)

    def _signal_worker_stop(self) -> None:
        if self._stop_sent:
            return
        self._stop_sent = True
        self._task_queue.put_nowait(_STOP)

    def _complete_close(self) -> None:
        self._assert_owner()
        with self._state_lock:
            if self._state is LaneState.CLOSED:
                callbacks, self._drain_callbacks = (
                    self._drain_callbacks,
                    [],
                )
            else:
                if self._active is not None or self._idle_barrier:
                    return
                callbacks = []
                self._signal_worker_stop()
        if callbacks:
            for callback in callbacks:
                callback()
            return
        if threading.get_ident() != self.worker_thread.ident:
            self.worker_thread.join(self._worker_join_timeout)
        if self.worker_thread.is_alive():
            self._break_lane(
                RuntimeError("Tk UI lane worker did not stop")
            )
            return
        with self._state_lock:
            self._state = LaneState.CLOSED
            callbacks, self._drain_callbacks = (
                self._drain_callbacks,
                [],
            )
            after_id = self._after_id
            self._after_id = None
        if after_id is not None:
            try:
                self._root.after_cancel(after_id)
            except Exception:
                pass
        self._release_pending_ui_calls(RuntimeError("Tk UI lane closed"))
        for callback in callbacks:
            callback()


class CoalescingTrigger:
    """Keep at most one active and one collapsed periodic trigger."""

    def __init__(
        self,
        lane: TkSerialUiLane,
        task_factory: Optional[Callable[[], LaneTask]] = None,
        on_admitted: Optional[Callable[[Admission], None]] = None,
    ) -> None:
        self._lane = lane
        self._task_factory = task_factory
        self._on_admitted = on_admitted
        self._running = False
        self._pending = False
        self._idle_wait_registered = False

    @property
    def pending_count(self) -> int:
        return int(self._pending)

    def trigger(
        self,
        task_factory: Optional[Callable[[], LaneTask]] = None,
    ) -> Admission:
        self._lane._assert_owner()
        if task_factory is not None:
            self._task_factory = task_factory
        if self._task_factory is None:
            raise TypeError("coalescing trigger requires a task factory")
        if self._running:
            self._pending = True
            return Admission(False, reason="busy")
        admission = self._start()
        if not admission.accepted and admission.reason == "busy":
            self._pending = True
            self._register_idle_wait()
        return admission

    def _register_idle_wait(self) -> None:
        if self._idle_wait_registered:
            return
        self._idle_wait_registered = True
        self._lane.defer_until_idle(self._resume_when_idle)

    def _resume_when_idle(self) -> None:
        self._idle_wait_registered = False
        if self._running or not self._pending:
            return
        self._pending = False
        admission = self._start()
        if not admission.accepted and admission.reason == "busy":
            self._pending = True
            self._register_idle_wait()

    def _start(self) -> Admission:
        assert self._task_factory is not None
        task = self._task_factory()
        original_idle = task.on_idle

        def on_idle() -> None:
            if original_idle is not None:
                original_idle()
            self._running = False
            if self._pending:
                self._lane._root.after(0, self._resume_when_idle)

        admission = self._lane.submit(replace(task, on_idle=on_idle))
        self._running = admission.accepted
        if admission.accepted and self._on_admitted is not None:
            self._on_admitted(admission)
        return admission


__all__ = [
    "Admission",
    "CoalescingTrigger",
    "DRAIN_TO_DURABLE_HANDOFF",
    "DRAIN_TO_TERMINAL",
    "LaneState",
    "LaneTask",
    "TkSerialUiLane",
    "UI_LANE_SPEC",
]
