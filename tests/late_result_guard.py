"""App-independent stale-response scenarios. Store no HTTP data or credentials.

Factories receive HeldTransport; adapters expose the contract documented below.
Only normalized enums, attempt numbers and counts appear in failure messages.
"""
from dataclasses import dataclass, field
from enum import Enum
import threading
from typing import Callable, Hashable, Protocol


class ResultKind(str, Enum):
    SUCCESS = "success"
    NONRETRY_FAILURE = "nonretry_failure"
    RETRY_FAILURE = "retry_failure"
    TIMEOUT = "timeout"
    DISCONNECT = "disconnect"


class LocalState(str, Enum):
    COMPLETE = "complete"
    PENDING = "pending"
    SENDING = "sending"
    NEEDS_REVIEW = "needs_review"


class AuditKind(str, Enum):
    FAILURE = "failure"
    REVIEW = "review"
    CORRECTION = "correction"


@dataclass(frozen=True)
class Snapshot:
    state: LocalState
    attempt: int
    reason: ResultKind | None = None


@dataclass(frozen=True)
class AuditEvent:
    attempt: int
    kind: AuditKind
    result: ResultKind | None = None


@dataclass(frozen=True)
class DrainResult:
    work: int
    blocked: bool = False


@dataclass(frozen=True)
class Attempt:
    command: Hashable = field(repr=False)
    number: int
    request: int


class HeldTransport:
    """Capture each actual POST, then release a controlled result independently.

    A fake POST calls hold(command, durable_attempt_number), then wait(attempt).
    Adapter.start_attempt returns that captured Attempt. Adapter.deliver calls
    release(attempt, result), then waits for the real local handler to finish.
    Synchronous self-test adapters may apply the released result directly.
    Central success is deduplicated by immutable command, regardless of attempt.
    Receipt reads must use central_effects(), never hold() (which counts POSTs).
    """
    def __init__(self):
        self._lock = threading.Lock()
        self._held = {}
        self._committed = set()

    def hold(self, command: Hashable, number: int) -> Attempt:
        if type(number) is not int or number < 1:
            raise ValueError("durable attempt number must be a positive integer")
        with self._lock:
            attempt = Attempt(command, number, len(self._held) + 1)
            self._held[attempt] = [threading.Event(), None]
            return attempt

    def release(self, attempt: Attempt, result: ResultKind) -> ResultKind:
        if not isinstance(result, ResultKind):
            raise TypeError("result must be a normalized ResultKind")
        with self._lock:
            record = self._held[attempt]
            if record[1] is not None:
                raise ValueError("held attempt already released")
            if result == ResultKind.SUCCESS:
                self._committed.add(attempt.command)
            record[1] = result
            record[0].set()
        return result

    def wait(self, attempt: Attempt, timeout: float = 5.0) -> ResultKind:
        with self._lock:
            ready = self._held[attempt][0]
        if not ready.wait(timeout):
            raise TimeoutError("held response was not released within adapter bound")
        return self.outcome(attempt)

    def outcome(self, attempt: Attempt) -> ResultKind | None:
        with self._lock:
            return self._held[attempt][1]

    def request_count(self, command: Hashable) -> int:
        with self._lock:
            return sum(attempt.command == command for attempt in self._held)

    def central_effects(self, command: Hashable) -> int:
        with self._lock:
            return int(command in self._committed)


class Adapter(Protocol):
    """Use real durable/claim/result/drain paths; bound all waits in the adapter.

    expire_attempt satisfies the real lease-expiry condition without bypassing
    admission/result guards or reducing the clock. audit returns an immutable
    normalized summary with no free text. Counters count effects and POSTs per
    command. Declare late_success_policy='reconcile' only to opt into D1.
    """
    late_success_policy: str | None

    def create_command(self, label: str) -> Hashable: ...
    def start_attempt(self, command: Hashable) -> Attempt: ...
    def deliver(self, attempt: Attempt, result: ResultKind) -> None: ...
    def expire_attempt(self, attempt: Attempt) -> None: ...
    def background_once(self) -> DrainResult: ...
    def snapshot(self, command: Hashable) -> Snapshot: ...
    def central_effects(self, command: Hashable) -> int: ...
    def request_count(self, command: Hashable) -> int: ...
    def audit(self, command: Hashable) -> tuple[AuditEvent, ...]: ...
    def reconcile(self, command: Hashable) -> None: ...


REQUIRED_SCENARIOS = ("A1", "A2", "A3", "A4", "B1", "C1")
SCENARIOS = REQUIRED_SCENARIOS + ("D1",)
_OLD_FAILURES = dict(zip(REQUIRED_SCENARIOS[:4], (
    ResultKind.NONRETRY_FAILURE, ResultKind.RETRY_FAILURE,
    ResultKind.TIMEOUT, ResultKind.DISCONNECT,
)))


class LateResultViolation(AssertionError):
    pass


def _summary(state):
    if state is None:
        return "unobserved"
    if (not isinstance(state, Snapshot) or not isinstance(state.state, LocalState)
            or type(state.attempt) is not int
            or (state.reason is not None and not isinstance(state.reason, ResultKind))):
        return "invalid-normalized-state"
    reason = state.reason.value if state.reason is not None else "none"
    return f"{state.state.value}/attempt={state.attempt}/reason={reason}"


def _check(ok, scenario, result, detail, before=None, after=None):
    __tracebackhide__ = True
    if not ok:
        raise LateResultViolation(
            f"{scenario} result={result.value}: {detail}; "
            f"{_summary(before)} -> {_summary(after)}")


def _counts(adapter, transport, command, scenario, result, effects, posts):
    state = adapter.snapshot(command)
    _check(adapter.central_effects(command) == transport.central_effects(command) == effects,
           scenario, result, "central effect count changed", state, state)
    _check(adapter.request_count(command) == transport.request_count(command) == posts,
           scenario, result, "POST count changed", state, state)


def _deliver(adapter, transport, attempt, result, scenario):
    adapter.deliver(attempt, result)
    _check(transport.outcome(attempt) == result, scenario, result,
           "adapter did not release the captured response")


def _new_attempt(adapter, transport, command, scenario, previous=None):
    before = adapter.snapshot(command)
    attempt = adapter.start_attempt(command)
    after = adapter.snapshot(command)
    _check(isinstance(attempt, Attempt) and attempt.command == command
           and after == Snapshot(LocalState.SENDING, attempt.number)
           and transport.outcome(attempt) is None,
           scenario, ResultKind.SUCCESS, "attempt not captured in sending state", before, after)
    if previous is not None:
        _check(attempt.number > previous.number, scenario, ResultKind.SUCCESS,
               "durable attempt number did not advance", before, after)
    return attempt


def _unchanged(adapter, command, before, audit, scenario, result):
    after = adapter.snapshot(command)
    _check(after == before, scenario, result, "local state overwritten", before, after)
    _check(adapter.audit(command) == audit, scenario, result,
           "audit/review records changed", before, after)


def _idle(adapter, transport, command, before, audit, scenario, result, posts):
    drained = adapter.background_once()
    _check(drained == DrainResult(0, False), scenario, result,
           "background drain blocked or performed work", before, adapter.snapshot(command))
    _unchanged(adapter, command, before, audit, scenario, result)
    _counts(adapter, transport, command, scenario, result, 1, posts)


def _late_failure(adapter, transport, scenario, result):
    # Check both newer-SENDING and newer-COMPLETE; the audit found both races.
    for finish_new_first in (False, True):
        command = adapter.create_command("X-inflight" if not finish_new_first else "X-complete")
        old = _new_attempt(adapter, transport, command, scenario)
        adapter.expire_attempt(old)
        new = _new_attempt(adapter, transport, command, scenario, old)
        if finish_new_first:
            _deliver(adapter, transport, new, ResultKind.SUCCESS, scenario)
        before, audit = adapter.snapshot(command), adapter.audit(command)
        if finish_new_first:
            _check(before == Snapshot(LocalState.COMPLETE, new.number), scenario, result,
                   "new success was not applied", before, before)
        _deliver(adapter, transport, old, result, scenario)
        _unchanged(adapter, command, before, audit, scenario, result)
        _counts(adapter, transport, command, scenario, result, int(finish_new_first), 2)
        if not finish_new_first:
            _deliver(adapter, transport, new, ResultKind.SUCCESS, scenario)
        complete = adapter.snapshot(command)
        _check(complete == Snapshot(LocalState.COMPLETE, new.number), scenario, result,
               "new success was lost", before, complete)
        _idle(adapter, transport, command, complete, adapter.audit(command), scenario, result, 2)


def _both_fail(adapter, transport):
    scenario, result = "B1", ResultKind.NONRETRY_FAILURE
    command = adapter.create_command("X")
    old = _new_attempt(adapter, transport, command, scenario)
    adapter.expire_attempt(old)
    new = _new_attempt(adapter, transport, command, scenario, old)
    _deliver(adapter, transport, new, ResultKind.RETRY_FAILURE, scenario)
    before, audit = adapter.snapshot(command), adapter.audit(command)
    _check(before.attempt == new.number and before.reason == ResultKind.RETRY_FAILURE
           and before.state in (LocalState.PENDING, LocalState.NEEDS_REVIEW),
           scenario, result, "new failure record missing", before, before)
    _deliver(adapter, transport, old, result, scenario)
    _unchanged(adapter, command, before, audit, scenario, result)
    _counts(adapter, transport, command, scenario, result, 0, 2)


def _other_command(adapter, transport):
    scenario, result = "C1", ResultKind.NONRETRY_FAILURE
    x, y = adapter.create_command("X"), adapter.create_command("Y")
    _check(x != y, scenario, result, "adapter reused command identity")
    old = _new_attempt(adapter, transport, x, scenario)
    adapter.expire_attempt(old)
    new = _new_attempt(adapter, transport, x, scenario, old)
    _deliver(adapter, transport, new, ResultKind.SUCCESS, scenario)
    y_attempt = _new_attempt(adapter, transport, y, scenario)
    before_x, before_y = adapter.snapshot(x), adapter.snapshot(y)
    audit_x, audit_y = adapter.audit(x), adapter.audit(y)
    _check(before_x == Snapshot(LocalState.COMPLETE, new.number), scenario, result,
           "X success was not applied", before_x, before_x)
    _deliver(adapter, transport, old, result, scenario)
    _unchanged(adapter, y, before_y, audit_y, scenario, result)
    _unchanged(adapter, x, before_x, audit_x, scenario, result)
    _counts(adapter, transport, x, scenario, result, 1, 2)
    _counts(adapter, transport, y, scenario, result, 0, 1)
    _deliver(adapter, transport, y_attempt, ResultKind.SUCCESS, scenario)
    complete_y = adapter.snapshot(y)
    _check(complete_y == Snapshot(LocalState.COMPLETE, y_attempt.number), scenario, result,
           "Y success was lost", before_y, complete_y)
    _idle(adapter, transport, y, complete_y, adapter.audit(y), scenario, result, 1)
    _unchanged(adapter, x, before_x, audit_x, scenario, result)
    _counts(adapter, transport, x, scenario, result, 1, 2)


def _late_success(adapter, transport):
    scenario, result = "D1", ResultKind.SUCCESS
    command = adapter.create_command("X")
    old = _new_attempt(adapter, transport, command, scenario)
    adapter.expire_attempt(old)
    new = _new_attempt(adapter, transport, command, scenario, old)
    _deliver(adapter, transport, new, ResultKind.NONRETRY_FAILURE, scenario)
    before, audit = adapter.snapshot(command), adapter.audit(command)
    _check(before.attempt == new.number and before.reason == ResultKind.NONRETRY_FAILURE
           and before.state in (LocalState.PENDING, LocalState.NEEDS_REVIEW)
           and any(event.attempt == new.number and event.kind in (AuditKind.FAILURE, AuditKind.REVIEW)
                   and event.result == ResultKind.NONRETRY_FAILURE for event in audit),
           scenario, result, "new failure/audit record missing", before, before)
    _counts(adapter, transport, command, scenario, result, 0, 2)
    _deliver(adapter, transport, old, result, scenario)
    _check(adapter.snapshot(command) == before, scenario, result,
           "late success applied before reconciliation", before, adapter.snapshot(command))
    _counts(adapter, transport, command, scenario, result, 1, 2)
    after_late_audit = adapter.audit(command)
    _check(after_late_audit[:len(audit)] == audit, scenario, result,
           "failure audit erased before reconciliation", before, adapter.snapshot(command))
    adapter.reconcile(command)
    after, corrected = adapter.snapshot(command), adapter.audit(command)
    _check(after == Snapshot(LocalState.COMPLETE, new.number), scenario, result,
           "reconciliation differs from central effect", before, after)
    _check(corrected[:len(after_late_audit)] == after_late_audit
           and any(event.kind == AuditKind.CORRECTION for event in corrected[len(after_late_audit):]),
           scenario, result, "failure audit erased or correction missing", before, after)
    _counts(adapter, transport, command, scenario, result, 1, 2)
    _idle(adapter, transport, command, after, corrected, scenario, result, 2)


def run_late_result_scenarios(factory: Callable[[HeldTransport], Adapter], scenario=None):
    """Run all mandatory cases plus declared D1, or one parametrized scenario.

    A fresh isolated adapter is made per scenario. D1 alone skips when undeclared;
    the combined call simply omits undeclared D1. Return the IDs actually checked.
    """
    if scenario is not None and scenario not in SCENARIOS:
        raise ValueError("unknown late-result scenario")
    completed = []
    for name in (SCENARIOS if scenario is None else (scenario,)):
        transport = HeldTransport()
        adapter = factory(transport)
        if name == "D1":
            policy = getattr(adapter, "late_success_policy", None)
            if policy is None:
                if scenario is not None:
                    import pytest
                    pytest.skip("D1 requires declared late_success_policy='reconcile'")
                continue
            if policy != "reconcile":
                raise ValueError("unsupported normalized late-success policy")
        if name in _OLD_FAILURES:
            _late_failure(adapter, transport, name, _OLD_FAILURES[name])
        elif name == "B1":
            _both_fail(adapter, transport)
        elif name == "C1":
            _other_command(adapter, transport)
        else:
            _late_success(adapter, transport)
        completed.append(name)
    return tuple(completed)
