"""Decisions for startup updates; code and business data have separate lifetimes.

The decision functions are pure. The protected bootstrap persists records before
switching a code pointer; business data is never copied or restored here.
"""
from __future__ import annotations

from datetime import datetime, timezone
import hashlib
import ast
import json
import math
import os
from pathlib import Path
import re
import sys
import time
import uuid
from contextlib import contextmanager
from typing import Any, Mapping

if __name__ == "__main__":
    sys.path.insert(0, str(Path(__file__).resolve().parent))
    sys.path.insert(1, str(Path(__file__).resolve().parent / "site-packages"))

from startup_timing import startup_timed, startup_timing_step, StartupStopRequested
from writer_session_fence import WriterFencedError, writer_sink, _assert_no_reparse_components, WriterFenceError


SCHEMA = "label-match-startup-update-v1"
READY_CHECKS = (
    "gui_initialized", "identity_readable", "ledger_readable",
    "unfinished_readable", "outbox_readable", "ordinary_user_relay",
)
RELAY_READY_CHECKS = ("relay_initialized", *READY_CHECKS[1:])


APPLY_PROGRESS_TIMEOUT = 60
STARTUP_WAIT_SECONDS = 240
READY_WINDOW_SECONDS = 600
# The bootstrap decision wait must outlast one whole READY window plus the
# normal-stop and settle tail it always had (the former 480 = 240 + 240).
BOOTSTRAP_OBSERVATION_CAP_SECONDS = READY_WINDOW_SECONDS + 240
# A transient failure of one candidate sha gets another chance; the failed
# list (permanent) is for confirmed rejections and for an exhausted budget.
RETRY_ATTEMPT_LIMIT = 3
RETRY_WINDOW_SECONDS = 24 * 60 * 60
# Maximum shared by the parent, independent bootstrap and installer relay waits.
# A parented bootstrap reserves result time inside its inherited absolute deadline.
RELAY_PREPARE_SECONDS = 600
RELAY_PARENT_RESULT_RESERVE_SECONDS = 10
UPDATE_RELAY_STOP_REQUESTED = "UPDATE_RELAY_STOP_REQUESTED"
STARTUP_PROGRESS_MESSAGE = "업데이트가 진행 중입니다. 잠시 후 다시 실행하세요."
UPDATE_MUTEX_RECOVERY_MESSAGE = (
    "설치 복구 필요: 업데이트 상태와 설치 파일을 확인하지 못해 시작을 멈췄습니다. "
    "자료와 복구 기록을 보존하고 관리자에게 지원 복구를 요청하세요."
)


from label_atomic_replace import atomic_replace

class UpdateMutexRecoveryRequired(RuntimeError):
    """An abandoned owner never grants permission to use unverified code."""

    def __init__(self, policy):
        super().__init__("UPDATE_MUTEX_ABANDONED_RECOVERY_REQUIRED")
        self.policy = policy


class _RelayPreparationUnavailable(RuntimeError):
    def __init__(self, code: str):
        super().__init__(code)
        self.code = code


class _RelayPreparationDeadline(float):
    """One deadline and parent lifetime, shared without process-global state."""

    def __new__(cls, value, *, parent_pid=None, parent_identity=None):
        instance = super().__new__(cls, value)
        instance.parent_pid = parent_pid
        instance.parent_identity = parent_identity
        instance._failure_code = ""
        return instance

    def bounded_by(self, deadline: float):
        """Shorten a trial window without dropping the captured parent identity."""
        return type(self)(min(self, deadline), parent_pid=self.parent_pid,
                          parent_identity=self.parent_identity)

    def failure_code(self) -> str:
        if self._failure_code:
            return self._failure_code
        if time.monotonic() >= self:
            self._failure_code = "UPDATE_APPLY_NO_PROGRESS"
        elif self.parent_pid is not None:
            try:
                parent = native_process_identity(self.parent_pid)
                if parent.get("status") == "ABSENT":
                    self._failure_code = "UPDATE_APPLY_NO_PROGRESS"
                elif parent.get("status") != "LIVE" or not parent.get("creation_identity"):
                    self._failure_code = "UPDATE_SUPERVISION_UNAVAILABLE"
                elif parent["creation_identity"] != self.parent_identity:
                    self._failure_code = "UPDATE_APPLY_NO_PROGRESS"
            except (OSError, ValueError, TypeError, KeyError):
                self._failure_code = "UPDATE_SUPERVISION_UNAVAILABLE"
        return self._failure_code


def _relay_prepare_failure(deadline: float) -> str:
    if isinstance(deadline, _RelayPreparationDeadline):
        return deadline.failure_code()
    return "UPDATE_APPLY_NO_PROGRESS" if time.monotonic() >= deadline else ""


def _relay_prepare_result(deadline: float) -> int:
    failure = _relay_prepare_failure(deadline)
    if failure:
        emit_support_record({"reason_code": failure}, stage="update_apply")
        return 20
    return 0


def _relay_preparation_deadline(parent_deadline, parent_pid) -> _RelayPreparationDeadline:
    entry = time.monotonic()
    independent = entry + RELAY_PREPARE_SECONDS
    if parent_deadline is None and parent_pid is None:
        return _RelayPreparationDeadline(independent)
    try:
        if (parent_deadline is None or parent_pid is None
                or isinstance(parent_deadline, bool) or isinstance(parent_pid, bool)
                or not re.fullmatch(r"[0-9]+", str(parent_pid))):
            raise ValueError
        received = float(parent_deadline)
        pid = int(parent_pid)
        if not math.isfinite(received) or pid <= 0:
            raise ValueError
    except (ValueError, TypeError, OverflowError):
        raise _RelayPreparationUnavailable("UPDATE_PREPARATION_FAILED") from None
    value = min(independent, received - RELAY_PARENT_RESULT_RESERVE_SECONDS)
    if value <= entry:
        return _RelayPreparationDeadline(value)
    if pid != os.getppid():
        raise _RelayPreparationUnavailable("UPDATE_SUPERVISION_UNAVAILABLE")
    try:
        parent = native_process_identity(pid)
        if parent.get("status") == "ABSENT":
            raise _RelayPreparationUnavailable("UPDATE_APPLY_NO_PROGRESS")
        own = native_process_identity(os.getpid())
        if parent.get("status") != "LIVE" or own.get("status") != "LIVE":
            raise _RelayPreparationUnavailable("UPDATE_SUPERVISION_UNAVAILABLE")
        parent_birth = int(parent["creation_identity"])
        own_birth = int(own["creation_identity"])
        if parent_birth > own_birth:
            raise _RelayPreparationUnavailable("UPDATE_APPLY_NO_PROGRESS")
    except (OSError, ValueError, TypeError, KeyError):
        raise _RelayPreparationUnavailable("UPDATE_SUPERVISION_UNAVAILABLE") from None
    return _RelayPreparationDeadline(value, parent_pid=pid,
                                     parent_identity=parent["creation_identity"])


def support_record(fields=None, *, stage):
    from failure_redaction import public_record
    return public_record({**dict(fields or {}), "stage": stage})


def emit_support_record(fields, *, stage, policy=None):
    report = support_record(fields, stage=stage)
    if policy is not None and policy.get("staging_root"):
        try:
            write_json_durable(Path(policy["staging_root"]) / "startup-support.json", report)
        except (OSError, ValueError, TypeError, WriterFenceError):
            emit_support_record({"reason_code": "REPORT_UNAVAILABLE"}, stage=stage)
    print(json.dumps(report, ensure_ascii=False, sort_keys=True))


def _sha(value: Any, *, length: int = 40) -> str:
    if not isinstance(value, str) or re.fullmatch(r"[0-9a-f]{%d}" % length, value) is None:
        raise ValueError("update identity is malformed")
    return value


def _utc(value: Any) -> datetime:
    if not isinstance(value, str):
        raise ValueError("update time is absent")
    result = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if result.tzinfo is None or result.utcoffset() is None:
        raise ValueError("update time must include an offset")
    try:
        return result.astimezone(timezone.utc)
    except OverflowError as error:
        raise ValueError("update time is outside the representable UTC range") from error


def startup_action(
    transition: Mapping[str, Any] | None, *, prepared_present: bool,
) -> str:
    """Inspect both the candidate and the record, including before the task runs."""
    if transition is None:
        return "REQUEST_APPLY" if prepared_present else "START_CURRENT"
    if transition.get("schema") != SCHEMA:
        raise ValueError("startup update record is unsupported")
    phase = transition.get("phase")
    if phase == "DEFERRED" and transition.get("recovery_pending") is True:
        return "WAIT_FOR_RECOVERY"
    if phase == "PREPARED":
        return "REQUEST_APPLY"
    if phase in {"APPLIED_PENDING_READY", "ROLLBACK_PENDING_READY"}:
        return "WAIT_FOR_TRIAL" if transition.get("trial_pid") else "REQUEST_TRIAL"
    if phase in {"ROLLBACK_PENDING", "UNKNOWN", "FAILED"}:
        return "WAIT_FOR_RECOVERY"
    if phase not in {"CONFIRMED", "ROLLED_BACK", "DEFERRED"}:
        raise ValueError("startup update phase is unsupported")
    return "REQUEST_APPLY" if prepared_present else "START_CURRENT"


def prepare_transition(
    *, previous_sha: str, candidate_sha: str, packet_sha256: str,
    forward_pin: str, rollback_pin: str, transition_id: str, nonce: str,
    now: str, failed_shas: list[str] | None = None,
    retry_shas: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    previous_sha = _sha(previous_sha)
    candidate_sha = _sha(candidate_sha)
    if previous_sha == candidate_sha:
        raise ValueError("update does not change the version")
    failures = list(failed_shas or [])
    for failed in failures:
        _sha(failed)
    if candidate_sha in failures:
        raise ValueError("a failed version must not be reapplied")
    _utc(now)
    prepared = {
        "schema": SCHEMA, "phase": "PREPARED",
        "previous_sha": previous_sha, "candidate_sha": candidate_sha,
        "packet_sha256": _sha(packet_sha256, length=64),
        "forward_pin": _sha(forward_pin, length=64),
        "rollback_pin": _sha(rollback_pin, length=64),
        "transition_id": _sha(transition_id, length=32),
        "nonce": _sha(nonce, length=32), "started_at": now,
        "failed_shas": failures, "trial_pid": None,
    }
    if isinstance(retry_shas, Mapping) and retry_shas:
        prepared["retry_shas"] = dict(retry_shas)
    return prepared


def apply_transition(
    prepared: Mapping[str, Any], *, writers_absent: bool,
    packet_verified: bool, durable_prepared: bool, now: str,
) -> dict[str, Any]:
    """A false/unknown observation authorizes no pointer change."""
    if prepared.get("schema") != SCHEMA or prepared.get("phase") != "PREPARED":
        raise ValueError("update was not prepared")
    if writers_absent is not True or packet_verified is not True or durable_prepared is not True:
        raise ValueError("update requires writer absence, verification and durable preparation")
    _utc(now)
    return {**prepared, "phase": "APPLIED_PENDING_READY", "applied_at": now}


def bind_trial(
    transition: Mapping[str, Any], *, sha: str, pid: int,
    ordinary_user: bool, now: str, role: str = "gui",
) -> dict[str, Any]:
    phase = transition.get("phase")
    expected_sha = transition.get("previous_sha" if phase == "ROLLBACK_PENDING_READY" else "candidate_sha")
    if phase not in {"APPLIED_PENDING_READY", "ROLLBACK_PENDING_READY"} or sha != expected_sha:
        raise ValueError("trial version differs from the pending version")
    if transition.get("trial_pid") is not None:
        raise ValueError("the trial has already been started")
    if type(pid) is not int or pid < 1 or ordinary_user is not True or role not in {"gui", "relay"}:
        raise ValueError("trial must be a live ordinary-user process")
    _utc(now)
    return {**transition, "trial_pid": pid, "trial_sha": sha, "trial_started_at": now, "trial_role": role}


def _retained_confirmation_state(state) -> bool:
    """The late path adds no behavior to other terminal or failed states."""
    return bool(state and state.get("schema") == SCHEMA
        and state.get("phase") == "DEFERRED" and state.get("retained_candidate") is True
        and re.fullmatch(r"[0-9a-f]{40}", str(state.get("candidate_sha", "")))
        and re.fullmatch(r"[0-9a-f]{32}", str(state.get("transition_id", "")))
        and re.fullmatch(r"[0-9a-f]{32}", str(state.get("nonce", "")))
        and state.get("failure_decision", {}).get("code") in {"TRIAL_GENERATION_ADVANCED", "TRIAL_GENERATION_UNKNOWN"}
        and state.get("failure_decision", {}).get("ready_result") == "FAILED"
        and state.get("failure_decision", {}).get("selected_sha") == state["candidate_sha"]
        and state["candidate_sha"] not in (state.get("failed_shas") or [])
        and not state.get("rollback_at") and not state.get("confirmed_at")
        and not state.get("confirmation_audit_applied")
        and state.get("trial_sha", state["candidate_sha"]) == state["candidate_sha"])


def _retained_confirmation_allowed(state, active) -> bool:
    """Only the same unconfirmed, nonfailed retained candidate may settle late."""
    return bool(_retained_confirmation_state(state) and active
        and active.get("sha") == state["candidate_sha"]
        and active.get("transition_id") == state["transition_id"])


def _retained_native_created_at(identity):
    ticks = int(identity["creation_identity"])
    seconds, remainder = divmod(ticks, 10000000)
    return datetime.fromtimestamp(seconds - 11644473600, timezone.utc).replace(microsecond=remainder // 10)


def _retained_trial_binding(state, active, claim, rows, policy):
    """Project an existing claim onto its current native lifetime; no journal write."""
    if not _retained_confirmation_allowed(state, active) or not claim:
        return None
    role, pid = claim.get("role", "gui"), claim.get("pid")
    if (role not in {"gui", "relay"} or type(pid) is not int or pid < 1
            or role != state.get("trial_role", role)
            or claim.get("nonce") != state["nonce"]
            or claim.get("transition_id") != state["transition_id"]):
        return None
    matches = [row for row in rows if row.get("pid") == pid]
    if len(matches) != 1:
        return None
    row = matches[0]
    projected = {**state, "trial_pid": pid, "trial_sha": state["candidate_sha"],
                 "trial_role": role, "trial_started_at": row.get("created_at")}
    identity = native_process_identity(pid)
    try:
        if (identity.get("status") != "LIVE" or not identity.get("creation_identity")
                or _utc(row.get("created_at")) != _retained_native_created_at(identity)
                or _utc(row.get("created_at")) > _utc(claim.get("claimed_at"))
                or not process_matches_trial(row, state=projected, sha=active["sha"],
                    sid=policy["operator_sid"], role=role)):
            return None
    except (KeyError, TypeError, ValueError, OverflowError, OSError):
        return None
    return projected, identity


def ready_matches(
    transition: Mapping[str, Any], ready: Mapping[str, Any], *,
    process_matches: bool, relay_matches: bool, now: str, active=None,
) -> bool:
    """PID/heartbeat/exit zero alone do not establish application readiness."""
    phase = transition.get("phase")
    if (phase not in {"APPLIED_PENDING_READY", "ROLLBACK_PENDING_READY"}
            and (phase != "DEFERRED" or transition.get("retained_candidate") is not True
                 or not _retained_confirmation_allowed(transition, active))):
        return False
    sha = transition.get("previous_sha" if phase == "ROLLBACK_PENDING_READY" else "candidate_sha")
    role = transition.get("trial_role", "gui")
    checks = RELAY_READY_CHECKS if role == "relay" else READY_CHECKS
    if (
        role not in {"gui", "relay"} or ready.get("role", "gui") != role
        or process_matches is not True or relay_matches is not True
        or type(ready.get("pid")) is not int or ready["pid"] < 1
        or ready["pid"] != transition.get("trial_pid")
        or ready.get("sha") != sha
        or ready.get("nonce") != transition.get("nonce")
        or ready.get("transition_id") != transition.get("transition_id")
        or any(ready.get(check) is not True for check in checks)
    ):
        return False
    try:
        return _utc(transition["trial_started_at"]) <= _utc(ready["ready_at"]) <= _utc(now)
    except (KeyError, TypeError, ValueError):
        return False


def confirm_transition(
    transition: Mapping[str, Any], ready: Mapping[str, Any], *,
    process_matches: bool, relay_matches: bool, now: str, active=None,
) -> dict[str, Any]:
    if not ready_matches(transition, ready, process_matches=process_matches, relay_matches=relay_matches, now=now, active=active):
        raise ValueError("update application readiness is unproven")
    rollback = transition["phase"] == "ROLLBACK_PENDING_READY"
    return {**transition, "phase": "ROLLED_BACK" if rollback else "CONFIRMED", "confirmed_at": now}


def record_retry_chance(transition: Mapping[str, Any], sha: str, *, now: str) -> tuple[dict[str, Any], bool]:
    """Count one transient failure of ``sha``; return the new retry record and
    whether the sha has used up its chances inside the retry window."""
    sha = _sha(sha)
    current = _utc(now)
    entries: dict[str, Any] = {}
    raw = transition.get("retry_shas")
    for key, entry in (raw.items() if isinstance(raw, Mapping) else ()):
        try:
            _sha(key)
            age = (current - _utc(entry["first_at"])).total_seconds()
            if 0 <= age < RETRY_WINDOW_SECONDS and type(entry["attempts"]) is int and entry["attempts"] >= 1:
                _utc(entry["last_at"])
                entries[key] = {"attempts": entry["attempts"], "first_at": entry["first_at"], "last_at": entry["last_at"]}
        except (ValueError, KeyError, TypeError):
            continue
    prior = entries.get(sha)
    attempts = (prior["attempts"] if prior else 0) + 1
    entries[sha] = {"attempts": attempts, "first_at": prior["first_at"] if prior else now, "last_at": now}
    return entries, attempts >= RETRY_ATTEMPT_LIMIT


def request_rollback(transition: Mapping[str, Any], *, reason: str, transient: bool = False,
                     now: str | None = None) -> dict[str, Any]:
    if transition.get("phase") != "APPLIED_PENDING_READY":
        raise ValueError("rollback requires a pending trial")
    failures = list(transition.get("failed_shas", []))
    candidate = _sha(transition.get("candidate_sha"))
    extra: dict[str, Any] = {}
    permanent = True
    if transient:
        extra["retry_shas"], permanent = record_retry_chance(transition, candidate, now=now or now_utc())
    if permanent and candidate not in failures:
        failures.append(candidate)
    return {**transition, "phase": "ROLLBACK_PENDING", "reason": str(reason), "failed_shas": failures, **extra}


def rollback_transition(
    transition: Mapping[str, Any], *, writers_absent: bool,
    previous_verified: bool, durable_rollback: bool, nonce: str, now: str,
) -> dict[str, Any]:
    if transition.get("phase") != "ROLLBACK_PENDING":
        raise ValueError("rollback request is absent")
    if writers_absent is not True:
        return {**transition, "phase": "UNKNOWN", "reason": "writer absence is unproven"}
    if previous_verified is not True or durable_rollback is not True:
        raise ValueError("rollback requires verified previous code and a durable request")
    if transition.get("failure_decision", {}).get("selected_sha") != transition.get("previous_sha"):
        raise ValueError("rollback requires an observed unchanged data generation")
    _utc(now)
    # Restored code is a pending recovery until its actual ordinary relay is
    # prepared again. The same role-specific READY contract closes this phase.
    restored = {**transition, "phase": "ROLLBACK_PENDING_READY", "nonce": _sha(nonce, length=32),
                "trial_pid": None, "trial_sha": None, "trial_started_at": None, "rollback_at": now}
    restored.pop("confirmed_at", None)
    return restored


def recovery_sha(transition: Mapping[str, Any], *, pointer_sha: str) -> str:
    """Choose the recorded pointer after power loss; never restore user data."""
    if transition.get("schema") != SCHEMA:
        raise ValueError("startup update record is unsupported")
    phase = transition.get("phase")
    previous = _sha(transition.get("previous_sha"))
    candidate = _sha(transition.get("candidate_sha"))
    if pointer_sha not in {previous, candidate}:
        raise ValueError("active pointer differs from both recorded versions")
    if phase == "PREPARED":
        if pointer_sha != previous:
            raise ValueError("pointer changed before the applied record was durable")
        return previous
    if phase in {"APPLIED_PENDING_READY", "CONFIRMED"}:
        return candidate
    if phase == "ROLLBACK_PENDING":
        selected = transition.get("failure_decision", {}).get("selected_sha", candidate)
        if selected not in {previous, candidate}:
            raise ValueError("failure decision names another version")
        return selected
    if phase == "DEFERRED" and transition.get("reason_code") == UPDATE_RELAY_STOP_REQUESTED:
        if (transition.get("retained_candidate", False) is not False
                or "recovery_pending" in transition):
            raise ValueError("relay stop deferral is not an unapplied transition")
        return pointer_sha
    if phase == "DEFERRED" and transition.get("retained_candidate") is True:
        if transition.get("failure_decision", {}).get("selected_sha") != candidate:
            raise ValueError("retained trial decision differs from candidate")
        return candidate
    if phase in {"ROLLBACK_PENDING_READY", "ROLLED_BACK"}:
        return previous
    if phase in {"UNKNOWN", "FAILED"}:
        raise ValueError("unproven recovery must preserve the pointer and stop switching")
    raise ValueError("recovery phase is unsupported")


def read_trial_generation(policy: Mapping[str, Any]) -> dict[str, Any]:
    """Read the ordinary operator's shared commit slot, never SYSTEM's profile."""
    from kmtech_shared import data_generation as generation
    from writer_session_fence import canonical_control_root

    try:
        sid = policy["operator_sid"]
        generation._identity("Label_Match", sid)
        local = Path(policy["operator_local_app_data"])
        if not local.is_absolute():
            raise ValueError("operator profile is not absolute")
        path = canonical_control_root({"LOCALAPPDATA": str(local)}) / "data-generation" / (sid + ".json")
        record = generation._read(path, "Label_Match", sid)
        return {"status": "READ", "marker_path": str(path), "record": record}
    except (OSError, ValueError, TypeError, KeyError, generation.DataGenerationError):
        return {"status": "UNKNOWN"}


def decide_trial_failure(transition: Mapping[str, Any], policy: Mapping[str, Any]) -> dict[str, Any]:
    """After normal writer stop, choose once from actual generation readback.

    The caller durably publishes this decision before changing any pointer.
    Both GUI and relay trials share this boundary; READY is never a G proxy.
    """
    from kmtech_shared import data_generation as generation

    if transition.get("phase") != "ROLLBACK_PENDING":
        raise ValueError("trial failure requires a normal-stop request")
    if transition.get("failure_decision") is not None:
        return dict(transition)
    before = transition.get("generation_before", {"status": "UNKNOWN"})
    after = read_trial_generation(policy)
    previous = transition.get("generation_previous_release", {})
    candidate = transition.get("generation_candidate_release", {})
    statuses = {}
    for name, release in (("previous", previous), ("candidate", candidate)):
        statuses[name] = generation.probe_downgrade(
            [after["marker_path"]] if after.get("status") == "READ" else [],
            release.get("version"), app_id="Label_Match",
            subject_sid=policy.get("operator_sid"), target_build_id=release.get("source_commit"),
        )["status"]
    # An I/O race is uncertainty, even if a separate guard probe succeeded.
    stable = after == read_trial_generation(policy)
    unchanged = (stable and before.get("status") == after.get("status") == "READ"
                 and before == after and statuses["previous"] == "ALLOW")
    code = "TRIAL_GENERATION_UNCHANGED" if unchanged else "TRIAL_GENERATION_UNKNOWN"
    record = after.get("record") or {}
    if (not unchanged and stable and before.get("status") == after.get("status") == "READ"
            and record.get("build_id") == candidate.get("source_commit")
            and record.get("version") == candidate.get("version")):
        code = "TRIAL_GENERATION_ADVANCED"
    selected = transition["previous_sha"] if unchanged else transition["candidate_sha"]
    decision = {"before": before, "after": after, "previous_status": statuses["previous"],
                "candidate_status": statuses["candidate"], "selected_sha": selected,
                "code": code, "ready_result": "FAILED", "decided_at": now_utc()}
    state = {**transition, "failure_decision": decision}
    if not unchanged:
        state = {**state, "phase": "DEFERRED", "retained_candidate": True,
                 "deferred_at": now_utc(), "reason": code}
    return state



PIN_FIELDS = ("from_commit", "to_commit", "from_contract_sha256", "to_contract_sha256")


def directional_pin(
    approvals: list[Mapping[str, Any]], *, from_commit: str, to_commit: str,
    from_contract_sha256: str, to_contract_sha256: str,
) -> str:
    """Accept an explicit direction; the reverse pair never grants this one."""
    wanted = dict(zip(PIN_FIELDS, (
        _sha(from_commit), _sha(to_commit),
        _sha(from_contract_sha256, length=64), _sha(to_contract_sha256, length=64),
    )))
    if wanted["from_commit"] == wanted["to_commit"]:
        raise ValueError("update direction must name different commits")
    if not isinstance(approvals, list):
        raise ValueError("approved update transitions must be a list")
    matched = False
    seen = set()
    for entry in approvals:
        if not isinstance(entry, Mapping) or set(entry) != set(PIN_FIELDS):
            raise ValueError("approved update transition fields differ")
        values = tuple(_sha(entry[field], length=40 if field.endswith("commit") else 64) for field in PIN_FIELDS)
        if values in seen:
            raise ValueError("approved update transitions are duplicated")
        seen.add(values)
        matched = matched or dict(entry) == wanted
    if not matched:
        raise ValueError("update direction has no exact approval")
    return hashlib.sha256(json.dumps(wanted, sort_keys=True, separators=(",", ":")).encode("ascii")).hexdigest()


def bidirectional_pins(
    approvals: list[Mapping[str, Any]], *, current: Mapping[str, Any],
    candidate: Mapping[str, Any],
) -> tuple[str, str]:
    # Raw consumer hashes are evidence. An installed-key authenticated exact
    # independently reviewed signed pair grants authority; byte equality does not.
    _sha(current.get("data_api_contract_sha256"), length=64)
    _sha(candidate.get("data_api_contract_sha256"), length=64)
    forward = directional_pin(
        approvals, from_commit=current["source_commit"], to_commit=candidate["source_commit"],
        from_contract_sha256=current["writer_sink_inventory_contract_sha256"],
        to_contract_sha256=candidate["writer_sink_inventory_contract_sha256"],
    )
    backward = directional_pin(
        approvals, from_commit=candidate["source_commit"], to_commit=current["source_commit"],
        from_contract_sha256=candidate["writer_sink_inventory_contract_sha256"],
        to_contract_sha256=current["writer_sink_inventory_contract_sha256"],
    )
    return forward, backward


def manifest_transition_pins(manifest: Mapping[str, Any], *, current: Mapping[str, Any], candidate: Mapping[str, Any]) -> tuple[str, str, dict[str, str], str]:
    """Only call after authentication of this complete manifest and packet."""
    current = {**current, "writer_sink_inventory_contract_sha256": current["contract_sha256"]}
    candidate = {**candidate, "writer_sink_inventory_contract_sha256": candidate["contract_sha256"]}
    pair = dict(zip(PIN_FIELDS, (current["source_commit"], candidate["source_commit"], current["writer_sink_inventory_contract_sha256"], candidate["writer_sink_inventory_contract_sha256"])))
    reverse = dict(zip(PIN_FIELDS, (pair["to_commit"], pair["from_commit"], pair["to_contract_sha256"], pair["from_contract_sha256"])))
    approvals = manifest.get("approved_transitions", [])
    if not isinstance(approvals, list) or len(approvals) != 2 or any(entry not in (pair, reverse) for entry in approvals):
        raise ValueError("signed approvals must contain exactly the current/candidate pair and its reverse")
    forward, rollback = bidirectional_pins(approvals, current=current, candidate=candidate)
    return forward, rollback, reverse, "SIGNED_EXACT_PAIR"


def authorize_feed_transition(manifest: Mapping[str, Any], *,
                             current: Mapping[str, Any], candidate: Mapping[str, Any]):
    """Called after signature/packet authentication and actual version readback."""
    pair = manifest.get("auto_update")
    if not isinstance(pair, Mapping) or set(pair) != {
        "from_commit", "to_commit", "forward_contract_sha256", "rollback_contract_sha256"
    }:
        raise ValueError("signed exact pair fields differ")
    if (_sha(pair["from_commit"]) != _sha(current.get("source_commit"))
            or _sha(pair["to_commit"]) != _sha(candidate.get("source_commit"))):
        raise ValueError("signed pair differs from actual installed/candidate identity")
    if manifest.get("source_commit") != candidate.get("source_commit") or manifest.get("version") != candidate.get("version"):
        raise ValueError("signed target identity differs from actual packet")
    ordered = []
    for version in (current.get("version"), manifest.get("version")):
        if not isinstance(version, str) or re.fullmatch(r"v[0-9]+(?:\.[0-9]+){2,3}", version) is None:
            raise ValueError("actual release version is malformed")
        parts = tuple(int(part) for part in version[1:].split("."))
        ordered.append(parts + (0,) * (4 - len(parts)))
    if ordered[1] <= ordered[0]:
        raise ValueError("feed version must be newer than the actual installed version")
    forward, rollback, reverse, authority = manifest_transition_pins(manifest, current=current, candidate=candidate)
    if (_sha(pair["forward_contract_sha256"], length=64) != forward
            or _sha(pair["rollback_contract_sha256"], length=64) != rollback):
        raise ValueError("signed direction contract differs from actual bytes")
    previous = _sha(current.get("data_api_contract_sha256"), length=64)
    target = _sha(candidate.get("data_api_contract_sha256"), length=64)
    evidence = {"previous_data_api_sha256": previous, "candidate_data_api_sha256": target,
                "consumer_bytes_equal": previous == target}
    return forward, rollback, reverse, authority, evidence



DATA_API_FILES = ('contract.lock.json', 'label_recovery_schema.py', 'label_data_manager.py', 'label_completion.py', 'label_transition.py', 'package_command_draft.py', 'package_logistics.py', 'package_outbox.py', 'package_outbox_processor.py', 'deferred_intent_capture.py', 'carrier_identity_port.py', 'sealed_transfer_exchange.py', 'terminal_operation_lease.py', 'producer_runtime_client.py', 'direct_sync_push.py', 'logistics_runtime_profile.py', 'event_stream_policy.py', 'Label_Match.py', 'user_relay.py', 'current_user_onboarding.py')
CONTRACT_FILES = ("tools/label_writer_fence_contract.json", "app/writer_session_fence.py")


def source_version_literal(source: bytes) -> str:
    """Build consistency; signed manifest and verified stock own versions."""
    tree = ast.parse(source.decode("utf-8-sig"))
    versions = [node for node in tree.body if isinstance(node, ast.Assign)
                and any(isinstance(target, ast.Name) and target.id == "APP_VERSION" for target in node.targets)]
    if (len(versions) != 1 or not isinstance(versions[0].value, ast.Constant)
            or not isinstance(versions[0].value.value, str)
            or re.fullmatch(r"v[0-9]+(?:\.[0-9]+){2,3}", versions[0].value.value) is None):
        raise ValueError("actual application literal is malformed")
    return versions[0].value.value


def version_identity(read_bytes) -> dict[str, Any]:
    value = json.loads(read_bytes("portable-manifest.json").decode("utf-8-sig"), object_pairs_hook=unique_object)
    if (value.get("schema") != "label-match-portable-tree-v1"
            or type(value.get("startup_update_protocol")) is not int or value["startup_update_protocol"] != 1):
        raise ValueError("version requires a stock portable packet")
    version = value.get("version")
    if not isinstance(version, str) or re.fullmatch(r"v[0-9]+(?:\.[0-9]+){2,3}", version) is None:
        raise ValueError("설치 판 번호 기록이 없는 옛 설치 파일입니다. 새 설치 파일로 지원 재설치를 진행하세요.")
    if source_version_literal(read_bytes("app/Label_Match.py")) != version:
        raise ValueError("packet version differs from its actual application literal")
    digests = []
    for paths in (CONTRACT_FILES, tuple("app/" + name for name in DATA_API_FILES)):
        digest = hashlib.sha256()
        for name in paths:
            digest.update((name + "\n").encode("ascii"))
            digest.update(hashlib.sha256(read_bytes(name)).digest())
        digests.append(digest.hexdigest())
    return {**value, "source_commit": _sha(value.get("source_commit")), "version": version,
            "contract_sha256": digests[0], "data_api_contract_sha256": digests[1]}


def packet_version_identity(packet: Path) -> dict[str, Any]:
    import zipfile
    with zipfile.ZipFile(packet) as archive:
        return version_identity(lambda name: archive.read("Label_Match/" + name))


def data_api_contract_sha256(app_root: Path) -> str:
    digest = hashlib.sha256()
    for name in DATA_API_FILES:
        path = app_root / name
        _assert_no_reparse_components(path)
        digest.update((name + "\n").encode("ascii"))
        raw = path.read_bytes()
        digest.update(hashlib.sha256(raw).digest())
    return digest.hexdigest()


def unique_object(pairs):
    value = {}
    for name, item in pairs:
        if name in value:
            raise ValueError("duplicate update record field")
        value[name] = item
    return value


def read_json(path: Path, *, maximum: int = 1024 * 1024) -> dict[str, Any]:
    _assert_no_reparse_components(path)
    for attempt in range(3):
        try:
            if path.stat().st_size > maximum:
                raise ValueError("update record is oversized")
            with path.open("r", encoding="utf-8-sig") as source:
                value = json.load(source, object_pairs_hook=unique_object)
            break
        except PermissionError:
            if attempt == 2:
                raise
            time.sleep(0.05)
    if not isinstance(value, dict):
        raise ValueError("update record must be an object")
    return value


from urllib.parse import urlparse, parse_qsl
UPDATE_GITHUB_UPDATE_HOSTS = {"github.com", "githubusercontent.com", "github.io"}
UPDATE_SECRET_QUERY_KEYS = {
    "access_token",
    "api_key",
    "client_secret",
    "github_token",
    "pat",
    "private_key",
    "sig",
    "signature",
    "token",
}
UPDATE_SECRET_QUERY_PREFIXES = ("x_amz_", "x_goog_")
def _assert_https_update_url(url, *, require_zip=False):
    parsed = urlparse(str(url or ""))
    if parsed.scheme.lower() != "https" or not parsed.netloc:
        raise ValueError("Update URL must be HTTPS")
    if not parsed.hostname or not parsed.hostname.isascii():
        raise ValueError("업데이트 주소의 호스트는 ASCII IP 또는 도메인이어야 합니다.")
    parsed.port  # Reject a malformed port before requesting a URL.
    if parsed.username is not None or parsed.password is not None:
        raise ValueError("Update URL must not include userinfo")
    if parsed.fragment:
        raise ValueError("Update URL must not include fragments")
    if require_zip and not parsed.path.lower().endswith(".zip"):
        raise ValueError("Update artifact URL must point to a .zip file")
    for key, _value in parse_qsl(parsed.query, keep_blank_values=True):
        normalized_key = key.lower().replace("-", "_")
        if normalized_key in UPDATE_SECRET_QUERY_KEYS or normalized_key.startswith(UPDATE_SECRET_QUERY_PREFIXES):
            raise ValueError("Update URL must not contain raw token query parameters")
def _is_github_hosted_update_url(url):
    parsed = urlparse(str(url or ""))
    host = (parsed.hostname or "").lower().rstrip(".")
    return any(host == root or host.endswith("." + root) for root in UPDATE_GITHUB_UPDATE_HOSTS)
assert_https_update_url = _assert_https_update_url


def policy_from_settings(settings: Mapping[str, Any]) -> dict[str, Any]:

    if not isinstance(settings, Mapping):
        raise ValueError("auto-update settings must be an object")
    allowed = {"feed_url", "channel", "apply_during_work", "profile_auth"}
    if set(settings) - allowed:
        raise ValueError("auto-update settings contain unsupported fields")
    feed = settings.get("feed_url", "")
    if not isinstance(feed, str):
        raise ValueError("auto-update feed must be a string")
    if feed:
        assert_https_update_url(feed)
        if _is_github_hosted_update_url(feed):
            raise ValueError("daily update feed cannot use GitHub hosting")
    if settings.get("apply_during_work", False) is not False:
        raise ValueError("applying during work requires separate scanner acceptance")
    channel = settings.get("channel", "stable")
    if not isinstance(channel, str) or re.fullmatch(r"[a-z0-9_-]{1,32}", channel) is None:
        raise ValueError("auto-update channel is malformed")
    auth = settings.get("profile_auth", bool(feed))
    if feed and auth is not True:
        raise ValueError("company feed requires existing device authentication")
    if type(auth) is not bool:
        raise ValueError("profile authentication setting must be a boolean")
    return {"feed_url": feed, "channel": channel, "apply_during_work": False, "profile_auth": auth}


def canonical_startup_settings(settings: Mapping[str, Any]) -> bytes:
    return (json.dumps(policy_from_settings(settings), sort_keys=True, indent=2) + "\n").encode("utf-8")


def load_policy(control: Path) -> dict[str, Any]:
    policy = read_json(control / "policy.json")
    path = control.parent / "current/app/auto-update-settings.json"
    _assert_no_reparse_components(path)
    raw = path.read_bytes()
    canonical = canonical_startup_settings(json.loads(raw))
    digest = hashlib.sha256(canonical).hexdigest()
    if raw != canonical or digest != _sha(policy.get("settings_sha256"), length=64):
        raise ValueError("installed startup settings differ from their protected canonical SHA")
    if policy_from_settings({k: policy[k] for k in ("feed_url", "channel", "apply_during_work", "profile_auth")}) != json.loads(canonical):
        raise ValueError("installed startup policy differs from its packet settings")
    return policy


@writer_sink("auto_update_write_json_durable")
def write_json_durable(path: Path, payload: Mapping[str, Any]) -> None:
    """Write-through replacement keeps the journal ahead of the active pointer."""
    # failure-<uuid>.json is a diagnostic snapshot, never a recovery input.
    if re.fullmatch(r"failure-[0-9a-f]{32}\.json", path.name):
        from failure_redaction import redact_json_for_sink
        serialized = json.dumps(redact_json_for_sink(dict(payload)), sort_keys=True,
                                separators=(",", ":"), ensure_ascii=True) + "\n"
    else:
        from failure_redaction import assert_machine_metadata_text
        serialized = json.dumps(dict(payload), sort_keys=True, separators=(",", ":"),
                                ensure_ascii=True) + "\n"
        assert_machine_metadata_text(serialized, environ=os.environ)
    _assert_no_reparse_components(path)
    temporary = path.with_name(path.name + "." + uuid.uuid4().hex + ".tmp")
    with temporary.open("x", encoding="utf-8", newline="\n") as output:
        output.write(serialized)
        output.flush()
        os.fsync(output.fileno())
    atomic_replace(temporary, path, write_through=True)


def control_root(app_root: Path) -> Path | None:
    code = app_root.absolute()
    if code.name != "app":
        return None
    bundle = code.parent
    if bundle.name == "current":
        managed = bundle.parent
    elif bundle.parent.name == "versions" and re.fullmatch(r"[0-9a-f]{40}", bundle.name):
        managed = bundle.parent.parent
    else:
        return None
    policy = managed / "update-control" / "policy.json"
    if not policy.is_file():
        return None
    _assert_no_reparse_components(policy)
    return policy.parent


def version_bundle(control: Path, sha: str) -> Path:
    return control.parent / "versions" / _sha(sha)


_VERSION_WRITER_INVENTORY_PASS = {}


@startup_timed("full_inventory")
def version_writer_inventory_sha256(bundle: Path) -> str:
    from writer_sink_inventory import _runtime_files
    from writer_session_fence import remote_request
    selected = bundle.absolute()
    def inputs():
        paths = [*_runtime_files(bundle / "app", ".py"), *_runtime_files(bundle / "app", ".ps1"),
                 bundle / "INSTALL_THIS_PC.ps1"]
        result = []
        for path in sorted(paths):
            _assert_no_reparse_components(path)
            result.append((path.relative_to(bundle).as_posix(), path.read_bytes()))
        return tuple(result)
    def shape():
        # Metadata only (no file bytes) of the same file set.
        paths = [*_runtime_files(bundle / "app", ".py"), *_runtime_files(bundle / "app", ".ps1"),
                 bundle / "INSTALL_THIS_PC.ps1"]
        rows = []
        for path in sorted(paths):
            _assert_no_reparse_components(path)
            info = path.lstat()
            rows.append((path.relative_to(bundle).as_posix(), info.st_size, info.st_mtime_ns))
        return tuple(rows)
    with remote_request():
        cached = _VERSION_WRITER_INVENTORY_PASS.get(selected)
        # Same bundle already verified by this process and metadata unchanged:
        # do not read every file again (G100b); otherwise compare the bytes.
        if cached is not None and len(cached) > 2 and cached[2] == shape():
            return cached[1]
        observed = inputs()
        if cached is None or cached[0] != observed:
            cached = next((entry for entry in _VERSION_WRITER_INVENTORY_PASS.values() if entry[0] == observed), None)
        if cached is not None and cached[0] == observed:
            _VERSION_WRITER_INVENTORY_PASS[selected] = (cached[0], cached[1], shape())
            return cached[1]
        before = shape()
        result = _version_writer_inventory_sha256(bundle)
        if inputs() != observed or shape() != before:
            raise ValueError("writer inventory changed during verification")
        _VERSION_WRITER_INVENTORY_PASS[selected] = (observed, result, before)
        return result


def _version_writer_inventory_sha256(bundle: Path) -> str:
    # Run the already installed scanner over bytes/AST only. Candidate modules
    # are never imported by the protected updater.
    from writer_sink_inventory import derive_writer_sink_inventory, _powershell_inventory, writer_sink_inventory_sha256
    rows = derive_writer_sink_inventory(bundle / "app")
    rows += _powershell_inventory(bundle / "INSTALL_THIS_PC.ps1", bundle)
    rows.sort(key=lambda row: (row.source, row.source_path.casefold(), row.source_line, row.qualified_name))
    return writer_sink_inventory_sha256(rows)


def version_manifest(bundle: Path) -> dict[str, Any]:
    def read_bytes(name):
        path = bundle / name
        _assert_no_reparse_components(path)
        return path.read_bytes()
    value = version_identity(read_bytes)
    if type(value.get("startup_update_protocol")) is not int or value["startup_update_protocol"] != 1:
        raise ValueError("version has no supported startup readiness protocol")
    _sha(value.get("source_commit"))
    _sha(value.get("writer_sink_inventory_contract_sha256"), length=64)
    if version_writer_inventory_sha256(bundle) != value["writer_sink_inventory_contract_sha256"]:
        raise ValueError("version writer inventory differs from actual source bytes")
    return value


def installation_root(code_root: Path | None = None) -> Path | None:
    root = (code_root or Path(__file__).resolve().parent).absolute()
    if root.name.casefold() == "app":
        root = root.parent
    if root.name.casefold() == "current":
        return root.parent
    if root.parent.name.casefold() == "versions" and re.fullmatch(r"[0-9a-f]{40}", root.name):
        return root.parent.parent
    return None


def assert_registration_storage(code_root: Path, paths) -> None:
    """Check storage before registration can write into machine-owned code."""
    managed = installation_root(code_root)
    roots = [Path("C:/KMTech/Apps/Label_Match")]
    if managed is not None and managed not in roots:
        roots.append(managed)
    for value in paths:
        candidate = Path(value).expanduser().absolute()
        _assert_no_reparse_components(candidate)
        for machine in roots:
            for slot in ("current", "versions", "update-control", "removal-evidence", "removed.json"):
                boundary = machine / slot
                if candidate == boundary or candidate.is_relative_to(boundary) or boundary.is_relative_to(candidate):
                    raise ValueError(f"등록 자료 경로가 코드 또는 control 영역과 겹칩니다: {candidate}")


@startup_timed("full_observe_lifecycle")
def lifecycle_task_observation(root: Path) -> dict[str, Any]:
    """External supported stock can observe task absence after code removal."""
    import subprocess
    helper = Path(__file__).resolve().parent.parent / "tools/apply_prepared_update.ps1"
    completed = subprocess.run([system_executable("WindowsPowerShell/v1.0/powershell.exe"),
        "-NoLogo", "-NoProfile", "-NonInteractive", "-WindowStyle", "Hidden", "-ExecutionPolicy", "Bypass", "-File", str(helper),
        "-InstallRoot", str(root / "current"), "-Mode", "ObserveLifecycle", "-SupportedLifecycle"],
        capture_output=True, text=True, creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0))
    if completed.returncode:
        error = RuntimeError("supported task observation failed")
        error.child_output = child_failure_output(completed)
        raise error
    return json.loads(completed.stdout)


def _supported_removal_artifacts(root: Path) -> list[Path]:
    result = []
    for name in ("versions", "update-control"):
        parent = root / name
        if not (parent.exists() or parent.is_symlink() or parent.is_junction()):
            continue
        if parent.is_symlink() or parent.is_junction() or not parent.is_dir():
            result.append(parent)
            continue
        for child in parent.iterdir():
            if name == "versions":
                if re.fullmatch(r"[0-9a-f]{40}", child.name):
                    result.append(child)
            elif child.name in {"policy.json", "active.json", "transition.json"} or re.fullmatch(
                    r"(?:packet|failure|policy-before)-[0-9a-f]{32}(?:\.json)?", child.name):
                result.append(child)
    try:
        marker = read_json(root / "removed.json")
        if marker.get("schema") == "label-match-supported-removal-v1" and marker.get("lifecycle_state") == "REMOVED":
            retained = marker.get("retained_paths", ())
            if isinstance(retained, list):
                result = [path for path in result if not any(isinstance(value, str)
                    and (path.is_relative_to(root / "versions") or path.name.startswith("packet-"))
                    and (Path(value) == path or Path(value).is_relative_to(path)) for value in retained)]
    except (OSError, ValueError):
        pass
    return result


def resolve_lifecycle_context(root: Path, *, expected_from_commit: str | None = None,
                              operation: str = "inspect") -> dict[str, Any]:
    if operation not in {"inspect", "install", "uninstall", "user_remove", "register"}:
        raise ValueError("supported lifecycle operation is malformed")
    fixed = root / "current"
    base = dict(status="PASS", fixed_root=str(fixed), owner_app_root=str(fixed / "app"),
                from_commit=None, active_code_root=None, active_app_root=None)
    if operation in {"uninstall", "user_remove"}:
        return {**base, "lifecycle_state": "REMOVAL_READY"}
    control = root / "update-control"
    for output in (fixed, root / "versions", control):
        _assert_no_reparse_components(output)
        if output.exists() and not output.is_dir():
            raise ValueError(f"설치가 만드는 경로와 남긴 항목이 겹칩니다. 자료를 보존하고 경로를 확인하세요: {output}")
    policy_path = control / "policy.json"
    if not fixed.exists() and not _supported_removal_artifacts(root):
        if expected_from_commit is not None or operation == "register":
            raise ValueError("supported bootstrap was removed")
        observation = lifecycle_task_observation(root)
        if observation.get("task_exists") is not False:
            raise ValueError("supported removal task absence is unproven")
        return {**base, "lifecycle_state": "REMOVED"}
    if not policy_path.is_file():
        raise ValueError(f"설치가 만드는 경로와 남긴 항목이 겹칩니다. 자료를 보존하고 경로를 확인하세요: {fixed}")
    _assert_no_reparse_components(fixed)
    if not fixed.is_dir():
        raise ValueError("supported bootstrap is absent")
    policy = load_policy(control)
    pointer = read_json(control / "active.json")
    state = read_json(control / "transition.json")
    if pointer.get("schema") != SCHEMA or state.get("schema") != SCHEMA:
        raise ValueError("supported control identity differs")
    active = _sha(pointer.get("sha"))
    if state.get("phase") not in {"CONFIRMED", "ROLLED_BACK", "DEFERRED"}:
        raise ValueError("supported transition requires removal and reinstall")
    if expected_from_commit is not None and _sha(expected_from_commit) != active:
        raise ValueError("supported from identity differs")
    if state.get("candidate_sha") is not None and recovery_sha(state, pointer_sha=active) != active:
        raise ValueError("supported transition and pointer differ")
    # The selected Verify already authenticates the fixed bootstrap in
    # Get-Authority / Assert-LabelStartupIntegrity before verifying the version.
    if operation != "register":
        fixed_helper(control, "Verify", code_root=fixed)
    fixed_identity = version_manifest(fixed)
    selected = version_bundle(control, active)
    fixed_helper(control, "Verify", code_root=selected)
    actual = version_manifest(selected)
    if actual["source_commit"] != active:
        raise ValueError("supported active code identity differs")
    integrity = read_json(fixed / "bootstrap-integrity.json")
    if lifecycle_task_observation(root).get("task_exists") is not True:
        raise ValueError("supported installed task is absent or unknown")
    if read_json(control / "active.json") != pointer or read_json(control / "transition.json") != state:
        raise ValueError("supported lifecycle changed during observation")
    return {**base, "lifecycle_state": "INSTALLED", "from_commit": active,
            "bootstrap_commit": fixed_identity["source_commit"], "phase": state["phase"],
            "active_code_root": str(selected), "active_app_root": str(selected / "app"),
            "bootstrap_aggregate_sha256": _sha(integrity.get("aggregate_sha256"), length=64)}


@writer_sink("auto_update_supported_lifecycle_transition")
def supported_lifecycle_transition(root: Path, *, expected_from_commit: str, candidate_commit: str) -> dict[str, Any]:
    from writer_session_fence import active_fence, canonical_control_root, _delegation_matches
    security = process_security(os.getpid())
    if security.get("elevated") is not True:
        raise ValueError("supported placement requires the elevated installer")
    fence = active_fence(canonical_control_root(), environ=os.environ)
    if fence is None or not _delegation_matches(fence, source="canonical_placement", environ=os.environ):
        raise ValueError("supported placement requires exact installer delegation")
    control = root / "update-control"
    with update_lock(control, load_policy(control)["operator_sid"]):
        context = resolve_lifecycle_context(root, expected_from_commit=expected_from_commit)
        candidate_commit = _sha(candidate_commit)
        control = root / "update-control"
        pointer = read_json(control / "active.json")
        state = read_json(control / "transition.json")
        target = version_bundle(control, candidate_commit)
        fixed_helper(control, "Verify", code_root=target)
        identity = version_manifest(target)
        if identity["source_commit"] != candidate_commit:
            raise ValueError("supported candidate identity differs")
        fixed = Path(context["fixed_root"])
        for name in ("update-manifest-key-config.json", "auto-update-settings.json"):
            if (target / "app" / name).read_bytes() != (fixed / "app" / name).read_bytes():
                raise ValueError("supported candidate trust/settings differ from fixed bootstrap")
        if read_json(control / "active.json") != pointer or read_json(control / "transition.json") != state:
            raise ValueError("supported lifecycle changed during placement")
        if candidate_commit == context["from_commit"]:
            # Verify above includes the actual selected inventory and fixed
            # trust/settings. Same identity never grants a new transition.
            return {**context, "code_replaced": False}
        next_state = dict(schema=SCHEMA, phase="DEFERRED", failed_shas=list(state.get("failed_shas", [])),
                          deferred_at=now_utc(), reason="supported placement", transition_id=uuid.uuid4().hex)
        persist_switch(control, next_state, candidate_commit)
        return resolve_lifecycle_context(root, expected_from_commit=candidate_commit)


def _assert_canonical_lifecycle_root(root: Path) -> None:
    if root.absolute() != Path("C:/KMTech/Apps/Label_Match").absolute():
        raise ValueError("supported lifecycle requires the canonical installation")


def lifecycle_context_main(arguments: list[str]) -> int:
    import argparse
    parser = __import__("failure_redaction").InstallerArgumentParser()
    parser.add_argument("--install-root", type=Path, required=True)
    parser.add_argument("--expected-from-commit")
    parser.add_argument("--operation", default="inspect")
    parser.add_argument("--activate-supported-commit")
    parser.add_argument("--machine-installer-authority", action="store_true")
    parser.add_argument("--installer-authority-root", type=Path)
    parser.add_argument("--installer-owner-record", type=Path)
    args = parser.parse_args(arguments)
    try:
        _assert_canonical_lifecycle_root(args.install_root)
        from contextlib import nullcontext
        from writer_session_fence import installer_lifecycle_authority
        if args.machine_installer_authority and (args.installer_authority_root is None or args.installer_owner_record is None):
            raise ValueError("machine installer authority is required")
        if (args.installer_authority_root is None) != (args.installer_owner_record is None):
            raise ValueError("installer lifecycle authority arguments are incomplete")
        authority = (installer_lifecycle_authority(args.installer_authority_root, args.installer_owner_record,
            args.install_root, source="canonical_placement" if args.activate_supported_commit else "auto_update_fixed_helper")
            if args.installer_authority_root is not None else nullcontext())
        with authority:
            if args.activate_supported_commit:
                result = supported_lifecycle_transition(args.install_root,
                    expected_from_commit=args.expected_from_commit, candidate_commit=args.activate_supported_commit)
            else:
                result = resolve_lifecycle_context(args.install_root,
                    expected_from_commit=args.expected_from_commit, operation=args.operation)
    except Exception as error:
        from failure_redaction import emit_support_failure
        try:
            emit_support_failure(error, prefix="LABEL_MATCH_LIFECYCLE_FAILURE")
        except Exception:
            # Evidence failure must still preserve native rc and a fixed stderr
            # record; no raw exception or authority enters the public channel.
            print("LABEL_MATCH_LIFECYCLE_FAILURE=" + json.dumps({"status":"FAILED", "stage":"SUPPORT", "reason_code":"INSTALLER_FAILED",
                "exit_code":1, "report_status":"REPORT_UNAVAILABLE"}), file=sys.stderr, flush=True)
        return 1
    print(json.dumps(result, ensure_ascii=True))
    return 0


def verify_prepared_packet(
    stage: Path, *, key_config: str, current_version: str, channel: str,
) -> tuple[dict[str, Any], dict[str, Any]]:
    from Label_Match import (
        _sha256_file as file_sha256, _verify_update_manifest_signature as verify_update_manifest_signature,
        _update_candidate_from_manifest as update_candidate_from_private_manifest,
    )

    manifest = read_json(stage / "manifest.json")
    signature_path = stage / "manifest.sig"
    _assert_no_reparse_components(signature_path)
    if signature_path.stat().st_size > 16384:
        raise ValueError("update signature is oversized")
    verify_update_manifest_signature(manifest, signature_path.read_bytes(), key_config)
    candidate = update_candidate_from_private_manifest(manifest, current_version=current_version, expected_channel=channel)
    if candidate is None or candidate["archive"].get("layout") != "canonical_portable":
        raise ValueError("update is not a targeted canonical portable candidate")
    packet = stage / "packet.zip"
    _assert_no_reparse_components(packet)
    if packet.stat().st_size != manifest["artifact"]["size_bytes"] or file_sha256(packet) != candidate["sha256"]:
        raise ValueError("prepared packet size or SHA256 differs")
    _sha(manifest.get("source_commit"))
    return manifest, candidate


def task_name(app_root: Path, operator_sid: str) -> str:
    if not isinstance(operator_sid, str) or re.fullmatch(r"S-1-(?:[0-9]+-)+[0-9]+", operator_sid) is None:
        raise ValueError("update operator SID is malformed")
    path = os.path.normcase(str(app_root.absolute())).replace("/", "\\")
    identity = hashlib.sha256((path + "\n" + operator_sid).encode("utf-8")).hexdigest()[:24]
    return "KMTech.LabelMatch.StartupUpdate." + identity


def logon_task_xml(*, app_root: Path, operator_sid: str) -> str:
    """Generate only a fixed protected helper action, with a SID logon trigger."""
    from xml.sax.saxutils import escape

    task_name(app_root, operator_sid)
    root = str(app_root.absolute())
    if any(character in root for character in ('"', "\r", "\n")):
        raise ValueError("update install path is unsafe")
    arguments = '-NoLogo -NoProfile -NonInteractive -WindowStyle Hidden -ExecutionPolicy Bypass -File "' + root + '\\tools\\apply_prepared_update.ps1" -InstallRoot "' + root + '"'
    executable = system_executable("WindowsPowerShell/v1.0/powershell.exe")
    return (
        '<?xml version="1.0" encoding="UTF-16"?>'
        '<Task version="1.4" xmlns="http://schemas.microsoft.com/windows/2004/02/mit/task">'
        '<Triggers><LogonTrigger><Enabled>true</Enabled><UserId>' + escape(operator_sid) + '</UserId></LogonTrigger></Triggers>'
        '<Principals><Principal id="System"><UserId>S-1-5-18</UserId><RunLevel>HighestAvailable</RunLevel></Principal></Principals>'
        '<Settings><MultipleInstancesPolicy>IgnoreNew</MultipleInstancesPolicy><DisallowStartIfOnBatteries>false</DisallowStartIfOnBatteries><StopIfGoingOnBatteries>false</StopIfGoingOnBatteries><AllowHardTerminate>false</AllowHardTerminate><StartWhenAvailable>true</StartWhenAvailable><AllowStartOnDemand>true</AllowStartOnDemand><Hidden>true</Hidden><ExecutionTimeLimit>PT0S</ExecutionTimeLimit></Settings>'
        '<Actions Context="System"><Exec><Command>' + escape(executable) + '</Command><Arguments>' + escape(arguments) + '</Arguments></Exec></Actions></Task>'
    )


def install_task_spec(app_root: Path, operator_sid: str) -> dict[str, str]:
    return {"name": task_name(app_root, operator_sid), "xml": logon_task_xml(app_root=app_root, operator_sid=operator_sid)}


def now_utc() -> str:
    return datetime.now(timezone.utc).isoformat()


def system_executable(relative: str) -> str:
    import ctypes
    from ctypes import wintypes

    if os.name != "nt":
        raise OSError("startup updates require Windows")
    kernel = ctypes.WinDLL("kernel32", use_last_error=True)
    get_directory = kernel.GetSystemDirectoryW
    get_directory.argtypes = [wintypes.LPWSTR, wintypes.UINT]
    get_directory.restype = wintypes.UINT
    buffer = ctypes.create_unicode_buffer(32768)
    size = get_directory(buffer, len(buffer))
    if not size or size >= len(buffer):
        raise OSError(ctypes.get_last_error(), "Windows system directory is unobservable")
    return str(Path(buffer.value) / relative)


def optional_record(path: Path) -> dict[str, Any] | None:
    try:
        return read_json(path)
    except FileNotFoundError:
        return None


def prepared_marker_sha256(policy: Mapping[str, Any] | None) -> str | None:
    if not policy or not policy.get("staging_root"):
        return None
    try:
        path = Path(policy["staging_root"]) / "prepared.json"
        _assert_no_reparse_components(path)
        with path.open("rb") as stream:
            return hashlib.file_digest(stream, "sha256").hexdigest()
    except (OSError, ValueError, WriterFenceError):
        return None


@contextmanager
def update_lock(control: Path, operator_sid: str, *, trial: bool = False, abandoned_check=None):
    """One cross-session mutex shared by SYSTEM and the installed operator SID."""
    import ctypes
    from ctypes import wintypes

    task_name(control.parent / "current", operator_sid)
    kernel = ctypes.WinDLL("kernel32", use_last_error=True)
    advapi = ctypes.WinDLL("advapi32", use_last_error=True)
    class Attributes(ctypes.Structure):
        _fields_ = [("length", wintypes.DWORD), ("descriptor", wintypes.LPVOID), ("inherit", wintypes.BOOL)]
    descriptor = wintypes.LPVOID()
    convert = advapi.ConvertStringSecurityDescriptorToSecurityDescriptorW
    convert.argtypes = [wintypes.LPCWSTR, wintypes.DWORD, ctypes.POINTER(wintypes.LPVOID), ctypes.POINTER(wintypes.DWORD)]
    convert.restype = wintypes.BOOL
    sddl = "D:P(A;;GA;;;SY)(A;;GA;;;BA)(A;;0x00100001;;;" + operator_sid + ")"
    if not convert(sddl, 1, ctypes.byref(descriptor), None):
        raise OSError(ctypes.get_last_error(), "update mutex ACL conversion failed")
    create = kernel.CreateMutexExW
    create.argtypes = [ctypes.POINTER(Attributes), wintypes.LPCWSTR, wintypes.DWORD, wintypes.DWORD]
    create.restype = wintypes.HANDLE
    kernel.WaitForSingleObject.argtypes = [wintypes.HANDLE, wintypes.DWORD]
    kernel.WaitForSingleObject.restype = wintypes.DWORD
    kernel.ReleaseMutex.argtypes = [wintypes.HANDLE]
    kernel.CloseHandle.argtypes = [wintypes.HANDLE]
    kernel.LocalFree.argtypes = [wintypes.HLOCAL]
    identity = hashlib.sha256(os.path.normcase(str(control.absolute())).encode("utf-8")).hexdigest()[:24]
    attributes = Attributes(ctypes.sizeof(Attributes), descriptor, False)
    try:
        handle = create(ctypes.byref(attributes), "Global\\KMTech.LabelMatch.Update." + identity + (".Trial" if trial else ""), 0, 0x00100001)
    finally:
        kernel.LocalFree(descriptor)
    if not handle:
        raise OSError(ctypes.get_last_error(), "update mutex cannot be opened")
    acquired = False
    try:
        result = kernel.WaitForSingleObject(handle, 5000)
        acquired = result in {0, 0x80}
        if result == 0x80:
            if abandoned_check is None or trial:
                raise RuntimeError("update mutex ownership was abandoned")
            # WAIT_ABANDONED grants ownership. Keep it until the caller has
            # proved the durable state and installed bytes, before yielding.
            abandoned_check()
        if result == 0x102:
            raise RuntimeError("update mutex ownership is busy")
        if result not in {0, 0x80}:
            raise OSError(ctypes.get_last_error(), "update mutex wait failed")
        yield
    finally:
        if acquired:
            kernel.ReleaseMutex(handle)
        kernel.CloseHandle(handle)


@writer_sink("auto_update_fixed_helper")
def fixed_helper(control: Path, mode: str, *, code_root: Path | None = None) -> str:
    if mode == "Verify":
        from writer_session_fence import remote_request
        with startup_timing_step("full_lifecycle_verify"), remote_request():
            return _run_fixed_helper(control, mode, code_root=code_root)
    return _run_fixed_helper(control, mode, code_root=code_root)


def _run_fixed_helper(control: Path, mode: str, *, code_root: Path | None = None) -> str:
    import subprocess

    root = control.parent / "current"
    executable = system_executable("WindowsPowerShell/v1.0/powershell.exe")
    arguments = [str(executable), "-NoLogo", "-NoProfile", "-NonInteractive", "-WindowStyle", "Hidden", "-ExecutionPolicy", "Bypass", "-File", str(root / "tools/apply_prepared_update.ps1"), "-InstallRoot", str(root), "-Mode", mode]
    if code_root is not None:
        arguments.extend(["-CodeRoot", str(code_root)])
    completed = subprocess.run(arguments, capture_output=True, text=True, creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0))
    if completed.returncode:
        error = RuntimeError("fixed update helper failed: " + mode + "; " + safe_update_text(completed.stderr))
        error.child_output = child_failure_output(completed)
        from failure_redaction import try_write_failure_evidence
        error.cause_code = "INSTALLER_FAILED"
        reference = try_write_failure_evidence(json.dumps({
            "stage": "FIXED_UPDATE_HELPER", "mode": mode, "child_output": error.child_output,
        }, ensure_ascii=False))
        error._failure_report_ref = ({key: reference[key] for key in ("path", "sha256")}
                                     if "path" in reference and "sha256" in reference else None)
        raise error
    return completed.stdout.strip()


def process_snapshot(control: Path) -> list[dict[str, Any]]:
    # Observe only lists processes (apply_prepared_update.ps1 exits before any
    # Apply/Verify step) and mutates no product state, so it takes no writer
    # admission: a live relay holds admission for its whole cycle.
    rows = json.loads(_run_fixed_helper(control, "Observe"))
    if not isinstance(rows, list) or not all(isinstance(row, Mapping) for row in rows):
        raise ValueError("update process observation is incomplete")
    return rows


def writer_processes(rows: list[Mapping[str, Any]], *, own_pid: int) -> list[Mapping[str, Any]]:
    writers = []
    for row in rows:
        if row.get("pid") == own_pid:
            continue
        if row.get("bootstrap") is True:
            continue
        if row.get("observable") is not True:
            raise ValueError("an application process cannot be observed")
        writers.append(row)
    return writers


def process_matches_trial(row: Mapping[str, Any], *, state: Mapping[str, Any], sha: str, sid: str, role: str) -> bool:
    if role == state.get("trial_role", "gui") and state.get("trial_pid") == row.get("pid") and state.get("trial_started_at"):
        if _utc(row.get("created_at")) != _utc(state["trial_started_at"]):
            return False
    return (
        row.get("observable") is True and row.get("sha") == sha
        and row.get("sid") == sid and row.get("role") == role
        and type(row.get("pid")) is int and row["pid"] > 0
        and row.get("ordinary_user") is True
        and _utc(row.get("created_at")) >= _utc(state["applied_at"] if role == "gui" and state["phase"] == "APPLIED_PENDING_READY" else state.get("rollback_at", state["applied_at"]))
    )


@writer_sink("auto_update_persist_switch")
def persist_switch(control: Path, state: Mapping[str, Any], sha: str) -> None:
    # An interrupted pointer write is repaired from this already-flushed record.
    write_json_durable(control / "transition.json", state)
    write_json_durable(control / "active.json", {"schema": SCHEMA, "sha": _sha(sha), "transition_id": state["transition_id"]})
    pointer = read_json(control / "active.json")
    if pointer.get("sha") != sha or pointer.get("transition_id") != state["transition_id"]:
        raise ValueError("active pointer readback differs from durable decision")


def record_confirmed_transition(control: Path, state: Mapping[str, Any]) -> dict[str, Any]:
    """Record reverse authority only after durable GUI confirmation."""
    if state.get("phase") != "CONFIRMED" or state.get("confirmation_audit_applied") is True:
        return dict(state)
    pair = state.get("rollback_pair")
    if pair is None:
        return dict(state)
    directional_pin([pair], **pair)
    if pair["from_commit"] != state["candidate_sha"] or pair["to_commit"] != state["previous_sha"]:
        raise ValueError("confirmed reverse audit is bound to another transition")
    settled = {**state, "confirmation_audit_applied": True}
    write_json_durable(control / "transition.json", settled)
    return settled


def native_process_identity(pid: int) -> dict[str, Any]:
    """Observe process lifetime without treating inaccessible owners as absent."""
    import ctypes
    from ctypes import wintypes

    if not isinstance(pid, int) or isinstance(pid, bool) or pid <= 0:
        return {"status": "UNKNOWN"}
    kernel = ctypes.WinDLL("kernel32", use_last_error=True)
    kernel.OpenProcess.argtypes = [wintypes.DWORD, wintypes.BOOL, wintypes.DWORD]
    kernel.OpenProcess.restype = wintypes.HANDLE
    kernel.WaitForSingleObject.argtypes = [wintypes.HANDLE, wintypes.DWORD]
    kernel.WaitForSingleObject.restype = wintypes.DWORD
    kernel.GetProcessTimes.argtypes = [wintypes.HANDLE] + [ctypes.POINTER(wintypes.FILETIME)] * 4
    kernel.GetProcessTimes.restype = wintypes.BOOL
    kernel.CloseHandle.argtypes = [wintypes.HANDLE]
    handle = kernel.OpenProcess(0x1000 | 0x00100000, False, pid)
    if not handle:
        return {"status": "ABSENT" if ctypes.get_last_error() == 87 else "UNKNOWN"}
    try:
        wait = kernel.WaitForSingleObject(handle, 0)
        if wait == 0:
            return {"status": "ABSENT"}
        if wait != 258:
            return {"status": "UNKNOWN"}
        times = [wintypes.FILETIME() for _ in range(4)]
        if not kernel.GetProcessTimes(handle, *[ctypes.byref(value) for value in times]):
            return {"status": "UNKNOWN"}
        identity = (times[0].dwHighDateTime << 32) | times[0].dwLowDateTime
        return {"status": "LIVE", "creation_identity": str(identity)}
    finally:
        kernel.CloseHandle(handle)


def new_apply_claim() -> dict[str, Any]:
    owner = native_process_identity(os.getpid())
    if owner["status"] != "LIVE":
        raise ValueError("apply owner creation identity is unobservable")
    return {"schema": "code-update-apply-claim-v1", "status": "RUNNING",
            "pid": os.getpid(), "creation_identity": owner["creation_identity"],
            "started_at": now_utc()}


def read_settled_apply_claim_input(control: Path) -> bytes | None:
    """Read FAST evidence once; RUNNING never triggers an owner probe here."""
    path = control / "apply-claim.json"
    _assert_no_reparse_components(path)
    maximum = 1024 * 1024
    for attempt in range(3):
        try:
            if path.stat().st_size > maximum:
                raise ValueError("apply claim is oversized")
            with path.open("rb") as source:
                raw = source.read(maximum + 1)
            break
        except FileNotFoundError:
            return None
        except PermissionError:
            if attempt == 2:
                raise
            time.sleep(0.05)
    if len(raw) > maximum:
        raise ValueError("apply claim is oversized")
    claim = json.loads(raw.decode("utf-8-sig"), object_pairs_hook=unique_object)
    if (not isinstance(claim, dict) or claim.get("schema") != "code-update-apply-claim-v1"
            or claim.get("status") != "SETTLED"):
        raise ValueError("apply claim is not settled")
    return raw


def apply_claim_status(control: Path) -> str:
    """A protected claim is progress evidence, never pointer or READY authority."""
    try:
        claim = optional_record(control / "apply-claim.json")
        if claim is None:
            return "NONE"
        if claim.get("schema") != "code-update-apply-claim-v1":
            return "UNKNOWN"
        if claim.get("status") == "SETTLED":
            return "NONE"
        if claim.get("status") != "RUNNING" or not isinstance(claim.get("creation_identity"), str):
            return "UNKNOWN"
        owner = native_process_identity(claim.get("pid"))
        if owner["status"] != "LIVE":
            return owner["status"]
        return "LIVE" if owner["creation_identity"] == claim["creation_identity"] else "ABSENT"
    except (OSError, ValueError, TypeError, KeyError):
        return "UNKNOWN"


def complete_apply_claim(control: Path) -> None:
    """Durably retire only this process's claim through the existing writer sink."""
    try:
        claim = optional_record(control / "apply-claim.json")
        if not claim or claim.get("pid") != os.getpid() or claim.get("status") != "RUNNING":
            return
        owner = native_process_identity(os.getpid())
        if owner.get("creation_identity") != claim.get("creation_identity"):
            return
        write_json_durable(control / "apply-claim.json", {**claim, "status": "SETTLED", "completed_at": now_utc()})
    except (OSError, ValueError, TypeError, KeyError):
        emit_support_record({"reason_code": "REPORT_UNAVAILABLE"}, stage="update_apply")


def emit_progress_guidance() -> None:
    report = support_record({"reason_code": "UPDATE_IN_PROGRESS"}, stage="update_apply")
    report["guidance"] = STARTUP_PROGRESS_MESSAGE
    print(json.dumps(report, ensure_ascii=False, sort_keys=True))


def apply_prepared(control: Path) -> int:
    """Preserve a failed preparation; never fall back after an uncertain switch."""
    from contextlib import nullcontext

    policy, before = None, None
    try:
        fixed_helper(control, "Authority")
        policy = load_policy(control)
        before = read_json(control / "active.json")
        return _apply_prepared(control)
    except Exception as error:
        with update_lock(control, policy["operator_sid"]) if policy else nullcontext():
            state = optional_record(control / "transition.json")
            # Authority/policy/active failures are also preparation failures.
            # An in-flight journal remains UNKNOWN until a later SYSTEM decide.
            not_applied = (not state or state.get("phase") in {"CONFIRMED", "ROLLED_BACK", "DEFERRED", "PREPARED"})
            if before is not None:
                try:
                    not_applied = not_applied and read_json(control / "active.json") == before
                except (OSError, ValueError):
                    not_applied = False
            failure = {**(state or {}), "schema": SCHEMA,
                       "phase": "DEFERRED" if not_applied else "UNKNOWN",
                       "deferred_at": now_utc(), "reason_code": "UPDATE_PREPARATION_FAILED",
                       "reason": type(error).__name__, "launch_failure_details": launch_failure_details(error, include_original=True)}
            marker_sha256 = prepared_marker_sha256(policy)
            if not_applied and marker_sha256:
                if (state or {}).get("failed_prepared_sha256") == marker_sha256 and (state or {}).get("phase") == "DEFERRED":
                    return 0
                failure["failed_prepared_sha256"] = marker_sha256
                try:
                    marker = read_json(Path(policy["staging_root"]) / "prepared.json")
                    failed_sha = _sha(marker.get("source_commit"))
                    failure["failed_shas"] = sorted(set(failure.get("failed_shas", [])) | {failed_sha})
                except (OSError, ValueError, TypeError):
                    pass  # Malformed hints are quarantined by their exact bytes.
            write_json_durable(control / ("failure-" + uuid.uuid4().hex + ".json"), failure)
            write_json_durable(control / "transition.json", failure)
        return 0 if not_applied else 20

    finally:
        complete_apply_claim(control)


def settle_trial_failure(control: Path, state: Mapping[str, Any], policy: Mapping[str, Any]) -> dict[str, Any]:
    """SYSTEM only, with writer absence and the update lock already proved."""
    state = decide_trial_failure({**state, "phase": "ROLLBACK_PENDING",
                                  "recovery_pending": False}, policy)
    write_json_durable(control / "transition.json", state)
    selected = state["failure_decision"]["selected_sha"]
    fixed_helper(control, "Verify", code_root=version_bundle(control, selected))
    if selected == state["previous_sha"]:
        state = rollback_transition(state, writers_absent=True, previous_verified=True,
                                    durable_rollback=True, nonce=uuid.uuid4().hex, now=now_utc())
    persist_switch(control, state, selected)
    return state



def authorize_prepared_replay(control: Path, policy: Mapping[str, Any], state: Mapping[str, Any]) -> None:
    """Reauthenticate a durable PREPARED record after an interrupted apply."""
    active = read_json(control / "active.json")
    if active.get("sha") != state.get("previous_sha"):
        raise ValueError("prepared replay differs from the active pointer")
    protected = Path(state["prepared_packet_root"])
    if (protected.parent != control or re.fullmatch(r"packet-[0-9a-f]{32}", protected.name) is None):
        raise ValueError("prepared replay packet is outside protected storage")
    previous = version_bundle(control, state["previous_sha"])
    target = version_bundle(control, state["candidate_sha"])
    for bundle in (previous, target):
        fixed_helper(control, "Verify", code_root=bundle)
    current, candidate = version_manifest(previous), version_manifest(target)
    key = read_json(control.parent / "current/app/update-manifest-key-config.json")["manifest_public_key"]
    manifest, packet = verify_prepared_packet(protected, key_config=key,
        current_version=current["version"], channel=policy["channel"])
    if packet["sha256"] != state["packet_sha256"]:
        raise ValueError("prepared replay packet differs")
    pins = authorize_feed_transition(manifest, current=current, candidate=candidate)
    if pins[:2] != (state["forward_pin"], state["rollback_pin"]):
        raise ValueError("prepared replay direction differs")
    if candidate["source_commit"] in state.get("failed_shas", []):
        raise ValueError("a failed version must not be reapplied")


class _RollbackReadyPending(Exception):
    """Unwind update ownership before supervising the ordinary recovery trial."""


def observe_trial_ready(control, policy, state, rows, claim, receipt, *, deadline):
    sha = state["previous_sha"] if state["phase"] == "ROLLBACK_PENDING_READY" else state["candidate_sha"]
    if state.get("trial_pid") is None and claim and claim.get("nonce") == state["nonce"] and claim.get("transition_id") == state["transition_id"]:
        row = next((row for row in rows if row.get("pid") == claim.get("pid")), None)
        role = claim.get("role", "gui")
        if role in {"gui", "relay"} and row and process_matches_trial(row, state=state, sha=sha, sid=policy["operator_sid"], role=role):
            state = bind_trial(state, sha=sha, pid=row["pid"], ordinary_user=True, now=row["created_at"], role=role)
            write_json_durable(control / "transition.json", state)
    trial = next((row for row in rows if row.get("pid") == state.get("trial_pid")), None)
    relays = [row for row in rows if process_matches_trial(row, state=state, sha=sha, sid=policy["operator_sid"], role="relay")]
    process_ok = bool(trial and process_matches_trial(trial, state=state, sha=sha, sid=policy["operator_sid"], role=state.get("trial_role", "gui")))
    if (time.monotonic() < deadline and receipt and ready_matches(state, receipt,
            process_matches=process_ok, relay_matches=len(relays) == 1, now=now_utc())):
        confirmed = confirm_transition(state, receipt, process_matches=process_ok, relay_matches=True, now=now_utc())
        write_json_durable(control / "transition.json", confirmed)
        record_confirmed_transition(control, confirmed)
        return confirmed, True, False
    return state, False, bool(state.get("trial_pid") and trial is None)


class _RetainedAuditPending(Exception):
    """A durable late confirmation must survive a transient audit-write failure."""


def _confirm_retained_ready_once(control, policy, state):
    """SYSTEM's existing root owner makes one late decision; never waits for READY."""
    active = read_json(control / "active.json")
    if not _retained_confirmation_allowed(state, active):
        return state
    stage = Path(policy["staging_root"])
    with update_lock(control, policy["operator_sid"], trial=True):
        try:
            claim, receipt = optional_record(stage / "trial.json"), optional_record(stage / "ready.json")
            failure = optional_record(stage / "trial-failure.json")
        except (OSError, ValueError, TypeError, KeyError):
            return state
        if not receipt:
            return state
        if failure and failure.get("nonce") == state["nonce"] and failure.get("transition_id") == state["transition_id"]:
            return state
        try:
            rows = writer_processes(process_snapshot(control), own_pid=os.getpid())
            binding = _retained_trial_binding(state, active, claim, rows, policy)
        except (OSError, ValueError, TypeError, KeyError):
            return state
        if binding is None:
            return state
        projected, identity = binding
        relays = [row for row in rows if process_matches_trial(row, state=projected,
            sha=active["sha"], sid=policy["operator_sid"], role="relay")]
        if not ready_matches(projected, receipt, process_matches=True,
                relay_matches=len(relays) == 1, now=now_utc(), active=active):
            return state
        pair = projected.get("rollback_pair")
        if pair is not None:
            try:
                directional_pin([pair], **pair)
                if pair["from_commit"] != projected["candidate_sha"] or pair["to_commit"] != projected["previous_sha"]:
                    return state
            except (KeyError, TypeError, ValueError):
                return state
        current_identity = native_process_identity(claim["pid"])
        if (optional_record(control / "transition.json") != state
                or read_json(control / "active.json") != active
                or optional_record(stage / "trial.json") != claim
                or optional_record(stage / "ready.json") != receipt
                or any(current_identity.get(key) != identity.get(key)
                       for key in ("status", "creation_identity"))):
            return state
        confirmed = confirm_transition(projected, receipt, process_matches=True,
            relay_matches=True, now=now_utc(), active=active)
        write_json_durable(control / "transition.json", confirmed)
        try:
            return record_confirmed_transition(control, confirmed)
        except OSError as error:
            raise _RetainedAuditPending() from error


def wait_for_rollback_ready(control: Path, policy, *, timeout_seconds=READY_WINDOW_SECONDS) -> int:
    """SYSTEM observes the ordinary old-version trial; it never launches it."""
    deadline = time.monotonic() + timeout_seconds
    stage = Path(policy["staging_root"])
    from contextlib import ExitStack, contextmanager

    @contextmanager
    def rollback_lock(deadline):
        with ExitStack() as stack:
            try:
                stack.enter_context(update_lock(control, policy["operator_sid"]))
            except RuntimeError as error:
                if str(error) != "update mutex ownership is busy" or time.monotonic() >= deadline:
                    raise
                acquired = False
            else:
                acquired = True
            yield acquired

    while time.monotonic() < deadline:
        with rollback_lock(deadline) as acquired:
            if not acquired:
                time.sleep(min(0.25, max(0.0, deadline - time.monotonic())))
                continue
            state = read_json(control / "transition.json")
            if (state.get("phase") != "ROLLBACK_PENDING_READY"
                    or read_json(control / "active.json")["sha"] != state["previous_sha"]):
                return 20
            try:
                rows = process_snapshot(control)
                claim = optional_record(stage / "trial.json")
                receipt = optional_record(stage / "ready.json")
                state, confirmed, exited = observe_trial_ready(control, policy, state, rows, claim, receipt, deadline=deadline)
            except OSError:
                confirmed = exited = False
            if confirmed:
                return 0
            if exited:
                break
        time.sleep(min(0.25, max(0.0, deadline - time.monotonic())))
    with update_lock(control, policy["operator_sid"]):
        state = read_json(control / "transition.json")
        if state.get("phase") == "ROLLBACK_PENDING_READY":
            write_json_durable(control / "transition.json", {**state, "phase": "FAILED", "reason": "previous version readiness failed"})
    return 20


def _apply_prepared(control: Path) -> int:
    try:
        return _apply_prepared_once(control)
    except _RollbackReadyPending as pending:
        if pending.args and pending.args[0] is not None:
            return wait_for_rollback_ready(control, load_policy(control),
                timeout_seconds=max(0.0, min(READY_WINDOW_SECONDS, pending.args[0] - time.monotonic())))
        return wait_for_rollback_ready(control, load_policy(control))


def relay_stop_requested(policy: Mapping[str, Any]) -> bool:
    """Read only the protected policy's operator root, including under SYSTEM."""
    from user_relay import user_relay_stop_path

    local = Path(policy["operator_local_app_data"])
    if not local.is_absolute():
        raise ValueError("operator profile is not absolute")
    root = local / "KMTech" / "DirectSync" / "label_match"
    _assert_no_reparse_components(root)
    marker = user_relay_stop_path(root)
    _assert_no_reparse_components(marker)
    try:
        marker.stat()
    except FileNotFoundError:
        return False
    return True


def defer_stopped_relay(control: Path, policy: Mapping[str, Any], state) -> bool:
    if not relay_stop_requested(policy):
        return False
    if not (state and state.get("phase") == "DEFERRED"
            and state.get("reason_code") == UPDATE_RELAY_STOP_REQUESTED
            and state.get("retained_candidate", False) is False
            and "recovery_pending" not in state):
        deferred = {**(state or {}), "schema": SCHEMA,
            "phase": "DEFERRED", "deferred_at": now_utc(),
            "reason_code": UPDATE_RELAY_STOP_REQUESTED, "retained_candidate": False}
        deferred.pop("recovery_pending", None)
        write_json_durable(control / "transition.json", deferred)
    return True


@contextmanager
def _initial_ready_lock(control: Path, operator_sid: str):
    """An unlocked journal supplies only the existing pending window's bound."""
    from contextlib import ExitStack
    deadline = None
    last_busy = None
    while True:
        if deadline is not None and time.monotonic() >= deadline:
            raise last_busy
        with ExitStack() as stack:
            try:
                stack.enter_context(update_lock(control, operator_sid))
            except RuntimeError as error:
                if str(error) != "update mutex ownership is busy":
                    raise
                last_busy = error
                if deadline is None:
                    try:
                        budget = optional_record(control / "transition.json")
                        phase = (budget or {}).get("phase")
                        if phase not in {"APPLIED_PENDING_READY", "ROLLBACK_PENDING_READY"}:
                            raise error
                        anchor = budget["rollback_at" if phase == "ROLLBACK_PENDING_READY" else "applied_at"]
                        sampled_at = time.monotonic()
                        remaining = READY_WINDOW_SECONDS - (_utc(now_utc()) - _utc(anchor)).total_seconds()
                        deadline = sampled_at + max(0.0, min(READY_WINDOW_SECONDS, remaining))
                    except Exception:
                        raise error
                if time.monotonic() >= deadline:
                    raise
            else:
                yield deadline
                return
        time.sleep(min(0.25, max(0.0, deadline - time.monotonic())))


def _apply_prepared_once(control: Path) -> int:

    """SYSTEM applies code only; the operator bootstrap launches the trial."""
    import time
    from Label_Match import _sha256_file as file_sha256

    opened_here = False
    fixed_helper(control, "Authority")
    policy = load_policy(control)
    # SYSTEM must never resolve user credentials or start a user process.
    os.environ["LOCALAPPDATA"] = policy["operator_local_app_data"]
    stage = Path(policy["staging_root"])
    with _initial_ready_lock(control, policy["operator_sid"]) as initial_ready_deadline:
        state = optional_record(control / "transition.json")
        if (state and state.get("phase") == "DEFERRED" and state.get("retained_candidate") is True
                and not state.get("recovery_pending")):
            try:
                state = _confirm_retained_ready_once(control, policy, state)
            except _RetainedAuditPending:
                return 20
        if (state and state.get("phase") == "DEFERRED" and state.get("reason_code") == "UPDATE_PREPARATION_FAILED"
                and not state.get("recovery_pending") and not prepared_pending(control, policy, state)):
            active_sha = read_json(control / "active.json")["sha"]
            if not state.get("retained_candidate") or recovery_sha(state, pointer_sha=active_sha) == active_sha:
                return 0
        write_json_durable(control / "apply-claim.json", new_apply_claim())
        if state and state["phase"] == "CONFIRMED":
            try:
                state = record_confirmed_transition(control, state)
            except OSError:
                if state.get("retained_candidate") is True:
                    return 20  # Retry the existing audit on the next SYSTEM invocation.
                raise
        if state and state.get("recovery_pending") is True:
            if writer_processes(process_snapshot(control), own_pid=os.getpid()):
                return 0  # Pending recovery never admits a new prepared packet.
            state = settle_trial_failure(control, state, policy)
            if state and state.get("phase") == "DEFERRED" and state.get("retained_candidate") is True:
                try:
                    state = _confirm_retained_ready_once(control, policy, state)
                except _RetainedAuditPending:
                    return 20
        if state and state["phase"] in {"APPLIED_PENDING_READY", "UNKNOWN", "FAILED"}:
            rows = process_snapshot(control)
            writers = writer_processes(rows, own_pid=os.getpid())
            claim = optional_record(stage / "trial.json")
            bound = state.get("trial_pid")
            if (not bound and claim and claim.get("nonce") == state.get("nonce")
                    and claim.get("transition_id") == state.get("transition_id")):
                bound = claim.get("pid")
            trial_live = state["phase"] == "APPLIED_PENDING_READY" and any(
                row.get("pid") == bound and process_matches_trial(row, state=state,
                    sha=state["candidate_sha"], sid=policy["operator_sid"],
                    role=state.get("trial_role", (claim or {}).get("role", "gui"))) for row in writers)
            if writers and not trial_live:
                # A later SYSTEM invocation never stops an ordinary active writer
                # or upgrades its unconfirmed trial to CONFIRMED.
                write_json_durable(control / "transition.json", {**state, "phase": "DEFERRED",
                    "retained_candidate": False, "deferred_at": now_utc(), "reason_code": "UPDATE_EXISTING_WRITER",
                    "recovery_pending": True, "recovery_phase": state["phase"],
                    "recovery_reason_code": state.get("reason_code")})
                return 0
        if state and state["phase"] == "ROLLBACK_PENDING_READY":
            raise _RollbackReadyPending(initial_ready_deadline)
        if state and state["phase"] in {"UNKNOWN", "FAILED"}:
            if writer_processes(process_snapshot(control), own_pid=os.getpid()):
                return 20
            state = settle_trial_failure(control, state, policy)
        if state and state.get("retained_candidate") is True and state["phase"] == "DEFERRED":
            selected = recovery_sha(state, pointer_sha=read_json(control / "active.json")["sha"])
            if read_json(control / "active.json")["sha"] != selected:
                fixed_helper(control, "Verify", code_root=version_bundle(control, selected))
                persist_switch(control, state, selected)
                return 20
        if state is None or state["phase"] in {"CONFIRMED", "ROLLED_BACK", "DEFERRED"}:
            if not prepared_pending(control, policy, state):
                return 0
            if defer_stopped_relay(control, policy, state):
                return 0
            if writer_processes(process_snapshot(control), own_pid=os.getpid()):
                deferred = {**(state or {}), "schema": SCHEMA, "phase": "DEFERRED", "deferred_at": now_utc(), "reason": "an existing application writer continues working"}
                write_json_durable(control / "transition.json", deferred)
                return 0  # A late task defers; it never stops an existing application.
            active = read_json(control / "active.json")
            fixed_helper(control, "Verify", code_root=version_bundle(control, active["sha"]))
            current = version_manifest(version_bundle(control, active["sha"]))
            key = read_json(control.parent / "current/app/update-manifest-key-config.json")["manifest_public_key"]
            # Copy user-owned bytes, then authenticate the protected copy. No path,
            # executable or command supplied by the prepared marker is executed.
            protected = control / ("packet-" + uuid.uuid4().hex)
            create_protected_stage(protected)
            for name in ("manifest.json", "manifest.sig", "packet.zip", "prepared.json"):
                copy_prepared_file(stage / name, protected / name, expected_owner_sid=policy["operator_sid"])
            manifest, candidate = verify_prepared_packet(protected, key_config=key, current_version=current["version"], channel=policy["channel"])
            sha = manifest["source_commit"]
            if sha in (state or {}).get("failed_shas", []):
                return 0
            extracted = protected / "extracted"
            extract_prepared_code(protected / "packet.zip", extracted, candidate["archive"])
            target = extracted / "Label_Match"
            candidate_manifest = version_manifest(target)
            if candidate_manifest["source_commit"] != sha or candidate_manifest["version"] != manifest["version"]:
                raise ValueError("signed target SHA differs from the packet")
            if file_sha256(target / "app/update-manifest-key-config.json") != file_sha256(control.parent / "current/app/update-manifest-key-config.json"):
                raise ValueError("candidate signing key differs from the installed key")
            forward, rollback, rollback_pair, pin_authority, data_api_evidence = authorize_feed_transition(manifest, current=current, candidate=candidate_manifest)
            # The fixed helper validates the portable tree and writes its own
            # integrity record. It never imports or executes candidate code.
            fixed_helper(control, "Verify", code_root=target)
            target_bundle = version_bundle(control, sha)
            if target_bundle.exists():
                fixed_helper(control, "Verify", code_root=target_bundle)
            preserve_candidate_version(target, target_bundle)
            generation_before = read_trial_generation(policy)
            if generation_before.get("status") != "READ":
                write_json_durable(control / "transition.json", {**(state or {}), "schema": SCHEMA,
                    "phase": "DEFERRED", "deferred_at": now_utc(), "reason_code": "UPDATE_GENERATION_UNKNOWN"})
                return 0
            state = prepare_transition(previous_sha=active["sha"], candidate_sha=sha, packet_sha256=candidate["sha256"], forward_pin=forward, rollback_pin=rollback, transition_id=uuid.uuid4().hex, nonce=uuid.uuid4().hex, now=now_utc(), failed_shas=(state or {}).get("failed_shas", []), retry_shas=(state or {}).get("retry_shas"))
            state = {**state, "rollback_pair": rollback_pair, "pin_authority": pin_authority, "policy_sha256": file_sha256(control / "policy.json"), "data_api_evidence": data_api_evidence, "prepared_packet_root": str(protected), "generation_before": generation_before, "generation_previous_release": current, "generation_candidate_release": candidate_manifest}
            write_json_durable(control / "transition.json", state)
            if defer_stopped_relay(control, policy, state):
                return 0
            state = apply_transition(state, writers_absent=not writer_processes(process_snapshot(control), own_pid=os.getpid()), packet_verified=True, durable_prepared=True, now=now_utc())
            opened_here = True
            persist_switch(control, state, sha)
        else:
            pointer = read_json(control / "active.json")
            if state["phase"] == "PREPARED":
                if defer_stopped_relay(control, policy, state):
                    return 0
                if writer_processes(process_snapshot(control), own_pid=os.getpid()):
                    return 20
                authorize_prepared_replay(control, policy, state)
                fixed_helper(control, "Verify", code_root=version_bundle(control, state["candidate_sha"]))
                if defer_stopped_relay(control, policy, state):
                    return 0
                state = apply_transition(state, writers_absent=True, packet_verified=True, durable_prepared=True, now=now_utc())
                opened_here = True
                persist_switch(control, state, state["candidate_sha"])
            if state["phase"] == "ROLLBACK_PENDING":
                if writer_processes(process_snapshot(control), own_pid=os.getpid()):
                    return 20
                settled = settle_trial_failure(control, state, policy)
                if settled.get("retained_candidate"):
                    return 20
                raise _RollbackReadyPending
            sha = recovery_sha(state, pointer_sha=pointer["sha"])
            if pointer["sha"] != sha:
                if writer_processes(process_snapshot(control), own_pid=os.getpid()):
                    return 20
                persist_switch(control, state, sha)
    from contextlib import ExitStack, contextmanager

    @contextmanager
    def confirmation_lock(deadline):
        with ExitStack() as stack:
            try:
                stack.enter_context(update_lock(control, policy["operator_sid"]))
            except RuntimeError as error:
                if str(error) != "update mutex ownership is busy" or time.monotonic() >= deadline:
                    raise
                acquired = False
            else:
                acquired = True
            yield acquired

    deadline = time.monotonic() + READY_WINDOW_SECONDS
    if not opened_here and initial_ready_deadline is not None:
        deadline = min(deadline, initial_ready_deadline)
    candidate_ended_early = False
    while time.monotonic() < deadline:
        with confirmation_lock(deadline) as acquired:
            if not acquired:
                time.sleep(0.25)
                continue
            state = read_json(control / "transition.json")
            rows = process_snapshot(control)
            claim = None
            receipt = None
            launch_failure = None
            try:
                claim = optional_record(stage / "trial.json")
                receipt = optional_record(stage / "ready.json")
                launch_failure = optional_record(stage / "trial-failure.json")
            except OSError as error:
                # A contested user proof is unconfirmed; keep the original
                # journal deadline and normal stop/rollback decision alive.
                state = {**state, "last_ready_error": type(error).__name__,
                         "last_ready_error_details": launch_failure_details(error, include_original=True)}
                write_json_durable(control / "transition.json", state)
            sha = state["previous_sha"] if state["phase"] == "ROLLBACK_PENDING_READY" else state["candidate_sha"]
            if (launch_failure and launch_failure.get("nonce") == state["nonce"]
                    and launch_failure.get("transition_id") == state["transition_id"]
                    and launch_failure.get("sha") == sha):
                candidate_ended_early = True
                break  # The ordinary bootstrap stays alive to launch verified recovery.
            state, confirmed, exited = observe_trial_ready(control, policy, state, rows, claim, receipt, deadline=deadline)
            if confirmed:
                return 0
            if exited:
                candidate_ended_early = True
                break
        time.sleep(0.25)
    with update_lock(control, policy["operator_sid"]):
        state = read_json(control / "transition.json")
        if state["phase"] == "ROLLBACK_PENDING_READY":
            write_json_durable(control / "transition.json", {**state, "phase": "FAILED", "reason": "previous version readiness failed"})
            return 20
        if opened_here:
            # A timeout alone may be a slow or busy PC: the candidate gets another
            # chance (bounded). A candidate that already failed to launch or exited stays final.
            state = request_rollback(state, reason="candidate startup failed or readiness timed out",
                                     transient=not candidate_ended_early, now=now_utc())
        else:
            if state["phase"] != "APPLIED_PENDING_READY":
                raise ValueError("rollback requires a pending trial")
            # An inherited window supplies no supervised failure evidence.
            state = {**state, "phase": "UNKNOWN"}
        write_json_durable(control / "transition.json", state)
    deadline = time.monotonic() + 60
    while time.monotonic() < deadline:
        with update_lock(control, policy["operator_sid"]):
            if not writer_processes(process_snapshot(control), own_pid=os.getpid()):
                settled = settle_trial_failure(control, state, policy)
                if settled.get("retained_candidate"):
                    return 20
                raise _RollbackReadyPending
        time.sleep(0.25)
    write_json_durable(control / "transition.json", {**state, "phase": "UNKNOWN", "reason": "candidate normal stop could not be proven"})
    return 20


@writer_sink("auto_update_create_protected_stage")
def create_protected_stage(path: Path) -> None:
    _assert_no_reparse_components(path)
    path.mkdir()


@writer_sink("auto_update_extract_prepared_code")
def extract_prepared_code(packet: Path, target: Path, archive_policy: Mapping[str, Any]) -> None:
    from Label_Match import _safe_extract_update_zip as safe_extract_update_zip
    import zipfile

    # A short SYSTEM control admission delegates only extraction, and releases
    # before the ordinary-user GUI/relay waits for readiness.
    with zipfile.ZipFile(packet) as archive:
        safe_extract_update_zip(archive, target, archive_policy=archive_policy)


@writer_sink("auto_update_copy_prepared_file")
def copy_prepared_file(source: Path, target: Path, *, expected_owner_sid: str | None = None) -> None:
    _copy_prepared_file(source, target, expected_owner_sid=expected_owner_sid)


def _copy_prepared_file(source: Path, target: Path, *, expected_owner_sid: str | None = None) -> None:
    import shutil

    _assert_no_reparse_components(source)
    _assert_no_reparse_components(target)
    with unredirected_source(source, expected_owner_sid=expected_owner_sid) as incoming:
        if os.fstat(incoming.fileno()).st_size > (512 * 1024 * 1024 if source.name == "packet.zip" else 1024 * 1024):
            raise ValueError("prepared input is oversized")
        with target.open("xb") as outgoing:
            shutil.copyfileobj(incoming, outgoing)
            outgoing.flush()
            os.fsync(outgoing.fileno())


@contextmanager
def unredirected_source(source: Path, *, expected_owner_sid: str | None = None):
    """Hold every parent and reject redirected/hardlinked SYSTEM input."""
    _assert_no_reparse_components(source)
    if os.name != "nt":
        with source.open("rb") as incoming:
            if os.fstat(incoming.fileno()).st_nlink != 1:
                raise ValueError("prepared input has multiple hardlinks")
            yield incoming
        return
    import ctypes
    import msvcrt
    from ctypes import wintypes

    class FileInformation(ctypes.Structure):
        _fields_ = [("attributes", wintypes.DWORD), ("created", wintypes.FILETIME), ("accessed", wintypes.FILETIME), ("written", wintypes.FILETIME), ("volume", wintypes.DWORD), ("size_high", wintypes.DWORD), ("size_low", wintypes.DWORD), ("links", wintypes.DWORD), ("index_high", wintypes.DWORD), ("index_low", wintypes.DWORD)]

    kernel = ctypes.WinDLL("kernel32", use_last_error=True)
    kernel.CreateFileW.argtypes = [wintypes.LPCWSTR, wintypes.DWORD, wintypes.DWORD, wintypes.LPVOID, wintypes.DWORD, wintypes.DWORD, wintypes.HANDLE]
    kernel.CreateFileW.restype = wintypes.HANDLE
    kernel.GetFileInformationByHandle.argtypes = [wintypes.HANDLE, ctypes.POINTER(FileInformation)]
    kernel.GetFileInformationByHandle.restype = wintypes.BOOL
    kernel.CloseHandle.argtypes = [wintypes.HANDLE]
    kernel.LocalFree.argtypes = [wintypes.HLOCAL]
    kernel.LocalFree.restype = wintypes.HLOCAL
    handles = []
    try:
        paths = [*reversed(source.absolute().parents), source.absolute()]
        for index, path in enumerate(paths):
            leaf = index == len(paths) - 1
            # Sharing neither write nor delete closes the check/open race for
            # the leaf and every ancestor, including directory reparse edits.
            handle = kernel.CreateFileW(str(path), 0x80000000 if leaf else 0x80, 1, None, 3, 0x00200000 | (0x02000000 if not leaf else 0x08000000), None)
            if handle in (None, ctypes.c_void_p(-1).value):
                raise OSError(ctypes.get_last_error(), "prepared input cannot be held read-only")
            handles.append(handle)
            info = FileInformation()
            if not kernel.GetFileInformationByHandle(handle, ctypes.byref(info)):
                raise OSError(ctypes.get_last_error(), "prepared input handle cannot be observed")
            if info.attributes & 0x400 or bool(info.attributes & 0x10) == leaf:
                raise ValueError("prepared input is redirected or has an unexpected type")
            if leaf and info.links != 1:
                raise ValueError("prepared input has multiple hardlinks")
        if expected_owner_sid is not None:
            advapi = ctypes.WinDLL("advapi32", use_last_error=True)
            advapi.GetSecurityInfo.argtypes = [wintypes.HANDLE, wintypes.DWORD, wintypes.DWORD, ctypes.POINTER(wintypes.LPVOID), ctypes.POINTER(wintypes.LPVOID), ctypes.POINTER(wintypes.LPVOID), ctypes.POINTER(wintypes.LPVOID), ctypes.POINTER(wintypes.LPVOID)]
            advapi.GetSecurityInfo.restype = wintypes.DWORD
            advapi.ConvertSidToStringSidW.argtypes = [wintypes.LPVOID, ctypes.POINTER(wintypes.LPWSTR)]
            owner, descriptor, text = wintypes.LPVOID(), wintypes.LPVOID(), wintypes.LPWSTR()
            try:
                if advapi.GetSecurityInfo(handles[-1], 1, 1, ctypes.byref(owner), None, None, None, ctypes.byref(descriptor)):
                    raise OSError("prepared input owner cannot be observed")
                if not advapi.ConvertSidToStringSidW(owner, ctypes.byref(text)) or text.value != expected_owner_sid:
                    raise ValueError("prepared input is not owned by the installed operator")
            finally:
                if text:
                    kernel.LocalFree(text)
                if descriptor:
                    kernel.LocalFree(descriptor)
        fd = msvcrt.open_osfhandle(int(handles[-1]), os.O_RDONLY | os.O_BINARY | getattr(os, "O_NOINHERIT", 0))
        handles.pop()  # The Python stream owns the leaf handle from here.
        with os.fdopen(fd, "rb") as incoming:
            yield incoming
    finally:
        for handle in reversed(handles):
            kernel.CloseHandle(handle)


@writer_sink("auto_update_preserve_candidate_version")
def preserve_candidate_version(source: Path, target: Path) -> None:
    _assert_no_reparse_components(target)
    if target.exists():
        from Label_Match import _sha256_file as file_sha256

        def inventory(root):
            values = {}
            for path in root.rglob("*"):
                _assert_no_reparse_components(path)
                if path.is_file() and path.name != "bootstrap-integrity.json":
                    values[path.relative_to(root).as_posix()] = file_sha256(path)
            return values
        if inventory(source) != inventory(target):
            raise ValueError("a version folder already has different bytes; preserve it for recovery")
        return  # Reuse only an independently verified, byte-identical version.
    os.rename(source, target)


def current_process_sid() -> str:
    import ctypes
    from ctypes import wintypes

    advapi = ctypes.WinDLL("advapi32", use_last_error=True)
    kernel = ctypes.WinDLL("kernel32", use_last_error=True)
    kernel.GetCurrentProcess.restype = wintypes.HANDLE
    kernel.CloseHandle.argtypes = [wintypes.HANDLE]
    kernel.LocalFree.argtypes = [wintypes.HLOCAL]
    advapi.OpenProcessToken.argtypes = [wintypes.HANDLE, wintypes.DWORD, ctypes.POINTER(wintypes.HANDLE)]
    advapi.GetTokenInformation.argtypes = [wintypes.HANDLE, ctypes.c_int, wintypes.LPVOID, wintypes.DWORD, ctypes.POINTER(wintypes.DWORD)]
    advapi.ConvertSidToStringSidW.argtypes = [wintypes.LPVOID, ctypes.POINTER(wintypes.LPWSTR)]
    token, needed, sid_text = wintypes.HANDLE(), wintypes.DWORD(), wintypes.LPWSTR()
    if not advapi.OpenProcessToken(kernel.GetCurrentProcess(), 8, ctypes.byref(token)):
        raise OSError("update operator token cannot be observed")
    try:
        advapi.GetTokenInformation(token, 1, None, 0, ctypes.byref(needed))
        if not 0 < needed.value < 65536:
            raise ValueError("update operator token is malformed")
        buffer = ctypes.create_string_buffer(needed.value)
        if not advapi.GetTokenInformation(token, 1, buffer, needed.value, ctypes.byref(needed)):
            raise OSError("update operator SID cannot be observed")
        sid = ctypes.cast(buffer, ctypes.POINTER(wintypes.LPVOID)).contents
        if not advapi.ConvertSidToStringSidW(sid, ctypes.byref(sid_text)):
            raise OSError("update operator SID cannot be formatted")
        return sid_text.value
    finally:
        if sid_text:
            kernel.LocalFree(sid_text)
        kernel.CloseHandle(token)


def _inspect_current_process_execution_context() -> dict[str, Any]:
    if os.name != "nt":
        raise ValueError("replacement lifecycle token inspection requires Windows")
    import ctypes
    from ctypes import wintypes

    token_query = 0x0008
    token_elevation = 20
    token_integrity_level = 25
    error_insufficient_buffer = 122
    process_token = wintypes.HANDLE()
    advapi32 = ctypes.WinDLL("advapi32", use_last_error=True)
    kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
    kernel32.GetCurrentProcess.restype = wintypes.HANDLE
    kernel32.CloseHandle.argtypes = [wintypes.HANDLE]
    kernel32.CloseHandle.restype = wintypes.BOOL
    advapi32.OpenProcessToken.argtypes = [
        wintypes.HANDLE,
        wintypes.DWORD,
        ctypes.POINTER(wintypes.HANDLE),
    ]
    advapi32.OpenProcessToken.restype = wintypes.BOOL
    advapi32.GetTokenInformation.argtypes = [
        wintypes.HANDLE,
        ctypes.c_int,
        wintypes.LPVOID,
        wintypes.DWORD,
        ctypes.POINTER(wintypes.DWORD),
    ]
    advapi32.GetTokenInformation.restype = wintypes.BOOL
    advapi32.GetSidSubAuthorityCount.argtypes = [wintypes.LPVOID]
    advapi32.GetSidSubAuthorityCount.restype = ctypes.POINTER(ctypes.c_ubyte)
    advapi32.GetSidSubAuthority.argtypes = [wintypes.LPVOID, wintypes.DWORD]
    advapi32.GetSidSubAuthority.restype = ctypes.POINTER(wintypes.DWORD)
    if not advapi32.OpenProcessToken(
        kernel32.GetCurrentProcess(), token_query, ctypes.byref(process_token)
    ):
        raise OSError(ctypes.get_last_error(), "OpenProcessToken failed")
    try:
        elevated = wintypes.DWORD()
        returned = wintypes.DWORD()
        if not advapi32.GetTokenInformation(
            process_token,
            token_elevation,
            ctypes.byref(elevated),
            ctypes.sizeof(elevated),
            ctypes.byref(returned),
        ):
            raise OSError(ctypes.get_last_error(), "TokenElevation readback failed")

        required = wintypes.DWORD()
        advapi32.GetTokenInformation(
            process_token,
            token_integrity_level,
            None,
            0,
            ctypes.byref(required),
        )
        if ctypes.get_last_error() != error_insufficient_buffer or required.value <= 0:
            raise OSError(
                ctypes.get_last_error(), "TokenIntegrityLevel sizing failed"
            )
        buffer = ctypes.create_string_buffer(required.value)
        if not advapi32.GetTokenInformation(
            process_token,
            token_integrity_level,
            buffer,
            required.value,
            ctypes.byref(required),
        ):
            raise OSError(
                ctypes.get_last_error(), "TokenIntegrityLevel readback failed"
            )

        class SidAndAttributes(ctypes.Structure):
            _fields_ = [("sid", wintypes.LPVOID), ("attributes", wintypes.DWORD)]

        sid = ctypes.cast(buffer, ctypes.POINTER(SidAndAttributes)).contents.sid
        count_pointer = advapi32.GetSidSubAuthorityCount(sid)
        if not count_pointer:
            raise OSError(ctypes.get_last_error(), "integrity SID count failed")
        count = count_pointer.contents.value
        if count <= 0:
            raise ValueError("integrity SID contains no subauthority")
        rid_pointer = advapi32.GetSidSubAuthority(sid, count - 1)
        if not rid_pointer:
            raise OSError(ctypes.get_last_error(), "integrity SID RID failed")
        rid = rid_pointer.contents.value
    finally:
        kernel32.CloseHandle(process_token)

    if 0x2000 <= rid < 0x3000:
        integrity = "MEDIUM"
    elif rid < 0x2000:
        integrity = "LOW"
    else:
        integrity = "HIGH"
    return {
        "status": "PASS",
        "token_elevated": bool(elevated.value),
        "integrity_level": integrity,
    }

def process_security(process_id: int) -> dict[str, Any]:
    """Read a process token without reading credentials or a profile store."""
    import ctypes
    from ctypes import wintypes

    kernel = ctypes.WinDLL("kernel32", use_last_error=True)
    advapi = ctypes.WinDLL("advapi32", use_last_error=True)
    kernel.OpenProcess.argtypes = [wintypes.DWORD, wintypes.BOOL, wintypes.DWORD]
    kernel.OpenProcess.restype = wintypes.HANDLE
    kernel.CloseHandle.argtypes = [wintypes.HANDLE]
    kernel.LocalFree.argtypes = [wintypes.HLOCAL]
    advapi.OpenProcessToken.argtypes = [wintypes.HANDLE, wintypes.DWORD, ctypes.POINTER(wintypes.HANDLE)]
    advapi.GetTokenInformation.argtypes = [wintypes.HANDLE, ctypes.c_int, wintypes.LPVOID, wintypes.DWORD, ctypes.POINTER(wintypes.DWORD)]
    advapi.ConvertSidToStringSidW.argtypes = [wintypes.LPVOID, ctypes.POINTER(wintypes.LPWSTR)]
    advapi.GetSidSubAuthorityCount.argtypes = [wintypes.LPVOID]
    advapi.GetSidSubAuthorityCount.restype = ctypes.POINTER(ctypes.c_ubyte)
    advapi.GetSidSubAuthority.argtypes = [wintypes.LPVOID, wintypes.DWORD]
    advapi.GetSidSubAuthority.restype = ctypes.POINTER(wintypes.DWORD)
    process = kernel.OpenProcess(0x1000, False, process_id)
    token, sid_text = wintypes.HANDLE(), wintypes.LPWSTR()
    if not process:
        raise OSError(ctypes.get_last_error(), "OpenProcess")
    try:
        if not advapi.OpenProcessToken(process, 8, ctypes.byref(token)):
            raise OSError(ctypes.get_last_error(), "OpenProcessToken")
        def information(kind):
            needed = wintypes.DWORD()
            advapi.GetTokenInformation(token, kind, None, 0, ctypes.byref(needed))
            if not 0 < needed.value <= 65536:
                raise ValueError("process token information sizing differs")
            value = ctypes.create_string_buffer(needed.value)
            if not advapi.GetTokenInformation(token, kind, value, needed, ctypes.byref(needed)):
                raise OSError(ctypes.get_last_error(), "GetTokenInformation:" + str(kind))
            return value
        user = information(1)
        sid = ctypes.cast(user, ctypes.POINTER(wintypes.LPVOID)).contents
        if not advapi.ConvertSidToStringSidW(sid, ctypes.byref(sid_text)):
            raise OSError(ctypes.get_last_error(), "ConvertSidToStringSidW")
        integrity = information(25)
        integrity_sid = ctypes.cast(integrity, ctypes.POINTER(wintypes.LPVOID)).contents
        count = advapi.GetSidSubAuthorityCount(integrity_sid)
        if not count or not count.contents.value:
            raise ValueError("process integrity SID differs")
        rid = advapi.GetSidSubAuthority(integrity_sid, count.contents.value - 1).contents.value
        return {"sid": sid_text.value,
                "session_id": ctypes.cast(information(12), ctypes.POINTER(wintypes.DWORD)).contents.value,
                "elevated": bool(ctypes.cast(information(20), ctypes.POINTER(wintypes.DWORD)).contents.value),
                "integrity_rid": rid}
    finally:
        if sid_text:
            kernel.LocalFree(sid_text)
        if token:
            kernel.CloseHandle(token)
        kernel.CloseHandle(process)


def safe_update_text(value: Any, *, limit: int = 300) -> str:
    """Only source-declared, input-free guidance may reach a public notice."""
    from failure_redaction import RULES
    text = str(value or "")
    return text if text in RULES["safe_messages"] else RULES["safe_messages"][0]


def child_failure_output(completed: Any) -> dict[str, Any]:
    from failure_redaction import redact_for_sink
    return {"returncode": completed.returncode,
            "stdout": redact_for_sink(getattr(completed, "stdout", "") or ""),
            "stderr": redact_for_sink(getattr(completed, "stderr", "") or "")}


def launch_failure_details(error: BaseException, *, include_original: bool = False) -> list[dict[str, Any]]:
    """Preserve each original cause and native call without emitting authority."""
    chain, seen = [], set()
    calls = {"GetShellWindow", "OpenProcess", "OpenProcess:shell", "OpenProcessToken", "OpenProcessToken:shell",
             "DuplicateTokenEx:primary", "CreateEnvironmentBlock", "CreateProcessWithTokenW", "GetExitCodeProcess",
             "ConvertSidToStringSidW"}
    while error is not None and id(error) not in seen and len(chain) < 16:
        seen.add(id(error))
        call = getattr(error, "call", "") or (error.strerror if isinstance(error, OSError) else "")
        if not isinstance(call, str) or (call not in calls and not re.fullmatch(r"GetTokenInformation:[0-9]+", call)):
            call = ""
        code = (getattr(error, "winerror", None) or error.errno) if isinstance(error, OSError) else None
        detail = {"type": type(error).__module__ + "." + type(error).__name__, "call": call,
                  "win32_error": code if type(code) is int else None}
        if include_original:
            from failure_redaction import try_write_failure_evidence
            import traceback
            detail["message"] = safe_update_text(str(error))
            detail["full_exception_trace"] = try_write_failure_evidence(
                "".join(traceback.TracebackException.from_exception(error).format()))
        output = getattr(error, "child_output", None)
        if include_original and isinstance(output, Mapping):
            detail["child_output"] = {"returncode": output.get("returncode"),
                                     "stdout_evidence": try_write_failure_evidence(output.get("stdout", "")),
                                     "stderr_evidence": try_write_failure_evidence(output.get("stderr", ""))}
        elif include_original and (getattr(error, "stdout", None) or getattr(error, "stderr", None)):
            detail["child_output"] = {"returncode": getattr(error, "returncode", None),
                                     "stdout_evidence": try_write_failure_evidence(getattr(error, "stdout", "")),
                                     "stderr_evidence": try_write_failure_evidence(getattr(error, "stderr", ""))}
        chain.append(detail)
        error = error.__cause__ or error.__context__
    from failure_redaction import public_diagnostic_tree
    return public_diagnostic_tree(chain)


def start_ordinary_process(command: list[str], *, cwd: str, environment: Mapping[str, str] | None = None):
    """All user relay starts use the caller's verified interactive shell token."""
    import subprocess

    caller = process_security(os.getpid())
    if caller["session_id"] <= 0:
        raise ValueError("user relay requires an interactive session")
    if caller["elevated"]:
        if environment is not None:
            raise ValueError("shell launch requires its fresh native environment")
        process = _start_with_shell_token(command, cwd=cwd, expected=caller)
    elif caller["integrity_rid"] == 8192:
        process = subprocess.Popen(command, cwd=cwd, env=environment, close_fds=True,
                                   creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0),
                                   stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL,
                                   stderr=subprocess.DEVNULL)
    else:
        raise ValueError("user relay caller integrity differs")
    child = process_security(process.pid)
    if (child["sid"] != caller["sid"] or child["session_id"] != caller["session_id"]
            or child["elevated"] or child["integrity_rid"] != 8192):
        raise ValueError("user relay normal token readback differs")
    return process


def _start_with_shell_token(command: list[str], *, cwd: str, expected: Mapping[str, Any]):
    import ctypes
    import subprocess
    from ctypes import wintypes

    class Startup(ctypes.Structure):
        _fields_ = [("cb", wintypes.DWORD), ("reserved", wintypes.LPWSTR),
                    ("desktop", wintypes.LPWSTR), ("title", wintypes.LPWSTR),
                    *[(name, wintypes.DWORD) for name in ("x", "y", "cx", "cy", "xchars", "ychars", "fill", "flags")],
                    ("show", wintypes.WORD), ("reserved_bytes", wintypes.WORD),
                    ("reserved2", wintypes.LPVOID), ("input", wintypes.HANDLE),
                    ("output", wintypes.HANDLE), ("error", wintypes.HANDLE)]
    class ProcessInfo(ctypes.Structure):
        _fields_ = [("process", wintypes.HANDLE), ("thread", wintypes.HANDLE),
                    ("pid", wintypes.DWORD), ("tid", wintypes.DWORD)]
    kernel = ctypes.WinDLL("kernel32", use_last_error=True)
    user = ctypes.WinDLL("user32", use_last_error=True)
    advapi = ctypes.WinDLL("advapi32", use_last_error=True)
    environment_api = ctypes.WinDLL("userenv", use_last_error=True)
    user.GetShellWindow.restype = wintypes.HWND
    user.GetWindowThreadProcessId.argtypes = [wintypes.HWND, ctypes.POINTER(wintypes.DWORD)]
    kernel.OpenProcess.argtypes = [wintypes.DWORD, wintypes.BOOL, wintypes.DWORD]
    kernel.OpenProcess.restype = wintypes.HANDLE
    kernel.CloseHandle.argtypes = [wintypes.HANDLE]
    kernel.GetExitCodeProcess.argtypes = [wintypes.HANDLE, ctypes.POINTER(wintypes.DWORD)]
    advapi.OpenProcessToken.argtypes = [wintypes.HANDLE, wintypes.DWORD, ctypes.POINTER(wintypes.HANDLE)]
    advapi.DuplicateTokenEx.argtypes = [wintypes.HANDLE, wintypes.DWORD, wintypes.LPVOID, ctypes.c_int, ctypes.c_int, ctypes.POINTER(wintypes.HANDLE)]
    advapi.CreateProcessWithTokenW.argtypes = [wintypes.HANDLE, wintypes.DWORD, wintypes.LPCWSTR, wintypes.LPWSTR, wintypes.DWORD, wintypes.LPVOID, wintypes.LPCWSTR, ctypes.POINTER(Startup), ctypes.POINTER(ProcessInfo)]
    environment_api.CreateEnvironmentBlock.argtypes = [ctypes.POINTER(wintypes.LPVOID), wintypes.HANDLE, wintypes.BOOL]
    environment_api.DestroyEnvironmentBlock.argtypes = [wintypes.LPVOID]
    def checked(success, call):
        if not success:
            error = OSError(ctypes.get_last_error(), call)
            error.call = call
            raise error
    shell_pid = wintypes.DWORD()
    checked(user.GetWindowThreadProcessId(user.GetShellWindow(), ctypes.byref(shell_pid)), "GetShellWindow")
    shell = process_security(shell_pid.value)
    if (shell["sid"] != expected["sid"] or shell["session_id"] != expected["session_id"]
            or shell["elevated"] or shell["integrity_rid"] != 8192):
        raise ValueError("interactive shell security differs")
    shell_process = kernel.OpenProcess(0x1000, False, shell_pid.value)
    checked(shell_process, "OpenProcess:shell")
    token, primary, environment = wintypes.HANDLE(), wintypes.HANDLE(), wintypes.LPVOID()
    try:
        checked(advapi.OpenProcessToken(shell_process, 10, ctypes.byref(token)), "OpenProcessToken:shell")
        checked(advapi.DuplicateTokenEx(token, 0x18B, None, 2, 1, ctypes.byref(primary)), "DuplicateTokenEx:primary")
        checked(environment_api.CreateEnvironmentBlock(ctypes.byref(environment), primary, False), "CreateEnvironmentBlock")
        startup = Startup(); startup.cb = ctypes.sizeof(Startup); startup.desktop = "winsta0\\default"
        child = ProcessInfo()
        checked(advapi.CreateProcessWithTokenW(primary, 0, command[0],
                ctypes.create_unicode_buffer(subprocess.list2cmdline(command)), 0x08000400,
                environment, cwd, ctypes.byref(startup), ctypes.byref(child)), "CreateProcessWithTokenW")
        kernel.CloseHandle(child.thread)
        class Process:
            pid = child.pid
            def poll(self):
                exit_code = wintypes.DWORD()
                checked(kernel.GetExitCodeProcess(child.process, ctypes.byref(exit_code)), "GetExitCodeProcess")
                return None if exit_code.value == 259 else exit_code.value
            def __del__(self):
                kernel.CloseHandle(child.process)
        return Process()
    finally:
        if environment:
            environment_api.DestroyEnvironmentBlock(environment)
        if primary:
            kernel.CloseHandle(primary)
        if token:
            kernel.CloseHandle(token)
        kernel.CloseHandle(shell_process)


def ordinary_user_context(operator_sid: str | None = None) -> bool:
    context = _inspect_current_process_execution_context()
    return context.get("status") == "PASS" and context.get("token_elevated") is False and context.get("integrity_level") == "MEDIUM" and (operator_sid is None or current_process_sid() == operator_sid)


def runtime_matches(app_root: Path) -> bool:
    executable = os.path.normcase(os.path.abspath(sys.executable))
    return executable in {os.path.normcase(str(app_root.parent / "runtime" / name)) for name in ("python.exe", "pythonw.exe")}


def assert_version_writer(app_root: Path, *, source: str, environ: Mapping[str, str]) -> None:
    """Fence old tools too; deployment delegation is checked by the caller."""
    if source.startswith("auto_update_") or source == "current_user_setup_removal":
        return
    control = control_root(app_root)
    if control is None:
        return
    if source == "current_user_onboarding":
        resolve_lifecycle_context(control.parent, operation="register")
        return
    if not runtime_matches(app_root):
        raise ValueError("writer must use its protected packet runtime")
    policy = load_policy(control)
    if not ordinary_user_context(policy["operator_sid"]):
        raise ValueError("writer is not the installed ordinary operator")
    state = optional_record(control / "transition.json")
    active = read_json(control / "active.json")
    own_sha = _sha(read_json(app_root.parent / "portable-manifest.json").get("source_commit"))
    if own_sha != active["sha"]:
        raise ValueError("writer code is not the active version")
    if source == "gui_force_exit_checkpoint" and state and (
            state.get("phase") not in {"CONFIRMED", "ROLLED_BACK", "DEFERRED"}
            or state.get("recovery_pending") is True):
        raise ValueError("bounded force exit requires settled update state")
    deferred_at = environ.get("KMTECH_LM_UPDATE_DEFERRED_AT")
    if state and state["phase"] in {"PREPARED", "FAILED", "UNKNOWN", "APPLIED_PENDING_READY", "ROLLBACK_PENDING_READY", "ROLLBACK_PENDING"} and deferred_at:
        # The caller already holds writer admission. Do not invert the lock order
        # update_lock -> writer mutex: only read the same ordinary-start proof.
        assert_ordinary_recovery(control, policy, own_sha, deferred_at)
        return
    if state and state["phase"] not in {"CONFIRMED", "ROLLED_BACK", "DEFERRED"}:
        cleanup_sources = {'gui_settings_save', 'persistent_relay_status', 'scanner_audit_outbox_save', 'scanner_audit_segment_write', 'gui_current_state_save'}
        if (state["phase"] in {"ROLLBACK_PENDING", "UNKNOWN", "FAILED"} and source in cleanup_sources
                and environ.get("KMTECH_LM_UPDATE_TRIAL_NONCE") == state.get("nonce")
                and (source == "persistent_relay_status" or state.get("trial_pid") == os.getpid())):
            return  # Only the bound trial's ordinary flush/close may finish.
        if state["phase"] not in {"APPLIED_PENDING_READY", "ROLLBACK_PENDING_READY"} or environ.get("KMTECH_LM_UPDATE_TRIAL_NONCE") != state.get("nonce"):
            raise ValueError("writer denied by the startup transition")


def trial_context(app_root: Path) -> tuple[Path, dict[str, Any], dict[str, Any]] | None:
    control = control_root(app_root)
    if control is None:
        return None
    state = optional_record(control / "transition.json")
    retained = False
    if state and state.get("phase") == "DEFERRED" and state.get("retained_candidate") is True and _retained_confirmation_state(state):
        try:
            retained = _retained_confirmation_allowed(state, read_json(control / "active.json"))
        except (OSError, ValueError, TypeError, KeyError):
            return None
    if not state or (not retained and state.get("phase") not in {"APPLIED_PENDING_READY", "ROLLBACK_PENDING_READY", "ROLLBACK_PENDING", "UNKNOWN", "FAILED"}):
        return None
    own_sha = version_manifest(app_root.parent)["source_commit"]
    expected = state.get("trial_sha") or (state["previous_sha"] if state["phase"] == "ROLLBACK_PENDING_READY" or state.get("rollback_at") else state["candidate_sha"])
    if own_sha != expected or os.environ.get("KMTECH_LM_UPDATE_TRIAL_NONCE") != state.get("nonce"):
        return None
    if retained:
        # As in a newly launched pending trial, metadata may precede the claim.
        # Only SYSTEM's fully bound receipt can grant late confirmation.
        state = {**state, "trial_pid": None}
        claim = optional_record(Path(read_json(control / "policy.json")["staging_root"]) / "trial.json")
        if (claim and claim.get("pid") == os.getpid() and claim.get("nonce") == state["nonce"]
                and claim.get("transition_id") == state["transition_id"]):
            identity = native_process_identity(os.getpid())
            if identity.get("status") == "LIVE":
                state.update(trial_pid=os.getpid(), trial_role=claim.get("role", "gui"),
                    trial_started_at=_retained_native_created_at(identity).isoformat())
    return control, read_json(control / "policy.json"), state


def prepared_pending(control: Path, policy: Mapping[str, Any], state: Mapping[str, Any] | None) -> bool:
    if not policy.get("feed_url"):
        return False
    path = Path(policy["staging_root"]) / "prepared.json"
    if (state and state.get("phase") == "DEFERRED" and state.get("failed_prepared_sha256")
            and state["failed_prepared_sha256"] == prepared_marker_sha256(policy)):
        return False
    try:
        marker = optional_record(path)
    except (OSError, ValueError):
        return path.exists()  # A corrupt hint still requests SYSTEM verification.
    if marker is None:
        return False
    try:
        sha = _sha(marker.get("source_commit"))
    except ValueError:
        return True
    active = read_json(control / "active.json")
    return sha != active["sha"] and sha not in (state or {}).get("failed_shas", [])


def prepare_feed_packet(app_root: Path) -> dict[str, Any]:
    """The ordinary relay downloads; only completed authenticated bytes are marked."""
    from urllib.parse import urlsplit, urlunsplit
    import requests
    from Label_Match import _verify_update_manifest_signature as verify_update_manifest_signature, _update_candidate_from_manifest as update_candidate_from_private_manifest, _sha256_file as file_sha256
    from current_user_onboarding import resolve_current_user_onboarding_paths, _default_profile_loader
    from label_data_generation import check_user_data_open

    control = control_root(app_root)
    if control is None:
        return {"status": "OFF"}
    policy = load_policy(control)
    if not policy.get("feed_url"):
        return {"status": "OFF"}
    key = read_json(control.parent / "current/app/update-manifest-key-config.json")["manifest_public_key"]
    active = read_json(control / "active.json")
    fixed_helper(control, "Verify", code_root=version_bundle(control, active["sha"]))
    current = version_manifest(version_bundle(control, active["sha"]))
    profile = None
    if policy.get("profile_auth"):
        profile_path = resolve_current_user_onboarding_paths(app_root.parent).logistics_profile_path
        check_user_data_open(app_root, (profile_path.parent,))
        profile = _default_profile_loader(profile_path)
    feed = urlsplit(policy["feed_url"])
    origin = lambda url: (url.scheme.lower(), (url.hostname or "").lower(), url.port or 443)
    if profile is not None and origin(feed) != origin(urlsplit(profile.base_url)):
        raise ValueError("profile authentication requires the configured authority origin")
    def fetch(session, url):
        assert_https_update_url(url)
        if profile is not None and profile.tls_ca_bundle_path:
            check_user_data_open(app_root, (Path(profile.tls_ca_bundle_path).parent,))
        session.cookies.clear()
        headers = {"Authorization": "Bearer " + profile.bearer_token} if profile is not None and origin(urlsplit(url)) == origin(feed) else {}
        headers["User-Agent"] = "KMTech-LabelMatch/1.0"
        response = session.get(url, headers=headers, timeout=10, stream=True, allow_redirects=False, verify=(profile.tls_ca_bundle_path or True) if profile is not None else True)
        if response.status_code != 200:
            response.close()
            reason = "회사 서버 오류" if response.status_code >= 500 else "회사 서버가 업데이트 요청을 거절했습니다"
            raise ValueError(f"{reason}(코드 {response.status_code}).")
        if response.url != url:
            response.close()
            raise ValueError("update download response URL differs")
        return response
    def bounded(response, maximum):
        with response:
            chunks = []
            count = 0
            for chunk in response.iter_content(65536):
                count += len(chunk)
                if count > maximum:
                    raise ValueError("update metadata is oversized")
                chunks.append(chunk)
            return b"".join(chunks)
    with requests.Session() as session:
        session.trust_env = False  # No ambient proxy/netrc credentials.
        manifest_bytes = bounded(fetch(session, policy["feed_url"]), 1024 * 1024)
        manifest = json.loads(manifest_bytes, object_pairs_hook=unique_object)
        signature_url = urlunsplit((feed.scheme, feed.netloc, feed.path + ".sig", feed.query, ""))
        signature = bounded(fetch(session, signature_url), 16384)
        verify_update_manifest_signature(manifest, signature, key)
        candidate = update_candidate_from_private_manifest(manifest, current_version=current["version"], expected_channel=policy["channel"])
        if candidate is None:
            return {"status": "NO_UPDATE"}
        if candidate["archive"].get("layout") != "canonical_portable":
            raise ValueError("daily updates require the canonical portable layout")
        sha = _sha(manifest.get("source_commit"))
        state = optional_record(control / "transition.json")
        if sha in (state or {}).get("failed_shas", []):
            return {"status": "FAILED_VERSION_PRESERVED", "source_commit": sha}
        stage = Path(policy["staging_root"])
        existing = optional_record(stage / "prepared.json")
        if existing and existing.get("source_commit") == sha and existing.get("packet_sha256") == candidate["sha256"]:
            return {"status": "PREPARED", "source_commit": sha}
        def download(attempt, temps):
            write_download_bytes(attempt / "manifest.json", manifest_bytes)
            write_download_bytes(attempt / "manifest.sig", signature)
            write_download_bytes(attempt / "packet.zip", b"")
            count = 0
            with fetch(session, candidate["url"]) as response:
                for chunk in response.iter_content(256 * 1024):
                    count += len(chunk)
                    if count > min(manifest["artifact"]["size_bytes"], 512 * 1024 * 1024):
                        raise ValueError("update packet is oversized")
                    write_download_chunk(attempt / "packet.zip", chunk)
            if count != manifest["artifact"]["size_bytes"] or file_sha256(attempt / "packet.zip") != candidate["sha256"]:
                raise ValueError("downloaded packet size or SHA256 differs")
            from Label_Match import _validate_update_archive_contract
            import zipfile
            with zipfile.ZipFile(attempt / "packet.zip") as archive:
                _validate_update_archive_contract(archive, archive_policy=candidate["archive"])
            authorize_feed_transition(manifest, current=current,
                                      candidate=packet_version_identity(attempt / "packet.zip"))
            # Final fixed-name copies are replaced only after every byte is verified.
            temps.update(stage_prepared_copies(attempt, stage))
            publish_download(stage, temps)
            write_json_durable(stage / "prepared.json", {"source_commit": sha, "packet_sha256": candidate["sha256"], "prepared_at": now_utc()})
            return {"status": "PREPARED", "source_commit": sha}
        return _run_download_attempt(stage, download)


@writer_sink("auto_update_create_download_stage")
def create_download_stage(path: Path) -> None:
    _assert_no_reparse_components(path)
    path.mkdir(parents=True, exist_ok=True)


@writer_sink("auto_update_write_download_bytes", probe_only=True)
def write_download_bytes(path: Path, data: bytes) -> None:
    _assert_no_reparse_components(path)
    with path.open("xb") as output:
        output.write(data)
        output.flush()
        os.fsync(output.fileno())


@writer_sink("auto_update_write_download_chunk", probe_only=True)
def write_download_chunk(path: Path, data: bytes) -> None:
    _assert_no_reparse_components(path)
    with path.open("ab") as output:
        output.write(data)
        output.flush()
        os.fsync(output.fileno())


@writer_sink("auto_update_stage_prepared_copies", probe_only=True)
def stage_prepared_copies(attempt: Path, stage: Path) -> dict[str, Path]:
    temps = {name: stage / (name + "." + uuid.uuid4().hex + ".tmp")
             for name in ("manifest.json", "manifest.sig", "packet.zip")}
    for temporary in temps.values():
        _assert_no_reparse_components(temporary)
        if temporary.exists():
            raise FileExistsError("prepared temporary already exists")
    owned: dict[str, Path] = {}
    try:
        for name, temporary in temps.items():
            owned[name] = temporary
            try:
                _copy_prepared_file(attempt / name, temporary)
            except FileExistsError:
                # An exclusive-create collision is not owned by this attempt.
                owned.pop(name)
                raise
    except BaseException as failure:
        try:
            _discard_prepared_copies(stage, owned)
        except Exception as cleanup_error:
            failure.add_note("UPDATE_ATTEMPT_CLEANUP_FAILED: " + type(cleanup_error).__name__)
        raise
    return temps


@writer_sink("auto_update_publish_download")
def publish_download(stage: Path, temps: dict[str, Path]) -> None:
    for name in ("manifest.json", "manifest.sig", "packet.zip"):
        atomic_replace(temps[name], stage / name)


def _discard_prepared_copies(stage: Path, temps: Mapping[str, Path]) -> None:
    _assert_no_reparse_components(stage)
    for name, temporary in temps.items():
        if (name not in ("manifest.json", "manifest.sig", "packet.zip")
                or temporary.parent.absolute() != stage.absolute()
                or re.fullmatch(re.escape(name) + r"\.[0-9a-f]{32}\.tmp", temporary.name) is None):
            raise ValueError("prepared temporary path differs from this attempt")
        _assert_no_reparse_components(temporary)
    for temporary in temps.values():
        temporary.unlink(missing_ok=True)


@writer_sink("auto_update_run_download_attempt", probe_only=True)
def _run_download_attempt(stage: Path, run) -> dict[str, Any]:
    # Own the attempt and its finally under this initial probe. A fresh cleanup
    # probe would reject a fence raised during the off-admission body and leave
    # the downloaded packet behind. Only these owned temporary names are removed.
    create_download_stage(stage)
    attempt = stage / ("download-" + uuid.uuid4().hex)
    temps: dict[str, Path] = {}
    created = False
    failure = None
    try:
        create_protected_stage(attempt)
        created = True
        return run(attempt, temps)
    except BaseException as error:
        failure = error
        raise
    finally:
        try:
            if (attempt.parent.absolute() != stage.absolute()
                    or re.fullmatch(r"download-[0-9a-f]{32}", attempt.name) is None):
                raise ValueError("download attempt path differs")
            _assert_no_reparse_components(attempt)
            entries = list(attempt.iterdir()) if created else []
            for entry in entries:
                if entry.name not in ("manifest.json", "manifest.sig", "packet.zip"):
                    raise ValueError("download attempt contains an unexpected entry")
                _assert_no_reparse_components(entry)
                if not entry.is_file():
                    raise ValueError("download attempt entry is not a file")
            _discard_prepared_copies(stage, temps)
            for entry in entries:
                entry.unlink()
            if created:
                attempt.rmdir()
        except Exception as cleanup_error:
            if failure is None:
                raise
            failure.add_note("UPDATE_ATTEMPT_CLEANUP_FAILED: " + type(cleanup_error).__name__)


def trial_stop_requested(app_root: Path) -> bool:
    context = trial_context(app_root)
    return bool(context and context[2]["phase"] in {"ROLLBACK_PENDING", "UNKNOWN", "FAILED"})


def relay_ready_metadata(app_root: Path) -> dict[str, Any]:
    context = trial_context(app_root)
    if context is None:
        if control_root(app_root) is None:
            return {}
        # Publish this process's identity, never the desired policy identity.
        # Older/missing metadata still requires the ordinary Observe path.
        try:
            return {"source_commit": read_json(app_root.parent / "portable-manifest.json")["source_commit"],
                    "sid": current_process_sid(), "ordinary_user": ordinary_user_context()}
        except (OSError, ValueError, KeyError):
            return {}  # Optional cache metadata cannot add a first-start refusal.
    _, _, state = context
    metadata = {"process_id": os.getpid(), "source_commit": version_manifest(app_root.parent)["source_commit"], "update_nonce": state["nonce"], "ordinary_user": ordinary_user_context()}
    try:
        metadata["sid"] = current_process_sid()
    except (OSError, ValueError):
        pass  # Retain the existing nonce/readiness proof and observed fallback.
    return metadata


def _trial_ready_receipt(app_root: Path, *, role: str, pid: int, context=None) -> tuple[Path, dict[str, Any]] | bool:
    """User-only readback; SYSTEM sees booleans, never identities or credentials."""
    import sqlite3
    from current_user_onboarding import resolve_current_user_onboarding_paths, inspect_current_user_state
    from user_relay import user_relay_status_path

    context = trial_context(app_root) if context is None else context
    if context is None:
        return True
    control, policy, state = context
    if (role not in {"gui", "relay"} or type(pid) is not int or pid < 1
            or (state.get("trial_pid") is not None and (
                state["trial_pid"] != pid or state.get("trial_role", "gui") != role))):
        return False
    paths = resolve_current_user_onboarding_paths(app_root.parent)
    if not ordinary_user_context() or inspect_current_user_state(paths).get("status") != "READY":
        return False
    # Read both existing durable queues without creating or migrating them.
    for database in (paths.ledger_path, paths.direct_sync_root / "queue/direct_sync_relay.sqlite3"):
        _assert_no_reparse_components(database)
        if not database.is_file():
            return False
        try:
            with sqlite3.connect(database.as_uri() + "?mode=ro", uri=True) as connection:
                if connection.execute("PRAGMA quick_check").fetchall() != [("ok",)]:
                    return False
        except sqlite3.Error:
            return False
    from label_data_manager import read_recovery_file
    for path in paths.data_root.rglob("*_current_set_state_packaging.json"):
        if read_recovery_file(path, current_state=True).get("verified") is not True:
            return False
    for path in (paths.data_root / "_scanner_audit_outbox").glob("*.json"):
        read_json(path)
    for path in paths.data_root.glob("*held*state*.json"):
        read_json(path)
    for path in (paths.direct_sync_root / "queue").glob("*.json"):
        read_json(path)
    relay = optional_record(user_relay_status_path(paths.direct_sync_root))
    sha = version_manifest(app_root.parent)["source_commit"]
    if not relay or relay.get("status") != "RUNNING" or relay.get("source_commit") != sha or relay.get("update_nonce") != state["nonce"] or relay.get("ordinary_user") is not True:
        return False
    if role == "relay" and (relay.get("process_id") != pid
            or _utc(relay.get("updated_at")) < _utc(state.get("trial_started_at") or state.get("rollback_at") or state["applied_at"]).replace(microsecond=0)):
        return False
    payload = {
        **{check: True for check in (RELAY_READY_CHECKS if role == "relay" else READY_CHECKS)},
        "role": role, "pid": pid, "sha": sha,
        "nonce": state["nonce"], "transition_id": state["transition_id"], "ready_at": now_utc(),
    }
    return Path(policy["staging_root"]) / "ready.json", payload


def _write_ready_receipt(proof) -> bool:
    """Commit a previously read receipt; bootstrap holds both update locks."""
    if isinstance(proof, bool):
        return proof
    path, payload = proof
    write_json_durable(path, payload)
    return True


def _publish_trial_ready(app_root: Path, *, role: str, pid: int, context=None) -> bool:
    result = _write_ready_receipt(_trial_ready_receipt(app_root, role=role, pid=pid, context=context))
    if result:
        _request_retained_confirmation(app_root)
    return result


def publish_gui_ready(app_root: Path) -> bool:
    return _publish_trial_ready(app_root, role="gui", pid=os.getpid())


def _request_retained_confirmation(app_root):
    """A published late witness requests the existing SYSTEM task, best effort."""
    try:
        control = control_root(app_root)
        if control is None:
            return
        state = optional_record(control / "transition.json")
        if not (state and state.get("phase") == "DEFERRED" and state.get("retained_candidate") is True
                and _retained_confirmation_state(state)):
            return
        policy = load_policy(control)
        with update_lock(control, policy["operator_sid"]):
            state = optional_record(control / "transition.json")
            active = read_json(control / "active.json")
            if not _retained_confirmation_allowed(state, active):
                return
            with update_lock(control, policy["operator_sid"], trial=True):
                claim = optional_record(Path(policy["staging_root"]) / "trial.json")
                ready = optional_record(Path(policy["staging_root"]) / "ready.json")
                if not (claim and ready and ready.get("pid") == claim.get("pid")
                        and ready.get("nonce") == state["nonce"]
                        and ready.get("transition_id") == state["transition_id"]):
                    return
            request_apply_task(control, policy)
    except Exception:
        pass  # An optional late decision cannot turn a READY application into a stop.


def _relay_ready_receipt(app_root: Path, pid: int) -> tuple[Path, dict[str, Any]] | bool:
    """Ordinary bootstrap reads the same durable data after the target relay cycle."""
    # The launcher does not inherit its child's nonce. Bind the read-only
    # witness to the actual claim and native target instead of accepting an
    # absent self-trial context without writing a receipt. This also supports
    # immutable historical relays that do not publish their own READY receipt.
    if not ordinary_user_context() or type(pid) is not int or pid < 1:
        return False
    control = control_root(app_root)
    if control is None:
        return True
    policy = load_policy(control)
    state = optional_record(control / "transition.json")
    if state and state.get("phase") == "DEFERRED" and state.get("retained_candidate") is True and _retained_confirmation_state(state):
        active = read_json(control / "active.json")
        if (state.get("phase") == "DEFERRED" and state.get("retained_candidate") is True
                and _retained_confirmation_allowed(state, active)):
            return _retained_relay_ready_receipt(app_root, pid, control, policy, state, active)
    if not state or state.get("phase") not in {"APPLIED_PENDING_READY", "ROLLBACK_PENDING_READY"}:
        return True
    sha = state["previous_sha"] if state["phase"] == "ROLLBACK_PENDING_READY" else state["candidate_sha"]
    if (version_manifest(app_root.parent)["source_commit"] != sha
            or read_json(control / "active.json")["sha"] != sha):
        return False
    claim = optional_record(Path(policy["staging_root"]) / "trial.json")
    if (not claim or claim.get("role") != "relay" or claim.get("pid") != pid
            or claim.get("nonce") != state["nonce"] or claim.get("transition_id") != state["transition_id"]):
        return False
    rows = writer_processes(process_snapshot(control), own_pid=os.getpid())
    relays = [row for row in rows if process_matches_trial(row, state=state, sha=sha,
              sid=policy["operator_sid"], role="relay")]
    if (len(relays) != 1 or relays[0]["pid"] != pid
            or _utc(relays[0]["created_at"]) > _utc(claim.get("claimed_at"))):
        return False
    return _trial_ready_receipt(app_root, role="relay", pid=pid, context=(control, policy, state))


def _retained_relay_ready_receipt(app_root, pid, control, policy, state, active):
    claim = optional_record(Path(policy["staging_root"]) / "trial.json")
    if not claim or claim.get("role") != "relay" or claim.get("pid") != pid:
        return False
    rows = writer_processes(process_snapshot(control), own_pid=os.getpid())
    binding = _retained_trial_binding(state, active, claim, rows, policy)
    if binding is None or version_manifest(app_root.parent)["source_commit"] != active["sha"]:
        return False
    projected, _identity = binding
    relays = [row for row in rows if process_matches_trial(row, state=projected,
        sha=active["sha"], sid=policy["operator_sid"], role="relay")]
    if len(relays) != 1 or relays[0]["pid"] != pid:
        return False
    return _trial_ready_receipt(app_root, role="relay", pid=pid, context=(control, policy, projected))


def publish_relay_ready(app_root: Path, pid: int) -> bool:
    """Publish the unchanged relay receipt after its read-only checks."""
    return _write_ready_receipt(_relay_ready_receipt(app_root, pid))


def _read_bootstrap_trial_observation(control, policy, state, active, claim, *, identity=None):
    """Read an existing trial without either update mutex or writer admission."""
    identity = native_process_identity(claim["pid"]) if identity is None else identity
    proof = False
    error = None
    try:
        claimed_role = claim.get("role", "gui")
        row = next((row for row in process_snapshot(control)
                    if row.get("pid") == claim.get("pid")), None)
        observed_state = state
        if _retained_confirmation_allowed(state, active):
            binding = _retained_trial_binding(state, active, claim, [row] if row else [], policy)
            if binding is None:
                raise ValueError("existing trial process identity or creation time differs")
            observed_state = binding[0]
        if (not row or claim.get("transition_id") != state["transition_id"]
                or claimed_role not in {"gui", "relay"}
                or not process_matches_trial(row, state=observed_state, sha=active["sha"],
                    sid=policy["operator_sid"], role=claimed_role)
                or _utc(row.get("created_at")) > _utc(claim.get("claimed_at"))):
            raise ValueError("existing trial process identity or creation time differs")
        if claimed_role == "relay":
            proof = _relay_ready_receipt(version_bundle(control, active["sha"]) / "app", claim["pid"])
    except Exception as caught:
        error = caught
    return identity, proof, error


def _commit_bootstrap_trial_observation(control, policy, state, active, claim, observation, *, expected_ready=None):
    """Caller owns update→trial; discard any changed state or native lifetime."""
    identity, proof, error = observation
    if (optional_record(control / "transition.json") != state
            or read_json(control / "active.json") != active
            or optional_record(Path(policy["staging_root"]) / "trial.json") != claim
            or optional_record(Path(policy["staging_root"]) / "ready.json") != expected_ready):
        return False
    current_identity = native_process_identity(claim["pid"])
    if (identity.get("status") != "LIVE" or not identity.get("creation_identity")
            or any(current_identity.get(key) != identity.get(key)
                   for key in ("status", "creation_identity"))):
        return False
    failure = optional_record(Path(policy["staging_root"]) / "trial-failure.json")
    if failure and failure.get("nonce") == state["nonce"] and failure.get("transition_id") == state["transition_id"]:
        return False
    if error is not None:
        raise error
    if not isinstance(proof, bool):
        _path, payload = proof
        if (payload.get("nonce") != state["nonce"]
                or payload.get("transition_id") != state["transition_id"]
                or payload.get("pid") != claim["pid"]
                or payload.get("sha") != active["sha"]):
            return False
        _write_ready_receipt(proof)
    return True


def confirmed_trial(app_root: Path) -> bool:
    control = control_root(app_root)
    if control is None:
        return False
    state = optional_record(control / "transition.json")
    active = read_json(control / "active.json")
    sha = version_manifest(app_root.parent)["source_commit"]
    return bool(state and state.get("phase") in {"CONFIRMED", "ROLLED_BACK"}
                and state.get("trial_sha") == sha and active.get("sha") == sha)


def manual_lifecycle_gate(control: Path, mode: str) -> int | None:
    """User removal owns only its Run/relay; registration requires health."""
    if mode == "--remove-current-user-setup":
        return None
    from writer_session_fence import active_fence, canonical_control_root, _delegation_matches
    fence = active_fence(canonical_control_root(), environ=os.environ)
    source = "current_user_onboarding"
    if fence is not None and _delegation_matches(fence, source=source, environ=os.environ):
        return None
    try:
        resolve_lifecycle_context(control.parent, operation="register")
        return None
    except Exception as error:
        details = launch_failure_details(error, include_original=True)
    print(json.dumps({
        "status": "RECOVERY_REQUIRED", "code": "SUPPORTED_REMOVAL_REQUIRED",
        "data_preserved": True,
        "message": "자료와 등록 및 전송 대기를 보존하고 지원 제거를 먼저 실행한 뒤 다시 설치하세요.",
        "errors": details,
    }, ensure_ascii=False))
    return 20


def startup_bootstrap_reason(sha: str, active: Mapping[str, Any], state, prepared: bool) -> str | None:
    """The ordinary-start decision, without locks, writes or bootstrap launch."""
    if prepared:
        return "UPDATE_PREPARED"
    if state and state.get("recovery_pending") is True:
        return "UPDATE_RECOVERY"
    if state and state["phase"] not in {"CONFIRMED", "ROLLED_BACK", "DEFERRED"}:
        return "UPDATE_PHASE"
    if sha != active["sha"]:
        return "ACTIVE_BUILD_MISMATCH"
    return None


def _record_abandoned_update(policy, *, verified: bool) -> None:
    fields = {"reason_code": ("UPDATE_MUTEX_ABANDONED_VERIFIED" if verified else
                              "UPDATE_MUTEX_ABANDONED_RECOVERY_REQUIRED"),
              "exit_code": 0 if verified else 20}
    try:
        emit_support_record(fields, stage="update_apply", policy=policy)
    except Exception:
        # A diagnostic failure cannot mask the recovery decision at the
        # module-level entrypoint. Publish only fixed codes, never exceptions.
        try:
            print(json.dumps({**fields, "stage": "update_apply"}, sort_keys=True))
        except Exception:
            pass


def _verify_abandoned_update(control: Path, policy: Mapping[str, Any]) -> None:
    """Read-only proof while the abandoned update mutex is still owned."""
    try:
        state = read_json(control / "transition.json")
        active = read_json(control / "active.json")
        if state.get("recovery_pending") is True:
            raise ValueError("update recovery remains pending")
        resolve_lifecycle_context(control.parent, expected_from_commit=_sha(active.get("sha")),
                                  operation="register")
        if (read_json(control / "transition.json") != state
                or read_json(control / "active.json") != active or load_policy(control) != policy):
            raise ValueError("update installation changed during abandoned-owner verification")
    except Exception as error:
        raise UpdateMutexRecoveryRequired(policy) from error
    _record_abandoned_update(policy, verified=True)


def _show_update_mutex_recovery_notice() -> None:
    try:
        from startup_observation import show_stoppable_startup_notice
        show_stoppable_startup_notice("설치 복구 필요", UPDATE_MUTEX_RECOVERY_MESSAGE)
    except Exception:
        pass  # The fixed stderr guidance remains available when Tk cannot open.


def _refuse_abandoned_update(policy, *, role: str) -> int:
    _record_abandoned_update(policy, verified=False)
    try:
        print(UPDATE_MUTEX_RECOVERY_MESSAGE + " [UPDATE_MUTEX_ABANDONED_RECOVERY_REQUIRED]", file=sys.stderr)
    except Exception:
        pass
    if role == "gui":
        try:
            from startup_observation import observe_startup
            observe_startup(["INSTALL_MANIFEST_MISMATCH"], identity={}, refused=False)
        except Exception:
            pass
        try:
            _show_update_mutex_recovery_notice()
        except Exception:
            pass
    return 20


def startup_gate(app_root: Path, arguments: list[str], *, role: str = "gui") -> int | None:
    if arguments[:1] in (["--label-match-user-relay"], ["--label-match-direct-sync-relay"]):
        role = "relay"
    try:
        return _startup_gate(app_root, arguments, role=role)
    except UpdateMutexRecoveryRequired as error:
        return _refuse_abandoned_update(error.policy, role=role)
    except RuntimeError as error:
        if str(error) == "update mutex ownership was abandoned":
            return _refuse_abandoned_update(None, role=role)
        if str(error) != "update mutex ownership is busy":
            raise
        control = control_root(app_root)
        deferred_at = os.environ.get("KMTECH_LM_UPDATE_DEFERRED_AT")
        state = optional_record(control / "transition.json") if control is not None else None
        if deferred_at and control is not None and (not state or state.get("phase") in {"PREPARED", "CONFIRMED", "ROLLED_BACK", "DEFERRED", "FAILED", "UNKNOWN", "APPLIED_PENDING_READY", "ROLLBACK_PENDING_READY", "ROLLBACK_PENDING"}):
            policy = load_policy(control)
            assert_ordinary_recovery(control, policy, version_manifest(app_root.parent)["source_commit"], deferred_at,
                                     allow_relay_parent=role == "relay")
            assert_version_writer(app_root, source="gui_settings_save", environ=os.environ)
            emit_support_record({"reason_code": "UPDATE_APPLY_NO_PROGRESS", "exit_code": 0}, stage="update_apply", policy=policy)
            return None
        return bootstrap(app_root, arguments, role=role)


@contextmanager
def _startup_trial_lock(control, policy, *, role):
    """Wait only for this nonce trial's remaining durable READY window."""
    from contextlib import ExitStack
    deadline = None
    binding = None
    while True:
        if deadline is not None and time.monotonic() >= deadline:
            raise last_busy
        ownership = ExitStack()
        try:
            ownership.enter_context(update_lock(control, policy["operator_sid"],
                abandoned_check=lambda: _verify_abandoned_update(control, policy)))
        except RuntimeError as busy:
            ownership.close()
            if str(busy) != "update mutex ownership is busy" or role != "relay":
                raise
            sampled_at = time.monotonic()
            try:
                state = optional_record(control / "transition.json")
                claim = optional_record(Path(policy["staging_root"]) / "trial.json")
                nonce = os.environ.get("KMTECH_LM_UPDATE_TRIAL_NONCE")
                if (not state or state["phase"] not in {"APPLIED_PENDING_READY", "ROLLBACK_PENDING_READY"}
                        or not nonce or nonce != state["nonce"] or not claim
                        or claim.get("nonce") != nonce
                        or not isinstance(state["transition_id"], str) or not state["transition_id"]
                        or claim.get("transition_id") != state["transition_id"]):
                    raise ValueError("trial retry context differs")
                current_binding = (state["transition_id"], nonce, state["phase"])
                if binding is not None and current_binding != binding:
                    raise ValueError("trial retry context changed")
                anchor = state["rollback_at"] if state["phase"] == "ROLLBACK_PENDING_READY" else state["applied_at"]
                remaining = READY_WINDOW_SECONDS - (_utc(now_utc()) - _utc(anchor)).total_seconds()
                if remaining <= 0:
                    raise ValueError("trial retry window expired")
            except (OSError, ValueError, TypeError, KeyError, OverflowError, AttributeError):
                raise busy from None
            bounded = sampled_at + min(READY_WINDOW_SECONDS, remaining)
            deadline = bounded if deadline is None else min(deadline, bounded)
            binding = current_binding
            last_busy = busy
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise busy
            time.sleep(min(0.1, remaining))
            continue
        except BaseException:
            ownership.close()
            raise
        with ownership:
            yield
        return


def _startup_gate(app_root: Path, arguments: list[str], *, role: str) -> int | None:
    if arguments[:1] == ["--remove-current-user-setup"]:
        return None
    if arguments[:1] in (["--label-match-user-relay"], ["--label-match-direct-sync-relay"]):
        role = "relay"
    control = control_root(app_root)
    if control is None:
        return None
    if arguments and arguments[0] in {"--onboard-current-user", "--remove-current-user-setup", "--restore-current-user-lifecycle-after-replacement"}:
        return manual_lifecycle_gate(control, arguments[0])
    policy = load_policy(control)
    if not ordinary_user_context(policy["operator_sid"]):
        if role == "gui":
            try:
                from startup_observation import observe_startup
                observe_startup(["OPERATOR_ACCOUNT_MISMATCH"], identity={}, refused=True)
            except Exception:
                pass
        return 20
    with _startup_trial_lock(control, policy, role=role):
        state = optional_record(control / "transition.json")
        active = read_json(control / "active.json")
        release = version_manifest(app_root.parent)
        sha = release["source_commit"]
        if (sha == active["sha"] and state and (state["phase"] in {"APPLIED_PENDING_READY", "ROLLBACK_PENDING_READY"}
                or (state.get("phase") == "DEFERRED" and state.get("retained_candidate") is True
                    and _retained_confirmation_allowed(state, active)))):
            if os.environ.get("KMTECH_LM_UPDATE_TRIAL_NONCE") == state["nonce"] and ordinary_user_context():
                claim = optional_record(Path(policy["staging_root"]) / "trial.json")
                if claim and claim.get("nonce") == state["nonce"] and claim.get("transition_id") == state["transition_id"] and (role == "relay" or claim.get("pid") == os.getpid()):
                    return None
        deferred_at = os.environ.get("KMTECH_LM_UPDATE_DEFERRED_AT")
        if sha == active["sha"] and deferred_at and (not state or state["phase"] in {"PREPARED", "CONFIRMED", "ROLLED_BACK", "DEFERRED", "FAILED", "UNKNOWN", "APPLIED_PENDING_READY", "ROLLBACK_PENDING_READY", "ROLLBACK_PENDING"}):
            _utc(deferred_at)
            if state and (state["phase"] in {"PREPARED", "FAILED", "UNKNOWN", "APPLIED_PENDING_READY", "ROLLBACK_PENDING_READY", "ROLLBACK_PENDING"}
                          or (state["phase"] == "DEFERRED" and state.get("recovery_pending") is True)):
                assert_ordinary_recovery(control, policy, sha, deferred_at)
            # Ordinary active-code admission only. The unchanged G guard still
            # runs before data access; this grants no pointer or trial authority.
            return None
        prepared = prepared_pending(control, policy, state)
        reason = startup_bootstrap_reason(sha, active, state, prepared)
        if reason is None:
            return None
    if role == "gui":
        try:
            from startup_observation import observe_startup
            observe_startup([reason], identity={"version": release.get("version", ""), "build_id": sha})
        except Exception:
            pass
    request_bootstrap(control, arguments, role=role)
    return 0


@writer_sink("auto_update_request_bootstrap")
def request_bootstrap(control: Path, arguments: list[str], *, role: str) -> None:
    root = control.parent / "current"
    start_ordinary_process([str(root / "runtime/pythonw.exe"), "-I", "-B", str(root / "app/auto_update.py"), "--bootstrap", "--role", role, "--", *arguments], cwd=str(root))


@writer_sink("auto_update_request_apply_task")
def request_apply_task(control: Path, policy: Mapping[str, Any]) -> None:
    import subprocess

    executable = system_executable("schtasks.exe")
    result = subprocess.run([executable, "/Run", "/TN", task_name(control.parent / "current", policy["operator_sid"])], capture_output=True, creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0))
    if result.returncode:
        error = RuntimeError("the installed update task could not be requested; " + safe_update_text(result.stderr))
        error.child_output = child_failure_output(result)
        raise error


@writer_sink("auto_update_launch_version")
def launch_version(control: Path, sha: str, arguments: list[str], *, nonce: str = "", deferred_at: str = "",
                   relay_deadline: float | None = None) -> int:
    bundle = version_bundle(control, sha)
    if version_manifest(bundle)["source_commit"] != sha:
        raise ValueError("active version identity differs")
    environment = dict(os.environ)
    environment.pop("KMTECH_LM_UPDATE_TRIAL_NONCE", None)
    environment.pop("KMTECH_LM_UPDATE_DEFERRED_AT", None)
    environment.pop("KMTECH_LM_RELAY_PARENT", None)
    if (not nonce and isinstance(relay_deadline, _RelayPreparationDeadline)
            and relay_deadline.parent_pid is not None and relay_deadline.parent_identity is not None):
        environment["KMTECH_LM_RELAY_PARENT"] = f"{relay_deadline.parent_pid}:{relay_deadline.parent_identity}"
    if nonce:
        environment["KMTECH_LM_UPDATE_TRIAL_NONCE"] = _sha(nonce, length=32)
    if deferred_at:
        _utc(deferred_at)
        environment["KMTECH_LM_UPDATE_DEFERRED_AT"] = deferred_at
    headless = bool(arguments[:1] and arguments[0] in {
        "--label-match-user-relay", "--label-match-direct-sync-relay"
    })
    entry = headless_product_arguments(bundle, arguments) if headless else [str(bundle / "app/main.py"), *arguments]
    command = [str(bundle / "runtime/pythonw.exe"), "-I", "-B", *entry]
    # The writer decorator may have waited successfully. Check after that wait
    # and all launch preparation, immediately before creating the new process.
    if relay_deadline is not None:
        failure = _relay_prepare_failure(relay_deadline)
        if failure:
            raise _RelayPreparationUnavailable(failure)
    process = start_ordinary_process(command, cwd=str(bundle), environment=environment)
    return process.pid


# Historical packets already own these modes in their protected product host.
# Run that dispatcher directly rather than importing their GUI main module.
LEGACY_HEADLESS_BOOTSTRAP = (
    "import sys;from pathlib import Path;p=Path(sys.argv[1]);"
    "sys.path[:0]=[str(p),str(p/'site-packages')];"
    "from label_match_product_host import dispatch_product_mode;"
    "r=dispatch_product_mode(sys.argv[2:]);raise SystemExit(1 if r is None else int(r))"
)


def headless_product_arguments(bundle: Path, arguments: list[str]) -> list[str]:
    if not arguments or arguments[0] not in {
        "--label-match-user-relay", "--label-match-direct-sync-relay",
        "--onboard-current-user", "--remove-current-user-setup",
    }:
        raise ValueError("headless product mode is unsupported")
    app = bundle / "app"
    entry = app / "label_match_relay_entry.py"
    if entry.is_file():
        return [str(entry), *arguments]
    if not (app / "label_match_product_host.py").is_file():
        raise ValueError("historical packet has no supported headless product host")
    return ["-c", LEGACY_HEADLESS_BOOTSTRAP, str(app), *arguments]


def current_relay_ready(control: Path, policy: Mapping[str, Any], sha: str, *,
                        started_at: str, pid: int | None = None) -> bool:
    """A fresh cycle and native identity together prove the actual target relay."""
    from current_user_onboarding import resolve_current_user_onboarding_paths
    from user_relay import user_relay_status_path

    app = version_bundle(control, sha) / "app"
    paths = resolve_current_user_onboarding_paths(app.parent)
    status = optional_record(user_relay_status_path(paths.direct_sync_root))
    if (not status or status.get("status") != "RUNNING"
            or status.get("source_commit") != sha or status.get("ordinary_user") is not True
            or type(status.get("process_id")) is not int
            or (pid is not None and status["process_id"] != pid)
            # Stock relay cycle timestamps have whole-second precision; the
            # native PID creation time below still uses the exact launch time.
            or _utc(status.get("updated_at")) < _utc(started_at).replace(microsecond=0)
            or os.path.normcase(str(Path(status.get("code_root", "")).resolve())) not in {
                os.path.normcase(str(app.resolve())), os.path.normcase(str(app.parent.resolve()))}):
        return False
    rows = writer_processes(process_snapshot(control), own_pid=os.getpid())
    relays = [row for row in rows if row.get("role") == "relay"]
    return len(relays) == 1 and all(
        row.get("pid") == status["process_id"] and row.get("observable") is True
        and row.get("sha") == sha and row.get("sid") == policy["operator_sid"]
        and row.get("ordinary_user") is True
        and (pid is None or _utc(row.get("created_at")) >= _utc(started_at))
        for row in relays)


def wait_current_relay(control: Path, policy: Mapping[str, Any], sha: str, *,
                       started_at: str, deadline: float, pid: int | None = None) -> bool:
    child_identity = None
    while time.monotonic() < deadline:
        if _relay_prepare_failure(deadline):
            return False
        _relay_startup_stop_checkpoint(policy)
        try:
            if read_json(control / "active.json")["sha"] != sha:
                return False
            if current_relay_ready(control, policy, sha, started_at=started_at, pid=pid):
                return not _relay_prepare_failure(deadline)
        except (OSError, ValueError, TypeError, KeyError):
            pass  # Contested/incomplete proof never establishes readiness.
        if pid is not None:
            child = native_process_identity(pid)
            if (child.get("status") == "ABSENT"
                    or (child_identity is not None and child.get("status") == "LIVE"
                        and child.get("creation_identity") != child_identity)):
                raise _RelayPreparationUnavailable("RELAY_EXITED_BEFORE_RUNNING")
            if child.get("status") == "LIVE" and child.get("creation_identity"):
                child_identity = child["creation_identity"]
        time.sleep(min(0.1, max(0, deadline - time.monotonic())))
    return False


def current_generation_status(control: Path, policy: Mapping[str, Any], sha: str) -> str:
    from kmtech_shared import data_generation as generation

    try:
        observed = read_trial_generation(policy)
        if observed.get("status") != "READ":
            return "UNKNOWN"
        release = version_manifest(version_bundle(control, sha))
        return generation.probe_downgrade([observed["marker_path"]], release["version"],
            app_id="Label_Match", subject_sid=policy["operator_sid"], target_build_id=sha)["status"]
    except (OSError, ValueError, TypeError, KeyError):
        return "UNKNOWN"



def assert_ordinary_recovery(control: Path, policy: Mapping[str, Any], sha: str, started_at: str, *,
                             allow_relay_parent: bool = False) -> list[dict[str, Any]]:
    """Read-only proof under startup update ownership or existing writer admission."""
    if read_json(control / "active.json")["sha"] != sha:
        raise ValueError("ordinary recovery code is not the active version")
    if current_generation_status(control, policy, sha) != "ALLOW":
        raise ValueError("ordinary recovery requires current data generation allowance")
    started = _utc(started_at)
    rows = writer_processes(process_snapshot(control), own_pid=os.getpid())
    state = optional_record(control / "transition.json")
    if state and state.get("phase") == "ROLLBACK_PENDING":
        if (sha != _sha(state.get("previous_sha"))
                or apply_claim_status(control) not in {"NONE", "ABSENT"}
                or len([row for row in rows if row.get("role") == "relay"]) != 1):
            raise ValueError("pending rollback requires the previous active code and one ordinary relay")
    parent = (re.fullmatch(r"([1-9][0-9]*):([1-9][0-9]*)", os.environ.get("KMTECH_LM_RELAY_PARENT", ""))
              if allow_relay_parent else None)
    parent_skipped = False
    for row in rows:
        if (parent is not None and not parent_skipped
                and type(row.get("pid")) is int and str(row["pid"]) == parent[1]
                and row.get("sha") == sha and row.get("sid") == policy["operator_sid"]
                and row.get("ordinary_user") is True):
            try:
                identity = native_process_identity(int(parent[1]))
                if identity.get("status") == "LIVE" and identity.get("creation_identity") == parent[2]:
                    parent_skipped = True
                    continue
            except (OSError, ValueError, TypeError, KeyError, OverflowError):
                pass  # An unproved/reused parent remains an ordinary writer below.
        if (row.get("sha") != sha or row.get("sid") != policy["operator_sid"]
                or row.get("ordinary_user") is not True or row.get("role") not in {"gui", "relay"}
                or _utc(row.get("created_at")) < started):
            raise ValueError("an earlier or unrelated writer prevents ordinary recovery")
    return rows



def _launch_bootstrap_version(control: Path, sha: str, arguments: list[str], *, deadline: float, **kwargs) -> int:
    """Retry transient relay admission within this bootstrap's existing window."""
    while True:
        if isinstance(deadline, _RelayPreparationDeadline):
            failure = _relay_prepare_failure(deadline)
            if failure:
                raise _RelayPreparationUnavailable(failure)
        try:
            return launch_version(control, sha, arguments, relay_deadline=deadline, **kwargs)
        except WriterFencedError as error:
            if arguments[:1] != ["--label-match-user-relay"] or error.code != "WRITER_GATE_TIMEOUT":
                raise
            if isinstance(deadline, _RelayPreparationDeadline):
                failure = _relay_prepare_failure(deadline)
                if failure:
                    raise _RelayPreparationUnavailable(failure) from None
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise
            # launch_version has unwound its writer admission before waiting.
            time.sleep(min(0.1, remaining))
            if not isinstance(deadline, _RelayPreparationDeadline) and time.monotonic() >= deadline:
                raise


def _bootstrap_relay_snapshot(control, policy):
    stage = Path(policy["staging_root"])
    return (optional_record(control / "transition.json"), read_json(control / "active.json"),
            optional_record(stage / "trial.json"), optional_record(stage / "ready.json"))


def _bootstrap_relay_readback(control, policy, sha, readback, *, ownership=None, pid=None, started_at=None, deadline=None):
    """Release only bootstrap's root for read-only READY, then prove its binding again."""
    if ownership is None:
        return readback()
    expected = _bootstrap_relay_snapshot(control, policy)
    initial_identity = native_process_identity(pid) if pid is not None else None
    observed_identity = None
    error = None
    status_path = status = None
    ready = False
    ownership.close()
    try:
        try:
            ready = readback()
        except Exception as caught:
            error = caught
        if ready and pid is None:
            from current_user_onboarding import resolve_current_user_onboarding_paths
            from user_relay import user_relay_status_path
            paths = resolve_current_user_onboarding_paths(version_bundle(control, sha))
            status_path = user_relay_status_path(paths.direct_sync_root)
            status = optional_record(status_path)
            pid = (status or {}).get("process_id")
            if type(pid) is not int or pid <= 0 or started_at is None:
                ready = False
            else:
                initial_identity = native_process_identity(pid)
                ready = current_relay_ready(control, policy, sha,
                    started_at=started_at, pid=None)
                # A GUI may reuse a relay born before this GUI request. Keep
                # that original predicate, while binding its observed PID.
                ready = ready and optional_record(status_path) == status
        if ready or (error is not None and pid is not None):
            observed_identity = native_process_identity(pid)
    finally:
        ownership.enter_context(update_lock(control, policy["operator_sid"],
            abandoned_check=lambda: _verify_abandoned_update(control, policy)))
    if (_bootstrap_relay_snapshot(control, policy) != expected
            or expected[1].get("sha") != sha
            or (status_path is not None and optional_record(status_path) != status)):
        return False
    if error is not None:
        if pid is not None:
            current_identity = native_process_identity(pid)
            if (not initial_identity or initial_identity.get("status") != "LIVE"
                    or not initial_identity.get("creation_identity")
                    or any(not identity or identity.get("status") not in {"LIVE", "ABSENT"}
                           or (identity.get("status") == "LIVE" and
                               identity.get("creation_identity") != initial_identity["creation_identity"])
                           for identity in (observed_identity, current_identity))):
                return False
        raise error
    if not ready:
        return False
    if deadline is not None and (time.monotonic() >= deadline or _relay_prepare_failure(deadline)):
        return False
    current_identity = native_process_identity(pid)
    return bool(initial_identity and initial_identity.get("status") == "LIVE"
                and initial_identity.get("creation_identity")
                and all(identity and identity.get("status") == "LIVE"
                        and identity.get("creation_identity") == initial_identity["creation_identity"]
                        for identity in (observed_identity, current_identity)))


def _bootstrap_retained_candidate(control, policy, state, active, arguments, *, deadline, ownership):
    """Reuse the original nonce/claim and normal startup budget, without journal writes."""
    if (ownership is None or deadline is None
            or not _retained_confirmation_allowed(state, active)):
        return None
    role = "relay" if arguments[:1] and arguments[0] in {
        "--label-match-user-relay", "--label-match-direct-sync-relay"} else "gui"
    if role != state.get("trial_role", role):
        return None
    stage, started_at = Path(policy["staging_root"]), now_utc()
    with update_lock(control, policy["operator_sid"], trial=True):
        claim = optional_record(stage / "trial.json")
        claimed_identity = native_process_identity(claim["pid"]) if claim and type(claim.get("pid")) is int else {"status": "ABSENT"}
        if claimed_identity.get("status") == "LIVE":
            try:
                same = (claim.get("nonce") == state["nonce"]
                    and claim.get("transition_id") == state["transition_id"]
                    and claim.get("role", "gui") == role
                    and _retained_native_created_at(claimed_identity) <= _utc(claim.get("claimed_at")))
            except (KeyError, TypeError, ValueError, OverflowError, OSError):
                same = False
            if not same:
                return None
            # A known live claim needs no repeated absence Observe under root.
            # Full role/SID/build/receipt readback follows outside both locks.
            pid = claim["pid"]
            started_at = _retained_native_created_at(claimed_identity).isoformat()
        elif claimed_identity.get("status") != "ABSENT":
            return None
        else:
            rows = writer_processes(process_snapshot(control), own_pid=os.getpid())
            if any(row.get("role") == role for row in rows):
                return None  # The normal existing-writer path preserves its owner.
            pid = _launch_bootstrap_version(control, active["sha"], arguments,
                nonce=state["nonce"], deferred_at=started_at, deadline=deadline)
            claim = {"pid": pid, "role": role, "nonce": state["nonce"],
                     "transition_id": state["transition_id"], "claimed_at": now_utc()}
            write_json_durable(stage / "trial.json", claim)
            if role == "gui" and not any(row.get("role") == "relay" for row in rows):
                _launch_bootstrap_version(control, active["sha"], ["--label-match-user-relay"],
                    nonce=state["nonce"], deferred_at=started_at, deadline=deadline)
        identity = native_process_identity(pid)
    # No other named lock is held while the same normal RUNNING proof waits.
    ownership.close()
    error = None
    try:
        ready = wait_current_relay(control, policy, active["sha"], started_at=started_at,
            deadline=deadline, pid=pid if role == "relay" else None)
    except _RelayPreparationUnavailable as caught:
        ready, error = False, caught
    finally:
        ownership.enter_context(update_lock(control, policy["operator_sid"],
            abandoned_check=lambda: _verify_abandoned_update(control, policy)))
    current_identity = native_process_identity(pid)
    if (optional_record(control / "transition.json") != state
            or read_json(control / "active.json") != active
            or optional_record(stage / "trial.json") != claim
            or identity.get("status") != "LIVE" or not identity.get("creation_identity")
            or any(current_identity.get(key) != identity.get(key)
                   for key in ("status", "creation_identity"))):
        emit_support_record({"reason_code": "UPDATE_APPLY_NO_PROGRESS"}, stage="update_apply")
        return 20
    if error is not None:
        emit_support_record({"reason_code": error.code}, stage="update_apply")
        return 20
    if not ready:
        return 20
    from contextlib import ExitStack
    with ExitStack() as trial_ownership:
        trial_ownership.enter_context(update_lock(control, policy["operator_sid"], trial=True))
        expected_ready = optional_record(stage / "ready.json")
        identity = native_process_identity(pid)
        trial_ownership.close()
        ownership.close()
        try:
            observation = _read_bootstrap_trial_observation(control, policy, state, active, claim,
                identity=identity)
        finally:
            ownership.enter_context(update_lock(control, policy["operator_sid"],
                abandoned_check=lambda: _verify_abandoned_update(control, policy)))
            trial_ownership.enter_context(update_lock(control, policy["operator_sid"], trial=True))
        if (optional_record(control / "transition.json") != state
                or read_json(control / "active.json") != active
                or optional_record(stage / "trial.json") != claim):
            return 20
        if observation[2] is not None:
            return None  # Normal existing-writer handling still decides the ordinary start.
        try:
            committed = _commit_bootstrap_trial_observation(control, policy, state, active, claim,
                observation, expected_ready=expected_ready)
        except (OSError, ValueError, TypeError, RuntimeError, WriterFenceError):
            committed = False
        published = committed and (isinstance(observation[1], tuple) or bool(expected_ready))
    if published:
        try:
            request_apply_task(control, policy)
        except Exception:
            pass  # Preserve the normal READY start and the durable retained decision.
    return 0


def start_current(control: Path, policy: Mapping[str, Any], state, active, arguments, *, code: str, report_once: bool = False, deadline: float | None = None, _ownership=None) -> int:
    """Reuse ordinary launch and publish only the E9 allowlisted support fields."""
    if state and state.get("phase") == "ROLLBACK_PENDING":
        # Only recovery_writer_result carries the proved relay boundary. A task
        # request with no progress cannot manufacture an ordinary GUI cohort.
        emit_progress_guidance()
        return 20
    if state and state.get("phase") == "DEFERRED" and state.get("retained_candidate") is True:
        late = _bootstrap_retained_candidate(control, policy, state, active, arguments,
            deadline=deadline, ownership=_ownership)
        if late is not None:
            return late
    if _ownership is not None and arguments[:1] == ["--label-match-user-relay"]:
        # Root still protects absence -> launch. A prior bootstrap's Popen is
        # already visible; reuse its ordinary relay without a shared record.
        existing = writer_processes(process_snapshot(control), own_pid=os.getpid())
        if any(row.get("role") == "relay" for row in existing):
            reused = recovery_writer_result(control, policy, active, arguments, role="relay",
                deadline=deadline, state=state, claim_status=apply_claim_status(control),
                _ownership=_ownership)
            if reused is not None:
                return reused
        # Non-relay writers retain the original launch admission, including
        # its exact parent-lifetime exception; reuse grants no new authority.
    report = support_record({"reason_code": code,
        "transition_code": (state or {}).get("failure_decision", {}).get("code", code),
        "retained_candidate": (state or {}).get("retained_candidate") is True}, stage="update_apply")
    try:
        report_path = Path(policy["staging_root"]) / "startup-support.json"
        previous = None
        if report_once:
            claim_hash = hashlib.sha256((control / "apply-claim.json").read_bytes()).hexdigest()
            report = support_record({**report, "apply_claim_sha256": claim_hash}, stage="update_apply")
            previous = optional_record(report_path)
        if not (report_once and previous and previous.get("reason_code") == code
                and previous.get("apply_claim_sha256") == report["apply_claim_sha256"]):
            write_json_durable(report_path, report)
    except (OSError, ValueError, TypeError, WriterFenceError):
        # A support sink failure cannot turn a safe ordinary launch into a stop.
        emit_support_record({"reason_code": "REPORT_UNAVAILABLE"}, stage="update_apply")
    started_at = now_utc()
    if state and state["phase"] in {"PREPARED", "FAILED", "UNKNOWN", "APPLIED_PENDING_READY"}:
        assert_ordinary_recovery(control, policy, active["sha"], started_at)
    if deadline is None:
        deadline = time.monotonic() + RELAY_PREPARE_SECONDS
    pid = None
    failure = _relay_prepare_failure(deadline)
    if not failure:
        try:
            pid = _launch_bootstrap_version(control, active["sha"], arguments, deadline=deadline, deferred_at=started_at)
        except _RelayPreparationUnavailable as error:
            failure = error.code
    relay_pid = pid if arguments[:1] and arguments[0] in {"--label-match-user-relay", "--label-match-direct-sync-relay"} else None
    ready = False
    if pid is not None:
        try:
            ready = _bootstrap_relay_readback(control, policy, active["sha"],
                lambda: wait_current_relay(control, policy, active["sha"], started_at=started_at,
                    deadline=deadline, pid=relay_pid), ownership=_ownership, pid=relay_pid,
                started_at=started_at, deadline=deadline)
        except _RelayPreparationUnavailable as error:
            if error.code != "RELAY_EXITED_BEFORE_RUNNING":
                failure = error.code
    if not ready:
        failure = failure or _relay_prepare_failure(deadline) or "UPDATE_APPLY_NO_PROGRESS"
        failed_report = support_record({"reason_code": failure,
            "transition_code": (state or {}).get("failure_decision", {}).get("code", code),
            "retained_candidate": (state or {}).get("retained_candidate") is True}, stage="update_apply")
        try:
            write_json_durable(Path(policy["staging_root"]) / "startup-support.json", failed_report)
        except (OSError, ValueError, TypeError, WriterFenceError):
            emit_support_record({"reason_code": "REPORT_UNAVAILABLE"}, stage="update_apply")
        emit_support_record({"reason_code": failure}, stage="update_apply")
        return 20
    return 0



def gui_recovery_state(state, claim_status: str) -> bool:
    """Eligible ordinary recovery may add the missing GUI to a proved relay."""
    if not state or claim_status not in {"NONE", "ABSENT"}:
        return False
    phase = state.get("phase")
    return (phase in {"FAILED", "UNKNOWN", "ROLLBACK_PENDING"}
            or (phase == "DEFERRED" and state.get("recovery_pending") is True)
            or (phase in {"APPLIED_PENDING_READY", "ROLLBACK_PENDING_READY"}
                and claim_status == "ABSENT"))


def recovery_writer_result(control: Path, policy: Mapping[str, Any], active,
                           arguments: list[str], *, role: str, deadline: float,
                           state=None, claim_status: str = "", _ownership=None) -> int | None:
    """Under update ownership, preserve GUI reuse and prove an ordinary relay.

    A recovered GUI is a writer, but does not satisfy its own relay request.
    Only its inherited recovery boundary permits launching that missing relay.
    This path grants no trial authority and publishes no persistent records.
    """
    failure = _relay_prepare_failure(deadline)
    if failure:
        emit_support_record({"reason_code": failure}, stage="update_apply")
        return 20
    try:
        rows = writer_processes(process_snapshot(control), own_pid=os.getpid())
        if not rows:
            return None
        if role != "relay":
            if (role != "gui" or any(row.get("role") == "gui" for row in rows)
                    or not gui_recovery_state(state, claim_status)
                    or arguments[:1] in (["--label-match-user-relay"], ["--label-match-direct-sync-relay"])):
                return _relay_prepare_result(deadline)
            if len(rows) != 1 or rows[0].get("role") != "relay":
                raise ValueError("ordinary GUI recovery requires exactly one relay")
            if type(rows[0].get("pid")) is not int or rows[0]["pid"] <= 0:
                raise ValueError("ordinary recovery relay process identity is unreadable")
            boundary = rows[0].get("created_at")
            if _utc(boundary) >= _utc(now_utc()):
                raise ValueError("ordinary recovery relay is not in the past")
            rows = assert_ordinary_recovery(control, policy, active["sha"], boundary)
            if any(row.get("role") == "gui" for row in rows):
                return _relay_prepare_result(deadline)
            if len(rows) != 1 or rows[0].get("role") != "relay":
                raise ValueError("the proved ordinary relay cohort changed")
            if type(rows[0].get("pid")) is not int or rows[0]["pid"] <= 0:
                raise ValueError("proved ordinary relay process identity is unreadable")
            # The proof's own observation determines both the sole relay and
            # the GUI's boundary, even if the earlier candidate was replaced.
            boundary = rows[0]["created_at"]
            if _utc(boundary) >= _utc(now_utc()):
                raise ValueError("ordinary recovery relay is not in the past")
            _launch_bootstrap_version(control, active["sha"], arguments,
                                      deadline=deadline, deferred_at=boundary)
            return _relay_prepare_result(deadline)
        started_at = os.environ.get("KMTECH_LM_UPDATE_DEFERRED_AT")
        if started_at:
            if _utc(started_at) >= _utc(now_utc()):
                raise ValueError("ordinary recovery boundary is not in the past")
            rows = assert_ordinary_recovery(control, policy, active["sha"], started_at)
        else:
            relays = [row for row in rows if row.get("role") == "relay"]
            if not relays:
                raise ValueError("ordinary relay launch requires an inherited recovery boundary")
            # Without a boundary this can only reuse an observed live relay.
            # Validate every writer's identity; no new launch is authorized.
            observed_start = min(rows, key=lambda row: _utc(row.get("created_at")))["created_at"]
            rows = assert_ordinary_recovery(control, policy, active["sha"], observed_start)
        relays = [row for row in rows if row.get("role") == "relay"]
        if len(relays) > 1:
            raise ValueError("ordinary recovery has multiple relays")
        if not started_at:
            if not relays:
                raise ValueError("ordinary relay reuse requires a proved live relay")
            started_at = relays[0]["created_at"]
        if relays:
            if _bootstrap_relay_readback(control, policy, active["sha"],
                lambda: wait_current_relay(control, policy, active["sha"],
                    started_at=started_at, deadline=deadline, pid=relays[0]["pid"]),
                ownership=_ownership, pid=relays[0]["pid"], started_at=started_at, deadline=deadline):
                return _relay_prepare_result(deadline)
        else:
            pid = _launch_bootstrap_version(control, active["sha"], arguments,
                                            deadline=deadline, deferred_at=started_at)
            if arguments[:1] == ["--label-match-direct-sync-relay"]:
                # This finite helper is observed as a writer, not a resident
                # relay. Its successful launch cannot publish resident READY.
                return _relay_prepare_result(deadline)
            if _bootstrap_relay_readback(control, policy, active["sha"],
                lambda: wait_current_relay(control, policy, active["sha"], started_at=started_at,
                    deadline=deadline, pid=pid), ownership=_ownership, pid=pid, started_at=started_at, deadline=deadline):
                return _relay_prepare_result(deadline)
    except _RelayPreparationUnavailable as error:
        reason = "UPDATE_APPLY_NO_PROGRESS" if error.code == "RELAY_EXITED_BEFORE_RUNNING" else error.code
        emit_support_record({"reason_code": reason}, stage="update_apply")
        return 20
    except (OSError, ValueError, TypeError, KeyError, RuntimeError):
        if role != "relay" and not (role == "gui" and gui_recovery_state(state, claim_status)):
            raise
    failure = _relay_prepare_failure(deadline)
    if failure:
        emit_support_record({"reason_code": failure}, stage="update_apply")
    else:
        emit_progress_guidance()
    return 20


def bootstrap(app_root: Path, arguments: list[str], *, role: str,
              parent_deadline=None, parent_pid=None) -> int:
    try:
        if parent_deadline is not None or parent_pid is not None:
            return _bootstrap(app_root, arguments, role=role,
                              parent_deadline=parent_deadline, parent_pid=parent_pid)
        return _bootstrap(app_root, arguments, role=role)
    except StartupStopRequested:
        return 0
    except _RelayPreparationUnavailable as error:
        emit_support_record({"reason_code": error.code}, stage="update_apply")
        return 20
    except UpdateMutexRecoveryRequired as error:
        return _refuse_abandoned_update(error.policy, role=role)
    except RuntimeError as error:
        if str(error) == "update mutex ownership was abandoned":
            return _refuse_abandoned_update(None, role=role)
        if str(error) not in {"update mutex ownership is busy", "update mutex ownership was abandoned"}:
            raise
        record = support_record({"reason_code": "UPDATE_IN_PROGRESS"}, stage="update_apply")
        record["guidance"] = STARTUP_PROGRESS_MESSAGE
        print(json.dumps(record, ensure_ascii=False, sort_keys=True))
        return 20



def _relay_startup_stop_checkpoint(policy):
    from startup_timing import StartupStopRequested
    if relay_stop_requested(policy):
        raise StartupStopRequested()


def _bootstrap_awaiting_apply_decision(state, claim_status) -> bool:
    return claim_status == "LIVE" and (state is None or state.get("phase") in {
        "PREPARED", "APPLIED_PENDING_READY", "ROLLBACK_PENDING", "ROLLBACK_PENDING_READY"})


def bootstrap_supervision_deadline(state, *, started: float, now: float, utc_now: str,
                                   claim_status=None) -> float:
    """Observe durable stage clocks without reopening any of their windows."""
    if _bootstrap_awaiting_apply_decision(state, claim_status):
        return started + BOOTSTRAP_OBSERVATION_CAP_SECONDS
    phase = (state or {}).get("phase")
    timestamp, window = None, 0
    if phase == "APPLIED_PENDING_READY":
        timestamp, window = state.get("applied_at"), READY_WINDOW_SECONDS
    elif phase == "ROLLBACK_PENDING_READY":
        timestamp, window = state.get("rollback_at", state.get("applied_at")), READY_WINDOW_SECONDS
    elif phase in {"ROLLBACK_PENDING", "UNKNOWN"}:
        timestamp, window = state.get("normal_stop_started_at"), 60
    stage_end = now + (window - (_utc(utc_now) - _utc(timestamp)).total_seconds()) if timestamp else started
    return min(started + BOOTSTRAP_OBSERVATION_CAP_SECONDS, max(started + STARTUP_WAIT_SECONDS, stage_end + 15))


def _bootstrap(app_root: Path, arguments: list[str], *, role: str,
               parent_deadline=None, parent_pid=None) -> int:
    from contextlib import ExitStack
    try:
        relay_deadline = _relay_preparation_deadline(parent_deadline, parent_pid)
    except _RelayPreparationUnavailable as error:
        emit_support_record({"reason_code": error.code}, stage="update_apply")
        return 20
    failure = _relay_prepare_failure(relay_deadline)
    if failure:
        emit_support_record({"reason_code": failure}, stage="update_apply")
        return 20
    if role == "relay":
        if not arguments or arguments[0] not in {"--label-match-user-relay", "--label-match-direct-sync-relay"}:
            raise ValueError("relay bootstrap requires an explicit relay mode")
        from current_user_onboarding import _machine_worker_context
        _machine_worker_context(app_root)
    control = control_root(app_root)
    if control is None:
        raise ValueError("bootstrap requires the installed ordinary-user context")
    policy = load_policy(control)
    if not ordinary_user_context(policy["operator_sid"]):
        raise ValueError("bootstrap requires the installed ordinary operator")
    requested = False
    requested_at = None
    progress_deadline = None
    saw_trial = False
    apply_started = False
    waiting_for_decision = False
    started = time.monotonic()
    deadline = started + STARTUP_WAIT_SECONDS
    while True:
        if waiting_for_decision:
            observed_state = optional_record(control / "transition.json")
            if _bootstrap_awaiting_apply_decision(observed_state, "LIVE"):
                if apply_claim_status(control) != "LIVE":
                    break
        if time.monotonic() >= deadline:
            state = optional_record(control / "transition.json")
            claim_status = apply_claim_status(control)
            if not _bootstrap_awaiting_apply_decision(state, claim_status):
                break
            deadline = bootstrap_supervision_deadline(state, started=started,
                now=time.monotonic(), utc_now=now_utc(), claim_status=claim_status)
            if time.monotonic() >= deadline:
                break
            waiting_for_decision = True
        trial_observation_changed = False
        failure = _relay_prepare_failure(relay_deadline)
        if failure:
            emit_support_record({"reason_code": failure}, stage="update_apply")
            return 20
        try:
            with ExitStack() as bootstrap_ownership:
                bootstrap_ownership.enter_context(update_lock(control, policy["operator_sid"],
                    abandoned_check=lambda: _verify_abandoned_update(control, policy)))
                state = optional_record(control / "transition.json")
                claim_status = apply_claim_status(control)
                stage_deadline = bootstrap_supervision_deadline(state, started=started,
                    now=time.monotonic(), utc_now=now_utc())
                if waiting_for_decision:
                    if not _bootstrap_awaiting_apply_decision(state, "LIVE"):
                        # Terminal publication precedes claim settlement. Keep
                        # the original extension until its owner is finished.
                        stage_deadline = deadline
                        if claim_status not in {"LIVE", "UNKNOWN"}:
                            waiting_for_decision = False
                    elif claim_status != "LIVE":
                        break
                    elif time.monotonic() >= stage_deadline:
                        # SYSTEM can still hold the root during its final Observe.
                        # Leave it ownership and never start another trial here.
                        bootstrap_ownership.close()
                        time.sleep(0.1)
                        continue
                    else:
                        waiting_for_decision = False
                deadline = stage_deadline
                apply_started = apply_started or claim_status in {"LIVE", "UNKNOWN"} or bool(state and state["phase"] in {"APPLIED_PENDING_READY", "ROLLBACK_PENDING_READY"})
                if role == "relay" and relay_stop_requested(policy):
                    if claim_status in {"LIVE", "UNKNOWN"}:
                        time.sleep(0.1)
                        continue
                    _relay_startup_stop_checkpoint(policy)
                if state and state["phase"] in {"APPLIED_PENDING_READY", "ROLLBACK_PENDING_READY"}:
                    saw_trial = True
                if claim_status in {"LIVE", "UNKNOWN"} and not (state and state["phase"] in {"APPLIED_PENDING_READY", "ROLLBACK_PENDING_READY"}):
                    time.sleep(0.1)
                    continue
                prepared = prepared_pending(control, policy, state)
                action = startup_action(state, prepared_present=prepared)
                active = read_json(control / "active.json")
                startup_arguments = list(arguments)
                if role == "gui" and gui_recovery_state(state, claim_status):
                    writer_result = recovery_writer_result(control, policy, active, arguments,
                        role=role, deadline=relay_deadline, state=state, claim_status=claim_status, _ownership=bootstrap_ownership)
                    if writer_result is not None:
                        return writer_result
                    if state["phase"] == "ROLLBACK_PENDING" and claim_status == "ABSENT":
                        emit_progress_guidance()
                        return 20
                if state and state["phase"] == "DEFERRED" and state.get("retained_candidate") is True:
                    if active["sha"] != recovery_sha(state, pointer_sha=active["sha"]):
                        action = "WAIT_FOR_RECOVERY"
                    elif not prepared:
                        return start_current(control, policy, state, active, startup_arguments, code="UPDATE_TERMINAL_RECOVERED", deadline=relay_deadline, _ownership=bootstrap_ownership)
                    # A different, nonfailed prepared version follows the same
                    # request/stop/progress path as an ordinary next start.
                if state and state["phase"] in {"CONFIRMED", "ROLLED_BACK"} and saw_trial:
                    if state["phase"] == "ROLLED_BACK":
                        if role == "relay" and state.get("trial_role", "gui") == "relay":
                            rollback_relay_ready = False
                            if state.get("trial_sha") == active["sha"]:
                                try:
                                    rollback_relay_ready = _bootstrap_relay_readback(control, policy, active["sha"], lambda: wait_current_relay(control, policy, active["sha"],
                                        started_at=state["trial_started_at"], deadline=relay_deadline,
                                        pid=state["trial_pid"]), ownership=bootstrap_ownership, pid=state["trial_pid"], deadline=relay_deadline)
                                except _RelayPreparationUnavailable as error:
                                    if error.code != "RELAY_EXITED_BEFORE_RUNNING":
                                        raise
                            if rollback_relay_ready:
                                return _relay_prepare_result(relay_deadline)
                            emit_support_record({"reason_code": "UPDATE_APPLY_NO_PROGRESS"}, stage="update_apply")
                            return 20
                        return start_current(control, policy, state, active, startup_arguments, code="UPDATE_PREVIOUS_CONFIRMED", deadline=relay_deadline, _ownership=bootstrap_ownership)
                    if role == "gui" and state.get("trial_role", "gui") == "relay":
                        return start_current(control, policy, state, active, startup_arguments,
                                             code="UPDATE_TERMINAL_RECOVERED", deadline=relay_deadline, _ownership=bootstrap_ownership)
                    return _relay_prepare_result(relay_deadline)  # The verified trial owns its normal relay.
                if state and state["phase"] == "DEFERRED" and requested_at and _utc(state["deferred_at"]) >= _utc(requested_at):
                    code = state.get("reason_code")
                    if code not in {"UPDATE_PREPARATION_FAILED", "UPDATE_EXISTING_WRITER"}:
                        code = "UPDATE_TERMINAL_RECOVERED"
                    return start_current(control, policy, state, active, arguments, code=code, deadline=relay_deadline, _ownership=bootstrap_ownership)
                if claim_status == "ABSENT":
                    writer_result = recovery_writer_result(control, policy, active, arguments, role=role, deadline=relay_deadline, state=state, claim_status=claim_status, _ownership=bootstrap_ownership)
                    if writer_result is not None:
                        return writer_result
                    if current_generation_status(control, policy, active["sha"]) != "ALLOW":
                        emit_progress_guidance()
                        return 20
                    return start_current(control, policy, state, active, arguments, code="UPDATE_SUPERVISION_UNAVAILABLE", report_once=True, deadline=relay_deadline, _ownership=bootstrap_ownership)
                if action == "START_CURRENT":
                    if state and state["phase"] in {"ROLLED_BACK", "DEFERRED"}:
                        return start_current(control, policy, state, active, arguments, code="UPDATE_TERMINAL_RECOVERED", deadline=relay_deadline, _ownership=bootstrap_ownership)
                    return start_current(control, policy, state, active, arguments,
                                         code="UPDATE_TERMINAL_RECOVERED", deadline=relay_deadline, _ownership=bootstrap_ownership)
                if state and state["phase"] in {"UNKNOWN", "FAILED"}:
                    if claim_status not in {"LIVE", "UNKNOWN"}:
                        writer_result = recovery_writer_result(control, policy, active, arguments, role=role, deadline=relay_deadline, state=state, claim_status=claim_status, _ownership=bootstrap_ownership)
                        if writer_result is not None:
                            return writer_result
                        if current_generation_status(control, policy, active["sha"]) == "ALLOW" and not prepared:
                            return start_current(control, policy, state, active, arguments, code="UPDATE_TERMINAL_RECOVERED", deadline=relay_deadline, _ownership=bootstrap_ownership)
                if action in {"REQUEST_APPLY", "REQUEST_TRIAL", "WAIT_FOR_RECOVERY", "WAIT_FOR_TRIAL"} and not requested:
                    if ((action == "REQUEST_APPLY" or (state and state.get("phase") == "DEFERRED"
                            and state.get("reason_code") == UPDATE_RELAY_STOP_REQUESTED))
                            and relay_stop_requested(policy)):
                        return start_current(control, policy, state, active, arguments,
                                             code=UPDATE_RELAY_STOP_REQUESTED, deadline=relay_deadline, _ownership=bootstrap_ownership)
                    requested_at = now_utc()
                    progress_deadline = time.monotonic() + APPLY_PROGRESS_TIMEOUT
                    try:
                        request_apply_task(control, policy)
                    except Exception:
                        if apply_started or apply_claim_status(control) in {"LIVE", "UNKNOWN"}:
                            requested = True
                            continue
                        if not state or state["phase"] in {"PREPARED", "CONFIRMED", "ROLLED_BACK", "DEFERRED", "FAILED", "UNKNOWN", "APPLIED_PENDING_READY"}:
                            if state and state["phase"] == "APPLIED_PENDING_READY":
                                writer_result = recovery_writer_result(control, policy, active, arguments, role=role, deadline=relay_deadline, state=state, claim_status=claim_status, _ownership=bootstrap_ownership)
                                if writer_result is not None:
                                    return writer_result
                            code = "UPDATE_SUPERVISION_UNAVAILABLE" if state and state["phase"] == "APPLIED_PENDING_READY" else "UPDATE_APPLY_REQUEST_FAILED"
                            return start_current(control, policy, state, active, arguments, code=code, deadline=relay_deadline, _ownership=bootstrap_ownership)
                        raise
                    requested = True
                    # The task may already have settled or switched this journal.
                    # Release ownership and reread it before creating a trial.
                    continue
                if action in {"REQUEST_TRIAL", "WAIT_FOR_TRIAL"}:
                    # The first launcher claims its own role; both roles retain
                    # durable-data checks and exactly one ordinary target relay.
                    with ExitStack() as trial_ownership:
                        trial_ownership.enter_context(update_lock(control, policy["operator_sid"], trial=True))
                        state = read_json(control / "transition.json")
                        claim = optional_record(Path(policy["staging_root"]) / "trial.json")
                        failure_path = Path(policy["staging_root"]) / "trial-failure.json"
                        failure = optional_record(failure_path)
                        if not (failure and failure.get("nonce") == state["nonce"]
                                and failure.get("transition_id") == state["transition_id"]):
                            observation = None
                            if claim and claim.get("nonce") == state["nonce"]:
                                # Only existing-claim readback leaves both locks.
                                # Initial absence proof and launch remain atomic;
                                # terminal READY also releases root (lane 22:0x).
                                expected_ready = optional_record(Path(policy["staging_root"]) / "ready.json")
                                expected_identity = native_process_identity(claim["pid"])
                                trial_ownership.close()
                                bootstrap_ownership.close()
                                observation = _read_bootstrap_trial_observation(control, policy, state, active, claim,
                                    identity=expected_identity)
                                bootstrap_ownership.enter_context(update_lock(control, policy["operator_sid"],
                                    abandoned_check=lambda: _verify_abandoned_update(control, policy)))
                                trial_ownership.enter_context(update_lock(control, policy["operator_sid"], trial=True))
                            failed_role = role
                            try:
                                if observation is not None:
                                    trial_observation_changed = not _commit_bootstrap_trial_observation(
                                        control, policy, state, active, claim, observation,
                                        expected_ready=expected_ready)
                                else:
                                    pid = _launch_bootstrap_version(control, active["sha"], startup_arguments, nonce=state["nonce"], deadline=relay_deadline.bounded_by(deadline))
                                    write_json_durable(Path(policy["staging_root"]) / "trial.json", {"pid": pid, "role": role, "nonce": state["nonce"], "transition_id": state["transition_id"], "claimed_at": now_utc()})
                                    failed_role = "relay"
                                    # LM's user settings select both children's data root.
                                    if role == "gui":
                                        _launch_bootstrap_version(control, active["sha"], ["--label-match-user-relay"], nonce=state["nonce"], deadline=relay_deadline.bounded_by(deadline))
                                if not trial_observation_changed:
                                    saw_trial = True
                            except _RelayPreparationUnavailable as error:
                                emit_support_record({"reason_code": error.code}, stage="update_apply")
                                return 20
                            except Exception as error:
                                write_json_durable(failure_path, {
                                    "nonce": state["nonce"], "transition_id": state["transition_id"],
                                    "sha": active["sha"], "failed_role": failed_role,
                                    "launch_failure_details": launch_failure_details(error, include_original=True),
                                })
                                # A failed start is not permission to change the pointer.
                                # SYSTEM must prove normal stop and previous-code trust.
                if not trial_observation_changed and requested and progress_deadline is not None:
                    claim_status = apply_claim_status(control)
                    if claim_status == "ABSENT":
                        if state and state["phase"] == "APPLIED_PENDING_READY":
                            writer_result = recovery_writer_result(control, policy, active, arguments, role=role, deadline=relay_deadline, state=state, claim_status=claim_status, _ownership=bootstrap_ownership)
                            if writer_result is not None:
                                return writer_result
                        return start_current(control, policy, state, active, arguments, code="UPDATE_SUPERVISION_UNAVAILABLE", report_once=True, deadline=relay_deadline, _ownership=bootstrap_ownership)
                    if claim_status in {"LIVE", "UNKNOWN"}:
                        apply_started = True
                    if not apply_started and time.monotonic() >= progress_deadline:
                        return start_current(control, policy, state, active, arguments, code="UPDATE_APPLY_NO_PROGRESS", deadline=relay_deadline, _ownership=bootstrap_ownership)
        except RuntimeError as error:
            if str(error) not in {'update mutex ownership was abandoned', 'update mutex ownership is busy'}:
                raise
            if str(error) == "update mutex ownership was abandoned":
                raise
        time.sleep(0.1)
    if waiting_for_decision and _bootstrap_awaiting_apply_decision(
            optional_record(control / "transition.json"), apply_claim_status(control)):
        emit_progress_guidance()
        return 20
    if apply_claim_status(control) == "UNKNOWN":
        try:
            with ExitStack() as bootstrap_ownership:
                bootstrap_ownership.enter_context(update_lock(control, policy["operator_sid"],
                    abandoned_check=lambda: _verify_abandoned_update(control, policy)))
                try:
                    stale_claim = time.time() - (control / "apply-claim.json").stat().st_mtime > 15 * 60
                except OSError:
                    stale_claim = False
                if stale_claim and apply_claim_status(control) == "UNKNOWN":
                    state = optional_record(control / "transition.json")
                    active = read_json(control / "active.json")
                    # Stale-owner recovery shares the original relay deadline;
                    # finishing the apply wait cannot issue a fresh budget.
                    writer_result = recovery_writer_result(control, policy, active, arguments,
                        role=role, deadline=relay_deadline, state=state, claim_status="UNKNOWN", _ownership=bootstrap_ownership)
                    if writer_result is not None:
                        return writer_result
                    if current_generation_status(control, policy, active["sha"]) == "ALLOW" and not prepared_pending(control, policy, state):
                        return start_current(control, policy, state, active, arguments, code="UPDATE_SUPERVISION_UNAVAILABLE", report_once=True, deadline=relay_deadline, _ownership=bootstrap_ownership)
        except RuntimeError as error:
            if str(error) not in {'update mutex ownership was abandoned', 'update mutex ownership is busy'}:
                raise
    active = read_json(control / "active.json")
    if current_generation_status(control, policy, active["sha"]) != "ALLOW":
        emit_progress_guidance()
        return 20
    if role == "gui" and gui_recovery_state(optional_record(control / "transition.json"),
                                            apply_claim_status(control)):
        with ExitStack() as bootstrap_ownership:
            bootstrap_ownership.enter_context(update_lock(control, policy["operator_sid"],
                abandoned_check=lambda: _verify_abandoned_update(control, policy)))
            state = optional_record(control / "transition.json")
            claim_status = apply_claim_status(control)
            active = read_json(control / "active.json")
            if gui_recovery_state(state, claim_status):
                writer_result = recovery_writer_result(control, policy, active, arguments,
                    role=role, deadline=relay_deadline, state=state, claim_status=claim_status, _ownership=bootstrap_ownership)
                if writer_result is not None:
                    return writer_result
    assert_ordinary_recovery(control, policy, active["sha"], now_utc())
    result = start_current(control, policy, optional_record(control / "transition.json"), active,
                           arguments, code="UPDATE_APPLY_NO_PROGRESS", deadline=relay_deadline)
    emit_support_record({"reason_code": "UPDATE_APPLY_NO_PROGRESS", "exit_code": result}, stage="update_apply", policy=policy)
    return result


def main(arguments: list[str] | None = None) -> int:
    import argparse
    import sys

    parser = __import__("failure_redaction").InstallerArgumentParser()
    parser.add_argument("--bootstrap", action="store_true")
    parser.add_argument("--apply-prepared", action="store_true")
    parser.add_argument("--control-root", type=Path)
    parser.add_argument("--role", choices=("gui", "relay"), default="gui")
    parser.add_argument("--relay-parent-pid", nargs="?", const="")
    parser.add_argument("--relay-parent-deadline", nargs="?", const="")
    parser.add_argument("--print-logon-task", action="store_true")
    parser.add_argument("--install-root", type=Path)
    parser.add_argument("--operator-sid")
    parser.add_argument("arguments", nargs=argparse.REMAINDER)
    # argparse otherwise treats signed nonfinite numbers as new options. Join
    # only this option's value, leaving other options and the remainder intact.
    arguments = list(sys.argv[1:] if arguments is None else arguments)
    position = 0
    while position < len(arguments) and arguments[position] != "--":
        optional = parser._parse_optional(arguments[position])
        if isinstance(optional, list):
            if len(optional) != 1:
                break
            optional = optional[0]
        if optional is None or optional[0] is None:
            break
        action, option, *details = optional
        explicit = details[-1]
        position += 1
        if action.nargs == 0 or explicit is not None or position == len(arguments):
            continue
        value = arguments[position]
        if option == "--relay-parent-deadline" and value.casefold() in {"-inf", "-infinity", "-nan"}:
            arguments[position - 1:position + 1] = [option + "=" + value]
        elif value == "--":
            break
        elif parser._parse_optional(value) is None:
            position += 1
    options = parser.parse_args(arguments)
    has_parent_context = options.relay_parent_pid is not None or options.relay_parent_deadline is not None
    if has_parent_context and not options.bootstrap:
        emit_support_record({"reason_code": "UPDATE_PREPARATION_FAILED"}, stage="update_apply")
        return 20
    if options.print_logon_task:
        if options.apply_prepared or options.bootstrap or options.arguments or options.install_root is None:
            raise ValueError("logon task generation arguments differ")
        print(json.dumps(install_task_spec(options.install_root, options.operator_sid)))
        return 0
    if options.apply_prepared:
        if options.bootstrap or options.arguments or options.control_root is None:
            raise ValueError("system updater arguments differ")
        expected = Path(__file__).resolve().parents[2] / "update-control"
        if options.control_root.absolute() != expected:
            raise ValueError("system updater control root differs")
        return apply_prepared(expected)
    if options.bootstrap:
        args = options.arguments[1:] if options.arguments[:1] == ["--"] else options.arguments
        if options.role == "relay":
            from current_user_onboarding import _machine_worker_context
            _machine_worker_context(Path(__file__).resolve().parent)
        if control_root(Path(__file__).resolve().parent) is None:
            if has_parent_context:
                emit_support_record({"reason_code": "UPDATE_PREPARATION_FAILED"}, stage="update_apply")
                return 20
            if options.role == "relay":
                from label_match_relay_entry import main as relay_main
                return relay_main(args)
            from Label_Match import main as application_main

            return int(application_main(args) or 0)
        if has_parent_context:
            return bootstrap(Path(__file__).resolve().parent, args, role=options.role,
                             parent_deadline=options.relay_parent_deadline,
                             parent_pid=options.relay_parent_pid)
        return bootstrap(Path(__file__).resolve().parent, args, role=options.role)
    # Source/old-layout launch remains the ordinary portable main entrypoint.
    from Label_Match import main as application_main

    return int(application_main(options.arguments) or 0)


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as error:
        __import__("failure_redaction").emit_support_failure(error)
        raise SystemExit(1)
