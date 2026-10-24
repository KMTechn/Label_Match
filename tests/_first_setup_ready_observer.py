"""Observe original READY calls and their executed rejection sites; never reread state."""
import ast
import functools
import inspect
import json
import os
from pathlib import Path
import sys
import textwrap
import time


REJECTIONS = {
    "_relay_ready_receipt": (
        "ORDINARY_CONTEXT_AND_PID", "ACTIVE_CODE_BINDING", "RELAY_CLAIM_BINDING",
        "SINGLE_NATIVE_RELAY_AND_CLAIM_TIME",
    ),
    "_trial_ready_receipt": (
        "ROLE_PID_AND_TRIAL_BINDING", "ORDINARY_CONTEXT_AND_ONBOARDING_READY",
        "DURABLE_QUEUE_EXISTS", "DURABLE_QUEUE_QUICK_CHECK", "DURABLE_QUEUE_SQLITE_ERROR",
        "CURRENT_SET_VERIFIED", "RELAY_RUNNING_BUILD_NONCE_AND_USER",
        "RELAY_PID_AND_FRESH_STATUS",
    ),
    "_retained_relay_ready_receipt": (
        "RETAINED_RELAY_CLAIM", "RETAINED_NATIVE_BINDING_AND_BUILD", "RETAINED_SINGLE_RELAY",
    ),
}


def rejection_sites(function):
    source, start = inspect.getsourcelines(inspect.unwrap(function))
    tree = ast.parse(textwrap.dedent("".join(source)))
    returns = sorted((node for node in ast.walk(tree) if isinstance(node, ast.Return)
                      and isinstance(node.value, ast.Constant) and node.value.value is False),
                     key=lambda node: node.lineno)
    names = REJECTIONS[function.__name__]
    assert len(returns) == len(names), "READY observer must match the original rejection sites"
    return {start + node.lineno - 1: name for node, name in zip(returns, names)}


def local_checks(name, values, updates):
    """Only compare inputs already present in this invocation's Python frame."""
    state = values.get("state") or {}
    pid = values.get("pid")
    checks = {"pid_positive_integer": type(pid) is int and pid > 0}
    if name in {"_relay_ready_receipt", "_retained_relay_ready_receipt"}:
        claim = values.get("claim") or {}
        if "claim" in values:
            checks.update(claim_present=bool(claim), claim_role_relay=claim.get("role") == "relay",
                claim_pid_matches=claim.get("pid") == pid,
                claim_nonce_matches=claim.get("nonce") == state.get("nonce"),
                claim_transition_matches=claim.get("transition_id") == state.get("transition_id"))
        if "relays" in values:
            relays = values["relays"]
            checks["exactly_one_matching_relay"] = len(relays) == 1
            if len(relays) == 1:
                checks["matching_relay_pid"] = relays[0].get("pid") == pid
                checks["relay_created_no_later_than_claim"] = (
                    updates._utc(relays[0].get("created_at")) <= updates._utc(claim.get("claimed_at")))
        if "binding" in values:
            checks["retained_native_binding_proven"] = values["binding"] is not None
    else:
        role = values.get("role")
        checks.update(role_supported=role in {"gui", "relay"},
            trial_pid_matches=state.get("trial_pid") is None or state.get("trial_pid") == pid,
            trial_role_matches=state.get("trial_pid") is None or state.get("trial_role", "gui") == role)
        if "relay" in values:
            relay = values["relay"] or {}
            checks.update(relay_present=bool(relay), relay_running=relay.get("status") == "RUNNING",
                relay_build_matches=relay.get("source_commit") == values.get("sha"),
                relay_nonce_matches=relay.get("update_nonce") == state.get("nonce"),
                relay_ordinary_user=relay.get("ordinary_user") is True,
                relay_pid_matches=relay.get("process_id") == pid)
            if relay and role == "relay":
                boundary = state.get("trial_started_at") or state.get("rollback_at") or state.get("applied_at")
                try:
                    checks["relay_status_at_or_after_trial"] = (
                        updates._utc(relay.get("updated_at")) >= updates._utc(boundary).replace(microsecond=0))
                except (TypeError, ValueError):
                    checks["relay_status_time_parseable"] = False
    return checks


def observed_receipt(original, updates, emit):
    targets = {inspect.unwrap(getattr(updates, name)).__code__: name for name in REJECTIONS}
    sites = {name: rejection_sites(getattr(updates, name)) for name in REJECTIONS}
    helper_names = ("ordinary_user_context", "version_manifest", "read_json", "process_matches_trial")
    helpers = {inspect.unwrap(getattr(updates, name)).__code__: name for name in helper_names}
    onboarding = sys.modules.get("current_user_onboarding")
    if onboarding is not None:
        helpers[inspect.unwrap(onboarding.inspect_current_user_state).__code__] = "onboarding_state"
    serial = 0

    @functools.wraps(original)
    def observed(*args, **kwargs):
        nonlocal serial
        serial += 1
        began = time.monotonic()
        record = {"iteration": serial, "monotonic": began, "checks": {}, "rejections": []}
        previous = sys.gettrace()

        def trace(frame, event, argument):
            delegated = previous(frame, event, argument) if previous is not None else None
            code = frame.f_code
            if code not in targets and code not in helpers:
                return delegated

            def local(current, kind, result):
                nonlocal delegated
                if delegated is not None:
                    delegated = delegated(current, kind, result)
                if kind != "return":
                    return local
                try:
                    name = targets.get(code)
                    values = current.f_locals
                    if name is not None:
                        record["checks"].update(local_checks(name, values, updates))
                        state = values.get("state") or {}
                        if (state.get("phase") in {"APPLIED_PENDING_READY", "ROLLBACK_PENDING_READY"}
                                or (state.get("phase") == "DEFERRED" and state.get("retained_candidate") is True)):
                            record["phase"] = state["phase"]
                            boundary = state.get("rollback_at") if state["phase"] == "ROLLBACK_PENDING_READY" else state.get("applied_at")
                            record["window_elapsed_seconds"] = time.time() - updates._utc(boundary).timestamp()
                        if result is False:
                            reason = sites[name].get(current.f_lineno)
                            if reason is None and name == "_relay_ready_receipt" and record["rejections"]:
                                reason = "TRIAL_RECEIPT_REJECTED"
                            record["rejections"].append({"function": name, "line": current.f_lineno,
                                "reason_code": reason or "UNMAPPED_RETURN",
                                "checks": local_checks(name, values, updates)})
                    elif helpers[code] == "ordinary_user_context":
                        record["checks"]["ordinary_user_context"] = result is True
                    elif helpers[code] == "onboarding_state":
                        record["checks"]["onboarding_state_ready"] = result.get("status") == "READY"
                    elif helpers[code] == "process_matches_trial":
                        row, state = values["row"], values["state"]
                        boundary = (state["applied_at"] if values["role"] == "gui"
                                    and state["phase"] == "APPLIED_PENDING_READY"
                                    else state.get("rollback_at", state["applied_at"]))
                        checks = dict(result=result is True, observable=row.get("observable") is True,
                            build_matches=row.get("sha") == values["sha"],
                            sid_matches=row.get("sid") == values["sid"],
                            role_matches=row.get("role") == values["role"],
                            pid_positive_integer=type(row.get("pid")) is int and row["pid"] > 0,
                            ordinary_user=row.get("ordinary_user") is True,
                            created_at_or_after_trial=updates._utc(row.get("created_at")) >= updates._utc(boundary))
                        if (values["role"] == state.get("trial_role", "gui")
                                and state.get("trial_pid") == row.get("pid") and state.get("trial_started_at")):
                            checks["created_matches_trial_process"] = updates._utc(row.get("created_at")) == updates._utc(state["trial_started_at"])
                        record.setdefault("native_row_checks", []).append(checks)
                    elif (current.f_back is not None
                          and targets.get(current.f_back.f_code) in {"_relay_ready_receipt", "_retained_relay_ready_receipt"}):
                        caller = current.f_back.f_locals
                        if helpers[code] == "version_manifest" and "sha" in caller:
                            record["checks"]["code_build_matches_active_target"] = result.get("source_commit") == caller.get("sha")
                        elif helpers[code] == "read_json" and "sha" in caller:
                            record["checks"]["active_pointer_matches_target"] = result.get("sha") == caller["sha"]
                except Exception as error:
                    record["observation_error_type"] = type(error).__name__
                return local
            return local

        try:
            sys.settrace(trace)
            result = original(*args, **kwargs)
            record["outcome"] = ("RECEIPT" if isinstance(result, tuple) else
                                 "NO_RECEIPT_REQUIRED" if result is True else "REJECTED")
            return result
        except BaseException as error:
            record.update(outcome="RAISED", error_type=type(error).__name__)
            raise
        finally:
            sys.settrace(previous)
            record["elapsed_seconds"] = time.monotonic() - began
            try:
                emit(record)
            except Exception:
                pass
    return observed


def install(root, updates):
    destination = Path(root) / "ready-receipt-observations" / (str(os.getpid()) + ".jsonl")
    destination.parent.mkdir(parents=True, exist_ok=True)

    def emit(record):
        with destination.open("a", encoding="utf-8") as stream:
            stream.write(json.dumps(record, sort_keys=True) + "\n")

    updates._relay_ready_receipt = observed_receipt(updates._relay_ready_receipt, updates, emit)
