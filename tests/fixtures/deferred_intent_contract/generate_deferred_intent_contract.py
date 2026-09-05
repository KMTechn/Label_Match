"""Recreate the lost seq292 specification from the pinned Rework implementation.

Uses only the standard library and reads source with AST; no application import,
database, DPAPI, network or runtime side effect is needed. Output is UTF-8/LF.
The caller supplies provenance time explicitly, so repeating a command is exact.
"""
from __future__ import annotations

import argparse
import ast
from datetime import datetime
import hashlib
import json
from pathlib import Path
import re

GENERATOR = "rework_worker/shared_runtime_overlays/generate_deferred_intent_contract.py"
VECTOR_SCHEMA_VERSION = "kmtech.deferred-intent.golden-vectors.v1"
SOURCES = (
    "rework_worker/deferred_intent_capture.py",
    "rework_worker/deferred_intent_recovery.py",
    "rework_worker/deferred_intent_recovery_schema.py",
)
OUTPUT = "rework_worker/shared_runtime_overlays/deferred_intent_contract"
COUNTERS = (
    "captured_intent_rows", "server_requests", "server_mutations", "receipt_rows",
    "authoritative_local_writes", "outbox_rows", "downstream_outbox_rows", "dashboard_projection_rows", "ui_saved_signals",
)
OPERATIONS = (
    "capture_commit", "capture_duplicate", "capture_conflict", "capture_failure", "restart",
    "claim_validation", "validation_result", "materialization_dispatch", "materialization_readback",
    "materialize_command", "claim_submit", "submit_result", "reconciliation_readback",
    "apply_local_effect", "legacy_handoff", "cancel", "crash", "expire_claim", "attempt_transition",
)
VALIDATION_DESTINATIONS = {
    "VALID": "VALIDATED", "RETRYABLE_UNAVAILABLE": "RETRY_WAIT_VALIDATION",
    "REQUIRED_ABSENT": "WAITING_DEPENDENCY", "ABSENT_MATERIALIZABLE": "WAITING_DEPENDENCY",
    "INVALID": "BLOCKED_INVALID", "CONFLICT": "OPERATOR_REVIEW",
    "UNKNOWN_COMMIT": "RECONCILE_PENDING_VALIDATION",
}
RECONCILIATION_DESTINATIONS = {
    "validation": {"FOUND_EXACT": "VALIDATING", "NOT_FOUND": "VALIDATING",
                   "FOUND_MISMATCH": "OPERATOR_REVIEW", "READBACK_UNAVAILABLE": "RECONCILE_PENDING_VALIDATION"},
    "submit": {"FOUND_EXACT": "ACKED", "NOT_FOUND": "RETRY_WAIT_SUBMIT",
               "FOUND_MISMATCH": "OPERATOR_REVIEW", "READBACK_UNAVAILABLE": "RECONCILE_PENDING_SUBMIT"},
}


def canonical(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False).encode("utf-8")


def sha(data):
    return hashlib.sha256(data).hexdigest()


def literal(tree, name):
    for node in tree.body:
        if isinstance(node, ast.Assign) and any(isinstance(t, ast.Name) and t.id == name for t in node.targets):
            return ast.literal_eval(node.value)
    raise ValueError("source contract literal missing: " + name)


def source_contract(repo):
    source = (repo / SOURCES[0]).read_text(encoding="utf-8")
    tree = ast.parse(source)
    schema = literal(tree, "DEFERRED_INTENT_SCHEMA_SQL")
    codes_match = re.search(r"transition_code TEXT NOT NULL CHECK \(transition_code IN \((.*?)\)\)", schema, re.S)
    if codes_match is None:
        raise ValueError("transition code constraint missing")
    codes = re.findall(r"'([^']+)'", codes_match.group(1))
    edges = {state: re.findall(r"'([^']+)'", destinations) for state, destinations in
             re.findall(r"OLD.state='([^']+)' AND NEW.state IN \(([^)]+)\)", literal(tree, "_RECOVERY_STATE_EDGE_TRIGGER_SQL"))}
    return {"contract_version": literal(tree, "CONTRACT_VERSION"),
            "payload_protection": literal(tree, "PAYLOAD_PROTECTION"),
            "entropy_purpose": literal(tree, "DPAPI_V2_ENTROPY_PURPOSE"),
            "capture_c14n_version": literal(tree, "CAPTURE_C14N_VERSION"),
            "capture_schema_version": literal(tree, "CAPTURE_SCHEMA_VERSION"),
            "transition_codes": codes, "allowed_state_edges": edges}


def action(op, state, *, effects=None, accepted=None, **inputs):
    expected = {"state": state}
    if effects:
        expected["effects_delta"] = dict(effects)
        if "outbox_rows" in effects:
            expected["effects_delta"]["downstream_outbox_rows"] = effects["outbox_rows"]
    if accepted is not None:
        expected["accepted"] = accepted
    return {"op": op, **inputs, "expect": expected}


def case(identity, actions, *, state=None, initial=None, effects=None, premise=""):
    initial = dict(initial) if initial is not None else {"state": state}
    final = dict.fromkeys(COUNTERS, 0)
    final["captured_intent_rows"] = len(initial.get("intents", [])) or int(initial.get("state") is not None)
    main_state = next((row["state"] for row in initial.get("intents", []) if row["target"] == "main"), initial.get("state"))
    for item in actions:
        for key, delta in item["expect"].get("effects_delta", {}).items():
            final[key] += delta
        if item.get("target", "main") == "main":
            main_state = item["expect"]["state"]
    if effects is not None and final != effects:
        raise ValueError("independent final counters differ for " + identity)
    final["state"] = main_state
    final.update({key: initial[key] for key in ("inventory_quantity", "completion_count", "downstream_release_count") if key in initial})
    body = {"id": identity, "initial": initial, "actions": actions,
            "final_expect": final, "premise": premise}
    return {**body, "case_sha256": sha(canonical(body))}


def cases():
    captured = "CAPTURED_UNVERIFIED"
    capture = action("capture_commit", captured, effects={"captured_intent_rows": 1})
    full = [capture, action("claim_validation", "VALIDATING", accepted=True),
            action("validation_result", "VALIDATED", outcome="VALID"),
            action("materialize_command", "READY_TO_SUBMIT"), action("claim_submit", "SUBMITTING"),
            action("submit_result", "RECONCILE_PENDING_SUBMIT", outcome="UNKNOWN_COMMIT",
                   effects={"server_requests": 1, "server_mutations": 1}),
            action("reconciliation_readback", "ACKED", phase="submit", outcome="FOUND_EXACT", effects={"receipt_rows": 1}),
            action("apply_local_effect", "COMPLETED", outcome="APPLIED", effects={"authoritative_local_writes": 1, "outbox_rows": 1})]
    result = [
        case("MEASURED_LABEL_CLOSED_PORT_CAPTURE_PRESERVES_SCAN", [capture, action("restart", captured)],
             premise="Historical scenario identifier retained; synthetic specification, not a new physical measurement. Remote port is unavailable after durable capture."),
        case("MEASURED_DEFECT_OFFLINE_QUEUE_ONLINE_FLUSH_EXACT_RECEIPT", full,
             premise="Historical scenario identifier retained; synthetic specification. One remote commit loses its reply, then exact readback enables one local effect."),
        case("CRASH_BEFORE_CAPTURE_COMMIT", [action("crash", None, cut_point="after_begin_before_intent_insert"), action("restart", None)],
             premise="SQLite capture transaction never committed; no saved acknowledgement."),
        case("CRASH_AFTER_CAPTURE_COMMIT_BEFORE_UI_ACK", [capture, action("crash", captured, cut_point="after_commit_before_ui_ack"), action("restart", captured)]),
        case("DUPLICATE_CAPTURE_SAME_PAYLOAD_CONVERGES", [capture, action("capture_duplicate", captured, accepted=True)]),
        case("DUPLICATE_CAPTURE_DIFFERENT_PAYLOAD_BLOCKS", [capture,
             {**action("capture_conflict", "BLOCKED_INVALID"), "expect": {"state": "BLOCKED_INVALID", "error_code": "DUPLICATE_IDENTITY_PAYLOAD_MISMATCH"}}]),
        case("FORBID_CAPTURED_TO_SUBMIT_FAMILY", [action("attempt_transition", captured, accepted=False, target="main", to="SUBMITTING")], state=captured),
        case("CAPTURE_ONLY_NO_AUTHORITATIVE_LEAK", [capture],
             initial={"state": None, "inventory_quantity": 7, "completion_count": 3, "downstream_release_count": 2},
             premise="No products, ledger event, outbox, dashboard or server success may be inferred from capture."),
        case("CAPTURE_ONLY_ONLINE_LEGACY_HANDOFF_PREVENTS_FUTURE_DUPLICATE", [capture,
             {**action("legacy_handoff", "SUPERSEDED", effects={"authoritative_local_writes": 1, "outbox_rows": 1},
                       downstream_outbox_ref="conformance-outbox:legacy-1"),
              "expect": {"state": "SUPERSEDED", "audit_reason": "LEGACY_PATH_OWNS_SUBMISSION",
                         "effects_delta": {"authoritative_local_writes": 1, "outbox_rows": 1, "downstream_outbox_rows": 1}}},
             action("claim_validation", "SUPERSEDED", accepted=False)],
             premise="Legacy result, outbox and supersede audit are in one SQLite transaction; legacy outbox owns submission."),
        case("CAPTURE_ONLY_ROW_IS_FORWARD_COMPATIBLE_WITH_FULL_ENGINE", [capture, action("claim_validation", "VALIDATING", accepted=True)]),
        case("CAPTURE_DISK_FULL_NEVER_REPORTS_SAVED", [action("capture_failure", None, outcome="SQLITE_FULL")],
             premise="Disk-full commit failure produces no saved UI signal or durable row."),
    ]
    for outcome, destination in VALIDATION_DESTINATIONS.items():
        result.append(case("VALIDATION_" + outcome, [action("validation_result", destination, outcome=outcome)], state="VALIDATING"))
    for phase, outcomes in RECONCILIATION_DESTINATIONS.items():
        for outcome, destination in outcomes.items():
            result.append(case("RECONCILIATION_" + phase.upper() + "_" + outcome,
                [action("reconciliation_readback", destination, phase=phase, outcome=outcome)],
                state="RECONCILE_PENDING_" + phase.upper(),
                premise="Readback reconciles the original immutable request; exact identity is required and no blind POST is allowed."))
    result.extend([
        case("LOCAL_EFFECT_APPLIES_ONCE", [action("apply_local_effect", "COMPLETED", outcome="APPLIED", effects={"authoritative_local_writes": 1, "outbox_rows": 1})], state="ACKED"),
        case("LOCAL_EFFECT_UNAVAILABLE_PRESERVES_ACK", [action("apply_local_effect", "LOCAL_EFFECT_PENDING", outcome="READBACK_UNAVAILABLE")], state="ACKED"),
    ])
    result.append(case("TERMINAL_COMPLETED_NEVER_RECLAIMED", [action("claim_validation", "COMPLETED", accepted=False)], state="COMPLETED"))
    result.append(case("FIFO_PREDECESSOR_BLOCKS_LATER_CAPTURE",
        [action("claim_validation", captured, target="later", accepted=False)],
        initial={"intents": [{"target": "main", "state": "WAITING_DEPENDENCY", "partition_key": "P1", "partition_seq": 1},
                             {"target": "later", "state": captured, "partition_key": "P1", "partition_seq": 2}]},
        premise="An unresolved earlier intent blocks validation of the later record in the same partition."))
    result.append(case("STALE_WORKER_FENCE_CANNOT_WRITE", [action("expire_claim", "RETRY_WAIT_VALIDATION"),
        {**action("claim_validation", "VALIDATING", accepted=True), "expect": {"state": "VALIDATING", "accepted": True, "fence": 2}},
        action("attempt_transition", "VALIDATING", accepted=False, target="main", to="VALIDATED", worker_fence=1)],
        initial={"state": "VALIDATING", "fence": 1}, premise="Lease expiry and reclaim replace fence 1 with fence 2; the old worker may not write."))
    for identity, state, destination in (
        ("FORBID_WAITING_DEPENDENCY_TO_SUBMIT", "WAITING_DEPENDENCY", "SUBMITTING"),
        ("FORBID_RETRY_VALIDATION_TO_SUBMIT", "RETRY_WAIT_VALIDATION", "SUBMITTING"),
        ("UNKNOWN_SUBMIT_NEVER_BLINDLY_REPOSTS", "RECONCILE_PENDING_SUBMIT", "SUBMITTING"),
    ):
        result.append(case(identity, [action("attempt_transition", state, accepted=False, target="main", to=destination)], state=state))
    result.append(case("UNKNOWN_SUBMIT_CANNOT_CANCEL", [action("cancel", "RECONCILE_PENDING_SUBMIT", accepted=False)], state="RECONCILE_PENDING_SUBMIT"))
    if len(result) != 35 or len({item["id"] for item in result}) != 35:
        raise ValueError("expected 35 distinct reconstructed conformance cases")
    return result


def capture_profile(rows):
    capture_ops = {"capture_commit", "capture_duplicate", "capture_conflict", "capture_failure", "legacy_handoff"}
    guards = {"attempt_transition", "cancel"}
    cuts = {"after_begin_before_intent_insert", "after_commit_before_ui_ack"}
    applicable = []
    for row in rows:
        operations = {action["op"] for action in row["actions"]}
        if (operations & capture_ops or operations <= guards
                or any(action["op"] == "crash" and action.get("cut_point") in cuts for action in row["actions"])):
            applicable.append(row)
    actions = sum(1 for row in applicable for action in row["actions"]
        if action["op"] in capture_ops | guards | {"restart"}
        or (action["op"] == "crash" and action.get("cut_point") in cuts)
        or (action["op"] == "claim_validation" and action["expect"]["state"] in {"COMPLETED", "CANCELLED", "SUPERSEDED"}))
    if len(applicable) != 15 or actions != 23:
        raise ValueError("shared consumer capture profile must retain 15 scenarios and 23 real storage actions")
    return {"capture_storage_cases_per_implementation": len(applicable), "capture_storage_actions_per_implementation": actions}


def generate(repo, *, source_commit, generation_time):
    if not re.fullmatch(r"[0-9a-f]{40}", source_commit):
        raise ValueError("source commit must be a full Git object identity")
    if datetime.fromisoformat(generation_time).tzinfo is None:
        raise ValueError("generation time must include a timezone")
    extracted = source_contract(repo)
    if len(extracted["transition_codes"]) != 27:
        raise ValueError("source transition vocabulary changed; review the generator")
    source_hashes = {name: sha((repo / name).read_bytes()) for name in SOURCES}
    provenance = {"generator": GENERATOR, "generator_sha256": sha(Path(__file__).read_bytes()),
                  "reconstruction_revision": 2,
                  "source_commit": source_commit, "generation_time": generation_time,
                  "source_sha256": source_hashes,
                  "source_state": "rw13 reviewed worktree based on source_commit; source_sha256 identifies exact inputs",
                  "reconstruction_notice": "seq292 originals were lost on 2026-09-06; this is a newly reconstructed specification with new pins, not byte recovery or new native measurement"}
    entropy_context = {"app_id": "rework", "authority_scope_id": "SCOPE-CONFORMANCE",
        "capture_key": "a" * 64, "contract_version": extracted["contract_version"],
        "intent_kind": "REWORK_GOOD_PRECLAIM", "producer_install_id": "install-conformance",
        "purpose": extracted["entropy_purpose"]}
    payload = {"product_barcode": "AAA0000000001R001", "operator_id": "검증작업자", "result": "GOOD"}
    vectors = {"provenance": provenance, "contract_version": extracted["contract_version"],
        "vector_schema_version": VECTOR_SCHEMA_VERSION,
        "payload_protection": {"emit": extracted["payload_protection"], "legacy_v1_status": "AMBIGUOUS_DO_NOT_EMIT",
                               "plaintext_envelope": "RAW_CANONICAL_BYTES", "entropy_derivation": "SHA256_RFC8785_JCS_CONTEXT_V1"},
        "states": list(extracted["allowed_state_edges"]) + ["COMPLETED", "CANCELLED", "SUPERSEDED"],
        "terminal_states": ["COMPLETED", "CANCELLED", "SUPERSEDED"], "operation_vocabulary": list(OPERATIONS),
        "transition_codes": extracted["transition_codes"], "allowed_state_edges": extracted["allowed_state_edges"],
        "allowed_edges": {**extracted["allowed_state_edges"], "COMPLETED": [], "CANCELLED": [], "SUPERSEDED": []},
        "default_effect_counters": dict.fromkeys(COUNTERS, 0),
        "validation_outcome_destinations": VALIDATION_DESTINATIONS,
        "reconciliation_outcome_destinations": RECONCILIATION_DESTINATIONS,
        "canonicalization_vectors": [{"id": "UTF8_INTEGER_STRING_SUBSET", "value": payload,
                                       "canonical_json": canonical(payload).decode("utf-8"), "sha256": sha(canonical(payload))}],
        "entropy_vectors": [{"id": "V2_CURRENT_USER_CONTEXT", "context": entropy_context, "sha256": sha(canonical(entropy_context))}],
        "cases": cases()}
    vectors["consumer_profile"] = capture_profile(vectors["cases"])
    for row in vectors["cases"]:
        for item in row["actions"]:
            state = item["expect"]["state"]
            if state is not None and state not in set(extracted["allowed_state_edges"]) | {"COMPLETED", "CANCELLED", "SUPERSEDED"}:
                raise ValueError("case destination absent from source state vocabulary: " + state)
    body_hash = sha(canonical({key: value for key, value in vectors.items() if key != "provenance"}))
    vectors["provenance"]["vector_body_sha256"] = body_hash
    vector_bytes = (json.dumps(vectors, ensure_ascii=False, sort_keys=True, indent=2) + "\n").encode("utf-8")
    contract = render_contract(extracted, provenance, body_hash)
    return {"CONTRACT.md": contract.encode("utf-8"), "golden-vectors.json": vector_bytes}


def render_contract(source, provenance, body_hash):
    edges = "\n".join(f"| {state} | {', '.join(destinations)} |" for state, destinations in source["allowed_state_edges"].items())
    hashes = "\n".join(f"- `{name}` SHA256 `{digest}`" for name, digest in provenance["source_sha256"].items())
    return f'''# Deferred intent contract {source['contract_version']} — reconstructed 2026-09-06

Generator: `{GENERATOR}`; SHA256 `{provenance['generator_sha256']}`.
Source base commit: `{provenance['source_commit']}`. Exact generation inputs are the reviewed rw13 worktree hashes below, not a claim that this dirty increment existed in that base commit.
Generation time: `{provenance['generation_time']}` (explicit reproducible input).
The seq292 originals were lost on 2026-09-06 during storage cleanup. Main authorized recreation and in-repository vendoring. These files carry NEW pins; neither byte-exact restoration nor fresh native measurement is claimed. Historical MEASURED scenario IDs are preserved as names for synthetic specification cases.

## Durable capture boundary

Commit one encrypted, authenticated operator intent before the first external request. Capture success means local durable acceptance only, never business completion. A failed commit or disk-full condition must not report saved. A crash before commit leaves no intent; a crash after commit before UI acknowledgement preserves one recoverable intent. Equal capture identity and payload converge; conflicting payload is blocked. Captures produce no authoritative product/ledger/outbox/dashboard or server result. The registered legacy handoff supersedes the capture atomically with its local result and outbox so only one submission owner remains.

Bind producer/install, source host, manifest, authority scope, partition sequence, capture key and payload hash into the current-user seal. Copying or changing those bindings must fail owned-payload verification. Canonical payloads use `{source['capture_c14n_version']}`, schema {source['capture_schema_version']}, restricted to supported JSON integer/string/bool/null/list/object values; floats and non-string object keys fail closed. UTF-8, sorted keys, no whitespace and no ASCII escaping determine bytes.

Emit `{source['payload_protection']}` with RAW_CANONICAL_BYTES plaintext and no alternate envelope. V2 optional entropy is SHA256 of canonical JSON containing app_id, authority_scope_id, capture_key, contract_version, intent_kind, producer_install_id and purpose `{source['entropy_purpose']}`. Common readers refuse ambiguous V1 records; owning historical decoders retain responsibility for them. The reconstructed canonicalization and entropy vectors are fixed synthetic specimens, not ciphertext or credentials.

## Recovery and effects

Claim only the earliest eligible nonterminal partition head with exact lease/fence/row-version CAS. WAITING_DEPENDENCY is preserved without silently skipping to later work. BLOCKED_INVALID and OPERATOR_REVIEW retain evidence. Validation freezes exact identity/snapshot before command materialization. Persist an idempotent validation mutation request and its immutable identity before dispatch; unknown outcomes use the same request's readback, including after lease takeover. A found-exact validation receipt returns to validation; it is not permission to submit an unfrozen command.

An immutable command and idempotency key precede SUBMITTING. Lost acknowledgement enters RECONCILE_PENDING_SUBMIT and permits readback, not a fresh POST. FOUND_EXACT requires exact bound receipt identity and permits ACKED; NOT_FOUND permits an ordinary retry of the same command/key; mismatch requires review and unavailable readback remains pending. Apply or reconcile one authoritative local effect/outbox only after exact acknowledgement. Do not call COMPLETED until the shared remote and local terminal conditions are both satisfied. Replaced worker leases cannot write, and retry exhaustion preserves reviewable intent rather than deleting it.

## Cancellation and ownership

Terminal rows are COMPLETED, CANCELLED and SUPERSEDED and are never reclaimed. General cancellation/supersession/reassignment requires the existing authenticated resolution evidence. The rw13 operator cancellation extension is limited to owned unsubmitted captures with no command, receipt, outbox, local effect, submission attempt, validation mutation, live lease or foreground dispatch marker. A new capture marks its foreground-guard capability in its capture audit; before any possible foreground mutation, FOREGROUND_DISPATCH_STARTED is durably audited. Every older capture without that capability, including malformed input, retains its recovery/review barrier: format and zero counters do not prove that no old foreground request was sent. Automatic malformed-head cancellation is restricted to a capture with the durable guard capability and no evidence of started effects. Uncertain history uses authenticated review/recovery and preserves Korean administrator-review guidance. Operator-cancelled captures retain payload/audit; an intentional new scan derives a new sealed capture identity.

## Source-enforced state edges

The shared JSON schema uses `states` (17 names), `terminal_states` (3 names), `operation_vocabulary` (19 operations), and `allowed_edges` (one entry per state, including empty terminal entries). `allowed_state_edges` is retained as the source-extraction alias; consumers use `allowed_edges`. An action's `target` is an intent record key such as `main` or `later`; `to` is the proposed destination for `attempt_transition`. `initial.intents` represents ordered records for FIFO scenarios. `outbox_rows` and `downstream_outbox_rows` are equal aliases for the same downstream outbox count, not two physical effects; the latter is required by the existing Label/Defect models. Crash actions preserve the recognized cut_point names, conflicting capture specifies DUPLICATE_IDENTITY_PAYLOAD_MISMATCH, and legacy_handoff supplies an exact downstream_outbox_ref plus LEGACY_PATH_OWNS_SUBMISSION audit reason. The capture/storage profile remains 15 scenarios and 23 real actions per implementation; full-engine actions are explicitly outside that profile. Existing consumer assertions are preserved.

Self transitions may record audit/retry evidence. The following directed edges are extracted from the current SQL guard. An allowed state edge is necessary but not sufficient: ownership, lease/fence, frozen evidence and transaction guards still apply.

| From | Allowed destinations |
|---|---|
{edges}

Audit vocabulary ({len(source['transition_codes'])} codes): {', '.join('`'+code+'`' for code in source['transition_codes'])}.

## Reproduction and evidence limits

From the repository root, run:

```powershell
python -B {GENERATOR} --source-commit {provenance['source_commit']} --generation-time {provenance['generation_time']} --check
```

Omit `--check` only to intentionally regenerate the two files for a reviewed source change, then update consumer pins. `--output-dir` writes an independent reproduction directory. The generator uses AST/source inspection and the standard library without importing the app, invoking DPAPI, opening a database or contacting a server. Identical source and explicit provenance arguments produce identical UTF-8/LF bytes. It reconstructs 35 distinct cases: the eleven capture IDs already required by the consumer plus validation, reconciliation, local-effect, terminal, lease/FIFO and unknown-commit scenarios. These are specification traces; live SQLite/DPAPI/transport behavior is separately tested by the consumer's unchanged semantic tests and recovery tests. The generator does not manufacture a claim that old measured runs or their missing bytes were recovered.

Timestamp-free vector body SHA256: `{body_hash}`. Timestamps occur only in the provenance header, never in per-case bodies. Each case has its own SHA256 over canonical JSON excluding its case_sha256 field.

Exact source inputs:

{hashes}
'''


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo-root", type=Path, default=Path(__file__).resolve().parents[2])
    parser.add_argument("--output-dir", type=Path)
    parser.add_argument("--source-commit", required=True)
    parser.add_argument("--generation-time", required=True)
    parser.add_argument("--check", action="store_true")
    args = parser.parse_args(argv)
    output = args.output_dir or args.repo_root / OUTPUT
    artifacts = generate(args.repo_root, source_commit=args.source_commit, generation_time=args.generation_time)
    if not args.check:
        output.mkdir(parents=True, exist_ok=True)
    for name, data in artifacts.items():
        target = output / name
        if args.check:
            if not target.is_file() or target.read_bytes() != data:
                raise SystemExit("generated contract differs: " + str(target))
        else:
            target.write_bytes(data)
    print(json.dumps({"output": str(output), "check": args.check,
                      "artifacts": {name: {"bytes": len(data), "sha256": sha(data)} for name, data in artifacts.items()}}, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
