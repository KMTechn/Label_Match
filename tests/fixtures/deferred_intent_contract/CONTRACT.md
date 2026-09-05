# Deferred intent contract kmtech.deferred-intent.v1.1 — reconstructed 2026-09-06

Generator: `rework_worker/shared_runtime_overlays/generate_deferred_intent_contract.py`; SHA256 `7a073b44a909f1fedb4558e7c2f2bf556415f35a4cd3a2ff6c8d5a08a3123284`.
Source base commit: `cd9e03e45d830f166911f8c051c22ea428c31917`. Exact generation inputs are the reviewed rw13 worktree hashes below, not a claim that this dirty increment existed in that base commit.
Generation time: `2026-09-06T05:54:44+09:00` (explicit reproducible input).
The seq292 originals were lost on 2026-09-06 during storage cleanup. Main authorized recreation and in-repository vendoring. These files carry NEW pins; neither byte-exact restoration nor fresh native measurement is claimed. Historical MEASURED scenario IDs are preserved as names for synthetic specification cases.

## Durable capture boundary

Commit one encrypted, authenticated operator intent before the first external request. Capture success means local durable acceptance only, never business completion. A failed commit or disk-full condition must not report saved. A crash before commit leaves no intent; a crash after commit before UI acknowledgement preserves one recoverable intent. Equal capture identity and payload converge; conflicting payload is blocked. Captures produce no authoritative product/ledger/outbox/dashboard or server result. The registered legacy handoff supersedes the capture atomically with its local result and outbox so only one submission owner remains.

Bind producer/install, source host, manifest, authority scope, partition sequence, capture key and payload hash into the current-user seal. Copying or changing those bindings must fail owned-payload verification. Canonical payloads use `rfc8785-jcs-v1`, schema 1, restricted to supported JSON integer/string/bool/null/list/object values; floats and non-string object keys fail closed. UTF-8, sorted keys, no whitespace and no ASCII escaping determine bytes.

Emit `WIN_DPAPI_CURRENT_USER_V2` with RAW_CANONICAL_BYTES plaintext and no alternate envelope. V2 optional entropy is SHA256 of canonical JSON containing app_id, authority_scope_id, capture_key, contract_version, intent_kind, producer_install_id and purpose `kmtech.deferred-intent.payload-protection.v2`. Common readers refuse ambiguous V1 records; owning historical decoders retain responsibility for them. The reconstructed canonicalization and entropy vectors are fixed synthetic specimens, not ciphertext or credentials.

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
| CAPTURED_UNVERIFIED | VALIDATING, BLOCKED_INVALID, CANCELLED, SUPERSEDED |
| VALIDATING | VALIDATED, RETRY_WAIT_VALIDATION, WAITING_DEPENDENCY, BLOCKED_INVALID, RECONCILE_PENDING_VALIDATION, OPERATOR_REVIEW |
| RETRY_WAIT_VALIDATION | VALIDATING, CANCELLED, SUPERSEDED, OPERATOR_REVIEW |
| WAITING_DEPENDENCY | VALIDATING, CANCELLED, SUPERSEDED, OPERATOR_REVIEW |
| BLOCKED_INVALID | CANCELLED, SUPERSEDED, OPERATOR_REVIEW |
| RECONCILE_PENDING_VALIDATION | VALIDATING, BLOCKED_INVALID, OPERATOR_REVIEW |
| VALIDATED | READY_TO_SUBMIT, RETRY_WAIT_VALIDATION, OPERATOR_REVIEW |
| READY_TO_SUBMIT | SUBMITTING, RETRY_WAIT_VALIDATION, OPERATOR_REVIEW |
| SUBMITTING | ACKED, RETRY_WAIT_SUBMIT, RECONCILE_PENDING_SUBMIT, OPERATOR_REVIEW, SUPERSEDED |
| RETRY_WAIT_SUBMIT | SUBMITTING, OPERATOR_REVIEW |
| RECONCILE_PENDING_SUBMIT | ACKED, RETRY_WAIT_SUBMIT, OPERATOR_REVIEW |
| ACKED | LOCAL_EFFECT_PENDING, COMPLETED, OPERATOR_REVIEW |
| LOCAL_EFFECT_PENDING | COMPLETED, OPERATOR_REVIEW |
| OPERATOR_REVIEW | CANCELLED, SUPERSEDED |

Audit vocabulary (27 codes): `T1_CAPTURE`, `T1D_DUPLICATE_SUPPRESSED`, `T2_CLAIM_VALIDATION`, `T3_LOCAL_INTEGRITY`, `T4_LOCAL_INVALID`, `T5_VALIDATE_PLAN`, `T5A_RECORD_MUTATION_ATTEMPT`, `T6_VALIDATION_RETRY`, `T6A_VALIDATION_UNKNOWN`, `T6B_VALIDATION_RECONCILED`, `T6C_VALIDATION_MISMATCH`, `T7_CLASSIFY_ABSENCE`, `T8_MATERIALIZE_DEPENDENCY`, `T9_WAIT_DEPENDENCY`, `T10_REJECT_OR_REVIEW`, `T11_FREEZE_VALIDATION`, `T12_MATERIALIZE_COMMAND`, `T13_REVALIDATE_BEFORE_COMMAND`, `T14_CLAIM_SUBMIT`, `T15_CLASSIFY_SUBMIT`, `T16_SUBMIT_UNKNOWN`, `T17_SUBMIT_RECONCILED`, `T18_APPLY_LOCAL_EFFECT`, `T19_COMPLETE`, `TC_CANCEL`, `TS_SUPERSEDE`, `TR_RETRY_AUDIT`.

## Reproduction and evidence limits

From the repository root, run:

```powershell
python -B rework_worker/shared_runtime_overlays/generate_deferred_intent_contract.py --source-commit cd9e03e45d830f166911f8c051c22ea428c31917 --generation-time 2026-09-06T05:54:44+09:00 --check
```

Omit `--check` only to intentionally regenerate the two files for a reviewed source change, then update consumer pins. `--output-dir` writes an independent reproduction directory. The generator uses AST/source inspection and the standard library without importing the app, invoking DPAPI, opening a database or contacting a server. Identical source and explicit provenance arguments produce identical UTF-8/LF bytes. It reconstructs 35 distinct cases: the eleven capture IDs already required by the consumer plus validation, reconciliation, local-effect, terminal, lease/FIFO and unknown-commit scenarios. These are specification traces; live SQLite/DPAPI/transport behavior is separately tested by the consumer's unchanged semantic tests and recovery tests. The generator does not manufacture a claim that old measured runs or their missing bytes were recovered.

Timestamp-free vector body SHA256: `a01566fc8dd8ec16144a4b7b5d9076ee24e35157b86858321831490660de6357`. Timestamps occur only in the provenance header, never in per-case bodies. Each case has its own SHA256 over canonical JSON excluding its case_sha256 field.

Exact source inputs:

- `rework_worker/deferred_intent_capture.py` SHA256 `798c2a58a6600466fa5ff682cdd8895ad6a37668bd80402394280277536d5cb9`
- `rework_worker/deferred_intent_recovery.py` SHA256 `50378914c4a54549e02041de8b285c4ca999e5a680379fcaaea9cf2ff4f435d3`
- `rework_worker/deferred_intent_recovery_schema.py` SHA256 `b637a4bbbf01ca264b1d2c2d9946037540f5d0d1347a6be861f00ba110e8d228`
