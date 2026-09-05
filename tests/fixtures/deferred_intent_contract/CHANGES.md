# Reconstructed shared contract, revision 2

The seq292 originals were lost on 2026-09-06. Revision 1 reproduced Rework's smaller model, but Label and Defect rejected its incomplete shared schema. Revision 2 was reconciled read-only against Label's capture and lease-clock tests, Defect's capture model, real cross-application harness and strictness tests, and Rework's capture/recovery tests. No matching deferred-intent/golden-vector consumer was found in Container_Audit. These are new reviewed reconstruction bytes, not recovery of the deleted original.

| Field | Revision 2 contract |
|---|---|
| `contract_version`, `payload_protection` | Retained v1.1, V2 current-user raw canonical payload and derived entropy declarations; V1 remains ambiguous for common readers. |
| `vector_schema_version` | Restored the schema identifier reported by Defect's conformance receipt; `kmtech.deferred-intent.golden-vectors.v1` names the preserved shared grammar, while provenance separately identifies reconstruction revision 2. |
| `states` | Added all 17 source states; state and destination membership is checked by existing consumers. |
| `terminal_states` | Added COMPLETED, CANCELLED and SUPERSEDED. |
| `operation_vocabulary` | Added the 19 operations recognized by Label and Defect, including capture, validation, materialization, submission, readback, cancellation, crash, expiry and forbidden-transition probes. |
| `transition_codes` | Retained the 27 source SQL audit codes, including T1D_DUPLICATE_SUPPRESSED. |
| `allowed_edges` | Added the shared state-edge map, with empty lists for terminal states. Allowed edges do not replace evidence, ownership or lease checks. |
| `allowed_state_edges` | Retained revision 1's source-extraction alias; consumers should use `allowed_edges`. |
| `default_effect_counters` | Retained existing counters and added required `downstream_outbox_rows`. It and `outbox_rows` are equal aliases for one physical downstream-outbox count; matching effect deltas increment both once. The actual Label source requires `downstream_outbox_rows`; both spellings are preserved to avoid another rename. |
| `validation_outcome_destinations` | Retained the seven validation classifications and source-compatible destination states. |
| `reconciliation_outcome_destinations` | Retained exact/missing/mismatch/unavailable readback outcomes for validation and submit phases. Unknown replies never authorize blind resubmission. |
| `consumer_profile` | Declares the existing 15 capture/storage scenarios and 23 real actions per implementation (30 scenarios/46 actions across Label and Defect); full-engine actions remain outside the capture-only harness. |
| `cases[].initial` | Retains ordinary initial state and supports the existing `intents` record list, partition ordering and fence values. The no-authoritative-effect scenario also preserves inventory, completion and release metrics. |
| `cases[].actions[].target` | Corrected to an intent record key, default `main`; FIFO can address `later`. It no longer names a destination state. |
| `cases[].actions[].to` | Added the proposed state for forbidden-transition probes. |
| `cases[].actions[].cut_point` | Restored `after_begin_before_intent_insert` and `after_commit_before_ui_ack`, so the real storage harness executes both crash boundaries. |
| Conflict expectation | Restored `DUPLICATE_IDENTITY_PAYLOAD_MISMATCH` with retained original ciphertext. |
| Legacy handoff | Restored exact `downstream_outbox_ref` and `LEGACY_PATH_OWNS_SUBMISSION` audit reason in the same transaction as the outbox handoff. |
| `cases[].final_expect` | Adds final main-record state and preserved domain metrics; all declared counters remain checked. |
| Capture scenario IDs | All eleven IDs explicitly enumerated by Rework/Defect and the Label required subset are unchanged, including the historical MEASURED names. Those names describe synthetic specification scenarios, not new measurement claims. |
| Remaining scenarios | Corrected FIFO to use two ordered records and stale-worker fencing to expire/reclaim a lease. Four actual SQL guard scenarios cover dependency/retry-to-submit and unknown-submit repost/cancel; unsupported revision 1 probes that confused state-edge permission with lease evidence were replaced. Total remains 35 distinct scenarios. |
| `canonicalization_vectors`, `entropy_vectors` | Retained fixed UTF-8 and V2 entropy specimens checked against the runtime. |
| `case_sha256`, `vector_body_sha256` | Recomputed for corrected semantic bodies; provenance timestamps remain outside those bodies. |
| `provenance` | Adds reconstruction revision 2, a new explicit generation time, generator hash and refreshed exact source hashes. The source base commit remains distinguished from the modified generation inputs. |

B1 safety correction is included in the source contract: every pre-guard capture, including malformed input, retains its recovery/review barrier unless the existing authenticated resolution route establishes safety. Barcode shape and zero deferred counters are insufficient. New guarded malformed captures with no started effects can still be safely cancelled, and new invalid scans are rejected before capture.

The committed consumer-model fixtures are exact source extractions with hashes, not rewritten assertions. Shared revision 1 bytes remain preserved; revision 2 is published separately under `deferred-intent-20260906-v2` after compatibility verification. New pins are recorded in the repository overlay manifest and the shared SHA256SUMS file.
