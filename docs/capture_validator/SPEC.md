# M7 external capture bundle v1: reviewed reconstruction candidate

This is a reconstruction compatible with surviving v1 consumers, not recovered
original code. Historical equivalence and organizational approval are UNPROVEN.
The source references and decisions below are subject to independent review.
No production integration is authorized by this artifact.

## Established interface and interpretation

Python 3.11+ CLI: `python -B tools/validate_capture_bundle_v1.py ROOT_OR_INDEX
--app APP [--describe-json FILE]`. Directory and explicit immutable index forms
are supported. One ASCII-safe UTF-8 JSON object is emitted on stdout, ordinary
stderr is empty. `result`, integer `exit_code`, `summary` counts and `checks`
are exposed; each check has `status`, `reason_code`, `subject`, `detail`.
Exit 0/PASS requires FAIL=0 and APPROVAL_PENDING=0; exit 2/FAIL requires FAIL>0;
exit 3/APPROVAL_PENDING requires FAIL=0 and pending>0. Summary counts are derived
from emitted checks. Any real error takes precedence over pending organization.
Label publisher's existing exit-0-only acceptance remains unchanged. Defect's
materializer may accept structurally valid pending evidence.

Schema string is exactly `M7 external capture bundle v1`. App is the `app`
field, never `app_id`. App/source/tool identities are declarations: the validator
cannot prove source checkout correspondence or tool origin. Source commit/tree
and tool commit are lowercase 40-hex; tool blob digest and byte digests are
lowercase 64-hex. Portable artifacts may contain inert fixture bytes. Actual
artifact bytes must match the supplied digest. Capture tool paths match the
five surviving declarations; no tool file is imported or executed.

## Fixed registry (49 states)

Required sets are literal and independent of describe output. Complete reordered
lists are accepted; duplicates, missing and extra states fail. One capture per
state is required. The JSON key order is irrelevant.

| App | Required IDs | Capture tool path |
| --- | --- | --- |
| Container_Audit (9) | m7_phs2_preflight, m7_central_preflight_queue, m7_completion_busy, m7_recovery_transition, m7_direct_sync_backlog_ack, m7_exact_good_membership, m7_lease_fail_closed, m7_transfer_receipt_status, m7_partial_atomic_exchange | tools/capture_container_operator_ui.py |
| Defect_Inspection (4) | m7_scan_processing_input_disabled, m7_saved_pending_last_save, m7_converged_accepted_count_verify, m7_central_reject_quarantine | tools/capture_return_work_focus_ui.py |
| Inspection_worker (18) | inspection-waiting, inspection-normal, inspection-f12, inspection-residual, inspection-overflow, inspection-attachment, remnant, remnant-source, remnant-f12, remnant-attachment, exchange, inspection-completion, inspection-scan-processing-busy-undo, inspection-relay-operator-review, inspection-relay-failed-permanent, inspection-local-saved, inspection-final-good-hold, inspection-final-good-countdown | tools/capture_uiux_fullscreen.py |
| Rework_worker (9) | rework.processing, rework.durable_pending, rework.auto_recovery, rework.review.auto_recovery, rework.review.central_completion, rework.save_failure, rework.busy_rejection, rework.recovery_init_hard_block, rework.good_completion | scripts/capture_rework_process_uiux.py |
| Label_Match (9) | phs2_admitted_busy, phs2_rejected_input_preserved, f4_admitted_busy, f4_rejected_input_preserved, f3_admitted_busy, f3_rejected_input_preserved, central_submission_wait, central_submission_conflict, broken_fail_closed_warning | tools/capture_label_operator_ui.py |

## Document graph

All references are evidence-root-relative POSIX paths. Bundle B is
`<app>__<source-commit12>__<YYYYMMDDTHHMMSSZ>__<lowercase-hex-nonce8>`.
Its prefix P is `<app>/<B>`. The immutable index name is
`indexes/handover-index__<YYYYMMDDTHHMMSSZ>__<lowercase-hex-nonce8>.json`.
Index UTC may differ from bundle UTC. Dates must exist.

| Document/object | Required keys |
| --- | --- |
| index | schema, manifests |
| index manifest entry | app, manifest_file, manifest_sha256 |
| P/manifest.json | schema, app, app_source, portable_artifact, capture_tool, captures, approval |
| app_source | commit, tree |
| portable_artifact | file, sha256 |
| capture_tool | path, commit, blob_sha256 |
| capture row | state_id, viewport, dpi, generated_at, image_file, image_sha256, state_manifest_file, state_manifest_sha256 |
| viewport | width_px, height_px |
| P/states/STATE__WIDTHxHEIGHT__DPIdpi.json | schema, app, bundle_id, state_id, viewport, dpi, generated_at, image_file, image_sha256 |
| P/capture-set.json | schema, app, bundle_id, app_source, portable_artifact, capture_tool, captures |
| P/approval/approval-receipt.json | schema, app, bundle_id, capture_set_file, capture_set_sha256, approver |
| P/approval/custody-receipt.json | schema, app, bundle_id, capture_set_file, capture_set_sha256, approval_receipt_file, approval_receipt_sha256, custodian, custody_location, retention_period |
| manifest approval | approver, approval_receipt_file, approval_receipt_sha256, custody_receipt_file, custody_receipt_sha256 |
| optional describe JSON | schema, app, required_state_ids |

`app_specific` may optionally be an object in manifest, capture-set, state and
describe documents. It is diagnostic, never evidence of PASS. Manifest and
capture-set app_specific presence and values must agree. Other listed objects
use exact keys (decision D2 below).

Images occupy P/captures/STATE__WIDTHxHEIGHT__DPIdpi.png. Positive dimensions
and DPI are integers, not booleans. Decoded PNG dimensions must equal viewport
and filename. Tiny PNGs are allowed. `generated_at` is RFC3339 UTC ending Z,
fractional seconds allowed; its UTC second matches B. Capture rows bind actual
image/state bytes; state fields exactly repeat row values plus schema/app/B.
Capture-set duplicates every shared manifest field, including capture row order.
Both receipts bind capture-set bytes/path. Custody binds approval receipt bytes/path.
Manifest binds both receipts bytes/path and repeats approval approver. Index binds
manifest bytes/path. Hash raw file bytes, never reserialized JSON. This cannot
prove historical seal order; producer order tests remain separate obligations.

Within P the only directories are captures, states, approval, and the only files
are the exact listed graph nodes. Extra bundle files and nested directories fail.
Other bundles/artifacts/index files may exist in the evidence root.

## Organization

The exact placeholder `미정 — 조직 확정 필요(Q1)` yields one pending check at each
of manifest.approval.approver, approval-receipt.approver, custody-receipt.custodian,
custody-receipt.custody_location and custody-receipt.retention_period. Thus an all
placeholder bundle has five `ORGANIZATION_PLACEHOLDER` checks. Fields must be
nonempty strings; resolved fixture strings permit structural PASS only. The
validator makes no assertion that a person has organizational approval authority.

## Explicit reconstruction decisions (not historical authority)

- D1 Selection: require explicit --app. Directory input needs exactly one named
  immutable index; zero/multiple fail. Explicit index must be under indexes and
  selects one manifest for the requested app. Reject duplicate manifest paths
  (case-insensitive) and zero/multiple requested-app entries. Other app entries
  receive envelope/path/digest-shape checks; their complete bundles are not
  validated by --app. Direct manifest input is unsupported.
- D2 Parsing/extension: strict UTF-8 without BOM, duplicate keys and nonfinite
  constants rejected. Exact keys follow surviving field assertions; optional
  object app_specific locations are those observed, with uniform optional support
  for state extensions. Unknown fields fail, rather than silently becoming proof.
- D3 Bounds: JSON <=2 MiB each, <=100 captures/index entries, <=10000 root entries,
  path <=1024 chars, dimensions/DPI 1..65535, encoded PNG <=128 MiB, pixels <=64
  megapixels. Hashes stream in 1 MiB chunks; artifacts have no arbitrary byte cap.
  These modest operational bounds prevent unbounded parsing/traversal/reports;
  they are new compatibility limits requiring review for real future captures.
- D4 Paths: no absolute/drive, empty, dot/traversal, backslash, Windows reserved
  characters/device names, controls, trailing dot/space or alternate data streams.
  Unicode and ordinary spaces remain allowed. All root ancestors and recursively
  encountered root entries reject symlinks/junctions/reparse points; referenced
  files reject hardlinks. `latest` name/stem aliases case-insensitively fail.
  The ancestor/reparse and latest policies have source backing; Windows spelling,
  whole-root limits and hardlink choices close ambiguous read targets explicitly.
- D5 PNG: Pillow verifies container integrity and loads pixels; reject animation,
  malformed content, decompression warnings and limit violations. Full decoding
  is a reconstruction robustness choice beyond the established dimension/hash
  requirement. No image is rendered or written by validation.
- D6 Errors: fail at the first structural error, retain pending checks already
  found, always exit 2 if FAIL exists. Fixed short messages do not echo evidence
  values. Known consumed reasons are preserved; new reasons name the defective
  invariant. --help uses conventional argparse help/exit 0; normal validation and
  invalid arguments use the JSON protocol. Unexpected programmer defects are not
  disguised as a success. Ordinary I/O/parse errors produce bounded FAIL JSON.
- D7 Read-only scope: validator opens evidence only for reading, creates no output
  files, imports no repository code, invokes no processes and opens no network.
  Supply quiescent evidence; file size/mtime changes during hashing fail but this
  is not an atomic filesystem snapshot or a defense against concurrent hostile
  replacement. OS access-time updates are outside the no-content-write promise.
- D8 Equality: all duplicated JSON fields match with type-sensitive equality;
  row order is preserved across documents. Describe order itself is ignored while
  exact membership/uniqueness is checked. Tool commit need not equal app commit:
  surviving fixtures deliberately distinguish them. Optional app_specific PASS
  flags have no acceptance role.

## Outcome boundaries

PASS means supplied structure, bindings and non-placeholder organization strings
meet this reconstruction. It proves neither real UI capture nor runnable artifact,
native readiness, real organizational approval, source provenance, historical
equivalence, publisher/network success or final integration. Rollback is keeping
the explicit missing-validator gap until independent acceptance; later integration
can be reverted per repository without modifying preserved evidence.
