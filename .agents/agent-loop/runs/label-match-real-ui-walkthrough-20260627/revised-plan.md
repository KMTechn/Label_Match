# Revised Plan

status: locked_after_initial_5lane_research_merge

plan_model_policy: local_controller_with_required_5lane_research_merge
plan_lock_reason: This is a mixed UI/data/docs workflow. Initial five-lane research found that existing automated drivers are useful but insufficient for first-time operator proof, so a `run_tests=False` operator-style capture stage is required.

## Requirement Trace

| Req | Implementation claim | Verification | Status |
|---|---|---|---|
| REQ-UI-001 | Run the actual Label_Match UI from startup as a first-time operator. | Screenshot sequence and walkthrough report. | completed |
| REQ-UI-002 | Exercise normal scan flow and all meaningful operator buttons. | Captures before/after each action and UI state notes. | completed |
| REQ-UI-003 | Capture real modal/dialog behavior that automated `run_tests=True` drivers suppress. | Native/Tk dialog screenshots for manual complete, cancel, error, close/restore, and date picker. | completed |
| REQ-UI-004 | Use at least one real `Item.csv` master-label scenario, not only synthetic `VALID-` labels. | Fresh run report and marker-scoped CSV rows using `AAA2270730100`. | completed |
| REQ-DATA-001 | Verify local logs/data match UI actions. | Marker-scoped CSV/log inspection. | completed |
| REQ-SEC-001 | Check malicious operator inputs through UI-visible paths. | Captures/log excerpts proving safe rendering or issue record. | completed |
| REQ-FIX-001 | Fix meaningful issues discovered. | Focused code/docs changes and rerun affected scenario. | completed |
| REQ-DOC-001 | Update the existing Outline target with image-heavy final workflow. | Reload existing Outline document and count images/sections. | completed |

## Locked Stage Graph

1. Create a `run_tests=False` operator-style UI walkthrough driver that:
   - sets `LABEL_MATCH_SAVE_DIR` to isolated evidence data,
   - snapshots/restores `config/app_settings.json`,
   - suppresses only update-check/audio side effects that are not part of operator workflow,
   - enters scans through the entry `<Return>` binding,
   - invokes visible buttons or shortcut-bound handlers,
   - captures real native/Tk dialogs before auto-confirming them,
   - emits a screenshot manifest and marker-scoped CSV evidence.
2. Run the existing broad `run_current_pc_worker_e2e_capture.py` without relay for comprehensive automated coverage.
3. Run the new operator-style driver on a non-primary monitor geometry.
4. Inspect screenshots semantically and produce `walkthrough_review.md`:
   - expected state,
   - actual state,
   - pass/fail,
   - issue/fix decision.
5. Patch meaningful UI/data/security issues found; rerun affected scenario and targeted checks.
6. Curate fresh screenshots for the local worker manual. If screenshot count or asset folder changes, update publisher validation accordingly.
7. Update existing Outline document, not a new page, through Chrome session or API token if available. Completed through logged-in Chrome editor because `OUTLINE_API_TOKEN` is not present on this PC.
8. Validate local manual, dry-run publisher, and Chrome reload of existing Outline. Completed; reload verified 2026-06-27 content, 26 rendered image refs, 24 unique attachment refs, and no local asset refs.
9. Record commit intent for all loop-owned changes; do not commit without explicit `$loop 커밋`.

## Current Batch

Batch `DOC-CURATION-001`: curate final screenshots and update the local manual/Outline target.

Risk tier: tier2_material because the local manual and existing Outline document become the operator-facing artifact for this workflow.

Verification:

- Curate screenshot set from `operator-ui-20260627_121625`.
- Update `docs/OUTLINE_LABEL_MATCH_USER_MANUAL_20260626.md`.
- Validate publisher dry-run or Chrome/Outline route against existing doc `label_match-uMZaThRmO1`.
