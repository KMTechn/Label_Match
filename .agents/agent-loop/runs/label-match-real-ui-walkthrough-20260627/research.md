# Research

status: initial-5lane-research-merged

## Local Facts Collected

- Repository root: `C:\company\program\Label_Match`
- Main app: `Label_Match.py`
- Existing field-evidence tools:
  - `tools/run_current_pc_worker_e2e_capture.py`
  - `tools/label_match_real_pc_e2e_driver.py`
- Existing Outline target:
  - `https://wiki.kmtecherp.com/doc/label_match-uMZaThRmO1`
  - document id previously confirmed as `4115be8b-488a-4934-80af-f0f9e4ee721b`
- Current dirty files before this loop:
  - `docs/OUTLINE_LABEL_MATCH_USER_MANUAL_20260626.md`
  - `docs/OUTLINE_LABEL_MATCH_USER_MANUAL_PUBLISHING_NOTES_20260626.md`
  - `docs/outline_user_manual_publish_dry_run_20260626.json`
  - `tools/publish_outline_user_manual.py`
- Project guidance:
  - `CLAUDE.md` and `CODEX.md` read.
  - `README.txt` may be stale relative to current code.

## Delegated Research

capability_mode: delegated_agents_authorized_by_loop_tool_available

Initial five-lane research is required before plan lock because this is a material mixed workflow touching real UI, data, security, and documentation.

Dispatch receipts:

- `delegated-research/dispatch-receipts.json`

Completed lanes:

- architecture_dependency: completed
- failure_verification: completed
- goal_efficiency: completed
- requirement_alignment: completed
- implementation_quality: completed

## Local Runtime Notes

- `Label_Match.py` supports `LABEL_MATCH_SAVE_DIR`, so field walkthrough data can be isolated under a scoped output directory instead of polluting default storage.
- Existing drivers use `run_tests=True`, which suppresses dialogs and update checks. This is useful for repeatable evidence, but final UX review must inspect screenshots and should not treat method calls alone as a human-use proof.

## Merged Research Synthesis

### What Existing Tools Cover

- `tools/run_current_pc_worker_e2e_capture.py` is the fastest broad evidence driver. It captures startup, full tray, manual complete, mismatch, current-set cancel, completed-tray cancel, malicious input, past history, today restore, screenshots, blank detection, marker-scoped CSV counts, and optional relay state.
- `tools/label_match_real_pc_e2e_driver.py` covers restart/restore better but still uses `run_tests=True` and direct app methods.

### What Existing Tools Do Not Prove

- They do not prove real first-time operator UX because they patch message boxes, use `run_tests=True`, and often call app methods directly.
- Synthetic `VALID-...` labels bypass normal `Item.csv` validation, so a real `Item.csv` master such as `AAA2270730100` must be used in at least one operator-style scenario.
- Existing error screenshots can miss the fullscreen blocking error popup.
- Completed-tray cancel and manual-complete confirmation dialogs need screenshot evidence.
- Restore prompt, date-picker modal, past-history blocked scan, raw-detail/security rendering, and settings/worker-name behavior need explicit coverage.

### Execution Implications

- Use `LABEL_MATCH_SAVE_DIR` pointing at a run-scoped evidence data folder for UI-only validation.
- Preserve and restore `config/app_settings.json` around any `run_tests=False` run because app close persists UI settings and worker name.
- Keep DirectSync/relay out of the first UI workflow run unless server transport is explicitly in scope for that batch.
- Build or run a `run_tests=False` operator-style driver that interacts through the visible entry, buttons, function-key routes, and real native/Tk modal dialogs while capturing them.
- Patch only meaningful issues seen in screenshots/logs. For `Label_Match.py` fixes, run `python -m py_compile Label_Match.py` and targeted `tests/test_label_match_core.py` cases.
- Existing Outline target remains `https://wiki.kmtecherp.com/doc/label_match-uMZaThRmO1` / document id `4115be8b-488a-4934-80af-f0f9e4ee721b`.
