# Walkthrough Review

reviewed_at: 2026-06-27 12:20 KST

## Final Operator-Style Run

- command: `python tools\label_match_operator_ui_walkthrough.py --output-dir C:\company\program\Label_Match\outputs\real-ui-walkthrough-20260627\operator-ui-20260627_121625 --geometry 1280x900+900+-1390`
- report: `C:\company\program\Label_Match\outputs\real-ui-walkthrough-20260627\operator-ui-20260627_121625\label_match_operator_ui_walkthrough_report.json`
- status: PASS
- marker: `OP_UI_20260627121625`
- screenshot count: 28 files including native dialog JSON receipts
- isolated local data: `C:\company\program\Label_Match\outputs\real-ui-walkthrough-20260627\operator-ui-20260627_121625\isolated_data`

## Verified Operator Flow

| Step | Evidence | Result |
|---|---|---|
| Startup/idle first operator screen | `00_startup_idle.png` | PASS |
| Settings dialog open and worker-name field visible | `01_settings_dialog.png`, `02_settings_worker_name_entered.png` | PASS |
| Real Item.csv normal full tray using `AAA2270730100` | `03_normal_scan_step_1..5.png` | PASS |
| Manual complete button and native confirmation | `04_manual_complete_ready.png`, `05_manual_complete_confirm.png`, `06_manual_complete_done.png` | PASS |
| Current set cancel via F1 path | `07_current_set_cancel_before.png`, `08_current_set_cancel_after.png` | PASS |
| Completed tray cancel via F2 path | `09_completed_tray_before_cancel.png` through `12_completed_tray_cancel_after.png` | PASS |
| Mismatch error modal | `13_mismatch_fullscreen_modal.png`, `14_mismatch_after_confirm.png` | PASS after fix |
| Close and restore partial set | `15_restore_before_close_partial.png`, `16_close_confirm_partial_set.png`, `17_restore_prompt.png`, `18_after_restore.png`, `19_restored_set_completed.png` | PASS after driver/product/focus fixes |
| Date picker and past-history view-only mode | `20_date_picker_dialog.png`, `21_past_history_view.png`, `22_past_history_scan_blocked.png`, `23_today_restored.png` | PASS |
| Malicious operator input rendering/logging | `25_malicious_input_after_confirm.png` plus report `malicious_rows` | PASS; rendered as text, not executed |

## Local Data Result

Final operator-style event counts:

- `SCAN_ATTEMPT`: 15
- `SCAN_OK`: 14
- `TRAY_COMPLETE`: 4
- `SET_CANCELLED`: 1
- `TRAY_COMPLETION_CANCELLED`: 1
- `ERROR_MISMATCH`: 1
- `SET_RESTORED`: 1

The past-history blocked scan assertion passed with `blocked_rows_before=0` and `blocked_rows_after=0`.

## Issues Found And Fixed

| Issue | Impact | Fix | Reverification |
|---|---|---|---|
| Error modal used primary monitor fullscreen and clipped long Korean/barcode text when the app ran on a secondary monitor. | Operator could miss the error on the working screen or see truncated instructions. | `Label_Match.py` now sizes the modal to the app window, uses `Malgun Gothic`, responsive font sizes, and bounded wrap length. | PASS run `operator-ui-20260627_121625`; modal screenshot is red, readable, and app-local. |
| Error modal close did not always restore scan-entry focus strongly enough for immediate next scanner input. | A fast scan after error confirmation could remain in the input field without being processed. | `_close_popup` now schedules `entry.focus_force`; the driver also focuses the entry before scanner-like Return input. | PASS run `operator-ui-20260627_121625`; restore flow records `SET_RESTORED=1`. |
| Restore scenario reused product barcodes from the normal scenario after switching to `REAL_MASTER`. | Driver false-negative: duplicate product detection cleared the partial set before restart. | Restore scenario now uses `RESTORE_` product/label suffixes. | PASS run `operator-ui-20260627_121625`. |
| Native/Tk dialog automation originally hung on modal dialogs and date picker. | Evidence driver could not complete unattended. | Helper code now avoids premature Win32 enumeration cancellation, uses direct native controls where possible, uses Tk callbacks for simpledialog/date picker, and a same-process restore prompt thread. | PASS run `operator-ui-20260627_121625`. |

## Broad Driver Regression

- command: `python tools\run_current_pc_worker_e2e_capture.py --output-root C:\company\program\Label_Match\outputs\real-ui-walkthrough-20260627\broad-current-pc-postfix --save-dir C:\company\program\Label_Match\outputs\real-ui-walkthrough-20260627\broad-current-pc-postfix-data --capture-geometry 1280x900+900+-1390`
- report: `C:\company\program\Label_Match\outputs\real-ui-walkthrough-20260627\broad-current-pc-postfix\current-pc-worker-e2e-20260627_121800\label_match_current_pc_worker_ui_e2e_report.json`
- status: `PASS_WITH_REMAINING_FIELD_BLOCKERS`
- local rows: 53
- marker rows: 52

Remaining blockers in that report are external field/prod gates: clean install self-enroll, physical scanner, 20 physical PCs, Syncthing shadow, and rollback rehearsal.
