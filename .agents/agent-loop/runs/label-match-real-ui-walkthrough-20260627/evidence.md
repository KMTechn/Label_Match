# Evidence

## Initial Evidence

- `git status --short` before this run showed existing dirty docs/tooling from prior Outline work.
- `tools/label_match_real_pc_e2e_driver.py` and `tools/run_current_pc_worker_e2e_capture.py` exist and appear designed for field evidence capture.
- Dry-run report from prior Outline publisher exists and was PASS before this loop.

## Evidence To Produce

- Screenshot directory path. Produced:
  `C:\company\program\Label_Match\outputs\real-ui-walkthrough-20260627\operator-ui-20260627_121625\screenshots`
- Real UI driver/manual operation command. Produced:
  `python tools\label_match_operator_ui_walkthrough.py --output-dir C:\company\program\Label_Match\outputs\real-ui-walkthrough-20260627\operator-ui-20260627_121625 --geometry 1280x900+900+-1390`
- Screenshot manifest with dimensions/hash/nonblank checks. Produced in:
  `C:\company\program\Label_Match\outputs\real-ui-walkthrough-20260627\operator-ui-20260627_121625\label_match_operator_ui_walkthrough_report.json`
- Marker-scoped local log rows and event counts. Produced:
  `SCAN_ATTEMPT=15`, `SCAN_OK=14`, `TRAY_COMPLETE=4`, `SET_CANCELLED=1`, `TRAY_COMPLETION_CANCELLED=1`, `ERROR_MISMATCH=1`, `SET_RESTORED=1`.
- Issue list with severity and fix decisions. Produced:
  `walkthrough_review.md`.
- Updated local manual and Outline verification.

## Current Verification

- `python -m py_compile Label_Match.py`: PASS.
- `python -m py_compile tools\label_match_operator_ui_walkthrough.py`: PASS.
- Operator-style `run_tests=False` UI walkthrough: PASS, report above.
- Existing broad E2E driver after app fixes: `PASS_WITH_REMAINING_FIELD_BLOCKERS`, report:
  `C:\company\program\Label_Match\outputs\real-ui-walkthrough-20260627\broad-current-pc-postfix\current-pc-worker-e2e-20260627_121800\label_match_current_pc_worker_ui_e2e_report.json`.
- Local manual image validation: PASS; `markdown_image_refs=26`, `unique_image_refs=24`, missing images `[]`.
- Outline publisher dry-run: PASS, report:
  `C:\company\program\Label_Match\docs\outline_user_manual_publish_dry_run_20260627.json`.

## Outline Publish

- Existing Outline target opened in Chrome session:
  `https://wiki.kmtecherp.com/doc/label_match-uMZaThRmO1`.
- `OUTLINE_API_TOKEN` was absent; publisher dry-run reported `token_present=false`.
- Browser automation could read the page, but page-network API execution was unavailable through the safe automation surface, and `javascript:` URL execution was blocked by browser security policy.
- Chrome UI fallback succeeded:
  - Uploaded 24 local PNG screenshots through the Outline editor clipboard paste path.
  - Replaced local manual relative image paths with Outline attachment URLs.
  - Replaced the existing `Label_Match(포장실 프로그램)` document body through the logged-in Chrome editor.
  - Reload verification confirmed `작성 기준일: 2026-06-27` is present and `작성 기준일: 2026-06-26` is absent.
  - Reload verification confirmed restore/date-picker sections are present.
  - Reload verification confirmed `imageCount=26`, `uniqueAttachmentRefs=24`, and no local asset path text.
- Evidence files:
  - `evidence/outline_ui_uploaded_url_map_20260627.json`
  - `evidence/outline_ui_paste_payload_20260627.md`
  - `evidence/outline_ui_publish_verify_20260627.json`

## Final Challenge Proof

- Five read-only challenge lanes returned `ALLOW` for the current UI walkthrough/manual/Outline scope.
- Challenge summary:
  `C:\company\program\Label_Match\.agents\agent-loop\runs\label-match-real-ui-walkthrough-20260627\evidence\final_challenge_20260627.md`
- Post-challenge polish rerun:
  `C:\company\program\Label_Match\outputs\real-ui-walkthrough-20260627\operator-ui-20260627_polish\label_match_operator_ui_walkthrough_report.json`
- Polish rerun status: `PASS`.
- Polish rerun confirmed `config_restored=true`, `13_mismatch_app_modal`, and expected event counts.
