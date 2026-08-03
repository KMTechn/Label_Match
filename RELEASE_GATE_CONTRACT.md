# Label_Match release gate contract

| Gate | Accident prevented | Unique signal | Timing | Failure decision |
| --- | --- | --- | --- | --- |
| quick | Changed-area contract breakage | Focused pytest node; release identity tool only for version/release changes | Before main push | Do not push |
| full | Application regression and relevant hosted UI geometry | Hash-locked Python 3.12.10 environment, non-physical pytest once, and one conditional hosted retry-UI node; physical DISPLAY2 remains field-only | Once per main-push SHA; no PR or manual trigger | Make a focused fix and validate the new SHA; do not tag the failed SHA |
| release | Wrong identity, unsafe archive, staged installer or signature failure | exact tag/version/SHA/main/clean checkout, exact-SHA Full CI, PyInstaller/helpers, staged installer, deterministic ZIP CRC/membership/byte parity, SHA, manifest self-verification | Tag push | Before GitHub publication: publish nothing. A post-release feed failure leaves a TEST1 prerelease quarantined |
| test1 | Physical PHS2/scanner/display/direct-sync/update rollback failure | Exact artifact SHA, non-primary DISPLAY2, real scanner, relay receipt, local durable state, update and rollback preservation | After GitHub release, before stable rollout | Keep rollout 0; quarantine artifact |

## Exact commands

Full CI:

```powershell
python -I -m pip install --disable-pip-version-check --only-binary=:all: --require-hashes --no-deps -r requirements-release.txt
python -I -m pip check
python -m pytest -q --deselect tests/test_label_operator_workbench.py::test_live_submission_retry_hides_raw_server_error_and_keeps_five_scan_rows --deselect tests/test_label_operator_workbench.py::test_display2_1366_scale100_keeps_operator_content_inside_its_regions
# Only when UI-impact paths changed:
python -m pytest -q tests/test_label_operator_workbench.py::test_live_submission_retry_hides_raw_server_error_and_keeps_five_scan_rows
```

Release-only packaged installer verification remains:

```powershell
python tools/verify_staged_release_installer.py --package-root dist/Label_Match --report dist/Label_Match/staged-installer-verification.json
python -m pytest -q -p no:cacheprovider tests/test_staged_release_installer.py
```

TEST1:

```powershell
python tools/run_current_pc_worker_e2e_capture.py --output-root <FRESH_EVIDENCE_DIR> --save-dir <TEST_DATA_ROOT> --direct-sync-root <TEST_DIRECT_SYNC_ROOT> --capture-geometry 1366x768+0+0 --run-relay
```

The staged-installer pytest command is a unique frozen-package context, not a duplicate full suite. The hosted retry-UI node is conditional on a fail-closed UI-impact classifier and does not claim the non-primary DISPLAY2 signal; that physical node stays in TEST1. A newer `main` push cancels an obsolete in-progress Full CI run. Every tag run creates a GitHub prerelease that is not latest; private-feed publication additionally enforces rollout `0` and occurs only after GitHub Release success. A failed feed upload leaves that prerelease quarantined and not stable/latest. Stable promotion is a separate, currently external decision: allow only TEST1, obtain the physical DISPLAY2 result and rollback evidence, then require owner approval. Branch protection/CODEOWNER, feed credentials, TEST1 hardware/server, and approval are external blockers.
