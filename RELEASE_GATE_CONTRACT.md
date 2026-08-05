# Label_Match release gate contract

| Gate | Accident prevented | Unique signal | Timing | Failure decision |
| --- | --- | --- | --- | --- |
| quick-check | Changed-area contract breakage | Focused pytest node; release identity tool only for version/release changes | During development, before final candidate freeze | Fix the affected area; do not advance the candidate |
| full-ci | Application regression and relevant hosted UI geometry | Hash-locked Python 3.12.10 environment, non-physical pytest once, and one conditional hosted retry-UI node; physical DISPLAY2 remains field-only | Once for the final `main` SHA; no PR or manual trigger | Make a focused fix and validate the new SHA; do not tag the failed SHA |
| release-gate | Wrong identity, unsafe archive, staged installer or signature failure | exact tag/version/SHA/main/clean checkout, exact-SHA Full CI, PyInstaller/helpers, staged installer, deterministic ZIP CRC/membership/byte parity, SHA, manifest self-verification | Tag push after exact-SHA full-ci success | Before GitHub publication: publish nothing. A post-release feed failure leaves a TEST1 prerelease quarantined |
| test1-e2e | Physical PHS2/scanner/display/direct-sync/update rollback failure | Exact artifact SHA, non-primary DISPLAY2, real scanner, relay receipt, local durable state, update and rollback preservation | Candidate rehearsal before final CI and the identical release artifact before stable rollout | Keep rollout 0; quarantine artifact |

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

Source/current-PC readiness evidence only (`READY_COMPONENT_WRITE`, never
release acceptance):

```powershell
python tools/run_current_pc_worker_e2e_capture.py --output-root <FRESH_EVIDENCE_DIR> --save-dir <TEST_DATA_ROOT> --direct-sync-root <TEST_DIRECT_SYNC_ROOT> --capture-geometry 1366x768+0+0 --run-relay
```

Exact installed-package identity launch:

```powershell
python tools/run_test1_exact_artifact.py --archive <RELEASE_ZIP> --expected-archive-sha256 <APPROVED_ZIP_SHA256> --exe <EXTRACTED_ZIP>\Label_Match\Label_Match.exe --expected-exe-sha256 <APPROVED_EXE_SHA256> --archive-member Label_Match/Label_Match.exe --evidence-json <FRESH_EVIDENCE_DIR>\exact-artifact-identity.json
```

### Process-only TEST1 legacy override

`KMTECH_TEST1_ALLOW_ISOLATED_LEGACY_LOGISTICS=1` is allowed only for the
approved TEST1 E2E process on the physical machine whose real
`COMPUTERNAME` is `TEST1`. Do not persist it at User/Machine scope, place it in
the release workflow, or use it on a production worker.
`KM_LOGISTICS_PROFILE_PATH` and `KM_LOGISTICS_REQUIRED` must be absent. The
Label save root, direct-sync root, evidence, and private CA must all stay below
one fresh `C:\KMTech\Test1\Runs\<run>` root; production `%ProgramData%`, queues,
receipts, or labels are forbidden.

The only accepted legacy endpoint is exact
`https://127.0.0.1:<port>` with no suffix or alias. Its certificate must contain
IP SAN `127.0.0.1` and chain to the approved TEST1-only CA in
`REQUESTS_CA_BUNDLE`. The loopback proxy may forward only to the isolated TEST1
backend, never to a LAN, Internet, production, or production-credential
endpoint.

After provisioning the CA outside Git, replace the placeholders and execute:

```powershell
$ErrorActionPreference = "Stop"
if ($env:COMPUTERNAME -ine "TEST1") { throw "BLOCKED: this packet runs only on TEST1" }

$runId = "label-<UTC-RUN-ID>"
$scope = "TEST1-LABEL-<UTC-RUN-ID>"
$token = "<TEST1-only-token>"
$archive = "<APPROVED-RELEASE-ZIP>"
$expectedArchiveSha256 = "<APPROVED-ZIP-SHA256>"
$packageExe = "<EXTRACTED-EXACT-SHA-ZIP>\Label_Match\Label_Match.exe"
$expectedExeSha256 = "<APPROVED-EXE-SHA256>"
if (@(
  $runId, $scope, $token, $archive, $expectedArchiveSha256,
  $packageExe, $expectedExeSha256
) -match "<|>") {
  throw "BLOCKED: replace every TEST1 placeholder"
}
$runRoot = "C:\KMTech\Test1\Runs\$runId"
$caBundle = "$runRoot\tls\test1-ca.pem"
New-Item -ItemType Directory -Force `
  -Path "$runRoot\LabelMatch", "$runRoot\direct-sync", "$runRoot\evidence",
        "$runRoot\tls" |
  Out-Null
if (-not (Test-Path -LiteralPath $caBundle -PathType Leaf)) {
  throw "BLOCKED: approved TEST1 CA is missing"
}

Remove-Item Env:KM_LOGISTICS_PROFILE_PATH -ErrorAction SilentlyContinue
Remove-Item Env:KM_LOGISTICS_REQUIRED -ErrorAction SilentlyContinue
$env:KMTECH_TEST1_ALLOW_ISOLATED_LEGACY_LOGISTICS = "1"
$env:LABEL_MATCH_SAVE_DIR = "$runRoot\LabelMatch"
$env:LABEL_MATCH_LOGISTICS_API_BASE_URL = "https://127.0.0.1:19443"
$env:LABEL_MATCH_LOGISTICS_API_TOKEN = $token
$env:LABEL_MATCH_LOGISTICS_AUTHORITY_SCOPE_ID = $scope
$env:LABEL_MATCH_LOGISTICS_SOURCE_HOST_ID = "test1-label-host"
$env:LABEL_MATCH_LOGISTICS_DEVICE_ID = "test1-label-device"
$env:REQUESTS_CA_BUNDLE = $caBundle

python -c "from package_logistics import package_client_from_env; c = package_client_from_env(probe_required=False); assert c is not None and c.config.authority_scope_id.startswith('TEST1-'); print('test1_profile=PASS')"
if ($LASTEXITCODE -ne 0) { throw "BLOCKED: TEST1 process profile preflight failed" }

# Run the released executable first. Perform the physical DISPLAY2/scanner scenario
# in this exact process, then close it normally.
python tools/run_test1_exact_artifact.py `
  --archive $archive `
  --expected-archive-sha256 $expectedArchiveSha256 `
  --exe $packageExe `
  --expected-exe-sha256 $expectedExeSha256 `
  --archive-member "Label_Match/Label_Match.exe" `
  --evidence-json "$runRoot\evidence\exact-artifact-identity.json"
if ($LASTEXITCODE -ne 0) {
  throw "BLOCKED: exact Label package identity or execution failed"
}
```

The expected hashes must come from the approved release attestation, not from
hashing the candidate supplied to this command. A PASS from
`run_current_pc_worker_e2e_capture.py` is only `READY_COMPONENT_WRITE` supporting evidence
and must use a separate fresh supporting run. The release
TEST1 gate additionally requires
`exact-artifact-identity.json` from the released process plus the DISPLAY2,
scanner, relay receipt, durable-state, update, and rollback evidence produced
while that same exact package process is under test.

Any throw, nonzero exit, CA/origin/scope mismatch, missing exact relay receipt,
or write outside `$runRoot` is `TEST1 BLOCKED`. Keep rollout at `0`, quarantine
the artifact, never substitute production data/profile/credentials, and
preserve the failed run directory. A corrected retry uses a new run ID.

The staged-installer pytest command is a unique frozen-package context, not a duplicate full suite. The hosted retry-UI node is conditional on a fail-closed UI-impact classifier and does not claim the non-primary DISPLAY2 signal; that physical node stays in TEST1. A newer `main` push cancels an obsolete in-progress Full CI run. Every tag run creates a GitHub prerelease that is not latest; private-feed publication additionally enforces rollout `0` and occurs only after GitHub Release success. A failed feed upload leaves that prerelease quarantined and not stable/latest. Stable promotion is a separate, currently external decision: allow only TEST1, obtain the physical DISPLAY2 result and rollback evidence, then require owner approval. Branch protection/CODEOWNER, feed credentials, TEST1 hardware/server, and approval are external blockers.
