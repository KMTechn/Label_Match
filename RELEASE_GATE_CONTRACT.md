# Label_Match release gate contract

## Factory Contract adoption status

`RETIRE_CANONICAL_ADOPTION_LAYER` is the governing compatibility decision. The
never-merged Factory Contract 1.1.0 canonical-adoption candidate is rejected:
its ADR-0002 synchronized-adoption proposal and ADR-0003 executable-plan
authorization proposal are `HISTORICAL_REJECTED`. None of that candidate's
generation pointers, cohorts, receipts, coordinators, owner plans, or
contract-change blocking ceremony is a release prerequisite or authority.

The existing Factory Contract 1.0.3 bundle and `contract.lock.json` remain
enforced, including schemas required for wire and package compatibility.
Package, installer, runtime/active-work, and exact tag/artifact verification
also remain enforced. Uses of “canonical” below mean deterministic encoding or
exact business, Git, or artifact identity; they do not revive synchronized
canonical adoption.

| Gate | Accident prevented | Unique signal | Timing | Failure decision |
| --- | --- | --- | --- | --- |
| quick-check | Changed-area contract breakage | Focused pytest node; release identity tool only for version/release changes | During development, before final candidate freeze | Fix the affected area; do not advance the candidate |
| full-ci | Application regression and relevant UI geometry | Hash-locked Python 3.12.10 local environment, non-physical pytest once, and one conditional retry-UI node; physical DISPLAY2 remains field-only | Locally for the final candidate SHA before the isolated-mirror build; Hosted CI is factual supplemental evidence only | Make a focused fix and validate the new SHA locally; record unused or unavailable Hosted CI as `WAIVED_NOT_TESTED` |
| release-gate | Wrong identity or publication of bytes other than the qualified candidate | one final canonical annotated tag object recorded in the isolated mirror before identity/build, one fully qualified frozen ZIP/checksum pair built before any push, and later checksum/CRC/safe-path/exact-manifest/evidence verification without rebuilding | Qualify exact local bytes, push `main`, factually record Hosted CI, prove the same local tag object and candidate bytes are unchanged, then push that tag object and publish the same bytes | Any byte or identity mismatch leaves the prerelease quarantined; never recreate/move the tag or rebuild, reseal, or recompress the candidate |
| test1-e2e | Physical PHS2/scanner/display/direct-sync/update rollback failure | Exact artifact SHA, non-primary DISPLAY2, real scanner, relay receipt, local durable state, update and rollback preservation | Candidate rehearsal before final CI and the identical release artifact before stable rollout | Keep rollout 0; quarantine artifact |

## Exact commands

Exact-SHA local CI (Hosted execution of the same commands is supplemental only):

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

### Frozen-byte publication sequence (v2.0.67 and later)

The governing order is Phase B/8.3, then Phase C/9, then Phase D/10. GitHub
Actions is not a release gate and cannot replace the local exact-SHA gates.

1. Run the required exact-SHA local CI in an isolated environment. If Hosted CI
   was not used, record `WAIVED_NOT_TESTED`; never turn absence into `PASS`.
2. Prepare an existing isolated local bare mirror and a separate clean,
   non-bare release work clone. The work clone's `origin` URL must be the exact
   absolute path (or `file://` URI) of that mirror, local `main` must be checked
   out, and work-clone `HEAD`, `refs/heads/main`,
   `refs/remotes/origin/main`, plus mirror `refs/heads/main` must all resolve to
   the exact candidate commit. A GitHub/HTTPS origin is not accepted here.
3. Create the one final intended annotated tag object in that isolated local
   mirror environment before release identity generation or build, and prepare
   the work clone with that identical tag object. Its complete
   canonical message is the single line below (with the normal final LF):

```text
Release v2.0.67
```

   Record its object ID, object type `tag`, and peeled commit before invoking
   `verify_release_identity.py` or any build command. Both the work clone and
   bare mirror must expose that exact object as `refs/tags/v2.0.67`, report type
   `tag`, and peel it to the candidate commit. Run the builder from the release
   work-clone root with fresh output and the offline wheelhouse:

```powershell
pwsh -NoProfile -File .\tools\build_frozen_release_candidate.ps1 `
  -OutputRoot <FRESH-ABSOLUTE-CANDIDATE-DIRECTORY> `
  -Tag v2.0.67 `
  -PythonPath <EXACT-WINDOWS-X64-CPYTHON-3.12.10> `
  -Wheelhouse <ABSOLUTE-OFFLINE-WHEELHOUSE> `
  -MirrorRoot <ABSOLUTE-LOCAL-BARE-MIRROR>
```

   The builder creates no refs and performs no fetch, push, tag mutation, or
   network operation. Its required `refs/remotes/origin/main` is only the
   prepared tracking ref for the supplied local bare mirror; it does not require
   or claim live GitHub `origin/main`, Hosted CI, or repository immutability at
   this pre-push phase.
4. Build and fully qualify the candidate ZIP/checksum once from that clone. Freeze
   the filename, size, archive SHA-256, checksum SHA-256, main EXE SHA-256,
   manifest, source epoch, commit/tree, and final tag object in the preserved
   `label-match-pre-push-qualification-v2` receipt. Never recreate, retarget, or
   move the tag after this point. The tag is never replaced after artifact
   generation, and no artifact hash belongs in its message.
5. Before remote mutation, use a separate publication preflight context (never
   retarget the release work clone's local `origin`) to recheck live GitHub
   `origin/main`, fast-forward safety, absence of the tag/release/asset names,
   repository immutable-releases `enabled=true`, unrelated remote changes, and
   zero nonterminal workflows.
6. From that publication context, push only the validated `main` commit, fetch
   it, and prove its commit/tree match the receipt. Record exact-SHA Hosted CI
   factually as `PASS_NON_GATING` when it succeeded or `WAIVED_NOT_TESTED`
   otherwise. Hosted failure or absence alone does not invalidate locally
   qualified bytes.
7. Re-hash the still-local frozen ZIP/checksum and re-read the unchanged release
   work clone plus bare mirror tag object/type/peel and all four prepared `main`
   refs. Require exact equality with the preserved receipt/pre-build identity.
   Any change stops publication; do not rebuild or recreate the tag.
8. Push that exact pre-existing annotated tag object. Then create a draft
   prerelease for it, upload exactly the two frozen files, verify the draft title,
   body, asset names/size/digests, and publish it. The published prerelease must
   report `immutable=true`; never upload to a visible mutable release.
9. The read-only tag workflow waits at most 1800 seconds for that externally
   finalized immutable prerelease, parses the canonical simple tag, verifies the
   exact release/body/two-asset snapshot, downloads the pair, and validates it
   without building or mutating anything.

The prerelease title must be exactly `Release v2.0.67`. Its body must be exactly
the following LF-normalized identity record (a single trailing newline is
allowed):

```text
Internal prerelease; not production-ready.
Tag: v2.0.67
Commit: <40 lowercase hex>
Tree: <40 lowercase hex>
Artifact: Label_Match-v2.0.67.zip
Artifact-SHA256: <64 lowercase hex>
Artifact-Size: <positive decimal bytes>
Main-EXE-SHA256: <64 lowercase hex>
Factory-Contract-SHA256: adaa08684ebb291837327f63f967a4f22650dff72c4c1dc56ce1a9bee6b5404a
Status: QUARANTINED_PENDING_FACTORY_QUALIFICATION
```

Repository immutable releases are an external pre-tag gate. Query
`GET /repos/KMTechn/Label_Match/immutable-releases` with GitHub API version
`2026-03-10` and require `enabled=true` before the tag is pushed. As of the
2026-08-12 read-only preflight it is `false`; do not publish v2.0.67 until an
authorized repository administrator enables it. The workflow rechecks both the
`release.immutable=true` field and the exact release/asset snapshot before and
after byte verification. The repository-policy endpoint itself requires
repository Administration permission, so it remains an operator pre-tag gate
and is deliberately not queried by the read-only tag workflow token.

The public workflow takes artifact identity from the exact immutable release
body, not from the tag message, and runs the following standard-library verifier
with the freshly parsed tag object and tagged commit epoch:

```powershell
python -I -S tools/verify_frozen_release_assets.py `
  --archive <DOWNLOADED-FROZEN-ZIP> `
  --checksum <DOWNLOADED-FROZEN-CHECKSUM> `
  --expected-tag <TAG> `
  --expected-commit <EXACT-MAIN-SHA> `
  --expected-tree <EXACT-MAIN-TREE> `
  --expected-tag-object <PARSED-CANONICAL-TAG-OBJECT> `
  --expected-source-epoch <TAGGED-COMMIT-EPOCH> `
  --expected-archive-sha256 <EXACT-RELEASE-BODY-ARCHIVE-SHA256> `
  --expected-archive-size <EXACT-RELEASE-BODY-ARCHIVE-SIZE> `
  --expected-main-exe-sha256 <EXACT-RELEASE-BODY-MAIN-EXE-SHA256> `
  --report <VERIFICATION-REPORT>
```

The workflow parses the raw annotated tag object with
`tools/verify_release_tag_attestation.py`, rejects any hash-bearing, multi-line,
malformed, or noncanonical tag message, and checks the downloaded ZIP hash/size
and embedded `Label_Match.exe` hash against the immutable release body. After
byte verification it re-queries the immutable release and remote refs, proving
the tag object, peeled commit, `origin/main`, release target, and the same two
asset IDs/sizes/digests remained unchanged.

Because the preserved Phase-B receipt is intentionally local evidence, the
public workflow must report its receipt comparison as
`NOT_TESTED_EXTERNAL_REQUIRED`; it must not reconstruct or claim that receipt.
An external operator must then download the immutable ZIP/checksum and rerun the
same verifier with the freshly parsed remote tag identity plus:

```powershell
  --qualification-receipt <PRESERVED-PHASE-B-QUALIFICATION-JSON>
```

That invocation derives the approved archive/EXE identity from the receipt and
must return `qualification_receipt_status=PASS`. This exact post-download byte
parity, including checksum bytes and tag object identity, replaces the forbidden
tag-hash design. Until it passes, the prerelease remains quarantined.

The tag workflow is verification-only. It must not run PyInstaller, install
release dependencies, mutate package contents, generate identities/manifests,
seal a package, create or recompress a ZIP, generate a checksum, create/edit a
release, upload assets, or promote the private feed. The verifier fails closed
on checksum, CRC, unsafe/duplicate/case-colliding paths, membership differing
from the embedded sealed manifest, payload byte mismatch, wrong
commit/tree/version, contract bundle drift, abbreviated or malformed staged
installer/CLI/probe evidence, missing CLI source members, missing onedir runtime
payload, or a failed staged-installer claim. It safe-extracts to a temporary
directory and calls the exact resolved sibling
`build_release_archive.validate_release_evidence` implementation, so build-time
and post-download evidence validation cannot silently diverge.

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

The expected hashes must come from the preserved Phase-B qualification receipt, not from
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

The staged-installer pytest command is a unique frozen-package context, not a duplicate full suite. The hosted retry-UI node is conditional on a fail-closed UI-impact classifier and does not claim the non-primary DISPLAY2 signal; that physical node stays in TEST1. A newer `main` push cancels an obsolete in-progress Full CI run. Every tag run verifies an externally created, frozen TEST1 prerelease and never creates or edits it. Stable promotion and private-feed publication are separate, currently external decisions: keep rollout `0`, allow only TEST1, obtain the physical DISPLAY2 result and rollback evidence, then require owner approval. Branch protection, feed credentials, TEST1 hardware/server, and approval are external blockers.
