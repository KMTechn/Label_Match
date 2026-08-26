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
$verifyRoot = "E:\KMTech\label-match-release-installer-<UTC-RUN-ID>"
$env:TEMP = Join-Path $verifyRoot "tmp"
$env:TMP = $env:TEMP
$env:PYTHONDONTWRITEBYTECODE = "1"
New-Item -ItemType Directory -Path $env:TEMP -Force | Out-Null
$packageRoot = (Resolve-Path -LiteralPath "dist/Label_Match").Path
$env:LABEL_MATCH_STAGED_PACKAGE_ROOT = $packageRoot
$env:LABEL_MATCH_REQUIRE_STAGED_INSTALLER_TEST = "1"
python tools/verify_staged_release_installer.py --package-root $packageRoot --report (Join-Path $packageRoot "staged-installer-verification.json")
python -m pytest -q -p no:cacheprovider --basetemp (Join-Path $verifyRoot "pytest-staged") tests/test_staged_release_installer.py
```

Both packaged-installer commands are isolated dry-run gates. They require no
elevation, invoke no UAC flow, and perform no Task Scheduler mutation; the
sealed-package pytest runs only after `build-manifest.json` is created.

The versioned public install contract is `INSTALL_THIS_PC.ps1` from the ordinary
extracted `Label_Match/` root. The ZIP keeps that top-level directory. Before
any installer effect, the entrypoint verifies every manifest-declared path,
size, and SHA-256, rejects traversal, drive/ADS/backslash paths, case collisions,
reparse points, missing files, and extras, then verifies a same-volume candidate
before its atomic rename to `C:\KMTech\Apps\Label_Match\current`. An unknown
target is a conflict; it is never mirror-deleted or overlaid.

One discoverability resource is owned: the all-users Start Menu link at
`C:\ProgramData\Microsoft\Windows\Start Menu\Programs\KMTech\Label Match.lnk`.
Its target and icon are the installed `Label_Match.exe`, and its working
directory is the canonical app root. Install must reopen and verify those exact
properties. No Desktop link, MSI, setup EXE, or flattened ZIP is part of this
contract.

`-Uninstall` is `DATA_PRESERVING_UNINSTALL`: it removes only summary-owned app,
launcher, task wrapper, relay identity/credential, and machine-profile files,
while explicitly preserving Label business data plus queue/spool/status/log and
receipt evidence. It never claims pre-install parity. `-Rollback
-EvidenceArchiveRoot <ABSOLUTE-EXTERNAL-PATH>` is the separate
`EXACT_FRESH_TARGET_ROLLBACK` path; every active resource must record an absent
prestate, business evidence is copied within the fixed 10,000-file/2-GiB bounds
and hash-verified into a fresh absent, non-reparse external root, and the final
receipt is outside every removed/restored path. The install summary binds the
exact app, DirectSync, data, machine-profile, launcher, and task identities;
the app identity uses `label-match-app-immutable-inventory-v1`, requires the
regular non-reparse runtime leaf `_internal/config/app_settings.json`, and
allows only that file's normal settings bytes to change. Every other app file,
including `config/app_settings.json`, remains in the immutable count and
SHA-256 inventory, so any addition, removal, or byte drift outside that sole
mutable path is fatal;
created parent-directory ancestry is recorded and restored without removing a
pre-existing empty directory. Task evidence is typed and ordered `stop`,
`delete`, `absence`; every phase report is fresh and bound to the requested
phase, mode, task name, and owned action before mutation. Drift, foreign
ownership, a live GUI, failed evidence parity, a reused evidence destination,
or any missing postcondition is a nonzero result. The nested rollback report
does not claim parity while app-root removal is pending; only the public wrapper
may finalize that external report and exact receipt after all owned roots and
recorded parent directories are proven restored.
Immediately before the final receipt, the public wrapper re-reads the bounded
inventory and re-hashes every archived payload file. The receipt binds both the
evidence-inventory SHA-256 and the finalized rollback-resource-report SHA-256;
a missing, added, reparse-backed, resized, or changed archived file is fatal.

The staged v2 report proves this contract with an isolated manifest-bound public
entrypoint dry run and is classified `STATIC_ISOLATED_DRY_RUN`; it must state
`dynamic_qualification=NOT_TESTED`. Both packaged installer scripts compute file
digests with their identical `Get-FileSha256` authority: a read-only file stream,
.NET SHA-256, exact 32-byte digest validation, and lowercase invariant hex. They
must not invoke ambient `Get-FileHash` or resolve an executable from `PATH`; the
focused Windows regression disables module autoload and removes both module and
executable lookup paths while proving known bytes and missing-file failure. This
static gate does not replace a fresh Windows target, ordinary-operator launcher,
live task, data-preserving uninstall, or exact rollback qualification run. The
checked-in one-shot release runner also parses itself, the official builder, and
both public installer scripts with the PowerShell AST before launch. It rejects
both a Boolean token parsed as a `Test-Path` command parameter and every
`Test-Path` command nested in an `-and`/`-or` binary expression; the Windows
PowerShell 5.1 preflight regression proves clean acceptance, injected ambiguous
syntax rejection, and independent stdout/stderr freshness checks without
starting the builder.

### Frozen-byte publication sequence (v2.0.84 current candidate)

The initial elevated v2.0.67 infrastructure cohort at
`E:\KMTech\label-v2067-daa3-phase83-elevated-20260812` failed before
producing a qualified archive. A later, separate exact-SHA cohort at
`E:\KMTech\l67e2` produced the preserved v2.0.67 ZIP/checksum and Phase
8.3/9 PASS receipts for annotated tag object
`1368827125097961cdb7faa8bac421ebb926edf7`, commit
`daa3fd47103ea2ebf915fce60ebe5f9b5d014164`, and tree
`1a9a4cc7e8edb8783551f9960ce66122962f1c29`. That later evidence does not
erase the earlier one-shot failure. Treat every v2.0.67 tag, artifact, and
receipt as immutable quarantined history: do not publish, delete, recreate,
retarget, reuse, or promote it. The successor selected after that history was
v2.0.68.

On 2026-08-18 the sole local v2.0.68 annotated tag object
`0a3e522c783d64138e18050e83698b5151e7921c`, for commit
`99d94583d563237fe411376ee3a5372fef0b33f0` and tree
`2c9847cb55165206143d585c94e7ae5eb17236ed`, failed canonical tag attestation.
Its raw message was the exact 15-byte text `Release v2.0.68` without the required
final LF (SHA-256
`78200303ea8ad2867435761b484867a76429bf7eeb4897ade0647cc8fbe0e762`). The
failure occurred before mirror tag push, UAC, build, scheduled-task mutation, or
output creation. Preserve that local tag object and preparation as immutable
quarantined history: do not delete, recreate, retarget, push, reuse, or promote
it. The successor selected after that failure was v2.0.69.

On 2026-08-18 the sole v2.0.69 annotated tag object
`ea236828420ac03f01b496df9fc27b1bdfe31d57`, for commit
`c187e1f45b2a98f604cf96a06fe9bcea09ddc603` and tree
`93982f7dc2dbfa5e91999a908100c2ecb71b0317`, passed the exact canonical
16-byte `Release v2.0.69\n` message attestation (SHA-256
`f6ac482dd61f5f9ac1b56f4687d4d2214a71e9d0838aeebda3ad53679d38d403`)
and was pushed as the identical object to the isolated local bare mirror. Its
single elevated builder invocation then failed at the first fresh-copy
`direct_sync_relay_runner.exe --help` probe with Windows return code
`4294967295` (`0xffffffff`, signed `-1`). The quarantined partial output is
`E:\KMTech\Label_Match\v2.0.69-frozen-candidate-c187e1f-20260818`; it has no
ZIP, checksum, archive-verification report, or Phase 1 qualification receipt,
and the staged public uninstall/scheduled-task gate was never reached. The
bounded failure receipt is
`E:\KMTech\Label_Match\v2.0.69-one-shot-c187e1f-20260818\one-shot-failure-summary.json`
(SHA-256
`51e308c3a213fe54865aebc00c5260e3c329ed39940578e3fc1025d3a5688b1c`).
Preserve its work-clone tag, mirror tag, logs, and partial output as immutable
quarantined history: do not rerun, relaunch, delete, recreate, retarget, reuse,
or promote them. The successor selected after that failure was v2.0.70.

On 2026-08-18 the clean v2.0.70 source candidate was commit
`2737ce4de2b426bf15cbdde0cd0b92486fd6e036`, tree
`889cd3a6dff0f49e170099b8595ee5bb51ff84f6`, with sole parent
`c187e1f45b2a98f604cf96a06fe9bcea09ddc603`. Its exact-SHA offline CI passed
with bounded receipt SHA-256
`eee5de3b5a6e460842aca60b90c40fa9e94d508bc92dc304375cedd9e10ce9e2`, and
its one untagged native CLI pretag gate passed with receipt SHA-256
`7475a18984c8cda606d72ae97fa05fc14370ec403f234246e3826b0cf2956926`
and manifest SHA-256
`95b2b91ebd67972c42f954bcf9de9da376c20f095de2700dce7842a9eb3961ed`.
After independent preburn GO, the CreateNew message file was correctly frozen as
the exact 16-byte `Release v2.0.70\n` message (SHA-256
`fda39b188da8bbf35e0599b7bcb4fecf19d093636c1116f6d9ea047714a6ced4`).
The ad-hoc PowerShell wrapper was then rejected by the parser at the expression
`(git show-ref --verify --quiet refs/tags/v2.0.70; $LASTEXITCODE -eq 0)` before
its first statement or any Git invocation. Therefore v2.0.70 has no work-clone
or mirror tag, no output, no UAC launch, no build invocation, and no scheduled
task mutation. Its bounded failure receipt is
`E:\KMTech\Label_Match\v2.0.70-one-shot-2737ce4-20260818\pretag-wrapper-failure-summary.json`
(SHA-256
`95b03e2d8ecc693762f7f3925d315ff24058665d861b2a1fec7b18558d87dffa`).
Preserve all v2.0.70 preparation and failure evidence as immutable quarantined
history: do not retry the wrapper, create its tag, or reuse its candidate. The
successor selected after that failure was v2.0.71.

On 2026-08-18 the clean v2.0.71 source candidate was commit
`778614b6d0286a9a90f4159847f4012258b1d15f`, tree
`a3dfb4db1cf3e27fd6a1dd3cff64e8e036364b97`, with sole parent
`2737ce4de2b426bf15cbdde0cd0b92486fd6e036`. Its exact-SHA offline CI passed
with receipt SHA-256
`6d02ffc7e198408b0c1c2af671f1f4dbeb0e0306bf37f04ad5f3e7d79b874cb7`, and
its one untagged native CLI pretag gate passed with receipt SHA-256
`f7adddfac41355eca6ffe2a74ffa0d6c02afa4cb676afcc719a2faa44d1b6632`
and manifest SHA-256
`558492a0b17f7fa7b26ac671537e82774f062e32ebbb9057212e2bb50540a926`.
The versioned tag authority then created the exact canonical 16-byte
`Release v2.0.71\n` annotated tag object
`9236c952621c6528d2888c8f36db183c89151f0e` (message SHA-256
`af69dbb9b8a42902a4f3c7d5157ba74716305ca2089088d3527b30260b363bd0`)
and pushed that identical object only to the isolated local mirror. Its PASS
tag-burn receipt SHA-256 is
`023b356816bdb6a677b5b85530cf2b1697c8f709d6ab5e41d1f28f745ee81ef3`.
The sole elevated builder invocation then failed at the first fresh-copy
`direct_sync_relay_runner.exe --help` probe. The onefile bootloader returned
signed `-1`/unsigned `0xffffffff` while opening the exact 68-character member
`cryptography-49.0.0.dist-info\sboms\cryptography-rust.cyclonedx.json` below
the probe TEMP. The builder had fixed TEMP under its 73-character output work
path; the deterministic probe/TEMP/`_MEIxxxxxx` structure projected the target
to 261 characters while this host had `LongPathsEnabled=0`. The failed EXE was
not preserved; its recorded size was 15,880,900 bytes and its SHA-256 was
`46593f8c4250721c1e8da557edd5d61204c54b97962bf9eb5d92147429555e6f`.
The elevated runner receipt SHA-256 is
`6e85bea7f919b29e1bd156212d1bb9b6b5c4f233f45608c5a486577c4c5a61ac`,
and the launch-result receipt SHA-256 is
`e303cc5bef618a13e9ea743de7aab323d49e25c8473d4fbd3379d628f356c06f`.
The quarantined partial output is
`E:\KMTech\Label_Match\v2.0.71-frozen-candidate-778614b-20260818`; it has no
ZIP, checksum, archive-verification report, qualification receipt, or release
notes, and the staged public uninstall/scheduled-task gate was never reached.
Preserve the v2.0.71 work/mirror tag, receipts, logs, and partial output as
immutable quarantined history: do not rerun, relaunch, delete, recreate,
retarget, reuse, or promote them. The successor selected after that failure was
v2.0.72.

On 2026-08-18 the clean v2.0.72 source candidate was commit
`03513faf6523a500ba4eefbc65503567dcbb6c49`, tree
`0727047c0a3bcd919110b01a11641d23183834b6`, with sole parent
`778614b6d0286a9a90f4159847f4012258b1d15f`. Its exact-SHA offline CI passed
with receipt SHA-256
`cb0bb50e0308fb585c540aaa86716739f42eb2c48c8c21785f274e77c98b7768`, and
its one untagged native CLI pretag gate passed with receipt SHA-256
`5097c702faa122bece8379d9376e8351d0a97a1def71a66401fcf30931f94403`.
The versioned tag authority created the exact canonical 16-byte
`Release v2.0.72\n` annotated tag object
`3f22d88a5c99a61c550d385a7226f211b6731434` (message SHA-256
`85004b0d6b037337be06fd5346bfd0e86a837122a9dd2a95545cd3fc113e44f1`)
and pushed that identical object only to the isolated local mirror. Its PASS
tag-burn receipt SHA-256 is
`38acff06dbd03c4a8fba5906034a54c6f7d46c4cebf08ab46f611bfb6974a94f`.
The sole elevated builder invocation then stopped at the staged installer gate
with verifier exit 2 and the bounded denial
`staged_installer=DENY reason=typed scheduled-task probe failed`; the task-create
step was never reached and scheduled-task mutation count remained zero. The
v2.0.72 verifier discarded the probe's exact return code, stdout, stderr,
exception-chain type, HRESULT, and stage, so the preserved evidence cannot
distinguish a wrapped `0x80070002` missing-task result from an otherwise exact
success accompanied by bounded diagnostic stderr. The elevated builder receipt
SHA-256 is
`08ed2bece1840318fb571117b0d9414b2d3318c726838a4816dc23d18c16c0dc`,
and the launch-result receipt SHA-256 is
`8ecc39e3477f3df11f92391439c2bc1d09a292dab1894f3cd0cacc3fad9ae708`.
The quarantined partial output is
`E:\KMTech\Label_Match\v2.0.72-frozen-candidate-03513fa-20260818`; it has no
ZIP, checksum, archive-verification report, qualification receipt, or release
notes. The short extraction root `E:\KMTech\x72release03513fa` was removed with
an absence proof. Preserve the v2.0.72 work/mirror tag, receipts, logs, partial
output, and diagnostic evidence as immutable quarantined history: do not rerun,
relaunch, delete, recreate, retarget, reuse, or promote them. The successor selected after that failure was
v2.0.73.

On 2026-08-18 the sole v2.0.73 annotated tag object
`8afcb61bc6a25fb5c4e9a1aaa515c133d3e39110`, for commit
`ab361544f48c246968a665f102169ea989774bc3` and tree
`bdd4d41042814bf45b80516cc1d12a81fd1e49c8`, was created with the exact
16-byte `Release v2.0.73\n` message (SHA-256
`b1595789be58e66cd5dac8eeebc74abb9c3fe89b3df5862330f4e1bd361727ca`)
and pushed only to the isolated local mirror. The PASS tag-burn receipt
SHA-256 is
`d0b76157ae6b0977b8fba11c7aeaf6d49b633d77e8eab0ec08976d15d2c81ea3`,
and the one-shot builder claim SHA-256 is
`7775d373b38084454186dca6636e0a7df56d5db622de6a1a8800df1b3ba4bbd9`.
The sole builder invocation completed PASS with exit code 0; its runner receipt
file SHA-256 is
`79138044c64c30f256ac3e2144116202a0ae20f705fafb532954fec25e63f547`,
records invocation count 1 and `retry_allowed=false`, and produced the frozen
121,426,081-byte ZIP with SHA-256
`1d60ebce9960a4a9313f17da02fb5ef01d9a53b0e38a6ae9bde2642d5af2bce3`.
The preserved PASS Phase-B qualification receipt SHA-256 is
`297d6debf923f922195224ed809d838247ddf650cd9eb52f3a0234e2019b3aa6`.
Those exact bytes remain immutable quarantined history; do not rerun, rebuild,
recreate, retarget, reuse, or promote them as this changed source. The successor
selected after that release was v2.0.74.

On 2026-08-21 the sole v2.0.74 annotated tag object
`5ec3134554948ce127c5255830fde73b2f84f490`, for commit
`800d37bd38807c364335d47d3ce94f7af9d8c4a6` and tree
`9c1059301f0b87e877b6066037031a880c80ce41`, was created with the exact
16-byte `Release v2.0.74\n` message (SHA-256
`dc59cc5e094b4a9d15b987031dad77303586ce71f4beb84ad4fac07d16b99f3b`)
and pushed only to its isolated local mirror. The PASS exactly-once tag-burn
receipt SHA-256 is
`8d3c00d1cf031ac5cc4dacac9f3773b23c4776b4bb47457d65e732d56b679b06`.
The sole builder invocation recorded invocation count 1 and
`retry_allowed=false`, then exited 1 before OutputRoot creation because the
approved short `R:\release\work` spelling and Git's canonical physical
`E:\KMTech\label-match-v2074-successor-20260821\release\work` spelling were
compared as text instead of as one directory identity. The builder log SHA-256
is `4870ec4c74ffcd00e89e01da40a4f19153464cd008acd3d0509b95517df9138b`
and the failure receipt SHA-256 is
`bca4e78cc36fdbc58704a8826ce751a63a388c851a7f57845c5ea93e4b8e004d`.
No ZIP, checksum, archive report, or Phase-B receipt was produced. Preserve
`E:\KMTech\label-match-v2074-successor-20260821`, its work clone, mirror, tag,
receipts, and logs as immutable quarantined history: do not retry, rebuild,
delete, recreate, retarget, reuse, or promote them. The successor selected after
that failure was v2.0.75.

On 2026-08-21 the sole v2.0.75 annotated tag object
`ebbfa7d1570f1b734db98e4b3208ada98b6eadb5`, for commit
`7458ed8258bd909b756e4a22976333e59f60a10f` and tree
`5c9983e65f4f77252f6f3d585285a555b6cb328b`, was created with the exact
16-byte `Release v2.0.75\n` message (SHA-256
`b7ec6cb414742449672bf41a3c16af8f4f756d8057cf94c6942423a5a630b7f0`)
and pushed only to its isolated local mirror. Its PASS exactly-once tag-burn
receipt SHA-256 is
`e87edae928bd9827efacda777a44bf56fd360b026367f70fbff4c561a170f6b6`.
The builder claim SHA-256 is
`6bea016b2eabffb634f1bab0c6fbe9e8da4a529d85e4971270b2955ef70241e7`
and records invocation count 1 with `retry_allowed=false`. The sole official
builder invocation exited 1 at its staged-installer verifier: the verifier
exited 2 after the public installer exited 1 because `Get-FileHash` was not
available at `INSTALL_THIS_PC.ps1:416`. The builder-runner receipt SHA-256 is
`477ff394459b594497a2099dffe0d244646fc55e5f06bf35c8b72658dd47bb71`
and the bounded builder log SHA-256 is
`db87fc7a53cad0e64503cd7e85dd70f6c26e7eeca165d279ded39f89a4d7e8bf`.
That frozen run classified the deeper module-resolution mechanism `UNPROVEN`;
the v2.0.76 successor later reproduced the cause as Windows PowerShell 5.1
inheriting a PowerShell 7 Utility module ahead of its compatible system module.
The quarantined partial output `E:\KMTech\l75r\candidate` contains 19,342 files
totalling 595,722,228 bytes and an early tag-identity report with SHA-256
`e4b66b0abe6b05ab15ef8126235aee6e0c0ea755c14cfe73200a77a816f228d3`.
No ZIP, checksum, archive report, Phase-1 receipt, or release notes were produced.
The final containment receipt SHA-256 is
`33400b900c0d21e218533eeb84e9db283a630c4db4d6528601b9ebf0b63c3a32`.
Preserve `E:\KMTech\label-match-v2075-successor-20260821`, its work clone,
mirror, tag, receipts, log, and partial output as immutable quarantined history:
do not retry, rebuild, delete, recreate, retarget, reuse, or promote them. The
successor selected after that failure was v2.0.76.

On 2026-08-21 the sole v2.0.76 annotated tag object
`23adf0a86a8bdadea91b6e988f1a2c237188322a`, for commit
`2504cb6efd66adbfc604685c8075987d086b0f44` and tree
`fbe5661f7c0195aa8edd1ef7957d6f52f73ceec2`, was created with the exact
16-byte `Release v2.0.76\n` message (SHA-256
`00b8f0960c91ed1ad69435befaf2ae9d8ad4136f043f9d3d9e2fb5587735fe18`)
and pushed only to its isolated local mirror. Its builder claim SHA-256 is
`e3a9d3f2629c5c68bfea36cb27b0f17fc4524b398395626c1f46cefccf85fd36`
and records one claimed invocation with `retry_allowed=false`. The sole outer
runner attempt then exited 1 before `Start-Process`: Windows PowerShell parsed
`Test-Path $stdout -or Test-Path $stderr` as one command and bound `-or` as an
invalid `Test-Path` parameter. The official builder process invocation count is
zero, and no candidate directory, ZIP, checksum, archive report, qualification
receipt, release notes, or builder log exists. The runner receipt SHA-256 is
`f674c0f76ee917ed7ec7ef4573e2d4c7a3336146f389a26df39a383f73b41f4b`,
and the final containment receipt SHA-256 is
`4b0bff3071f6c4756263fa61d5808bcf2237d5efae908dccca0cdefc58a25754`.
Preserve `E:\KMTech\label-match-v2076-successor-20260821`, its work clone,
mirror, tag, claims, and evidence as immutable quarantined history: do not
retry, rebuild, delete, recreate, retarget, reuse, or promote them. The active
successor is v2.0.80.

The v2.0.79 annotated tag object
`81f756bcb37aa1581005e2fa707ff8351ff169de` remains immutable at commit
`1ddf302c55c33ff6859f0eb2f44bfd9cb7ef8700` and tree
`b033b0c3188972019573225000a8be2284424b59`. Its official staged public-install
gate failed because the newly dot-sourced embedded host enabled strict mode in
the caller and turned an intentionally absent dry-run registration field into a
terminating property error. v2.0.80 is the required direct-child product
correction; it scopes strict mode to the embedded-host function while preserving
the public installer command and in-process install/registration execution. The
packaged `direct_sync_relay_runner.exe` remains the unchanged SYSTEM scheduled
relay action; only the retired install-pack and registration helper executables
remain forbidden package members.

The v2.0.80 annotated tag object
`b93d0097f23308b406111c17d2193f4b8b80381f` remains immutable at commit
`fe98c2a69d206e7e714ef0fa7ca268d54d50c7b1` and tree
`ea7469a5ae0af8dea2bce46e1b09ce4849074af6`. Its first official build stopped
at the staged-installer gate because an external task root produced a
260-character destination. The authorized short-path successor build cleared
that gate 4/4, then stopped before archive creation because the factory build
manifest sealed `tools/direct_sync_relay_runner.exe` while
`FACTORY_EXPECTED_FILES` omitted it. Preserve both attempts and the v2.0.80 tag
as immutable quarantined history.

The v2.0.81 annotated tag object
`1375dd564f2754f1f1ca82a489121ec40bded28b` remains immutable at commit
`7f976b5eedbceef829fd9692b1eb80194f44e7ff` and tree
`3ede781ff496311bfaa0c0808394d3686ea14a4c`. Its official builder created
internally qualified frozen bytes, but the independent verifier rejected the
preserved qualification receipt because its exact key contract omitted the
builder-emitted `path_identity`. The required post-build group also failed
0/12 because stale source/package runner-context assertions and fixtures still
expected the Python runner shape or omitted the bundled executable. Preserve
the v2.0.81 tag, bytes, and evidence as immutable quarantined history. The
v2.0.83 source identity remains frozen at reviewed X03 commit
`778ba13f1871574a81aa61da1eba28ff2374a37b`, which preserves X02 and the
stacked runner-context corrections while adding the exact receipt key. v2.0.84
is the required successor candidate from the authoritative v2.0.83-aligned
bounded diagnostic unit at commit
`b4b0630c35120ad9e240cd36bdb45bf4f380d06d`.

The governing order is Phase B/8.3, then Phase C/9, then Phase D/10. GitHub
Actions is not a release gate and cannot replace the local exact-SHA gates.

1. Run the required exact-SHA local CI in an isolated environment. Its focused
   native hash gate must run before mirror or tag preparation:

```powershell
python -m pytest -q -p no:cacheprovider `
  tests/test_staged_release_installer.py -k file_hash_authority `
  --basetemp <FRESH-EXTERNAL-E-BASETEMP>
```

   This source gate must prove both package-bound scripts without `Get-FileHash`
   available; the later staged-package gate repeats the same primitive proof
   against both exact packaged copies. If Hosted CI was not used, record
   `WAIVED_NOT_TESTED`; never turn absence into `PASS`.
2. Prepare an existing isolated local bare mirror and a separate clean,
   non-bare release work clone. The work clone's `origin` URL must be the exact
   absolute path (or `file://` URI) of that mirror, local `main` must be checked
   out, and work-clone `HEAD`, `refs/heads/main`,
   `refs/remotes/origin/main`, plus mirror `refs/heads/main` must all resolve to
   the exact candidate commit. A GitHub/HTTPS origin is not accepted here.
   All builder paths must be fully qualified local DOS-drive paths with no
   filesystem reparse component. A single short `SUBST` spelling is allowed
   only when its backing path resolves to a local disk and Win32 directory
   handles prove the same volume serial number and 128-bit file ID as the
   canonical physical path. The builder retains the short operational spelling
   for build tools, rejects different identities and nested/remote aliases,
   proves prospective fresh-output containment through its nearest existing
   parent, and reopens every protected directory identity after the build.
3. Create the one final intended annotated tag object in that isolated local
   mirror environment before release identity generation or build, and prepare
   the work clone with that identical tag object. The only authorized creation
   path is the repository's fail-closed, exactly-once burner:

```powershell
python -I .\tools\burn_local_release_tag_once.py `
  --repo-root <ABSOLUTE-RELEASE-WORK-CLONE> `
  --mirror-root <ABSOLUTE-LOCAL-BARE-MIRROR> `
  --evidence-root <FRESH-ABSOLUTE-TAG-BURN-EVIDENCE-DIRECTORY> `
  --tag v2.0.84 `
  --expected-commit <EXACT-CANDIDATE-COMMIT> `
  --expected-tree <EXACT-CANDIDATE-TREE>
```

   The evidence root must be fresh and external to the work clone and mirror.
   The burner has one invocation authority: regardless of PASS or failure, do not retry it,
   recreate or move the tag, or reuse its evidence root. Its
   complete canonical message is the single line below (with the normal final
   LF):

```text
Release v2.0.84
```

   Those 16 canonical bytes have SHA-256
   `330374f00d61c0410d8af110a6ed824cbd28773f46fb47a2843f59efbd97fc02`.

   Record its object ID, object type `tag`, and peeled commit before invoking
   `verify_release_identity.py` or any build command. Both the work clone and
   bare mirror must expose that exact object as `refs/tags/v2.0.84`, report type
   `tag`, and peel it to the candidate commit. Run the checked-in one-shot runner
   from the release work-clone root with fresh output, log paths, and the offline
   wheelhouse:

```powershell
powershell.exe -NoProfile -NonInteractive -ExecutionPolicy Bypass -File .\tools\run_frozen_release_candidate_once.ps1 `
  -PowerShellPath <EXACT-POWERSHELL-7-PWSH.EXE> `
  -OutputRoot <FRESH-ABSOLUTE-CANDIDATE-DIRECTORY> `
  -Tag v2.0.84 `
  -PythonPath <EXACT-WINDOWS-X64-CPYTHON-3.12.10> `
  -Wheelhouse <ABSOLUTE-OFFLINE-WHEELHOUSE> `
  -MirrorRoot <ABSOLUTE-LOCAL-BARE-MIRROR> `
  -StdoutPath <FRESH-EXTERNAL-BUILDER-STDOUT-LOG> `
  -StderrPath <FRESH-EXTERNAL-BUILDER-STDERR-LOG>
```

   The runner performs the Windows PowerShell 5.1-compatible AST and independent
   path-freshness prelaunch checks, then starts
   `tools\build_frozen_release_candidate.ps1` exactly once and returns its exit
   code; any runner or builder outcome consumes that attempt and must not be
   retried. The builder creates no refs and performs no fetch, push, tag mutation, or
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

The prerelease title must be exactly `Release v2.0.84`. Its body must be exactly
the following LF-normalized identity record (a single trailing newline is
allowed):

```text
Internal prerelease; not production-ready.
Tag: v2.0.84
Commit: <40 lowercase hex>
Tree: <40 lowercase hex>
Artifact: Label_Match-v2.0.84.zip
Artifact-SHA256: <64 lowercase hex>
Artifact-Size: <positive decimal bytes>
Main-EXE-SHA256: <64 lowercase hex>
Factory-Contract-SHA256: adaa08684ebb291837327f63f967a4f22650dff72c4c1dc56ce1a9bee6b5404a
Status: QUARANTINED_PENDING_FACTORY_QUALIFICATION
```

Repository immutable releases are an external pre-tag gate. Query
`GET /repos/KMTechn/Label_Match/immutable-releases` with GitHub API version
`2026-03-10` and require `enabled=true` before the tag is pushed. As of the
2026-08-12 read-only preflight it is `false`; do not publish v2.0.84 until an
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
commit/tree/version, contract bundle drift, abbreviated or malformed staged-v2
self-staging/launcher/uninstall/rollback/task evidence, missing CLI source
members, missing onedir runtime payload, unsafe Windows manifest paths, reparse
points, unbounded installer output, or a failed staged-installer claim. It safe-extracts to a temporary
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
