import re
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
WORKFLOW = ROOT / ".github" / "workflows" / "release.yml"


def _workflow() -> str:
    return WORKFLOW.read_text(encoding="utf-8")


def test_tag_workflow_is_verification_only_and_read_only():
    workflow = _workflow()

    assert "Verify Frozen GitHub Release for Label_Match" in workflow
    assert 'tags:\n      - "v*"' in workflow
    assert "contents: read" in workflow
    assert "actions: read" in workflow
    assert "contents: write" not in workflow
    assert "workflow_dispatch:" not in workflow
    assert "repository_dispatch:" not in workflow
    assert "PyInstaller" not in workflow
    assert "build_release_archive" not in workflow
    assert "Build and verify internal release archive" not in workflow
    assert "Generate release SHA256 checksum" not in workflow
    assert "Create Release and Upload Asset" not in workflow
    assert "softprops/action-gh-release" not in workflow
    assert "Generate private update manifest" not in workflow
    assert "Sign private update manifest" not in workflow
    assert "Promote private update feed" not in workflow
    assert "gh release create" not in workflow
    assert "gh release upload" not in workflow


def test_tag_workflow_keeps_exact_source_gate_and_reports_hosted_ci_non_blockingly():
    workflow = _workflow()

    assert "fetch-depth: 0" in workflow
    assert "persist-credentials: false" in workflow
    assert "tools/verify_release_identity.py" in workflow
    assert '--expected-tag "$env:LABEL_MATCH_RELEASE_TAG"' in workflow
    assert '--expected-sha "$env:GITHUB_SHA"' in workflow
    assert '--reviewed-ref "refs/remotes/origin/main"' in workflow
    assert "Record exact-SHA Hosted CI status without making it a release gate" in workflow
    assert "actions/workflows/ci.yml/runs" in workflow
    assert "hosted_ci=PASS_NON_GATING" in workflow
    assert "hosted_ci=WAIVED_NOT_TESTED" in workflow
    assert "exact release SHA must have exactly one" not in workflow
    assert "run_attempt -ne 1" not in workflow
    assert "tools/verify_release_tag_attestation.py" in workflow
    assert "label-match-canonical-tag-identity.json" in workflow
    assert "label-match-canonical-annotated-tag-v1" in workflow
    assert "$localTagType -cne \"tag\"" in workflow
    assert "$localTagCommit -cne $releaseCommit" in workflow
    assert "$reviewedMain -cne $releaseCommit" in workflow
    assert "$tagIdentity.tag_object -cne $localTagObject" in workflow
    assert '$tagIdentity.message -cne "Release $env:LABEL_MATCH_RELEASE_TAG"' in workflow
    assert "Qualified-ZIP-SHA256" not in workflow


def test_tag_workflow_boundedly_waits_for_exact_external_prerelease():
    workflow = _workflow()

    assert "Wait boundedly for externally published frozen prerelease" in workflow
    assert "$attemptLimit = 180" in workflow
    assert "Start-Sleep -Seconds 10" in workflow
    assert "within 1800 seconds" in workflow
    assert "$candidateRelease.draft -eq $false" in workflow
    assert "$candidateRelease.prerelease -eq $true" in workflow
    assert "$candidateRelease.immutable -eq $true" in workflow
    assert "$assetsReady" in workflow
    assert "$_.digest -cnotmatch '^sha256:[0-9a-f]{64}$'" in workflow
    assert 'releases/tags/$env:LABEL_MATCH_RELEASE_TAG' in workflow
    assert workflow.count('X-GitHub-Api-Version: 2026-03-10') == 2
    assert 'repos/$env:GITHUB_REPOSITORY/immutable-releases' not in workflow
    assert "$release.immutable -ne $true" in workflow
    assert "$release.draft -ne $false" in workflow
    assert "$release.prerelease -ne $true" in workflow
    assert "$release.immutable -ne $true" in workflow
    assert 'git ls-remote origin "refs/heads/main"' in workflow
    assert "$remoteMainCommit -cne $releaseCommit" in workflow
    assert '$release.target_commitish -cne "main"' in workflow
    assert "$release.target_commitish -cne $releaseCommit" in workflow
    assert '"Commit: $releaseCommit"' in workflow
    assert '"Tree: $releaseTree"' in workflow
    assert "'^Artifact-SHA256: ([0-9a-f]{64})$'" in workflow
    assert "'^Artifact-Size: ([1-9][0-9]*)$'" in workflow
    assert "'^Main-EXE-SHA256: ([0-9a-f]{64})$'" in workflow
    assert '"Status: QUARANTINED_PENDING_FACTORY_QUALIFICATION"' in workflow
    assert "release title or exact identity notes are not canonical" in workflow
    assert "release must contain exactly the frozen ZIP and checksum" in workflow
    assert "$actualAssets.Count -ne 2" in workflow
    assert '$asset.state -cne "uploaded"' in workflow
    assert "$zipAsset[0].size -ne $artifactSize" in workflow
    assert '$zipAsset[0].digest -cne "sha256:$artifactSha256"' in workflow


def test_tag_workflow_downloads_only_zip_and_checksum_to_fresh_temp():
    workflow = _workflow()

    assert 'gh release download "$env:LABEL_MATCH_RELEASE_TAG"' in workflow
    assert '--pattern "$zipName"' in workflow
    assert '--pattern "$checksumName"' in workflow
    assert 'label-match-frozen-$env:GITHUB_RUN_ID-$env:GITHUB_RUN_ATTEMPT' in workflow
    assert "Fresh frozen-asset path already exists" in workflow
    assert "$downloaded.Count -ne 2" in workflow
    assert "Clean downloaded verification inputs" in workflow
    assert "Refusing to clean frozen assets outside RUNNER_TEMP" in workflow


def test_tag_workflow_invokes_independent_standard_library_verifier():
    workflow = _workflow()

    assert "python -I -S tools/verify_frozen_release_assets.py" in workflow
    assert "--archive (Join-Path $env:FROZEN_ASSET_ROOT $env:FROZEN_ZIP_NAME)" in workflow
    assert "--checksum (Join-Path $env:FROZEN_ASSET_ROOT $env:FROZEN_CHECKSUM_NAME)" in workflow
    assert '--expected-commit "$releaseCommit"' in workflow
    assert '--expected-tree "$releaseTree"' in workflow
    assert '--expected-tag-object "$($tagIdentity.tag_object)"' in workflow
    assert '--expected-source-epoch "$sourceEpoch"' in workflow
    assert '--expected-archive-sha256 "$env:EXPECTED_ARCHIVE_SHA256"' in workflow
    assert '--expected-archive-size "$env:EXPECTED_ARCHIVE_SIZE"' in workflow
    assert '--expected-main-exe-sha256 "$env:EXPECTED_MAIN_EXE_SHA256"' in workflow
    assert "--report \"$env:RUNNER_TEMP\\label-match-frozen-release-verification.json\"" in workflow
    assert "external_phase1_receipt_parity=REQUIRED_NOT_TESTED" in workflow
    assert "--qualification-receipt" not in workflow


def test_final_remote_metadata_recheck_is_fail_closed():
    workflow = _workflow()

    assert "Recheck immutable remote metadata after byte verification" in workflow
    assert "release or asset IDs/sizes/digests changed during verification" in workflow
    assert 'git ls-remote origin "refs/tags/$env:LABEL_MATCH_RELEASE_TAG"' in workflow
    assert 'git ls-remote origin "refs/tags/$env:LABEL_MATCH_RELEASE_TAG^{}"' in workflow
    assert 'git ls-remote origin "refs/heads/main"' in workflow
    assert "$tagObject -cne $tagIdentity.tag_object" in workflow
    assert "$peeledCommit -cne $releaseCommit" in workflow
    assert "$mainCommit -cne $releaseCommit" in workflow


def test_every_external_action_is_pinned_to_a_full_commit():
    uses = re.findall(r"(?m)^\s+uses:\s+([^\s#]+)", _workflow())

    assert uses
    assert all(re.search(r"@[0-9a-f]{40}$", value) for value in uses)


def test_local_gate_remains_authoritative_with_hosted_and_physical_evidence_separate():
    ci = (ROOT / ".github" / "workflows" / "ci.yml").read_text(encoding="utf-8")
    release = _workflow()

    assert "pull_request:" not in ci
    assert "workflow_dispatch:" not in ci
    assert "cancel-in-progress: true" in ci
    assert ci.count("python -m pytest") == 2
    assert "if: steps.ui_scope.outputs.required == 'true'" in ci
    assert "test_live_submission_retry_hides_raw_server_error" in ci
    assert "test_display2_1366_scale100" in ci
    assert "Record exact-SHA Hosted CI status without making it a release gate" in release
    assert "hosted_ci=WAIVED_NOT_TESTED" in release
    assert "TEST1" not in release
