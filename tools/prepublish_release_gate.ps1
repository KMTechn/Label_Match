[CmdletBinding()]
param(
    [Parameter(Mandatory = $true)]
    [ValidateSet("AcquireDraft", "VerifyLocal", "PublishDraft")]
    [string]$Mode,

    [Parameter(Mandatory = $true)]
    [ValidatePattern('^v\d+\.\d+\.\d+$')]
    [string]$Tag,

    [Parameter(Mandatory = $true)]
    [ValidatePattern('^[0-9a-fA-F]{40}(?:[0-9a-fA-F]{24})?$')]
    [string]$ExpectedCommit,

    [Parameter(Mandatory = $true)]
    [string]$ZipName,

    [Parameter(Mandatory = $true)]
    [string]$ChecksumName,

    [Parameter(Mandatory = $true)]
    [string]$RecordMember,

    [Parameter(Mandatory = $true)]
    [string]$WorkRoot,

    [Parameter(Mandatory = $true)]
    [string]$StatePath,

    [string]$VerifierReportPath = "",

    [ValidateRange(1, 180)]
    [int]$MaximumAttempts = 180,

    [ValidateRange(1, 60)]
    [int]$PollSeconds = 10
)

$ErrorActionPreference = "Stop"
Set-StrictMode -Version Latest
Add-Type -AssemblyName System.IO.Compression.FileSystem

function Get-FileSha256 {
    param([Parameter(Mandatory = $true)][string]$LiteralPath)
    return (Get-FileHash -LiteralPath $LiteralPath -Algorithm SHA256).Hash.ToLowerInvariant()
}

function Assert-LocalArchiveGate {
    param(
        [Parameter(Mandatory = $true)][string]$ZipPath,
        [Parameter(Mandatory = $true)][string]$ChecksumPath
    )
    if (-not (Test-Path -LiteralPath $ZipPath -PathType Leaf)) {
        throw "Prepublish ZIP is missing: $ZipPath"
    }
    if (-not (Test-Path -LiteralPath $ChecksumPath -PathType Leaf)) {
        throw "Prepublish checksum is missing: $ChecksumPath"
    }
    $zipHash = Get-FileSha256 -LiteralPath $ZipPath
    $zipSize = (Get-Item -LiteralPath $ZipPath).Length
    if ($zipSize -lt 1) {
        throw "Prepublish ZIP must not be empty."
    }
    $checksumLines = @(
        Get-Content -LiteralPath $ChecksumPath |
            Where-Object { -not [string]::IsNullOrWhiteSpace($_) }
    )
    $expectedChecksumLine = "$zipHash  $ZipName"
    if ($checksumLines.Count -ne 1 -or $checksumLines[0] -cne $expectedChecksumLine) {
        throw "Checksum must contain the exact ZIP SHA256 and filename."
    }

    $archive = [IO.Compression.ZipFile]::OpenRead((Resolve-Path -LiteralPath $ZipPath))
    try {
        $recordEntries = @($archive.Entries | Where-Object {
            -not [string]::IsNullOrEmpty($_.Name) -and $_.FullName -ceq $RecordMember
        })
        if ($recordEntries.Count -ne 1 -or $recordEntries[0].Length -lt 1) {
            throw "Release ZIP must contain exactly one nonempty integrity record: $RecordMember"
        }
    } finally {
        $archive.Dispose()
    }
    return [pscustomobject]@{
        zip_sha256 = $zipHash
        zip_size = $zipSize
        checksum_sha256 = Get-FileSha256 -LiteralPath $ChecksumPath
        checksum_size = (Get-Item -LiteralPath $ChecksumPath).Length
    }
}

function Assert-VerifierReport {
    param(
        [Parameter(Mandatory = $true)][string]$ReportPath,
        [Parameter(Mandatory = $true)][string]$ExpectedZipSha256
    )
    if (-not (Test-Path -LiteralPath $ReportPath -PathType Leaf)) {
        throw "App-owned verifier report is missing: $ReportPath"
    }
    $report = Get-Content -Raw -Encoding UTF8 -LiteralPath $ReportPath | ConvertFrom-Json
    if ([string]$report.status -notin @("PASS", "PASS_SELF_CONSISTENCY")) {
        throw "App-owned verifier report status is not PASS."
    }
    if ($null -eq $report.bootstrap_integrity -or
        [string]$report.bootstrap_integrity.status -cne "PASS") {
        throw "App-owned verifier did not accept bootstrap-integrity.json."
    }
    $reportedZipSha256 = ""
    if ($null -ne $report.archive -and
        $null -ne $report.archive.PSObject.Properties["sha256"]) {
        $reportedZipSha256 = [string]$report.archive.sha256
    } elseif ($null -ne $report.PSObject.Properties["archive_sha256"]) {
        $reportedZipSha256 = [string]$report.archive_sha256
    }
    if ($reportedZipSha256.ToLowerInvariant() -cne $ExpectedZipSha256) {
        throw "App-owned verifier report is not bound to the exact prepublish ZIP."
    }
    return $report
}

function Get-Release {
    $raw = gh api `
        -H "X-GitHub-Api-Version: 2026-03-10" `
        "repos/$env:GITHUB_REPOSITORY/releases/tags/$Tag" 2>$null
    if ($LASTEXITCODE -ne 0) {
        $LASTEXITCODE = 0
        return $null
    }
    return ConvertFrom-Json -InputObject $raw
}

function Get-BodyEvidence {
    param([Parameter(Mandatory = $true)][string]$Body)
    $normalized = $Body.Replace("`r`n", "`n").TrimEnd([char[]]"`n")
    $hashMatch = [regex]::Match($normalized, '(?m)^Artifact-SHA256: ([0-9a-f]{64})$')
    $sizeMatch = [regex]::Match($normalized, '(?m)^Artifact-Size: ([1-9][0-9]*)$')
    $exeMatch = [regex]::Match($normalized, '(?m)^Main-EXE-SHA256: ([0-9a-f]{64})$')
    if (-not $hashMatch.Success -or -not $sizeMatch.Success -or -not $exeMatch.Success) {
        return $null
    }
    [int64]$size = 0
    if (-not [int64]::TryParse($sizeMatch.Groups[1].Value, [ref]$size) -or $size -lt 1) {
        return $null
    }
    foreach ($requiredLine in @(
        "Tag: $Tag",
        "Commit: $($ExpectedCommit.ToLowerInvariant())",
        "Artifact: $ZipName"
    )) {
        if (@($normalized -split "`n" | Where-Object { $_ -ceq $requiredLine }).Count -ne 1) {
            return $null
        }
    }
    return [pscustomobject]@{
        normalized_body = $normalized
        zip_sha256 = $hashMatch.Groups[1].Value
        zip_size = $size
        main_exe_sha256 = $exeMatch.Groups[1].Value
    }
}

function Assert-ReleaseMatchesState {
    param(
        [Parameter(Mandatory = $true)]$Release,
        [Parameter(Mandatory = $true)]$State,
        [Parameter(Mandatory = $true)][bool]$ExpectedDraft,
        [Parameter(Mandatory = $true)][bool]$ExpectedImmutable
    )
    $body = ([string]$Release.body).Replace("`r`n", "`n").TrimEnd([char[]]"`n")
    if (
        [string]$Release.id -cne [string]$State.release_id -or
        $Release.tag_name -cne $Tag -or
        $Release.name -cne "Release $Tag" -or
        [bool]$Release.draft -ne $ExpectedDraft -or
        [bool]$Release.prerelease -ne $true -or
        [bool]$Release.immutable -ne $ExpectedImmutable -or
        [string]$Release.target_commitish -cne [string]$State.target_commitish -or
        $body -cne [string]$State.body
    ) {
        throw "Release identity or state changed across the prepublish gate."
    }
    $assets = @($Release.assets)
    $stateAssets = @($State.assets)
    if ($assets.Count -ne 2 -or $stateAssets.Count -ne 2) {
        throw "Release must contain exactly the ZIP and checksum assets."
    }
    foreach ($expected in $stateAssets) {
        $actual = @($assets | Where-Object { $_.name -ceq [string]$expected.name })
        if (
            $actual.Count -ne 1 -or
            [string]$actual[0].id -cne [string]$expected.id -or
            [int64]$actual[0].size -ne [int64]$expected.size -or
            [string]$actual[0].digest -cne [string]$expected.digest -or
            [string]$actual[0].state -cne "uploaded"
        ) {
            throw "Release asset identity changed across the prepublish gate: $($expected.name)"
        }
    }
}

$resolvedWorkRoot = [IO.Path]::GetFullPath($WorkRoot)
$resolvedStatePath = [IO.Path]::GetFullPath($StatePath)
$expectedCommitLower = $ExpectedCommit.ToLowerInvariant()
$zipPath = Join-Path $resolvedWorkRoot $ZipName
$checksumPath = Join-Path $resolvedWorkRoot $ChecksumName

if ($Mode -ceq "VerifyLocal") {
    $local = Assert-LocalArchiveGate -ZipPath $zipPath -ChecksumPath $checksumPath
    [void](Assert-VerifierReport -ReportPath $VerifierReportPath -ExpectedZipSha256 $local.zip_sha256)
    Write-Output "prepublish_release_gate=PASS mode=VerifyLocal bootstrap_integrity=PASS zip_sha256=$($local.zip_sha256)"
    return
}

if ([string]::IsNullOrWhiteSpace($env:GITHUB_REPOSITORY)) {
    throw "GITHUB_REPOSITORY is required for draft acquisition or publication."
}

if ($Mode -ceq "AcquireDraft") {
    if (Test-Path -LiteralPath $resolvedWorkRoot) {
        throw "Fresh prepublish work root already exists: $resolvedWorkRoot"
    }
    if (Test-Path -LiteralPath $resolvedStatePath) {
        throw "Fresh prepublish state path already exists: $resolvedStatePath"
    }
    [IO.Directory]::CreateDirectory($resolvedWorkRoot) | Out-Null
    $release = $null
    $evidence = $null
    for ($attempt = 1; $attempt -le $MaximumAttempts; $attempt++) {
        $candidate = Get-Release
        if ($null -ne $candidate) {
            $candidateEvidence = Get-BodyEvidence -Body ([string]$candidate.body)
            $assets = @($candidate.assets)
            $zipAsset = @($assets | Where-Object { $_.name -ceq $ZipName -and $_.state -ceq "uploaded" })
            $checksumAsset = @($assets | Where-Object { $_.name -ceq $ChecksumName -and $_.state -ceq "uploaded" })
            $target = [string]$candidate.target_commitish
            if (
                $candidate.tag_name -ceq $Tag -and
                $candidate.name -ceq "Release $Tag" -and
                $candidate.draft -eq $true -and
                $candidate.prerelease -eq $true -and
                $candidate.immutable -ne $true -and
                ($target -ceq $expectedCommitLower -or $target -ceq "main") -and
                $null -ne $candidateEvidence -and
                $assets.Count -eq 2 -and
                $zipAsset.Count -eq 1 -and
                $checksumAsset.Count -eq 1 -and
                $zipAsset[0].digest -cmatch '^sha256:[0-9a-f]{64}$' -and
                $checksumAsset[0].digest -cmatch '^sha256:[0-9a-f]{64}$' -and
                [int64]$zipAsset[0].size -eq $candidateEvidence.zip_size -and
                $zipAsset[0].digest -ceq "sha256:$($candidateEvidence.zip_sha256)"
            ) {
                $release = $candidate
                $evidence = $candidateEvidence
                break
            }
        }
        if ($attempt -lt $MaximumAttempts) {
            Write-Output "draft_release_wait attempt=$attempt/$MaximumAttempts"
            Start-Sleep -Seconds $PollSeconds
        }
    }
    if ($null -eq $release) {
        throw "A canonical draft prerelease with exact staged assets was not found before timeout."
    }
    gh release download $Tag `
        --repo $env:GITHUB_REPOSITORY `
        --pattern $ZipName `
        --pattern $ChecksumName `
        --dir $resolvedWorkRoot
    if ($LASTEXITCODE -ne 0) {
        throw "Draft release asset download failed."
    }
    $local = Assert-LocalArchiveGate -ZipPath $zipPath -ChecksumPath $checksumPath
    $zipAsset = @($release.assets | Where-Object { $_.name -ceq $ZipName })[0]
    $checksumAsset = @($release.assets | Where-Object { $_.name -ceq $ChecksumName })[0]
    if (
        $local.zip_sha256 -cne $evidence.zip_sha256 -or
        $local.zip_size -ne $evidence.zip_size -or
        $local.zip_sha256 -cne $zipAsset.digest.Substring(7) -or
        $local.zip_size -ne [int64]$zipAsset.size -or
        $local.checksum_sha256 -cne $checksumAsset.digest.Substring(7) -or
        $local.checksum_size -ne [int64]$checksumAsset.size
    ) {
        throw "Downloaded draft assets differ from GitHub metadata or release body evidence."
    }
    $state = [ordered]@{
        schema_version = "kmtech-prepublish-release-gate-v1"
        release_id = $release.id
        tag = $Tag
        expected_commit = $expectedCommitLower
        target_commitish = $release.target_commitish
        name = $release.name
        body = $evidence.normalized_body
        zip_name = $ZipName
        checksum_name = $ChecksumName
        record_member = $RecordMember
        zip_sha256 = $local.zip_sha256
        zip_size = $local.zip_size
        checksum_sha256 = $local.checksum_sha256
        checksum_size = $local.checksum_size
        main_exe_sha256 = $evidence.main_exe_sha256
        assets = @($release.assets | Sort-Object name | ForEach-Object {
            [ordered]@{
                id = $_.id
                name = $_.name
                size = $_.size
                digest = $_.digest
                state = $_.state
            }
        })
    }
    $state | ConvertTo-Json -Depth 6 | Set-Content -Encoding utf8NoBOM -LiteralPath $resolvedStatePath
    if ([string]::IsNullOrWhiteSpace($env:GITHUB_ENV)) {
        throw "GITHUB_ENV is required to export exact draft asset evidence."
    }
    foreach ($line in @(
        "PREPUBLISH_ZIP_PATH=$zipPath",
        "PREPUBLISH_CHECKSUM_PATH=$checksumPath",
        "PREPUBLISH_STATE_PATH=$resolvedStatePath",
        "PREPUBLISH_ZIP_SHA256=$($local.zip_sha256)",
        "PREPUBLISH_ZIP_SIZE=$($local.zip_size)",
        "PREPUBLISH_CHECKSUM_SHA256=$($local.checksum_sha256)",
        "PREPUBLISH_CHECKSUM_SIZE=$($local.checksum_size)",
        "PREPUBLISH_MAIN_EXE_SHA256=$($evidence.main_exe_sha256)"
    )) {
        $line | Out-File -FilePath $env:GITHUB_ENV -Encoding utf8 -Append
    }
    Write-Output "prepublish_release_gate=ACQUIRED_DRAFT record_member=$RecordMember zip_sha256=$($local.zip_sha256)"
    return
}

if (-not (Test-Path -LiteralPath $resolvedStatePath -PathType Leaf)) {
    throw "Prepublish state is missing: $resolvedStatePath"
}
$state = Get-Content -Raw -Encoding UTF8 -LiteralPath $resolvedStatePath | ConvertFrom-Json
if (
    $state.schema_version -cne "kmtech-prepublish-release-gate-v1" -or
    $state.tag -cne $Tag -or
    $state.expected_commit -cne $expectedCommitLower -or
    $state.zip_name -cne $ZipName -or
    $state.checksum_name -cne $ChecksumName -or
    $state.record_member -cne $RecordMember
) {
    throw "Prepublish state identity differs from the requested release."
}
$local = Assert-LocalArchiveGate -ZipPath $zipPath -ChecksumPath $checksumPath
if (
    $local.zip_sha256 -cne [string]$state.zip_sha256 -or
    $local.zip_size -ne [int64]$state.zip_size -or
    $local.checksum_sha256 -cne [string]$state.checksum_sha256 -or
    $local.checksum_size -ne [int64]$state.checksum_size
) {
    throw "Prepublish asset bytes changed after draft acquisition."
}
[void](Assert-VerifierReport -ReportPath $VerifierReportPath -ExpectedZipSha256 $local.zip_sha256)
$draft = Get-Release
if ($null -eq $draft) {
    throw "Draft release disappeared before publication."
}
Assert-ReleaseMatchesState -Release $draft -State $state -ExpectedDraft $true -ExpectedImmutable $false
gh release edit $Tag --repo $env:GITHUB_REPOSITORY --draft=false --prerelease=true --latest=false
if ($LASTEXITCODE -ne 0) {
    throw "Verified draft prerelease publication failed."
}
$published = $null
for ($attempt = 1; $attempt -le 30; $attempt++) {
    $candidate = Get-Release
    if ($null -ne $candidate -and $candidate.draft -eq $false -and
        $candidate.prerelease -eq $true -and $candidate.immutable -eq $true) {
        $published = $candidate
        break
    }
    if ($attempt -lt 30) { Start-Sleep -Seconds 2 }
}
if ($null -eq $published) {
    throw "Published prerelease did not become immutable within 60 seconds."
}
Assert-ReleaseMatchesState -Release $published -State $state -ExpectedDraft $false -ExpectedImmutable $true
Write-Output "prepublish_release_gate=PUBLISHED_AFTER_PASS release_id=$($state.release_id) zip_sha256=$($state.zip_sha256)"
