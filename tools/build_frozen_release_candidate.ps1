[CmdletBinding()]
param(
    [Parameter(Mandatory = $true)]
    [string]$OutputRoot,

    [ValidatePattern('^v(?:0|[1-9][0-9]*)\.(?:0|[1-9][0-9]*)\.(?:0|[1-9][0-9]*)$')]
    [string]$Tag = "v2.0.67",

    [string]$PythonPath = "",

    [Parameter(Mandatory = $true)]
    [string]$Wheelhouse,

    [Parameter(Mandatory = $true)]
    [string]$MirrorRoot
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"
$PSNativeCommandUseErrorActionPreference = $true

$ExpectedPythonVersion = "3.12.10"
$ExpectedPyInstallerVersion = "6.20.0"
$FactoryContractSha256 = "adaa08684ebb291837327f63f967a4f22650dff72c4c1dc56ce1a9bee6b5404a"
$ProbeName = "KMTechActiveWorkProbe"
$ProbeVersion = "v1.0.3.4"
$AllProbeApps = "Inspection_worker,Rework_worker,Defect_Inspection,Container_Audit,Label_Match"

function Assert-LastExitCode {
    param([Parameter(Mandatory = $true)][string]$Operation)
    if ($LASTEXITCODE -ne 0) {
        throw "$Operation failed with exit code $LASTEXITCODE."
    }
}

function Get-GitValueAt {
    param(
        [Parameter(Mandatory = $true)][string]$Repository,
        [Parameter(Mandatory = $true)][string[]]$Arguments
    )
    $value = (& git -C $Repository @Arguments)
    if ($LASTEXITCODE -ne 0) {
        throw "Git command failed in $Repository`: git $($Arguments -join ' ')"
    }
    if ($null -eq $value) {
        return
    }
    return (([string[]]$value) -join "`n").Trim()
}

function Get-NormalizedLocalOriginPath {
    param([Parameter(Mandatory = $true)][string]$RemoteUrl)
    if ($RemoteUrl.StartsWith("file://", [StringComparison]::OrdinalIgnoreCase)) {
        return [IO.Path]::GetFullPath(([Uri]$RemoteUrl).LocalPath).TrimEnd([char[]]"\/")
    }
    if (-not [IO.Path]::IsPathRooted($RemoteUrl)) {
        throw "Prepared clone origin must be an absolute local path or file URI."
    }
    return [IO.Path]::GetFullPath($RemoteUrl).TrimEnd([char[]]"\/")
}

function Write-NewUtf8File {
    param(
        [Parameter(Mandatory = $true)][string]$Path,
        [Parameter(Mandatory = $true)][string]$Text
    )
    $stream = [IO.File]::Open(
        $Path,
        [IO.FileMode]::CreateNew,
        [IO.FileAccess]::Write,
        [IO.FileShare]::None
    )
    try {
        $writer = [IO.StreamWriter]::new($stream, [Text.UTF8Encoding]::new($false))
        try {
            $writer.NewLine = "`n"
            $writer.Write($Text)
            $writer.Flush()
        }
        finally {
            $writer.Dispose()
        }
    }
    finally {
        $stream.Dispose()
    }
}

function Copy-RequiredFile {
    param(
        [Parameter(Mandatory = $true)][string]$Source,
        [Parameter(Mandatory = $true)][string]$Destination
    )
    if (-not (Test-Path -LiteralPath $Source -PathType Leaf)) {
        throw "Required release input is missing: $Source"
    }
    if (Test-Path -LiteralPath $Destination) {
        throw "Fresh release destination already exists: $Destination"
    }
    $destinationParent = Split-Path -Parent $Destination
    [IO.Directory]::CreateDirectory($destinationParent) | Out-Null
    Copy-Item -LiteralPath $Source -Destination $Destination
}

function Assert-ProbeIdentity {
    param(
        [Parameter(Mandatory = $true)][string]$Path,
        [Parameter(Mandatory = $true)][string]$ExpectedWorkflowMode,
        [Parameter(Mandatory = $true)][string[]]$ExpectedSupportedApps,
        [Parameter(Mandatory = $true)][string]$ExpectedSourceCommit,
        [Parameter(Mandatory = $true)][string]$ExpectedArtifactSha256
    )
    $identity = Get-Content -Raw -Encoding UTF8 -LiteralPath $Path | ConvertFrom-Json
    if ($identity.schema_version -cne "kmtech-active-work-probe-build-v1.0.3.4") {
        throw "Probe identity schema mismatch: $Path"
    }
    if ($identity.probe_source_commit -cne $ExpectedSourceCommit) {
        throw "Probe source commit mismatch: $Path"
    }
    if ($identity.workflow_mode -cne $ExpectedWorkflowMode) {
        throw "Probe workflow mode mismatch: $Path"
    }
    if ($identity.probe_name -cne $ProbeName -or $identity.probe_version -cne $ProbeVersion) {
        throw "Probe name or version mismatch: $Path"
    }
    if ($identity.probe_artifact_sha256 -cne $ExpectedArtifactSha256) {
        throw "Probe artifact hash mismatch: $Path"
    }
    $supportedApps = @($identity.supported_apps)
    if (
        $supportedApps.Count -ne $ExpectedSupportedApps.Count -or
        ($supportedApps -join ",") -cne ($ExpectedSupportedApps -join ",")
    ) {
        throw "Probe supported-app scope mismatch: $Path"
    }
}

function Invoke-OneFileBuild {
    param(
        [Parameter(Mandatory = $true)][string]$VenvPython,
        [Parameter(Mandatory = $true)][string]$RepositoryRoot,
        [Parameter(Mandatory = $true)][string]$PackageRoot,
        [Parameter(Mandatory = $true)][string]$WorkRoot,
        [Parameter(Mandatory = $true)][string]$Name,
        [Parameter(Mandatory = $true)][string]$Source
    )
    $toolWork = Join-Path $WorkRoot "${Name}_pyinstaller"
    & $VenvPython -I -m PyInstaller `
        --paths $RepositoryRoot `
        --name $Name `
        --onefile `
        --console `
        --distpath $PackageRoot `
        --workpath $toolWork `
        --specpath $toolWork `
        --clean `
        --noupx `
        --noconfirm `
        $Source
    Assert-LastExitCode "PyInstaller build for $Name"
}

$repoRoot = [IO.Path]::GetFullPath((Join-Path $PSScriptRoot "..")).TrimEnd([char[]]"\/")
$mirrorRoot = [IO.Path]::GetFullPath($MirrorRoot).TrimEnd([char[]]"\/")
$originalLocation = (Get-Location).Path
$repoPrefix = $repoRoot + [IO.Path]::DirectorySeparatorChar
$mirrorPrefix = $mirrorRoot + [IO.Path]::DirectorySeparatorChar
$resolvedOutputRoot = [IO.Path]::GetFullPath($OutputRoot).TrimEnd([char[]]"\/")
$resolvedOutputPrefix = $resolvedOutputRoot + [IO.Path]::DirectorySeparatorChar
$outputDriveRoot = [IO.Path]::GetPathRoot($resolvedOutputRoot).TrimEnd([IO.Path]::DirectorySeparatorChar)
if (-not (Test-Path -LiteralPath $mirrorRoot -PathType Container)) {
    throw "MirrorRoot must be an existing prepared local bare mirror."
}
if ($repoRoot.Equals($mirrorRoot, [StringComparison]::OrdinalIgnoreCase)) {
    throw "The release work clone and local bare mirror must be isolated paths."
}
if (
    $resolvedOutputRoot -ceq "" -or
    $resolvedOutputRoot.Equals($outputDriveRoot, [StringComparison]::OrdinalIgnoreCase) -or
    $resolvedOutputRoot.Equals($repoRoot, [StringComparison]::OrdinalIgnoreCase) -or
    $resolvedOutputRoot.Equals($mirrorRoot, [StringComparison]::OrdinalIgnoreCase) -or
    $resolvedOutputPrefix.StartsWith($repoPrefix, [StringComparison]::OrdinalIgnoreCase) -or
    $resolvedOutputPrefix.StartsWith($mirrorPrefix, [StringComparison]::OrdinalIgnoreCase)
) {
    throw "OutputRoot must be a dedicated directory outside the release work clone and local bare mirror: $resolvedOutputRoot"
}
if (Test-Path -LiteralPath $resolvedOutputRoot) {
    throw "OutputRoot must be fresh and must not already exist: $resolvedOutputRoot"
}
$resolvedWheelhouse = [IO.Path]::GetFullPath($Wheelhouse)
if (-not (Test-Path -LiteralPath $resolvedWheelhouse -PathType Container)) {
    throw "The offline wheelhouse is missing: $resolvedWheelhouse"
}
if ([string]::IsNullOrWhiteSpace($PythonPath)) {
    $pythonCommand = Get-Command python.exe -CommandType Application -ErrorAction Stop
    $resolvedPython = [IO.Path]::GetFullPath($pythonCommand.Source)
}
else {
    $resolvedPython = [IO.Path]::GetFullPath($PythonPath)
}
if (-not (Test-Path -LiteralPath $resolvedPython -PathType Leaf)) {
    throw "PythonPath is not an executable file: $resolvedPython"
}
Set-Location -LiteralPath $repoRoot
try {

$insideWorkTree = Get-GitValueAt -Repository $repoRoot -Arguments @(
    "rev-parse", "--is-inside-work-tree"
)
$workCloneIsBare = Get-GitValueAt -Repository $repoRoot -Arguments @(
    "rev-parse", "--is-bare-repository"
)
if ($insideWorkTree -cne "true" -or $workCloneIsBare -cne "false") {
    throw "The builder must run from a non-bare isolated release work clone."
}
$worktreeTop = [IO.Path]::GetFullPath(
    (Get-GitValueAt -Repository $repoRoot -Arguments @("rev-parse", "--show-toplevel"))
).TrimEnd([char[]]"\/")
if ($worktreeTop -cne $repoRoot) {
    throw "The builder script must belong to the isolated release work clone root."
}
if (
    (Get-GitValueAt -Repository $mirrorRoot -Arguments @(
        "rev-parse", "--is-bare-repository"
    )) -cne "true"
) {
    throw "MirrorRoot must be a bare Git repository."
}
$status = Get-GitValueAt -Repository $repoRoot -Arguments @(
    "status", "--porcelain=v1", "--untracked-files=all"
)
if (-not [string]::IsNullOrEmpty($status)) {
    throw "Isolated release work clone must be clean before the one-shot build."
}
$headRef = Get-GitValueAt -Repository $repoRoot -Arguments @(
    "symbolic-ref", "--quiet", "HEAD"
)
if ($headRef -cne "refs/heads/main") {
    throw "Prepared release work clone must have exact local main checked out."
}
$originUrl = Get-GitValueAt -Repository $repoRoot -Arguments @(
    "remote", "get-url", "origin"
)
$localOriginPath = Get-NormalizedLocalOriginPath -RemoteUrl $originUrl
if ($localOriginPath -cne $mirrorRoot) {
    throw "Prepared release work clone origin must be the exact supplied local bare mirror."
}
$headCommit = (Get-GitValueAt -Repository $repoRoot -Arguments @(
    "rev-parse", "--verify", "HEAD^{commit}"
)).ToLowerInvariant()
$headTree = (Get-GitValueAt -Repository $repoRoot -Arguments @(
    "rev-parse", "--verify", "HEAD^{tree}"
)).ToLowerInvariant()
$localMainCommit = (Get-GitValueAt -Repository $repoRoot -Arguments @(
    "rev-parse", "--verify", "refs/heads/main^{commit}"
)).ToLowerInvariant()
$originMainCommit = (Get-GitValueAt -Repository $repoRoot -Arguments @(
    "rev-parse", "--verify", "refs/remotes/origin/main^{commit}"
)).ToLowerInvariant()
$mirrorMainCommit = (Get-GitValueAt -Repository $mirrorRoot -Arguments @(
    "rev-parse", "--verify", "refs/heads/main^{commit}"
)).ToLowerInvariant()
if ($headCommit -cnotmatch '^[0-9a-f]{40}$' -or $headTree -cnotmatch '^[0-9a-f]{40}$') {
    throw "Candidate commit or tree identity is malformed."
}
if (
    $headCommit -cne $localMainCommit -or
    $headCommit -cne $originMainCommit -or
    $headCommit -cne $mirrorMainCommit
) {
    throw "HEAD, local main, origin/main, and local bare mirror main must be the exact candidate commit."
}
$tagRef = "refs/tags/$Tag"
$finalTagObject = (Get-GitValueAt -Repository $repoRoot -Arguments @(
    "rev-parse", "--verify", $tagRef
)).ToLowerInvariant()
$mirrorTagObject = (Get-GitValueAt -Repository $mirrorRoot -Arguments @(
    "rev-parse", "--verify", $tagRef
)).ToLowerInvariant()
if (
    $finalTagObject -cnotmatch '^[0-9a-f]{40}$' -or
    $mirrorTagObject -cnotmatch '^[0-9a-f]{40}$' -or
    $finalTagObject -cne $mirrorTagObject
) {
    throw "Prepared clone and local bare mirror must contain the exact same FINAL intended tag object."
}
$tagObjectType = Get-GitValueAt -Repository $repoRoot -Arguments @(
    "cat-file", "-t", $tagRef
)
$mirrorTagObjectType = Get-GitValueAt -Repository $mirrorRoot -Arguments @(
    "cat-file", "-t", $tagRef
)
if ($tagObjectType -cne "tag" -or $mirrorTagObjectType -cne "tag") {
    throw "The FINAL intended release ref must be an annotated tag object in clone and local bare mirror."
}
$tagCommit = (Get-GitValueAt -Repository $repoRoot -Arguments @(
    "rev-parse", "--verify", "$tagRef^{commit}"
)).ToLowerInvariant()
$mirrorTagCommit = (Get-GitValueAt -Repository $mirrorRoot -Arguments @(
    "rev-parse", "--verify", "$tagRef^{commit}"
)).ToLowerInvariant()
if ($tagCommit -cne $headCommit -or $mirrorTagCommit -cne $headCommit) {
    throw "The FINAL intended tag must peel to exact HEAD and local bare mirror main."
}

# The packaged staged-installer gate executes the committed installer, whose
# contract requires an elevated Windows session. Check this after authenticating
# the isolated source topology, but before creating output or doing build work.
$windowsIdentity = [Security.Principal.WindowsIdentity]::GetCurrent()
$windowsPrincipal = [Security.Principal.WindowsPrincipal]::new($windowsIdentity)
if (-not $windowsPrincipal.IsInRole(
    [Security.Principal.WindowsBuiltInRole]::Administrator
)) {
    throw "Frozen release candidate qualification requires an elevated Windows administrator session."
}

$pythonFacts = & $resolvedPython -I -c `
    "import json,platform,sys; print(json.dumps({'version': platform.python_version(), 'system': platform.system(), 'machine': platform.machine(), 'bits': platform.architecture()[0]}))" |
    ConvertFrom-Json
Assert-LastExitCode "Python runtime inspection"
if (
    $pythonFacts.version -cne $ExpectedPythonVersion -or
    $pythonFacts.system -cne "Windows" -or
    $pythonFacts.bits -cne "64bit" -or
    @("AMD64", "x86_64") -cnotcontains $pythonFacts.machine
) {
    throw "Release Python must be exact Windows x64 CPython $ExpectedPythonVersion."
}
$sourceEpochText = Get-GitValueAt -Repository $repoRoot -Arguments @(
    "show", "-s", "--format=%ct", "HEAD"
)
if ($sourceEpochText -cnotmatch '^[1-9][0-9]*$') {
    throw "Commit source epoch is invalid."
}
$sourceEpoch = [Int64]::Parse($sourceEpochText, [Globalization.CultureInfo]::InvariantCulture)
$builtAtUtc = [DateTimeOffset]::FromUnixTimeSeconds($sourceEpoch).UtcDateTime.ToString(
    "yyyy-MM-ddTHH:mm:ssZ",
    [Globalization.CultureInfo]::InvariantCulture
)

[IO.Directory]::CreateDirectory($resolvedOutputRoot) | Out-Null
$tagIdentityPath = Join-Path $resolvedOutputRoot "Label_Match-$Tag.final-tag-identity.json"
& $resolvedPython -I -S (Join-Path $repoRoot "tools\verify_release_tag_attestation.py") `
    --repo-root $repoRoot `
    --expected-tag $Tag `
    --expected-commit $headCommit `
    --report $tagIdentityPath
Assert-LastExitCode "Record the final canonical annotated tag before release identity and build"
$tagIdentity = Get-Content -Raw -Encoding UTF8 -LiteralPath $tagIdentityPath | ConvertFrom-Json
if (
    $tagIdentity.schema_version -cne "label-match-canonical-annotated-tag-v1" -or
    $tagIdentity.status -cne "PASS" -or
    $tagIdentity.tag -cne $Tag -or
    $tagIdentity.tag_object -cne $finalTagObject -or
    $tagIdentity.tag_object_type -cne "tag" -or
    $tagIdentity.annotated_tag -ne $true -or
    $tagIdentity.commit -cne $headCommit -or
    $tagIdentity.peeled_commit -cne $headCommit -or
    $tagIdentity.message -cne "Release $Tag"
) {
    throw "Recorded final tag identity differs from the isolated mirror candidate."
}
$tagIdentitySha256 = (Get-FileHash -LiteralPath $tagIdentityPath -Algorithm SHA256).Hash.ToLowerInvariant()
$venvRoot = Join-Path $resolvedOutputRoot "venv"
$workRoot = Join-Path $resolvedOutputRoot "work"
$distRoot = Join-Path $resolvedOutputRoot "dist"
[IO.Directory]::CreateDirectory($workRoot) | Out-Null
[IO.Directory]::CreateDirectory($distRoot) | Out-Null
$taskTempRoot = Join-Path $workRoot "temp"
$pyInstallerConfigRoot = Join-Path $workRoot "pyinstaller-config"
[IO.Directory]::CreateDirectory($taskTempRoot) | Out-Null
[IO.Directory]::CreateDirectory($pyInstallerConfigRoot) | Out-Null
$env:TEMP = $taskTempRoot
$env:TMP = $taskTempRoot
$env:PYINSTALLER_CONFIG_DIR = $pyInstallerConfigRoot

& $resolvedPython -I -m venv $venvRoot
Assert-LastExitCode "Create release virtual environment"
$venvPython = Join-Path $venvRoot "Scripts\python.exe"
if (-not (Test-Path -LiteralPath $venvPython -PathType Leaf)) {
    throw "Release virtual environment did not create python.exe."
}
$env:PIP_NO_INDEX = "1"
$env:PIP_DISABLE_PIP_VERSION_CHECK = "1"
$env:PYTHONNOUSERSITE = "1"
$env:PYTHONDONTWRITEBYTECODE = "1"
$env:PYTEST_DISABLE_PLUGIN_AUTOLOAD = "1"
& $venvPython -I -m pip install `
    --no-index `
    --no-cache-dir `
    --find-links $resolvedWheelhouse `
    --only-binary=:all: `
    --require-hashes `
    --no-deps `
    -r (Join-Path $repoRoot "requirements-release.txt")
Assert-LastExitCode "Install hash-locked release dependencies from the offline wheelhouse"
& $venvPython -I -m pip check
Assert-LastExitCode "Check release dependencies"
$toolchain = & $venvPython -I -c `
    "import importlib.metadata,platform; print(platform.python_version() + '|' + importlib.metadata.version('pyinstaller'))"
Assert-LastExitCode "Inspect release toolchain"
if ($toolchain.Trim() -cne "$ExpectedPythonVersion|$ExpectedPyInstallerVersion") {
    throw "Hash-locked venv toolchain mismatch: $toolchain"
}

$releaseIdentityPath = Join-Path $workRoot "release-identity.json"
& $venvPython -I -S (Join-Path $repoRoot "tools\verify_release_identity.py") `
    --repo-root $repoRoot `
    --expected-tag $Tag `
    --expected-sha $headCommit `
    --reviewed-ref "refs/remotes/origin/main" `
    --report $releaseIdentityPath
Assert-LastExitCode "Verify unsigned release identity"

$factoryIdentityRoot = Join-Path $workRoot "factory_contract_identity"
& $venvPython -m kmtech_factory_contracts.build_cli prepare `
    --repository $repoRoot `
    --stage-root $factoryIdentityRoot `
    --app-id label_match `
    --app-version $Tag `
    --db-schema-current 0
Assert-LastExitCode "Prepare exact factory compatibility identity"

$mainWorkRoot = Join-Path $workRoot "label_match_pyinstaller"
$mainArguments = @(
    "--name", "Label_Match",
    "--onedir",
    "--windowed",
    "--icon", (Join-Path $repoRoot "assets\logo.ico"),
    "--add-data", "$(Join-Path $repoRoot 'assets');assets",
    "--add-data", "$(Join-Path $repoRoot 'config');config",
    "--add-data", "$(Join-Path $repoRoot 'kmtech_factory_contracts\bundle');kmtech_factory_contracts/bundle",
    "--add-data", "$(Join-Path $factoryIdentityRoot 'build-identity.json');.",
    "--add-data", "$(Join-Path $factoryIdentityRoot 'build-compatibility.json');.",
    "--add-data", "$(Join-Path $repoRoot 'contract.lock.json');.",
    "--hidden-import", "pygame",
    "--hidden-import", "PIL",
    "--hidden-import", "tkcalendar",
    "--distpath", $distRoot,
    "--workpath", $mainWorkRoot,
    "--specpath", $mainWorkRoot,
    "--clean",
    "--noupx",
    "--noconfirm",
    (Join-Path $repoRoot "Label_Match.py")
)
& $venvPython -I -m PyInstaller @mainArguments
Assert-LastExitCode "Build Label_Match onedir package"
$packageRoot = Join-Path $distRoot "Label_Match"
if (-not (Test-Path -LiteralPath $packageRoot -PathType Container)) {
    throw "PyInstaller package root is missing: $packageRoot"
}

& $venvPython -I (Join-Path $repoRoot "tools\build_release_cli_tools.py") `
    --destination (Join-Path $packageRoot "tools") `
    --help-timeout-seconds 15 `
    --probe-count 3
Assert-LastExitCode "Build exact unsigned release CLI tools"

Invoke-OneFileBuild `
    -VenvPython $venvPython `
    -RepositoryRoot $repoRoot `
    -PackageRoot $packageRoot `
    -WorkRoot $workRoot `
    -Name "KMTech_Logistics_Profile_Install" `
    -Source (Join-Path $repoRoot "tools\install_logistics_runtime_profile.py")
Invoke-OneFileBuild `
    -VenvPython $venvPython `
    -RepositoryRoot $repoRoot `
    -PackageRoot $packageRoot `
    -WorkRoot $workRoot `
    -Name "KMTech_Logistics_Profile_Check" `
    -Source (Join-Path $repoRoot "tools\check_logistics_runtime_profile.py")
Invoke-OneFileBuild `
    -VenvPython $venvPython `
    -RepositoryRoot $repoRoot `
    -PackageRoot $packageRoot `
    -WorkRoot $workRoot `
    -Name "Label_Match_Protected_Admin_Install" `
    -Source (Join-Path $repoRoot "tools\install_protected_admin.py")

$probeWorkRoot = Join-Path $workRoot "active_work_probe_pyinstaller"
$contractBundlePath = Join-Path $repoRoot "kmtech_factory_contracts\bundle"
& $venvPython -I -m PyInstaller `
    --paths $repoRoot `
    --name $ProbeName `
    --onefile `
    --console `
    --distpath $packageRoot `
    --workpath $probeWorkRoot `
    --specpath $probeWorkRoot `
    --add-data "$contractBundlePath;kmtech_factory_contracts/bundle" `
    --collect-submodules kmtech_factory_contracts.active_work_probe `
    --clean `
    --noupx `
    --noconfirm `
    (Join-Path $repoRoot "tools\active_work_probe.py")
Assert-LastExitCode "Build active-work probe"

$probeArtifactPath = Join-Path $packageRoot "$ProbeName.exe"
$independentIdentityPath = Join-Path $packageRoot "$ProbeName.independent.build-identity.json"
$integratedIdentityPath = Join-Path $packageRoot "$ProbeName.integrated.build-identity.json"
$probeArtifactSha256 = (Get-FileHash -LiteralPath $probeArtifactPath -Algorithm SHA256).Hash.ToLowerInvariant()
& $venvPython -m kmtech_factory_contracts.active_work_probe `
    -Mode build-identity `
    -OutputPath $independentIdentityPath `
    -ProbeArtifactPath $probeArtifactPath `
    -ProbeSourceCommit $headCommit `
    -WorkflowMode independent `
    -SupportedApps "Label_Match" `
    -ProbeName $ProbeName `
    -ProbeVersion $ProbeVersion
Assert-LastExitCode "Generate authoritative independent probe identity"
& $venvPython -m kmtech_factory_contracts.active_work_probe `
    -Mode build-identity `
    -OutputPath $integratedIdentityPath `
    -ProbeArtifactPath $probeArtifactPath `
    -ProbeSourceCommit $headCommit `
    -WorkflowMode integrated `
    -SupportedApps $AllProbeApps `
    -ProbeName $ProbeName `
    -ProbeVersion $ProbeVersion
Assert-LastExitCode "Generate authoritative integrated probe identity"
$independentApps = @("Label_Match")
$integratedApps = @($AllProbeApps -split ",")
Assert-ProbeIdentity $independentIdentityPath independent $independentApps $headCommit $probeArtifactSha256
Assert-ProbeIdentity $integratedIdentityPath integrated $integratedApps $headCommit $probeArtifactSha256

& $probeArtifactPath --help | Out-Null
Assert-LastExitCode "Probe executable help smoke"
$probeComparisonRoot = Join-Path $workRoot "active_work_probe_identity_comparison"
[IO.Directory]::CreateDirectory($probeComparisonRoot) | Out-Null
$packagedIndependentPath = Join-Path $probeComparisonRoot "$ProbeName.independent.build-identity.json"
$packagedIntegratedPath = Join-Path $probeComparisonRoot "$ProbeName.integrated.build-identity.json"
& $probeArtifactPath `
    -Mode build-identity `
    -OutputPath $packagedIndependentPath `
    -ProbeArtifactPath $probeArtifactPath `
    -ProbeSourceCommit $headCommit `
    -WorkflowMode independent `
    -SupportedApps "Label_Match" `
    -ProbeName $ProbeName `
    -ProbeVersion $ProbeVersion
Assert-LastExitCode "Packaged independent probe identity generation"
& $probeArtifactPath `
    -Mode build-identity `
    -OutputPath $packagedIntegratedPath `
    -ProbeArtifactPath $probeArtifactPath `
    -ProbeSourceCommit $headCommit `
    -WorkflowMode integrated `
    -SupportedApps $AllProbeApps `
    -ProbeName $ProbeName `
    -ProbeVersion $ProbeVersion
Assert-LastExitCode "Packaged integrated probe identity generation"
foreach ($pair in @(
    @($independentIdentityPath, $packagedIndependentPath),
    @($integratedIdentityPath, $packagedIntegratedPath)
)) {
    $sourceBytes = [IO.File]::ReadAllBytes($pair[0])
    $packagedBytes = [IO.File]::ReadAllBytes($pair[1])
    if (-not [Linq.Enumerable]::SequenceEqual($sourceBytes, $packagedBytes)) {
        throw "Source and packaged active-work probe identities differ: $($pair[0])"
    }
}

$copies = [ordered]@{
    (Join-Path $repoRoot "install_label_match_direct_sync.ps1") = (Join-Path $packageRoot "install_label_match_direct_sync.ps1")
    (Join-Path $repoRoot "INSTALL_THIS_PC.ps1") = (Join-Path $packageRoot "INSTALL_THIS_PC.ps1")
    $releaseIdentityPath = (Join-Path $packageRoot "release-identity.json")
    (Join-Path $factoryIdentityRoot "build-identity.json") = (Join-Path $packageRoot "build-identity.json")
    (Join-Path $factoryIdentityRoot "build-compatibility.json") = (Join-Path $packageRoot "build-compatibility.json")
    (Join-Path $factoryIdentityRoot "contract.lock.json") = (Join-Path $packageRoot "contract.lock.json")
    (Join-Path $repoRoot "direct_sync_push.py") = (Join-Path $packageRoot "direct_sync_push.py")
    (Join-Path $repoRoot "direct_sync_runtime.py") = (Join-Path $packageRoot "direct_sync_runtime.py")
    (Join-Path $repoRoot "producer_runtime_client.py") = (Join-Path $packageRoot "producer_runtime_client.py")
    (Join-Path $repoRoot "direct_sync_operator.py") = (Join-Path $packageRoot "direct_sync_operator.py")
    (Join-Path $repoRoot "logistics_runtime_profile.py") = (Join-Path $packageRoot "logistics_runtime_profile.py")
    (Join-Path $repoRoot "docs\LOGISTICS_RUNTIME_PROFILE.md") = (Join-Path $packageRoot "CENTRAL_LOGISTICS_PC_ROLLOUT.md")
    (Join-Path $repoRoot "tools\provision_protected_admin_acl.ps1") = (Join-Path $packageRoot "PROVISION_PROTECTED_ADMIN_ACL.ps1")
    (Join-Path $repoRoot "docs\PROTECTED_ADMIN_PROVISIONING.md") = (Join-Path $packageRoot "PROTECTED_ADMIN_PROVISIONING.md")
}
foreach ($entry in $copies.GetEnumerator()) {
    Copy-RequiredFile -Source $entry.Key -Destination $entry.Value
}
foreach ($toolName in @(
    "direct_sync_relay_runner.py",
    "direct_sync_relay_operator.py",
    "direct_sync_relay_install_pack.py",
    "direct_sync_phase_g_label_match_runtime_report.py",
    "register_label_match_worker_pc.py",
    "install_logistics_runtime_profile.py",
    "check_logistics_runtime_profile.py"
)) {
    Copy-RequiredFile `
        -Source (Join-Path $repoRoot "tools\$toolName") `
        -Destination (Join-Path $packageRoot "tools\$toolName")
}
Write-NewUtf8File `
    -Path (Join-Path $packageRoot "tools\enrollment_token.txt.template") `
    -Text "Normal installs use PRODUCER_SELF_ENROLL_ALLOWED_IPS on the server. Do not bundle a deployment token unless an explicit fallback change window requires it.`n"

& (Join-Path $packageRoot "Label_Match_Protected_Admin_Install.exe") --help | Out-Null
Assert-LastExitCode "Protected administrator installer help probe"
& (Join-Path $packageRoot "Label_Match_Protected_Admin_Install.exe") --dry-run | Out-Null
Assert-LastExitCode "Protected administrator installer dry run"
& powershell.exe `
    -NoLogo `
    -NoProfile `
    -NonInteractive `
    -ExecutionPolicy Bypass `
    -File (Join-Path $packageRoot "PROVISION_PROTECTED_ADMIN_ACL.ps1") `
    -DryRun | Out-Null
Assert-LastExitCode "Protected administrator ACL wrapper dry run"

$settingsPaths = @(
    (Join-Path $packageRoot "_internal\config\app_settings.json"),
    (Join-Path $packageRoot "config\app_settings.json")
)
$sourceSettingsPath = $settingsPaths | Where-Object {
    Test-Path -LiteralPath $_ -PathType Leaf
} | Select-Object -First 1
if ([string]::IsNullOrWhiteSpace($sourceSettingsPath)) {
    throw "Bundled app settings are missing."
}
$settings = Get-Content -Raw -Encoding UTF8 -LiteralPath $sourceSettingsPath | ConvertFrom-Json
$fixedUpdateSettings = [ordered]@{
    provider = "github"
    channel = "stable"
}
$settings | Add-Member `
    -NotePropertyName update_settings `
    -NotePropertyValue ([pscustomobject]$fixedUpdateSettings) `
    -Force
$settingsJson = ($settings | ConvertTo-Json -Depth 100) + "`n"
foreach ($settingsPath in $settingsPaths) {
    [IO.Directory]::CreateDirectory((Split-Path -Parent $settingsPath)) | Out-Null
    [IO.File]::WriteAllText($settingsPath, $settingsJson, [Text.UTF8Encoding]::new($false))
    $written = Get-Content -Raw -Encoding UTF8 -LiteralPath $settingsPath | ConvertFrom-Json
    $updateFields = @($written.update_settings.PSObject.Properties.Name)
    if (
        $updateFields.Count -ne 2 -or
        ($updateFields -join ",") -cne "provider,channel" -or
        $written.update_settings.provider -cne "github" -or
        $written.update_settings.channel -cne "stable"
    ) {
        throw "Packaged update settings are not exactly github/stable: $settingsPath"
    }
}

$stagedInstallerReport = Join-Path $packageRoot "staged-installer-verification.json"
& $venvPython -I (Join-Path $repoRoot "tools\verify_staged_release_installer.py") `
    --package-root $packageRoot `
    --report $stagedInstallerReport
Assert-LastExitCode "Verify staged installer without system Python"
& $venvPython (Join-Path $packageRoot "tools\direct_sync_relay_runner.py") --help | Out-Null
Assert-LastExitCode "Staged relay source help probe"
& $venvPython (Join-Path $packageRoot "tools\direct_sync_relay_operator.py") --help | Out-Null
Assert-LastExitCode "Staged relay operator source help probe"
$env:LABEL_MATCH_STAGED_PACKAGE_ROOT = $packageRoot
$env:LABEL_MATCH_REQUIRE_STAGED_INSTALLER_TEST = "1"
& $venvPython -m pytest `
    -q `
    -p no:cacheprovider `
    (Join-Path $repoRoot "tests\test_staged_release_installer.py")
Assert-LastExitCode "Run staged installer release gate"

& $venvPython -m kmtech_factory_contracts.build_cli manifest `
    --stage-root $packageRoot `
    --expected-file Label_Match.exe `
    --expected-file KMTechActiveWorkProbe.exe `
    --expected-file KMTechActiveWorkProbe.independent.build-identity.json `
    --expected-file KMTechActiveWorkProbe.integrated.build-identity.json `
    --expected-file contract.lock.json `
    --expected-file build-identity.json `
    --expected-file build-compatibility.json `
    --built-at-utc $builtAtUtc
Assert-LastExitCode "Seal exact factory package manifest"
& $venvPython -m kmtech_factory_contracts.build_cli verify `
    --stage-root $packageRoot `
    --expected-contract-sha256 $FactoryContractSha256
Assert-LastExitCode "Verify exact current factory package"

$archiveName = "Label_Match-$Tag.zip"
$archivePath = Join-Path $resolvedOutputRoot $archiveName
$archiveReportPath = Join-Path $resolvedOutputRoot "Label_Match-$Tag.archive-verification.json"
& $venvPython -I -S (Join-Path $repoRoot "tools\build_release_archive.py") `
    --package-root $packageRoot `
    --archive $archivePath `
    --source-epoch $sourceEpoch `
    --top-level "Label_Match" `
    --expected-tag $Tag `
    --report $archiveReportPath
Assert-LastExitCode "Build deterministic unsigned frozen candidate archive"
$archiveReport = Get-Content -Raw -Encoding UTF8 -LiteralPath $archiveReportPath | ConvertFrom-Json
if (
    $archiveReport.status -cne "PASS" -or
    $archiveReport.release_trust -cne "internal_unsigned" -or
    $archiveReport.tag_signature_verified -ne $false -or
    $archiveReport.authenticode_required -ne $false -or
    $archiveReport.tag -cne $Tag -or
    $archiveReport.commit -cne $headCommit -or
    $archiveReport.tree -cne $headTree
) {
    throw "Archive verification report is not the exact unsigned candidate identity."
}

$checksumPath = Join-Path $resolvedOutputRoot "$archiveName.sha256"
$checksumText = "$($archiveReport.archive_sha256)  $archiveName`n"
Write-NewUtf8File -Path $checksumPath -Text $checksumText
$liveArchiveSha256 = (Get-FileHash -LiteralPath $archivePath -Algorithm SHA256).Hash.ToLowerInvariant()
$liveArchiveSize = (Get-Item -LiteralPath $archivePath).Length
$liveChecksumText = [IO.File]::ReadAllText($checksumPath, [Text.UTF8Encoding]::new($false))
if (
    $liveArchiveSha256 -cne $archiveReport.archive_sha256 -or
    $liveArchiveSize -ne $archiveReport.archive_size -or
    $liveChecksumText -cne $checksumText
) {
    throw "Frozen ZIP/checksum bytes changed after archive qualification."
}
$postBuildHead = (Get-GitValueAt -Repository $repoRoot -Arguments @(
    "rev-parse", "--verify", "HEAD^{commit}"
)).ToLowerInvariant()
$postBuildTree = (Get-GitValueAt -Repository $repoRoot -Arguments @(
    "rev-parse", "--verify", "HEAD^{tree}"
)).ToLowerInvariant()
$postBuildHeadRef = Get-GitValueAt -Repository $repoRoot -Arguments @(
    "symbolic-ref", "--quiet", "HEAD"
)
$postBuildLocalMain = (Get-GitValueAt -Repository $repoRoot -Arguments @(
    "rev-parse", "--verify", "refs/heads/main^{commit}"
)).ToLowerInvariant()
$postBuildOriginMain = (Get-GitValueAt -Repository $repoRoot -Arguments @(
    "rev-parse", "--verify", "refs/remotes/origin/main^{commit}"
)).ToLowerInvariant()
$postBuildMirrorMain = (Get-GitValueAt -Repository $mirrorRoot -Arguments @(
    "rev-parse", "--verify", "refs/heads/main^{commit}"
)).ToLowerInvariant()
$postBuildOriginPath = Get-NormalizedLocalOriginPath -RemoteUrl (
    Get-GitValueAt -Repository $repoRoot -Arguments @("remote", "get-url", "origin")
)
$postBuildTagObject = (Get-GitValueAt -Repository $repoRoot -Arguments @(
    "rev-parse", "--verify", $tagRef
)).ToLowerInvariant()
$postBuildTagType = Get-GitValueAt -Repository $repoRoot -Arguments @(
    "cat-file", "-t", $tagRef
)
$postBuildTagCommit = (Get-GitValueAt -Repository $repoRoot -Arguments @(
    "rev-parse", "--verify", "$tagRef^{commit}"
)).ToLowerInvariant()
$postBuildMirrorTagObject = (Get-GitValueAt -Repository $mirrorRoot -Arguments @(
    "rev-parse", "--verify", $tagRef
)).ToLowerInvariant()
$postBuildMirrorTagType = Get-GitValueAt -Repository $mirrorRoot -Arguments @(
    "cat-file", "-t", $tagRef
)
$postBuildMirrorTagCommit = (Get-GitValueAt -Repository $mirrorRoot -Arguments @(
    "rev-parse", "--verify", "$tagRef^{commit}"
)).ToLowerInvariant()
$postBuildStatus = Get-GitValueAt -Repository $repoRoot -Arguments @(
    "status", "--porcelain=v1", "--untracked-files=all"
)
if (
    $postBuildHead -cne $headCommit -or
    $postBuildTree -cne $headTree -or
    $postBuildHeadRef -cne "refs/heads/main" -or
    $postBuildLocalMain -cne $headCommit -or
    $postBuildOriginMain -cne $headCommit -or
    $postBuildMirrorMain -cne $headCommit -or
    $postBuildOriginPath -cne $mirrorRoot -or
    $postBuildTagObject -cne $finalTagObject -or
    $postBuildTagType -cne "tag" -or
    $postBuildTagCommit -cne $headCommit -or
    $postBuildMirrorTagObject -cne $finalTagObject -or
    $postBuildMirrorTagType -cne "tag" -or
    $postBuildMirrorTagCommit -cne $headCommit -or
    -not [string]::IsNullOrEmpty($postBuildStatus)
) {
    throw "FINAL tag object/type/peel, HEAD/tree/main refs, local mirror topology, or clean state changed during qualification."
}

$releaseNotesPath = Join-Path $resolvedOutputRoot "Label_Match-$Tag.release-notes.txt"
$releaseNotes = @(
    "Internal prerelease; not production-ready."
    "Tag: $Tag"
    "Commit: $headCommit"
    "Tree: $headTree"
    "Artifact: $archiveName"
    "Artifact-SHA256: $($archiveReport.archive_sha256)"
    "Artifact-Size: $($archiveReport.archive_size)"
    "Main-EXE-SHA256: $($archiveReport.main_exe_sha256)"
    "Factory-Contract-SHA256: $FactoryContractSha256"
    "Status: QUARANTINED_PENDING_FACTORY_QUALIFICATION"
) -join "`n"
Write-NewUtf8File -Path $releaseNotesPath -Text ($releaseNotes + "`n")

$qualification = [ordered]@{
    schema_version = "label-match-pre-push-qualification-v2"
    status = "PASS"
    phase = "phase_b_pre_push_frozen_candidate"
    release_title = "Release $Tag"
    release_trust = "internal_unsigned"
    tag = $Tag
    tag_object = $finalTagObject
    tag_object_type = "tag"
    tag_peeled_commit = $headCommit
    canonical_tag_message = "Release $Tag"
    tag_identity_report = [IO.Path]::GetFileName($tagIdentityPath)
    tag_identity_report_sha256 = $tagIdentitySha256
    tag_recorded_before_release_identity_and_build = $true
    commit = $headCommit
    tree = $headTree
    tag_signature_verified = $false
    python_version = $ExpectedPythonVersion
    pyinstaller_version = $ExpectedPyInstallerVersion
    source_epoch = $sourceEpoch
    archive = $archiveName
    archive_sha256 = $archiveReport.archive_sha256
    archive_size = $archiveReport.archive_size
    checksum = [IO.Path]::GetFileName($checksumPath)
    checksum_sha256 = (Get-FileHash -LiteralPath $checksumPath -Algorithm SHA256).Hash.ToLowerInvariant()
    main_exe_sha256 = $archiveReport.main_exe_sha256
    factory_contract_sha256 = $FactoryContractSha256
    update_provider = "github"
    update_channel = "stable"
    frozen_bytes = $true
    network_used = $false
    publication_mutated = $false
    tag_mutated = $false
    external_post_download_parity_required = $true
}
$qualificationPath = Join-Path $resolvedOutputRoot "Label_Match-$Tag.phase1-qualification.json"
Write-NewUtf8File `
    -Path $qualificationPath `
    -Text (($qualification | ConvertTo-Json -Depth 8) + "`n")

Write-Output "frozen_candidate=PASS tag=$Tag commit=$headCommit"
Write-Output "archive=$archivePath"
Write-Output "checksum=$checksumPath"
Write-Output "archive_report=$archiveReportPath"
Write-Output "phase1_qualification=$qualificationPath"
Write-Output "final_tag_identity=$tagIdentityPath"
Write-Output "release_title=Release $Tag"
Write-Output "release_notes=$releaseNotesPath"
Write-Output "NEXT REQUIRED (outside this script): preserve this receipt and frozen pair; push main, record Hosted CI factually (WAIVED_NOT_TESTED when not proven), recheck the unchanged local tag object and bytes, then push that same tag object and publish the immutable prerelease."
Write-Output "POST-DOWNLOAD REQUIRED: compare the externally downloaded ZIP/checksum and canonical remote tag object to the preserved phase1 qualification receipt; do not rebuild or recreate the tag."
}
finally {
    Set-Location -LiteralPath $originalLocation
}
