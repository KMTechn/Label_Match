[CmdletBinding()]
param(
    [Parameter(Mandatory = $true)]
    [string]$PowerShellPath,

    [Parameter(Mandatory = $true)]
    [string]$OutputRoot,

    [ValidatePattern('^v(?:0|[1-9][0-9]*)\.(?:0|[1-9][0-9]*)\.(?:0|[1-9][0-9]*)$')]
    [string]$Tag = "v2.0.88",

    [Parameter(Mandatory = $true)]
    [string]$PythonPath,

    [Parameter(Mandatory = $true)]
    [string]$Wheelhouse,

    [Parameter(Mandatory = $true)]
    [string]$MirrorRoot,

    [Parameter(Mandatory = $true)]
    [string]$StdoutPath,

    [Parameter(Mandatory = $true)]
    [string]$StderrPath,

    [switch]$PreflightOnly
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

function Get-ExistingFilePath {
    param(
        [Parameter(Mandatory = $true)][string]$Path,
        [Parameter(Mandatory = $true)][string]$Label
    )
    $fullPath = [IO.Path]::GetFullPath($Path)
    if (-not (Test-Path -LiteralPath $fullPath -PathType Leaf)) {
        throw "$Label must be an existing file: $fullPath"
    }
    return $fullPath
}

function Get-ExistingDirectoryPath {
    param(
        [Parameter(Mandatory = $true)][string]$Path,
        [Parameter(Mandatory = $true)][string]$Label
    )
    $fullPath = [IO.Path]::GetFullPath($Path)
    if (-not (Test-Path -LiteralPath $fullPath -PathType Container)) {
        throw "$Label must be an existing directory: $fullPath"
    }
    return $fullPath.TrimEnd([char[]]"\/")
}

function Get-FreshPath {
    param(
        [Parameter(Mandatory = $true)][string]$Path,
        [Parameter(Mandatory = $true)][string]$Label
    )
    $fullPath = [IO.Path]::GetFullPath($Path)
    if (Test-Path -LiteralPath $fullPath) {
        throw "$Label must be absent before the one-shot launch: $fullPath"
    }
    $parent = [IO.Path]::GetDirectoryName($fullPath)
    if ([string]::IsNullOrWhiteSpace($parent)) {
        throw "$Label must have an existing parent directory: $fullPath"
    }
    if (-not (Test-Path -LiteralPath $parent -PathType Container)) {
        throw "$Label parent directory must exist before the one-shot launch: $parent"
    }
    return $fullPath
}

function Assert-NoAmbiguousTestPathBoolean {
    param([Parameter(Mandatory = $true)][string]$Path)

    $tokens = $null
    $parseErrors = $null
    $ast = [System.Management.Automation.Language.Parser]::ParseFile(
        $Path,
        [ref]$tokens,
        [ref]$parseErrors
    )
    if ($parseErrors.Count -ne 0) {
        $details = @($parseErrors | ForEach-Object { $_.ToString() }) -join "; "
        throw "PowerShell AST parse failed for release surface $Path`: $details"
    }

    $violations = New-Object 'System.Collections.Generic.List[string]'
    $commands = $ast.FindAll({
        param($node)
        $node -is [System.Management.Automation.Language.CommandAst]
    }, $true)
    foreach ($command in $commands) {
        if ($command.GetCommandName() -ine "Test-Path") {
            continue
        }

        $ambiguous = $false
        foreach ($element in $command.CommandElements) {
            if (
                $element -is [System.Management.Automation.Language.CommandParameterAst] -and
                $element.ParameterName -in @("and", "or")
            ) {
                $ambiguous = $true
                break
            }
        }

        $cursor = $command.Parent
        while ($null -ne $cursor -and $cursor -isnot [System.Management.Automation.Language.StatementBlockAst]) {
            if ($cursor -is [System.Management.Automation.Language.BinaryExpressionAst]) {
                if (
                    $cursor.Operator -eq [System.Management.Automation.Language.TokenKind]::And -or
                    $cursor.Operator -eq [System.Management.Automation.Language.TokenKind]::Or
                ) {
                    $ambiguous = $true
                    break
                }
            }
            $cursor = $cursor.Parent
        }

        if ($ambiguous) {
            $extent = ($command.Extent.Text -replace '\s+', ' ').Trim()
            $violations.Add("$Path`:$($command.Extent.StartLineNumber): $extent")
        }
    }

    if ($violations.Count -ne 0) {
        throw "Ambiguous Test-Path Boolean expression is forbidden: $($violations -join '; ')"
    }
}

function ConvertTo-NativeCommandLineArgument {
    param([Parameter(Mandatory = $true)][AllowEmptyString()][string]$Value)

    if ($Value.IndexOf([char]0) -ge 0) {
        throw "Native process argument contains a NUL character."
    }
    if ($Value.IndexOf('"') -ge 0) {
        throw "Native process argument contains an unsupported quote character."
    }
    if ($Value.Length -ne 0 -and $Value -cnotmatch '\s') {
        return $Value
    }

    $trailingBackslashCount = 0
    for ($index = $Value.Length - 1; $index -ge 0; $index--) {
        if ($Value[$index] -ne '\') {
            break
        }
        $trailingBackslashCount++
    }
    $escaped = $Value
    if ($trailingBackslashCount -gt 0) {
        $escaped += (('\' * $trailingBackslashCount) -join '')
    }
    return '"' + $escaped + '"'
}

$sourceRoot = Split-Path -Parent $PSScriptRoot
$builderPath = Join-Path $PSScriptRoot "build_frozen_release_candidate.ps1"
$releaseSurfacePaths = @(
    $PSCommandPath,
    $builderPath,
    (Join-Path $sourceRoot "INSTALL_THIS_PC.ps1")
)
foreach ($surfacePath in $releaseSurfacePaths) {
    $surfaceFullPath = Get-ExistingFilePath $surfacePath "release PowerShell surface"
    Assert-NoAmbiguousTestPathBoolean $surfaceFullPath
}

$resolvedPowerShellPath = Get-ExistingFilePath $PowerShellPath "PowerShell 7 executable"
$resolvedPythonPath = Get-ExistingFilePath $PythonPath "release Python executable"
$resolvedWheelhouse = Get-ExistingDirectoryPath $Wheelhouse "offline wheelhouse"
$resolvedMirrorRoot = Get-ExistingDirectoryPath $MirrorRoot "local bare mirror"
$resolvedOutputRoot = Get-FreshPath $OutputRoot "candidate output root"
$resolvedStdoutPath = Get-FreshPath $StdoutPath "builder stdout log"
$resolvedStderrPath = Get-FreshPath $StderrPath "builder stderr log"

if ([StringComparer]::OrdinalIgnoreCase.Equals($resolvedStdoutPath, $resolvedStderrPath)) {
    throw "Builder stdout and stderr logs must be different fresh paths."
}

if ($PreflightOnly.IsPresent) {
    [Console]::Out.Write("release_runner_prelaunch=PASS surfaces=$($releaseSurfacePaths.Count)")
    return
}

$builderArguments = @(
    "-NoLogo",
    "-NoProfile",
    "-NonInteractive",
    "-File",
    $builderPath,
    "-OutputRoot",
    $resolvedOutputRoot,
    "-Tag",
    $Tag,
    "-PythonPath",
    $resolvedPythonPath,
    "-Wheelhouse",
    $resolvedWheelhouse,
    "-MirrorRoot",
    $resolvedMirrorRoot
)
$argumentLine = (@($builderArguments | ForEach-Object {
    ConvertTo-NativeCommandLineArgument ([string]$_)
}) -join ' ')

$process = Start-Process `
    -FilePath $resolvedPowerShellPath `
    -ArgumentList $argumentLine `
    -RedirectStandardOutput $resolvedStdoutPath `
    -RedirectStandardError $resolvedStderrPath `
    -NoNewWindow `
    -Wait `
    -PassThru
exit ([int]$process.ExitCode)
