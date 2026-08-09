$ErrorActionPreference = "Stop"

function ConvertTo-ElevationArgument([string]$Value) {
    if ([string]::IsNullOrEmpty($Value)) { return '""' }
    if ($Value -notmatch '[\s"]') { return $Value }
    $builder = New-Object System.Text.StringBuilder
    [void]$builder.Append('"')
    $slashCount = 0
    foreach ($character in $Value.ToCharArray()) {
        if ($character -eq '\') { $slashCount += 1; continue }
        if ($character -eq '"') {
            [void]$builder.Append((('\' * (($slashCount * 2) + 1)) -join ''))
            [void]$builder.Append('"')
            $slashCount = 0
            continue
        }
        if ($slashCount -gt 0) {
            [void]$builder.Append((('\' * $slashCount) -join ''))
            $slashCount = 0
        }
        [void]$builder.Append($character)
    }
    if ($slashCount -gt 0) {
        [void]$builder.Append((('\' * ($slashCount * 2)) -join ''))
    }
    [void]$builder.Append('"')
    return $builder.ToString()
}

function Invoke-SelfElevated([string]$ScriptPath, [hashtable]$BoundParameters, [object[]]$RemainingArguments) {
    $identity = [Security.Principal.WindowsIdentity]::GetCurrent()
    $principal = New-Object Security.Principal.WindowsPrincipal($identity)
    if ($principal.IsInRole([Security.Principal.WindowsBuiltInRole]::Administrator)) { return }
    $powershellExe = Join-Path ([Environment]::SystemDirectory) 'WindowsPowerShell\v1.0\powershell.exe'
    $launchArguments = @('-NoProfile', '-ExecutionPolicy', 'Bypass', '-File', $ScriptPath)
    $launchArguments += @($RemainingArguments | ForEach-Object { [string]$_ })
    $argumentLine = ($launchArguments | ForEach-Object { ConvertTo-ElevationArgument $_ }) -join ' '
    $process = Start-Process -FilePath $powershellExe -Verb RunAs -ArgumentList $argumentLine -Wait -PassThru -ErrorAction Stop
    exit $process.ExitCode
}

Invoke-SelfElevated $MyInvocation.MyCommand.Path $PSBoundParameters $args

$installer = Join-Path (Split-Path -Parent $MyInvocation.MyCommand.Path) "install_label_match_direct_sync.ps1"
if (-not (Test-Path -LiteralPath $installer -PathType Leaf)) {
    throw "Release package is incomplete. Missing: $installer"
}

# Preserve the established installer's arguments. Its normal path performs
# tokenless self-enrollment, installs the relay as SYSTEM, and starts it once.
& $installer @args
exit $LASTEXITCODE
