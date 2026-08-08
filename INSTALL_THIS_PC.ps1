#Requires -RunAsAdministrator

$ErrorActionPreference = "Stop"
$installer = Join-Path (Split-Path -Parent $MyInvocation.MyCommand.Path) "install_label_match_direct_sync.ps1"
if (-not (Test-Path -LiteralPath $installer -PathType Leaf)) {
    throw "Release package is incomplete. Missing: $installer"
}

# Preserve the established installer's arguments. Its normal path performs
# tokenless self-enrollment, installs the relay as SYSTEM, and starts it once.
& $installer @args
exit $LASTEXITCODE
