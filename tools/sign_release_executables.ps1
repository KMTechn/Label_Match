[CmdletBinding()]
param(
    [Parameter(Mandatory = $true)]
    [string]$PackageRoot,
    [Parameter(Mandatory = $true)]
    [string]$TimestampUrl,
    [Parameter(Mandatory = $true)]
    [string]$ExpectedThumbprint,
    [string]$ReportPath = ""
)

$ErrorActionPreference = "Stop"
$package = [System.IO.Path]::GetFullPath($PackageRoot)
if (-not (Test-Path -LiteralPath $package -PathType Container)) {
    throw "Package root does not exist: $package"
}

$timestampUri = [System.Uri]$TimestampUrl
if ($timestampUri.Scheme -ne "https") {
    throw "TimestampUrl must use HTTPS."
}
if (-not [string]::IsNullOrWhiteSpace($timestampUri.UserInfo) -or
    -not [string]::IsNullOrWhiteSpace($timestampUri.Query) -or
    -not [string]::IsNullOrWhiteSpace($timestampUri.Fragment)) {
    throw "TimestampUrl must not contain userinfo, query, or fragment."
}
$expectedSignerThumbprint = ($ExpectedThumbprint -replace "\s", "").ToUpperInvariant()
if ($expectedSignerThumbprint -notmatch "^[0-9A-F]{40}$") {
    throw "ExpectedThumbprint must be a 40-character SHA-1 certificate thumbprint."
}

$targets = @(
    "Label_Match.exe",
    "KMTech_Logistics_Profile_Install.exe",
    "KMTech_Logistics_Profile_Check.exe",
    "tools\direct_sync_relay_runner.exe",
    "tools\direct_sync_relay_install_pack\direct_sync_relay_install_pack.exe",
    "tools\register_label_match_worker_pc.exe"
)
foreach ($relativePath in $targets) {
    $target = Join-Path $package $relativePath
    if (-not (Test-Path -LiteralPath $target -PathType Leaf)) {
        throw "Required executable is missing: $relativePath"
    }
}

$sdkRoot = Join-Path ${env:ProgramFiles(x86)} "Windows Kits\10\bin"
$signtool = Get-ChildItem -LiteralPath $sdkRoot -Filter signtool.exe -Recurse -File |
    Where-Object { $_.FullName -match "\\x64\\signtool\.exe$" } |
    Sort-Object FullName -Descending |
    Select-Object -First 1
if (-not $signtool) {
    throw "x64 signtool.exe was not found under $sdkRoot"
}

$signingCertificates = @(
    Get-ChildItem Cert:\CurrentUser\My | Where-Object {
        $_.Thumbprint -eq $expectedSignerThumbprint -and
        $_.HasPrivateKey -and
        @($_.EnhancedKeyUsageList | Where-Object { $_.ObjectId.Value -eq "1.3.6.1.5.5.7.3.3" }).Count -eq 1
    }
)
if ($signingCertificates.Count -ne 1) {
    throw "The protected signing store must contain exactly one matching private-key Code Signing certificate."
}
$certificate = $signingCertificates[0]
$now = Get-Date
if ($now -lt $certificate.NotBefore -or $now -gt $certificate.NotAfter) {
    throw "Code-signing certificate is outside its validity window."
}

$entries = @()
foreach ($relativePath in $targets) {
    $target = Join-Path $package $relativePath
    & $signtool.FullName sign /sha1 $certificate.Thumbprint /s My /fd SHA256 /td SHA256 /tr $TimestampUrl $target
    if ($LASTEXITCODE -ne 0) {
        throw "signtool failed for $relativePath with exit code $LASTEXITCODE"
    }
    $signature = Get-AuthenticodeSignature -LiteralPath $target
    if ($signature.Status -ne [System.Management.Automation.SignatureStatus]::Valid) {
        throw "Authenticode status is not Valid for ${relativePath}: $($signature.Status)"
    }
    if (-not $signature.SignerCertificate -or $signature.SignerCertificate.Thumbprint -ne $certificate.Thumbprint) {
        throw "Signer thumbprint mismatch for $relativePath"
    }
    if (-not $signature.TimeStamperCertificate) {
        throw "Trusted timestamp is missing for $relativePath"
    }
    $entries += [ordered]@{
        path = $relativePath.Replace("\", "/")
        size = (Get-Item -LiteralPath $target).Length
        sha256 = (Get-FileHash -Algorithm SHA256 -LiteralPath $target).Hash.ToLowerInvariant()
        status = $signature.Status.ToString()
        signer_thumbprint = $signature.SignerCertificate.Thumbprint
        timestamp_thumbprint = $signature.TimeStamperCertificate.Thumbprint
    }
}

$report = [ordered]@{
    schema_version = "label-match-authenticode-manifest-v1"
    status = "PASS"
    signing_key_source = "protected_current_user_certificate_store"
    timestamp_url = $TimestampUrl
    signer_thumbprint = $certificate.Thumbprint
    signer_subject = $certificate.Subject
    signer_not_before = $certificate.NotBefore.ToUniversalTime().ToString("o")
    signer_not_after = $certificate.NotAfter.ToUniversalTime().ToString("o")
    executables = $entries
}
$reportTarget = if ([string]::IsNullOrWhiteSpace($ReportPath)) {
    Join-Path $package "authenticode-manifest.json"
}
else {
    [System.IO.Path]::GetFullPath($ReportPath)
}
$report | ConvertTo-Json -Depth 8 | Set-Content -Encoding utf8NoBOM -LiteralPath $reportTarget
