Set-StrictMode -Version Latest

$Script:LabelWriterFenceActiveSchema = 'label-match-all-writer-fence-active-v1'
$Script:LabelWriterFenceAppId = 'label_match'
$Script:LabelWriterFenceTupleVersion = 'label-match-deployment-session-authority-v1'
$Script:LabelWriterFenceSessionMutexPrefix = 'Local\KMTech.LabelMatch.DeploymentSession.'
$Script:LabelWriterFenceAdmissionMutexName = 'Local\KMTech.LabelMatch.WriterAdmission.v1'
$Script:LabelWriterFenceInventorySha256 = '00015dcba9c1cb6ca54ba9d051796dc91d425d09cc569d4f42ba0302b710461a'
$Script:LabelWriterFenceMaximumBytes = 262144
$Script:LabelWriterFenceActiveFields = @(
    'schema','status','app_id','session_id','attempt_id','replacement_transaction_id',
    'session_started_at_utc','orchestrator_sha256','writer_contract_sha256',
    'session_authority_mutex_name','writer_inventory_sha256','owner_kind',
    'delegation_sha256','delegated_sources','delegation_expires_at_utc',
    'activated_at_utc','secret_values_recorded'
)

function Get-LabelWriterFenceStringSha256([string]$Value) {
    $algorithm = [Security.Cryptography.SHA256]::Create()
    try {
        $bytes = (New-Object Text.UTF8Encoding($false)).GetBytes($Value)
        return ([BitConverter]::ToString($algorithm.ComputeHash($bytes))).Replace('-', '').ToLowerInvariant()
    }
    finally { $algorithm.Dispose() }
}

function Test-LabelWriterFenceHex([string]$Value, [int]$Length) {
    return $Value -cmatch ('\A[0-9a-f]{' + $Length.ToString([Globalization.CultureInfo]::InvariantCulture) + '}\z')
}

function ConvertTo-LabelWriterFenceUtc([string]$Value) {
    try {
        return [DateTime]::Parse(
            $Value,
            [Globalization.CultureInfo]::InvariantCulture,
            [Globalization.DateTimeStyles]::RoundtripKind
        ).ToUniversalTime()
    }
    catch { throw 'LABEL_WRITER_FENCE_TIMESTAMP_INVALID' }
}

function Get-LabelWriterSessionAuthorityMutexName(
    [string]$SessionId,
    [string]$AttemptId,
    [string]$OrchestratorSha256,
    [string]$ReplacementTransactionId,
    [string]$WriterContractSha256
) {
    $tuple = @(
        $Script:LabelWriterFenceTupleVersion,
        $SessionId,
        $AttemptId,
        $OrchestratorSha256,
        $ReplacementTransactionId,
        $WriterContractSha256
    ) -join "`n"
    return $Script:LabelWriterFenceSessionMutexPrefix + (Get-LabelWriterFenceStringSha256 $tuple)
}

function Get-LabelWriterFenceControlRoot {
    param([string]$ControlRoot = '')
    if (-not [string]::IsNullOrWhiteSpace($ControlRoot)) {
        return [IO.Path]::GetFullPath($ControlRoot)
    }
    if ([string]::IsNullOrWhiteSpace($env:LOCALAPPDATA)) {
        throw 'LABEL_WRITER_FENCE_LOCALAPPDATA_UNAVAILABLE'
    }
    return Join-Path ([IO.Path]::GetFullPath($env:LOCALAPPDATA)) 'KMTech\DirectSync\label_match\control\writer-session'
}

function Get-LabelWriterFenceNormalizedRoot([string]$ControlRoot) {
    return ([IO.Path]::GetFullPath($ControlRoot).TrimEnd('\').Normalize([Text.NormalizationForm]::FormC).ToLowerInvariant())
}

function Get-LabelWriterAdmissionMutexName {
    param([string]$ControlRoot = '')
    $selected = Get-LabelWriterFenceNormalizedRoot (Get-LabelWriterFenceControlRoot $ControlRoot)
    $production = ''
    try { $production = Get-LabelWriterFenceNormalizedRoot (Get-LabelWriterFenceControlRoot) } catch { }
    if (-not [string]::IsNullOrWhiteSpace($production) -and $selected -ceq $production) {
        return $Script:LabelWriterFenceAdmissionMutexName
    }
    return $Script:LabelWriterFenceAdmissionMutexName + '.' + (Get-LabelWriterFenceStringSha256 $selected).Substring(0, 16)
}

function Assert-LabelWriterFenceNoReparse([string]$PathValue) {
    $full = [IO.Path]::GetFullPath($PathValue)
    $root = [IO.Path]::GetPathRoot($full)
    $relative = $full.Substring($root.Length)
    $current = $root
    foreach ($part in @($relative -split '[\\/]' | Where-Object { $_ })) {
        $current = Join-Path $current $part
        if (Test-Path -LiteralPath $current) {
            $item = Get-Item -LiteralPath $current -Force -ErrorAction Stop
            if (($item.Attributes -band [IO.FileAttributes]::ReparsePoint) -ne 0) {
                throw 'LABEL_WRITER_FENCE_REPARSE_REJECTED'
            }
        }
    }
}

function Test-LabelWriterFenceExactPropertySet($Value, [string[]]$Expected) {
    if ($null -eq $Value) { return $false }
    $actual = @($Value.PSObject.Properties.Name)
    if ($actual.Count -ne $Expected.Count) { return $false }
    foreach ($name in $Expected) {
        if ($name -cnotin $actual) { return $false }
    }
    return $true
}

function Enter-LabelWriterAdmission {
    param([string]$ControlRoot = '', [int]$TimeoutMilliseconds = 5000)
    $name = Get-LabelWriterAdmissionMutexName $ControlRoot
    $created = $false
    $mutex = New-Object Threading.Mutex($false, $name, [ref]$created)
    try {
        try { $acquired = $mutex.WaitOne([Math]::Max(0, $TimeoutMilliseconds)) }
        catch [Threading.AbandonedMutexException] {
            try { $mutex.ReleaseMutex() } catch { }
            throw 'LABEL_WRITER_ADMISSION_MUTEX_ABANDONED'
        }
        if (-not $acquired) { throw 'LABEL_WRITER_ADMISSION_MUTEX_TIMEOUT' }
        return [pscustomobject][ordered]@{ mutex=$mutex; name=$name; acquired=$true }
    }
    catch {
        $mutex.Dispose()
        throw
    }
}

function Exit-LabelWriterAdmission($Lease) {
    if ($null -eq $Lease) { return }
    try {
        if ($Lease.acquired -isnot [bool] -or -not [bool]$Lease.acquired) {
            throw 'LABEL_WRITER_ADMISSION_LEASE_INVALID'
        }
        $Lease.mutex.ReleaseMutex()
    }
    finally { $Lease.mutex.Dispose() }
}

function Enter-LabelWriterSessionAuthority {
    param(
        [string]$SessionId,
        [string]$AttemptId,
        [string]$OrchestratorSha256,
        [string]$ReplacementTransactionId,
        [string]$WriterContractSha256
    )
    $name = Get-LabelWriterSessionAuthorityMutexName $SessionId $AttemptId $OrchestratorSha256 $ReplacementTransactionId $WriterContractSha256
    $created = $false
    $mutex = New-Object Threading.Mutex($true, $name, [ref]$created)
    if (-not $created) {
        $mutex.Dispose()
        throw 'LABEL_WRITER_SESSION_AUTHORITY_ALREADY_EXISTS'
    }
    return [pscustomobject][ordered]@{ mutex=$mutex; name=$name; acquired=$true }
}

function Exit-LabelWriterSessionAuthority($Lease) {
    if ($null -eq $Lease) { return }
    try { $Lease.mutex.ReleaseMutex() }
    finally { $Lease.mutex.Dispose() }
}

function Test-LabelWriterSessionAuthorityHeldByOther([string]$Name) {
    $mutex = $null
    try {
        $mutex = [Threading.Mutex]::OpenExisting($Name)
        try { $acquired = $mutex.WaitOne(0) }
        catch [Threading.AbandonedMutexException] {
            try { $mutex.ReleaseMutex() } catch { }
            throw 'LABEL_WRITER_SESSION_AUTHORITY_ABANDONED'
        }
        if ($acquired) {
            $mutex.ReleaseMutex()
            return $false
        }
        return $true
    }
    catch [Threading.WaitHandleCannotBeOpenedException] { return $false }
    finally { if ($null -ne $mutex) { $mutex.Dispose() } }
}

function Get-LabelWriterFenceActivePath([string]$ControlRoot = '') {
    return Join-Path (Get-LabelWriterFenceControlRoot $ControlRoot) 'active.json'
}

function Read-LabelWriterFence {
    param([string]$ControlRoot = '', [switch]$AllowAbsent)
    $root = Get-LabelWriterFenceControlRoot $ControlRoot
    Assert-LabelWriterFenceNoReparse $root
    $path = Get-LabelWriterFenceActivePath $root
    if (-not (Test-Path -LiteralPath $path -PathType Leaf)) {
        if ($AllowAbsent.IsPresent) { return $null }
        throw 'LABEL_WRITER_FENCE_ABSENT'
    }
    Assert-LabelWriterFenceNoReparse $path
    $item = Get-Item -LiteralPath $path -Force -ErrorAction Stop
    if ($item.Length -le 0 -or $item.Length -gt $Script:LabelWriterFenceMaximumBytes) {
        throw 'LABEL_WRITER_FENCE_SIZE_INVALID'
    }
    $beforeLength = [int64]$item.Length
    $beforeMtime = $item.LastWriteTimeUtc.Ticks
    $payload = Get-Content -LiteralPath $path -Raw -Encoding UTF8 | ConvertFrom-Json
    $after = Get-Item -LiteralPath $path -Force -ErrorAction Stop
    if ([int64]$after.Length -ne $beforeLength -or $after.LastWriteTimeUtc.Ticks -ne $beforeMtime) {
        throw 'LABEL_WRITER_FENCE_CHANGED_DURING_READ'
    }
    $sources = @($payload.delegated_sources)
    if (
        -not (Test-LabelWriterFenceExactPropertySet $payload $Script:LabelWriterFenceActiveFields) -or
        $payload.schema -isnot [string] -or
        $payload.status -isnot [string] -or
        $payload.app_id -isnot [string] -or
        $payload.session_id -isnot [string] -or
        $payload.attempt_id -isnot [string] -or
        $payload.replacement_transaction_id -isnot [string] -or
        $payload.session_started_at_utc -isnot [string] -or
        $payload.orchestrator_sha256 -isnot [string] -or
        $payload.writer_contract_sha256 -isnot [string] -or
        $payload.session_authority_mutex_name -isnot [string] -or
        $payload.writer_inventory_sha256 -isnot [string] -or
        $payload.owner_kind -isnot [string] -or
        $payload.delegation_sha256 -isnot [string] -or
        $payload.delegation_expires_at_utc -isnot [string] -or
        $payload.activated_at_utc -isnot [string] -or
        [string]$payload.schema -cne $Script:LabelWriterFenceActiveSchema -or
        [string]$payload.app_id -cne $Script:LabelWriterFenceAppId -or
        [string]$payload.status -cnotin @('QUIESCING','INSTALLING','RESTORING','RESTORE_FAILED') -or
        [string]$payload.owner_kind -cne 'canonical_installer' -or
        -not (Test-LabelWriterFenceHex ([string]$payload.session_id) 32) -or
        -not (Test-LabelWriterFenceHex ([string]$payload.attempt_id) 32) -or
        -not (Test-LabelWriterFenceHex ([string]$payload.replacement_transaction_id) 32) -or
        -not (Test-LabelWriterFenceHex ([string]$payload.orchestrator_sha256) 64) -or
        -not (Test-LabelWriterFenceHex ([string]$payload.writer_contract_sha256) 64) -or
        [string]$payload.writer_inventory_sha256 -cne $Script:LabelWriterFenceInventorySha256 -or
        $payload.secret_values_recorded -isnot [bool] -or [bool]$payload.secret_values_recorded -or
        $payload.delegated_sources -isnot [Object[]] -or
        @($payload.delegated_sources | Where-Object { $_ -isnot [string] }).Count -ne 0
    ) { throw 'LABEL_WRITER_FENCE_BINDING_INVALID' }
    [void](ConvertTo-LabelWriterFenceUtc ([string]$payload.session_started_at_utc))
    [void](ConvertTo-LabelWriterFenceUtc ([string]$payload.activated_at_utc))
    $expectedName = Get-LabelWriterSessionAuthorityMutexName ([string]$payload.session_id) ([string]$payload.attempt_id) ([string]$payload.orchestrator_sha256) ([string]$payload.replacement_transaction_id) ([string]$payload.writer_contract_sha256)
    if ([string]$payload.session_authority_mutex_name -cne $expectedName) {
        throw 'LABEL_WRITER_FENCE_AUTHORITY_NAME_INVALID'
    }
    $sorted = @($sources | Sort-Object -Unique)
    if ($sorted.Count -ne $sources.Count) { throw 'LABEL_WRITER_FENCE_DELEGATION_INVALID' }
    for ($index = 0; $index -lt $sources.Count; $index++) {
        if ([string]::IsNullOrWhiteSpace([string]$sources[$index]) -or [string]$sources[$index] -cne [string]$sorted[$index]) {
            throw 'LABEL_WRITER_FENCE_DELEGATION_INVALID'
        }
    }
    if ($sources.Count -eq 0) {
        if (-not [string]::IsNullOrEmpty([string]$payload.delegation_sha256) -or -not [string]::IsNullOrEmpty([string]$payload.delegation_expires_at_utc)) {
            throw 'LABEL_WRITER_FENCE_DELEGATION_INVALID'
        }
    }
    else {
        if (-not (Test-LabelWriterFenceHex ([string]$payload.delegation_sha256) 64)) {
            throw 'LABEL_WRITER_FENCE_DELEGATION_INVALID'
        }
        [void](ConvertTo-LabelWriterFenceUtc ([string]$payload.delegation_expires_at_utc))
    }
    return $payload
}

function Write-LabelWriterFenceAtomic([string]$ControlRoot, $Payload) {
    $root = Get-LabelWriterFenceControlRoot $ControlRoot
    Assert-LabelWriterFenceNoReparse $root
    [void](New-Item -ItemType Directory -Path $root -Force)
    Assert-LabelWriterFenceNoReparse $root
    $path = Get-LabelWriterFenceActivePath $root
    $temporary = Join-Path $root ('.active.' + [Guid]::NewGuid().ToString('N') + '.tmp')
    $backup = Join-Path $root ('.active.' + [Guid]::NewGuid().ToString('N') + '.bak')
    try {
        $json = ($Payload | ConvertTo-Json -Depth 20) + "`n"
        $bytes = (New-Object Text.UTF8Encoding($false)).GetBytes($json)
        $stream = New-Object IO.FileStream($temporary, [IO.FileMode]::CreateNew, [IO.FileAccess]::Write, [IO.FileShare]::None)
        try {
            $stream.Write($bytes, 0, $bytes.Length)
            $stream.Flush($true)
        }
        finally { $stream.Dispose() }
        if (Test-Path -LiteralPath $path -PathType Leaf) {
            [IO.File]::Replace($temporary, $path, $backup)
        }
        else { [IO.File]::Move($temporary, $path) }
    }
    finally {
        foreach ($cleanupPath in @($temporary, $backup)) {
            if (Test-Path -LiteralPath $cleanupPath) { Remove-Item -LiteralPath $cleanupPath -Force }
        }
    }
    $readback = Read-LabelWriterFence $root
    if (($readback | ConvertTo-Json -Depth 20 -Compress) -cne ($Payload | ConvertTo-Json -Depth 20 -Compress)) {
        throw 'LABEL_WRITER_FENCE_WRITE_READBACK_FAILED'
    }
    return $readback
}

function New-LabelWriterFencePayload {
    param(
        [string]$Status,
        [string]$SessionId,
        [string]$AttemptId,
        [string]$ReplacementTransactionId,
        [string]$SessionStartedAtUtc,
        [string]$OrchestratorSha256,
        [string]$WriterContractSha256,
        [string]$DelegationToken = '',
        [string[]]$DelegatedSources = @(),
        [string]$DelegationExpiresAtUtc = ''
    )
    $sources = @($DelegatedSources | Sort-Object -Unique)
    return [pscustomobject][ordered]@{
        schema=$Script:LabelWriterFenceActiveSchema
        status=$Status
        app_id=$Script:LabelWriterFenceAppId
        session_id=$SessionId
        attempt_id=$AttemptId
        replacement_transaction_id=$ReplacementTransactionId
        session_started_at_utc=$SessionStartedAtUtc
        orchestrator_sha256=$OrchestratorSha256
        writer_contract_sha256=$WriterContractSha256
        session_authority_mutex_name=Get-LabelWriterSessionAuthorityMutexName $SessionId $AttemptId $OrchestratorSha256 $ReplacementTransactionId $WriterContractSha256
        writer_inventory_sha256=$Script:LabelWriterFenceInventorySha256
        owner_kind='canonical_installer'
        delegation_sha256=if ([string]::IsNullOrEmpty($DelegationToken)) { '' } else { Get-LabelWriterFenceStringSha256 $DelegationToken }
        delegated_sources=[Object[]]$sources
        delegation_expires_at_utc=$DelegationExpiresAtUtc
        activated_at_utc=[DateTime]::UtcNow.ToString('o')
        secret_values_recorded=$false
    }
}

function Start-LabelWriterFence {
    param(
        [string]$ControlRoot = '',
        [string]$Status = 'QUIESCING',
        [string]$SessionId,
        [string]$AttemptId,
        [string]$ReplacementTransactionId,
        [string]$SessionStartedAtUtc,
        [string]$OrchestratorSha256,
        [string]$WriterContractSha256,
        [string]$DelegationToken = '',
        [string[]]$DelegatedSources = @(),
        [string]$DelegationExpiresAtUtc = '',
        [switch]$AuthorityOwnedByCaller
    )
    $lease = Enter-LabelWriterAdmission $ControlRoot
    try {
        if ($null -ne (Read-LabelWriterFence $ControlRoot -AllowAbsent)) {
            throw 'LABEL_WRITER_FENCE_ALREADY_ACTIVE'
        }
        $payload = New-LabelWriterFencePayload `
            -Status $Status `
            -SessionId $SessionId `
            -AttemptId $AttemptId `
            -ReplacementTransactionId $ReplacementTransactionId `
            -SessionStartedAtUtc $SessionStartedAtUtc `
            -OrchestratorSha256 $OrchestratorSha256 `
            -WriterContractSha256 $WriterContractSha256 `
            -DelegationToken $DelegationToken `
            -DelegatedSources $DelegatedSources `
            -DelegationExpiresAtUtc $DelegationExpiresAtUtc
        if (-not $AuthorityOwnedByCaller.IsPresent -and -not (Test-LabelWriterSessionAuthorityHeldByOther ([string]$payload.session_authority_mutex_name))) {
            throw 'LABEL_WRITER_FENCE_SESSION_AUTHORITY_NOT_HELD'
        }
        return Write-LabelWriterFenceAtomic $ControlRoot $payload
    }
    finally { Exit-LabelWriterAdmission $lease }
}

function Set-LabelWriterFenceDelegation {
    param(
        [string]$ControlRoot = '',
        [string]$Status,
        [string]$SessionId,
        [string]$AttemptId,
        [string]$ReplacementTransactionId,
        [string]$DelegationToken,
        [string[]]$DelegatedSources,
        [int]$LifetimeSeconds = 300
    )
    if ($DelegationToken.Length -lt 32) { throw 'LABEL_WRITER_FENCE_DELEGATION_TOKEN_INVALID' }
    $lease = Enter-LabelWriterAdmission $ControlRoot
    try {
        $active = Read-LabelWriterFence $ControlRoot
        if (
            [string]$active.session_id -cne $SessionId -or
            [string]$active.attempt_id -cne $AttemptId -or
            [string]$active.replacement_transaction_id -cne $ReplacementTransactionId
        ) { throw 'LABEL_WRITER_FENCE_DELEGATION_SESSION_MISMATCH' }
        $active.status = $Status
        $active.delegation_sha256 = Get-LabelWriterFenceStringSha256 $DelegationToken
        $active.delegated_sources = [Object[]]@($DelegatedSources | Sort-Object -Unique)
        $active.delegation_expires_at_utc = [DateTime]::UtcNow.AddSeconds([Math]::Max(15, [Math]::Min(1200, $LifetimeSeconds))).ToString('o')
        $active.activated_at_utc = [DateTime]::UtcNow.ToString('o')
        return Write-LabelWriterFenceAtomic $ControlRoot $active
    }
    finally { Exit-LabelWriterAdmission $lease }
}

function Assert-LabelWriterFenceOwner {
    param(
        [string]$ControlRoot = '',
        [string]$SessionId,
        [string]$AttemptId,
        [string]$ReplacementTransactionId,
        [string]$DelegationToken = '',
        [string]$Source = ''
    )
    $active = Read-LabelWriterFence $ControlRoot
    if (
        [string]$active.session_id -cne $SessionId -or
        [string]$active.attempt_id -cne $AttemptId -or
        [string]$active.replacement_transaction_id -cne $ReplacementTransactionId
    ) { throw 'LABEL_WRITER_FENCE_OWNER_MISMATCH' }
    if (-not [string]::IsNullOrEmpty($DelegationToken)) {
        if (
            [string]$active.delegation_sha256 -cne (Get-LabelWriterFenceStringSha256 $DelegationToken) -or
            [string]$Source -cnotin @($active.delegated_sources) -or
            (ConvertTo-LabelWriterFenceUtc ([string]$active.delegation_expires_at_utc)) -lt [DateTime]::UtcNow -or
            -not (Test-LabelWriterSessionAuthorityHeldByOther ([string]$active.session_authority_mutex_name))
        ) { throw 'LABEL_WRITER_FENCE_OWNER_DELEGATION_MISMATCH' }
    }
    return $active
}

function Enter-LabelWriterDelegatedOperation {
    param(
        [string]$ControlRoot = '',
        [string]$SessionId,
        [string]$AttemptId,
        [string]$ReplacementTransactionId,
        [string]$DelegationToken,
        [string]$Source,
        [int]$TimeoutMilliseconds = 5000
    )
    $lease = Enter-LabelWriterAdmission $ControlRoot $TimeoutMilliseconds
    try {
        [void](Assert-LabelWriterFenceOwner `
            -ControlRoot $ControlRoot `
            -SessionId $SessionId `
            -AttemptId $AttemptId `
            -ReplacementTransactionId $ReplacementTransactionId `
            -DelegationToken $DelegationToken `
            -Source $Source)
        return $lease
    }
    catch {
        Exit-LabelWriterAdmission $lease
        throw
    }
}

function Stop-LabelWriterFence {
    param(
        [string]$ControlRoot = '',
        [string]$SessionId,
        [string]$AttemptId,
        [string]$ReplacementTransactionId,
        [int]$TimeoutMilliseconds = 90000
    )
    $lease = Enter-LabelWriterAdmission $ControlRoot $TimeoutMilliseconds
    try {
        [void](Assert-LabelWriterFenceOwner `
            -ControlRoot $ControlRoot `
            -SessionId $SessionId `
            -AttemptId $AttemptId `
            -ReplacementTransactionId $ReplacementTransactionId)
        $path = Get-LabelWriterFenceActivePath $ControlRoot
        Remove-Item -LiteralPath $path -Force
        if (Test-Path -LiteralPath $path) { throw 'LABEL_WRITER_FENCE_CLEAR_READBACK_FAILED' }
        return [pscustomobject][ordered]@{
            status='RELEASED'
            session_id=$SessionId
            attempt_id=$AttemptId
            replacement_transaction_id=$ReplacementTransactionId
            writer_inventory_sha256=$Script:LabelWriterFenceInventorySha256
        }
    }
    finally { Exit-LabelWriterAdmission $lease }
}

function Abort-LabelWriterFence {
    param(
        [string]$ControlRoot = '',
        [string]$SessionId,
        [string]$AttemptId,
        [string]$ReplacementTransactionId,
        [int]$TimeoutMilliseconds = 90000
    )
    return Stop-LabelWriterFence `
        -ControlRoot $ControlRoot `
        -SessionId $SessionId `
        -AttemptId $AttemptId `
        -ReplacementTransactionId $ReplacementTransactionId `
        -TimeoutMilliseconds $TimeoutMilliseconds
}
