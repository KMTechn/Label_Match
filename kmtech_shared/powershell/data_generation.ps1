# Leaf only: callers verify release metadata and hold existing writer admission.
# Marker parents must already exist. This helper never opens business databases.
function ConvertTo-KmtechDataVersion([string]$Version) {
    if ($Version -cnotmatch '\Av?(0|[1-9][0-9]{0,8})\.(0|[1-9][0-9]{0,8})\.(0|[1-9][0-9]{0,8})\z') {
        throw 'Data generation version is not comparable.'
    }
    return [Version]::new([int]$Matches[1], [int]$Matches[2], [int]$Matches[3])
}

function Assert-KmtechDataIdentity([string]$AppId, [string]$SubjectSid) {
    if ($AppId -cnotmatch '\A[A-Za-z0-9_.-]{1,64}\z' -or
        $SubjectSid.Length -gt 256 -or $SubjectSid -cnotmatch '\AS-1-[0-9]+(?:-[0-9]+){1,15}\z') {
        throw 'Data generation application or user identity is invalid.'
    }
}

function Assert-KmtechDataBuildIdentity([string]$BuildId) {
    if ($BuildId -cnotmatch '\A(?:[0-9a-f]{40}|[0-9a-f]{64})\z') {
        throw 'Data generation build identity is invalid.'
    }
}

function Get-KmtechDataLiteralPath([string]$Path) {
    if ([string]::IsNullOrWhiteSpace($Path) -or -not [IO.Path]::IsPathRooted($Path) -or
        $Path.StartsWith('\\?\') -or $Path.StartsWith('\\.\')) {
        throw 'Data generation path must be literal and absolute.'
    }
    $full = [IO.Path]::GetFullPath($Path)
    $component = $full
    while ($component) {
        try {
            $attributes = [IO.File]::GetAttributes($component)
            if (($attributes -band [IO.FileAttributes]::ReparsePoint) -ne 0) {
                throw 'Data generation path contains a reparse point.'
            }
            if ($component -cne $full -and ($attributes -band [IO.FileAttributes]::Directory) -eq 0) {
                throw 'Data generation ancestor is not a directory.'
            }
        }
        catch [IO.FileNotFoundException] { }
        catch [IO.DirectoryNotFoundException] { }
        $parent = [IO.Path]::GetDirectoryName($component.TrimEnd('\','/'))
        if ($parent -ceq $component) { break }
        $component = $parent
    }
    return $full
}

function Read-KmtechDataGeneration([string]$Path, [string]$AppId, [string]$SubjectSid) {
    $full = Get-KmtechDataLiteralPath $Path
    try { $attributes = [IO.File]::GetAttributes($full) }
    catch [IO.FileNotFoundException] { return $null }
    catch [IO.DirectoryNotFoundException] { return $null }
    if (($attributes -band [IO.FileAttributes]::Directory) -ne 0) {
        throw 'Data generation marker is not a file.'
    }
    $stream = [IO.File]::Open($full, [IO.FileMode]::Open, [IO.FileAccess]::Read, [IO.FileShare]::Read)
    try {
        if ($stream.Length -le 0 -or $stream.Length -gt 4096) { throw 'Data generation marker size is invalid.' }
        $reader = [IO.StreamReader]::new($stream, [Text.UTF8Encoding]::new($false,$true), $false)
        try { $raw = $reader.ReadToEnd() } finally { $reader.Dispose() }
    }
    finally { $stream.Dispose() }
    [void](Get-KmtechDataLiteralPath $full)
    $record = $raw | ConvertFrom-Json -ErrorAction Stop
    $fields = @('schema','app_id','subject_sid','version','data_compatible_min_version','build_id','compatibility_verified')
    if (@($record.PSObject.Properties.Name).Count -ne $fields.Count) { throw 'Data generation fields differ.' }
    foreach ($field in $fields) {
        if ($null -eq $record.PSObject.Properties[$field] -or
            ($field -ceq 'compatibility_verified' -and $record.$field -isnot [bool]) -or
            ($field -cne 'compatibility_verified' -and $record.$field -isnot [string]) -or
            [regex]::Matches($raw, ('"' + $field + '"\s*:')).Count -ne 1) {
            throw 'Data generation field type or uniqueness differs.'
        }
    }
    if ($record.schema -cne 'kmtech-data-generation-v2' -or $record.app_id -cne $AppId -or $record.subject_sid -cne $SubjectSid) {
        throw 'Data generation identity differs.'
    }
    Assert-KmtechDataBuildIdentity $record.build_id
    $version = ConvertTo-KmtechDataVersion $record.version
    $floor = ConvertTo-KmtechDataVersion $record.data_compatible_min_version
    if ($floor.CompareTo($version) -gt 0) { throw 'Data generation compatibility floor is invalid.' }
    if (-not $record.compatibility_verified -and $floor.CompareTo($version) -ne 0) { throw 'Unverified data generation compatibility floor.' }
    return $record
}

function Get-KmtechDataDowngradeDecision(
    [string[]]$MarkerPaths, [string]$TargetVersion, [string]$AppId, [string]$SubjectSid, [string]$TargetBuildId
) {
    try {
        Assert-KmtechDataIdentity $AppId $SubjectSid
        Assert-KmtechDataBuildIdentity $TargetBuildId
        $target = ConvertTo-KmtechDataVersion $TargetVersion
        if (@($MarkerPaths).Count -eq 0) { throw 'Data generation marker locations are absent.' }
        $latest = $null
        $floor = $null
        $newest = @()
        foreach ($path in $MarkerPaths) {
            $record = Read-KmtechDataGeneration $path $AppId $SubjectSid
            if ($null -eq $record) { continue }
            $version = ConvertTo-KmtechDataVersion $record.version
            $minimum = ConvertTo-KmtechDataVersion $record.data_compatible_min_version
            if ($null -eq $latest -or $version.CompareTo($latest) -gt 0) {
                $latest = $version; $floor = $minimum
                $newest = @($record)
            }
            elseif ($version.CompareTo($latest) -eq 0) {
                $newest += $record
                if ($minimum.CompareTo($floor) -gt 0) { $floor = $minimum }
            }
        }
        if ($target.CompareTo($latest) -eq 0) {
            foreach ($record in $newest) {
                if ($record.build_id -cne $TargetBuildId -and -not $record.compatibility_verified) {
                    throw 'Same version has a different unverified build.'
                }
            }
        }
        if ($null -eq $latest) {
            return [pscustomobject]@{status='ALLOW';allowed=$true;message_key='no_generation';message='';version=$null;minimum_version=$null}
        }
        $allowed = $target.CompareTo($floor) -ge 0
        return [pscustomobject]@{
            status=$(if ($allowed) {'ALLOW'} else {'REFUSE'}); allowed=$allowed
            message_key=$(if ($allowed) {'compatible_generation'} else {'data_generation_downgrade_refused'})
            message=$(if ($allowed) {''} else {'이 PC의 자료가 새 판으로 이미 열려 옛 판으로 되돌릴 수 없습니다. 같은 새 판을 다시 시작하거나 고친 판으로 지원 업데이트하세요. 관리자 지원 복구는 설치 전 백업과 옛 판을 함께 복원해야 합니다.'})
            version=('v' + $latest.ToString()); minimum_version=('v' + $floor.ToString())
        }
    }
    catch {
        return [pscustomobject]@{
            status='UNKNOWN';allowed=$false;message_key='data_generation_unknown'
            message='이 PC의 자료 판을 확인할 수 없어 옛 판으로 되돌릴 수 없습니다. 자료와 확인 기록을 보존하고 관리자 지원 복구를 요청하세요.'
            version=$null;minimum_version=$null
        }
    }
}

function Write-KmtechDataGeneration(
    [string]$MarkerPath, [string]$Version, [string]$AppId, [string]$SubjectSid,
    [string]$DataCompatibleMinVersion = '', [bool]$CompatibilityVerified = $false, [string]$BuildId
) {
    Assert-KmtechDataIdentity $AppId $SubjectSid
    Assert-KmtechDataBuildIdentity $BuildId
    $CompatibilityVerified = $CompatibilityVerified -and -not [string]::IsNullOrWhiteSpace($DataCompatibleMinVersion)
    $release = ConvertTo-KmtechDataVersion $Version
    $floor = $release
    if ($CompatibilityVerified -and $DataCompatibleMinVersion) {
        $floor = ConvertTo-KmtechDataVersion $DataCompatibleMinVersion
    }
    if ($floor.CompareTo($release) -gt 0) { throw 'Data generation compatibility floor exceeds release.' }
    $full = Get-KmtechDataLiteralPath $MarkerPath
    if (-not [IO.Directory]::Exists([IO.Path]::GetDirectoryName($full))) { throw 'Data generation parent must already exist.' }
    $previous = Read-KmtechDataGeneration $full $AppId $SubjectSid
    if ($null -ne $previous) {
        $comparison = (ConvertTo-KmtechDataVersion $previous.version).CompareTo($release)
        if ($comparison -gt 0 -or ($comparison -eq 0 -and $previous.build_id -ceq $BuildId)) { return $previous }
        if ($comparison -eq 0) {
            # Bind the admitted new build; never inherit a predecessor's grant.
            $CompatibilityVerified = $CompatibilityVerified -and $previous.compatibility_verified
            if (-not $CompatibilityVerified) { $floor = $release }
            else {
                $previousFloor = ConvertTo-KmtechDataVersion $previous.data_compatible_min_version
                if ($previousFloor.CompareTo($floor) -gt 0) { $floor = $previousFloor }
            }
        }
    }
    $record = [ordered]@{
        schema='kmtech-data-generation-v2';app_id=$AppId;subject_sid=$SubjectSid
        version=('v' + $release.ToString());data_compatible_min_version=('v' + $floor.ToString())
        build_id=$BuildId;compatibility_verified=$CompatibilityVerified
    }
    $temporary = $full + '.tmp.' + [Guid]::NewGuid().ToString('N')
    try {
        [void](Get-KmtechDataLiteralPath $temporary)
        $bytes = [Text.UTF8Encoding]::new($false).GetBytes(($record | ConvertTo-Json -Compress) + "`n")
        $stream = [IO.File]::Open($temporary, [IO.FileMode]::CreateNew, [IO.FileAccess]::Write, [IO.FileShare]::None)
        try { $stream.Write($bytes,0,$bytes.Length); $stream.Flush($true) } finally { $stream.Dispose() }
        [void](Get-KmtechDataLiteralPath $full)
        if ([IO.File]::Exists($full)) { [IO.File]::Replace($temporary,$full,[NullString]::Value) }
        else { [IO.File]::Move($temporary,$full) }
        $readback = Read-KmtechDataGeneration $full $AppId $SubjectSid
        foreach ($field in $record.Keys) {
            if ($null -eq $readback -or $readback.$field -cne $record[$field]) {
                throw 'Data generation durable readback differs.'
            }
        }
    }
    finally { if ([IO.File]::Exists($temporary)) { [IO.File]::Delete($temporary) } }
    return [pscustomobject]$record
}
