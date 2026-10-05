$Script:KmtechRedactionRules = Get-Content -LiteralPath (Join-Path $PSScriptRoot 'failure_redaction_rules.json') -Raw -Encoding UTF8 | ConvertFrom-Json

function Test-KmtechSecretEnvironmentValue {
    param([string]$Name, [AllowEmptyString()][string]$Value)
    $rules = $Script:KmtechRedactionRules
    if ($Name -cin $rules.explicit) { return $true }
    if ($Name -notmatch $rules.pattern -or $Name -match $rules.path_suffix) { return $false }
    foreach ($identifier in $rules.identifier_patterns.PSObject.Properties) {
        if ($Name -match $identifier.Name -and
            $Value -cmatch ('\A(?:' + $identifier.Value + ')\z')) { return $false }
    }
    return $true
}

function Get-KmtechSinkEnvironment {
    $values = @{}
    # Internal product lookup only: names and values never leave this function.
    $process = [Environment]::GetEnvironmentVariables('Process')
    foreach ($name in $process.Keys) {
        if (Test-KmtechSecretEnvironmentValue -Name ([string]$name) -Value ([string]$process[$name])) {
            $values[$name] = $process[$name]
        }
    }
    return $values
}

function ConvertTo-KmtechRedactedSinkText {
    param([AllowEmptyString()][string]$Text, [Collections.IDictionary]$Environment = $null)
    $rules = $Script:KmtechRedactionRules
    if ($null -eq $Environment) { $Environment = Get-KmtechSinkEnvironment }
    $notes = [Collections.Generic.List[string]]::new()
    $entries = @($Environment.Keys | Where-Object {
        Test-KmtechSecretEnvironmentValue -Name ([string]$_) -Value ([string]$Environment[$_])
    } | Sort-Object @{Expression={([string]$Environment[$_]).Length};Descending=$true}, @{Expression={[string]$_}})
    foreach ($name in $entries) {
        $value = [string]$Environment[$name]
        if (-not $value) { continue }
        if ($value.Length -lt 4) { $notes.Add("[REDACTION_NOTE:short_value_skipped:$name]"); continue }
        $replacement = "[REDACTED:$name]"
        $escaped = ($value | ConvertTo-Json -Compress)
        $escaped = $escaped.Substring(1,$escaped.Length-2)
        $unicode = -join @($value.ToCharArray() | ForEach-Object { '\u{0:x4}' -f [int]$_ })
        foreach ($form in @($escaped,$unicode,$value.Replace("'", "''"),$value.Replace('"','""'),$value) | Select-Object -Unique) {
            $Text = $Text.Replace($form,$replacement)
        }
    }
    # Generated key/value grammar: ordinary prefixes leave their values searchable.
    $keyPattern = '(?<![A-Za-z0-9_.-])(?<key>"(?:\\.|[^"\\])*"|''(?:\\.|[^''\\])*''|[A-Za-z_][\w.-]*)(?<separator>\s*[:=]\s*)'
    $valuePattern = '\G(?:"(?:\\.|[^"\\])*"|''(?:\\.|[^''\\])*''|[^\s,;}]+)'
    $valueRegex = [regex]::new($valuePattern)
    $parts = [Text.StringBuilder]::new()
    $copied = 0; $protectedUntil = 0
    foreach ($match in [regex]::Matches($Text, $keyPattern)) {
        if ($match.Index -lt $protectedUntil) { continue }
        $quotes=[char[]]@('"',"'")
        $key=$match.Groups['key'].Value.Trim($quotes)
        $metadataKey=($key -match $rules.path_suffix -or
            ($key -match $rules.digest_suffix -and $key -notmatch $rules.short_secret_digest))
        $sensitive=($key -match $rules.pattern -or $key -imatch '^(pin|otp|password)$')
        if (-not $sensitive -and -not $metadataKey) { continue }
        $valueMatch=$valueRegex.Match($Text,$match.Index+$match.Length)
        if (-not $valueMatch.Success) { continue }
        $value=$valueMatch.Value
        $protectedUntil=$valueMatch.Index+$valueMatch.Length
        if ($metadataKey -or $value -cin @('true','false','null') -or
            $value.Trim($quotes).StartsWith('[REDACTED:')) { continue }
        $replacement=if($value.StartsWith('"') -or $value.StartsWith("'")){
            $value.Substring(0,1)+$rules.field+$value.Substring(0,1)
        }else{$rules.field}
        [void]$parts.Append($Text,$copied,$match.Index-$copied)
        [void]$parts.Append($match.Groups['key'].Value+$match.Groups['separator'].Value+$replacement)
        $copied=$protectedUntil
    }
    [void]$parts.Append($Text,$copied,$Text.Length-$copied)
    $Text=$parts.ToString()
    $Text=[regex]::Replace($Text,'(?i)\b(Bearer\s+)(?!\[REDACTED:)[^\s"'',;]+',[Text.RegularExpressions.MatchEvaluator]{param($match) $match.Groups[1].Value+$rules.field})
    if ($notes.Count) { $Text += [string][char]10 + ($notes -join [string][char]10) }
    $utf8 = [Text.UTF8Encoding]::new($false,$true)
    $bytes = $utf8.GetBytes($Text)
    if ($bytes.Length -le $rules.limit) { return $Text }
    $headLength = [int]$rules.head
    while (($bytes[$headLength] -band 0xC0) -eq 0x80) { $headLength-- }
    $tailLength = [int]$rules.limit - $headLength
    do {
        $omitted = $bytes.Length - $headLength - $tailLength
        $marker = [string][char]10 + "[TRUNCATED: $omitted bytes omitted]" + [string][char]10
        $next = [int]$rules.limit - $headLength - $utf8.GetByteCount($marker)
        if ($next -ge $tailLength) { break }
        $tailLength = $next
    } while ($true)
    $start = $bytes.Length - $tailLength
    while (($bytes[$start] -band 0xC0) -eq 0x80) { $start++ }
    $omitted = $start - $headLength
    $marker = [string][char]10 + "[TRUNCATED: $omitted bytes omitted]" + [string][char]10
    return $utf8.GetString($bytes,0,$headLength) + $marker + $utf8.GetString($bytes,$start,$bytes.Length-$start)
}

function Get-KmtechSinkReplacements {
    $environment = Get-KmtechSinkEnvironment
    $result = [Collections.Generic.Dictionary[string,string]]::new([StringComparer]::Ordinal)
    foreach ($name in $environment.Keys) {
        $value = [string]$environment[$name]
        if ($value.Length -lt 4) { continue }
        $escaped = ($value | ConvertTo-Json -Compress)
        $escaped = $escaped.Substring(1,$escaped.Length-2)
        $unicode = -join @($value.ToCharArray() | ForEach-Object { '\u{0:x4}' -f [int]$_ })
        foreach ($form in @($escaped,$unicode,$value.Replace("'", "''"),$value.Replace('"','""'),$value) | Select-Object -Unique) {
            if (-not $result.ContainsKey($form)) { $result[$form] = "[REDACTED:$name]" }
        }
    }
    return ,$result
}

function Assert-KmtechMachineMetadataText {
    param([AllowEmptyString()][string]$Text, [Collections.IDictionary]$Environment = $null)
    if ($null -eq $Environment) { $Environment = Get-KmtechSinkEnvironment }
    foreach ($name in $Environment.Keys) {
        $value = [string]$Environment[$name]
        if (-not $value -or -not (Test-KmtechSecretEnvironmentValue -Name ([string]$name) -Value $value)) { continue }
        $escaped = ($value | ConvertTo-Json -Compress)
        $escaped = $escaped.Substring(1, $escaped.Length - 2)
        $unicode = -join @($value.ToCharArray() | ForEach-Object { '\u{0:x4}' -f [int]$_ })
        foreach ($form in @($value, $escaped, $unicode, $value.Replace("'", "''"), $value.Replace('"', '""')) | Select-Object -Unique) {
            if ($Text.Contains($form)) { throw 'PRODUCT_METADATA_SECRET_VALUE_REFUSED' }
        }
    }
}

function ConvertTo-KmtechRedactedSinkObject {
    param([AllowNull()]$Value, [Collections.IDictionary]$Environment = $null)
    if ($null -eq $Value) { return $null }
    if ($Value -is [string]) { return ConvertTo-KmtechRedactedSinkText -Text $Value -Environment $Environment }
    if ($Value -is [Collections.IDictionary] -or $Value -is [pscustomobject]) {
        $result = [ordered]@{}
        $names = if ($Value -is [Collections.IDictionary]) { @($Value.Keys) } else {
            @($Value.PSObject.Properties | ForEach-Object { $_.Name })
        }
        foreach ($name in $names) {
            $entry = if ($Value -is [Collections.IDictionary]) { $Value[$name] } else { $Value.$name }
            $rules=$Script:KmtechRedactionRules
            $metadata=($null -eq $entry -or $entry -is [bool] -or [string]$name -match $rules.path_suffix -or
                ([string]$name -match $rules.digest_suffix -and [string]$name -notmatch $rules.short_secret_digest))
            if (-not $metadata -and ([string]$name -match $rules.pattern -or [string]$name -imatch '^(pin|otp|password)$')) {
                $result[$name] = $Script:KmtechRedactionRules.field
            } else { $result[$name] = ConvertTo-KmtechRedactedSinkObject -Value $entry -Environment $Environment }
        }
        return $result
    }
    if ($Value -is [Collections.IEnumerable]) {
        $result = @($Value | ForEach-Object { ConvertTo-KmtechRedactedSinkObject -Value $_ -Environment $Environment })
        return ,$result
    }
    return $Value
}

function Write-KmtechProtectedFailureSidecar {
    param([Parameter(Mandatory)][string]$Text)
    $protected=ConvertTo-KmtechRedactedSinkText -Text $Text
    $bytes=[Text.Encoding]::UTF8.GetBytes($protected)
    $root=[IO.Path]::GetFullPath([IO.Path]::GetTempPath())
    $ancestor=$root
    while($ancestor){
        $item=Get-Item -LiteralPath $ancestor -ErrorAction Stop
        if(-not $item.PSIsContainer -or ($item.Attributes -band [IO.FileAttributes]::ReparsePoint)){
            throw 'Diagnostic parent is not an ordinary directory.'
        }
        $parent=Split-Path -Parent $ancestor
        if($parent -eq $ancestor){break};$ancestor=$parent
    }
    $directory=Join-Path $root ('label-legacy-native-diagnostic-'+[Guid]::NewGuid().ToString('N'))
    [void](New-Item -ItemType Directory -Path $directory -ErrorAction Stop)
    $acl=[Security.AccessControl.DirectorySecurity]::new();$acl.SetAccessRuleProtection($true,$false)
    foreach($sid in @([Security.Principal.WindowsIdentity]::GetCurrent().User,
        [Security.Principal.SecurityIdentifier]::new('S-1-5-18'),
        [Security.Principal.SecurityIdentifier]::new('S-1-5-32-544'))){
        $acl.AddAccessRule([Security.AccessControl.FileSystemAccessRule]::new($sid,'FullControl','ContainerInherit,ObjectInherit','None','Allow'))
    }
    Set-Acl -LiteralPath $directory -AclObject $acl -ErrorAction Stop
    $path=Join-Path $directory 'native-exception.txt'
    $stream=[IO.File]::Open($path,'CreateNew','Write','Read')
    try{$stream.Write($bytes,0,$bytes.Length);$stream.Flush($true)}finally{$stream.Dispose()}
    $readback=[IO.File]::ReadAllBytes($path)
    if($readback.Length -ne $bytes.Length){throw 'Diagnostic readback differs.'}
    $hash=[Security.Cryptography.SHA256]::Create()
    try{
        $sha=([BitConverter]::ToString($hash.ComputeHash($readback))).Replace('-','').ToLowerInvariant()
        $expected=([BitConverter]::ToString($hash.ComputeHash($bytes))).Replace('-','').ToLowerInvariant()
    }finally{$hash.Dispose()}
    if($sha -cne $expected){throw 'Diagnostic readback differs.'}
    return [ordered]@{path=$path;sha256=$sha;bytes=$bytes.Length;limit_bytes=65536;truncated=$protected.Contains('[TRUNCATED:')}
}
