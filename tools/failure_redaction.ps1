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
function ConvertTo-KmtechPublicRecord {
    param([Collections.IDictionary]$Fields,
          [string[]]$AllowedCodes = $Script:KmtechRedactionRules.allowed_codes,
          [string[]]$SafeMessages = $Script:KmtechRedactionRules.safe_messages)
    $patterns = @{
        key='\A[a-z][a-z0-9_]{0,63}\z'; identifier='\A[A-Za-z_][A-Za-z0-9_.]{0,127}\z'
        sha256='\A[0-9a-f]{64}\z'; commit='\A(?:[0-9a-f]{40}|[0-9a-f]{64})\z'
        version='\Av?[0-9]{1,5}(?:\.[0-9]{1,5}){1,3}\z'
        id='\A(?:[0-9A-Fa-f]{8}-[0-9A-Fa-f]{4}-[0-9A-Fa-f]{4}-[0-9A-Fa-f]{4}-[0-9A-Fa-f]{12}|[0-9A-Fa-f]{32}|[0-9A-Fa-f]{64})\z'
        utc='\A[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}(?:\.[0-9]{1,7})?Z\z'
        path='\A(?:[A-Za-z]:\\[^\x00-\x1f"<>|?*]{0,256}|/[^\x00-\x1f]{0,255})\z'
    }
    $result = [ordered]@{}; $bad = [Collections.Generic.List[string]]::new(); $invalid = 0
    foreach ($key in $Fields.Keys) {
        if ($key -isnot [string] -or -not [regex]::IsMatch($key,$patterns.key)) { $invalid++; continue }
        $value=$Fields[$key]; $kind=''
        switch -CaseSensitive ($key) {
            {$_ -cin @('status','stage','code')} {$kind='code';break}
            'guidance' {$kind='message';break}
            {$_ -cin @('exception_type','location_module','location_function')} {$kind='identifier';break}
            {$_ -cin @('exit_code','location_line')} {$kind='integer';break}
            'version' {$kind='version';break}
            'utc' {$kind='utc';break}
            default {
                foreach ($rule in @(
                    @('_(status|stage|code)$','code'), @('_message$','message'),
                    @('_(count|bytes|ms|line)$','integer'), @('_sha256$','sha256'),
                    @('_commit$','commit'), @('_version$','version'), @('_id$','id'),
                    @('_utc$','utc'), @('_(path|dir|root|file)$','path'))) {
                    if ([regex]::IsMatch($key,$rule[0])) {$kind=$rule[1];break}
                }
            }
        }
        $allowed=$null -eq $value -or $value -is [bool]
        if (-not $allowed) {
            if ($kind -ceq 'integer') {
                $allowed=$value -is [byte] -or $value -is [sbyte] -or $value -is [int16] -or $value -is [uint16] -or $value -is [int32] -or $value -is [uint32] -or $value -is [int64] -or $value -is [uint64]
            } elseif ($value -is [string]) {
                if ($kind -ceq 'code') {$allowed=$AllowedCodes -ccontains $value}
                elseif ($kind -ceq 'message') {$allowed=$SafeMessages -ccontains $value}
                elseif ($patterns.ContainsKey($kind)) {$allowed=[regex]::IsMatch($value,$patterns[$kind])}
            }
        }
        if ($allowed) {$result[$key]=$value} else {$bad.Add($key)}
    }
    if ($bad.Count) {$result.nonconforming_fields=@($bad.ToArray())}
    if ($invalid) {$result.nonconforming_invalid_key_count=$invalid}
    if ($bad.Count -or $invalid) {$result.public_record_code='PUBLIC_FIELD_NONCONFORMING'}
    return $result
}

function ConvertTo-KmtechPublicDiagnosticTree {
    param([AllowNull()][object]$Value)
    # Transaction/recovery metadata is re-read by supported removal. Only the
    # diagnostic subrecords are public records; those are never recovery input.
    if ($Value -is [Collections.IDictionary] -or $Value -is [pscustomobject]) {
        $fields=[ordered]@{}
        if ($Value -is [Collections.IDictionary]) {
            foreach ($key in $Value.Keys) {$fields[[string]$key]=$Value[$key]}
        } else { foreach ($property in $Value.PSObject.Properties) {$fields[$property.Name]=$property.Value} }
        if ($fields.Contains('returncode') -and ($fields.Contains('stdout') -or $fields.Contains('stderr'))) {
            $result=[ordered]@{}
            foreach ($key in $fields.Keys) {
                if ($key -cin @('stdout','stderr') -and $fields[$key]) {$result[$key]=ConvertTo-KmtechPublicDiagnosticTree ([ordered]@{message=$fields[$key]})}
                else {$result[$key]=ConvertTo-KmtechPublicDiagnosticTree $fields[$key]}
            }
            return $result
        }
        if (@($fields.Keys | Where-Object {$_ -cin @('message','last_line','inner_exception_message','stdout','stderr','exception_chain')}).Count) {
            $candidate=[ordered]@{status='FAILED';reason_code='INSTALLER_FAILED';guidance=[string]$Script:KmtechRedactionRules.safe_messages[0]}
            foreach ($key in $fields.Keys) {
                if ($key -cin @('message','last_line','inner_exception_message','stdout','stderr','exception_chain')) {continue}
                $candidate[$key]=$fields[$key]
            }
            foreach ($pair in @(@('type','exception_type'),@('inner_exception_type','exception_type'),@('child_exit_code','exit_code'),@('failure_code','reason_code'))) {
                if ($fields.Contains($pair[0])) {$candidate[$pair[1]]=$fields[$pair[0]]}
            }
            try {
                $reference=Write-KmtechProtectedFailureSidecar -Text ($Value | ConvertTo-Json -Depth 30)
                $candidate.report_path=[string]$reference.path
                $candidate.report_sha256=[string]$reference.sha256
            } catch {$candidate.report_status='REPORT_UNAVAILABLE'}
            return ConvertTo-KmtechPublicRecord $candidate
        }
        $result=[ordered]@{}
        foreach ($key in $fields.Keys) {
            $item=$fields[$key]
            if ($key -cin @('failure','error','error_message','reason') -and $item -is [string] -and $item) {
                if ($Script:KmtechRedactionRules.allowed_codes -ccontains $item) {$result[$key]=$item}
                else {$result[$key]=ConvertTo-KmtechPublicDiagnosticTree ([ordered]@{message=$item})}
            } else {$result[$key]=ConvertTo-KmtechPublicDiagnosticTree $item}
        }
        return $result
    }
    if ($Value -is [Collections.IEnumerable] -and $Value -isnot [string]) {
        $rows=@(foreach ($item in $Value) {ConvertTo-KmtechPublicDiagnosticTree $item})
        return ,$rows
    }
    return $Value
}
