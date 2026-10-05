$Script:KmtechRedactionRules = Get-Content -LiteralPath (Join-Path $PSScriptRoot 'failure_redaction_rules.json') -Raw -Encoding UTF8 | ConvertFrom-Json

function Get-KmtechSinkEnvironment {
    $values = @{}
    # Internal product lookup only: names and values never leave this function.
    $process = [Environment]::GetEnvironmentVariables('Process')
    foreach ($name in $process.Keys) {
        if (([string]$name -match $Script:KmtechRedactionRules.pattern -or [string]$name -cin $Script:KmtechRedactionRules.explicit) -and [string]$name -notmatch $Script:KmtechRedactionRules.path_suffix) {
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
        ([string]$_ -match $rules.pattern -or [string]$_ -cin $rules.explicit) -and [string]$_ -notmatch $rules.path_suffix
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
    $Text = [regex]::Replace($Text, '(?<key>"(?:\\.|[^"\\])*"|[A-Za-z_][\w.-]*)(?<separator>\s*[:=]\s*)(?<value>"(?:\\.|[^"\\])*"|[^\s,;}]+)', [Text.RegularExpressions.MatchEvaluator]{
        param($match)
        $key=$match.Groups['key'].Value.Trim('"');$value=$match.Groups['value'].Value
        $metadata=($value -cin @('true','false','null') -or $key -match $rules.path_suffix -or
            ($key -match $rules.digest_suffix -and $key -notmatch $rules.short_secret_digest))
        if ($metadata -or ($key -notmatch $rules.pattern -and $key -inotmatch '^(pin|otp|password)$') -or $value.Trim('"').StartsWith('[REDACTED:')) { return $match.Value }
        $replacement=if($value.StartsWith('"')){'"'+$rules.field+'"'}else{$rules.field}
        return $match.Groups['key'].Value+$match.Groups['separator'].Value+$replacement
    })
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

function ConvertTo-KmtechRedactedSinkObject {
    param([AllowNull()]$Value, [Collections.IDictionary]$Environment = $null)
    if ($null -eq $Value) { return $null }
    if ($Value -is [string]) { return ConvertTo-KmtechRedactedSinkText -Text $Value -Environment $Environment }
    if ($Value -is [Collections.IDictionary] -or $Value -is [pscustomobject]) {
        $result = [ordered]@{}
        $names = if ($Value -is [Collections.IDictionary]) { @($Value.Keys) } else { @($Value.PSObject.Properties.Name) }
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
