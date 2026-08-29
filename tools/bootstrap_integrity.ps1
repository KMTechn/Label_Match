$BootstrapIntegrityFileName = "bootstrap-integrity.json"
$BootstrapIntegritySchema = "label-match-bootstrap-integrity-v1"
$BootstrapPortableCodeRoot = "."

function Get-BootstrapStrictFullPath {
    param(
        [Parameter(Mandatory = $true)][string]$Path,
        [Parameter(Mandatory = $true)][string]$Purpose
    )
    if ([string]::IsNullOrWhiteSpace($Path) -or -not [IO.Path]::IsPathRooted($Path)) {
        throw "$Purpose must be an absolute path."
    }
    if ($Path.StartsWith('\\?\') -or $Path.StartsWith('\\.\')) {
        throw "$Purpose must not use a device path."
    }
    return [IO.Path]::GetFullPath($Path).TrimEnd([char[]]"\/")
}

function Get-BootstrapFileSha256 {
    param([Parameter(Mandatory = $true)][string]$Path)
    $stream = [IO.File]::OpenRead($Path)
    $sha = [Security.Cryptography.SHA256]::Create()
    try {
        return ([BitConverter]::ToString($sha.ComputeHash($stream))).Replace('-', '').ToLowerInvariant()
    }
    finally {
        $sha.Dispose()
        $stream.Dispose()
    }
}

function Get-BootstrapRelativeCodePath {
    param(
        [Parameter(Mandatory = $true)][string]$Root,
        [Parameter(Mandatory = $true)][string]$Path
    )
    $rootFull = (Get-BootstrapStrictFullPath $Root "inventory root") + '\'
    $pathFull = [IO.Path]::GetFullPath($Path)
    if (-not $pathFull.StartsWith($rootFull, [StringComparison]::OrdinalIgnoreCase)) {
        throw "Inventory path escaped its root."
    }
    return $pathFull.Substring($rootFull.Length).Replace('\', '/')
}

function Get-BootstrapCodeInventory {
    param(
        [Parameter(Mandatory = $true)][string]$Root,
        [string]$IntegrityFileName = $BootstrapIntegrityFileName
    )
    $rootFull = Get-BootstrapStrictFullPath $Root "code root"
    $result = @()
    foreach ($file in @(Get-ChildItem -LiteralPath $rootFull -File -Force -Recurse | Sort-Object FullName)) {
        $relative = Get-BootstrapRelativeCodePath $rootFull $file.FullName
        if ($relative.Equals($IntegrityFileName, [StringComparison]::OrdinalIgnoreCase)) {
            continue
        }
        $result += [pscustomobject][ordered]@{
            path = $relative
            size = [int64]$file.Length
            sha256 = Get-BootstrapFileSha256 $file.FullName
        }
    }
    return $result
}

function Get-BootstrapInventoryAggregate {
    param([Parameter(Mandatory = $true)][object[]]$Inventory)
    $lines = @($Inventory | ForEach-Object { "$($_.sha256) $($_.size) $($_.path)" })
    $bytes = (New-Object Text.UTF8Encoding($false)).GetBytes(($lines -join "`n") + "`n")
    $sha = [Security.Cryptography.SHA256]::Create()
    try {
        return ([BitConverter]::ToString($sha.ComputeHash($bytes))).Replace('-', '').ToLowerInvariant()
    }
    finally {
        $sha.Dispose()
    }
}

function Write-BootstrapUtf8Json {
    param(
        [Parameter(Mandatory = $true)][string]$Path,
        [Parameter(Mandatory = $true)]$Payload
    )
    $temporary = "$Path.tmp.$PID"
    $json = $Payload | ConvertTo-Json -Depth 8
    [IO.File]::WriteAllText($temporary, $json + [Environment]::NewLine, (New-Object Text.UTF8Encoding($false)))
    Move-Item -LiteralPath $temporary -Destination $Path -Force | Out-Null
}

function Write-BootstrapIntegrityRecord {
    param(
        [Parameter(Mandatory = $true)][string]$Root,
        [Parameter(Mandatory = $true)][string]$CodeRoot,
        [object[]]$Inventory = @(),
        [string]$InstalledAt = "",
        [string]$IntegrityFileName = $BootstrapIntegrityFileName,
        [string]$IntegritySchema = $BootstrapIntegritySchema
    )
    $rootFull = Get-BootstrapStrictFullPath $Root "code root"
    if ($CodeRoot -cne $BootstrapPortableCodeRoot -and -not [IO.Path]::IsPathRooted($CodeRoot)) {
        throw "bootstrap integrity code root must be absolute or the portable package marker."
    }
    $selectedInventory = @($Inventory)
    if ($selectedInventory.Count -eq 0) {
        $selectedInventory = @(Get-BootstrapCodeInventory -Root $rootFull -IntegrityFileName $IntegrityFileName)
    }
    if ($selectedInventory.Count -eq 0) {
        throw "Frozen release code inventory is empty."
    }
    $aggregate = Get-BootstrapInventoryAggregate -Inventory $selectedInventory
    $record = [ordered]@{
        schema_version = $IntegritySchema
        status = 'PASS'
        code_root = $CodeRoot
        installed_at = if ([string]::IsNullOrWhiteSpace($InstalledAt)) {
            (Get-Date).ToUniversalTime().ToString('o')
        } else {
            $InstalledAt
        }
        file_count = $selectedInventory.Count
        aggregate_sha256 = $aggregate
        files = $selectedInventory
        identity_profile_created = $false
        state_scope = 'current_user_first_run'
    }
    Write-BootstrapUtf8Json -Path (Join-Path $rootFull $IntegrityFileName) -Payload $record
    return [pscustomobject]$record
}

function Assert-BootstrapIntegrityRecord {
    param(
        [Parameter(Mandatory = $true)][string]$Root,
        [string]$IntegrityFileName = $BootstrapIntegrityFileName,
        [string]$IntegritySchema = $BootstrapIntegritySchema
    )
    $rootFull = Get-BootstrapStrictFullPath $Root "bootstrap integrity root"
    $recordPath = Join-Path $rootFull $IntegrityFileName
    if (-not (Test-Path -LiteralPath $recordPath -PathType Leaf)) {
        throw "Bootstrap integrity record is absent."
    }
    $record = Get-Content -LiteralPath $recordPath -Raw -Encoding UTF8 | ConvertFrom-Json
    if (
        [string]$record.schema_version -cne $IntegritySchema -or
        [string]$record.status -cne 'PASS'
    ) {
        throw "Bootstrap integrity record schema or status is invalid."
    }
    if ([string]$record.code_root -ceq $BootstrapPortableCodeRoot) {
        $declaredCodeRoot = $rootFull
    }
    else {
        $declaredCodeRoot = Get-BootstrapStrictFullPath `
            ([string]$record.code_root) `
            "bootstrap integrity code root"
    }
    if (-not $declaredCodeRoot.Equals($rootFull, [StringComparison]::OrdinalIgnoreCase)) {
        throw "Bootstrap integrity record code root is invalid."
    }
    $inventory = @(
        Get-BootstrapCodeInventory -Root $rootFull -IntegrityFileName $IntegrityFileName
    )
    if (
        [int]$record.file_count -ne $inventory.Count -or
        @($record.files).Count -ne $inventory.Count
    ) {
        throw "Bootstrap integrity record file count is invalid."
    }
    $actualByPath = @{}
    foreach ($actual in $inventory) {
        $actualPath = [string]$actual.path
        if ($actualByPath.ContainsKey($actualPath)) {
            throw "Bootstrap integrity actual inventory contains duplicate paths."
        }
        $actualByPath[$actualPath] = $actual
    }
    for ($index = 0; $index -lt @($record.files).Count; $index += 1) {
        $expected = @($record.files)[$index]
        $expectedPath = [string]$expected.path
        if (-not $actualByPath.ContainsKey($expectedPath)) {
            throw "Bootstrap integrity inventory is missing the recorded path at index $index."
        }
        $actual = $actualByPath[$expectedPath]
        if (
            [string]$expected.path -cne [string]$actual.path -or
            [int64]$expected.size -ne [int64]$actual.size -or
            [string]$expected.sha256 -cne [string]$actual.sha256
        ) {
            throw "Bootstrap integrity inventory differs at index $index."
        }
        $actualByPath.Remove($expectedPath)
    }
    if ($actualByPath.Count -ne 0) {
        throw "Bootstrap integrity actual inventory contains unrecorded paths."
    }
    $aggregate = Get-BootstrapInventoryAggregate -Inventory @($record.files)
    if ([string]$record.aggregate_sha256 -cne $aggregate) {
        throw "Bootstrap integrity aggregate is invalid."
    }
    $frozenMainCount = @(
        $inventory | Where-Object { [string]$_.path -ieq 'Label_Match.exe' }
    ).Count
    $portablePythonCount = @(
        $inventory | Where-Object { [string]$_.path -ieq 'runtime/pythonw.exe' }
    ).Count
    $portableMainCount = @(
        $inventory | Where-Object { [string]$_.path -ieq 'app/main.py' }
    ).Count
    $frozenLayout = $frozenMainCount -eq 1
    $portableLayout = $portablePythonCount -eq 1 -and $portableMainCount -eq 1
    if ($frozenLayout -eq $portableLayout) {
        throw "Bootstrap integrity record does not identify exactly one supported release layout."
    }
    return [pscustomobject]@{
        status = 'PASS'
        record_path = $recordPath
        file_count = $inventory.Count
        aggregate_sha256 = $aggregate
    }
}
