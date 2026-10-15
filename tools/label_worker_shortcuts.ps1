[CmdletBinding()]
param(
    [string]$InstallRoot = '',
    [string]$RecordPath = '',
    [switch]$Install,
    [switch]$Rollback,
    [switch]$Uninstall,
    [switch]$CommonOnly,
    [switch]$UserOnly
)

$ErrorActionPreference = 'Stop'

function Invoke-LabelPinnedAtomicReplace {
    param([string]$Source,[string]$Destination,[AllowNull()]$Backup = $null,
          [ValidateSet('Move','Replace','WriteThrough')][string]$Mode = 'Move')
    # The supported generator binds this copy to the one canonical function.
    # Inline bytes also work in the attested in-memory installer scope.
    $expectedAtomicBodySha256 = 'c13d9869332a9f14b763f1ffa8345453ab64bdc75d1c7605627194346a77791a'
# LABEL_ATOMIC_INLINE_START
$text = 'function Invoke-LabelAtomicReplace {
    param([string]$Source,[string]$Destination,[AllowNull()]$Backup = $null,
          [ValidateSet(''Move'',''Replace'',''WriteThrough'')][string]$Mode = ''Move'')
    $timer = [Diagnostics.Stopwatch]::StartNew()
    if ($Mode -ceq ''WriteThrough'' -and -not (''KMTechStartupUpdateAtomicFile'' -as [type])) {
        Add-Type -TypeDefinition @''
using System.Runtime.InteropServices;
public static class KMTechStartupUpdateAtomicFile {
 [DllImport("kernel32.dll", CharSet=CharSet.Unicode, SetLastError=true)]
 public static extern bool MoveFileExW(string source, string target, uint flags);
}
''@
    }
    if ($timer.Elapsed.TotalMilliseconds -ge 1000) { throw [TimeoutException]::new(''Atomic publication preparation exceeded its deadline.'') }
    $firstError = $null
    while ($true) {
        if ($null -ne $firstError -and $timer.Elapsed.TotalMilliseconds -ge 1000) { throw $firstError }
        try {
            if ($Mode -ceq ''WriteThrough'') {
                if (-not [KMTechStartupUpdateAtomicFile]::MoveFileExW($Source,$Destination,9)) {
                    throw [ComponentModel.Win32Exception]::new([Runtime.InteropServices.Marshal]::GetLastWin32Error())
                }
            } elseif ($Mode -ceq ''Replace'') {
                $replaceBackup = if ($null -eq $Backup) { [NullString]::Value } else { $Backup }
                [IO.File]::Replace($Source,$Destination,$replaceBackup)
            } else {
                [IO.File]::Move($Source,$Destination)
            }
            return
        } catch {
            $cause = $_.Exception
            while ($null -ne $cause.InnerException) { $cause = $cause.InnerException }
            $code = if ($cause -is [ComponentModel.Win32Exception]) { $cause.NativeErrorCode }
                    elseif ($cause -is [IO.IOException] -and ($cause.HResult -band 0xffff0000) -eq -2147024896) { $cause.HResult -band 0xffff }
                    elseif ($cause -is [UnauthorizedAccessException] -and $cause.HResult -eq -2147024891) { 5 }
                    else { 0 }
            if ($code -notin @(5,32)) { throw }
            if ($null -eq $firstError) { $firstError = $_ }
            $remaining = 1000 - $timer.Elapsed.TotalMilliseconds
            if ($remaining -le 0) { throw $firstError }
            Start-Sleep -Milliseconds ([Math]::Min(50,[Math]::Ceiling($remaining)))
        }
    }
}'
# LABEL_ATOMIC_INLINE_END
    $algorithm = [Security.Cryptography.SHA256]::Create()
    try { $actual = ([BitConverter]::ToString($algorithm.ComputeHash([Text.Encoding]::UTF8.GetBytes($text)))).Replace('-','').ToLowerInvariant() }
    finally { $algorithm.Dispose() }
    if ($actual -cne $expectedAtomicBodySha256) { throw 'Atomic replacement source pin differs.' }
    . ([scriptblock]::Create($text))
    Invoke-LabelAtomicReplace @PSBoundParameters
}

function Get-LabelWorkerShortcutFolders([switch]$CommonOnly, [switch]$UserOnly) {
    foreach ($name in @('Desktop', 'Programs', 'CommonDesktopDirectory', 'CommonPrograms')) {
        $common = $name.StartsWith('Common')
        if (($CommonOnly -and -not $common) -or ($UserOnly -and $common)) { continue }
        $folder = [Environment+SpecialFolder]::$name
        $path = [Environment]::GetFolderPath($folder)
        if ([string]::IsNullOrWhiteSpace($path)) { throw 'Worker shortcut folder is unavailable.' }
        [pscustomobject]@{name=$name;path=$path;recursive=$name.EndsWith('Programs')}
    }
}

function Assert-LabelShortcutPath([string]$Path) {
    if (-not [IO.Path]::IsPathRooted($Path)) { throw 'Shortcut path must be absolute.' }
    $cursor = [IO.Path]::GetFullPath($Path)
    while ($cursor) {
        if (Test-Path -LiteralPath $cursor) {
            if (((Get-Item -LiteralPath $cursor -Force).Attributes -band [IO.FileAttributes]::ReparsePoint) -ne 0) {
                throw 'Shortcut path contains a reparse point.'
            }
        }
        $cursor = [IO.Path]::GetDirectoryName($cursor)
    }
}

function Get-LabelShortcutSha256([string]$Path) {
    $stream = [IO.File]::OpenRead($Path)
    $hash = [Security.Cryptography.SHA256]::Create()
    try { return ([BitConverter]::ToString($hash.ComputeHash($stream))).Replace('-', '').ToLowerInvariant() }
    finally { $hash.Dispose(); $stream.Dispose() }
}

function Get-LabelShortcutProperties([string]$Path) {
    Assert-LabelShortcutPath $Path
    if (-not (Test-Path -LiteralPath $Path -PathType Leaf)) { return $null }
    $shell = New-Object -ComObject WScript.Shell
    $link = $shell.CreateShortcut($Path)
    [ordered]@{target_path=[string]$link.TargetPath;arguments=[string]$link.Arguments;
        working_directory=[string]$link.WorkingDirectory;icon_location=[string]$link.IconLocation;
        window_style=[int]$link.WindowStyle;description=[string]$link.Description;hotkey=[string]$link.Hotkey}
}

function Set-LabelShortcutProperties([string]$Path, $Properties) {
    Assert-LabelShortcutPath $Path
    $existed = Test-Path -LiteralPath $Path -PathType Leaf
    [void][IO.Directory]::CreateDirectory((Split-Path -Parent $Path))
    $temporary = $Path + '.tmp.' + [guid]::NewGuid().ToString('N') + '.lnk'
    try {
    if ($existed) { Copy-LabelAtomicFile $Path $temporary }
    $shell = New-Object -ComObject WScript.Shell
    $link = $shell.CreateShortcut($temporary)
    $link.TargetPath = [string]$Properties.target_path
    $link.Arguments = [string]$Properties.arguments
    $link.WorkingDirectory = [string]$Properties.working_directory
    $link.IconLocation = [string]$Properties.icon_location
    $link.WindowStyle = [int]$Properties.window_style
    if (-not $existed) {
        $link.Description = [string]$Properties.description
        $link.Hotkey = [string]$Properties.hotkey
    }
    $link.Save()
    if (-not (Test-LabelShortcutProperties (Get-LabelShortcutProperties $temporary) $Properties)) {
        throw 'Prepared worker shortcut readback differs.'
    }
    Copy-LabelAtomicFile $temporary $Path
    } finally {
        if (Test-Path -LiteralPath $temporary) { Remove-Item -LiteralPath $temporary }
    }
}

function Copy-LabelAtomicStream($SourceStream, $DestinationStream) {
    $SourceStream.CopyTo($DestinationStream)
    $DestinationStream.Flush($true)
}

function Copy-LabelAtomicFile([string]$Source, [string]$Destination) {
    Assert-LabelShortcutPath $Source
    Assert-LabelShortcutPath $Destination
    $temporary = $Destination + '.tmp.' + [guid]::NewGuid().ToString('N')
    $inputStream = [IO.File]::Open($Source,'Open','Read','Read')
    $digest = [Security.Cryptography.SHA256]::Create()
    try {
        $expected = ([BitConverter]::ToString($digest.ComputeHash($inputStream))).Replace('-','').ToLowerInvariant()
        $inputStream.Position = 0
        $outputStream = [IO.File]::Open($temporary,'CreateNew','Write','None')
        try { Copy-LabelAtomicStream $inputStream $outputStream } finally { $outputStream.Dispose() }
    } finally { $digest.Dispose(); $inputStream.Dispose() }
    # The live destination is untouched until the complete copy is verified.
    if ((Get-LabelShortcutSha256 $temporary) -cne $expected) { throw 'Atomic copy preparation differs.' }
    Assert-LabelShortcutPath $Destination
    if ([IO.File]::Exists($Destination)) {
        Invoke-LabelPinnedAtomicReplace -Source $temporary -Destination $Destination -Mode Replace
    } else { Invoke-LabelPinnedAtomicReplace -Source $temporary -Destination $Destination -Mode Move }
    if ((Get-LabelShortcutSha256 $Destination) -cne $expected) { throw 'Atomic copy readback differs.' }
}

function Save-LabelShortcutRecord([string]$Path, $Record) {
    Assert-LabelShortcutPath $Path
    [void][IO.Directory]::CreateDirectory((Split-Path -Parent $Path))
    $temporary = $Path + '.tmp.' + [Guid]::NewGuid().ToString('N')
    $bytes = [Text.Encoding]::UTF8.GetBytes(($Record | ConvertTo-Json -Depth 12))
    $stream = [IO.File]::Open($temporary, 'CreateNew', 'Write', 'None')
    try { $stream.Write($bytes, 0, $bytes.Length); $stream.Flush($true) } finally { $stream.Dispose() }
    if (Test-Path -LiteralPath $Path) { Invoke-LabelPinnedAtomicReplace -Source $temporary -Destination $Path -Backup ([NullString]::Value) -Mode Replace }
    else { Invoke-LabelPinnedAtomicReplace -Source $temporary -Destination $Path -Mode Move }
}

function Get-LabelWorkerShortcutFiles($Folders) {
    $seen = @{}
    foreach ($folder in $Folders) {
        Assert-LabelShortcutPath $folder.path
        $pending = [Collections.Generic.Stack[string]]::new()
        if (Test-Path -LiteralPath $folder.path -PathType Container) { $pending.Push($folder.path) }
        while ($pending.Count -gt 0) {
            $directory = $pending.Pop()
            foreach ($item in Get-ChildItem -LiteralPath $directory -Force) {
                if (($item.Attributes -band [IO.FileAttributes]::ReparsePoint) -ne 0) { continue }
                if ($item.PSIsContainer) {
                    if ($folder.recursive) { $pending.Push($item.FullName) }
                } elseif ($item.Extension -ieq '.lnk' -and -not $seen.ContainsKey($item.FullName)) {
                    $seen[$item.FullName] = $true
                    $item.FullName
                }
            }
        }
    }
}

function Test-LabelShortcutProperties($Actual, $Expected) {
    if ($null -eq $Actual) { return $false }
    foreach ($name in @('target_path','arguments','working_directory','icon_location','window_style')) {
        if ([string]$Actual.$name -cne [string]$Expected.$name) { return $false }
    }
    return $true
}

function Get-LabelWorkerShortcutPublished($Record) {
    $folders = @(Get-LabelWorkerShortcutFolders -CommonOnly:([bool]$Record.common_only) -UserOnly:([bool]$Record.user_only))
    $paths = @($Record.entries | ForEach-Object { $_.path })
    $preserved = @()
    if (-not $Record.user_only) {
        foreach ($folder in @($folders | Where-Object {$_.name.StartsWith('Common')})) {
            $relative = if ($folder.recursive) { 'KMTech/포장.lnk' } else { '포장.lnk' }
            $path = Join-Path $folder.path $relative
            if ((Test-Path -LiteralPath $path -PathType Leaf) -and $path -notin $paths) {
                $actual = Get-LabelShortcutProperties $path
                if (Test-LabelShortcutProperties $actual $Record.expected) { $paths += $path }
                else {
                    $preserved += [ordered]@{location=$(if ($folder.recursive) {'start_menu'} else {'desktop'});
                        shortcut_path=[string]$path;reason_code='UNRELATED_SHORTCUT_PRESERVED'}
                }
            }
        }
    }
    if ($Record -is [Collections.IDictionary] -or $null -ne $Record.PSObject.Properties['preserved']) { $Record.preserved = $preserved }
    else { $Record | Add-Member -NotePropertyName preserved -NotePropertyValue $preserved -Force }
    foreach ($path in @($paths | Select-Object -Unique)) {
        $folder = @($folders | Where-Object {$path.StartsWith($_.path.TrimEnd('\')+'\', [StringComparison]::OrdinalIgnoreCase)})
        if ($folder.Count -ne 1) { throw 'Published shortcut escaped its known folder.' }
        $actual = Get-LabelShortcutProperties $path
        if (-not (Test-LabelShortcutProperties $actual $Record.expected)) { throw 'Published shortcut readback differs.' }
        [ordered]@{location=$(if ($folder[0].recursive) {'start_menu'} else {'desktop'});
            shortcut_path=[string]$path;target_path=[string]$actual.target_path;
            arguments=[string]$actual.arguments;working_directory=[string]$actual.working_directory}
    }
}

function Install-LabelWorkerShortcuts([string]$Root, $Manifest, [string]$Path, [switch]$CommonOnly, [switch]$UserOnly) {
    if ($CommonOnly -and $UserOnly) { throw 'Shortcut scope is ambiguous.' }
    Assert-LabelShortcutPath $Root
    Assert-LabelShortcutPath $Path
    $Path = [IO.Path]::GetFullPath($Path)
    $rootFull = [IO.Path]::GetFullPath($Root).TrimEnd('\')
    if ([string]$Manifest.launcher -cne 'launch-label-match.cmd') { throw 'Worker launcher differs from the portable manifest.' }
    $launcher = Join-Path $rootFull ([string]$Manifest.launcher)
    $pythonw = Join-Path $rootFull 'runtime/pythonw.exe'
    $entrypoint = Join-Path $rootFull 'app/main.py'
    if ($entrypoint.Contains('"')) { throw 'Worker entrypoint path contains a quote.' }
    $icon = Join-Path $rootFull 'app/assets/logo.ico'
    foreach ($file in @($pythonw, $entrypoint, $icon)) {
        Assert-LabelShortcutPath $file
        if (-not (Test-Path -LiteralPath $file -PathType Leaf)) { throw 'Installed worker launcher/icon is absent.' }
    }
    $legacy = [ordered]@{target_path=$launcher;arguments='';working_directory=$rootFull;
        icon_location=($icon+',0');window_style=7;description='포장';hotkey=''}
    $expected = [ordered]@{target_path=$pythonw;arguments=('-I -B "'+$entrypoint+'"');working_directory=$rootFull;
        icon_location=($icon+',0');window_style=1;description='포장';hotkey=''}
    $record = $null; $previousExpected = $null; $targets = @()
    if (Test-Path -LiteralPath $Path) {
        $saved = Get-Content -LiteralPath $Path -Raw -Encoding UTF8 | ConvertFrom-Json
        if ($saved.schema -cne 'label-match-worker-shortcuts-v1' -or $saved.install_root -ine $rootFull -or
            $saved.status -cne 'INSTALLED' -or [bool]$saved.common_only -ne [bool]$CommonOnly -or
            [bool]$saved.user_only -ne [bool]$UserOnly) { throw 'Shortcut record cannot be replaced.' }
        $wasLegacy = Test-LabelShortcutProperties $saved.expected $legacy
        if (-not $wasLegacy -and -not (Test-LabelShortcutProperties $saved.expected $expected)) {
            throw 'Installed worker shortcut definition differs.'
        }
        foreach ($entry in @($saved.entries)) {
            if (-not (Test-LabelShortcutProperties (Get-LabelShortcutProperties $entry.path) $saved.expected) -or
                (Get-LabelShortcutSha256 $entry.path) -cne $entry.installed_sha256) {
                throw 'Installed worker shortcut drifted.'
            }
            if ($wasLegacy -and $null -ne $entry.original) {
                Assert-LabelShortcutPath $entry.backup_path
                if ([IO.Path]::GetFullPath($entry.backup_path) -ine $entry.backup_path -or
                    -not ([string]$entry.original.target_path).StartsWith($rootFull+'\', [StringComparison]::OrdinalIgnoreCase) -or
                    -not $entry.backup_path.StartsWith($Path+'.preimages\', [StringComparison]::OrdinalIgnoreCase) -or
                    (Get-LabelShortcutSha256 $entry.backup_path) -cne $entry.original_sha256) {
                    throw 'Shortcut preimage is unproven.'
                }
            }
        }
        $published = @(Get-LabelWorkerShortcutPublished $saved)
        if ($null -ne $saved.PSObject.Properties['published']) { $saved.published = $published }
        else { $saved | Add-Member -NotePropertyName published -NotePropertyValue $published }
        if ($wasLegacy) {
            # Keep the original installation preimages while upgrading our old wrapper links.
            $previousExpected = $legacy; $record = $saved
            $record.expected = $expected; $record.status = 'PREPARED'
            foreach ($published in @($saved.published)) {
                if ($published.shortcut_path -notin @($saved.entries | ForEach-Object {$_.path})) {
                    $targets += $published.shortcut_path
                }
            }
        } else {
        Save-LabelShortcutRecord $Path $saved
        return [ordered]@{status='PASS';record_path=$Path;count=@($saved.entries).Count;published=@($saved.published)}
        }
    }
    if ($null -eq $record) {
    $folders = @(Get-LabelWorkerShortcutFolders -CommonOnly:$CommonOnly -UserOnly:$UserOnly)
    foreach ($file in @(Get-LabelWorkerShortcutFiles $folders)) {
        $before = Get-LabelShortcutProperties $file
        $target = [string]$before.target_path
        if ([IO.Path]::IsPathRooted($target) -and
            [IO.Path]::GetFullPath($target).StartsWith($rootFull+'\', [StringComparison]::OrdinalIgnoreCase) -and
            (-not (Test-Path -LiteralPath $target) -or (Test-LabelShortcutProperties $before $legacy))) { $targets += $file }
    }
    # Public links ensure a visible entry even when only a private legacy link exists.
    if (-not $UserOnly) {
        foreach ($folder in @($folders | Where-Object {$_.name.StartsWith('Common')})) {
            $hasPublic = @($targets | Where-Object {$_.StartsWith($folder.path.TrimEnd('\')+'\', [StringComparison]::OrdinalIgnoreCase)}).Count -gt 0
            if ($hasPublic) { continue }
            $relative = if ($folder.recursive) { 'KMTech/포장.lnk' } else { '포장.lnk' }
            $file = Join-Path $folder.path $relative
            $existing = Get-LabelShortcutProperties $file
            if ($null -ne $existing) {
                # Existing unrelated links belong to their original application.
                # The publication readback records preservation without adopting them.
                continue
            }
            $targets += $file
        }
    }
    $record = [ordered]@{schema='label-match-worker-shortcuts-v1';install_root=$rootFull;
        common_only=[bool]$CommonOnly;user_only=[bool]$UserOnly;status='PREPARED';expected=$expected;entries=@()}
    }
    foreach ($file in @($targets | Select-Object -Unique)) {
        $before = Get-LabelShortcutProperties $file
        $backup = ''; $beforeSha = ''
        if ($null -ne $before) {
            $backup = Join-Path ($Path+'.preimages') ($record.entries.Count.ToString()+'.lnk')
            Assert-LabelShortcutPath $backup
            if (Test-Path -LiteralPath $backup) { throw 'Shortcut preimage path is already occupied.' }
            [void][IO.Directory]::CreateDirectory((Split-Path -Parent $backup))
            Copy-Item -LiteralPath $file -Destination $backup
            $beforeSha = (Get-LabelShortcutSha256 $file)
            if ((Get-LabelShortcutSha256 $backup) -cne $beforeSha) { throw 'Shortcut preimage differs.' }
        }
        $record.entries += [ordered]@{path=$file;original=$before;original_sha256=$beforeSha;
            backup_path=$backup;installed_sha256='';restored=$false}
    }
    Save-LabelShortcutRecord $Path $record
    try {
        foreach ($entry in $record.entries) {
            $current = Get-LabelShortcutProperties $entry.path
            if ($null -ne $previousExpected) {
                $previousSha = if ($entry.installed_sha256) {$entry.installed_sha256} else {$entry.original_sha256}
                if (-not (Test-LabelShortcutProperties $current $previousExpected) -or
                    (Get-LabelShortcutSha256 $entry.path) -cne $previousSha) { throw 'Worker shortcut changed after capture.' }
            } elseif ($null -ne $entry.original -and (
                -not (Test-LabelShortcutProperties $current $entry.original) -or
                (Get-LabelShortcutSha256 $entry.path) -cne $entry.original_sha256 -or
                ((Test-Path -LiteralPath ([string]$entry.original.target_path)) -and
                    -not (Test-LabelShortcutProperties $entry.original $legacy)))) {
                throw 'Worker shortcut changed after capture.'
            }
            if ($null -eq $previousExpected -and $null -eq $entry.original -and $null -ne $current) { throw 'Worker shortcut appeared after capture.' }
            Set-LabelShortcutProperties $entry.path $expected
            if (-not (Test-LabelShortcutProperties (Get-LabelShortcutProperties $entry.path) $expected)) { throw 'Worker shortcut readback differs.' }
            $entry.installed_sha256 = (Get-LabelShortcutSha256 $entry.path)
            Save-LabelShortcutRecord $Path $record
        }
        $record.status = 'INSTALLED'
        $record.published = @(Get-LabelWorkerShortcutPublished $record)
        Save-LabelShortcutRecord $Path $record
    } catch {
        $failure = $_
        try { Restore-LabelWorkerShortcuts $Root $Path -CommonOnly:$CommonOnly -UserOnly:$UserOnly }
        catch { $failure.Exception.Data['shortcut_rollback_error_type'] = $_.Exception.GetType().FullName }
        throw $failure
    }
    [ordered]@{status='PASS';record_path=$Path;count=$record.entries.Count;published=@($record.published)}
}

function Restore-LabelWorkerShortcuts([string]$Root, [string]$Path, [switch]$CommonOnly, [switch]$UserOnly) {
    Assert-LabelShortcutPath $Path
    $Path = [IO.Path]::GetFullPath($Path)
    if (-not (Test-Path -LiteralPath $Path -PathType Leaf)) { return }
    $record = Get-Content -LiteralPath $Path -Raw -Encoding UTF8 | ConvertFrom-Json
    $rootFull = [IO.Path]::GetFullPath($Root).TrimEnd('\')
    if ($CommonOnly -and $UserOnly) { throw 'Shortcut scope is ambiguous.' }
    $legacy = [ordered]@{target_path=(Join-Path $rootFull 'launch-label-match.cmd');arguments='';working_directory=$rootFull;
        icon_location=((Join-Path $rootFull 'app/assets/logo.ico')+',0');window_style=7;description='포장';hotkey=''}
    $expected = [ordered]@{target_path=(Join-Path $rootFull 'runtime/pythonw.exe');arguments=('-I -B "'+(Join-Path $rootFull 'app/main.py')+'"');working_directory=$rootFull;
        icon_location=((Join-Path $rootFull 'app/assets/logo.ico')+',0');window_style=1;description='포장';hotkey=''}
    if ($record.schema -cne 'label-match-worker-shortcuts-v1' -or $record.install_root -ine $rootFull -or
        [bool]$record.common_only -ne [bool]$CommonOnly -or [bool]$record.user_only -ne [bool]$UserOnly -or
        (-not (Test-LabelShortcutProperties $record.expected $expected) -and -not (Test-LabelShortcutProperties $record.expected $legacy)) -or
        $record.status -cnotin @('PREPARED','INSTALLED','RESTORED')) { throw 'Worker shortcut restoration record differs.' }
    $folders = @(Get-LabelWorkerShortcutFolders -CommonOnly:([bool]$record.common_only) -UserOnly:([bool]$record.user_only))
    # Admit every link/preimage before any restoration; unrelated drift is preserved.
    foreach ($entry in @($record.entries)) {
        Assert-LabelShortcutPath $entry.path
        if ([IO.Path]::GetFullPath($entry.path) -ine $entry.path -or
            [IO.Path]::GetExtension($entry.path) -ine '.lnk' -or @($folders | Where-Object {
            $entry.path.StartsWith($_.path.TrimEnd('\')+'\', [StringComparison]::OrdinalIgnoreCase)
        }).Count -eq 0) { throw 'Recorded shortcut escaped its known folder.' }
        if ($null -ne $entry.original) {
            Assert-LabelShortcutPath $entry.backup_path
            if ([IO.Path]::GetFullPath($entry.backup_path) -ine $entry.backup_path -or
                -not ([string]$entry.original.target_path).StartsWith($rootFull+'\', [StringComparison]::OrdinalIgnoreCase) -or
                -not $entry.backup_path.StartsWith($Path+'.preimages\', [StringComparison]::OrdinalIgnoreCase) -or
                (Get-LabelShortcutSha256 $entry.backup_path) -cne $entry.original_sha256) { throw 'Shortcut preimage is unproven.' }
        }
        $current = Get-LabelShortcutProperties $entry.path
        $isOriginal = $null -eq $current -and $null -eq $entry.original
        if ($null -ne $current -and $null -ne $entry.original) {
            $isOriginal = (Get-LabelShortcutSha256 $entry.path) -ceq $entry.original_sha256
        }
        $pendingLegacy = $record.status -ceq 'PREPARED' -and $entry.installed_sha256 -and
            (Test-LabelShortcutProperties $record.expected $expected) -and (Test-LabelShortcutProperties $current $legacy)
        if (-not $isOriginal -and ((-not (Test-LabelShortcutProperties $current $record.expected) -and -not $pendingLegacy) -or
            ($entry.installed_sha256 -and (Get-LabelShortcutSha256 $entry.path) -cne $entry.installed_sha256))) {
            throw 'Worker shortcut drifted; restoration refused.'
        }
    }
    foreach ($entry in @($record.entries)) {
        if ($null -ne $entry.original) {
            Copy-LabelAtomicFile $entry.backup_path $entry.path
            if ((Get-LabelShortcutSha256 $entry.path) -cne $entry.original_sha256) { throw 'Worker shortcut preimage readback differs.' }
        } elseif (Test-Path -LiteralPath $entry.path) {
            Remove-Item -LiteralPath $entry.path
            if (Test-Path -LiteralPath $entry.path) { throw 'Created worker shortcut remains.' }
        }
        $entry.restored = $true
        Save-LabelShortcutRecord $Path $record
    }
    $record.status = 'RESTORED'
    Save-LabelShortcutRecord $Path $record
}

if ($Install -or $Rollback -or $Uninstall) {
    if (@($Install,$Rollback,$Uninstall | Where-Object {$_}).Count -ne 1) { throw 'Choose one worker shortcut operation.' }
    if ($Install) {
        $manifest = Get-Content -LiteralPath (Join-Path $InstallRoot 'portable-manifest.json') -Raw -Encoding UTF8 | ConvertFrom-Json
        Install-LabelWorkerShortcuts $InstallRoot $manifest $RecordPath -CommonOnly:$CommonOnly -UserOnly:$UserOnly
    } else { Restore-LabelWorkerShortcuts $InstallRoot $RecordPath -CommonOnly:$CommonOnly -UserOnly:$UserOnly }
}
