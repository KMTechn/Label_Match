Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'
$ProgressPreference = 'SilentlyContinue'

$evidenceRoot = 'C:\Seq296\evidence'
$mappedNegative = 'C:\Seq296\mapped-negative'
$mappedPositive = 'C:\Seq296\mapped-positive'
$workRoot = 'C:\Seq296Work'
$negativeRoot = Join-Path $workRoot 'negative'
$positiveRoot = Join-Path $workRoot 'positive'
$summaryPath = Join-Path $evidenceRoot 'summary.json'
$inputBindingPath = Join-Path $evidenceRoot 'input-binding.json'
$negativeEventsPath = Join-Path $evidenceRoot 'negative-ci-events.json'
$positiveEventsPath = Join-Path $evidenceRoot 'positive-ci-events.json'
$positiveInventoryPath = Join-Path $evidenceRoot 'positive-pe-inventory.json'
$catalogDiagnosticPath = Join-Path $evidenceRoot 'item-catalog-startup-diagnostic.json'
$logPath = Join-Path $evidenceRoot 'guest.log'
$completePath = Join-Path $evidenceRoot 'complete.marker'
$ciLogName = 'Microsoft-Windows-CodeIntegrity/Operational'
$utf8 = [Text.UTF8Encoding]::new($false)
$inputBinding = Get-Content -LiteralPath $inputBindingPath -Raw | ConvertFrom-Json
if (
    [string]$inputBinding.schema -cne 'label-sac-input-binding-v1' -or
    [string]$inputBinding.positive_source_commit -cnotmatch '^[0-9a-f]{40}$' -or
    [string]$inputBinding.positive_manifest_sha256 -cnotmatch '^[0-9a-f]{64}$' -or
    [string]$inputBinding.positive_inventory_sha256 -cnotmatch '^[0-9a-f]{64}$' -or
    [string]$inputBinding.negative_executable_sha256 -cnotmatch '^[0-9a-f]{64}$'
) { throw 'Sandbox input binding is absent or malformed.' }
$expectedNegativeSha256 = [string]$inputBinding.negative_executable_sha256

function ConvertTo-BoundedText {
    param([AllowNull()][object]$Value, [int]$MaximumCharacters = 32768)
    if ($null -eq $Value) { return '' }
    $text = [string]$Value
    if ($text.Length -le $MaximumCharacters) { return $text }
    return $text.Substring(0, $MaximumCharacters) + "`n[TRUNCATED]"
}

function Write-GuestLog {
    param([Parameter(Mandatory = $true)][string]$Message)
    $safe = ConvertTo-BoundedText -Value (($Message -replace '[\r\n]+', ' ').Trim()) -MaximumCharacters 2048
    [IO.File]::AppendAllText(
        $logPath,
        ('{0} {1}{2}' -f [DateTimeOffset]::UtcNow.ToString('o'), $safe, [Environment]::NewLine),
        $utf8
    )
}

function Write-JsonFile {
    param([Parameter(Mandatory = $true)][string]$Path, [Parameter(Mandatory = $true)][object]$Value)
    $json = $Value | ConvertTo-Json -Depth 20
    if ($json.Length -gt 8388608) { throw ('Refusing JSON evidence larger than 8 MiB: {0}' -f $Path) }
    [IO.File]::WriteAllText($Path, ($json + [Environment]::NewLine), $utf8)
}

function Get-RegistryObservation {
    param([Parameter(Mandatory = $true)][string]$Path, [Parameter(Mandatory = $true)][string]$Name)
    try {
        return [pscustomobject][ordered]@{
            path = $Path
            name = $Name
            status = 'AVAILABLE'
            value = [int64](Get-ItemPropertyValue -LiteralPath $Path -Name $Name -ErrorAction Stop)
            error = $null
        }
    }
    catch {
        return [pscustomobject][ordered]@{
            path = $Path
            name = $Name
            status = 'UNAVAILABLE'
            value = $null
            error = ConvertTo-BoundedText -Value $_.Exception.Message -MaximumCharacters 1024
        }
    }
}

function Get-SacRegistrySnapshot {
    return [pscustomobject][ordered]@{
        captured_at_utc = [DateTimeOffset]::UtcNow.ToString('o')
        policy_state = Get-RegistryObservation -Path 'HKLM:\SYSTEM\CurrentControlSet\Control\CI\Policy' -Name 'VerifiedAndReputablePolicyState'
        minimum_value_seen = Get-RegistryObservation -Path 'HKLM:\SYSTEM\CurrentControlSet\Control\CI\Protected' -Name 'VerifiedAndReputablePolicyStateMinValueSeen'
        learning_mode_switch = Get-RegistryObservation -Path 'HKLM:\SOFTWARE\Microsoft\Windows Defender' -Name 'SacLearningModeSwitch'
    }
}

function Get-UnsignedExitCodeHex {
    param([int]$ExitCode)
    return ('0x{0:X8}' -f [BitConverter]::ToUInt32([BitConverter]::GetBytes($ExitCode), 0))
}

function Invoke-BoundedProcess {
    param(
        [Parameter(Mandatory = $true)][string]$FilePath,
        [string]$Arguments = '',
        [int]$TimeoutMilliseconds = 20000
    )
    $process = [Diagnostics.Process]::new()
    $info = [Diagnostics.ProcessStartInfo]::new()
    $info.FileName = $FilePath
    $info.Arguments = $Arguments
    $info.UseShellExecute = $false
    $info.CreateNoWindow = $true
    $info.RedirectStandardInput = $true
    $info.RedirectStandardOutput = $true
    $info.RedirectStandardError = $true
    $process.StartInfo = $info
    $startedAt = [DateTimeOffset]::UtcNow
    $started = $false
    $timedOut = $false
    $exitCode = $null
    $stdout = ''
    $stderr = ''
    $processId = $null
    $exceptionType = $null
    $exceptionMessage = $null
    $nativeErrorCode = $null
    $hresult = $null
    try {
        $started = $process.Start()
        if ($started) {
            $processId = $process.Id
            $stdoutTask = $process.StandardOutput.ReadToEndAsync()
            $stderrTask = $process.StandardError.ReadToEndAsync()
            $process.StandardInput.Close()
            if (-not $process.WaitForExit($TimeoutMilliseconds)) {
                $timedOut = $true
                $process.Kill()
            }
            $process.WaitForExit()
            $stdout = ConvertTo-BoundedText -Value $stdoutTask.Result -MaximumCharacters 32768
            $stderr = ConvertTo-BoundedText -Value $stderrTask.Result -MaximumCharacters 32768
            $exitCode = [int]$process.ExitCode
        }
    }
    catch {
        $exceptionType = $_.Exception.GetType().FullName
        $exceptionMessage = ConvertTo-BoundedText -Value $_.Exception.Message -MaximumCharacters 4096
        $hresult = Get-UnsignedExitCodeHex -ExitCode ([int]$_.Exception.HResult)
        if ($_.Exception.PSObject.Properties.Name -contains 'NativeErrorCode') {
            $nativeErrorCode = $_.Exception.NativeErrorCode
        }
    }
    finally { $process.Dispose() }
    return [pscustomobject][ordered]@{
        file_path = $FilePath
        arguments = $Arguments
        started_at_utc = $startedAt.ToString('o')
        finished_at_utc = [DateTimeOffset]::UtcNow.ToString('o')
        started = $started
        process_id = $processId
        timed_out_and_terminated = $timedOut
        exit_code = $exitCode
        exit_code_hex = if ($null -ne $exitCode) { Get-UnsignedExitCodeHex -ExitCode $exitCode } else { $null }
        stdout = $stdout
        stderr = $stderr
        exception_type = $exceptionType
        exception_message = $exceptionMessage
        native_error_code = $nativeErrorCode
        hresult = $hresult
    }
}

function Get-LatestCiRecordId {
    try {
        return [int64](Get-WinEvent -LogName $ciLogName -MaxEvents 1 -ErrorAction Stop).RecordId
    }
    catch {
        Write-GuestLog ('CI_BASELINE_UNAVAILABLE={0}' -f $_.Exception.Message)
        return [int64]0
    }
}

function Get-CiEventsAfter {
    param([Parameter(Mandatory = $true)][int64]$AfterRecordId)
    $xpath = "*[System[((EventID=3076) or (EventID=3077) or (EventID=3082) or (EventID=3089)) and EventRecordID > $AfterRecordId]]"
    $queryErrors = @()
    $rawEvents = @(Get-WinEvent -LogName $ciLogName -FilterXPath $xpath -MaxEvents 1001 -ErrorAction SilentlyContinue -ErrorVariable queryErrors)
    $truncated = $rawEvents.Count -gt 1000
    if ($truncated) { $rawEvents = @($rawEvents | Select-Object -First 1000) }
    $summaries = @()
    foreach ($event in @($rawEvents | Sort-Object RecordId)) {
        $eventData = [ordered]@{}
        try {
            $xml = [xml]$event.ToXml()
            $index = 0
            foreach ($node in @($xml.Event.EventData.Data)) {
                $key = if ($node.Name) { [string]$node.Name } else { 'property_{0}' -f $index }
                if ($eventData.Contains($key)) { $key = '{0}_{1}' -f $key, $index }
                $eventData[$key] = ConvertTo-BoundedText -Value $node.InnerText -MaximumCharacters 4096
                $index++
            }
        }
        catch { $eventData['xml_parse_error'] = ConvertTo-BoundedText -Value $_.Exception.Message -MaximumCharacters 1024 }
        $summaries += [pscustomobject][ordered]@{
            id = [int]$event.Id
            record_id = [int64]$event.RecordId
            time_created_utc = if ($event.TimeCreated) { $event.TimeCreated.ToUniversalTime().ToString('o') } else { $null }
            data = [pscustomobject]$eventData
            message = ConvertTo-BoundedText -Value $event.Message -MaximumCharacters 8192
        }
    }
    return [pscustomobject][ordered]@{
        after_record_id = $AfterRecordId
        captured_at_utc = [DateTimeOffset]::UtcNow.ToString('o')
        returned_count = $summaries.Count
        truncated_at_1000 = $truncated
        query_errors = @(
            $queryErrors |
                Where-Object { $_.Exception.Message -notlike 'No events were found*' } |
                ForEach-Object { ConvertTo-BoundedText -Value $_.Exception.Message -MaximumCharacters 1024 }
        )
        events = $summaries
    }
}

function Test-MzFile {
    param([Parameter(Mandatory = $true)][IO.FileInfo]$File)
    if ($File.Length -lt 2) { return $false }
    $stream = [IO.File]::OpenRead($File.FullName)
    try { return ($stream.ReadByte() -eq 0x4D -and $stream.ReadByte() -eq 0x5A) }
    finally { $stream.Dispose() }
}

function Get-PeInventory {
    param([Parameter(Mandatory = $true)][string]$Root)
    $rows = @()
    foreach ($file in @(Get-ChildItem -LiteralPath $Root -Recurse -File | Sort-Object FullName)) {
        if (-not (Test-MzFile -File $file)) { continue }
        $signature = Get-AuthenticodeSignature -FilePath $file.FullName
        $rows += [pscustomobject][ordered]@{
            relative_path = $file.FullName.Substring($Root.Length + 1)
            bytes = [int64]$file.Length
            sha256 = (Get-FileHash -LiteralPath $file.FullName -Algorithm SHA256).Hash.ToLowerInvariant()
            status = [string]$signature.Status
            signer_subject = if ($signature.SignerCertificate) { $signature.SignerCertificate.Subject } else { $null }
        }
    }
    return [pscustomobject][ordered]@{
        root = $Root
        captured_at_utc = [DateTimeOffset]::UtcNow.ToString('o')
        pe_count = $rows.Count
        valid_count = @($rows | Where-Object status -eq 'Valid').Count
        not_signed_count = @($rows | Where-Object status -eq 'NotSigned').Count
        other_status_count = @($rows | Where-Object { $_.status -notin @('Valid', 'NotSigned') }).Count
        unsigned_paths = @($rows | Where-Object status -eq 'NotSigned' | ForEach-Object relative_path)
        files = $rows
    }
}

if (-not ('Seq296WindowProbe' -as [type])) {
    Add-Type -TypeDefinition @'
using System;
using System.Collections.Generic;
using System.Runtime.InteropServices;
using System.Text;
public static class Seq296WindowProbe {
    [StructLayout(LayoutKind.Sequential)] public struct RECT { public int Left, Top, Right, Bottom; }
    public delegate bool EnumProc(IntPtr hWnd, IntPtr parameter);
    [DllImport("user32.dll")] public static extern bool EnumWindows(EnumProc callback, IntPtr parameter);
    [DllImport("user32.dll")] public static extern uint GetWindowThreadProcessId(IntPtr hWnd, out uint processId);
    [DllImport("user32.dll")] public static extern bool IsWindowVisible(IntPtr hWnd);
    [DllImport("user32.dll", CharSet=CharSet.Unicode)] public static extern int GetWindowText(IntPtr hWnd, StringBuilder text, int maximum);
    [DllImport("user32.dll")] public static extern bool GetWindowRect(IntPtr hWnd, out RECT rectangle);
}
'@
}

function Get-VisibleWindowsForProcess {
    param([Parameter(Mandatory = $true)][int]$ProcessId)
    $rows = [Collections.Generic.List[object]]::new()
    [Seq296WindowProbe]::EnumWindows({
        param($handle, $parameter)
        [uint32]$ownerProcessId = 0
        [void][Seq296WindowProbe]::GetWindowThreadProcessId($handle, [ref]$ownerProcessId)
        if ($ownerProcessId -eq $ProcessId -and [Seq296WindowProbe]::IsWindowVisible($handle)) {
            $text = [Text.StringBuilder]::new(1024)
            [void][Seq296WindowProbe]::GetWindowText($handle, $text, 1024)
            $rectangle = [Seq296WindowProbe+RECT]::new()
            [void][Seq296WindowProbe]::GetWindowRect($handle, [ref]$rectangle)
            $rows.Add([pscustomobject][ordered]@{
                handle = $handle.ToInt64()
                title = $text.ToString()
                left = $rectangle.Left
                top = $rectangle.Top
                right = $rectangle.Right
                bottom = $rectangle.Bottom
            })
        }
        return $true
    }, [IntPtr]::Zero) | Out-Null
    return @($rows)
}

function Get-ExactPythonwProcess {
    param([Parameter(Mandatory = $true)][string]$ExecutablePath, [int[]]$ExcludeIds = @())
    return @(
        Get-CimInstance Win32_Process -Filter "Name='pythonw.exe'" -ErrorAction SilentlyContinue |
            Where-Object {
                $_.ExecutablePath -eq $ExecutablePath -and
                $ExcludeIds -notcontains [int]$_.ProcessId
            } |
            Sort-Object CreationDate |
            Select-Object -First 1
    ) | Select-Object -First 1
}

New-Item -ItemType Directory -Path $evidenceRoot -Force | Out-Null
[IO.File]::WriteAllText($logPath, '', $utf8)
Write-GuestLog 'SEQ296_GUEST_START'
$startedAt = [DateTimeOffset]::UtcNow
$verdict = 'INCONCLUSIVE_GUEST_ERROR'
$overallError = $null
$negative = $null
$positive = $null
$baselineRegistry = $null
$finalRegistry = $null
$guiProcessId = $null
$guiCommandProcessId = $null
try {
    $baselineRegistry = Get-SacRegistrySnapshot
    if ($baselineRegistry.policy_state.status -ne 'AVAILABLE' -or $baselineRegistry.policy_state.value -ne 1) {
        $verdict = 'INVALID_SAC_STATE_NOT_ENFORCE_1'
        throw ('Sandbox SAC state is not ENFORCE 1: {0}' -f $baselineRegistry.policy_state.value)
    }
    New-Item -ItemType Directory -Path $workRoot -Force | Out-Null

    # The exact current frozen Label artifact is the negative control and must run first.
    Write-GuestLog 'NEGATIVE_COPY_START'
    Copy-Item -LiteralPath $mappedNegative -Destination $negativeRoot -Recurse
    $negativeExe = Join-Path $negativeRoot 'Label_Match.exe'
    $negativeHash = (Get-FileHash -LiteralPath $negativeExe -Algorithm SHA256).Hash.ToLowerInvariant()
    if ($negativeHash -cne $expectedNegativeSha256) { throw 'Negative frozen Label executable identity mismatch.' }
    $negativeBaseline = Get-LatestCiRecordId
    $negativeBehavior = Invoke-BoundedProcess -FilePath $negativeExe -TimeoutMilliseconds 12000
    Start-Sleep -Seconds 5
    $negativeEvents = Get-CiEventsAfter -AfterRecordId $negativeBaseline
    Write-JsonFile -Path $negativeEventsPath -Value $negativeEvents
    $negativeMatches = @(
        $negativeEvents.events | Where-Object {
            $text = ($_ | ConvertTo-Json -Depth 12 -Compress).ToLowerInvariant()
            $_.id -eq 3077 -and
            $text.Contains('label_match.exe') -and
            $text.Contains('verifiedandreputabledesktop') -and
            $text.Contains($expectedNegativeSha256)
        }
    )
    $negative = [pscustomobject][ordered]@{
        tested_first = $true
        executable_path = $negativeExe
        executable_sha256 = $negativeHash
        behavior = $negativeBehavior
        ci_event_3077_count = @($negativeEvents.events | Where-Object id -eq 3077).Count
        exact_policy_hash_path_3077_count = $negativeMatches.Count
        policy_name = 'VerifiedAndReputableDesktop'
        block_proven = $negativeMatches.Count -gt 0
    }
    if (-not $negative.block_proven) {
        $verdict = 'INCONCLUSIVE_NEGATIVE_LABEL_NOT_BLOCKED'
        throw 'Frozen Label negative control did not produce exact VerifiedAndReputableDesktop Code Integrity 3077.'
    }

    Write-GuestLog 'POSITIVE_COPY_START'
    Copy-Item -LiteralPath $mappedPositive -Destination $positiveRoot -Recurse
    $positiveManifestPath = Join-Path $positiveRoot 'portable-manifest.json'
    $positiveManifestHash = (Get-FileHash -LiteralPath $positiveManifestPath -Algorithm SHA256).Hash.ToLowerInvariant()
    $positiveManifest = Get-Content -LiteralPath $positiveManifestPath -Raw | ConvertFrom-Json
    if (
        $positiveManifestHash -cne [string]$inputBinding.positive_manifest_sha256 -or
        [string]$positiveManifest.source_commit -cne [string]$inputBinding.positive_source_commit
    ) { throw 'Positive portable identity differs after the read-only Sandbox mapping.' }
    $inventory = Get-PeInventory -Root $positiveRoot
    Write-JsonFile -Path $positiveInventoryPath -Value $inventory
    if ($inventory.pe_count -ne 46 -or $inventory.valid_count -ne 46 -or $inventory.not_signed_count -ne 0 -or $inventory.other_status_count -ne 0) {
        throw ('Portable PE inventory mismatch: PE={0} Valid={1} Unsigned={2} Other={3}' -f $inventory.pe_count, $inventory.valid_count, $inventory.not_signed_count, $inventory.other_status_count)
    }

    $env:LABEL_MATCH_AUTOMATED_TEST = '1'
    $env:LABEL_MATCH_AUDIO_ENABLED = 'off'
    $env:LABEL_MATCH_DIRECT_SYNC_BOOTSTRAP = 'off'
    $env:LABEL_MATCH_SESSION_SYNC_TRIGGER = 'off'
    $env:LABEL_MATCH_UPDATE_PROVIDER = 'off'
    $env:LABEL_MATCH_ENABLE_FIRST_RUN_ONBOARDING = 'off'
    $env:PYTHONDONTWRITEBYTECODE = '1'
    $env:LABEL_MATCH_SAVE_DIR = Join-Path $workRoot 'data'
    $env:LABEL_MATCH_PORTABLE_SMOKE_MARKER = Join-Path $evidenceRoot 'portable-smoke.json'
    $positiveBaseline = Get-LatestCiRecordId
    $launcher = Join-Path $positiveRoot 'launch-label-match.cmd'
    $launcherBehavior = Invoke-BoundedProcess -FilePath "$env:SystemRoot\System32\cmd.exe" -Arguments ('/d /c "{0}" --zero-pe-portable-smoke' -f $launcher) -TimeoutMilliseconds 60000
    $smokePass = (
        $launcherBehavior.started -and
        -not $launcherBehavior.timed_out_and_terminated -and
        $launcherBehavior.exit_code -eq 0 -and
        (Test-Path -LiteralPath $env:LABEL_MATCH_PORTABLE_SMOKE_MARKER -PathType Leaf)
    )
    if (-not $smokePass) { throw 'Portable .cmd smoke launch did not complete successfully.' }

    $runtimePythonw = (Resolve-Path -LiteralPath (Join-Path $positiveRoot 'runtime\pythonw.exe')).Path
    $beforePythonwIds = @(Get-Process pythonw -ErrorAction SilentlyContinue | ForEach-Object Id)
    $guiCommand = Start-Process -FilePath "$env:SystemRoot\System32\cmd.exe" -ArgumentList @('/d', '/c', $launcher) -PassThru -WindowStyle Hidden
    $guiCommandProcessId = $guiCommand.Id
    $processDeadline = [DateTimeOffset]::UtcNow.AddSeconds(60)
    $guiProcess = $null
    while ($null -eq $guiProcess -and [DateTimeOffset]::UtcNow -lt $processDeadline) {
        $guiProcess = Get-ExactPythonwProcess -ExecutablePath $runtimePythonw -ExcludeIds $beforePythonwIds
        if ($null -eq $guiProcess) { Start-Sleep -Milliseconds 200 }
    }
    if ($null -eq $guiProcess) { throw 'Portable GUI pythonw process did not appear within 60 seconds.' }
    $guiProcessId = [int]$guiProcess.ProcessId

    $windowDeadline = [DateTimeOffset]::UtcNow.AddSeconds(90)
    $mainWindow = $null
    while ($null -eq $mainWindow -and [DateTimeOffset]::UtcNow -lt $windowDeadline) {
        $mainWindow = @(
            Get-VisibleWindowsForProcess -ProcessId $guiProcessId |
                Where-Object { $_.title -like '*(v2.0.94)*' } |
                Select-Object -First 1
        ) | Select-Object -First 1
        if ($null -eq $mainWindow) { Start-Sleep -Milliseconds 250 }
    }
    if ($null -eq $mainWindow) {
        $observedWindows = @(Get-VisibleWindowsForProcess -ProcessId $guiProcessId)
        throw ('Portable GUI main window did not appear; observed titles={0}' -f (($observedWindows | ForEach-Object title) -join ' | '))
    }

    $readyAt = [DateTimeOffset]::UtcNow
    Write-GuestLog ('GUI_READY pid={0} hwnd={1} title={2}' -f $guiProcessId, $mainWindow.handle, $mainWindow.title)
    Start-Sleep -Seconds 45
    $survivedHold = $null -ne (Get-Process -Id $guiProcessId -ErrorAction SilentlyContinue)
    $windowAfterHold = @(
        Get-VisibleWindowsForProcess -ProcessId $guiProcessId |
            Where-Object { $_.handle -eq $mainWindow.handle -and $_.title -like '*(v2.0.94)*' } |
            Select-Object -First 1
    ) | Select-Object -First 1
    $visibleAfterHold = $null -ne $windowAfterHold

    $runtimeProcess = Get-CimInstance Win32_Process -Filter "ProcessId=$guiProcessId" -ErrorAction SilentlyContinue
    $exactRuntimePath = $null -ne $runtimeProcess -and $runtimeProcess.ExecutablePath -eq $runtimePythonw
    if ($runtimeProcess) { Stop-Process -Id $guiProcessId -Force -ErrorAction Stop }
    $commandProcess = Get-Process -Id $guiCommandProcessId -ErrorAction SilentlyContinue
    if ($commandProcess) { Stop-Process -Id $guiCommandProcessId -Force -ErrorAction Stop }
    $guiProcessId = $null
    $guiCommandProcessId = $null

    Start-Sleep -Seconds 7
    $positiveEvents = Get-CiEventsAfter -AfterRecordId $positiveBaseline
    Write-JsonFile -Path $positiveEventsPath -Value $positiveEvents
    $ciCounts = [ordered]@{}
    foreach ($eventId in @(3076, 3077, 3082, 3089)) {
        $ciCounts[[string]$eventId] = @($positiveEvents.events | Where-Object id -eq $eventId).Count
    }
    $positiveCiZero = (
        $positiveEvents.returned_count -eq 0 -and
        -not $positiveEvents.truncated_at_1000 -and
        $positiveEvents.query_errors.Count -eq 0
    )
    $guiPass = $exactRuntimePath -and $survivedHold -and $visibleAfterHold

    $sourceCatalogDiagnostic = Join-Path $env:LOCALAPPDATA 'KMTech\DirectSync\label_match\status\item_catalog_startup_diagnostic.json'
    $catalogDiagnostic = $null
    if (Test-Path -LiteralPath $sourceCatalogDiagnostic -PathType Leaf) {
        Copy-Item -LiteralPath $sourceCatalogDiagnostic -Destination $catalogDiagnosticPath -Force
        if ((Get-Item -LiteralPath $catalogDiagnosticPath).Length -le 1048576) {
            $catalogDiagnostic = Get-Content -LiteralPath $catalogDiagnosticPath -Raw | ConvertFrom-Json
        }
    }
    $positive = [pscustomobject][ordered]@{
        identity = [pscustomobject][ordered]@{
            source_commit = [string]$positiveManifest.source_commit
            manifest_sha256 = $positiveManifestHash
        }
        inventory = [pscustomobject][ordered]@{
            pe_count = $inventory.pe_count
            valid_count = $inventory.valid_count
            not_signed_count = $inventory.not_signed_count
            other_status_count = $inventory.other_status_count
            unsigned_paths = @($inventory.unsigned_paths)
        }
        smoke_launcher = [pscustomobject][ordered]@{
            behavior = $launcherBehavior
            marker_exists = Test-Path -LiteralPath $env:LABEL_MATCH_PORTABLE_SMOKE_MARKER -PathType Leaf
            pass = $smokePass
        }
        gui_launcher = [pscustomobject][ordered]@{
            command_process_id = $guiCommand.Id
            pythonw_process_id = $guiProcess.ProcessId
            runtime_pythonw_path = $runtimePythonw
            exact_runtime_path = $exactRuntimePath
            main_window = $mainWindow
            ready_at_utc = $readyAt.ToString('o')
            hold_seconds = 45
            survived_hold = $survivedHold
            visible_after_hold = $visibleAfterHold
            pass = $guiPass
        }
        catalog_diagnostic = $catalogDiagnostic
        code_integrity = [pscustomobject][ordered]@{
            counts = [pscustomobject]$ciCounts
            returned_count = $positiveEvents.returned_count
            truncated_at_1000 = $positiveEvents.truncated_at_1000
            query_error_count = $positiveEvents.query_errors.Count
            all_requested_event_counts_zero = $positiveCiZero
        }
    }
    if ($smokePass -and $guiPass -and $positiveCiZero) {
        $verdict = 'PASS_SAC_ENFORCE_LABEL_NEGATIVE_THEN_PORTABLE_GUI'
    }
    else {
        $verdict = 'FAILED_SAC_ENFORCE_PORTABLE_GATE'
        throw 'Portable Label Sandbox behavior did not satisfy smoke, GUI, and zero-CI-event gates.'
    }
}
catch {
    $overallError = [pscustomobject][ordered]@{
        type = $_.Exception.GetType().FullName
        message = ConvertTo-BoundedText -Value $_.Exception.Message -MaximumCharacters 4096
        hresult = Get-UnsignedExitCodeHex -ExitCode ([int]$_.Exception.HResult)
        script_stack_trace = ConvertTo-BoundedText -Value $_.ScriptStackTrace -MaximumCharacters 8192
    }
    Write-GuestLog ('GUEST_ERROR={0}' -f $overallError.message)
}
finally {
    if ($null -ne $guiProcessId) {
        $process = Get-Process -Id $guiProcessId -ErrorAction SilentlyContinue
        if ($process -and $process.Path -eq (Join-Path $positiveRoot 'runtime\pythonw.exe')) {
            Stop-Process -Id $guiProcessId -Force -ErrorAction SilentlyContinue
        }
    }
    if ($null -ne $guiCommandProcessId) {
        $process = Get-Process -Id $guiCommandProcessId -ErrorAction SilentlyContinue
        if ($process -and $process.ProcessName -eq 'cmd') {
            Stop-Process -Id $guiCommandProcessId -Force -ErrorAction SilentlyContinue
        }
    }
    $finalRegistry = Get-SacRegistrySnapshot
    if (
        $verdict -eq 'PASS_SAC_ENFORCE_LABEL_NEGATIVE_THEN_PORTABLE_GUI' -and
        ($finalRegistry.policy_state.status -ne 'AVAILABLE' -or $finalRegistry.policy_state.value -ne 1)
    ) {
        $verdict = 'FAILED_SAC_STATE_CHANGED'
    }
    $summary = [pscustomobject][ordered]@{
        schema = 'seq296-label-sandbox-sac/v1'
        run_started_at_utc = $startedAt.ToString('o')
        run_finished_at_utc = [DateTimeOffset]::UtcNow.ToString('o')
        verdict = $verdict
        input_binding = $inputBinding
        baseline_registry = $baselineRegistry
        final_registry = $finalRegistry
        negative_control = $negative
        positive_candidate = $positive
        error = $overallError
        host_policy_written = $false
        guest_policy_written = $false
    }
    Write-JsonFile -Path $summaryPath -Value $summary
    $marker = @(
        'STATUS=COMPLETE',
        ('VERDICT={0}' -f $verdict),
        ('SUMMARY_BYTES={0}' -f (Get-Item -LiteralPath $summaryPath).Length),
        ('FINISHED_UTC={0}' -f [DateTimeOffset]::UtcNow.ToString('o'))
    ) -join [Environment]::NewLine
    [IO.File]::WriteAllText($completePath, ($marker + [Environment]::NewLine), $utf8)
    Write-GuestLog 'SEQ296_EVIDENCE_COMPLETE'
    Start-Sleep -Seconds 2
    if ($env:USERNAME -ieq 'WDAGUtilityAccount' -and (Test-Path -LiteralPath $completePath -PathType Leaf)) {
        & "$env:SystemRoot\System32\shutdown.exe" /s /t 0
    }
}
