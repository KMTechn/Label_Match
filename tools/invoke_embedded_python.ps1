if (-not ("KMTech.LabelMatchEmbeddedPythonHost" -as [type])) {
    Add-Type -TypeDefinition @'
using System;
using System.ComponentModel;
using System.Runtime.InteropServices;

namespace KMTech
{
    public static class LabelMatchEmbeddedPythonHost
    {
        private const uint LOAD_WITH_ALTERED_SEARCH_PATH = 0x00000008;
        private static readonly object Sync = new object();
        private static IntPtr module = IntPtr.Zero;
        private static PyRunSimpleStringFlags runSimpleString;

        [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
        private delegate void PyInitialize();

        [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
        private delegate int PyIsInitialized();

        [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
        private delegate int PyRunSimpleStringFlags(
            [MarshalAs(UnmanagedType.LPStr)] string command,
            IntPtr flags
        );

        [DllImport("kernel32.dll", CharSet = CharSet.Unicode, SetLastError = true)]
        private static extern IntPtr LoadLibraryExW(
            string fileName,
            IntPtr file,
            uint flags
        );

        [DllImport("kernel32.dll", CharSet = CharSet.Ansi, SetLastError = true)]
        private static extern IntPtr GetProcAddress(IntPtr module, string name);

        [DllImport("kernel32.dll", CharSet = CharSet.Unicode, SetLastError = true)]
        private static extern bool SetDllDirectoryW(string path);

        private static Delegate Resolve(string name, Type delegateType)
        {
            IntPtr address = GetProcAddress(module, name);
            if (address == IntPtr.Zero)
            {
                throw new EntryPointNotFoundException(name);
            }
            return Marshal.GetDelegateForFunctionPointer(address, delegateType);
        }

        private static void EnsureInitialized(string pythonDllPath)
        {
            if (module != IntPtr.Zero)
            {
                return;
            }
            string runtimeDirectory = System.IO.Path.GetDirectoryName(pythonDllPath);
            if (!SetDllDirectoryW(runtimeDirectory))
            {
                throw new Win32Exception(Marshal.GetLastWin32Error());
            }
            module = LoadLibraryExW(
                pythonDllPath,
                IntPtr.Zero,
                LOAD_WITH_ALTERED_SEARCH_PATH
            );
            if (module == IntPtr.Zero)
            {
                throw new Win32Exception(Marshal.GetLastWin32Error());
            }
            PyInitialize initialize = (PyInitialize)Resolve(
                "Py_Initialize",
                typeof(PyInitialize)
            );
            PyIsInitialized isInitialized = (PyIsInitialized)Resolve(
                "Py_IsInitialized",
                typeof(PyIsInitialized)
            );
            runSimpleString = (PyRunSimpleStringFlags)Resolve(
                "PyRun_SimpleStringFlags",
                typeof(PyRunSimpleStringFlags)
            );
            initialize();
            if (isInitialized() == 0)
            {
                throw new InvalidOperationException("Embedded Python did not initialize.");
            }
        }

        public static int Run(string pythonDllPath, string command)
        {
            lock (Sync)
            {
                EnsureInitialized(pythonDllPath);
                return runSimpleString(command, IntPtr.Zero);
            }
        }
    }
}
'@
}

$AllowedInstallerFailureCodes = @(
    "CHILD_EXCEPTION",
    "CHILD_IMPORT_FAILED",
    "CHILD_NONZERO_EXIT",
    "CHILD_PROCESS_START_FAILED",
    "CHILD_PROCESS_TIMEOUT",
    "NESTED_INSTALLER_EXCEPTION",
    "NESTED_INSTALLER_NONZERO_EXIT",
    "PUBLIC_POSTCONDITION_FAILED",
    "SCHEDULED_TASK_START_FAILED"
)

function ConvertTo-BoundedDiagnosticText {
    param(
        [AllowNull()][object]$Value,
        [int]$MaximumLength = 512
    )
    $text = [string]$Value
    $text = [regex]::Replace($text, '[\x00-\x1F\x7F]+', ' ')
    $text = [regex]::Replace(
        $text,
        '(?i)\b([A-Za-z0-9_-]*(?:token|password|secret|authorization|cookie|api[_-]?key)[A-Za-z0-9_-]*)\b(\s*[:=]\s*)([^\s,;]+)',
        '$1$2[redacted]'
    )
    $text = [regex]::Replace($text, '(?i)\bbearer\s+[^\s,;]+', 'Bearer [redacted]')
    $text = [regex]::Replace($text.Trim(), '\s+', ' ')
    if ($text.Length -gt $MaximumLength) {
        return $text.Substring(0, $MaximumLength)
    }
    return $text
}

function Test-DiagnosticProperty([AllowNull()][object]$Value, [string]$Name) {
    if ($null -eq $Value) { return $false }
    if ($Value -is [System.Collections.IDictionary]) { return $Value.Contains($Name) }
    return $null -ne $Value.PSObject.Properties[$Name]
}

function Get-DiagnosticProperty([AllowNull()][object]$Value, [string]$Name) {
    if (-not (Test-DiagnosticProperty $Value $Name)) { return $null }
    if ($Value -is [System.Collections.IDictionary]) { return $Value[$Name] }
    return $Value.PSObject.Properties[$Name].Value
}

function New-InstallerFailureDiagnostic {
    param(
        [string]$CommandIdentity,
        [AllowNull()][object]$ChildExitCode,
        [string]$FailureCode,
        [AllowNull()][System.Exception]$Exception = $null
    )
    $identity = ConvertTo-BoundedDiagnosticText $CommandIdentity 96
    if ($identity -cnotmatch '^[A-Za-z0-9][A-Za-z0-9._:-]{0,95}$') {
        $identity = "child_process"
    }
    $code = if ($FailureCode -cin $AllowedInstallerFailureCodes) {
        $FailureCode
    }
    else {
        "CHILD_EXCEPTION"
    }
    $typedExitCode = $null
    if ($null -ne $ChildExitCode) {
        $parsedExitCode = 0
        if ([int]::TryParse([string]$ChildExitCode, [ref]$parsedExitCode)) {
            $typedExitCode = $parsedExitCode
        }
    }
    $diagnostic = [ordered]@{
        diagnostic_version = "label-match-child-failure-v1"
        command_identity = $identity
        child_exit_code = $typedExitCode
        failure_code = $code
    }
    if ($null -ne $Exception) {
        $inner = $Exception
        $innerDepth = 0
        while (
            $innerDepth -lt 8 -and
            $null -ne $inner.InnerException -and
            -not [object]::ReferenceEquals($inner, $inner.InnerException)
        ) {
            $inner = $inner.InnerException
            $innerDepth += 1
        }
        $diagnostic["inner_exception_type"] = ConvertTo-BoundedDiagnosticText ($inner.GetType().Name) 128
        $message = ConvertTo-BoundedDiagnosticText $inner.Message 512
        if (-not [string]::IsNullOrWhiteSpace($message)) {
            $diagnostic["inner_exception_message"] = $message
        }
    }
    return $diagnostic
}

function ConvertTo-InstallerFailureDiagnostic {
    param(
        [AllowNull()][object]$Candidate,
        [string]$FallbackCommandIdentity,
        [AllowNull()][object]$FallbackChildExitCode,
        [string]$FallbackFailureCode
    )
    $commandIdentity = $FallbackCommandIdentity
    if (Test-DiagnosticProperty $Candidate "command_identity") {
        $candidateIdentity = ConvertTo-BoundedDiagnosticText (Get-DiagnosticProperty $Candidate "command_identity") 96
        if ($candidateIdentity -cmatch '^[A-Za-z0-9][A-Za-z0-9._:-]{0,95}$') {
            $commandIdentity = $candidateIdentity
        }
    }
    $childExitCode = $FallbackChildExitCode
    if (Test-DiagnosticProperty $Candidate "child_exit_code") {
        $childExitCode = Get-DiagnosticProperty $Candidate "child_exit_code"
    }
    $failureCode = $FallbackFailureCode
    if (Test-DiagnosticProperty $Candidate "failure_code") {
        $candidateFailureCode = [string](Get-DiagnosticProperty $Candidate "failure_code")
        if ($candidateFailureCode -cin $AllowedInstallerFailureCodes) {
            $failureCode = $candidateFailureCode
        }
    }
    $diagnostic = New-InstallerFailureDiagnostic `
        $commandIdentity $childExitCode $failureCode
    if (Test-DiagnosticProperty $Candidate "inner_exception_type") {
        $exceptionType = ConvertTo-BoundedDiagnosticText (Get-DiagnosticProperty $Candidate "inner_exception_type") 128
        if ($exceptionType -cmatch '^[A-Za-z_][A-Za-z0-9_.]{0,127}$') {
            $diagnostic["inner_exception_type"] = $exceptionType
        }
    }
    if (Test-DiagnosticProperty $Candidate "inner_exception_message") {
        $exceptionMessage = ConvertTo-BoundedDiagnosticText (Get-DiagnosticProperty $Candidate "inner_exception_message") 512
        if (-not [string]::IsNullOrWhiteSpace($exceptionMessage)) {
            $diagnostic["inner_exception_message"] = $exceptionMessage
        }
    }
    return $diagnostic
}

function Invoke-KMTechEmbeddedPython {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory = $true)]
        [string]$AppRoot,

        [Parameter(Mandatory = $true)]
        [string]$ScriptPath,

        [string[]]$Arguments = @(),

        [System.Management.Automation.PSReference]$FailureDiagnostic = $null
    )

    Set-StrictMode -Version Latest
    if ($null -ne $FailureDiagnostic) {
        $FailureDiagnostic.Value = $null
    }

    $resolvedAppRoot = [System.IO.Path]::GetFullPath($AppRoot).TrimEnd('\')
    $resolvedScriptPath = [System.IO.Path]::GetFullPath($ScriptPath)
    $appPrefix = $resolvedAppRoot + '\'
    if (-not $resolvedScriptPath.StartsWith($appPrefix, [System.StringComparison]::OrdinalIgnoreCase)) {
        throw "Embedded Python script must be inside the installed application root."
    }
    if (-not (Test-Path -LiteralPath $resolvedScriptPath -PathType Leaf)) {
        throw "Embedded Python script is missing: $resolvedScriptPath"
    }

    $runtimeRoot = Join-Path $resolvedAppRoot '_internal'
    $pythonDll = Join-Path $runtimeRoot 'python312.dll'
    $baseLibrary = Join-Path $runtimeRoot 'base_library.zip'
    foreach ($requiredPath in @($pythonDll, $baseLibrary)) {
        if (-not (Test-Path -LiteralPath $requiredPath -PathType Leaf)) {
            throw "Embedded Python runtime is incomplete. Missing: $requiredPath"
        }
    }

    $request = [ordered]@{
        script = $resolvedScriptPath
        argv = @($Arguments | ForEach-Object { [string]$_ })
    } | ConvertTo-Json -Compress -Depth 4
    $requestBase64 = [Convert]::ToBase64String(
        [System.Text.Encoding]::UTF8.GetBytes($request)
    )
    $requestName = 'KMTECH_LABEL_MATCH_EMBEDDED_REQUEST_B64'
    $exitName = 'KMTECH_LABEL_MATCH_EMBEDDED_EXIT_CODE'
    $diagnosticName = 'KMTECH_LABEL_MATCH_EMBEDDED_DIAGNOSTIC_B64'
    $priorValues = @{}
    foreach ($name in @(
        $requestName,
        $exitName,
        $diagnosticName,
        'PYTHONHOME',
        'PYTHONPATH',
        'PYTHONUTF8',
        'PYTHONDONTWRITEBYTECODE'
    )) {
        $priorValues[$name] = [Environment]::GetEnvironmentVariable($name, 'Process')
    }

    $pythonPath = @(
        $resolvedAppRoot,
        (Join-Path $resolvedAppRoot 'tools'),
        $runtimeRoot,
        $baseLibrary
    ) -join [System.IO.Path]::PathSeparator
    [Environment]::SetEnvironmentVariable($requestName, $requestBase64, 'Process')
    [Environment]::SetEnvironmentVariable($exitName, $null, 'Process')
    [Environment]::SetEnvironmentVariable($diagnosticName, $null, 'Process')
    [Environment]::SetEnvironmentVariable('PYTHONHOME', $runtimeRoot, 'Process')
    [Environment]::SetEnvironmentVariable('PYTHONPATH', $pythonPath, 'Process')
    [Environment]::SetEnvironmentVariable('PYTHONUTF8', '1', 'Process')
    [Environment]::SetEnvironmentVariable('PYTHONDONTWRITEBYTECODE', '1', 'Process')

    $bootstrap = @'
import base64
import json
import os
import re
import runpy
import sys

_request = json.loads(base64.b64decode(os.environ["KMTECH_LABEL_MATCH_EMBEDDED_REQUEST_B64"]))
sys.argv = [_request["script"], *[str(value) for value in _request.get("argv", [])]]
_exit_code = 0
_diagnostic = None

def _bounded_message(value):
    text = str(value or "")[:4096]
    for raw_value in sorted(
        {str(item) for item in _request.get("argv", []) if str(item)},
        key=len,
        reverse=True,
    ):
        text = text.replace(raw_value, "[redacted]")
    text = re.sub(r"[\x00-\x1f\x7f]+", " ", text)
    text = re.sub(
        r"(?i)\b([A-Za-z0-9_-]*(?:token|password|secret|authorization|cookie|api[_-]?key)[A-Za-z0-9_-]*)\b(\s*[:=]\s*)([^\s,;]+)",
        r"\1\2[redacted]",
        text,
    )
    text = re.sub(r"(?i)\bbearer\s+[^\s,;]+", "Bearer [redacted]", text)
    return " ".join(text.split())[:512]

try:
    runpy.run_path(_request["script"], run_name="__main__")
except SystemExit as _exc:
    if _exc.code is None:
        _exit_code = 0
    elif isinstance(_exc.code, int):
        _exit_code = int(_exc.code)
    else:
        _exit_code = 1
    if _exit_code != 0:
        _diagnostic = {
            "diagnostic_version": "label-match-child-failure-v1",
            "command_identity": os.path.basename(_request["script"]),
            "child_exit_code": _exit_code,
            "failure_code": "CHILD_NONZERO_EXIT",
        }
except BaseException as _exc:
    _exit_code = 1
    _diagnostic = {
        "diagnostic_version": "label-match-child-failure-v1",
        "command_identity": os.path.basename(_request["script"]),
        "child_exit_code": None,
        "failure_code": "CHILD_EXCEPTION",
        "inner_exception_type": type(_exc).__name__,
        "inner_exception_message": _bounded_message(_exc),
    }
os.environ["KMTECH_LABEL_MATCH_EMBEDDED_EXIT_CODE"] = str(_exit_code)
if _diagnostic is not None:
    os.environ["KMTECH_LABEL_MATCH_EMBEDDED_DIAGNOSTIC_B64"] = base64.b64encode(
        json.dumps(_diagnostic, ensure_ascii=True, separators=(",", ":")).encode("utf-8")
    ).decode("ascii")
'@

    try {
        $hostResult = [KMTech.LabelMatchEmbeddedPythonHost]::Run($pythonDll, $bootstrap)
        if ($hostResult -ne 0) {
            throw "Embedded Python host failed before returning an exit code: $hostResult"
        }
        $exitText = [Environment]::GetEnvironmentVariable($exitName, 'Process')
        $exitCode = 0
        if (-not [int]::TryParse($exitText, [ref]$exitCode)) {
            throw "Embedded Python script did not return a typed exit code."
        }
        $diagnosticText = [Environment]::GetEnvironmentVariable($diagnosticName, 'Process')
        if (
            $null -ne $FailureDiagnostic -and
            -not [string]::IsNullOrWhiteSpace($diagnosticText) -and
            $diagnosticText.Length -le 8192
        ) {
            try {
                $diagnosticBytes = [Convert]::FromBase64String($diagnosticText)
                if ($diagnosticBytes.Length -le 4096) {
                    $diagnosticJson = [System.Text.Encoding]::UTF8.GetString($diagnosticBytes)
                    $FailureDiagnostic.Value = $diagnosticJson | ConvertFrom-Json
                }
            }
            catch {
                $FailureDiagnostic.Value = $null
            }
        }
        return $exitCode
    }
    finally {
        foreach ($name in @(
            $requestName,
            $exitName,
            $diagnosticName,
            'PYTHONHOME',
            'PYTHONPATH',
            'PYTHONUTF8',
            'PYTHONDONTWRITEBYTECODE'
        )) {
            [Environment]::SetEnvironmentVariable($name, $priorValues[$name], 'Process')
        }
    }
}
