Set-StrictMode -Version Latest

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

function Invoke-KMTechEmbeddedPython {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory = $true)]
        [string]$AppRoot,

        [Parameter(Mandatory = $true)]
        [string]$ScriptPath,

        [string[]]$Arguments = @()
    )

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
    $priorValues = @{}
    foreach ($name in @(
        $requestName,
        $exitName,
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
    [Environment]::SetEnvironmentVariable('PYTHONHOME', $runtimeRoot, 'Process')
    [Environment]::SetEnvironmentVariable('PYTHONPATH', $pythonPath, 'Process')
    [Environment]::SetEnvironmentVariable('PYTHONUTF8', '1', 'Process')
    [Environment]::SetEnvironmentVariable('PYTHONDONTWRITEBYTECODE', '1', 'Process')

    $bootstrap = @'
import base64
import json
import os
import runpy
import sys
import traceback

_request = json.loads(base64.b64decode(os.environ["KMTECH_LABEL_MATCH_EMBEDDED_REQUEST_B64"]))
sys.argv = [_request["script"], *[str(value) for value in _request.get("argv", [])]]
_exit_code = 0
try:
    runpy.run_path(_request["script"], run_name="__main__")
except SystemExit as _exc:
    if _exc.code is None:
        _exit_code = 0
    elif isinstance(_exc.code, int):
        _exit_code = int(_exc.code)
    else:
        print(str(_exc.code), file=sys.stderr)
        _exit_code = 1
except BaseException:
    traceback.print_exc()
    _exit_code = 1
os.environ["KMTECH_LABEL_MATCH_EMBEDDED_EXIT_CODE"] = str(_exit_code)
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
        return $exitCode
    }
    finally {
        foreach ($name in @(
            $requestName,
            $exitName,
            'PYTHONHOME',
            'PYTHONPATH',
            'PYTHONUTF8',
            'PYTHONDONTWRITEBYTECODE'
        )) {
            [Environment]::SetEnvironmentVariable($name, $priorValues[$name], 'Process')
        }
    }
}
