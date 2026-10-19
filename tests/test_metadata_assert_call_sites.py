"""Every metadata secret check passes its environment explicitly.

Contract #31 (main version) makes an omitted environment mean an empty one, so a
call that forgets the argument silently loses its protection. These tests read
the product sources only (Python AST, PowerShell parser tokens) and pin the call
count, so a new call site cannot slip in without being reviewed.
"""
import ast
import json
import os
import shutil
import subprocess
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
PYTHON_CHECK = "assert_machine_metadata_text"
POWERSHELL_CHECK = "Assert-KmtechMachineMetadataText"
EXPECTED_PYTHON_CALLS = 3
EXPECTED_POWERSHELL_CALLS = 2
SKIPPED_DIRECTORIES = {".git", "tests", "__pycache__", ".pytest_cache", "node_modules"}


def product_files(suffixes):
    """Product sources below the repository root; links and junctions are never followed."""
    stack = [ROOT]
    found = []
    while stack:
        directory = stack.pop()
        with os.scandir(directory) as entries:
            for entry in entries:
                if entry.is_symlink() or getattr(entry, "is_junction", lambda: False)():
                    continue
                if entry.is_dir(follow_symlinks=False):
                    if entry.name not in SKIPPED_DIRECTORIES:
                        stack.append(Path(entry.path))
                elif entry.name.lower().endswith(suffixes):
                    found.append(Path(entry.path))
    return sorted(found)


def python_calls():
    rows = []
    for path in product_files((".py",)):
        try:
            tree = ast.parse(path.read_bytes().decode("utf-8-sig"))
        except (SyntaxError, UnicodeDecodeError, ValueError):
            continue
        names = {PYTHON_CHECK}
        for node in ast.walk(tree):
            if isinstance(node, ast.ImportFrom):
                names.update(alias.asname for alias in node.names if alias.name == PYTHON_CHECK and alias.asname)
        parents = {child: parent for parent in ast.walk(tree) for child in ast.iter_child_nodes(parent)}
        for node in ast.walk(tree):
            if not isinstance(node, ast.Call):
                continue
            function = node.func
            name = function.id if isinstance(function, ast.Name) else getattr(function, "attr", None)
            if name not in names:
                continue
            enclosing = node
            while enclosing in parents and not isinstance(enclosing, (ast.FunctionDef, ast.AsyncFunctionDef)):
                enclosing = parents[enclosing]
            rows.append((path.relative_to(ROOT).as_posix(), node.lineno, node, enclosing))
    return rows


def environment_argument(call):
    keywords = {keyword.arg: keyword.value for keyword in call.keywords}
    if None in keywords or any(isinstance(argument, ast.Starred) for argument in call.args):
        return None
    if "environ" in keywords:
        return keywords["environ"]
    return call.args[1] if len(call.args) >= 2 else None


def test_python_metadata_check_call_count_is_pinned():
    rows = python_calls()
    assert len(rows) == EXPECTED_PYTHON_CALLS, "metadata check call sites changed; review each new call"


def test_every_python_metadata_check_call_passes_an_environment():
    missing = []
    for relative, line, call, enclosing in python_calls():
        argument = environment_argument(call)
        omitted = argument is None or (isinstance(argument, ast.Constant) and argument.value is None) or (
            isinstance(argument, ast.Dict) and not argument.keys)
        if not omitted and isinstance(argument, ast.Name) and isinstance(enclosing, ast.FunctionDef):
            # A parameter that defaults to None would be an empty environment again.
            arguments = enclosing.args
            positional = arguments.posonlyargs + arguments.args
            defaults = [None] * (len(positional) - len(arguments.defaults)) + list(arguments.defaults)
            pairs = list(zip(positional, defaults)) + list(zip(arguments.kwonlyargs, arguments.kw_defaults))
            for parameter, default in pairs:
                if parameter.arg == argument.id and isinstance(default, ast.Constant) and default.value is None:
                    omitted = True
        if omitted:
            missing.append(f"{relative}:{line}")
    assert not missing, "metadata check called without a usable environment: " + ", ".join(missing)


def test_python_metadata_check_never_reads_the_process_environment_itself():
    tree = ast.parse((ROOT / "failure_redaction.py").read_bytes().decode("utf-8-sig"))
    function = next(node for node in tree.body
                    if isinstance(node, ast.FunctionDef) and node.name == PYTHON_CHECK)
    default = function.args.defaults[-1]
    assert isinstance(default, ast.Constant) and default.value is None
    for node in ast.walk(function):
        assert not (isinstance(node, ast.Attribute) and node.attr in ("environ", "getenv", "environb"))


POWERSHELL_SCAN = r'''
$ErrorActionPreference = 'Stop'
$files = Get-Content -LiteralPath $env:LM_SCAN_FILES -Raw -Encoding UTF8 | ConvertFrom-Json
$name = $env:LM_SCAN_NAME
$rows = @()
foreach ($relative in $files) {
    $path = Join-Path $env:LM_SCAN_ROOT $relative
    $text = [Text.UTF8Encoding]::new($false).GetString([IO.File]::ReadAllBytes($path)).TrimStart([char]0xFEFF)
    $tokens = $null; $errors = $null
    $ast = [Management.Automation.Language.Parser]::ParseInput($text, [ref]$tokens, [ref]$errors)
    if ($errors.Count) { $rows += [ordered]@{ file = $relative; line = 0; kind = 'parse_error'; usable = $false }; continue }
    foreach ($command in $ast.FindAll({ param($n) $n -is [Management.Automation.Language.CommandAst] }, $true)) {
        if ($command.GetCommandName() -cne $name) { continue }
        $usable = $false
        $elements = @($command.CommandElements)
        for ($i = 1; $i -lt $elements.Count; $i++) {
            $element = $elements[$i]
            if ($element -is [Management.Automation.Language.CommandParameterAst] -and $element.ParameterName -ieq 'Environment') {
                $value = if ($null -ne $element.Argument) { $element.Argument } elseif ($i + 1 -lt $elements.Count) { $elements[$i + 1] } else { $null }
                $valueText = if ($null -ne $value) { $value.Extent.Text.Trim() } else { '' }
                $usable = $valueText -and $valueText -ne '$null' -and $valueText -ne '@{}'
            }
        }
        $rows += [ordered]@{ file = $relative; line = $command.Extent.StartLineNumber; kind = 'call'; usable = [bool]$usable }
    }
    foreach ($definition in $ast.FindAll({ param($n) $n -is [Management.Automation.Language.FunctionDefinitionAst] -and $n.Name -ceq $name }, $true)) {
        $readsProcess = @($definition.Body.FindAll({ param($n)
            ($n -is [Management.Automation.Language.InvokeMemberExpressionAst] -and $n.Member.Extent.Text -ceq 'GetEnvironmentVariables') -or
            ($n -is [Management.Automation.Language.VariableExpressionAst] -and $n.VariablePath.UserPath -like 'env:*') }, $true)).Count
        $parameter = @(@($definition.Parameters) + @(if ($definition.Body.ParamBlock) { $definition.Body.ParamBlock.Parameters }) | Where-Object { $_.Name.VariablePath.UserPath -ceq 'Environment' })
        $defaultIsNull = ($parameter.Count -eq 1 -and $null -ne $parameter[0].DefaultValue -and $parameter[0].DefaultValue.Extent.Text -eq '$null')
        $rows += [ordered]@{ file = $relative; line = $definition.Extent.StartLineNumber; kind = 'definition'
            usable = [bool]($readsProcess -eq 0 -and $defaultIsNull) }
    }
}
[Console]::Out.Write((ConvertTo-Json -InputObject @($rows) -Compress))
'''


def scan_powershell(tmp_path):
    executable = shutil.which("powershell.exe") or shutil.which("pwsh.exe")
    if not executable:
        pytest.skip("PowerShell is required to read parser tokens")
    files = [path.relative_to(ROOT).as_posix() for path in product_files((".ps1", ".psm1"))]
    listing = tmp_path / "scan-files.json"
    listing.write_text(json.dumps(files), encoding="utf-8")
    environment = dict(os.environ, LM_SCAN_FILES=str(listing), LM_SCAN_NAME=POWERSHELL_CHECK, LM_SCAN_ROOT=str(ROOT))
    driver = tmp_path / "scan.ps1"
    driver.write_text(POWERSHELL_SCAN, encoding="utf-8-sig")
    completed = subprocess.run([executable, "-NoLogo", "-NoProfile", "-NonInteractive", "-File", str(driver)],
                               env=environment, capture_output=True, creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0))
    assert completed.returncode == 0, "PowerShell parser scan failed"
    rows = json.loads(completed.stdout.decode("utf-8-sig"))
    return rows if isinstance(rows, list) else [rows]


def test_powershell_metadata_check_call_count_is_pinned_and_every_call_passes_an_environment(tmp_path):
    rows = scan_powershell(tmp_path)
    assert not [row for row in rows if row["kind"] == "parse_error"], "a product PowerShell file did not parse"
    calls = [row for row in rows if row["kind"] == "call"]
    assert len(calls) == EXPECTED_POWERSHELL_CALLS, "metadata check call sites changed; review each new call"
    missing = [f"{row['file']}:{row['line']}" for row in calls if not row["usable"]]
    assert not missing, "metadata check called without a usable -Environment: " + ", ".join(missing)


def test_powershell_metadata_check_defaults_to_an_empty_environment_and_never_reads_the_process(tmp_path):
    definitions = [row for row in scan_powershell(tmp_path) if row["kind"] == "definition"]
    assert len(definitions) == 1
    assert definitions[0]["usable"], "the check must default -Environment to $null and not read the process environment"
