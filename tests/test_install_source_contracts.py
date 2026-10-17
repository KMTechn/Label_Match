"""Public source identity data and the onboarding nested writer call graph."""
import ast
import hashlib
import json
import re
from pathlib import Path

from tests.test_shared_ancestor_protection import run_functions

ROOT = Path(__file__).resolve().parents[1]


def test_real_generated_launcher_keeps_payload_param_before_redaction(tmp_path):
    result = run_functions(tmp_path, ROOT / 'INSTALL_CANONICAL_PORTABLE.ps1',
                           ['ByteSha', 'InvokeFrozenPlacementHelper'], r'''
$root=$env:LM_PARAM_BINDING_ROOT
$script:observed=$false
$frozen=[pscustomobject]@{root=$root;helper_path=(Join-Path $root 'helper.ps1');helper_sha256=('a'*64);
 integrity_sha256=('b'*64);writer_fence_path=(Join-Path $root 'writer.ps1');writer_fence_sha256=('c'*64);
 data_generation_path=(Join-Path $root 'generation.ps1');data_generation_sha256=('d'*64)}
function Start-Process {
 param($FilePath,$Verb,$ArgumentList,$WindowStyle,[switch]$Wait,[switch]$PassThru)
 $launcher=@(Get-ChildItem -LiteralPath $root -Filter 'placement-child-*.ps1')
 if($launcher.Count -ne 1){throw 'Generated launcher is missing or ambiguous'}
 $tokens=$null;$errors=$null
 $ast=[Management.Automation.Language.Parser]::ParseFile($launcher[0].FullName,[ref]$tokens,[ref]$errors)
 if($errors.Count -or $null -eq $ast.ParamBlock -or $ast.ParamBlock.Parameters.Count -ne 1){throw 'Redaction prelude broke the payload param block'}
 $parameter=$ast.ParamBlock.Parameters[0]
 if($parameter.Name.VariablePath.UserPath -cne 'PayloadBytes' -or $parameter.StaticType -ne [byte[]]){throw 'Payload binding contract changed'}
 $script:observed=$true
 throw [ComponentModel.Win32Exception]::new(1223)
}
try {[void](InvokeFrozenPlacementHelper $frozen @{ElevationLogPath=(Join-Path $root 'placement')})}catch{}
if(-not $script:observed){throw 'Actual generated launcher did not retain its typed payload param'}
'PASS'
''', values={'LM_PARAM_BINDING_ROOT': str(tmp_path)})
    assert result.returncode == 0, 'Generated launcher param regression failed in isolated root'
    assert result.stdout.strip() == 'PASS'


def test_official_release_entries_match_product_constants(tmp_path):
    entries = json.loads((ROOT / "tests/fixtures/official_legacy_relay_sources.json").read_text())
    output = tmp_path / "identity.json"
    result = run_functions(tmp_path, ROOT / "INSTALL_THIS_PC.ps1", [
        'Get-LabelOfficialLegacyTaskExpectation', 'Get-LabelOfficialLegacyPrincipalModes',
        'Get-LabelOfficialLegacyTaskExpectations'], r'''
$env:COMPUTERNAME='SYNTHETIC-PC';$env:SystemRoot='C:\Windows'
function Get-ItemProperty {param($Path,$Name,$ErrorAction);return @{MachineGuid='SYNTHETIC-MACHINE-GUID'}}
[IO.File]::WriteAllText($env:LM_EXPECTATION_OUTPUT,(@(Get-LabelOfficialLegacyTaskExpectations)|ConvertTo-Json -Depth 8),[Text.UTF8Encoding]::new($false))
''', values={"LM_EXPECTATION_OUTPUT":str(output)})
    assert result.returncode == 0, result.stdout + result.stderr
    actual = json.loads(output.read_text())
    suffix = hashlib.sha256(b'SYNTHETIC-MACHINE-GUID').hexdigest()[:12]
    bindings = {"program_data":r"c:\programdata", "system_directory":r"c:\windows\system32",
                "label_match_source_host_id":f"label-match-synthetic-pc-{suffix}", "current_user":"CURRENT_USER"}
    expected = []
    for entry in entries:
        fields = {}
        for key, value in entry["field_templates"].items():
            for name, bound in bindings.items():
                value = value.replace("{" + name + "}",bound)
            if key.startswith(('task.','action.command','action.arguments')):
                value = value.lower() if key != 'action.arguments' else '//B //NoLogo ' + value[len('//B //NoLogo '):].lower()
            fields[key] = value
        assert all('{' not in value for value in fields.values())
        assert fields in actual, entry['tag'] + ':' + entry['branch']
        assert set(entry['source_refs']) == set(fields)
        expected.append(fields)
    assert all(item in expected for item in actual), 'Product must not admit an unpublished source identity'


def test_restore_delegation_covers_onboarding_nested_writer_calls():
    text = (ROOT / 'INSTALL_CANONICAL_PORTABLE.ps1').read_text(encoding='utf-8-sig')
    block = re.search(r'\$restoreSources = @\((.*?)\n\)',text,re.S).group(1)
    permitted = set(re.findall(r"'([^']+)'",block))
    modules = {}
    def load(module):
        if module in modules:
            return modules[module]
        path = ROOT / (module.replace('.', '/') + '.py')
        if not path.is_file():
            return None
        tree = ast.parse(path.read_text(encoding='utf-8-sig'))
        functions = {node.name:node for node in tree.body if isinstance(node,ast.FunctionDef)}
        imports = {}
        for node in ast.walk(tree):
            if isinstance(node,ast.ImportFrom) and node.module:
                for item in node.names:
                    imports[item.asname or item.name] = (node.module,item.name)
        modules[module] = (functions,imports)
        return modules[module]
    visited, needed = set(), set()
    def walk(module,name):
        if (module,name) in visited:
            return
        visited.add((module,name))
        # contracts.md writer admission; aedcbca makes this diagnostic best effort.
        if (module, name) == ('auto_update', 'emit_support_record'):
            return
        data = load(module)
        if not data or name not in data[0]:
            return
        functions, imports = data
        node = functions[name]
        for decorator in node.decorator_list:
            if isinstance(decorator,ast.Call) and isinstance(decorator.func,ast.Name) and decorator.func.id=='writer_sink':
                needed.add(ast.literal_eval(decorator.args[0]))
        for call in ast.walk(node):
            if isinstance(call,ast.Call) and isinstance(call.func,ast.Name):
                callee = call.func.id
                if callee in functions:
                    walk(module,callee)
                elif callee in imports:
                    walk(*imports[callee])
    walk('current_user_onboarding','onboard_current_user')
    assert 'auto_update_fixed_helper' in needed, 'The pre-report lifecycle Verify call must be included'
    assert needed <= permitted, 'Nested writer sources missing from canonical restoration: ' + repr(sorted(needed-permitted))


def test_restore_diagnostic_refusal_preserves_original_public_report(tmp_path, monkeypatch, capsys):
    """The diagnostic boundary excluded above must retain the original refusal."""
    import auto_update as updates
    from writer_session_fence import WriterFenceError

    def refuse_diagnostic(*_args, **_kwargs):
        raise WriterFenceError("diagnostic fixture refused")

    monkeypatch.setattr(updates, "write_json_durable", refuse_diagnostic)
    fields = {"status": "FAILED", "reason_code": "INSTALLER_FAILED"}
    updates.emit_support_record(fields, stage="SUPPORT", policy={"staging_root": str(tmp_path)})
    reports = [json.loads(line) for line in capsys.readouterr().out.splitlines()]
    assert reports[-1] == updates.support_record(fields, stage="SUPPORT")
    assert not (tmp_path / "startup-support.json").exists()
