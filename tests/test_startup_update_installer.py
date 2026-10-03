"""Injected installer boundaries: no task registration, process start or lifecycle."""
from pathlib import Path
import json
import re
import subprocess

import pytest
import auto_update

ROOT = Path(__file__).resolve().parents[1]


def quote(value):
    return "'" + str(value).replace("'", "''") + "'"


def functions(path, names):
    # PowerShell's parser supplies only function definitions; installer top-level
    # code is never evaluated. The returned bodies may then be tested with mocks.
    names = ['ConvertTo-LabelTaskSid', *names]
    return f"""
$tokens=$null; $errors=$null
$tree=[Management.Automation.Language.Parser]::ParseFile({quote(ROOT/path)},[ref]$tokens,[ref]$errors)
if ($errors.Count) {{ throw 'Installer parse failed' }}
$wanted=@({','.join(quote(n) for n in names)})
foreach ($node in $tree.FindAll({{param($a) $a -is [Management.Automation.Language.FunctionDefinitionAst]}},$true)) {{
    if ($node.Name -in $wanted) {{ Invoke-Expression $node.Extent.Text }}
}}
"""


def ps(tmp_path, code):
    path = tmp_path / "boundary.ps1"
    path.write_text("$ErrorActionPreference='Stop'\n"+code, encoding="utf-8-sig")
    result = subprocess.run(["powershell.exe", "-NoProfile", "-NonInteractive", "-File", str(path)],
                            cwd=tmp_path, capture_output=True, text=True)
    assert result.returncode == 0, result.stdout + result.stderr
    return result.stdout


@pytest.mark.parametrize("change", [None, "extra_action", "extra_trigger", "principal", "hard_terminate", "hidden", "arguments"])
def test_owned_task_definition_refuses_foreign_or_dangerous_replacement(tmp_path, change):
    xml = auto_update.logon_task_xml(app_root=tmp_path/"current", operator_sid="S-1-5-21-1-2-3-1001")
    expected = tmp_path/"expected.xml"
    expected.write_text(xml, encoding="utf-8")
    mutation = {
        None: "",
        "extra_action": "$copy=$a.Task.Actions.Exec.CloneNode($true); [void]$a.Task.Actions.AppendChild($copy)",
        "extra_trigger": "$copy=$a.Task.Triggers.LogonTrigger.CloneNode($true); [void]$a.Task.Triggers.AppendChild($copy)",
        "principal": "$a.Task.Principals.Principal.UserId='S-1-5-19'",
        "hard_terminate": "$a.Task.Settings.AllowHardTerminate='true'",
        "hidden": "$a.Task.Settings.Hidden='false'",
        "arguments": "$a.Task.Actions.Exec.Arguments='foreign command'",
    }[change]
    out = ps(tmp_path, functions("INSTALL_THIS_PC.ps1", ["Assert-LabelStartupUpdateTaskDefinition"])+f"""
$text=Get-Content -LiteralPath {quote(expected)} -Raw
$a=[xml]$text; $b=[xml]$text
{mutation}
try {{ Assert-LabelStartupUpdateTaskDefinition $a $b; 'PASS' }} catch {{ 'REFUSED' }}
""")
    assert out.strip() == ("PASS" if change is None else "REFUSED")


@pytest.mark.parametrize("child,accepted", [
    ({"sid":"same","session_id":1,"elevated":False,"integrity_rid":8192},True),
    ({"sid":"other","session_id":1,"elevated":False,"integrity_rid":8192},False),
    ({"sid":"same","session_id":2,"elevated":False,"integrity_rid":8192},False),
    ({"sid":"same","session_id":1,"elevated":True,"integrity_rid":12288},False),
])
def test_forward_and_rollback_share_verified_shell_launch(tmp_path, child, accepted):
    child_path=tmp_path/"child.json"
    child_path.write_text(json.dumps(child),encoding="utf-8")
    out=ps(tmp_path,functions("INSTALL_CANONICAL_PORTABLE.ps1",["StartRaw"])+f"""
$script:called=0
function Get-CanonicalRelaySecurity([int]$ProcessId) {{
    if ($ProcessId -eq $PID) {{ return @{{sid='same';session_id=1;elevated=$true;integrity_rid=12288}} }}
    return Get-Content -LiteralPath {quote(child_path)} -Raw | ConvertFrom-Json
}}
function Start-CanonicalLimitedRelay($Line,$UserSid,$SessionId) {{
    if ($UserSid -cne 'same' -or $SessionId -ne 1) {{ throw 'Wrong shell' }}
    $script:called++; return 99999
}}
function Invoke-CimMethod {{ throw 'Installer-token process start forbidden' }}
try {{ [void](StartRaw 'fixed protected relay command'); 'PASS' }} catch {{ 'REFUSED' }}
if ($script:called -ne 1) {{ throw 'Limited launch bypassed' }}
""")
    assert out.strip()==("PASS" if accepted else "REFUSED")


def test_normal_stop_waits_for_exact_preimage_exit_and_refuses_foreign_writer(tmp_path):
    out=ps(tmp_path,functions("INSTALL_CANONICAL_PORTABLE.ps1",["Wait-LabelRelayPreimageExit"])+"""
function Same($a,$b) { return $a -ceq $b }
function Normalize-RelayExecutableQuoting($line,$exe) { return $line }
function Start-Sleep { }
$script:count=0
$old=[pscustomobject]@{ExecutablePath='old/pythonw.exe';CommandLine='old actual command'}
function UnquiescedProductWriters { $script:count++; if ($script:count -lt 3) { return $old } }
Wait-LabelRelayPreimageExit @($old)
if ($script:count -ne 3) { throw 'Lease/process race was not awaited' }
function UnquiescedProductWriters { return [pscustomobject]@{ExecutablePath='foreign';CommandLine='foreign'} }
try { Wait-LabelRelayPreimageExit @($old); throw 'Foreign accepted' } catch {
    if ($_.Exception.Message -cne 'Unexpected writer remains after normal removal.') { throw }
}
'PASS'
""")
    assert out.strip()=="PASS"


def test_launch_failure_keeps_native_call_code_chain_without_messages(tmp_path):
    out=ps(tmp_path,functions("INSTALL_CANONICAL_PORTABLE.ps1",["Get-LabelLaunchFailure"])+"""
$native=New-Object ComponentModel.Win32Exception(5,'do not emit this message')
$native.Data['call']='DuplicateTokenEx:primary'
$outer=New-Object Exception('do not emit arguments',$native)
$failure=[pscustomobject]@{Exception=$outer}
Get-LabelLaunchFailure $failure 'rollback' | ConvertTo-Json -Depth 8 -Compress
""")
    payload=json.loads(out)
    assert payload['phase']=='rollback'
    assert payload['exception_chain'][1]['call']=='DuplicateTokenEx:primary'
    assert payload['exception_chain'][1]['win32_error']==5
    assert 'do not emit' not in out


def test_old_run_and_active_version_commands_keep_separate_roots(tmp_path):
    current=tmp_path/'current'; current.mkdir()
    sha='a'*40
    version=tmp_path/'versions'/sha
    (version/'app').mkdir(parents=True); (version/'app/main.py').touch()
    control=tmp_path/'update-control'; control.mkdir()
    (control/'active.json').write_text(json.dumps({'schema':auto_update.SCHEMA,'sha':sha}),encoding='utf-8')
    out=ps(tmp_path,functions("INSTALL_CANONICAL_PORTABLE.ps1",["Command","RelayCodeRoot"])+f"""
function Arg($value) {{ return '"'+$value+'"' }}
$old=Command {quote(current)}
if ($old -match '--bootstrap') {{ throw 'Old Run recalculated as candidate' }}
Set-Content -LiteralPath {quote(control/'policy.json')} -Value '{{}}'
$fixed=Command {quote(current)}
$code=RelayCodeRoot {quote(current)}
$relay=Command $code
if ($fixed -notmatch '--bootstrap' -or $relay -match '--bootstrap' -or $code -cne {quote(version)}) {{ throw 'Run/code roots differ incorrectly' }}
'PASS'
""")
    assert out.strip()=='PASS'


def test_native_launcher_compiles_without_invoking_it(tmp_path):
    out=ps(tmp_path,functions("INSTALL_CANONICAL_PORTABLE.ps1",["Initialize-CanonicalUserProcessApi"])+"""
Initialize-CanonicalUserProcessApi
if (-not ('LabelMatchCanonicalUserProcess' -as [type])) { throw 'Native launcher missing' }
'COMPILED_NO_NATIVE_CALL'
""")
    assert out.strip()=='COMPILED_NO_NATIVE_CALL'


def test_all_installer_relay_launch_sites_share_the_security_boundary():
    source=(ROOT/'INSTALL_CANONICAL_PORTABLE.ps1').read_text(encoding='utf-8')
    assert 'DuplicateTokenEx(token,0x18B,' in source
    assert 'DuplicateTokenEx(token,11,' not in source
    # Forward, rollback-old and Run-recovery starts use the same StartRaw function.
    calls=re.findall(r'(?m)^.*(?:=|\[void\]\()\s*StartRaw\s+([^\n]+)',source)
    assert len(calls)>=2
    restore=re.search(r'\$restoreSources = @\((.*?)\n\)',source,re.S).group(1)
    sources=re.findall(r"'([^']+)'",restore)
    assert len(sources)==len(set(sources))
    assert {'auto_update_task_mutation','auto_update_restore_preimage'} <= set(sources)


def test_company_feed_requires_existing_authenticated_profile():
    assert auto_update.policy_from_settings({'feed_url':'https://company.test/lm'})['profile_auth'] is True
    with pytest.raises(ValueError,match='existing device authentication'):
        auto_update.policy_from_settings({'feed_url':'https://company.test/lm','profile_auth':False})


def test_python_launch_failure_omits_messages_and_keeps_call_and_code():
    native=OSError(5,'DuplicateTokenEx:primary')
    outer=RuntimeError('do not emit arguments')
    outer.__cause__=native
    details=auto_update.launch_failure_details(outer)
    assert details[1]['call']=='DuplicateTokenEx:primary'
    assert details[1]['win32_error']==5
    assert 'do not emit' not in json.dumps(details)
    assert auto_update.launch_failure_details(OSError(2,'private path or message'))[0]['call']==''


def test_task_query_failure_is_unknown_and_foreign_folder_is_preserved(tmp_path):
    out=ps(tmp_path,functions('INSTALL_THIS_PC.ps1',['Get-LabelStartupUpdateTask'])+"""
function Get-ScheduledTask { throw 'query denied' }
try { [void](Get-LabelStartupUpdateTask 'owned'); throw 'Denied query treated as absent' }
catch { if ($_.Exception.Message -cne 'query denied') { throw } }
function Get-ScheduledTask { return @([pscustomobject]@{TaskName='owned';TaskPath='\\foreign\\'},[pscustomobject]@{TaskName='other';TaskPath='\\'}) }
if ($null -ne (Get-LabelStartupUpdateTask 'owned')) { throw 'Foreign task adopted' }
'PASS'
""")
    assert out.strip()=='PASS'


@pytest.mark.parametrize('delegated',[True,False])
def test_exact_deployment_delegation_owns_admission_and_denial_releases_lease(monkeypatch,tmp_path,delegated):
    from types import SimpleNamespace
    import writer_session_fence as fence
    released=[];checked=[]
    lease=SimpleNamespace(abandoned=False,release=lambda:released.append(True))
    monkeypatch.setattr(fence,'_acquire_named_mutex',lambda *_args:lease)
    monkeypatch.setattr(fence,'_admitted_writer_sources',lambda *_args,**_kwargs:frozenset({'current_user_onboarding'}) if delegated else None)
    def guard(*args,**kwargs):
        checked.append(True)
        raise ValueError('ordinary version differs')
    monkeypatch.setattr(auto_update,'assert_version_writer',guard)
    if delegated:
        with fence.writer_admission('current_user_onboarding',control_root=tmp_path,environ={}):pass
        assert checked==[]
    else:
        with pytest.raises(ValueError,match='ordinary version differs'):
            with fence.writer_admission('current_user_onboarding',control_root=tmp_path,environ={}):pass
        assert checked==[True]
    assert released==[True]


@pytest.mark.parametrize('action',['replacement','uninstall'])
def test_supported_lifecycle_preflight_preserves_all_state_until_action(tmp_path,action):
    current=tmp_path/'current';current.mkdir();control=tmp_path/'update-control';control.mkdir()
    for path,value in [(current/'portable-manifest.json',{'source_commit':'a'*40}),
                       (control/'policy.json',{}),(control/'active.json',{'sha':'b'*40}),
                       (control/'transition.json',{'phase':'CONFIRMED'})]:
        path.write_text(json.dumps(value),encoding='utf-8')
    data=tmp_path/'data';data.mkdir();(data/'outbox.json').write_bytes(b'unique unfinished outbox')
    before={p:p.read_bytes() for p in [*control.iterdir(),current/'portable-manifest.json',data/'outbox.json']}
    # The signed-pair/lifecycle requirements retire the old unconditional A/B
    # refusal. Corrupt control still refuses installation, while removal's
    # preflight never reads it; actual content moves are tested separately.
    if action == 'replacement':
        with pytest.raises((ValueError, OSError)):
            auto_update.resolve_lifecycle_context(tmp_path, operation='install')
    else:
        assert auto_update.resolve_lifecycle_context(tmp_path, operation='uninstall')['lifecycle_state'] == 'REMOVAL_READY'
    assert before=={p:p.read_bytes() for p in before}


@pytest.mark.parametrize('change',[None,'http','github','unknown','apply','format','auth'])
def test_installer_validates_packet_settings_before_preimage_or_task_mutation(tmp_path,change):
    value={'feed_url':'https://company.test/lm/feed','channel':'stable','profile_auth':True,'apply_during_work':False}
    if change=='http':value['feed_url']='http://company.test/lm/feed'
    elif change=='github':value['feed_url']='https://github.com/org/feed'
    elif change=='unknown':value['unexpected']=True
    elif change=='apply':value['apply_during_work']=True
    elif change=='format':value['profile_auth']='true'
    elif change=='auth':value['profile_auth']=False
    path=tmp_path/'settings.json';path.write_text(json.dumps(value),encoding='utf-8')
    out=ps(tmp_path,functions('INSTALL_THIS_PC.ps1',['Assert-LabelStartupUpdateSettings'])+f"""
$settings=Get-Content -LiteralPath {quote(path)} -Raw | ConvertFrom-Json
try {{ Assert-LabelStartupUpdateSettings $settings; 'PASS' }} catch {{ 'REFUSED' }}
""")
    assert out.strip()==('PASS' if change is None else 'REFUSED')
    source=(ROOT/'INSTALL_THIS_PC.ps1').read_text(encoding='utf-8')
    assert source.index('Assert-LabelStartupUpdateSettings $settings') < source.index("Save-LabelStartupUpdatePreimage $Root 'install'")
