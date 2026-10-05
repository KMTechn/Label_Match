import json
from pathlib import Path
import shutil
import subprocess
import sys

import pytest

REPO = Path(__file__).resolve().parents[1]
PROBE = 'import contextlib\nimport hashlib\nimport io\nimport json\nimport os\nfrom pathlib import Path\nimport secrets\nimport sys\n\nrepo, root, mode, state, output = sys.argv[1:]\nrepo, root, output = Path(repo), Path(root), Path(output)\nroot.mkdir(parents=True, exist_ok=False)\nfor name, part in {\n    \'LOCALAPPDATA\':\'local\', \'APPDATA\':\'roaming\', \'PROGRAMDATA\':\'program\',\n    \'LABEL_MATCH_SAVE_DIR\':\'data\', \'LABEL_MATCH_SETTINGS_PATH\':\'settings/input.json\',\n    \'LABEL_MATCH_LOGISTICS_PROFILE_PATH\':\'profile/input.json\',\n    \'LABEL_MATCH_DIRECT_SYNC_ROOT\':\'sync\', \'KMTECH_LABEL_WRITER_CONTROL_ROOT\':\'control\',\n    \'TEMP\':\'temp\', \'TMP\':\'temp\',\n}.items():\n    os.environ[name]=str(root/part)\nos.environ[\'KMTECH_LABEL_WRITER_TEST_MODE\']=\'1\'\nfor name in (\'LABEL_MATCH_ENROLLMENT_TLS_CA_BUNDLE_PATH\',\n             \'LABEL_MATCH_LOGISTICS_TLS_CA_BUNDLE_PATH\',\n             \'KMTECH_LABEL_CONFLICT_RESOLUTION_RECEIPT_PATH\',\n             \'LABEL_MATCH_PROTECTED_ADMIN_PROFILE\'):\n    os.environ.pop(name,None)\nsys.path.insert(0,str(repo))\nimport protected_admin\nimport label_data_generation as generation\nfrom kmtech_shared.data_generation import record_generation\nimport label_match_product_host as entry\nimport direct_sync_runtime as runtime\nfrom direct_sync_push import DEFAULT_ENDPOINT_PATH\nimport requests\nsubject=\'-\'.join(map(str,(\'S\',1,5,21,266,901,902,1001)))\nprotected_admin.current_process_sid=lambda:subject\nexternal=root/\'external\' if mode==\'explicit\' else root/\'local\'/\'CompanyProducerConnector\'\nexternal.mkdir(parents=True)\ntarget=external/\'secrets\'/(secrets.token_hex(8)+\'.dpapi\')\ntarget.parent.mkdir()\ntarget.write_bytes(secrets.token_bytes(32))\ncredential = root / "locator" / "credential.json"\ncredential.parent.mkdir()\npayload={\'producer_id\':secrets.token_hex(8),\'key_id\':secrets.token_hex(8),\n         \'secret_ref\':\'dpapi:\'+target.stem,\n         \'endpoint_url\':\'https://example.invalid\'+DEFAULT_ENDPOINT_PATH}\nif mode==\'explicit\': payload[\'secret_data_dir\']=str(external)\ncredential.write_text(json.dumps(payload),encoding=\'utf-8\')\nidentity=generation.release_identity(repo)\nmarker=external/\'data-generation.json\'\nif state!=\'absent\':\n    record_generation(marker,identity[\'version\'] if state==\'same\' else \'v999.0.0\',\n        app_id=generation.APP_ID,subject_sid=subject,build_id=str(identity[\'build_id\']))\n    if state==\'unknown\': marker.write_bytes(b\'{}\')\nevents={\'secret_reads\':0,\'decrypt_calls\':0,\'downstream_calls\':0,\'network_calls\':0,\'native_calls\':0}\ndef decrypt(_):\n    events[\'decrypt_calls\']+=1\n    return secrets.token_hex(32).encode()\nruntime._dpapi_unprotect_current_user=decrypt\ndef downstream(**kwargs):\n    events[\'downstream_calls\']+=1\n    return {\'status\':\'READY\'}\nentry.restore_relay_spool_from_server=downstream\ndef blocked_network(*a,**k):\n    events[\'network_calls\']+=1\n    raise AssertionError(\'network blocked\')\nrequests.sessions.Session.request=blocked_network\nimport winreg\ndef blocked_native(*a,**k):\n    events[\'native_calls\']+=1\n    raise AssertionError(\'native mutation blocked\')\nwinreg.SetValueEx=winreg.DeleteValue=blocked_native\ndef snapshot():\n    return {str(p.relative_to(root)):hashlib.sha256(p.read_bytes()).hexdigest()\n            for p in root.rglob(\'*\') if p.is_file()}\nos.environ[\'LABEL_MATCH_DIRECT_SYNC_ROOT\'] = str(credential.parent)\nbefore=snapshot()\ndirect_refused=False\ntry: generation.check_user_data_open(repo,(external,))\nexcept generation.DataGenerationError: direct_refused=True\noriginal=Path.open\ndef observed(path,mode=\'r\',*a,**k):\n    if Path(path) == credential and \'r\' in mode: events[\'locator_reads\'] = events.get(\'locator_reads\', 0) + 1\n    if Path(path).resolve()==target.resolve() and \'r\' in mode:events[\'secret_reads\']+=1\n    return original(path,mode,*a,**k)\nPath.open=observed\nstdout,stderr=io.StringIO(),io.StringIO()\nerror_type=\'\'\nwith contextlib.redirect_stdout(stdout),contextlib.redirect_stderr(stderr):\n    try:\n        code=entry.dispatch_product_mode([\'--onboard-current-user\', \'--app-root\', str(repo), \'--check-data-generation-only\'])\n    except Exception as exc:\n        code=-1;error_type=type(exc).__name__\nPath.open=original\nafter=snapshot()\nadded=set(after)-set(before)\nresult={\'mode\':mode,\'state\':state,\'code\':code,\'error_type\':error_type,\n        \'direct_external_refused\':direct_refused,**events,\'new_files\':len(added),\n        \'generation_records\':sum(n.endswith(\'data-generation.json\') or \'data-generation/\' in n.replace(\'\\\\\',\'/\') for n in added),\n        \'existing_changed\':sum(after.get(n)!=v for n,v in before.items()),\n        \'scope\':\'real operator main/guard/record/credential and secret reader; synthetic SID/decrypt/downstream; network/native blocked\'}\nresult[\'locator_reads\'] = events.get(\'locator_reads\', 0)\noutput.write_text(json.dumps(result,indent=2),encoding=\'utf-8\')\nprint(json.dumps(result))\n\nraise SystemExit(result[\'code\'] if result[\'code\'] is not None else 1)\n'


@pytest.mark.parametrize("shell", ["powershell", "pwsh"])
@pytest.mark.parametrize("mode", ["explicit", "default"])
@pytest.mark.parametrize("state", ["higher", "unknown", "same", "absent"])
def test_canonical_nested_admission_precedes_all_installer_changes(tmp_path, shell, mode, state):
    executable = shutil.which(shell)
    if executable is None:
        pytest.skip("PowerShell is unavailable")
    probe = tmp_path / "probe.py"
    probe.write_text(PROBE, encoding="utf-8")
    output = tmp_path / "nested.json"
    lifecycle = tmp_path / "lifecycle.json"
    application = tmp_path / "application"
    source = (REPO / "INSTALL_CANONICAL_PORTABLE.ps1").read_text(encoding="utf-8-sig")
    source = source[source.index("$source = Full $SourceRoot"):]
    admission = source[source.index("$generationProductRoot = Full"):source.index("$old = @(Relays)")]
    assert source.index("$generationProductRoot = Full") < source.index("Save $auditPath $audit")
    assert source.index("$generationProductRoot = Full") < source.index("Start-LabelWriterFence `", source.index("$before = Snapshot"))
    script = tmp_path / "installer.ps1"
    script.write_text(r"""param($Python,$Probe,$Repo,$Application,$Mode,$State,$Output,$Lifecycle)
$ErrorActionPreference = 'Stop'
$source = $Repo
$onboardingArguments = @()
$steps = @{fence=0;placement=0;audit=0;rollback=0;refused=$false}
function Full { param($Value,$Label) return $source }
function Same { param($Left,$Right) return [StringComparer]::OrdinalIgnoreCase.Equals($Left,$Right) }
function Product {
    param($Root,$ModeValue,$ExtraArguments)
    if ($ModeValue -cne '--onboard-current-user' -or $ExtraArguments -notcontains '--check-data-generation-only') { throw 'wrong product invocation' }
    & $Python -B $Probe $Repo $Application $Mode $State $Output > ($Output + '.stdout') 2> ($Output + '.stderr')
    if ($LASTEXITCODE -ne 0) { throw 'nested admission refused' }
}
try {
""" + admission + r"""
    $steps.fence++
    $steps.placement++
    $steps.audit++
    $steps.rollback++
} catch { $steps.refused = $true }
$steps | ConvertTo-Json -Compress | Set-Content -LiteralPath $Lifecycle -Encoding utf8
""", encoding="utf-8-sig")
    completed = subprocess.run([executable, "-NoProfile", "-File", str(script), sys.executable,
        str(probe), str(REPO), str(application), mode, state, str(output), str(lifecycle)],
        capture_output=True, timeout=60, creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0))
    assert completed.returncode == 0
    data = json.loads(output.read_text(encoding="utf-8-sig"))
    steps = json.loads(lifecycle.read_text(encoding="utf-8-sig"))
    refused = state in {"higher", "unknown"}
    assert steps["refused"] is refused
    assert steps["fence"] == int(not refused)
    assert steps["placement"] == int(not refused)
    assert steps["audit"] == int(not refused)
    assert steps["rollback"] == int(not refused)
    assert data["locator_reads"] == 1
    assert data["secret_reads"] == 0
    assert data["decrypt_calls"] == 0
    assert data["downstream_calls"] == 0
    assert data["network_calls"] == 0
    assert data["native_calls"] == 0
    assert data["new_files"] == 0
    assert data["generation_records"] == 0
    assert data["existing_changed"] == 0
    assert data["error_type"] == ""
