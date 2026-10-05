"""Actual isolated operator admission and file reads in child processes."""
import ast
import json
import os
from pathlib import Path
import subprocess
import sys

import pytest


def _observe(tmp_path, mode, state, variant="nested"):
    tree = ast.parse(_PROBE)
    if variant in {"locator", "protected_parent"}:
        marker = next(n for n in tree.body if isinstance(n, ast.Assign)
            and any(isinstance(x, ast.Name) and x.id == "marker" for x in n.targets))
        marker.value.left = (ast.Attribute(ast.Name("credential", ast.Load()), "parent", ast.Load())
            if variant == "locator" else ast.BinOp(ast.Name("external", ast.Load()), ast.Div(), ast.Constant("secrets")))
    if variant in {"malformed", "duplicate"}:
        before = next(n for n in tree.body if isinstance(n, ast.Assign)
            and any(isinstance(x, ast.Name) and x.id == "before" for x in n.targets))
        invalid = "{" if variant == "malformed" else '{"producer_id":"a","producer_id":"b"}'
        tree.body.insert(tree.body.index(before), ast.Expr(ast.Call(
            ast.Attribute(ast.Name("credential", ast.Load()), "write_text", ast.Load()),
            [ast.Constant(invalid)], [ast.keyword("encoding", ast.Constant("utf-8"))])))
    script = ast.unparse(ast.fix_missing_locations(tree))
    output = tmp_path / "observations.json"
    process = subprocess.run([sys.executable, "-B", "-c", script,
        str(Path(__file__).resolve().parents[1]), str(tmp_path / "input"), mode, state, str(output)],
        capture_output=True, env=dict(os.environ), creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0))
    assert process.returncode == 0
    return json.loads(output.read_text(encoding="utf-8"))


@pytest.mark.parametrize("mode", ["explicit", "default"])
@pytest.mark.parametrize("state", ["higher", "unknown", "same", "absent"])
def test_nested_credential_owner_before_reads_and_records(tmp_path, mode, state):
    result = _observe(tmp_path, mode, state)
    assert result["locator_reads"] == 1
    assert result["existing_changed"] == 0
    assert result["native_calls"] == result["network_calls"] == 0
    if state in {"higher", "unknown"}:
        assert result["code"] == 2
        assert result["secret_reads"] == result["decrypt_calls"] == result["downstream_calls"] == 0
        assert result["generation_records"] == result["new_files"] == 0
    else:
        assert result["code"] == 1  # Existing downstream stand-in after credential success.
        assert result["secret_reads"] == result["decrypt_calls"] == result["downstream_calls"] == 1
        assert result["generation_records"] == 4
        assert result["new_files"] == 5


@pytest.mark.parametrize("variant", ["locator", "protected_parent"])
def test_credential_parent_refused_before_payload_and_records(tmp_path, variant):
    result = _observe(tmp_path, "explicit", "higher", variant)
    assert result["code"] == 2
    assert result["locator_reads"] == (0 if variant == "locator" else 1)
    assert result["secret_reads"] == result["decrypt_calls"] == result["downstream_calls"] == 0
    assert result["generation_records"] == result["new_files"] == result["existing_changed"] == 0


@pytest.mark.parametrize("variant", ["malformed", "duplicate"])
def test_locator_validation_keeps_existing_operator_report(tmp_path, variant):
    result = _observe(tmp_path, "explicit", "same", variant)
    assert result["code"] == 1
    assert result["locator_reads"] == 1
    assert result["secret_reads"] == result["decrypt_calls"] == result["downstream_calls"] == 0
    assert result["generation_records"] == 4
    assert result["new_files"] == 5
    assert result["existing_changed"] == 0

_PROBE = "import contextlib\nimport hashlib\nimport io\nimport json\nimport os\nfrom pathlib import Path\nimport secrets\nimport sys\n\nrepo, root, mode, state, output = sys.argv[1:]\nrepo, root, output = Path(repo), Path(root), Path(output)\nroot.mkdir(parents=True, exist_ok=False)\nfor name, part in {\n    'LOCALAPPDATA':'local', 'APPDATA':'roaming', 'PROGRAMDATA':'program',\n    'LABEL_MATCH_SAVE_DIR':'data', 'LABEL_MATCH_SETTINGS_PATH':'settings/input.json',\n    'LABEL_MATCH_LOGISTICS_PROFILE_PATH':'profile/input.json',\n    'LABEL_MATCH_DIRECT_SYNC_ROOT':'sync', 'KMTECH_LABEL_WRITER_CONTROL_ROOT':'control',\n    'TEMP':'temp', 'TMP':'temp',\n}.items():\n    os.environ[name]=str(root/part)\nos.environ['KMTECH_LABEL_WRITER_TEST_MODE']='1'\nfor name in ('LABEL_MATCH_ENROLLMENT_TLS_CA_BUNDLE_PATH',\n             'LABEL_MATCH_LOGISTICS_TLS_CA_BUNDLE_PATH',\n             'KMTECH_LABEL_CONFLICT_RESOLUTION_RECEIPT_PATH',\n             'LABEL_MATCH_PROTECTED_ADMIN_PROFILE'):\n    os.environ.pop(name,None)\nsys.path.insert(0,str(repo))\nimport protected_admin\nimport label_data_generation as generation\nfrom kmtech_shared.data_generation import record_generation\nfrom tools import direct_sync_relay_operator as entry\nimport direct_sync_runtime as runtime\nfrom direct_sync_push import DEFAULT_ENDPOINT_PATH\nimport requests\nsubject='-'.join(map(str,('S',1,5,21,266,901,902,1001)))\nprotected_admin.current_process_sid=lambda:subject\nexternal=root/'external' if mode=='explicit' else root/'local'/'CompanyProducerConnector'\nexternal.mkdir(parents=True)\ntarget=external/'secrets'/(secrets.token_hex(8)+'.dpapi')\ntarget.parent.mkdir()\ntarget.write_bytes(secrets.token_bytes(32))\ncredential=root/'credential'/'input.json'\ncredential.parent.mkdir()\npayload={'producer_id':secrets.token_hex(8),'key_id':secrets.token_hex(8),\n         'secret_ref':'dpapi:'+target.stem,\n         'endpoint_url':'https://example.invalid'+DEFAULT_ENDPOINT_PATH}\nif mode=='explicit': payload['secret_data_dir']=str(external)\ncredential.write_text(json.dumps(payload),encoding='utf-8')\nidentity=generation.release_identity(repo)\nmarker=external/'data-generation.json'\nif state!='absent':\n    record_generation(marker,identity['version'] if state=='same' else 'v999.0.0',\n        app_id=generation.APP_ID,subject_sid=subject,build_id=str(identity['build_id']))\n    if state=='unknown': marker.write_bytes(b'{}')\nevents={'secret_reads':0,'decrypt_calls':0,'downstream_calls':0,'network_calls':0,'native_calls':0}\ndef decrypt(_):\n    events['decrypt_calls']+=1\n    return secrets.token_hex(32).encode()\nruntime._dpapi_unprotect_current_user=decrypt\ndef downstream(**kwargs):\n    events['downstream_calls']+=1\n    return {'status':'READY'}\nentry.restore_relay_spool_from_server=downstream\ndef blocked_network(*a,**k):\n    events['network_calls']+=1\n    raise AssertionError('network blocked')\nrequests.sessions.Session.request=blocked_network\nimport winreg\ndef blocked_native(*a,**k):\n    events['native_calls']+=1\n    raise AssertionError('native mutation blocked')\nwinreg.SetValueEx=winreg.DeleteValue=blocked_native\ndef snapshot():\n    return {str(p.relative_to(root)):hashlib.sha256(p.read_bytes()).hexdigest()\n            for p in root.rglob('*') if p.is_file()}\nbefore=snapshot()\ndirect_refused=False\ntry: generation.check_user_data_open(repo,(external,))\nexcept generation.DataGenerationError: direct_refused=True\noriginal=Path.open\ndef observed(path,mode='r',*a,**k):\n    if Path(path) == credential and 'r' in mode: events['locator_reads'] = events.get('locator_reads', 0) + 1\n    if Path(path).resolve()==target.resolve() and 'r' in mode:events['secret_reads']+=1\n    return original(path,mode,*a,**k)\nPath.open=observed\nstdout,stderr=io.StringIO(),io.StringIO()\nerror_type=''\nwith contextlib.redirect_stdout(stdout),contextlib.redirect_stderr(stderr):\n    try:\n        code=entry.main(['restore-spool','--db-path',str(root/'db'/'queue.sqlite'),\n            '--relay-id',secrets.token_hex(8),'--spool-root',str(root/'spool'),\n            '--credential-path',str(credential),'--operator-id',secrets.token_hex(8),\n            '--reason','isolated review','--report-path',str(root/'report.json')])\n    except Exception as exc:\n        code=-1;error_type=type(exc).__name__\nPath.open=original\nafter=snapshot()\nadded=set(after)-set(before)\nresult={'mode':mode,'state':state,'code':code,'error_type':error_type,\n        'direct_external_refused':direct_refused,**events,'new_files':len(added),\n        'generation_records':sum(n.endswith('data-generation.json') or 'data-generation/' in n.replace('\\\\','/') for n in added),\n        'existing_changed':sum(after.get(n)!=v for n,v in before.items()),\n        'scope':'real operator main/guard/record/credential and secret reader; synthetic SID/decrypt/downstream; network/native blocked'}\nresult['locator_reads'] = events.get('locator_reads', 0)\noutput.write_text(json.dumps(result,indent=2),encoding='utf-8')\nprint(json.dumps(result))\n"
