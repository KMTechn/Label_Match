import json
import re
from pathlib import Path
import shutil
import subprocess
import sys

import pytest
import auto_update as update
import writer_session_fence as fence
from tests.test_supported_lifecycle import healthy

ROOT = Path(__file__).resolve().parents[1]


@pytest.mark.parametrize('engine',['powershell.exe','pwsh.exe'])
@pytest.mark.parametrize('scenario',['same','new','restore',
    'forward-token','forward-tuple','forward-source','forward-expiry',
    'restore-token','restore-tuple','restore-source','restore-expiry'])
def test_actual_machine_parent_passes_scoped_authority_to_both_readbacks(tmp_path,monkeypatch,engine,scenario):
    fixed,current,control,_verified = healthy(tmp_path/'machine',monkeypatch)
    policy=update.read_json(control/'policy.json')
    policy['operator_local_app_data']=str(tmp_path/'worker-local')
    (control/'policy.json').write_text(json.dumps(policy))
    before_sha = update.read_json(control/'active.json')['sha']
    selected = before_sha if scenario=='same' else ('d'*40 if before_sha != 'd'*40 else 'e'*40)
    assert scenario == 'same' or selected != before_sha
    candidate = control.parent/'versions'/selected
    if not candidate.exists():
        shutil.copytree(current,candidate)
        manifest = update.read_json(candidate/'portable-manifest.json')
        manifest['source_commit'] = selected
        (candidate/'portable-manifest.json').write_text(json.dumps(manifest))
    stock = tmp_path/'stock'
    shutil.copytree(candidate,stock)
    manifest=update.read_json(stock/'portable-manifest.json')
    manifest['startup_update_protocol']=1
    (stock/'portable-manifest.json').write_text(json.dumps(manifest))
    config = dict(managed=str(control.parent),source=str(stock),before_sha=before_sha,
        selected=selected,prior=str(current),scenario=scenario,inventory=fence.WRITER_INVENTORY_SHA256)
    (tmp_path/'case.json').write_text(json.dumps(config))
    executable = shutil.which(engine)
    assert executable is not None
    child = subprocess.run([executable,'-NoLogo','-NoProfile','-NonInteractive','-ExecutionPolicy','Bypass',
        '-File',str(ROOT/'tests/lm_machine_parent_lifecycle.ps1'),'-Repo',str(ROOT),'-Root',str(tmp_path),
        '-Python',sys.executable],capture_output=True,text=True,creationflags=subprocess.CREATE_NO_WINDOW)
    (tmp_path/'child-metadata.json').write_text(json.dumps(dict(rc=child.returncode,out_len=len(child.stdout),err_len=len(child.stderr),errors=re.findall(r'(?:FullyQualifiedErrorId\s*:\s*|\[)([A-Za-z_][A-Za-z_0-9.,]+)',child.stderr),lines=re.findall(r'(?:line|:)([0-9]+)',child.stderr))))
    assert child.returncode==0 and child.stdout.splitlines()[-1]=='PASS'
    rows=[json.loads(line) for line in (tmp_path/'readback-results.jsonl').read_text().splitlines()]
    assert all(row['default_root_restored'] for row in rows)
    if scenario in {'same','new'}:
        assert len(rows)==1 and rows[0]['rc']==0 and rows[0]['verify_root_bound']
    elif scenario=='restore':
        assert len(rows)==2 and all(row['rc']==0 and row['verify_root_bound'] for row in rows)
    elif scenario.startswith('forward-'):
        assert [row['rc'] for row in rows]==[1,0] and rows[1]['verify_root_bound']
    else:
        assert [row['rc'] for row in rows]==[0,1] and rows[0]['verify_root_bound']
