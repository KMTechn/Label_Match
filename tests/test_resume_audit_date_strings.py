"""Independent terminal observation; full ordinary-resume proof awaits lane answer."""
from pathlib import Path
import copy
import hashlib
import json
import re
import shutil
import subprocess

import pytest
import writer_session_fence as fence
from tests.test_dead_installer_fence_entries import carried, failed_install, fence_only_removal

REPO = Path(__file__).resolve().parents[1]

@pytest.mark.parametrize('engine', ['powershell.exe', 'pwsh.exe'], ids=['PS51', 'PS7'])
def test_resume_reader_preserves_dates_through_save_and_orphan_exit(carried, tmp_path, monkeypatch, engine):
    control, payload, audit_path, original = carried
    driver_text = (REPO / 'tests/terminal_orphan_admin_driver.ps1').read_text(encoding='utf-8-sig')
    # Use the existing driver OS-only boundaries and actual product Save/redaction.
    for name in ('ConvertTo-KmtechPublicDiagnosticTree', 'ConvertTo-KmtechRedactedSinkObject'):
        driver_text, count = re.subn(r'^function ' + name + r'\(\$Value\)\{return \$Value\}\r?\n',
                                     '', driver_text, flags=re.M)
        assert count == 1
    driver_text = driver_text.replace(
        '$script:ReplayCount=0',
        ". (Join-Path $Repo 'tools/failure_redaction.ps1')\n$script:ReplayCount=0", 1)
    driver_text = driver_text.replace('([scriptblock]::Create($node.Extent.Text))',
        '([scriptblock]::Create(("`n"*($node.Extent.StartLineNumber-1))+$node.Extent.Text))')
    driver_text, trap_count = re.subn(r"(?m)^\$ErrorActionPreference\s*=\s*'Stop'\s*$", lambda _: '''$ErrorActionPreference='Stop'
trap {
    Set-StrictMode -Off
    $diagnostic=[ordered]@{type=$_.Exception.GetType().Name;line=$_.InvocationInfo.ScriptLineNumber;stack_lines=@($_.ScriptStackTrace -split "`n"|ForEach-Object{if($_ -match ': line ([0-9]+)'){$Matches[1]}})}
    [IO.File]::WriteAllText((Join-Path (Split-Path -Parent $Output) 'terminal-driver-error.json'),($diagnostic|ConvertTo-Json -Depth 4))
    exit 1
}''', driver_text, count=1)
    assert trap_count == 1
    marker = '    $ExpectedRollbackAuditSha256=Sha $ResumeRollbackAuditPath'
    assert driver_text.count(marker) == 2
    pipeline = r"""
    $pipelineReaderType='System.String'
    $pipelineReaderEqual=$true
    $pipelineSaveBytesEqual=$true
    if($script:CaseMode -ceq 'ordinary-reader-resave'){
        __ACTUAL_RESUME_AUDIT_READER__
        $audit=[ordered]@{}
        foreach($property in $saved.PSObject.Properties){$audit[$property.Name]=$property.Value}
        foreach($name in @('rollback','writer_fence')){
            $map=[ordered]@{}
            foreach($property in $audit[$name].PSObject.Properties){$map[$property.Name]=$property.Value}
            $audit[$name]=$map
        }
        $pipelineReaderType=$audit.writer_fence.session_started_at_utc.GetType().FullName
        $pipelineReaderEqual=([string]$audit.writer_fence.session_started_at_utc -ceq [string]$seedCase.active.session_started_at_utc)
        $pipelineBeforeSave=Sha $ResumeRollbackAuditPath
        Save $ResumeRollbackAuditPath $audit
        $pipelineSaveBytesEqual=(Sha $ResumeRollbackAuditPath) -ceq $pipelineBeforeSave
    }
"""
    installer_text = (REPO / "INSTALL_CANONICAL_PORTABLE.ps1").read_text(encoding="utf-8-sig")
    resume_text = installer_text.split("function Invoke-LabelResumeRollback", 1)[1]
    resume_reader = resume_text.split("throw 'Rollback audit pin/ordinary-file check failed.' }", 1)[1].split("# Terminal evidence", 1)[0].strip()
    assert resume_reader.count("ConvertFrom-Json") >= 1
    pipeline = pipeline.replace("__ACTUAL_RESUME_AUDIT_READER__", resume_reader.replace("$auditPath", "$ResumeRollbackAuditPath"))
    head, found, tail = driver_text.rpartition(marker)
    assert found
    driver_text = head + pipeline + '    $originalOwnerHash=Sha $ownerPath\n' + marker + tail
    marker_result = '    $results += [pscustomobject]$item'
    assert driver_text.count(marker_result) >= 1
    driver_text = driver_text.replace(marker_result,
        "    $item['pipeline_save_bytes_equal']=$pipelineSaveBytesEqual\n    $item['owner_unchanged']=(Sha $ownerPath) -ceq $originalOwnerHash\n" + marker_result)
    marker = '    $item[\'layout\']'
    assert marker in driver_text
    pos = driver_text.index(marker)
    driver_text = driver_text[:pos] + '''    $item['pipeline_reader_type']=$pipelineReaderType
    $item['pipeline_reader_equal']=$pipelineReaderEqual
    $item['pipeline_saved_equal']=((Read-LabelOrphanJson $ResumeRollbackAuditPath).writer_fence.session_started_at_utc -ceq [string]$seedCase.active.session_started_at_utc)
''' + driver_text[pos:]
    driver = tmp_path / 'terminal-pipeline.ps1'
    driver.write_text(driver_text, encoding='utf-8-sig')
    seeds = []
    for index, mode in enumerate(('preserved', 'ordinary-reader-resave')):
        case_root = tmp_path.parent / ('c' + hashlib.sha256(str(tmp_path).encode()).hexdigest()[:6] + str(index))
        install = case_root / 'install/current'
        install.parent.mkdir(parents=True)
        active = copy.deepcopy(payload)
        # A legitimate round-trip UTC string with a trailing fractional zero.
        active['session_started_at_utc'] = '2026-10-21T12:00:00.1234560Z'
        assert active["session_started_at_utc"].endswith("0Z")
        record = copy.deepcopy(original)
        record['writer_fence']['session_started_at_utc'] = active['session_started_at_utc']
        record.update(status='ROLLBACK_FAILED', install_root=str(install), source_commit='a' * 40,
                      code_state='ABSENT', code_preimage='ABSENT', rollback={'applied': False})
        seeds.append(dict(case_root=str(case_root), audit=record, active=active, mode=mode,
                          code_class='ABSENT', owner_created=record['writer_fence']['owner_process_started_at_filetime'],
                          row=dict(site=5559, status='ROLLBACK_FAILED', fence_status=active['status'])))
    seed = tmp_path / 'seeds.json'
    seed.write_text(json.dumps(seeds), encoding='utf-8')
    output = tmp_path / 'terminal-output.json'
    process = subprocess.run([shutil.which(engine), '-NoLogo', '-NoProfile', '-NonInteractive',
        '-ExecutionPolicy', 'Bypass', '-File', str(driver), '-Repo', str(REPO),
        '-Seed', str(seed), '-Output', str(output)], capture_output=True,
        creationflags=subprocess.CREATE_NO_WINDOW)
    stderr = process.stderr.decode('utf-8', errors='replace')
    (tmp_path / 'terminal-process-summary.json').write_text(json.dumps({
        'rc': process.returncode, 'stdout_bytes': len(process.stdout), 'stderr_bytes': len(process.stderr),
        'qualified_error_ids': re.findall(r'FullyQualifiedErrorId\s*:\s*([A-Za-z_0-9.]+)', stderr),
        'line_numbers': re.findall(r'(?::|\bline\s+)([0-9]+)(?:\s|\s*char|\s*문자)', stderr)
    }), encoding='utf-8')
    assert process.returncode == 0, 'terminal driver failed before observation'
    rows = json.loads(output.read_text(encoding='utf-8-sig'))
    summaries = []
    for row in rows:
        root = Path(row['control'])
        real_active = fence.active_fence
        with monkeypatch.context() as patch:
            patch.setattr(fence, 'active_fence', lambda *args, **kwargs: real_active(root))
            # Exact admission used by real writers; the body records entry only.
            entered = False
            code = ''
            try:
                with fence.writer_admission('data_manager_log_writer', timeout_seconds=0):
                    entered = True
            except fence.WriterFencedError as exc:
                code = exc.code
        summary = {k: row.get(k) for k in ('mode', 'released', 'idempotent',
            'audit_unchanged', 'owner_unchanged', 'fence_unchanged', 'error_type', 'error_line',
            'pipeline_reader_type', 'pipeline_reader_equal', 'pipeline_saved_equal')}
        reason = row.get('reason', '')
        summary.update(save_bytes_equal=row["pipeline_save_bytes_equal"], engine=engine, reason=reason if re.fullmatch(r'ORPHAN_[A-Z_]+', reason) else '',
                       writer_admitted=entered, writer_refusal=code,
                       active_exists=(root / fence.ACTIVE_FILENAME).exists())
        summaries.append(summary)
    (tmp_path / 'terminal-comparison-summary.json').write_text(json.dumps(summaries, indent=2), encoding='utf-8')
    assert all(row["save_bytes_equal"] for row in summaries)
    assert summaries[1]["pipeline_reader_type"] == "System.String"
    assert all(row["pipeline_reader_equal"] and row["pipeline_saved_equal"] for row in summaries)
    assert summaries[0]['released'] and summaries[0]['writer_admitted']
    assert all(row['audit_unchanged'] and row['owner_unchanged'] for row in summaries)
    assert summaries[1]['released'] and summaries[1]['writer_admitted']
