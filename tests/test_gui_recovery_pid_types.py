"""Both candidate and proved relay rows require an actual positive integer PID."""
import copy
import json
import re

import pytest

from test_gui_recovery_pid_proof import memory_world
from tests.test_shared_ancestor_protection import ROOT, run_functions


@pytest.mark.parametrize('observed', ['candidate', 'proved'])
@pytest.mark.parametrize('bad_pid', [0, -1, True, '123', None, 'missing'])
def test_each_relay_observation_rejects_invalid_pid(observed, bad_pid):
    world = memory_world('FAILED', 'NONE', writer='relay')

    def corrupt(row):
        if bad_pid == 'missing':
            row.pop('pid', None)
        else:
            row['pid'] = bad_pid

    if observed == 'candidate':
        corrupt(world.rows[0])
    else:
        proof = world.ns['assert_ordinary_recovery']

        def prove(*args, **kwargs):
            rows = proof(*args, **kwargs)
            corrupt(rows[0])
            return rows

        world.ns['assert_ordinary_recovery'] = prove
    before = copy.deepcopy(world.files)
    rc, _ = world.bootstrap('gui')
    assert rc == 20
    assert not world.launches
    assert world.files == before


@pytest.mark.parametrize('engine', ['powershell', 'pwsh'])
def test_observe_process_id_cast_remains_a_valid_relay_row(tmp_path, engine):
    source = ROOT / 'tools/apply_prepared_update.ps1'
    expression = re.search(r'pid=(\[int\]\$process\.ProcessId);observable=\$true',
                           source.read_text(encoding='utf-8-sig')).group(1)
    result = run_functions(tmp_path, source, ['Write-LabelStartupVerifyFailure'],
        '$process=[pscustomobject]@{ProcessId=[uint32]4242}\n'
        '$observedPid=' + expression + '\n'
        "if($observedPid -isnot [int]){throw 'OBSERVE_PID_TYPE'}\n"
        "@{pid=$observedPid}|ConvertTo-Json -Compress\n", engine=engine)
    assert result.returncode == 0, 'Observe PID cast failed; values withheld'
    world = memory_world('FAILED', 'NONE', writer='relay')
    world.rows[0].update(json.loads(result.stdout))
    rc, _ = world.bootstrap('gui')
    assert rc == 0
    assert len(world.launches) == 1
