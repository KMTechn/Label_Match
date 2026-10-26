"""The exited bootstrap still has time to publish its protected relay owner."""
import ast
import json
from pathlib import Path
import sys
from types import ModuleType, SimpleNamespace

import pytest

ROOT = Path(__file__).resolve().parents[2]


class RelayError(RuntimeError):
    def __init__(self, message, **kwargs):
        super().__init__(message)


@pytest.mark.parametrize('ready_after', [20, 90, 119, 121])
def test_finished_bootstrap_uses_120_seconds_within_the_parent_budget(monkeypatch, capsys, ready_after):
    clock = [0.0]

    def wait(seconds):
        assert 0 < seconds <= 0.1
        clock[0] += seconds

    timing = ModuleType('startup_timing')
    timing.startup_stop_checkpoint = lambda: None
    timing.startup_progress = lambda event: None
    timing.startup_wait = lambda seconds, waiter: waiter(seconds) if seconds > 0 else None
    monkeypatch.setitem(sys.modules, 'startup_timing', timing)
    tree = ast.parse((ROOT / 'user_relay.py').read_text(encoding='utf-8-sig'))
    selected = [node for node in tree.body if isinstance(node, ast.FunctionDef)
                and node.name in {'_read_started_relay', '_log_wait_elapsed'}]
    namespace = {'time': SimpleNamespace(monotonic=lambda: clock[0]), 'sys': sys, 'json': json,
                 'UserRelayError': RelayError, 'RELAY_OWNER_READBACK_TIMEOUT_SECONDS': 120}
    exec(compile(ast.Module(body=selected, type_ignores=[]), str(ROOT / 'user_relay.py'), 'exec'), namespace)
    process = SimpleNamespace(poll=lambda: 0, pid=100)
    owner = lambda: {'process_id': 200, 'updated_at': 'test'} if clock[0] >= ready_after else None
    if ready_after < 120:
        result = namespace['_read_started_relay'](process, owner, 0, 600, True, wait)
        assert result['process_id'] == 200
    else:
        with pytest.raises(RelayError, match='RELAY_EXITED_BEFORE_RUNNING'):
            namespace['_read_started_relay'](process, owner, 0, 600, True, wait)
    assert clock[0] == pytest.approx(min(ready_after, 120), abs=0.11)
    captured = capsys.readouterr()
    assert captured.out == ''
    record = json.loads(captured.err)
    assert record['event'] == 'full_relay_owner_readback'
    assert record['status'] == ('ALIVE' if ready_after < 120 else 'UNKNOWN')
    assert record['elapsed_ms'] == pytest.approx(min(ready_after, 120) * 1000, abs=110)
