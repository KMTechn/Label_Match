"""The bootstrap observer follows the SYSTEM worker's durable stop window."""
import ast
from datetime import datetime, timedelta, timezone
from pathlib import Path
import sys
from types import ModuleType

import pytest

ROOT = Path(__file__).resolve().parents[2]


@pytest.mark.parametrize('phase', ['ROLLBACK_PENDING', 'UNKNOWN'])
@pytest.mark.parametrize('budget,expected', [(150, 465), (270, 480)])
def test_supervision_follows_the_same_stop_budget_without_reset(monkeypatch, phase, budget, expected):
    relay = ModuleType('user_relay')
    relay.RELAY_NORMAL_STOP_TIMEOUT_SECONDS = budget
    monkeypatch.setitem(sys.modules, 'user_relay', relay)
    tree = ast.parse((ROOT / 'auto_update.py').read_text(encoding='utf-8-sig'))
    function = next(node for node in tree.body if isinstance(node, ast.FunctionDef)
                    and node.name == 'bootstrap_supervision_deadline')
    namespace = {'_utc': datetime.fromisoformat, 'STARTUP_WAIT_SECONDS': 240,
                 'READY_WINDOW_SECONDS': 240, '_bootstrap_awaiting_apply_decision': lambda *args: False}
    module = ast.Module(body=[ast.ImportFrom(module='__future__', names=[ast.alias(name='annotations')], level=0),
                              function], type_ignores=[])
    exec(compile(ast.fix_missing_locations(module), str(ROOT / 'auto_update.py'), 'exec'), namespace)
    epoch = datetime(2026, 10, 26, tzinfo=timezone.utc)
    state = {'phase': phase, 'normal_stop_started_at': (epoch + timedelta(seconds=300)).isoformat()}
    assert namespace['bootstrap_supervision_deadline'](
        state, started=0, now=350, utc_now=(epoch + timedelta(seconds=350)).isoformat()) == expected
    assert state['normal_stop_started_at'] == (epoch + timedelta(seconds=300)).isoformat()
