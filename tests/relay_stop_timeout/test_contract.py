"""Run the real stop loop with a simulated clock and no native/product calls."""
from __future__ import annotations

import ast
from contextlib import nullcontext
from datetime import datetime, timedelta, timezone
import json
from pathlib import Path
import sys
from types import SimpleNamespace
import uuid

import pytest


ROOT = Path(__file__).resolve().parents[2]
CHILD_LIMIT = 'RELAY_DIRECT_SYNC_PROCESS_TIMEOUT_SECONDS'
STOP_LIMIT = 'RELAY_NORMAL_STOP_TIMEOUT_SECONDS'


class Clock:
    def __init__(self):
        self.now = 0.0

    def monotonic(self):
        return self.now

    def sleep(self, seconds):
        assert 0 < seconds <= 0.25
        self.now += seconds


class Lease:
    def release(self):
        pass

    close = release


def stop_namespace(clock, *, child_limit=None):
    tree = ast.parse((ROOT / 'user_relay.py').read_text(encoding='utf-8-sig'))
    nodes = []
    for node in tree.body:
        if isinstance(node, ast.Assign) and any(
            isinstance(target, ast.Name) and target.id in {CHILD_LIMIT, STOP_LIMIT}
            for target in node.targets
        ):
            if child_limit is not None and node.targets[0].id == CHILD_LIMIT:
                node.value = ast.Constant(child_limit)
            nodes.append(node)
        elif isinstance(node, ast.FunctionDef) and node.name in {'request_user_relay_stop', '_log_wait_elapsed'}:
            node.decorator_list = []
            nodes.append(node)
    marker = {}

    def write_marker(path, payload, **kwargs):
        marker.update(payload)

    namespace = {
        'time': clock, 'Path': Path, 'uuid': uuid, 'json': json, 'sys': sys,
        '_now': lambda: '2026-10-26T00:00:00+00:00',
        'user_relay_stop_path': lambda root: root / 'control' / 'stop.json',
        '_write_json_atomic': write_marker, '_write_machine_json_atomic': write_marker,
        'acquire_runtime_instance': lambda path: Lease(),
        '_acquire_relay_lease': lambda path: Lease(),
        'writer_admission': lambda source: nullcontext(),
        'STOP_MARKER_V1': 'label-match-user-relay-stop-v1',
        'read_stop_marker': lambda path: (dict(marker), b'', 'receipt-hash'),
    }
    module = ast.Module(body=[ast.ImportFrom(
        module='__future__', names=[ast.alias(name='annotations')], level=0
    ), *nodes], type_ignores=[])
    exec(compile(ast.fix_missing_locations(module), str(ROOT / 'user_relay.py'), 'exec'), namespace)
    return namespace


@pytest.mark.parametrize('child_limit', [120, 240])
def test_stop_budget_tracks_the_direct_sync_child_limit(child_limit):
    namespace = stop_namespace(Clock(), child_limit=child_limit)
    assert namespace.get(STOP_LIMIT) == child_limit + 30
    assert namespace['request_user_relay_stop'].__kwdefaults__['timeout_seconds'] == child_limit + 30


@pytest.mark.parametrize('exit_after', [20, 90, 149, 150, 151])
def test_default_stop_allows_one_child_cycle_and_preserves_unknown(tmp_path, exit_after):
    clock = Clock()
    namespace = stop_namespace(clock)
    result = namespace['request_user_relay_stop'](
        tmp_path, wait=clock.sleep,
        lease_factory=lambda path: Lease() if clock.now >= exit_after else None,
    )
    assert result['status'] == ('ABSENT' if exit_after <= 150 else 'UNKNOWN')
    assert clock.now == min(exit_after, 150)
    if exit_after > 150:
        assert result['reason'] == 'relay process absence was not proven before timeout'


def test_explicit_request_only_does_not_wait(tmp_path):
    clock = Clock()
    result = stop_namespace(clock)['request_user_relay_stop'](
        tmp_path, timeout_seconds=0, wait=clock.sleep, lease_factory=lambda path: None,
    )
    assert result['status'] == 'UNKNOWN'
    assert clock.now == 0


def test_missing_stderr_never_redirects_diagnostics_to_stdout(monkeypatch, capsys):
    helper = stop_namespace(Clock()).get('_log_wait_elapsed')
    with monkeypatch.context() as patch:
        patch.setattr(sys, 'stderr', None)
        if helper is not None:
            helper('diagnostic_stream_test', 0, 'ABSENT')
    assert capsys.readouterr().out == ''


def test_callers_keep_the_shared_default_for_removal_and_containment():
    tree = ast.parse((ROOT / 'current_user_onboarding.py').read_text(encoding='utf-8-sig'))
    names = {'remove_current_user_setup', 'restore_current_user_lifecycle_after_replacement'}
    functions = [node for node in tree.body if isinstance(node, ast.FunctionDef) and node.name in names]
    assert functions
    for function in functions:
        defaults = dict(zip([arg.arg for arg in function.args.kwonlyargs], function.args.kw_defaults))
        assert isinstance(defaults['relay_stopper'], ast.Name)
        assert defaults['relay_stopper'].id == 'request_user_relay_stop'
        calls = [node for node in ast.walk(function) if isinstance(node, ast.Call)
                 and isinstance(node.func, ast.Name) and node.func.id == 'relay_stopper']
        # LM dispatches callbacks in its existing removal step table.
        assert calls or any(isinstance(node, ast.Name) and node.id == 'relay_stopper'
                            for node in ast.walk(function))
        assert all(not node.keywords for node in calls)


@pytest.mark.parametrize('exit_after', [20, 151])
def test_stop_elapsed_is_one_stderr_json_line_without_stdout(tmp_path, capsys, exit_after):
    clock = Clock()
    result = stop_namespace(clock)['request_user_relay_stop'](
        tmp_path, wait=clock.sleep,
        lease_factory=lambda path: Lease() if clock.now >= exit_after else None,
    )
    captured = capsys.readouterr()
    assert captured.out == ''
    # CA removal is also called by the PS5.1 helper's 2>&1 + Stop path.
    # ANSWER-3 keeps that shared path silent, including its stop helper.
    if (ROOT / 'container_audit_product_host.py').is_file():
        assert captured.err == ''
        return
    lines = captured.err.splitlines()
    assert len(lines) == 1
    assert json.loads(lines[0]) == {
        'event': 'relay_normal_stop', 'elapsed_ms': min(exit_after, 150) * 1000,
        'status': result['status'],
    }


@pytest.mark.parametrize('exit_after,age', [(90, 0), (149, 0), (151, 0), (90, 80), (90, 200)])
def test_candidate_stop_uses_the_original_durable_budget(tmp_path, capsys, exit_after, age):
    tree = ast.parse((ROOT / 'auto_update.py').read_text(encoding='utf-8-sig'))
    function = next(node for node in tree.body if isinstance(node, ast.FunctionDef) and node.name == '_apply_prepared_once')
    stop_loop = [index for index, node in enumerate(function.body)
                     if isinstance(node, (ast.While, ast.Try)) and any(
                         isinstance(call, ast.Call) and isinstance(call.func, ast.Name)
                         and call.func.id == 'settle_trial_failure' for call in ast.walk(node))][-1]
    start = stop_loop - 1
    while not isinstance(function.body[start], ast.With):
        start -= 1
    tail = function.body[start:]
    clock = Clock()
    epoch = datetime(2026, 10, 26, tzinfo=timezone.utc)
    original = (epoch - timedelta(seconds=age)).isoformat()
    initial = {'phase': 'APPLIED_PENDING_READY', 'normal_stop_started_at': original}
    writes, settlements = [], []

    def settle(*args):
        settlements.append(clock.now)
        return {'retained_candidate': True}

    namespace = {
        'time': clock, 'os': __import__('os'), 'Path': Path,
        'control': tmp_path, 'policy': {'operator_sid': 'test', 'staging_root': str(tmp_path)},
        'state': dict(initial), 'opened_here': True,
        'update_lock': lambda *args: nullcontext(),
        'read_json': lambda path: dict(initial),
        'write_json_durable': lambda path, value: writes.append(dict(value)),
        'now_utc': lambda: (epoch + timedelta(seconds=clock.now)).isoformat(),
        '_utc': datetime.fromisoformat,
        'request_rollback': lambda state, **kwargs: {**state, 'phase': 'ROLLBACK_PENDING'},
        'process_snapshot': lambda control: [],
        'writer_processes': lambda *args, **kwargs: [] if clock.now >= exit_after else ['relay'],
        'settle_trial_failure': settle,
        'RELAY_NORMAL_STOP_TIMEOUT_SECONDS': 150,
        '_log_wait_elapsed': stop_namespace(clock).get('_log_wait_elapsed', lambda *args: None),
    }
    exercise = ast.FunctionDef(name='exercise', args=ast.arguments(
        posonlyargs=[], args=[], kwonlyargs=[], kw_defaults=[], defaults=[]),
        body=tail, decorator_list=[])
    exec(compile(ast.fix_missing_locations(ast.Module(body=[exercise], type_ignores=[])),
                 str(ROOT / 'auto_update.py'), 'exec'), namespace)
    assert namespace['exercise']() == 20
    budget = max(0, 150 - age)
    assert bool(settlements) == (exit_after < budget)
    assert clock.now == min(exit_after, budget)
    assert writes[0]['normal_stop_started_at'] == original
    if exit_after >= budget:
        assert writes[-1]['phase'] == 'UNKNOWN'
        assert writes[-1]['reason'] == 'candidate normal stop could not be proven'
    captured = capsys.readouterr()
    assert captured.out == ''
    if (ROOT / 'container_audit_product_host.py').is_file():
        assert captured.err == ''
    else:
        assert json.loads(captured.err) == {
            'event': 'candidate_normal_stop', 'elapsed_ms': int(clock.now * 1000),
            'status': 'ABSENT' if settlements else 'UNKNOWN',
        }


@pytest.mark.skipif(not (ROOT / 'container_audit_product_host.py').is_file(), reason='CA replacement containment')
@pytest.mark.parametrize('stop_fails', [False, True])
def test_containment_log_uses_the_safe_finite_producer(tmp_path, capsys, stop_fails):
    tree = ast.parse((ROOT / 'current_user_onboarding.py').read_text(encoding='utf-8-sig'))
    function = next(node for node in tree.body if isinstance(node, ast.FunctionDef)
                    and node.name == 'restore_current_user_lifecycle_after_replacement')
    timed = [node for node in ast.walk(function) if isinstance(node, ast.Try) and node.finalbody
             and any(isinstance(call, ast.Call) and isinstance(call.func, ast.Name)
                     and call.func.id == '_log_wait_elapsed' for statement in node.finalbody for call in ast.walk(statement))]
    assert len(timed) == 1
    clock = Clock()

    def stop(root):
        clock.now = 90
        if stop_fails:
            raise ValueError('original-stop-failure')
        return {'status': 'ABSENT'}

    namespace = dict(time=clock, paths=SimpleNamespace(direct_sync_root=tmp_path),
                     relay_stopper=stop, elapsed_started=0, elapsed_status='ERROR',
                     _log_wait_elapsed=stop_namespace(clock)['_log_wait_elapsed'])
    preceding = None
    for parent in ast.walk(function):
        for _, children in ast.iter_fields(parent):
            if isinstance(children, list) and timed[0] in children:
                index = children.index(timed[0])
                preceding = children[index - 2:index]
    assert preceding and all(isinstance(node, ast.Assign) for node in preceding)
    exercise = ast.FunctionDef(name='exercise', args=ast.arguments(posonlyargs=[], args=[],
        kwonlyargs=[], kw_defaults=[], defaults=[]), body=[*preceding, timed[0]], decorator_list=[])
    exec(compile(ast.fix_missing_locations(ast.Module(body=[exercise], type_ignores=[])),
                 str(ROOT / 'current_user_onboarding.py'), 'exec'), namespace)
    if stop_fails:
        with pytest.raises(ValueError, match='original-stop-failure'):
            namespace['exercise']()
    else:
        namespace['exercise']()
    assert json.loads(capsys.readouterr().err) == {
        'event': 'replacement_relay_containment', 'elapsed_ms': 90000,
        'status': 'ERROR' if stop_fails else 'ABSENT',
    }
