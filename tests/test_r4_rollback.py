"""Real installer catch/finally and G; native runtime effects are isolated doubles."""
import pytest

from tests.test_r4_support import engine, run_driver


@pytest.mark.parametrize('case', ('success', 'early_failure', 'runtime_unproven', 'g_refusal'))
def test_rollback_independent_tail_and_generation_custody(tmp_path, engine, case):
    record = run_driver(tmp_path, engine, 'rollback-driver.ps1', Root=tmp_path / 'state', Case=case)
    assert record['real_generation_helper'] and record['real_product_catch_and_finally']
    counts = record['counts']
    assert counts['authority_release'] == 1
    if case == 'g_refusal':
        assert record['failed'] and record['g_calls'] == 1
        for name in ('product', 'run_restore', 'task_restore', 'fence_release', 'relay_restart', 'inventory'):
            assert counts[name] == 0, name
        assert record['writer_fence_still_started']
        assert record['guidance_present']
        return
    assert record['g_calls'] == 2
    assert record['code_restored'] is True
    assert counts['product'] == counts['run_restore'] == counts['task_restore'] == 1
    for phase in ('writer_absence', 'code_restore', 'run_restore', 'task_restore',
                  'stop_marker_restore', 'removal_report_restore', 'protected_startup_restore', 'preimage_readback'):
        assert phase in record['phases'], phase
    if case == 'runtime_unproven':
        assert record['failed']
        assert counts['fence_release'] == counts['relay_restart'] == 0
        assert record['writer_fence_still_started']
        assert record['guidance_present'], 'unproven runtime must retain custody with Korean guidance'
    else:
        assert counts['fence_release'] == counts['relay_restart'] == 1
        assert not record['writer_fence_still_started']
        if case == 'early_failure':
            assert record['failed']
            assert 'current_user_removal' in record['failed_steps']
            assert record['original_failure_preserved']


@pytest.mark.parametrize('case', ('success', 'parent_failure', 'child_failure'))
def test_nested_phase_records_keep_parent_identity(tmp_path, engine, case):
    record = run_driver(tmp_path, engine, 'phase-driver.ps1', Case=case)
    phases = [row['phase'] for row in record['steps']]
    assert len(phases) == 3
    assert sorted(phases) == ['child_one', 'child_two', 'parent']
    failed = [row['phase'] for row in record['steps'] if row['status'] == 'FAILED']
    expected = {'success': [], 'parent_failure': ['parent'], 'child_failure': ['child_one']}[case]
    assert failed == expected
    assert record['failure_phases'] == expected


def test_preflight_observes_official_dynamic_task(tmp_path, engine):
    import json
    from tests.test_r4_official_relay import TABLE, vector_input
    payload = vector_input(TABLE['vectors'][0])
    payload['mode'] = 'preflight'
    input_path = tmp_path / 'input.json'
    input_path.write_text(json.dumps(payload), encoding='utf-8')
    record = run_driver(tmp_path, engine, 'preflight-driver.ps1', InputPath=input_path)
    assert record['scheduler_queries'] >= 1
    assert record['observed_count'] == 1, 'canonical preflight silently ignored the official dynamic task'
