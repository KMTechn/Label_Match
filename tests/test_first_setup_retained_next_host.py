"""The lane-authorized retained X -> signed Y process scene (one execution)."""
import json
import os
from pathlib import Path
import time

import pytest

from tests import _first_setup_host as host
from tests import _retained_diagnostic_observer as diagnostic


def test_actual_timeout_retained_candidate_accepts_next_signed_update(tmp_path):
    from kmtech_zero_pe import P256KeyPair
    import user_relay
    packet = os.environ.get('LABEL_MATCH_HOST_TEST_PACKET')
    if not packet:
        pytest.skip('an authenticated portable packet is required')
    with P256KeyPair.generate() as signer:
        config = host.prepare(tmp_path, Path(__file__).resolve().parents[1], Path(packet), signer=signer)
        candidate = host.prepare_update(config, signer, include_compatibility=False)
        following = host.prepare_update(config, signer, candidate_version='v2.0.126',
            base_bundle=tmp_path / 'offer/Label_Match', offer_name='offer126',
            compatibility_floor='v2.0.124')
        control = tmp_path / 'machine/update-control'
        host.write(tmp_path / diagnostic.CONTROL, dict(candidate=candidate, following=following))
        import writer_session_fence as fence
        restore_observer = diagnostic.install_mutex_observer(tmp_path, fence)
        timeline = diagnostic.Timeline(tmp_path, 'retained_test')
        stage = 'initial_ready'

        def mark(value):
            nonlocal stage
            stage = value
            timeline.event('retained_stage', stage=stage)

        def read_state():
            return json.loads((control / 'transition.json').read_bytes())

        def prepared_is(sha):
            path = tmp_path / 'stage/prepared.json'
            return path.exists() and json.loads(path.read_bytes()).get('source_commit') == sha

        def state_facts(state):
            return dict(phase=state['phase'], retained=state.get('retained_candidate') is True,
                active_is_candidate=json.loads((control / 'active.json').read_bytes())['sha'] == candidate,
                candidate_failed=candidate in state.get('failed_shas', []),
                recovery_pending=state.get('recovery_pending') is True,
                rollback_at_present=bool(state.get('rollback_at')),
                confirmed_at_present=bool(state.get('confirmed_at')),
                generation_advanced=(state.get('failure_decision') or {}).get('code') == 'TRIAL_GENERATION_ADVANCED')

        try:
            mark('initial_ready')
            host.ready(config)
            mark('offer_X')
            assert host.offer_update(config) == candidate
            host.wait_for(config, lambda: prepared_is(candidate))
            mark('restart_X_for_actual_timeout')
            host.restart(config, 'retained-actual-timeout')
            mark('assert_retained_X')
            retained = state_facts(read_state())
            assert retained == dict(phase='DEFERRED', retained=True, active_is_candidate=True,
                candidate_failed=True, recovery_pending=False, rollback_at_present=False,
                confirmed_at_present=False, generation_advanced=True)
            host.write(tmp_path / 'retained-created-proof.json', retained)

            # One ordinary X start before Y is offered. No field timeout changes.
            mark('ordinary_X_session_end')
            host.session_end(config, 'retained-ordinary-X')
            started = time.monotonic()
            launches_before = set((tmp_path / 'launches').glob('*.json'))
            mark('ordinary_X_autorun')
            host.autorun(config)
            bootstrap_seconds = time.monotonic() - started
            mark('ordinary_X_ready')
            result = host.ready(config, 'retained-ordinary-X')
            ordinary_seconds = time.monotonic() - started
            ordinary = state_facts(read_state())
            assert ordinary == retained
            task_launches = 0
            for path in set((tmp_path / 'launches').glob('*.json')) - launches_before:
                task_launches += '--apply-prepared' in json.loads(path.read_bytes()).get('arguments', [])
            assert task_launches == 0
            events = [json.loads(line) for path in (tmp_path / 'cycle-observations').glob('*.jsonl')
                      for line in path.read_text().splitlines() if line.strip()]
            current = [row for row in events if row['pid'] == result['relay_start']['process_id']
                       and row['status'] == 'RUNNING']
            assert current and current[-1]['screen_warning_decision'] is False
            host.write(tmp_path / 'retained-ordinary-start-proof.json', {
                **ordinary, 'bootstrap_seconds': bootstrap_seconds,
                'ordinary_to_setup_ready_seconds': ordinary_seconds,
                'update_confirmation_wait_seconds': 0.0, 'system_apply_launches': task_launches,
                'update_confirmation_wait_seconds_source': 'original_test_literal_not_measurement',
                'current_user_ready': result['status'] == 'READY', 'owner_alive': result['relay_start']['status'] == 'ALIVE',
                'screen_state': current[-1]['screen_state'], 'screen_warning_decision': False,
                'real_gui_render_verified': False, 'headless_screen_decision_authorized': True})

            mark('offer_Y')
            assert host.offer_update(config, 'offer126') == following
            host.wait_for(config, lambda: prepared_is(following))
            mark('restart_Y')
            host.restart(config, 'retained-next-Y')
            mark('wait_Y_CONFIRMED')
            host.wait_for(config, lambda: read_state().get('phase') == 'CONFIRMED')
            mark('assert_Y_CONFIRMED')
            final = read_state()
            assert json.loads((control / 'active.json').read_bytes())['sha'] == following
            assert final['previous_sha'] == candidate and final['candidate_sha'] == following
            assert final['rollback_pair']['to_commit'] == candidate
            assert final['confirmation_audit_applied'] is True
            assert candidate in final['failed_shas']
            owner = host.launch(config, 'read-owner')
            assert owner.wait() == 0
            owner_result = json.loads((tmp_path / 'update-owner-result.json').read_bytes())
            assert owner_result['status'] == 'ALIVE' and owner_result['existing_relay_reused'] is True
            status = json.loads(user_relay.user_relay_status_path(
                tmp_path / 'local/KMTech/DirectSync/label_match').read_bytes())
            assert status['status'] == 'RUNNING' and status['source_commit'] == following
            host.prove_next_cycle(config, name='retained-next-Y', process_id=status['process_id'], accept_initial_pass=True)
            host.write(tmp_path / 'retained-next-Y-confirmed-proof.json', {
                'phase': final['phase'], 'previous_is_retained_X': True, 'reverse_targets_X': True,
                'active_is_Y': True, 'X_failed_still_present': True, 'audit_applied': True,
                'owner_reused': True, 'ordinary_X_started_before_Y': True,
                'actual_timeout_retained_X': True, 'real_gui_render_verified': False})
            timeline.event('retained_body_passed', stage=stage)
        except BaseException as error:
            diagnostic.emit_error(tmp_path, 'body', stage, error)
            raise
        finally:
            try:
                host.stop_owned_relays(config)
                timeline.event('retained_cleanup_passed', body_last_stage=stage)
            except BaseException as error:
                diagnostic.emit_error(tmp_path, 'cleanup', stage, error)
                raise
            finally:
                restore_observer()
