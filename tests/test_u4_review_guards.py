"""§13 conservative migration, explicit refusal, and downgrade safety."""
import sqlite3
from dataclasses import replace

import pytest
import Label_Match as lm
import package_logistics as logistics
from package_logistics import PackageCancellationOutbox, PackageLogisticsError, PackageOutbox
from tests.test_package_logistics import _draft_for_set, _cancellation_intent
from tests.test_u4_cancellation_order import queued
from tests.test_u4_repack_pair import pair, cancel_local, enqueue_repack
from tests.test_lm_scan_cancel import app_for_delete


@pytest.mark.parametrize('store_type', [PackageOutbox, PackageCancellationOutbox])
@pytest.mark.parametrize('chain_length', [2, 3])
def test_legacy_reversed_intents_require_durable_review_and_independent_single_proceeds(tmp_path, store_type, chain_length):
    outbox = PackageOutbox(tmp_path/'outbox.sqlite3')
    cancels = PackageCancellationOutbox(outbox.db_path)
    drafts = [_draft_for_set(name) for name in ('A','B','C')[:chain_length]]
    rows = {draft.set_id: outbox.enqueue(draft) for draft in reversed(drafts)}
    for draft in drafts:
        outbox.mark_local_completion_committed(rows[draft.set_id]['idempotency_key'])
        if draft is not drafts[-1]:
            intent = _cancellation_intent(draft)
            assert cancels.enqueue(intent)['status'] == 'DEFERRED'
            cancels.mark_local_event_committed(intent.cancellation_event_id)
    normal = _draft_for_set('independent', independent_source=True)
    queued(outbox, normal)
    before = {draft.set_id: outbox.get_by_set_id(draft.set_id) for draft in drafts}
    cancellation_before = [cancels.get_by_set_id(draft.set_id) for draft in drafts]
    with sqlite3.connect(outbox.db_path) as conn:
        conn.execute('DROP INDEX ix_package_local_completion_order')
        conn.execute('ALTER TABLE package_command_outbox DROP COLUMN predecessor_package_key')
        conn.execute('ALTER TABLE package_command_outbox DROP COLUMN local_completion_order')
        conn.execute("UPDATE package_outbox_schema_info SET value='label-match-package-outbox-v11' WHERE key='schema_version'")
    store_type(outbox.db_path)
    reopened = PackageOutbox(outbox.db_path)
    changed = {'status','review_status','last_error_code','last_error_message','retry_after_at','updated_at',
               'predecessor_package_key','local_completion_order'}
    frozen = []
    for draft in drafts:
        row = reopened.get_by_set_id(draft.set_id)
        assert row['status'] == 'CONFLICT' and row['review_status'] == 'OPERATOR_REVIEW'
        assert row['last_error_code'] == 'REPACK_PREDECESSOR_REVIEW_REQUIRED'
        assert row['local_completion_order'] is None and row['predecessor_package_key'] is None
        assert {k:v for k,v in row.items() if k not in changed} == {k:v for k,v in before[draft.set_id].items() if k not in changed}
        frozen.append(row)
    assert [cancels.get_by_set_id(draft.set_id) for draft in drafts] == cancellation_before
    events = reopened.list_post_review_csv_pending()
    assert len(events) == chain_length
    store_type(outbox.db_path)
    assert [reopened.get_by_set_id(draft.set_id) for draft in drafts] == frozen
    assert reopened.list_post_review_csv_pending() == events
    app = app_for_delete(tmp_path/'notice', central=False)
    app.package_outbox = reopened
    app.package_cancellation_outbox = cancels
    app._refresh_package_cancellation_review_notice(app._read_package_review_snapshot())
    assert '관리자' in app._package_create_review_notice.message
    assert reopened.claim_next()['set_id'] == normal.set_id
    assert reopened.claim_next() is None
    assert cancels.claim_next() is None
    later = _draft_for_set('later')
    queued(reopened, later)
    assert reopened.claim_next() is None
    assert reopened.get_by_set_id(later.set_id)['status'] == 'CONFLICT'


@pytest.mark.parametrize('receipt_first', [False, True])
def test_explicit_reopen_false_blocks_post_even_when_source_available_and_survives_restart(tmp_path, receipt_first):
    app, server, create, cancel, original, master = pair(tmp_path, mode='refused')
    cancel_local(app, 'f2')
    if receipt_first:
        assert cancel.drain()['acked'] == 1
    repack = enqueue_repack(app, server, master)
    if not receipt_first:
        assert cancel.drain()['acked'] == 1
        assert app.package_outbox.get_by_set_id(repack.set_id)['status'] == 'CONFLICT'
    # The source can become valid later; the explicit refusal still governs this repack.
    server.mode = 'reopen'
    server.source['bundle_state'] = 'AVAILABLE'
    assert server.source['bundle_state'] == 'AVAILABLE'
    sent = list(server.sent)
    assert create.drain() == {'acked':0,'retry':0,'conflict':0}
    assert server.sent == sent
    assert sent == [('CREATE_PACKAGE',original.package_bundle_id),('CANCEL_PACKAGE',original.package_bundle_id)]
    row = app.package_outbox.get_by_set_id(repack.set_id)
    assert row['status'] == 'CONFLICT' and row['last_error_code'] == 'REPACK_REOPEN_REFUSED'
    assert row['attempt_count'] == 0 and row['command_json'] is None and row['local_completion_committed'] == 1
    assert len(app.package_outbox.list_post_review_csv_pending()) == 1
    app.package_outbox = PackageOutbox(app.package_outbox.db_path)
    assert app.package_outbox.claim_next() is None
    assert app.package_outbox.get_by_set_id(repack.set_id) == row
    app._refresh_package_cancellation_review_notice(app._read_package_review_snapshot())
    assert app._package_create_review_rows[0]['repack_server_notice'] == 'REOPEN_REFUSED'
    assert '되돌리지 못했습니다' in app._package_create_review_notice.message
    assert server.sent == sent


@pytest.mark.parametrize('store_type', [PackageOutbox, PackageCancellationOutbox])
def test_higher_saved_schema_is_refused_before_any_write_and_not_restamped(tmp_path, monkeypatch, store_type):
    path = tmp_path/'future.sqlite3'
    outbox = PackageOutbox(path)
    queued(outbox,_draft_for_set('preserved'))
    with sqlite3.connect(path) as conn:
        conn.execute("UPDATE package_outbox_schema_info SET value='label-match-package-outbox-v13' WHERE key='schema_version'")
    before = path.read_bytes()
    def forbidden(conn):
        pytest.fail('compatibility writes must not start for a newer schema')
    monkeypatch.setattr(logistics,'ensure_deferred_intent_schema_compatibility',forbidden)
    for _ in range(2):
        with pytest.raises(PackageLogisticsError) as failure:
            store_type(path)
        assert failure.value.code == 'PACKAGE_OUTBOX_SCHEMA_TOO_NEW'
        assert '관리자' in str(failure.value) and 'DB 사본 복원' in str(failure.value)
        assert path.read_bytes() == before
    with sqlite3.connect(path) as conn:
        assert conn.execute("SELECT value FROM package_outbox_schema_info WHERE key='schema_version'").fetchone()[0] == 'label-match-package-outbox-v13'


def test_higher_schema_main_stops_and_publishes_administrator_notice(tmp_path, monkeypatch, capsys):
    import auto_update
    import startup_observation
    monkeypatch.setattr(lm,'LABEL_MATCH_SCREEN_HOST',dict(lm.LABEL_MATCH_SCREEN_HOST))
    path = tmp_path/'future.sqlite3'
    PackageOutbox(path)
    with sqlite3.connect(path) as conn:
        conn.execute("UPDATE package_outbox_schema_info SET value='label-match-package-outbox-v13' WHERE key='schema_version'")
    before = path.read_bytes()
    monkeypatch.setattr(auto_update,'startup_gate',lambda *a,**k: None)
    for name in ('dispatch_product_mode','validate_pending_worker_install','check_user_data_open',
                 'verify_factory_contract_startup','_label_match_startup_trace'):
        monkeypatch.setattr(lm,name,lambda *a,**k: None)
    monkeypatch.setattr(lm,'release_identity',lambda *a: {})
    monkeypatch.setattr(lm,'_first_run_onboarding_enabled',lambda: False)
    monkeypatch.setattr(lm,'_startup_app_stop_requested',lambda: False)
    monkeypatch.setattr(lm,'_default_label_match_settings_path',lambda: tmp_path/'settings.json')
    monkeypatch.setattr(lm,'resolve_data_scope',lambda **k: str(tmp_path))
    monkeypatch.setattr(lm,'run_guarded_entrypoint',lambda *a,**k: PackageOutbox(path))
    notices = []
    monkeypatch.setattr(startup_observation,'begin_startup',lambda *a,**k: None)
    monkeypatch.setattr(startup_observation,'observe_startup',lambda reasons,**k: notices.append((reasons,k)))
    assert lm.main([]) == lm.ONBOARDING_EXIT_CODE
    assert notices == [(['DATA_GENERATION_REFUSED'],{'refused':True})]
    assert 'PACKAGE_OUTBOX_SCHEMA_TOO_NEW' in capsys.readouterr().err
    assert path.read_bytes() == before


def test_ambiguous_migration_also_preserves_and_reviews_the_local_completed_lease(tmp_path, monkeypatch):
    from tests.test_terminal_operation_lease import _refused_lease_fixture
    outbox, leases, draft, clock = _refused_lease_fixture(tmp_path,monkeypatch,status='LOCAL_COMPLETED')
    assert leases.get(lease_id=draft.operation_lease_id)['status'] == 'LOCAL_COMPLETED'
    another = replace(_draft_for_set('next'), source_bundle_id=draft.source_bundle_id,
                      source_input_tag_id=draft.source_input_tag_id)
    queued(outbox,another)
    original = leases.get(lease_id=draft.operation_lease_id)
    with sqlite3.connect(outbox.db_path) as conn:
        conn.execute('DROP INDEX ix_package_local_completion_order')
        conn.execute('ALTER TABLE package_command_outbox DROP COLUMN predecessor_package_key')
        conn.execute('ALTER TABLE package_command_outbox DROP COLUMN local_completion_order')
        conn.execute("UPDATE package_outbox_schema_info SET value='label-match-package-outbox-v11' WHERE key='schema_version'")
    PackageOutbox(outbox.db_path)
    reviewed = leases.get(lease_id=draft.operation_lease_id)
    assert reviewed['status'] == 'OPERATOR_REVIEW'
    assert reviewed['last_error_code'] == 'REPACK_PREDECESSOR_REVIEW_REQUIRED'
    for field in ('operation_result_id','operation_completed_at','consume_idempotency_key',
                  'token','artifact_json','binding_json','operation_snapshot_json','snapshot_hash','fence'):
        assert reviewed[field] == original[field]
    assert outbox.claim_next() is None


@pytest.mark.parametrize('saved_status', ['ACKED','CONFLICT'])
def test_unknown_legacy_history_also_blocks_a_later_repack_without_changing_old_delivery(tmp_path, saved_status):
    outbox = PackageOutbox(tmp_path/'history.sqlite3')
    drafts = [_draft_for_set(name) for name in ('old-A','old-B','old-C')]
    for draft in reversed(drafts):
        outbox.enqueue(draft)
    for draft in drafts:
        outbox.mark_local_completion_committed(outbox.get_by_set_id(draft.set_id)['idempotency_key'])
    with sqlite3.connect(outbox.db_path) as conn:
        conn.execute('DROP INDEX ix_package_local_completion_order')
        conn.execute('ALTER TABLE package_command_outbox DROP COLUMN predecessor_package_key')
        conn.execute('ALTER TABLE package_command_outbox DROP COLUMN local_completion_order')
        conn.execute("UPDATE package_outbox_schema_info SET value='label-match-package-outbox-v11' WHERE key='schema_version'")
        conn.execute("UPDATE package_command_outbox SET status=?,last_error_code='PRESERVED_DELIVERY_EVIDENCE',receipt_json=?", (saved_status,'{}'))
    reopened = PackageOutbox(outbox.db_path)
    old = [reopened.get_by_set_id(draft.set_id) for draft in drafts]
    assert all(row['status']==saved_status and row['local_completion_order'] is None for row in old)
    later = _draft_for_set('later')
    queued(reopened,later)
    assert reopened.claim_next() is None
    row = reopened.get_by_set_id(later.set_id)
    assert row['status']=='CONFLICT' and row['last_error_code']=='REPACK_PREDECESSOR_REVIEW_REQUIRED'
    assert row['predecessor_package_key'] is None and row['attempt_count']==0
    assert [reopened.get_by_set_id(draft.set_id) for draft in drafts] == old
    events = reopened.list_post_review_csv_pending()
    PackageOutbox(outbox.db_path)
    assert reopened.get_by_set_id(later.set_id)==row and reopened.list_post_review_csv_pending()==events
