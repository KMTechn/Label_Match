"""Transfer upload lag keeps durable packaging moving; HTTP and Tk are synthetic."""
import json
from datetime import datetime, timedelta, timezone
from types import MethodType, SimpleNamespace

import pytest

import Label_Match as label_module
import package_logistics as logistics
from package_logistics import PackageApiError, PackageOutbox, PackageOutboxProcessor
from tests.test_w9scanonly_central import central  # noqa: F401
from tests.test_deferred_lease_clock_retry import clock_case  # noqa: F401
from tests.test_w9scanonly_packaging import packaging, completions, products, scan  # noqa: F401


def complete(central, transition=True):
    app, dialogs, case, root, _ = central
    app._legacy_label_transition_enabled = transition
    case.instant[0] = datetime(2026, 9, 5, 3, 28, 54, tzinfo=timezone.utc)
    app.sealed_transfer_exchange_store = SimpleNamespace(blocking_rows=lambda **kwargs: [])
    # Use the actual notice and recovery-list consumers, with no visible widgets.
    app._set_active_package_submission_notice = MethodType(
        label_module.Label_Match._set_active_package_submission_notice, app)
    master = case.group['scan_payload']
    scan(app, master)
    root.run_until(lambda: not app.__dict__.get('_phs2_background_pending'), timeout=5)
    identity = app.current_set_info['id']
    for member in case.snapshot['work_group_source']['members']:
        scan(app, member['normalized_barcode'])
    assert len(completions(app)) == 1
    return app, case, root, identity


def processor(app, stage, error):
    client = app.package_logistics_client
    original = getattr(client, stage)
    def fail(*args, **kwargs):
        raise error
    setattr(client, stage, fail)
    return PackageOutboxProcessor(app.package_outbox, client,
        prepare_scanner_delivery=app._prepare_scanner_package_delivery), original


@pytest.mark.parametrize('transition', [False, True])
@pytest.mark.parametrize('stage', ['resolve_package_source_evidence', 'issue_operation_lease'])
def test_transfer_wait_keeps_row_notice_and_next_completion(central, transition, stage):
    app, case, root, identity = complete(central, transition)
    delivery, _ = processor(app, stage, PackageApiError(
        409, 'PHS_WORK_GROUP_TRANSFER_NOT_READY', 'synthetic upload lag', committed=False))
    assert delivery.drain(limit=1) == {'acked': 0, 'retry': 1, 'conflict': 0}
    row = app.package_outbox.get_by_set_id(identity)
    assert row['status'] == 'PENDING' and row['local_completion_committed'] == 1
    assert row['transfer_wait_started_at'] and row['retry_after_at']
    app._set_active_package_submission_notice(row)
    assert app.__dict__.get('_workflow_blocking_notice') is None
    assert app._workflow_notice is None
    from ui.workflow_snapshot_adapter import adapt_workflow_snapshot
    from ui.workflow_view_state import present_workflow
    view = present_workflow(adapt_workflow_snapshot(app.current_set_info,
        blocking_notice=app.__dict__.get('_workflow_blocking_notice') or app._workflow_notice))
    assert view.scan_input_enabled
    # A different tray can finish while the first tray remains durably pending.
    next_master = case.group['scan_payload'].replace('ITG=', 'ITG=NEXT-')
    scan(app, next_master)
    for value in products(item=case.group['item_id'], prefix='NEXT'):
        scan(app, value)
    assert len(completions(app)) == 2
    assert app.package_outbox.get_by_set_id(identity)['status'] == 'PENDING'


def test_transfer_wait_restart_backoff_and_server_recovery_once(central, monkeypatch):
    app, case, root, identity = complete(central)
    now = [datetime(2026, 10, 23, tzinfo=timezone.utc)]
    class Clock(datetime):
        @classmethod
        def now(cls, tz=None):
            return now[0].astimezone(tz)
    monkeypatch.setattr(logistics, 'datetime', Clock)
    delivery, original = processor(app, 'resolve_package_source_evidence', PackageApiError(
        409, 'PHS_WORK_GROUP_TRANSFER_NOT_READY', 'synthetic lag', committed=False))
    assert delivery.drain(limit=1)['retry'] == 1
    first = app.package_outbox.get_by_set_id(identity)
    started, key = first['transfer_wait_started_at'], first['idempotency_key']
    assert delivery.drain(limit=1) == {'acked': 0, 'retry': 0, 'conflict': 0}
    reopened = PackageOutbox(app.package_outbox.db_path)
    app.package_outbox = reopened
    assert reopened.get_by_set_id(identity)['transfer_wait_started_at'] == started
    for seconds in (30, 60, 120, 240, 480, 600, 600):
        row = reopened.get_by_set_id(identity)
        due = datetime.fromisoformat(row['retry_after_at'].replace('Z', '+00:00'))
        assert (due - now[0]).total_seconds() == seconds
        now[0] = due
        retry = PackageOutboxProcessor(reopened, app.package_logistics_client,
            prepare_scanner_delivery=app._prepare_scanner_package_delivery)
        assert retry.drain(limit=1)['retry'] == 1
        assert reopened.get_by_set_id(identity)['transfer_wait_started_at'] == started
    app.package_logistics_client.resolve_package_source_evidence = original
    now[0] += timedelta(days=2)
    # Real HTTP command builder and receipt validator, with a synthetic server response.
    from tests.test_package_logistics import _work_group_receipt
    from package_command_draft import PackageCommandDraft
    sent = []
    def transport(method, url, headers, body, timeout):
        if method == 'GET':
            return {'ok': True, 'data': case.response}
        command = json.loads(body) if isinstance(body, (bytes, str)) else body
        sent.append(command['idempotency_key'])
        draft = PackageCommandDraft.from_dict(json.loads(reopened.get_by_set_id(identity)['draft_json']))
        receipt = _work_group_receipt(draft, command)
        receipt['data']['operation_lease_consumption'] = {
            'contract_version': 'terminal-operation-lease-consume-v1',
            'lease_id': draft.operation_lease_id, 'status': 'CONSUMED',
            'fence': draft.operation_lease_fence,
            'operation_result_id': receipt['receipt_id'],
            'consumed_at': '2026-10-23T00:00:00Z',
        }
        return {'ok': True, 'data': receipt}
    http = logistics.PackageLogisticsClient(app.package_logistics_client.config, transport=transport)
    app.package_logistics_client.build_create_package_command = http.build_create_package_command
    app.package_logistics_client.create_package = http.create_package
    final = PackageOutboxProcessor(reopened, app.package_logistics_client,
        prepare_scanner_delivery=app._prepare_scanner_package_delivery)
    assert final.drain(limit=1)['acked'] == 1
    assert final.drain(limit=1) == {'acked': 0, 'retry': 0, 'conflict': 0}
    assert sent == [key]
    assert reopened.get_by_set_id(identity)['status'] == 'ACKED'
    assert reopened.list_transfer_waiting() == []


@pytest.mark.parametrize('status,code,committed', [
    (409, 'MEMBERSHIP_CONFLICT', False), (409, 'RESIDUAL_TAG_NOT_ALLOWED', False),
    (409, 'PHS_WORK_GROUP_TRANSFER_NOT_READY', True),
    (412, 'PHS_WORK_GROUP_TRANSFER_NOT_READY', False),
    (409, 'OPERATION_RESOURCE_ALREADY_LEASED', False),
])
def test_other_refusals_still_conflict(central, status, code, committed):
    app, case, root, identity = complete(central)
    delivery, _ = processor(app, 'resolve_package_source_evidence',
        PackageApiError(status, code, 'synthetic refusal', committed=committed))
    assert delivery.drain(limit=1)['conflict'] == 1
    assert app.package_outbox.get_by_set_id(identity)['status'] == 'CONFLICT'
    app._set_active_package_submission_notice(app.package_outbox.get_by_set_id(identity))
    assert app._workflow_blocking_notice.kind == 'submission_blocked'


def test_general_retry_notice_allows_scanning(central):
    app, case, root, identity = complete(central)
    delivery, _ = processor(app, 'resolve_package_source_evidence',
        PackageApiError(503, 'TEMPORARILY_UNAVAILABLE', 'synthetic retry', retryable=True, committed=False))
    assert delivery.drain(limit=1)['retry'] == 1
    row = app.package_outbox.get_by_set_id(identity)
    app._set_active_package_submission_notice(row)
    assert app.__dict__.get('_workflow_blocking_notice') is None
    assert app._workflow_notice is None


def test_transfer_wait_is_visible_in_existing_recovery_workbench(central):
    app, case, root, identity = complete(central)
    delivery, _ = processor(app, 'resolve_package_source_evidence', PackageApiError(
        409, 'PHS_WORK_GROUP_TRANSFER_NOT_READY', 'synthetic lag', committed=False))
    delivery.drain(limit=1)
    app._package_recovery_candidates = MethodType(label_module.Label_Match._package_recovery_candidates, app)
    found = [row for row in app._package_recovery_candidates() if row['set_id'] == identity]
    assert len(found) == 1 and found[0]['reason'] == '이적 기록 기다림'


def test_transfer_wait_count_is_only_a_nonblocking_notice(central):
    app, case, root, identity = complete(central)
    delivery, _ = processor(app, 'resolve_package_source_evidence', PackageApiError(
        409, 'PHS_WORK_GROUP_TRANSFER_NOT_READY', 'synthetic lag', committed=False))
    delivery.drain(limit=1)
    snapshot = app._read_package_review_snapshot()
    app._refresh_package_cancellation_review_notice(snapshot)
    notice = app._package_transfer_wait_review_notice
    assert notice.title == '이적 기록 기다림 1건'
    assert app.__dict__.get('_workflow_blocking_notice') is None
    assert notice.kind == 'scanner_notice'


def test_uncommitted_local_storage_recovery_notice_remains_blocking(central):
    app, case, root, identity = complete(central)
    row = dict(app.package_outbox.get_by_set_id(identity), local_completion_committed=0)
    app._set_active_package_submission_notice(row)
    assert app._workflow_blocking_notice.kind == 'submission_blocked'


def test_transfer_not_ready_after_command_is_saved_still_conflicts(central):
    app, case, root, identity = complete(central)
    def build(draft, *, idempotency_key):
        return 'synthetic-source', {'idempotency_key': idempotency_key, 'payload': {}}
    def send(command):
        raise PackageApiError(409, 'PHS_WORK_GROUP_TRANSFER_NOT_READY', 'synthetic refusal', committed=False)
    delivery = PackageOutboxProcessor(app.package_outbox,
        SimpleNamespace(build_create_package_command=build, create_package=send),
        prepare_scanner_delivery=lambda draft: draft)
    assert delivery.drain(limit=1) == {'acked': 0, 'retry': 0, 'conflict': 1}
    assert app.package_outbox.get_by_set_id(identity)['status'] == 'CONFLICT'
