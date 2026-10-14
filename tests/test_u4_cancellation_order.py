"""A reopened tray must wait for its cancellation, without holding other trays."""
from dataclasses import replace
import sqlite3

import pytest

from package_logistics import PackageCancellationOutbox, PackageLogisticsError, PackageOutbox
from tests.test_package_logistics import (
    _ack_package_creation, _cancellation_intent, _cancellation_receipt, _draft_for_set,
)


def queued(outbox, draft):
    row = outbox.enqueue(draft)
    outbox.mark_local_completion_committed(row['idempotency_key'])
    return row


@pytest.mark.parametrize('status', ['PENDING', 'SENDING', 'CONFLICT'])
@pytest.mark.parametrize('identity', ['transfer', 'input_tag'])
def test_repack_waits_for_same_source_cancel_ack_and_other_tray_can_send(tmp_path, status, identity):
    outbox = PackageOutbox(tmp_path / 'outbox.sqlite3')
    original = _draft_for_set('ORIGINAL')
    if identity == 'input_tag':
        original = replace(original, source_input_tag_id='ITG-REPACK')
    _ack_package_creation(outbox, original)
    cancels = PackageCancellationOutbox(outbox.db_path)
    intent = _cancellation_intent(original)
    cancels.enqueue(intent)
    cancels.mark_local_event_committed(intent.cancellation_event_id)
    if status != 'PENDING':
        claim = cancels.claim_next()
        if status == 'CONFLICT':
            cancels.mark_conflict(claim['idempotency_key'], PackageLogisticsError('synthetic cancel conflict'))
    repack = replace(_draft_for_set('REPACK'), source_input_tag_id=original.source_input_tag_id)
    if identity == 'input_tag':
        repack = replace(repack, source_bundle_id='')
    repack_row = queued(outbox, repack)
    other = replace(_draft_for_set('OTHER'), source_bundle_id='TRANSFER-OTHER', source_input_tag_id='ITG-OTHER')
    other_row = queued(outbox, other)
    assert outbox.claim_next()['idempotency_key'] == other_row['idempotency_key']
    assert outbox.claim_next() is None
    assert outbox.get_by_set_id(repack.set_id)['attempt_count'] == 0
    assert outbox.get_by_set_id(repack.set_id)['status'] == 'PENDING'
    if status == 'CONFLICT':
        assert cancels.get_by_event_id(intent.cancellation_event_id)['status'] == 'CONFLICT'
        return
    if status == 'PENDING':
        cancels.claim_next()
    assert cancels.mark_acked(cancels.get_by_event_id(intent.cancellation_event_id)['idempotency_key'], _cancellation_receipt(original, intent=intent))
    assert outbox.claim_next()['idempotency_key'] == repack_row['idempotency_key']


def test_predecessor_follows_durable_completion_order_not_enqueue_order_or_clock(tmp_path, monkeypatch):
    import package_logistics

    monkeypatch.setattr(package_logistics, "utc_now", lambda: "2026-10-14T00:00:00Z")
    monkeypatch.setattr(package_logistics, "_utc_before", lambda seconds: "2026-10-13T23:55:00Z")
    outbox = PackageOutbox(tmp_path / "outbox.db")
    # C's intent exists first, but its durable local completion happens last.
    drafts = [_draft_for_set(name) for name in ("A", "B", "C")]
    rows = {draft.set_id: outbox.enqueue(draft) for draft in reversed(drafts)}
    for draft in drafts:
        outbox.mark_local_completion_committed(rows[draft.set_id]["idempotency_key"])
    expected = [None, rows["A"]["idempotency_key"], rows["B"]["idempotency_key"]]
    restarted = PackageOutbox(outbox.db_path)
    for order, (draft, predecessor) in enumerate(zip(drafts, expected), 1):
        row = restarted.get_by_set_id(draft.set_id)
        assert row["local_completion_order"] == order
        assert row["predecessor_package_key"] == predecessor
        restarted.mark_local_completion_committed(row["idempotency_key"])
        assert restarted.get_by_set_id(draft.set_id) == row
    assert restarted.claim_next()["set_id"] == "A"
    assert restarted.claim_next() is None


@pytest.mark.parametrize("store_type", [PackageOutbox, PackageCancellationOutbox])
def test_v11_migration_freezes_predecessors_without_changing_live_delivery(tmp_path, store_type):
    outbox = PackageOutbox(tmp_path / "outbox.db")
    originals = [_draft_for_set(name) for name in ("A", "B", "C")]
    for draft in originals:
        queued(outbox, draft)
    claimed = outbox.claim_next()
    before = [outbox.get_by_set_id(draft.set_id) for draft in originals]
    with sqlite3.connect(outbox.db_path) as conn:
        conn.execute("DROP INDEX ix_package_local_completion_order")
        conn.execute("ALTER TABLE package_command_outbox DROP COLUMN local_completion_order")
        conn.execute("ALTER TABLE package_command_outbox DROP COLUMN predecessor_package_key")
        conn.execute("UPDATE package_outbox_schema_info SET value='label-match-package-outbox-v11' WHERE key='schema_version'")
    store_type(outbox.db_path)
    restarted = PackageOutbox(outbox.db_path)
    expected = [None, before[0]["idempotency_key"], before[1]["idempotency_key"]]
    for order, (draft, old, predecessor) in enumerate(zip(originals, before, expected), 1):
        row = restarted.get_by_set_id(draft.set_id)
        assert row["local_completion_order"] == order
        assert row["predecessor_package_key"] == predecessor
        causal = {"local_completion_order", "predecessor_package_key"}
        assert {key: value for key, value in row.items() if key not in causal} == {
            key: value for key, value in old.items() if key not in causal}
    assert restarted.get_by_set_id(claimed["set_id"])["status"] == "SENDING"
    assert restarted.claim_next() is None


def test_offline_original_create_can_send_before_its_deferred_cancel_but_repack_cannot(tmp_path):
    outbox = PackageOutbox(tmp_path / 'outbox.sqlite3')
    original = _draft_for_set('ORIGINAL')
    original_row = queued(outbox, original)
    cancels = PackageCancellationOutbox(outbox.db_path)
    intent = _cancellation_intent(original)
    assert cancels.enqueue(intent)['status'] == 'DEFERRED'
    cancels.mark_local_event_committed(intent.cancellation_event_id)
    queued(outbox, _draft_for_set('REPACK'))
    assert outbox.claim_next()['idempotency_key'] == original_row['idempotency_key']
    assert outbox.claim_next() is None
    assert outbox.get_by_set_id('REPACK')['attempt_count'] == 0
