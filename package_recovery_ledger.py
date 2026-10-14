"""Append-only cancellation recovery evidence and once-only review closure."""
import json

from package_command_draft import canonical_json
from package_errors import PackageLogisticsError
from package_recovery import CONTRACT_SHA256, recovery_expectation, validate_recovery_read


def initialize_recovery_schema(conn, *, validate=True):
    conn.execute('''CREATE TABLE IF NOT EXISTS package_recovery_evidence (
        cancel_key TEXT PRIMARY KEY, package_key TEXT NOT NULL,
        cancel_receipt_id TEXT NOT NULL UNIQUE, recovery_receipt_id TEXT NOT NULL UNIQUE,
        expectation_json TEXT NOT NULL, evidence_json TEXT NOT NULL,
        contract_sha256 TEXT NOT NULL, confirmed_at TEXT NOT NULL,
        operator_id TEXT NOT NULL, operator_name TEXT NOT NULL)''')
    conn.execute('''CREATE TABLE IF NOT EXISTS package_recovery_review_closures (
        review_key TEXT PRIMARY KEY, cancel_key TEXT NOT NULL,
        review_snapshot_json TEXT NOT NULL, closed_at TEXT NOT NULL,
        operator_id TEXT NOT NULL, operator_name TEXT NOT NULL)''')
    if not validate:
        return
    # A corrupted record must never become authority to skip a product/source hold.
    for row in conn.execute('SELECT * FROM package_recovery_evidence').fetchall():
        expected = expectation_for_cancel(conn, row['cancel_key'])
        if (expected is None or canonical_json(expected) != row['expectation_json']
                or row['contract_sha256'] != CONTRACT_SHA256
                or validate_recovery_read(json.loads(row['evidence_json']), expected) != 'RECOVERED'):
            raise PackageLogisticsError('PACKAGE_RECOVERY_STORED_EVIDENCE_INVALID')
    if conn.execute('''SELECT 1 FROM package_recovery_review_closures AS closure
        LEFT JOIN package_recovery_evidence AS proof ON proof.cancel_key=closure.cancel_key
        WHERE proof.cancel_key IS NULL LIMIT 1''').fetchone():
        raise PackageLogisticsError('PACKAGE_RECOVERY_STORED_EVIDENCE_INVALID')
    for closure in conn.execute('SELECT * FROM package_recovery_review_closures').fetchall():
        snapshot = json.loads(closure['review_snapshot_json'])
        target = recovery_target(conn, snapshot['set_id'], include_closed=True)
        if (target is None or target['review']['idempotency_key'] != closure['review_key']
                or target['cancel']['idempotency_key'] != closure['cancel_key']
                or canonical_json(target['review']) != closure['review_snapshot_json']):
            raise PackageLogisticsError('PACKAGE_RECOVERY_STORED_EVIDENCE_INVALID')


def expectation_for_cancel(conn, cancel_key):
    row = conn.execute('''SELECT package.receipt_json AS creation, cancel.receipt_json AS cancellation
        FROM package_cancellation_outbox AS cancel JOIN package_command_outbox AS package
        ON package.idempotency_key=cancel.package_idempotency_key
        WHERE cancel.idempotency_key=? AND cancel.status='ACKED' AND package.status='ACKED'
        AND NOT EXISTS (SELECT 1 FROM package_receipt_rechecks AS recheck
            WHERE recheck.idempotency_key=cancel.idempotency_key
            AND recheck.outbox_kind='cancel' AND recheck.verified_at IS NULL)''', (cancel_key,)).fetchone()
    if row is None:
        return None
    return recovery_expectation(json.loads(row['creation']), json.loads(row['cancellation']))


def recovery_target(conn, set_id, *, include_closed=False):
    """Follow only proven predecessor links; ambiguous legacy chains grant nothing."""
    row = conn.execute('SELECT * FROM package_command_outbox WHERE set_id=?', (set_id,)).fetchone()
    if row is None:
        return None
    review = dict(row)
    if not include_closed and conn.execute('SELECT 1 FROM package_recovery_review_closures WHERE review_key=?',
                    (row['idempotency_key'],)).fetchone():
        return None
    if not (row['local_completion_committed'] and (row['status'] == 'ACKED' or
            (row['status'] == 'CONFLICT' and row['last_error_code'] in
             ('REPACK_REOPEN_REFUSED', 'REPACK_PREDECESSOR_REVIEW_REQUIRED')))):
        return None
    visited = set()
    while row is not None and row['idempotency_key'] not in visited:
        visited.add(row['idempotency_key'])
        cancels = conn.execute('''SELECT * FROM package_cancellation_outbox
            WHERE package_idempotency_key=? AND status='ACKED' ''', (row['idempotency_key'],)).fetchall()
        for cancel in cancels:
            try:
                expected = expectation_for_cancel(conn, cancel['idempotency_key'])
            except (PackageLogisticsError, ValueError, TypeError):
                continue
            if expected is not None:
                return dict(review=review, cancel=dict(cancel), package=dict(row), expected=expected)
        row = conn.execute('SELECT * FROM package_command_outbox WHERE idempotency_key=?',
                           (row['predecessor_package_key'],)).fetchone()
    return None


def close_recovery(store, target, response, *, operator_id, operator_name):
    if not str(operator_id or '').strip():
        raise PackageLogisticsError('PACKAGE_RECOVERY_OPERATOR_REQUIRED')
    if validate_recovery_read(response, target['expected']) != 'RECOVERED':
        return False
    with store._lock, store._connect() as conn:
        conn.execute('BEGIN IMMEDIATE')
        current = recovery_target(conn, target['review']['set_id'])
        if current is None or canonical_json(current) != canonical_json(target):
            conn.rollback()
            return False
        now = store._utc_now()
        cancel, package, review = target['cancel'], target['package'], target['review']
        existing = conn.execute('SELECT * FROM package_recovery_evidence WHERE cancel_key=?',
                                (cancel['idempotency_key'],)).fetchone()
        if existing is not None:
            old = json.loads(existing['evidence_json'])
            if (old['recovery_receipt_id'] != response['recovery_receipt_id']
                    or old['transfer_bundle_id'] != response['transfer_bundle_id']):
                conn.rollback()
                return False
        else:
            conn.execute('''INSERT INTO package_recovery_evidence VALUES (?,?,?,?,?,?,?,?,?,?)''',
                (cancel['idempotency_key'], package['idempotency_key'], target['expected']['cancel_receipt_id'],
                 response['recovery_receipt_id'], canonical_json(target['expected']), canonical_json(response),
                 CONTRACT_SHA256, now, str(operator_id), str(operator_name)))
        # Close only rows proven to derive from this original refused cancellation.
        # Keep their statuses, commands, markers and receipts entirely unchanged.
        candidates = conn.execute("SELECT set_id FROM package_command_outbox WHERE status IN ('ACKED','CONFLICT')").fetchall()
        for candidate in candidates:
            bound = recovery_target(conn, candidate['set_id'])
            if bound is not None and bound['cancel']['idempotency_key'] == cancel['idempotency_key']:
                conn.execute('''INSERT INTO package_recovery_review_closures VALUES (?,?,?,?,?,?)''',
                    (bound['review']['idempotency_key'], cancel['idempotency_key'], canonical_json(bound['review']),
                     now, str(operator_id), str(operator_name)))
        conn.commit()
    return True
