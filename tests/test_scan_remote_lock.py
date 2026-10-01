"""Public scan ingress remains durable during actual transport delays."""
import json
import os
from pathlib import Path
import threading
import time
import subprocess
import sys

import pytest

from direct_sync_push import build_source_file_plan, upload_source_file
from tests.test_direct_sync_push import make_manifest, write_csv, make_credentials, FakeSession, FakeResponse
from tests.test_w9scanonly_packaging import packaging, products, scan
from tests.test_legacy_label_transition import MASTER
from tests.test_tk_serial_ui_lane import FakeTkRoot
from tk_serial_ui_lane import TkSerialUiLane
from package_logistics import PackageClientConfig, PackageLogisticsClient, PackageOutbox, PackageOutboxProcessor
from label_admin_pin import AdminPinClient
import writer_session_fence as fence


@pytest.mark.parametrize('delay', [0.3, 6.0, 35.0])
@pytest.mark.parametrize('kind', ['relay', 'relay-child', 'package', 'pin'])
def test_public_scans_durable_while_relay_transport_waits(packaging, tmp_path, lock_remote, delay, kind):
    app, dialogs = packaging
    scan(app, MASTER)
    root = FakeTkRoot()
    app._ui_lane_generation = 0
    app.after = root.after
    app.after_cancel = root.after_cancel
    app.ui_lane = TkSerialUiLane(root, poll_ms=1, generation_provider=lambda: app._ui_lane_generation)
    _manifest, manifest_path = make_manifest(tmp_path)
    credentials = make_credentials()
    plan = build_source_file_plan(source_file_path=write_csv(tmp_path),
        producer_manifest_path=manifest_path, credentials=credentials)
    session = FakeSession(FakeResponse(503, {'committed': False, 'retryable': True}))
    entered = threading.Event()
    completed = threading.Event()
    failures = []
    original = session.post

    def delayed(*args, **kwargs):
        entered.set()
        time.sleep(delay)
        return original(*args, **kwargs)

    def delayed_answer(value):
        entered.set()
        time.sleep(delay)
        return value

    session.post = lock_remote.wrap_transport(delayed, 'relay upload')
    remote = lambda: upload_source_file(plan, credentials, session=session, status_dir=tmp_path / 'status')
    if kind == 'package':
        client = PackageLogisticsClient(PackageClientConfig('https://logistics.test', 'synthetic', 'scope', 'host', 'device'),
            transport=lock_remote.wrap_transport(lambda *args: delayed_answer({'ok': True, 'data': {}}), 'package command'))
        remote = lambda: client.issue_operation_lease(authority_scope_id='scope', operation='CREATE_PACKAGE',
            scan_payload='synthetic scan', idempotency_key='synthetic-request')
    elif kind == 'pin':
        client = AdminPinClient(credentials, transport=lock_remote.wrap_transport(
            lambda *args: delayed_answer((200, b'{"ok":true}')), 'PIN request'))
        remote = lambda: client.post('status', {'probe': True})
    def upload():
        try:
            remote()
        except Exception as error:
            failures.append(type(error).__name__)
        finally:
            completed.set()
    worker = threading.Thread(target=upload)
    child = None
    child_root = tmp_path / 'relay-child'
    if kind == 'relay-child':
        child_root.mkdir()
        child = subprocess.Popen([sys.executable, '-B', '-m', 'tests._scan_upload_child', str(child_root), str(delay)],
            stdout=subprocess.PIPE, stderr=subprocess.PIPE,
            creationflags=subprocess.CREATE_NEW_PROCESS_GROUP | subprocess.CREATE_NO_WINDOW)
        until = time.monotonic() + 5
        while not (child_root / 'entered').exists() and child.poll() is None and time.monotonic() < until:
            time.sleep(0.01)
        if (child_root / 'entered').exists():
            entered.set()
    else:
        worker.start()
    try:
        assert entered.wait(3), 'transport did not reach its delay'
        durations = []
        for value in products()[:2]:
            started = time.monotonic()
            scan(app, value)
            paths = list(Path(app.save_directory).rglob('_current_set_state*.json'))
            assert any(value in path.read_text(encoding='utf-8-sig') for path in paths)
            durations.append(time.monotonic() - started)
            assert app.entry.cget('state') == 'normal'
            assert not (completed.is_set() if child is None else (child_root / 'done').exists()), 'scan waited for remote completion'
        assert max(durations) < 0.3, durations
        assert app.current_set_info['raw'] == [MASTER, *products()[:2]]
        began_completion = time.monotonic()
        for value in products()[2:]:
            scan(app, value)
        root.run_until(lambda: not app.ui_lane.is_busy(), timeout=2)
        assert app.data_manager.flush(timeout=2)
        assert len(app.durable_commits) == 1
        assert not app.current_set_info['raw']
        completion_seconds = time.monotonic() - began_completion
        assert completion_seconds < 1, completion_seconds
        scan(app, MASTER)
        scan(app, products(prefix='NEXT')[0])
        assert app.current_set_info['raw'] == [MASTER, products(prefix='NEXT')[0]]
        assert dialogs == []
        metrics = os.environ.get('SUB85_METRICS')
        if metrics:
            with Path(metrics).open('a', encoding='utf-8') as stream:
                stream.write(json.dumps({'kind': kind, 'delay': delay, 'scan_durable_seconds': durations,
                    'completion_seconds': completion_seconds, 'input_enabled': True, 'failures': failures}) + '\n')
    finally:
        if child is None:
            worker.join(40)
            assert not worker.is_alive()
        else:
            _output, errors = child.communicate(timeout=40)
            if errors:
                metrics = os.environ.get('SUB85_METRICS')
                if metrics:
                    Path(metrics).with_suffix('.child-error.log').write_bytes(errors[:50000])
            assert child.returncode == 0
            observed = json.loads((child_root / 'done').read_text(encoding='utf-8'))
            assert observed['violations'] == 0 and observed['pid'] != os.getpid()
            assert observed['mutex_name'] == fence.writer_admission_mutex_name(fence.canonical_control_root())
        if app.ui_lane.is_busy():
            root.run_until(lambda: not app.ui_lane.is_busy(), timeout=10)
        app.ui_lane.close_idle()
    assert failures == []


@pytest.mark.parametrize('mutation', ['fence', 'row'])
def test_fence_change_preserves_saved_command_and_recovers_same_receipt_once(tmp_path, monkeypatch, lock_remote, mutation):
    from tests.test_package_logistics import _draft, _projection, _receipt, SCOPE
    draft = _draft()
    outbox = PackageOutbox(tmp_path / 'pending.sqlite3')
    queued = outbox.enqueue(draft)
    outbox.mark_local_completion_committed(queued['idempotency_key'])
    effects = set()
    deny_apply = False
    original = fence._admitted_writer_sources
    def admitted(*args, **kwargs):
        if deny_apply:
            raise fence.WriterFencedError('ACTIVE_WRITER_FENCE', 'synthetic deployment')
        return original(*args, **kwargs)
    monkeypatch.setattr(fence, '_admitted_writer_sources', admitted)
    def transport(method, url, headers, body, timeout):
        nonlocal deny_apply
        if method == 'POST':
            effects.add(json.loads(body)['idempotency_key'])
            if mutation == 'fence':
                deny_apply = True
            else:
                from package_logistics import PackageTransportError
                with fence.writer_admission('gui_package_enqueue'):
                    outbox.mark_retry(queued['idempotency_key'], PackageTransportError('newer local retry'))
            return {'ok': True, 'data': _receipt(draft)}
        if '/receipts/' in url:
            return {'ok': True, 'data': _receipt(draft)}
        return {'ok': True, 'data': _projection()}
    client = PackageLogisticsClient(PackageClientConfig('https://logistics.test', 'synthetic', SCOPE, 'host', 'device'),
        transport=lock_remote.wrap_transport(transport, 'package effect'))
    processor = PackageOutboxProcessor(outbox, client)
    if mutation == 'fence':
        with pytest.raises(fence.WriterFencedError):
            processor.drain(limit=1)
    else:
        assert processor.drain(limit=1)['acked'] == 0
    pending = outbox.get_by_set_id(draft.set_id)
    assert pending['status'] == ('SENDING' if mutation == 'fence' else 'PENDING')
    assert pending['command_json'] and pending['receipt_json'] is None
    assert effects == {queued['idempotency_key']}
    deny_apply = False
    outbox._utc_before = lambda _seconds: '9999-01-01T00:00:00Z'
    assert processor.drain(limit=1)['acked'] == 1
    assert processor.drain(limit=1)['acked'] == 0
    final = outbox.get_by_set_id(draft.set_id)
    assert final['command_json'] == pending['command_json']
    assert final['idempotency_key'] == queued['idempotency_key']
    assert effects == {queued['idempotency_key']}
