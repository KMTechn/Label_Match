"""Hidden F5 recovery serializes writers; other HTTP keeps the shared guard."""
import copy
import http.server
import json
import os
import threading
from urllib.parse import urlsplit

import pytest

import writer_session_fence as fence
from package_logistics import PackageClientConfig, PackageLogisticsClient, _default_transport
from phs_label_workflow import PHSLabelExchangeCoordinator, PHSLabelExchangeJournal
from tests.conftest import register_lm_mutex
from tests.lock_remote_guard import LockRemoteGuard, RemoteWhileLocked
from tests.test_phs_label_workflow import (
    SCOPE, TARGET_INSTRUCTION, _Client, _Printer, _Renderer, _current_set,
)
from tests.test_legacy_label_transition import MASTER
from tests.test_w9scanonly_packaging import packaging, products, scan
from tests.test_tk_serial_ui_lane import FakeTkRoot
from tk_serial_ui_lane import LaneTask, TkSerialUiLane


@pytest.mark.parametrize('source', [
    'phs_single_exchange', 'phs_reconciliation_exchange',
    'phs_current_recovery', 'phs_reconciliation_recovery',
])
def test_only_f5_client_calls_are_exempt_while_f5_admission_is_held(source, lock_remote):
    observed = []
    def answer(*args):
        observed.append(fence._WRITER_LOCAL.depth)
        return {'ok': True, 'data': _Client()._exchange('COMMITTED', 3)}
    client = PackageLogisticsClient(PackageClientConfig(
        'https://synthetic.invalid', 'synthetic', SCOPE, 'host', 'device'),
        transport=lock_remote.wrap_transport(answer, 'F5 activation'))
    with fence.writer_admission(source):
        client.activate_phs_label_exchange('EXCHANGE-ONE', authority_scope_id=SCOPE,
                                          expected_exchange_version=2)
    assert observed and observed[0] > 0
    assert any(event['event'] == 'remote_f5_locked' and event['locks']
               for event in lock_remote.events)


def test_f5_exemption_does_not_hide_unrelated_http(monkeypatch):
    guard = LockRemoteGuard()
    register_lm_mutex(monkeypatch, guard)
    client = PackageLogisticsClient(PackageClientConfig(
        'https://synthetic.invalid', 'synthetic', SCOPE, 'host', 'device'),
        transport=guard.wrap_transport(lambda *args: {'ok': True, 'data': {}}, 'receipt GET'))
    with fence.writer_admission('phs_current_recovery'):
        with pytest.raises(RemoteWhileLocked, match='writer mutex'):
            client.get_receipt('unrelated-command', authority_scope_id=SCOPE)
    assert any(event['event'] == 'remote' and event['locks'] for event in guard.events)
    assert not any(event['event'] == 'remote_f5_locked' for event in guard.events)


def test_f5_method_outside_f5_keeps_remote_admission_release(lock_remote):
    depths = []
    def answer(*args):
        depths.append(fence._WRITER_LOCAL.depth)
        return {'ok': True, 'data': _Client()._exchange('COMMITTED', 3)}
    client = PackageLogisticsClient(PackageClientConfig(
        'https://synthetic.invalid', 'synthetic', SCOPE, 'host', 'device'),
        transport=lock_remote.wrap_transport(answer, 'active-label GET'))
    with fence.writer_admission('gui_package_enqueue'):
        client.get_phs_label_exchange('EXCHANGE-ONE', authority_scope_id=SCOPE)
    assert depths == [0]
    assert not any(event['event'] == 'remote_f5_locked' for event in lock_remote.events)


def test_f5_activation_blocks_new_generation_and_preserves_waiting_scan(
        tmp_path, packaging, lock_remote):
    # Coordinator 2026-10-02: F5 alone waits; queued writers run after its response.
    app, dialogs = packaging
    scan(app, MASTER)
    entered, release = threading.Event(), threading.Event()
    recovery_started, recovered = threading.Event(), threading.Event()
    central = _Client()
    class Handler(http.server.BaseHTTPRequestHandler):
        def log_message(self, *args):
            pass
        def do_POST(self):
            self.rfile.read(int(self.headers.get('Content-Length', 0)))
            response = central.activate_phs_label_exchange('EXCHANGE-ONE')
            encoded = json.dumps({'ok': True, 'data': response}).encode()
            entered.set()
            assert release.wait(15)
            self.send_response(200)
            self.send_header('Content-Length', str(len(encoded)))
            self.end_headers()
            self.wfile.write(encoded)
    server = http.server.ThreadingHTTPServer(('127.0.0.1', 0), Handler)
    service = threading.Thread(target=server.serve_forever, daemon=True)
    service.start()
    def routed(method, url, headers, body, timeout):
        local = 'http://127.0.0.1:%d%s' % (server.server_address[1], urlsplit(url).path)
        return _default_transport(method, local, headers, body, timeout)
    actual = PackageLogisticsClient(PackageClientConfig(
        'https://synthetic.invalid', 'synthetic', SCOPE, 'host', 'device',
        timeout_seconds=10), transport=routed)
    class FirstClient:
        def __getattr__(self, name):
            return getattr(central, name)
        def activate_phs_label_exchange(self, *args, **kwargs):
            return actual.activate_phs_label_exchange(*args, **kwargs)
    current = _current_set()
    checkpoint = tmp_path / 'current.json'
    writes, errors, results = [], [], {}
    def persist():
        encoded = json.dumps(current, sort_keys=True).encode()
        with checkpoint.open('wb') as stream:
            stream.write(encoded)
            stream.flush()
            os.fsync(stream.fileno())
        writes.append(copy.deepcopy(current))
        return True
    persist()
    journal_path = tmp_path / 'journal.json'
    first = PHSLabelExchangeCoordinator(FirstClient(), PHSLabelExchangeJournal(journal_path),
        _Renderer(tmp_path / 'first.png'), _Printer())
    second = PHSLabelExchangeCoordinator(central, PHSLabelExchangeJournal(journal_path),
        _Renderer(tmp_path / 'second.png'), _Printer())
    def run_old():
        try:
            results['old'] = first.execute_single(current, TARGET_INSTRUCTION,
                persist_current_set=persist).status
        except BaseException as error:
            errors.append(type(error).__name__)
    def run_new():
        recovery_started.set()
        try:
            results['new'] = second.recover_current(current, persist_current_set=persist)
        except BaseException as error:
            errors.append(type(error).__name__)
        finally:
            recovered.set()
    product = products()[0]
    root = FakeTkRoot()
    app._ui_lane_generation = 0
    app._ui_lane_busy_task = 'f5-label-exchange'
    app.after, app.after_cancel = root.after, root.after_cancel
    app.ui_lane = TkSerialUiLane(root, poll_ms=1,
        generation_provider=lambda: app._ui_lane_generation)
    rejected = []
    app._show_ui_lane_rejection = rejected.append
    new = threading.Thread(target=run_new)
    try:
        assert app.ui_lane.submit(LaneTask('f5-label-exchange', 0, run_old,
            lambda _value: None, lambda error: errors.append(str(error)))).accepted
        assert entered.wait(10), 'real activation HTTP did not start'
        before = copy.deepcopy(current)
        before_checkpoint = checkpoint.read_bytes()
        before_journal = journal_path.read_bytes()
        write_count = len(writes)
        new.start()
        assert recovery_started.wait(2)
        assert not recovered.wait(0.3)
        assert current == before and current['active_label_version'] == 1
        assert checkpoint.read_bytes() == before_checkpoint and len(writes) == write_count
        assert journal_path.read_bytes() == before_journal
        scan(app, product)
        assert rejected == ['busy']
        assert app.entry.get() == product
        assert app.current_set_info['raw'] == [MASTER]
        assert app.data_manager.load_current_state()['current_set_info']['raw'] == [MASTER]
        release.set()
        root.run_until(lambda: not app.ui_lane.is_busy(), timeout=10)
        new.join(10)
        assert not new.is_alive()
        assert errors == []
        assert results == {'old': 'COMMITTED', 'new': None}
        assert all(saved['active_label_version'] == 1 for saved in writes)
        assert json.loads(checkpoint.read_bytes()) == current
        assert first.journal.load()['status'] == 'COMMITTED'
        app._handle_scan_enter()  # The same preserved entry, after F5 becomes idle.
        assert app.current_set_info['raw'] == [MASTER, product]
        assert app.data_manager.load_current_state()['current_set_info']['raw'] == [MASTER, product]
        assert dialogs == []
    finally:
        release.set()
        if new.ident is not None:
            new.join(15)
        root.run_until(lambda: not app.ui_lane.is_busy(), timeout=15)
        app.ui_lane.close_idle()
        server.shutdown()
        server.server_close()
        service.join(3)
