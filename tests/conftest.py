"""Keep default writer admission isolated across pytest runs and their children."""

import functools
import threading
import time

import pytest


@pytest.fixture
def isolated_dpapi(monkeypatch, tmp_path):
    """Use a shared authenticated double unless a guest explicitly requests native DPAPI."""
    import os
    from pathlib import Path
    from tests.dpapi_double import DPAPIDouble

    if os.environ.get('KMTECH_TEST_REAL_DPAPI') == '1':
        return lambda _module: None
    from cryptography.hazmat.primitives.ciphers.aead import AESGCM
    import tools.label_auth_recovery_canary as canary

    material = AESGCM.generate_key(bit_length=256)
    cipher = DPAPIDouble(material)
    cipher.install(monkeypatch)
    child_material = tmp_path / 'dpapi-double.bin'
    child_material.write_bytes(material)
    original = canary.subprocess.Popen

    def launch(command, *args, **kwargs):
        if '--_recovery-child' in command:
            command = list(command)
            index = command.index(str(Path(canary.__file__).resolve()))
            command[index:index + 1] = [str(Path(__file__).with_name('dpapi_double.py')),
                                      str(child_material), command[index]]
            kwargs['creationflags'] = kwargs.get('creationflags', 0) | getattr(
                canary.subprocess, 'CREATE_NO_WINDOW', 0)
        return original(command, *args, **kwargs)

    monkeypatch.setattr(canary.subprocess, 'Popen', launch)
    return lambda module: cipher.install(monkeypatch, registration=module)


@pytest.fixture(scope="session", autouse=True)
def _isolated_writer_control_root(tmp_path_factory):
    # Children inherit this root; competing writers still share its real mutex.
    with pytest.MonkeyPatch.context() as patch:
        patch.setenv("KMTECH_LABEL_WRITER_TEST_MODE", "1")
        patch.setenv(
            "KMTECH_LABEL_WRITER_CONTROL_ROOT",
            str(tmp_path_factory.getbasetemp() / "writer-control"),
        )
        yield



from tests.lock_remote_guard import LockRemoteGuard


def register_lm_mutex(monkeypatch, guard):
    import writer_session_fence as fence
    acquire = fence._acquire_named_mutex

    # Preserve the concrete native lease type, including suspend/resume.
    def registered(*args, **kwargs):
        raw = acquire(*args, **kwargs)
        if raw is not None:
            guard.enter(id(raw), 'writer mutex')
        return raw

    release = fence._NamedMutexLease.release
    suspend = fence._NamedMutexLease.suspend
    resume = fence._NamedMutexLease.resume
    def released(lease):
        release(lease)
        guard.leave(id(lease))
    def suspended(lease):
        suspend(lease)
        guard.leave(id(lease))
    def resumed(lease, *args):
        resume(lease, *args)
        guard.enter(id(lease), 'writer mutex')

    monkeypatch.setattr(fence, '_acquire_named_mutex', registered)
    monkeypatch.setattr(fence._NamedMutexLease, 'release', released)
    monkeypatch.setattr(fence._NamedMutexLease, 'suspend', suspended)
    monkeypatch.setattr(fence._NamedMutexLease, 'resume', resumed)
    # Coordinator 2026-10-02: only saved F5 recovery calls retain admission.
    from package_logistics import PackageLogisticsClient
    f5_sources = {
        'phs_current_recovery', 'phs_reconciliation_recovery',
    }
    for name in (
        'prepare_phs_label_exchange',
        'prepare_phs_reconciliation_label_exchange', 'get_phs_label_exchange',
        'request_phs_label_print', 'complete_phs_label_print',
        'activate_phs_label_exchange',
    ):
        original = getattr(PackageLogisticsClient, name)
        @functools.wraps(original)
        def f5_call(*args, _original=original, **kwargs):
            depth = getattr(guard.local, 'f5_call_depth', 0)
            guard.local.f5_call_depth = depth + 1
            try:
                return _original(*args, **kwargs)
            finally:
                guard.local.f5_call_depth = depth
        monkeypatch.setattr(PackageLogisticsClient, name, f5_call)
    transport = guard.transport
    def registered_transport(label):
        if (getattr(guard.local, 'f5_call_depth', 0)
                and getattr(fence._WRITER_LOCAL, 'source', None) in f5_sources):
            guard.events.append({
                'event': 'remote_f5_locked', 'transport': label,
                'locks': [{'kind': kind, 'site': site}
                          for _identity, kind, site in guard.held()],
                'thread': threading.get_ident(), 'at': time.monotonic(),
                'node': guard.node,
            })
            return
        transport(label)
    monkeypatch.setattr(guard, 'transport', registered_transport)
    guard.instrument_transports(monkeypatch)


@pytest.fixture
def lock_remote(monkeypatch, request):
    guard = LockRemoteGuard()
    register_lm_mutex(monkeypatch, guard)
    import package_outbox_processor
    import package_logistics
    import package_outbox
    import phs_label_workflow
    for owner, attributes in (
        (package_outbox_processor.PackageOutboxProcessor, ('_drain_lock',)),
        (package_logistics.PackageCancellationOutboxProcessor, ('_drain_lock',)),
        (package_outbox.PackageOutbox, ('_lock',)),
        (package_logistics.PackageCancellationOutbox, ('_lock',)),
        (phs_label_workflow.PHSLabelExchangeJournal, ('_lock',)),
    ):
        original = owner.__init__
        def create(instance, *args, _original=original, _attributes=attributes, **kwargs):
            _original(instance, *args, **kwargs)
            for attribute in _attributes:
                setattr(instance, attribute, guard.wrap_lock(getattr(instance, attribute),
                    label=type(instance).__name__ + '.' + attribute))
        monkeypatch.setattr(owner, '__init__', create)
    if 'packaging' in request.fixturenames:
        app, _dialogs = request.getfixturevalue('packaging')
        for attribute, value in vars(app.data_manager).items():
            if 'lock' in attribute and hasattr(value, 'acquire') and hasattr(value, 'release'):
                setattr(app.data_manager, attribute, guard.wrap_lock(value, label='scan checkpoint ' + attribute))
    yield guard
    guard.assert_clean()


@pytest.fixture(params=["create", "cancel"])
def late_result_adapter_factory(tmp_path, monkeypatch, request):
    from tests.test_package_late_results import PackageLateAdapter
    # The shared fake clock only satisfies claim expiry/backoff; remote guards stay active.
    original = __import__("package_logistics")._utc_before
    monkeypatch.setattr(__import__("package_logistics"), "_utc_before", original)
    return lambda transport: PackageLateAdapter(tmp_path, request.param, transport, monkeypatch)
