"""Keep default writer admission isolated across pytest runs and their children."""

import pytest


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
