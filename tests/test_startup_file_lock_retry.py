from types import SimpleNamespace
from unittest.mock import Mock
import errno
import io

import pytest

import Label_Match as app_module


def locked(kind):
    if kind == 'permission':
        return PermissionError('locked')
    error = OSError('locked')
    error.winerror = kind
    return error


@pytest.mark.parametrize('kind', ['permission', 5, 32, 33])
@pytest.mark.parametrize('release', [True, False])
def test_items_retry_only_file_locks(monkeypatch, kind, release):
    reader = Mock(side_effect=[locked(kind), locked(kind)] + (
        [io.StringIO('Item Code,Item Name,Spec\n123,Part,S1\n')] if release
        else [locked(kind), locked(kind)]))
    sleeps = Mock()
    errors = Mock()
    app = app_module.Label_Match.__new__(app_module.Label_Match)
    app.run_tests = False
    monkeypatch.setattr(app_module, 'open', reader, raising=False)
    monkeypatch.setattr(app_module.time, 'sleep', sleeps)
    monkeypatch.setattr(app_module, 'get_verified_catalog_snapshot', lambda path: None)
    monkeypatch.setattr(app_module, 'requires_verified_catalog_snapshot', lambda path: False)
    monkeypatch.setattr(app_module.messagebox, 'showerror', errors)
    rows = app._load_items_data(read_retry=app_module.StartupReadRetry())
    assert reader.call_count == (3 if release else 4)
    assert sleeps.call_args_list == [((1,),)] * (2 if release else 3)
    if release:
        assert rows['123']['Item Name'] == 'Part'
        errors.assert_not_called()
        assert app.__dict__.get('_items_read_locked') is False
    else:
        assert rows == {}
        assert app.__dict__.get('_items_read_locked') is True
        assert errors.call_args.args[0] == '기준 정보 로드 오류'


@pytest.mark.parametrize('error,title', [
    (OSError(errno.EIO, 'disk error'), '기준 정보 로드 오류'),
    (FileNotFoundError('absent'), '기준 정보 파일 없음'),
])
def test_items_other_errors_keep_existing_failure(monkeypatch, error, title):
    reader = Mock(side_effect=error)
    sleeps = Mock()
    errors = Mock()
    app = app_module.Label_Match.__new__(app_module.Label_Match)
    app.run_tests = False
    monkeypatch.setattr(app_module, 'open', reader, raising=False)
    monkeypatch.setattr(app_module.time, 'sleep', sleeps)
    monkeypatch.setattr(app_module, 'get_verified_catalog_snapshot', lambda path: None)
    monkeypatch.setattr(app_module, 'requires_verified_catalog_snapshot', lambda path: False)
    monkeypatch.setattr(app_module.messagebox, 'showerror', errors)
    monkeypatch.setattr(app_module.messagebox, 'showwarning', errors)
    assert app._load_items_data() == {}
    reader.assert_called_once()
    sleeps.assert_not_called()
    assert errors.call_args.args[0] == title
    assert not app.__dict__.get('_items_read_locked', False)


def test_locked_empty_catalog_scan_is_not_reported_unregistered(monkeypatch):
    app = app_module.Label_Match.__new__(app_module.Label_Match)
    app.items_data = {}
    app._items_read_locked = True
    app._render_operator_workbench = Mock()
    app.after = Mock()
    worker = Mock()
    monkeypatch.setattr(app_module.threading, 'Thread', Mock(return_value=worker))
    assert app._retry_locked_item_catalog() is True
    assert '미등록' not in app._phs_label_guidance_notice.message
    assert app._phs_label_guidance_notice.message == '품목 정보를 불러오지 못했습니다\n현장 관리자에게 알리세요'
    worker.start.assert_called_once()
    assert app._retry_locked_item_catalog() is True
    worker.start.assert_called_once()


@pytest.mark.parametrize('kind', ['permission', 5, 32, 33])
@pytest.mark.parametrize('release', [True, False])
def test_startup_settings_lock_keeps_original_failure(monkeypatch, kind, release):
    reader = Mock(side_effect=[locked(kind), locked(kind)] + (
        [io.StringIO('{}')] if release else [locked(kind), locked(kind)]))
    sleeps = Mock()
    app = app_module.Label_Match.__new__(app_module.Label_Match)
    app.app_settings_path = 'unused.json'
    monkeypatch.setattr(app_module, 'open', reader, raising=False)
    monkeypatch.setattr(app_module.time, 'sleep', sleeps)
    if release:
        assert app._load_app_settings(read_retry=app_module.StartupReadRetry()) == {}
    else:
        with pytest.raises(OSError):
            app._load_app_settings(read_retry=app_module.StartupReadRetry())
    assert reader.call_count == (3 if release else 4)
    assert sleeps.call_args_list == [((1,),)] * (2 if release else 3)


@pytest.mark.parametrize('shutdown', [False, True])
def test_scan_reload_publishes_on_owner_thread_without_replaying_or_resetting(monkeypatch, shutdown):
    app = app_module.Label_Match.__new__(app_module.Label_Match)
    app.items_data = {}
    app._items_read_locked = True
    original_set = {'raw': [], 'marker': 'preserve'}
    app.current_set_info = original_set
    app._render_operator_workbench = Mock()
    callbacks = []
    app.after = lambda delay, callback: callbacks.append(callback)
    jobs = []
    class Worker:
        def __init__(self, *, target, **kwargs):
            self.target = target
        def start(self):
            jobs.append(self.target)
    monkeypatch.setattr(app_module.threading, 'Thread', Worker)
    items = {'123': {'Item Name': 'Part'}}
    app._load_items_data = Mock(return_value=items)
    assert app._retry_locked_item_catalog() is True
    notice = app._phs_label_guidance_notice
    callbacks.pop(0)()  # Still waiting, no synchronous sleep or duplicate reader.
    assert app.items_data == {}
    assert len(jobs) == 1
    jobs[0]()
    app._items_read_locked = False  # Reader finished; publication still awaits Tk.
    assert app._retry_locked_item_catalog() is True
    assert app._phs_label_guidance_notice is notice
    app._tk_shutdown_requested = shutdown
    callbacks.pop(0)()
    app._load_items_data.assert_called_once()
    assert app._load_items_data.call_args.kwargs['show_errors'] is False
    assert isinstance(app._load_items_data.call_args.kwargs['read_retry'], app_module.StartupReadRetry)
    assert app.items_data == ({} if shutdown else items)
    assert app.current_set_info is original_set
    assert app.current_set_info['raw'] == []
    if not shutdown:
        assert app._phs_label_guidance_notice is None
        assert app._items_reload_in_progress is False
        assert app._retry_locked_item_catalog() is False


@pytest.mark.parametrize('failure,result', [
    (OSError(errno.EIO, 'disk error'), None),
    (FileNotFoundError('absent'), {}),
    (ValueError('invalid'), None),
])
def test_settings_other_errors_are_not_retried(monkeypatch, failure, result):
    app = app_module.Label_Match.__new__(app_module.Label_Match)
    app.app_settings_path = 'unused.json'
    reader = Mock(side_effect=failure)
    sleeps = Mock()
    monkeypatch.setattr(app_module, 'open', reader, raising=False)
    monkeypatch.setattr(app_module.time, 'sleep', sleeps)
    if result is None:
        with pytest.raises(type(failure)):
            app._load_app_settings()
    else:
        assert app._load_app_settings() == result
    reader.assert_called_once()
    sleeps.assert_not_called()


@pytest.mark.parametrize('release', [True, False])
def test_recovery_startup_read_keeps_existing_evidence_result(monkeypatch, tmp_path, release):
    import label_data_manager as data
    reads = Mock(side_effect=[locked(32), locked(32)] + (
        [b'{}'] if release else [locked(32), locked(32)]))
    monkeypatch.setattr(data, 'read_checked_bytes', reads)
    sleeps = Mock()
    monkeypatch.setattr(app_module.time, 'sleep', sleeps)
    result = data.read_recovery_file(
        tmp_path / 'missing.json', read_retry=app_module.StartupReadRetry())
    assert result['verified'] is release
    assert result['value'] == ({} if release else None)
    if not release:
        assert result['reason'] == 'OSError'
    assert reads.call_count == (3 if release else 4)
    assert sleeps.call_count == (2 if release else 3)


@pytest.mark.parametrize('kind', ['permission', 5, 32, 33])
@pytest.mark.parametrize('release', [True, False])
def test_credential_locator_retry_preserves_admission_and_error_kind(monkeypatch, tmp_path, kind, release):
    import direct_sync_runtime as runtime
    import label_data_generation as generation
    admission = Mock()
    monkeypatch.setattr(generation, 'check_user_data_open', admission)
    reads = Mock(side_effect=[locked(kind), locked(kind)] + (
        [{}] if release else [locked(kind), locked(kind)]))
    sleeps = Mock()
    monkeypatch.setattr(app_module.time, 'sleep', sleeps)
    kwargs = dict(app_root=tmp_path, reader=reads, read_retry=app_module.StartupReadRetry())
    if not release and kind == 'permission':
        with pytest.raises(generation.DataGenerationError) as caught:
            runtime.prepare_credentials_from_json(tmp_path / 'locator.json', **kwargs)
        assert isinstance(caught.value.__cause__, PermissionError)
    else:
        result = runtime.prepare_credentials_from_json(tmp_path / 'locator.json', **kwargs)
        assert (result.error is None) is release
        if not release:
            assert result.error.winerror == kind
    admission.assert_called_once()
    assert reads.call_count == (3 if release else 4)
    assert sleeps.call_count == (2 if release else 3)


def test_runtime_recovery_and_credential_reader_do_not_sleep(monkeypatch, tmp_path):
    import direct_sync_runtime as runtime
    import label_data_manager as data
    import label_data_generation as generation
    sleeps = Mock()
    monkeypatch.setattr(app_module.time, 'sleep', sleeps)
    reads = Mock(side_effect=PermissionError('locked'))
    monkeypatch.setattr(data, 'read_checked_bytes', reads)
    result = data.read_recovery_file(tmp_path / 'missing.json')
    assert result['verified'] is False
    assert result['reason'] == 'PermissionError'
    reads.assert_called_once()
    reads.reset_mock()
    monkeypatch.setattr(generation, 'check_user_data_open', Mock())
    with pytest.raises(generation.DataGenerationError):
        runtime.prepare_credentials_from_json(tmp_path / 'locator.json', app_root=tmp_path, reader=reads)
    reads.assert_called_once()
    sleeps.assert_not_called()


def test_profile_factory_passes_startup_budget_only_when_given(monkeypatch):
    import package_logistics
    profile_loader = Mock(return_value=None)
    monkeypatch.setattr(package_logistics, 'load_logistics_runtime_profile', profile_loader)
    budget = app_module.StartupReadRetry()
    assert package_logistics.package_client_from_env(
        environ={}, probe_required=False, read_retry=budget) is None
    assert profile_loader.call_args.kwargs['read_retry'] is budget
    profile_loader.reset_mock()
    package_logistics.package_client_from_env(environ={}, probe_required=False)
    assert 'read_retry' not in profile_loader.call_args.kwargs
