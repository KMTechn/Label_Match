from types import SimpleNamespace
from unittest.mock import Mock
import errno
import io

import pytest

import Label_Match as app_module


@pytest.fixture(autouse=True)
def fresh_startup_process_budget(monkeypatch):
    import logistics_runtime_profile as profiles
    monkeypatch.setattr(profiles, '_startup_read_retry', profiles.StartupReadRetry(), raising=False)


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
    # A scan-time reload is one read; startup's sleep budget ends at startup.
    app._load_items_data.assert_called_once_with(show_errors=False)
    assert app.items_data == ({} if shutdown else items)
    assert app.current_set_info is original_set
    assert app.current_set_info['raw'] == []
    if not shutdown:
        assert app._phs_label_guidance_notice is None
        assert app._items_reload_in_progress is False
        assert app._retry_locked_item_catalog() is False


def test_scan_reload_locked_reads_wait_for_the_next_scan_without_sleep(monkeypatch):
    app = app_module.Label_Match.__new__(app_module.Label_Match)
    app.items_data = {}
    app._items_read_locked = True
    original_set = {'raw': [], 'marker': 'preserve'}
    app.current_set_info = original_set
    app._render_operator_workbench = Mock()
    callbacks = []
    app.after = lambda delay, callback: callbacks.append(callback)

    class Worker:
        def __init__(self, *, target, **kwargs):
            self.target = target

        def start(self):
            self.target()

    reader = Mock(side_effect=[locked(32), locked(32),
                              io.StringIO('Item Code,Item Name,Spec\n123,Part,S1\n')])
    sleeps = Mock()
    monkeypatch.setattr(app_module.threading, 'Thread', Worker)
    monkeypatch.setattr(app_module, 'open', reader, raising=False)
    monkeypatch.setattr(app_module.time, 'sleep', sleeps)
    monkeypatch.setattr(app_module, 'get_verified_catalog_snapshot', lambda path: None)
    monkeypatch.setattr(app_module, 'requires_verified_catalog_snapshot', lambda path: False)
    for attempt in range(1, 4):
        assert app._retry_locked_item_catalog() is True
        callbacks.pop(0)()
        assert reader.call_count == attempt
        sleeps.assert_not_called()
        assert app.current_set_info is original_set
        assert original_set['raw'] == []
        if attempt < 3:
            assert app.items_data == {}
            assert app._items_read_locked is True
            assert app._phs_label_guidance_notice is not None
    assert app.items_data['123']['Item Name'] == 'Part'
    assert app._retry_locked_item_catalog() is False


@pytest.mark.parametrize('preconstructor', [False, True])
@pytest.mark.parametrize('release', [True, False])
def test_constructor_template_read_shares_the_profile_startup_budget(monkeypatch, tmp_path, release, preconstructor):
    import json
    from pathlib import Path
    import logistics_runtime_profile as profiles

    class StartupReachedDataManager(Exception):
        pass

    profile_path = tmp_path / 'profile.json'
    profile_path.write_text('{}', encoding='utf-8')
    template = tmp_path / 'template.json'
    template.write_text('{"ui_settings": {"base_font_size": 15}}', encoding='utf-8')
    settings_path = tmp_path / 'config' / 'settings.json'
    original_bytes = Path.read_bytes
    original_open = open
    profile_reads = Mock(side_effect=[locked(32), locked(32), b'{}', b'{}'])
    template_reads = Mock(side_effect=([locked(32), None] if release else locked(32)))
    sleeps = Mock()
    writes = []

    def read_bytes(path):
        return profile_reads() if path == profile_path else original_bytes(path)

    def read_file(path, *args, **kwargs):
        if Path(path) == template:
            template_reads()
        return original_open(path, *args, **kwargs)

    def client(*, probe_required, read_retry):
        assert probe_required is False
        assert profiles._read_profile(profile_path, read_retry=read_retry) == {}
        return None

    def publish(path, settings):
        writes.append(settings)
        Path(path).write_text(json.dumps(settings), encoding='utf-8')

    def data_manager(*args, **kwargs):
        assert kwargs['read_retry'].remaining == 0
        raise StartupReachedDataManager

    monkeypatch.setattr(Path, 'read_bytes', read_bytes)
    monkeypatch.setattr(app_module, 'open', read_file, raising=False)
    monkeypatch.setattr(app_module.time, 'sleep', sleeps)
    monkeypatch.setattr(app_module, 'package_client_from_env', client)
    monkeypatch.setattr(app_module.tk.Tk, '__init__', lambda self: setattr(self, 'tk', Mock()))
    monkeypatch.setattr(app_module.Label_Match, 'title', lambda self: '')
    monkeypatch.setitem(app_module.LABEL_MATCH_SCREEN_HOST, 'active', False)
    monkeypatch.setattr(app_module, '_label_match_capture_startup_request', lambda: None)
    monkeypatch.setattr(app_module, '_label_match_prepare_capture_startup', lambda *args: None)
    monkeypatch.setattr(app_module, '_label_match_startup_trace', lambda *args, **kwargs: None)
    monkeypatch.setattr(app_module, 'logistics_runtime_required', lambda: False)
    monkeypatch.setattr(app_module, '_default_label_match_settings_path', lambda: str(settings_path))
    monkeypatch.setattr(app_module, 'resource_path', lambda path: str(template))
    monkeypatch.setattr(app_module, 'prepare_user_directory', lambda path: Path(path).mkdir(parents=True, exist_ok=True))
    monkeypatch.setattr(app_module.label_worker_roster, '_atomic_write', publish)
    monkeypatch.setattr(app_module.Label_Match, '_resolve_configured_save_path', lambda self: str(tmp_path / 'data'))
    monkeypatch.setattr(app_module.Label_Match, '_update_save_directory', lambda self: setattr(self, 'save_directory', self.custom_save_path))
    monkeypatch.setattr(app_module, '_label_match_local_log_id', lambda: 'test-worker')
    monkeypatch.setattr(app_module, '_open_label_data_manager', data_manager)
    if preconstructor:
        assert profiles._read_profile(profile_path) == {}
    app = app_module.Label_Match.__new__(app_module.Label_Match)
    with pytest.raises(StartupReachedDataManager if release else OSError):
        app_module.Label_Match.__init__(app)
    assert sleeps.call_args_list == [((1,),)] * 3
    assert profile_reads.call_count == (4 if preconstructor else 3)
    assert template_reads.call_count == 2
    assert len(writes) == int(release)
    assert settings_path.exists() is release
    assert template.read_text(encoding='utf-8') == '{"ui_settings": {"base_font_size": 15}}'


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
