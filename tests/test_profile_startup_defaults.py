"""Profile locks at the real pre-constructor startup boundary; no native UI."""
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

import logistics_runtime_profile as profiles
import item_catalog_sync as sync
from tests.test_logistics_runtime_profile import _profile
from tests.test_item_catalog_sync import CATALOG, FakeResponse
from tests.test_startup_read_budget import locked


@pytest.fixture(autouse=True)
def fresh_process_budget(monkeypatch):
    monkeypatch.setattr(profiles, '_startup_read_retry', profiles.StartupReadRetry(), raising=False)


@pytest.fixture
def profile_catalog(monkeypatch, tmp_path):
    path = _profile(tmp_path)
    original = Path.read_bytes
    raw = original(path)
    reads, sleeps, decrypt = Mock(), Mock(), Mock(return_value='sub734-fixture')

    def read(candidate):
        return reads() if candidate == path else original(candidate)

    monkeypatch.setattr(Path, 'read_bytes', read)
    monkeypatch.setattr(profiles.time, 'sleep', sleeps)
    monkeypatch.setattr(profiles, 'unprotect_machine_secret', decrypt)
    monkeypatch.setattr(profiles, 'unprotect_current_user_secret', decrypt)
    monkeypatch.setenv(getattr(profiles, 'APP_PROFILE_PATH_ENV', profiles.PROFILE_PATH_ENV), str(path))
    monkeypatch.setenv(profiles.REQUIRED_ENV, '1')
    for name in (sync.URL_ENV, sync.ACTIVE_PATH_ENV, *sync.BASE_URL_ENV_NAMES):
        monkeypatch.delenv(name, raising=False)
    bundled = tmp_path / 'bundle.csv'
    bundled.write_bytes(CATALOG)
    cache = tmp_path / 'cache' / 'Item.csv'
    transport = Mock(return_value=FakeResponse(CATALOG))
    refresh = sync.refresh_item_catalog

    def catalog(*args, **kwargs):
        try:
            return refresh(bundled, cache_path=cache, get=transport, **kwargs)
        except Exception as error:
            import json
            import traceback
            chain = []
            while error is not None:
                frames = traceback.extract_tb(error.__traceback__)
                chain.append({'type': type(error).__name__, 'frames': [
                    {'file': Path(frame.filename).name, 'line': frame.lineno, 'function': frame.name}
                    for frame in frames[-3:]]})
                error = error.__cause__ or error.__context__
            (tmp_path / 'error-chain.json').write_text(json.dumps(chain), encoding='utf-8')
            raise

    return SimpleNamespace(path=path, raw=raw, reads=reads, sleeps=sleeps,
                           decrypt=decrypt, catalog=catalog, transport=transport,
                           original=original)


def test_explicit_zero_does_not_spend_the_default_startup_budget(monkeypatch):
    sleeps = Mock()
    monkeypatch.setattr(profiles.time, 'sleep', sleeps)
    path = Mock(read_bytes=Mock(side_effect=PermissionError('locked')))
    with pytest.raises(profiles.LogisticsRuntimeConfigurationError):
        profiles._read_profile(path, read_retry=None)
    path.read_bytes.assert_called_once()
    sleeps.assert_not_called()
    assert profiles._startup_read_retry.remaining == 3


def test_default_readers_share_one_process_budget(monkeypatch):
    sleeps = Mock()
    monkeypatch.setattr(profiles.time, 'sleep', sleeps)
    first = Mock(read_bytes=Mock(side_effect=[locked(32), locked(32), b'{}']))
    second = Mock(read_bytes=Mock(side_effect=locked(33)))
    assert profiles._read_profile(first) == {}
    with pytest.raises(profiles.LogisticsRuntimeConfigurationError):
        profiles._read_profile(second)
    assert first.read_bytes.call_count == 3
    assert second.read_bytes.call_count == 2
    assert sleeps.call_args_list == [((1,),)] * 3

import Label_Match as app_module
import label_data_generation
import auto_update


@pytest.fixture
def startup(monkeypatch, tmp_path, profile_catalog):
    context = profile_catalog
    app = SimpleNamespace(title=lambda: '', state=lambda: '', mainloop=Mock(),
                          _finish_after_mainloop=Mock())
    created, retry_notice = Mock(return_value=app), Mock(return_value=False)
    created.FILES = app_module.Label_Match.FILES
    monkeypatch.setattr(label_data_generation, 'check_user_data_open', Mock())
    monkeypatch.setattr(app_module, '_startup_app_stop_requested', lambda: False)
    monkeypatch.setattr(app_module, 'legacy_label_transition_enabled', lambda: False)
    monkeypatch.setattr(app_module, 'refresh_item_catalog', context.catalog)
    monkeypatch.setattr(app_module, 'resource_path', lambda path: str(tmp_path / 'bundle.csv'))
    monkeypatch.setattr(app_module, 'Label_Match', created)
    monkeypatch.setattr(app_module, '_offer_item_catalog_startup_retry', retry_notice)
    monkeypatch.setattr(app_module, '_show_logistics_profile_warning', Mock())
    monkeypatch.setattr(app_module, '_show_item_catalog_cache_warning', Mock())
    monkeypatch.setattr(app_module, '_label_match_startup_trace', Mock())
    monkeypatch.setattr(app_module, 'write_item_catalog_startup_diagnostic', Mock())
    monkeypatch.setattr(auto_update, 'trial_context', lambda root: None)
    monkeypatch.setitem(app_module.LABEL_MATCH_SCREEN_HOST, 'requested', False)
    monkeypatch.setitem(app_module.LABEL_MATCH_SCREEN_HOST, 'active', False)
    return SimpleNamespace(context=context, app=app, created=created, retry_notice=retry_notice)


@pytest.mark.parametrize('kind', ['permission', 5, 32, 33])
@pytest.mark.parametrize('release', [True, False])
def test_real_startup_profile_locks_before_constructor(startup, kind, release):
    context = startup.context
    context.reads.side_effect = [locked(kind), locked(kind), context.raw] if release else locked(kind)
    if release:
        assert app_module._run_label_match_application() == 0
        startup.created.assert_called_once()
        startup.app.mainloop.assert_called_once()
        startup.app._finish_after_mainloop.assert_called_once()
        startup.retry_notice.assert_not_called()
    else:
        with pytest.raises(sync.ItemCatalogSyncError) as caught:
            app_module._run_label_match_application()
        assert caught.value.cause_code == sync.PROFILE_LOAD_FAILED
        startup.retry_notice.assert_called_once()
        assert startup.retry_notice.call_args.args[0] is caught.value
        startup.created.assert_not_called()
    assert context.reads.call_count == (3 if release else 4)
    assert context.sleeps.call_args_list == [((1,),)] * (2 if release else 3)
    assert context.original(context.path) == context.raw
    assert context.decrypt.call_count == int(release)
    assert context.transport.call_count == int(release)
    if release:
        context.reads.side_effect = PermissionError('business lock')
        context.reads.reset_mock()
        context.sleeps.reset_mock()
        with pytest.raises(profiles.LogisticsRuntimeConfigurationError):
            profiles._read_profile(context.path)
        context.reads.assert_called_once()
        context.sleeps.assert_not_called()
