import errno
import inspect
from pathlib import Path
from unittest.mock import Mock

import pytest

import logistics_runtime_profile as profiles


def locked(kind):
    if kind == 'permission':
        return PermissionError('locked')
    error = OSError('locked')
    error.winerror = kind
    return error


@pytest.mark.parametrize('kind', ['permission', 5, 32, 33])
def test_startup_waits_are_shared_between_readers(monkeypatch, kind):
    sleeps = Mock()
    monkeypatch.setattr(profiles.time, 'sleep', sleeps)
    budget = profiles.StartupReadRetry()
    first = Mock(side_effect=[locked(kind), locked(kind), 'ready'])
    second = Mock(side_effect=locked(kind))
    assert budget(first) == 'ready'
    with pytest.raises(OSError):
        budget(second)
    assert first.call_count == 3
    assert second.call_count == 2
    assert budget.remaining == 0
    assert sleeps.call_args_list == [((1,),)] * 3
    with pytest.raises(OSError):
        budget(second)
    assert second.call_count == 3
    assert sleeps.call_count == 3


@pytest.mark.parametrize('kind', ['permission', 5, 32, 33])
@pytest.mark.parametrize('release', [True, False])
def test_startup_profile_lock_preserves_configuration_error(monkeypatch, kind, release):
    reads = Mock(side_effect=[locked(kind), locked(kind)] + (
        [b'{}'] if release else [locked(kind), locked(kind)]))
    path = Mock(read_bytes=reads)
    sleeps = Mock()
    monkeypatch.setattr(profiles.time, 'sleep', sleeps)
    if release:
        assert profiles._read_profile(path, read_retry=profiles.StartupReadRetry()) == {}
    else:
        with pytest.raises(profiles.LogisticsRuntimeConfigurationError) as caught:
            profiles._read_profile(path, read_retry=profiles.StartupReadRetry())
        assert isinstance(caught.value.__cause__, OSError)
    assert reads.call_count == (3 if release else 4)
    assert sleeps.call_args_list == [((1,),)] * (2 if release else 3)


def test_runtime_profile_reader_has_no_implicit_wait(monkeypatch):
    sleeps = Mock()
    monkeypatch.setattr(profiles.time, 'sleep', sleeps)
    path = Mock(read_bytes=Mock(side_effect=PermissionError('locked')))
    with pytest.raises(profiles.LogisticsRuntimeConfigurationError):
        profiles._read_profile(path)
    path.read_bytes.assert_called_once()
    sleeps.assert_not_called()


@pytest.mark.parametrize('value', [OSError(errno.EIO, 'disk error'), b'{invalid'])
def test_profile_non_lock_or_invalid_data_is_not_retried(monkeypatch, value):
    reads = Mock(side_effect=value) if isinstance(value, OSError) else Mock(return_value=value)
    sleeps = Mock()
    monkeypatch.setattr(profiles.time, 'sleep', sleeps)
    with pytest.raises(profiles.LogisticsRuntimeConfigurationError):
        profiles._read_profile(Mock(read_bytes=reads), read_retry=profiles.StartupReadRetry())
    reads.assert_called_once()
    sleeps.assert_not_called()


@pytest.mark.parametrize('kind', ['permission', 5, 32, 33])
@pytest.mark.parametrize('release', [True, False])
def test_protected_file_startup_read_retries_before_decryption(monkeypatch, tmp_path, kind, release):
    class StopAfterRead(Exception):
        pass
    profile = tmp_path / 'profile.json'
    profile.write_bytes(b'{}')
    artifact = tmp_path / 'protected.bin'
    artifact.write_bytes(b'x')
    payload = {'contract_version': profiles.PROFILE_CONTRACT_VERSION, 'bearer_token_ref': 'artifact'}
    monkeypatch.setattr(profiles, '_read_profile', lambda *args, **kwargs: payload)
    monkeypatch.setattr(profiles, '_resolve_secret_path', lambda *args: artifact)
    monkeypatch.setattr(profiles, '_resolve_tls_ca_bundle_path', lambda *args: '')
    reads = Mock(side_effect=[locked(kind), locked(kind)] + (
        [b'x'] if release else [locked(kind), locked(kind)]))
    original_read = Path.read_bytes
    def read(path):
        return reads() if path == artifact else original_read(path)
    monkeypatch.setattr(Path, 'read_bytes', read)
    sleeps = Mock()
    monkeypatch.setattr(profiles.time, 'sleep', sleeps)
    admission = Mock()
    options = {}
    if 'data_admission' in inspect.signature(profiles.load_logistics_runtime_profile).parameters:
        options['data_admission'] = admission
    else:
        import data_generation_guard
        monkeypatch.setattr(data_generation_guard, 'probe_before_owner_paths', admission)
    decrypt = Mock(side_effect=StopAfterRead)
    budget = profiles.StartupReadRetry()
    expected = StopAfterRead if release else profiles.LogisticsRuntimeConfigurationError
    with pytest.raises(expected):
        profiles.load_logistics_runtime_profile(
            required=True, profile_path=profile, decryptor=decrypt,
            read_retry=budget, **options)
    assert reads.call_count == (3 if release else 4)
    assert sleeps.call_count == (2 if release else 3)
    assert admission.call_count == 2
    if release:
        decrypt.assert_called_once_with(b'x')
    else:
        decrypt.assert_not_called()
