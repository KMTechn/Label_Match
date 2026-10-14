import os
from types import SimpleNamespace

import pytest

import label_atomic_replace as publication


def test_write_through_prepares_native_once_and_rechecks_after_retry_path_conversion(monkeypatch):
    clock = [0.0]
    calls = []
    prepared = []
    first = OSError('Injected sharing refusal')
    first.winerror = 32
    def native(source, destination, flags):
        calls.append((source, destination, flags))
        raise first
    def prepare():
        prepared.append(True)
        return native
    class DelayedPath:
        conversions = 0
        def __fspath__(self):
            self.conversions += 1
            if self.conversions == 2:
                clock[0] = 1.1
            return 'temporary-source'
    monkeypatch.setattr(publication, 'os', SimpleNamespace(name='nt', fspath=os.fspath))
    monkeypatch.setattr(publication, '_prepare_write_through', prepare)
    monkeypatch.setattr(publication.time, 'monotonic', lambda: clock[0])
    monkeypatch.setattr(publication.time, 'sleep', lambda seconds: clock.__setitem__(0, clock[0] + seconds))
    with pytest.raises(OSError) as caught:
        publication.atomic_replace(DelayedPath(), 'destination', write_through=True)
    assert caught.value is first
    assert len(prepared) == 1
    assert len(calls) == 1


def test_slow_initial_native_preparation_does_not_start_publication(monkeypatch):
    clock=[0.0]
    calls=[]
    def prepare():
        clock[0]=2.0
        return lambda *args: calls.append(args) or True
    monkeypatch.setattr(publication,'os',SimpleNamespace(name='nt',fspath=os.fspath))
    monkeypatch.setattr(publication,'_prepare_write_through',prepare)
    monkeypatch.setattr(publication.time,'monotonic',lambda:clock[0])
    with pytest.raises(TimeoutError):
        publication.atomic_replace('source','destination',write_through=True)
    assert calls==[]
