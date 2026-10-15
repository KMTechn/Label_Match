import os
from pathlib import Path
from types import SimpleNamespace

import pytest

import label_atomic_replace as publication
import user_relay


@pytest.mark.parametrize("write_through", [False, True])
@pytest.mark.parametrize("exhaustion", ["expired", "path", "fsync"])
def test_first_native_call_cannot_publish_after_the_callers_deadline(
        tmp_path, monkeypatch, write_through, exhaustion):
    source, destination = tmp_path / "temporary", tmp_path / "live"
    source.write_bytes(b"complete-new")
    destination.write_bytes(b"old")
    clock, calls = [0.0], []
    monkeypatch.setattr(publication.time, "monotonic", lambda: clock[0])
    def native(*args, **kwargs):
        calls.append(True)
        pytest.fail("An expired deadline reached native publication")
    monkeypatch.setattr(publication, "_prepare_write_through", lambda: native)
    monkeypatch.setattr(publication, "_replace_once", native)
    monkeypatch.setattr(publication, "os", SimpleNamespace(name="nt", fspath=os.fspath))
    class SlowPath:
        def __fspath__(self):
            clock[0] = 2.0
            return str(source)
    deadline = -1.0 if exhaustion == "expired" else 1.0
    with pytest.raises(TimeoutError, match="^Atomic publication deadline exceeded$"):
        if exhaustion == "fsync":
            def fsync(handle):
                clock[0] = 2.0
            monkeypatch.setattr(user_relay.os, "fsync", fsync)
            replace = publication.atomic_replace
            monkeypatch.setattr(user_relay, "atomic_replace", lambda a, b, **kw:
                replace(a, b, write_through=write_through, **kw))
            user_relay._write_json_bytes_atomic(destination, "complete-new", deadline=deadline)
        else:
            publication.atomic_replace(SlowPath() if exhaustion == "path" else source,
                destination, write_through=write_through, deadline=deadline)
    assert calls == []
    assert source.read_bytes() == b"complete-new" and destination.read_bytes() == b"old"
    assert sorted(path.name for path in tmp_path.iterdir()) == ["live", "temporary"]
