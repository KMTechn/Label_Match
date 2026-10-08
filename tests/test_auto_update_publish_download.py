"""Native writer admission and a competing process for feed preparation."""
from contextlib import contextmanager
import json
from pathlib import Path
import subprocess
import sys
import threading
import time
from types import SimpleNamespace

import pytest

import auto_update as updates
import user_relay
import writer_session_fence as fence


SCAN_CHILD = r'''
import json, sys, time
from pathlib import Path
import writer_session_fence as fence
root = Path(sys.argv[1])
waits, rejected = [], 0
(root / "ready").write_text("ready")
while not (root / "stop").exists():
    started = time.perf_counter()
    try:
        with fence.writer_admission("gui_tray_state_save"):
            pass
    except fence.WriterFenceError:
        rejected += 1
    waits.append((time.perf_counter() - started) * 1000)
    time.sleep(0.1)
(root / "scan.json").write_text(json.dumps(dict(scan_wait_max_ms=max(waits, default=0), scan_rejected=rejected, scan_attempts=len(waits))))
'''


@contextmanager
def scanning_process(root):
    root.mkdir()
    child = subprocess.Popen([sys.executable, "-B", "-c", SCAN_CHILD, str(root)],
                             stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                             creationflags=subprocess.CREATE_NO_WINDOW | subprocess.CREATE_NEW_PROCESS_GROUP)
    metrics = {}
    try:
        deadline = time.monotonic() + 5
        while not (root / "ready").exists() and child.poll() is None and time.monotonic() < deadline:
            time.sleep(0.01)
        assert (root / "ready").exists(), "competing writer did not start"
        yield metrics
    finally:
        (root / "stop").write_text("stop")
        child.wait(timeout=10)
        child.communicate()
    assert child.returncode == 0
    metrics.update(json.loads((root / "scan.json").read_text()))


@contextmanager
def observed_admissions():
    """Observe acquisition/release without replacing the native lease or guard."""
    original_acquire = fence._acquire_named_mutex
    original_release = fence._NamedMutexLease.release
    starts, held = {}, []

    def acquire(*args, **kwargs):
        source = sys._getframe(1).f_locals.get("selected_source", "")
        lease = original_acquire(*args, **kwargs)
        if lease is not None:
            starts[id(lease)] = (threading.get_ident(), source, time.perf_counter())
        return lease

    def release(lease):
        start = starts.pop(id(lease), None)
        finished = time.perf_counter()
        original_release(lease)
        if start is not None:
            held.append(dict(thread=start[0], source=start[1], held_ms=(finished - start[2]) * 1000))

    with pytest.MonkeyPatch.context() as patch:
        patch.setattr(fence, "_acquire_named_mutex", acquire)
        patch.setattr(fence._NamedMutexLease, "release", release)
        yield held


def fake_feed(patch, root, mode, depths):
    import Label_Match
    import requests

    control = root / "control"
    control.mkdir()
    original_control_root = updates.control_root
    patch.setattr(updates, "control_root", lambda app: (None if mode == "off" else control)
                  if app == root else original_control_root(app))
    patch.setattr(updates, "load_policy", lambda _: dict(feed_url="https://example.test/feed", profile_auth=False, channel="stable"))
    patch.setattr(updates, "read_json", lambda path: {"sha": "1" * 40} if path.name == "active.json" else {"manifest_public_key": None})
    patch.setattr(updates, "fixed_helper", lambda *args, **kwargs: None)
    patch.setattr(updates, "version_bundle", lambda *args: root)
    patch.setattr(updates, "version_manifest", lambda _: {"version": "v2.0.96"})
    patch.setattr(Label_Match, "_verify_update_manifest_signature", lambda *args: None)
    patch.setattr(Label_Match, "_update_candidate_from_manifest", lambda *args, **kwargs: None)

    class Response:
        status_code = 200

        def __init__(self, url):
            self.url = url

        def __enter__(self):
            return self

        def __exit__(self, *args):
            self.close()

        def close(self):
            pass

        def iter_content(self, size):
            depths.append(getattr(fence._WRITER_LOCAL, "depth", 0))
            yield b"fixture signature" if self.url.endswith(".sig") else b"{}"

    class Session:
        cookies = SimpleNamespace(clear=lambda: None)

        def __enter__(self):
            return self

        def __exit__(self, *args):
            pass

        def get(self, url, **kwargs):
            depths.append(getattr(fence._WRITER_LOCAL, "depth", 0))
            if mode == "refused":
                raise ConnectionRefusedError("synthetic connection refused")
            if mode == "delayed" and not url.endswith(".sig"):
                time.sleep(8)
            return Response(url)

    patch.setattr(requests, "Session", Session)


@pytest.mark.parametrize("mode", ["delayed", "refused", "off"])
def test_relay_prepares_feed_outside_cycle_admission(tmp_path, record_property, mode):
    depths, failure_depths, order = [], [], []
    preserve = user_relay._preserve_failure_evidence

    def preserved(*args):
        failure_depths.append(getattr(fence._WRITER_LOCAL, "depth", 0))
        return preserve(*args)

    def direct_sync(**kwargs):
        order.append("direct-sync")
        assert getattr(fence._WRITER_LOCAL, "depth", 0) > 0
        return {"status": "PASS", "returncode": 0}

    def prepare():
        order.append("feed")
        return updates.prepare_feed_packet(tmp_path)

    with pytest.MonkeyPatch.context() as patch:
        fake_feed(patch, tmp_path, mode, depths)
        patch.setattr(user_relay, "_preserve_failure_evidence", preserved)
        patch.setattr(user_relay, "run_session_direct_sync_once", direct_sync)
        with observed_admissions() as held, scanning_process(tmp_path / "scanner") as scan:
            started = time.perf_counter()
            result = user_relay.run_persistent_relay_loop(
                lambda prepared: user_relay._runtime_cycle(app_root=tmp_path, direct_sync_root=tmp_path,
                    scan_source_dir=tmp_path, update_preparation=prepared),
                prepare_cycle=prepare, status_path=tmp_path / "status.json", interval_seconds=0, max_cycles=1)
            elapsed_ms = (time.perf_counter() - started) * 1000
    cycle_holds = [row["held_ms"] for row in held if row["source"] == "persistent_relay_cycle"]
    metrics = dict(mode=mode, elapsed_ms=elapsed_ms, cycle_hold_max_ms=max(cycle_holds), **scan)
    record_property("relay_metrics", json.dumps(metrics))
    assert all(depth == 0 for depth in depths)
    assert order == ["feed", "direct-sync"]
    assert result["cycle_count"] == 1
    # Coordinator decision (2026-10-08 23:4x): W2 covers HTTP waiting.
    # Refusal has no server wait; W3 keeps its existing durable evidence write
    # admitted. Measure that write, while asserting every HTTP call is outside.
    if mode != "refused":
        assert scan["scan_rejected"] == 0, metrics
    if mode == "delayed":
        assert elapsed_ms >= 8000
        assert max(cycle_holds) < elapsed_ms - 7000, metrics
        assert result["last_cycle"]["auto_update"]["status"] == "NO_UPDATE"
    elif mode == "refused":
        preparation = result["last_cycle"]["auto_update"]
        assert preparation["status"] == "DEFERRED"
        assert preparation["error_type"] == "ConnectionRefusedError"
        assert preparation["failure_evidence_status"] == "PROVEN"
        assert Path(preparation["failure_evidence_path"]).is_file()
        assert failure_depths and all(depth > 0 for depth in failure_depths)
    else:
        assert "auto_update" not in result["last_cycle"]
    stored = json.loads((tmp_path / "status.json").read_text())
    assert stored["last_cycle"] == result["last_cycle"]
    assert stored["persistent_retry"] is True


def test_feed_error_is_carried_to_the_admitted_cycle(tmp_path):
    original = ConnectionRefusedError("synthetic connection refused")

    def prepare():
        assert getattr(fence._WRITER_LOCAL, "depth", 0) == 0
        raise original

    def cycle(error):
        assert error is original
        assert getattr(fence._WRITER_LOCAL, "depth", 0) > 0
        return {"status": "same-shape"}

    result = user_relay.run_persistent_relay_loop(cycle, prepare_cycle=prepare,
        status_path=tmp_path / "status.json", interval_seconds=0, max_cycles=1)
    assert result["last_cycle"] == {"status": "same-shape"}


@pytest.mark.parametrize("error_type", [fence.WriterFenceError, user_relay.DataGenerationError])
def test_cycle_writer_error_is_still_rethrown(tmp_path, error_type):
    original = error_type("SYNTHETIC", "synthetic writer error")

    def cycle(prepared):
        raise original

    with pytest.raises(error_type) as caught:
        user_relay.run_persistent_relay_loop(cycle, prepare_cycle=lambda: {"status": "OFF"},
            status_path=tmp_path / "status.json", interval_seconds=0, max_cycles=1)
    assert caught.value is original
    assert not (tmp_path / "status.json").exists()
