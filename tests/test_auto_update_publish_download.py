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


# Append to the phase-one test file only after commit 1 passes its focused gate.
import hashlib
import os
import shutil

from tests.test_writer_session_fence import _payload


NAMES = ("manifest.json", "manifest.sig", "packet.zip")


def inputs(root, packet_bytes=1024):
    attempt, stage = root / "attempt", root / "stage"
    attempt.mkdir()
    stage.mkdir()
    for name in NAMES:
        (stage / name).write_bytes(b"previous " + name.encode())
    (stage / "prepared.json").write_bytes(b"previous prepared marker")
    (attempt / "manifest.json").write_bytes(b'{"verified":true}')
    (attempt / "manifest.sig").write_bytes(b"verified signature")
    block = bytes(range(256)) * 1024
    with (attempt / "packet.zip").open("wb") as stream:
        remaining = packet_bytes
        while remaining:
            chunk = block[:min(remaining, len(block))]
            stream.write(chunk)
            remaining -= len(chunk)
    return attempt, stage


def fixed_bytes(stage):
    return {name: (stage / name).read_bytes() for name in (*NAMES, "prepared.json")}


def digest(path):
    with path.open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def publish(attempt, stage):
    updates.publish_download(stage, updates.stage_prepared_copies(attempt, stage))


def measured_publish(attempt, stage):
    with observed_admissions() as held, scanning_process(stage.parent / "scanner") as scan:
        started = time.perf_counter()
        publish(attempt, stage)
        elapsed_ms = (time.perf_counter() - started) * 1000
    return dict(publish_ms=elapsed_ms, admission_max_ms=max(row["held_ms"] for row in held),
                admission_sum_ms=sum(row["held_ms"] for row in held), admission_count=len(held),
                admissions=held, **scan)


def test_large_copy_fsync_is_outside_admission_and_allows_scans(tmp_path, record_property):
    attempt, stage = inputs(tmp_path, 64 * 1024 * 1024)
    fsync = updates.os.fsync
    depths = []

    def slow_fsync(descriptor):
        depths.append(getattr(fence._WRITER_LOCAL, "depth", 0))
        if os.fstat(descriptor).st_size >= 64 * 1024 * 1024:
            time.sleep(8)
        return fsync(descriptor)

    with pytest.MonkeyPatch.context() as patch:
        patch.setattr(updates.os, "fsync", slow_fsync)
        metrics = measured_publish(attempt, stage)
    record_property("admission_metrics", json.dumps(metrics))
    assert depths == [0, 0, 0]
    assert metrics["admission_count"] == 2
    assert metrics["admission_max_ms"] <= 200, metrics
    assert metrics["scan_rejected"] == 0, metrics
    assert metrics["scan_attempts"] >= 2
    for name in NAMES:
        assert digest(stage / name) == digest(attempt / name)
    assert (stage / "prepared.json").read_bytes() == b"previous prepared marker"


def test_download_creation_and_chunk_fsync_are_outside_admission(tmp_path, record_property):
    packet = tmp_path / "packet.zip"
    fsync = updates.os.fsync
    depths = []

    def slow_fsync(descriptor):
        depths.append(getattr(fence._WRITER_LOCAL, "depth", 0))
        if os.fstat(descriptor).st_size:
            time.sleep(8)
        return fsync(descriptor)

    with pytest.MonkeyPatch.context() as patch:
        patch.setattr(updates.os, "fsync", slow_fsync)
        with observed_admissions() as held, scanning_process(tmp_path / "scanner") as scan:
            updates.write_download_bytes(packet, b"")
            updates.write_download_chunk(packet, b"x" * (256 * 1024))
    metrics = dict(admission_max_ms=max(row["held_ms"] for row in held), **scan)
    record_property("download_metrics", json.dumps(metrics))
    assert depths == [0, 0]
    assert len(held) == 2
    assert metrics["admission_max_ms"] <= 200, metrics
    assert scan["scan_rejected"] == 0, metrics
    assert scan["scan_attempts"] >= 2
    assert packet.read_bytes() == b"x" * (256 * 1024)


@pytest.mark.parametrize("failure", ["disk", "write-denied"])
def test_late_copy_failure_preserves_fixed_names_and_original_error(tmp_path, failure):
    attempt, stage = inputs(tmp_path)
    before = fixed_bytes(stage)
    copy = shutil.copyfileobj
    original = OSError("synthetic disk error") if failure == "disk" else PermissionError("synthetic write denied")

    def interrupted(incoming, outgoing, *args, **kwargs):
        if Path(outgoing.name).name.startswith("packet.zip."):
            raise original
        return copy(incoming, outgoing, *args, **kwargs)

    with pytest.MonkeyPatch.context() as patch:
        patch.setattr(shutil, "copyfileobj", interrupted)
        with pytest.raises(type(original)) as caught:
            publish(attempt, stage)
    assert caught.value is original
    assert fixed_bytes(stage) == before


def test_fence_during_copy_denies_publish_without_changing_fixed_names(tmp_path):
    attempt, stage = inputs(tmp_path)
    before = fixed_bytes(stage)
    copy = shutil.copyfileobj
    active = fence.canonical_control_root() / fence.ACTIVE_FILENAME
    assert not active.exists()

    def fenced(incoming, outgoing, *args, **kwargs):
        result = copy(incoming, outgoing, *args, **kwargs)
        if Path(outgoing.name).name.startswith("packet.zip."):
            assert getattr(fence._WRITER_LOCAL, "depth", 0) == 0
            with fence.writer_admission("gui_tray_state_save"):
                active.parent.mkdir(parents=True, exist_ok=True)
                active.write_text(json.dumps(_payload(delegated_sources=[])), encoding="utf-8")
        return result

    try:
        with pytest.MonkeyPatch.context() as patch:
            patch.setattr(shutil, "copyfileobj", fenced)
            with pytest.raises(fence.WriterFencedError):
                publish(attempt, stage)
        assert fixed_bytes(stage) == before
        # A probe does not cancel its body: the private copy finishes, then
        # the next guarded operation rejects the active fence.
        packet_temps = list(stage.glob("packet.zip.*.tmp"))
        assert len(packet_temps) == 1
        assert digest(packet_temps[0]) == digest(attempt / "packet.zip")
    finally:
        active.unlink(missing_ok=True)


@pytest.mark.parametrize("damage", ["hardlink", "oversized"])
def test_invalid_late_source_preserves_all_fixed_files(tmp_path, damage):
    attempt, stage = inputs(tmp_path)
    before = fixed_bytes(stage)
    if damage == "hardlink":
        os.link(attempt / "packet.zip", tmp_path / "alias")
        reason = "hardlinks"
    else:
        with (attempt / "packet.zip").open("r+b") as stream:
            stream.truncate(512 * 1024 * 1024 + 1)
        reason = "oversized"
    with pytest.raises(ValueError, match=reason):
        publish(attempt, stage)
    assert fixed_bytes(stage) == before


def test_all_temporary_files_finish_before_first_replace(tmp_path):
    attempt, stage = inputs(tmp_path)
    replace = updates.os.replace
    replacements = []

    def observed(source, target):
        if not replacements:
            for name in NAMES:
                candidates = list(stage.glob(name + ".*.tmp"))
                assert len(candidates) == 1
                assert digest(candidates[0]) == digest(attempt / name)
        assert getattr(fence._WRITER_LOCAL, "depth", 0) > 0
        replacements.append(Path(target).name)
        return replace(source, target)

    with pytest.MonkeyPatch.context() as patch:
        patch.setattr(updates.os, "replace", observed)
        publish(attempt, stage)
    assert replacements == list(NAMES)
    assert not list(stage.glob("*.tmp"))
    assert (stage / "prepared.json").read_bytes() == b"previous prepared marker"


def test_system_copy_keeps_spanning_admission_and_owner_argument(tmp_path):
    source, target = tmp_path / "source", tmp_path / "target"
    source.write_bytes(b"system input")
    unredirected = updates.unredirected_source
    observed = []

    @contextmanager
    def owner_checked(path, **kwargs):
        observed.append((getattr(fence._WRITER_LOCAL, "depth", 0), kwargs))
        with unredirected(path) as incoming:
            yield incoming

    with pytest.MonkeyPatch.context() as patch:
        patch.setattr(updates, "unredirected_source", owner_checked)
        with observed_admissions() as held:
            updates.copy_prepared_file(source, target, expected_owner_sid="fixture-owner")
    assert len(held) == 1
    assert observed == [(1, {"expected_owner_sid": "fixture-owner"})]
    assert target.read_bytes() == b"system input"


def test_only_exact_approved_probe_pairs_are_allowed():
    assert isinstance(fence._PROBE_ONLY_WRITER_SINKS, frozenset)
    assert fence._PROBE_ONLY_WRITER_SINKS == frozenset({
        ("relay_child_launch", "user_relay.run_session_direct_sync_once"),
        ("user_relay_process_start", "user_relay.start_user_relay_process"),
        ("current_user_onboarding", "current_user_onboarding.onboard_current_user"),
        ("auto_update_write_download_bytes", "auto_update.write_download_bytes"),
        ("auto_update_write_download_chunk", "auto_update.write_download_chunk"),
        ("auto_update_stage_prepared_copies", "auto_update.stage_prepared_copies"),
        ("auto_update_run_download_attempt", "auto_update._run_download_attempt"),
    })
    assert fence._STAGING_PROBE_ONLY_WRITER_SINKS == frozenset({
        ("auto_update_write_download_bytes", "auto_update.write_download_bytes"),
        ("auto_update_write_download_chunk", "auto_update.write_download_chunk"),
        ("auto_update_stage_prepared_copies", "auto_update.stage_prepared_copies"),
        ("auto_update_run_download_attempt", "auto_update._run_download_attempt"),
    })

    def impostor():
        pytest.fail("unapproved probe body ran")

    with pytest.raises(fence.WriterFencedError) as caught:
        fence.writer_sink("auto_update_write_download_chunk", probe_only=True)(impostor)
    assert caught.value.code == "WRITER_PROBE_ONLY_SINK_NOT_ALLOWED"
    assert str(caught.value) == "probe-only admission is not approved for this writer sink"


@pytest.mark.parametrize("failure", [None, "fence", "download-fence", "disk", "rst", "stop", "sha", "archive", "transition"])
def test_feed_publish_and_marker_follow_complete_copies(tmp_path, record_property, failure):
    import io
    import zipfile
    import Label_Match
    import requests

    attempt, stage = inputs(tmp_path)
    marker = {"source_commit": "1" * 40, "packet_sha256": "0" * 64}
    (stage / "prepared.json").write_text(json.dumps(marker))
    before = fixed_bytes(stage)
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w") as archive:
        archive.writestr("fixture.txt", b"verified packet bytes")
    packet = buffer.getvalue()
    packet_hash = hashlib.sha256(packet).hexdigest()
    manifest = json.dumps({"source_commit": "2" * 40, "artifact": {"size_bytes": len(packet)}}).encode()
    candidate = {"url": "https://example.test/packet", "sha256": packet_hash,
                 "archive": {"layout": "canonical_portable"}}
    control = tmp_path / "control"
    control.mkdir()
    native_control = updates.control_root
    read_json = updates.read_json
    original_copy = shutil.copyfileobj
    publish_download = updates.publish_download
    write_json = updates.write_json_durable
    calls, depths = [], []
    active = fence.canonical_control_root() / fence.ACTIVE_FILENAME
    assert not active.exists()
    original_error = OSError("synthetic copy disk error")
    network_error = ConnectionResetError("synthetic connection reset")
    stop_error = KeyboardInterrupt("synthetic download stop")

    class Response:
        status_code = 200

        def __init__(self, url):
            self.url = url

        def __enter__(self):
            return self

        def __exit__(self, *args):
            pass

        def close(self):
            pass

        def iter_content(self, size):
            depths.append(getattr(fence._WRITER_LOCAL, "depth", 0))
            if self.url.endswith("packet"):
                yield packet[:len(packet) // 2]
                if failure == "rst":
                    raise network_error
                if failure == "stop":
                    raise stop_error
                if failure == "download-fence":
                    with fence.writer_admission("gui_tray_state_save"):
                        active.parent.mkdir(parents=True, exist_ok=True)
                        active.write_text(json.dumps(_payload(delegated_sources=[])), encoding="utf-8")
                yield packet[len(packet) // 2:]
            else:
                yield b"fixture signature" if self.url.endswith(".sig") else manifest

    class Session:
        cookies = SimpleNamespace(clear=lambda: None)

        def __enter__(self):
            return self

        def __exit__(self, *args):
            pass

        def get(self, url, **kwargs):
            depths.append(getattr(fence._WRITER_LOCAL, "depth", 0))
            return Response(url)

    def read(path):
        if path.name == "active.json":
            return {"sha": "1" * 40}
        if path.name == "update-manifest-key-config.json":
            return {"manifest_public_key": None}
        return read_json(path)

    def copied(incoming, outgoing, *args, **kwargs):
        assert getattr(fence._WRITER_LOCAL, "depth", 0) == 0
        if Path(outgoing.name).name.startswith("packet.zip.") and failure == "disk":
            raise original_error
        result = original_copy(incoming, outgoing, *args, **kwargs)
        calls.append("copy")
        if Path(outgoing.name).name.startswith("packet.zip.") and failure == "fence":
            with fence.writer_admission("gui_tray_state_save"):
                active.parent.mkdir(parents=True, exist_ok=True)
                active.write_text(json.dumps(_payload(delegated_sources=[])), encoding="utf-8")
        return result

    def published(*args):
        assert calls == ["copy", "copy", "copy"]
        result = publish_download(*args)
        calls.append("publish")
        return result

    def marked(path, payload):
        assert calls == ["copy", "copy", "copy", "publish"]
        with observed_admissions() as marker_holds:
            result = write_json(path, payload)
        record_property("prepared_json_metrics", json.dumps(marker_holds))
        calls.append("prepared")
        return result

    validation_error = ValueError("synthetic invalid authenticated packet")

    def validate_archive(*args, **kwargs):
        if failure == "archive":
            raise validation_error

    def authorize(*args, **kwargs):
        if failure == "transition":
            raise validation_error

    try:
        with pytest.MonkeyPatch.context() as patch:
            patch.setattr(updates, "control_root", lambda root: control if root == tmp_path else native_control(root))
            patch.setattr(updates, "load_policy", lambda _: dict(feed_url="https://example.test/feed", profile_auth=False,
                              channel="stable", staging_root=str(stage)))
            patch.setattr(updates, "read_json", read)
            patch.setattr(updates, "fixed_helper", lambda *args, **kwargs: None)
            patch.setattr(updates, "version_bundle", lambda *args: tmp_path)
            patch.setattr(updates, "version_manifest", lambda _: {"version": "v2.0.96"})
            patch.setattr(updates, "authorize_feed_transition", authorize)
            patch.setattr(updates, "packet_version_identity", lambda _: {})
            patch.setattr(Label_Match, "_verify_update_manifest_signature", lambda *args: None)
            patch.setattr(Label_Match, "_update_candidate_from_manifest", lambda *args, **kwargs: candidate)
            patch.setattr(Label_Match, "_validate_update_archive_contract", validate_archive)
            patch.setattr(requests, "Session", Session)
            patch.setattr(shutil, "copyfileobj", copied)
            patch.setattr(updates, "publish_download", published)
            patch.setattr(updates, "write_json_durable", marked)
            if failure == "sha":
                candidate["sha256"] = "0" * 64
            if failure is None:
                result = updates.prepare_feed_packet(tmp_path)
                assert result == {"status": "PREPARED", "source_commit": "2" * 40}
            else:
                expected = {"fence": fence.WriterFencedError, "download-fence": fence.WriterFencedError, "disk": OSError,
                            "rst": ConnectionResetError, "stop": KeyboardInterrupt,
                            "sha": ValueError, "archive": ValueError, "transition": ValueError}[failure]
                for _ in range(3 if failure == "sha" else 1):
                    with pytest.raises(expected) as caught:
                        updates.prepare_feed_packet(tmp_path)
                    assert not list(stage.glob("download-*"))
                    assert not list(stage.glob("*.tmp"))
                    assert fixed_bytes(stage) == before
                if failure == "disk":
                    assert caught.value is original_error
                elif failure == "rst":
                    assert caught.value is network_error
                elif failure == "stop":
                    assert caught.value is stop_error
                elif failure in {"archive", "transition"}:
                    assert caught.value is validation_error
        assert depths and all(depth == 0 for depth in depths)
        if failure is None:
            assert calls == ["copy", "copy", "copy", "publish", "prepared"]
            assert (stage / "manifest.json").read_bytes() == manifest
            assert (stage / "manifest.sig").read_bytes() == b"fixture signature"
            assert digest(stage / "packet.zip") == packet_hash
            assert json.loads((stage / "prepared.json").read_text())["packet_sha256"] == packet_hash
        else:
            assert fixed_bytes(stage) == before
            assert "publish" not in calls and "prepared" not in calls
        assert not list(stage.glob("download-*"))
        assert not list(stage.glob("*.tmp"))
    finally:
        active.unlink(missing_ok=True)


@pytest.mark.parametrize("operation", ["create", "chunk", "copy", "attempt"])
def test_staging_probes_reject_inherited_admission_before_mutation(tmp_path, operation):
    attempt, stage = inputs(tmp_path)
    before = fixed_bytes(stage)
    path = tmp_path / "new-download"
    callbacks = {
        "create": lambda: updates.write_download_bytes(path, b"new"),
        "chunk": lambda: updates.write_download_chunk(path, b"new"),
        "copy": lambda: updates.stage_prepared_copies(attempt, stage),
        "attempt": lambda: updates._run_download_attempt(stage, lambda *args: pytest.fail("attempt body ran")),
    }
    with fence.writer_admission("gui_tray_state_save"):
        with pytest.raises(fence.WriterFencedError) as caught:
            callbacks[operation]()
    assert caught.value.code == "STAGING_WRITER_ADMISSION_HELD"
    assert fixed_bytes(stage) == before
    assert not path.exists()
    assert not list(stage.glob("*.tmp"))
    assert not list(stage.glob("download-*"))


def test_download_attempt_collision_preserves_unowned_folder(tmp_path, monkeypatch):
    attempt, stage = inputs(tmp_path)
    collision = stage / ("download-" + "a" * 32)
    collision.mkdir()
    (collision / "packet.zip").write_bytes(b"unowned packet")
    monkeypatch.setattr(updates.uuid, "uuid4", lambda: SimpleNamespace(hex="a" * 32))
    with pytest.raises(FileExistsError):
        updates._run_download_attempt(stage, lambda *args: pytest.fail("collision body ran"))
    assert (collision / "packet.zip").read_bytes() == b"unowned packet"


def test_copy_collision_preserves_unowned_temporary(tmp_path, monkeypatch):
    attempt, stage = inputs(tmp_path)
    collision = stage / ("packet.zip." + "b" * 32 + ".tmp")
    collision.write_bytes(b"unowned temporary")
    monkeypatch.setattr(updates.uuid, "uuid4", lambda: SimpleNamespace(hex="b" * 32))
    with pytest.raises(FileExistsError):
        updates.stage_prepared_copies(attempt, stage)
    assert collision.read_bytes() == b"unowned temporary"
    assert list(stage.glob("*.tmp")) == [collision]


def test_failed_copy_preserves_unstarted_unowned_temporary(tmp_path, monkeypatch):
    attempt, stage = inputs(tmp_path)
    before = fixed_bytes(stage)
    collision = stage / ("packet.zip." + "d" * 32 + ".tmp")
    monkeypatch.setattr(updates.uuid, "uuid4", lambda: SimpleNamespace(hex="d" * 32))
    failure = OSError("copy interrupted")

    def interrupted(source, target, **kwargs):
        target.write_bytes(b"owned partial copy")
        collision.write_bytes(b"unowned future temporary")
        raise failure

    monkeypatch.setattr(updates, "_copy_prepared_file", interrupted)
    with pytest.raises(OSError) as caught:
        updates.stage_prepared_copies(attempt, stage)
    assert caught.value is failure
    assert collision.read_bytes() == b"unowned future temporary"
    assert list(stage.glob("*.tmp")) == [collision]
    assert fixed_bytes(stage) == before


def test_cleanup_refuses_outside_or_non_uuid_paths(tmp_path):
    attempt, stage = inputs(tmp_path)
    outside = tmp_path / ("packet.zip." + "c" * 32 + ".tmp")
    outside.write_bytes(b"outside")
    with pytest.raises(ValueError, match="differs"):
        updates._discard_prepared_copies(stage, {"packet.zip": outside})
    with pytest.raises(ValueError, match="differs"):
        updates._discard_prepared_copies(stage, {"packet.zip": stage / "packet.zip"})
    assert outside.read_bytes() == b"outside"
    assert (stage / "packet.zip").read_bytes() == b"previous packet.zip"


def test_cleanup_rejects_reparse_attempt_and_preserves_target(tmp_path):
    attempt, stage = inputs(tmp_path)
    target = tmp_path / "preserved-target"
    target.mkdir()
    (target / "packet.zip").write_bytes(b"preserved outside attempt")
    before = fixed_bytes(stage)
    junction = None

    def redirected(owned_attempt, temps):
        nonlocal junction
        owned_attempt.rmdir()
        junction = owned_attempt
        quote = lambda path: "'" + str(path).replace("'", "''") + "'"
        command = "New-Item -ItemType Junction -Path " + quote(junction) + " -Target " + quote(target) + " | Out-Null"
        completed = subprocess.run(["powershell.exe", "-NoLogo", "-NoProfile", "-NonInteractive", "-Command", command],
                                   capture_output=True, creationflags=subprocess.CREATE_NO_WINDOW)
        assert completed.returncode == 0
        return {"status": "fixture"}

    try:
        with pytest.raises(fence.WriterFenceError, match="reparse") as caught:
            updates._run_download_attempt(stage, redirected)
        assert caught.value.code == "FENCE_REPARSE_REJECTED"
        assert (target / "packet.zip").read_bytes() == b"preserved outside attempt"
        assert fixed_bytes(stage) == before
    finally:
        if junction is not None and junction.exists():
            junction.rmdir()  # Remove the literal junction itself, without recursion.


def test_system_defers_prepared_copy_while_relay_is_live(tmp_path, monkeypatch):
    attempt, stage = inputs(tmp_path)
    control = tmp_path / "control"
    control.mkdir()
    before = fixed_bytes(stage)
    policy = dict(staging_root=str(stage), operator_sid="S-1-5-21-1000-1001-1002-1003",
                  operator_local_app_data=str(tmp_path))
    monkeypatch.setenv("LOCALAPPDATA", str(tmp_path))
    # Inject only policy/observations; every production write still uses its sink.
    original_control = updates.control_root
    monkeypatch.setattr(updates, "control_root", lambda app: None if app == tmp_path else original_control(app))
    monkeypatch.setattr(updates, "fixed_helper", lambda *args, **kwargs: None)
    monkeypatch.setattr(updates, "load_policy", lambda _: policy)
    monkeypatch.setattr(updates, "optional_record", lambda _: None)
    monkeypatch.setattr(updates, "prepared_pending", lambda *args: True)
    monkeypatch.setattr(updates, "process_snapshot", lambda _: [{"pid": 123, "role": "relay"}])
    monkeypatch.setattr(updates, "writer_processes", lambda rows, **kwargs: rows)
    monkeypatch.setattr(updates, "copy_prepared_file", lambda *args, **kwargs: pytest.fail("SYSTEM consumed staging while relay was live"))
    with fence.writer_admission("gui_tray_state_save"):
        assert updates._apply_prepared(control) == 0
    stored = json.loads((control / "transition.json").read_text())
    assert stored["phase"] == "DEFERRED"
    assert fixed_bytes(stage) == before


def test_fence_actors_keep_feed_staging_separate():
    root = Path(updates.__file__).parent
    for relative in ("INSTALL_CANONICAL_PORTABLE.ps1", "tools/bootstrap_integrity.ps1",
                     "current_user_onboarding.py", "tools/label_writer_fence.ps1"):
        source = (root / relative).read_text(encoding="utf-8-sig")
        assert "staging_root" not in source
        assert "download-" not in source
        assert "stage_prepared_copies" not in source
    installer = (root / "INSTALL_THIS_PC.ps1").read_text(encoding="utf-8-sig")
    assert installer.count("staging_root=") == 1  # Policy declaration only.
    assert "$stagingRoot = Join-Path $applicationParent ('.current.bootstrap.'" in installer
    assert "download-" not in installer
