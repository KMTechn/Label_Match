"""Persistent checkpoint failures keep one UNKNOWN set out of further work."""

import copy
from datetime import datetime, timedelta
import json
from pathlib import Path
from types import MethodType

import pytest

import Label_Match as lm
import label_data_manager as storage
import writer_session_fence as fence
from package_logistics import PackageOutbox
from terminal_operation_lease import OperationLeaseError
from tests.test_legacy_label_transition import MASTER, PHS2_LABEL
from tests.test_label_match_core import _FakeEntry
from tests.test_w9scanonly_packaging import EXCHANGE, packaging, products, scan, completions  # noqa: F401


@pytest.fixture
def station(packaging, monkeypatch):
    app, dialogs = packaging
    app._publish_durable_commit_block = MethodType(
        lm.Label_Match._publish_durable_commit_block, app,
    )
    app.entry = None
    monkeypatch.setattr(_FakeEntry, "focus_set", lambda _self: None, raising=False)
    return app, dialogs


def prepare_set(app, *, prefix="HELD-SAMPLE"):
    values = [MASTER, *products(prefix=prefix)]
    for value in values[:4]:
        scan(app, value)
    app.data_manager.flush(timeout=5)
    return values


def fail_checkpoint(app, monkeypatch, *, entire_root=False):
    target = Path(app.save_directory) / app.FILES.CURRENT_STATE
    failing_entry = target.stat().st_ino
    original = storage.write_checked_bytes

    def write(path, payload, **kwargs):
        if entire_root or (Path(path) == target and target.exists()
                           and target.stat().st_ino == failing_entry):
            raise PermissionError("checkpoint write denied")
        return original(path, payload, **kwargs)

    monkeypatch.setattr(storage, "write_checked_bytes", write)
    return target


def test_persistent_checkpoint_failure_holds_unknown_and_next_set_continues(station, monkeypatch):
    app, dialogs = station
    values = prepare_set(app)
    target = fail_checkpoint(app, monkeypatch)
    original = target.read_bytes()
    set_id = app.current_set_info["id"]
    scan(app, values[-1])
    assert app._workflow_blocking_notice is not None
    assert app.package_outbox.list_workbench_holds() == []
    app._retry_blocked_submission()

    held = app.package_outbox.get_workbench_hold(set_id)
    evidence = json.loads(held["snapshot_json"])
    assert evidence["storage_failure"]["outcome"] == "UNKNOWN"
    assert evidence["current_set_info"]["raw"] == values[:4]
    assert Path(evidence["archive_path"]).read_bytes() == original
    assert not target.exists()
    assert app.current_set_info["raw"] == []
    assert app._workflow_blocking_notice is None
    assert "pass" not in app.sounds
    assert completions(app) == []
    assert dialogs == []
    scan(app, MASTER)
    scan(app, values[1])
    assert app.current_set_info["raw"] == [MASTER]
    for value in products(prefix="NEXT-SAMPLE"):
        scan(app, value)
    assert len(completions(app)) == 1


@pytest.mark.parametrize("failure", ["root", "record", "probe-read", "probe-delete", "archive"])
def test_failed_probe_or_hold_record_keeps_station_stopped(station, monkeypatch, failure):
    app, _dialogs = station
    values = prepare_set(app)
    target = fail_checkpoint(app, monkeypatch, entire_root=failure == "root")
    original = target.read_bytes()

    def denied(*args, **kwargs):
        raise OSError("isolating storage failed")

    if failure == "record":
        monkeypatch.setattr(app.package_outbox, "hold_workbench_set", denied)
    elif failure == "probe-read":
        original_read = storage.read_checked_bytes

        def read(path, **kwargs):
            if Path(path).name.startswith("._storage_probe_"):
                return b"different readback"
            return original_read(path, **kwargs)

        monkeypatch.setattr(storage, "read_checked_bytes", read)
    elif failure == "probe-delete":
        monkeypatch.setattr(storage, "unlink_checked", denied)
    elif failure == "archive":
        monkeypatch.setattr(storage, "quarantine_entry", denied)
    scan(app, values[-1])
    app._retry_blocked_submission()
    assert app.current_set_info["raw"] == values[:4]
    assert target.read_bytes() == original
    assert "이 PC에 저장할 수 없습니다" in app._workflow_blocking_notice.message
    assert "다른 작업대로 옮겨 계속하세요" in app._workflow_blocking_notice.message
    assert "pass" not in app.sounds
    assert completions(app) == []


@pytest.mark.parametrize('window_expired', [False, True])
def test_fourth_distinct_set_stops_but_repeat_does_not_count_twice(station, monkeypatch, window_expired):
    app, _dialogs = station
    for index in range(4):
        with monkeypatch.context() as patch:
            if index == 3 and window_expired:
                times = [json.loads(row['snapshot_json'])['storage_failure']['recorded_at']
                         for row in app.package_outbox.list_workbench_holds()]
                later = max(datetime.fromisoformat(value) for value in times) + timedelta(minutes=10, microseconds=1)

                class Clock(datetime):
                    @classmethod
                    def now(cls, tz=None):
                        return cls.fromtimestamp(later.timestamp(), tz)

                patch.setattr(app.data_manager, '_clock', lambda: Clock)
            values = prepare_set(app, prefix=f"BOUNDARY-SAMPLE-{index}")
            fail_checkpoint(app, patch)
            scan(app, values[-1])
            before = copy.deepcopy(app.current_set_info)
            app._retry_blocked_submission()
            holds = app.package_outbox.list_workbench_holds()
            assert len(holds) == (index + 1 if window_expired else min(index + 1, 3))
            if index < 3 or window_expired:
                assert app.current_set_info["raw"] == []
                assert app._workflow_blocking_notice is None
                # The same durable evidence is reused without another HOLD row.
                snapshot = json.loads(holds[-1]["snapshot_json"])
                app.data_manager._last_state_write_failure = (
                    PermissionError("checkpoint write denied"), snapshot["current_set_info"], 2,
                )
                repeated = app.data_manager.hold_failed_checkpoint(
                    app.package_outbox, source_phs2=holds[-1]["source_phs2"],
                    source_input_tag_id=holds[-1]["source_input_tag_id"],
                )
                assert repeated["set_id"] == holds[-1]["set_id"]
                assert len(app.package_outbox.list_workbench_holds()) == index + 1
            else:
                assert app.current_set_info == before
                assert "이 PC에 저장할 수 없습니다" in app._workflow_blocking_notice.message
    assert completions(app) == []


def test_original_and_hold_history_are_proved_outside_writer_admission(station, monkeypatch):
    app, _dialogs = station
    values = prepare_set(app)
    target = fail_checkpoint(app, monkeypatch)
    read = storage.read_checked_bytes
    history = app.package_outbox.list_workbench_holds
    reads = []

    def checked_read(path, **kwargs):
        if Path(path) == target or Path(path).name.startswith(target.name + '.held-'):
            assert not getattr(fence._WRITER_LOCAL, 'depth', 0)
            reads.append(Path(path))
        return read(path, **kwargs)

    def checked_history():
        assert not getattr(fence._WRITER_LOCAL, 'depth', 0)
        return history()

    monkeypatch.setattr(storage, 'read_checked_bytes', checked_read)
    monkeypatch.setattr(app.package_outbox, 'list_workbench_holds', checked_history)
    scan(app, values[-1])
    app._retry_blocked_submission()
    assert app._workflow_blocking_notice is None
    assert target in reads and any(path != target for path in reads)


def test_retry_after_hold_record_does_not_complete_or_overwrite_the_original(station, monkeypatch):
    app, _dialogs = station
    values = prepare_set(app)
    target = fail_checkpoint(app, monkeypatch)
    original = target.read_bytes()
    with monkeypatch.context() as patch:
        def denied(*args, **kwargs):
            raise PermissionError('injected archive move failure')

        patch.setattr(storage, 'quarantine_entry', denied)
        scan(app, values[-1])
        app._retry_blocked_submission()
    assert app._workflow_blocking_notice is not None
    assert len(app.package_outbox.list_workbench_holds()) == 1
    assert target.read_bytes() == original
    app._retry_blocked_submission()
    assert app._workflow_blocking_notice is None
    assert app.current_set_info['raw'] == []
    assert len(app.package_outbox.list_workbench_holds()) == 1
    held = app.package_outbox.list_workbench_holds()[0]
    assert Path(json.loads(held['snapshot_json'])['archive_path']).read_bytes() == original
    assert completions(app) == []
    assert 'test_pass' not in app.sounds


def test_storage_held_physical_label_cannot_start_a_transition_local_set(station, monkeypatch):
    app, _dialogs = station
    app._transition_start_local(PHS2_LABEL, MASTER, lm.label_transition.PHS2_LOCAL)
    values = products()
    for value in values[:3]:
        scan(app, value)
    app.data_manager.flush(timeout=5)
    with monkeypatch.context() as patch:
        fail_checkpoint(app, patch)
        scan(app, values[-1])
        app._retry_blocked_submission()
    assert app._workflow_blocking_notice is None
    scan(app, PHS2_LABEL)
    assert app.current_set_info['raw'] == []
    assert completions(app) == []
    scan(app, MASTER)
    assert app.current_set_info['raw'] == [MASTER]


def test_hold_and_duplicate_evidence_survive_restart_across_midnight(station, monkeypatch):
    app, _dialogs = station
    values = prepare_set(app)
    with monkeypatch.context() as patch:
        fail_checkpoint(app, patch)
        scan(app, values[-1])
        app._retry_blocked_submission()
    held = app.package_outbox.list_workbench_holds()[0]
    assert app._workflow_blocking_notice is None
    snapshot = json.loads(held["snapshot_json"])
    now = datetime.fromisoformat(snapshot["storage_failure"]["recorded_at"])

    class Tomorrow(datetime):
        @classmethod
        def now(cls, tz=None):
            value = now + timedelta(days=1)
            return cls.fromtimestamp(value.timestamp(), tz)

    monkeypatch.setattr(lm, "datetime", Tomorrow)
    restarted = PackageOutbox(app.package_outbox.db_path)
    app.package_outbox = restarted
    assert restarted.get_workbench_hold(held["set_id"])["snapshot_json"] == held["snapshot_json"]
    assert Path(snapshot["archive_path"]).exists()
    scan(app, MASTER)
    scan(app, values[1])
    assert app.current_set_info["raw"] == [MASTER]
    assert completions(app) == []


def test_storage_held_product_cannot_be_used_as_exchange_replacement(station, monkeypatch):
    app, dialogs = station
    values = prepare_set(app)
    with monkeypatch.context() as patch:
        fail_checkpoint(app, patch)
        scan(app, values[-1])
        app._retry_blocked_submission()
    target, replacement = products(prefix='EXCHANGE_NEXT')[:2]
    for value in [MASTER, target, EXCHANGE, target]:
        scan(app, value)
    before = copy.deepcopy(app.current_set_info)
    scan(app, values[1])
    assert app.current_set_info == before
    scan(app, replacement)
    assert app.current_set_info['raw'] == [MASTER, replacement]
    assert completions(app) == []
    assert dialogs == []


def test_first_failure_after_storage_recovery_does_not_reuse_the_old_root_warning(station, monkeypatch):
    app, _dialogs = station
    values = prepare_set(app)
    app._storage_root_failure = True
    fail_checkpoint(app, monkeypatch)
    scan(app, values[-1])
    assert app._storage_root_failure is False
    assert app._workflow_blocking_notice is not None
    assert app.package_outbox.list_workbench_holds() == []
    assert '다른 작업대로' not in app._workflow_blocking_notice.message


def test_recompletion_identity_does_not_isolate_under_the_old_set_id(station, monkeypatch):
    app, _dialogs = station
    prepare_set(app)
    app.current_set_info['completion_identity'] = str(app.current_set_info['id']) + '-undo'
    assert app._save_current_set_state()
    target = fail_checkpoint(app, monkeypatch)
    original = target.read_bytes()
    for _ in range(2):
        assert app._save_current_set_state() is False
    app._publish_durable_commit_block(OSError('injected recompletion checkpoint failure'))
    assert app.package_outbox.list_workbench_holds() == []
    assert app._workflow_blocking_notice is not None
    assert target.read_bytes() == original
    assert completions(app) == []


def test_lease_refusal_and_existing_csv_completion_are_never_storage_holds(station, monkeypatch):
    app, _dialogs = station
    prepare_set(app)
    fail_checkpoint(app, monkeypatch)
    for _ in range(2):
        assert app._save_current_set_state() is False
    error = OperationLeaseError("OPERATION_LEASE_NOT_YET_VALID", "clock behind")
    app._publish_durable_commit_block(error)
    assert app.package_outbox.list_workbench_holds() == []
    app._csv_completed_set = {"current_snapshot": copy.deepcopy(app.current_set_info)}
    app._publish_durable_commit_block(OSError("save failed"))
    assert app.package_outbox.list_workbench_holds() == []
    assert app.current_set_info["raw"]
