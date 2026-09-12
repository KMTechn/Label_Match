"""Independent boundary checks against actual completion/commit consumers."""
import csv
from datetime import date, timedelta
import io
import json
import os
from types import SimpleNamespace

import pytest

from tests.test_completion_csv_index_review import csv_file, manager, truth, PREFIX, obsolete_index
from tests.test_label_match_core import load_label_match_module


def commit(module, data, identity):
    app = object.__new__(module.Label_Match)
    app.data_manager = data
    markers = []
    app.package_outbox = SimpleNamespace(mark_local_completion_committed=lambda *a, **k: markers.append(a))
    app._queue_authoritative_package = lambda **kwargs: {'idempotency_key': 'same-key', 'membership_mode': 'INHERIT_ALL'}
    app._commit_finalized_set_durable(
        details={'set_id': identity}, item_code='ITEM', is_manual_complete=False,
        result=app.Results.PASS, central_inherit_all=True, set_id_for_log=identity,
    )
    assert markers == [('same-key',)]


@pytest.mark.parametrize('age', [0, 3, 4])
@pytest.mark.parametrize('state', ['deleted', 'corrupt', 'stale'])
def test_commit_keeps_one_row_at_date_boundary_with_untrusted_index(tmp_path, age, state):
    module = load_label_match_module()
    target_date = (date.today() - timedelta(days=age)).strftime('%Y%m%d')
    path = csv_file(tmp_path, ['real-set']).rename(tmp_path / f'{PREFIX}{target_date}.csv')
    data = module.DataManager(str(tmp_path), 'AUDIT', 'WORKER', 'PC1')
    data._get_log_filepath_for_item = lambda item: str(path)
    try:
        assert module._label_match_local_completion_event_exists(data, 'missing') is False
        index_path = obsolete_index(tmp_path)
        if state == 'deleted':
            index_path.unlink()
        elif state == 'corrupt':
            index_path.write_bytes(b'broken cache')
        else:
            with path.open('a', encoding='utf-8-sig', newline='') as handle:
                csv.writer(handle).writerow(['2026-09-13', 'AUDIT', 'SCAN_ATTEMPT', '{}'])
                handle.flush()
                os.fsync(handle.fileno())
        commit(module, data, 'real-set')
        assert truth(path) == ['real-set']
        assert not data._writer_errors
    finally:
        data.close(5)


def test_rotation_completed_before_lookup_keeps_one_completion(tmp_path):
    module = load_label_match_module()
    path = csv_file(tmp_path, ['real-set'])
    data = module.DataManager(str(tmp_path), 'AUDIT', 'WORKER', 'PC1')
    try:
        assert module._label_match_local_completion_event_exists(data, 'missing') is False
        rotated = path.rename(tmp_path / f'{PREFIX}20000101_rotated.csv')
        data._get_log_filepath_for_item = lambda item: str(rotated)
        commit(module, data, 'real-set')
        assert truth(rotated) == ['real-set']
    finally:
        data.close(5)


@pytest.mark.parametrize('actual_commit', [False, True])
def test_rotation_after_enumeration_rescans_current_files(tmp_path, monkeypatch, actual_commit):
    module = load_label_match_module()
    path = csv_file(tmp_path, ['real-set'])
    data = module.DataManager(str(tmp_path), 'AUDIT', 'WORKER', 'PC1')
    rotated = tmp_path / f'{PREFIX}20000101_rotated.csv'
    data._get_log_filepath_for_item = lambda item: str(rotated)
    try:
        assert module._label_match_local_completion_event_exists(data, 'missing') is False
        original_listdir = module.os.listdir
        observed = []

        def listdir_with_rotation(directory):
            result = original_listdir(directory)
            if str(directory) == str(tmp_path) and not observed:
                observed.append('rotated-after-enumeration')
                path.rename(rotated)
            return result

        monkeypatch.setattr(module.os, 'listdir', listdir_with_rotation)
        assert truth(path) == ['real-set']
        if actual_commit:
            commit(module, data, 'real-set')
            assert observed == ['rotated-after-enumeration']
            assert not data._writer_errors
            assert truth(rotated).count('real-set') == 1
        else:
            found = module._label_match_local_completion_event_exists(data, 'real-set')
            assert observed == ['rotated-after-enumeration']
            assert truth(rotated) == ['real-set']
            assert found is True
    finally:
        data.close(5)


@pytest.mark.parametrize('ending', ['no-newline', 'partial-then-finished'])
def test_completed_tail_is_detected_after_partial_or_newline_free_write(tmp_path, ending):
    module = load_label_match_module()
    path = csv_file(tmp_path, ['old-set']).rename(tmp_path / f'{PREFIX}20000101.csv')
    data = module.DataManager(str(tmp_path), 'AUDIT', 'WORKER', 'PC1')
    data._get_log_filepath_for_item = lambda item: str(path)
    try:
        assert module._label_match_local_completion_event_exists(data, 'real-set') is False
        out = io.StringIO(newline='')
        csv.writer(out).writerow(['2026-09-13', 'AUDIT', 'TRAY_COMPLETE', json.dumps({'set_id': 'real-set'})])
        record = out.getvalue().rstrip('\r\n').encode('utf-8')
        if ending == 'partial-then-finished':
            with path.open('ab') as handle:
                handle.write(record[:-6])
            assert module._label_match_local_completion_event_exists(data, 'old-set') is True
            assert module._label_match_local_completion_event_exists(data, 'real-set') is False
            record = record[-6:]
        with path.open('ab') as handle:
            handle.write(record)
            handle.flush()
            os.fsync(handle.fileno())
        commit(module, data, 'real-set')
        assert truth(path) == ['old-set', 'real-set']
    finally:
        data.close(5)


def test_newer_index_timestamp_does_not_override_changed_archive(tmp_path):
    module = load_label_match_module()
    path = csv_file(tmp_path, ['old-set']).rename(tmp_path / f'{PREFIX}20000101.csv')
    data = manager(tmp_path)
    assert module._label_match_local_completion_event_exists(data, 'new-set') is False
    index_path = obsolete_index(tmp_path)
    stat = index_path.stat()
    os.utime(index_path, ns=(stat.st_atime_ns, stat.st_mtime_ns + 86_400_000_000_000))
    before = path.stat()
    path.write_bytes(path.read_bytes().replace(b'old-set', b'new-set'))
    os.utime(path, ns=(before.st_atime_ns, before.st_mtime_ns))
    assert index_path.stat().st_mtime_ns > path.stat().st_mtime_ns
    assert module._label_match_local_completion_event_exists(data, 'new-set') is True


def test_repeated_rotation_stops_commit_without_duplicate_or_marker(tmp_path, monkeypatch):
    module = load_label_match_module()
    path = csv_file(tmp_path, ['real-set'])
    data = module.DataManager(str(tmp_path), 'AUDIT', 'WORKER', 'PC1')
    data._get_log_filepath_for_item = lambda item: str(path)
    original_listdir = module.os.listdir
    current = [path]
    rotations = []

    def moving_files(directory):
        result = original_listdir(directory)
        if str(directory) == str(tmp_path):
            rotated = tmp_path / f'{PREFIX}rotation-{len(rotations)}.csv'
            current[0].rename(rotated)
            current[0] = rotated
            rotations.append(rotated)
        return result

    monkeypatch.setattr(module.os, 'listdir', moving_files)
    app = object.__new__(module.Label_Match)
    app.data_manager = data
    markers = []
    app.package_outbox = SimpleNamespace(mark_local_completion_committed=lambda *a, **k: markers.append(a))
    app._queue_authoritative_package = lambda **kwargs: {'idempotency_key': 'same-key', 'membership_mode': 'INHERIT_ALL'}
    try:
        with pytest.raises(module.PackageLogisticsError, match='kept moving'):
            app._commit_finalized_set_durable(
                details={'set_id': 'real-set'}, item_code='ITEM', is_manual_complete=False,
                result=app.Results.PASS, central_inherit_all=True, set_id_for_log='real-set',
            )
        assert len(rotations) == 2
        assert markers == []
        assert truth(current[0]) == ['real-set']
        assert not path.exists()
        assert not data._writer_errors
    finally:
        data.close(5)
