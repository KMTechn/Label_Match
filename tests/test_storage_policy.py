from pathlib import Path

import pytest

from storage_policy import LOCAL_EVENTS_DIR_NAME, label_match_local_events_dir


def test_local_events_directory_is_a_sibling_of_the_relay_source(tmp_path):
    scan_source = tmp_path / "Label_Match" / "data"

    local_events = label_match_local_events_dir(scan_source)

    assert local_events == (
        tmp_path / "Label_Match" / LOCAL_EVENTS_DIR_NAME
    ).resolve()
    assert local_events.parent == scan_source.resolve().parent
    assert local_events != scan_source.resolve()
    assert scan_source.resolve() not in local_events.parents


def test_local_events_directory_requires_a_save_directory():
    with pytest.raises(ValueError, match="save directory is required"):
        label_match_local_events_dir("")


def test_relative_save_directory_is_resolved_before_sibling_selection(
    monkeypatch,
    tmp_path,
):
    monkeypatch.chdir(tmp_path)

    local_events = label_match_local_events_dir(Path("runtime") / "data")

    assert local_events == (tmp_path / "runtime" / "local_events").resolve()
