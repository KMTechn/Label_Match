"""Scanner-only packaging requirements; intentionally RED on a20cd01.

설계 초안 값 — 바뀔 수 있음: KMCMD:EXCHANGE / KMCMD:UNDO;
known snapshot members restrict samples, with replacement goods excepted.
Every product here also meets the old final-label length/date restrictions:
these tests do not decide whether those restrictions survive for old labels.
"""

import copy
import csv
import json
from datetime import datetime
from pathlib import Path
from types import MethodType, SimpleNamespace

import pytest

import Label_Match as label_module
import label_completion
from tests.test_label_match_core import _FakeEntry
from tests.test_legacy_label_transition import MASTER, PHS2_LABEL, _close, _events, _packaging_app


EXCHANGE = "KMCMD:EXCHANGE"


def products(item=MASTER, *, prefix="SAMPLE"):
    day = datetime.now().strftime("%Y%m%d")
    return [f"{item}-{prefix}-{i:04d}<GS>6D{day}" for i in range(1, 5)]


def fake_dialogs(monkeypatch):
    calls = []
    for name in ("showinfo", "showwarning", "showerror", "askquestion", "askokcancel",
                 "askyesno", "askyesnocancel", "askretrycancel"):
        def record(*args, _name=name, **kwargs):
            calls.append((_name, args))
            return False
        monkeypatch.setattr(label_module.messagebox, name, record)
    return calls


def scan(app, value):
    app.entry = _FakeEntry(value)
    return app._handle_scan_enter()


@pytest.fixture
def packaging(tmp_path, monkeypatch):
    app, _syncs = _packaging_app(
        label_module, tmp_path, monkeypatch, registered=True, transition=True,
    )
    app.operator_workbench_ready = True
    # Restore production error admission and current-state persistence. The
    # imported storage fixture otherwise replaces both with recording stubs.
    app._present_inline_workflow_error = MethodType(
        label_module.Label_Match._present_inline_workflow_error, app,
    )
    app._save_current_set_state = MethodType(
        label_module.Label_Match._save_current_set_state, app,
    )
    app._render_operator_workbench = lambda: SimpleNamespace(scan_input_enabled=True)
    app.durable_commits = []
    commit = label_completion._commit_finalized_set_durable

    def observe_commit(owner, **kwargs):
        result = commit(owner, **kwargs)
        app.durable_commits.append(kwargs)
        return result

    monkeypatch.setattr(label_completion, "_commit_finalized_set_durable", observe_commit)
    calls = fake_dialogs(monkeypatch)
    try:
        yield app, calls
    finally:
        _close(app)


def completions(app):
    assert app.data_manager.flush(timeout=5), "test event writer did not flush"
    return _events(app.save_directory, app.Events.TRAY_COMPLETE)


def test_old_label_and_four_products_complete_on_fifth_scan_without_confirmation(packaging):
    app, dialogs = packaging
    values = [MASTER, *products()]
    for value in values[:4]:
        scan(app, value)
    assert completions(app) == [], "four scans must not complete the package"
    scan(app, values[4])
    rows = completions(app)
    assert len(rows) == 1, "fifth scan must durably complete exactly one package"
    assert rows[0][1]["final_result"] == app.Results.PASS
    assert rows[0][1]["scan_count"] == 5
    assert rows[0][1]["scanned_product_barcodes"] == values
    assert app.durable_commits[-1]["result"] == app.Results.PASS
    assert app.current_set_info["raw"] == []
    assert dialogs == [], "the five-scan flow must not request confirmation"
    scan(app, MASTER)
    assert app.current_set_info["raw"] == [MASTER], "next package must start immediately"


@pytest.mark.parametrize("position", [2, 3, 4, 5])
@pytest.mark.parametrize("misplaced", ["AAA2270730101", PHS2_LABEL], ids=["old-label", "compact-phs2"])
def test_label_in_product_slot_is_ignored_and_next_product_continues(packaging, position, misplaced):
    app, dialogs = packaging
    values = products()
    for value in [MASTER, *values[:position - 2]]:
        scan(app, value)
    before = copy.deepcopy(app.current_set_info)
    scan(app, misplaced)
    assert app.current_set_info["raw"] == before["raw"], "misplaced label must not occupy a slot"
    assert app.current_set_info["id"] == before["id"], "misplaced label must not reset the package"
    assert app.current_set_info["parsed"] == before["parsed"]
    scan(app, values[position - 2])
    if position < 5:
        assert app.current_set_info["raw"] == before["raw"] + [values[position - 2]], (
            "the next product must be accepted; it must not acknowledge an error and reset the set"
        )
    else:
        rows = completions(app)
        assert len(rows) == 1 and rows[0][1]["final_result"] == app.Results.PASS, (
            "the next product in slot five must complete the retained package"
        )
    assert "fail" in app.sounds, "ignored label must give an error sound"
    assert dialogs == [], "misplaced label must not open a Windows message box"


def test_known_membership_rejects_a_nonmember_without_losing_the_sample_slot(packaging):
    app, dialogs = packaging
    values = products()
    scan(app, MASTER)
    # Production snapshot shape, populated by a previous central lookup.
    app.current_set_info["package_source_snapshot"] = {
        "work_group_source": {
            "member_ids": [f"UNIT-{i}" for i in range(4)],
            "members": [
                {"unit_id": f"UNIT-{i}", "normalized_barcode": value}
                for i, value in enumerate(values)
            ],
        },
    }
    before = copy.deepcopy(app.current_set_info)
    scan(app, products(prefix="NONMEMBER")[0])
    assert app.current_set_info["raw"] == before["raw"], (
        "same-item product outside the known snapshot must be ignored"
    )
    assert "fail" in app.sounds
    scan(app, values[0])
    assert app.current_set_info["raw"] == before["raw"] + [values[0]]
    assert dialogs == []


def test_without_membership_snapshot_same_item_samples_are_accepted(packaging):
    app, dialogs = packaging
    scan(app, MASTER)
    scan(app, products(prefix="UNRESOLVED")[0])
    assert len(app.current_set_info["raw"]) == 2
    assert dialogs == []


def test_exchange_command_replaces_one_sample_and_the_package_continues(packaging):
    app, dialogs = packaging
    values = products()
    replacement = products(prefix="REPLACEMENT")[0]
    for value in [MASTER, values[0], EXCHANGE, values[0], replacement, *values[1:]]:
        scan(app, value)
    rows = completions(app)
    assert len(rows) == 1 and rows[0][1]["final_result"] == app.Results.PASS, (
        "exchange command, target and replacement must continue the existing package"
    )
    assert rows[0][1]["scanned_product_barcodes"] == [MASTER, replacement, *values[1:]]
    assert rows[0][1]["scan_count"] == 5, "exchange control scans do not add sample slots"
    assert dialogs == []


def test_exchange_pair_is_recorded_locally_before_the_set_continues(packaging):
    app, dialogs = packaging
    target = products()[0]
    replacement = products(prefix="REPLACEMENT")[0]
    for value in [MASTER, target, EXCHANGE, target, replacement]:
        scan(app, value)
    assert app.data_manager.flush(timeout=5)
    recorded = []
    for path in Path(app.save_directory).rglob("*.csv"):
        with path.open(encoding="utf-8-sig", newline="") as stream:
            for row in csv.DictReader(stream):
                if row.get("details"):
                    recorded.append(json.loads(row["details"]))
    # Do not prescribe a new event name or schema: a durable event must link
    # the pair, rather than merely recording two unrelated SCAN_ATTEMPT rows.
    assert any(target in json.dumps(row) and replacement in json.dumps(row) for row in recorded), (
        "a local exchange record must link the defective product and replacement"
    )
    assert dialogs == []


@pytest.mark.parametrize("warning", ["unconfirmed-catalog", "verified-cache", "profile-binding"])
def test_startup_warning_does_not_open_a_windows_message_box(tmp_path, monkeypatch, warning):
    dialogs = fake_dialogs(monkeypatch)
    monkeypatch.setenv(label_module.LABEL_MATCH_DIRECT_SYNC_ROOT_ENV, str(tmp_path / "sync"))
    if warning == "unconfirmed-catalog":
        error = label_module.ItemCatalogSyncError("offline", cause_code="REQUEST_FAILED_NO_CACHE")
        label_module._start_without_confirmed_item_catalog(error)
    elif warning == "verified-cache":
        label_module._show_item_catalog_cache_warning({"cache_last_modified_utc": "2026-09-30T00:00:00Z"})
    else:
        label_module._show_logistics_profile_warning({
            "profile_selection_warning": label_module.PROFILE_WARNING_CURRENT_USER_MISMATCH,
        })
    assert dialogs == [], "startup warning must be shown inside the workbench"
