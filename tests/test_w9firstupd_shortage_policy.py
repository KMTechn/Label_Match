"""Only complete small new-label groups; retain old-label shortage undo."""

import copy

import pytest

import Label_Match as label_module
from package_logistics import PackageLogisticsError
from tests import test_deferred_lease_clock_retry as lease_tests
from tests import test_package_logistics as source_tests
from tests.test_deferred_lease_clock_retry import clock_case  # noqa: F401
from tests.test_legacy_label_transition import MASTER
from tests.test_w9scanonly_central import central  # noqa: F401
from tests.test_w9scanonly_packaging import completions, packaging, products, scan  # noqa: F401
from tests.test_w9scanonly_short_tray import scanner_clock  # noqa: F401


NOTICE = "모자란 상자: 대체품으로 채워 정상 마감 — 못 채우면 마감하지 말고 따로 두고 관리자에게"


@pytest.fixture(autouse=True)
def source_group(request, monkeypatch):
    total, selected = getattr(request, "param", (4, 4))
    monkeypatch.setattr(source_tests, "UNITS", tuple(f"UNIT-POLICY-{i}" for i in range(total)))
    item = "ITEM000000001"
    day = label_module.datetime.now().strftime("%Y%m%d")
    monkeypatch.setattr(source_tests, "BARCODES", tuple(
        f"{item}-POLICY-{i:04d}<GS>6D{day}" for i in range(total)))
    original = source_tests._work_group_response
    monkeypatch.setattr(lease_tests, "_work_group_response", lambda **kwargs:
        original(split=selected < total, split_size=selected))
    return total, selected


def start_resolved(central):
    app, _dialogs, case, root, _releases = central
    master = case.group["scan_payload"]
    scan(app, master)
    root.run_until(lambda: not app.__dict__.get("_phs2_background_pending"), timeout=5)
    assert app.current_set_info["package_source_snapshot"] == case.snapshot
    values = [row["normalized_barcode"] for row in case.snapshot["work_group_source"]["members"]]
    return app, master, values


@pytest.mark.parametrize("source_group", [(2, 2), (3, 3)], indirect=True)
def test_all_members_of_a_small_group_can_finish(central, scanner_clock):
    app, master, values = start_resolved(central)
    for value in values:
        scan(app, value)
    scanner_clock[0] += 5
    scan(app, master)
    [(_row, details)] = completions(app)
    assert details["shortage_confirmed"] is True
    assert details["product_sample_barcodes"] == values
    assert app._scanner_shortage_undo
    assert not app.blocks and central[1] == []


@pytest.mark.parametrize("source_group", [(5, 5)], indirect=True)
def test_three_samples_cannot_finish_a_five_member_group(central, scanner_clock):
    app, master, values = start_resolved(central)
    for value in values[:3]:
        scan(app, value)
    before = copy.deepcopy(app.current_set_info)
    scanner_clock[0] += 5
    scan(app, master)
    assert completions(app) == []
    assert app.current_set_info == before
    assert app._phs_label_guidance_notice.message == NOTICE
    assert not app.__dict__.get("_scanner_shortage_undo")
    assert central[1] == []


def test_new_label_without_a_server_count_waits_for_connection(central, scanner_clock):
    app, master, values = start_resolved(central)
    app.current_set_info["package_source_snapshot"] = None
    app.package_logistics_client = None
    scan(app, values[0])
    before = copy.deepcopy(app.current_set_info)
    scanner_clock[0] += 5
    scan(app, master)
    assert completions(app) == []
    assert app.current_set_info == before
    assert "서버 연결 뒤 마감" in app._phs_label_guidance_notice.message
    # Only this incomplete tray is held; its remaining products still complete.
    for value in values[1:]:
        scan(app, value)
    [(_row, details)] = completions(app)
    assert details["shortage_confirmed"] is False
    assert not app.blocks and central[1] == []


@pytest.mark.parametrize("source_group", [(4, 4), (5, 5)], indirect=True)
def test_four_samples_keep_normal_completion(central):
    app, _master, values = start_resolved(central)
    for value in values[:4]:
        scan(app, value)
    [(_row, details)] = completions(app)
    assert details["shortage_confirmed"] is False
    assert not app.blocks and central[1] == []


@pytest.mark.parametrize("source_group", [(2, 1)], indirect=True)
def test_a_sealed_two_member_source_can_finish_its_one_member_group(central, scanner_clock):
    app, master, values = start_resolved(central)
    snapshot = app.current_set_info["package_source_snapshot"]
    assert snapshot["member_count"] == 1
    [source] = snapshot["work_group_source"]["source_transfers"]
    assert source["source_member_count"] == 2
    assert source["selected_member_count"] == source["remainder_member_count"] == 1
    scan(app, values[0])
    scanner_clock[0] += 5
    scan(app, master)
    [(_row, details)] = completions(app)
    assert details["shortage_confirmed"] is True
    assert details["product_sample_barcodes"] == values
    assert not app.blocks and central[1] == []


@pytest.mark.parametrize("registered", [False, True], ids=["local", "online"])
@pytest.mark.parametrize("scanner_only", [False, True], ids=["legacy-path", "scanner-path"])
def test_obsolete_manual_partial_is_refused_before_any_completion(packaging, registered, scanner_only):
    app, dialogs = packaging
    if not registered:
        app.package_logistics_client = None
        app._logistics_authoritative_required = False
    for value in [MASTER, *products()[:2]]:
        scan(app, value)
    app.current_set_info["scanner_only"] = scanner_only
    before = copy.deepcopy(app.current_set_info)
    assert app._finalize_set(app.Results.PASS, is_manual_complete=True) is False
    assert completions(app) == []
    assert app.current_set_info == before
    assert app._phs_label_guidance_notice.message == NOTICE
    with pytest.raises(PackageLogisticsError, match="manual packaging completion is disabled"):
        app._queue_authoritative_package(item_code=MASTER, is_manual_complete=True)
    assert app.current_set_info == before
    assert dialogs == []
