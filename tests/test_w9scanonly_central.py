"""PHS2 samples complete locally without waiting for central confirmation.

설계 초안 값 — 바뀔 수 있음: the fifth new-label scan uses the
same product rule as slots 2–4. A saved membership restricts the samples.
Real capture/validation and CSV consumers; transport and Tk are in-memory.
"""

import threading

import pytest

from package_logistics import PackageTransportError
from tests.test_deferred_lease_clock_retry import clock_case  # noqa: F401
from tests.test_tk_serial_ui_lane import FakeTkRoot
from tests.test_w9scanonly_packaging import completions, packaging, products, scan  # noqa: F401
from tk_serial_ui_lane import TkSerialUiLane


@pytest.fixture
def central(packaging, clock_case):
    app, dialogs = packaging
    case = clock_case
    for name in ("deferred_intent_capture", "package_logistics_client",
                 "package_operation_lease_store", "package_operation_lease_keyring"):
        setattr(app, name, getattr(case.app, name))
    app._deferred_intent_capture_error = ""
    item = case.group["item_id"]
    app.items_data[item] = {"Item Name": "Synthetic sample item", "Spec": "PCS"}
    app._ui_lane_generation = 0
    app._ui_lane_busy_label = ""
    app._ui_lane_busy_task = ""
    app._tk_shutdown_requested = False
    app._focus_scan_entry_if_available = lambda: None
    root = FakeTkRoot()
    app.ui_lane = TkSerialUiLane(root, poll_ms=1, generation_provider=lambda: app._ui_lane_generation)
    releases = []
    try:
        yield app, dialogs, case, root, releases
    finally:
        for release in releases:
            release.set()
        root.run_until(lambda: not app.ui_lane.is_busy(), timeout=5)
        app.ui_lane.close_idle()


def test_resolved_phs2_and_four_member_products_complete_without_f3_or_confirmation(central):
    app, dialogs, case, root, _releases = central
    master = case.group["scan_payload"]
    values = [row["normalized_barcode"] for row in case.snapshot["work_group_source"]["members"]]
    assert len(values) == 4, "fixture must represent four real source members"
    scan(app, master)
    root.run_until(lambda: not app.ui_lane.is_busy(), timeout=5)
    assert app.current_set_info["raw"] == [master], "valid synthetic central evidence must be accepted"
    assert app.current_set_info["package_source_snapshot"] == case.snapshot
    for value in values:
        scan(app, value)
    rows = completions(app)
    assert len(rows) == 1 and rows[0][1]["final_result"] == app.Results.PASS, (
        "four member products after PHS2 must complete locally on scan five, without F3"
    )
    assert rows[0][1]["scan_count"] == 5
    assert rows[0][1]["scanned_product_barcodes"] == [master, *values]
    assert app.durable_commits[-1]["result"] == app.Results.PASS
    assert dialogs == [], "PHS2 packaging must not open a completion or scan warning dialog"


def test_unanswered_central_lookup_does_not_block_samples_or_local_completion(central):
    app, dialogs, case, root, releases = central
    entered, release = threading.Event(), threading.Event()
    releases.append(release)

    def unanswered(_draft):
        entered.set()
        release.wait()  # Released by fixture teardown, including assertion failures.
        raise PackageTransportError("synthetic disconnected transport")

    app.package_logistics_client.resolve_package_source_evidence = unanswered
    master = case.group["scan_payload"]
    values = products(case.group["item_id"])
    scan(app, master)
    root.run_until(entered.is_set, timeout=5)
    # The central transport remains unanswered throughout these scans. There
    # is no latency assertion or artificially reduced production timeout.
    assert not release.is_set()
    for value in values:
        scan(app, value)
    rows = completions(app)
    assert len(rows) == 1 and rows[0][1]["final_result"] == app.Results.PASS, (
        "local durable completion must finish while central lookup remains unanswered"
    )
    assert rows[0][1]["scanned_product_barcodes"] == [master, *values]
    assert app.durable_commits[-1]["result"] == app.Results.PASS
    assert not release.is_set(), "completion must precede the central response"
    assert dialogs == []
    scan(app, master.replace("ITG-WORK-ONE", "ITG-NEXT-PACKAGE").replace("LBL-WORK-PACKAGE", "LBL-NEXT"))
    assert app.current_set_info["raw"], "next PHS2 must be accepted while the earlier lookup is pending"


def test_disconnected_central_transport_still_allows_five_scan_local_completion(central):
    app, dialogs, case, root, _releases = central
    calls = []

    def disconnected(_draft):
        calls.append("disconnected")
        raise PackageTransportError("synthetic disconnected transport")

    app.package_logistics_client.resolve_package_source_evidence = disconnected
    master = case.group["scan_payload"]
    values = products(case.group["item_id"])
    scan(app, master)
    root.run_until(lambda: not app.ui_lane.is_busy(), timeout=5)
    assert calls == ["disconnected"], "the fake transport must actually be exercised"
    for value in values:
        scan(app, value)
    rows = completions(app)
    assert len(rows) == 1 and rows[0][1]["final_result"] == app.Results.PASS, (
        "transport loss must leave a locally completable five-scan set"
    )
    assert rows[0][1]["scan_count"] == 5
    assert rows[0][1]["scanned_product_barcodes"] == [master, *values]
    assert app.durable_commits[-1]["result"] == app.Results.PASS
    assert dialogs == []
