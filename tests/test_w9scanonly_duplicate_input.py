"""G5: duplicate inputs leave the durable package and its undo position intact."""
import copy

import pytest

from tests.test_deferred_lease_clock_retry import clock_case  # noqa: F401
from tests.test_legacy_label_transition import MASTER
from tests.test_w9scanonly_central import central  # noqa: F401
from tests.test_w9scanonly_packaging import completions, packaging, products, scan  # noqa: F401


def assert_ignored_duplicate(app, value, dialogs):
    before = copy.deepcopy(app.current_set_info)
    scan(app, value)
    assert app.current_set_info == before, "duplicate must retain samples, exchange and undo state"
    assert not app.__dict__.get("_pending_workflow_error")
    assert completions(app) == [], "duplicate must not finalize a failed package"
    assert app.sounds[-1] == "fail"
    assert dialogs == []


@pytest.mark.parametrize("duplicate_scope", ["set", "all"])
def test_legacy_duplicate_is_ignored_and_next_product_continues(packaging, duplicate_scope):
    app, dialogs = packaging
    values = products()
    scan(app, MASTER)
    scan(app, values[0])
    duplicate = values[0] if duplicate_scope == "set" else products(prefix="DONE")[0]
    if duplicate_scope == "all":
        app.global_scanned_set.add(duplicate)
    assert_ignored_duplicate(app, duplicate, dialogs)
    scan(app, values[1])
    assert app.current_set_info["raw"] == [MASTER, *values[:2]]
    scan(app, "KMCMD:UNDO")
    assert app.current_set_info["raw"] == [MASTER, values[0]]


@pytest.mark.parametrize("duplicate_scope", ["set", "all"])
def test_central_duplicate_retains_exchange_overlay_and_undo(central, duplicate_scope):
    app, dialogs, case, root, _releases = central
    master = case.group["scan_payload"]
    values = [row["normalized_barcode"] for row in case.snapshot["work_group_source"]["members"]]
    scan(app, master)
    root.run_until(lambda: not app.__dict__.get("_phs2_background_pending"), timeout=5)
    scan(app, values[0])
    app.current_set_info["scanner_undo_exchange"] = {"synthetic": "retained"}
    duplicate = values[0] if duplicate_scope == "set" else products(case.group["item_id"], prefix="DONE")[0]
    if duplicate_scope == "all":
        app.global_scanned_set.add(duplicate)
    assert_ignored_duplicate(app, duplicate, dialogs)
    scan(app, values[1])
    assert app.current_set_info["raw"] == [master, *values[:2]]


def test_duplicate_after_exchange_keeps_real_pair_and_exchange_undo(packaging):
    app, dialogs = packaging
    target = products()[0]
    replacement = products(prefix="REPLACEMENT")[0]
    for value in [MASTER, target, "KMCMD:EXCHANGE", target, replacement]:
        scan(app, value)
    assert_ignored_duplicate(app, replacement, dialogs)
    scan(app, "KMCMD:UNDO")
    assert app.current_set_info["raw"] == [MASTER, target]
    assert app.current_set_info["scanner_exchanges"] == []
