"""App data stays complete at the final web presentation boundary."""
from types import SimpleNamespace
import pytest

from tests.test_label_screen_bridge import (
    CONTRACT, MASTER, call, meta, schema_errors, screen,
)


def test_recent_and_history_codes_use_source_instead_of_tk_cells(screen):
    raw = "PRODUCT-FULL-SCAN-0000123456789"
    call(screen, screen.bridge.scan, MASTER, meta(1))
    call(screen, screen.bridge.scan, raw, meta(2))
    iid, cells = screen.app.history_tree.rows[0]
    screen.app.history_row_details_map[str(iid)] = {
        "scanned_product_barcodes": [MASTER, raw],
    }
    shortened = list(cells)
    shortened[1:3] = ["…master", "…6789"]
    screen.app.history_tree.rows = [(iid, shortened)]
    state = call(screen, screen.bridge.state)
    assert schema_errors(CONTRACT["state"], state) == []
    assert state["work"]["result"]["sub"] == raw
    assert state["today"]["rows"][0]["raw"] == [MASTER, raw]
    assert state["today"]["rows"][0]["tag"] == MASTER


def test_summary_products_are_membership_totals_and_unknown_stays_optional(screen):
    app = screen.app
    iid, cells = app.summary_tree.rows[0]
    trays = int(cells[2])
    app.Results = SimpleNamespace(PASS="pass")
    app.summary_row_raw_values = {iid: ("today", "ITEM", "-")}
    app.history_row_details_map = {}
    app.set_details_map = {
        str(i): {"item_code": "ITEM", "phase": "-", "final_result": "pass",
                 "scanned_product_barcodes": [MASTER, "SAMPLE"],
                 "package_logistics": {"expected_member_count": 9 + i}}
        for i in range(trays)
    }
    state = call(screen, screen.bridge.state)
    assert schema_errors(CONTRACT["state"], state) == []
    row = state["today"]["summary"][0]
    assert row["count"] == trays
    assert row["products"] == sum(9 + i for i in range(trays))
    app.set_details_map["0"].pop("package_logistics")
    assert "products" not in call(screen, screen.bridge.state)["today"]["summary"][0]


def test_exchange_contract_includes_both_full_codes(screen):
    app = screen.app
    remove, insert = "REMOVE-PRODUCT-FULL-0123456789", "INSERT-PRODUCT-FULL-9876543210"
    for source, expected in (
        ({"scanner_exchange_phase": "target"}, ("remove", None, None)),
        ({"scanner_exchange_phase": "replacement", "scanner_exchange_target": remove},
         ("insert", remove, None)),
        ({"scanner_undo_exchange": "exchange-one", "scanner_exchanges": [
            {"idempotency_key": "exchange-one", "target_barcode": remove,
             "replacement_barcode": insert}]}, ("done", remove, insert)),
    ):
        app.current_set_info = {"raw": [MASTER], "parsed": [MASTER], **source}
        app._render_operator_workbench()
        state = call(screen, screen.bridge.state)
        assert schema_errors(CONTRACT["state"], state) == []
        exchange = state["work"]["exchange"]
        assert (exchange["phase"], exchange["remove"]["code"], exchange["insert"]["code"]) == expected


def test_ui_contract_declares_worker_and_restoration_sheets():
    sheets = CONTRACT["sheets"]
    for name in ("LABEL.SET_HOLD", "LABEL.F5_HOLD", "LABEL.SUPPORT_HANDOFF", "LABEL.PHS_LOOKUP"):
        assert sheets[name]["pin"] is False
    assert sheets["LABEL.RECHECK"]["pin"] is True


@pytest.mark.parametrize("action", [
    "LABEL.SET_HOLD", "LABEL.F5_HOLD", "LABEL.SUPPORT_HANDOFF", "LABEL.PHS_LOOKUP",
])
def test_worker_sheet_commands_reach_app_without_credentials(screen, monkeypatch, action):
    received = []
    def submit(name, target, physical, *, window, finish, fail):
        received.append((name, target, physical))
        finish(True)
        return True
    monkeypatch.setattr(screen.app, "_submit_package_pin_action", submit)
    target = "F5:hold" if action == "LABEL.F5_HOLD" else "held-set"
    args = {"target": target}
    if action == "LABEL.PHS_LOOKUP":
        args["physical_qr"] = MASTER
    assert call(screen, screen.bridge.command, action, args)["result"]["ok"]
    assert received == [(action, target, args.get("physical_qr", ""))]
    assert "_screen_admin_credentials" not in screen.app.__dict__


def test_ordinary_recheck_and_restore_cannot_use_worker_command(screen, monkeypatch):
    monkeypatch.setattr(screen.app, "_submit_package_pin_action", lambda *_a, **_k: pytest.fail("unauthorized effect"))
    assert call(screen, screen.bridge.command, "LABEL.RECHECK", {"target": "ordinary-set"})["result"]["ok"] is False
    reply = call(screen, screen.bridge.admin, "LABEL.RECHECK", {"target": "ordinary-set"})
    assert reply["result"]["ok"] is False
    reply = call(screen, screen.bridge.command, "LABEL.SET_HOLD", {
        "target": "ordinary-set", "physical_qr": "__RESTORE_CHANGED_PIN_FILE__",
    })
    assert reply["result"]["ok"] is False
