"""Show local-only completed boxes without changing recorded PASS or delivery."""

import pytest

import Label_Match as label_module
import label_transition
from tests.test_label_match_core import _RecordingTree
from tests.test_legacy_label_transition import MASTER, _load_history
from tests.test_w9scanonly_packaging import completions, packaging, products, scan  # noqa: F401


NOTICE = "출고 전 관리자 보충 필요"


class RowTree(_RecordingTree):
    def item(self, iid, option=None, **updates):
        row = self.rows[iid]
        row.update(updates)
        return row.get(option) if option else row


def row_values(app, result):
    return (1, MASTER, *("" for _ in range(app.TOTAL_SCAN_COUNT - 1)), result, "12:34:56")


@pytest.mark.parametrize("registered,local_class,notice_expected", [
    (False, None, True), (True, label_transition.LEGACY, False),
    (True, label_transition.PHS2_LOCAL, True), (True, label_transition.PHS2_MALFORMED, True),
], ids=["unregistered-local", "old-label-local", "new-label-local", "malformed-local"])
def test_local_completion_notice_survives_history_reload(
    packaging, monkeypatch, registered, local_class, notice_expected,
):
    app, dialogs = packaging
    if not registered:
        app.package_logistics_client = None
        app._logistics_authoritative_required = False
    monkeypatch.setattr(label_module, "logistics_runtime_required", lambda: registered)
    app.history_tree = RowTree()
    app.session_tree = RowTree()
    app.summary_tree = RowTree()
    scan(app, MASTER)
    identity = str(app.current_set_info["id"])
    if local_class:
        app.current_set_info.update(label_transition.fields(local_class, [], False))
    app.history_tree.insert("", "end", iid=identity, values=row_values(app, app.Results.IN_PROGRESS))
    for value in products():
        scan(app, value)
    [(_row, details)] = completions(app)
    assert details["final_result"] == app.Results.PASS
    assert not (details.get("package_logistics") or {}).get("idempotency_key")
    assert (NOTICE in app.save_status_label.kwargs["text"]) == notice_expected, (
        details["final_result"], (details.get("package_logistics") or {}).get("status"),
        bool((details.get("package_logistics") or {}).get("package_id")))
    assert (NOTICE in app.history_tree.item(identity, "values")[1 + app.TOTAL_SCAN_COUNT]) == notice_expected
    assert app._history_result_value(app.history_tree.item(identity, "values")) == app.Results.PASS
    assert (NOTICE in app._barcode_inline_detail_text(details)) == notice_expected
    app._refresh_session_tree()
    assert (NOTICE in app.session_tree.item(f"session-{identity}", "values")[2]) == notice_expected

    restored = _load_history(label_module, app)
    app.history_row_details_map = restored["set_details_map"]
    app.history_tree = RowTree()
    for _ in app._history_display_steps(restored):
        pass
    assert (NOTICE in app.history_tree.item(identity, "values")[1 + app.TOTAL_SCAN_COUNT]) == notice_expected
    app._refresh_session_tree()
    assert (NOTICE in app.session_tree.item(f"session-{identity}", "values")[2]) == notice_expected
    app._refresh_history_tree_display_values()
    assert app.history_tree.item(identity, "values")[1 + app.TOTAL_SCAN_COUNT].count(NOTICE) == int(notice_expected)
    assert app.history_row_details_map[identity]["final_result"] == app.Results.PASS
    assert app._history_result_value(app.history_tree.item(identity, "values")) == app.Results.PASS
    assert not app.blocks and dialogs == []


@pytest.mark.parametrize("result,metadata", [
    ("통과", {"status": "PENDING", "idempotency_key": "package-intent"}),
    ("통과", {"status": "ACKED", "idempotency_key": "package-intent"}),
    ("통과", {"status": "CONFLICT", "idempotency_key": "package-intent"}),
    ("통과", {"status": "LOCAL_SHORT_CONFIRMATION"}),
    ("통과", {"status": "LEGACY_DIRECT_SYNC_ONLY",
              "transition_class": label_transition.LEGACY}),
    ("통과", {"status": label_transition.LOCAL_ONLY_STATUS,
              "transition_class": label_transition.LEGACY}),
    ("진행 중", {}), ("입력 오류", {}),
], ids=["pending", "acked", "conflict", "shortage-grace", "legacy-direct-sync",
        "legacy-package-metadata", "unfinished", "failed"])
def test_delivery_and_unfinished_rows_keep_their_existing_status(result, metadata):
    app = object.__new__(label_module.Label_Match)
    app.current_set_info = {}
    app.set_details_map = {}
    details = {"final_result": result, "package_logistics": metadata,
               "parsed_product_barcodes": [MASTER]}
    app.history_row_details_map = {"box": details}
    app.history_tree = RowTree()
    app.history_tree.insert("", "end", iid="box", values=row_values(app, result))
    app._refresh_history_tree_display_values()
    assert app.history_tree.item("box", "values")[1 + app.TOTAL_SCAN_COUNT] == result
    assert NOTICE not in app._barcode_inline_detail_text(details)
