"""G9 changes slot wording while preserving the old fifth-input validation."""
import pytest

from tests.test_legacy_label_transition import MASTER
from tests.test_w9scanonly_failure_input import live_presenter
from tests.test_w9scanonly_packaging import packaging, products, scan, completions  # noqa: F401


def test_legacy_final_slot_describes_the_fourth_product(packaging):
    app, _dialogs = packaging
    live_presenter(app)
    for value in [MASTER, *products()[:3]]:
        scan(app, value)
    view = app._render_operator_workbench()
    assert view.current_stage_label == "제품4"
    assert view.next_action == "5/5 제품 4 스캔"
    assert app._next_action_text(4) == "5/5 제품 4 스캔"
    assert app.STEP_NAMES[-1] == "제품4"


@pytest.mark.parametrize("bad", [MASTER + "-SHORT", MASTER + "-LONG-PRODUCT-WITHOUT-PRODUCTION-DATE"])
def test_legacy_final_slot_still_requires_length_and_production_date(packaging, bad):
    app, dialogs = packaging
    for value in [MASTER, *products()[:3], bad]:
        scan(app, value)
    rows = completions(app)
    assert len(rows) == 1 and rows[0][1]["final_result"] == app.Results.FAIL_INPUT_ERROR
    assert dialogs == []
