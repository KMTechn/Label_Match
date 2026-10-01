"""G8: authenticated progress reads never own scanner input or work context."""
import copy
from types import SimpleNamespace
from urllib.parse import parse_qs, urlsplit

import pytest

from package_logistics import PackageLogisticsClient, PackageTransportError
from tests.test_deferred_lease_clock_retry import clock_case  # noqa: F401
from tests.test_legacy_label_transition import MASTER
from tests.test_w9scanonly_packaging import packaging, products, scan  # noqa: F401
from tests.test_w9scanonly_query_input import Panes
from tests.test_tk_serial_ui_lane import FakeTkRoot


PROGRESS = {"date": "2026-10-01", "generated_at": "2026-10-01T00:00:00Z", "items": [
    {"item_id": MASTER, "name": "시험 품목", "plan_qty": 120, "packaged_qty": 60,
     "remaining_qty": 60, "remaining_boxes": 1},
]}


def test_progress_uses_existing_authenticated_client_and_packaging_scope(clock_case):
    requests = []
    def transport(method, url, headers, body, timeout):
        requests.append((method, url, body))
        return {"ok": True, "data": copy.deepcopy(PROGRESS)}
    client = PackageLogisticsClient(clock_case.app.package_logistics_client.config, transport=transport)
    assert client.get_progress(business_date="2026-10-01") == PROGRESS
    [(method, url, body)] = requests
    assert method == "GET" and body is None
    assert urlsplit(url).path == "/logistics/api/v1/progress"
    assert parse_qs(urlsplit(url).query) == {
        "authority_scope_id": [client.config.authority_scope_id],
        "date": ["2026-10-01"], "stage": ["packaging"],
    }


@pytest.fixture
def progress_app(packaging):
    app, dialogs = packaging
    root = FakeTkRoot()
    app.after = root.after
    app.operator_notebook = Panes()
    app.session_tab = "work"
    app.logistics_progress_tab = "progress"
    app.progress_text = ""
    app._render_logistics_progress = lambda: setattr(app, "progress_text", app._logistics_progress_text())
    yield app, dialogs, root
    root.run_until(lambda: not app.__dict__.get("_logistics_progress_thread") or
                   not app._logistics_progress_thread.is_alive(), timeout=5)


def open_progress(app):
    app.operator_notebook.select("progress")
    app._on_logistics_progress_tab_changed()


def test_one_click_progress_refreshes_each_minute_and_keeps_last_good_value_on_loss(progress_app):
    app, dialogs, root = progress_app
    calls = []
    offline = False
    def get_progress(**kwargs):
        calls.append(kwargs)
        if offline:
            raise PackageTransportError("synthetic disconnect")
        return copy.deepcopy(PROGRESS)
    app.package_logistics_client = SimpleNamespace(get_progress=get_progress)
    open_progress(app)
    root.run_until(lambda: bool(app.__dict__.get("_logistics_progress_last_success")), timeout=5)
    before = copy.deepcopy(app._logistics_progress_snapshot)
    last_success = app._logistics_progress_last_success
    assert "시험 품목" in app.progress_text and "120" in app.progress_text
    assert "60" in app.progress_text and "남은 상자 1" in app.progress_text
    assert any(delay == 60000 for _, delay, _, _ in root.jobs)
    offline = True
    app._refresh_logistics_progress()
    root.run_until(lambda: app.__dict__.get("_logistics_progress_connected") is False, timeout=5)
    assert app._logistics_progress_snapshot == before
    assert app._logistics_progress_last_success == last_success
    assert "연결 끊김" in app.progress_text and last_success in app.progress_text
    assert app.operator_notebook.select() == "progress" and dialogs == []


def test_progress_scan_returns_to_work_once_and_late_result_stays_in_background(progress_app):
    import threading
    app, dialogs, root = progress_app
    release = threading.Event()
    entered = threading.Event()
    def delayed(**kwargs):
        entered.set()
        release.wait()
        return copy.deepcopy(PROGRESS)
    app.package_logistics_client = SimpleNamespace(get_progress=delayed)
    try:
        scan(app, MASTER)
        open_progress(app)
        root.run_until(entered.is_set, timeout=5)
        value = products()[0]
        scan(app, value)
        assert app.operator_notebook.select() == "work"
        assert app.current_set_info["raw"] == [MASTER, value]
        release.set()
        root.run_until(lambda: bool(app.__dict__.get("_logistics_progress_snapshot")), timeout=5)
        assert app.operator_notebook.select() == "work"
        assert app.current_set_info["raw"] == [MASTER, value] and dialogs == []
    finally:
        release.set()


def test_invalid_progress_response_preserves_previous_success(progress_app):
    app, _dialogs, root = progress_app
    app._logistics_progress_snapshot = copy.deepcopy(PROGRESS)
    app._logistics_progress_last_success = "2026-10-01 09:00:00"
    app.package_logistics_client = SimpleNamespace(get_progress=lambda **kw: {"items": "broken"})
    app._refresh_logistics_progress()
    root.run_until(lambda: app.__dict__.get("_logistics_progress_connected") is False, timeout=5)
    assert app._logistics_progress_snapshot == PROGRESS
    assert "2026-10-01 09:00:00" in app.progress_text and "연결 끊김" in app.progress_text
