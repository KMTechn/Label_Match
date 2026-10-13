"""Packing top scan count from the company web setting (packing-settings-v1).

Fake server and fake transport only: no network, no window.
"""

import json
import threading
import time
from types import SimpleNamespace

import pytest

import Label_Match as label_module
import label_packing_settings as settings
from package_command_draft import PackageCommandDraft
from package_errors import PackageApiError, PackageLogisticsError
from package_logistics import PackageOutboxProcessor
from tests.test_deferred_lease_clock_retry import clock_case  # noqa: F401
from tests.test_tk_serial_ui_lane import FakeTkRoot
from tests.test_w9scanonly_central import central  # noqa: F401
from tests.test_w9scanonly_packaging import completions, packaging, products, scan  # noqa: F401
from tests.test_legacy_label_transition import MASTER
from ui.workflow_snapshot_adapter import adapt_workflow_snapshot
from ui.workflow_view_state import present_workflow


def _server(count, version=1):
    return settings.PackingSettingsState(count, settings.SOURCE_SERVER, version)


def _use(app, count, version=1):
    app._packing_settings = _server(count, version)


def _many_products(item, count):
    values = [value for prefix in ("SAMPLE", "EXTRA", "MORE") for value in products(item, prefix=prefix)]
    assert len(values) >= count
    return values[:count]


# ---- web read: parse, transport, trust, fallback ---------------------------------

class _Response:
    def __init__(self, status, body):
        self.status_code = status
        self.content = json.dumps(body).encode("utf-8") if not isinstance(body, bytes) else body


def _profile(base="https://packing.example.test"):
    return SimpleNamespace(bearer_token="synthetic-bearer", source_host_id="host-1",
                           device_id="device-1", base_url=base, tls_ca_bundle_path="")


def test_fetch_reads_the_contract_with_item_catalog_headers():
    calls = []

    def get(url, **kwargs):
        calls.append((url, kwargs))
        return _Response(200, {"contract": "packing-settings-v1", "top_scan_count": 6,
                               "version": 3, "updated_at": "2026-10-13T09:00:00+09:00"})

    result = settings.fetch(get=get, profile_loader=_profile)
    assert result == {"top_scan_count": 6, "version": 3, "updated_at": "2026-10-13T09:00:00+09:00"}
    url, kwargs = calls[0]
    assert url == "https://packing.example.test/inbound/api/packing-settings.json"
    assert kwargs["allow_redirects"] is False
    assert set(kwargs["headers"]) == {"User-Agent", "Authorization", "X-Logistics-Source-Host-Id",
                                      "X-Logistics-Device-Id", "X-Logistics-Program"}


@pytest.mark.parametrize("status", [400, 401, 302, 503])
def test_server_refusals_are_unavailable(status):
    with pytest.raises(settings.PackingSettingsReadError) as caught:
        settings.fetch(get=lambda url, **kw: _Response(status, {"ok": False}), profile_loader=_profile)
    assert caught.value.code == settings.UNAVAILABLE


def test_transport_failure_and_missing_profile_are_unavailable_without_request():
    def broken(url, **kwargs):
        raise TimeoutError("synthetic timeout")

    with pytest.raises(settings.PackingSettingsReadError) as caught:
        settings.fetch(get=broken, profile_loader=_profile)
    assert caught.value.code == settings.UNAVAILABLE
    sent = []
    for loader in (lambda: None, lambda: _profile("http://packing.example.test")):
        with pytest.raises(settings.PackingSettingsReadError) as caught:
            settings.fetch(get=lambda url, **kw: sent.append(url), profile_loader=loader)
        assert caught.value.code == settings.UNAVAILABLE
    assert sent == [], "an untrusted URL or no profile sends no request"


@pytest.mark.parametrize("body", [
    {"contract": "packing-settings-v2", "top_scan_count": 4, "version": 0, "updated_at": None},
    {"contract": "packing-settings-v1", "top_scan_count": 0, "version": 0, "updated_at": None},
    {"contract": "packing-settings-v1", "top_scan_count": 7, "version": 0, "updated_at": None},
    {"contract": "packing-settings-v1", "top_scan_count": True, "version": 0, "updated_at": None},
    {"contract": "packing-settings-v1", "top_scan_count": 4.0, "version": 0, "updated_at": None},
    {"contract": "packing-settings-v1", "top_scan_count": 4, "version": -1, "updated_at": None},
    b"not json",
])
def test_wrong_contract_or_value_is_invalid(body):
    with pytest.raises(settings.PackingSettingsReadError) as caught:
        settings.fetch(get=lambda url, **kw: _Response(200, body), profile_loader=_profile)
    assert caught.value.code == settings.INVALID


def test_fallback_texts_for_received_and_never_received(tmp_path):
    saved = {"top_scan_count": 6, "version": 2, "updated_at": None, "fetched_at": None}
    cases = [
        (settings.UNAVAILABLE, saved, 6, "last_saved", "회사 서버 설정을 받지 못해 마지막 값(6개)으로 작업합니다"),
        (settings.UNAVAILABLE, None, 4, "default", "회사 서버 설정을 받지 못해 기본값(4개)으로 작업합니다"),
        (settings.INVALID, saved, 6, "last_saved",
         "회사 서버 설정 값이 올바르지 않아 마지막 값(6개)으로 작업합니다 — 관리자에게 알려 주세요"),
        (settings.INVALID, None, 4, "default",
         "회사 서버 설정 값이 올바르지 않아 기본값(4개)으로 작업합니다 — 관리자에게 알려 주세요"),
    ]
    for code, last, value, source, text in cases:
        state = settings.state_after_read(None, code, last)
        assert (state.value, state.source, state.alert_code, state.alert_text) == (value, source, code, text)
    assert settings.state_after_read(None, settings.UNAVAILABLE, None).version == 0


def test_received_value_is_kept_and_survives_restart(tmp_path):
    path = tmp_path / "packing-settings.json"
    assert settings.load_saved(path) is None
    settings.save_received(path, {"top_scan_count": 2, "version": 5,
                                  "updated_at": None, "fetched_at": "2026-10-13T00:00:00+00:00"})
    state = settings.state_from_saved(settings.load_saved(path))
    assert (state.value, state.version, state.source, state.alert_code) == (2, 5, "last_saved", None)
    path.write_text("{broken", encoding="utf-8")
    assert settings.load_saved(path) is None


# ---- the Tk thread never waits for the network -------------------------------

def test_refresh_reads_off_the_tk_thread_and_applies_on_poll(tmp_path):
    app = object.__new__(label_module.Label_Match)
    root = FakeTkRoot()
    app.after = root.after
    app._render_operator_workbench = lambda: None
    app._packing_settings_path = str(tmp_path / "packing-settings.json")
    app._packing_settings = settings.default_state()
    app._packing_settings_queue = __import__("queue").Queue()
    app._packing_settings_thread = None
    app._packing_settings_pending = False
    app._packing_settings_poll_after_id = None
    entered, release = threading.Event(), threading.Event()
    tk_thread = threading.get_ident()
    seen = []

    def slow_fetch():
        seen.append(threading.get_ident())
        entered.set()
        release.wait(5)
        return {"top_scan_count": 6, "version": 7, "updated_at": None}

    started = time.monotonic()
    assert app._request_packing_settings_refresh(fetch=slow_fetch) is True
    assert time.monotonic() - started < 1.0, "the request must not wait for the server"
    assert entered.wait(5)
    assert app._packing_settings_state().value == 4, "nothing applied before the result"
    release.set()
    root.run_until(lambda: app._packing_settings_state().source == "server", timeout=5)
    assert seen and seen[0] != tk_thread
    assert (app._packing_settings.value, app._packing_settings.version) == (6, 7)
    assert settings.load_saved(app._packing_settings_path)["top_scan_count"] == 6

    def unavailable():
        raise settings.PackingSettingsReadError(settings.UNAVAILABLE)

    app._request_packing_settings_refresh(fetch=unavailable)
    root.run_until(lambda: app._packing_settings_state().alert_code is not None, timeout=5)
    state = app._packing_settings_state()
    assert (state.value, state.source) == (6, "last_saved")
    notice = app._packing_settings_notice()
    assert notice.tone == "warning" and notice.message == state.alert_text


# ---- view state ----------------------------------------------------------------

@pytest.mark.parametrize("count", [1, 2, 4, 6])
def test_view_slots_follow_the_set_count(count):
    raw = ["M", *[f"P{i}" for i in range(count)]]
    current = {"raw": raw[:2], "parsed": raw[:2]}
    if count != 4:
        current["top_scan_count"] = count
    view = present_workflow(adapt_workflow_snapshot(current, next_set_top_scan_count=3))
    assert (view.top_scan_count, view.scan_total, view.scan_done) == (count, count + 1, 2)
    assert view.qa_progress_text == f"2/{count + 1}"
    assert [slot.key for slot in view.slots][0] == "master_label"
    assert view.slots[-1].key == "final_label" and view.slots[-1].label == f"제품{count}"
    assert [slot.role for slot in view.slots] == ["master_label", *["product"] * (count - 1), "last_product"]
    assert view.next_set_top_scan_count == (3 if count != 3 else None)
    full = present_workflow(adapt_workflow_snapshot({"raw": raw, "parsed": raw, **(
        {"top_scan_count": count} if count != 4 else {})}))
    assert full.manual_complete_hint == f"이미 {count + 1}개 완료됨"


def test_view_without_a_set_uses_the_next_set_count_and_carries_alerts():
    alert = {"kind": "warn", "code": settings.UNAVAILABLE, "text": "t"}
    view = present_workflow(adapt_workflow_snapshot(
        {"raw": [], "parsed": []}, next_set_top_scan_count=6,
        top_scan_setting={"value": 6, "source": "server", "version": 2,
                          "updated_at": None, "fetched_at": None},
        alerts=(alert,)))
    assert (view.top_scan_count, view.scan_total, len(view.slots)) == (6, 7, 7)
    assert view.next_set_top_scan_count is None
    assert view.top_scan_setting.value == 6 and view.top_scan_setting.source == "server"
    assert [(a.kind, a.code) for a in view.alerts] == [("warn", settings.UNAVAILABLE)]
    default = present_workflow(adapt_workflow_snapshot({"raw": ["M"], "parsed": ["M"]}))
    assert default.qa_progress_text == "1/5" and default.slots[-1].label == "제품4"


# ---- draft: samples and version -------------------------------------------------

def _scanner_draft(count, samples, *, short=False, version=0):
    return PackageCommandDraft.build(
        set_id="SET-1", item_code="ITEM", source_input_tag_id="ITG-1",
        external_label="PKG-1", sample_barcodes=samples, scanner_only=True,
        short_confirmed=short, top_scan_count=count, packing_settings_version=version)


@pytest.mark.parametrize("count", [1, 2, 4, 6])
def test_scanner_draft_requires_n_samples_or_short_one_to_n_minus_one(count):
    samples = [f"S{i}" for i in range(count)]
    draft = _scanner_draft(count, samples, version=3)
    assert draft.top_scan_count == count and draft.packing_settings_version == 3
    assert PackageCommandDraft.from_dict(draft.to_dict()) == draft
    with pytest.raises(PackageLogisticsError):
        _scanner_draft(count, samples[:-1])
    if count > 1:
        assert _scanner_draft(count, samples[:-1], short=True).short_confirmed
    with pytest.raises(PackageLogisticsError):
        _scanner_draft(count, samples, short=True)


def test_default_draft_keeps_todays_bytes():
    draft = _scanner_draft(4, ["A", "B", "C", "D"])
    assert "top_scan_count" not in draft.to_dict() and "packing_settings_version" not in draft.to_dict()


# ---- scanner flow ----------------------------------------------------------------

@pytest.mark.parametrize("count", [1, 2, 4, 6])
def test_set_completes_after_n_products(packaging, count):
    app, dialogs = packaging
    _use(app, count, version=9)
    values = [MASTER, *_many_products(MASTER, count)]
    for value in values[:-1]:
        scan(app, value)
    assert completions(app) == []
    assert app._workflow_total_scan_count() == count + 1
    scan(app, values[-1])
    rows = completions(app)
    assert len(rows) == 1 and rows[0][1]["final_result"] == app.Results.PASS
    assert rows[0][1]["scan_count"] == count + 1
    assert rows[0][1]["scanned_product_barcodes"] == values
    if count == 4:
        assert "top_scan_count" not in rows[0][1]
    else:
        assert rows[0][1]["top_scan_count"] == count
        assert rows[0][1]["packing_settings_version"] == 9
    assert dialogs == []


def test_n4_with_version_zero_writes_no_new_set_keys(packaging):
    app, _dialogs = packaging
    scan(app, MASTER)
    assert "top_scan_count" not in app.current_set_info
    assert "packing_settings_version" not in app.current_set_info


def test_middle_change_keeps_the_old_count_and_next_set_uses_the_new(packaging):
    app, dialogs = packaging
    _use(app, 2, version=1)
    first = _many_products(MASTER, 6)
    scan(app, MASTER)
    scan(app, first[0])
    _use(app, 6, version=2)  # the web setting changes mid-set
    assert app._workflow_total_scan_count() == 3, "the open set keeps its count"
    scan(app, first[1])
    rows = completions(app)
    assert len(rows) == 1 and rows[0][1]["scan_count"] == 3
    assert rows[0][1]["packing_settings_version"] == 1
    second = _many_products(MASTER, 12)[6:12]
    scan(app, MASTER)
    assert app.current_set_info["top_scan_count"] == 6
    for value in second[:5]:
        scan(app, value)
    assert len(completions(app)) == 1
    scan(app, second[5])
    assert len(completions(app)) == 2 and completions(app)[-1][1]["scan_count"] == 7
    assert dialogs == []


def test_undo_and_reopen_keep_the_count(packaging):
    app, dialogs = packaging
    _use(app, 6, version=4)
    values = _many_products(MASTER, 6)
    scan(app, MASTER)
    scan(app, values[0])
    scan(app, values[1])
    scan(app, "KMCMD:UNDO")
    assert app.current_set_info["raw"] == [MASTER, values[0]]
    saved = app.data_manager.load_current_state()["current_set_info"]
    assert (saved["top_scan_count"], saved["packing_settings_version"]) == (6, 4)
    # Reopen after a restart while the web setting is 2 now.
    _use(app, 2, version=5)
    reopened = {**saved}
    assert app._workflow_total_scan_count(reopened) == 7
    assert label_module._label_match_set_total_scan_count({"raw": ["x"]}) == 5, "old record = 4"
    for value in values[1:]:
        scan(app, value)
    rows = completions(app)
    assert len(rows) == 1 and rows[0][1]["scan_count"] == 7
    assert rows[0][1]["packing_settings_version"] == 4
    assert dialogs == []


def test_manual_short_confirm_uses_the_set_count(packaging):
    app, dialogs = packaging
    _use(app, 6, version=2)
    values = _many_products(MASTER, 6)
    for value in [MASTER, *values[:3]]:
        scan(app, value)
    app._scanner_previous_scan = None
    scan(app, MASTER)  # the label again = short confirmation
    rows = completions(app)
    assert len(rows) == 1 and rows[0][1]["scan_count"] == 4
    assert rows[0][1]["shortage_confirmed"] is True
    assert app.current_set_info["shortage_undo_snapshot"]["raw"] == [MASTER, *values[:3]]
    assert dialogs == []


def test_manual_complete_hint_and_texts_follow_the_count(packaging):
    app, _dialogs = packaging
    _use(app, 6)
    assert "상단 제품 6 · 일곱 번째 스캔에서 자동 완료" in app._operator_workflow_hint_text()
    scan(app, MASTER)
    assert app._next_action_text(1) == "2/7 제품 1 스캔"
    app.current_set_info["raw"] = [MASTER] * 7
    app.current_set_info["parsed"] = [MASTER] * 7
    assert app._manual_complete_hint() == "이미 7개 완료됨"


# ---- confirm request carries the set's version; 400 resends ------------------------

def _central_short_set(app, case, root, count, version):
    _use(app, count, version=version)
    master = case.group["scan_payload"]
    values = [row["normalized_barcode"] for row in case.snapshot["work_group_source"]["members"]]
    scan(app, master)
    root.run_until(lambda: not app.__dict__.get("_phs2_background_pending"), timeout=5)
    identity = app.current_set_info["id"]
    for value in values:
        scan(app, value)
    return identity, master, values


def test_confirm_payload_carries_the_set_version_and_n6_allows_four_member_short(central):
    app, dialogs, case, root, _releases = central
    identity, master, values = _central_short_set(app, case, root, 6, 8)
    assert completions(app) == [], "four members of a six-count set wait for more or a short confirm"
    app._scanner_previous_scan = None
    scan(app, master)
    assert len(completions(app)) == 1
    assert app._release_shortage_confirmation()
    row = app.package_outbox.get_by_set_id(identity)
    draft = PackageCommandDraft.from_dict(json.loads(row["draft_json"]))
    assert (draft.top_scan_count, draft.packing_settings_version, draft.short_confirmed) == (6, 8, True)
    sent = []

    def build(draft, **kwargs):
        return "TRANSFER-SYNTHETIC", {"idempotency_key": kwargs["idempotency_key"],
                                      "payload": {"sample_barcodes": list(draft.sample_barcodes)}}

    def send(command):
        sent.append(json.loads(json.dumps(command)))
        raise PackageApiError(409, "UPSTREAM_COMPLETION_PENDING", "waiting", retryable=True, committed=False)

    client = SimpleNamespace(build_create_package_command=build, create_package=send)
    processor = PackageOutboxProcessor(app.package_outbox, client, prepare_scanner_delivery=lambda d: d)
    assert processor.drain(limit=1) == {"acked": 0, "retry": 1, "conflict": 0}
    assert sent[0]["payload"]["packing_settings_version"] == 8
    assert sent[0]["payload"]["short_confirmed"] is True
    assert dialogs == []


def test_version_invalid_400_keeps_the_confirm_rereads_and_resends(central):
    app, dialogs, case, root, _releases = central
    identity, _master, _values = _central_short_set(app, case, root, 4, 0)
    assert len(completions(app)) == 1
    sent, reads = [], []

    def build(draft, **kwargs):
        return "TRANSFER-SYNTHETIC", {"idempotency_key": kwargs["idempotency_key"], "authority_scope_id": "",
                                      "payload": {"sample_barcodes": list(draft.sample_barcodes)}}

    def send(command):
        sent.append(json.loads(json.dumps(command)))
        if len(sent) == 1:
            raise PackageApiError(400, "PACKING_SETTINGS_VERSION_INVALID", "bad version",
                                  retryable=False, committed=False)
        raise PackageApiError(409, "UPSTREAM_COMPLETION_PENDING", "waiting", retryable=True, committed=False)

    def reread():
        reads.append(1)
        return 12

    client = SimpleNamespace(build_create_package_command=build, create_package=send,
                             get_receipt_if_exists=lambda *a, **k: None)
    processor = PackageOutboxProcessor(app.package_outbox, client, prepare_scanner_delivery=lambda d: d,
                                       read_packing_settings_version=reread)
    assert processor.drain(limit=1) == {"acked": 0, "retry": 1, "conflict": 0}
    row = app.package_outbox.get_by_set_id(identity)
    assert row["status"] == "PENDING", "the confirm is kept, not held for review"
    assert reads == [1]
    assert json.loads(row["command_json"])["payload"]["packing_settings_version"] == 12
    assert PackageCommandDraft.from_dict(json.loads(row["draft_json"])).packing_settings_version == 12
    assert processor.drain(limit=1)["conflict"] == 0
    assert [command["payload"]["packing_settings_version"] for command in sent] == [0, 12]
    assert sent[0]["idempotency_key"] == sent[1]["idempotency_key"]
    assert dialogs == []


def test_other_400_codes_still_go_to_review(central):
    app, _dialogs, case, root, _releases = central
    identity, _master, _values = _central_short_set(app, case, root, 4, 0)

    def build(draft, **kwargs):
        return "TRANSFER-SYNTHETIC", {"idempotency_key": kwargs["idempotency_key"],
                                      "payload": {"sample_barcodes": list(draft.sample_barcodes)}}

    def send(command):
        raise PackageApiError(400, "SHORT_PACKAGE_SAMPLE_MISMATCH_X", "other", retryable=False, committed=False)

    client = SimpleNamespace(build_create_package_command=build, create_package=send)
    processor = PackageOutboxProcessor(app.package_outbox, client, prepare_scanner_delivery=lambda d: d,
                                       read_packing_settings_version=lambda: pytest.fail("no reread"))
    assert processor.drain(limit=1) == {"acked": 0, "retry": 0, "conflict": 1}
    assert app.package_outbox.get_by_set_id(identity)["status"] == "CONFLICT"
