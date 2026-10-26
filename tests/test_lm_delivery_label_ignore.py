"""납품처 라벨(ISO/IEC 15434 [)> RS ... GS ...) 은 새 방식 세트에서 무시된다.

잠정 글(w9mgrui 확정): 제목 「납품처 라벨입니다」 · 줄 「찍지 않아도 됩니다 — 제품을 찍으세요」.
세트 그대로 · duplicate 소리 한 번 · 실패음 0 · 사이렌 0 · 제품 셈 0. 옛 방식 +1 자리는 그대로 받는다.
"""
import copy
import json
from datetime import datetime
from pathlib import Path

import pytest

import Label_Match as label_module
from tests.test_deferred_lease_clock_retry import clock_case  # noqa: F401
from tests.test_legacy_label_transition import MASTER
from tests.test_w9scanonly_central import central  # noqa: F401
from tests.test_w9scanonly_packaging import completions, packaging, products, scan  # noqa: F401

TITLE = "납품처 라벨입니다"
LINE = "찍지 않아도 됩니다 — 제품을 찍으세요"
RS, GS = "\x1e", "\x1d"


def delivery_label(item=None, rs=True):
    day = datetime.now().strftime("%Y%m%d")
    fields = ["[)>" + (RS if rs else "") + "06", "P" + (item or "NOITEM"), "1T" + "L" * 40, "6D" + day, "Q" + "9" * 120]
    if not rs:  # shape recorded from the field scans: GS kept, RS and EOT absent
        return GS.join(fields)
    return GS.join(fields) + RS + "\x04"


def members(case):
    return [row["normalized_barcode"] for row in case.snapshot["work_group_source"]["members"]]


def arm(app, monkeypatch):
    sirens = []
    monkeypatch.setattr(label_module, "_label_match_automated_test_mode", lambda: False)
    app.sound_objects = {"fail": object(), "duplicate": object()}
    app._start_error_siren_thread = lambda: sirens.append("siren")
    return sirens


def assert_ignored(app, sirens, raw_before, completed_before):
    assert app.current_set_info["raw"] == raw_before, "set must stay as it was"
    assert len(completions(app)) == completed_before, "no failed or extra set row"
    assert sirens == [], "no siren"
    assert "fail" not in app.sounds, "no failure sound"
    assert app.sounds.count("duplicate") == 1, "duplicate cue exactly once"
    notice = app.__dict__.get("_phs_label_guidance_notice")
    assert notice is not None and notice.title == TITLE and notice.message == LINE
    assert notice.tone == "warning" and notice.kind == "scanner_notice"


def test_detector_matches_header_only():
    f = label_module._label_match_is_delivery_label_scan
    value = delivery_label()
    assert f(value)
    assert f(value.strip())
    assert f(delivery_label(rs=False)), "field shape: [)> then 06 then GS, no RS"
    assert f("[)><RS>06" + GS + "P1") and f("[)><rs>06" + GS + "P1")
    assert not f("[)>X06"), "header needs RS, <RS> or two digits"
    assert not f("[)>6"), "one digit is not a format number"
    assert not f("PHS=2|CLC=AAA2270730100|SPC=P|QT=1"), "product QR is never a delivery label"
    assert not f(MASTER) and not f("") and not f(None)
    assert not f("X" + value)


@pytest.mark.parametrize("with_item", [False, True], ids=["no-item-code", "item-code-no-members"])
def test_new_set_product_slot_ignores_delivery_label(central, monkeypatch, with_item):
    app, dialogs, case, root, _releases = central
    master = case.group["scan_payload"]
    item = case.group["item_id"]
    values = members(case)
    scan(app, master)
    root.run_until(lambda: not app.__dict__.get("_phs2_background_pending"), timeout=5)
    scan(app, values[0])
    if with_item:
        app.current_set_info["package_source_snapshot"] = {"work_group_source": {}}  # no member list
    before = list(app.current_set_info["raw"])
    sirens = arm(app, monkeypatch)
    app.sounds = []
    scan(app, delivery_label(item if with_item else None))
    assert_ignored(app, sirens, before, 0)
    assert app.current_set_info.get("final_result") is None
    scan(app, values[1])
    assert app.current_set_info["raw"] == before + [values[1]]


def test_new_set_final_slot_ignores_delivery_label(central, monkeypatch):
    app, dialogs, case, root, _releases = central
    master = case.group["scan_payload"]
    values = members(case)
    scan(app, master)
    root.run_until(lambda: not app.__dict__.get("_phs2_background_pending"), timeout=5)
    for value in values[:3]:
        scan(app, value)
    before = list(app.current_set_info["raw"])
    sirens = arm(app, monkeypatch)
    app.sounds = []
    scan(app, delivery_label(case.group["item_id"]))
    assert_ignored(app, sirens, before, 0)
    scan(app, values[3])
    assert len(completions(app)) == 1


def test_first_scan_right_after_a_finished_new_set_is_ignored(central, monkeypatch):
    app, dialogs, case, root, _releases = central
    master = case.group["scan_payload"]
    values = members(case)
    scan(app, master)
    root.run_until(lambda: not app.__dict__.get("_phs2_background_pending"), timeout=5)
    for value in values:
        scan(app, value)
    assert len(completions(app)) == 1 and app.current_set_info["raw"] == []
    sirens = arm(app, monkeypatch)
    app.sounds = []
    scan(app, delivery_label(case.group["item_id"]))
    assert_ignored(app, sirens, [], 1)
    assert not app.__dict__.get("_pending_workflow_error")


def test_first_scan_on_an_empty_screen_is_ignored(packaging, monkeypatch):
    app, dialogs = packaging
    sirens = arm(app, monkeypatch)
    app.sounds = []
    scan(app, delivery_label())
    assert_ignored(app, sirens, [], 0)
    scan(app, MASTER)
    assert app.current_set_info["raw"] == [MASTER], "a normal label still starts a set"


def test_first_scan_of_the_field_shape_without_rs_is_ignored(packaging, monkeypatch):
    app, dialogs = packaging
    sirens = arm(app, monkeypatch)
    app.sounds = []
    scan(app, delivery_label(rs=False))
    assert_ignored(app, sirens, [], 0)


def test_old_method_plus_one_slot_still_takes_the_delivery_label(packaging, monkeypatch):
    app, dialogs = packaging
    values = products()
    final = delivery_label(MASTER)
    for value in [MASTER, *values[:3]]:
        scan(app, value)
    sirens = arm(app, monkeypatch)
    app.sounds = []
    scan(app, final)
    rows = completions(app)
    assert len(rows) == 1 and rows[0][1]["final_result"] == app.Results.PASS
    assert rows[0][1]["scanned_product_barcodes"][-1] == final
    assert sirens == [] and "duplicate" not in app.sounds and "fail" not in app.sounds


def new_method_state(app, snapshot):
    scan(app, MASTER)
    app.current_set_info.update(
        raw=["PHS=2|SRC=KMTECH_INPUT_TAG|ITG=DELIVERY-TEST|CLC=" + MASTER
             + "|LBL=DELIVERY-TEST|HSH=aaaaaaaaaaaaaaaa"],
        scanner_only=True, package_source_snapshot=snapshot,
    )


@pytest.mark.parametrize("header", ["control", "text", "field"])
@pytest.mark.parametrize("exchanged", [False, True], ids=["source-member", "exchanged-member"])
def test_exact_member_precedes_delivery_header(packaging, monkeypatch, tmp_path, header, exchanged):
    app, _dialogs = packaging
    value = delivery_label(MASTER, rs=header != "field")
    if header == "text":
        value = value.replace(RS, "<RS>").replace(GS, "<GS>")
    old = products()[0]
    snapshot = {"work_group_source": {"members": [{"normalized_barcode": old if exchanged else value}]}}
    new_method_state(app, snapshot)
    if exchanged:
        app.current_set_info["scanner_exchanges"] = [{
            "target_barcode": old, "replacement_barcode": value,
        }]
    arm(app, monkeypatch)
    app.sounds = []
    scan(app, value)
    assert app.current_set_info["raw"][1:] == [value]
    assert "duplicate" not in app.sounds and "fail" not in app.sounds
    assert app._save_current_set_state()
    saved = json.loads((tmp_path / app.FILES.CURRENT_STATE).read_text(encoding="utf-8"))
    assert saved["current_set_info"]["raw"][1:] == [value]


@pytest.mark.parametrize("snapshot", [None, {"work_group_source": {}}], ids=["snapshot-none", "members-absent"])
def test_unconfirmed_iso_value_still_counts_as_delivery(packaging, monkeypatch, snapshot):
    app, _dialogs = packaging
    new_method_state(app, snapshot)
    sirens = arm(app, monkeypatch)
    before = copy.deepcopy(app.current_set_info)
    app.sounds = []
    scan(app, delivery_label(MASTER, rs=False))
    assert_ignored(app, sirens, before["raw"], 0)
    assert app.current_set_info == before


@pytest.mark.parametrize("header", ["control", "text", "field"])
@pytest.mark.parametrize("sampled_target", [True, False], ids=["replace-sample", "append-sample"])
def test_exchange_delivery_preserves_checkpoint(packaging, monkeypatch, tmp_path, header, sampled_target):
    app, _dialogs = packaging
    target = products()[0]
    new_method_state(app, {"work_group_source": {"members": [{"normalized_barcode": target}]}})
    if sampled_target:
        scan(app, target)
    scan(app, "KMCMD:EXCHANGE")
    scan(app, target)
    assert app.current_set_info["scanner_exchange_phase"] == "replacement"
    before = copy.deepcopy(app.current_set_info)
    path = tmp_path / app.FILES.CURRENT_STATE
    checkpoint = path.read_bytes()
    sirens = arm(app, monkeypatch)
    app.sounds = []
    value = delivery_label(MASTER, rs=header != "field")
    if header == "text":
        value = value.replace(RS, "<RS>").replace(GS, "<GS>")
    scan(app, value)
    assert_ignored(app, sirens, before["raw"], 0)
    assert app.current_set_info == before
    assert path.read_bytes() == checkpoint
