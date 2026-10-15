"""Coordinator follow-up: repeated label confirms a tray with <4 samples.

설계 초안 값 — 바뀔 수 있음: duplicate read window about 3 seconds;
KMCMD:UNDO reverses the shortage confirmation before the next label.
The clock advances explicitly; no sleeping or production timeout changes.
"""

import copy
from types import SimpleNamespace

import pytest

import Label_Match as label_module
from tests.test_legacy_label_transition import MASTER
from tests.test_w9scanonly_packaging import completions, packaging, products, scan  # noqa: F401


@pytest.fixture
def scanner_clock(monkeypatch):
    instant = [100.0]
    real_time = label_module.time
    clock = SimpleNamespace(**{name: getattr(real_time, name) for name in dir(real_time)})
    clock.monotonic = lambda: instant[0]
    clock.time = lambda: 1_800_000_000 + instant[0]
    clock.time_ns = lambda: int(clock.time() * 1_000_000_000)
    # Replace only the application's time namespace, preserving the real
    # clocks used by event-writer threads and other pytest modules.
    monkeypatch.setattr(label_module, "time", clock)
    return instant


def test_rescanning_a_label_with_zero_products_cannot_confirm_a_short_tray(packaging, scanner_clock):
    app, dialogs = packaging
    scan(app, MASTER)
    before = copy.deepcopy(app.current_set_info)
    scanner_clock[0] += 5
    scan(app, MASTER)
    assert completions(app) == [], "zero-product tray must never be shortage-confirmed"
    assert app.current_set_info["raw"] == [MASTER]
    scanner_clock[0] += 5
    scan(app, products()[0])
    assert app.current_set_info["raw"] == [MASTER, products()[0]], (
        "ignored label repeat must leave the original tray ready for its first product"
    )
    assert app.current_set_info["id"] == before["id"]
    assert dialogs == []


def test_same_label_read_twice_within_three_seconds_is_ignored(packaging, scanner_clock):
    app, dialogs = packaging
    scan(app, MASTER)
    scanner_clock[0] += 1
    scan(app, MASTER)
    assert completions(app) == []
    scanner_clock[0] += 1
    scan(app, products()[0])
    assert app.current_set_info["raw"] == [MASTER, products()[0]], (
        "duplicate scanner read must not install an error-acknowledgment gate"
    )
    assert app.sounds, "duplicate read must give a short audible response"
    assert dialogs == []


def test_shortage_confirmation_has_distinct_sound_and_large_missing_count(packaging, scanner_clock):
    app, dialogs = packaging
    # Obtain the normal completion sound from the real five-scan path.
    for value in [MASTER, *products(prefix="FULL")]:
        scan(app, value)
    assert len(completions(app)) == 1
    assert app.sounds
    normal_completion_sound = app.sounds[-1]
    app.sounds.clear()
    display = []
    app.update_big_display = lambda text, *args, **kwargs: display.append(str(text))
    values = products(prefix="SHORT")
    for value in [MASTER, *values[:2]]:
        scanner_clock[0] += 5
        scan(app, value)
    scanner_clock[0] += 5
    scan(app, MASTER)
    assert app._partial_submit_confirm_state() is not None
    scan(app, MASTER)
    rows = completions(app)
    assert len(rows) == 2 and rows[-1][1]["final_result"] == app.Results.PASS, (
        "repeating the same label after two products must durably confirm the short tray"
    )
    assert rows[-1][1]["scanned_product_barcodes"] == [MASTER, *values[:2]]
    assert any(text.startswith("부분 제출 완료 —") for text in display)
    assert app.sounds and app.sounds[-1] != normal_completion_sound, (
        "shortage confirmation must sound different from ordinary completion"
    )
    assert dialogs == []


def test_duplicate_label_after_shortage_confirmation_does_not_start_a_new_tray(packaging, scanner_clock):
    app, dialogs = packaging
    scan(app, MASTER)
    scanner_clock[0] += 5
    scan(app, products()[0])
    scanner_clock[0] += 5
    scan(app, MASTER)
    assert app._partial_submit_confirm_state() is not None
    scan(app, MASTER)
    rows = completions(app)
    assert len(rows) == 1 and rows[0][1]["final_result"] == app.Results.PASS, (
        "shortage confirmation is a prerequisite for checking the duplicate read"
    )
    scanner_clock[0] += 1
    scan(app, MASTER)
    assert app.current_set_info["raw"] == [], "one-second duplicate must not start another tray"
    assert completions(app) == rows, "duplicate read must not record another completion"
    assert dialogs == []


def test_undo_during_confirmation_is_a_normal_product_undo(packaging, scanner_clock):
    app, dialogs = packaging
    values = products()
    for value in [MASTER, *values[:2]]:
        scanner_clock[0] += 3.1
        scan(app, value)
    before = copy.deepcopy(app.current_set_info)
    scan(app, MASTER)
    assert app._partial_submit_confirm_state() is not None
    scan(app, "KMCMD:UNDO")
    assert app._partial_submit_confirm_state() is None
    assert app.current_set_info["raw"] == before["raw"][:-1]
    assert app.current_set_info["parsed"] == before["parsed"][:-1]
    for value in values[1:]:
        scanner_clock[0] += 3.1
        scan(app, value)
    rows = completions(app)
    assert len(rows) == 1 and rows[0][1]["final_result"] == app.Results.PASS
    assert rows[0][1]["scanned_product_barcodes"] == [MASTER, *values]
    assert dialogs == []
