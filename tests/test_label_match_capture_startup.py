import importlib.util
from pathlib import Path

import pytest


def load_label_match_module(monkeypatch, tmp_path):
    for name, directory in (
        ("ProgramData", tmp_path / "programdata"),
        ("LOCALAPPDATA", tmp_path / "localappdata"),
        ("TEMP", tmp_path / "temp"),
    ):
        directory.mkdir(parents=True, exist_ok=True)
        monkeypatch.setenv(name, str(directory))
    module_path = Path(__file__).resolve().parents[1] / "Label_Match.py"
    spec = importlib.util.spec_from_file_location(
        "label_match_capture_startup_for_tests",
        module_path,
    )
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_capture_startup_geometry_is_disabled_for_normal_runtime(monkeypatch, tmp_path):
    module = load_label_match_module(monkeypatch, tmp_path)
    monkeypatch.setenv(
        module.LABEL_MATCH_CAPTURE_STARTUP_GEOMETRY_ENV,
        "1366x768+693-1440",
    )
    monkeypatch.setenv(module.LABEL_MATCH_CAPTURE_STARTUP_DPI_ENV, "96")
    monkeypatch.delenv(module.LABEL_MATCH_AUTOMATED_TEST_ENV, raising=False)

    assert module._label_match_capture_startup_request() is None


def test_capture_startup_geometry_accepts_and_canonicalizes_virtual_desktop_coordinates(
    monkeypatch,
    tmp_path,
):
    module = load_label_match_module(monkeypatch, tmp_path)
    monkeypatch.setenv(
        module.LABEL_MATCH_CAPTURE_STARTUP_GEOMETRY_ENV,
        "2560x1392+693-1440",
    )
    monkeypatch.setenv(module.LABEL_MATCH_CAPTURE_STARTUP_DPI_ENV, "96")
    monkeypatch.setenv(module.LABEL_MATCH_AUTOMATED_TEST_ENV, "1")

    assert module._label_match_capture_startup_request() == {
        "geometry": "2560x1392+693-1440",
        "target_dpi": 96,
    }


@pytest.mark.parametrize(
    "value",
    (
        "",
        "1366x768",
        "1366X768+0+0",
        "639x768+0+0",
        "1366x479+0+0",
        "32769x768+0+0",
        "1366x768+131073+0",
        "1366x768+0-131073",
        "1366x768+-1+0",
    ),
)
def test_capture_startup_geometry_rejects_ambiguous_or_unsafe_values(
    monkeypatch,
    value,
    tmp_path,
):
    module = load_label_match_module(monkeypatch, tmp_path)
    monkeypatch.setenv(module.LABEL_MATCH_CAPTURE_STARTUP_GEOMETRY_ENV, value)
    monkeypatch.setenv(module.LABEL_MATCH_CAPTURE_STARTUP_DPI_ENV, "96")
    monkeypatch.setenv(module.LABEL_MATCH_AUTOMATED_TEST_ENV, "1")

    if not value:
        with pytest.raises(RuntimeError):
            module._label_match_capture_startup_request()
    else:
        with pytest.raises(RuntimeError):
            module._label_match_capture_startup_request()


@pytest.mark.parametrize("dpi", ("", "x96", "71", "961", "９６"))
def test_capture_startup_request_rejects_missing_or_invalid_dpi(
    monkeypatch,
    dpi,
    tmp_path,
):
    module = load_label_match_module(monkeypatch, tmp_path)
    monkeypatch.setenv(
        module.LABEL_MATCH_CAPTURE_STARTUP_GEOMETRY_ENV,
        "1366x768+693-1440",
    )
    monkeypatch.setenv(module.LABEL_MATCH_CAPTURE_STARTUP_DPI_ENV, dpi)
    monkeypatch.setenv(module.LABEL_MATCH_AUTOMATED_TEST_ENV, "1")

    with pytest.raises(RuntimeError):
        module._label_match_capture_startup_request()


def test_capture_root_stays_hidden_until_final_geometry_is_reapplied(
    monkeypatch,
    tmp_path,
):
    module = load_label_match_module(monkeypatch, tmp_path)
    events = []

    class FakeTk:
        def __init__(self):
            self.scaling = 2.0

        def call(self, *args):
            if args[:2] != ("tk", "scaling"):
                raise AssertionError(f"unexpected Tk call: {args}")
            if len(args) == 3:
                self.scaling = float(args[2])
                events.append(("tk-scaling", self.scaling))
            return self.scaling

    class FakeRoot:
        def __init__(self):
            self.tk = FakeTk()

        def withdraw(self):
            events.append("withdraw")

        def geometry(self, value):
            events.append(("geometry", value))

        def update_idletasks(self):
            events.append("idletasks")

        def deiconify(self):
            events.append("deiconify")

        def winfo_fpixels(self, value):
            assert value == "1i"
            return 96.0

        def winfo_id(self):
            return 4242

    root = FakeRoot()
    geometry = "1366x768+693-1440"

    placement = module._label_match_prepare_capture_startup(
        root,
        geometry,
        96,
        initial_window_dpi_getter=lambda current_root: 96,
        native_window_placer=lambda current_root, current_geometry, current_dpi: {
            "wrapper_hwnd": 5151,
            "native_rect": [693, -1440, 2059, -672],
            "window_dpi": current_dpi,
            "visible": False,
        },
    )
    assert placement == {
        "geometry": geometry,
        "tk_geometry": geometry,
        "initial_window_dpi": 96,
        "target_dpi": 96,
        "expected_tk_scaling": pytest.approx(96 / 72),
        "observed_tk_scaling": pytest.approx(96 / 72),
        "pixels_per_inch": 96.0,
        "native_placement": {
            "wrapper_hwnd": 5151,
            "native_rect": [693, -1440, 2059, -672],
            "window_dpi": 96,
            "visible": False,
        },
    }
    assert "deiconify" not in events
    assert events == [
        "withdraw",
        ("geometry", geometry),
        "idletasks",
        ("geometry", geometry),
        ("tk-scaling", pytest.approx(96 / 72)),
        "idletasks",
    ]

    assert module._label_match_reveal_capture_startup(
        root,
        geometry,
        96,
        window_dpi_getter=lambda hwnd: 96 if hwnd == 5151 else 0,
        native_window_placer=lambda current_root, current_geometry, current_dpi: {
            "wrapper_hwnd": 5151,
            "native_rect": [693, -1440, 2059, -672],
            "window_dpi": current_dpi,
            "visible": False,
        },
    ) == {
        "window_dpi": 96,
        "native_placement_before_reveal": {
            "wrapper_hwnd": 5151,
            "native_rect": [693, -1440, 2059, -672],
            "window_dpi": 96,
            "visible": False,
        },
    }
    assert events[-4:] == [
        "idletasks",
        "idletasks",
        "deiconify",
        "idletasks",
    ]
    assert events.index("deiconify") > max(
        index for index, event in enumerate(events) if event == ("geometry", geometry)
    )
    with pytest.raises(RuntimeError, match="already revealed"):
        module._label_match_reveal_capture_startup(root, geometry, 96)


def test_capture_root_helpers_are_noops_without_capture_geometry(monkeypatch, tmp_path):
    module = load_label_match_module(monkeypatch, tmp_path)

    class RejectUnexpectedCall:
        def __getattr__(self, name):
            raise AssertionError(f"unexpected root call: {name}")

    root = RejectUnexpectedCall()
    assert module._label_match_prepare_capture_startup(root, None, None) is None
    assert module._label_match_reveal_capture_startup(root, None) is False


def test_capture_window_dpi_is_attested_only_after_the_root_is_revealed(
    monkeypatch,
    tmp_path,
):
    module = load_label_match_module(monkeypatch, tmp_path)
    events = []

    class FakeRoot:
        _label_match_capture_revealed = False

        def update_idletasks(self):
            events.append("idletasks")

        def geometry(self, value):
            events.append(("geometry", value))

        def deiconify(self):
            events.append("deiconify")

        def winfo_id(self):
            events.append("winfo_id")
            return 4242

    def get_window_dpi(hwnd):
        assert hwnd == 5151
        events.append("get_window_dpi")
        assert "deiconify" in events
        return 96

    receipt = module._label_match_reveal_capture_startup(
        FakeRoot(),
        "1600x900+1173-1194",
        96,
        window_dpi_getter=get_window_dpi,
        native_window_placer=lambda current_root, current_geometry, current_dpi: {
            "wrapper_hwnd": 5151,
            "native_rect": [1173, -1194, 2773, -294],
            "window_dpi": current_dpi,
            "visible": False,
        },
    )

    assert receipt["window_dpi"] == 96
    assert events.index("deiconify") < events.index("get_window_dpi")


def test_capture_reveal_rejects_post_map_dpi_mismatch(monkeypatch, tmp_path):
    module = load_label_match_module(monkeypatch, tmp_path)

    class FakeRoot:
        _label_match_capture_revealed = False

        def update_idletasks(self):
            pass

        def geometry(self, value):
            pass

        def deiconify(self):
            pass

        def winfo_id(self):
            return 4242

    with pytest.raises(RuntimeError, match="after reveal"):
        module._label_match_reveal_capture_startup(
            FakeRoot(),
            "1600x900+1173-1194",
            96,
            window_dpi_getter=lambda hwnd: 144,
            native_window_placer=lambda current_root, current_geometry, current_dpi: {
                "wrapper_hwnd": 5151,
                "native_rect": [1173, -1194, 2773, -294],
                "window_dpi": current_dpi,
                "visible": False,
            },
        )


def test_capture_prepare_rejects_a_native_placer_that_reveals_the_root(
    monkeypatch,
    tmp_path,
):
    module = load_label_match_module(monkeypatch, tmp_path)

    class FakeTk:
        scaling = 96 / 72

        def call(self, *args):
            if len(args) == 3:
                self.scaling = float(args[2])
            return self.scaling

    class FakeRoot:
        tk = FakeTk()

        def withdraw(self):
            pass

        def geometry(self, value):
            pass

        def update_idletasks(self):
            pass

        def winfo_fpixels(self, value):
            return 96.0

    with pytest.raises(RuntimeError, match="visible window"):
        module._label_match_prepare_capture_startup(
            FakeRoot(),
            "1600x900+1173-1194",
            96,
            initial_window_dpi_getter=lambda current_root: 96,
            native_window_placer=lambda root, geometry, dpi: {
                "window_dpi": dpi,
                "visible": True,
            },
        )


def test_native_capture_placement_never_requests_window_visibility():
    source = (
        Path(__file__).resolve().parents[1] / "Label_Match.py"
    ).read_text(encoding="utf-8")
    start = source.index("def _label_match_place_hidden_native_window(")
    end = source.index("\ndef _label_match_prepare_capture_startup(", start)
    helper = source[start:end]

    assert "SetWindowPos" in helper
    assert "SWP_SHOWWINDOW" not in helper
    assert "ShowWindow" not in helper


def test_tk_geometry_compensates_for_the_initial_monitor_dpi(monkeypatch, tmp_path):
    module = load_label_match_module(monkeypatch, tmp_path)

    assert module._label_match_scaled_tk_geometry(
        "1600x900+1173-1194",
        144,
        96,
    ) == "2400x1350+1760-1791"
    assert module._label_match_scaled_tk_geometry(
        "1600x900+1173-1194",
        96,
        96,
    ) == "1600x900+1173-1194"


def test_constructor_orders_capture_prepare_before_widgets_and_reveal_after_bindings():
    source = (
        Path(__file__).resolve().parents[1] / "Label_Match.py"
    ).read_text(encoding="utf-8")
    constructor_start = source.index("    def __init__(self, run_tests=False):")
    constructor_end = source.index("\n    def _start_package_outbox_drain", constructor_start)
    constructor = source[constructor_start:constructor_end]

    assert constructor.index("_label_match_prepare_capture_startup(") < constructor.index(
        "self._create_widgets()"
    )
    assert constructor.index("self._create_widgets()") < constructor.index(
        "self.show_loading_overlay()"
    )
    assert constructor.index("self.protocol(\"WM_DELETE_WINDOW\"") < constructor.index(
        "_label_match_reveal_capture_startup("
    )
    assert constructor.count("_label_match_reveal_capture_startup(") == 1
    assert constructor.count("self.state('zoomed')") == 1


def test_automated_capture_main_exception_exits_without_packaged_dialog():
    source = (
        Path(__file__).resolve().parents[1] / "Label_Match.py"
    ).read_text(encoding="utf-8")
    main_block = source[source.index('if __name__ == "__main__":') :]

    assert "if _label_match_explicit_automated_test_mode():" in main_block
    assert "raise SystemExit(1) from None" in main_block
