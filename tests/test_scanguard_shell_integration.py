"""Headless checks against the vendored shell and exported app contracts."""
import ast
import hashlib
import importlib
import inspect
import json
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
from types import SimpleNamespace
import unittest


ROOT = Path(__file__).resolve().parents[1]
APP = "ca" if (ROOT / "ca_screen_bridge.py").exists() else "lm"
UI = ROOT / ("ui" if APP == "ca" else "ui/parts")


class ScanGuardShellIntegration(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.module_names = ("host", "bridge", "event_order", "journal", "pedal", "scan_rules", "shell_fileio", "sound")
        cls.saved = {name: sys.modules.pop(name) for name in cls.module_names if name in sys.modules}
        cls.saved_path = sys.path[:]
        sys.path.insert(0, str(ROOT / "shell"))
        cls.host = importlib.import_module("host")
        assert Path(cls.host.__file__).resolve() == ROOT / "shell/host.py"

    @classmethod
    def tearDownClass(cls):
        for name in cls.module_names:
            sys.modules.pop(name, None)
        sys.modules.update(cls.saved)
        sys.path[:] = cls.saved_path

    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory(prefix="scanguard-")
        self.addCleanup(self.temporary.cleanup)
        self.data = Path(self.temporary.name)
        self.sounds = []
        self.calls = []

        def business_call(*args):
            self.calls.append(args)
            raise AssertionError("blocked scan reached the business processor")

        processor = SimpleNamespace(state=lambda: {"count": 3}, scan=business_call,
                                    command=business_call, admin=business_call)
        self.bridge = self.host.Bridge(self.data, "rw", sound=SimpleNamespace(play=self.sounds.append),
                                       owner="external")
        self.bridge.attach_owner(processor)
        self.addCleanup(self.bridge.close)

    def test_blocked_scan_reaches_real_api_without_business_dispatch(self):
        api = self.host.API(self.bridge)
        before = self.bridge.journal.state()
        self.assertEqual(api.scan_blocked({"reason": "sheet", "n_keys": 20}), {"ok": True})
        self.assertEqual(self.sounds, ["error"])
        self.assertEqual(self.calls, [])
        self.assertEqual(self.bridge.journal.state(), before)
        self.assertEqual(self.bridge._processor_state(), {"count": 3})

    def test_field_reason_and_repeated_notification_do_not_double_sound(self):
        api = self.host.API(self.bridge)
        self.assertEqual(api.scan_blocked({"reason": "field", "n_keys": 0}), {"ok": True})
        self.assertEqual(api.scan_blocked({"reason": "field", "n_keys": 6}), {"ok": True})
        self.assertEqual(self.sounds, ["error"])
        self.assertEqual(self.calls, [])

    def test_bad_metadata_never_dispatches_or_plays_sound(self):
        invalid = (None, {}, {"reason": "other", "n_keys": 6}, {"reason": "sheet", "n_keys": -1},
                   {"reason": "sheet", "n_keys": True}, {"reason": "sheet", "n_keys": 1.5},
                   {"reason": "sheet", "n_keys": "6"}, {"reason": "sheet", "n_keys": 6, "text": "scan"})
        for info in invalid:
            with self.subTest(info_type=type(info).__name__):
                self.assertEqual(self.host.API(self.bridge).scan_blocked(info), {"ok": False})
        self.assertEqual(self.calls, [])
        self.assertEqual(self.sounds, [])

    def test_sound_failure_does_not_turn_into_business_input(self):
        def fail(_kind):
            raise OSError("test audio device unavailable")
        self.bridge._sound = SimpleNamespace(play=fail)
        self.assertEqual(self.host.API(self.bridge).scan_blocked({"reason": "sheet", "n_keys": 6}), {"ok": True})
        self.assertEqual(self.calls, [])

    def test_g72_app_names_and_entry_keywords_exist_in_actual_shell(self):
        entry = ROOT / ("ca_screen_bridge.py" if APP == "ca" else "label_match_screen.py")
        tree = ast.parse(entry.read_bytes())
        calls = [node for node in ast.walk(tree) if isinstance(node, ast.Call)
                 and isinstance(node.func, ast.Name) and node.func.id == "run_app"]
        self.assertTrue(calls)
        for call in calls:
            inspect.signature(self.host.run_app).bind(*([None] * len(call.args)),
                                                      **{kw.arg: None for kw in call.keywords})
        inspect.signature(self.host.web_allowed).bind(None, None, write_guard=None)
        if APP == "ca":
            for name in ("threading", "Bridge", "NativeHost", "ClosingWait"):
                self.assertTrue(hasattr(self.host, name), name)
        else:
            fileio = importlib.import_module("shell_fileio")
            inspect.signature(fileio.set_write_guard).bind(None)
            inspect.signature(fileio.FileIO).bind(None)
            inspect.signature(fileio.FileIO.replace_atomic).bind(None, None, None, None, fsync=True)
            inspect.signature(fileio.FileIO.make_dirs).bind(None, None, None)
        self.assertTrue(callable(self.host.API.scan_blocked))
        self.assertTrue(callable(self.host.Bridge.scan_blocked))

    def test_export_contract_call_has_a_real_shell_endpoint(self):
        contract = json.loads((UI / "app" / APP / "contract.json").read_bytes())
        self.assertTrue(callable(getattr(self.host.API, "scan_blocked")))
        schema = contract["calls"]["scan_blocked"]["args"][0]
        self.assertEqual(set(schema["required"]), {"reason", "n_keys"})
        self.assertEqual(set(schema["properties"]["reason"]["enum"]), {"sheet", "field"})
        self.assertFalse(schema["additionalProperties"])

    def test_exported_scan_sink_in_upstream_headless_node_vm(self):
        node = shutil.which("node")
        if node is None:
            try:
                from playwright._impl._driver import compute_driver_executable
                node = str(compute_driver_executable()[0])
            except ImportError:
                self.skipTest("Node and Playwright driver Node unavailable")
        result = subprocess.run([node, str(ROOT / "tests/check_scanguard.mjs"), str(UI / "app/scan.js")],
                                capture_output=True, text=True, encoding="utf-8", timeout=60,
                                creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0))
        self.assertEqual(result.returncode, 0, "upstream editable-scan VM did not pass")
        checks = json.loads(result.stdout)
        self.assertTrue(checks)
        self.assertTrue(all(value is True for value in checks.values()), "editable-scan VM check failed")

    def test_export_and_shell_manifest_payloads_match_on_disk(self):
        manifest = json.loads((UI / "MANIFEST.json").read_bytes())
        self.assertEqual(manifest["app"], APP)
        self.assertEqual(manifest["commit"], "95abf3045beaed6360b872a1ec054ec0ab39704f")
        self.assertTrue(manifest["dirty"])
        for row in manifest["files"]:
            raw = (UI / row["path"]).read_bytes()
            self.assertEqual(len(raw), row["bytes"], row["path"])
            self.assertEqual(hashlib.sha256(raw).hexdigest(), row["sha256"], row["path"])
        ready_name = "SHELL-READY-v3.16.json" if APP == "ca" else "SHELL-READY.json"
        ready = json.loads((ROOT / "shell" / ready_name).read_bytes())
        self.assertEqual(ready["version"], "v3.16.3.1")
        for row in ready["files"]:
            rel = Path(row["path"])
            self.assertEqual(rel.parts[0], "shell-v3.16.3.1")
            raw = (ROOT / "shell" / Path(*rel.parts[1:])).read_bytes()
            self.assertEqual(len(raw), row["bytes"], row["path"])
            self.assertEqual(hashlib.sha256(raw).hexdigest(), row["sha256"], row["path"])


if __name__ == "__main__":
    unittest.main()
