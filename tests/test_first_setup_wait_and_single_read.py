"""G100b packaging: first setup relay waits share one value; trees are read once per process."""
import re
import time
from pathlib import Path
from types import SimpleNamespace

import pytest

import auto_update
import current_user_onboarding as onboarding

ROOT = Path(__file__).resolve().parents[1]
INSTALLER = ROOT / "INSTALL_CANONICAL_PORTABLE.ps1"


def test_bootstrap_waits_for_relay_ready_at_76_seconds(monkeypatch, tmp_path):
    """A relay that becomes ready 76 s after launch must not be missed."""
    clock = {"now": 1000.0}
    monkeypatch.setattr(auto_update.time, "monotonic", lambda: clock["now"])
    monkeypatch.setattr(auto_update.time, "sleep", lambda s: clock.__setitem__("now", clock["now"] + s))
    seen = {}
    monkeypatch.setattr(auto_update, "support_record", lambda *a, **k: {})
    monkeypatch.setattr(auto_update, "write_json_durable", lambda *a, **k: None)
    monkeypatch.setattr(auto_update, "assert_ordinary_recovery", lambda *a, **k: None)
    monkeypatch.setattr(auto_update, "emit_support_record", lambda *a, **k: None)
    monkeypatch.setattr(auto_update, "_launch_bootstrap_version",
                        lambda control, sha, arguments, *, deadline, **k: seen.setdefault("deadline", deadline) and 4321)
    ready_at = 1000.0 + 76

    def wait(control, policy, sha, *, started_at, deadline, pid=None):
        while clock["now"] < deadline:
            if clock["now"] >= ready_at:
                return True
            clock["now"] += 1
        return False
    monkeypatch.setattr(auto_update, "wait_current_relay", wait)
    result = auto_update.start_current(tmp_path, {"staging_root": str(tmp_path)}, None, {"sha": "a" * 40},
                                       ["--label-match-user-relay"], code="X")
    assert result == 0


def test_every_relay_wait_uses_the_one_value():
    text = (ROOT / "auto_update.py").read_text(encoding="utf-8")
    assert auto_update.RELAY_PREPARE_SECONDS == 600
    assert not re.search(r"monotonic\(\)\s*\+\s*75\b", text)
    assert text.count("+ RELAY_PREPARE_SECONDS") >= 3


def test_installer_and_user_relay_use_the_same_value():
    import user_relay
    value = auto_update.RELAY_PREPARE_SECONDS
    ps = INSTALLER.read_text(encoding="utf-8")
    assert f"AddSeconds({value})" in ps and f"{value}-([DateTime]::UtcNow-$relayStartedAt)" in ps
    import inspect
    sig = inspect.signature(user_relay.start_user_relay) if hasattr(user_relay, "start_user_relay") else None
    src = Path(user_relay.__file__).read_text(encoding="utf-8")
    assert f"bootstrap_readiness_seconds: float = {value}.0" in src
    assert f"min({value}.0," in src


def _tree(root):
    (root / "app").mkdir(parents=True)
    (root / "app" / "a.py").write_bytes(b"x=1\n")
    (root / "app" / "b.py").write_bytes(b"y=2\n")
    rec = root / "bootstrap-integrity.json"
    rec.write_text("{}")
    return SimpleNamespace(app_root=root / "app", bootstrap_integrity_path=rec)


def test_second_integrity_check_does_not_reread_the_tree(monkeypatch, tmp_path):
    paths = _tree(tmp_path)
    calls = []
    monkeypatch.setattr(onboarding, "_verify_bootstrap_integrity_uncached",
                        lambda p, *, required: calls.append(1) or {"status": "PASS"})
    reads = []
    original = Path.read_bytes
    monkeypatch.setattr(Path, "read_bytes", lambda self: reads.append(self) or original(self))
    onboarding._BOOTSTRAP_INTEGRITY_PASS.clear()
    assert onboarding.verify_bootstrap_integrity(paths, required=True)["status"] == "PASS"
    first = len(reads)
    assert first >= 2
    for _ in range(3):
        assert onboarding.verify_bootstrap_integrity(paths, required=True)["status"] == "PASS"
    assert len(reads) == first and len(calls) == 1


def test_changed_file_after_pass_is_still_rejected_or_reverified(monkeypatch, tmp_path):
    paths = _tree(tmp_path)
    results = [{"status": "PASS"}, {"status": "FAIL"}]
    monkeypatch.setattr(onboarding, "_verify_bootstrap_integrity_uncached",
                        lambda p, *, required: results.pop(0))
    onboarding._BOOTSTRAP_INTEGRITY_PASS.clear()
    assert onboarding.verify_bootstrap_integrity(paths, required=True)["status"] == "PASS"
    (paths.app_root / "a.py").write_bytes(b"x=99\n")
    assert onboarding.verify_bootstrap_integrity(paths, required=True)["status"] == "FAIL"


def test_second_writer_inventory_does_not_reread_the_tree(monkeypatch, tmp_path):
    import writer_sink_inventory
    bundle = tmp_path / "b"
    (bundle / "app").mkdir(parents=True)
    (bundle / "app" / "m.py").write_bytes(b"z=1\n")
    (bundle / "INSTALL_THIS_PC.ps1").write_bytes(b"#\n")
    monkeypatch.setattr(writer_sink_inventory, "_runtime_files",
                        lambda root, suffix: sorted(root.glob("*" + suffix)))
    monkeypatch.setattr(auto_update, "_version_writer_inventory_sha256", lambda b: "h" * 64)
    auto_update._VERSION_WRITER_INVENTORY_PASS.clear()
    reads = []
    original = Path.read_bytes
    monkeypatch.setattr(Path, "read_bytes", lambda self: reads.append(self) or original(self))
    assert auto_update.version_writer_inventory_sha256(bundle) == "h" * 64
    first = len(reads)
    assert auto_update.version_writer_inventory_sha256(bundle) == "h" * 64
    assert len(reads) == first
    (bundle / "app" / "m.py").write_bytes(b"z=2\n")
    auto_update.version_writer_inventory_sha256(bundle)
    assert len(reads) > first
