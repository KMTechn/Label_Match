from __future__ import annotations

import importlib.util
from pathlib import Path
import sys


MODULE_PATH = Path(__file__).resolve().parents[1] / "Label_Match.py"
SPEC = importlib.util.spec_from_file_location("label_match_release_bootstrap_for_tests", MODULE_PATH)
assert SPEC and SPEC.loader
module = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = module
SPEC.loader.exec_module(module)


def test_retired_install_helper_is_unreachable_even_when_legacy_files_exist(tmp_path):
    tools = tmp_path / "tools"
    nested = tools / "direct_sync_relay_install_pack/direct_sync_relay_install_pack.exe"
    legacy = tools / "direct_sync_relay_install_pack.exe"
    nested.parent.mkdir(parents=True)
    nested.write_bytes(b"nested")
    legacy.write_bytes(b"legacy")
    script = tools / "direct_sync_relay_install_pack.py"
    script.write_text("# fixture\n", encoding="utf-8")
    context = {"app_root": str(tmp_path)}

    assert module._label_match_direct_sync_tool_command(context) == []

    nested.unlink()
    assert module._label_match_direct_sync_tool_command(context) == []


def test_retired_install_helper_resolver_always_returns_empty(tmp_path):
    tools = tmp_path / "tools"
    tools.mkdir()
    script = tools / "direct_sync_relay_install_pack.py"
    script.write_text("# fixture\n", encoding="utf-8")
    context = {"app_root": str(tmp_path)}
    command = module._label_match_direct_sync_tool_command(context)

    assert command == []
    script.unlink()
    assert module._label_match_direct_sync_tool_command(context) == []
