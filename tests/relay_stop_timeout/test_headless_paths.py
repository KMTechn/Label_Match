"""The long waits are reachable through headless lifecycle/SYSTEM modes."""
import ast
from contextlib import nullcontext
from pathlib import Path
import sys
from types import ModuleType

import pytest

ROOT = Path(__file__).resolve().parents[2]
IS_CA = (ROOT / 'container_audit_product_host.py').is_file()


def test_gui_does_not_directly_call_the_changed_waits():
    source = ROOT / ('Container_Audit.py' if IS_CA else 'Label_Match.py')
    tree = ast.parse(source.read_text(encoding='utf-8-sig'))
    blocked = {'request_user_relay_stop', 'remove_current_user_setup',
               'restore_current_user_lifecycle_after_replacement', 'apply_prepared',
               '_apply_prepared', '_apply_prepared_once'}
    direct = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Call):
            name = node.func.id if isinstance(node.func, ast.Name) else node.func.attr if isinstance(node.func, ast.Attribute) else ''
            if name in blocked:
                direct.append((name, node.lineno))
    assert direct == []


@pytest.mark.parametrize('mode', ['--remove-current-user-setup'] +
                         (['--restore-current-user-lifecycle-after-replacement'] if IS_CA else []))
def test_product_host_dispatches_the_wait_before_any_gui_import(monkeypatch, mode):
    source = ROOT / ('container_audit_product_host.py' if IS_CA else 'label_match_product_host.py')
    tree = ast.parse(source.read_text(encoding='utf-8-sig'))
    selected = []
    for node in tree.body:
        if isinstance(node, ast.Assign) and any(isinstance(target, ast.Name)
            and (target.id.endswith('_MODE') or target.id == 'PRODUCT_MODES') for target in node.targets):
            selected.append(node)
        elif isinstance(node, ast.FunctionDef) and node.name == 'dispatch_product_mode':
            selected.append(node)
    calls = []
    onboarding = ModuleType('current_user_onboarding')
    onboarding.removal_main = lambda args: calls.append(('remove', args)) or 17
    onboarding.replacement_lifecycle_restore_main = lambda args: calls.append(('restore', args)) or 17
    update = ModuleType('auto_update')
    update.startup_gate = lambda *args, **kwargs: None
    monkeypatch.setitem(sys.modules, 'current_user_onboarding', onboarding)
    monkeypatch.setitem(sys.modules, 'auto_update', update)
    namespace = {'_usable_output_streams': lambda: nullcontext(), 'Path': Path, '__file__': str(source)}
    module = ast.Module(body=[ast.ImportFrom(module='__future__', names=[ast.alias(name='annotations')], level=0),
                              *selected], type_ignores=[])
    exec(compile(ast.fix_missing_locations(module), str(source), 'exec'), namespace)
    assert namespace['dispatch_product_mode']([mode, '--app-root', 'isolated-fixture']) == 17
    assert calls == [('restore' if 'restore-' in mode else 'remove', ['--app-root', 'isolated-fixture'])]
