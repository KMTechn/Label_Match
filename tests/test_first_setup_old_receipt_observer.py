"""Execute the real install_os observation block with previous/current APIs."""
import ast
from functools import wraps
from pathlib import Path
import sys
from types import ModuleType, SimpleNamespace
import pytest

@pytest.mark.parametrize('has_receipt', [False, True], ids=['previous123','current124'])
def test_optional_receipt_observation_preserves_runtime_capability(monkeypatch,tmp_path,has_receipt):
    source=Path(__file__).resolve().parents[1]/'tests/_first_setup_host.py'
    tree=ast.parse(source.read_text(encoding='utf-8-sig'))
    function=next(n for n in tree.body if isinstance(n,ast.FunctionDef) and n.name=='install_os')
    block=next(n for n in function.body if isinstance(n,ast.If) and 'LABEL_MATCH_HOST_TEST_LOCK_TIMELINE' in ast.unparse(n.test))
    calls=[]
    def receipt(*args,**kwargs):
        calls.append((args,kwargs));return True
    updates=SimpleNamespace()
    if has_receipt:
        import auto_update as real
        from tests import _first_setup_ready_observer as observer
        names=set(observer.REJECTIONS)|{'ordinary_user_context','version_manifest','read_json','process_matches_trial'}
        updates=SimpleNamespace(**{name:getattr(real,name) for name in names})
        receipt=wraps(real._relay_ready_receipt)(receipt)
        updates._relay_ready_receipt=receipt
    lock=ModuleType('tests._first_setup_lock_observer');lock.install=lambda *args:None
    monkeypatch.setitem(sys.modules,'tests._first_setup_lock_observer',lock)
    namespace=dict(os=SimpleNamespace(environ={'LABEL_MATCH_HOST_TEST_LOCK_TIMELINE':'1'}),
        root=tmp_path,updates=updates,fence=object(),native_lock=object(),native_sid=object(),app=tmp_path)
    exec(compile(ast.Module(body=[block],type_ignores=[]),str(source),'exec'),namespace)
    assert calls==[], 'install must not invoke product receipt or add product reads'
    if has_receipt:
        assert callable(updates._relay_ready_receipt) and updates._relay_ready_receipt is not receipt
    else:
        assert not hasattr(updates,'_relay_ready_receipt')
        assert not (tmp_path/'ready-receipt-observations').exists()