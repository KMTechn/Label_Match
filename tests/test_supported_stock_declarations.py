import ast
import hashlib
import json
from pathlib import Path
import re
import shutil
import sys
from typing import Any, Mapping

import pytest

from tools import refresh_first_update_declarations as generator
from writer_sink_inventory import derive_writer_sink_inventory, writer_sink_inventory_sha256, _powershell_inventory

ROOT = Path(__file__).resolve().parents[1]
STOCK = (
    'e7b9075b2383889e77f524be7a1f0f80d7630033',
    '4272c5601e035f6a111e493a4acfa5d8d182978f',
    '06a474b46095fa044a0c46a045b3445cece1ee57',
)
LOADERS = {f'app/site-packages/webview/lib/runtimes/{arch}/native/WebView2Loader.dll'
           for arch in ('win-arm64','win-x86')}


def probe_tree():
    source = (ROOT/'INSTALL_CANONICAL_PORTABLE.ps1').read_text(encoding='utf-8-sig')
    _, _, tree, assignments = generator.writer_probe(source)
    return tree, assignments


def test_stock_inputs_and_directional_loader_additions():
    _, assignments = probe_tree()
    replacements = ast.literal_eval(assignments['startup_release_replacements'].value)
    additions = ast.literal_eval(assignments['startup_release_additions'].value)
    assert set(STOCK) <= set(generator.INSTALLED_REFS)
    assert set(STOCK) <= replacements.keys() and len(replacements) == 23
    for revision in STOCK[:2]:
        assert LOADERS <= additions[revision].keys()
    assert not LOADERS.intersection(additions[STOCK[2]])


@pytest.fixture(scope='module')
def stock_layouts(tmp_path_factory):
    work = tmp_path_factory.mktemp('immutable-stock-layouts')
    layouts={}
    for revision in (*STOCK, 'candidate'):
        source=ROOT if revision == 'candidate' else work/revision/'source'
        if revision != 'candidate':generator.snapshot(revision,source)
        package=work/revision/'package'
        import_path = sys.path[:]
        imported_modules = sys.modules.copy()
        try:
            files=generator.application_layout(source,package)
        finally:
            sys.path[:] = import_path
            for name, module in tuple(sys.modules.items()):
                origin = getattr(module, '__file__', None)
                if not origin or not Path(origin).resolve().is_relative_to(work.resolve()):
                    continue
                if name in imported_modules:
                    sys.modules[name] = imported_modules[name]
                else:
                    sys.modules.pop(name, None)
        policy_tree=ast.parse((source/'auto_update.py').read_text(encoding='utf-8-sig'))
        policy=next(node for node in policy_tree.body if isinstance(node,ast.FunctionDef) and node.name=='policy_from_settings')
        policy_scope={'Mapping':Mapping,'Any':Any,'re':re}
        exec(compile(ast.Module(body=[policy],type_ignores=[]),'stock-policy','exec'),policy_scope)
        (package/'app/auto-update-settings.json').write_bytes(
            (json.dumps(policy_scope['policy_from_settings']({}),sort_keys=True,indent=2)+'\n').encode())
        # Key rotation is a separate guard. This file-rule qualification holds
        # the external public configuration at the unchanged stock 105 input.
        shutil.copyfile(ROOT/'tests/fixtures/lm_stock_public_update_key.json',
                        package/'app/update-manifest-key-config.json')
        for relative in files:
            if relative.startswith('app/'):
                continue
            destination=package/relative
            destination.parent.mkdir(parents=True,exist_ok=True)
            shutil.copyfile(source/relative,destination)
        shutil.copyfile(source/'portable/launch-label-match.cmd',package/'launch-label-match.cmd')
        (package/'runtime').mkdir()
        (package/'portable-manifest.json').write_text(json.dumps({'source_commit':revision}),encoding='utf-8')
        layouts[revision]=package
    return layouts


def run_declared_file_rule(source, installed, revision):
    tree, assignments=probe_tree()
    definitions=[node for node in tree.body if isinstance(node,ast.FunctionDef)]
    literals=[]
    for node in tree.body:
        if isinstance(node,ast.Assign):
            try: ast.literal_eval(node.value)
            except (ValueError,TypeError):continue
            literals.append(node)
    namespace={'ast':ast,'hashlib':hashlib,'json':json,'pathlib':__import__('pathlib'),'re':re,'sys':sys,'Any':Any,'Mapping':Mapping,
               'derive_writer_sink_inventory':derive_writer_sink_inventory,
               'writer_sink_inventory_sha256':writer_sink_inventory_sha256,'_powershell_inventory':_powershell_inventory}
    exec(compile(ast.Module(body=definitions+literals,type_ignores=[]),'installer-file-rule','exec'),namespace)
    namespace.update(source=source,installed=installed,startup_from=revision,
                     startup_changes=ast.literal_eval(assignments['startup_release_replacements'].value)[revision],
                     source_admission_transition=False)
    branch=next(node for node in tree.body if isinstance(node,ast.If)
                and isinstance(node.test,ast.Compare) and isinstance(node.test.left,ast.Name)
                and node.test.left.id=='startup_changes')
    exec(compile(ast.Module(body=[branch],type_ignores=[]),'installer-file-rule','exec'),namespace)


@pytest.mark.parametrize('revision',STOCK)
@pytest.mark.parametrize('mutation',['none','unknown_file','bad_loader'])
def test_real_installer_file_rule_accepts_exact_stock_delta_only(tmp_path,stock_layouts,revision,mutation,capsys):
    candidate=tmp_path/'candidate'
    shutil.copytree(stock_layouts['candidate'],candidate)
    if mutation == 'unknown_file':
        (candidate/'app/unapproved_file.py').write_text('pass\n',encoding='utf-8')
    elif mutation == 'bad_loader':
        (candidate/sorted(LOADERS)[0]).write_bytes(b'invalid native bytes')
    if mutation == 'none':
        with pytest.raises(SystemExit) as finished:
            run_declared_file_rule(candidate,stock_layouts[revision],revision)
        assert finished.value.code == 0
        result=json.loads(capsys.readouterr().out)
        assert result['compatibility'] == 'PINNED_STARTUP_UPDATE_BOOTSTRAP'
    else:
        with pytest.raises(ValueError):
            run_declared_file_rule(candidate,stock_layouts[revision],revision)


def test_unregistered_stock_cannot_claim_the_declared_additions(tmp_path,stock_layouts):
    installed=tmp_path/'unregistered'
    shutil.copytree(stock_layouts[STOCK[1]],installed)
    (installed/'portable-manifest.json').write_text(json.dumps({'source_commit':'f'*40}),encoding='utf-8')
    _,code,_,_=generator.writer_probe((ROOT/'INSTALL_CANONICAL_PORTABLE.ps1').read_text(encoding='utf-8-sig'))
    original_arguments=sys.argv
    original_paths=list(sys.path)
    try:
        sys.argv=['installer-file-rule',str(stock_layouts['candidate']),str(installed)]
        with pytest.raises(ValueError,match='WRITER_TRANSITION_SOURCE_SET_DIFFERS'):
            exec(compile(code,'installer-file-rule','exec'),{})
    finally:
        sys.argv=original_arguments
        sys.path[:]=original_paths
