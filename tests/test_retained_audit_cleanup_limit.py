"""Read-only review reproduction source; deliberately NOT EXECUTED by sub-186.

Future lane run only after authorization, with pytest basetemp beneath the
review-tmp root from the task. Product code comes from the fixed reviewed commit.
Only filesystem/ACL/process/mutex boundaries are replaced.
"""
import ast
import json
import os
from pathlib import Path
import re
import subprocess
from types import SimpleNamespace
import unicodedata
from typing import Any, Mapping
import uuid

import pytest

REPO = Path(__file__).resolve().parents[1]
REVISION = "19bdc712005268b70d9c2d3ce18554f4dfddff35"
REVIEW_TMP = Path(os.environ['IWRUN_CONFIG']).resolve().parent


def _load_real_cleanup_function(monkeypatch):
    monkeypatch.setenv("GIT_CONFIG_COUNT", "0")
    tree = ast.parse((REPO / 'writer_session_fence.py').read_text(encoding='utf-8-sig'))
    function_names = {
        "_ascii_lower", "_normalized_control_root",
        "_failed_installer_removal_sources",
    }
    constant_names = {
        "_INSTALLER_TUPLE_FIELDS", "_INSTALLER_OWNER_SCHEMA",
        "_FAILED_INSTALL_STATUSES",
    }
    body = []
    for node in tree.body:
        if isinstance(node, ast.FunctionDef) and node.name in function_names:
            body.append(node)
        elif isinstance(node, ast.Assign) and any(
            isinstance(target, ast.Name) and target.id in constant_names
            for target in node.targets
        ):
            body.append(node)
    namespace = {
        "os": os, "Path": Path, "re": re, "unicodedata": unicodedata,
        "Mapping": Mapping, "Any": Any,
    }
    exec(compile(ast.Module(body=body, type_ignores=[]),
                 "reviewed_writer_cleanup.py", "exec"), namespace)
    function = next(node for node in body if isinstance(node, ast.FunctionDef)
                    and node.name == "_failed_installer_removal_sources")
    install_root = next(
        node.args[0].value for node in ast.walk(function)
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Name)
        and node.func.id == "_normalized_control_root" and node.args
        and isinstance(node.args[0], ast.Constant)
        and isinstance(node.args[0].value, str)
    )
    return namespace, install_root


@pytest.mark.parametrize("old_count", [0, 127, 128])
def test_retained_pretransaction_audits_do_not_disable_current_cleanup(
    tmp_path, monkeypatch, old_count
):
    if os.name != "nt":
        pytest.skip("Windows boundary reproduction")
    try:
        tmp_path.resolve().relative_to(REVIEW_TMP.resolve())
    except ValueError:
        pytest.fail("basetemp must be beneath the authorized review-tmp root")

    namespace, install_root = _load_real_cleanup_function(monkeypatch)
    local = tmp_path / "local"
    audit_root = local / "KMTech" / "Label_Match" / "install-audit"
    owner_root = tmp_path / "protected-owner"
    control_root = tmp_path / "control"
    audit_root.mkdir(parents=True)
    owner_root.mkdir()
    control_root.mkdir()
    run_id = "20000101T000000000Z-" + uuid.uuid4().hex
    active = {name: uuid.uuid4().hex
              for name in namespace["_INSTALLER_TUPLE_FIELDS"]}
    active["session_authority_mutex_name"] = "isolated-review-authority"
    owner = dict(active)
    owner.update(
        schema=namespace["_INSTALLER_OWNER_SCHEMA"],
        run_id=run_id, control_root=str(control_root),
        install_root=install_root,
        owner_process_id=1, owner_process_started_at_filetime="1",
    )
    owner_path = owner_root / (
        ".installer-owner-" + active["session_id"] + "-"
        + run_id.rsplit("-", 1)[-1] + ".json"
    )
    owner_path.write_text(json.dumps(owner), encoding="utf-8")
    current = {
        "schema": "label-match-canonical-portable-install-v1",
        "run_id": run_id, "install_root": install_root,
        "status": sorted(namespace["_FAILED_INSTALL_STATUSES"])[0],
        "writer_fence": dict(owner),
    }
    (audit_root / ("canonical-portable-" + run_id + ".json")).write_text(
        json.dumps(current), encoding="utf-8"
    )
    for index in range(old_count):
        old_id = "20000101T000000000Z-" + format(index, "032x")
        record = {
            "schema": "label-match-canonical-portable-install-v1",
            "run_id": old_id,
            "status": "FAILED",
            "preparation_status": "FAILED_PREPARATION_ADMISSION",
            "code_placement": "NOT_STARTED",
            "rollback": {"applied": False},
        }
        (audit_root / ("canonical-portable-" + old_id + ".json")).write_text(
            json.dumps(record), encoding="utf-8"
        )

    namespace.update(
        _acquire_named_mutex=lambda *args: SimpleNamespace(release=lambda: None),
        _installer_owner_root=lambda: owner_root,
        _bounded_json=lambda path: json.loads(path.read_text(encoding="utf-8")),
        _assert_protected_owner_path=lambda path: None,
        _assert_no_reparse_components=lambda path: None,
        _installer_process_exited=lambda *args: True,
        active_fence=lambda *args, **kwargs: active,
        _WRITER_LOCAL=SimpleNamespace(),
        WriterFenceError=type("WriterFenceError", (Exception,), {}),
    )
    before = {p: p.read_bytes() for p in audit_root.iterdir()}
    allowed = namespace["_failed_installer_removal_sources"](
        control_root, active, {"LOCALAPPDATA": str(local)}
    )
    assert allowed is not None
    assert {p: p.read_bytes() for p in audit_root.iterdir()} == before
