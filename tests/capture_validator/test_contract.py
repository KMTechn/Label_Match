from copy import deepcopy
import hashlib
import json
import os
from pathlib import Path
import stat
import struct
import subprocess
import sys
import zlib

import pytest

from .fixture_contract import REGISTRY, SCHEMA, make_bundle, sha, write_json, write_png

VALIDATOR = Path(__file__).resolve().parents[2] / "tools/validate_capture_bundle_v1.py"


def invoke(bundle, *, code=0, reason=None, directory=False, described=None, app=None):
    command = [sys.executable, "-B", str(VALIDATOR), str(bundle.root if directory else bundle.index_path), "--app", app or bundle.app]
    if described is not None:
        command.extend(["--describe-json", str(described)])
    completed = subprocess.run(command, stdin=subprocess.DEVNULL, capture_output=True, timeout=30)
    assert completed.stderr == b""
    assert len(completed.stdout) < 16384
    report = json.loads(completed.stdout.decode("utf-8"))
    assert completed.returncode == code, report
    assert report["exit_code"] == code
    assert report["result"] == {0: "PASS", 2: "FAIL", 3: "APPROVAL_PENDING"}[code]
    counts = {key: sum(check["status"] == key for check in report["checks"]) for key in ("PASS", "FAIL", "APPROVAL_PENDING")}
    assert report["summary"] == counts
    assert (counts["FAIL"] > 0) == (code == 2)
    if reason is not None:
        assert [check["reason_code"] for check in report["checks"] if check["status"] == "FAIL"] == [reason]
    return report


@pytest.mark.parametrize("app", list(REGISTRY))
@pytest.mark.parametrize("directory", [False, True], ids=["index", "directory"])
@pytest.mark.parametrize("pending", [False, True], ids=["resolved", "placeholder"])
@pytest.mark.parametrize("describe", [False, True], ids=["no-description", "description"])
def test_five_apps_both_forms_and_outcomes(tmp_path, app, directory, pending, describe):
    bundle = make_bundle(tmp_path, app, pending)
    report = invoke(bundle, directory=directory, code=3 if pending else 0,
                    described=bundle.describe() if describe else None)
    assert report["summary"]["APPROVAL_PENDING"] == (5 if pending else 0)
    if pending:
        checks = [check for check in report["checks"] if check["status"] == "APPROVAL_PENDING"]
        assert {check["reason_code"] for check in checks} == {"ORGANIZATION_PLACEHOLDER"}
        assert {check["subject"] for check in checks} == {
            "manifest.approval.approver", "approval-receipt.approver", "custody-receipt.custodian",
            "custody-receipt.custody_location", "custody-receipt.retention_period"}


@pytest.mark.parametrize(("target", "reason"), [
    ("image", "IMAGE_DIGEST_MISMATCH"), ("state", "STATE_DIGEST_MISMATCH"),
    ("capture-set", "CAPTURE_SET_DIGEST_MISMATCH"), ("approval", "RECEIPT_DIGEST_MISMATCH"),
    ("custody", "RECEIPT_DIGEST_MISMATCH"), ("manifest", "MANIFEST_DIGEST_MISMATCH"),
    ("artifact", "ARTIFACT_DIGEST_MISMATCH"),
])
def test_single_byte_tamper_restores_to_pass(tmp_path, target, reason):
    bundle = make_bundle(tmp_path)
    paths = {
        "image": bundle.root / bundle.manifest["captures"][0]["image_file"],
        "state": bundle.root / bundle.states[0][0], "capture-set": bundle.root / bundle.prefix / "capture-set.json",
        "approval": bundle.root / bundle.prefix / "approval/approval-receipt.json",
        "custody": bundle.root / bundle.prefix / "approval/custody-receipt.json",
        "manifest": bundle.manifest_path, "artifact": bundle.root / "artifacts/portable.bin",
    }
    path = paths[target]
    original = path.read_bytes()
    invoke(bundle)
    with path.open("ab") as stream:
        stream.write(b" ")
    invoke(bundle, code=2, reason=reason)
    path.write_bytes(original)
    invoke(bundle)


@pytest.mark.parametrize(("defect", "reason"), [
    ("state_app", "APP_MISMATCH"), ("state_bundle", "BUNDLE_MISMATCH"),
    ("state_id", "SEMANTIC_MISMATCH"), ("state_viewport", "SEMANTIC_MISMATCH"),
    ("state_dpi", "SEMANTIC_MISMATCH"), ("state_time", "SEMANTIC_MISMATCH"),
    ("state_image_path", "SEMANTIC_MISMATCH"), ("state_image_hash", "SEMANTIC_MISMATCH"),
    ("set_source", "SEMANTIC_MISMATCH"), ("set_tool", "SEMANTIC_MISMATCH"),
    ("set_artifact", "SEMANTIC_MISMATCH"), ("set_capture", "SEMANTIC_MISMATCH"),
    ("set_extension", "SEMANTIC_MISMATCH"), ("approval_approver", "SEMANTIC_MISMATCH"),
    ("approval_bundle", "BUNDLE_MISMATCH"), ("custody_app", "APP_MISMATCH"),
    ("approval_set_path", "PATH_BINDING_MISMATCH"), ("custody_set_path", "PATH_BINDING_MISMATCH"),
    ("custody_approval_path", "PATH_BINDING_MISMATCH"), ("arbitrary_receipt", "SCHEMA_VIOLATION"),
])
def test_fully_resealed_semantic_defects_and_restoration(tmp_path, defect, reason):
    bundle = make_bundle(tmp_path)
    original = deepcopy(bundle)
    invoke(bundle)
    state = bundle.states[0][1]
    if defect.startswith("state_"):
        key, value = {
            "state_app": ("app", "Container_Audit"), "state_bundle": ("bundle_id", "wrong"),
            "state_id": ("state_id", "wrong"), "state_viewport": ("viewport", {"width_px": 3, "height_px": 1}),
            "state_dpi": ("dpi", 120), "state_time": ("generated_at", "2026-09-03T01:02:03Z"),
            "state_image_path": ("image_file", "different.png"), "state_image_hash": ("image_sha256", "0" * 64),
        }[defect]
        state[key] = value
    elif defect == "set_source": bundle.capture_set["app_source"]["tree"] = "a" * 40
    elif defect == "set_tool": bundle.capture_set["capture_tool"]["commit"] = "a" * 40
    elif defect == "set_artifact": bundle.capture_set["portable_artifact"]["file"] = "different.bin"
    elif defect == "set_capture": bundle.capture_set["captures"][0]["dpi"] = 120
    elif defect == "set_extension": bundle.capture_set["app_specific"] = {"status": "PASS"}
    elif defect == "approval_approver": bundle.approval["approver"] = "Different synthetic role"
    elif defect == "approval_bundle": bundle.approval["bundle_id"] = "wrong"
    elif defect == "custody_app": bundle.custody["app"] = "Container_Audit"
    elif defect == "approval_set_path": bundle.approval["capture_set_file"] = "other/capture-set.json"
    elif defect == "custody_set_path": bundle.custody["capture_set_file"] = "other/capture-set.json"
    elif defect == "custody_approval_path": bundle.custody["approval_receipt_file"] = "other/approval-receipt.json"
    elif defect == "arbitrary_receipt": bundle.approval = {"arbitrary": "receipt"}
    bundle.seal()
    invoke(bundle, code=2, reason=reason)
    original.seal()
    invoke(original)


@pytest.mark.parametrize(("defect", "reason"), [("missing", "STATE_MISSING"), ("extra", "STATE_EXTRA"), ("duplicate", "STATE_DUPLICATE")])
def test_exact_state_set(tmp_path, defect, reason):
    bundle = make_bundle(tmp_path)
    original = deepcopy(bundle)
    for document in (bundle.manifest, bundle.capture_set):
        if defect == "missing": document["captures"].pop()
        elif defect == "duplicate": document["captures"].append(deepcopy(document["captures"][0]))
        else:
            row = deepcopy(document["captures"][0])
            row["state_id"] = "undeclared-state"
            document["captures"].append(row)
    bundle.seal()
    invoke(bundle, code=2, reason=reason)
    original.seal()
    invoke(original)


@pytest.mark.parametrize("path", ["../outside.bin", "/outside.bin", "C:/outside.bin", "dir\\file", "a//b", "./file", "file:stream", "a/../b", "NUL.txt", "a./file", "a /file"])
def test_unsafe_artifact_reference(tmp_path, path):
    bundle = make_bundle(tmp_path)
    bundle.manifest["portable_artifact"]["file"] = path
    bundle.seal()
    invoke(bundle, code=2, reason="PATH_INVALID")


@pytest.mark.parametrize(("field", "value", "reason"), [
    ("schema", "wrong", "SCHEMA_VIOLATION"), ("app", "Container_Audit", "APP_MISMATCH"),
    ("tree", "b" * 39, "SCHEMA_VIOLATION"), ("blob", "D" * 64, "SCHEMA_VIOLATION"),
    ("tool_path", "tools/other.py", "TOOL_PATH_MISMATCH"), ("commit", "a" * 40, "BUNDLE_MISMATCH"),
    ("dpi", True, "SCHEMA_VIOLATION"), ("width", 0, "SCHEMA_VIOLATION"),
    ("time", "2026-09-03T01:02:04Z", "TIMESTAMP_MISMATCH"),
    ("time", "2026-09-33T01:02:03Z", "SCHEMA_VIOLATION"),
    ("time", "2026-09-03T01:02:03+00:00", "SCHEMA_VIOLATION"),
    ("organization", "", "SCHEMA_VIOLATION"), ("organization", " value ", "SCHEMA_VIOLATION"),
    ("root_bundle", "wrong", "SCHEMA_VIOLATION"),
])
def test_identity_and_field_shape(tmp_path, field, value, reason):
    bundle = make_bundle(tmp_path)
    m = bundle.manifest
    if field in ("schema", "app"): m[field] = value
    elif field in ("tree", "commit"): m["app_source"][field] = value
    elif field == "blob": m["capture_tool"]["blob_sha256"] = value
    elif field == "tool_path": m["capture_tool"]["path"] = value
    elif field == "width": m["captures"][0]["viewport"]["width_px"] = value
    elif field in ("dpi", "time"): m["captures"][0]["generated_at" if field == "time" else field] = value
    elif field == "organization": m["approval"]["approver"] = bundle.approval["approver"] = value
    elif field == "root_bundle": m["bundle_id"] = value
    bundle.seal()
    invoke(bundle, code=2, reason=reason)


@pytest.mark.parametrize("defect", ["missing", "duplicate", "extra", "app", "schema"])
def test_describe_does_not_define_registry(tmp_path, defect):
    bundle = make_bundle(tmp_path)
    path = bundle.describe()
    doc = json.loads(path.read_text(encoding="utf-8"))
    if defect == "missing": doc["required_state_ids"].pop()
    elif defect == "duplicate": doc["required_state_ids"].append(doc["required_state_ids"][0])
    elif defect == "extra": doc["required_state_ids"].append("extra")
    elif defect == "app": doc["app"] = "Container_Audit"
    else: doc["schema"] = "wrong"
    write_json(path, doc)
    invoke(bundle, described=path, code=2, reason={"app": "APP_MISMATCH", "schema": "SCHEMA_VIOLATION"}.get(defect, "DESCRIBE_MISMATCH"))


def test_pending_never_masks_real_failure_or_app_specific_pass(tmp_path):
    bundle = make_bundle(tmp_path, pending=True)
    bundle.manifest["app_specific"] = bundle.capture_set["app_specific"] = {"status": "PASS", "release_allowed": True}
    bundle.seal()
    (bundle.root / bundle.manifest["captures"][0]["image_file"]).unlink()
    report = invoke(bundle, code=2, reason="FILE_MISSING")
    assert report["summary"]["APPROVAL_PENDING"] == 5


@pytest.mark.parametrize("defect", ["dimensions", "malformed"])
def test_resealed_actual_png_is_validated(tmp_path, defect):
    bundle = make_bundle(tmp_path)
    row = bundle.manifest["captures"][0]
    path = bundle.root / row["image_file"]
    if defect == "dimensions": write_png(path, width=3)
    else: path.write_text("not an image", encoding="ascii")
    for document in (bundle.manifest, bundle.capture_set): document["captures"][0]["image_sha256"] = sha(path)
    bundle.states[0][1]["image_sha256"] = sha(path)
    bundle.seal()
    invoke(bundle, code=2, reason="IMAGE_DIMENSION_MISMATCH" if defect == "dimensions" else "IMAGE_INVALID")


@pytest.mark.parametrize("defect", ["duplicate_key", "nonfinite", "bad_utf8", "too_large"])
def test_strict_json_parser(tmp_path, defect):
    bundle = make_bundle(tmp_path)
    path = bundle.index_path
    if defect == "duplicate_key": path.write_text('{"schema":"first","schema":"second","manifests":[]}', encoding="utf-8")
    elif defect == "nonfinite": path.write_text('{"schema":NaN,"manifests":[]}', encoding="utf-8")
    elif defect == "bad_utf8": path.write_bytes(bytes((255, 254)))
    else:
        with path.open("w", encoding="ascii") as stream:
            for _ in range(33): stream.write(" " * 65536)
    invoke(bundle, code=2, reason="RESOURCE_LIMIT" if defect == "too_large" else "SCHEMA_VIOLATION")


def test_directory_ambiguity_requires_explicit_index(tmp_path):
    bundle = make_bundle(tmp_path)
    extra = bundle.index_path.with_name("handover-index__20260903T010205Z__aaaaaaaa.json")
    extra.write_bytes(bundle.index_path.read_bytes())
    invoke(bundle, directory=True, code=2, reason="INDEX_AMBIGUOUS")
    invoke(bundle)


@pytest.mark.parametrize("defect", ["duplicate", "multiple_bundles", "missing_app"])
def test_ambiguous_index_entries(tmp_path, defect):
    bundle = make_bundle(tmp_path)
    doc = bundle.index
    if defect == "missing_app": doc["manifests"] = []
    else:
        row = deepcopy(doc["manifests"][0])
        if defect == "multiple_bundles": row["manifest_file"] = row["manifest_file"].replace("1234abcd", "aaaaaaaa")
        doc["manifests"].append(row)
    write_json(bundle.index_path, doc)
    invoke(bundle, code=2, reason="SCHEMA_VIOLATION" if defect == "missing_app" else "INDEX_AMBIGUOUS")


def test_extra_bundle_file_and_latest_alias(tmp_path):
    bundle = make_bundle(tmp_path)
    extra = bundle.manifest_path.parent / "extra.txt"
    extra.write_text("extra", encoding="ascii")
    invoke(bundle, code=2, reason="TOPOLOGY_MISMATCH")
    extra.unlink()
    alias = bundle.root / "LATEST.json"
    alias.write_text("alias", encoding="ascii")
    invoke(bundle, code=2, reason="MUTABLE_ALIAS_FORBIDDEN")
    alias.unlink()
    invoke(bundle)


def test_complete_reordering_and_diagnostic_extensions(tmp_path):
    bundle = make_bundle(tmp_path)
    for document in (bundle.manifest, bundle.capture_set):
        document["captures"].reverse()
        document["app_specific"] = {"status": "FAIL", "note": "diagnostic is not authority"}
    bundle.states[0][1]["app_specific"] = {"diagnostic": True}
    bundle.seal()
    described = bundle.describe()
    write_json(described, dict(schema=SCHEMA, app=bundle.app, required_state_ids=list(reversed(REGISTRY[bundle.app]))))
    invoke(bundle, described=described)


@pytest.mark.parametrize("kind", ["symlink", "broken_symlink", "junction", "hardlink"])
def test_windows_link_escape_is_rejected(tmp_path, kind):
    bundle = make_bundle(tmp_path)
    outside = tmp_path / "outside"
    outside.mkdir()
    target = outside / "artifact.bin"
    target.write_text("outside", encoding="ascii")
    link = bundle.root / "redirect"
    if kind == "junction":
        if os.name != "nt": pytest.skip("Windows junction requires Windows")
        # Both literal absolute paths are fixture-owned E: paths; no recursive delete/move.
        assert link.resolve().is_relative_to(tmp_path.resolve())
        assert outside.resolve().is_relative_to(tmp_path.resolve())
        quoted_link = str(link).replace("'", "''")
        quoted_target = str(outside).replace("'", "''")
        completed = subprocess.run(["powershell", "-NoProfile", "-NonInteractive", "-Command",
            f"New-Item -ItemType Junction -Path '{quoted_link}' -Target '{quoted_target}' | Out-Null"], capture_output=True)
        assert completed.returncode == 0
    elif kind == "hardlink":
        artifact = bundle.root / "artifacts/portable.bin"
        os.link(artifact, link)
    else:
        try:
            os.symlink(target if kind == "symlink" else outside / "missing", link)
        except OSError as exc:
            pytest.skip(f"OS does not permit symlink creation: winerror={getattr(exc, 'winerror', None)} errno={exc.errno}")
    invoke(bundle, code=2, reason="HARDLINK_FORBIDDEN" if kind == "hardlink" else "SYMLINK_FORBIDDEN")


def test_read_only_inputs_unchanged(tmp_path):
    bundle = make_bundle(tmp_path)
    files = list(bundle.root.rglob("*"))
    before = {str(p.relative_to(bundle.root)): (sha(p), p.stat().st_mtime_ns, p.stat().st_size) for p in files if p.is_file()}
    before_dirs = {str(p.relative_to(bundle.root)) for p in files if p.is_dir()}
    for path in files:
        if path.is_file(): path.chmod(stat.S_IREAD)
    try:
        invoke(bundle)
        after = {str(p.relative_to(bundle.root)): (sha(p), p.stat().st_mtime_ns, p.stat().st_size) for p in bundle.root.rglob("*") if p.is_file()}
        assert before == after
        assert before_dirs == {str(p.relative_to(bundle.root)) for p in bundle.root.rglob("*") if p.is_dir()}
    finally:
        for path in files:
            if path.is_file(): path.chmod(stat.S_IREAD | stat.S_IWRITE)


@pytest.mark.parametrize("args", [[], ["--unknown"], ["missing", "--app", "unknown"]])
def test_invalid_cli_is_bounded_json_failure(args):
    completed = subprocess.run([sys.executable, "-B", str(VALIDATOR), *args], capture_output=True, timeout=10)
    assert completed.returncode == 2
    assert completed.stderr == b""
    report = json.loads(completed.stdout)
    assert report["result"] == "FAIL" and report["exit_code"] == 2
    assert report["summary"]["FAIL"] == 1


def test_oversized_png_header_produces_json_without_allocating_pixels(tmp_path):
    bundle = make_bundle(tmp_path)
    path = bundle.root / bundle.manifest["captures"][0]["image_file"]
    payload = bytearray(path.read_bytes())
    payload[16:24] = struct.pack(">II", 65535, 65535)
    payload[29:33] = struct.pack(">I", zlib.crc32(payload[12:29]) & 0xffffffff)
    path.write_bytes(payload)
    for document in (bundle.manifest, bundle.capture_set): document["captures"][0]["image_sha256"] = sha(path)
    bundle.states[0][1]["image_sha256"] = sha(path)
    bundle.seal()
    invoke(bundle, code=2, reason="RESOURCE_LIMIT")


@pytest.mark.parametrize("directory", [False, True])
def test_symlink_ancestor_of_input_is_rejected(tmp_path, directory):
    bundle = make_bundle(tmp_path)
    alias = tmp_path / "root-alias"
    try:
        os.symlink(bundle.root, alias, target_is_directory=True)
    except OSError as exc:
        pytest.skip(f"OS does not permit directory symlink creation: winerror={getattr(exc, 'winerror', None)} errno={exc.errno}")
    bundle.root = alias
    invoke(bundle, directory=directory, code=2, reason="SYMLINK_FORBIDDEN")


@pytest.mark.parametrize("target", ["image", "state", "artifact"])
def test_referenced_symlink_escape_is_rejected(tmp_path, target):
    bundle = make_bundle(tmp_path)
    path = bundle.root / ({"image": bundle.manifest["captures"][0]["image_file"],
                           "state": bundle.states[0][0], "artifact": "artifacts/portable.bin"}[target])
    outside = tmp_path / "outside-file"
    outside.write_bytes(path.read_bytes())
    path.unlink()
    try:
        os.symlink(outside, path)
    except OSError as exc:
        pytest.skip(f"OS does not permit file symlink creation: winerror={getattr(exc, 'winerror', None)} errno={exc.errno}")
    invoke(bundle, code=2, reason="SYMLINK_FORBIDDEN")


@pytest.mark.parametrize(("defect", "reason"), [
    ("index_digest", "MANIFEST_DIGEST_MISMATCH"), ("index_digest_shape", "SCHEMA_VIOLATION"),
    ("index_schema", "SCHEMA_VIOLATION"), ("index_wrong_app", "INDEX_AMBIGUOUS"),
    ("image_path", "PATH_BINDING_MISMATCH"), ("state_path", "PATH_BINDING_MISMATCH"),
    ("approval_path", "PATH_BINDING_MISMATCH"), ("custody_path", "PATH_BINDING_MISMATCH"),
])
def test_index_and_graph_reference_defects(tmp_path, defect, reason):
    bundle = make_bundle(tmp_path)
    if defect.startswith("index_"):
        if defect == "index_digest": bundle.index["manifests"][0]["manifest_sha256"] = "0" * 64
        elif defect == "index_digest_shape": bundle.index["manifests"][0]["manifest_sha256"] = "A" * 64
        elif defect == "index_schema": bundle.index["schema"] = "wrong"
        else:
            bundle.index["manifests"][0]["app"] = "Container_Audit"
            bundle.index["manifests"][0]["manifest_file"] = "Container_Audit/other/manifest.json"
        write_json(bundle.index_path, bundle.index)
    else:
        if defect == "image_path": bundle.manifest["captures"][0]["image_file"] = "other/image.png"
        elif defect == "state_path": bundle.manifest["captures"][0]["state_manifest_file"] = "other/state.json"
        elif defect == "approval_path": bundle.manifest["approval"]["approval_receipt_file"] = "other/approval-receipt.json"
        else: bundle.manifest["approval"]["custody_receipt_file"] = "other/custody-receipt.json"
        bundle.seal()
    invoke(bundle, code=2, reason=reason)
