"""Regenerate first-update pairs from each release's own application builder.

Historical replacement preimages remain immutable. Work files belong to the
explicit --work-root; --check changes no repository files. This complements
the earlier feature-specific generators after those features are assembled.
"""
from __future__ import annotations

import argparse
import ast
import hashlib
import importlib.util
import io
import json
from pathlib import Path
import re
import shutil
import subprocess
import sys
from typing import Any, Mapping

ROOT = Path(__file__).resolve().parents[1]
INSTALLED_REFS = (
    "d9160b6", "89155f17233fb1c8b5bae2a9e470832772a4d785",
    "e7b9075b2383889e77f524be7a1f0f80d7630033",  # stock 104
    "4272c5601e035f6a111e493a4acfa5d8d182978f",  # stock 105
    "06a474b46095fa044a0c46a045b3445cece1ee57",  # stock 106
)
SHELL_READY_VERSION = "v3.16.3.1"
SHELL_READY_SHA256 = "19389ecbdc0af9288f93015929975e5c603ff3c2066290d166742efea952ab71"


def validate_shell_ready_pin(root: Path) -> None:
    """Release declarations use the reviewed shell; runtime checks candidates."""
    path = root / "shell/SHELL-READY.json"
    if not path.is_file() or hashlib.sha256(path.read_bytes()).hexdigest() != SHELL_READY_SHA256:
        raise ValueError("declarations require pinned SHELL-READY " + SHELL_READY_VERSION)
    if json.loads(path.read_bytes()).get("version") != SHELL_READY_VERSION:
        raise ValueError("declaration shell version differs")


def git(*args: str) -> bytes:
    return subprocess.check_output(
        ["git", "-c", "core.autocrlf=false", "-C", str(ROOT), *args]
    )


def digest(raw: bytes) -> str:
    return hashlib.sha256(raw if raw.startswith(b"MZ") else raw.replace(b"\r\n", b"\n")).hexdigest()


def encode_source(path: Path, source: str, original: bytes) -> bytes:
    keep_bom = original.startswith(b"\xef\xbb\xbf")
    require_bom = path.suffix.lower() == ".ps1" and any(ord(char) > 127 for char in source)
    return source.encode("utf-8-sig" if keep_bom or require_bom else "utf-8")


def snapshot(revision: str, target: Path) -> None:
    """Read attested Git blobs without checkout filters or a second worktree."""
    files = []
    for entry in filter(None, git("ls-tree", "-r", "-z", revision).split(b"\0")):
        metadata, name = entry.split(b"\t", 1)
        mode, kind, oid = metadata.split()
        if kind != b"blob" or mode not in (b"100644", b"100755"):
            raise ValueError("source snapshot must contain regular Git files")
        files.append((name.decode("utf-8"), oid))
    result = subprocess.run(
        ["git", "-c", "core.autocrlf=false", "-C", str(ROOT), "cat-file", "--batch"],
        input=b"".join(oid + b"\n" for _, oid in files), capture_output=True, check=True,
    )
    with io.BytesIO(result.stdout) as stream:
        for name, oid in files:
            header = stream.readline().split()
            if len(header) != 3 or header[:2] != [oid, b"blob"]:
                raise ValueError("source snapshot header differs")
            size = int(header[2])
            raw = stream.read(size)
            if len(raw) != size or stream.read(1) != b"\n":
                raise ValueError("source snapshot is incomplete")
            file = (target / name).resolve()
            if not file.is_relative_to(target.resolve()):
                raise ValueError("source snapshot escapes work root")
            file.parent.mkdir(parents=True, exist_ok=True)
            file.write_bytes(raw)


def application_layout(source: Path, target: Path) -> dict[str, str]:
    """Use that source revision's actual portable import closure and entrypoint."""
    builder_path = source / "tools/build_portable_release_candidate.py"
    spec = importlib.util.spec_from_file_location("first_update_stock_builder", builder_path)
    builder = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(builder)
    builder._copy_application(source, target / "app")
    site_packages = target / "app/site-packages"
    site_packages.mkdir()
    builder._copy_third_party(site_packages)
    build_tree = ast.parse(builder_path.read_text(encoding="utf-8-sig"))
    build_node = next(n for n in build_tree.body if isinstance(n, ast.FunctionDef) and n.name == "build")
    build_names = {n.id for n in ast.walk(build_node) if isinstance(n, ast.Name)}
    build_strings = {n.value for n in ast.walk(build_node) if isinstance(n, ast.Constant) and isinstance(n.value, str)}
    settings_publisher = getattr(builder, "_write_auto_update_settings", None)
    publishes_settings = "_write_auto_update_settings" in build_names
    if publishes_settings or "auto-update-settings.json" in build_strings:
        policy_tree = ast.parse((source / "auto_update.py").read_text(encoding="utf-8-sig"))
        policy = next(n for n in policy_tree.body if isinstance(n, ast.FunctionDef) and n.name == "policy_from_settings")
        namespace = {"Mapping": Mapping, "Any": Any, "re": re}
        exec(compile(ast.Module(body=[policy], type_ignores=[]), "<stock policy>", "exec"), namespace)
        settings_raw = (json.dumps(namespace["policy_from_settings"]({}), sort_keys=True, indent=2) + "\n").encode()
        if publishes_settings:
            # Run the same publisher as build, then inventory its actual output.
            # Do not infer its output filename from strings in the caller AST.
            settings_publisher(target / "app", settings_raw)
        else:
            # Immutable revisions before the publisher helper wrote inline.
            (target / "app/auto-update-settings.json").write_bytes(settings_raw)
    result = {
        file.relative_to(target).as_posix(): digest(file.read_bytes())
        for file in (target / "app").rglob("*") if file.is_file()
    }
    bootstrap = {"INSTALL_THIS_PC.ps1"}
    for constant in ("BOOTSTRAP_INTEGRITY_HELPER", "WRITER_FENCE_HELPER", "WRITER_FENCE_CONTRACT",
                     "WORKER_SHORTCUT_HELPER", "FAILURE_SINK_DATA"):
        if constant not in build_names:
            continue
        values = getattr(builder, constant)
        if not isinstance(values, (tuple, list)):
            values = [values]
        bootstrap.update(Path(value).as_posix() for value in values)
    if "tools/apply_prepared_update.ps1" in build_strings:
        bootstrap.add("tools/apply_prepared_update.ps1")
    for name in bootstrap:
        result[name] = digest((source / name).read_bytes())
    return result


def historical_layout(revision: str, work: Path) -> dict[str, str]:
    directory = work / "historical-layouts" / revision
    receipt = directory / "layout.json"
    if receipt.is_file():
        value = json.loads(receipt.read_text(encoding="utf-8"))
        if value["revision"] != revision or value["schema"] != 1:
            raise ValueError("historical layout receipt differs")
        return value["files"]
    directory.mkdir(parents=True, exist_ok=True)
    source = directory / "source"
    if source.exists():
        raise ValueError("incomplete historical snapshot; preserve and use a new work root")
    snapshot(revision, source)
    files = application_layout(source, directory / "package")
    receipt.write_text(json.dumps({"schema": 1, "revision": revision, "files": files},
                                  sort_keys=True, indent=2), encoding="utf-8")
    return files


def writer_probe(source: str):
    found = []
    for match in re.finditer(r"@'\r?\n(.*?)\r?\n'@", source, re.S):
        code = match.group(1).lstrip()
        try:
            tree = ast.parse(code)
        except SyntaxError:
            continue
        assignments = {
            node.targets[0].id: node for node in tree.body
            if isinstance(node, ast.Assign) and len(node.targets) == 1
            and isinstance(node.targets[0], ast.Name)
        }
        if "startup_release_replacements" in assignments:
            found.append((match, code, tree, assignments))
    if len(found) != 1:
        raise ValueError("writer probe declaration is ambiguous")
    return found[0]


def refresh(work: Path, *, check: bool, installed_refs: list[str]) -> None:
    validate_shell_ready_pin(ROOT)
    sys.path.insert(0, str(ROOT))
    from writer_sink_inventory import derive_writer_sink_inventory, writer_sink_inventory_sha256

    # Standalone removal cannot load bootstrap code to establish trust.
    # Generate its two static pins from the candidate packet bytes instead.
    entry = ROOT / "INSTALL_THIS_PC.ps1"
    entry_raw = entry.read_bytes()
    updated = entry_raw
    for relative in ("tools/failure_redaction.ps1", "tools/failure_redaction_rules.json"):
        digest = hashlib.sha256((ROOT / relative).read_bytes()).hexdigest().encode()
        pattern = rb"('" + re.escape(relative.encode()) + rb"' = ')[0-9a-f]{64}(')"
        updated, count = re.subn(pattern, lambda match: match[1] + digest + match[2], updated)
        if count != 1:
            raise ValueError("standalone removal diagnostic pin is ambiguous: " + relative)
    if check and entry_raw != updated:
        raise ValueError("standalone removal diagnostic pins are stale")
    if not check:
        entry.write_bytes(updated)

    rows = derive_writer_sink_inventory(ROOT)
    inventory = writer_sink_inventory_sha256(rows)
    pins = {}
    for name, pattern in (
        ("writer_session_fence.py", rb'(WRITER_INVENTORY_SHA256 = ")[0-9a-f]{64}(\")'),
        ("tools/label_writer_fence.ps1", rb"(\$Script:LabelWriterFenceInventorySha256 = ')[0-9a-f]{64}(')"),
    ):
        raw = (ROOT / name).read_bytes()
        updated, count = re.subn(pattern, lambda m: m[1] + inventory.encode() + m[2], raw)
        if count != 1:
            raise ValueError("writer inventory pin is ambiguous")
        pins[name] = updated
        if not check:
            (ROOT / name).write_bytes(updated)
        elif raw != updated:
            raise ValueError("writer inventory pin is stale: " + name)

    candidate_directory = work / "candidate-layout"
    if candidate_directory.exists():
        if not candidate_directory.resolve().is_relative_to(work.resolve()):
            raise ValueError("candidate layout escapes work root")
        shutil.rmtree(candidate_directory)
    candidate = application_layout(ROOT, candidate_directory)
    installer = ROOT / "INSTALL_CANONICAL_PORTABLE.ps1"
    raw = installer.read_bytes()
    source = raw.decode("utf-8-sig")
    match, code, tree, assignments = writer_probe(source)
    replacements = ast.literal_eval(assignments["startup_release_replacements"].value)
    additions = ast.literal_eval(assignments["startup_release_additions"].value)
    historical = {revision: dict(pairs) for revision, pairs in replacements.items()}
    unsupplied = []
    for ref in installed_refs:
        revision = git("rev-parse", ref + "^{commit}").decode().strip()
        replacements.setdefault(revision, {})
        additions.setdefault(revision, {})
    for revision, pairs in replacements.items():
        before = historical_layout(revision, work)
        extra = additions.setdefault(revision, {})
        for relative, pair in list(pairs.items()):
            if relative not in candidate and relative not in before:
                # Older feature declarations also listed development helpers
                # absent from both real stock packages. They have no upgrade
                # preimage; retain only the builder's actual supply contract.
                unsupplied.append({"revision": revision, "path": relative, "kind": "replacement"})
                del pairs[relative]
                continue
            if relative not in candidate or relative not in before:
                raise ValueError("reviewed replacement is outside stock layout: " + relative)
            if before[relative] != pair[0]:
                raise ValueError("historical stock preimage differs: " + relative)
            pairs[relative] = pair[0], candidate[relative]
        for relative in list(extra):
            if relative not in candidate and relative not in before:
                unsupplied.append({"revision": revision, "path": relative, "kind": "addition"})
                del extra[relative]
                continue
            if relative not in candidate or relative in before:
                raise ValueError("reviewed addition is outside stock delta: " + relative)
            extra[relative] = candidate[relative]
        for relative, after in candidate.items():
            if relative in before:
                if before[relative] != after:
                    pairs[relative] = before[relative], after
            else:
                extra[relative] = after
    for revision, pairs in historical.items():
        for relative, pair in pairs.items():
            if relative not in replacements[revision]:
                continue  # Proven absent from both immutable builder layouts.
            if replacements[revision][relative][0] != pair[0]:
                raise ValueError("historical preimage changed")

    encoded = code.encode("utf-8")
    offsets = [0]
    for line in encoded.splitlines(keepends=True):
        offsets.append(offsets[-1] + len(line))
    edits = {}
    refreshed = 0

    def edit(node, replacement):
        start = offsets[node.lineno - 1] + node.col_offset
        end = offsets[node.end_lineno - 1] + node.end_col_offset
        edits[start, end] = replacement.encode("utf-8")

    def candidate_field(node, relative):
        nonlocal refreshed
        if (isinstance(node, ast.Constant) and isinstance(node.value, str)
                and re.fullmatch("[0-9a-f]{64}", node.value) and relative in candidate
                and node.value != candidate[relative]):
            edit(node, repr(candidate[relative]))
            refreshed += 1

    for name, assignment in assignments.items():
        if name in {"startup_release_replacements", "startup_release_additions"}:
            value = replacements if name.endswith("replacements") else additions
            if ast.literal_eval(assignment.value) != value:
                edit(assignment.value, repr(value))
        elif name == "startup_candidate_membership":
            membership = [(row.source, row.source_path, row.qualified_name, row.guard_kind)
                          for row in rows]
            if ast.literal_eval(assignment.value) != membership:
                edit(assignment.value, repr(membership))
        elif name.endswith(("_replacements", "_additions", "_companions")):
            for node in ast.walk(assignment.value):
                if not isinstance(node, ast.Dict):
                    continue
                for key, value in zip(node.keys, node.values):
                    if not isinstance(key, ast.Constant) or not isinstance(key.value, str):
                        continue
                    if isinstance(value, ast.Tuple) and len(value.elts) == 2:
                        candidate_field(value.elts[1], key.value)
                    elif name.endswith("_additions"):
                        candidate_field(value, key.value)
        elif name in {"scan_input_pairs", "nextfix_label_pair", "scanner_label_pair"}:
            pairs = assignment.value.elts if isinstance(assignment.value, ast.Set) else [assignment.value]
            for pair in pairs:
                candidate_field(pair.elts[1], "app/Label_Match.py")
        elif name == "integrity_order_transition":
            # This historical predicate keeps its installed preimage fixed and
            # binds the source side directly, outside the replacement tables.
            matches = [node for node in ast.walk(assignment.value)
                       if isinstance(node, ast.Compare) and isinstance(node.left, ast.Call)
                       and isinstance(node.left.func, ast.Name) and node.left.func.id == "release_digest"
                       and len(node.left.args) == 1 and isinstance(node.left.args[0], ast.BinOp)
                       and isinstance(node.left.args[0].left, ast.Name) and node.left.args[0].left.id == "source"
                       and isinstance(node.left.args[0].right, ast.Name) and node.left.args[0].right.id == "integrity_order_path"
                       and len(node.comparators) == 1]
            if len(matches) != 1:
                raise ValueError("integrity source declaration is ambiguous")
            candidate_field(matches[0].comparators[0], ast.literal_eval(assignments["integrity_order_path"].value))
            for comparison in ast.walk(assignment.value):
                if (isinstance(comparison, ast.Compare) and len(comparison.comparators) == 1
                        and isinstance(comparison.left, ast.Call) and comparison.left.args
                        and isinstance(comparison.left.args[0], ast.BinOp)
                        and isinstance(comparison.left.args[0].left, ast.Name)
                        and comparison.left.args[0].left.id == "source"):
                    candidate_field(comparison.comparators[0], "app/current_user_onboarding.py")
    # The scanner override also preserves its installed preimages and refreshes
    # only candidate fields; it is nested outside the declaration assignments.
    scanner_updates = [node.args[0] for node in ast.walk(tree)
                       if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute)
                       and isinstance(node.func.value, ast.Name) and node.func.value.id == "startup_changes"
                       and node.func.attr == "update" and len(node.args) == 1
                       and isinstance(node.args[0], ast.Dict)]
    if len(scanner_updates) != 1:
        raise ValueError("scanner source declaration is ambiguous")
    for key, value in zip(scanner_updates[0].keys, scanner_updates[0].values):
        if not isinstance(key, ast.Constant) or not isinstance(value, ast.Tuple) or len(value.elts) != 2:
            raise ValueError("scanner source pair is invalid")
        candidate_field(value.elts[1], key.value)

    for node in ast.walk(tree):
        if not (isinstance(node, ast.Compare) and len(node.ops) == 1 and isinstance(node.ops[0], ast.Eq)
                and isinstance(node.left, ast.Call) and isinstance(node.left.func, ast.Name)
                and node.left.func.id == "release_digest" and len(node.left.args) == 1):
            continue
        path = node.left.args[0]
        if not (isinstance(path, ast.BinOp) and isinstance(path.op, ast.Div)
                and isinstance(path.left, ast.Name) and path.left.id == "source"
                and isinstance(path.right, ast.Name) and path.right.id in assignments):
            continue
        relative = assignments[path.right.id].value
        if isinstance(relative, ast.Constant) and isinstance(relative.value, str):
            candidate_field(node.comparators[0], relative.value)
    for (start, end), replacement in sorted(edits.items(), reverse=True):
        encoded = encoded[:start] + replacement + encoded[end:]
    leading = len(match.group(1)) - len(match.group(1).lstrip())
    updated = source[:match.start(1) + leading] + encoded.decode() + source[match.end(1):]
    updated_match, _, _, _ = writer_probe(updated)
    body = updated_match.group(1).replace("\r\n", "\n").encode("utf-8")
    tail = updated[updated_match.end():]
    pin_match = re.search(r"-ProbeBytes \$probeBytes -ExpectedSha256 '([0-9a-f]{64})'", tail)
    if pin_match is None:
        raise ValueError("writer probe pin is missing")
    pin_start = updated_match.end() + pin_match.start(1)
    pin_end = updated_match.end() + pin_match.end(1)
    updated = updated[:pin_start] + hashlib.sha256(body).hexdigest() + updated[pin_end:]
    output = encode_source(installer, updated, raw)
    if check and raw != output:
        raise ValueError("first-update declarations are stale")
    if not check:
        installer.write_bytes(output)
    receipt = {"mode": "check" if check else "write", "sinks": len(rows),
               "releases": len(replacements), "historical_before_changes": 0,
               "unsupplied_declarations_removed": len(unsupplied),
               "replacement_pairs": sum(map(len, replacements.values())),
                "additions": sum(map(len, additions.values())), "candidate_fields_refreshed": refreshed}
    if unsupplied:
        (work / "unsupplied-declarations-removed.json").write_text(
            json.dumps(unsupplied, sort_keys=True, indent=2), encoding="utf-8")
    (work / ("first-update-declarations-" + receipt["mode"] + ".json")).write_text(
        json.dumps(receipt, indent=2), encoding="utf-8")
    print(json.dumps(receipt))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--work-root", type=Path, required=True)
    parser.add_argument("--installed-ref", action="append")
    parser.add_argument("--check", action="store_true")
    args = parser.parse_args()
    args.work_root = args.work_root.resolve()
    if args.work_root.is_relative_to(ROOT):
        parser.error("work root must be outside the repository")
    args.work_root.mkdir(parents=True, exist_ok=True)
    refresh(args.work_root, check=args.check,
            installed_refs=args.installed_ref or list(INSTALLED_REFS))
