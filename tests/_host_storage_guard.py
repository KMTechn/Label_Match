"""Read-only host evidence and known-folder isolation for headless tests."""

import argparse
import ctypes
import hashlib
import json
import os
from pathlib import Path
import re
import subprocess
import sys


HOST_ROOT = Path("C:/ProgramData/KMTech")
OWN_ROOT = Path("E:/KMTech/w9firstupd-tmp/w9firstupdsub121")


def storage_snapshot(root):
    root = Path(root)
    rows = {}
    pending = [root] if root.exists() else []
    while pending:
        path = pending.pop()
        stat = path.lstat()
        key = hashlib.sha256(str(path.relative_to(root)).encode("utf-8")).hexdigest()
        row = {"size": stat.st_size, "mtime_ns": stat.st_mtime_ns,
               "created_ns": getattr(stat, "st_birthtime_ns", stat.st_ctime_ns),
               "attributes": getattr(stat, "st_file_attributes", 0)}
        if os.name == "nt":
            attributes = ctypes.WinDLL("kernel32", use_last_error=True).GetFileAttributesW
            attributes.argtypes = [ctypes.c_wchar_p]
            attributes.restype = ctypes.c_ulong
            native = int(attributes(str(path)))
            if native != 0xFFFFFFFF:
                row["attributes"] = native
        if row["attributes"] & 0x400 or path.is_symlink():
            row["reparse"] = True
        elif path.is_dir():
            row["directory"] = True
            # NTFS directory allocation length and Python's extra stat flags
            # vary across queries; child entries and native attributes do not.
            row.pop("size")
            try:
                pending.extend(path.iterdir())
            except PermissionError:
                row["unreadable_children"] = True
        elif path.name == "data-generation.json":
            digest = hashlib.sha256()
            try:
                with path.open("rb") as stream:
                    for block in iter(lambda: stream.read(1024 * 1024), b""):
                        digest.update(block)
                row["sha256"] = digest.hexdigest()
            except PermissionError:
                row["unreadable_contents"] = True
        rows[key] = row
    return rows


def storage_changes(before, after):
    return {"new": sorted(after.keys() - before.keys()),
            "changed": sorted(key for key in before.keys() & after.keys()
                              if before[key] != after[key]),
            "removed": sorted(before.keys() - after.keys())}


def host_write_blocker(root=HOST_ROOT):
    scope = os.path.normcase(os.path.abspath(root))

    def under_scope(value):
        if not isinstance(value, (str, bytes, os.PathLike)):
            return False
        path = os.path.normcase(os.path.abspath(os.fsdecode(value)))
        return path == scope or path.startswith(scope + os.sep)

    def audit(event, args):
        if event == "open":
            path, mode, flags = args
            writing = (isinstance(mode, str) and any(c in mode for c in "wax+")) or (
                flags & (os.O_WRONLY | os.O_RDWR | os.O_CREAT | os.O_TRUNC | os.O_APPEND))
            paths = [path] if writing else []
        elif event in {"os.mkdir", "os.remove", "os.rmdir", "os.chmod", "os.utime", "os.truncate"}:
            paths = args[:1]
        elif event in {"os.rename", "os.link", "os.symlink"}:
            paths = args[:2]
        else:
            return
        if any(under_scope(path) for path in paths):
            raise PermissionError("Host KMTech storage writes are blocked in tests")

    return audit


def isolate_known_folders(code, root, environ):
    """Inject at the test script boundary; installed product bytes stay exact."""
    root = Path(root).resolve()
    if not root.is_relative_to(OWN_ROOT.resolve()):
        raise ValueError("Native test root is outside the owned isolation root")
    pattern = re.compile(
        r"\[(?:System\.)?Environment\]::GetFolderPath\(\s*"
        r"(?:['\"](?P<quoted>\w+)['\"]|"
        r"\[(?:System\.)?Environment\+SpecialFolder\]::(?P<enum>\w+))"
        r"(?:\s*,\s*\[(?:System\.)?Environment\+SpecialFolderOption\]::DoNotVerify)?\s*\)",
        re.I,
    )
    mapping = {"commonapplicationdata": "PROGRAMDATA", "localapplicationdata": "LOCALAPPDATA",
               "applicationdata": "APPDATA"}

    def replace(match):
        folder = (match.group("quoted") or match.group("enum")).lower()
        if folder not in mapping and folder not in {"startup", "commonstartup", "programs", "commonprograms",
                                                    "desktopdirectory", "commondesktopdirectory"}:
            raise ValueError("Unreviewed native known folder in test script")
        name = mapping.get(folder)
        target = Path(environ[name]) if name and environ.get(name) else root / "known-folders" / folder
        target = target.resolve()
        if not target.is_relative_to(OWN_ROOT.resolve()):
            raise ValueError("Known folder escaped the owned isolation root")
        target.mkdir(parents=True, exist_ok=True)
        return "[IO.Path]::GetFullPath('" + str(target).replace("'", "''") + "')"

    isolated = pattern.sub(replace, code)
    if re.search(r"\[(?:System\.)?Environment\]::GetFolderPath", isolated, re.I):
        raise ValueError("Unsupported native known-folder call; test not executed")
    return isolated


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--evidence", required=True)
    parser.add_argument("--watcher-receipt", required=True)
    parser.add_argument("command", nargs=argparse.REMAINDER)
    args = parser.parse_args()
    evidence = Path(args.evidence).resolve()
    if not evidence.is_relative_to(OWN_ROOT.resolve()):
        raise ValueError("Host evidence is outside the owned isolation root")
    watcher_path = Path(args.watcher_receipt).resolve()
    if not watcher_path.is_relative_to(OWN_ROOT.resolve()):
        raise ValueError("Host watcher receipt is outside the owned isolation root")
    watcher = json.loads(watcher_path.read_text(encoding="utf-8-sig"))
    if (not watcher.get("armed") or not watcher.get("recursive")
            or os.path.normcase(watcher.get("scope", "")) != os.path.normcase(str(HOST_ROOT))):
        raise ValueError("Recursive host watcher is not armed; test not executed")
    command = args.command[1:] if args.command[:1] == ["--"] else args.command
    sys.addaudithook(host_write_blocker())
    before = storage_snapshot(HOST_ROOT)
    unreadable = sum(bool(r.get("unreadable_children") or r.get("unreadable_contents")) for r in before.values())
    print("Host KMTech storage start: new=0 changed=0; recursive watcher armed; baseline entries="
          + str(len(before)) + " unreadable entries=" + str(unreadable), flush=True)
    result = subprocess.run(command, creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0), check=False)
    after = storage_snapshot(HOST_ROOT)
    changes = storage_changes(before, after)
    counts = {key: len(value) for key, value in changes.items()}
    evidence.write_text(json.dumps({"scope": str(HOST_ROOT), "before": before, "after": after,
        "changes": changes, "counts": counts, "command_exit": result.returncode,
        "unreadable_entries": unreadable, "recursive_watcher_receipt": str(watcher_path)}, indent=2), encoding="utf-8")
    print("Host KMTech storage end: " + json.dumps(counts), flush=True)
    return result.returncode if not any(counts.values()) else 1


if __name__ == "__main__":
    raise SystemExit(main())
