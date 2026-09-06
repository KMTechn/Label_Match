"""Read-only reconstruction of surviving M7 v1 consumers; see SPEC/PROVENANCE.

This is not the lost historical validator or an organizational approval authority.
Runtime dependencies: Python 3.11+ and Pillow (already used by all five clients).
"""
from __future__ import annotations

import argparse
from collections import Counter
from datetime import datetime
import hashlib
import json
import os
from pathlib import Path, PurePosixPath
import re
import stat
import sys
import warnings


SCHEMA = "M7 external capture bundle v1"
PLACEHOLDER = "미정 — 조직 확정 필요(Q1)"
# Deliberately literal: never import producer constants or trust caller descriptions.
STATES = {
    "Container_Audit": (
        "m7_phs2_preflight", "m7_central_preflight_queue", "m7_completion_busy",
        "m7_recovery_transition", "m7_direct_sync_backlog_ack", "m7_exact_good_membership",
        "m7_lease_fail_closed", "m7_transfer_receipt_status", "m7_partial_atomic_exchange",
    ),
    "Defect_Inspection": (
        "m7_scan_processing_input_disabled", "m7_saved_pending_last_save",
        "m7_converged_accepted_count_verify", "m7_central_reject_quarantine",
    ),
    "Inspection_worker": (
        "inspection-waiting", "inspection-normal", "inspection-f12", "inspection-residual",
        "inspection-overflow", "inspection-attachment", "remnant", "remnant-source",
        "remnant-f12", "remnant-attachment", "exchange", "inspection-completion",
        "inspection-scan-processing-busy-undo", "inspection-relay-operator-review",
        "inspection-relay-failed-permanent", "inspection-local-saved",
        "inspection-final-good-hold", "inspection-final-good-countdown",
    ),
    "Rework_worker": (
        "rework.processing", "rework.durable_pending", "rework.auto_recovery",
        "rework.review.auto_recovery", "rework.review.central_completion",
        "rework.save_failure", "rework.busy_rejection", "rework.recovery_init_hard_block",
        "rework.good_completion",
    ),
    "Label_Match": (
        "phs2_admitted_busy", "phs2_rejected_input_preserved", "f4_admitted_busy",
        "f4_rejected_input_preserved", "f3_admitted_busy", "f3_rejected_input_preserved",
        "central_submission_wait", "central_submission_conflict", "broken_fail_closed_warning",
    ),
}
TOOL_PATHS = {
    "Container_Audit": "tools/capture_container_operator_ui.py",
    "Defect_Inspection": "tools/capture_return_work_focus_ui.py",
    "Inspection_worker": "tools/capture_uiux_fullscreen.py",
    "Rework_worker": "scripts/capture_rework_process_uiux.py",
    "Label_Match": "tools/capture_label_operator_ui.py",
}
MAX_JSON = 2 * 1024 * 1024
MAX_IMAGE = 128 * 1024 * 1024
MAX_PIXELS = 64 * 1024 * 1024
MAX_ENTRIES = 10000
CAPTURE_KEYS = "state_id viewport dpi generated_at image_file image_sha256 state_manifest_file state_manifest_sha256"
STATE_KEYS = "schema app bundle_id state_id viewport dpi generated_at image_file image_sha256"
CORE_KEYS = "schema app app_source portable_artifact capture_tool captures"
APPROVAL_KEYS = "approver approval_receipt_file approval_receipt_sha256 custody_receipt_file custody_receipt_sha256"


class Invalid(Exception):
    def __init__(self, reason: str, subject: str, detail: str):
        self.reason, self.subject, self.detail = reason, subject, detail


def require(condition, reason, subject, detail):
    if not condition:
        raise Invalid(reason, subject, detail)


def shape(value, keys, subject, *, extension=False):
    required = set(keys.split())
    allowed = required | ({"app_specific"} if extension else set())
    require(isinstance(value, dict) and required <= value.keys() <= allowed,
            "SCHEMA_VIOLATION", subject, "Required fields or allowed field locations differ")
    if "app_specific" in value:
        require(isinstance(value["app_specific"], dict), "SCHEMA_VIOLATION", subject,
                "app_specific must be an object; it cannot authorize acceptance")


def equal(actual, expected, subject, reason="SEMANTIC_MISMATCH"):
    # JSON equality must not treat true as the numeric value 1.
    require(json.dumps(actual, sort_keys=True) == json.dumps(expected, sort_keys=True),
            reason, subject, "Bound values differ")


def hex_value(value, length, subject):
    require(isinstance(value, str) and re.fullmatch(r"[0-9a-f]{%d}" % length, value),
            "SCHEMA_VIOLATION", subject, "Expected lowercase hexadecimal identity")


def relative(value, subject):
    require(isinstance(value, str) and 0 < len(value) <= 1024,
            "PATH_INVALID", subject, "Expected a bounded relative POSIX path")
    parts = value.split("/")
    require(value == value.strip() and not any(p in ("", ".", "..") for p in parts)
            and not any(c in value for c in '\\:*?"<>|')
            and not any(ord(c) < 32 for c in value)
            and not any(p.endswith((".", " ")) for p in parts)
            and not any(re.fullmatch(r"(?i)(con|prn|aux|nul|com[1-9]|lpt[1-9])(?:\..*)?", p) for p in parts),
            "PATH_INVALID", subject, "Path is not a portable, unambiguous relative POSIX path")
    require(not any(PurePosixPath(p).stem.lower() == "latest" for p in parts),
            "MUTABLE_ALIAS_FORBIDDEN", subject, "latest aliases are not immutable evidence")
    return value


def safe_path(path, subject, *, directory=False):
    path = Path(os.path.abspath(path))
    current = Path(path.anchor)
    info = current.lstat()
    for part in path.parts[1:]:
        current /= part
        try:
            info = current.lstat()
        except FileNotFoundError:
            raise Invalid("FILE_MISSING", subject, "Required path does not exist") from None
        require(not stat.S_ISLNK(info.st_mode) and not (
            getattr(info, "st_file_attributes", 0) & 0x400),
            "SYMLINK_FORBIDDEN", subject, "Symlinks, junctions and reparse points are forbidden")
    require(path.is_dir() if directory else stat.S_ISREG(info.st_mode),
            "PATH_INVALID", subject, "Required path has the wrong file type")
    require(os.path.normcase(str(path.resolve())) == os.path.normcase(str(path)),
            "SYMLINK_FORBIDDEN", subject, "Lexical and resolved path identities differ")
    if not directory:
        require(info.st_nlink == 1, "HARDLINK_FORBIDDEN", subject, "Evidence must not have hardlink aliases")
    return path


def no_duplicates(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("duplicate JSON key")
        result[key] = value
    return result


def invalid_constant(_value):
    raise ValueError("non-finite JSON number")


def read_json(path, subject):
    path = safe_path(path, subject)
    require(path.stat().st_size <= MAX_JSON, "RESOURCE_LIMIT", subject, "JSON exceeds 2 MiB")
    with path.open("rb") as stream:
        payload = stream.read(MAX_JSON + 1)
    require(len(payload) <= MAX_JSON, "RESOURCE_LIMIT", subject, "JSON exceeds 2 MiB")
    try:
        document = json.loads(payload.decode("utf-8"), object_pairs_hook=no_duplicates,
                              parse_constant=invalid_constant)
    except (UnicodeError, ValueError, RecursionError):
        raise Invalid("SCHEMA_VIOLATION", subject, "Expected strict UTF-8 JSON without duplicate keys") from None
    return document, hashlib.sha256(payload).hexdigest()


def digest(path, subject):
    path = safe_path(path, subject)
    result = hashlib.sha256()
    with path.open("rb") as stream:
        before = os.fstat(stream.fileno())
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            result.update(block)
        after = os.fstat(stream.fileno())
    require((before.st_size, before.st_mtime_ns) == (after.st_size, after.st_mtime_ns),
            "INPUT_CHANGED", subject, "File changed during hashing")
    return result.hexdigest()


def png_dimensions(path, expected):
    subject = "capture.image"
    path = safe_path(path, subject)
    require(path.stat().st_size <= MAX_IMAGE, "RESOURCE_LIMIT", subject, "PNG exceeds 128 MiB")
    try:
        from PIL import Image
    except ImportError:
        raise Invalid("DEPENDENCY_MISSING", subject, "Pillow is required to decode PNG evidence") from None
    try:
        with warnings.catch_warnings():
            warnings.simplefilter("error")
            with path.open("rb") as stream:
                with Image.open(stream) as image:
                    require(image.format == "PNG", "IMAGE_INVALID", subject, "Image must be PNG")
                    require(image.width * image.height <= MAX_PIXELS, "RESOURCE_LIMIT", subject,
                            "PNG exceeds 64 megapixels")
                    equal(list(image.size), expected, subject, "IMAGE_DIMENSION_MISMATCH")
                    require(getattr(image, "n_frames", 1) == 1, "IMAGE_INVALID", subject,
                            "Capture must contain one PNG frame")
                    image.verify()
            with path.open("rb") as stream, Image.open(stream) as image:
                image.load()
    except Invalid:
        raise
    except (Image.DecompressionBombError, Image.DecompressionBombWarning):
        raise Invalid("RESOURCE_LIMIT", subject, "PNG exceeds decoder decompression limits") from None
    except (OSError, ValueError, SyntaxError, Warning):
        raise Invalid("IMAGE_INVALID", subject, "PNG verification or pixel decoding failed") from None


def timestamp(value, subject, *, compact=False):
    pattern = r"[0-9]{8}T[0-9]{6}Z" if compact else r"[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}(?:\.[0-9]+)?Z"
    require(isinstance(value, str) and re.fullmatch(pattern, value),
            "SCHEMA_VIOLATION", subject, "Expected UTC timestamp ending in Z")
    try:
        parsed = (datetime.strptime(value, "%Y%m%dT%H%M%SZ") if compact
                  else datetime.fromisoformat(value[:-1]))
    except ValueError:
        raise Invalid("SCHEMA_VIOLATION", subject, "Timestamp contains an impossible date or time") from None
    return parsed.replace(microsecond=0)


class Validator:
    def __init__(self, app):
        self.app = app
        self.checks = []

    def check(self, status, reason, subject, detail):
        # All callers use fixed descriptions; never echo supplied JSON or organization values.
        self.checks.append(dict(status=status, reason_code=reason, subject=subject[:160], detail=detail[:320]))

    def document(self, path, keys, subject, *, extension=False, bundle=None):
        doc, actual = read_json(path, subject)
        shape(doc, keys, subject, extension=extension)
        equal(doc["schema"], SCHEMA, subject, "SCHEMA_VIOLATION")
        if "app" in doc:
            equal(doc["app"], self.app, subject, "APP_MISMATCH")
        if bundle is not None:
            equal(doc["bundle_id"], bundle, subject, "BUNDLE_MISMATCH")
        return doc, actual

    def ref(self, value, subject):
        return self.root / relative(value, subject)

    def organization(self, value, subject):
        require(isinstance(value, str) and 0 < len(value) <= 1024 and value == value.strip(),
                "SCHEMA_VIOLATION", subject, "Organization field must be a nonempty bounded string")
        if value == PLACEHOLDER:
            self.check("APPROVAL_PENDING", "ORGANIZATION_PLACEHOLDER", subject,
                       "Organization must resolve this supplied placeholder")

    def scan(self):
        # Inspect links without traversing them; limit ordinary tree traversal too.
        pending = [self.root]
        count = 0
        while pending:
            with os.scandir(pending.pop()) as entries:
                for entry in entries:
                    count += 1
                    require(count <= MAX_ENTRIES, "RESOURCE_LIMIT", "evidence-root", "Tree exceeds 10000 entries")
                    relative(Path(entry.path).relative_to(self.root).as_posix(), "evidence-root")
                    info = entry.stat(follow_symlinks=False)
                    require(not stat.S_ISLNK(info.st_mode) and not (getattr(info, "st_file_attributes", 0) & 0x400),
                            "SYMLINK_FORBIDDEN", "evidence-root", "Tree contains a link or reparse point")
                    if stat.S_ISDIR(info.st_mode):
                        pending.append(Path(entry.path))
                    else:
                        require(stat.S_ISREG(info.st_mode), "PATH_INVALID", "evidence-root", "Tree contains a special file")

    def run(self, supplied, described=None):
        require(self.app in STATES, "APP_MISMATCH", "--app", "App is not in the fixed five-client registry")
        candidate = Path(os.path.abspath(supplied))
        # Check ancestors even before resolving or enumerating directory input.
        if candidate.is_dir():
            self.root = safe_path(candidate, "evidence-root", directory=True)
            self.scan()
            indexes = safe_path(self.root / "indexes", "indexes", directory=True)
            candidates = list(indexes.glob("handover-index__*.json"))
            require(len(candidates) == 1, "INDEX_AMBIGUOUS", "indexes", "Directory input requires exactly one immutable index")
            candidate = candidates[0]
        else:
            safe_path(candidate, "index")
            require(candidate.parent.name == "indexes", "PATH_INVALID", "index", "Explicit index must be under root/indexes")
            self.root = safe_path(candidate.parent.parent, "evidence-root", directory=True)
            self.scan()
        match = re.fullmatch(r"handover-index__([0-9]{8}T[0-9]{6}Z)__[0-9a-f]{8}\.json", candidate.name)
        require(match is not None, "PATH_INVALID", "index", "Index filename must encode UTC and lowercase nonce")
        timestamp(match.group(1), "index", compact=True)
        index, _ = self.document(candidate, "schema manifests", "index")
        require(isinstance(index["manifests"], list) and 0 < len(index["manifests"]) <= 100,
                "SCHEMA_VIOLATION", "index", "Expected 1 to 100 manifest entries")
        paths, chosen = set(), []
        for entry in index["manifests"]:
            shape(entry, "app manifest_file manifest_sha256", "index.entry")
            require(isinstance(entry["app"], str) and entry["app"] in STATES,
                    "APP_MISMATCH", "index.entry", "Index app is not registered")
            path = relative(entry["manifest_file"], "index.entry")
            hex_value(entry["manifest_sha256"], 64, "index.entry")
            require(path.casefold() not in paths, "INDEX_AMBIGUOUS", "index", "Duplicate manifest reference")
            paths.add(path.casefold())
            require(path.startswith(entry["app"] + "/") and path.endswith("/manifest.json") and len(path.split("/")) == 3,
                    "PATH_INVALID", "index.entry", "Manifest must be root/app/bundle/manifest.json")
            if entry["app"] == self.app:
                chosen.append(entry)
        require(len(chosen) == 1, "INDEX_AMBIGUOUS", "index", "Index requires exactly one manifest for requested app")
        entry = chosen[0]
        manifest_path = self.ref(entry["manifest_file"], "manifest")
        manifest, actual = self.document(manifest_path, CORE_KEYS + " approval", "manifest", extension=True)
        equal(actual, entry["manifest_sha256"], "manifest", "MANIFEST_DIGEST_MISMATCH")
        self.check("PASS", "SCHEMA_MATCH", self.app, "Manifest uses the surviving v1 envelope")
        source, tool, artifact = (manifest[key] for key in ("app_source", "capture_tool", "portable_artifact"))
        shape(source, "commit tree", "app_source")
        for key in ("commit", "tree"):
            hex_value(source[key], 40, "app_source." + key)
        shape(tool, "path commit blob_sha256", "capture_tool")
        relative(tool["path"], "capture_tool.path")
        equal(tool["path"], TOOL_PATHS[self.app], "capture_tool.path", "TOOL_PATH_MISMATCH")
        hex_value(tool["commit"], 40, "capture_tool.commit")
        hex_value(tool["blob_sha256"], 64, "capture_tool.blob_sha256")
        shape(artifact, "file sha256", "portable_artifact")
        hex_value(artifact["sha256"], 64, "portable_artifact.sha256")
        equal(digest(self.ref(artifact["file"], "portable_artifact.file"), "portable_artifact"),
              artifact["sha256"], "portable_artifact", "ARTIFACT_DIGEST_MISMATCH")
        bundle = manifest_path.parent.name
        match = re.fullmatch(re.escape(self.app + "__" + source["commit"][:12]) +
                             r"__([0-9]{8}T[0-9]{6}Z)__[0-9a-f]{8}", bundle)
        require(match is not None, "BUNDLE_MISMATCH", "bundle", "Bundle must bind app, source commit prefix, UTC and nonce")
        bundle_time = timestamp(match.group(1), "bundle", compact=True)
        prefix = self.app + "/" + bundle
        approval = manifest["approval"]
        shape(approval, APPROVAL_KEYS, "manifest.approval")
        approval_path = prefix + "/approval/approval-receipt.json"
        custody_path = prefix + "/approval/custody-receipt.json"
        set_path = prefix + "/capture-set.json"
        for key, expected in (("approval_receipt_file", approval_path), ("custody_receipt_file", custody_path)):
            relative(approval[key], "manifest.approval." + key)
            equal(approval[key], expected, "manifest.approval." + key, "PATH_BINDING_MISMATCH")
        approved, approval_hash = self.document(self.ref(approval_path, "approval"),
            "schema app bundle_id capture_set_file capture_set_sha256 approver", "approval-receipt", bundle=bundle)
        custody, custody_hash = self.document(self.ref(custody_path, "custody"),
            "schema app bundle_id capture_set_file capture_set_sha256 approval_receipt_file approval_receipt_sha256 custodian custody_location retention_period",
            "custody-receipt", bundle=bundle)
        for key, actual in (("approval_receipt_sha256", approval_hash), ("custody_receipt_sha256", custody_hash)):
            hex_value(approval[key], 64, "manifest.approval." + key)
            equal(approval[key], actual, "manifest.approval." + key, "RECEIPT_DIGEST_MISMATCH")
        equal(custody["approval_receipt_file"], approval_path, "custody.approval_receipt_file", "PATH_BINDING_MISMATCH")
        hex_value(custody["approval_receipt_sha256"], 64, "custody.approval_receipt_sha256")
        equal(custody["approval_receipt_sha256"], approval_hash, "custody", "RECEIPT_DIGEST_MISMATCH")
        equal(approval["approver"], approved["approver"], "approval.approver")
        self.organization(approval["approver"], "manifest.approval.approver")
        self.organization(approved["approver"], "approval-receipt.approver")
        for key in ("custodian", "custody_location", "retention_period"):
            self.organization(custody[key], "custody-receipt." + key)
        capture_set, set_hash = self.document(self.ref(set_path, "capture-set"),
            CORE_KEYS + " bundle_id", "capture-set", extension=True, bundle=bundle)
        for doc, subject in ((approved, "approval"), (custody, "custody")):
            relative(doc["capture_set_file"], subject + ".capture_set_file")
            equal(doc["capture_set_file"], set_path, subject, "PATH_BINDING_MISMATCH")
            hex_value(doc["capture_set_sha256"], 64, subject + ".capture_set_sha256")
            equal(doc["capture_set_sha256"], set_hash, subject, "CAPTURE_SET_DIGEST_MISMATCH")
        captures = manifest["captures"]
        require(isinstance(captures, list) and len(captures) <= 100,
                "SCHEMA_VIOLATION", "captures", "Expected bounded capture array")
        for row in captures:
            shape(row, CAPTURE_KEYS, "capture")
            require(isinstance(row["state_id"], str), "SCHEMA_VIOLATION", "capture.state_id", "State ID must be a string")
        ids = [row["state_id"] for row in captures]
        require(len(ids) == len(set(ids)), "STATE_DUPLICATE", "captures", "State IDs must be unique")
        require(not (set(ids) - set(STATES[self.app])), "STATE_EXTRA", "captures", "Unexpected state ID")
        require(set(ids) == set(STATES[self.app]), "STATE_MISSING", "captures", "Required state ID missing")
        expected_files = {"manifest.json", "capture-set.json", "approval/approval-receipt.json", "approval/custody-receipt.json"}
        for row in captures:
            shape(row["viewport"], "width_px height_px", "capture.viewport")
            width, height = (row["viewport"][key] for key in ("width_px", "height_px"))
            require(all(type(n) is int and 0 < n <= 65535 for n in (width, height, row["dpi"])),
                    "SCHEMA_VIOLATION", "capture.viewport/dpi", "Dimensions and DPI must be positive integers")
            require(timestamp(row["generated_at"], "capture.generated_at") == bundle_time,
                    "TIMESTAMP_MISMATCH", "capture.generated_at", "Capture UTC second must match bundle")
            stem = f'{row["state_id"]}__{width}x{height}__{row["dpi"]}dpi'
            for key, expected in (("image_file", prefix + "/captures/" + stem + ".png"),
                                  ("state_manifest_file", prefix + "/states/" + stem + ".json")):
                relative(row[key], "capture." + key)
                equal(row[key], expected, "capture." + key, "PATH_BINDING_MISMATCH")
                expected_files.add(expected[len(prefix) + 1:])
            image_path = self.ref(row["image_file"], "capture.image")
            hex_value(row["image_sha256"], 64, "capture.image_sha256")
            equal(digest(image_path, "capture.image"), row["image_sha256"], "capture.image", "IMAGE_DIGEST_MISMATCH")
            png_dimensions(image_path, [width, height])
            state, state_hash = self.document(self.ref(row["state_manifest_file"], "state"),
                STATE_KEYS, "state", extension=True, bundle=bundle)
            hex_value(row["state_manifest_sha256"], 64, "capture.state_manifest_sha256")
            equal(state_hash, row["state_manifest_sha256"], "state", "STATE_DIGEST_MISMATCH")
            for key in "state_id viewport dpi generated_at image_file image_sha256".split():
                equal(state[key], row[key], "state." + key)
        for key in CORE_KEYS.split() + ["app_specific"]:
            require((key in capture_set) == (key in manifest), "SEMANTIC_MISMATCH", "capture-set." + key, "Field presence differs")
            equal(capture_set.get(key), manifest.get(key), "capture-set." + key)
        actual_files = set()
        actual_dirs = set()
        for current, dirs, files in os.walk(manifest_path.parent, followlinks=False):
            base = Path(current)
            actual_files.update((base / name).relative_to(manifest_path.parent).as_posix() for name in files)
            actual_dirs.update((base / name).relative_to(manifest_path.parent).as_posix() for name in dirs)
        equal(sorted(actual_files), sorted(expected_files), "bundle.files", "TOPOLOGY_MISMATCH")
        equal(sorted(actual_dirs), ["approval", "captures", "states"], "bundle.directories", "TOPOLOGY_MISMATCH")
        self.check("PASS", "BUNDLE_BINDINGS_MATCH", self.app, "All required states, byte hashes, PNGs and document bindings agree")
        if described is not None:
            described_doc, _ = self.document(described, "schema app required_state_ids", "--describe-json", extension=True)
            listed = described_doc["required_state_ids"]
            require(isinstance(listed, list) and all(isinstance(v, str) for v in listed),
                    "SCHEMA_VIOLATION", "--describe-json", "Expected array of state strings")
            equal(sorted(listed), sorted(STATES[self.app]), "--describe-json", "DESCRIBE_MISMATCH")
            self.check("PASS", "SCHEMA_MATCH", "--describe-json", "Description uses the surviving v1 envelope")
            self.check("PASS", "DESCRIBE_REQUIRED_STATES_COMPLETE", "--describe-json",
                       f"Description contains all {len(STATES[self.app])} canonical state IDs")

    def report(self):
        summary = dict.fromkeys(("PASS", "FAIL", "APPROVAL_PENDING"), 0)
        summary.update(Counter(check["status"] for check in self.checks))
        code = 2 if summary["FAIL"] else 3 if summary["APPROVAL_PENDING"] else 0
        return dict(result={0: "PASS", 2: "FAIL", 3: "APPROVAL_PENDING"}[code],
                    exit_code=code, summary=summary, checks=self.checks)


class Parser(argparse.ArgumentParser):
    def error(self, message):
        raise Invalid("CLI_INVALID", "arguments", "Expected root-or-index --app APP [--describe-json FILE]")


def main(argv=None):
    validator = Validator(None)
    try:
        parser = Parser(description=__doc__)
        parser.add_argument("bundle")
        parser.add_argument("--app", required=True)
        parser.add_argument("--describe-json")
        args = parser.parse_args(argv)
        validator.app = args.app
        validator.run(args.bundle, args.describe_json)
    except Invalid as exc:
        validator.check("FAIL", exc.reason, exc.subject, exc.detail)
    except (OSError, ValueError, RecursionError):
        validator.check("FAIL", "INPUT_UNREADABLE", "input", "Input could not be safely read or processed")
    report = validator.report()
    print(json.dumps(report, ensure_ascii=True, separators=(",", ":")))
    return report["exit_code"]


if __name__ == "__main__":
    raise SystemExit(main())
