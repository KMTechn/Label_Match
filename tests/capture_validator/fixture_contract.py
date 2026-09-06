"""Independent tiny synthetic fixtures from SPEC; no producer/validator imports."""
from copy import deepcopy
from dataclasses import dataclass
import hashlib
import json
from pathlib import Path
import struct
import zlib

SCHEMA = "M7 external capture bundle v1"
PENDING = "미정 — 조직 확정 필요(Q1)"
REGISTRY = {
    "Container_Audit": ["m7_phs2_preflight", "m7_central_preflight_queue", "m7_completion_busy", "m7_recovery_transition", "m7_direct_sync_backlog_ack", "m7_exact_good_membership", "m7_lease_fail_closed", "m7_transfer_receipt_status", "m7_partial_atomic_exchange"],
    "Defect_Inspection": ["m7_scan_processing_input_disabled", "m7_saved_pending_last_save", "m7_converged_accepted_count_verify", "m7_central_reject_quarantine"],
    "Inspection_worker": ["inspection-waiting", "inspection-normal", "inspection-f12", "inspection-residual", "inspection-overflow", "inspection-attachment", "remnant", "remnant-source", "remnant-f12", "remnant-attachment", "exchange", "inspection-completion", "inspection-scan-processing-busy-undo", "inspection-relay-operator-review", "inspection-relay-failed-permanent", "inspection-local-saved", "inspection-final-good-hold", "inspection-final-good-countdown"],
    "Rework_worker": ["rework.processing", "rework.durable_pending", "rework.auto_recovery", "rework.review.auto_recovery", "rework.review.central_completion", "rework.save_failure", "rework.busy_rejection", "rework.recovery_init_hard_block", "rework.good_completion"],
    "Label_Match": ["phs2_admitted_busy", "phs2_rejected_input_preserved", "f4_admitted_busy", "f4_rejected_input_preserved", "f3_admitted_busy", "f3_rejected_input_preserved", "central_submission_wait", "central_submission_conflict", "broken_fail_closed_warning"],
}
TOOLS = {
    "Container_Audit": "tools/capture_container_operator_ui.py",
    "Defect_Inspection": "tools/capture_return_work_focus_ui.py",
    "Inspection_worker": "tools/capture_uiux_fullscreen.py",
    "Rework_worker": "scripts/capture_rework_process_uiux.py",
    "Label_Match": "tools/capture_label_operator_ui.py",
}


def sha(path):
    # Fixture files are tiny; independent from the validator's streaming reader.
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def write_json(path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def write_png(path, width=2, height=1):
    # No binary fixture payload is embedded in source or emitted to stdout.
    def chunk(kind, payload):
        return struct.pack(">I", len(payload)) + kind + payload + struct.pack(">I", zlib.crc32(kind + payload) & 0xffffffff)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("wb") as stream:
        stream.write(bytes((137, 80, 78, 71, 13, 10, 26, 10)))
        stream.write(chunk(b"IHDR", struct.pack(">IIBBBBB", width, height, 8, 2, 0, 0, 0)))
        stream.write(chunk(b"IDAT", zlib.compress((bytes((0,)) + bytes((30, 100, 180)) * width) * height)))
        stream.write(chunk(b"IEND", b""))


@dataclass
class Bundle:
    root: Path
    app: str
    manifest: dict
    capture_set: dict
    approval: dict
    custody: dict
    index: dict
    states: list

    @property
    def prefix(self):
        return self.app + "/" + self.capture_set["bundle_id"]

    @property
    def index_path(self):
        return self.root / "indexes/handover-index__20260903T010204Z__87654321.json"

    @property
    def manifest_path(self):
        return self.root / self.index["manifests"][0]["manifest_file"]

    def seal(self):
        """Rehash the original graph only; never repair duplicated semantic fields."""
        for path, document in self.states:
            write_json(self.root / path, document)
            for container in (self.manifest, self.capture_set):
                for row in container["captures"]:
                    if row["state_manifest_file"] == path:
                        row["state_manifest_sha256"] = sha(self.root / path)
        set_path = self.root / self.prefix / "capture-set.json"
        write_json(set_path, self.capture_set)
        for document in (self.approval, self.custody):
            document["capture_set_sha256"] = sha(set_path)
        approval_path = self.root / self.prefix / "approval/approval-receipt.json"
        write_json(approval_path, self.approval)
        self.custody["approval_receipt_sha256"] = sha(approval_path)
        custody_path = self.root / self.prefix / "approval/custody-receipt.json"
        write_json(custody_path, self.custody)
        self.manifest["approval"]["approval_receipt_sha256"] = sha(approval_path)
        self.manifest["approval"]["custody_receipt_sha256"] = sha(custody_path)
        write_json(self.manifest_path, self.manifest)
        self.index["manifests"][0]["manifest_sha256"] = sha(self.manifest_path)
        write_json(self.index_path, self.index)

    def describe(self):
        path = self.root.parent / "describe.json"
        write_json(path, dict(schema=SCHEMA, app=self.app, required_state_ids=REGISTRY[self.app]))
        return path


def make_bundle(parent, app="Label_Match", pending=False):
    root = parent / "evidence"
    bundle_id = app + "__0123456789ab__20260903T010203Z__1234abcd"
    prefix = app + "/" + bundle_id
    artifact = root / "artifacts/portable.bin"
    artifact.parent.mkdir(parents=True)
    artifact.write_text("Independent inert artifact; no executable behavior\n", encoding="ascii")
    captures, states = [], []
    for state_id in REGISTRY[app]:
        image_file = prefix + "/captures/" + state_id + "__2x1__96dpi.png"
        state_file = prefix + "/states/" + state_id + "__2x1__96dpi.json"
        write_png(root / image_file)
        row = dict(state_id=state_id, viewport=dict(width_px=2, height_px=1), dpi=96,
                   generated_at="2026-09-03T01:02:03.125Z", image_file=image_file,
                   image_sha256=sha(root / image_file))
        states.append((state_file, dict(schema=SCHEMA, app=app, bundle_id=bundle_id, **deepcopy(row))))
        captures.append(dict(**row, state_manifest_file=state_file, state_manifest_sha256="0" * 64))
    organization = PENDING if pending else "Synthetic organization value"
    approval = dict(schema=SCHEMA, app=app, bundle_id=bundle_id,
                    capture_set_file=prefix + "/capture-set.json", capture_set_sha256="0" * 64,
                    approver=organization)
    custody = dict(schema=SCHEMA, app=app, bundle_id=bundle_id,
                   capture_set_file=prefix + "/capture-set.json", capture_set_sha256="0" * 64,
                   approval_receipt_file=prefix + "/approval/approval-receipt.json",
                   approval_receipt_sha256="0" * 64, custodian=organization,
                   custody_location=organization, retention_period=organization)
    manifest = dict(schema=SCHEMA, app=app,
                    app_source=dict(commit="0123456789abcdef0123456789abcdef01234567", tree="b" * 40),
                    portable_artifact=dict(file="artifacts/portable.bin", sha256=sha(artifact)),
                    capture_tool=dict(path=TOOLS[app], commit="c" * 40, blob_sha256="d" * 64),
                    captures=captures)
    capture_set = dict(**deepcopy(manifest), bundle_id=bundle_id)
    manifest["approval"] = dict(approver=organization,
                               approval_receipt_file=prefix + "/approval/approval-receipt.json",
                               approval_receipt_sha256="0" * 64,
                               custody_receipt_file=prefix + "/approval/custody-receipt.json",
                               custody_receipt_sha256="0" * 64)
    index = dict(schema=SCHEMA, manifests=[dict(app=app, manifest_file=prefix + "/manifest.json", manifest_sha256="0" * 64)])
    fixture = Bundle(root, app, manifest, capture_set, approval, custody, index, states)
    fixture.seal()
    return fixture
