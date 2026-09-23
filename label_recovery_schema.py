"""Shapes accepted at the durable package recovery boundary.

The same rules are used before writing and after reading.  Older records may
omit optional fields, but a field that is present must retain its JSON type.
"""

from __future__ import annotations

from datetime import datetime
from pathlib import Path
import math
import re


_TEXT = "text"
_INTEGER = "integer"
_BOOLEAN = "boolean"
_OBJECT = "object"
_TEXT_LIST = "text_list"
_TIME = "time"
_NULLABLE_TIME = "nullable_time"
_NULLABLE_TEXT = "nullable_text"
_NULLABLE_OBJECT = "nullable_object"
_RENDERED_PATH = "rendered_path"

RECOVERY_SCHEMAS = {
    "current": {
        "required": {"current_set_info": "current_set"},
        "optional": {"timestamp": _TIME, "worker_name": _TEXT},
    },
    "current_set": {
        "required": {"id": _NULLABLE_TEXT, "raw": _TEXT_LIST},
        "optional": {
            "parsed": _TEXT_LIST, "start_time": _NULLABLE_TIME,
            "exact_rescan_active": _BOOLEAN,
            "exact_rescan_complete": _BOOLEAN,
            "exact_rescan_target_count": _INTEGER,
            "exact_rescan_barcodes": _TEXT_LIST,
            "canonical_input_tag_qr": _TEXT,
            "physical_scanned_qr_payload": _TEXT,
            "active_label_qr_payload": _TEXT,
            "sealed_transfer": _NULLABLE_OBJECT,
            "package_source_snapshot": "nullable_source_snapshot",
            "central_inherit_all": _BOOLEAN,
            "error_count": _INTEGER,
            "has_error_or_reset": _BOOLEAN,
            "active_label_version": _INTEGER,
            "active_membership_version": _INTEGER,
            "operation_lease_fence": _INTEGER,
            "phase": _NULLABLE_TEXT,
            "item_name_override": _NULLABLE_TEXT,
            "production_date": _NULLABLE_TEXT,
            "active_label_id": _TEXT,
            "active_label_business_date": _TEXT,
            "active_label_worker_code": _TEXT,
            "active_label_instruction_id": _TEXT,
            "active_label_resolution": _TEXT,
            "phs_label_replaced_scan": _BOOLEAN,
            "phs_label_guidance": _TEXT,
            "resolved_transfer_bundle_id": _TEXT,
            "deferred_intent_id": _TEXT,
            "package_submission_idempotency_key": _TEXT,
            "package_submission_status": _TEXT,
            "operation_lease_id": _TEXT,
            "operation_lease_snapshot_hash": _TEXT,
            "operation_lease_expires_at": _TEXT,
            "operation_lease_completed_at": _TEXT,
            "sealed_transfer_exchange_intent_id": _TEXT,
            "exact_rescan_source_bundle_id": _TEXT,
            "recovery_operator_review": _BOOLEAN,
            "restored_from_package_outbox": _BOOLEAN,
        },
    },
    "draft": {
        "required": {"set_id": _TEXT, "membership_mode": _TEXT},
        "optional": {
            "source_input_tag_id": _TEXT,
            "source_input_tag_label_id": _TEXT,
            "source_input_tag_hash_prefix": _TEXT,
            "source_canonical_input_tag_qr": _TEXT,
            "source_active_label_qr_payload": _TEXT,
            "source_active_label_version": _INTEGER,
            "source_active_membership_version": _INTEGER,
            "expected_member_count": _INTEGER,
            "expected_authority_epoch": _INTEGER,
            "expected_plane_epoch": _INTEGER,
            "expected_seal_revision": _INTEGER,
            "operation_lease_fence": _INTEGER,
            "sample_barcodes": _TEXT_LIST,
            "exact_rescan_barcodes": _TEXT_LIST,
            "source_session_ids": _TEXT_LIST,
            "phs_work_group": _OBJECT,
            "work_group_source": _OBJECT,
            "item_code": _TEXT,
            "source_bundle_id": _TEXT,
            "source_external_label": _TEXT,
            "source_bundle_hint": _TEXT,
            "source_authority_scope_id": _TEXT,
            "expected_membership_hash": _TEXT,
            "expected_ledger_plane": _TEXT,
            "package_bundle_id": _TEXT,
            "external_label": _TEXT,
            "source_active_label_business_date": _TEXT,
            "source_active_label_worker_code": _TEXT,
            "source_active_label_instruction_id": _TEXT,
            "expected_seal_id": _TEXT,
            "expected_seal_token": _TEXT,
            "expected_seal_qr_payload": _TEXT,
            "source_resolution_basis": _TEXT,
            "operation_lease_id": _TEXT,
            "operation_lease_token": _TEXT,
            "operation_lease_snapshot_hash": _TEXT,
            "operation_lease_completed_at": _TEXT,
        },
    },
    "journal": {
        "required": {"schema_version": _TEXT, "state": "journal_state"},
        "optional": {},
    },
    "journal_state": {
        "required": {},
        "optional": {
            "status": _TEXT, "workflow_mode": _TEXT,
            "set_id": _TEXT, "scan_payload": _TEXT,
            "canonical_input_tag_qr": _TEXT,
            "source_label_id": _TEXT, "active_scan_label_id": _TEXT,
            "exchange_id": _TEXT, "input_tag_id": _TEXT,
            "authority_scope_id": _TEXT, "prepare_idempotency_key": _TEXT,
            "source_label_version": _INTEGER,
            "source_membership_version": _INTEGER,
            "print_attempt_no": _INTEGER,
            "expected_reconciliation_version": _INTEGER,
            "exchange_entity_version": _INTEGER,
            "target_prints": "target_prints",
            "action_resolution": "action_resolution",
            "render_context": "render_context",
            "target_instruction": "target_instruction",
            "target_label": _OBJECT,
            "target_labels": "object_list",
            "exchange_items": "object_list",
            "process_context": _TEXT,
            "prepare_ack": _OBJECT,
            "committed_ack": _OBJECT,
            "print_request_ack": _OBJECT,
            "print_complete_ack": _OBJECT,
            "print_failure_ack": _OBJECT,
            "local_print_proof": _OBJECT,
            "action_ids": _TEXT_LIST,
            "reconciliation_id": _TEXT,
            "failed_target_label_id": _TEXT,
            "target_instruction_id": _TEXT,
            "source_membership_hash": _TEXT,
            "print_attempt_id": _TEXT,
            "print_idempotency_key": _TEXT,
            "print_error_code": _TEXT,
            "print_error_message": _TEXT,
            "rendered_artifact_hash": _TEXT,
            "rendered_path": _RENDERED_PATH,
            "updated_at": _TIME,
        },
    },
    "source_snapshot": {
        "required": {},
        "optional": {
            "bundle_id": _TEXT, "authority_scope_id": _TEXT,
            "member_count": _INTEGER, "membership_hash": _TEXT,
            "authority_epoch": _INTEGER, "ledger_plane": _TEXT,
            "plane_epoch": _INTEGER, "entity_version": _INTEGER,
            "source_resolution_basis": _TEXT, "phs_work_group": _OBJECT,
        },
    },
    "target_print_state": {
        "required": {},
        "optional": {
            "status": _TEXT, "attempt_no": _INTEGER,
            "print_idempotency_key": _TEXT, "print_attempt_id": _TEXT,
            "print_request_ack": _OBJECT, "print_complete_ack": _OBJECT,
            "print_failure_ack": _OBJECT, "local_print_proof": _OBJECT,
            "rendered_path": _RENDERED_PATH, "rendered_artifact_hash": _TEXT,
            "print_error_code": _TEXT, "print_error_message": _TEXT,
        },
    },
    "action_resolution": {
        "required": {},
        "optional": {
            "selection": "action_selection", "scan": "action_scan",
            "actions": "action_list", "reconciliation": _OBJECT,
            "authority_scope_id": _TEXT, "scan_payload": _TEXT,
        },
    },
    "action_selection": {
        "required": {"mode": _TEXT, "reconciliation_id": _TEXT,
                     "action_ids": _TEXT_LIST,
                     "expected_reconciliation_version": _INTEGER},
        "optional": {},
    },
    "action_scan": {
        "required": {},
        "optional": {"resolution": _TEXT, "scanned_label_id": _TEXT,
                     "active_label_id": _TEXT, "replacement_required": _BOOLEAN,
                     "active_qr_payload": _TEXT},
    },
    "action_item": {
        "required": {"action_id": _TEXT, "sources": "source_list",
                     "targets": "target_list"},
        "optional": {"action_index": _INTEGER, "action_type": _TEXT,
                     "action_state": _TEXT, "exchange_id": "nullable_string",
                     "item_id": _TEXT, "before_qty_pcs": _INTEGER,
                     "after_qty_pcs": _INTEGER,
                     "source_member_union_count": _INTEGER,
                     "source_member_union_hash": _TEXT,
                     "source_member_ids": _TEXT_LIST,
                     "split_member_ids_by_target": _OBJECT,
                     "process_membership": "object_list", "display": _OBJECT},
    },
    "action_source": {
        "required": {"source_label_id": _TEXT, "member_ids": _TEXT_LIST,
                     "qr_payload": _TEXT},
        "optional": {"group_id": _TEXT, "instruction_id": _TEXT,
                     "business_date": "date", "item_id": _TEXT,
                     "display_item_code": _TEXT, "item_daily_ordinal": _INTEGER,
                     "worker_code": _TEXT, "qty_pcs": _INTEGER,
                     "label_version": _INTEGER, "membership_version": _INTEGER,
                     "membership_hash": _TEXT},
    },
    "action_target": {
        "required": {"instruction_id": _TEXT, "business_date": "date"},
        "optional": {"item_id": _TEXT, "display_item_code": _TEXT,
                     "item_daily_ordinal": _INTEGER, "worker_code": _TEXT,
                     "qty_pcs": _INTEGER},
    },
    "render_context": {
        "required": {},
        "optional": {
            "parsed": _TEXT_LIST, "item_name_override": _TEXT,
            "package_source_snapshot": _OBJECT,
        },
    },
    "target_instruction": {
        "required": {},
        "optional": {
            "instruction_id": _TEXT, "business_date": _TEXT,
            "item_id": _TEXT, "uom": _TEXT, "target_qty_pcs": _INTEGER,
            "item_daily_ordinal": _INTEGER, "worker_code": _TEXT,
            "entity_version": _INTEGER, "state": _TEXT,
        },
    },
}

_PHS2 = re.compile(
    r"PHS=2\|SRC=KMTECH_INPUT_TAG\|ITG=([^\s|=\x00-\x1f\x7f]+)\|"
    r"CLC=[^\s|=\x00-\x1f\x7f]+\|LBL=([^\s|=\x00-\x1f\x7f]+)\|"
    r"HSH=[0-9a-fA-F]{16}"
)
_PHS2_FIELDS = {
    "current_set": {"canonical_input_tag_qr", "physical_scanned_qr_payload",
                    "active_label_qr_payload"},
    "draft": {"source_canonical_input_tag_qr", "source_active_label_qr_payload"},
    "journal_state": {"canonical_input_tag_qr", "scan_payload"},
    "action_resolution": {"scan_payload"},
    "action_scan": {"active_qr_payload"},
    "action_source": {"qr_payload"},
}
_ITG_FIELDS = {"current_set": "input_tag_id", "draft": "source_input_tag_id",
               "journal_state": "input_tag_id"}
_DATETIME = re.compile(
    r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d{1,6})?(?:Z|[+-]\d{2}:\d{2})?$"
)
_DATE = re.compile(r"^\d{4}-\d{2}-\d{2}$")
_ARCHIVE_BASE = re.compile(r"[A-Za-z0-9._-]{1,170}")
_RENDERED_NAME = re.compile(r"[A-Za-z0-9._-]{1,120}\.png")
_RESERVED = {"CON", "PRN", "AUX", "NUL", *(f"COM{i}" for i in range(1, 10)),
             *(f"LPT{i}" for i in range(1, 10))}


def valid_recovery_business_date(value):
    if not isinstance(value, str) or not _DATE.fullmatch(value):
        return False
    try:
        datetime.fromisoformat(value)
    except ValueError:
        return False
    return True


def valid_recovery_archive_path(value, digest):
    """Accept only a local absolute archive basename bound to its digest."""
    if (not isinstance(value, str) or "\x00" in value
            or not isinstance(digest, str) or not re.fullmatch(r"[0-9a-f]{64}", digest)):
        return False
    path = Path(value)
    suffix = ".held-" + digest
    if not path.is_absolute() or ".." in path.parts or not path.name.endswith(suffix):
        return False
    base = path.name[:-len(suffix)]
    return bool(_ARCHIVE_BASE.fullmatch(base) and base.split(".")[0].upper() not in _RESERVED)


def valid_recovery_rendered_path(value):
    """Match the path shape produced by PHSLabelRenderer.render."""
    if not isinstance(value, str) or "\x00" in value or len(value) > 1024:
        return False
    path = Path(value)
    if not path.is_absolute() or ".." in path.parts:
        return False
    if path.parent.name != "phs_label_exchange" or not _valid("date", path.parent.parent.name):
        return False
    if not _RENDERED_NAME.fullmatch(path.name):
        return False
    base = path.name[:-4]
    return base not in {".", ".."} and base.split(".")[0].upper() not in _RESERVED


def _valid(rule, value):
    if rule == _RENDERED_PATH:
        return valid_recovery_rendered_path(value)
    if rule in RECOVERY_SCHEMAS:
        return validate_recovery_record(rule, value)
    if rule == "nullable_source_snapshot":
        return value is None or validate_recovery_record("source_snapshot", value)
    if rule == "target_prints":
        return (isinstance(value, dict) and all(
            _valid(_TEXT, key) and validate_recovery_record("target_print_state", item)
            for key, item in value.items()
        ))
    if rule in {"action_list", "source_list", "target_list"}:
        kind = {"action_list": "action_item", "source_list": "action_source",
                "target_list": "action_target"}[rule]
        return isinstance(value, list) and all(validate_recovery_record(kind, item)
                                               for item in value)
    if rule == "nullable_string":
        return value is None or _valid(_TEXT, value)
    if rule == "date":
        return valid_recovery_business_date(value)
    if rule == _TEXT:
        return isinstance(value, str) and "\x00" not in value
    if rule == _NULLABLE_TEXT:
        return value is None or type(value) in (str, int) and type(value) is not bool
    if rule == _INTEGER:
        return type(value) is int and value >= 0
    if rule == _BOOLEAN:
        return type(value) is bool
    if rule == _OBJECT:
        return isinstance(value, dict) and _json_tree_valid(value)
    if rule == _NULLABLE_OBJECT:
        return value is None or _valid(_OBJECT, value)
    if rule == _TEXT_LIST:
        return isinstance(value, list) and all(_valid(_TEXT, item) for item in value)
    if rule == "object_list":
        return isinstance(value, list) and all(_valid(_OBJECT, item) for item in value)
    if rule in (_TIME, _NULLABLE_TIME):
        if value is None and rule == _NULLABLE_TIME:
            return True
        if not _valid(_TEXT, value) or not _DATETIME.fullmatch(value):
            return False
        try:
            datetime.fromisoformat(value)
        except ValueError:
            return False
        return True
    return False


def _json_tree_valid(value):
    """Reject NUL/non-string keys without recursive Python traversal."""
    stack = [value]
    while stack:
        node = stack.pop()
        if isinstance(node, dict):
            if any(not _valid(_TEXT, key) for key in node):
                return False
            stack.extend(node.values())
        elif isinstance(node, list):
            stack.extend(node)
        elif isinstance(node, str):
            if "\x00" in node:
                return False
        elif type(node) is float:
            if not math.isfinite(node):
                return False
        elif node is None or type(node) in (int, bool):
            continue
        else:
            return False
    return True


def validate_recovery_record(kind, value, *, journal_version=None):
    schema = RECOVERY_SCHEMAS[kind]
    if not isinstance(value, dict) or not _json_tree_valid(value):
        return False
    required = schema["required"]
    optional = schema["optional"]
    if not required.keys() <= value.keys():
        return False
    if kind == "journal_state" and value.keys() - {"updated_at"} and "status" not in value:
        return False
    if kind == "target_print_state" and value and "status" not in value:
        return False
    if kind == "journal" and value["schema_version"] != journal_version:
        return False
    for name, item in value.items():
        rule = required.get(name, optional.get(name))
        if rule is not None and not _valid(rule, item):
            return False
        if name.endswith("_at") and isinstance(item, str) and item:
            if not _valid(_TIME, item):
                return False
        if name in _PHS2_FIELDS.get(kind, ()) and item:
            if not isinstance(item, str) or not _PHS2.fullmatch(item):
                return False
    if kind == "action_source":
        source = _PHS2.fullmatch(value["qr_payload"])
        if source is None or source.group(2) != value["source_label_id"]:
            return False
    if kind == "journal_state":
        labels = ([value["target_label"]] if "target_label" in value else [])
        labels.extend(value.get("target_labels") or [])
        for label in labels:
            qr = label.get("qr_payload")
            if qr:
                match = _PHS2.fullmatch(qr) if isinstance(qr, str) else None
                if (match is None or (label.get("label_id")
                                      and label["label_id"] != match.group(2))
                        or (label.get("scan_anchor_input_tag_id")
                            and label["scan_anchor_input_tag_id"] != match.group(1))):
                    return False
    identity_name = _ITG_FIELDS.get(kind)
    if identity_name and value.get(identity_name):
        identity = value[identity_name]
        if (not isinstance(identity, str) or not re.fullmatch(
                r"[^\s|=\x00-\x1f\x7f]+", identity)):
            return False
        canonical = value.get("canonical_input_tag_qr") or value.get(
            "source_canonical_input_tag_qr")
        if canonical and _PHS2.fullmatch(canonical).group(1) != identity:
            return False
    return True


def require_recovery_record(kind, value, *, journal_version=None):
    if not validate_recovery_record(kind, value, journal_version=journal_version):
        raise ValueError(f"invalid {kind} recovery record")
    return value
