"""Transition-mode classes for packaging completions (pure helpers).

While the administrator switch is on, every start label is accepted and each
completion carries exactly one class, shared with Container_Audit:

- LEGACY: a label without new-system lineage (13 digits, old phase QR).
- PHS2_CENTRAL: a new label whose central package flow succeeded.
- PHS2_LOCAL: a valid new label that a central or strict check blocked.
- PHS2_MALFORMED: a label with new-system keys whose format is broken.

Only PHS2_CENTRAL reaches the package ledger; the others complete locally.
See docs/spec/operations.md#legacy-label-transition.
"""

import hashlib
import json
import re
import unicodedata

from carrier_identity_port import decode_carrier_scan, parse_legacy_fields

LEGACY = "LEGACY"
PHS2_CENTRAL = "PHS2_CENTRAL"
PHS2_LOCAL = "PHS2_LOCAL"
PHS2_MALFORMED = "PHS2_MALFORMED"
CLASSES = (LEGACY, PHS2_CENTRAL, PHS2_LOCAL, PHS2_MALFORMED)
LOCAL_CLASSES = frozenset((LEGACY, PHS2_LOCAL, PHS2_MALFORMED))
# package_logistics.status of a transition completion that never enters the
# package outbox.
LOCAL_ONLY_STATUS = "TRANSITION_LOCAL_ONLY"

SHAPE_LEGACY = "LEGACY"
SHAPE_PHS2 = "PHS2"
SHAPE_STRUCTURED = "STRUCTURED"
SHAPE_SEALED = "SEALED"
SHAPE_MALFORMED = "MALFORMED"

_REASON = re.compile(r"[A-Z][A-Z0-9_]{1,63}")
# ---- Transition label rules shared by Container_Audit (label_qr.py) and
# Label_Match (label_transition.py).  Keep this block identical in both; both
# run the same vectors (tests/transition_label_vectors.json).
TRANSITION_LABEL_LEGACY = "LEGACY"
TRANSITION_LABEL_NEW = "NEW"
TRANSITION_LABEL_MALFORMED = "MALFORMED"
# Every label defect is recorded once, in this order; then DUPLICATE_LABEL,
# then what blocked the central path.
TRANSITION_REASON_ORDER = (
    "DUPLICATE_KEY", "PHS_EMPTY", "PHS_MISSING", "PHS_NOT_2", "LINEAGE_MISSING",
    "QT_INVALID", "FORMAT_INVALID", "SEALED_QR_INVALID", "ITEM_UNCONFIRMED",
)
# Keys only the new system prints: present at all, they mark a new label.
_TRANSITION_LINEAGE_KEYS = frozenset(("TRF", "BND", "ITG"))
# Values only the new system prints: a non-empty one marks a new label.
_TRANSITION_IDENTITY_VALUE_KEYS = frozenset((
    "LBL", "HSH", "HSH_CORE", "HSH_LABEL", "BUNDLE_ID", "SOURCE_BUNDLE_ID",
))
_TRANSITION_QUANTITY_KEYS = frozenset(("QT", "QTY", "QUANTITY"))
_TRANSITION_COMPACT_KEYS = ("PHS", "SRC", "ITG", "CLC", "LBL", "HSH")


class _TransitionJsonPairs(list):
    pass


def transition_label_pairs(text):
    """(pairs, is_json): every (KEY, value) of a pipe or JSON label in scan
    order with repeats kept; value None when a segment has no '='.  Keys are
    upper-case, keys and values trimmed, empty segments skipped.  No pairs:
    not a label (13 digits, no '|', another separator)."""

    stripped = str(text or "").strip()
    if stripped.startswith("{") and stripped.endswith("}"):
        try:
            parsed = json.loads(stripped, object_pairs_hook=_TransitionJsonPairs)
        except ValueError:
            parsed = None
        if isinstance(parsed, _TransitionJsonPairs):
            return [(str(key).strip().upper(), str(value).strip()) for key, value in parsed], True
    if "|" not in stripped:
        return [], False
    pairs = []
    for segment in stripped.split("|"):
        if segment.strip():
            key, separator, value = segment.partition("=")
            pairs.append((key.strip().upper(), value.strip() if separator else None))
    return pairs, False


def _transition_old_qr_readable(pairs, is_json):
    """Whether the old QR reader (CLC, SPC and PHS, with the CLC=INSPECTION
    aliases; the last value wins) reads the label."""

    if is_json:
        return False
    fields = {key: value for key, value in pairs if value is not None}
    if str(fields.get("CLC") or "").upper() == "INSPECTION":
        item = str(fields.get("ITEM") or fields.get("ITEM_CODE") or "")
        if not item:
            return False
        fields = dict(fields, CLC=item)
        fields.setdefault("SPC", str(fields.get("ITEM_NAME") or item))
        fields.setdefault("PHS", str(fields.get("PHASE") or "INSPECTION"))
    return all(fields.get(key) for key in ("CLC", "SPC", "PHS"))


def transition_label_item_code(pairs):
    """The label's item: CLC, or ITEM/ITEM_CODE for CLC=INSPECTION or no CLC."""

    fields = {key: value for key, value in pairs if value}
    clc = str(fields.get("CLC") or "")
    alias = str(fields.get("ITEM") or fields.get("ITEM_CODE") or "")
    return alias if clc.upper() == "INSPECTION" else (clc or alias)


def transition_label_text_defects(pairs):
    """Defects of any label's text: a repeated key (two quantity keys count),
    a quantity that is not a positive integer, a segment without '=' or key."""

    keys = [key for key, _value in pairs]
    defects = []
    if (
        any(keys.count(key) > 1 for key in keys if key)
        or sum(key in _TRANSITION_QUANTITY_KEYS for key in keys) > 1
    ):
        defects.append("DUPLICATE_KEY")
    if any(
        key in _TRANSITION_QUANTITY_KEYS and not re.fullmatch(r"[0-9]*[1-9][0-9]*", value or "")
        for key, value in pairs
    ):
        defects.append("QT_INVALID")
    if any(value is None or not key for key, value in pairs):
        defects.append("FORMAT_INVALID")
    return defects


def _transition_compact_label(text):
    """The exact central label PHS=2|SRC=KMTECH_INPUT_TAG|ITG|CLC|LBL|HSH."""

    fields = {}
    parts = str(text or "").strip().split("|")
    for part, expected in zip(parts, _TRANSITION_COMPACT_KEYS):
        key, separator, value = part.partition("=")
        if key.strip() != expected or not separator or "=" in value or not value.strip():
            return False
        fields[expected] = value.strip()
    return (
        len(parts) == len(_TRANSITION_COMPACT_KEYS)
        and fields["PHS"] == "2"
        and fields["SRC"].upper() == "KMTECH_INPUT_TAG"
        and re.fullmatch(r"[0-9A-Fa-f]{16}", fields["HSH"]) is not None
        and all(
            len(fields[key]) <= 256
            and not any(unicodedata.category(character).startswith("C") for character in fields[key])
            for key in ("ITG", "CLC", "LBL")
        )
    )


def transition_ordered_reasons(reasons):
    order = {code: index for index, code in enumerate(TRANSITION_REASON_ORDER)}
    return tuple(sorted(dict.fromkeys(reasons), key=lambda code: order.get(code, len(order))))


def classify_transition_label(text):
    """(kind, reasons, item_code) of one decoded start label.

    LEGACY: no new-system marker, or an old QR the old reader reads without a
    lineage key (an old phase QR with PHS=2 or an LBL value stays LEGACY).
    A marker is PHS=2, SRC=KMTECH_INPUT_TAG, a TRF/BND/ITG key or an
    LBL/HSH/HSH_CORE/HSH_LABEL/BUNDLE_ID/SOURCE_BUNDLE_ID value, in any
    occurrence.  NEW: the exact compact central label.  MALFORMED: any other
    marked label, with every defect.
    """

    pairs, is_json = transition_label_pairs(text)
    keys = {key for key, _value in pairs}
    fields = {key: value for key, value in pairs if value}
    item_code = transition_label_item_code(pairs)
    lineage = bool(_TRANSITION_LINEAGE_KEYS & keys) or any(
        key == "SRC" and str(value or "").upper() == "KMTECH_INPUT_TAG" for key, value in pairs
    )
    marked = lineage or any(
        (key == "PHS" and value == "2") or (key in _TRANSITION_IDENTITY_VALUE_KEYS and value)
        for key, value in pairs
    )
    reasons = transition_label_text_defects(pairs)
    if not marked or (not lineage and _transition_old_qr_readable(pairs, is_json)):
        return TRANSITION_LABEL_LEGACY, transition_ordered_reasons(reasons), item_code
    phases = [value for key, value in pairs if key == "PHS"]
    if not phases:
        reasons.append("PHS_MISSING")
    if "" in phases:
        reasons.append("PHS_EMPTY")
    if any(value and value != "2" for value in phases):
        reasons.append("PHS_NOT_2")
    if not all(fields.get(key) for key in ("SRC", "ITG", "LBL", "HSH")):
        reasons.append("LINEAGE_MISSING")
    elif item_code and not reasons and not _transition_compact_label(text):
        reasons.append("FORMAT_INVALID")
    if not item_code:
        reasons.append("ITEM_UNCONFIRMED")
    kind = TRANSITION_LABEL_MALFORMED if reasons else TRANSITION_LABEL_NEW
    return kind, transition_ordered_reasons(reasons), item_code


def transition_start_reasons(label_reasons=(), *, item_unconfirmed=False, duplicate=False, blocked=()):
    """A start's reasons in the shared order: label defects, ITEM_UNCONFIRMED,
    DUPLICATE_LABEL, then what blocked the central path."""

    reasons = list(transition_ordered_reasons(
        [*label_reasons, *(["ITEM_UNCONFIRMED"] if item_unconfirmed else [])]
    ))
    if duplicate:
        reasons.append("DUPLICATE_LABEL")
    return list(dict.fromkeys([*reasons, *blocked]))
# ---- End of the shared transition label rules.


def classify_start_label(raw_value, *, parse_sealed):
    """Return (shape, reasons, item_code) of one start label.

    The shared rules above decide LEGACY, the compact PHS2 label and
    MALFORMED.  Two lineage shapes only this application reads keep their own
    checks: a sealed transfer QR (TRF) and a structured BND label without
    ITG or SRC=KMTECH_INPUT_TAG.  ``parse_sealed`` is the application's
    sealed-transfer parser.
    """

    decoded = decode_carrier_scan(raw_value)
    kind, reasons, item_code = classify_transition_label(decoded)
    pairs, _is_json = transition_label_pairs(decoded)
    keys = {key for key, _value in pairs}
    input_tag = any(
        key == "SRC" and str(value or "").upper() == "KMTECH_INPUT_TAG" for key, value in pairs
    )
    if kind == TRANSITION_LABEL_LEGACY or not (
        "TRF" in keys or ("BND" in keys and "ITG" not in keys and not input_tag)
    ):
        shape = {
            TRANSITION_LABEL_LEGACY: SHAPE_LEGACY,
            TRANSITION_LABEL_NEW: SHAPE_PHS2,
            TRANSITION_LABEL_MALFORMED: SHAPE_MALFORMED,
        }[kind]
        return shape, reasons, item_code
    fields = {key: value for key, value in pairs if value}
    reasons = transition_label_text_defects(pairs)
    if "TRF" in keys:
        shape = SHAPE_SEALED
        try:
            sealed = parse_sealed(decoded)
        except ValueError:
            sealed = None
        if not sealed:
            reasons.append("SEALED_QR_INVALID")
    else:
        # Judged as the package flow reads it: through the carrier parser's
        # aliases (CLC=INSPECTION: ITEM, ITEM_NAME, PHASE, QTY).
        shape = SHAPE_STRUCTURED
        normalized = parse_legacy_fields(decoded) or {}
        phases = [value for key, value in pairs if key == "PHS"]
        if "" in phases:
            reasons.append("PHS_EMPTY")
        if not phases and not normalized.get("PHS"):
            reasons.append("PHS_MISSING")
        if (
            not (fields.get("SPC") or normalized.get("SPC"))
            # The carrier parser cannot read it, and no other code says why.
            or not (normalized or {"PHS_MISSING", "PHS_EMPTY"} & set(reasons) or not item_code)
        ):
            reasons.append("FORMAT_INVALID")
    if not fields.get("BND"):
        reasons.append("LINEAGE_MISSING")
    if not item_code:
        reasons.append("ITEM_UNCONFIRMED")
    if reasons:
        return SHAPE_MALFORMED, transition_ordered_reasons(reasons), item_code
    return shape, (), item_code


def reason_code(value):
    """Keep only a stable upper-case code; never free text."""

    text = str(value or "").strip().upper()
    return text if _REASON.fullmatch(text) else "REASON_CODE_REDACTED"


def event_key(pc_id, set_id, event):
    """Stable event ID (detail.idempotency_key, <=128) of one set's event.

    A resend of the same event reuses it; a new completion has a new set.
    """

    digest = hashlib.sha256(f"{pc_id}|{set_id}|{event}".encode("utf-8")).hexdigest()
    return f"LM-{event}-{digest[:40]}"


def cancellation_key(pc_id, set_id):
    """Event ID of one completed set's cancellation.

    History delete (SET_DELETED) and label cancel (TRAY_COMPLETION_CANCELLED)
    share it, so a retry through the other route reuses the first row.
    """

    return event_key(pc_id, set_id, "CANCELLATION")


def fields(transition_class, reasons=(), duplicate=False):
    """The shared completion-detail contract (CA and LM use the same names)."""

    return {
        "transition_class": transition_class,
        "transition_reasons": list(dict.fromkeys(reason_code(value) for value in reasons or ())),
        "transition_duplicate": bool(duplicate),
    }
