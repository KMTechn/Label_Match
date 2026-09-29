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
import re

from carrier_identity_port import decode_carrier_scan, parse_compact_carrier, parse_legacy_fields

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
# Keys whose empty or repeated value breaks a new-system label.
_IDENTITY_KEYS = frozenset((
    "PHS", "SRC", "ITG", "BND", "CLC", "SPC", "LBL", "HSH", "TRF", "QT", "QTY", "ITEM", "ITEM_CODE",
))
# The carrier parser reads QTY as QT: both name the one quantity.
_QUANTITY_KEYS = frozenset(("QT", "QTY"))
# Its other aliases (CLC=INSPECTION): a repeat would hide a value.
_ALIAS_KEYS = frozenset(("ITEM_NAME", "PHASE"))
# Container_Audit's other new-system identity values (ITG and BND are lineage).
_IDENTITY_VALUE_KEYS = frozenset(("LBL", "HSH", "HSH_CORE", "HSH_LABEL", "BUNDLE_ID", "SOURCE_BUNDLE_ID"))


def _pairs(decoded):
    """Every key/value in scan order, duplicates kept (None: no '=')."""

    if "|" not in decoded and "=" not in decoded:
        return []
    pairs = []
    for part in decoded.split("|"):
        key, separator, value = part.partition("=")
        pairs.append((key.strip().upper(), value.strip() if separator else None))
    return pairs


def _item_code(values):
    """The label's item, with the carrier parser's INSPECTION alias."""

    clc = str(values.get("CLC") or "").strip()
    alias = str(values.get("ITEM") or values.get("ITEM_CODE") or "").strip()
    return alias if clc.upper() == "INSPECTION" else (clc or alias)


def _positive_integer(value):
    return bool(re.fullmatch(r"[0-9]+", str(value or "").strip())) and int(value) > 0


def _unread_new_label(decoded, pairs):
    """A label Container_Audit reads as new (PHS=2 or an identity value) that
    the old-QR parser cannot read, so the base refused it at the 13-digit check."""

    return (
        "|" in decoded
        and any(
            (key == "PHS" and value == "2") or (key in _IDENTITY_VALUE_KEYS and value)
            for key, value in pairs
        )
        and parse_legacy_fields(decoded) is None
    )


def classify_start_label(raw_value, *, parse_sealed):
    """Return (shape, reasons, item_code) of one start label.

    Keys of the new system (TRF, BND, ITG or any SRC=KMTECH_INPUT_TAG) make a
    label new-shaped; a PHS value alone does not, so an old phase QR stays
    LEGACY.  A label no LM parser reads is new-shaped without lineage when
    Container_Audit reads it as new (PHS=2 or an identity value such as LBL).
    Every raw pair counts, so a repeated key cannot hide a marker.
    ``parse_sealed`` is the application's sealed-transfer parser.
    """

    decoded = decode_carrier_scan(raw_value)
    pairs = _pairs(decoded)
    keys = [key for key, _value in pairs]
    values = {key: value for key, value in pairs if value}
    item_code = _item_code(values)
    input_tag = any(
        key == "SRC" and str(value or "").strip().upper() == "KMTECH_INPUT_TAG"
        for key, value in pairs
    )
    lineage_marked = bool({"TRF", "BND", "ITG"} & set(keys) or input_tag)
    if not lineage_marked and not _unread_new_label(decoded, pairs):
        return SHAPE_LEGACY, (), item_code
    unread = not lineage_marked
    reasons = []
    if (
        any(keys.count(key) > 1 for key in _IDENTITY_KEYS | _ALIAS_KEYS)
        or sum(key in _QUANTITY_KEYS for key in keys) > 1
    ):
        reasons.append("DUPLICATE_KEY")
    if any(key in _IDENTITY_KEYS and value == "" for key, value in pairs) or "" in keys:
        reasons.append("PHS_EMPTY")
    if "TRF" in keys:
        shape, lineage = SHAPE_SEALED, "BND"
    elif "ITG" in keys or input_tag or unread:
        shape, lineage = SHAPE_PHS2, "ITG"
    else:
        shape, lineage = SHAPE_STRUCTURED, "BND"
    # A structured label is judged as the package flow reads it: through the
    # carrier parser's aliases (CLC=INSPECTION: ITEM, ITEM_NAME, PHASE, QTY).
    normalized = (
        (parse_legacy_fields(decoded) or {}) if shape == SHAPE_STRUCTURED else {}
    )
    if shape != SHAPE_SEALED and "PHS" not in keys and not normalized.get("PHS"):
        reasons.append("PHS_MISSING")
    elif shape == SHAPE_PHS2 and str(values.get("PHS") or "2") != "2":
        reasons.append("PHS_NOT_2")
    if not values.get(lineage):
        reasons.append("LINEAGE_MISSING")
    if any(key in _QUANTITY_KEYS and not _positive_integer(value) for key, value in pairs):
        reasons.append("QT_INVALID")
    if shape == SHAPE_SEALED:
        try:
            sealed = parse_sealed(decoded)
        except ValueError:
            sealed = None
        if not sealed:
            reasons.append("SEALED_QR_INVALID")
    elif unread:
        # Lineage is missing, so the compact carrier is not judged; a segment
        # without "=" is still a broken format.
        if any(value is None for _key, value in pairs if _key):
            reasons.append("FORMAT_INVALID")
    elif shape == SHAPE_PHS2:
        try:
            parse_compact_carrier(decoded)
        except ValueError:
            reasons.append("PHS2_FORMAT_INVALID")
    elif (
        not (values.get("SPC") or normalized.get("SPC"))
        or any(value is None for _key, value in pairs if _key)
        # The carrier parser cannot read it, and no other code says why.
        or not (normalized or {"PHS_MISSING", "PHS_EMPTY"} & set(reasons) or not item_code)
    ):
        reasons.append("FORMAT_INVALID")
    if not item_code:
        reasons.append("ITEM_UNCONFIRMED")
    if reasons:
        return SHAPE_MALFORMED, tuple(dict.fromkeys(reasons)), item_code
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
