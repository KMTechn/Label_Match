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

from carrier_identity_port import decode_carrier_scan, parse_compact_carrier

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
    "PHS", "SRC", "ITG", "BND", "CLC", "SPC", "LBL", "HSH", "TRF", "QT", "ITEM", "ITEM_CODE",
))


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


def classify_start_label(raw_value, *, parse_sealed):
    """Return (shape, reasons, item_code) of one start label.

    Keys of the new system (TRF, BND, ITG or any SRC=KMTECH_INPUT_TAG) make a
    label new-shaped; a PHS value alone does not, so an old phase QR stays
    LEGACY.  Every raw pair counts, so a repeated key cannot hide a marker.
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
    if not ({"TRF", "BND", "ITG"} & set(keys) or input_tag):
        return SHAPE_LEGACY, (), item_code
    reasons = []
    if any(keys.count(key) > 1 for key in _IDENTITY_KEYS):
        reasons.append("DUPLICATE_KEY")
    if any(key in _IDENTITY_KEYS and value == "" for key, value in pairs) or "" in keys:
        reasons.append("PHS_EMPTY")
    if "TRF" in keys:
        shape, lineage = SHAPE_SEALED, "BND"
    elif "ITG" in keys or input_tag:
        shape, lineage = SHAPE_PHS2, "ITG"
    else:
        shape, lineage = SHAPE_STRUCTURED, "BND"
    if shape != SHAPE_SEALED and "PHS" not in keys:
        reasons.append("PHS_MISSING")
    elif shape == SHAPE_PHS2 and str(values.get("PHS") or "2") != "2":
        reasons.append("PHS_NOT_2")
    if not values.get(lineage):
        reasons.append("LINEAGE_MISSING")
    if any(key == "QT" and not _positive_integer(value) for key, value in pairs):
        reasons.append("QT_INVALID")
    if shape == SHAPE_SEALED:
        try:
            sealed = parse_sealed(decoded)
        except ValueError:
            sealed = None
        if not sealed:
            reasons.append("SEALED_QR_INVALID")
    elif shape == SHAPE_PHS2:
        try:
            parse_compact_carrier(decoded)
        except ValueError:
            reasons.append("PHS2_FORMAT_INVALID")
    elif not values.get("SPC") or any(value is None for _key, value in pairs if _key):
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


def fields(transition_class, reasons=(), duplicate=False):
    """The shared completion-detail contract (CA and LM use the same names)."""

    return {
        "transition_class": transition_class,
        "transition_reasons": list(dict.fromkeys(reason_code(value) for value in reasons or ())),
        "transition_duplicate": bool(duplicate),
    }
