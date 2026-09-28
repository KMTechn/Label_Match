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


def _pairs(decoded):
    """Every key/value in scan order, duplicates kept (None: no '=')."""

    if "|" not in decoded and "=" not in decoded:
        return []
    pairs = []
    for part in decoded.split("|"):
        key, separator, value = part.partition("=")
        pairs.append((key.strip().upper(), value.strip() if separator else None))
    return pairs


def classify_start_label(raw_value, *, parse_sealed):
    """Return (shape, reasons, item_code) of one start label.

    Keys of the new system (TRF, BND, ITG or SRC=KMTECH_INPUT_TAG) make a label
    new-shaped; a PHS value alone does not, so an old phase QR stays LEGACY.
    ``parse_sealed`` is the application's sealed-transfer parser.
    """

    decoded = decode_carrier_scan(raw_value)
    pairs = _pairs(decoded)
    keys = [key for key, _value in pairs]
    values = {key: value for key, value in pairs if value}
    item_code = str(
        values.get("CLC") or values.get("ITEM") or values.get("ITEM_CODE") or ""
    ).strip()
    input_tag = str(values.get("SRC") or "").upper() == "KMTECH_INPUT_TAG"
    if not ({"TRF", "BND", "ITG"} & set(keys) or input_tag):
        return SHAPE_LEGACY, (), item_code
    reasons = []
    if len(keys) != len(set(keys)):
        reasons.append("DUPLICATE_KEY")
    if any(not key or value == "" for key, value in pairs):
        reasons.append("PHS_EMPTY")
    if any(value is None for _key, value in pairs):
        reasons.append("PHS2_FORMAT_INVALID")
    if "TRF" in keys:
        shape, lineage = SHAPE_SEALED, "BND"
    elif "ITG" in keys or input_tag:
        shape, lineage = SHAPE_PHS2, "ITG"
    else:
        shape, lineage = SHAPE_STRUCTURED, "BND"
    if shape != SHAPE_SEALED and "PHS" not in keys:
        reasons.append("PHS_MISSING")
    if not values.get(lineage):
        reasons.append("LINEAGE_MISSING")
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
    elif not values.get("SPC"):
        reasons.append("PHS2_FORMAT_INVALID")
    if not item_code:
        reasons.append("ITEM_UNCONFIRMED")
    if reasons:
        return SHAPE_MALFORMED, tuple(dict.fromkeys(reasons)), item_code
    return shape, (), item_code


def reason_code(value):
    """Keep only a stable upper-case code; never free text."""

    text = str(value or "").strip().upper()
    return text if _REASON.fullmatch(text) else "REASON_CODE_REDACTED"


def fields(transition_class, reasons=(), duplicate=False):
    """The shared completion-detail contract (CA and LM use the same names)."""

    return {
        "transition_class": transition_class,
        "transition_reasons": list(dict.fromkeys(reason_code(value) for value in reasons or ())),
        "transition_duplicate": bool(duplicate),
    }
