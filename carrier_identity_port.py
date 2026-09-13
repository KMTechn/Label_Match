"""Current Label_Match carrier boundaries, with explicit inputs only.

Compact admission and legacy compatibility remain separate. Raw workflow
parsing deliberately does not decode scanner Base64; its caller supplies the
existing exception type to preserve its code/message contract without an app
import. Catalog I/O, presentation, printing, and authoritative membership and
receipt verification belong to the callers.
"""

import base64
import binascii
from datetime import datetime
import re
from typing import Any



def decode_carrier_scan(raw_value):
    text = str(raw_value or "").strip()
    if not text or "|" in text or len(text) <= 20:
        return text
    try:
        temp_b64 = text.replace('-', '+').replace('_', '/')
        padded_b64 = temp_b64 + '=' * (-len(temp_b64) % 4)
        decoded = base64.b64decode(padded_b64).decode('utf-8')
        return decoded if '|' in decoded and '=' in decoded else text
    except (binascii.Error, UnicodeDecodeError):
        return text


def parse_legacy_fields(raw_value):
    decoded = decode_carrier_scan(raw_value)
    if '|' not in decoded or '=' not in decoded:
        return None
    try:
        fields = {
            key.strip().upper(): value.strip()
            for key, value in (
                item.split('=', 1)
                for item in decoded.split('|')
                if '=' in item
            )
        }
    except Exception:
        return None
    if str(fields.get("SRC") or "").strip().upper() == "KMTECH_INPUT_TAG":
        item_code = str(fields.get("CLC") or fields.get("ITEM") or fields.get("ITEM_CODE") or "").strip()
        phase = str(fields.get("PHS") or "").strip()
        if not item_code or not phase:
            return None
        normalized = dict(fields)
        normalized["CLC"] = item_code
        normalized.setdefault("SPC", str(fields.get("ITEM_NAME") or fields.get("ITEM") or item_code).strip())
        fields = normalized
    if str(fields.get("CLC") or "").strip().upper() == "INSPECTION":
        item_code = str(fields.get("ITEM") or fields.get("ITEM_CODE") or "").strip()
        if not item_code:
            return None
        normalized = dict(fields)
        normalized["CLC"] = item_code
        normalized.setdefault("SPC", str(fields.get("ITEM_NAME") or item_code).strip())
        normalized.setdefault("PHS", str(fields.get("PHASE") or "INSPECTION").strip())
        if not normalized.get("QT") and fields.get("QTY"):
            normalized["QT"] = str(fields["QTY"]).strip()
        fields = normalized
    if not all(fields.get(key) for key in ('CLC', 'SPC', 'PHS')):
        return None
    return fields


def parse_compact_carrier(raw_value):
    """Parse the exact six-field central PHS2 physical-label contract."""

    decoded = decode_carrier_scan(raw_value)
    parts = decoded.split("|") if decoded else []
    expected_keys = ("PHS", "SRC", "ITG", "CLC", "LBL", "HSH")
    if len(parts) != len(expected_keys):
        raise ValueError("PHS2 must contain exactly six canonical fields")
    fields = {}
    keys = []
    for part in parts:
        if part.count("=") != 1:
            raise ValueError("PHS2 field syntax is invalid")
        key, value = part.split("=", 1)
        key = key.strip().upper()
        value = value.strip()
        if not key or not value or key in fields:
            raise ValueError("PHS2 fields must be non-empty and unique")
        keys.append(key)
        fields[key] = value
    if tuple(keys) != expected_keys:
        raise ValueError(
            "PHS2 fields must be ordered PHS,SRC,ITG,CLC,LBL,HSH"
        )
    if fields["PHS"] != "2" or fields["SRC"].upper() != "KMTECH_INPUT_TAG":
        raise ValueError("only central KMTECH_INPUT_TAG PHS=2 is accepted")
    digest = fields["HSH"].lower()
    if len(digest) != 16 or any(value not in "0123456789abcdef" for value in digest):
        raise ValueError("PHS2 HSH must be a 16-character hexadecimal prefix")
    fields["SRC"] = "KMTECH_INPUT_TAG"
    fields["HSH"] = digest
    return fields


def parse_raw_compact_carrier(raw_value: Any, *, error_type) -> dict[str, str]:
    """Parse raw workflow text with the caller's existing (code, message) error."""
    value = str(raw_value or "").strip()
    expected_keys = ("PHS", "SRC", "ITG", "CLC", "LBL", "HSH")
    parts = value.split("|") if value else []
    if len(parts) != len(expected_keys):
        raise error_type(
            "PHS2_FORMAT_INVALID",
            "PHS2는 여섯 개의 표준 필드여야 합니다.",
        )
    fields: dict[str, str] = {}
    ordered: list[str] = []
    for part in parts:
        if part.count("=") != 1:
            raise error_type(
                "PHS2_FORMAT_INVALID",
                "PHS2 필드 형식이 올바르지 않습니다.",
            )
        key, field_value = part.split("=", 1)
        key = key.strip().upper()
        field_value = field_value.strip()
        if not key or not field_value or key in fields:
            raise error_type(
                "PHS2_FORMAT_INVALID",
                "PHS2 필드는 비어 있거나 중복될 수 없습니다.",
            )
        ordered.append(key)
        fields[key] = field_value
    if tuple(ordered) != expected_keys:
        raise error_type(
            "PHS2_FORMAT_INVALID",
            "PHS2 필드 순서가 표준과 다릅니다.",
        )
    if (
        fields["PHS"] != "2"
        or fields["SRC"].upper() != "KMTECH_INPUT_TAG"
        or not re.fullmatch(r"[0-9a-fA-F]{16}", fields["HSH"])
    ):
        raise error_type(
            "PHS2_FORMAT_INVALID",
            "중앙 KMTECH_INPUT_TAG PHS2 형식이 아닙니다.",
        )
    fields["SRC"] = "KMTECH_INPUT_TAG"
    fields["HSH"] = fields["HSH"].lower()
    return fields


def parse_legacy_production_date(raw_input):
    """Extract 6D date text; the caller owns the existing diagnostic/None fallback."""
    normalized_input = re.sub(r"<gs>", "\x1D", str(raw_input or ""), flags=re.IGNORECASE)
    fields = normalized_input.split('\x1D')
    for field in fields:
        if field.startswith('6D'):
            date_str = field[2:]
            if len(date_str) == 8 and date_str.isdigit():
                production_date = datetime.strptime(date_str, "%Y%m%d")
                return production_date.strftime("%Y-%m-%d")
    return None


def item_catalog_view(rows):
    """Keep the loader's exact Item Code keys and last-row-wins semantics."""
    return {row['Item Code']: row for row in rows}


def item_lookup(view, code, default=None):
    """Return the existing row/default without normalizing the lookup key."""
    return view.get(code, default)


def legacy_qa_sample_error(raw_samples):
    """Check legacy QA samples before the caller canonicalizes barcodes."""
    if any(not value for value in raw_samples) or len(raw_samples) != len(set(raw_samples)):
        return 'sample_barcodes must be non-empty and unique'
    if len(raw_samples) > 3:
        return 'legacy packaging QA samples cannot exceed three barcodes'
    return None


def package_membership_error(mode, exact, raw_exact_count, *, has_source):
    """Keep samples separate from source inheritance/full exact rescan.

    The caller still validates the mode, canonicalizes exact barcodes, and
    verifies authoritative source and receipt membership/version evidence.
    """
    if mode == 'INHERIT_ALL' and not has_source:
        return 'INHERIT_ALL requires a sealed transfer QR or structured PHS BND/ITG identity'
    if mode == 'INHERIT_ALL' and exact:
        return 'INHERIT_ALL cannot use sample/exact rescan barcodes as membership'
    if mode == 'EXACT_RESCAN' and (not exact or len(exact) != raw_exact_count):
        return 'EXACT_RESCAN requires a non-empty unique full rescan'
    return None
