"""IW components_v2.py:808–833; input_tag_ledger.py:419–436 @a211ea4.
RW scan_entry.py:1–25 @347a50e3 overrides the incomplete-PHS2 guard.
No idle framing in either profile (lane correction 2026-10-12).
"""


def standard_prefix(text):
    return str(text or '').startswith('PHS=2|SRC=KMTECH_INPUT_TAG|')


def complete_phs2(text):
    parts = str(text or '').strip().split('|')
    order = ('PHS', 'SRC', 'ITG', 'CLC', 'LBL', 'HSH')
    if len(parts) != len(order):
        return False
    fields = {}
    for key, part in zip(order, parts):
        if part.count('=') != 1:
            return False
        actual, value = part.split('=', 1)
        if actual != key or not value or value != value.strip() or any(ord(c) < 32 for c in value):
            return False
        fields[key] = value
    digest = fields['HSH'].strip().lower()
    return (fields['PHS'] == '2' and fields['SRC'] == 'KMTECH_INPUT_TAG'
            and bool(fields['ITG'].strip()) and bool(fields['CLC'].strip())
            and bool(fields['LBL'].strip()) and len(digest) == 16
            and all(c in '0123456789abcdef' for c in digest))


def can_submit(profile, text):
    return bool(text) and not (profile == 'iw' and standard_prefix(text) and not complete_phs2(text))
