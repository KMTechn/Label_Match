"""Differential checks against the frozen pre-W5-B2 caller contracts."""

import base64
from contextlib import redirect_stdout
import io
import itertools
from pathlib import Path
import random
import runpy

import pytest

import Label_Match as app
import carrier_identity_port as port
import package_logistics as logistics
import phs_label_workflow as workflow


BASELINE = runpy.run_path(str(Path(__file__).parent / 'fixtures/carrier_identity_baseline.py'))
VALID = 'PHS=2|SRC=KMTECH_INPUT_TAG|ITG=ITAG-1|CLC=AAA2270730200|LBL=LBL-1|HSH=aB0123456789CDef'
LEGACY = 'CLC=AAA2270730200|SPC=Product|PHS=1'


def encoded(raw):
    return base64.urlsafe_b64encode(raw.encode('utf-8')).decode('ascii').rstrip('=')


VECTORS = {
    'valid': VALID,
    'case-and-space': ' phs = 2 |src=kmtech_input_tag| itg = ITAG-1 |clc=AAA2270730200|lbl=LBL-1|hsh=AB0123456789CDEF ',
    'order': VALID.replace('ITG=ITAG-1|CLC=AAA2270730200', 'CLC=AAA2270730200|ITG=ITAG-1'),
    'hash-short': VALID[:-1],
    'hash-long': VALID + '0',
    'hash-nonhex': VALID[:-1] + 'g',
    'hash-uppercase': VALID[:-16] + 'ABCDEF0123456789',
    'hash-lowercase': VALID[:-16] + 'abcdef0123456789',
    'wrong-source': VALID.replace('KMTECH_INPUT_TAG', 'OTHER'),
    'wrong-phase': VALID.replace('PHS=2', 'PHS=1'),
    'extra': VALID + '|QT=3',
    'missing': VALID.rsplit('|', 1)[0],
    'duplicate': VALID.replace('LBL=LBL-1', 'CLC=LBL-1'),
    'empty-value': VALID.replace('ITG=ITAG-1', 'ITG='),
    'empty-key': VALID.replace('ITG=ITAG-1', '=ITAG-1'),
    'extra-equals': VALID.replace('ITG=ITAG-1', 'ITG=ITAG=1'),
    'missing-equals': VALID.replace('ITG=ITAG-1', 'ITG'),
    'legacy': LEGACY,
    'legacy-duplicate': LEGACY + '|SPC=Last',
    'legacy-inspection': 'CLC=INSPECTION|ITEM=AAA2270730200|ITEM_NAME=Part|QTY=3',
    'legacy-central': 'SRC=KMTECH_INPUT_TAG|ITEM_CODE=AAA2270730200|PHS=1',
    'legacy-empty-spc': 'SRC=KMTECH_INPUT_TAG|CLC=A|PHS=1|SPC=',
    'encoded-compact': encoded(VALID),
    'encoded-legacy': encoded(LEGACY),
    'encoded-padding': encoded(VALID) + '==',
    'encoded-no-fields': encoded('this has no carrier fields'),
    'encoded-invalid-utf8': base64.b64encode(b'\xff' * 30).decode('ascii'),
    'decode-short-boundary': 'Q0xDPUF8U1BDPUJ8UEhT',
    'decode-nonascii': '품' * 21,
    'raw-unicode': VALID.replace('ITAG-1', '현품-1'),
    'empty': '',
    'none': None,
    'whitespace': ' \t\r\n',
    'zero': 0,
    'bytes': VALID.encode('utf-8'),
}


def observe(function, *args):
    output = io.StringIO()
    with redirect_stdout(output):
        try:
            result = ('return', function(*args))
        except Exception as exc:
            result = ('error', type(exc).__name__, str(exc), getattr(exc, 'code', None),
                      getattr(exc, 'retryable', None), getattr(exc, 'details', None))
    return result, output.getvalue()


def assert_parity(raw):
    for current, old in (
        (app._label_match_decode_possible_base64_label, BASELINE['_label_match_decode_possible_base64_label']),
        (app._label_match_parse_new_format_fields, BASELINE['_label_match_parse_new_format_fields']),
        (app._label_match_parse_compact_phs2, BASELINE['_label_match_parse_compact_phs2']),
        (workflow.parse_compact_phs2, BASELINE['parse_compact_phs2']),
        (port.decode_carrier_scan, BASELINE['_label_match_decode_possible_base64_label']),
        (port.parse_legacy_fields, BASELINE['_label_match_parse_new_format_fields']),
        (port.parse_compact_carrier, BASELINE['_label_match_parse_compact_phs2']),
        (parse_raw_port, BASELINE['parse_compact_phs2']),
    ):
        assert observe(current, raw) == observe(old, raw), (current.__name__, raw)


def parse_raw_port(raw):
    return port.parse_raw_compact_carrier(raw, error_type=workflow.PHSLabelWorkflowError)


@pytest.mark.parametrize('raw', VECTORS.values(), ids=VECTORS.keys())
def test_recorded_carrier_vectors(raw):
    assert_parity(raw)


def test_generated_order_and_encoding_properties():
    # All field permutations, with raw and scanner encoding, retain the same
    # accept/reject partition and exact exception messages as the old callers.
    for parts in itertools.permutations(VALID.split('|')):
        raw = '|'.join(parts)
        assert_parity(raw)
        assert_parity(encoded(raw))


def test_generated_field_and_legacy_properties():
    rng = random.Random(20260913)
    values = ['', ' ', '2', '1', 'a' * 15, 'F' * 16, '0' * 17, 'g' * 16,
              '품목', 'A=B', 'A|B', '\x1d', 'kmtech_input_tag', 'INSPECTION']
    for _ in range(600):
        parts = rng.choice([VALID, LEGACY]).split('|')
        index = rng.randrange(len(parts))
        key, _ = parts[index].split('=', 1)
        parts[index] = rng.choice([key, key.lower(), ' ' + key + ' ', 'ITEM', 'ITEM_CODE', '']) + '=' + rng.choice(values)
        raw = '|'.join(parts)
        assert_parity(raw)
        assert_parity(encoded(raw))


@pytest.mark.parametrize('raw', [None, '', '6D20240229', 'x<Gs>6D20260228',
    'x\x1d6D20260231\x1d6D20260228', 'x\x1d6D2026AB01\x1d6D20260228',
    'x\x1d6D２０２６０２２８', 'x\x1d6D00010101', 'x\x1d6d20260228'])
def test_recorded_legacy_date_vectors(raw):
    assert observe(app.Label_Match._extract_production_date, None, raw) == observe(
        BASELINE['_extract_production_date'], None, raw)


def test_generated_legacy_date_properties():
    rng = random.Random(6)
    for _ in range(400):
        raw = f'x{rng.choice([chr(29), "<GS>", "<gs>", "|"])}6D{rng.randrange(1999, 2030):04}{rng.randrange(15):02}{rng.randrange(35):02}'
        assert observe(app.Label_Match._extract_production_date, None, raw) == observe(
            BASELINE['_extract_production_date'], None, raw)


def test_item_lookup_preserves_exact_keys_last_row_and_object_identity():
    rows = [{'Item Code': key, 'Item Name': str(index)} for index, key in enumerate(
        ['AAA2270730200', 'aaa2270730200', ' AAA2270730200 ', 'ＡＡＡ2270730200', '', None, 'AAA2270730200'])]
    view = port.item_catalog_view(iter(rows))
    assert view == BASELINE['item_catalog_view'](rows)
    original = view.copy()
    fallback = {'Item Name': '테스트 품목', 'Spec': 'T-SPEC'}
    for code in [row['Item Code'] for row in rows] + ['AAA', 'AAA2270730200-A', 'missing', 0]:
        for default in (None, {}, fallback):
            assert port.item_lookup(view, code, default) is BASELINE['item_lookup'](view, code, default)
    assert port.item_lookup(view, 'AAA2270730200') is rows[-1]
    assert view == original
    assert observe(port.item_catalog_view, [{}]) == observe(BASELINE['item_catalog_view'], [{}])
    assert observe(port.item_lookup, view, []) == observe(BASELINE['item_lookup'], view, [])


def test_generated_sample_and_exact_membership_properties():
    rng = random.Random(487493)
    for _ in range(800):
        samples = [rng.choice(['', 'A', 'a', 'Ａ', 'B']) for _ in range(rng.randrange(6))]
        raw_exact = [rng.choice(['', 'A', 'a', 'Ａ', 'B']) for _ in range(rng.randrange(6))]
        exact = logistics.canonical_barcodes(raw_exact)
        mode = rng.choice(['INHERIT_ALL', 'EXACT_RESCAN'])
        source_id, source_itg, source_hint = [rng.choice(['', 'source']) for _ in range(3)]
        expected = BASELINE['package_checks'](samples, mode, exact, raw_exact, source_id, source_itg, source_hint)
        actual = port.legacy_qa_sample_error(samples) or port.package_membership_error(
            mode, exact, len(raw_exact), has_source=bool(source_id or source_itg or source_hint))
        assert actual == expected
