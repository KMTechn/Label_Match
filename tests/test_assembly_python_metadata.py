"""Real metadata serialization/write bodies, with synthetic secret lookup only."""
import json
import uuid

import pytest

import auto_update
import current_user_onboarding
import failure_redaction
import user_relay


WRITERS = {
    'settings': current_user_onboarding._write_machine_json_atomic,
    'stop-marker': user_relay._write_machine_json_atomic,
    # Writer admission is covered by the G suite; exercise its real write body.
    'transition': auto_update.write_json_durable.__wrapped__,
}


def expected_bytes(kind, payload):
    options = dict(sort_keys=True, ensure_ascii=kind == 'transition')
    options.update(separators=(',', ':')) if kind == 'transition' else options.update(indent=2)
    return (json.dumps(payload, **options) + '\n').encode('utf-8')


@pytest.fixture
def synthetic_lookup(monkeypatch):
    selected = {}
    original = failure_redaction.assert_machine_metadata_text
    monkeypatch.setattr(failure_redaction, 'assert_machine_metadata_text',
                        lambda text, environ=None: original(text, environ=selected))
    return selected


@pytest.mark.parametrize('kind', WRITERS)
def test_machine_writer_preserves_large_bytes_and_identifier(kind, tmp_path, synthetic_lookup):
    identifier = str(uuid.uuid4())
    synthetic_lookup['KMTECH_LABEL_WRITER_DELEGATION_TRANSACTION_ID'] = identifier
    payload = {'reason': 'x' * 70001, 'transaction_id': identifier,
               'report_version': current_user_onboarding.ONBOARDING_REPORT_VERSION,
               'text': chr(0x20ac)}
    target = tmp_path / ('transition.json' if kind == 'transition' else 'metadata.json')
    before = expected_bytes(kind, payload)
    assert len(before) > 65536
    WRITERS[kind](target, payload)
    assert target.read_bytes() == before
    assert json.loads(target.read_text(encoding='utf-8')) == payload


@pytest.mark.parametrize('kind', WRITERS)
@pytest.mark.parametrize('escaped', [False, True])
def test_machine_writer_refuses_secret_before_target_or_parent_mutation(kind, escaped,
                                                                      tmp_path, synthetic_lookup):
    secret = 'fixture-' + uuid.uuid4().hex
    if escaped:
        secret += '"\n' + chr(0x20ac)
    synthetic_lookup['KMTECH_FIXTURE_PASSWORD'] = secret
    payload = {'text': secret}
    target = tmp_path / 'missing-parent' / 'metadata.json'
    with pytest.raises(ValueError, match='^PRODUCT_METADATA_SECRET_VALUE_REFUSED$'):
        WRITERS[kind](target, payload)
    assert not target.parent.exists()
    target.parent.mkdir()
    target.write_bytes(b'{"preserved":true}\n')
    original = target.read_bytes()
    with pytest.raises(ValueError, match='^PRODUCT_METADATA_SECRET_VALUE_REFUSED$'):
        WRITERS[kind](target, payload)
    assert target.read_bytes() == original
    assert [path.name for path in target.parent.iterdir()] == [target.name]


def test_metadata_assertion_refuses_fully_unicode_escaped_json():
    secret = 'fixture-' + uuid.uuid4().hex + chr(0x20ac)
    encoded = ''.join('\\u%04x' % ord(character) for character in secret)
    with pytest.raises(ValueError, match='^PRODUCT_METADATA_SECRET_VALUE_REFUSED$'):
        failure_redaction.assert_machine_metadata_text('{"value":"' + encoded + '"}',
                                                       environ={'KMTECH_FIXTURE_PASSWORD': secret})
