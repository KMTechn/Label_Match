"""Retain the pre-existing Bearer tail grammar when adding Basic masking."""
import uuid

import pytest

from failure_redaction import redact_for_sink


@pytest.mark.parametrize('scheme', ('Basic', 'basic', 'Bearer', 'bearer', 'Token', 'tOkEn', 'Negotiate', 'nEgOtIaTe'))
@pytest.mark.parametrize('folded', (False, True))
def test_complete_legacy_authorization_value(scheme, folded):
    pieces = (uuid.uuid4().hex, uuid.uuid4().hex)
    material = ':'.join(pieces)
    separator = '\r\n\t' if folded else ' '
    text = 'Authorization: ' + scheme + separator + material
    safe = redact_for_sink(text, {})
    for piece in pieces:
        if piece in safe:
            pytest.fail('authorization credential fragment remained', pytrace=False)
