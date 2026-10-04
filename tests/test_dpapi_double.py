"""Verify properties relied on by incidental-DPAPI recovery tests."""

import pytest
from cryptography.exceptions import InvalidTag

from tests.dpapi_double import DPAPIDouble


@pytest.mark.parametrize('cleartext', [b'', b'private-test-row', b'large-private-row' * 65536],
                         ids=['empty', 'small', 'large'])
def test_protected_bytes_roundtrip_without_plaintext(cleartext):
    cipher = DPAPIDouble()
    encrypted = cipher.protect(cleartext, b'bound-entropy')
    if cleartext:
        assert cleartext not in encrypted
    assert cipher.unprotect(encrypted, b'bound-entropy') == cleartext
    assert cipher.protect(cleartext, b'bound-entropy') != encrypted
    with pytest.raises(InvalidTag):
        cipher.unprotect(encrypted[:-1] + bytes([encrypted[-1] ^ 1]), b'bound-entropy')
    with pytest.raises(InvalidTag):
        DPAPIDouble().unprotect(encrypted, b'bound-entropy')
    with pytest.raises(InvalidTag):
        cipher.unprotect(encrypted, b'other-entropy')


def test_registration_bad_ciphertext_uses_product_exception(isolated_dpapi):
    from tests.test_register_label_match_worker_pc import load_registration_module
    from direct_sync_push import DirectSyncPushError

    module = load_registration_module()
    isolated_dpapi(module)
    with pytest.raises(DirectSyncPushError, match='dpapi secret verify failed'):
        module._dpapi_unprotect_current_user(b'tampered')
    from logistics_runtime_profile import protect_current_user_secret
    # Profile protection has entropy; registration protection has none.
    with pytest.raises(DirectSyncPushError, match='dpapi secret verify failed'):
        module._dpapi_unprotect_current_user(protect_current_user_secret('test-only'))


def test_capture_bad_ciphertext_and_entropy_use_product_exceptions(isolated_dpapi):
    from deferred_intent_capture import DeferredIntentCaptureStore as Store, DeferredIntentCaptureError
    from logistics_runtime_profile import LogisticsRuntimeConfigurationError

    with pytest.raises(LogisticsRuntimeConfigurationError, match='DPAPI bearer token could not be decrypted'):
        Store._dpapi_unprotect_bytes(b'tampered')
    with pytest.raises(DeferredIntentCaptureError) as error:
        Store._dpapi_unprotect_payload_v2(b'tampered', b'e' * 32)
    assert error.value.code == 'CAPTURE_DPAPI_READBACK_FAILED'
    with pytest.raises(DeferredIntentCaptureError) as error:
        Store._dpapi_protect_payload_v2(b'cleartext', b'wrong-length')
    assert error.value.code == 'CAPTURE_DPAPI_ENTROPY_INVALID'
