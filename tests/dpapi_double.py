"""Authenticated test protection; never opens a Windows credential store."""

import os

from cryptography.exceptions import InvalidTag
from cryptography.hazmat.primitives.ciphers.aead import AESGCM


class DPAPIDouble:
    def __init__(self, key=None):
        self._cipher = AESGCM(key if key is not None else AESGCM.generate_key(bit_length=256))

    def protect(self, value, entropy=b''):
        nonce = os.urandom(12)
        return b'TEST-DPAPI\x00' + nonce + self._cipher.encrypt(nonce, bytes(value), bytes(entropy))

    def unprotect(self, value, entropy=b''):
        value = bytes(value)
        if not value.startswith(b'TEST-DPAPI\x00') or len(value) < 39:
            raise InvalidTag
        return self._cipher.decrypt(value[11:23], value[23:], bytes(entropy))

    def install(self, patch, registration=None):
        import current_user_onboarding
        import deferred_intent_capture as capture
        import direct_sync_runtime
        import logistics_runtime_profile as profile
        import tools.install_logistics_runtime_profile as installer
        from direct_sync_push import DirectSyncPushError

        def read_text(value):
            try:
                return self.unprotect(value, profile.DPAPI_ENTROPY).decode('utf-8')
            except (InvalidTag, UnicodeError) as error:
                raise profile.LogisticsRuntimeConfigurationError(
                    'DPAPI bearer token could not be decrypted') from error

        def write_text(value):
            return self.protect(value.encode('utf-8'), profile.DPAPI_ENTROPY)

        for module in (profile, installer, current_user_onboarding):
            for name, function in (('protect_current_user_secret', write_text),
                                   ('unprotect_current_user_secret', read_text)):
                if hasattr(module, name):
                    patch.setattr(module, name, function)
        # load_logistics_runtime_profile defaults to this reader even when the
        # file was protected for the current user.
        patch.setattr(profile, 'unprotect_machine_secret', read_text)

        def read_producer(value):
            try:
                return self.unprotect(value)
            except InvalidTag as error:
                raise DirectSyncPushError('dpapi secret_ref could not be read') from error

        patch.setattr(direct_sync_runtime, '_dpapi_unprotect_current_user', read_producer)

        def read_capture(value, entropy=b''):
            try:
                return self.unprotect(value, entropy)
            except InvalidTag as error:
                raise capture.DeferredIntentCaptureError(
                    'CAPTURE_DPAPI_READBACK_FAILED',
                    'current-user DPAPI payload readback failed') from error

        def read_seal(value):
            try:
                return self.unprotect(value, profile.DPAPI_ENTROPY)
            except InvalidTag as error:
                raise profile.LogisticsRuntimeConfigurationError(
                    'DPAPI bearer token could not be decrypted') from error

        def payload_protect(value, entropy):
            require_entropy(entropy)
            return self.protect(value, entropy)

        def payload_unprotect(value, entropy):
            require_entropy(entropy)
            return read_capture(value, entropy)

        def require_entropy(entropy):
            if len(bytes(entropy)) != 32:
                raise capture.DeferredIntentCaptureError(
                    'CAPTURE_DPAPI_ENTROPY_INVALID', 'payload-protection v2 entropy must be 32 bytes')

        store = capture.DeferredIntentCaptureStore
        patch.setattr(store, '_dpapi_protect_bytes', staticmethod(
            lambda value: self.protect(value, profile.DPAPI_ENTROPY)))
        patch.setattr(store, '_dpapi_unprotect_bytes', staticmethod(read_seal))
        patch.setattr(store, '_dpapi_protect_payload_v2', staticmethod(payload_protect))
        patch.setattr(store, '_dpapi_unprotect_payload_v2', staticmethod(payload_unprotect))
        if registration is not None:
            def read_registration(value):
                try:
                    return self.unprotect(value).decode('utf-8')
                except (InvalidTag, UnicodeError) as error:
                    raise DirectSyncPushError('dpapi secret verify failed') from error
            patch.setattr(registration, '_dpapi_protect_current_user',
                          lambda value: self.protect(value.encode('utf-8')))
            patch.setattr(registration, '_dpapi_unprotect_current_user', read_registration)


def run_canary_child():
    """Bootstrap the existing isolated crash child with the same test cipher."""
    import runpy
    import sys
    from pathlib import Path
    import pytest

    # -I deliberately ignores PYTHONPATH; explicitly use this test snapshot.
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
    key_path, script, *arguments = sys.argv[1:]
    guard = os.environ.get('KMTECH_TEST_HOST_GUARD')
    if guard:
        runpy.run_path(guard)
    with pytest.MonkeyPatch.context() as patch:
        DPAPIDouble(Path(key_path).read_bytes()).install(patch)
        sys.argv = [script, *arguments]
        runpy.run_path(script, run_name='__main__')


if __name__ == '__main__':
    run_canary_child()
