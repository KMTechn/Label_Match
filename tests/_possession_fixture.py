"""Dependency-free possession-key values shared with isolated native fixtures."""

TEST_POSSESSION_JWK = {
    "kty": "EC",
    "crv": "P-256",
    "x": "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
    "y": "BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB",
}
TEST_POSSESSION_FINGERPRINT = "test-possession-fingerprint"


def fake_possession_descriptor(*, created=True):
    return {
        "contract_version": "producer-machine-possession-key-v1",
        "provider_name": "Microsoft Software Key Storage Provider",
        "key_name": "KMTech.DirectSync.Possession.v1",
        "scope": "current_user",
        "unique_name": "test-unique-name",
        "created": created,
        "public_jwk": dict(TEST_POSSESSION_JWK),
        "fingerprint": TEST_POSSESSION_FINGERPRINT,
        "machine_key": False,
        "export_policy": 0,
        "key_usage": 2,
    }
