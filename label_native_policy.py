"""Canonical unsigned application native pins, shared with release validation."""
from pathlib import Path
import hashlib

UNSIGNED_NATIVE_SHA256 = {
    "site-packages/_cffi_backend.cp312-win_amd64.pyd": "9af5e4f68a41f26b726f2e9424f9d58a4f5666189f6a48be95f406b257102aec",
    "site-packages/clr_loader/ffi/dlls/amd64/ClrLoader.dll": "07f62bcb1b70320f221ffb2cfc5ce6133815195f3e7cdb8d833222506188c2ac",
    "site-packages/pythonnet/runtime/Python.Runtime.dll": "2ebd4492e28442ef1f1af587afe5b3a090c2ebfa220758ded4f1450f2a27f13a",
}


def unsigned_native_manifest_valid(root: Path, listed) -> bool:
    if listed == []:
        return True
    if (not isinstance(listed, list) or len(listed) != len(UNSIGNED_NATIVE_SHA256)
            or any(not isinstance(path, str) for path in listed)
            or len(set(listed)) != len(listed) or set(listed) != set(UNSIGNED_NATIVE_SHA256)):
        return False
    for relative in listed:
        path = root / "app" / relative
        if not path.is_file() or hashlib.sha256(path.read_bytes()).hexdigest() != UNSIGNED_NATIVE_SHA256[relative]:
            return False
    return True
