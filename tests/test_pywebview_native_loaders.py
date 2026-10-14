"""Preserve the original signed loaders pywebview resolves during import."""
import hashlib
import json
import shutil
import subprocess

import pytest

from tools import build_portable_release_candidate as builder


LOADERS = {
    'win-arm64': (147016, '0937030efc718f3da77c5e0978fb5046e381d0bb7b01acaad31a535a1b588a33'),
    'win-x64': (161864, 'eb5335edeaa6f2122c044e82aab2351dc8c9b4e0fe5e578c71922fb61be285e7'),
    'win-x86': (121928, '04814b2c59314c6452d61b821a4a3a5c5f8e8577b55b985f6a7a4b59f8907ec5'),
}


def test_native_loader_set_retains_all_three_architectures_and_unsigned_policy(tmp_path):
    builder._copy_third_party(tmp_path)
    expected = {f'site-packages/webview/lib/runtimes/{arch}/native/WebView2Loader.dll'
                for arch in LOADERS}
    actual = {name for name in builder.APP_NATIVE_ALLOWLIST if name.endswith('/WebView2Loader.dll')}
    assert actual == expected
    assert len(builder.APP_NATIVE_ALLOWLIST) == 8
    assert len(builder.APP_NATIVE_UNSIGNED) == 3
    assert actual.isdisjoint(builder.APP_NATIVE_UNSIGNED)
    for arch, pin in LOADERS.items():
        relative = f'webview/lib/runtimes/{arch}/native/WebView2Loader.dll'
        assert relative not in builder.THIRD_PARTY_EXCLUDED_PATHS
        assert builder.APP_NATIVE_ALLOWLIST['site-packages/' + relative] == pin
        source = builder.SHELL_RUNTIME_SITE_PACKAGES / relative
        copied = tmp_path / relative
        assert copied.read_bytes() == source.read_bytes()
        assert copied.stat().st_size == pin[0]
        assert hashlib.sha256(copied.read_bytes()).hexdigest() == pin[1]


@pytest.mark.parametrize('arch', ('win-arm64', 'win-x86'))
def test_added_original_loader_has_valid_microsoft_authenticode_signature(arch):
    shell = shutil.which('powershell.exe')
    assert shell is not None, 'The native signature verification shell is required'
    path = builder.SHELL_RUNTIME_SITE_PACKAGES / f'webview/lib/runtimes/{arch}/native/WebView2Loader.dll'
    quoted = str(path).replace("'", "''")
    script = (
        "$ErrorActionPreference='Stop'; $s=Get-AuthenticodeSignature -LiteralPath '" + quoted + "'; "
        "[pscustomobject]@{valid=($s.Status -eq 'Valid'); "
        "microsoft=($null -ne $s.SignerCertificate -and "
        "$s.SignerCertificate.Subject -match 'O=Microsoft Corporation')} | ConvertTo-Json -Compress"
    )
    result = subprocess.run([shell, '-NoProfile', '-NonInteractive', '-Command', script],
                            capture_output=True, text=True, check=True)
    status = json.loads(result.stdout)
    assert status == {'valid': True, 'microsoft': True}
