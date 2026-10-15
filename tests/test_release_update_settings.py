import hashlib
import json
from pathlib import Path
import zipfile

import pytest
from tools import build_portable_release_candidate as builder
from tools import build_release_archive as archive_builder


APPROVED = Path('D:/KMTech/program-improvement-20260912/work/program/w9mgrweb/field-update-feed-1015/label_match.auto-update-settings.json')


@pytest.fixture
def approved_settings():
    if not APPROVED.is_file():
        pytest.skip('approved release settings qualification input is unavailable')
    assert hashlib.sha256(APPROVED.read_bytes()).hexdigest() == builder.RELEASE_AUTO_UPDATE_SETTINGS_SHA256
    return APPROVED


@pytest.mark.parametrize('damage', ['omitted', 'empty-object', 'empty-feed', 'sha'])
def test_release_entry_rejects_unqualified_settings_before_output(tmp_path, monkeypatch, approved_settings, damage):
    value = None
    if damage != 'omitted':
        value = tmp_path / 'settings.json'
        if damage == 'empty-object':
            value.write_text('{}')
        elif damage == 'empty-feed':
            value.write_text(json.dumps(dict(feed_url='', channel='stable', profile_auth=True)))
        else:
            value.write_bytes(approved_settings.read_bytes() + b'\n')
    called = []
    monkeypatch.setattr(builder, '_assert_clean_source', lambda *_: called.append('clean'))
    output = tmp_path / 'output'
    with pytest.raises(builder.PortableBuildError) as caught:
        builder.build(tmp_path / 'source', tmp_path / 'python', output,
                      update_manifest_public_key_config='', auto_update_settings=value)
    assert str(caught.value) in {'RELEASE_UPDATE_SETTINGS_REQUIRED', 'RELEASE_UPDATE_FEED_REQUIRED',
                                 'RELEASE_UPDATE_SETTINGS_SHA256_DIFFERS'}
    assert not called and not output.exists()


def test_release_entry_accepts_exact_file_before_any_build(tmp_path, monkeypatch, approved_settings):
    class PreflightAccepted(Exception):
        pass
    def stop_before_build(_root):
        raise PreflightAccepted
    monkeypatch.setattr(builder, '_assert_clean_source', stop_before_build)
    with pytest.raises(PreflightAccepted):
        builder.build(tmp_path, tmp_path / 'python', tmp_path / 'output',
                      update_manifest_public_key_config='', auto_update_settings=approved_settings)
    assert not (tmp_path / 'output').exists()


def test_approved_settings_survive_publisher_and_actual_archive_bytes(tmp_path, monkeypatch, approved_settings):
    raw, policy = builder._release_update_settings(approved_settings)
    assert raw == approved_settings.read_bytes() and policy['feed_url']
    package = tmp_path / 'package'
    app = package / 'app'
    app.mkdir(parents=True)
    builder._write_auto_update_settings(app, raw)
    (package / 'Label_Match.exe').write_bytes(b'inert archive transport fixture')
    monkeypatch.setattr(archive_builder, 'validate_release_evidence', lambda *_a, **_k: {})
    target = tmp_path / 'transport.zip'
    report = archive_builder.build_release_archive(package, target, source_epoch=1600000000)
    assert report['byte_parity'] and report['crc_verified'] and report['exact_membership']
    with zipfile.ZipFile(target) as archive:
        assert archive.read('Label_Match/app/auto-update-settings.json') == approved_settings.read_bytes()


def test_custom_settings_require_explicit_test_only_path(tmp_path):
    with pytest.raises(builder.PortableBuildError):
        builder._release_update_settings({})
    raw, _policy = builder._release_update_settings({}, test_only=True)
    assert isinstance(json.loads(raw), dict)
    args = builder.parse_args(['--output', str(tmp_path / 'output'), '--test-only-custom-update-settings'])
    assert args.test_only_custom_update_settings


def test_cli_passes_approved_file_to_build_without_reserialization(tmp_path, monkeypatch, capsys, approved_settings):
    def preflight_only(_repo, _python, _output, **kwargs):
        assert kwargs['auto_update_settings'] == approved_settings
        assert kwargs['test_only_custom_update_settings'] is False
        raw, _policy = builder._release_update_settings(kwargs['auto_update_settings'])
        assert raw == approved_settings.read_bytes()
        return {'status': 'PASS'}
    monkeypatch.setattr(builder, 'build', preflight_only)
    assert builder.main(['--output', str(tmp_path / 'output'), '--repo-root', str(tmp_path),
                         '--update-manifest-public-key-config', '{}',
                         '--auto-update-settings', str(approved_settings)]) == 0
    assert json.loads(capsys.readouterr().out)['status'] == 'PASS'
