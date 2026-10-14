"""Loopback fixture server from an explicitly supplied, unchanged U3 archive."""
from pathlib import Path
import json
import logging
import os
import sys
import traceback
import types
import uuid
import ipaddress
from datetime import datetime, timedelta, timezone


def main():
    source, state, ready, mode = sys.argv[1:]
    source, state, ready = Path(source), Path(state), Path(ready)
    state.mkdir(exist_ok=True)
    sys.path[:0] = [str(source), str(source / 'tests')]
    for name in tuple(sys.modules):
        if name == 'tests' or name.startswith('tests.'):
            del sys.modules[name]
    package = types.ModuleType('tests')
    package.__path__ = [str(source / 'tests')]
    sys.modules['tests'] = package
    os.environ['WORKER_ANALYSIS_ENV'] = 'development'
    os.environ['APP_ENV'] = os.environ['FLASK_ENV'] = 'development'
    os.environ['WORKER_ANALYSIS_DB_PATH'] = str(state / 'server.db')
    os.environ['WORKER_ANALYSIS_APP_SETTINGS_FILE'] = str(state / 'settings.json')
    os.environ['WORKER_ANALYSIS_LOG_FOLDER_PATH'] = str(state / 'incoming')
    logging.disable(logging.CRITICAL)
    from tests.app_test_environment import AppTestMonkeyPatch
    from test_inbound_item_catalog_api import _explicit_development_catalog_profile
    patch = AppTestMonkeyPatch()
    profile = _explicit_development_catalog_profile.__wrapped__(patch, None, state)
    if hasattr(profile, '__next__'):
        next(profile)
    extra = {}
    if mode in ('legacy','spill'):
        from test_logistics_api_v1 import _app, _headers, SCOPE
        from test_sealed_transfer_replacement import _seed
        app, database = _app(state, opening_qty=30)
        seed = _seed(app.test_client(), source_count=1, target_count=4)
        target = next(value for value in seed.values()
                      if isinstance(value, dict) and value.get('seal_qr_payload'))
        projection_response = app.test_client().get(
            '/logistics/api/v1/bundles/' + SCOPE + '/' + target['bundle_id'], headers=_headers())
        assert projection_response.status_code == 200
        projection = projection_response.get_json()['data']
        label = None
        headers = _headers(program='Label_Match')
        if mode == 'spill':
            from test_sealed_transfer_replacement import _complete_phs, _post, membership_hash
            other_id, other_version, other_members = _complete_phs(
                app.test_client(), session_id='spill-other-session', count=4,
                label='spill-other-label', start_index=21)
            other = app.test_client().get('/logistics/api/v1/bundles/'+SCOPE+'/'+other_id,
                                          headers=_headers()).get_json()['data']
            sealed = _post(app.test_client(), '/logistics/api/v1/transfers/seal',
                'SEAL_TRANSFER_BUNDLE', 'spill-other-seal', {'bundle:'+other_id:other_version},
                {'item_id':other.get('item_id') or other.get('item_code'),
                 'source_bundle_id':other_id,'transfer_bundle_id':'spill-other-transfer',
                 'external_label':'spill-other-transfer-label','member_ids':other_members,
                 'membership_hash':membership_hash(other_members),
                 'scanned_barcodes':[row['normalized_barcode'] for row in other['members']]})
            if sealed.status_code not in (200,201):
                import re
                code = str(sealed.get_json().get('error',{}).get('code','UNKNOWN'))
                (state/'seed-seal-failure.safe.json').write_text(json.dumps({
                    'status':sealed.status_code,'code':code if re.fullmatch('[A-Z_0-9]{1,80}',code) else 'MASKED'}))
            assert sealed.status_code in (200,201)
            extra['other_projection'] = app.test_client().get(
                '/logistics/api/v1/bundles/'+SCOPE+'/spill-other-transfer',
                headers=_headers()).get_json()['data']
    else:
        from test_phs_label_exchange import ledger
        from test_phs_label_overlay_api import _app, _machine_headers
        from test_phs_package_work_group import _allow_package_location_fixture, _seed_sealed_label, _resolve_package_source
        database = ledger.__wrapped__(state)
        app = _app(database, patch)[0]
        _allow_package_location_fixture(database, patch)
        seed, label, _resolved = _seed_sealed_label(database, app.test_client(), suffix='U4', ordinal=1, unit_count=4)
        response = _resolve_package_source(app.test_client(), label)
        assert response.status_code == 200
        projection = response.get_json()['data']
        headers = _machine_headers()
    credential = uuid.uuid4().hex
    app.config['LOGISTICS_API_TOKEN'] = credential
    from cryptography import x509
    from cryptography.hazmat.primitives import hashes, serialization
    from cryptography.hazmat.primitives.asymmetric import rsa
    from cryptography.x509.oid import NameOID
    signer = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    subject = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, 'localhost')])
    instant = datetime.now(timezone.utc)
    certificate = (x509.CertificateBuilder().subject_name(subject).issuer_name(subject)
                   .public_key(signer.public_key()).serial_number(x509.random_serial_number())
                   .not_valid_before(instant - timedelta(minutes=5)).not_valid_after(instant + timedelta(hours=2))
                   .add_extension(x509.SubjectAlternativeName([
                       x509.DNSName('localhost'), x509.IPAddress(ipaddress.ip_address('127.0.0.1'))]), critical=False)
                   .add_extension(x509.BasicConstraints(ca=True, path_length=None), critical=True)
                   .sign(signer, hashes.SHA256()))
    certificate_path, signer_path = state / 'ca.pem', state / 'tls-private.pem'
    certificate_path.write_bytes(certificate.public_bytes(serialization.Encoding.PEM))
    signer_path.write_bytes(signer.private_bytes(serialization.Encoding.PEM,
        serialization.PrivateFormat.PKCS8, serialization.NoEncryption()))
    from werkzeug.serving import make_server, WSGIRequestHandler

    class Quiet(WSGIRequestHandler):
        def log(self, *args, **kwargs):
            pass

    server = make_server('127.0.0.1', 0, app, request_handler=Quiet,
                         ssl_context=(str(certificate_path), str(signer_path)))
    metadata = {'port': server.server_port, 'database': str(database), 'projection': projection,
                'seed': seed, 'label': label, 'headers': headers, 'credential': credential,
                'tls_ca_bundle_path': str(certificate_path)}
    metadata.update(extra)
    ready.write_text(json.dumps(metadata), encoding='utf-8')
    server.serve_forever()


if __name__ == '__main__':
    try:
        main()
    except BaseException as error:
        target = Path(sys.argv[3]).with_suffix('.failure.json')
        target.write_text(json.dumps({'type': type(error).__name__, 'frames': [
            {'file': frame.filename, 'line': frame.lineno}
            for frame in traceback.extract_tb(error.__traceback__)]}), encoding='utf-8')
        raise SystemExit(1)
