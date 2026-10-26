"""Stock conversion and the application's actual canonical archive consumer."""
import hashlib
import importlib.util
import json
from pathlib import Path
import stat
import zipfile

import pytest


ROOT = Path(__file__).resolve().parents[1]
REQUIRED = (
    'portable-manifest.json', 'runtime/python.exe', 'runtime/pythonw.exe',
    'app/main.py', 'INSTALL_CANONICAL_PORTABLE.ps1',
    'tools/apply_prepared_update.ps1', 'app/update-manifest-key-config.json',
)


def load_tool():
    spec = importlib.util.spec_from_file_location(
        'canonical_packet_tool', ROOT / 'tools/make_canonical_update_packet.py')
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def stock_zip(path, names=REQUIRED):
    with zipfile.ZipFile(path, 'w') as archive:
        archive.comment = b'stock archive'
        for index, name in enumerate(names):
            info = zipfile.ZipInfo(name, (2024, 2, 4, 6, 8, 10 + 2 * index))
            # Bypass ZipInfo's Windows separator normalization in rejection fixtures.
            info.filename = info.orig_filename = name
            info.compress_type = zipfile.ZIP_STORED if index % 2 else zipfile.ZIP_DEFLATED
            info.external_attr = (stat.S_IFREG | 0o640) << 16
            archive.writestr(info, ('payload-' + str(index)).encode())


def test_reprefix_preserves_payload_order_metadata_and_is_deterministic(tmp_path):
    tool = load_tool()
    stock = tmp_path / 'stock.zip'
    stock_zip(stock)
    first, second = tmp_path / 'first.zip', tmp_path / 'second.zip'
    receipt = tmp_path / 'receipt.json'
    tool.make_packet(stock, first, receipt)
    tool.make_packet(stock, second, tmp_path / 'second.json')
    assert first.read_bytes() == second.read_bytes()
    with zipfile.ZipFile(stock) as source, zipfile.ZipFile(first) as target:
        assert target.namelist() == ['Label_Match/' + name for name in source.namelist()]
        for before, after in zip(source.infolist(), target.infolist(), strict=True):
            assert (after.date_time, after.compress_type, after.external_attr) == (
                before.date_time, before.compress_type, before.external_attr)
            assert target.read(after) == source.read(before)
    proof = json.loads(receipt.read_text(encoding='utf-8'))
    assert proof['schema_version'] == 1
    assert proof['entry_count'] == len(REQUIRED)
    assert proof['stock'] == {
        'name': stock.name, 'size_bytes': stock.stat().st_size,
        'sha256': hashlib.sha256(stock.read_bytes()).hexdigest(),
    }
    assert proof['canonical'] == {
        'name': first.name, 'size_bytes': first.stat().st_size,
        'sha256': hashlib.sha256(first.read_bytes()).hexdigest(),
    }
    assert [row['stock_name'] for row in proof['entries']] == list(REQUIRED)
    for index, row in enumerate(proof['entries']):
        digest = hashlib.sha256(('payload-' + str(index)).encode()).hexdigest()
        assert row == {'stock_name': REQUIRED[index],
                       'canonical_name': 'Label_Match/' + REQUIRED[index],
                       'stock_sha256': digest, 'canonical_sha256': digest}


def test_application_validator_accepts_canonical_and_rejects_stock(tmp_path):
    stock, output = tmp_path / 'stock.zip', tmp_path / 'canonical.zip'
    stock_zip(stock)
    load_tool().make_packet(stock, output, tmp_path / 'receipt.json')
    spec = importlib.util.spec_from_file_location('label_packet_consumer', ROOT / 'Label_Match.py')
    app = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(app)
    policy = {'format': 'zip', 'layout': 'canonical_portable',
              'top_level': 'Label_Match', 'entrypoint': 'Label_Match/app/main.py',
              'required_files': ['Label_Match/' + name for name in REQUIRED]}
    with zipfile.ZipFile(output) as archive:
        app._validate_update_archive_contract(archive, policy)
    with zipfile.ZipFile(stock) as archive, pytest.raises(ValueError):
        app._validate_update_archive_contract(archive, policy)


@pytest.mark.parametrize('names', [
    (), ('folder/',), ('../bad',), ('app/../bad',), ('/absolute',), ('C:/absolute',),
    ('app\\bad',), ('app/a', 'APP/A'), ('app/a', 'app/a'),
    ('./bad',), ('app//bad',), ('app/a:b',), ('app/CON.txt',),
    ('app/bad.',), ('app/bad ',), ('app/bad\x01',),
    ('app', 'APP/child'), ('Label_Match/app/main.py',),
])
def test_unsafe_input_is_rejected_before_creating_outputs(tmp_path, names):
    stock, output, receipt = tmp_path / 'stock.zip', tmp_path / 'output.zip', tmp_path / 'receipt.json'
    stock_zip(stock, names)
    with pytest.raises(ValueError):
        load_tool().make_packet(stock, output, receipt)
    assert not output.exists() and not receipt.exists()


@pytest.mark.parametrize('mode', [stat.S_IFDIR, stat.S_IFLNK, stat.S_IFIFO])
def test_nonregular_attributes_are_rejected(tmp_path, mode):
    stock, output, receipt = tmp_path / 'stock.zip', tmp_path / 'output.zip', tmp_path / 'receipt.json'
    with zipfile.ZipFile(stock, 'w') as archive:
        info = zipfile.ZipInfo('app/item')
        info.external_attr = (mode | 0o700) << 16
        archive.writestr(info, b'payload')
    with pytest.raises(ValueError):
        load_tool().make_packet(stock, output, receipt)
    assert not output.exists() and not receipt.exists()


@pytest.mark.parametrize('existing', ['output.zip', 'receipt.json'])
def test_existing_destination_is_preserved(tmp_path, existing):
    stock, output, receipt = tmp_path / 'stock.zip', tmp_path / 'output.zip', tmp_path / 'receipt.json'
    stock_zip(stock)
    (tmp_path / existing).write_bytes(b'preserve')
    with pytest.raises(FileExistsError):
        load_tool().make_packet(stock, output, receipt)
    assert (tmp_path / existing).read_bytes() == b'preserve'
    assert not (receipt if existing == 'output.zip' else output).exists()


@pytest.mark.parametrize('same', ['stock-output', 'stock-receipt', 'output-receipt'])
def test_paths_must_be_distinct(tmp_path, same):
    stock, output, receipt = tmp_path / 'stock.zip', tmp_path / 'output.zip', tmp_path / 'receipt.json'
    stock_zip(stock)
    if same == 'stock-output':
        output = stock
    elif same == 'stock-receipt':
        receipt = stock
    else:
        receipt = output
    before = stock.read_bytes()
    with pytest.raises(ValueError):
        load_tool().make_packet(stock, output, receipt)
    assert stock.read_bytes() == before


def test_crc_failure_is_rejected_before_outputs(tmp_path):
    stock, output, receipt = tmp_path / 'stock.zip', tmp_path / 'output.zip', tmp_path / 'receipt.json'
    stock_zip(stock, ('file', 'plain'))
    raw = stock.read_bytes()
    assert b'payload-1' in raw
    stock.write_bytes(raw.replace(b'payload-1', b'corrupt-1'))
    with pytest.raises(zipfile.BadZipFile):
        load_tool().make_packet(stock, output, receipt)
    assert not output.exists() and not receipt.exists()


@pytest.mark.parametrize('alteration', ['encrypted', 'nul', 'dos-directory'])
def test_wire_level_unsafe_metadata_is_rejected(tmp_path, alteration):
    stock, output, receipt = tmp_path / 'stock.zip', tmp_path / 'output.zip', tmp_path / 'receipt.json'
    stock_zip(stock, ('file',))
    raw = bytearray(stock.read_bytes())
    central = raw.index(b'PK\x01\x02')
    if alteration == 'encrypted':
        raw[6] |= 1
        raw[central + 8] |= 1
    elif alteration == 'dos-directory':
        raw[central + 38] |= 0x10
    else:
        raw = raw.replace(b'file', b'fi\x00e')
    stock.write_bytes(raw)
    with pytest.raises(ValueError):
        load_tool().make_packet(stock, output, receipt)
    assert not output.exists() and not receipt.exists()


@pytest.mark.parametrize('destination', ['output.zip', 'receipt.json'])
def test_exclusive_create_preserves_a_competing_destination(tmp_path, monkeypatch, destination):
    tool = load_tool()
    stock, output, receipt = tmp_path / 'stock.zip', tmp_path / 'output.zip', tmp_path / 'receipt.json'
    stock_zip(stock)
    original = Path.open

    def competing_open(path, mode='r', *args, **kwargs):
        if path == tmp_path / destination and mode.startswith('x'):
            with original(path, 'wb') as stream:
                stream.write(b'competing-writer')
        return original(path, mode, *args, **kwargs)

    monkeypatch.setattr(Path, 'open', competing_open)
    with pytest.raises(FileExistsError):
        tool.make_packet(stock, output, receipt)
    assert (tmp_path / destination).read_bytes() == b'competing-writer'
    if destination == 'output.zip':
        assert not receipt.exists()
    else:
        assert zipfile.is_zipfile(output)


def test_zero_external_attributes_are_preserved(tmp_path):
    stock, output, receipt = tmp_path / 'stock.zip', tmp_path / 'output.zip', tmp_path / 'receipt.json'
    stock_zip(stock, ('file',))
    raw = bytearray(stock.read_bytes())
    central = raw.index(b'PK\x01\x02')
    raw[central + 38:central + 42] = bytes(4)
    stock.write_bytes(raw)
    load_tool().make_packet(stock, output, receipt)
    with zipfile.ZipFile(output) as archive:
        assert archive.infolist()[0].external_attr == 0


def test_receipt_failure_preserves_output_and_retry_refuses_it(tmp_path):
    tool = load_tool()
    stock, output = tmp_path / 'stock.zip', tmp_path / 'output.zip'
    stock_zip(stock)
    receipt = tmp_path / 'missing' / 'receipt.json'
    with pytest.raises(FileNotFoundError):
        tool.make_packet(stock, output, receipt)
    assert output.is_file()
    before = output.read_bytes()
    with pytest.raises(FileExistsError):
        tool.make_packet(stock, output, receipt)
    assert output.read_bytes() == before


@pytest.mark.parametrize('interruption', [OSError, KeyboardInterrupt])
def test_write_failure_or_stop_keeps_artifacts_and_retry_refuses_them(tmp_path, monkeypatch, interruption):
    tool = load_tool()
    stock, output, receipt = tmp_path / 'stock.zip', tmp_path / 'output.zip', tmp_path / 'receipt.json'
    stock_zip(stock)
    original_stock = stock.read_bytes()

    def interrupted_write(*_args, **_kwargs):
        raise interruption()

    monkeypatch.setattr(zipfile._ZipWriteFile, 'write', interrupted_write)
    args = ['--stock', str(stock), '--output', str(output), '--receipt', str(receipt)]
    assert tool.main(args) == 2
    assert output.exists() and not receipt.exists()
    before = output.read_bytes()
    assert tool.main(args) == 2
    assert output.read_bytes() == before and stock.read_bytes() == original_stock


def test_cli_has_fixed_status_and_exit_codes(tmp_path, capsys):
    tool = load_tool()
    stock, output, receipt = tmp_path / 'stock.zip', tmp_path / 'output.zip', tmp_path / 'receipt.json'
    stock_zip(stock)
    args = ['--stock', str(stock), '--output', str(output), '--receipt', str(receipt)]
    assert tool.main(args) == 0
    assert capsys.readouterr().out == 'CANONICAL_PACKET_OK\n'
    assert tool.main(args) == 2
    captured = capsys.readouterr()
    assert captured.err == 'CANONICAL_PACKET_ERROR\n'
    assert captured.out == ''
    assert tool.main([]) == 2
    captured = capsys.readouterr()
    assert captured.err == 'CANONICAL_PACKET_ERROR\n' and captured.out == ''
