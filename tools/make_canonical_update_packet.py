"""Prefix a stock portable ZIP without rebuilding its application payload.

The output and receipt must be new, distinct paths. On failure, any files
already created are retained for inspection; a retry refuses those paths.
"""
from __future__ import annotations

import argparse
import copy
import hashlib
import json
import os
from pathlib import Path
import stat
import sys
import zipfile


PREFIX = 'Label_Match/'
_BLOCK_SIZE = 1024 * 1024
_RESERVED = {'CON', 'PRN', 'AUX', 'NUL'} | {
    stem + str(index) for stem in ('COM', 'LPT') for index in range(1, 10)
}


def _sha256(stream) -> str:
    digest = hashlib.sha256()
    while block := stream.read(_BLOCK_SIZE):
        digest.update(block)
    return digest.hexdigest()


def _validate_members(members: list[zipfile.ZipInfo]) -> None:
    if not members:
        raise ValueError('empty stock archive')
    files = set()
    parents = set()
    for member in members:
        name = member.orig_filename
        parts = name.split('/')
        mode = stat.S_IFMT(member.external_attr >> 16)
        if (name != member.filename or not name or '\\' in name or '\x00' in name
                or member.is_dir() or member.external_attr & 0x10
                or mode not in (0, stat.S_IFREG)
                or member.flag_bits & 1
                or parts[0].casefold() == PREFIX[:-1].casefold()
                or any(part in ('', '.', '..') or ':' in part
                       or part.endswith((' ', '.'))
                       or part.split('.', 1)[0].upper() in _RESERVED
                       or any(ord(char) < 32 for char in part) for part in parts)):
            raise ValueError('unsafe stock member')
        key = name.casefold()
        if key in files:
            raise ValueError('duplicate stock member')
        files.add(key)
        parents.update('/'.join(parts[:index]).casefold() for index in range(1, len(parts)))
    if files & parents:
        raise ValueError('stock file and directory collision')


def make_packet(stock: Path, output: Path, receipt: Path) -> dict:
    """Create a canonical ZIP and its ordered, independently reread SHA receipt."""
    stock = Path(stock).resolve()
    output, receipt = (Path(path).absolute() for path in (output, receipt))
    if len({path.resolve() for path in (stock, output, receipt)}) != 3:
        raise ValueError('paths must be distinct')
    # Check and exclusively create the caller's paths, including dangling links.
    if any(os.path.lexists(path) for path in (output, receipt)):
        raise FileExistsError('destination already exists')

    with stock.open('rb') as source_stream:
        stock_sha = _sha256(source_stream)
        stock_size = os.fstat(source_stream.fileno()).st_size
        source_stream.seek(0)
        with zipfile.ZipFile(source_stream) as source:
            members = source.infolist()
            _validate_members(members)
            expected = []
            # Read every member (including its CRC check) before creating output.
            for member in members:
                with source.open(member) as entry:
                    expected.append(_sha256(entry))

            with output.open('xb') as output_stream:
                with zipfile.ZipFile(output_stream, 'w') as target:
                    target.comment = source.comment
                    for member in members:
                        renamed = copy.copy(member)
                        renamed.filename = renamed.orig_filename = PREFIX + member.filename
                        with source.open(member) as before, target.open(renamed, 'w') as after:
                            while block := before.read(_BLOCK_SIZE):
                                after.write(block)
                        # zipfile supplies defaults for zero external attributes.
                        # Restore the input value before the central directory is written.
                        renamed.external_attr = member.external_attr

            entries = []
            with zipfile.ZipFile(output) as target:
                converted = target.infolist()
                if len(converted) != len(members):
                    raise ValueError('canonical member count differs')
                for before, after, digest in zip(members, converted, expected, strict=True):
                    if (after.filename, after.date_time, after.compress_type, after.external_attr) != (
                            PREFIX + before.filename, before.date_time,
                            before.compress_type, before.external_attr):
                        raise ValueError('canonical metadata differs')
                    with target.open(after) as entry:
                        actual = _sha256(entry)
                    if actual != digest:
                        raise ValueError('canonical payload differs')
                    entries.append({'stock_name': before.filename, 'canonical_name': after.filename,
                                    'stock_sha256': digest, 'canonical_sha256': actual})
        source_stream.seek(0)
        if _sha256(source_stream) != stock_sha:
            raise ValueError('stock archive changed during conversion')

    with output.open('rb') as stream:
        canonical_sha = _sha256(stream)
        canonical_size = os.fstat(stream.fileno()).st_size
    proof = {
        'schema_version': 1,
        'stock': {'name': stock.name, 'size_bytes': stock_size, 'sha256': stock_sha},
        'canonical': {'name': output.name, 'size_bytes': canonical_size, 'sha256': canonical_sha},
        'entry_count': len(entries),
        'entries': entries,
    }
    with receipt.open('x', encoding='utf-8', newline='\n') as stream:
        json.dump(proof, stream, ensure_ascii=True, indent=2, sort_keys=True)
        stream.write('\n')
    return proof


class _Parser(argparse.ArgumentParser):
    def error(self, message):
        raise ValueError('invalid arguments')


def main(argv=None) -> int:
    parser = _Parser(description=__doc__)
    parser.add_argument('--stock', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--receipt', type=Path, required=True)
    try:
        args = parser.parse_args(argv)
        make_packet(args.stock, args.output, args.receipt)
    except (Exception, KeyboardInterrupt):
        print('CANONICAL_PACKET_ERROR', file=sys.stderr)
        return 2
    print('CANONICAL_PACKET_OK')
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
