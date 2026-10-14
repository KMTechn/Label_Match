"""Synthetic scan journal: append + fsync before acceptance; replay at startup."""
import json
import threading
import time
from collections import deque
from pathlib import Path

import shell_fileio


class Events:
    def __init__(self, data, fileio=None):
        self.fileio = fileio or shell_fileio.FileIO()
        self.path = Path(data) / 'events.jsonl'
        self.lock = threading.Lock()
        self.dropped = 0  # diagnostic rows lost to a refused/failed write; screen/scan flow continues
        try: self.fileio.make_dirs(self.path.parent, 'events')
        except Exception: self.dropped += 1

    def record(self, src, ev, **fields):
        row = {'t': time.time(), **fields, 'src': src, 'ev': ev}
        encoded = json.dumps(row, ensure_ascii=False, separators=(',', ':')) + '\n'
        with self.lock:
            try: self.fileio.append_bytes(self.path, encoded.encode('utf-8'), 'events')
            except Exception: self.dropped += 1  # only this diagnostic row is lost
        return row

    def find_receipt(self, seq, offset):
        # Rejected requests have no journal row. Reuse their existing event log
        # as the current process's spill storage; never retain all outcomes.
        found = None
        if not self.path.exists(): return None
        with self.lock, self.path.open('rb') as stream:
            stream.seek(offset)
            for line in stream:
                if not line.endswith(b'\n'): break
                row = json.loads(line)
                if row.get('ev') == 'scan_receipt' and row.get('seq') == seq:
                    found = {**row['result'], 'text': row['text']}
        return found


class Journal:
    def __init__(self, data, fileio=None):
        self.fileio = fileio or shell_fileio.FileIO()
        self.path = Path(data) / 'journal.jsonl'
        self.fileio.make_dirs(self.path.parent, 'journal')
        self.lock = threading.RLock()
        self.rows = deque(maxlen=8)
        self._count = self._ok = self._ng = self._last_seq = 0
        self.recovered_tail = None
        boundary = 0
        if self.path.exists():
            with self.path.open('rb') as stream:
                for line in stream:
                    if not line.endswith(b'\n'):
                        saved = self.path.with_name(f'journal-incomplete-{time.time_ns()}.jsonl')
                        self.fileio.create_new(saved, line, 'journal')
                        self.recovered_tail = str(saved)
                        break
                    row = json.loads(line.decode('utf-8'))
                    if type(row['seq']) is not int or row['seq'] <= self._last_seq or row['verdict'] not in ('ok', 'ng'):
                        raise ValueError('journal sequence/verdict invalid; original preserved')
                    self._remember(row)
                    boundary = stream.tell()
        if self.recovered_tail:
            self.fileio.truncate(self.path, boundary, 'journal')

    def _remember(self, row):
        self._count += 1
        self._ok += row['verdict'] == 'ok'
        self._ng += row['verdict'] == 'ng'
        self._last_seq = row['seq']
        self.rows.append(dict(row))

    def find(self, seq):
        if not self.path.exists(): return None
        with self.lock, self.path.open('rb') as stream:
            for line in stream:
                row = json.loads(line)
                if row['seq'] == seq: return row
                if row['seq'] > seq: return None
        return None

    def append(self, row):
        with self.lock:
            if type(row['seq']) is not int or row['seq'] <= self._last_seq:
                raise ValueError('journal sequence out of order')
            encoded = (json.dumps(row, ensure_ascii=False, separators=(',', ':')) + '\n').encode('utf-8')
            self.fileio.append_bytes(self.path, encoded, 'journal', fsync=True)
            self._remember(row)

    def state(self):
        with self.lock:
            return {'count': self._count, 'ok': self._ok, 'ng': self._ng,
                    'last_seq': self._last_seq, 'journal_lines': self._count,
                    'recent': [dict(r) for r in self.rows]}
