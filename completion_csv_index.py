"""Rebuildable completion locations; CSV bytes remain the source of truth."""
from contextlib import closing
import csv
import hashlib
import io
import json
import os
from pathlib import Path
import shutil
import sqlite3
import tempfile
import threading


class CompletionCsvIndex:
    def __init__(self, directory, prefix):
        self.directory = Path(directory)
        self.prefix = prefix
        scope = hashlib.sha256(prefix.encode("utf-8")).hexdigest()[:24]
        self.path = self.directory / f"_completion_index_{scope}.sqlite3"
        self._lock = threading.RLock()
        self._coverage = None
        self._stamp = None

    @staticmethod
    def signature(path):
        try:
            stat = os.stat(path)
            return stat.st_size, stat.st_mtime_ns, stat.st_ctime_ns
        except FileNotFoundError:
            return None

    @classmethod
    def _content_signature(cls, path):
        before = cls.signature(path)
        if before is None:
            return None
        with open(path, "rb") as handle:
            digest = hashlib.file_digest(handle, "sha256").hexdigest()
        if before != cls.signature(path):
            raise OSError("completion index input changed while hashing")
        # size is the covered EOF offset. Hash all bytes, including the tail:
        # metadata and a tail-only hash cannot detect an in-place middle edit.
        return (*before, digest)

    def _read_source(self, path):
        before = self.signature(path)
        with open(path, "rb") as handle:
            content = handle.read()
        if before is None or before != self.signature(path) or len(content) != before[0]:
            raise OSError("completion CSV changed while reading")
        identities = set()
        for row in csv.DictReader(io.StringIO(content.decode("utf-8-sig"), newline="")):
            if row.get("event") != "TRAY_COMPLETE":
                continue
            try:
                details = json.loads(row.get("details") or "{}")
            except (TypeError, ValueError):
                continue
            if isinstance(details, dict):
                identity = str(details.get("set_id") or "").strip()
                if identity:
                    identities.add(identity)
        return (*before, hashlib.sha256(content).hexdigest()), identities

    def _sources(self):
        with os.scandir(self.directory) as entries:
            return {
                entry.name: self._content_signature(entry.path)
                for entry in entries
                if entry.name.startswith(self.prefix) and entry.name.lower().endswith(".csv")
            }

    def _publish(self, temporary):
        with open(temporary, "r+b") as handle:
            os.fsync(handle.fileno())
        digest = self._content_signature(temporary)[-1]
        os.replace(temporary, self.path)
        return (*self.signature(self.path), digest)

    def _rebuild(self, sources):
        # Build away from the live index. A crash cannot publish partial
        # coverage, and a missing/unreadable CSV cannot prove absence.
        descriptor, temporary = tempfile.mkstemp(prefix=self.path.name + ".", dir=self.directory)
        os.close(descriptor)
        try:
            with closing(sqlite3.connect(temporary)) as conn:
                # This private build is never read as coverage. Publish only
                # after closing and fsyncing the entire completed database;
                # per-statement durable schema commits merely delay cold F3.
                conn.execute("PRAGMA journal_mode=OFF")
                conn.execute("PRAGMA synchronous=OFF")
                conn.execute("BEGIN")
                conn.execute("CREATE TABLE sources (id INTEGER PRIMARY KEY, name TEXT UNIQUE, size INTEGER, mtime INTEGER, ctime INTEGER, digest TEXT)")
                conn.execute("CREATE TABLE completions (set_id TEXT, source_id INTEGER, PRIMARY KEY (set_id, source_id))")
                for number, (name, signature) in enumerate(sorted(sources.items())):
                    if signature is None:
                        raise OSError("completion CSV disappeared during indexing")
                    observed, identities = self._read_source(self.directory / name)
                    if observed != signature:
                        raise OSError("completion CSV changed during indexing")
                    conn.execute("INSERT INTO sources VALUES (?, ?, ?, ?, ?, ?)", (number, name, *signature))
                    conn.executemany("INSERT INTO completions VALUES (?, ?)", ((identity, number) for identity in identities))
                if sources != self._sources():
                    raise OSError("completion CSV coverage changed during indexing")
                conn.execute("PRAGMA user_version=2")
                conn.commit()
                if conn.execute("PRAGMA integrity_check").fetchall() != [("ok",)]:
                    raise sqlite3.DatabaseError("completion index integrity failure")
            stamp = self._publish(temporary)
        finally:
            if os.path.exists(temporary):
                os.unlink(temporary)
        self._coverage = sources
        self._stamp = stamp

    def candidates(self, identity):
        """Return locations, [] for covered absence, or None for full fallback."""
        with self._lock:
            try:
                sources = self._sources()
                stamp = self._content_signature(self.path)
                if stamp is None or stamp != self._stamp:
                    self._coverage = None
                # Persisted metadata and even integrity_check cannot prove
                # semantic completeness (valid SQL can delete a completion).
                # Only bytes built from CSV in this instance are trusted;
                # restart or any index-byte change requires a CSV rebuild.
                if self._coverage != sources:
                    self._rebuild(sources)
                with closing(sqlite3.connect(self.path, timeout=.1)) as conn:
                    paths = [str(self.directory / row[0]) for row in conn.execute(
                        "SELECT sources.name FROM completions JOIN sources ON sources.id=completions.source_id WHERE set_id=? ORDER BY sources.name DESC",
                        (identity,),
                    )]
                if sources != self._sources() or self._stamp != self._content_signature(self.path):
                    self._coverage = None
                    return None
                return paths
            except (OSError, sqlite3.Error, UnicodeError, csv.Error):
                self._coverage = None
                return None

    def note_append(self, path, previous_signature, event, details):
        """Advance known coverage only after the writer has closed the CSV.

        Read the entire changed CSV: a pre-write stat does not exclude another
        append or an in-place repair. Never certify only the writer's own row.
        """
        path = Path(path)
        if path.parent != self.directory or not path.name.startswith(self.prefix):
            return
        with self._lock:
            temporary = None
            try:
                if self._coverage is None:
                    return
                if self._content_signature(self.path) != self._stamp:
                    self._coverage = None
                    return
                if event != "TRAY_COMPLETE":
                    # Leave coverage stale until a durable completion or lookup
                    # can recertify all rows, including external completions.
                    return
                signature, identities = self._read_source(path)
                # Only mutate a private copy of the previously certified DB.
                # Concurrent cache edits cannot be blessed by our new stamp.
                descriptor, temporary = tempfile.mkstemp(prefix=self.path.name + ".", dir=self.directory)
                os.close(descriptor)
                shutil.copyfile(self.path, temporary)
                if self._content_signature(temporary)[-1] != self._stamp[-1]:
                    raise OSError("completion index changed while copying")
                with closing(sqlite3.connect(temporary, timeout=.1)) as conn:
                    conn.execute("PRAGMA journal_mode=OFF")
                    conn.execute("PRAGMA synchronous=OFF")
                    with conn:
                        conn.execute(
                            "INSERT INTO sources (name, size, mtime, ctime, digest) VALUES (?, ?, ?, ?, ?) ON CONFLICT(name) DO UPDATE SET size=excluded.size, mtime=excluded.mtime, ctime=excluded.ctime, digest=excluded.digest",
                            (path.name, *signature),
                        )
                        source_id = conn.execute("SELECT id FROM sources WHERE name=?", (path.name,)).fetchone()[0]
                        conn.execute("DELETE FROM completions WHERE source_id=?", (source_id,))
                        conn.executemany("INSERT INTO completions VALUES (?, ?)", ((identity, source_id) for identity in identities))
                        if signature != self._content_signature(path):
                            raise OSError("completion CSV changed during append indexing")
                        if conn.execute("PRAGMA integrity_check").fetchall() != [("ok",)]:
                            raise sqlite3.DatabaseError("completion index integrity failure")
                self._stamp = self._publish(temporary)
                self._coverage[path.name] = signature
            except (OSError, sqlite3.Error, TypeError, ValueError, UnicodeError, csv.Error):
                # A cache failure never turns a fsynced completion into a
                # failed business write. Next lookup validates/rebuilds it.
                self._coverage = None
            finally:
                if temporary is not None and os.path.exists(temporary):
                    try:
                        os.unlink(temporary)
                    except OSError:
                        pass
