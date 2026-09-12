"""Rebuildable completion locations; CSV bytes remain the source of truth."""
from contextlib import closing
import csv
import hashlib
import json
import os
from pathlib import Path
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

    def _sources(self):
        with os.scandir(self.directory) as entries:
            return {
                entry.name: self.signature(entry.path)
                for entry in entries
                if entry.name.startswith(self.prefix) and entry.name.lower().endswith(".csv")
            }

    def _rebuild(self, sources):
        # Build away from the live index. A crash cannot publish partial
        # coverage, and a missing/unreadable CSV cannot prove absence.
        descriptor, temporary = tempfile.mkstemp(prefix=self.path.name + ".", dir=self.directory)
        os.close(descriptor)
        try:
            with closing(sqlite3.connect(temporary)) as conn:
                conn.execute("CREATE TABLE sources (id INTEGER PRIMARY KEY, name TEXT UNIQUE, size INTEGER, mtime INTEGER, ctime INTEGER)")
                conn.execute("CREATE TABLE completions (set_id TEXT, source_id INTEGER, PRIMARY KEY (set_id, source_id))")
                for number, (name, signature) in enumerate(sorted(sources.items())):
                    if signature is None:
                        raise OSError("completion CSV disappeared during indexing")
                    conn.execute("INSERT INTO sources VALUES (?, ?, ?, ?, ?)", (number, name, *signature))
                    with open(self.directory / name, "r", encoding="utf-8-sig", newline="") as handle:
                        identities = set()
                        for row in csv.DictReader(handle):
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
                        conn.executemany("INSERT INTO completions VALUES (?, ?)", ((identity, number) for identity in identities))
                if sources != self._sources():
                    raise OSError("completion CSV coverage changed during indexing")
                conn.execute("PRAGMA user_version=1")
                conn.commit()
            os.replace(temporary, self.path)
        finally:
            if os.path.exists(temporary):
                os.unlink(temporary)
        self._coverage = sources
        self._stamp = self.signature(self.path)

    def candidates(self, identity):
        """Return locations, [] for covered absence, or None for full fallback."""
        with self._lock:
            try:
                sources = self._sources()
                stamp = self.signature(self.path)
                if stamp is None or stamp != self._stamp or self._coverage is None:
                    self._coverage = None
                    if stamp is not None:
                        try:
                            with closing(sqlite3.connect(self.path, timeout=.1)) as conn:
                                if conn.execute("PRAGMA user_version").fetchone()[0] == 1 and conn.execute("PRAGMA quick_check").fetchone()[0] == "ok":
                                    self._coverage = {
                                        row[0]: tuple(row[1:])
                                        for row in conn.execute("SELECT name, size, mtime, ctime FROM sources")
                                    }
                                    # Validate the lookup table before trusting
                                    # complete (possibly empty) coverage.
                                    conn.execute("SELECT set_id, source_id FROM completions LIMIT 1").fetchall()
                        except sqlite3.Error:
                            self._coverage = None
                if self._coverage != sources:
                    self._rebuild(sources)
                with closing(sqlite3.connect(self.path, timeout=.1)) as conn:
                    paths = [str(self.directory / row[0]) for row in conn.execute(
                        "SELECT sources.name FROM completions JOIN sources ON sources.id=completions.source_id WHERE set_id=? ORDER BY sources.name DESC",
                        (identity,),
                    )]
                if sources != self._sources():
                    self._coverage = None
                    return None
                self._stamp = self.signature(self.path)
                return paths
            except (OSError, sqlite3.Error, UnicodeError, csv.Error):
                self._coverage = None
                return None

    def note_append(self, path, previous_signature, event, details):
        """Advance known coverage only after the writer has closed the CSV.

        Non-completion appends update memory only. A restart detects the older
        persisted signature and rebuilds; a durable completion updates both
        its location and signature in one SQLite transaction after CSV fsync.
        """
        path = Path(path)
        if path.parent != self.directory or not path.name.startswith(self.prefix):
            return
        with self._lock:
            try:
                if self._coverage is None:
                    return
                if self.signature(self.path) != self._stamp or self._coverage.get(path.name) != previous_signature:
                    self._coverage = None
                    return
                signature = self.signature(path)
                if signature is None:
                    self._coverage = None
                    return
                if event == "TRAY_COMPLETE":
                    identity = str(json.loads(details).get("set_id") or "").strip()
                    with closing(sqlite3.connect(self.path, timeout=.1)) as conn:
                        with conn:
                            conn.execute(
                                "INSERT INTO sources (name, size, mtime, ctime) VALUES (?, ?, ?, ?) ON CONFLICT(name) DO UPDATE SET size=excluded.size, mtime=excluded.mtime, ctime=excluded.ctime",
                                (path.name, *signature),
                            )
                            if identity:
                                conn.execute(
                                    "INSERT OR IGNORE INTO completions SELECT ?, id FROM sources WHERE name=?",
                                    (identity, path.name),
                                )
                    self._stamp = self.signature(self.path)
                self._coverage[path.name] = signature
            except (OSError, sqlite3.Error, TypeError, ValueError):
                # A cache failure never turns a fsynced completion into a
                # failed business write. Next lookup validates/rebuilds it.
                self._coverage = None
