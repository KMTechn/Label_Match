"""Queued event persistence and atomic current-set storage.

The application facade supplies live callbacks for its clock, file operations,
event projection and file names; this module does not import the Tk application.
"""

import csv
from collections import deque
from contextlib import contextmanager, nullcontext
import hashlib
import io
import json
import os
import queue
import threading
import time
import uuid
import label_worker_roster
from pathlib import Path


class _LogCloseRequest:
    """Identify one close attempt so a cancelled sentinel cannot stop a later one."""


class _DurableLogRetryPending(RuntimeError):
    """A durable non-audit row is retained for a safe preappend retry."""


from event_stream_policy import LOCAL_ONLY_EVENT_TYPES, local_only_event_log_path
from protected_admin import (
    PROTECTED_ADMIN_OPERATOR_ID,
    PROTECTED_ADMIN_ROLE,
    persistent_operator_name,
    redact_protected_admin_code,
    sanitize_persistent_value,
)
from storage_policy import label_match_local_events_dir
from label_recovery_schema import require_recovery_record
from label_safe_path import (describe_entry, entry_digest, read_checked_bytes,
                             quarantine_entry, unlink_checked, write_checked_bytes)
from writer_session_fence import writer_sink


RECOVERY_MAX_BYTES = 8 * 1024 * 1024
AUDIT_SEGMENT_BYTES = 1024 * 1024
STATE_SAVE_LOCK_TIMEOUT_SECONDS = 10.0
GENERAL_LOG_RETRY_BACKOFF_SECONDS = 1.0


def read_recovery_file(path, *, journal_schema=None, current_state=False, expected_bytes=None):
    """Return raw bytes, digest, and a validated value without modifying evidence."""
    result = {"verified": False, "raw": None, "sha256": "", "value": None,
              "reason": "missing", "entry": None}
    try:
        path = Path(path)
        try:
            raw = read_checked_bytes(path)
        except FileNotFoundError:
            return result
        result["raw"] = raw
        result["sha256"] = hashlib.sha256(raw).hexdigest()
        if len(raw) > RECOVERY_MAX_BYTES and not current_state:
            result["reason"] = "oversized"
            return result
        decoded = json.loads(raw.decode("utf-8"))
        if not isinstance(decoded, dict):
            raise ValueError("recovery root is not an object")
        if journal_schema is not None:
            require_recovery_record("journal", decoded, journal_version=journal_schema)
            result["value"] = decoded["state"]
        elif current_state:
            require_recovery_record("current", decoded)
            # Older versions accepted an unbounded inline audit backlog. Keep
            # those checkpoints readable without relaxing the business limit.
            business = {key: value for key, value in decoded.items()
                        if key not in ('scanner_audit_outbox', 'scanner_audit_segments')}
            if len(json.dumps(business, ensure_ascii=False, indent=4).encode('utf-8')) > RECOVERY_MAX_BYTES:
                result['reason'] = 'oversized'
                return result
            result["value"] = decoded
        else:
            result["value"] = decoded
        if expected_bytes is not None and raw != expected_bytes:
            raise ValueError("recovery bytes differ from durable record")
        if current_state:
            pending = list(decoded.get('scanner_audit_outbox', []))
            unresolved = []
            for digest in decoded.get('scanner_audit_segments', []):
                segment = read_recovery_file(path.parent / '_scanner_audit_outbox' / (digest + '.json'))
                try:
                    if not segment['verified'] or segment['sha256'] != digest:
                        raise ValueError('audit segment bytes are unverified')
                    require_recovery_record('scanner_audit_segment', segment['value'])
                    pending.extend(segment['value']['scanner_audit_outbox'])
                except Exception as error:
                    unresolved.append(digest)
                    print(f'스캐너 감사 조각 재생 대기: {digest}: {error}')
            if pending:
                decoded['scanner_audit_outbox'] = pending
            result['unverified_audit_segments'] = unresolved
        result["verified"] = True
        result["reason"] = ""
    except Exception as exc:
        result["reason"] = ('oversized' if result['raw'] is not None and
                            len(result['raw']) > RECOVERY_MAX_BYTES else type(exc).__name__)
        if result["raw"] is None:
            try:
                result["entry"] = describe_entry(path)
                result["sha256"] = entry_digest(path, result["entry"])
            except Exception:
                result["sha256"] = entry_digest(path, {})
    return result


class DataManager:
    def __init__(
        self,
        save_dir,
        process_name,
        worker_name,
        unique_id,
        *,
        authenticated_admin=False,
        open_file,
        clock,
        enrich_event,
        datetime_encoder,
        current_state_filename,
        durable_event_types,
        csv_safe_cell,
    ):
        self._open_file = open_file
        self._clock = clock
        self._enrich_event = enrich_event
        self._datetime_encoder = datetime_encoder
        self._current_state_filename = current_state_filename
        self._durable_event_types = durable_event_types
        self._csv_safe_cell = csv_safe_cell
        self.save_directory = save_dir
        self.local_event_directory = str(
            label_match_local_events_dir(save_dir)
        )
        self.process_name = redact_protected_admin_code(process_name)
        self.worker_name = str(worker_name or "").strip()
        self.worker_role = (
            PROTECTED_ADMIN_ROLE
            if authenticated_admin and self.worker_name == PROTECTED_ADMIN_OPERATOR_ID
            else "PACKAGING"
        )
        self.persistent_worker_name = persistent_operator_name(self.worker_name)
        self.record_context = None
        self.unique_id = redact_protected_admin_code(unique_id)
        self.log_queue = queue.Queue()
        self._close_lock = threading.Lock()
        self._close_requested = False
        self._close_request = None
        self._writer_stopping = False
        self._writer_errors = []
        self._writer_error_keys = {}
        self._recovered_audit_errors = set()
        self._log_retry_lock = threading.Lock()
        self._log_retry_items = {}
        self._log_retry_errors = {}
        self._unresolved_log_items = {}
        self._state_lock = threading.RLock()
        self._checkpoint_generation = 0
        self._force_exit_prepared_checkpoint = None
        self._archived_audit_outbox = []
        self._audit_csv_lock = threading.RLock()
        self._audit_csv_indexes = {}
        self._last_state_write_failure = None
        self._state_write_failure_attempts = {}
        evidence = read_recovery_file(
            os.path.join(save_dir, self._current_state_filename()), current_state=True)
        self._audit_outbox = list((evidence['value'] or {}).get('scanner_audit_outbox', [])) if evidence['verified'] else []
        self._unverified_audit_segments = evidence.get('unverified_audit_segments', [])
        prefixes = (evidence['value'] or {}).get('scanner_audit_csv_prefixes', {}) if evidence['verified'] else {}
        self._audit_csv_prefixes = dict(prefixes) if isinstance(prefixes, dict) else {}
        if not isinstance(prefixes, dict):
            print('감사 CSV 증명 손상: 기존 전 행 검증으로 재생')
        self.log_thread = threading.Thread(target=self._log_writer_thread, daemon=True)
        self.log_thread.start()
    def _get_log_filepath(self, target_date=None):
        if target_date is None:
            target_date = self._clock().now()
        filename = f"{self.process_name}작업이벤트로그_{self.unique_id}_{target_date.strftime('%Y%m%d')}.csv"
        return os.path.join(self.save_directory, filename)
    def _get_log_filepath_for_item(self, log_item):
        try:
            target_date = self._clock().fromisoformat(str(log_item[0]))
        except Exception:
            target_date = self._clock().now()
        contract_path = self._get_log_filepath(target_date)
        if str(log_item[2] or "") in LOCAL_ONLY_EVENT_TYPES:
            return str(
                local_only_event_log_path(
                    contract_path,
                    local_events_dir=self.local_event_directory,
                )
            )
        return contract_path
    def _log_writer_thread(self):
        audit_stream = None
        audit_context = None
        audit_path = None
        last_audit_key = ''
        pending_general = {}
        blocked_paths = set()
        no_pending_path = object()
        retry_blocked = False
        round_remaining = 0
        while True:
            log_item = None
            got_item = False
            failed_write = False
            append_started = False
            retry_write = False
            filepath = None
            try:
                pending_path = next((path for path in pending_general if path not in blocked_paths),
                                    no_pending_path)
                if pending_path is not no_pending_path:
                    pending = pending_general[pending_path]
                    log_item = pending.popleft()
                    if not pending:
                        del pending_general[pending_path]
                elif retry_blocked:
                    if round_remaining <= 0:
                        raise queue.Empty
                    log_item = self.log_queue.get_nowait()
                    round_remaining -= 1
                else:
                    log_item = self.log_queue.get()
                got_item = True
                if isinstance(log_item, _LogCloseRequest):
                    with self._close_lock:
                        if log_item is not self._close_request:
                            continue
                        if self.has_pending_log_retries or self._has_pending_durable_log_retries:
                            self.log_queue.put(log_item)
                            continue
                        self._writer_stopping = True
                    break
                general_item = self._is_general_log_item(log_item)
                # An unresolved route keeps the old conservative ordering until
                # it can be identified; known independent CSVs keep progressing.
                if not (retry_blocked and general_item and None in blocked_paths):
                    filepath = self._get_log_filepath_for_item(log_item)
                if retry_blocked and general_item and (None in blocked_paths or filepath in blocked_paths):
                    pending_general.setdefault(filepath, deque()).append(log_item)
                    with self._log_retry_lock:
                        self._log_retry_items[id(log_item)] = log_item
                        self._unresolved_log_items[id(log_item)] = log_item
                    got_item = False  # This buffered row still owns its queue task.
                    continue
                file_exists = os.path.exists(filepath)
                os.makedirs(os.path.dirname(filepath), exist_ok=True)
                audit_key = self._audit_key(log_item)
                with self._audit_csv_lock:
                    # Keep the same handle only for a contiguous audit burst.
                    # Every row still flushes and fsyncs before task_done/ACK.
                    if audit_stream is not None and (not audit_key or not file_exists or audit_path != filepath
                            or os.stat(filepath).st_ino != os.fstat(audit_stream.fileno()).st_ino):
                        context = audit_context
                        audit_context = None
                        audit_stream = None
                        # A previous audit handle's close is not a retryable
                        # failure of this ordinary row's open/stat.
                        append_started = True
                        context.__exit__(None, None, None)
                        append_started = False
                    if audit_key:
                        if audit_stream is None:
                            context = self._open_file(filepath, 'a+', newline='', encoding='utf-8-sig')
                            audit_stream = context.__enter__()
                            audit_context = context
                            audit_path = filepath
                        last_audit_key = audit_key
                        file_context = nullcontext(audit_stream)
                    else:
                        file_context = self._open_file(filepath, 'a', newline='', encoding='utf-8-sig')
                with self._audit_csv_lock, file_context as f:
                    index = None
                    if audit_key:
                        index = self._audit_csv_index(f, filepath)
                        previous = index['keys'].get(audit_key)
                        if audit_key in index['conflicts'] or (previous is not None and previous != log_item):
                            raise ValueError('audit key has different CSV content')
                        f.seek(0, os.SEEK_END)
                        if previous is not None:
                            f.flush()
                            os.fsync(f.fileno())
                            continue
                    writer = csv.writer(f)
                    empty = not file_exists or os.stat(filepath).st_size == 0
                    header = ["timestamp", "worker_name", "event", "details"]
                    if empty:
                        append_started = True
                        writer.writerow(header)
                    append_started = True
                    writer.writerow(log_item)
                    if audit_key or str(log_item[2] or "") in self._durable_event_types():
                        f.flush()
                        os.fsync(f.fileno())
                    if index is not None:
                        appended = (self._csv_row_bytes(header) if empty else b'') + self._csv_row_bytes(log_item)
                        if empty:
                            appended = b'\xef\xbb\xbf' + appended
                        index['hash'].update(appended)
                        index['size'] += len(appended)
                        index['rows'] += 1
                        index['last_row_sha256'] = hashlib.sha256(self._csv_row_bytes(log_item)).hexdigest()
                        index['keys'][audit_key] = list(log_item)
                        index['signature'] = self._csv_signature(f)
                        self._audit_csv_prefixes[index['relative']] = self._csv_prefix(index)
                self._complete_general_log_retry(log_item)
            except queue.Empty:
                round_remaining = 0
                continue
            except Exception as e:
                failed_write = True
                retry_write = (isinstance(e, OSError) and not append_started
                               and self._is_non_audit_log_item(log_item))
                self._remember_log_write_failure(log_item, e, retry=retry_write)
                if retry_write:
                    # Reuse the exact timestamp, worker, event and details.
                    pending_general.setdefault(filepath, deque()).appendleft(log_item)
                    got_item = False
                    if not retry_blocked:
                        round_remaining = self.log_queue.qsize()
                    blocked_paths.add(filepath)
                    retry_blocked = True
                print(f"로그 쓰기 스레드 오류: {e}")
            finally:
                if audit_stream is not None and (failed_write or isinstance(log_item, _LogCloseRequest)
                                                 or self.log_queue.empty()):
                    context = audit_context
                    audit_context = None
                    audit_stream = None
                    try:
                        context.__exit__(None, None, None)
                    except Exception as error:
                        self._writer_errors.append(error)
                        self._writer_error_keys[id(error)] = self._audit_key(log_item) or last_audit_key
                        print(f"로그 쓰기 스레드 오류: {error}")
                if got_item:
                    self.log_queue.task_done()
                if (retry_blocked and round_remaining <= 0
                        and not any(path not in blocked_paths for path in pending_general)):
                    time.sleep(GENERAL_LOG_RETRY_BACKOFF_SECONDS)
                    retry_blocked = False
                    blocked_paths.clear()
    @property
    def has_pending_log_retries(self):
        with self._log_retry_lock:
            return any(self._is_general_log_item(item) for item in self._log_retry_items.values())
    @property
    def _has_pending_durable_log_retries(self):
        with self._log_retry_lock:
            return any(not self._is_general_log_item(item) for item in self._log_retry_items.values())
    def _is_non_audit_log_item(self, log_item):
        return log_item is not None and not self._audit_key(log_item)
    def _is_general_log_item(self, log_item):
        return (self._is_non_audit_log_item(log_item)
                and str(log_item[2] or '') not in self._durable_event_types())
    def _remember_log_write_failure(self, log_item, error, *, retry):
        with self._log_retry_lock:
            item_id = id(log_item)
            if self._is_non_audit_log_item(log_item):
                self._unresolved_log_items[item_id] = log_item
            if retry and item_id in self._log_retry_errors:
                return  # Retain the first failure without an unbounded error list.
            if not retry:
                self._log_retry_items.pop(item_id, None)
                self._log_retry_errors.pop(item_id, None)
            self._writer_errors.append(error)
            self._writer_error_keys[id(error)] = self._audit_key(log_item)
            if retry:
                self._log_retry_items[item_id] = log_item
                self._log_retry_errors[item_id] = id(error)
    def _complete_general_log_retry(self, log_item):
        with self._log_retry_lock:
            item_id = id(log_item)
            self._unresolved_log_items.pop(item_id, None)
            self._log_retry_items.pop(item_id, None)
            recovered = self._log_retry_errors.pop(item_id, None)
            if recovered is not None:
                self._recovered_audit_errors.add(recovered)
    @staticmethod
    def _csv_signature(stream):
        stat = os.fstat(stream.fileno())
        return (stat.st_dev, stat.st_ino, stat.st_size, stat.st_mtime_ns, stat.st_ctime_ns)
    @staticmethod
    def _csv_row_bytes(row):
        stream = io.StringIO(newline='')
        csv.writer(stream).writerow(row)
        return stream.getvalue().encode('utf-8')
    @staticmethod
    def _csv_prefix(index):
        return {name: index[name] for name in ('size', 'rows', 'last_row_sha256')} | {
            'sha256': index['hash'].hexdigest()}
    def _csv_prefix_matches(self, raw, proof):
        try:
            if not isinstance(proof, dict) or set(proof) != {'size', 'rows', 'sha256', 'last_row_sha256'}:
                return False
            if any(type(proof[name]) is not int or proof[name] < 0 for name in ('size', 'rows')):
                return False
            prefix = raw[:proof['size']]
            if len(prefix) != proof['size'] or hashlib.sha256(prefix).hexdigest() != proof['sha256']:
                return False
            rows = list(csv.reader(io.StringIO(prefix.decode('utf-8-sig'), newline='')))
            last_hash = hashlib.sha256(self._csv_row_bytes(rows[-1])).hexdigest() if len(rows) > 1 else ''
            return proof['rows'] == max(0, len(rows)-1) and proof['last_row_sha256'] == last_hash
        except (KeyError, TypeError, ValueError, csv.Error):
            return False
    def _audit_csv_index(self, stream, filepath):
        """Validate a prefix once, then index only this writer's durable appends."""
        signature = self._csv_signature(stream)
        previous = self._audit_csv_indexes.get(filepath)
        if previous is not None and previous['signature'] == signature:
            return previous
        stream.seek(0)
        raw = stream.buffer.read()
        stream.seek(0)
        relative = Path(filepath).absolute().relative_to(Path(self.save_directory).absolute()).as_posix()
        proof = self._csv_prefix(previous) if previous is not None else self._audit_csv_prefixes.get(relative)
        if (previous is not None or relative in self._audit_csv_prefixes) and not self._csv_prefix_matches(raw, proof):
            print('감사 CSV 증명 불일치: 기존 전 행 검증으로 재생')
        # A proof is only an optimization hint. Always rebuild from all rows on
        # restart/change, using exactly the old csv.reader and key comparison.
        # Identical duplicates, reordered rows and unrelated malformed rows did
        # not globally stop the old writer and must not gain new hard errors.
        rows = list(csv.reader(io.StringIO(raw.decode('utf-8-sig'), newline='')))
        keys, conflicts = {}, set()
        for row in rows:
            key = self._audit_key(row)
            if key:
                if key in keys and keys[key] != row:
                    conflicts.add(key)
                keys[key] = row
        index = dict(signature=signature, relative=relative, hash=hashlib.sha256(raw),
                     size=len(raw), rows=max(0, len(rows)-1), keys=keys, conflicts=conflicts,
                     last_row_sha256=hashlib.sha256(self._csv_row_bytes(rows[-1])).hexdigest() if len(rows) > 1 else '')
        self._audit_csv_indexes[filepath] = index
        return index
    def _verify_audit_csv_prefixes(self, keys, ordered_keys):
        # A fresh byte read before ACK also catches same-size edits whose file
        # times were restored. Metadata is only a cache invalidation hint.
        failed = set()
        with self._audit_csv_lock:
            for filepath, index in list(self._audit_csv_indexes.items()):
                if keys.isdisjoint(index['keys']):
                    continue
                with self._open_file(filepath, 'rb') as stream:
                    raw = stream.read()
                if len(raw) != index['size'] or hashlib.sha256(raw).hexdigest() != index['hash'].hexdigest():
                    expected = {key: index['keys'][key] for key in ordered_keys
                                if key in keys and key in index['keys']}
                    # A restored stat can hide a byte edit during a burst. Use
                    # the same full-row verdict, retaining only affected keys.
                    self._audit_csv_indexes.pop(filepath)
                    with self._open_file(filepath, 'a+', newline='', encoding='utf-8-sig') as stream:
                        index = self._audit_csv_index(stream, filepath)
                        for key, row in expected.items():
                            previous = index['keys'].get(key)
                            if key in index['conflicts'] or (previous is not None and previous != row):
                                error = ValueError('audit key has different CSV content')
                                self._writer_errors.append(error)
                                self._writer_error_keys[id(error)] = key
                                failed.add(key)
                            elif previous is None:
                                # The old full scan would append a missing key,
                                # even if a restored stat hid its removal.
                                stream.seek(0, os.SEEK_END)
                                empty = index['size'] == 0
                                header = ['timestamp', 'worker_name', 'event', 'details']
                                writer = csv.writer(stream)
                                if empty:
                                    writer.writerow(header)
                                writer.writerow(row)
                                stream.flush()
                                os.fsync(stream.fileno())
                                appended = self._csv_row_bytes(row)
                                if empty:
                                    appended = b'\xef\xbb\xbf' + self._csv_row_bytes(header) + appended
                                index['hash'].update(appended)
                                index['size'] += len(appended)
                                index['rows'] += 1
                                index['last_row_sha256'] = hashlib.sha256(self._csv_row_bytes(row)).hexdigest()
                                index['keys'][key] = row
                                index['signature'] = self._csv_signature(stream)
                self._audit_csv_prefixes[index['relative']] = self._csv_prefix(index)
        return failed
    @staticmethod
    def _csv_details(details):
        details = dict(details)
        for field in ('scanner_exchange', 'scanner_exchanges'):
            value = details.get(field)
            if isinstance(value, dict):
                details[field] = {key: item for key, item in value.items()
                                  if key not in ('worker_id', 'worker_name_snapshot')}
            elif isinstance(value, list):
                details[field] = [{key: item for key, item in pair.items()
                                   if key not in ('worker_id', 'worker_name_snapshot')}
                                  for pair in value]
        return details

    def log_event(self, event_type, details, *, worker_identity=None, identity_enabled=None):
        details = dict(details or {})
        identity, enabled = self.record_context() if callable(self.record_context) else ({}, False)
        if worker_identity is not None:
            identity = worker_identity
        if identity_enabled is not None:
            enabled = identity_enabled
        # Audit details are already the immutable CSV projection, including the
        # capability decision made before their durable checkpoint.
        if not details.get('scanner_audit') and identity and enabled:
            details.setdefault('worker_id', identity['worker_id'])
            details.setdefault('worker_name_snapshot', identity['worker_name_snapshot'])
        if not details.get('scanner_audit'):
            details = self._csv_details(details)
        enriched_details = sanitize_persistent_value(
            self._enrich_event(event_type, details or {}, self.unique_id)
        )
        detail_text = json.dumps(
            enriched_details,
            ensure_ascii=False,
            cls=self._datetime_encoder(),
        )
        audit = enriched_details.get('scanner_audit') or {}
        log_item = [
            audit.get('occurred_at') or self._clock().now().isoformat(),
            self._csv_safe_cell(persistent_operator_name(audit.get('operator',
                identity.get('worker_name_snapshot', self.worker_name)))),
            redact_protected_admin_code(event_type),
            redact_protected_admin_code(detail_text),
        ]
        with self._close_lock:
            if self._close_requested:
                raise RuntimeError("DataManager is closing; new log events are not accepted")
            self.log_queue.put(log_item)
    @staticmethod
    def _audit_key(log_item):
        try:
            return str(json.loads(log_item[3]).get('scanner_audit', {}).get('key') or '')
        except (TypeError, IndexError, AttributeError, ValueError):
            return ''
    def prepare_audit_event(self, event_type, details, *, worker_identity=None):
        details = self._csv_details(details)
        identity, enabled = self.record_context() if callable(self.record_context) else ({}, False)
        if worker_identity is not None:
            identity = worker_identity
        if identity and enabled:
            details.update(identity)
        details['scanner_audit'] = {
            'key': uuid.uuid4().hex,
            'occurred_at': self._clock().now().isoformat(),
            'operator': persistent_operator_name(identity.get('worker_name_snapshot', self.worker_name)),
        }
        return {'event_type': event_type, **identity, 'details': sanitize_persistent_value(
            self._enrich_event(event_type, details, self.unique_id))}
    def replay_audit_events(self, timeout=5, *, archived_events=()):
        """Replay the saved CSV projection; a failure leaves the checkpoint intact."""
        with self._state_lock:
            try:
                if self._unverified_audit_segments:
                    evidence = read_recovery_file(
                        Path(self.save_directory) / self._current_state_filename(), current_state=True)
                    if evidence['verified']:
                        restored = {item['details']['scanner_audit']['key']: item
                                    for item in self._audit_outbox}
                        for item in evidence['value'].get('scanner_audit_outbox', []):
                            key = item['details']['scanner_audit']['key']
                            if key in restored and restored[key] != item:
                                raise ValueError('repaired audit key has different content')
                            restored[key] = item
                        self._audit_outbox = list(restored.values())
                        self._unverified_audit_segments = evidence.get('unverified_audit_segments', [])
                archived = {item['details']['scanner_audit']['key']: item
                            for item in self._archived_audit_outbox}
                for item in archived_events:
                    require_recovery_record('scanner_audit_event', item)
                    key = item['details']['scanner_audit']['key']
                    if key in archived and archived[key] != item:
                        raise ValueError('held audit key has different content')
                    archived[key] = item
                self._archived_audit_outbox = list(archived.values())
                pending_by_key = dict(archived)
                for item in self._audit_outbox:
                    key = item['details']['scanner_audit']['key']
                    if key in pending_by_key and pending_by_key[key] != item:
                        raise ValueError('active and held audit content differ')
                    pending_by_key[key] = item
                # The verified immutable hold owns these intents now. Moving
                # them out of the active cache keeps audit ACKs from rewriting
                # a current file whose bytes are already bound to a PIN.
                self._audit_outbox = [item for item in self._audit_outbox
                    if item['details']['scanner_audit']['key'] not in archived]
            except Exception as error:
                print(f'보류 감사 재생 대기: {error}')
                return False
            if not pending_by_key:
                return not self._unverified_audit_segments
            pending = list(pending_by_key.values())
            keys = {item['details']['scanner_audit']['key'] for item in pending}
            before_errors = len(self._writer_errors)
            try:
                failed = set()
                for item in pending:
                    try:
                        self.log_event(item['event_type'], item['details'])
                    except Exception as error:
                        failed.add(item['details']['scanner_audit']['key'])
                        print(f'스캐너 감사 append 대기: {error}')
                try:
                    self.flush(timeout=timeout)
                except RuntimeError as error:
                    # Keep the original failures as evidence. Only successful
                    # replay of their own audit keys can recover writer health.
                    if not self._writer_errors or not str(error).startswith('Log writer failed: '):
                        raise
                failed.update(self._writer_error_keys[id(error)]
                              for error in self._writer_errors[before_errors:])
                completed = keys - failed
                if not completed:
                    return False
                failed.update(self._verify_audit_csv_prefixes(completed,
                    [item['details']['scanner_audit']['key'] for item in pending]))
                completed -= failed
                if not completed:
                    return False
                active_completed = completed & {item['details']['scanner_audit']['key']
                                                for item in self._audit_outbox}
                if active_completed:
                    state = self.load_current_state()
                    if state is None or not self.save_current_state(state, _completed_audit_keys=active_completed):
                        return False
                self._archived_audit_outbox = [item for item in self._archived_audit_outbox
                    if item['details']['scanner_audit']['key'] not in completed]
                self._recovered_audit_errors.update(
                    id(error) for error in self._writer_errors
                    if self._writer_error_keys.get(id(error)) in completed)
                return not failed and not self._unverified_audit_segments
            except Exception as error:
                print(f'스캐너 감사 재생 대기: {error}')
                return False
    def close(self, timeout=None):
        with self._close_lock:
            if not self._close_requested:
                self._close_requested = True
                if self.log_thread.is_alive():
                    self._close_request = _LogCloseRequest()
                    self.log_queue.put(self._close_request)
        self.log_thread.join(timeout)
        if self.log_thread.is_alive():
            raise TimeoutError("Log writer did not stop before timeout")
        if any(id(error) not in self._recovered_audit_errors for error in self._writer_errors):
            raise RuntimeError(f"Log writer failed: {self._writer_errors[-1]}")
        return True
    def _cancel_close_request(self):
        with self._close_lock:
            if (not self._close_requested or self._writer_stopping
                    or not self.log_thread.is_alive()):
                return False
            self._close_requested = False
            self._close_request = None
            return True
    def flush(self, timeout=None):
        deadline = None if timeout is None else time.monotonic() + timeout
        while self.log_queue.unfinished_tasks:
            with self._log_retry_lock:
                if self.log_queue.unfinished_tasks <= len(self._log_retry_items):
                    break  # Surface durable retries below; ordinary rows do not block work.
            if deadline is not None and time.monotonic() >= deadline:
                raise TimeoutError("Log writer did not flush before timeout")
            time.sleep(0.01)
        with self._log_retry_lock:
            retry_errors = {error_id for item_id, error_id in self._log_retry_errors.items()
                            if self._is_general_log_item(self._log_retry_items[item_id])}
            durable_retry_errors = set(self._log_retry_errors.values()) - retry_errors
            unresolved = [error for error in self._writer_errors
                          if id(error) not in self._recovered_audit_errors and id(error) not in retry_errors]
        if unresolved:
            error_type = (_DurableLogRetryPending
                          if all(id(error) in durable_retry_errors for error in unresolved) else RuntimeError)
            raise error_type(f"Log writer failed: {unresolved[-1]}")
        return True
    @contextmanager
    def _bounded_save_lock(self, lock, *, timeout_seconds=STATE_SAVE_LOCK_TIMEOUT_SECONDS):
        if not lock.acquire(timeout=timeout_seconds):
            raise TimeoutError("저장 잠금 대기 상한")
        try:
            yield
        finally:
            lock.release()

    def save_current_state(self, state_data, *, _completed_audit_keys=(), _force_exit=False,
                           _force_exit_generation=None):
        checkpoint_write = False
        deadline = time.monotonic() + 0.25 if _force_exit else None
        lock_timeout = 0 if _force_exit else STATE_SAVE_LOCK_TIMEOUT_SECONDS
        try:
            with self._bounded_save_lock(self._state_lock, timeout_seconds=lock_timeout):
                state_path = os.path.join(self.save_directory, self._current_state_filename())
                if _force_exit:
                    if _force_exit_generation != self.__dict__.get('_checkpoint_generation', 0):
                        return False  # Normal persistence superseded this memory snapshot.
                    payload = self.__dict__.get('_force_exit_prepared_checkpoint')
                    if not payload:
                        return False
                    # Metadata, audit references and schema were proved during
                    # normal saving. Reuse only those bounded immutable bytes;
                    # never walk/serialize a large audit backlog in this hook.
                    prepared = json.loads(payload.decode('utf-8'))
                    snapshot = json.loads(json.dumps(sanitize_persistent_value(state_data),
                                                     cls=self._datetime_encoder()))
                    if prepared['current_set_info'] != snapshot.get('current_set_info'):
                        return False
                    audits = {item['details']['scanner_audit']['key']: item
                              for item in prepared.get('scanner_audit_outbox', [])}
                    if any(audits.get(item['details']['scanner_audit']['key']) != item
                           for item in snapshot.get('scanner_audit_outbox', [])):
                        return False
                    if time.monotonic() >= deadline:
                        return False
                    write_checked_bytes(state_path, payload, allowed_root=self.save_directory)
                    self._checkpoint_generation += 1
                    self._audit_outbox = list(audits.values())
                    self._last_state_write_failure = None
                    return True
                state_data_with_worker = sanitize_persistent_value(dict(state_data or {}))
                with self._bounded_save_lock(self._audit_csv_lock, timeout_seconds=lock_timeout):
                    if self._audit_csv_prefixes:
                        state_data_with_worker['scanner_audit_csv_prefixes'] = dict(self._audit_csv_prefixes)
                pending = {item['details']['scanner_audit']['key']: item for item in self._audit_outbox}
                for item in state_data_with_worker.get('scanner_audit_outbox', []):
                    require_recovery_record('scanner_audit_event', item)
                    key = item['details']['scanner_audit']['key']
                    if key in pending and pending[key] != item:
                        raise ValueError('audit key has different checkpoint content')
                    pending[key] = item
                for key in _completed_audit_keys:
                    pending.pop(key, None)
                if pending:
                    state_data_with_worker['scanner_audit_outbox'] = list(pending.values())
                else:
                    state_data_with_worker.pop('scanner_audit_outbox', None)
                current = state_data_with_worker.get('current_set_info') or {}
                identity = label_worker_roster.record_identity(current.get('shortage_undo_snapshot') or current)
                state_data_with_worker.update(identity)
                state_data_with_worker['worker_name'] = persistent_operator_name(
                    identity.get('worker_name_snapshot', state_data_with_worker.get('worker_name', self.worker_name)
                        if _completed_audit_keys else self.worker_name))
                encoded = json.dumps(state_data_with_worker, ensure_ascii=False, indent=4,
                                     cls=self._datetime_encoder())
                require_recovery_record("current", json.loads(encoded))
                # A checkpoint commits the references only after all immutable
                # segments are durable. Unreferenced files from interrupted
                # writes are preserved, but never replayed as accepted actions.
                state_data_with_worker.pop('scanner_audit_segments', None)
                segments = list(self._unverified_audit_segments)
                if len(encoded.encode('utf-8')) > RECOVERY_MAX_BYTES and pending:
                    pieces, inline, chunk, chunk_bytes = [], [], [], 0
                    for item in pending.values():
                        item_bytes = len(json.dumps(item, ensure_ascii=False, indent=4,
                                                   cls=self._datetime_encoder()).encode('utf-8'))
                        # A single unusually large event remains inline and
                        # uses the same legacy audit-only size exemption.
                        if item_bytes > AUDIT_SEGMENT_BYTES // 2:
                            inline = list(pending.values())
                            pieces, chunk = [], []
                            break
                        if chunk and chunk_bytes + item_bytes > AUDIT_SEGMENT_BYTES // 2:
                            pieces.append(chunk)
                            chunk, chunk_bytes = [], 0
                        chunk.append(item)
                        chunk_bytes += item_bytes
                    if chunk:
                        pieces.append(chunk)
                    for piece in pieces:
                        payload = json.dumps({'scanner_audit_outbox': piece}, ensure_ascii=False,
                                             indent=4, cls=self._datetime_encoder()).encode('utf-8')
                        if len(payload) > RECOVERY_MAX_BYTES:
                            raise ValueError('audit segment exceeds recovery size limit')
                        digest = hashlib.sha256(payload).hexdigest()
                        segment_path = Path(self.save_directory) / '_scanner_audit_outbox' / (digest + '.json')
                        try:
                            existing = read_checked_bytes(segment_path, allowed_root=self.save_directory)
                        except FileNotFoundError:
                            write_checked_bytes(segment_path, payload, allowed_root=self.save_directory,
                                                replace=False)
                        else:
                            if existing != payload:
                                raise ValueError('existing audit segment bytes differ')
                        segments.append(digest)
                    if inline:
                        state_data_with_worker['scanner_audit_outbox'] = inline
                    else:
                        state_data_with_worker.pop('scanner_audit_outbox', None)
                if segments:
                    state_data_with_worker['scanner_audit_segments'] = list(dict.fromkeys(segments))
                encoded = json.dumps(state_data_with_worker, ensure_ascii=False, indent=4,
                                     cls=self._datetime_encoder())
                require_recovery_record('current', json.loads(encoded))
                business = {key: value for key, value in state_data_with_worker.items()
                            if key not in ('scanner_audit_outbox', 'scanner_audit_segments')}
                if len(json.dumps(business, ensure_ascii=False, indent=4,
                                  cls=self._datetime_encoder()).encode('utf-8')) > RECOVERY_MAX_BYTES:
                    raise ValueError('current business state exceeds recovery size limit')
                checkpoint_payload = encoded.encode('utf-8')
                self._force_exit_prepared_checkpoint = checkpoint_payload if len(checkpoint_payload) <= 65536 else None
                checkpoint_write = True
                write_checked_bytes(state_path, checkpoint_payload,
                                    allowed_root=self.save_directory)
                self._checkpoint_generation = self.__dict__.get('_checkpoint_generation', 0) + 1
                self._audit_outbox = list(pending.values())
                self._last_state_write_failure = None
                self._state_write_failure_attempts.pop(str(current.get('id') or ''), None)
                return True
        except TimeoutError as e:
            if _force_exit:
                return False
            self._last_state_write_failure = None
            if checkpoint_write:
                # Writer admission waits do not prove a checkpoint IO failure.
                raise
            print(f"임시 상태 저장 실패: {e}")
            return False
        except Exception as e:
            if _force_exit:
                return False
            self._last_state_write_failure = None
            if checkpoint_write and isinstance(e, OSError):
                current = (state_data or {}).get('current_set_info') or {}
                set_id = str(current.get('id') or '')
                if set_id:
                    attempts = self._state_write_failure_attempts.get(set_id, 0) + 1
                    self._state_write_failure_attempts[set_id] = attempts
                    saved = json.loads(json.dumps(current, cls=self._datetime_encoder()))
                    self._last_state_write_failure = (e, saved, attempts, len(checkpoint_payload))
            print(f"임시 상태 저장 실패: {e}")
            return False

    def hold_failed_checkpoint(self, outbox, *, source_phs2, source_input_tag_id):
        """Keep a failed checkpoint UNKNOWN; never replay its uncommitted audits."""
        failure = self._last_state_write_failure
        if not failure or failure[2] < 2:
            return None
        error, current, _attempts, checkpoint_bytes = failure
        set_id = str(current['id'])
        root = Path(self.save_directory).absolute()
        active = root / self._current_state_filename()
        original = read_recovery_file(active, current_state=True)
        if original['reason'] != 'missing' and not original['verified']:
            raise OSError('checkpoint original cannot be verified for isolation')
        original_id = str(((original['value'] or {}).get('current_set_info') or {}).get('id') or '')
        if original_id and original_id != set_id:
            raise OSError('another set owns the current checkpoint')
        description = describe_entry(active, allowed_root=root) if os.path.lexists(active) else None
        revision = outbox.workbench_hold_revision()
        recent = [json.loads(row['snapshot_json'])['storage_failure']['recorded_at']
                  for row in outbox.list_workbench_holds() if row.get('reason') == 'STORAGE_WRITE_FAILURE']
        with self._state_lock:
            pending = list(self._audit_outbox)
        archive = str(active) + '.held-' + original['sha256'] if original['raw'] is not None else ''
        snapshot = {'current_set_info': current, 'scanner_audit_outbox': pending,
                    'archive_path': archive, 'current_file_sha256': original['sha256'],
                    'storage_failure': {'outcome': 'UNKNOWN', 'file': str(active),
                        'error_type': type(error).__name__, 'error': str(error),
                        'checkpoint_bytes': checkpoint_bytes,
                        'recorded_at': self._clock().now().isoformat(), 'scan_order': list(current.get('raw') or [])}}
        require_recovery_record('current', snapshot)
        if len(json.dumps(snapshot, ensure_ascii=False).encode('utf-8')) > 65536:
            raise OSError('checkpoint hold exceeds the small writer admission bound')
        held = self._isolate_failed_checkpoint(outbox, failure, snapshot, original, description,
            revision, recent, source_phs2=source_phs2, source_input_tag_id=source_input_tag_id)
        saved = json.loads(held['snapshot_json'])
        archive = saved['archive_path']
        if archive and hashlib.sha256(read_checked_bytes(archive, allowed_root=root)).hexdigest() != saved['current_file_sha256']:
            raise OSError('held checkpoint readback differs')
        with self._state_lock:
            if self._last_state_write_failure is not failure or self._audit_outbox != pending:
                raise OSError('checkpoint audit changed while isolation was verified')
            self._audit_outbox = []
        return held

    @writer_sink("data_manager_storage_failure_hold")
    def _isolate_failed_checkpoint(self, outbox, failure, snapshot, original, description,
                                  revision, recent, *, source_phs2, source_input_tag_id):
        root = Path(self.save_directory).absolute()
        active = root / self._current_state_filename()
        set_id = str(snapshot['current_set_info']['id'])
        with self._state_lock:
            if (self._last_state_write_failure is not failure
                    or self._audit_outbox != snapshot['scanner_audit_outbox']
                    or self._unverified_audit_segments or self.log_queue.unfinished_tasks
                    or outbox.workbench_hold_revision() != revision):
                raise OSError('checkpoint or hold history changed before isolation')
            held = outbox.get_workbench_hold(set_id)
            if held and held.get('reason') != 'STORAGE_WRITE_FAILURE':
                raise OSError('another hold already owns this checkpoint')
            if not held:
                now = self._clock().now()
                if sum(0 <= now.timestamp() - self._clock().fromisoformat(value).timestamp() <= 600
                       for value in recent) >= 3:
                    raise OSError('four distinct checkpoint failures within ten minutes')
                snapshot['storage_failure']['recorded_at'] = now.isoformat()
                if len(json.dumps(snapshot, ensure_ascii=False).encode('utf-8')) > 65536:
                    raise OSError('checkpoint hold exceeds the small writer admission bound')
            probe = root / ('._storage_probe_' + uuid.uuid4().hex)
            payload = os.urandom(min(failure[3], 65536))
            try:
                write_checked_bytes(probe, payload, allowed_root=root, replace=False)
                if read_checked_bytes(probe, allowed_root=root) != payload:
                    raise OSError('storage probe readback differs')
            finally:
                if os.path.lexists(probe):
                    unlink_checked(probe, allowed_root=root)
            saved = json.loads(held['snapshot_json']) if held else snapshot
            if os.path.lexists(active):
                if (original['sha256'] != saved['current_file_sha256']
                        or describe_entry(active, allowed_root=root) != description):
                    raise OSError('checkpoint changed after isolation was prepared')
            elif description is not None:
                raise OSError('checkpoint disappeared before isolation')
            if not held:
                held = outbox.hold_workbench_set(set_id=set_id, source_phs2=source_phs2,
                    source_input_tag_id=source_input_tag_id, snapshot=snapshot,
                    reason='STORAGE_WRITE_FAILURE', held_by='SYSTEM_STORAGE_CHECK')
            snapshot = json.loads(held['snapshot_json'])
            self._force_exit_prepared_checkpoint = None
            self._checkpoint_generation = self.__dict__.get('_checkpoint_generation', 0) + 1
            archive = snapshot['archive_path']
            if os.path.lexists(active):
                quarantine_entry(active, archive, description=description,
                                 allowed_root=root)
            return held

    def resume_storage_checkpoint_hold(self, outbox, held):
        """Finish a recorded isolation before startup can restore its active file."""
        snapshot = json.loads(held['snapshot_json'])
        require_recovery_record('current', snapshot)
        root = Path(self.save_directory).absolute()
        active = root / self._current_state_filename()
        original = read_recovery_file(active, current_state=True)
        archive = str(active) + '.held-' + original['sha256']
        if (not original['verified'] or original['sha256'] != snapshot.get('current_file_sha256', original['sha256'])
                or str(original['value']['current_set_info'].get('id') or '') != str(held['set_id'])
                or str(snapshot['current_set_info'].get('id') or '') != str(held['set_id'])
                or snapshot.get('archive_path', archive) != archive):
            raise OSError('held checkpoint original cannot be verified for startup isolation')
        description = describe_entry(active, allowed_root=root)
        with self._state_lock:
            pending = list(self._audit_outbox)
        self._resume_storage_checkpoint_hold_rename(outbox, held, description, pending, archive)
        if hashlib.sha256(read_checked_bytes(archive, allowed_root=root)).hexdigest() != original['sha256']:
            raise OSError('held checkpoint startup readback differs')
        with self._state_lock:
            if self._audit_outbox != pending:
                raise OSError('checkpoint audit changed during startup isolation')
            self._audit_outbox = []
        return held

    @writer_sink("data_manager_storage_failure_resume")
    def _resume_storage_checkpoint_hold_rename(self, outbox, held, description, pending, archive):
        root = Path(self.save_directory).absolute()
        active = root / self._current_state_filename()
        with self._state_lock:
            if (self._audit_outbox != pending or self._unverified_audit_segments
                    or self.log_queue.unfinished_tasks
                    or outbox.get_workbench_hold(str(held['set_id'])) != held
                    or describe_entry(active, allowed_root=root) != description):
                raise OSError('held checkpoint changed before startup isolation')
            self._force_exit_prepared_checkpoint = None
            self._checkpoint_generation = self.__dict__.get('_checkpoint_generation', 0) + 1
            quarantine_entry(active, archive, description=description, allowed_root=root)

    def _save_completed_worker_identity(self, current):
        """Archive a durable CSV completion, never an editable checkpoint."""
        with self._state_lock:
            current = sanitize_persistent_value(dict(current or {}))
            identity = label_worker_roster.record_identity(current)
            if not identity or not current.get('id') or 'completion_worker_identity' not in current:
                return
            # The current checkpoint already froze the decision before CSV.
            # Only its committed event has immutable scan/exchange evidence.
            completed = {'current_set_info': {
                'id': str(current.get('completion_identity') or current['id']),
                'raw': list(current.get('raw') or []),
                'scanner_exchanges': list(current.get('scanner_exchanges') or []),
                'completion_worker_identity': current['completion_worker_identity'], **identity}, **identity}
            require_recovery_record('current', completed)
            payload = json.dumps(completed, ensure_ascii=False, sort_keys=True,
                                 cls=self._datetime_encoder()).encode('utf-8')
            record_path = Path(self.save_directory) / '_worker_record_identity' / (
                hashlib.sha256(completed['current_set_info']['id'].encode('utf-8')).hexdigest() + '.json')
            try:
                original = read_checked_bytes(record_path, allowed_root=self.save_directory)
            except FileNotFoundError:
                write_checked_bytes(record_path, payload, allowed_root=self.save_directory, replace=False)
            else:
                if original != payload:
                    raise ValueError('completed worker record identity differs')
    def load_current_state(self, *, verified_bytes=None):
        state_path = os.path.join(self.save_directory, self._current_state_filename())
        try:
            if verified_bytes is None:
                evidence = read_recovery_file(state_path, current_state=True)
                if evidence["reason"] == "missing":
                    return None
                if not evidence["verified"]:
                    raise ValueError("current state is unverified: " + evidence["reason"])
                state = evidence["value"]
            else:
                state = json.loads(verified_bytes.decode('utf-8'))
            if isinstance(state, dict):
                saved_worker = str(state.get('worker_name') or '').strip()
                safe_state = sanitize_persistent_value(state)
                safe_worker = persistent_operator_name(saved_worker)
                safe_state['worker_name'] = safe_worker
                if safe_state != state:
                    state = safe_state
                    # Sanitize the stored representation, preserving both its
                    # segment references and the original worker attribution.
                    persisted = sanitize_persistent_value(json.loads(
                        (verified_bytes if verified_bytes is not None else evidence['raw']).decode('utf-8')))
                    persisted['worker_name'] = safe_worker
                    encoded = json.dumps(persisted, ensure_ascii=False, indent=4,
                                         cls=self._datetime_encoder()).encode('utf-8')
                    write_checked_bytes(state_path, encoded, allowed_root=self.save_directory)
            return state
        except Exception as e:
            print(f"임시 상태 로드 실패: {e}"); return None
    def delete_current_state(self):
        with self._state_lock:
            if self._audit_outbox or self._unverified_audit_segments:
                if not self.save_current_state({'current_set_info': {'id': None, 'raw': []}}):
                    raise OSError('pending scanner audit checkpoint could not be preserved')
                return
            state_path = os.path.join(self.save_directory, self._current_state_filename())
            if os.path.exists(state_path):
                try:
                    unlink_checked(state_path, allowed_root=self.save_directory)
                    self._checkpoint_generation = self.__dict__.get('_checkpoint_generation', 0) + 1
                except Exception as e: print(f"임시 상태 파일 삭제 실패: {e}")
            else:
                self._checkpoint_generation = self.__dict__.get('_checkpoint_generation', 0) + 1
