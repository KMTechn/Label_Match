"""Queued event persistence and atomic current-set storage.

The application facade supplies live callbacks for its clock, file operations,
event projection and file names; this module does not import the Tk application.
"""

import csv
import hashlib
import json
import os
import queue
import threading
import time
import uuid
from pathlib import Path

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
                             unlink_checked, write_checked_bytes)


RECOVERY_MAX_BYTES = 8 * 1024 * 1024
AUDIT_SEGMENT_BYTES = 1024 * 1024


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
        self.unique_id = redact_protected_admin_code(unique_id)
        self.log_queue = queue.Queue()
        self._close_lock = threading.Lock()
        self._close_requested = False
        self._writer_errors = []
        self._writer_error_keys = {}
        self._recovered_audit_errors = set()
        self._state_lock = threading.RLock()
        self._archived_audit_outbox = []
        evidence = read_recovery_file(
            os.path.join(save_dir, self._current_state_filename()), current_state=True)
        self._audit_outbox = list((evidence['value'] or {}).get('scanner_audit_outbox', [])) if evidence['verified'] else []
        self._unverified_audit_segments = evidence.get('unverified_audit_segments', [])
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
        while True:
            log_item = None
            got_item = False
            try:
                log_item = self.log_queue.get()
                got_item = True
                if log_item is None: break
                filepath = self._get_log_filepath_for_item(log_item)
                file_exists = os.path.exists(filepath)
                os.makedirs(os.path.dirname(filepath), exist_ok=True)
                audit_key = self._audit_key(log_item)
                with self._open_file(filepath, 'a+' if audit_key else 'a', newline='', encoding='utf-8-sig') as f:
                    if audit_key:
                        f.seek(0)
                        duplicate = False
                        for row in csv.reader(f):
                            if self._audit_key(row) == audit_key:
                                if row != log_item:
                                    raise ValueError('audit key has different CSV content')
                                duplicate = True
                        f.seek(0, os.SEEK_END)
                        if duplicate:
                            f.flush()
                            os.fsync(f.fileno())
                            continue
                    writer = csv.writer(f)
                    if not file_exists or os.stat(filepath).st_size == 0:
                        writer.writerow(["timestamp", "worker_name", "event", "details"])
                    writer.writerow(log_item)
                    if audit_key or str(log_item[2] or "") in self._durable_event_types():
                        f.flush()
                        os.fsync(f.fileno())
            except queue.Empty:
                continue
            except Exception as e:
                self._writer_errors.append(e)
                self._writer_error_keys[id(e)] = self._audit_key(log_item)
                print(f"로그 쓰기 스레드 오류: {e}")
            finally:
                if got_item:
                    self.log_queue.task_done()
    def log_event(self, event_type, details):
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
            self._csv_safe_cell(persistent_operator_name(audit.get('operator', self.worker_name))),
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
    def prepare_audit_event(self, event_type, details):
        details = dict(details)
        details['scanner_audit'] = {
            'key': uuid.uuid4().hex,
            'occurred_at': self._clock().now().isoformat(),
            'operator': persistent_operator_name(self.worker_name),
        }
        return {'event_type': event_type, 'details': sanitize_persistent_value(
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
                    self.log_queue.put(None)
        self.log_thread.join(timeout)
        if self.log_thread.is_alive():
            raise TimeoutError("Log writer did not stop before timeout")
        if any(id(error) not in self._recovered_audit_errors for error in self._writer_errors):
            raise RuntimeError(f"Log writer failed: {self._writer_errors[-1]}")
        return True
    def flush(self, timeout=None):
        deadline = None if timeout is None else time.monotonic() + timeout
        while self.log_queue.unfinished_tasks:
            if deadline is not None and time.monotonic() >= deadline:
                raise TimeoutError("Log writer did not flush before timeout")
            time.sleep(0.01)
        if any(id(error) not in self._recovered_audit_errors for error in self._writer_errors):
            raise RuntimeError(f"Log writer failed: {self._writer_errors[-1]}")
        return True
    def save_current_state(self, state_data, *, _completed_audit_keys=()):
        with self._state_lock:
            state_path = os.path.join(self.save_directory, self._current_state_filename())
            try:
                state_data_with_worker = sanitize_persistent_value(dict(state_data or {}))
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
                state_data_with_worker['worker_name'] = persistent_operator_name(
                    state_data_with_worker.get('worker_name', self.worker_name)
                    if _completed_audit_keys else self.worker_name
                )
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
                write_checked_bytes(state_path, encoded.encode('utf-8'),
                                    allowed_root=self.save_directory)
                self._audit_outbox = list(pending.values())
                return True
            except Exception as e:
                print(f"임시 상태 저장 실패: {e}")
                return False
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
                try: unlink_checked(state_path, allowed_root=self.save_directory)
                except Exception as e: print(f"임시 상태 파일 삭제 실패: {e}")
