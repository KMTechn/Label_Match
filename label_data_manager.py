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
        if len(raw) > 8 * 1024 * 1024:
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
            result["value"] = decoded
        else:
            result["value"] = decoded
        if expected_bytes is not None and raw != expected_bytes:
            raise ValueError("recovery bytes differ from durable record")
        result["verified"] = True
        result["reason"] = ""
    except Exception as exc:
        result["reason"] = type(exc).__name__
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
                with self._open_file(filepath, 'a', newline='', encoding='utf-8-sig') as f:
                    writer = csv.writer(f)
                    if not file_exists or os.stat(filepath).st_size == 0:
                        writer.writerow(["timestamp", "worker_name", "event", "details"])
                    writer.writerow(log_item)
                    if str(log_item[2] or "") in self._durable_event_types():
                        f.flush()
                        os.fsync(f.fileno())
            except queue.Empty:
                continue
            except Exception as e:
                self._writer_errors.append(e)
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
        log_item = [
            self._clock().now().isoformat(),
            self._csv_safe_cell(persistent_operator_name(self.worker_name)),
            redact_protected_admin_code(event_type),
            redact_protected_admin_code(detail_text),
        ]
        with self._close_lock:
            if self._close_requested:
                raise RuntimeError("DataManager is closing; new log events are not accepted")
            self.log_queue.put(log_item)
    def close(self, timeout=None):
        with self._close_lock:
            if not self._close_requested:
                self._close_requested = True
                if self.log_thread.is_alive():
                    self.log_queue.put(None)
        self.log_thread.join(timeout)
        if self.log_thread.is_alive():
            raise TimeoutError("Log writer did not stop before timeout")
        if self._writer_errors:
            raise RuntimeError(f"Log writer failed: {self._writer_errors[-1]}")
        return True
    def flush(self, timeout=None):
        deadline = None if timeout is None else time.monotonic() + timeout
        while self.log_queue.unfinished_tasks:
            if deadline is not None and time.monotonic() >= deadline:
                raise TimeoutError("Log writer did not flush before timeout")
            time.sleep(0.01)
        if self._writer_errors:
            raise RuntimeError(f"Log writer failed: {self._writer_errors[-1]}")
        return True
    def save_current_state(self, state_data):
        state_path = os.path.join(self.save_directory, self._current_state_filename())
        try:
            state_data_with_worker = sanitize_persistent_value(dict(state_data or {}))
            state_data_with_worker['worker_name'] = persistent_operator_name(
                self.worker_name
            )
            encoded = json.dumps(state_data_with_worker, ensure_ascii=False, indent=4,
                                 cls=self._datetime_encoder())
            require_recovery_record("current", json.loads(encoded))
            write_checked_bytes(state_path, encoded.encode('utf-8'),
                                allowed_root=self.save_directory)
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
                    encoded = json.dumps(state, ensure_ascii=False, indent=4,
                                         cls=self._datetime_encoder()).encode('utf-8')
                    write_checked_bytes(state_path, encoded,
                                        allowed_root=self.save_directory)
            return state
        except Exception as e:
            print(f"임시 상태 로드 실패: {e}"); return None
    def delete_current_state(self):
        state_path = os.path.join(self.save_directory, self._current_state_filename())
        if os.path.exists(state_path):
            try: unlink_checked(state_path, allowed_root=self.save_directory)
            except Exception as e: print(f"임시 상태 파일 삭제 실패: {e}")
