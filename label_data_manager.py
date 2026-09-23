"""Queued event persistence and atomic current-set storage.

The application facade supplies live callbacks for its clock, file operations,
event projection and file names; this module does not import the Tk application.
"""

import csv
import hashlib
import json
import os
import queue
import stat
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


def read_recovery_file(path, *, journal_schema=None, current_state=False, expected_bytes=None):
    """Return raw bytes, digest, and a validated value without modifying evidence."""
    result = {"verified": False, "raw": None, "sha256": "", "value": None,
              "reason": "missing"}
    try:
        path = Path(path)
        try:
            metadata = path.stat()
        except FileNotFoundError:
            return result
        if not stat.S_ISREG(metadata.st_mode):
            raise ValueError("recovery path is not a regular file")
        raw = path.read_bytes()
        result["raw"] = raw
        result["sha256"] = hashlib.sha256(raw).hexdigest()
        if len(raw) > 8 * 1024 * 1024:
            result["reason"] = "oversized"
            return result
        decoded = json.loads(raw.decode("utf-8"))
        if not isinstance(decoded, dict):
            raise ValueError("recovery root is not an object")
        if journal_schema is not None:
            state = decoded.get("state")
            if (decoded.get("schema_version") != journal_schema
                    or not isinstance(state, dict)
                    or any(not isinstance(state[key], str)
                           for key in ("status", "set_id", "scan_payload",
                                       "canonical_input_tag_qr", "source_label_id",
                                       "active_scan_label_id", "exchange_id",
                                       "input_tag_id", "authority_scope_id",
                                       "workflow_mode", "prepare_idempotency_key")
                           if key in state)):
                raise ValueError("label journal structure is invalid")
            result["value"] = state
        elif current_state:
            current = decoded.get("current_set_info")
            if (not isinstance(current, dict)
                    or not isinstance(current.get("id"), (str, int))
                    or not isinstance(current.get("raw", []), list)
                    or any(not isinstance(value, str)
                           for value in current.get("raw", []))
                    or not isinstance(current.get("parsed", []), list)
                    or any(not isinstance(value, str)
                           for value in current.get("parsed", []))
                    or ("start_time" in current
                        and current["start_time"] is not None
                        and not isinstance(current["start_time"], str))
                    or ("timestamp" in decoded
                        and not isinstance(decoded["timestamp"], str))):
                raise ValueError("current set structure is invalid")
            result["value"] = decoded
        else:
            result["value"] = decoded
        if expected_bytes is not None and raw != expected_bytes:
            raise ValueError("recovery bytes differ from durable record")
        result["verified"] = True
        result["reason"] = ""
    except Exception as exc:
        result["reason"] = type(exc).__name__
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
        temp_path = f"{state_path}.tmp-{os.getpid()}-{threading.get_ident()}"
        try:
            os.makedirs(os.path.dirname(state_path), exist_ok=True)
            state_data_with_worker = sanitize_persistent_value(dict(state_data or {}))
            state_data_with_worker['worker_name'] = persistent_operator_name(
                self.worker_name
            )
            with self._open_file(temp_path, 'w', encoding='utf-8') as f:
                json.dump(state_data_with_worker, f, ensure_ascii=False, indent=4, cls=self._datetime_encoder())
                f.flush()
                os.fsync(f.fileno())
            os.replace(temp_path, state_path)
            return True
        except Exception as e:
            try:
                if os.path.exists(temp_path):
                    os.remove(temp_path)
            except Exception:
                pass
            print(f"임시 상태 저장 실패: {e}")
            return False
    def load_current_state(self, *, verified_bytes=None):
        state_path = os.path.join(self.save_directory, self._current_state_filename())
        try:
            if verified_bytes is None:
                evidence = read_recovery_file(state_path)
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
                    temp_path = f"{state_path}.migrate-{os.getpid()}-{threading.get_ident()}"
                    try:
                        with self._open_file(temp_path, 'w', encoding='utf-8') as handle:
                            json.dump(state, handle, ensure_ascii=False, indent=4, cls=self._datetime_encoder())
                            handle.flush()
                            os.fsync(handle.fileno())
                        os.replace(temp_path, state_path)
                    finally:
                        try:
                            if os.path.exists(temp_path):
                                os.remove(temp_path)
                        except OSError:
                            pass
            return state
        except Exception as e:
            print(f"임시 상태 로드 실패: {e}"); return None
    def delete_current_state(self):
        state_path = os.path.join(self.save_directory, self._current_state_filename())
        if os.path.exists(state_path):
            try: os.remove(state_path)
            except Exception as e: print(f"임시 상태 파일 삭제 실패: {e}")
