"""Worker choices and optional, frozen record identity metadata."""
from __future__ import annotations

import hashlib
import copy
import datetime as dt
import json
import logging
import os
from pathlib import Path
from typing import Any
import unicodedata
import uuid
from datetime import datetime, timezone
from writer_session_fence import writer_sink, WriterFenceError
from protected_admin import (PROTECTED_ADMIN_OPERATOR_ID, PROTECTED_ADMIN_DISPLAY_NAME,
                             is_protected_admin_candidate)

CONTRACT_VERSION = "worker-roster.v1"
CONTRACT_VERSION_V2 = "worker-roster.v2"
STAGE_A = False  # Stage B registers names; it never calculates inactivity or hides workers.
MAX_CACHE_BYTES = 2 * 1024 * 1024
MAX_LOCAL_BYTES = 4 * 1024 * 1024


def normalize_registration_name(value):
    if not isinstance(value, str):
        raise ValueError("이름을 적어 주세요.")
    if any(unicodedata.category(char) in {"Cc", "Cf", "Cs", "Zl", "Zp"} for char in value):
        raise ValueError("이름에 보이지 않는 문자가 있습니다. 다시 적어 주세요.")
    name = " ".join(unicodedata.normalize("NFC", value).split())
    if (not valid_name(name, remote=True) or is_protected_admin_candidate(name)
            or name in {PROTECTED_ADMIN_OPERATOR_ID, PROTECTED_ADMIN_DISPLAY_NAME}):
        raise ValueError("이름은 1~64자로 적어 주세요. 관리자 코드나 파일에 쓸 수 없는 문자는 이름으로 쓸 수 없습니다.")
    return name


def registration_enabled(answer):
    if not isinstance(answer, dict):
        return False
    data = answer.get("data", answer)
    if not isinstance(data, dict):
        return False
    ids = data.get("capability_ids")
    capabilities = data.get("capabilities")
    feature = capabilities.get("worker_roster_v2") if isinstance(capabilities, dict) else None
    return (isinstance(ids, list) and "worker_roster_v2_stage_b" in ids) or (
        isinstance(feature, dict) and feature.get("stage_b") is True
        and feature.get("temporary_identity_ingest") is True)


def worker_choice_label(row):
    number = row.get("worker_number")
    if number is None and row.get("local_sequence"):
        number = "임시" + str(row["local_sequence"])
    return row["display_name"] + (" #" + str(number) if number is not None else "")


def record_identity(value):
    """Read a captured identity; legacy records deliberately return no fields."""
    if not isinstance(value, dict) or "worker_name_snapshot" not in value:
        return {}
    identity = value.get("worker_id")
    if identity is not None and (type(identity) is not str or str(uuid.UUID(identity)) != identity):
        raise ValueError("invalid recorded worker_id")
    name = value["worker_name_snapshot"]
    if not valid_name(name):
        raise ValueError("invalid recorded worker_name_snapshot")
    return {"worker_id": identity, "worker_name_snapshot": name}


def identity_capability_enabled(answer):
    if not isinstance(answer, dict):
        return False
    data = answer.get("data", answer)
    capabilities = data.get("capabilities") if isinstance(data, dict) else None
    feature = capabilities.get("worker_record_identity_v1") if isinstance(capabilities, dict) else None
    return (isinstance(feature, dict) and feature.get("enabled") is True) or registration_enabled(answer)


def query_identity_capability(client, identity=None):
    """A failed or unavailable advertisement keeps the old wire format."""
    try:
        answer = client.get_capabilities()
        store = getattr(client, "_label_roster_b", None)
        if isinstance(store, WorkerRoster) and isinstance(identity, dict):
            worker_id = identity.get("worker_id")
            if any(row["worker_id"] == worker_id for row in store.local["workers"]):
                return registration_enabled(answer)
            if store.local_blocked and not any(row["worker_id"] == worker_id
                                              for row in (store.snapshot or {}).get("workers", [])):
                return False
        return identity_capability_enabled(answer)
    except Exception:
        return False


def valid_name(value, *, remote=False):
    if not (type(value) is str and bool(value.strip())
            and not any(unicodedata.category(char) == "Cc" for char in value)):
        return False
    if not remote:
        return True
    name = value.strip()
    return (len(name) <= 64 and name[0] not in "=+-@"
            and not any(ch in r'\/:*?"<>|' or unicodedata.category(ch) in ("Cc", "Cf", "Cs", "Zl", "Zp") for ch in value))


def _unique_object(pairs):
    result = {}
    for name, value in pairs:
        if name in result:
            raise ValueError("duplicate field")
        result[name] = value
    return result


def _digest(payload):
    raw = json.dumps(payload, ensure_ascii=False, sort_keys=True,
                     separators=(",", ":"), allow_nan=False).encode("utf-8", errors="backslashreplace")
    return hashlib.sha256(raw).hexdigest()


def validate_roster(payload, scope):
    if (not isinstance(payload, dict) or not scope
            or payload.get("contract_version") not in {CONTRACT_VERSION, CONTRACT_VERSION_V2}
            or payload.get("authority_scope_id") != scope
            or type(payload.get("version")) is not int or payload["version"] < 0
            or type(payload.get("workers")) is not list):
        raise ValueError("invalid roster")
    if payload["contract_version"] == CONTRACT_VERSION_V2:
        count = payload.get("source_row_count", len(payload["workers"]))
        rejected = payload.get("unselectable_worker_ids", [])
        if (type(count) is not int or count < len(payload["workers"]) or not isinstance(rejected, list)
                or any(type(identity) is not str or str(uuid.UUID(identity)) != identity for identity in rejected)):
            raise ValueError("invalid roster source rows")
    seen = set()
    for worker in payload["workers"]:
        if not isinstance(worker, dict):
            raise ValueError("invalid worker")
        identity = worker.get("worker_id")
        if (type(identity) is not str or str(uuid.UUID(identity)) != identity
                or identity in seen or type(worker.get("display_name")) is not str
                or worker.get("status") not in {"ACTIVE", "DISABLED"}):
            raise ValueError("invalid worker")
        seen.add(identity)
        if payload["contract_version"] == CONTRACT_VERSION_V2 and (
                type(worker.get("worker_number")) is not int or worker["worker_number"] < 1):
            raise ValueError("invalid worker number")
    return payload


def _usable_roster(payload):
    received = payload["workers"]
    workers = [{**worker, "display_name": worker["display_name"].strip()}
               for worker in received if valid_name(worker["display_name"], remote=True)]
    if received == workers:
        return payload
    discarded = len(received) - len(workers)
    if discarded:
        logging.getLogger(__name__).warning("Worker roster discarded invalid-name rows: %d", discarded)
    # Keep the received rows for cache bytes, same-version comparison and the
    # distinction between a truly empty roster and an unusable nonempty one.
    return {**payload, "workers": workers, "_received_workers": received}


def uses_legacy_names(roster):
    return roster is None or not roster.get("source_row_count", len(roster.get("_received_workers", roster["workers"])))


def _read_cache(path, scope):
    with path.open("rb") as handle:
        raw = handle.read(MAX_CACHE_BYTES + 1)
    if len(raw) > MAX_CACHE_BYTES:
        raise ValueError("cache too large")
    payload = json.loads(raw.decode("utf-8"), object_pairs_hook=_unique_object)
    validate_roster(payload, scope)
    fields = {"contract_version", "authority_scope_id", "version", "workers", "fetched_at", "sha256"}
    if set(payload) != fields and not (payload.get("contract_version") == CONTRACT_VERSION_V2
            and fields <= set(payload) <= fields | {"source_row_count", "unselectable_worker_ids"}):
        raise ValueError("invalid cache fields")
    stamp = payload["fetched_at"]
    if type(stamp) is not str or datetime.fromisoformat(stamp).tzinfo is None:
        raise ValueError("invalid cache timestamp")
    unsigned = {name: value for name, value in payload.items() if name != "sha256"}
    if payload["sha256"] != _digest(unsigned):
        raise ValueError("cache digest mismatch")
    return _usable_roster(payload)


def load_cache(path, scope):
    path = Path(path)
    for candidate in (path, path.with_name("roster.last-good.json")):
        try:
            return _read_cache(candidate, scope)
        except FileNotFoundError:
            continue
        except (OSError, ValueError, TypeError, OverflowError):
            # Never overwrite the evidence with a fetched response or an empty list.
            try:
                candidate.rename(candidate.with_name(candidate.name + ".invalid-" + uuid.uuid4().hex))
            except OSError:
                pass
    return None


def _atomic_write(path, payload):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(path.name + ".tmp-" + uuid.uuid4().hex)
    try:
        with temporary.open("w", encoding="utf-8", errors="backslashreplace", newline="\n") as handle:
            json.dump(payload, handle, ensure_ascii=False, sort_keys=True,
                      separators=(",", ":"), allow_nan=False)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
    finally:
        if temporary.exists():
            temporary.unlink()


def save_cache(path, payload, scope):
    validate_roster(payload, scope)
    cached = {name: payload[name] for name in (
        "contract_version", "authority_scope_id", "version", "workers")}
    cached["workers"] = payload.get("_received_workers", payload["workers"])
    validate_roster(cached, scope)
    cached["fetched_at"] = datetime.now(timezone.utc).isoformat()
    cached["sha256"] = _digest(cached)
    # Two atomic copies give the next offline launch a verified fallback.
    _atomic_write(Path(path).with_name("roster.last-good.json"), cached)
    _atomic_write(path, cached)
    return _usable_roster(cached)


def apply_response(answer, previous, scope):
    if not isinstance(answer, dict) or answer.get("ok") is not True:
        raise ValueError("query failed")
    if (answer.get("contract_version") != CONTRACT_VERSION
            or answer.get("authority_scope_id") != scope
            or type(answer.get("version")) is not int
            or type(answer.get("unchanged")) is not bool):
        raise ValueError("invalid response")
    if answer["unchanged"]:
        if previous is None or answer["version"] != previous["version"]:
            raise ValueError("unchanged without matching cache")
        return previous
    validate_roster(answer, scope)
    if previous is not None and answer["version"] < previous["version"]:
        raise ValueError("older response")
    if previous is not None and answer["version"] == previous["version"]:
        if answer["workers"] != previous.get("_received_workers", previous["workers"]):
            raise ValueError("changed workers without a new version")
        return previous
    # Private cache-view metadata never comes from the remote response.
    return _usable_roster({name: value for name, value in answer.items()
                           if name != "_received_workers"})


def choices(roster, recent_ids, legacy_names):
    if uses_legacy_names(roster):
        return [{"worker_id": None, "display_name": name, "legacy_local": True}
                for name in dict.fromkeys(legacy_names) if valid_name(name)]
    roster = _usable_roster(roster)
    recent_ids = list(dict.fromkeys(identity for identity in recent_ids if type(identity) is str))
    order = {identity: index for index, identity in enumerate(recent_ids)}
    active = [dict(worker) for worker in roster["workers"] if worker["status"] == "ACTIVE"]
    return sorted(active, key=lambda worker: (
        order.get(worker["worker_id"], len(order)), worker["display_name"]))


class WorkerRoster:
    def __init__(self, path, authority_scope_id):
        self.path = Path(path)
        self.authority_scope_id = authority_scope_id
        self.snapshot = load_cache(self.path, authority_scope_id)
        self.local_path = self.path.with_name(self.path.stem + ".registrations.json")
        self.local = {"schema": "worker-roster-b.local.v1", "authority_scope_id": authority_scope_id,
                      "sequence": 0, "workers": [], "requests": [], "temp_links": {}, "snapshot": None}
        self.local_blocked = False
        self._load_local()

    def _validate(self, value: Any) -> dict[str, Any]:
        if isinstance(value, dict) and value.get("contract_version") == CONTRACT_VERSION:
            validate_roster(value, self.authority_scope_id)
            return _usable_roster(value)
        if (not isinstance(value, dict) or value.get("contract_version") not in {CONTRACT_VERSION, CONTRACT_VERSION_V2}
                or not self.authority_scope_id
                or value.get("authority_scope_id") != self.authority_scope_id
                or type(value.get("version")) is not int or value["version"] < 0):
            raise ValueError("invalid roster identity or version")
        rows = value.get("workers")
        if not isinstance(rows, list):
            raise ValueError("invalid roster workers")
        source_row_count = value.get("source_row_count", len(rows))
        if type(source_row_count) is not int or source_row_count < len(rows):
            raise ValueError("invalid roster source row count")
        workers, seen, unselectable = [], set(), set(value.get("unselectable_worker_ids", []))
        if any(str(uuid.UUID(identity)) != identity for identity in unselectable):
            raise ValueError("invalid unselectable worker identity")
        for row in rows:
            if (not isinstance(row, dict) or type(row.get("worker_id")) is not str
                    or str(uuid.UUID(row["worker_id"])) != row["worker_id"]
                    or row["worker_id"] in seen or row.get("status") not in {"ACTIVE", "DISABLED"}
                    or type(row.get("display_name")) is not str):
                raise ValueError("invalid or duplicate roster worker")
            seen.add(row["worker_id"])
            name = row["display_name"].strip()
            if (name and len(name) <= 64 and not name.startswith(("=", "+", "-", "@"))
                    and not any(character in '\\/:*?"<>|' for character in name)
                    and not any(unicodedata.category(character) in {"Cc", "Cf", "Cs", "Zl", "Zp"}
                                for character in row["display_name"])):
                normalized = {"worker_id": row["worker_id"], "display_name": name, "status": row["status"]}
                if value["contract_version"] == CONTRACT_VERSION_V2:
                    if type(row.get("worker_number")) is not int or row["worker_number"] < 1:
                        raise ValueError("invalid worker number")
                    normalized["worker_number"] = row["worker_number"]
                workers.append(normalized)
            else:
                unselectable.add(row["worker_id"])
        result = {"contract_version": value["contract_version"], "authority_scope_id": self.authority_scope_id,
                  "version": value["version"], "workers": workers, "source_row_count": source_row_count}
        if unselectable:
            result["unselectable_worker_ids"] = sorted(unselectable)
        return result

    @writer_sink("label_roster_registration_quarantine")
    def _preserve_bad(self, path: Path) -> None:
        # A rename preserves the exact bytes, including malformed UTF-8/JSON.
        path.replace(path.with_name(path.name + ".bad-" + uuid.uuid4().hex))

    @property
    def known_version(self) -> int | None:
        return self.snapshot["version"] if self.snapshot is not None else None

    def _validate_local(self, value):
        if (not isinstance(value, dict) or value.get("schema") != self.local["schema"]
                or value.get("authority_scope_id") != self.authority_scope_id
                or type(value.get("sequence")) is not int or value["sequence"] < 0
                or not isinstance(value.get("workers"), list)
                or not isinstance(value.get("requests"), list)
                or not isinstance(value.get("temp_links"), dict)):
            raise ValueError("invalid local registration envelope")
        unsigned = {key: item for key, item in value.items() if key != "sha256"}
        if value.get("sha256") != _digest(unsigned):
            raise ValueError("registration envelope digest mismatch")
        seen, sequences, creates = set(), set(), {}
        for row in value["workers"]:
            identity = row["worker_id"]
            if (str(uuid.UUID(identity)) != identity or identity in seen
                    or type(row["local_sequence"]) is not int or row["local_sequence"] < 1
                    or row["local_sequence"] > value["sequence"] or row["local_sequence"] in sequences
                    or normalize_registration_name(row["display_name"]) != row["display_name"]):
                raise ValueError("invalid local worker")
            seen.add(identity)
            sequences.add(row["local_sequence"])
        request_ids = set()
        for item in value["requests"]:
            request_id = item["request_id"]
            if (str(uuid.UUID(request_id)) != request_id or request_id in request_ids
                    or item["state"] not in {"PENDING", "ACKED", "REVIEW", "TERMINAL"}
                    or type(item["error"]) is not str):
                raise ValueError("invalid local request")
            request_ids.add(request_id)
            if item["body"] is not None:
                body = item["body"]
                self._validate_request(body)
                if body["request_id"] != request_id:
                    raise ValueError("request identity changed")
                if body["mode"] == "create":
                    if body["temp_uuid"] not in seen or body["temp_uuid"] in creates:
                        raise ValueError("missing or reused temporary worker")
                    creates[body["temp_uuid"]] = request_id
            elif not item.get("depends_on"):
                raise ValueError("request has no body or dependency")
        if seen != set(creates):
            raise ValueError("local worker lacks durable create request")
        for item in value["requests"]:
            if item.get("depends_on"):
                intent = item["intent"]
                if creates.get(intent["temp_uuid"]) != item["depends_on"]:
                    raise ValueError("invalid return dependency")
                self._action_time(intent["action_at"])
                normalize_registration_name(intent["display_name"])
        for temp_uuid, link in value["temp_links"].items():
            self._validate_link({"temp_uuid": temp_uuid, **link})
        if value.get("snapshot") is not None:
            self._validate(value["snapshot"])
        return unsigned

    @staticmethod
    def _action_time(value):
        parsed = dt.datetime.fromisoformat(value.replace("Z", "+00:00"))
        if parsed.tzinfo is None or parsed.utcoffset() != dt.timedelta(0):
            raise ValueError("invalid registration action time")
        return parsed

    def _validate_request(self, body):
        mode = body.get("mode")
        fields = {"mode", "authority_scope_id", "request_id", "display_name", "action_at"}
        fields.add("temp_uuid" if mode == "create" else "existing_worker_id")
        if (mode not in {"create", "return"} or set(body) != fields
                or body["authority_scope_id"] != self.authority_scope_id
                or str(uuid.UUID(body["request_id"])) != body["request_id"]
                or normalize_registration_name(body["display_name"]) != body["display_name"]):
            raise ValueError("invalid registration request")
        identity = body["temp_uuid" if mode == "create" else "existing_worker_id"]
        if str(uuid.UUID(identity)) != identity:
            raise ValueError("invalid registration worker identity")
        self._action_time(body["action_at"])

    @staticmethod
    def _validate_link(link):
        if (not isinstance(link, dict) or type(link.get("worker_number")) is not int
                or link["worker_number"] < 1):
            raise ValueError("invalid temporary identity mapping")
        for key in ("temp_uuid", "worker_id"):
            if str(uuid.UUID(link[key])) != link[key]:
                raise ValueError("invalid mapping UUID")
        return {key: link[key] for key in ("worker_id", "worker_number")}

    def _load_local(self):
        try:
            with self.local_path.open("rb") as stream:
                raw = stream.read(MAX_LOCAL_BYTES + 1)
            if len(raw) > MAX_LOCAL_BYTES:
                raise ValueError("oversized registration envelope")
            value = json.loads(raw.decode("utf-8"), object_pairs_hook=_unique_object)
            self.local = self._validate_local(value)
            self.local_blocked = False
            snapshot = self.local["snapshot"]
            if snapshot is not None and (self.known_version is None or snapshot["version"] >= self.known_version):
                self.snapshot = snapshot
        except FileNotFoundError:
            # Preserved damage must never be mistaken for a fresh empty queue.
            self.local_blocked = any(self.local_path.parent.glob(self.local_path.name + ".bad-*"))
        except (ValueError, TypeError, KeyError, UnicodeError, AttributeError, OverflowError, RecursionError):
            self.local_blocked = True
            try:
                self._preserve_bad(self.local_path)
            except (OSError, WriterFenceError):
                pass  # Preserve in place when quarantine is unavailable; web names still work.
        except OSError:
            self.local_blocked = True

    @writer_sink("label_roster_registration_save")
    def _save_local(self, value):
        if self.local_blocked:
            raise ValueError("이 PC의 이름 등록 기록을 확인해야 합니다. 기존 이름으로 작업을 계속하세요.")
        encoded = {**value, "sha256": _digest(value)}
        if len(json.dumps(encoded, ensure_ascii=False).encode("utf-8")) > MAX_LOCAL_BYTES // 2:
            raise ValueError("회사 서버로 보낼 이름이 많습니다. 기존 이름으로 작업을 계속하세요.")
        self._validate_local(encoded)
        _atomic_write(self.local_path, encoded)
        self.local = value

    def duplicate_workers(self, name):
        name = normalize_registration_name(name)
        candidates = list((self.snapshot or {}).get("workers", [])) + self.local["workers"]
        result, seen = [], set()
        for row in candidates:
            if row.get("status", "ACTIVE") != "ACTIVE":
                continue
            try:
                matches = normalize_registration_name(row["display_name"]) == name
            except ValueError:
                matches = False
            identity = self.resolved_id(row["worker_id"])
            active = self.active_worker(worker_id=row["worker_id"])
            if matches and active is not None and identity not in seen:
                result.append(active)
                seen.add(identity)
        return result

    @writer_sink("label_roster_registration_create")
    def register_local(self, name, *, existing_worker_id=""):
        name = normalize_registration_name(name)
        # Admission serializes processes; refresh the durable envelope while
        # holding it so another GUI's rows, requests and sequence are retained.
        self._load_local()
        value = copy.deepcopy(self.local)
        request_id = str(uuid.uuid4())
        action_at = dt.datetime.now(dt.timezone.utc).isoformat()
        body = {"mode": "create", "authority_scope_id": self.authority_scope_id,
                "request_id": request_id, "display_name": name, "action_at": action_at}
        item = {"request_id": request_id, "body": body, "state": "PENDING", "error": ""}
        if existing_worker_id:
            row = self.active_worker(worker_id=existing_worker_id)
            if row is None:
                raise ValueError("이 이름은 지금 사용할 수 없습니다. 다른 이름을 골라 주세요.")
            source = next((entry for entry in value["requests"]
                           if entry["body"] and entry["body"].get("temp_uuid") == existing_worker_id), None)
            if source is not None and existing_worker_id not in value["temp_links"]:
                item.update(body=None, depends_on=source["request_id"],
                            intent={"temp_uuid": existing_worker_id, "display_name": name, "action_at": action_at})
            else:
                body.update(mode="return", existing_worker_id=self.resolved_id(existing_worker_id))
            selected = existing_worker_id
        else:
            if self.duplicate_workers(name):
                raise ValueError("같은 이름이 있습니다. 같은 분인지 확인하거나 구분할 이름을 적어 주세요.")
            selected = str(uuid.uuid4())
            value["sequence"] += 1
            body["temp_uuid"] = selected
            value["workers"].append({"worker_id": selected, "display_name": name,
                                     "local_sequence": value["sequence"]})
        value["requests"].append(item)
        self._save_local(value)
        return self.active_worker(worker_id=selected)

    def pending_requests(self):
        return [copy.deepcopy(item["body"]) for item in self.local["requests"]
                if item["state"] == "PENDING" and item["body"] is not None]

    def resolved_id(self, worker_id):
        return self.local["temp_links"].get(worker_id, {}).get("worker_id", worker_id)

    def same_person(self, first, second):
        return bool(first and second and self.resolved_id(first) == self.resolved_id(second))

    def _merge_links(self, value, links):
        if not isinstance(links, list):
            raise ValueError("invalid roster mappings")
        for link in links:
            normalized = self._validate_link(link)
            worker = next((row for row in (value["snapshot"] or {}).get("workers", [])
                           if row["worker_id"] == link["worker_id"]), None)
            if worker is not None and worker.get("worker_number", link["worker_number"]) != link["worker_number"]:
                raise ValueError("mapping worker number does not match roster")
            previous = value["temp_links"].get(link["temp_uuid"])
            if previous is not None and previous != normalized:
                raise ValueError("temporary mapping changed")
            value["temp_links"][link["temp_uuid"]] = normalized

    def _resolve_requests(self, value):
        workers = {row["worker_id"]: row for row in (value["snapshot"] or {}).get("workers", [])}
        for item in value["requests"]:
            body = item["body"]
            if body and body["mode"] == "create":
                link = value["temp_links"].get(body["temp_uuid"])
                if link:
                    row = workers.get(link["worker_id"])
                    if item["state"] == "PENDING" or (item["state"] == "REVIEW" and row
                                                          and row["display_name"] == body["display_name"]):
                        item.update(state="ACKED", error=item["error"] if item["state"] == "REVIEW" else "")
            if body is None:
                intent = item["intent"]
                link = value["temp_links"].get(intent["temp_uuid"])
                if link:
                    item["body"] = {"mode": "return", "authority_scope_id": self.authority_scope_id,
                                    "request_id": item["request_id"], "display_name": intent["display_name"],
                                    "action_at": intent["action_at"], "existing_worker_id": link["worker_id"]}
                    body = item["body"]
            if body and body["mode"] == "return" and item["state"] == "PENDING":
                row = workers.get(body["existing_worker_id"])
                if row and row["status"] == "DISABLED":
                    item.update(state="TERMINAL", error="WORKER_DISABLED", error_version=value["snapshot"]["version"])

    @writer_sink("label_roster_registration_ack")
    def apply_registration(self, request, answer=None, error=None):
        self._load_local()
        value = copy.deepcopy(self.local)
        item = next((row for row in value["requests"] if row["request_id"] == request["request_id"]), None)
        if item is None or item["body"] != request or item["state"] != "PENDING":
            return
        if error is not None:
            code, status = getattr(error, "code", "VERIFY_UNAVAILABLE"), getattr(error, "status", None)
            item["error"] = code
            if status == 409 or code in {"REQUEST_CONFLICT", "TEMP_ID_CONFLICT"}:
                item["state"] = "REVIEW"
            elif request["mode"] == "return" and code in {"WORKER_DISABLED", "WORKER_NOT_FOUND"}:
                item.update(state="TERMINAL", error_version=self.known_version)
            elif status == 400:
                future = self._action_time(request["action_at"]) > (
                    dt.datetime.now(dt.timezone.utc) + dt.timedelta(minutes=5))
                if code != "INVALID_REQUEST" or not future:
                    item["state"] = "REVIEW"
        else:
            if (not isinstance(answer, dict) or answer.get("ok") is not True
                    or answer.get("contract_version") != CONTRACT_VERSION_V2
                    or answer.get("authority_scope_id") != self.authority_scope_id
                    or answer.get("request_id") != request["request_id"]):
                raise ValueError("invalid registration acknowledgement")
            snapshot = self._validate({**answer, "workers": [answer.get("worker")]})
            row = snapshot["workers"][0] if snapshot["workers"] else None
            if row is None or row["status"] != "ACTIVE":
                raise ValueError("invalid registration worker")
            if request["mode"] == "create":
                link = answer.get("temp_link")
                if (not isinstance(link, dict) or link.get("temp_uuid") != request["temp_uuid"]
                        or link.get("worker_id") != row["worker_id"]
                        or link.get("worker_number") != row["worker_number"]):
                    raise ValueError("acknowledgement does not match temporary worker")
                self._merge_links(value, [link])
            elif row["worker_id"] != request["existing_worker_id"] or answer.get("temp_link") is not None:
                raise ValueError("return acknowledgement changed worker identity")
            # A replay can contain an older name/status. It never wins over a newer query.
            current = value["snapshot"]
            if self.snapshot is not None and (current is None or self.snapshot["version"] > current["version"]):
                current = self.snapshot
            if current is None or snapshot["version"] >= current["version"]:
                others = [entry for entry in (current or {}).get("workers", []) if entry["worker_id"] != row["worker_id"]]
                snapshot.update(workers=others + [row], source_row_count=len(others) + 1,
                                fetched_at=dt.datetime.now(dt.timezone.utc).isoformat())
                value["snapshot"] = snapshot
            item.update(state="ACKED", error="")
        self._resolve_requests(value)
        self._save_local(value)
        if value["snapshot"] is not None:
            self.snapshot = value["snapshot"]

    @writer_sink("label_roster_query_apply")
    def apply(self, answer: Any, *, requested_version: int | None) -> None:
        if isinstance(answer, dict) and answer.get("contract_version") == CONTRACT_VERSION:
            self._load_local()
            accepted = apply_response(answer, self.snapshot, self.authority_scope_id)
            if accepted is not self.snapshot:
                if not self.local_blocked and (self.local["workers"] or self.local["requests"]):
                    local = copy.deepcopy(self.local)
                    local["snapshot"] = accepted
                    self._resolve_requests(local)
                    self._save_local(local)
                self.snapshot = save_cache(self.path, accepted, self.authority_scope_id)
            return
        self._load_local()
        if (not isinstance(answer, dict) or answer.get("ok") is not True
                or answer.get("unchanged") is not False):
            raise ValueError("unsuccessful roster response")
        value = self._validate(answer)
        if self.known_version is not None and value["version"] < self.known_version:
            if not self.local_blocked:
                # Mapping remains useful after ACK loss despite an obsolete roster.
                local = copy.deepcopy(self.local)
                self._merge_links(local, answer.get("temp_links"))
                self._resolve_requests(local)
                self._save_local(local)
                return
            raise ValueError("obsolete roster version")
        if value["contract_version"] == CONTRACT_VERSION_V2 and not self.local_blocked:
            local = copy.deepcopy(self.local)
            value["fetched_at"] = dt.datetime.now(dt.timezone.utc).isoformat()
            local["snapshot"] = value
            self._merge_links(local, answer.get("temp_links"))
            self._resolve_requests(local)
            self._save_local(local)
            self.snapshot = value
            return
        # Damaged local requests stay preserved, while web names remain available.
        cached = {**value, "fetched_at": dt.datetime.now(dt.timezone.utc).isoformat()}
        cached["sha256"] = _digest(cached)
        _atomic_write(self.path.with_name("roster.last-good.json"), cached)
        _atomic_write(self.path, cached)
        self.snapshot = value

    def active_workers(self) -> list[dict[str, Any]]:
        snapshot = self.snapshot or {}
        known = {row["worker_id"]: row for row in snapshot.get("workers", [])}
        stopped = {item["body"]["existing_worker_id"] for item in self.local["requests"]
                   if item["body"] and item["body"]["mode"] == "return" and item["state"] == "TERMINAL"
                   and item["error"] in {"WORKER_DISABLED", "WORKER_NOT_FOUND"}
                   and not (item.get("error_version") is not None
                            and snapshot.get("version", -1) > item["error_version"]
                            and known.get(item["body"]["existing_worker_id"], {}).get("status") == "ACTIVE")}
        rows = [dict(row) for row in (self.snapshot or {}).get("workers", [])
                if row["status"] == "ACTIVE" and row["worker_id"] not in stopped]
        for row in self.local["workers"]:
            resolved = self.resolved_id(row["worker_id"])
            if resolved in known or resolved in stopped or resolved in (self.snapshot or {}).get("unselectable_worker_ids", []):
                continue  # Including DISABLED: never resurrect through the local alias.
            link = self.local["temp_links"].get(row["worker_id"], {})
            rows.append({**row, **link, "worker_id": resolved, "status": "ACTIVE"})
        return rows

    def active_worker(self, *, worker_id: str = "", name: str = "") -> dict[str, str] | None:
        for row in self.active_workers():
            matches = row["worker_id"] == self.resolved_id(worker_id) if worker_id else row["display_name"] == name
            if matches:
                return row
        return None
