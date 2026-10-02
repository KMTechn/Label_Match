"""Stage-one worker choices; record and recovery identities remain names."""
from __future__ import annotations

import hashlib
import json
import logging
import os
from pathlib import Path
import unicodedata
import uuid
from datetime import datetime, timezone

CONTRACT_VERSION = "worker-roster.v1"
MAX_CACHE_BYTES = 2 * 1024 * 1024


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
            or payload.get("contract_version") != CONTRACT_VERSION
            or payload.get("authority_scope_id") != scope
            or type(payload.get("version")) is not int or payload["version"] < 0
            or type(payload.get("workers")) is not list):
        raise ValueError("invalid roster")
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
    return roster is None or not roster.get("_received_workers", roster["workers"])


def _read_cache(path, scope):
    with path.open("rb") as handle:
        raw = handle.read(MAX_CACHE_BYTES + 1)
    if len(raw) > MAX_CACHE_BYTES:
        raise ValueError("cache too large")
    payload = json.loads(raw.decode("utf-8"), object_pairs_hook=_unique_object)
    validate_roster(payload, scope)
    if set(payload) != {"contract_version", "authority_scope_id", "version",
                        "workers", "fetched_at", "sha256"}:
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
