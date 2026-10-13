"""Shared startup verification hold for the GUI and DirectSync relay."""
from __future__ import annotations

import json
import os
from pathlib import Path

from writer_session_fence import WriterFencedError, writer_admission, writer_sink, remote_request


def startup_send_hold_path(direct_sync_root) -> Path:
    return Path(direct_sync_root) / "control" / "startup-verification.json"


def read_startup_send_hold(direct_sync_root):
    from logistics_runtime_profile import assert_path_has_no_reparse_components
    path = startup_send_hold_path(direct_sync_root)
    try:
        assert_path_has_no_reparse_components(path, label="startup verification hold")
        with path.open("rb") as stream:
            raw = stream.read(4097)
        if len(raw) > 4096:
            return {"start_path": "INVALID"}
        value = json.loads(raw.decode("utf-8"))
        if (not isinstance(value, dict) or set(value) != {"pid", "creation_identity", "started_at", "start_path"}
                or type(value["pid"]) is not int or value["pid"] <= 0
                or not isinstance(value["creation_identity"], str) or not value["creation_identity"].isdigit()
                or value["start_path"] != "FAST_WINDOW_FIRST"):
            return {"start_path": "INVALID"}
        from auto_update import _utc
        _utc(value["started_at"])
        return value
    except FileNotFoundError:
        return None
    except (OSError, ValueError, TypeError, RuntimeError):
        return {"start_path": "INVALID"}


def startup_send_deferred(direct_sync_root):
    hold = read_startup_send_hold(direct_sync_root)
    if hold is None:
        return None
    reason = "STARTUP_VERIFICATION_PENDING"
    if hold.get("start_path") == "INVALID":
        reason = "STARTUP_VERIFICATION_HOLD_INVALID"
    else:
        from auto_update import native_process_identity
        owner = native_process_identity(hold["pid"])
        if owner.get("status") != "LIVE" or owner.get("creation_identity") != hold["creation_identity"]:
            reason = "STARTUP_VERIFICATION_OWNER_UNAVAILABLE"
    return {"status": "DEFERRED", "reason_code": reason, "error_code": reason}


@writer_sink("startup_send_hold")
def create_startup_send_hold(direct_sync_root):
    from auto_update import native_process_identity, now_utc
    from user_relay import _write_machine_json_atomic
    if read_startup_send_hold(direct_sync_root) is not None:
        return None
    owner = native_process_identity(os.getpid())
    if owner.get("status") != "LIVE":
        raise WriterFencedError("STARTUP_VERIFICATION_OWNER_UNAVAILABLE", "startup owner is unavailable")
    hold = {"pid": os.getpid(), "creation_identity": owner["creation_identity"],
            "started_at": now_utc(), "start_path": "FAST_WINDOW_FIRST"}
    _write_machine_json_atomic(startup_send_hold_path(direct_sync_root), hold)
    return hold


@writer_sink("startup_send_release")
def release_startup_send_hold(direct_sync_root, expected, *, verified: bool, full: bool = False) -> bool:
    if not verified or expected is None or read_startup_send_hold(direct_sync_root) != expected:
        return False
    if not full:
        from auto_update import native_process_identity
        owner = native_process_identity(os.getpid())
        if (expected.get("pid") != os.getpid() or owner.get("status") != "LIVE"
                or owner.get("creation_identity") != expected.get("creation_identity")):
            return False
    startup_send_hold_path(direct_sync_root).unlink()
    return True


class StartupGatedSession:
    """Admit each new HTTP separately; an admitted request may finish."""
    def __init__(self, direct_sync_root, session=None):
        self.direct_sync_root = direct_sync_root
        if session is None:
            import requests
            session = requests.Session()
        self.session = session

    def post(self, *args, **kwargs):
        with writer_admission("startup_send_request"):
            deferred = startup_send_deferred(self.direct_sync_root)
            if deferred:
                raise WriterFencedError(deferred["reason_code"], "startup verification is pending")
            with remote_request():
                return self.session.post(*args, **kwargs)

    def __getattr__(self, name):
        return getattr(self.session, name)
