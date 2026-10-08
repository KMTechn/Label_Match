"""A real relay status writer sharing the isolated Windows admission mutex."""
import json
import os
from pathlib import Path
import sys
import time

import user_relay
from writer_session_fence import WriterFencedError


def main():
    root, app_root, start, stop, events = map(Path, sys.argv[1:])
    lease = user_relay._acquire_relay_lease(root / "user-relay-instance")
    assert lease is not None
    status = user_relay.user_relay_status_path(root)
    original = user_relay.writer_admission

    from contextlib import contextmanager

    @contextmanager
    def admission(*args, **kwargs):
        started = time.monotonic()
        try:
            with original(*args, **kwargs):
                yield
        except WriterFencedError as exc:
            with events.open("a", encoding="utf-8") as stream:
                stream.write(json.dumps({"code": exc.code, "seconds": time.monotonic() - started}) + "\n")
            raise

    user_relay.writer_admission = admission
    try:
        while not start.exists() and not stop.exists():
            time.sleep(0.02)
        user_relay.run_persistent_relay_loop(
            lambda: {"status": "PASS"}, status_path=status,
            interval_seconds=1, stop_requested=stop.exists,
            metadata={"worker_id": user_relay.LABEL_MATCH_WORKER_ID,
                      "process_id": os.getpid(), "app_root": str(app_root),
                      "code_root": str(app_root), "direct_sync_root": str(root)},
        )
    finally:
        lease.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
