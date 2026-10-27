"""Separate native mutex owner used by the relay entry regression tests."""
import runpy
import sys
import time
from pathlib import Path


def main():
    root, mode, guard = sys.argv[1:]
    root = Path(root).resolve()
    if guard:
        runpy.run_path(guard)
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
    import user_relay

    key = root / "direct" / "user-relay-instance"
    stop = root / "observer-stop"
    if mode == "hold":
        lease = user_relay._acquire_relay_lease(key)
        assert lease is not None
        try:
            (root / "observer-ready").touch()
            while not stop.exists():
                time.sleep(0.01)
        finally:
            lease.close()
        return
    assert mode == "observe"
    previous = ""
    (root / "observer-ready").touch()
    while not stop.exists():
        started = time.monotonic()
        turn_path = root / "observer-turn"
        turn = turn_path.read_text() if turn_path.exists() else ""
        lease = user_relay._acquire_relay_lease(key)
        if lease is not None:
            try:
                if turn and turn != previous:
                    # One real 50 ms observation pulse establishes overlap.
                    (root / "observer-held").write_text(turn)
                    time.sleep(0.05)
                    previous = turn
            finally:
                lease.close()
        time.sleep(max(0.0, 0.05 - (time.monotonic() - started)))


if __name__ == "__main__":
    main()
