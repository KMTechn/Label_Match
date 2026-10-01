"""Review regressions using real functions; no installer, relay or GUI starts."""
from pathlib import Path

import pytest
import auto_update as updates


ROOT = Path(__file__).resolve().parents[1]


@pytest.mark.parametrize("trial", [False, True])
def test_ordinary_user_reopens_native_update_mutex(tmp_path, trial):
    if not updates.ordinary_user_context():
        pytest.skip("native reopening requires an ordinary interactive caller")
    # The unique tmp path hashes to a fresh object; no product mutex is touched.
    sid = updates.current_process_sid()
    with updates.update_lock(tmp_path, sid, trial=trial):
        with updates.update_lock(tmp_path, sid, trial=trial):
            pass
