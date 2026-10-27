"""ANSWER-12: follow an expired startup lock into the first real writer gate."""
import contextlib
from pathlib import Path
from types import SimpleNamespace

import pytest

import auto_update as updates
import writer_session_fence as fence
from tests.test_first_setup_relay_parent_cohort import cohort, PARENT_ENV


@pytest.mark.parametrize("difference", ["unbound-parent", "other-writer"])
def test_expired_busy_relay_refuses_unrelated_writer_before_first_write(
        cohort, monkeypatch, tmp_path, difference):
    import current_user_onboarding as onboarding
    import label_data_generation
    import user_relay

    if difference == "unbound-parent":
        monkeypatch.delenv(PARENT_ENV)
    else:
        import json
        other = dict(cohort.rows[0])
        other["ProcessId"] += 1
        cohort.processes.write_text(json.dumps([*cohort.rows, other]))
    monkeypatch.setattr(updates, "startup_update_lock",
        lambda *_a, **_kw: contextlib.nullcontext(False))

    sentinel = tmp_path / "business-sentinel"
    sentinel.write_bytes(b"unchanged fixture business bytes")
    paths = SimpleNamespace(data_root=tmp_path / "data", ledger_path=tmp_path / "ledger/ledger.db",
        direct_sync_root=tmp_path / "direct", queue_dir=tmp_path / "queue",
        logistics_profile_path=tmp_path / "profile/profile.json")
    monkeypatch.setattr(onboarding, "resolve_current_user_onboarding_paths", lambda *_: paths)
    monkeypatch.setattr(label_data_generation, "normalize_data_arguments", lambda *_: None)
    # Read-only generation observation is ALLOW; the actual writer permission
    # check below remains intact. No profile/credential/field data is read.
    monkeypatch.setattr(user_relay, "check_user_data_open", lambda *_: {"status": "ALLOW"})
    writes = []

    def first_writer_boundary(*_a, **_kw):
        with fence.writer_admission("data_generation_record"):
            writes.append("first writer admitted")
            raise AssertionError("first write boundary reached before unrelated-writer refusal")

    monkeypatch.setattr(user_relay, "record_user_data_open", first_writer_boundary)
    args = SimpleNamespace(request_app_stop=False, direct_sync_root="", scan_source_dir="")
    with pytest.raises(ValueError, match="earlier or unrelated writer"):
        user_relay._run_user_relay(args, cohort.bundle, [], credential_input=object())
    assert writes == []
    assert sentinel.read_bytes() == b"unchanged fixture business bytes"
