"""The bootstrap owns the update mutex until its real relay readback ends."""
from types import SimpleNamespace
import json
import time

import pytest

import user_relay


BUSY = "update mutex ownership is busy"


def test_busy_owner_readback_retries_only_while_own_bootstrap_is_alive():
    attempts = []
    process = SimpleNamespace(pid=41, poll=lambda: None)

    def owner():
        attempts.append(True)
        if len(attempts) < 3:
            raise RuntimeError(BUSY)
        return {"process_id": 42, "updated_at": "2026-10-24T00:00:00Z"}

    result = user_relay._read_started_relay(
        process, owner, 0, time.monotonic() + 5, True, lambda _: None,
    )
    assert result["status"] == "ALIVE"
    assert len(attempts) == 3


@pytest.mark.parametrize("returncode,message", [
    (0, BUSY), (None, "update mutex ownership was abandoned"),
    (None, "unrelated runtime failure"),
])
def test_other_owner_readback_errors_keep_the_original_exception(returncode, message):
    original = RuntimeError(message)

    def owner():
        raise original

    with pytest.raises(RuntimeError) as caught:
        user_relay._read_started_relay(
            SimpleNamespace(pid=41, poll=lambda: returncode), owner,
            0, time.monotonic() + 5, True, lambda _: None,
        )
    assert caught.value is original


def test_busy_is_not_ignored_if_bootstrap_exits_during_the_query():
    state = [None]

    def owner():
        state[0] = 0
        raise RuntimeError(BUSY)

    with pytest.raises(RuntimeError, match=BUSY):
        user_relay._read_started_relay(
            SimpleNamespace(pid=41, poll=lambda: state[0]), owner,
            0, time.monotonic() + 5, True, lambda _: None,
        )


def test_busy_until_parent_deadline_remains_unproven(monkeypatch):
    clock = [0.0]
    monkeypatch.setattr(user_relay.time, "monotonic", lambda: clock[0])

    def owner():
        clock[0] += 1
        raise RuntimeError(BUSY)

    with pytest.raises(user_relay.UserRelayError, match="did not prove"):
        user_relay._read_started_relay(
            SimpleNamespace(pid=41, poll=lambda: None), owner,
            0, 3, True, lambda _: None,
        )


@pytest.mark.parametrize("exception_type", [OSError, RuntimeError])
def test_parent_owner_readback_exception_has_a_fixed_support_code(tmp_path, monkeypatch, exception_type):
    import auto_update
    import current_user_onboarding as onboarding

    original = exception_type("owner query failed")
    records, calls = [], []
    monkeypatch.setattr(onboarding, "_machine_worker_context", lambda *_: None)
    monkeypatch.setattr(onboarding, "resolve_current_user_onboarding_paths",
                        lambda *_: SimpleNamespace(direct_sync_root=tmp_path / "direct"))
    monkeypatch.setattr(user_relay, "build_user_relay_command",
                        lambda *_: ["test-runtime", "--bootstrap", "--"])
    monkeypatch.setattr(auto_update, "start_ordinary_process",
                        lambda *_a, **_kw: SimpleNamespace(pid=41, poll=lambda: None))

    def owner(*_args, **_kwargs):
        calls.append(True)
        if len(calls) > 1:
            raise original
        return None

    monkeypatch.setattr(user_relay, "_existing_user_relay_owner", owner)
    monkeypatch.setattr(user_relay, "_record_relay_reason", lambda code, **kw: records.append((code, kw)))
    with pytest.raises(exception_type) as caught:
        user_relay.start_user_relay_process(tmp_path, survival_seconds=0)
    assert caught.value is original
    assert [code for code, _ in records] == ["RELAY_OWNER_READBACK_FAILED"]
    assert records[0][1]["error"] is original


def test_owner_readback_support_code_survives_the_actual_public_record(capsys):
    user_relay._record_relay_reason("RELAY_OWNER_READBACK_FAILED", error=RuntimeError("private query detail"))
    record = json.loads(capsys.readouterr().out)
    assert record["reason_code"] == "RELAY_OWNER_READBACK_FAILED"
    assert record["exception_type"] == "RuntimeError"
    assert "nonconforming_fields" not in record
    assert "private query detail" not in json.dumps(record)
