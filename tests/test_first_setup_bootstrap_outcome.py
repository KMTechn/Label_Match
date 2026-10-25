"""Exercise the registered autorun contract without starting any process."""
from datetime import datetime, timezone
import json
from pathlib import Path
import subprocess
from types import SimpleNamespace

import pytest

import auto_update as updates
from tests import _first_setup_host as host


def _scenario(tmp_path, monkeypatch, *, pending, returncode=1, terminal="CONFIRMED"):
    config = tmp_path / "host.json"
    config.write_text("{}", encoding="utf-8")
    command = subprocess.list2cmdline([
        "C:/test/python.exe", "-I", "-B", "C:/test/auto_update.py",
        "--bootstrap", "--role", "relay", "--label-match-user-relay"])
    host.write(tmp_path / "hkcu-run.json", {"command": command})
    log = tmp_path / "bootstrap.private.log"
    log.write_text("  in _bootstrap\n  in assert_ordinary_recovery\nValueError: an earlier or unrelated writer prevents ordinary recovery\n",
                   encoding="utf-8")
    clock, waits, stops = [130.0 if pending else 160.0], [], []
    epoch = 1760000000.0
    def stamp(seconds):
        return datetime.fromtimestamp(epoch + seconds, timezone.utc).isoformat()
    prepared = updates.prepare_transition(previous_sha="a" * 40, candidate_sha="b" * 40,
        packet_sha256="c" * 64, forward_pin="d" * 64, rollback_pin="e" * 64,
        transition_id="1" * 32, nonce="2" * 32, now=stamp(-1))
    state = updates.apply_transition(prepared, writers_absent=True,
        packet_verified=True, durable_prepared=True, now=stamp(0))
    state.update(trial_pid=123, trial_sha="b" * 40, trial_role="relay", trial_started_at=stamp(24))
    journal = tmp_path / "machine/update-control/transition.json"
    def settle():
        state["phase"] = terminal
        if terminal == "CONFIRMED":
            state["confirmed_at"] = stamp(150)
        elif terminal == "ROLLED_BACK":
            # The real rollback retains trial identity but rotates its nonce.
            state.update(nonce="3" * 32, confirmed_at=stamp(150), rollback_at=stamp(140))
        host.write(journal, state)
    host.write(journal, state)
    if not pending:
        settle()
    def sleep(seconds):
        waits.append(seconds)
        clock[0] += seconds
        if terminal != "NEVER" and clock[0] >= 150:
            settle()
    process = SimpleNamespace(wait=lambda: returncode, host_test_output=log,
                              terminate=lambda: stops.append("terminate"),
                              kill=lambda: stops.append("kill"))
    launches = []
    def launch(*args, **kwargs):
        launches.append((args, kwargs))
        return process
    monkeypatch.setattr(host, "launch", launch)
    monkeypatch.setattr(host, "time", SimpleNamespace(
        monotonic=lambda: clock[0], time=lambda: epoch + clock[0], sleep=sleep))
    return config, clock, waits, stops, launches


@pytest.mark.parametrize("pending", [False, True])
def test_known_bootstrap_tail_does_not_stop_system_confirmation(tmp_path, monkeypatch, pending):
    config, clock, waits, stops, launches = _scenario(tmp_path, monkeypatch, pending=pending)
    host.autorun(config)
    proof = json.loads((tmp_path / "bootstrap-outcome-proof.json").read_bytes())
    assert proof["bootstrap_return_code"] == 1 and proof["known_p3_tail"]
    assert proof["phase"] == "CONFIRMED" and proof["confirmed_after_applied_seconds"] == 150
    assert not proof["candidate_stopped_by_observer"] and not stops
    assert len(launches) == 1 and clock[0] == 160
    assert bool(waits) is pending


@pytest.mark.parametrize("terminal", ["ROLLED_BACK", "DEFERRED", "NEVER"])
def test_nonconfirmed_system_outcome_stays_a_failure(tmp_path, monkeypatch, terminal):
    config, clock, waits, stops, _ = _scenario(tmp_path, monkeypatch, pending=True, terminal=terminal)
    with pytest.raises(AssertionError):
        host.autorun(config)
    proof = json.loads((tmp_path / "bootstrap-outcome-proof.json").read_bytes())
    assert not stops and proof["phase"] != "CONFIRMED"
    assert clock[0] <= 300 and waits
    assert proof["timed_out"] is (terminal == "NEVER")


def test_other_bootstrap_failure_is_not_accepted(tmp_path, monkeypatch):
    config, _, _, stops, _ = _scenario(tmp_path, monkeypatch, pending=False, returncode=2)
    with pytest.raises(AssertionError, match="outside the allowed secondary"):
        host.autorun(config)
    proof = json.loads((tmp_path / "bootstrap-outcome-proof.json").read_bytes())
    assert not proof["known_p3_tail"] and not stops


def test_zero_bootstrap_exit_keeps_the_existing_success_path(tmp_path, monkeypatch):
    config, _, waits, stops, launches = _scenario(tmp_path, monkeypatch, pending=True, returncode=0)
    host.autorun(config)
    assert len(launches) == 1 and not waits and not stops
    assert not (tmp_path / "bootstrap-outcome-proof.json").exists()
