"""Bounded, headless coverage of official stop during standalone notices."""

from types import SimpleNamespace
from unittest.mock import Mock

import pytest

import Label_Match as lm
import startup_observation
import user_relay


@pytest.mark.parametrize("notice", ["registration-recovery", "startup-refusal"])
def test_startup_notice_observes_official_request(monkeypatch, tmp_path, notice):
    monkeypatch.setenv("LABEL_MATCH_DIRECT_SYNC_ROOT", str(tmp_path / "relay"))
    monkeypatch.setattr(user_relay, "_app_stop_process_snapshot", lambda **kwargs: [])
    monkeypatch.setattr(user_relay, "request_user_relay_stop",
                        lambda *args, **kwargs: {"status": "ABSENT"})
    parents = []

    class Parent:
        def __init__(self):
            self.jobs = []
            self.destroyed = False
            self.destroyed_in_loop = False
            self.elapsed_ms = 0
            parents.append(self)

        def __getattr__(self, name):
            return Mock()

        def after(self, delay, callback):
            self.jobs.append((delay, callback))
            return str(len(self.jobs))

        def destroy(self):
            self.destroyed = True

        def mainloop(self):
            # Use the public publisher, with only OS process/relay observation
            # replaced. The live fake window must not be reported as stopped.
            monkeypatch.setattr(user_relay, "_owned_app_stop_processes",
                                lambda *args, **kwargs: [object()] if not self.destroyed else [])
            result = user_relay.request_app_stop(
                tmp_path / "relay", app_root=tmp_path / "app", timeout_seconds=0,
            )
            assert result["status"] == "UNKNOWN"
            assert result["remaining_process_count"] == 1
            for _ in range(8):
                if self.destroyed or not self.jobs:
                    break
                delay, callback = self.jobs.pop(0)
                self.elapsed_ms += delay
                callback()
            self.destroyed_in_loop = self.destroyed

    monkeypatch.setattr(lm.tk, "Tk", Parent)
    widget = lambda *args, **kwargs: SimpleNamespace(pack=Mock())
    monkeypatch.setattr(lm.tk, "Label", widget)
    monkeypatch.setattr(lm.tk, "Button", widget)
    if notice == "registration-recovery":
        lm._show_first_run_registration_recovery("POSSESSION_PROOF_REATTACH_REQUIRED")
    else:
        startup_observation.show_startup_blocked_notice()

    assert len(parents) == 1
    assert parents[0].destroyed_in_loop, "official stop must close the running notice loop"
    assert parents[0].elapsed_ms <= 500
