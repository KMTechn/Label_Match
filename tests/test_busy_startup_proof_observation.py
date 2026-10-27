"""The optional remember observation cannot weaken or revoke busy admission."""
import json
from pathlib import Path

import pytest

import auto_update as updates
from tests.test_first_setup_relay_parent_cohort import cohort


@pytest.mark.parametrize("observation", ["changed-active", "unreadable-active"])
def test_busy_remember_race_keeps_start_but_issues_no_proof(cohort, monkeypatch, observation):
    actual_writer_proof = updates.assert_version_writer
    actual_read = updates.read_json
    remembering = [False]

    def after_strict_proof(root, **kwargs):
        actual_writer_proof(root, **kwargs)
        if kwargs["source"] == "gui_settings_save":
            remembering[0] = True
            if observation == "changed-active":
                (cohort.control / "active.json").write_text(json.dumps({"sha": "b" * 40}))

    def read(path, *args, **kwargs):
        if remembering[0] and observation == "unreadable-active" and Path(path) == cohort.control / "active.json":
            raise OSError("isolated remember observation unavailable")
        return actual_read(path, *args, **kwargs)

    monkeypatch.setattr(updates, "assert_version_writer", after_strict_proof)
    monkeypatch.setattr(updates, "read_json", read)
    assert updates.startup_gate(cohort.app, ["--label-match-user-relay"]) is None
    assert remembering == [True]
    assert updates._STARTUP_ADMISSION is None


@pytest.mark.parametrize("proof", ["assert_ordinary_recovery", "assert_version_writer"])
def test_busy_strict_proof_refusal_still_propagates(cohort, monkeypatch, proof):
    failure = ValueError("isolated strict proof refusal")

    def refused(*_args, **_kwargs):
        raise failure

    monkeypatch.setattr(updates, proof, refused)
    with pytest.raises(ValueError) as observed:
        updates.startup_gate(cohort.app, ["--label-match-user-relay"])
    assert observed.value is failure
    assert updates._STARTUP_ADMISSION is None
