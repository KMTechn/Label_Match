"""Reject duplicate or wrong-role primary children before selecting evidence."""
import json
from types import SimpleNamespace

import pytest

from _retained_endpoint_fixture import observation_path, primary_observation


class Endpoint(tuple):
    pass


def endpoint(tmp_path, observations):
    result = Endpoint((tmp_path / "observation.json",))
    result.children = []
    for pid, role, peer in observations:
        result.children.append(SimpleNamespace(pid=pid))
        observation_path(result[-1], pid).write_text(json.dumps({
            "pid": pid, "relay": role == "relay", "peer": peer, "nonce_present": False,
        }))
    return result


@pytest.mark.parametrize("second_role", ["gui", "relay"])
def test_primary_observation_rejects_an_extra_non_peer(tmp_path, second_role):
    value = endpoint(tmp_path, [(101, "gui", False), (102, second_role, False)])
    with pytest.raises(AssertionError):
        primary_observation(value, role="gui", start=0)


def test_primary_observation_allows_one_primary_and_a_peer(tmp_path):
    value = endpoint(tmp_path, [(101, "gui", False), (102, "relay", True)])
    assert primary_observation(value, role="gui", start=0)["pid"] == 101
