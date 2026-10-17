"""Unexecuted contract probe for the pinned sub-184 review.

Run only in a lane-authorized isolated copy of the pinned commit, with that
copy's tests directory on the import path and the lane's headless runner.
This file was written and parsed as source only. It is not RED evidence.
No process identifier, fixture value, or exception body is printed here.
"""
from __future__ import annotations

import copy
import pytest

from test_recovery_gui_bootstrap import memory_world as _memory_world


def memory_world(*args, **kwargs):
    world = _memory_world(*args, **kwargs)
    bootstrap = world.bootstrap
    world.bootstrap = lambda *a, **kw: (bootstrap(*a, **kw)['rc'], None)
    return world


@pytest.mark.parametrize("shape", ["absent", "not_integer", "nonpositive", "boolean"])
def test_gui_recovery_requires_positive_integer_relay_pid(shape):
    world = memory_world("FAILED", "NONE", writer="relay")
    row = world.rows[0]
    if shape == "absent":
        row.pop("pid", None)
    elif shape == "not_integer":
        row["pid"] = None
    elif shape == "nonpositive":
        row["pid"] = -1
    else:
        row["pid"] = True
    before = copy.deepcopy(world.files)

    result, _ = world.bootstrap("gui")

    assert result != 0
    assert not world.launches
    assert world.files == before
