"""Read-only host inventory guarding isolated installer subprocesses."""

import os
from pathlib import Path
import stat

import pytest


class HostProgramDataGuard:
    def __init__(self):
        self.root = Path("C:/ProgramData/KMTech")
        self.before, self.unreadable = self._snapshot()

    def _snapshot(self):
        entries, unreadable = set(), set()
        pending = [self.root]
        while pending:
            parent = pending.pop()
            try:
                with os.scandir(parent) as children:
                    for child in children:
                        entries.add(Path(child.path).relative_to(self.root))
                        if child.is_dir(follow_symlinks=False) and not child.is_symlink():
                            # Never follow junctions into other host locations.
                            attributes = getattr(child.stat(follow_symlinks=False), "st_file_attributes", 0)
                            if not attributes & stat.FILE_ATTRIBUTE_REPARSE_POINT:
                                pending.append(Path(child.path))
            except FileNotFoundError:
                continue
            except PermissionError:
                unreadable.add(parent.relative_to(self.root))
        return entries, unreadable

    def assert_unchanged(self):
        current, unreadable = self._snapshot()
        assert not current.difference(self.before), "Host ProgramData has new items"
        assert unreadable == self.unreadable, "Host ProgramData visibility changed"


@pytest.fixture
def host_program_data_guard():
    guard = HostProgramDataGuard()
    guard.assert_unchanged()  # Check before any installer child is started.
    yield guard
    guard.assert_unchanged()
