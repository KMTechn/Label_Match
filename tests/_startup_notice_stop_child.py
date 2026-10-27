"""A bounded fake Tk seam, never a product entrypoint or a real window."""
import os
from pathlib import Path
import sys
import time
from types import SimpleNamespace
from unittest.mock import Mock

root = Path(sys.argv[1])
os.environ["LABEL_MATCH_DIRECT_SYNC_ROOT"] = str(root / "relay")
os.environ["TEMP"] = str(root)
os.environ["TMP"] = str(root)

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import Label_Match as lm


class Parent:
    def __init__(self):
        self.jobs = []
        self.closed = False
        self.closed_in_loop = False

    def __getattr__(self, name):
        return Mock()

    def after(self, delay, callback):
        self.jobs.append((delay, callback))
        return str(len(self.jobs))

    def destroy(self):
        self.closed = True

    def mainloop(self):
        print("NOTICE_READY", flush=True)
        for _ in range(8):
            if self.closed or (root / "finish-test").exists():
                break
            if not self.jobs:
                break
            delay, callback = self.jobs.pop(0)
            time.sleep(delay / 1000)
            callback()
        self.closed_in_loop = self.closed


parent = Parent()
lm.tk.Tk = lambda: parent
lm.tk.Label = lm.tk.Button = lambda *args, **kwargs: SimpleNamespace(pack=Mock())
lm._show_first_run_registration_recovery("POSSESSION_PROOF_REATTACH_REQUIRED")
assert parent.closed_in_loop
