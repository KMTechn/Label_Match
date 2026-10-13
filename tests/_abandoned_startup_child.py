"""Exit an isolated test child while it owns the requested real mutex."""
import os
from pathlib import Path
import sys
from types import SimpleNamespace

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import auto_update as updates
import writer_session_fence as fence

mode, root, marker, sid = sys.argv[1:]
root, marker = Path(root), Path(marker)
if mode == "update":
    with updates.update_lock(root, sid):
        marker.write_text("owned")
        os._exit(17)
elif mode == "phs":
    os.environ[fence.TEST_MODE_ENV] = "1"
    os.environ[fence.CONTROL_ROOT_OVERRIDE_ENV] = str(root)
    updates.assert_version_writer = lambda *_a, **_k: None
    from phs_label_workflow import PHSLabelExchangeCoordinator

    class Client:
        def get_phs_label_exchange(self, *_args, **_kwargs):
            with fence.remote_request():
                assert fence._WRITER_LOCAL.source == "phs_current_recovery"
                assert fence._WRITER_LOCAL.lease.owned
                marker.write_text("owned")
                os._exit(17)

    state = {"status": "PREPARED", "canonical_input_tag_qr": "test-source",
             "set_id": "test-set", "exchange_id": "test-exchange"}
    coordinator = SimpleNamespace(journal=SimpleNamespace(load=lambda: state),
                                  available=lambda: True, client=Client())
    PHSLabelExchangeCoordinator._resume_current(coordinator,
        {"id": "test-set", "canonical_input_tag_qr": "test-source"},
        persist_current_set=lambda: True)
else:
    raise SystemExit(2)
