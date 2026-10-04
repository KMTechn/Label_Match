"""Real entrypoint/process shutdown with fake Tk and a test-only native mutex."""

from __future__ import annotations

import json
import os
from pathlib import Path
import subprocess
import sys
import threading
import time
from types import SimpleNamespace

import pytest


@pytest.mark.parametrize("mode", [
    "close", "decline", "raise", "return", "accepted_return", "stall",
    "interrupt_cleanup", "exit_cleanup", "sync_return",
])
def test_entrypoint_exit_releases_workers_and_replays_durable_state(tmp_path, mode):
    root = tmp_path / mode
    root.mkdir()
    env = os.environ.copy()
    env.update({
        "PYTHONDONTWRITEBYTECODE": "1", "LABEL_MATCH_AUTOMATED_TEST": "1",
        "LABEL_MATCH_SAVE_DIR": str(root), "LABEL_MATCH_AUDIO_ENABLED": "off",
        "LABEL_MATCH_DIRECT_SYNC_BOOTSTRAP": "off", "LABEL_MATCH_SESSION_SYNC_TRIGGER": "off",
        "LABEL_MATCH_UPDATE_PROVIDER": "off", "KMTECH_TEST_SILENT_AUDIO": "1",
        "KMTECH_LABEL_WRITER_TEST_MODE": "1", "KMTECH_LABEL_WRITER_CONTROL_ROOT": str(root / "control"),
    })
    # communicate's observation deadline never kills a child. The probe's own
    # daemon observer sends the lane's normal STOP only after recording failure.
    audit_prefix = {}
    pending_identity = None
    for stage in (mode, "restart"):
        process = subprocess.Popen(
            [sys.executable, "-B", str(Path(__file__).resolve()), "--probe", stage, str(root)],
            env=env, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
            creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0),
        )
        stdout, stderr = process.communicate(timeout=10)
        (root / f"{stage}-stdout.log").write_bytes(stdout + stderr)
        assert process.returncode == (17 if stage == "exit_cleanup" else 0), (
            stdout + stderr).decode("utf-8", errors="replace")
        observed = json.loads((root / f"{stage}.json").read_text(encoding="utf-8"))
        assert observed["owner"] is True
        if stage == "stall":
            assert any(not worker["daemon"] for worker in observed["workers"])
            assert observed["destroyed"] is False
            assert observed["guard_held_during_cleanup"] is True
            rescue = json.loads((root / f"{stage}-rescue.json").read_text(encoding="utf-8"))
            assert rescue["guard_held"] is True
        else:
            assert observed["workers"] == []
            assert observed["destroyed"] is True
            assert not (root / f"{stage}-rescue.json").exists()
        assert observed["pending_preserved"] is True
        assert observed["audit_preserved"] is True
        assert observed["guard_held_during_cleanup"] is True
        assert observed["current_state_preserved"] is True
        expected_close_events = (1 if mode == "stall" else 2) if stage == "restart" else int(mode != "stall")
        assert observed["close_events"] == expected_close_events
        if stage == "restart":
            assert observed["pending_identity"] == pending_identity
            for relative, prefix in audit_prefix.items():
                assert (root / relative).read_bytes().startswith(prefix)
        else:
            pending_identity = observed["pending_identity"]
            audit_prefix = {p.relative_to(root): p.read_bytes() for p in root.rglob("*작업이벤트로그*.csv")}
        if stage == "decline":
            assert observed["decline_kept_work"] is True
        expected = {"raise": "RuntimeError", "stall": "TimeoutError",
                    "interrupt_cleanup": "KeyboardInterrupt", "exit_cleanup": "SystemExit"}
        assert observed["exception"] == expected.get(stage)


def _probe(mode, root):
    import csv
    import hashlib
    import Label_Match as lm
    import label_match_single_instance as instance
    from package_logistics import PackageOutbox
    from tests.test_package_logistics import _draft_for_set
    from tests.test_tk_serial_ui_lane import FakeTkRoot
    from tk_serial_ui_lane import LaneTask, LaneState, TkSerialUiLane

    root = Path(root)
    instance.mutex_name_for_data_scope = lambda scope: (
        "Local\\KMTech.LabelMatch.ExitTest."
        + hashlib.sha256(str(Path(scope).resolve()).casefold().encode()).hexdigest()[:24]
    )
    acquired = []

    def acquire(scope):
        lease = instance.acquire_data_scope_mutex(scope)
        acquired.append(lease.owner)
        return lease

    def owns_guard():
        other = instance.acquire_data_scope_mutex(root)
        owns = not other.owner
        other.close()
        return owns

    original = lm.Label_Match
    apps = []
    release = threading.Event()
    main_returned = threading.Event()
    verdict = {"exception": None, "decline_kept_work": False}

    class ProbeApp(original):
        def __getattr__(self, name):
            raise AttributeError(name)

        def __init__(self):
            self.root = FakeTkRoot()
            self.run_tests = True
            self.initialized_successfully = True
            self.save_directory = str(root)
            self.current_set_info = {"id": "exit-partial", "raw": ["unfinished"], "parsed": []}
            self.is_blinking = False
            self._ui_lane_generation = 0
            self.data_manager = lm.DataManager(str(root), "exit-test", "operator", "lm281")
            self.package_outbox = PackageOutbox(root / "pending.sqlite3")
            if mode != "restart":
                self.package_outbox.enqueue(_draft_for_set("EXIT-PENDING"))
                self.data_manager.log_event(self.Events.APP_START, {"message": "exit-test"})
                self.data_manager.flush(timeout=2)
            self.ui_lane = TkSerialUiLane(self, poll_ms=1, worker_name="lm281-ui-lane")
            self._save_current_set_state = lambda: self.data_manager.save_current_state(
                {"current_set_info": self.current_set_info})
            if mode != "restart":
                assert self._save_current_set_state()
            self._has_background_work = lambda: False
            self._sealed_transfer_exchange_blocks_local_action = lambda *_: False
            self._release_shortage_confirmation = lambda: True
            self._show_ui_lane_rejection = lambda *_: None
            self._save_app_settings = lambda: None
            self._show_app_close_cleanup_delay = lambda: None
            self._stop_error_siren = lambda: None
            self.guard_observations = [owns_guard()]
            self.direct_sync_bootstrap_context = {"scan_source_dir": str(root)}
            apps.append(self)

        def after(self, delay, callback, *args):
            # The fake scheduler preserves callback order. The watchdog is
            # intentionally unused: this probe's real work completes in 40ms.
            if getattr(callback, "__name__", "") == "expire_overdue_drain":
                return "unused-watchdog"
            return self.root.after(delay, callback, *args)

        def after_cancel(self, job):
            return self.root.after_cancel(job)

        def update(self):
            self.guard_observations.append(owns_guard())
            self.root.run_one()
            time.sleep(0.001)

        def title(self):
            return "fake Label Match"

        def state(self):
            return "withdrawn"

        def mainloop(self):
            if mode in {"close", "restart", "decline"}:
                if mode == "decline":
                    self.run_tests = False
                    lm.messagebox.askokcancel = lambda *_a, **_k: False
                    self.on_closing()
                    verdict["decline_kept_work"] = (
                        not self.__dict__.get("_app_close_in_progress", False)
                        and self.ui_lane.worker_thread.is_alive() and owns_guard()
                        and self.current_set_info["raw"] == ["unfinished"]
                    )
                    self.run_tests = True
                self.on_closing()
                self.root.run_until(lambda: self.__dict__.get("_tk_destroy_complete", False))
            elif mode in {"accepted_return", "stall"}:
                self.ui_lane.submit(LaneTask(
                    name="durable-before-exit", generation=0,
                    work=lambda: (release.wait(), self.data_manager.log_event(
                        self.Events.APP_START, {"message": "accepted-work"})),
                    finish=lambda _value: None, fail=lambda error: (_ for _ in ()).throw(error),
                ))
                if mode != "stall":
                    threading.Thread(target=lambda: (time.sleep(0.04), release.set()), daemon=True).start()
                self.on_closing()
            elif mode == "raise":
                raise RuntimeError("injected mainloop failure")
            elif mode == "sync_return":
                self.run_tests = False
                lm.messagebox.askokcancel = lambda *_a, **_k: True
                self.on_closing()
            elif mode == "interrupt_cleanup":
                raise KeyboardInterrupt()
            elif mode == "exit_cleanup":
                raise SystemExit(17)

    lm.Label_Match = ProbeApp
    def destroy_root(self):
        self.guard_observations.append(owns_guard())
        self.root.destroy()
    lm.tk.Tk.destroy = destroy_root
    lm.dispatch_product_mode = lambda _args: None
    lm.verify_factory_contract_startup = lambda: None
    lm._first_run_onboarding_enabled = lambda: False
    lm._default_label_match_settings_path = lambda: root / "settings.json"
    lm.prepare_startup_item_catalog = lambda: None
    lm._label_match_startup_trace = lambda *_a, **_k: None
    lm._label_match_explicit_automated_test_mode = lambda: False
    lm._label_match_bind_current_log_source = lambda context, _manager: context
    lm._label_match_run_and_record_session_direct_sync = lambda *_a, **_k: {"status": "PASS"}
    if mode == "stall":
        lm.LABEL_MATCH_APP_CLOSE_TOTAL_TIMEOUT_SECONDS = 0.1
    if mode in {"interrupt_cleanup", "exit_cleanup"} and hasattr(original, "_finish_after_mainloop"):
        finish = original._finish_after_mainloop
        def finish_then_fail(self):
            finish(self)
            raise RuntimeError("injected cleanup failure")
        ProbeApp._finish_after_mainloop = finish_then_fail
    if os.environ.get("LM281_MUTATION") == "skip-cleanup":
        ProbeApp._finish_after_mainloop = lambda self: None
    if os.environ.get("LM281_MUTATION") == "release-guard":
        lm._retain_current_instance_lease = lambda: None
    lm.run_guarded_entrypoint = lambda start, **kwargs: instance.run_guarded_entrypoint(
        start, data_scope=root, acquire=acquire,
        activate=lambda: instance.ActivationResult(found=False, activated=False),
    )

    def rescue():
        main_returned.wait()
        time.sleep(0.4)
        app = apps[0]
        workers = [{"name": t.name, "daemon": t.daemon}
                   for t in threading.enumerate() if t.name == "lm281-ui-lane"]
        (root / f"{mode}-rescue.json").write_text(json.dumps({
            "workers": workers, "guard_held": owns_guard(),
            "interpreter_shutdown_survived": True,
        }), encoding="utf-8")
        release.set()
        app.ui_lane._signal_worker_stop()
        app.data_manager.close(timeout=2)

    threading.Thread(target=rescue, name="lm281-test-observer", daemon=True).start()
    original_exit = None
    try:
        lm.main([])
    except BaseException as error:
        import traceback
        cause = error.__cause__ if isinstance(error, SystemExit) and error.__cause__ else error
        verdict["exception"] = type(cause).__name__
        verdict["exception_frames"] = [(frame.filename, frame.lineno, frame.name)
                                        for frame in traceback.extract_tb(cause.__traceback__)]
        if isinstance(error, SystemExit):
            original_exit = error
    app = apps[0]
    log_files = list(root.rglob("*작업이벤트로그*.csv"))
    close_events = 0
    for log_path in log_files:
        with log_path.open(encoding="utf-8-sig", newline="") as stream:
            close_events += sum(row["event"] == app.Events.APP_CLOSE for row in csv.DictReader(stream))
    pending = app.package_outbox.get_by_set_id("EXIT-PENDING")
    verdict.update({
        "owner": acquired == [True],
        "workers": [{"name": t.name, "daemon": t.daemon}
                    for t in (app.ui_lane.worker_thread, app.data_manager.log_thread) if t.is_alive()],
        "destroyed": app.__dict__.get("_tk_destroy_complete", False),
        "pending_preserved": pending["status"] == "PENDING",
        "pending_identity": [pending[field] for field in ("idempotency_key", "command_fingerprint", "draft_json")],
        "audit_preserved": bool(log_files) and all(p.stat().st_size > 0 for p in log_files),
        "close_events": close_events,
        "guard_held_during_cleanup": all(app.guard_observations) and (
            not app.ui_lane.worker_thread.is_alive() or owns_guard()),
        "current_state_preserved": app.data_manager.load_current_state()["current_set_info"]["raw"] == ["unfinished"],
    })
    (root / f"{mode}.json").write_text(json.dumps(verdict), encoding="utf-8")
    main_returned.set()
    if original_exit is not None:
        raise original_exit


if __name__ == "__main__":
    _probe(sys.argv[2], sys.argv[3])
