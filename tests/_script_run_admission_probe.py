"""Private script-run fixture: stop before the first real writer admission.

The copied fence imports this helper after defining its unmodified product code.
No writer admission, task registration, GUI, child launcher or socket is allowed.
"""
import atexit
import json
from pathlib import Path
import sys


class FirstAdmission(BaseException):
    pass


def install(app):
    app = Path(app).resolve()
    managed = app.parent.parent
    evidence = managed / "probe-evidence.json"
    result = {"first_admission": None, "main_entered": False, "definition_rejections": [],
              "script_sinks": {}, "socket_requests": 0, "child_requests": 0,
              "staging_rejections": [], "ordinary_context_fixture": False}
    atexit.register(lambda: evidence.write_text(json.dumps(result, indent=2), encoding="utf-8"))

    def audit(event, args):
        if event in {"socket.connect", "socket.bind", "subprocess.Popen", "os.system"}:
            field = "socket_requests" if event.startswith("socket.") else "child_requests"
            result[field] += 1
            raise AssertionError("external execution forbidden in script-run fixture")

    def trace(frame, event, arg):
        filename = frame.f_code.co_filename
        if event == "exception" and frame.f_code.co_name == "decorate":
            error = arg[1]
            if getattr(error, "code", "") == "WRITER_PROBE_ONLY_SINK_NOT_ALLOWED":
                result["definition_rejections"].append(error.code)
        if event == "return" and frame.f_code.co_name == "decorate" and arg is not None:
            function = frame.f_locals.get("function")
            if function is not None and function.__module__ == "__main__":
                result["script_sinks"][function.__qualname__] = frame.f_locals["sink_id"]
        if event == "call" and frame.f_code.co_name == "main" and Path(filename).resolve() == app / "auto_update.py":
            result["main_entered"] = True
            namespace = frame.f_globals
            if "--bootstrap" in sys.argv:
                control = managed / "update-control"
                control.mkdir(exist_ok=True)
                settings = dict(feed_url="", channel="stable", apply_during_work=False, profile_auth=False)
                raw = namespace["canonical_startup_settings"](settings)
                (app / "auto-update-settings.json").write_bytes(raw)
                policy = dict(settings, operator_sid=namespace["current_process_sid"](),
                              staging_root=str(control / "stage"),
                              settings_sha256=namespace["hashlib"].sha256(raw).hexdigest())
                (control / "policy.json").write_text(json.dumps(policy), encoding="utf-8")
                (control / "active.json").write_text(json.dumps({"sha": "1" * 40}), encoding="utf-8")
                namespace["ordinary_user_context"] = lambda _sid=None: True
                result["ordinary_context_fixture"] = True
            if "--print-logon-task" in sys.argv:
                import writer_session_fence as fence
                fence._WRITER_LOCAL.depth = 1
                try:
                    for name in ("write_download_bytes", "write_download_chunk", "stage_prepared_copies", "_run_download_attempt"):
                        try:
                            namespace[name]()
                        except fence.WriterFencedError as error:
                            result["staging_rejections"].append(error.code)
                finally:
                    fence._WRITER_LOCAL.depth = 0
        if event == "call" and frame.f_code.co_name == "writer_admission" and Path(filename).resolve() == app / "writer_session_fence.py":
            import writer_session_fence as fence
            result["first_admission"] = {"source": frame.f_locals["source"],
                                         "depth": getattr(fence._WRITER_LOCAL, "depth", 0),
                                         "acquired": False}
            raise FirstAdmission()
        return trace

    sys.addaudithook(audit)
    sys.settrace(trace)
