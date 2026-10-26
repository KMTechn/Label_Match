import pytest

import auto_update as updates
from writer_session_fence import WriterFencedError


@pytest.mark.parametrize("arguments", [[], ["--label-match-user-relay"]], ids=["gui", "relay"])
@pytest.mark.parametrize("error", [
    WriterFencedError("WRITER_GATE_TIMEOUT", "writer admission timed out"),
    OSError("support unavailable"), ValueError("support invalid"), TypeError("support invalid"),
], ids=["fenced", "os", "value", "type"])
def test_support_record_failure_keeps_start_current_launching(monkeypatch, tmp_path, arguments, error):
    launched, emitted = [], []
    deadlines = []
    def launch(*args, relay_deadline, **kwargs):
        deadlines.append(relay_deadline)
        launched.append((args, kwargs))
        return 713
    def ready(*args, deadline, **kwargs):
        assert deadline is deadlines[0]
        return True
    def fail_write(path, report):
        assert path == tmp_path / "startup-support.json"
        raise error
    monkeypatch.setattr(updates, "write_json_durable", fail_write)
    monkeypatch.setattr(updates, "emit_support_record", lambda report, **kw: emitted.append((report, kw)))
    monkeypatch.setattr(updates, "now_utc", lambda: "2026-10-09T04:00:00+00:00")
    monkeypatch.setattr(updates, "launch_version", launch)
    monkeypatch.setattr(updates, "wait_current_relay", ready)
    control = tmp_path / "control"
    assert updates.start_current(control, {"staging_root": str(tmp_path)}, None,
                                 {"sha": "1" * 40}, arguments, code="UPDATE_APPLY_NO_PROGRESS") == 0
    assert emitted == [({"reason_code": "REPORT_UNAVAILABLE"}, {"stage": "update_apply"})]
    assert launched == [((control, "1" * 40, arguments), {"deferred_at": "2026-10-09T04:00:00+00:00"})]


def test_start_current_does_not_swallow_unrelated_runtime_errors(monkeypatch, tmp_path):
    error = RuntimeError("unrelated support failure")
    def fail_write(*args):
        raise error
    monkeypatch.setattr(updates, "write_json_durable", fail_write)
    monkeypatch.setattr(updates, "launch_version", lambda *a, **kw: pytest.fail("unsafe continuation"))
    with pytest.raises(RuntimeError) as caught:
        updates.start_current(tmp_path, {"staging_root": str(tmp_path)}, None,
                              {"sha": "1" * 40}, [], code="UPDATE_APPLY_NO_PROGRESS")
    assert caught.value is error


def test_fenced_support_record_still_requires_ordinary_recovery(monkeypatch, tmp_path):
    def fail_write(*args):
        raise WriterFencedError("WRITER_GATE_TIMEOUT", "writer admission timed out")
    error = ValueError("ordinary recovery refused")
    def refuse_recovery(*args):
        raise error
    monkeypatch.setattr(updates, "write_json_durable", fail_write)
    monkeypatch.setattr(updates, "emit_support_record", lambda *a, **kw: None)
    monkeypatch.setattr(updates, "assert_ordinary_recovery", refuse_recovery)
    monkeypatch.setattr(updates, "launch_version", lambda *a, **kw: pytest.fail("unsafe recovery"))
    guidance = []
    monkeypatch.setattr(updates, "emit_progress_guidance", lambda: guidance.append(True))
    assert updates.start_current(tmp_path, {"staging_root": str(tmp_path)}, {"phase": "FAILED"},
                                 {"sha": "1" * 40}, [], code="UPDATE_APPLY_NO_PROGRESS") == 20
    assert guidance == [True]
