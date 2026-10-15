"""Machine activation consumer: real scoped root, journal and delegation checks."""
import ctypes
from ctypes import wintypes
from datetime import datetime, timedelta, timezone
import json
import os
from pathlib import Path

import pytest

import auto_update as update
import writer_session_fence as fence
from tests.test_supported_lifecycle import healthy, CANDIDATE
from tests.test_writer_session_fence import _payload, _write_active


def write(path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value), encoding="utf-8")


def current_native_birth():
    kernel=ctypes.WinDLL("kernel32",use_last_error=True)
    kernel.GetCurrentProcess.restype=wintypes.HANDLE
    kernel.GetProcessTimes.argtypes=[wintypes.HANDLE,*([ctypes.POINTER(wintypes.FILETIME)]*4)]
    created,exited,system,user=(wintypes.FILETIME() for _ in range(4))
    assert kernel.GetProcessTimes(kernel.GetCurrentProcess(),ctypes.byref(created),ctypes.byref(exited),ctypes.byref(system),ctypes.byref(user))
    return str((created.dwHighDateTime<<32)|created.dwLowDateTime)


@pytest.fixture
def machine(tmp_path,monkeypatch):
    managed=tmp_path/"product"
    fixed,current,control,verified=healthy(managed,monkeypatch)
    current_sha=update.read_json(control/"active.json")["sha"]
    candidate=managed/"versions"/CANDIDATE
    (candidate/"app").mkdir(parents=True)
    write(candidate/"portable-manifest.json",dict(source_commit=CANDIDATE))
    for name in ("update-manifest-key-config.json","auto-update-settings.json"):
        (candidate/"app"/name).write_bytes((fixed/"app"/name).read_bytes())
    machine_root=tmp_path/"ProgramData"/"install-audit"/"machine-writer"
    marker="synthetic authority fixture placeholder".ljust(64,".")
    sources=["canonical_placement","auto_update_supported_lifecycle_transition",
        "auto_update_persist_switch","auto_update_write_json_durable","auto_update_fixed_helper"]
    active=_payload(delegated_sources=sources,token=marker)
    _write_active(machine_root,active)
    run="20261015T010203004Z-"+"a"*32
    owner=managed/(".installer-owner-"+active["session_id"]+"-"+"a"*32+".json")
    record=dict(schema="label-match-installer-owner-v1",run_id=run,install_root=str(managed/"current"),
        control_root=str(machine_root),owner_process_id=os.getpid(),owner_process_started_at_filetime=current_native_birth(),
        **{name:active[name] for name in fence._INSTALLER_TUPLE_FIELDS})
    write(owner,record)
    monkeypatch.setattr(fence,"_installer_owner_root",lambda:managed)
    protected=[]
    # The factory filesystem is not a production protected code directory.
    # Keep all authority/owner/delegation logic; replace only its native ACL facet.
    monkeypatch.setattr(fence,"_assert_protected_owner_path",lambda path, **kwargs:protected.append(path))
    monkeypatch.setattr(fence,"_named_mutex_held_by_other",lambda name:True)
    monkeypatch.setenv(fence.DELEGATION_TOKEN_ENV,marker)
    for env,key in ((fence.DELEGATION_SESSION_ENV,"session_id"),(fence.DELEGATION_ATTEMPT_ENV,"attempt_id"),
            (fence.DELEGATION_TRANSACTION_ENV,"replacement_transaction_id")):
        monkeypatch.setenv(env,active[key])
    monkeypatch.setattr(update,"process_security",lambda pid:dict(elevated=True))
    monkeypatch.setattr(update,"_assert_canonical_lifecycle_root",lambda root:None)
    return dict(managed=managed,fixed=fixed,current=current,control=control,current_sha=current_sha,
        candidate=candidate,root=machine_root,owner=owner,record=record,active=active,protected=protected)


@pytest.mark.parametrize("same",[True,False])
def test_actual_machine_lifecycle_activation_uses_explicit_authority(machine,monkeypatch,capsys,same):
    m=machine
    selected=m["current_sha"] if same else CANDIDATE
    before={p:p.read_bytes() for p in m["control"].iterdir() if p.is_file()}
    default=fence.canonical_control_root()
    assert default!=m["root"]
    original=update.supported_lifecycle_transition
    roots=[]
    def transition(*args,**kwargs):
        roots.append(fence.canonical_control_root())
        return original(*args,**kwargs)
    monkeypatch.setattr(update,"supported_lifecycle_transition",transition)
    result=update.lifecycle_context_main(["--install-root",str(m["managed"]),"--expected-from-commit",m["current_sha"],
        "--activate-supported-commit",selected,"--machine-installer-authority",
        "--installer-authority-root",str(m["root"]),"--installer-owner-record",str(m["owner"])])
    assert result==0 and roots==[m["root"]]
    assert fence.canonical_control_root()==default
    assert m["owner"] in m["protected"] and m["root"]/fence.ACTIVE_FILENAME in m["protected"]
    response=json.loads(capsys.readouterr().out)
    assert response["from_commit"]==selected
    if same:
        assert response["code_replaced"] is False
        assert all(p.read_bytes()==value for p,value in before.items())
    else:
        assert update.read_json(m["control"]/"active.json")["sha"]==CANDIDATE
        assert update.read_json(m["control"]/"transition.json")["phase"]=="DEFERRED"


@pytest.mark.parametrize("damage",["missing","root","tuple","expired","source","owner","birth","mutex","acl","reparse","settings"])
def test_machine_authority_failure_cannot_fall_back_to_user_root(machine,monkeypatch,capsys,damage):
    m=machine
    before={p:p.read_bytes() for p in m["control"].iterdir() if p.is_file()}
    root=m["root"]
    arguments=["--install-root",str(m["managed"]),"--expected-from-commit",m["current_sha"],
        "--activate-supported-commit",CANDIDATE,"--machine-installer-authority"]
    if damage=="root":root=m["managed"]/"wrong-root"
    if damage=="tuple":m["record"]["attempt_id"]="f"*32;write(m["owner"],m["record"])
    if damage=="expired":
        m["active"]["delegation_expires_at_utc"]=(datetime.now(timezone.utc)-timedelta(seconds=1)).isoformat()
        write(m["root"]/fence.ACTIVE_FILENAME,m["active"])
    if damage=="source":m["active"]["delegated_sources"]=["auto_update_fixed_helper"];write(m["root"]/fence.ACTIVE_FILENAME,m["active"])
    if damage=="owner":m["record"]["owner_process_id"]=0;write(m["owner"],m["record"])
    if damage=="birth":m["record"]["owner_process_started_at_filetime"]="1";write(m["owner"],m["record"])
    if damage=="mutex":monkeypatch.setattr(fence,"_named_mutex_held_by_other",lambda name:False)
    if damage=="acl":monkeypatch.setattr(fence,"_assert_protected_owner_path",lambda path, **kwargs:(_ for _ in ()).throw(PermissionError()))
    if damage=="reparse":monkeypatch.setattr(fence,"_assert_no_reparse_components",lambda path:(_ for _ in ()).throw(fence.WriterFenceError("FENCE_REPARSE","fixture")))
    if damage=="settings":(m["candidate"]/"app/auto-update-settings.json").write_bytes(b"changed")
    if damage!="missing":arguments += ["--installer-authority-root",str(root),"--installer-owner-record",str(m["owner"])]
    assert update.lifecycle_context_main(arguments)==1
    assert all(p.read_bytes()==value for p,value in before.items())
    captured=capsys.readouterr()
    assert captured.out=="" and "LABEL_MATCH_LIFECYCLE_FAILURE=" in captured.err
    assert getattr(fence._WRITER_LOCAL,"installer_authority_root",None) is None


def test_lifecycle_evidence_failure_preserves_native_failure_record(machine,monkeypatch,capsys):
    import failure_redaction
    monkeypatch.setattr(failure_redaction,"emit_support_failure",lambda *a,**k:(_ for _ in ()).throw(OSError()))
    assert update.lifecycle_context_main(["--install-root",str(machine["managed"]),"--machine-installer-authority"])==1
    output=capsys.readouterr()
    record=json.loads(output.err.split("=",1)[1])
    assert output.out=="" and record["exit_code"]==1 and record["report_status"]=="REPORT_UNAVAILABLE"
