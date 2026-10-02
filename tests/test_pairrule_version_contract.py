"""Signed authority, actual stock bytes and build-only source literals."""
import hashlib
import json
from pathlib import Path

import pytest
import auto_update as updates
from tests.test_auto_update import feed_versions, pins, signed_pair, OLD, NEW


@pytest.mark.parametrize("damage", ["no_pair", "extra_pair_field", "from", "to", "forward", "rollback"])
def test_signed_pair_must_bind_both_actual_directions(damage):
    current, candidate = feed_versions()
    pair = signed_pair()
    if damage == "no_pair":
        pair = None
    elif damage == "extra_pair_field":
        pair["feed_rollback"] = True
    else:
        field = {"from": "from_commit", "to": "to_commit",
                 "forward": "forward_contract_sha256", "rollback": "rollback_contract_sha256"}[damage]
        pair[field] = "9" * (40 if field.endswith("commit") else 64)
    manifest = dict(source_commit=NEW, version=candidate["version"],
                    approved_transitions=pins(), auto_update=pair)
    with pytest.raises(ValueError):
        updates.authorize_feed_transition(manifest, current=current, candidate=candidate)


@pytest.mark.parametrize("old,new,allowed", [
    ("v2.0.9", "v2.0.10", True), ("v2.0.10", "v2.0.9", False),
    ("v2.0.24", "v2.0.24.0", False), ("v2.0.24", "v2.0.24.1", True),
    ("v2.0.24", "v2.0.25-hotfix", False), ("v2.0.24", "999", False),
])
def test_numeric_signed_version_order(old, new, allowed):
    current, candidate = feed_versions()
    current["version"], candidate["version"] = old, new
    manifest = dict(source_commit=NEW, version=new, approved_transitions=pins(), auto_update=signed_pair())
    if allowed:
        assert updates.authorize_feed_transition(manifest, current=current, candidate=candidate)[3] == "SIGNED_EXACT_PAIR"
    else:
        with pytest.raises(ValueError):
            updates.authorize_feed_transition(manifest, current=current, candidate=candidate)


@pytest.mark.parametrize("source", [b'APP_VERSION="v2.0.24"\nAPP_VERSION="v2.0.25"',
                                   b'APP_VERSION=get_version()', b'APP_VERSION="2.0.24"'])
def test_literal_format_is_build_consistency(source):
    with pytest.raises(ValueError):
        updates.source_version_literal(source)


@pytest.mark.parametrize("missing_version", [True, False])
def test_stock_version_and_raw_contract_identity(monkeypatch, missing_version):
    monkeypatch.setattr(updates, "CONTRACT_FILES", ("tools/contract.json", "app/fence.py"))
    monkeypatch.setattr(updates, "DATA_API_FILES", ("Label_Match.py",))
    stock = dict(schema="label-match-portable-tree-v1", startup_update_protocol=1, source_commit=OLD)
    if not missing_version:
        stock["version"] = "v2.0.24"
    files = {"portable-manifest.json": json.dumps(stock).encode(),
             "app/Label_Match.py": b'APP_VERSION="v2.0.24"\nraise RuntimeError("never execute")\n',
             "tools/contract.json": b'{"contract":1}\r\n', "app/fence.py": b'PIN="evidence"\n'}
    if missing_version:
        with pytest.raises(ValueError, match="재설치"):
            updates.version_identity(files.__getitem__)
        return
    identity = updates.version_identity(files.__getitem__)
    expected = hashlib.sha256(b"".join((name + "\n").encode() + hashlib.sha256(files[name]).digest()
                                     for name in updates.CONTRACT_FILES)).hexdigest()
    assert identity["contract_sha256"] == expected
    files["tools/contract.json"] = b'{"contract":1}\n'
    assert updates.version_identity(files.__getitem__)["contract_sha256"] != expected


@pytest.mark.parametrize("host,allowed", [("company.test", True), ("127.0.0.1", True),
    ("xn--bcher-kva.test", True), ("bücher.test", False), ("회사.test", False)])
def test_feed_hostname_is_ascii(host, allowed):
    if allowed:
        updates.policy_from_settings({"feed_url": "https://" + host + "/feed"})
    else:
        with pytest.raises(ValueError, match="ASCII"):
            updates.policy_from_settings({"feed_url": "https://" + host + "/feed"})


@pytest.mark.parametrize("url", ["https://@company.test/feed", "https://company.test/feed#fragment"])
def test_even_empty_userinfo_and_fragment_are_rejected(url):
    with pytest.raises(ValueError):
        updates.policy_from_settings({"feed_url": url})


def test_stock_builder_records_and_compares_version():
    # The prohibited stock build is not run; inspect its literal/copy boundary.
    import ast
    source = (Path(__file__).resolve().parents[1] / "tools/build_portable_release_candidate.py").read_text(encoding="utf-8-sig")
    tree = ast.parse(source)
    build = next(node for node in tree.body if isinstance(node, ast.FunctionDef) and node.name == "build")
    calls = [node for node in ast.walk(build) if isinstance(node, ast.Call)
             and isinstance(node.func, ast.Name) and node.func.id == "source_version_literal"]
    assert len(calls) == 2
    assert '"version": version' in source


@pytest.mark.parametrize("damage", [None, "signature", "pointer", "path", "pin", "failed"])
def test_system_prepared_replay_reauthenticates_before_switch(tmp_path, monkeypatch, damage):
    control = tmp_path / "update-control"; control.mkdir()
    key = tmp_path / "current/app/update-manifest-key-config.json"; key.parent.mkdir(parents=True)
    key.write_text(json.dumps(dict(manifest_public_key="synthetic-input")))
    (control / "active.json").write_text(json.dumps(dict(sha=NEW if damage == "pointer" else OLD)))
    current, candidate = feed_versions()
    state = dict(previous_sha=OLD, candidate_sha=NEW, packet_sha256="3" * 64,
        prepared_packet_root=str(control / ("wrong" if damage == "path" else "packet-" + "a" * 32)),
        forward_pin=signed_pair()["forward_contract_sha256"], rollback_pin=signed_pair()["rollback_contract_sha256"],
        failed_shas=[NEW] if damage == "failed" else [])
    if damage == "pin":
        state["rollback_pin"] = "9" * 64
    verified = []
    monkeypatch.setattr(updates, "fixed_helper", lambda _root, mode, **kw: verified.append(mode) or "")
    monkeypatch.setattr(updates, "version_manifest", lambda root: current if root.name == OLD else candidate)
    def verify(*args, **kwargs):
        if damage == "signature":
            raise ValueError("signature rejected")
        return dict(source_commit=NEW, version=candidate["version"], approved_transitions=pins(), auto_update=signed_pair()), dict(sha256="3" * 64)
    monkeypatch.setattr(updates, "verify_prepared_packet", verify)
    if damage:
        with pytest.raises(ValueError):
            updates.authorize_prepared_replay(control, {"channel": "stable"}, state)
    else:
        updates.authorize_prepared_replay(control, {"channel": "stable"}, state)
        assert verified == ["Verify", "Verify"]
    assert updates.read_json(control / "active.json")["sha"] == (NEW if damage == "pointer" else OLD)
