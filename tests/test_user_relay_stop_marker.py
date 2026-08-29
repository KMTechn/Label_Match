import json
from pathlib import Path
from types import SimpleNamespace

import pytest

import user_relay
from user_relay_stop_marker import (
    MAX_SUCCESSOR_DEPTH,
    STOP_MARKER_V1,
    StopMarkerLineageError,
    build_successor_marker,
    canonical_marker_bytes,
    read_stop_marker,
    validate_marker_successor_lineage,
)


def _write_marker(path: Path, value: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(canonical_marker_bytes(value))


def _root_marker(request_id: str = "a" * 32) -> dict:
    return {
        "schema_version": STOP_MARKER_V1,
        "request_id": request_id,
        "requested_at": "2026-08-29T10:00:00+00:00",
    }


def test_successor_embeds_and_verifies_the_exact_receipt_marker(tmp_path):
    marker_path = tmp_path / "stop.json"
    root = _root_marker()
    _write_marker(marker_path, root)
    _value, _raw, root_hash = read_stop_marker(marker_path)

    successor = build_successor_marker(
        marker_path,
        request_id="b" * 32,
        requested_at="2026-08-29T10:01:00+00:00",
    )
    _write_marker(marker_path, successor)

    proof = validate_marker_successor_lineage(
        marker_path,
        anchor_request_id=root["request_id"],
        anchor_sha256=root_hash,
    )
    assert proof["status"] == "SUCCESSOR"
    assert proof["successor_hops"] == 1
    assert successor["predecessor_marker"] == root
    assert successor["predecessor_sha256"] == root_hash


def test_unrelated_or_broken_marker_lineage_is_rejected(tmp_path):
    marker_path = tmp_path / "stop.json"
    root = _root_marker()
    _write_marker(marker_path, root)
    _value, _raw, root_hash = read_stop_marker(marker_path)

    _write_marker(marker_path, _root_marker("c" * 32))
    with pytest.raises(StopMarkerLineageError, match="not a verified successor"):
        validate_marker_successor_lineage(
            marker_path,
            anchor_request_id=root["request_id"],
            anchor_sha256=root_hash,
        )

    _write_marker(marker_path, root)
    broken = build_successor_marker(
        marker_path,
        request_id="d" * 32,
        requested_at="2026-08-29T10:02:00+00:00",
    )
    broken["predecessor_marker"]["request_id"] = "e" * 32
    _write_marker(marker_path, broken)
    with pytest.raises(StopMarkerLineageError, match="predecessor hash differs"):
        validate_marker_successor_lineage(
            marker_path,
            anchor_request_id=root["request_id"],
            anchor_sha256=root_hash,
        )


def test_successor_lineage_is_bounded_and_never_silently_truncated(tmp_path):
    marker_path = tmp_path / "stop.json"
    _write_marker(marker_path, _root_marker())
    for index in range(MAX_SUCCESSOR_DEPTH):
        successor = build_successor_marker(
            marker_path,
            request_id=f"{index + 1:032x}",
            requested_at=f"2026-08-29T10:0{index + 1}:00+00:00",
        )
        _write_marker(marker_path, successor)

    with pytest.raises(StopMarkerLineageError, match="fresh receipt"):
        build_successor_marker(
            marker_path,
            request_id="f" * 32,
            requested_at="2026-08-29T10:09:00+00:00",
        )


def test_stop_request_records_a_successor_and_release_is_exact_cas(tmp_path):
    direct_root = tmp_path / "direct"
    marker_path = user_relay.user_relay_stop_path(direct_root)
    root = _root_marker()
    _write_marker(marker_path, root)
    closed = []

    report = user_relay.request_user_relay_stop(
        direct_root,
        timeout_seconds=0,
        lease_factory=lambda _key: SimpleNamespace(
            close=lambda: closed.append("relay")
        ),
        marker_lease_factory=lambda _key: SimpleNamespace(
            close=lambda: closed.append("marker")
        ),
    )
    successor = json.loads(marker_path.read_text(encoding="utf-8"))
    assert report["status"] == "ABSENT"
    assert report["stop_marker_schema"] == "label-match-user-relay-stop-v2"
    assert successor["predecessor_marker"] == root
    assert closed == ["marker", "relay"]

    with pytest.raises(user_relay.UserRelayError, match="changed after"):
        user_relay.release_user_relay_stop_marker(
            direct_root,
            expected_request_id=report["request_id"],
            expected_sha256="0" * 64,
            marker_lease_factory=lambda _key: SimpleNamespace(close=lambda: None),
        )
    assert marker_path.is_file()

    released = user_relay.release_user_relay_stop_marker(
        direct_root,
        expected_request_id=report["request_id"],
        expected_sha256=report["stop_request_sha256"],
        marker_lease_factory=lambda _key: SimpleNamespace(close=lambda: None),
    )
    assert released["status"] == "RELEASED"
    assert not marker_path.exists()


def test_successor_marker_still_blocks_the_path_only_relay_gate(
    monkeypatch, tmp_path
):
    app_root = (tmp_path / "app").resolve()
    direct_root = (tmp_path / "direct").resolve()
    data_root = (tmp_path / "data").resolve()
    profile_path = tmp_path / "profile.json"
    app_root.mkdir()
    _write_marker(user_relay.user_relay_stop_path(direct_root), _root_marker())
    marker_path = user_relay.user_relay_stop_path(direct_root)
    successor = build_successor_marker(
        marker_path,
        request_id="b" * 32,
        requested_at="2026-08-29T10:01:00+00:00",
    )
    _write_marker(marker_path, successor)

    monkeypatch.setattr(
        "current_user_onboarding.resolve_current_user_onboarding_paths",
        lambda _app_root: SimpleNamespace(
            direct_sync_root=direct_root,
            data_root=data_root,
            logistics_profile_path=profile_path,
        ),
    )
    monkeypatch.setattr(
        "current_user_onboarding.apply_current_user_runtime_environment",
        lambda _paths: None,
    )
    monkeypatch.setattr(
        "logistics_runtime_profile.load_logistics_runtime_profile",
        lambda **_kwargs: SimpleNamespace(tls_ca_bundle_path=""),
    )
    monkeypatch.setattr(
        user_relay,
        "_acquire_relay_lease",
        lambda _key: (_ for _ in ()).throw(
            AssertionError("relay mutex must not be reached while marker exists")
        ),
    )

    assert (
        user_relay.main(
            [
                "--app-root",
                str(app_root),
                "--direct-sync-root",
                str(direct_root),
                "--scan-source-dir",
                str(data_root),
                "--once",
            ]
        )
        == 0
    )
