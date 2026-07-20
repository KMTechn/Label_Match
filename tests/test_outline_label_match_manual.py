from __future__ import annotations

import copy
import hashlib
import json
import shutil
import subprocess
from pathlib import Path
from types import SimpleNamespace

import pytest
from PIL import Image

from tools import capture_label_match_manual_20260715 as capture
from tools import publish_outline_user_manual as publisher


LIVE_ASSETS_AVAILABLE = (publisher.DEFAULT_ASSET_DIR / "manifest.json").is_file()


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


@pytest.mark.skipif(not LIVE_ASSETS_AVAILABLE, reason="fresh DISPLAY2 manual packet not generated yet")
def test_outline_manual_uses_current_five_step_contract_and_pass_assets():
    manual_text = publisher.DEFAULT_MANUAL.read_text(encoding="utf-8")

    assert "Label_Match v2.0.36" in manual_text
    assert "중앙 하단" in manual_text
    assert "실제 스캔" in manual_text
    assert "QA 제품 샘플 3" in manual_text
    assert "N+5회" in manual_text
    assert "sealed 이적 QR" in manual_text
    assert "N=60" in manual_text
    assert "총 65회" in manual_text
    assert "ACK/COMMITTED/readback" in manual_text
    assert "다시 스캔하거나" in manual_text
    assert "제품4" not in manual_text
    assert "6/6" not in manual_text
    assert "label_match_user_manual_20260715" not in manual_text
    assert publisher.ASSET_PREFIX in manual_text

    _, report = publisher._build_outline_text(
        publisher.DEFAULT_MANUAL,
        publisher.DEFAULT_ASSET_DIR,
    )

    assert report["markdown_image_refs"] == 17
    assert report["unique_image_refs"] == 17
    assert report["missing_images"] == []
    assert report["capture_manifest_status"] == "PASS"
    assert report["capture_manifest_image_count"] == 17
    assert report["capture_manifest_image_contract_ok"] is True
    assert report["capture_manifest_verified_image_files"] == 34
    assert report["capture_manifest_source_commit"] == publisher.EXPECTED_SOURCE_COMMIT
    assert report["capture_manifest_source_tree"] == publisher.EXPECTED_SOURCE_TREE
    assert report["capture_manifest_display_device"] == publisher.EXPECTED_DISPLAY_DEVICE
    assert report["capture_manifest_display_is_primary"] is False
    assert report["capture_manifest_central_scan_list_status"] == "PASS"
    assert report["capture_manifest_raw_image_uniqueness_status"] == "PASS"
    assert report["capture_manifest_workflow_state_status"] == "PASS"
    assert report["capture_manifest_privacy_status"] == "PASS"
    assert report["capture_manifest_max_near_black_increase_ratio"] <= 0.005


@pytest.mark.skipif(not LIVE_ASSETS_AVAILABLE, reason="fresh DISPLAY2 manual packet not generated yet")
def test_capture_manifest_hashes_dimensions_foreground_and_central_rows_match_files():
    asset_dir = publisher.DEFAULT_ASSET_DIR
    manifest = json.loads((asset_dir / "manifest.json").read_text(encoding="utf-8"))

    assert manifest["status"] == "PASS"
    assert manifest["image_contract_ok"] is True
    assert manifest["app_version"] == "v2.0.36"
    assert manifest["source_identity"]["commit"] == publisher.EXPECTED_SOURCE_COMMIT
    assert manifest["source_identity"]["tree"] == publisher.EXPECTED_SOURCE_TREE
    assert manifest["monitor_target"]["device"] == r"\\.\DISPLAY2"
    assert manifest["monitor_target"]["is_primary"] is False
    assert len(manifest["images"]) == 17
    assert tuple(manifest["expected_names"]) == publisher.EXPECTED_IMAGE_NAMES
    assert manifest["raw_image_uniqueness_contract"]["status"] == "PASS"
    assert manifest["workflow_state_contract"]["status"] == "PASS"
    assert all(
        checkpoint["status"] == "PASS"
        for checkpoint in manifest["workflow_state_contract"]["checkpoints"]
    )
    assert manifest["expected_names"] == manifest["actual_names"]

    for entry in manifest["images"]:
        raw_path = asset_dir / entry["raw_path"]
        annotated_path = asset_dir / entry["annotated_path"]
        assert raw_path.exists()
        assert annotated_path.exists()
        assert _sha256(raw_path) == entry["raw_sha256"]
        assert _sha256(annotated_path) == entry["annotated_sha256"]
        assert entry["raw_sha256"] != entry["annotated_sha256"]
        assert entry["monitor_device"] == r"\\.\DISPLAY2"
        assert entry["monitor_is_primary"] is False
        assert entry["target_is_foreground"] is True
        assert entry["target_contained_in_monitor"] is True
        assert entry["app_root_matches_work_area"] is True

        with Image.open(raw_path) as raw_image, Image.open(annotated_path) as annotated_image:
            assert raw_image.format == "PNG"
            assert annotated_image.format == "PNG"
            assert raw_image.size == publisher.EXPECTED_CAPTURE_SIZE
            assert annotated_image.size == publisher.EXPECTED_CAPTURE_SIZE

        observed = publisher._pixel_comparison(raw_path, annotated_path)
        assert observed["changed_pixel_count"] > 0
        assert observed["near_black_increase_ratio"] <= 0.005
        assert observed["changed_pixel_count"] == entry["pixel_qa"]["changed_pixel_count"]
        assert observed["near_black_increase_ratio"] == pytest.approx(
            entry["pixel_qa"]["near_black_increase_ratio"], abs=1e-12
        )

        central = entry.get("central_scan_list")
        if entry["name"] in publisher.EXPECTED_MAIN_SCREEN_NAMES:
            assert central["location"] == "central_lower"
            assert central["mapped"] is True
            assert central["viewable"] is True
            assert central["within_center_pane"] is True
            assert central["below_scan_entry"] is True
            assert central["positive_geometry"] is True
            assert central["final_row_visible"] is True
            assert central["rows_exact_match"] is True
            assert central["observed_rows"] == central["expected_rows"]
        else:
            assert central is None
    assert len({entry["raw_sha256"] for entry in manifest["images"]}) == len(
        manifest["images"]
    )


@pytest.mark.parametrize(
    "url",
    [
        "http://wiki.kmtecherp.com",
        "https://wiki.kmtecherp.com.evil.test",
        "https://user@wiki.kmtecherp.com",
        "//wiki.kmtecherp.com",
        "https://wiki.kmtecherp.com/path",
        "https://wiki.kmtecherp.com?next=evil",
        "https://wiki.kmtecherp.com#fragment",
        "https://wiki.kmtecherp.com:444",
        "https://wiki.kmtecherp.com\\@evil.test",
    ],
)
def test_outline_base_url_rejects_untrusted_or_ambiguous_origins(url):
    with pytest.raises(ValueError):
        publisher._validate_outline_base_url(url)


def test_upload_url_requires_exact_https_origin():
    trusted = frozenset({"https://wiki.kmtecherp.com:443", "https://bucket.example:443"})
    assert publisher._resolve_upload_url(
        "https://wiki.kmtecherp.com",
        "/upload?signature=abc",
        trusted,
    ) == "https://wiki.kmtecherp.com/upload?signature=abc"
    assert publisher._resolve_upload_url(
        "https://wiki.kmtecherp.com",
        "https://bucket.example/object?signature=abc",
        trusted,
    ) == "https://bucket.example/object?signature=abc"
    for bad in (
        "//evil.test/upload",
        "http://bucket.example/upload",
        "https://bucket.example.evil.test/upload",
        "https://user@bucket.example/upload",
        "https://bucket.example:444/upload",
        "https://evil.test/upload",
        "relative/upload",
        "https://bucket.example/upload#fragment",
    ):
        with pytest.raises(ValueError):
            publisher._resolve_upload_url("https://wiki.kmtecherp.com", bad, trusted)


@pytest.mark.parametrize(
    "value",
    [
        "https://wiki.kmtecherp.com/api/attachments.redirect?id=abc",
        "//wiki.kmtecherp.com/api/attachments.redirect?id=abc",
        "/api/attachments.redirect",
        "/api/attachments.redirect?id=",
        "/api/attachments.redirect?id=a&id=b",
        "/api/attachments.redirect?id=a&other=b",
        "/api/attachments.redirect/extra?id=a",
        "/api/attachments.redirect?id=a#fragment",
    ],
)
def test_attachment_url_requires_one_relative_outline_redirect_id(value):
    with pytest.raises(ValueError):
        publisher._validate_attachment_url(value)


class _FakeResponse:
    def __init__(self, status_code: int, payload: dict | None = None, text: str = "") -> None:
        self.status_code = status_code
        self._payload = payload or {}
        self.text = text

    def json(self):
        return self._payload


class _FakeSession:
    def __init__(self, *responses: _FakeResponse) -> None:
        self.responses = list(responses)
        self.calls: list[tuple[tuple, dict]] = []

    def post(self, *args, **kwargs):
        self.calls.append((args, kwargs))
        if not self.responses:
            raise AssertionError("unexpected network call")
        return self.responses.pop(0)


def _created_attachment(upload_url: str = "/upload?sig=abc") -> dict:
    return {
        "ok": True,
        "data": {
            "uploadUrl": upload_url,
            "attachment": {"url": "/api/attachments.redirect?id=attachment-1"},
            "form": {"key": "object-key"},
        },
    }


def test_upload_uses_separate_no_auth_session_and_disables_redirects(tmp_path):
    image_path = tmp_path / "manual.png"
    Image.new("RGB", (2, 2), "white").save(image_path)
    client = publisher.OutlineClient("https://wiki.kmtecherp.com", "secret-token")
    client.api_session = _FakeSession(_FakeResponse(200, _created_attachment()))
    client.upload_session = _FakeSession(_FakeResponse(204))

    attachment_url = client.upload_image("doc-1", image_path)

    assert attachment_url == "/api/attachments.redirect?id=attachment-1"
    _api_args, api_kwargs = client.api_session.calls[0]
    assert api_kwargs["headers"]["Authorization"] == "Bearer secret-token"
    assert api_kwargs["allow_redirects"] is False
    assert isinstance(api_kwargs["auth"], publisher._NoAuth)
    upload_args, upload_kwargs = client.upload_session.calls[0]
    assert upload_args[0] == "https://wiki.kmtecherp.com/upload?sig=abc"
    assert "Authorization" not in upload_kwargs["headers"]
    assert upload_kwargs["allow_redirects"] is False
    assert isinstance(upload_kwargs["auth"], publisher._NoAuth)


def test_exact_allowlisted_cross_origin_upload_never_receives_outline_token(tmp_path):
    image_path = tmp_path / "manual.png"
    Image.new("RGB", (2, 2), "white").save(image_path)
    client = publisher.OutlineClient(
        "https://wiki.kmtecherp.com",
        "secret-token",
        trusted_upload_origins=["https://bucket.example"],
    )
    client.api_session = _FakeSession(
        _FakeResponse(200, _created_attachment("https://bucket.example/object?sig=abc"))
    )
    client.upload_session = _FakeSession(_FakeResponse(201))

    client.upload_image("doc-1", image_path)

    upload_args, upload_kwargs = client.upload_session.calls[0]
    assert upload_args[0] == "https://bucket.example/object?sig=abc"
    assert "Authorization" not in upload_kwargs["headers"]


def test_api_and_upload_redirects_fail_closed(tmp_path):
    client = publisher.OutlineClient("https://wiki.kmtecherp.com", "secret-token")
    client.api_session = _FakeSession(_FakeResponse(302, text="redirect"))
    with pytest.raises(RuntimeError, match="HTTP 302"):
        client.api("attachments.create", {})

    image_path = tmp_path / "manual.png"
    Image.new("RGB", (2, 2), "white").save(image_path)
    client.api_session = _FakeSession(_FakeResponse(200, _created_attachment()))
    client.upload_session = _FakeSession(_FakeResponse(307, text="redirect"))
    with pytest.raises(RuntimeError, match="HTTP 307"):
        client.upload_image("doc-1", image_path)


def test_untrusted_absolute_upload_is_rejected_before_file_session_call(tmp_path):
    image_path = tmp_path / "manual.png"
    Image.new("RGB", (2, 2), "white").save(image_path)
    client = publisher.OutlineClient("https://wiki.kmtecherp.com", "secret-token")
    client.api_session = _FakeSession(
        _FakeResponse(200, _created_attachment("https://evil.test/upload"))
    )
    client.upload_session = _FakeSession()
    with pytest.raises(ValueError, match="not explicitly trusted"):
        client.upload_image("doc-1", image_path)
    assert client.upload_session.calls == []


class _NamedWidget:
    def __init__(self, name: str, rect: list[int]) -> None:
        self.name = name
        self.rect = rect

    def __str__(self) -> str:
        return self.name


class _FakeTree(_NamedWidget):
    def __init__(self, name: str, rect: list[int], rows: list[dict]) -> None:
        super().__init__(name, rect)
        self.rows = rows

    def get_children(self, _parent=""):
        return tuple(row["iid"] for row in self.rows)

    def item(self, iid, option):
        row = next(row for row in self.rows if row["iid"] == iid)
        return tuple(row[option])

    def winfo_ismapped(self):
        return True

    def winfo_viewable(self):
        return True

    def winfo_height(self):
        return self.rect[3] - self.rect[1]

    def bbox(self, iid):
        index = [row["iid"] for row in self.rows].index(iid)
        return (0, 8 + index * 24, self.rect[2] - self.rect[0], 22)


class _FakeNotebook:
    def __init__(self, selected: str) -> None:
        self.selected = selected

    def select(self):
        return self.selected


def _fake_central_app(rows: list[dict]):
    qa_frame = _NamedWidget("qa-frame", [120, 220, 820, 620])
    exact_frame = _NamedWidget("exact-frame", [120, 220, 820, 620])
    qa_tree = _FakeTree("qa-tree", [140, 300, 800, 470], rows)
    exact_tree = _FakeTree("exact-tree", [140, 300, 800, 470], [])
    slots = tuple(
        SimpleNamespace(index=index, label=label, value=value, state=state)
        for index, (label, value, state) in enumerate(
            [
                ("현품표", "MASTER", "complete"),
                ("제품1", "-", "current"),
                ("제품2", "-", "pending"),
                ("제품3", "-", "pending"),
                ("라벨지", "-", "pending"),
            ],
            1,
        )
    )
    mapping = {"complete": "완료", "current": "현재", "pending": "대기"}
    return SimpleNamespace(
        qa_scan_tree=qa_tree,
        exact_rescan_tree=exact_tree,
        live_scan_notebook=_FakeNotebook(str(qa_frame)),
        qa_scan_frame=qa_frame,
        exact_rescan_frame=exact_frame,
        operator_center_pane=_NamedWidget("center", [100, 100, 840, 650]),
        entry=_NamedWidget("entry", [140, 220, 800, 270]),
        _last_workflow_view=SimpleNamespace(slots=slots),
        _workflow_state_text=lambda state: mapping[state],
        _workflow_view_source=lambda: {"exact_rescan_barcodes": []},
        update_idletasks=lambda: None,
    )


def test_central_scan_list_compares_presenter_expected_rows_and_geometry(monkeypatch):
    rows = [
        {
            "iid": f"qa-slot-{index}",
            "values": [f"{index}. {label}", value, state_text],
            "tags": [state],
        }
        for index, (label, value, state, state_text) in enumerate(
            [
                ("현품표", "MASTER", "complete", "완료"),
                ("제품1", "-", "current", "현재"),
                ("제품2", "-", "pending", "대기"),
                ("제품3", "-", "pending", "대기"),
                ("라벨지", "-", "pending", "대기"),
            ],
            1,
        )
    ]
    app = _fake_central_app(rows)
    monkeypatch.setattr(capture, "_tk_rect", lambda widgets: list(widgets[0].rect))

    tree, evidence = capture._central_scan_list_evidence(app)

    assert tree is app.qa_scan_tree
    assert evidence["location"] == "central_lower"
    assert evidence["below_scan_entry"] is True
    assert evidence["rows_exact_match"] is True
    assert evidence["observed_rows_sha256"] == evidence["expected_rows_sha256"]

    app.qa_scan_tree.rows[1]["values"][1] = "WRONG"
    with pytest.raises(RuntimeError, match="geometry/data contract failed"):
        capture._central_scan_list_evidence(app)


def test_existing_asset_root_is_preserved_and_rejected(monkeypatch, tmp_path):
    tool_root = tmp_path / "repo"
    asset_root = tool_root / "docs" / "assets" / "packet"
    asset_root.mkdir(parents=True)
    sentinel = asset_root / "sentinel.txt"
    sentinel.write_text("keep", encoding="utf-8")
    monkeypatch.setattr(capture, "TOOL_ROOT", tool_root)
    monkeypatch.setattr(capture, "RAW_DIR", asset_root / "raw")
    monkeypatch.setattr(capture, "ANNOTATED_DIR", asset_root / "annotated")

    with pytest.raises(RuntimeError, match="already exists"):
        capture._assert_new_asset_root(asset_root)

    assert sentinel.read_text(encoding="utf-8") == "keep"


def test_manifest_privacy_walk_rejects_real_paths_but_allows_display_device():
    assert capture._privacy_contract({"device": r"\\.\DISPLAY2", "path": "raw/a.png"})["status"] == "PASS"
    assert capture._privacy_contract({"path": r"C:\Users\operator\AppData\Local\Temp\x"})["status"] == "FAIL"
    assert capture._privacy_contract({"hostname": "DESKTOP-SECRET"})["status"] == "FAIL"


def _fake_workflow_state_app():
    exact_view = SimpleNamespace(status="active", completed=1, target=3)
    view = SimpleNamespace(
        qa_completed=1,
        current_stage="exact_rescan",
        exact_rescan=exact_view,
    )
    return SimpleNamespace(
        current_set_info={
            "raw": ["MASTER"],
            "parsed": ["AAA2270730100"],
            "exact_rescan_active": True,
            "exact_rescan_complete": False,
            "exact_rescan_target_count": 3,
            "exact_rescan_barcodes": ["MEMBER-1"],
        },
        _workflow_completion_kind=None,
        _workflow_display_scans=(),
        _pending_workflow_error=None,
        _workflow_pending_error=None,
        _last_workflow_view=view,
        _render_operator_workbench=lambda: view,
    )


def test_capture_workflow_state_contract_rejects_wrong_fixture_progression():
    app = _fake_workflow_state_app()
    records = []

    record = capture._require_workflow_state(
        records,
        app,
        "f4-member-1",
        raw_count=1,
        exact_active=True,
        exact_complete=False,
        exact_target=3,
        exact_count=1,
        pending_error=False,
        view_qa_completed=1,
        view_exact_status="active",
        view_exact_completed=1,
        view_exact_target=3,
    )

    assert record["status"] == "PASS"
    assert records == [record]
    with pytest.raises(RuntimeError, match="capture fixture state mismatch"):
        capture._require_workflow_state(
            records,
            app,
            "invalid-f4-member-2",
            exact_count=2,
        )
    assert records[-1]["status"] == "FAIL"
    assert records[-1]["mismatches"]["exact_count"] == {
        "expected": 2,
        "observed": 1,
    }


def test_source_identity_requires_exact_commit_tree_and_clean_worktree(tmp_path):
    repo = tmp_path / "source"
    (repo / "tools").mkdir(parents=True)
    (repo / "Label_Match.py").write_text('APP_VERSION = "v2.0.36"\n', encoding="utf-8")
    (repo / "tools" / "label_match_operator_ui_walkthrough.py").write_text("VALUE = 1\n", encoding="utf-8")
    subprocess.run(["git", "init", "-q", str(repo)], check=True)
    subprocess.run(["git", "-C", str(repo), "config", "user.name", "Test"], check=True)
    subprocess.run(["git", "-C", str(repo), "config", "user.email", "test@example.invalid"], check=True)
    subprocess.run(["git", "-C", str(repo), "add", "."], check=True)
    subprocess.run(["git", "-C", str(repo), "commit", "-qm", "fixture"], check=True)
    commit = subprocess.check_output(["git", "-C", str(repo), "rev-parse", "HEAD"], text=True).strip()
    tree = subprocess.check_output(["git", "-C", str(repo), "rev-parse", "HEAD^{tree}"], text=True).strip()

    identity = capture._source_identity(repo, expected_commit=commit, expected_tree=tree)
    assert identity["commit"] == commit
    assert identity["tree"] == tree
    assert identity["worktree_clean"] is True

    (repo / "untracked.txt").write_text("dirty", encoding="utf-8")
    with pytest.raises(RuntimeError, match="worktree is dirty"):
        capture._source_identity(repo, expected_commit=commit, expected_tree=tree)


@pytest.mark.skipif(not LIVE_ASSETS_AVAILABLE, reason="fresh DISPLAY2 manual packet not generated yet")
def test_publisher_rejects_mutated_manifest_and_image_bytes(tmp_path):
    asset_dir = tmp_path / publisher.ASSET_FOLDER
    shutil.copytree(publisher.DEFAULT_ASSET_DIR, asset_dir)
    manifest_path = asset_dir / "manifest.json"
    original_manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    links = [publisher.ASSET_PREFIX + entry["annotated_path"] for entry in original_manifest["images"]]

    def write_manifest(value):
        manifest_path.write_text(json.dumps(value, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")

    mutated = copy.deepcopy(original_manifest)
    mutated["app_version"] = "v2.0.35"
    write_manifest(mutated)
    with pytest.raises(RuntimeError, match="app version"):
        publisher._validate_capture_manifest(asset_dir, links)

    mutated = copy.deepcopy(original_manifest)
    mutated["images"][0]["target_is_foreground"] = False
    write_manifest(mutated)
    with pytest.raises(RuntimeError, match="monitor/foreground"):
        publisher._validate_capture_manifest(asset_dir, links)

    mutated = copy.deepcopy(original_manifest)
    mutated["images"][0]["central_scan_list"]["expected_rows"][0]["values"][1] = "WRONG"
    write_manifest(mutated)
    with pytest.raises(RuntimeError, match="central scan-list"):
        publisher._validate_capture_manifest(asset_dir, links)

    write_manifest(original_manifest)
    annotated_path = asset_dir / original_manifest["images"][0]["annotated_path"]
    original_bytes = annotated_path.read_bytes()
    annotated_path.write_bytes(original_bytes + b"tampered")
    with pytest.raises(RuntimeError, match="manifest hashes"):
        publisher._validate_capture_manifest(asset_dir, links)
    annotated_path.write_bytes(original_bytes)

    raw_path = asset_dir / original_manifest["images"][0]["raw_path"]
    original_raw = raw_path.read_bytes()
    Image.new("RGB", (1, 1), "black").save(raw_path)
    mutated = copy.deepcopy(original_manifest)
    mutated["images"][0]["raw_sha256"] = _sha256(raw_path)
    write_manifest(mutated)
    with pytest.raises(RuntimeError, match="format/dimensions"):
        publisher._validate_capture_manifest(asset_dir, links)
    raw_path.write_bytes(original_raw)

    mutated = copy.deepcopy(original_manifest)
    mutated["images"][0]["raw_path"] = "../escape.png"
    write_manifest(mutated)
    with pytest.raises(RuntimeError, match="paths do not match"):
        publisher._validate_capture_manifest(asset_dir, links)
