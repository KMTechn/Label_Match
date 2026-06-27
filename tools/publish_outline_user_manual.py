#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Publish the Label_Match worker manual to Outline with guarded verification."""

from __future__ import annotations

import argparse
import json
import mimetypes
import os
import re
import sys
from pathlib import Path
from typing import Any

import requests


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_MANUAL = ROOT / "docs" / "OUTLINE_LABEL_MATCH_USER_MANUAL_20260626.md"
DEFAULT_ASSET_DIR = ROOT / "docs" / "assets" / "label_match_user_manual_20260626"
DEFAULT_OUTLINE_URL = "https://wiki.kmtecherp.com"
DEFAULT_DOCUMENT_ID = "4115be8b-488a-4934-80af-f0f9e4ee721b"
DEFAULT_TITLE = "Label_Match(포장실 프로그램)"
EXPECTED_UNIQUE_WORKER_IMAGES = 24
EXPECTED_MARKDOWN_IMAGE_REFS = 26


def _load_env_file(path: Path) -> dict[str, str]:
    values: dict[str, str] = {}
    if not path.exists():
        return values
    for raw in path.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        if line.startswith("export "):
            line = line[len("export ") :].strip()
        key, value = line.split("=", 1)
        values[key.strip()] = value.strip().strip('"').strip("'")
    return values


def _candidate_env_files(explicit: str = "") -> list[Path]:
    candidates: list[Path] = []
    if explicit:
        candidates.append(Path(explicit))
    if os.environ.get("OUTLINE_ENV_FILE"):
        candidates.append(Path(os.environ["OUTLINE_ENV_FILE"]))
    candidates.extend(
        [
            ROOT / ".outline_env",
            ROOT.parent / ".outline_env",
            Path.home() / ".outline_env",
            Path("/root/.outline_env"),
        ]
    )
    seen: set[str] = set()
    unique: list[Path] = []
    for candidate in candidates:
        key = str(candidate)
        if key not in seen:
            unique.append(candidate)
            seen.add(key)
    return unique


def _load_outline_config(args: argparse.Namespace) -> tuple[str, str]:
    env_values: dict[str, str] = {}
    for env_file in _candidate_env_files(args.env_file):
        env_values.update(_load_env_file(env_file))
    outline_url = args.outline_url or os.environ.get("OUTLINE_URL") or env_values.get("OUTLINE_URL") or DEFAULT_OUTLINE_URL
    token = os.environ.get("OUTLINE_API_TOKEN") or env_values.get("OUTLINE_API_TOKEN") or ""
    return outline_url.rstrip("/"), token


def _manual_image_paths(text: str) -> list[str]:
    return re.findall(r"!\[[^\]]*\]\((assets/label_match_user_manual_20260626/[^)\s]+\.png)\)", text)


def _count_today_button_typo(text: str) -> int:
    return len(re.findall(r"(?<!오)늘 버튼으로", text))


def _build_outline_text(manual_path: Path, asset_dir: Path, attachment_urls: dict[str, str] | None = None) -> tuple[str, dict[str, Any]]:
    text = manual_path.read_text(encoding="utf-8")
    links = _manual_image_paths(text)
    unique_links = list(dict.fromkeys(links))
    missing = [rel for rel in unique_links if not (asset_dir / Path(rel).name).exists()]
    report: dict[str, Any] = {
        "manual_path": str(manual_path),
        "asset_dir": str(asset_dir),
        "markdown_image_refs": len(links),
        "unique_image_refs": len(unique_links),
        "missing_images": missing,
        "mermaid_block_count": text.count("```mermaid"),
        "local_file_upload_text_count": text.count("파일 업로드"),
        "local_typo_count": _count_today_button_typo(text),
        "local_today_phrase_count": text.count("`오늘` 버튼으로") + text.count("오늘 버튼으로"),
    }
    if len(unique_links) != EXPECTED_UNIQUE_WORKER_IMAGES:
        raise RuntimeError(f"expected {EXPECTED_UNIQUE_WORKER_IMAGES} unique worker images, got {len(unique_links)}")
    if len(links) != EXPECTED_MARKDOWN_IMAGE_REFS:
        raise RuntimeError(f"expected {EXPECTED_MARKDOWN_IMAGE_REFS} markdown image references, got {len(links)}")
    if missing:
        raise FileNotFoundError(f"missing manual images: {missing}")
    if report["local_file_upload_text_count"] != 0:
        raise RuntimeError("local manual unexpectedly contains '파일 업로드'")
    if report["local_typo_count"] != 0:
        raise RuntimeError("local manual unexpectedly contains '늘 버튼으로'")
    if report["local_today_phrase_count"] < 1:
        raise RuntimeError("local manual does not contain the expected today-button phrase")
    if attachment_urls:
        for rel in unique_links:
            text = text.replace(rel, attachment_urls[rel])
        report.update(
            {
                "outline_attachment_refs": text.count("/api/attachments.redirect"),
                "relative_image_refs_after_replace": text.count("assets/label_match_user_manual_20260626/"),
            }
        )
    return text, report


class OutlineClient:
    def __init__(self, base_url: str, token: str) -> None:
        if not token:
            raise RuntimeError("OUTLINE_API_TOKEN is required for publish mode")
        self.base_url = base_url.rstrip("/")
        self.token = token
        self.session = requests.Session()
        self.headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
            "Accept": "application/json",
            "User-Agent": "LabelMatchManualPublisher/20260626",
        }

    def api(self, method: str, payload: dict[str, Any]) -> dict[str, Any]:
        response = self.session.post(f"{self.base_url}/api/{method}", headers=self.headers, json=payload, timeout=60)
        if not response.ok:
            raise RuntimeError(f"{method} HTTP {response.status_code}: {response.text[:500]}")
        data = response.json()
        if data.get("ok") is False:
            raise RuntimeError(f"{method} not ok: {data}")
        return data

    def upload_image(self, document_id: str, path: Path) -> str:
        content_type = mimetypes.guess_type(path.name)[0] or "image/png"
        created = self.api(
            "attachments.create",
            {
                "name": path.name,
                "contentType": content_type,
                "size": path.stat().st_size,
                "documentId": document_id,
            },
        )["data"]
        upload_url = created["uploadUrl"]
        if upload_url.startswith("/"):
            upload_url = self.base_url + upload_url
        attachment_url = created["attachment"]["url"]
        if attachment_url.startswith("/"):
            attachment_url = self.base_url + attachment_url
        with path.open("rb") as image_file:
            uploaded = self.session.post(
                upload_url,
                headers={
                    "Authorization": f"Bearer {self.token}",
                    "Accept": "application/json,*/*",
                    "User-Agent": "LabelMatchManualPublisher/20260626",
                },
                data=created.get("form") or {},
                files={"file": (path.name, image_file, content_type)},
                timeout=180,
            )
        if not uploaded.ok:
            raise RuntimeError(f"upload failed {path.name}: HTTP {uploaded.status_code}: {uploaded.text[:500]}")
        return attachment_url


def _write_report(path: str, report: dict[str, Any]) -> None:
    if not path:
        return
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Publish the Label_Match worker manual to Outline")
    parser.add_argument("--manual", default=str(DEFAULT_MANUAL))
    parser.add_argument("--asset-dir", default=str(DEFAULT_ASSET_DIR))
    parser.add_argument("--outline-url", default="")
    parser.add_argument("--document-id", default=DEFAULT_DOCUMENT_ID)
    parser.add_argument("--title", default=DEFAULT_TITLE)
    parser.add_argument("--env-file", default="")
    parser.add_argument("--report-path", default="")
    parser.add_argument("--dry-run", action="store_true", help="Validate local manual and prepared payload without network writes")
    args = parser.parse_args(argv)

    manual_path = Path(args.manual)
    asset_dir = Path(args.asset_dir)
    outline_url, token = _load_outline_config(args)

    try:
        if args.dry_run:
            _, report = _build_outline_text(manual_path, asset_dir)
            report.update(
                {
                    "status": "PASS",
                    "mode": "dry-run",
                    "outline_url": outline_url,
                    "document_id": args.document_id,
                    "title": args.title,
                    "token_present": bool(token),
                }
            )
            _write_report(args.report_path, report)
            print(json.dumps(report, ensure_ascii=False, sort_keys=True))
            return 0

        client = OutlineClient(outline_url, token)
        source_text = manual_path.read_text(encoding="utf-8")
        unique_links = list(dict.fromkeys(_manual_image_paths(source_text)))
        attachment_urls = {
            rel: client.upload_image(args.document_id, asset_dir / Path(rel).name)
            for rel in unique_links
        }
        outline_text, local_report = _build_outline_text(manual_path, asset_dir, attachment_urls)
        client.api("documents.update", {"id": args.document_id, "title": args.title, "text": outline_text, "editMode": "replace", "publish": True})
        info = client.api("documents.info", {"id": args.document_id})
        data = info.get("data") or {}
        doc = data.get("document") or data
        doc_text = doc.get("text") or ""
        report = {
            **local_report,
            "status": "PASS",
            "mode": "publish",
            "outline_url": outline_url + str(doc.get("url", "")),
            "document_id": args.document_id,
            "unique_images_uploaded": len(attachment_urls),
            "document_markdown_image_refs": doc_text.count("!["),
            "document_attachment_refs": doc_text.count("/api/attachments.redirect"),
            "document_relative_image_refs": doc_text.count("assets/label_match_user_manual_20260626/"),
            "document_file_upload_text_count": doc_text.count("파일 업로드"),
            "document_typo_count": _count_today_button_typo(doc_text),
            "document_today_phrase_count": doc_text.count("`오늘` 버튼으로") + doc_text.count("오늘 버튼으로"),
            "document_text_length": len(doc_text),
        }
        required = {
            "document_markdown_image_refs": EXPECTED_MARKDOWN_IMAGE_REFS,
            "document_attachment_refs": EXPECTED_MARKDOWN_IMAGE_REFS,
            "document_relative_image_refs": 0,
            "document_file_upload_text_count": 0,
            "document_typo_count": 0,
        }
        for key, expected in required.items():
            if report[key] != expected:
                report["status"] = "FAIL"
                raise RuntimeError(f"{key} expected {expected}, got {report[key]}")
        if report["document_today_phrase_count"] < 1:
            report["status"] = "FAIL"
            raise RuntimeError("published document does not contain expected today-button phrase")
        _write_report(args.report_path, report)
        print(json.dumps(report, ensure_ascii=False, sort_keys=True))
        return 0
    except Exception as exc:
        report = {"status": "FAIL", "error": str(exc), "mode": "dry-run" if args.dry_run else "publish"}
        _write_report(args.report_path, report)
        print(json.dumps(report, ensure_ascii=False, sort_keys=True), file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
