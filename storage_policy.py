from __future__ import annotations

from pathlib import Path


LOCAL_EVENTS_DIR_NAME = "local_events"


def label_match_local_events_dir(save_directory: str | Path) -> Path:
    """Resolve a sibling directory outside the non-recursive relay source."""

    text = str(save_directory or "").strip()
    if not text:
        raise ValueError("Label_Match save directory is required")
    source_dir = Path(text).expanduser().resolve(strict=False)
    return source_dir.parent / LOCAL_EVENTS_DIR_NAME


__all__ = ["LOCAL_EVENTS_DIR_NAME", "label_match_local_events_dir"]
