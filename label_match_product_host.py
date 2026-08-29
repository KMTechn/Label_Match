"""Dispatch non-GUI modes through the packaged Label_Match host."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timezone
import io
import json
import os
from pathlib import Path
import sys
from typing import Iterator, Sequence
import uuid

DIRECT_SYNC_RELAY_MODE = "--label-match-direct-sync-relay"
USER_RELAY_MODE = "--label-match-user-relay"
SCHEDULED_RELAY_MODE = "--label-match-scheduled-relay"
ONBOARD_CURRENT_USER_MODE = "--onboard-current-user"
REMOVE_CURRENT_USER_MODE = "--remove-current-user-setup"
PRODUCT_MODES = frozenset(
    {
        DIRECT_SYNC_RELAY_MODE,
        USER_RELAY_MODE,
        SCHEDULED_RELAY_MODE,
        ONBOARD_CURRENT_USER_MODE,
        REMOVE_CURRENT_USER_MODE,
    }
)
HOSTED_RELAY_FAILURE_EXIT_CODE = 1
BOOTSTRAP_INTEGRITY_WARNING_FILENAME = "bootstrap_integrity_warning.json"


def _default_product_root() -> Path:
    module_parent = Path(__file__).resolve().parent
    portable_root = module_parent.parent
    if (
        module_parent.name.casefold() == "app"
        and (portable_root / "runtime" / "pythonw.exe").is_file()
        and (portable_root / "portable-manifest.json").is_file()
    ):
        return portable_root
    return module_parent


def _verify_frozen_host_integrity() -> dict[str, object]:
    if not getattr(sys, "frozen", False):
        return {"status": "NOT_TESTED", "reason": "source mode"}
    from current_user_onboarding import (
        resolve_current_user_onboarding_paths,
        verify_bootstrap_integrity,
    )

    app_root = Path(sys.executable).resolve().parent
    return verify_bootstrap_integrity(
        resolve_current_user_onboarding_paths(app_root),
        required=True,
    )


def _record_bootstrap_integrity_absent(arguments: Sequence[str], mode: str) -> None:
    captured_at = datetime.now(timezone.utc).replace(microsecond=0).isoformat()
    payload: dict[str, object] = {
        "status": "warning",
        "app": "Label_Match",
        "mode": mode,
        "warning_code": "bootstrap_integrity_absent",
        "warning_message": (
            "hosted relay continued without a bootstrap integrity record"
        ),
        "updated_at": captured_at,
    }
    try:
        from current_user_onboarding import resolve_current_user_onboarding_paths

        selected_root = _option_value(arguments, "--app-root")
        app_root = (
            Path(selected_root).expanduser().resolve()
            if selected_root
            else (
                Path(sys.executable).resolve().parent
                if getattr(sys, "frozen", False)
                else _default_product_root()
            )
        )
        warning_path = (
            resolve_current_user_onboarding_paths(app_root).status_dir
            / BOOTSTRAP_INTEGRITY_WARNING_FILENAME
        )
        _write_json_atomic(warning_path, payload)
    except Exception:
        pass
    log_path = _option_value(arguments, "--log-path")
    if log_path:
        try:
            _append_jsonl(
                Path(log_path),
                {
                    "event": "bootstrap_integrity_absent",
                    "app": "Label_Match",
                    "mode": mode,
                    "generated_at": captured_at,
                },
            )
        except Exception:
            pass


def _option_value(arguments: Sequence[str], option: str) -> str:
    for index, argument in enumerate(arguments):
        if argument == option and index + 1 < len(arguments):
            return str(arguments[index + 1])
        prefix = f"{option}="
        if argument.startswith(prefix):
            return argument[len(prefix) :]
    return ""


def _write_json_atomic(path: Path, payload: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f"{path.name}.tmp.{os.getpid()}.{uuid.uuid4().hex}")
    try:
        with temporary.open("w", encoding="utf-8", newline="\n") as handle:
            json.dump(payload, handle, ensure_ascii=False, indent=2, sort_keys=True)
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def _append_jsonl(path: Path, payload: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8", newline="\n") as handle:
        handle.write(json.dumps(payload, ensure_ascii=False, sort_keys=True))
        handle.write("\n")
        handle.flush()
        os.fsync(handle.fileno())


def _record_hosted_relay_failure(arguments: Sequence[str], error: Exception) -> None:
    captured_at = datetime.now(timezone.utc).replace(microsecond=0).isoformat()
    error_type = error.__class__.__name__[:128]
    worker_id = _option_value(arguments, "--worker-id")[:256]
    diagnostic = {
        "status": "runtime_error",
        "app": "Label_Match",
        "worker_id": worker_id,
        "error_code": "hosted_relay_unhandled_exception",
        "error_message": f"hosted DirectSync relay failed unexpectedly: {error_type}",
        "runtime_status_write_status": "PASS",
        "updated_at": captured_at,
    }
    status_path = _option_value(arguments, "--runtime-status-path")
    if not status_path and (
        USER_RELAY_MODE in arguments or SCHEDULED_RELAY_MODE in arguments
    ):
        try:
            from current_user_onboarding import resolve_current_user_onboarding_paths

            selected_root = _option_value(arguments, "--app-root")
            app_root = (
                Path(selected_root).expanduser().resolve()
                if selected_root
                else (
                    Path(sys.executable).resolve().parent
                    if getattr(sys, "frozen", False)
                    else _default_product_root()
                )
            )
            status_name = (
                "scheduled_direct_sync_relay_status.json"
                if SCHEDULED_RELAY_MODE in arguments
                else "label_match_user_relay.json"
            )
            status_path = str(
                resolve_current_user_onboarding_paths(app_root).status_dir / status_name
            )
        except Exception:
            status_path = ""
    if status_path:
        try:
            _write_json_atomic(Path(status_path), diagnostic)
        except Exception:
            pass
    log_path = _option_value(arguments, "--log-path")
    if log_path:
        try:
            _append_jsonl(
                Path(log_path),
                {
                    "event": "hosted_relay_unhandled_exception",
                    "app": "Label_Match",
                    "worker_id": worker_id,
                    "error_code": diagnostic["error_code"],
                    "error_type": error_type,
                    "generated_at": captured_at,
                },
            )
        except Exception:
            pass


@contextmanager
def _usable_output_streams() -> Iterator[None]:
    original_stdout = sys.stdout
    original_stderr = sys.stderr
    if sys.stdout is None:
        sys.stdout = io.StringIO()
    if sys.stderr is None:
        sys.stderr = io.StringIO()
    try:
        yield
    finally:
        sys.stdout = original_stdout
        sys.stderr = original_stderr


def dispatch_product_mode(argv: Sequence[str]) -> int | None:
    arguments = list(argv)
    if not arguments or arguments[0] not in PRODUCT_MODES:
        return None
    mode = arguments.pop(0)

    with _usable_output_streams():
        if mode in {
            DIRECT_SYNC_RELAY_MODE,
            USER_RELAY_MODE,
            SCHEDULED_RELAY_MODE,
        }:
            try:
                integrity = _verify_frozen_host_integrity()
                if integrity.get("status") == "ABSENT":
                    _record_bootstrap_integrity_absent(arguments, mode)
                    print(
                        "warning: bootstrap integrity record is absent; "
                        "hosted relay is continuing without file verification",
                        file=sys.stderr,
                    )
            except Exception as exc:
                _record_hosted_relay_failure([mode, *arguments], exc)
                return HOSTED_RELAY_FAILURE_EXIT_CODE
        if mode == DIRECT_SYNC_RELAY_MODE:
            try:
                from tools import direct_sync_relay_runner

                return int(direct_sync_relay_runner.main(arguments))
            except Exception as exc:
                _record_hosted_relay_failure(arguments, exc)
                return HOSTED_RELAY_FAILURE_EXIT_CODE
        if mode == USER_RELAY_MODE:
            try:
                from user_relay import main as user_relay_main

                return int(user_relay_main(arguments))
            except Exception as exc:
                _record_hosted_relay_failure([USER_RELAY_MODE, *arguments], exc)
                return HOSTED_RELAY_FAILURE_EXIT_CODE
        if mode == SCHEDULED_RELAY_MODE:
            try:
                from user_relay import scheduled_main

                return int(scheduled_main(arguments))
            except Exception as exc:
                _record_hosted_relay_failure([SCHEDULED_RELAY_MODE, *arguments], exc)
                return HOSTED_RELAY_FAILURE_EXIT_CODE
        if mode == ONBOARD_CURRENT_USER_MODE:
            from current_user_onboarding import onboarding_main

            return int(onboarding_main(arguments))
        if mode == REMOVE_CURRENT_USER_MODE:
            from current_user_onboarding import removal_main

            return int(removal_main(arguments))
    raise AssertionError(f"unhandled Label_Match product mode: {mode}")
