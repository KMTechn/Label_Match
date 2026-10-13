"""Decisions for startup updates; code and business data have separate lifetimes.

The decision functions are pure. The protected bootstrap persists records before
switching a code pointer; business data is never copied or restored here.
"""
from __future__ import annotations

from datetime import datetime, timezone
import hashlib
import ast
import json
import os
from pathlib import Path
import re
import sys
import time
import uuid
from contextlib import contextmanager
from typing import Any, Mapping

if __name__ == "__main__":
    sys.path.insert(0, str(Path(__file__).resolve().parent))
    sys.path.insert(1, str(Path(__file__).resolve().parent / "site-packages"))

from writer_session_fence import WriterFencedError, writer_sink, _assert_no_reparse_components, WriterFenceError


SCHEMA = "label-match-startup-update-v1"
READY_CHECKS = (
    "gui_initialized", "identity_readable", "ledger_readable",
    "unfinished_readable", "outbox_readable", "ordinary_user_relay",
)


APPLY_PROGRESS_TIMEOUT = 60
STARTUP_WAIT_SECONDS = 240
STARTUP_PROGRESS_MESSAGE = "업데이트가 진행 중입니다. 잠시 후 다시 실행하세요."
UPDATE_MUTEX_RECOVERY_MESSAGE = (
    "설치 복구 필요: 업데이트 상태와 설치 파일을 확인하지 못해 시작을 멈췄습니다. "
    "자료와 복구 기록을 보존하고 관리자에게 지원 복구를 요청하세요."
)


class UpdateMutexRecoveryRequired(RuntimeError):
    """An abandoned owner never grants permission to use unverified code."""

    def __init__(self, policy):
        super().__init__("UPDATE_MUTEX_ABANDONED_RECOVERY_REQUIRED")
        self.policy = policy


def support_record(fields=None, *, stage):
    from failure_redaction import public_record
    return public_record({**dict(fields or {}), "stage": stage})


def emit_support_record(fields, *, stage, policy=None):
    report = support_record(fields, stage=stage)
    if policy is not None and policy.get("staging_root"):
        try:
            write_json_durable(Path(policy["staging_root"]) / "startup-support.json", report)
        except (OSError, ValueError, TypeError, WriterFenceError):
            emit_support_record({"reason_code": "REPORT_UNAVAILABLE"}, stage=stage)
    print(json.dumps(report, ensure_ascii=False, sort_keys=True))


def _sha(value: Any, *, length: int = 40) -> str:
    if not isinstance(value, str) or re.fullmatch(r"[0-9a-f]{%d}" % length, value) is None:
        raise ValueError("update identity is malformed")
    return value


def _utc(value: Any) -> datetime:
    if not isinstance(value, str):
        raise ValueError("update time is absent")
    result = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if result.tzinfo is None or result.utcoffset() is None:
        raise ValueError("update time must include an offset")
    try:
        return result.astimezone(timezone.utc)
    except OverflowError as error:
        raise ValueError("update time is outside the representable UTC range") from error


def startup_action(
    transition: Mapping[str, Any] | None, *, prepared_present: bool,
) -> str:
    """Inspect both the candidate and the record, including before the task runs."""
    if transition is None:
        return "REQUEST_APPLY" if prepared_present else "START_CURRENT"
    if transition.get("schema") != SCHEMA:
        raise ValueError("startup update record is unsupported")
    phase = transition.get("phase")
    if phase == "DEFERRED" and transition.get("recovery_pending") is True:
        return "WAIT_FOR_RECOVERY"
    if phase == "PREPARED":
        return "REQUEST_APPLY"
    if phase in {"APPLIED_PENDING_READY", "ROLLBACK_PENDING_READY"}:
        return "WAIT_FOR_TRIAL" if transition.get("trial_pid") else "REQUEST_TRIAL"
    if phase in {"ROLLBACK_PENDING", "UNKNOWN", "FAILED"}:
        return "WAIT_FOR_RECOVERY"
    if phase not in {"CONFIRMED", "ROLLED_BACK", "DEFERRED"}:
        raise ValueError("startup update phase is unsupported")
    return "REQUEST_APPLY" if prepared_present else "START_CURRENT"


def prepare_transition(
    *, previous_sha: str, candidate_sha: str, packet_sha256: str,
    forward_pin: str, rollback_pin: str, transition_id: str, nonce: str,
    now: str, failed_shas: list[str] | None = None,
) -> dict[str, Any]:
    previous_sha = _sha(previous_sha)
    candidate_sha = _sha(candidate_sha)
    if previous_sha == candidate_sha:
        raise ValueError("update does not change the version")
    failures = list(failed_shas or [])
    for failed in failures:
        _sha(failed)
    if candidate_sha in failures:
        raise ValueError("a failed version must not be reapplied")
    _utc(now)
    return {
        "schema": SCHEMA, "phase": "PREPARED",
        "previous_sha": previous_sha, "candidate_sha": candidate_sha,
        "packet_sha256": _sha(packet_sha256, length=64),
        "forward_pin": _sha(forward_pin, length=64),
        "rollback_pin": _sha(rollback_pin, length=64),
        "transition_id": _sha(transition_id, length=32),
        "nonce": _sha(nonce, length=32), "started_at": now,
        "failed_shas": failures, "trial_pid": None,
    }


def apply_transition(
    prepared: Mapping[str, Any], *, writers_absent: bool,
    packet_verified: bool, durable_prepared: bool, now: str,
) -> dict[str, Any]:
    """A false/unknown observation authorizes no pointer change."""
    if prepared.get("schema") != SCHEMA or prepared.get("phase") != "PREPARED":
        raise ValueError("update was not prepared")
    if writers_absent is not True or packet_verified is not True or durable_prepared is not True:
        raise ValueError("update requires writer absence, verification and durable preparation")
    _utc(now)
    return {**prepared, "phase": "APPLIED_PENDING_READY", "applied_at": now}


def bind_trial(
    transition: Mapping[str, Any], *, sha: str, pid: int,
    ordinary_user: bool, now: str,
) -> dict[str, Any]:
    phase = transition.get("phase")
    expected_sha = transition.get("previous_sha" if phase == "ROLLBACK_PENDING_READY" else "candidate_sha")
    if phase not in {"APPLIED_PENDING_READY", "ROLLBACK_PENDING_READY"} or sha != expected_sha:
        raise ValueError("trial version differs from the pending version")
    if transition.get("trial_pid") is not None:
        raise ValueError("the trial has already been started")
    if type(pid) is not int or pid < 1 or ordinary_user is not True:
        raise ValueError("trial must be a live ordinary-user process")
    _utc(now)
    return {**transition, "trial_pid": pid, "trial_sha": sha, "trial_started_at": now}


def ready_matches(
    transition: Mapping[str, Any], ready: Mapping[str, Any], *,
    process_matches: bool, relay_matches: bool, now: str,
) -> bool:
    """PID/heartbeat/exit zero alone do not establish application readiness."""
    phase = transition.get("phase")
    if phase not in {"APPLIED_PENDING_READY", "ROLLBACK_PENDING_READY"}:
        return False
    sha = transition.get("previous_sha" if phase == "ROLLBACK_PENDING_READY" else "candidate_sha")
    if (
        process_matches is not True or relay_matches is not True
        or type(ready.get("pid")) is not int or ready["pid"] < 1
        or ready["pid"] != transition.get("trial_pid")
        or ready.get("sha") != sha
        or ready.get("nonce") != transition.get("nonce")
        or ready.get("transition_id") != transition.get("transition_id")
        or any(ready.get(check) is not True for check in READY_CHECKS)
    ):
        return False
    try:
        return _utc(transition["trial_started_at"]) <= _utc(ready["ready_at"]) <= _utc(now)
    except (KeyError, TypeError, ValueError):
        return False


def confirm_transition(
    transition: Mapping[str, Any], ready: Mapping[str, Any], *,
    process_matches: bool, relay_matches: bool, now: str,
) -> dict[str, Any]:
    if not ready_matches(transition, ready, process_matches=process_matches, relay_matches=relay_matches, now=now):
        raise ValueError("update application readiness is unproven")
    rollback = transition["phase"] == "ROLLBACK_PENDING_READY"
    return {**transition, "phase": "ROLLED_BACK" if rollback else "CONFIRMED", "confirmed_at": now}


def request_rollback(transition: Mapping[str, Any], *, reason: str) -> dict[str, Any]:
    if transition.get("phase") != "APPLIED_PENDING_READY":
        raise ValueError("rollback requires a pending trial")
    failures = list(transition.get("failed_shas", []))
    candidate = _sha(transition.get("candidate_sha"))
    if candidate not in failures:
        failures.append(candidate)
    return {**transition, "phase": "ROLLBACK_PENDING", "reason": str(reason), "failed_shas": failures}


def rollback_transition(
    transition: Mapping[str, Any], *, writers_absent: bool,
    previous_verified: bool, durable_rollback: bool, nonce: str, now: str,
) -> dict[str, Any]:
    if transition.get("phase") != "ROLLBACK_PENDING":
        raise ValueError("rollback request is absent")
    if writers_absent is not True:
        return {**transition, "phase": "UNKNOWN", "reason": "writer absence is unproven"}
    if previous_verified is not True or durable_rollback is not True:
        raise ValueError("rollback requires verified previous code and a durable request")
    if transition.get("failure_decision", {}).get("selected_sha") != transition.get("previous_sha"):
        raise ValueError("rollback requires an observed unchanged data generation")
    _utc(now)
    # E6: previous was already confirmed; a second READY is no recovery gate.
    return {**transition, "phase": "ROLLED_BACK", "nonce": _sha(nonce, length=32), "trial_pid": None, "trial_sha": None, "trial_started_at": None, "rollback_at": now, "confirmed_at": now}


def recovery_sha(transition: Mapping[str, Any], *, pointer_sha: str) -> str:
    """Choose the recorded pointer after power loss; never restore user data."""
    if transition.get("schema") != SCHEMA:
        raise ValueError("startup update record is unsupported")
    phase = transition.get("phase")
    previous = _sha(transition.get("previous_sha"))
    candidate = _sha(transition.get("candidate_sha"))
    if pointer_sha not in {previous, candidate}:
        raise ValueError("active pointer differs from both recorded versions")
    if phase == "PREPARED":
        if pointer_sha != previous:
            raise ValueError("pointer changed before the applied record was durable")
        return previous
    if phase in {"APPLIED_PENDING_READY", "CONFIRMED"}:
        return candidate
    if phase == "ROLLBACK_PENDING":
        selected = transition.get("failure_decision", {}).get("selected_sha", candidate)
        if selected not in {previous, candidate}:
            raise ValueError("failure decision names another version")
        return selected
    if phase == "DEFERRED" and transition.get("retained_candidate") is True:
        if transition.get("failure_decision", {}).get("selected_sha") != candidate:
            raise ValueError("retained trial decision differs from candidate")
        return candidate
    if phase in {"ROLLBACK_PENDING_READY", "ROLLED_BACK"}:
        return previous
    if phase in {"UNKNOWN", "FAILED"}:
        raise ValueError("unproven recovery must preserve the pointer and stop switching")
    raise ValueError("recovery phase is unsupported")


def read_trial_generation(policy: Mapping[str, Any]) -> dict[str, Any]:
    """Read the ordinary operator's shared commit slot, never SYSTEM's profile."""
    from kmtech_shared import data_generation as generation
    from writer_session_fence import canonical_control_root

    try:
        sid = policy["operator_sid"]
        generation._identity("Label_Match", sid)
        local = Path(policy["operator_local_app_data"])
        if not local.is_absolute():
            raise ValueError("operator profile is not absolute")
        path = canonical_control_root({"LOCALAPPDATA": str(local)}) / "data-generation" / (sid + ".json")
        record = generation._read(path, "Label_Match", sid)
        return {"status": "READ", "marker_path": str(path), "record": record}
    except (OSError, ValueError, TypeError, KeyError, generation.DataGenerationError):
        return {"status": "UNKNOWN"}


def decide_trial_failure(transition: Mapping[str, Any], policy: Mapping[str, Any]) -> dict[str, Any]:
    """After normal writer stop, choose once from actual generation readback.

    The caller durably publishes this decision before changing any pointer.
    Both GUI and relay trials share this boundary; READY is never a G proxy.
    """
    from kmtech_shared import data_generation as generation

    if transition.get("phase") != "ROLLBACK_PENDING":
        raise ValueError("trial failure requires a normal-stop request")
    if transition.get("failure_decision") is not None:
        return dict(transition)
    before = transition.get("generation_before", {"status": "UNKNOWN"})
    after = read_trial_generation(policy)
    previous = transition.get("generation_previous_release", {})
    candidate = transition.get("generation_candidate_release", {})
    statuses = {}
    for name, release in (("previous", previous), ("candidate", candidate)):
        statuses[name] = generation.probe_downgrade(
            [after["marker_path"]] if after.get("status") == "READ" else [],
            release.get("version"), app_id="Label_Match",
            subject_sid=policy.get("operator_sid"), target_build_id=release.get("source_commit"),
        )["status"]
    # An I/O race is uncertainty, even if a separate guard probe succeeded.
    stable = after == read_trial_generation(policy)
    unchanged = (stable and before.get("status") == after.get("status") == "READ"
                 and before == after and statuses["previous"] == "ALLOW")
    code = "TRIAL_GENERATION_UNCHANGED" if unchanged else "TRIAL_GENERATION_UNKNOWN"
    record = after.get("record") or {}
    if (not unchanged and stable and before.get("status") == after.get("status") == "READ"
            and record.get("build_id") == candidate.get("source_commit")
            and record.get("version") == candidate.get("version")):
        code = "TRIAL_GENERATION_ADVANCED"
    selected = transition["previous_sha"] if unchanged else transition["candidate_sha"]
    decision = {"before": before, "after": after, "previous_status": statuses["previous"],
                "candidate_status": statuses["candidate"], "selected_sha": selected,
                "code": code, "ready_result": "FAILED", "decided_at": now_utc()}
    state = {**transition, "failure_decision": decision}
    if not unchanged:
        state = {**state, "phase": "DEFERRED", "retained_candidate": True,
                 "deferred_at": now_utc(), "reason": code}
    return state



PIN_FIELDS = ("from_commit", "to_commit", "from_contract_sha256", "to_contract_sha256")


def directional_pin(
    approvals: list[Mapping[str, Any]], *, from_commit: str, to_commit: str,
    from_contract_sha256: str, to_contract_sha256: str,
) -> str:
    """Accept an explicit direction; the reverse pair never grants this one."""
    wanted = dict(zip(PIN_FIELDS, (
        _sha(from_commit), _sha(to_commit),
        _sha(from_contract_sha256, length=64), _sha(to_contract_sha256, length=64),
    )))
    if wanted["from_commit"] == wanted["to_commit"]:
        raise ValueError("update direction must name different commits")
    if not isinstance(approvals, list):
        raise ValueError("approved update transitions must be a list")
    matched = False
    seen = set()
    for entry in approvals:
        if not isinstance(entry, Mapping) or set(entry) != set(PIN_FIELDS):
            raise ValueError("approved update transition fields differ")
        values = tuple(_sha(entry[field], length=40 if field.endswith("commit") else 64) for field in PIN_FIELDS)
        if values in seen:
            raise ValueError("approved update transitions are duplicated")
        seen.add(values)
        matched = matched or dict(entry) == wanted
    if not matched:
        raise ValueError("update direction has no exact approval")
    return hashlib.sha256(json.dumps(wanted, sort_keys=True, separators=(",", ":")).encode("ascii")).hexdigest()


def bidirectional_pins(
    approvals: list[Mapping[str, Any]], *, current: Mapping[str, Any],
    candidate: Mapping[str, Any],
) -> tuple[str, str]:
    # Raw consumer hashes are evidence. An installed-key authenticated exact
    # independently reviewed signed pair grants authority; byte equality does not.
    _sha(current.get("data_api_contract_sha256"), length=64)
    _sha(candidate.get("data_api_contract_sha256"), length=64)
    forward = directional_pin(
        approvals, from_commit=current["source_commit"], to_commit=candidate["source_commit"],
        from_contract_sha256=current["writer_sink_inventory_contract_sha256"],
        to_contract_sha256=candidate["writer_sink_inventory_contract_sha256"],
    )
    backward = directional_pin(
        approvals, from_commit=candidate["source_commit"], to_commit=current["source_commit"],
        from_contract_sha256=candidate["writer_sink_inventory_contract_sha256"],
        to_contract_sha256=current["writer_sink_inventory_contract_sha256"],
    )
    return forward, backward


def manifest_transition_pins(manifest: Mapping[str, Any], *, current: Mapping[str, Any], candidate: Mapping[str, Any]) -> tuple[str, str, dict[str, str], str]:
    """Only call after authentication of this complete manifest and packet."""
    current = {**current, "writer_sink_inventory_contract_sha256": current["contract_sha256"]}
    candidate = {**candidate, "writer_sink_inventory_contract_sha256": candidate["contract_sha256"]}
    pair = dict(zip(PIN_FIELDS, (current["source_commit"], candidate["source_commit"], current["writer_sink_inventory_contract_sha256"], candidate["writer_sink_inventory_contract_sha256"])))
    reverse = dict(zip(PIN_FIELDS, (pair["to_commit"], pair["from_commit"], pair["to_contract_sha256"], pair["from_contract_sha256"])))
    approvals = manifest.get("approved_transitions", [])
    if not isinstance(approvals, list) or len(approvals) != 2 or any(entry not in (pair, reverse) for entry in approvals):
        raise ValueError("signed approvals must contain exactly the current/candidate pair and its reverse")
    forward, rollback = bidirectional_pins(approvals, current=current, candidate=candidate)
    return forward, rollback, reverse, "SIGNED_EXACT_PAIR"


def authorize_feed_transition(manifest: Mapping[str, Any], *,
                             current: Mapping[str, Any], candidate: Mapping[str, Any]):
    """Called after signature/packet authentication and actual version readback."""
    pair = manifest.get("auto_update")
    if not isinstance(pair, Mapping) or set(pair) != {
        "from_commit", "to_commit", "forward_contract_sha256", "rollback_contract_sha256"
    }:
        raise ValueError("signed exact pair fields differ")
    if (_sha(pair["from_commit"]) != _sha(current.get("source_commit"))
            or _sha(pair["to_commit"]) != _sha(candidate.get("source_commit"))):
        raise ValueError("signed pair differs from actual installed/candidate identity")
    if manifest.get("source_commit") != candidate.get("source_commit") or manifest.get("version") != candidate.get("version"):
        raise ValueError("signed target identity differs from actual packet")
    ordered = []
    for version in (current.get("version"), manifest.get("version")):
        if not isinstance(version, str) or re.fullmatch(r"v[0-9]+(?:\.[0-9]+){2,3}", version) is None:
            raise ValueError("actual release version is malformed")
        parts = tuple(int(part) for part in version[1:].split("."))
        ordered.append(parts + (0,) * (4 - len(parts)))
    if ordered[1] <= ordered[0]:
        raise ValueError("feed version must be newer than the actual installed version")
    forward, rollback, reverse, authority = manifest_transition_pins(manifest, current=current, candidate=candidate)
    if (_sha(pair["forward_contract_sha256"], length=64) != forward
            or _sha(pair["rollback_contract_sha256"], length=64) != rollback):
        raise ValueError("signed direction contract differs from actual bytes")
    previous = _sha(current.get("data_api_contract_sha256"), length=64)
    target = _sha(candidate.get("data_api_contract_sha256"), length=64)
    evidence = {"previous_data_api_sha256": previous, "candidate_data_api_sha256": target,
                "consumer_bytes_equal": previous == target}
    return forward, rollback, reverse, authority, evidence



DATA_API_FILES = ('contract.lock.json', 'label_recovery_schema.py', 'label_data_manager.py', 'label_completion.py', 'label_transition.py', 'package_command_draft.py', 'package_logistics.py', 'package_outbox.py', 'package_outbox_processor.py', 'deferred_intent_capture.py', 'carrier_identity_port.py', 'sealed_transfer_exchange.py', 'terminal_operation_lease.py', 'producer_runtime_client.py', 'direct_sync_push.py', 'logistics_runtime_profile.py', 'event_stream_policy.py', 'Label_Match.py', 'user_relay.py', 'current_user_onboarding.py')
CONTRACT_FILES = ("tools/label_writer_fence_contract.json", "app/writer_session_fence.py")


def source_version_literal(source: bytes) -> str:
    """Build consistency; signed manifest and verified stock own versions."""
    tree = ast.parse(source.decode("utf-8-sig"))
    versions = [node for node in tree.body if isinstance(node, ast.Assign)
                and any(isinstance(target, ast.Name) and target.id == "APP_VERSION" for target in node.targets)]
    if (len(versions) != 1 or not isinstance(versions[0].value, ast.Constant)
            or not isinstance(versions[0].value.value, str)
            or re.fullmatch(r"v[0-9]+(?:\.[0-9]+){2,3}", versions[0].value.value) is None):
        raise ValueError("actual application literal is malformed")
    return versions[0].value.value


def version_identity(read_bytes) -> dict[str, Any]:
    value = json.loads(read_bytes("portable-manifest.json").decode("utf-8-sig"), object_pairs_hook=unique_object)
    if (value.get("schema") != "label-match-portable-tree-v1"
            or type(value.get("startup_update_protocol")) is not int or value["startup_update_protocol"] != 1):
        raise ValueError("version requires a stock portable packet")
    version = value.get("version")
    if not isinstance(version, str) or re.fullmatch(r"v[0-9]+(?:\.[0-9]+){2,3}", version) is None:
        raise ValueError("설치 판 번호 기록이 없는 옛 설치 파일입니다. 새 설치 파일로 지원 재설치를 진행하세요.")
    if source_version_literal(read_bytes("app/Label_Match.py")) != version:
        raise ValueError("packet version differs from its actual application literal")
    digests = []
    for paths in (CONTRACT_FILES, tuple("app/" + name for name in DATA_API_FILES)):
        digest = hashlib.sha256()
        for name in paths:
            digest.update((name + "\n").encode("ascii"))
            digest.update(hashlib.sha256(read_bytes(name)).digest())
        digests.append(digest.hexdigest())
    return {**value, "source_commit": _sha(value.get("source_commit")), "version": version,
            "contract_sha256": digests[0], "data_api_contract_sha256": digests[1]}


def packet_version_identity(packet: Path) -> dict[str, Any]:
    import zipfile
    with zipfile.ZipFile(packet) as archive:
        return version_identity(lambda name: archive.read("Label_Match/" + name))


def data_api_contract_sha256(app_root: Path) -> str:
    digest = hashlib.sha256()
    for name in DATA_API_FILES:
        path = app_root / name
        _assert_no_reparse_components(path)
        digest.update((name + "\n").encode("ascii"))
        raw = path.read_bytes()
        digest.update(hashlib.sha256(raw).digest())
    return digest.hexdigest()


def unique_object(pairs):
    value = {}
    for name, item in pairs:
        if name in value:
            raise ValueError("duplicate update record field")
        value[name] = item
    return value


def read_json(path: Path, *, maximum: int = 1024 * 1024) -> dict[str, Any]:
    _assert_no_reparse_components(path)
    for attempt in range(3):
        try:
            if path.stat().st_size > maximum:
                raise ValueError("update record is oversized")
            with path.open("r", encoding="utf-8-sig") as source:
                value = json.load(source, object_pairs_hook=unique_object)
            break
        except PermissionError:
            if attempt == 2:
                raise
            time.sleep(0.05)
    if not isinstance(value, dict):
        raise ValueError("update record must be an object")
    return value


from urllib.parse import urlparse, parse_qsl
UPDATE_GITHUB_UPDATE_HOSTS = {"github.com", "githubusercontent.com", "github.io"}
UPDATE_SECRET_QUERY_KEYS = {
    "access_token",
    "api_key",
    "client_secret",
    "github_token",
    "pat",
    "private_key",
    "sig",
    "signature",
    "token",
}
UPDATE_SECRET_QUERY_PREFIXES = ("x_amz_", "x_goog_")
def _assert_https_update_url(url, *, require_zip=False):
    parsed = urlparse(str(url or ""))
    if parsed.scheme.lower() != "https" or not parsed.netloc:
        raise ValueError("Update URL must be HTTPS")
    if not parsed.hostname or not parsed.hostname.isascii():
        raise ValueError("업데이트 주소의 호스트는 ASCII IP 또는 도메인이어야 합니다.")
    parsed.port  # Reject a malformed port before requesting a URL.
    if parsed.username is not None or parsed.password is not None:
        raise ValueError("Update URL must not include userinfo")
    if parsed.fragment:
        raise ValueError("Update URL must not include fragments")
    if require_zip and not parsed.path.lower().endswith(".zip"):
        raise ValueError("Update artifact URL must point to a .zip file")
    for key, _value in parse_qsl(parsed.query, keep_blank_values=True):
        normalized_key = key.lower().replace("-", "_")
        if normalized_key in UPDATE_SECRET_QUERY_KEYS or normalized_key.startswith(UPDATE_SECRET_QUERY_PREFIXES):
            raise ValueError("Update URL must not contain raw token query parameters")
def _is_github_hosted_update_url(url):
    parsed = urlparse(str(url or ""))
    host = (parsed.hostname or "").lower().rstrip(".")
    return any(host == root or host.endswith("." + root) for root in UPDATE_GITHUB_UPDATE_HOSTS)
assert_https_update_url = _assert_https_update_url


def policy_from_settings(settings: Mapping[str, Any]) -> dict[str, Any]:

    if not isinstance(settings, Mapping):
        raise ValueError("auto-update settings must be an object")
    allowed = {"feed_url", "channel", "apply_during_work", "profile_auth"}
    if set(settings) - allowed:
        raise ValueError("auto-update settings contain unsupported fields")
    feed = settings.get("feed_url", "")
    if not isinstance(feed, str):
        raise ValueError("auto-update feed must be a string")
    if feed:
        assert_https_update_url(feed)
        if _is_github_hosted_update_url(feed):
            raise ValueError("daily update feed cannot use GitHub hosting")
    if settings.get("apply_during_work", False) is not False:
        raise ValueError("applying during work requires separate scanner acceptance")
    channel = settings.get("channel", "stable")
    if not isinstance(channel, str) or re.fullmatch(r"[a-z0-9_-]{1,32}", channel) is None:
        raise ValueError("auto-update channel is malformed")
    auth = settings.get("profile_auth", bool(feed))
    if feed and auth is not True:
        raise ValueError("company feed requires existing device authentication")
    if type(auth) is not bool:
        raise ValueError("profile authentication setting must be a boolean")
    return {"feed_url": feed, "channel": channel, "apply_during_work": False, "profile_auth": auth}


def canonical_startup_settings(settings: Mapping[str, Any]) -> bytes:
    return (json.dumps(policy_from_settings(settings), sort_keys=True, indent=2) + "\n").encode("utf-8")


def load_policy(control: Path) -> dict[str, Any]:
    policy = read_json(control / "policy.json")
    path = control.parent / "current/app/auto-update-settings.json"
    _assert_no_reparse_components(path)
    raw = path.read_bytes()
    canonical = canonical_startup_settings(json.loads(raw))
    digest = hashlib.sha256(canonical).hexdigest()
    if raw != canonical or digest != _sha(policy.get("settings_sha256"), length=64):
        raise ValueError("installed startup settings differ from their protected canonical SHA")
    if policy_from_settings({k: policy[k] for k in ("feed_url", "channel", "apply_during_work", "profile_auth")}) != json.loads(canonical):
        raise ValueError("installed startup policy differs from its packet settings")
    return policy


@writer_sink("auto_update_write_json_durable")
def write_json_durable(path: Path, payload: Mapping[str, Any]) -> None:
    """Write-through replacement keeps the journal ahead of the active pointer."""
    import ctypes
    from ctypes import wintypes

    # failure-<uuid>.json is a diagnostic snapshot, never a recovery input.
    if re.fullmatch(r"failure-[0-9a-f]{32}\.json", path.name):
        from failure_redaction import redact_json_for_sink
        serialized = json.dumps(redact_json_for_sink(dict(payload)), sort_keys=True,
                                separators=(",", ":"), ensure_ascii=True) + "\n"
    else:
        from failure_redaction import assert_machine_metadata_text
        serialized = json.dumps(dict(payload), sort_keys=True, separators=(",", ":"),
                                ensure_ascii=True) + "\n"
        assert_machine_metadata_text(serialized)
    _assert_no_reparse_components(path)
    temporary = path.with_name(path.name + "." + uuid.uuid4().hex + ".tmp")
    with temporary.open("x", encoding="utf-8", newline="\n") as output:
        output.write(serialized)
        output.flush()
        os.fsync(output.fileno())
    if os.name == "nt":
        move = ctypes.WinDLL("kernel32", use_last_error=True).MoveFileExW
        move.argtypes = [wintypes.LPCWSTR, wintypes.LPCWSTR, wintypes.DWORD]
        move.restype = wintypes.BOOL
        if not move(str(temporary), str(path), 0x1 | 0x8):
            raise OSError(ctypes.get_last_error(), "update write-through replacement failed")
    else:
        os.replace(temporary, path)


def control_root(app_root: Path) -> Path | None:
    code = app_root.absolute()
    if code.name != "app":
        return None
    bundle = code.parent
    if bundle.name == "current":
        managed = bundle.parent
    elif bundle.parent.name == "versions" and re.fullmatch(r"[0-9a-f]{40}", bundle.name):
        managed = bundle.parent.parent
    else:
        return None
    policy = managed / "update-control" / "policy.json"
    if not policy.is_file():
        return None
    _assert_no_reparse_components(policy)
    return policy.parent


def version_bundle(control: Path, sha: str) -> Path:
    return control.parent / "versions" / _sha(sha)


_VERSION_WRITER_INVENTORY_PASS = {}


def version_writer_inventory_sha256(bundle: Path) -> str:
    from writer_sink_inventory import _runtime_files
    from writer_session_fence import remote_request
    selected = bundle.absolute()
    def inputs():
        paths = [*_runtime_files(bundle / "app", ".py"), *_runtime_files(bundle / "app", ".ps1"),
                 bundle / "INSTALL_THIS_PC.ps1"]
        result = []
        for path in sorted(paths):
            _assert_no_reparse_components(path)
            result.append((path.relative_to(bundle).as_posix(), path.read_bytes()))
        return tuple(result)
    with remote_request():
        observed = inputs()
        cached = _VERSION_WRITER_INVENTORY_PASS.get(selected)
        if cached is None or cached[0] != observed:
            cached = next((entry for entry in _VERSION_WRITER_INVENTORY_PASS.values() if entry[0] == observed), None)
        if cached is not None and cached[0] == observed:
            _VERSION_WRITER_INVENTORY_PASS[selected] = cached
            return cached[1]
        result = _version_writer_inventory_sha256(bundle)
        if inputs() != observed:
            raise ValueError("writer inventory changed during verification")
        _VERSION_WRITER_INVENTORY_PASS[selected] = (observed, result)
        return result


def _version_writer_inventory_sha256(bundle: Path) -> str:
    # Run the already installed scanner over bytes/AST only. Candidate modules
    # are never imported by the protected updater.
    from writer_sink_inventory import derive_writer_sink_inventory, _powershell_inventory, writer_sink_inventory_sha256
    rows = derive_writer_sink_inventory(bundle / "app")
    rows += _powershell_inventory(bundle / "INSTALL_THIS_PC.ps1", bundle)
    rows.sort(key=lambda row: (row.source, row.source_path.casefold(), row.source_line, row.qualified_name))
    return writer_sink_inventory_sha256(rows)


def version_manifest(bundle: Path) -> dict[str, Any]:
    def read_bytes(name):
        path = bundle / name
        _assert_no_reparse_components(path)
        return path.read_bytes()
    value = version_identity(read_bytes)
    if type(value.get("startup_update_protocol")) is not int or value["startup_update_protocol"] != 1:
        raise ValueError("version has no supported startup readiness protocol")
    _sha(value.get("source_commit"))
    _sha(value.get("writer_sink_inventory_contract_sha256"), length=64)
    if version_writer_inventory_sha256(bundle) != value["writer_sink_inventory_contract_sha256"]:
        raise ValueError("version writer inventory differs from actual source bytes")
    return value


def installation_root(code_root: Path | None = None) -> Path | None:
    root = (code_root or Path(__file__).resolve().parent).absolute()
    if root.name.casefold() == "app":
        root = root.parent
    if root.name.casefold() == "current":
        return root.parent
    if root.parent.name.casefold() == "versions" and re.fullmatch(r"[0-9a-f]{40}", root.name):
        return root.parent.parent
    return None


def assert_registration_storage(code_root: Path, paths) -> None:
    """Check storage before registration can write into machine-owned code."""
    managed = installation_root(code_root)
    roots = [Path("C:/KMTech/Apps/Label_Match")]
    if managed is not None and managed not in roots:
        roots.append(managed)
    for value in paths:
        candidate = Path(value).expanduser().absolute()
        _assert_no_reparse_components(candidate)
        for machine in roots:
            for slot in ("current", "versions", "update-control", "removal-evidence", "removed.json"):
                boundary = machine / slot
                if candidate == boundary or candidate.is_relative_to(boundary) or boundary.is_relative_to(candidate):
                    raise ValueError(f"등록 자료 경로가 코드 또는 control 영역과 겹칩니다: {candidate}")


def lifecycle_task_observation(root: Path) -> dict[str, Any]:
    """External supported stock can observe task absence after code removal."""
    import subprocess
    helper = Path(__file__).resolve().parent.parent / "tools/apply_prepared_update.ps1"
    completed = subprocess.run([system_executable("WindowsPowerShell/v1.0/powershell.exe"),
        "-NoLogo", "-NoProfile", "-NonInteractive", "-WindowStyle", "Hidden", "-ExecutionPolicy", "Bypass", "-File", str(helper),
        "-InstallRoot", str(root / "current"), "-Mode", "ObserveLifecycle", "-SupportedLifecycle"],
        capture_output=True, text=True, creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0))
    if completed.returncode:
        error = RuntimeError("supported task observation failed")
        error.child_output = child_failure_output(completed)
        raise error
    return json.loads(completed.stdout)


def _supported_removal_artifacts(root: Path) -> list[Path]:
    result = []
    for name in ("versions", "update-control"):
        parent = root / name
        if not (parent.exists() or parent.is_symlink() or parent.is_junction()):
            continue
        if parent.is_symlink() or parent.is_junction() or not parent.is_dir():
            result.append(parent)
            continue
        for child in parent.iterdir():
            if name == "versions":
                if re.fullmatch(r"[0-9a-f]{40}", child.name):
                    result.append(child)
            elif child.name in {"policy.json", "active.json", "transition.json"} or re.fullmatch(
                    r"(?:packet|failure|policy-before)-[0-9a-f]{32}(?:\.json)?", child.name):
                result.append(child)
    try:
        marker = read_json(root / "removed.json")
        if marker.get("schema") == "label-match-supported-removal-v1" and marker.get("lifecycle_state") == "REMOVED":
            retained = marker.get("retained_paths", ())
            if isinstance(retained, list):
                result = [path for path in result if not any(isinstance(value, str)
                    and (path.is_relative_to(root / "versions") or path.name.startswith("packet-"))
                    and (Path(value) == path or Path(value).is_relative_to(path)) for value in retained)]
    except (OSError, ValueError):
        pass
    return result


def resolve_lifecycle_context(root: Path, *, expected_from_commit: str | None = None,
                              operation: str = "inspect") -> dict[str, Any]:
    if operation not in {"inspect", "install", "uninstall", "user_remove", "register"}:
        raise ValueError("supported lifecycle operation is malformed")
    fixed = root / "current"
    base = dict(status="PASS", fixed_root=str(fixed), owner_app_root=str(fixed / "app"),
                from_commit=None, active_code_root=None, active_app_root=None)
    if operation in {"uninstall", "user_remove"}:
        return {**base, "lifecycle_state": "REMOVAL_READY"}
    control = root / "update-control"
    for output in (fixed, root / "versions", control):
        _assert_no_reparse_components(output)
        if output.exists() and not output.is_dir():
            raise ValueError(f"설치가 만드는 경로와 남긴 항목이 겹칩니다. 자료를 보존하고 경로를 확인하세요: {output}")
    policy_path = control / "policy.json"
    if not fixed.exists() and not _supported_removal_artifacts(root):
        if expected_from_commit is not None or operation == "register":
            raise ValueError("supported bootstrap was removed")
        observation = lifecycle_task_observation(root)
        if observation.get("task_exists") is not False:
            raise ValueError("supported removal task absence is unproven")
        return {**base, "lifecycle_state": "REMOVED"}
    if not policy_path.is_file():
        raise ValueError(f"설치가 만드는 경로와 남긴 항목이 겹칩니다. 자료를 보존하고 경로를 확인하세요: {fixed}")
    _assert_no_reparse_components(fixed)
    if not fixed.is_dir():
        raise ValueError("supported bootstrap is absent")
    policy = load_policy(control)
    pointer = read_json(control / "active.json")
    state = read_json(control / "transition.json")
    if pointer.get("schema") != SCHEMA or state.get("schema") != SCHEMA:
        raise ValueError("supported control identity differs")
    active = _sha(pointer.get("sha"))
    if state.get("phase") not in {"CONFIRMED", "ROLLED_BACK", "DEFERRED"}:
        raise ValueError("supported transition requires removal and reinstall")
    if expected_from_commit is not None and _sha(expected_from_commit) != active:
        raise ValueError("supported from identity differs")
    if state.get("candidate_sha") is not None and recovery_sha(state, pointer_sha=active) != active:
        raise ValueError("supported transition and pointer differ")
    # The selected Verify already authenticates the fixed bootstrap in
    # Get-Authority / Assert-LabelStartupIntegrity before verifying the version.
    if operation != "register":
        fixed_helper(control, "Verify", code_root=fixed)
    fixed_identity = version_manifest(fixed)
    selected = version_bundle(control, active)
    fixed_helper(control, "Verify", code_root=selected)
    actual = version_manifest(selected)
    if actual["source_commit"] != active:
        raise ValueError("supported active code identity differs")
    integrity = read_json(fixed / "bootstrap-integrity.json")
    if lifecycle_task_observation(root).get("task_exists") is not True:
        raise ValueError("supported installed task is absent or unknown")
    if read_json(control / "active.json") != pointer or read_json(control / "transition.json") != state:
        raise ValueError("supported lifecycle changed during observation")
    return {**base, "lifecycle_state": "INSTALLED", "from_commit": active,
            "bootstrap_commit": fixed_identity["source_commit"], "phase": state["phase"],
            "active_code_root": str(selected), "active_app_root": str(selected / "app"),
            "bootstrap_aggregate_sha256": _sha(integrity.get("aggregate_sha256"), length=64)}


@writer_sink("auto_update_supported_lifecycle_transition")
def supported_lifecycle_transition(root: Path, *, expected_from_commit: str, candidate_commit: str) -> dict[str, Any]:
    from writer_session_fence import active_fence, canonical_control_root, _delegation_matches
    security = process_security(os.getpid())
    if security.get("elevated") is not True:
        raise ValueError("supported placement requires the elevated installer")
    fence = active_fence(canonical_control_root(), environ=os.environ)
    if fence is None or not _delegation_matches(fence, source="canonical_placement", environ=os.environ):
        raise ValueError("supported placement requires exact installer delegation")
    control = root / "update-control"
    with update_lock(control, load_policy(control)["operator_sid"]):
        context = resolve_lifecycle_context(root, expected_from_commit=expected_from_commit)
        candidate_commit = _sha(candidate_commit)
        control = root / "update-control"
        pointer = read_json(control / "active.json")
        state = read_json(control / "transition.json")
        target = version_bundle(control, candidate_commit)
        fixed_helper(control, "Verify", code_root=target)
        identity = version_manifest(target)
        if identity["source_commit"] != candidate_commit:
            raise ValueError("supported candidate identity differs")
        fixed = Path(context["fixed_root"])
        for name in ("update-manifest-key-config.json", "auto-update-settings.json"):
            if (target / "app" / name).read_bytes() != (fixed / "app" / name).read_bytes():
                raise ValueError("supported candidate trust/settings differ from fixed bootstrap")
        if read_json(control / "active.json") != pointer or read_json(control / "transition.json") != state:
            raise ValueError("supported lifecycle changed during placement")
        next_state = dict(schema=SCHEMA, phase="DEFERRED", failed_shas=list(state.get("failed_shas", [])),
                          deferred_at=now_utc(), reason="supported placement", transition_id=uuid.uuid4().hex)
        persist_switch(control, next_state, candidate_commit)
        return resolve_lifecycle_context(root, expected_from_commit=candidate_commit)


def lifecycle_context_main(arguments: list[str]) -> int:
    import argparse
    parser = __import__("failure_redaction").InstallerArgumentParser()
    parser.add_argument("--install-root", type=Path, required=True)
    parser.add_argument("--expected-from-commit")
    parser.add_argument("--operation", default="inspect")
    parser.add_argument("--activate-supported-commit")
    args = parser.parse_args(arguments)
    try:
        if args.install_root.absolute() != Path("C:/KMTech/Apps/Label_Match").absolute():
            raise ValueError("supported lifecycle requires the canonical installation")
        if args.activate_supported_commit:
            result = supported_lifecycle_transition(args.install_root,
                expected_from_commit=args.expected_from_commit, candidate_commit=args.activate_supported_commit)
        else:
            result = resolve_lifecycle_context(args.install_root,
                expected_from_commit=args.expected_from_commit, operation=args.operation)
    except Exception as error:
        from failure_redaction import emit_support_failure
        emit_support_failure(error, prefix="LABEL_MATCH_LIFECYCLE_FAILURE")
        return 1
    print(json.dumps(result, ensure_ascii=True))
    return 0


def verify_prepared_packet(
    stage: Path, *, key_config: str, current_version: str, channel: str,
) -> tuple[dict[str, Any], dict[str, Any]]:
    from Label_Match import (
        _sha256_file as file_sha256, _verify_update_manifest_signature as verify_update_manifest_signature,
        _update_candidate_from_manifest as update_candidate_from_private_manifest,
    )

    manifest = read_json(stage / "manifest.json")
    signature_path = stage / "manifest.sig"
    _assert_no_reparse_components(signature_path)
    if signature_path.stat().st_size > 16384:
        raise ValueError("update signature is oversized")
    verify_update_manifest_signature(manifest, signature_path.read_bytes(), key_config)
    candidate = update_candidate_from_private_manifest(manifest, current_version=current_version, expected_channel=channel)
    if candidate is None or candidate["archive"].get("layout") != "canonical_portable":
        raise ValueError("update is not a targeted canonical portable candidate")
    packet = stage / "packet.zip"
    _assert_no_reparse_components(packet)
    if packet.stat().st_size != manifest["artifact"]["size_bytes"] or file_sha256(packet) != candidate["sha256"]:
        raise ValueError("prepared packet size or SHA256 differs")
    _sha(manifest.get("source_commit"))
    return manifest, candidate


def task_name(app_root: Path, operator_sid: str) -> str:
    if not isinstance(operator_sid, str) or re.fullmatch(r"S-1-(?:[0-9]+-)+[0-9]+", operator_sid) is None:
        raise ValueError("update operator SID is malformed")
    path = os.path.normcase(str(app_root.absolute())).replace("/", "\\")
    identity = hashlib.sha256((path + "\n" + operator_sid).encode("utf-8")).hexdigest()[:24]
    return "KMTech.LabelMatch.StartupUpdate." + identity


def logon_task_xml(*, app_root: Path, operator_sid: str) -> str:
    """Generate only a fixed protected helper action, with a SID logon trigger."""
    from xml.sax.saxutils import escape

    task_name(app_root, operator_sid)
    root = str(app_root.absolute())
    if any(character in root for character in ('"', "\r", "\n")):
        raise ValueError("update install path is unsafe")
    arguments = '-NoLogo -NoProfile -NonInteractive -WindowStyle Hidden -ExecutionPolicy Bypass -File "' + root + '\\tools\\apply_prepared_update.ps1" -InstallRoot "' + root + '"'
    executable = system_executable("WindowsPowerShell/v1.0/powershell.exe")
    return (
        '<?xml version="1.0" encoding="UTF-16"?>'
        '<Task version="1.4" xmlns="http://schemas.microsoft.com/windows/2004/02/mit/task">'
        '<Triggers><LogonTrigger><Enabled>true</Enabled><UserId>' + escape(operator_sid) + '</UserId></LogonTrigger></Triggers>'
        '<Principals><Principal id="System"><UserId>S-1-5-18</UserId><RunLevel>HighestAvailable</RunLevel></Principal></Principals>'
        '<Settings><MultipleInstancesPolicy>IgnoreNew</MultipleInstancesPolicy><DisallowStartIfOnBatteries>false</DisallowStartIfOnBatteries><StopIfGoingOnBatteries>false</StopIfGoingOnBatteries><AllowHardTerminate>false</AllowHardTerminate><StartWhenAvailable>true</StartWhenAvailable><AllowStartOnDemand>true</AllowStartOnDemand><Hidden>true</Hidden><ExecutionTimeLimit>PT0S</ExecutionTimeLimit></Settings>'
        '<Actions Context="System"><Exec><Command>' + escape(executable) + '</Command><Arguments>' + escape(arguments) + '</Arguments></Exec></Actions></Task>'
    )


def install_task_spec(app_root: Path, operator_sid: str) -> dict[str, str]:
    return {"name": task_name(app_root, operator_sid), "xml": logon_task_xml(app_root=app_root, operator_sid=operator_sid)}


def now_utc() -> str:
    return datetime.now(timezone.utc).isoformat()


def system_executable(relative: str) -> str:
    import ctypes
    from ctypes import wintypes

    if os.name != "nt":
        raise OSError("startup updates require Windows")
    kernel = ctypes.WinDLL("kernel32", use_last_error=True)
    get_directory = kernel.GetSystemDirectoryW
    get_directory.argtypes = [wintypes.LPWSTR, wintypes.UINT]
    get_directory.restype = wintypes.UINT
    buffer = ctypes.create_unicode_buffer(32768)
    size = get_directory(buffer, len(buffer))
    if not size or size >= len(buffer):
        raise OSError(ctypes.get_last_error(), "Windows system directory is unobservable")
    return str(Path(buffer.value) / relative)


def optional_record(path: Path) -> dict[str, Any] | None:
    try:
        return read_json(path)
    except FileNotFoundError:
        return None


def prepared_marker_sha256(policy: Mapping[str, Any] | None) -> str | None:
    if not policy or not policy.get("staging_root"):
        return None
    try:
        path = Path(policy["staging_root"]) / "prepared.json"
        _assert_no_reparse_components(path)
        with path.open("rb") as stream:
            return hashlib.file_digest(stream, "sha256").hexdigest()
    except (OSError, ValueError, WriterFenceError):
        return None


@contextmanager
def update_lock(control: Path, operator_sid: str, *, trial: bool = False, abandoned_check=None):
    """One cross-session mutex shared by SYSTEM and the installed operator SID."""
    import ctypes
    from ctypes import wintypes

    task_name(control.parent / "current", operator_sid)
    kernel = ctypes.WinDLL("kernel32", use_last_error=True)
    advapi = ctypes.WinDLL("advapi32", use_last_error=True)
    class Attributes(ctypes.Structure):
        _fields_ = [("length", wintypes.DWORD), ("descriptor", wintypes.LPVOID), ("inherit", wintypes.BOOL)]
    descriptor = wintypes.LPVOID()
    convert = advapi.ConvertStringSecurityDescriptorToSecurityDescriptorW
    convert.argtypes = [wintypes.LPCWSTR, wintypes.DWORD, ctypes.POINTER(wintypes.LPVOID), ctypes.POINTER(wintypes.DWORD)]
    convert.restype = wintypes.BOOL
    sddl = "D:P(A;;GA;;;SY)(A;;GA;;;BA)(A;;0x00100001;;;" + operator_sid + ")"
    if not convert(sddl, 1, ctypes.byref(descriptor), None):
        raise OSError(ctypes.get_last_error(), "update mutex ACL conversion failed")
    create = kernel.CreateMutexExW
    create.argtypes = [ctypes.POINTER(Attributes), wintypes.LPCWSTR, wintypes.DWORD, wintypes.DWORD]
    create.restype = wintypes.HANDLE
    kernel.WaitForSingleObject.argtypes = [wintypes.HANDLE, wintypes.DWORD]
    kernel.WaitForSingleObject.restype = wintypes.DWORD
    kernel.ReleaseMutex.argtypes = [wintypes.HANDLE]
    kernel.CloseHandle.argtypes = [wintypes.HANDLE]
    kernel.LocalFree.argtypes = [wintypes.HLOCAL]
    identity = hashlib.sha256(os.path.normcase(str(control.absolute())).encode("utf-8")).hexdigest()[:24]
    attributes = Attributes(ctypes.sizeof(Attributes), descriptor, False)
    try:
        handle = create(ctypes.byref(attributes), "Global\\KMTech.LabelMatch.Update." + identity + (".Trial" if trial else ""), 0, 0x00100001)
    finally:
        kernel.LocalFree(descriptor)
    if not handle:
        raise OSError(ctypes.get_last_error(), "update mutex cannot be opened")
    acquired = False
    try:
        result = kernel.WaitForSingleObject(handle, 5000)
        acquired = result in {0, 0x80}
        if result == 0x80:
            if abandoned_check is None or trial:
                raise RuntimeError("update mutex ownership was abandoned")
            # WAIT_ABANDONED grants ownership. Keep it until the caller has
            # proved the durable state and installed bytes, before yielding.
            abandoned_check()
        if result == 0x102:
            raise RuntimeError("update mutex ownership is busy")
        if result not in {0, 0x80}:
            raise OSError(ctypes.get_last_error(), "update mutex wait failed")
        yield
    finally:
        if acquired:
            kernel.ReleaseMutex(handle)
        kernel.CloseHandle(handle)


@writer_sink("auto_update_fixed_helper")
def fixed_helper(control: Path, mode: str, *, code_root: Path | None = None) -> str:
    if mode == "Verify":
        from writer_session_fence import remote_request
        with remote_request():
            return _run_fixed_helper(control, mode, code_root=code_root)
    return _run_fixed_helper(control, mode, code_root=code_root)


def _run_fixed_helper(control: Path, mode: str, *, code_root: Path | None = None) -> str:
    import subprocess

    root = control.parent / "current"
    executable = system_executable("WindowsPowerShell/v1.0/powershell.exe")
    arguments = [str(executable), "-NoLogo", "-NoProfile", "-NonInteractive", "-WindowStyle", "Hidden", "-ExecutionPolicy", "Bypass", "-File", str(root / "tools/apply_prepared_update.ps1"), "-InstallRoot", str(root), "-Mode", mode]
    if code_root is not None:
        arguments.extend(["-CodeRoot", str(code_root)])
    completed = subprocess.run(arguments, capture_output=True, text=True, creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0))
    if completed.returncode:
        error = RuntimeError("fixed update helper failed: " + mode + "; " + safe_update_text(completed.stderr))
        error.child_output = child_failure_output(completed)
        from failure_redaction import try_write_failure_evidence
        error.cause_code = "INSTALLER_FAILED"
        reference = try_write_failure_evidence(json.dumps({
            "stage": "FIXED_UPDATE_HELPER", "mode": mode, "child_output": error.child_output,
        }, ensure_ascii=False))
        error._failure_report_ref = ({key: reference[key] for key in ("path", "sha256")}
                                     if "path" in reference and "sha256" in reference else None)
        raise error
    return completed.stdout.strip()


def process_snapshot(control: Path) -> list[dict[str, Any]]:
    # Observe only lists processes (apply_prepared_update.ps1 exits before any
    # Apply/Verify step) and mutates no product state, so it takes no writer
    # admission: a live relay holds admission for its whole cycle.
    rows = json.loads(_run_fixed_helper(control, "Observe"))
    if not isinstance(rows, list):
        raise ValueError("update process observation is incomplete")
    return rows


def writer_processes(rows: list[Mapping[str, Any]], *, own_pid: int) -> list[Mapping[str, Any]]:
    writers = []
    for row in rows:
        if row.get("pid") == own_pid:
            continue
        if row.get("bootstrap") is True:
            continue
        if row.get("observable") is not True:
            raise ValueError("an application process cannot be observed")
        writers.append(row)
    return writers


def process_matches_trial(row: Mapping[str, Any], *, state: Mapping[str, Any], sha: str, sid: str, role: str) -> bool:
    if role == "gui" and state.get("trial_pid") == row.get("pid") and state.get("trial_started_at"):
        if _utc(row.get("created_at")) != _utc(state["trial_started_at"]):
            return False
    return (
        row.get("observable") is True and row.get("sha") == sha
        and row.get("sid") == sid and row.get("role") == role
        and type(row.get("pid")) is int and row["pid"] > 0
        and row.get("ordinary_user") is True
        and _utc(row.get("created_at")) >= _utc(state["applied_at"] if role == "gui" and state["phase"] == "APPLIED_PENDING_READY" else state.get("rollback_at", state["applied_at"]))
    )


@writer_sink("auto_update_persist_switch")
def persist_switch(control: Path, state: Mapping[str, Any], sha: str) -> None:
    # An interrupted pointer write is repaired from this already-flushed record.
    write_json_durable(control / "transition.json", state)
    write_json_durable(control / "active.json", {"schema": SCHEMA, "sha": _sha(sha), "transition_id": state["transition_id"]})
    pointer = read_json(control / "active.json")
    if pointer.get("sha") != sha or pointer.get("transition_id") != state["transition_id"]:
        raise ValueError("active pointer readback differs from durable decision")


def record_confirmed_transition(control: Path, state: Mapping[str, Any]) -> dict[str, Any]:
    """Record reverse authority only after durable GUI confirmation."""
    if state.get("phase") != "CONFIRMED" or state.get("confirmation_audit_applied") is True:
        return dict(state)
    pair = state.get("rollback_pair")
    if pair is None:
        return dict(state)
    directional_pin([pair], **pair)
    if pair["from_commit"] != state["candidate_sha"] or pair["to_commit"] != state["previous_sha"]:
        raise ValueError("confirmed reverse audit is bound to another transition")
    settled = {**state, "confirmation_audit_applied": True}
    write_json_durable(control / "transition.json", settled)
    return settled


def native_process_identity(pid: int) -> dict[str, Any]:
    """Observe process lifetime without treating inaccessible owners as absent."""
    import ctypes
    from ctypes import wintypes

    if not isinstance(pid, int) or isinstance(pid, bool) or pid <= 0:
        return {"status": "UNKNOWN"}
    kernel = ctypes.WinDLL("kernel32", use_last_error=True)
    kernel.OpenProcess.argtypes = [wintypes.DWORD, wintypes.BOOL, wintypes.DWORD]
    kernel.OpenProcess.restype = wintypes.HANDLE
    kernel.WaitForSingleObject.argtypes = [wintypes.HANDLE, wintypes.DWORD]
    kernel.WaitForSingleObject.restype = wintypes.DWORD
    kernel.GetProcessTimes.argtypes = [wintypes.HANDLE] + [ctypes.POINTER(wintypes.FILETIME)] * 4
    kernel.GetProcessTimes.restype = wintypes.BOOL
    kernel.CloseHandle.argtypes = [wintypes.HANDLE]
    handle = kernel.OpenProcess(0x1000 | 0x00100000, False, pid)
    if not handle:
        return {"status": "ABSENT" if ctypes.get_last_error() == 87 else "UNKNOWN"}
    try:
        wait = kernel.WaitForSingleObject(handle, 0)
        if wait == 0:
            return {"status": "ABSENT"}
        if wait != 258:
            return {"status": "UNKNOWN"}
        times = [wintypes.FILETIME() for _ in range(4)]
        if not kernel.GetProcessTimes(handle, *[ctypes.byref(value) for value in times]):
            return {"status": "UNKNOWN"}
        identity = (times[0].dwHighDateTime << 32) | times[0].dwLowDateTime
        return {"status": "LIVE", "creation_identity": str(identity)}
    finally:
        kernel.CloseHandle(handle)


def new_apply_claim() -> dict[str, Any]:
    owner = native_process_identity(os.getpid())
    if owner["status"] != "LIVE":
        raise ValueError("apply owner creation identity is unobservable")
    return {"schema": "code-update-apply-claim-v1", "status": "RUNNING",
            "pid": os.getpid(), "creation_identity": owner["creation_identity"],
            "started_at": now_utc()}


def apply_claim_status(control: Path) -> str:
    """A protected claim is progress evidence, never pointer or READY authority."""
    try:
        claim = optional_record(control / "apply-claim.json")
        if claim is None:
            return "NONE"
        if claim.get("schema") != "code-update-apply-claim-v1":
            return "UNKNOWN"
        if claim.get("status") == "SETTLED":
            return "NONE"
        if claim.get("status") != "RUNNING" or not isinstance(claim.get("creation_identity"), str):
            return "UNKNOWN"
        owner = native_process_identity(claim.get("pid"))
        if owner["status"] != "LIVE":
            return owner["status"]
        return "LIVE" if owner["creation_identity"] == claim["creation_identity"] else "ABSENT"
    except (OSError, ValueError, TypeError, KeyError):
        return "UNKNOWN"


def complete_apply_claim(control: Path) -> None:
    """Durably retire only this process's claim through the existing writer sink."""
    try:
        claim = optional_record(control / "apply-claim.json")
        if not claim or claim.get("pid") != os.getpid() or claim.get("status") != "RUNNING":
            return
        owner = native_process_identity(os.getpid())
        if owner.get("creation_identity") != claim.get("creation_identity"):
            return
        write_json_durable(control / "apply-claim.json", {**claim, "status": "SETTLED", "completed_at": now_utc()})
    except (OSError, ValueError, TypeError, KeyError):
        emit_support_record({"reason_code": "REPORT_UNAVAILABLE"}, stage="update_apply")


def emit_progress_guidance() -> None:
    report = support_record({"reason_code": "UPDATE_IN_PROGRESS"}, stage="update_apply")
    report["guidance"] = STARTUP_PROGRESS_MESSAGE
    print(json.dumps(report, ensure_ascii=False, sort_keys=True))


def apply_prepared(control: Path) -> int:
    """Preserve a failed preparation; never fall back after an uncertain switch."""
    from contextlib import nullcontext

    policy, before = None, None
    try:
        fixed_helper(control, "Authority")
        policy = load_policy(control)
        before = read_json(control / "active.json")
        return _apply_prepared(control)
    except Exception as error:
        with update_lock(control, policy["operator_sid"]) if policy else nullcontext():
            state = optional_record(control / "transition.json")
            # Authority/policy/active failures are also preparation failures.
            # An in-flight journal remains UNKNOWN until a later SYSTEM decide.
            not_applied = (not state or state.get("phase") in {"CONFIRMED", "ROLLED_BACK", "DEFERRED", "PREPARED"})
            if before is not None:
                try:
                    not_applied = not_applied and read_json(control / "active.json") == before
                except (OSError, ValueError):
                    not_applied = False
            failure = {**(state or {}), "schema": SCHEMA,
                       "phase": "DEFERRED" if not_applied else "UNKNOWN",
                       "deferred_at": now_utc(), "reason_code": "UPDATE_PREPARATION_FAILED",
                       "reason": type(error).__name__, "launch_failure_details": launch_failure_details(error, include_original=True)}
            marker_sha256 = prepared_marker_sha256(policy)
            if not_applied and marker_sha256:
                if (state or {}).get("failed_prepared_sha256") == marker_sha256 and (state or {}).get("phase") == "DEFERRED":
                    return 0
                failure["failed_prepared_sha256"] = marker_sha256
                try:
                    marker = read_json(Path(policy["staging_root"]) / "prepared.json")
                    failed_sha = _sha(marker.get("source_commit"))
                    failure["failed_shas"] = sorted(set(failure.get("failed_shas", [])) | {failed_sha})
                except (OSError, ValueError, TypeError):
                    pass  # Malformed hints are quarantined by their exact bytes.
            write_json_durable(control / ("failure-" + uuid.uuid4().hex + ".json"), failure)
            write_json_durable(control / "transition.json", failure)
        return 0 if not_applied else 20

    finally:
        complete_apply_claim(control)


def settle_trial_failure(control: Path, state: Mapping[str, Any], policy: Mapping[str, Any]) -> dict[str, Any]:
    """SYSTEM only, with writer absence and the update lock already proved."""
    state = decide_trial_failure({**state, "phase": "ROLLBACK_PENDING",
                                  "recovery_pending": False}, policy)
    write_json_durable(control / "transition.json", state)
    selected = state["failure_decision"]["selected_sha"]
    fixed_helper(control, "Verify", code_root=version_bundle(control, selected))
    if selected == state["previous_sha"]:
        state = rollback_transition(state, writers_absent=True, previous_verified=True,
                                    durable_rollback=True, nonce=uuid.uuid4().hex, now=now_utc())
    persist_switch(control, state, selected)
    return state



def authorize_prepared_replay(control: Path, policy: Mapping[str, Any], state: Mapping[str, Any]) -> None:
    """Reauthenticate a durable PREPARED record after an interrupted apply."""
    active = read_json(control / "active.json")
    if active.get("sha") != state.get("previous_sha"):
        raise ValueError("prepared replay differs from the active pointer")
    protected = Path(state["prepared_packet_root"])
    if (protected.parent != control or re.fullmatch(r"packet-[0-9a-f]{32}", protected.name) is None):
        raise ValueError("prepared replay packet is outside protected storage")
    previous = version_bundle(control, state["previous_sha"])
    target = version_bundle(control, state["candidate_sha"])
    for bundle in (previous, target):
        fixed_helper(control, "Verify", code_root=bundle)
    current, candidate = version_manifest(previous), version_manifest(target)
    key = read_json(control.parent / "current/app/update-manifest-key-config.json")["manifest_public_key"]
    manifest, packet = verify_prepared_packet(protected, key_config=key,
        current_version=current["version"], channel=policy["channel"])
    if packet["sha256"] != state["packet_sha256"]:
        raise ValueError("prepared replay packet differs")
    pins = authorize_feed_transition(manifest, current=current, candidate=candidate)
    if pins[:2] != (state["forward_pin"], state["rollback_pin"]):
        raise ValueError("prepared replay direction differs")
    if candidate["source_commit"] in state.get("failed_shas", []):
        raise ValueError("a failed version must not be reapplied")


def _apply_prepared(control: Path) -> int:
    """SYSTEM applies code only; the operator bootstrap launches the trial."""
    import time
    from Label_Match import _sha256_file as file_sha256

    opened_here = False
    fixed_helper(control, "Authority")
    policy = load_policy(control)
    # SYSTEM must never resolve user credentials or start a user process.
    os.environ["LOCALAPPDATA"] = policy["operator_local_app_data"]
    stage = Path(policy["staging_root"])
    with update_lock(control, policy["operator_sid"]):
        state = optional_record(control / "transition.json")
        if (state and state.get("phase") == "DEFERRED" and state.get("reason_code") == "UPDATE_PREPARATION_FAILED"
                and not state.get("recovery_pending") and not prepared_pending(control, policy, state)):
            active_sha = read_json(control / "active.json")["sha"]
            if not state.get("retained_candidate") or recovery_sha(state, pointer_sha=active_sha) == active_sha:
                return 0
        write_json_durable(control / "apply-claim.json", new_apply_claim())
        if state and state["phase"] == "CONFIRMED":
            state = record_confirmed_transition(control, state)
        if state and state.get("recovery_pending") is True:
            if writer_processes(process_snapshot(control), own_pid=os.getpid()):
                return 0  # Pending recovery never admits a new prepared packet.
            state = settle_trial_failure(control, state, policy)
        if state and state["phase"] in {"APPLIED_PENDING_READY", "UNKNOWN", "FAILED"}:
            rows = process_snapshot(control)
            writers = writer_processes(rows, own_pid=os.getpid())
            claim = optional_record(stage / "trial.json")
            bound = state.get("trial_pid")
            if (not bound and claim and claim.get("nonce") == state.get("nonce")
                    and claim.get("transition_id") == state.get("transition_id")):
                bound = claim.get("pid")
            trial_live = state["phase"] == "APPLIED_PENDING_READY" and any(
                row.get("pid") == bound and process_matches_trial(row, state=state,
                    sha=state["candidate_sha"], sid=policy["operator_sid"], role="gui") for row in writers)
            if writers and not trial_live:
                # A later SYSTEM invocation never stops an ordinary active writer
                # or upgrades its unconfirmed trial to CONFIRMED.
                write_json_durable(control / "transition.json", {**state, "phase": "DEFERRED",
                    "retained_candidate": False, "deferred_at": now_utc(), "reason_code": "UPDATE_EXISTING_WRITER",
                    "recovery_pending": True, "recovery_phase": state["phase"],
                    "recovery_reason_code": state.get("reason_code")})
                return 0
        if state and state["phase"] in {"UNKNOWN", "FAILED", "ROLLBACK_PENDING_READY"}:
            if writer_processes(process_snapshot(control), own_pid=os.getpid()):
                return 20
            state = settle_trial_failure(control, state, policy)
        if state and state.get("retained_candidate") is True and state["phase"] == "DEFERRED":
            selected = recovery_sha(state, pointer_sha=read_json(control / "active.json")["sha"])
            if read_json(control / "active.json")["sha"] != selected:
                fixed_helper(control, "Verify", code_root=version_bundle(control, selected))
                persist_switch(control, state, selected)
                return 20
        if state is None or state["phase"] in {"CONFIRMED", "ROLLED_BACK", "DEFERRED"}:
            if not prepared_pending(control, policy, state):
                return 0
            if writer_processes(process_snapshot(control), own_pid=os.getpid()):
                deferred = {**(state or {}), "schema": SCHEMA, "phase": "DEFERRED", "deferred_at": now_utc(), "reason": "an existing application writer continues working"}
                write_json_durable(control / "transition.json", deferred)
                return 0  # A late task defers; it never stops an existing application.
            active = read_json(control / "active.json")
            fixed_helper(control, "Verify", code_root=version_bundle(control, active["sha"]))
            current = version_manifest(version_bundle(control, active["sha"]))
            key = read_json(control.parent / "current/app/update-manifest-key-config.json")["manifest_public_key"]
            # Copy user-owned bytes, then authenticate the protected copy. No path,
            # executable or command supplied by the prepared marker is executed.
            protected = control / ("packet-" + uuid.uuid4().hex)
            create_protected_stage(protected)
            for name in ("manifest.json", "manifest.sig", "packet.zip", "prepared.json"):
                copy_prepared_file(stage / name, protected / name, expected_owner_sid=policy["operator_sid"])
            manifest, candidate = verify_prepared_packet(protected, key_config=key, current_version=current["version"], channel=policy["channel"])
            sha = manifest["source_commit"]
            if sha in (state or {}).get("failed_shas", []):
                return 0
            extracted = protected / "extracted"
            extract_prepared_code(protected / "packet.zip", extracted, candidate["archive"])
            target = extracted / "Label_Match"
            candidate_manifest = version_manifest(target)
            if candidate_manifest["source_commit"] != sha or candidate_manifest["version"] != manifest["version"]:
                raise ValueError("signed target SHA differs from the packet")
            if file_sha256(target / "app/update-manifest-key-config.json") != file_sha256(control.parent / "current/app/update-manifest-key-config.json"):
                raise ValueError("candidate signing key differs from the installed key")
            forward, rollback, rollback_pair, pin_authority, data_api_evidence = authorize_feed_transition(manifest, current=current, candidate=candidate_manifest)
            # The fixed helper validates the portable tree and writes its own
            # integrity record. It never imports or executes candidate code.
            fixed_helper(control, "Verify", code_root=target)
            target_bundle = version_bundle(control, sha)
            if target_bundle.exists():
                fixed_helper(control, "Verify", code_root=target_bundle)
            preserve_candidate_version(target, target_bundle)
            generation_before = read_trial_generation(policy)
            if generation_before.get("status") != "READ":
                write_json_durable(control / "transition.json", {**(state or {}), "schema": SCHEMA,
                    "phase": "DEFERRED", "deferred_at": now_utc(), "reason_code": "UPDATE_GENERATION_UNKNOWN"})
                return 0
            state = prepare_transition(previous_sha=active["sha"], candidate_sha=sha, packet_sha256=candidate["sha256"], forward_pin=forward, rollback_pin=rollback, transition_id=uuid.uuid4().hex, nonce=uuid.uuid4().hex, now=now_utc(), failed_shas=(state or {}).get("failed_shas", []))
            state = {**state, "rollback_pair": rollback_pair, "pin_authority": pin_authority, "policy_sha256": file_sha256(control / "policy.json"), "data_api_evidence": data_api_evidence, "prepared_packet_root": str(protected), "generation_before": generation_before, "generation_previous_release": current, "generation_candidate_release": candidate_manifest}
            write_json_durable(control / "transition.json", state)
            state = apply_transition(state, writers_absent=not writer_processes(process_snapshot(control), own_pid=os.getpid()), packet_verified=True, durable_prepared=True, now=now_utc())
            opened_here = True
            persist_switch(control, state, sha)
        else:
            pointer = read_json(control / "active.json")
            if state["phase"] == "PREPARED":
                if writer_processes(process_snapshot(control), own_pid=os.getpid()):
                    return 20
                authorize_prepared_replay(control, policy, state)
                fixed_helper(control, "Verify", code_root=version_bundle(control, state["candidate_sha"]))
                state = apply_transition(state, writers_absent=True, packet_verified=True, durable_prepared=True, now=now_utc())
                opened_here = True
                persist_switch(control, state, state["candidate_sha"])
            if state["phase"] == "ROLLBACK_PENDING":
                if writer_processes(process_snapshot(control), own_pid=os.getpid()):
                    return 20
                settled = settle_trial_failure(control, state, policy)
                return 20 if settled.get("retained_candidate") else 0
            sha = recovery_sha(state, pointer_sha=pointer["sha"])
            if pointer["sha"] != sha:
                if writer_processes(process_snapshot(control), own_pid=os.getpid()):
                    return 20
                persist_switch(control, state, sha)
    deadline = time.monotonic() + 120
    while time.monotonic() < deadline:
        with update_lock(control, policy["operator_sid"]):
            state = read_json(control / "transition.json")
            rows = process_snapshot(control)
            claim = None
            receipt = None
            launch_failure = None
            try:
                claim = optional_record(stage / "trial.json")
                receipt = optional_record(stage / "ready.json")
                launch_failure = optional_record(stage / "trial-failure.json")
            except OSError as error:
                # A contested user proof is unconfirmed; keep the original
                # journal deadline and normal stop/rollback decision alive.
                state = {**state, "last_ready_error": type(error).__name__,
                         "last_ready_error_details": launch_failure_details(error, include_original=True)}
                write_json_durable(control / "transition.json", state)
            sha = state["previous_sha"] if state["phase"] == "ROLLBACK_PENDING_READY" else state["candidate_sha"]
            if (launch_failure and launch_failure.get("nonce") == state["nonce"]
                    and launch_failure.get("transition_id") == state["transition_id"]
                    and launch_failure.get("sha") == sha):
                break  # The ordinary bootstrap stays alive to launch verified recovery.
            if state.get("trial_pid") is None and claim and claim.get("nonce") == state["nonce"] and claim.get("transition_id") == state["transition_id"]:
                row = next((row for row in rows if row.get("pid") == claim.get("pid")), None)
                if row and process_matches_trial(row, state=state, sha=sha, sid=policy["operator_sid"], role="gui"):
                    state = bind_trial(state, sha=sha, pid=row["pid"], ordinary_user=True, now=row["created_at"])
                    write_json_durable(control / "transition.json", state)
            gui = next((row for row in rows if row.get("pid") == state.get("trial_pid")), None)
            relays = [row for row in rows if process_matches_trial(row, state=state, sha=sha, sid=policy["operator_sid"], role="relay")]
            process_ok = bool(gui and process_matches_trial(gui, state=state, sha=sha, sid=policy["operator_sid"], role="gui"))
            if receipt and ready_matches(state, receipt, process_matches=process_ok, relay_matches=len(relays) == 1, now=now_utc()):
                confirmed = confirm_transition(state, receipt, process_matches=process_ok, relay_matches=True, now=now_utc())
                write_json_durable(control / "transition.json", confirmed)
                record_confirmed_transition(control, confirmed)
                return 0
            if state.get("trial_pid") and gui is None:
                break
        time.sleep(0.25)
    with update_lock(control, policy["operator_sid"]):
        state = read_json(control / "transition.json")
        if state["phase"] == "ROLLBACK_PENDING_READY":
            write_json_durable(control / "transition.json", {**state, "phase": "FAILED", "reason": "previous version readiness failed"})
            return 20
        if opened_here:
            state = request_rollback(state, reason="candidate startup failed or readiness timed out")
        else:
            if state["phase"] != "APPLIED_PENDING_READY":
                raise ValueError("rollback requires a pending trial")
            # An inherited window supplies no supervised failure evidence.
            state = {**state, "phase": "UNKNOWN"}
        write_json_durable(control / "transition.json", state)
    deadline = time.monotonic() + 60
    while time.monotonic() < deadline:
        with update_lock(control, policy["operator_sid"]):
            if not writer_processes(process_snapshot(control), own_pid=os.getpid()):
                settled = settle_trial_failure(control, state, policy)
                return 20 if settled.get("retained_candidate") else 0
        time.sleep(0.25)
    write_json_durable(control / "transition.json", {**state, "phase": "UNKNOWN", "reason": "candidate normal stop could not be proven"})
    return 20


@writer_sink("auto_update_create_protected_stage")
def create_protected_stage(path: Path) -> None:
    _assert_no_reparse_components(path)
    path.mkdir()


@writer_sink("auto_update_extract_prepared_code")
def extract_prepared_code(packet: Path, target: Path, archive_policy: Mapping[str, Any]) -> None:
    from Label_Match import _safe_extract_update_zip as safe_extract_update_zip
    import zipfile

    # A short SYSTEM control admission delegates only extraction, and releases
    # before the ordinary-user GUI/relay waits for readiness.
    with zipfile.ZipFile(packet) as archive:
        safe_extract_update_zip(archive, target, archive_policy=archive_policy)


@writer_sink("auto_update_copy_prepared_file")
def copy_prepared_file(source: Path, target: Path, *, expected_owner_sid: str | None = None) -> None:
    _copy_prepared_file(source, target, expected_owner_sid=expected_owner_sid)


def _copy_prepared_file(source: Path, target: Path, *, expected_owner_sid: str | None = None) -> None:
    import shutil

    _assert_no_reparse_components(source)
    _assert_no_reparse_components(target)
    with unredirected_source(source, expected_owner_sid=expected_owner_sid) as incoming:
        if os.fstat(incoming.fileno()).st_size > (512 * 1024 * 1024 if source.name == "packet.zip" else 1024 * 1024):
            raise ValueError("prepared input is oversized")
        with target.open("xb") as outgoing:
            shutil.copyfileobj(incoming, outgoing)
            outgoing.flush()
            os.fsync(outgoing.fileno())


@contextmanager
def unredirected_source(source: Path, *, expected_owner_sid: str | None = None):
    """Hold every parent and reject redirected/hardlinked SYSTEM input."""
    _assert_no_reparse_components(source)
    if os.name != "nt":
        with source.open("rb") as incoming:
            if os.fstat(incoming.fileno()).st_nlink != 1:
                raise ValueError("prepared input has multiple hardlinks")
            yield incoming
        return
    import ctypes
    import msvcrt
    from ctypes import wintypes

    class FileInformation(ctypes.Structure):
        _fields_ = [("attributes", wintypes.DWORD), ("created", wintypes.FILETIME), ("accessed", wintypes.FILETIME), ("written", wintypes.FILETIME), ("volume", wintypes.DWORD), ("size_high", wintypes.DWORD), ("size_low", wintypes.DWORD), ("links", wintypes.DWORD), ("index_high", wintypes.DWORD), ("index_low", wintypes.DWORD)]

    kernel = ctypes.WinDLL("kernel32", use_last_error=True)
    kernel.CreateFileW.argtypes = [wintypes.LPCWSTR, wintypes.DWORD, wintypes.DWORD, wintypes.LPVOID, wintypes.DWORD, wintypes.DWORD, wintypes.HANDLE]
    kernel.CreateFileW.restype = wintypes.HANDLE
    kernel.GetFileInformationByHandle.argtypes = [wintypes.HANDLE, ctypes.POINTER(FileInformation)]
    kernel.GetFileInformationByHandle.restype = wintypes.BOOL
    kernel.CloseHandle.argtypes = [wintypes.HANDLE]
    kernel.LocalFree.argtypes = [wintypes.HLOCAL]
    kernel.LocalFree.restype = wintypes.HLOCAL
    handles = []
    try:
        paths = [*reversed(source.absolute().parents), source.absolute()]
        for index, path in enumerate(paths):
            leaf = index == len(paths) - 1
            # Sharing neither write nor delete closes the check/open race for
            # the leaf and every ancestor, including directory reparse edits.
            handle = kernel.CreateFileW(str(path), 0x80000000 if leaf else 0x80, 1, None, 3, 0x00200000 | (0x02000000 if not leaf else 0x08000000), None)
            if handle in (None, ctypes.c_void_p(-1).value):
                raise OSError(ctypes.get_last_error(), "prepared input cannot be held read-only")
            handles.append(handle)
            info = FileInformation()
            if not kernel.GetFileInformationByHandle(handle, ctypes.byref(info)):
                raise OSError(ctypes.get_last_error(), "prepared input handle cannot be observed")
            if info.attributes & 0x400 or bool(info.attributes & 0x10) == leaf:
                raise ValueError("prepared input is redirected or has an unexpected type")
            if leaf and info.links != 1:
                raise ValueError("prepared input has multiple hardlinks")
        if expected_owner_sid is not None:
            advapi = ctypes.WinDLL("advapi32", use_last_error=True)
            advapi.GetSecurityInfo.argtypes = [wintypes.HANDLE, wintypes.DWORD, wintypes.DWORD, ctypes.POINTER(wintypes.LPVOID), ctypes.POINTER(wintypes.LPVOID), ctypes.POINTER(wintypes.LPVOID), ctypes.POINTER(wintypes.LPVOID), ctypes.POINTER(wintypes.LPVOID)]
            advapi.GetSecurityInfo.restype = wintypes.DWORD
            advapi.ConvertSidToStringSidW.argtypes = [wintypes.LPVOID, ctypes.POINTER(wintypes.LPWSTR)]
            owner, descriptor, text = wintypes.LPVOID(), wintypes.LPVOID(), wintypes.LPWSTR()
            try:
                if advapi.GetSecurityInfo(handles[-1], 1, 1, ctypes.byref(owner), None, None, None, ctypes.byref(descriptor)):
                    raise OSError("prepared input owner cannot be observed")
                if not advapi.ConvertSidToStringSidW(owner, ctypes.byref(text)) or text.value != expected_owner_sid:
                    raise ValueError("prepared input is not owned by the installed operator")
            finally:
                if text:
                    kernel.LocalFree(text)
                if descriptor:
                    kernel.LocalFree(descriptor)
        fd = msvcrt.open_osfhandle(int(handles[-1]), os.O_RDONLY | os.O_BINARY | getattr(os, "O_NOINHERIT", 0))
        handles.pop()  # The Python stream owns the leaf handle from here.
        with os.fdopen(fd, "rb") as incoming:
            yield incoming
    finally:
        for handle in reversed(handles):
            kernel.CloseHandle(handle)


@writer_sink("auto_update_preserve_candidate_version")
def preserve_candidate_version(source: Path, target: Path) -> None:
    _assert_no_reparse_components(target)
    if target.exists():
        from Label_Match import _sha256_file as file_sha256

        def inventory(root):
            values = {}
            for path in root.rglob("*"):
                _assert_no_reparse_components(path)
                if path.is_file() and path.name != "bootstrap-integrity.json":
                    values[path.relative_to(root).as_posix()] = file_sha256(path)
            return values
        if inventory(source) != inventory(target):
            raise ValueError("a version folder already has different bytes; preserve it for recovery")
        return  # Reuse only an independently verified, byte-identical version.
    os.rename(source, target)


def current_process_sid() -> str:
    import ctypes
    from ctypes import wintypes

    advapi = ctypes.WinDLL("advapi32", use_last_error=True)
    kernel = ctypes.WinDLL("kernel32", use_last_error=True)
    kernel.GetCurrentProcess.restype = wintypes.HANDLE
    kernel.CloseHandle.argtypes = [wintypes.HANDLE]
    kernel.LocalFree.argtypes = [wintypes.HLOCAL]
    advapi.OpenProcessToken.argtypes = [wintypes.HANDLE, wintypes.DWORD, ctypes.POINTER(wintypes.HANDLE)]
    advapi.GetTokenInformation.argtypes = [wintypes.HANDLE, ctypes.c_int, wintypes.LPVOID, wintypes.DWORD, ctypes.POINTER(wintypes.DWORD)]
    advapi.ConvertSidToStringSidW.argtypes = [wintypes.LPVOID, ctypes.POINTER(wintypes.LPWSTR)]
    token, needed, sid_text = wintypes.HANDLE(), wintypes.DWORD(), wintypes.LPWSTR()
    if not advapi.OpenProcessToken(kernel.GetCurrentProcess(), 8, ctypes.byref(token)):
        raise OSError("update operator token cannot be observed")
    try:
        advapi.GetTokenInformation(token, 1, None, 0, ctypes.byref(needed))
        if not 0 < needed.value < 65536:
            raise ValueError("update operator token is malformed")
        buffer = ctypes.create_string_buffer(needed.value)
        if not advapi.GetTokenInformation(token, 1, buffer, needed.value, ctypes.byref(needed)):
            raise OSError("update operator SID cannot be observed")
        sid = ctypes.cast(buffer, ctypes.POINTER(wintypes.LPVOID)).contents
        if not advapi.ConvertSidToStringSidW(sid, ctypes.byref(sid_text)):
            raise OSError("update operator SID cannot be formatted")
        return sid_text.value
    finally:
        if sid_text:
            kernel.LocalFree(sid_text)
        kernel.CloseHandle(token)


def _inspect_current_process_execution_context() -> dict[str, Any]:
    if os.name != "nt":
        raise ValueError("replacement lifecycle token inspection requires Windows")
    import ctypes
    from ctypes import wintypes

    token_query = 0x0008
    token_elevation = 20
    token_integrity_level = 25
    error_insufficient_buffer = 122
    process_token = wintypes.HANDLE()
    advapi32 = ctypes.WinDLL("advapi32", use_last_error=True)
    kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
    kernel32.GetCurrentProcess.restype = wintypes.HANDLE
    kernel32.CloseHandle.argtypes = [wintypes.HANDLE]
    kernel32.CloseHandle.restype = wintypes.BOOL
    advapi32.OpenProcessToken.argtypes = [
        wintypes.HANDLE,
        wintypes.DWORD,
        ctypes.POINTER(wintypes.HANDLE),
    ]
    advapi32.OpenProcessToken.restype = wintypes.BOOL
    advapi32.GetTokenInformation.argtypes = [
        wintypes.HANDLE,
        ctypes.c_int,
        wintypes.LPVOID,
        wintypes.DWORD,
        ctypes.POINTER(wintypes.DWORD),
    ]
    advapi32.GetTokenInformation.restype = wintypes.BOOL
    advapi32.GetSidSubAuthorityCount.argtypes = [wintypes.LPVOID]
    advapi32.GetSidSubAuthorityCount.restype = ctypes.POINTER(ctypes.c_ubyte)
    advapi32.GetSidSubAuthority.argtypes = [wintypes.LPVOID, wintypes.DWORD]
    advapi32.GetSidSubAuthority.restype = ctypes.POINTER(wintypes.DWORD)
    if not advapi32.OpenProcessToken(
        kernel32.GetCurrentProcess(), token_query, ctypes.byref(process_token)
    ):
        raise OSError(ctypes.get_last_error(), "OpenProcessToken failed")
    try:
        elevated = wintypes.DWORD()
        returned = wintypes.DWORD()
        if not advapi32.GetTokenInformation(
            process_token,
            token_elevation,
            ctypes.byref(elevated),
            ctypes.sizeof(elevated),
            ctypes.byref(returned),
        ):
            raise OSError(ctypes.get_last_error(), "TokenElevation readback failed")

        required = wintypes.DWORD()
        advapi32.GetTokenInformation(
            process_token,
            token_integrity_level,
            None,
            0,
            ctypes.byref(required),
        )
        if ctypes.get_last_error() != error_insufficient_buffer or required.value <= 0:
            raise OSError(
                ctypes.get_last_error(), "TokenIntegrityLevel sizing failed"
            )
        buffer = ctypes.create_string_buffer(required.value)
        if not advapi32.GetTokenInformation(
            process_token,
            token_integrity_level,
            buffer,
            required.value,
            ctypes.byref(required),
        ):
            raise OSError(
                ctypes.get_last_error(), "TokenIntegrityLevel readback failed"
            )

        class SidAndAttributes(ctypes.Structure):
            _fields_ = [("sid", wintypes.LPVOID), ("attributes", wintypes.DWORD)]

        sid = ctypes.cast(buffer, ctypes.POINTER(SidAndAttributes)).contents.sid
        count_pointer = advapi32.GetSidSubAuthorityCount(sid)
        if not count_pointer:
            raise OSError(ctypes.get_last_error(), "integrity SID count failed")
        count = count_pointer.contents.value
        if count <= 0:
            raise ValueError("integrity SID contains no subauthority")
        rid_pointer = advapi32.GetSidSubAuthority(sid, count - 1)
        if not rid_pointer:
            raise OSError(ctypes.get_last_error(), "integrity SID RID failed")
        rid = rid_pointer.contents.value
    finally:
        kernel32.CloseHandle(process_token)

    if 0x2000 <= rid < 0x3000:
        integrity = "MEDIUM"
    elif rid < 0x2000:
        integrity = "LOW"
    else:
        integrity = "HIGH"
    return {
        "status": "PASS",
        "token_elevated": bool(elevated.value),
        "integrity_level": integrity,
    }

def process_security(process_id: int) -> dict[str, Any]:
    """Read a process token without reading credentials or a profile store."""
    import ctypes
    from ctypes import wintypes

    kernel = ctypes.WinDLL("kernel32", use_last_error=True)
    advapi = ctypes.WinDLL("advapi32", use_last_error=True)
    kernel.OpenProcess.argtypes = [wintypes.DWORD, wintypes.BOOL, wintypes.DWORD]
    kernel.OpenProcess.restype = wintypes.HANDLE
    kernel.CloseHandle.argtypes = [wintypes.HANDLE]
    kernel.LocalFree.argtypes = [wintypes.HLOCAL]
    advapi.OpenProcessToken.argtypes = [wintypes.HANDLE, wintypes.DWORD, ctypes.POINTER(wintypes.HANDLE)]
    advapi.GetTokenInformation.argtypes = [wintypes.HANDLE, ctypes.c_int, wintypes.LPVOID, wintypes.DWORD, ctypes.POINTER(wintypes.DWORD)]
    advapi.ConvertSidToStringSidW.argtypes = [wintypes.LPVOID, ctypes.POINTER(wintypes.LPWSTR)]
    advapi.GetSidSubAuthorityCount.argtypes = [wintypes.LPVOID]
    advapi.GetSidSubAuthorityCount.restype = ctypes.POINTER(ctypes.c_ubyte)
    advapi.GetSidSubAuthority.argtypes = [wintypes.LPVOID, wintypes.DWORD]
    advapi.GetSidSubAuthority.restype = ctypes.POINTER(wintypes.DWORD)
    process = kernel.OpenProcess(0x1000, False, process_id)
    token, sid_text = wintypes.HANDLE(), wintypes.LPWSTR()
    if not process:
        raise OSError(ctypes.get_last_error(), "OpenProcess")
    try:
        if not advapi.OpenProcessToken(process, 8, ctypes.byref(token)):
            raise OSError(ctypes.get_last_error(), "OpenProcessToken")
        def information(kind):
            needed = wintypes.DWORD()
            advapi.GetTokenInformation(token, kind, None, 0, ctypes.byref(needed))
            if not 0 < needed.value <= 65536:
                raise ValueError("process token information sizing differs")
            value = ctypes.create_string_buffer(needed.value)
            if not advapi.GetTokenInformation(token, kind, value, needed, ctypes.byref(needed)):
                raise OSError(ctypes.get_last_error(), "GetTokenInformation:" + str(kind))
            return value
        user = information(1)
        sid = ctypes.cast(user, ctypes.POINTER(wintypes.LPVOID)).contents
        if not advapi.ConvertSidToStringSidW(sid, ctypes.byref(sid_text)):
            raise OSError(ctypes.get_last_error(), "ConvertSidToStringSidW")
        integrity = information(25)
        integrity_sid = ctypes.cast(integrity, ctypes.POINTER(wintypes.LPVOID)).contents
        count = advapi.GetSidSubAuthorityCount(integrity_sid)
        if not count or not count.contents.value:
            raise ValueError("process integrity SID differs")
        rid = advapi.GetSidSubAuthority(integrity_sid, count.contents.value - 1).contents.value
        return {"sid": sid_text.value,
                "session_id": ctypes.cast(information(12), ctypes.POINTER(wintypes.DWORD)).contents.value,
                "elevated": bool(ctypes.cast(information(20), ctypes.POINTER(wintypes.DWORD)).contents.value),
                "integrity_rid": rid}
    finally:
        if sid_text:
            kernel.LocalFree(sid_text)
        if token:
            kernel.CloseHandle(token)
        kernel.CloseHandle(process)


def safe_update_text(value: Any, *, limit: int = 300) -> str:
    """Only source-declared, input-free guidance may reach a public notice."""
    from failure_redaction import RULES
    text = str(value or "")
    return text if text in RULES["safe_messages"] else RULES["safe_messages"][0]


def child_failure_output(completed: Any) -> dict[str, Any]:
    from failure_redaction import redact_for_sink
    return {"returncode": completed.returncode,
            "stdout": redact_for_sink(getattr(completed, "stdout", "") or ""),
            "stderr": redact_for_sink(getattr(completed, "stderr", "") or "")}


def launch_failure_details(error: BaseException, *, include_original: bool = False) -> list[dict[str, Any]]:
    """Preserve each original cause and native call without emitting authority."""
    chain, seen = [], set()
    calls = {"GetShellWindow", "OpenProcess", "OpenProcess:shell", "OpenProcessToken", "OpenProcessToken:shell",
             "DuplicateTokenEx:primary", "CreateEnvironmentBlock", "CreateProcessWithTokenW", "GetExitCodeProcess",
             "ConvertSidToStringSidW"}
    while error is not None and id(error) not in seen and len(chain) < 16:
        seen.add(id(error))
        call = getattr(error, "call", "") or (error.strerror if isinstance(error, OSError) else "")
        if not isinstance(call, str) or (call not in calls and not re.fullmatch(r"GetTokenInformation:[0-9]+", call)):
            call = ""
        code = (getattr(error, "winerror", None) or error.errno) if isinstance(error, OSError) else None
        detail = {"type": type(error).__module__ + "." + type(error).__name__, "call": call,
                  "win32_error": code if type(code) is int else None}
        if include_original:
            from failure_redaction import try_write_failure_evidence
            import traceback
            detail["message"] = safe_update_text(str(error))
            detail["full_exception_trace"] = try_write_failure_evidence(
                "".join(traceback.TracebackException.from_exception(error).format()))
        output = getattr(error, "child_output", None)
        if include_original and isinstance(output, Mapping):
            detail["child_output"] = {"returncode": output.get("returncode"),
                                     "stdout_evidence": try_write_failure_evidence(output.get("stdout", "")),
                                     "stderr_evidence": try_write_failure_evidence(output.get("stderr", ""))}
        elif include_original and (getattr(error, "stdout", None) or getattr(error, "stderr", None)):
            detail["child_output"] = {"returncode": getattr(error, "returncode", None),
                                     "stdout_evidence": try_write_failure_evidence(getattr(error, "stdout", "")),
                                     "stderr_evidence": try_write_failure_evidence(getattr(error, "stderr", ""))}
        chain.append(detail)
        error = error.__cause__ or error.__context__
    from failure_redaction import public_diagnostic_tree
    return public_diagnostic_tree(chain)


def start_ordinary_process(command: list[str], *, cwd: str, environment: Mapping[str, str] | None = None):
    """All user relay starts use the caller's verified interactive shell token."""
    import subprocess

    caller = process_security(os.getpid())
    if caller["session_id"] <= 0:
        raise ValueError("user relay requires an interactive session")
    if caller["elevated"]:
        if environment is not None:
            raise ValueError("shell launch requires its fresh native environment")
        process = _start_with_shell_token(command, cwd=cwd, expected=caller)
    elif caller["integrity_rid"] == 8192:
        process = subprocess.Popen(command, cwd=cwd, env=environment, close_fds=True,
                                   creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0),
                                   stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL,
                                   stderr=subprocess.DEVNULL)
    else:
        raise ValueError("user relay caller integrity differs")
    child = process_security(process.pid)
    if (child["sid"] != caller["sid"] or child["session_id"] != caller["session_id"]
            or child["elevated"] or child["integrity_rid"] != 8192):
        raise ValueError("user relay normal token readback differs")
    return process


def _start_with_shell_token(command: list[str], *, cwd: str, expected: Mapping[str, Any]):
    import ctypes
    import subprocess
    from ctypes import wintypes

    class Startup(ctypes.Structure):
        _fields_ = [("cb", wintypes.DWORD), ("reserved", wintypes.LPWSTR),
                    ("desktop", wintypes.LPWSTR), ("title", wintypes.LPWSTR),
                    *[(name, wintypes.DWORD) for name in ("x", "y", "cx", "cy", "xchars", "ychars", "fill", "flags")],
                    ("show", wintypes.WORD), ("reserved_bytes", wintypes.WORD),
                    ("reserved2", wintypes.LPVOID), ("input", wintypes.HANDLE),
                    ("output", wintypes.HANDLE), ("error", wintypes.HANDLE)]
    class ProcessInfo(ctypes.Structure):
        _fields_ = [("process", wintypes.HANDLE), ("thread", wintypes.HANDLE),
                    ("pid", wintypes.DWORD), ("tid", wintypes.DWORD)]
    kernel = ctypes.WinDLL("kernel32", use_last_error=True)
    user = ctypes.WinDLL("user32", use_last_error=True)
    advapi = ctypes.WinDLL("advapi32", use_last_error=True)
    environment_api = ctypes.WinDLL("userenv", use_last_error=True)
    user.GetShellWindow.restype = wintypes.HWND
    user.GetWindowThreadProcessId.argtypes = [wintypes.HWND, ctypes.POINTER(wintypes.DWORD)]
    kernel.OpenProcess.argtypes = [wintypes.DWORD, wintypes.BOOL, wintypes.DWORD]
    kernel.OpenProcess.restype = wintypes.HANDLE
    kernel.CloseHandle.argtypes = [wintypes.HANDLE]
    kernel.GetExitCodeProcess.argtypes = [wintypes.HANDLE, ctypes.POINTER(wintypes.DWORD)]
    advapi.OpenProcessToken.argtypes = [wintypes.HANDLE, wintypes.DWORD, ctypes.POINTER(wintypes.HANDLE)]
    advapi.DuplicateTokenEx.argtypes = [wintypes.HANDLE, wintypes.DWORD, wintypes.LPVOID, ctypes.c_int, ctypes.c_int, ctypes.POINTER(wintypes.HANDLE)]
    advapi.CreateProcessWithTokenW.argtypes = [wintypes.HANDLE, wintypes.DWORD, wintypes.LPCWSTR, wintypes.LPWSTR, wintypes.DWORD, wintypes.LPVOID, wintypes.LPCWSTR, ctypes.POINTER(Startup), ctypes.POINTER(ProcessInfo)]
    environment_api.CreateEnvironmentBlock.argtypes = [ctypes.POINTER(wintypes.LPVOID), wintypes.HANDLE, wintypes.BOOL]
    environment_api.DestroyEnvironmentBlock.argtypes = [wintypes.LPVOID]
    def checked(success, call):
        if not success:
            error = OSError(ctypes.get_last_error(), call)
            error.call = call
            raise error
    shell_pid = wintypes.DWORD()
    checked(user.GetWindowThreadProcessId(user.GetShellWindow(), ctypes.byref(shell_pid)), "GetShellWindow")
    shell = process_security(shell_pid.value)
    if (shell["sid"] != expected["sid"] or shell["session_id"] != expected["session_id"]
            or shell["elevated"] or shell["integrity_rid"] != 8192):
        raise ValueError("interactive shell security differs")
    shell_process = kernel.OpenProcess(0x1000, False, shell_pid.value)
    checked(shell_process, "OpenProcess:shell")
    token, primary, environment = wintypes.HANDLE(), wintypes.HANDLE(), wintypes.LPVOID()
    try:
        checked(advapi.OpenProcessToken(shell_process, 10, ctypes.byref(token)), "OpenProcessToken:shell")
        checked(advapi.DuplicateTokenEx(token, 0x18B, None, 2, 1, ctypes.byref(primary)), "DuplicateTokenEx:primary")
        checked(environment_api.CreateEnvironmentBlock(ctypes.byref(environment), primary, False), "CreateEnvironmentBlock")
        startup = Startup(); startup.cb = ctypes.sizeof(Startup); startup.desktop = "winsta0\\default"
        child = ProcessInfo()
        checked(advapi.CreateProcessWithTokenW(primary, 0, command[0],
                ctypes.create_unicode_buffer(subprocess.list2cmdline(command)), 0x08000400,
                environment, cwd, ctypes.byref(startup), ctypes.byref(child)), "CreateProcessWithTokenW")
        kernel.CloseHandle(child.thread)
        class Process:
            pid = child.pid
            def poll(self):
                exit_code = wintypes.DWORD()
                checked(kernel.GetExitCodeProcess(child.process, ctypes.byref(exit_code)), "GetExitCodeProcess")
                return None if exit_code.value == 259 else exit_code.value
            def __del__(self):
                kernel.CloseHandle(child.process)
        return Process()
    finally:
        if environment:
            environment_api.DestroyEnvironmentBlock(environment)
        if primary:
            kernel.CloseHandle(primary)
        if token:
            kernel.CloseHandle(token)
        kernel.CloseHandle(shell_process)


def ordinary_user_context(operator_sid: str | None = None) -> bool:
    context = _inspect_current_process_execution_context()
    return context.get("status") == "PASS" and context.get("token_elevated") is False and context.get("integrity_level") == "MEDIUM" and (operator_sid is None or current_process_sid() == operator_sid)


def runtime_matches(app_root: Path) -> bool:
    executable = os.path.normcase(os.path.abspath(sys.executable))
    return executable in {os.path.normcase(str(app_root.parent / "runtime" / name)) for name in ("python.exe", "pythonw.exe")}


def assert_version_writer(app_root: Path, *, source: str, environ: Mapping[str, str]) -> None:
    """Fence old tools too; deployment delegation is checked by the caller."""
    if source.startswith("auto_update_") or source == "current_user_setup_removal":
        return
    control = control_root(app_root)
    if control is None:
        return
    if source == "current_user_onboarding":
        resolve_lifecycle_context(control.parent, operation="register")
        return
    if not runtime_matches(app_root):
        raise ValueError("writer must use its protected packet runtime")
    policy = load_policy(control)
    if not ordinary_user_context(policy["operator_sid"]):
        raise ValueError("writer is not the installed ordinary operator")
    state = optional_record(control / "transition.json")
    active = read_json(control / "active.json")
    own_sha = _sha(read_json(app_root.parent / "portable-manifest.json").get("source_commit"))
    if own_sha != active["sha"]:
        raise ValueError("writer code is not the active version")
    if source == "gui_force_exit_checkpoint" and state and (
            state.get("phase") not in {"CONFIRMED", "ROLLED_BACK", "DEFERRED"}
            or state.get("recovery_pending") is True):
        raise ValueError("bounded force exit requires settled update state")
    deferred_at = environ.get("KMTECH_LM_UPDATE_DEFERRED_AT")
    if state and state["phase"] in {"PREPARED", "FAILED", "UNKNOWN", "APPLIED_PENDING_READY"} and deferred_at:
        # The caller already holds writer admission. Do not invert the lock order
        # update_lock -> writer mutex: only read the same ordinary-start proof.
        assert_ordinary_recovery(control, policy, own_sha, deferred_at)
        return
    if state and state["phase"] not in {"CONFIRMED", "ROLLED_BACK", "DEFERRED"}:
        cleanup_sources = {'gui_settings_save', 'persistent_relay_status', 'scanner_audit_outbox_save', 'scanner_audit_segment_write', 'gui_current_state_save'}
        if (state["phase"] in {"ROLLBACK_PENDING", "UNKNOWN", "FAILED"} and source in cleanup_sources
                and environ.get("KMTECH_LM_UPDATE_TRIAL_NONCE") == state.get("nonce")
                and (source == "persistent_relay_status" or state.get("trial_pid") == os.getpid())):
            return  # Only the bound trial's ordinary flush/close may finish.
        if state["phase"] not in {"APPLIED_PENDING_READY", "ROLLBACK_PENDING_READY"} or environ.get("KMTECH_LM_UPDATE_TRIAL_NONCE") != state.get("nonce"):
            raise ValueError("writer denied by the startup transition")


def trial_context(app_root: Path) -> tuple[Path, dict[str, Any], dict[str, Any]] | None:
    control = control_root(app_root)
    if control is None:
        return None
    state = optional_record(control / "transition.json")
    if not state or state.get("phase") not in {"APPLIED_PENDING_READY", "ROLLBACK_PENDING_READY", "ROLLBACK_PENDING", "UNKNOWN", "FAILED"}:
        return None
    own_sha = version_manifest(app_root.parent)["source_commit"]
    expected = state.get("trial_sha") or (state["previous_sha"] if state["phase"] == "ROLLBACK_PENDING_READY" or state.get("rollback_at") else state["candidate_sha"])
    if own_sha != expected or os.environ.get("KMTECH_LM_UPDATE_TRIAL_NONCE") != state.get("nonce"):
        return None
    return control, read_json(control / "policy.json"), state


def prepared_pending(control: Path, policy: Mapping[str, Any], state: Mapping[str, Any] | None) -> bool:
    if not policy.get("feed_url"):
        return False
    path = Path(policy["staging_root"]) / "prepared.json"
    if (state and state.get("phase") == "DEFERRED" and state.get("failed_prepared_sha256")
            and state["failed_prepared_sha256"] == prepared_marker_sha256(policy)):
        return False
    try:
        marker = optional_record(path)
    except (OSError, ValueError):
        return path.exists()  # A corrupt hint still requests SYSTEM verification.
    if marker is None:
        return False
    try:
        sha = _sha(marker.get("source_commit"))
    except ValueError:
        return True
    active = read_json(control / "active.json")
    return sha != active["sha"] and sha not in (state or {}).get("failed_shas", [])


def prepare_feed_packet(app_root: Path) -> dict[str, Any]:
    """The ordinary relay downloads; only completed authenticated bytes are marked."""
    from urllib.parse import urlsplit, urlunsplit
    import requests
    from Label_Match import _verify_update_manifest_signature as verify_update_manifest_signature, _update_candidate_from_manifest as update_candidate_from_private_manifest, _sha256_file as file_sha256
    from current_user_onboarding import resolve_current_user_onboarding_paths, _default_profile_loader
    from label_data_generation import check_user_data_open

    control = control_root(app_root)
    if control is None:
        return {"status": "OFF"}
    policy = load_policy(control)
    if not policy.get("feed_url"):
        return {"status": "OFF"}
    key = read_json(control.parent / "current/app/update-manifest-key-config.json")["manifest_public_key"]
    active = read_json(control / "active.json")
    fixed_helper(control, "Verify", code_root=version_bundle(control, active["sha"]))
    current = version_manifest(version_bundle(control, active["sha"]))
    profile = None
    if policy.get("profile_auth"):
        profile_path = resolve_current_user_onboarding_paths(app_root.parent).logistics_profile_path
        check_user_data_open(app_root, (profile_path.parent,))
        profile = _default_profile_loader(profile_path)
    feed = urlsplit(policy["feed_url"])
    origin = lambda url: (url.scheme.lower(), (url.hostname or "").lower(), url.port or 443)
    if profile is not None and origin(feed) != origin(urlsplit(profile.base_url)):
        raise ValueError("profile authentication requires the configured authority origin")
    def fetch(session, url):
        assert_https_update_url(url)
        if profile is not None and profile.tls_ca_bundle_path:
            check_user_data_open(app_root, (Path(profile.tls_ca_bundle_path).parent,))
        session.cookies.clear()
        headers = {"Authorization": "Bearer " + profile.bearer_token} if profile is not None and origin(urlsplit(url)) == origin(feed) else {}
        headers["User-Agent"] = "KMTech-LabelMatch/1.0"
        response = session.get(url, headers=headers, timeout=10, stream=True, allow_redirects=False, verify=(profile.tls_ca_bundle_path or True) if profile is not None else True)
        if response.status_code != 200:
            response.close()
            reason = "회사 서버 오류" if response.status_code >= 500 else "회사 서버가 업데이트 요청을 거절했습니다"
            raise ValueError(f"{reason}(코드 {response.status_code}).")
        if response.url != url:
            response.close()
            raise ValueError("update download response URL differs")
        return response
    def bounded(response, maximum):
        with response:
            chunks = []
            count = 0
            for chunk in response.iter_content(65536):
                count += len(chunk)
                if count > maximum:
                    raise ValueError("update metadata is oversized")
                chunks.append(chunk)
            return b"".join(chunks)
    with requests.Session() as session:
        session.trust_env = False  # No ambient proxy/netrc credentials.
        manifest_bytes = bounded(fetch(session, policy["feed_url"]), 1024 * 1024)
        manifest = json.loads(manifest_bytes, object_pairs_hook=unique_object)
        signature_url = urlunsplit((feed.scheme, feed.netloc, feed.path + ".sig", feed.query, ""))
        signature = bounded(fetch(session, signature_url), 16384)
        verify_update_manifest_signature(manifest, signature, key)
        candidate = update_candidate_from_private_manifest(manifest, current_version=current["version"], expected_channel=policy["channel"])
        if candidate is None:
            return {"status": "NO_UPDATE"}
        if candidate["archive"].get("layout") != "canonical_portable":
            raise ValueError("daily updates require the canonical portable layout")
        sha = _sha(manifest.get("source_commit"))
        state = optional_record(control / "transition.json")
        if sha in (state or {}).get("failed_shas", []):
            return {"status": "FAILED_VERSION_PRESERVED", "source_commit": sha}
        stage = Path(policy["staging_root"])
        existing = optional_record(stage / "prepared.json")
        if existing and existing.get("source_commit") == sha and existing.get("packet_sha256") == candidate["sha256"]:
            return {"status": "PREPARED", "source_commit": sha}
        def download(attempt, temps):
            write_download_bytes(attempt / "manifest.json", manifest_bytes)
            write_download_bytes(attempt / "manifest.sig", signature)
            write_download_bytes(attempt / "packet.zip", b"")
            count = 0
            with fetch(session, candidate["url"]) as response:
                for chunk in response.iter_content(256 * 1024):
                    count += len(chunk)
                    if count > min(manifest["artifact"]["size_bytes"], 512 * 1024 * 1024):
                        raise ValueError("update packet is oversized")
                    write_download_chunk(attempt / "packet.zip", chunk)
            if count != manifest["artifact"]["size_bytes"] or file_sha256(attempt / "packet.zip") != candidate["sha256"]:
                raise ValueError("downloaded packet size or SHA256 differs")
            from Label_Match import _validate_update_archive_contract
            import zipfile
            with zipfile.ZipFile(attempt / "packet.zip") as archive:
                _validate_update_archive_contract(archive, archive_policy=candidate["archive"])
            authorize_feed_transition(manifest, current=current,
                                      candidate=packet_version_identity(attempt / "packet.zip"))
            # Final fixed-name copies are replaced only after every byte is verified.
            temps.update(stage_prepared_copies(attempt, stage))
            publish_download(stage, temps)
            write_json_durable(stage / "prepared.json", {"source_commit": sha, "packet_sha256": candidate["sha256"], "prepared_at": now_utc()})
            return {"status": "PREPARED", "source_commit": sha}
        return _run_download_attempt(stage, download)


@writer_sink("auto_update_create_download_stage")
def create_download_stage(path: Path) -> None:
    _assert_no_reparse_components(path)
    path.mkdir(parents=True, exist_ok=True)


@writer_sink("auto_update_write_download_bytes", probe_only=True)
def write_download_bytes(path: Path, data: bytes) -> None:
    _assert_no_reparse_components(path)
    with path.open("xb") as output:
        output.write(data)
        output.flush()
        os.fsync(output.fileno())


@writer_sink("auto_update_write_download_chunk", probe_only=True)
def write_download_chunk(path: Path, data: bytes) -> None:
    _assert_no_reparse_components(path)
    with path.open("ab") as output:
        output.write(data)
        output.flush()
        os.fsync(output.fileno())


@writer_sink("auto_update_stage_prepared_copies", probe_only=True)
def stage_prepared_copies(attempt: Path, stage: Path) -> dict[str, Path]:
    temps = {name: stage / (name + "." + uuid.uuid4().hex + ".tmp")
             for name in ("manifest.json", "manifest.sig", "packet.zip")}
    for temporary in temps.values():
        _assert_no_reparse_components(temporary)
        if temporary.exists():
            raise FileExistsError("prepared temporary already exists")
    owned: dict[str, Path] = {}
    try:
        for name, temporary in temps.items():
            owned[name] = temporary
            try:
                _copy_prepared_file(attempt / name, temporary)
            except FileExistsError:
                # An exclusive-create collision is not owned by this attempt.
                owned.pop(name)
                raise
    except BaseException as failure:
        try:
            _discard_prepared_copies(stage, owned)
        except Exception as cleanup_error:
            failure.add_note("UPDATE_ATTEMPT_CLEANUP_FAILED: " + type(cleanup_error).__name__)
        raise
    return temps


@writer_sink("auto_update_publish_download")
def publish_download(stage: Path, temps: dict[str, Path]) -> None:
    for name in ("manifest.json", "manifest.sig", "packet.zip"):
        os.replace(temps[name], stage / name)


def _discard_prepared_copies(stage: Path, temps: Mapping[str, Path]) -> None:
    _assert_no_reparse_components(stage)
    for name, temporary in temps.items():
        if (name not in ("manifest.json", "manifest.sig", "packet.zip")
                or temporary.parent.absolute() != stage.absolute()
                or re.fullmatch(re.escape(name) + r"\.[0-9a-f]{32}\.tmp", temporary.name) is None):
            raise ValueError("prepared temporary path differs from this attempt")
        _assert_no_reparse_components(temporary)
    for temporary in temps.values():
        temporary.unlink(missing_ok=True)


@writer_sink("auto_update_run_download_attempt", probe_only=True)
def _run_download_attempt(stage: Path, run) -> dict[str, Any]:
    # Own the attempt and its finally under this initial probe. A fresh cleanup
    # probe would reject a fence raised during the off-admission body and leave
    # the downloaded packet behind. Only these owned temporary names are removed.
    create_download_stage(stage)
    attempt = stage / ("download-" + uuid.uuid4().hex)
    temps: dict[str, Path] = {}
    created = False
    failure = None
    try:
        create_protected_stage(attempt)
        created = True
        return run(attempt, temps)
    except BaseException as error:
        failure = error
        raise
    finally:
        try:
            if (attempt.parent.absolute() != stage.absolute()
                    or re.fullmatch(r"download-[0-9a-f]{32}", attempt.name) is None):
                raise ValueError("download attempt path differs")
            _assert_no_reparse_components(attempt)
            entries = list(attempt.iterdir()) if created else []
            for entry in entries:
                if entry.name not in ("manifest.json", "manifest.sig", "packet.zip"):
                    raise ValueError("download attempt contains an unexpected entry")
                _assert_no_reparse_components(entry)
                if not entry.is_file():
                    raise ValueError("download attempt entry is not a file")
            _discard_prepared_copies(stage, temps)
            for entry in entries:
                entry.unlink()
            if created:
                attempt.rmdir()
        except Exception as cleanup_error:
            if failure is None:
                raise
            failure.add_note("UPDATE_ATTEMPT_CLEANUP_FAILED: " + type(cleanup_error).__name__)


def trial_stop_requested(app_root: Path) -> bool:
    context = trial_context(app_root)
    return bool(context and context[2]["phase"] in {"ROLLBACK_PENDING", "UNKNOWN", "FAILED"})


def relay_ready_metadata(app_root: Path) -> dict[str, Any]:
    context = trial_context(app_root)
    if context is None:
        if control_root(app_root) is None:
            return {}
        # Publish this process's identity, never the desired policy identity.
        # Older/missing metadata still requires the ordinary Observe path.
        try:
            return {"source_commit": read_json(app_root.parent / "portable-manifest.json")["source_commit"],
                    "sid": current_process_sid(), "ordinary_user": ordinary_user_context()}
        except (OSError, ValueError, KeyError):
            return {}  # Optional cache metadata cannot add a first-start refusal.
    _, _, state = context
    metadata = {"process_id": os.getpid(), "source_commit": version_manifest(app_root.parent)["source_commit"], "update_nonce": state["nonce"], "ordinary_user": ordinary_user_context()}
    try:
        metadata["sid"] = current_process_sid()
    except (OSError, ValueError):
        pass  # Retain the existing nonce/readiness proof and observed fallback.
    return metadata


def publish_gui_ready(app_root: Path) -> bool:
    """User-only readback; SYSTEM sees booleans, never identities or credentials."""
    import sqlite3
    from current_user_onboarding import resolve_current_user_onboarding_paths, inspect_current_user_state
    from user_relay import user_relay_status_path

    context = trial_context(app_root)
    if context is None:
        return True
    control, policy, state = context
    paths = resolve_current_user_onboarding_paths(app_root.parent)
    if not ordinary_user_context() or inspect_current_user_state(paths).get("status") != "READY":
        return False
    # Read both existing durable queues without creating or migrating them.
    for database in (paths.ledger_path, paths.direct_sync_root / "queue/direct_sync_relay.sqlite3"):
        _assert_no_reparse_components(database)
        if not database.is_file():
            return False
        try:
            with sqlite3.connect(database.as_uri() + "?mode=ro", uri=True) as connection:
                if connection.execute("PRAGMA quick_check").fetchall() != [("ok",)]:
                    return False
        except sqlite3.Error:
            return False
    from label_data_manager import read_recovery_file
    for path in paths.data_root.rglob("*_current_set_state_packaging.json"):
        if read_recovery_file(path, current_state=True).get("verified") is not True:
            return False
    for path in (paths.data_root / "_scanner_audit_outbox").glob("*.json"):
        read_json(path)
    for path in paths.data_root.glob("*held*state*.json"):
        read_json(path)
    for path in (paths.direct_sync_root / "queue").glob("*.json"):
        read_json(path)
    relay = optional_record(user_relay_status_path(paths.direct_sync_root))
    sha = version_manifest(app_root.parent)["source_commit"]
    if not relay or relay.get("status") != "RUNNING" or relay.get("source_commit") != sha or relay.get("update_nonce") != state["nonce"] or relay.get("ordinary_user") is not True:
        return False
    payload = {
        **{check: True for check in READY_CHECKS}, "pid": os.getpid(), "sha": sha,
        "nonce": state["nonce"], "transition_id": state["transition_id"], "ready_at": now_utc(),
    }
    write_json_durable(Path(policy["staging_root"]) / "ready.json", payload)
    return True


def confirmed_trial(app_root: Path) -> bool:
    control = control_root(app_root)
    if control is None:
        return False
    state = optional_record(control / "transition.json")
    active = read_json(control / "active.json")
    sha = version_manifest(app_root.parent)["source_commit"]
    return bool(state and state.get("phase") in {"CONFIRMED", "ROLLED_BACK"}
                and state.get("trial_sha") == sha and active.get("sha") == sha)


def manual_lifecycle_gate(control: Path, mode: str) -> int | None:
    """User removal owns only its Run/relay; registration requires health."""
    if mode == "--remove-current-user-setup":
        return None
    from writer_session_fence import active_fence, canonical_control_root, _delegation_matches
    fence = active_fence(canonical_control_root(), environ=os.environ)
    source = "current_user_onboarding"
    if fence is not None and _delegation_matches(fence, source=source, environ=os.environ):
        return None
    try:
        resolve_lifecycle_context(control.parent, operation="register")
        return None
    except Exception as error:
        details = launch_failure_details(error, include_original=True)
    print(json.dumps({
        "status": "RECOVERY_REQUIRED", "code": "SUPPORTED_REMOVAL_REQUIRED",
        "data_preserved": True,
        "message": "자료와 등록 및 전송 대기를 보존하고 지원 제거를 먼저 실행한 뒤 다시 설치하세요.",
        "errors": details,
    }, ensure_ascii=False))
    return 20


def startup_bootstrap_reason(sha: str, active: Mapping[str, Any], state, prepared: bool) -> str | None:
    """The ordinary-start decision, without locks, writes or bootstrap launch."""
    if prepared:
        return "UPDATE_PREPARED"
    if state and state.get("recovery_pending") is True:
        return "UPDATE_RECOVERY"
    if state and state["phase"] not in {"CONFIRMED", "ROLLED_BACK", "DEFERRED"}:
        return "UPDATE_PHASE"
    if sha != active["sha"]:
        return "ACTIVE_BUILD_MISMATCH"
    return None


def _record_abandoned_update(policy, *, verified: bool) -> None:
    fields = {"reason_code": ("UPDATE_MUTEX_ABANDONED_VERIFIED" if verified else
                              "UPDATE_MUTEX_ABANDONED_RECOVERY_REQUIRED"),
              "exit_code": 0 if verified else 20}
    try:
        emit_support_record(fields, stage="update_apply", policy=policy)
    except Exception:
        # A diagnostic failure cannot mask the recovery decision at the
        # module-level entrypoint. Publish only fixed codes, never exceptions.
        try:
            print(json.dumps({**fields, "stage": "update_apply"}, sort_keys=True))
        except Exception:
            pass


def _verify_abandoned_update(control: Path, policy: Mapping[str, Any]) -> None:
    """Read-only proof while the abandoned update mutex is still owned."""
    try:
        state = read_json(control / "transition.json")
        active = read_json(control / "active.json")
        if state.get("recovery_pending") is True:
            raise ValueError("update recovery remains pending")
        resolve_lifecycle_context(control.parent, expected_from_commit=_sha(active.get("sha")),
                                  operation="register")
        if (read_json(control / "transition.json") != state
                or read_json(control / "active.json") != active or load_policy(control) != policy):
            raise ValueError("update installation changed during abandoned-owner verification")
    except Exception as error:
        raise UpdateMutexRecoveryRequired(policy) from error
    _record_abandoned_update(policy, verified=True)


def _show_update_mutex_recovery_notice() -> None:
    parent = None
    try:
        import tkinter as tk
        from tkinter import messagebox
        parent = tk.Tk()
        parent.withdraw()
        messagebox.showerror("설치 복구 필요", UPDATE_MUTEX_RECOVERY_MESSAGE, parent=parent)
    except Exception:
        pass  # The fixed stderr guidance remains available when Tk cannot open.
    finally:
        if parent is not None:
            try:
                parent.destroy()
            except Exception:
                pass


def _refuse_abandoned_update(policy, *, role: str) -> int:
    _record_abandoned_update(policy, verified=False)
    try:
        print(UPDATE_MUTEX_RECOVERY_MESSAGE + " [UPDATE_MUTEX_ABANDONED_RECOVERY_REQUIRED]", file=sys.stderr)
    except Exception:
        pass
    if role == "gui":
        try:
            from startup_observation import observe_startup
            observe_startup(["INSTALL_MANIFEST_MISMATCH"], identity={}, refused=False)
        except Exception:
            pass
        try:
            _show_update_mutex_recovery_notice()
        except Exception:
            pass
    return 20


def startup_gate(app_root: Path, arguments: list[str], *, role: str = "gui") -> int | None:
    if arguments[:1] in (["--label-match-user-relay"], ["--label-match-direct-sync-relay"]):
        role = "relay"
    try:
        return _startup_gate(app_root, arguments, role=role)
    except UpdateMutexRecoveryRequired as error:
        return _refuse_abandoned_update(error.policy, role=role)
    except RuntimeError as error:
        if str(error) == "update mutex ownership was abandoned":
            return _refuse_abandoned_update(None, role=role)
        if str(error) != "update mutex ownership is busy":
            raise
        control = control_root(app_root)
        deferred_at = os.environ.get("KMTECH_LM_UPDATE_DEFERRED_AT")
        state = optional_record(control / "transition.json") if control is not None else None
        if deferred_at and control is not None and (not state or state.get("phase") in {"PREPARED", "CONFIRMED", "ROLLED_BACK", "DEFERRED", "FAILED", "UNKNOWN", "APPLIED_PENDING_READY"}):
            policy = load_policy(control)
            assert_ordinary_recovery(control, policy, version_manifest(app_root.parent)["source_commit"], deferred_at)
            assert_version_writer(app_root, source="gui_settings_save", environ=os.environ)
            emit_support_record({"reason_code": "UPDATE_APPLY_NO_PROGRESS", "exit_code": 0}, stage="update_apply", policy=policy)
            return None
        return bootstrap(app_root, arguments, role=role)


def _startup_gate(app_root: Path, arguments: list[str], *, role: str) -> int | None:
    if arguments[:1] == ["--remove-current-user-setup"]:
        return None
    if arguments[:1] in (["--label-match-user-relay"], ["--label-match-direct-sync-relay"]):
        role = "relay"
    control = control_root(app_root)
    if control is None:
        return None
    if arguments and arguments[0] in {"--onboard-current-user", "--remove-current-user-setup", "--restore-current-user-lifecycle-after-replacement"}:
        return manual_lifecycle_gate(control, arguments[0])
    policy = load_policy(control)
    if not ordinary_user_context(policy["operator_sid"]):
        if role == "gui":
            try:
                from startup_observation import observe_startup
                observe_startup(["OPERATOR_ACCOUNT_MISMATCH"], identity={}, refused=True)
            except Exception:
                pass
        return 20
    with update_lock(control, policy["operator_sid"],
                     abandoned_check=lambda: _verify_abandoned_update(control, policy)):
        state = optional_record(control / "transition.json")
        active = read_json(control / "active.json")
        release = version_manifest(app_root.parent)
        sha = release["source_commit"]
        if sha == active["sha"] and state and state["phase"] in {"APPLIED_PENDING_READY", "ROLLBACK_PENDING_READY"}:
            if os.environ.get("KMTECH_LM_UPDATE_TRIAL_NONCE") == state["nonce"] and ordinary_user_context():
                claim = optional_record(Path(policy["staging_root"]) / "trial.json")
                if claim and claim.get("nonce") == state["nonce"] and claim.get("transition_id") == state["transition_id"] and (role == "relay" or claim.get("pid") == os.getpid()):
                    return None
        deferred_at = os.environ.get("KMTECH_LM_UPDATE_DEFERRED_AT")
        if sha == active["sha"] and deferred_at and (not state or state["phase"] in {"PREPARED", "CONFIRMED", "ROLLED_BACK", "DEFERRED", "FAILED", "UNKNOWN", "APPLIED_PENDING_READY"}):
            _utc(deferred_at)
            if state and state["phase"] in {"PREPARED", "FAILED", "UNKNOWN", "APPLIED_PENDING_READY"}:
                assert_ordinary_recovery(control, policy, sha, deferred_at)
            # Ordinary active-code admission only. The unchanged G guard still
            # runs before data access; this grants no pointer or trial authority.
            return None
        prepared = prepared_pending(control, policy, state)
        reason = startup_bootstrap_reason(sha, active, state, prepared)
        if reason is None:
            return None
    if role == "gui":
        try:
            from startup_observation import observe_startup
            observe_startup([reason], identity={"version": release.get("version", ""), "build_id": sha})
        except Exception:
            pass
    request_bootstrap(control, arguments, role=role)
    return 0


@writer_sink("auto_update_request_bootstrap")
def request_bootstrap(control: Path, arguments: list[str], *, role: str) -> None:
    root = control.parent / "current"
    start_ordinary_process([str(root / "runtime/pythonw.exe"), "-I", "-B", str(root / "app/auto_update.py"), "--bootstrap", "--role", role, "--", *arguments], cwd=str(root))


@writer_sink("auto_update_request_apply_task")
def request_apply_task(control: Path, policy: Mapping[str, Any]) -> None:
    import subprocess

    executable = system_executable("schtasks.exe")
    result = subprocess.run([executable, "/Run", "/TN", task_name(control.parent / "current", policy["operator_sid"])], capture_output=True, creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0))
    if result.returncode:
        error = RuntimeError("the installed update task could not be requested; " + safe_update_text(result.stderr))
        error.child_output = child_failure_output(result)
        raise error


@writer_sink("auto_update_launch_version")
def launch_version(control: Path, sha: str, arguments: list[str], *, nonce: str = "", deferred_at: str = "") -> int:
    bundle = version_bundle(control, sha)
    if version_manifest(bundle)["source_commit"] != sha:
        raise ValueError("active version identity differs")
    environment = dict(os.environ)
    environment.pop("KMTECH_LM_UPDATE_TRIAL_NONCE", None)
    environment.pop("KMTECH_LM_UPDATE_DEFERRED_AT", None)
    if nonce:
        environment["KMTECH_LM_UPDATE_TRIAL_NONCE"] = _sha(nonce, length=32)
    if deferred_at:
        _utc(deferred_at)
        environment["KMTECH_LM_UPDATE_DEFERRED_AT"] = deferred_at
    process = start_ordinary_process([str(bundle / "runtime/pythonw.exe"), "-I", "-B", str(bundle / "app/main.py"), *arguments], cwd=str(bundle), environment=environment)
    return process.pid


def current_generation_status(control: Path, policy: Mapping[str, Any], sha: str) -> str:
    from kmtech_shared import data_generation as generation

    try:
        observed = read_trial_generation(policy)
        if observed.get("status") != "READ":
            return "UNKNOWN"
        release = version_manifest(version_bundle(control, sha))
        return generation.probe_downgrade([observed["marker_path"]], release["version"],
            app_id="Label_Match", subject_sid=policy["operator_sid"], target_build_id=sha)["status"]
    except (OSError, ValueError, TypeError, KeyError):
        return "UNKNOWN"



def assert_ordinary_recovery(control: Path, policy: Mapping[str, Any], sha: str, started_at: str) -> None:
    """Read-only proof under startup update ownership or existing writer admission."""
    if read_json(control / "active.json")["sha"] != sha:
        raise ValueError("ordinary recovery code is not the active version")
    if current_generation_status(control, policy, sha) != "ALLOW":
        raise ValueError("ordinary recovery requires current data generation allowance")
    started = _utc(started_at)
    for row in writer_processes(process_snapshot(control), own_pid=os.getpid()):
        if (row.get("sha") != sha or row.get("sid") != policy["operator_sid"]
                or row.get("ordinary_user") is not True or row.get("role") not in {"gui", "relay"}
                or _utc(row.get("created_at")) < started):
            raise ValueError("an earlier or unrelated writer prevents ordinary recovery")



def _launch_bootstrap_version(control: Path, sha: str, arguments: list[str], *, deadline: float, **kwargs) -> int:
    """Retry transient relay admission within this bootstrap's existing window."""
    while True:
        try:
            return launch_version(control, sha, arguments, **kwargs)
        except WriterFencedError as error:
            if arguments[:1] != ["--label-match-user-relay"] or error.code != "WRITER_GATE_TIMEOUT":
                raise
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise
            # launch_version has unwound its writer admission before waiting.
            time.sleep(min(0.1, remaining))
            if time.monotonic() >= deadline:
                raise


def start_current(control: Path, policy: Mapping[str, Any], state, active, arguments, *, code: str, report_once: bool = False, deadline: float | None = None) -> int:
    """Reuse ordinary launch and publish only the E9 allowlisted support fields."""
    report = support_record({"reason_code": code,
        "transition_code": (state or {}).get("failure_decision", {}).get("code", code),
        "retained_candidate": (state or {}).get("retained_candidate") is True}, stage="update_apply")
    try:
        report_path = Path(policy["staging_root"]) / "startup-support.json"
        previous = None
        if report_once:
            claim_hash = hashlib.sha256((control / "apply-claim.json").read_bytes()).hexdigest()
            report = support_record({**report, "apply_claim_sha256": claim_hash}, stage="update_apply")
            previous = optional_record(report_path)
        if not (report_once and previous and previous.get("reason_code") == code
                and previous.get("apply_claim_sha256") == report["apply_claim_sha256"]):
            write_json_durable(report_path, report)
    except (OSError, ValueError, TypeError, WriterFenceError):
        # A support sink failure cannot turn a safe ordinary launch into a stop.
        emit_support_record({"reason_code": "REPORT_UNAVAILABLE"}, stage="update_apply")
    started_at = now_utc()
    if state and state["phase"] in {"PREPARED", "FAILED", "UNKNOWN", "APPLIED_PENDING_READY"}:
        assert_ordinary_recovery(control, policy, active["sha"], started_at)
    if deadline is None:
        launch_version(control, active["sha"], arguments, deferred_at=started_at)
    else:
        _launch_bootstrap_version(control, active["sha"], arguments, deadline=deadline, deferred_at=started_at)
    return 0



def bootstrap(app_root: Path, arguments: list[str], *, role: str) -> int:
    try:
        return _bootstrap(app_root, arguments, role=role)
    except UpdateMutexRecoveryRequired as error:
        return _refuse_abandoned_update(error.policy, role=role)
    except RuntimeError as error:
        if str(error) == "update mutex ownership was abandoned":
            return _refuse_abandoned_update(None, role=role)
        if str(error) not in {"update mutex ownership is busy", "update mutex ownership was abandoned"}:
            raise
        record = support_record({"reason_code": "UPDATE_IN_PROGRESS"}, stage="update_apply")
        record["guidance"] = STARTUP_PROGRESS_MESSAGE
        print(json.dumps(record, ensure_ascii=False, sort_keys=True))
        return 20



def _bootstrap(app_root: Path, arguments: list[str], *, role: str) -> int:
    import time

    control = control_root(app_root)
    if control is None:
        raise ValueError("bootstrap requires the installed ordinary-user context")
    policy = load_policy(control)
    if not ordinary_user_context(policy["operator_sid"]):
        raise ValueError("bootstrap requires the installed ordinary operator")
    requested = False
    requested_at = None
    progress_deadline = None
    saw_trial = False
    apply_started = False
    deadline = time.monotonic() + STARTUP_WAIT_SECONDS
    while time.monotonic() < deadline:
        try:
            with update_lock(control, policy["operator_sid"],
                             abandoned_check=lambda: _verify_abandoned_update(control, policy)):
                state = optional_record(control / "transition.json")
                claim_status = apply_claim_status(control)
                apply_started = apply_started or claim_status in {"LIVE", "UNKNOWN"} or bool(state and state["phase"] in {"APPLIED_PENDING_READY", "ROLLBACK_PENDING_READY"})
                if state and state["phase"] in {"APPLIED_PENDING_READY", "ROLLBACK_PENDING_READY"}:
                    saw_trial = True
                if claim_status in {"LIVE", "UNKNOWN"} and not (state and state["phase"] in {"APPLIED_PENDING_READY", "ROLLBACK_PENDING_READY"}):
                    time.sleep(0.1)
                    continue
                prepared = prepared_pending(control, policy, state)
                action = startup_action(state, prepared_present=prepared)
                active = read_json(control / "active.json")
                gui_arguments = arguments[1:] if role == "relay" and arguments[:1] == ["--label-match-user-relay"] else arguments
                if state and state["phase"] == "DEFERRED" and state.get("retained_candidate") is True:
                    if active["sha"] != recovery_sha(state, pointer_sha=active["sha"]):
                        action = "WAIT_FOR_RECOVERY"
                    else:
                        return start_current(control, policy, state, active, gui_arguments, code="UPDATE_TERMINAL_RECOVERED", deadline=deadline)
                if state and state["phase"] in {"CONFIRMED", "ROLLED_BACK"} and saw_trial:
                    if state["phase"] == "ROLLED_BACK":
                        return start_current(control, policy, state, active, gui_arguments, code="UPDATE_PREVIOUS_CONFIRMED", deadline=deadline)
                    return 0  # The already-launched GUI owns input and its ordinary relay.
                if state and state["phase"] == "DEFERRED" and requested_at and _utc(state["deferred_at"]) >= _utc(requested_at):
                    code = state.get("reason_code")
                    if code not in {"UPDATE_PREPARATION_FAILED", "UPDATE_EXISTING_WRITER"}:
                        code = "UPDATE_TERMINAL_RECOVERED"
                    return start_current(control, policy, state, active, arguments, code=code, deadline=deadline)
                if claim_status == "ABSENT":
                    if writer_processes(process_snapshot(control), own_pid=os.getpid()):
                        return 0
                    if current_generation_status(control, policy, active["sha"]) != "ALLOW":
                        emit_progress_guidance()
                        return 20
                    return start_current(control, policy, state, active, arguments, code="UPDATE_SUPERVISION_UNAVAILABLE", report_once=True, deadline=deadline)
                if action == "START_CURRENT":
                    if state and state["phase"] in {"ROLLED_BACK", "DEFERRED"}:
                        return start_current(control, policy, state, active, arguments, code="UPDATE_TERMINAL_RECOVERED", deadline=deadline)
                    _launch_bootstrap_version(control, active["sha"], arguments, deadline=deadline)
                    return 0
                if state and state["phase"] in {"UNKNOWN", "FAILED"}:
                    if claim_status not in {"LIVE", "UNKNOWN"}:
                        if writer_processes(process_snapshot(control), own_pid=os.getpid()):
                            return 0  # The existing single application writer owns work.
                        if current_generation_status(control, policy, active["sha"]) == "ALLOW" and not prepared:
                            return start_current(control, policy, state, active, arguments, code="UPDATE_TERMINAL_RECOVERED", deadline=deadline)
                if action in {"REQUEST_APPLY", "REQUEST_TRIAL", "WAIT_FOR_RECOVERY", "WAIT_FOR_TRIAL"} and not requested:
                    requested_at = now_utc()
                    progress_deadline = time.monotonic() + APPLY_PROGRESS_TIMEOUT
                    try:
                        request_apply_task(control, policy)
                    except Exception:
                        if apply_started or apply_claim_status(control) in {"LIVE", "UNKNOWN"}:
                            requested = True
                            continue
                        if not state or state["phase"] in {"PREPARED", "CONFIRMED", "ROLLED_BACK", "DEFERRED", "FAILED", "UNKNOWN", "APPLIED_PENDING_READY"}:
                            if state and state["phase"] == "APPLIED_PENDING_READY" and writer_processes(process_snapshot(control), own_pid=os.getpid()):
                                return 0
                            code = "UPDATE_SUPERVISION_UNAVAILABLE" if state and state["phase"] == "APPLIED_PENDING_READY" else "UPDATE_APPLY_REQUEST_FAILED"
                            return start_current(control, policy, state, active, arguments, code=code, deadline=deadline)
                        raise
                    requested = True
                    # The task may already have settled or switched this journal.
                    # Release ownership and reread it before creating a trial.
                    continue
                if action == "REQUEST_TRIAL":
                    # Both the Run relay and a launcher request this same GUI trial.
                    # The GUI's ordinary onboarding starts its single normal relay.
                    with update_lock(control, policy["operator_sid"], trial=True):
                        state = read_json(control / "transition.json")
                        claim = optional_record(Path(policy["staging_root"]) / "trial.json")
                        failure_path = Path(policy["staging_root"]) / "trial-failure.json"
                        failure = optional_record(failure_path)
                        if not (failure and failure.get("nonce") == state["nonce"]
                                and failure.get("transition_id") == state["transition_id"]):
                            failed_role = "gui"
                            try:
                                if claim and claim.get("nonce") == state["nonce"]:
                                    row = next((row for row in process_snapshot(control) if row.get("pid") == claim.get("pid")), None)
                                    if (not row or claim.get("transition_id") != state["transition_id"]
                                            or not process_matches_trial(row, state=state, sha=active["sha"], sid=policy["operator_sid"], role="gui")
                                            or _utc(row.get("created_at")) > _utc(claim.get("claimed_at"))):
                                        raise ValueError("existing trial process identity or creation time differs")
                                    failed_role = "relay"
                                    if not any(process_matches_trial(row, state=state, sha=active["sha"], sid=policy["operator_sid"], role="relay") for row in process_snapshot(control)):
                                        raise ValueError("existing trial relay is absent")
                                else:
                                    pid = _launch_bootstrap_version(control, active["sha"], gui_arguments, nonce=state["nonce"], deadline=deadline)
                                    write_json_durable(Path(policy["staging_root"]) / "trial.json", {"pid": pid, "nonce": state["nonce"], "transition_id": state["transition_id"], "claimed_at": now_utc()})
                                    failed_role = "relay"
                                    # LM's user settings select both children's data root.
                                    _launch_bootstrap_version(control, active["sha"], ["--label-match-user-relay"], nonce=state["nonce"], deadline=deadline)
                                saw_trial = True
                            except Exception as error:
                                write_json_durable(failure_path, {
                                    "nonce": state["nonce"], "transition_id": state["transition_id"],
                                    "sha": active["sha"], "failed_role": failed_role,
                                    "launch_failure_details": launch_failure_details(error, include_original=True),
                                })
                                # A failed start is not permission to change the pointer.
                                # SYSTEM must prove normal stop and previous-code trust.
                if requested and progress_deadline is not None:
                    claim_status = apply_claim_status(control)
                    if claim_status == "ABSENT":
                        if state and state["phase"] == "APPLIED_PENDING_READY" and writer_processes(process_snapshot(control), own_pid=os.getpid()):
                            return 0
                        return start_current(control, policy, state, active, arguments, code="UPDATE_SUPERVISION_UNAVAILABLE", report_once=True, deadline=deadline)
                    if claim_status in {"LIVE", "UNKNOWN"}:
                        apply_started = True
                    if not apply_started and time.monotonic() >= progress_deadline:
                        return start_current(control, policy, state, active, arguments, code="UPDATE_APPLY_NO_PROGRESS", deadline=deadline)
        except RuntimeError as error:
            if str(error) not in {'update mutex ownership was abandoned', 'update mutex ownership is busy'}:
                raise
            if str(error) == "update mutex ownership was abandoned":
                raise
        time.sleep(0.1)
    if apply_claim_status(control) == "UNKNOWN":
        try:
            with update_lock(control, policy["operator_sid"],
                             abandoned_check=lambda: _verify_abandoned_update(control, policy)):
                try:
                    stale_claim = time.time() - (control / "apply-claim.json").stat().st_mtime > 15 * 60
                except OSError:
                    stale_claim = False
                if stale_claim and apply_claim_status(control) == "UNKNOWN":
                    state = optional_record(control / "transition.json")
                    active = read_json(control / "active.json")
                    if writer_processes(process_snapshot(control), own_pid=os.getpid()):
                        return 0
                    if current_generation_status(control, policy, active["sha"]) == "ALLOW" and not prepared_pending(control, policy, state):
                        return start_current(control, policy, state, active, arguments, code="UPDATE_SUPERVISION_UNAVAILABLE", report_once=True, deadline=deadline)
        except RuntimeError as error:
            if str(error) not in {'update mutex ownership was abandoned', 'update mutex ownership is busy'}:
                raise
    active = read_json(control / "active.json")
    if current_generation_status(control, policy, active["sha"]) != "ALLOW":
        emit_progress_guidance()
        return 20
    assert_ordinary_recovery(control, policy, active["sha"], now_utc())
    result = start_current(control, policy, optional_record(control / "transition.json"), active,
                           arguments, code="UPDATE_APPLY_NO_PROGRESS", deadline=deadline)
    emit_support_record({"reason_code": "UPDATE_APPLY_NO_PROGRESS", "exit_code": result}, stage="update_apply", policy=policy)
    return result


def main(arguments: list[str] | None = None) -> int:
    import argparse
    import sys

    parser = __import__("failure_redaction").InstallerArgumentParser()
    parser.add_argument("--bootstrap", action="store_true")
    parser.add_argument("--apply-prepared", action="store_true")
    parser.add_argument("--control-root", type=Path)
    parser.add_argument("--role", choices=("gui", "relay"), default="gui")
    parser.add_argument("--print-logon-task", action="store_true")
    parser.add_argument("--install-root", type=Path)
    parser.add_argument("--operator-sid")
    parser.add_argument("arguments", nargs=argparse.REMAINDER)
    options = parser.parse_args(arguments)
    if options.print_logon_task:
        if options.apply_prepared or options.bootstrap or options.arguments or options.install_root is None:
            raise ValueError("logon task generation arguments differ")
        print(json.dumps(install_task_spec(options.install_root, options.operator_sid)))
        return 0
    if options.apply_prepared:
        if options.bootstrap or options.arguments or options.control_root is None:
            raise ValueError("system updater arguments differ")
        expected = Path(__file__).resolve().parents[2] / "update-control"
        if options.control_root.absolute() != expected:
            raise ValueError("system updater control root differs")
        return apply_prepared(expected)
    if options.bootstrap:
        args = options.arguments[1:] if options.arguments[:1] == ["--"] else options.arguments
        if control_root(Path(__file__).resolve().parent) is None:
            from Label_Match import main as application_main

            return int(application_main(args) or 0)
        return bootstrap(Path(__file__).resolve().parent, args, role=options.role)
    # Source/old-layout launch remains the ordinary portable main entrypoint.
    from Label_Match import main as application_main

    return int(application_main(options.arguments) or 0)


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as error:
        __import__("failure_redaction").emit_support_failure(error)
        raise SystemExit(1)
