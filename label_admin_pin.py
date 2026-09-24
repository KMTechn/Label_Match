"""Admin PIN transport and durable operation intents for package recovery."""

from __future__ import annotations

from contextvars import ContextVar
import hashlib
import hmac
import json
from pathlib import Path
import sqlite3
import urllib.error
import urllib.parse
import urllib.request
import unicodedata
import uuid
from typing import Any, Mapping

from direct_sync_push import SIGNATURE_VERSION
from direct_sync_runtime import load_credentials_from_json
from producer_runtime_client import canonical_json, _utc_now_text
from writer_session_fence import writer_sink


CONTRACT_VERSION = "admin-pin.v1"
FAILURE_MESSAGE = (
    "관리자 확인에 실패했습니다. ID와 PIN을 확인하세요. "
    "잠긴 경우 최상위 관리자에게 해제를 요청하세요."
)
active_pin_operation: ContextVar[tuple[str, str, str, str, str, str, str] | None] = (
    ContextVar("label_active_pin_operation", default=None)
)


class AdminPinError(RuntimeError):
    def __init__(self, code: str, *, unknown: bool = False):
        self.code = code
        self.unknown = unknown
        super().__init__(code)


def operator_message(code: str) -> str:
    if code in {"PIN_INVALID", "PROOF_INVALID", "PROOF_REVOKED"}:
        return FAILURE_MESSAGE
    if code == "TARGET_CHANGED":
        return "대상 또는 작업자가 바뀌었습니다. 다시 조회하세요."
    if code == "CENTRAL_ACTION_UNAVAILABLE":
        return "이 관리자 동작의 서버 확인을 사용할 수 없습니다. 이 건만 보류하고 다른 작업을 계속하세요."
    if code in {"PIN_BUSY", "PIN_RATE_LIMITED"}:
        return "관리자 확인이 바쁩니다. 잠시 뒤 다시 확인하세요. 이 건만 보류하고 다른 작업을 계속하세요."
    return "관리자 확인이 불확실합니다. 다시 확인하거나 이 건만 보류하고 다른 작업을 계속하세요."


def fingerprint(value: Mapping[str, Any]) -> str:
    return hashlib.sha256(canonical_json(value).encode("utf-8")).hexdigest()


def operator_local_id(worker_name: str) -> str:
    raw = unicodedata.normalize("NFC", worker_name.strip()).encode("utf-8")
    if not raw:
        raise AdminPinError("OPERATOR_MISSING")
    return ("wn:" + raw.hex()) if len(raw) <= 62 else ("wh:" + hashlib.sha256(raw).hexdigest())


def _unique_json_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("duplicate JSON key")
        result[key] = value
    return result


def _uuid(value: Any) -> bool:
    if type(value) is not str:
        return False
    try:
        return str(uuid.UUID(value)) == value
    except ValueError:
        return False


class AdminPinClient:
    def __init__(self, credentials: Any, *, tls_ca_bundle_path: str = "", transport: Any = None):
        parsed = urllib.parse.urlsplit(str(credentials.endpoint_url))
        if (parsed.scheme.lower() != "https" or not parsed.netloc
                or parsed.username or parsed.password):
            raise AdminPinError("VERIFY_UNAVAILABLE")
        self.origin = f"{parsed.scheme}://{parsed.netloc}"
        self.credentials = credentials
        self.transport = transport
        if transport is None:
            import ssl
            context = ssl.create_default_context(cafile=tls_ca_bundle_path or None)

            class NoRedirect(urllib.request.HTTPRedirectHandler):
                def redirect_request(self, request, fp, code, msg, headers, newurl):
                    return None

            self.opener = urllib.request.build_opener(
                urllib.request.HTTPSHandler(context=context), NoRedirect()
            )

    @classmethod
    def from_credential_path(cls, path: str | Path, *, tls_ca_bundle_path: str = ""):
        return cls(load_credentials_from_json(path), tls_ca_bundle_path=tls_ca_bundle_path)

    @writer_sink("label_admin_pin_request")
    def post(self, route: str, payload: Mapping[str, Any]) -> dict[str, Any]:
        path = "/api/admin-pin/v1/" + route
        body = canonical_json(payload).encode("utf-8")
        if len(body) > 8192:
            raise AdminPinError("REQUEST_TOO_LARGE")
        timestamp, nonce = _utc_now_text(), uuid.uuid4().hex
        identity = self.credentials
        digest = hashlib.sha256(body).hexdigest()
        signed = "\n".join((
            SIGNATURE_VERSION, "POST", path, "", timestamp, nonce,
            str(identity.producer_id), str(identity.key_id), digest, digest,
            str(len(body)), "application/json",
        ))
        secret = identity.secret
        secret_bytes = secret.encode("utf-8") if isinstance(secret, str) else secret
        headers = {
            "Content-Type": "application/json", "Accept": "application/json",
            "X-Producer-Id": str(identity.producer_id),
            "X-Producer-Key-Id": str(identity.key_id),
            "X-Producer-Timestamp": timestamp,
            "X-Producer-Nonce": nonce,
            "X-Producer-Signature": hmac.new(
                secret_bytes, signed.encode("utf-8"), hashlib.sha256
            ).hexdigest(),
        }
        try:
            if self.transport is not None:
                status, raw = self.transport(path, body, headers)
            else:
                request = urllib.request.Request(
                    self.origin + path, data=body, headers=headers, method="POST"
                )
                try:
                    response = self.opener.open(request, timeout=10)
                except urllib.error.HTTPError as exc:
                    response = exc
                if response.geturl() != self.origin + path:
                    raise AdminPinError("VERIFY_UNAVAILABLE", unknown=True)
                status, raw = int(response.status), response.read(8193)
        except AdminPinError:
            raise
        except Exception:
            raise AdminPinError("VERIFY_UNAVAILABLE", unknown=True) from None
        if len(raw) > 8192:
            raise AdminPinError("VERIFY_UNAVAILABLE", unknown=True)
        try:
            answer = json.loads(raw.decode("utf-8"), object_pairs_hook=_unique_json_object)
        except (ValueError, UnicodeError):
            raise AdminPinError("VERIFY_UNAVAILABLE", unknown=True) from None
        if not isinstance(answer, dict):
            raise AdminPinError("VERIFY_UNAVAILABLE", unknown=True)
        error = answer.get("error")
        code = str(error.get("code") or "") if isinstance(error, dict) else ""
        if status >= 500:
            raise AdminPinError("PIN_BUSY" if code == "PIN_BUSY" else "VERIFY_UNAVAILABLE", unknown=True)
        if not 200 <= status < 300 or answer.get("ok") is not True:
            raise AdminPinError(code or "VERIFY_UNAVAILABLE", unknown=not code)
        return answer

    def verify(self, *, request_id: str, admin_id: str, pin: str, action: str,
               target: Mapping[str, str], operator: Mapping[str, str]) -> dict[str, Any]:
        request = {"contract_version": CONTRACT_VERSION, "request_id": request_id,
                   "admin_id": admin_id, "pin": pin, "action": action,
                   "target": dict(target), "operator": dict(operator)}
        try:
            answer = self.post("verifications", request)
        except AdminPinError as exc:
            if not exc.unknown:
                raise
            answer = self.post("verifications", request)
        if (not _uuid(answer.get("verification_id"))
                or type(answer.get("proof")) is not str or not answer["proof"]
                or type(answer.get("issued_at")) is not int
                or type(answer.get("expires_at")) is not int
                or answer["issued_at"] <= 0 or answer["expires_at"] <= answer["issued_at"]
                or answer.get("admin_id") != admin_id or answer.get("action") != action
                or answer.get("target_fingerprint") != target["state_fingerprint"]
                or type(answer.get("device_ref")) is not str or not answer["device_ref"]
                or answer.get("request_id") != request_id):
            raise AdminPinError("VERIFY_UNAVAILABLE", unknown=True)
        return answer

    @staticmethod
    def _valid_consumption(answer: Mapping[str, Any], verification_id: str,
                           operation_key: str) -> bool:
        return (answer.get("verification_id") == verification_id
                and answer.get("operation_key") == operation_key
                and _uuid(answer.get("redemption_id"))
                and type(answer.get("consumed_at")) is int
                and answer["consumed_at"] > 0)

    def redeem(self, *, verification_id: str, proof: str, operation_key: str,
               action: str, target: Mapping[str, str], operator: Mapping[str, str]) -> None:
        request = {"contract_version": CONTRACT_VERSION,
                   "verification_id": verification_id, "proof": proof,
                   "operation_key": operation_key, "action": action,
                   "target": dict(target), "operator": dict(operator)}
        try:
            answer = self.post("redemptions", request)
        except AdminPinError as exc:
            if not exc.unknown:
                raise
        else:
            if not self._valid_consumption(answer, verification_id, operation_key):
                raise AdminPinError("VERIFY_UNAVAILABLE", unknown=True)
            return
        state = self.status(verification_id, operation_key)
        if state == "CONSUMED":
            return
        if state == "UNUSED":
            answer = self.post("redemptions", request)
            if self._valid_consumption(answer, verification_id, operation_key):
                return
        raise AdminPinError("VERIFY_UNAVAILABLE", unknown=True)

    def status(self, verification_id: str, operation_key: str) -> str:
        answer = self.post("redemptions/status", {
            "contract_version": CONTRACT_VERSION, "verification_id": verification_id,
            "operation_key": operation_key,
        })
        status = answer.get("status")
        if (answer.get("verification_id") != verification_id
                or answer.get("operation_key") != operation_key
                or type(status) is not str
                or status not in {"UNUSED", "CONSUMED", "EXPIRED"}
                or (status == "CONSUMED" and not self._valid_consumption(
                    answer, verification_id, operation_key
                )) or (status != "CONSUMED" and (
                    "redemption_id" in answer or "consumed_at" in answer
                ))):
            raise AdminPinError("VERIFY_UNAVAILABLE", unknown=True)
        return status


class AdminPinIntentStore:
    """Full-sync, secret-free intent records; the existing effect audit carries the key."""

    def __init__(self, path: str | Path):
        self.path = Path(path)

    def _connect(self, *, initialize: bool = False) -> sqlite3.Connection:
        if initialize:
            self.path.parent.mkdir(parents=True, exist_ok=True)
        conn = sqlite3.connect(self.path, timeout=10)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA synchronous=FULL")
        if initialize:
            conn.execute("PRAGMA journal_mode=DELETE")
            conn.execute("""CREATE TABLE IF NOT EXISTS admin_pin_intents (
                operation_key TEXT PRIMARY KEY, verification_id TEXT NOT NULL,
                action TEXT NOT NULL, target_kind TEXT NOT NULL, target_id TEXT NOT NULL,
                target_fingerprint TEXT NOT NULL, source_evidence_sha256 TEXT NOT NULL,
                operator_id TEXT NOT NULL, login_epoch TEXT NOT NULL, admin_id TEXT NOT NULL,
                state TEXT NOT NULL, error_code TEXT NOT NULL DEFAULT ''
            )""")
            conn.execute("""CREATE TABLE IF NOT EXISTS admin_pin_file_moves (
                operation_key TEXT NOT NULL, source_path TEXT NOT NULL,
                archive_path TEXT NOT NULL, source_sha256 TEXT NOT NULL,
                state TEXT NOT NULL, PRIMARY KEY(operation_key, source_path)
            )""")
        return conn

    @writer_sink("label_admin_pin_file_move")
    def stage_file_move(self, operation_key: str, source_path: str,
                        archive_path: str, source_sha256: str) -> None:
        """Commit the exact rename plan before touching the source file."""
        with self._connect(initialize=True) as conn:
            conn.execute("BEGIN IMMEDIATE")
            row = conn.execute("""SELECT archive_path, source_sha256, state
                FROM admin_pin_file_moves WHERE operation_key=? AND source_path=?""",
                (operation_key, source_path)).fetchone()
            if row:
                if (row["archive_path"] != archive_path
                        or row["source_sha256"] != source_sha256
                        or row["state"] == "RESTORED_CHANGED"):
                    raise AdminPinError("TARGET_CHANGED")
            else:
                conn.execute("""INSERT INTO admin_pin_file_moves VALUES (?,?,?,?,?)""",
                             (operation_key, source_path, archive_path, source_sha256,
                              "MOVING"))
        if not any(row["source_path"] == source_path
                   and row["archive_path"] == archive_path
                   and row["source_sha256"] == source_sha256
                   for row in self.file_moves(operation_key)):
            raise AdminPinError("INTENT_UNVERIFIED", unknown=True)

    def file_moves(self, operation_key: str) -> list[dict[str, str]]:
        if not self.path.exists():
            return []
        with self._connect() as conn:
            if not conn.execute("""SELECT 1 FROM sqlite_master WHERE type='table'
                AND name='admin_pin_file_moves'""").fetchone():
                return []
            return [dict(row) for row in conn.execute("""SELECT * FROM admin_pin_file_moves
                WHERE operation_key=? ORDER BY rowid""", (operation_key,))]

    def all_file_moves(self) -> list[dict[str, str]]:
        if not self.path.exists():
            return []
        with self._connect() as conn:
            if not conn.execute("""SELECT 1 FROM sqlite_master WHERE type='table'
                AND name='admin_pin_file_moves'""").fetchone():
                return []
            return [dict(row) for row in conn.execute("SELECT * FROM admin_pin_file_moves")]

    @writer_sink("label_admin_pin_file_move_state")
    def finish_file_move(self, operation_key: str, source_path: str, state: str) -> None:
        if state not in {"VERIFIED", "RESTORED_CHANGED"}:
            raise ValueError("invalid file move state")
        with self._connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            changed = conn.execute("""UPDATE admin_pin_file_moves SET state=?
                WHERE operation_key=? AND source_path=?""",
                (state, operation_key, source_path))
            if changed.rowcount != 1:
                raise AdminPinError("INTENT_MISSING", unknown=True)
        if not any(row["source_path"] == source_path and row["state"] == state
                   for row in self.file_moves(operation_key)):
            raise AdminPinError("INTENT_UNVERIFIED", unknown=True)

    def pending(self, kind: str, target_id: str) -> dict[str, str] | None:
        if not self.path.exists():
            return None
        with self._connect() as conn:
            row = conn.execute("""SELECT * FROM admin_pin_intents
                WHERE target_kind=? AND target_id=? AND state IN
                ('PREPARED','UNKNOWN','AUTHORIZED','UNVERIFIED_OPERATOR_HOLD')
                ORDER BY rowid DESC LIMIT 1""", (kind, target_id)).fetchone()
            return dict(row) if row else None

    def applied(self, kind: str, target_id: str, action: str) -> dict[str, str] | None:
        if not self.path.exists():
            return None
        with self._connect() as conn:
            row = conn.execute("""SELECT * FROM admin_pin_intents
                WHERE target_kind=? AND target_id=? AND action=? AND state='APPLIED'
                ORDER BY rowid DESC LIMIT 1""", (kind, target_id, action)).fetchone()
            return dict(row) if row else None

    def list_pending(self) -> list[dict[str, str]]:
        if not self.path.exists():
            return []
        with self._connect() as conn:
            return [dict(row) for row in conn.execute("""SELECT * FROM admin_pin_intents
                WHERE state IN ('PREPARED','UNKNOWN','AUTHORIZED','UNVERIFIED_OPERATOR_HOLD')
                ORDER BY rowid""")]

    def list_handed_off_unknown(self) -> list[dict[str, str]]:
        if not self.path.exists():
            return []
        with self._connect() as conn:
            return [dict(row) for row in conn.execute("""SELECT * FROM admin_pin_intents
                WHERE state='HANDED_OFF_UNKNOWN' ORDER BY rowid""")]

    @writer_sink("label_admin_pin_intent_prepare")
    def prepare(self, *, operation_key: str, verification_id: str, action: str,
                target: Mapping[str, str], operator: Mapping[str, str], admin_id: str,
                source_evidence_sha256: str) -> None:
        with self._connect(initialize=True) as conn:
            conn.execute("BEGIN IMMEDIATE")
            if conn.execute("""SELECT 1 FROM admin_pin_intents WHERE target_kind=?
                AND target_id=? AND state IN
                ('PREPARED','UNKNOWN','AUTHORIZED','UNVERIFIED_OPERATOR_HOLD')""",
                (target["kind"], target["id"])).fetchone():
                raise AdminPinError("TARGET_HELD")
            conn.execute("""INSERT INTO admin_pin_intents VALUES
                (?,?,?,?,?,?,?,?,?,?,?,?)""", (
                operation_key, verification_id, action, target["kind"], target["id"],
                target["state_fingerprint"], source_evidence_sha256,
                operator["local_id"], operator["login_epoch"], admin_id,
                "PREPARED", "",
            ))
        if self.pending(target["kind"], target["id"])["operation_key"] != operation_key:
            raise AdminPinError("INTENT_UNVERIFIED", unknown=True)

    @writer_sink("label_admin_pin_intent_transition")
    def transition(self, operation_key: str, state: str, *, error_code: str = "") -> None:
        if state not in {"AUTHORIZED", "APPLIED", "UNKNOWN", "UNVERIFIED_OPERATOR_HOLD", "SUPERSEDED", "HANDED_OFF_UNKNOWN"}:
            raise ValueError("invalid PIN intent state")
        with self._connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            prior = conn.execute(
                "SELECT state FROM admin_pin_intents WHERE operation_key=?", (operation_key,)
            ).fetchone()
            if prior is None:
                raise AdminPinError("INTENT_MISSING", unknown=True)
            if prior["state"] == "APPLIED" and state != "APPLIED":
                raise AdminPinError("INTENT_APPLIED", unknown=True)
            changed = conn.execute(
                "UPDATE admin_pin_intents SET state=?,error_code=? WHERE operation_key=?",
                (state, error_code, operation_key),
            )
            if changed.rowcount != 1:
                raise AdminPinError("INTENT_MISSING", unknown=True)
