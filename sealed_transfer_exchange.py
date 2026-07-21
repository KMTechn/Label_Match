"""Durable controlled member replacement for a sealed TRANSFER bundle.

The server performs target/source CAS, moves damaged units to process-damage
hold, invalidates the old seal and issues a new opaque seal.  This module keeps
the exact command and receipt durable across the two crash windows around the
local Label_Match state update.
"""

from __future__ import annotations

from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import sqlite3
from typing import Any, Iterable, Mapping
import unicodedata
from urllib.parse import unquote

from package_logistics import (
    PackageApiError,
    PackageLogisticsClient,
    PackageLogisticsError,
    PackageTransportError,
    barcode_membership_hash,
    canonical_barcodes,
    canonical_member_ids,
    membership_hash,
)


SCHEMA_VERSION = "label-match-sealed-transfer-exchange-v1"
COMMAND_TYPE = "REPLACE_SEALED_TRANSFER_MEMBERS"
CAPABILITY_ID = "sealed_transfer_member_replacement_v1"
RECEIPT_CONTRACT_VERSION = "sealed-transfer-member-replacement-v1"
SEAL_QR_CONTRACT_VERSION = "transfer-seal-qr-v1"
MAX_PAIRS = 2
PENDING_STATUSES = (
    "PREPARED",
    "COMMAND_READY",
    "RETRY_WAIT",
    "OPERATOR_REVIEW",
)


class SealedTransferExchangeError(PackageLogisticsError):
    def __init__(self, code: str, message: str) -> None:
        self.code = str(code or "SEALED_TRANSFER_EXCHANGE_ERROR")
        self.message = str(message or "sealed transfer exchange failed")
        super().__init__(f"{self.code}: {self.message}")


def _now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _json(value: Any) -> str:
    return json.dumps(
        value,
        ensure_ascii=False,
        allow_nan=False,
        sort_keys=True,
        separators=(",", ":"),
    )


def _hash(value: Any) -> str:
    return hashlib.sha256(_json(value).encode("utf-8")).hexdigest()


def normalize_barcode(value: Any) -> str:
    return unicodedata.normalize("NFKC", str(value or "")).strip().upper()


def _qr_fields(payload: str) -> dict[str, str]:
    return {
        key.strip().upper(): unquote(value.strip())
        for key, value in (
            part.split("=", 1) for part in str(payload or "").split("|") if "=" in part
        )
    }


def _member_pairs(values: Any) -> tuple[tuple[str, str], ...]:
    if not isinstance(values, (list, tuple)):
        raise PackageLogisticsError("exact unit/barcode member rows are missing")
    pairs: list[tuple[str, str]] = []
    for value in values:
        if not isinstance(value, Mapping):
            raise PackageLogisticsError("exact unit/barcode member row is invalid")
        unit_id = str(value.get("unit_id") or "").strip()
        barcode = normalize_barcode(value.get("normalized_barcode"))
        if not unit_id or not barcode:
            raise PackageLogisticsError("exact unit/barcode member identifier is missing")
        pairs.append((unit_id, barcode))
    ordered = tuple(sorted(pairs))
    if (
        len({unit_id for unit_id, _barcode in ordered}) != len(ordered)
        or len({barcode for _unit_id, barcode in ordered}) != len(ordered)
    ):
        raise PackageLogisticsError("exact unit/barcode member rows are ambiguous")
    return ordered


def _identifier(value: Any, field: str) -> str:
    normalized = unicodedata.normalize("NFKC", str(value or "")).strip()
    if not normalized or "\x00" in normalized:
        raise PackageLogisticsError(f"{field} is missing or unsafe")
    return normalized


def _positive_int(value: Any, field: str, *, allow_zero: bool = False) -> int:
    lower = 0 if allow_zero else 1
    if isinstance(value, bool) or not isinstance(value, int) or value < lower:
        raise PackageLogisticsError(f"{field} is invalid")
    return value


def _exact_bundle(
    projection: Mapping[str, Any],
    *,
    bundle_type: str,
    location: str,
) -> dict[str, Any]:
    if not isinstance(projection, Mapping):
        raise PackageLogisticsError("exact bundle projection is missing")
    bundle = dict(projection)
    bundle_id = _identifier(bundle.get("bundle_id"), "bundle_id")
    if (
        str(bundle.get("bundle_type") or "").upper() != bundle_type
        or str(bundle.get("bundle_state") or "").upper() != "AVAILABLE"
        or str(bundle.get("current_location") or "").upper() != location
    ):
        raise PackageLogisticsError(
            f"replacement bundle {bundle_id} is not AVAILABLE at {location}"
        )
    raw_ids = bundle.get("member_ids")
    raw_rows = bundle.get("members")
    if not isinstance(raw_ids, list) or not isinstance(raw_rows, list):
        raise PackageLogisticsError("exact bundle membership is missing")
    normalized_ids = tuple(str(value or "").strip() for value in raw_ids)
    member_ids = canonical_member_ids(normalized_ids)
    if (
        not member_ids
        or len(normalized_ids) != len(member_ids)
        or any(not value for value in normalized_ids)
        or bundle.get("member_count") != len(member_ids)
        or str(bundle.get("membership_hash") or "").lower()
        != membership_hash(member_ids)
        or len(raw_rows) != len(member_ids)
    ):
        raise PackageLogisticsError("exact bundle membership evidence is invalid")
    by_barcode: dict[str, dict[str, Any]] = {}
    row_ids: list[str] = []
    for raw_row in raw_rows:
        if not isinstance(raw_row, Mapping):
            raise PackageLogisticsError("exact bundle member row is invalid")
        row = dict(raw_row)
        unit_id = str(row.get("unit_id") or "").strip()
        barcode = normalize_barcode(row.get("normalized_barcode"))
        if not unit_id or not barcode or barcode in by_barcode:
            raise PackageLogisticsError("exact bundle barcode mapping is ambiguous")
        row_ids.append(unit_id)
        by_barcode[barcode] = row
    barcodes = canonical_barcodes(by_barcode)
    member_pairs = _member_pairs(
        [
            {
                "unit_id": row_ids[index],
                "normalized_barcode": normalize_barcode(raw_rows[index].get("normalized_barcode")),
            }
            for index in range(len(raw_rows))
        ]
    )
    barcode_member_count = bundle.get("barcode_member_count")
    barcode_membership_digest = str(
        bundle.get("barcode_membership_hash") or ""
    ).lower()
    active_seal = bundle.get("active_seal")
    if (
        (barcode_member_count is None or not barcode_membership_digest)
        and isinstance(active_seal, Mapping)
    ):
        barcode_member_count = active_seal.get("sealed_member_count")
        barcode_membership_digest = str(
            active_seal.get("sealed_barcode_membership_hash") or ""
        ).lower()
    if (
        len(row_ids) != len(set(row_ids))
        or set(row_ids) != set(member_ids)
        or barcode_member_count != len(barcodes)
        or barcode_membership_digest != barcode_membership_hash(barcodes)
    ):
        raise PackageLogisticsError("exact bundle barcode membership is invalid")
    return {
        "bundle": bundle,
        "bundle_id": bundle_id,
        "member_ids": member_ids,
        "membership_hash": membership_hash(member_ids),
        "barcodes": barcodes,
        "barcode_membership_hash": barcode_membership_hash(barcodes),
        "by_barcode": by_barcode,
        "members": tuple(
            {"unit_id": unit_id, "normalized_barcode": barcode}
            for unit_id, barcode in member_pairs
        ),
        "entity_version": _positive_int(bundle.get("entity_version"), "entity_version"),
        "authority_scope_id": _identifier(
            bundle.get("authority_scope_id"), "authority_scope_id"
        ),
        "authority_epoch": _positive_int(
            bundle.get("authority_epoch"), "authority_epoch", allow_zero=True
        ),
        "ledger_plane": _identifier(bundle.get("ledger_plane"), "ledger_plane").upper(),
        "plane_epoch": _positive_int(bundle.get("plane_epoch"), "plane_epoch"),
        "item_id": _identifier(bundle.get("item_id"), "item_id"),
        "inbound_iin": _identifier(
            bundle.get("source_iin") or bundle.get("inbound_iin"), "inbound_iin"
        ),
        "uom": _identifier(bundle.get("uom"), "uom"),
    }


@dataclass(frozen=True)
class SealedTransferExchangeAttempt:
    intent_id: str
    set_id: str
    status: str
    seal_verification_status: str
    local_apply_status: str
    old_barcodes: tuple[str, ...] = ()
    new_barcodes: tuple[str, ...] = ()
    target_bundle_id: str = ""
    damage_bundle_id: str = ""
    old_seal_qr_payload: str = ""
    new_seal_qr_payload: str = ""
    receipt_id: str = ""
    idempotency_key: str = ""
    entity_versions: dict[str, int] = field(default_factory=dict)
    error_code: str = ""
    error_message: str = ""

    @property
    def retryable(self) -> bool:
        return self.status == "RETRY_WAIT"


class SealedTransferExchangeStore:
    def __init__(self, db_path: str | Path):
        self.db_path = str(Path(db_path))
        Path(self.db_path).parent.mkdir(parents=True, exist_ok=True)
        self._initialize()

    @contextmanager
    def _connect(self):
        conn = sqlite3.connect(self.db_path, timeout=10.0)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA busy_timeout=10000")
        try:
            yield conn
        finally:
            conn.close()

    def _initialize(self) -> None:
        with self._connect() as conn:
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute("PRAGMA synchronous=FULL")
            conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS sealed_transfer_exchange_intents (
                    intent_id TEXT PRIMARY KEY,
                    schema_version TEXT NOT NULL,
                    set_id TEXT NOT NULL,
                    status TEXT NOT NULL CHECK(status IN (
                        'PREPARED','COMMAND_READY','RETRY_WAIT','ACKED','OPERATOR_REVIEW'
                    )),
                    seal_verification_status TEXT NOT NULL DEFAULT 'PENDING'
                        CHECK(seal_verification_status IN ('PENDING','VERIFIED','OPERATOR_REVIEW')),
                    local_apply_status TEXT NOT NULL DEFAULT 'PENDING'
                        CHECK(local_apply_status IN ('PENDING','APPLIED','OPERATOR_REVIEW')),
                    target_bundle_id TEXT NOT NULL,
                    item_id TEXT NOT NULL,
                    authority_scope_id TEXT NOT NULL,
                    operator TEXT NOT NULL,
                    old_seal_qr_payload TEXT NOT NULL,
                    old_seal_fields_json TEXT NOT NULL,
                    old_barcodes_json TEXT NOT NULL,
                    new_barcodes_json TEXT NOT NULL,
                    pair_count INTEGER NOT NULL CHECK(pair_count BETWEEN 1 AND 2),
                    intent_hash TEXT NOT NULL UNIQUE,
                    command_id TEXT UNIQUE,
                    command_json TEXT,
                    command_hash TEXT,
                    receipt_json TEXT,
                    new_seal_qr_payload TEXT,
                    seal_verified_at TEXT,
                    local_apply_receipt_json TEXT,
                    last_error_code TEXT,
                    last_error_message TEXT,
                    attempt_count INTEGER NOT NULL DEFAULT 0,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    CHECK((command_json IS NULL) = (command_id IS NULL)),
                    CHECK((command_json IS NULL) = (command_hash IS NULL))
                );
                CREATE INDEX IF NOT EXISTS ix_sealed_transfer_exchange_pending
                    ON sealed_transfer_exchange_intents(status,seal_verification_status,
                                                        local_apply_status,created_at);
                CREATE INDEX IF NOT EXISTS ix_sealed_transfer_exchange_set
                    ON sealed_transfer_exchange_intents(set_id,created_at);
                CREATE TRIGGER IF NOT EXISTS trg_sealed_transfer_exchange_command_immutable
                BEFORE UPDATE OF command_id,command_json,command_hash
                ON sealed_transfer_exchange_intents
                WHEN OLD.command_json IS NOT NULL AND (
                    NEW.command_id <> OLD.command_id OR
                    NEW.command_json <> OLD.command_json OR
                    NEW.command_hash <> OLD.command_hash
                )
                BEGIN SELECT RAISE(ABORT, 'sealed transfer exchange command is immutable'); END;
                """
            )

    def prepare(
        self,
        *,
        set_id: str,
        target_bundle_id: str,
        item_id: str,
        authority_scope_id: str,
        operator: str,
        old_seal_qr_payload: str,
        old_seal_fields: Mapping[str, Any],
        old_barcodes: Iterable[str],
        new_barcodes: Iterable[str],
    ) -> sqlite3.Row:
        old_values = tuple(normalize_barcode(value) for value in old_barcodes)
        new_values = tuple(normalize_barcode(value) for value in new_barcodes)
        if (
            not 1 <= len(old_values) <= MAX_PAIRS
            or len(old_values) != len(new_values)
            or len(set(old_values)) != len(old_values)
            or len(set(new_values)) != len(new_values)
            or set(old_values) & set(new_values)
        ):
            raise PackageLogisticsError(
                "sealed transfer exchange requires one or two unique barcode pairs"
            )
        material = {
            "set_id": _identifier(set_id, "set_id"),
            "target_bundle_id": _identifier(target_bundle_id, "target_bundle_id"),
            "item_id": _identifier(item_id, "item_id"),
            "authority_scope_id": _identifier(
                authority_scope_id, "authority_scope_id"
            ),
            "old_seal_qr_payload": _identifier(
                old_seal_qr_payload, "old_seal_qr_payload"
            ),
            "old_seal_fields": dict(old_seal_fields),
            "old_barcodes": list(old_values),
            "new_barcodes": list(new_values),
        }
        digest = _hash(material)
        intent_id = f"label-sealed-transfer-exchange-{digest[:32]}"
        now = _now()
        with self._connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            conn.execute(
                """INSERT OR IGNORE INTO sealed_transfer_exchange_intents (
                       intent_id,schema_version,set_id,status,target_bundle_id,item_id,
                       authority_scope_id,operator,old_seal_qr_payload,
                       old_seal_fields_json,old_barcodes_json,new_barcodes_json,
                       pair_count,intent_hash,created_at,updated_at
                   ) VALUES (?,?,?,'PREPARED',?,?,?,?,?,?,?,?,?,?,?,?)""",
                (
                    intent_id,
                    SCHEMA_VERSION,
                    material["set_id"],
                    material["target_bundle_id"],
                    material["item_id"],
                    material["authority_scope_id"],
                    str(operator or "").strip(),
                    material["old_seal_qr_payload"],
                    _json(material["old_seal_fields"]),
                    _json(material["old_barcodes"]),
                    _json(material["new_barcodes"]),
                    len(old_values),
                    digest,
                    now,
                    now,
                ),
            )
            row = conn.execute(
                "SELECT * FROM sealed_transfer_exchange_intents WHERE intent_id=?",
                (intent_id,),
            ).fetchone()
            conn.commit()
        assert row is not None
        return row

    def load(self, intent_id: str) -> sqlite3.Row:
        with self._connect() as conn:
            row = conn.execute(
                "SELECT * FROM sealed_transfer_exchange_intents WHERE intent_id=?",
                (intent_id,),
            ).fetchone()
        if row is None:
            raise KeyError(intent_id)
        return row

    def bind_command(self, intent_id: str, command: Mapping[str, Any]) -> sqlite3.Row:
        encoded = _json(dict(command))
        digest = hashlib.sha256(encoded.encode("utf-8")).hexdigest()
        command_id = _identifier(command.get("idempotency_key"), "idempotency_key")
        with self._connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            row = conn.execute(
                "SELECT * FROM sealed_transfer_exchange_intents WHERE intent_id=?",
                (intent_id,),
            ).fetchone()
            if row is None:
                raise KeyError(intent_id)
            if row["command_json"]:
                if (
                    row["command_id"] != command_id
                    or row["command_json"] != encoded
                    or row["command_hash"] != digest
                ):
                    raise PackageLogisticsError(
                        "durable sealed transfer exchange command changed"
                    )
            else:
                conn.execute(
                    """UPDATE sealed_transfer_exchange_intents
                          SET status='COMMAND_READY',command_id=?,command_json=?,
                              command_hash=?,last_error_code=NULL,
                              last_error_message=NULL,updated_at=?
                        WHERE intent_id=? AND command_json IS NULL""",
                    (command_id, encoded, digest, _now(), intent_id),
                )
            row = conn.execute(
                "SELECT * FROM sealed_transfer_exchange_intents WHERE intent_id=?",
                (intent_id,),
            ).fetchone()
            conn.commit()
        assert row is not None
        return row

    def record_error(self, intent_id: str, error: Exception) -> sqlite3.Row:
        code = str(getattr(error, "code", "SEALED_TRANSFER_EXCHANGE_ERROR"))
        committed = getattr(error, "committed", None)
        terminal_api_error = isinstance(error, PackageApiError) and int(
            getattr(error, "status_code", 0) or 0
        ) in {400, 403, 409, 412, 422}
        unknown_api_outcome = isinstance(error, PackageApiError) and (
            committed is None
            or int(getattr(error, "status_code", 0) or 0) >= 500
        )
        # Unknown transport outcome remains retryable so the immutable command
        # can recover by receipt; deterministic CAS/contract failures need review.
        retryable = not terminal_api_error and (
            isinstance(error, PackageTransportError)
            or unknown_api_outcome
            or bool(getattr(error, "retryable", False))
        )
        status = "RETRY_WAIT" if retryable else "OPERATOR_REVIEW"
        with self._connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            conn.execute(
                """UPDATE sealed_transfer_exchange_intents
                      SET status=?,last_error_code=?,last_error_message=?,
                          attempt_count=attempt_count+1,updated_at=?
                    WHERE intent_id=?""",
                (status, code, str(error), _now(), intent_id),
            )
            row = conn.execute(
                "SELECT * FROM sealed_transfer_exchange_intents WHERE intent_id=?",
                (intent_id,),
            ).fetchone()
            conn.commit()
        assert row is not None
        return row

    def record_receipt(
        self, intent_id: str, receipt: Mapping[str, Any], new_seal_qr_payload: str
    ) -> sqlite3.Row:
        with self._connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            conn.execute(
                """UPDATE sealed_transfer_exchange_intents
                      SET status='ACKED',receipt_json=?,new_seal_qr_payload=?,
                          last_error_code=NULL,last_error_message=NULL,
                          attempt_count=attempt_count+1,updated_at=?
                    WHERE intent_id=?""",
                (
                    _json(dict(receipt)),
                    _identifier(new_seal_qr_payload, "new_seal_qr_payload"),
                    _now(),
                    intent_id,
                ),
            )
            row = conn.execute(
                "SELECT * FROM sealed_transfer_exchange_intents WHERE intent_id=?",
                (intent_id,),
            ).fetchone()
            conn.commit()
        assert row is not None
        return row

    def mark_seal_verified(self, intent_id: str, scanned_qr: str) -> sqlite3.Row:
        with self._connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            row = conn.execute(
                "SELECT * FROM sealed_transfer_exchange_intents WHERE intent_id=?",
                (intent_id,),
            ).fetchone()
            if row is None:
                raise KeyError(intent_id)
            if row["status"] != "ACKED" or not row["new_seal_qr_payload"]:
                raise PackageLogisticsError("central reseal must be ACKed first")
            if str(scanned_qr or "").strip() != str(row["new_seal_qr_payload"]):
                raise PackageLogisticsError("scanned seal QR is not the newly issued seal")
            conn.execute(
                """UPDATE sealed_transfer_exchange_intents
                      SET seal_verification_status='VERIFIED',seal_verified_at=?,updated_at=?
                    WHERE intent_id=? AND seal_verification_status='PENDING'""",
                (_now(), _now(), intent_id),
            )
            row = conn.execute(
                "SELECT * FROM sealed_transfer_exchange_intents WHERE intent_id=?",
                (intent_id,),
            ).fetchone()
            conn.commit()
        assert row is not None
        return row

    def mark_local_applied(
        self, intent_id: str, evidence: Mapping[str, Any]
    ) -> sqlite3.Row:
        encoded = _json(dict(evidence or {}))
        with self._connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            row = conn.execute(
                "SELECT * FROM sealed_transfer_exchange_intents WHERE intent_id=?",
                (intent_id,),
            ).fetchone()
            if row is None:
                raise KeyError(intent_id)
            if row["status"] != "ACKED" or row["seal_verification_status"] != "VERIFIED":
                raise PackageLogisticsError("new seal must be ACKed and scan-verified")
            if row["local_apply_status"] == "APPLIED":
                if row["local_apply_receipt_json"] != encoded:
                    raise PackageLogisticsError("local apply evidence is immutable")
            else:
                conn.execute(
                    """UPDATE sealed_transfer_exchange_intents
                          SET local_apply_status='APPLIED',local_apply_receipt_json=?,updated_at=?
                        WHERE intent_id=? AND local_apply_status='PENDING'""",
                    (encoded, _now(), intent_id),
                )
            row = conn.execute(
                "SELECT * FROM sealed_transfer_exchange_intents WHERE intent_id=?",
                (intent_id,),
            ).fetchone()
            conn.commit()
        assert row is not None
        return row

    def mark_local_review(self, intent_id: str, reason: str) -> sqlite3.Row:
        with self._connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            conn.execute(
                """UPDATE sealed_transfer_exchange_intents
                      SET local_apply_status='OPERATOR_REVIEW',
                          last_error_code='LOCAL_APPLY_CONFLICT',
                          last_error_message=?,updated_at=?
                    WHERE intent_id=? AND status='ACKED' AND local_apply_status='PENDING'""",
                (str(reason or "local packaging state changed"), _now(), intent_id),
            )
            row = conn.execute(
                "SELECT * FROM sealed_transfer_exchange_intents WHERE intent_id=?",
                (intent_id,),
            ).fetchone()
            conn.commit()
        assert row is not None
        return row

    def pending_ids(self) -> list[str]:
        placeholders = ",".join("?" for _ in PENDING_STATUSES)
        with self._connect() as conn:
            rows = conn.execute(
                f"""SELECT intent_id FROM sealed_transfer_exchange_intents
                      WHERE status IN ({placeholders}) ORDER BY created_at""",
                PENDING_STATUSES,
            ).fetchall()
        return [str(row["intent_id"]) for row in rows]

    def pending_local(self, *, set_id: str = "") -> list[sqlite3.Row]:
        query = (
            "SELECT * FROM sealed_transfer_exchange_intents "
            "WHERE status='ACKED' AND local_apply_status='PENDING'"
        )
        params: tuple[Any, ...] = ()
        if set_id:
            query += " AND set_id=?"
            params = (str(set_id),)
        query += " ORDER BY created_at"
        with self._connect() as conn:
            return list(conn.execute(query, params).fetchall())

    def blocking_rows(self, *, set_id: str = "") -> list[sqlite3.Row]:
        query = (
            "SELECT * FROM sealed_transfer_exchange_intents WHERE "
            "(status IN ('PREPARED','COMMAND_READY','RETRY_WAIT','OPERATOR_REVIEW') OR "
            " (status='ACKED' AND (seal_verification_status!='VERIFIED' OR "
            " local_apply_status!='APPLIED')))"
        )
        params: tuple[Any, ...] = ()
        if set_id:
            query += " AND set_id=?"
            params = (str(set_id),)
        query += " ORDER BY created_at"
        with self._connect() as conn:
            return list(conn.execute(query, params).fetchall())


class SealedTransferExchangeCoordinator:
    def __init__(
        self,
        store: SealedTransferExchangeStore,
        client: PackageLogisticsClient | None,
    ):
        self.store = store
        self.client = client

    def prepare(
        self,
        *,
        set_id: str,
        old_seal_qr_payload: str,
        old_seal_fields: Mapping[str, Any],
        operator: str,
        old_barcodes: Iterable[str],
        new_barcodes: Iterable[str],
    ) -> SealedTransferExchangeAttempt:
        row = self.store.prepare(
            set_id=set_id,
            target_bundle_id=str(old_seal_fields.get("BND") or ""),
            item_id=str(old_seal_fields.get("CLC") or ""),
            authority_scope_id=str(old_seal_fields.get("AUTH_SCOPE") or ""),
            operator=operator,
            old_seal_qr_payload=old_seal_qr_payload,
            old_seal_fields=old_seal_fields,
            old_barcodes=old_barcodes,
            new_barcodes=new_barcodes,
        )
        return self._attempt(row)

    def _require_capability(self) -> None:
        if self.client is None:
            raise PackageLogisticsError("central logistics client is not configured")
        raw = self.client.get_capabilities()
        ids = raw.get("capability_ids")
        capabilities = raw.get("capabilities")
        capability = (
            capabilities.get(CAPABILITY_ID)
            if isinstance(capabilities, Mapping)
            else None
        )
        if (
            not isinstance(ids, list)
            or CAPABILITY_ID not in ids
            or not isinstance(capability, Mapping)
            or capability.get("enabled") is not True
            or capability.get("command_type") != COMMAND_TYPE
            or capability.get("endpoint_template")
            != "/logistics/api/v1/transfers/{target_bundle_id}/members/replace-and-reseal"
            or capability.get("receipt_contract_version")
            != RECEIPT_CONTRACT_VERSION
            or capability.get("seal_qr_contract_version")
            != SEAL_QR_CONTRACT_VERSION
            or capability.get("max_pairs") != MAX_PAIRS
            or capability.get("atomic") is not True
            or capability.get("fail_closed_when_unavailable") is not True
            or capability.get("disabled_server_behavior")
            != "REJECT_COMMAND_DO_NOT_MUTATE_LOCAL_STATE"
            or capability.get("client_rollout_gate")
            != "REQUIRE_ENABLED_CAPABILITY_AND_EXACT_RECEIPT"
            or capability.get("replacement_source_bundle_cardinality")
            != "EXACTLY_ONE_ACTIVE_MEMBER"
            or capability.get("multi_member_source_policy")
            != "REJECT_STALE_PHYSICAL_LABEL"
            or capability.get("multi_member_source_error_code")
            != "REPLACEMENT_SOURCE_NOT_SINGLETON"
        ):
            raise PackageLogisticsError(
                "server does not advertise controlled sealed-transfer replacement"
            )

    @staticmethod
    def _validate_active_seal(
        target: dict[str, Any],
        *,
        old_qr: str,
        old_fields: Mapping[str, Any],
    ) -> dict[str, Any]:
        bundle = target["bundle"]
        raw = bundle.get("active_seal")
        if not isinstance(raw, Mapping):
            raise PackageLogisticsError("sealed transfer active seal evidence is missing")
        seal = dict(raw)
        required_matches = {
            "seal_contract_version": SEAL_QR_CONTRACT_VERSION,
            "seal_state": "ACTIVE",
            "sealed_bundle_id": target["bundle_id"],
            "sealed_bundle_version": target["entity_version"],
            "sealed_member_count": len(target["member_ids"]),
            "sealed_membership_hash": target["membership_hash"],
            "sealed_barcode_membership_hash": target["barcode_membership_hash"],
            "seal_qr_payload": old_qr,
        }
        if any(seal.get(key) != value for key, value in required_matches.items()):
            raise PackageLogisticsError("printed transfer seal is stale")
        if (
            canonical_member_ids(seal.get("sealed_member_ids") or ())
            != target["member_ids"]
            or _member_pairs(seal.get("sealed_members"))
            != _member_pairs(target["members"])
            or canonical_barcodes(seal.get("sealed_normalized_barcodes") or ())
            != target["barcodes"]
            or not str(seal.get("seal_id") or "").strip()
            or not str(seal.get("seal_token") or "").strip()
            or _positive_int(seal.get("seal_revision"), "seal_revision")
            != int(old_fields.get("SREV") or 0)
            or str(seal.get("seal_id")) != str(old_fields.get("SID") or "")
            or str(seal.get("seal_token")) != str(old_fields.get("STK") or "")
            or target["bundle_id"] != str(old_fields.get("BND") or "")
            or target["authority_scope_id"]
            != str(old_fields.get("AUTH_SCOPE") or "")
            or target["item_id"] != str(old_fields.get("CLC") or "")
            or len(target["member_ids"]) != int(old_fields.get("QT") or 0)
            or target["membership_hash"] != str(old_fields.get("HSH") or "").lower()
            or target["authority_epoch"] != int(old_fields.get("EPOCH") or -1)
            or target["ledger_plane"] != str(old_fields.get("PLANE") or "").upper()
            or target["plane_epoch"] != int(old_fields.get("PE") or 0)
        ):
            raise PackageLogisticsError("active seal and scanned transfer QR differ")
        return seal

    @staticmethod
    def _good_source(resolved: Mapping[str, Any], barcode: str) -> dict[str, Any]:
        if (
            resolved.get("contract_version")
            != "logistics-good-replacement-source-v1"
            or resolved.get("candidate_count") != 1
            or not isinstance(resolved.get("source_bundle"), Mapping)
            or not isinstance(resolved.get("unit"), Mapping)
            or not isinstance(resolved.get("replacement_evidence"), Mapping)
        ):
            raise PackageLogisticsError("good replacement resolver contract is invalid")
        source_bundle = dict(resolved["source_bundle"])
        source_members = (
            source_bundle.get("members")
            if isinstance(source_bundle.get("members"), list)
            else []
        )
        source_barcodes = [
            normalize_barcode(row.get("normalized_barcode"))
            for row in source_members
            if isinstance(row, Mapping)
        ]
        source_bundle.update(
            {
                "authority_scope_id": resolved.get("authority_scope_id"),
                "authority_epoch": resolved.get("authority_epoch"),
                "ledger_plane": resolved.get("ledger_plane"),
                "plane_epoch": resolved.get("plane_epoch"),
                "source_iin": resolved.get("inbound_iin"),
                "barcode_member_count": len(source_barcodes),
                "barcode_membership_hash": (
                    barcode_membership_hash(source_barcodes)
                    if source_barcodes
                    else ""
                ),
            }
        )
        # Resolver source bundles are PHS, but may expose either good location.
        source_location = str(resolved["unit"].get("current_location") or "").upper()
        if source_location not in {"PHS_GOOD", "REWORK_GOOD_READY"}:
            raise PackageLogisticsError("replacement unit is not in a good-ready location")
        source_bundle["current_location"] = source_location
        source = _exact_bundle(
            source_bundle,
            bundle_type="PHS",
            location=source_location,
        )
        requested = normalize_barcode(barcode)
        unit = dict(resolved["unit"])
        unit_id = str(unit.get("unit_id") or "").strip()
        row = source["by_barcode"].get(requested)
        if (
            len(source["member_ids"]) != 1
            or len(source["by_barcode"]) != 1
            or source["member_ids"] != (unit_id,)
        ):
            raise SealedTransferExchangeError(
                "REPLACEMENT_SOURCE_NOT_SINGLETON",
                "replacement donor must be a PHS with exactly one active member",
            )
        evidence = dict(resolved["replacement_evidence"])
        if (
            normalize_barcode(unit.get("normalized_barcode")) != requested
            or not isinstance(row, Mapping)
            or str(row.get("unit_id") or "") != unit_id
            or str(resolved.get("source_bundle_id") or "") != source["bundle_id"]
            or resolved.get("source_bundle_entity_version")
            != source["entity_version"]
            or str(evidence.get("new_unit_id") or "") != unit_id
            or str(evidence.get("new_source_bundle_id") or "")
            != source["bundle_id"]
            or evidence.get("expected_source_bundle_version")
            != source["entity_version"]
            or canonical_member_ids(evidence.get("source_member_ids") or ())
            != source["member_ids"]
            or str(evidence.get("source_membership_hash") or "").lower()
            != source["membership_hash"]
        ):
            raise PackageLogisticsError("good replacement exact owner evidence differs")
        source["selected_unit_id"] = unit_id
        source["selected_barcode"] = requested
        return source

    def _build_command(self, row: sqlite3.Row) -> dict[str, Any]:
        self._require_capability()
        assert self.client is not None
        old_fields = json.loads(row["old_seal_fields_json"])
        target = _exact_bundle(
            self.client.get_bundle(
                row["target_bundle_id"],
                authority_scope_id=row["authority_scope_id"],
            ),
            bundle_type="TRANSFER",
            location="TRANSFER",
        )
        seal = self._validate_active_seal(
            target,
            old_qr=row["old_seal_qr_payload"],
            old_fields=old_fields,
        )
        old_barcodes = tuple(json.loads(row["old_barcodes_json"]))
        new_barcodes = tuple(json.loads(row["new_barcodes_json"]))
        old_units: list[str] = []
        for barcode in old_barcodes:
            target_row = target["by_barcode"].get(normalize_barcode(barcode))
            if not isinstance(target_row, Mapping):
                raise PackageLogisticsError(
                    "damaged barcode is not in the active sealed transfer"
                )
            old_units.append(_identifier(target_row.get("unit_id"), "old_unit_id"))
        good_sources: list[dict[str, Any]] = []
        for barcode in new_barcodes:
            source = self._good_source(
                self.client.resolve_good_source(
                    authority_scope_id=target["authority_scope_id"], barcode=barcode
                ),
                barcode,
            )
            if (
                source["authority_scope_id"] != target["authority_scope_id"]
                or source["authority_epoch"] != target["authority_epoch"]
                or source["ledger_plane"] != target["ledger_plane"]
                or source["plane_epoch"] != target["plane_epoch"]
                or source["item_id"] != target["item_id"]
                or source["inbound_iin"] != target["inbound_iin"]
                or source["uom"] != target["uom"]
                or source["bundle_id"] == target["bundle_id"]
            ):
                raise PackageLogisticsError(
                    "replacement good must have the same lot/item/uom/ledger identity"
                )
            good_sources.append(source)
        new_units = [source["selected_unit_id"] for source in good_sources]
        if len(set(old_units)) != len(old_units) or len(set(new_units)) != len(new_units):
            raise PackageLogisticsError("replacement unit mapping is duplicated")
        expected_versions = {
            f"bundle:{target['bundle_id']}": target["entity_version"]
        }
        pairs: list[dict[str, Any]] = []
        for old_unit, source in zip(old_units, good_sources, strict=True):
            key = f"bundle:{source['bundle_id']}"
            prior = expected_versions.get(key)
            if prior is not None and prior != source["entity_version"]:
                raise PackageLogisticsError("replacement source versions conflict")
            expected_versions[key] = source["entity_version"]
            pairs.append(
                {
                    "old_unit_id": old_unit,
                    "new_unit_id": source["selected_unit_id"],
                    "new_source_bundle_id": source["bundle_id"],
                    "new_source_evidence": {
                        "bundle_id": source["bundle_id"],
                        "entity_version": source["entity_version"],
                        "member_ids": list(source["member_ids"]),
                        "members": [dict(member) for member in source["members"]],
                        "member_count": len(source["member_ids"]),
                        "membership_hash": source["membership_hash"],
                        "normalized_barcodes": list(source["barcodes"]),
                        "barcode_membership_hash": source[
                            "barcode_membership_hash"
                        ],
                    },
                }
            )
        damage_bundle_id = (
            "PROCESS-DAMAGE-"
            + _hash(
                {
                    "target_bundle_id": target["bundle_id"],
                    "old_unit_ids": sorted(old_units),
                    "intent_hash": row["intent_hash"],
                }
            )[:24].upper()
        )
        expected_versions[f"bundle:{damage_bundle_id}"] = 0
        target_evidence = {
            "bundle_id": target["bundle_id"],
            "entity_version": target["entity_version"],
            "member_ids": list(target["member_ids"]),
            "members": [dict(member) for member in target["members"]],
            "member_count": len(target["member_ids"]),
            "membership_hash": target["membership_hash"],
            "normalized_barcodes": list(target["barcodes"]),
            "barcode_membership_hash": target["barcode_membership_hash"],
            "seal_id": seal["seal_id"],
            "seal_revision": seal["seal_revision"],
            "seal_token": seal["seal_token"],
            "seal_qr_payload": seal["seal_qr_payload"],
        }
        return {
            "contract_version": "logistics-v1",
            "command_type": COMMAND_TYPE,
            "authority_scope_id": target["authority_scope_id"],
            "authority_epoch": target["authority_epoch"],
            "ledger_plane": target["ledger_plane"],
            "plane_epoch": target["plane_epoch"],
            "idempotency_key": f"label-sealed-transfer-exchange:{row['intent_hash']}",
            "expected_versions": expected_versions,
            "payload": {
                "target_bundle_id": target["bundle_id"],
                "damage_bundle_id": damage_bundle_id,
                "damage_external_label": damage_bundle_id,
                "target_evidence": target_evidence,
                "pairs": pairs,
            },
            "client_exact_evidence": {
                "target_item_id": target["item_id"],
                "old_barcodes": list(old_barcodes),
                "new_barcodes": list(new_barcodes),
            },
            "reason": "label_match_prepackage_process_damage_exchange",
            "evidence_refs": [row["intent_id"], row["intent_hash"]],
        }

    @staticmethod
    def _validate_receipt(
        command: Mapping[str, Any], receipt: Mapping[str, Any]
    ) -> str:
        data_value = receipt.get("data")
        data = dict(data_value) if isinstance(data_value, Mapping) else dict(receipt)
        payload = command["payload"]
        target_before = set(payload["target_evidence"]["member_ids"])
        pairs = list(payload["pairs"])
        old_ids = {pair["old_unit_id"] for pair in pairs}
        new_ids = {pair["new_unit_id"] for pair in pairs}
        expected_members = canonical_member_ids((target_before - old_ids) | new_ids)
        old_barcodes = set(payload["target_evidence"]["normalized_barcodes"])
        client_evidence = command.get("client_exact_evidence") or {}
        expected_barcodes = canonical_barcodes(
            (old_barcodes - set(client_evidence.get("old_barcodes") or ()))
            | set(client_evidence.get("new_barcodes") or ())
        )
        versions_value = receipt.get("entity_versions")
        if not isinstance(versions_value, Mapping):
            versions_value = data.get("entity_versions")
        versions = dict(versions_value) if isinstance(versions_value, Mapping) else {}
        expected_versions = {
            key: (1 if int(version) == 0 else int(version) + 1)
            for key, version in command["expected_versions"].items()
        }
        actual_pairs = data.get("pairs")
        normalized_pairs = (
            sorted(
                (
                    str(pair.get("old_unit_id") or ""),
                    str(pair.get("new_unit_id") or ""),
                    str(pair.get("new_source_bundle_id") or ""),
                )
                for pair in actual_pairs
                if isinstance(pair, Mapping)
            )
            if isinstance(actual_pairs, list)
            else []
        )
        expected_pairs = sorted(
            (
                pair["old_unit_id"],
                pair["new_unit_id"],
                pair["new_source_bundle_id"],
            )
            for pair in pairs
        )
        target_evidence = payload["target_evidence"]
        target_member_pairs = _member_pairs(target_evidence.get("members"))
        target_member_map = dict(target_member_pairs)
        expected_member_map = {
            unit_id: barcode
            for unit_id, barcode in target_member_pairs
            if unit_id not in old_ids
        }
        expected_damage_pairs = tuple(
            sorted(
                (unit_id, target_member_map[unit_id])
                for unit_id in old_ids
            )
        )
        expected_sources: dict[str, dict[str, Any]] = {}
        supplied_new_barcodes = list(client_evidence.get("new_barcodes") or ())
        for pair_index, pair in enumerate(pairs):
            source_evidence = pair["new_source_evidence"]
            source_id = pair["new_source_bundle_id"]
            source = expected_sources.setdefault(
                source_id,
                {
                    "evidence": source_evidence,
                    "selected": set(),
                    "selected_barcodes": set(),
                },
            )
            if source["evidence"] != source_evidence:
                raise PackageLogisticsError(
                    "sealed transfer replacement source evidence conflicts"
                )
            source["selected"].add(pair["new_unit_id"])
            if pair_index < len(supplied_new_barcodes):
                source["selected_barcodes"].add(
                    normalize_barcode(supplied_new_barcodes[pair_index])
                )
            source_member_map = dict(
                _member_pairs(source_evidence.get("members"))
            )
            expected_member_map[pair["new_unit_id"]] = source_member_map[
                pair["new_unit_id"]
            ]
        expected_new_pairs = tuple(sorted(expected_member_map.items()))
        actual_sources = data.get("sources")
        source_rows = {
            str(source.get("source_bundle_id") or ""): source
            for source in (actual_sources if isinstance(actual_sources, list) else [])
            if isinstance(source, Mapping)
        }
        sources_valid = len(source_rows) == len(expected_sources)
        if sources_valid:
            for source_id, expected in expected_sources.items():
                actual = source_rows.get(source_id)
                evidence = expected["evidence"]
                selected = set(expected["selected"])
                selected_barcodes = set(expected["selected_barcodes"])
                before = set(evidence["member_ids"])
                remainder = canonical_member_ids(before - selected)
                remainder_barcodes = canonical_barcodes(
                    set(evidence["normalized_barcodes"]) - selected_barcodes
                )
                source_pairs = _member_pairs(evidence.get("members"))
                source_pair_map = dict(source_pairs)
                selected_pairs = tuple(
                    sorted((unit_id, source_pair_map[unit_id]) for unit_id in selected)
                )
                remainder_pairs = tuple(
                    pair for pair in source_pairs if pair[0] not in selected
                )
                # Unit-to-barcode pairing is already exact in each resolver
                # projection.  The receipt validates the authoritative unit
                # remainder; its barcode hash is independently checked below.
                if not isinstance(actual, Mapping) or (
                    actual.get("source_version_before")
                    != evidence["entity_version"]
                    or actual.get("source_version_after")
                    != evidence["entity_version"] + 1
                    or canonical_member_ids(actual.get("source_member_ids_before") or ())
                    != canonical_member_ids(evidence["member_ids"])
                    or _member_pairs(actual.get("source_members_before"))
                    != source_pairs
                    or actual.get("source_member_count_before")
                    != evidence["member_count"]
                    or str(actual.get("source_membership_hash_before") or "").lower()
                    != evidence["membership_hash"]
                    or canonical_barcodes(
                        actual.get("source_normalized_barcodes_before") or ()
                    )
                    != canonical_barcodes(evidence["normalized_barcodes"])
                    or str(
                        actual.get("source_barcode_membership_hash_before") or ""
                    ).lower()
                    != evidence["barcode_membership_hash"]
                    or canonical_member_ids(actual.get("selected_member_ids") or ())
                    != canonical_member_ids(selected)
                    or _member_pairs(actual.get("selected_members"))
                    != selected_pairs
                    or canonical_member_ids(actual.get("remainder_member_ids") or ())
                    != remainder
                    or _member_pairs(actual.get("remainder_members"))
                    != remainder_pairs
                    or actual.get("remainder_member_count") != len(remainder)
                    or str(actual.get("remainder_membership_hash") or "").lower()
                    != membership_hash(remainder)
                    or canonical_barcodes(
                        actual.get("remainder_normalized_barcodes") or ()
                    )
                    != remainder_barcodes
                    or str(
                        actual.get("remainder_barcode_membership_hash") or ""
                    ).lower()
                    != barcode_membership_hash(remainder_barcodes)
                    or actual.get("source_bundle_state_after")
                    != ("CONSUMED" if not remainder else "AVAILABLE")
                ):
                    sources_valid = False
                    break
        qr = str(data.get("seal_qr_payload") or "").strip()
        qr_fields = _qr_fields(qr)
        expected_qr_fields = {
            "TRF": "1",
            "BND": str(payload["target_bundle_id"]),
            "AUTH_SCOPE": str(command["authority_scope_id"]),
            "CLC": str(client_evidence.get("target_item_id") or ""),
            "QT": str(len(expected_members)),
            "HSH": membership_hash(expected_members),
            "EPOCH": str(command["authority_epoch"]),
            "PLANE": str(command["ledger_plane"]),
            "PE": str(command["plane_epoch"]),
            "SID": str(data.get("seal_id") or ""),
            "SREV": str(data.get("seal_revision") or ""),
            "STK": str(data.get("seal_token") or ""),
        }
        if (
            not str(receipt.get("receipt_id") or "").strip()
            or receipt.get("contract_version") != "logistics-v1"
            or receipt.get("command_type") != COMMAND_TYPE
            or str(receipt.get("status") or "").upper() != "COMMITTED"
            or receipt.get("authority_scope_id") != command["authority_scope_id"]
            or receipt.get("authority_epoch") != command["authority_epoch"]
            or str(receipt.get("resolved_ledger_plane") or "").upper()
            != command["ledger_plane"]
            or receipt.get("resolved_plane_epoch") != command["plane_epoch"]
            or not str(receipt.get("committed_at") or "").strip()
            or not isinstance(receipt.get("event_ids"), (list, tuple))
            or not receipt.get("event_ids")
            or not isinstance(receipt.get("outbox_ids"), (list, tuple))
            or not receipt.get("outbox_ids")
            or data.get("receipt_contract_version") != RECEIPT_CONTRACT_VERSION
            or data.get("idempotency_key") != command["idempotency_key"]
            or data.get("target_bundle_id") != payload["target_bundle_id"]
            or data.get("target_bundle_type") != "TRANSFER"
            or data.get("target_version_before") != target_evidence["entity_version"]
            or data.get("target_version_after")
            != target_evidence["entity_version"] + 1
            or data.get("old_seal_id") != target_evidence["seal_id"]
            or data.get("old_seal_revision") != target_evidence["seal_revision"]
            or data.get("old_seal_qr_payload") != target_evidence["seal_qr_payload"]
            or data.get("old_seal_token_hash")
            != hashlib.sha256(
                str(target_evidence["seal_token"]).encode("utf-8")
            ).hexdigest()
            or canonical_member_ids(data.get("old_member_ids") or ())
            != canonical_member_ids(target_evidence["member_ids"])
            or _member_pairs(data.get("old_members")) != target_member_pairs
            or data.get("old_member_count") != target_evidence["member_count"]
            or str(data.get("old_membership_hash") or "").lower()
            != target_evidence["membership_hash"]
            or canonical_barcodes(data.get("old_normalized_barcodes") or ())
            != canonical_barcodes(target_evidence["normalized_barcodes"])
            or str(data.get("old_barcode_membership_hash") or "").lower()
            != target_evidence["barcode_membership_hash"]
            or data.get("damage_bundle_id") != payload["damage_bundle_id"]
            or canonical_member_ids(data.get("damage_member_ids") or ())
            != canonical_member_ids(old_ids)
            or _member_pairs(data.get("damage_members")) != expected_damage_pairs
            or str(data.get("damage_membership_hash") or "").lower()
            != membership_hash(old_ids)
            or data.get("damage_location") != "PROCESS_DAMAGE_HOLD"
            or data.get("atomic") is not True
            or data.get("requires_reseal") is not True
            or data.get("resealed") is not True
            or not isinstance(data.get("movement_ids"), (list, tuple))
            or not data.get("movement_ids")
            or len({str(value).strip() for value in data.get("movement_ids")})
            != len(data.get("movement_ids"))
            or any(not str(value).strip() for value in data.get("movement_ids"))
            or data.get("pair_count") != len(pairs)
            or normalized_pairs != expected_pairs
            or not sources_valid
            or canonical_member_ids(data.get("new_member_ids") or ())
            != expected_members
            or _member_pairs(data.get("new_members")) != expected_new_pairs
            or data.get("new_member_count") != len(expected_members)
            or str(data.get("new_membership_hash") or "").lower()
            != membership_hash(expected_members)
            or canonical_barcodes(data.get("new_normalized_barcodes") or ())
            != expected_barcodes
            or str(data.get("new_barcode_membership_hash") or "").lower()
            != barcode_membership_hash(expected_barcodes)
            or canonical_member_ids(data.get("member_ids") or ()) != expected_members
            or _member_pairs(data.get("members")) != expected_new_pairs
            or data.get("member_count") != len(expected_members)
            or str(data.get("membership_hash") or "").lower()
            != membership_hash(expected_members)
            or canonical_barcodes(data.get("normalized_barcodes") or ())
            != expected_barcodes
            or str(data.get("barcode_membership_hash") or "").lower()
            != barcode_membership_hash(expected_barcodes)
            or data.get("seal_contract_version") != SEAL_QR_CONTRACT_VERSION
            or data.get("seal_state") != "ACTIVE"
            or data.get("sealed_bundle_id") != payload["target_bundle_id"]
            or data.get("sealed_bundle_version")
            != target_evidence["entity_version"] + 1
            or canonical_member_ids(data.get("sealed_member_ids") or ())
            != expected_members
            or _member_pairs(data.get("sealed_members")) != expected_new_pairs
            or data.get("sealed_member_count") != len(expected_members)
            or str(data.get("sealed_membership_hash") or "").lower()
            != membership_hash(expected_members)
            or canonical_barcodes(data.get("sealed_normalized_barcodes") or ())
            != expected_barcodes
            or str(data.get("sealed_barcode_membership_hash") or "").lower()
            != barcode_membership_hash(expected_barcodes)
            or data.get("seal_revision")
            != int(target_evidence["seal_revision"]) + 1
            or not str(data.get("seal_id") or "").strip()
            or not str(data.get("seal_token") or "").strip()
            or str(data.get("seal_token_hash") or "")
            != hashlib.sha256(str(data.get("seal_token") or "").encode("utf-8")).hexdigest()
            or data.get("seal_id") == target_evidence["seal_id"]
            or data.get("seal_token") == target_evidence["seal_token"]
            or data.get("new_seal_id") != data.get("seal_id")
            or data.get("new_seal_revision") != data.get("seal_revision")
            or data.get("new_seal_token") != data.get("seal_token")
            or data.get("new_seal_token_hash") != data.get("seal_token_hash")
            or data.get("new_seal_qr_payload") != qr
            or not qr
            or qr_fields != expected_qr_fields
            or any(versions.get(key) != value for key, value in expected_versions.items())
        ):
            raise PackageLogisticsError(
                "sealed transfer replacement receipt exact evidence differs"
            )
        return qr

    def attempt(self, intent_id: str) -> SealedTransferExchangeAttempt:
        row = self.store.load(intent_id)
        if row["status"] == "ACKED":
            return self._attempt(row)
        command_was_durable = row["command_json"] is not None
        operator_review = row["status"] == "OPERATOR_REVIEW"
        if operator_review and not command_was_durable:
            return self._attempt(row)
        try:
            if row["command_json"] is None:
                row = self.store.bind_command(intent_id, self._build_command(row))
            command = json.loads(row["command_json"])
            if self.client is None:
                raise PackageLogisticsError("central logistics client is not configured")
            if command_was_durable:
                receipt_lookup = getattr(self.client, "get_receipt_if_exists", None)
                if callable(receipt_lookup):
                    try:
                        receipt = receipt_lookup(
                            str(command["idempotency_key"]),
                            authority_scope_id=str(command["authority_scope_id"]),
                        )
                    except Exception:
                        if operator_review:
                            return self._attempt(row)
                        raise
                    if receipt is not None:
                        if operator_review:
                            try:
                                new_qr = self._validate_receipt(command, receipt)
                                row = self.store.record_receipt(
                                    intent_id, receipt, new_qr
                                )
                            except Exception:
                                return self._attempt(row)
                        else:
                            new_qr = self._validate_receipt(command, receipt)
                            row = self.store.record_receipt(
                                intent_id, receipt, new_qr
                            )
                        return self._attempt(row)
                if operator_review:
                    # A review row may represent a committed command whose ACK
                    # failed exact validation.  Never POST it again; only an
                    # exact idempotency receipt may move it back to ACKED.
                    return self._attempt(row)
            receipt = self.client.replace_and_reseal_transfer(command)
            new_qr = self._validate_receipt(command, receipt)
            row = self.store.record_receipt(intent_id, receipt, new_qr)
        except (PackageLogisticsError, PackageApiError, PackageTransportError) as exc:
            row = self.store.record_error(intent_id, exc)
        except Exception as exc:
            row = self.store.record_error(
                intent_id,
                PackageTransportError(
                    f"local sealed transfer exchange failed: {exc.__class__.__name__}"
                ),
            )
        return self._attempt(row)

    def drain_pending(self) -> list[SealedTransferExchangeAttempt]:
        return [self.attempt(intent_id) for intent_id in self.store.pending_ids()]

    def pending_local_attempts(
        self, *, set_id: str = ""
    ) -> list[SealedTransferExchangeAttempt]:
        return [self._attempt(row) for row in self.store.pending_local(set_id=set_id)]

    @staticmethod
    def _attempt(row: sqlite3.Row) -> SealedTransferExchangeAttempt:
        command = json.loads(row["command_json"]) if row["command_json"] else {}
        payload = command.get("payload") if isinstance(command.get("payload"), Mapping) else {}
        receipt = json.loads(row["receipt_json"]) if row["receipt_json"] else {}
        versions = receipt.get("entity_versions") if isinstance(receipt, Mapping) else {}
        return SealedTransferExchangeAttempt(
            intent_id=str(row["intent_id"]),
            set_id=str(row["set_id"]),
            status=str(row["status"]),
            seal_verification_status=str(row["seal_verification_status"]),
            local_apply_status=str(row["local_apply_status"]),
            old_barcodes=tuple(json.loads(row["old_barcodes_json"])),
            new_barcodes=tuple(json.loads(row["new_barcodes_json"])),
            target_bundle_id=str(row["target_bundle_id"]),
            damage_bundle_id=str(payload.get("damage_bundle_id") or ""),
            old_seal_qr_payload=str(row["old_seal_qr_payload"]),
            new_seal_qr_payload=str(row["new_seal_qr_payload"] or ""),
            receipt_id=str(receipt.get("receipt_id") or ""),
            idempotency_key=str(row["command_id"] or ""),
            entity_versions={
                str(key): int(value)
                for key, value in (
                    versions.items() if isinstance(versions, Mapping) else ()
                )
                if isinstance(value, int) and not isinstance(value, bool)
            },
            error_code=str(row["last_error_code"] or ""),
            error_message=str(row["last_error_message"] or ""),
        )


__all__ = [
    "CAPABILITY_ID",
    "COMMAND_TYPE",
    "MAX_PAIRS",
    "SealedTransferExchangeAttempt",
    "SealedTransferExchangeCoordinator",
    "SealedTransferExchangeStore",
    "normalize_barcode",
]
