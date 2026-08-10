"""Small dependency-free strict server fake for desktop contract tests."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Mapping

from .canonical import canonical_sha256
from .errors import FactoryContractError


LOGISTICS_CONTRACT_VERSION = "logistics-v1"
COMMAND_FIELDS = {
    "contract_version",
    "correlation_id",
    "idempotency_key",
    "expected_versions",
    "command_type",
    "occurred_at_utc",
    "payload_digest",
    "payload",
}


@dataclass
class StrictFactoryServer:
    """Deterministic in-memory boundary fake; it never performs I/O."""

    versions: dict[str, int] = field(default_factory=dict)
    receipts: dict[str, tuple[str, dict[str, Any]]] = field(default_factory=dict)

    def handle(self, command: Mapping[str, Any]) -> dict[str, Any]:
        if set(command) != COMMAND_FIELDS:
            raise FactoryContractError(
                "EVENT_SCHEMA_INVALID",
                "strict command fields are incomplete or unexpected",
            )
        received_version = command.get("contract_version")
        if received_version != LOGISTICS_CONTRACT_VERSION:
            raise FactoryContractError(
                "CONTRACT_VERSION_MISMATCH",
                "strict server contract version differs",
                details={
                    "expected_version": LOGISTICS_CONTRACT_VERSION,
                    "received_version": received_version,
                },
            )
        payload = command.get("payload")
        if not isinstance(payload, dict) or command.get("payload_digest") != canonical_sha256(payload):
            raise FactoryContractError("EVENT_SCHEMA_INVALID", "command payload digest differs")
        expected_versions = command.get("expected_versions")
        if not isinstance(expected_versions, dict) or any(
            isinstance(value, bool) or not isinstance(value, int) or value < 0
            for value in expected_versions.values()
        ):
            raise FactoryContractError("EVENT_SCHEMA_INVALID", "expected_versions is invalid")
        canonical_digest = canonical_sha256(dict(command))
        idempotency_key = str(command.get("idempotency_key") or "")
        if not idempotency_key:
            raise FactoryContractError("EVENT_SCHEMA_INVALID", "idempotency_key is required")
        existing = self.receipts.get(idempotency_key)
        if existing:
            if existing[0] != canonical_digest:
                raise FactoryContractError(
                    "IDEMPOTENCY_CONFLICT",
                    "idempotency key was reused with a different canonical command",
                )
            return {**existing[1], "status": "replayed"}
        mismatched = {
            aggregate: {"expected": expected, "received": self.versions.get(aggregate, 0)}
            for aggregate, expected in expected_versions.items()
            if self.versions.get(aggregate, 0) != expected
        }
        if mismatched:
            raise FactoryContractError(
                "CAS_VERSION_MISMATCH",
                "aggregate version compare-and-swap failed",
                details={"versions": mismatched},
            )
        next_versions = dict(expected_versions)
        for aggregate, expected in expected_versions.items():
            self.versions[aggregate] = expected + 1
            next_versions[aggregate] = expected + 1
        receipt = {
            "ok": True,
            "status": "committed",
            "committed": True,
            "retryable": False,
            "correlation_id": str(command["correlation_id"]),
            "contract_version": LOGISTICS_CONTRACT_VERSION,
            "expected_version": None,
            "received_version": None,
            "receipt": {
                "command_digest": canonical_digest,
                "entity_versions": next_versions,
            },
            "error": None,
        }
        self.receipts[idempotency_key] = (canonical_digest, receipt)
        return receipt
