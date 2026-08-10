"""Pure source-liveness semantics shared by server tests and installers."""

from __future__ import annotations

from typing import Any, Iterable, Mapping


def evaluate_source_liveness(evidence: Mapping[str, Any]) -> dict[str, Any]:
    enrolled = bool(
        evidence.get("active_credential_reference")
        and evidence.get("active_manifest_binding")
    )
    lease_active = bool(
        evidence.get("lease_state") == "ACTIVE"
        and evidence.get("lease_unexpired") is True
        and evidence.get("install_identity_match") is True
        and evidence.get("fence_identity_match") is True
    )
    manifest_match = evidence.get("manifest_match") is True
    quarantined = evidence.get("quarantined") is True
    profile_match = evidence.get("profile_match") is True
    transport_failed = evidence.get("recent_transport_failure") is True
    connected = bool(
        enrolled
        and lease_active
        and manifest_match
        and not quarantined
        and profile_match
    )
    clean_receipt = bool(
        evidence.get("clean_receipt_within_sla") is True
        and int(evidence.get("receipt_errors") or 0) == 0
        and int(evidence.get("receipt_quarantined") or 0) == 0
    )
    idle_current = bool(connected and not evidence.get("last_business_event_at"))
    unhealthy_reasons = []
    if not enrolled:
        unhealthy_reasons.append("NOT_ENROLLED")
    if not lease_active:
        unhealthy_reasons.append("RUNTIME_LEASE_INACTIVE")
    if not manifest_match:
        unhealthy_reasons.append("MANIFEST_HASH_MISMATCH")
    if quarantined:
        unhealthy_reasons.append("SOURCE_QUARANTINED")
    if not profile_match:
        unhealthy_reasons.append("PROFILE_OR_CREDENTIAL_MISMATCH")
    if transport_failed:
        unhealthy_reasons.append("TRANSPORT_FAILURE")
    return {
        "enrolled": enrolled,
        "lease_active": lease_active,
        "connected": connected,
        "idle_current": idle_current,
        "active_producer": bool(connected and clean_receipt),
        "unhealthy": bool(unhealthy_reasons),
        "unhealthy_reason_codes": unhealthy_reasons,
        "last_business_event": evidence.get("last_business_event_at"),
    }


def connected_pc_count(sources: Iterable[Mapping[str, Any]]) -> int:
    return len(
        {
            str(source.get("pc_id") or "")
            for source in sources
            if source.get("connected") is True and str(source.get("pc_id") or "")
        }
    )


def business_kpi_denominator(events: Iterable[Mapping[str, Any]]) -> int:
    """Count distinct business entities; physical PC liveness is intentionally absent."""
    return len(
        {
            (str(event.get("entity_type") or ""), str(event.get("entity_id") or ""))
            for event in events
            if event.get("entity_type") and event.get("entity_id")
        }
    )
