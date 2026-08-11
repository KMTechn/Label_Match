"""Adapter registry for the canonical active-work probe."""

from __future__ import annotations

from collections.abc import Callable

from ..container import (
    BLOCKER_KIND_CATALOG as CONTAINER_BLOCKER_KIND_CATALOG,
    create_adapter as create_container_adapter,
)
from ..core import ProbeAdapter, ProbeError
from ..label import (
    BLOCKER_KIND_CATALOG as LABEL_BLOCKER_KIND_CATALOG,
    create_adapter as create_label_adapter,
)
from .common import RELAY_DYNAMIC_BLOCKER_KINDS, TrustedRoots, relay_plan
from .defect import RETURN_PLAN, WAREHOUSE_PLAN, create_adapter as create_defect_adapter
from .inspection_rework import LEDGER_PLAN, create_adapter as create_inspection_rework_adapter


SUPPORTED_APPS = (
    "Inspection_worker",
    "Rework_worker",
    "Defect_Inspection",
    "Container_Audit",
    "Label_Match",
)

BLOCKER_KIND_CATALOG = tuple(
    sorted(
        {
            *(query.kind for query in LEDGER_PLAN.queries),
            *(query.kind for query in RETURN_PLAN.queries),
            *(query.kind for query in WAREHOUSE_PLAN.queries),
            *(query.kind for query in relay_plan(defect=False).queries),
            *(query.kind for query in relay_plan(defect=True).queries),
            *RELAY_DYNAMIC_BLOCKER_KINDS,
            *CONTAINER_BLOCKER_KIND_CATALOG,
            *LABEL_BLOCKER_KIND_CATALOG,
            "session_recovery_active",
            "rework_bundle_claimed",
            "rework_completion_pending",
            "rework_result_intent_pending",
            "rework_candidate_claim",
            "rework_candidate_result_pending",
            "rework_candidate_claim_attempt",
            "defect_local_buffer_pending",
        }
    )
)
if len(BLOCKER_KIND_CATALOG) > 64:  # pragma: no cover - import-time contract guard
    raise RuntimeError("active-work blocker-kind catalog exceeds broker limit")

ADAPTER_FACTORIES: dict[
    str,
    Callable[..., ProbeAdapter],
] = {
    "Inspection_worker": create_inspection_rework_adapter,
    "Rework_worker": create_inspection_rework_adapter,
    "Defect_Inspection": create_defect_adapter,
    "Container_Audit": create_container_adapter,
    "Label_Match": create_label_adapter,
}


def create_adapter(
    app: str,
    target_pc: str,
    *,
    roots: TrustedRoots | None = None,
) -> ProbeAdapter:
    factory = ADAPTER_FACTORIES.get(app)
    if factory is None:
        raise ProbeError("APP_UNSUPPORTED", "no active-work adapter is registered")
    return factory(app, target_pc, roots=roots)


__all__ = [
    "ADAPTER_FACTORIES",
    "BLOCKER_KIND_CATALOG",
    "SUPPORTED_APPS",
    "create_adapter",
]
