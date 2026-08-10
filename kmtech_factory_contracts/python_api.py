"""Exact Inspection provider/Rework consumer contract."""

from __future__ import annotations

import inspect
from dataclasses import dataclass, fields, is_dataclass
from typing import Any, Callable, Mapping, Protocol, get_args, get_origin, get_type_hints

from .errors import FactoryContractError


BOOTSTRAP_CAPABILITY_VERSION = "central-ng-resolver-bootstrap-v1"
BOOTSTRAP_REQUIRED_RESULT_FIELDS = (
    "success",
    "ng_bundle_id",
    "manifest_id",
    "manifest_hash",
    "actual_quantity",
    "child_barcodes",
    "event_id",
    "outbox_id",
    "error_code",
    "error_message",
)


@dataclass(frozen=True)
class BootstrapCentralNgHoldResult:
    success: bool
    ng_bundle_id: str = ""
    manifest_id: str = ""
    manifest_hash: str = ""
    actual_quantity: int = 0
    child_barcodes: tuple[str, ...] = ()
    event_id: str = ""
    outbox_id: str = ""
    error_code: str = ""
    error_message: str = ""


class BootstrapCentralNgHoldProvider(Protocol):
    def bootstrap_central_ng_hold_from_resolver(
        self,
        *,
        ng_bundle_id: str,
        central_source_bundle_id: str,
        authority_scope_id: str,
        authority_epoch: int,
        ledger_plane: str,
        plane_epoch: int,
        inbound_iin: str,
        item_code: str,
        uom: str,
        member_ids: list[str],
        membership_hash: str,
        members: list[dict[str, Any]],
        central_bundle_entity_version: int,
        source_session_id: str,
        actor: str,
        actor_role: str,
    ) -> BootstrapCentralNgHoldResult: ...


def _annotation_name(annotation: Any) -> str:
    if annotation is inspect.Signature.empty:
        return ""
    origin = get_origin(annotation)
    if origin is not None:
        origin_name = getattr(origin, "__name__", str(origin).replace("typing.", ""))
        return f"{origin_name}[{','.join(_annotation_name(item) for item in get_args(annotation))}]"
    if annotation is Any:
        return "Any"
    return getattr(annotation, "__name__", str(annotation).replace("typing.", ""))


def normalized_signature(callable_object: Callable[..., Any]) -> tuple[dict[str, Any], ...]:
    rows = []
    for parameter in inspect.signature(callable_object).parameters.values():
        if parameter.name in {"self", "cls"}:
            continue
        rows.append(
            {
                "name": parameter.name,
                "kind": parameter.kind.name,
                "required": parameter.default is inspect.Parameter.empty,
                "default": None if parameter.default is inspect.Parameter.empty else parameter.default,
                "annotation": _annotation_name(parameter.annotation),
            }
        )
    return tuple(rows)


def expected_bootstrap_signature() -> tuple[dict[str, Any], ...]:
    return normalized_signature(
        BootstrapCentralNgHoldProvider.bootstrap_central_ng_hold_from_resolver
    )


def validate_bootstrap_provider(
    provider: Callable[..., Any],
    *,
    result_type: type[Any] | None = None,
    capability_version: str = BOOTSTRAP_CAPABILITY_VERSION,
) -> dict[str, Any]:
    if capability_version != BOOTSTRAP_CAPABILITY_VERSION:
        raise FactoryContractError(
            "PROVIDER_CAPABILITY_VERSION_MISMATCH",
            "Inspection bootstrap capability version does not match",
        )
    actual = normalized_signature(provider)
    expected = expected_bootstrap_signature()
    if actual != expected:
        raise FactoryContractError(
            "PY_API_SIGNATURE_MISMATCH",
            "Inspection bootstrap provider signature differs from the exact contract",
            details={"expected": expected, "actual": actual},
        )
    return_annotation = inspect.signature(provider).return_annotation
    if return_annotation is inspect.Signature.empty:
        raise FactoryContractError(
            "PY_API_SIGNATURE_MISMATCH",
            "Inspection bootstrap provider must declare a return type",
        )
    resolved_result_type = result_type
    if resolved_result_type is None:
        try:
            candidate = get_type_hints(provider).get("return")
        except (NameError, TypeError) as exc:
            raise FactoryContractError(
                "PY_API_SIGNATURE_MISMATCH",
                "Inspection bootstrap return type cannot be resolved",
            ) from exc
        if isinstance(candidate, type):
            resolved_result_type = candidate
    if resolved_result_type is None:
        raise FactoryContractError(
            "PY_API_SIGNATURE_MISMATCH",
            "Inspection bootstrap return contract must be a concrete type",
        )
    if resolved_result_type is not None:
        available = (
            {field.name for field in fields(resolved_result_type)}
            if is_dataclass(resolved_result_type)
            else set(getattr(resolved_result_type, "__annotations__", {}))
        )
        missing = sorted(set(BOOTSTRAP_REQUIRED_RESULT_FIELDS) - available)
        if missing:
            raise FactoryContractError(
                "PY_API_SIGNATURE_MISMATCH",
                "Inspection bootstrap result contract is incomplete",
                details={"missing_result_fields": missing},
            )
    return {
        "capability_version": BOOTSTRAP_CAPABILITY_VERSION,
        "signature": actual,
        "return_annotation": _annotation_name(return_annotation),
        "required_result_fields": BOOTSTRAP_REQUIRED_RESULT_FIELDS,
    }


def bind_bootstrap_arguments(kwargs: Mapping[str, Any]) -> inspect.BoundArguments:
    signature = inspect.signature(
        BootstrapCentralNgHoldProvider.bootstrap_central_ng_hold_from_resolver
    )
    parameters = [
        parameter
        for parameter in signature.parameters.values()
        if parameter.name not in {"self", "cls"}
    ]
    callable_signature = signature.replace(parameters=parameters)
    try:
        return callable_signature.bind(**dict(kwargs))
    except TypeError as exc:
        raise FactoryContractError(
            "PY_API_ARGUMENT_BIND_FAILED",
            "Rework bootstrap arguments do not bind to the exact provider contract",
            details={"argument_names": sorted(str(key) for key in kwargs)},
        ) from exc
