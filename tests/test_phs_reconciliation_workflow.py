import copy
import hashlib
import json
from pathlib import Path

import pytest

from phs_label_workflow import (
    PHSLabelExchangeCoordinator,
    PHSLabelExchangeJournal,
    PHSLabelWorkflowError,
    PhysicalPrintEvidence,
    RenderedPHSLabel,
)


SCOPE = "TEST1-GOAL-20260722-EXACT-SIX"
ITEM = "AAA2270730200"


def _membership_hash(values):
    members = tuple(sorted(values))
    payload = json.dumps(
        members,
        ensure_ascii=False,
        separators=(",", ":"),
    )
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def _qr(anchor, label_id, prefix):
    return (
        "PHS=2|SRC=KMTECH_INPUT_TAG|"
        f"ITG={anchor}|CLC={ITEM}|LBL={label_id}|HSH={prefix}"
    )


def _source(index, members):
    prefix = f"{index:x}" * 16
    label_id = f"LBL-SOURCE-{index}"
    anchor = f"ITAG-PACK-{index}"
    return {
        "source_label_id": label_id,
        "group_id": f"GROUP-{index}",
        "instruction_id": f"INSTRUCTION-SOURCE-{index}",
        "business_date": "2026-07-27",
        "item_id": ITEM,
        "display_item_code": "30200",
        "item_daily_ordinal": index,
        "worker_code": f"7월27일-{index}",
        "qty_pcs": len(members),
        "label_version": 1,
        "membership_version": 1,
        "membership_hash": _membership_hash(members),
        "member_ids": list(sorted(members)),
        "qr_payload": _qr(anchor, label_id, prefix),
    }


def _target(index, quantity):
    return {
        "instruction_id": f"INSTRUCTION-TARGET-{index}",
        "business_date": "2026-07-28",
        "item_id": ITEM,
        "display_item_code": "30200",
        "item_daily_ordinal": index,
        "worker_code": f"7월28일-{index}",
        "qty_pcs": quantity,
    }


def _action(action_id, action_type, sources, targets, split=None):
    union = sorted(
        member
        for source in sources
        for member in source["member_ids"]
    )
    process = [
        {
            "unit_id": member,
            "owner_type": "PACKAGE",
            "owner_id": f"TRANSFER-{member}",
            "bundle_type": "TRANSFER",
            "bundle_state": "AVAILABLE",
            "location_code": "TRANSFER",
            "unit_state": "AVAILABLE",
        }
        for member in union
    ]
    return {
        "action_id": action_id,
        "action_index": int(action_id.rsplit("-", 1)[-1]),
        "action_type": action_type,
        "action_state": "PROPOSED",
        "exchange_id": None,
        "item_id": ITEM,
        "before_qty_pcs": len(union),
        "after_qty_pcs": sum(target["qty_pcs"] for target in targets),
        "sources": copy.deepcopy(sources),
        "targets": copy.deepcopy(targets),
        "source_member_union_count": len(union),
        "source_member_union_hash": _membership_hash(union),
        "source_member_ids": union,
        "split_member_ids_by_target": copy.deepcopy(split or {}),
        "process_membership": process,
        "display": {
            "item_id": ITEM,
            "sources": [],
            "targets": [],
        },
    }


def _resolution(kind):
    source_one = _source(1, ["UNIT-001", "UNIT-002"])
    if kind == "BATCH":
        source_two = _source(2, ["UNIT-003", "UNIT-004"])
        actions = [
            _action(
                "ACTION-1",
                "EXCHANGE_DATE",
                [source_one],
                [_target(1, 2)],
            ),
            _action(
                "ACTION-2",
                "EXCHANGE_DATE",
                [source_two],
                [_target(2, 2)],
            ),
        ]
        mode = "MULTI_EXCHANGE_DATE"
    elif kind == "SPLIT":
        source_one = _source(
            1,
            ["UNIT-001", "UNIT-002", "UNIT-003", "UNIT-004"],
        )
        targets = [_target(1, 2), _target(2, 2)]
        actions = [
            _action(
                "ACTION-1",
                "SPLIT",
                [source_one],
                targets,
                {
                    targets[0]["instruction_id"]: [
                        "UNIT-001",
                        "UNIT-002",
                    ],
                    targets[1]["instruction_id"]: [
                        "UNIT-003",
                        "UNIT-004",
                    ],
                },
            )
        ]
        mode = "SINGLE_TOPOLOGY"
    elif kind == "MERGE":
        source_two = _source(2, ["UNIT-003", "UNIT-004"])
        actions = [
            _action(
                "ACTION-1",
                "MERGE",
                [source_one, source_two],
                [_target(1, 4)],
            )
        ]
        mode = "SINGLE_TOPOLOGY"
    else:
        actions = [
            _action(
                "ACTION-1",
                "EXCHANGE_DATE",
                [source_one],
                [_target(1, 2)],
            )
        ]
        mode = "SINGLE_EXCHANGE_DATE"
    scan_source = actions[0]["sources"][0]
    return {
        "contract_version": "phs-work-control-v1",
        "authority_scope_id": SCOPE,
        "process_context": "packaging",
        "scan": {
            "resolution": "OVERLAY_ACTIVE",
            "scanned_label_id": scan_source["source_label_id"],
            "active_label_id": scan_source["source_label_id"],
            "replacement_required": False,
            "active_qr_payload": scan_source["qr_payload"],
        },
        "reconciliation": {
            "reconciliation_id": "RECONCILIATION-1",
            "reconciliation_no": 1,
            "business_date": "2026-07-27",
            "state": "PROPOSED",
            "entity_version": 4,
            "proposed_at": "2026-07-28T00:00:00Z",
        },
        "actions": actions,
        "selection": {
            "mode": mode,
            "reconciliation_id": "RECONCILIATION-1",
            "action_ids": [
                action["action_id"] for action in actions
            ],
            "expected_reconciliation_version": 4,
        },
        "scan_payload": scan_source["qr_payload"],
    }


def _edges(resolution):
    edges = []
    for action in resolution["actions"]:
        if action["action_type"] == "EXCHANGE_DATE":
            edges.append(
                (
                    "PAIR",
                    action["sources"][0],
                    action["targets"][0],
                    action["sources"][0]["member_ids"],
                )
            )
        elif action["action_type"] == "SPLIT":
            for target in action["targets"]:
                edges.append(
                    (
                        "SPLIT_SUCCESSOR",
                        action["sources"][0],
                        target,
                        action["split_member_ids_by_target"][
                            target["instruction_id"]
                        ],
                    )
                )
        else:
            for source in action["sources"]:
                edges.append(
                    (
                        "MERGE_SOURCE",
                        source,
                        action["targets"][0],
                        source["member_ids"],
                    )
                )
    return edges


class _Renderer:
    def __init__(self, root):
        self.root = Path(root)
        self.calls = []

    def render(self, current_set, target):
        self.root.mkdir(parents=True, exist_ok=True)
        path = self.root / f"{target['label_id']}.png"
        path.write_bytes(b"png")
        self.calls.append(
            (target["label_id"], target["member_count"])
        )
        return RenderedPHSLabel(str(path), "d" * 64)


class _Printer:
    def __init__(self, fail_once_label_id=""):
        self.fail_once_label_id = fail_once_label_id
        self.failed = False
        self.calls = []

    def print_png(self, filepath, *, document_name):
        label_id = Path(filepath).stem
        self.calls.append(label_id)
        if (
            label_id == self.fail_once_label_id
            and not self.failed
        ):
            self.failed = True
            raise RuntimeError("printer offline")
        return PhysicalPrintEvidence(
            printer_name="TEST-PRINTER",
            spool_job_id=len(self.calls),
            document_name=document_name,
            submitted_at="2026-07-28T00:00:00Z",
        )


class _CrashDuringSpoolPrinter:
    def print_png(self, filepath, *, document_name):
        raise KeyboardInterrupt("simulated process termination")


class _Client:
    def __init__(
        self,
        resolution,
        *,
        lose_prepare_ack=False,
        lose_complete_ack_label_id="",
        lose_activate_ack=False,
    ):
        self.config = type(
            "Config",
            (),
            {"authority_scope_id": SCOPE},
        )()
        self.resolution = copy.deepcopy(resolution)
        self.kind = (
            "BATCH"
            if resolution["selection"]["mode"]
            == "MULTI_EXCHANGE_DATE"
            else (
                resolution["actions"][0]["action_type"]
                if resolution["actions"][0]["action_type"]
                in {"SPLIT", "MERGE"}
                else "SINGLE"
            )
        )
        self.calls = []
        self.prepare_keys = []
        self.prepare_write_count = 0
        self.lose_prepare_ack = lose_prepare_ack
        self.lost = False
        self.lose_complete_ack_label_id = (
            lose_complete_ack_label_id
        )
        self.complete_ack_lost = False
        self.lose_activate_ack = lose_activate_ack
        self.activate_ack_lost = False
        self.exchange_state = "PREPARED"
        self.exchange_version = 1
        self.attempts = {}
        self.attempt_keys = {}
        self.succeeded = set()
        self.failed = set()
        self.target_by_instruction = {}
        self.target_members = {}
        for edge_role, source, target, members in _edges(
            self.resolution
        ):
            instruction_id = target["instruction_id"]
            self.target_members.setdefault(instruction_id, set()).update(
                members
            )
            self.target_by_instruction.setdefault(
                instruction_id,
                {
                    **target,
                    "label_id": f"LBL-TARGET-{len(self.target_by_instruction) + 1}",
                    "source_anchor": source["qr_payload"].split("|")[2].split("=", 1)[1],
                },
            )

    def resolve_phs_reconciliation_actions(self, **kwargs):
        self.calls.append(("resolve", kwargs))
        return copy.deepcopy(self.resolution)

    def prepare_phs_reconciliation_label_exchange(
        self,
        reconciliation_id,
        **kwargs,
    ):
        self.calls.append(("prepare", kwargs))
        key = kwargs["idempotency_key"]
        self.prepare_keys.append(key)
        if self.prepare_write_count == 0:
            self.prepare_write_count = 1
        response = self._projection()
        response["approved_action_ids"] = list(
            self.resolution["selection"]["action_ids"]
        )
        response["reconciliation"] = {
            **self.resolution["reconciliation"],
            "state": "APPROVED",
            "entity_version": 5,
        }
        if self.lose_prepare_ack and not self.lost:
            self.lost = True
            raise RuntimeError("lost prepare ACK")
        return response

    def get_phs_label_exchange(self, exchange_id, **kwargs):
        self.calls.append(("get", exchange_id))
        return self._projection()

    def request_phs_label_print(self, exchange_id, **kwargs):
        label_id = kwargs["label_id"]
        key = kwargs["idempotency_key"]
        self.calls.append(("request_print", label_id, key))
        if key in self.attempt_keys:
            attempt_id = self.attempt_keys[key]
        else:
            attempt_id = f"PRINT-{label_id}-{len(self.attempts) + 1}"
            self.attempt_keys[key] = attempt_id
            self.attempts[attempt_id] = {
                "print_attempt_id": attempt_id,
                "label_id": label_id,
                "state": "REQUESTED",
            }
        return {
            "print_attempt": copy.deepcopy(
                self.attempts[attempt_id]
            ),
            "exchange": copy.deepcopy(
                self._projection()["exchange"]
            ),
        }

    def complete_phs_label_print(
        self,
        print_attempt_id,
        *,
        succeeded,
        **kwargs,
    ):
        attempt = self.attempts[print_attempt_id]
        label_id = attempt["label_id"]
        self.calls.append(("complete_print", label_id, succeeded))
        attempt["state"] = "SUCCEEDED" if succeeded else "FAILED"
        if succeeded:
            self.succeeded.add(label_id)
            self.failed.discard(label_id)
        else:
            self.failed.add(label_id)
        all_target_ids = {
            value["label_id"]
            for value in self.target_by_instruction.values()
        }
        if self.succeeded == all_target_ids:
            self.exchange_state = "READY"
        elif self.succeeded:
            self.exchange_state = "PRINT_PARTIAL"
        elif self.failed:
            self.exchange_state = "PRINT_FAILED"
        self.exchange_version += 1
        if (
            succeeded
            and label_id == self.lose_complete_ack_label_id
            and not self.complete_ack_lost
        ):
            self.complete_ack_lost = True
            raise RuntimeError("lost complete ACK")
        return {
            "print_attempt": copy.deepcopy(attempt),
            "exchange": copy.deepcopy(
                self._projection()["exchange"]
            ),
        }

    def activate_phs_label_exchange(self, exchange_id, **kwargs):
        self.calls.append(("activate", kwargs))
        assert self.exchange_state == "READY"
        self.exchange_state = "COMMITTED"
        self.exchange_version += 1
        if self.lose_activate_ack and not self.activate_ack_lost:
            self.activate_ack_lost = True
            raise RuntimeError("lost activate ACK")
        return self._projection()

    def _projection(self):
        committed = self.exchange_state == "COMMITTED"
        source_labels = []
        for action in self.resolution["actions"]:
            for source in action["sources"]:
                if any(
                    value["label_id"] == source["source_label_id"]
                    for value in source_labels
                ):
                    continue
                prefix = source["qr_payload"].rsplit("=", 1)[-1]
                source_labels.append(
                    {
                        "label_id": source["source_label_id"],
                        "group_id": source["group_id"],
                        "qr_payload": source["qr_payload"],
                        "label_instance_hash": prefix + ("0" * 48),
                        "hash_prefix": prefix,
                        "scan_anchor_input_tag_id": source["qr_payload"]
                        .split("|")[2]
                        .split("=", 1)[1],
                        "item_id": source["item_id"],
                        "business_date": source["business_date"],
                        "instruction_id": source["instruction_id"],
                        "display_item_code": source["display_item_code"],
                        "item_daily_ordinal": source[
                            "item_daily_ordinal"
                        ],
                        "worker_code": source["worker_code"],
                        "state": "SUPERSEDED" if committed else "ACTIVE",
                        "label_version": 2 if committed else 1,
                        "membership_version": (
                            2
                            if committed
                            and self.kind in {"SPLIT", "MERGE"}
                            else 1
                        ),
                        "member_count": len(source["member_ids"]),
                        "membership_hash": source["membership_hash"],
                    }
                )
        target_labels = []
        for instruction_id, value in self.target_by_instruction.items():
            members = sorted(self.target_members[instruction_id])
            prefix = f"{len(target_labels) + 9:x}"[-1] * 16
            label_id = value["label_id"]
            state = (
                "ACTIVE"
                if committed
                else (
                    "PRINT_FAILED"
                    if label_id in self.failed
                    else "PENDING_ACTIVATION"
                )
            )
            target_labels.append(
                {
                    "label_id": label_id,
                    "group_id": f"TARGET-GROUP-{len(target_labels) + 1}",
                    "qr_payload": _qr(
                        value["source_anchor"],
                        label_id,
                        prefix,
                    ),
                    "label_instance_hash": prefix + ("0" * 48),
                    "hash_prefix": prefix,
                    "scan_anchor_input_tag_id": value["source_anchor"],
                    "item_id": value["item_id"],
                    "business_date": value["business_date"],
                    "instruction_id": instruction_id,
                    "display_item_code": value["display_item_code"],
                    "item_daily_ordinal": value["item_daily_ordinal"],
                    "worker_code": value["worker_code"],
                    "state": state,
                    "label_version": 2 if committed else 1,
                    "membership_version": 1,
                    "member_count": len(members),
                    "membership_hash": _membership_hash(members),
                }
            )
        targets_by_instruction = {
            value["instruction_id"]: value for value in target_labels
        }
        items = []
        for index, (edge_role, source, target, members) in enumerate(
            _edges(self.resolution),
            start=1,
        ):
            target_label = targets_by_instruction[
                target["instruction_id"]
            ]
            items.append(
                {
                    "item_index": index,
                    "edge_role": edge_role,
                    "source_group_id": source["group_id"],
                    "source_label_id": source["source_label_id"],
                    "target_group_id": target_label["group_id"],
                    "target_label_id": target_label["label_id"],
                    "expected_label_version": 1,
                    "expected_membership_version": 1,
                    "before_business_date": source["business_date"],
                    "after_business_date": target["business_date"],
                    "before_instruction_id": source["instruction_id"],
                    "after_instruction_id": target["instruction_id"],
                    "member_count": len(members),
                    "membership_hash": _membership_hash(members),
                    "state": (
                        "COMMITTED"
                        if committed
                        else (
                            "READY"
                            if target_label["label_id"] in self.succeeded
                            else "PREPARED"
                        )
                    ),
                }
            )
        return {
            "status": self.exchange_state,
            "exchange": {
                "exchange_id": "EXCHANGE-1",
                "exchange_kind": self.kind,
                "state": self.exchange_state,
                "entity_version": self.exchange_version,
            },
            "items": items,
            "source_labels": source_labels,
            "target_labels": target_labels,
        }


def _current_set(resolution):
    source = resolution["actions"][0]["sources"][0]
    return {
        "id": "SET-CURRENT",
        "raw": [source["qr_payload"]],
        "parsed": [ITEM],
        "central_inherit_all": True,
        "canonical_input_tag_qr": source["qr_payload"],
        "active_label_qr_payload": source["qr_payload"],
        "active_label_id": source["source_label_id"],
        "active_label_business_date": source["business_date"],
        "active_label_worker_code": source["worker_code"],
        "active_label_instruction_id": source["instruction_id"],
        "active_label_version": 1,
        "active_membership_version": 1,
        "active_label_resolution": "OVERLAY_ACTIVE",
        "package_source_snapshot": {
            "authority_scope_id": SCOPE,
            "member_count": len(source["member_ids"]),
            "membership_hash": source["membership_hash"],
        },
        "package_submission_status": "",
        "progress_marker": "KEEP",
    }


def _coordinator(tmp_path, client, printer=None):
    return PHSLabelExchangeCoordinator(
        client,
        PHSLabelExchangeJournal(tmp_path / "journal.json"),
        _Renderer(tmp_path / "rendered"),
        printer or _Printer(),
    )


@pytest.mark.parametrize("kind", ["SINGLE", "BATCH", "SPLIT", "MERGE"])
def test_reconciliation_topologies_print_all_then_activate_once(
    tmp_path,
    kind,
):
    resolution = _resolution(kind)
    client = _Client(resolution)
    current = _current_set(resolution)
    before = copy.deepcopy(current)
    coordinator = _coordinator(tmp_path, client)

    result = coordinator.execute_reconciliation(
        resolution,
        current_set=current,
        persist_current_set=lambda: True,
    )

    assert result.success is True
    assert result.status == "COMMITTED"
    assert [call[0] for call in client.calls].count("prepare") == 1
    assert [call[0] for call in client.calls].count("activate") == 1
    assert len(
        [call for call in client.calls if call[0] == "request_print"]
    ) == len(client.target_by_instruction)
    assert current["raw"] == before["raw"]
    assert current["parsed"] == before["parsed"]
    assert (
        current["package_source_snapshot"]
        == before["package_source_snapshot"]
    )
    assert current["progress_marker"] == "KEEP"
    if kind in {"SINGLE", "BATCH"}:
        assert current["active_label_id"].startswith("LBL-TARGET-")
        assert not current.get("phs_label_topology_refresh_required")
    else:
        assert current["active_label_qr_payload"] == ""
        assert current["active_label_resolution"] == (
            "TOPOLOGY_REFRESH_REQUIRED"
        )
        assert current["phs_label_topology_refresh_required"] is True
        assert current["phs_label_topology_successors"]


def test_completed_packaging_label_runs_without_mutating_current_set(tmp_path):
    resolution = _resolution("MERGE")
    client = _Client(resolution)
    unrelated = {"id": "UNRELATED", "raw": [], "progress": 7}
    before = copy.deepcopy(unrelated)

    result = _coordinator(tmp_path, client).execute_reconciliation(
        resolution,
        current_set=None,
        persist_current_set=None,
    )

    assert result.success is True
    assert unrelated == before
    assert [call[0] for call in client.calls].count("activate") == 1


@pytest.mark.parametrize(
    "mutator,code",
    [
        (
            lambda value: value.update(
                {"authority_scope_id": "WRONG-SCOPE"}
            ),
            "PHS_RECONCILIATION_RESPONSE_INVALID",
        ),
        (
            lambda value: value["actions"][0]["process_membership"][0].update(
                {"location_code": "PHS_GOOD"}
            ),
            "PHS_RECONCILIATION_PROCESS_INVALID",
        ),
        (
            lambda value: value["actions"][0].update(
                {"source_member_union_hash": "f" * 64}
            ),
            "PHS_RECONCILIATION_MEMBERSHIP_INVALID",
        ),
        (
            lambda value: value["actions"][0]["targets"].append(
                _target(8, 2)
            ),
            "PHS_RECONCILIATION_TOPOLOGY_INVALID",
        ),
    ],
)
def test_wrong_scope_process_hash_or_topology_fails_before_prepare(
    tmp_path,
    mutator,
    code,
):
    resolution = _resolution("SINGLE")
    mutator(resolution)
    client = _Client(_resolution("SINGLE"))
    coordinator = _coordinator(tmp_path, client)

    with pytest.raises(PHSLabelWorkflowError) as error:
        coordinator.execute_reconciliation(resolution)

    assert error.value.code == code
    assert not any(call[0] == "prepare" for call in client.calls)


def test_partial_print_retry_only_reprints_failed_target(tmp_path):
    resolution = _resolution("BATCH")
    client = _Client(resolution)
    failed_id = "LBL-TARGET-2"
    printer = _Printer(fail_once_label_id=failed_id)
    coordinator = _coordinator(tmp_path, client, printer)

    first = coordinator.execute_reconciliation(resolution)

    assert first.success is False
    assert first.journal_state["failed_target_label_id"] == failed_id
    assert printer.calls == ["LBL-TARGET-1", failed_id]
    blocked = coordinator.recover_reconciliation()
    assert blocked.success is False
    assert printer.calls == ["LBL-TARGET-1", failed_id]

    result = coordinator.recover_reconciliation(
        retry_failed_target_ids={failed_id},
    )

    assert result.success is True
    assert printer.calls.count("LBL-TARGET-1") == 1
    assert printer.calls.count(failed_id) == 2
    assert [call[0] for call in client.calls].count("activate") == 1


def test_lost_prepare_ack_restarts_with_same_key_and_one_server_write(tmp_path):
    resolution = _resolution("SINGLE")
    client = _Client(resolution, lose_prepare_ack=True)
    journal = PHSLabelExchangeJournal(tmp_path / "journal.json")
    first = PHSLabelExchangeCoordinator(
        client,
        journal,
        _Renderer(tmp_path / "rendered"),
        _Printer(),
    )

    with pytest.raises(RuntimeError, match="lost prepare ACK"):
        first.execute_reconciliation(resolution)

    recovered = PHSLabelExchangeCoordinator(
        client,
        journal,
        _Renderer(tmp_path / "rendered-2"),
        _Printer(),
    ).recover_reconciliation()

    assert recovered.success is True
    assert len(client.prepare_keys) == 2
    assert len(set(client.prepare_keys)) == 1
    assert client.prepare_write_count == 1
    assert [call[0] for call in client.calls].count("activate") == 1


def test_lost_activate_ack_recovers_committed_exchange_by_get(tmp_path):
    resolution = _resolution("SINGLE")
    client = _Client(resolution, lose_activate_ack=True)

    result = _coordinator(tmp_path, client).execute_reconciliation(
        resolution
    )

    assert result.success is True
    assert [call[0] for call in client.calls].count("activate") == 1
    assert [call[0] for call in client.calls].count("get") == 2
    assert [call[0] for call in client.calls][-2:] == [
        "activate",
        "get",
    ]


def test_committed_local_failure_recovers_by_get_without_second_prepare(
    tmp_path,
):
    resolution = _resolution("SINGLE")
    client = _Client(resolution)
    current = _current_set(resolution)
    journal = PHSLabelExchangeJournal(tmp_path / "journal.json")
    coordinator = PHSLabelExchangeCoordinator(
        client,
        journal,
        _Renderer(tmp_path / "rendered"),
        _Printer(),
    )

    with pytest.raises(PHSLabelWorkflowError) as first_error:
        coordinator.execute_reconciliation(
            resolution,
            current_set=current,
            persist_current_set=lambda: False,
        )

    assert first_error.value.code == "PHS_LOCAL_STATE_WRITE_FAILED"
    assert journal.load()["status"] == "COMMITTED_LOCAL_REFRESH_PENDING"
    prepare_count = [call[0] for call in client.calls].count("prepare")
    activate_count = [call[0] for call in client.calls].count("activate")

    recovered_current = _current_set(resolution)
    result = coordinator.recover_reconciliation(
        current_set=recovered_current,
        persist_current_set=lambda: True,
    )

    assert result.success is True
    assert [call[0] for call in client.calls].count("prepare") == prepare_count
    assert [call[0] for call in client.calls].count("activate") == activate_count
    assert recovered_current["active_label_id"].startswith("LBL-TARGET-")


def test_prepare_projection_membership_corruption_blocks_all_prints(
    tmp_path,
):
    resolution = _resolution("SPLIT")
    client = _Client(resolution)
    original_projection = client._projection

    def corrupt_projection():
        value = original_projection()
        value["target_labels"][0]["membership_hash"] = "f" * 64
        return value

    client._projection = corrupt_projection

    with pytest.raises(PHSLabelWorkflowError) as error:
        _coordinator(tmp_path, client).execute_reconciliation(
            resolution
        )

    assert error.value.code == "PHS_RECONCILIATION_TARGET_INVALID"
    assert not any(
        call[0] in {"request_print", "complete_print", "activate"}
        for call in client.calls
    )


def test_lost_print_complete_ack_reuses_attempt_without_reprinting(
    tmp_path,
):
    resolution = _resolution("SINGLE")
    target_id = "LBL-TARGET-1"
    client = _Client(
        resolution,
        lose_complete_ack_label_id=target_id,
    )
    journal = PHSLabelExchangeJournal(tmp_path / "journal.json")
    printer = _Printer()
    coordinator = PHSLabelExchangeCoordinator(
        client,
        journal,
        _Renderer(tmp_path / "rendered"),
        printer,
    )

    with pytest.raises(RuntimeError, match="lost complete ACK"):
        coordinator.execute_reconciliation(resolution)

    assert (
        journal.load()["target_prints"][target_id]["status"]
        == "PRINT_COMPLETE_PENDING"
    )
    recovered = PHSLabelExchangeCoordinator(
        client,
        journal,
        _Renderer(tmp_path / "rendered-2"),
        printer,
    ).recover_reconciliation()

    assert recovered.success is True
    assert printer.calls == [target_id]
    assert len(
        [call for call in client.calls if call[0] == "request_print"]
    ) == 1
    assert len(
        [call for call in client.calls if call[0] == "complete_print"]
    ) == 1


def test_restart_requires_explicit_confirmation_after_ambiguous_spool(
    tmp_path,
):
    resolution = _resolution("SINGLE")
    client = _Client(resolution)
    journal = PHSLabelExchangeJournal(tmp_path / "journal.json")
    first = PHSLabelExchangeCoordinator(
        client,
        journal,
        _Renderer(tmp_path / "rendered"),
        _CrashDuringSpoolPrinter(),
    )

    with pytest.raises(KeyboardInterrupt):
        first.execute_reconciliation(resolution)

    target_id = "LBL-TARGET-1"
    assert (
        journal.load()["target_prints"][target_id]["status"]
        == "LOCAL_PRINT_STARTING"
    )
    restarted = PHSLabelExchangeCoordinator(
        client,
        journal,
        _Renderer(tmp_path / "rendered-2"),
        _Printer(),
    )
    with pytest.raises(PHSLabelWorkflowError) as confirmation:
        restarted.recover_reconciliation()
    assert (
        confirmation.value.code
        == "PHS_PRINT_REPRINT_CONFIRMATION_REQUIRED"
    )

    recovered = restarted.recover_reconciliation(
        confirm_ambiguous_reprint_target_ids={target_id},
    )

    assert recovered.success is True
    assert [call[0] for call in client.calls].count("prepare") == 1
    assert [call[0] for call in client.calls].count("activate") == 1
