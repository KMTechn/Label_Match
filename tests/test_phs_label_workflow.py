import copy
from pathlib import Path

import pytest

from phs_label_workflow import (
    PHSLabelExchangeCoordinator,
    PHSLabelExchangeJournal,
    PHSLabelWorkflowError,
    PhysicalPrintEvidence,
    RenderedPHSLabel,
    normalize_packaging_phs_label_evidence,
)


SCOPE = "TEST1-GOAL-20260722-EXACT-SIX"
INPUT_TAG_ID = "ITAG-TEST-LABEL-MATCH"
ITEM_ID = "AAA2270730200"
MEMBERSHIP_HASH = "c" * 64
CANONICAL_QR = (
    "PHS=2|SRC=KMTECH_INPUT_TAG|"
    f"ITG={INPUT_TAG_ID}|CLC={ITEM_ID}|LBL=LBL-OLD|"
    "HSH=aaaaaaaaaaaaaaaa"
)
TARGET_QR = (
    "PHS=2|SRC=KMTECH_INPUT_TAG|"
    f"ITG={INPUT_TAG_ID}|CLC={ITEM_ID}|LBL=LBL-NEW|"
    "HSH=bbbbbbbbbbbbbbbb"
)


def _label(
    *,
    label_id,
    qr_payload,
    hash_prefix,
    state="ACTIVE",
    instruction_id="INS-OLD",
    business_date="2026-07-27",
    worker_code="2270730200-1",
    label_version=1,
    membership_version=1,
):
    return {
        "label_id": label_id,
        "qr_payload": qr_payload,
        "hash_prefix": hash_prefix,
        "scan_anchor_input_tag_id": INPUT_TAG_ID,
        "item_id": ITEM_ID,
        "state": state,
        "instruction_id": instruction_id,
        "business_date": business_date,
        "worker_code": worker_code,
        "label_version": label_version,
        "membership_version": membership_version,
        "member_count": 2,
        "membership_hash": MEMBERSHIP_HASH,
    }


def _package_response(resolution):
    return {
        "bundle": {
            "candidate_count": 1,
            "bundle_role": "PACKAGE_SOURCE",
            "bundle_state": "AVAILABLE",
            "item_id": ITEM_ID,
            "source_session_id": INPUT_TAG_ID,
            "member_count": 2,
            "membership_hash": MEMBERSHIP_HASH,
            "authority_scope_id": SCOPE,
        },
        "input_tag": {
            "input_tag_id": INPUT_TAG_ID,
            "item_id": ITEM_ID,
            "label_id": "LBL-OLD",
            "hash_prefix": "aaaaaaaaaaaaaaaa",
            "qr_payload": CANONICAL_QR,
        },
        "phs_label_resolution": resolution,
    }


def _replaced_resolution(*, effective=None):
    return {
        "resolution": "OVERLAY_REPLACED",
        "status": "REPLACED",
        "scanned_label": _label(
            label_id="LBL-OLD",
            qr_payload=CANONICAL_QR,
            hash_prefix="aaaaaaaaaaaaaaaa",
            state="SUPERSEDED",
        ),
        "effective_labels": (
            effective
            if effective is not None
            else [
                _label(
                    label_id="LBL-NEW",
                    qr_payload=TARGET_QR,
                    hash_prefix="bbbbbbbbbbbbbbbb",
                    instruction_id="INS-NEW",
                    business_date="2026-07-28",
                    worker_code="2270730200-2",
                    label_version=2,
                    membership_version=1,
                )
            ]
        ),
    }


def _current_set():
    return {
        "id": "SET-ONE",
        "raw": [CANONICAL_QR],
        "parsed": [ITEM_ID],
        "central_inherit_all": True,
        "canonical_input_tag_qr": CANONICAL_QR,
        "physical_scanned_qr_payload": CANONICAL_QR,
        "active_label_qr_payload": CANONICAL_QR,
        "active_label_id": "LBL-OLD",
        "active_label_business_date": "2026-07-27",
        "active_label_worker_code": "2270730200-1",
        "active_label_instruction_id": "INS-OLD",
        "active_label_version": 1,
        "active_membership_version": 1,
        "active_label_resolution": "OVERLAY_ACTIVE",
        "package_source_snapshot": {
            "authority_scope_id": SCOPE,
            "member_count": 2,
            "membership_hash": MEMBERSHIP_HASH,
            "bundle_id": "TRANSFER-ONE",
        },
        "package_submission_status": "",
        "sealed_transfer": {"SID": "SEAL-ONE"},
    }


TARGET_INSTRUCTION = {
    "instruction_id": "INS-NEW",
    "business_date": "2026-07-28",
    "item_id": ITEM_ID,
    "uom": "PCS",
    "target_qty_pcs": 2,
    "item_daily_ordinal": 2,
    "worker_code": "2270730200-2",
    "entity_version": 4,
    "state": "PLANNED",
}


class _Renderer:
    def __init__(self, path):
        self.path = Path(path)

    def render(self, current_set, target):
        self.path.write_bytes(b"png")
        return RenderedPHSLabel(str(self.path), "d" * 64)


class _Printer:
    def print_png(self, filepath, *, document_name):
        return PhysicalPrintEvidence(
            printer_name="TEST-PRINTER",
            spool_job_id=17,
            document_name=document_name,
            submitted_at="2026-07-28T00:00:00Z",
        )


class _FailingPrinter:
    def print_png(self, filepath, *, document_name):
        raise RuntimeError("printer offline")


class _Client:
    def __init__(self):
        self.calls = []
        self.exchange_state = "PREPARED"
        self.source_membership_hash = MEMBERSHIP_HASH
        self.target = _label(
            label_id="LBL-NEW",
            qr_payload=TARGET_QR,
            hash_prefix="bbbbbbbbbbbbbbbb",
            state="PENDING_ACTIVATION",
            instruction_id="INS-NEW",
            business_date="2026-07-28",
            worker_code="2270730200-2",
            label_version=1,
            membership_version=1,
        )

    def list_phs_work_instruction_candidates(self, **kwargs):
        self.calls.append(("candidates", kwargs))
        return {
            "authority_scope_id": SCOPE,
            "business_date": "2026-07-28",
            "item_id": ITEM_ID,
            "uom": "PCS",
            "target_qty_pcs": 2,
            "candidate_count": 1,
            "candidates": [dict(TARGET_INSTRUCTION)],
        }

    def resolve_active_phs_label(self, input_tag_id, **kwargs):
        self.calls.append(("resolve_active", input_tag_id))
        return {
            "status": "ACTIVE",
            "resolution": "OVERLAY_ACTIVE",
            "input_tag": {"qr_payload": CANONICAL_QR},
            "active_label": _label(
                label_id="LBL-OLD",
                qr_payload=CANONICAL_QR,
                hash_prefix="aaaaaaaaaaaaaaaa",
            ),
        }

    def adopt_phs_label(self, **kwargs):
        self.calls.append(("adopt", kwargs))
        return {
            "label": _label(
                label_id="LBL-OLD",
                qr_payload=CANONICAL_QR,
                hash_prefix="aaaaaaaaaaaaaaaa",
            )
        }

    def prepare_phs_label_exchange(self, **kwargs):
        self.calls.append(("prepare", kwargs))
        return self._exchange("PREPARED", 1)

    def get_phs_label_exchange(self, exchange_id, **kwargs):
        self.calls.append(("get", exchange_id))
        version = 3 if self.exchange_state == "COMMITTED" else 2
        return self._exchange(self.exchange_state, version)

    def request_phs_label_print(self, exchange_id, **kwargs):
        self.calls.append(("request_print", kwargs))
        return {
            "print_attempt": {
                "print_attempt_id": "PRINT-ONE",
                "label_id": "LBL-NEW",
                "state": "REQUESTED",
            },
            **self._exchange("PRINTING", 1),
        }

    def complete_phs_label_print(
        self, print_attempt_id, *, succeeded, **kwargs
    ):
        self.calls.append(("complete_print", succeeded))
        if succeeded:
            self.exchange_state = "READY"
            return {
                "print_attempt": {
                    "print_attempt_id": "PRINT-ONE",
                    "label_id": "LBL-NEW",
                    "state": "SUCCEEDED",
                },
                **self._exchange("READY", 2),
            }
        return {
            "print_attempt": {
                "print_attempt_id": "PRINT-ONE",
                "label_id": "LBL-NEW",
                "state": "FAILED",
            },
            **self._exchange("PREPARED", 2),
        }

    def activate_phs_label_exchange(self, exchange_id, **kwargs):
        self.calls.append(("activate", kwargs))
        self.exchange_state = "COMMITTED"
        return self._exchange("COMMITTED", 3)

    def _exchange(self, state, version):
        target = dict(self.target)
        source = _label(
            label_id="LBL-OLD",
            qr_payload=CANONICAL_QR,
            hash_prefix="aaaaaaaaaaaaaaaa",
            state=("SUPERSEDED" if state == "COMMITTED" else "ACTIVE"),
            label_version=(2 if state == "COMMITTED" else 1),
        )
        source["membership_hash"] = self.source_membership_hash
        if state == "COMMITTED":
            target["state"] = "ACTIVE"
            target["label_version"] = 2
        return {
            "status": state,
            "exchange": {
                "exchange_id": "EXCHANGE-ONE",
                "exchange_kind": "SINGLE",
                "state": state,
                "entity_version": version,
            },
            "source_labels": [source],
            "target_labels": [target],
        }


def test_replaced_scan_keeps_canonical_and_selects_one_active_successor():
    evidence = normalize_packaging_phs_label_evidence(
        CANONICAL_QR,
        _package_response(_replaced_resolution()),
    )

    assert evidence.canonical_input_tag_qr == CANONICAL_QR
    assert evidence.physical_scanned_qr_payload == CANONICAL_QR
    assert evidence.active_label_qr_payload == TARGET_QR
    assert evidence.active_label_id == "LBL-NEW"
    assert evidence.active_label_business_date == "2026-07-28"
    assert evidence.active_label_worker_code == "2270730200-2"
    assert evidence.replaced_scan is True


def test_pending_or_ambiguous_successor_is_fail_closed():
    pending = _replaced_resolution()
    pending["resolution"] = "OVERLAY_NOT_ACTIVE"
    pending["status"] = "PENDING_ACTIVATION"
    with pytest.raises(PHSLabelWorkflowError) as pending_error:
        normalize_packaging_phs_label_evidence(
            CANONICAL_QR, _package_response(pending)
        )
    assert pending_error.value.code == "PHS2_LABEL_NOT_ACTIVE"

    ambiguous = _replaced_resolution(
        effective=[
            _replaced_resolution()["effective_labels"][0],
            {
                **_replaced_resolution()["effective_labels"][0],
                "label_id": "LBL-OTHER",
            },
        ]
    )
    with pytest.raises(PHSLabelWorkflowError) as ambiguous_error:
        normalize_packaging_phs_label_evidence(
            CANONICAL_QR, _package_response(ambiguous)
        )
    assert (
        ambiguous_error.value.code
        == "PHS2_ACTIVE_LABEL_AMBIGUOUS"
    )


def test_single_exchange_prints_then_activates_without_changing_packaging_state(
    tmp_path,
):
    current = _current_set()
    before_raw = copy.deepcopy(current["raw"])
    before_parsed = copy.deepcopy(current["parsed"])
    before_snapshot = copy.deepcopy(
        current["package_source_snapshot"]
    )
    client = _Client()
    coordinator = PHSLabelExchangeCoordinator(
        client,
        PHSLabelExchangeJournal(tmp_path / "journal.json"),
        _Renderer(tmp_path / "label.png"),
        _Printer(),
    )
    persisted = []

    result = coordinator.execute_single(
        current,
        TARGET_INSTRUCTION,
        persist_current_set=lambda: (
            persisted.append(copy.deepcopy(current)) or True
        ),
    )

    assert result.success is True
    assert result.status == "COMMITTED"
    assert current["raw"] == before_raw
    assert current["parsed"] == before_parsed
    assert current["package_source_snapshot"] == before_snapshot
    assert current["active_label_qr_payload"] == TARGET_QR
    assert current["active_label_business_date"] == "2026-07-28"
    assert current["active_label_worker_code"] == "2270730200-2"
    call_names = [call[0] for call in client.calls]
    assert call_names.index("prepare") < call_names.index("request_print")
    assert call_names.index("request_print") < call_names.index(
        "complete_print"
    )
    assert call_names.index("complete_print") < call_names.index(
        "activate"
    )
    assert persisted


def test_committed_remote_local_failure_reuses_exchange_without_second_write(
    tmp_path,
):
    visible = _current_set()
    working = copy.deepcopy(visible)
    client = _Client()
    journal = PHSLabelExchangeJournal(tmp_path / "journal.json")
    coordinator = PHSLabelExchangeCoordinator(
        client,
        journal,
        _Renderer(tmp_path / "label.png"),
        _Printer(),
    )

    with pytest.raises(PHSLabelWorkflowError) as first:
        coordinator.execute_single(
            working,
            TARGET_INSTRUCTION,
            persist_current_set=lambda: False,
        )
    assert first.value.code == "PHS_LOCAL_STATE_WRITE_FAILED"
    assert visible["active_label_qr_payload"] == CANONICAL_QR
    assert journal.load()["status"] == "COMMITTED_LOCAL_REFRESH_PENDING"
    assert [name for name, *_rest in client.calls].count("prepare") == 1
    assert [name for name, *_rest in client.calls].count("activate") == 1

    recovered = copy.deepcopy(visible)
    result = coordinator.execute_single(
        recovered,
        TARGET_INSTRUCTION,
        persist_current_set=lambda: True,
    )

    assert result.success is True
    assert recovered["active_label_qr_payload"] == TARGET_QR
    assert [name for name, *_rest in client.calls].count("prepare") == 1
    assert [name for name, *_rest in client.calls].count("activate") == 1


def test_print_failure_keeps_old_label_active_and_never_activates(tmp_path):
    current = _current_set()
    client = _Client()
    coordinator = PHSLabelExchangeCoordinator(
        client,
        PHSLabelExchangeJournal(tmp_path / "journal.json"),
        _Renderer(tmp_path / "label.png"),
        _FailingPrinter(),
    )

    result = coordinator.execute_single(
        current,
        TARGET_INSTRUCTION,
        persist_current_set=lambda: True,
    )

    assert result.success is False
    assert result.status == "PRINT_FAILED"
    assert current["active_label_qr_payload"] == CANONICAL_QR
    assert ("complete_print", False) in client.calls
    assert not any(name == "activate" for name, *_rest in client.calls)


def test_source_target_membership_mismatch_blocks_print_and_activation(tmp_path):
    current = _current_set()
    client = _Client()
    client.source_membership_hash = "f" * 64
    coordinator = PHSLabelExchangeCoordinator(
        client,
        PHSLabelExchangeJournal(tmp_path / "journal.json"),
        _Renderer(tmp_path / "label.png"),
        _Printer(),
    )

    with pytest.raises(PHSLabelWorkflowError) as error:
        coordinator.execute_single(
            current,
            TARGET_INSTRUCTION,
            persist_current_set=lambda: True,
        )

    assert error.value.code == "PHS_TARGET_LABEL_INVALID"
    assert current["active_label_qr_payload"] == CANONICAL_QR
    assert not any(
        name in {"request_print", "complete_print", "activate"}
        for name, *_rest in client.calls
    )


def test_corrupt_journal_blocks_second_prepare(tmp_path):
    path = tmp_path / "journal.json"
    path.write_text('{"schema_version":"wrong","state":{}}', encoding="utf-8")
    coordinator = PHSLabelExchangeCoordinator(
        _Client(),
        PHSLabelExchangeJournal(path),
        _Renderer(tmp_path / "label.png"),
        _Printer(),
    )

    with pytest.raises(PHSLabelWorkflowError) as error:
        coordinator.execute_single(
            _current_set(),
            TARGET_INSTRUCTION,
            persist_current_set=lambda: True,
        )
    assert error.value.code == "PHS_LABEL_JOURNAL_CORRUPT"


def test_package_draft_uses_active_physical_label_but_keeps_canonical_recovery():
    from Label_Match import _label_match_package_draft

    current = _current_set()
    current.update(
        {
            "active_label_qr_payload": TARGET_QR,
            "active_label_id": "LBL-NEW",
            "active_label_business_date": "2026-07-28",
            "active_label_worker_code": "2270730200-2",
            "active_label_instruction_id": "INS-NEW",
            "active_label_version": 2,
            "active_membership_version": 1,
            "sealed_transfer": None,
        }
    )

    draft = _label_match_package_draft(
        current,
        item_code=ITEM_ID,
    )
    saved = draft.to_dict()

    assert current["raw"] == [CANONICAL_QR]
    assert draft.source_input_tag_id == INPUT_TAG_ID
    assert draft.source_input_tag_label_id == "LBL-NEW"
    assert draft.source_input_tag_hash_prefix == "bbbbbbbbbbbbbbbb"
    assert saved["source_canonical_input_tag_qr"] == CANONICAL_QR
    assert saved["source_active_label_qr_payload"] == TARGET_QR
    assert saved["source_active_label_worker_code"] == "2270730200-2"
