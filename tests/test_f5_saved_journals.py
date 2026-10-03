"""Saved F5 journals remain readable after removal of new date exchanges."""
from pathlib import Path
import copy
import hashlib
import json

import pytest

import Label_Match as lm
from package_logistics import PackageLogisticsClient
from phs_label_workflow import PHSLabelExchangeCoordinator, PHSLabelExchangeJournal
from tests import test_phs_label_workflow as single
from tests import test_phs_reconciliation_workflow as reconciliation
from tests._f5_recovery_fixture import restore_saved_f5_journal

FIXTURES = Path(__file__).parent / "fixtures" / "f5-recovery-167ac7e"


def test_new_date_exchange_entrypoints_are_removed():
    for name in ("execute_single", "execute_reconciliation", "list_candidates", "resolve_reconciliation_actions"):
        assert not hasattr(PHSLabelExchangeCoordinator, name)
    for name in ("list_phs_work_instruction_candidates", "resolve_phs_reconciliation_actions"):
        assert not hasattr(PackageLogisticsClient, name)
    for name in ("_handle_phs_label_exchange_shortcut", "_show_phs_reconciliation_scan_window",
                 "_begin_phs_reconciliation_lookup", "_show_phs_reconciliation_action_window",
                 "_show_phs_label_candidate_window", "_begin_phs_label_candidate_lookup",
                 "_start_phs_label_exchange", "_start_phs_reconciliation_exchange",
                 "PHS_LABEL_EXCHANGE_BUTTON_TEXT"):
        assert not hasattr(lm.Label_Match, name)


@pytest.mark.parametrize("name", ["single", "reconciliation-single", "reconciliation-batch",
                                 "reconciliation-split", "reconciliation-merge"])
def test_baseline_journal_bytes_recover_with_identical_state_and_request_identity(tmp_path, name):
    evidence = json.loads((FIXTURES / (name + ".state.json")).read_bytes())
    raw = (FIXTURES / (name + ".journal.json")).read_bytes()
    assert hashlib.sha256(raw).hexdigest() == evidence["journal_sha256"]
    current = copy.deepcopy(evidence["before"])
    original = json.loads(raw)["state"]
    if name == "single":
        client = single._Client()
        coordinator = PHSLabelExchangeCoordinator(client, PHSLabelExchangeJournal(tmp_path / "journal.json"),
                                                   single._Renderer(tmp_path / "label.png"), single._Printer())
        restore_saved_f5_journal(coordinator, name)
        result = coordinator.recover_current(current, persist_current_set=lambda: True)
    else:
        client = reconciliation._Client(reconciliation._resolution(name.split("-", 1)[1].upper()))
        coordinator = reconciliation._coordinator(tmp_path, client)
        restore_saved_f5_journal(coordinator, name)
        result = coordinator.recover_reconciliation(current_set=current, persist_current_set=lambda: True)
    assert result.success and result.status == evidence["status"]
    assert current == evidence["after"]
    state = coordinator.journal.load()
    assert state["prepare_idempotency_key"] == original["prepare_idempotency_key"]
    assert state["set_id"] == original["set_id"]
    assert coordinator.journal.path.is_file()
    assert (FIXTURES / (name + ".journal.json")).read_bytes() == raw
    assert current["raw"] == evidence["before"]["raw"]
    assert current["parsed"] == evidence["before"]["parsed"]
    assert current["package_source_snapshot"] == evidence["before"]["package_source_snapshot"]


def test_no_saved_journal_cannot_create_an_exchange_or_write_a_journal(tmp_path):
    client = single._Client()
    journal = PHSLabelExchangeJournal(tmp_path / "journal.json")
    coordinator = PHSLabelExchangeCoordinator(client, journal, single._Renderer(tmp_path / "label.png"), single._Printer())
    assert coordinator.recover_current(single._current_set(), persist_current_set=lambda: pytest.fail("unexpected write")) is None
    assert coordinator.recover_reconciliation() is None
    assert client.calls == []
    assert not journal.path.exists()


def test_unrelated_current_set_keeps_saved_journal_bytes_and_makes_no_requests(tmp_path):
    client = single._Client()
    coordinator = PHSLabelExchangeCoordinator(client, PHSLabelExchangeJournal(tmp_path / "journal.json"),
                                               single._Renderer(tmp_path / "label.png"), single._Printer())
    restore_saved_f5_journal(coordinator, "single")
    before = coordinator.journal.path.read_bytes()
    current = single._current_set()
    current["id"] = "UNRELATED"
    assert coordinator.recover_current(current, persist_current_set=lambda: pytest.fail("unexpected write")) is None
    assert coordinator.journal.path.read_bytes() == before
    assert client.calls == []
