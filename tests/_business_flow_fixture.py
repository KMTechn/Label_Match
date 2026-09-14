"""Headless business fixtures; only UI, clock and remote transport are replaced.

Use through business_factory in test_business_flow_fixtures. See
docs/spec/operations.md#business-fixtures-w7lmfix for boundaries and reuse.
"""

from collections import defaultdict
from copy import deepcopy
from datetime import datetime, timezone
import json
from types import SimpleNamespace
from urllib.parse import parse_qs, unquote, urlsplit

import Label_Match as lm
import package_logistics as package
import terminal_operation_lease as lease
from sealed_transfer_exchange import (
    SealedTransferExchangeCoordinator, SealedTransferExchangeStore,
)
from tests import test_package_logistics as package_fixture
from tests import test_sealed_transfer_exchange as exchange_fixture
from tests.test_label_match_core import _FakeHistoryTree, _FakeLabel
from tests.test_phs_label_workflow import _source_input_tag


class BusinessClock(datetime):
    instant = datetime(2026, 9, 14, 23, 59, tzinfo=timezone.utc)

    @classmethod
    def now(cls, tz=None):
        return cls.instant.astimezone(tz) if tz else cls.instant.replace(tzinfo=None)


def source_response(projection, *, input_tag_id, raw):
    return {
        "candidate_count": 1,
        "bundle": {**projection, "bundle_role": "PACKAGE_SOURCE",
                   "source_session_id": input_tag_id},
        "input_tag": _source_input_tag(input_tag_id, raw,
                                       item_id=projection["item_id"], uom="EA"),
    }


class BusinessProvider:
    """An in-memory service boundary behind the real PackageLogisticsClient.

    Receipts come from the existing contract fixtures. Effects count provider
    commits, not a real server transaction. No live socket or server is used.
    """

    def __init__(self, *, exchange=False, pair_count=2):
        self.exchange = exchange_fixture.BatchClient(
            pair_count=pair_count, extension=False,
        ) if exchange else None
        projection = (self.exchange.get_bundle(exchange_fixture.TARGET,
                      authority_scope_id=exchange_fixture.SCOPE)
                      if exchange else package_fixture._projection())
        if exchange:
            projection["controlled_reseal_eligible"] = True
        self.raw = (
            "PHS=2|SRC=KMTECH_INPUT_TAG|ITG=ITG-BUSINESS|"
            f"CLC={projection['item_id']}|LBL=LBL-BUSINESS|HSH=0123456789abcdef"
        )
        self.response = source_response(projection, input_tag_id="ITG-BUSINESS", raw=self.raw)
        self.config = package.PackageClientConfig(
            base_url="https://business.example.test", token="fixture-only",
            authority_scope_id=projection["authority_scope_id"],
            authority_epoch=projection["authority_epoch"],
            ledger_plane=projection["ledger_plane"], plane_epoch=projection["plane_epoch"],
            source_host_id="HOST-PACK-01", device_id="PACK-01",
        )
        self.signer = package_fixture._OperationLeaseTestSigner()
        self.mode = "online"
        self.bad_signature = False
        self.calls = []
        self.receipts = {}
        self.commands = {}
        self.effects = 0

    def client(self):
        return package.PackageLogisticsClient(self.config, transport=self.transport)

    def transport(self, method, url, headers, body, timeout):
        route = urlsplit(url)
        assert route.netloc == "business.example.test"
        assert headers["Authorization"] == "Bearer fixture-only"
        assert headers["X-Logistics-Device-Id"] == self.config.device_id
        assert headers["X-Logistics-Source-Host-Id"] == self.config.source_host_id
        key = headers.get("Idempotency-Key", "")
        self.calls.append((method, route.path, key))
        command = json.loads(body) if body else None
        if self.mode == "unauthorized":
            raise package.PackageApiError(401, "UNAUTHORIZED", "fixture access refused")
        if route.path.endswith("/operation-leases/issue"):
            assert method == "POST" and key
            assert command["scan_payload"] == self.raw
            assert command["operation"] == "CREATE_PACKAGE"
            snapshot = lm._label_match_package_source_snapshot(self.response)
            binding = lm._label_match_operation_lease_binding(self.raw, snapshot, self.config)
            artifact = self.signer.artifact(binding=binding, operation_snapshot=self.response)
            if self.bad_signature:
                other = package_fixture._OperationLeaseTestSigner()
                other.kid = self.signer.kid
                artifact["token"] = other.artifact(
                    binding=binding, operation_snapshot=self.response,
                )["token"]
            return {"ok": True, "data": artifact}
        if "/receipts/" in route.path:
            receipt_key = unquote(route.path.rsplit("/", 1)[-1])
            if self.mode in {"offline", "lost_ack"}:
                raise package.PackageTransportError("fixture receipt unavailable")
            if receipt_key not in self.receipts:
                raise package.PackageApiError(404, "RECEIPT_NOT_FOUND", "absent", committed=False)
            return {"ok": True, "data": deepcopy(self.receipts[receipt_key])}
        if route.path.endswith("/capabilities"):
            value = self.exchange.get_capabilities()
        elif "/replacements/good-source/resolve" in route.path:
            query = parse_qs(route.query)
            value = self.exchange.resolve_good_source(
                authority_scope_id=query["authority_scope_id"][0], barcode=query["barcode"][0],
            )
        elif method == "GET" and route.path.endswith("/bundles/resolve"):
            value = self.response
        elif method == "GET" and "/bundles/" in route.path:
            assert unquote(route.path.rsplit("/", 1)[-1]) == self.response["bundle"]["bundle_id"]
            value = self.response["bundle"]
        elif method == "POST" and (
            route.path.endswith("/members/replace-and-reseal")
            or route.path.endswith("/packages")
        ):
            assert key and command["idempotency_key"] == key
            if self.mode == "offline":
                raise package.PackageTransportError("fixture offline before commit")
            if self.mode == "conflict":
                raise package.PackageApiError(409, "VERSION_CONFLICT", "fixture stale version", committed=False)
            if key in self.commands:
                assert command == self.commands[key]
            else:
                if self.exchange:
                    value = self.exchange.replace_and_reseal_transfer(command)
                else:
                    # Keep the original receipt's membership/version evidence;
                    # bind only the package identity and real signed lease.
                    draft_data = package_fixture._draft().to_dict()
                    draft_data.update(package_bundle_id=command["payload"]["package_bundle_id"],
                                      sample_barcodes=[], external_label=command["payload"]["external_label"])
                    value = package_fixture._receipt(package.PackageCommandDraft.from_dict(draft_data))
                    value["idempotency_key"] = key
                    op = command["payload"]["operation_lease"]
                    value["data"]["operation_lease_consumption"] = {
                        "contract_version": "terminal-operation-lease-consume-v1",
                        "lease_id": op["lease_id"], "status": "CONSUMED", "fence": op["fence"],
                        "operation_result_id": value["receipt_id"],
                        "consumed_at": BusinessClock.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
                    }
                self.commands[key] = deepcopy(command)
                self.receipts[key] = deepcopy(value)
                self.effects += 1
            if self.mode == "lost_ack":
                raise package.PackageTransportError("fixture response lost after commit")
            value = self.receipts[key]
        else:
            raise AssertionError(f"Unexpected fixture route: {method} {route.path}")
        return {"ok": True, "data": deepcopy(value)}


class BusinessCase:
    def __init__(self, root, provider):
        self.root, self.provider = root, provider
        self.actions = []
        self.open()

    def open(self):
        app = object.__new__(lm.Label_Match)
        app.tk = SimpleNamespace()  # No Tcl interpreter; UI methods are observers.
        app.current_set_info = {"id": None, "raw": [], "parsed": [], "start_time": None}
        app.initialized_successfully = True
        app.run_tests = app.is_running_simulation = app.is_blinking = False
        app._logistics_authoritative_required = True
        app.worker_name = "fixture-packer"
        app.save_directory = str(self.root)
        app.data_manager = lm.DataManager(str(self.root), "포장실", app.worker_name, "BUSINESS")
        database = self.root / "package_logistics_outbox.sqlite3"
        app.package_outbox = package.PackageOutbox(database)
        app.package_logistics_client = self.provider.client()
        app.package_operation_lease_store = lease.OperationLeaseStore(database)
        app.package_operation_lease_keyring = lease.PinnedOperationLeaseKeyring(self.root / "keys.json")
        app.sealed_transfer_exchange_store = SealedTransferExchangeStore(database)
        app.sealed_transfer_exchange_coordinator = SealedTransferExchangeCoordinator(
            app.sealed_transfer_exchange_store, app.package_logistics_client,
        )
        item = self.provider.response["bundle"]["item_id"]
        app.items_data = {item: {"Item Name": "Fixture item", "Spec": "EA"}}
        app.scan_count = defaultdict(lambda: defaultdict(int))
        app.global_scanned_set = set()
        app.set_details_map, app.history_row_details_map = {}, {}
        app.history_tree, app.save_status_label = _FakeHistoryTree(), _FakeLabel()
        app.progress_bar = {}
        app.entry = SimpleNamespace(focus_set=lambda: None)
        for name in ("update_big_display", "_update_status_label", "_update_history_tree_in_progress",
                     "_render_operator_workbench", "_focus_scan_entry_if_available"):
            setattr(app, name, lambda *a, **k: None)
        app.after = lambda *a: None  # Tests explicitly advance processor work.
        app._play_sound = lambda sound: self.actions.append("sound:" + sound)
        app._start_package_outbox_drain = lambda: self.actions.append("drain")
        app._update_summary_tree = lambda: self.actions.append("summary")
        self.app = app

    def accept(self, *, set_id="SET-BUSINESS"):
        evidence, snapshot, sealed, operation_lease = self.app._resolve_central_phs2_scan_overlay(
            self.provider.raw, self.provider.response["bundle"]["item_id"],
        )
        assert operation_lease is None
        if self.provider.exchange:
            # The existing direct TRANSFER projection has no work-group topology.
            # Start the exchange domain fixture with a product-validated seal;
            # this does not prove the current work-group F4 dialog admission.
            sealed = lm._label_match_active_seal_from_package_source(self.provider.response["bundle"])
        assert self.app._accept_resolved_central_phs2_scan(
            evidence, snapshot, sealed, local_work_identity=set_id,
        ) is True
        self.app.data_manager.flush(timeout=5)
        self.actions.clear()

    def close(self):
        self.app.data_manager.flush(timeout=5)
        assert self.app.data_manager.close(timeout=5) is True
        assert not self.app.data_manager.log_thread.is_alive()

    def restart(self, *, at=None):
        assert self.app._save_current_set_state()
        self.close()
        if at is not None:
            BusinessClock.instant = at
        self.open()
        self.app._load_current_set_state()

    def drain(self):
        return package.PackageOutboxProcessor(
            self.app.package_outbox, self.app.package_logistics_client,
        ).drain()
