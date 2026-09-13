"""F3 preparation and durable completion, called through the application owner.

The owner supplies live dependencies and retains writer admission, action gates,
success presentation and worker dispatch. Storage APIs and transaction order
remain unchanged.
"""


def _queue_authoritative_package(
    self,
    *,
    item_code,
    is_manual_complete,
    current_set_info=None,
    persist_current_state=None,
    logistics_runtime_required,
    _central_scan_count,
    _label_match_has_central_source_identity,
    _label_match_parse_sealed_transfer_qr,
    _label_match_parse_new_format_fields,
    _label_match_existing_package_row_metadata,
    _label_match_package_draft,
    PackageLogisticsError,
    OperationLeaseError,
    _clock,
    _timezone,
    _restore_draft,
):
    required_mode = bool(
        self.__dict__.get("_logistics_authoritative_required", False)
    ) or logistics_runtime_required()
    if (
        self.__dict__.get("run_tests", False)
        or self.__dict__.get("is_running_simulation", False)
    ):
        return None
    if is_manual_complete:
        if required_mode:
            raise PackageLogisticsError(
                "AUTHORITATIVE_LOGISTICS_REQUIRED: manual packaging completion is disabled"
            )
        return None
    current = (
        current_set_info
        if isinstance(current_set_info, dict)
        else (self.current_set_info or {})
    )
    raw = list(current.get("raw") or [])
    central_inherit_all = bool(current.get("central_inherit_all"))
    if (
        not central_inherit_all
        and not current.get("exact_rescan_active")
        and not current.get("exact_rescan_complete")
    ):
        central_inherit_all = bool(
            self.__dict__.get("package_logistics_client") is not None
            and raw
            and _label_match_has_central_source_identity(raw[0])
        )
    required_scan_count = (
        _central_scan_count()
        if central_inherit_all
        else self.TOTAL_SCAN_COUNT
    )
    if len(raw) != required_scan_count:
        return None
    if central_inherit_all:
        current["central_inherit_all"] = True
    try:
        sealed_transfer = _label_match_parse_sealed_transfer_qr(raw[0])
    except ValueError as exc:
        raise PackageLogisticsError(str(exc)) from exc
    exact_mode = bool(current.get("exact_rescan_complete"))
    central_enabled = self.__dict__.get("package_logistics_client") is not None
    if required_mode and not central_enabled:
        raise PackageLogisticsError(
            "AUTHORITATIVE_LOGISTICS_REQUIRED: installed central client/profile is unavailable"
        )
    if not sealed_transfer and not exact_mode:
        fields = _label_match_parse_new_format_fields(raw[0]) or {}
        has_structured_phs_identity = bool(
            str(fields.get("BND") or "").strip()
            or str(fields.get("ITG") or "").strip()
        )
        if central_enabled and not has_structured_phs_identity:
            raise PackageLogisticsError(
                "central packaging requires a sealed transfer QR, structured PHS BND/ITG "
                "lineage, or FULL EXACT_RESCAN; three product samples are not membership"
            )
        if not central_enabled:
            return {"status": "LEGACY_DIRECT_SYNC_ONLY", "sample_barcodes_are_membership": False}
    outbox = self.__dict__.get("package_outbox")
    if outbox is None:
        raise PackageLogisticsError("durable package outbox is unavailable")
    existing_row = outbox.get_by_set_id(
        str(current.get("id") or "")
    )
    if existing_row is not None:
        captured_intent_id = str(
            current.get("deferred_intent_id") or ""
        ).strip()
        if captured_intent_id:
            outbox.link_captured_intent_to_existing(
                captured_intent_id=captured_intent_id,
                set_id=str(current.get("id") or ""),
                idempotency_key=str(
                    existing_row.get("idempotency_key") or ""
                ),
            )
        return _label_match_existing_package_row_metadata(
            existing_row,
            current,
            item_code,
        )
    draft = _label_match_package_draft(
        current,
        item_code=item_code,
        require_source_snapshot=central_inherit_all,
    )
    operation_lease_id = ""
    operation_completed_at = ""
    if central_inherit_all and callable(
        getattr(
            self.__dict__.get("package_logistics_client"),
            "issue_operation_lease",
            None,
        )
    ):
        operation_lease_id = str(
            current.get("operation_lease_id") or ""
        ).strip()
        physical_qr = str(
            current.get("physical_scanned_qr_payload")
            or raw[0]
            or ""
        ).strip()
        lease_store = self.__dict__.get(
            "package_operation_lease_store"
        )
        lease_keyring = self.__dict__.get(
            "package_operation_lease_keyring"
        )
        if lease_store is None or lease_keyring is None:
            raise OperationLeaseError(
                "OPERATION_LEASE_REQUIRED",
                "durable operation lease support is required",
            )
        if operation_lease_id:
            verified_parts = self._load_reusable_operation_lease(
                physical_qr,
                expected_snapshot=current.get("package_source_snapshot"),
            )
        else:
            verified_parts = self._acquire_operation_lease(
                physical_qr,
                expected_snapshot=current.get("package_source_snapshot"),
            )
        if verified_parts is None:
            raise OperationLeaseError(
                "OPERATION_LEASE_REQUIRED",
                "a verified operation lease is required",
            )
        verified_lease = dict(verified_parts[3] or {})
        verified_lease_id = str(
            verified_lease.get("lease_id") or ""
        ).strip()
        if operation_lease_id and operation_lease_id != verified_lease_id:
            raise OperationLeaseError(
                "OPERATION_LEASE_STATE_CONFLICT",
                "current work references a different durable lease",
            )
        operation_lease_id = verified_lease_id
        lease_store.attach_set(
            operation_lease_id,
            str(current.get("id") or ""),
        )
        lease_row = lease_store.get(lease_id=operation_lease_id)
        if (
            not lease_row
            or str(lease_row.get("status") or "") != "PREFETCHED"
            or str(lease_row.get("set_id") or "")
            != str(current.get("id") or "")
        ):
            raise OperationLeaseError(
                "OPERATION_LEASE_STATE_CONFLICT",
                "the prefetched operation lease is unavailable",
            )
        if (
            str(lease_row.get("snapshot_hash") or "")
            != str(verified_lease.get("snapshot_hash") or "")
            or int(lease_row.get("fence") or 0)
            != int(verified_lease.get("fence") or 0)
        ):
            raise OperationLeaseError(
                "OPERATION_LEASE_ARTIFACT_MISMATCH",
                "the durable operation lease metadata differs",
            )
        current["operation_lease_id"] = operation_lease_id
        current["operation_lease_fence"] = int(
            verified_lease.get("fence") or 0
        )
        current["operation_lease_snapshot_hash"] = str(
            verified_lease.get("snapshot_hash") or ""
        )
        current["operation_lease_expires_at"] = str(
            verified_lease.get("expires_at") or ""
        )
        if self.__dict__.get("initialized_successfully", False):
            if callable(persist_current_state):
                persisted = bool(persist_current_state(current))
            else:
                persisted = bool(self._save_current_set_state())
            if not persisted:
                raise PackageLogisticsError(
                    "prefetched operation lease current-state save failed"
                )
        operation_completed_at = str(
            current.get("operation_lease_completed_at") or ""
        ).strip() or _clock().now(_timezone().utc).strftime(
            "%Y-%m-%dT%H:%M:%SZ"
        )
        current["operation_lease_completed_at"] = operation_completed_at
        draft_data = draft.to_dict()
        draft_data.update(
            {
                "operation_lease_id": operation_lease_id,
                "operation_lease_token": str(
                    lease_row.get("token") or ""
                ),
                "operation_lease_fence": int(
                    verified_lease.get("fence") or 0
                ),
                "operation_lease_snapshot_hash": str(
                    verified_lease.get("snapshot_hash") or ""
                ),
                "operation_lease_completed_at": operation_completed_at,
            }
        )
        draft = _restore_draft(draft_data)
    row = outbox.enqueue(
        draft,
        captured_intent_id=str(
            current.get("deferred_intent_id") or ""
        ).strip(),
    )
    return {
        "status": str(row.get("status") or "PENDING"),
        "idempotency_key": row["idempotency_key"],
        "source_bundle_id": draft.source_bundle_id,
        "source_bundle_ids": list(
            draft.work_group_source.get(
                "source_transfer_bundle_ids"
            )
            or ()
        ),
        "source_session_ids": list(draft.source_session_ids),
        "source_work_group_id": str(
            draft.phs_work_group.get("group_id") or ""
        ),
        "source_external_label": draft.source_external_label,
        "package_bundle_id": draft.package_bundle_id,
        "membership_mode": draft.membership_mode,
        "sample_barcodes": list(draft.sample_barcodes),
        "sample_barcodes_are_membership": False,
        "exact_rescan_count": len(draft.exact_rescan_barcodes),
        "expected_member_count": draft.expected_member_count,
        "expected_membership_hash": draft.expected_membership_hash,
        "operation_lease_id": operation_lease_id,
        "operation_lease_completed_at": operation_completed_at,
    }


def _persist_ui_lane_current_set_snapshot(self, current, *, _clock):
    return bool(
        self.data_manager.save_current_state(
            {
                "current_set_info": current,
                "timestamp": _clock().now().isoformat(),
            }
        )
    )


def _commit_finalized_set_durable(
    self,
    *,
    details,
    item_code,
    is_manual_complete,
    result,
    central_inherit_all,
    set_id_for_log,
    current_snapshot=None,
    deepcopy,
    _label_match_local_completion_event_exists,
    PackageLogisticsError,
):
    detached = isinstance(current_snapshot, dict)
    if (
        detached
        and self.__dict__.get("initialized_successfully", False)
        and list(current_snapshot.get("raw") or [])
        and not self._persist_ui_lane_current_set_snapshot(
            current_snapshot
        )
    ):
        raise PackageLogisticsError(
            "current packaging state could not be saved before completion"
        )
    package_logistics = (
        self._queue_authoritative_package(
            item_code=item_code,
            is_manual_complete=is_manual_complete,
            current_set_info=current_snapshot if detached else None,
            persist_current_state=(
                self._persist_ui_lane_current_set_snapshot
                if detached
                else None
            ),
        )
        if result == self.Results.PASS
        else None
    )
    durable_details = deepcopy(dict(details or {}))
    if package_logistics:
        durable_details["package_logistics"] = package_logistics
        durable_details["package_membership_mode"] = (
            package_logistics.get("membership_mode")
        )
        durable_details["sample_barcodes_are_membership"] = False
    local_event_durable = bool(
        central_inherit_all
        and package_logistics
        and _label_match_local_completion_event_exists(
            self.__dict__.get("data_manager"),
            set_id_for_log,
        )
    )
    if not local_event_durable:
        self.data_manager.log_event(
            self.Events.TRAY_COMPLETE,
            durable_details,
        )
        self._flush_data_manager_if_supported()
    if central_inherit_all and package_logistics:
        outbox = self.__dict__.get("package_outbox")
        if outbox is None:
            raise PackageLogisticsError(
                "durable package outbox disappeared before local completion"
            )
        lease_id = str(
            package_logistics.get("operation_lease_id") or ""
        )
        if lease_id:
            outbox.mark_local_completion_committed(
                package_logistics.get("idempotency_key"),
                operation_lease_id=lease_id,
                operation_completed_at=str(
                    package_logistics.get(
                        "operation_lease_completed_at"
                    )
                    or ""
                ),
            )
        else:
            outbox.mark_local_completion_committed(
                package_logistics.get("idempotency_key")
            )
    return {
        "details": durable_details,
        "package_logistics": package_logistics,
        "current_snapshot": current_snapshot if detached else None,
    }


def _apply_ui_lane_completion_snapshot(self, snapshot, *, deepcopy):
    if not isinstance(snapshot, dict):
        return True
    current = self.current_set_info or {}
    if (
        str(current.get("id") or "")
        != str(snapshot.get("id") or "")
        or tuple(current.get("raw") or ())
        != tuple(snapshot.get("raw") or ())
    ):
        return False
    for key in (
        "central_inherit_all",
        "operation_lease_id",
        "operation_lease_fence",
        "operation_lease_snapshot_hash",
        "operation_lease_expires_at",
        "operation_lease_completed_at",
    ):
        if key in snapshot:
            current[key] = deepcopy(snapshot[key])
    self.current_set_info = current
    return True


def _submit_finalized_set_on_lane(
    self,
    *,
    result,
    error_details,
    is_manual_complete,
    details,
    item_code,
    central_inherit_all,
    set_id_for_log,
    deepcopy,
    PackageLogisticsError,
):
    current_snapshot = deepcopy(self.current_set_info or {})

    def work():
        return self._commit_finalized_set_durable(
            details=details,
            item_code=item_code,
            is_manual_complete=is_manual_complete,
            result=result,
            central_inherit_all=central_inherit_all,
            set_id_for_log=set_id_for_log,
            current_snapshot=current_snapshot,
        )

    def finish(durable_completion):
        if not self._apply_ui_lane_completion_snapshot(
            durable_completion.get("current_snapshot")
        ):
            self._publish_durable_commit_block(
                PackageLogisticsError(
                    "packaging state changed before durable completion apply"
                )
            )
            return
        self._finalize_set(
            result,
            error_details,
            is_manual_complete,
            _durable_completion=durable_completion,
        )

    def fail(error):
        self._publish_durable_commit_block(error)

    admission = self._submit_ui_lane_task(
        name="f3-package-completion",
        busy_text="포장 완료 · 권한 확인 및 로컬 완료 저장 중",
        work=work,
        finish=finish,
        fail=fail,
    )
    return bool(admission is not None and admission.accepted)
