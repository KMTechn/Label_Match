"""F3 preparation and durable completion, called through the application owner.

The owner supplies live dependencies and retains writer admission, action gates,
success presentation and worker dispatch. Storage APIs and transaction order
remain unchanged.
"""

import label_transition

_TRANSITION_KEYS = ("transition_class", "transition_reasons", "transition_duplicate")


def _registered_label_shape(
    self,
    current,
    *,
    _label_match_parse_sealed_transfer_qr,
):
    """(shape, reasons) of a registered PC's own-path start label.

    Any new-system key (sealed transfer QR, PHS2, BND/ITG) is lineage, even in
    a malformed label, so such a label is never LEGACY.  Central sets and the
    F4 exact rescan keep their own paths and return ("", ()).
    """

    if (
        self.__dict__.get("package_logistics_client") is None
        or current.get("central_inherit_all")
        or current.get("exact_rescan_active")
        or current.get("exact_rescan_complete")
    ):
        return "", ()
    raw = list(current.get("raw") or [])
    if not raw:
        return "", ()
    shape, reasons, _item_code = label_transition.classify_start_label(
        raw[0], parse_sealed=_label_match_parse_sealed_transfer_qr,
    )
    return shape, reasons


def _transition_active(self):
    """The administrator switch applies to a registered or required PC only.

    Tests and simulations keep the base flow, as the completion gate does.
    """

    state = self.__dict__
    return bool(
        state.get("_legacy_label_transition_enabled", False)
        and not state.get("run_tests", False)
        and not state.get("is_running_simulation", False)
        and (
            state.get("package_logistics_client") is not None
            or state.get("_logistics_authoritative_required", False)
        )
    )


def _transition_applies(self, current):
    """The switch is on, or the set started while it was on (class pinned)."""

    state = self.__dict__
    if state.get("run_tests", False) or state.get("is_running_simulation", False):
        return False
    return _transition_active(self) or (
        str((current or {}).get("transition_class") or "") in label_transition.CLASSES
    )


def _transition_local(transition_class, reasons=(), duplicate=False):
    # Never a package outbox command: the server only observes the
    # TRAY_COMPLETE event through the existing direct sync.
    return {
        "status": label_transition.LOCAL_ONLY_STATUS,
        "sample_barcodes_are_membership": False,
        **label_transition.fields(transition_class, reasons, duplicate),
    }


def _transition_label_class(current, *, parse_sealed):
    """LEGACY or PHS2_MALFORMED from the start label, else ("", ())."""

    raw = list(current.get("raw") or [])
    if (
        not raw
        or current.get("central_inherit_all")
        or current.get("exact_rescan_active")
        or current.get("exact_rescan_complete")
    ):
        return "", ()
    shape, reasons, _item_code = label_transition.classify_start_label(
        raw[0], parse_sealed=parse_sealed,
    )
    if shape == label_transition.SHAPE_LEGACY:
        return label_transition.LEGACY, ()
    if shape == label_transition.SHAPE_MALFORMED:
        return label_transition.PHS2_MALFORMED, reasons
    return "", ()


def _transition_completion(current, *, parse_sealed):
    """Class decided before the central path, or ("", reasons, duplicate)."""

    reasons = list(current.get("transition_reasons") or ())
    decided = str(current.get("transition_class") or "")
    if decided not in label_transition.LOCAL_CLASSES:
        decided, label_reasons = _transition_label_class(
            current, parse_sealed=parse_sealed,
        )
        reasons.extend(label_reasons)
    return decided, reasons, bool(current.get("transition_duplicate"))


def _close_transition_capture(self, current):
    """Close the set's unsubmitted PHS2 capture exactly as F1 would.

    A local completion must not leave that capture VALIDATED: an open intent
    holds every later capture in its FIFO partition.  False when the capture
    is not safely cancellable (unknown lease or validation result).
    """

    if not str(current.get("deferred_intent_id") or "").strip():
        return True
    try:
        self._cancel_deferred_capture_for_set(current)
    except Exception as error:
        print(
            "transition local completion kept the central path: "
            f"{getattr(error, 'code', error.__class__.__name__)}"
        )
        return False
    return True


def _decide_transition_local(
    self,
    current,
    transition_class,
    reasons,
    duplicate,
    *,
    persist_current_state=None,
):
    """Pin a local decision in the saved set before any completion row.

    A retry after an uncertain flush, a restart or a switch-off then completes
    the set the same way and never re-runs the central command.  False (and
    nothing changed) when the capture cannot be closed or the state not saved.
    """

    outbox = self.__dict__.get("package_outbox")
    if outbox is not None and outbox.get_by_set_id(str(current.get("id") or "")) is not None:
        return False  # A submitted (or unknown) package is never recorded as local.
    previous = {key: current[key] for key in _TRANSITION_KEYS if key in current}

    def restore():
        for key in _TRANSITION_KEYS:
            current.pop(key, None)
        current.update(previous)

    # The decision is saved while the capture is still open: a failed save or a
    # stop here leaves the saved set and its capture as they were.
    current.update(label_transition.fields(transition_class, reasons, duplicate))
    if not _persist_transition_state(self, current, persist_current_state):
        restore()
        return False
    if not _close_transition_capture(self, current):
        restore()
        _persist_transition_state(self, current, persist_current_state)
        return False
    # The closed capture no longer owns the set.  A stop before this save is
    # safe too: recovery keeps a pinned local set whose capture is cancelled.
    if current.pop("deferred_intent_id", None) is not None:
        _persist_transition_state(self, current, persist_current_state)
    return True


def _persist_transition_state(self, current, persist_current_state=None):
    if not self.__dict__.get("initialized_successfully", False):
        return True
    return bool(
        persist_current_state(current)
        if callable(persist_current_state)
        else self._save_current_set_state()
    )


def _queue_transition_package(
    self,
    current,
    *,
    package,
    is_manual_complete,
    parse_sealed,
    errors,
    reason,
    PackageLogisticsError,
    persist_current_state=None,
):
    """Accept every set; only a successful central flow enters the ledger."""

    decided, reasons, duplicate = _transition_completion(
        current, parse_sealed=parse_sealed,
    )
    if decided:
        # A decision saved just before a stop may still own an open capture.
        if not _close_transition_capture(self, current):
            raise PackageLogisticsError(
                "transition local completion could not close the set's PHS2 capture"
            )
        current.pop("deferred_intent_id", None)
        return _transition_local(decided, reasons, duplicate)
    if is_manual_complete:
        reasons = [*reasons, "PARTIAL_PACKAGE"]
        if not _decide_transition_local(
            self, current, label_transition.PHS2_LOCAL, reasons, duplicate,
            persist_current_state=persist_current_state,
        ):
            raise PackageLogisticsError(
                "AUTHORITATIVE_LOGISTICS_REQUIRED: manual packaging completion is disabled"
            )
        return _transition_local(label_transition.PHS2_LOCAL, reasons, duplicate)
    try:
        queued = package()
    except errors as error:
        outbox = self.__dict__.get("package_outbox")
        reasons = [*reasons, reason(error)]
        if (
            outbox is not None
            and outbox.get_by_set_id(str(current.get("id") or "")) is not None
        ) or not _decide_transition_local(
            self, current, label_transition.PHS2_LOCAL, reasons, duplicate,
            persist_current_state=persist_current_state,
        ):
            raise
        return _transition_local(label_transition.PHS2_LOCAL, reasons, duplicate)
    if not queued:
        reasons = [*reasons, "PACKAGE_NOT_CREATED"]
        if not _decide_transition_local(
            self, current, label_transition.PHS2_LOCAL, reasons, duplicate,
            persist_current_state=persist_current_state,
        ):
            raise PackageLogisticsError("transition local decision could not be saved")
        return _transition_local(label_transition.PHS2_LOCAL, reasons, duplicate)
    return {
        **queued,
        **label_transition.fields(label_transition.PHS2_CENTRAL, reasons, duplicate),
    }


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
    _transition_errors=(),
    _transition_reason=str,
):
    def package():
        return _queue_package(
            self,
            item_code=item_code,
            is_manual_complete=is_manual_complete,
            current_set_info=current_set_info,
            persist_current_state=persist_current_state,
            logistics_runtime_required=logistics_runtime_required,
            _central_scan_count=_central_scan_count,
            _label_match_has_central_source_identity=_label_match_has_central_source_identity,
            _label_match_parse_sealed_transfer_qr=_label_match_parse_sealed_transfer_qr,
            _label_match_parse_new_format_fields=_label_match_parse_new_format_fields,
            _label_match_existing_package_row_metadata=_label_match_existing_package_row_metadata,
            _label_match_package_draft=_label_match_package_draft,
            PackageLogisticsError=PackageLogisticsError,
            OperationLeaseError=OperationLeaseError,
            _clock=_clock,
            _timezone=_timezone,
            _restore_draft=_restore_draft,
        )

    current = (
        current_set_info
        if isinstance(current_set_info, dict)
        else (self.__dict__.get("current_set_info") or {})
    )
    if not _transition_applies(self, current):
        return package()
    return _queue_transition_package(
        self,
        current,
        package=package,
        is_manual_complete=is_manual_complete,
        parse_sealed=_label_match_parse_sealed_transfer_qr,
        errors=tuple(_transition_errors),
        reason=_transition_reason,
        PackageLogisticsError=PackageLogisticsError,
        persist_current_state=persist_current_state,
    )


def _queue_package(
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


def _transition_failed_completion(current, *, parse_sealed):
    """Class of a failed set's completion row while the switch is on."""

    if not list(current.get("raw") or []):
        return None
    decided, reasons, duplicate = _transition_completion(
        current, parse_sealed=parse_sealed,
    )
    if not decided:
        decided = label_transition.PHS2_LOCAL
        reasons.append("LABEL_MATCH_FAILED_OR_MISMATCH")
    return label_transition.fields(decided, reasons, duplicate)


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
    _label_match_parse_sealed_transfer_qr=None,
    _app_version="",
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
    transition = None
    if isinstance(package_logistics, dict) and _TRANSITION_KEYS[0] in package_logistics:
        package_logistics = dict(package_logistics)
        transition = {key: package_logistics.pop(key) for key in _TRANSITION_KEYS}
    elif result != self.Results.PASS and callable(_label_match_parse_sealed_transfer_qr):
        current = (
            current_snapshot
            if detached
            else (self.__dict__.get("current_set_info") or {})
        )
        if _transition_applies(self, current):
            transition = _transition_failed_completion(
                current, parse_sealed=_label_match_parse_sealed_transfer_qr,
            )
    if package_logistics:
        durable_details["package_logistics"] = package_logistics
        durable_details["package_membership_mode"] = (
            package_logistics.get("membership_mode")
        )
        durable_details["sample_barcodes_are_membership"] = False
    if transition is not None:
        durable_details.update(transition)
        # The shared event ID: a resend reuses this one row (see below).
        durable_details["idempotency_key"] = label_transition.event_key(
            str(getattr(self.__dict__.get("data_manager"), "unique_id", "") or ""),
            set_id_for_log,
            self.Events.TRAY_COMPLETE,
        )
        if _app_version:
            durable_details["app_version"] = _app_version
        # The relay takes a raw-only receipt for this row only when the
        # uploading manifest names the same PC (direct_sync_push).  Not
        # detail.source_host_id: Web checks that key against the install.
        source_host_id = str(getattr(
            getattr(self.__dict__.get("package_logistics_client"), "config", None),
            "source_host_id",
            "",
        ) or "").strip()
        if source_host_id:
            durable_details["transition_source_host_id"] = source_host_id
    if (
        transition is not None
        and transition["transition_class"] != label_transition.PHS2_CENTRAL
    ):
        # The saved set marks the attempt before its row is appended.  A retry
        # (or a restart) after an uncertain flush then finds that one row in
        # every retained daily file and resends it, never a second row.
        marked = (
            current_snapshot
            if detached
            else (self.__dict__.get("current_set_info") or {})
        )
        if marked.get("transition_row_key") == durable_details["idempotency_key"]:
            self._flush_data_manager_if_supported()
            local_event_durable = bool(
                _label_match_local_completion_event_exists(
                    self.__dict__.get("data_manager"),
                    set_id_for_log,
                )
            )
        else:
            marked["transition_row_key"] = durable_details["idempotency_key"]
            if not _persist_transition_state(
                self,
                marked,
                self._persist_ui_lane_current_set_snapshot if detached else None,
            ):
                raise PackageLogisticsError(
                    "current packaging state could not be saved before completion"
                )
            local_event_durable = False
    else:
        if transition is not None:
            # A retry must not decide absence while its row is still queued.
            self._flush_data_manager_if_supported()
        local_event_durable = bool(
            (
                (central_inherit_all and package_logistics)
                or transition is not None
            )
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
    if (
        central_inherit_all
        and package_logistics
        and package_logistics.get("status") != label_transition.LOCAL_ONLY_STATUS
    ):
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
        *_TRANSITION_KEYS,
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
    failure_adapter=None,
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
        # A saved local decision stays with the live set for the retry.
        live = self.current_set_info or {}
        if (
            str(live.get("id") or "") == str(current_snapshot.get("id") or "")
            and tuple(live.get("raw") or ()) == tuple(current_snapshot.get("raw") or ())
        ):
            if "transition_row_key" in current_snapshot:
                live["transition_row_key"] = current_snapshot["transition_row_key"]
            if current_snapshot.get("transition_class") in label_transition.LOCAL_CLASSES:
                for key in _TRANSITION_KEYS:
                    if key in current_snapshot:
                        live[key] = deepcopy(current_snapshot[key])
                if "deferred_intent_id" not in current_snapshot:
                    live.pop("deferred_intent_id", None)
        self._publish_durable_commit_block(error)

    admission = self._submit_ui_lane_task(
        name="f3-package-completion",
        busy_text="포장 완료 · 권한 확인 및 로컬 완료 저장 중",
        work=work,
        finish=finish,
        fail=fail,
        failure_adapter=failure_adapter,
    )
    return bool(admission is not None and admission.accepted)
