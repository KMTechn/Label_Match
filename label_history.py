"""Read complete history snapshots and calculate their summary rows.

The application owns load generations, active input gates and all Tk updates.
Clock, I/O and barcode interpretation are supplied by its facade callbacks.
"""

from collections import defaultdict
import csv
import json


def _label_match_summary_date(details):
    source = details or {}
    return str(
        source.get("production_date")
        or source.get("packaging_completed_date")
        or ""
    ).strip()


def _async_load_history_task(
    self, result_queue, target_date=None, updates_active_state=None,
    load_generation=None, *, open_file, clock, tray_complete_result,
    tray_complete_passed, summary_date, duplicate_index_barcodes,
):
    log_filepath = None
    reader = None
    damaged_rows = 0
    row_errors = []
    try:
        if updates_active_state is None:
            updates_active_state = self._history_load_updates_active_state(target_date)
        completed_sets = {}
        voided_set_ids = set()
        cancelled_set_ids = set()

        log_filepath = self.data_manager._get_log_filepath(target_date)

        try:
            with open_file(log_filepath, 'r', encoding='utf-8-sig', newline='') as f:
                reader = csv.DictReader(f, strict=True)
                if not {'timestamp', 'event', 'details'}.issubset(reader.fieldnames or ()):
                    raise ValueError("기록 파일 머리글이 올바르지 않습니다")
                for row in reader:
                    line_number = reader.line_num
                    try:
                        event = row.get('event')
                        details = json.loads(row.get('details') or '')
                        if not isinstance(details, dict):
                            raise ValueError("details must be an object")
                        timestamp_str = clock().fromisoformat(row.get('timestamp', '')).strftime('%H:%M:%S')

                        set_id = details.get('set_id')
                        if event == self.Events.SET_DELETED and set_id:
                            voided_set_ids.add(set_id)
                            continue
                        if event == self.Events.TRAY_COMPLETION_CANCELLED and details.get('cancelled_set_id'):
                            cancelled_set_ids.add(details['cancelled_set_id'])
                            continue

                        if set_id is None: continue

                        if event == self.Events.TRAY_COMPLETE:
                            displays = details.get('parsed_product_barcodes', [])
                            first_scan = displays[0] if displays else "N/A"
                            other_scans = displays[1:self.TOTAL_SCAN_COUNT]

                            result_display = tray_complete_result(details)
                            values_to_display = (
                                set_id,
                                first_scan,
                                *other_scans + [""] * ((self.TOTAL_SCAN_COUNT - 1) - len(other_scans)),
                                result_display,
                                timestamp_str,
                            )

                            completed_sets[set_id] = {'values': values_to_display, 'tags': ("success" if tray_complete_passed(details) else "error",), 'details': details}
                    except (ValueError, TypeError, AttributeError, KeyError) as exc:
                        damaged_rows += 1
                        if len(row_errors) < 20:
                            row_errors.append(f"행 {line_number}: timestamp/details 형식 오류 ({type(exc).__name__})")

        except FileNotFoundError:
            # A day with no log has no completed sets; other read failures
            # are errors, never a confirmed empty history.
            if reader is not None:
                raise
        if damaged_rows:
            raise ValueError("\n".join(row_errors))

        final_sets = {sid: data for sid, data in completed_sets.items() if sid not in voided_set_ids and sid not in cancelled_set_ids}
        def _history_sort_key(item):
            set_id, data = item
            details = data.get('details', {}) if isinstance(data, dict) else {}
            return (
                details.get('end_time')
                or details.get('timestamp')
                or details.get('start_time')
                or str(set_id)
            )

        sorted_final_sets = sorted(final_sets.items(), key=_history_sort_key)
        temp_scan_count = defaultdict(lambda: defaultdict(int))
        temp_global_scanned_set = set()
        temp_set_details_map = {sid: data['details'] for sid, data in final_sets.items()}
        for set_id, data in sorted_final_sets:
            details = data['details']
            if tray_complete_passed(details):
                passed_code = details.get('item_code')
                production_date = summary_date(details)
                phase = details.get('phase') or '-'
                if passed_code and production_date:
                    temp_scan_count[production_date][(passed_code, phase)] += 1
                temp_global_scanned_set.update(duplicate_index_barcodes(details))

        result_queue.put({
            'sorted_sets': sorted_final_sets,
            'scan_count': temp_scan_count,
            'summary_items': self._summary_items(temp_scan_count),
            'global_scanned_set': temp_global_scanned_set,
            'set_details_map': temp_set_details_map,
            'updates_active_state': updates_active_state,
            'load_generation': load_generation,
        })
    except Exception as e:
        error = (
            f"기록 조회 불완전 · 손상 행 {damaged_rows}개"
            if damaged_rows else "기록 파일을 읽지 못했습니다"
        )
        location = f"파일: {log_filepath}\n행: {getattr(reader, 'line_num', 0)}"
        detail = "\n".join(row_errors) if row_errors else type(e).__name__
        result_queue.put({
            'error': error,
            'error_detail': f"{location}\n손상 행: {damaged_rows}개 (위치 최대 20개)\n{detail}",
            'damaged_rows': damaged_rows,
            'updates_active_state': updates_active_state,
            'load_generation': load_generation,
        })


def _summary_items(scan_count, *, clock):
    summary_counts = defaultdict(int)
    for date_str, items in (scan_count or {}).items():
        try:
            clock().strptime(str(date_str), '%Y-%m-%d')
        except (ValueError, TypeError) as e:
            print(f"요약 트리 업데이트 중 날짜 형식 오류: {date_str}, 오류: {e}")
            continue
        for (code, phase), count in (items or {}).items():
            if count > 0:
                summary_counts[(code, phase or "-")] += count
    return sorted(summary_counts.items(), key=lambda item: (-item[1], str(item[0][0]), str(item[0][1])))
