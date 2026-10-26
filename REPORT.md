REJECT · P1 0 · P2 1 · P3 3

# 독립 후속 검토 calm-auditloss2-lm 907ebd01..13f64750
읽기만(시험·빌드 실행 0). 범위: 1 커밋·11 파일과 그 호출처.

## P2
1. 앱 종료 close 의미가 바뀜 — `Label_Match.py:9322-9328`
   교체 함수를 설정(`:20524`)만이 아니라 앱 종료(`:9174`)도 부름. 전: 종료 close 10 초 초과·기록기 살아 있음 → False → fail-closed 보류(`:9186`, 입력 막힘·1 초 재시도, 잠김이 풀린 뒤 「종료 보류」). 후: 10 초 뒤 바로 close 취소 → 입력 다시 열고 「종료 보류」 대화(`:9229`)로 재개. 멈춤 요청 길은 500 ms 폴링(`:6927`)이 다시 `on_closing`(`:6909`) → 잠김 내내 10 초 Tk 멈춤 + 대화 + APP_CLOSE/APP_CLOSE_CANCELLED 쌍이 되풀이(`_has_background_work` `:13323` 은 retry 를 안 봄). 행 유실·중복은 0. 종료 경로 시험 없음(ui_lane 시험은 이 함수를 stub). 고침 방향: 취소는 설정 except 에서만.

## P3
1. FIFO 가 파일 경계를 넘음 — `label_data_manager.py:214-239,312-315`
   일반 행 하나가 계속 실패하면 뒤 일반 행이 모두 메모리에 묶임. 예: LOCAL_ONLY 행(형제 폴더 `storage_policy.py:16`, `:191-197`)의 CSV 가 Excel 에 열려 있으면 멀쩡한 계약 CSV 로 갈 ERROR_INPUT 등도 안 써지고 강제 종료 때 잃음. 앞(회전)은 써졌음. 파일별 FIFO 면 피함.
2. 재시도 중 저장·복사 toast 가 안 보임 — `Label_Match.py:7158-7169`
   pending 이면 넘긴 글을 버리고 안내로 바꾸며, 5 초 tick 이 떠 있는 toast 도 덮음. 「서로 안 지움」 중 안내→toast 쪽 불충족. 시험이 이를 기대값으로 고정(`tests/test_general_event_retry_tk.py:26-28,47-50`). 충돌 등은 따로 알림이 있어 P3.
3. 좁은 경합 — `Label_Match.py:9325-9328`
   `is_alive()` 참 직후 기록기가 토큰을 받아 `_writer_stopping=True`(`label_data_manager.py:229`) 면 취소 False·교체 없음 → 설정 길에 닫힌 관리자가 남음(앞 P1 증상). 10 초 경계에 잠김이 풀리는 순간만. 취소 False 면 join 뒤 교체로 넘기면 닫힘.

## 확인(문제 없음)
1. P1(설정): 시간 초과 → `_cancel_close_request` 가 `_close_lock` 아래 `_close_requested=False`·토큰 None. 묵은 토큰은 `is not` 로 버려져 뒤 close 를 못 멈춤. 기록기 계속 → 남은 retry 행 한 번만 기록, 중복 0. 작업자 이름 되돌림.
2. 작업 계수: 묶인 행은 got_item=False 로 task 를 계속 갖고 처리 때 한 번 task_done. 묶인 행도 `_log_retry_items` 에 있어 flush 경계(`unfinished<=len`) 맞음. `_log_retry_errors` 기준으로 바꿔 묶였던 행의 첫 실패도 오류 1 건·성공 때 회복.
3. P2(Tk): `save_status_label` 은 `operator_status_frame` 에 grid, 숨김 경로 0. 5 초 tick 이 안내 표시, retry 끝나면 글이 안내일 때만 지움. toast 타이머는 pending 중 안내를 안 지움.
4. P3-1/2: 일반 행 A,B,C 순서 유지. 실패 때 큐 스냅샷만큼 durable/audit 을 먼저 쓰고 sleep 은 회차당 한 번(최대 1 초). close 토큰을 회차 안에서 되넣어도 바쁜 반복 없음.
5. P3-3: dialogs 를 안내 앞에 둠 → band+stopped+대화 2 면 안내만 잘림(Tk 에는 보임).
6. 시험 입력: failing_open 은 OSError 라 TEST_EVENT 는 이제 retry 가 맞음. APP_CLOSE 는 durable → flush·close 의 같은 RuntimeError 단언 유지, sticky 약화 0. TRAY_COMPLETE·scanner-audit 은 `test_general_event_retry.py:261` 이 따로 지킴.
7. 핀 전파뿐: INSTALL 차이 = 바뀐 5 파일 옛 LF sha256 → 새 값 1:1 치환(개수 39/29/23/37/34 같음, 옛 값 잔존 0) + probe 자기 해시(재계산 일치). inventory 핀 두 곳 같은 값(재계산은 실행이라 안 함). 문서 2 곳은 설명 갱신.
