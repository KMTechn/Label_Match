REJECT · P1 1 · P2 1 · P3 3

# 독립 검토 calm-auditloss-lm 256ca67b..907ebd01
읽기만(시험·빌드 실행 0). 범위: 2 커밋·9 파일과 그 호출처.

## P1
1. 설정 변경이 닫힌 DataManager 를 남김 — `Label_Match.py:20505-20509`
   retry 만 남으면 flush 가 성공해 `close(10)` 에 닿음. 잠김이 10 초 넘으면 TimeoutError → 교체 함수는 스레드가 살아 있어 False(`:9312`) → `_close_requested=True` 관리자가 그대로 남아 이후 모든 `log_event` 가 `DataManager is closing`(`label_data_manager.py:477-479`). 잠김이 풀려도 교체 안 됨 → TRAY_COMPLETE·감사 append 가 설정 재시도/재시작 전까지 거부. 앞 코드는 flush 가 먼저 실패해 관리자가 열린 채 남았으므로 이 diff 의 회귀. 앱 종료(`:9147`)도 잠김 내내 fail-closed 보류. 시험 없음.

## P2
1. Tk 안내가 화면에 안 보임 — `Label_Match.py:7143-7154`
   안내를 `relay_screen_status_label` 에 쓰지만 `_create_widgets` 가 이 라벨을 무조건 `grid_remove()`(`:23732`), 기존 시험도 unmapped 단언(`tests/test_label_operator_workbench.py:858,889`). 새 시험(`test_general_event_retry.py:155`)은 가짜 라벨이라 못 잡음. 웹 quiet 띠는 보임.

## P3
1. 순서: 재시도 행을 큐 끝에 다시 넣어(`label_data_manager.py:281`) 여러 행이 도는 중 풀리면 CSV 순서가 회전(C,A,B)하고 뒤에 온 TRAY_COMPLETE 가 앞선 일반 행보다 먼저 써질 수 있음. 중복 0·원문 시각 유지. 읽는 쪽 순서 의존은 범위 밖이라 미확인.
2. 상한 없음: 영구 실패 일반 행 K 개면 한 바퀴마다 K 초 sleep(`:296-297`)이 뒤 durable·감사 행을 늦춤(예: LOCAL_ONLY 경로만 실패). 감사 replay 의 flush(5)는 보류로 끝남(유실 0). 메모리도 계속 쌓임.
3. 웹 `_alerts[:4]`(`label_screen_bridge.py:824-827`): 안내가 dialogs 앞이라 band+stopped+안내일 때 DIALOG_LIMIT=2 중 둘째 대화가 잘림.

## 확인(문제 없음)
1. 중복 0: retry 는 OSError·`append_started` 전·일반 행만. 주입 open 은 plain `open`(`Label_Match.py:4560`)이라 open('a')/stat/makedirs 는 바이트 0. 이전 감사 핸들 close·쓴 뒤 close 실패는 재시도 안 함, 기존 오류 유지·재append 0. 같은 list 재put 이라 원문 동일.
2. flush 계수: remember→put→task_done 순서라 거짓 break 없음. sentinel 은 retry 남으면 다시 넣어 앞지르지 않음, close 는 join(timeout) 준수.
3. durable 집합(`Label_Match.py:353`) 무변경. audit_key 행은 retry 제외, 오류·키 기록 동일. 감사 행이 남으면 flush 는 계속 기다림. WriterFenceError 는 RuntimeError 라 retry 대상 아님.
4. 비차단: 소리·입력 막기 0. 웹은 5 초 relay 갱신→footer→push 로 저장 뒤 사라짐.
5. 핀 전파뿐: INSTALL 39 줄 = 바뀐 5 파일 새 digest(LF sha256 일치) + 최근 3 릴리스에 label_data_manager 항목 추가 + probe 자기 해시(재계산 일치). 옛 digest 잔존 0. inventory 핀 두 곳 같은 값(재계산은 실행이라 안 함).
