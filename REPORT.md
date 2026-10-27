REJECT · P1 0 · P2 1 · P3 3
검토 모델: 세션 규칙상 저장소에 모델 식별자 생략 — 세션 답에 적음

범위 0c90f30e..f9715a0b: merge 13개 충돌 풀이(--remerge-diff)·b4aa7022·d98c9921·0f58f1f5·240326ab·22192170 과 그 diff 가 부르는 코드만 읽음. 시험·빌드·설치기·앱 실행 0. 웹 저장소는 범위 밖.

## P2
1. tools/direct_sync_relay_runner.py:580·938 (43632b8 풀이) — 배정 쥔 행 복구의 원본 읽기를 source_file_operation() 으로 감쌈(:616-624·655). 원본 잠김·없음(EACCES·EBUSY·ENOENT)이 이제 OSError 아닌 SourceFileInputError 로 나와 :580 `except OSError: continue` 도, 단건 CLI :938 `except (OSError, sqlite3.DatabaseError, ValueError)` 도 못 잡음 → 본사 `--enqueue-source-file` 에서 원본이 잠긴(엑셀 열림 등)·없는 배정 행이면 traceback·rc 1, runtime status·event 0 (pclock 단독은 그 행을 건너뛰고 계속). 트랜잭션은 되돌려져 배정은 보존. 스캔 경로는 :984 가 잡아 그 파일만 격리. 고침: :580 을 (OSError, SourceFileInputError) 로, 또는 :938 에서 SourceFileInputError 를 기존 enqueue_error·고정 코드로.

## P3
1. Label_Match.py:1367 (43632b8 풀이) — 어느 재료에도 없던 글 바꿈 「그 뒤 기록 N건」→「기록 N건」, contracts.md:23·operations.md:34 는 옛 글. 같은 풀이에서 tests/test_stale_timestamp_retry.py:692·696 을 '기록 7건'·'기록을' 로 느슨하게 함(두 글 다 통과 → 글 고정 0).
2. direct_sync_runtime.py:1017 (풀이) — 맨 OSError 코드 direct_sync_source_file_error→direct_sync_enqueue_error. 전역 처리는 맞으나 단건 CLI error_code 가 바뀜(status·event 이름은 같음). runner :1020-1031 scan_enqueue_errors·rc 2 길은 맨 UnicodeError 만 남아 사실상 죽음, operations.md:30 「원본 하나의 일반 enqueue 실패는 파일별 목록…종료 코드 2」 는 현재와 다름.
3. 화면 바이트·글 — frame.css:627 은 ui1028(웹 00e66d79·97c47d9b…)이 아니라 base 7297e63(큰 글에서 메뉴 보임) 자동 병합본(66258f79…), lm.js:10 duplicate(pgsrs) 도 ui1028 밖. 동작은 맞으나 BACKLOG.md:879 「frame.css 만 웹 00e66d79 바이트」, README.md:14(6ced39fb·76파일·3583f1d2 → 지금 d8f82fa4·91파일·bab78bd8), operations.md:1849(scan.js 블롭 d390424b → 지금 474b279c) 가 옛 값.

## 확인(문제 0)
- auto_update.py:2119-2140·4059-4073: retry_shas 소비·normal_stop_started_at 둘 다 남음, 150 s 기한은 원 시각 기준, READY 900·상한 1140 그대로.
- runner: 파일 오류 격리·drain 계속, `except Exception` 제거로 DB·공용 부모 오류 전역, 복사 실패는 ENOSPC·EIO·DB·부모면 전역·그 밖은 그 원본만(되돌림으로 원 배정 보존), 전역 enqueue_error 는 drain 멈춤(base 와 같음).
- _write_runtime_status·_persist: 파일 오류 목록·clock_error·upload_block 는 다른 키 — 지움 0.
- _alerts: 1+1+대화 ≤2+quiet 1=5, 넘침 0. 파일 꼬리는 서버 줄(:804), Tk 줄도 셋 함께.
- __init__·process_input·_load_items_data·DataManager: 재료 합집합, 겹침 0.
- 240326ab·22192170: 백그라운드는 큐만·Tk 0, 주 스레드의 보이는 주인 창(웹은 띠), 세 안내 모두 멈춤 감시, destroy 는 finally, 시작 중(입력 꺼짐)에만.
- 하한 v2.0.126: 최신 표지 하한과 비교 → .120/.112 표지는 .126 허용, .126 표지는 옛 판 거절.
- 시험: 0f58f1f 는 시계 분리만(270/480 칸→실제 상한 1140 칸), 풀이 시험은 P3-1 외 약화 0.
- 선언: 원본 있는 after 핀 4655개 = 머리 release_digest(독립 계산), inventory 핀 py·ps1 같음(tests 는 밖). 생성기 --check 안 돌림. MANIFEST 91 파일 일치, contract.json 은 maxItems 4→5 만. 웹 98903b6d 바이트 대조는 범위 밖.
