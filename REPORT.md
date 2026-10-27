REJECT · P1 0 · P2 2 · P3 3
모델: 세션 규칙상 저장소 글에 모델 식별자 미기재 — 이 커밋 Co-Authored-By 참고

범위 c02e29d9..47326181 diff 와 그 diff 가 부르는 파일만 읽음. 시험·빌드·설치기·앱 실행 0.

## 맞는 것
- affb40d: guard 는 (file,function,sha256)+count 일치만 허용. b589… = `[IO.File]::Move((Get-LabelRemovalIoPath $moved),$io)` 직접 계산 일치, count=1, 다른 Move/Replace 안 풀림. 기존 10 레코드 값 불변(CRLF→LF 만).
- aa4cd75: 총예산 entry+1140(부모 있으면 min(·,부모−10)) 그대로, 넘으면 첫 검사에서 20. relay 몫은 첫 launch/readback 때 한 번만 시작, min(총예산, 시작+600). SYSTEM 결정 대기는 몫을 안 먹음. 값 늘림·이중 계산·끝없는 대기 0.
- 2413386: ROLLBACK_PENDING 의 ValueError 만 UPDATE_PHASE handoff(처리 0). busy 두 증명·writer 소비자 거절 그대로. 되돌림 중 새 앱·적용 끼어듦 0.
- 47326181: writer_session_fence 72ce…·label_writer_fence fde7…·auto_update df73…·probe 70f7… 모두 현재 바이트와 일치. 인벤토리 핀 53c1… 은 스캐너 실행 금지로 재계산 못 함(두 파일 값 일치만 확인).
- 시험 약화 0: 복구 뒤 두 번째 호출 hot 적용 단언(x7·deferred_recovery·system_restart[APR]), 손상 서명 failed_shas==[], tray 감사 정확히 1 유지.

## P2
1. tests/test_bootstrap_fallback_recovery.py:29 · tests/test_first_setup_owner_readback_mutex.py:460·1562·1640 — aa4cd75 가 `_relay_preparation_deadline(..., defer_preparation=True)` 로 부르는데 대역 `lambda *a`·`make_deadline(*args)` 는 키워드를 못 받음 → bootstrap 이 TypeError(안 잡힘) → 이 relay·bootstrap 보호 시험 실패. 같은 원인으로 tests/test_first_setup_wait_and_single_read.py:131 `"independent = entry + RELAY_PREPARE_SECONDS"` AST 단언도 실패(ast.unparse 로 확인).
2. tests/test_update_lm113.py:129 — `from user_relay import StartupStopRequested` 인데 user_relay 는 함수 안(880·1445)에서만 import → relay 매개변수 ImportError → 「stop 표지 우선」 단언이 실행 안 됨. startup_timing 에서 가져와야 함.

## P3
1. tests/test_auto_update_apply_claim.py:300 — `crash_phase == 'DEFERRED'` 는 매개변수(268)에 없는 죽은 단언. 같은 시험 [PREPARED] 는 복구 첫 호출 분리가 없어, x7 처럼 첫 호출이 ROLLED_BACK 에서 끝나면 PREPARED 기록 0 → SystemExit 안 남 → 실패 예상(실행 금지로 미확정).
2. tools/refresh_atomic_replace_declarations.py:50 · tools/_atomic_restore_catalog.ps1:23 — 재생성이 파일 전체를 LF 로 다시 쓰고 새 레코드 키 순서가 hashtable 순서라 실행마다 바뀜(47326181 의 순서만 바뀐 diff) → 선언 churn. 새 레코드만 reason 없음.
3. auto_update.py:3747·4405 — 자기 trial 을 띄우는 순간 relay 몫이 시작돼 그 뒤 READY 대기(창 900)는 launch+600 에 20 으로 끝남. 이전(entry+600)보다 나빠지지 않은 남은 한계.
