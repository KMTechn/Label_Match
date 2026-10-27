REJECT · P1 0 · P2 2 · P3 4
검토 모델: 세션 규칙상 저장소에 모델 식별자 생략 — 세션 답에 적음

범위: 세 diff 와 그 diff 가 부르는 코드만 읽음. 시험·빌드·설치기·앱 실행 0. 지문은 git 블롭으로 따로 계산.

## (가) f9715a0b..0a6db149
확인: runner:580 단건은 OSError·SourceFileInputError 를 올려 트랜잭션 되돌림 → :945 → :768 runtime_error·enqueue_runtime_error·rc1·drain 0. 스캔은 그 행만 continue, 공용 부모·DB·ENOSPC·EIO·역압은 전과 같음. 시험 :692·696 은 Label_Match.py:1367 글 전체를 고정. frame.css 66258f79·d8f82fa4/91/bab78bd8·scan.js 474b279c 는 머리와 같음, 화면 바이트 0.
P3-1 runner:777 — 단건의 모든 OSError(배정 행 spool 복사 ENOSPC·EIO, 상태 쓰기)를 direct_sync_enqueue_error 로 덮음(문서는 원본 복구만). operations.md:30·contracts.md:12 「일반 OSError→direct_sync_enqueue_error」 는 enqueue 단계만 맞고, 스캔 중 원본 읽기의 미분류 OSError 는 :1161 scan_runtime_error·코드=클래스 이름(rc1 은 맞음).
P3-2 tests/test_held_source_input_boundary.py:58·80 — calls_before 가 같은 list 라 :80 은 늘 참(drain 0 은 no_drain 이 지킴). oserror 의 스캔 행 건너뛰기 시험 없음.

## (나) 07c7f2be..004d1d1e
확인: auto_update.py:1104 건너뛰기 = 시작 증명+UNKNOWN/FAILED 만. PREPARED·candidate≠previous·pointer=candidate 는 :525 ValueError. :3202 가 UpdateMutexRecoveryRequired 를 다시 올려 :3353 → 「아무것도 지우지 말고」·20; 호출자 넷 모두 startup_gate 경유, abandoned_check 가 있어 맨 RuntimeError 길 0. 004d1d1e 는 SELECT 두 이름만.
P3-3 tests/test_first_setup_startup_proof.py:105·108 — rc·예외 종류는 장면별 고정, CLI 20 의 code·ValueError 문구는 안 봄. :192 는 APPLIED_PENDING_READY·ROLLBACK_PENDING_READY 의 어긋난 pointer 거절 장면 없음.

## (다) f9715a0b..3cc990e1
확인: 다섯 자리 한 몸통(runtime:110), 기존 mutex·중첩 규칙, 기록만 0.5 s, 줄 바이트·경로 같음, 잘린 끝 뒤 LF, OSError 는 호출자로. runtime·runner 는 writer_sink 안(대기 0).
P2-1 direct_sync_operator.py:762-764 — ack-reviewed --recover-expired-runtime 은 「감사 실패면 ACK·재개 되돌림」(감사 경로 필수 :585). 이제 허가 거절(relay 가 mutex 를 0.5 s 넘게 쥠·배포 fence)은 runtime:128 이 조용히 생략 → 옛 grant 증거 없이 ACK·runtime 재개 커밋.
P2-2 runtime:128 — 비중첩 관리자 도구에서 writer_session_fence.py:1277 의 ValueError(auto_update.py:2625·2628·2633·2651: 묶음 runtime 밖·승격·다른 계정·미정착 transition)를 못 잡음. retry-dead·ack-reviewed·pause·resume·restore-spool 은 커밋 뒤 기록에서 터져 tools/direct_sync_relay_operator.py:177 이 FAIL·rc1(실제는 바뀜, 재실행은 BLOCKED), 감사 줄·셈 0.
P3-4 INSTALL_CANONICAL_PORTABLE.ps1:7447 — 새 source relay_runtime_storage 가 위임 목록에 없어 설치기 위임 relay 동안 JSONL 줄 전부 생략(writer_session_fence.py:1205).

## 지문(세 머리)
WRITER_INVENTORY_SHA256 py=ps1=인벤토리 재계산, probe sha=핀, after 핀 4486·4486·4489 = 머리 release_digest, startup_candidate_membership = 파생(다는 _append_jsonl 넣어 108), host 새 쌍 앞값 b81478 = stock 104~106.

## 공통
runner 는 (가):577-946·(다):801-838 로 안 겹치고 뜻 충돌 0. 세 diff 모두 writer_session_fence.py:47·label_writer_fence.ps1:9·INSTALL 핀·probe sha 를 제 값으로 바꿈 → 합칠 때 어느 쪽을 골라도 낡은 지문, 합친 뒤 생성기로 다시 내고 재계산 확인 필요. 시험 약화 0.
