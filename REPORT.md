ACCEPT · P1 0 · P2 0 · P3 5
Claude Opus 5.5

범위: f9715a0b..07c7f2be(1커밋) diff 와 그 diff 가 부르는 함수만 읽음. 시험·실행 0.

## 확인(문제 없음)
1. `_startup_gate`(auto_update.py:3495)·busy 출구(3363)·CLI 게이트(3195)·register 재확인(3337)이 모두 `_accept_startup_proof` 호출. 옛 trial/defer 분기와 조건·순서 같음(active SHA·claim nonce/transition/PID·defer phase 집합·assert_ordinary_recovery), operator 검사만 하나 더함.
2. 증명은 모듈 전역(PID·app 경로·policy/active/transition+env 해시)에만 있고 파일·env 로 안 나감. register 때 PID·`__file__` 경로·해시 비교 뒤 같은 함수로 재검사(3328-3338).
3. startup_proven 은 register+startup+비정상 phase 일 때만(1095). install/inspect/제거·`_verify_abandoned_update`(3245)·relay 재개(current_user_onboarding.py:141)는 그대로. 증명 없는 PREPARED 는 1099 에서 거절. 설치 위임(3184)·지원 제거(3179, 3461) 그대로. transition/active 쓰기 0.
4. 증명 함수는 읽기만. 실패는 기존 거절·UNKNOWN 대체 길(writer_session_fence.py:1410-1433)로 감.
5. busy 출구의 assert_ordinary_recovery(allow_relay_parent=role=="relay")(3360)·assert_version_writer(3362) 그대로, 새 호출도 같은 값. launch_version 의 RELAY_PARENT 제거(3539) 그대로.
6. 기존 시험은 주석 3줄만 바뀜(단언·기대 변경 0). 설치기 고정값 4종(auto_update·writer_session_fence·label_writer_fence·probe)은 새 파일 해시와 일치. WRITER_INVENTORY_SHA256 은 스캐너 실행 없이 확인 못 함.

## P3
1. auto_update.py:3187-3197 — CLI 증명은 시작 관측과 따로 묶이지 않고 그 자리에서 env 로 새로 만듦: DEFERRED_AT 을 물려받은(또는 직접 넣은) 같은 사용자 프로세스가 `main.py --onboard-current-user`(`--restore-current-user-lifecycle-after-replacement` 도)로 PREPARED/FAILED/UNKNOWN 등에서 register 통과. 범위 안엔 그런 실행자 없음(부트스트랩은 onboard 인자를 안 받음) → 실제 호출자 근거나 launch_version 이 넘긴 값에만 묶는 표식 필요.
2. auto_update.py:1102 — 증명이 있으면 recovery_sha 일관성 검사를 phase 와 상관없이 생략: PREPARED 인데 pointer 가 이미 candidate 인 상태도 register 통과. UNKNOWN/FAILED 말고는 검사 유지 가능. 시험(test_first_setup_startup_proof.py:115)은 candidate==previous 라 차이를 못 봄.
3. auto_update.py:3363 — busy 출구의 새 호출은 결과를 안 쓰면서 assert_ordinary_recovery·trial.json 읽기를 다시 하고 예외를 그대로 올림 → 두 관측 사이 writer·세대 값이 바뀌면 전엔 통과하던 시작이 예외 종료(드묾). 기억용이면 예외를 삼켜야 함.
4. auto_update.py:3190 — CLI 게이트가 update mutex 를 새로 잡음: 버려진 mutex+검증 실패면 update_lock 이 mutex 를 풀어 신호를 소비하고, UPDATE_MUTEX_ABANDONED_RECOVERY_REQUIRED 기록·'아무것도 지우지 말고' 안내 대신 '지원 제거 후 재설치' 안내만 냄(env 있을 때만).
5. 시험 — 새 시험은 Label_Match.main→startup_gate→첫 설정 진입(prepare_current_user_data_open 직전)까지 실제로 지나지만 startup_update_lock·process_snapshot·세대·window-first 를 바꿔 둠. 거절 시험(…:104-108)은 0/20/예외 모두 통과라 거절 이유가 고정 안 됨. busy 출구 증명·relay 부모·다른 PID/모듈·버려진 mutex 시험 없음. 판정 사본(3358, writer_session_fence.py:1426-1428, auto_update.py:2642-2647)은 따로 남음.
