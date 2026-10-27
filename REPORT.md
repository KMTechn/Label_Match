REJECT · P1 0 · P2 1 · P3 1
모델: Claude — 세션 규칙상 정확한 모델 식별자는 저장소 글에 안 씀(이 커밋 Co-Authored-By 줄 참고)

범위 47326181..dc25e3f3 diff 와 그 diff 가 부르는 파일만 읽음(P2-1 확인용으로 같은 함수 대역만 grep). 시험·빌드·설치기·앱 실행 0.

## 맞는 것
- 제품 파일 바뀜 0(tests/·tools/ 만). 카탈로그 = c02e29d9 의 10 레코드 바이트 그대로 + Restore-RemovalFilePreimage 1개(b589…·reason·키 순서 고정), CRLF 복원, 도구 출력과 바이트 같음 → 앞 P3-2 풀림.
- 앞 P2-2 풀림: test_update_lm113.py:129 가 startup_timing 에서 import.
- 앞 P3-1 풀림: 죽은 DEFERRED 분기 삭제, 복구 첫 호출을 두 매개변수 모두 분리. PREPARED 기대 [hot]→[] 는 제품(auto_update.py:2135 transient=True)과 맞는 정정이고, retry_shas 를 {hot:{attempts 1, first_at==last_at}} 정확 일치·재개 호출 시각 안·그 호출이 APPLIED_PENDING_READY 를 씀까지 더해 약화 아님.
- AST: independent 를 `entry + (CAP if defer_preparation else 600)` 로 정확 일치, 600·1140 고정, 실제 begin_preparation 이 min(절대, 시작+600)·한 번만·경계 1초 앞 ""/경계 NO_PROGRESS 를 대기 100·900 두 갈래로 확인. 예전 부분 문자열보다 엄격.
- E7: 첫 bootstrap 에서 primary(nonce·writer)·ready.json(sha·실제 pid·role·nonce·역할별 checks 전부 True)·SYSTEM 뒤 CONFIRMED·자식 종료 뒤 process_snapshot==[]·ALLOW 확인. 다음 bootstrap 은 assert_ordinary 원 단언(후보 SHA·gate None·ALLOW·ordinary True·writer) 그대로. 대역 4곳은 defer_preparation=True 를 단언(460 은 그대로 넘김).

## P2
1. tests/test_live_startup_stop.py:188 `lambda *_` · tests/test_relay_review_fixes.py:232 `lambda *a` — 앞 P2-1 과 같은 대역이 남음. bootstrap → _bootstrap 이 auto_update.py:4197 에서 defer_preparation=True 로 부름 → TypeError(bootstrap 이 못 잡음) → relay stop 의 LIVE/UNKNOWN 대기·rollback rc 보호 시험 실패. P2-1 일부만 풀림.

## P3
1. tests/_retained_endpoint_fixture.py:67·70 · tests/test_auto_update_endpoints.py:185·270·271 — select_primary 가 역할이 같은 자식만 골라 공유 observation 을 덮어씀 → 마지막 `relay == (role == "relay")`·`nonce_present is False` 는 늘 참(역할이 틀리면 "did not publish" 로만 걸림). 다음 bootstrap 이 다른 역할 non-peer 자식을 더 띄워도 안 보임. children[start:] 의 non-peer 관찰이 정확히 1개이고 역할이 맞음을 단언하면 됨.

## 범위 밖
- 앞 P3-3(auto_update.py:3747·4405)은 제품을 안 바꿔 그대로.
