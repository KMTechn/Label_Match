ACCEPT · P1 0 · P2 0 · P3 3
모델: Claude — 세션 규칙상 정확한 식별자는 저장소 글에 안 씀(커밋 Co-Authored-By 줄 참고)

범위 = first-parent e88ebfc·9555adc·d292440 와 그 diff 가 부르는 파일만 읽음. 시험·빌드·설치기·앱 실행 0.

## 맞는 것
1. 9555adc: 모든 user relay 진입(HKCU Run·bootstrap·product host·fallback)이 user_relay.main → _run_user_relay:1553 한 곳을 지남. 소유=새로 만든 mutex 뿐이라 중복 relay 0 그대로. 진짜 holder 면 2 초 뒤 지금처럼 rc 0. 매 회 stop 표지를 먼저 봄(0.1 초 안 끝). 기다리는 동안 쥔 잠금·writer 허가 0 → 교착 0. 부모 대기(_read_started_relay·wait_current_relay)는 모두 자기 마감으로 묶여 +2 초가 600 초/240 초 예산을 못 넘김. 제거 길은 표지를 먼저 쓰고(request_user_relay_stop) 복구 _stopped 는 표지가 있을 때만 잡으니 다시 잡기가 곧 끝남.
2. e88ebfc: 실제 mutex 자식이 50 ms 쥔 채 relay 를 10 번 시작 → 읽어 보면 고치기 전엔 겹친 회차가 LEASE_UNAVAILABLE 로 빨강, 고친 뒤 0(시험 커밋이 고침보다 앞). 진짜 holder(≥2 초·rc 0)·stop 표지(<2 초) 도 확인. fix18 P2-1 풀림(:188·:232 가 defer_preparation=True 를 단언, 저장소의 대역 6 곳 모두 kwargs 받음). fix18 P3-1 풀림(_retained_endpoint_fixture.py:65·70 = 자식마다 관찰·pid 일치·non-peer 정확히 1 개·역할 일치, 단위 시험 추가). peer 표시는 gui 가 살아 있을 때 뜬 relay 에만 붙고 role=relay 에선 오히려 실패로 잡혀 약화 아님.
3. d292440: user_relay c7e6→d4c2, writer_session_fence a5a8→7c57, label_writer_fence ce74→3f3e 가 바뀐 바이트(LF)와 일치. 옛 값 남은 곳 0. writer inventory 다시 계산 = cd6e0c3c…(두 pin 일치). 설치기 probe 지문 15398d66… 다시 계산과 일치.

## P3
1. user_relay.py:1554-1565 — 다시 잡기 고리는 stop 표지만 보고 :1580 의 trial_stop_requested 는 안 봄 → 되돌림(ROLLBACK_PENDING 등) 중 시작한 relay 가 최대 2 초 늦게 끝남(잠금 안 쥠, 잡으면 고리 머리에서 바로 끝).
2. user_relay.py:907·975 × :1553 — update-control 없는 portable(비 bootstrap) 시작에서 다른 holder 가 있으면 자식이 시작 시간+2 초 살아 survival 2.0 초를 넘김 → ALIVE·곧 rc 0 으로 사라질 pid 반환. contracts.md:594 「새 child exit 0만으로 수용 안 함」과 어긋남. 정식 설치(bootstrap)는 wait_current_relay(pid) 가 그대로 잡음.
3. tests/test_relay_instance_startup_retry.py:90-106 — 겹침이 실제 일어났는지 단언 없음 → 고친 뒤 초록이 「다시 잡음」인지 「겹침 0」인지 구별 못 함. relay 쪽 _acquire_relay_lease None 횟수 ≥1/회 단언 권함.
