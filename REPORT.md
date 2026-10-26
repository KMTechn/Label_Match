REJECT · P1 1 · P2 0 · P3 3

범위 7ec0c770..9e938325 와 그 diff 가 부르는 코드만 읽음. 시험·빌드 실행 0.

## P1 — 배정을 보존한 terminal 행이 권한 scope 를 영구히 잡아 24 h gate 로 이어짐
- 길: ACK 유실 행(앞선 배정 있음)이 spool 없음/그 밖 OSError 로 끝나면 `failed_permanent` + `preserve` 로 닫힌다(`direct_sync_push.py:2376-2380`, `2687`, `2981-2985`). authority 는 ACTIVE 이고 `assigned_relay_id` 가 이 행을 계속 가리킨다. terminal 행은 다시 claim 되지 않으므로 권한을 풀 길이 없다.
- 결과: 그 뒤의 모든 행(다른 원본 포함)은 `runtime_request_in_flight` 로 retry_wait 가 된다(`producer_runtime_client.py:864-871`). 24 h 가 지나면 `oldest_active` 가 gate 를 넘고(`direct_sync_runtime.py:557-575`), 회복 drain 도 retry_wait 라 멈춘다(`:885`). 결국 새 enqueue 전체가 막힌다. observe·enforce 모두 같고, 새 시험의 `following.retryable is True`·`source_calls==1` 이 이 상태를 고정한다. 띠에는 「막힌 파일 1개」만 보인다.
- 어긋남: 새 contracts.md 문장 「24 h gate 는 소실 행 때문에 다른 새 enqueue 를 막지 않는다」 및 요청 기준과 맞지 않는다. 24 h 시험은 runtime lease 를 격리해서 이 경우를 다루지 않는다.
- 퇴행 여부: base 에서도 같은 행이 retry_wait 로 남아 막았다. 다만 그 밖 OSError(일시 자원 오류)는 재시도로 풀릴 수 있었는데, 이제는 출구가 0 이다. 또한 「배정된 행이 terminal 이 되면 authority 는 operator_review」(`:2392-2398`)라는 불변식을 처음으로 깬다. operator `retry-dead` 는 다섯 칸을 지우고 새 배정을 잡는데, 같은 relay 에 배정이 남아 있으면 `assigned_relay_id IS NULL` 조건(`producer_runtime_client.py:923`) 때문에 출구가 되지 못할 것으로 보인다(읽기 추정).
- 결정 필요: 현 권한 모델에서는 「배정 보존」과 「전체를 막는 길 0」을 함께 만족할 수 없다. 선택지는 (a) 보존 + 별도 권한 코드·띠·운영자 출구를 두고 contract 문장을 한정하는 것, (b) 서버 토큰 상태를 조회해 ACK 유실을 해소한 뒤 해제하는 것이다.

## 볼 것 결과
1. 중복·유실 ✔: preserve 경로는 토큰을 해제하거나 재사용하지 않고, 메타 다섯 칸도 그대로 둔다(시험 확인). release 는 배정이 없던 행, 즉 이번 시도의 토큰을 보내지 않은 행에만 일어난다. 유실은 같은 원본 뒤 구간이 막히는 것으로 드러난다.
2. 잠김→terminal 0 ✔: digest 와 본문 다시 열기 모두 `_spool_read_is_retryable`(PermissionError, winerror 5/32/33)로 가른다. 본문을 연 뒤나 POST 를 시작한 뒤의 OSError 는 transport 재시도로 남는다(`:1452-1455`).
3. 24 h·K/N·띠: 배정 없는 행 ✔(시험). 배정 보존 행 ✘(P1).
4. 같은 원본 앞 구간 ✔: scan 코드 변경 0. 같은 원본 delta 는 막히고 다른 원본은 enqueue 됨(시험).
5. 핀 전파 ✔: INSTALL 8057 줄 구조는 같고 바뀐 것은 64-hex 4쌍뿐이다. `direct_sync_push.py` ad19d442…, `writer_session_fence.py` 7e360f09…, `label_writer_fence.ps1` 3884eea9…, probe here-string d75318d3… 는 LF 정규화 sha256 로 다시 계산해 일치했다. inventory 핀 59976590… 은 py·ps1 이 같은 값이며, scanner 를 실행하지 않아 다시 계산하지 않았다.

## P3
- 그 밖 OSError=terminal 은 명세대로이다. 다만 EMFILE·winerror 1450 같은 일시 자원 오류도 spool 이 멀쩡한 행을 영구히 닫는다(자동 복구 0).
- winerror 5 는 영구 ACL 거부도 잠김으로 보아 retry 가 끝나지 않고 24 h gate 로 이어진다. 명세대로이며 기록만 한다.
- contracts.md 의 고친 문장에서 「본문을 연 뒤·before_post 의 로컬 OSError 재시도」가 빠졌다. 코드는 그대로이며 문서만의 문제이다.
