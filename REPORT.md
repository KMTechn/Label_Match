ACCEPT · P1 0 · P2 0 · P3 4

범위 e55a6285..b19038de(3 커밋)와 그 diff 가 부르는 코드만 읽음. 시험·빌드 실행 0.

## 볼 것 결과
1. 넘김·강제 종료 0 ✔: 넘기면 relay 정지 UNKNOWN, `_apply_prepared_once` UNKNOWN+20, 소유 재조회는 기존 UserRelayError, PS 는 같은 throw 이다. 더한 Kill·Stop-Process 0, guardian 연결 실패도 Dispose 만 한다. 자료 손 0.
2. 상수 결속 ✔: `RELAY_NORMAL_STOP_TIMEOUT_SECONDS`=자식 한도+30 을 정지 기본값·후보 정지·감독 창이 함께 쓴다. PS `$Script:LabelRelayNormalStopTimeoutSeconds = 150` 은 정규식+ast 계약 시험이 자식+30 에 묶는다. SYSTEM 작업은 ExecutionTimeLimit PT0S·AllowHardTerminate false 라 150 초가 잘리지 않는다.
3. 화면 스레드 대기 0 ✔: 바뀐 기다림은 product host(GUI import 전)·SYSTEM·설치기에서만 돈다. 소유 재조회 120 초는 FULL 길 첫 창 전 main(Tk 루프 없음)이다. 완료 뒤 소유 부재 실패는 2→120 초로 늦지만 상한은 원 부모 마감(600 초) 그대로이다. window-first 는 lambda launcher 라 닿지 않는다.
4. 진단 줄 ✔: Python 은 stderr None 이면 반환하고 예외를 모두 삼키며 finally 안이라 결과가 그대로이다. PS 도 try/catch {}. stderr 를 stdout 에 합치는 호출(2>&1·STDOUT) 0. Pinned probe·Product·lifecycle 호출자는 stderr 를 끝까지 비우고 실패 때만 읽는다. `Invoke-LabelTimedWait` 블록 변수가 $Event·$Action·$status·$result 에 가려지는 곳 0.
5. Add-Type 이동 ✔: 컴파일은 여전히 파일 조작 앞이고, 실패는 원 예외로 나가며 교체 시작 0. 1000 ms 기한·재시도·5/32 판정은 그대로이다. inline 세 사본은 정규 함수와 바이트가 같고 sha256 18e20812… 와 맞는다.
6. 핀 전파 ✔: INSTALL 의 64-hex 를 가린 골격 diff 는 의도한 코드 줄뿐이다. LF 정규화 digest 6 개(INSTALL_THIS_PC 5ab5c40f…, user_relay d34b2e1e…, auto_update 9b271ddb…, writer_session_fence d34b052e…, label_writer_fence 11a5ea34…, label_worker_shortcuts 1f4d1c43…)와 probe af78fc32… 를 다시 계산해 선언과 맞았고, base digest 남음 0. inventory 핀 8aea68d0… 은 py·ps1 이 같은 값이며, scanner 를 실행하지 않아 다시 계산하지 않았다.

## P3
- 숫자 복사: 새 120000 이 세 ps1 의 16 곳(CANONICAL 12·THIS_PC 3·writer_fence 1)에 있고, Python `RELAY_OWNER_READBACK_TIMEOUT_SECONDS=120` 은 따로 있다. probe(`INSTALL_CANONICAL_PORTABLE.ps1:5655,5681`)는 새 상수 대신 `자식+30` 을 다시 쓴다. 계약 시험은 `INSTALL_THIS_PC.ps1:442,902` 와 되돌림 쪽(`INSTALL_CANONICAL_PORTABLE.ps1:4885,5825,5934,6070,6074,7557`)을 묶지 않는다. 이적 판과 같은 지적이다.
- 진단 분류: `Wait-LabelRelayPreimageExit` 시간 넘김과 admission 시간 넘김(throw)은 status ERROR 로 남아 Python(UNKNOWN)과 어긋난다. `INSTALL_CANONICAL_PORTABLE.ps1:4105` `$elapsedStatus='COMPLETE'` 는 throw 뒤라 도달할 수 없고, try 본문 들여쓰기도 맞지 않는다. 결과 영향 0.
- `INSTALL_THIS_PC.ps1:1104`(와 inline 3 사본)에서 StartNew 바로 뒤의 1000 ms 검사는 이제 사문이며, 'preparation exceeded' 문구가 나올 길이 없다.
- `auto_update.py:1948-1954`: `normal_stop_started_at` 을 쓰는 곳은 이 줄뿐이고 그 뒤 phase 는 APPLIED_PENDING_READY 가 아니다. 그래서 실제 흐름에서는 늘 새 150 초이다. 원 시각으로 남은 시간을 셈하는 분기(0 이면 관찰 0 회로 UNKNOWN)는 합성 상태 시험에서만 닿는다. 감독 창 결속은 유효하다.
