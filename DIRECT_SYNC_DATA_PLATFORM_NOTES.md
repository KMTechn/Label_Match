# Direct Sync Data Platform Notes

작성 기준: 2026-07-22

이 파일은 포장 프로그램이 서버 취합/direct-sync 장기 구조와 맞물릴 때 유지해야 할 사항이다.

## 데이터망 기준

- 운영 데이터망 기준에서 Syncthing은 제외한다.
- 서버 취합 주경로는 `포장 scan set/direct-sync spool -> HTTPS relay/producer ingest -> WorkerAnalysisGUI-web`이다.
- `C:\Sync`, Syncthing, legacy mirror/shadow 경로가 남아 있더라도 운영 수량, 용량, 장애 원인 판단의 기준으로 삼지 않는다.

## 이 프로그램의 역할

- `Label_Match`의 중앙 표준 경로는 원본 compact PHS2 한 번으로 이적 완료 멤버십을 조회하고, 필요 시 F4로 동일 품목 1~2개를 원자 교체한 뒤 F3 포장 완료 명령을 만든다. 제품 3개와 최종 라벨 scan set은 명시적으로 분류된 레거시 입력에만 적용한다.
- 중앙 PHS2 포장은 durable outbox intent와 로컬 `TRAY_COMPLETE` 이벤트를 먼저 flush한 뒤 완료로 표시한다. `PENDING/SENDING`은 같은 idempotency key로 FIFO 재전송하며 다음 준비 작업을 막지 않는다. `CONFLICT`는 `OPERATOR_REVIEW`로 투영하되 이미 commit된 로컬 완료를 취소하지 않는다.
- 이벤트는 로컬 저장소와 direct-sync spool을 거쳐 서버로 올라간다.
- 포장 데이터는 서버 projection에서 원본 PHS2, 현재 제품 멤버십, F4 교체 이력과 포장 ACK를 맞추는 핵심 입력이다.

## 완료 상태와 receipt 경계

- `package_command_outbox.status`는 중앙 전달 상태이고 `local_completion_committed`는 로컬 완료 권위다. DB row가 `PENDING`이어도 marker가 `0`이면 `LOCAL_COMMITTED`가 아니다.
- 문서와 테스트에서 쓰는 `PREPARED`는 `PENDING + local_completion_committed=0` 조합을 설명하는 파생 용어일 뿐이다. DB status 값, enum, 별도 state로 저장하지 않는다.
- F3는 current-set recovery 파일을 먼저 원자 저장한 뒤 outbox intent를 만든다. 이후 `TRAY_COMPLETE` CSV row의 `flush + fsync`, marker와 operation lease의 같은 transaction 순서가 끝나야 로컬 완료다. 중앙 claim은 `PENDING + marker=1`만 선택한다.
- marker가 `0`이면 outbox status가 무엇이든 로컬 완료로 해석하지 않는다. 특히 pre-write `CONFLICT`는 기존 관리자 확인 경로이며 PASS로 자동 완료하지 않는다. marker가 `1`인 `CONFLICT`만 “로컬 완료는 유지되고 중앙 충돌은 검토 중”이라는 두 축의 조합이다.
- Package logistics receipt의 `status=COMMITTED`는 포장 명령 ledger 결과다. Producer ingest receipt의 `status=accepted, committed=true`는 source-file 수신 결과다. 어느 한쪽도 다른 쪽의 ACK 증거로 사용하지 않는다.
- Relay의 `pending/leased/retry_wait` 범위는 exact spool path/hash/byte가 확인될 때만 중복 enqueue를 피하기 위한 in-flight 증거다. 검증된 producer `accepted` receipt로 row가 `acked`가 된 뒤에만 명시적 진척도를 기록한다. Active spool이 없거나 손상되면 source range를 다시 평가해 기존 dedupe repair로 보낸다.
- `operator_review`와 `failed_permanent`가 이미 소유한 동일 prefix는 row와 spool을 보존한 채 이후 scan을 차단한다. accepted되지 않은 prefix를 bytes-0 delta로 다시 포장하지 않는다.

## 꼭 유지할 사항

- Spool 파일은 서버 receipt 확정 전까지 재전송 가능한 원천 payload다.
- Enqueue commit 결과가 불확실하면 새 read connection으로 같은 relay row를 확인한다. 일치하는 row가 있거나 DB 결과 자체가 불확실하면 spool을 보존하고, row가 없다는 것이 확인될 때만 생성 중 spool을 정리한다.
- Dedupe는 path/hash/length만 비교하지 않는다. 같은 install/host source identity에서는 `client_batch_id`를 제외한 immutable source-file fingerprint와 기존 producer/key/endpoint binding이 모두 같아야 같은 row를 재사용한다. Manifest fingerprint 불일치는 기존 row/spool을 보존한 채 conflict로 닫고, install/host identity rotation은 별도 row로 기록한다. Runtime lease/token/fencing은 기존 relay id의 attempt-state이므로 fingerprint에서 제외하고 그대로 보존한다.
- Missing/unreadable spooled file은 `operator_review`로 쌓아두지 않고 `failed_permanent`로 닫는다. 로컬 파일 손실은 재시도로 복구되지 않는다.
- Relay id 기반 deterministic retry jitter를 유지한다.
- 서버 `Retry-After`가 유효하면 producer가 보존해야 한다. `0`도 유효한 즉시 재시도 값이다.
- 서버가 이미 commit한 non-2xx는 무한 retry로 되돌리지 말고 operator review 계열로 분리한다.
- PHS2/F4/F3 명령 schema나 barcode field 이름을 바꿀 때는 서버 trace projection, idempotency/CAS와 명시적 legacy fallback을 같이 확인한다.
- 동일 PHS2의 다중 PC 요청은 중앙에서 한 번만 commit하고 나머지는 conflict로 격리해야 한다. 재전송은 같은 idempotency key를 유지한다.
- transient 오류, timeout, lost ACK는 outbox intent를 삭제하거나 로컬 완료를 rollback하지 않는다. 한 drain 주기에서 실패한 첫 행이 뒤의 준비된 행을 굶기지 않으며, 재시작·재연결 뒤 생성 순서대로 자동 재시도한다.
- 날짜가 바뀌어도 미확정 중앙 outbox와 PHS2 상태를 삭제하지 않는다.

## 미룬 작업

- terminal acked spool/status retention은 receipt 재시도 안전성 검증 전까지 자동 cleanup 대상으로 보지 않는다.

## Session 14 candidate fixture 확인

- Session 14의 not-pinned Label golden payload header는 `timestamp,event_type,worker_name,detail`이고 실제 `DataManager` emitter는 `timestamp,worker_name,event,details`다. 따라서 fixture와 emitter의 byte/column-name 동일성은 성립하지 않는다.
- 두 형식 모두 기존 server decoder의 Label 경로에서 accepted되는 것은 별도 focused test로 확인한다. 이는 decoder 호환 증거이지 fixture가 실제 emitter bytes라는 뜻은 아니다.
- 이 확인으로 Factory Contract bundle, lock, consumer pin을 변경하지 않는다.

## 현재 리포트/가드레일

- `direct_sync_push.py`의 `relay_queue_status()`는 `acked_retention`을 출력한다. ACKED spool/status 용량과 누락 상태를 보여주는 read-only 리포트이며 cleanup 승인이 아니다.
- `acked_relay_retention_candidates()`는 full receipt validation, status artifact 일치, spool hash/byte 검증을 통과한 보존 검토 후보만 반환한다. 반환 결과도 삭제 권한이 아니다.

## 관련 검증

```powershell
cd C:\company\program\Label_Match
python -m pytest -q -p no:cacheprovider tests\test_direct_sync_push.py
```
