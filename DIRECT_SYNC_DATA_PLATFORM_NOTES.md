# Direct Sync Data Platform Notes

작성 기준: 2026-07-22

이 파일은 포장 프로그램이 서버 취합/direct-sync 장기 구조와 맞물릴 때 유지해야 할 사항이다.

## 데이터망 기준

- 운영 데이터망 기준에서 Syncthing은 제외한다.
- 서버 취합 주경로는 `포장 scan set/direct-sync spool -> HTTPS relay/producer ingest -> WorkerAnalysisGUI-web`이다.
- `C:\Sync`, Syncthing, legacy mirror/shadow 경로가 남아 있더라도 운영 수량, 용량, 장애 원인 판단의 기준으로 삼지 않는다.

## 이 프로그램의 역할

- `Label_Match`의 중앙 표준 경로는 원본 compact PHS2 한 번으로 이적 완료 멤버십을 조회하고, 필요 시 F4로 동일 품목 1~2개를 원자 교체한 뒤 F3 포장 완료 명령을 만든다. 제품 3개와 최종 라벨 scan set은 명시적으로 분류된 레거시 입력에만 적용한다.
- 중앙 PHS2 포장은 durable outbox에 먼저 기록되고 중앙 `ACKED` 뒤에만 완료된다. `PENDING/SENDING/CONFLICT`를 로컬 성공 이벤트로 투영하지 않는다.
- 이벤트는 로컬 저장소와 direct-sync spool을 거쳐 서버로 올라간다.
- 포장 데이터는 서버 projection에서 원본 PHS2, 현재 제품 멤버십, F4 교체 이력과 포장 ACK를 맞추는 핵심 입력이다.

## 꼭 유지할 사항

- Spool 파일은 서버 receipt 확정 전까지 재전송 가능한 원천 payload다.
- Missing/unreadable spooled file은 `operator_review`로 쌓아두지 않고 `failed_permanent`로 닫는다. 로컬 파일 손실은 재시도로 복구되지 않는다.
- Relay id 기반 deterministic retry jitter를 유지한다.
- 서버 `Retry-After`가 유효하면 producer가 보존해야 한다. `0`도 유효한 즉시 재시도 값이다.
- 서버가 이미 commit한 non-2xx는 무한 retry로 되돌리지 말고 operator review 계열로 분리한다.
- PHS2/F4/F3 명령 schema나 barcode field 이름을 바꿀 때는 서버 trace projection, idempotency/CAS와 명시적 legacy fallback을 같이 확인한다.
- 동일 PHS2의 다중 PC 요청은 중앙에서 한 번만 commit하고 나머지는 conflict로 격리해야 한다. 재전송은 같은 idempotency key를 유지한다.
- 날짜가 바뀌어도 미확정 중앙 outbox와 PHS2 상태를 삭제하지 않는다.

## 미룬 작업

- terminal acked spool/status retention은 receipt 재시도 안전성 검증 전까지 자동 cleanup 대상으로 보지 않는다.

## 현재 리포트/가드레일

- `direct_sync_push.py`의 `relay_queue_status()`는 `acked_retention`을 출력한다. ACKED spool/status 용량과 누락 상태를 보여주는 read-only 리포트이며 cleanup 승인이 아니다.
- `acked_relay_retention_candidates()`는 full receipt validation, status artifact 일치, spool hash/byte 검증을 통과한 보존 검토 후보만 반환한다. 반환 결과도 삭제 권한이 아니다.

## 관련 검증

```powershell
cd C:\company\program\Label_Match
python -m pytest -q -p no:cacheprovider tests\test_direct_sync_push.py
```
