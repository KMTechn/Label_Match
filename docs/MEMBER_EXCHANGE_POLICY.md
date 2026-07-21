# 포장 제품 교체 정책

포장 프로그램은 중앙에서 봉인된 `TRANSFER` 현품표를 읽은 뒤, 아직 `CREATE_PACKAGE`가
커밋되지 않은 현재 세트에서만 제품 1~2개 교체를 허용한다. 작업자는 **제품 교체(F4)** 에서
교체 대상과 새 양품을 차례로 스캔한다.

중앙 명령 `REPLACE_SEALED_TRANSFER_MEMBERS`는 다음을 한 트랜잭션으로 수행한다.

1. 현재 `TRANSFER`의 전체 unit/barcode membership과 active seal을 검증한다.
2. 새 양품의 원본 PHS 전체 membership과 동일 입고 lot·품목·UOM을 검증한다.
   공여 PHS는 활성 제품이 정확히 1개인 단품이어야 한다. 다품목 PHS에서 일부만
   가져오면 기존 인쇄 라벨이 실제 잔량과 달라지므로 중앙 호출 전에 차단한다.
3. 대상 TRANSFER, 양품 source PHS, 신규 damage bundle을 entity-version CAS한다.
4. 손상품을 `PROCESS_DAMAGE_HOLD`로, 새 양품을 `TRANSFER`로 이동한다.
5. 이전 seal을 무효화하고 revision이 증가한 새 seal QR을 발급한다.

receipt의 unit↔barcode 매핑, source 잔여품, damage membership, 모든 version이 명령과
정확히 일치해야만 ACK로 인정한다. ACK 후 프로그램은 새 QR을 화면에 표시하며, 작업자가
그 새 QR을 다시 스캔하기 전에는 다음 제품 스캔·현재 세트 취소·프로그램 종료를 막는다.
재스캔이 끝나면 현품표 QR과 이미 읽은 QA 표본 중 교체 대상만 한꺼번에 바꾼다.

중앙 ACK와 로컬 상태 저장 사이에 프로그램이 중단돼도 SQLite intent/receipt에서 복구한다.
기존 QR은 active seal 검증에서 즉시 거부되며 새 QR만 `CREATE_PACKAGE`에 사용할 수 있다.

다음 경우는 계속 fail-closed다.

- 서버가 `sealed_transfer_member_replacement_v1` capability를 광고하지 않는 경우
- 이미 PACKAGE가 생성됐거나 TRANSFER가 소비된 경우
- 현재 QR에 `SID/SREV/STK` 중앙 seal 증거가 없는 경우
- 새 양품이 다품목 PHS에 들어 있어 `REPLACEMENT_SOURCE_NOT_SINGLETON`인 경우
- 3개 이상 교체, 다른 lot·품목·UOM, stale version 또는 불완전 receipt
