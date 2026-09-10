# 포장 제품 교체 정책

포장 프로그램은 원본 물리 PHS2를 한 번 읽어 전체 단일 `TRANSFER` 멤버십을 확인한 뒤,
아직 `CREATE_PACKAGE`가 커밋되지 않은 현재 세트에서 **제품 교체(F4)** 를 허용한다.
작업자는 수량을 먼저 정하지 않고 교체 대상과 새 양품을 차례로 스캔해 목록에 추가한다.
실제 대상 멤버 수 이내에서 목록을 확인·수정·삭제하고, 미완성 입력은 취소할 수 있다.
목록을 완성해도 자동 제출하지 않으며 **교체 적용** 한 번으로 전체 쌍을 원자 제출한다.
비어 있거나 미완성인 목록, 중복·양쪽 교차 barcode와 대상 수 초과는 제출할 수 없다.

기존 `sealed_transfer_member_replacement_v1`와 `max_pairs: 2`는 그대로 유지한다.
3쌍 이상은 `capability_ids`의 `sealed_transfer_member_replacement_target_members_v1`과
동일 이름의 object에서 `enabled: true`,
`base_capability: sealed_transfer_member_replacement_v1`,
`pair_limit_basis: TARGET_MEMBER_COUNT`, 정수 `min_pairs: 1`을 모두 확인해야 한다.
이 확인 실패는 durable intent 생성 전에 거부하므로 작업자가 목록을 고칠 수 있다.
접수 후 pending·오류는 전체 목록을 잠근 상태로 같은 저장 intent/명령을 복구한다.
`prepare` 성공 직후 첫 `attempt.load` 실패도 새 제출을 허용하지 않는다.

중앙 명령 `REPLACE_SEALED_TRANSFER_MEMBERS`는 다음을 한 트랜잭션으로 수행한다.

1. 현재 `TRANSFER`의 전체 unit/barcode membership과 active seal을 검증한다.
2. 새 양품의 원본 PHS 전체 membership과 동일 권한·원장·품목·UOM을 검증한다.
   공여 PHS는 활성 제품이 정확히 1개인 단품이어야 한다. 다품목 PHS에서 일부만
   가져오면 기존 인쇄 라벨이 실제 잔량과 달라지므로 중앙 호출 전에 차단한다.
   각 bundle의 현재 accounting IIN과 그 member binding은 내부적으로 일치해야 한다.
   공여와 대상의 accounting IIN 자체는 달라도 되며, 중앙 sealed replacement 경로가
   공여 IIN에서 대상 IIN으로 원장 이동과 accounting rebind를 같은 transaction에서
   수행한다. immutable origin IIN은 보존하고 receipt·movement·membership 계보를 결속한다.
   작업 날짜나 origin IIN을 수정해서 적합한 양품으로 만들지 않는다.
3. 대상 TRANSFER, 양품 source PHS, 신규 damage bundle을 entity-version CAS한다.
4. 손상품을 `PROCESS_DAMAGE_HOLD`로, 새 양품을 `TRANSFER`로 이동한다.
5. 이전 seal을 무효화하고 revision이 증가한 새 seal QR을 발급한다.

receipt의 unit↔barcode 매핑, source 잔여품, damage membership, 모든 version이 명령과
정확히 일치해야만 ACK로 인정한다. ACK 후 프로그램은 새 QR을 화면에 표시하며, 작업자가
그 새 QR을 다시 스캔하기 전에는 다음 제품 스캔·현재 세트 취소를 막는다.
정상 프로그램 종료는 durable journal을 보존하며, 재시작 후 미완료 확인·적용을 복구한다.
재스캔이 끝나면 active 전자 seal과 현재 멤버십을 함께 반영하고 source 확인을 갱신한다.
원본 물리 PHS2는 그대로 보존하며, 레거시 QA 표본이 있으면 교체 대상만 함께 바꾼다.

중앙 ACK와 로컬 상태 저장 사이에 프로그램이 중단돼도 SQLite intent/receipt에서 복구한다.
이전 전자 seal은 active seal 검증에서 거부되며 `CREATE_PACKAGE`는 새 seal에 결속한다.
원본 물리 PHS2를 다시 인쇄하거나 전체 제품을 재스캔하는 절차를 추가하지 않는다.

다음 경우는 계속 fail-closed다.

- 서버가 `sealed_transfer_member_replacement_v1` capability를 광고하지 않는 경우
- 이미 PACKAGE가 생성됐거나 TRANSFER가 소비된 경우
- 현재 QR에 `SID/SREV/STK` 중앙 seal 증거가 없는 경우
- 새 양품이 다품목 PHS에 들어 있어 `REPLACEMENT_SOURCE_NOT_SINGLETON`인 경우
- 실제 대상 수 초과 또는 3쌍 이상에 필요한 추가 capability 부재
- 권한·원장·품목·UOM 또는 bundle 내부 accounting binding 불일치,
  stale version 또는 불완전 receipt

정확한 구버전 IIN equality 오류로 command 생성 전에 멈춘 review는
[좁은 동일 intent 복구 계약](spec/contracts.md#c-03)에 따라 기존 normal drain에서
fresh source/seal 검증 후 command를 먼저 durable bind한다. 다른 review를 일괄 재개하거나
원 입력을 다시 만들지 않는다. durable-command review는 receipt 조회가 기본이며,
정확한 precommit PHS instruction 거부만 같은 계약의 strict receipt 부재·저장 command/hash
무결성·fresh command 완전 일치 뒤 같은 key로 복구할 수 있다. 반복 terminal 거부는
별도 review reason으로 멈추며 상태 reset·command 재bind·새 intent는 허용하지 않는다.

기존 SQLite 저장소는 정상 initializer의 한 transaction으로 오래된 2쌍 CHECK만
양수 조건으로 옮긴다. ordered rowid와 모든 row 값/JSON bytes, 기존 index/trigger SQL을
보존하고 실패는 rollback하며 재실행은 no-op이다. 외부 view/FK가 있으면 원본을
보존한 채 거부한다. live DB 수동 migration·reset을 수행하지 않는다.
[현재 소스 검증과 남은 실제 수용](spec/operations.md#f4-editable-list)을 구분한다.
