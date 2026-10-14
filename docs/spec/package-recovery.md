# 취소 포장의 양품 전량 회수 — package-recovery-read-v1

2026-10-15 소스 계약. 앱 포장 105·이적 109의 연결 기준이며 설치본·현장 수용을 뜻하지 않는다.

`CANCEL_PACKAGE`의 `reopen:false`는 취소 성공의 불변 증거다. 이후에도 원 CREATE/CANCEL
요청·키·영수증·완료 marker·사건·PACKAGED root를 바꾸지 않는다. 별도 관리자가 현재 취소
PACKAGE의 양품 전량을 실물과 대조하고 **새 TRANSFER**로 받는다. 원 TRANSFER·잔량·다른
PACKAGE의 제품은 옮기지 않는다. SHIPPING-WAIT → TRANSFER 이동은 새 회수 사실이며 과거
CREATE 이동의 reversal이 아니다. 취소 PACKAGE의 투영은 보존하고 상태만 CONSUMED로 바꾼다.

## 관리자 preview → 실행

기존 포장 기록 보충과 같은 개인 관리자·장부 관리자 역할, HTTPS·CSRF·본인 PIN 문을 쓴다.
일반 machine에 ADMIN 쓰기 권한을 추가하지 않는다. 실행 직전 PIN 확인 후 원장 거래 안에서
관리자 연결·역할·PIN 판을 재검사한다. PIN은 저장되는 요청·영수증·브라우저 pending에 넣지 않는다.

| 동작 | 경로·본문 |
|---|---|
| 읽기 preview | `POST /dashboard/api/logistics/packages/recovery/preview?authority_scope_id=…`, `{package_bundle_id,cancel_receipt_id,scanned_barcodes:[…],label_scans:[현재 현품표 QR,…]}` |
| 실행 | `POST /dashboard/api/logistics/packages/recovery/confirm?authority_scope_id=…`, `{preview_fingerprint,idempotency_key,reason,current_pin}` |

preview는 scope/authority epoch/plane/plane epoch, 원 PACKAGE·취소·생성 영수증, 정확한
멤버·hash·제품 판·위치·품목·IIN, 재고 판, 현재 활성 작업군·현품표와 변경 계보를 고정한다.
개인·세션에 서명된 preview를 결속한다. 제품 바코드는 현재 PACKAGE 전량과 중복 없이 같아야
하며 현품표는 그 제품의 현재 ACTIVE 표 전부여야 한다. 은퇴 그룹이나 옛 표를 되살리지 않는다.

회수 가능한 원 거절 코드는 `SOURCE_TRANSFER_AMBIGUOUS`, `SOURCE_TRANSFER_CHANGED`,
`PACKAGE_WORK_GROUP_CHANGED` 세 개다. `SOURCE_TRANSFER_MISSING`,
`PACKAGE_CREATION_RECEIPT_INVALID`, `PACKAGE_CREATION_RECEIPT_MISSING`,
`PACKAGE_CREATION_EVIDENCE_CONFLICT`, `PACKAGE_CREATION_MOVEMENT_MISSING`,
`PACKAGE_CREATION_MOVEMENT_CONFLICT` 여섯 개는 `PACKAGE_RECOVERY_MANUAL_REVIEW_REQUIRED`로
보류한다. 앞의 세 코드라도 원 생성 영수증·전체 계보·이동·legs, 현재 전량 소유·AVAILABLE·
SHIPPING-WAIT·동일 품목/IIN, 현재 표·작업군 근거가 부족하면 효과 0으로 거절한다.
출고 완료·미해소 operation lease·다른 보류·실물 부족·중복·hash/판 충돌도 그 건만 막는다.

`BEGIN IMMEDIATE` 하나에 새 `PHS_ADMIN_RECOVER_PACKAGE_TRANSFER` 결과, 원 포장 CAS,
새 TRANSFER·멤버 소유·정확한 이동/두 legs·balance CAS, 새 봉인, 현재 그룹의
TRANSFER_BUNDLE root·판 증가, domain event/outbox와 확인 건 사건을 저장한다.
확인 건은 과거 RESOLVED였어도 같은 원 cancel identity로 찾는다. 응답 유실·재시작 때는
원 preview·새 키·사유를 그대로 재전송하고 PIN만 다시 입력한다. 같은 키/같은 내용은 원 결과,
다른 내용은 충돌이다. 다른 관리자의 새 키도 같은 PACKAGE를 두 번 회수하지 못한다.

## 앱 105·109가 읽는 최소 모델

`GET /logistics/api/v1/packages/<authority_scope_id>/<package_id>/recovery?cancel_receipt_id=…`
는 인증된 machine의 기존 scope 읽기 허가를 요구한다. 브라우저의 같은 모델은
`GET /dashboard/api/logistics/packages/<package_id>/recovery?authority_scope_id=…&cancel_receipt_id=…`다.
관리자 원 요청·개인정보·PIN·사유는 반환하지 않는다. 일반 `/receipts`는 이 PHS_ADMIN 명령을
계속 숨긴다. 원 CANCEL key의 replay/receipt는 회수 전후 모두 원 `reopen:false`다.

| 필드 | 의미 |
|---|---|
| `contract_version` | `package-recovery-read-v1` |
| `authority_scope_id,ledger_plane,plane_epoch` | 현재 읽기 권역·세대 |
| `package_bundle_id,cancel_receipt_id,reopen,reopen_reason` | 원 취소 신원·false·당시 사유 |
| `recovery_state` | `AWAITING_RECOVERY` 또는 영수증/취소/생성/이동/사건/계보를 검증한 `RECOVERED` |
| `recovery_receipt_id,transfer_bundle_id` | 회수 전 null, 회수 후 서로 결속된 새 영수증·새 이적 |
| `member_ids,member_count,membership_hash,item_id,inbound_iin` | 회수한 정확한 전량과 품목·IIN (회수 후) |
| `transfer_state,transfer_entity_version` | 현재 새 이적 상태·판 |
| `current_transfer_member_ids,current_transfer_membership_hash` | 지금 그 이적이 소유하는 제품·hash |
| `current_transfer_projection_ids,package_source_ready` | 현재 이적 투영·소유·위치·품목/IIN을 대조한 포장 준비 여부. 준비 true여도 현품표 PACKAGE_SOURCE 재조회·CAS는 필요 |
| `current_labels` | 현재 ACTIVE 표별 `group_id,label_id,scan_payload`, 작업군·멤버·표 판, 이적 제품과의 정확한 교집합. `group_member_count,group_membership_hash`는 NG 감사 멤버를 포함한 그룹 전량이며 `member_ids,membership_hash`는 해당 이적 제품과의 교집합 |

105는 목록 부재나 RECHECK/RESOLVED를 회수 성공으로 읽으면 안 된다. 원 취소 신원으로
이 모델을 읽고 원 false 옆에 **새 recovery receipt와 새 TRANSFER**를 내구 보관한다.
현재 AVAILABLE 이적의 멤버·hash·판·실물을 대조한 뒤 현재 현품표의 PACKAGE_SOURCE를
다시 조회한다. 새 포장 번호는 recovery receipt에 결속된다. 원 취소 번호/키/marker를
바꾸지 않고 새 번호·새 명령으로 포장한다. 이미 다시 포장·출고한 경우 현재 TRANSFER가
CONSUMED일 수 있으므로 `RECOVERED`만 보고 CREATE를 재실행하지 않는다.

109는 새 TRANSFER의 기존 조회·봉인 확인을 사용한다. 회수 제품을 검사 PHS로 간주하여
일반 이적 봉인을 다시 제출하지 않는다. 회수 원천은 `PACKAGE_RECOVERY_TRANSFER_V1`로
PHS 원천 검증기에서 별도로 검증하며, 과거 PACKAGED root와 정당한 표 변경 계보를 보존한다.
지연 완료 중복 분류도 검증된 회수 증거가 있는 과거 포장만 제외한다.

## 회수 대기 목록·리포트 (R11)

웹·기계 `operator-reviews`는 원 `state`와 별도로 `recovery_state`,
`recovery_status_label`(「회수 대기」/「회수 완료」), `cancel_receipt_id`,
`recovery_receipt_id`, `transfer_bundle_id`를 더한다. 유효한 회수 증거가 없으면 원 state가
RESOLVED여도 「회수 대기」로 페이지에 포함한다. RECHECK가 만든 과거 state·사건은 보존한다.
회수 증거가 손상되거나 읽기 불가여도 목록에서 숨기지 않는다. 새 실행이 실제로 회수했을 때만
원장과 같은 거래에서 확인 건에 `RECOVER_PACKAGE_TRANSFER` 사건과 새 receipt를 기록한다.

이 방식은 과거 RESOLVED 행을 다시 쓰거나 DDL을 추가하지 않고 양쪽 목록·스캔 조회·리포트를
같은 검증에 연결한다. 흐름 리포트 `reviewsOpen`에는 이 대기 건을 포함하며
`packageRecoveryPending`은 그 수를 별도로 제공한다. 오늘 화면도 「회수 대기」로 표시한다.

## 저장·릴리스 경계

DDL 0, 새 표/열/색인/트리거 0, release-v1 표 목록 변경 0, 이행 스크립트 불필요.
기존 receipts/domain events/outbox/lineage/review events/group roots만 쓴다.
공개 추가 계약은 이 읽기 모델과 관리자 preview/실행, 회수 원천 계약, 목록/리포트의 추가 필드다.
서버 설치·앱 105/109 구현·현장 짝 수용은 별도 승인·검토 단계다.
