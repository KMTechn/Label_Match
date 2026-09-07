# Label_Match 데이터·통합 계약

[제품·기능](README.md) · [운영·복구](operations.md) · [백로그](BACKLOG.md) · [중앙 통합](../../../Program_Spec_Hub/INTEGRATIONS.md) · [공통 용어](../../../Program_Spec_Hub/GLOSSARY.md)

기준일·dirty 소스·증거 적용 범위는 [README 기준](README.md#기준과-판정-범위)과 같다. 다음은 2026-09-07 source-backed 정적 계약 대조이며 실제 설치본의 호출·권한·서버 설정·화면 반영 입증이 아니다. `L`은 `/logistics/api/v1`이다. 서버 상세 계약을 복제해 새 정본으로 만들지 않고 클라이언트의 소비·검증 책임을 기록한다.

## 엔터티와 identity

| 항목 | 발급·유일성·의미 | 소유 근거 |
| --- | --- | --- |
| 물리 PHS2 | 중앙 `ITG`와 `LBL/HSH`, 품목 `CLC`로 현재 계보를 조회. barcode 제품 1개나 포장 세트 ID가 아님 | [parser](../../Label_Match.py) `_label_match_parse_compact_phs2`, [중앙 공정 계약](../../../WorkerAnalysisGUI-web/docs/PHS2_RESIDUAL_PROCESS_CONTRACT.md) |
| 제품 unit/barcode | 중앙 unit ID와 실제 정규화 barcode 매핑. 멤버십 hash와 barcode hash를 각각 대조 | [package_logistics](../../package_logistics.py) `resolve_transfer_evidence`, `resolve_package_source_projection` |
| TRANSFER / PACKAGE | 원장 bundle ID·entity version·authority scope에 결속. 포장은 현재 유효 source 구성의 상속 | [서버 packages](../../../WorkerAnalysisGUI-web/logistics_ledger/packages.py), [service.create_package](../../../WorkerAnalysisGUI-web/logistics_ledger/service.py) |
| seal | seal ID·revision·token·QR과 현재 membership. F4는 active seal을 교체하지만 원본 물리 PHS2 identity는 유지 | [sealed_transfer_exchange](../../sealed_transfer_exchange.py), [앱 `_prompt_new_seal_verification`](../../Label_Match.py) |
| 로컬 set / 명령 key | `set_id`, `package_bundle_id`에서 `label-package-`+`stable_id('cmd', …)` key 파생. 기존 set 또는 key가 다른 fingerprint와 결합하면 enqueue 거부 | [PackageOutbox.enqueue](../../package_logistics.py) |
| F4 intent | 저장 intent/hash에서 `label-sealed-transfer-exchange:{intent_hash}` key를 파생. 재시도 때 새 명령 identity를 만들지 않음 | [교체 `_build_command`](../../sealed_transfer_exchange.py) |
| operation lease | 서버 lease ID·fence·snapshot hash·token·UTC 완료시각, 로컬 set의 결속을 검사. 일반 전송 worker의 SENDING lease와 별개 | [terminal_operation_lease](../../terminal_operation_lease.py), [앱 `_queue_authoritative_package`](../../Label_Match.py) |
| capture / producer source | 접수 capture key·partition sequence와 producer install/host/source-file identity는 서로 다른 중복 방지 범위 | [접수 저장](../../deferred_intent_capture.py), [전송](../../direct_sync_push.py) |

<a id="states"></a>
## 상태와 확정 경계

**중앙 명령 ACK와 producer ACK는 서로를 대체하지 않는다.** [완료 정책](../../DIRECT_SYNC_DATA_PLATFORM_NOTES.md)의 원칙과 [DataManager/완료 함수](../../Label_Match.py), [outbox](../../package_logistics.py), [producer receipt 검증](../../direct_sync_push.py)을 함께 따른다.

| 관측 지점 | 확정 조건 | 아직 입증하지 않는 것 |
| --- | --- | --- |
| 접수 보존 | encrypted/authenticated intent의 durable commit | 바코드 유효성, 포장 완료, 중앙 재고 변경 |
| 로컬 포장 완료 | current-state 보존, intent, `TRAY_COMPLETE` flush/fsync, 완료 marker·lease transaction | 중앙 PACKAGE 명령 COMMITTED |
| 중앙 명령 확정 | 같은 authority/key/command에 대한 검증된 logistics `status=COMMITTED` receipt | CSV ingest·집계·화면 최신성 |
| producer 수신·투영 | source identity 및 행 합계가 맞는 `status=accepted`, `committed=true`, `projection_disposition=COMPLETE` | 명령 원장 ACK, 모든 소비 화면 표시 |
| 소비 화면 | 해당 API의 filter/flag/source readiness 아래 실제 값·표시를 관찰 | 실물 랩핑·인쇄·다른 화면의 최신성 |

`package_command_outbox.status`는 전달 상태(`PENDING`, `SENDING`, `ACKED`, `CONFLICT`)이며 `local_completion_committed`가 로컬 완료 축이다. `PREPARED`는 `PENDING + marker=0`의 설명용 용어로 DB enum이 아니다. marker=0은 status와 무관하게 로컬 완료가 아니며, marker=1의 중앙 conflict는 기존 로컬 완료를 보존하고 `OPERATOR_REVIEW`/사후 검토 사건으로 투영한다. 근거: [mark_local_completion_committed, mark_conflict](../../package_logistics.py), [완료 정책](../../DIRECT_SYNC_DATA_PLATFORM_NOTES.md).

접수 저장소는 `CAPTURED_UNVERIFIED → VALIDATING` 이후 `VALIDATED/READY_TO_SUBMIT`, `SUBMITTING`, `ACKED/LOCAL_EFFECT_PENDING`, `COMPLETED`를 구분한다. `RETRY_WAIT_VALIDATION`, `WAITING_DEPENDENCY`, `RECONCILE_PENDING_VALIDATION`, `RETRY_WAIT_SUBMIT`, `RECONCILE_PENDING_SUBMIT`, `BLOCKED_INVALID`, `OPERATOR_REVIEW`가 예외를 보존한다. `CANCELLED/SUPERSEDED`는 종결-미완료다. 모든 입력이 이 상태들을 반드시 한 번씩 순서대로 통과한다는 뜻은 아니다. 상태·전이의 상세 원본은 [deferred_intent_capture](../../deferred_intent_capture.py)와 [fixture 계약](../../tests/fixtures/deferred_intent_contract/CONTRACT.md)이다. 이 fixture는 2026-09-06 재구성본이며 과거 이름의 MEASURED 시나리오를 현재 native 측정으로 승격하지 않는다.

<a id="quantities"></a>
## 수량·시각·로컬 파일

| 값 | 단위·모집단·포함/제외 | 시간·집계·중복 |
| --- | --- | --- |
| source `member_count` | 현재 유효 TRANSFER/work-group 제품 구성원 수. PHS2 한 번의 스캔 수와 다름; `INHERIT_ALL`은 그 집합 전체 | 현재 source snapshot/version 기준. unit와 barcode hash를 각각 대조 |
| QA 표본 3개 | 명시 호환 경로의 검증 표본. 포장 전체 멤버십 수나 중앙 표준 필수 입력 수가 아님 | 표준 PHS2 입력은 1회; 표본을 제품 총량으로 합산하지 않음 |
| F4 1~2쌍 | 교체 쌍 수. 유효 교체는 기존 멤버를 새 멤버로 치환 | 중앙 원자 처리·같은 key 재생. 전체 구성 수 보존 |
| 포장 세트 | `PACKAGING_SET_COUNT`, 웹 `total_sets_completed`. 포장 완료 세트 수이며 제품 개수와 환산하지 않음 | 취소·부분 제출 등을 제외하는 projection 규칙. 원본 PHS2/최종 라벨은 제품 barcode에서 제외 |
| CSV row / accepted row | event 전달·투영 처리 건수. 포장 세트나 제품 개수와 다름 | 중복·quarantine·처리 행 합계와 source identity로 ACK 검증 |

근거: [package draft/command](../../package_logistics.py), [서버 Label event normalize](../../../WorkerAnalysisGUI-web/common_projection_sync.py), [포장 projection](../../../WorkerAnalysisGUI-web/common_projection.py)의 `_label_match_payload_passed` 및 포장·취소 집계, [dashboard_standard.source.js](../../../WorkerAnalysisGUI-web/static/dashboard_standard.source.js) `total_sets_completed` 표시. `Pcs/EA/piece`의 전 제품 공통 환산은 미확정이며 [LM-B06](BACKLOG.md#lm-b06)·[공통 용어](../../../Program_Spec_Hub/GLOSSARY.md)에서 관리한다.

`DataManager.log_event`는 timezone 없는 로컬 `datetime.now().isoformat()`을 기록한다. 서버 Label transport 시간대는 `Asia/Seoul`이며 lease `operation_completed_at`은 UTC `Z`다. 생산일, 포장 업무일, event 발생, 수신, projection, 화면 조회 시각은 별개다. 날짜 변경은 outbox 만료 기준이 아니다. 근거: [DataManager/lease](../../Label_Match.py), [서버 시간대·포장-출고 집계](../../../WorkerAnalysisGUI-web/common_projection.py). PC 시계차 허용치·업무일 cutoff·표시 지연 목표는 [LM-B02](BACKLOG.md#lm-b02)의 미정 요구다.

로컬 emitter CSV는 UTF-8 BOM(`utf-8-sig`), 헤더 `timestamp,worker_name,event,details`이며 details는 JSON이다. 완료 등 durable event에 flush/fsync를 적용하고 writer 오류를 `flush/close`에서 반환한다. 현재 세트 JSON은 임시 파일 flush/fsync 후 `os.replace`한다. 이 파일과 SQLite outbox·lease가 하나의 파일 transaction인 것은 아니므로 복구 절차로 경계를 조정한다([DataManager](../../Label_Match.py), [완료 정책](../../DIRECT_SYNC_DATA_PLATFORM_NOTES.md)). 일부 fixture의 `event_type/detail` 헤더는 emitter 원본 byte가 아니다. 서버 decoder의 alias 호환과 실제 emitter 동일성을 분리한다.

`event_stream_policy.LOCAL_ONLY_EVENT_TYPES`는 별도 로컬 이벤트 파일로 나뉜다. `PHS_LABEL_ACTIVE_RESOLVED`, `EXACT_RESCAN_STARTED/OK/COMPLETED` 같은 로컬 전용 event를 원격 실적으로 가정하지 않는다. `CONTRACT_CANDIDATE_EVENT_TYPES`의 이름만으로 전체 전송 계약 수용을 증명하지 않는다([이벤트 정책](../../event_stream_policy.py), [DataManager._get_log_filepath_for_item](../../Label_Match.py)).

로컬 GUI writer의 기존 저장 위치를 보존하기 위해 [guard `resolve_data_scope`](../../label_match_single_instance.py)는 앱과 같은 nonempty `custom_save_path` → `LABEL_MATCH_SAVE_DIR` → ProgramData 순서로 소스 교정했다. null/빈 custom은 fallback하며 데이터 이동·schema/업무 identity 변경은 없다. 이름은 앞뒤 공백 제거 후 기존 `ntpath.normcase(normpath(path))`의 UTF-8 SHA-256 앞 32자리와 `Global\KMTech.LabelMatch.`로 만들고, callback 종료까지 handle을 보유한다. [SaveRoot13 실제 13 PASS/39 ordered phase PASS](operations.md#saveroot13-evidence)는 custom 우선 선택, missing/empty/whitespace/null × env 유무의 실제 writer resolver 대조, 단일 process의 native mutex 중복 callback 제외·해제 후 재진입을 입증한다. 실제 파일 alias/relative 경로의 동일성이나 settings 생성·변경 race를 해결한 것은 아니다. [onboarding의 guard 이전 ledger와 persistent relay의 별도 root](operations.md#storage-root-residual)는 여전히 OPEN이다. 두 GUI writer·packaged/source·pending identity 복구는 [LM-B10](BACKLOG.md#lm-b10)의 별도 미입증 수용 범위이며 과거 Modules3 PASS를 이 교정에 확장하지 않는다.

<a id="c-00"></a>
## C-00 공통 명령·권한

명령 envelope는 `contract_version=logistics-v1`, `command_type`, `authority_scope_id`, `authority_epoch`, `ledger_plane`, `plane_epoch`, `idempotency_key`, `expected_versions`, `payload`를 사용한다. payload의 source/package·membership·seal/lease evidence가 중앙 snapshot과 맞아야 한다. 임의 수량을 중앙 멤버십 대신 보내지 않는다. 클라이언트는 JSON boolean `ok` 등 응답 구조와 receipt의 identity·구성·버전을 검증한다([package_logistics.build_create_package_command/_data/_validate_receipt](../../package_logistics.py)).

HTTPS 요청은 Bearer 및 logistics token 헤더, `X-Logistics-Source-Host-Id`, `X-Logistics-Device-Id`, `X-Logistics-Program=Label_Match`, 명령 `Idempotency-Key`를 보낸다. 실제 값은 문서에 저장하지 않는다. 서버는 기계 토큰·device·authority scope를 검증하며 사람 이름 입력은 이 권한을 만들지 않는다. 같은 scope+key+fingerprint는 receipt 재생, 다른 fingerprint는 충돌이다. 근거: [클라이언트 `_request`](../../package_logistics.py), [서버 auth](../../../WorkerAnalysisGUI-web/blueprints/logistics/auth.py), [replay_or_conflict](../../../WorkerAnalysisGUI-web/logistics_ledger/idempotency.py).

프로필 선택·HTTPS·required mode와 DPAPI 경계는 [운영 설정](operations.md#configuration), 실제 installed provider/flags/권한 확인은 [LM-B04](BACKLOG.md#lm-b04)다. 현행 current-user 배포의 DPAPI·프로필과 기존 Machine anchor 우선권은 별도 구성으로 대조하며 공통 Machine profile 안내를 모든 설치본에 적용하지 않는다([릴리스 계약](../../RELEASE_GATE_CONTRACT.md), [profile resolver](../../logistics_runtime_profile.py)). API 존재와 운영 장비의 접근 성공은 별도다.

<a id="c-01"></a>
## C-01 중앙 품목 → 앱 표시

- 방향/형식: 서버 `GET /inbound/api/item-catalog.csv` → `refresh_item_catalog` → 인증 cache/snapshot → 품목 표시. `Item Code,Item Name,Spec,Tray Image` 열을 사용한다.
- 권한/오류: 서버 reader 권한·cache 방지 응답, 클라이언트 중앙 등록 identity에 따른 인증 cache 복구. 등록 상태에서 인증 cache가 없으면 요청 실패를 bundled 파일 성공으로 숨기지 않는다.
- 단위/효과: 표시용 품목 마스터이며 재고·수량 예약이나 포장 command가 아니다. 일반 명령 idempotency key/transaction/cursor가 적용되는 쓰기 경로가 아니다.
- 양쪽 근거: [item_catalog_sync.refresh_item_catalog](../../item_catalog_sync.py), [api_item_catalog_csv](../../../WorkerAnalysisGUI-web/blueprints/inbound/__init__.py). [LM-02](README.md#lm-02), [LM-B04](BACKLOG.md#lm-b04).

<a id="c-02"></a>
## C-02 앱 → 현재 포장 source 조회

- API: `GET L/bundles/resolve`, `bundle_role=PACKAGE_SOURCE`, `input_tag_id`(ITG), 품목·scope, `input_tag_label_id`(LBL)와 `input_tag_hash_prefix`(HSH). 물리 label 표시 문자열을 독립 조회 권위로 사용하지 않는다.
- 검증: LBL/HSH는 함께 공급하며 ITG가 필요하다. HSH는 16자리 hex다. 선택적인 barcode membership hash와 member count filter도 쌍으로 검증한다. 응답의 unit/barcode 매핑·membership hash·entity version·work-group topology가 현재 작업에 결속된다.
- 효과/오류: 조회 자체는 포장 확정이 아니다. 잘못된 라벨·불완전 증거·구성 변화는 fail-closed; 최신 snapshot과 operation lease의 일치는 F3에서 다시 필요하다. 읽기 조회에 명령 receipt/cursor를 가정하지 않는다.
- 양쪽 근거: [resolve_transfer_bundle/resolve_package_source_projection](../../package_logistics.py), [resolve_bundle](../../../WorkerAnalysisGUI-web/blueprints/logistics/api.py). [LM-03](README.md#lm-03), [LM-B05](BACKLOG.md#lm-b05).

<a id="c-03"></a>
## C-03 F4 교체·재봉인

- API: `GET L/replacements/good-source/resolve`, `POST L/transfers/{id}/members/replace-and-reseal`; command `REPLACE_SEALED_TRANSFER_MEMBERS`, capability `sealed_transfer_member_replacement_v1`.
- payload: `target_bundle_id`, `damage_bundle_id`, target evidence, `pairs`, 예상 대상/donor/damage version. 1~2쌍, 같은 lot·품목·UOM, 활성 제품 1개 donor PHS를 검사한다. 예상 damage version 0도 명령에 결속한다.
- 원자 효과: 대상·donor·damage CAS, 손상품 `PROCESS_DAMAGE_HOLD` 이동, 양품 TRANSFER 편입, 이전 전자 seal 무효화·새 revision 발급이 한 중앙 transaction이다. 이 중앙 성공과 앱의 QR 확인·로컬 반영은 별도 경계다.
- 중복/복구: 같은 intent hash key와 저장 command를 사용하며 receipt의 매핑·잔량·damage membership·version까지 비교한다. ACK 유실 또는 로컬 적용 실패는 intent/receipt에서 복구한다. 새 전자 QR 확인 전 제한은 앱 gate가 소유한다.
- 양쪽 근거: [교체 workflow](../../sealed_transfer_exchange.py) `_build_command/attempt`, [서버 replace_sealed_transfer_members](../../../WorkerAnalysisGUI-web/logistics_ledger/service.py), [앱 QR 확인](../../Label_Match.py). [기존 교체 정책](../MEMBER_EXCHANGE_POLICY.md)은 seal 기반 설명과 물리 PHS2 유지의 구별이 필요하다([LM-B01](BACKLOG.md#lm-b01)).

<a id="c-04"></a>
## C-04 F3 포장 명령·lease·outbox

- API: `POST L/operation-leases/issue`, `POST L/packages`, `GET L/receipts/{scope}/{key}`. 기본 membership mode는 `INHERIT_ALL`; 정확한 work-group 경로와 호환 `EXACT_RESCAN`의 command 생성은 [package_logistics](../../package_logistics.py)에 남아 있다.
- lease: current set에 attach된 `PREFETCHED` lease의 ID·snapshot hash·fence가 검증 결과와 일치해야 한다. UTC 완료시각을 결속해 로컬 marker와 lease를 같은 SQLite transaction으로 확정한다. 서버 승인 없이 새 offline lease를 임의 발급하지 않는다.
- 중앙 원자성: source TRANSFER의 `AVAILABLE → CONSUMED` CAS, PACKAGE 및 `SHIPPING-WAIT`, `PACKAGE_CREATED` event·outbox·receipt를 중앙 transaction으로 기록한다. 앱 CSV와 중앙 transaction 사이에는 분산 원자 commit이 없으며 동일 키 복구로 연결한다.
- 선택/순서: `claim_next`는 marker=1, due PENDING만 선택한다. stale `SENDING`은 300초 기준으로 PENDING 회수하며 `COALESCE(last_attempt_at,created_at), created_at, idempotency_key`로 정렬한다. 한 drain에서 시도한 key를 제외하므로 첫 실패가 뒤의 준비 row를 영구 차단하는 엄격 FIFO가 아니다. 300초는 **전송 claim 구현 상수**로 업무 lease 허용 시간과 다르다.
- 오류: transport는 retry. `408/425/429`, 5xx 또는 retryable 오류는 409/412가 아닐 때 retry 대상이다. `committed=true` 오류, 409/412, 비재시도 거부 및 receipt 불일치는 conflict로 보존한다. `Retry-After` 처리와 재시도 시 같은 key를 유지한다.
- 부분 효과: marker=0 중단은 복구 대상이고 전송 대상이 아니다. marker=1 뒤 중앙 충돌은 로컬 완료 유지·검토 사건이며 실물·원본 증거를 보존한다. 취소는 C-06의 별도 명령이다.
- 양쪽 근거: [앱 `_queue_authoritative_package`/완료](../../Label_Match.py), [PackageOutbox/PackageOutboxProcessor](../../package_logistics.py), [서버 API](../../../WorkerAnalysisGUI-web/blueprints/logistics/api.py), [service.create_package](../../../WorkerAnalysisGUI-web/logistics_ledger/service.py), [CAS 구현](../../../WorkerAnalysisGUI-web/logistics_ledger/packages.py). [LM-06/07](README.md#lm-06), [LM-B02](BACKLOG.md#lm-b02), [LM-B05](BACKLOG.md#lm-b05).

<a id="c-05"></a>
## C-05 관측 CSV → 수신·projection → 화면

- 경로: 로컬 event CSV → spool/relay → `POST /api/producer-ingest/v1/source-file` → server common projection → `/dashboard/api/operations_flow` → 웹 표시. source system `label_match`, dataset `legacy_packaging_csv`, 앱 scan 계약 `label_match_current_v1`이다.
- wire/identity: HMAC 서명 multipart의 CSV+metadata에 install/host/stream/source identity, content SHA-256, byte length, row count, client batch ID를 결속한다. 로그 내용·인증 값을 문서에 복사하지 않는다.
- 수신/진척: accepted/committed, source-file identity·행 합계·projection COMPLETE까지 확인한다. 전송 중 `pending/leased/retry_wait`는 정확한 spool path/hash/byte가 일치할 때 in-flight 중복 억제 증거다. 그것만으로 source prefix 진척을 ACK 처리하지 않는다.
- cursor/중복: 일반 물류 command에 공통 consumer cursor가 있는 것은 아니다. 이 전송은 source range/prefix와 immutable fingerprint, producer/key/endpoint binding으로 중복을 판단한다. `client_batch_id` 변경만으로 새 내용으로 취급하지 않으며 runtime lease/fencing은 attempt 상태다. 손상/없는 spool은 source range 재평가 대상이다.
- 오류/보존: deterministic jitter·유효 Retry-After(0 포함)를 유지한다. missing/unreadable spool은 `failed_permanent`, 이미 commit한 non-2xx는 검토로 분리한다. `operator_review/failed_permanent`의 동일 prefix·spool을 보존하며 미수신 구간을 0-byte delta로 바꿔 진행시키지 않는다. ACKED retention 후보도 삭제 승인이 아니다.
- 소비: `normalize_legacy_csv_row/ingest_label_match_csv_file`이 event/detail alias를 처리하고 common projection이 세트·취소 의미를 계산한다. `PROJECTION_API_READ_ENABLED`와 source readiness에 따라 operations_flow가 제한될 수 있다. 브라우저는 operations_flow를 요청하고 포장을 세트로 표시한다. 실제 배포 flag·조회 결과·신선도는 미입증이다.
- 양쪽 근거: [direct_sync_push](../../direct_sync_push.py), [완료/보존 정책](../../DIRECT_SYNC_DATA_PLATFORM_NOTES.md), [producer_ingest](../../../WorkerAnalysisGUI-web/producer_ingest.py), [HTTPS edge 계약](../../../WorkerAnalysisGUI-web/docs/PRODUCER_INGEST_HTTPS_EDGE.md), [common_projection_sync](../../../WorkerAnalysisGUI-web/common_projection_sync.py), [common_projection](../../../WorkerAnalysisGUI-web/common_projection.py), [dashboard API](../../../WorkerAnalysisGUI-web/app.py), [표준 표시](../../../WorkerAnalysisGUI-web/static/dashboard_standard.source.js), [enhanced 요청](../../../WorkerAnalysisGUI-web/static/dashboard_enhanced.js). [LM-11](README.md#lm-11), [LM-B06](BACKLOG.md#lm-b06).

<a id="c-06"></a>
## C-06 포장 취소

`POST L/packages/cancel` / `CANCEL_PACKAGE`는 package ID·사유·비어 있지 않은 evidence와 예상 version을 요구한다. 로컬 취소 outbox는 원래 CREATE 명령 ACK에 의존하며 동일 취소 command/key로 재시도한다. 서버는 포장 완료 유효성을 취소하고 **재고는 SHIPPING-WAIT에 유지**한다. 이를 TRANSFER 재고 반환이나 실제 포장 해체라고 해석하지 않는다. 집계의 취소 제외는 별도 projection 소비 결과다. 근거: [PackageCancellationOutbox/processor](../../package_logistics.py), [API](../../../WorkerAnalysisGUI-web/blueprints/logistics/api.py), [service.cancel_package](../../../WorkerAnalysisGUI-web/logistics_ledger/service.py). [LM-09](README.md#lm-09), [LM-B05](BACKLOG.md#lm-b05).

<a id="c-07"></a>
## C-07 F5 현품표 출력·활성화

`L/phs-label-exchanges/prepare`, exchange별 status/`prints`/print-attempt complete/`activate`와 reconciliation 경로가 준비·출력·활성화를 분리한다. 로컬 `label-match-phs-label-exchange-v1` journal과 `execute_single`, `recover_current`, `recover_reconciliation`은 서버 결과·물리 출력 증거·로컬 적용을 조정한다. Windows proof의 `proof_kind=WINDOWS_GDI_SPOOL`, printer/job/document/submitted_at와 `windows_gdi_end_doc=true`는 큐 접수 증거이며 종이 배출·QR 가독성 측정이 아니다. 상세 분기 payload와 설치 capability 조합의 전수 검증은 미완료다. 양쪽 근거: [phs_label_workflow](../../phs_label_workflow.py), [서버 phs-label-exchanges API](../../../WorkerAnalysisGUI-web/blueprints/logistics/api.py). [LM-10](README.md#lm-10), [LM-B04](BACKLOG.md#lm-b04).

## 계약 유지·검증 연결

기능 기준은 [README 카드](README.md#기능-카드), 실행 설계는 [운영 수용 시나리오](operations.md#verification)에서 연결한다. 서명·권한·version/capability와 source identity가 달라지면 해당 계약의 실제 양쪽 evidence를 재대조한다. 현재 설치 provider/overlay, 서버 flag, upstream 입력 발행과 downstream 출고·화면의 전체 연결은 [LM-B04](BACKLOG.md#lm-b04)·[LM-B06](BACKLOG.md#lm-b06)·[LM-B08](BACKLOG.md#lm-b08)의 확인 과제이며 코드 존재를 연동 성공으로 집계하지 않는다.
