# Label_Match 데이터·통합 계약

## Committed stale-runtime review의 명시적 복구 · 2026-09-12

기존 `ack-reviewed --recover-expired-runtime`만 독립 검토된 committed `STALE_RUNTIME_FENCE` 수신의 local ACK와 만료 authority 재개를 같은 SQLite transaction에서 처리한다. 기존 source/request/hash/byte/count·receipt 검증을 유지하고 실제 보존 spool도 대조한다. 단일 authority의 scope/install·runtime ID·fence·lease ID·expiry와 terminal public key/token audit digest를 결속하며, future expiry·다른 review 원인·assignment/pending/token·다른 미종결 runtime-bound row가 있으면 변경 없이 거부한다. 현재 producer credential·원 relay/metadata/spool/receipt는 바꾸지 않는다.

필수 audit 경로에 이전 authority와 receipt/metadata hash의 PREPARED 근거를 먼저 fsync한다. 실패하면 ACK와 authority 변경을 rollback하며, 성공 시 authority의 status만 EXPIRED로 바꾸고 원 lease/fence/evidence를 남긴다. 후속 정상 `ensure_runtime_authority`가 기존 expiration 경로로 새 ephemeral runtime과 인증된 forward grant를 취득한다. 이 로컬 명령 자체는 HTTP 요청이나 원 source 재전송을 하지 않는다. 재호출은 이미 ACKED인 행을 거부해 authority를 다시 초기화하지 않고, 정상 acquire의 실패는 기존 persisted issue idempotency로 재시도한다.

`observed_rejected`를 정상 회전으로 인정하지 않으며 `retry-dead`의 review 제외와 다른 fail-closed 조건을 유지한다. 원 APP_CLOSE raw 승인·fence 거부와 projection 의미도 바꾸지 않는다. [실제 실패·호스트 교정과 실행 경계](operations.md#committed-stale-runtime-recovery-20260912).

## 일상 화면의 정보 경계 · 2026-09-12

durable PHS2 capture가 완료된 입력만 scan Entry에서 지운다. serial lane이 Entry를 비활성화한 동안에도 지울 수 있도록 위젯 상태를 잠시 전환하고 즉시 원래 상태로 돌린다. 실패·busy·미수락 입력과 F-key/focus 조건은 보존한다.

대기 화면의 read-only SQLite snapshot은 deferred capture와 실제 `package_command_outbox`를 함께 조회한다. 원 `state_counts`와 package status는 그대로 노출하며 operator 집계에서만 같은 set·정확한 downstream ref의 `SUPERSEDED` handoff를 중복 제외한다. package PENDING은 전송 대기, SENDING은 결과 확인, CONFLICT는 관리자 확인이며 ACKED도 로컬 completion marker가 있어야 완료로 센다. 지원 복구에서 이미 dismiss된 prewrite CONFLICT는 종결-미완료로 표시하며 관리자 확인/최장 대기를 다시 열지 않는다. 실제 대상이 없는 SUPERSEDED와 CANCELLED는 미완료로 남긴다. producer relay의 별도 settlement를 이 집계로 판정하지 않는다.

`작업 상세 보기/닫기`는 위젯 가시성만 바꾸며 accepted raw, 원 PHS2, 수량, 명령·receipt·lease·복구 상태나 F-key 허용 조건을 바꾸지 않는다. 표준 PHS2의 제품 수량은 `package_source_snapshot.member_count`를 사용하며 snapshot이 없거나 `None`인 교체 후 재조회 구간에만 검증된 `sealed_transfer.QT`를 사용한다. 기존 logistics 정수 검증으로 bool·실수·문자열·0/음수·누락을 거부하고, 양쪽 근거가 있으면 같은 양수 정수인지 확인한다. 명시적 invalid snapshot을 과거 seal로 대체하거나 소수를 잘라 수량으로 표시하지 않는다. 근거가 없거나 상충하면 수량 확인 상태를 표시하며 스캔 횟수나 이력 행 수를 수량으로 대체하지 않는다. 중앙 대기·관리자 확인과 로컬 완료의 의미는 그대로 유지하며 접수 ID·상태 코드·선행조건 identity만 상세로 이동한다. 긴 상세는 읽기 전용 스크롤 영역에 보존한다. [한정 근거](operations.md#routine-details-20260912).

## S05 단순화의 계약 경계

호출되지 않는 `_deferred_operation_lease_evidence`와 `_payload_entropy_from_row` 삭제는 lease evidence·payload 저장 형식을 변경하지 않는다. 실제 검증·암호화 경로의 `payload_protection_entropy`, `common_reader_v2_entropy`, owned payload reader와 F1 cancellation transaction은 유지한다. 사용되지 않는 catalog 경로 판별 함수도 삭제하며 실제 authenticated cache 선택·복구는 유지한다.

공개 취소/APPLIED payload의 exact equality는 기존 테스트에 남고, 그 equality가 이미 포함하는 문자열 부재 assertion과 같은 `receipt_json`의 두 번째 비교만 제거한다. private seal/receipt 보존, durable write 순서, 저장 실패 후 같은 intent 재시도·no-repost 사례는 유지한다. writer inventory는 같은 44개 identity/guard에 새 소스 위치·해시를 결속하며 Python/PowerShell pin을 함께 갱신한다([실제 소스 검증](operations.md#s05-simplification)).

[제품·기능](README.md) · [운영·복구](operations.md) · [백로그](BACKLOG.md) · [중앙 통합](../../../Program_Spec_Hub/INTEGRATIONS.md) · [공통 용어](../../../Program_Spec_Hub/GLOSSARY.md)

최초 기준일·후속 소스 종결·증거 적용 범위는 [README 기준](README.md#기준과-판정-범위)과 같다. 다음은 2026-09-07 source-backed 정적 계약 대조이며 실제 설치본의 호출·권한·서버 설정·화면 반영 입증이 아니다. `L`은 `/logistics/api/v1`이다. 서버 상세 계약을 복제해 새 정본으로 만들지 않고 클라이언트의 소비·검증 책임을 기록한다.

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

<a id="recovery-evidence-paths"></a>
## 복구 증거 경로 · 2026-09-08 후속 소스 검증

[실제 guarded reconcile](../../label_guarded_runtime_reconcile.py)과 [initializer rehearsal 도구](../../tools/label_server_initializer_rehearsal.py)의 개발 호스트 E: 전용 조건은 Main이 승인한 이식성 교정으로 제거했다. 경로는 절대 경로로 정규화하며 client backup은 실제 live DB와 다른 기존 SQLite 파일이어야 하고, server snapshot/rehearsal은 서로 및 live DB와 달라야 한다. 새 rehearsal 산출물 세 경로는 서로 다르고 기존 파일을 덮어쓰지 않는다. SHA-256·SQLite integrity/logical preimage·초기화 no-op·source/endpoint binding·CAS·정상 인증 fence 획득·forward-only/모호한 응답 시 fail-closed 조건과 receipt schema는 유지한다. 호스트 작업 산출물을 E:에 두는 운영 지침은 제품 실행 드라이브 제한과 별개다.

계정 인계 뒤 [blackdwarfian 후속 검증](operations.md#label-black-continuation)의 VM01 guest C:에서 기존 recovery 14개와 경로 보호 7개, 합계 **21 PASS**를 확인했다. 정상 인증 acquirer/receipt와 모호한 dispatch 뒤 fail-closed, 실제 initializer subprocess 및 live/hardlink·충돌 출력 거부를 포함한다. initializer는 결정적 no-op 서버 모듈을 사용했으므로 실제 배포 서버 초기화·설치·rollback 수용은 **UNPROVEN**이다. 동결 `8b55bd3` Full의 원래 두 실패와 변경 전 593개 결과는 각 원래 범위에 보존한다.

guarded reconcile이 파일 경로로 실행하는 initializer helper는 import 탐색만으로 portable에 포함되지 않았다. [기존 builder](../../tools/build_portable_release_candidate.py)의 명시 external tool 목록에 추가하고 기존 tool closure 검사 4개를 통과했다. update public key는 [앱의 정상 consumer](../../Label_Match.py)의 기존 `UPDATE_BOOTSTRAP_MANIFEST_PUBLIC_KEY` fallback을 builder 인자로 재사용한다. 새 key·서명 절차·receipt schema·신뢰 정책은 만들지 않는다. helper 포함 portable build와 signature validation을 유지한 canonical `-PlanOnly`는 **PROVEN**이며 실제 설치·서버 수용은 **UNPROVEN**이다([운영 후속](operations.md#label-black-continuation)).

## 정상 설치의 기존 identity 복구 · 새 VM 관측

[receipt12 최종 native continuation](D:/KMTech/cold-program-material/from-E/KMTech/label-install-qualification-20260908/RECEIPT12-NATIVE.md)은 수용 source `da60e05e9bf07855f701fff328df70c10db0a094` / ZIP `782d585bf09396da8c33fbb3f1c3b24afc6a8716f73e0b4983b951a896633d6e` / manifest `829f5f2230add6f0245106c5b4dac40547f56941792c2da46dbf5318effa6111`에 결속된다. 원 assigned VM에서 동일 artifact의 정상 설치 attempt02 native0, exact retained14의 지원 `ack-reviewed` native0, 지원 current-user 제거·code-only 제거, 수용 timing06 prior-code 설치/보존, 최종 receipt12 복귀 설치03:42:16Z/native0와 readback0을 완료했다. GUI13은 기존 resident를 재사용하여 PHS2 대기·이전 완료 이력을 표시하고03:44:40Z/native0으로 정상 종료했다. 실제 정상 shutdown 뒤 Off/uptime0을 관측하고 새 boot03:46:01Z·Explorer4640/session1에서 원 SID의 resident7556이 03:46:22Z에 자동 시작한 것을 확인했다. 수동 product 시작 전 단일 resident·정상 HKCU Run·canonical task0·stop/fence 없음이며 postboot native0은 payload3,391개와 보호 파일25개, 최신 post-review backup의 업무11 tables/40 rows를 확인한다. 전체 상태 메타데이터는 40/51개 동일하며 나머지 11개 status/control/settings/log 변경을 기록했다. app_settings 필드별 차이는 미확인이고 전체 config/DB/schema/hidden-rowid 동일성이나 새 대상 설치를 주장하지 않는다. [최종 보존 readback0](D:/KMTech/cold-program-material/from-E/KMTech/label-install-qualification-20260908/RECEIPT12-FINAL-PRESERVATION02.json)은 원 receipt/metadata/spool/attempt1·audit1과 inserted10/quarantined4를 유지하고 queue11ACKED를 기록한다. 원 event CSV prefix 두 개가 동일하며 APP_START1·APP_CLOSE1만 추가됐다. 원 F1/F4/F3·shipping·로컬7 업무나 pre-F3 DB 복원을 반복하지 않았다. 첫 설치 native1/UAC 취소·지원 rollback, 별도 pre-execution policy rejection과 reader 실패는 이력으로 보존한다. [Main의 선택 범위 최종 native 수용](E:/KMTech/resume-after-input-20260908/LABEL-RECEIPT12-FINAL-NATIVE-ACCEPTED.json)은 `PROVEN_SELECTED_LABEL_FINAL_NATIVE_ACCEPTED`이며 Main msg_b42de21979ea 승인에 따른 정상 guest shutdown 뒤 [최종 VM Off/uptime0](D:/KMTech/cold-program-material/from-E/KMTech/label-install-qualification-20260908/RECEIPT12-FINAL-VM-OFF.json)을 03:58:51Z에 확인했다. 이 수용은 기존 assigned VM의 선택된 Label 범위에 한정한다. 전체 제품 Ready는 **0/6**이며 여섯 프로그램의 현재 조합 판정과 다음 S05 source workspace 결정은 Main 소유다.

[receipt12 공개 이벤트·strict RAW 교정](D:/KMTech/cold-program-material/from-E/KMTech/label-install-qualification-20260908/RECEIPT12-SOURCE.md)은 `SET_CANCELLED`를 공개 `set_id`만 기록하도록 제한하고 `SEALED_TRANSFER_EXCHANGE_APPLIED`에서 `old_seal_qr_payload`·`new_seal_qr_payload`만 제거한다. 공개 set/intent/receipt/bundle·멤버 목록·version과 private 취소·복구·apply 상태는 유지한다. strict RAW allowlist에 정확히 두 이름만 추가하며 receipt/identity/hash/bytes/행·event 합계, `OBSERVED/RAW_EVIDENCE_ONLY/NOT_PROJECTED/NO_STAGE1_REDUCER`, quarantine/errors=0 및 runtime fence를 그대로 요구한다. 저장된 bytes와 근거 hash를 확인하여 focused60·inventory4 PASS와 parent causal6 FAIL을 재실행 없이 재사용했다. 동일44 writer identity/guard의 pin은 `507da9c952a88649cd06c090f45fb6e1bb5129de12260a747741b744ccc1d4cb`다. receipt11 `7c4abfa`/ZIP2094ba6a를 parent/recovery로 보존하고 단 한 번 고정·빌드한 수용 source12는 `da60e05e9bf07855f701fff328df70c10db0a094`다. Web05 normalizer의 기존 공개 형식을 사용하며 reducer/비밀 검증을 변경하지 않는다. [native 완료 근거](D:/KMTech/cold-program-material/from-E/KMTech/label-install-qualification-20260908/RECEIPT12-NATIVE.md)와 [최종 source 소유권 지도](D:/KMTech/cold-program-material/from-E/KMTech/label-install-qualification-20260908/RECEIPT12-SOURCE-OWNERSHIP.md)를 연결한다. 이 문서의 최종 native 기록만 갱신했으며 고정 artifact는 재빌드하지 않았다. Ready **0/6**이다.

<a id="current-user-task-reuse"></a>
<a id="current-user-resident-relay"></a>
## 정상 사용자 시작·상주 동기화와 과거 task 이행

사용자 승인 D11과 [현행 소스](operations.md#resident10-alignment)는 정확한 HKCU Run `KMTech.LabelMatch.Relay`와 기존 `user_relay`를 유일한 정상 persistence 경로로 사용한다. GUI와 독립적인 cycle/network retry, fresh owner 생존 확인, instance lease·stop marker·writer fence 및 identity/profile/queue/spool/receipt 계약은 유지한다. onboarding의 예약 작업 creator/필수 성공, 별도 scheduled one-shot main/parser/활성 mode를 제거했다. 옛 argv는 GUI로 흘러가지 않고 exit2로 거부한다. task10의 exact Running `Apply/REUSED` 교정은 폐기한 별도 설계의 이력이다.

[current-user task helper](../../current_user_scheduled_task.py)는 과거 task의 exact ownership·현재 사용자 name/SAM/SID·Interactive/Limited·canonical executable/argv/cwd/log·기존 단일 TimeTrigger/PT1M·IgnoreNew/StartWhenAvailable/PT2M을 확인하는 제거 전용이다. 없는 task는 아무것도 바꾸지 않는다. 있는 task는 Disable 후 Schedule.Service `GetInstances(0)`으로 자연 종료를 관측하고, zero instances 뒤에도 fresh exact disabled 정의를 확인한 다음 제거·부재를 확인한다. unknown instance count·drift·timeout은 실패하며 force-stop/재등록/재활성화하지 않는다. marker 해제는 성공한 HKCU 등록과 과거 task 부재 뒤 기존 admission/CAS 안에서만 이루어진다.

정상 사용자 제거는 Run 제거 → resident 정상 stop/ABSENT 증명 → 과거 current-user task 제거 순서다. UNKNOWN을 FAILED 또는 ABSENT로 바꾸지 않으며 data를 보존한다. [canonical installer](../../INSTALL_CANONICAL_PORTABLE.ps1)는 기존 버전 rollback의 실제 task snapshot 소비자만 유지하고 후보 onboarding에는 `scheduled_task_remove`를 위임한다. [elevated bootstrap](../../INSTALL_THIS_PC.ps1)의 과거 SYSTEM task uninstall 소비자는 exact root/name·SYSTEM ServiceAccount·알려진 전체 launcher argv·빈 cwd·정규화한 전체 XML 정의를 확인하며 Disable→자연 종료→최종 정의 재확인→제거/부재만 수행한다. 경로 부분 문자열 소유권이나 Stop-ScheduledTask는 사용하지 않는다.

[분리된 영향 검증136PASS](D:/KMTech/cold-program-material/from-E/KMTech/label-install-qualification-20260908/RESIDENT-ALIGNMENT.md)는 재사용한 source 근거다. 단일 GUI/resident 재사용과 정상 종료의 선행 수용 근거에 더해, [최종 동일 receipt12](D:/KMTech/cold-program-material/from-E/KMTech/label-install-qualification-20260908/RECEIPT12-NATIVE.md)의 기본 GUI 시작·정상 종료0, 실제 coldboot 자동 resident 시작, 지원 제거·prior-code recovery·복귀 설치·업무/보호 상태 보존을 완료했다. 원 F4와 PHS09 설치 뒤 GUI08 실패를 보존하며 source PASS를 전체 제품 Ready로 대체하지 않는다.

resident10 c6e72592의 [실제 설치 및 기본 GUI09 시작](operations.md#resident10-alignment)은 native0·READY/REUSED·기존 단일 resident 유지·task0으로 확인했다. 현재-user migration은 실제 ABSENT/ALREADY_ABSENT였으며, 역사적 Running task 제거의23/38 PASS fixture를 실제 해당 task native 제거로 확대하지 않는다. 동일 원 command는 ACKED/attempt3·서버 receipt로 복구됐고 원 로컬7 custody 묶음은 보존됐다. 중앙 oldF1 lease의 별도 EXPIRED_UNRECONCILED 관측은 [현재 증거 범위](operations.md#resident10-alignment)로 기록하며 fence2/unconsumed/unreconciled를 유지한다. 후속 원 새 seal VERIFIED/local APPLIED·F3 ACKED1/completion1은 실제 근거로 확인했다. 현재 receipt11의 설치 및6행 실제 transport와 남은 최종 lifecycle은 위 현재 근거로 구별한다.

<a id="states"></a>
## 상태와 확정 경계

**중앙 명령 ACK와 producer ACK는 서로를 대체하지 않는다.** [완료 정책](../../DIRECT_SYNC_DATA_PLATFORM_NOTES.md)의 원칙과 [DataManager/완료 함수](../../Label_Match.py), [outbox](../../package_logistics.py), [producer receipt 검증](../../direct_sync_push.py)을 함께 따른다.

| 관측 지점 | 확정 조건 | 아직 입증하지 않는 것 |
| --- | --- | --- |
| 접수 보존 | encrypted/authenticated intent의 durable commit | 바코드 유효성, 포장 완료, 중앙 재고 변경 |
| 로컬 포장 완료 | current-state 보존, intent, `TRAY_COMPLETE` flush/fsync, 완료 marker·lease transaction | 중앙 PACKAGE 명령 COMMITTED |
| 중앙 명령 확정 | 같은 authority/key/command에 대한 검증된 logistics `status=COMMITTED` receipt | CSV ingest·집계·화면 최신성 |
| producer 수신·투영 | source identity·행 합계가 맞는 `accepted/committed`와 업무 `COMPLETE`; exact nonprojecting lifecycle은 아래 C-05의 `RAW_LEGITIMATE` 조건 | raw lifecycle은 업무 투영·수량을 증명하지 않음; 명령 원장 ACK·화면 표시도 별도 |
| 소비 화면 | 해당 API의 filter/flag/source readiness 아래 실제 값·표시를 관찰 | 실물 랩핑·인쇄·다른 화면의 최신성 |

`package_command_outbox.status`는 전달 상태(`PENDING`, `SENDING`, `ACKED`, `CONFLICT`)이며 `local_completion_committed`가 로컬 완료 축이다. `PREPARED`는 `PENDING + marker=0`의 설명용 용어로 DB enum이 아니다. marker=0은 status와 무관하게 로컬 완료가 아니며, marker=1의 중앙 conflict는 기존 로컬 완료를 보존하고 `OPERATOR_REVIEW`/사후 검토 사건으로 투영한다. 근거: [mark_local_completion_committed, mark_conflict](../../package_logistics.py), [완료 정책](../../DIRECT_SYNC_DATA_PLATFORM_NOTES.md).

접수 저장소는 `CAPTURED_UNVERIFIED → VALIDATING` 이후 `VALIDATED/READY_TO_SUBMIT`, `SUBMITTING`, `ACKED/LOCAL_EFFECT_PENDING`, `COMPLETED`를 구분한다. `RETRY_WAIT_VALIDATION`, `WAITING_DEPENDENCY`, `RECONCILE_PENDING_VALIDATION`, `RETRY_WAIT_SUBMIT`, `RECONCILE_PENDING_SUBMIT`, `BLOCKED_INVALID`, `OPERATOR_REVIEW`가 예외를 보존한다. `CANCELLED/SUPERSEDED`는 종결-미완료다. 모든 입력이 이 상태들을 반드시 한 번씩 순서대로 통과한다는 뜻은 아니다. 상태·전이의 상세 원본은 [deferred_intent_capture](../../deferred_intent_capture.py)와 [fixture 계약](../../tests/fixtures/deferred_intent_contract/CONTRACT.md)이다. 이 fixture는 2026-09-06 재구성본이며 과거 이름의 MEASURED 시나리오를 현재 native 측정으로 승격하지 않는다.

<a id="f1-capture-cancellation"></a>
### F1 현재 접수 취소 · cancel07 소스 계약

Main의 `msg_8450ebdcf336` 감사는 기존 Label의 정상 F1을 현재의 정확한 owned·unsubmitted 접수에 연결하도록 승인했다. producer/install/source host/manifest/scope, intent/set, 보존된 물리 입력과 seal, row version·fence·validation generation을 확인하고 한 SQLite transaction에서 `CANCELLED`, `TC_CANCEL`, `OPERATOR_CANCELLED_LOCAL_CAPTURE`를 기록한 뒤 UI/cache를 비운다. `VALIDATED → CANCELLED`는 정상 초기화 때 기존 DB trigger까지 transaction 안에서 교체한다. 이미 CANCELLED인 같은 접수의 반복은 추가 취소 audit를 만들지 않는다.

확정되어 로컬에 보존된 미소비 PREFETCHED lease의 존재·만료는 로컬 접수 취소를 일괄 금지하지 않는다. 해당 lease, ACTIVE issue attempt, 기존 payload·검증 이력·서버 reservation·receipt는 바꾸지 않으며 취소는 포장 완료·중앙 lease release·재고 반환이 아니다. 과거 mutation은 정확한 request/hash/idempotency와 검증된 발급 결과를 보존 lease에 결속해야 하고, actual pending/UNKNOWN·live claim·package outbox·미완료 F4/F5·UI in-flight guard는 유지한다. 기존 fingerprint/resource별 lease 선택과 같은 원본의 F4 차단도 그대로다. 재구성 rw13 fixture의 별도 foreground marker/credential을 Label 업무 요구로 추가하지 않는다.

commit 뒤 cache 삭제 전 crash에서는 exact CANCELLED 접수의 stale cache를 복원하지 않고, 늦은 VALIDATED callback도 현재 DB state를 다시 확인해 materialize하지 않는다. 저장 실패·identity/CAS 불일치 때 화면/접수는 보존한다. [host 회귀와 구분한 native 범위](operations.md#f1-cancel07-qualification)는 별도 판정이며 이 계약은 실제 중앙 reservation 해제를 입증하지 않는다.



실제 원래 세트의 cancel07 수용은 정상 F1 한 번의 VALIDATED/v7 → CANCELLED/v8/TC_CANCEL 1건과 정상 GUI 재시작 후 미복원을 확인했다. [local full-row/payload 보존](D:/KMTech/cold-program-material/from-E/KMTech/label-install-qualification-20260908/CANCEL07-CUSTODY-AFTER-RESTART.json) 및 [중앙 10테이블/19기록 무변경](E:/KMTech/web-integration-20260908/label-original-f1-central-comparison-01.json)은 원래 lease/ACTIVE attempt/reservation/receipt 보존 근거다. 이 결과를 새 case04 F4/F3·중앙 포장 확정 또는 최종 artifact lifecycle PASS로 확장하지 않는다([실제 환경·관측 한계](operations.md#f1-cancel07-native)).

<a id="quantities"></a>
## 수량·시각·로컬 파일

| 값 | 단위·모집단·포함/제외 | 시간·집계·중복 |
| --- | --- | --- |
| source `member_count` | 현재 유효 TRANSFER/work-group 제품 구성원 수. PHS2 한 번의 스캔 수와 다름; `INHERIT_ALL`은 그 집합 전체 | 현재 source snapshot/version 기준. unit와 barcode hash를 각각 대조 |
| QA 표본 3개 | 명시 호환 경로의 검증 표본. 포장 전체 멤버십 수나 중앙 표준 필수 입력 수가 아님 | 표준 PHS2 입력은 1회; 표본을 제품 총량으로 합산하지 않음 |
| F4 교체 목록 | 1쌍 이상 실제 대상 멤버 수 이내. 3쌍 이상은 추가 capability 필요 | 명시적 단일 제출·중앙 원자 처리·같은 key 재생. 전체 구성 수 보존 |
| 포장 세트 | `PACKAGING_SET_COUNT`, 웹 `total_sets_completed`. 포장 완료 세트 수이며 제품 개수와 환산하지 않음 | 취소·부분 제출 등을 제외하는 projection 규칙. 원본 PHS2/최종 라벨은 제품 barcode에서 제외 |
| CSV row / accepted row | event 전달·투영 처리 건수. 포장 세트나 제품 개수와 다름 | 중복·quarantine·처리 행 합계와 source identity로 ACK 검증 |

근거: [package draft/command](../../package_logistics.py), [서버 Label event normalize](../../../WorkerAnalysisGUI-web/common_projection_sync.py), [포장 projection](../../../WorkerAnalysisGUI-web/common_projection.py)의 `_label_match_payload_passed` 및 포장·취소 집계, [dashboard_standard.source.js](../../../WorkerAnalysisGUI-web/static/dashboard_standard.source.js) `total_sets_completed` 표시. `Pcs/EA/piece`의 전 제품 공통 환산은 미확정이며 [LM-B06](BACKLOG.md#lm-b06)·[공통 용어](../../../Program_Spec_Hub/GLOSSARY.md)에서 관리한다.

`DataManager.log_event`는 timezone 없는 로컬 `datetime.now().isoformat()`을 기록한다. 서버 Label transport 시간대는 `Asia/Seoul`이며 lease `operation_completed_at`은 UTC `Z`다. 생산일, 포장 업무일, event 발생, 수신, projection, 화면 조회 시각은 별개다. 날짜 변경은 outbox 만료 기준이 아니다. 근거: [DataManager/lease](../../Label_Match.py), [서버 시간대·포장-출고 집계](../../../WorkerAnalysisGUI-web/common_projection.py). PC 시계차 허용치·업무일 cutoff·표시 지연 목표는 [LM-B02](BACKLOG.md#lm-b02)의 미정 요구다.

로컬 emitter CSV는 UTF-8 BOM(`utf-8-sig`), 헤더 `timestamp,worker_name,event,details`이며 details는 JSON이다. 완료 등 durable event에 flush/fsync를 적용하고 writer 오류를 `flush/close`에서 반환한다. 현재 세트 JSON은 임시 파일 flush/fsync 후 `os.replace`한다. 이 파일과 SQLite outbox·lease가 하나의 파일 transaction인 것은 아니므로 복구 절차로 경계를 조정한다([DataManager](../../Label_Match.py), [완료 정책](../../DIRECT_SYNC_DATA_PLATFORM_NOTES.md)). 일부 fixture의 `event_type/detail` 헤더는 emitter 원본 byte가 아니다. 서버 decoder의 alias 호환과 실제 emitter 동일성을 분리한다.

`event_stream_policy.LOCAL_ONLY_EVENT_TYPES`는 별도 로컬 이벤트 파일로 나뉜다. `PHS_LABEL_ACTIVE_RESOLVED`, `EXACT_RESCAN_STARTED/OK/COMPLETED` 같은 로컬 전용 event를 원격 실적으로 가정하지 않는다. `CONTRACT_CANDIDATE_EVENT_TYPES`의 이름만으로 전체 전송 계약 수용을 증명하지 않는다([이벤트 정책](../../event_stream_policy.py), [DataManager._get_log_filepath_for_item](../../Label_Match.py)).

로컬 GUI writer의 기존 저장 위치를 보존하기 위해 [guard `resolve_data_scope`](../../label_match_single_instance.py)는 앱과 같은 nonempty `custom_save_path` → `LABEL_MATCH_SAVE_DIR` → ProgramData 순서로 소스 교정했다. null/빈 custom은 fallback하며 데이터 이동·schema/업무 identity 변경은 없다. 이름은 앞뒤 공백 제거 후 기존 `ntpath.normcase(normpath(path))`의 UTF-8 SHA-256 앞 32자리와 `Global\KMTech.LabelMatch.`로 만들고, callback 종료까지 handle을 보유한다. [SaveRoot13 실제 13 PASS/39 ordered phase PASS](operations.md#saveroot13-evidence)는 custom 우선 선택, missing/empty/whitespace/null × env 유무의 실제 writer resolver 대조, 단일 process의 native mutex 중복 callback 제외·해제 후 재진입을 입증한다. 실제 파일 alias/relative 경로의 동일성이나 settings 생성·변경 race를 해결한 것은 아니다. [onboarding의 guard 이전 ledger 선택](operations.md#storage-root-residual)은 여전히 OPEN이며, relay의 기존 custom CSV 발견 경로는 아래 C-05와 [한정 26 PASS](operations.md#relay-custom-root-evidence)로 좁게 교정했다. 두 GUI writer·packaged/source·pending identity 복구는 [LM-B10](BACKLOG.md#lm-b10)의 별도 미입증 수용 범위이며 과거 Modules3 PASS를 이 교정에 확장하지 않는다.

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

- UI lane의 terminal callback은 아직 BUSY인 상태에서 실행된다. 정상 scan admission은 lane이 task ownership을 해제한 `on_idle`에서 다시 계산하고 기존 caller의 `on_idle`도 유지한다. initializing/history/notice/staged PHS2/close/query/exchange/fence 조건을 직접 풀지 않는다. 실제 `8425fe1` GUI에서 F3 후 회색 입력 필드가 남은 현상을 source callback 순서와 [causal 3 FAIL → 기존 lane/action-gate 97 PASS](D:/KMTech/cold-program-material/from-E/KMTech/label-install-qualification-20260908/idle-admission-green.xml)로 연결했으며, 별도 successor `eb79e519`의 [초기 native 입력](D:/KMTech/cold-program-material/from-E/KMTech/label-install-qualification-20260908/GUI04-F4-INPUT-PERSISTENCE.json)은 전체 139자 즉시/12초 후 일치다. 새 코드의 F3 후 입력 재개 수용은 아직 미실행이다.

- API: `GET L/bundles/resolve`, `bundle_role=PACKAGE_SOURCE`, `input_tag_id`(ITG), 품목·scope, `input_tag_label_id`(LBL)와 `input_tag_hash_prefix`(HSH). 물리 label 표시 문자열을 독립 조회 권위로 사용하지 않는다.
- 검증: LBL/HSH는 함께 공급하며 ITG가 필요하다. HSH는 16자리 hex다. 선택적인 barcode membership hash와 member count filter도 쌍으로 검증한다. 응답의 unit/barcode 매핑·membership hash·entity version·work-group topology가 현재 작업에 결속된다.
- 효과/오류: ordinary PHS2 조회는 `PACKAGE_SOURCE`를 resolve하며 새 exclusive `CREATE_PACKAGE` lease를 발급하지 않는다. 재사용 가능한 같은 lease가 없을 때 ordinary F3가 발급받는다. 조회 자체는 포장 확정이 아니다. 잘못된 라벨·불완전 증거·구성 변화는 fail-closed; 최신 snapshot과 operation lease의 일치는 F3에서 다시 필요하다. 읽기 조회에 명령 receipt/cursor를 가정하지 않는다.
- 양쪽 근거: [resolve_transfer_bundle/resolve_package_source_projection](../../package_logistics.py), [resolve_bundle](../../../WorkerAnalysisGUI-web/blueprints/logistics/api.py). [LM-03](README.md#lm-03), [LM-B05](BACKLOG.md#lm-b05).

<a id="c-03"></a>
## C-03 F4 교체·재봉인

복원된 현재 세트에 차단 중인 교체 요청이 있으면 주 화면 안내도 포장 완료 보류 상태를 나타내야 한다. 저장 command가 있는 review의 읽기 전용 F4 목록과 ACKED 뒤 새 봉인 확인 경로는 유지하되, F3·새 스캔·취소나 재시도 권한을 새로 열지 않는다. 안내와 버튼은 같은 기존 요청 관측을 사용하며 이력 조회·로딩·오류·완료·busy·종료의 기존 제한을 존중한다. [2026-09-11 실제 불일치와 수정 상태](operations.md#restored-exchange-guidance-20260911)를 요구와 검증 범위로 구분한다.

F4 초안은 비어 있지 않은 정확한 barcode 목록과 seal QT의 일치를 항상 요구한다. 표준 PHS2의 accepted work-group members가 없으면 팝업/intent 전에 거부한다. scalar count/hash만 남는 명시적 레거시 direct-seal QR은 기존 F4 worker preflight에서 현재 target bundle·active seal을 검증해 얻은 barcode 목록을 사용한다. 이 조회는 Tk 밖에서 수행하며 lease/pending gate와 제출 시 fresh server 검증을 유지한다.

실제 cancel07 case04 첫 실패와 원 입력은 보존한다. `source_iin`/resolver `inbound_iin`은 현재·frozen accounting IIN이며 immutable origin과 구분한다. 각 bundle의 current member binding은 그 bundle의 accounting IIN과 일치해야 하지만 donor와 target 사이 equality는 필요하지 않다. 수용된 중앙 sealed replacement는 donor→target accounting 이동/rebind와 origin·receipt·movement·membership 계보 보존을 같은 transaction에서 수행한다. Main/Web의 실제 필드·서버 source 비교에 따라 iin08은 클라이언트 equality만 제거한다. 다른 권한·원장·품목·UOM, 다품목 donor, stale seal/version과 불완전 receipt guard는 유지한다.

지원 복구는 `OPERATOR_REVIEW` + exact `SEALED_TRANSFER_EXCHANGE_ERROR` + exact legacy detail `replacement good must have the same lot/item/uom/ledger identity`, seal/local-apply 모두 `PENDING`, command_id/json/hash·receipt_json·new_seal_qr_payload·seal_verified_at·local_apply_receipt_json 모두 `NULL`인 pre-command row만 기존 normal drain에서 다시 검증한다. 동일 intent/input/hash/created_at/attempt 이력을 유지하며 capability·target/seal·donor를 GET/검증한 뒤 기존 atomic bind가 command를 먼저 저장하고 POST한다. 일반 error/NULL command만으로 review를 열거나 상태를 초기화하지 않는다. durable command review의 기본 동작은 exact receipt 조회이며 아래의 한정된 precommit 호환 복구만 예외다. 원래 4개 입력을 다시 스캔하는 복구가 아니며, corrected client 시작 자체가 복구 command를 보낼 수 있어 공유 서버·후보 수용과 현재 정상 grant가 먼저 필요하다. [소스·48 PASS·실제 pause와 한계](D:/KMTech/cold-program-material/from-E/KMTech/label-install-qualification-20260908/IIN08-SOURCE-RECOVERY.md)는 native F4 성공이나 새 seal/F3를 대신하지 않는다.


phs09의 durable-command 예외는 `OPERATOR_REVIEW`, exact code `PHS_REPLACEMENT_INSTRUCTION_CONFLICT`, 전체 저장 detail `PHS_REPLACEMENT_INSTRUCTION_CONFLICT: The PHS work group differs from its completed plan instruction.`, seal/local-apply `PENDING`, receipt/new-seal/verified-at/local-apply-receipt 모두 `NULL`에만 적용한다. command JSON은 canonical encoding과 SHA256이 저장값에 일치하고 command key는 저장 command_id 및 원 intent_hash에서 파생한 key와 같아야 한다. 기존 `get_receipt`가 **HTTP404 + RECEIPT_NOT_FOUND + committed=false**를 반환한 경우만 authoritative 부재다. generic404·실패·unknown·None·누락된 lookup은 원 review row를 보존한다. receipt가 있으면 먼저 기존 exact receipt 검증을 수행하며 POST하지 않는다.

authoritative 부재 뒤 기존 capability·target/seal·donor 검증으로 계산한 command가 저장 JSON과 완전히 같아야 원 command를 같은 key로 호출한다. fresh 조회 실패나 변경은 원 review를 유지하며 command 재생성/재bind·상태 초기화·새 intent·입력 재스캔은 없다. 이 호환 호출에서 terminal API 거부(400/403/409/412/422)가 다시 오면 실제 code/detail을 보존한 `SEALED_TRANSFER_EXCHANGE_RECOVERY_REJECTED` review로 기록해 자동 예외를 반복하지 않는다. 이 자동 호환 호출의 전송/ACK 불확정은 기존 RETRY_WAIT·exact receipt 복구를 따른다. Web은 frozen26의 해당 guard가 atomic transaction 내부, command 저장 전이며 예외 시 rollback됨을 입증했다(`msg_d2fdcd19526e`); 이 증거를 timeout/다른 오류에 확대하지 않는다. [실제 failure와 host79PASS](D:/KMTech/cold-program-material/from-E/KMTech/label-install-qualification-20260908/PHS09-SOURCE-RECOVERY.md)는 paired server/client 수용·현재 grant/custody가 필요한 실제 복구의 성공을 뜻하지 않는다.

2026-09-10의 명시적 작업자 복구는 기존 `attempt(intent_id, *, operator_retry=False)`의 기본값을 유지한다. `operator_retry=True`는 위 전체 instruction-conflict detail을 정확히 감싼 `SEALED_TRANSFER_EXCHANGE_RECOVERY_REJECTED` durable review이고 seal/local PENDING·모든 결과 필드 NULL일 때만 후보가 된다. 기존 F4 창은 저장 목록을 읽기 전용으로 보여 주고 `같은 교체 재시도`의 기본 아니요 확인에서 실제 목록 건수와 관리자 조치 확인을 알린다. 현재 세트/원 입력, 기존 lane·outbox thread·lease/writer/grant gate를 통과한 한 번의 명시적 요청만 원 intent로 보낸다. 새 intent/prepare/rebind·입력 재스캔·저장 상태 초기화는 없다.

이 명시적 경로도 exact receipt를 먼저 소비하며, 위 authoritative 부재와 저장 JSON/hash/key/scope 검증, fresh command 완전 일치 뒤 POST 직전에 같은 row를 다시 확인한다. 진행 중 F4 lane에서는 별도 daemon의 exchange drain을 생략하며 다른 package 작업을 바꾸지 않는다. 반복된 terminal 거부는 review로 유지하고, 명시적 요청의 전송/5xx/예상 밖 불확정은 `SEALED_TRANSFER_EXCHANGE_RETRY_UNCERTAIN` review에 남겨 뒤의 자동 drain이 POST를 만들지 못하게 한다. 이후 exact receipt는 자동으로 확인할 수 있으며 늦은 오류는 이미 저장된 ACKED를 덮어쓰지 않는다. [소스 검증·실제 실패 보존·수용 한계](operations.md#f4-explicit-retry-20260910)는 실제 retry/새 seal/F3 성공과 구분한다.

명시적 guard 거부는 durable row/error/attempt를 바꾸지 않고 반환 객체의 일시적인 `operator_retry_refusal`로 이유를 표시한다. 실패한 조회 동안 exact ACK가 기록되었다면 최신 ACK를 반환하며 거부 안내로 덮지 않는다. 저장 목록의 안내는 편집 초안과 구분해 닫아도 요청·목록이 남는다고 알린다. 저장 command key가 있는 `OPERATOR_REVIEW`는 오류 종류와 무관하게 같은 F4 목록을 읽기 전용으로 다시 열 수 있으며, `operator_retry_available`만 명시적 재시도 버튼을 활성화한다. `RETRY_UNCERTAIN`과 다른 비대상 review는 재전송 버튼·자동 POST·로컬 DB 초기화를 허용하지 않는다. receipt가 확인되지 않은 불확정 결과는 여전히 해결되지 않은 상태이며 관리자 DB 조작을 지원 복구로 안내하지 않는다. prepare 전후 결과를 알 수 없으면 접수되었을 가능성으로 안내하며 접수를 단정하지 않는다. 기존 lease gate의 명시적 재시도 거부는 일시 보류로 표시하고 다음 작업자 요청에서 같은 gate를 다시 확인한다.

- 2026-09-08 현행 `eb79e519`의 정상 START가 F3 전에 CREATE_PACKAGE lease를 발급해 [F4 수량 전 경고](D:/KMTech/cold-program-material/from-E/KMTech/label-install-qualification-20260908/F4-QUANTITY-DIALOG.json)를 만든 결함을 확인했다. Main의 승인된 교정은 **초기 PHS2 검증·materialization을 읽기 전용 source 확인으로 끝내고 실제 F3가 lease를 발급**하게 하는 것이다. [후속 source 단위](D:/KMTech/cold-program-material/from-E/KMTech/label-install-qualification-20260908/TIMING06-SOURCE-CHECK.json)는 F4의 PREFETCHED/LOCAL_COMPLETED·ACTIVE issue attempt gate와 전체 single-transfer·전자 seal 확인·CAS·수량 보존을 유지한다. 기존 발급 fence2는 여전히 포장 소유이며 만료만으로 해제하거나 로컬 status를 지우지 않는다. 새 중앙 API를 이 교정의 전제로 요구하지 않으며 Main이 합법적인 별도 기존 대상 또는 지원 관리 복구를 배정한다. 후속 source의 실제 F4 성공은 아직 미실행이다.

- API: `GET L/replacements/good-source/resolve`, `POST L/transfers/{id}/members/replace-and-reseal`; command `REPLACE_SEALED_TRANSFER_MEMBERS`, capability `sealed_transfer_member_replacement_v1`.
- payload: `target_bundle_id`, `damage_bundle_id`, target evidence, `pairs`, 예상 대상/donor/damage version. 쌍 수는 양의 정수이며 실제 대상 membership 수를 넘지 않는다. 같은 권한·원장·품목·UOM과 bundle 내부 accounting binding, 활성 제품 1개 donor PHS를 검사한다. 예상 damage version 0도 명령에 결속한다.
- 호환 capability: 기존 `sealed_transfer_member_replacement_v1`와 `max_pairs: 2`를 유지한다. 3쌍 이상에는 `capability_ids`의 `sealed_transfer_member_replacement_target_members_v1` 및 같은 이름의 object가 추가로 필요하며, `enabled: true`, `base_capability: sealed_transfer_member_replacement_v1`, `pair_limit_basis: TARGET_MEMBER_COUNT`, 정수 `min_pairs: 1`을 모두 확인한다. receipt/command/QR schema는 v1을 유지한다.
- 목록·접수 경계: 수량 대화상자와 개수 도달 자동 제출을 없애고 old→new 목록의 수정·삭제·입력 취소 및 명시적 단일 제출을 제공한다. 비어 있거나 미완성인 입력, 중복·양쪽 교차 barcode, 현재 대상 밖 old 또는 대상 안 new, 실제 대상 수 초과는 차단한다. 3쌍 이상 미지원은 `store.prepare` 전에 거부해 durable intent 없이 목록을 보존한다. `prepare` 이후 첫 `attempt.load` 예외까지 모두 같은 intent와 잠긴 목록을 유지하며 새 identity나 일반 오류에 의한 재제출을 만들지 않는다.
- 저장소 호환: 기존 initializer의 정상 transaction에서 obsolete `pair_count BETWEEN 1 AND 2` CHECK만 양수 조건으로 옮긴다. ordered rowid·모든 row 값/JSON bytes·기존 index/trigger SQL을 보존하고 실패 시 rollback하며 재시작은 no-op이다. 외부 view/FK dependency는 원본 보존 상태로 거부한다. live DB 수동 변경·reset은 수행하지 않는다([소스 검증 범위](operations.md#f4-editable-list)).
- 원자 효과: 대상·donor·damage CAS, 손상품 `PROCESS_DAMAGE_HOLD` 이동, 양품 TRANSFER 편입, 이전 전자 seal 무효화·새 revision 발급이 한 중앙 transaction이다. 이 중앙 성공과 앱의 QR 확인·로컬 반영은 별도 경계다.
- 중복/복구: 같은 intent hash key와 저장 command를 사용하며 receipt의 매핑·잔량·damage membership·version까지 비교한다. ACK 유실 또는 로컬 적용 실패는 intent/receipt에서 복구한다. 새 전자 QR 확인 전 제한은 앱 gate가 소유한다.
- 양쪽 근거: [교체 workflow](../../sealed_transfer_exchange.py) `_build_command/attempt`, [서버 replace_sealed_transfer_members](../../../WorkerAnalysisGUI-web/logistics_ledger/service.py), [앱 QR 확인](../../Label_Match.py), [교체 정책](../MEMBER_EXCHANGE_POLICY.md). 작업자 정본의 후속 QR 안내 갱신은 [LM-B01](BACKLOG.md#lm-b01)에 남는다.

<a id="c-04"></a>
## C-04 F3 포장 명령·lease·outbox

- 2026-09-12 회귀 정합: 초기 source의 `VALIDATED`는 lease 발급·F3 완료 신호가 아니다. 현재 F3의 future-issued 응답은 서명 검증 뒤에도 `issued_at` 전에는 거부하고 동일 durable issue key로만 다시 요청한다. expiry 경계·서명·단말/source binding·snapshot hash·artifact fence 오류는 package enqueue와 완료를 차단한다. 보존된 과거 2단계 plan에서 signed clock 대기 및 definite service 실패는 원 요청을 유지하지만 unknown issue는 `RECONCILE_PENDING_VALIDATION`, API 오류 문구만의 clock 주장은 `BLOCKED_INVALID`다. [32개 회귀와 기존38개 계약 검사](operations.md#clock-recovery-tests-20260912)는 host 증거이며 이 계약의 제품 변경은 없다.
- 초기 검증과 권한 시점: 새 `LABEL_PACKAGE_SOURCE` plan은 `label-package-source/READ_ONLY` 1단계이며 source membership·authority/version 증거를 VALIDATED로 동결한다. 초기 검증에서 CREATE_PACKAGE 발급·mutation attempt를 만들지 않는다. 이 읽기 증거의 expiry는 기존 validation claim의 만료시각(기본 claim 300초)에 결속하고, bounded expiry·claim/fence·snapshot 검증을 유지한다. 과거 2단계 plan의 정확한 step 정의·signed evidence·미확정 mutation 복구는 보존하며 새 기본 plan으로 발급하지 않는다. 실제 F3는 lease가 없으면 기존 `_acquire_operation_lease`를 호출하고 expected snapshot·서명·현재 set·fence·local marker를 원래 순서로 검증한다. [111 deferred](D:/KMTech/cold-program-material/from-E/KMTech/label-install-qualification-20260908/timing06-deferred-green03.xml), [218 lane/package/lease](D:/KMTech/cold-program-material/from-E/KMTech/label-install-qualification-20260908/timing06-boundaries.xml), [writer static 4](D:/KMTech/cold-program-material/from-E/KMTech/label-install-qualification-20260908/timing06-inventory.xml)는 host 회귀이며 새 설치 수용은 별도다.

- 실제 적용 범위: Main이 수용한 timing06의 [정상 설치 native 0/전체 payload 재해시](D:/KMTech/cold-program-material/from-E/KMTech/label-install-qualification-20260908/REINSTALL-TIMING06-READBACK.json)는 코드 배치와 기존 상태 지속성을 입증한다. [공유 중지 뒤 원래 F4](D:/KMTech/cold-program-material/from-E/KMTech/label-install-qualification-20260908/SHARED-PAUSE-TIMING06-BUSINESS.json)는 expiry 이후에도 기존 fence2 PREFETCHED/ACTIVE와 원본 PHS2를 보존했고 package/exchange 명령을 만들지 않았다. 초기 시점 교정이나 새 설치는 기존 lease의 해제·권한 변경·terminal receipt를 뜻하지 않는다. 제한된 기존 ADMIN machine 권한이 없다는 의존을 보존하며 이를 위한 credential/permission 변경·강제 retirement·새 취소 체계는 실행하지 않았다. 새 소스의 GUI 업무 수용과 Main 소유 정상 후속 fixture는 별도 미실행이다.

- API: `POST L/operation-leases/issue`, `POST L/packages`, `GET L/receipts/{scope}/{key}`. 기본 membership mode는 `INHERIT_ALL`; 정확한 work-group 경로와 호환 `EXACT_RESCAN`의 command 생성은 [package_logistics](../../package_logistics.py)에 남아 있다.
- lease: current set에 attach된 `PREFETCHED` lease의 ID·snapshot hash·fence가 검증 결과와 일치해야 한다. UTC 완료시각을 결속해 로컬 marker와 lease를 같은 SQLite transaction으로 확정한다. 서버 승인 없이 새 offline lease를 임의 발급하지 않는다.
- 기존 완료 재사용·복구: `_label_match_local_completion_event_exists`는 발견한 같은 set의 `TRAY_COMPLETE`를 writer flush/barrier 뒤 `r+`로 다시 열어 같은 descriptor에서 재대조·fsync한 뒤에만 durable event로 재사용한다. sticky writer 오류·재대조·동기화 실패는 `PackageLogisticsError`로 전파해 중복 append와 marker 승격을 막는다. ACKED orphan의 동기화 실패는 같은 command/set/scans를 복원·보존하고 기존 입력/재시도 차단 안내를 적용한다. 유효한 orphan의 lease가 있으면 원래 완료시각과 함께 marker transaction에 전달한다.
- 근거·한계: [M3/N6 사례·소스 매핑](operations.md#residual-source-evidence)의 M3 변경 13사례는 실제 저장 거부·자정/중단·materializer 및 worker/Tk 적용 경계, 별도 N6 6사례는 기존 CSV 재동기화·거부·writer 오류·재대조를 입증한다. N6의 역사 ACKED/marker0 row는 lease 없이 로컬 seed한 것이며 현재 `claim_next`는 marker1을 요구한다. 따라서 서버가 그 역사 상태를 생성했거나 real lease-bearing orphan의 동기화/marker 실패가 하나의 통합 재시작 경로로 통과했다는 증거는 **UNPROVEN**이다. 원격 lease 발급·backend ACK·native F3는 별도 수용 범위다.
- 중앙 원자성: source TRANSFER의 `AVAILABLE → CONSUMED` CAS, PACKAGE 및 `SHIPPING-WAIT`, `PACKAGE_CREATED` event·outbox·receipt를 중앙 transaction으로 기록한다. 앱 CSV와 중앙 transaction 사이에는 분산 원자 commit이 없으며 동일 키 복구로 연결한다.
- 선택/순서: `claim_next`는 marker=1, due PENDING만 선택한다. stale `SENDING`은 300초 기준으로 PENDING 회수하며 `COALESCE(last_attempt_at,created_at), created_at, idempotency_key`로 정렬한다. 한 drain에서 시도한 key를 제외하므로 첫 실패가 뒤의 준비 row를 영구 차단하는 엄격 FIFO가 아니다. 300초는 **전송 claim 구현 상수**로 업무 lease 허용 시간과 다르다.
- 오류: transport는 retry. `408/425/429`, 5xx 또는 retryable 오류는 409/412가 아닐 때 retry 대상이다. `committed=true` 오류, 409/412, 비재시도 거부 및 receipt 불일치는 conflict로 보존한다. `Retry-After` 처리와 재시도 시 같은 key를 유지한다.
- 부분 효과: marker=0 중단은 복구 대상이고 전송 대상이 아니다. marker=1 뒤 중앙 충돌은 로컬 완료 유지·검토 사건이며 실물·원본 증거를 보존한다. 취소는 C-06의 별도 명령이다.
- 양쪽 근거: [앱 `_queue_authoritative_package`/완료](../../Label_Match.py), [PackageOutbox/PackageOutboxProcessor](../../package_logistics.py), [서버 API](../../../WorkerAnalysisGUI-web/blueprints/logistics/api.py), [service.create_package](../../../WorkerAnalysisGUI-web/logistics_ledger/service.py), [CAS 구현](../../../WorkerAnalysisGUI-web/logistics_ledger/packages.py). [LM-06/07](README.md#lm-06), [LM-B02](BACKLOG.md#lm-b02), [LM-B05](BACKLOG.md#lm-b05).

<a id="c-05"></a>
## C-05 관측 CSV → 수신·projection → 화면

- 상주 owner 재사용: 정상 onboarding은 stop marker가 없고 instance lease가 점유된 상태에서 기존 fresh RUNNING/live PID와 동일 app/state 경로를 survival window 전후로 확인한 동일 relay만 ALIVE로 재사용한다. 새 child의 exit 0만으로는 수용하지 않는다. [새 대상의 실제 실패·한정 교정](operations.md#label-fresh-qualification)을 유지하며, 이 liveness는 enqueue/receipt/projection 성공을 뜻하지 않는다.
- 과거 task의 이행·rollback 소유권: [historical spec](../../current_user_scheduled_task.py)의 canonical runtime/entrypoint·현재 사용자 Limited/Interactive·app cwd·`--app-root`·정확한 `logs/scheduled_direct_sync_relay.jsonl` 경로를 [기존 installer](../../INSTALL_CANONICAL_PORTABLE.ps1)가 이전 버전 snapshot 소비에서만 비교한다. 다른 log 경로를 거부하며 새 정상 실행에는 task를 생성하지 않는다. 현재 이행·정상 종료는 [resident 계약](#current-user-resident-relay)을 따른다.

- 경로: 로컬 event CSV → spool/relay → `POST /api/producer-ingest/v1/source-file` → server common projection → `/dashboard/api/operations_flow` → 웹 표시. source system `label_match`, dataset `legacy_packaging_csv`, 앱 scan 계약 `label_match_current_v1`이다.
- 파일 발견: 기존 상주 relay의 시작 시 `--scan-source-dir` → 기존 사용자 settings의 nonempty `custom_save_path` → onboarding data root 순서로 선택한다([user_relay](../../user_relay.py)). GUI/session sync가 쓰는 기존 custom CSV를 로그인 후에도 찾기 위한 교정이며 queue/spool·producer identity/manifest·HMAC·receipt와 중앙 명령 계약은 변경하지 않는다. [headless 발견 검증](operations.md#relay-custom-root-evidence)을 실제 enqueue/서버 수신 성공으로 확대하지 않는다.
- wire/identity: HMAC 서명 multipart의 CSV+metadata에 install/host/stream/source identity, content SHA-256, byte length, row count, client batch ID를 결속한다. 로그 내용·인증 값을 문서에 복사하지 않는다.
- 수신/진척: 업무 projection은 `accepted/committed`, source-file identity·행 합계·`COMPLETE`를 요구한다. 2026-09-08 교정은 다음 lifecycle 예외만 추가한다. 전송 중 `pending/leased/retry_wait`는 exact spool path/hash/byte의 in-flight 중복 억제 증거이며 자체로 source prefix 진척을 ACK 처리하지 않는다.
- lifecycle 예외: `label_match/legacy_packaging_csv`, role `label_match`, stream `label_match_events`의 canonical emitter 헤더 `timestamp,worker_name,event,details`와 실제 `APP_START/APP_CLOSE/SCAN_ATTEMPT/SCAN_OK/ERROR_INPUT/SET_RESTORED/SET_CANCELLED/SEALED_TRANSFER_EXCHANGE_APPLIED` 행만 허용한다. receipt11의 SCAN_OK·SET_RESTORED와 receipt12의 SET_CANCELLED·SEALED_TRANSFER_EXCHANGE_APPLIED는 명시된 두 차례의 한정 확장이며 business event의 COMPLETE 요구는 유지한다. 공개 SET_RESTORED는 set_id·continued_by, SET_CANCELLED는 set_id만 기록한다. 공개 SEALED_TRANSFER_EXCHANGE_APPLIED는 기존 identity·멤버 목록·entity_versions·atomic_local_apply를 유지하고 두 raw seal QR 필드만 제외하며 private durable receipt/apply 상태를 변경하지 않는다. `RAW_LEGITIMATE` receipt의 install/source-file identity v2, install scope/hash, source content SHA-256/byte/range, 로컬 파일 hash와 실제 event별 개수가 일치해야 한다. observation v1은 `observation_only=true`, `OBSERVED`, `projection_required=false`, 모든 행 `NOT_PROJECTED/RAW_EVIDENCE_ONLY/NO_STAGE1_REDUCER`이며 unknown/required/projected 수는 0이어야 한다. 나머지 관측 행 수는 업로드 행 수와 같다. 2xx·boolean committed·accepted·retryable=false·retry/error 없음·정수 행 합계·errors/quarantine=0과 기존 runtime fence/rotation 검증을 유지한다. ACK 및 read-only retention 후보에 같은 검증을 적용한다. COMPLETE의 기존 조건이나 runtime observe/legacy 정책을 확대하지 않는다.
- 정상 종료 근거/한계: F3 `TRAY_COMPLETE` ACK 뒤 relay가 새 `APP_CLOSE` delta만 보내는 경로와 동결 서버의 raw 분류는 [정적 소스 대조](E:/KMTech/coordinator-handoff-20260907-01a07992/label-producer-close-contract-fix/PREPARATION.md)로 확인했다. Main이 수용한 [ProducerClose 실제 50/150 PASS](operations.md#producer-close-evidence)는 동결 receipt·로컬 session double을 통한 uploader/retention/runtime 회귀다. raw 수신은 이전 tray receipt·새 F3·포장 명령 ACK·업무 projection을 대체하지 않는다. 비업무 catalog 전체로 예외를 넓히지 않았으며 실제 서버·native F3/종료·재개 연동은 **NOT TESTED / UNPROVEN**이다.
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

<a id="c-08"></a>
## C-08 이력 읽기 완전성

- 이력 reload 시작 시 활성 `scan_count`, `global_scanned_set`, `set_details_map`을 지우지 않는다. 같은 generation의 완전한 오늘 결과만 세 색인을 교체한다. 과거 날짜 결과는 조회 전용이다.
- timestamp·JSON syntax/객체 shape·CSV/파일 읽기 오류가 있으면 부분 집계를 적용하지 않는다. 손상 행 수와 파일·행 위치(최대 20개)를 이력 상세에 남기고 오늘 조회 오류는 재조회 성공 전까지 작업을 차단한다. 로그 파일이 없는 날짜만 정상 빈 결과다.
- 근거: [이력 회귀](../../tests/test_label_match_core.py) `test_history_reload_requires_complete_read_before_replacing_active_indexes`, `test_history_file_errors_are_not_confirmed_empty_history`; 취소·삭제·stale generation·과거 조회 계약을 유지한다.

## 계약 유지·검증 연결

기능 기준은 [README 카드](README.md#기능-카드), 실행 설계는 [운영 수용 시나리오](operations.md#verification)에서 연결한다. 서명·권한·version/capability와 source identity가 달라지면 해당 계약의 실제 양쪽 evidence를 재대조한다. 현재 설치 provider/overlay, 서버 flag, upstream 입력 발행과 downstream 출고·화면의 전체 연결은 [LM-B04](BACKLOG.md#lm-b04)·[LM-B06](BACKLOG.md#lm-b06)·[LM-B08](BACKLOG.md#lm-b08)의 확인 과제이며 코드 존재를 연동 성공으로 집계하지 않는다.
