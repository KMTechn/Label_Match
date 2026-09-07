# Label_Match 기술 명세

포장실 작업자가 원본 PHS2로 현재 제품 구성을 확인하고, 필요한 제품 교체와 실물 랩핑 후 포장 완료를 기록하는 Windows 앱이다. 앱은 입력·로컬 보존·명령 재전송을, 중앙 서버는 멤버십·재고·명령 확정을, 웹은 관측 이벤트의 집계·표시를 소유한다. 근거: [CODEX](../../CODEX.md), [앱 main](../../Label_Match.py), [물류 클라이언트](../../package_logistics.py).

[전체 허브](../../../Program_Spec_Hub/README.md) · [계약](contracts.md) · [운영](operations.md) · [남은 일](BACKLOG.md) · [중앙 준비도](../../../Program_Spec_Hub/READINESS.md)

## 기준과 판정 범위

- 조사·작성일: **2026-09-07**. Label HEAD `3f535c97086129862e5c4699f0d4d0b3b69152a7`, 소스의 `APP_VERSION=v2.0.94`를 기준으로 한다. 버전 문자열은 설치본의 신원이 아니다.
- 읽은 작업 트리는 `Label_Match.py`, `tests/test_deferred_intent_capture.py`, `tests/test_label_match_core.py`, `tests/test_label_ui_lane_integration.py` 수정 및 `tests/test_completion_csv_durability.py` 미추적 상태다. HEAD만으로 이 상태를 재현할 수 없다. [작성 전 파일·index 해시](E:/KMTech/spec-hub-build-20260907/Label_Match/PRESTATE.json)와 [문서 검토 보고](E:/KMTech/spec-hub-build-20260907/Label_Match/IMPLEMENTATION.md)를 연결한다.
- 후속 LM-B10 작업에서 `label_match_single_instance.py`와 `tests/test_label_match_single_instance.py`를 수정했다. 기존 writer 위치·위 dirty 파일·HEAD/index는 보존했다. [소스 교정·정적 검토](E:/KMTech/coordinator-handoff-20260907-01a07992/label-save-root-fix/IMPLEMENTATION.md)에 전후 해시·diff·미실행 회귀 범위를 연결하며, 아래 과거 PASS를 새 코드에 상속하지 않는다.
- [독립 소스 검토](E:/KMTech/coordinator-handoff-20260907-01a07992/label-save-root-independent/REVIEW.md), [Windows focused packet](E:/KMTech/coordinator-handoff-20260907-01a07992/label-save-root-vm-prepare/PREPARATION.md)의 [독립 검토](E:/KMTech/coordinator-handoff-20260907-01a07992/label-save-root-vm-independent/REVIEW.md)와 Main의 전체 제어 파일·diff 검토를 마쳤다. Main은 **2026-09-07 13:09:40Z에 SaveRoot13 실제 13 PASS / 39 ordered phase PASS**를 수용했다. 기존 VM01 interpreter/providers, 원본 108개와 guard/test 2개의 110개 source closure에서 선택한 6개 정의만 해당한다. [실제 결과의 독립 검토·명세 갱신](E:/KMTech/coordinator-handoff-20260907-01a07992/label-save-root-actual-source-close/REVIEW-AND-UPDATE.md)과 [정확한 실행 범위·한계](operations.md#saveroot13-evidence)를 연결한다.
- [완료된 조사](E:/KMTech/spec-hub-research-20260907/Label_Match/RESEARCH.md), [근거 인덱스 S01–S58](E:/KMTech/spec-hub-research-20260907/Label_Match/SOURCE-MAP.tsv)를 바탕으로 관련 현행 소스를 재대조했다. 서버 조사 기준 HEAD는 `0218a671e13bb4d2210b3197a7245e9bec35417a`와 당시 dirty 작업 트리다. 서버 링크는 소유 저장소의 변하는 소스이며 설치 서버 증거가 아니다.

| 판단 축 | 이 기준선에서 말할 수 있는 범위 |
| --- | --- |
| 구현 | **PROVEN — 2026-09-07 위 작업 트리에서 아래 인용 분기·저장 경계의 정적 존재를 확인한 범위만**. 모든 함수의 완전성 판정은 아님 |
| 실제 연동 | **UNPROVEN — 이 기준선과 실제 설치 앱·서버·소비 화면 조합의 일치 및 종단 결과** |
| 수용 검증 | **PROVEN — ProducerClose 실제 50개/150 ordered phase, SaveRoot13 실제 13개/39 phase 및 아래 과거 증거의 각 한정 범위**. 서로 합산하지 않으며 Baseline8, 카드 전체 및 설치 조합의 남은 기준은 **UNPROVEN / NOT TESTED** |
| 운영 준비 | **UNPROVEN — 최종 설치·재시작·재설치·롤백·통합 E2E 및 실장비 범위**. 중앙 준비도에서 통합 판정 |

기존 Goal의 Label Modules3 **313 PASS / 939 ordered PASS**는 2026-09-07 08:11:02Z 코디네이터가 수정된 reader 결과를 수용한 이력이다. 원래 reader의 11개 repr ID/33개 비교 불일치 실패도 보존된다. 이번에 재실행하거나 현재 dirty 코드 전체로 PASS를 확장하지 않았다. 정확한 동결 소스·provider·환경·원본 결과와 남은 Baseline8/통합 범위는 [중앙 준비도](../../../Program_Spec_Hub/READINESS.md)와 [DIRECTION](E:/KMTech/coordinator-handoff-20260907-01a07992/DIRECTION.md)의 해당 시점이 소유한다. 문서 기준선 작성, 개발 작업 완료, 제품 Ready는 별도 상태다.

### 2026-09-08 producer 정상 종료 계약 교정

[소스·회귀·새 VM 준비](E:/KMTech/coordinator-handoff-20260907-01a07992/label-producer-close-contract-fix/PREPARATION.md)에서 F3 전송 ACK 뒤 `APP_CLOSE`만 새 delta로 전송되는 경로를 정적으로 확인하고 `direct_sync_push.py`와 해당 테스트를 교정했다. 동결 서버는 이 lifecycle evidence에 `accepted/committed + RAW_LEGITIMATE`를 반환하므로, exact CSV·identity·hash/byte·행별 nonprojecting observation이 일치할 때만 raw 수신 ACK를 허용한다. 업무 투영은 계속 `COMPLETE`가 필요하다.

구현 분기·도달 가능성은 **PROVEN(정적)**이며, 독립/Main 소스·packet 검토 후 Main이 [ProducerClose 실제 50 collected/50 PASS·150 ordered phase PASS](operations.md#producer-close-evidence)를 수용했다. `Label112-ProducerClose-20260908`의 동결 receipt·로컬 session double 회귀에 한정하며 native close/F3·실제 서버 연동은 **NOT TESTED / UNPROVEN**이다. [소스 종결 근거](E:/KMTech/coordinator-handoff-20260907-01a07992/label-producer-close-source-close/CLOSE.md)는 테스트 당시 uploader·회귀 두 파일과 네 명세의 커밋·parent·해시를 결속한다. 다른 dirty 다섯 파일과 과거 packet·Main hash pins는 보존한다. Modules3 313/939와 SaveRoot13 13/39는 각 원래 소스 범위로 유지하며 합산·상속하지 않는다. Ready **0/6** 유지.

## 사용자·경계·지원 경로

포장 작업자는 이름 귀속, 스캔, 실물 대조·랩핑, 완료·취소를 수행한다. 작업 리더/관리자는 충돌 실물과 복구 사건을 인계받는다. 기계 API 권한과 작업자 표시명은 다르다([protected_admin](../../protected_admin.py), [계약 C-00](contracts.md#c-00)). ERPNext는 별도 제품이며 그 스캔 한도를 이 앱의 업무 제한으로 적용하지 않는다. 상류 PHS 발행·검사·이적 전체 구현은 본 앱 소유가 아니다([공정 계약](../../../WorkerAnalysisGUI-web/docs/PHS2_RESIDUAL_PROCESS_CONTRACT.md)).

| 지원 상태 | 진입점·조건 | 현행 의미·설치본 확인 |
| --- | --- | --- |
| 기본 중앙 업무 | `Label_Match.py:main` → onboarding/guard → Tk, [portable/main.py:main](../../portable/main.py)도 같은 앱 진입 | 원본 compact PHS2 1회 → 필요 시 F4 → 랩핑 → F3. 중앙 프로필·멤버십 확인 필요; 설치 형태·활성 설정 미확인 |
| 조건부 F4 | 전체 단일 TRANSFER를 대표하는 현재 작업, 교체 capability·버전·적합 donor | 1~2쌍 원자 교체 후 새 **전자** 봉인 QR 확인. 부분/복수 transfer work-group은 F3 가능해도 F4 불가 |
| 조건부 F5 | 현품표 교환·reconciliation 대상과 중앙 API, Windows 프린터 | 출력·ACK·활성화 journal을 가진 별도 업무. 일반 F4에 물리 재인쇄를 요구하지 않음 |
| 호환 | 명시적으로 분류된 과거 입력 및 membership mode | 현품표+제품 표본 3개+최종 라벨, F4 전체 재스캔 분기가 남아 있음. 미분류 중앙 입력의 자동 fallback 아님 |
| 현재 사용하는 이름 | `legacy_packaging_csv` | producer 전송 dataset 식별자. `legacy`라는 이름 때문에 폐기로 분류하지 않음 |
| 이력·제외 | [README.txt](../../README.txt), 오래된 OUTLINE/UI; Syncthing·`C:\Sync` | 과거 문서는 현행 절차 근거로 쓰지 않음. 운영 전송은 HTTPS/direct-sync([정책](../../DIRECT_SYNC_DATA_PLATFORM_NOTES.md)) |

경로 근거: [앱 `_handle_f4_action`, `_prompt_exact_rescan`, `main`](../../Label_Match.py), [F5 workflow](../../phs_label_workflow.py), [물류 프로필](../LOGISTICS_RUNTIME_PROFILE.md). 호환 분기의 현장 지원 기간과 설치 활성 여부는 [LM-B08](BACKLOG.md#lm-b08)에서 확인한다.

## 정상·예외 업무 흐름

1. 시작 시 작업자·품목 캐시·저장 위치와 복구 안내를 확인한다. 복구할 이전 세트가 있으면 그 작업의 identity를 유지한다.
2. 원본 PHS2를 한 번 제출한다. 서버가 라벨 identity와 현재 제품 멤버십을 확인한다. 표준 업무에 제품 3개·최종 라벨을 추가하지 않는다.
3. 교체가 필요하면 F4에서 대상→새 양품을 1~2쌍 입력한다. 중앙 교체 ACK 뒤 화면의 새 전자 QR을 스캐너로 다시 확인한다. 원본 물리 PHS2는 유지한다.
4. 현재 멤버십과 실물을 대조하고 실제로 랩핑한 뒤 F3 확인을 확정한다. current-state, intent, CSV flush/fsync, 완료 marker·lease 처리가 끝나야 로컬 성공을 표시한다.
5. 같은 키로 중앙 확정을 재시도하며 다음 준비 작업으로 돌아간다. 로컬 완료 후 단순 전송 대기와, 복구 미완료/중앙 충돌로 작업이 차단된 상태를 구분한다.

근거: [앱 `_commit_finalized_set_durable`, `_return_to_idle_after_finalized_set`, `_prompt_new_seal_verification`](../../Label_Match.py), [완료 정책](../../DIRECT_SYNC_DATA_PLATFORM_NOTES.md). [작업자 정본](../LABEL_MATCH_WORKER_GUIDE.md)의 F4 요약에는 전자 QR 후속 확인이 누락되어 있다. 이 기준선은 코드 차이를 명시하며 기존 정본 수정은 [LM-B01](BACKLOG.md#lm-b01)로 남긴다.

| 예외 | 관측·다음 행동 | 기능 |
| --- | --- | --- |
| 중앙 조회 지연/오프라인 접수 | 저장됨-검증대기는 업무 완료가 아님. 검증·동일 작업 lease 없이 F3 완료하지 않음 | [LM-04](#lm-04), [LM-06](#lm-06) |
| 저장 실패/종료 중단 | 성공 선행 없음; 현재 세트·intent·로그를 보존해 재시작 시 같은 작업으로 복구 | [LM-06](#lm-06), [LM-08](#lm-08) |
| 서버 ACK 유실 | 저장된 명령의 receipt부터 확인; 새 키로 다시 완료하지 않음 | [LM-07](#lm-07) |
| 중앙 충돌 | 로컬 commit 여부를 유지하며 관리자 검토·실물 구분 보관으로 인계 | [LM-07](#lm-07) |
| busy 중 추가 스캔 | 접수되지 않은 입력칸 값을 보존; idle 뒤 한 번 제출 | [LM-12](#lm-12) |
| 출력 여부 불명확 | journal·서버 상태를 조정하고 재출력 판단; spool 접수를 실물 출력 증거로 대체하지 않음 | [LM-10](#lm-10) |

## 기능 카드

아래 ID는 안정적인 탐색 단위다. 12개는 대표 기능 묶음이며 전체 함수 수·개발량·완성도 분모가 아니다. 모든 카드의 확인일·네 판단 축은 위 기준을 공유한다. 수용 기준은 **현행 계약을 보존하기 위한 확인 항목**이며 새 사업 규칙 승인이나 실행 PASS를 뜻하지 않는다. 담당 실명·기한은 미지정이고 역할·다음 행동은 연결한 백로그에서 관리한다.

| 기능·카드 | 주요 진입·소유 심볼 |
| --- | --- |
| [LM-01 시작·작업자](#lm-01) | [run_guarded_entrypoint](../../label_match_single_instance.py) |
| [LM-02 품목·캐시](#lm-02) | [refresh_item_catalog](../../item_catalog_sync.py) |
| [LM-03 PHS2 접수](#lm-03) | [resolve_package_source_projection](../../package_logistics.py) |
| [LM-04 지연 입력 보존](#lm-04) | [DeferredIntentCaptureStore](../../deferred_intent_capture.py) |
| [LM-05 F4 교체·QR 확인](#lm-05) | [_prompt_new_seal_verification](../../Label_Match.py) |
| [LM-06 F3 로컬 확정](#lm-06) | [_commit_finalized_set_durable](../../Label_Match.py) |
| [LM-07 중앙 ACK·충돌](#lm-07) | [PackageOutboxProcessor.drain](../../package_logistics.py) |
| [LM-08 중단·날짜 복구](#lm-08) | [_load_current_set_state](../../Label_Match.py) |
| [LM-09 초기화·완료 취소](#lm-09) | [PackageCancellationOutbox](../../package_logistics.py) |
| [LM-10 F5 인쇄·조정](#lm-10) | [execute_single / recover_current](../../phs_label_workflow.py) |
| [LM-11 관측·세트 집계](#lm-11) | [direct_sync_push](../../direct_sync_push.py) |
| [LM-12 화면·입력 gate](#lm-12) | [TkSerialUiLane](../../tk_serial_ui_lane.py) |

<a id="lm-01"></a>
### LM-01 시작·작업자 귀속

- 시작/입력: source 또는 portable 진입, 현재 사용자 설정·작업자 이름. onboarding, factory wire 계약, 데이터 범위 mutex를 거쳐 앱을 시작한다.
- 효과: 작업자 정규화 후 CSV·현재 상태에 귀속한다. 보호 관리자 인증 값은 표시·지속 저장용 identity와 분리한다.
- 실패/복구: 필수 profile·catalog·onboarding 실패는 시작을 차단한다. guard는 앱과 같은 기존 nonempty `custom_save_path` → `LABEL_MATCH_SAVE_DIR` → ProgramData 순서로 GUI writer 범위를 선택하도록 소스 교정했다. 실제 writer resolver의 null/빈 설정 fallback과 서로 다른 env·같은 custom 저장소의 **단일 process native mutex callback 제외·해제 후 재진입은 PROVEN**이다([SaveRoot13](operations.md#saveroot13-evidence)). 두 GUI writer와 같은 pending identity 복구는 미입증이다. guard 이전 onboarding ledger·persistent relay 선택과 template/alias 한계는 [LM-B10](BACKLOG.md#lm-b10)에 별도로 남긴다. 이전 작업자가 다르면 복원 확인을 거친다.
- 수용 기준: 같은 실제 저장소에서 중복 GUI writer callback을 차단하고 첫 소유자 종료 후 같은 저장소를 복구한다. conflicting env/custom 및 null/빈 설정 fallback을 실제 writer resolver와 대조하며, 인증 실패가 일반 완료로 내려가지 않고 관리자 비밀이 로그/히스토리에 남지 않는다.
- 근거: [main](../../Label_Match.py), [run_guarded_entrypoint](../../label_match_single_instance.py), [canonical_operator_id/persistent_operator_name](../../protected_admin.py). [운영 구성](operations.md#configuration), [LM-B04](BACKLOG.md#lm-b04).

<a id="lm-02"></a>
### LM-02 품목 마스터와 캐시

- 시작/입력: 시작 시 중앙 품목 CSV를 갱신하고 품목 코드로 이름·규격을 조회한다.
- 검증/저장: 중앙 등록 장비는 identity에 결속된 인증 캐시·검증된 snapshot을 사용한다. 비등록 호환 경로의 로컬 cache/bundled `Item.csv` fallback과 다르다.
- 실패/재시작: 중앙 요청 실패 시 인증된 이전 cache 복구가 가능하며, 없으면 중앙 등록 장비 시작을 차단한다. 임의 파일을 중앙 마스터로 인정하지 않는다.
- 수용 기준: 정상 갱신, 인증 cache 복구, 손상/다른 identity cache 거부를 구분하고 화면에 실제 출처·경고가 대응한다.
- 근거: [refresh_item_catalog](../../item_catalog_sync.py), [prepare_startup_item_catalog](../../Label_Match.py). [C-01](contracts.md#c-01), [LM-B04](BACKLOG.md#lm-b04).

<a id="lm-03"></a>
### LM-03 PHS2 접수·현재 구성 조회

- 시작/입력: idle 작업 화면에서 원본 PHS2 1회. `PHS,SRC,ITG,CLC,LBL,HSH` 여섯 필드의 순서·빈 값·중복·16자리 hex를 검사한다.
- 검증/효과: `PACKAGE_SOURCE` 조회로 중앙 라벨 계보, unit↔barcode, 멤버십 hash·version·work-group topology를 대조해 현재 세트 snapshot을 만든다. 수량은 현재 멤버십에서 얻는다.
- 실패/취소: 오형식·불일치는 포장 준비로 승격하지 않는다. 정상 미완료 작업의 F1과 중앙 미확정/교체 중 작업의 차단 조건은 별도 gate를 따른다.
- 수용 기준: 정상 PHS2 한 번으로 준비되며 잘못된 LBL/HSH·미분류 중앙 입력은 거부된다. 부분/다중 work-group의 F3/F4 허용 차이가 화면과 일치한다.
- 근거: [parser/현재 세트](../../Label_Match.py), [resolve_package_source_projection](../../package_logistics.py). [C-02](contracts.md#c-02), [LM-B05](BACKLOG.md#lm-b05).

<a id="lm-04"></a>
### LM-04 지연 입력 보존·검증 대기

- 시작/입력: 외부 요청 전에 접수 intent를 durable 저장한다. capture identity와 payload hash·사용자 보호 문맥을 결속한다.
- 효과: `CAPTURED_UNVERIFIED`에서 검증/의존 대기/전송/로컬 적용 상태를 구분한다. 저장됨은 제품·재고·포장 완료가 아니다.
- 실패/재시작: 디스크 실패는 저장 성공으로 알리지 않는다. 유효성 실패는 격리하고 결과 불명확은 reconcile한다. legacy outbox 인계는 같은 로컬 transaction에서 기존 capture를 supersede하여 제출 소유권을 겹치지 않게 한다.
- 수용 기준: 같은 입력의 중복 접수는 수렴하고 다른 payload는 차단되며, 중단 후 원래 identity로 복구된다. ACKED와 COMPLETED를 같은 표시로 합치지 않는다.
- 근거: [deferred_intent_capture](../../deferred_intent_capture.py), [재구성된 fixture 계약](../../tests/fixtures/deferred_intent_contract/CONTRACT.md). [상태](contracts.md#states), [LM-B02](BACKLOG.md#lm-b02), [LM-B05](BACKLOG.md#lm-b05).

<a id="lm-05"></a>
### LM-05 F4 제품 교체·새 전자 QR 확인

- 시작/입력: PACKAGE 생성 전 전체 단일 TRANSFER 작업에서 F4, 기존 제품→새 GOOD 제품 1~2쌍. 중앙 capability와 현재 seal, 같은 lot·품목·UOM, 단품 donor PHS를 대조한다.
- 쓰기/결과: 중앙은 대상·donor·damage bundle version을 검사해 원자 교체하고 새 seal receipt를 만든다. 앱은 저장된 receipt를 검증한 뒤 새 QR 확인을 요구한다. **원본 물리 PHS2는 유지하고 새 전자 봉인 QR을 화면에서 다시 스캔한다.**
- 실패/복구: 부분/다중 TRANSFER work-group, 부적합 donor·stale version·불완전 receipt는 차단한다. ACK 유실은 저장 intent/receipt로 복구하고 재확인 전 정상 후속 동작을 제한한다. 일반 F4는 물리 출력 업무가 아니다.
- 수용 기준: 1쌍/2쌍 성공 시 제품 수는 보존되고 교체 멤버·seal version이 일치한다. 거부 시 부분 교체가 없고, 새 QR 검증·로컬 저장 중단 후에도 두 번 교체하지 않는다.
- 근거: [교체 command/attempt](../../sealed_transfer_exchange.py), [QR 확인/gate](../../Label_Match.py), [기존 정책](../MEMBER_EXCHANGE_POLICY.md). [C-03](contracts.md#c-03), [LM-B01](BACKLOG.md#lm-b01), [LM-B05](BACKLOG.md#lm-b05).

<a id="lm-06"></a>
### LM-06 F3 랩핑 완료·로컬 확정

- 시작/입력: 현재 업무 날짜 화면에서 실물 랩핑 후 F3 확인. 정확한 source snapshot에 결속된 검증 lease와 작업자·현재 set가 필요하다. 수기 수량으로 우회하지 않는다.
- 쓰기/결과: current-state 보존 → outbox intent → `TRAY_COMPLETE` CSV flush/fsync → `local_completion_committed=1` 및 operation lease transaction → 성공음·이력·다음 준비. marker=0인 PENDING은 로컬 완료가 아니다.
- 실패/취소/복구: 유효한 재사용 lease 없이 오프라인 완료하지 않는다. CSV·marker 실패는 성공을 표시하지 않고 같은 작업으로 복구한다. 서버 ACK 실패로 이미 durable한 로컬 완료를 취소하지 않는다.
- 수용 기준: 각 쓰기 경계의 중단에서 거짓 성공이 없고, 재시작·자정 변경 뒤 완료 이벤트와 중앙 효과가 중복되지 않는다. 로컬 확정 후 전송 pending은 다음 준비를 막지 않는다.
- 근거: [ `_queue_authoritative_package`, `_commit_finalized_set_durable`](../../Label_Match.py), [mark_local_completion_committed](../../package_logistics.py). [C-04](contracts.md#c-04), [LM-B02](BACKLOG.md#lm-b02), [LM-B05](BACKLOG.md#lm-b05).

<a id="lm-07"></a>
### LM-07 중앙 ACK·재전송·충돌 인계

- 시작/입력: marker=1인 due PENDING row를 claim하고 저장 명령 또는 draft를 전송한다. key는 같은 set/package identity에서 유지한다.
- 효과: 저장 명령이 있으면 receipt부터 조회한다. 검증된 중앙 COMMITTED만 ACKED로 저장한다. due-time·마지막 시도 시각으로 후속 준비 row도 진행한다.
- 실패/복구: transport/일시 오류는 retry, 409/412 및 비재시도 오류·불일치 receipt는 conflict다. marker=1 충돌은 로컬 완료를 보존하며 `OPERATOR_REVIEW` 사건으로 인계한다. marker=0 충돌을 PASS로 복구하지 않는다.
- 수용 기준: 동일 key 재요청은 중앙 효과 1회, 첫 실패가 뒤 작업을 굶기지 않음, 다중 PC 경합의 승자/충돌이 중앙 CAS 결과와 일치함. 검토 종결·실물 처리는 운영 요구 확정이 필요하다.
- 근거: [claim_next/drain/mark_conflict](../../package_logistics.py). [C-04](contracts.md#c-04), [LM-B02](BACKLOG.md#lm-b02), [LM-B06](BACKLOG.md#lm-b06).

<a id="lm-08"></a>
### LM-08 중단·날짜 변경 복구

- 시작/입력: 시작 시 `_current_set_state_packaging.json`, outbox marker, 기존 완료 이벤트·교체/접수 journal을 확인한다.
- 검증/효과: 작업자 변경·중앙 상태 migration·이미 기록된 이벤트를 대조해 같은 set를 복원한다. 날짜가 달라도 미확정 PHS2/outbox를 폐기하지 않는다.
- 실패/취소: 읽기 손상·writer 오류·marker 경계 불일치를 정상 완료로 취급하지 않는다. 상태·DB를 수동 편집해 복구했다고 판단하지 않는다.
- 수용 기준: CSV 기록 직후/marker 직전/성공 표시 직전 중단과 날짜 변경을 구분해 중복 없이 복구하며 미확정 증거를 유지한다.
- 근거: [DataManager, `_load_current_set_state`, `_label_match_local_completion_event_exists`](../../Label_Match.py), [완료 내구성 테스트 설계](../../tests/test_completion_csv_durability.py). [복구 운영](operations.md#recovery), [LM-B05](BACKLOG.md#lm-b05), [LM-B07](BACKLOG.md#lm-b07).

<a id="lm-09"></a>
### LM-09 현재 작업 초기화·완료 취소

- 시작/입력: F1은 gate가 허용하는 현재 미완료 세트 초기화, F2는 완료 트레이 선택·사유와 관련 취소 event를 사용한다.
- 쓰기/결과: 취소 outbox는 원래 CREATE_PACKAGE key/ACK에 결속한다. 중앙 취소는 포장의 유효성을 변경하며 재고는 `SHIPPING-WAIT`에 남는다. 로컬 삭제나 재고 반환 명령과 같지 않다.
- 실패/복구: CREATE 미확정은 취소 dependency로 기다리고 전송 실패·ACK 유실은 취소 identity를 유지해 재시도한다. 교체/복구 차단 상태를 F1로 우회하지 않는다.
- 수용 기준: 생성 ACK 전후 취소와 중복 취소가 연결되고, 취소 세트는 집계에서 제외되며, 실제 재고 위치가 임의 복귀하지 않는다.
- 근거: [F1/F2 handlers](../../Label_Match.py), [PackageCancellationOutbox/processor](../../package_logistics.py). [C-06](contracts.md#c-06), [LM-B05](BACKLOG.md#lm-b05), [LM-B06](BACKLOG.md#lm-b06).

<a id="lm-10"></a>
### LM-10 F5 현품표 교환·인쇄·조정

- 시작/입력: 지원 대상 현품표 교환/reconciliation 화면에서 중앙 prepare와 현재 intent를 사용한다. F4 교체와 독립된 조건부 기능이다.
- 쓰기/결과: 로컬 journal, 중앙 prepare/status/print attempt/activate를 연결한다. Windows GDI 출력 proof는 printer·job ID·문서·시각·EndDoc를 기록한다.
- 실패/복구: 출력 실패와 출력 후 ACK 유실을 구분해 `recover_current`/`recover_reconciliation`으로 조정한다. 결과가 불명확하면 무조건 새 출력부터 반복하지 않는다.
- 수용 기준: 재시작 후 준비/인쇄/활성화 단계가 일치하고 중복 출력을 통제하며 기본 프린터 설정을 보존한다. 큐 접수와 실물 종이·QR 가독성을 각각 관찰한다.
- 근거: [PhysicalPrintEvidence/execute_single/recover_current](../../phs_label_workflow.py). [C-07](contracts.md#c-07), [장비](operations.md#devices), [LM-B04](BACKLOG.md#lm-b04).

<a id="lm-11"></a>
### LM-11 관측 전송·포장 세트 집계

- 시작/입력: 작업 event CSV와 source-file metadata를 direct-sync spool에 보존하고 HTTPS producer-ingest로 전송한다.
- 검증/결과: 업무 projection은 receipt identity·행 합계·accepted/committed·`COMPLETE`를 요구한다. `APP_START/APP_CLOSE/SCAN_ATTEMPT`만 있는 canonical emitter CSV는 [C-05](contracts.md#c-05)의 exact nonprojecting receipt 검증을 통과한 `RAW_LEGITIMATE`도 raw 수신 ACK로 처리한다. 웹의 `total_sets_completed`는 포장 세트이며 raw lifecycle로 증가시키지 않는다.
- 실패/복구: pending·retry·operator review·permanent failure를 보존한다. CSV 수신 ACK를 package command ACK로 사용하지 않으며 누락 spool은 통신 재시도로 복원되지 않는다.
- 수용 기준: 정상/취소/부분·중복 event의 집계 단위와 수신·투영·화면 반영을 각각 확인한다. F3 ACK 이후 close-only delta와 reopen-close lifecycle batch가 원래 hash/key로 수신되고, 미투영 업무·거부·불완전 receipt는 ACK되지 않아야 한다. 명령 ACK, raw 수신, 업무 projection을 서로 대체하지 않는다.
- 근거: [direct_sync_push](../../direct_sync_push.py), [이벤트 분리](../../event_stream_policy.py). [C-05](contracts.md#c-05), [수량](contracts.md#quantities), [LM-B06](BACKLOG.md#lm-b06).

<a id="lm-12"></a>
### LM-12 작업 화면·이력·입력 gate

- 시작/입력: 현재/과거 날짜 조회, 히스토리·집계, 스캔 Enter와 업무 버튼/단축키. F1–F4는 날짜·busy·교체/복구 상태의 공통 action gate를 사용하며 F5 교환·조정은 해당 진입 조건을 검사한다.
- 효과: 비동기 작업은 UI lane에 제출하고 오래된 generation의 결과 적용을 차단한다. busy 거절 시 입력칸 값을 보존하며 과거 조회에서 F3를 차단한다.
- 실패/취소/종료: BROKEN lane은 추가 입력을 차단한다. 처리 중 종료 요청은 drain 후 종료하며 타임아웃의 강제 중단을 정상 저장 증거로 삼지 않는다.
- 수용 기준: 버튼/키보드 gate가 일치하고 busy 입력 보존·idle 재제출·stale 결과 무시·종료 경계가 대응한다. 실장비 입력과 화면 배율의 가독성은 별도 확인한다.
- 근거: [앱 workflow handlers](../../Label_Match.py), [TkSerialUiLane](../../tk_serial_ui_lane.py), [관련 테스트 설계](../../tests/test_label_ui_lane_integration.py). [LM-B04](BACKLOG.md#lm-b04), [LM-B05](BACKLOG.md#lm-b05).

## 명세 진행·알려진 미조사 범위

최초 명세 기준선은 AGENTS·기능·계약·운영·백로그를 정적으로 대조하고 당시 제품 코드·테스트를 보존했으며, [독립 교차 검토](E:/KMTech/spec-hub-build-20260907/cross-review/REVIEW.md)와 Main 대조도 완료했다. 후속 LM-B10 단위에서는 guard와 해당 회귀 테스트, 이 네 spec 문서를 수정했고 SaveRoot13의 한정 runtime 수용까지 연결했다. 앞선 실제 증거 검토·명세 갱신은 문서 작업이었다. 2026-09-08 후속 producer-close 소스 종결은 테스트 당시 uploader·회귀와 이 네 명세를 한 소스 단위로 묶고 실제 50/150 수용을 연결했다. 동결 packet과 과거 Main hash pins는 보존했다. 기존 CODEX·작업자 정본 및 다른 dirty 코드는 보존했다. 설정 template/사용자 쓰기 위치, onboarding/기본/Machine 경로와 오래된 안내는 [운영](operations.md#configuration), [LM-B09](BACKLOG.md#lm-b09), [LM-B10](BACKLOG.md#lm-b10)에서 구분한다. 소스 교정·focused runtime 수용·제품 준비도는 별도다.

이번 산출물은 핵심 업무·예외·계약·운영 공백과 수용 기준을 연결한 **소스 기반 기준선**이다. 메뉴 전체, 모든 호환 barcode/수정·관리자 분기, 업데이트 모든 장애 단계, 실제 provider/overlay·설치 플래그, 상류 PHS 발행부터 출고까지, 검토 사건의 실제 화면 소비·종결은 전수 확인하지 않았다. 테스트 파일의 존재를 실행 증거로 세지 않는다. 후속 작업은 [BACKLOG](BACKLOG.md)에서 관리하고 공통 관계·우선순위는 [중앙 통합](../../../Program_Spec_Hub/INTEGRATIONS.md)과 [중앙 백로그](../../../Program_Spec_Hub/BACKLOG.md)에 전달한다.
