# Label_Match 진행·공백·추가 제안

[제품·기능](README.md) · [계약](contracts.md) · [운영](operations.md) · [공통 백로그](../../../Program_Spec_Hub/BACKLOG.md) · [실제 준비도](../../../Program_Spec_Hub/READINESS.md)

최초 기준일: **2026-09-07**, 후속 갱신: **2026-09-08**. 최초 5문서 명세 기준선의 독립/Main 교차 검토와 승인된 LM-B10 guard·회귀 테스트·spec 소스 단위, 후속 **SaveRoot13 실제 13 PASS/39 ordered phase PASS** 수용을 반영했다. 2026-09-08 후속 소스 종결은 Main이 수용한 ProducerClose 실제 50 PASS/150 ordered phase PASS와 승인된 두 소스·네 명세를 연결했으며 이번 검토에서 runtime을 재실행하지 않았다. 후속 [잔여 소스 종결](E:/KMTech/coordinator-handoff-20260907-01a07992/label-residual-source-close/CLOSE.md)은 승인된 잔여 다섯 경로와 네 명세를 묶고 [M3/N6 사례·소스 매핑](operations.md#residual-source-evidence)을 완료했다. 문서·소스 단위 완료나 이 한정 PASS로 제품 Ready를 판정하지 않는다. P1은 필수 업무/계약·검증 공백의 우선순위, P2는 추가 요구 후보다. 담당은 책임 역할이며 실명·기한·목표 릴리스는 미지정이다. 실제 순서는 Main의 현행 지시를 따른다.

## 현재 진행

| 구분 | 상태·근거 | 다음 행동 |
| --- | --- | --- |
| 승인된 명세 구축 | AGENTS와 4개 spec 기준선 작성·정적 검토 및 [독립 교차 검토](E:/KMTech/spec-hub-build-20260907/cross-review/REVIEW.md)·Main 대조 완료. [IMPLEMENTATION](E:/KMTech/spec-hub-build-20260907/Label_Match/IMPLEMENTATION.md) | 후속 변경마다 기능·계약·증거·남은 일 유지; 전수 기능·설치 자격과 구별 |
| 개발 작업 | LM-B10의 좁은 guard 교정·[독립 소스 검토](E:/KMTech/coordinator-handoff-20260907-01a07992/label-save-root-independent/REVIEW.md)·packet 독립/Main 검토 완료, 한정 회귀 PROVEN. [실제 검토·문서 갱신](E:/KMTech/coordinator-handoff-20260907-01a07992/label-save-root-actual-source-close/REVIEW-AND-UPDATE.md)은 기존 dirty 제품·테스트·HEAD/index·동결 packet 보존 | Main이 guard·회귀·명세와 기존 AGENTS 기준선을 한 번의 소스 단위 커밋으로 수용. LM-B10 잔여 수용·운영 범위 OPEN |
| producer-close 소스·한정 회귀 | [LM-B11](#lm-b11) 독립/Main 소스·packet 검토와 실제 50 PASS/150 ordered phase PASS 수용, [두 소스·네 명세 종결](E:/KMTech/coordinator-handoff-20260907-01a07992/label-producer-close-source-close/CLOSE.md) | native N/I·F3 close/reopen/서버 연동은 NOT TESTED. 당시 보존한 잔여 다섯 경로는 아래 소스 종결에 연결 |
| 잔여 완료 CSV·복구 소스 종결 | [독립 전체 diff·증거 검토](E:/KMTech/coordinator-handoff-20260907-01a07992/label-residual-source-review/REVIEW.md)와 Main 승인에 따라 다섯 source/test working bytes 보존·네 명세 갱신, [잔여 소스 종결](E:/KMTech/coordinator-handoff-20260907-01a07992/label-residual-source-close/CLOSE.md)에 parent/commit·working/blob pins 연결 | V03/V07 변경 13사례는 M3 내부, N6 6사례는 별도 선택으로 매핑 완료. native/backend/FULL은 source-frozen 후속 범위 |
| 실제 검증 | [M3 313/939 내부 변경 13사례와 별도 N6 6/18](operations.md#residual-source-evidence), [ProducerClose 50/150](operations.md#producer-close-evidence), [SaveRoot13 13/39](operations.md#saveroot13-evidence)를 각 원래 exact source/선택 범위로 유지; 초기 export·원래 reader 실패 보존 | Baseline8 causal RED는 NOT TESTED 역사 공백이며 이번 소스 종결 선행 조건이 아님. cohort 합산·재실행·통합 FULL/GUI PASS 확장 없음 |
| 운영 준비 | 목표 릴리스 실제 연동·설치·cold boot·재설치·rollback·통합 E2E의 전체 근거 UNPROVEN | LM-B04/06/07 및 중앙 Q06/Q07 추적. 임의 완료율 없음 |

<a id="lm-b01"></a>
## LM-B01 · F4 작업 안내의 후속 QR 확인 누락

- 유형/우선: **확인된 문서 불일치 · P1**. 작업자가 교체 ACK 뒤 확인 단계를 모르고 멈출 수 있다.
- 근거: [앱 `_prompt_new_seal_verification`](../../Label_Match.py)와 [작업자 정본의 기본 순서](../LABEL_MATCH_WORKER_GUIDE.md); [LM-05](README.md#lm-05), [C-03](contracts.md#c-03). 원본 물리 PHS2 유지와 새 전자 QR 확인은 동시에 성립한다.
- 상태/다음: 기준선에는 현행 순서 반영 완료, 기존 정본 수정은 **남음**. Label 문서·교육 담당이 정본의 F4 문구·관련 화면/기존 캡처 적용 범위를 대조한다. 이번 5문서 소유 범위 밖의 정본은 수정하지 않았다.
- 완료 기준: 허용 F4→중앙 교체→새 전자 QR 확인→랩핑/F3의 순서와 실패/재시작 안내가 일치한다. 일반 F4에 물리 인쇄나 전량 제품 재스캔을 추가하지 않는다.
- 의존/병렬: 중앙 [S02](../../../Program_Spec_Hub/BACKLOG.md#specification)와 연결. 문구 대조는 장비 검증과 독립 진행 가능; 실물 판독 증거는 LM-B04.

<a id="lm-b02"></a>
## LM-B02 · 오프라인·지연·충돌 인계 요구 미정

- 유형/우선: **요구·운영 계약 공백 · P1**. 오래된 pending/충돌 실물의 보관·인계 시점과 담당 판단이 불명확하다.
- 근거: [C-04](contracts.md#c-04)의 검증 lease·due 재시도·marker별 conflict, [복구](operations.md#recovery), [기존 완료 정책](../../DIRECT_SYNC_DATA_PLATFORM_NOTES.md). 현행 유효 lease 없는 F3 차단과 marker=1 로컬 완료 보존은 요구 미정 때문에 완화하지 않는다.
- 상태/다음: **요구 확인 대기**. 포장 운영·Label·서버 담당과 Main이 허용 offline 범위, lease 재사용 한계, pending/검토 인계 시간, 책임 역할·종결 권한, 실제 실물 조치를 정리한다. 시계차·업무일 cutoff·latency/throughput 목표도 확정값 없음.
- 완료 기준: 업무/안전 범위가 요구 근거와 연결되고, 운영 응답 목표·코드 timeout/claim 상수·실측이 분리된다. 단절/lease 경계/다중 PC/ACK 유실별 기대 결과·실물 인계·종결 근거를 기록한다.
- 의존/병렬: 중앙 [I04](../../../Program_Spec_Hub/BACKLOG.md#integration), LM-B06의 검토 소비 경로에 의존. 기존 증거 매핑 LM-B05는 병렬 가능.

<a id="lm-b03"></a>
## LM-B03 · 명령·관측·화면 상태를 연결한 조회 후보

- 유형/우선: **추가 기능 제안 · P2**, 승인된 미구현 요구나 확인된 결함이 아니다.
- 문제/근거: 같은 포장에 로컬 완료, 중앙 command ACK, producer projection, 화면 반영이 달라 문의·충돌 인계가 어려울 수 있다([상태 경계](contracts.md#states)). 실제 기존 화면으로 해결되지 않는 불편은 아직 확인하지 않았다.
- 상태/다음: **후보**. Label·Web·운영 담당이 현행 pending/검토 표시·사건 소비/종결과 사용자 문제를 먼저 조사한다. 이를 근거로 필요성·노출 범위·권한을 결정한다.
- 착수/완료 기준: 필요성이 요구로 확정된 경우에만 개발 범위를 정한다. 이후 동일 set/key의 각 효과·마지막 갱신·인계 담당/종결을 오해 없이 확인하는 수용 기준을 연결한다.
- 의존/병렬: LM-B02/06 및 중앙 [추가 제안 P01](../../../Program_Spec_Hub/BACKLOG.md). 이번 기준선에서 UI·상태 기계를 추가하지 않는다.

<a id="lm-b04"></a>
## LM-B04 · 실제 설치 구성·권한·장비 수용 연결

- 유형/우선: **구성·검증 공백 · P1**. 소스의 정상 경로와 설치 PC의 선택 프로필/장비가 다를 수 있다.
- 근거: [운영 구성·설정](operations.md#configuration), [장비](operations.md#devices), [LM-01/02/05/10/12](README.md#lm-01). Machine anchor, current-user profile/DPAPI, packaged settings와 실제 쓰기 위치, F4 capability, F5 driver·spool을 구분한다.
- 상태/다음: **실제 조합 미확인**. 릴리스·Label·현장 담당이 기존 후보 근거에서 artifact/source/test 식별, 실제 module/provider/overlay·Python/라이브러리, 비밀 없는 profile identity·권한·flags, scanner·프린터·사운드·화면 조건을 연결한다.
- 완료 기준: 지정 조합의 시작/품목 cache/권한 거부, Enter·종료문자·IME·busy/포커스·배율, F4 전자 QR, 조건부 F5 실물 출력이 각 수용 결과와 연결된다. spool 성공과 종이·판독은 별도 판단하고 비적용 장비는 이유를 기록한다.
- 의존/병렬: Main의 목표 후보·장비 범위와 LM-B10 선택 일치 확인 필요. 설정·기존 증거 정리는 VM/장비 실행과 독립 가능. 실제 실행은 현행 qualification 작업이 소유한다.

<a id="lm-b05"></a>
## LM-B05 · 기존 PASS 적용 범위와 남은 수용 검증 매핑

- 유형/우선: **증거 매핑·승인된 잔여 소스 종결 완료 / native·통합 수용 공백 OPEN · P1**. 한정 실행 수를 전체 제품 완료로 해석하지 않는다.
- 근거/완료 범위: [독립 전체 diff·증거 검토](E:/KMTech/coordinator-handoff-20260907-01a07992/label-residual-source-review/REVIEW.md), [잔여 소스 종결](E:/KMTech/coordinator-handoff-20260907-01a07992/label-residual-source-close/CLOSE.md), [M3/N6 사례·소스 매핑](operations.md#residual-source-evidence)에서 `Label_Match.py`와 deferred/core/lane/신규 completion CSV 테스트의 다섯 경로를 원래 P109/PC112 exact working pins에 결속했다. M3 **313/939** 안의 변경 **13사례**는 V03 실제 저장 거부·중단/자정·materializer 및 V07 worker/Tk 적용 경계를, 별도 N6 **6/18**은 기존 CSV 재동기화·writer 오류·재대조를 입증한다. Main이 승인한 한 소스 단위에 다섯 경로와 네 명세를 포함하며 테스트 당시 working bytes와 Git 정규화 blob pins를 구분했다.
- 상태: SaveRoot13의 V01 **13/39**, ProducerClose의 V06 **50/150**은 별도 source/선택으로 유지한다. Candidate13은 변경 M3 사례와 겹치며 합산하지 않는다. 원래 M3 reader 11 repr/33 비교 실패, Candidate13 11 PASS/2 FAIL, ProducerClose Export01 **FAILED**를 보존한다. 기존 수용을 반복 실행하거나 깨끗한 Git 상태를 FULL/제품 수용으로 바꾸지 않는다.
- 역사 공백: Baseline8 causal RED는 **NOT TESTED**이며 start/evidence/Main 결과가 없다. Main은 이미 수용한 수정의 이번 종결에 이를 선행 조건으로 요구하지 않았다. exact baseline pins는 독립 검토에 남으며 예상 RED를 실제 결과로 승격하지 않는다.
- 남은 수용: real lease-bearing ACKED-orphan의 동기화/marker 실패를 통합 재시작으로 연결한 범위는 **UNPROVEN**이다. N6는 lease 없는 역사 ACKED/marker0 seed, M3 #257은 mocked forwarding, materializer/lane의 lease atomicity는 별도 로컬 증거다. native F3/close/reopen, 두 GUI same-store/pending 복구, 실제 backend lease/ACK·producer receipt, hardware power-loss와 FULL/설치도 별도 미입증 범위다. 일부 CSV 예외별 공백은 확인된 결함 판정이 아니다.
- 다음/완료 기준: 종결된 candidate와 provider·환경을 동결하고 기존 N/I same-store key/member/hash/lease 및 native 입력/F3/정상 종료·중단·재개/EOF·실제 transport 관측을 연결한다. [ProducerClose의 anchor/grant/backend 준비 잔여](operations.md#producer-close-evidence), LM-B04/06/07/10과 FULL 요구를 해당 실제 결과로 판정한다. 기존 수용 cohort의 재실행이나 새 절차 gate를 이 소스 종결의 조건으로 추가하지 않는다.
- 의존/소유: Label은 로컬 상세 명세, Main은 중앙 [Q08 및 Q06/Q07](../../../Program_Spec_Hub/BACKLOG.md#qualification)·준비도 갱신과 후속 실행을 소유한다. Ready **0/6** 유지.

<a id="lm-b06"></a>
## LM-B06 · 원장→관측→소비 화면과 충돌 종결 확인

- 유형/우선: **실제 통합·정의/소비 확인 공백 · P1**. 중앙 완료와 표시 지연을 혼동하거나 세트·제품·취소 수량을 잘못 해석할 수 있다.
- 근거: [C-05](contracts.md#c-05), [C-06](contracts.md#c-06), [수량](contracts.md#quantities). `legacy_packaging_csv`는 활성 전송 식별자, `total_sets_completed`는 포장 세트이며 취소는 재고 반환이 아니다. POST_REVIEW_REQUIRED의 실제 화면 소비·담당 종결까지는 미확인이다.
- 상태/다음: **정적 양쪽 경로 확인 / 목표 조합 연동 UNPROVEN**. Label·Web 담당이 scope/set/key·원본 event → producer receipt → projection → API flag/readiness → 화면을 연결하고, 명령/CSV 도착 순서·취소/중복/부분/검토 사건을 대조한다.
- 완료 기준: 지정 입력의 제품 membership·포장 세트·취소 효과·수신/표시 시각과 출처를 설명할 수 있고 각 관측 지점에 실제 근거가 있다. 단위 환산 Pcs/EA/piece와 freshness 미정 항목을 임의로 채우지 않는다. 실제 검토 사건 종결 근거는 LM-B02와 연결한다.
- 의존/병렬: 중앙 [INT-04/05/09/10](../../../Program_Spec_Hub/INTEGRATIONS.md)과 [I02/I03/I04](../../../Program_Spec_Hub/BACKLOG.md#integration). Web 구현·배포 flag·상류/후속 인계는 해당 소유자에게 전달하고 다른 저장소는 수정하지 않는다.

<a id="lm-b07"></a>
## LM-B07 · 백업·복원·재설치·롤백·보존 정책 수용

- 유형/우선: **운영 요구·검증 공백 · P1**. 코드 교체/제거 뒤 identity·미전송 업무·검토 증거를 잃으면 중복 효과 또는 복구 불가 위험이 있다.
- 근거: [운영 복구·설치](operations.md#recovery), [릴리스 계약](../../RELEASE_GATE_CONTRACT.md), [spool 보존 정책](../../DIRECT_SYNC_DATA_PLATFORM_NOTES.md). ACKED retention report/candidate는 자동 삭제 권한이 아니다.
- 상태/다음: **운영 준비 미입증**. Label 릴리스·운영 담당과 Main이 exact 후보의 install/첫 실행/cold boot/제거·재설치/rollback 근거를 연결하고 백업 범위·일관성·DPAPI 재사용 한계·보존 기간·RPO/RTO를 요구로 확정한다.
- 완료 기준: 데이터/CSV/current-state/공유 DB·lease/keyring/F5 journal/relay queue·spool·receipt와 identity가 수용된 절차로 보존·복원되고, 중앙 receipt 대조로 중복 없는 후속 작업이 확인된다. 실패·복원 불가와 손실 범위를 숨기지 않는다.
- 의존/병렬: LM-B04 후보 구성, LM-B02 운영 목표, 중앙 [Q07](../../../Program_Spec_Hub/BACKLOG.md#qualification). 정책·기존 증거 대조는 장비 실행과 독립 가능.

<a id="lm-b08"></a>
## LM-B08 · 호환 경로·조건부 F5·미조사 기능의 지원 범위

- 유형/우선: **지원 계약·조사 공백 · P1**. 코드 잔존을 현장 지원으로 오해하거나 대표 12기능을 전체 범위로 세면 안내가 어긋난다.
- 근거: [지원 경로](README.md#사용자경계지원-경로), [미조사 범위](README.md#명세-진행알려진-미조사-범위), [C-07](contracts.md#c-07), [RELEASE_GATE_CONTRACT](../../RELEASE_GATE_CONTRACT.md).
- 상태/다음: **대표 기준선 완료 / 전수 확인 남음**. Label·운영 담당이 명시 legacy 5단계/EXACT_RESCAN, F5 reconciliation payload·capability, 관리자/수정·메뉴/단축키, product-host 잔존 mode, 상류 PHS2 발행→출고의 실제 지원 조건·제외를 확정한다.
- 완료 기준: 각 지원 분기에 진입점·설정/권한·설치 활성 여부·업무/예외·수용 기준 또는 미확인 사유가 연결된다. 미분류 중앙 입력을 호환 성공으로 바꾸지 않는다. ERPnext는 별도 범위다.
- 의존/병렬: LM-B04 실제 구성, 중앙 [지원 경로 지도](../../../Program_Spec_Hub/INTEGRATIONS.md). 추가 조사 범위는 담당이 정하며 이번 작업에서 넓은 소스 조사를 재시작하지 않았다.

<a id="lm-b09"></a>
## LM-B09 · 설정·프로필·업데이트의 오래된 안내 정합

- 유형/우선: **확인된 문서/현행 소스 차이 · P1**. packaged 설정이나 공통 Machine profile을 모든 설치의 정본 위치로 잘못 안내할 수 있다.
- 근거: [CODEX](../../CODEX.md)의 설정/GitHub 자동 업데이트 요약, [프로필 안내](../LOGISTICS_RUNTIME_PROFILE.md)의 공통 경로와 [현재 설정 선택](operations.md#configuration), [릴리스 사용자 상태 계약](../../RELEASE_GATE_CONTRACT.md). 현재 앱의 `_can_apply_updates()`는 False이고 일반 fallback·onboarding·Machine 우선권을 구별해야 한다.
- 상태/다음: **기준선에 차이 반영 / 오래된 안내 수정 남음**. Label 문서·릴리스 담당이 현재 지원 배포 topology와 소스 근거를 대조해 적용 조건을 명시한다. 기존 문서는 이번 허용 5경로 밖이므로 보존했다.
- 완료 기준: 사용자 설정 vs packaged template, current-user vs Machine profile, ProgramData fallback vs 실제 데이터 위치, update 조회 vs installer 적용 책임이 안내와 일치한다. 기본 `off`나 코드 적용 차단을 실제 installed provider 관찰로 확대하지 않는다.
- 의존/병렬: LM-B04/08 및 중앙 구성 요약. 기존 안내 대조는 실제 실행 없이 가능.

<a id="lm-b10"></a>
## LM-B10 · mutex 범위와 실제 저장 위치 선택의 차이

- 유형/우선: **좁은 소스 교정·SaveRoot13 회귀 PROVEN / 잔여 수용·운영 공백 OPEN · P1**. 서로 다른 env를 가진 두 ordinary source launch가 같은 기존 custom 저장소에 쓰면서 다른 mutex를 가질 수 있었음은 [정적 영향 보고](E:/KMTech/coordinator-handoff-20260907-01a07992/label-save-root-impact/IMPACT.md)의 호출 경로로 확인했다. 실제 중복 GUI writer·손상이나 교정 전 실행 RED는 관측하지 않았다.
- 반영한 구현: [guard](../../label_match_single_instance.py)는 기존 [앱 writer resolver](../../Label_Match.py)와 같은 nonempty `custom_save_path` → `LABEL_MATCH_SAVE_DIR` → ProgramData 순서를 사용하며 null/빈 설정 fallback도 맞췄다. writer 위치·데이터/schema/업무 identity를 이동·변경하지 않았다. [이번 diff·해시·정적 확인](E:/KMTech/coordinator-handoff-20260907-01a07992/label-save-root-fix/IMPLEMENTATION.md)을 정본 근거로 한다.
- 회귀/상태: Main이 2026-09-07 13:09:40Z에 [실제 SaveRoot13](operations.md#saveroot13-evidence)을 수용했다. [선택한 회귀 소스](../../tests/test_label_match_single_instance.py)의 6개 정의가 실제 13 collected/13 PASS, 39 ordered setup/call/teardown 모두 PASS, failure/error/skip 0으로 연결됐다. custom 우선, 실제 writer의 default/env × missing/empty/whitespace/null fallback, 단일 process native key·중복 callback 제외·해제 후 재진입만 입증한다. 기존 config/default·정규화·native 수명 oracle은 보존됐고 env-first assertion만 승인된 custom 우선 요구로 바꿨다. 이번 호스트 검토는 기존 원본의 정적 대조이며 새 실행은 없다. 이전 313/939와 reader 실패는 원래 소스·환경 범위로 보존한다.
- 별도 잔여 항목: default packaged 시작에서 기존 custom C가 있어도 onboarding ledger/persistent scan source는 A를 선택할 수 있다. 앱 session sync는 C를 전달하므로 enqueue 이전 CSV·기존 spool을 나눠 [bounded 다음 대조](operations.md#storage-root-residual)를 진행한다. guard 이전 ledger 쓰기, settings가 없을 때 template 복사 시점, relative/alias의 실제 동일성도 이번 수정의 해결 범위가 아니다. **OPEN**, 추가 구현은 이 단위에 포함하지 않았다.
- 다음/완료 기준: [focused packet](E:/KMTech/coordinator-handoff-20260907-01a07992/label-save-root-vm-prepare/PREPARATION.md)의 독립/Main 검토와 실제 13개 수용은 완료했고 VM01 Label 예약도 해제됐다. 다음 좁은 준비 단위는 기존 source/source 두 GUI launch에서 서로 다른 env A/B·공통 settings C를 주어 두 번째 writer 생성 전 제외, 첫 소유자 종료/중단 뒤 같은 C의 pending identity 복구를 관측하도록 기존 진입·복구 경로에 결속하는 것이다. 실행은 Main의 별도 자원·지시를 따르며 이 packet의 SaveRoot13 선택을 GUI 시험으로 해석하지 않는다. packaged/source의 exact 설치 후보, onboarding/relay A/C pre-enqueue 발견 및 지원 경로 잔여 항목은 별도로 해결 또는 근거 있는 제한을 확정해야 한다. 이 기준들 전에는 LM-B10 전체를 종결하지 않으며 기존 Baseline8·최종 qualification도 미입증이다.
- 의존/소유: [LM-01](README.md#lm-01), [LM-B04](#lm-b04), [LM-B09](#lm-b09). 코드/spec는 Label 담당, 중앙 계약·준비도·우선순위 요약은 Main 소유다.

<a id="lm-b11"></a>
## LM-B11 · producer lifecycle 정상 종료 receipt 불일치

- 유형/우선: **도달 가능한 계약 불일치 교정·한정 50/150 회귀 PROVEN / native 연동 수용 NOT TESTED · P1**. F3 ACK 뒤 APP_CLOSE-only delta에 서버가 `RAW_LEGITIMATE`를 반환하지만 종전 Label과 final reader는 COMPLETE만 허용했다. 교정 전 문제의 근거는 관측된 runtime 실패가 아닌 정적 소스 대조다.
- 구현/근거: [준비 보고](E:/KMTech/coordinator-handoff-20260907-01a07992/label-producer-close-contract-fix/PREPARATION.md), [C-05](contracts.md#c-05). uploader의 exact nonprojecting lifecycle 검증을 ACK/retention에 적용했고 업무 COMPLETE·실패/거부·원래 runtime/명령 검증은 유지했다. reader만 완화하거나 F3·이전 receipt를 대신 넣지 않았다. Web 소스는 읽기 전용으로 보존했다.
- 상태/다음: **독립/Main 소스·packet 검토 및 실제 50 collected/50 PASS·150 ordered phase PASS 수용 완료**. [두 소스·네 명세의 소스 단위](E:/KMTech/coordinator-handoff-20260907-01a07992/label-producer-close-source-close/CLOSE.md)는 테스트 당시 working bytes를 유지한다. 이전 Label110/SaveRoot13/Modules3 결과와 실패는 각 원래 pins에 남으며 새 소스에 상속하지 않는다. [실제 transport/authority/native 준비 잔여](operations.md#producer-close-evidence)를 해결한 뒤 원래 N/I same-store identity·F3·정상 close·중단·재개를 검증한다.
- 완료 기준: 지정 source/provider/환경의 실제 50개/150 phase 회귀는 충족했다. 실제 F3 ACK 선행 APP_CLOSE delta·reopen-close 수신, 불완전 business/rejected/wrong-scope/key/hash/member/fence 거부와 native close custody의 연동 수용은 별도로 남는다. implementation/연동/수용/운영을 구분하며 LM-B11 전체 종결이나 Ready 승격으로 해석하지 않는다. Ready 0/6 유지.
- 소유: 코드·로컬 명세는 Label, 동결 backend 계약/회귀는 Web, runtime grants·VM 예약·중앙 INT-09/Q08·준비도 요약은 Main. LM-B06/10 및 FULL/설치 기준은 계속 OPEN이다.

## 갱신·종결 규칙

항목을 끝낼 때 기능·계약·수용 기준·실제 증거와 적용 한계를 같은 작업 단위에서 갱신한다. 제안→승인 요구 전환은 근거를 남기고, 모르는 기능을 결함 또는 미구현이라고 단정하지 않는다. 과거 FAILED/NOT TESTED를 보존하고 후속 결과를 정확한 후보에 연결한다. 공통 관계·우선순위·준비도 영향은 Main에게 전달하며 중앙 문서는 중앙 소유자가 갱신한다.
