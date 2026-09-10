# Label_Match 진행·공백·추가 제안

## 2026-09-10 · relay 예외 위치 기록 — 원인 확인 중

- 기존 UNKNOWN 예외 기록에 가장 안쪽8개 basename/function/line만 추가하고 메시지·인자·locals·payload는 제외한다. 기존 실패→성공 시험의 기준1 FAIL/1 PASS와 후속 loop/writer6 PASS, 동일44개 writer와 정확한 소스는 [운영 근거](operations.md#relay-exception-location-20260910)를 따른다.
- 자연 실패: 정상395113e 적용/복구 뒤 첫 cycle에서 `_run_command:215` stdout slicing을 확인했다. 유일한 소비자가 읽지 않는 text capture/tail을 제거해 Windows reader thread 두 개도 없앴다. 실제 무해한 undecodable child0/2 기준2 FAIL/4경고, 후속 loop/writer 포함8 PASS와 동일44개 writer를 [운영 근거](operations.md#relay-exception-location-20260910)에 기록했다.
- cad3b68 실제: 정상395→cad 적용/복구·123 파일·after-close 상태/DB/identity·fresh CAS release0을 확인했다. 자연 cycle2에서 TypeError 없이 process FAIL/exit2와 `existing_terminal_blocked`를 기록하며 원본 PHS2/세트·멤버/seal·네 durable 표0을 보존했다. [정확한 native 범위와 실패](operations.md#relay-exception-location-20260910)를 따른다.
- 남은 일: 실제 child `existing_terminal_blocked`는 별도 기존 업무 blocker로 보존한다. UNKNOWN은 전송 부재의 증거가 아니며 원래 F4/QR/F3/ACK/후속 흐름·전체 native UI·Today 비교는 미완료다. 강제 종료·수동 cycle·업무 replay는 하지 않는다.

## 2026-09-10 · 루트 화면 깜박임·잘림 — 소스 교정, 실제 수용 대기

- Main의95b 실제027에서 무입력 중 busy 제목/회색 버튼의 순간 표시를 확인했다. 주기 readback이 실제 후보를 찾은 뒤 기존 lane에 접수하도록 교정하고, 열린 세트의 불필요한 복원 조회·도중 입력·읽기 실패·종료 취소를 기존 시험에서 확인했다.
- 단계/상태·최근 완료 열의 실제 폭, 좌측 줄바꿈, 대기 상세의 한국어/줄 높이, 두 F5 footer를 바로잡았다. 기준13 PASS, 보정27 PASS와 마지막 변경의 scheduler/writer11 PASS 및 보존한 fixture 오류는 [운영 근거](operations.md#root-ui-20260910)를 따른다. 원본44개 writer와 업무 계약은 같다.
- 94c 후속 검토: render 예외 뒤 검증 접수/재예약을 보장하고 예상된 background busy 거절만 조용히 재시도한다. 비어 있지 않은 retry 목록은 기존 상세에 `자동 재시도가 예정되어 있습니다.`로 표시한다. 교정 전2 FAIL과 독립 host 시험 정리 보정, 후속24 PASS/13.35s·동일44개 writer는 [운영 근거](operations.md#root-ui-20260910)와 E 보고서에 기록했다. guest95b는 그대로이며 94c 중간 적용은 보류했다.
- a70 실제 부분 확인: 정상95b→a70 적용/복구 후 idle241프레임/36.016s의 버튼·제목 불변, 루트/네 탭과 F4 목록0건·안내·다섯 footer의 무잘림을 확인했다. [정상 lifecycle과 보존한 실패·정확한 native 범위](operations.md#root-ui-20260910)를 따른다. 설정/날짜/원문/오류/F5·실제 busy 입력 유지, 원래 F4 적용·F3/ACK/후속 업무와 Today 전후 비교는 미완료다.

<a id="lm-f4-list"></a>
## 2026-09-10 · F4 교체 목록 — 소스 구현, 실제 수용 대기

- 안내 문구 후속: Claude28/Main 합의로 단일 목록 건수, 대상→양품 반복·단일 적용·적용 전 닫기 소실과 실제 대기/오류 안내를 정리했다. 기존12 PASS와 마지막 문장 뒤 writer3 PASS, 동일44개 writer이며 동작 변경·새 시험은 없다. Main의95b 실제 목록 추가/편집 취소/삭제/닫기와 최종 소스의 native 미수용을 [운영 근거](operations.md#f4-editable-list)에서 구분한다.
- 실제 footer 후속: 95b 화면에서 목록/제출 건수와 세로 배치는 확인했지만 닫기 버튼과 안내 끝의 가로 잘림이 남았다. 두 생성문에 `width=0`, 안내에 `wraplength=700`만 추가하고 최종 기존 선택11 PASS/9.70s·동일44개 writer를 확인했다. [원 실패·정상95b 적용·Main 입력 인계와 후속 전체 버튼/안내 수용](operations.md#f4-editable-list)을 분리한다.
- 후속 교정: 빈 members+양수 QT 초안의 실제 admission gap을 닫았다. 표준 PHS2는 정확한 목록 부재를 거부하고 레거시 direct-seal은 실제 target/seal worker 조회로 호환한다. [한정 11 PASS와 남은 수용](operations.md#f4-editable-list)을 분리한다.
- 구현: 수량 선입력·개수 도달 자동 제출을 없애고 old→new 목록의 확인·수정·삭제·입력 취소와 명시적 단일 적용을 제공한다. 실제 대상 멤버 수, 추가 capability, 기존 원자 명령/복구/전자 seal·물리 PHS2 보존 계약을 유지한다.
- 근거: [LM-05](README.md#lm-05), [C-03](contracts.md#c-03), [정책](../MEMBER_EXCHANGE_POLICY.md), [사전 기준·실제 소스 검증](operations.md#f4-editable-list). 정상 저장소 migration은 기존 row/JSON/rowid/index/trigger 보존 및 rollback을 검증했다.
- 남은 일: Main에 인계한 guest95b의 목록0건·Submit0 상태에서, Main의 정상 적용 뒤 다섯 버튼과 목록 편집·일괄 교체·새 전자 QR 확인·원 세트 보존·F3/ACK 복구 및 후속 연결 흐름을 검증한다. Host 입력은 금지이며 source PASS는 실제 교체 성공이나 최종 GUI/성능 수용이 아니다.

## 2026-09-10 · 이력표 잘림 교정 — 실제 화면 검증 대기

- 머리글 우선 폭·넓은 창의 우측 공간·우측 탭 스타일·원문 높이 상한 교정과 기존 비 GUI64 PASS를 [운영 근거](operations.md#history-layout-20260910)에 연결했다. 원래 crop/014와 사전 실패를 보존한다.
- 남은 일: Main 배정 VM에서 실제 조회 화면의 세 머리글·네 탭/도구버튼, +/-·크기 왕복·원문/선택/Today 보존과 최종 Claude 검토. 새 F4 쌍 목록의 검토·수정 후 단일 원자 제출 요구는 별도 구현/검증이며 이번 교정으로 완료 처리하지 않는다.

<a id="lm-s05"></a>
## 2026-09-09 · S05 승인 단순화

- 소스 결과: 미사용 UI builder·보조 함수 11개와 전환기 비교 도구 1개 제거, 기존 취소/교체 scenario를 보존한 assertion 중복 정리, 같은44 writer identity/guard pin 갱신. [기존 focused 선택217 PASS와 범위](operations.md#s05-simplification).
- 보존: 현재 UI/legacy 입력 호환, F1/F4/F3·private payload와 복구, resident lifecycle, 실제 capture의 동적 refresh hook, shared vendored contract/zero-PE provider. Phase G의 lost-ACK·process-kill·backpressure regression과 물리 인쇄·liveness 조사 도구는 유지한다.
- 원본 저장소 계속 작업: Main이 기존 C: 변경을 `a57d50b52695030bc91eb2094469c9e3fd730a7c`로 commit하고 E:의 S05 delta만 한 번 반영했다. 후속 검토·명세·commit은 원본 `C:\company\program\Label_Match`에서 수행한다. E: source/checkpoint와 frozen receipt12 source·원 artifact는 보존한다. 남은 일은 Main의 S05 독립 검토, shared hub 및 실제 후속 작업의 writer pin 참조 정렬, 같은 기준의 전후 codebase 크기 비교와 향후 배포 범위 선택이다. 과거 frozen receipt의 pin을 새 값으로 고치지 않고 실제 후속 입력만 새 source에 맞춘다. push하지 않는다.
- 아래의 과거 Ready 0/6·VM 대기·Full 요구는 당시 근거의 범위다. Main이 종결한 선택 여섯 프로그램 qualification을 S05의 반복 gate로 다시 열지 않는다.

[제품·기능](README.md) · [계약](contracts.md) · [운영](operations.md) · [공통 백로그](../../../Program_Spec_Hub/BACKLOG.md) · [실제 준비도](../../../Program_Spec_Hub/READINESS.md)

<a id="lm-history-optimization"></a>
## 2026-09-09 · 이력 적용 최적화 — 검증 중

- 구현: helper-only 추출 대신 빈 보조 셀의 열 너비 조회와 중복 workbench 반영을 제거했다. private 소비자·한 callback의 완료 의미·오늘 색인/과거 details·F3/F4/durability와 44개 writer guard를 유지한다.
- 한정 증거: [고정 기준·결과 및 보존한 timing 실패](operations.md#history-optimization), 기존 focused 64 PASS와 adapter/pin 3 PASS. 실질 product 변경은 호출 1줄 제거와 빈 값 조건이며 새 상태/계층/상시 도구를 추가하지 않았다.
- 한정 native 증거: 배정 VM의 동결 v2/원본·후보 소스에서 1,000행 오늘/과거 각30회가 모든 사전 수치 제한·work/digest/pending/gate assertion을 통과했다. callback 중앙값은 20.3657→12.0721 ms·20.0075→12.5966 ms다. 실제 이력/집계/세션/입력 Tk 위젯에 한정하며 workbench 일부와 font fitting은 fake다.
- 후속 LM-A2: 오늘 모드·호환 전체 재스캔 시작/제품 접수의 중복 호출 3곳을 제거했다. 고정 work/state 비교는 오늘 render 3→2·과거 2 유지, 재스캔 각 단계 2→1 및 전체 상태/display/log/save digest 일치를 확인했다. 기존 실제 DataManager 테스트에 상태/presentation 검증을 결합했고 선택 7개와 pin 2개가 통과했다. 복구 ACK 메서드는 동일하며 새 ms/native 주장은 없다. [범위와 증거](operations.md#history-render-dedup).
- 남은 일: 사용자가 요구한 Computer Use 전체 Label/연결 업무는 남아 있다. Label 담당자는 전체 source/assets·calendar dependency·지속 relay 경로를 준비하며, Main이 VM·격리 backend·새 upstream dummy data와 정상 등록 입력을 배정한다. 기존 운영/복구 상태를 사용하지 않으며 native resource는 자체 task/process 종료 확인 후 Main에 반환했다. 이 항목은 component timing 통과만으로 완료하지 않는다.

최초 기준일: **2026-09-07**, 후속 갱신: **2026-09-08**. 최초 5문서 명세 기준선의 독립/Main 교차 검토와 승인된 LM-B10 guard·회귀 테스트·spec 소스 단위, 후속 **SaveRoot13 실제 13 PASS/39 ordered phase PASS** 수용을 반영했다. 2026-09-08 후속 소스 종결은 Main이 수용한 ProducerClose 실제 50 PASS/150 ordered phase PASS와 승인된 두 소스·네 명세를 연결했으며 이번 검토에서 runtime을 재실행하지 않았다. 후속 [잔여 소스 종결](E:/KMTech/coordinator-handoff-20260907-01a07992/label-residual-source-close/CLOSE.md)은 승인된 잔여 다섯 경로와 네 명세를 묶고 [M3/N6 사례·소스 매핑](operations.md#residual-source-evidence)을 완료했다. 문서·소스 단위 완료나 이 한정 PASS로 제품 Ready를 판정하지 않는다. P1은 필수 업무/계약·검증 공백의 우선순위, P2는 추가 요구 후보다. 담당은 책임 역할이며 실명·기한·목표 릴리스는 미지정이다. 실제 순서는 Main의 현행 지시를 따른다.

## 현재 진행

[receipt12 공개 이벤트·strict RAW 교정](E:/KMTech/label-install-qualification-20260908/RECEIPT12-SOURCE.md)은 `SET_CANCELLED`를 공개 `set_id`만 기록하도록 제한하고 `SEALED_TRANSFER_EXCHANGE_APPLIED`에서 `old_seal_qr_payload`·`new_seal_qr_payload`만 제거한다. 공개 set/intent/receipt/bundle·멤버 목록·version과 private 취소·복구·apply 상태는 유지한다. strict RAW allowlist에 정확히 두 이름만 추가하며 receipt/identity/hash/bytes/행·event 합계, `OBSERVED/RAW_EVIDENCE_ONLY/NOT_PROJECTED/NO_STAGE1_REDUCER`, quarantine/errors=0 및 runtime fence를 그대로 요구한다. 저장된 bytes와 근거 hash를 확인하여 focused60·inventory4 PASS와 parent causal6 FAIL을 재실행 없이 재사용했다. 동일44 writer identity/guard의 pin은 `507da9c952a88649cd06c090f45fb6e1bb5129de12260a747741b744ccc1d4cb`다. receipt11 `7c4abfa`/ZIP2094ba6a를 parent/recovery로 보존하고 단 한 번 고정·빌드한 수용 source12는 `da60e05e9bf07855f701fff328df70c10db0a094`다. Web05 normalizer의 기존 공개 형식을 사용하며 reducer/비밀 검증을 변경하지 않는다. [native 완료 근거](E:/KMTech/label-install-qualification-20260908/RECEIPT12-NATIVE.md)와 [최종 source 소유권 지도](E:/KMTech/label-install-qualification-20260908/RECEIPT12-SOURCE-OWNERSHIP.md)를 연결한다. 이 문서의 최종 native 기록만 갱신했으며 고정 artifact는 재빌드하지 않았다. Ready **0/6**이다.

| 구분 | 상태·근거 | 다음 행동 |
| --- | --- | --- |
| 승인된 명세 구축 | AGENTS와 4개 spec 기준선 작성·정적 검토 및 [독립 교차 검토](E:/KMTech/spec-hub-build-20260907/cross-review/REVIEW.md)·Main 대조 완료. [IMPLEMENTATION](E:/KMTech/spec-hub-build-20260907/Label_Match/IMPLEMENTATION.md) | 후속 변경마다 기능·계약·증거·남은 일 유지; 전수 기능·설치 자격과 구별 |
| 개발 작업 | LM-B10의 좁은 guard 교정·[독립 소스 검토](E:/KMTech/coordinator-handoff-20260907-01a07992/label-save-root-independent/REVIEW.md)·packet 독립/Main 검토 완료, 한정 회귀 PROVEN. [실제 검토·문서 갱신](E:/KMTech/coordinator-handoff-20260907-01a07992/label-save-root-actual-source-close/REVIEW-AND-UPDATE.md)은 기존 dirty 제품·테스트·HEAD/index·동결 packet 보존 | Main이 guard·회귀·명세와 기존 AGENTS 기준선을 한 번의 소스 단위 커밋으로 수용. LM-B10 잔여 수용·운영 범위 OPEN |
| producer-close 소스·한정 회귀 | [LM-B11](#lm-b11) 독립/Main 소스·packet 검토와 실제 50 PASS/150 ordered phase PASS 수용, [두 소스·네 명세 종결](E:/KMTech/coordinator-handoff-20260907-01a07992/label-producer-close-source-close/CLOSE.md) | native N/I·F3 close/reopen/서버 연동은 NOT TESTED. 당시 보존한 잔여 다섯 경로는 아래 소스 종결에 연결 |
| 잔여 완료 CSV·복구 소스 종결 | [독립 전체 diff·증거 검토](E:/KMTech/coordinator-handoff-20260907-01a07992/label-residual-source-review/REVIEW.md)와 Main 승인에 따라 다섯 source/test working bytes 보존·네 명세 갱신, [잔여 소스 종결](E:/KMTech/coordinator-handoff-20260907-01a07992/label-residual-source-close/CLOSE.md)에 parent/commit·working/blob pins 연결 | V03/V07 변경 13사례는 M3 내부, N6 6사례는 별도 선택으로 매핑 완료. native/backend/FULL은 source-frozen 후속 범위 |
| relay custom CSV 발견 교정 | [LM-B10 후속 구현·headless 검증](operations.md#relay-custom-root-evidence): 수정 전 custom 4 FAIL/대조 10 PASS, 최종 26 PASS. relay의 두 진입점과 기존 writer inventory pin 두 곳 교정 | 변경 단위와 [추가 source 입력](E:/KMTech/coordinator-handoff-20260907-01a07992/repo-parallel-0826/label-ready-improvement/CHANGE.md)을 Main에 전달. 원래 394c21f Stage/MainStage는 보존하고 successor source/host bindings 후 VM01 Setup/FULL 진행; native/설치/실제 transport는 미실행 |
| relay 독립 소스 종결·후속 FULL 준비 | [검토·commit·successor 입력 정본](E:/KMTech/coordinator-handoff-20260907-01a07992/repo-parallel-0826/label-relay-closure-review/REVIEW-PREPARE.md), frozen product/test와 45개 writer identity·guard 종류 독립 대조. [successor02 제어 독립 검토](E:/KMTech/label-rp-0908/REVIEW.md)에서 기존 pin·raw 405개·index 405개·clean status와 native Git 단계의 log/export·PS5/PS7 근거 대조 완료 | **당시 QUEUED**: VM01 Inspection 소유·Defect 대기, Label 실행 권한 없음. 기존 Stage/MainStage는 394c21f의 역사 근거이며 새 Stage/MainStage/Setup 및 FULL은 Main의 packet 수용·VM01 배정·live admission 뒤 별도 실행. 최초 source proof/refresh FAILED와 native/backend/설치 공백·Ready 0/6 유지 |
| VM01 소스 검증·계정 인계 | [실제 결과·미검증 수정](operations.md#relay-vm01-continuation): 기존 provider 재사용, Stage/MainStage·Setup/MainSetup PROVEN. 원래 Full은 2,259 PASS/72 FAIL/16 SKIP, MainFull FAILED; `.pytest_cache` 5개 추가 외 기존 파일 변경·삭제 없음 | 후속 593개 검사는 571 PASS/22 FAIL, 19파일 stable export·소스 전후 동일. healthy lifecycle 15/writer transition 6/Display2 waiting 행 수 1개 실패 보존. 실제 E: 복구 경로 제한 제거·initializer/portable fixture 후속 수정은 미검증이며 계정 전환으로 추가 실행 중단. [인계](E:/KMTech/label-vm3-20260908/ACCOUNT-HANDOFF.md)·VM01 task Ready 0/2/1 및 owned process 0 반환 기록. 제외 runtime cache의 전체 불변은 UNPROVEN, 설치·backend·복구 수용과 Ready 0/6 유지 |
| blackdwarfian 복구·portable 준비 완료 | [새 검증 범위](operations.md#label-black-continuation): recovery21·Display2 1·portable closure4 PASS, lifecycle/writer 잔여 교정 확인. focused03의 36 PASS/1 fixture FAIL을 보존하고 공유 fixture 교정 후 fresh2+소비자12 PASS, capture headless5 PASS. 실제 core 계약·writer45/pin 유지 | 최초 선택69개의 사례별 마지막 PASS와 실패 이력·source 범위를 연결했다. 기존 정상 public key의 portable build와 signature/canonical 검증을 유지한 정본 PlanOnly PROVEN, guest snapshot d39511a·3,390파일. 실제 설치 대상·사업/서버/장비/복구 수용과 Ready0/6은 Main의 후속 범위 |
| 실제 검증 | [M3 313/939 내부 변경 13사례와 별도 N6 6/18](operations.md#residual-source-evidence), [ProducerClose 50/150](operations.md#producer-close-evidence), [SaveRoot13 13/39](operations.md#saveroot13-evidence)를 각 원래 exact source/선택 범위로 유지; 초기 export·원래 reader 실패 보존 | Baseline8 causal RED는 NOT TESTED 역사 공백이며 이번 소스 종결 선행 조건이 아님. cohort 합산·재실행·통합 FULL/GUI PASS 확장 없음 |
| 운영 준비 | 목표 릴리스 실제 연동·설치·cold boot·재설치·rollback·통합 E2E의 전체 근거 UNPROVEN | LM-B04/06/07 및 중앙 Q06/Q07 추적. 임의 완료율 없음 |

### 2026-09-09 · 현재 F1 수정과 최종 artifact 수용

[정상 F1 계약](contracts.md#f1-capture-cancellation)의 원 세트와 관련 없는 로컬7 custody를 보존한다. 선행 resident10의 원 F4 ACKED3/VERIFIED/APPLIED·F3 ACKED1/completion1과 receipt11의 두 지원 review·6행 실제 ACK는 수용된 업무 이력이다. [현재 receipt12 native](E:/KMTech/label-install-qualification-20260908/RECEIPT12-NATIVE.md)는 남았던 exact14 transport review, timing06 prior-code recovery, 동일 receipt12 복귀, 정상 GUI 종료, 실제 coldboot/autostart 및 보존을 완료했다. 원 중앙 F1 lease의 EXPIRED_UNRECONCILED는 일반 expiry에 한정하며 미소비·미조정을 유지한다. 최신 업무11 tables/40 rows와 원 CSV prefix를 보존했고 APP_START1·APP_CLOSE1 외 업무 입력은 반복하지 않았다. Ready **0/6**.


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
- 2026-09-09 최종 Label native continuation: [receipt12 최종 native continuation](E:/KMTech/label-install-qualification-20260908/RECEIPT12-NATIVE.md)은 수용 source `da60e05e9bf07855f701fff328df70c10db0a094` / ZIP `782d585bf09396da8c33fbb3f1c3b24afc6a8716f73e0b4983b951a896633d6e` / manifest `829f5f2230add6f0245106c5b4dac40547f56941792c2da46dbf5318effa6111`에 결속된다. 원 assigned VM에서 동일 artifact의 정상 설치 attempt02 native0, exact retained14의 지원 `ack-reviewed` native0, 지원 current-user 제거·code-only 제거, 수용 timing06 prior-code 설치/보존, 최종 receipt12 복귀 설치03:42:16Z/native0와 readback0을 완료했다. GUI13은 기존 resident를 재사용하여 PHS2 대기·이전 완료 이력을 표시하고03:44:40Z/native0으로 정상 종료했다. 실제 정상 shutdown 뒤 Off/uptime0을 관측하고 새 boot03:46:01Z·Explorer4640/session1에서 원 SID의 resident7556이 03:46:22Z에 자동 시작한 것을 확인했다. 수동 product 시작 전 단일 resident·정상 HKCU Run·canonical task0·stop/fence 없음이며 postboot native0은 payload3,391개와 보호 파일25개, 최신 post-review backup의 업무11 tables/40 rows를 확인한다. 전체 상태 메타데이터는 40/51개 동일하며 나머지 11개 status/control/settings/log 변경을 기록했다. app_settings 필드별 차이는 미확인이고 전체 config/DB/schema/hidden-rowid 동일성이나 새 대상 설치를 주장하지 않는다. [최종 보존 readback0](E:/KMTech/label-install-qualification-20260908/RECEIPT12-FINAL-PRESERVATION02.json)은 원 receipt/metadata/spool/attempt1·audit1과 inserted10/quarantined4를 유지하고 queue11ACKED를 기록한다. 원 event CSV prefix 두 개가 동일하며 APP_START1·APP_CLOSE1만 추가됐다. 원 F1/F4/F3·shipping·로컬7 업무나 pre-F3 DB 복원을 반복하지 않았다. 첫 설치 native1/UAC 취소·지원 rollback, 별도 pre-execution policy rejection과 reader 실패는 이력으로 보존한다. [Main의 선택 범위 최종 native 수용](E:/KMTech/resume-after-input-20260908/LABEL-RECEIPT12-FINAL-NATIVE-ACCEPTED.json)은 `PROVEN_SELECTED_LABEL_FINAL_NATIVE_ACCEPTED`이며 Main msg_b42de21979ea 승인에 따른 정상 guest shutdown 뒤 [최종 VM Off/uptime0](E:/KMTech/label-install-qualification-20260908/RECEIPT12-FINAL-VM-OFF.json)을 03:58:51Z에 확인했다. 이 수용은 기존 assigned VM의 선택된 Label 범위에 한정한다. 전체 제품 Ready는 **0/6**이며 여섯 프로그램의 현재 조합 판정과 다음 S05 source workspace 결정은 Main 소유다.
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

- 유형/우선: **guard의 SaveRoot13 회귀와 후속 relay 발견의 headless 검증 PROVEN / 잔여 수용·운영 공백 OPEN · P1**. 서로 다른 env를 가진 두 ordinary source launch가 같은 기존 custom 저장소에 쓰면서 다른 mutex를 가질 수 있었음은 [정적 영향 보고](E:/KMTech/coordinator-handoff-20260907-01a07992/label-save-root-impact/IMPACT.md)의 호출 경로로 확인했다. 실제 중복 GUI writer·손상이나 guard 교정 전 실행 RED는 관측하지 않았다. 이번 relay 경로의 4개 RED와는 다른 범위다.
- 반영한 구현: [guard](../../label_match_single_instance.py)는 기존 [앱 writer resolver](../../Label_Match.py)와 같은 nonempty `custom_save_path` → `LABEL_MATCH_SAVE_DIR` → ProgramData 순서를 사용하며 null/빈 설정 fallback도 맞췄다. writer 위치·데이터/schema/업무 identity를 이동·변경하지 않았다. [이번 diff·해시·정적 확인](E:/KMTech/coordinator-handoff-20260907-01a07992/label-save-root-fix/IMPLEMENTATION.md)을 정본 근거로 한다.
- 회귀/상태: Main이 2026-09-07 13:09:40Z에 [실제 SaveRoot13](operations.md#saveroot13-evidence)을 수용했다. [선택한 회귀 소스](../../tests/test_label_match_single_instance.py)의 6개 정의가 실제 13 collected/13 PASS, 39 ordered setup/call/teardown 모두 PASS, failure/error/skip 0으로 연결됐다. custom 우선, 실제 writer의 default/env × missing/empty/whitespace/null fallback, 단일 process native key·중복 callback 제외·해제 후 재진입만 입증한다. 기존 config/default·정규화·native 수명 oracle은 보존됐고 env-first assertion만 승인된 custom 우선 요구로 바꿨다. 이번 호스트 검토는 기존 원본의 정적 대조이며 새 실행은 없다. 이전 313/939와 reader 실패는 원래 소스·환경 범위로 보존한다.
- 2026-09-08 후속 교정: [상주·예약 relay](../../user_relay.py)의 default scan source가 기존 custom C를 무시하던 분기를 교정했다. 명시 `--scan-source-dir` 우선과 empty/null/missing/invalid settings fallback은 유지하며, 수정 전 custom 4 FAIL과 후속 [headless 26 PASS](operations.md#relay-custom-root-evidence)를 기록했다. 실제 command builder·CSV scanner의 재진입 전후 발견과 queue 경로·spool/settings bytes 보존에 한정한다. 기존 writer inventory pin은 baseline에서 이미 불일치였으며 후속 source에 맞춘 두 상수와 기존 검사 3개도 이 26개에 포함된다. 45개 writer 식별자·guard 종류는 바꾸지 않았다.
- 별도 잔여 항목: default packaged 시작에서 기존 custom C가 있어도 onboarding ledger·registration `--sync-dir`는 여전히 A다. 앱 session sync는 C를 전달하며 기존 queue의 실제 재전송과 새 CSV의 enqueue/ACK는 [후속 실제 검증](operations.md#storage-root-residual)으로 남는다. guard 이전 ledger 쓰기, settings가 없을 때 template 복사 시점, 실행 중 설정 변경의 재읽기, relative/alias의 실제 동일성은 **OPEN**이다. 데이터 이동·프로필 재설계는 이 단위에 포함하지 않았다.
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
