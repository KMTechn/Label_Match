# Label_Match 진행·공백·추가 제안

## X13-B · PowerShell leaf 채택

R1의 구 설치본→X13-B 지원 전환은 57f52e1 shared preimage와 이번 release의 추가/교체 파일 pin에 한정한다. 양 엔진의 합성 upgrade/교체 후 검증과 변조·누락·링크 거부는 [현재 결과](operations.md#shared-powershell-x13b)를 따르며, 실제 설치 lifecycle 수용은 별도다.

정본 0.3.1 전체 5파일·manifest·lock을 다시 채택했다. 0.3.0 배열 결함의 두 실패 로그는 보존하며 양 엔진에서 singleton·중첩·빈 배열의 원본 parity를 재검증한다. 실제 lifecycle 수용은 별도다.

0.3.1/5파일과 standalone checker·패키징을 채택하고, 신뢰 확인 뒤 설치기 leaf만 위임한다. LM task migration·conflict receipt·ordinal inventory/복원은 앱에 유지한다. [검증·잔여 범위](operations.md#shared-powershell-x13b)의 합성 PS5/PS7·lifecycle 회귀와 실제 배정 대상의 설치/제거/복원 수용을 구별하며, 실제 lifecycle/GUI/전체 build는 이번 레인에서 수행하지 않는다.

## X04-B · shared core 채택

catalog 8개 leaf·renderer 호환 facade·고정 package·manifest/lock·portable/PyInstaller 생성 입력을 채택했다. [headless 검증](operations.md#shared-core-x04b)에서 v2 bytes/복구·LM profile/URL/진단 adapter, PNG/QR/반환 class와 기존 writer 경계를 확인했다. 인증 I/O·복구·snapshot의 추가 공통화는 이번 pin에 없는 후속 범위이며 실제 GUI·인쇄·설치 수용은 LM-B04에 남는다.

단독 checkout의 pin 검사는 앱 QA 진입점에서 자급하며 정본 교차 대조는 명시 실행 노드로 분리한다. 정본 checker 갱신 시 [검사 절차](operations.md#shared-core-x04b)로 복사된 검증 함수의 동등성을 확인한다.

X13-B package pin은 0.3.1/5파일이며 catalog/raster/runtime 계산 bytes는 유지한다. PowerShell leaf도 portable 및 실제 frozen data/배포 목록에 명시한다. portable의 기존 package 포함과 frozen runtime hidden import에 결속하고 factory lock은 변경하지 않는다.

## X05-B · producer runtime 공통화

25개 leaf/binding/transaction 함수를 pinned runtime facade로 채택하고 기존 runtime45·새 callback/ACK 원자성7 회귀를 통과했다. [검증 경계](operations.md#shared-runtime-x05b)의 LM reviewed 복구·transport·writer admission·신원 생성·예약 orchestration은 앱에 남는다. 실제 현장 복구/설치 수용은 기존 업무에 남으며 CA의 자동 복구 정책이나 전체 onboarding/relay 공통화로 확대하지 않는다.

## LM-W2 · 감사 응답성 개선

- 최종 w2fix3 host 검증은 **469 PASS**, R1–R5 재현 **28 PASS**, walkthrough/원 R6 **21 PASS**·소비자 집합 **1 PASS**·추가 호출자 **3 PASS**다. 기존469 노드 누락0으로 W1 감사350·내구33·이력40·B01/B03 응답성10을 유지한다. R4/R5와 수정 전 R6 실패는 보존하며 [최종 JUnit·원 실패·잔여 범위](operations.md#responsiveness-w2)를 따른다.

- LM-B03 한정 완료: 완전 이력의 논리 상태 설치와 표시 배치를 분리해 오늘 적용 중 입력을 허용한다. after 배치는 generation과 종료를 확인하고 새 완료·취소·집계 갱신을 보존한다. 기존 history40·새4 PASS. D03의 별도 read-only 이력 창과 읽기/과거 조회 gate 제거는 후속이며 [한계](operations.md#responsiveness-w2)를 유지한다.

- LM-B02 철회: 두 차례 검토의 R1–R4 거짓 부재·중복 완료 위험과 전체 바이트 검증 비용 때문에 위치 색인을 제거하고 같은 PC prefix의 전체 CSV 직접 검색을 유지한다. flush→재대조→fsync와 rename 시 재열거/중단을 보존한다. **보류:** 완료 존재 확인의 비용은 보관 파일 수에 비례하며 보관 정책으로 관리한다. [철회 근거·검증](operations.md#responsiveness-w2).

- LM-B01 유지: package review 정리·조회와 workbench F4 상태 표시는 기존 worker의 불변 snapshot이다. set·generation·업무 epoch guard와 행동 직전 정본 guard를 유지한다. R5 capture는 worker 완료 snapshot으로 개수·경고를 검증하고 R6 walkthrough도 review-only worker와 poll을 연결한다. 새 소비자 집합 검사는 앱·도구·테스트의 호출/override/동적 바인딩 변경 시 재검토를 요구한다. 실제 GUI·운영 지연은 [미검증 범위](operations.md#responsiveness-w2)다.

<a id="lm-w1"></a>
## LM-W1 · 감사 실패 처리·의존성·안내 정합

- LM-A01 소스 교정 완료: 이력 부분 성공을 거부하고 이전 활성 색인·행 위치를 보존한다. 격리 host 이력40 PASS(새9 포함), [로그·JUnit](D:/KMTech/program-improvement-20260912/work/Label_Match/w1/logs/history-01.xml). 실제 손상 이력 화면·현장 데이터는 미확인이다.
- LM-A02 소스 교정 완료: 생성·취소 조회 실패에서 마지막 경고·목록을 유지하고 오래됨/미확인을 표시한다. 격리 host review12 PASS(실제 SQLite 잠금 새4 포함), [로그·JUnit](D:/KMTech/program-improvement-20260912/work/Label_Match/w1/logs/review-01.xml). Tk 동기 조회 지연 개선은 이번 범위 밖이다.
- LM-A03 소스 교정 완료: 중앙 캐시의 timestamp 오류는 원본을 보존한 수리 잠금이다. 격리 host 복구15 PASS(새5·기존 midnight2 포함), [로그·JUnit](D:/KMTech/program-improvement-20260912/work/Label_Match/w1/logs/recovery-01.xml). 실제 손상 캐시 지원 수리·화면은 미확인이다.
- LM-C02 입력 정합 완료: portable chardet5.2.0 wheel을 hash lock에 추가하고 builder9개 version/hash 입력을 자동 대조한다. zero-PE11 PASS(새1 포함), [로그·JUnit](D:/KMTech/program-improvement-20260912/work/Label_Match/w1/logs/dependency-01.xml). wheel SHA256은 PyPI 및 격리 다운로드로 확인했으며 clean 설치·전체 빌드는 미실행이다.
- LM-D02 문구 교정 완료: F3 권한 확인·로컬 완료 저장과 중앙 전송 대기·확정·충돌·로컬 복구를 구별한다. 기존 F3/제출/충돌26 PASS, [로그·JUnit](D:/KMTech/program-improvement-20260912/work/Label_Match/w1/logs/f3-copy-01.xml). 로컬 내구 단계 전 성공 금지와 입력 보존 검사를 유지했으며 새 문구의 native 가독성은 미확인이다.
- LM-A05/LM-D01 문서 교정 완료: due/공정 재시도 요약을 통일하고 F4 목록·일괄 적용·추가 capability·새 전자 QR 절차를 작업자 정본에 반영했다. 소스/diff·참조 확인이며 앱 실행·설치 버전 확인 증거로 확대하지 않는다.
- 최종 host 수용: 감사 필수331개와 새19개 모두 PASS(중복 제거350개), 추가 focused를 포함한 고유407개 PASS. 계약299 PASS·직렬 내구33 PASS와 [JUnit 대조](D:/KMTech/program-improvement-20260912/work/Label_Match/w1/logs/acceptance-summary.json)를 보존했다. 병행 최초 내구 실행의2 FAIL은 진단2 PASS·직렬33 PASS에서 재현되지 않았지만 원인 미확정이며 원 로그/XML은 삭제하지 않았다. 전체 suite·실제 화면·서버 수용은 별도다([RESULT](D:/KMTech/program-improvement-20260912/work/Label_Match/w1/RESULT.md)).

## 2026-09-12 · Committed stale-runtime 복구의 호스트 교정

- Main/Web의 실제 APP_CLOSE75 승인·만료 fence6 판정에 따라 기존 ack-reviewed에 정확한 만료 authority 복구 옵션을 추가했다. 새 focused19·기존 선택13 PASS와 보존·거부 범위는 [운영 근거](operations.md#committed-stale-runtime-recovery-20260912)를 따른다. 서버/fencing/기존 receipt는 바꾸지 않는다.
- 다음은 Main의 최종 source/정상 command 검토와 한 번의 배정에서 원590b disposition·정상 forward acquisition·남은 원 batch settlement다. producer 종결 뒤 실제 같은 날 history와 새 최종 후보를 고정한 Today30/30을 수행한다. 현재 VM Saved/0, live recovery와 Today samples0이며 원23+게시 실패를 보존한다.

## 2026-09-12 · M06 표시 교정·격리 native와 남은 producer/Today

- 호스트 수정: durable capture 후 disabled 입력 정리와 deferred→package 실제 대기 readback. 이미 dismiss된 prewrite conflict의 종결과 원 queue/receipt/audit·수량·멤버십·F-key·focus를 유지하며 [검증/실행 경계](operations.md#package-waiting-display-20260912)를 따른다.
- Main은 원 M06 한-F3/same-key ACKED와 보호 shipping handoff를 수용했다. 이 Label lane에서는 재입력/F3/shipping을 반복하지 않는다.
- 완료된 제한 실행: 최종8d의 격리 native8장면·정상 close/native0, Main 결정에 따른 원 REVIEW 한 건의 지원 local ACK·감사1과21ACKED/3RETRY_WAIT, 근거 반출·자체 task2 제거 뒤 원76a3 Saved/0을 Main이 수용했다. 정상 설치1ac·원 marker/owner와 모든 실패 근거를 보존했다.
- 후속 정상 복구 실패: Main 배정의 원590b 한 재시도는 accepted/committed APP_CLOSE receipt와 STALE_RUNTIME_FENCE review를 함께 남겼다. 제품의 terminal authority/token 정리와 정상 CSV 발견을 보존해 최종21ACKED/1REVIEW/3RETRY_WAIT이며 지원 stop·Python0·자체 task 제거 뒤 원 VM09:47:29Z Saved/0이다. package preflight9개0과 실제 당일10행은 확인했으나 Today는 시작하지 않았다.
- 남은 일: Main/Web의 실제 receipt·runtime disposition과 기존 consumer에 없는 post-review authority 복구의 한정 제품 결정, producer settlement 후 최종 후보/실제 당일 history를 결속한 Today30/30. 원23+1 실패는 보존하며 격리 native/local ACK는 성능/전체 Goal 종결이 아니다.

## 2026-09-12 · M06 복구 후 계정 전환 checkpoint

- 실제 한 멤버 F3/lost-ACK는 같은 key의 로컬 PENDING→정상 재시작 ACKED와 중앙 단일 COMMITTED 효과로 확인했다. shipping은 Main이 보호 원 PHS2와 실제 package handoff를 수용·배정해야 한다.
- Main의 blackdwarfian 계정 후속 작업자에게 원76a3 Saved/0·정상 profile/runtime/source1ac·자체 stop marker·모든 실패 근거를 인계한다. account347 작업자의 유한 checkpoint이며 원 continuation은 미완료다.
- producer 원 quarantined SET_RESTORED REVIEW1과 RETRY_WAIT3의 실제 서버 판정/지원 복구가 남는다. package readiness9개0을 producer settlement로 간주하지 않는다.
- 긴 원문 입력이 wrapping-ready 동안 노출되는 문제와 deferred SUPERSEDED 집계가 실제 package PENDING을 누락하는 일상 대기 표시를 Main이 후속 범위로 정한다. 제품/observer를 조용히 바꾸거나 원 F3를 재실행하지 않는다.
- 실제 settled 당일 history와 최종 후보를 사전 결속하고 별도 전체 Today30/30 배정이 필요하다. 새 snapshot·case·limits·samples는 없으며 기존 실패와 기준을 유지한다. [정확한 인계](operations.md#m06-account-handoff-20260912)를 따른다.

## 2026-09-12 · M06 lost-ACK·Today 후속 — 호스트 준비, 실제 배정 대기

- 완료된 준비: 실제 제품1ac의123파일 입력·새 observer/ref·한 멤버 fault 출력 경로, 기존 함수/통계/criteria 보존과 두 소스의 관측 widget 결속을 [호스트 근거](operations.md#m06-today-preparation-20260912)에 기록했다. 제품 변경·clock 회귀 재실행은 없다.
- 실제 의존: CA 정상 seal/보호된 PHS2 handoff → Main의 원76a3 정확한 interval → 일반 F3 한 command lost-ACK/같은 key 복구 → quiescent 실제 당일 history → 별도 배정 Today30/30. 호스트에서 실제 binding·입력·PASS preflight·limits를 만들지 않는다.
- 유지: 원901–904/F4/F3/shipping은 재실행하지 않으며 case03의23 ACK+게시 실패와 만료 입력은 보존한다. 원래 수치 기준을 유지하고 business/성능/전체 Goal은 각각 실제 결과로 판정한다.

## 2026-09-12 · clock14 실패 진단·회귀 정합 완료

- 원인/교정: 초기 PHS2에서 lease를 요구하던 과거14검사를 현행 read-only admission과 실제 F3 발급 경로로 정렬했다. 제품 코드는 유지하며 과거2단계 plan의 원 요청/key·signed clock·unknown/definite 실패 구분도 별도 회귀로 남겼다.
- 근거: [격리 host32 PASS와 기존 계약38 PASS](operations.md#clock-recovery-tests-20260912). 원 baseline14 FAIL과 신규 fixture backoff/UTC 표현 실패는 D 보고서에 보존한다. 원13개 version-zero guard는 모두 유지했다.
- 종결/잔여: 이 한정 clock 회귀 단위는 완료다. 변경 없는 제품의 기존 native UI 근거를 재사용하며 성능·실제 F3/shipping·전체 Goal과 중앙 계약 변경은 포함하지 않는다.

## 2026-09-12 · 일상 화면 최소화와 상세 열기

- 구현: PHS2의 중복 안내·단계표·원문을 줄이고 제품 수량·다음 행동을 표시한다. `작업 상세 보기`는 전체 원문·세트/접수·대기 항목·진단 코드·절차를 제공하며 대기·실패·복구 안내와 기존 키/상태 계약은 유지한다.
- 검증: 격리 host151 PASS, native2 제외. Main 검토 뒤 명시적 invalid/상충 수량의 falsey fallback·실수 절삭을 제거했고 후속88 PASS로 정상48/12와13개 수량 경계를 확인했다. 기존 clock14 FAIL과 layout fake의 baseline 실패/보완을 [운영 근거](operations.md#routine-details-20260912)와 D 보고서에 구분한다.
- 완료: 제품1ac5729의 배정 VM 격리 native13상태/14PNG를 실제 확인했다. Space 상세 왕복·입력/focus, 경고·수량 미확인·이력의 관리자 확인,1366×768/글자1.4배를 포함하며 native03 정상 종료0이다. 선택적 pywin32 helper 실패2회와 UTF8 reader 실패를 보존하고 기존 Win32 API adapter/명시 UTF8으로 한정 교정했다.
- 반환/한계: own462파일을 D에 SHA 대조 보존하고 임시 guest root·task·process를 정리해 원76a3를06:33:15Z Saved/0으로 반환했다. 이번 UI 단위는 완료이며 실물 스캐너·업무 receipt·설치·Today 성능은 미검사다. 당시 clock14 FAIL은 원 근거로 보존하며 후속 진단은 위 clock 회귀 항목을 따른다. 원 업무의 [한정 근거](operations.md#routine-details-20260912)는 유지한다.

## 2026-09-11 · 원래 F4 복구·F3 실제 진행

- Main의 Web78 전환·정상 grant 뒤 같은 runtime8df·원 VM에서 저장 F4를 같은 key/2953B command로 한 번 재시도하여 ACKED/attempt3, 중앙 receipt1·새902/903/904·seal2를 확인했다. 화면 QR 해독값을 정상 guest 입력으로 확인해 VERIFIED/APPLIED가 됐으며 물리 스캐너 장비 수용과는 구분한다.
- 원 PHS2/raw1을 유지한 일반 F3에서 완료 event1·local marker1·outbox ACKED/attempt1, fresh lease1/ACKED와 저장 COMMITTED receipt를 확인했다. 화면은 다음 PHS2로 복귀했다. Web 독립 readback은 중앙 효과1건·원902/903/904의 AVAILABLE package·lease 소비1건을 확인했다. 저장 JSON 기본 공백 hash와 compact canonical hash의 차이도 실제 원문 재계산으로 해소했다.
- Today 첫 시도: 같은 baseline a7e57b7/current8df의123-file export, 실제 당일11행/완료 세트1건 snapshot, 정상 READY·아홉 업무 대기 count0을 확인했다. Main은 실제 profile CA 보존 guard 교정과 실행을 수용했다. 기존 GUI 정상 종료0 뒤 정상 UI 설정 저장이 이전 fingerprint와 달라 첫 baseline observer가 GUI 초기화 전 종료1로 중단됐다. [실제 실패·수정한 준비 순서](D:/KMTech/optimization-implementation-20260909/GOAL-RESUME-20260911/Label_Match/transition-continuation-20260911-ctx8980/today-current8df/FIRST-INVALID-RESULT.md)는 업무 DB·기존 원문 prefix·다른 보안 파일 불변과 APP_CLOSE1건을 확인한다. 관측0/0이며 성능은 미입증이다. 원 실패를 보존하고 정상 종료 후 실제 설정 hash를 공통으로 고정한 당시 새 case02만 준비하고 다른 worker 자원 창 동안 재실행하지 않았다.
- 후속 Today case03은 개별 정상 응답23회 뒤24번째 상태 파일 게시 PermissionError로 중단했다. 저장 결과24행은 보존 확인 후 게시 실패를 구분하는 근거이며 수용된30회가 아니다. 정상 종료 때 Input1 열 너비253→191도 고정 guard와 충돌해 observer 종료1을 보존했다. current 관측·재시도·수치 수용은 없고, 제어 파일 게시 수정 후보만 호스트에서 검증했다. [실패·후보·원 VM 인계](D:/KMTech/optimization-implementation-20260909/GOAL-RESUME-20260911/Label_Match/transition-continuation-20260911-ctx8980/today-current8df/CASE03-FAILURE-AND-HANDOFF.md)에 따라 원 업무·보안 보존, 릴레이 정상 정지와 helper31개 해제를 끝내 Main/Defect에14:11:57Z 인계했다. 당시 미해결 열 너비 guard의 후속 호스트 교정은 다음 항목과 구분하며 새30/30은 미실행이다.
- 2026-09-12 호스트 observer/doc 종결: 최종 `f92a8671`의 실제 `read_layout`은 두 고정 소스가 구성하는 세 operator pane을 읽고 legacy `content_pane`은 선택적으로 기록한다. 기존27개 host 검사/종료0·구문 검사와 고유 진행 파일 probe를 재사용했고 이번에는 정적 소스·1317B delta와 문서만 대조했다. 이전 a4 후보의 없는 pane AttributeError와 `read_layout`을 대체했던21개 검사의 결속 공백을 보존한다. [최종 버전·근거·한계](operations.md#today-host-repair-20260911)에 따라 reader·source exports·설정·업무·통계·기준·원9월11일11행 입력은 유지한다. 합성 host geometry는 native 수용이 아니며 guest 적용·새30/30은 미실행이다. Main은 작은 pane 교정을 직접 대조했고 중앙 수용을 소유한다. Claude는 USER-ENDED/NOT COMPLETED로 완료 verdict·PASS·대체 검토 대기가 없다.
- 다음 의존성은 기존 M06 Inspection receiving → CA → Label 일반 F3/lost-ACK reconciliation → quiescent 실제 당일 history → 배정된 Today 성능 block이다. Inspection이 원 VM76a3·live M06/공유 원장을 독점하며 이 Label 호스트 종결은 자원 인계가 아니다. 원901–904/F4/F3/shipping은 완료되어 재실행하지 않는다. 새 Label Today 입력·case는 아직 없고, 만료된9월11일 case03은23 acknowledged + 저장된 게시 실패1행/current0·수용30/30 없음·동결 limits 없음으로 보존한다. 실제 새 입력 이후에만 날짜·행/세트 수·event 구성을 기록해 공통 case를 결속하고, 동일 snapshot과 기존 기준식으로 새 baseline30의 limits를 고정한다. [최소 실행 handoff](D:/KMTech/optimization-implementation-20260909/GOAL-RESUME-20260911/Label_Match/host-closure-20260912/HANDOFF.md)를 따르며 전체 Label native·성능·Goal은 미완료다.
- 기존 relay terminal review/lastcycleFAIL과 실패 근거는 보존한다. post-F4 snapshot을 즉시 가정한 기존 reader의 실패도 제품의 정상 F3 refresh와 구분한다. 후속 고유 업무·lost-ACK 복구·full-GUI Today30/30은 남으며 [정확한 실행·제한](operations.md#original-f4-f3-continuation-20260911)을 따른다.

## 2026-09-11 · 저장된 교체 요청의 주 화면 안내

- 실제 0c73 복원 화면에서 F3 비활성 상태와 `포장 준비`/F3 안내의 불일치를 확인했다. 저장 F4 목록 자체는 원 쌍·거부·읽기 전용 안내를 보존하며 1920×1080에서 잘리지 않았다.
- 기존 exchange attempt 한 번의 관측으로 안내와 버튼을 맞췄다. 격리 회귀91 PASS와 기존 writer4 PASS/native0, 동일44개 identity/guard 및 새 pin을 확인했고, 실제 Claude의 정확한8dfcee7 소스 검토는 필수 수정 없는 PASS다.
- 정상5파일 적용·123파일 readback과 실제1920×1080 주 화면/F4 확인을 마쳤다. 포장 보류 안내·F3 비활성/F4 활성, 원901→904 목록·거절·읽기 전용 상태를 확인했으며 새 재시도·F3는 없다. 초기 제목 녹색·일반 절차 힌트의 잘림은 비차단 표시 한계로 남긴다. 원 relay 종료1/terminal review와 이전 실패는 보존했다.
- 위 UI 전환 이후 Main/Web backend·정상 grant 뒤 원 F4 같은 요청/새 seal과 일반 F3를 실제 진행했다. [후속 근거](operations.md#original-f4-f3-continuation-20260911)와 [당시 UI 근거](operations.md#restored-exchange-guidance-20260911)를 구분하며, 후속 고유 업무·lost-ACK 복구·최종 Today30/30은 남는다.

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

[receipt12 공개 이벤트·strict RAW 교정](D:/KMTech/cold-program-material/from-E/KMTech/label-install-qualification-20260908/RECEIPT12-SOURCE.md)은 `SET_CANCELLED`를 공개 `set_id`만 기록하도록 제한하고 `SEALED_TRANSFER_EXCHANGE_APPLIED`에서 `old_seal_qr_payload`·`new_seal_qr_payload`만 제거한다. 공개 set/intent/receipt/bundle·멤버 목록·version과 private 취소·복구·apply 상태는 유지한다. strict RAW allowlist에 정확히 두 이름만 추가하며 receipt/identity/hash/bytes/행·event 합계, `OBSERVED/RAW_EVIDENCE_ONLY/NOT_PROJECTED/NO_STAGE1_REDUCER`, quarantine/errors=0 및 runtime fence를 그대로 요구한다. 저장된 bytes와 근거 hash를 확인하여 focused60·inventory4 PASS와 parent causal6 FAIL을 재실행 없이 재사용했다. 동일44 writer identity/guard의 pin은 `507da9c952a88649cd06c090f45fb6e1bb5129de12260a747741b744ccc1d4cb`다. receipt11 `7c4abfa`/ZIP2094ba6a를 parent/recovery로 보존하고 단 한 번 고정·빌드한 수용 source12는 `da60e05e9bf07855f701fff328df70c10db0a094`다. Web05 normalizer의 기존 공개 형식을 사용하며 reducer/비밀 검증을 변경하지 않는다. [native 완료 근거](D:/KMTech/cold-program-material/from-E/KMTech/label-install-qualification-20260908/RECEIPT12-NATIVE.md)와 [최종 source 소유권 지도](D:/KMTech/cold-program-material/from-E/KMTech/label-install-qualification-20260908/RECEIPT12-SOURCE-OWNERSHIP.md)를 연결한다. 이 문서의 최종 native 기록만 갱신했으며 고정 artifact는 재빌드하지 않았다. Ready **0/6**이다.

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

[정상 F1 계약](contracts.md#f1-capture-cancellation)의 원 세트와 관련 없는 로컬7 custody를 보존한다. 선행 resident10의 원 F4 ACKED3/VERIFIED/APPLIED·F3 ACKED1/completion1과 receipt11의 두 지원 review·6행 실제 ACK는 수용된 업무 이력이다. [현재 receipt12 native](D:/KMTech/cold-program-material/from-E/KMTech/label-install-qualification-20260908/RECEIPT12-NATIVE.md)는 남았던 exact14 transport review, timing06 prior-code recovery, 동일 receipt12 복귀, 정상 GUI 종료, 실제 coldboot/autostart 및 보존을 완료했다. 원 중앙 F1 lease의 EXPIRED_UNRECONCILED는 일반 expiry에 한정하며 미소비·미조정을 유지한다. 최신 업무11 tables/40 rows와 원 CSV prefix를 보존했고 APP_START1·APP_CLOSE1 외 업무 입력은 반복하지 않았다. Ready **0/6**.


<a id="lm-b01"></a>
## LM-B01 · F4 작업 안내와 후속 QR 확인

- 유형/우선: **소스 기준 문서 정합 완료**. 목록 편집·일괄 적용·새 전자 QR 확인을 작업자 정본에 반영했다.
- 근거: [앱 `_prompt_new_seal_verification`](../../Label_Match.py)와 [작업자 정본의 기본 순서](../LABEL_MATCH_WORKER_GUIDE.md); [LM-05](README.md#lm-05), [C-03](contracts.md#c-03). 원본 물리 PHS2 유지와 새 전자 QR 확인은 동시에 성립한다.
- 상태/다음: LM-D01에서 정본의 수량 선입력·1~2개 고정 안내를 현재 목록 절차로 교정했다. 3쌍 이상 추가 capability와 원본 물리 PHS2 유지·새 전자 QR 확인을 명시했다. 실제 설치 버전·화면/기존 캡처 적용 범위는 LM-B04의 남은 확인이다.
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

- 2026-09-10 F4 실제 단일 Apply는 instruction-conflict/OPERATOR_REVIEW·attempt2로 보존 중이다. [기존 창의 명시적 같은 요청 재시도](operations.md#f4-explicit-retry-20260910)는 원 C에서142 PASS로 검증했으며, live source 적용·실제 retry/새 QR·F3와 Today 전체 GUI 전후 비교는 미실행이다. 별도 검토 CSV acknowledgment 뒤 자연 relay PASS/idle·13 ACKED는 transport에 한정하며 이 업무 잔여를 닫지 않는다.
- 실제 Claude 후속의 저장 목록 안내·명시적 거부 표시·불확정 목록 읽기 전용 재열기는 기존 소스147사례로 검증했다. 선행142·원 실패·ee73 packet을 보존하며 최종 source/backend 검토, 후속 후보의 정상 적용과 native 화면·재시도/새 QR/F3는 별도 잔여다. 불확정은 receipt-only이며 재전송·DB 초기화로 해소하지 않는다.
- a362 실제 Claude의 F1–F3는 다른 저장-command review의 읽기 전용 재열기·접수 가능성 안내·lease 재시도 보류 문구로 교정했다. 기존 영향25사례 PASS와 수정 전3 FAIL을 [최종 소스 근거](operations.md#f4-explicit-retry-20260910)에 보존하고, 변경 없는 coordinator의 이전 근거를 재사용한다. live source는 별도이며 native 재시도·새 QR/F3·전체 GUI 수용 잔여는 유지한다.
- 유형/우선: **구성·검증 공백 · P1**. 소스의 정상 경로와 설치 PC의 선택 프로필/장비가 다를 수 있다.
- 근거: [운영 구성·설정](operations.md#configuration), [장비](operations.md#devices), [LM-01/02/05/10/12](README.md#lm-01). Machine anchor, current-user profile/DPAPI, packaged settings와 실제 쓰기 위치, F4 capability, F5 driver·spool을 구분한다.
- 상태/다음: **실제 조합 미확인**. 릴리스·Label·현장 담당이 기존 후보 근거에서 artifact/source/test 식별, 실제 module/provider/overlay·Python/라이브러리, 비밀 없는 profile identity·권한·flags, scanner·프린터·사운드·화면 조건을 연결한다.
- 2026-09-09 최종 Label native continuation: [receipt12 최종 native continuation](D:/KMTech/cold-program-material/from-E/KMTech/label-install-qualification-20260908/RECEIPT12-NATIVE.md)은 수용 source `da60e05e9bf07855f701fff328df70c10db0a094` / ZIP `782d585bf09396da8c33fbb3f1c3b24afc6a8716f73e0b4983b951a896633d6e` / manifest `829f5f2230add6f0245106c5b4dac40547f56941792c2da46dbf5318effa6111`에 결속된다. 원 assigned VM에서 동일 artifact의 정상 설치 attempt02 native0, exact retained14의 지원 `ack-reviewed` native0, 지원 current-user 제거·code-only 제거, 수용 timing06 prior-code 설치/보존, 최종 receipt12 복귀 설치03:42:16Z/native0와 readback0을 완료했다. GUI13은 기존 resident를 재사용하여 PHS2 대기·이전 완료 이력을 표시하고03:44:40Z/native0으로 정상 종료했다. 실제 정상 shutdown 뒤 Off/uptime0을 관측하고 새 boot03:46:01Z·Explorer4640/session1에서 원 SID의 resident7556이 03:46:22Z에 자동 시작한 것을 확인했다. 수동 product 시작 전 단일 resident·정상 HKCU Run·canonical task0·stop/fence 없음이며 postboot native0은 payload3,391개와 보호 파일25개, 최신 post-review backup의 업무11 tables/40 rows를 확인한다. 전체 상태 메타데이터는 40/51개 동일하며 나머지 11개 status/control/settings/log 변경을 기록했다. app_settings 필드별 차이는 미확인이고 전체 config/DB/schema/hidden-rowid 동일성이나 새 대상 설치를 주장하지 않는다. [최종 보존 readback0](D:/KMTech/cold-program-material/from-E/KMTech/label-install-qualification-20260908/RECEIPT12-FINAL-PRESERVATION02.json)은 원 receipt/metadata/spool/attempt1·audit1과 inserted10/quarantined4를 유지하고 queue11ACKED를 기록한다. 원 event CSV prefix 두 개가 동일하며 APP_START1·APP_CLOSE1만 추가됐다. 원 F1/F4/F3·shipping·로컬7 업무나 pre-F3 DB 복원을 반복하지 않았다. 첫 설치 native1/UAC 취소·지원 rollback, 별도 pre-execution policy rejection과 reader 실패는 이력으로 보존한다. [Main의 선택 범위 최종 native 수용](E:/KMTech/resume-after-input-20260908/LABEL-RECEIPT12-FINAL-NATIVE-ACCEPTED.json)은 `PROVEN_SELECTED_LABEL_FINAL_NATIVE_ACCEPTED`이며 Main msg_b42de21979ea 승인에 따른 정상 guest shutdown 뒤 [최종 VM Off/uptime0](D:/KMTech/cold-program-material/from-E/KMTech/label-install-qualification-20260908/RECEIPT12-FINAL-VM-OFF.json)을 03:58:51Z에 확인했다. 이 수용은 기존 assigned VM의 선택된 Label 범위에 한정한다. 전체 제품 Ready는 **0/6**이며 여섯 프로그램의 현재 조합 판정과 다음 S05 source workspace 결정은 Main 소유다.
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
- 근거: [CODEX](../../CODEX.md)의 설정/GitHub 자동 업데이트 요약, [프로필 안내](../LOGISTICS_RUNTIME_PROFILE.md)의 공통 경로와 [현재 설정 선택](operations.md#configuration), [릴리스 사용자 상태 계약](../../RELEASE_GATE_CONTRACT.md). 앱 내부 updater apply는 LM-F01에서 제거했으며 후보·manifest 조회와 외부 installer 책임을 구별한다. 일반 fallback·onboarding·Machine 우선권도 구별해야 한다.
- 상태/다음: **기준선에 차이 반영 / 오래된 안내 수정 남음**. Label 문서·릴리스 담당이 현재 지원 배포 topology와 소스 근거를 대조해 적용 조건을 명시한다. 기존 문서는 이번 허용 5경로 밖이므로 보존했다.
- 완료 기준: 사용자 설정 vs packaged template, current-user vs Machine profile, ProgramData fallback vs 실제 데이터 위치, update 조회 vs installer 적용 책임이 안내와 일치한다. 기본 `off`나 코드 적용 차단을 실제 installed provider 관찰로 확대하지 않는다.
- 의존/병렬: LM-B04/08 및 중앙 구성 요약. 기존 안내 대조는 실제 실행 없이 가능.

<a id="lm-b10"></a>
## LM-B10 · mutex 범위와 실제 저장 위치 선택의 차이

- 유형/우선: **guard의 SaveRoot13 회귀와 후속 relay 발견의 headless 검증 PROVEN / 잔여 수용·운영 공백 OPEN · P1**. 서로 다른 env를 가진 두 ordinary source launch가 같은 기존 custom 저장소에 쓰면서 다른 mutex를 가질 수 있었음은 [정적 영향 보고](E:/KMTech/coordinator-handoff-20260907-01a07992/label-save-root-impact/IMPACT.md)의 호출 경로로 확인했다. 실제 중복 GUI writer·손상이나 guard 교정 전 실행 RED는 관측하지 않았다. 이번 relay 경로의 4개 RED와는 다른 범위다.
- 반영한 구현: [guard](../../label_match_single_instance.py)는 기존 [앱 writer resolver](../../Label_Match.py)와 같은 nonempty `custom_save_path` → `LABEL_MATCH_SAVE_DIR` → ProgramData 순서를 사용하며 null/빈 설정 fallback도 맞췄다. writer 위치·데이터/schema/업무 identity를 이동·변경하지 않았다. [이번 diff·해시·정적 확인](E:/KMTech/coordinator-handoff-20260907-01a07992/label-save-root-fix/IMPLEMENTATION.md)을 정본 근거로 한다.
- 회귀/상태: Main이 2026-09-07 13:09:40Z에 [실제 SaveRoot13](operations.md#saveroot13-evidence)을 수용했다. [선택한 회귀 소스](../../tests/test_label_match_single_instance.py)의 6개 정의가 실제 13 collected/13 PASS, 39 ordered setup/call/teardown 모두 PASS, failure/error/skip 0으로 연결됐다. custom 우선, 실제 writer의 default/env × missing/empty/whitespace/null fallback, 단일 process native key·중복 callback 제외·해제 후 재진입만 입증한다. 기존 config/default·정규화·native 수명 oracle은 보존됐고 env-first assertion만 승인된 custom 우선 요구로 바꿨다. 이번 호스트 검토는 기존 원본의 정적 대조이며 새 실행은 없다. 이전 313/939와 reader 실패는 원래 소스·환경 범위로 보존한다.
- 2026-09-08 후속 교정: [상주·예약 relay](../../user_relay.py)의 default scan source가 기존 custom C를 무시하던 분기를 교정했다. 명시 `--scan-source-dir` 우선과 empty/null/missing/invalid settings fallback은 유지하며, 수정 전 custom 4 FAIL과 후속 [headless 26 PASS](operations.md#relay-custom-root-evidence)를 기록했다. 실제 command builder·CSV scanner의 재진입 전후 발견과 queue 경로·spool/settings bytes 보존에 한정한다. 기존 writer inventory pin은 baseline에서 이미 불일치였으며 후속 source에 맞춘 두 상수와 기존 검사 3개도 이 26개에 포함된다. 45개 writer 식별자·guard 종류는 바꾸지 않았다.
- LM-C03 소스 교정: onboarding·GUI/guard·relay의 root resolver를 통일했다. fresh custom C는 ledger·registration `--sync-dir`도 C로 선택하고, 기존 ledger A가 있으면 A를 보존·진단한다. R1은 최초 legacy 등록부터 환경 적용·relay·재시작까지 LOCALAPPDATA(A)를 선택하며 onboarding 상태도 ProgramData 과거 파일보다 우선한다. ProgramData fallback은 등록 문맥·상태 없는 standalone 전용이고, 등록 후 P를 계속 쓰려면 운영자가 custom_save_path를 수동 설정한다. R2는 선택·분리 진단을 기본 WARNING 로그 채널에서 검증한다. settings template은 복사 전에 같은 값으로 해석한다. 원본 등록 재현 FAIL→PASS·일곱 fixture 입력 보존·기준 경로 대조, 두 process native callback 제외·해제와 split 설치의 원 데이터/identity/queue/spool 보존은 [Wave 3 등록 수명주기 회귀](D:/KMTech/program-improvement-20260912/work/Label_Match/w3fix2/RESULT.md)로 연결한다. guard 이전 ledger 초기화 순서, 실행 중 설정 변경/race, 모든 filesystem alias, 실제 GUI 복구·queue 재전송·enqueue/ACK는 **OPEN**이다. 설정 자동 기록·migration·프로필 재설계는 제공하지 않는다.
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
