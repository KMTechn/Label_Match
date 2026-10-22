- w9firstupdsub483 / LM R4 live 첫 설정 공식 stop: 성공한 내구 경계와 살아 있는 relay owner 대기에 협력 취소를 연결하고, 정상 rc 0/APP_STOP_REQUESTED_CLOSE 및 정상 보고서·기존 보호 거절을 보존한다. 순수 process/task 조회만 기존 10초 감독 자식으로 분리하며 mutation/Verify/실제 relay는 포함하지 않는다. 격리 main→onboarding→공식 fresh 부재 모델과 query timeout을 확인하며, 실제 현장 150초 수용·기존 보호 P2 종결은 독립 검토/후속 앱 회차에 남는다([원격 정상 멈춤](operations.md#remote-app-stop)).

- w9firstupdsub468 / REVIEW-119-LM-R4-1 P1-1: 보존 자격이 있는 같은 PC 재결속 전에 저장 자리·폴더 생성·교체 권한·scope·신원/manifest/키 결속을 검사하고 키 열기 실패에도 정상 보고서와 기존 관리자 회복 분류를 보존하며, 검증된 진행 epoch에만 reattach_rotation_admitted로 기존 profile 토큰·producer 보호 자격을 함께 교체한다. 회전 뒤 저장 실패는 원본과 정상 보고서를 보존하고 별도 고정 진단을 남긴다. 진행 표지 또는 실패 진단이 있으면 READY 시작 단축을 거절하고, 정확히 결속되면 다음 공식 시작의 기존 ABSENT_RETRYABLE 입구에서 같은 키로 한 번 재결속해 READY로 복구하며, 실패/거절은 그 실행을 멈춘다. 새 키·새 발급·신원 삭제·서버/허가 변경은 없다. 격리 자식 종료와 다음 공식 onboarding 회귀를 포함한다([계약](contracts.md#같은-키-재결속의-저장-중단-진단)).
- w9firstupdsub466 / P3 시험 결함: 8290f94f1078da21af84ccb5df33a2773fada40a의 의도한 현장 문구 표준화에 맞춰 writer fence 시작 거절 시험의 보고서·stderr 단언을 현재 INSTALLATION_RECOVERY_REQUIRED_MESSAGE 전체 상수로 대조한다. 제품 문구·종료값·순서·상태·원인·trace·무쓰기 단언은 보존한다.
- w9firstupdsub463 / 포장 119 R4 안내 창 멈춤: 본창 전 등록 복구·시작 거절·첫 설정 오류·설치 확인 경고·품목 오류·품목 재시도·설치 mutex 복구 안내가 공식 요청으로 정상 닫히고 기존 startup trace에 APP_STOP_REQUESTED_CLOSE를 남긴다. 문구·안전 취소/종료 rc·원인 code·신원·등록 보고서는 보존한다. Tk 생성 실패의 native 안내에도 상한을 두며, 기존 5초 중복 안내와 30초 화면 실패 안내는 자체 종료 예외다. bounded fake Tk·공식 요청 뒤 격리 Python 자식 종료와 실제 Windows 창 수용은 구분한다([원격 정상 멈춤](operations.md#remote-app-stop)).
- w9firstupdsub464 / 포장 119 R4 재결속: 등록기의 409 `reattach_proof_required`에 기존 키의 공식 challenge·서명·reattach를 연결한다. 같은 신원/키와 proof·회전 자격·영수증 결속을 검증하고 기존 준비 완료 경로로 저장한다. 실패는 원 재결속 요구 상태로 보존하며 새 키·새 발급·신원 삭제·반복은 하지 않는다. 다른 키의 관리자 복구는 유지한다. 가짜 서버 집중 RED/GREEN과 실제 서버·현장 PC 수용을 구분한다([운영 계약](operations.md#onboarding-registration-recovery)).
- w9firstupdsub435 / REVIEW-119-LM P1 둘 + REVIEW-119-LM-2 P1-1/P2-1: 현재 판 relay의 모든 시작·복구·준비 대기는 한 절대 마감을 공유한다. 부모는 spawn 기준 기본600초에 survival을 포함해 PID/마감을 전달하며, 자식은 결과 여유10초를 뺀 값 이하로 줄인다. trial 두 launch도 부모 신원을 유지한 min(업데이트240 마감, 공유 relay 마감)을 받아 admission 시도·재시도에서 사망/소진 뒤 추가 retry·새 launch0, exit20이다. 분리 -inf와 붙임 값 모두 UPDATE_PREPARATION_FAILED/20이며 다른 인수 parse는 유지한다. 부모 경로의 값 오류·누락 재발급0, 지난 마감 준비/새 시작0, 비정상 exit 즉시 실패. 부모 사망/교체/미증명은 기존 코드로 exit20이며 이미 시작한 relay 강제 종료0이다. 독립 진입만 자기 진입+600이다. 시작 업데이트240·보호 증명·설치기 상한·공개 가림 허용 목록은 유지한다. 잔여 예산 행렬·부모 사망·반복의 headless 회귀와 실제 현장 수용은 구분한다([운영 계약](operations.md)).
- w9firstupd / G63·G64 P3: 제품 고침 0. 주인이 멈춘 뒤 우리 코드가 그 폴더 앞뒤를 대는 SQLite 읽기 자리는 0 이고, active_work_probe 는 앱·키트 흐름에서 부름 0 이다. -wal·-shm 휘발·immutable 판정 금지 규칙은 contracts.md 「수량·시각·로컬 파일」 끝 두 문단.
- w9firstupdsub309 / G62 지킴: 독립 PowerShell 진입 10개의 dot-source·Import 적재 집합과 선택 함수·고정 본문을 PS5.1 AST로 읽어 Verb-Noun 함수 정의를 대조한다. OS 허용 목록은 이름별 이유를 명시하며 제품 스크립트는 실행하지 않는다.
- w9firstupdsub306 / G60 지킴: git 추적 비 시험 .ps1/.psm1 중 비 ASCII 파일은 UTF-8 BOM 정확히 하나를 요구하는 별도 조립 관문 시험으로 고정한다.
- w9firstupdsub299 / G36 전수 대조: direct-sync 설치 도우미를 포함한 공개 실패 생산자 여섯 곳에 같은 ASCII 계약과 PS5.1/PS7 × 949/65001 벡터 회귀를 적용했다.
- w9firstupdsub299 / L5 닫음: sub-184 Q1 relay PID 양의 int 검사, sub-186 Q1 앞 실행 트랜잭션 전 감사 기록의 현재 제거 상한 제외(보존·128 유지), sub-186 Q2 S-a 반대 증거 거절, G36 공개 실패 줄 ASCII를 구현했다. 검토자 단언은 유지하고 저장소/하네스 준비부만 맞췄다. 계약·운영의 L5 절과 집중 회귀를 따른다.
- w9firstupdsub299 / sub-184 Q2 닫음: 기존 전용 회귀에 매 관측 relay 정확히 하나·GUI writer 최대 하나 단언을 보강했다. 실제 bootstrap/update_lock 경합·GUI 정확히 하나·둘째 요청 재사용 rc 0·기록 바이트 불변을 지정 바탕과 고침에서 각각 8P로 확인했다. ANSWER-2354의 레인 판정대로 process_snapshot 대역을 띄우기와 같은 OS 프로세스 경계의 짝으로 허용한다. 바탕 GREEN인 지킴 시험이며 실제 CIM/창 수용을 주장하지 않는다. 시험만 별도 커밋했다.

- w9firstupdsub267 바탕 회귀 후속: 고정 시험 authority 식별자의 병렬 회차 충돌은 단독·직렬 원시험 결과와 분리한다. 복구 위임의 정적 허가 집합, signed G 역방향 stderr 기대, release metrics 기대 및 준비 단계 뒤 미확인은 [바탕 F 장부](D:/KMTech/program-improvement-20260912/work/program/w9firstupd/sub-267/resume-0045/BASELINE-FAILURES.md)에 남긴다. 원자 게시 deadline을 쓰는 시간 시험은 단독 확인하며, 보호 영역·권한 규칙은 이 고침에서 바꾸지 않는다.

## L2 — 제거 사슬·관리자 작업자 증명·숨김 manifest

G53은 생성 시각을 다시 확인한 실제 OS 셸 조상만 제외하고 다른 인수의 앱 경로와
runtime/사슬 밖 프로세스 거절을 유지한다. G28은 보호 작업자 바인딩과 증명 넷을
검사하며 사용자 제거 보고의 operator_sid를 대조한다. G35는 공용 0.3.8 전체
package/manifest/lock과 bootstrap pin을 채택한다. 구현·회귀·남은 수용 범위는
`D:/KMTech/program-improvement-20260912/work/program/w9firstupd/sub-294/RESULT.md`를 따른다.

## G49 — 첫 설치의 트랜잭션 전 감사 기록과 재시도

설치기 자신의 트랜잭션 전 기록·사본만 있는 install-audit는 새 PC 판정을 막지 않으며 파일은 보존한다. 등록 시도·active 울타리·다른 잔재·증명 실패는 기존 거절/복구 길을 유지한다. AST 원문 갈림길·기록/재개/정리·돌연변이 회귀는 sub-282 증거를 따르며 전체 설치/등록 READY와 현장 수용은 별도 회차다.


- w9firstupdsub267 검토 회귀: 작업자 계정 누락의 비공개 예외 메시지 정규식 기대를 기존 typed pending 표지와 WORKER_ARGS_INCOMPLETE reason으로 맞춘다. 콘솔 조회·쓰기 금지와 기존 성공 sentinel 단언은 유지한다.

- w9firstupdsub267 검토 보완: 최상위 일반 trap은 개인 진단/공개 변환 실패를 기존 REPORT_UNAVAILABLE로 수렴시키고 finally에서 명시 실패 종료한다. 되돌림/작업자 인수 충돌의 사전 적재 rc 0을 닫고 정상 되돌림 성공 출력·typed pending·권한·디스크 형식을 유지한다.

- w9firstupdsub267: 실제 설치의 작업자 계정 누락·빈 값 거절을 기존 typed pending 경계 안으로 옮겨 사전 적재 helper에서도 `PENDING_TARGET_INVALID` / `WORKER_ARGS_INCOMPLETE`·rc 1·공개 실패 한 줄·개인 진단/파일 쓰기 0을 유지한다. 거절 조건·권한·디스크 형식은 그대로다.

- 첫 업데이트(11/1): A-PREP 빈 catch가 2차 오류를 남기지 않음(sub-147 P3-1).
- 첫 업데이트(11/1): profile 별칭을 등록 origin과 견줘 정상 재사용이 FULL로 떨어짐(같은 꼴 장부 G06, P2).
- 첫 업데이트(11/1): frozen 릴리스 길(verify_frozen_release_assets) 시험 17 — frozen 길을 다시 쓰기 전 정리 필수(관리자 06:0x).
- 첫 업데이트(11/1): 세 앱 공용 「emitter 항목 단위 catalog 대조」 시험 하나 — 지금 포장 전수 시험은 조립 이름·변수·키워드로 만든 emitter를 모으지 않고, catalog마다 집합 한 단언이라 항목별 실패가 갈리지 않음(sub-148 P2 둘, 같은 꼴 장부 G27 공용 후보).

- w9firstupdsub244 / B28: 배치 진단 공개 변환에서 선택 필드가 없어도 최초 자식 종료 코드·있는 예외 종류를 보존하도록 고쳤다. 불완전한 보호 보고서는 미검증으로 남기며 원 진단·권한·디스크 형식은 바꾸지 않는다. [운영 계약](operations.md)과 PS5.1/PS7 선택 필드 회귀 시험을 연결한다.

- w9firstupdsub244 / B16·B26: 기존 검토된 공개 셸 결과 `web`/`exit` → 앱 종료 코드 0/3을 [셸 계약](contracts.md#new-screen-shell-write-guard)에 명시했다. 구현·종료 순서·fallback 판정 변경 없이 명세 빈칸만 보완했다.

- w9firstupd / 기준일 119: UI 6ced39fb(76파일·c83ed483720e2b61), 다리 글·원문 코드·교환 두 칸·전체 제품 합계, 부분 제출 확인과 기존 결정 즉시 release, PIN 없는 작업자 네 동작을 구현했다. 아래의 60초 미달 Undo 유예는 이 확인 계약으로 대체된다. M124(10/15 18:03 확정): SET_HOLD·F5_HOLD·SUPPORT_HANDOFF·PHS_LOOKUP 네 동작은 작업자가 PIN 없이 한다. 일반 RECHECK와 격리 파일 복원은 섞인 시트/처리기이므로 기준일에는 관리자 PIN을 유지한다. L04 작업자 재조회와 L05 관리자 복원을 다른 시트·action으로 나누는 일은 첫 업데이트에서 w9mgrui가 설계한다. sheets.*.pin은 메뉴 묶음·표시이며 인증 방식을 새로 정하지 않는다. admin·cancel_completed는 기존 모양, 완료 취소는 kind:form과 P7의 기존 승인을 유지한다. 부분 제출·PIN의 좁은 독립 검토와 실제 화면/PC 수용은 1차·관리자 인계다. 셸 v3.16.3.1을 고정한다.

- 기준일 P2 스캔 막이: 화면 6ced39fb·셸 v3.16.3.1에 반영했다. 열린 입력 판의 스캐너 꼴 입력은 앰버 띠로 알리고 처리 0건이다.

- P3 셋(레인 review-scanguard/REVIEW-LM.md·FREEZE-P3-1018.md 10/19 14:3x 판정): ① generated-settings 전이 시험은 바탕 실패로 stock 3릴리스 구간에 도달하지 않음 → 11/15 시험 정리. ② 허용 5경로×3릴리스 before 해시가 기존 단언 밖 → 11/15 단언 보완. ③ README·operations·BACKLOG의 화면/셸 고정 문구 누락 → 이번 115 문서 커밋에서 갱신.

- 새 화면 이음: v2.0.104 후보의 writer/lane 끝내기·초기화 예외 정리·시작 재시도·건별 관리자 재조회/보류·관리자 비상 링크를 구현한다. 셸 v3.16.2 READY 19/19와 최종 UI 4차 97efc8e7 52/52를 합쳤다. 실제 창·현장 PC·조립판 설치 packet은 확인 대기다([계약](contracts.md#new-screen-shell-write-guard)).

- v2.0.101 조립: 셸 v3.16·패킷 native 핀 검증은 빌더 몫. 상한 있는 다른 스레드용 저장 함수의 on_force_exit 연결을 확인한다. 실제 WebView2/스캐너/현장 설치·넓은 회귀는 inspection6 담당이며 headless PASS로 대체하지 않는다.
- LM-7 B: 이름만 등록·내구 outbox·동명 확인·다음 트레이 신원 대응 구현됨. `stage_a=false`, 숨김 없음. 실제 화면과 회사 서버 확인은 별도다. [등록 B 계약](worker-roster-b.md).

# Label_Match 진행·공백·추가 제안

- w9firstupdsub247: 106·107 공통의 종료 감사 두 종류(`APP_CLOSE_CANCELLED`, `멈춤 요청 미룸`)가 서버 catalog 두 벌에 없어 정상 relay ACK를 막는 결함을 로컬 전용 분류로 고쳤다. emitter 전수 대조와 격리 재현·정상 업무 ACK·로컬 감사 재열기 회귀를 추가한다. 기존 업무 CSV·spool의 수동 검토 건 복구와 실제 서버·현장 PC 수용은 이 수정에 포함하지 않는다.

- 첫 업데이트: 승인 from 표에서 v2.0.99/100을 뺄지, qrcode/tests 10파일의 정확한 바이트에 묶인 제거 선언 계약을 둘지 정한다. 현재 107 제자리 교체는 그 파일 집합 차이를 거절하며 지원은 코드 전용 제거 후 새 설치다.

- G43 / LM113 L1: relay stop 표지가 있으면 후보 적용을 DEFERRED·UPDATE_RELAY_STOP_REQUESTED(recovery_pending 키 없음)로 미룬다. 기존 onboarding 재개 뒤 다음 로그온 적용을 사용한다. headless 회귀와 실제 로그온 수용은 구별한다.

- bootstrap 없는 SYSTEM 적용이 정상 후보를 실패 판으로 막음: 제품 C(bootstrap 부재면 적용 전 DEFERRED)는 10/16 로그온 장면의 경쟁 실측 뒤 정한다.

- 부작용 없는 업데이트 검증 진입점(제품, 첫 업데이트 뒤): 외부 놓기 도구가 설치된 `auto_update`를 import하면 앱 자기 startup trace·실패 증거를 앱 평소 자리에 쓰는 연결을 분리하여, import만으로 쓰기 0인 검증 입구를 제공한다.

- w9firstupdsub195 ①(107): 39807d8의 고정 원인 전달을 유지하고 길 A(a282142)는 새 revert 커밋으로 제거한다. 두 회복 원인 모두 실제 첫 설정 실패·보고만 남은 재시작에서 WMX0010·WMX0006–0007 고정 안내와 작은 코드 줄·관리자용 자세히를 표시한다. 재연결 진행 글 없이 다시 enroll하지 않고 신원·자격을 보존한다. F1 실제 첫 실행 회귀와 독립 검토·검사 5 수용은 구분한다.

- w9firstupdsub195 첫 업데이트(11/1): 길 A(a282142 꼴) + F2(정상 CA 파일 남음 → profile FileExistsError로 서버만 새 세대)·F3(한 세대 journal 게시·시작 때 마저/자기 바이트 되돌림)·F4(다른 결속 거절 뒤 enroll 금지 결정 보존, 길 A 몫)·F5(통신 장애 → 네트워크 원인)·F6(영수증 값 공개 보고서 0) 고침 → 좁은 다시 검토 → 검사 5 실물(409 → 길 A → READY); 길 B 관리자 승인 identity-recovery 입구도 첫 업데이트 몫이다.

- w9firstupdsub193: 104 공개 기계 설치가 지원 제거 뒤 lifecycle 조회에서 script 범위의 없는 임대를 읽던 결함을 호출 helper의 명시적 임대 참조로 고친다. PS5.1/7 메모리 helper와 기존 fence/자식 실패 회귀를 확인한다. 설치/되돌림 코드의 독립 검토, 새 판 packet 빌드, 검사 6 지원 제거→공개 재설치→같은 작업자 READY 실측은 1차의 후속 확인 대상이다.

U4 앱: 같은 현품표의 취소 ACK 전 새 포장 전송을 기다리고, F2 결과와 옛 서버/되돌리기 거절 안내를 구분한다. 새 화면 `scan_cancel`은 기존 취소 outbox를 사용한다. 계약 대역 짝 시험 4종(새 포장·끊김 순서·F4 새 봉인·옛 서버 안내), 취소 ACK 재대조, 실제 PHS2의 F2/scan_cancel 뒤 같은 표 5회 스캔 로컬 완료를 headless로 확인했다. 실제 U3 서버의 작업군 되돌림·현장 PC/폰 출고 수용은 부모 레인의 후속 검증이다.

- 설치/등록 주소 비교: 기계 표지와 등록에 쓴 relay endpoint가 다르면 `RECOVERY_REQUIRED`로 시작·relay 재사용을 거절하고 자료·대기열을 보존한다. 서버 반환 profile은 비교하지 않는다. 실제 재설치·안내 화면·옛 relay 종료 수용은 랩 확인 대상이다([절차](operations.md#onboarding-writer-admission)).

- w9firstupdsub167lm: portable direct-sync relay 자식의 앱 적재를 없애고 기존 product host와 relay 시작 관문을 유지한다. 격리 RED→GREEN·인수/종료/오류 및 전송 결과 대조는 focused 검증 범위다. 실제 바퀴 CPU 4초 미만·stock packet/설치·넓은 회귀는 현장 PC inspection6 몫이며, 안전한 무효화 근거가 없는 프로세스 간 판 지문 캐시는 추가하지 않는다.

- w9firstupdsub158: 검사실 잔량표는 일반/과도기/끊김 스캔과 현재 상태·보류·관리자 재판독에서 포장 시작 전에 거절한다. 서버 `RESIDUAL_TAG_NOT_ALLOWED`는 확정 거절로 자동 재전송하지 않는다. 실제 장치·화면·회사 서버 짝 확인은 후속 범위다([계약](contracts.md#residual-tag-rejection)).

- L02a: 반복 checkpoint 쓰기 실패의 건별 UNKNOWN 보류·원본 보관·다음 세트 진행과 10분 네 번째 건의 전체 저장 장애 안내를 구현한다. 날짜 만료 없이 보존하고 관리자 확인 전 자동 감사 재생·같은 제품/새 현품표 재처리를 막는다. CSV writer/flush의 미확정 오류 및 이미 완료한 CSV 후속 저장 오류는 기존 복구 경로다. 실제 HDD·창·전원 중단과 CSV 격리 확대는 후속 확인 대상이다.
- 창 먼저 시작(w9firstupdsub166): READY·신선한 같은 판 relay는 주 창 뒤에서 전체 검증한다. 공유 전송 보류·실패/소유자 없음 뒤 FULL 재시작·허가 시간 초과 DEFERRED를 구현한다. 실제 HDD 20초 목표, 상태 줄·스캐너·재전송과 넓은 회귀는 현장 PC inspection6에서 확인한다([운영](operations.md#window-first-start)).
- sub166 archrev 18:44 교정: 단일 GUI 소유 뒤 표식 기록, 물류·PIN·이름 등록 송신 입장 보류, FAST 품목 오프라인 fallback·재시도와 시작 정보 고정 전달을 반영했다. 중복 바로 가기와 보류 중 각 업무 요청은 좁은 headless 회귀로 확인하며 실제 화면 수용은 현장 몫이다.
- sub166 archrev 19:4x 덧: 보내기 확인 실패 때 다시 켜기 안내·하단 「다시 확인」을 추가했다. 배경 재확인 성공은 동일 표식 해제, 실패는 유지, 연타는 한 번만 확인한다. STARTUP_ 보류 PIN 안내는 틀린 횟수로 세지 않는다고 알린다.
- w9firstupdsub63 관리자 입구·기계 배치·PENDING·기존 첫 시작 연결 후보를 구현한다. 같은 SID의 상승 SSH도 자기 프로필 준비·등록·Run·relay/화면을 첫 일반 작업자 시작까지 미루고 실패 시 새 공용 링크·PENDING을 회수한다. LM-2의 키트 관리자 가지 연결과 실제 VM 설치/첫 시작 전후 키트 Rollback 증명은 남는다. 아래의 기존 묶음 결정은 보존하며 이 후보의 승인·조립/출시는 1차와 키트 담당의 후속 판정이다.
- 작업자 바로 가기: 공용 기계 단계와 현재 사용자 단계의 설치·기록·복원을 구현한다. PS5.1/7의 격리 .lnk 회귀로 대상·인수·아이콘·원본 바이트·다른 루트 보존을 확인한다. 관리자 직접 설치 뒤 작업자 개인 링크 처리는 첫 시작 onboarding에서 같은 UserOnly helper를 받을 후속 자리이며, 실제 Explorer·검은 창·아이콘·현장 복귀는 랩 확인 대상이다.
- 원격 정상 멈춤(w9firstupdsub94): `user_relay.py --request-app-stop`으로 GUI의 기존 입력 보존·정상 종료와 relay stop을 요청한다. SQLite checkpoint/close를 적용하며 실제 PC의 프로세스 종료·WAL/SHM 부재·키트 되돌림은 별도 확인 대상이다([절차](operations.md#remote-app-stop)).

w9firstupd sub-151: 포장 상단 확인 수 N(웹 설정 1–6, 기본 4)을 앱이 읽어 세트마다 고정해 쓴다(구현·가짜 서버 시험 PROVEN). 남음(NOT TESTED): 실서버(bbcab868) 맞춤·현장 PC 확인, Tk 관리자 메뉴 보기 칸, 기록 표 열(5칸 고정 — N>4 세트는 상세에서 전체), 「다음 세트부터 N개」 Tk 표시.

w9firstupd sub-114: 서버가 PIN을 거절한 건도 명시적으로 미확인 보류하고 다른 포장을 계속할 수 있다. 안내·intent·단추가 같은 판정을 쓰며 대상·작업자 변경과 감사 실패는 보류하지 않는다. 기존 저장 형식·원본·요청 키·수량·원장을 보존한다. 실제 화면·장치·운영 서버 수용은 [운영 범위](operations.md#package-admin-pin-ui-lane)에 남는다.

LM-W5: 건별 복구의 관리자 PIN 요청·중앙 조회·로컬 적용을 기존 UI lane의 두 단계로 옮겼다. PIN 입력과 표시·목록 갱신은 UI 단계이며, 창 닫힘 뒤에도 내구 적용은 끝낸다. 중복 접수·빈 결과·취소/예외의 대기 표시와 reset의 화면 소유를 headless 회귀로 확인한다. 이 lane에는 foreground/background 구분 API가 없어 기존 busy 판정을 유지한다. 실제 화면·장치·서버 및 생성 선언의 통합 수용은 [운영 범위](operations.md#package-admin-pin-ui-lane)에 남는다.

### 7-결정. 정해진 줄 (관리자 10/11 02:51–03:36 — 묶음 계획 FIRST-INSTALL-BUNDLE-1014 §3-7b·§3-7c·§3-8; archplan 이 명세 가지와 합칠 때 덮지 않음)
| # | 정해진 것 |
|---|---|
| LM-7 | **B(등록만) = 이번 묶음**(이름만·승인/PIN 없음·번호는 화면만·로컬 저장 뒤 보냄·숨김 없음 stage_a=false), **A(30일 숨김) = 다음 판**(웹에 30일 숨김 계산이 없음). 조건: 동결 전 격리 웹(플래그 켬) 랩 증명, 플래그 꺼짐(503) 때 깔끔한 안내 + 웹 사전 등록 이름 고르기·멈춤 0, 화 10/13 12:00 점검, 회사 서버 readback 은 10/19 켠 뒤. 웹 계약 = W/w9webux1008/sub-23/RESULT.md |
| LM-17 | 옛 수동 마감(PARTIAL)은 LOCAL·ONLINE 모두 거절한다. 새 현품표 재스캔은 회사 서버에서 확인한 현재 작업군 멤버 수가 1–3이고 찍은 제품 수와 같을 때만 완료한다(원 TRANSFER 봉인 수와 구별). 멤버 수를 모르면 「서버 연결 뒤 마감」, 수가 다르면 「모자란 상자: 대체품으로 채워 정상 마감 — 못 채우면 마감하지 말고 따로 두고 관리자에게」를 보이고 현재 상자를 유지한다. 제품 4개 정상 완료와 옛 현품표의 재스캔 미달 완료·60초 되돌리기는 유지한다. 실제 낱개 수·멤버 보내기는 다음 판 |
| LM-1 | PC 안에서만 끝나 PACKAGE가 없는 새 현품표(PHS=2) 상자는 완료 안내·이번 작업 목록·스캔 기록·기록 상세에 「출고 전 관리자 보충 필요」를 표시한다. 기록을 다시 읽어도 기존 완료 상태에서 같은 안내를 계산한다. 전송 대기·ACK·충돌·미달 되돌리기 유예는 기존 상태를 표시한다. 옛 현품표(LEGACY) 상자는 과도기 옛 상자 출고 길을 사용하므로 이 안내를 표시하지 않는다. 이번 묶음(UI 고침 한 바퀴) |
| 숨김 설치 | **이번 묶음은 막는 줄만** — 제품 변경 0, 지금 거절을 시험으로 고정(w9firstupdrd). 숨김 설치 길 전체 = 다음 판. 기준일 밤 설치 = 현장 사람이 Windows 허용 창(UAC) 승인(§8-1) |
| 글자 층 | K1(3곳)·문안 v2 「글자만」 줄 = 이번 묶음(w9fieldfit1008 sub-30 표·apply_changes.py, 개수 맞을 때만) |


## 목표 명세 고칠 목록(10/11)

- 지원 source 사용자 제거·되돌림의 자료 세대 증명: 고정 bootstrap 및 관리 기록·활성 versions 무결성·신뢰 키를 검증한 활성 판의 전체 inventory·판·build에 결속했다. FG-01의 A 고정/B 활성/B 자료 하한과 초기 설치·지원 배치·첫 업데이트 기록 형태, 위조/부분 일치 거절을 headless로 교정했다. headless 회귀와 실제 Windows 설치 수용은 구분하며, 실물 L7은 설치 레인에서 확인한다.

- LM-12 글자 층: K1-LM-0001/0002/0013·문안 v2 C02a(두 대화상자)·공통 C05 적용. C01e/C01f/C02b/C04e는 코드 필요로 남는다. 공통 C03 조치 없음. writer 선언·설치 후보 해시는 현재 소스로 재생성하며 역사적 기준과 비교·저장·전송 값은 유지한다.

- 출처: [목표 명세](README.md) §7(w9archplan 10/11, 기준 v2.0.99 55cab72). 아래 기존 줄은 그대로 둔다.
- 누가: 포장 코드 주인 w9firstupd, 설치 설계 w9firstupdrd. 판: 「다음 판」 = 10/14 동결 묶음.

| # | 종류 | 무엇 | 등급 | 판 | 누가 | ◆ |
|---|---|---|---|---|---|---|
| LM-1 | 바로잡기 | PC 안에서만 끝나 PACKAGE가 없는 새 현품표(PHS=2) 상자는 완료 안내·이번 작업 목록·스캔 기록·기록 상세에 「출고 전 관리자 보충 필요」를 표시한다. 기록을 다시 읽어도 기존 완료 상태에서 같은 안내를 계산한다. 전송 대기·ACK·충돌·미달 되돌리기 유예는 기존 상태를 표시한다. 옛 현품표(LEGACY) 상자는 과도기 옛 상자 출고 길을 사용하므로 이 안내를 표시하지 않는다. 웹 보충 길은 WEB-N1의 후속 범위 | P1 | 이번 묶음 | 구현·headless 시험 완료(w9firstupd sub-73) | — |
| LM-2 | 바로잡기 | **원격 숨김 설치 받기**. 현장 키트가 일반 작업자 세션에서만 돌고, 상승은 제품 안 Windows 허용 창(UAC RunAs)으로 한다 → 관리자 원격 숨김 설치 길이 없다(`install-onsite.ps1:68`·`:603`, `bootstrap_integrity.ps1:1753`·`:1787` — TEST1 sub-490 02:02, archrev 02:03). 사용자 9/30 규칙(원격 설치는 창·허용 창 0)과 어긋남. 검사의 `-WorkerAccount <SID>` 꼴처럼 관리자 SSH 에서 작업자 계정을 받아 깔게 한다 | P1 | w9firstupdrd 크기(일 12:00) 뒤 w9mgrcut 이 동결 안/밖 결정 — 밖이면 기준일 밤은 현장 사람이 허용 창을 누름 | w9firstupdrd 설계 → w9firstupd | — |
| LM-3 | 확인 | 화면 결함 LM-01–05 는 고친 코드가 이 판에 있다(조사표 J). 실제 PC(1366×768·125 %) 캡처로 닫는다 | P1 | 지금 판 | TEST1 캡처 뒤 w9fieldfit1008 | — |
| LM-4 | 확인 | 업데이트 길 증명: v2.0.99 위에 다음 판을 같은 숨김 길로 덮어 깔고 자료·등록·보낼 것이 남는지 readback(§8-3). 지금은 코드만 있고 실제 PC 증명 없음 | P1 | 기준일 전(w9firstupd 계획서 §3 업데이트 길 줄 — 화 랩 또는 TEST1 L7 뒤) | w9firstupd·w9setupprep | — |
| LM-5 | 바로잡기 | 작업자 이름 붙이기: 회사 서버 기능 확인이 실패한 것과 꺼진 것을 구분하고, 실행 중에 주기적으로 다시 묻는다(지금은 모르면 빼고 보냄 — 받아들여지지만 이름이 빠짐) | P2 | 다음 판 | w9firstupd | — |
| LM-6 | 확인 | 기준일 실사로 들어온 「포장 끝·출고 대기」 옛 현품표 상자는 웹의 옛 상자 길로 나간다. 기준일 실사 확정이 D+1 첫 출고 전에 끝나는지 기준일 절차(G11)에서 확인한다(웹 WEB-N2, w9stockimpl 10/11 01:42) | P2 | 기준일 | w9firstupd(기준일 절차) | — |
| LM-7 | 구현 | **앱 안 작업자 등록**: 명단에 없는 사람이 그 PC 에서 바로 이름을 등록(관리자 승인 없음, 시스템 번호를 붙여 「이름 #번호」, 개인정보 없음, 잘못·대충 입력 방지). 등록 B 구현됨([계약](worker-roster-b.md)): 로컬 이름·요청·순번을 함께 저장하고 배경 전송. `stage_a=false`, 숨김 없음. 30일 숨김 A는 다음 판 | P1(새 사람이 웹 관리자 없이 시작 못 함) | 다음 판(이번 묶음 = 첫 업데이트, 사용자 10/08 결정). w9firstupd 가 이적 짝과 함께 크기(일 12:00) 뒤 안/밖 — 밖이면 코디에 알림 | w9firstupd | ◆ |
| LM-8 | 바로잡기 | 정보 창의 단축키 안내를 지금 것으로: F3 수동 완료 줄을 빼고 F1·F2·F4(제품 교환)·명령 카드(교환·되돌리기)를 넣는다(F-68) | P2 | 다음 판 | w9firstupd | — |
| LM-9 | 빼기 | 스캔 기록 우클릭·Delete 의 「선택 항목 삭제」 를 없애고, 끝난 트레이 취소는 「완료된 트레이 취소 (F2)」 하나로(같은 처리기). FAIL 줄 정리가 필요하면 관리자 메뉴로(F-37) | P2 | 다음 판 | w9firstupd | ◆ |
| LM-10 | 바로잡기 | 「대기 현황」 탭을 관리자 메뉴로 옮기고 현장 말로. 작업자는 바닥 줄 「보낼 것 N건」 만 본다(F-50) | P2 | 다음 판 | w9firstupd | — |
| LM-11 | 바로잡기 | 바코드 원문 보기·복사를 관리자 메뉴로(F-45) | P2 | 그 뒤 | w9firstupd | — |
| LM-12 | 이름 | §6 표의 낱말을 작업자 화면·안내 글에서 바꾼다. 창 제목 「바코드 세트 검증기」 → 「포장」, 옛 현품표 꺼짐 거절 글 → 「옛 현품표입니다. 이 PC 는 옛 현품표 받기가 꺼져 있습니다 — 실물을 그대로 두고 관리자에게 알리세요」. 비교·저장·전송에 쓰이는 글은 바꾸지 않는다 | P2 | 다음 판 | w9firstupd | — |
| LM-13 | 이름 | 저장 실패 안내에서 관리자용 기술 줄을 「자세히」(관리자)로 내리고, 작업자에게는 「저장이 안 됐습니다 — 저장 다시 하기를 누르세요. 계속되면 관리자에게」 한 줄(F-29) | P2 | 다음 판 | w9firstupd | — |
| LM-14 | 바로잡기 | 설정의 「관리자 인증」(보호 관리자 코드)을 개인 관리자 PIN 하나로 합친다. 관리자 확인 수단이 둘이면 헷갈린다(F-54, DECISIONS :148) | P2 | 그 뒤 | w9firstupd | — |
| LM-15 | 빼기 | 옛 판에서 저장된 현품표 교환 이어 하기·그 인쇄·재출력(F-62·F-63)을 뺀다. 현장 PC 는 새로 설치해서 저장된 교환이 없다. 빼기 전에 설치 원장의 PC 마다 그 저장 파일이 0 인지 읽는다 | P2 | 그 뒤 | w9firstupd | — |
| LM-16 | 빼기 | 개발용 자동 시험 바코드(F-69)와 옛 GUI 업데이트 후보 확인(F-70)을 제품 코드에서 뺀다. 둘 다 현장에서는 꺼져 있다 | P2 | 그 뒤 | w9firstupd | — |
| LM-17 | 바로잡기 | 옛 수동 마감(PARTIAL)은 LOCAL·ONLINE 모두 거절한다. 새 현품표 재스캔은 회사 서버에서 확인한 현재 작업군 멤버 수가 1–3이고 찍은 제품 수와 같을 때만 완료한다(원 TRANSFER 봉인 수와 구별). 멤버 수를 모르면 「서버 연결 뒤 마감」, 수가 다르면 「모자란 상자: 대체품으로 채워 정상 마감 — 못 채우면 마감하지 말고 따로 두고 관리자에게」를 보이고 현재 상자를 유지한다. 제품 4개 정상 완료와 옛 현품표의 재스캔 미달 완료·60초 되돌리기는 유지한다. | P1 | 이번 묶음 | 구현·headless 시험 완료(w9firstupd sub-73) | ◆ |


시험 장치 흔들림(컷 뒤): tests/test_label_ui_lane_integration.py::test_saved_exchange_retry_keeps_immutable_command_off_tk 세 매개변수 — 부하 중 fake Tk pump 2.0 s 벽시계, a63caec 에서도 단독 실패(w9setupprep sub-451)

LM-02 follow-up(코디 10-09 18:2x): full 완료의 방금 결과 문구·초록색과 표준 PHS2 알림의 중복 `다음:` 줄 생략을 창 차단 headless 시험으로 검증한다. 부분 완료·다른 화면·시작 안내와 업무 판정은 유지한다. 실제 Tk 창 수용은 별도 범위다([계약](contracts.md#scanner-only-packaging)).

P1-LM-02·03·05: 트레이 진행·다음 스캔·방금 결과와 현재 트레이 표를 계속 보이고, 복구 목록은 관리자 메뉴로 옮기며, 바닥 상태는 공통 문구 한 줄로 표시한다. 제목 접두·PIN 관문·입력 보존·after_idle 한 번 복귀는 창 차단 headless 시험으로 검증한다. 실제 Tk 창·DPI·스캐너·설치 수용은 sub-510 결합 뒤 w9setupprep stock과 배정 VM 회차에서 판정한다. 이 레인은 판 번호·서버 계약을 바꾸지 않는다.

onboarding admission 분리 후 랩 수용: 새 MachineGuid의 처음 설치·첫 GUI와 로그온 Run relay가 뜨기 전 GUI 시작, 기존 relay의 상태 갱신·실제 전송 지속을 확인한다. headless 두 프로세스의 실제 named mutex 검증과 실제 GUI/전송 수용은 구분하며, 기존 옛 예약 작업 은퇴의 admission 보유 시간은 별도 측정한다.

sub-474 측정 P2(컷 뒤): 기존 자료 세대 5-root 기록 묶음의 실제 admission은 6.641초였다. relay는 5초 허가를 한 번 놓쳐도 기존 재시도로 135초 신선도와 READY를 유지했다. 코디 msg_db8476bebd24에 따라 G 내부와 B2의 app_generation 1회 단언을 보존하며, 이전 예약 작업 은퇴 8.890초와 함께 후속으로 남긴다.

sub-160 두 번째 시작(10/13): relay 재사용 판정의 Observe와 시작 probe는 admission 밖으로 옮겼다. onboarding의 나머지 짧은 admission(진입 probe·자동 시작 등록·옛 작업 은퇴·READY 게시 등)은 relay 바퀴가 admission을 쥔 동안 각각 최대 5초 기다리고 넘으면 `WRITER_GATE_TIMEOUT`으로 거절될 수 있다. 후속 제안: relay 바퀴의 네트워크 구간을 admission 밖으로 빼 보유 시간을 줄인다(실측 뒤 결정).

sub-165 두 번째 시작: 90초 이내 steady relay의 판·운영 SID·일반 사용자 상태를 읽기만 해 즉시 재사용하고, 창 뒤 배경 Observe 한 번으로 신원을 대조한다. 칸 부재·시험 전이·첫 실행은 기존 길이다. focused headless의 1초 이내 판정과 실제 HDD/창 수용은 구분한다. onboarding의 나머지 admission 대기와 실제 화면·전송 확인은 남는다.

sub-474 fixed helper P2(컷 뒤): 코디 msg_eb0b977f005f에 따라 Verify helper 자신의 auto_update_fixed_helper admission도 기존대로 둔다. 실제 stock 데코레이터·native PowerShell 계수기의 두 보유 시간은 최대 0.422초·합 0.704초, 호출 3회·READY 12.547초다. 계수기 payload만 실행했으므로 설치된 stock Verify 업무 시간은 미측정이며 실제 랩에서 확인한다. 다른 helper mode·source·호출에 probe를 추가하지 않는다.

설치 packet의 release identity PASS 기억과 첫 admission 전 확인을 적용한다. 생성 선언·정확한 G 전환 쌍과 최종 stock gate는 통합 sub-480에서 갱신·검증한다. GUI onboarding 직접 확인의 시작 1회 비용은 별도 계수하며, sub-474의 짧은 admission이 합쳐진 머리에서 I5 위치를 재확인한다. 실제 HDD 첫 시작·GUI·Defender 수용은 headless 계수·stock 표본과 별도다.

sub-412 B-2: versions의 활성 코드 경로가 current 전용 옛 작업 이전에 전달되던 결함을 고정 current 입력으로 수정한다. 코디 msg_c2085909122c의 N2'/N3' 승인에 따라 canonical 확정 전 등록 READY에서는 세대·ledger·settings를 열지 않고 T1 뒤 정상 시작으로 연기한다. 기존 generation 거부와 Run/task preimage 복원은 유지한다. PS5.1/PS7 격리 실제 함수의 등록 성공·작업 이전 실패·옛 폴더 원위치·StartupUpdate 제거·확정 뒤 실패/멱등 재시작, 142/538·선언·stock 근거는 [sub-412 보고서](D:/KMTech/program-improvement-20260912/work/program/w9setupprep/sub-412/REPORT.md)에 기록한다. 독립 검토·1차 FF와 실제 포장 회차 B 재실행은 코디네이터 수용 항목이다.

sub-412 N8: relay PASS 뒤 GUI의 실제 첫 ledger/settings 열기 실패도 `FAILED/BUSINESS_DATA_OPEN_FAILED`·기존 보고서·같은 사용자 지원 명령을 표시하고 종료한다. 실제 app.main 경로의 창 API만 대역으로 검사하고 같은 onboarding CLI 재시도를 증명한다. 재부착 미지원은 서버 등록 정리 후 재설치가 필요한 별도 P1 후속이다.

sub-399 P2: 등록·배치 자식의 동기 CIM 관찰이 30초 종료 예산 밖으로 늘어나는 경로를 공용 벽시계 마감으로 제한한다. 관찰 runspace의 남은 시간 대기·CIM 시간 제한을 함께 적용하고 만료 뒤 종료 동작0·미증명 복원0을 유지한다. PS5.1/PS7의 35초 지연 RED→GREEN, 정상 종료 및 최종 142/538·같은 SHA의 feed 유무 stock 근거는 sub-404 REPORT에 남긴다. GitHub 공개 release/tag/latest 동결과 랩 수용 OPEN은 유지한다.

sub-388 제어 응답 소실: 1차 msg_e36ee0956cca에 따라 정상 인증 frame 전 오류는 HELPER_CHILD_UNPROVEN·코드 UNKNOWN·복원0이다. 정상 frame 뒤 결과 파일 거절은 기존 같은 guardian 복원이며, 미증명 진단 파일 손상은 복원 허가로 바꾸지 않는다. 실제 native pipe fault·parse/identity·missing 진단·정상 frame 뒤 missing 결과를 양 엔진에서 회귀 고정한다. 앞 09d3df6의 142/538/stock은 그 판의 증거로 보존하고 마지막 SHA에 다시 검증한다.

sub-388 결과 검증 예외: native 배치 뒤 결과 파일 검증이 throw해도 확정 전 catch에서 실제 startup preimage와 검증된 code backup을 수집한다. 모호한 backup은 거절하고 별도 capture 진단을 보존하며 원 실패의 형식·위치를 교체하지 않는다. PS5.1/7 실제 catch와 파일 SHA 회귀를 추가하고 마지막 커밋에 142/538/stock을 다시 결속한다.

sub-388 T6 회귀: 초기 서버 주소 거절의 기존 FAILED/SERVER_BASE_URL_INVALID/REPORT_UNAVAILABLE·고정 안내에 원 예외 형식·모듈·함수·양수 줄을 필수로 검사한다. URL 값 비노출과 plan/설치·경로 생성 부작용 0 단언은 유지한다. 예전 네 필드만 허용하여 생긴 68536dc의 538 회차 추가 실패는 원본 근거로 보존한다.

sub-388: 부모 custody를 유지하는 단일 guardian, 거래/SHA 결과 검증, Confirm-LabelCanonicalPlacement 확정 경계, 확정 뒤 새 코드 유지와 등록 자식 600초/종료 확인 30초를 구현한다. 배치 자식 720초·소유/종료 미증명 복원 거부와 SSH 로컬 pipe의 NETWORK 그룹 수용도 같은 범위다. PS5.1/PS7 숨김 protocol 회귀와 기존 선택·pin·stock의 실제 판정은 sub-388 REPORT에 기록한다. 실제 상승 승인 횟수·보호 startup/task/ACL 복원·현재 사용자 SSH 신원 수용은 별도 랩 OPEN이며, 공개 GitHub latest/태그 동결은 유지한다.

회차 B-1의 등록 delegation 누락과 no-relay 복구의 StrictMode 속성 접근을 수정한다. 등록 실패 전에 쓰던 자료 세대 기록을 등록 READY/readback 뒤로 옮기며, 기존 새 세대 업무 자료의 downgrade guard와 wrapper를 보존한다. 같은 이름 current-user CNG 키의 검증·재사용은 기존 설계를 유지한다. 격리 회귀·표준 선언·후보 stock 근거는 [sub-377 보고서](D:/KMTech/program-improvement-20260912/work/program/w9setupprep/sub-377/REPORT.md)에서 판정하고 B-1/B-2 랩 재실행과 독립 검토는 후속 수용 항목이다.

회차 5의 실패 복구 delegation과 Verify 진단 참조는 `tests/test_round5_startup_rollback.py`, `tests/test_fixed_helper_failure_evidence.py`로 회귀를 고정한다. 원 감사의 code placement는 PASS였고 onboarding의 fixed helper Verify가 실패했으며, rollback의 protected startup restore 자식이 OWNER_DELEGATION_MISMATCH를 남겼다. 9adc801 stock의 실제 VM 보호 배치 뒤 standalone Verify에서 미export 함수 5종의 결함을 재현했고 첫 실패는 Enter-LabelStartupUpdateAuthority의 Get-StrictFullPath다. 새 자식에서 module만 가져온 전체 Verify·manifest 변조·진단 보존 회귀를 추가한다. 원 fence 만료값과 원 Verify 예외는 미회수이므로 역사적 실행의 세부 원인을 단정하지 않는다. 최종 stock의 성공 등록/의도한 TLS 실패·옛 앱 재시작은 별도 랩 회차 수용 대상이다. 결과·자식 전수 목록·남은 범위는 sub-368 REPORT.md를 따른다.
컷 전 P1 JSON escape 가림: 공통 Python/PowerShell helper가 값·키의 escape를 풀어 알려진 비밀을 가리고 최종 직렬화 텍스트를 재검사한다. 키 충돌·복원된 비밀·구조 손상은 `REPORT_UNAVAILABLE`로 게시를 거절하며 한 원소 배열도 유지한다. 합성 변종·실제 보호 파일/SHA, 기준 대비 회귀 및 미시험 범위는 [sub-357 보고서](D:/KMTech/program-improvement-20260912/work/program/w9setupprep/sub-357/REPORT.md)를 따른다. 독립 대조·통합 stock/설치 수용은 코디네이터가 별도로 진행한다.

canonical의 embedded probe 3개는 CRLF→LF/UTF-8 바이트 SHA를 PowerShell AST로 다시 계산하는 회귀로 고정 pin을 확인한다. source admission 뒤 전이·healthy lifecycle·preimage 준비 실패도 기존 FAILED/FAILED_PREPARATION_ADMISSION audit와 install_status=FAILED를 남긴다. 기존 FAILED_PRETRANSACTION archive는 보존하고 같은 공개 상태를 반환한다. 실제 설치·등록·복구 수용은 랩 별도이며 [sub-348 근거](D:/KMTech/program-improvement-20260912/work/program/w9setupprep/sub-348/REPORT.md)를 따른다.

staged bootstrap source 검사의 문자열→코드 생성/실행 빈틈은 정상 stock의 정적 명령·메서드·생성자만 허용하는 기본 거절로 교정한다. 기존 비리터럴 호출 14개·대입 7개는 유지하며 두 고정 Win32 Add-Type 선언만 정확 owner·본문·개수 예외로 추가한다(허용 호출 16개). sub-337의 유효 RED 39셀과 sub-332의 28셀, 정상 설명/stock 및 선언 변조 회귀를 AST 판독만으로 확인한다. 실제 설치·예약 작업 생성·GUI·VM 수용은 이 source 검증 범위가 아니며 실행 결과·남은 독립 검토는 [sub-338 보고서](D:/KMTech/program-improvement-20260912/work/program/w9setupprep/sub-338/REPORT.md)를 따른다.

새 설치 source admission은 설치 후 생성되는 bootstrap-integrity.json 대신 canonical이 검증한 기존 전체 source inventory와 manifest/정적 version 바이트를 결속한다. 중첩 credential owner G, 설치된 앱 bootstrap 및 ES256 호환 검사를 유지하고 사전 거절은 status=FAILED·preparation_status=FAILED_PREPARATION_ADMISSION·rollback.status=NOT_REQUIRED audit와 install_status=FAILED로 반환한다. 승인 ZIP의 bootstrap 없는 원본 RED 및 같은 바이트 admission GREEN, 변조/잘못된 metadata·중첩 G 거절과 PS5/PS7 실패 결과는 headless 검증 범위이며 실제 VM 설치/등록/복구는 NOT TESTED다. 정확 회귀·새 stock 및 최종 SHA는 [sub-339 보고서](D:/KMTech/program-improvement-20260912/work/program/w9setupprep/sub-339/REPORT.md)를 따른다.

컷 뒤 P2: 미종결 다중줄 구조화 값의 가림 범위는 C# reader(INSTALL_CANONICAL_PORTABLE.ps1:2366–2374, Finish:2424)가 뒤 진단 줄까지, Python(failure_redaction.py:272–300)·PS(tools/failure_redaction.ps1:72–94)는 첫 줄 끝 fallback으로 다르다. C#은 시작 때 marker만 출력하고 미종결 state5 값 문자를 내지 않아 이 갈래의 잔여 노출은 없다는 코드 근거가 있으나 새 다중줄 실측은 NOT TESTED다. 교차 동등 GREEN은 동결 구조화 4개 단일줄에 한정한다. 코디 2026-10-06 13:1x 답(msg_60f158e77301)은 계약 0f 멈춤 규칙에 따라 컷 전 범위를 확대하지 않고 기록하도록 결정했다.

소리 장치 실패가 업무 처리를 막지 않아야 한다. 9f960fa의 일반·반복 재생, 초기화, preload, siren, 종료 및 inline 확인은 예외를 처리했으나 이전 모달 창 생성 실패 뒤 stop 호출은 트레이 리셋을 중단시켰다. 이 호출만 보호하며 실제 PlaySound 대역 실패 RED 1F→GREEN 4P/0F/0S로 리셋, 완료 durable 저장·CSV 로그·서버 동기화 시작·다음 스캔 및 초기화/siren 실패를 headless 검증한다. 실제 오디오 장치·서버 전송은 NOT TESTED이며 근거는 sub-325 REPORT.md에 기록한다.

stock 빌더의 경로 상수와 실제 application/runtime/third-party 복사 입력에서 PowerShell(.ps1/.psm1/.psd1) 집합을 도출해 비ASCII 바이트가 있으면 UTF-8 BOM을 요구한다. 실제 PS5.1은 BOM 파일을 UTF-8로, 나머지를 cp949로 디코드한 본문을 ParseInput으로 읽고 UTF-8 기준의 한국어 AST 리터럴과 비교한다. 기존 3개 경비 밖 data_generation.ps1의 임시 사본에서 BOM 3바이트만 제거하자 2F(리터럴 불일치 2), 실제 9개 배포 source/runtime member에서는 2P/0F/0S·구문 오류 0·한국어 리터럴 37개 일치였다. 호스트 65001과 cp949 판독 모의의 경계, stock 전체 확인 및 독립 검토는 sub-325 REPORT.md에 기록한다.

표준 도구로 candidate-facing LF digest를 재생성했다. writer sink inventory 80은 유지되며 소리 stop 보호로 바뀐 source 위치의 inventory pin을 다시 도출했다. 역사 before 값은 바꾸지 않는다. --check와 같은 입력 재생성의 바이트 동일 확인을 통과했다. 재생성은 기능 수정과 별도 커밋이며 최종 142·stock builder·독립 검토는 sub-325 REPORT.md로 판정한다.

10/8 컷 전 병합 대상의 URL 앞단 거절은 SERVER_BASE_URL_INVALID와 고정 한국어 안내를 공개한다. URL 원문·이전 보고서 참조·상태 변경 0을 유지하며 회귀와 실제 native 설치 한계는 sub-325 REPORT.md에 기록한다.

10/8 컷 전 병합 대상의 보호 보고서 가림은 민감 key의 mapping·list·중첩 값을 인용·escape와 괄호 짝을 확인해 통째로 가린다. 짝이 없거나 어긋나면 해당 줄 끝까지 가리고 정상 후속 field·원인 접두는 보존한다. 공용 rules의 structured_delimiters가 Python·PS·C#의 구조 문법 입력이며 기존 18벡터에 동결 구조화 4벡터를 추가한다. 공개 허용 레코드 계약과 길이 상한·가림 후 절단 순서는 유지한다. 엔진·공개/보호 셀 실측과 남은 한계는 sub-325 REPORT.md에 기록한다.

10/8 컷 전 병합 대상 P2 검증 입력을 교정한다. zero-touch 검사는 PowerShell AST의 실제 작업 생성 명령과 helper의 설명 문자열을 구별하며 helper 안의 작업 생성 명령은 계속 거절한다. writer 순서 검사는 최상위 transaction 시작 뒤 호출을 비교한다. shared-leaf fixture는 실제 가림 helper·규칙을, G fixture는 공식 launcher 검사 정의를 제공한다. 부분 복구 fixture의 registrar·profile·package 모듈은 같은 byte-verified 설치 ROOT를 사용하며 제품 guard·단언·timeout은 유지한다. 검증 결과와 독립 검토 P1·P2 0 후 병합 경계는 sub-325 REPORT.md에 기록한다.

옛 공식 task가 있으면 identity admission과 VBS→동명 PS1/CMD launcher binding을 첫 audit/evidence·보호 base·ACL 변경 전에 읽기 전용으로 검증한다. task가 없으면 launcher를 요구하지 않는다. 공개 LM v2.0.38의 실제 producer는 VBS→PS1이며 source-host bin 경로에 결속된다. 어긋나면 기존 healthy-lifecycle 안내로 거절한다.

예외 args의 Python 단일 인용 mapping도 비밀 field 가림을 거친다. 일반 예외 접두문 뒤의 assignment·mapping은 key/value를 분리한 공용 가림으로 계속 검사한다. Python·PS 공용 text 가림과 bounded child C# reader의 최종 변환은 인용 경계/escape를 인식해 원인 메시지·타입·trace를 보존하고 비밀 value를 사용자 sink에 다시 연결하지 않는다. 엄격한 PID·start·mutex 식별자 예외와 공통 14 벡터를 함께 유지한다.

relay install pack의 상대 program-data root는 경로 정규화와 G 판독/기록 전에 인수 오류로 거절한다. 거절은 정적 stderr 안내와 exit 2만 남기고 report·자료·generation을 쓰지 않는다. 유효한 sealed app fixture에서 입력 바이트와 runtime 부재를 함께 검증한다.

조립 fixture 교정: receipt gate는 실제 읽기 전용 G 진입과 표준 의존성을 제공하고 다른 제품 mode는 계속 거절한다. 제거 진단 회귀는 실제 clean source identity, 예약 작업 대역은 제품과 같은 가림 helper 입력을 사용한다. isolated child의 program-data override 잔류를 제거했다. 원 실패 XML과 변경 전 증거는 sub-292에 보존한다.

sub-300 첫 설치 P1: canonical base가 없는 옛 공식 작업 PC에서도 Disable 전이 전에 기존 보호 base 초기화를 수행한다. 설치자 owner record는 일반 배치에서만 쓰며 G·DACL 결정 B는 바꾸지 않는다. StrictMode 진단의 빈 객체를 안전하게 열거해 원 예외를 보존한다. 격리된 실제 제어 흐름 회귀와 실제 PC 설치 수용은 구분한다(후자는 coordinator 예행 대상).

v2.0.95 2c46de1의 전체 source 정방향은 기존 delayed-reader pair 시험이 요구하는 승인 입력이다. 원 표준 stock before에 정확히 결속하는 조립 선언으로 보존한다(현장 해당 없음). prototype 4070818/160d2b4와 TEST1에 추가 선언은 없다.

REDACTION-CONTRACT 1a의 Python machine writer도 대조했다. update transition/active, settings, relay stop marker는 진단과 파일 종류별로 분리해 원 직렬화 바이트를 보존하고 쓰기 전에 비밀 값을 거절한다. 실제 writer body에 >64 KiB, 식별자 보존 및 원문/JSON escape 비밀의 기존 대상·없는 부모 무변경 회귀를 추가했다. writer sink membership와 기존 G는 그대로다.

v2.0.95 조상 a56a587의 실제 표준 stock before를 보존해 기존 정방향 G 시험 입력을 조립 선언에 결속했다(현장 해당 없음). 순수 G 서명 역방향 입력은 전체 362f30f이며 companion ac745 전체는 같은 계약의 입력이 아니다. TEST1 새 설치에는 추가 선언이 없다.

sub-292 G 회귀에서 junction 등록 경로의 WriterFenceError도 공통 복구 안내가 필요함을 확인했다. ValueError와 같은 초기 거절 경계에서 공용 가림을 적용하며 등록·정체 판독·상태 변경 0을 유지한다.

조립의 registrar 경로 선검사 거절은 읽기·등록·변경 전에 기존 오류와 공용 한국어 자료 보존/지원 복구 안내를 전달한다. 설치 상태를 보존하는 조기 catch도 최종 예외 가림과 audit 원인 보존을 적용한다. r4로 바뀐 catch/선검사 구조와 추가 readback에 맞춰 G fixture의 locator·namespace를 교정하며 실제 guard와 기존 단언을 유지한다.

공식 legacy relay의 repetition_interval은 SCHEMA의 XmlConvert TimeSpan 왕복으로 정규화한다. 같은 기간의 다른 표기는 같은 13개 필드 지문을 유지하고 다른/무효 기간은 기존 소유권 오류로 상태 변경 전에 거절한다. 기존 정규 표기와 공유 기대값 지문은 불변이며 PS5.1·7 격리 회귀 10 PASS다.

GUI/onboarding의 legacy readback·현재 사용자 task 제거와 source 판 확인 Git 호출은 CREATE_NO_WINDOW를 명시한다. source resident/autostart는 확인된 sibling pythonw.exe를 쓰며 없으면 기존 오류로 거절한다. one-shot console·packaged/portable·r4 bootstrap과 과거 task ownership 정의는 보존한다. 앱 안 GUI 시작은 manifest.entrypoint의 runtime\pythonw.exe -I -B app\main.py 또는 관리 설치 bootstrap을 사용한다. 설치기가 만드는 작업자 링크는 manifest의 current/launch-label-match.cmd를 최소화 창(WindowStyle=7)으로 실행한다. 직접 콘솔 기동과 설치된 작업자 링크를 구별한다. mock 회귀 22 PASS; 실제 Explorer·로그온·스캐너 포커스는 NOT TESTED다.

rollback 최종 예외와 원 설치 예외의 재전달은 기존 Get-LabelLaunchFailure의 공용 가림/마지막 줄(300자 이하)을 사용한다. 원 예외 chain/type과 가린 원인 전문/참조는 기존 감사·보호 evidence에 보존하며 raw message를 사용자 sink로 다시 연결하지 않는다.

조립판 자동 업데이트 다운로드는 실제 profile 위치 G 뒤 profile을 한 번 판독하고, loader의 secret/TLS owner G를 통과한다. 각 HTTP 요청의 TLS CA 첫 판독 직전에도 현재 G를 재확인한다. 늦은 거절은 해당 보호 판독·복호·접속·다운로드 상태 변경 전에 끝나며 기존 업데이트 상태 계약을 보존한다. 격리 회귀와 stock/랩 수용은 별도로 기록한다.

GUI PIN·명단 client는 실제 첫 요청 때 locator 위치 G → JSON 한 번 → 하위 보호 secret/TLS owner G를 다시 확인하고 그 CredentialInput을 loader에 넘긴다. startup의 허용이나 준비 입력을 첫 PIN 판독 근거로 재사용하지 않는다. relay의 반복 run/enqueue는 상태 기록·lease reset 전에 현재 locator를 다시 준비하며, profile 재판독과 각 앱 transport의 TLS CA 판독에도 기존 G를 적용한다. 거절은 보호 판독·복호·연결과 해당 회차 상태 변경보다 먼저이고 PIN 화면은 기존 쉬운 한국어 원인을 표시한다. 공용 G helper·syntax guard·writer membership은 보존하며 byte/line 변경에 따른 pin과 exact forward 선언만 표준 재생성한다. `auto_update.py`가 없는 이 기준 `2c46de1`의 별도 업데이트 consumer는 조립판에서 확인한다. [sub-281 구현·별도 process 대조](D:/KMTech/program-improvement-20260912/work/program/w9setupprep/sub-281/REPORT.md)의 범위는 호스트 격리이며 실제 GUI·DPAPI·서버·stock은 NOT TESTED다.

외부 CA·conflict receipt·보호 관리자 verifier의 G parent 누락을 sub-262에서 교정한다. legacy install-pack fixture는 실제 release identity와 bootstrap inventory 입력을 보충하며 기존 단언을 유지한다. headless/정의-only 회귀와 stock 거절 근거는 [sub-262](D:/KMTech/program-improvement-20260912/work/program/w9setupprep/sub-262/REPORT.md)에 결속하고 독립 재확인·중앙 병합·native 수용은 1차 후속이다.

예행 4회차 sub-274: 실패 전문 가림·보존, current 없는 보호 startup 복원, 보호 선행 rollback, 오류 수와 독립된 안전 readback, 일반 지원 재개, 공식 옛 relay 작업의 Enabled 전환, 중첩 phase 기록을 교정한다. onboarding 복원 위임 목록에 실제 nested source `auto_update_fixed_helper`를 추가하며 writer 목록 76과 기존 G를 유지한다. 새 stock 및 동일 기준 집중 회귀 결과와 sub-278 단언 불변 추출 diff는 [sub-274 보고서](D:/KMTech/program-improvement-20260912/work/program/w9setupprep/sub-274/REPORT.md)에 기록한다. 실제 공개 v2.0.38 작업/암호 저장 Enable·Disable, 일반 실패 지원 재개, G 거절 두 갈래의 native 수용은 예행 5회차 guest 검증 전까지 NOT TESTED다.
F6: 이력 삭제·완료 취소·실물 불확실 재출력·미완료 종료의 키보드 승인을 기존 안내의 마우스 확인으로 교정하고 원문·날짜 선택을 이력 영역에 재사용한다. headless 원문 메서드/위젯 대역 회귀와 실제 한국어 Windows·스캐너·IME 수용은 구분하며, 후자는 NOT TESTED다. [과제 결과](D:/KMTech/program-improvement-20260912/work/program/w9setupprep/sub-298/REPORT.md)와 [guest 확인](D:/KMTech/program-improvement-20260912/work/program/w9setupprep/sub-298/GUEST-CHECK-F6.md)을 따른다.

legacy upgrade guard의 G/nextfix 조합은 정의된 exact tuple을 사용하고 시험 helper의 중복 BOM을 제거한다. 기존 66개 read-only 선언/stock/archive preflight와 신규 source/self·BOM 구문 회귀는 [sub-242 보고서](D:/KMTech/program-improvement-20260912/work/program/w9setupprep/sub-242/REPORT.md)에서 기준/후보별 결과를 구분한다. 실제 설치·공개 v2.0.38의 백업/새 설치·등록 복구/현장 rollback 수용은 별도이며, 호스트에서 실행하지 않는다.

첫 재실행의 남은 17개는 금지 입력을 성공시킨 결함이 아니라 G writer/file source-set 선거절을 옛 semantics 문자열로 기대하던 시험 결함이다. G writer 추가를 독립적으로 대조한 뒤 정확한 첫 거절 코드만 교정하며 기존 부정 입력·nonzero exit·불변 자료 단언은 유지한다. 원 실패 및 최종 재실행 결과는 같은 보고서에서 구분한다.

w9scanonly audit CSV index 병합 후 G 전이에 빠진 manager/schema 두 파일은 승인된 committed before/after exact 전진 조합으로 결속한다. 기존 G 10항목·signed reverse·공용 helper·syntax guard는 유지하며 current/낡은 일부 바이트/한 바이트/목록 밖/역방향 및 이미 G인 두 판의 회귀·hash 전 항목 표를 같은 sub-242 보고서에서 확인한다.

직접 입구의 G 경로와 실제 reader 경로를 일치시킨다. 빈 profile 값은 기존 loader의 환경 변수/기본 선택을 사용하고, `~`·상대 경로·대소문자는 기존 경로 resolver로 한 번 해석한 값을 guard와 reader에 전달한다. 링크/junction은 기존 no-reparse 계약에 따라 UNKNOWN으로 거절한다. 제어 root에 marker 없이 실제 외부 경로에 높은 marker만 둔 별도 프로세스 회귀와 정상 허용 profile 경로 일치 시험을 sub-234에 기록한다. 합성 build fixture 사이 세대 기록 혼입을 막기 위해 known folder와 제어 root를 시험별로 격리하며 원래 단언·native 시험 선택은 유지한다.

G 후보의 새 기준은 종료 잔류 수정·w9scanonly를 포함한 `e89c188`이며, 종료 알림 sink를 유지한 58→59 명단과 exact 설치기 선언을 다시 생성한다. 새 기준의 집중 묶음·전체 실패 집합과 후보의 비교는 같은 선택/격리 harness로 다시 측정하며, 이전 4b/f1ee 증거는 해당 판의 기록으로 보존한다.

직접 지원 입구 G 부류 교정은 `a56a587` 위에서 진행한다. 등록/관리자·부분 복구, onboarding·제거, raw/hosted relay와 queue·profile·지원 복구가 첫 신원/자료 판독 전에 판단하며, G 거절 때 기존 relay·Run/task·상태 report를 보존한다. bootstrap 판 확인의 사용자 설정 선행 판독과 installed `app/tools`의 packet 신원 선택도 교정한다. 기존 공용 helper와 sink 59개를 유지한다. headless 실제 별도 프로세스 및 정상 허용 경로의 판별 근거는 [sub-234](D:/KMTech/program-improvement-20260912/work/program/w9setupprep/sub-234/REPORT.md)에 기록하며, 실제 GUI·native 설치·VM/서버 수용과 독립 검토는 별도다. 외부 입력 누락의 후속 교정은 settings·spool·stop marker와 명시 TLS/인증 파일을 각각 검사하며, sub-244의 독립 입력 회귀와 INPUT-ROOTS 표를 근거로 판단한다.

coordinator가 허용한 정확한 G 전환 선언 확장을 별도 커밋으로 검토한다. 기존 pre-G→G 및 이미-G `a56a5877`→교정 후보의 실제 read-only installer probe, 후진/선언 파일 한 바이트/미목록 파일 변조 거절을 검사한다. 자료 G 거절을 우회하는 배포 허가는 포함하지 않는다.

자료 세대 G 후보(`v2.0.95`): 지원 옛 packet 선택·canonical/verified replacement 실패 복원 앞 guard와 직접 앱·relay·온보딩의 실제 첫 자료 열기 경계 기록을 연결했다. 공통 helper 0.3.3의 동일 바이트와 기존 사용자 control/data 자리, 단일 guarded sink 1개를 사용한다. 호환 하한은 기존 ES256 private manifest의 판·build 결속만 신뢰하며, 기록 없음은 ALLOW, 손상/같은 판 다른 build는 거절한다. 명시 호환으로 허용된 같은 판 새 build가 자료를 열면 새 build를 기록하고, 두 하한의 더 엄격한 값과 신뢰 교집합을 보존하여 이전 승인을 물려주지 않는다. 직접 Python exe·PS5/PS7·온보딩 회귀는 **PROVEN(headless, 신원/GUI 대역)**이고 실제 frozen exe·UAC·원 사용자 lifecycle 및 독립 검토는 남아 있다. [첫 현장 설치](operations.md#first-field-data-generation)는 옛 `v2.0.38`의 설치 직전 일관 백업과 관리자 코드/자료 함께 복원을 요구하며 실제 현장/랩 수용은 **NOT TESTED**이다. 정확한 commit·집중 비교·stock 증거는 [sub-220 보고서](D:/KMTech/program-improvement-20260912/work/program/w9setupprep/sub-220/REPORT.md)에 기록한다.

명단 2단은 로컬 묶음 귀속·이름 정정/중지·신호별 본문·감사/물류 재전송·옛 상태 읽기를 합성 headless 경로에서 검증한다. 완료 CSV 전 실패의 lane 재시도·재시작과 미완료 UNDO, CSV 후 archive·marker·lease 실패의 재시도는 완료된 원 구성·신원·신호 결정에 결속한다. 완료 후 편집은 거절하며 재시작도 기존 CSV 완료를 확인한다. 실제 서버 신호 켜기·설치 전이·장치/화면·번호별 웹 집계는 통합 수용에서 확인하며 downgrade 필드 보존은 미검증이다([C-00](contracts.md#c-00)).

LM-A1/LM-B4: 빈 화면 종료 확인을 줄였고 F5의 새 날짜 교환·전용 조회·단추·선택 창·전용 시험은 제거했다. 이전 판이 미완료 journal을 남길 수 있으므로 건별 복구·hold·PIN intent·v1 형식과 같은 요청의 prepare/print/activate는 보존한다. 잠금 면제는 복구 두 source만 남는다.

LM mainloop 종료: 예외·직접 반환·종료 접수 직후 반환의 non-daemon UI lane 정리는 mainloop 반환 뒤 45초 상한으로 끝낸다. 상한 뒤 내구 표지와 rc 1 종료, 다음 시작의 기존 세트·outbox 복구를 사용한다. 시작 단계·닫기 중단 뒤 감시와 150초 전체 프로세스 나무 종료 확인은 headless 회귀 대상이다([원격 멈춤](operations.md#remote-app-stop)). 실제 Tk 모달 닫힘·셸 closing 저장 순서·WebView2 퇴장·장비/서버는 inspection6 및 셸 통합 수용에 남는다.

새 PC 판정은 정해진 bootstrap CA 디렉터리의 ca-bundle.pem 한 파일만 있을 때 기존 bounded certificate-only 검사로 공개 인증서만 확인하여 새 설치를 허용한다. 개인키·잘못된 인증서·다른 파일·빈 디렉터리·재분석 경로·정체/credential/profile/설치 영수증 흔적은 이 예외에 해당하지 않으며 기존 수명주기/복구 판정을 유지한다. 설치 통합 후보에서는 기존 9개 startup 조상 before를 보존하고 각 조상의 추가 변경 파일/추가 파일 및 e89 전진 통합을 정확 LF 쌍으로 결속한다. 기존 순수 G의 명시 서명 역방향은 e89/362f30f 역사 쌍에서 보존하고 startup 통합 후보의 역방향은 거절한다. 기존 inventory 생성기로 76개 sink와 Python/PowerShell pin을 함께 갱신하고, stdin probe의 최종 SHA를 다시 결속한다. 새 wildcard·역방향 선언은 추가하지 않는다. 격리 시험·stock·독립 검토·실제 설치 수용은 서로 구분하며 통합 근거는 D:/KMTech/program-improvement-20260912/work/program/w9setupprep/sub-247/REPORT.md를 따른다. PS5.1·PS7 회귀와 실제 guest 설치 결과는 별도 근거다.


canonical probe의 명령줄 길이 결함은 고정 SHA256 검사 후 동일 UTF-8 bytes stdin 실행으로 교정한다. PS5.1/PS7 실제 자식 회귀·정식 기준 대비 guard 실패 집합·stock packet·빈 guest 1회 예행은 `D:/KMTech/program-improvement-20260912/work/program/w9setupprep/sub-166/REPORT.md`에서 PROVEN/FAILED/NOT TESTED로 구분하며 출시 수용은 별도다. w9next 교정 이식은 coordinator 소유다.


<a id="busy-scan-durable-fifo"></a>

후속: 남은 F5 journal 복구·옛 deferred validation의 busy 스캔 거절을 내구 FIFO와 정확히 한 번 적용으로 전환한다. 이번 잠금 컷은 정상 스캐너의 HTTP 무대기·fence 변화 뒤 같은 key 재대조 및 로컬 완료 저장 중 기존 메모리 FIFO의 보존을 검증하며, FIFO의 재시작 내구화는 별도 수용한다.

잠금 반환 후 오래된 원격 결과의 저장 경쟁은 포장/취소·교환의 결과 CAS, runtime pending-request 대조, F5 파일 SHA 대조로 닫는다. 같은 명령의 늦은 성공만 내구 receipt 표식을 통해 재POST 없이 자동 대조하며 기존 review 감사는 보존한다. 실제 설치·장치·서버 수용과 기존 busy FIFO 과제는 계속 별도다.

G5·G8·G9·G10/P3: 중복 입력 보존, 포장 진행 현황·스캔 복귀, 옛 제품4 슬롯 문구, 세 가지 소리·연결/전송 대기 표시, CA 분류표 바이트 정렬은 소스에 반영했다. [현재 계약](contracts.md#scanner-only-packaging)의 focused/collect/stock 검증과 별도로 실제 화면 배율·스캐너 연속 입력·소리 청취·단절/재연결 장치 수용은 남아 있다. REWORK_GOOD_READY 교환의 중앙 출처 규칙 변경은 웹 레인 범위다.

<a id="connection-observation-order"></a>
## 연결 표시 관측 순서 - 전 경로 시작 시각 기록

현황 조회는 요청 시작 시각으로 연결을 판정하며 저장된 retry의 늦은 읽기는 새 실패로 승격하지 않는다. 전송 ACK·retry는 현재 내구 `updated_at`을 비교한다. 포장에는 `last_attempt_at`이 있지만 relay·취소·교환에는 대응 시작 시각이 없으므로, 전송 A 시작 → 더 새 B 성공 → A 실패 도착(및 반대 방향)의 전 경로 순서 보장은 남아 있다. 각 요청의 시작 순서를 내구 기록해 ACK·retry와 결속하는 변경은 세 앱·웹을 함께 검토하는 후속 라운드 범위다. 서버 요청 모양·내구 완료·재시도 계약을 보존하고 양방향 순서 회귀를 확인한다.

<a id="lm-w9-scanner-review-fix"></a>
## W9 · 스캐너 독립 검토 수정

w9firstupdsub121: 날짜만으로 로컬 미완료 세트를 지우는 길을 제거한다. 원 바이트 사본 후 자동 복원하며, 손상·비중앙 시각 해석 오류는 기존 보류 저장과 원본 파일 readback 뒤 새 작업을 연다. 보관 실패는 기존 복구 잠금이다. 가짜 시계로 다음 스캔·보류 뒤 새 저장·기존 ID/일련번호/현품표 날짜·오늘 완료 집계를 검증하며 중앙 내구·과도기 예외는 유지한다. 정상 복원 뒤 검증된 사본은 세트별 3개로 정리하고 보류 사본은 유지한다. 실제 화면·현장 수용은 후속이다.

w9firstupdsub121 후속: 밤사이 만료된 PREFETCHED는 스캔 source 조회에 쓰지 않고 원 행을 보존한다. 완료 재사용에서만 로컬 만료 검사를 넘겨 원 잠금으로 서버 판정을 받으며, 새 발급의 만료·서명/결속/시각 검증은 유지한다. 가짜 시계·실제 서명/SQLite·가짜 HTTP로 만료/미만료 재사용·기존 세트 완료·새 발급 만료 거절·서버 409의 건별 충돌을 검증한다. 실제 서버 수용은 별도다.

DESIGN §11-13의 미달 UNDO 60초 유예를 적용한다. 선택 shortage_confirmed_at과 기존 완료·release 경로를 유지하며 무입력 만료·59초 다음 스캔·UNDO·종료 경합·재시작 61/10초·옛 상태 읽기·UNDO 취소 뒤 상태 저장 실패·60초를 넘긴 완료 저장은 가짜 시계·headless 회귀로 확인한다. 실제 guest에서 60초 뒤 중앙 완료와 재시작 전송 확인은 설치 수용에 남는다.


fake Tk pump는 예약 callback이 계속 생겨도 반복마다 작업 스레드에 실행 기회를 준다. 기본 2초 제한과 완료 단언은 유지하며 실제 정지된 lane이 여전히 제한 시간에 실패하는 부정 시험으로 검사한다. 실제 Tk·스캐너·서버·설치 수용과 브랜치 전체 시험은 별도 통합 검증 범위다.

DESIGN §1.7에 따라 조회 탭에서 스캐너 Entry 입력을 받으면 이번 세션 작업 화면으로 돌아가 같은 원문을 처리한다. 지난 날짜 조회는 오늘의 작업·중복 인덱스를 보존하며, 복귀 때 이전 날짜의 배경 조회/표시 generation을 무효화하고 보관한 오늘 화면으로 돌아간다. 초기화·오늘 인덱스 오류·저장 차단·중앙 처리 등 기존 안전 gate는 유지한다.

DESIGN §1.2와 코디 추가 결정에 따라 품목 불일치·입력 오류는 오류 순간 기존 FAIL 판정·기록 내용을 내구 완료한다. 로컬 current_set의 선택 필드 scanner_failure_result/scanner_failure_details가 판정과 원문을 보존하며 중단·재시작과 저장 재시도에도 같은 FAIL을 이어서 한 번만 기록한다. 안내를 닫는 다음 Entry 스캔도 새 입력으로 처리하고 Enter/ESC는 안내를 닫는다. 저장 실패 때는 기존 세트·실패 intent를 유지하고 재시도에서도 PASS로 바꾸지 않는다. 서버 필드·명령 형식은 바꾸지 않는다.

보류 현품표·중앙/교환 대기·음향/이력 callback 오류는 창 안 안내와 기존 소리를 사용한다. 미완료 저장본은 질문 없이 현재 교대 작업자에게 복원하며 원 스캔·접수와 과거 작업자 귀속을 보존한다. 복원 안내 자체는 입력을 잠그지 않고 다음 정상 제품 뒤 사라진다. 작업자 선택/등록, 설치·기준 파일 수리, 미완료 현품표 교환 복구·완료 이력 취소/삭제·설정·켜진 시뮬레이션의 PC 앞 창은 유지한다.

교환·일반 UNDO·미달 release·자동 source 검토 감사는 current-state의 scanner_audit_outbox에 업무 checkpoint와 함께 저장한다. 고정 key·원 시각·작업자·이벤트 내용을 보존하고 직후·다음 스캔 전·저장 재시도·기동 때 기존 CSV writer로 재생한다. 같은 key의 동일 CSV 행은 재append하지 않고 fsync하며, CSV와 outbox 제거 checkpoint가 모두 성공한 항목만 목록에서 지운다. CSV 실패는 입력을 막지 않고 pending을 유지한다; 업무 checkpoint 실패만 기존 차단한다. UNDO·reset·완료 후에도 미전송 감사는 보존하므로 교환과 그 UNDO가 모두 남는다. 목표 수량에 도달하지 않은 재시도를 PASS 완료하지 않는다. 기동·복구는 기존 DB 보류 snapshot과 검증된 archive의 pending 감사도 읽어서 같은 ID로 재생한다. 보류 원본·PIN에 결속된 현재 파일은 재작성하지 않고, 활성 파일이 없으면 만들지 않는다; 보류 감사의 재생 여부는 CSV readback으로 판정한다. 큰 감사 목록은 `_scanner_audit_outbox/<SHA256>.json`의 내구 조각을 먼저 저장한 뒤 `scanner_audit_segments` 참조와 업무 checkpoint를 원자 저장한다. 재기동은 SHA·스키마가 맞는 조각을 기록 순서대로 재생하며, 손상·소실 조각의 원본과 참조는 보존한 채 다른 감사와 입력을 계속한다. 이전 8MiB 초과 inline 감사도 업무 본문 검증 뒤 읽는다. 백업·보류 복원에는 checkpoint와 `_scanner_audit_outbox` 폴더를 함께 보존하며, 완료된 조각도 자동 삭제하지 않는다. CSV 재생은 prefix의 byte 수·행 수·SHA256·마지막 행 hash를 기존 current-state의 scanner_audit_csv_prefixes에 함께 저장한다. 재기동·파일 변화 때 전체 prefix와 전 행을 검증하고 같은 writer의 내구 append만 증분 색인하며 ACK 직전에 원 바이트를 재대조한다. 증명 손상·불일치는 기존 전 행 비교로 되돌아가며 업무 복구를 차단하지 않는다. 같은 key의 다른 내용만 기존처럼 거절하고 동일 중복·순서 변경·잘린 끝에 새 전역 오류를 추가하지 않는다. 행별 fsync·마지막 ACK checkpoint와 찢긴 쓰기 뒤 정확히 한 번 재생을 유지한다.

교환 pair·제품 raw/parsed·UNDO 위치·감사 intent를 같은 checkpoint에 저장한 뒤 표시한다. 이벤트 저장이 끊겨도 제품과 되돌림 위치를 함께 복원하며, 옛 pair-only tail 저장본의 UNDO도 받는다. 표본·교환 대상·늦은 source 확인은 교환 overlay를 적용한 같은 유효 구성원 목록을 사용한다. 대체품은 알려진 원 봉인 구성원·대상 자신·앞선 교환의 불량/대체품을 상태 변경 전에 거절하며 source 목록을 모르면 기존 배경 판정으로 넘긴다. 일반 UNDO도 checkpoint 저장 직후 메모리를 맞춰 후속 이벤트 실패 때 저장본과 어긋나지 않는다.

스캐너만 포장: 구현·격리 회귀는 scanner-only-packaging 계약과 [제거 경계](scanner-retirement.md)를 따른다. 실제 스캐너·음향·화면 및 새 서버 계약의 현장 수용은 후속 랩 검증이다.

## W9 · 교환 journal 크기 경계 후속

`PHSLabelExchangeJournal.save`는 JSON 크기 상한 없이 저장하지만 `load`는 8MiB 초과를 거절한다. 한 세트의 교환 기록으로 정상 크기는 세트 크기에 묶이며, 감사 outbox처럼 장애 기간에 비례해 누적되지 않는다. 실제 초과 재현은 미실행이다. 권장 후속은 저장 시 같은 한계 검사 또는 큰 목록 분할이며, 이번 감사 backlog 수정 범위에서는 변경하지 않았다.

## W9 · 로컬 제품 자리의 현품표 거절·막힌 접수 안내 (w9nextfix)

항목 6: 새 체계 현품표가 로컬 5단계의 제품·최종 라벨로 들어가던 경로를 공통 표식 분류로 거절한다. 3종 현품표 × 3종 로컬 세트 × 2자리의 헤드리스 RED 18 FAIL → GREEN 18 PASS와 다음 정상 제품·최종 라벨 수용을 확인했다. e88011c→이 수정 판의 정확한 방향성 업그레이드 핀을 추가했다. 실제 창·스캐너·packet 설치 수용은 별도 확인 대상이다.

항목 3: `BLOCKED_INVALID`·스캔 0 자기 접수의 F1 안내·키·단추를 같은 안전 술어로 연결했다. 기존 취소 transaction이 원본을 보존해 `CANCELLED`로 닫으며, CA 봉인 뒤 같은 현품표는 새 접수로 검증·세트 진행한다. 문구만으로 재사용할 경로가 없던 F-a–F-d를 동작 수정으로 해소했다. 관리자 검토·다른 접수·검증 대기·검증 완료·반영 중·UI lane 처리 중과 초기화·이력·오류·완료 gate는 유지한다. 실제 창·스캐너 확인은 NOT TESTED다. 근거는 `D:/KMTech/program-improvement-20260912/work/program/w9nextfix/sub-2b/REPORT.md`에 기록한다.

남은 제안(P3, 제품 변경 없음):
- 입력 오류 공통 문구: `_handle_input_error`는 모든 입력 오류에서 큰 화면에 '입력 오류 - 새 현품표부터 시작'과 '확인 후 새 현품표부터 시작'을 띄우지만, 제품 자리 오류 뒤에도 세트는 계속되고 사유 문장은 '현재 세트의 제품 바코드를 스캔'을 안내한다. 세트가 계속되는 오류와 새 현품표가 필요한 오류를 나눠 큰 화면 문구를 맞춘다.
- 옛 방식(LEGACY) 현품표 QR은 제품 바코드와 오판 없이 가를 표식이 없어 로컬 제품 자리에서 여전히 접수된다(w9nextfix sub-2 같은 부류 확인). 구별 기준이 생기면 같은 거절로 넓힌다.
- 제자리 업그레이드 핀은 e88011c→이 판만 연다. 956cc8f 등 다른 설치 판은 설치기가 거절하므로 지원 경로(정상 제거→재설치→재등록)를 쓴다. 다음 판 packet 은 설치된 판과 같은 update key 설정으로 빌드해야 한다(다른 설정의 후보는 `app/update-manifest-key-config.json` 차이로 거절 — sub-6 gate).

<a id="lm-w9-legacy-transition"></a>
## W9 · 과도기 모드 (w9lmtransit)

관리자 스위치로 등록 PC가 모든 현품표를 받고 완료마다 네 분류·사유·중복을 기록한다([절차](operations.md#legacy-label-transition)). 남은 일: ① Web에서 분류·사유·중복을 PC·날짜별로 세고 새 사건 ID로 온 의도한 중복 완료를 관측 1건으로 받기(웹 후속, 지금은 detail 원문만 보존), ② 두 체인 이중 계수를 찾는 바코드 대조 보고서, ③ 꺼짐 상태 F4 전체 재스캔에 없는 TRANSFER ID를 넣으면 포장 전송함에 중앙 충돌이 쌓이는 기존 위험(미교정), ④ F3 단계에서 로컬로 바뀐 세트의 lease가 PREFETCHED로 남아 만료되는 동작 확인, ⑤ 실제 화면·랩·현장 수용은 NOT TESTED, ⑥ 결과 불명 capture(결과확인중·관리자 검토)는 켬에서도 기존 화면으로 남음. ⑦ 복원 끝 12경우 시험 표에 일시 회복·닫기 결과 불명 사례 추가(sub-06 선택 후속). ⑧ CA 와 맞춘 분류 표(w9transitalign)의 사용자 판단 3건 — 읽히는 옛 단계 QR `CLC|SPC|PHS=2`를 LEGACY 로 둘지(현장에 없으면 MALFORMED 가 더 정확), 품목 코드 없는 새 모양을 CA 도 받게 할지(LM 은 받음·제품 대조 없음, CA 는 그 현품표만 거절), 품목 목록 밖 정상 새 현품표의 PHS2_LOCAL 기준 차이(CA 는 중앙 전에 품목 요구) — 지금은 권장안(LEGACY·LM 받음/CA 거절 유지·그대로).

<a id="lm-w9-scanner-flow"></a>
## W9 · 스캐너만 흐름 (w9lmscanmsg)

w9scanflow 비교(옛 앱 대비 스캐너만으로 일하기)의 포장 항목을 고쳤다: 과도기 로컬·형식 오류·중복 세트의 시작 알림은 Windows 경고창 대신 창 안 안내 띠와 경고음(F2), 처리 중 스캔은 '입력 보존'이 아니라 '스캔 안 받음·끝나면 다시 스캔'과 경고음(F3, 처리 전부터 입력칸에 있던 값만 '입력 보존'), PHS2 캡처 저장 실패 때 입력칸 비우기와 스캔 없이 시작하는 뒤 작업(저장된 현품표 중앙 확인)의 스캔 중 시작 미루기(남은 값이 다음 스캔과 붙어 한 값으로 접수되던 경로). 실제 창·스캐너 확인은 NOT TESTED. 남은 일: ① F5 새 현품표 포장을 스캐너만으로 끝내기 — 지금은 현품표 스캔 뒤 F3과 '포장 완료 확인' 예(Enter)가 매 세트 필요하다(`_prompt_manual_complete`; 대기 중 스캔은 `포장 완료 대기` 경고창). 같은 현품표 재스캔이나 인쇄한 '포장 완료' 바코드를 F3으로 받고 스캔으로 시작한 완료는 예/아니오 창을 생략하는 안. ② F6 여섯 자리는 기존 안내·이력 영역으로 교정했다: 키보드 승인을 거절하고 같은 확인 요청의 마우스 누름·놓음만 승인한다. 실제 한국어 Windows·스캐너·IME 확인은 NOT TESTED이며 위 F6 과제 결과·guest 절차를 따른다. ③ 비동기로 뜨는 차단 안내(포장 전송함 결과 등)가 스캔 도중 입력칸을 잠그면 앞부분이 남아 다음 스캔과 붙을 수 있다(드묾, 미교정). ④ F키를 처리 중에 누르면 스캔이 아닌데도 '입력 보존' 문구가 나온다. ⑤ 뒤 작업이 중앙 응답을 기다리는 동안 입력칸이 잠기고, 그 사이 스캔은 F3의 새 안내로 거절된다. ⑥ 붙은 값을 값 검사로 거절하는 규칙은 넣지 않았다: 남은 모양(앞 글자 몇 개+온전한 스캔)은 현품표 코드를 한 번만 포함해 좁은 규칙으로 못 잡고, 길이·형식 규칙은 받는 값의 범위를 바꾼다. 화면 잠금(이력 불러오기·지난 이력 보기·차단 안내·중앙 1/1 F3 대기)·조회 중·종료 중에 들어온 스캔도 경고음과 안내를 낸다(재검토 1733). ⑦ 완료 저장 중 스캔 대기열(B)은 넣었다(README LM-12). 남은 한계: 대기 스캔을 처리하다 그 스캔이 다른 lane 작업(PHS2 캡처 등)을 시작하면 나머지는 기다리지 않고 거절 안내로 다시 스캔을 받는다; 중앙 F3의 사전 확인(`f3-package-source-refresh`, 서버 조회)과 다른 lane 작업 중 스캔은 여전히 거절 안내다; 실제 Tk·스캐너 속도에서의 확인은 NOT TESTED. 완료 저장 중 상태줄 꼬리 문구 `처리 완료 전 추가 입력은 받지 않습니다.`는 B 뒤 사실과 다르다(스캔은 대기열로 받는다; 첫 대기 스캔부터 `이어서 처리할 스캔 N건`으로 바뀐다) — 꼬리 문구를 작업별로 나누려면 M7 캡처 계약(`_set_ui_lane_busy` 꼬리 문구 1개를 캡처 도구가 그대로 씀)을 함께 바꿔야 한다. ⑧ A′: 중앙 F3에서 lease를 그때 서버에서 받으면 응답(제한 시간 8초)만큼 입력칸이 잠긴다 — 발급 대기를 2–3초로 줄이고 켬에서는 로컬 완료로 넘기는 안(서버 통신 전체를 잠금 밖으로 옮기는 A는 전송함·lease 계약 변경이라 하지 않음).

## W9 · 부분 등록 지원 복구 (O11, w9lmpartial)

**w9lmrelay P1:** 기존 큐/authority와 정상 stop marker가 있는 PC의 partial/full 복구 뒤 key 회전으로 재개·설치가 막히던 경로를 교정한다. 중앙 확정에 결속된 원자 handoff와 보호 감사, canonical onboarding 재개, 정지/다음 행동 안내를 추가한다. Web ad0fc84 실제 handler의 미전송·lost-ACK 재서명/단일 ACK 및 stock installer의 read-only healthy 판정을 격리 시험한다. 실제 설치·업그레이드·Windows 재로그인/화면·현장 첫 업무는 후속 랩 수용이며, 다른 앱의 자격 회전 경로는 이 LM 변경으로 교정되지 않는다.

랩 최종 리허설 P2 두 건도 교정했다: 설치된 등록기가 bundled 의존성을 찾는 직접 실행 경로, 필수 명시 인수 누락 시 한국어 옵션·서버 읽기 출처 안내. stock app 복사와 격리 Python에서 직접 `--help` 실행, 신원 조회 전 누락 거부를 회귀로 고정한다.

신원 파일 일부 부재·손상으로 기존 관리자 복구도 거부하던 분기에 명시 `--recover-partial-local-state` 경로를 추가했다. 같은 사용자·PC의 유도 install ID·기존 키·root 승인을 검증하고 원본 보호 보관과 기존 서버 2단계 복구로 중단을 재개한다([절차](operations.md#partial-registration-recovery)). 소스 회귀 범위는 파일 조합·거부·ACK 유실·이동/로컬 저장 중단·실제 DPAPI/profile·READY 이후 첫 요청이다. w9lmpartial2는 만료·종결 journal에서 새 root 승인으로 전환하며 이전 journal·원본을 보존한다. prepare 전 중단·이동 후 만료·완료 후 profile 유실, 진행/불명 상태·타 신원 거부, 새 후보/전환 중단·ACK 유실을 회귀로 고정한다. 실제 packet 설치·서버 승인/첫 runtime lease·PHS2/F3의 중앙 receipt와 화면은 후속 랩 수용으로 남는다. 비유도 legacy install ID의 부분 신원 이관은 이 옵션의 지원 범위 밖이다.

후속 후보: cutover 뒤 Web의 prepare ID 없는 승인/request 상태 조회를 검토한다. 현재는 Web ad0fc84 계약을 유지하며 보호된 후보와 새 prepare 성공으로 superseded를 확인한다. 일반 authorization 오류를 terminal로 취급하는 우회는 허용하지 않는다.

<a id="lm-w9-lease-screen"></a>
## W9 · runtime lease 격리 자동 회복 이후 (w9calmleasefix)

갱신 거절(`STALE_RUNTIME_FENCE`)의 자동 회복은 소스·격리 시험과 Web 362ae46 lease 서비스 재현으로 확인했다([계약](contracts.md#lm-lease-dead-renewal)). 작업 화면 하단에 서버 반영 상태(정상·재시도 중·멈춤)를 표시한다([계약](contracts.md#lm-relay-screen-status), w9lmdistatus; 실제 창 확인은 NOT TESTED). 저장이 relay 상태 파일을 다시 써도 relay DB 의 lease 검토·응답 없는 요청이 남아 거짓 '정상'이 되지 않는다(w9lmdistatus2). 남은 한계: relay 가 멈추면 lease 만료(마지막 갱신 뒤 최대 900초)까지 `정상`이 남을 수 있고, `LEGACY_DISABLED`(observe·lease 미지원 서버)는 만료가 없어 오래됨을 판정하지 못한다. DB 에 남지 않는 relay 오류(자격 증명·manifest)는 다음 relay 기록까지 저장 기록에 가려질 수 있다. 재설치 복제본 거절은 옛 lease가 끝난 뒤 첫 주기에, 자료 경로 dead fence는 enforce 자격증명에서도 회복한다(w9calmenforce, 소스·격리 시험·Web 362ae46 하네스). 자동으로 풀리지 않는 lease 검토(살아 있는 복제본 충돌·토큰 불일치, 교정 전 판본이 남긴 자료 경로 STALE 검토)는 화면에 `서버 반영 멈춤`으로 보이고, 원인 확인·해제는 기존 `tools/direct_sync_relay_operator.py`로 한다. 실제 HTTPS·랩·회사 서버 회복은 NOT TESTED다.

## W9 · 기존 시험 실패 감사

닫기 실패의 Tk scheduler double, onboarding의 Verify→ObserveLifecycle 순서·허가 밖 실행, 실제 relay 자식의 DEFERRED/WRITER_GATE_TIMEOUT·업무 전송 0, 신원 cache의 매 호출 무결성 확인을 현행 계약으로 단언한다. 실제 자식 시험은 변경 없는 committed HEAD에서 실행한다.

최소 mutex 권한의 소스 회귀와 정확한 전이 pin을 확인한 뒤에도 새 packet의 상승 설치→일반 제거·재등록과 실제 다른 계정의 거부 보존은 랩 수용으로 남는다.

기존 전체 suite의 21 FAIL은 제품 변경 없이 fixture/API 16개와 저장 루트 환경 의존 5개로 교정했고 첫 전체 3065 PASS/5 SKIP를 확인했다([분류·현재 검증](operations.md#test-audit-w9)). 마지막 시험 변경 뒤 전체 3회 반복의 결과는 해당 RESULT로 결속하며 실제 GUI 5개·VM·서버·운영 lifecycle 수용은 별도로 남는다.

## W5-B2 · carrier 경계 이후

U13 문구 대조에서 activity 제목을 `이 PC의 최근 기록`으로 명확히 했다. 진행·통과·오류를 포함한 로컬 이력이며 중앙 포장 ACK 목록이 아니다. 행·상태·F3/F4 절차는 보존하고 기존 workbench 시험에서 의미와 원 결과를 확인한다. 실제 글자 가독성은 화면 검증 대상으로 남는다([확정 경계](contracts.md#states)).

LM-3 파생 capture 시험 helper는 F3 busy 문구를 소유한 `label_completion.py`까지 AST 검색 범위를 확장했다. 기존 단일 main 검색의 admitted-busy 3 FAIL은 원 로그로 보존하며 기대 문구·상태·제품 경계는 바꾸지 않는다.

H-LM3는 수용된 `198cad02`의 F3 권한 확인·로컬 완료 저장 문구와 `734160c`의 비동기 review 소비에 capture fixture를 정합한다. Tk 없는 activity fixture에 thread 초기값·timer 대체를 제공하고 기존 assertion·payload·209노드와 제품을 보존한다([4건 분류·검증](D:/KMTech/program-improvement-20260912/work/Label_Match/w6hlm3/RESULT.md)).

[현행 carrier/legacy/Item Code 포트](carrier_identity.md)는 기존 동작을 명시 인자로 옮긴 경계다. W5-L 공용 leaf는 두 실제 소비자의 입력·반환·오류 의미가 동등하다는 근거 뒤에만 검토한다. raw workflow와 스캔 Base64, compact와 legacy, sample과 exact membership의 차이를 통합하지 않는다. 실제 새 제품 정책·profile/schema 활성화와 PHS2/receipt/hash 변경은 별도 요구·레인 소유이며 이번 작업으로 활성화하지 않는다.

H-LM에서 `test_standard_operator_copy_describes_one_phs2_and_atomic_replacement`를 현행 요구와 대조했다. 제품 문구는 PHS2 1회 → 선택 F4 교체 → 랩핑 후 F3 완료에 맞으며, F4 목록의 일괄 적용 안내와 F3 로컬 내구 완료·중앙 ACK 표시는 각 화면에 유지한다. 제품 변경 없이 시험을 순서·선택 조건·새 전자 QR 확인·원본 유지·레거시 단계 배제 검사로 정정했다. W5-B2 fresh baseline의 문구 1 FAIL과 기본 경로 1 PASS는 별개 관찰로 보존하며, 재사용 환경의 기본 경로 실패는 synthetic onboarding 상태의 영향이다. [요구 대조·현재 focused 검증](D:/KMTech/program-improvement-20260912/work/Label_Match/w6hlm/RESULT.md)과 [원 검증·격리 근거](operations.md#carrier-identity-w5b2)를 따른다.

**H-LM2 · restored-F4 workbench 6건 정합:** `aa12be1`의 의도된 worker snapshot 전환 뒤에도 옛 동기 accessor를 mock하던 시험으로 분류했다. 현재 context의 `_package_review_snapshot`을 입력으로 사용하며 네 blocked-action 벡터의 원 안내·F3/F4/입력/F1 상태, 기존 blocking notice 우선, 후속 빈 관측에서만 해제되는 조건과 원 세트 보존을 검증한다. 렌더의 동기 accessor 조회는 없어야 한다. 제품·계약·pin과 기존 기대값은 유지하며 skip/xfail은 추가하지 않았다. [분류표·도입 근거·현재 검증](D:/KMTech/program-improvement-20260912/work/Label_Match/w6hlm2/RESULT.md)을 따른다. H-LM의 `428a3b7` archive, LM-1/LM-2 전후의 동일 6 FAIL 및 이번 수정 전 6 FAIL은 보존한다. 당시 여섯 모듈 **531 PASS**는 해당 버전·선택의 별도 결과다.

## X13-B · PowerShell leaf 채택

R1의 구 설치본→X13-B 지원 전환은 57f52e1 shared preimage와 이번 release의 추가/교체 파일 pin에 한정한다. 양 엔진의 합성 upgrade/교체 후 검증과 변조·누락·링크 거부는 [현재 결과](operations.md#shared-powershell-x13b)를 따르며, 실제 설치 lifecycle 수용은 별도다.

정본 0.3.1 전체 5파일·manifest·lock을 다시 채택했다. 0.3.0 배열 결함의 두 실패 로그는 보존하며 양 엔진에서 singleton·중첩·빈 배열의 원본 parity를 재검증한다. 실제 lifecycle 수용은 별도다.

0.3.6/9파일과 standalone checker·패키징을 채택하고, 신뢰 확인 뒤 설치기 leaf만 위임한다. LM task migration·conflict receipt·ordinal inventory/복원은 앱에 유지한다. [검증·잔여 범위](operations.md#shared-powershell-x13b)의 합성 PS5/PS7·lifecycle 회귀와 실제 배정 대상의 설치/제거/복원 수용을 구별하며, 실제 lifecycle/GUI/전체 build는 이번 레인에서 수행하지 않는다.

## X04-B · shared core 채택

catalog 8개 leaf·renderer 호환 facade·고정 package·manifest/lock·portable/PyInstaller 생성 입력을 채택했다. [headless 검증](operations.md#shared-core-x04b)에서 v2 bytes/복구·LM profile/URL/진단 adapter, PNG/QR/반환 class와 기존 writer 경계를 확인했다. 인증 I/O·복구·snapshot의 추가 공통화는 이번 pin에 없는 후속 범위이며 실제 GUI·인쇄·설치 수용은 LM-B04에 남는다.

단독 checkout의 pin 검사는 앱 QA 진입점에서 자급하며 정본 교차 대조는 명시 실행 노드로 분리한다. 정본 checker 갱신 시 [검사 절차](operations.md#shared-core-x04b)로 복사된 검증 함수의 동등성을 확인한다.

package pin은 0.3.6/9파일이며 catalog/raster/runtime 계산 bytes는 유지한다. 새 진단 로그 잎과 PowerShell 머리 정본도 portable 및 실제 frozen data/배포 목록에 포함한다. 진단 로그의 앱 연결은 별도 판이며 factory lock은 변경하지 않는다. 자료 세대 UNKNOWN의 비공개 원인은 앱 guard에 보존한다. 컷 통합 뒤 생성 writer/설치기·업그레이드 선언을 한 번 재생성하고 실제 설치·현장 수용을 확인해야 한다.

## X05-B · producer runtime 공통화

25개 leaf/binding/transaction 함수를 pinned runtime facade로 채택하고 기존 runtime45·새 callback/ACK 원자성7 회귀를 통과했다. [검증 경계](operations.md#shared-runtime-x05b)의 LM reviewed 복구·transport·writer admission·신원 생성·예약 orchestration은 앱에 남는다. 실제 현장 복구/설치 수용은 기존 업무에 남으며 CA의 자동 복구 정책이나 전체 onboarding/relay 공통화로 확대하지 않는다.

W5-S0는 기존 scope tuple 3곳과 package-client 생성 1곳의 named 전달을 적용했다([검증](operations.md#named-context-w5s0)). 다음 DI → IW → RW 확장은 각각 감사 HMAC/replay 권한·return/outbox, ledger/outbox·maintenance admission, 고정 provider·queue/writer identity를 보존하는 별도 소유 단위다. RW 검증은 직렬이며 tenant/customer/line·다중 scope·새 Shared leaf는 이번에 도입하지 않는다.

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

- UI-AUDIT-1009 U1·U5·U7: 서버 연결 줄은 단추 아래의 독립 row 2, 네 단추는 같은 폭의 두 열로 교정했다. 호스트 창 없는 회귀와 별도로 VM에서 1366×768·1920×1080 각 100%·125%의 정상/끊김/긴 대기 표시, 네 단추의 전체 글자·클릭 영역을 확인해야 한다.
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

- W9NB는 현재 슬롯을 점유하던 일반 충돌·복수 미완료 command·교체 review를 set별 보류로 분리했다. 관리자 코드 확인과 hold readback 뒤 다른 set 업무는 계속하며, 동일 PHS2와 보류 command 자동 전송은 막는다. 저장소 공통 장애는 durable 보류도 불가능해 현재 세트 유지·원인 안내·재시도·정상 종료로 제한한다. 보류 사건의 실물 해소·종결 권한과 UNKNOWN의 중앙 대조는 이 항목의 P1 잔여다.
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

- 상승 토큰의 배치 helper 직접 실행 갈래를 추가했다. 실제 SSH 관리자 토큰의 승인 창 0·성공 거래·등록 실패 복원 수용은 NOT TESTED이며 현재 호스트 비상승 토큰의 transport 회귀와 구분한다. SSH 계정과 작업자 등록 신원의 선택은 w9installarch·1차 설계의 열린 질문으로 남는다.

- 등록 토큰 선택화: 정상 등록은 기존 빈 토큰 허용을 유지하고 관리자 복구의 HTTP 이전 필수 토큰 guard를 제거했다. 토큰 생략·서버 허용/거부·명시 토큰·일회용 복구 승인 보존은 headless 회귀로 검증하며 [IP 기준 절차](operations.md#enrollment-ip-policy)를 따른다. 실제 설치 PC의 허용/비허용 IP와 서버 연동·화면 확인은 별도 수용으로 남는다.

- 2026-09-10 F4 실제 단일 Apply는 instruction-conflict/OPERATOR_REVIEW·attempt2로 보존 중이다. [기존 창의 명시적 같은 요청 재시도](operations.md#f4-explicit-retry-20260910)는 원 C에서142 PASS로 검증했으며, live source 적용·실제 retry/새 QR·F3와 Today 전체 GUI 전후 비교는 미실행이다. 별도 검토 CSV acknowledgment 뒤 자연 relay PASS/idle·13 ACKED는 transport에 한정하며 이 업무 잔여를 닫지 않는다.
- 실제 Claude 후속의 저장 목록 안내·명시적 거부 표시·불확정 목록 읽기 전용 재열기는 기존 소스147사례로 검증했다. 선행142·원 실패·ee73 packet을 보존하며 최종 source/backend 검토, 후속 후보의 정상 적용과 native 화면·재시도/새 QR/F3는 별도 잔여다. 불확정은 receipt-only이며 재전송·DB 초기화로 해소하지 않는다.
- a362 실제 Claude의 F1–F3는 다른 저장-command review의 읽기 전용 재열기·접수 가능성 안내·lease 재시도 보류 문구로 교정했다. 기존 영향25사례 PASS와 수정 전3 FAIL을 [최종 소스 근거](operations.md#f4-explicit-retry-20260910)에 보존하고, 변경 없는 coordinator의 이전 근거를 재사용한다. live source는 별도이며 native 재시도·새 QR/F3·전체 GUI 수용 잔여는 유지한다.
- 유형/우선: **구성·검증 공백 · P1**. 소스의 정상 경로와 설치 PC의 선택 프로필/장비가 다를 수 있다.
- 근거: [운영 구성·설정](operations.md#configuration), [장비](operations.md#devices), [LM-01/02/05/10/12](README.md#lm-01). Machine anchor, current-user profile/DPAPI, packaged settings와 실제 쓰기 위치, F4 capability, F5 driver·spool을 구분한다.
- 상태/다음: **실제 조합 미확인**. 릴리스·Label·현장 담당이 기존 후보 근거에서 artifact/source/test 식별, 실제 module/provider/overlay·Python/라이브러리, 비밀 없는 profile identity·권한·flags, scanner·프린터·사운드·화면 조건을 연결한다.
- 2026-09-09 최종 Label native continuation: [receipt12 최종 native continuation](D:/KMTech/cold-program-material/from-E/KMTech/label-install-qualification-20260908/RECEIPT12-NATIVE.md)은 수용 source `da60e05e9bf07855f701fff328df70c10db0a094` / ZIP `782d585bf09396da8c33fbb3f1c3b24afc6a8716f73e0b4983b951a896633d6e` / manifest `829f5f2230add6f0245106c5b4dac40547f56941792c2da46dbf5318effa6111`에 결속된다. 원 assigned VM에서 동일 artifact의 정상 설치 attempt02 native0, exact retained14의 지원 `ack-reviewed` native0, 지원 current-user 제거·code-only 제거, 수용 timing06 prior-code 설치/보존, 최종 receipt12 복귀 설치03:42:16Z/native0와 readback0을 완료했다. GUI13은 기존 resident를 재사용하여 PHS2 대기·이전 완료 이력을 표시하고03:44:40Z/native0으로 정상 종료했다. 실제 정상 shutdown 뒤 Off/uptime0을 관측하고 새 boot03:46:01Z·Explorer4640/session1에서 원 SID의 resident7556이 03:46:22Z에 자동 시작한 것을 확인했다. 수동 product 시작 전 단일 resident·정상 HKCU Run·canonical task0·stop/fence 없음이며 postboot native0은 payload3,391개와 보호 파일25개, 최신 post-review backup의 업무11 tables/40 rows를 확인한다. 전체 상태 메타데이터는 40/51개 동일하며 나머지 11개 status/control/settings/log 변경을 기록했다. app_settings 필드별 차이는 미확인이고 전체 config/DB/schema/hidden-rowid 동일성이나 새 대상 설치를 주장하지 않는다. [최종 보존 readback0](D:/KMTech/cold-program-material/from-E/KMTech/label-install-qualification-20260908/RECEIPT12-FINAL-PRESERVATION02.json)은 원 receipt/metadata/spool/attempt1·audit1과 inserted10/quarantined4를 유지하고 queue11ACKED를 기록한다. 원 event CSV prefix 두 개가 동일하며 APP_START1·APP_CLOSE1만 추가됐다. 원 F1/F4/F3·shipping·로컬7 업무나 pre-F3 DB 복원을 반복하지 않았다. 첫 설치 native1/UAC 취소·지원 rollback, 별도 pre-execution policy rejection과 reader 실패는 이력으로 보존한다. [Main의 선택 범위 최종 native 수용](E:/KMTech/resume-after-input-20260908/LABEL-RECEIPT12-FINAL-NATIVE-ACCEPTED.json)은 `PROVEN_SELECTED_LABEL_FINAL_NATIVE_ACCEPTED`이며 Main msg_b42de21979ea 승인에 따른 정상 guest shutdown 뒤 [최종 VM Off/uptime0](D:/KMTech/cold-program-material/from-E/KMTech/label-install-qualification-20260908/RECEIPT12-FINAL-VM-OFF.json)을 03:58:51Z에 확인했다. 이 수용은 기존 assigned VM의 선택된 Label 범위에 한정한다. 전체 제품 Ready는 **0/6**이며 여섯 프로그램의 현재 조합 판정과 다음 S05 source workspace 결정은 Main 소유다.
- 완료 기준: 지정 조합의 시작/품목 cache/권한 거부, Enter·종료문자·IME·busy/포커스·배율, F4 전자 QR, 이전 판 F5 복구의 실물 출력이 각 수용 결과와 연결된다. spool 성공과 종이·판독은 별도 판단하고 비적용 장비는 이유를 기록한다.
- 의존/병렬: Main의 목표 후보·장비 범위와 LM-B10 선택 일치 확인 필요. 설정·기존 증거 정리는 VM/장비 실행과 독립 가능. 실제 실행은 현행 qualification 작업이 소유한다.

<a id="lm-b05"></a>
## LM-B05 · 기존 PASS 적용 범위와 남은 수용 검증 매핑

- W7LMFIX의 [세 행 업무 fixture](operations.md#business-fixtures-w7lmfix)는 실제 exchange/current-state 복원, signed lease의 CSV→marker→pending/ACK/conflict 및 합성 자정·lost ACK를 연결한다. 원 W7-V4의 store 0행/표시 DTO ACK와 실패는 보존한다. 최신 full-single work-group의 PHS2→F4 팝업, async ACK 화면·native 종료/재시작·실서비스 수용은 여전히 UNPROVEN/NOT TESTED이며 추가 VM 재장면에는 별도 사용자 승인이 필요하다.

- LM-3의 [F3 완료·pane 분리](operations.md#module-boundaries-lm3)는 원 command·내구 저장·성공/ACK·widget/binding 순서를 보존하는 headless 동등성 범위다. 기존 시험은 변경하지 않으며 실제 GUI·장비·설치·중앙 transport 수용을 대신하지 않는다.

- LM-2의 포장 물류 분리는 [기존 API·동등성 검증](operations.md#module-boundaries-lm2) 범위다. 기존 workbench 6 FAIL과 GUI·설치·실제 transport 수용은 별도이며 분리로 종결하지 않는다.

- LM-1의 저장·이력 계산 모듈 분리는 기존 API·root·내구 저장 계약을 유지하는 소스 변경이다. [동등성·회귀 범위](operations.md#module-boundaries-lm1)와 실제 설치·화면 수용을 구분한다.

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

- 옛 허용 전이 출발 코드 + 자기 등록 ABSENT의 canonical 첫 등록 경로를 추가한다. 부분/손상 상태·알 수 없는 판·역방향 거절과 공용 신원 비차용을 유지한다. 실패는 credential 선이동/각 3회 보존 재시도, ABSENT 확인 뒤 옛 코드 복원으로 처리한다. 미완료는 fence·남은 경로·시도 기록을 보존하고 동일 명령이 rollback만 재개한다. 보류 기록의 fence가 없으면 정상 Start로 획득하며 다른 writer fence는 거절한다; 앞 사본을 덮어쓰거나 서버 producer를 지우지 않는다. 재등록 409 지원 관리자 복구, 실제 설치·등록 실패/재실행·옛 앱 재개의 랩 수용은 남아 있다([현재 절차](operations.md#설치업그레이드제거백업롤백)). 합성 경계 회귀와 실제 수용을 구분하며 현장 사용자의 자기 등록 완전성을 코드만으로 단정하지 않는다.
- W9NB 설치/등록 L08은 배포 파도에 이관한다. 기존 identity·등록 승인·stop marker·writer fence를 앱의 업무 보류 버튼으로 우회하지 않는다. 정상 등록 도구 진입과 실패 원인 안내의 실제 설치 화면 수용은 P1 잔여다.
- 건별 보류 기록은 공유 outbox SQLite의 `package_workbench_holds`(읽지 않은 current/F5 파일 항목 포함), `phs_label_workbench_holds`, F5 전체 source 결속 표 `phs_label_workbench_hold_sources`, PIN 결속 감사와 `.held-<ID>` 원 항목에 있고, 별도 `label_admin_pin_intents.sqlite3`에는 원 지문·operation key·UNKNOWN 인계가 있다. 두 SQLite와 원본 항목을 함께 백업·복원한다. PIN 변경본은 `(operation_key, file_kind, source_path)`별로 보존하며 기존 단일 key 행을 손실 없이 이전한다. 관리자 복원 후 재시작과 연결 current/journal의 개별 복원은 headless 회귀로 검사하고, DB·파일 불일치는 목록의 재복원 경로를 유지한다. 보류·재조회·실물 조회·지원 인계의 관리자 본인 확인은 웹 개인 PIN으로 바꾸고 기존 보호 코드→SID 승인은 제거했다. 복구 창의 실제 Tk 표시·실물 스캔·운영 중앙 조회·설치본 백업/복원은 headless 회귀와 별도 수용으로 남는다.
- 비정상 stop marker 복구: capture8의 첫 제거 실패는 PowerShell wrapper가 정상 stderr를 terminating error로 승격한 것이며 제품 제거 결함이 아니다. 기존 제거→정상 marker/report→stock 업그레이드·2회 headless 무결성 readback·데이터 보존 경로를 회귀로 검사하고 [복구 명령과 host/guest 검증 경계](operations.md#abnormal-stop-marker-recovery)를 명시한다. writer/receipt/무결성 guard는 유지한다.
- 재시작 무결성 순서 오탐: `w9labelintegrityorder`에서 `d0e504e`의 ordinal/casefold 불일치와 기존 v1 기록 호환을 교정한다. 파일 무결성·receipt 검사는 보존하며 capture6의 수정 후보 재설치·동일 사용자 재시작은 [운영 절차](operations.md#bootstrap-integrity-order)에 따른 미검증 항목이다.
- writer 업그레이드: 실제 onboarding 의미 변경의 directional pin은 유지한다. 검토된 슬림화 소스의 문구·파생 inventory pin 변경도 기존 preimage와 목록을 유지한 채 후보 쪽 release-pair/companion digest를 함께 갱신한다. capture8의 `aa4d59b` 설치 거부는 checkout CRLF가 bundled CSV/설정에 들어간 빌드 결함이며 `w9labelwriterupgrade2`는 committed snapshot 패키징으로 교정한다. host 회귀는 실제 LF `d0e504e` stock 설치본에서 canonical 교체·2회 headless 무결성 readback·사용자 상태/구 record 보존을 검사한다. 새 stock 후보의 guest 설치·동일 사용자 GUI 2회 재시작 수용은 capture8 소유다([절차](operations.md#bootstrap-integrity-writer-transition)).
- 기존 `healthy_lifecycle` 실패 복원의 `quoted=False`·`quoted=True`, `integrity-order`·`after-release` 네 변종은 파일별 복사를 robocopy로 바꾼 뒤 2026-09-24 동시 pytest 부하에서 각각 PowerShell 자식 180초 제한을 넘어 감사가 `PREIMAGE_SAVED`에 머물렀다. 다른 Label pytest·relay가 모두 종료된 quiet window에서 시험 한도와 원 단언 그대로 네 변종을 독립 재실행해 모두 PASS했고, 네 audit가 `FAILED_ROLLED_BACK`, `rollback.runtime_restored=true`, `rollback.code_restored=true`, `RESTORED_PREIMAGE`를 기록했다. canonical 설치기·writer fence 전체 90개도 PASS했다. 부하 중 시간 여유와 실물 설치·복원 수용은 P1 검증 대상으로 유지한다.

- 유형/우선: **운영 요구·검증 공백 · P1**. 코드 교체/제거 뒤 identity·미전송 업무·검토 증거를 잃으면 중복 효과 또는 복구 불가 위험이 있다.
- 근거: [운영 복구·설치](operations.md#recovery), [릴리스 계약](../../RELEASE_GATE_CONTRACT.md), [spool 보존 정책](../../DIRECT_SYNC_DATA_PLATFORM_NOTES.md). ACKED retention report/candidate는 자동 삭제 권한이 아니다.
- 상태/다음: **운영 준비 미입증**. Label 릴리스·운영 담당과 Main이 exact 후보의 install/첫 실행/cold boot/제거·재설치/rollback 근거를 연결하고 백업 범위·일관성·DPAPI 재사용 한계·보존 기간·RPO/RTO를 요구로 확정한다.
- 완료 기준: 데이터/CSV/current-state/공유 DB·lease/keyring/F5 journal/relay queue·spool·receipt와 identity가 수용된 절차로 보존·복원되고, 중앙 receipt 대조로 중복 없는 후속 작업이 확인된다. 실패·복원 불가와 손실 범위를 숨기지 않는다.
- 의존/병렬: LM-B04 후보 구성, LM-B02 운영 목표, 중앙 [Q07](../../../Program_Spec_Hub/BACKLOG.md#qualification). 정책·기존 증거 대조는 장비 실행과 독립 가능.

<a id="lm-b08"></a>
## LM-B08 · 호환 경로·이전 판 F5 복구·미조사 기능의 지원 범위

- 유형/우선: **지원 계약·조사 공백 · P1**. 코드 잔존을 현장 지원으로 오해하거나 대표 12기능을 전체 범위로 세면 안내가 어긋난다.
- 근거: [지원 경로](README.md#supported-workflows), [미조사 범위](README.md#specification-coverage), [C-07](contracts.md#c-07), [RELEASE_GATE_CONTRACT](../../RELEASE_GATE_CONTRACT.md).
- 상태/다음: **대표 기준선 완료 / 전수 확인 남음**. Label·운영 담당이 명시 legacy 5단계/EXACT_RESCAN, 저장된 F5 reconciliation의 복구 payload·capability, 관리자/수정·메뉴/단축키, product-host 잔존 mode, 상류 PHS2 발행→출고의 실제 지원 조건·제외를 확정한다.
- 완료 기준: 각 지원 분기에 진입점·설정/권한·설치 활성 여부·업무/예외·수용 기준 또는 미확인 사유가 연결된다. 미분류 중앙 입력을 호환 성공으로 바꾸지 않는다. ERPnext는 별도 범위다.
- 의존/병렬: LM-B04 실제 구성, 중앙 [지원 경로 지도](../../../Program_Spec_Hub/INTEGRATIONS.md). 추가 조사 범위는 담당이 정하며 이번 작업에서 넓은 소스 조사를 재시작하지 않았다.

<a id="lm-b09"></a>
## LM-B09 · 설정·프로필·업데이트의 오래된 안내 정합

- 유형/우선: **확인된 문서/현행 소스 차이 · P1**. packaged 설정이나 공통 Machine profile을 모든 설치의 정본 위치로 잘못 안내할 수 있다.
- 근거: [CODEX](../../CODEX.md)의 설정/GitHub 자동 업데이트 요약, [프로필 안내](../LOGISTICS_RUNTIME_PROFILE.md)의 공통 경로와 [현재 설정 선택](operations.md#configuration), [릴리스 사용자 상태 계약](../../RELEASE_GATE_CONTRACT.md). 앱 내부 updater apply는 LM-F01에서 제거했으며 후보·manifest 조회와 외부 installer 책임을 구별한다. 일반 fallback·onboarding·Machine 우선권도 구별해야 한다.
- 상태/다음: **운영 구성 표의 profile 선택 교정 / 나머지 오래된 안내 수정 남음**. w9setupprep R1은 상속 공용 경로·ProgramData 암묵 선택을 제거하고 first-run·client·도구 기본을 같은 사용자 resolver로 묶는다. J1은 물류 위치를 정하는 원 경로 정션을 해소 전에 거절하며 LOCALAPPDATA·앱 profile override·명시 data root가 없을 때 ProgramData 옛 data root로 물류 위치를 계산하지 않는다. TEST1은 새 앱 anchor도 거절한다. 일반 business 저장 정션·명시 도구 경로·legacy 환경은 유지하며 실제 설치본·서버 수용은 NOT TESTED. Label 문서·릴리스 담당은 나머지 CODEX/프로필 안내와 배포 topology를 대조한다.
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


## W9 슬림화 잔여 수용

- credential locator의 명시/기본 DPAPI owner는 2단 G와 동일 입력 전달로 교정한다(sub-268). 실제 native 설치·현재 사용자 자격 저장소·GUI의 독립 수용은 미실행이며 후속 범위다.
- canonical nested admission은 기존 제품의 읽기 전용 onboarding 판정을 audit/preimage·fence 앞에 호출하여 교정한다(sub-268 추가 P1). 실제 G를 켠 격리 child 회귀와 native 설치·UAC·옛 앱 복원 수용을 구분하며 rollback G는 유지한다.
- 옛 pre-G `-SourceRoot`의 version 없는 source는 설치 읽기 전용 API에서 bootstrap inventory와 정적 version/commit을 결속하여 허용 계약을 보존한다(sub-268, 1차 msg_f2a15b50ff42). 일반 release_identity와 기록·rollback G는 변경하지 않는다.

설정 저장 조건과 안내를 일치시키고 건별 복구의 원 기술 정보를 읽기 전용 상세에 보존했다. 소스·합성 headless 보장과 실제 화면 수용을 구별한다. 실제 랩에서 작업 중 설정 안내, 복구 항목 선택·스크롤·원 SHA/UNKNOWN 확인은 NOT TESTED다. 제품의 완전한 CSV/queue/identity/F5/receipt 동시점 백업·복원과 미전송분 재개의 공개 보장은 아직 없다; raw artifact 다운로드나 코드 rollback을 그 보장으로 승격하지 않는다.

## LM-StartupUpdate1 · 로그온 자동 코드 적용의 실기 수용

- 숨김 `startup_check.py --json`은 시작 판정 부품을 공유하는 읽기 전용 CLI다. 격리 입력의 파일 무변경·부작용 0 및 까닭 묶음은 headless 검증한다. HDD 실제 설치본 시간, 작업자 계정의 실행기 연결·결과 폴더 ACL·GUI 건강 표지는 화 낮 실물 검증 몫이다.
- 시작 경로 G는 기존 결론·판정 호출 횟수와 기준 소스를 대조하여 새 거부 0을 검증한다. 원자 결과 기록, 고정 안내, UNKNOWN·쓰기 실패의 계속 시작과 시험 전용 묶음 주입은 headless 검증 대상이다. 실제 Tk 화면·HDD 시간·알림과 관리자 SSH 연결은 설치 키트 및 현장 검증에서 확인한다.

- sub-458: 기존 설치기/진단 fixture 실패는 최종 동일 시험 본문의 clean 기준·후보에서 단언 AST·예외 형식·오류 코드가 일치한 경우만 상속으로 기록한다. fixture 정리는 컷 뒤이며 원 출력과 분류는 `D:/KMTech/program-improvement-20260912/work/program/w9scanonly/sub-458/INHERITED-FAILURES.md`에 보존한다.

D-16 S/E1–E9: READY 실패 뒤 writer 정상 부재와 실제 공통 SID G를 확인하고, 결정부터 내구 저장한 뒤 코드 포인터를 정착한다. G 불변·이전 판 ALLOW면 앞 판 포인터를 복원하고 ROLLBACK_PENDING_READY에서 실제 일반 relay의 역할별 READY를 확인한 뒤 ROLLED_BACK한다. G 진전은 후보를 유지하고 같은 bootstrap이 GUI 일반 실행을 시작한다. shared/owner marker와 업무 자료는 되돌리지 않는다.
SYSTEM 요청 실패·60초 안에 apply 시작 증거가 나타나지 않음 및 FAILED/UNKNOWN 뒤에도 정확한 active 판이 G ALLOW면 기존 일반 사용자 경로로 시작한다. 현재 사용자·보호 runtime·active SHA 및 같은 시작 이후 GUI/relay cohort를 검증한다. startup의 update mutex와 native 시작은 직렬이며, writer mutex를 이미 가진 sink는 update mutex를 다시 얻지 않고 같은 조건을 읽기 전용으로 확인한다.
늦은 SYSTEM이 기존 writer를 만나면 DEFERRED와 recovery_pending 및 원 phase·원 코드를 보존한다. writer 부재 뒤 같은 G decide로 한 번 정착하고 pending=false를 내구 저장한 다음에만 새 prepared를 적용한다. 다음 아이콘도 pending 회복을 요청한다. generation_before UNKNOWN에서는 trial을 시작하지 않으며 G 판정 불가는 기존 고정 지원 안내로 자료와 증거를 보존한다. 보고는 허용된 transition_code·retained_candidate만 사용한다. headless 실제 자식/서명 packet 회귀와 실제 GUI READY·SYSTEM task·랩 실물 수용은 구별한다.

- 예행 3회차의 tree 지표 오류는 루트 integrity record를 payload 지표에서만 제외하여 교정한다. 실제 교정 stock의 설치기 지표 함수와 추가 파일/하위 동명/1 B 변경 거절을 확인하고 예행 4회차 입력으로 전달한다. 공용 상위 ACL preimage 복원 지시는 철회됐으며 결정 B 일치와 실패 뒤 앱 부모 재설치 admission을 확인한다. LegacyCA·공용 Apps 및 모든 기존 하위의 guest 전후 ACL/옛 앱 재시작은 예행 4회차에서 확인해야 한다. frozen archive의 실제 staged producer→consumer 계약 연결도 같은 후보에서 검증한다.

- 설치 통합 최종 후보: ac745f0의 audit-index 전진 쌍과 74c02c2의 짧은 상승 전달을 합치고, G metadata가 없는 lifecycle/installer fixture와 staged verifier의 오래된 task keyword 거절을 교정한다. 기존 수락한 v2.0.94의 네 조상 전진을 정확 선언으로 보존해 총 14 조상을 결속하고 G 부정 delta의 보고 이유를 복원한다. 실제 판 기록은 stock builder 결과와 source/G 판을 대조한다. [sub-264 보고](D:/KMTech/program-improvement-20260912/work/program/w9setupprep/sub-264/REPORT.md)의 PROVEN/FAILED/UNPROVEN과 시험 비교를 수용 근거로 쓰며 독립 검토·w9next 병합·native 예행은 1차 소유다.

  staged verifier의 다른 호출 이름 거짓 통과(sub-332 P2)는 모듈 한정 정규화와 새 동적 호출·별칭·실행 문자열 거절로 교정한다. 기존 비리터럴 호출은 정확한 소유자·본문·횟수와 대상 대입/scriptblock 위임에 결속한다. 28셀 RED/GREEN·정확142·검사기 회귀 및 source-only 한계는 [sub-334 보고](D:/KMTech/program-improvement-20260912/work/program/w9setupprep/sub-334/REPORT.md)를 따른다. 독립 검토·병합·native 수용은 1차 소유다.

- sub-254 상승 launcher: 긴 script/payload를 private 감사 파일과 SHA256에 결속하고 상승 명령은 16,000자 이하의 bootstrap으로 제한한다. 259자 경로·20자 사용자, 한 바이트 변조/부재/reparse, 읽기 이후 쓰기·교체 거절, 기존 helper 결과와 UAC 취소 대역의 실제 rollback 회귀를 확인했다([동작·증명 경계](operations.md#placement-launcher-transport)). 실제 RunAs·새 packet의 공개 v2.0.38 업그레이드·P1/P2 독립 검토는 중앙 OPEN이며 배치 성공으로 승격하지 않는다.

- sub-153 SYSTEM 작업 XML: XML에서 허용하지 않는 ServiceAccount LogonType만 생략했다. Task1.4·SYSTEM SID·HighestAvailable·사용자 SID trigger·고정 action/조건과 preimage 복원은 유지한다. emitted principal의 정식 schema 유형·필수 업무 필드 회귀는 PS5.1·PS7 RED4→GREEN14이며 실제 랩 task 등록/되읽기·독립 확인·최종 선언/pin은 OPEN이다.

- sub-153 부재 관측: canonical 작업 조회 5자리의 오류 숨김, Run 관측 2자리의 넓은 부재 처리, 제거 후 FileNotFoundException 부재 되읽기와 선택적 설정의 디렉터리 건너뛰기를 교정했다. 정확한 API 부재만 인정하고 접근 거부·다른 오류는 실패한다. PS5.1·PS7 주입 회귀와 원 실패는 sub-153 REPORT에 보존한다. 실제 새 설치·복구·제거의 scheduler/registry 수용은 중앙 랩 OPEN이다.

- sub-150/153 strict leaf: 두 literal 상위 폴더를 bootstrap/검증된 daily의 기존 exact ACL 규칙으로 통일하고 직접 admission의 없는 폴더 생성·허용을 거절한다. standalone 제거도 helper를 로드하지 않는 경계 안에서 공유 순수 판정의 정확한 사본과 AST 대조 회귀를 사용한다. ordinary 준비의 기본 모양/없는 바탕 사전 읽기와 다른 leaf mask는 유지한다. 실제 NTFS 회귀와 기준 대비 집중 실패 집합은 sub-153 REPORT에 기록한다. 최종 선언/pin 재생성·독립 확인·빈 guest 새 설치/공개 v2.0.38 업그레이드는 중앙 OPEN이다.

- sub-141 상위 바탕 준비: 공유 두 폴더의 관측 기본→보호 ACL, SID 신뢰·ACE 집합 비교·전파 뒤 재읽기·중간 상태/멱등·사전 검사→상승 준비→앱 부모→신뢰 재검사·종료 코드 61–66을 옮겼다. D: 격리 실제 NTFS와 PS5.1/PS7 source 회귀를 REPORT에 기록한다. exact-after/companion/source/writer 선언은 재생성하지 않았으며 목록과 독립 검토·빈 W9R-lab-ready-01 새 packet 설치 1회는 중앙 OPEN이다. 이적의 증거 위치·메모리 wrapper·ACL reset 범위·실패 복구·재진입은 이 단위 밖이다.

- 10/02 sub-99: R1 native 일반 사용자 mutex 재열기, R2 전체 placement 함수 묶음, R3 실패→검증 복귀/UNKNOWN·claim, R5 세 feed gate, R6 실제 PS5.1 BOM decode를 좁은 회귀로 교정했다. (g) Python bootstrap PID 소비자는 실제 active relay readback으로 교정했고 canonical (g)(h)는 기존 defer/일반 시작을 유지한다. R4 유한 자식 진단은 교정했으나 상주 stream은 1차 결정으로 OPEN이다. 별도 relay bounded/masked startup log·설치기 exit/log 회수와 logger 초기화 전 crash 랩 증거가 필요하다. 핀 재생성·재검토·실제 Windows 수용은 중앙 잔여다.

- R4 유한 자식 helper/task/relay-cycle의 원 출력·exit와 준비/전이/ready-read/persistent-cycle 원 예외 사슬은 가림·상한을 두어 보존한다. 상주 자식 stream은 부모 종료 뒤에도 가림·상한·handle 수명을 유지할 logging/launcher 작업이 OPEN이다. 기존 실제 native/설치 수용을 이 headless 교정으로 완료 처리하지 않는다.

회사 feed 설정은 검증된 canonical 빌드 입력이며 설치 뒤 보호 SHA로 변조를 거절한다. app/main.py·패키지·실제 app/tools companion도 정확한 preimage/후보 선언에 포함한다. 원 아카이브 누락 실패 13건은 보존한다.

- 구현: 내구 상태기계·서명된 미래 exact 양방향 쌍·실제 기존 version 상승·READY·코드만 복귀, 고정 SYSTEM helper·작업 preimage, GUI/relay 토큰 경계, 정확한 공개 키 회전과 기존 release 핀을 추가했다. 소비자 원문 SHA는 전이 증거다. policy 미래 allowlist나 추가 승인 체계는 없다. 빈 feed·업무 중 적용 끔이 기본이다.
- PROVEN 범위: 독립 clone 집중 기존 비교, 새 순수/주입 경계, source-derived inventory와 공개 키 정확 쌍. 개수·버전·환경·원 실패·한계는 w9setupprep sub-88 REPORT.md에 기록한다.
- 10/03 sub-128 구현: [서명 쌍 계약](PAIR-RULE-CONTRACT.md)과 [지원 수명주기](LIFECYCLE-DESIGN.md)를 이식했다. 정상 A/B→C, control 비의존 제거·사용자 제거, 내용 증명 개별 이동·미확정/목록 보존 및 자료 경로 거절을 구현했다. headless 회귀 판/수치/원 실패는 sub-128 REPORT.md이며 실제 Windows 수용은 OPEN이다. CONFIRMED 후 임의 feed 하향은 지원하지 않는다.
- OPEN / NOT TESTED: 중앙 전체 suite·stock builder/실제 packet·독립 검토 및 로그온/빠른 시작/전원 손실/옛 판 등록/교체/제거/재설치/복귀 랩. 작업 ACL readback, 실제 0x18B 토큰의 모든 native 시작 자리, 실패 진단·Run 다음 로그온 복구, 데이터/미완료/outbox 보존을 확인한다. 실제 w9next packet matching-key·feed/manifest 게시·최종 병합은 중앙 담당이다.

- 10/02 w9setupprep sub-104: 통합 기준 `134bab3`와 이식판 `3104b0e`에서 같은 invalid-receipt restart 시험의 GUI 없는 `__new__` fixture가 실제 scanner notice의 Tk 속성으로 들어가 RecursionError를 냈다. notice만 수집하는 fixture로 고치고 OPERATOR_REVIEW·재전송 금지·다음 스캔 차단 단언을 유지하면서 안내 제목/내용도 확인한다. 제품/수량/멤버십/receipt 계약은 변경하지 않는다. 원 FAILED와 최종 focused/collect 근거는 `D:/KMTech/program-improvement-20260912/work/program/w9setupprep/sub-104/REPORT.md`에 보존하며 실제 GUI 수용은 NOT TESTED다.
- sub-104 충돌 해소 마감: legacy/scan/nextfix의 동일값 중복 선언 5개를 제거했다. 전후 모든 literal mapping 동일·BOM 유지이며 source/writer/companion pin 재생성은 이식 뒤 중앙 단계로 유보한다.

- sub-128/129 중앙 잔여: 동결 뒤 exact-after/추가 asset 선언, Python·PowerShell inventory/fence pin, app/tools companion 및 설치기·stock source digest를 같은 최종 입력으로 한 번 재생성한다. committed db1f114의 선언 대기는 43건(기존 40 + startup old_key/new_key/apply_helper 3)이다. 추가 3건은 lifecycle sink·canonical_placement 행이 빠진 startup_candidate_membership의 SOURCE_SET_DIFFERS가 예상 검사를 선행하여 발생한다. 원 실패를 보존하고 membership도 최종 source에서 재생성해 재검증한다. 독립 검토와 실제 설치/작업/로그온/제거/전원 단절/relay·credential·옛 첫 등록·업무 자료 불변 수용까지 Ready로 올리지 않는다. 중앙 hub 반영은 코디네이터 소유다.

- sub-130 N1: 건강한 A/A·A/B 및 다음 C에서 사용자 제거→재등록의 표식 검증을 고정 owner/actual active 문맥으로 교정했다. 정상 제거의 정확한 ID/SHA·부재 증거를 요구하고 credential handoff·conflict receipt의 권한은 유지한다. source headless 검증과 원 실패는 sub-130 REPORT.md이며 실제 Windows 설치·relay/Run·credential 수용은 OPEN이다.
- sub-130/136 N2: canonical 내장 URL gate의 ASCII host·빈 userinfo·port 검사와 PC 설정 gate를 맞췄다. PC의 scheme 추출도 대소문자 무관하게 처리하여 `HTTPS`·`hTtPs`의 빈 userinfo·비 ASCII 호스트를 거절한다. 정상 사내 주소·punycode·IPv4/IPv6 및 scheme/호스트 대소문자 변형 허용과 기존 호스팅 별칭 거절을 유지한다. runtime/내장/PC 함수 교차 회귀이며 실제 feed/설치 수용과 최종 선언 재생성은 OPEN이다.
- LM-U1/sub-143: PC gate가 runtime의 query 금지 목록과 이름 정규화를 사용하도록 맞췄다. 기존 세 경로 교차 회귀에 모든 금지 이름·두 접두사·percent/대소문자/하이픈/빈 값 및 정상 주소 사례를 추가했다. 독립 검토·최종 선언 재생성·실제 feed/설치는 OPEN이다.
- sub-130 N3: archive 후보 fixture는 staged/unstaged 변경이 있는 입력을 거절하며 candidate의 공개 설정·적용 helper도 해당 committed archive에서 읽는다. 검증 집계는 실제 실행 XML과 입력 SHA·clean 상태로 계산하고 RED/중간/재사용 증거를 최종 합계에 중복 가산하지 않는다. 최종 source SHA·판정·선언 대기 목록은 sub-130 REPORT.md에 기록한다.

- LM-T1: 예약 작업의 계정 표현을 SID로 결속하고 raw XML/기존 구조 거절을 유지한다. PS5.1/7 격리 함수 회귀와 원 실패는 sub-158 REPORT에 기록한다. 실제 등록·로그온·제거 수용과 최종 선언/pin은 중앙 후속 OPEN이다.

- sub-158 소유자 규칙: 특권 SID 셋 순수 함수를 설치/준비/daily/적용/제거 경계에 통일하고 D NTFS 회귀와 경계 사본 동등성을 확인한다. 실제 SYSTEM/TrustedInstaller 소유권 설정·랩 설치와 최종 pin 수용은 OPEN이다.

- LM-O1: canonical Snapshot과 현재 사용자 relay 관측의 key 없음 판단을 native RegOpenKeyExW 코드 2로 한정하고 나머지 오류/race 실패를 보존한다. PS5.1/7 격리 회귀는 sub-158 REPORT; 실제 registry 오류·Run/설치 수용과 최종 선언/pin은 OPEN이다.

- sub-158 조건부 교정: sub-155가 확인한 db1f114의 canonical_placement source 중복을 정식 가지에서 하나의 실제 placement/removal admission으로 모았다. PS5.1/7 위임·timeout/해제와 duplicate 거절 회귀를 보존한다. 선언/pin/최종 packet 및 실제 lifecycle 수용은 OPEN이다.


- sub-177: 고정 helper 원 bytes hash 유지 + UTF-8 디코드 선두 BOM 하나 제거를 canonical 다섯 실행 자리에 적용하고, preimage 뒤·writer 거래 전 실패의 해당 run 진단/임시 상태를 보존 archive하여 새 설치 판정을 안전하게 이어간다. 기존 receipt·등록·data residue 거절 및 거래 안 rollback은 유지한다. [동작·회복 경계](operations.md#verified-helper-text-pretransaction)와 D:/KMTech/program-improvement-20260912/work/program/w9setupprep/sub-177/REPORT.md에 실제 회귀/예행/실패 한계를 기록한다. 정식 선언/pin·독립 검토·현장 수용은 중앙 후속 OPEN이다.


<a id="w9setupprep-sub192-child"></a>

- 배치 자식 실패 보존과 선택값: source 회귀 PROVEN. 정식 선언/pin 재생성 0; 예행 packet·native 수용·독립 검토는 [sub-192 보고](D:/KMTech/program-improvement-20260912/work/program/w9setupprep/sub-192/REPORT.md)의 실제 결과와 남은 한계를 따른다.


<a id="w9setupprep-sub192-archive"></a>

- 거래 전 실패의 다른 실행 보존: source 회귀 PROVEN. 정식 선언/pin 재생성 0; 예행 packet·native 수용·독립 검토는 [sub-192 보고](D:/KMTech/program-improvement-20260912/work/program/w9setupprep/sub-192/REPORT.md)의 실제 결과와 남은 한계를 따른다.


<a id="w9setupprep-sub192-removal"></a>

- 실패한 설치 자신의 fence와 지원 사용자 제거: source 회귀 PROVEN. 정식 선언/pin 재생성 0; 예행 packet·native 수용·독립 검토는 [sub-192 보고](D:/KMTech/program-improvement-20260912/work/program/w9setupprep/sub-192/REPORT.md)의 실제 결과와 남은 한계를 따른다.


<a id="w9setupprep-sub192-rollback"></a>

- 되돌리기 독립 단계와 정확한 끝 상태: source 회귀 PROVEN. 정식 선언/pin 재생성 0; 예행 packet·native 수용·독립 검토는 [sub-192 보고](D:/KMTech/program-improvement-20260912/work/program/w9setupprep/sub-192/REPORT.md)의 실제 결과와 남은 한계를 따른다.

- 10/04 sub-192 추가/sub-197 P2-1: FAILED_SUPPORTED_REMOVAL_REQUIRED의 정확한 자기 실패도 지원 제거 상태로 수용한다. 종료/살아 있는 소유자와 비실패 상태 회귀 RED→GREEN; guest ROLLBACK_FAILED의 제거 성공과 구별한다. 소유자 신뢰 경계 P2-2는 별도 교정하며 선언/pin 재생성은 0이다.

- 10/04 sub-192 추가/sub-197 P2-2: 실패 fence의 지원 제거를 기존 protected code-parent 소유자 기록의 run/PID/native 생성 FILETIME과 정확한 fence tuple에 결속했다. mutable audit 단독·상충/live/unknown·중복은 전체 거절하고 활성 writer/authority를 유지한다. 위조/중복 RED→GREEN, PS5.1/7 NTFS immutable 발행 및 native PID 회귀; positive privileged ACL/실제 guest 발행은 NOT TESTED다. 추가 교정은 정식 가지에만 두며 선언/pin/stock 재생성 0이다. 고정 예행 packet guest의 최초 InnerText 실패와 rollback 실패는 원 출력으로 보존하고 native 설치 수용은 OPEN이다.

- 10/04 sub-192 native 원인 (가): 실제 startup task XML comparer의 빠진 선택 노드/기본 true export 형태를 PS5.1/7 공통 CLR node/count 비교로 교정했다. 원문·엄격한 ownership 비교 유지, RED 16 FAIL·8 PASS → GREEN 24 PASS. 선언/pin/추가 guest는 0이며 고친 packet의 native 설치 증명은 동결 후 OPEN이다.

- 10/04 sub-192 native 원인 (나): absent-code rollback의 부모 admission/자식 admission 자기 경합을 정확한 기존 session authority/token 위임으로 교정했다. 부모가 원 authority를 유지하며 검증 후 admission을 넘기고 자식은 기존 admission·HeldByOther 검사 뒤만 제거한다. PS5.1/7 native mutex RED 2 FAIL → GREEN 14 PASS, 잘못된 위임·busy child 거절과 실패/fence 보존. 선언/pin/추가 guest 0; 고친 packet native 수용 OPEN이다.

- 설치 통합 F1 회귀: 비동기 CSV writer 완료 전 읽기의 0행 실패를 재현하여 fixture에 log_queue.join 경계만 추가. 기존 취소 1행·새 세트·capture 보존 단언 유지, 제품 변경0. 최종 실행 근거는 sub-264 REPORT.

- 설치 통합 canonical fixture: 실제 relay-exit 함수·startup 의존 코드 보충, 현재 rollback exact locator, 예상 밖 writer 거절 음성 추가. 제품 변경0·기존 단언 불변; sub-264 REPORT.

- rollback static 상태는 현재 exact legacy/ordinary 조건문을 확인하며 기존 ROLLBACK_FAILED 의미를 보존한다. 각 branch를 성공으로 바꾼 입력은 거절; msg_b0774576a440의 exact static 계약 교정 범위.

- 2026-10-05 조립: 실패 가림 식별자 예외와 제품 상태 writer 분리 완료. 계약·writer 표 및 회귀 범위는 assembly_failure_contract.md 참조; 최종 조립 stock 수용은 sub-292 REPORT로 확인.

- 2026-10-06 공식 옛 LM VBS: v2.0.38 writer의 CR CR LF 전체 본문을 launcher admission의 정확한 대안으로 인정. 보존 VBS 472바이트 완전 재생성 및 PS5.1/7 RED 2 FAIL 확인; source ownership :2929 귀속은 원 native의 principal/command 결합 문구와 달라 폐기. 최종 조립·stock 근거는 sub-311 REPORT, 실제 relay-01 설치/rollback은 NOT TESTED이며 coordinator guest 읽기로 native 단계 귀속을 별도 확인한다.

# 설치 공개 실패 기록 수렴 (sub-310, 2026-10-06)

- 2026-10-07 sub-409: 독립 production 제거의 helper 미적재 P1 hash·P2 진단 네 호출을 기존 hash 정의 재사용과 packet SHA 확인 후 redaction load로 교정한다. 새 PS5.1/PS7 실제 진입점의 격리 adapter RED/GREEN·같은 부류 전수·정확142/관련538·stock 근거는 sub-409 REPORT에 남기며 실제 예약 작업·레지스트리·상승·VM 제거 수용은 별도다.

컷 전 범위는 설치기·업데이트 적용기·지원 도구의 가변 진단 출력이며 업무 로그/스캔 화면은 제외한다. 허용 필드/고정 문구 표와 같은 시도 보호 보고서의 경로·UTF-8 SHA 결속, 게시 실패/옛 보고서 회귀를 추가한다. 고정 안내·성공 출력 및 지원 제거의 거래·복구 metadata를 유지한다. 전수 자리 분류·RED→GREEN·접두/긴 assignment 병합·선언 재생성·focused/조립142의 실제 판정과 남은 native/GUI 한계는 sub-310 REPORT에 기록하며 전체 준비 완료로 간주하지 않는다. 전면 공개 형식 통일은 컷 뒤 범위다.

- 2026-10-06 sub-310: 계약 0e에 따라 공개 출력 가변 진단만 교정하고 고정 문구·typed 상태·복구 metadata 유지. sub-308 99c9298 merge 완료; 최종 재생성/회귀 기록은 sub-310 REPORT. stock 빌드와 독립 검토는 코디 msg_89d701df2105에 따라 306+308+310+311 통합 조립에서 한 번 수행.

- 04:0x 범위 재분류: 직접 자리 (가) 26→21, (나) 115, (다) 0. 내부 경로·종류·고정 옵션/코드 출력 6곳은 80a93ab 줄로 복원하고 누락 예외 출력 1곳을 (가)로 정정했다. 얽힌 부분 복구 안내 1곳은 (가) 유지 사유를 INVENTORY에 기록한다. 이 복원 뒤 재생성은 수행하지 않으며 최종 재생성·142 회귀는 통합 sub-315/v2가 소유한다.

- 2026-10-06 sub-315 v2: 실제 5aa2c03 stock의 PS5/PS7 정상 PlanOnly 2 PASS와 pin 한 바이트 변조의 null-array 종료 2건으로 초기 trap의 helper 미로드 P2를 확인했다. 최상위 trap의 helper 존재 조건만 교정해 초기 고정 실패 코드와 REPORT_UNAVAILABLE을 유지하며 pin/G/거절 순서는 바꾸지 않는다. 공개 자유 문장에 의존하던 테스트 3함수는 같은 시도 보호 보고서 SHA와 기존 거절·rollback 순서·자료 무변경 단언으로 교정한다. 판정용 stock과 실패는 보존하고 승인된 최종 재생성·142/영향 focused·공개 셀·최종 stock 근거 및 native/독립 검토 한계는 sub-315 REPORT로 확인한다.

- 2026-10-06 sub-315: PS7 placement child에서 `File.SetAccessControl` API 부재로 보호 trace가 0바이트이며 최초 원인 참조가 사라지는 P2를 실측했다. 같은 ACL의 Set-Acl 적용만 최소 교정하며 phase/exit·가림·CreateNew·flush·SHA·거절 및 G는 유지한다. 원 실패와 PS5.1/PS7 교정 검증·재생성·공개 셀·142·최종 stock의 실제 범위는 sub-315 REPORT에 남긴다. helper 전의 잘못된 서버 URL은 계약 0d의 정확한 FAILED/INSTALLER_FAILED/REPORT_UNAVAILABLE fallback을 유지하며 더 구체적인 reason code는 컷 뒤 P3이다.

- 2026-10-06 sub-353: 코드 배치 실패 복구의 StrictMode 미초기화 phase stack과 지원 재개의 replacement 플래그 누락을 교정한다. 실제 생성 launcher·transport 거절·진단 없는 native 종료는 원인 코드와 보호 기록을 남긴다. RED/GREEN·정확142·관련538·새 stock·VM-02의 실제 판정과 한계는 sub-353 REPORT에 기록하며 독립 검토와 회차 A-포장은 1차가 맡는다.

- sub-353 stock 검사: delegated trap 5줄 추가로 이동한 기존 startup task 호출 두 곳의 고정 위치만 820/952로 갱신한다. 기존 함수·명령·mutation/delegation 허용 계약은 그대로 검사하며 최초 거절 stock과 검사 실패를 보존한다.

### D-16 E1 · apply 시작 증거(23:0x 정정)

60초는 SYSTEM apply 시작 claim이 없는 상한이다. Authority 뒤 보호 apply-claim에 PID·native creation identity를 기록한다. LIVE/UNKNOWN 소유자는 기존240초 창에서 기다리며 느린90초 apply 동안 일반 실행을 시작하지 않는다. 정상 종료는 기존 guarded sink로 claim을 SETTLED 기록해 증거를 보존한다. 시험을 본 같은 bootstrap도 FAILED/UNKNOWN에서 claim이 NONE/ABSENT이고 일반 writer가 없으며 active G ALLOW·prepared 부재이면 현재 판을 일반 실행한다. 240초 뒤에도 UNKNOWN이면 update_lock 안에서 claim과 수정 시각을 다시 확인한다. 수정된 지 15분을 넘긴 claim만 같은 writer·G·prepared 조건으로 `UPDATE_SUPERVISION_UNAVAILABLE` 일반 실행을 허용하며 같은 claim hash의 지원 코드는 한 번 기록한다. LIVE·신선한 UNKNOWN·시각 읽기 실패·G 거절은 진행 안내+20을 유지한다. 이 복귀는 포인터·시험·READY·CONFIRMED를 쓰지 않는다.

사망한 SYSTEM의 같은 claim은 같은 세션에서 재요청하지 않고 현재 G 허용 판을 일반 실행한다. 지원 코드는 같은 protected claim hash에 대해 한 번 게시하며 다음 로그온의 기존 SYSTEM 작업이 새 claim을 덮어써 재시도한다(1차 결정 msg_c93ef274762a).

B1–B5 headless 실제 자식/반대 guard 회귀는 sub-470 RESULT에 결속한다. 실제 pythonw 로그온·GUI·SYSTEM 예약 작업 수용은 랩 NOT VERIFIED다.


### Bootstrap relay 시작의 일시 admission 대기

자동 시작/update 경합(sub-126): headless 실제 다른 프로세스의 시험 전용 native mutex 6초·30초 대기와 한도 뒤 실제 현재 판 자식, 손상 prepared 두 번째 상태 불변, onboarding 관측 회귀를 검증한다. 기존 실패와의 정확한 비교·생성 선언/핀 결과는 sub-126 RESULT에 결속한다. 실제 SYSTEM↔사용자·pythonw 로그온·GUI·stock/서명 패킷 수용은 랩 NOT TESTED다.

Run 및 GUI가 만든 bootstrap의 relay 실행은 `WriterFencedError` 중 `WRITER_GATE_TIMEOUT`만 기존 240초 창에서 0.1초씩 쉬며 재시도한다. 각 시도는 기존 launch_version의 admission을 다시 얻으며 시도 사이에는 admission을 보유하지 않는다. 상한은 240초 + 마지막 admission 대기 최대 5초이며 마감 뒤에는 기존 실패 출구를 유지한다. fence 확정·버려진 소유권 등 다른 거절과 GUI 실행 자체의 timeout은 재시도하지 않는다. 포인터·trial claim·READY·CONFIRMED·G 허가·cohort·잠금 순서는 유지한다. bootstrap 밖의 relay launcher admission과 실제 로그온/pythonw 수용은 별도 범위다.

- P2(코디 msg_c06cdb448ab5·msg_3efe0acd3297 범위 제외): 예약 작업 퇴역 native helper의 ABSENT 길을 소유 합성 작업 이름 둘로 대응해 측정하니 PowerShell 시작 포함 writer admission 8.890초(5초 초과)였다. 호스트 scheduler 변경 0; 기존 제품 작업 존재 시 거절 길 2.047초. 실제 제품 이름 ABSENT 길은 NOT VERIFIED(랩 첫 GUI 재실행). 다음 수정에서 helper/퇴역 대기를 admission 밖으로 나누고 쓰기 직전 재확인한다.

- 첫 업데이트: 거절 뒤 `wal_checkpoint(TRUNCATE)`가 commit 된 WAL을 주 DB로 옮김 — 바이트 불변은 읽기 전용 admission으로 구조 나눔(sub-90 P2-2). 107은 거절 길의 논리 내용 보존까지다.
# G33 · isolated portable entry imports / gate 7

Move startup_timing after auto_update's isolated-entry path setup. Complete-app regression and built ZIP runtime checks cover task JSON, relay argument refusal and prepared-update root refusal; apply the same gate 7 to both apps. No version, installed layout or business-data contract changes.

- w9firstupdsub269 G41/G45 P1: 다섯 relay fallback과 증명 스냅샷 재사용을 고치고, 지정 복구 상태의 단일 일반 relay에서 검증된 생성 경계로 GUI 하나를 연다. 기존 GUI·SYSTEM writer 보호·trial/READY 권한은 유지한다. 실제 현장 수용은 1차 회차에서 확인한다.

- w9firstupdsub277 G46 P1 닫음: Observe에서 기존 판정이 참인 고정 GUI·relay bootstrap 행을 역할·소유자 조회 전에 제외해 되돌림 READY의 실제 relay 하나만 센다. READY·census 코드는 유지하며 실제 relay 둘의 확인 거절과 G41 동시 요청 재사용을 격리 회귀로 확인한다.

- G45 / LM113 L1: ROLLBACK_PENDING + claim NONE/ABSENT에서 검증된 앞 판 active·일반 relay 하나·정착 phase와 같은 세대 ALLOW 증명으로 일반 GUI를 허용한다. 상태 쓰기·trial/READY 권한은 없고 writer 부재 뒤 SYSTEM 정착은 그사이 쓴 자료·원장을 보존한다. 실제 현장 수용은 별도 회차다.
- D-05 포장: 켤 때 지난 이름을 이번 실행의 작업자로 쓰지 않음(작업자 미정·고르기 판 자동 열림·콤보 빈 값). 설정 파일 값은 보존하고 「지난번」 표시만. 복구된 미완 세트가 있어도 고르기 판이 열리고, 활성 세트 중 설정 변경 막음은 「미정에서 처음 고르기」 한 번만 푼다. 세트 기록 주인은 세트 스냅샷, 세트 밖 기록은 고른 사람(고르기 전엔 「작업자 미정」 표시).
- 포장 보류 스캔(완료 저장 뒤로 미룬 스캔): lane 종단 장애·격리 성공 두 길 모두 「받지 않았습니다 — 관리자 확인 뒤 다시 스캔하세요」 거절·fail 소리 뒤 목록을 비움(조용한 소실 0). 종단 안내문과 격리 보류 안내에 건수를 덧붙임. 격리 스냅샷 held_scans 기록은 유지(재처리 0).
- G88(machine metadata 비밀 거절): 계약 #31 다음 판 본 판을 들였다. 선택 판(JSON 낱말 `true/false/null`·숫자 낱말 경계)은 보류이며 흔한 4자 값은 거절한다. 남은 일: Python 코드포인트와 PowerShell UTF-16 길이 판정이 BMP 밖 문자에서 갈리는 경계는 계약 개정 때 한 기준으로 맞춘다(지금은 시험으로 현재 동작을 고정).
- G91 막이(포장): 앱 전용 프로필 env 가 옛 공용 ProgramData 프로필·secrets 를 가리키면 무시. 남은: 실제 업무 PC env 값은 얼림 전에 읽지 못해 현장 확인은 NOT TESTED.
- w9firstupd / G99 포장: 설치기 프로세스가 통째로 죽어 울타리만 남고 감사가 끝나지 않은 PC(공식 입구 넷 거절)를 푸는 입구를 더했다. 지원 `--remove-current-user-setup`이 죽음(보호 주인 기록·프로세스 종료)과 쓰는 프로세스 0·relay 0을 증명하면 실패 상태 때와 같은 좁은 출처 넷만 받고, 통과 뒤 그 울타리 하나를 `settled/`에 정확한 사본으로 두고 지운다. 언제 열리고 무엇만 받는지는 [운영](operations.md#설치기가-통째로-죽은-뒤의-지원-제거-입구-g99). 현장 안내: 같은 작업자 일반 세션에서 `--remove-current-user-setup` → 공식 다시 설치. 남은 일: 주인 기록을 쓰기 전에 죽거나 `update-control`이 없는 첫 설치 중단, 되돌리기 중(`RESTORING`) 죽음은 이 입구 밖이다. 실제 설치기 강제 종료 뒤 현장 PC 확인은 NOT VERIFIED.

- relay 준비 대기 상한 600초(포장 117): 찬 첫 읽기(설치 코드 트리 전체 무결성 확인) 실제 소요 시간 실측은 VM 10/21 몫. 실측 뒤 상한 재조정 여부 판단.

- w9firstupd / 포장 118: G100b — 첫 설정·relay 시작 사슬의 relay 준비 대기를 한 값 `auto_update.RELAY_PREPARE_SECONDS`(600 s)로 모았다(bootstrap :2891·두 복구 자리 75 → 600, `user_relay` bootstrap_readiness_seconds 600, 설치기 `AddSeconds(600)`은 정적 시험으로 같음을 묶음). 이미 통과한 트리는 같은 프로세스에서 다시 읽지 않는다(메타데이터 같으면 캐시, 다르면 바이트 비교). 남은: 메타데이터(크기·mtime)가 같게 바뀐 파일은 같은 프로세스 안에서 두 번째 확인이 못 본다(첫 확인 동안의 바뀜 거절은 그대로) — 「바이트를 안 읽음」을 `test_known_limit_second_check_does_not_read_bytes_of_a_same_size_same_mtime_change` 가 보이고, 보장 하향 유지는 관리자 승인 대기(operations.md 「한 번 읽기의 한계」). 상한 표는 operations.md 「고정 상한 판정표」로 닫음(600000 Wait-LabelRegistrationChild 만 묶고 720000·90000·120000·3 s·CLOSING_WAIT 120 은 다른 뜻 — 손 0). VM 10/21 실측.
- w9firstupdsub431 C1: prepared 후보 없는 정상 DEFERRED·CONFIRMED의 실제 `_bootstrap()`에서 relay가 300초 뒤 준비되면 시작 업데이트용 240초 deadline 때문에 실패하던 경계를 RED 2건으로 재현했다. START_CURRENT의 deadline 전달 두 자리만 빼 기존 RELAY_PREPARE_SECONDS(600초)를 사용하고, 기존 상한 정적 시험에 이 전달 경계 단언을 더했다. 같은 좁은 확인은 56 통과·새 실패 0이며, 즉시 준비·prepared 후보 경로와 보호 소유 증명 검사는 유지한다. 실제 첫 설정 실패 원인 확정·독립 검토·현장 재회차는 1차 레인에 인계한다.
- w9firstupd / 포장 118: G99c — 설치기가 울타리 전에 감사에 튜플을 저장하고, 입구는 튜플 없는 감사도 보호 주인 기록으로 받는다. 118 검토 고침: 주인 기록을 울타리 앞에 쓴다(배치 도우미 `-WriteInstallerOwnerRecordOnly`, 두 길) — ② 갇힘 0, 울타리 없는 고아 주인 기록은 무해. 기계 길도 튜플·문맥 표지를 울타리 앞에 저장해 ⑤(RESTORING)가 정리로 풀린다(이어 되돌리기는 기계 길에 답하지 않음). 남은: 사용자 길은 승인 창이 한 번 더 뜰 수 있고 실제 UAC·도우미 기동은 시험 PC 몫. 코드가 반쯤 복사된 PC의 lifecycle 자식(`--label-match-lifecycle-context`) 실패는 울타리가 아니라 코드 상태 때문이라 이어 되돌리기로만 풀린다 — VM 확인 몫.
- w9firstupd / 포장 10/28 판(관리자 10/20 18:0x ARM_SHORTCUT 판정): 기계 길로 깐 PC 의 **작업자 준비 단계**에서 그 작업자의 바탕화면·시작 메뉴에 `KMTech 포장.lnk`(current/runtime/pythonw.exe -I -B current/app/main.py)를 만들고 감사에 published 로 남기며 제거가 둘 다 치운다(이적 INSTALL_CANONICAL_PORTABLE.ps1 :4044–4047·:4249–4264 본보기). 사람이 만든 다른 링크는 손 0. RED = 작업자 준비 뒤 두 링크가 current 를 가리킴 / 제거 뒤 둘 다 없음. 118 은 공용 시작 메뉴만(키트는 sub-611 로 감사의 공용 시작 메뉴 링크를 받음).

- w9firstupdsub442 / 포장 G104: 끝·실패 RR/RF/FRI 감사와 자기 잔류 울타리의 관리자 출구를 기존 `-ResumeRollbackAuditPath`에 추가했다. 같은 소유 튜플·보호 owner·PID/FILETIME 종료·관리자 CIM 자식/relay 부재·현재 전체 inventory와 알려진 산출물을 증명한 뒤 울타리만 정리한다. 원 감사/상태는 보존하고 고정 `orphan-fence-release-<attempt_id>.json`의 RELEASING→RELEASED로 중단 재시도·멱등을 확인한다. 작업자 입구는 유지한다. 부분 상태·모순·관측 불가와 다른 결속은 거절하며, 실제 현장 PC/VM 수용·통합 빌더 관문은 레인 회차 몫이다([운영](operations.md#끝실패-감사의-자기-잔류-울타리-지원-정리-g104)).
- w9firstupdsub469 / REVIEW-119-LM-R4-2 P1-2: 정상 문서 없는 첫 회전의 진행/실패 표지와 새 단독 재결속 요구 보고서는 retained=false·이전 로컬 세대 없음(null)·endpoint·결정적 신원/manifest·기존 키 지문을 남긴다. 저장 실패·응답 전 종료·진단 쓰기 실패·진행 표지 쓰기 실패 뒤 다음 공식 시작은 같은 기존 키로 한 번 재결속해 READY로 진행한다. 부분 저장 파일도 같은 결속일 때만 허용하고, 다른 키·신원·키 열기 실패는 HTTP 0회로 거절한다. 키 지문·manifest가 없는 R3 옛 단독 보고서는 결정적 신원 셋이 정확히 맞을 때 한 번 서버의 키 판정을 받으며, 다른 키의 identity conflict는 기존 관리자 회복을 유지한다. 새 키·신원 삭제·실행 내 반복은 없고 설치 서버 origin 검사도 유지한다.
