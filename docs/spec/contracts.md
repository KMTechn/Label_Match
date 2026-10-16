# Label_Match 데이터·통합 계약

기준일 화면 계약(w9firstupdsub208): 최근 결과 sub와 today.rows[].raw는 축약 Tk 값 대신 원문, work.exchange는 remove/insert/done과 remove.code·insert.code를 보낸다. today.summary[].count는 트레이 수이고 optional products는 완료 metadata의 전체 제품 수 합계다. 근거가 빠지면 생략하며 today.rows[].products의 표본 수/취소 자격 뜻을 유지한다. M124(10/15 18:03 확정): SET_HOLD·F5_HOLD·SUPPORT_HANDOFF·PHS_LOOKUP 네 동작은 작업자가 PIN 없이 한다. 일반 RECHECK와 격리 파일 복원은 섞인 시트/처리기이므로 기준일에는 관리자 PIN을 유지한다. L04 작업자 재조회와 L05 관리자 복원을 다른 시트·action으로 나누는 일은 첫 업데이트에서 w9mgrui가 설계한다. sheets.*.pin은 메뉴 묶음·표시이며 인증 방식을 새로 정하지 않는다. admin·cancel_completed는 기존 모양, 완료 취소는 kind:form과 P7의 기존 승인을 유지한다.

등록 도구의 기존 신원 거절은 첫 설정의 `RECOVERY_REQUIRED`와 원 `recovery_action`으로 보존한다. 공개 고정 원인은 `ADMIN_RECOVERY_ACTION` 또는 `POSSESSION_PROOF_REATTACH_REQUIRED`이며 등록 보고만 남은 재시작과 시작 거절 관측에서도 동일하다. 107은 길 A 없이 두 원인 모두 첫 설정의 고정 안내·작은 코드 줄·관리자용 자세히를 사용한다. 원 신원·자격·자료·대기열을 바꾸거나 다시 enroll하거나 Run/업무 자료 준비로 진행하지 않는다([첫 설정 안내](operations.md#onboarding-registration-recovery)).

표지 PID의 실제 실행 파일은 두 번째 프로세스 자신의 실행 파일을 기준으로 대조해 다른 실행 파일을 거절한다. 다만 같은 실행 파일의 다른 프로세스와 자료 뿌리 mutex 소유자의 결속까지 증명하지 않으므로 같은 사용자 권한의 표지 위변조를 완전히 막는 것은 아니다.

자료 뿌리 범위의 한 개만 잠금은 소스·portable 입구에서 무거운 import·시작 준비·셸보다 먼저 잡는다. PID 표지는 같은 뿌리의 식별자를 이름으로 삼고 LOCALAPPDATA의 사용자 폴더에만 둔다. 표지 PID가 살아 있고 실행 파일 절대 경로가 같을 때만 그 PID의 보이는 최상위 창을 찾는다(제목 검색 없음). 최소화 창은 복원하고 foreground 거절 때 작업 표시줄을 깜박인다. 창이 있으면 안내 없이 rc0으로 끝난다. 창이 없으면 「포장」 제목·확인 단추 하나의 켜지는 중/닫는 중 안내를 5초 뒤 닫고 rc0으로 끝난다. 표지는 기존 writer sink 안에서 쓰고 DELETE_ON_CLOSE 핸들로 소유하므로 종료 admission을 닫은 뒤 새 쓰기 없이 정리한다. 운영 자식·relay·업데이트 helper는 기존 별도 진입을 유지한다.

포장 PC의 새 화면·비상 Tk 기록 판은 오늘 기록만 다시 불러온다. `history`는 빈 입력만 받으며 날짜가 있는 요청은 `HISTORY_TODAY_ONLY`로 거절한다. 비상 Tk는 날짜 단추·달력을 만들지 않으며 같은 오늘 판정 함수가 다른 날짜의 loader 호출을 상태 변경 전에 거절한다. 두 화면은 `label_history.HISTORY_LIMIT`의 최근 100줄을 보이고 기록 파일·전체 집계·중복·취소 색인은 보존한다. 기록 칸 아래에 「지난 기록은 회사 웹 리포트에서 봅니다」를 안내한다. `scan_cancel({rows:[today.rows[].row]})`은 오늘 보이는 완료 통과·불량 줄 1–100개를 기존 선택 삭제와 pointer 승인 callback으로 보낸다. 진행 중 줄은 `SCAN_CANCEL_IN_PROGRESS`로 거절한다. 승인 대상의 행 ID·상세·표 값 snapshot을 다시 대조하며 새 삭제 업무 경로를 만들지 않는다. 삭제한 줄은 목록과 기존 기록 재조회에서 빠진다. 승인 응답은 실제 삭제 수와 saved 「기록을 취소했습니다」 또는 SCAN_CANCEL_*·warn을 돌려주며, 확인 대기만 result를 생략한다. 셸 응답에는 최신 state가 항상 있다. 남은 줄의 status/products는 실제 상세, cancellation은 실제 outbox에서 계산하며 text에는 사실만 담는다.

v2.0.104 후보는 인수 없이 새 화면(셸 v3.16.2)을 엽니다. 비상 `--ui tk`·`--old-screen`은 셸을 가져오지 않습니다. `--new-screen`·`--ui web`과 모르는 `--ui` 값은 기본 새 화면을 고릅니다. 셸 가져오기·자료 폴더·WebView2 준비 실패는 같은 시작 임대 안에서 Tk로 이어집니다. 처리 층을 만든 뒤 시작 실패는 셸이 쓰기 문·창을 닫고 같은 판 표지를 기록하며 writer/lane 종료를 기다려 끝냅니다. 다음 실행은 `web_allowed`가 같은 판 표지를 보고 Tk를 고릅니다. 자동 재실행과 PID 기다림은 없습니다. 한 개만 판정은 입구에서 먼저 수행하며, 기존 시작 확인·업데이트·설치·자료 거절의 상대 순서는 유지합니다.

조립 후보 v2.0.104는 앱·자료 판 번호를 함께 올리며 자료 꼴은 유지한다. v2.0.99·v2.0.100·v2.0.101·v2.0.102·v2.0.103 자료는 열고 v2.0.104 자료의 낮은 판 되돌림은 거절한다. 같은 판의 다른 미검증 build는 자료 판 불명으로 거절하므로 자료가 남는 PC에 설치하는 다음 build는 자료 판보다 엄격히 높은 판을 사용한다. 별도 서명 호환 증명 없이 기본 자료 호환 하한은 자기 판 v2.0.104이며 자료 꼴·서명·검증은 바꾸지 않는다. 새 화면 셸 v3.16.2/READY와 기존 검사 런타임의 정확한 native 6개를 바이트로 고정한다. 두 설치기의 unsigned 허용은 없음(옛 판) 또는 정확한 3경로·해시이며, 빌더·설치기·패킷 핀이 다르면 빌드 실패다.

READY 판정과 살아 있는 relay 재사용은 설치기가 남긴 `update-control/worker-pending.json`의 명시적 `server_base_url`과 등록에 사용한 relay credential의 `endpoint_url`을 로컬에서 대조한다. 완료된 기계 표지도 사용하며 scheme·host·유효 port만 비교한다(대소문자·끝 슬래시·기본 port는 동등). 둘 다 판독 가능하고 다르면 `RECOVERY_REQUIRED`·`INSTALL_SERVER_ORIGIN_CHANGED`로 등록·relay 시작/재사용 전에 거절하고 로컬 자료·대기열을 보존한다. 환경 override와 서버가 반환한 logistics profile 주소는 비교하지 않는다. 주소가 없거나 판독 불가능한 옛 기록은 이 비교를 건너뛰며 기존 검증을 유지한다. 네트워크·새 대기·writer admission은 추가하지 않는다.

portable direct-sync relay 자식은 기존 시작 관문을 지난 뒤 `label_match_product_host.dispatch_product_mode`를 호출하며 `Label_Match` 앱 모듈을 적재하지 않는다. host의 `startup_gate(role="relay")`·업데이트 trial/판 확인·자료 세대·credential 준비·writer sink·인수·종료/오류 계약과 별도 자식 프로세스는 유지한다. 일반 GUI와 다른 제품 인수의 진입 경로는 그대로다.

<a id="new-screen-shell-write-guard"></a>
107 조립의 어댑터는 셸 v3.16.3의 공개 `web_allowed → run_app` 순서로 시작한다. 어댑터에서 backend 초기화·renderer·Core import·runtime 판 문자열을 먼저 검사해 Tk를 고르지 않는다. 셸이 실제 시작 실패의 원인·예외 종류/HResult와 재시도 횟수를 기록한다. 첫 일반 시작 실패는 안내와 count 1로 끝나며 다음 실행은 새 화면을 다시 시도한다. 연속 둘째 실패는 count 2와 fallback 표지를 남겨 다음 실행에서 Tk를 고른다. runtime 부재의 별도 셸 판정도 그대로 따른다.

공개 셸 `run_app`의 `web` 결과는 앱 종료 코드 0(정상 닫기), `exit` 결과는 앱 종료 코드 3(시작 실패)으로 변환한다. 이는 검토 받은 35627cd2e의 앱 경계 계약이며, 기존 lane 종료·쓰기 정리·재시도와 다음 시작의 fallback 판정은 유지한다.

v2.0.104 어댑터는 `web_allowed(data, version)` 판정과 `before_exit(code, deadline_s)`를 셸에 연결한다. 끝내기 admission과 lane 시작을 같은 잠금에서 닫고, normal writer 허가와 실제 worker 종료(BROKEN 포함)를 확인한다. 확인된 위임 허가는 worker 종료를 기준으로 한다. 창 생성 뒤 실패는 셸이 표지를 남기고 끝내며 다음 사람의 실행에서 Tk를 고른다. 자동 재실행·PID 기다림은 없다. 새 화면의 앱 마무리는 쓰기가 살아 있는 동안 강제 종료하지 않는다.

새 화면 입구는 셸 import·WebView2 준비를 먼저 확인하고, `_prepare_screen`에서 `shell_fileio.set_write_guard(shell_write_guard)`가 돌려주는 private FileIO로 첫 WebView2 프로필 폴더를 만든다. 이어 `run_app(write_guard=shell_write_guard)`에도 같은 담장을 전달한다. 셸의 events·journal·sound_cache·udf·test_control 쓰기는 `shell/shell_fileio.py`의 다섯 함수로 모이며, 각각 기존 `writer_admission("shell_fileio")` 안에서만 실행한다. 선언에는 이 위임을 `shell_fileio` 한 source로 기록한다. 허가는 한 번 쓰기 뒤 내려놓으며 화면·스캔·사람 응답·네트워크를 기다리는 동안 쥐지 않는다.

선언 생성기는 고정 `shell/SHELL-READY.json`(v3.16.2, SHA256 `8c3b35d4740a86481e2d011630bf78271316c38ca5f1e137de8109f320e1eb88`)을 대조한다. 설치된 실행 스캐너는 후보의 `shell/SHELL-READY*.json` 하나와 파일 바이트·Python 파일 집합의 자기 일관, 앱 입구의 담장 설치를 확인하므로 다른 셸 판의 자기 일관 후보도 받는다. 거절된 events는 해당 진단 줄만 잃고 `events_dropped`로 센다. 처리 층 모드의 셸 journal 실패는 앱 저장 결과를 바꾸지 않고 다음 스캔과 최근 seq 중복 응답을 계속한다. 창 생성 전 일반 준비 실패(셸 import·WebView2·시작 폴더)는 고정 SHELL_* 원인 한 줄을 남기고 기존 시작 임대 안에서 같은 프로세스의 Tk로 이어진다. 이미 숨김 Tk가 있으면 그 처리 층을 재사용한다. writer 거절은 다시 던져 시작을 거절하며 Tk로 우회하지 않는다. 처리 층 생성 뒤 시작 실패는 셸의 표지·끝내기 길로 간다.

창 X는 0.3초 안에 처리 층 답이 없으면 취소하고 「끝내는 중」 띠를 보인다. 늦은 허용 답은 닫고 거절 답은 띠를 내린다. 셸의 기본 120초 뒤에는 저장 손실 경고와 「그래도 닫기 / 더 기다리기」를 보인다. 어댑터는 앱의 `save_current_set_on_force_exit`를 `on_force_exit` 콜백으로 넘긴다. 셸은 별도 실에서 이 콜백을 호출하고 2초까지 join한다. 콜백은 검증된 내구 스냅숏 저장을 시도하며 Tk 모달·위젯을 호출하지 않는다. 새 화면의 앱 마무리는 지연을 알리며 강제 마감하지 않는다. before_exit는 120초까지 5초마다 writer 허가와 실제 lane 종료를 확인하고 False면 셸이 끝내기를 미룬다.

설치기와 배포 helper의 조상·검증 대상 조회 및 패키지 자식 열거는 숨김 속성이 있는 ProgramData·AppData·설치 경로도 검사한다. 읽기 `-Force`는 숨김 항목만 포함하며 reparse·파일 종류·바인딩 거절과 삭제 조건은 유지한다.

작업자 대상 거절은 기존 `PENDING_TARGET_INVALID`·종료 코드 1·공개 JSON 한 줄을 유지하며 `reason`과 `summary`를 덧붙인다. `summary`는 `PENDING_TARGET_INVALID reason=<고정 코드>`이며 계정·SID·경로·예외 원문을 포함하지 않는다. 인수·계정 해석·SID 종류·inventory 경로/파일/해독/바인딩·프로필·콘솔·감사 경로의 까닭을 구분하고 미분류 또는 허용 목록 밖 값은 `UNCLASSIFIED`로 공개한다. 실제 설치의 `-WorkerAccount` 누락·빈 값도 같은 typed pending 경계에서 `reason=WORKER_ARGS_INCOMPLETE`로 거절한다. 일반 호출과 helper 사전 적재 호출 모두 stdout 없이 stderr 공개 실패 한 줄·rc 1로 끝나며 개인 진단 호출·파일 쓰기는 없다. 거절 조건과 권한 판단은 유지한다.

설치기 최상위 일반 실패는 개인 진단 또는 공개 변환 helper가 throw해도 기존 INSTALLER_FAILED / REPORT_UNAVAILABLE 공개 레코드로 마무리하고 명시적인 비영 종료에 도달한다. 정상 진단의 공개 레코드와 기존 준비 실패의 비영 종료 코드는 유지한다. -ResumeRollbackAuditPath와 작업자 인수의 충돌도 stdout 없이 stderr 공개 실패 한 줄·rc 1·파일 변화 0으로 거절한다. 충돌 없는 정상 되돌림 계속의 성공 출력은 유지한다(w9firstupdsub267, 독립 검토 보완).

관리자 설치 입구(v0, w9firstupdsub63 1차 답)는 상승 또는 Session 0에서만 `-WorkerAccount <SID 또는 DOMAIN\이름>`·`-WorkerSurveyInventory <절대 JSON 경로>`를 받는다. 실제 상승과 두 인수가 필요하다. 제품은 `status == READY`, 해석한 작업자 SID와 같은 `target_sid`(상승 호출자와 같은 SID도 허용), 그 SID의 ProfileList와 같은 `target_profile_root`만 확인한다. 파일/JSON/필드/바인딩 실패는 import·감사·설치·프로필 쓰기 전 고정 실패 한 줄로 끝난다. 나머지 inventory 필드는 해석하지 않고 파일 SHA256만 기계 표지에 기록한다. 일반 작업자 세션에 이 인수가 있으면 거절한다.

기계 배치는 기존 pinned helper·guardian·검증/백업·자료 세대 거절·startup policy/task를 사용한다. startup의 operator SID/LocalAppData는 지정 작업자를 가리킨다. 상승 또는 Session 0의 기계 설치 길은 작업자와 같은 SID여도 HKCU Run·등록·TLS·업무 자료·개인 바로 가기를 준비하지 않고 첫 일반 작업자 시작을 기다린다. `caller_is_worker`는 SID 일치와 비상승 문맥을 함께 요구한다. 같은 세션·같은 SID의 상승 콘솔 호출은 기존 설치 길을 유지한다. 옛 앱 이전은 키트 몫이다. 실패하면 새 공용 링크·PENDING을 제거하고 기존 두 파일은 원 바이트로 복원한다. 감사 기록 실패도 두 복원 시도를 막지 않는다. `update-control/worker-pending.json`은 보호된 부모에 둔 읽기 전용 `label-match-machine-worker-pending-v1` 기록이며 설치 ID·대상 SID·source commit·판·inventory SHA256을 결속한다. `PASS_MACHINE_WORKER_PENDING`은 기계 몫의 성공과 작업자 준비 대기를 뜻한다.

첫 GUI 시작은 자료 세대 쓰기 전 표지의 SID를 확인하고 기존 current-user onboarding을 사용한다. READY·등록·relay 확인 뒤 기존 admission 안에서 작업자 HKCU `Software\KMTech\Label_Match\WorkerInstallCompleted`(REG_SZ, `<install_id>:<source_commit>`)를 쓰고 되읽는다. 같은 ID의 완료는 멱등이며 기계 표지를 지우거나 다시 쓰지 않는다. 실패/다른 SID는 완료 기록을 만들지 않는다. 일반 시작의 기존 onboarding 검증은 유지한다. 옛 앱 백업·끄기·복원과 관리자 `-Rollback`은 키트 계약이며 제품 canonical에 Rollback 인수나 callback을 추가하지 않는다.
<a id="company-http"></a>

회사 서버 HTTP 요청은 포장 앱의 고정 User-Agent를 보낸다. requests의 JSON·content·iter_content 압축 해제 경로는 유지하고, urllib 명단/PIN·물류 응답은 gzip·deflate(원시 deflate 포함)를 해제한 뒤 검증한다. 명단/PIN의 해제 후 상한은 기존 262,144/8,192바이트이며 물류 응답은 8 MiB를 넘거나 압축이 손상되면 거절한다. JSON 계약 오류는 기존 코드를 유지하고 HTML 등 비계약 403·5xx는 HTTP 코드와 쉬운 회사 서버 거절·오류 문구를 남긴다. 알려진 HTTP 오류만으로 POST 적용 여부를 확정하지 않으며 같은 멱등 키의 영수증 조회·복구를 유지한다. HMAC 서명·TLS 확인·시간 초과·원장·성공 표시 순서는 바꾸지 않는다.
작업자 바로 가기의 새 대상은 설치된 `current/runtime/pythonw.exe`이며 `-I -B "<current>/app/main.py"` 인수·설치 루트 작업 폴더·앱 아이콘·일반 창(WindowStyle=1)을 사용한다. 기존 `launch-label-match.cmd` 대상은 빈 인수·동일 작업 폴더·아이콘·옛 WindowStyle=7까지 일치할 때만 소유 링크로 이전하며 지원용 wrapper는 보존한다. 기계 단계는 공용 known folder만, 작업자 세션 단계는 현재 사용자의 known folder만 처리한다. 설치 루트 아래의 없는 대상과 위에서 증명한 옛 wrapper 링크만 변경하며 다른 루트·다른 앱·그 밖의 존재하는 대상은 보존한다. 변경 전 속성·원본 링크 바이트와 해시를 설치 감사 옆 JSON/원본 폴더에 저장한다. 복원은 정확한 root·scope·새 대상·원본 해시·현재 링크를 검증하고 변경 링크의 원본을 복원하거나 만든 링크만 제거한다. 손댄 뒤 실패하면 같은 기록으로 보상하고, 이후의 링크 변경은 덮지 않는다.
기록된 옛 wrapper 링크가 속성 또는 설치 해시에서 달라지면 원본 바이트와 기존 preimage를 보존하고 `MODIFIED_SHORTCUT_PRESERVED` 사유를 기록해 그 링크만 이전에서 제외한 채 설치를 계속한다. 기록·root·scope·preimage 무결성 거절은 유지한다. 설명·단축키만 다른 기록 없는 옛 링크도 옮긴다(Set이 이 둘을 보존하며 콘솔 창을 없앤다).

onboarding과 `start_user_relay_process`는 정확히 승인된 probe sink이며(onboarding 기본 launcher는 같은 소유 확인을 admission 없이 먼저 하는 `reuse_or_start_user_relay_process`이고, 재사용 실패 때만 이 probe sink를 부른다; `process_snapshot`의 Observe는 writer admission을 잡지 않는다) 부모 writer lease도 시작·생존 확인 전에 해제한다. onboarding lifecycle 검증은 호출당 한 번 onboarding·부모 admission 밖에서 실행하며 그 호출의 thread-local 범위에만 결과를 보관한다. 각 쓰기 admission은 helper를 반복하지 않고 policy·active·transition 원 바이트의 digest와 delegation을 재확인하며 변경 시 `WRITER_LIFECYCLE_CHANGED`로 거절한다. 다른 source·스레드·다음 호출은 기존 검증을 유지한다. 디렉터리·TLS·등록·세대·원장·설정·Run·옛 작업·stop 표지·환경·보고서 쓰기는 각각 기존 source admission을 얻는다. READY는 relay 확인 뒤 새 admission에서 fence 재확인과 환경 적용을 마친 뒤 게시한다. canonical의 연기와 기존 실패 코드·stop 복원·지원 복구는 유지한다. 기존 G 기록 묶음·옛 작업 은퇴·fixed helper 자기 admission은 측정 P2로 컷 뒤에 남긴다(코디 승인; BACKLOG).

등록·배치 자식의 종료 증명은 시작 시각 기준 하나의 30초 벽시계 마감을 공유한다. 모든 CIM 소유 관찰은 같은 프로세스의 읽기 전용 runspace에서 남은 시간만 기다리고 OperationTimeoutSec도 남은 초로 제한한다. 만료하면 BeginStop만 요청하고 관찰 결과·취소 완료를 기다리지 않으며 이후 새 종료 동작은 없다. 등록은 REGISTRATION_CHILD_UNPROVEN, 배치는 HELPER_CHILD_UNPROVEN으로 복원을 거절한다. 정상 기한 안의 live 부모 사슬·생성 시각·열린 process handle 검증은 유지한다.

정상 인증 frame 전 제어 Write/Read·parse·identity 오류는 helper 종료 미증명이다. HELPER_CHILD_UNPROVEN·코드 UNKNOWN·추가 복원 명령0으로 보존하고, RECOVERY_REQUIRED frame의 진단 파일 손상도 이 표지를 유지한다. 정상 인증 frame 뒤 결과 파일 오류는 같은 guardian 복원 경로를 유지한다. 이미 복원 중이면 기존 ROLLBACK_FAILED 계열 상태에 코드 UNKNOWN을 기록하며 파일 존재만으로 완료를 판단하지 않는다. 원 예외 형식·함수·줄을 교체하지 않는다(1차 msg_e36ee0956cca).

확정 전 배치 결과 검증이 예외로 끝나도 실제 보호 startup preimage와 검증된 code backup을 복원 전에 수집한다. 소유·종료 미증명 자식은 기존 RECOVERY_REQUIRED 분기에서 복원을 거절한다. 복원 입력 수집 자체의 실패는 별도 진단으로 보존하며 최초 결과 검증 실패의 typed 원인·위치를 유지한다.

guardian 결과·실패·seal의 선언 SHA는 생성한 메모리 바이트에서 계산한다. journal은 terminal을 flush한 뒤 같은 열린 handle에서 digest를 계산하고 닫으며, 봉인 뒤 append하지 않는다. HELPER_CHILD_UNPROVEN의 코드 상태는 UNKNOWN이다. pinned guardian 실패의 원 예외 형식·위치·보고서 참조는 기존 child_output 진단 채널로 부모에 전달한다.

실행 중 배치 자식에도 720초 상한을 적용한다. 기존 부모 사슬·생성 시각·열린 handle로 자기 자식 트리의 종료를 30초 안에 확인하면 실제 native ExitCode와 HELPER_EXECUTION_EXPIRED를 보존하고 같은 guardian을 복원 전용으로 유지한다. 소유·종료 미증명은 HELPER_CHILD_UNPROVEN·RECOVERY_REQUIRED이며 코드 복원은 0이다. 실행 중 부모 소멸도 복원 없이 진단·봉인한다. 로컬 pipe는 NETWORK 그룹 deny 대신 PIPE_REJECT_REMOTE_CLIENTS와 기존 peer PID·생성 시각으로 경계를 지킨다.

canonical 설치 거래는 상승 guardian 하나를 배치와 확정 전 복원에 재사용한다. 일반 토큰의 승인 요청은 거래당 1회, 이미 상승된 토큰은 0회이며 등록은 기존 사용자 문맥이다. 결과는 native 자식 ExitCode와 같은 거래·순번·65,536바이트 상한·ordinary/reparse 거절·메모리 SHA에 결속한다. 부모가 사라지면 기존 authority/delegation을 넘겨받지 않고 RECOVERY_REQUIRED로 보존한다.

확정 시점은 `Confirm-LabelCanonicalPlacement`의 active writer fence 1회 해제다. 직전 READY·서버 등록·설정한 TLS CA·Run readback을 다시 확인하며, 그 전 실패는 기록한 코드·Run·task·stop preimage만 복원한다. 확정 뒤 실패는 POST_CONFIRMATION_FAILED, 해제 불확실은 CONFIRMATION_UNCERTAIN으로 새 코드를 보존한다. 등록 관찰은 600초, 소유 부모 사슬·생성 시각으로 증명한 자식의 종료 확인은 30초, guardian 여유는 120초다. 종료 미증명은 REGISTRATION_CHILD_UNPROVEN이며 live writer와 복원이 경합하지 않는다. guardian은 720초 이후 복원만 받으며 추가 600초에도 명령이 없으면 판정 불가로 종료한다. journal의 terminal 줄 뒤 한 번 봉인하고 덧붙이지 않는다.

등록 준비는 기존 자료 세대의 읽기 전용 검사를 유지하며, 실패한 등록이나 dry-run으로 새 세대를 기록하지 않는다. 일반 등록 CLI는 서버 승인과 로컬 신원·manifest·credential·receipt·보고서 readback 뒤에, 정상 onboarding은 READY readback 뒤 업무 원장 열기 전에 세대를 기록한다. 정확한 canonical installer delegation의 확정 전 onboarding은 등록·Run·task만 준비하며 `ledger_status`와 `settings.status`를 `DEFERRED_CANONICAL_INSTALL`로 보고한다. 이때 READY는 등록 검증을 뜻한다. T1 뒤 정상 앱 시작이 세대·원장·설정을 멱등으로 처음 열며, 확정 뒤 실패는 새 코드 보존과 지원 복구다. 이미 있는 새 세대의 downgrade 거부는 유지한다. Run/task 변경은 기존 preimage·journal만으로 복원한다. 자동 시작·옛 작업 이전은 활성 `versions/<SHA>`가 아니라 고정 `current`를 사용하고 작업 정의의 canonical 경로 검사는 유지한다. canonical 복원 delegation은 정상 등록의 `worker_enrollment_network` source를 포함하며 관리자·partial 복구 source는 별도 권한을 유지한다. 등록 실패 뒤 같은 사용자 CNG 이름의 소유 키는 `provision_initial`이 정책 검증 후 재사용하며 교체·비공개 키 export를 하지 않는다.

T1 뒤 installer의 relay PASS와 GUI의 업무 자료 첫 열기는 별도 관측이다. managed 정상 onboarding의 실제 ledger/settings 열기 실패는 `FAILED`·`BUSINESS_DATA_OPEN_FAILED`, 기존 onboarding 보고서 위치와 같은 사용자용 `recovery_command`를 게시하고 앱 시작을 중단한다. 원인을 해결한 뒤 고정 current의 기존 onboarding CLI로 재준비한다. 등록·Run/task 실패와 세대 UNKNOWN/REFUSE는 기존 분류를 유지한다.

실패 복구의 protected preimage 자식 grant 갱신은 동일한 live 부모 authority와 session·attempt·transaction·token·source 결속을 요구한다. grant만 만료된 경우의 갱신은 새 소유자·새 source·일반 writer를 허용하지 않으며 generation REFUSE/UNKNOWN 및 원 backup 보존을 유지한다. fixed helper Verify는 TLS/등록 이전의 코드 신뢰 검사이고, 그 실패의 공개 레코드와 보호 child 진단 참조는 onboarding까지 유지한다.

fixed helper는 installer 부모의 비공개 함수를 전제하지 않는다. 검증한 bootstrap/portable export와 자기 경로/reparse 함수만 사용하며, dependency module만 있는 새 자식의 전체 Verify 회귀로 이 계약을 검사한다. ACL·integrity·manifest/pin·version의 실패 검사값과 native trace는 보호 진단에만 남기고 공개 출력은 고정 사유·report 경로/SHA를 유지한다.

옛 공식 relay task의 launcher admission은 첫 상태 변경보다 앞선다. task 0이면 launcher 0을 허용하고, task가 있으면 기존 identity 및 action VBS와 동명 wrapper의 존재·ordinary/no-reparse·공식 실행 template 일관성을 요구한다. 불일치는 기존 거절 안내를 사용한다.

relay install pack의 `--program-data-root`는 절대 경로 인수다. 상대 경로는 정규화·보호 판독·generation 기록 전 exit 2로 거절하고 상태/report를 쓰지 않는다.

첫 설치의 옛 공식 relay Disable 전이는 기존 보호 base 준비를 먼저 거친다. Restore는 기존 base 요구를 유지하고, 설치자 owner record는 일반 코드 배치가 작성한다. 잘못된 전이 인수는 base 준비 전 거절하며, 진단 가림 과정은 원 예외 타입·원인과 빈 metadata 객체를 보존한다.

<a id="residual-tag-rejection"></a>
잔량표(`SRC=KMTECH_RESIDUAL_TAG`)는 검사실 전용이다. 포장은 일반 판독·과도기 분류·스캔 로그·작업 시작·완료 대기 입력 보관보다 먼저 오류음과 「잔량표는 포장에 쓸 수 없습니다 — 검사실로 돌려보내세요」로 거절한다. 오류 확인 Enter와 완료 트레이 취소의 별도 판독도 잔량표로 확인·취소를 진행하지 않는다. 기존 트레이·교환·미달·재스캔 상태를 바꾸지 않는다. 현재 상태와 보류 스냅샷의 raw·물리/활성/정본 QR·미달 되돌림에도 같은 거절을 적용하며 복원 원본은 보존한다. 관리자 현품표 재판독은 관리자 승인·감사·서버 조회 전에 잔량표를 가른다.

서버 `RESIDUAL_TAG_NOT_ALLOWED`·retryable=false는 확정 거절이다. 지연 검증은 INVALID, 이미 완료한 포장/취소 outbox는 기존 CONFLICT, 교환은 기존 OPERATOR_REVIEW로 처리한다. 재시작 뒤에도 자동 재전송·재시도·영수증 재조회하지 않는다. 작업자는 관리자 보류로 원본과 거절 건을 보존한 뒤 다음 트레이를 시작할 수 있다. 일반 현품표의 내구 저장·성공·ACK·재시도 및 미확정 거래 처리 계약은 유지한다.

<a id="scanner-only-packaging"></a>

완료 직후 방금 결과 줄은 `completion_kind=full`이면 기존 성공 초록색으로 `트레이 완료 · 이 PC에 저장됨`을 표시한다. 부분 완료와 다른 결과는 기존 표시를 유지한다. 초기화가 끝난 표준 PHS2 작업 화면의 알림 상자는 `다음:` 줄을 덧붙이지 않고 가운데 다음 스캔 줄을 사용한다. 다른 공정·옛 방식 화면·시작 안내는 기존 표시를 유지한다. 스캔·저장·완료 판정은 바꾸지 않는다.

오른쪽 pane은 조회 notebook(row 0) → 작업 단추 틀(row 1) → 서버 연결 상태(row 2) 순서다. 연결 표시와 단추는 같은 grid 칸을 사용하지 않으며, 단추 넷은 동일 폭의 두 열·두 줄에 한 칸씩 배치한다. 기존 연결 문구·style·wraplength·ACK/대기 갱신과 단추 동작·입력 gate를 유지한다.

유지보수 active fence가 있으면 중앙 포장·교환·인쇄·PIN·등록·런타임 임대는 첫 HTTP 전에 거절한다. 공개 임대 갱신과 CLI 등록·부분 복구·관리자 복구도 같은 admission을 확인한다. 원 명령·멱등 키·pending 임대 요청·UNKNOWN·복구 원본을 보존하고 해제 뒤 같은 내구 요청을 재시도한다.

예외: 이전 판에서 저장된 F5 교환의 recover_current/recover_reconciliation만 원격 응답과 로컬 적용까지 writer admission을 보유한다(코디 결정 2026-10-02). 화면의 기존 busy gate는 스캔을 거절하고 미리 입력된 값은 보존하며, 응답 뒤 다시 Enter하면 저장한다.

원격 HTTP 구간은 공용 writer mutex와 writer depth를 보유하지 않는다. 반환 직후 같은 lease를 재획득하고 설치 fence·delegation을 다시 검사하며, 변화가 있으면 로컬 결과 적용을 거부한다. 기존 로컬 intent·명령·idempotency key는 보존한다. 포장/취소 명령 저장 및 모든 결과는 SENDING·expected attempt_count·command_json·보류 없음의 SQL transaction CAS로만 적용한다. stale 실패는 상태·감사·lease를 바꾸지 않는다. 검증된 stale 성공은 동일 불변 명령에만 receipt 재확인 표식을 내구 저장한다. 다음 drain은 재POST 없이 기존 key로 receipt를 조회하고, exact COMMITTED 증거가 있을 때만 현재 SENDING/PENDING/CONFLICT·attempt·명령·보류 및 표식을 CAS 대조하여 ACKED로 전이한다. receipt가 없거나 불명확하면 상태·표식을 보존하며 일반 CONFLICT는 자동 재대조 대상이 아니다. 기존 검토 감사는 보존하고 표식의 완료 감사에 이전 상태·오류와 중앙 receipt를 덧붙인다. 교환도 attempt·명령 hash·상태·로컬 적용 세대를 CAS로 대조하고 stale 성공만 같은 receipt 조회로 복구한다. runtime lease는 아직 동일 pending request인 응답만 검증 결과를 쓰며 F5 journal은 읽은 파일 SHA가 유지된 경우에만 원자 저장한다. 전송기 중복 실행은 짧은 lock으로 busy flag만 설정하고 HTTP 동안 lock을 보유하지 않는다.

성공 스캔은 내구 checkpoint 뒤 기존 `one.wav`(짧게), 정상 세트 완료는 intent·CSV·marker 뒤 `pass.wav`(길게), 거절은 `fail.wav`를 세 번 반복한다(G10). 다음 소리가 오면 이전 오류 반복 예약은 무효화한다. 별도 미달 확정 소리와 자동화 무음은 유지한다. 510의 오른쪽 단추 아래 별도 바닥 행은 공통 상태 한 줄을 표시한다. 정상은 `회사 서버 연결됨 · 보낼 것 N건`, 확인 전은 `회사 서버 확인 중`이다. 관리자 확인 수가 있으면 그 안내가 우선이며, 끊김은 주황 바탕, 연결된 상태에서 가장 오래된 미전송 항목이 10분 넘으면 주황 글자로 표시한다. ACK 시각은 쌓인 상태의 `마지막 전송 HH:MM`에만 쓰고, 기록이 없으면 그 조각을 생략한다. relay·포장·취소·교환·아직 materialize하지 않은 capture의 기존 건수를 읽으며 새 저장 구조는 없다. 폭이 부족하면 공통 짧은 꼴을 쓰고 줄바꿈·말줄임은 없다. 연결은 현황 요청 시작 시각과 전송 ACK·실패의 내구 기록 시각을 비교해 판정하며, 새 응답 전의 idle·유효 lease는 끊김을 해제하지 않는다. 실제 성공 증거 전에는 확인 중이며, 늦게 끝나거나 적용된 옛 현황 응답은 더 최근의 성공·실패를 덮지 않는다. 저장된 retry를 늦게 읽어도 읽은 시각을 새 실패 시각으로 쓰지 않는다. 현황 본문과 footer는 같은 연결 판정을 사용하고 마지막 성공 전송은 ACK만으로 갱신한다. SUPERSEDED capture는 중복 집계하지 않고 읽기 실패 때 건수를 0으로 단정하지 않는다. relay 중지·관리자 확인은 같은 연결 관측과 기존 관리자 건수로 공통 한 줄에 표시한다. 세부 상태는 기존 대기 현황에서 확인한다. 전송 경로 전체의 요청 시작 시각 기록은 [후속 범위](BACKLOG.md#connection-observation-order)다.

옛 현품표의 작업 슬롯도 현품표 + 제품1–4로 표시한다(G9). 다섯 번째 입력은 제품도 허용하며 과도기 검증의 31자 이상·유효 생산일자(6D) 조건은 유지한다. 이 슬롯의 내부 역할 키·완료 CSV 역할 계약은 기존 값을 보존한다.

진행 현황 탭은 기기 인증 client로 `GET /logistics/api/v1/progress?authority_scope_id=…&date=YYYY-MM-DD&stage=packaging`를 읽고 60초마다 별도 스레드에서 갱신한다. 품목별 계획·포장 완료·남은 수량/상자와 마지막 성공 수신 시각을 표시하고, 실패하면 마지막 값과 `연결 끊김`을 남긴다. 현황을 연 채 스캔하면 기존 조회 복귀 경로로 작업 화면에서 같은 원문을 처리한다. 늦은 현황 응답은 작업 화면 선택·세트·입력 gate를 바꾸지 않는다(G8, CONTRACT v1.5 §11).

중앙·과도기 세트의 제품 중복(현재 세트·이미 완료된 다른 세트)은 오류음과 안내로 그 입력만 거절한다. 세트·표본·교환·UNDO 위치를 그대로 보존하며 다음 정상 제품을 바로 받는다(G5). 품목 불일치·제품 형식 오류는 기존 내구 FAIL 경로이며 저장 실패·중앙 완료 충돌의 복구 계약도 유지한다.

fake Tk pump는 예약 callback이 계속 생겨도 반복마다 작업 스레드에 실행 기회를 준다. 기본 2초 제한과 완료 단언은 유지하며 실제 정지된 lane이 여전히 제한 시간에 실패하는 부정 시험으로 검사한다. 실제 Tk·스캐너·서버·설치 수용과 브랜치 전체 시험은 별도 통합 검증 범위다.

DESIGN §1.7에 따라 조회 탭에서 스캐너 Entry 입력을 받으면 이번 세션 작업 화면으로 돌아가 같은 원문을 처리한다. 지난 날짜 조회는 오늘의 작업·중복 인덱스를 보존하며, 복귀 때 이전 날짜의 배경 조회/표시 generation을 무효화하고 보관한 오늘 화면으로 돌아간다. 초기화·오늘 인덱스 오류·저장 차단·중앙 처리 등 기존 안전 gate는 유지한다.

DESIGN §1.2와 코디 추가 결정에 따라 품목 불일치·입력 오류는 오류 순간 기존 FAIL 판정·기록 내용을 내구 완료한다. 로컬 current_set의 선택 필드 scanner_failure_result/scanner_failure_details가 판정과 원문을 보존하며 중단·재시작과 저장 재시도에도 같은 FAIL을 이어서 한 번만 기록한다. 안내를 닫는 다음 Entry 스캔도 새 입력으로 처리하고 Enter/ESC는 안내를 닫는다. 저장 실패 때는 기존 세트·실패 intent를 유지하고 재시도에서도 PASS로 바꾸지 않는다. 서버 필드·명령 형식은 바꾸지 않는다.

보류 현품표·중앙/교환 대기·음향/이력 callback 오류는 창 안 안내와 기존 소리를 사용한다. 미완료 저장본은 질문 없이 현재 교대 작업자에게 복원하며 원 스캔·접수와 과거 작업자 귀속을 보존한다. 복원 안내 자체는 입력을 잠그지 않고 다음 정상 제품 뒤 사라진다. 작업자 선택/등록, 설치·기준 파일 수리, 설정·켜진 시뮬레이션의 PC 앞 창은 유지한다. 미완료 현품표 재출력·완료 이력 취소/삭제·미완료 종료 확인은 기존 강조 안내의 마우스 선택으로 진행한다. 스캔·Enter·Space·Y/N은 승인하지 않으며 같은 확인 요청에서 마우스를 누르고 놓은 경우만 원 대상과 현재 상태를 다시 확인한다. 누른 뒤 다른 확인 요청으로 바뀌면 다시 클릭해야 한다. 업무 입력칸의 스캔은 업무 입력으로 받고, 확인·원문·날짜 영역에서 받은 키는 화면에 다시 스캔 안내를 남긴다. 원문과 날짜 Calendar는 기존 이력 상세 영역을 사용하고 별도 창·grab·wait를 만들지 않는다.

빈 작업대는 종료 확인 없이 기존 저장·lane drain·전송 정리를 실행한다. 현재 세트 raw·교환 단계·미달 UNDO 또는 현품표 교환 미완료 상태가 있을 때만 종료를 묻는다. 내구 전송 대기열만 있는 것은 확인 사유가 아니다. 정상 화면의 F5 키·날짜 교환 단추·안내는 숨기며 저장된 교환은 `관리자 → 멈춘 작업 확인 (관리자) → 미완료 현품표 교환 복구`에서 같은 요청으로 재개한다.

mainloop가 예외나 직접 반환으로 끝나도 기존 종료 drain·내구 저장·writer 정리·Tk destroy를 한 번 이어 완료한 뒤 단일 실행 잠금을 해제한다. 이미 접수된 종료는 다시 기록하지 않는다. 마감은 지연 표시 시점이며 유한 작업이 끝날 때까지 기다린다. 지연 표시의 쓰기·삭제는 일반 writer sink로 선언하며 표시 삭제 실패는 시작을 막지 않는다. 원 loop 예외·SystemExit 코드는 정리 오류보다 우선한다.

교환·일반 UNDO·미달 release·자동 source 검토 감사는 current-state의 scanner_audit_outbox에 업무 checkpoint와 함께 저장한다. 고정 key·원 시각·작업자·이벤트 내용을 보존하고 직후·다음 스캔 전·저장 재시도·기동 때 기존 CSV writer로 재생한다. 같은 key의 동일 CSV 행은 재append하지 않고 fsync하며, CSV와 outbox 제거 checkpoint가 모두 성공한 항목만 목록에서 지운다. CSV 실패는 입력을 막지 않고 pending을 유지한다; 업무 checkpoint 실패만 기존 차단한다. UNDO·reset·완료 후에도 미전송 감사는 보존하므로 교환과 그 UNDO가 모두 남는다. 목표 수량에 도달하지 않은 재시도를 PASS 완료하지 않는다. 기동·복구는 기존 DB 보류 snapshot과 검증된 archive의 pending 감사도 읽어서 같은 ID로 재생한다. 보류 원본·PIN에 결속된 현재 파일은 재작성하지 않고, 활성 파일이 없으면 만들지 않는다; 보류 감사의 재생 여부는 CSV readback으로 판정한다. 큰 감사 목록은 `_scanner_audit_outbox/<SHA256>.json`의 내구 조각을 먼저 저장한 뒤 `scanner_audit_segments` 참조와 업무 checkpoint를 원자 저장한다. 재기동은 SHA·스키마가 맞는 조각을 기록 순서대로 재생하며, 손상·소실 조각의 원본과 참조는 보존한 채 다른 감사와 입력을 계속한다. 이전 8MiB 초과 inline 감사도 업무 본문 검증 뒤 읽는다. 백업·보류 복원에는 checkpoint와 `_scanner_audit_outbox` 폴더를 함께 보존하며, 완료된 조각도 자동 삭제하지 않는다. CSV 재생은 prefix의 byte 수·행 수·SHA256·마지막 행 hash를 기존 current-state의 scanner_audit_csv_prefixes에 함께 저장한다. 재기동·파일 변화 때 전체 prefix와 전 행을 검증하고 같은 writer의 내구 append만 증분 색인하며 ACK 직전에 원 바이트를 재대조한다. 증명 손상·불일치는 기존 전 행 비교로 되돌아가며 업무 복구를 차단하지 않는다. 같은 key의 다른 내용만 기존처럼 거절하고 동일 중복·순서 변경·잘린 끝에 새 전역 오류를 추가하지 않는다. 행별 fsync·마지막 ACK checkpoint와 찢긴 쓰기 뒤 정확히 한 번 재생을 유지한다.

교환 pair·제품 raw/parsed·UNDO 위치·감사 intent를 같은 checkpoint에 저장한 뒤 표시한다. 이벤트 저장이 끊겨도 제품과 되돌림 위치를 함께 복원하며, 옛 pair-only tail 저장본의 UNDO도 받는다. 표본·교환 대상·늦은 source 확인은 교환 overlay를 적용한 같은 유효 구성원 목록을 사용한다. 대체품은 알려진 원 봉인 구성원·대상 자신·앞선 교환의 불량/대체품을 상태 변경 전에 거절하며 source 목록을 모르면 기존 배경 판정으로 넘긴다. 일반 UNDO도 checkpoint 저장 직후 메모리를 맞춰 후속 이벤트 실패 때 저장본과 어긋나지 않는다.

미전송 옛 PHS2 저장본은 첫 정상 제품 입력을 내구 저장할 때 scanner 상태로 전환한다. 그 전에도 독점 UI lane의 배경 검증을 미뤄 제품 입력을 받는다. 이미 outbox에 저장된 옛 draft·명령은 덮어쓰지 않고 기존 복구 경로로 처리한다.

현품표 1장 + 상단 제품 4개를 받아 다섯 번째에서 자동 내구 완료한다. 새 PHS2의 네 제품 자리는 같은 품목·중복·알려진 구성원을 검사하며 마지막 자리에 추가 길이·생산일자 조건을 두지 않는다. 옛 현품표의 과도기 조건은 유지한다. 중앙 조회는 배경이고 무응답·단절에도 스캔과 로컬 완료를 받는다. 로컬 intent → 완료 CSV flush → commit marker → 표시 순서를 지키고 같은 key로 교환 ACK → source 확인 → 서명 lease → CREATE_PACKAGE를 배경 전송한다. 늦은 표본 불일치는 해당 outbox만 OPERATOR_REVIEW로 남기며 로컬 완료를 취소하지 않는다.

KMCMD:EXCHANGE/교환 단추는 불량 대상 → 대체 양품을 받아 연결된 로컬 교환을 먼저 저장한다. KMCMD:UNDO는 부분 제출 전 미완료 세트의 마지막 제품/교환만 되돌린다. 같은 현품표 재스캔은 기존 부족 조건을 검사하고 state.confirm={kind:partial_submit,total,done,short}를 메모리에만 둔다. 같은 현품표·빈 Enter·partial_submit은 기존 미달 결정(shortage_confirmation_pending·shortage_undo_snapshot·shortage_confirmed_at)을 먼저 내구 저장한 뒤 기존 완료와 release_pending → outbox를 유예 없이 수행한다. partial_continue·Esc는 확인만 내리고 기록하지 않는다. 제품·다른 현품표는 확인을 내린 뒤 기존 스캔으로 처리한다. 결정 이후에는 Undo가 없으며 결과는 「부분 제출 완료 — M개(K개 부족)」다. 옛 snapshot·결정 저장/완료/release 사이 중단은 다음 시작에 같은 완료 ID·멱등 키·감사로 즉시 재개하고, 같은 outbox 효과를 중복하지 않는다. 확인 자체는 저장하지 않아 재시작 때 다시 묻는다. 기존 3초 중복 읽기 억제·목표 수량 자동 완료·일반 교환·완료 기록 취소는 유지한다.

확정 계약은 w9scanonly CONTRACT.md v1.4(SHA256 2D69CD99913329E28A6C3CBDAFE0F64D6CCECAD0F00D779324070D04BED49EA6)이다. 요청 공통 필드는 원본 scan_payload, local_completion_id, local_completed_at, deferred_reflection=true이며 포장은 short_confirmed, sample_barcodes, exchange_idempotency_keys를 추가한다. 교환은 SCAN_REGISTER_GOOD와 old_barcode/new_barcode, exchanged_at을 전송하며 기존 전체 대상 구성원·CAS·봉인 증거를 보존한다. 응답의 pairs에는 old/new_unit_id, new_source_bundle_id, new_source_kind가 있고 source_membership_changes는 최상위에 둔다. UPSTREAM_COMPLETION_PENDING+retryable=true는 동일 key 재시도, COMPLETION_REVIEW_REQUIRED는 그 건의 확인 상태다.


교환 ACK 유실은 기존 journal의 같은 명령·영수증 조회로 복구하고, 예약 재시도 경로에서도 봉인을 전자 검증하여 로컬 적용 완료로 저장한다. 새 봉인 QR 스캔이나 적용 창은 필요하지 않다.


포장 상단 확인 수(w9firstupd sub-151, 서버 계약 packing-settings-v1 v1.1): 위의 「제품 4개·다섯 번째」와 미달 「1–3」은 기본값 N=4일 때의 값이다. 앱은 시작과 작업자 저장(작업 세션 시작) 때 `GET /inbound/api/packing-settings.json`을 item-catalog와 같은 profile·Bearer·신뢰 origin으로 배경 스레드에서 읽고(화면 스레드는 기다리지 않음), 받은 값을 `packing-settings.json`(app_settings 옆, writer sink `label_packing_settings_save`)에 둔다. 못 읽음(전송·4xx/5xx·profile 없음·믿을 수 없는 URL)은 `PACKING_SETTINGS_UNAVAILABLE`, 계약 이름·범위(1–6)·형식 오류는 `PACKING_SETTINGS_INVALID`이며 둘 다 마지막 받은 값(없으면 4)으로 계속하고 작업 화면 안내 줄에 경고(오류음 없음)를 보인다. 세트가 열릴 때(첫 스캔) N과 판을 current_set_info에 고정하고 그 세트는 끝까지·이어 열기에서도 그 N(스캔 N+1, 마지막 자리 = 제품N, 미달 1–(N−1))을 쓴다. N=4·판 0은 키를 쓰지 않아(없음 = 4·판 0) 지금 기록·초안 바이트가 같다. 스캐너 포장 확정 payload에는 늘 `packing_settings_version`(세트의 판)을 싣고, 400 `PACKING_SETTINGS_VERSION_INVALID`만은 그 확정을 보류하지 않고 설정을 다시 읽어 같은 key·새 판으로 다시 보낸다. 옛 QA 표본(비스캐너) 수는 공용 상한 3을 넘지 않는다. 앱에서 값을 바꾸는 길은 없다.

부분 제출 조건(LM-17): 옛 수동 마감(PARTIAL)은 LOCAL·ONLINE 모두 거절한다. 새 현품표는 회사 서버에서 확인한 현재 작업군 제품 수가 1–(이 세트 표본 수−1)이고 찍은 제품 수와 같을 때만 부분 제출을 묻는다(원 TRANSFER 봉인 수와 구별). 제품 수 미확인·불일치는 현재 트레이를 유지하고 기존 거절 안내를 보인다. 옛 현품표는 제품 하나 이상·표본 수 미만에서 같은 확인을 쓴다. 기존 60초 미달 Undo 유예는 위 확인/즉시 release 계약으로 대체한다.

LOCAL 완료 안내(LM-1): PC 안에서만 끝나 PACKAGE가 없는 새 현품표(PHS=2) 상자는 완료 안내·이번 작업 목록·스캔 기록·기록 상세에 「출고 전 관리자 보충 필요」를 표시한다. 기록을 다시 읽어도 기존 완료 상태에서 같은 안내를 계산한다. 전송 대기·ACK·충돌·부분 제출는 기존 상태를 표시한다. 옛 현품표(LEGACY) 상자는 과도기 옛 상자 출고 길을 사용하므로 이 안내를 표시하지 않는다. 통과 판정·집계·수량은 기존 완료 기록을 사용한다.

<a id="legacy-label-transition"></a>

공유 과도기 분류표 블록은 Container_Audit `ca6ab8fe:label_qr.py`의 UTF-8/LF 7765바이트와 동일하며, 고정 사본의 바이트 비교와 기존 111개 결과 벡터를 함께 검사한다(P3). 출력 분류·사유 순서는 유지한다.

제품 자리에 현품표(옛 13자리·옛 QR·PHS2·ITG/TRF)가 오면 공통 carrier/transition 판별로 오류음과 창 안 안내를 내고 그 입력만 무시한다. 이미 받은 스캔·세트·중복 집합·완료 원장·outbox를 유지하며 다음 정상 제품을 즉시 받는다. 오류 확인 스캔이나 리셋은 필요하지 않다(P2).

과도기 모드(Machine `KMTECH_LEGACY_LABEL_TRANSITION=1`, 등록·필수 PC만)는 모든 현품표를 받고 `TRAY_COMPLETE` detail 최상위에 Container_Audit와 같은 `transition_class`(`LEGACY`·`PHS2_CENTRAL`·`PHS2_LOCAL`·`PHS2_MALFORMED`)·`transition_reasons`(사유 코드 목록, 대문자 코드만)·`transition_duplicate`를 붙인다. 꺼져 있으면 세 필드가 없다. `PHS2_CENTRAL`만 기존 포장 전송함 흐름이고, 나머지 `package_logistics`는 `{status: TRANSITION_LOCAL_ONLY, sample_barcodes_are_membership: false}`로 포장 전송함 행·operation lease·중앙 명령을 만들지 않으며 취소도 로컬이다. 로컬 분류 세트는 5단계(`LEGACY_QA_SAMPLES`)로 진행하고 current state에 같은 세 필드를 보존한다(recovery schema가 타입을 검사). 켬에서 시작한 세트는 첫 스캔 때 분류를 고정하고(새 현품표는 `PHS2_CENTRAL`로 시작), 로컬 결정은 완료 행보다 먼저 저장해 재시도·재시작·끔 복원에서도 같은 분류·원장 밖으로 끝낸다. 이런 세트의 완료·취소 사건은 detail에 사건 ID `idempotency_key`(`label_transition.event_key`, 128자 이하, 같은 세트·같은 사건이면 같은 값; 취소는 `cancellation_key`로 세트마다 하나라 이력 삭제·현품표 취소 두 경로가 같은 값을 쓰고 다른 경로의 재시도도 첫 행을 재사용)와 `app_version`을 가지며, 완료 행은 등록 PC의 `transition_source_host_id`도 가진다(Web이 설치 신원과 대조하는 `source_host_id` 키는 쓰지 않는다). relay는 `RAW_LEGITIMATE` 영수증을 서버 카탈로그의 원문 전용 사건 행(아래 lifecycle 예외의 20종, `NO_STAGE1_REDUCER`) 또는 이 PC의 과도기 업무 행(`TRAY_COMPLETE`·`SET_DELETED`·`TRAY_COMPLETION_CANCELLED`, 사건 ID와 `transition_source_host_id` = 업로드 manifest source_host_id; 행의 나머지 내용은 서버가 판정)이 웹 영수증 표(w9webrecv `RECEIPT-CODES.md`, d928087)의 `TRANSITION_DUPLICATE_OBSERVED`·`TRANSITION_NOT_PROJECTABLE` 원문 관측으로 정확히 맞고 항목 수 합이 행 수와 같을 때만 성공으로 받는다. 첫 스캔 중앙 확인이 막힌 PHS2와 F3 중앙 경로가 막힌 PHS2는 그 세트의 미제출 capture를 F1과 같은 `cancel_unsubmitted`로 닫은 뒤에만 로컬이 된다(열린 VALIDATED intent는 같은 현품표 FIFO의 뒤 capture를 막는다). 로컬 결정은 capture를 닫기 전에 저장하고, 닫은 뒤 멈춘 고정 로컬 세트는 복원이 지우지 않는다. 닫을 수 없으면 capture는 관리자확인으로 남기되 세트에서 떼어, 다음 세트가 그 id·capture를 물려받지 않는다. 시작 적재에서 세트가 가리키는 capture의 저장 근거(원래 QR·세트 id)가 그 세트와 다르면(`CANCEL_CAPTURE_INTEGRITY`) 중앙이 아닌 세트는 capture를 떼고 이어지며(capture 행은 그대로, 관리자확인·BLOCKED_INVALID·옛 공용 구획 미완료면 건별 복구 목록), capture만 있는 세트는 비운다. 중앙 세트나 그 밖의 확인 실패(BINDING·저장본 없음·읽기 오류)는 저장된 세트를 그대로 건별 복구에 올려 보류할 수 있게 한다. 스캔이 다 찬 로컬 세트를 복원하면 '저장 재시도'로 같은 완료를 잇는다. 세트 없이 목록에 있는 capture도 '보류 후 계속'으로 보류된다(capture 행은 바꾸지 않는다). 새 capture는 현품표별 구획(`label-package-source/<파싱한 정규 PHS2 필드의 sha256>`, 파서가 같은 라벨로 읽는 키 대소문자·구분자 공백 변형은 같은 구획)에 들어가 같은 현품표 안에서만 FIFO를 지키고, 한 현품표의 미완료(검증대기·관리자확인·선행조건 대기)가 다른 현품표의 확인을 멈추지 않는다. 읽기 전용 `label-package-source`의 409/412는 서버를 바꾸지 않았으므로 취소 가능한 BLOCKED_INVALID다. 명령·영수증·outbox·전송 시도가 없고 읽기 전용 확정 거절로 생긴 OPERATOR_REVIEW도 F1·과도기 대체로 CANCELLED가 된다(명령이 나간 capture는 그대로 보존). capture만 있고 스캔이 없는 세트에 다른 현품표를 스캔하면 새 세트가 되고, 같은 현품표(표기만 다른 것 포함)를 다시 스캔하면 그 세트가 화면에 있든 빈 화면이든 새 capture 없이 그 capture를 이어받는다(확인할 차례면 확인하고 아니면 지금 상태를 다시 보인다. 세트에는 capture에 저장된 원래 QR을 기록). 관리자확인 안내는 상태별이다: 자기 세트의 BLOCKED_INVALID만 F1과 'CA 봉인 뒤 F1 후 같은 현품표 재스캔'을 말한다. 구조화 라벨 분류는 기존 carrier parser 별칭(`QTY`·`ITEM_NAME`·`PHASE`·기본값)을 따른다. 같은 세트의 완료 재시도(재시작 포함)는 대기열을 비운 뒤 보존된 모든 날짜의 로그에서 기존 `TRAY_COMPLETE` 행을 찾아 있으면 새 행을 쓰지 않는다(로컬 분류는 저장 상태의 `transition_row_key` 표식으로 재시도를 구별: 이 판이 첫 스캔에 둔 빈 표식의 첫 시도만 찾기를 건너뛰고, 표식 없는 옛 상태는 늘 찾는다). 꺼짐 상태는 옛 현품표와 유효 계보가 없는 새 모양 현품표의 제품 스캔을 원인 문구로 거부하고 F4 경로는 유지한다. 정책 거부(옛 현품표·필수 모드 F3 소량·중앙 프로필 부재)는 저장 실패 문구 대신 원인을 표시한다. 분류·사유 목록과 현장 절차는 [운영](operations.md#legacy-label-transition)을 따른다.

관리자 복구의 relay 전이는 검증된 중앙 recovered/COMMITTED 응답과 동일 MachineGuid/SID 유도 install ID·producer/source/manifest/endpoint, 정확한 정상 제거 marker/report, relay 부재를 요구한다. 한 SQLite FULL transaction에서 활성 authority의 scope/key와 큐의 key 결속만 바꾸고 current-user DPAPI 감사에 이전 authority·원 큐 digest·중앙 receipt를 보존한다. 원 payload/metadata·idempotency·runtime fence/token·행 상태/receipt는 바꾸지 않는다. runtime proof는 Web의 exact receipt replay에 포함되므로 새 HMAC 키로 원 요청을 재서명하며 fence는 ACK 뒤 정상 서버 lease 만료/발급에서 증가한다. 다른 신원·미확정 응답·격리 authority·경합 변경은 전이를 거부한다([지원 재개](operations.md#registration-relay-recovery)).

[w9 시험 감사](operations.md#test-audit-w9)는 기존 계약을 그대로 검증한다. bootstrap 전체 inventory는 신뢰된 `SharedCodeRoot`를 명시하고, F4 prepare에는 앱 parser가 검증한 정수 QT를 전달하며, relay ACK는 stdout 대신 durable status·queue·실제 수신 내용으로 확인한다. ProgramData 기본값 시험은 onboarding 상태가 없는 standalone 문맥을 명시한다.

`Label_Match.DataManager`는 [저장 구현](../../label_data_manager.py)에 위임한다. clock·open·이벤트 투영·현재 세트 파일명은 호출 시점의 앱 callback으로 결속하며 A/C/P 저장 루트 우선순위, CSV flush/fsync·원자 JSON 교체와 기존 save/load 결과는 유지한다([LM-1](operations.md#module-boundaries-lm1)).

`label_history.py`의 read/summary snapshot은 기존 `sorted_sets`, `scan_count`, `summary_items`, 중복·완료 색인, `updates_active_state`, `load_generation`과 오류 payload를 유지한다. 앱 façade는 기존 read·summary 이름과 clock/open/해석 callback을 유지하고 complete snapshot의 gate 해제·stale 결과 거부 및 화면 적용은 원 owner에서 수행한다.

## 고정 PowerShell leaf와 bootstrap 신뢰

지원 source의 사용자 제거·되돌림은 먼저 고정 current의 bootstrap을 검증한다. 관리 기록이 있으면 완전한 policy·active·transition, 단계별 SHA 관계, versions/<sha> bootstrap·build·고정 신뢰 키 일치를 증명한 활성 판 inventory에 실행 source 전체 bytes와 판/build를 결속한다. 초기 설치 DEFERRED 상태에는 거래 ID가 없으므로 pointer/state ID 동등성을 요구하지 않는다. 검증된 활성 host가 고정 A logical root를 정리할 때도 해당 활성 판 identity를 사용한다. 옛 A packet은 UNKNOWN, 고정 A host의 B 자료 접근은 REFUSE를 유지한다. 증명·최종 inventory 대조는 쓰기 허가 밖에서 수행하고, 복귀한 허가 아래 자료 세대를 다시 확인한다. 이 증명은 해당 제거 검사에만 유효하며 일반 시작의 required 기록·REFUSE·서명·같은 SID·writer delegation 계약은 유지한다.

설치 packet의 `release_identity`는 기존 전체 verifier와 판·build 검증을 통과한 PASS만 해석된 제품 루트·진입점별로 기억하고 사본을 반환한다. 실패·예외와 개발 source의 clean-Git 확인은 기억하지 않는다. 각 입구는 첫 writer admission 전에 미리 확인하고 실패는 기존 G 흐름에서 다시 판정한다. G 판단·마커 기록·sink·허가 구조와 onboarding의 별도 직접 무결성 확인은 유지한다.

canonical 배치 launcher는 현재 access token의 관리자 역할을 확인한다. 이미 상승된 SSH 관리자/셸에서는 같은 pinned helper를 `RunAs` 없이 직접 실행하여 승인 창을 요구하지 않고, 필터링된 관리자/일반 토큰에서는 기존 `RunAs` 경로를 사용한다. 계정의 그룹 소속만으로 상승을 가정하지 않으며 payload·helper pin·delegation·자료 세대 검증은 유지한다.

bootstrap v1 writer와 Python의 현재 파일 열거는 상대 POSIX 경로의 case-sensitive ordinal UTF-16 순서를 사용한다. Python onboarding·canonical stop-marker preflight는 기록 행을 같은 순서로 정렬해 경로·크기·SHA 전체 목록과 대조하므로 과거 기록 순서도 수용한다. aggregate는 기록에 저장된 원래 행 순서로 검증하며 중복·추가·누락·변조·대소문자 경로 변경을 허용하지 않는다. 순서만 다르면 `bootstrap_integrity_inventory_order_differs` 한 줄을 기록하고 파일을 재작성하지 않는다. installer의 canonical 출력·검증과 receipt/stop-marker 권한 검사는 유지한다.

writer 전환은 설치된 파일 집합을 기준으로 한다. 57f52e1의 shared 0.2.0에서 X13-B로 전환할 때만 `app/kmtech_shared/powershell/portable.ps1` 한 파일 추가와 명시한 여섯 파일(shared 버전·manifest·lock·vendor provenance, placement/bootstrap)의 전/후 SHA256 쌍을 허용한다. 선언의 text pin은 Git LF bytes 기준이며 해당 파일의 CRLF checkout 표현도 인정한다. writer pin 파생·멤버십, 나머지 Python AST와 runtime bytes·launcher/fence 계약 검사는 그대로다. 새 트리에서 leaf 누락, 무관한 파일 추가/삭제/변조, reparse는 거부한다.

`PINNED_LEGACY_LABEL_TRANSITION_MODE` 전환은 `3f4574a` → 과도기 모드 판(w9lmtransit)에만 적용한다. `Label_Match`·`deferred_intent_capture`·`direct_sync_push`·`label_completion`·`label_recovery_schema`·`label_workbench_context`·`logistics_runtime_profile`·`terminal_operation_lease`·`ui/workflow_snapshot_adapter`·`ui/workflow_view_state` 설치기에 선언한 모듈의 LF-normalized SHA256 전/후 쌍과 leaf `app/label_transition.py` 한 파일 추가만 허용한다. writer pin 파생·membership, 나머지 파일 AST·runtime·계약 검사는 그대로이며 역방향·다른 전/후 판·그 밖의 추가는 거부한다. 후보가 `3f4574a`의 저장 상태를 그대로 읽으므로 데이터 변환은 없다.

`PINNED_SCAN_INPUT_FIX` 전환은 `c83c72c`(w9lmtransit 최종)·`817d393`(w9lmscanmsg 1733만 넣은 판, TEST1 후보)·`92abdaa`(w9lmscanmsg) → w9lmp1fix 판에만 적용한다. `app/Label_Match.py`의 LF-normalized SHA256 전/후 쌍 세 개(`27c03a98…b1f1b5`·`78968d19…a261a`·`b30d80d4…51813d` → 후보)와 세 판에서 같은 설치기 companion 목록의 정확한 전/후 쌍만 허용하고, writer pin 파생·membership과 나머지 파일 AST·runtime·계약 검사는 그대로다. 저장 형식은 같아 데이터 변환이 없다(capture 상태 간선 guard의 OPERATOR_REVIEW→CANCELLED는 정상 initialize가 다시 만든다). 역방향은 거부한다. 검토된 후보의 문구나 파생 writer inventory pin이 바뀌어도 지원 preimage·파일 목록은 유지하고, legacy/scan-input/nextfix/scanner 및 current-user 선언의 후보 쪽 digest만 실제 LF 소스에 맞춘다. 목록 밖 변경은 계속 거부한다.

별도의 `PINNED_BOOTSTRAP_INTEGRITY_ORDER_FIX` 전환은 `d0e504e`/`0cf5bf6`의 `app/current_user_onboarding.py` LF-normalized SHA256 쌍 `bfd59326…20a57f` → `02bcbd5b…a73ab7`에만 적용한다. 양쪽의 파생 writer pin·동일 membership/guard와 모든 나머지 파일·runtime·계약을 검증한다. 기존 설치본의 bootstrap 기록 검증이 전환 판정보다 먼저 실행되고 실패는 fence·snapshot·교체 전에 중단한다. 변경은 기존 v1의 읽기 검증뿐이므로 mutable data 변환·기존 record 재작성은 없으며, 후보 record 생성과 기존 코드/record 보존은 원래 placement transaction이 담당한다. 역방향은 지원하지 않고 실패 transaction의 검증된 preimage 복원은 유지한다.

portable build는 clean source 확인 후 commit/tree를 고정하고 Git blob snapshot에서 저장소 파일을 복사한다. 소스의 줄바꿈·checkout filter가 산출물 bytes를 바꾸지 않으며 shared pin·import closure·zero-PE gate도 그 snapshot/산출물을 검사한다. 외부 CPython·고정 third-party와 명시 입력으로 생성하는 update-key config/manifest는 이 저장소 bytes 계약과 구분한다. bundled `assets/Item.csv`와 `config/app_settings.json`은 code tree 교체 대상이지만 사용자 catalog cache·설정·업무 data/queue는 별도 경로로 보존한다. 데이터 파일의 전이 예외나 일반 줄바꿈 정규화는 guard에 추가하지 않는다.

유효한 stop marker가 마지막 정상 제거 보고서와 다른 경우 설치기는 계속 거부한다. 같은 사용자의 `--remove-current-user-setup`은 writer admission 아래 기존 marker의 정확한 bytes/hash를 predecessor로 보존하고 relay·persistence·task 부재를 확인한 뒤 successor의 request/hash에 결속된 `PASS_DATA_PRESERVED` 보고서를 쓴다. 이 정상 쌍은 healthy lifecycle의 marker 조건을 충족하지만 나머지 identity/credential/ownership/무결성 조건을 대신하지 않는다. 손상·비정규 marker, lineage 한도 초과 또는 부재 확인 실패는 정상 제거 성공이 아니다([운영](operations.md#abnormal-stop-marker-recovery)).

0.3.1 reader는 원 parser의 배열 형태를 보존한다. PS5 `[manifest]`, 양 엔진의 중첩·빈 배열은 원본처럼 거부하고 PS7의 기존 singleton 수용을 유지한다. 0.3.0의 두 실패 원본과 0.3.1 회귀 결과는 [X13-B](operations.md#shared-powershell-x13b)에 연결한다.

canonical/placement 설치기는 자체 bootstrap으로 source 또는 portable `app/` root와 상위 경로의 reparse를 거부하고, 고정 0.3.6 consumer pin → manifest bytes → PowerShell leaf SHA256 순서로 확인한 뒤 dot-source한다. bootstrap 무결성 helper는 `-SharedCodeRoot`를 받은 inventory 소비자에서만 shared를 초기화하며, 초기 native SHA는 module auto-loading 없이 동작한다. 신뢰 확인에 shared 함수나 준비 전 Python checker를 사용하지 않는다.

Manifest는 기존 required-file 순서 → bounded parse → LM schema/field/hash → CPython signature 순서를 유지한다. `PortableInventory`의 domain framing·critical 3파일·ordinal UTF-16 정렬, task migration/conflict receipt·code/current-user 복원 정책은 앱 소유다. 고정 UAC helper closure는 기존 3개 helper 외 manifest/lock/leaf도 복사·검증·쓰기 거부하며, source 교체 후에도 같은 closure로 검사한다. [검증 범위](operations.md#shared-powershell-x13b).

## 고정 renderer core

`kmtech_shared` 0.3.6의 9파일은 앱 안에 바이트 그대로 고정 복사하며 `kmtech_shared.lock.json`이 manifest SHA256을 고정한다. 기존 factory `contract.lock.json`과 독립이다. `kmtech_zero_pe`의 `RasterImage`/`RasterCanvas` class와 import 경로는 facade로 유지하고 모든 image factory는 앱 class를 반환한다. PNG signature·bytes·GDI 픽셀·QR은 보존하며 제품 PNG 출력은 기존 PHS 교환·복구의 writer admission 아래에 있다. core에서 별도 writer 권한이나 앱 상태를 만들지 않는다.

자료 세대 판정의 공개 여섯 필드·순서·문구는 그대로다. UNKNOWN의 형식·코드 좌표만 담은 `private_cause`를 `DataGenerationError`의 비공개 속성으로 보존하며 공개 투영에 copy/pickle을 쓰지 않는다. `diagnostic_log.py`는 배포에 포함만 하고 런타임 import·호출·새 로그 저장 경로를 연결하지 않는다.

## 고정 runtime core와 앱 transaction

`producer_runtime_client`는 공개/기존 private 함수명을 유지하며 25개 leaf·binding·SQL 연산을 `kmtech_shared.runtime`에 위임한다. `_cng_jwk_thumbprint`, `normalize_public_jwk`, `new_runtime_identity`는 호출 시점의 앱 callback으로 전달한다. native identity 생성·TLS/timeout/endpoint·connection·writer admission과 `ensure_runtime_authority`/`prepare_runtime_metadata`의 예약·재시도·만료 판단은 앱 소유다. endpoint/producer/key/install scope, wire JSON/HMAC·nonce, 검증 오류·terminal redaction은 유지한다.

core는 connection이나 commit/rollback을 소유하지 않는다. 실제 relay `_set_relay_status`의 ACK 변경과 다음 token 저장은 같은 열린 SQLite transaction에서 commit하며, 중단 시 둘 다 rollback한다. LM의 명시 reviewed 복구는 아래 정책 그대로 앱에 남고 자동 만료 초기화 정책으로 대체하지 않는다. [검증과 한계](operations.md#shared-runtime-x05b).

W5-S0의 scope facade·ensure·prepare는 같은 credentials/install 식을 named 인자로 전달하고, profile/legacy factory는 기존 `PackageClientConfig`를 `config=config`로 전달한다. signature·positional 호출·기본값·profile 선택/검증 순서는 유지한다. runtime DB의 endpoint/producer/key/install 해시는 물류 authority scope와 별개이며, 새 관리 필드·scope migration·queued metadata/command/receipt/HMAC 재작성은 없다. [전후 parity](operations.md#named-context-w5s0)를 따른다.

<a id="lm-lease-dead-renewal"></a>
## runtime lease 격리 자동 회복 · 2026-09-25

저장된 갱신 요청(pending, `runtime_fence` 포함)을 같은 issue key로 다시 보내 `STALE_RUNTIME_FENCE`를 받으면 그 fence는 다시 ACTIVE가 되지 않는다는 서버의 확정 답이다. relay 행에 묶인 권한(`assigned_relay_id`)이 없으면 `ensure_runtime_authority`와 `prepare_runtime_metadata` 모두 기존 만료 전이(`_replace_expired_identity`: 새 runtime 신원·새 key의 증명 없는 발급)로 같은 주기에 다시 요청한다. 교정 전 판본이 남긴 같은 조건의 `OPERATOR_REVIEW`도 첫 주기에 풀린다. fence 번호(max+1)와 살아 있는 복제본의 `EXACT_CLONE_RUNTIME_CONFLICT` 거절은 서버가 정하며, 응답을 잃은 갱신은 같은 key 재전송이 저장된 grant를 돌려받는다. 증명 없는 발급이 `EXACT_CLONE_RUNTIME_CONFLICT`로 거절되면(재설치·relay DB 손실 뒤 옛 lease가 살아 있을 때) 서버가 그 거절을 key에 고정하므로, 묶인 행이 없을 때 같은 만료 전이로 다음 주기에 새 신원·새 key로 한 번 다시 요청한다(Rework b159d81과 같은 규칙, w9calmenforce). 거절된 key는 다시 보내지 않는다. 옛 lease가 ACTIVE인 동안에는 서버가 계속 거절하고(주기마다 요청 1회·서버 격리 감사 1건) 그 lease가 끝난 뒤 첫 주기에 max+1을 받는다. `STALE_RUNTIME_REQUEST_TOKEN`·묶인 행은 자동으로 풀지 않는다(자료 경로는 [아래](#lm-lease-dead-fence-datapath)). 자동으로 풀리지 않는 lease 검토는 relay 상태 파일의 `runtime_lease.status=operator_review`로 남고, 작업 화면 하단 오른쪽이 `작업 계속 가능 · 서버 반영 멈춤 · 관리자에게 알려 주세요`를 보인다([서버 반영 상태 표시](#lm-relay-screen-status)). 서버 계약·`kmtech_shared.runtime`은 바꾸지 않는다. [producer_runtime_client](../../producer_runtime_client.py)

<a id="lm-relay-screen-status"></a>
### 작업 화면 서버 반영 상태 (w9lmdistatus, w9lmdistatus2)

작업 화면 하단 오른쪽은 5초마다 두 원천을 읽어 더 나쁜 쪽을 보인다. ① relay DB(`queue/direct_sync_relay.sqlite3`, 읽기 전용, 매 주기): 아직 풀리지 않은 lease·행. ② relay 상태 파일(`direct_sync_relay_status.json`, 바뀐 경우에만 읽고 읽다 깨지면 직전 기록 유지): 마지막 relay 주기의 판정. 저장(enqueue)·설치 baseline·scan 만의 기록은 relay 판정이 아니므로 ①이 정한다. 그래서 저장이 상태 파일을 다시 써도 lease 검토·응답 없는 lease 요청은 지워지지 않는다.

- `회사 서버 전송 정상`: relay 판정이 `idle`·`acked`(또는 판정 없음)이고, 현재 자격 범위의 lease 행(가장 최근 갱신 행)이 만료 전 `ACTIVE`(요청 대기 없음; 행이 전송 중 잡고 있어도 만료 전이어야 한다) 또는 `LEGACY_DISABLED` 이고, 검토·영구 실패·재시도 행이 없을 때만.
- `회사 서버로 다시 보내는 중`: 응답 없는 lease 요청(`PENDING`·요청 대기), 만료된 lease(`EXPIRED`, 갱신 없이 `expires_at` 지남), lease 행 없음(첫 relay 주기 전), relay DB 없음, `retry_wait` 행, relay 판정 `retry_wait`·`runtime_error`·`enqueue_error`·queue 역압, DB 가 잠시 열리지 않을 때.
- `작업 계속 가능 · 서버 반영 멈춤 · 관리자에게 알려 주세요`(경고색): lease `OPERATOR_REVIEW`, 검토·영구 실패 행, relay 판정의 lease 검토·기존 행이 막은 원본·일시중지·디스크 부족·모르는 상태, 모르는 lease 상태, 읽을 수 없는 relay DB.
- 상태 파일과 relay DB 가 둘 다 없으면 아무것도 표시하지 않는다(relay 미설정).

'최근 성공'의 문턱은 서버가 준 lease 만료 시각이다. relay 는 매 주기 lease 를 확인하고 만료 120초 전부터 갱신하며 요청 TTL 은 900초다. relay 가 멈추면 늦어도 마지막 갱신 900초 뒤 `재시도 중`으로 바뀐다. 표시만 하며 스캔·저장·전송·lease 판단은 바꾸지 않는다. [Label_Match](../../Label_Match.py) `_label_match_relay_durable_state/_label_match_relay_screen_state`

<a id="lm-lease-dead-fence-datapath"></a>
## 자료 경로 dead fence 회복 · 2026-09-26

행이 묶은 fence N이 끊김 중 만료되면 observe 서버는 같은 본문 재전송의 원본을 커밋하고 `runtime_lease={contract_version, observed_rejected, STALE_RUNTIME_FENCE}` 영수증을 준다. 커밋된 `observed_rejected` 영수증은 observe 서버만 주므로(enforce 서버는 409 미커밋) 클라이언트 모드는 보지 않는다(등록 도구의 자격증명에는 `runtime_lease_mode`가 없어 기본 enforce, w9calmenforce). 2xx·기존 수락 검증(identity·shape·totals, `errors=0`, `quarantined=0`)과 contract_version이 맞으면 행을 ACK하고(`_local_runtime_lease_status=dead_fence`), 같은 transaction에서 권한이 이 행·같은 runtime 신원·같은 fence에 ACTIVE로 묶여 있을 때만 기존 만료 전이(`_replace_expired_identity`)로 푼다. 다음 liveness가 증명 없는 발급으로 fence N+1을 받고, 살아 있는 복제본이 있으면 `EXACT_CLONE_RUNTIME_CONFLICT` 검토로 멈춘다. 묶음이 다르면 행·권한 모두 지금처럼 `STALE_RUNTIME_FENCE` 검토이며 아래 수동 복구 대상이다. `STALE_RUNTIME_REQUEST_TOKEN`·검증 실패 영수증·enforce 409(미커밋)·전송 오류와 lost-ACK(같은 본문 재전송)는 그대로다. 같은 원본을 새 fence로 다시 보내지 않는다(observe 서버는 409 `idempotency_conflict`). 교정 전 판본이 이미 검토로 보낸 행·권한은 아래 수동 복구를 쓴다. 서버·`kmtech_shared.runtime` 불변, writer inventory 해시는 `direct_sync_push.py` 내용 변경으로만 바뀐다(sink 증감 0). [direct_sync_push](../../direct_sync_push.py), [producer_runtime_client](../../producer_runtime_client.py)

## Committed stale-runtime review의 명시적 복구 · 2026-09-12

기존 `ack-reviewed --recover-expired-runtime`만 독립 검토된 committed `STALE_RUNTIME_FENCE` 수신의 local ACK와 만료 authority 재개를 같은 SQLite transaction에서 처리한다. 기존 source/request/hash/byte/count·receipt 검증을 유지하고 실제 보존 spool도 대조한다. 단일 authority의 scope/install·runtime ID·fence·lease ID·expiry와 terminal public key/token audit digest를 결속하며, future expiry·다른 review 원인·assignment/pending/token·다른 미종결 runtime-bound row가 있으면 변경 없이 거부한다. 현재 producer credential·원 relay/metadata/spool/receipt는 바꾸지 않는다.

필수 audit 경로에 이전 authority와 receipt/metadata hash의 PREPARED 근거를 먼저 fsync한다. 실패하면 ACK와 authority 변경을 rollback하며, 성공 시 authority의 status만 EXPIRED로 바꾸고 원 lease/fence/evidence를 남긴다. 후속 정상 `ensure_runtime_authority`가 기존 expiration 경로로 새 ephemeral runtime과 인증된 forward grant를 취득한다. 이 로컬 명령 자체는 HTTP 요청이나 원 source 재전송을 하지 않는다. 재호출은 이미 ACKED인 행을 거부해 authority를 다시 초기화하지 않고, 정상 acquire의 실패는 기존 persisted issue idempotency로 재시도한다.

`observed_rejected`를 정상 회전으로 인정하지 않으며 `retry-dead`의 review 제외와 다른 fail-closed 조건을 유지한다. 원 APP_CLOSE raw 승인·fence 거부와 projection 의미도 바꾸지 않는다. [실제 실패·호스트 교정과 실행 경계](operations.md#committed-stale-runtime-recovery-20260912).

## 일상 화면의 정보 경계 · 2026-09-12

`이 PC의 최근 기록`은 현재 이력표의 최근 30행에서 진행·통과·오류 결과를 그대로 표시한다. 중앙 receipt나 완료 marker를 조회해 선별한 목록이 아니며 중앙 포장 확정·producer 수신을 입증하지 않는다.

durable PHS2 capture가 완료된 입력만 scan Entry에서 지운다. serial lane이 Entry를 비활성화한 동안에도 지울 수 있도록 위젯 상태를 잠시 전환하고 즉시 원래 상태로 돌린다. 실패·busy·미수락 입력과 F-key/focus 조건은 보존한다.

대기 화면의 read-only SQLite snapshot은 deferred capture와 실제 `package_command_outbox`를 함께 조회한다. 원 `state_counts`와 package status는 그대로 노출하며 operator 집계에서만 같은 set·정확한 downstream ref의 `SUPERSEDED` handoff를 중복 제외한다. package PENDING은 전송 대기, SENDING은 결과 확인, CONFLICT는 관리자 확인이며 ACKED도 로컬 completion marker가 있어야 완료로 센다. 지원 복구에서 이미 dismiss된 prewrite CONFLICT는 종결-미완료로 표시하며 관리자 확인/최장 대기를 다시 열지 않는다. 실제 대상이 없는 SUPERSEDED와 CANCELLED는 미완료로 남긴다. producer relay의 별도 settlement를 이 집계로 판정하지 않는다.

`작업 상세 보기/닫기`는 위젯 가시성만 바꾸며 accepted raw, 원 PHS2, 수량, 명령·receipt·lease·복구 상태나 F-key 허용 조건을 바꾸지 않는다. 표준 PHS2의 제품 수량은 `package_source_snapshot.member_count`를 사용하며 snapshot이 없거나 `None`인 교체 후 재조회 구간에만 검증된 `sealed_transfer.QT`를 사용한다. 기존 logistics 정수 검증으로 bool·실수·문자열·0/음수·누락을 거부하고, 양쪽 근거가 있으면 같은 양수 정수인지 확인한다. 명시적 invalid snapshot을 과거 seal로 대체하거나 소수를 잘라 수량으로 표시하지 않는다. 근거가 없거나 상충하면 수량 확인 상태를 표시하며 스캔 횟수나 이력 행 수를 수량으로 대체하지 않는다. 중앙 대기·관리자 확인과 로컬 완료의 의미는 그대로 유지하며 접수 ID·상태 코드·선행조건 identity만 상세로 이동한다. 긴 상세는 읽기 전용 스크롤 영역에 보존한다. [한정 근거](operations.md#routine-details-20260912).

## S05 단순화의 계약 경계

호출되지 않는 `_deferred_operation_lease_evidence`와 `_payload_entropy_from_row` 삭제는 lease evidence·payload 저장 형식을 변경하지 않는다. 실제 검증·암호화 경로의 `payload_protection_entropy`, `common_reader_v2_entropy`, owned payload reader와 F1 cancellation transaction은 유지한다. 사용되지 않는 catalog 경로 판별 함수도 삭제하며 실제 authenticated cache 선택·복구는 유지한다.

공개 취소/APPLIED payload의 exact equality는 기존 테스트에 남고, 그 equality가 이미 포함하는 문자열 부재 assertion과 같은 `receipt_json`의 두 번째 비교만 제거한다. private seal/receipt 보존, durable write 순서, 저장 실패 후 같은 intent 재시도·no-repost 사례는 유지한다. writer inventory는 같은 44개 identity/guard에 새 소스 위치·해시를 결속하며 Python/PowerShell pin을 함께 갱신한다([실제 소스 검증](operations.md#s05-simplification)).

[제품·기능](README.md) · [운영·복구](operations.md) · [백로그](BACKLOG.md) · [중앙 통합](../../../Program_Spec_Hub/INTEGRATIONS.md) · [공통 용어](../../../Program_Spec_Hub/GLOSSARY.md)

최초 기준일·후속 소스 종결·증거 적용 범위는 [README 기준](README.md#evidence-scope)과 같다. 다음은 2026-09-07 source-backed 정적 계약 대조이며 실제 설치본의 호출·권한·서버 설정·화면 반영 입증이 아니다. `L`은 `/logistics/api/v1`이다. 서버 상세 계약을 복제해 새 정본으로 만들지 않고 클라이언트의 소비·검증 책임을 기록한다.

앱은 업데이트 후보·서명/manifest·archive 검증을 유지하며 코드 배치는 외부 installer만 담당한다. 도달 불가 self-update apply/batch/prompt와 전용 시험은 제거했고 receipt·writer·복구 계약은 보존한다.

5단계(현품표 + 제품 3개 + 최종 라벨), `CLC|SPC|PHS`, 생산일자 `6D` 호환 입력은 **확장 축 ②(제품·양식) 정책 adapter 대상이며 현장 발생 확인 전 제거 금지**다. 현행 입력·검증 코드를 보존하고 이후 adapter 이행도 중앙 PHS2의 멤버십·수량·입력 순서 계약을 유지한다.

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

relay는 시험 시작 때도 자기 SID를 상태에 남겨 확정 뒤 재사용할 수 있게 한다. 선택적인 SID/steady 캐시 읽기가 실패하면 기존 첫 시작·nonce 준비 증명을 유지하고 관측 길을 쓴다. 창 뒤 대조는 시작 때 받아들인 SID와 판에 결속한다.

READY인 사용자 설정·등록 자료가 존재하고 설치 주소(등록 보고서에 있으면 그 주소)·운영 SID·현재 active 판·steady 전이(CONFIRMED/ROLLED_BACK/DEFERRED)가 맞으며, instance lease 점유·stop/검증 보류 표지 없음·RUNNING 상태의 worker/app/direct-sync 일치·살아 있는 PID·90초 이내 갱신·현재 code_root/source_commit·일반 사용자 표시가 모두 맞으면 `FAST_WINDOW_FIRST`로 창을 먼저 연다. 창 앞은 파일 확인과 짧은 writer admission으로 보류 표식 원자 기록 한 번이며 PowerShell/고정 helper 호출은 없다. 첫 실행·자료 미준비·relay 없음/오래됨·시험 전이·업데이트 대기·기존 검증 보류는 `FULL`/`FIRST_RUN`의 전체 확인을 따른다.

공유 `control/startup-verification.json`은 GUI PID·native 생성 시각·시작 시각·start_path만 담는다. 단일 GUI 소유권을 얻은 시작만 창 앞에서 표식을 기록한다. 중복 실행은 기존 창을 앞으로 내고 기존 송신·표식을 유지한다. GUI DirectSync 입구와 relay enqueue·각 HTTP 송신 직전은 같은 판정 함수를 사용한다. 포장 생성·취소·교환·PHS 복구와 PIN·이름 등록도 새 서버 변경 요청 입장 전에 같은 게이트를 확인한다. 보류면 로컬 outbox는 대기 상태를 유지하고 동기 작업은 「잠시 뒤 다시」를 안내한다. 창 뒤 lifecycle Verify 한 번(고정 helper의 bootstrap 확인 포함)·Python bootstrap 무결성 한 번·relay Observe 한 번이 성공할 때까지 새 송신을 미루고 로컬 스캔·확정·저장과 기존 대기열은 보존한다. 이미 입장한 HTTP 한 건은 응답과 기존 로컬 반영을 마친다. 느린 helper·바이트 확인/해시는 writer admission 밖에서 실행하며, 입력 바이트가 동일할 때만 프로세스 내 검증 결과를 재사용한다. 입력·전이·delegation이 달라지면 기존 담장으로 거절한다.

FAST 품목 준비는 시작 때 전달받은 보고서를 고정 사용하며 배경 확인이 전역 상태를 비워도 중앙 목록 새로 고침을 수행한다. FULL과 같은 과도기 오프라인 fallback·미확인 표기를 사용하고, 과도기 허용이 없으면 UI 스레드에서 같은 원본 재시도를 안내한다.

확인 중 화면은 「서버 보내기 확인 중」, 실패는 「서버 보내기가 멈췄습니다. 앱을 껐다 다시 켜 주세요(계속되면 관리자에게)」와 기존 reason/cause code를 남긴다. 같은 하단 위치의 「다시 확인」은 창 뒤 전체 확인을 배경에서 한 번 더 수행한다. 확인 중에는 단추를 비활성화하고 연타도 한 번만 입장한다. 성공하면 기존 동일 표식만 해제해 송신을 재개하고, 실패하면 표식·자료·대기열을 보존한다. STARTUP_ 보류 PIN 안내에는 「이 건만 보류 — 틀린 횟수로 세지 않음」을 덧붙인다.

실패·지연·소유 GUI 종료·PID 재사용·소유 관측 UNKNOWN·읽을 수 없는 표식은 자동 해제하지 않으며 다음 시작은 FULL이다. FULL 확인과 단일 GUI 입장이 끝난 뒤 시작 때 읽은 동일 표식만 지운다. READY/onboarding 또는 startup trace 최상위에는 start_path, relay_start.status, existing_relay_reused, fast_path를 남긴다. relay 자식의 WRITER_GATE_TIMEOUT은 처리되지 않은 예외 대신 DEFERRED로 기록하여 다음 바퀴에 재시도한다.

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

스캔 0인 자기 `BLOCKED_INVALID` 접수도 안전한 작업 화면에서는 F1 키·현재 세트 취소 단추로 같은 transaction을 실행한다. 안내의 F1 문장·키·단추는 같은 앱 술어를 사용하며 초기화·이력 로딩·읽기 전용·오류·완료·UI lane 처리 중에는 허용하지 않는다. `OPERATOR_REVIEW`·다른 세트 접수·검증 대기·검증 완료·로컬 반영 중 접수는 이 예외에 포함하지 않는다. 취소된 원본을 보존하고 CA 봉인 뒤 같은 현품표를 다시 스캔하면 새 접수로 검증한다.

확정되어 로컬에 보존된 미소비 PREFETCHED lease의 존재·만료는 로컬 접수 취소를 일괄 금지하지 않는다. 해당 lease, ACTIVE issue attempt, 기존 payload·검증 이력·서버 reservation·receipt는 바꾸지 않으며 취소는 포장 완료·중앙 lease release·재고 반환이 아니다. 과거 mutation은 정확한 request/hash/idempotency와 검증된 발급 결과를 보존 lease에 결속해야 하고, actual pending/UNKNOWN·live claim·package outbox·미완료 F4/F5·UI in-flight guard는 유지한다. 기존 fingerprint/resource별 lease 선택과 같은 원본의 F4 차단도 그대로다. 재구성 rw13 fixture의 별도 foreground marker/credential을 Label 업무 요구로 추가하지 않는다.

commit 뒤 cache 삭제 전 crash에서는 exact CANCELLED 접수의 stale cache를 복원하지 않고, 늦은 VALIDATED callback도 현재 DB state를 다시 확인해 materialize하지 않는다. 저장 실패·identity/CAS 불일치 때 화면/접수는 보존한다. [host 회귀와 구분한 native 범위](operations.md#f1-cancel07-qualification)는 별도 판정이며 이 계약은 실제 중앙 reservation 해제를 입증하지 않는다.



실제 원래 세트의 cancel07 수용은 정상 F1 한 번의 VALIDATED/v7 → CANCELLED/v8/TC_CANCEL 1건과 정상 GUI 재시작 후 미복원을 확인했다. [local full-row/payload 보존](D:/KMTech/cold-program-material/from-E/KMTech/label-install-qualification-20260908/CANCEL07-CUSTODY-AFTER-RESTART.json) 및 [중앙 10테이블/19기록 무변경](E:/KMTech/web-integration-20260908/label-original-f1-central-comparison-01.json)은 원래 lease/ACTIVE attempt/reservation/receipt 보존 근거다. 이 결과를 새 case04 F4/F3·중앙 포장 확정 또는 최종 artifact lifecycle PASS로 확장하지 않는다([실제 환경·관측 한계](operations.md#f1-cancel07-native)).

<a id="quantities"></a>
## 수량·시각·로컬 파일

저장 루트는 onboarding·GUI/guard·relay의 공통 resolver로 선택한다. custom → env를 우선하며, 둘 다 없으면 등록 수명주기(진입·진행 중·등록 직후 환경 적용·relay 시작·재시작)와 기존 onboarding 상태에서 항상 LOCALAPPDATA(A)를 쓴다. ProgramData(P) 업무 파일 유무는 이 선택에 영향을 주지 않으며 P fallback은 onboarding 문맥·상태가 전혀 없는 순수 standalone 실행 전용이다([현재 감지 규칙](operations.md#configuration)). 등록 후에도 P 데이터를 계속 쓰려면 운영자가 `custom_save_path`를 수동 설정해야 한다. 신규 custom onboarding/ledger는 C로 통일하고, 기존 C의 업무 CSV/DB와 A의 onboarding ledger가 갈린 설치는 각각 보존·기본 로그 수준에서 진단한다. 설정 자동 기록·데이터 이동/병합·identity 재등록을 하지 않으며 producer/key/endpoint에 결속된 queue·spool의 원 bytes와 미확정 상태는 경로 정리를 이유로 변경·종결하지 않는다.

| 값 | 단위·모집단·포함/제외 | 시간·집계·중복 |
| --- | --- | --- |
| source `member_count` | 현재 유효 TRANSFER/work-group 제품 구성원 수. PHS2 한 번의 스캔 수와 다름; `INHERIT_ALL`은 그 집합 전체 | 현재 source snapshot/version 기준. unit와 barcode hash를 각각 대조 |
| QA 표본 3개 | 명시 호환 경로의 검증 표본. 포장 전체 멤버십 수나 중앙 표준 필수 입력 수가 아님 | 표준 PHS2 입력은 1회; 표본을 제품 총량으로 합산하지 않음 |
| F4 교체 목록 | 1쌍 이상 실제 대상 멤버 수 이내. 3쌍 이상은 추가 capability 필요 | 명시적 단일 제출·중앙 원자 처리·같은 key 재생. 전체 구성 수 보존 |
| 포장 세트 | `PACKAGING_SET_COUNT`, 웹 `total_sets_completed`. 포장 완료 세트 수이며 제품 개수와 환산하지 않음 | 취소·부분 제출 등을 제외하는 projection 규칙. 원본 PHS2/최종 라벨은 제품 barcode에서 제외 |
| CSV row / accepted row | event 전달·투영 처리 건수. 포장 세트나 제품 개수와 다름 | 중복·quarantine·처리 행 합계와 source identity로 ACK 검증 |

근거: [package draft/command](../../package_logistics.py), [서버 Label event normalize](../../../WorkerAnalysisGUI-web/common_projection_sync.py), [포장 projection](../../../WorkerAnalysisGUI-web/common_projection.py)의 `_label_match_payload_passed` 및 포장·취소 집계, [dashboard_standard.source.js](../../../WorkerAnalysisGUI-web/static/dashboard_standard.source.js) `total_sets_completed` 표시. `Pcs/EA/piece`의 전 제품 공통 환산은 미확정이며 [LM-B06](BACKLOG.md#lm-b06)·[공통 용어](../../../Program_Spec_Hub/GLOSSARY.md)에서 관리한다.

`DataManager.log_event`는 timezone 없는 로컬 `datetime.now().isoformat()`을 기록한다. 서버 Label transport 시간대는 `Asia/Seoul`이며 lease `operation_completed_at`은 UTC `Z`다. 생산일, 포장 업무일, event 발생, 수신, projection, 화면 조회 시각은 별개다. 날짜 변경은 outbox 만료 기준이 아니다. 근거: [DataManager/lease](../../Label_Match.py), [서버 시간대·포장-출고 집계](../../../WorkerAnalysisGUI-web/common_projection.py). PC 시계차 허용치·업무일 cutoff·표시 지연 목표는 [LM-B02](BACKLOG.md#lm-b02)의 미정 요구다.

로컬 미완료 세트도 날짜만으로 삭제하지 않는다. 이전 날짜의 current-state 원 바이트를 SHA256 사본으로 먼저 보존한 뒤 같은 날의 기존 검증으로 복원한다. 세트 ID·원 스캔의 일련번호/현품표 날짜·시작 시각은 보존하며 오늘 집계는 기존 완료 event 날짜를 따른다. 검증 실패나 비중앙 timestamp 해석 오류는 기존 UNVERIFIED 보류 snapshot과 별도 원본 파일에 내구 저장·readback한 뒤에만 current-state 자리를 비운다. 보관 실패는 기존 복구 잠금으로 새 저장을 막는다. 중앙 내구·과도기 예외와 완료/원장/전송 계약은 유지한다.

정상 복원이 끝난 뒤 SHA가 맞는 `.resume-<SHA>` 사본은 같은 세트별로 이번 사본과 최신 사본을 합쳐 3개만 남긴다. 다른 세트, 보류와 `.held-<SHA>`, 그 보류에 연결된 사본은 정리하지 않는다. 읽기·삭제 실패는 자료를 보존하며 복원을 막지 않는다.

로컬 emitter CSV는 UTF-8 BOM(`utf-8-sig`), 헤더 `timestamp,worker_name,event,details`이며 details는 JSON이다. 완료 등 durable event에 flush/fsync를 적용하고 writer 오류를 `flush/close`에서 반환한다. 현재 세트 JSON은 임시 파일 flush/fsync 후 `os.replace`한다. 이 파일과 SQLite outbox·lease가 하나의 파일 transaction인 것은 아니므로 복구 절차로 경계를 조정한다([DataManager](../../Label_Match.py), [완료 정책](../../DIRECT_SYNC_DATA_PLATFORM_NOTES.md)). 일부 fixture의 `event_type/detail` 헤더는 emitter 원본 byte가 아니다. 서버 decoder의 alias 호환과 실제 emitter 동일성을 분리한다.

`event_stream_policy.LOCAL_ONLY_EVENT_TYPES`는 별도 로컬 이벤트 파일로 나뉜다. `PHS_LABEL_ACTIVE_RESOLVED`, `EXACT_RESCAN_STARTED/OK/COMPLETED`, 종료 취소 `APP_CLOSE_CANCELLED`와 `멈춤 요청 미룸` 같은 로컬 전용 event를 원격 실적으로 가정하지 않는다. 종료 감사 두 종류는 서버의 현재·구 catalog 모두 받지 않으므로 업무 CSV에 섞지 않고 로컬 기록을 보존한다. `CONTRACT_CANDIDATE_EVENT_TYPES`의 이름만으로 전체 전송 계약 수용을 증명하지 않는다([이벤트 정책](../../event_stream_policy.py), [DataManager._get_log_filepath_for_item](../../Label_Match.py)).

로컬 GUI writer의 기존 저장 위치를 보존하기 위해 [guard `resolve_data_scope`](../../label_match_single_instance.py)는 앱과 같은 nonempty `custom_save_path` → `LABEL_MATCH_SAVE_DIR` → ProgramData 순서로 소스 교정했다. null/빈 custom은 fallback하며 데이터 이동·schema/업무 identity 변경은 없다. 이름은 앞뒤 공백 제거 후 기존 `ntpath.normcase(normpath(path))`의 UTF-8 SHA-256 앞 32자리와 `Global\KMTech.LabelMatch.`로 만들고, callback 종료까지 handle을 보유한다. [SaveRoot13 실제 13 PASS/39 ordered phase PASS](operations.md#saveroot13-evidence)는 custom 우선 선택, missing/empty/whitespace/null × env 유무의 실제 writer resolver 대조, 단일 process의 native mutex 중복 callback 제외·해제 후 재진입을 입증한다. 실제 파일 alias/relative 경로의 동일성이나 settings 생성·변경 race를 해결한 것은 아니다. [onboarding의 guard 이전 ledger 선택](operations.md#storage-root-residual)은 여전히 OPEN이며, relay의 기존 custom CSV 발견 경로는 아래 C-05와 [한정 26 PASS](operations.md#relay-custom-root-evidence)로 좁게 교정했다. 두 GUI writer·packaged/source·pending identity 복구는 [LM-B10](BACKLOG.md#lm-b10)의 별도 미입증 수용 범위이며 과거 Modules3 PASS를 이 교정에 확장하지 않는다.

<a id="c-00"></a>
## C-00 공통 명령·권한

작업자 이름은 공용 명단의 ACTIVE 목록 또는 이 PC에서 내구 등록한 이름에서 선택한다. 「새 이름 등록」은 이름만 받고 승인·PIN은 요구하지 않는다. 등록 B(`stage_a=false`, 숨김 없음)의 저장·재시도·대응·번호 표시 계약은 [worker-roster-b.md](worker-roster-b.md)를 따른다. 조회는 기존 생산자 HTTPS/HMAC 신원으로 `POST /api/worker-roster/v2/query`에 `authority_scope_id`를 보내며, 404/405이면 v1 query의 `authority_scope_id, known_version`으로 호환한다. v1 같은 판의 `unchanged`는 검증된 같은 현장·같은 판 캐시가 있을 때만 수용한다. 조회·등록은 별도 배경 스레드에서 수행하며 스캔 내구 저장과 시작은 원격 응답을 기다리지 않는다. 쓰기 잠금 안에서 HTTP를 기다리지 않고 다른 시도·로그인·현장·명단 객체 또는 종료 뒤의 늦은 응답은 적용하지 않는다.

묶음 시작 때 선택한 `worker_id`와 trim한 `worker_name_snapshot`을 현재 세트·보류 snapshot·완료 draft·교환 pair·감사 intent에 선택 필드로 결속한다. 명단 없는 한시 선택의 번호는 null이다. 진행 중 snapshot은 이름 정정 뒤에도 고정하며 새 기록의 복구 귀속은 UUID로 대조하고 번호 없는 원본은 기존 이름 절차를 유지한다. 기억한 ACTIVE 이름은 선택 없이 시작하고, 중지·누락·이름 변경이 확인되면 다음 묶음부터 재선택한다. 진행 중인 묶음과 복구·보류·재전송은 원래 귀속으로 끝낼 수 있다. 명단과 캐시가 모두 없거나 정상 명단이 행 0개이면 기억한 이름으로 시작하거나 PC의 기존 최근 이름을 한시적으로 선택하고 안내한다. 목록도 없으면 관리자 등록 안내를 표시한다. 행이 있지만 ACTIVE가 0이거나 이름 방어로 모든 행을 버린 명단은 한시 선택 없이 관리자 등록 안내만 표시한다. trim 뒤 빈 이름과 Cc·Cf·Cs·Zl·Zp, 65자 이상, 경로 문자 \ / : * ? " < > |, 첫 글자 = + - @인 이름은 그 행만 버리고 정상 행은 유지하며 버린 수를 진단 기록에 남긴다. 형식·현장·번호 중복·cache digest 오류는 명단 전체를 거부한다. 명단 이름은 64자까지이고 최소 길이·언어 제한은 없다. PC의 기존 로컬 이름 검증과 파일·폴더·CSV 방식은 기준 그대로 유지한다. 설치 등록, 장비 권한, 보호 관리자 인증과 `operator.local_id` 계약은 유지한다.

`GET /logistics/api/v1/capabilities`의 `worker_record_identity_v1.enabled`가 JSON true일 때만 새 CSV details의 두 필드와 물류 payload의 `worker_identity`를 싣는다. 명단 조회의 배경 스레드에서 신호를 갱신하며 실패·누락·false는 기존 전송 형식을 쓴다. CREATE/교환은 첫 전송 전에 신호를 조회하고 command_json에 본문을 동결한다. 완료의 CSV 신호 결정은 current-state에 먼저 저장하며 lane 실패·저장 재시도·재시작은 같은 신원과 처음 결정을 복원한다. 감사 재생·ACK 유실은 저장된 본문을 그대로 사용한다. 로컬 번호를 승인 `operator.local_id`로 사용하지 않는다. 기존 CSV 열 순서·파일명·폴더명·수량/집계 단위와 내구→표시→배경 전송 순서를 유지한다. 새 칸 없는 상태·draft의 읽기와 fingerprint는 그대로다. 신호가 꺼진 로컬 전용 완료도 CSV 내구 저장 뒤 성공 표시 전에 `_worker_record_identity/<세트 ID SHA256>.json`에 당시 신원·교환·CSV 결정을 보존하며 활성 상태 정리 뒤에도 삭제하지 않는다. CSV 내구 저장 순간 완료된 세트가 되며 archive·outbox marker·lease 후속 저장 실패는 완료를 되돌리지 않는다. 완료 원 snapshot·CSV를 보존하고 같은 구성·신원·결정으로 재시도한다(재시작은 기존 CSV를 내구 readback). 완료 후 스캔·UNDO·교환·초기화는 거절하며 화면은 `완료됨 · 기록 마무리 중`과 저장 재시도를 안내한다. 미달 확정의 정상 UNDO는 후속 checkpoint 저장 뒤 다음 현품표 전까지 기존 예외대로 허용한다. 완료 전의 checkpoint는 불변 archive 검사로 봉쇄하지 않으므로 정상 UNDO·스캔 변경을 계속 받는다. 백업은 이 폴더를 현재 상태·보류·감사 조각과 함께 보존한다.

관리자 복구 CLI는 pc/producer/source-host/install ID와 expected-active-manifest-hash의 명시 입력을 후보 생성 전에 검사한다. 누락 안내는 필요한 옵션·서버 readback 출처만 포함하며 승인 토큰이나 기존 자격의 값을 출력하지 않는다. portable 등록기는 앱과 같은 `app`·`app/site-packages` 검색 경로를 사용하며 인증·권한 조건은 동일하다.

부분 등록의 명시 복구는 기존 root 일회용 승인·명시 identity·candidate manifest hash·TLS CA에 더해 현재 MachineGuid/SID로 유도한 install ID와 남은 파일의 해석 가능한 신원·소유 키를 대조한다. 공유 `KMTech.DirectSync.Possession.v1`은 기존 키 열기만 허용한다. current-user DPAPI journal에 경로·CA hash·신원·키·승인·request/commit ID를 결속하고 각 원본의 이동 의도·SHA를 먼저 내구 저장한 뒤 별도 슬롯으로 이동·재대조한다. 준비 자격을 보호 저장한 뒤 중앙 commit하며 ACK 유실은 서명된 status로 확인한다. 로컬 쓰기·승인 파일 정리 중단은 같은 transaction으로 재개한다. 새 root 승인은 같은 경로나 새 승인 경로로 전달하되, 기존 prepare ID가 있으면 신원이 결속된 status의 EXPIRED·ABORTED·COMMITTED만 전환을 허용한다. prepare ID가 없으면 별도 보호 후보를 먼저 저장하고 새 승인의 검증된 prepare 성공으로만 superseded를 확정한다. 서버는 root 발급 시 기존 pending 승인을 취소하고 RESERVED 발급·경쟁 PREPARED prepare를 거부한다. 실패·불명 응답은 terminal 증거가 아니며 기존 journal·원본을 보존하고 후보 request로 재확인한다. 이전 journal bytes/SHA·대체 시각·transaction 연결을 보관한 뒤 활성 journal을 전환하고, 이전 원본 슬롯을 이어받아 준비 자격은 새 슬롯에 저장한다. 다른 신원·사용자·PC·키·CA·출력 경로와 진행 중 승인의 교체는 거부한다. 검증된 committed response와 보호된 prepare를 조합한 receipt를 기존 등록 readback에 전달하며 READY나 첫 업무 ACK를 추정하지 않는다([지원 절차](operations.md#partial-registration-recovery)).

최초 등록·관리자 복구의 네트워크 승인 토큰은 선택 사항이다. 빈 토큰은 헤더/JSON 필드에서 생략하고 서버의 허용 IP 판정을 사용하며, 명시 토큰이 있으면 기존 토큰 인증을 유지한다. tokenless `enrollment_unauthorized`는 서버 HTTP 상태·오류 코드와 IP 등록/토큰 입력 안내를 CLI·등록 보고서에 남긴다. 관리자 복구의 별도 일회용 `recovery_token`·manifest·TLS·현재 사용자 소유 키 검증과 아래 업무 명령용 기계 자격증명은 유지한다([운영 절차](operations.md#enrollment-ip-policy)).

명령 envelope는 `contract_version=logistics-v1`, `command_type`, `authority_scope_id`, `authority_epoch`, `ledger_plane`, `plane_epoch`, `idempotency_key`, `expected_versions`, `payload`를 사용한다. payload의 source/package·membership·seal/lease evidence가 중앙 snapshot과 맞아야 한다. 임의 수량을 중앙 멤버십 대신 보내지 않는다. 클라이언트는 JSON boolean `ok` 등 응답 구조와 receipt의 identity·구성·버전을 검증한다([package_logistics.build_create_package_command/_data/_validate_receipt](../../package_logistics.py)).

HTTPS 요청은 Bearer 및 logistics token 헤더, `X-Logistics-Source-Host-Id`, `X-Logistics-Device-Id`, `X-Logistics-Program=Label_Match`, 명령 `Idempotency-Key`를 보낸다. 실제 값은 문서에 저장하지 않는다. 서버는 기계 토큰·device·authority scope를 검증하며 사람 이름 입력은 이 권한을 만들지 않는다. 같은 scope+key+fingerprint는 receipt 재생, 다른 fingerprint는 충돌이다. 근거: [클라이언트 `_request`](../../package_logistics.py), [서버 auth](../../../WorkerAnalysisGUI-web/blueprints/logistics/auth.py), [replay_or_conflict](../../../WorkerAnalysisGUI-web/logistics_ledger/idempotency.py).

프로필 선택·HTTPS·required mode와 DPAPI 경계는 [운영 설정](operations.md#configuration), 실제 installed provider/flags/권한 확인은 [LM-B04](BACKLOG.md#lm-b04)다. 현행 current-user 배포의 DPAPI·프로필과 기존 Machine anchor 우선권은 별도 구성으로 대조하며 공통 Machine profile 안내를 모든 설치본에 적용하지 않는다([릴리스 계약](../../RELEASE_GATE_CONTRACT.md), [profile resolver](../../logistics_runtime_profile.py)). API 존재와 운영 장비의 접근 성공은 별도다.

포장 건별 복구의 로컬 관리자 동작은 웹 정본 `admin-pin.v1`(Web `383e6c4`)의 `LABEL.F5_HOLD`, `LABEL.SET_HOLD`, `LABEL.RECHECK`, `LABEL.PHS_LOOKUP`(관리자 실물 조회만), `LABEL.SUPPORT_HANDOFF`를 쓴다. 일반 F5 조회와 F3 완료·outbox·ACK에는 PIN을 추가하지 않는다. 한 가림 대화상자에서 개인 `admin_id`와 6자리 PIN을 받고, 현재 `worker_name`의 앞뒤 공백 제거·NFC UTF-8 bytes를 `wn:<hex>`로 보내되 62 bytes 초과는 `wh:<SHA-256 hex>`로 보낸다. `login_epoch`는 시작·작업자 변경마다 새 메모리 값이며, 이 작업자 ID는 인증된 계정이 아닌 현장 세션 주장이다. 기존 producer 자격으로 HTTPS HMAC 서명하고, 원본/중앙 target readback→fsync intent/key→온라인 redemption→target·작업자 재대조→기존 writer 1회→key·verification ID가 결속된 효과 감사/readback 순서를 지킨다. 보류 행·파일 이동·화면 슬롯 해제 사이의 중단은 같은 operation key로 재개하고, 완료 상태와 결속 감사가 모두 readback된 뒤에만 APPLIED로 확정한다. APPLIED는 뒤따른 예외로 미확인 상태로 되돌리지 않고 같은 key의 완료 readback으로 재확인한다. 관리자 ID 공백·PIN 형식 오류도 action·target·operator가 있는 실패 감사로 남긴다. PIN/proof는 메모리에만 둔다. 응답 유실·불완전 200·변경·감사 실패는 성공으로 바꾸지 않는다. 잠긴 일반 관리자는 root가 웹에서, root 자신은 서버 CLI에서만 해제한다([복구 운영](operations.md#recovery)).

소비된 PIN key의 부분 효과는 같은 로컬 작업자·변하지 않은 원본이면 새 `login_epoch`에서도 같은 key로 완료하고 재개한 작업자 이름을 감사한다. 유효한 현재 파일의 원 bytes SHA256은 새 적용 target(세트 보류)과 redemption 전 같은 key의 감사(F5 연결 세트 포함)에 결속한다. PIN 보류는 현재 파일을 다시 저장하지 않고 검증된 원본 또는 메모리 상태를 내구 보류 행에 기록한다. 파일을 지우는 대신 같은 볼륨의 key 전용 보류 위치로 원자 이동한다. 원 경로·보류 경로·원 SHA를 이동 전에 내구 기록하고, 이동한 handle의 bytes를 원본과 대조한다. 재시작 때 보류 파일이 정확하면 같은 key의 효과와 readback을 이어서 완료한다. 다르면 새 활성 파일을 덮지 않고 원위치 복원한 뒤 변경 SHA·`TARGET_CHANGED_DURING_HOLD`를 별도 내구 기록하고 key 전용 미검증 위치로 다시 옮긴다. 원 PIN은 APPLIED로 만들지 않고 `QUARANTINED_CHANGED`로 닫으며 원 세트 보류는 유지한다. 관리자는 변경본을 별도 새 PIN으로 재조회·보류하거나, 활성 위치가 빈 경우 `LABEL.RECHECK`에 결속된 복원을 선택한다. 복원 후에도 원 세트 보류와 감사는 남는다. 변경본 행의 식별자는 `(operation_key, file_kind, source_path)`이며 current와 journal을 각각 추적한다. 기존 key 단독 행은 모든 상태·SHA·경로를 보존하는 한 SQLite transaction으로 이전한다. 시작 finalizer는 `RESTORED`와 활성 파일의 변경 SHA 일치·보류 파일 부재를 대조해 완료된 복원을 다시 이동하지 않는다. 불일치는 그 건의 미검증 상태로 표시하며 목록과 재복원 경로를 유지한다. 새 활성 파일이 있으면 두 파일을 모두 남겨 그 건만 보류한다. CURRENT 손상 파일과 F5 journal도 같은 방식으로 검증한다. 이동 기록 없는 고아 보류 파일은 삭제하지 않고 관리자 목록에 원인을 표시한다. 이동한 원본·활성 슬롯·연결 F5 journal과 완료 감사의 readback이 모두 성공해야 APPLIED로 표시한다. 문자열 오류는 보류 성공이 아니며 취소·보류 뒤 자동 PIN 재입력 창을 열지 않는다.

<a id="c-01"></a>
## C-01 중앙 품목 → 앱 표시

- Item Code는 정확한 dictionary key로 조회한다. [carrier·legacy·lookup 규칙 표](carrier_identity.md)의 반환·오류 차이를 유지한다.

- 방향/형식: 서버 `GET /inbound/api/item-catalog.csv` → `refresh_item_catalog` → 인증 cache/snapshot → 품목 표시. `Item Code,Item Name,Spec,Tray Image` 열을 사용한다.
- 권한/오류: 서버 reader 권한·cache 방지 응답, 클라이언트 중앙 등록 identity에 따른 인증 cache 복구. 등록 상태에서 인증 cache가 없으면 요청 실패를 bundled 파일 성공으로 숨기지 않는다.
- shared core 위임은 정확 4열 gate/오류 문구와 HMAC v2·sidecar 이름/bytes를 보존한다. program·URL 정규화·동일 authority 판정은 앱에서 전달하며 LM의 빈 port 거부와 profile 신원·진단 redaction 차이를 유지한다. authenticated read/write/recover와 snapshot 상태, `_atomic_write` seam 및 recovery→CSV→authority 내구 순서는 앱 소유다. 새 부재 판정 cache/index/flag나 공용 cache root는 만들지 않는다.
- 단위/효과: 표시용 품목 마스터이며 재고·수량 예약이나 포장 command가 아니다. 일반 명령 idempotency key/transaction/cursor가 적용되는 쓰기 경로가 아니다.
- 양쪽 근거: [item_catalog_sync.refresh_item_catalog](../../item_catalog_sync.py), [api_item_catalog_csv](../../../WorkerAnalysisGUI-web/blueprints/inbound/__init__.py). [LM-02](README.md#lm-02), [LM-B04](BACKLOG.md#lm-b04).

<a id="c-02"></a>
## C-02 앱 → 현재 포장 source 조회

- UI lane의 terminal callback은 아직 BUSY인 상태에서 실행된다. 정상 scan admission은 lane이 task ownership을 해제한 `on_idle`에서 다시 계산하고 기존 caller의 `on_idle`도 유지한다. initializing/history/notice/staged PHS2/close/query/exchange/fence 조건을 직접 풀지 않는다. 실제 `8425fe1` GUI에서 F3 후 회색 입력 필드가 남은 현상을 source callback 순서와 [causal 3 FAIL → 기존 lane/action-gate 97 PASS](D:/KMTech/cold-program-material/from-E/KMTech/label-install-qualification-20260908/idle-admission-green.xml)로 연결했으며, 별도 successor `eb79e519`의 [초기 native 입력](D:/KMTech/cold-program-material/from-E/KMTech/label-install-qualification-20260908/GUI04-F4-INPUT-PERSISTENCE.json)은 전체 139자 즉시/12초 후 일치다. 새 코드의 F3 후 입력 재개 수용은 아직 미실행이다.

- API: `GET L/bundles/resolve`, `bundle_role=PACKAGE_SOURCE`, `input_tag_id`(ITG), 품목·scope, `input_tag_label_id`(LBL)와 `input_tag_hash_prefix`(HSH). 물리 label 표시 문자열을 독립 조회 권위로 사용하지 않는다.
- 검증: LBL/HSH는 함께 공급하며 ITG가 필요하다. HSH는 16자리 hex다. 선택적인 barcode membership hash와 member count filter도 쌍으로 검증한다. 응답의 unit/barcode 매핑·membership hash·entity version·work-group topology가 현재 작업에 결속된다.
- 효과/오류: ordinary PHS2 조회는 `PACKAGE_SOURCE`를 resolve하며 새 exclusive `CREATE_PACKAGE` lease를 발급하지 않는다. 재사용 가능한 같은 lease가 없을 때 ordinary F3가 발급받는다. 조회 자체는 포장 확정이 아니다. 잘못된 라벨·불완전 증거·구성 변화는 fail-closed; 최신 snapshot과 operation lease의 일치는 F3에서 다시 필요하다. 읽기 조회에 명령 receipt/cursor를 가정하지 않는다.
- 저장된 PREFETCHED lease가 만료됐으면 스캔 조회에는 쓰지 않고 기존 lease 없는 source 조회를 한다. 원 행·발급 key·잠금은 그대로 보존해 완료 때 서버 판정을 받는다.
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
- 양쪽 근거: [교체 workflow](../../sealed_transfer_exchange.py) `_build_command/attempt`, [서버 replace_sealed_transfer_members](../../../WorkerAnalysisGUI-web/logistics_ledger/service.py), [앱 QR 확인](../../Label_Match.py), [교체 정책](../MEMBER_EXCHANGE_POLICY.md). [작업자 정본](../LABEL_MATCH_WORKER_GUIDE.md)은 목록·일괄 적용·3쌍 이상 capability·후속 전자 QR 확인을 반영한다([LM-B01](BACKLOG.md#lm-b01)).

<a id="c-04"></a>
## C-04 F3 포장 명령·lease·outbox

- L02a의 반복 current-state 쓰기 오류는 기존 재시도 후 같은 데이터 루트의 쓰기·fsync·readback·삭제 확인을 거친다. 확인 파일은 실제 실패한 checkpoint 바이트 수 이상으로 만들되 64KiB에서 제한한다. 확인에 성공하면 기존 `package_workbench_holds`에 원 세트, 파일·오류·시각·스캔 순서와 `UNKNOWN`을 내구 저장하고 원 checkpoint를 바이트 대조 후 보관한다. 그 뒤에만 현재 슬롯을 비운다. 완료 이벤트·성공음·수량 확정은 만들지 않고 해당 감사는 자동 재생하지 않는다. 관리자 확인 전 제품 중복 검사와 새 현품표 보류는 유지한다.
- 서로 다른 격리가 10분 안 네 번째에 이르면 전체 저장 장애 안내를 유지한다. 같은 세트는 한 번만 세고, 확인·기록·원본 보관 실패는 횟수와 무관하게 계속을 허용하지 않는다. 기록은 날짜 만료 없이 보존한다. CSV writer/flush의 미확정 실패·다른 완료 신원의 재완료와 이미 내구 완료한 CSV 후속 저장 실패는 기존 완료 경계·복구 경로를 유지한다.
- 원본 바이트·보류 목록 검증과 archive readback은 쓰기 허가 밖에서 한다. 허가 안에서는 기존 보류 감사행 cursor·checkpoint 파일 신원을 다시 확인하고 64KiB 이하 확인 파일·보류 기록·원본 이동만 적용한다. 원본 미확인·더 큰 기록·미확정 감사·세대 변경은 기존 중단·관리자 복구를 유지한다. 격리 기록 뒤 이동 실패를 재시도할 때는 기존 격리만 마무리하며 원 세트를 다시 저장·완료하지 않는다. 시작 복원 입구에서 현재 세트의 보류 행이 확인되면 이유·날짜와 관계없이 감사 재생·활성 복원 전에 `.held-SHA` 보관과 바이트 확인을 다시 마무리하고 현재 칸을 비운다. 다시 보관하지 못하면 E3로 유지한다. 중복 키에는 `held_by=SYSTEM` 또는 `SYSTEM_`으로 시작하는 시스템 자동 보류만 포함한다. 이유와 관계없이 그 세트의 제품과 고유 현품표를 새 세트·교환 입력에서 거절한다. `LABEL.SET_HOLD`·`F5_HOLD` 등 사람이 건 보류는 종전 입력 기준을 유지한다.

- 복구 창의 관리자 재확인·보류·지원 인계·실물 조회·관리자 요청 재조회·변경 파일 복원은 기존 UI lane의 준비/적용 단계로 실행한다. 사람 입력은 UI 단계에서 받고 새 verify는 입력 뒤 실행한다. 기존 pending status/중앙 대상 조회 순서와 verify 뒤 intent prepare 순서를 유지한다. 같은 대상의 요청은 중복 접수하지 않으며 창 닫힘은 내구 적용을 취소하지 않는다. 표시만 UI terminal 단계에서 창·generation을 확인한다. 내구 효과가 확인된 빈 결과도 APPLIED로 끝내고 기존 fallback 안내·목록 갱신을 한 번 수행한다.

포장 완료가 영구 거절한 operation lease는 기존 `OPERATOR_REVIEW` 행을 보고 대기 원천으로 쓴다. 영수증 재확인과 CREATE 뒤 남은 주기 예산에서 보고 HTTP를 최대 한 번 수행하며, 서버 응답 적용 전에 writer admission과 같은 전체 행을 다시 확인한다. 서명 시각 검증·업무 행·원 명령·리스 증거·로컬 완료는 보존하고 결과 코드만 기존 로그에 남긴다. 실제 관리자 해제·서버 수용은 별도 검증이다.

저장된 ACTIVE/PREFETCHED lease를 완료에 재사용할 때만 로컬 만료 검사를 생략한다. 서명·단말/source binding·snapshot·fence·미래 발급 시각 검증은 유지하고 원 lease ID/token으로 전송한다. 새 발급 응답은 만료도 계속 검증한다. 같은 소유자·변하지 않은 자원의 늦은 완료 허용과 같은 명령/key의 영수증 재생은 서버가 판정한다. 서버 409는 기존 그 세트의 충돌·검토 길로 보내며, 만료 행을 지우거나 같은 자원에 새 lease를 발급해 우회하지 않는다.

- [LM-3 완료 경계](operations.md#module-boundaries-lm3)는 기존 F3 handler·내구 commit signature와 호출 시점 전역/patch를 보존한다. 실패 뒤 원 작업·command, current-state→intent→CSV flush/fsync→marker→성공→중앙 ACK 순서는 불변이다.
- LM-3 pane 구성은 기존 widget 생성·binding 순서와 disabled 초기 상태를 유지한다. notice Enter binding과 danger 취소 버튼 생성자는 owner에 두고 기존 snapshot adapter·action gate를 재사용한다.

- [LM-2 모듈 경계](operations.md#module-boundaries-lm2)는 기존 façade API·동일 command key·membership/expected_versions를 유지한다. local durable→성공→중앙 ACK, due retry·취소 dependency·reviewed recovery의 순서는 분리 대상의 불변 계약이다.

- LM-B02 완료 CSV 위치 색인은 거짓 부재·중복 완료 위험 때문에 철회했다. 같은 데이터 루트·PC prefix의 모든 CSV를 날짜와 무관하게 직접 검색하며, 캐시·색인·metadata로 부재를 판정하지 않는다. 기존 색인·임시 파일은 조회·갱신·재구축하지 않는다.
- 적중 CSV는 writer flush→동일 파일 재대조→fsync를 끝낸 뒤에만 local marker를 허용한다. 조회 중 파일 소실은 현재 파일 목록으로 한 번 재검색하며, 계속 이동하거나 목록 열거가 실패하면 부재 대신 오류로 중단한다.
- writer는 기존 CSV append와 durable event의 flush/fsync를 유지한다. 완료 존재 확인 비용은 보관 파일 수에 비례하며 보관 정책으로 관리한다. 최종 검색 뒤의 다중 프로세스 동시 쓰기를 직렬화하는 계약은 추가하지 않는다.

- 주기 review 정리·조회 및 F4 표시 상태는 package worker의 불변 snapshot이다. Tk는 set·generation·업무 epoch를 확인한 결과만 적용하며 조회 실패 시 마지막 경고를 보존한다. 표시 snapshot은 F3/F4 직전 authoritative guard를 대체하지 않는다.
- capture fixture 소비자는 반환된 worker를 최대5초 기다린 뒤 완성 snapshot을 같은 guard로 적용하고 conflict 개수를 대조한다. worker 미완료·snapshot 누락·거부는 capture 실패이며 경고 assertion을 생략하지 않는다. 이 대기는 capture 도구에만 있고 제품 Tk callback의 DB 읽기는 worker에 유지한다.
- `_refresh_package_cancellation_review_notice()`는 worker 요청으로 `Thread` 또는 `None`을 반환한다. snapshot 인수를 주면 현재 context에서 적용한 취소 건수, 거부 시 `None`을 반환한다. walkthrough의 `_start_package_outbox_drain` override는 `review_only=True`를 그대로 위임해 이 계약을 보존하고 일반 전송 요청만 억제한다.
- lane 외부의 새 봉인 적용도 표시 epoch를 갱신한다.

- 2026-09-12 회귀 정합: 초기 source의 `VALIDATED`는 lease 발급·F3 완료 신호가 아니다. 현재 F3의 future-issued 응답은 서명 검증 뒤에도 `issued_at` 전에는 거부하고 동일 durable issue key로만 다시 요청한다. expiry 경계·서명·단말/source binding·snapshot hash·artifact fence 오류는 package enqueue와 완료를 차단한다. 보존된 과거 2단계 plan에서 signed clock 대기 및 definite service 실패는 원 요청을 유지하지만 unknown issue는 `RECONCILE_PENDING_VALIDATION`, API 오류 문구만의 clock 주장은 `BLOCKED_INVALID`다. [32개 회귀와 기존38개 계약 검사](operations.md#clock-recovery-tests-20260912)는 host 증거이며 이 계약의 제품 변경은 없다.
- 초기 검증과 권한 시점: 새 `LABEL_PACKAGE_SOURCE` plan은 `label-package-source/READ_ONLY` 1단계이며 source membership·authority/version 증거를 VALIDATED로 동결한다. 초기 검증에서 CREATE_PACKAGE 발급·mutation attempt를 만들지 않는다. 이 읽기 증거의 expiry는 기존 validation claim의 만료시각(기본 claim 300초)에 결속하고, bounded expiry·claim/fence·snapshot 검증을 유지한다. 과거 2단계 plan의 정확한 step 정의·signed evidence·미확정 mutation 복구는 보존하며 새 기본 plan으로 발급하지 않는다. 실제 F3는 lease가 없으면 기존 `_acquire_operation_lease`를 호출하고 expected snapshot·서명·현재 set·fence·local marker를 원래 순서로 검증한다. lease 시각 검사는 서버와 같이 PC 시계가 30초까지 늦어도 받는다(30초를 넘으면 NOT_YET_VALID이고 화면은 PC 시계 동기화를 말한다). 권한 발급이 완료보다 먼저이므로 PC 시각이 `issued_at`보다 이르면(30초 이내) `operation_lease_completed_at`을 `issued_at`으로 올린다 — 참값 쪽 교정이며 뒤로 당기거나 `expires_at`을 넘기지 않고, 올린 폭만 진단 출력에 남긴다. [111 deferred](D:/KMTech/cold-program-material/from-E/KMTech/label-install-qualification-20260908/timing06-deferred-green03.xml), [218 lane/package/lease](D:/KMTech/cold-program-material/from-E/KMTech/label-install-qualification-20260908/timing06-boundaries.xml), [writer static 4](D:/KMTech/cold-program-material/from-E/KMTech/label-install-qualification-20260908/timing06-inventory.xml)는 host 회귀이며 새 설치 수용은 별도다.

- 실제 적용 범위: Main이 수용한 timing06의 [정상 설치 native 0/전체 payload 재해시](D:/KMTech/cold-program-material/from-E/KMTech/label-install-qualification-20260908/REINSTALL-TIMING06-READBACK.json)는 코드 배치와 기존 상태 지속성을 입증한다. [공유 중지 뒤 원래 F4](D:/KMTech/cold-program-material/from-E/KMTech/label-install-qualification-20260908/SHARED-PAUSE-TIMING06-BUSINESS.json)는 expiry 이후에도 기존 fence2 PREFETCHED/ACTIVE와 원본 PHS2를 보존했고 package/exchange 명령을 만들지 않았다. 초기 시점 교정이나 새 설치는 기존 lease의 해제·권한 변경·terminal receipt를 뜻하지 않는다. 제한된 기존 ADMIN machine 권한이 없다는 의존을 보존하며 이를 위한 credential/permission 변경·강제 retirement·새 취소 체계는 실행하지 않았다. 새 소스의 GUI 업무 수용과 Main 소유 정상 후속 fixture는 별도 미실행이다.

- API: `POST L/operation-leases/issue`, `POST L/packages`, `GET L/receipts/{scope}/{key}`. 기본 membership mode는 `INHERIT_ALL`; 정확한 work-group 경로와 호환 `EXACT_RESCAN`의 command 생성은 [package_logistics](../../package_logistics.py)에 남아 있다.
- lease: current set에 attach된 `PREFETCHED` lease의 ID·snapshot hash·fence가 검증 결과와 일치해야 한다. UTC 완료시각을 결속해 로컬 marker와 lease를 같은 SQLite transaction으로 확정한다. 서버 승인 없이 새 offline lease를 임의 발급하지 않는다.
- 사용자 표시는 권한 확인을 포함한 로컬 완료 저장, durable 완료 뒤 중앙 전송 대기, ACKED 중앙 확정을 구분한다. 저장 중 안내나 성공음은 중앙 ACK를 대신하지 않으며 marker 전 성공 표시는 금지한다.
- 기존 완료 재사용·복구: `_label_match_local_completion_event_exists`는 발견한 같은 set의 `TRAY_COMPLETE`를 writer flush/barrier 뒤 `r+`로 다시 열어 같은 descriptor에서 재대조·fsync한 뒤에만 durable event로 재사용한다. sticky writer 오류·재대조·동기화 실패는 `PackageLogisticsError`로 전파해 중복 append와 marker 승격을 막는다. ACKED orphan의 동기화 실패는 같은 command/set/scans를 복원·보존하고 기존 입력/재시도 차단 안내를 적용한다. 유효한 orphan의 lease가 있으면 원래 완료시각과 함께 marker transaction에 전달한다.
- 중앙 current-state timestamp가 파싱되지 않으면 원본을 삭제하거나 덮어쓰지 않고 복구 잠금·관리자 수리 대상으로 남긴다. 정상 전날 중앙 캐시와 캐시 없는 orphan 복구의 기존 경계는 유지한다.
- 근거·한계: [M3/N6 사례·소스 매핑](operations.md#residual-source-evidence)의 M3 변경 13사례는 실제 저장 거부·자정/중단·materializer 및 worker/Tk 적용 경계, 별도 N6 6사례는 기존 CSV 재동기화·거부·writer 오류·재대조를 입증한다. N6의 역사 ACKED/marker0 row는 lease 없이 로컬 seed한 것이며 현재 `claim_next`는 marker1을 요구한다. 따라서 서버가 그 역사 상태를 생성했거나 real lease-bearing orphan의 동기화/marker 실패가 하나의 통합 재시작 경로로 통과했다는 증거는 **UNPROVEN**이다. 원격 lease 발급·backend ACK·native F3는 별도 수용 범위다.
- 중앙 원자성: source TRANSFER의 `AVAILABLE → CONSUMED` CAS, PACKAGE 및 `SHIPPING-WAIT`, `PACKAGE_CREATED` event·outbox·receipt를 중앙 transaction으로 기록한다. 앱 CSV와 중앙 transaction 사이에는 분산 원자 commit이 없으며 동일 키 복구로 연결한다.
- 선택/순서: **due/공정 재시도**다. `claim_next`는 marker=1, due PENDING만 선택한다. stale `SENDING`은 300초 기준으로 PENDING 회수하며 `COALESCE(last_attempt_at,created_at), created_at, idempotency_key`로 정렬한다. 한 drain에서 시도한 key를 제외해 첫 실패 뒤의 준비 row도 진행하고, 재전송은 같은 idempotency key를 유지한다. 300초는 **전송 claim 구현 상수**로 업무 lease 허용 시간과 다르다.
- U4 다시 포장 순서: 로컬 완료 marker와 같은 SQLite transaction에서 단조 완료 순번과 같은 이적 묶음/현품표의 바로 앞 완료 포장 키를 저장한다. 새 CREATE는 이 키의 취소 ACKED와 미확정 취소 receipt 재대조가 없음을 기다리며, 미래 포장의 취소를 기다리지 않는다. 명시 `reopen:false`는 내구 관리자 확인 상태로 보존하고 자동 CREATE를 보내지 않는다. v11 완료 행의 삽입 순서·시각으로 관계를 추측하지 않는다. 같은 원천의 완료 행이 하나이면 마이그레이션하고, 여러 행의 앞 관계를 입증할 수 없으면 원 명령·영수증·완료 marker를 보존한 내구 확인 상태(`REPACK_PREDECESSOR_REVIEW_REQUIRED`)로 해당 현품표의 자동 CREATE를 차단한다. 새 v12 사슬은 A CREATE → A CANCEL → B CREATE → B CANCEL → C CREATE로 복구하고 다른 현품표는 기존 due/공정 재시도를 계속한다. 저장된 schema 판이 앱의 이해 범위를 넘으면 schema 변경 전에 `PACKAGE_OUTBOX_SCHEMA_TOO_NEW`로 시작을 거절하고 관리자 안내를 남기며 판을 덮어 찍지 않는다. 앱 되돌림은 업데이트 앞 DB 사본 복원과 재대조를 함께 수행한다.
- 건별 보류: `package_workbench_holds`는 set별 원본 current-state snapshot, 검증된 물리 PHS2/ITG, 관리자 `admin_id`·시각·사유를 durable 저장하고 readback한 뒤 현재 작업 파일과 화면 슬롯을 비운다. 행 저장 뒤 중단되면 같은 key의 CONSUMED 요청이 파일 제거·화면 슬롯 해제를 재개하며 원본 snapshot과 완료 감사를 readback한다. 손상 draft에서 신뢰할 현품표 신원을 확인하지 못하면 원본과 작업대를 유지한다. 원 package/exchange command·key·receipt·UNKNOWN은 바꾸지 않는다. 보류 set은 package claim/stale 회수와 exchange 자동 drain에서 제외하고 동일 PHS2/ITG 재접수를 거부한다. 감사에는 action·target·작업자 원 이름과 송신 local_id·verification_id·operation_key를 결속한다. 보류 해소·실물 종결은 자동 승인하지 않는다. 기존 prewrite F1만 별도 경로로 유지한다.
- F5 건별 보류: 선택한 hold ID가 현재 active journal SHA256과 같고 기존 보류가 없을 때만 `phs_label_workbench_holds`에 원본 bytes·SHA256·label/source ID·관리자 ID와 보관 경로를 먼저 저장하고 readback한다. 연결된 현재 세트도 파일·슬롯까지 보류한 다음 active journal을 별도 파일로 보관해 다른 label 작업을 허용한다. 연결 세트의 hold 행만으로 완료를 판정하지 않는다. PIN 확인이 실패하면(서버 거절·틀린 PIN·바쁨·불통·모르는 코드 포함) 화면에서 명시적으로 선택한 건만 `UNVERIFIED_OPERATOR_HOLD`로 격리하며 관리자 승인으로 간주하지 않는다. 안내·intent 준비·대체 보류는 `label_admin_pin.unverified_hold_allowed`로 같은 코드 집합을 판정한다. `TARGET_CHANGED`·`AUDIT_UNAVAILABLE`는 보류하지 않는다. 대상 지문과 현재 작업자를 다시 대조하고 코드·작업자·관리자 ID의 감사를 내구 저장한 뒤 기존 보류 writer를 호출한다. 현재 세트의 보류도 같은 실패 경계를 쓰며 새 저장 형식이나 앱 잠금을 만들지 않는다. 이 격리의 journal이 주장하는 PHS2/ITG는 재접수를 막되 중앙 결속된 신원으로 승격하지 않는다. 원 exchange/prepare key와 UNKNOWN을 유지한다. 시작 시 DB hold와 원본 파일을 대조해 중단된 보관 단계를 완결한다. 이 동작은 중앙 교환의 성공이나 인쇄 완료를 추정하지 않는다.
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
- lifecycle 예외: `label_match/legacy_packaging_csv`, role `label_match`, stream `label_match_events`의 canonical emitter 헤더 `timestamp,worker_name,event,details`와 서버 LabelMatch 카탈로그(w9webrecv 6e27c99)에서 업무 사건(`TRAY_COMPLETE`·`SET_DELETED`·`TRAY_COMPLETION_CANCELLED`)을 뺀 원문 전용 20종(`direct_sync_push.LABEL_MATCH_RAW_ONLY_EVENTS`: APP_START/APP_CLOSE/SCAN_ATTEMPT/SCAN_OK/ERROR_INPUT/ERROR_MISMATCH/UI_ERROR/BASE64_DECODED/LABEL_MATCHED/PACKAGING_WAITING_OBSERVED/SHIPPING_WAITING_OBSERVED/SET_RESTORED/SET_CANCELLED/POST_REVIEW_REQUIRED/PHS_REPLACEMENT_WAITING_MARKED/PHS_LABEL_ACTIVE_RESOLVED/PHS_LABEL_EXCHANGE_RESULT/PHS_RECONCILIATION_EXCHANGE_RESULT/SEALED_TRANSFER_EXCHANGE_ACKED/SEALED_TRANSFER_EXCHANGE_APPLIED) 행만 허용한다(3f4574a까지는 앞 8종만이라 나머지 12종만 든 delta가 오면 전송 줄이 멈췄다). receipt11의 SCAN_OK·SET_RESTORED와 receipt12의 SET_CANCELLED·SEALED_TRANSFER_EXCHANGE_APPLIED는 명시된 두 차례의 한정 확장이며 business event의 COMPLETE 요구는 유지한다. 공개 SET_RESTORED는 set_id·continued_by, SET_CANCELLED는 set_id만 기록한다. 공개 SEALED_TRANSFER_EXCHANGE_APPLIED는 기존 identity·멤버 목록·entity_versions·atomic_local_apply를 유지하고 두 raw seal QR 필드만 제외하며 private durable receipt/apply 상태를 변경하지 않는다. `RAW_LEGITIMATE` receipt의 install/source-file identity v2, install scope/hash, source content SHA-256/byte/range, 로컬 파일 hash와 실제 event별 개수가 일치해야 한다. observation v1은 `observation_only=true`, `OBSERVED`, `projection_required=false`, 모든 행 `NOT_PROJECTED/RAW_EVIDENCE_ONLY/NO_STAGE1_REDUCER`이며 unknown/required/projected 수는 0이어야 한다. 나머지 관측 행 수는 업로드 행 수와 같다. 2xx·boolean committed·accepted·retryable=false·retry/error 없음·정수 행 합계·errors/quarantine=0과 기존 runtime fence/rotation 검증을 유지한다. ACK 및 read-only retention 후보에 같은 검증을 적용한다. COMPLETE의 기존 조건이나 runtime observe/legacy 정책을 확대하지 않는다.
- 정상 종료 근거/한계: F3 `TRAY_COMPLETE` ACK 뒤 relay가 새 `APP_CLOSE` delta만 보내는 경로와 동결 서버의 raw 분류는 [정적 소스 대조](E:/KMTech/coordinator-handoff-20260907-01a07992/label-producer-close-contract-fix/PREPARATION.md)로 확인했다. Main이 수용한 [ProducerClose 실제 50/150 PASS](operations.md#producer-close-evidence)는 동결 receipt·로컬 session double을 통한 uploader/retention/runtime 회귀다. raw 수신은 이전 tray receipt·새 F3·포장 명령 ACK·업무 projection을 대체하지 않는다. 비업무 catalog 전체로 예외를 넓히지 않았으며 실제 서버·native F3/종료·재개 연동은 **NOT TESTED / UNPROVEN**이다.
- cursor/중복: 일반 물류 command에 공통 consumer cursor가 있는 것은 아니다. 이 전송은 source range/prefix와 immutable fingerprint, producer/key/endpoint binding으로 중복을 판단한다. `client_batch_id` 변경만으로 새 내용으로 취급하지 않으며 runtime lease/fencing은 attempt 상태다. 손상/없는 spool은 source range 재평가 대상이다.
- 오류/보존: deterministic jitter·유효 Retry-After(0 포함)를 유지한다. missing/unreadable spool은 `failed_permanent`, 이미 commit한 non-2xx는 검토로 분리한다. `operator_review/failed_permanent`의 동일 prefix·spool을 보존하며 미수신 구간을 0-byte delta로 바꿔 진행시키지 않는다. ACKED retention 후보도 삭제 승인이 아니다.
- 소비: `normalize_legacy_csv_row/ingest_label_match_csv_file`이 event/detail alias를 처리하고 common projection이 세트·취소 의미를 계산한다. `PROJECTION_API_READ_ENABLED`와 source readiness에 따라 operations_flow가 제한될 수 있다. 브라우저는 operations_flow를 요청하고 포장을 세트로 표시한다. 실제 배포 flag·조회 결과·신선도는 미입증이다.
- 양쪽 근거: [direct_sync_push](../../direct_sync_push.py), [완료/보존 정책](../../DIRECT_SYNC_DATA_PLATFORM_NOTES.md), [producer_ingest](../../../WorkerAnalysisGUI-web/producer_ingest.py), [HTTPS edge 계약](../../../WorkerAnalysisGUI-web/docs/PRODUCER_INGEST_HTTPS_EDGE.md), [common_projection_sync](../../../WorkerAnalysisGUI-web/common_projection_sync.py), [common_projection](../../../WorkerAnalysisGUI-web/common_projection.py), [dashboard API](../../../WorkerAnalysisGUI-web/app.py), [표준 표시](../../../WorkerAnalysisGUI-web/static/dashboard_standard.source.js), [enhanced 요청](../../../WorkerAnalysisGUI-web/static/dashboard_enhanced.js). [LM-11](README.md#lm-11), [LM-B06](BACKLOG.md#lm-b06).

<a id="c-06"></a>
## C-06 포장 취소

- 생성·취소 관리자 경고의 조회 실패는 빈 결과가 아니다. 마지막 확인 목록·안내를 보존하며 오래됨을 표시하고 확인된 빈 조회만 해제한다. 로컬 완료 marker나 중앙 취소 결과를 조회 실패로 변경하지 않는다.

`POST L/packages/cancel` / `CANCEL_PACKAGE`는 package ID·사유·비어 있지 않은 evidence와 예상 version을 요구한다. 로컬 취소 outbox는 원래 CREATE 명령 ACK에 의존하며 동일 취소 command/key로 재시도한다. 서버는 포장 완료 유효성을 취소하고 **재고는 SHIPPING-WAIT에 유지**한다. 이를 TRANSFER 재고 반환이나 실제 포장 해체라고 해석하지 않는다. 집계의 취소 제외는 별도 projection 소비 결과다. 근거: [PackageCancellationOutbox/processor](../../package_logistics.py), [API](../../../WorkerAnalysisGUI-web/blueprints/logistics/api.py), [service.cancel_package](../../../WorkerAnalysisGUI-web/logistics_ledger/service.py). [LM-09](README.md#lm-09), [LM-B05](BACKLOG.md#lm-b05).

U4 앱은 U3 취소 영수증의 `reopened_transfer_bundle_id`·`transfer_entity_version`·`reversal_movement_id` 추가 칸을 보존한다. 이 칸이 없는 옛 서버는 위 SHIPPING-WAIT 계약이며, 다시 포장 충돌의 로컬 완료·원 영수증을 유지하고 「회사 서버가 취소한 현품표의 다시 포장을 지원하지 않습니다」를 덧붙인다. `reopen:false`이면 되돌리지 못했다는 안내를 구분한다. 이 판의 U3 응답 검증은 계약 대역이며 실제 서버의 원 이적 반환·새 포장 출고는 별도 수용이다. F2 결과는 기존 취소 대기/반출 보류 안내와 함께 「같은 현품표로 다시 포장할 수 있습니다」를 알린다.

U4 재포장 확인 상태(`REPACK_REOPEN_REFUSED`·`REPACK_PREDECESSOR_REVIEW_REQUIRED`)는 완료 marker=1이어도 새·옛 화면의 복구 목록에 「확인 필요」와 원 현품표 전체 코드로 나타난다. 같은 현품표의 재스캔은 「이 현품표는 확인이 필요합니다」·「웹 사무실에서 이 이적 묶음을 확인하세요」를 보이며 새 포장을 접수하지 않는다. 현재 U3에는 거절을 수락으로 바꾸는 재개 API가 없으므로 이 행은 표시만 하며 다시 확인 단추·확인 해소·CREATE 재개는 제공하지 않는다. 원본·키·완료 marker·영수증·감사는 그대로 유지한다. 새 화면은 기존 recovery row의 tag/item/status와 빈 actions를 사용하고 화면 글·모양은 M125b의 확정 범위를 따른다.

<a id="c-07"></a>
## C-07 저장된 F5 현품표 출력·활성화 복구

`L/phs-label-exchanges/prepare`, exchange별 status/`prints`/print-attempt complete/`activate`와 reconciliation 경로가 준비·출력·활성화를 분리한다. 로컬 `label-match-phs-label-exchange-v1` journal과 `recover_current`, `recover_reconciliation`은 서버 결과·물리 출력 증거·로컬 적용을 조정한다. 새 날짜 교환 실행·candidate 조회·reconciliation action resolve는 제거했으며, 기존 journal의 원 prepare key·source/target 신원·수량·구성원과 요청 형식을 유지한다. Windows proof의 `proof_kind=WINDOWS_GDI_SPOOL`, printer/job/document/submitted_at와 `windows_gdi_end_doc=true`는 큐 접수 증거이며 종이 배출·QR 가독성 측정이 아니다. 상세 분기 payload와 설치 capability 조합의 전수 검증은 미완료다. 양쪽 근거: [phs_label_workflow](../../phs_label_workflow.py), [서버 phs-label-exchanges API](../../../WorkerAnalysisGUI-web/blueprints/logistics/api.py). [LM-10](README.md#lm-10), [LM-B04](BACKLOG.md#lm-b04).

<a id="c-08"></a>
## C-08 이력 읽기 완전성

- 이력 reload 시작 시 활성 `scan_count`, `global_scanned_set`, `set_details_map`을 지우지 않는다. 같은 generation의 완전한 오늘 결과만 세 색인을 교체한다. 다른 날짜의 PC 조회는 상태 변경 전에 거절한다. 화면의 최근 100줄 제한은 세 색인이나 기록 파일을 줄이지 않는다.
- 완전 결과 수신 후 논리 색인을 설치하고 오늘 읽기 gate를 해제한다. 화면 삭제·삽입·집계는 callback당 최대100회 연산 또는8ms 경과 후 after로 양보하며, 단일 위젯 연산·OS 스케줄링 시간을 강제 제한하는 계약은 아니다. 새 generation/종료는 남은 배치를 무시하고, 적용 중 들어온 완료·취소·집계 갱신을 오래된 화면 snapshot으로 덮지 않는다. 읽기 중/과거 조회 gate와 별도 read-only 창 분리는 유지/후속 범위다.
- timestamp·JSON syntax/객체 shape·CSV/파일 읽기 오류가 있으면 부분 집계를 적용하지 않는다. 손상 행 수와 파일·행 위치(최대 20개)를 이력 상세에 남기고 오늘 조회 오류는 재조회 성공 전까지 작업을 차단한다. 로그 파일이 없는 날짜만 정상 빈 결과다.
- 근거: [이력 회귀](../../tests/test_label_match_core.py) `test_history_reload_requires_complete_read_before_replacing_active_indexes`, `test_history_file_errors_are_not_confirmed_empty_history`; 취소·삭제·stale generation·과거 조회 계약을 유지한다.

## 계약 유지·검증 연결

기능 기준은 [README 카드](README.md#feature-cards), 실행 설계는 [운영 수용 시나리오](operations.md#verification)에서 연결한다. 서명·권한·version/capability와 source identity가 달라지면 해당 계약의 실제 양쪽 evidence를 재대조한다. 현재 설치 provider/overlay, 서버 flag, upstream 입력 발행과 downstream 출고·화면의 전체 연결은 [LM-B04](BACKLOG.md#lm-b04)·[LM-B06](BACKLOG.md#lm-b06)·[LM-B08](BACKLOG.md#lm-b08)의 확인 과제이며 코드 존재를 연동 성공으로 집계하지 않는다.


## 설정·건별 복구 안내

진행 중 설정 창은 현재 작업을 완료하거나 취소한 뒤 저장하도록 안내한다. 기존 활성 세트·배경 작업·이력·종료 guard와 작업자 귀속은 그대로다. 건별 복구 목록은 쉬운 이유와 기존 다음 동작을 먼저 보이며, 선택한 항목의 원 ID·상태 코드·원SHA/변경SHA·파일 경로·미검증·UNKNOWN·관리자 요청 수는 읽기 전용 스크롤 상세에 그대로 남는다. 목록 순서·선택과 PIN·권한·재확인/보류/복원/인계 handler는 변하지 않는다.

## StartupUpdate1 · 매일 시작 시 코드 전환 계약

자동 시작의 update mutex busy는 최초 native 대기 5초 뒤 기존 bootstrap 240초 창에서 재시도한다(마지막 native 대기 최대 5초). 한도 뒤에는 현재 G ALLOW·active/runtime·ordinary cohort 증명을 유지한 현재 판 시작과 UPDATE_APPLY_NO_PROGRESS/0 기록을 사용한다. abandoned 및 현재 판 자체의 거절은 유지한다. 실패한 prepared 원문 네 개는 보호 packet 폴더에 보존하며 같은 판/같은 손상 표지는 실패·claim·transition을 다시 쓰지 않는다. 자동 onboarding만 허가 밖의 update lock 아래 한 번 재관측하고, transition의 JSON 뜻이 같으면 통과한다. 미해결 변경은 기존 일반/trial 쓰기 증명을 확인한 뒤 UNKNOWN/0으로 등록을 미루고 GUI guard를 계속한다. 수동 등록·설치의 엄격한 거절은 유지한다.

`app/startup_check.py --json`은 GUI 본체를 import하지 않는 별도 읽기 전용 입구다. 기존 bootstrap 인계 판정, 배포 담장 파일 판정, 설치 inventory 검증, 자료 세대 판정과 현재 사용자 준비 판정을 공유한다. 파일·SQLite·네트워크·mutex·자식 프로세스·화면을 만들지 않는다. 설치 inventory의 기존 고정값과 작은 시작 파일만 대조하며 전체 runtime 재해시는 수행하지 않는다. 같은 판의 build 불일치·자료 하한·손상 표지는 실제 자료 admission과 같은 입력으로 판정한다.

출력은 `app_startup_check.v1` JSON 한 줄이다. `app=LABEL`, `version`, `build_sha`, `startable`, `reasons`, `status`, `category`, `categories`, `checked_at`만 담는다. 계정·경로·주소·인증 값과 예외 원문은 포함하지 않는다. 종료 코드는 READY 0 / 증명한 시작 불가 10 / 못 읽음·판정 예외·시간 상한 20이다. `ALREADY_RUNNING`은 시작 가능 상태를 바꾸지 않는다. 첫 거부 묶음과 겹치는 묶음을 함께 기록하며 까닭→묶음 정본은 `startup_check.REASON_CATEGORIES` 하나다. 품목 목록은 인증된 저장 목록 또는 기존 과도기 허용으로 서버 없이 시작할 수 있는지 판정한다.

제품 시작에서는 이미 실행한 관문의 결과를 사용하며 CLI 전체 확인을 추가로 실행하지 않는다. G0는 새 거부 0이다. 기존 경고·재시도·bootstrap 인계·이미 열린 창 활성화와 종료 코드를 유지하고, 기존 관문이 종료를 확정한 경우에만 고정 한 문장과 닫기 단추로 안내한다. 새 결과 기록의 실패·예외·시간 초과는 기존 시작 결정을 바꾸지 않는다.

진단 writer `startup_check_result`는 업무 쓰기 허가·네트워크·추가 mutex 없이 기존 결과를 `C:\ProgramData\KMTech\start-check\<YYYYMMDD>-LABEL.json` 한 줄로 원자 교체한다. 폴더가 없거나 쓰기 실패하면 조용히 계속하며 폴더·ACL은 설치 키트가 준비한다. `KMTECH_START_CHECK_TEST_CATEGORY`는 결과 묶음만 강제하는 시험 전용 입력이며 설치기·업데이트는 설정하지 않는다. 실제 시작 결정은 주입과 무관하다.

D-16 S/E1–E9: READY 실패 뒤 writer 정상 부재와 실제 공통 SID G를 확인하고, 결정부터 내구 저장한 뒤 코드 포인터를 정착한다. G 불변·이전 판 ALLOW면 이미 확인된 판으로 ROLLED_BACK하며 두 번째 READY를 요구하지 않는다. G 진전은 후보를 유지하고 같은 bootstrap이 GUI 일반 실행을 시작한다. shared/owner marker와 업무 자료는 되돌리지 않는다.
SYSTEM 요청 실패·60초 안에 apply 시작 증거가 나타나지 않음 및 FAILED/UNKNOWN 뒤에도 정확한 active 판이 G ALLOW면 기존 일반 사용자 경로로 시작한다. 현재 사용자·보호 runtime·active SHA 및 같은 시작 이후 GUI/relay cohort를 검증한다. startup의 update mutex와 native 시작은 직렬이며, writer mutex를 이미 가진 sink는 update mutex를 다시 얻지 않고 같은 조건을 읽기 전용으로 확인한다.
늦은 SYSTEM이 기존 writer를 만나면 DEFERRED와 recovery_pending 및 원 phase·원 코드를 보존한다. writer 부재 뒤 같은 G decide로 한 번 정착하고 pending=false를 내구 저장한 다음에만 새 prepared를 적용한다. 다음 아이콘도 pending 회복을 요청한다. generation_before UNKNOWN에서는 trial을 시작하지 않으며 G 판정 불가는 기존 고정 지원 안내로 자료와 증거를 보존한다. 보고는 허용된 transition_code·retained_candidate만 사용한다. headless 실제 자식/서명 packet 회귀와 실제 GUI READY·SYSTEM task·랩 실물 수용은 구별한다.

relay 시작의 bootstrap PID는 임시 launcher다. 정상 exit 0 또는 launcher 생존만으로 relay를 승인하지 않는다. lease·fresh status와 보호 active version의 실제 relay PID·SID·역할·SHA·code root·생성 시각을 대조하고 실제 relay PID를 반환한다. trial에서는 nonce도 결속하며 UNKNOWN/FAILED 전이를 성공으로 바꾸지 않는다. 설치 onboarding은 기존 defer를 유지하고 일반 사용자 bootstrap guard를 완화하지 않는다.

feed 설정은 github.com·githubusercontent.com·github.io의 루트와 모든 하위 호스트(Pages/gist 포함)를 거절한다. 대소문자·DNS trailing dot을 정규화하고 닮은 회사 호스트는 이 거절 목록으로 차단하지 않는다. Python/PowerShell 설치기/embedded release gate가 같은 호스트 정책을 사용한다.

GUI/relay trial 시작 예외는 nonce·transition·SHA에 묶인 trial-failure 증거로 먼저 내구 저장한다. 같은 실패 trial은 재시작하지 않는다. SYSTEM의 writer 부재와 실제 G 결정으로 정착한 뒤 이전 판이나 유지한 후보를 같은 bootstrap에서 일반 실행한다. 이미 확인된 이전 판에 복귀 READY를 다시 요구하지 않는다. UNKNOWN/FAILED도 정확한 active 판의 현재 G ALLOW와 같은 ordinary cohort 조건을 만족하면 일반 시작하며 포인터·확정 상태는 사용자 쪽에서 바꾸지 않는다. 기존 trial claim은 nonce·실제 PID·SID·역할·SHA·생성 시각에 계속 결속한다.

SYSTEM/설치 사용자 공용 update/trial mutex는 일반 사용자에게 부여한 SYNCHRONIZE·MUTEX_MODIFY_STATE(0x00100001)만 요청해 다시 연다. busy·abandoned·native wait 오류는 각각 구분하고 전환을 허용하지 않는다. 경로별 새 native mutex의 일반 사용자 재열기는 호스트에서 PROVEN이며 실제 SYSTEM↔사용자 경합은 랩 NOT TESTED다.

설정은 출시 시 builder 입력이다. 순수 HTTPS·기존 GitHub 거절·정확한 필드/형식 검증을 적용한다. canonical 설정 SHA를 packet 추가 파일 선언과 설치기의 보호 policy에 묶고 설치 뒤 원문·형식·policy 설정이 바뀌면 시작/적용을 거절한다.

요구: 로그온 SYSTEM 작업은 보호된 `current/tools/apply_prepared_update.ps1`만 실행한다. 고정 `current` bootstrap은 `versions/<source SHA>`의 활성 코드를 선택한다. 사용자 데이터·장비 신원·미완료·outbox·대기 전송은 복사하거나 되돌리지 않는다. 준비 패킷과 내구 전이 기록을 GUI/relay 시작 전에 확인한다. 업무 중 적용은 기본 끔이며 켜기를 거절한다.

구현: `auto_update.py`는 기존 LM HTTPS·ES256 manifest·archive 검증을 재사용한다. 회사 feed는 기존 사용자 장비 인증과 같은 HTTPS origin만 사용한다. `authorize_feed_transition`은 신뢰 키로 인증된 manifest의 정확한 두 방향 from/to source commit·실제 재계산 계약 원문 SHA, 검증된 stock version 및 빌드 literal과 서명 version의 일치, 현재 stock 판보다 엄격히 높은 숫자 version을 요구한다. 설치 당시 알려진 쌍 목록을 policy에 요구하지 않는다. 소비자 원문 data/API SHA와 같음/다름은 전이 증거이며 원문 동일성은 승인 gate가 아니다. wildcard·단방향·다른 실제 from·같거나 낮은 버전·옛 feed 재생·다른 키·손상 패킷은 거절한다.

쌍 승인은 출시 컷 `w9setupprep`의 독립 검토와 중앙 서명으로 표현한다. 독립 검토는 데이터 형식·API의 양방향 불변과 보존을 확인한다. 이는 GUI 핫픽스의 정확한 미래 쌍을 승인하는 방식이며 schema/API 변경의 일반 승인이 아니다. 별도 승인 의식·receipt 체계는 추가하지 않는다. policy는 설치기가 packet의 feed/channel/user/staging 설정으로 초기화하고 기존 preimage를 보존한다. SYSTEM apply는 policy를 읽고 SHA를 기록하며 확인된 역방향 권한은 transition에 남긴다. policy 승인 목록을 쓰지 않는다.

적용/복귀 기록을 flush·write-through rename한 뒤 코드 포인터를 바꾼다. GUI READY는 nonce·SHA·PID/생성 시각·기동 시각, 읽기 전용 ledger/queue·미완료 확인과 일반 사용자 relay의 같은 버전/nonce에 묶는다. trial 입력은 보호된 CONFIRMED/ROLLED_BACK와 활성 SHA 확인까지 잠근다. 일반 회복 실행은 위의 active·현재 G ALLOW·ordinary cohort 검증을 별도로 만족해야 한다. READY 실패 뒤 검증된 writer 부재·정상 종료 및 실제 G 불변/이전 판 ALLOW를 증명할 때만 복귀하고 실패 SHA를 보존한다. CONFIRMED 뒤 사용자가 옛 feed로 임의 하향하는 경로는 없다.

설치기는 SYSTEM/SID-logon 작업과 policy/active/transition preimage를 보존하고 정확한 소유 작업만 다룬다. 옛 Run은 실제 옛 명령으로 대조한다. 상승 설치기의 정방향/복귀 relay는 같은 native shell-token 경계·DuplicateTokenEx 0x18B 및 자식 SID/세션/비상승/무결성 8192 readback을 사용한다. Python 시작도 같은 보안 조건을 검사한다. 추가 실패 진단은 호출 이름·Win32 번호·원 예외 사슬·권한 값을 가린 원 메시지를 보존한다. 유한 helper/task/relay-cycle 실패는 exit와 stdout/stderr를 각각 8KiB까지 보존하고 persistent-cycle 상태에는 별도 실패 증거 경로를 남긴다. 상주 일반 자식의 stream은 별도 logging/launcher 경계 교정이 OPEN이며 실제 native 자식 시작은 랩 NOT TESTED다.

기존 release-pair/companion/fence 선언을 갱신한다. 실제 e88011c 공개 설정에서 승인 LM ES256 공개 설정으로의 정확한 SHA256 쌍만 첫 수동 설치의 별도 회전으로 허용한다. 같은 공개 키 전이는 유지하고 임의 회전은 거절한다. w9next 소스에는 공개 설정이 추적되지 않으므로 실제 w9next packet matching-key 확인은 중앙 NOT TESTED 항목이다. 개인 키·런타임 데이터는 소스에 넣지 않는다.

지원 수명주기: 정상 current-owner A/active B에서 실제 B→C 설치·managed 등록을 허용하고, 손상/중간 상태의 설치는 지원 제거 후 재설치를 안내한다. 지원 제거와 사용자 제거는 control 기록의 일관성을 요구하지 않는다. 지원 제거는 내용 SHA가 증명한 파일만 이동하고 목록 메타데이터는 마지막에 대조한다. 미확정 파일은 표식·목록을 보존하고 제거 미완을 알린다. 기존 정확한 deployment delegation과 옛 첫 등록 경계를 유지한다. [쌍 계약](PAIR-RULE-CONTRACT.md)·[지원 수명주기](LIFECYCLE-DESIGN.md)의 구현과 실제 Windows 수용을 구별한다.

증거: 새 순수/주입 경계·독립 clone 집중 비교와 기존 source-derived inventory 핀 확인만 이 레인의 PROVEN 대상이다. 전체 suite·stock builder·실제 packet·작업 등록/ACL·CreateProcessWithTokenW·GUI·VM 수명주기·독립 검토는 NOT TESTED이며 중앙 랩 수용이 필요하다.
# 설치·지원 공개 실패 기록 (2026-10-06)

독립 production Uninstall은 bootstrap·설치본 코드 helper를 싣지 않는다. SHA는 이미 정의된 동일한 `Get-EarlyFileSha256`을 사용하며, 진단에는 스크립트 위치 기준 `tools/failure_redaction.ps1`·rules의 고정 SHA256을 먼저 확인한 뒤 기존 load 한 곳을 사용한다. 두 파일이 바뀌면 `INSTALL_THIS_PC.ps1`의 `UninstallRedactionPins`도 함께 갱신해야 하며 pin 일치 시험이 이를 검사한다. 불일치는 실행 전 기존 공개 실패 코드·REPORT_UNAVAILABLE·고정 소스 위치로 거절한다.

등록/제거의 실패 원문은 같은 시도의 기존 보호 보고서에만 가린 traceback으로 보존한다(UTF-8 64 KiB 상한). 공개 stderr `LABEL_MATCH_INSTALLER_FAILURE=`는 코드·고정 안내·예외 형식·소스 위치만 받으며, 그 시도가 원자 게시한 보고서의 `report_path`/`report_sha256`만 첨부한다. 게시 실패·조기 거절은 `REPORT_UNAVAILABLE`이며 옛 보고서 경로를 재사용하지 않는다. 성공 출력과 exit, 지원 제거가 읽는 거래·복구 metadata는 보존한다. [현재 기록 계약](assembly_failure_contract.md)을 따른다.

### D-16 E1 · apply 시작 증거(23:0x 정정)

60초는 SYSTEM apply 시작 claim이 없는 상한이다. Authority 뒤 보호 apply-claim에 PID·native creation identity를 기록한다. LIVE/UNKNOWN 소유자는 기존240초 창에서 기다리며 느린90초 apply 동안 일반 실행을 시작하지 않는다. 정상 종료는 기존 guarded sink로 claim을 SETTLED 기록해 증거를 보존한다. 시험을 본 같은 bootstrap도 FAILED/UNKNOWN에서 claim이 NONE/ABSENT이고 일반 writer가 없으며 active G ALLOW·prepared 부재이면 현재 판을 일반 실행한다. 240초 뒤에도 UNKNOWN이면 update_lock 안에서 claim과 수정 시각을 다시 확인한다. 수정된 지 15분을 넘긴 claim만 같은 writer·G·prepared 조건으로 `UPDATE_SUPERVISION_UNAVAILABLE` 일반 실행을 허용하며 같은 claim hash의 지원 코드는 한 번 기록한다. LIVE·신선한 UNKNOWN·시각 읽기 실패·G 거절은 진행 안내+20을 유지한다. 이 복귀는 포인터·시험·READY·CONFIRMED를 쓰지 않는다.

사망한 SYSTEM의 같은 claim은 같은 세션에서 재요청하지 않고 현재 G 허용 판을 일반 실행한다. 지원 코드는 같은 protected claim hash에 대해 한 번 게시하며 다음 로그온의 기존 SYSTEM 작업이 새 claim을 덮어써 재시도한다(1차 결정 msg_c93ef274762a).


### Bootstrap relay 시작의 일시 admission 대기

Run 및 GUI가 만든 bootstrap의 relay 실행은 `WriterFencedError` 중 `WRITER_GATE_TIMEOUT`만 기존 240초 창에서 0.1초씩 쉬며 재시도한다. 각 시도는 기존 launch_version의 admission을 다시 얻으며 시도 사이에는 admission을 보유하지 않는다. 상한은 240초 + 마지막 admission 대기 최대 5초이며 마감 뒤에는 기존 실패 출구를 유지한다. fence 확정·버려진 소유권 등 다른 거절과 GUI 실행 자체의 timeout은 재시도하지 않는다. 포인터·trial claim·READY·CONFIRMED·G 허가·cohort·잠금 순서는 유지한다. bootstrap 밖의 relay launcher admission과 실제 로그온/pythonw 수용은 별도 범위다.

### 원격 앱 멈춤과 SQLite 연결 정리

`user_relay.py --request-app-stop`은 같은 설치 소유자의 relay root `control/label_match_app.stop.json`에 작은 요청을 원자 저장하고 기존 relay stop을 요청한다. GUI는 기존 5초 상태 갱신에서 요청을 읽고 활성 입력·저장 대기·보류 처리·관리자 작업이 없으면 기존 `on_closing(_confirmed=True)`로 정상 닫기를 진행한다. 작업 중에는 요청 ID별로 이벤트 `멈춤 요청 미룸`과 현재 상태 checkpoint를 기록하고 종료 안내를 계속 표시하며 스캔을 유지한다. 기록 실패도 입력을 막지 않는다. 작업이 끝나도 자동 종료하지 않으며 현장 `[종료]`가 평소 닫기를 호출한다. 미룬 요청은 실제 Tk 종료 뒤 같은 ID만 정리하고 닫기 거절·후속 요청은 보존한다. 기존 확인 동작과 포인터 동의가 우선하며 종료 버튼은 스캐너 초점을 가져가지 않는다. CLI의 `REQUESTED`는 GUI 종료 증명이 아니고 relay의 `UNKNOWN`은 종료 확인 실패다.

CLI의 요청 시각은 마이크로초를 유지한 UTC aware 값이다. GUI는 application import 전에 기록한 자기 프로세스 시작 시각보다 앞선 요청을 같은 ID 확인 뒤 삭제하고 무시한다. 시작 뒤 요청만 정상 닫기로 넘기며, 시간대 없는 시각·잘못된 시각은 요청을 접수하지 않는다. 앱이 꺼져 있을 때 남은 요청이 다음 실행을 닫지 않는다.

앱·relay의 쓰기용 SQLite 연결은 기존 commit/rollback 뒤 `wal_checkpoint(TRUNCATE)`를 시도하고 닫는다. checkpoint의 대기 시간은 0이며 BUSY·오류·쓰기 허가 거절은 유형만 기록하고 close를 계속한다. 읽기 전용 relay 연결에는 checkpoint를 하지 않는다. GUI worker·로그 writer 정리 뒤와 relay lease 해제 전에 기존 DB만 쓰기 연결로 다시 열어 마지막 읽기가 만든 sidecar를 정리한다. 없는 DB는 만들지 않는다. 정상 종료 뒤 다른 연결이 없으면 WAL/SHM이 사라지며, 남아 있으면 종료·되돌림의 성공 근거로 삼지 않는다.

### 처리 중 오류 뒤 다음 세트 계속

처리 줄 고장은 시각·작업·세트·세대·바코드와 UNKNOWN을 먼저 내구 감사 기록에 남긴다. 요청은 기존 관리자 보류 표에 보존하고, 옛 작업자의 STOP/join 완료(기존 최대 2초)를 확인한 뒤 현재 파일 원본을 검증해 격리하고 세대를 올려 새 줄을 만든다. 기록·보존·종료 확인 실패, 식별 가능한 원본 부재, 10분 안 세 번째 고장 또는 같은 세트 두 번째 고장은 입력을 계속 막는다. 보류 세트는 완료 전송과 배경 확인·복원 후보에서 제외하며 원래 상태·수량·원장을 바꾸지 않는다. 고장 난 옛 줄의 늦은 결과는 정산·화면에 적용하지 않고, 정상 줄의 기존 세대 정산 계약은 유지한다.

요청 세대 또는 세트 ID가 현재 입력과 다르거나 화면 밖의 보류 검증 작업이면 자동 재개하지 않는다. 고장 기록 뒤 현재 입력을 그대로 두고 기존 관리자 안내로 멈춘다.

동일 품목 코드의 새 세트는 기존 중복 검사를 그대로 거친다. 격리한 원래 세트 ID는 보류 해소 전 자동 검증·물질화에서 제외하며, 품목 코드 전체를 막지 않는다.

처리 앱은 lane 생성 직후부터 초기화 예외와 모든 종료 길에서 기존 lane의 접수를 닫고 stop을 보낸다. 웹 관리자 재조회는 빈 입력을 거절하고, 추가 인증이 필요하면 웹 결과로 돌려준다(Tk 모달 0). 완료 값은 성공·실패·미확인으로 구분하며 오류 문자열은 성공으로 표시하지 않는다.

취소 완료 판정은 _delete_selected_row 업무 함수의 명시 bool 반환과 실제 목록에서 지운 수를 함께 확인한다. None은 확인 대기/미확정이며, 오류·부분 삭제·중앙 충돌은 성공으로 바꾸지 않는다. 대화상자 표시 목록의 길이나 Button.invoke 문자열 반환은 완료 증거가 아니다.


관리자 SSH/Session 0 machine-worker 길은 다른 작업자 세션으로 relay를 실행하지 않는다. 성공은 PASS_MACHINE_WORKER_PENDING/첫 일반 실행을 유지한다. 실패에는 코드·startup preimage·담장 자동 복원 사실과 값 없는 작업자 안내를 남기며, 끝 relay READY 의무는 일반 작업자·같은 세션 설치 길에만 적용한다. 현장 지원은 코드 전용 제거 후 새 설치, 업데이트는 apply_prepared를 사용한다.


## v2.0.107 조립 관문

v2.0.107 조립의 앱 판·자료 판·data_compatible_min_version 하한은 모두 v2.0.107이다. 빌드 --auto-update-settings 입력은 w9mgrweb/field-update-feed-1015/label_match.auto-update-settings.json 원본 파일 그대로이며 SHA256 b8b0cfa0bdc90cb123299a01a9143a723c528ebb32c70a4104e76e7885b101a7에 결속한다. feed_url=https://worker.kmtecherp.com/static/update-feed/channels/label_match/stable/latest.json, channel=stable, profile_auth=true. 내보낼 ZIP을 E:에 풀어 app/auto-update-settings.json을 입력 파일과 바이트로 대조하고 그 runtime으로 import·탐침 관문을 실행한다.


기계 installer authority는 OS KnownFolder의 ProgramData 아래 KMTech/Label_Match/install-audit/machine-writer-<run>에 결속한다. ProgramData와 그 위는 특권 owner 종류만, 그 아래 공용 조상은 rename·삭제·DACL·owner 변경 mask 0x500D0040으로 검사한다. 회차별 fence root는 생성 직후 상속을 끊고 SYSTEM/Administrators만 쓰도록 보호하며, root와 active 기록은 기존 엄격한 쓰기 거절을 적용한다. C:/KMTech/Apps의 installer owner record는 이 예외 없이 기존 판정을 유지한다. 부모의 배치·복원 readback 두 호출은 이번 회차 위임 네 이름을 자식 환경에만 전달하고 finally 복원한다.
