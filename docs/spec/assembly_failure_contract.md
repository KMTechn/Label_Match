# 조립 실패 기록 및 제품 상태 계약

오디오 재생·장치 초기화는 선택 피드백이다. 장치 실패 뒤에도 스캔 checkpoint, 트레이 durable 완료·로그, 동기화 시작, 오류 복구와 다음 스캔을 진행한다. 이전 모달 창 생성 실패 복구의 stop 예외도 현재 트레이 리셋을 막지 않는다. headless 가짜 PlaySound 회귀와 실제 장치/서버 미시험의 경계는 sub-325 REPORT.md에 기록한다.

stock 빌더가 배포하는 .ps1/.psm1/.psd1의 비ASCII 내용은 UTF-8 BOM(EF BB BF)을 보존한다. 정적 경비는 빌더의 script 경로와 실제 application/runtime/third-party 복사 입력을 추적하며 runtime Activate.ps1도 포함한다. PS5.1 회귀는 BOM 유무에 따라 UTF-8/cp949로 디코드한 본문을 Parser.ParseInput에 넣어 구문 오류 0과 UTF-8 기준 한국어 리터럴 일치를 요구한다. 65001 호스트의 기본 파일 판독만으로 cp949 현장 호환을 증명하지 않는다. 임시 사본의 BOM 제거 RED와 실제 stock source GREEN은 sub-325 REPORT.md에 기록하며 원본을 바꾸지 않는다.

구조화 가림의 추가 범위는 REDACTION-CONTRACT 0f(SHA256 c82c9bdccd16bb0caa0e423fb7578278646ba0e40ac18bd58291c00e70946ccf)의 동결 4벡터를 따른다. 아래 0e 기록은 조립 v2의 역사적 범위로 보존하며 10/8 컷 전 통합은 독립 검토 P1·P2 0 후 코디가 수행한다.

| 조기 실패 reason_code | 뜻과 고정 안내 |
| --- | --- |
| `SERVER_BASE_URL_INVALID` | 서버 URL origin 문법 또는 port 범위 거절. 서버 주소는 인증정보가 없는 HTTPS 호스트 주소로 입력하세요. |

잘못된 서버 URL은 첫 판독·PlanOnly·side effect 전에 거절한다. 가림 helper를 아직 읽지 못한 fallback에서도 이 코드와 위 고정 안내, FAILED/REPORT_UNAVAILABLE만 내며 URL 원문과 이전 보고서 참조는 공개하지 않는다. 코드와 안내는 공용 rules의 allowed_codes/safe_messages에 등록한다.

2026-10-06 REDACTION-CONTRACT SHA256 `a8708fd307dd55ae309f5f18a0b3dac603302862a6d269a9c815862112f137e7`의 0e 컷 전 범위를 따른다. 04:0x 지시에 따라 설치기·업데이트 적용기·지원 도구에서 예외 문장·child/프로세스 출력·명령줄·외부 입력값을 포함할 수 있는 공개 자리만 허용 필드 공개 레코드로 바꾼다. 고정 문구와 자체 계산한 개수·ID·코드·시각·경로를 내는 줄은 80a93ab 바이트를 유지하고 `tools/failure_redaction_rules.json`의 `safe_messages`에 등록한다. 업무 앱 로그와 스캔 화면은 이 범위 밖이다.

`public_record` / `ConvertTo-KmtechPublicRecord`는 코드·고정 문구·형식 이름·위치·정수·SHA·식별자·시각·절대 경로·bool/null만 받는다. 정확한 키가 suffix보다 먼저이며, 거절한 값은 내보내지 않고 유효한 키 이름과 `PUBLIC_FIELD_NONCONFORMING`만 남긴다. 상세 가림과 64 KiB 제한은 기존 보호 보고서/private evidence에서 유지한다.

등록/제거 CLI 실패는 stderr의 `LABEL_MATCH_INSTALLER_FAILURE=<compact JSON>`으로 원인을 공개한다. `onboarding_status`/`current_user_removal_status`, 고정 틀의 report 경로 줄 및 성공 stdout/exit는 유지한다. 이 경로 안내 줄은 게시 증명이 아니다. 공개 실패 레코드의 보고서 경로/SHA는 이 예외를 처리하는 시도가 원자 게시한 실제 UTF-8 바이트에만 결속한다. 게시 실패 또는 조기 자료 판 거절에는 `REPORT_UNAVAILABLE`만 남기고 옛 보고서를 참조하지 않는다. 같은 onboarding/removal 보고서에 가린 첫 원인 traceback을 보존한다.

지원 제거가 다시 읽는 audit의 중첩 거래·복구 필드는 1a machine metadata로 보존하며 전체 파일을 평탄화하지 않는다. 진단 하위 레코드와 안내만 공개 레코드로 바꾼다. 정확한 출력 자리 목록과 실제 검증 한계는 `work/program/w9setupprep/sub-310/INVENTORY.md` 및 `REPORT.md`에 기록한다. 호스트에서 실제 설치/ACL·GUI·VM·서버 수용은 수행하지 않는다.

환경 이름이 `_TRANSACTION_ID`, `_SESSION_ID`, `_ATTEMPT_ID`로 끝나고 값이 엄격 UUID 또는 32/64자리 16진수일 때만 비밀 값 치환에서 제외한다. `_PID`, `_START`, `_MUTEX`도 계약의 엄격한 값 모양과 이름 끝이 함께 맞을 때만 제외한다. 명시 비밀 이름은 예외를 받지 않는다. Python과 PowerShell은 필수 18개와 컷 뒤 구조화 4개 합성 벡터를 사용하며 식별자 값 규칙은 공통 rules JSON에서 읽는다. PowerShell child 출력 reader에도 같은 판정 함수를 전달한다.

텍스트 가림은 key와 value를 분리해 검사한다. `RuntimeError:` 같은 일반 접두문 뒤의 assignment·인용 mapping도 계속 검색하며, 비밀 value와 metadata value는 경계를 보존한다. status·JSONL 로그·보호 진단 및 PS5.1/PS7 child capture의 최종 변환에 같은 규칙을 적용한다. 길이 제한과 traceback 맥락은 유지한다.

보호 보고서의 민감 key 뒤 mapping·list·중첩 값은 인용·escape를 구별해 괄호 짝까지 통째로 가리고, 짝이 없거나 어긋난 값은 줄 끝까지 가린다. 구조 문법은 공용 rules의 `structured_delimiters` 하나에서 Python·PowerShell·C#으로 전달한다. 정상 값 뒤 후속 field와 원인 접두는 유지한다. 공개 허용 레코드·65,536바이트 상한·가림 후 절단 순서와 동결 공개 6벡터는 유지한다. 구조화 4벡터와 sink 셀의 실제 검증 결과·한계는 sub-325 REPORT.md에 기록한다.

C# bounded capture는 일반 접두문 뒤의 인용/비인용 assignment도 잘림 전에 가린다. chunk를 넘는 value와 이미 가린 환경 이름 표지를 보존하며, 비밀 배열·객체 및 metadata 예외는 기존 경계를 따른다. C# capture/최종 변환에 같은 14 벡터와 추가 4벡터(예외 접두문 2, 64 KiB 초과 plain/접두문 assignment 2)를 적용한다. 최종 sink가 정상 JSON을 다시 직렬화하는 경우에는 필드 값의 정확한 일치로, 일반 텍스트와 잘림 속성은 원 기대값으로 비교한다.

| writer / 파일 종류 | 처리 |
| --- | --- |
| `Save`: 설치 audit, evidence, 실패 진단 | 중첩 거래·복구 metadata 보존, diagnostic subrecord는 public record, 원문은 기존 private evidence |
| `Save-LabelMachineMetadata`: 첫 등록 격리 진행 기록 | 원 UTF-8 JSON 바이트 보존, 쓰기 전에 비밀 값 존재 시 거절 |
| `Save-FirstRegistrationAttempt` → `Save-LabelMachineMetadata`: 첫 등록 시도 기록 | 동일. 시도·거래·세션 결속 값을 보존 |
| `Write-BootstrapUtf8Json`: bootstrap 무결성 기록 | 원 UTF-8 JSON 바이트 보존, 쓰기 전에 비밀 값 존재 시 거절 |
| `direct_sync_relay_install_pack._write_json`: `--report-path` 진단 보고서 | 중첩 typed metadata와 고정 문구 유지, 가변 진단만 public record와 같은 시도 보호 evidence. 제품 설정 writer가 아님 |
| `current_user_onboarding._write_machine_json_atomic`: settings | 원 직렬화 UTF-8 보존, 쓰기 전 비밀 값 거절. report writer와 분리 |
| `user_relay._write_machine_json_atomic`: relay stop marker | 동일. 원 요청/lineage 결속을 보존하고 진단 status/evidence writer와 분리 |
| `auto_update.write_json_durable`: transition/active 및 machine record | 원 직렬화 UTF-8 보존, 쓰기 전 비밀 값 거절. `failure-<32 hex>.json`만 고정 진단 파일 종류로 가림 |

제품 상태에는 가림이나 64 KiB 상한을 적용하지 않는다. 비밀 쓰기 거절은 디렉터리·임시 파일·기존 대상 변경 전에 발생한다. 회귀는 두 metadata writer 각각에 대해 PS5.1/PS7의 64 KiB 초과 정확한 바이트 일치와 합성 비밀의 원문·JSON escape 거절을 확인한다. 공용 `kmtech_shared`는 변경하지 않는다.

rollback 시험 입력은 조립 판의 phase stack 및 공식 옛 relay 조회 의존성을 제공한다. 복원 정합이 증명된 경우 기존 G의 fence 해제·relay 재개 주장은 유지하고, 안내는 r4의 `RUN_RESTORED_RELAY_NEXT_LOGON` 계약을 확인한다. 정합 거절에는 관리자 복구 안내, fence 유지, relay 재개 0을 확인한다.

등록 storage 선검사는 junction의 `WriterFenceError`에도 허용 코드·고정 자료 보존/지원 복구 안내와 같은 시도 보호 보고서 참조를 전달하고 exit 2로 거절한다. 오류 종류에 관계없이 정체 판독·등록·변경보다 먼저 거절하는 경계는 유지한다.

순수 G의 명시 서명 역방향 회귀는 실제 전체 `362f30f` 소스를 자체 표준 builder로 만든다. audit-index companion `ac745f0` 전체 소스를 순수 G라고 취급하지 않는다. 조립 후보의 companion 역방향 거절 단언과 r4 시험 단언은 유지한다.

기존 정방향 회귀의 v2.0.95 조상 `a56a58769c43d1b19abdd9881425f95a49dbd7f7`는 원 표준 stock의 before 바이트를 보존하는 정확한 조립 선언만 추가한다. 현장 해당 없음: 설치 실측 입력이 아닌 기존 승인 G 시험 입력이며 G/min 및 역방향 허용은 바꾸지 않는다. TEST1은 새 설치이므로 별도 조상 선언이 없다.

같은 판 번호의 `2c46de16f2303117e88d4794bf3793f0d7de8c76`도 기존 `test_data_generation_pin_upgrade_pair.py:22`가 전체 소스의 정방향을 단언한다. 해당 원 표준 stock before를 결속하는 정확 선언으로 보존한다. 현장 해당 없음이며 역방향/변조 거절은 그대로다.

등록 도구의 source 고정 안내는 원 stdout과 중첩 report 문자열을 유지한다. 서버가 준 미등록 error code는 가변 진단으로 보호 보고서에 보존하며 HTTP 상태·거래·복구 metadata는 유지한다. exact-clone 지원 도구의 예외 레코드는 기존 `UNKNOWN`과 client/server state mutated·stop marker removed의 false 값을 유지한다. 실패 예외의 자유 문장을 검사하던 테스트만 실제 게시 SHA와 보호 trace 소비로 교정하며 기존 exit·원본 파일 보존·상태 변경 거절 단언은 유지한다.

초기 shared leaf pin 검사에는 가림 helper가 아직 없다. 최상위 trap은 `Get-LabelLaunchFailure`와 `ConvertTo-KmtechPublicRecord`가 모두 로드된 뒤에만 보호 보고서 경로를 호출하고, 그 전에는 기존 고정 `FAILED` / `INSTALLER_FAILED` / `REPORT_UNAVAILABLE` 레코드와 nonzero exit로 끝난다. 미로드 helper의 두 번째 예외로 첫 거절을 대체하지 않는다. pin 검사 순서와 G 판정은 유지하며, 이 조기 거절에 이전 보고서를 결속하지 않는다. 실제 stock PS5/PS7의 정상·한 바이트 변조 관측은 sub-315 REPORT에 보존한다.

PS7 placement child의 보호 trace는 원 `CreateNew` handle과 보호 ACL, 첫 원인 가림, durable flush, UTF-8 SHA 및 원 phase/exit를 유지한다. .NET Core에서 없는 정적 `File.SetAccessControl` 대신 같은 ACL을 `Set-Acl -LiteralPath -AclObject -ErrorAction Stop`으로 적용한다. sub-315의 실제 PS5.1 두 사례는 보호 trace를 저장했으나 PS7 두 사례는 0바이트/참조 없음으로 실패한 근거를 보존했다. 교정 뒤 양쪽 엔진의 실제 파일/SHA 및 공개 셀·142·stock 검증은 sub-315 REPORT에 별도로 기록한다.
