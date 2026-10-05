# 조립 실패 기록 및 제품 상태 계약

2026-10-06 REDACTION-CONTRACT SHA256 `06168bf7039acc37cf216283ecf30bc785ff3bdea9487f19d3c35b4bc1225ffc`을 따른다.

환경 이름이 `_TRANSACTION_ID`, `_SESSION_ID`, `_ATTEMPT_ID`로 끝나고 값이 엄격 UUID 또는 32/64자리 16진수일 때만 비밀 값 치환에서 제외한다. `_PID`, `_START`, `_MUTEX`도 계약의 엄격한 값 모양과 이름 끝이 함께 맞을 때만 제외한다. 명시 비밀 이름은 예외를 받지 않는다. Python과 PowerShell은 같은 14개 합성 벡터를 사용하며 식별자 값 규칙은 공통 rules JSON에서 읽는다. PowerShell child 출력 reader에도 같은 판정 함수를 전달한다.

텍스트 가림은 key와 value를 분리해 검사한다. `RuntimeError:` 같은 일반 접두문 뒤의 assignment·인용 mapping도 계속 검색하며, 비밀 value와 metadata value는 경계를 보존한다. status·JSONL 로그·보호 진단 및 PS5.1/PS7 child capture의 최종 변환에 같은 규칙을 적용한다. 길이 제한과 traceback 맥락은 유지한다.

C# bounded capture는 일반 접두문 뒤의 인용/비인용 assignment도 잘림 전에 가린다. chunk를 넘는 value와 이미 가린 환경 이름 표지를 보존하며, 비밀 배열·객체 및 metadata 예외는 기존 경계를 따른다. C# capture/최종 변환에 같은 14 벡터와 추가 4벡터(예외 접두문 2, 64 KiB 초과 plain/접두문 assignment 2)를 적용한다. 최종 sink가 정상 JSON을 다시 직렬화하는 경우에는 필드 값의 정확한 일치로, 일반 텍스트와 잘림 속성은 원 기대값으로 비교한다.

| writer / 파일 종류 | 처리 |
| --- | --- |
| `Save`: 설치 audit, evidence, 실패 진단 | 공용 가림 함수, 진단 문자열 상한 적용 |
| `Save-LabelMachineMetadata`: 첫 등록 격리 진행 기록 | 원 UTF-8 JSON 바이트 보존, 쓰기 전에 비밀 값 존재 시 거절 |
| `Save-FirstRegistrationAttempt` → `Save-LabelMachineMetadata`: 첫 등록 시도 기록 | 동일. 시도·거래·세션 결속 값을 보존 |
| `Write-BootstrapUtf8Json`: bootstrap 무결성 기록 | 원 UTF-8 JSON 바이트 보존, 쓰기 전에 비밀 값 존재 시 거절 |
| `direct_sync_relay_install_pack._write_json`: `--report-path` 진단 보고서 | 기존 공용 가림 유지. 제품 설정 writer가 아님 |
| `current_user_onboarding._write_machine_json_atomic`: settings | 원 직렬화 UTF-8 보존, 쓰기 전 비밀 값 거절. report writer와 분리 |
| `user_relay._write_machine_json_atomic`: relay stop marker | 동일. 원 요청/lineage 결속을 보존하고 진단 status/evidence writer와 분리 |
| `auto_update.write_json_durable`: transition/active 및 machine record | 원 직렬화 UTF-8 보존, 쓰기 전 비밀 값 거절. `failure-<32 hex>.json`만 고정 진단 파일 종류로 가림 |

제품 상태에는 가림이나 64 KiB 상한을 적용하지 않는다. 비밀 쓰기 거절은 디렉터리·임시 파일·기존 대상 변경 전에 발생한다. 회귀는 두 metadata writer 각각에 대해 PS5.1/PS7의 64 KiB 초과 정확한 바이트 일치와 합성 비밀의 원문·JSON escape 거절을 확인한다. 공용 `kmtech_shared`는 변경하지 않는다.

rollback 시험 입력은 조립 판의 phase stack 및 공식 옛 relay 조회 의존성을 제공한다. 복원 정합이 증명된 경우 기존 G의 fence 해제·relay 재개 주장은 유지하고, 안내는 r4의 `RUN_RESTORED_RELAY_NEXT_LOGON` 계약을 확인한다. 정합 거절에는 관리자 복구 안내, fence 유지, relay 재개 0을 확인한다.

등록 storage 선검사는 junction의 `WriterFenceError`에도 가린 마지막 오류 줄과 공통 자료 보존·지원 복구 안내를 전달하고 exit 2로 거절한다. 오류 종류에 관계없이 정체 판독·등록·변경보다 먼저 거절하는 경계는 유지한다.

순수 G의 명시 서명 역방향 회귀는 실제 전체 `362f30f` 소스를 자체 표준 builder로 만든다. audit-index companion `ac745f0` 전체 소스를 순수 G라고 취급하지 않는다. 조립 후보의 companion 역방향 거절 단언과 r4 시험 단언은 유지한다.

기존 정방향 회귀의 v2.0.95 조상 `a56a58769c43d1b19abdd9881425f95a49dbd7f7`는 원 표준 stock의 before 바이트를 보존하는 정확한 조립 선언만 추가한다. 현장 해당 없음: 설치 실측 입력이 아닌 기존 승인 G 시험 입력이며 G/min 및 역방향 허용은 바꾸지 않는다. TEST1은 새 설치이므로 별도 조상 선언이 없다.

같은 판 번호의 `2c46de16f2303117e88d4794bf3793f0d7de8c76`도 기존 `test_data_generation_pin_upgrade_pair.py:22`가 전체 소스의 정방향을 단언한다. 해당 원 표준 stock before를 결속하는 정확 선언으로 보존한다. 현장 해당 없음이며 역방향/변조 거절은 그대로다.
