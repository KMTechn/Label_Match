# 조립 실패 기록 및 제품 상태 계약

2026-10-05 REDACTION-CONTRACT SHA256 `b507945ad94ad9f0e3f935b25a2e1cdc210ff9daa15902f9750a14589faa3f30`을 따른다.

환경 이름이 `_TRANSACTION_ID`, `_SESSION_ID`, `_ATTEMPT_ID`로 끝나고 값이 엄격 UUID 또는 32/64자리 16진수일 때만 비밀 값 치환에서 제외한다. 명시 비밀 이름은 예외를 받지 않는다. Python과 PowerShell은 같은 12개 합성 벡터를 사용한다. PowerShell child 출력 reader에도 같은 판정 함수를 전달한다.

| writer / 파일 종류 | 처리 |
| --- | --- |
| `Save`: 설치 audit, evidence, 실패 진단 | 공용 가림 함수, 진단 문자열 상한 적용 |
| `Save-LabelMachineMetadata`: 첫 등록 격리 진행 기록 | 원 UTF-8 JSON 바이트 보존, 쓰기 전에 비밀 값 존재 시 거절 |
| `Save-FirstRegistrationAttempt` → `Save-LabelMachineMetadata`: 첫 등록 시도 기록 | 동일. 시도·거래·세션 결속 값을 보존 |
| `Write-BootstrapUtf8Json`: bootstrap 무결성 기록 | 원 UTF-8 JSON 바이트 보존, 쓰기 전에 비밀 값 존재 시 거절 |
| `direct_sync_relay_install_pack._write_json`: `--report-path` 진단 보고서 | 기존 공용 가림 유지. 제품 설정 writer가 아님 |

제품 상태에는 가림이나 64 KiB 상한을 적용하지 않는다. 비밀 쓰기 거절은 디렉터리·임시 파일·기존 대상 변경 전에 발생한다. 회귀는 두 metadata writer 각각에 대해 PS5.1/PS7의 64 KiB 초과 정확한 바이트 일치와 합성 비밀의 원문·JSON escape 거절을 확인한다. 공용 `kmtech_shared`는 변경하지 않는다.

rollback 시험 입력은 조립 판의 phase stack 및 공식 옛 relay 조회 의존성을 제공한다. 복원 정합이 증명된 경우 기존 G의 fence 해제·relay 재개 주장은 유지하고, 안내는 r4의 `RUN_RESTORED_RELAY_NEXT_LOGON` 계약을 확인한다. 정합 거절에는 관리자 복구 안내, fence 유지, relay 재개 0을 확인한다.

등록 storage 선검사는 junction의 `WriterFenceError`에도 가린 마지막 오류 줄과 공통 자료 보존·지원 복구 안내를 전달하고 exit 2로 거절한다. 오류 종류에 관계없이 정체 판독·등록·변경보다 먼저 거절하는 경계는 유지한다.
