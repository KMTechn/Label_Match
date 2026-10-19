# 설치·업데이트 실패 기록의 비밀 가림 계약 — 다섯 앱 공통 (w9setupprep 안, 10-05 13:2x, 코디 #31)

**적용 범위:** 포장·이적(w9setupprep), 검사(w9scanonly), 리워크·불량 창고(w9rwdi)의 설치기·업데이트 적용기·지원 도구다. 각 앱 안에 같은 동작의 함수 하나를 둔다. 공용 helper(kmtech_shared) 바이트는 이번에 바꾸지 않는다. 같은 동작인지는 아래 시험 벡터 파일로 검증한다.

## 0. 공개 출력 = 허용 목록, 원문 = 보호 보고서 (10-06 01:2x 추가, 코디 — 가림 수렴; 이 절이 아래 절보다 앞선다)
금지 목록(비밀 모양 찾기)을 촘촘히 하는 방식은 라운드마다 새 P1 이 나와 끝나지 않는다. 그래서 **공개 출력은 허용 목록으로 바꾼다.** 아래 1–3절의 가림은 보호 보고서와 이중 방어로 그대로 둔다.

### 0a. 공개 출력이란
설치기·업데이트 적용기·지원 도구가 내는 것 가운데 **보호 보고서(0d)가 아닌 모든 것**: stdout·stderr 줄, status·summary JSON, audit·evidence·진단 파일(보호 위치가 아닌 것), 로그, 이벤트(Windows 이벤트 로그 포함), 화면·콘솔 안내. 1a machine metadata 는 공개 출력이 아니다(그대로).

### 0b. 공개 출력에 실을 수 있는 것 — 평평한 레코드 하나, 키는 `^[a-z][a-z0-9_]{0,63}$`
| 키 | 값 |
|---|---|
| `status`·`stage`·`code` 또는 `_status`·`_stage`·`_code` 로 끝나는 키 | 앱 source 의 **코드 상수 표**(`allowed_codes`)에 있는 문자열과 정확히 같은 것 |
| `guidance` 또는 `_message` 로 끝나는 키 | 앱 source 의 **고정 안내 문구 표**(`safe_messages`)에 있는 문자열과 정확히 같은 것(한국어 가능, 끼워 넣는 값 없음) |
| `exception_type` | 예외 형식 이름만 `^[A-Za-z_][A-Za-z0-9_.]{0,127}$`(메시지 0) |
| `location_module`·`location_function` | `^[A-Za-z_][A-Za-z0-9_.]{0,127}$` |
| `exit_code`·`location_line` 또는 `_count`·`_bytes`·`_ms`·`_line` 로 끝나는 키 | 정수 |
| `_sha256` 로 끝나는 키 | `^[0-9a-f]{64}$` |
| `_commit` 로 끝나는 키 | `^[0-9a-f]{40}$` 또는 `^[0-9a-f]{64}$` |
| `version` 또는 `_version` 로 끝나는 키 | `^v?[0-9]{1,5}(\.[0-9]{1,5}){1,3}$` |
| `_id` 로 끝나는 키 | 2a 식별자 모양만 — UUID 또는 32·64 자리 16진수 |
| `utc` 또는 `_utc` 로 끝나는 키 | `^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}(\.[0-9]{1,7})?Z$` |
| `_path`·`_dir`·`_root`·`_file` 로 끝나는 키 | 로컬 절대 경로 `^[A-Za-z]:\\[^\x00-\x1f"<>\|?*]{0,256}$` 또는 `^/[^\x00-\x1f]{0,255}$` |
| 아무 유효한 키 | `true`·`false`·`null` |
- **키 판정 순서**: 값이 `true`·`false`·`null` 이면 먼저 받는다. 그다음 **정확한 키 이름**(`status`·`stage`·`code`·`guidance`·`exception_type`·`location_module`·`location_function`·`exit_code`·`location_line`·`version`·`utc`)이 끝 이름 규칙보다 먼저다(예: `exit_code` 는 `_code` 로 끝나도 정수 칸). 그다음 끝 이름 규칙을 표 순서대로 처음 맞는 것 하나만 적용한다.
- 위에 없는 키·형식에 안 맞는 값·객체·배열·위 표 밖 숫자는 **싣지 않는다**(예: 예외 문장 `message`, child stdout/stderr 원문, 명령 출력, 환경 값, 파일 내용, URL). 비밀 숫자(PIN 등)가 숫자 칸으로 새지 않게 숫자도 표의 키에서만 받는다.

### 0c. 함수 모양과 안 맞을 때
- Python `public_record(fields, allowed_codes, safe_messages) -> dict`, PowerShell `ConvertTo-KmtechPublicRecord -Fields <IDictionary> -AllowedCodes <string[]> -SafeMessages <string[]>`. 표·패턴은 함수 안 상수 한 곳(코드·문구 표는 앱 상수).
- 결과 = 맞는 필드를 **입력 순서대로**. 안 맞는 필드가 하나라도 있으면 끝에 `nonconforming_fields`(유효한 키 이름만, 입력 순서) 와 — 키 이름 자체가 무효인 것이 있으면 — `nonconforming_invalid_key_count`(정수), 그리고 `public_record_code: "PUBLIC_FIELD_NONCONFORMING"` 을 붙인다. 실패 보고 자체는 막지 않는다(fail closed 는 값에만).
- 줄 형식(stdout·stderr·로그): `<앱 접두>=` + 그 레코드의 compact JSON(공백 없음, UTF-8, 줄바꿈 없음). 리워크 `REWORK_INSTALLER_FAILURE=` 와 같은 모양(기준 구현 `C:\company\program\Rework_worker` 953073f7c69d8f161ad1a10fe0f49bb4bbbf9845 `rework_worker/current_user_onboarding.py`).
- 시험 벡터: `W\w9setupprep\PUBLIC-OUTPUT-VECTORS.json`(5절과 같은 방식, 다섯 앱 실패 0).

### 0d. 보호 보고서
- 원인 전문(가린 traceback·child 출력·예외 문장)은 **같은 시도의 보호 보고서에만** 둔다. 보호 위치 = 그 앱의 기존 보호 위치(작업 사용자·관리자만 읽는 ACL 의 onboarding/removal 보고서, 설치기 private evidence — 3절 끝 줄). 보호 보고서에도 1–3절 가림·64 KiB 상한을 그대로 적용한다(이중 방어).
- 공개 레코드에는 그 보고서가 **원자적으로 게시된 뒤에만** `report_path` + 실제 저장된 UTF-8 바이트의 `report_sha256` 을 싣는다. 미뤄 둔 옛 보고서를 새 실패에 다시 쓰지 않는다. 게시 실패면 `report_status: "REPORT_UNAVAILABLE"` 코드만(원문은 공개로 내보내지 않고 버린다).
- 원인이 화면에 꼭 보여야 하는 곳은 그 원인을 **코드로 정하고 그 코드의 고정 한국어 안내를 문구 표에** 넣는다. 사람이 전문을 봐야 하면 안내에 `report_path` 를 싣는다. 이 방식으로 안 되는 요구가 있으면 그 레인이 question.

### 0e. 컷(10/8) 전 범위 — 다섯 앱 같은 선 (코디 10-06 01:5x, w9rwdi 안 승인)
- 공개 출력 자리를 전수 목록으로 (가) 예외 문장·child 출력·명령줄·가변 문자열을 넣는 자리 (나) 고정 문구·허용 필드만 내는 자리 (다) 판정 불가 로 나눈다. 목록 방식 본보기 = `W\w9rwdi\sub-256\INVENTORY.md`(직접 sink CSV + 간접·보호·typed 저장 JSON + 문구 소비자 파일:줄).
- **컷 전**: (가)만 0c 공개 레코드 함수 + 0d 같은 시도 보호 보고서로 바꾸고 벡터 RED→GREEN. **(가) 판정(코디 10-06 04:0x)**: 공개 출력의 가변 부분이 다섯 가지 — 예외 문장, child/프로세스 출력, 명령줄, 외부 입력값(설정·레지스트리·환경 변수·HTTP 응답 본문·사용자 입력) — 중 하나일 수 있는 자리만 (가)다. 우리 코드가 계산한 값(개수·ID·코드값·시각·우리가 만든 경로)만 들어가는 자리는 (나)로 보고 코드 변경 없이 그 문구 틀을 허용 필드로 safe_messages 표에 등록한다. 비밀이 공개 출력에 닿는 길은 이 다섯 가지뿐이다. (나)는 **코드 변경 없이** 그 고정 문구를 앱 `safe_messages` 표에 등록만(설치 화면·IT 카드·사용자 문서·영상 캡처가 컷 이틀 전에 흔들리지 않게). (다)는 컷 전에 0 으로(판정해서 가 또는 나로).
- 0b 의 전면 형식 통일(모든 공개 출력을 평평한 레코드 한 모양으로)은 **컷 뒤**.
- 독립 검토는 전수 목록으로 (가) 0 을 확인한다.

### 0f. 가림 멈춤 규칙 (코디 10-06 03:4x, 다섯 앱 공통)
- 가림 빈틈 검토는 10-06 라운드가 마지막이다. 그 뒤 새 모양이 나오면 — **공개 sink**(화면·status·log·stdout·이벤트, 보호 위치 아닌 audit·evidence·진단)에 닿으면 **P1** 로 고치고, **보호 보고서에만** 남으면 **P2** 로 기록해 컷 뒤로.
- 마지막 라운드의 부류(예외 접두문 뒤 민감 키의 값이 mapping·list·중첩·짝 안 맞는 괄호 — w9rwdi sub-260 발견)는 **RED 확인용 벡터** `W\w9setupprep\REDACTION-VECTORS-STRUCTURED.json`(4개)로 둔다. 컷 전 필수 PASS 묶음(`REDACTION-VECTORS.json`)이 아니다 — 각 앱이 모든 sink 에서 돌려 위 기준으로 판정한다. 포장 실측(sub-318): 공개 출력 전환 전 판은 공개 sink 에 조각이 남고(P1 — 0절 (가) 전환으로 닫힘), 전환 판은 공개 0·보호 보고서만 RED(P2, 컷 뒤).

## 1. 어디에 적용하나 — sink
아래 sink 에 **쓰기 직전**에 가림 함수를 거친다.
- 실패 summary JSON(예: `installation_failure_summary`)
- audit·evidence·진단 파일
- child stdout/stderr 를 담는 파일
- 로그
- 사용자·관리자 안내 문구
- 원문(가리기 전 텍스트)은 임시 파일을 포함해 어디에도 쓰지 않는다.
- 사용자·관리자 안내에는 child 출력 전문을 넣지 않는다. ~~가린 뒤 마지막 예외 줄 하나(최대 300자)~~ → **0절로 대체(10-06): 안내는 고정 문구 표 + 코드 + 보호 보고서 경로만, 예외 문장 0.** 1–3절은 보호 보고서와 이중 방어에 적용한다.

### 1a. sink 가 아닌 것 — 제품 상태·인증 machine metadata (10-05 19:5x 추가, 이적 sub-350 발견)
- 대상: 다른 코드가 다시 읽어 **검증·결속에 쓰는** 파일 — 무결성 기록(`bootstrap-integrity.json` 등), manifest·inventory·선언·lock, 보호 journal·receipt·transition record, 설정·profile 같은 제품 상태. (audit 파일 안의 **진단 필드**·summary·stdout/stderr·로그·최종 throw 문구는 1절 sink 그대로다.)
- 처리: 가림 함수·3절 길이 상한을 **적용하지 않는다** — 원 바이트 그대로 쓴다(가림·잘림이 JSON 을 깨거나 결속 값을 바꿔 정상 복원·G 가 거절된 실측: 이적 stock 전체 inventory 가 64 KiB 를 넘어 잘림 → JSONDecodeError).
- 대신 **쓰기 직전 비밀 단언**: 쓸 바이트(UTF-8, JSON escape 형태 포함)에 2a 대상 환경 값(식별자 예외 제외)이 하나라도 들어 있으면 **쓰지 않고 거절**(fail closed, 기존 오류 계열·상태 변경 0). 가려서 쓰지 않는다.
- 이 구분은 파일 종류로 정하고(쓰는 함수·경로 목록을 앱마다 표로), 같은 파일을 경우에 따라 다르게 다루지 않는다.


### 1a-31. 다음 판 본 판 — 명시 환경·credential 경계 (2026-10-19, sub-412 답 1·3·4)
- Python `assert_metadata_has_no_secrets(text, environ=None)`와 PowerShell `Assert-KmtechMetadataHasNoSecrets -Text <string> -Environment <IDictionary>`는 **명시로 받은 환경만** 검사한다. 생략·`None`·`$null`은 빈 환경이며, 프로세스 환경이나 내부 환경 캐시로 대체하지 않는다. 제품 호출자는 사용 중인 환경을 명시로 넘긴다. 진단 redact 함수의 기본 환경 동작은 그대로다.
- 2a 대상 값 중 길이 **4 미만은 metadata 단언에서도 건너뛴다**. 언어별로 진단 redact와 같은 짧은 값 판정 함수를 쓴다. metadata에는 note를 붙이거나 바이트를 바꾸지 않는다. 길이 4 이상 값은 기존대로 원문·escape 변형 중 하나라도 있으면 거절한다. 흔한 네 글자 값 `true`도 본 판에서는 거절한다.
- credential·DPAPI·key 저장은 machine metadata가 아니다. credential writer에는 metadata 단언을 적용하지 않는다. credential JSON 구조·비밀 값·ACL·원자 교체·읽기 재검증·실패 되돌림은 기존 동작을 보존한다. credential 내용을 진단 sink에 복사하지 않는 2c는 그대로다.
- 거절 오류 문구는 Python `Machine metadata contains a known secret value`, PowerShell `Machine metadata contains a known secret value.` 그대로다. 환경 이름·값을 오류에 넣지 않는다.
- 공통 벡터는 기존 최상위 `vectors` 18개를 그대로 보존하고 `metadata_vectors` 배열을 더한다. 각 항목은 `id`, `operation`, `text`(JSON 텍스트), `environ`(합성 환경 또는 null), `expected`(`pass`/`reject`)다. `operation`은 `assert_metadata_has_no_secrets` 또는 `credential_write`다. 전자는 각 언어의 단언, 후자는 앱의 기존 credential writer와 읽기 검증을 시험한다. credential writer가 없는 언어 helper에는 단언을 새로 끼워 넣지 않는다.
- 본 판과 선택 판은 별도 커밋·디렉터리의 정확한 UTF-8/LF 계약·벡터 바이트 및 `CONTRACT_SHA256`·`VECTORS_SHA256`으로 배포한다. 선택 판의 JSON 단어·숫자 경계 예외는 본 판에 포함하지 않는다.

## 2. 무엇을 가리나

### 2a. 환경 변수 값
다음에 해당하는 환경 변수의 **값**을 가린다.
- 이름이 아래 정규식에 맞는 것
  - `(?i)(TOKEN|SECRET|PASSWORD|PASSWD|CREDENTIAL|PRIVATE_?KEY|API_?KEY|COOKIE|SESSION_?KEY|DELEGATION|BEARER|ENROLLMENT_CODE|RESTORE_CODE|ADMIN_PIN)`
  - 단, 이름이 `_FILE`·`_PATH`·`_DIR`·`_ROOT` 로 끝나면 제외한다. 경로라서 값은 가리지 않는다. 그 경로가 가리키는 파일 내용은 2c 규칙을 따른다.
  - **식별자 예외(10-05 20:0x 추가 — 이적 sub-350 발견, 포장에도 같은 이름 있음):** 이름이 `_TRANSACTION_ID`·`_SESSION_ID`·`_ATTEMPT_ID` 로 끝나고(예: `CONTAINER_AUDIT_WRITER_DELEGATION_TRANSACTION_ID`, `KMTECH_LABEL_WRITER_DELEGATION_SESSION_ID`) **값이 엄격한 식별자 모양일 때만** 제외한다 — UUID(`^[0-9A-Fa-f]{8}-[0-9A-Fa-f]{4}-[0-9A-Fa-f]{4}-[0-9A-Fa-f]{4}-[0-9A-Fa-f]{12}$`) 또는 32·64 자리 16진수. 이유: 정규식의 `DELEGATION` 에 걸려 위임 거래·세션·시도 UUID 가 가려지면, 그 값을 담는 보호 journal·감사의 원 결속(예: 이적 `REMOVAL_JOURNAL_ORIGIN_BINDING_DIFFERS`)이 깨진다. 식별자는 비밀이 아니다. 값이 그 모양이 아니면(다른 내용이 들어 있으면) 그대로 가린다. 명시 목록에 있는 이름은 이 예외를 받지 않는다. 이 규칙도 sink 종류와 상관없이 같다.
  - **식별자 예외 확장(10-06 00:3x 추가 — 검사 sub-328 실측 P1):** 같은 방식으로, 이름 끝과 값 모양이 **둘 다** 맞을 때만 제외한다.
    - `_PID` 로 끝남 + 값 `^[1-9][0-9]{0,9}$`
    - `_START` 로 끝남 + 값 `^(windows-filetime|linux-startticks):[1-9][0-9]{0,24}$`(검사 `PROCESS_IDENTITY_PATTERN` 과 같음)
    - `_MUTEX` 로 끝남 + 값 `^(Local|Global)\\[A-Za-z0-9._-]{1,200}$`(역슬래시 한 글자)
    - 이유: 검사 `maintenance_fence.py` 가 writer admission 안에서 자식용 `INSPECTION_WORKER_WRITER_DELEGATION_{ROOT_PID,PARENT_PID,ROOT_START,PARENT_START,MUTEX}` 를 넣는데, 정규식의 `DELEGATION` 에 걸려 비밀로 분류되면 ready proof(1a machine metadata)의 pid·start identity 가 그 '비밀' 값과 같아 쓰기가 거절되고(`MACHINE_METADATA_SECRET_VALUE_REJECTED`) 게시가 막힌다. 짧은 숫자를 비밀로 가리면 로그의 같은 숫자 조각까지 바뀌는 문제도 같이 막는다. pid·프로세스 시작 표지·mutex 이름은 비밀이 아니다. 토큰(`…_DELEGATION_TOKEN` 등)은 그대로 비밀이다.
    - 값 모양이 다르면 그대로 가린다. 명시 목록 이름은 예외를 받지 않는다. sink 종류와 상관없이 같다. 10-06 기준 이 이름 끝을 쓰는 곳은 검사와 리워크의 vendor 사본(`vendor/Inspection_worker/core/maintenance_fence.py`)뿐이다(포장 조립·이적 조립·불량 창고 0, 1차 git grep).
- 이름이 아래 **명시 목록**에 있는 것
  - 정규식에 맞든 안 맞든 포함한다. 각 레인이 아는 이름을 이 목록에 더한다.
- 제품 함수는 정규식 판정을 위해 프로세스 환경 이름을 **내부에서** 훑어도 된다(13:4x, sub-274 질문). 훑은 이름 목록과 값은 어디에도 출력·기록하지 않는다. 시험은 `environ`/`-Environment` 인수로 합성 환경만 넣고, 실제 호스트 환경으로 이 함수를 돌려 결과를 출력하지 않는다.

**명시 목록** — w9rwdi `sub-220/INVENTORY.md` 에서 가져온 이름과 이 레인 앱의 이름이다.
- DEFECT_FINAL_DEFECT_CLAIM_TOKEN
- DEFECT_FINAL_DEFECT_INTAKE_CLAIM_TOKEN
- DEFECT_LINKED_MANIFEST_LOCK_API_TOKEN
- DEFECT_LOGISTICS_API_TOKEN
- DEFECT_RETURN_MANIFEST_API_TOKEN
- DEFECT_SERVER_ACTIVE_LOCK_TOKEN
- DEFECT_TRANSFER_MANIFEST_API_TOKEN
- INSPECTION_WORKER_WAREHOUSE_RESPONSE_EXPORT_TOKEN
- KMTECH_DEFECT_WRITER_DELEGATION_TOKEN
- KMTECH_REWORK_TEMP_NG_CANDIDATE_TOKEN
- KMTECH_REWORK_TEMP_NG_CLAIM_TOKEN
- REWORK_ENROLLMENT_RESTORE_TOKEN
- REWORK_LOGISTICS_API_TOKEN
- CONTAINER_AUDIT_ENROLLMENT_TOKEN
- CONTAINER_AUDIT_WRITER_DELEGATION_TOKEN
- CONTAINER_RUNTIME_SECRET
- WORKER_ANALYSIS_LOGISTICS_API_TOKEN (13:4x, 이적 sub-273 발견)
- KMTECH_LABEL_WRITER_DELEGATION_TOKEN, PRODUCER_SELF_ENROLL_TOKEN, KM_LOGISTICS_INSTALL_BEARER_TOKEN, DIRECT_SYNC_AUTH_TOKEN, PRODUCER_API_TOKEN, LOGISTICS_API_TOKEN (14:0x, 포장 sub-274 발견)
- 각 앱의 `DELEGATION_TOKEN_ENV`·`DEFAULT_ENROLLMENT_TOKEN_ENV`·`DEFAULT_INSTALL_TOKEN_ENV` 상수가 가리키는 실제 이름

**값 치환 방법**
- 치환 대상: sink 텍스트 안에 그 값이 나오는 모든 자리. 다음 세 형태를 모두 찾는다.
  - 원문 그대로
  - JSON 문자열 escape 형태(`\\`, `\"`, `\uXXXX`)
  - PowerShell 따옴표 두 겹 형태
- 표시: `[REDACTED:<ENV_NAME>]`. 이름은 비밀이 아니므로 남긴다.
- 길이 4 미만인 값은 텍스트를 망가뜨리지 않도록 치환하지 않는다. 대신 sink 끝에 `[REDACTION_NOTE:short_value_skipped:<ENV_NAME>]` 를 붙인다. 실제 비밀은 이보다 길다.
- 긴 값부터 먼저 치환한다. 한 값이 다른 값을 포함하는 경우를 막기 위해서다.

### 2b. JSON 필드
sink 가 JSON 이면 직렬화 전에 처리한다.
- 대상: 키 이름이 2a 정규식에 맞는 필드, 또는 키가 정확히 `pin`·`otp`·`password` 인 필드(대소문자 무시)
- 처리: 그 값(문자열·숫자·객체 전체)을 `"[REDACTED:field]"` 로 바꾼다.
- 키 이름은 남긴다.
- **가리지 않는 것(10-05 13:5x 추가, sub-273 지적 — 비밀이 없는 타입 메타데이터까지 가리면 기존 guard 가 닫힌다):**
  - 값이 `true`·`false`·`null` 인 필드(예: `secret_values_recorded: false`, `identity_or_credential_copied: false`). 참·거짓에는 비밀이 없다.
  - 키가 `_sha256`·`_sha256s`·`_fingerprint`·`_hash` 로 끝나는 필드(예: `delegation_token_sha256`). 높은 엔트로피 비밀의 digest 는 비밀이 아니다. **단 키에 `pin`·`otp`·`password` 가 들어 있으면 digest 라도 가린다** — 짧은 값의 digest 는 되돌릴 수 있다.
  - 키가 `_path`·`_file`·`_dir`·`_root` 로 끝나는 필드. 2a 와 같은 이유다. 그 경로의 파일 내용은 2c 를 따른다.
- 이 규칙은 sink 종류(진단·오류·권한·복원 기록·summary·audit)와 상관없이 같다. sink 마다 다르게 하지 않는다.

### 2c. 비밀 파일 내용
credential 파일·DPAPI blob·키 파일·등록 승인 파일의 **내용**은 sink 에 복사하지 않는다. 이름·크기·SHA256 만 남긴다.

## 3. 길이 상한과 잘림 표시
- 순서는 반드시 **가림 먼저, 자르기 나중**이다.
- 상한: sink 하나당 텍스트는 UTF-8 기준 **65,536 바이트**까지다. 이 상한은 잘림 표식 줄까지 **포함한 출력 전체**에 적용한다(13:3x 정정, w9scanonly 지적 — 앞 판 문장은 표식만큼 상한을 넘었다).
- 넘으면 앞 몫·표식 줄·뒤 몫 순서로 남긴다.
  - 표식 줄: `\n[TRUNCATED: <생략 바이트 수> bytes omitted]\n`
  - 앞 몫: 앞 8,192 바이트 이하.
  - 뒤 몫: 65,536 − 앞 몫 − 표식 줄 바이트 수 이하. 표식 바이트는 뒤 몫에서 뺀다.
  - 앞은 첫 예외 맥락, 뒤는 Python traceback 의 마지막 예외를 담는다.
- UTF-8 문자 중간에서 자르지 않는다. 경계를 맞출 때는 몫을 줄이는 쪽으로만 맞춰 전체가 65,536 바이트를 넘지 않게 한다. 생략 바이트 수는 실제로 뺀 바이트 수다.
- 큰 진단은 환경 변수나 명령줄로 옮기지 않는다. helper 가 자기 private evidence 위치에 파일로 쓰고, 호출자는 경로와 SHA256 만 받는다(포장 sub-274 결정).

## 4. 함수 모양 (앱마다 같은 이름 권장)
- Python: `redact_for_sink(text: str, environ: Mapping[str, str] = os.environ) -> str` — JSON 은 `redact_json_for_sink(obj)`
- PowerShell: `ConvertTo-KmtechRedactedSinkText -Text <string> [-Environment <IDictionary>]`
- 이름 목록·정규식·표시 문자열·상한은 각 함수 안 **상수 한 곳**에만 둔다.

## 5. 시험 벡터 — 다섯 앱이 같은 결과를 내야 한다
`W\w9setupprep\REDACTION-VECTORS.json` 을 쓴다. 입력 텍스트와 환경, 기대 출력이 들어 있다.
- 각 앱은 이 파일을 읽어 자기 함수로 돌린다. 실패 0 이어야 한다.
- 벡터 값은 모두 합성값이다. 실제 비밀이 아니다.
- (10-06 02:1x) 벡터에 `text` 대신 `text_generator` 가 있으면 입력 = `head` + `fill_char` × `fill_bytes` + `tail` 이다(65,536 바이트를 넘는 긴 입력용 — 가림 먼저, 자르기 나중을 시험). 같은 날 예외 접두문 소비 2개·긴 assignment 2개를 더해 벡터는 18개다.

## 6. 회귀 (각 앱)
- 실제 실패 경로(설치기 child 실패·업데이트 적용 실패·지원 도구 실패)마다 다음 다섯 곳에서 바이트 검색해, 합성 비밀 값이 **0회**인지 본다.
  - 사용자 안내
  - 실패 summary
  - audit 파일
  - 진단 파일
  - 로그
- 마지막 예외 줄과 traceback 은 **보호 보고서에** 남아야 하고, 공개 출력에는 `exception_type`·`location_*`·`report_path`·`report_sha256` 만 남는다(0절).
- **0절 회귀**: 같은 실패 경로들의 공개 출력에서 예외 문장·child 출력 조각(합성 표지)이 **0회**, 공개 레코드가 `PUBLIC-OUTPUT-VECTORS.json` 규칙대로, 보호 보고서 SHA 가 실제 저장 바이트와 일치.
- 위 시험 벡터(REDACTION-VECTORS·PUBLIC-OUTPUT-VECTORS) 전부 PASS.
- **1a 회귀**: 64 KiB 를 넘는 machine metadata(실제 stock 의 inventory 크기)가 쓰고 다시 읽어 바이트 그대로·JSON 정상, 합성 비밀 값이 든 machine metadata 쓰기는 거절·파일 생성/변경 0.
