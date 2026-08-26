# Label_Match G2/G3 발사 전 fixture 계약

> 상태: **PRE-LAUNCH / NOT RUN / NOT QUALIFIED**
> 범위: 소스와 보존된 사전검증 문서만 읽어 확정했다. GUI 입력, 서버 호출, 빌드, Sandbox 기동은 수행하지 않았다. 아래의 `통과`, `COMMITTED`, `accepted`는 앞으로 관찰해야 할 값이지 현재 g2/g3 결과가 아니다.

## 1. 핵심 트랜잭션과 선택 이유

**고정 트랜잭션:** 제품 교체가 없는 표준 경로에서 **원본 물리 PHS2 한 장을 한 번 스캔하고, F4를 쓰지 않은 채 F3 `포장 완료`를 확인**한다. 앱은 PHS2가 가리키는 현재 서버 멤버십 전체를 `INHERIT_ALL`로 상속해 로컬 `TRAY_COMPLETE`를 내구 기록하고, 중앙 `CREATE_PACKAGE`를 제출한다.

이 경로를 고른 이유는 중앙 PHS2 자체가 검사→이적→포장→출고를 잇는 유일한 물리 라벨이고, 코드가 정확히 1회 스캔과 명시적 F3 커밋을 표준 흐름으로 선언하기 때문이다 (`Label_Match.py:195-209`). 운영 안내도 `PHS2 1회 스캔 → 필요 시 F4 ... → ... F3 포장 완료`로 고정되어 있다 (`Label_Match.py:4916-4919`). 첫 스캔은 서버 package source를 읽기 전용으로 resolve하고 (`Label_Match.py:8136-8180`), F3 뒤의 논리적 서버 쓰기는 `POST /logistics/api/v1/packages`로 보내는 idempotent `CREATE_PACKAGE` 명령 하나다 (`package_logistics.py:3314-3370`). 이 fixture에서는 선택적 교체인 F4를 의도적으로 제외하여 핵심 거래를 한 가지로 잠근다.

## 2. 기동 명령과 눈으로 확인할 준비 완료 기준

비프로덕션 값이 승인·확정된 뒤 같은 PowerShell 프로세스에서 실행할 명령은 다음과 같다. 실행 파일 경로와 환경변수 이름은 설치 스크립트의 비프로덕션 예시 그대로다 (`INSTALL_THIS_PC.ps1:18-27`).

```powershell
$env:LABEL_MATCH_DIRECT_SYNC_SERVER_BASE_URL = $approvedNonProductionServerBaseUrl
$env:LABEL_MATCH_DIRECT_SYNC_SOURCE_HOST_ID = $approvedNonProductionSourceHostId
& "C:\KMTech\Apps\Label_Match\current\Label_Match.exe"
```

`$approvedNonProductionServerBaseUrl`과 `$approvedNonProductionSourceHostId`의 실제 값은 7절의 UNKNOWN이다. 설치 계약도 canonical executable과 all-users shortcut을 각각 `C:\KMTech\Apps\Label_Match\current\Label_Match.exe`, `Label Match.lnk`로 고정한다 (`INSTALL_THIS_PC.ps1:56-60`, `INSTALL_THIS_PC.ps1:900-906`).

**준비 완료의 정확한 가시 기준:** 로딩 오버레이가 사라진 하나의 창에서 (1) 제목이 `바코드 세트 검증기 (`로 시작하고 (`Label_Match.py:258-259`), (2) 헤더 `Label Match · 포장 라벨 검증`이 보이며 (`Label_Match.py:13662-13665`), (3) `스캔 입력` 필드가 보이고 입력 가능하며 포커스를 가진 상태여야 한다 (`Label_Match.py:5745-5755`, `Label_Match.py:15567-15586`). 정확한 버전 문자열, 프로세스 존재만, 로그의 초기화 문구는 준비 완료 판정에 쓰지 않는다.

## 3. 정확한 입력 시퀀스, 위젯, 키와 PHS2 형식

1. 2절의 준비 완료 화면에서 `스캔 입력` 필드를 클릭하거나 이미 설정된 포커스를 유지한다. 이 entry의 완료 키는 `<Return>`이며 해당 키가 실제 처리 함수로 연결된다 (`Label_Match.py:14951-14960`, `Label_Match.py:15579-15586`).
2. **현 fixture용으로 사전확정된 원본 물리 PHS2 전체 문자열**을 스캐너로 한 번 입력하고 스캐너 종단키 `<Return>`을 보낸다. 허용 문법은 정확히 `PHS|SRC|ITG|CLC|LBL|HSH` 여섯 필드 순서, `PHS=2`, `SRC=KMTECH_INPUT_TAG`, 16자리 16진수 `HSH`다 (`Label_Match.py:1793-1824`).
3. 앱이 해당 입력을 compact PHS2로 해석하고 중앙 package source를 resolve한 뒤 현재 세트에 받아들일 때까지 기다린다 (`Label_Match.py:8468-8541`, `Label_Match.py:8182-8251`). `포장 완료 (F3)`가 활성화되는 것이 다음 입력으로 넘어갈 조건이다 (`Label_Match.py:16808-16820`). 두 번째 바코드는 스캔하지 않는다. 코드도 한 장이 이미 확인된 뒤 추가 입력 대신 F4 또는 F3을 요구한다 (`Label_Match.py:8438-8448`).
4. 키보드 **F3**을 한 번 누른다. F3은 `포장 완료` 동작에 바인딩된다 (`Label_Match.py:14173-14187`, `Label_Match.py:16082-16085`).
5. 제목 `포장 완료 확인` 대화상자에서, `스캔한 PHS2의 현재 이적 멤버 전체`를 기준으로 완료한다는 문구를 읽고 **예(Yes)** 를 한 번 누른다 (`Label_Match.py:12289-12331`). F4, 리셋, 두 번째 스캔은 이 fixture 입력 시퀀스에 없다.

문법 확인용 합성 테스트 예시는 다음과 같다 (`tests/test_package_logistics.py:351-359`). **이는 실물/실서버 fixture 값이 아니며 발사 입력으로 사용하면 안 된다.**

```text
PHS=2|SRC=KMTECH_INPUT_TAG|ITG=ITG-CENTRAL-2SCAN|CLC=ITEM000000001|LBL=LBL-CENTRAL-2SCAN|HSH=0123456789abcdef
```

실제 발사용 PHS2 문자열은 `UNKNOWN — 확인 못 함`이며 7절에 이유를 기록했다.

## 4. 로컬 성공 표시 판정 기준

F3 확인 후 다음 두 가시 증거를 모두 한 화면 또는 연속 캡처로 남겨야 로컬 성공 표시가 성립한다.

1. 상태 영역에 **`✓ 기록됨 (HH:MM:SS)`** 가 나타난다. 코드는 authoritative package intent/outbox, `TRAY_COMPLETE` CSV, 로컬 완료 마커를 내구 기록한 뒤에만 성공 소리와 이 문구를 표시하고, 문구는 3초 후 지운다 (`Label_Match.py:11398-11555`, `Label_Match.py:11605-11606`).
2. `이번 세션` 탭의 `최근 완료` 표에서 방금 스캔한 `CLC`/현품표의 최신 행이 보이고 `결과`가 **`통과`** 다. 표의 열과 표시는 `시각 / 현품표 / 결과`로 정의되며 (`Label_Match.py:15803-15829`), 완료 시 원본 history 행의 결과와 시각이 갱신되고 그 값이 session 표로 복사된다 (`Label_Match.py:11586-11603`, `Label_Match.py:15212-15256`).

스캔 수락 화면, F3 버튼 활성화, 소리, 로그 한 줄, 또는 서버 ACK만으로는 이 로컬 기준을 대신할 수 없다. 반대로 이 로컬 표시는 내구 로컬 커밋의 증거일 뿐 5절의 중앙 receipt/projection/dashboard 증거를 대신하지 않는다.

## 5. g3 서버 기대값: receipt, projection row, dashboard surface

모든 값은 3절의 스캔과 F3 확인으로 생성된 **동일한 `packaging_set_identity`와 `item_code`**에 연결되어야 하며, 로컬 성공 시각부터 5분 이내에 아래 세 계층이 모두 관찰되어야 한다. 이 시간·증거 경계는 정책 원문의 g3 기준이다 (`E:\KMTech\autoloop-policy-20260822\AUTONOMOUS_LOOP_POLICY.md:135-148`). identity 형식은 정확히 `label_match|{pc_id}|{set_id}`다 (`Label_Match.py:1470-1471`). 앱은 이벤트에 `source_system=label_match`, `source_transport_or_dataset=legacy_packaging_csv`, `raw_event_name=TRAY_COMPLETE`, `packaging_set_identity`를 넣고 (`Label_Match.py:3312-3343`), 성공한 표준 완료에는 `quantity_basis=PACKAGING_SET`, `measure_code=PACKAGING_SET_COUNT`, `packaging_set_count=1`, `downstream_count_excluded=false`를 넣는다 (`Label_Match.py:3343-3391`). 같은 로컬 `TRAY_COMPLETE` details에는 package outbox의 `idempotency_key`와 `package_bundle_id`도 포함되므로 logistics receipt와 projection identity를 한 거래로 조인한다 (`Label_Match.py:11150-11167`, `Label_Match.py:11510-11515`).

### 5.1 receipt 필드

**g3 producer-ingest receipt(필수):** 송신 경로는 `POST /api/producer-ingest/v1/source-file`, stream은 `label_match_events`, source/transport는 `label_match` / `legacy_packaging_csv`다 (`direct_sync_push.py:42-49`). 같은 source-file에 대한 응답에서 다음을 모두 확인한다.

- HTTP 2xx, `committed=true`, `status="accepted"`, `retryable=false`, `next_retry_after=null`, `error=null` (`direct_sync_push.py:926-935`, `direct_sync_push.py:969-1013`).
- `client_batch_id`, `request_id`, `server_source_file_id`가 비어 있지 않고 송신 계획/relay와 일치한다 (`direct_sync_push.py:912-923`, `direct_sync_operator.py:643-660`, `direct_sync_operator.py:705-715`).
- `totals.errors=0`, `totals.quarantined=0`, 그리고 `totals.inserted + totals.replayed = source_file.declared_row_count`다. 모든 업로드 행을 totals가 정확히 설명해야 하고, errors와 quarantined가 0일 때만 성공으로 취급한다 (`direct_sync_push.py:938-949`, `direct_sync_push.py:1037-1045`, `direct_sync_operator.py:661-688`).
- `source_file.content_sha256`, `source_file.byte_length`, `source_file.declared_row_count`가 송신 메타데이터와 정확히 일치한다. 따라서 hash mismatch는 0이어야 한다 (`direct_sync_operator.py:689-704`).

**핵심 거래의 logistics receipt(별도 필수 보조 증거):** `GET /logistics/api/v1/receipts/{authority_scope_id}/{idempotency_key}`에서 같은 F3 제출의 receipt를 회수한다 (`package_logistics.py:3472-3483`). 기대 상단 필드는 비어 있지 않은 `receipt_id`, `contract_version="logistics-v1"`, `command_type="CREATE_PACKAGE"`, `status="COMMITTED"`, 요청과 동일한 `authority_scope_id/authority_epoch/resolved_ledger_plane/resolved_plane_epoch`, `committed_at`, 한 개의 `event_ids`, 한 개의 `outbox_ids`다 (계약 상수: `package_logistics.py:37-40`, 서버 결과 생성: `C:\company\program\WorkerAnalysisGUI-web\logistics_ledger\service.py:758-785`, package 결과: `C:\company\program\WorkerAnalysisGUI-web\logistics_ledger\service.py:12323-12335`). `data`에는 동일한 `source_bundle_id`, `package_bundle_id`, `source_bundle_type=TRANSFER`, `membership_mode=INHERIT_ALL`, 정규화된 `member_ids/member_count/membership_hash`, 그리고 명령과 같은 `source_evidence.member_ids/membership_hash/barcode_membership_hash`가 있어야 한다 (`package_logistics.py:5135-5223`). 이 `COMMITTED` receipt와 producer의 `accepted/committed` receipt는 서로 다른 두 계약이며 어느 하나도 다른 하나를 대체하지 않는다 (`DIRECT_SYNC_DATA_PLATFORM_NOTES.md:20-27`).

### 5.2 projection row

동일한 `packaging_set_identity`로 `packaging_set_projection`에서 정확히 한 canonical 행을 확인한다.

| 필드 | 기대값 |
|---|---|
| `packaging_set_identity` | 로컬 `TRAY_COMPLETE` payload의 값과 정확히 동일 |
| `item_code` | 스캔한 PHS2의 `CLC`와 동일 |
| `latest_status` | `shipping_waiting_observed` |
| `packaging_set_count` | `1` |
| `cancelled` | `0` |
| `last_event_id` | 이번 projected `TRAY_COMPLETE`의 event ID |

서버 projection이 identity를 payload에서 선택하는 규칙은 `C:\company\program\WorkerAnalysisGUI-web\common_projection.py:2236-2250`, row의 실제 upsert와 값은 `C:\company\program\WorkerAnalysisGUI-web\common_projection.py:7457-7500`에 있다. 그 행의 기반 common event도 `source_system=label_match`, `event_projection_class=PACKAGING_LEGACY`, `projection_status=PROJECTED`, `raw_event_name=TRAY_COMPLETE`여야 한다 (`C:\company\program\WorkerAnalysisGUI-web\common_projection.py:7865-7885`). payload hash 검증에서 mismatch가 없어야 한다 (`C:\company\program\WorkerAnalysisGUI-web\common_projection.py:2153-2158`).

### 5.3 dashboard surface

정확한 읽기 API는 **`POST /dashboard/api/operations_flow`** 이며 서버 config와 route가 이를 고정한다 (`C:\company\program\WorkerAnalysisGUI-web\app.py:5041-5065`, `C:\company\program\WorkerAnalysisGUI-web\app.py:5092-5129`). headed UI surface는 **`/?workspace=flow&view=flow`의 `운영 관제` → `공정 현황`** 이다 (`C:\company\program\WorkerAnalysisGUI-web\templates\index.html:170-185`). 이 화면의 `label_match` source component 표시명은 **`포장 원장`** 이다 (`C:\company\program\WorkerAnalysisGUI-web\static\dashboard_enhanced.js:4019-4024`).

실행 직전 같은 필터의 baseline을 보존하고, 5분 안에 다음을 모두 확인한다.

- `포장 원장`이 stale/fallback이 아닌 현재 projection을 표시한다.
- 상태 `shipping_waiting_observed`가 이 완료를 1 packaging set으로 반영한다. 이 상태와 `packaging_to_shipping` metric은 `label_match`가 유일한 요구 source다 (`C:\company\program\WorkerAnalysisGUI-web\common_projection.py:12228-12244`).
- 동시 거래가 없는 격리 관찰 구간에서 `process_gaps[key="packaging_to_shipping"].delta`가 baseline보다 1 증가한다. 해당 metric은 `Label_Match TRAY_COMPLETE`, `quantity_basis=PACKAGING_SET`, `measure_code=PACKAGING_SET_COUNT`로 정의된다 (`C:\company\program\WorkerAnalysisGUI-web\common_projection.py:14173-14190`).
- 5.2의 identity 행과 receipt/event ID로 같은 거래임을 별도로 상관 확인한다. 집계 delta만으로는 same-transaction 증거가 아니다.

`/health/ingest`는 준비 상태 보조 surface일 뿐 g3 dashboard 증거가 아니다 (`C:\company\program\WorkerAnalysisGUI-web\app.py:5051-5059`).

## 6. 선행 조건과 설치 직후 실행 가능 여부

**결론: 설치 직후 바로 실행 가능하지 않다.** 기본 direct-sync 설치값은 production origin이고 (`install_label_match_direct_sync.ps1:14-22`), 이번 fixture의 정확한 비프로덕션 origin/SourceHostId가 아직 확정되지 않았다. 다음을 먼저 충족해야 한다.

1. **격리 origin과 기계 identity:** 승인된 비프로덕션 `ServerBaseUrl`, 고유 `SourceHostId`, authority scope/epoch/plane, device identity를 한 세트로 확정한다. 설치기는 self-enroll과 machine credential bundle, logistics runtime profile 생성을 지원한다 (`install_label_match_direct_sync.ps1:1084-1113`); 앱은 DPAPI token과 authoritative profile을 엄격히 읽는다 (`logistics_runtime_profile.py:534-610`, `package_logistics.py:5661-5688`).
2. **정확한 비즈니스 fixture:** completed/sealed GOOD PHS2와 active physical label이 있어야 하며, resolve 결과가 같은 item/scope의 `bundle_type=TRANSFER`, `bundle_role=PACKAGE_SOURCE`, `bundle_state=AVAILABLE`, `current_location=TRANSFER`이고 exact members/count/hash가 일치해야 한다 (`package_logistics.py:2655-2786`, `package_logistics.py:4335-4395`). 보존된 사전검증은 Label에 현재 exact sealed source가 없다고 명시한다 (`E:\KMTech\autoloop-20260824\APP_PREQUALIFICATION_MAP.md:83-87`).
3. **R4에서 차용할 형태만 적용:** 지원되는 business API로만 candidate를 준비하고, 실행 직전 list/detail/resolve를 읽기 전용으로 재확인하며, 직접 DB 쓰기·사전 materialize/claim을 하지 않는다. Rework R4의 OPEN/STAGED NG 후보 자체는 Label의 sealed GOOD 요구와 호환되지 않으므로 재사용하지 않는다 (`E:\KMTech\autoloop-20260824\APP_PREQUALIFICATION_MAP.md:89-93`, `E:\KMTech\autoloop-20260824\APP_PREQUALIFICATION_MAP.md:138-140`).
4. **g3 관찰 준비:** exact origin에 인증된 headed `operator` 세션과 operations-flow projection 읽기 기능이 준비되어야 하며, 거래 직전 동일 필터의 baseline을 저장해야 한다. 보존된 마지막 binding은 v2.0.78의 `https://server5.autoloop.test:18456`일 뿐이고 현재 세션은 proven이 아니다 (`E:\KMTech\autoloop-20260824\APP_PREQUALIFICATION_MAP.md:117-121`).
5. **격리·회수 경계:** 동시 Label 거래가 없는 5분 관찰창과, fixture 소비 후 사용할 승인된 business-API rollback/cleanup 절차를 사전에 확정한다. 이 source-only 작업은 fixture 생성·소비·rollback을 수행하지 않았다.

## 7. UNKNOWN — 확인 못 한 항목 전체

| 항목 | 상태와 확정하지 못한 이유 |
|---|---|
| 실제 발사용 원본 PHS2 바코드 전체값 (`ITG/CLC/LBL/HSH`) | **UNKNOWN — 확인 못 함.** 소스에는 문법과 합성 테스트값만 있고, 보존된 사전검증도 현재 exact sealed source를 찾지 못했다. |
| 실제 source/package bundle topology와 ID, member IDs/count/hash, entity versions | **UNKNOWN — 확인 못 함.** single TRANSFER와 exact work-group 두 resolve branch가 있고, 어느 branch인지는 live candidate의 읽기 전용 resolve 결과로만 정해진다 (`package_logistics.py:2655-2786`). |
| 현재 승인된 비프로덕션 origin | **UNKNOWN — 확인 못 함.** `:18456` 증거는 v2.0.78의 마지막 binding이며 현재 후보 binding이 아니다 (`E:\KMTech\autoloop-20260824\APP_PREQUALIFICATION_MAP.md:117-121`). |
| 현재 `SourceHostId`, device ID, authority scope/epoch/plane, protected token 전달 방식 | **UNKNOWN — 확인 못 함.** 설치기는 tokenless self-enroll과 token-file 입력을 지원하지만 이 qualification의 app-specific binding 선택·발급값은 소스에 없다 (`install_label_match_direct_sync.ps1:1084-1113`, `E:\KMTech\autoloop-20260824\APP_PREQUALIFICATION_MAP.md:133-135`). |
| completed/sealed GOOD candidate의 현재 AVAILABLE 상태와 active physical label | **UNKNOWN — 확인 못 함.** source-only 범위에서 business API live refetch나 fixture 준비를 하지 않았고, 기존 지도는 후보 부재를 기록한다. |
| 소비 후 rollback/cleanup 명령과 대상 identity | **UNKNOWN — 확인 못 함.** Label용으로 승인·입증된 rollback 계약이 소스/사전검증에 없으며, Rework R4의 `CANCEL_SESSION`은 이 상태에 호환되지 않는다. |
| exact origin의 현재 인증된 headed operator 세션과 projection feature 상태 | **UNKNOWN — 확인 못 함.** 최소 role과 과거 origin 정보만 있고 현재 세션/feature flag를 관찰하지 않았다. |
| runtime-generated idempotency key, operation lease, receipt/event/outbox/request/source-file ID와 실제 receipt 값 | **UNKNOWN — 확인 못 함.** 이 값들은 실제 F3 제출과 ingest에서 생성되며 이번 작업에서는 거래를 실행하지 않았다. |
| dashboard 실행 직전 baseline과 실행 후 count/delta/timestamp | **UNKNOWN — 확인 못 함.** 서버/dashboard를 읽지 않았고 실제 5분 관찰창을 열지 않았다. |
| g2 로컬 캡처, g3 receipt/projection/dashboard 상관 결과, end-to-end 소요 시간 | **UNKNOWN — 확인 못 함.** 이 문서는 발사 전 준비 계약이며 g2/g3를 실행하거나 판정하지 않았다. |
