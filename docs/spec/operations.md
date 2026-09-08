# Label_Match 운영·복구·검증

[제품·기능](README.md) · [데이터·통합 계약](contracts.md) · [남은 일](BACKLOG.md) · [중앙 준비도](../../../Program_Spec_Hub/READINESS.md)

최초 기준일은 **2026-09-07**, 후속 갱신은 **2026-09-08**이며 소스와 네 판단 축은 [README](README.md#기준과-판정-범위)를 따른다. 기존 문서 기준선·잔여 소스 종결에서는 앱·테스트·VM·프린터를 실행하지 않았다. 이후 이번 [relay custom 경로 교정](#relay-custom-root-evidence)은 격리된 호스트 headless 검증 26 PASS를 직접 기록했다. Main이 별도로 수행·수용한 [M3/N6](#residual-source-evidence), [ProducerClose](#producer-close-evidence), [SaveRoot13](#saveroot13-evidence)는 각각 원래 소스·환경에 남으며 나머지 복구·수용 항목을 실행 결과로 간주하지 않는다.

## 실행 구성과 소유 경계

`Label_Match.py:main`은 product-host 전용 인수를 먼저 분기하고 일반 앱에서는 factory 계약 검증, 조건부 current-user onboarding, 데이터 범위 mutex, 품목 갱신, Tk 앱 순서로 진행한다. UI lane은 외부 작업과 Tk 결과 적용을 분리하며 DataManager가 CSV/current-state를, SQLite가 명령·취소·operation lease·접수·교체 intent를 보존한다. producer relay는 포장 명령과 독립된 관측 전송이다. 근거: [main/DataManager](../../Label_Match.py), [product host](../../label_match_product_host.py), [TkSerialUiLane](../../tk_serial_ui_lane.py), [C-04/05](contracts.md#c-04).

| 구성 | 현재 근거와 적용 한계 |
| --- | --- |
| 현행 배포 계약 | [RELEASE_GATE_CONTRACT](../../RELEASE_GATE_CONTRACT.md)의 public `INSTALL_THIS_PC.ps1`은 hardened `C:\KMTech\Apps\Label_Match\current`의 PyInstaller onedir 코드 배치와 integrity inventory를 소유한다. mutable identity·profile·ledger·relay 등록은 첫 일반 사용자 실행의 책임이다. 실제 최종 설치 성공은 미입증 |
| 일반 사용자 실행 | [current_user_onboarding](../../current_user_onboarding.py) `resolve_current_user_onboarding_paths/apply_current_user_runtime_environment`가 사용자별 데이터·설정·relay·프로필 위치를 정하고 환경에 적용한다. Machine logistics anchor와 기존 사용자 설정의 영향은 아래처럼 별도 확인 |
| relay | 현행 릴리스 계약은 HKCU `KMTech.LabelMatch.Relay`, 동일 제품의 `--label-match-user-relay`/`--label-match-direct-sync-relay`, `:18456` 계약과 로그인 사용자 실행을 사용한다. SYSTEM/AtStartup task를 현행 지원 배포로 안내하지 않는다. 소스에 남은 scheduled/호환 mode의 존재는 지원 승격 근거가 아님([product host](../../label_match_product_host.py), [릴리스 계약](../../RELEASE_GATE_CONTRACT.md)) |
| source/portable 진입 | [portable/main.py](../../portable/main.py)도 같은 `Label_Match.main`을 부른다. checked-in 대체 진입점 존재와 현행 배포 후보의 실제 포함·선택은 다르다. [requirements-release](../../requirements-release.txt), factory bundle/pin과 실행 산출물 조합을 함께 식별 |
| provider·overlay | 이 조사에서는 Label 로컬 모듈과 서버 소비 경계를 확인했다. Rework의 vendor/sibling 선택 규칙을 Label에 적용하지 않는다. 설치된 Python/라이브러리·실제 로딩 파일·패키징 변경·공급자/overlay 유무는 해당 후보 근거로 확인할 항목이며, `APP_VERSION`이나 저장소 HEAD로 추정하지 않는다([LM-B04](BACKLOG.md#lm-b04)) |

<a id="configuration"></a>
## 설정 위치와 우선순위

**코드의 fallback, onboarding이 적용한 위치, 실제 저장 위치를 구분한다.** 비밀 값은 수집·문서화하지 않고 선택 경로·비밀 없는 identity·안정된 오류 코드만 근거에 연결한다.

| 항목 | 현행 선택 규칙 | 근거·주의 |
| --- | --- | --- |
| 사용자 설정 | `LABEL_MATCH_SETTINGS_PATH` → `%LOCALAPPDATA%\KMTech\Label_Match\config\app_settings.json`. 파일이 없으면 packaged `config/app_settings.json`을 template로 복사 | [앱 `_default_label_match_settings_path/_setup_paths`](../../Label_Match.py). CODEX의 packaged 경로를 일반 사용자 쓰기 위치로 해석하지 않음 |
| onboarding 데이터 | 명시 `LABEL_MATCH_SAVE_DIR` → `%LOCALAPPDATA%\KMTech\Label_Match\data`; 선택값을 process 환경에 적용 | [onboarding 경로·환경 적용](../../current_user_onboarding.py). 기존 사용자 settings는 재사용하며 내용이 자동으로 이 경로와 같아지는 것은 아님 |
| 앱의 실제 데이터·GUI mutex | 기존 settings의 nonempty `custom_save_path` → `LABEL_MATCH_SAVE_DIR` → `%ProgramData%\KMTech\Label_Match\data`. null/빈 값/공백 custom은 fallback | [앱 resolver](../../Label_Match.py)의 기존 선택은 보존하고 [guard](../../label_match_single_instance.py)의 순서만 맞춘 소스 교정. [SaveRoot13](#saveroot13-evidence)의 실제 resolver 대조·단일 process native 중복 callback 제외는 **PROVEN**; 두 GUI writer·설치본 복구는 [LM-B10](BACKLOG.md#lm-b10)에 남음 |
| producer 상태 | onboarding은 `LABEL_MATCH_DIRECT_SYNC_ROOT` 또는 호환 `LABEL_MATCH_DIRECT_SYNC_PROGRAM_DATA_ROOT` → `%LOCALAPPDATA%\KMTech\DirectSync\label_match`를 선택 | [onboarding](../../current_user_onboarding.py). 앱의 오래된 ProgramData bootstrap 상수만 보고 현재 relay 상태 위치를 정하지 않음 |
| relay의 CSV 발견 위치 | 명시 `--scan-source-dir` → 기존 사용자 settings의 nonempty `custom_save_path` → onboarding `data_root` | [user_relay `_resolve_scan_source_dir`](../../user_relay.py)가 상주·예약 진입에서 [GUI guard의 resolver](../../label_match_single_instance.py)를 재사용한다. 설정이 없거나 읽기 실패·invalid JSON·null/빈 custom이면 fallback한다. queue/spool·identity·profile 위치는 변경하지 않음; [검증 한계](#relay-custom-root-evidence) |
| logistics Machine anchor | 일반 Windows 경로에서 Machine `KM_LOGISTICS_PROFILE_PATH`/`KM_LOGISTICS_REQUIRED` 중 하나라도 있으면 두 process 값보다 Machine 쌍을 우선 | [logistics_runtime_profile._runtime_environment](../../logistics_runtime_profile.py), [프로필 안내](../LOGISTICS_RUNTIME_PROFILE.md). 테스트 전용 엄격한 TEST1 분기는 일반 운영 override가 아님 |
| logistics 프로필 위치 | onboarding은 명시 경로 또는 `%LOCALAPPDATA%\KMTech\Logistics\profiles\Label_Match\runtime-profile.json`을 적용. 일반 resolver 기본은 `%ProgramData%\KMTech\Logistics\profiles\Label_Match\runtime-profile.json`; 앱별 파일이 없고 옛 공통 파일이 있으면 `...\Logistics\runtime-profile.json` 호환 선택 | [selected_logistics_runtime_profile_path](../../logistics_runtime_profile.py). explicit path·Machine anchor·legacy 공통 경로 재선택을 함께 대조. 설치된 최종 선택은 미확인 |
| 권한·비밀 범위 | 현행 current-user 계약은 사용자 DPAPI와 AUTHORITATIVE profile을 사용. 기존 Machine profile의 machine-scope DPAPI/ACL 계약은 별도 구성 | [onboarding](../../current_user_onboarding.py), [릴리스 계약](../../RELEASE_GATE_CONTRACT.md), [프로필 안내](../LOGISTICS_RUNTIME_PROFILE.md). 프로필 JSON의 secret reference와 실제 비밀을 혼동하지 않음 |
| 필수 물류 | required 모드의 프로필·HTTPS·authority·인증 실패는 중앙 업무를 차단한다. 비필수 legacy 환경 fallback은 별도 호환 분기 | [package_client_from_env](../../package_logistics.py), [load_logistics_runtime_profile](../../logistics_runtime_profile.py). 필수 모드를 임의로 해제해 완료하지 않음 |
| 업데이트 | `LABEL_MATCH_UPDATE_PROVIDER` → packaged `update_settings.provider` → 코드 기본. 현재 `_can_apply_updates()`는 `False`이며 provider 미지정 기본은 `off`; `github/private_manifest` 조회 코드 존재와 앱 내부 코드 교체 허용은 다름 | [앱 update 함수](../../Label_Match.py). 명시 channel/manifest 설정도 별도 선택; 운영 값·원격 feed를 조회하지 않음. 기존 CODEX와 차이는 [LM-B09](BACKLOG.md#lm-b09) |

활성 프로필의 scope/epoch/plane·device/source identity, 서버 capability, 실제 endpoint 및 `PROJECTION_API_READ_ENABLED`를 후보별로 대조해야 한다. 토큰의 존재만으로 호출 권한이나 소비 화면 반영을 입증하지 않는다([C-00](contracts.md#c-00), [C-05](contracts.md#c-05)).

<a id="storage-root-residual"></a>
**relay 발견 경로는 좁게 교정 / onboarding 분리는 남음:** 기존 custom 경로 C가 있어도 onboarding은 기본 사용자 경로 A의 ledger를 준비하고 settings를 재사용한다([current_user_onboarding](../../current_user_onboarding.py) `resolve_current_user_onboarding_paths/_ensure_user_settings`). 종전 상주·예약 relay도 A만 스캔해 C의 미발견 CSV를 놓쳤으며, 이번 [실제 재현·교정](#relay-custom-root-evidence)으로 두 진입점이 기존 settings의 C를 선택한다. 앱 내 session sync는 원래 실제 `save_directory` C를 전달했다([Label_Match](../../Label_Match.py) `_label_match_direct_sync_context/_label_match_start_session_direct_sync`). 이미 queue/spool에 들어간 파일의 저장 위치나 producer identity/manifest를 옮기지 않는다.

onboarding의 ledger·registration `--sync-dir` 선택은 여전히 A이며, guard 이전 ledger 쓰기, 누락 settings의 template 복사 시점, relative/alias 경로의 실제 동일성도 **OPEN**이다. 실행 중인 relay가 나중의 설정 변경을 다시 읽는 기능은 이번 수정에 포함하지 않았다. native 로그인·앱 종료/재시작, 기존 queue의 실제 전송과 새 CSV의 enqueue/서버 receipt 연결은 **NOT TESTED / UNPROVEN**이다. 데이터 이동·프로필 재설계는 하지 않는다.

<a id="relay-custom-root-evidence"></a>
### 2026-09-08 · relay custom 경로의 재시작 후 발견

[변경·실행 보고](E:/KMTech/coordinator-handoff-20260907-01a07992/repo-parallel-0826/label-ready-improvement/CHANGE.md)는 clean parent `394c21fd535ae565d862a77aeeb680f463496cdb`와 후속 working source를 구분한다. 수정 전 [Baseline02](E:/KMTech/coordinator-handoff-20260907-01a07992/repo-parallel-0826/label-ready-improvement/evidence/Baseline02.xml)는 **custom 4 FAIL / 나머지 10 PASS**다. 최종 [Final02](E:/KMTech/coordinator-handoff-20260907-01a07992/repo-parallel-0826/label-ready-improvement/evidence/Final02.xml)는 **26 PASS, failure/error/skip 0**: relay 모듈 23개(신규 14개 포함)와 기존 writer inventory 검사 3개다. 별도 cohort와 합산하지 않는다.

Windows 호스트 CPython **3.12.10**, pytest **9.0.2**에서 실제 두 relay 진입점·command builder·CSV scanner를 사용했다. 상주/예약 × custom 기본·env 충돌/명시 scan override/empty/null/missing/invalid JSON을 확인하며, 각 사례는 두 번의 진입 사이에 CSV를 추가하고 process 환경을 되돌려 다음 발견을 대조한다. child process/transport와 profile loader·relay lease는 대체물이다. 같은 queue 경로와 기존 spool 파일·settings bytes 보존을 확인했지만 실제 queue enqueue/ACK나 OS 재부팅의 증거는 아니다. TEMP/TMP·pytest·상태·로그는 지정 E 작업 루트에 격리하고 bytecode/cache와 외부 pytest plugin 자동 로딩을 비활성화했다. 최초 Baseline의 14 FAIL은 긴 E 경로의 Win32 파일 열기 실패이며 원본을 보존했다; 이후 시험은 같은 E 루트의 extended-length 경로를 사용했다.

기존 inventory pin은 수정 전부터 실제 clean source와 달랐다(`a3852e…` vs `f3f9bc…`). 같은 [기존 scanner](../../writer_sink_inventory.py)로 후속 source를 계산해 [Python](../../writer_session_fence.py)과 [PowerShell](../../tools/label_writer_fence.ps1)의 pin을 `852dd0a0a0cb8377bf4e7ff5bd261806ab878100848bcb0cb0df0ad59c299e12`로 맞췄다. [대조 원본](E:/KMTech/coordinator-handoff-20260907-01a07992/repo-parallel-0826/label-ready-improvement/evidence/Writer-Inventory.json)은 45개 source 식별자·위치·qualified name·guard 종류가 동일함을 확인한다. pin/coverage 검사 3개와 PS5 구문 오류 0은 **PROVEN**이며 설치 시 실제 fence 동작·qualification을 입증하지 않는다.

원래 `label-clean-full-prepare`의 Stage/MainStage **9,445파일·issues 0** 수용은 `394c21f`의 역사 근거로 보존한다. 이 후속 소스에는 상속하지 않는다. [추가 source 입력과 다음 단계](E:/KMTech/coordinator-handoff-20260907-01a07992/repo-parallel-0826/label-ready-improvement/CHANGE.md)를 수용하고 successor source/host bindings를 새로 결속한 뒤 Main이 CA 정리 후 VM01을 배정해야 한다. Setup/FULL·native F3/F4·설치·실제 backend는 이번에 실행하지 않았고 Ready **0/6**을 유지한다.

<a id="relay-successor-preparation"></a>
### relay 독립 소스 종결·후속 FULL 준비

[독립 검토·소스/후속 입력 결속 보고](E:/KMTech/coordinator-handoff-20260907-01a07992/repo-parallel-0826/label-relay-closure-review/REVIEW-PREPARE.md)가 이 작업의 exact commit, 원래 394c21f와 새 후보의 차이, 새 host/guest bindings 및 정적 검증 범위를 기록한다. 최종 26 PASS를 만든 product/test bytes를 보존하고 45개 writer identity·guard 종류와 두 pin의 일치를 독립 대조했다. 이번 검토에서 앱·pytest·VM을 실행하지 않았으며 기존 Baseline/Baseline02 실패도 보존한다. 기존 `label-clean-full-prepare`와 `label-full-host-reader-fix`의 verified entry 경로를 통해 새 후보별 Stage → 별도 MainStage → Setup → ExportSetup → MainSetup → Full → ExportFull → MainFull을 분리한다.

VM01은 현재 Defect 파일 Stage 다음 Inspection 보조 runtime 순서로 Main이 관리하며 Label의 guest 조회·실행 권한은 없다. Main의 후속 packet 수용과 VM 소유권 배정이 있어야 새 Stage를 시작한다. 새 Setup/FULL·native 로그인/재시작·실제 queue/producer receipt·F3/F4·설치/cold boot/재설치/제거/rollback·통합 E2E는 **NOT TESTED / UNPROVEN**이고 Ready **0/6**을 유지한다.

<a id="devices"></a>
## 스캔·포커스·사운드·출력

| 관측 대상 | 현행 동작 | 아직 확인할 장비 조건 |
| --- | --- | --- |
| 제출·키보드 | 작업 입력은 Tk `<Return>` → `_handle_scan_enter/process_input`, 문자열 앞뒤 whitespace 제거. busy이면 입력칸을 비우지 않음. F1–F4는 버튼·키보드 공통 gate를 사용 | 스캐너 CR/LF/CRLF/Tab suffix, keyboard layout/IME, 연속 입력 간격, 중복 Enter, modal/오류 안내 후 포커스. Enter가 안내 확인으로 소비되는 경우 실제 다음 입력 확인 |
| QR/문자 | compact PHS2의 여섯 필드·순서·HSH를 검사. 첫 입력에는 조건부 encoded 문자열 UTF-8 decode 호환도 존재하지만 decode 자체는 중앙 유효성 승인이 아님 | scanner model·펌웨어·문자 인코딩·제어문자 보존·길이·QR 표시 배율. Tab·직렬 장비 지원을 이번 검색으로 보장하지 않음 |
| 소리 | `LABEL_MATCH_AUDIO_ENABLED`로 비활성 가능하며 automated-test mode는 무음. 성공음 호출은 F3 durable 경계 후에 있음 | 실제 소리 장치·볼륨·환경 소음·고장 시 시각 피드백. 소리만으로 중앙 ACK를 판정하지 않음 |
| F4 전자 QR | 중앙 교체 뒤 새 전자 QR을 화면에서 스캔해 확인하고 원본 물리 PHS2는 유지 | 화면 반사·해상도·배율에서 판독, 잘못된 QR 거부와 재시작 후 확인 단계 복구 |
| F5 물리 출력 | Windows GDI proof는 printer/job/document/time/EndDoc를 저장하는 spool 증거 | 용지 크기·driver·실제 배출·QR 가독성·기본 프린터 보존·paper-out. 큐 접수 성공은 실물 출력 성공과 별도 |

근거: [입력·단축키·audio](../../Label_Match.py) `process_input/_handle_scan_enter/_handle_workflow_shortcut/_label_match_audio_enabled`, [F4 확인](../../Label_Match.py) `_prompt_new_seal_verification`, [F5 프린터](../../phs_label_workflow.py) `WindowsGDIPhysicalLabelPrinter/PhysicalPrintEvidence`. 전체 장비 수용은 [LM-B04](BACKLOG.md#lm-b04), 업무 기준은 [LM-05/10/12](README.md#lm-05)에서 연결한다.

<a id="recovery"></a>
## 저장·오프라인·중단 복구

같은 데이터 루트의 `_current_set_state_packaging.json`, event CSV, `package_logistics_outbox.sqlite3`와 `package_operation_lease_keyring.json`을 업무 identity와 함께 보존한다. 같은 SQLite 파일에 생성·취소·lease·접수·교체 상태가 연결되며, F5는 `phs_label_exchange/phs_label_exchange_recovery.json`과 `labels`를 사용한다. producer의 queue/spool/status/receipt는 별도 root다. 근거: [앱 초기화/DataManager](../../Label_Match.py), [package_logistics](../../package_logistics.py), [보존 정책](../../DIRECT_SYNC_DATA_PLATFORM_NOTES.md).

| 사건 | 현행 경계와 작업자/지원 담당의 다음 행동 | 종료·확인 기준 |
| --- | --- | --- |
| 시작/품목 실패 | 중앙 등록은 검증된 이전 cache 복구 가능; 유효 cache가 없으면 시작 차단. 표시된 profile·catalog 진단을 확인 | 임의 Item.csv 성공으로 바꾸지 않고 정상 snapshot 또는 검증 cache가 선택됐는지 확인([LM-02](README.md#lm-02)) |
| 미검증 접수·오프라인 | `CAPTURED_UNVERIFIED`는 저장 접수다. F3는 현재 snapshot과 일치하는 유효 lease가 필요. 재사용 lease 없는 단절에서는 완료 차단 | 재연결 뒤 같은 capture/set identity의 검증·중앙 결과·로컬 적용 상태가 일치([C-04](contracts.md#c-04)) |
| 디스크/CSV/marker 실패 | 성공을 선행 표시하지 않고 현재 세트·intent·기존 CSV를 유지. writer 오류를 정상 flush로 오인하지 않음 | 같은 작업으로 복구해 이미 있는 완료 event를 대조하고 marker와 lease 경계를 연결; 새 set/key로 우회하지 않음([LM-06/08](README.md#lm-06)) |
| 로컬 완료 후 전송 대기 | marker=1 완료를 유지하며 due 재시도; 다음 준비 작업 가능. ACK 유실은 저장 명령의 receipt부터 조회 | 중앙 COMMITTED receipt와 원래 명령 identity 검증. 오래된 pending 실물 인계 기준은 [LM-B02](BACKLOG.md#lm-b02) |
| 중앙 충돌/다중 PC 경합 | marker=1 conflict는 로컬 완료를 보존하고 검토 사건으로 격리. marker=0을 완료로 승격하지 않음 | 작업자는 원본 PHS2·실물 구분을 유지하고 리더/지원 담당이 set/key·중앙 결과·원인을 대조. 사건 종결 권한·실물 처리 기준은 미확정([LM-B06](BACKLOG.md#lm-b06)) |
| 자정·재시작 | 미확정 PHS2/outbox를 날짜만으로 삭제하지 않음. 복원에서 기존 이벤트·작업자·중앙 migration을 확인 | 작업일 변경과 관계없이 기존 identity로 완료/검토 상태에 수렴. 자동 복구 불가 상태는 증거를 유지해 인계 |
| F1/F2 | F1은 gate가 허용하는 미완료 초기화. F2 취소는 CREATE ACK dependency와 별도 취소 key 사용 | 취소·중복 요청이 원래 PACKAGE에 연결되고 SHIPPING-WAIT 재고 유지. 실물 해체·재고 반환으로 해석하지 않음([C-06](contracts.md#c-06)) |
| F4 응답/로컬 확인 실패 | 저장 intent/receipt부터 복구; 중앙 성공 후 새 QR 확인·로컬 적용은 별도 단계 | 중복 교체 없이 현재 membership/seal을 확인한 뒤 후속 gate 해제([C-03](contracts.md#c-03)) |
| F5 출력 결과 불명 | `recover_current/recover_reconciliation`과 journal·서버 status·출력 evidence를 먼저 대조 | 재출력 여부를 결정하고 prepare/print/activate를 조정. 새로운 출력부터 반복하지 않음([C-07](contracts.md#c-07)) |
| spool 유실/손상 | 통신 재시도로 파일을 재생성하지 못함. `failed_permanent`/검토 prefix를 보존하고 원본 range와 identity를 대조 | 원본에서 복구 가능한 범위와 손실을 구분하고 유효 accepted receipt까지 진척을 확정하지 않음([C-05](contracts.md#c-05)) |
| 처리 중 종료 | UI lane drain·현재 상태/로그 저장·세션 동기화의 종료 경계를 따른다. BROKEN/timeout을 정상 저장으로 간주하지 않음 | 재시작으로 durable 상태를 확인. 강제 종료 후 성공 여부는 소리·창 닫힘으로 판정하지 않음([TkSerialUiLane](../../tk_serial_ui_lane.py), [앱 `on_closing`](../../Label_Match.py)) |

지원 인계에는 비밀 없는 set/package/key·발생 시각과 업무일·로컬 marker/status·중앙 receipt 또는 오류 코드·producer 상태·관련 산출물 경로·실물 구분을 연결한다. 원본 로그 전문을 복사하거나 DB/status를 수동 편집해 복구 성공을 만들지 않는다. 요구 RPO/RTO·재시도 후 인계 시점·최종 승인 역할은 아직 미정이다([LM-B02](BACKLOG.md#lm-b02), [LM-B07](BACKLOG.md#lm-b07)).

## 설치·업그레이드·제거·백업·롤백

현행 [릴리스 계약](../../RELEASE_GATE_CONTRACT.md)의 코드 배치와 첫 사용자 등록을 구분한다. `--remove-current-user-setup`은 정확한 사용자 persistence 제거·relay 종료와 lock 부재를 확인하면서 identity/profile/settings/ledger/queue/spool/status/logs/receipts를 보존하는 계약이다. 이후 elevated `INSTALL_THIS_PC.ps1 -Uninstall`이 코드를 제거하며 relay persistence가 남아 있으면 거부한다. 이 명세에서 설치·제거 명령을 실행하지 않았다.

업데이트 후보·서명/manifest 조회 코드가 있어도 현재 앱 내부 적용 gate는 닫혀 있다. 코드 교체와 integrity 재생성은 별도 installer의 책임이며, 이전 릴리스/현재 dirty 소스의 결과를 서로 자동 상속하지 않는다. 실제 업그레이드·재설치·rollback 뒤 기존 identity, current-state, 미전송/검토 row, F5 journal, receipt 정합성은 [LM-B07](BACKLOG.md#lm-b07)의 남은 수용 범위다.

백업·복원은 데이터와 미전송 효과를 함께 다뤄야 한다. 현행 [보존 정책](../../DIRECT_SYNC_DATA_PLATFORM_NOTES.md)은 미확정 spool/status 삭제 금지와 ACKED retention의 read-only 후보 판정을 제공하지만, 완전한 운영 백업 도구·주기·RPO/RTO·검증된 복원본을 이 조사에서 확인하지 않았다. DB 단독 복사나 코드 rollback만으로 앱 CSV/queue/keyring/F5 journal까지 일관된 시점으로 복원됐다고 할 수 없다. 서로 다른 PC의 DPAPI/identity를 단순 파일 이동으로 복구할 수 있다고 가정하지 않는다. 실제 복원 방식·중앙 receipt 대조·중복 방지 확인을 소유 운영 절차에 확정할 필요가 있다.

<a id="performance"></a>
## 처리량·동시성·최신성

| 항목 | 요구 목표 | 코드·계약에서 확인한 값 | 실제 측정 |
| --- | --- | --- | --- |
| 입력 간격·처리량·최대 membership | 미정; ERPnext 제한 전용 금지 | 표준 PHS2 1회, F4 1~2쌍은 업무 입력 규칙이며 처리량 SLA가 아님 | 이 작업 NOT TESTED |
| 물류 요청 시간 | 운영 응답 목표 미정 | 일반 profile timeout 기본 10초, 허용 0.1~60초; legacy client 기본 8초([profile](../../logistics_runtime_profile.py), [client](../../package_logistics.py)) | 설치값·지연 분포 미확인 |
| 전송 claim 회수 | 업무 lease 허용 기간과 별도 | outbox stale SENDING 300초, due-time/last-attempt 순서 선택([C-04](contracts.md#c-04)) | 장애 후 회복시간 미측정 |
| relay/종료 | RTO·허용 대기 미정 | 릴리스 사용자 relay 30초 retry loop; 앱 종료 총 예산 상수 105초/로그 10초([릴리스](../../RELEASE_GATE_CONTRACT.md), [앱 상수](../../Label_Match.py)) | 최대 소요 보장 또는 측정 p95로 해석하지 않음 |
| 동시 작업 | profile 문서의 10~30대 전환은 rollout 계획, 검증 capacity 아님 | GUI guard의 기존 custom 우선 선택을 writer와 일치시킨 소스 교정. Windows lexical 경로 정규화·mutex 수명 및 중앙 CAS/idempotency는 유지. [잔여 경계](#storage-root-residual), [LM-B10](BACKLOG.md#lm-b10) | [SaveRoot13](#saveroot13-evidence)의 단일 process native 제외·해제 후 재진입만 입증. 두 GUI writer·다중 PC 처리량은 미실행 |
| 지표·시계·보존 | 발생→ACK→수신→화면 지연, 시계차, 업무 cutoff, 보존 기간·RPO/RTO 미정 | [C-05](contracts.md#c-05)의 전송·projection/flag와 [수량·시간](contracts.md#quantities) | 실제 설치 조합의 신선도·손실·복원 측정 없음 |

<a id="verification"></a>
## 수용 기준과 기존 증거의 연결

아래는 핵심 확인 묶음이며 테스트 개수나 전체 완성도 분모가 아니다. 카드의 현행 계약 확인 기준을 입력/장애/관측 결과로 묶었다. 목표 설치 조합의 필수·조건부·비적용은 담당자가 근거와 함께 확정하고, 이미 확보된 실행을 먼저 연결한 뒤 실제 부족한 범위를 검증한다. **M3/N6의 V03 저장·복구 및 V07 worker/Tk 적용 경계, SaveRoot13의 V01 저장 경로, ProducerClose의 V06 receipt 회귀는 각각 아래 지정 범위에서 PROVEN**이며 V01–V08 전체를 통과로 바꾸지 않는다.

| 묶음·연결 | 입력·장애와 기대 결과 | 현재 연결할 근거·공백 |
| --- | --- | --- |
| V01 · LM-01/02/03 | 사용자 시작·catalog 정상/검증 cache/손상, 정상/오형식 PHS2 → 선택 경로·권한·멤버십 일치 또는 차단. 서로 다른 env + 같은 custom 저장소 → 실제 writer와 같은 mutex 및 중복 callback 제외; null/빈 custom fallback 대조 | [main](../../Label_Match.py), [품목](../../item_catalog_sync.py), [회귀 소스](../../tests/test_label_match_single_instance.py). [원래 선택 제안](E:/KMTech/coordinator-handoff-20260907-01a07992/label-save-root-fix/FUTURE-VM.md)에서 결속한 [focused packet](E:/KMTech/coordinator-handoff-20260907-01a07992/label-save-root-vm-prepare/PREPARATION.md)의 **실제 13 PASS/39 phase PASS**를 [아래](#saveroot13-evidence)에 연결. 저장 경로·단일 process native mutex만 해당하며 catalog/PHS2·실제 GUI·설치 provider·장비는 이 실행으로 입증하지 않음([LM-B04](BACKLOG.md#lm-b04)) |
| V02 · LM-05 | F4 1/2쌍, 다른 품목·UOM·stale·부분/복수 work-group, ACK 유실·QR 중단 → 원자 교체 또는 무변경 거부·동일 identity 복구 | [test_sealed_transfer_exchange](../../tests/test_sealed_transfer_exchange.py) 설계; 실제 QR/실물 [LM-B04](BACKLOG.md#lm-b04) |
| V03 · LM-04/06/08 | 유효 lease 유무별 offline, intent/CSV/marker 경계 중단·writer 실패·자정 → 거짓 성공 없음·같은 set 1회 복구 | [M3 변경 사례 #83–84, #160–168, #179 및 N6 #1–6](#residual-source-evidence)의 실제 저장/중단·재동기화 범위 **PROVEN**. core는 lease 없음, materializer는 로컬 lease/검증 double, N6는 lease 없는 역사 ACKED row다. real lease-bearing orphan 통합 재시작·원격 lease/backend·native F3는 **UNPROVEN**([LM-B05](BACKLOG.md#lm-b05)) |
| V04 · LM-07/09 | 다중 PC·lost ACK·429/409·취소 dependency → receipt 수렴, 후속 due row 진행, 로컬 완료 보존, 취소 후 재고 유지 | [test_package_logistics](../../tests/test_package_logistics.py) 설계와 [서버 계약](contracts.md#c-04); 중앙·실물 인계 [LM-B02](BACKLOG.md#lm-b02) |
| V05 · LM-10 | F5 출력 실패/출력 후 ACK 유실/재시작 → journal 단계 조정·중복 출력 통제·실물 판독 | [F5](../../phs_label_workflow.py) 정적 근거; 실제 프린터 [LM-B04](BACKLOG.md#lm-b04) |
| V06 · LM-11 | 정상·취소·중복·부분 이벤트, 명령/CSV 순서 바꿈, spool 손상 및 F3 ACK 뒤 raw APP_CLOSE → 수신·세트 projection·화면 별도 확인. [ProducerClose 실제 50/150 PASS](#producer-close-evidence)는 receipt 회귀에 한정하며 native/서버·화면 연동은 미실행 | [양쪽 전송/소비](contracts.md#c-05); 실제 flag·화면·지연 [LM-B06](BACKLOG.md#lm-b06) |
| V07 · LM-12 | busy/과거 날짜/연속 Enter/종료·stale 결과 → 입력 보존, gate 일치, 중단 후 저장 정합 | [M3 #287](#residual-source-evidence)의 real SQLite/DataManager worker 처리와 marker/lease commit 뒤 owner-thread UI 적용 **PROVEN**. FakeTkRoot이며 native F3/close/reopen·실장비/배율 및 V07 전체 수용은 **UNPROVEN** |
| V08 · 설치·복원 | exact candidate 설치→첫 업무→cold boot→업그레이드/제거·재설치→rollback → 데이터·미전송 identity 보존. 기존 custom 저장소의 owner 종료/중단 후 같은 위치 복구와 onboarding/relay A/C 선택을 별도 확인 | [릴리스 계약](../../RELEASE_GATE_CONTRACT.md), [중앙 준비도](../../../Program_Spec_Hub/READINESS.md); 전체 범위 [LM-B07](BACKLOG.md#lm-b07), [LM-B10](BACKLOG.md#lm-b10) |

<a id="residual-source-evidence"></a>
### M3/N6 · 완료 CSV·복구 잔여 소스의 기존 수용 매핑

2026-09-08 [독립 전체 diff·증거 검토](E:/KMTech/coordinator-handoff-20260907-01a07992/label-residual-source-review/REVIEW.md)에서 전체 tracked diff 1,575줄/71,863 bytes와 신규 완료 CSV 모듈 10,540 bytes를 대조했고 Main이 다섯 경로+네 명세의 한 소스 단위 종결을 승인했다. [잔여 소스 종결](E:/KMTech/coordinator-handoff-20260907-01a07992/label-residual-source-close/CLOSE.md)은 테스트 당시 working bytes를 유지하고 Git EOL 정규화 staged/commit blob pins를 별도로 남긴다. 아래는 기존 실제 실행의 매핑이며 신규 실행·collection·VM 관측이 아니다.

**PROVEN:** M3는 **313 PASS / 939 ordered setup/call/teardown PASS**(deferred 106 + core 174 + lane 33), 별도 N6는 **6 PASS / 18 ordered phase PASS**다. M3의 변경 13사례는 이미 그 313개에 포함되며 Candidate13과 겹친다. N6는 M3 밖의 별도 선택이다. 모두 원래 JUnit 순번이며 아래 각 사례의 세 phase는 PASS다.

| 원래 사례 | 변경 경로·정확한 함수/parameter | 입증된 경계 |
| --- | --- | --- |
| M3 #83–84 | `test_deferred_intent_capture.py::test_validated_materializer_flows_through_f3_durable_completion[first-success/marker-commit-refusal]` | 같은 command/lease/capture/CSV identity; marker/lease 외부 동시 가시성·거부 rollback·1회 복구 |
| M3 #160 | `test_label_match_core.py::test_central_finalize_commits_intent_and_local_event_before_ui_success` | 독립 DB/CSV read가 성공음·이력·count보다 선행 |
| M3 #161–165 | `test_label_match_core.py::test_f3_real_storage_refusal_preserves_packaging_and_recovery[intent_insert/intent_commit/csv_flush/csv_fsync/marker_commit]` | 각 실제 저장 거부에서 입력·복구 보존, 새 writer로 같은 작업 복구 |
| M3 #166 | `test_label_match_core.py::test_f3_pending_first_keeps_second_prepared_input_on_reopen` | 첫 pending과 다음 준비 입력을 재시작에서 각각 보존 |
| M3 #167–168 | `test_label_match_core.py::test_f3_crash_recovery_keeps_one_completion_across_midnight[csv_durable_marker_uncommitted/committed_before_ui]` | exit-73 두 중단 경계·자정 뒤 동일 완료 event 1개 |
| M3 #179 | `test_label_match_core.py::test_data_manager_fsyncs_local_completion_before_flush_returns` | 실제 fsync gate가 flush 반환 차단 |
| M3 #287 | `test_label_ui_lane_integration.py::test_f3_lease_outbox_and_flush_run_off_tk_before_ui_apply` | worker에서 real SQLite/DataManager; marker hold 때 0/PREFETCHED, commit 뒤 1/LOCAL_COMPLETED와 owner-thread apply/sound/drain |
| N6 #1–3 | `test_completion_csv_durability.py::test_acked_orphan_requires_existing_csv_sync_before_marker[None/fsync/write_open]` | write-capable fsync 뒤 marker; 거부는 marker0·동일 input/retry block·event identity 유지 |
| N6 #4–6 | `test_completion_csv_durability.py`의 `test_matching_csv_keeps_real_writer_failure_sticky`, `test_matching_csv_is_revalidated_after_writer_barrier`, `test_malformed_or_unrelated_discovery_rows_remain_absent` | writer 오류 유지, barrier 뒤 바뀐 row 거부, malformed/unrelated row 제외 |

다섯 working pins는 원래 P109와 PC112 `control/SOURCE-MANIFEST.json` 모두에 일치한다. P109 manifest SHA256 `40b2104364b6cc15476328f8b6318ae03f32867be52f590e4d4cf6f43f913b7f`, [선택](E:/KMTech/coordinator-handoff-20260907-01a07992/label-next-prepare/control/SELECTION.json) `e91a79cfee33e6273619632d2e204d48f03ff47772a4517fc2d06191d65870f2`다. 환경은 VM01 Windows/Python 3.12.10 x64/pytest 9.0.2, 22 versions/19 imports와 원래 tkcalendar warning, guest `C:\Qualification\vm-test-g6\label-b1-pid-03\source`다.

| 파일 | 테스트 당시 bytes / working SHA256 |
| --- | --- |
| `Label_Match.py` | 886147 / `a205c587c3ca0a518f80410729a4e6d28bb9c267282d21aa2957c1cb780bece5` |
| `tests/test_deferred_intent_capture.py` | 129177 / `dcb552cb26c747161f2621281c432124f274dbfc83f2c8d4657940265a335bb9` |
| `tests/test_label_match_core.py` | 242285 / `9ea2f893cab73ded5ac496bf8454be809529bad57d3ea365ab31172bf5ce902b` |
| `tests/test_label_ui_lane_integration.py` | 68783 / `7de68a05c4845653c76299f64c4a442566cf2445075c744fa38e086c7320ed55` |
| `tests/test_completion_csv_durability.py` | 10540 / `335bb278196e620ee5596b3a7643430eb73b42ebedb53997c43687f2495dd46d` |

[M3 Main readback](E:/KMTech/coordinator-handoff-20260907-01a07992/label-modules3-reader-fix/MAIN-Modules3-READBACK-REPR.json): **2,815,193 bytes / SHA256 `d0278b9eb6d6e1b5f118889e6be22082619bba0c4410d02ca8e502261a26ee12`**, Main 수용 `2026-09-07T08:11:02.5780662Z`. [N6 Main readback](E:/KMTech/coordinator-handoff-20260907-01a07992/label-next-prepare/MAIN-NewProduct6-READBACK.json): **2,479,468 bytes / `7b0e39fe772203b770627969a1562f03e20a3ed9744f40e68c27da1a8c4f3a6a`**, 수용 `2026-09-07T07:12:01.0865971Z`. 독립 검토에서 원본 XML/phase를 대조하고 M3 21개/N6 17개 export 전부 재해시해 불일치0, saved source/provider/stream gates 일치를 확인했다. 원시 XML/hooks는 `label-next-prepare/evidence-Modules3`, `evidence-NewProduct6`에 보존한다.

**FAILED 이력:** 원래 M3 reader의 11 repr ID/33 비교 불일치, Candidate13 11 PASS/2 FAIL, ProducerClose Export01을 보존한다. additive reader 교정과 후속 export는 시험 재실행이 아니다. SaveRoot13 **13/39**(source110 manifest `a68cd9c06dd1f32e0f9adfb085aee8fa1c9e6e706f5ef5bf6dc4cf00e09517e8`)와 ProducerClose **50/150**(source112 manifest `256401fab80446fe042ab6f2fb8eab8f1d7b56ed5a58294d795b6aeb08dc1430`)는 별도 선택이며 위 변경 사례를 선택하지 않는다. 수를 합산·반복하거나 원래109와 현재의 guard/uploader 차이를 통합 FULL PASS로 상속하지 않는다.

**NOT TESTED:** Baseline8 causal RED는 기존 [선택](E:/KMTech/coordinator-handoff-20260907-01a07992/label-next-prepare/control/SELECTION.json)에 있으나 start/evidence/Main 결과가 없는 역사 공백이다. Main이 이번 이미 수용된 수정의 소스 종결 선행 조건에서 명시적으로 제외했다. baseline exact pins는 독립 검토에 보존하며 RED를 관측했다고 주장하지 않는다.

**UNPROVEN:** real lease-bearing ACKED-orphan 동기화/marker 실패의 통합 재시작. M3 #257은 mocked forwarding, materializer/lane은 로컬 lease를 seed하고 검증·전송을 double로 대체한다. N6는 lease 없이 receipt `{}`인 역사 ACKED/marker0 row를 seed했으며 현재 전송 claim은 marker1만 선택한다. matched-file timeout/disappearance/decode/CSV 예외별 사례, native Tk F3/close/reopen, 두 GUI same-store 복구, 실제 원격 lease/backend ACK, hardware power-loss 및 통합 FULL은 별도 공백이다. 이번 FULL/build/install은 **NOT TESTED**다.

다음 요구는 종결된 candidate와 실제 provider·설정·입력/관측을 동결해 기존 N/I same-store key/member/hash/lease, native F3/정상 종료·중단/재개·EOF, producer transport/receipt, 두 GUI writer 제외·pending 복구 및 backend/FULL을 각 해당 범위에서 검증하는 것이다. 기존 Machine anchor·DNS/TLS/authority와 grants, Linux40/Label25 및 남은 준비 조건은 아래 ProducerClose 절과 LM-B04/05/06/07/10을 따른다. 이 소스 종결은 실행·배포 권한을 추가하지 않고 Ready **0/6**을 유지한다. 중앙 Q08 갱신은 Main 소유다.

<a id="producer-close-preparation"></a>
<a id="producer-close-evidence"></a>
### ProducerClose · lifecycle 수신 계약의 한정 회귀 수용

2026-09-08 [소스·VM packet 준비](E:/KMTech/coordinator-handoff-20260907-01a07992/label-producer-close-contract-fix/PREPARATION.md)는 uploader와 회귀 테스트를 교정하고 별도 `vm-packet02`를 작성했다. 기존 SaveRoot13 controls의 source/provider/native handle·dual EOF·phase/JUnit·보존 기준을 재사용하되 새 source manifest **Label112-ProducerClose-20260908**(109개 기존 guest copy + uploader/두 테스트 모듈 3개 delta), 새 guest `C:\Qualification\vm-test-g6\label-producer-close-07`, `ProducerClose/pc50`만 선택한다. 기존 Label110 snapshot과 source pins는 과거 증거로 보존하며 새 코드에 그대로 적용하지 않는다.

독립/Main 소스·packet 검토 후 Main이 **실제 50 collected/50 PASS(신규 38 + 기존 12), 150 contiguous ordered setup/call/teardown 모두 PASS, failure/error/skip 0**을 수용했다. pytest stdout은 `50 passed in 3.29s`다. canonical close-only/reopen-close/scan-close 및 replay ACK·retention, identity/hash/byte·observation·오류/격리·business 대체·HTTP commit·runtime fence 거부와 기존 COMPLETE/명령 구별/rotation의 동결 receipt·로컬 session double 회귀다. 실제 서버나 native GUI/F3/close 실행이 아니며 Label25/Linux40 설치 admission을 대신하지 않는다. 이번 소스 종결은 기존 원본의 정적 대조만 수행했고 동일 50개를 재실행하지 않았다. 준비/독립 검토 보고의 당시 NOT TESTED와 Main 원본의 수용 대기 문구는 이력으로 보존하며, 후속 Main 수용과 [커밋·정확한 범위](E:/KMTech/coordinator-handoff-20260907-01a07992/label-producer-close-source-close/CLOSE.md)를 별도로 연결한다.

[실제 Main readback](E:/KMTech/coordinator-handoff-20260907-01a07992/label-producer-close-contract-fix/vm-packet02/MAIN-ProducerClose-READBACK.json)은 **2,641,188 bytes / SHA256 `a922aed7a84255770a3193be5e1a9378828d0b06344a24ae8770c647aa58d91d`**다. [선택](E:/KMTech/coordinator-handoff-20260907-01a07992/label-producer-close-contract-fix/vm-packet02/control/SELECTION.json)과 [소스 manifest](E:/KMTech/coordinator-handoff-20260907-01a07992/label-producer-close-contract-fix/vm-packet02/control/SOURCE-MANIFEST.json)의 원래 pins를 변경하지 않는다.

| 근거 축 | 정확한 수용 범위 |
| --- | --- |
| 소스 pin | `direct_sync_push.py` 107,609 bytes / SHA256 `eebb80c832821cdac1d07829f7673557da6ba994720e24b1425a5f3fcf71ae00`; `tests/test_direct_sync_push.py` 86,502 bytes / `0defe8aeaf3a23555fb8f6e3143860c866be728612f3101d0164a39c9fc8194f`. 테스트 당시 working bytes를 유지하며 Git 저장 표현은 종결 근거에 별도 기록 |
| 환경·provider | VM01 `8d9fd433-02bb-4609-93fa-f992ef8a1838` / `KMTech-GenericPC-01`, 기존 `label-b1-f3-01\venv\Scripts\python.exe`, x64 Python 3.12.10·pytest 9.0.2. Label22의 22 versions/19 imports 및 원래 desktop·source/provider closure 일치 |
| 실제 Main 대조 | native 36784 birth `19:07:44.7865471Z` → 자연 종료0 `19:09:29.3768049Z`(2026-09-07 UTC), retained handle·dual EOF **803/0 bytes**. guest 8,503·host 17 불일치0, task Ready/result0/XML 일치, active owned0, evidenceComplete/candidateReadyForMainDisposition=true |
| 자식·stderr | controller2832·dependency2516·pytest5276 모두 자연 종료0 및 stdout/stderr 길이 대조. 각각 0/0·0/187·101/238 bytes이며 dependency의 정확한 기존 tkcalendar SyntaxWarning 1회와 pytest debug 경로 안내 두 줄을 보존 |

초기 [Export01 FAILED](E:/KMTech/coordinator-handoff-20260907-01a07992/label-producer-close-contract-fix/vm-packet02/MAIN-NATIVE-PRODUCERCLOSE-EXPORT-01.json)는 PID49544가 `19:02:00.4082666Z`에 자연 종료1한 이력이며, controller의 자연 종료0 `19:02:56.0121977Z`보다 앞선 관측이다. 원본 stderr의 observe-probe RemoteException과 실패를 보존한다. 이후 [Export02](E:/KMTech/coordinator-handoff-20260907-01a07992/label-producer-close-contract-fix/vm-packet02/MAIN-NATIVE-PRODUCERCLOSE-EXPORT-02.json)는 PID34512 birth `19:06:13.9159115Z` → 자연 종료0 `19:06:59.0967975Z`, dual EOF 6583/0 bytes로 안정된 17파일을 반출했고 별도 Main 수용 및 이번 호스트 재해시에서 불일치0이었다. 두 번째 export는 시험 재실행이 아니며 초기 실패를 지우거나 PASS로 바꾸지 않는다.

별도 5파일 E: `fixture-overlay`는 **PREP ONLY / NOT TESTED**이며 N_CLOSE/FINAL_CLOSE raw observation과 실제 업로드 lifecycle counts 비교, PHS2 조회/F3 lease 발급 설명, guest evidence를 쓸 때 `guestFilesWritten=true`를 반영했다. 기존 D controls/docs는 변경하지 않았다. 이 overlay만으로 native 실행은 준비되지 않는다. Main이 실제 Machine anchor·기존 DNS/TLS/authority·두 pending operation grant·Linux40/Label25 provider·새 source binding과 Stage/Start/Export/Main 및 native input/F3/normal-close/interruption/reopen/EOF·producer metadata capture/transport를 연결해야 한다. 실제 backend/security grant는 수행하지 않았다. N/I의 같은 store/key/member/hash/lease와 기존 실패·미실행을 유지하며 Ready **0/6**, FULL/설치/통합은 **NOT TESTED**다.

<a id="saveroot13-evidence"></a>
### SaveRoot13 · 실제 저장 경로 회귀의 한정 수용

Main은 **2026-09-07 13:09:40Z**에 [실제 Main readback](E:/KMTech/coordinator-handoff-20260907-01a07992/label-save-root-vm-prepare/MAIN-SaveRoot13-READBACK.json)을 수용했다. 2,556,398 bytes, SHA256 `5de446ed86984329e99c31709873efb88b998eecc0d72b836a380d048da450c2`다. [이번 독립 검토](E:/KMTech/coordinator-handoff-20260907-01a07992/label-save-root-actual-source-close/REVIEW-AND-UPDATE.md)는 기존 원시 JUnit·hook trace·로그·process journal·해시를 읽었으며 새 VM 관측이나 실행은 하지 않았다. packet 준비·독립/Main 검토가 끝난 뒤의 실제 실행 결과이며 준비 보고의 당시 NOT TESTED 기록은 보존한다.

| 근거 축 | 입증된 정확한 범위 |
| --- | --- |
| 소스·환경 | HEAD `3f535c97086129862e5c4699f0d4d0b3b69152a7`의 dirty 소스. app SHA256 `a205c587c3ca0a518f80410729a4e6d28bb9c267282d21aa2957c1cb780bece5`, guard `5a9bf7a5d235896c0160c192cc80f6696cf28d2af37d7807b94eb700f7485c97`, test `160dd2166b4cd04db1c01104b502de108e2b3c01619004f0c96cefacbe3078b2`. VM01 `8d9fd433-02bb-4609-93fa-f992ef8a1838`, host E:에 있는 guest C:의 `C:\Qualification\vm-test-g6\label-save-root-05\source` 110개 파일. 기존 `label-b1-f3-01\venv\Scripts\python.exe`의 x64 Python 3.12.10·pytest 9.0.2, 22 versions/19 imports와 provider closure 일치 |
| 실제 선택·결과 | [SELECTION](E:/KMTech/coordinator-handoff-20260907-01a07992/label-save-root-vm-prepare/control/SELECTION.json)의 순서대로 **13 collected / 13 PASS, stdout 1.00s, 39 contiguous setup/call/teardown 모두 PASS**, JUnit failure/error/skip 0. custom 우선 1, missing/empty/whitespace/null × default/env의 실제 writer resolver 대조 8, conflicting env의 native 중복 callback 제외·재진입 1, 기존 config/default 1·key 정규화 1·native 소유/해제 1 |
| 종료·원본 보존 | controller 1688·dependency 3400·pytest 3392의 PID/birth·자연 종료 0·stream 길이 일치. Main native 33844는 birth `13:05:50.1943144Z` → 자연 종료 0 `13:07:30.9324149Z`, retained handle·dual EOF 791/0 bytes. 안정된 export 17개와 Main의 guest 8,356개/host 17개 모두 일치, source/provider closure·지정 desktop 일치, active owned 0, task Ready/result 0/XML 일치, evidenceComplete=true |
| stderr | pytest 226 bytes는 정확히 두 debug 경로 안내 줄, dependency 187 bytes는 pinned tkcalendar warning 1회, controller stderr 0 bytes. 다른 stderr를 무시하거나 warning을 억제한 결과가 아님 |

**PROVEN은 안정된 settings에서 기존 custom 우선 선택·nullable fallback·동일 lexical 저장 경로의 native callback 제외/해제 범위다.** Tk를 구성하거나 두 process의 실제 GUI writer·파일 쓰기·중단 복구를 실행한 시험은 아니다. source/source 및 packaged/source의 중복 writer 제외, 같은 C의 pending identity 복구, onboarding/relay A/C의 enqueue 이전 CSV 발견, template/alias/settings race는 [LM-B10](BACKLOG.md#lm-b10)에 남는다. VM01 Label 예약은 Main이 해제했고 이 문서는 다음 실행 권한을 부여하지 않는다. `baselinePassClaim=false`, `fullExecuted=false`, `qualification=false`이며 generator/build/FULL/최종 설치 자격·제품 Ready를 입증하지 않는다.

기존 Label Modules3 **313 PASS / 939 ordered phase PASS**의 수탁 수용과 원래 reader의 실패는 [README 증거 경계](README.md#기준과-판정-범위)·[중앙 준비도](../../../Program_Spec_Hub/READINESS.md)에 보존한다. 이 숫자를 새 guard 또는 V01–V08 전체 통과로 바꾸지 않는다. 향후 근거에는 수행 시각, exact 소스·테스트·artifact·provider, 환경/설정·입력/관측, 원본 경로·실패/미실행·적용 한계를 남긴다.
