# Label_Match 작업자 안내 M7 정정 보고

- 조사·수정일: `2026-09-02`
- 현재 동작 대조 기준: `e3c56ca2458d64cda008a0f24a3aa2a5ec8811a3`
- 조사 대상 보고서: `E:\KMTech\production-readiness-20260830\RESEARCH\_USER-GUIDE-ALIGNMENT.md`
- 조사 방식: 현재 HEAD 소스·테스트와 문서의 읽기 전용 줄 대조, 문서 수정,
  비-GUI 회귀 테스트

## 판정

`PROVEN`: Label_Match는 조사 시작 HEAD의 제품 소스에서 공통 규격 식별자
`kmtech-tk-ui-lane-v1`의 Tk-safe serial UI lane을 채택했습니다. 따라서 낡은
보고서의 "Label은 M3 UI lane 미구현/변경 없음" 전제를 폐기하고 작업자 정본에
busy·입력 보존·반복 금지 설명을 활성화했습니다.

실제 배포 artifact와 공장 화면은 확인하지 않았으므로 `UNPROVEN`입니다.

## 1. 공통 UI lane 채택 근거

| 근거 | 현재 소스 판독 |
| --- | --- |
| `tk_serial_ui_lane.py:20` | 공통 규격 식별자를 `UI_LANE_SPEC = "kmtech-tk-ui-lane-v1"`로 선언합니다. |
| `tk_serial_ui_lane.py:210-262` | `TkSerialUiLane`이 별도 단일 worker를 만들고 Tk pump를 예약합니다. |
| `tk_serial_ui_lane.py:299-324` | 한 작업을 `BUSY`로 접수하고 active 작업이 있으면 후속 작업을 `reason="busy"`로 거절합니다. |
| `Label_Match.py:249-255` | 앱이 lane 규격과 `TkSerialUiLane`을 직접 import합니다. |
| `Label_Match.py:5310-5321` | 앱 초기화가 `TkSerialUiLane`과 coalescing trigger를 실제 생성합니다. |
| `Label_Match.py:5424-5487` | 앱 작업 adapter가 lane에 제출하고 접수 시 busy UI, 거절 시 rejection UI를 표시합니다. |
| `Label_Match.py:10096-10105` | PHS2 capture/validation이 lane에 제출됩니다. |
| `Label_Match.py:11580-11586` | F4 중앙 원자 교체가 lane에 제출됩니다. |
| `Label_Match.py:13770-13777` | F3 durable completion이 lane에 제출됩니다. |
| `tests/test_label_ui_lane_integration.py:424-451` | busy 중 scan을 지우지 않고 거절하는 통합 계약을 검증합니다. |

판정: **채택됨**. `9958058b`가 lane을 도입했고 `e3c56ca2`가 canonical gap을
보완한 뒤의 현재 소스가 위 경로를 실제 사용합니다.

범위 주의: 이 판정은 `Label_Match.py:5310-5321`의 정상 앱 초기화 경로에
대한 것입니다. 소스에는 lane이 없는 테스트·호환 분기를 위한 구 thread fallback도
남아 있으므로 "모든 fallback 삭제"로 확대 해석하지 않았습니다.

## 2. 문서 정정 내역

### 교체한 P1 항목 번호

- `L-W1` — `v2.0.58` 교육 기준/`v2.0.59` TEST1 후보라는 고정 버전·배포
  서술을 제거했습니다. 정본은 적용 commit
  `e3c56ca2458d64cda008a0f24a3aa2a5ec8811a3`만 동작 기준으로 기록하고 승인
  배포 버전은 증거 확보 전 `TODO`/`UNPROVEN`으로 둡니다.
- 참고: 현재 소스 상수는 `Label_Match.py:3389`의
  `APP_VERSION = "v2.0.94"`이지만, 이 상수만으로 승인된 실제 배포 버전을
  증명할 수 없으므로 작업자 문서의 배포 버전으로 단정하지 않았습니다.

### 추가한 P2 설명군

- `L-M1 — serial UI lane busy/미접수·입력 보존/반복 금지`: PHS2, F4, F3의
  operation-specific busy 문구, busy 중 추가 입력 미접수, 입력칸 값 보존,
  idle 뒤 보존값 1회 제출, `PENDING`/timeout에서 같은 PHS2·F3 반복 금지를
  정본 2절에 추가했습니다.
- 기존 `PENDING` 반복 금지는 삭제하지 않고
  "`PENDING` 또는 timeout이 확인된 경우에도 같은 PHS2·F3를 반복하지 말고
  자동 재시도를 기다리세요"로 유지했습니다.

### P3-1

- 작업자 정본을 `docs/LABEL_MATCH_WORKER_GUIDE.md` 하나로 선언했습니다.
- 구 OUTLINE, 구 게시자 노트와 `README.txt` 첫 화면에 정확히
  `HISTORICAL — 현장 사용 금지`와 정본 링크를 배치했습니다.

## 3. 보고서와 현재 소스가 달랐던 지점

보고서 작성 시각 `2026-09-01 18:00 +09:00` 뒤에 `9958058b`가
`2026-09-01 19:17:05 +09:00`, `e3c56ca2`가
`2026-09-02 01:26:55 +09:00`에 들어왔습니다.

| 낡은 보고서 | 현재 소스 |
| --- | --- |
| 8절 273행: Label은 M3 UI lane 미구현 | `Label_Match.py:5310-5321`에서 lane을 생성하고 PHS2/F4/F3 작업을 제출하므로 틀림 |
| 8절 279행: lane 채택 뒤에만 busy 문안 추가 | 채택이 이미 끝났으므로 이번 정정에서 busy 문안 활성화 |
| 9절 297행: Label M3 lane 변경 없음 | `9958058b`와 `e3c56ca2`가 현재 HEAD 이력과 소스에 존재하므로 틀림 |
| 11절 329-330행과 12절 340행: Label lane 완료 전제 FAILED/향후 UX | 현재 저장소 소스에 대해서는 더 이상 성립하지 않음. 실제 배포·공장 UX만 `UNPROVEN` |
| 9절 공통 초안의 `이번 스캔 미접수` | Label의 실제 표시 문자열은 `이전 작업 처리 중 · 입력 보존`과 `통신이 끝나지 않아 이번 입력은 접수하지 않았습니다. 입력을 보존했습니다.` (`Label_Match.py:5380-5397`)이므로 그대로 보정 |
| L-W1의 source line `Label_Match.py:3381` | 상수 값 `v2.0.94`는 같지만 현재 줄은 `Label_Match.py:3389`; 배포 승인 증거는 여전히 없음 |

그 밖에 이 정정 범위에서 보고서와 현재 소스가 달랐다고 확인한 지점은 없습니다.

## 4. 화면 표시 문자열 대조

문서에 화면 문자열로 적은 값은 모두 현재 소스의 문자열을 그대로 옮겼습니다.

- PHS2 busy `현품표 저장 · 중앙 확인 중` — `Label_Match.py:10096-10099`
- F4 busy `제품 교체 · 중앙 확인 중` — `Label_Match.py:11146-11149`
- F4 gate busy `제품 교체 · 포장 상태 확인 중` — `Label_Match.py:11353-11356`
- F4 commit busy `제품 교체 · 중앙 처리 중` — `Label_Match.py:11580-11583`
- F3 preflight busy `포장 완료 · 현재 제품 집합 확인 중` — `Label_Match.py:5760-5763`
- F3 commit busy `포장 완료 · 중앙 저장 중` — `Label_Match.py:13770-13773`
- busy 상태줄 suffix — `Label_Match.py:5351-5366`
- busy 거절 headline·설명 — `Label_Match.py:5380-5397`
- lane fault headline·중지 안내 — `Label_Match.py:5403-5417`
- 중앙 포장 대기·충돌 안내 — `Label_Match.py:5575-5624`

## 5. TODO로 남긴 것

1. 승인된 실제 배포 버전과 그 배포 증거
2. 승인 배포본의 PHS2/F4/F3 busy, 입력 보존, 중앙 대기·충돌 화면 캡처
3. 실제 공장 PC의 문구·배치와 문서 일치 qualification
4. 배포 승인자 직책과 화면 증거 책임 소재 — `미정`

새 스크린샷은 만들지 않았고 GUI도 실행하지 않았습니다.

## 6. 발견했지만 고치지 않은 코드 결함

- `UNPROVEN` 코드 경로 위험: `Label_Match.py:5324-5334`는 `BROKEN` lane도
  busy로 판정하고, `Label_Match.py:10263-10265`는 그 경우에도
  `_show_ui_lane_rejection("busy")`를 호출합니다. 이 함수는
  `Label_Match.py:5380-5397`에서 closing 외 사유를 모두 임시 busy 문구로
  표시하므로, `Label_Match.py:5403-5417`의
  `처리 상태 확인 필요`/추가 스캔 중지 안내가 이후 입력으로
  `이전 작업 처리 중 · 입력 보존`으로 덮일 가능성이 있습니다. 또한 fault
  handler가 busy 표식을 비운 뒤 workbench를 다시 그리므로
  (`Label_Match.py:5403-5422`, `:17268-17295`) 입력칸이 다시 활성화될 가능성도
  있습니다.
- `UNPROVEN` hardening gap: `_UiCallEnvelope`는 generation을 기록하지만
  `tk_serial_ui_lane.py:551-560`의 synchronous UI checkpoint 적용은 `op_id`만
  검사합니다. 현 UI guard에서 재현하지 않았으므로 잠재 위험으로만 보고합니다.
- 이 레인은 문서 전용이므로 제품 코드는 수정하지 않았습니다. GUI를 실행하지 않아
  실제 화면 재현은 하지 않았으며, 따라서 결함 판정은 `UNPROVEN`으로 남깁니다.

## 7. 검증 한계

- `PROVEN`: 현재 HEAD 소스·테스트 줄 대조, 문서 내부 링크,
  `python -B -m pytest -q -p no:cacheprovider tests/test_tk_serial_ui_lane.py tests/test_label_ui_lane_integration.py`
  결과 `38 passed`
- `UNPROVEN`: 승인 배포 artifact, 공장 PC, 현장 게시본, 실제 화면 배치
- `NOT TESTED`: GUI, 네트워크, 서버, 설치, 재시작, rollback, 새 화면 캡처
