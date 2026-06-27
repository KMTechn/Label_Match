# 포장실 사용 설명서 OUTLINE 게시자 노트

대상 문서: `OUTLINE_LABEL_MATCH_USER_MANUAL_20260626.md`

대상 이미지 폴더: `assets/label_match_user_manual_20260626`

작성 기준일: 2026-06-27

이 문서는 작업자에게 보여주는 본문이 아니라, OUTLINE 게시자와 관리자 확인용 메모입니다.

## 1. 게시 대상

실제 게시 위치:

- 기존 포장실 문서: `직산 사업장 / 💻 직산 프로그램 / Label_Match(포장실 프로그램)`
- 기존 문서 URL: `https://wiki.kmtecherp.com/doc/label_match-uMZaThRmO1`
- 기존 문서 ID: `4115be8b-488a-4934-80af-f0f9e4ee721b`
- 문서명 유지: `Label_Match(포장실 프로그램)`
- 본문 제목: `포장실 프로그램 사용 설명서`

중복/legacy 문서:

- `https://wiki.kmtecherp.com/doc/7ys7j6l7iukio2uhouhnoq3uoueqcdsgqzsmqkg7isk66qf7isc-qLDKyqnrpV`
- 위 문서는 초기에 별도 생성한 작업자 설명서입니다. 사용자가 기존 `Label_Match(포장실 프로그램)` 문서 업데이트를 요청했으므로 운영 기준 문서는 기존 Label_Match 문서입니다.

게시 파일:

- Markdown 원본: `Label_Match/docs/OUTLINE_LABEL_MATCH_USER_MANUAL_20260626.md`
- 이미지 폴더: `Label_Match/docs/assets/label_match_user_manual_20260626`

## 2. OUTLINE 이미지 처리

Markdown의 `assets/...` 상대 이미지 링크가 OUTLINE에서 자동으로 표시된다는 보장은 없습니다.

권장 방식:

1. OUTLINE 문서에 Markdown 본문을 붙여 넣습니다.
2. 이미지 24장을 OUTLINE attachment로 업로드합니다.
3. 본문 이미지 링크가 깨지면 OUTLINE attachment URL로 치환합니다.
4. 게시 후 이미지가 본문 순서대로 표시되는지 확인합니다.

주의:

- 업로드 대상은 아래 24장 목록 기준입니다.
- `docs/assets/label_match_admin_reference_20260626/21-history-detail-raw-barcodes.png`는 작업자 본문에서 제외한 관리자 검토용 이미지이므로 OUTLINE 작업자 문서에는 올리지 않습니다.

본문 등장 순서 기준 첨부 목록:

1. `00-contact-sheet.png`
2. `01-start-screen.png`
3. `19-settings-worker-name.png`
4. `02-master-label-scanned.png`
5. `03-label-scan-step.png`
6. `04-auto-complete.png`
7. `05-manual-complete-ready.png`
8. `06-manual-complete-done.png`
9. `09-current-set-before-cancel.png`
10. `10-current-set-after-cancel.png`
11. `11-cancel-tray-start.png`
12. `12-cancel-tray-progress.png`
13. `13-completed-tray-before-cancel.png`
14. `14-completed-tray-cancelled.png`
15. `07-error-wrong-barcode.png`
16. `08-error-recorded-reset.png`
17. `15-special-text-safe.png`
18. `16-past-history-view.png`
19. `23-date-picker.png`
20. `17-past-history-scan-blocked.png`
21. `18-today-restored.png`
22. `20-about-shortcuts.png`
23. `21-restore-prompt.png`
24. `22-after-restore.png`

## 3. 게시 결과 기록

게시 후 아래 값을 채웁니다.

| 항목 | 값 |
|---|---|
| OUTLINE URL | https://wiki.kmtecherp.com/doc/label_match-uMZaThRmO1 |
| OUTLINE parent URL | https://wiki.kmtecherp.com/doc/8jsuydsp4hsgrag7zse66gc6re4656o-7EK8SbJ618 |
| OUTLINE document id | `4115be8b-488a-4934-80af-f0f9e4ee721b` |
| legacy duplicate URL | https://wiki.kmtecherp.com/doc/7ys7j6l7iukio2uhouhnoq3uoueqcdsgqzsmqkg7isk66qf7isc-qLDKyqnrpV |
| 게시 시각 | 2026-06-27 |
| 게시자 | Chrome 로그인 세션: 박관호, Codex |
| 이미지 업로드 방식 | Chrome 편집기 PNG 붙여넣기로 attachment 24개 업로드 후 기존 Label_Match 문서 본문에 치환 |
| 본문 링크 치환 여부 | yes - 상대 이미지 링크 0개 |
| 이미지 렌더링 확인 | PASS - Markdown 이미지 참조 26개, 고유 attachment 24개 |
| 하위 문서 배치 확인 | PASS - `💻 직산 프로그램` 하위에 기존 `Label_Match(포장실 프로그램)` 표시 |
| 작업자 1회 열람 확인 | PASS - 기존 2025 본문/GitHub 설치 안내 제거, 25개 섹션 표시 |
| 상단 워크플로우 | PASS - 문서 상단에 Mermaid `flowchart TD` 블록 추가 |
| `파일 업로드` 확인 | PASS - 저장 본문(ProseMirror) 0개, Outline 편집기 숨은 file input 1개만 존재 |
| `오늘 버튼으로` 문구 확인 | PASS - 실제 오탈자 0개, 정상 문구 1개 |

## 4. 새 PC 설치 후 관리자 확인표

새 PC에 설치한 뒤 관리자는 아래를 확인합니다.

1. 프로그램 실행 가능 여부
2. 작업자 이름 저장 가능 여부
3. 스캐너 입력 가능 여부
4. `C:\ProgramData\KMTech\Label_Match\data`에 작업 로그 생성 여부
5. `C:\ProgramData\KMTech\DirectSync` 아래 개별 PC 폴더 생성 여부
6. `status\label_match_worker_pc_registration.json` 존재 여부
7. `status\direct_sync_relay_status.json` 갱신 여부
8. 작업 스케줄러의 `direct-sync-relay-...` 작업 존재 여부
9. 업로드 성공 status 파일 존재 여부
10. 서버에서 해당 PC의 데이터가 조회되는지 확인

설치 직후 작업자가 알아야 할 것은 간단합니다.

- 설치 후 별도 승인 없이 바로 프로그램을 실행합니다.
- 작업자 이름만 확인합니다.
- 스캐너가 입력되면 작업을 시작합니다.
- 인터넷이 끊겨도 로컬에는 저장됩니다.
- 서버 전송 문제는 담당자가 확인합니다.

## 5. 작업 리더 확인표

작업 리더는 아래 항목을 수시로 확인합니다.

일일 시작 전:

- 스캐너 정상 입력 확인
- 작업자 이름 확인
- 오늘 기록 화면인지 확인
- 자동 업로드 상태가 정상인지 확인

작업 중:

- 오류 기록이 반복되는 품목이 있는지 확인
- 완료 취소가 잦은 작업자가 있는지 확인
- 수동 완료(F3)가 과도하게 사용되는지 확인
- 과거 조회 화면에서 작업자가 스캔하려고 하지 않는지 확인

작업 종료 전:

- 오늘 누적 통과 코드 수량 확인
- 오류/취소 기록 확인
- 담당자 검토 필요 상태 여부 확인
- 서버 업로드 상태 확인

## 6. 관리자 참고

- 프로그램은 특수문자 입력이 화면을 깨뜨리지 않도록 처리합니다.
- 그래도 실제 생산 바코드가 아닌 문자열이면 서버 집계 전에 검토 대상이 될 수 있습니다.
- 일반 작업자용 본문에는 내부 경로, 상태 파일, 토큰, 인증 정보 설명을 넣지 않습니다.
- 내부 파일 원본 전달은 담당자 승인과 회사 승인 채널을 기준으로 합니다.

## 7. 현재 게시 자동화 상태

현재 저장소에는 OUTLINE API 재게시 스크립트가 있습니다.

스크립트:

- `tools/publish_outline_user_manual.py`
- 기본 재게시 대상: 기존 `Label_Match(포장실 프로그램)` 문서 ID `4115be8b-488a-4934-80af-f0f9e4ee721b`

로컬 dry-run 결과:

- 증적 파일: `docs/outline_user_manual_publish_dry_run_20260627.json`
- 상태: PASS
- 로컬 Markdown 이미지 참조: 26개
- 로컬 고유 이미지: 24개
- 누락 이미지: 0개
- 로컬 Mermaid 블록: 1개
- 로컬 `파일 업로드` 문구: 0개
- 로컬 `늘 버튼으로` 오탈자: 0개
- 현재 PC의 `OUTLINE_API_TOKEN`: 없음

확인 결과:

- `https://wiki.kmtecherp.com`은 HTTPS로 응답합니다.
- `root@175.45.200.171` SSH 접근은 현재 권한에서 `Permission denied`입니다.
- `tools/publish_outline_user_manual.py`는 `OUTLINE_API_TOKEN` 또는 env 파일을 통해 토큰을 읽고, 로컬 Markdown 원문 기준으로 attachment 업로드와 `documents.update` replace를 수행합니다.
- Chrome 로그인 세션으로 직산 사업장 컬렉션에 `포장실 프로그램 사용 설명서` 문서를 별도 생성했으나, 이후 사용자가 기존 `Label_Match(포장실 프로그램)` 문서 업데이트를 요청했습니다.
- 2026-06-27 기존 `Label_Match(포장실 프로그램)` 문서 `https://wiki.kmtecherp.com/doc/label_match-uMZaThRmO1`의 본문을 최신 작업자 설명서로 교체했습니다.
- 기존 문서 메뉴의 `문서 검색` 링크로 내부 document id `4115be8b-488a-4934-80af-f0f9e4ee721b`를 확인했습니다.
- Chrome 새로고침 검증 기준 `작성 기준일: 2026-06-27`이 보이고 `작성 기준일: 2026-06-26`은 보이지 않습니다.
- Chrome 새로고침 검증 기준 복구 화면(`작업 복구 확인`, `복구 후 이어서 작업`)과 날짜 선택 화면(`날짜 선택 창`)이 보입니다.
- Chrome 새로고침 검증 기준 Markdown 이미지 참조는 26개, 고유 attachment URL은 24개, 상대 이미지 경로는 0개입니다.
- 2026-06-27 문서 상단에 전체 작업 흐름 Mermaid `flowchart TD` 블록을 유지했습니다. Outline은 해당 블록을 Mermaid/코드 블록 형태로 표시합니다.
- `파일 업로드` 텍스트는 저장 본문이 아니라 Outline 편집기가 접근성용으로 렌더링하는 숨은 `<input type="file">` 라벨입니다. ProseMirror 저장 본문 기준 0개입니다.
- 섹션 12의 `오늘 버튼으로` 문구는 정상입니다. 기존 검증식은 `오늘 버튼으로` 안의 부분 문자열 `늘 버튼으로`를 잘못 센 false positive였고, 스크립트 검증식을 수정했습니다.
- 증적 파일:
  - `.agents/agent-loop/runs/label-match-real-ui-walkthrough-20260627/evidence/outline_ui_uploaded_url_map_20260627.json`
  - `.agents/agent-loop/runs/label-match-real-ui-walkthrough-20260627/evidence/outline_ui_publish_verify_20260627.json`

향후 API 재게시 방법:

1. 승인된 `OUTLINE_API_TOKEN`을 환경 변수 또는 env 파일로 제공합니다.
2. `python tools/publish_outline_user_manual.py --report-path docs/outline_user_manual_publish_result_20260627.json`을 실행합니다.
3. Chrome에서 문서를 강제 새로고침합니다.
4. Markdown 이미지 참조 26개, 고유 attachment 24개, ProseMirror 저장 본문 기준 `파일 업로드` 0개, 실제 `늘 버튼으로` 오탈자 0개를 확인합니다.

향후 같은 작업을 반복할 때는 OUTLINE 웹 붙여넣기보다 API attachment 업로드와 본문 URL 치환을 사용하는 편이 안전합니다.
