# 포장실 사용 설명서 OUTLINE 게시자 노트

대상 문서: `OUTLINE_LABEL_MATCH_USER_MANUAL_20260626.md`

대상 이미지 폴더: `assets/label_match_user_manual_20260626`

작성 기준일: 2026-06-26

이 문서는 작업자에게 보여주는 본문이 아니라, OUTLINE 게시자와 관리자 확인용 메모입니다.

## 1. 게시 대상

권장 게시 위치:

- 기존 포장실 문서: `직산 사업장 / Label_Match(포장실 프로그램)`
- 권장 문서명: `포장실 프로그램 사용 설명서`
- 보조 제목: `바코드 세트 검증기 (Label_Match) 사용자 설명서`

게시 파일:

- Markdown 원본: `Label_Match/docs/OUTLINE_LABEL_MATCH_USER_MANUAL_20260626.md`
- 이미지 폴더: `Label_Match/docs/assets/label_match_user_manual_20260626`

## 2. OUTLINE 이미지 처리

Markdown의 `assets/...` 상대 이미지 링크가 OUTLINE에서 자동으로 표시된다는 보장은 없습니다.

권장 방식:

1. OUTLINE 문서에 Markdown 본문을 붙여 넣습니다.
2. 이미지 22장을 OUTLINE attachment로 업로드합니다.
3. 본문 이미지 링크가 깨지면 OUTLINE attachment URL로 치환합니다.
4. 게시 후 이미지가 본문 순서대로 표시되는지 확인합니다.

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
18. `21-history-detail-raw-barcodes.png`
19. `16-past-history-view.png`
20. `17-past-history-scan-blocked.png`
21. `18-today-restored.png`
22. `20-about-shortcuts.png`

## 3. 게시 결과 기록

게시 후 아래 값을 채웁니다.

| 항목 | 값 |
|---|---|
| OUTLINE URL |  |
| OUTLINE document id |  |
| 게시 시각 |  |
| 게시자 |  |
| 이미지 업로드 방식 | attachment upload / relative import / other |
| 본문 링크 치환 여부 | yes / no |
| 이미지 렌더링 확인 | PASS / FAIL |
| 작업자 1회 열람 확인 | PASS / FAIL |

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

## 5. 현재 게시 자동화 상태

현재 저장소에는 OUTLINE API 자동 게시 스크립트가 없습니다.

확인 결과:

- `https://wiki.kmtecherp.com`은 HTTPS로 응답합니다.
- `root@175.45.200.171` SSH 접근은 현재 권한에서 `Permission denied`입니다.
- repo 내부에서 `OUTLINE_API`, `documents.create`, `attachments.create` 기반 게시 자동화는 확인되지 않았습니다.

따라서 현재는 OUTLINE 웹에서 직접 붙여 넣고 이미지를 첨부하는 방식이 현실적입니다. API 토큰이 준비되면 별도 자동 게시 스크립트를 만들어 attachment 업로드와 본문 URL 치환까지 자동화할 수 있습니다.

