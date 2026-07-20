# 포장실 사용 설명서 OUTLINE 게시자 노트

대상 문서: `OUTLINE_LABEL_MATCH_USER_MANUAL_20260626.md`

대상 이미지 폴더: `assets/label_match_user_manual_20260716_display2_v2_0_36`

작성 기준일: 2026-07-16

프로그램 기준: `Label_Match v2.0.36`

> 현재 UI 계약은 `현품표/이적 QR + QA 제품 샘플 3개 + 최종 라벨지 = 5단계`입니다. sealed 이적 QR 경로는 물리 스캔도 5회이고, PHS=2 + F4 경로는 전량 N개를 별도로 스캔하므로 물리 스캔이 N+5회입니다.

이 파일은 작업자 본문이 아니라 OUTLINE 게시자와 관리자 확인용입니다. 2026-07-16 작업에서는 로컬 원고와 게시 도구만 준비하며, 별도 게시 승인이 있기 전에는 OUTLINE에 쓰기 요청을 보내지 않습니다.

## 1. 게시 대상

- 기존 위치: `직산 사업장 / 💻 직산 프로그램 / Label_Match(포장실 프로그램)`
- 기존 문서 URL: `https://wiki.kmtecherp.com/doc/label_match-uMZaThRmO1`
- 기존 document id: `4115be8b-488a-4934-80af-f0f9e4ee721b`
- 문서명 유지: `Label_Match(포장실 프로그램)`
- Markdown 원본: `Label_Match/docs/OUTLINE_LABEL_MATCH_USER_MANUAL_20260626.md`
- 캡처 원본·주석본: `Label_Match/docs/assets/label_match_user_manual_20260716_display2_v2_0_36`

별도로 만들어졌던 legacy 문서 URL은 운영 게시 대상으로 사용하지 않습니다.

## 2. 2026-07-16 캡처 계약

- 작업자 본문 이미지: `annotated/` 17장
- 변경하지 않은 화면 원본: `raw/` 17장
- 캡처 해상도: 전부 2560×1440
- 캡처 위치: 비주 모니터 `\\.\DISPLAY2`, 화면 rect `(693, -1440)–(3253, 0)`, work rect `(693, -1440)–(3253, -48)`
- 전체 미리보기: `contact_sheet.png`
- 기하 좌표·파일 해시·격리·픽셀 QA: 매니페스트 계약 v2의 `manifest.json`
- 정확한 프로그램 소스: commit `faaca1c7783e2e7a91b0fea862e23eefefde09bd`, tree `3d169822fae1cf978b3623cfbb433e5e647615bb`, 앱 버전 `v2.0.36`
- 창 계약: 앱 root가 DISPLAY2 안에 완전히 포함되고 대상 앱 창이 foreground이며, 대화상자는 앱 root 소유·하위 창이어야 함
- 중앙 목록 계약: 일반 QA 상태는 기대 단계와 실제 관측값의 다섯 행, F4 상태는 exact 전량 행과 `x/N`을 독립적으로 추출해 매니페스트와 픽셀 캡처가 일치해야 함
- 개인정보 계약: 호스트명, 사용자 프로필·임시 폴더 절대경로, 운영 바코드·토큰·식별자를 이미지·매니페스트·게시 결과에 기록하지 않음
- 게시 가능한 매니페스트 결과: v2 `status=PASS`, `image_contract_ok=true`, 모든 캡처의 소스·버전·DISPLAY2·비주 모니터·foreground·root containment·중앙 목록·개인정보 검사가 통과함
- raw 대비 annotated near-black 픽셀 증가율: 각 이미지 0.5% 이하

주석은 Tk 위젯 또는 Win32 창의 실제 좌표에서 만든 빨간 테두리와 라벨입니다. 게시에는 `annotated/`만 올리고 `raw/`, 연락 시트, 매니페스트는 관리자 검증 증거로 보관합니다.

본문 등장 순서 기준 첨부 목록:

1. `01_startup_1_of_5.png`
2. `02_settings_worker.png`
3. `03_phs_master_f4_ready.png`
4. `04_f4_target_quantity.png`
5. `05_full_rescan_in_progress.png`
6. `06_full_rescan_complete.png`
7. `07_qa_sample_1.png`
8. `08_qa_sample_2.png`
9. `09_qa_sample_3.png`
10. `10_complete_5_of_5.png`
11. `17_sealed_transfer_qr.png`
12. `11_mismatch_error.png`
13. `12_duplicate_error.png`
14. `13_current_set_cancel.png`
15. `14_completed_tray_cancel_input.png`
16. `15_restore_before_close.png`
17. `16_restore_resumed.png`

## 3. 본문에서 반드시 유지할 운영 의미

- 화면의 주 정보는 현재 세트, 다음 스캔, 중앙 아래쪽 실제 스캔 목록입니다.
- 왼쪽은 현재 품목·작업·멤버십 정보만 보여 주고, 오른쪽의 `이번 세션`·`스캔 기록`·`통과 요약`은 보조 탭입니다.
- 중앙은 현재 단계, 5단계 진행, 한 위치의 안내·경고, 스캔 입력, 일반 QA 다섯 행 또는 F4 exact 전량 목록을 레이아웃 변경 없이 유지합니다.

- QA 제품 샘플 3개는 전체 멤버십이 아닙니다.
- sealed 이적 QR은 bundle의 정확 수량·멤버십 해시·권한 범위·원장 버전을 서버에서 검증하고 F4를 사용하지 않습니다.
- PHS=2 등 sealed 증거가 없는 중앙 포장은 QA 전에 F4를 시작합니다.
- F4 수량 N은 이적 화면과 실물을 대조해 수동 입력합니다. 발행 수량과 자동 연결되지 않습니다.
- F4는 동일 품목의 고유 바코드 N개를 모두 요구하며, 완료 뒤 별도의 QA 샘플 3개를 진행합니다.
- 최종 라벨지는 품목 코드, 최소 길이 31자, 유효한 `6DYYYYMMDD` 생산일자가 필요합니다.
- F3은 승인된 소량 예외일 뿐 sealed/F4 멤버십 증거의 대체 수단이 아닙니다.
- 같은 날 미완료 상태는 복구할 수 있고 전날 상태는 만료됩니다.
- Syncthing과 `C:\Sync`는 운영 경로로 설명하지 않습니다.

2026-07-16 전체 체인 시연에서는 다음 조건을 추가로 고정합니다.

- 시연 수량 `N=60`, PHS+F4 물리 스캔 총 65회
- 현재 이적 컨테이너 UI에 sealed transfer QR 표시·인쇄가 없으므로 sealed 경로 사용 금지
- 이적 `ACK/COMMITTED`와 authoritative readback의 bundle·수량·해시 일치 후 포장 시작
- 포장 receipt `ACKED`와 readback 확인
- 응답이 불명확하면 동일 실물 재스캔이나 새 idempotency key 발행 금지; 기존 bundle/key로 조회 후 에스컬레이션

## 4. 게시 전 로컬 검증

컴포넌트 저장소에서 다음을 실행합니다.

```powershell
python -B tools/publish_outline_user_manual.py --dry-run
python -B -m pytest -q -p no:cacheprovider tests/test_label_match_core.py tests/test_package_logistics.py tests/test_outline_label_match_manual.py
```

드라이런은 네트워크 쓰기를 하지 않으며 다음 조건을 검증합니다.

- Markdown 이미지 참조 17개, 고유 이미지 17개
- 모든 상대 링크가 승인된 새 자산 폴더 아래에 존재
- 매니페스트 PASS와 이미지 17개 일치
- 전 이미지 2560×1440
- 매니페스트 계약 v2와 앱 버전 `v2.0.36`
- commit `faaca1c7783e2e7a91b0fea862e23eefefde09bd`와 tree `3d169822fae1cf978b3623cfbb433e5e647615bb`
- 비주 `\\.\DISPLAY2`의 정확한 화면·work rect, foreground, 앱 root 완전 포함, 대화상자 소유 관계
- 일반 QA 다섯 행 또는 F4 exact 행의 기대값·관측값 일치
- 호스트명·사용자/임시 절대경로·운영 식별자가 없는 개인정보 검사
- near-black 증가율이 0.5% 이하
- `파일 업로드` 문구와 `늘 버튼으로` 오탈자 없음
- `오늘` 버튼 문구 존재

## 5. 승인 후 게시 방법

게시 권한과 승인된 `OUTLINE_API_TOKEN`이 별도로 준비된 경우에만 실행합니다.

```powershell
python -B tools/publish_outline_user_manual.py --report-path docs/outline_user_manual_publish_result_20260716.json
```

도구는 17개 주석 이미지를 attachment로 업로드하고 상대 링크를 attachment URL로 치환한 뒤 기존 document id에 `replace` 업데이트합니다. 게시 후에는 브라우저에서 강제 새로고침하고 다음을 확인합니다.

1. 문서 제목과 상위 컬렉션이 그대로인지
2. 이미지 17장이 본문 순서대로 보이는지
3. 상대 `assets/...` 링크가 0개인지
4. 5단계, sealed 5회, PHS+F4 N+5회와 전체 체인 N=60/65회가 정확히 보이는지
5. 오류·취소·복구 안내가 누락되지 않았는지
6. 게시 결과 보고서가 PASS이고 매니페스트 v2의 소스·DISPLAY2·foreground·중앙 실제 목록·개인정보 계약이 유지되는지

## 6. 새 PC 관리자 확인표

1. 프로그램 버전과 실행 상태
2. 작업자 이름 저장
3. 스캐너 입력
4. `C:\ProgramData\KMTech\Label_Match\data` 로컬 기록 생성
5. `C:\ProgramData\KMTech\DirectSync` 개별 PC 상태 생성
6. worker PC registration 상태
7. direct-sync relay 상태와 작업 스케줄러
8. 서버에서 해당 PC 데이터 조회
9. sealed 이적 QR 검증 경로
10. PHS+F4 전량 스캔과 같은 날 복구

운영 토큰, 접근 코드, 런타임 DB, 실제 생산 로그는 문서나 Git 저장소에 넣지 않습니다.
