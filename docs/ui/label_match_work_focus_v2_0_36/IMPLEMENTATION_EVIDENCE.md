# Label Match v2.0.36 로컬 구현 증빙

이 문서는 `Label_Match` 작업 중심 UI의 **로컬 후보 승인 및 패키지 준비 결과**를 고정한다. 현장 승인, 커밋, 원격 push, tag, release를 승인하는 문서가 아니다.

## 후보 잠금

| 항목 | 값 |
|---|---|
| 기준 HEAD | `d97c8de4ae7596943cdac22f933aa030590f1bf2` |
| 애플리케이션 버전 | `v2.0.36` |
| `Label_Match.py` SHA-256 | `AE117D111F451828272BB121E8CC474987C90287DC836C75C5AE34C5AD22C328` |
| `tools/capture_label_operator_ui.py` SHA-256 | `D083C051B1D2440F196075EBF3A0DBB195520E0316A7ABFAC1B050554E42A891` |

패키지와 캡처는 위 잠긴 소스·하네스를 기준으로 생성했다. 기존 logistics, outbox, direct-sync, 복구, 취소 계약과 사용자 소유 매뉴얼·캡처 변경분은 덮어쓰지 않았다.

## 구현 계약

- 선정안은 `option-1-three-column.svg`의 3열 작업대다.
- 중앙은 현재 단계, 다음 행동, 5단계 레일, 단일 안내/경고, 입력, 실제 현재 세트 스캔 목록을 유지한다.
- 중앙 하단은 요약 목업이 아니라 수락된 `current_set_info["raw"]` 값 5행을 표시한다.
- QA `x/5`와 F4 전체 재스캔 `x/N`은 별도 상태로 유지한다.
- 실패 입력은 정상 목록에 추가하지 않고 마지막 정상 스캔을 보존한다.
- 제출 차단 시 단일 중앙 경고와 `제출 재시도` 행동만 표시하며, 정상 상태 복귀 뒤 오래된 행동을 남기지 않는다.

## 자동 회귀 검증

| 범위 | 결과 |
|---|---:|
| 전체 pytest | `544 passed` |
| UI·행동 매핑 집중 회귀 | `189 passed` |
| 비즈니스·logistics 계약 회귀 | `140 passed` |
| Python compile | 통과 |
| `git diff --check` | 통과; 기존 줄바꿈 경고만 존재 |

## 상태·해상도 캡처

두 매트릭스 모두 `1280×1024`, `1366×768`, `1440×900`, `1920×1080`, `2560×1080`의 5개 크기와 11개 상태를 검사했다. 검사 상태는 대기, QA 진행, F4 진행/완료, sealed, 오류, 정상/부분 완료, 복구, 기록 읽기 전용, 제출 차단이다.

| 배율 | 매니페스트 | SHA-256 | 결과 |
|---|---|---|---:|
| `1.0` | `C:/company/program/Label_Match/tmp/label_operator_scale1_final_ae117d_d083_20260715/manifest.json` | `D1A7C05D443D776E3B5AFFEA26A6FD5E1CF3CFC85232C5A14937C3FD59ADEDF2` | `55/55` |
| `1.4` | `C:/company/program/Label_Match/tmp/label_operator_large_text_strict_final_ae117d_20260715/manifest.json` | `F33F54DC8875239E2D9D4240A13F481D469AFAB3D7DE7BB18FBEB69A038590CE` | `55/55` |

두 매니페스트 모두 실패·레이아웃 이슈가 없고 compact→wide→compact 왕복 검사가 통과했다. 동작 버튼은 오류·제출 차단 상태에서만 정확히 매핑되며 stale 행동은 0건이다.

## 격리 패키지 검증

| 항목 | 결과 |
|---|---|
| 패키지 | `C:/company/program/Label_Match/tmp/label_match_release_candidate_v2_0_36_20260715_191728/Label_Match-v2.0.36.zip` |
| 크기 | `88,695,377 bytes` |
| SHA-256 | `d69ed1f563a82e9018da84a4f4f8569b003acfb4307a07033b63516137b9c5b1` |
| PyInstaller spec | `4/4` (`Label_Match`, install pack, PC registration, relay runner) |
| CLI smoke | `3/3` |
| GUI smoke | 격리 환경에서 통과 |
| 필수 파일 | `25/25` |
| 금지 파일 | `0` |
| archive parity | `3,385/3,385` 파일 일치 |

패키지는 token 없는 설정 템플릿과 GitHub/stable 배포 설정을 사용했다. GUI smoke는 운영 `%ProgramData%` 대신 후보 폴더 아래의 격리된 데이터·TEMP 경로에서 수행했다.

## 승격 상태

- 로컬 구현 후보: 승인
- 로컬 패키지 후보: 생성·검증 완료
- 현장 검증: 대기
- UI 기능 브랜치 commit/push: 준비 완료
- tag/release: 현장 검증 전 미수행
