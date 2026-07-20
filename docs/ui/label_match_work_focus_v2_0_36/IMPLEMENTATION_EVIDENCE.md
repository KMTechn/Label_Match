# Label Match v2.0.36 작업 중심 UI 증빙

> **2026-07-20 통합 주의:** 이 문서는 아래에 고정한 과거 feature-source의
> 로컬 UI 증빙입니다. 이후 cancellation/visible-capture 통합 `main`의 정확한
> 매뉴얼, 서명 패키지 또는 현장 승인 증빙이 아니며 production 승격 판단은
> 계속 `DENY`입니다.

이 문서는 작업 중심 UI의 기능 브랜치 구현과 로컬 시각 검증 결과를
고정한다. 현장 승인, `main` 병합, tag, 서명, release 또는 배포를
승인하는 문서가 아니다.

## 소스 잠금

| 항목 | 값 |
|---|---|
| UI 소스 commit | `4e88dacff63587af8d1008cc0a216a03267a34a8` |
| UI 소스 tree | `afbc6292715f16608c81f49a88f0e1b99833445b` |
| 애플리케이션 버전 | `v2.0.36` |
| `Label_Match.py` SHA-256 | `b27e9fc7ae874a4cceb1513bcebe3d00914c1543ae9ccfaba9dc9887eeb2790f` |
| `tools/capture_label_operator_ui.py` SHA-256 | `f5040ebf87afebad78db43cc8a2db6dcdc9b63ae952343a94a1c528c75205228` |

캡처와 회귀 검증은 위 clean detached worktree에서 실행했다. 사용자 소유
취소·물류·매뉴얼·publisher 변경은 커밋 범위에서 제외했고, dirty primary를
동기화할 때도 동일 stable patch와 untracked 파일 집합을 보존했다.

## 표시·업무 계약

- 중앙은 현재 단계, 다음 행동, 5단계 레일, 단일 안내/경고, 입력, 실제
  현재 세트 스캔 목록을 유지한다.
- 현품표 행에는 권위 있는 품목코드만 표시한다.
- 제품 1~3, 최종 라벨, F4 전체 재스캔 행에는
  `품목코드 · 간략 식별값`만 표시한다. 목록 셀에 전문을 노출하지 않는다.
- 선택 행 상세에는 원래의 `raw` 전문을 byte-for-byte 유지한다.
- 표시 요약은 presentation-only 변환이다. `raw`, `parsed`,
  `current_set_info`, ledger, API, event, outbox, 저장 계약은 변경하지 않는다.
- QA `x/5`와 F4 전체 재스캔 `x/N`은 별도 상태로 유지한다.
- 실패 입력은 정상 목록에 추가하지 않고 마지막 정상 스캔을 보존한다.
- 제출 차단 시 단일 중앙 경고와 `제출 재시도` 행동만 표시하며, 정상
  상태 복귀 뒤 오래된 행동을 남기지 않는다.

## 자동 회귀 검증

| 범위 | 결과 |
|---|---:|
| 전체 pytest | `688 passed, 1 expected skip` |
| Python compile | 통과 |
| `git diff --check` | 통과 |
| 독립 code/capture/scope 감사 | `ALLOW`, P0/P1/P2 `0/0/0` |

expected skip은 release build 전 staged package가 없어서 건너뛰는 설치기
테스트 한 건이다. 화면 요약, 원문 상세 보존, 품목코드 prefix 보존,
opaque 식별값 fingerprint, 오류 후 마지막 정상 행, compact→wide→compact
왕복을 회귀 테스트로 고정했다.

## DISPLAY2 상태·해상도 캡처

두 승인 매트릭스는 비주 모니터 `\\.\DISPLAY2`, work area
`(693,-1440)-(3253,-48)`, DPI `96×96`에서 실행했다. 각 매트릭스는
`1366×768`, `1440×900`, `1920×1080`, `2560×1080`, `2560×1392`의
5개 크기와 다음 15개 상태를 검사한다.

`waiting`, `qa_master`, `exact_first`, `exact_active`, `exact_complete`,
`qa_progress`, `qa_product_2`, `qa_product_3`, `sealed`, `error`,
`full_complete`, `partial_complete`, `recovery`, `history_readonly`,
`submission_blocked`.

| 배율 | 증거 ID | manifest SHA-256 | 결과 |
|---|---|---|---:|
| `1.0` | `label-layout-postfix-4e88dac-scale100-display2-r9` | `b22f51e2ef2c9e33b3186fab5a157fe2f5c3333427e46e0de280344720f3da90` | `75/75` |
| `1.4` | `label-layout-postfix-4e88dac-scale140-display2-r11` | `bbceb418f48109c632f3ee1701bddeb9d27a349ca219246f5760306b8ece3647` | `75/75` |

두 매니페스트 모두 source commit/tree, clean-before/after, pycache 0,
non-primary monitor, DPI/Tk scaling, 창 containment, 글자 요청/실제 크기,
겹침, 검은 영역, 5행 목록, 표시 요약, 선택 원문, focus, 그리고
compact→wide→compact 계약을 통과했다. 독립 시각 감사의 최대 near-black
비율은 각각 `0.006042`, `0.006072`이며 기준은 `0.08`이다.

## DISPLAY2 프로세스 영상

| 항목 | 결과 |
|---|---|
| 증거 ID | `label-layout-postfix-4e88dac-display2-process-r12` |
| 재생 매트릭스 | 5개 크기 × 15개 상태, `75/75` |
| 영상 | H.264, `2560×1392`, `yuv420p`, 12 fps |
| 길이 / 크기 | `56.833초` / `203,095,162 bytes` |
| SHA-256 | `da9a6e02b03415dbc6ff692e667b9d1084c11b6675b2a9b757f2fece59cc6873` |
| FFmpeg blackdetect | `0` events |
| 동반 capture manifest SHA-256 | `292041251db13bc6900ee3af24006e3ba5c7202561ff7eb10a2f8a64d612007c` |

첫 scale-1.4 r10 실행은 62/75 이후 post-quiescence Tk job이 나타나
fail-closed로 거부했다. 해당 시도는 `attempts/` 진단 증거이며 승인 근거가
아니다. 같은 clean commit의 r11과 영상 동반 재생은 각각 75/75를 통과했다.

## 패키지·승격 상태

- 기능 브랜치 `origin/ui/label-work-focus-v2.0.36`: UI 소스
  `4e88dac`까지 push 완료.
- 현재 UI 소스에 대한 승인된 field ZIP: 없음.
- 과거 mixed-tree 또는 이전 UI ZIP은 현재 field/release 후보가 아니다.
- 현장 검증: Rework→Container→Label 순서의 앞 단계 승인 대기.
- `main` 병합, tag, Authenticode 서명, release, 배포: 미수행.
- 이 증빙의 production 승격 판단은 `DENY`다. current exact-commit
  package/manual, repository 보호, 신뢰 가능한 signing identity, 서명 검증,
  field 증거가 준비되고 강화 release workflow가 reviewed `main`이 되기
  전에는 tag 또는 release dispatch를 실행하면 안 된다.
