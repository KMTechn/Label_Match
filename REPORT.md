REJECT · P1 0 · P2 1 · P3 3
모델: 세션 규칙상 저장소 글에 모델 식별자 미기재 — 이 커밋 Co-Authored-By 참고

범위: 209c22aa..26a4181c diff 와 부르는 파일만 읽음. 지문·인벤토리 핀·probe 해시만 scratch 사본에서 재계산. 시험·빌드·설치기·앱 실행 0.

## P2
1. report_publication.py:116-120 · current_user_onboarding.py:499-524 — 대체 렌더가 원 렌더(redact_json_for_sink) 가린 트리가 아닌 날 payload 에서 출발(영수증용 public_diagnostic_tree 만 칸마다 씌움). 입력: 4자+ 비밀값 S, 보고에 S 품은 비문자 진단값(message 정수)과 비진단 문자열(hostname·경로·시각) → 원 렌더는 문자열은 가리나 정수로 REPORT_UNAVAILABLE → 대체가 문자열 S 원문을 남겨 PRODUCT_METADATA_SECRET_VALUE_REFUSED → 1886 READY(2048 제거) 쓰기 예외 → FAILED·rc≠0. 진단 칸 하나 표지로 될 일을 정본 전체 거절(바탕과 같은 결과라 새 길은 아님, 3번 미충족). tests/test_report_publication_review.py:133 이 이를 정상으로 못박음.

## P3
1. current_user_onboarding.py:503-510 · failure_redaction.py:165-183 — 칸마다 투영 뒤 부모 단계 public_diagnostic_tree 가 다시 재귀. 입력: 중첩 dict 의 exception_chain·last_line 자유글 → 안쪽 봉투가 부모를 자유글로 보이게 해 dict 전체가 public_record 로 접힘(pid 등 형제 칸 빠짐) · command 결과 stdout 문자열→{"message":…}. 511-512 주석과 다름(꼭대기 칸은 안전).
2. current_user_onboarding.py:2035 해제 단계 실패는 여전히 경로 없이 세어 셈 파일 0 · report_publication.py:86-92 셈은 프로세스 지역 값 → 다음 실행 첫 실패가 {"count":1} 로 덮음(시험 36 은 한 프로세스라 못 봄).
3. report_publication.py:109,138 기본 코드 MACHINE_METADATA_SECRET_DETECTED 던지는 곳 0 · 126-134 required=False·outcome 쓰는 곳 0 · tools/register_label_match_worker_pc.py:260-262 빈 줄 3.

## 볼 것 대조
1. 새 import 는 failure_redaction.py:158(함수 안)뿐. 진입 모두 sys.path 준비 뒤: auto_update.py:22-24→26 · register 23-28→30 · relay_install_pack 24-26→48 · relay_operator·guarded_reconcile·legacy_quiescence ✓. report_publication.py 는 같은 앱 뿌리, 포장 root *.py 복사(build_portable_release_candidate.py:346) → -I 시작 ModuleNotFoundError 0 ✓. 이적 전용(transition json·_write_json_atomic_create_new)은 이 저장소에 없음.
2. _write_registration_report 는 f9715a0b 와 뜻 같음 · 새 비밀 검사 0 · 등록 뒤 미게시 새 길 0 ✓.
3. 진단 문자열은 늘, 비문자는 엄격 검사 거절 때만 표지 ✓ · 표지는 「보고 못 씀」뿐, [REDACTED:이름]도 표지로(이름 0) ✓ · 출발점은 P2.
4. 셈 쓰기 except Exception 로 삼킴, cause·exit code 불변 ✓ · 폴더당 파일 하나 원자 교체 ✓ · PRODUCT_METADATA_SECRET_VALUE_REFUSED 를 prepare_report·fallback_tree·refuses 가 잡음 ✓.
5. rc0 길(1886·2048)은 정본 쓰기 성공 뒤뿐 ✓ · status·server_registration_verified 표지 0 ✓.
6. 바뀐 6 파일 옛 지문 0·새 지문 수 일치(startup 23 전환 report_publication.py 포함) ✓ · 인벤토리 핀 9ad98ef8 재계산 일치 ✓ · probe 2d27a327 재계산 일치 ✓ · 시험 삭제 0, 바뀐 곳은 셈 파일 이름·영수증 입력(바탕 복귀에 맞춤)뿐 ✓.
