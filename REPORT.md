REJECT · P1 1 · P2 1 · P3 2
Claude Opus 5.5

범위: f9715a0b..209c22aa(3커밋) diff 와 그 diff 가 부르는 파일만 읽음. scratch 사본에서 지문만 다시 계산. 시험·빌드·설치기·앱 실행 0.

## P1
1. tools/register_label_match_worker_pc.py:255-258,266,271 — 바탕엔 없던 비밀 꼴 검사를 영수증 **정상** 렌더에 새로 넣음. 이름이 비밀 꼴인 환경 변수의 4자 이상 값이 영수증 글자 칸(endpoint·host id·경로·시각·해시)에 부분 문자열로 든 PC → 렌더·대체 렌더 둘 다 거절 → 3195 영수증 미게시. 서버 등록(apply_registration)·신원·자격 파일(3113-3115)은 이미 끝난 뒤 → helper rc2·부분 상태 → 첫 설정 FAILED, 다음 회차 RECOVERY_REQUIRED. BLOCKED 영수증도 같은 값(report_context)으로 거절되면 main() 이 예외로 끝나 FAILED(CLI 로 직접 부르면 3421-3425 가 삼켜 rc0·새 영수증 0). 바탕은 같은 PC 에서 정본 가림이 그 값을 지우고 READY. 이 길 시험 0.

## P2
1. report_publication.py:10 · tools/register_label_match_worker_pc.py:268 · current_user_onboarding.py:495-498 — 대체 렌더가 원 렌더의 가림을 건너뜀(진단 7칸만 표지, 나머지 원문). 영수증은 public_diagnostic_tree(failure_redaction.py:159,184)가 빼던 자유글(blocked_reason·message_ko·error·reason·exception_chain·server_error_code)이 그대로, 정본 FAILED 보고는 failure_traceback 에 [REDACTED:<변수 이름>]이 남음. 입력: 못 지우는 값이 stdout/detail 칸에만 있고 예외 문장이 함께인 실패 → 「보고 못 씀」 보고에 예외 원문·변수 이름(「값 0·이름 0」 위반, 키 이름·Bearer 가림도 빠짐).

## P3
1. report_publication.py:72-104 flush_report_counter·report_io·publish_report 호출처 0(required 규칙은 죽은 코드) · 80,106 기본 코드 MACHINE_METADATA_SECRET_DETECTED 를 던지는 곳 0(실제 코드는 failure_redaction.py:241 PRODUCT_METADATA_SECRET_VALUE_REFUSED).
2. report_publication.py:63-65 셈 파일이 PID 마다 status/ 에 새로 생기고 안 지워짐 → 시작마다 쌓임 · helper 와 onboarding 이 같은 때 실패하면 파일 둘에 각 1(한 셈 2 아님) · current_user_onboarding.py:2011 해제 단계 실패는 경로 없이 세어 셈 파일 0.

## 볼 것 대조
1. 정본: 정상 렌더 그대로, 못 지운 진단만 표지·첫 설정 이어 감 ✓ · 영수증은 P1, 대체 렌더는 P2.
2. status·server_registration_verified·recovery_action·failure_category·manifest_hash* 는 진단 칸 밖 → 표지 0 ✓ · 필수 칸 걸림은 거절·미게시 ✓.
3. helper rc≠0·권한·자료 실패 → FAILED, cause_code·__cause__ 보존 ✓.
4. 셈은 OSError 만 삼킴(atomic_replace 는 OSError 계열) ✓ · 표지 원자 교체 ✓ · 새 로그 0 ✓ · 한 시도 안 같은 보고 중복 셈 0 ✓.
5. onboarding·removal 의 rc0 길은 모두 정본 쓰기 뒤, 표지 길도 쓰거나 예외 → 앞 회차 READY 통과 길 0 ✓(helper CLI rc0 길은 P1 참고).
6. 인벤토리 핀 재계산 21bca7c7 = py·ps1 핀 ✓ · 설치기 probe 해시 재계산 d25947c0 ✓ · startup 전환 표 23개 모두 새 지문 + report_publication.py 추가 ✓ · 옛 표 after 지문 갱신 ✓ · 쓰기 구성원 목록 불변 ✓ · 포장은 root *.py 복사로 실림 ✓. 이적 transition json·APP_ROOT_FILES 는 이 저장소에 없음 → 확인 못 함(맞출 것: 새 파일 1 · 바뀐 5 파일 지문 · after_inventory 21bca7c7).
7. d72b3d0·7a5cc8a 는 tests/ 추가만, 기존 시험 삭제·약화 0 ✓ · 고침 커밋의 시험도 추가만.
