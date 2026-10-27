REJECT · P1 0 · P2 1 · P3 1
검토 모델: Claude — 세션 규칙상 정확한 식별자는 저장소 글에 안 씀(커밋 Co-Authored-By 줄 참고)

범위 f9715a0b..865caa65(65 커밋): merge 8개 --remerge-diff, 덧 d0b3efa·1472aeb·865caa65, 겹친 함수, 753·756, 선언만 읽음. 시험·빌드·설치기·앱 실행 0(sha256·정적 inventory 스캐너만 계산).

## P2
1. tests/test_recovery_relay_bootstrap.py:81 (865caa65) — `updates.FIRST_SETUP_READY_SECONDS` 는 저장소 어디에도 없음. _bootstrap 의 relay_deadline 은 늘 _RelayPreparationDeadline(auto_update.py:4197→3747)이라 가짜 launch_version 이 이 가지로 들어가 AttributeError, recovery_writer_result:4127 은 이를 안 잡음 → test_recovery_gui_gets_one_ordinary_relay 12건·concurrent·launch_failure 재시도·readiness_failure 등 최소 15 사례 빨강. 상수가 있어도 :83-84 `begin_preparation()==600` 은 launch 때 시계 0 일 때만 참(request_absent·stale 는 sleep 으로 시계가 감). 즉 ZIP 머리에서 738 relay 준비 시작점 회귀가 초록일 수 없음. 제품·선언 바이트 영향 0. 고침: :81 을 실제 절대 상한(시작 시계+BOOTSTRAP_OBSERVATION_CAP_SECONDS)과 비교, :83-84 를 `begin_preparation()==deadline`(한 번만 시작)으로 바꾸고 이 파일을 실제로 돌려 확인.

## P3
1. docs/spec/contracts.md:12·operations.md:30 (d0b3efa) — 「배정 행 원본 범위 재구성 중 잡은 OSError 는 그 행만 건너뜀」이라 쓰나 runner:580 은 SourceFileInputError(OSError 아님 · 잠김·없음)도 같이 건너뜀. 동작은 맞고(그 파일은 뒤 스캔 읽기에서 파일 오류 목록에 남음) 글만 빠짐.

## 확인(문제 0)
- 충돌 손 풀이: 4a4ed3e·8a14077·c18ed93·232abf5 충돌은 선언 셋뿐, 코드는 자동 병합. c18ed93 auto_update = 8f95704+18a753d 델타, 머리 = 그 위 aa4cd75·2413386 정확히. 754 네 파일 = aec850b, runner = 0a6db14.
- auto_update 겹침: ROLLBACK_PENDING 증명 거절만 _startup_gate:3536 에서 bootstrap(UPDATE_PHASE)으로 넘김, 그때 _STARTUP_ADMISSION 0 유지. 첫 설정 manual 길은 그대로 거절, resolve_lifecycle_context 는 UNKNOWN/FAILED 만 포인터 검사 면제. busy 증명은 lock-wait(START_CURRENT) 길에 새로 붙고 busy 예외 길은 base 와 같은 두 증명. 준비 예산은 bootstrap 만 defer(상한 1140, 첫 relay launch/readback 에서 600 시작, bounded_by 가 시작점 공유).
- user_relay 2 초 다시 잡기: 잠금·허가 안 쥠, 600 초 안. fix19 P3 셋은 그대로.
- onboarding: 746 은 이 파일 줄 변경 0, 754 report_attempt·writer_admission 은 startup=True lifecycle 범위 안. INSTALL_THIS_PC 는 rules pin 한 줄(3cbc23da=머리 바이트)만, 제거 복원 함수 안 바뀜.
- 753: direct_sync_runtime·operator·product_host = f9715a0b 바이트, runner = 752 머리, test_relay_log_admission·문서 753 줄 없음, 선언에 3cc990e 쪽 해시 0.
- 756: 8b014f92·636a5848 범위 밖, Authorization/Basic 추가 줄 0, rules json 은 UPDATE_LOCK_WAIT_EXPIRED 한 줄만.
- 선언: 전환·교체 표 after 7356 개 = 머리 release_digest(어긋남 0), inventory 다시 계산 cd6e0c3c… = py·ps1 핀, 설치기 probe 15398d66… 일치, 원자 교체 카탈로그 Restore 지문 b589aebd… = INSTALL_THIS_PC.ps1:2439 Move 1개.
- 시험 약화: 빠진 단언 24줄은 같은/더 센 단언으로 바뀜(글은 base 제품 글에 정확 일치로 조임). 새 skip 은 새 파일 test_relay_instance_startup_retry.py:17(nt 전용) 하나, 기존 시험 제외 아님. d0b3efa(별칭 복사로 조임·scan oserror 추가)·1472aeb(report_publication:146-174 대로 거절 시 바이트·셈 0)·865caa65 endpoints 두 곳(복구 ROLLED_BACK 뒤 둘째 호출이 hotfix, BACKLOG 11/15 항목과 같음)·w9scanonly 한 줄은 약화 0.
