ACCEPT · P1 0 · P2 0 · P3 3
모델: 세션 규칙상 저장소 글에 모델 식별자 미기재 — 이 커밋 Co-Authored-By 참고

범위: 26a4181c..aec850b9 diff 와 그 diff 가 부르는 파일만 읽음. 지문·인벤토리 핀·probe 해시만 scratch 사본에서 재계산. 시험·빌드·설치기·앱 실행 0.

## P3
1. tools/register_label_match_worker_pc.py:260-262 — 빈 줄 3 그대로(앞 P3-3 미해결). 린트 → E303.
2. tests/test_report_publication_review.py:150 — 옛 :133 의 「대체도 거절되면 옛 정본 바이트 보존·셈 0」 단언이 남은 거절 길로 옮겨지지 않고 빠짐. 입력 {"status":"READY","pid":4826}+비밀 4826(비진단 비문자) → 여전히 PRODUCT_METADATA_SECRET_VALUE_REFUSED(바탕과 같음)인데 그 길을 지키는 시험 0.
3. report_publication.py:36,147 replace_strings=True 기본·가지, :92 failure_count(path=None) 의 None 열쇠 — 제품 호출 0(시험만) · 시험 이름 test_report_publication_review.py:92 「public_projection」 은 투영 제거 뒤 뜻이 안 맞음.

## 앞 REPORT 대조
1. P2 풀림 ✓ — 대체가 _redact_json_tree(원 렌더와 같은 칸·키 가림)에서 출발(current_user_onboarding.py:502-506). 정수 message 는 refuses 로만 표지(report_publication.py:41-43). hostname·경로·시각은 가림 유지("station-보고 못 씀"). 남은 비밀은 최종 assert(:497-500)가 막음(fail closed) → READY :1874·제거 :2038 쓰기 성공. 비진단 문자열 라벨도 이름 없는 표지로 바뀜(failure_redaction.py:386-388): 원 렌더와 라벨 꼴만 다르고 가림은 같음. 옛 :133 기대 → :150 바로잡힘 ✓.
2. P3 풀림 — 대체 public_diagnostic_tree 투영 제거로 형제 칸 접힘 0(review3:28 시험) ✓ · 해제 단계가 _report_path 를 넘기고 report_attempt(:1995-2025) 안이라 시도당 한 번 셈 ✓ · 셈은 파일 읽고 더해 씀(:129-137). 호출자가 writer admission 을 쥔 채라 프로세스 사이 순서 보장 ✓ · report_io·MACHINE_METADATA_SECRET_DETECTED 기본 제거, unavailable_codes 필수 ✓ · 빈 줄 ✗(P3-1).
3. 새 깨짐 0 — rc0 은 _write_json_bytes_atomic 성공 뒤만(:513) ✓ · status 는 진단 칸 아님, server_registration_verified(bool)는 손 안 댐 ✓ · 셈 읽기·쓰기 실패는 except Exception 로 삼키고 delta 보존. WriterFenceError·StartupStopRequested 도 Exception. 셈은 공개 성공 뒤만, 공개 실패면 0 ✓ · 등록 영수증은 f9715a0b 대비 공백만 다름 ✓ · 새 최상위 import 는 contextvars 뿐. report_publication 은 함수 안(failure_redaction.py:387,419), 같은 앱 뿌리·PS1 핀 대상 ✓ · 바뀐 5 파일: 옛 지문 0, 새 지문 수 같음(32/24/23/37/34). 인벤토리 핀 7404dc93·probe efaf3743 재계산 일치. PS1·핀 diff 는 해시 치환뿐 ✓ · 시험 삭제 0. 바뀐 단언은 새 API·공개 뒤 셈에 맞춤(셈 0→1 확인 추가) ✓. 빠진 것은 P3-2 뿐.
