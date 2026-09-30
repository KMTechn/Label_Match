# 스캐너 포장 전환의 제거 경계

DESIGN §9-나의 LM 화면 진입점을 대체 구현 6dc4ae8에서 함께 제거했다. 구형 캡처 도구 두 곳도 같은 커밋에서 새 스캔 진입 또는 폐기 안내로 바꾸었다. 시작 경고의 모달 호출은 3791aac에서 작업 화면 안내로 바꾸었다.

| 제거한 화면 진입점 | 현재 동작 |
| --- | --- |
| `_prompt_manual_complete`, F3 수동 완료 분기 | 현품표 + 제품 네 개의 다섯 번째 내구 완료 |
| 중앙 PHS2 1/1 제품 거부 상태 | 배경 구성원 조회와 표본 네 개 입력 |
| `_prompt_sealed_transfer_exchange`, `_prompt_new_seal_verification` | 교환 카드·대상·대체품과 전자 영수증 검증 |
| `_prompt_exact_rescan` | 스캐너 교환 입력; 저장된 옛 exact-rescan 복구 자료는 보존 |
| 현품표의 차단 중앙 조회와 시작 경고창 | 로컬 보호 저장 후 배경 조회, 창 안 안내와 소리 |

생산 Python, 설치 PowerShell, PyInstaller spec에서 네 `_prompt_*` 이름을 고정 문자열로 검색하면 참조는 0이다. 시험의 부재 검증과 이 문서의 목록은 호출 소비자가 아니다. 모듈 파일이나 설치 자산은 삭제하지 않았으며 캡처 도구 두 파일의 구문 검증을 통과했다. stock portable builder의 import probe는 최종 후보 검증에 포함한다.

DESIGN §9-가의 옛 다섯 입력 구현·SAMPLE/FINAL 상수·오류 처리·carrier 분류·과도기 분류를 보존했다. 기존 exchange journal과 전송부, 전체 구성원 snapshot, CAS, 봉인·서명 lease, 인증·관리자 복구, 설치 목록도 계속 사용한다. 표본은 중앙 멤버십이나 수량을 정하지 않는다.
