REJECT · P1 0 · P2 1 · P3 2
검토 모델: Claude Opus 5.5

범위 8b014f92..636a5848 · 읽기 + 가림 함수 단독 대조(f9715a0b/8b014f92/636a5848, 가짜 값 `<C>`)만 · 시험·빌드·설치기·앱 실행 0 · pwsh 없음 → PS 는 정적 대조

## P2
1. `failure_redaction.py:388` · `tools/failure_redaction.ps1:175` — 앞 P2-2 가 반만 풀림. 예외가 「이름=」 바로 뒤 따옴표만 봄. `=` 둘레 공백(RFC 7235 BWS)이면 꼬리(py:315·ps1:115)가 안쪽 키 이름을 먹고, 키 훑기는 py:320·ps1:120 에서 건너뜀. `Authorization: Token token = "<C>"`(`token= "<C>"`·`token ="<C>"`·`Basic password = "<C>"`·JSON 안 `\"` 꼴도) → 옛 `token = "[REDACTED:field]"` → 새 `[REDACTED:field] = "<C>"` 원문. Bearer 는 건너뛰기로 가려짐.

## P3
1. `py:327` · `ps1:126` — 키 전체 일치라 JSON 글의 `\n`·`\t` 뒤 헤더는 키가 `nAuthorization` 이 되어 못 잡음. `{"stderr":"Host: h\nAuthorization: Basic <C>"}`(Token·Negotiate·HTTP_ 꼴 도) → 8b014f92 가림 → 새 원문. f9715a0b 도 원문이라 판정 기준 밖 · 범위 안 생산자 못 찾음(lifecycle_channels 는 경로·sha 뿐) · 계약 문서 「인용 JSON·stdout/stderr 같은 규칙」보다 좁음.
2. `py:311` · `ps1:112` — 새 `\"…\"` 값 가지는 닫는 `\"` 바로 뒤 붙은 글을 남김(모든 민감 키). `password=\"<C>\"<C>` → 옛 전부 가림 → 새 끝 `<C>` 원문. 이런 꼴은 드묾.

## 풀림 확인
- P2-1: 헤더 5종(Proxy-·X-·HTTP_ 포함) × `: `·`=`·`:` × 틈 9종(LF·들여쓴 LF·`\t`CRLF·빈 줄 등) × 대소문자 3 = 405 입력에서 옛 판보다 덜 가림 0. 출력도 옛 `Bearer [REDACTED:field]` 꼴.
- P2-2: 붙은 `token="`·`'`·`access_token=`·`password=`·쉼표 목록·JSON `\"` 꼴 가림(P2 공백 꼴 빼고).
- P3: `proxy_authorization:`·`authorization_status=` 뒤 진단 원문 보존 · 꼬리 복사 제거(`match(text,pos)`·`\G`): 2만/4만/8만 줄 0.29/0.50/1.01 s 선형(8b014f92 4만 줄 1.89 s) · 역슬래시 짝: `\"` 앞 멈춤, 끝 `\\`·`\/`·가운데 `\` 가림, 출력 JSON 10꼴 모두 유효 · 문서 두 곳 동작과 맞음(P3-1 빼고).
- Py↔PS: 새 정규식·`\G`/match(pos)·`\Z`/`\z`·Bearer 건너뛰기·따옴표 예외·`\"` 바꿈 같은 뜻 · ps1 BOM 없음/비ASCII 0/LF.
- 원 오류: errno/winerror·native 보존, Bearer 다음 줄 첫 토큰 가림은 옛 판과 같음(ANSWER-4) · 기록 꼴·잘림·상한 코드 변경 0.
- 지문: 인벤토리 재계산 `c4d085c9…`(107 sink) = py·ps1 핀 · 5 파일 새 sha = 실제 · 설치기 표 옛 값 0, 새 값 개수 = 옛 개수 · probe 본문 sha `b44a6914…` 재계산 일치 · 바뀐 줄은 해시뿐, BOM·LF 유지.
- 시험: 새 파일 1개 · 고친 곳은 Bearer 기대값을 옛 꼴로 정확히 바꾼 것(삭제·완화 0) · P2 공백 꼴은 시험에 없음.

## 참고(기존·판정 밖)
`token: Bearer <C>`·`bearer_token: Bearer <C>` 는 f9715a0b 부터 원문(건너뛰기가 authorization 키에만).
