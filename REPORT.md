REJECT · P1 0 · P2 2 · P3 4
검토 모델: Claude Opus 5.5

범위 f9715a0b..8b014f92 · 읽기 + 가림 함수 단독 대조(옛/새, 가짜 값 `<C>`)만 · 시험·빌드·설치기·앱 실행 0 · pwsh 없음 → PS 는 정적 대조

## P2
1. `failure_redaction.py:373-382` · `tools/failure_redaction.ps1:165-171` — Bearer 가림이 줄어듦. 키 가지가 스킴 낱말 "Bearer" 를 먼저 `[REDACTED:field]` 로 바꾸는데 꼬리는 `[ \t]+`·`[ \t]*CRLF[ \t]+`·「CRLF+한 토큰 줄」만 잡음 → 끝의 `Bearer\s+`(py:387·ps1:182)가 닻을 잃음. `Authorization: Bearer` + LF + `<C>`(들여씀 유무 무관)·`\t`CRLF·빈 줄 → 옛 `Bearer [REDACTED]` → 새 `[REDACTED:field]` + 줄바꿈 + `<C>` 원문. Python text 모드로 받은 stdout/stderr 는 CRLF→LF 라 실제 길. Proxy- 도 같음.
2. `failure_redaction.py:374,385` · `ps1:166,178` — 꼬리가 따옴표 앞에서 멈추고 `copied` 가 그 자리까지 건너뛰어 안쪽 민감 키 가림을 막음. `Authorization: Token token="<C>"`(작은따옴표·`Bearer access_token="<C>"`·`Basic password="<C>"` 도) → 옛 `token="[REDACTED]"` → 새 `[REDACTED:field]"<C>"`.

## P3
1. `failure_redaction.py:322` · `ps1:123` — 키 부분 일치라 헤더 이름 밖 키도 잡고 같은 줄 다음 토큰을 먹음. `proxy_authorization: failed winerror=5 exit_code=7` → `winerror=5` 사라짐 · `authorization_status=denied errno=13` → `errno=13` 사라짐.
2. `failure_redaction.py:373` · `ps1:165` — authorization 키마다 남은 글 전체를 복사(슬라이스·Substring) → O(n²). Bearer 4만 줄(1.8 MB): 0.30 s → 3.14 s(py). `match(text, pos)`/`\G` 로 충분.
3. 같은 꼬리 경계가 JSON 이스케이프 `\` 까지 먹음: `json.dumps({"detail": 'curl -H "Authorization: Basic <C>" x'})` → JSON 깨짐 → 알려진 env 비밀이 있으면 REPORT_UNAVAILABLE. Bearer 에 있던 결함이 모든 스킴으로 넓어짐.
4. `docs/spec/BACKLOG.md:882` · `assembly_failure_contract.md:66` 「Bearer … 보존」 글이 P2-1 과 어긋남 · Bearer 출력이 `Bearer [REDACTED:field]` → `[REDACTED:field]`(스킴 낱말 사라짐, 칸·형식은 같음).

## 통과
- 목표 꼴: Basic/Token/Negotiate 같은 줄·`\r\n\t` 접힘·`Authorization:`CRLF 값·CRLF+한 토큰·`a:b`·repr/`-H`/HTTP_AUTHORIZATION/X- → 자격 0. CRLF 다음 줄 `errno=5`·여러 낱말 보존.
- Py↔PS: 정규식 글자 묶음·`\Z`/`\z`·`=` 떼기·할당 검사 같은 뜻 · ps1 BOM 없음/비ASCII 0/LF 유지 · 이 범위에서 글자 복사 0.
- 잘림·상한·칸 코드 변경 0.
- 지문: 인벤토리 재계산 `8a8d629c…`(107 sink) = py·ps1 핀 · INSTALL_THIS_PC 제거 핀 = 실제 sha · 설치기 probe 표 23 판 × 5 파일 새 값, 옛 값 0, BOM 유지. refresh `--check` 는 얕은 clone(d9160b6 없음)이라 안 돌림 — 핀 계산만 직접 재현.
- 시험: 새 파일 2개만 더함, 기존 시험 고침·삭제 0. P2 꼴은 새 시험에 없음.
