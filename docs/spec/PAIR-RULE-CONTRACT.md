# 포장 자동 업데이트의 서명 쌍 계약

요구와 구현 기준: w9setupprep sub-98/106 및 검사 앱 3f96bd2. 실제 현장 수용과 중앙 서명·게시를 대신하지 않는다.

- 권한은 설치된 공개 신뢰 키로 검증한 manifest 하나다. `auto_update`는 정확히 `from_commit`, `to_commit`, `forward_contract_sha256`, `rollback_contract_sha256`를 갖는다. 기존 `approved_transitions`의 정확한 양방향 source/contract 쌍도 같은 manifest에서 일치해야 한다. policy의 미래 쌍 목록은 권한이 아니다.
- 현재 판 번호는 무결성 검증된 stock의 `portable-manifest.json.version`, 후보 판 번호 권한은 서명 manifest다. 후보 stock version 및 모듈 수준 `APP_VERSION` literal과도 일치해야 한다. `vN.N.N[.N]`를 숫자로 비교하고 엄격한 상승만 허용한다. stock version이 없는 옛 입력은 재설치 안내로 거절하며 source에서 판 번호를 복구하지 않는다. builder의 literal 검사는 실행 없는 빌드 정합성 검사다.
- 계약 SHA는 `tools/label_writer_fence_contract.json`, `app/writer_session_fence.py` 순서로 경로 ASCII + LF + 실제 파일 SHA256의 32바이트를 누적해 재계산한다. JSON/개행/BOM을 정규화하지 않는다. 포장에는 검사 앱의 별도 sink JSON이 없으므로 기존 AST inventory 및 stock pin 검사는 구조·무결성 검사로 보존한다.
- data/API 원문 SHA와 같음/다름은 전이 증거다. 같다는 이유로 승인을 만들거나 다르다는 이유로 서명 쌍을 대체하지 않는다.
- 사용자 준비와 SYSTEM 최초 적용 및 PREPARED 재개는 `authorize_feed_transition`의 동일 규칙을 사용한다. 재개는 보호 packet, 설치 신뢰 키, 이전·후보 stock 및 기록의 두 방향 pin을 다시 확인한다. READY 실패의 PC 내부 코드 복귀만 기존 역방향 권한을 사용한다. CONFIRMED 뒤 임의 feed 하향은 지원하지 않는다.
- feed와 packet 주소는 ASCII IP/도메인(명시적 punycode 포함), HTTPS, 정확한 응답 URL만 사용한다. Unicode 원 호스트·userinfo·fragment·redirect와 기존 GitHub 호스팅 계열은 거절한다. 기존 장비 인증·같은 origin·CA 조건을 유지한다.

runtime 설정과 canonical 설치기의 내장 release-pair gate는 같은 URL 규칙을 사용한다. 빈 userinfo도 거절하고 port 형식/범위를 검증한다. PC 설치기의 순수 설정 검사도 빈 userinfo를 거절한다. 정상 사내 도메인·ASCII IPv4/IPv6·명시적 punycode 주소는 허용하며 요청·응답 주소에 별도의 정규화나 변환을 추가하지 않는다.

구현: `auto_update.py`의 `version_identity`, `packet_version_identity`, `authorize_feed_transition`, `authorize_prepared_replay`, 사용자 준비 및 SYSTEM caller; `Label_Match.py`의 URL 검사; portable builder와 PowerShell 설정 검사. focused 회귀·원 실패·판·환경은 sub-128 REPORT.md에 기록한다. stock/전체/실제 SYSTEM·feed·전원 손실·독립 검토는 NOT TESTED다.
